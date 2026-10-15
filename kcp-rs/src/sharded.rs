//! UDP listener: one receive task demuxes a shared socket, and each accepted
//! session drives its own KCP state machine.
//!
//! ```text
//! rx task (one per listener, spawned on the caller's runtime)
//!   recvmmsg → group the burst by source address (order preserved)
//!        │
//!        ├─ building peer  → per-session queue (bounded, drop-tail)
//!        └─ live session   → feed_raw_batch(whole peer group) inline:
//!                            one decrypt pass, ONE KCP lock, ONE flush, one
//!                            sendmmsg for the group — no cross-task hop
//!        ▼
//! KcpStream task (the stream's own input loop drains the build queue +
//! flush loop) → datagrams → shared socket
//! ```
//!
//! Grouping the burst before feeding is what keeps the stale-session guard
//! honest: `process_inbound_batch` counts a burst as stale only when EVERY
//! datagram in it mismatched the session (a re-dialed peer), so a single late
//! retransmission mixed into a burst of fresh data must not evict the session.
//! Feeding per datagram would make every datagram a one-packet burst and evict
//! live sessions under loss — the bug this grouping exists to prevent.
//!
//! The library opens no OS threads and no private runtime: the receive task
//! and every session task are `knet::spawn_task`s on whatever runtime the
//! caller built.
//!
//! Outbound datagrams of every session share the one unconnected listen
//! socket. The syscall is safe to issue concurrently, but the kernel send
//! buffer is socket-wide, so sends take a lock that covers only the syscall
//! (see [`crate::transport::PeerTransport`]). A session that finds the buffer
//! full drops the lock before retrying on a bounded timer, so one session's
//! wait cannot stall the others' syscalls.

use rustc_hash::FxHashMap as HashMap;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::io;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::config::KcpConfig;
use crate::conn::{conv_setter, kcp_config_setters, resolve_one, KcpStream};
use crate::transport::{PeerQueue, PeerTransport, SharedSendLock, TransportWrapper, MAX_DATAGRAM};
use knet::Notify;

/// How many datagrams one receive wakeup pulls off the socket before yielding
/// back to the caller's runtime. Bounds how long the receive task occupies a
/// runtime worker under a flood. Must stay comfortably above a peer's
/// congestion window: the stale-session guard evaluates whole bursts, so a
/// ceiling near one peer's send window would split its traffic into
/// mostly-fresh bursts and dilute the guard.
const RECV_BATCH: usize = 256;

/// Default bound on a session's inbound queue. A datagram that does not fit is
/// dropped and counted; KCP retransmission recovers it. An unbounded queue
/// would let one fast peer pin the listener's memory.
const SESSION_INBOX_CAP: usize = 2048;

/// How often the receive task reaps dead and idle sessions. With traffic the
/// sweep still runs on this cadence — never per batch, so its O(sessions)
/// clone-and-scan cannot scale with packet rate.
const SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// Max concurrent sessions before new peers are refused.
///
/// A server session is created from a single inbound datagram, so an unbounded
/// map is a remote memory-exhaustion primitive. Raise it through
/// [`WorkerPoolLimits`] to serve more.
const DEFAULT_MAX_SESSIONS: usize = 4096;
/// Consecutive all-stale bursts required before eviction (P0-6 / M-4).
const DEFAULT_STALE_EVICT_THRESHOLD: u32 = 3;
/// Minimum wall-clock span of a stale streak before eviction. Together with
/// the burst-count threshold this keeps eviction off until a peer has been
/// *persistently* talking in a previous generation — one lost-ACK burst, or
/// a short forged burst, must not tear a live session down.
const EVICT_MIN_STALE_SPAN_MS: u64 = 1000;
/// Default per-IP new-session rate (P1-10).
const DEFAULT_PER_IP_SESSION_RATE: u32 = 20;
const IP_LIMITER_CAP: usize = 4096;
const IP_LIMITER_CLEANUP_BUDGET: usize = 32;
const IP_LIMITER_WINDOW: Duration = Duration::from_secs(1);

/// Default idle-session reap threshold. A KCP session with no inbound datagram
/// and no successful write for this long is closed and removed.
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(600);

/// Default cap on how long a peer may sit in the `building` map.
const DEFAULT_BUILDING_TIMEOUT: Duration = Duration::from_secs(10);

// ─── RX buffer pool (lock-free recycling) ─────────────────────────────────

const BUFPOOL_CAP: usize = 4096;

type BufRx = crossbeam_channel::Receiver<Vec<u8>>;
type BufTx = crossbeam_channel::Sender<Vec<u8>>;

fn bufpool() -> &'static (BufTx, BufRx) {
    use crossbeam_channel::bounded;
    use std::sync::OnceLock;
    static POOL: OnceLock<(BufTx, BufRx)> = OnceLock::new();
    POOL.get_or_init(|| {
        let (tx, rx) = bounded(BUFPOOL_CAP);
        (tx, rx)
    })
}

/// Return an empty RX buffer to the pool. Only buffers with full datagram
/// capacity are retained; undersized buffers are freed as normal.
#[inline]
pub(crate) fn recycle_buf(mut buf: Vec<u8>) {
    if buf.capacity() >= MAX_DATAGRAM {
        buf.clear();
        let _ = bufpool().0.try_send(buf);
    }
}

/// Return a whole burst to the pool on a drop path (listener closed, session
/// cap hit, integrity gate failed).
fn recycle_all(datagrams: Vec<Vec<u8>>) {
    for buf in datagrams {
        recycle_buf(buf);
    }
}

/// Take a pooled buffer, or `None` when the pool is empty. The returned buffer
/// has capacity `≥ MAX_DATAGRAM` and length 0.
#[inline]
pub(crate) fn acquire_buf() -> Option<Vec<u8>> {
    match bufpool().1.try_recv() {
        Ok(buf) if buf.capacity() >= MAX_DATAGRAM => Some(buf),
        _ => None,
    }
}

// ─── KcpListener ─────────────────────────────────────────────────────────

/// Resource limits for [`KcpListener`].
#[derive(Debug, Clone, Copy)]
pub struct WorkerPoolLimits {
    /// Max concurrent sessions, including builds and unaccepted connections
    /// (0 = unlimited sessions, with a 4096-entry accept-backlog limit).
    pub max_sessions_per_worker: usize,
    /// Drop-tail cap on each session's inbound queue (0 = `SESSION_INBOX_CAP`).
    pub worker_channel_cap: usize,
    /// Max datagrams the receive task pulls per wakeup (0 = `RECV_BATCH`).
    pub max_drain_packets: usize,
    /// A peer stuck in `building` longer than this is forgotten.
    /// `Duration::ZERO` = no timeout.
    pub building_timeout: Duration,
    /// A session with no inbound datagram and no successful write for this long
    /// is closed and removed. `Duration::ZERO` = never reap on idleness.
    ///
    /// KCP has no keepalive of its own: an idle session emits nothing, so
    /// `is_dead()` (retransmission budget exhausted) never trips for it, and
    /// without this timeout an abandoned peer is pinned until the process
    /// exits.
    pub idle_timeout: Duration,
    /// Consecutive all-stale bursts required before a session is evicted
    /// (P0-6 / M-4). `1` reproduces the old single-burst behaviour; the
    /// default `3` survives an ACK-blackhole RTO of segment 0.
    pub stale_evict_threshold: u32,
    /// Max **new sessions per second** from one IP (P1-10). `0` disables.
    /// Only admission is limited — established sessions keep full throughput.
    pub per_ip_session_rate: u32,
    /// Max concurrent sessions (published + building) admitted per source IP
    /// (`0` = unlimited). Opt-in defence-in-depth: the rate limit above does
    /// not bound how many sessions one host can *hold*, so a slow-and-steady
    /// client can still pin the whole global budget by varying source ports.
    pub max_sessions_per_ip: usize,
}

impl Default for WorkerPoolLimits {
    fn default() -> Self {
        Self {
            max_sessions_per_worker: DEFAULT_MAX_SESSIONS,
            worker_channel_cap: SESSION_INBOX_CAP,
            max_drain_packets: 0,
            building_timeout: DEFAULT_BUILDING_TIMEOUT,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            stale_evict_threshold: DEFAULT_STALE_EVICT_THRESHOLD,
            per_ip_session_rate: DEFAULT_PER_IP_SESSION_RATE,
            max_sessions_per_ip: 0,
        }
    }
}

fn accept_backlog_limit(limits: WorkerPoolLimits) -> usize {
    if limits.max_sessions_per_worker == 0 {
        DEFAULT_MAX_SESSIONS
    } else {
        limits.max_sessions_per_worker
    }
}

/// Live snapshot of [`KcpListener`] resource accounting.
#[derive(Debug, Default, Clone, Copy)]
pub struct WorkerPoolStats {
    /// Sessions currently in the map.
    pub sessions: usize,
    /// Sessions currently under construction (`building` table).
    pub building: usize,
    /// Datagrams dropped because a session's inbound queue was full.
    pub channel_drops: u64,
    /// New sessions refused because `max_sessions_per_worker` was reached.
    pub session_drops: u64,
    /// `KcpStream::build` failures.
    pub build_failures: u64,
    /// Datagrams from an unknown peer that failed the pre-admission integrity
    /// check, so no session was created (spoofed or stray traffic).
    pub unauthenticated_drops: u64,
    /// Datagrams dropped because the source address could not be parsed out of
    /// the receive batch (they cannot be routed to a session).
    pub bad_addr_drops: u64,
    /// Sessions closed and removed by the idle reaper.
    pub idle_reaps: u64,
}

/// Atomic counters behind [`KcpListener::stats`].
#[derive(Default)]
struct ListenerStats {
    channel_drops: AtomicU64,
    session_drops: AtomicU64,
    build_failures: AtomicU64,
    unauthenticated_drops: AtomicU64,
    bad_addr_drops: AtomicU64,
    idle_reaps: AtomicU64,
}

struct PendingAccept {
    conn: KcpStream,
    peer: SocketAddr,
}

/// The caller holds `sessions` before `pending` whenever both are needed.
/// Detach owners while locked, but close/drop them after releasing the locks.
fn take_pending_matching(
    pending: &mut VecDeque<PendingAccept>,
    mut matches: impl FnMut(&PendingAccept) -> bool,
) -> Vec<PendingAccept> {
    if pending.is_empty() {
        return Vec::new();
    }
    let mut kept = VecDeque::with_capacity(pending.len());
    let mut removed = Vec::new();
    for entry in pending.drain(..) {
        if matches(&entry) {
            removed.push(entry);
        } else {
            kept.push_back(entry);
        }
    }
    *pending = kept;
    removed
}

fn close_pending(pending: Vec<PendingAccept>) {
    for mut entry in pending {
        entry.conn.attach_owner();
        entry.conn.close();
    }
}

type WatchedSession = (SocketAddr, KcpStream, u64);

type IpSessionCounts = Arc<Mutex<HashMap<IpAddr, usize>>>;

fn session_count_for_ip(counts: &IpSessionCounts, ip: IpAddr) -> usize {
    counts.lock().get(&ip).copied().unwrap_or(0)
}

/// Only the bookkeeping entry owns a reservation. Build tasks and stream
/// clones never own it, so a timed-out build cannot release a replacement's
/// slot when its result arrives late.
struct IpSessionReservation {
    counts: IpSessionCounts,
    ip: IpAddr,
}

impl IpSessionReservation {
    fn try_acquire(counts: &IpSessionCounts, ip: IpAddr, limit: usize) -> Option<Self> {
        let mut map = counts.lock();
        let count = map.get(&ip).copied().unwrap_or(0);
        if limit > 0 && count >= limit {
            return None;
        }
        map.insert(ip, count.checked_add(1)?);
        Some(Self {
            counts: counts.clone(),
            ip,
        })
    }
}

impl Drop for IpSessionReservation {
    fn drop(&mut self) {
        let mut map = self.counts.lock();
        if let Some(count) = map.get_mut(&self.ip) {
            if *count > 1 {
                *count -= 1;
            } else {
                map.remove(&self.ip);
            }
        } else {
            debug_assert!(false, "missing per-IP session reservation");
        }
    }
}

struct BuildingEntry {
    queue: Arc<PeerQueue>,
    started: Instant,
    generation: u64,
    ip_reservation: Option<IpSessionReservation>,
}

/// One accepted peer. The build queue is not held here: the session's
/// transport owns it (and drains it during the building window), so the map
/// entry needs only the connection handle.
struct Session {
    conn: KcpStream,
    _ip_reservation: Option<IpSessionReservation>,
}

/// KCP listener over one shared UDP socket.
///
/// A single receive task demultiplexes inbound datagrams by source address
/// into per-session queues. Each accepted [`KcpStream`] runs its own input and
/// flush tasks on the caller's runtime, so the listener opens no threads of
/// its own.
pub struct KcpListener {
    socket: Arc<knet::DatagramSocket>,
    sessions: Arc<Mutex<HashMap<SocketAddr, Session>>>,
    building: Arc<Mutex<HashMap<SocketAddr, BuildingEntry>>>,
    pending: Arc<Mutex<VecDeque<PendingAccept>>>,
    accept_notify: Arc<Notify>,
    closed: Arc<AtomicBool>,
    /// Cancels the receive task's socket read the moment [`close`](Self::close)
    /// runs, so shutdown does not wait out the sweep interval.
    stop: knet::CancellationToken,
    last_error: Arc<Mutex<Option<io::Error>>>,
    stats: Arc<ListenerStats>,
    _rx: knet::JoinHandle<()>,
}

impl Drop for KcpListener {
    fn drop(&mut self) {
        // `close()` only stops accept. Drop is what ends the receive task, so
        // a discarded listener does not keep reading the socket.
        self.close();
        self.stop.cancel();
    }
}

impl KcpListener {
    /// Bind a UDP socket on `addr` and return a builder.
    pub fn bind(addr: impl ToSocketAddrs) -> KcpListenerBuilder {
        bind_listener(addr)
    }

    /// Use an already-bound datagram socket, preserving caller socket options.
    pub fn from_socket(socket: Arc<knet::DatagramSocket>) -> KcpListenerBuilder {
        from_socket_listener(socket)
    }

    /// Remove a peer from the session map after its accepted connection ends.
    /// A later datagram from the same address creates a fresh connection and is
    /// surfaced by [`accept`](Self::accept).
    ///
    /// The removed session is closed: the map holds the last non-owning clone,
    /// so dropping it would leave the flush loop running until the process
    /// exits. A not-yet-accepted build for the same peer is dropped too, so
    /// [`accept`](Self::accept) cannot hand out a connection this call just
    /// tore down.
    /// Returns `true` if a live session, build, or backlog entry was removed.
    pub fn remove_peer(&self, peer: SocketAddr) -> bool {
        // Publish takes this same lock order. Removing the build generation
        // and its published state together prevents a late build from adding
        // a backlog entry after this call returned.
        let (building, session, stale) = {
            let mut building = self.building.lock();
            let mut sessions = self.sessions.lock();
            let mut pending = self.pending.lock();
            (
                building.remove(&peer),
                sessions.remove(&peer),
                take_pending_matching(&mut pending, |p| p.peer == peer),
            )
        };
        let removed = building.is_some() || session.is_some() || !stale.is_empty();
        if let Some(build) = building {
            build.queue.mark_closed();
        }
        if let Some(session) = session {
            session.conn.close();
        }
        close_pending(stale);
        removed
    }

    /// Current number of known peer sessions.
    pub fn session_count(&self) -> usize {
        self.sessions.lock().len()
    }

    /// Current number of peers whose session is still being built.
    pub fn building_count(&self) -> usize {
        self.building.lock().len()
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Accept the next client connection.
    pub async fn accept(&self) -> io::Result<(KcpStream, SocketAddr)> {
        loop {
            if let Some(accepted) = self.try_accept()? {
                return Ok(accepted);
            }
            let notified = self.accept_notify.notified();
            if let Some(accepted) = self.try_accept()? {
                return Ok(accepted);
            }
            notified.await;
        }
    }

    /// Accept within `timeout`.
    pub async fn accept_timeout(&self, timeout: Duration) -> io::Result<(KcpStream, SocketAddr)> {
        knet::timeout(timeout, self.accept())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "accept timed out"))?
    }

    /// Non-blocking accept.
    pub fn try_accept(&self) -> io::Result<Option<(KcpStream, SocketAddr)>> {
        if self.closed.load(Ordering::Acquire) {
            self.discard_pending();
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "KcpListener closed",
            ));
        }
        let (accepted, stale) = {
            let mut sessions = self.sessions.lock();
            let mut pending = self.pending.lock();
            let mut accepted = None;
            let mut stale = Vec::new();
            while !self.closed.load(Ordering::Acquire) {
                let Some(mut entry) = pending.pop_front() else {
                    break;
                };
                let same_session = sessions
                    .get(&entry.peer)
                    .map(|s| s.conn.same_connection(&entry.conn))
                    .unwrap_or(false);
                // dead_link state is informational (see lookup_established): a
                // pending session whose KCP hit dead_link during a loss storm is
                // still recoverable — never discard it here.
                if same_session && !entry.conn.is_closed() {
                    entry.conn.attach_owner();
                    accepted = Some((entry.conn, entry.peer));
                    break;
                }
                if same_session {
                    sessions.remove(&entry.peer);
                }
                stale.push(entry);
            }
            (accepted, stale)
        };
        close_pending(stale);
        if accepted.is_some() {
            return Ok(accepted);
        }
        if self.closed.load(Ordering::Acquire) {
            self.discard_pending();
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "KcpListener closed",
            ));
        }
        Ok(None)
    }

    /// Surface and clear the last transport error.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        Ok(self.last_error.lock().take())
    }

    /// Stop accepting new connections. A connection already returned by
    /// [`accept`](Self::accept) keeps running: the receive task stays up,
    /// because it still needs inbound datagrams. A session that hits a full
    /// send buffer retries on its own bounded timer and does not depend on any
    /// listener task.
    ///
    /// A connection that was built but not yet accepted is closed here, and so
    /// is one whose build finishes after this call. Both tasks end when the
    /// listener is dropped.
    pub fn close(&self) {
        if self
            .closed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.discard_pending();
            self.accept_notify.notify_waiters();
        }
    }

    /// Close every connection waiting to be accepted. Called once `closed` is
    /// set, so a build that finished in the meantime is not handed out.
    fn discard_pending(&self) {
        let (queued, builds) = {
            let mut building = self.building.lock();
            let mut sessions = self.sessions.lock();
            let mut pending = self.pending.lock();
            let queued: Vec<PendingAccept> = pending.drain(..).collect();
            for entry in &queued {
                if sessions
                    .get(&entry.peer)
                    .map(|s| s.conn.same_connection(&entry.conn))
                    .unwrap_or(false)
                {
                    sessions.remove(&entry.peer);
                }
            }
            let builds: Vec<BuildingEntry> = building.drain().map(|(_, entry)| entry).collect();
            (queued, builds)
        };
        for build in builds {
            build.queue.mark_closed();
        }
        close_pending(queued);
    }

    /// Live resource-accounting snapshot.
    pub fn stats(&self) -> WorkerPoolStats {
        // Each lock is taken and **released** by its own `let` statement. In
        // the previous struct-literal form both temporaries lived until the
        // end of the statement, so this call held `sessions` while waiting
        // for `building` — the inverse of the publish/admit/register paths'
        // documented `building` → `sessions` order. That ABBA inversion
        // deadlocked `session_cap_test` under load (main parked in
        // `stats()` at this line, a build task parked in `spawn_build_task`
        // holding `building` and waiting for `sessions`); lldb backtraces in
        // bugs/BUGREPORT_STATS_BUILDING_SESSIONS_ABBA_DEADLOCK.md.
        let sessions = self.sessions.lock().len();
        let building = self.building.lock().len();
        WorkerPoolStats {
            sessions,
            building,
            channel_drops: self.stats.channel_drops.load(Ordering::Relaxed),
            session_drops: self.stats.session_drops.load(Ordering::Relaxed),
            build_failures: self.stats.build_failures.load(Ordering::Relaxed),
            unauthenticated_drops: self.stats.unauthenticated_drops.load(Ordering::Relaxed),
            bad_addr_drops: self.stats.bad_addr_drops.load(Ordering::Relaxed),
            idle_reaps: self.stats.idle_reaps.load(Ordering::Relaxed),
        }
    }
}

// ─── Builder ────────────────────────────────────────────────────────────

/// Builder for [`KcpListener`].
pub struct KcpListenerBuilder {
    addr: Option<SocketAddr>,
    socket: Option<Arc<knet::DatagramSocket>>,
    config: KcpConfig,
    resolve_err: Option<io::Error>,
    transport_wrapper: Option<TransportWrapper>,
    limits: Option<WorkerPoolLimits>,
    /// Test hook: sleep this long after `KcpStream::build()` returns and
    /// before the session is published, so a `close()` can land on a finished
    /// but not-yet-queued build. `Duration::ZERO` disables it.
    testing_build_delay: Duration,
}

impl KcpListenerBuilder {
    kcp_config_setters!();
    conv_setter!();

    /// Wrap each accepted peer transport before constructing its `KcpStream`.
    pub fn transport_wrapper<F>(mut self, wrapper: F) -> Self
    where
        F: Fn(
                Arc<dyn crate::transport::PacketTransport>,
                SocketAddr,
            ) -> Arc<dyn crate::transport::PacketTransport>
            + Send
            + Sync
            + 'static,
    {
        self.transport_wrapper = Some(Arc::new(wrapper));
        self
    }

    /// Override resource limits.
    pub fn limits(mut self, limits: WorkerPoolLimits) -> Self {
        self.limits = Some(limits);
        self
    }

    /// Test hook: hold a finished build before publishing it, so `close()` can
    /// race an in-flight build deterministically. Not for production use.
    #[doc(hidden)]
    pub fn testing_build_delay(mut self, delay: Duration) -> Self {
        self.testing_build_delay = delay;
        self
    }

    /// Bind the listen socket, spawn the receive task, and return the listener.
    ///
    /// The receive task and every accepted session's tasks run on the caller's
    /// runtime. The listener creates none of its own.
    pub async fn build(self) -> io::Result<KcpListener> {
        crate::conn::validate_async_config(&self.config)?;
        if let Some(e) = self.resolve_err {
            return Err(e);
        }

        let socket = match self.socket {
            Some(s) => s,
            None => {
                let addr = self.addr.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "KcpListener: bind address required",
                    )
                })?;
                Arc::new(knet::DatagramSocket::Udp(knet::UdpSocket::bind(addr)?))
            }
        };

        let limits = self.limits.unwrap_or_default();
        let sessions = Arc::new(Mutex::new(HashMap::default()));
        let building = Arc::new(Mutex::new(HashMap::default()));
        let pending = Arc::new(Mutex::new(VecDeque::new()));
        let accept_notify = Arc::new(Notify::new());
        let closed = Arc::new(AtomicBool::new(false));
        let stop = knet::CancellationToken::new();
        let last_error = Arc::new(Mutex::new(None));
        let stats = Arc::new(ListenerStats::default());
        let send_lock = SharedSendLock::new();

        let rx = spawn_rx(RxArgs {
            socket: socket.clone(),
            sessions: sessions.clone(),
            building: building.clone(),
            build_gen: AtomicU64::new(0),
            pending: pending.clone(),
            accept_notify: accept_notify.clone(),
            closed: closed.clone(),
            stop: stop.clone(),
            last_error: last_error.clone(),
            stats: stats.clone(),
            send_lock,
            config: self.config,
            transport_wrapper: self.transport_wrapper,
            limits,
            ip_limiter: Arc::new(Mutex::new(IpRateLimiter::default())),
            ip_counts: Arc::new(Mutex::new(HashMap::default())),
            testing_build_delay: self.testing_build_delay,
        });

        Ok(KcpListener {
            socket,
            sessions,
            building,
            pending,
            accept_notify,
            closed,
            stop,
            last_error,
            stats,
            _rx: rx,
        })
    }
}

/// `KcpListener::bind(addr).await` — awaitable without an explicit `.build()`.
impl std::future::IntoFuture for KcpListenerBuilder {
    type Output = io::Result<KcpListener>;
    type IntoFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.build())
    }
}

/// Bind a UDP socket on `addr` and return a builder for the listener.
pub fn bind_listener(addr: impl ToSocketAddrs) -> KcpListenerBuilder {
    match resolve_one(addr) {
        Ok(a) => KcpListenerBuilder {
            addr: Some(a),
            socket: None,
            config: KcpConfig::default(),
            resolve_err: None,
            transport_wrapper: None,
            limits: None,
            testing_build_delay: Duration::ZERO,
        },
        Err(e) => KcpListenerBuilder {
            addr: None,
            socket: None,
            config: KcpConfig::default(),
            resolve_err: Some(e),
            transport_wrapper: None,
            limits: None,
            testing_build_delay: Duration::ZERO,
        },
    }
}

/// Build a listener on an already-bound datagram socket.
pub fn from_socket_listener(socket: Arc<knet::DatagramSocket>) -> KcpListenerBuilder {
    KcpListenerBuilder {
        addr: None,
        socket: Some(socket),
        config: KcpConfig::default(),
        resolve_err: None,
        transport_wrapper: None,
        limits: None,
        testing_build_delay: Duration::ZERO,
    }
}

// ─── Receive task ───────────────────────────────────────────────────────

struct RxArgs {
    socket: Arc<knet::DatagramSocket>,
    sessions: Arc<Mutex<HashMap<SocketAddr, Session>>>,
    /// Peers whose session is being built. Datagrams that arrive in that window
    /// go to the queue the build owns, instead of opening a second session.
    /// Each entry records its generation: a build that outlives its
    /// `building_timeout` is forgotten, and only the build that still owns the
    /// slot may publish the session (so a slow build cannot overwrite a
    /// replacement that already took its place).
    building: Arc<Mutex<HashMap<SocketAddr, BuildingEntry>>>,
    /// Source of [`Self::building`] generations.
    build_gen: AtomicU64,
    pending: Arc<Mutex<VecDeque<PendingAccept>>>,
    accept_notify: Arc<Notify>,
    closed: Arc<AtomicBool>,
    stop: knet::CancellationToken,
    last_error: Arc<Mutex<Option<io::Error>>>,
    stats: Arc<ListenerStats>,
    send_lock: Arc<SharedSendLock>,
    config: KcpConfig,
    transport_wrapper: Option<TransportWrapper>,
    limits: WorkerPoolLimits,
    /// Per-IP new-session rate limiter (P1-10).
    ip_limiter: Arc<Mutex<IpRateLimiter>>,
    /// Reserved building + published slots. Disabled caps do not add entries.
    ip_counts: IpSessionCounts,
    /// Test hook: hold a finished build before publishing. See
    /// [`KcpListenerBuilder::testing_build_delay`].
    testing_build_delay: Duration,
}

/// P1-10: admit at most `rate` new sessions per second from one IP.
/// Returns `false` when the peer is over budget. Only *new* sessions are
/// limited — established ones keep full throughput.
fn allow_new_session(limiter: &Mutex<IpRateLimiter>, ip: IpAddr, rate: u32) -> bool {
    if rate == 0 {
        return true;
    }
    limiter.lock().allow(ip, rate, Instant::now())
}

#[derive(Default)]
struct IpRateLimiter {
    entries: HashMap<IpAddr, (Instant, u32)>,
    /// One deadline record per admitted window; refreshed entries may leave
    /// an older record until budgeted cleanup reaches it.
    expiry: BinaryHeap<Reverse<(Instant, IpAddr)>>,
}

impl IpRateLimiter {
    /// At most `budget` heap records, never a retain scan over fresh entries.
    fn expire(&mut self, now: Instant, budget: usize) -> usize {
        let mut removed = 0;
        while removed < budget {
            let Some(&Reverse((window, ip))) = self.expiry.peek() else {
                break;
            };
            if now.saturating_duration_since(window) < IP_LIMITER_WINDOW {
                break;
            }
            self.expiry.pop();
            if self.entries.get(&ip).map(|entry| entry.0) == Some(window) {
                self.entries.remove(&ip);
            }
            removed += 1;
        }
        removed
    }

    fn allow(&mut self, ip: IpAddr, rate: u32, now: Instant) -> bool {
        if rate == 0 {
            return true;
        }
        self.expire(now, IP_LIMITER_CLEANUP_BUDGET);
        if let Some(entry) = self.entries.get_mut(&ip) {
            if now.saturating_duration_since(entry.0) >= IP_LIMITER_WINDOW {
                *entry = (now, 0);
                self.expiry.push(Reverse((now, ip)));
            }
            if entry.1 >= rate {
                return false;
            }
            entry.1 += 1;
        } else {
            // Do not evict an active IP to admit another: that would reset
            // its rate budget under a high-cardinality source flood.
            if self.entries.len() >= IP_LIMITER_CAP {
                return false;
            }
            self.entries.insert(ip, (now, 1));
            self.expiry.push(Reverse((now, ip)));
        }
        // An expired refresh always pops at least one due heap record first,
        // so the expiry index is bounded as well as the live table.
        debug_assert!(self.expiry.len() <= IP_LIMITER_CAP);
        true
    }
}

fn spawn_rx(args: RxArgs) -> knet::JoinHandle<()> {
    knet::spawn_task(async move {
        let batch_cap = if args.limits.max_drain_packets > 0 {
            args.limits.max_drain_packets
        } else {
            RECV_BATCH
        };
        let inbox_cap = if args.limits.worker_channel_cap > 0 {
            args.limits.worker_channel_cap
        } else {
            SESSION_INBOX_CAP
        };
        // First-packet scratch for each wakeup. Refilled from the shared
        // BufPool after `take` hands the buffer to `burst` — a fresh
        // `vec![0; MAX_DATAGRAM]` would malloc 2KB on every quiet wakeup.
        let mut slot = acquire_buf().unwrap_or_else(|| Vec::with_capacity(MAX_DATAGRAM));
        let mut watched: Vec<WatchedSession> = Vec::new();
        // Reused across bursts: the datagram buffers themselves move into
        // queues / KCP, so only the index vectors are cleared.
        let mut burst: Vec<(SocketAddr, Vec<u8>)> = Vec::with_capacity(64);
        let mut groups: Vec<(SocketAddr, Vec<Vec<u8>>)> = Vec::new();
        let mut group_index: HashMap<SocketAddr, usize> = HashMap::default();
        let mut last_sweep = Instant::now();
        // recvmmsg scratch: one syscall fills up to RECV_MMSG_MAX slots.
        // Slots move into `burst` and are replaced from the buffer pool.
        const RECV_MMSG_MAX: usize = 64;
        let mut recv_bufs: Vec<Vec<u8>> = Vec::with_capacity(RECV_MMSG_MAX);
        let mut recv_peers: Vec<SocketAddr> = Vec::with_capacity(RECV_MMSG_MAX);

        loop {
            // `close()` does not end this task. An accepted session still needs
            // its peer's datagrams, and `deliver_group` refuses new peers once
            // `closed` is set. The task ends when the listener is dropped,
            // which cancels `stop`.
            //
            // No per-wakeup `yield_now` / `evict_stale` here: live sessions
            // are fed inline in `process_burst`, so their stale counters are
            // already up to date when that returns. Eviction then runs once
            // per burst on the peers just watched.

            // Refill `slot` after a previous `take` emptied it: keep the
            // `vec![0; ...]`-equivalent len, but source capacity from the
            // pool so a taken (cap-0) vec doesn't malloc 2KB every wakeup.
            if slot.len() < MAX_DATAGRAM {
                if slot.capacity() < MAX_DATAGRAM {
                    slot = acquire_buf().unwrap_or_else(|| Vec::with_capacity(MAX_DATAGRAM));
                    if slot.capacity() < MAX_DATAGRAM {
                        slot.reserve(MAX_DATAGRAM - slot.capacity());
                    }
                }
                slot.resize(MAX_DATAGRAM, 0);
            }
            // The timeout bounds a live listener so the idle sweep runs on a
            // quiet socket. `stop` ends the task when the listener is dropped.
            let first = knet::timeout(
                SWEEP_INTERVAL,
                knet::race(
                    args.stop.cancelled(),
                    std::pin::pin!(args.socket.recv_from(&mut slot)),
                ),
            )
            .await;
            let (n, peer) = match first {
                Ok(knet::RaceOutcome::First(())) => break,
                Ok(knet::RaceOutcome::Second(Ok((n, peer)))) if n > 0 => (n, peer),
                Ok(knet::RaceOutcome::Second(Ok(_))) => {
                    sweep(&args);
                    last_sweep = Instant::now();
                    continue;
                }
                Ok(knet::RaceOutcome::Second(Err(e))) => {
                    *args.last_error.lock() = Some(e);
                    knet::sleep_ms(10).await;
                    continue;
                }
                Err(_) => {
                    // Quiet for a whole sweep interval.
                    sweep(&args);
                    last_sweep = Instant::now();
                    continue;
                }
            };

            let mut buf = std::mem::take(&mut slot);
            buf.truncate(n);
            burst.push((peer, buf));

            // The rest of the burst is already queued in the kernel. Drain it
            // here so one peer's burst is grouped and fed to KCP as ONE batch —
            // per-datagram feeding makes every datagram a one-packet burst,
            // which both multiplies KCP locks and send syscalls and defeats
            // the whole-burst stale-session guard.
            //
            // Linux drains via `recvmmsg` (one syscall per ≤64 datagrams).
            // `try_recv_batch_from_into` returns `Ok(0)` for TcpRaw (no batch
            // path), so fall back to single `try_recv_from` to keep draining.
            while burst.len() < batch_cap {
                let want = (batch_cap - burst.len()).min(RECV_MMSG_MAX);
                while recv_bufs.len() < want {
                    let mut b = acquire_buf().unwrap_or_else(|| Vec::with_capacity(MAX_DATAGRAM));
                    if b.capacity() < MAX_DATAGRAM {
                        b.reserve(MAX_DATAGRAM - b.capacity());
                    }
                    recv_bufs.push(b);
                }
                let batch_n = match args
                    .socket
                    .try_recv_batch_from_into(&mut recv_bufs[..want], &mut recv_peers)
                {
                    Ok(n) => n,
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => 0,
                    Err(e) => {
                        *args.last_error.lock() = Some(e);
                        knet::sleep_ms(10).await;
                        break;
                    }
                };
                if batch_n > 0 {
                    let parsed = batch_n.min(recv_peers.len());
                    for i in 0..parsed {
                        let buf = std::mem::take(&mut recv_bufs[i]);
                        let peer = recv_peers[i];
                        // `try_recv_batch_from_into` leaves len == payload.
                        burst.push((peer, buf));
                        recv_bufs[i] =
                            acquire_buf().unwrap_or_else(|| Vec::with_capacity(MAX_DATAGRAM));
                    }
                    // Source address missing/unparseable: cannot route.
                    let bad = batch_n - parsed;
                    if bad > 0 {
                        args.stats
                            .bad_addr_drops
                            .fetch_add(bad as u64, Ordering::Relaxed);
                        for item in recv_bufs.iter_mut().take(batch_n).skip(parsed) {
                            item.clear();
                        }
                        // Progress requires a routable peer. A batch that
                        // yielded none would spin forever dropping datagrams.
                        if parsed == 0 {
                            break;
                        }
                    }
                    continue;
                }
                // Batch empty or unsupported (TcpRaw) — single-datagram drain.
                let mut buf = std::mem::take(&mut recv_bufs[0]);
                if buf.capacity() < MAX_DATAGRAM {
                    buf.reserve(MAX_DATAGRAM - buf.capacity());
                }
                buf.resize(MAX_DATAGRAM, 0);
                match args.socket.try_recv_from(&mut buf) {
                    Ok((n, peer)) if n > 0 => {
                        buf.truncate(n);
                        burst.push((peer, buf));
                        recv_bufs[0] =
                            acquire_buf().unwrap_or_else(|| Vec::with_capacity(MAX_DATAGRAM));
                    }
                    _ => {
                        buf.clear();
                        recv_bufs[0] = buf;
                        break;
                    }
                }
            }

            process_burst(
                &args,
                &mut burst,
                &mut groups,
                &mut group_index,
                inbox_cap,
                &mut watched,
            );
            // `feed_raw_batch` updated every watched session's stale counter
            // before returning, so the eviction check needs no yield.
            if !watched.is_empty() {
                evict_stale(&args, &mut watched);
            }

            // The sweep is O(sessions) and must not scale with packet rate:
            // run it on its interval, not once per drained batch.
            if last_sweep.elapsed() >= SWEEP_INTERVAL {
                sweep(&args);
                last_sweep = Instant::now();
            }
        }
    })
}

/// Group one drained burst by source address, preserving first-seen order, and
/// hand each peer's datagrams to [`deliver_group`] as a unit. Per-peer grouping
/// is what restores main's `feed_raw_batch` amortization — one KCP lock, one
/// flush and one sendmmsg per peer per burst — and what keeps the whole-burst
/// stale-session guard meaningful.
fn process_burst(
    args: &RxArgs,
    burst: &mut Vec<(SocketAddr, Vec<u8>)>,
    groups: &mut Vec<(SocketAddr, Vec<Vec<u8>>)>,
    group_index: &mut HashMap<SocketAddr, usize>,
    inbox_cap: usize,
    watched: &mut Vec<WatchedSession>,
) {
    for (peer, buf) in burst.drain(..) {
        match group_index.get(&peer) {
            Some(&i) => groups[i].1.push(buf),
            None => {
                group_index.insert(peer, groups.len());
                groups.push((peer, vec![buf]));
            }
        }
    }
    group_index.clear();
    for (peer, datagrams) in groups.drain(..) {
        deliver_group(args, peer, datagrams, inbox_cap, watched);
    }
}

/// Outcome of the established-session lookup (one `sessions` lock).
enum Established {
    Live(KcpStream),
    Reaped(KcpStream),
    None,
}

/// Phase 1 of [`deliver_group`]: a peer whose session is being built is
/// claimed even when its queue is full. Falling through would open a second
/// session for the same address and split one handshake across two state
/// machines. Returns `None` when the burst was consumed by the build's queue.
fn push_to_building(
    args: &RxArgs,
    peer: SocketAddr,
    datagrams: Vec<Vec<u8>>,
    inbox_cap: usize,
) -> Option<Vec<Vec<u8>>> {
    // None => the burst was consumed by an in-flight build's queue;
    // Some => no build owns this peer, continue with the established lookup.
    let queue = match args.building.lock().get(&peer) {
        Some(build) => build.queue.clone(),
        None => return Some(datagrams),
    };
    for buf in datagrams {
        if !queue.push(buf, inbox_cap) {
            args.stats.channel_drops.fetch_add(1, Ordering::Relaxed);
        }
    }
    None
}

/// Phase 2: one `sessions` lock for lookup + optional reap. `close()` runs
/// outside the guard: holding the map across it stalls every other peer's
/// deliver.
fn lookup_established(args: &RxArgs, peer: SocketAddr) -> Established {
    let (removed, stale) = {
        let mut sessions = args.sessions.lock();
        match sessions.get(&peer) {
            // `is_dead()` (dead_link, xmit ≥ 20) is INFORMATIONAL: kcp-go sets
            // `state = -1` and nothing in Go ever reads it back — sess.go has no
            // close-on-dead path. Treating it as removal here turned recoverable
            // loss-storm stalls (peer alive, delivery resumed within the RTO
            // ladder) into hard session death: observed live in the corruption
            // hunt, `bugs/ANALYSIS_KCP_RTO_INFLATION_TAIL_STALLS.md`. Closed
            // sessions still leave on sight; a truly vanished peer is covered by
            // the idle reaper (and, in the production stack, SMUX keepalive).
            Some(s) if !s.conn.is_closed() => {
                return Established::Live(s.conn.clone());
            }
            None => return Established::None,
            Some(_) => {}
        }
        let removed = sessions.remove(&peer).unwrap().conn;
        let stale = take_pending_matching(&mut args.pending.lock(), |p| {
            p.peer == peer && p.conn.same_connection(&removed)
        });
        (removed, stale)
    };
    close_pending(stale);
    Established::Reaped(removed)
}

/// Phase 3: admission is decided before any state is allocated. In-flight
/// builds count against the cap: with `--crypt none`/`null` the integrity
/// gate is a no-op, so a spoofed source-address flood would otherwise pile
/// up unbounded `building` entries (each a spawn task + PeerQueue) while
/// every published session is still under the limit.
///
/// Returns `false` when the burst must be dropped (stats already counted).
fn admit_new_session(args: &RxArgs, peer: SocketAddr) -> bool {
    // Lock order is `building` then `sessions`, matching the publish path.
    {
        let building = args.building.lock();
        let sessions = args.sessions.lock();
        let pending = args.pending.lock();
        if (args.limits.max_sessions_per_worker > 0
            && sessions.len() + building.len() >= args.limits.max_sessions_per_worker)
            || pending.len() + building.len() >= accept_backlog_limit(args.limits)
        {
            drop(pending);
            drop(sessions);
            drop(building);
            args.stats.session_drops.fetch_add(1, Ordering::Relaxed);
            return false;
        }
    }

    // P1-10: per-IP new-session rate limit (defence in depth). Without crypto
    // a single host can burn the whole `max_sessions_per_worker` quota.
    if !allow_new_session(&args.ip_limiter, peer.ip(), args.limits.per_ip_session_rate) {
        args.stats.session_drops.fetch_add(1, Ordering::Relaxed);
        log::debug!("listener: per-IP session rate limit hit for {}", peer.ip());
        return false;
    }

    // Opt-in per-IP cap on pinned session state. The rate limit above does
    // not bound how many sessions one address can *hold*, so a slow-and-steady
    // client can still pin the whole `max_sessions_per_worker` budget by
    // varying source ports. The register phase reserves the slot atomically;
    // this inexpensive first check avoids doing integrity work for a full IP.
    if args.limits.max_sessions_per_ip > 0 {
        let count = session_count_for_ip(&args.ip_counts, peer.ip());
        if count >= args.limits.max_sessions_per_ip {
            args.stats.session_drops.fetch_add(1, Ordering::Relaxed);
            log::debug!(
                "listener: per-IP concurrent session cap hit for {} ({count})",
                peer.ip(),
            );
            return false;
        }
    }
    true
}

/// Phase 4: integrity-gate a *copy* of the first datagram: one that fails
/// the transport's CRC32 / AEAD check buys no state at all. The copy is
/// discarded — the originals stay ciphertext and are what the queue
/// receives, so the session input loop decrypts exactly once (decrypting
/// here *and* there would drop the handshake burst).
///
/// On success the fresh `PeerQueue` and the (possibly wrapped) transport for
/// the upcoming build are returned; on failure `unauthenticated_drops` is
/// counted and the caller drops the burst.
fn gate_new_session(
    args: &RxArgs,
    peer: SocketAddr,
    datagrams: &[Vec<u8>],
) -> Option<(Arc<PeerQueue>, Arc<dyn crate::transport::PacketTransport>)> {
    let mut gate = vec![datagrams[0].clone()];
    let queue = Arc::new(PeerQueue::new());
    let transport = Arc::new(PeerTransport {
        queue: queue.clone(),
        socket: args.socket.clone(),
        peer,
        send_lock: Some(args.send_lock.clone()),
    });
    let transport: Arc<dyn crate::transport::PacketTransport> = match &args.transport_wrapper {
        Some(wrapper) => wrapper(transport, peer),
        None => transport,
    };
    decrypt_batch_in_place(transport.as_ref(), &mut gate);
    if gate.is_empty() {
        args.stats
            .unauthenticated_drops
            .fetch_add(1, Ordering::Relaxed);
        return None;
    }
    Some((queue, transport))
}

/// Phase 5: hand the burst to a fresh build. The build runs on its own task
/// — `deliver_group` runs inside the receive task, and nesting a runtime
/// there panics; the datagrams wait in the queue, which is registered before
/// the spawn so the next datagram finds it. Returns `false` when the second
/// admission check rejected the burst (it counts the stat and recycles the
/// burst itself).
fn register_and_spawn_build(
    args: &RxArgs,
    peer: SocketAddr,
    datagrams: Vec<Vec<u8>>,
    inbox_cap: usize,
    queue: Arc<PeerQueue>,
    transport: Arc<dyn crate::transport::PacketTransport>,
) {
    let gen = args.build_gen.fetch_add(1, Ordering::Relaxed);
    {
        // Second admission check: the integrity gate above can take long
        // enough for concurrent builds to finish, and the cap must hold
        // across the whole admission path, not just at its entry.
        let mut b = args.building.lock();
        let sessions = args.sessions.lock();
        let pending = args.pending.lock();
        if args.closed.load(Ordering::Acquire) {
            drop(pending);
            drop(sessions);
            drop(b);
            recycle_all(datagrams);
            return;
        }
        if (args.limits.max_sessions_per_worker > 0
            && sessions.len() + b.len() >= args.limits.max_sessions_per_worker)
            || pending.len() + b.len() >= accept_backlog_limit(args.limits)
        {
            drop(pending);
            drop(sessions);
            drop(b);
            args.stats.session_drops.fetch_add(1, Ordering::Relaxed);
            recycle_all(datagrams);
            return;
        }
        // The integrity gate may have taken time. Recheck + reserve while
        // holding lifecycle locks; no publish/remove can change this budget
        // between the reservation and insertion.
        let ip_reservation = if args.limits.max_sessions_per_ip > 0 {
            match IpSessionReservation::try_acquire(
                &args.ip_counts,
                peer.ip(),
                args.limits.max_sessions_per_ip,
            ) {
                Some(reservation) => Some(reservation),
                None => {
                    drop(pending);
                    drop(sessions);
                    drop(b);
                    args.stats.session_drops.fetch_add(1, Ordering::Relaxed);
                    recycle_all(datagrams);
                    return;
                }
            }
        } else {
            None
        };
        b.insert(
            peer,
            BuildingEntry {
                queue: queue.clone(),
                started: Instant::now(),
                generation: gen,
                ip_reservation,
            },
        );
    }
    for buf in datagrams {
        if !queue.push(buf, inbox_cap) {
            args.stats.channel_drops.fetch_add(1, Ordering::Relaxed);
        }
    }
    spawn_build_task(args, peer, gen, queue, transport);
}

/// Spawn the build task with the `RxArgs` handles it needs after
/// [`register_and_spawn_build`] registered the `building` slot.
fn spawn_build_task(
    args: &RxArgs,
    peer: SocketAddr,
    gen: u64,
    queue: Arc<PeerQueue>,
    transport: Arc<dyn crate::transport::PacketTransport>,
) {
    let sessions = args.sessions.clone();
    let building = args.building.clone();
    let pending = args.pending.clone();
    let accept_notify = args.accept_notify.clone();
    let closed = args.closed.clone();
    let stats = args.stats.clone();
    let config = args.config.clone();
    let testing_build_delay = args.testing_build_delay;
    let max_sessions = args.limits.max_sessions_per_worker;
    // Even callers opting out of the live-session cap cannot retain an
    // unlimited accept backlog. With a live cap, every waiting connection is
    // also represented in `sessions`, and consumes that same admission slot.
    let max_pending = accept_backlog_limit(args.limits);
    let queue = queue.clone();
    knet::spawn_task(async move {
        let conn = match KcpStream::with_transport(transport, peer)
            .connected(false)
            .adopt_conv(true)
            .config(config)
            .build()
            .await
        {
            Ok(conn) => {
                // Test hook: hold a *finished* build so `close()` can land
                // before it is published. No-op in production.
                if !testing_build_delay.is_zero() {
                    knet::sleep_ms(testing_build_delay.as_millis() as u64).await;
                }
                conn
            }
            Err(_) => {
                stats.build_failures.fetch_add(1, Ordering::Relaxed);
                let mut b = building.lock();
                if matches!(b.get(&peer), Some(build) if build.generation == gen) {
                    b.remove(&peer);
                }
                return;
            }
        };
        // Publish only if this build still owns the `building` slot. A build
        // that outlived `building_timeout` has been forgotten (and its queue
        // closed) and may have been replaced by a fresh session — inserting
        // unconditionally would overwrite that session and leak it. Hold the
        // building lock across the insert so the reaper cannot drop the slot
        // in between; `deliver` checks `building` first and both entries point
        // at the same queue, so a datagram that lands in the gap still reaches
        // this session.
        {
            let mut b = building.lock();
            if !matches!(b.get(&peer), Some(build) if build.generation == gen) {
                drop(b);
                conn.close();
                return;
            }
            let entry = b.remove(&peer).unwrap();
            // Final cap check before publish. Several builds that passed
            // admission together can all finish at once; without this the
            // published map would overshoot `max_sessions_per_worker`.
            let mut live = sessions.lock();
            let mut waiting = pending.lock();
            if closed.load(Ordering::Acquire) || conn.is_closed() {
                drop(waiting);
                drop(live);
                drop(b);
                conn.close();
                queue.mark_closed();
                return;
            }
            if (max_sessions > 0 && live.len() >= max_sessions) || waiting.len() >= max_pending {
                drop(waiting);
                drop(live);
                drop(b);
                log::warn!(
                    "kcp-rs: dropping built session for {peer}: session or accept-backlog limit reached before publish"
                );
                conn.close();
                // The build's queue still holds the handshake datagrams and
                // its input loop may be parked on it; release the slot's
                // memory instead of leaving a dead build's backlog pinned.
                queue.mark_closed();
                stats.session_drops.fetch_add(1, Ordering::Relaxed);
                return;
            }
            // All lifecycle mutations use building → sessions → pending.
            // remove_peer cannot slip between insertion and publication, and
            // close discards this exact connection if its flag changes here.
            live.insert(
                peer,
                Session {
                    conn: conn.clone(),
                    _ip_reservation: entry.ip_reservation,
                },
            );
            waiting.push_back(PendingAccept { conn, peer });
        }
        accept_notify.notify_one();
    });
}

/// Push one peer's burst at its existing session, or start building one.
///
/// Orchestrates the five phases: [`push_to_building`], the established
/// lookup (inline feed / reap), the closed-listener check,
/// [`admit_new_session`], [`gate_new_session`] and
/// [`register_and_spawn_build`].
fn deliver_group(
    args: &RxArgs,
    peer: SocketAddr,
    datagrams: Vec<Vec<u8>>,
    inbox_cap: usize,
    watched: &mut Vec<WatchedSession>,
) {
    let Some(datagrams) = push_to_building(args, peer, datagrams, inbox_cap) else {
        return;
    };

    match lookup_established(args, peer) {
        Established::Live(conn) => {
            // Feed the whole group straight into KCP on this task and send the
            // ACKs it produces before returning to the socket. Queueing it for
            // the session's own input task costs a scheduling hop, and on a
            // coarse timer wheel that hop measured 6–13ms — the whole of the
            // fast-mode round trip. The count is taken first:
            // `feed_raw_batch` updates it synchronously, and `evict_stale`
            // compares against it once this burst has been delivered. The
            // batch is the stale-session guard's unit: a group counts as a
            // previous generation only when every datagram in it mismatched.
            let before = conn.stale_burst_count();
            conn.feed_raw_batch(datagrams);
            watched.push((peer, conn, before));
            return;
        }
        Established::Reaped(conn) => {
            conn.close();
            // Fall through: the group now counts as traffic from an unknown
            // peer and may open a fresh session below.
        }
        Established::None => {}
    }

    // No session yet. A closed listener takes no new peers; datagrams for one
    // already being built still went to its queue in phase 1.
    if args.closed.load(Ordering::Acquire) {
        recycle_all(datagrams);
        return;
    }

    if !admit_new_session(args, peer) {
        recycle_all(datagrams);
        return;
    }

    let Some((queue, transport)) = gate_new_session(args, peer, &datagrams) else {
        recycle_all(datagrams);
        return;
    };

    register_and_spawn_build(args, peer, datagrams, inbox_cap, queue, transport);
}

/// Evict sessions whose drained burst belonged entirely to a previous
/// generation. Called right after [`process_burst`], which feeds live sessions
/// inline and therefore updates their stale counters before returning.
///
/// The unit is the per-peer group fed by [`deliver_group`]:
/// `process_inbound_batch` bumps `stale_bursts` once, and only when EVERY
/// datagram of the group mismatched the session. A single stale datagram mixed
/// into fresh traffic therefore never reaches this function's eviction —
/// evicting on it would kill live sessions whose ACKs are being lost (the
/// peer's `una` freezes at 0 and its RTO retransmits segment 0, which is
/// indistinguishable from a re-dial until more traffic arrives).
///
/// P0-6 / M-4: even an *entire* all-stale burst is not enough. Under an ACK
/// black hole the peer's `snd_una` never advances, its RTO retransmits sn=0
/// with `una=0`, and if that retransmit is the only unacked datagram the
/// whole burst looks stale. Requiring N consecutive all-stale bursts (any
/// live burst resets the streak) is what separates that from a real re-dial.
fn evict_stale(args: &RxArgs, watched: &mut Vec<WatchedSession>) {
    let observed = std::mem::take(watched);
    let threshold = args.limits.stale_evict_threshold.max(1);
    for (peer, observed_conn, before) in observed {
        let (removed, stale) = {
            let mut sessions = args.sessions.lock();
            let Some(session) = sessions.get(&peer) else {
                continue;
            };
            if !session.conn.same_connection(&observed_conn)
                || session.conn.stale_burst_count() <= before
            {
                continue;
            }
            // A single all-stale burst can be an ACK-blackhole RTO of sn=0.
            // Only a sustained streak — spanning both N bursts and a
            // wall-clock window — means the peer really re-dialed. The time
            // span stops a forged burst from satisfying the count threshold
            // in zero time.
            let now_ms = knet::mono_ms();
            let first = session.conn.first_stale_ms();
            if session.conn.consecutive_stale_burst_count() < threshold as u64
                || first == 0
                || now_ms.saturating_sub(first) < EVICT_MIN_STALE_SPAN_MS
            {
                continue;
            }
            let removed = sessions.remove(&peer).unwrap();
            let stale = take_pending_matching(&mut args.pending.lock(), |p| {
                p.peer == peer && p.conn.same_connection(&removed.conn)
            });
            (removed, stale)
        };
        log::warn!(
            "listener: evicting stale session for {peer}: the peer re-dialed on the same address"
        );
        removed.conn.close();
        close_pending(stale);
    }
}

/// Decrypt a burst in place and drop every datagram that fails the transport's
/// integrity check, compacting the survivors to the front.
fn decrypt_batch_in_place(
    transport: &dyn crate::transport::PacketTransport,
    datagrams: &mut Vec<Vec<u8>>,
) {
    let mut write = 0usize;
    for read in 0..datagrams.len() {
        let n = datagrams[read].len();
        let pn = transport.decrypt_packet_in_place(&mut datagrams[read], n);
        if pn > 0 {
            datagrams[read].truncate(pn);
            if write != read {
                datagrams.swap(write, read);
            }
            write += 1;
        }
    }
    for d in datagrams.drain(write..) {
        recycle_buf(d);
    }
}

/// Drop sessions the listener should no longer keep alive.
///
/// A session is reaped when it is already closed, or when
/// `limits.idle_timeout` has passed with no inbound datagram and no
/// successful write. Every reaped session is closed, not just dropped: the
/// map holds a non-owning clone, so a bare `remove` leaves the flush loop
/// running.
///
/// `is_dead()` (dead_link) is deliberately NOT a reap trigger: kcp-go sets
/// `state = -1` and nothing in Go reads it back, and under a loss storm
/// dead_link fires on *recoverable* links (20 fast-ladder retransmits ≈ 11 s
/// at `rto=100`) — killing then converts a slow-but-healing session into a
/// hard teardown while the peer is alive. See
/// `bugs/ANALYSIS_KCP_RTO_INFLATION_TAIL_STALLS.md`.
fn sweep(args: &RxArgs) {
    // On a silent listener the existing sweep timer also bounds stale limiter
    // storage lifetime: at most ceil(4096 / 32) sweep intervals after expiry.
    args.ip_limiter
        .lock()
        .expire(Instant::now(), IP_LIMITER_CLEANUP_BUDGET);
    // Scan the whole map. The lock is only held to clone the session handles;
    // the `is_closed()` / idle checks run after it is dropped. Capping the
    // scan (the old 4096) starved anything past that prefix: `HashMap`
    // iteration order is bucket order, so the same entries came first on
    // every pass until one of them was removed, and a closed session behind
    // them was never reaped.
    let candidates: Vec<(SocketAddr, KcpStream)> = {
        let sessions = args.sessions.lock();
        sessions.iter().map(|(p, s)| (*p, s.conn.clone())).collect()
    };
    if args.limits.building_timeout > Duration::ZERO {
        // Close the queue of a build that never finished. Otherwise it stays
        // open with whatever handshake datagrams it held, and nothing drains it.
        let abandoned: Vec<Arc<crate::transport::PeerQueue>> = {
            let mut building = args.building.lock();
            let expired: Vec<SocketAddr> = building
                .iter()
                .filter(|(_, build)| build.started.elapsed() >= args.limits.building_timeout)
                .map(|(peer, _)| *peer)
                .collect();
            expired
                .iter()
                .filter_map(|peer| building.remove(peer).map(|build| build.queue))
                .collect()
        };
        for queue in abandoned {
            queue.mark_closed();
        }
    }
    if candidates.is_empty() {
        return;
    }
    let now = knet::mono_ms();
    let idle_ms = args.limits.idle_timeout.as_millis() as u64;
    let mut dead: Vec<(SocketAddr, KcpStream, bool)> = Vec::new();
    for (peer, conn) in candidates {
        if conn.is_closed() {
            dead.push((peer, conn, false));
        } else if idle_ms > 0 && now.saturating_sub(conn.last_activity_ms()) >= idle_ms {
            dead.push((peer, conn, true));
        }
    }
    if dead.is_empty() {
        return;
    }
    let (evicted, stale, idle) = {
        let mut sessions = args.sessions.lock();
        let mut evicted = HashMap::default();
        let mut idle = 0u64;
        for (peer, observed_conn, was_idle) in dead {
            if sessions
                .get(&peer)
                .map(|s| s.conn.same_connection(&observed_conn))
                .unwrap_or(false)
            {
                evicted.insert(peer, sessions.remove(&peer).unwrap());
                idle += u64::from(was_idle);
            }
        }
        let stale = take_pending_matching(&mut args.pending.lock(), |p| {
            evicted
                .get(&p.peer)
                .map(|s| s.conn.same_connection(&p.conn))
                .unwrap_or(false)
        });
        (evicted, stale, idle)
    };
    for (_, session) in evicted {
        session.conn.close();
    }
    close_pending(stale);
    if idle > 0 {
        args.stats.idle_reaps.fetch_add(idle, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;

    // The per-IP concurrent cap is enforced in `deliver_group` and covered
    // end-to-end by
    // `tests/kcpstream_listener.rs::per_ip_session_limit_bounds_admission_from_one_address`.

    /// P1-10: a single IP must not open more than `rate` new sessions per
    /// second; other IPs are unaffected.
    #[test]
    fn per_ip_session_rate_limits_one_host() {
        let limiter = Mutex::new(IpRateLimiter::default());
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        let other: IpAddr = "10.0.0.2".parse().unwrap();
        let rate = 5;

        for i in 0..rate {
            assert!(
                allow_new_session(&limiter, ip, rate),
                "call {i} should be allowed"
            );
        }
        assert!(
            !allow_new_session(&limiter, ip, rate),
            "over-budget admission must be refused"
        );
        // A different IP is unaffected.
        assert!(allow_new_session(&limiter, other, rate));
        // rate=0 disables the limit.
        assert!(allow_new_session(&limiter, ip, 0));
    }
}

#[cfg(test)]
#[path = "sharded/optimization_tests.rs"]
mod optimization_tests;
