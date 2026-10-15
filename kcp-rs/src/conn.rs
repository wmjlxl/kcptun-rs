//! Async KCP connection (`KcpStream`) over a datagram transport.
//!
//! Raw KCP with optional Reed-Solomon FEC. Encryption is **not** inside
//! `KcpStream` — inject it via [`PacketTransport`] (e.g. `CryptoTransport` in
//! kcptun-common). Stack: UDP → decrypt → FEC → KCP (in); reverse outbound.
//!
//! Provides `knet::AsyncRead + AsyncWrite` so upper layers (SMUX, etc.) can
//! treat KCP like a reliable stream.
//!
//! Layout (engine/facade layering, mirroring a kernel socket split):
//! - `raw_queue` — wire-packet FIFO + read-prefetch buffer (`RawPacketQueue`,
//!   `ReadBuffer`)
//! - `endpoint` — the session engine: `SharedIoState` (the user-space
//!   `struct sock`) + the background input/flush loops
//! - `halves` — `AsyncRead`/`AsyncWrite` impls + tokio-style split halves
//! - this file — the `KcpStream` facade + builder
//!
//! Enable with `--features async` / `async` (tokio) or `async`.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::io;
use std::net::{Shutdown, SocketAddr, ToSocketAddrs};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;

use crate::config::KcpConfig;
#[cfg(test)]
use crate::config::KcpMode;

enum DialTransport {
    Udp,
    TcpRaw,
}
use crate::fec::{
    fec_expand_packets, fec_kcp_from_recovered, FecDecoder, FecEncoder, FEC_HEADER_SIZE_PLUS_2,
    FEC_TYPE_DATA, FEC_TYPE_PARITY,
};
use crate::kcp::KCP;
use crate::segment::KCP_MAX_FRAG;
use crate::transport::{PacketTransport, MAX_DATAGRAM};
use knet::CancellationToken;

/// FEC header + SIZE field (`fecHeaderSizePlus2` in Go).
const FEC_HDR: usize = FEC_HEADER_SIZE_PLUS_2;

/// Safety-net poll interval for `Notify` waits. The normal wake path fires
/// immediately via `notify_one` (permit-storing) or `notify_waiters` (registered
/// waiters); this longer interval only recovers from a *lost* wake — a
/// `notify_waiters` landing with no waiter registered, a multi-reader permit
/// race, or `close()` racing a `notified()` registration. 10ms instead of the
/// previous 2ms cuts the tokio timer-wheel churn ~5x under saturation while
/// still bounding any lost-wake recovery latency.
const WAIT_FALLBACK_MS: u64 = 10;

/// Active connections with unacknowledged data keep a fine-grained driver
/// deadline. This is scheduling precision only; KCP's protocol interval/RTO
/// fields remain unchanged. Idle connections still park without a timer.
const ACTIVE_UPDATE_MAX_MS: u64 = 10;
/// Keep a recently active connection's flush task armed long enough to avoid
/// repeated cold park/wake cycles on sparse interactive traffic. After one
/// quiet grace interval, a truly idle connection parks without another timer.
const IDLE_PARK_GRACE_MS: u64 = 1_000;

/// Default number of `yield_now` spins in `read()` before parking on `Notify`.
/// Overridable at process start via `KCP_BUSY_YIELDS` env var for tuning.
///
/// 0 = disabled (pure event-driven via `Notify`). This is the safe default for
/// multi-connection production servers where N readers spinning would waste CPU
/// and introduce cross-connection scheduling interference. For single-connection
/// or low-concurrency latency-sensitive workloads (e.g. benchmarking), set
/// `KCP_BUSY_YIELDS=256` or `512` to trade CPU for lower P999 tail latency
/// (measured: 512 yields → P999 0.6ms vs 0 → P999 13ms on a single connection).
const BUSY_POLL_YIELDS_DEFAULT: u16 = 0;

/// Read the effective busy-poll yield count from env var `KCP_BUSY_YIELDS`,
/// falling back to `BUSY_POLL_YIELDS_DEFAULT`. Cached in a `OnceLock` so the
/// env lookup happens only once.
fn busy_poll_yields() -> u16 {
    use std::sync::OnceLock;
    static VAL: OnceLock<u16> = OnceLock::new();
    *VAL.get_or_init(|| {
        std::env::var("KCP_BUSY_YIELDS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(BUSY_POLL_YIELDS_DEFAULT)
    })
}

/// Max bytes per `send_to_kcp` call.  Limits KCP mutex hold time by chunking
/// large writes (~49 segments at MSS=1326), matching Go's `Write` pattern where
/// the echo loop's 64KB buffer naturally chunks sends.  The caller's `write_all`
/// loop re-acquires the mutex between chunks, letting the input loop process
/// ACKs and open the send window sooner.
const KCP_SEND_CHUNK: usize = 64 * 1024;

// ─── Engine submodules (split for layering; all public paths unchanged) ────
mod endpoint;
mod halves;
mod raw_queue;

pub(crate) use endpoint::SharedIoState;
use endpoint::{spawn_flush_loop, spawn_input_loop};
pub use halves::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, WriteHalf};
#[cfg(test)]
use raw_queue::MAX_RETAINED_RAW_BATCH;
use raw_queue::{RawPacketQueue, ReadBuffer};

// ─── KcpStream ──────────────────────────────────────────────────────────────────

/// Reliable KCP stream over a datagram transport (`AsyncRead + AsyncWrite`).
///
/// Optional Reed-Solomon FEC when configured via [`.fec(d, p)`](KcpStreamBuilder::fec).
/// No encryption — inject via [`PacketTransport`] (e.g. CryptoTransport).
/// Background input/flush loops drive the KCP state machine; user I/O only
/// touches shared buffers + notifies.
pub struct KcpStream {
    shared: Arc<SharedIoState>,
    _handles: Vec<knet::JoinHandle<()>>,
    /// `true` for the original `KcpStream` created by the builder; `false` for
    /// clones.  Only the owner's `Drop` calls `close()` — clones dropping
    /// do NOT close the connection.
    owns_connection: bool,
}

impl KcpStream {
    /// Dial `addr` with a fresh UDP socket bound to `0.0.0.0:0` / `[::]:0`.
    ///
    /// ```no_run
    /// use kcp_rs::KcpStream;
    /// # fn main() {
    /// # let _fut = async {
    /// let conn = KcpStream::connect("127.0.0.1:29900").mtu(1400).build().await?;
    /// # Ok::<_, std::io::Error>(conn)
    /// # };
    /// # }
    /// ```
    ///
    /// Unless [`.conv()`](KcpStreamBuilder::conv) pins the conversation ID,
    /// the dial gets a **fresh random conv** (Go kcp-go `Dial` parity) — see
    /// [`random_conv`](crate::random_conv).
    pub fn connect(addr: impl ToSocketAddrs) -> KcpStreamBuilder {
        match resolve_one(addr) {
            Ok(remote) => KcpStreamBuilder {
                remote: Some(remote),
                transport: None,
                config: KcpConfig::default(),
                connected: true,
                resolve_err: None,
                dial: DialTransport::Udp,
                adopt_conv: false,
                conv_explicit: false,
                background_input: true,
                spawn_flush_loop: true,
                connect_timeout: None,
            },
            Err(e) => KcpStreamBuilder {
                remote: None,
                transport: None,
                config: KcpConfig::default(),
                connected: true,
                resolve_err: Some(e),
                dial: DialTransport::Udp,
                adopt_conv: false,
                conv_explicit: false,
                background_input: true,
                spawn_flush_loop: true,
                connect_timeout: None,
            },
        }
    }

    /// Connect to `addr` with a timeout. Returns a `KcpStreamBuilder` that
    /// will fail the build if no KCP response arrives within `timeout`.
    ///
    /// Mirrors `std::net::TcpStream::connect_timeout`.
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use kcp_rs::KcpStream;
    /// # fn main() {
    /// # let _fut = async {
    /// let conn = KcpStream::connect_timeout("127.0.0.1:29900", Duration::from_secs(5))
    ///     .build().await?;
    /// # Ok::<_, std::io::Error>(conn)
    /// # };
    /// # }
    /// ```
    pub fn connect_timeout(addr: impl ToSocketAddrs, timeout: Duration) -> KcpStreamBuilder {
        let mut b = Self::connect(addr);
        b.connect_timeout = Some(timeout);
        b
    }

    /// Build on an existing [`PacketTransport`] (UDP / TcpRaw / CryptoTransport).
    ///
    /// By default the transport is treated as **unconnected** (`send_batch_to`).
    /// Call [`.connected(true)`](KcpStreamBuilder::connected) when the socket was
    /// created via `UdpSocket::connect`.
    pub fn with_transport(
        transport: Arc<dyn PacketTransport>,
        remote: SocketAddr,
    ) -> KcpStreamBuilder {
        KcpStreamBuilder {
            remote: Some(remote),
            transport: Some(transport),
            config: KcpConfig::default(),
            connected: false,
            resolve_err: None,
            dial: DialTransport::Udp,
            adopt_conv: false,
            conv_explicit: false,
            background_input: true,
            spawn_flush_loop: true,
            connect_timeout: None,
        }
    }

    /// Dial over Linux raw-TCP (tcpraw). Non-Linux returns `io::Unsupported`
    /// at build time (stub), matching binary `--tcp`.
    pub fn connect_tcp(addr: impl ToSocketAddrs) -> KcpStreamBuilder {
        match resolve_one(addr) {
            Ok(remote) => KcpStreamBuilder {
                remote: Some(remote),
                transport: None,
                config: KcpConfig::default(),
                connected: true,
                resolve_err: None,
                dial: DialTransport::TcpRaw,
                adopt_conv: false,
                conv_explicit: false,
                background_input: true,
                spawn_flush_loop: true,
                connect_timeout: None,
            },
            Err(e) => KcpStreamBuilder {
                remote: None,
                transport: None,
                config: KcpConfig::default(),
                connected: true,
                resolve_err: Some(e),
                dial: DialTransport::TcpRaw,
                adopt_conv: false,
                conv_explicit: false,
                background_input: true,
                spawn_flush_loop: true,
                connect_timeout: None,
            },
        }
    }

    // ── KCP-specific tuning ────────────────────────────────────────────────
    // Prefixed `set_kcp_*` so the plain `set_*` names stay reserved for the
    // TcpStream-aligned surface below (no name collisions).

    /// Adjust the KCP send/receive window sizes after construction.
    pub fn set_kcp_window_size(&self, snd_wnd: u32, rcv_wnd: u32) {
        let mut kcp = self.shared.kcp.lock();
        kcp.set_snd_wnd(snd_wnd);
        kcp.set_rcv_wnd(rcv_wnd);
        let effective_snd_wnd = kcp.snd_wnd() as usize;
        self.shared
            .snd_wnd
            .store(effective_snd_wnd, Ordering::Relaxed);
        if self.shared.backpressure_relieved() {
            self.shared.write_notify.notify_one();
        }
    }

    // ── TcpStream-aligned surface ────────────────────────────────────────────

    /// TCP-style Nagle toggle. Maps to the KCP fast path
    /// (`nodelay=1, interval=10, resend=2, nc=1`) when `true`, and the normal
    /// path (`nodelay=0, interval=40, resend=2, nc=1`) when `false`. Full
    /// 4-knob KCP control is available via [`KcpConfig::nodelay`] at
    /// construction time.
    pub fn set_nodelay(&self, nodelay: bool) {
        self.shared.nodelay.store(nodelay, Ordering::Release);
        let (n, i, r, c) = if nodelay {
            (1, 10, 2, 1)
        } else {
            (0, 40, 2, 1)
        };
        self.shared.kcp.lock().set_nodelay(n, i, r, c);
    }

    /// Last value passed to [`set_nodelay`](Self::set_nodelay) (or the builder's
    /// configured mode on construction).
    pub fn nodelay(&self) -> bool {
        self.shared.nodelay.load(Ordering::Acquire)
    }

    /// Surface the last non-transient I/O error from the background loops,
    /// clearing it. Mirrors `std::net::TcpStream::take_error`.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        Ok(self.shared.last_error.lock().take())
    }

    /// Set poll-style I/O. When `true`, [`read`](Self::read) returns
    /// [`io::ErrorKind::WouldBlock`] instead of parking when nothing is
    /// buffered, and [`write_all`](Self::write_all) returns `WouldBlock` when
    /// the send window is full. `poll_read` / `poll_write` are unchanged:
    /// they already return `Pending` rather than parking the task.
    ///
    /// Default is `false` (park until data arrives or the window opens).
    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.shared
            .nonblocking
            .store(nonblocking, Ordering::Release);
        Ok(())
    }

    /// Set the TTL (hop limit). KCP runs over UDP, so this maps to the
    /// underlying socket's TTL when a raw `UdpSocket` is in use. For
    /// custom transports (e.g. `PeerTransport`), this is a no-op.
    ///
    /// Mirrors `std::net::TcpStream::set_ttl`.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.shared.transport.set_ttl(ttl)
    }

    /// Get the TTL (hop limit). Returns the underlying socket's TTL, or
    /// `64` (default) when the transport does not expose it.
    ///
    /// Mirrors `std::net::TcpStream::ttl`.
    pub fn ttl(&self) -> io::Result<u32> {
        self.shared.transport.ttl()
    }

    /// Peek at already-buffered inbound bytes without consuming them.
    ///
    /// Unlike `std::net::TcpStream::peek`, `KcpStream` is inherently async, so
    /// this is **non-blocking**: it returns [`io::ErrorKind::WouldBlock`] when
    /// no data has arrived yet. Use [`readable`](Self::readable) to await data
    /// first, then `peek`.
    pub fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        if self.shared.read_closed.load(Ordering::Acquire) {
            return Ok(0);
        }
        if buf.is_empty() {
            return Ok(0);
        }
        {
            let rb = self.shared.read_buf.lock();
            if let Some(data) = rb.front() {
                let n = data.len().min(buf.len());
                buf[..n].copy_from_slice(&data[..n]);
                return Ok(n);
            }
        }
        // With direct KCP reads, complete data normally remains in KCP rather
        // than the spill slot. Peek the first queued segment without consuming
        // it. (A fragmented message exposes its first segment, which is still
        // sufficient for the non-consuming readiness contract.)
        let kcp = self.shared.kcp.lock();
        let Some(_) = kcp.peeksize() else {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "no buffered data to peek",
            ));
        };
        let Some(seg) = kcp.peek_recv() else {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "no buffered data to peek",
            ));
        };
        let n = seg.data.len().min(buf.len());
        buf[..n].copy_from_slice(&seg.data[..n]);
        Ok(n)
    }

    /// Shut down the read and/or write half, mirroring
    /// `std::net::TcpStream::shutdown`.
    ///
    /// - [`Shutdown::Write`]: stop accepting writes (`poll_write` returns
    ///   `BrokenPipe`); queued data is still flushed. The **peer is not
    ///   notified** — KCP has no wire FIN, so peer-aware half-close lives at the
    ///   SMUX/session layer.
    /// - [`Shutdown::Read`]: stop surfacing inbound data; `poll_read` returns
    ///   `Ok(0)` (EOF).
    /// - [`Shutdown::Both`]: equivalent to [`close`](Self::close).
    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        match how {
            Shutdown::Read => {
                self.shared.read_closed.store(true, Ordering::Release);
                self.shared.read_buf.lock().clear();
                *self.shared.read_deadline.lock() = None;
                self.shared.wake_reader();
            }
            Shutdown::Write => {
                self.shared.write_closed.store(true, Ordering::Release);
                self.shared.flush_notify.notify_one();
                self.shared.wake_writer();
            }
            Shutdown::Both => {
                self.shared.read_closed.store(true, Ordering::Release);
                self.shared.write_closed.store(true, Ordering::Release);
                self.shared.close();
            }
        }
        Ok(())
    }

    /// Set the read timeout. [`read`](Self::read), `poll_read`,
    /// and [`readable`](Self::readable) return [`io::ErrorKind::TimedOut`] after
    /// it elapses with no data. `None` disables.
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        *self.shared.read_timeout.lock() = dur.map(|d| d.as_millis() as u64);
        Ok(())
    }

    /// Current read timeout.
    pub fn read_timeout(&self) -> io::Result<Option<Duration>> {
        Ok(self.shared.read_timeout.lock().map(Duration::from_millis))
    }

    /// Set the write timeout. [`write_all`](Self::write_all) and
    /// `poll_write` return [`io::ErrorKind::TimedOut`] after it elapses while
    /// blocked on a full send window. `None` disables.
    pub fn set_write_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        *self.shared.write_timeout.lock() = dur.map(|d| d.as_millis() as u64);
        Ok(())
    }

    /// Current write timeout.
    pub fn write_timeout(&self) -> io::Result<Option<Duration>> {
        Ok(self.shared.write_timeout.lock().map(Duration::from_millis))
    }

    /// Check for a partial read-buffer segment or a complete KCP message.
    /// Locks are deliberately acquired separately: the input path only holds
    /// the KCP lock, while readers only hold the read-buffer lock while
    /// inspecting the spill slot.
    fn has_read_data(&self) -> bool {
        if !self.shared.read_buf.lock().is_empty() {
            return true;
        }
        self.shared.kcp.lock().peeksize().is_some()
    }

    /// Copy available bytes from the bounded prefetch/spill queue and directly
    /// from KCP's receive queue. After acquiring KCP we recheck the queue so
    /// prefetched data cannot be overtaken by a direct read.
    pub(crate) fn read_available(&self, out: &mut [u8]) -> usize {
        let mut filled = 0usize;
        let mut consumed_kcp = false;

        loop {
            // First consume prefetched messages and any partial segment left
            // by an earlier short read.
            {
                let mut rb = self.shared.read_buf.lock();
                while filled < out.len() {
                    let Some(mut data) = rb.pop_front() else {
                        break;
                    };
                    let n = data.len().min(out.len() - filled);
                    out[filled..filled + n].copy_from_slice(&data[..n]);
                    filled += n;
                    if n < data.len() {
                        let _ = data.split_to(n);
                        rb.push_front(data);
                        break;
                    }
                }
            }
            if filled >= out.len() {
                break;
            }

            let mut kcp = self.shared.kcp.lock();
            // An input task may have prefetched between the first drain and
            // this lock acquisition. KCP excludes further prefetch while this
            // check runs; retry the queue to preserve strict FIFO order.
            if !self.shared.read_buf.lock().is_empty() {
                drop(kcp);
                continue;
            }
            while filled < out.len() {
                let data = match kcp.recv_bytes() {
                    Ok(data) if !data.is_empty() => data,
                    _ => break,
                };
                consumed_kcp = true;
                let n = data.len().min(out.len() - filled);
                out[filled..filled + n].copy_from_slice(&data[..n]);
                filled += n;
                if n < data.len() {
                    self.shared.read_buf.lock().push_front(data.slice(n..));
                    break;
                }
            }
            break;
        }

        // recv_bytes() may set KCP's ASK_TELL probe when the receive window
        // opens. Let the protocol-deadline flush loop emit that WINS packet.
        if consumed_kcp {
            self.shared.flush_notify.notify_one();
        }
        filled
    }

    /// Wait until data is available to read (or the connection is closed).
    /// Mirrors `tokio::net::TcpStream::readable`.
    pub async fn readable(&self) -> io::Result<()> {
        let deadline = self
            .shared
            .read_timeout
            .lock()
            .map(|ms| knet::mono_ms().saturating_add(ms));
        loop {
            if self.shared.read_closed.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "KcpStream read half closed",
                ));
            }
            if self.has_read_data() {
                return Ok(());
            }
            if self.shared.is_closed() || self.shared.read_closed.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "KcpStream closed",
                ));
            }
            match deadline {
                Some(dl) => {
                    let remaining = dl.saturating_sub(knet::mono_ms());
                    if remaining == 0 {
                        return Err(io::Error::new(io::ErrorKind::TimedOut, "read timed out"));
                    }
                    match knet::timeout(
                        Duration::from_millis(remaining),
                        knet::race(
                            Box::pin(self.shared.read_notify.notified()),
                            self.shared.cancel_token.cancelled(),
                        ),
                    )
                    .await
                    {
                        Ok(knet::RaceOutcome::First(_)) | Ok(knet::RaceOutcome::Second(_)) => {}
                        Err(_) => {
                            return Err(io::Error::new(io::ErrorKind::TimedOut, "read timed out"));
                        }
                    }
                }
                None => {
                    let _ = knet::race(
                        Box::pin(self.shared.read_notify.notified()),
                        self.shared.cancel_token.cancelled(),
                    )
                    .await;
                }
            }
        }
    }

    /// Wait until the send window has room (or the connection is closed).
    /// Mirrors `tokio::net::TcpStream::writable`.
    pub async fn writable(&self) -> io::Result<()> {
        let deadline = self
            .shared
            .write_timeout
            .lock()
            .map(|ms| knet::mono_ms().saturating_add(ms));
        loop {
            if self.shared.is_closed() || self.shared.write_closed.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "KcpStream closed",
                ));
            }
            if self.shared.backpressure_relieved() {
                return Ok(());
            }
            match deadline {
                Some(dl) => {
                    let remaining = dl.saturating_sub(knet::mono_ms());
                    if remaining == 0 {
                        return Err(io::Error::new(io::ErrorKind::TimedOut, "write timed out"));
                    }
                    match knet::timeout(
                        Duration::from_millis(remaining),
                        knet::race(
                            Box::pin(self.shared.write_notify.notified()),
                            self.shared.cancel_token.cancelled(),
                        ),
                    )
                    .await
                    {
                        Ok(knet::RaceOutcome::First(_)) | Ok(knet::RaceOutcome::Second(_)) => {}
                        Err(_) => {
                            return Err(io::Error::new(io::ErrorKind::TimedOut, "write timed out"));
                        }
                    }
                }
                None => {
                    let _ = knet::race(
                        Box::pin(self.shared.write_notify.notified()),
                        self.shared.cancel_token.cancelled(),
                    )
                    .await;
                }
            }
        }
    }

    pub fn close(&self) {
        self.shared.close();
    }

    pub fn is_closed(&self) -> bool {
        self.shared.is_closed()
    }

    /// Mark this `KcpStream` as the connection owner: its `Drop` will call
    /// `close()` when it is dropped. Used by `ShardedKcpListener::accept` to
    /// ensure the caller-owned stream tears down the session on drop, rather
    /// than leaking it until the idle reaper sweeps.
    pub(crate) fn attach_owner(&mut self) {
        self.owns_connection = true;
    }

    /// Monotonic timestamp in milliseconds of the latest successful read or write.
    pub fn last_activity_ms(&self) -> u64 {
        self.shared.last_activity_ms.load(Ordering::Relaxed)
    }

    /// Diagnostic: wire datagrams this endpoint has handed to its transport
    /// (counts retransmissions; excludes drops made by an outer wrapper).
    pub fn tx_pkts(&self) -> u64 {
        self.shared.tx_pkts.load(Ordering::Relaxed)
    }

    /// Diagnostic: wire datagrams this endpoint has accepted from its
    /// transport (post-wrapper, pre-FEC/KCP parse).
    pub fn rx_pkts(&self) -> u64 {
        self.shared.rx_pkts.load(Ordering::Relaxed)
    }

    /// The effective KCP MTU after any FEC overhead adjustment.
    #[doc(hidden)]
    pub fn kcp_mtu(&self) -> u32 {
        self.shared.kcp.lock().mtu()
    }

    /// Effective RTO (ms): when the next retransmission of a lost segment is
    /// due. Under sustained loss this inflates toward `RTO_MAX` (60 s) —
    /// stall diagnostics depend on seeing it live.
    pub fn rx_rto(&self) -> u32 {
        self.shared.kcp.lock().rx_rto()
    }

    /// Smoothed RTT (ms) feeding [`rx_rto`](Self::rx_rto).
    pub fn rx_srtt(&self) -> i32 {
        self.shared.kcp.lock().rx_srtt()
    }

    /// RTT variance (ms) feeding [`rx_rto`](Self::rx_rto).
    pub fn rx_rttvar(&self) -> i32 {
        self.shared.kcp.lock().rx_rttvar()
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.shared.remote_addr
    }

    /// Last transport error recorded on this session, if any (diagnostic —
    /// `note_io_error` decides fatality; a session closed by a stray ICMP
    /// during a loss storm shows it here).
    pub fn last_io_error(&self) -> Option<String> {
        self.shared
            .last_error
            .lock()
            .as_ref()
            .map(|e| e.to_string())
    }

    /// Datagrams dropped because they carried a different conversation ID.
    ///
    /// The listener uses this to tell "the peer re-dialed from this address"
    /// (evict the stale session) from "the peer went quiet" (leave it alone).
    pub fn conv_mismatch_count(&self) -> u64 {
        self.shared.conv_mismatch.load(Ordering::Relaxed)
    }

    /// Next sequence number this connection expects from the peer (0 = nothing
    /// received yet). Diagnostics for the re-dial detection above.
    pub fn rcv_nxt(&self) -> u32 {
        self.shared.kcp.lock().rcv_nxt()
    }

    /// Milliseconds since the last datagram received from the peer, of any kind.
    ///
    /// ACK-only packets count: a peer whose KCP still acknowledges our data is
    /// alive even while it sends no SMUX frames of its own, so "nothing at all
    /// arrived" — not "no frames arrived" — is the real peer-gone signal.
    pub fn rx_age_ms(&self) -> u64 {
        self.shared.rx_age_ms()
    }

    /// Next sequence number this connection will send. Diagnostics: a frozen
    /// `snd_nxt` with `wait_send > 0` means the sender stopped producing.
    pub fn snd_nxt(&self) -> u32 {
        self.shared.kcp.lock().snd_nxt()
    }

    /// Segments accepted by `send()` but not yet flushed into `snd_buf`
    /// (live read — unlike `wait_send()`, which reports the last cached
    /// flush-loop value). Stall forensics: `snd_queue_len > 0` with idle
    /// `tx` = flush never ran; `snd_nxt - snd_una > 0` with idle `tx` =
    /// retransmission ladder stalled.
    pub fn snd_queue_len(&self) -> u32 {
        self.shared.kcp.lock().snd_queue_len() as u32
    }

    /// Whether a sender currently holds the single-sender wire token
    /// (`is_sending`). Stall forensics: `sending=true` for the whole of a TX
    /// freeze with `raw_queue_len()>0` ⇒ the token is stuck (no drainer can
    /// send until it releases); `sending=false` with a frozen `tx` ⇒ the
    /// flush loop itself is not running.
    pub fn sending(&self) -> bool {
        self.shared.is_sending.load(Ordering::Acquire)
    }

    /// Wire packets drained from the write path and waiting for the token
    /// holder to hand them to the transport.
    pub fn raw_queue_len(&self) -> u32 {
        self.shared.raw_packets.lock().len() as u32
    }

    /// Flush-loop ticks (stall forensics — flat while segments are stuck in
    /// `snd_buf` means the loop stopped running).
    pub fn flush_ticks(&self) -> u64 {
        self.shared.flush_ticks.load(Ordering::Relaxed)
    }

    /// This connection's KCP `flush_with_current` call count.
    pub fn kcp_flush_count(&self) -> u64 {
        self.shared.kcp.lock().flush_calls()
    }

    /// This connection's RTO-branch retransmission count.
    pub fn rto_fires(&self) -> u64 {
        self.shared.kcp.lock().rto_fires()
    }

    /// Segments numbered 0 seen on a session that already received data: the
    /// peer re-dialed and reused this address, so this session is the stale one.
    pub fn peer_restart_count(&self) -> u64 {
        self.shared.peer_restart.load(Ordering::Relaxed)
    }

    /// Bursts in which every datagram belonged to a previous generation of the
    /// peer. The listener evicts a session on this, not on a single stale
    /// datagram inside an otherwise fresh burst.
    pub(crate) fn stale_burst_count(&self) -> u64 {
        self.shared.stale_bursts.load(Ordering::Relaxed)
    }

    /// Consecutive all-stale bursts, reset by any live burst (P0-6 / M-4).
    pub(crate) fn consecutive_stale_burst_count(&self) -> u64 {
        self.shared.consecutive_stale_bursts.load(Ordering::Relaxed)
    }

    /// When the current all-stale streak began (`mono_ms`), or 0 if none.
    pub(crate) fn first_stale_ms(&self) -> u64 {
        self.shared.first_stale_ms.load(Ordering::Relaxed)
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.shared.transport.local_addr()
    }

    /// Configured send window (for diagnostics / backpressure).
    pub fn snd_wnd(&self) -> usize {
        self.shared.snd_wnd.load(Ordering::Relaxed)
    }

    /// Configured receive window (for diagnostics).
    pub fn rcv_wnd(&self) -> u32 {
        self.shared.kcp.lock().rcv_wnd()
    }

    /// Current KCP wait_send snapshot.
    pub fn wait_send(&self) -> usize {
        self.shared.wait_send.load(Ordering::Relaxed)
    }

    /// Sequence number of the oldest outbound segment the peer has not ACKed.
    ///
    /// Polled together with [`KcpStream::wait_send`]: a frozen value while data
    /// is in flight means the peer is no longer acknowledging our segments
    /// (its KCP state was reset), which `dead_link` only notices after its
    /// full retransmission budget.
    pub fn snd_una(&self) -> u32 {
        self.shared.kcp.lock().snd_una()
    }

    /// Test seam: put this connection's KCP state into `dead_link` so the
    /// listener's sweep/delivery handling of dead sessions can be asserted
    /// without engineering 20 unacked retransmits.
    #[doc(hidden)]
    pub fn testing_mark_dead(&self) {
        self.shared.kcp.lock().testing_mark_dead();
    }

    /// Peer's most recently advertised receive window.
    ///
    /// Zero means the peer is out of buffer space and is *choosing* not to
    /// accept more data, so stalled acknowledgement progress is flow control
    /// rather than a desynchronized peer.
    pub fn rmt_wnd(&self) -> u32 {
        self.shared.kcp.lock().rmt_wnd()
    }

    /// Whether KCP has declared the link dead (retransmission budget spent).
    ///
    /// The background flush loop keeps running after this; callers (the
    /// binaries' dead-link detection) poll it and tear down the session.
    pub fn is_dead(&self) -> bool {
        self.shared.kcp.lock().is_dead()
    }

    /// Feed one peer's still-encrypted burst into KCP and send the ACKs it
    /// produces before returning. Used by the listener's receive task so an
    /// established session is not woken through its queue; the burst shares
    /// one KCP lock, one flush and one send, and is the stale-session guard's
    /// unit (eviction requires every datagram of the burst to mismatch).
    pub(crate) fn feed_raw_batch(&self, datagrams: Vec<Vec<u8>>) {
        self.shared.feed_raw_batch(datagrams)
    }

    /// Async read borrowing `&self` — safe for **concurrent** read/write tasks
    /// (the internal state is already shared behind mutexes/atomics).
    ///
    /// Mirrors `poll_read_into` semantics without needing `Pin<&mut Self>`.
    /// Waits on an internal notify when no KCP data is available (wake is immediate
    /// on data arrival; `close()` calls `notify_waiters()` to wake on close).
    ///
    /// Naming mirrors `std::io::Read::read` — the `&self` receiver allows
    /// concurrent calls from multiple tasks without `Pin<&mut Self>`.
    pub async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        let deadline = self
            .shared
            .read_timeout
            .lock()
            .map(|ms| knet::mono_ms().saturating_add(ms));
        loop {
            if buf.is_empty() {
                return Ok(0);
            }
            if self.shared.read_closed.load(Ordering::Acquire) {
                return Ok(0);
            }
            let filled = self.read_available(buf);
            if filled > 0 {
                return Ok(filled);
            }
            if self.shared.is_closed() || self.shared.read_closed.load(Ordering::Acquire) {
                return Ok(0);
            }
            if self.shared.nonblocking.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "no buffered data to read",
                ));
            }
            // A configured read timeout takes precedence. With no timeout,
            // wait on the permit-storing Notify indefinitely; a periodic
            // fallback here creates needless timer-wheel work and tail jitter.
            match deadline {
                Some(dl) => {
                    let remaining = dl.saturating_sub(knet::mono_ms());
                    if remaining == 0 {
                        return Err(io::Error::new(io::ErrorKind::TimedOut, "read timed out"));
                    }
                    match knet::timeout(
                        Duration::from_millis(remaining),
                        knet::race(
                            Box::pin(self.shared.read_notify.notified()),
                            self.shared.cancel_token.cancelled(),
                        ),
                    )
                    .await
                    {
                        Ok(knet::RaceOutcome::First(_)) | Ok(knet::RaceOutcome::Second(_)) => {}
                        Err(_) => {
                            return Err(io::Error::new(io::ErrorKind::TimedOut, "read timed out"));
                        }
                    }
                }
                None => {
                    // Spin-bounded busy-poll: yield a few times before
                    // parking on Notify. When the input loop's `wake_reader`
                    // fires, data is usually available within 1–2 yield cycles
                    // because `yield_now` only lets other ready tasks run (no
                    // timer-wheel or park/unpark overhead). This avoids the
                    // tokio task-schedule hop (notify → wake → schedule →
                    // poll) that adds 0.2–2ms of P999 tail latency per hop.
                    let mut spins = 0u16;
                    let max_spins = busy_poll_yields();
                    loop {
                        let filled = self.read_available(buf);
                        if filled > 0 {
                            return Ok(filled);
                        }
                        if self.shared.is_closed()
                            || self.shared.read_closed.load(Ordering::Acquire)
                        {
                            return Ok(0);
                        }
                        spins += 1;
                        if spins >= max_spins {
                            break;
                        }
                        knet::yield_now().await;
                    }
                    // Park on Notify — the permit-storing design means a
                    // `notify_one` that fired during the spin loop is not lost.
                    let _ = knet::race(
                        Box::pin(self.shared.read_notify.notified()),
                        self.shared.cancel_token.cancelled(),
                    )
                    .await;
                }
            }
        }
    }

    /// Async `write_all` borrowing `&self` — safe for concurrent read/write.
    ///
    /// Inline fast path (kcp.Send + kcp.flush under the KCP lock, matching
    /// kcp-go `UDPSession.Write`), then drains the resulting wire segments and
    /// sends them immediately — bypassing the background flush loop's 1–2ms
    /// wake-up that would otherwise add one hop per write (raw-KCP latency).
    /// When the send window is full it waits on the write notify.
    ///
    /// Naming mirrors `std::io::Write::write_all` — the `&self` receiver
    /// allows concurrent calls from multiple tasks without `Pin<&mut Self>`.
    pub async fn write_all(&self, buf: &[u8]) -> io::Result<()> {
        let mut offset = 0usize;
        let mut blocked_deadline = None;
        while offset < buf.len() {
            if self.shared.is_closed() || self.shared.write_closed.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "KcpStream closed",
                ));
            }
            if self.shared.nonblocking.load(Ordering::Acquire)
                && !self.shared.backpressure_relieved()
            {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "send window full",
                ));
            }
            // Inline send + flush under the KCP lock; NO await while the guard
            // is held (the guard is `!Send`, so all `.await`s live outside).
            let sent = {
                let mut kcp = self.shared.kcp.lock();
                let ws = kcp.wait_send() as usize;
                if ws >= self.shared.snd_wnd.load(Ordering::Relaxed) {
                    drop(kcp);
                    0
                } else {
                    let sent = self.shared.send_to_kcp(&mut kcp, &buf[offset..]);
                    drop(kcp);
                    self.shared.write_notify.notify_one();
                    sent
                }
            };
            // Inline send: drain raw_packets and send directly, bypassing the
            // flush loop's scheduling hop.  Falls back to `notify_one()` when
            // the flush loop is currently sending (is_sending=true).  This
            // matches Go's synchronous `UDPSession.Write` send path.
            if sent > 0 {
                blocked_deadline = None;
                let _ = self.shared.try_drain_and_send().await;
                // Inline UDP send handles the current packet, but KCP still
                // needs a retransmission deadline if that packet is lost.
                // Always wake the protocol loop so an idle parked connection
                // arms maintenance even when we acquired the send token.
                self.shared.flush_notify.notify_one();
            }
            if sent == 0 {
                // Send window full — strict backpressure matching Go's
                // `UDPSession.Write` (blocks on `chWriteEvent` when the window
                // is full, no extra buffering). This unifies `write_all_shared`
                // with `do_poll_write` which also returns Pending on window
                // full (P1 #7: previously `write_all_shared` buffered up to
                // one extra window, causing different in-flight data / latency
                // / memory peaks depending on which write API the caller used).
                // Copy the timeout out first: the parking_lot guard is `!Send`
                // and must not be held across the `.await` below.
                let write_timeout_ms = *self.shared.write_timeout.lock();
                match write_timeout_ms {
                    Some(ms) => {
                        let deadline = *blocked_deadline
                            .get_or_insert_with(|| knet::mono_ms().saturating_add(ms));
                        let remaining = deadline.saturating_sub(knet::mono_ms());
                        if remaining == 0 {
                            if !self.shared.backpressure_relieved() {
                                return Err(io::Error::new(
                                    io::ErrorKind::TimedOut,
                                    "write timed out",
                                ));
                            }
                            blocked_deadline = None;
                            continue;
                        }
                        match knet::timeout(
                            Duration::from_millis(remaining.max(1)),
                            knet::race(
                                Box::pin(self.shared.write_notify.notified()),
                                self.shared.cancel_token.cancelled(),
                            ),
                        )
                        .await
                        {
                            Ok(knet::RaceOutcome::First(_)) | Ok(knet::RaceOutcome::Second(_)) => {}
                            Err(_) => {
                                if !self.shared.backpressure_relieved() {
                                    return Err(io::Error::new(
                                        io::ErrorKind::TimedOut,
                                        "write timed out",
                                    ));
                                }
                                blocked_deadline = None;
                            }
                        }
                    }
                    None => {
                        let _ = knet::race(
                            Box::pin(self.shared.write_notify.notified()),
                            self.shared.cancel_token.cancelled(),
                        )
                        .await;
                    }
                }
                continue;
            }
            offset += sent;
        }
        Ok(())
    }
}

impl Drop for KcpStream {
    fn drop(&mut self) {
        // Only the owner closes the connection.  Clones share the same
        // `Arc<SharedIoState>` but must not close it when dropped.
        if self.owns_connection {
            self.shared.close();
        }
    }
}

impl Clone for KcpStream {
    /// Clone shares the same background tasks, KCP state, and transport.
    /// The clone does NOT own the connection — dropping it will NOT close
    /// the connection.  Only the original `KcpStream`'s `Drop` closes it.
    fn clone(&self) -> Self {
        KcpStream {
            shared: self.shared.clone(),
            _handles: Vec::new(),
            owns_connection: false,
        }
    }
}

// ─── Builder ──────────────────────────────────────────────────────────────────

macro_rules! kcp_config_setters {
    () => {
        pub fn mtu(mut self, value: u32) -> Self {
            self.config.mtu = value;
            self
        }

        pub fn sndwnd(mut self, value: u32) -> Self {
            self.config.sndwnd = value;
            self
        }

        pub fn rcvwnd(mut self, value: u32) -> Self {
            self.config.rcvwnd = value;
            self
        }

        pub fn mode(mut self, value: crate::KcpMode) -> Self {
            self.config.mode = value;
            self
        }

        pub fn stream(mut self, value: bool) -> Self {
            self.config.stream = value;
            self
        }

        pub fn acknodelay(mut self, value: bool) -> Self {
            self.config.acknodelay = value;
            self
        }

        pub fn token(mut self, value: u32) -> Self {
            self.config.token = value;
            self
        }

        /// Raw KCP latency knobs `(nodelay, interval, resend, nc)`, switching
        /// the mode to [`KcpMode::Manual`]. Named apart from
        /// [`KcpStream::set_nodelay`], which is the bool fast-path toggle.
        pub fn set_kcp_nodelay(
            mut self,
            nodelay: u32,
            interval: u32,
            resend: u32,
            nc: u32,
        ) -> Self {
            self.config.mode = crate::KcpMode::Manual;
            self.config.nodelay = nodelay;
            self.config.interval = interval;
            self.config.resend = resend;
            self.config.nc = nc;
            self
        }

        /// Enable Reed-Solomon FEC (`datashard` / `parityshard`, both must be > 0).
        pub fn fec(mut self, datashard: u32, parityshard: u32) -> Self {
            self.config.datashard = datashard;
            self.config.parityshard = parityshard;
            self
        }

        pub fn config(mut self, config: KcpConfig) -> Self {
            self.config = config;
            self
        }
    };
}

/// `.conv()` setter for builders that must remember the caller pinned it
/// explicitly. `KcpStreamBuilder` uses the flagged arm so `build()` can tell
/// "never touched" (randomize, Go-parity) from "pinned to any value, even
/// [`DEFAULT_CONV`](crate::config::DEFAULT_CONV)" (honour it). `KcpListenerBuilder` has no
/// dial side and uses the plain arm.
macro_rules! conv_setter {
    () => {
        pub fn conv(mut self, value: u32) -> Self {
            self.config.conv = value;
            self
        }
    };
    ($flag:ident) => {
        pub fn conv(mut self, value: u32) -> Self {
            self.config.conv = value;
            self.$flag = true;
            self
        }
    };
}

// Shared with the listener builders (KcpListenerBuilder / KcpTcpListenerBuilder).
pub(crate) use conv_setter;
pub(crate) use kcp_config_setters;

/// Builder for [`KcpStream`]. Call [`.build().await`](Self::build) to construct.
pub struct KcpStreamBuilder {
    remote: Option<SocketAddr>,
    transport: Option<Arc<dyn PacketTransport>>,
    config: KcpConfig,
    connected: bool,
    resolve_err: Option<io::Error>,
    dial: DialTransport,
    adopt_conv: bool,
    /// Set by `.conv()` — pins the conv even when it equals
    /// [`DEFAULT_CONV`](crate::config::DEFAULT_CONV), suppressing the random-dial default.
    conv_explicit: bool,
    background_input: bool,
    spawn_flush_loop: bool,
    connect_timeout: Option<Duration>,
}

impl KcpStreamBuilder {
    kcp_config_setters!();
    conv_setter!(conv_explicit);

    /// Adopt `conv` from the first valid inbound KCP segment.
    ///
    /// Server listeners use this because Go kcptun clients choose the
    /// conversation ID; dialed client connections keep their configured ID —
    /// or, when `.conv()` was never called, a fresh random one (Go-parity
    /// default, see [`random_conv`](crate::random_conv)).
    pub fn adopt_conv(mut self, enabled: bool) -> Self {
        self.adopt_conv = enabled;
        self
    }

    /// Whether the transport is already `connect()`ed (use `send` / `send_batch`).
    ///
    /// Default: `true` for [`KcpStream::connect`], `false` for [`KcpStream::with_transport`].
    pub fn connected(mut self, v: bool) -> Self {
        self.connected = v;
        self
    }

    /// Spawn the background input-loop task (default `true`).
    ///
    /// Set to `false` when an external driver feeds inbound via
    /// [`KcpStream::feed_raw_batch`] (the listener worker pipeline), so
    /// the connection's tasks stay on the driver's runtime instead of being
    /// scheduled onto the process-wide executor.
    pub fn background_input(mut self, enabled: bool) -> Self {
        self.background_input = enabled;
        self
    }

    /// Whether `build()` should spawn the async flush loop (default `true`).
    ///
    /// Default `true`, which is what both clients and the listener workers use:
    /// the loop runs retransmit / delayed-ACK / window-probe deadlines and is
    /// woken by `flush_notify`. Set to `false` only for a caller that drives
    /// those deadlines itself (the in-crate unit tests do).
    pub fn spawn_flush_loop(mut self, enabled: bool) -> Self {
        self.spawn_flush_loop = enabled;
        self
    }

    /// Require the first conv-valid inbound packet (peer probe `WINS` / ACK)
    /// within `timeout`, failing [`build`](Self::build) with `TimedOut`
    /// otherwise. KCP has no handshake, so this is a **reachability + conv
    /// match** check, not a connection-established handshake: the dialing side
    /// forces a `WASK` probe immediately and waits for any valid response.
    ///
    /// Requires the default background input loop (`background_input(true)`).
    /// A dead peer always costs the full timeout (UDP has no RST-style fast
    /// failure). On timeout the connection is closed and dropped.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Construct the connection and start background input/flush loops.
    pub async fn build(self) -> io::Result<KcpStream> {
        if let Some(e) = self.resolve_err {
            return Err(e);
        }
        let remote = self.remote.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "KcpStream: remote address required",
            )
        })?;

        // Captured before `self.transport` is moved out below.
        let caller_supplied_transport = self.transport.is_some();
        let (transport, connected) = if let Some(t) = self.transport {
            (t, self.connected)
        } else {
            match self.dial {
                DialTransport::Udp => {
                    let bind = if remote.is_ipv4() {
                        SocketAddr::from(([0, 0, 0, 0], 0))
                    } else {
                        SocketAddr::from(([0u16; 8], 0))
                    };
                    let udp = knet::UdpSocket::connect(bind, remote)?;
                    let sock: Arc<dyn PacketTransport> = Arc::new(knet::DatagramSocket::Udp(udp));
                    (sock, true)
                }
                DialTransport::TcpRaw => {
                    let conn = knet::tcpraw_dial(&remote)?;
                    let sock: Arc<dyn PacketTransport> =
                        Arc::new(knet::DatagramSocket::TcpRaw(conn));
                    (sock, true)
                }
            }
        };

        let mut config = self.config;
        // Go kcp-go's Dial picks a fresh random conv per connection (sess.go
        // `DialWithOptions` → `binary.Read(rand.Reader, …)`), so when a peer
        // re-dials from a reused UDP source address the listener sees a conv
        // mismatch and *replaces* the lingering session instead of feeding the
        // new connection into the old state machine (which yields wrong-bytes
        // delivery or a stuck timeout — see bugs/BUGREPORT_KCP_LISTENER_
        // STALE_SESSION_ALIASING.md). Same rule here, scoped to what that
        // hazard actually needs: plain UDP dials (`transport.is_none()` &&
        // `DialTransport::Udp`), never a pinned `.conv()`, never an adopting
        // (server-side) build. `with_transport` callers own the transport
        // identity — TCP pairs have no address-reuse aliasing — so they keep
        // the configured conv; kcptun-common opts its UDP dial in explicitly.
        if !caller_supplied_transport
            && matches!(self.dial, DialTransport::Udp)
            && !self.adopt_conv
            && !self.conv_explicit
            && config.conv == crate::config::DEFAULT_CONV
        {
            config.conv = crate::config::random_conv();
        }
        let raw_packets = Arc::new(Mutex::new(RawPacketQueue::default()));
        let raw_packets_cb = raw_packets.clone();

        let mut kcp = KCP::new(config.conv, config.token, move |data: Bytes| {
            // `OutPkts` is counted on the wire batch in `flush_tx_batch`, not
            // here: this callback fires once per KCP segment, before FEC
            // expansion, so counting here reports segments and undercounts the
            // parity datagrams that actually go out.
            raw_packets_cb.lock().push(data);
        });
        kcp.apply(&config)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;
        let effective_snd_wnd = kcp.snd_wnd() as usize;

        // FEC (header_offset=0): crypto would wrap the whole FEC frame later.
        let fec_enabled = config.datashard > 0 && config.parityshard > 0;
        let (fec_encoder, fec_decoder) = if fec_enabled {
            let d = config.datashard as usize;
            let p = config.parityshard as usize;
            (
                FecEncoder::new(d, p, 0).map(Mutex::new),
                FecDecoder::new(d, p).map(Mutex::new),
            )
        } else {
            (None, None)
        };

        // When FEC is active, each KCP segment is wrapped in a 6+2 byte FEC
        // header on the wire (`[seq 4][type 2][size 2][kcp]`). If the KCP MTU
        // is not reduced by that overhead, a `--mtu 1350` session emits 1358-
        // byte datagrams — silently exceeding PPPoE/VPN MTU limits and
        // causing random packet loss. Go does `mtu -= fecHeaderSize` in
        // `sess.go`; we do the same here after `apply` so the MSS (and thus
        // the segment size KCP segments into) already accounts for FEC.
        if fec_enabled {
            let adj = config.mtu.saturating_sub(FEC_HEADER_SIZE_PLUS_2 as u32);
            if adj >= crate::segment::KCP_OVERHEAD as u32 {
                kcp.set_mtu(adj);
            }
        }

        let shared = Arc::new(SharedIoState {
            transport,
            kcp: Arc::new(Mutex::new(kcp)),
            read_buf: Mutex::new(ReadBuffer::default()),
            raw_packets,
            flush_notify: Arc::new(knet::Notify::new()),
            write_notify: Arc::new(knet::Notify::new()),
            read_notify: Arc::new(knet::Notify::new()),
            read_waker: Mutex::new(None),
            write_waker: Mutex::new(None),
            wait_send: Arc::new(AtomicUsize::new(0)),
            snd_wnd: AtomicUsize::new(effective_snd_wnd),
            acknodelay: AtomicBool::new(config.acknodelay),
            remote_addr: remote,
            connected,
            closed: Arc::new(AtomicBool::new(false)),
            cancel_token: CancellationToken::new(),
            adopt_conv: AtomicBool::new(self.adopt_conv),
            conv_mismatch: AtomicU64::new(0),
            peer_restart: AtomicU64::new(0),
            stale_bursts: AtomicU64::new(0),
            consecutive_stale_bursts: AtomicU64::new(0),
            first_stale_ms: AtomicU64::new(0),
            background_input: self.background_input,
            last_activity_ms: AtomicU64::new(knet::mono_ms()),
            last_rx_ms: AtomicU64::new(knet::mono_ms()),
            tx_pkts: AtomicU64::new(0),
            rx_pkts: AtomicU64::new(0),
            is_sending: AtomicBool::new(false),
            flush_ticks: AtomicU64::new(0),
            fec_encoder,
            fec_decoder,
            read_timeout: Mutex::new(None),
            write_timeout: Mutex::new(None),
            read_deadline: Mutex::new(None),
            write_deadline: Mutex::new(None),
            last_error: Mutex::new(None),
            nodelay: AtomicBool::new(
                config
                    .mode
                    .nodelay_params()
                    .map(|(n, ..)| n != 0)
                    .unwrap_or(config.nodelay != 0),
            ),
            write_closed: AtomicBool::new(false),
            read_closed: AtomicBool::new(false),
            first_inbound: AtomicBool::new(false),
            nonblocking: AtomicBool::new(false),
        });

        let mut handles = Vec::with_capacity(2);
        if shared.background_input {
            handles.push(spawn_input_loop(shared.clone()));
        }
        if self.spawn_flush_loop {
            handles.push(spawn_flush_loop(shared.clone()));
        }

        let conn = KcpStream {
            shared: shared.clone(),
            _handles: handles,
            owns_connection: true,
        };

        // Optional connect-timeout: force a `WASK` probe and wait for the first
        // conv-valid inbound (peer `WINS` / ACK) within the deadline. KCP has no
        // handshake, so this proves reachability + conv match, not connection
        // establishment.
        if let Some(t) = self.connect_timeout {
            if !shared.background_input {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "connect_timeout requires the background input loop (background_input(true))",
                ));
            }
            shared.kcp.lock().request_probe();
            shared.flush_notify.notify_one();
            match knet::timeout(t, async {
                loop {
                    if shared.first_inbound.load(Ordering::Acquire) {
                        return;
                    }
                    if shared.is_closed() {
                        return;
                    }
                    // Safety-net timeout recovers lost-wake races. Count the
                    // fallback fires so P999 spikes can be correlated against
                    // this path (plan Phase 3.1).
                    if knet::timeout(
                        Duration::from_millis(WAIT_FALLBACK_MS),
                        shared.read_notify.notified(),
                    )
                    .await
                    .is_err()
                    {
                        crate::snmp::add(&crate::snmp::DEFAULT_SNMP.read_fallback_timeout, 1);
                    }
                }
            })
            .await
            {
                Ok(()) => {}
                Err(_) => {
                    shared.close();
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!(
                            "KCP connect timeout: no response from {} within {}ms",
                            remote,
                            t.as_millis()
                        ),
                    ));
                }
            }
            if !shared.first_inbound.load(Ordering::Acquire) {
                shared.close();
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "KCP connection closed during connect",
                ));
            }
        }

        Ok(conn)
    }
}

/// `KcpStream::connect(addr).await` — awaitable without an explicit `.build()`.
impl std::future::IntoFuture for KcpStreamBuilder {
    type Output = io::Result<KcpStream>;
    type IntoFuture = Pin<Box<dyn std::future::Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.build())
    }
}

pub(crate) fn resolve_one(addr: impl ToSocketAddrs) -> io::Result<SocketAddr> {
    addr.to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "could not resolve address"))
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "async"))]
mod integ;
