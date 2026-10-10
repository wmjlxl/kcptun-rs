//! Core KCP state machine.
//!
//! This module implements the KCP ARQ protocol state machine — the heart of
//! the reliable transport. It manages send/receive buffers, retransmission
//! timers, congestion control, and window management.
//!
//! ## One-to-one correspondence with Go kcp-go v5
//!
//! This implementation is a port of Go's `github.com/xtaci/kcp-go/v5/kcp.go`
//! and aims for **wire-level protocol compatibility** — packets produced by
//! this Rust implementation must be decodable by the Go implementation and
//! vice versa.

use std::cmp;
use std::collections::VecDeque;
use std::fmt;

use bytes::{Buf, Bytes, BytesMut};
use smallvec::SmallVec;

use crate::segment::SegmentPool;
use crate::segment::{
    Command, Segment, KCP_ASK_SEND, KCP_ASK_TELL, KCP_DEFAULT_WND, KCP_MAX_FRAG, KCP_OVERHEAD,
    KCP_THRESHOLD_INIT, MTU,
};
use crate::snmp::{self, DEFAULT_SNMP};

// ─── Constants (matching Go kcp-go v5) ─────────────────────────────────────

/// No delay min RTO (enabled by NoDelay).  Matches Go `IKCP_RTO_NDL`.
const RTO_NODELAY_MIN: u32 = 30;
/// Normal min RTO.  Matches Go `IKCP_RTO_MIN`.
const RTO_DEFAULT_MIN: u32 = 100;
/// Default RTO (200ms).  Matches Go `IKCP_RTO_DEF`.
const RTO_DEFAULT: u32 = 200;
/// Max RTO cap.  Matches Go `IKCP_RTO_MAX`.
const RTO_MAX: u32 = 60000;

/// The first probe interval (500ms, matching the Rust port's prior default).
/// kcp-go uses 7000ms — both are far slower than a tunnel wants to recover a
/// closed peer window under burst load.
pub(crate) const PROBE_INIT: u32 = 500;
/// Probe interval used in nodelay modes (P3). When the peer window closes
/// mid-burst, 500ms between WAsk probes stalls recovery; 50ms cuts the
/// window-reopen latency ~10× (measured 947ms→68ms on 256KB@RPS=250).
pub(crate) const PROBE_INIT_NODELAY: u32 = 50;
/// Max probe interval (120s, matching Go)
pub const IKCP_PROBE_LIMIT: u32 = 120000;
/// After this many retransmits, mark the connection as dead.
/// Matches Go `IKCP_DEADLINK`.
const DEAD_LINK_RETRIES: u32 = 20;

/// State value indicating a dead connection (matching Go kcp-go `dead_link`).
const DEAD_LINK_STATE: u32 = 0xFFFF_FFFF;

/// Slow-start threshold minimum.  Matches Go `IKCP_THRESH_MIN`.
const THRESH_MIN: u32 = 2;

/// A queued ACK entry: sequence number + timestamp of the received segment.
#[derive(Clone, Copy)]
struct AckEntry {
    sn: u32,
    ts: u32,
}

// ─── KCP State Machine ─────────────────────────────────────────────────────

/// The primary KCP protocol state machine.
pub struct KCP {
    // ── Conversation identity ──
    conv: u32,
    /// Authentication token (matching Go kcp-go).
    pub token: u32,
    state: u32, // 0xFFFFFFFF = dead (matching Go dead_link)

    // ── Send side ──
    snd_queue: VecDeque<Segment>,
    snd_buf: VecDeque<Segment>,
    snd_nxt: u32,
    snd_una: u32,

    // ── Receive side ──
    rcv_queue: VecDeque<Segment>,
    rcv_buf: VecDeque<Segment>,
    rcv_nxt: u32,

    // ── Windows ──
    snd_wnd: u32,
    rcv_wnd: u32,
    rmt_wnd: u32,
    cwnd: u32,
    ssthresh: u32,

    // ── MSS / MTU ──
    mss: u32,
    mtu: u32,

    // ── RTT estimation ──
    rx_srtt: i32,   // Go uses int32 for signed delta
    rx_rttvar: i32, // Go uses int32 for signed delta
    rx_rto: u32,
    rx_minrto: u32,

    // ── Timers ──
    interval: u32,
    ts_flush: u32,
    updated: u32, // whether first Update() has been called (matching Go)

    // ── Options ──
    nodelay: u32,
    fastresend: i32,
    nocwnd: i32,
    stream: i32,

    // ── Window probe ──
    probe: u32,
    probe_wait: u32,
    ts_probe: u32,
    /// Set when [`input_no_flush`] saw something that needs a flush (UNA
    /// advance / ACK queue fill / ack_no_delay ACKs). Deferred so a batch of
    /// input datagrams triggers ONE `flush_with_current` instead of one per
    /// segment (each full flush iterates the whole snd_buf).
    pending_flush: u32,

    // ── Congestion control ──
    dead_link: u32,
    incr: u32,

    // ── ACK list ──
    /// Most flush cycles accumulate ≤16 ACKs; SmallVec avoids heap alloc on
    /// the common path while spilling to the heap for large bursts.
    acklist: SmallVec<[AckEntry; 16]>,

    // ── Output buffer ──
    buffer: BytesMut,

    // ── Callbacks ──
    output: Box<dyn FnMut(Bytes) + Send>,

    // ── Segment pool ──
    pool: SegmentPool,

    // ── Reusable ACK segment (avoids pool acquire/release per flush) ──
    /// A reusable segment for ACK/WASK/WINS headers during flush.
    /// This avoids the overhead of pool.acquire()/release() on every flush cycle.
    /// The segment's data buffer is not used (ACKs have no payload), so we only
    /// need to reset the header fields before each use.
    ack_seg: Segment,
}

impl fmt::Debug for KCP {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KCP")
            .field("conv", &self.conv)
            .field("state", &self.state)
            .field("snd_una", &self.snd_una)
            .field("snd_nxt", &self.snd_nxt)
            .field("rcv_nxt", &self.rcv_nxt)
            .field("snd_wnd", &self.snd_wnd)
            .field("rcv_wnd", &self.rcv_wnd)
            .field("rmt_wnd", &self.rmt_wnd)
            .field("cwnd", &self.cwnd)
            .field("ssthresh", &self.ssthresh)
            .field("rx_rto", &self.rx_rto)
            .field("interval", &self.interval)
            .field("snd_queue.len", &self.snd_queue.len())
            .field("snd_buf.len", &self.snd_buf.len())
            .field("rcv_buf.len", &self.rcv_buf.len())
            .field("rcv_queue.len", &self.rcv_queue.len())
            .finish()
    }
}

/// Signed difference for wrapping-safe sequence number comparisons.
/// Matches the original KCP C function `itimediff(later, earlier)`.
///
/// Accepts any integer widening to `u64` (both `u32` KCP fields and `u64`
/// timestamps from `current_ms()`) without explicit casts at call sites.
/// The result is `i32` — KCP wire timestamps are 32-bit and `itimediff`
/// only cares about the low 32 bits of the difference.
#[inline]
fn itimediff(later: impl Into<u64>, earlier: impl Into<u64>) -> i32 {
    later.into().wrapping_sub(earlier.into()) as i32
}

/// Wall-clock ms since UNIX_EPOCH via a process-wide cached clock: one
/// `SystemTime::now()` at first use, then `Instant::elapsed()` per call —
/// removes the epoch-conversion syscall from the per-packet paths. Monotonic by
/// construction (ignores wall-clock adjustments), which KCP/FEC timing only
/// cares about via 32-bit `itimediff` gaps.
/// pprof evidence: `Timespec::now` was 6.52% of CPU (1489ms/20s) from ~2000
/// `SystemTime::now()` calls/sec across flush_data_only + input_no_flush +
/// flush_input_batch + send_to_kcp paths (and one call per FEC encode before
/// the clock was shared).
#[inline]
pub(crate) fn wall_ms() -> u64 {
    use std::sync::OnceLock;
    struct ClockOffset {
        epoch_ms: u64,
        instant: std::time::Instant,
    }
    static OFFSET: OnceLock<ClockOffset> = OnceLock::new();
    let offset = OFFSET.get_or_init(|| ClockOffset {
        epoch_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
        instant: std::time::Instant::now(),
    });
    offset.epoch_ms + offset.instant.elapsed().as_millis() as u64
}

/// Result of [`KCP::flush_data_segs`]: retransmission counters (consumed by
/// SNMP accounting and congestion control) plus the nearest RTO-derived
/// deadline for the next update tick.
struct FlushDataStats {
    /// Next update deadline: the flush interval or the nearest pending RTO.
    next_update: u32,
    /// Segments that hit fast or early retransmit (RFC 6937 rate halving).
    change: u64,
    /// Segments that hit RTO timeout (RFC 5681 backoff).
    lost_segs: u64,
    /// Segments sent by fast retransmit this round.
    fast_retrans_segs: u64,
    /// Segments sent by early retransmit this round.
    early_retrans_segs: u64,
    /// Total segments written to the wire this round.
    out_segs: u64,
}

impl KCP {
    /// Create a new KCP instance.
    ///
    /// `conv` is the conversation ID, `token` is an additional identifier, and
    /// `output` is called whenever the KCP instance has bytes to send over the
    /// wire.  The callback is boxed internally — callers can pass closures or
    /// function pointers without manual `Box::new`.
    pub fn new<F: FnMut(Bytes) + Send + 'static>(conv: u32, token: u32, output: F) -> Self {
        let mtu = MTU as u32;
        KCP {
            conv,
            token,
            state: 0,
            // High-concurrency servers may hold thousands of mostly idle KCP
            // states. Allocate segment slots on first traffic instead of
            // reserving four 128-entry Segment arrays for every connection.
            snd_queue: VecDeque::new(),
            snd_buf: VecDeque::new(),
            snd_nxt: 0,
            snd_una: 0,
            rcv_queue: VecDeque::new(),
            rcv_buf: VecDeque::new(),
            rcv_nxt: 0,
            snd_wnd: KCP_DEFAULT_WND,
            rcv_wnd: KCP_DEFAULT_WND,
            rmt_wnd: KCP_DEFAULT_WND,
            cwnd: KCP_DEFAULT_WND, // Diverges from both references: Go `NewKCP` and C
            // `ikcp_create` (kcp/ikcp.c:250) both start at 0,
            // i.e. slow start from 1 segment. Only observable
            // with `--nc 0` (nocwnd == 0); every kcptun `--mode`
            // preset uses nc=1, where cwnd is ignored. Changing
            // it alters `--nc 0` startup and needs a bench.
            ssthresh: KCP_THRESHOLD_INIT,
            mss: mtu.saturating_sub(KCP_OVERHEAD as u32),
            mtu,
            rx_srtt: 0,
            rx_rttvar: 0,
            rx_rto: RTO_DEFAULT,
            rx_minrto: RTO_DEFAULT_MIN,
            interval: 100,
            ts_flush: 100, // Go initializes ts_flush = IKCP_INTERVAL = 100
            updated: 0,
            nodelay: 0,
            fastresend: 0,
            nocwnd: 0,
            stream: 0,
            probe: 0,
            probe_wait: 0,
            ts_probe: 0,
            pending_flush: 0,
            dead_link: DEAD_LINK_RETRIES,
            incr: 0,
            acklist: SmallVec::new(),
            buffer: BytesMut::with_capacity(MTU),
            output: Box::new(output),
            pool: SegmentPool::new(4096),
            // Pre-allocate ACK segment for flush path (no data buffer needed)
            ack_seg: Segment::with_capacity(0),
        }
    }

    /// Compute the unused window (wnd_unused in Go).
    fn wnd_unused(&self) -> u16 {
        let queued = self.rcv_queue.len() as u32;
        if queued < self.rcv_wnd {
            (self.rcv_wnd - queued) as u16
        } else {
            0
        }
    }

    // ── Configuration ─────────────────────────────────────────────────────

    /// Set the conversation ID. Used by a server session to adopt a client's
    /// (random) conv from the first inbound packet — matching Go kcp-go's
    /// server, which keys each accepted session by the first packet's conv.
    pub fn set_conv(&mut self, conv: u32) {
        self.conv = conv;
    }

    /// Set the MTU (Maximum Transmission Unit).
    /// Returns true if successful.
    pub fn set_mtu(&mut self, mtu: u32) -> bool {
        if mtu < 50 || mtu < KCP_OVERHEAD as u32 {
            return false;
        }
        self.mtu = mtu;
        self.mss = mtu - KCP_OVERHEAD as u32;
        self.buffer = BytesMut::with_capacity(self.mtu as usize);
        true
    }

    /// Get the current MTU.
    #[inline]
    pub fn mtu(&self) -> u32 {
        self.mtu
    }

    /// Set the send window size.
    #[inline]
    pub fn set_snd_wnd(&mut self, wnd: u32) {
        // Go: no clamping, just assign if > 0
        if wnd > 0 {
            self.snd_wnd = wnd;
        }
    }

    /// Set the receive window size.
    ///
    /// P2 / L-2: clamped to `u16::MAX` so a huge value cannot blow up
    /// `wnd_unused` bookkeeping. `0` is ignored (keeps the current value).
    #[inline]
    pub fn set_rcv_wnd(&mut self, wnd: u32) {
        if wnd > 0 {
            self.rcv_wnd = wnd.min(u16::MAX as u32);
        }
    }

    /// Set nodelay parameters (matches the Go kcp-go `NoDelay` API).
    ///
    /// - `nodelay`: 0 = default, 1 = enable nodelay
    /// - `interval`: internal update interval in milliseconds
    /// - `resend`: fast retransmit threshold (0 = disabled)
    /// - `nc`: no congestion control (0 = default, 1 = off)
    ///
    /// All four knobs are assigned unconditionally, as in Go (whose `>= 0`
    /// guards are vacuous for unsigned parameters). Gating the assignment on
    /// a non-zero value turns each knob into a one-way latch: switching from
    /// `Fast3` back to `Normal` would leave `nodelay` set (half-linear RTO
    /// backoff, 50 ms probe init), and `resend = 0` / `nc = 0` could never
    /// re-disable fast retransmit / re-enable congestion control.
    pub fn set_nodelay(&mut self, nodelay: u32, interval: u32, resend: u32, nc: u32) {
        self.nodelay = nodelay;
        if nodelay != 0 {
            self.rx_minrto = RTO_NODELAY_MIN; // Go: nodelay → rx_minrto = 30
        } else {
            self.rx_minrto = RTO_DEFAULT_MIN; // Go: no nodelay → rx_minrto = 100
        }

        let interval = interval as i32;
        if interval > 5000 {
            self.interval = 5000;
        } else if interval < 10 {
            self.interval = 10;
        } else {
            self.interval = interval as u32;
        }

        self.fastresend = resend as i32;
        self.nocwnd = nc as i32;
    }

    /// Enable or disable stream mode.
    #[inline]
    pub fn set_stream_mode(&mut self, enable: bool) {
        self.stream = if enable { 1 } else { 0 };
    }

    /// Ask KCP to emit a window-size probe (`WASK`, cmd 83) on the next flush.
    ///
    /// A live peer replies with `WINS` (cmd 84), so a dialing side can observe
    /// that response to confirm reachability + conv match without sending user
    /// data (used by [`crate::KcpStream`]'s connect-timeout first-packet wait).
    #[inline]
    pub fn request_probe(&mut self) {
        self.probe |= KCP_ASK_SEND;
    }

    // ── User API ──────────────────────────────────────────────────────────

    /// Send data through the KCP connection.
    ///
    /// This queues the data for transmission. SN is assigned during flush()
    /// when the segment is moved to snd_buf, matching Go behavior.
    pub fn send(&mut self, data: &[u8]) -> Result<(), KcpError> {
        // P2 / L-3: refuse absurd payloads instead of silently truncating
        // `len` to u32. A >4 GiB send is a caller bug.
        if data.len() > u32::MAX as usize {
            return Err(KcpError::InvalidConfig(format!(
                "send of {} bytes exceeds u32::MAX",
                data.len()
            )));
        }
        if data.is_empty() {
            return Err(KcpError::NoData);
        }

        let original_len = data.len() as u64;

        // Stream mode: append to previous segment in snd_queue if possible
        let data = if self.stream != 0 {
            if let Some(last) = self.snd_queue.back_mut() {
                if (last.data.len() as u32) < self.mss {
                    let capacity = self.mss as usize - last.data.len();
                    let extend = cmp::min(data.len(), capacity);
                    last.data.extend_from_slice(&data[..extend]);
                    last.len = last.data.len() as u32;
                    if extend >= data.len() {
                        snmp::add(&DEFAULT_SNMP.bytes_sent, original_len);
                        return Ok(());
                    }
                    // Advance past appended bytes (matching Go: buffer = buffer[extend:])
                    &data[extend..]
                } else {
                    data
                }
            } else {
                data
            }
        } else {
            data
        };

        // Calculate fragment count
        let count = if data.len() as u32 <= self.mss {
            1
        } else {
            (data.len() as u32).div_ceil(self.mss)
        };

        if count > KCP_MAX_FRAG {
            return Err(KcpError::TooManyFragments);
        }

        let mut offset = 0usize;
        for i in 0..count {
            let mut seg = self.pool.acquire();
            let size = cmp::min(data.len() - offset, self.mss as usize);
            seg.conv = self.conv;
            seg.cmd = Command::Push as u8;
            if self.stream == 0 {
                seg.frg = (count - 1 - i) as u8;
            } else {
                seg.frg = 0;
            }
            seg.data.extend_from_slice(&data[offset..offset + size]);
            seg.len = size as u32;
            // Note: payload freezing is deferred to flush() — see the freeze
            // step before encode() in the snd_buf transmit loop.  Freezing
            // here would empty `data` and break stream-mode append on the
            // next send() call (capacity check uses data.len()).
            self.snd_queue.push_back(seg);
            offset += size;
        }

        // Upper-level bytes accepted for send (Go UDPSession.WriteBuffers).
        snmp::add(&DEFAULT_SNMP.bytes_sent, original_len);

        Ok(())
    }

    /// Receive data from the KCP connection. Returns `None` if no data is
    /// available.
    pub fn recv(&mut self) -> Result<BytesMut, KcpError> {
        let peeksize = self.peeksize().ok_or(KcpError::NoData)?;

        let fast_recover = self.rcv_queue.len() as u32 >= self.rcv_wnd;

        // Merge fragments
        let mut data = BytesMut::with_capacity(peeksize);
        loop {
            match self.rcv_queue.pop_front() {
                Some(seg) => {
                    let size = cmp::min(seg.len as usize, seg.data.len());
                    if size > 0 {
                        data.extend_from_slice(&seg.data[..size]);
                    }
                    self.pool.release(seg);
                    if data.len() >= peeksize {
                        break;
                    }
                }
                None => break,
            }
        }

        // Move available data from rcv_buf → rcv_queue
        self.move_receive_buffer();

        // Fast recover
        if (self.rcv_queue.len() as u32) < self.rcv_wnd && fast_recover {
            self.probe |= KCP_ASK_TELL;
        }

        snmp::add(&DEFAULT_SNMP.bytes_received, data.len() as u64);

        Ok(data)
    }

    /// Receive data as `Bytes` (zero-copy when single-segment).
    ///
    /// When the message fits in a single KCP segment (frg == 0, the
    /// common case in stream mode), this returns a reference-counted
    /// `Bytes` slice of the segment's data — no `extend_from_slice`
    /// copy is performed.
    ///
    /// When the message spans multiple segments, they are merged via
    /// `extend_from_slice` (same as `recv()`).
    pub fn recv_bytes(&mut self) -> Result<bytes::Bytes, KcpError> {
        let peeksize = self.peeksize().ok_or(KcpError::NoData)?;

        let fast_recover = self.rcv_queue.len() as u32 >= self.rcv_wnd;

        // ── Fast path: single-segment message (frg == 0) ──
        // This is the common case in stream mode. Zero-copy: just
        // freeze the segment's BytesMut data and return a slice.
        if peeksize == self.rcv_queue.front().map(|s| s.len as usize).unwrap_or(0) {
            if let Some(mut seg) = self.rcv_queue.pop_front() {
                let size = cmp::min(seg.len as usize, seg.data.len());
                let data = seg.data.split_to(size).freeze();
                self.pool.release(seg);

                // Move available data from rcv_buf → rcv_queue
                self.move_receive_buffer();

                // Fast recover
                if (self.rcv_queue.len() as u32) < self.rcv_wnd && fast_recover {
                    self.probe |= KCP_ASK_TELL;
                }

                snmp::add(&DEFAULT_SNMP.bytes_received, data.len() as u64);

                return Ok(data);
            }
        }

        // ── Slow path: multi-segment message ──
        let mut data = BytesMut::with_capacity(peeksize);
        loop {
            match self.rcv_queue.pop_front() {
                Some(seg) => {
                    let size = cmp::min(seg.len as usize, seg.data.len());
                    if size > 0 {
                        data.extend_from_slice(&seg.data[..size]);
                    }
                    self.pool.release(seg);
                    if data.len() >= peeksize {
                        break;
                    }
                }
                None => break,
            }
        }

        // Move available data from rcv_buf → rcv_queue
        self.move_receive_buffer();

        // Fast recover
        if (self.rcv_queue.len() as u32) < self.rcv_wnd && fast_recover {
            self.probe |= KCP_ASK_TELL;
        }

        let out = data.freeze();
        snmp::add(&DEFAULT_SNMP.bytes_received, out.len() as u64);
        Ok(out)
    }

    /// Check the size of the next message in the receive queue.
    /// Returns `None` if no complete message is available.
    pub fn peeksize(&self) -> Option<usize> {
        let front = self.rcv_queue.front()?;
        if front.frg == 0 {
            return Some(front.len as usize);
        }

        let count = self.rcv_queue.len();
        // A fragmented message needs at least `frg + 1` segments.  Keep the
        // queue length as `usize`; narrowing it to u8 made a full 256-entry
        // queue look empty when the fragment count was 255.
        if count <= front.frg as usize {
            return None;
        }

        let mut length = 0usize;
        for seg in &self.rcv_queue {
            length = length.checked_add(seg.len as usize)?;
            if seg.frg == 0 {
                break;
            }
        }
        Some(length)
    }

    // ── Ack processing ────────────────────────────────────────────────────

    fn parse_ack(&mut self, sn: u32) {
        if itimediff(sn, self.snd_una) < 0 || itimediff(sn, self.snd_nxt) >= 0 {
            return;
        }

        // snd_buf is contiguous from snd_una, including across u32 wrap.  Use
        // the wrapping sequence delta as the deque index, then verify the SN
        // at that index so malformed/gapped state cannot ACK a neighbour.
        let index = sn.wrapping_sub(self.snd_una) as usize;
        let Some(seg) = self.snd_buf.get_mut(index) else {
            return;
        };
        if seg.sn == sn {
            seg.acked = true;
            // The flush loop skips acked segments (`if seg.acked { continue }`),
            // so the payload is dead weight until `parse_una` eventually pops
            // the segment from the front of `snd_buf`. With `snd_wnd = 1024`
            // that can be ~1.4 MB per connection of payloads already accepted
            // by the peer but still pinned in memory.
            seg.data.clear();
            seg.len = 0;
            // Go: recycleSegment frees the data buffer but leaves the entry
            // so we don't need to remove from snd_buf here.
        }
    }

    fn parse_fastack(&mut self, sn: u32, ts: u32) -> bool {
        // Fast retransmit disabled: with `fastresend <= 0` the flush loop sets
        // `resent = 0xFFFFFFFF`, so no fastack count can ever trigger a
        // retransmit — skip the O(inflight) snd_buf scan entirely (every
        // inbound ACK would otherwise walk the deque for nothing).
        if self.fastresend <= 0 {
            return false;
        }
        if itimediff(sn, self.snd_una) < 0 || itimediff(sn, self.snd_nxt) >= 0 {
            return false;
        }

        let mut should_fast_ack = false;
        for seg in &mut self.snd_buf {
            if itimediff(sn, seg.sn) < 0 {
                break;
            } else if sn != seg.sn && itimediff(seg.ts, ts) <= 0 {
                if seg.fastack != 0xFFFFFFFF {
                    seg.fastack += 1;
                    if seg.fastack >= self.fastresend as u32 {
                        should_fast_ack = true;
                    }
                }
            }
        }

        should_fast_ack
    }

    fn parse_una(&mut self, una: u32) -> usize {
        // snd_buf is ordered by SN.  Pop from the front while UNA advances;
        // this performs one scan and releases in the same front-to-back order
        // as Go's shrink_buf path.
        let mut count = 0;
        loop {
            let remove = self
                .snd_buf
                .front()
                .is_some_and(|seg| itimediff(una, seg.sn) > 0);
            if !remove {
                break;
            }
            if let Some(seg) = self.snd_buf.pop_front() {
                self.pool.release(seg);
                count += 1;
            }
        }
        count
    }

    fn shrink_buf(&mut self) {
        if let Some(seg) = self.snd_buf.front() {
            self.snd_una = seg.sn;
        } else {
            self.snd_una = self.snd_nxt;
        }
    }

    fn ack_push(&mut self, sn: u32, ts: u32) {
        self.acklist.push(AckEntry { sn, ts });
    }

    // ── Input processing ─────────────────────────────────────────────────

    /// Feed incoming raw data into the KCP state machine.
    ///
    /// Process one inbound datagram **without** triggering a flush.
    ///
    /// `ack_no_delay` matches Go's `ackNoDelay` parameter — when true, ACKs
    /// are requested immediately, but the actual `flush_with_current` is
    /// deferred: the caller batches many datagrams (each typically advancing
    /// `una` and thus marking `flush_segments`) and calls
    /// [`flush_if_pending`](Self::flush_if_pending) once. A per-segment flush
    /// iterates the whole `snd_buf` (~O(500)), so at 50k+ pkt/s that alone is
    /// ~30M snd_buf iterations/s — the dominant per-segment cost.
    ///
    /// Datagrams passed here are treated as *regular* (received from the
    /// network). FEC-reconstructed datagrams must go through
    /// [`input_no_flush_typed`](Self::input_no_flush_typed) with
    /// `regular = false`.
    pub fn input_no_flush(&mut self, data: &[u8], ack_no_delay: bool) -> Result<usize, KcpError> {
        self.input_no_flush_typed(data, true, ack_no_delay)
    }

    /// [`input_no_flush`](Self::input_no_flush) with Go's `pktType` /
    /// `regular` distinction.
    ///
    /// `regular = false` marks a datagram that was reconstructed by
    /// Reed-Solomon rather than received from the peer. Such a datagram
    /// carries a stale window advertisement and a stale `ts`, so Go
    /// (`kcp.go` `Input`) skips three things for it: the `rmt_wnd` update,
    /// the RTT sample, and the `RepeatSegs` counter. Feeding recovered
    /// packets as regular lets an old window overwrite a fresher one and
    /// folds the FEC group-fill delay into `rx_srtt`, inflating the RTO on
    /// exactly the lossy links FEC exists for.
    pub fn input_no_flush_typed(
        &mut self,
        data: &[u8],
        regular: bool,
        ack_no_delay: bool,
    ) -> Result<usize, KcpError> {
        let snd_una = self.snd_una;
        let mut offset = 0usize;
        let mut latest_ts = 0u32;
        let mut update_rtt = false;
        let mut in_segs = 0u64;
        let mut flush_segments = 0u32;

        while data.len().saturating_sub(offset) >= KCP_OVERHEAD {
            // Decode 24B header from stack slice (P2.1: no alloc; single copy of header words).
            let hdr = &data[offset..offset + KCP_OVERHEAD];
            let conv = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
            let cmd = hdr[4];
            let frg = hdr[5];
            let wnd = u16::from_le_bytes([hdr[6], hdr[7]]);
            let ts = u32::from_le_bytes([hdr[8], hdr[9], hdr[10], hdr[11]]);
            let sn = u32::from_le_bytes([hdr[12], hdr[13], hdr[14], hdr[15]]);
            let una = u32::from_le_bytes([hdr[16], hdr[17], hdr[18], hdr[19]]);
            let length = u32::from_le_bytes([hdr[20], hdr[21], hdr[22], hdr[23]]) as usize;

            // Validate conv FIRST (matching Go: check conv before length)
            if conv != self.conv {
                return Err(KcpError::ConvMismatch {
                    expected: self.conv,
                    got: conv,
                });
            }

            let total_len = match offset
                .checked_add(KCP_OVERHEAD)
                .and_then(|header_end| header_end.checked_add(length))
            {
                Some(total_len) if total_len <= data.len() => total_len,
                _ => return Err(KcpError::InvalidLength),
            };

            // Validate command
            if cmd != Command::Push as u8
                && cmd != Command::Ack as u8
                && cmd != Command::WAsk as u8
                && cmd != Command::WIns as u8
            {
                return Err(KcpError::UnknownCommand(cmd));
            }

            // Only trust window updates from regular packets — a
            // FEC-recovered datagram carries a stale advertisement
            // (Go: `if pktType == IKCP_PACKET_REGULAR`).
            if regular {
                self.rmt_wnd = wnd as u32;
            }

            // Process UNA (matching Go: parse_una → shrink_buf)
            if self.parse_una(una) > 0 {
                flush_segments |= 1;
            }
            self.shrink_buf();

            match cmd {
                c if c == Command::Ack as u8 => {
                    self.parse_ack(sn);
                    if self.parse_fastack(sn, ts) {
                        flush_segments |= 2;
                    }
                    update_rtt = true;
                    latest_ts = ts;
                }
                c if c == Command::Push as u8 => {
                    // Window check: only ack if within window
                    if itimediff(sn, self.rcv_nxt.wrapping_add(self.rcv_wnd)) < 0 {
                        self.ack_push(sn, ts);
                        // Go keeps `repeat` true unless `parse_data` rejects the
                        // segment, so a duplicate below `rcv_nxt` — the common
                        // case for a retransmission that arrives after the
                        // original was delivered — is still counted. Counting
                        // only the in-window branch left RepeatSegs blind to
                        // exactly the retransmissions it exists to measure.
                        let mut repeat = true;
                        if itimediff(sn, self.rcv_nxt) >= 0 {
                            // Create segment from received data
                            let mut seg = self.pool.acquire();
                            seg.conv = conv;
                            seg.cmd = cmd;
                            seg.frg = frg;
                            seg.wnd = wnd;
                            seg.ts = ts;
                            seg.sn = sn;
                            seg.una = una;
                            seg.len = length as u32;
                            if length > 0 {
                                seg.data
                                    .extend_from_slice(&data[offset + KCP_OVERHEAD..total_len]);
                            }

                            // Insert into receive buffer (matching Go parse_data).
                            repeat = self.parse_data(seg);
                        }
                        // Go increments RepeatSegs for regular packets only.
                        if repeat && regular {
                            snmp::add(&DEFAULT_SNMP.repeat_segs, 1);
                        }
                    }
                }
                c if c == Command::WAsk as u8 => {
                    // Ready to send back WIns in flush
                    self.probe |= KCP_ASK_TELL;
                }
                c if c == Command::WIns as u8 => {
                    // Window update received, nothing to do
                }
                _ => {}
            }

            in_segs += 1;
            offset = total_len;
        }

        // Update SNMP
        if in_segs > 0 {
            snmp::add(&DEFAULT_SNMP.in_segs, in_segs);
        }

        // Update RTT with the latest ts (matching Go: only for regular
        // packets — a recovered packet's `ts` predates the FEC group fill).
        // Fetch the current timestamp once and reuse it for flush triggers below.
        let now_ms = self.current_ms();
        if update_rtt && regular && itimediff(now_ms, latest_ts) >= 0 {
            self.update_ack(itimediff(now_ms, latest_ts));
        }

        // Congestion window update (matching Go: nocwnd check)
        if self.nocwnd == 0 {
            if itimediff(self.snd_una, snd_una) > 0 {
                if self.cwnd < self.rmt_wnd {
                    let mss = self.mss;
                    if self.cwnd < self.ssthresh {
                        self.cwnd += 1;
                        self.incr += mss;
                    } else {
                        if self.incr < mss {
                            self.incr = mss;
                        }
                        self.incr += (mss * mss) / self.incr + (mss / 16);
                        if (self.cwnd + 1) * mss <= self.incr {
                            if mss > 0 {
                                self.cwnd = self.incr.div_ceil(mss);
                            } else {
                                self.cwnd = self.incr + mss - 1;
                            }
                        }
                    }
                    if self.cwnd > self.rmt_wnd {
                        self.cwnd = self.rmt_wnd;
                        self.incr = self.rmt_wnd * mss;
                    }
                }
            }
        }

        // Deferred flush triggers (matching Go's intent, but batched): record
        // what needs flushing; `flush_if_pending()` performs ONE
        // `flush_with_current` for the whole batch instead of one per segment.
        if flush_segments != 0 {
            self.pending_flush |= 1;
        } else if self.acklist.len() >= (self.mtu / KCP_OVERHEAD as u32) as usize {
            self.pending_flush |= 1;
        } else if ack_no_delay && !self.acklist.is_empty() {
            self.pending_flush |= 1;
        }

        Ok(offset)
    }

    /// Run one `flush_with_current` if any [`input_no_flush`](Self::input_no_flush)
    /// since the last flush requested it. Returns the next update interval
    /// (`flush_with_current`'s value, or `self.interval` when nothing pending).
    pub fn flush_if_pending(&mut self, current: u32) -> u32 {
        if self.pending_flush != 0 {
            self.pending_flush = 0;
            self.flush_with_current(current, true)
        } else {
            self.interval
        }
    }

    /// Emit queued ACKs and nothing else.
    ///
    /// [`flush_with_current`](Self::flush_with_current) also walks the whole send
    /// buffer for retransmissions. An ACK-only burst does not need that: one
    /// datagram holds `mtu / 24` ACKs (about 56 at the default MTU) and costs a
    /// single `sendmmsg`, around a microsecond. Holding a short `acklist` for
    /// the 10ms maintenance tick instead costs 10ms per round trip, which is
    /// what a one-packet exchange measures.
    ///
    /// A full datagram's worth of ACKs is already flushed by
    /// [`flush_if_pending`](Self::flush_if_pending) (the input path sets
    /// `pending_flush` once `acklist` reaches `mtu / 24`). This sends whatever
    /// is left over, so a burst that did not fill a datagram still leaves
    /// immediately.
    pub fn flush_acks_only(&mut self) {
        if self.acklist.is_empty() {
            return;
        }
        let mtu = self.mtu as usize;
        let rcv_nxt = self.rcv_nxt;
        let acks: Vec<(u32, u32)> = self
            .acklist
            .iter()
            .enumerate()
            .filter(|(i, entry)| itimediff(entry.sn, rcv_nxt) >= 0 || *i + 1 == self.acklist.len())
            .map(|(_, entry)| (entry.sn, entry.ts))
            .collect();
        self.acklist.clear();

        self.ack_seg.conv = self.conv;
        self.ack_seg.cmd = Command::Ack as u8;
        self.ack_seg.wnd = self.wnd_unused();
        self.ack_seg.una = rcv_nxt;
        self.ack_seg.len = 0;
        for (sn, ts) in acks {
            if self.buffer.len() + crate::segment::KCP_OVERHEAD > mtu && !self.buffer.is_empty() {
                let data = self.buffer.copy_to_bytes(self.buffer.len());
                (self.output)(data);
                if self.buffer.capacity() < mtu {
                    self.buffer.reserve(mtu);
                }
            }
            self.ack_seg.sn = sn;
            self.ack_seg.ts = ts;
            Segment::encode(&self.ack_seg, &mut self.buffer);
        }
        if !self.buffer.is_empty() {
            let data = self.buffer.copy_to_bytes(self.buffer.len());
            (self.output)(data);
        }
    }

    /// Backward-compatible single-datagram input: process then flush
    /// immediately (matches Go `kcp.Input` + `flush(true)` semantics for
    /// callers that don't batch).
    pub fn input(&mut self, data: &[u8], ack_no_delay: bool) -> Result<usize, KcpError> {
        let r = self.input_no_flush(data, ack_no_delay)?;
        let current = self.current_ms() as u32;
        self.flush_if_pending(current);
        Ok(r)
    }

    /// Insert a segment into the receive buffer and try to move data to the
    /// receive queue. Matches Go's `parse_data`.
    ///
    /// Returns `true` if the segment was a duplicate (or outside window after
    /// already being acked), matching Go's `repeat` flag for SNMP.RepeatSegs.
    fn parse_data(&mut self, newseg: Segment) -> bool {
        let sn = newseg.sn;

        // Check if outside receive window
        if itimediff(sn, self.rcv_nxt.wrapping_add(self.rcv_wnd)) >= 0
            || itimediff(sn, self.rcv_nxt) < 0
        {
            // The caller acquired this segment from the pool.  Recycle it on
            // every rejected path (including duplicate/out-of-window input),
            // rather than silently dropping its allocation.
            self.pool.release(newseg);
            return true;
        }

        // The common case is an in-order segment.  Bypass the receive-buffer
        // search and enqueue it directly when the local receive queue has
        // room; any already-buffered successor segments are then promoted by
        // `move_receive_buffer`.
        if sn == self.rcv_nxt && (self.rcv_queue.len() as u32) < self.rcv_wnd {
            self.rcv_nxt = self.rcv_nxt.wrapping_add(1);
            self.rcv_queue.push_back(newseg);
            self.move_receive_buffer();
            return false;
        }

        // Single backward pass: detect duplicate and find insertion position
        // simultaneously. Backward (as in Go kcp-go) because an out-of-order
        // segment almost always belongs near the tail — a forward scan costs
        // one comparison per buffered segment (up to `rcv_wnd`, i.e. ~1024)
        // for every reordered packet.
        let mut is_dup = false;
        let mut insert_pos = 0usize;
        for (i, s) in self.rcv_buf.iter().enumerate().rev() {
            if s.sn == sn {
                is_dup = true;
                break;
            }
            if itimediff(sn, s.sn) > 0 {
                insert_pos = i + 1;
                break;
            }
        }
        if !is_dup {
            self.rcv_buf.insert(insert_pos, newseg);
        } else {
            // Duplicate segment storage is no longer needed; return it to the
            // pool while retaining the existing buffered copy.
            self.pool.release(newseg);
        }

        // Move available data from rcv_buf → rcv_queue
        self.move_receive_buffer();
        is_dup
    }

    /// Move segments from the receive buffer into the receive queue when
    /// they are in-order and within the receive window.
    fn move_receive_buffer(&mut self) {
        while (self.rcv_queue.len() as u32) < self.rcv_wnd {
            let in_order = self
                .rcv_buf
                .front()
                .is_some_and(|seg| seg.sn == self.rcv_nxt);
            if !in_order {
                break;
            }
            if let Some(seg) = self.rcv_buf.pop_front() {
                self.rcv_nxt = self.rcv_nxt.wrapping_add(1);
                self.rcv_queue.push_back(seg);
            }
        }
    }

    // ── RTT estimation (matching Go's update_ack) ─────────────────────────

    fn update_ack(&mut self, rtt: i32) {
        // https://tools.ietf.org/html/rfc6298
        if self.rx_srtt == 0 {
            self.rx_srtt = rtt;
            self.rx_rttvar = rtt >> 1;
        } else {
            let mut delta = rtt - self.rx_srtt;
            self.rx_srtt += delta >> 3;
            if delta < 0 {
                delta = -delta;
            }
            if rtt < self.rx_srtt - self.rx_rttvar {
                // Low-RTT special case: 8x reduced weight
                self.rx_rttvar += (delta - self.rx_rttvar) >> 5;
            } else {
                self.rx_rttvar += (delta - self.rx_rttvar) >> 2;
            }
        }
        let rto = self.rx_srtt as u32 + self.interval.max((self.rx_rttvar as u32) << 2);
        self.rx_rto = rto.clamp(self.rx_minrto, RTO_MAX);
    }

    // ── Update / flush ────────────────────────────────────────────────────

    /// Advance the KCP state machine by one tick.
    ///
    /// `current` is the current timestamp in milliseconds.
    /// Should be called at a regular interval (typically 10–100ms).
    /// Returns the milliseconds until the next meaningful event (matching
    /// Go's `flush()` return value used by `SystemTimedSched`).
    pub fn update(&mut self, current: u32) -> u32 {
        if self.updated == 0 {
            self.updated = 1;
            self.ts_flush = current;
        }

        let mut slap = itimediff(current, self.ts_flush);

        // Clock jump detection (matching Go)
        if !(-10000..10000).contains(&slap) {
            self.ts_flush = current;
            slap = 0; // Go resets slap to 0 after jump (kcp.go:1017)
        }

        if slap >= 0 {
            self.ts_flush = self.ts_flush.wrapping_add(self.interval);
            if itimediff(current, self.ts_flush) >= 0 {
                self.ts_flush = current.wrapping_add(self.interval);
            }
            self.flush_with_current(current, true)
        } else {
            self.interval
        }
    }

    /// Force-flush all pending data.
    ///
    /// Returns `next_update` — the milliseconds until the next meaningful
    /// event (nearest RTO or interval), matching Go kcp-go's `flush()` return.
    #[inline]
    pub fn flush(&mut self) -> u32 {
        let current = self.current_ms() as u32;
        self.flush_with_current(current, true)
    }

    /// Flush WITHOUT emitting ACK segments (data / retransmit / window-probe
    /// only). `KcpStream` uses this on the inline write path so writes cannot
    /// consume ACKs before inbound processing or the protocol-deadline flush
    /// emits them in queue order.
    #[inline]
    pub fn flush_data_only(&mut self) -> u32 {
        let current = self.current_ms() as u32;
        self.flush_with_current(current, false)
    }

    /// Same as [`flush_data_only`](Self::flush_data_only) but with a pre-computed
    /// `current` timestamp, avoiding a redundant `current_ms()` call when the
    /// caller already has the value (e.g., flush loop, `send_to_kcp`).
    /// pprof: eliminates ~1000 redundant `current_ms()` calls/sec.
    #[inline]
    pub fn flush_data_only_with_current(&mut self, current: u32) -> u32 {
        self.flush_with_current(current, false)
    }

    /// Flush with an externally-provided timestamp.
    ///
    /// Use this when the caller already has the current monotonic millisecond
    /// value (e.g., `update()` receives `current`), avoiding a redundant
    /// `SystemTime::now()` syscall inside the flush loop.
    pub fn flush_with_current(&mut self, current: u32, flush_acks: bool) -> u32 {
        let mtu = self.mtu as usize;

        // Reuse the pre-allocated ACK segment (avoids pool acquire/release per
        // flush). Header fields are (re)initialised once per flush and reused
        // by the ACK, WAsk, and WIns sections below.
        self.reset_ack_seg();

        // ── Flush ACKs ──
        // The inline write path passes `false`; inbound immediate flushes and
        // the serialized protocol-deadline flush pass `true`. All operate
        // under the KCP mutex, preserving ACK queue order.
        if flush_acks {
            self.flush_ack_list(mtu);
        }

        // ── Window probing ──
        // P3: nodelay modes probe the closed peer window 10× sooner (50ms vs
        // 500ms) so a burst-induced wnd=0 recovers in one RTO instead of
        // stalling on the probe backoff (measured 947ms→68ms at 256KB@RPS=250).
        self.update_probe(current);

        // ── Flush WAsk / WIns ──
        self.flush_probe_segs(mtu);

        // ── Calculate window ──
        let mut cwnd = self.snd_wnd.min(self.rmt_wnd);
        if self.nocwnd == 0 {
            cwnd = self.cwnd.min(cwnd);
        }

        // ── Move segments from snd_queue to snd_buf ──
        let new_segs_count = self.move_snd_queue(cwnd);

        // ── Calculate resent threshold ──
        let resent = if self.fastresend <= 0 {
            0xFFFFFFFF
        } else {
            self.fastresend as u32
        };

        // ── Flush data segments ──
        let stats = self.flush_data_segs(current, mtu, resent, new_segs_count);

        // Update retransmission stats (matching Go)
        let retrans_sum = stats.lost_segs + stats.fast_retrans_segs + stats.early_retrans_segs;
        if retrans_sum > 0 {
            snmp::add(&DEFAULT_SNMP.retrans_segs, retrans_sum);
        }
        if stats.lost_segs > 0 {
            snmp::add(&DEFAULT_SNMP.lost_segs, stats.lost_segs);
        }
        if stats.fast_retrans_segs > 0 {
            snmp::add(&DEFAULT_SNMP.fast_retrans, stats.fast_retrans_segs);
        }
        if stats.early_retrans_segs > 0 {
            snmp::add(&DEFAULT_SNMP.early_retrans, stats.early_retrans_segs);
        }
        if stats.out_segs > 0 {
            snmp::add(&DEFAULT_SNMP.out_segs, stats.out_segs);
        }

        // ── Congestion control (matching Go) ──
        self.apply_congestion(cwnd, resent, &stats);

        // ── Flush remaining buffer ──
        Self::flush_buf(&mut self.buffer, &mut self.output, mtu);

        // Update SNMP queue stats (matching Go)
        snmp::store(
            &DEFAULT_SNMP.ring_buffer_snd_queue,
            self.snd_queue.len() as u64,
        );
        snmp::store(
            &DEFAULT_SNMP.ring_buffer_rcv_queue,
            self.rcv_queue.len() as u64,
        );
        snmp::store(
            &DEFAULT_SNMP.ring_buffer_snd_buffer,
            self.snd_buf.len() as u64,
        );

        // Note: ack_seg is now a reusable field, no need to release back to pool

        stats.next_update
    }

    /// Flush the output buffer and keep a datagram-sized capacity.
    ///
    /// `BytesMut::copy_to_bytes(len)` is `split_to(len).freeze()` (bytes 1.12):
    /// the tail capacity stays on `buf`, but when the buffer was full that
    /// tail is ~0 and the next `Segment::encode` would grow field-by-field.
    /// Re-reserve `mtu` after each flush so encoding stays allocation-free
    /// in the steady state.
    #[inline]
    fn flush_buf(buf: &mut BytesMut, output: &mut Box<dyn FnMut(Bytes) + Send>, mtu: usize) {
        if !buf.is_empty() {
            let data = buf.copy_to_bytes(buf.len());
            output(data);
        }
        if buf.capacity() < mtu {
            buf.reserve(mtu);
        }
    }

    /// Reset the reusable ACK segment header before a flush round.
    ///
    /// Compute values first to avoid borrow conflict with `&mut self.ack_seg`.
    fn reset_ack_seg(&mut self) {
        let wnd = self.wnd_unused();
        let ack_seg = &mut self.ack_seg;
        ack_seg.conv = self.conv;
        ack_seg.cmd = Command::Ack as u8;
        ack_seg.wnd = wnd;
        ack_seg.una = self.rcv_nxt;
        ack_seg.len = 0; // ACK has no payload
    }

    /// Drain the pending ACK list into the output buffer (`mtu` = datagram
    /// size). Assumes [`Self::reset_ack_seg`] ran for this flush round.
    fn flush_ack_list(&mut self, mtu: usize) {
        if self.acklist.is_empty() {
            return;
        }
        let ack_seg = &mut self.ack_seg;
        let n = self.acklist.len();
        for i in 0..n {
            let (ack_sn, ack_ts) = (self.acklist[i].sn, self.acklist[i].ts);
            // Check space
            if self.buffer.len() + KCP_OVERHEAD > mtu {
                Self::flush_buf(&mut self.buffer, &mut self.output, mtu);
            }
            // Filter jitter caused by bufferbloat (matching Go)
            if itimediff(ack_sn, self.rcv_nxt) >= 0 || i == n - 1 {
                ack_seg.sn = ack_sn;
                ack_seg.ts = ack_ts;
                ack_seg.encode(&mut self.buffer);
            }
        }
        self.acklist.clear();
    }

    /// Advance the window-probe timer/backoff while the peer window is closed.
    fn update_probe(&mut self, current: u32) {
        let probe_init = if self.nodelay != 0 {
            PROBE_INIT_NODELAY
        } else {
            PROBE_INIT
        };
        if self.rmt_wnd == 0 {
            if self.probe_wait == 0 {
                self.probe_wait = probe_init;
                self.ts_probe = current.wrapping_add(self.probe_wait);
            } else if itimediff(current, self.ts_probe) >= 0 {
                if self.probe_wait < probe_init {
                    self.probe_wait = probe_init;
                }
                self.probe_wait += self.probe_wait / 2;
                if self.probe_wait > IKCP_PROBE_LIMIT {
                    self.probe_wait = IKCP_PROBE_LIMIT;
                }
                self.ts_probe = current.wrapping_add(self.probe_wait);
                self.probe |= KCP_ASK_SEND;
            }
        } else {
            self.ts_probe = 0;
            self.probe_wait = 0;
        }
    }

    /// Emit the WAsk/WIns probe segments for the pending probe flags, then
    /// clear the flags for the next round.
    fn flush_probe_segs(&mut self, mtu: usize) {
        if (self.probe & KCP_ASK_SEND) != 0 {
            self.ack_seg.cmd = Command::WAsk as u8;
            if self.buffer.len() + KCP_OVERHEAD > mtu {
                Self::flush_buf(&mut self.buffer, &mut self.output, mtu);
            }
            self.ack_seg.encode(&mut self.buffer);
        }

        if (self.probe & KCP_ASK_TELL) != 0 {
            self.ack_seg.cmd = Command::WIns as u8;
            if self.buffer.len() + KCP_OVERHEAD > mtu {
                Self::flush_buf(&mut self.buffer, &mut self.output, mtu);
            }
            self.ack_seg.encode(&mut self.buffer);
        }

        self.probe = 0;
    }

    /// Move segments from snd_queue into snd_buf while the send window has
    /// room. Returns how many new segments were admitted this flush (the
    /// early-retransmit gate uses this to tell an idle pipe from a busy one).
    fn move_snd_queue(&mut self, cwnd: u32) -> u32 {
        let mut new_segs_count = 0u32;
        while itimediff(self.snd_nxt, self.snd_una.wrapping_add(cwnd)) < 0 {
            match self.snd_queue.pop_front() {
                Some(mut newseg) => {
                    newseg.conv = self.conv;
                    newseg.cmd = Command::Push as u8;
                    // SN is assigned here (matching Go: when moving to snd_buf)
                    newseg.sn = self.snd_nxt;
                    self.snd_buf.push_back(newseg);
                    self.snd_nxt = self.snd_nxt.wrapping_add(1);
                    new_segs_count += 1;
                }
                None => break,
            }
        }
        new_segs_count
    }

    /// Transmit / retransmit the segments in snd_buf (first send, fast
    /// retransmit, early retransmit, RTO) and collect the retransmission
    /// counters plus the nearest RTO-derived next-update deadline.
    fn flush_data_segs(
        &mut self,
        current: u32,
        mtu: usize,
        resent: u32,
        new_segs_count: u32,
    ) -> FlushDataStats {
        let mut next_update = self.interval;

        // Pre-compute window for use in the loop (avoids borrow conflict)
        let current_wnd = self.wnd_unused();

        // ── Flush data segments ──
        let mut change = 0u64;
        let mut lost_segs = 0u64;
        let mut fast_retrans_segs = 0u64;
        let mut early_retrans_segs = 0u64;
        let mut out_segs = 0u64;

        for seg in &mut self.snd_buf {
            if seg.acked {
                continue;
            }

            let mut needsend = false;

            if seg.xmit == 0 {
                // First transmission
                needsend = true;
                seg.rto = self.rx_rto;
                seg.resendts = current.wrapping_add(seg.rto);
            } else if seg.fastack >= resent && seg.fastack != 0xFFFFFFFF {
                // Fast retransmit — matching Go kcp-go exactly: fire when
                // `fastack >= fastresend` (typically 2 duplicate ACKs),
                // regardless of whether new data is being sent. An earlier
                // `new_segs_count > 0` gate disabled fast retransmit whenever
                // the send window was full — exactly when loss is most likely
                // — forcing ALL retransmissions through RTO timeout (200ms+),
                // which was the direct cause of P99/P999 tail latency spikes.
                needsend = true;
                seg.fastack = 0xFFFFFFFF; // must wait until RTO to reset
                seg.rto = self.rx_rto;
                seg.resendts = current.wrapping_add(seg.rto);
                change += 1;
                fast_retrans_segs += 1;
            } else if seg.fastack > 0 && seg.fastack != 0xFFFFFFFF && new_segs_count == 0 {
                // Early retransmit (Go `kcp.go`: same condition). One dup ACK
                // is enough *only* when this flush queued no new segments,
                // i.e. the pipe is idle and a speculative retransmit is
                // nearly free. The condition is not `new_segs_count > 0`:
                // firing while new data is being sent is what produced the
                // historical retransmit storm.
                needsend = true;
                seg.fastack = 0xFFFFFFFF;
                seg.rto = self.rx_rto;
                seg.resendts = current.wrapping_add(seg.rto);
                change += 1;
                early_retrans_segs += 1;
            } else if itimediff(current, seg.resendts) >= 0 {
                // RTO timeout
                needsend = true;
                if self.nodelay == 0 {
                    seg.rto += self.rx_rto; // Linear backoff
                } else {
                    seg.rto += self.rx_rto / 2; // Half-linear backoff (keep original)
                }
                seg.fastack = 0;
                seg.resendts = current.wrapping_add(seg.rto);
                lost_segs += 1;
            }

            if needsend {
                seg.xmit += 1;
                seg.ts = current;
                // Note: Go refreshes currentMs() per segment, but Rust borrow
                // rules prevent calling self.current_ms() inside the snd_buf loop.
                // This is a known minor deviation.
                seg.wnd = current_wnd;
                seg.una = self.rcv_nxt;

                let need = KCP_OVERHEAD + seg.len as usize;
                if self.buffer.len() + need > mtu {
                    Self::flush_buf(&mut self.buffer, &mut self.output, mtu);
                }

                // Encode header + payload directly into self.buffer
                // (encode() writes both header and data, avoiding per-seg Vec alloc)
                seg.encode(&mut self.buffer);

                // Aggregate SNMP updates once per flush instead of paying an
                // atomic gated increment for every segment in a burst.
                out_segs += 1;

                // Dead link check (matching Go)
                if seg.xmit >= self.dead_link {
                    self.state = DEAD_LINK_STATE;
                }
            }

            // Track nearest RTO for nextUpdate (matching Go kcp-go)
            if !seg.acked {
                let rto = itimediff(seg.resendts, current);
                if rto > 0 && (rto as u32) < next_update {
                    next_update = rto as u32;
                }
            }
        }

        FlushDataStats {
            next_update,
            change,
            lost_segs,
            fast_retrans_segs,
            early_retrans_segs,
            out_segs,
        }
    }

    /// Congestion control (matching Go): RFC 6937 rate halving on fast
    /// retransmit, RFC 5681 backoff to one segment on loss.
    fn apply_congestion(&mut self, cwnd: u32, resent: u32, stats: &FlushDataStats) {
        // ── Congestion control (matching Go) ──
        if self.nocwnd == 0 {
            // Rate halving (RFC 6937)
            if stats.change > 0 {
                let inflight = self.snd_nxt.wrapping_sub(self.snd_una);
                self.ssthresh = (inflight / 2).max(THRESH_MIN);
                self.cwnd = self.ssthresh + resent;
                self.incr = self.cwnd * self.mss;
            }

            // Congestion control (RFC 5681)
            if stats.lost_segs > 0 {
                self.ssthresh = (cwnd / 2).max(THRESH_MIN);
                self.cwnd = 1;
                self.incr = self.mss;
            }

            if self.cwnd < 1 {
                self.cwnd = 1;
                self.incr = self.mss;
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    /// Current send window (packets in-flight).
    #[inline]
    pub fn wait_send(&self) -> u32 {
        self.snd_buf.len() as u32 + self.snd_queue.len() as u32
    }

    /// Number of segments in the send queue.
    #[inline]
    pub fn snd_queue_len(&self) -> usize {
        self.snd_queue.len()
    }

    /// Number of unacknowledged ACKs queued for the next flush.
    #[inline]
    pub fn acklist_len(&self) -> usize {
        self.acklist.len()
    }

    /// Whether the async driver still has protocol maintenance to schedule
    /// after sending the current output batch.
    #[inline]
    #[cfg(feature = "async")]
    pub(crate) fn needs_update(&self) -> bool {
        self.wait_send() > 0
            || !self.acklist.is_empty()
            || self.pending_flush != 0
            || self.probe != 0
    }

    /// Whether a deferred flush is pending (ACKs / data to emit).
    #[inline]
    pub fn pending_flush_flag(&self) -> u32 {
        self.pending_flush
    }

    /// Check if there's data ready to receive.
    #[inline]
    pub fn can_recv(&self) -> bool {
        !self.rcv_queue.is_empty()
    }

    /// Peek at the receive queue without consuming.
    #[inline]
    pub fn peek_recv(&self) -> Option<&Segment> {
        self.rcv_queue.front()
    }

    /// Get the conversation ID.
    #[inline]
    pub fn conv(&self) -> u32 {
        self.conv
    }

    /// KCP connection state. `0xFFFFFFFF` means dead (dead_link exceeded).
    #[inline]
    pub fn state(&self) -> u32 {
        self.state
    }

    /// Returns true if KCP connection is dead (state == 0xFFFFFFFF).
    #[inline]
    pub fn is_dead(&self) -> bool {
        self.state == DEAD_LINK_STATE
    }

    /// Get the current RTO value.
    #[inline]
    pub fn rx_rto(&self) -> u32 {
        self.rx_rto
    }

    /// Get the current congestion window.
    #[inline]
    pub fn cwnd(&self) -> u32 {
        self.cwnd
    }

    /// Get the current update interval.
    #[inline]
    pub fn interval(&self) -> u32 {
        self.interval
    }

    /// Get the current timestamp in milliseconds (for matching Go).
    ///
    /// Returns `u64` to avoid 32-bit overflow after ~49.7 days of uptime.
    /// Callers that need the KCP 32-bit wire timestamp should truncate with
    /// `as u32` — `itimediff` handles wraparound correctly.
    ///
    /// **Optimization**: delegates to the shared cached clock [`wall_ms`].
    pub fn current_ms(&self) -> u64 {
        wall_ms()
    }

    /// Get the maximum segment size.
    #[inline]
    pub fn mss(&self) -> u32 {
        self.mss
    }

    /// Get the send window size.
    #[inline]
    pub fn snd_wnd(&self) -> u32 {
        self.snd_wnd
    }

    /// Get the receive window size.
    #[inline]
    pub fn rcv_wnd(&self) -> u32 {
        self.rcv_wnd
    }

    /// Get the segment pool reference.
    #[inline]
    pub fn pool(&self) -> &SegmentPool {
        &self.pool
    }

    /// Get next sequence number.
    #[inline]
    /// Next sequence number this KCP expects to receive.
    ///
    /// Zero means nothing has been received yet; anything above zero means the
    /// peer's sequence space is already open, so a later segment numbered 0 can
    /// only come from a *restarted* peer KCP (sequence numbers never wrap back).
    pub fn rcv_nxt(&self) -> u32 {
        self.rcv_nxt
    }

    pub fn snd_nxt(&self) -> u32 {
        self.snd_nxt
    }

    /// Get oldest unacknowledged sequence number.
    #[inline]
    pub fn snd_una(&self) -> u32 {
        self.snd_una
    }

    /// Peer's most recently advertised receive window (`rmt_wnd`).
    ///
    /// Zero means the peer is out of buffer space and is deliberately not
    /// accepting new data, so a stalled `snd_una` is flow control rather than
    /// a lost peer.
    #[inline]
    pub fn rmt_wnd(&self) -> u32 {
        self.rmt_wnd
    }
}

// ─── Errors ──────────────────────────────────────────────────────────────

/// Errors produced by the KCP state machine.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KcpError {
    /// No data available to receive.
    #[error("no data available")]
    NoData,
    /// Too many fragments for a single send.
    #[error("too many fragments")]
    TooManyFragments,
    /// Invalid segment - length exceeds remaining data.
    #[error("invalid length")]
    InvalidLength,
    /// Invalid segment - conversation ID mismatch.
    #[error("conv mismatch: expected={expected}, got={got}")]
    ConvMismatch { expected: u32, got: u32 },
    /// Invalid segment - unknown command byte.
    #[error("unknown command: 0x{0:02x}")]
    UnknownCommand(u8),
    /// Invalid KCP configuration (rejected setter).
    #[error("invalid config: {0}")]
    InvalidConfig(String),
}

// ─── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests;
