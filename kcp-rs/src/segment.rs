//! KCP wire-format segment definitions and zero-allocation pool.
//!
//! The segment header is 24 bytes on the wire:
//!
//! ```text
//! 0      4       8       12      16      20      24
//! +------+-------+-------+-------+-------+-------+
//! | conv | cmd   | frg   | wnd   | ts    | sn    |
//! +------+-------+-------+-------+-------+-------+
//! | una  | len   | data...                       |
//! +------+-------+                               |
//! |                                               |
//! +-----------------------------------------------+
//! ```
//!
//! All multibyte fields are **little-endian** on the wire.

use std::fmt;

use bytes::{BufMut, BytesMut};

// ─── Wire constants ───────────────────────────────────────────────────────────

/// Maximum segment size (MTU-safe default).
/// Matches Go kcp-go `IKCP_MTU_DEF = 1400`.
pub const MTU: usize = 1400;
/// Overhead of the KCP header (24 bytes).
pub const KCP_OVERHEAD: usize = 24;
/// Default reliable window.
/// Matches Go kcp-go `IKCP_WND_SND = 32` / `IKCP_WND_RCV = 32`.
pub const KCP_DEFAULT_WND: u32 = 32;
/// Maximum number of fragments per segment.
/// Matches Go kcp-go limit of 255 (uint8 max).
pub const KCP_MAX_FRAG: u32 = 255;
/// KCP command codes.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    /// Push data.
    Push = 81, // 'Q' — command for IKCP_CMD_PUSH
    /// Acknowledgment.
    Ack = 82, // 'R' — command for IKCP_CMD_ACK
    /// Window probe.
    WAsk = 83, // 'S' — command for IKCP_CMD_WASK
    /// Window size advertisement.
    WIns = 84, // 'T' — command for IKCP_CMD_WINS
}

impl Command {
    #[inline]
    pub fn from_u8(v: u8) -> Option<Command> {
        match v {
            81 => Some(Command::Push),
            82 => Some(Command::Ack),
            83 => Some(Command::WAsk),
            84 => Some(Command::WIns),
            _ => None,
        }
    }
}

/// Flag: send window size advertisement (WIns) in next flush.
/// Set when a WAsk is received from the peer, or proactively when
/// our receive window opens up after being full.
/// Matches the original KCP C define `IKCP_ASK_TELL = 2`.
pub const KCP_ASK_TELL: u32 = 2;
/// Ask threshold for window probing.
pub const KCP_ASK_SEND: u32 = 1;
/// Data segment is ready to send.
pub const KCP_THRESHOLD_INIT: u32 = 2;
// ─── Segment ─────────────────────────────────────────────────────────────────

/// A single KCP segment. Fields correspond directly to the wire header.
#[derive(Clone)]
pub struct Segment {
    /// Conversation ID.
    pub conv: u32,
    /// Command (PUSH, ACK, WASK, WINS).
    pub cmd: u8,
    /// Fragment count (for reassembly).
    pub frg: u8,
    /// Remote window size (advertised).
    pub wnd: u16,
    /// Timestamp.
    pub ts: u32,
    /// Sequence number.
    pub sn: u32,
    /// Unacknowledged sequence number.
    pub una: u32,
    /// Payload length.
    pub len: u32,
    /// Payload data.
    pub data: BytesMut,
    /// Number of retransmissions.
    pub resendts: u32,
    /// Retransmission timeout.
    pub rto: u32,
    /// Fast acknowledgment count.
    pub fastack: u32,
    /// Whether this segment has been acknowledged.
    pub acked: bool,
    /// Number of times this segment has been transmitted (0 = never sent).
    pub xmit: u32,
}

impl Segment {
    /// Create a new segment with the given capacity for payload data.
    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        Segment {
            conv: 0,
            cmd: 0,
            frg: 0,
            wnd: 0,
            ts: 0,
            sn: 0,
            una: 0,
            len: 0,
            data: BytesMut::with_capacity(cap),
            resendts: 0,
            rto: 0,
            fastack: 0,
            acked: false,
            xmit: 0,
        }
    }

    /// Reset all fields for re-use — avoids deallocation.
    #[inline]
    pub fn reset(&mut self) {
        self.conv = 0;
        self.cmd = 0;
        self.frg = 0;
        self.wnd = 0;
        self.ts = 0;
        self.sn = 0;
        self.una = 0;
        self.len = 0;
        self.data.clear();
        self.resendts = 0;
        self.rto = 0;
        self.fastack = 0;
        self.acked = false;
        self.xmit = 0;
    }

    /// Encode this segment into the provided `BufMut`.
    ///
    /// Returns the number of bytes written (header + payload).
    /// Header is written directly to the buffer (zero temp-array allocation).
    #[inline(always)]
    pub fn encode<B: BufMut>(&self, buf: &mut B) -> usize {
        // Write header fields directly to avoid stack array allocation
        // This saves ~24 bytes of stack space per call and reduces memory writes
        buf.put_u32_le(self.conv);
        buf.put_u8(self.cmd);
        buf.put_u8(self.frg);
        buf.put_u16_le(self.wnd);
        buf.put_u32_le(self.ts);
        buf.put_u32_le(self.sn);
        buf.put_u32_le(self.una);
        buf.put_u32_le(self.len);
        if self.len > 0 {
            buf.put_slice(&self.data[..self.len as usize]);
        }
        KCP_OVERHEAD + self.len as usize
    }

    /// Decode a segment header (and optionally payload) from the given bytes.
    ///
    /// Returns `None` if the input is too short to contain a header.
    pub fn decode(data: &[u8]) -> Option<(Segment, usize)> {
        if data.len() < KCP_OVERHEAD {
            return None;
        }
        // Direct indexing after length guard — consistent with FEC/CFB style
        // (avoids try_into overhead; all accesses are in-bounds).
        let conv = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let cmd = data[4];
        let frg = data[5];
        let wnd = u16::from_le_bytes([data[6], data[7]]);
        let ts = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);
        let sn = u32::from_le_bytes([data[12], data[13], data[14], data[15]]);
        let una = u32::from_le_bytes([data[16], data[17], data[18], data[19]]);
        let len = u32::from_le_bytes([data[20], data[21], data[22], data[23]]);

        // Keep the arithmetic checked before using the attacker-controlled
        // payload length for slicing/allocation.  On 32-bit targets a
        // `u32` length plus the 24-byte header can overflow `usize`.
        let payload_len = usize::try_from(len).ok()?;
        let total_len = KCP_OVERHEAD.checked_add(payload_len)?;
        if data.len() < total_len {
            return None;
        }

        let mut seg = Segment::with_capacity(payload_len);
        seg.conv = conv;
        seg.cmd = cmd;
        seg.frg = frg;
        seg.wnd = wnd;
        seg.ts = ts;
        seg.sn = sn;
        seg.una = una;
        seg.len = len;

        if len > 0 {
            seg.data.extend_from_slice(&data[KCP_OVERHEAD..total_len]);
        }

        Some((seg, total_len))
    }
}

impl fmt::Debug for Segment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Segment")
            .field("conv", &self.conv)
            .field("cmd", &self.cmd)
            .field("frg", &self.frg)
            .field("wnd", &self.wnd)
            .field("ts", &self.ts)
            .field("sn", &self.sn)
            .field("una", &self.una)
            .field("len", &self.len)
            .field("acked", &self.acked)
            .finish()
    }
}

// ─── Segment Pool ─────────────────────────────────────────────────────────────

// Preserve all 4096 default-MTU buffers in KCP's pool (about 5.5 MiB), while
// limiting the idle cost of a large-window / jumbo-MTU burst. These bounds
// cover full backing capacity, including known KCP receive tails whose Bytes
// payload may still be live. Unknown shared views use the conservative public
// release path so a small view cannot hide a much larger allocation.
const POOL_MAX_RETAINED_BYTES: usize = 8 * 1024 * 1024;
const POOL_MAX_BUFFER_CAPACITY: usize = 16 * 1024;

struct PooledEntry {
    segment: Segment,
    charged_capacity: usize,
    // Only recv_bytes can create this entry, recording the exclusively owned
    // buffer's capacity before splitting off the returned Bytes payload.
    pending_tail: bool,
}

/// A pool of [`Segment`] instances.
///
/// Backed by a simple stack. KCP is always behind a
/// `Mutex<KCP>`, so pool access is serialized — no need for lock-free
/// atomics (the previous `crossbeam::SegQueue` used CAS per acquire/release,
/// adding ~10-20ns of atomic overhead per segment on the hot path).
///
/// When the pool is empty, new segments are allocated directly — the
/// amortized cost is near-zero under steady-state traffic because segments
/// are recycled as soon as they are acknowledged.
///
/// Retention is bounded by the constructor's segment count, 8 MiB of payload
/// capacity, and 16 KiB per payload buffer. Larger buffers are dropped when
/// returned. Public `release` rejects unknown shared views and reclaims unique
/// views before accounting. KCP can retain a receive tail with a known full
/// capacity; acquire checks ownership and restores the full buffer before
/// reuse. Active segments and their wire format are unchanged.
/// Allocator metadata / the segment index are outside the payload budget.
pub struct SegmentPool {
    inner: Vec<PooledEntry>,
    max_capacity: usize,
    retained_bytes: usize,
    created: u32,
}

impl SegmentPool {
    /// Create a new pool with the given maximum capacity.
    #[inline]
    pub fn new(max_capacity: usize) -> Self {
        SegmentPool {
            // Reserve actual Segment storage only when traffic creates one.
            // A fixed 64-slot index allocation per idle KCP connection adds
            // substantial cold RSS at high connection counts without avoiding
            // any payload allocation (the pool starts logically empty).
            inner: Vec::new(),
            max_capacity,
            retained_bytes: 0,
            created: 0,
        }
    }

    /// Acquire a segment from the pool, or allocate a fresh one.
    #[inline]
    pub fn acquire(&mut self) -> Segment {
        match self.inner.pop() {
            Some(mut entry) => {
                self.retained_bytes -= entry.charged_capacity;
                if !entry.pending_tail {
                    return entry.segment;
                }
                // A nonempty split leaves an empty tail with less than the
                // charged full capacity. Asking to reclaim that full capacity
                // exceeds the tail's spare room, so try_reclaim must check
                // unique ownership rather than take its capacity-only shortcut.
                // It never allocates and restores the whole buffer on success.
                if entry.segment.data.is_empty()
                    && entry.segment.data.capacity() < entry.charged_capacity
                {
                    if entry.segment.data.try_reclaim(entry.charged_capacity)
                        && entry.segment.data.capacity() == entry.charged_capacity
                    {
                        return entry.segment;
                    }
                    drop(entry);
                    return self.fresh_segment();
                }
                // split_to has already promoted this buffer, including an
                // empty zero-capacity tail, to BytesMut's shared storage.
                // A zero-length split leaves the full-capacity view: it needs
                // an explicit unique check because try_reclaim(full_capacity)
                // would otherwise succeed without checking ownership.
                let data = std::mem::take(&mut entry.segment.data).freeze();
                if let Ok(mut data) = data.try_into_mut() {
                    let _ = data.try_reclaim(data.capacity() + 1);
                    if data.capacity() == entry.charged_capacity {
                        entry.segment.data = data;
                        return entry.segment;
                    }
                }
                // A caller still owns the payload (or the trusted capacity
                // invariant was broken). Drop our old handle and return a
                // new whole buffer, never a shared/sliced view to KCP.
                self.fresh_segment()
            }
            None => self.fresh_segment(),
        }
    }

    #[inline]
    fn fresh_segment(&mut self) -> Segment {
        // This diagnostic must not panic after repeated cache misses.
        self.created = self.created.saturating_add(1);
        Segment::with_capacity(MTU)
    }

    #[inline]
    fn can_retain(&self, capacity: usize) -> bool {
        self.inner.len() < self.max_capacity
            && capacity != 0
            && capacity <= POOL_MAX_BUFFER_CAPACITY
            && capacity <= POOL_MAX_RETAINED_BYTES - self.retained_bytes
    }

    #[inline]
    fn push(&mut self, segment: Segment, charged_capacity: usize, pending_tail: bool) {
        self.retained_bytes += charged_capacity;
        self.inner.push(PooledEntry {
            segment,
            charged_capacity,
            pending_tail,
        });
    }

    /// KCP-only return of a whole, exclusively owned buffer acquired here.
    /// send/input only fill or clear that buffer; recv_bytes is the sole split
    /// path and must instead call release_after_split with its pre-split size.
    #[inline]
    pub(crate) fn release_unique(&mut self, mut seg: Segment) {
        let capacity = seg.data.capacity();
        if self.can_retain(capacity) {
            seg.reset();
            self.push(seg, capacity, false);
        }
    }

    /// KCP-only return after splitting a formerly whole, exclusively owned
    /// buffer. Charge its full pre-split capacity, including a zero-size tail,
    /// and defer ownership/reclamation until acquire.
    #[inline]
    pub(crate) fn release_after_split(&mut self, mut seg: Segment, full_capacity: usize) {
        if self.can_retain(full_capacity) {
            seg.reset();
            self.push(seg, full_capacity, true);
        }
    }

    /// Return a segment to the pool for reuse.
    #[inline]
    pub fn release(&mut self, mut seg: Segment) {
        let capacity = seg.data.capacity();
        if self.inner.len() >= self.max_capacity
            || capacity == 0
            || capacity > POOL_MAX_BUFFER_CAPACITY
        {
            return;
        }
        seg.reset();
        // Promote Vec-backed buffers once to BytesMut's shared representation.
        // Freezing an empty Vec directly can lose its spare capacity, or incur
        // a metadata allocation on every return. The zero-length split makes
        // the conversion preserve storage without zeroing/copying the payload.
        // Its temporary handle is dropped before checking exclusive ownership.
        drop(seg.data.split_to(0));
        let data = std::mem::take(&mut seg.data).freeze();
        let Ok(mut data) = data.try_into_mut() else {
            // recv_bytes may still own the other half of this allocation.
            // Drop this handle rather than pinning that backing after the
            // caller eventually drops the returned payload.
            return;
        };
        let capacity = data.capacity();
        if capacity > POOL_MAX_BUFFER_CAPACITY {
            return;
        }
        // Asking for one byte beyond the current view cannot allocate. A
        // unique tail can reclaim the prefix; its full capacity is then visible
        // to the bounds below. Full buffers simply return false unchanged.
        let _ = data.try_reclaim(capacity + 1);
        let capacity = data.capacity();
        if !self.can_retain(capacity) {
            return;
        }
        seg.data = data;
        self.push(seg, capacity, false);
    }

    /// Total number of segments created, saturating at `u32::MAX`.
    #[inline]
    pub fn created(&self) -> u32 {
        self.created
    }

    /// Approximate number of segments currently in the pool.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the pool contains no segments.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
#[path = "segment/pool_tests.rs"]
mod pool_tests;

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_encode_decode_roundtrip() {
        let mut seg = Segment::with_capacity(32);
        seg.conv = 0xDEAD;
        seg.cmd = Command::Push as u8;
        seg.frg = 1;
        seg.wnd = 512;
        seg.ts = 1000;
        seg.sn = 42;
        seg.una = 10;
        seg.len = 4;
        seg.data.extend_from_slice(b"test");

        let mut buf = Vec::with_capacity(KCP_OVERHEAD + 4);
        seg.encode(&mut buf);

        let (decoded, consumed) = Segment::decode(&buf).unwrap();
        assert_eq!(consumed, KCP_OVERHEAD + 4);
        assert_eq!(decoded.conv, 0xDEAD);
        assert_eq!(decoded.cmd, Command::Push as u8);
        assert_eq!(decoded.frg, 1);
        assert_eq!(decoded.wnd, 512);
        assert_eq!(decoded.ts, 1000);
        assert_eq!(decoded.sn, 42);
        assert_eq!(decoded.una, 10);
        assert_eq!(decoded.len, 4);
        assert_eq!(&decoded.data[..4], b"test");
    }

    #[test]
    fn segment_decode_short_buffer() {
        assert!(Segment::decode(&[0u8; 10]).is_none());
    }

    #[test]
    fn segment_decode_rejects_wrapping_payload_length() {
        let mut wire = vec![0u8; KCP_OVERHEAD];
        wire[KCP_OVERHEAD - 4..].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(Segment::decode(&wire).is_none());
    }

    #[test]
    fn segment_pool_reuses_segments() {
        let mut pool = SegmentPool::new(10);
        let seg = pool.acquire();
        assert_eq!(seg.len, 0);
        pool.release(seg);
        let seg2 = pool.acquire();
        assert_eq!(seg2.len, 0);
        assert_eq!(pool.created(), 1, "pool should reuse the released segment");
    }

    #[test]
    fn segment_pool_respects_capacity() {
        let mut pool = SegmentPool::new(2);
        let mut segs = Vec::new();
        for _ in 0..10 {
            segs.push(pool.acquire());
        }
        for s in segs {
            pool.release(s);
        }
        assert!(pool.len() <= 2, "pool should not exceed max_capacity");
    }

    #[test]
    fn command_from_u8() {
        assert_eq!(Command::from_u8(81), Some(Command::Push));
        assert_eq!(Command::from_u8(82), Some(Command::Ack));
        assert_eq!(Command::from_u8(83), Some(Command::WAsk));
        assert_eq!(Command::from_u8(84), Some(Command::WIns));
        assert_eq!(Command::from_u8(0), None);
    }

    #[test]
    fn segment_reset_clears_fields() {
        let mut seg = Segment::with_capacity(64);
        seg.conv = 1;
        seg.cmd = 81;
        seg.frg = 2;
        seg.wnd = 100;
        seg.ts = 500;
        seg.sn = 10;
        seg.una = 5;
        seg.len = 8;
        seg.data.extend_from_slice(b"12345678");
        seg.resendts = 1000;
        seg.rto = 200;
        seg.fastack = 3;
        seg.acked = true;

        seg.reset();

        assert_eq!(seg.conv, 0);
        assert_eq!(seg.len, 0);
        assert!(seg.data.is_empty());
        assert!(!seg.acked);
        // Ensure capacity is preserved
        assert!(seg.data.capacity() >= 64);
    }
}
