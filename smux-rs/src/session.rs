//! SMUX session — the core multiplexer that manages multiple streams over a
//! single transport connection.
//!
//! A `Session` wraps a transport `io::Read + io::Write` and provides:
//! - Opening and accepting streams
//! - Multiplexing data frames across streams
//! - Keepalive (ping/pong)
//! - Graceful shutdown

use log::debug;
use std::collections::{HashMap, VecDeque};
use std::io::{self};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use bytes::{Bytes, BytesMut};

use crate::frame::{Cmd, Frame, FrameCodec};
use crate::stream::{Stream, StreamState};

/// Highest stream id an `open_stream` may hand out.
///
/// This is an id-space limit, not a concurrency limit: ids are never reused,
/// so the counter only stops when it runs out of room (Go signals `ErrGoAway`
/// when `nextStreamID` wraps past 2^32).
const MAX_STREAMS: u32 = u32::MAX - 1;
/// SMUX session configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// SMUX protocol version (1 or 2).
    pub version: u8,
    /// Maximum overall receive buffer for the session (bytes).
    pub max_receive_buffer: usize,
    /// Maximum per-stream receive buffer (bytes).
    pub max_stream_buffer: usize,
    /// Maximum frame size (bytes).
    pub max_frame_size: usize,
    /// Hard cap on undecoded bytes sitting in the frame codec (bytes).
    ///
    /// A peer that announces a huge `length` and then dribbles fragments
    /// must not be able to pin unbounded memory while `decode` returns
    /// `None` (P0-4 / M-2). Default [`DEFAULT_MAX_BUFFERED`].
    pub max_codec_buffered: usize,
    /// Maximum number of live inbound (SYN-created) streams.
    ///
    /// Each stream allocates buffers, atomics and notify state. Without a
    /// cap a SYN flood from one peer linearly drives RSS (P0-5 / M-3).
    /// Default [`DEFAULT_MAX_INBOUND_STREAMS`].
    pub max_inbound_streams: u32,
    /// Keepalive interval in seconds.
    pub keepalive_interval: u64,
    /// Keepalive timeout in seconds (0 = disabled).
    pub keepalive_timeout: u64,
}

/// Default SMUX configuration.
pub static DEFAULT_CONFIG: Config = Config {
    version: 1,
    max_receive_buffer: 4 * 1024 * 1024,
    max_stream_buffer: 256 * 1024,
    max_frame_size: 16 * 1024,
    max_codec_buffered: crate::frame::DEFAULT_MAX_BUFFERED,
    max_inbound_streams: DEFAULT_MAX_INBOUND_STREAMS,
    keepalive_interval: 10,
    keepalive_timeout: 30,
};

/// Default cap on live inbound streams (P0-5 / M-3).
pub const DEFAULT_MAX_INBOUND_STREAMS: u32 = 1024;

impl Config {
    /// Verify that the configuration is valid.
    pub fn verify(&self) -> Result<(), SessionError> {
        if self.version != 1 && self.version != 2 {
            return Err(SessionError::InvalidConfig(format!(
                "unsupported smux version: {}",
                self.version
            )));
        }
        if self.keepalive_timeout != 0 {
            if self.keepalive_interval == 0 {
                return Err(SessionError::InvalidConfig(
                    "keep-alive interval must be positive".into(),
                ));
            }
            if self.keepalive_timeout < self.keepalive_interval {
                return Err(SessionError::InvalidConfig(
                    "keep-alive timeout must not be shorter than interval".into(),
                ));
            }
        }
        if self.max_frame_size == 0 {
            return Err(SessionError::InvalidConfig(
                "max frame size must be positive".into(),
            ));
        }
        if self.max_frame_size > u16::MAX as usize {
            return Err(SessionError::InvalidConfig(
                "max frame size must not exceed 65535".into(),
            ));
        }
        // One complete max-size frame must fit, or legitimate traffic would
        // trip the incomplete-frame flood guard.
        let min_codec = crate::frame::FRAME_HEADER_SIZE + self.max_frame_size;
        if self.max_codec_buffered < min_codec {
            return Err(SessionError::InvalidConfig(format!(
                "max codec buffered must be at least {min_codec} (header + max frame size)"
            )));
        }
        if self.max_inbound_streams == 0 {
            return Err(SessionError::InvalidConfig(
                "max inbound streams must be positive".into(),
            ));
        }
        if self.max_receive_buffer == 0 {
            return Err(SessionError::InvalidConfig(
                "max receive buffer must be positive".into(),
            ));
        }
        if self.max_receive_buffer > i32::MAX as usize {
            return Err(SessionError::InvalidConfig(
                "max receive buffer must not exceed 2147483647".into(),
            ));
        }
        if self.max_stream_buffer == 0 {
            return Err(SessionError::InvalidConfig(
                "max stream buffer must be positive".into(),
            ));
        }
        if self.max_stream_buffer > self.max_receive_buffer {
            return Err(SessionError::InvalidConfig(
                "max stream buffer must not exceed max receive buffer".into(),
            ));
        }
        if self.max_stream_buffer > i32::MAX as usize {
            return Err(SessionError::InvalidConfig(
                "max stream buffer must not exceed 2147483647".into(),
            ));
        }
        Ok(())
    }
}

/// Errors from the SMUX session.
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    /// I/O error from the underlying transport.
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    /// Invalid configuration.
    #[error("invalid config: {0}")]
    InvalidConfig(String),
    /// Session is closed.
    #[error("session closed")]
    SessionClosed,
    /// Maximum number of streams reached.
    #[error("too many streams")]
    TooManyStreams,
    /// Stream not found.
    #[error("stream {0} not found")]
    StreamNotFound(u32),
    /// Invalid frame received.
    #[error("invalid frame: {0}")]
    InvalidFrame(String),
}

/// A pending UPD frame to be sent to the peer.
///
/// Retained for API compatibility; UPD frames are now emitted directly
/// into the outbound buffer by [`Session::emit_upd_frames`], so this
/// struct is no longer produced by any Session method.
#[derive(Debug, Clone)]
pub struct UpdFrame {
    pub stream_id: u32,
    pub consumed: u32,
    pub window: u32,
}

/// COW snapshot type: contiguous slice of `(stream_id, Arc<Stream>)` pairs
/// for lock-free iteration on the flush path.
type StreamSnapshot = Arc<[(u32, Arc<Stream>)]>;

/// The SMUX session — multiplexes streams over a single transport.
pub struct Session {
    /// Session configuration.
    config: Config,
    /// Whether the session is closed.
    closed: Arc<AtomicBool>,
    /// All active streams, keyed by stream ID (point queries: get/insert/remove).
    streams: Arc<Mutex<HashMap<u32, Arc<Stream>>>>,
    /// COW snapshot of `(stream_id, Arc<Stream>)` pairs for lock-free iteration.
    ///
    /// Rebuilt by [`rebuild_snapshot`] whenever the HashMap changes
    /// (insert/remove/drain). The flush path does one `Mutex::lock().clone()`
    /// (a single atomic increment) then iterates the slice without any
    /// further lock — eliminating `process_data` ↔ `prepare_outbound`
    /// contention and replacing HashMap bucket pointer-chase with linear
    /// cache-prefetched traversal.
    stream_snapshot: Mutex<StreamSnapshot>,
    /// Next stream ID to assign (for client: odd, server: even).
    next_stream_id: AtomicU32,
    /// Latched once the id space is exhausted. `fetch_add` wraps mod 2^32,
    /// so the strict `id > max_streams` guard alone can never catch reuse
    /// of the top id — this latch makes "ids are never reused" hold
    /// (equivalent to Go's `ErrGoAway`).
    stream_ids_exhausted: AtomicBool,
    /// Frame codec for encoding/decoding frames.
    codec: Arc<Mutex<FrameCodec>>,
    /// Keepalive interval.
    keepalive_interval: Duration,
    /// Time of last keepalive.
    last_keepalive_ms: AtomicU64,
    /// Time of last inbound activity (any frame).
    last_activity_ms: AtomicU64,
    /// Maximum streams allowed.
    max_streams: u32,
    /// Token bucket for receive flow control (bytes remaining).
    token_bucket: AtomicI32,
    /// Bytes currently sitting undecoded in `codec` that have already been
    /// charged against `token_bucket`. Adjusted after every `process_data`
    /// so an incomplete-frame flood cannot occupy the codec rent-free (P0-4).
    codec_residual_charged: AtomicUsize,
    /// Accepted stream IDs waiting for SmuxConn::accept() to pick up.
    /// Only populated when `accept_enabled` is true (SmuxConn server mode).
    accepted_streams: Arc<Mutex<VecDeque<u32>>>,
    /// Notify for waking SmuxConn::accept() when a new stream arrives.
    accept_notify: knet::Notify,
    /// Only push to accepted_streams when true. kcptun never sets this,
    /// so the queue stays empty and there's zero overhead.
    accept_enabled: AtomicBool,
}

impl Session {
    /// Returns the configured SMUX protocol version (1 or 2).
    /// Go smux validates: hdr.Version() != config.Version → reject.
    pub fn version(&self) -> u8 {
        self.config.version
    }

    /// Create a new SMUX session.
    ///
    /// `is_client` controls the starting stream ID: client uses odd IDs
    /// (starting at 1), server uses even IDs (starting at 0).
    fn new(config: &Config, is_client: bool) -> Result<Self, SessionError> {
        config.verify()?;
        let next_id = if is_client { 1 } else { 0 };
        Ok(Session {
            config: config.clone(),
            closed: Arc::new(AtomicBool::new(false)),
            streams: Arc::new(Mutex::new(HashMap::new())),
            stream_snapshot: Mutex::new(Arc::from([])),
            next_stream_id: AtomicU32::new(next_id),
            stream_ids_exhausted: AtomicBool::new(false),
            // One frame is at most 8 + 65535 bytes, and `decode` hands out
            // `Bytes` views into this block — a slice retained by a slow
            // stream pins the whole allocation. Sizing it at
            // `max_receive_buffer` (4 MiB by default) meant every session
            // reserved 4 MiB up front and any unread payload kept it alive.
            codec: Arc::new(Mutex::new(FrameCodec::with_max_buffered(
                (crate::frame::FRAME_HEADER_SIZE + u16::MAX as usize)
                    .min(config.max_receive_buffer),
                config.max_codec_buffered,
            ))),
            keepalive_interval: Duration::from_secs(config.keepalive_interval),
            last_keepalive_ms: AtomicU64::new(knet::mono_ms()),
            last_activity_ms: AtomicU64::new(knet::mono_ms()),
            max_streams: MAX_STREAMS,
            token_bucket: AtomicI32::new(config.max_receive_buffer as i32),
            codec_residual_charged: AtomicUsize::new(0),
            accepted_streams: Arc::new(Mutex::new(VecDeque::new())),
            accept_notify: knet::Notify::new(),
            accept_enabled: AtomicBool::new(false),
        })
    }

    /// Create a new client-side SMUX session.
    ///
    /// A client session initiates stream creation and uses odd-numbered stream IDs.
    #[inline]
    pub fn new_client(config: &Config) -> Result<Self, SessionError> {
        Self::new(config, true)
    }

    /// Create a new server-side SMUX session.
    ///
    /// A server session accepts stream creation and uses even-numbered stream IDs.
    #[inline]
    pub fn new_server(config: &Config) -> Result<Self, SessionError> {
        Self::new(config, false)
    }

    /// Check if the session is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    /// Get a reference to the streams map.
    #[inline]
    pub fn streams(&self) -> Arc<Mutex<HashMap<u32, Arc<Stream>>>> {
        self.streams.clone()
    }

    /// Rebuild the COW stream snapshot from the HashMap.
    ///
    /// Call this after any insert/remove/drain on `streams`.
    ///
    /// Both `streams` and `stream_snapshot` locks are held for the full
    /// duration so that a concurrent caller cannot collect an older set of
    /// streams and then overwrite a newer snapshot (lost-update race).
    fn rebuild_snapshot(&self) {
        let mut snapshot_slot = self.stream_snapshot.lock();
        let pairs: Vec<(u32, Arc<Stream>)> = {
            let streams = self.streams.lock();
            streams.iter().map(|(&id, s)| (id, s.clone())).collect()
        };
        *snapshot_slot = Arc::from(pairs.into_boxed_slice());
    }

    /// Get a snapshot of all active streams for lock-free iteration.
    ///
    /// Returns an `Arc<[(u32, Arc<Stream>)]>` — a contiguous slice of
    /// `(stream_id, stream)` pairs.  One atomic increment; iterate
    /// without holding any lock.
    #[inline]
    pub fn stream_snapshot(&self) -> StreamSnapshot {
        self.stream_snapshot.lock().clone()
    }

    /// Get the session configuration.
    #[inline]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Get the current token bucket value (remaining receive window in bytes).
    #[inline]
    pub fn token_bucket_value(&self) -> i32 {
        self.token_bucket.load(Ordering::Relaxed)
    }

    /// Whether the session still has receive-window budget.
    ///
    /// A transport read loop must consult this before pulling more data:
    /// with `version = 1` there is no per-stream window at all, so this
    /// bucket is the only thing bounding how much unread data a fast peer
    /// can make us buffer. Go's `recvLoop` stops reading the transport while
    /// its bucket is non-positive; not implementing that meant
    /// `max_receive_buffer` was pure bookkeeping and memory grew without
    /// limit whenever the application read slower than the peer wrote.
    #[inline]
    pub fn has_receive_capacity(&self) -> bool {
        self.token_bucket.load(Ordering::Relaxed) > 0
    }

    /// Charge the receive window for a frame that was buffered for a stream.
    #[inline]
    fn consume_tokens(&self, n: usize) {
        self.token_bucket
            .fetch_sub(n.min(i32::MAX as usize) as i32, Ordering::Relaxed);
    }

    /// Return tokens to the token bucket (bytes consumed by the reader).
    /// This replenishes the flow control window after data has been read.
    pub fn return_tokens(&self, n: usize) {
        self.token_bucket
            .fetch_add(n.min(i32::MAX as usize) as i32, Ordering::Relaxed);
    }

    /// Reclaim the receive window for bytes the application has consumed.
    ///
    /// Go returns tokens from inside `Stream.Read`; a `Stream` here has no
    /// back-reference to its session, so the flush cycle sweeps the streams
    /// instead. Returns the number of bytes reclaimed.
    pub fn reclaim_tokens(&self) -> usize {
        let snapshot = self.stream_snapshot();
        let reclaimed: usize = snapshot.iter().map(|(_, s)| s.take_return_tokens()).sum();
        if reclaimed > 0 {
            self.return_tokens(reclaimed);
        }
        reclaimed
    }

    /// Queue a SYN frame to be sent by the next prepare_outbound_into() call.
    ///
    /// Used by `SmuxConn::open_stream` and `kcptun_session::open_stream`.
    /// The flag lives on the Stream so the flush path can emit SYN in the
    /// same snapshot walk that drains PSH/FIN — a session-level queue drained
    /// *before* the snapshot raced with `open_stream` and let PSH overtake SYN.
    pub fn queue_syn(&self, stream_id: u32) {
        if let Some(s) = self.streams.lock().get(&stream_id) {
            s.arm_syn();
        }
    }

    /// Pop the next accepted stream ID (for SmuxConn::accept()).
    ///
    /// Returns None when no new streams have been accepted since the last call.
    pub fn pop_accepted_stream(&self) -> Option<u32> {
        self.accepted_streams.lock().pop_front()
    }

    /// Get the accept notification handle (for SmuxConn::accept()).
    pub fn accept_notify(&self) -> &knet::Notify {
        &self.accept_notify
    }

    /// Enable accept queue (SmuxConn server mode).
    /// When enabled, process_data() will push accepted stream IDs and notify.
    pub fn enable_accept(&self) {
        self.accept_enabled.store(true, Ordering::Release);
    }

    /// Open a new stream on this session (client side).
    ///
    /// Returns the new stream.
    pub fn open_stream(&self) -> Result<Arc<Stream>, SessionError> {
        if self.is_closed() {
            return Err(SessionError::SessionClosed);
        }

        // Go only refuses to open once the id space itself is exhausted
        // (`ErrGoAway` when `nextStreamID` wraps). Capping at 65536 turned
        // the 32768th stream *ever opened* on a session into a hard failure,
        // which the client handles by closing the whole KCP session and
        // every stream still running on it — reachable in hours on a busy
        // short-connection proxy.
        //
        // The wrap itself must never hand out a live id again: `fetch_add`
        // wraps mod 2^32, and the server's even ids top out exactly at
        // `max_streams` (u32::MAX - 1), so the strict `>` below can never
        // catch the reuse. `stream_ids_exhausted` latches the exhaustion so
        // the "ids are never reused" invariant actually holds (L-1).
        if self.stream_ids_exhausted.load(Ordering::SeqCst) {
            return Err(SessionError::TooManyStreams);
        }
        let id = self.next_stream_id.fetch_add(2, Ordering::SeqCst);
        if id > self.max_streams {
            self.stream_ids_exhausted.store(true, Ordering::SeqCst);
            return Err(SessionError::TooManyStreams);
        }
        // This side's largest legal id has now been handed out (checked_add
        // overflows on the server's top id; a client's second-to-top id is
        // followed by one above `max_streams`). The current allocation still
        // succeeds — every later one fails without wrapping the counter.
        if id.checked_add(2).is_none_or(|next| next > self.max_streams) {
            self.stream_ids_exhausted.store(true, Ordering::SeqCst);
        }

        let stream = Arc::new(Stream::with_buffer(id, self.config.max_stream_buffer));
        stream.set_self_ref(Arc::downgrade(&stream));
        stream.set_state(StreamState::Ready);
        stream.mark_opened();
        // SMUX v1 has no UPD / per-stream send window.
        if self.config.version == 1 {
            stream.disable_peer_window();
        }

        self.streams.lock().insert(id, stream.clone());
        self.rebuild_snapshot();
        Ok(stream)
    }

    /// Accept the next incoming stream (server side).
    ///
    /// Returns the accepted stream, or `None` if a stream with this id
    /// already exists — a duplicated or replayed SYN must not evict a live
    /// stream (Go ignores the frame in that case). The old `Arc<Stream>`
    /// stayed in the application's hands but was no longer in the session
    /// map, so it never received data again and its writes were never
    /// drained: a silently wedged connection until the peer's TCP timeout.
    pub fn accept_stream(&self, id: u32) -> Result<Option<Arc<Stream>>, SessionError> {
        if self.is_closed() {
            return Err(SessionError::SessionClosed);
        }

        let mut streams = self.streams.lock();
        if streams.contains_key(&id) {
            return Ok(None);
        }

        let stream = Arc::new(Stream::with_buffer(id, self.config.max_stream_buffer));
        stream.set_self_ref(Arc::downgrade(&stream));
        stream.set_state(StreamState::Ready);
        stream.mark_opened();
        // SMUX v1 has no UPD / per-stream send window.
        if self.config.version == 1 {
            stream.disable_peer_window();
        }

        streams.insert(id, stream.clone());
        drop(streams);
        self.rebuild_snapshot();
        Ok(Some(stream))
    }

    /// Process incoming data from the transport.
    ///
    /// This should be called whenever new data arrives on the underlying
    /// connection.
    pub fn process_data(&self, data: &[u8]) -> Result<Vec<(u32, bytes::Bytes)>, SessionError> {
        if self.is_closed() {
            return Err(SessionError::SessionClosed);
        }

        let mut codec = self.codec.lock();
        if !codec.feed(data) {
            // P0-4 / M-2: a peer that announces a huge `length` and then
            // dribbles fragments used to pin unbounded bytes in `codec.buf`
            // while `decode` returned None and the token bucket stayed full.
            // Refuse to buffer past `max_buffered` and kill the session.
            let cap = codec.max_buffered();
            log::warn!("SMUX: codec buffer exceeded max_buffered ({cap}), closing session");
            drop(codec);
            self.close();
            return Err(SessionError::InvalidFrame(format!(
                "codec buffer exceeded max_buffered ({cap})"
            )));
        }

        self.decode_frames(&mut codec, data.len())
    }

    /// Detach the codec's inbound buffer for a direct transport read (O13).
    ///
    /// Returns the buffer (possibly holding an undecoded partial frame) plus
    /// how many more bytes it may accept before `max_buffered`. The reader
    /// fills it via `read_buf` (one memcpy less than read-vec → `feed`) and
    /// hands it back through [`end_inbound_read`]. Only the session's own
    /// read task does this — `process_data` callers see an empty codec
    /// window in between, which is fine because that same read task is the
    /// only producer of inbound bytes.
    pub(crate) fn begin_inbound_read(&self) -> (bytes::BytesMut, usize) {
        let mut codec = self.codec.lock();
        let allowance = codec.max_buffered().saturating_sub(codec.len());
        (codec.take_buf(), allowance)
    }

    /// Reattach a buffer from [`begin_inbound_read`] and decode it.
    ///
    /// Cap semantics match `feed()`: if the read pushed the buffered bytes
    /// past `max_buffered`, the session is killed with the same error.
    pub(crate) fn end_inbound_read(
        &self,
        buf: bytes::BytesMut,
    ) -> Result<Vec<(u32, bytes::Bytes)>, SessionError> {
        if self.is_closed() {
            return Err(SessionError::SessionClosed);
        }
        let mut codec = self.codec.lock();
        codec.restore_buf(buf);
        if codec.len() > codec.max_buffered() {
            let cap = codec.max_buffered();
            log::warn!("SMUX: codec buffer exceeded max_buffered ({cap}), closing session");
            drop(codec);
            self.close();
            return Err(SessionError::InvalidFrame(format!(
                "codec buffer exceeded max_buffered ({cap})"
            )));
        }
        let hint = codec.len();
        self.decode_frames(&mut codec, hint)
    }

    /// Decode every frame currently buffered in `codec` and run the frame
    /// handlers. Shared by [`process_data`] (feed first) and the direct
    /// transport-read path (`begin_inbound_read`/`end_inbound_read`), which
    /// reads straight into the codec buffer and skips the feed copy.
    fn decode_frames(
        &self,
        codec: &mut FrameCodec,
        hint: usize,
    ) -> Result<Vec<(u32, bytes::Bytes)>, SessionError> {
        // Frame-count hint: a KCP read chunk usually carries either a couple
        // of near-max frames or a burst of small ones. len/256 clamped to
        // [2, 16] lands the allocation on the common cases without the
        // 4→8→16→32 growth ladder `Vec::new()` pays for control bursts.
        let mut results = Vec::with_capacity((hint >> 8).clamp(2, 16));
        // O14: single-entry lookup cache for this call — consecutive frames
        // for the same stream (bulk burst) pay one `streams` lock per burst.
        let mut stream_cache: Option<(u32, Arc<Stream>)> = None;

        while let Some(frame) = codec.decode() {
            // Any received frame confirms peer is alive.
            self.update_activity();
            if frame.ver != self.config.version {
                // Go rejects a version mismatch with ErrInvalidProtocol
                // rather than half-processing the frame.
                log::warn!(
                    "SMUX: frame version {} != session version {}, closing session",
                    frame.ver,
                    self.config.version
                );
                self.close();
                return Err(SessionError::InvalidFrame(format!(
                    "frame version {} != session version {}",
                    frame.ver, self.config.version
                )));
            }
            match frame.cmd {
                Cmd::Syn => {
                    // Incoming stream request (Go cmdSYN = 0)
                    debug!("SMUX: received SYN for stream {}", frame.stream_id);
                    // P0-5 / M-3: refuse new inbound streams past the cap.
                    // smux has no RST command on the wire (Go's frame set is
                    // SYN/FIN/PSH/NOP/UPD only), so the SYN is dropped without
                    // allocating a Stream — that is what stops the RSS
                    // amplification. A peer that keeps pushing data on the
                    // rejected id has it silently ignored (no stream in the
                    // map), same as a SYN that never arrived.
                    {
                        let live = self.streams.lock().len();
                        if live >= self.config.max_inbound_streams as usize {
                            log::warn!(
                                "SMUX: inbound stream limit reached ({}/{}), dropping SYN for stream {}",
                                live,
                                self.config.max_inbound_streams,
                                frame.stream_id
                            );
                            continue;
                        }
                    }
                    if self.accept_stream(frame.stream_id)?.is_some()
                        && self.accept_enabled.load(Ordering::Acquire)
                    {
                        let mut accepted = self.accepted_streams.lock();
                        // Keep the accept backlog inside the same bound so a
                        // consumer that never drains cannot outgrow the map.
                        if accepted.len() < self.config.max_inbound_streams as usize {
                            accepted.push_back(frame.stream_id);
                            drop(accepted);
                            self.accept_notify.notify_one();
                        }
                    }
                }
                Cmd::Fin => {
                    // Stream closed by remote (Go cmdFIN = 1) — may carry last data
                    debug!("SMUX: received FIN for stream {}", frame.stream_id);
                    if let Some(stream) = self.stream_cached(&mut stream_cache, frame.stream_id) {
                        if !frame.data.is_empty() {
                            self.consume_tokens(frame.data.len());
                            if let Err(e) = stream.push_data_bytes(frame.data.clone()) {
                                // Push failed — the bytes were not buffered, so
                                // the charge must come back or the window leaks.
                                self.return_tokens(frame.data.len());
                                log::warn!(
                                    "push_data overflow FIN stream {}: {:?}",
                                    frame.stream_id,
                                    e
                                );
                            }
                        }
                        stream.mark_remote_closed();
                        stream.set_state(StreamState::FinReceived);
                    }
                }
                Cmd::Psh => {
                    // Data push (Go cmdPSH = 2)
                    if let Some(stream) = self.stream_cached(&mut stream_cache, frame.stream_id) {
                        // Use zero-copy push_data_bytes: the frame.data is a
                        // reference-counted Bytes slice from the codec buffer.
                        self.consume_tokens(frame.data.len());
                        if let Err(e) = stream.push_data_bytes(frame.data.clone()) {
                            // Not buffered → give the receive window back.
                            self.return_tokens(frame.data.len());
                            log::warn!(
                                "push_data overflow DATA stream {}: {:?}",
                                frame.stream_id,
                                e
                            );
                        }
                        results.push((frame.stream_id, frame.data));
                    }
                }
                Cmd::Nop => {
                    // No operation / keepalive (Go cmdNOP = 3)
                    // Go smux sends NOP frames as keepalive probes.
                    // Nothing to do on receive — the frame itself confirms
                    // the connection is alive.
                }
                Cmd::Upd => {
                    // Window update (Go cmdUPD = 4, v2 only)
                    // Format: [consumed 4B LE][window 4B LE]
                    //
                    // v1 sessions must ignore UPD: the send path
                    // (`emit_upd_frames`) already gates on `version >= 2`, and
                    // Go v1 peers never emit the frame. Accepting one here is
                    // an asymmetric hole — a crafted window=0 would clamp
                    // `peer_send_window` to 0 and, since `apply_peer_update`
                    // only wakes writers on a 0→positive transition, leave the
                    // writer Pending forever.
                    if self.config.version < 2 {
                        debug!(
                            "SMUX: v1 session received unexpected UPD frame on stream {}, dropped",
                            frame.stream_id
                        );
                        continue;
                    }
                    if frame.data.len() >= 8 {
                        let consumed =
                            u32::from_le_bytes(frame.data[0..4].try_into().unwrap_or([0; 4]));
                        let window =
                            u32::from_le_bytes(frame.data[4..8].try_into().unwrap_or([0; 4]));
                        // Apply per-stream peer window (write-side flow control).
                        if let Some(stream) = self.stream_cached(&mut stream_cache, frame.stream_id)
                        {
                            stream.apply_peer_update(consumed, window);
                        }
                        // NOTE: a UPD advertises the *peer's* receive window;
                        // it says nothing about ours. Adding it to the local
                        // token bucket (as this used to) inflated our own
                        // receive window by 256 KiB on every window update.
                        debug!(
                            "SMUX: UPD stream {} consumed={} window={}",
                            frame.stream_id, consumed, window
                        );
                    }
                }
            }
        }

        // P0-4 / M-2: charge the token bucket for bytes still sitting in the
        // codec awaiting a complete frame. Payload charges above cover only
        // frames that decoded; an incomplete-frame flood used to occupy
        // `codec.buf` without ever touching the bucket. The residual is
        // sticky across calls, so only the delta is applied.
        let residual = codec.len();
        let prev = self
            .codec_residual_charged
            .swap(residual, Ordering::Relaxed);
        if residual > prev {
            self.consume_tokens(residual - prev);
        } else if prev > residual {
            self.return_tokens(prev - residual);
        }

        Ok(results)
    }

    /// Emit pending UPD frames directly into the outbound buffer.
    ///
    /// Scans all streams for pending UPD flags and encodes each into `buf`
    /// immediately. Because `take_upd()` (which atomically clears the flag)
    /// and the frame encoding happen under the same `streams` lock, there is
    /// no window where a flag is cleared but the frame is lost — the old
    /// channel path could drop a frame on a full channel and relied on
    /// `rearm_upd()` to recover, which still left a race window.
    ///
    /// No-op for `version = 1`, which has no UPD frame.
    ///
    /// Callers that already hold the `streams` lock should use
    /// [`emit_upd_frames_locked`](Self::emit_upd_frames_locked) instead
    /// to avoid re-entrant locking.
    pub fn emit_upd_frames(&self, buf: &mut BytesMut, ver: u8) {
        if self.config.version < 2 {
            return;
        }
        let snapshot = self.stream_snapshot();
        self.emit_upd_frames_locked(&snapshot, buf, ver);
    }

    /// Same as [`emit_upd_frames`](Self::emit_upd_frames) but assumes the
    /// caller already holds the `streams` lock. Used by
    /// [`prepare_outbound_into_controlled`](Self::prepare_outbound_into_controlled)
    /// to merge all three passes (reclaim tokens, UPD emit, PSH drain) into
    /// a single `streams` lock acquisition.
    fn emit_upd_frames_locked(&self, snapshot: &[(u32, Arc<Stream>)], buf: &mut BytesMut, ver: u8) {
        if self.config.version < 2 {
            return;
        }
        for &(stream_id, ref stream) in snapshot {
            if let Some((consumed, window)) = stream.take_upd() {
                Frame::encode_header_into(buf, ver, Cmd::Upd, stream_id, 8);
                buf.extend_from_slice(&consumed.to_le_bytes());
                buf.extend_from_slice(&window.to_le_bytes());
                debug!(
                    "SMUX: emitted UPD frame stream={} consumed={} window={}",
                    stream_id, consumed, window
                );
            }
        }
    }

    /// O14: single-entry stream lookup cache scoped to one `decode_frames`
    /// call. Consecutive frames for the same stream (the common bulk burst)
    /// pay one `streams` lock per burst instead of one per frame.
    ///
    /// Only *positive* lookups are cached: a SYN earlier in this same batch
    /// may have just created the stream, so a miss must re-query. The cache
    /// is call-local, so removals/rebuilds between calls need no
    /// invalidation. As a side effect the `streams` mutex is no longer held
    /// across `push_data_bytes`/`apply_peer_update` (the old `if let Some =
    /// lock().get()` kept the guard alive for the whole arm body) — those
    /// actions only touch the stream itself, which the `Arc` pins.
    #[inline]
    fn stream_cached(
        &self,
        cache: &mut Option<(u32, Arc<Stream>)>,
        id: u32,
    ) -> Option<Arc<Stream>> {
        if let Some((cid, s)) = cache {
            if *cid == id {
                return Some(s.clone());
            }
        }
        let found = self.streams.lock().get(&id).cloned();
        if let Some(s) = &found {
            *cache = Some((id, s.clone()));
        }
        found
    }

    /// Close the session and all streams.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.accept_notify.notify_waiters();
        let mut streams = self.streams.lock();
        for (_, stream) in streams.drain() {
            stream.close();
        }
        drop(streams);
        self.rebuild_snapshot();
    }

    /// Remove one stream from the session map and fully close it.
    ///
    /// Returns `true` if the id was present. Used for SYN-send failures and
    /// force-reap paths so map entries cannot leak.
    pub fn remove_stream(&self, id: u32) -> bool {
        let mut streams = self.streams.lock();
        if let Some(stream) = streams.remove(&id) {
            let recycled = stream.recycle_tokens();
            stream.close();
            drop(streams);
            self.rebuild_snapshot();
            self.return_tokens(recycled);
            true
        } else {
            false
        }
    }

    /// Reap streams that are fully closed, or local-closed past `linger` without
    /// a peer FIN (zombie half-open streams under proxy short-connect load).
    ///
    /// The linger branch requires `pending_send() == 0` so that a stream with
    /// queued outbound data is not force-removed before its data is delivered.
    ///
    /// Returns stream ids that still need a wire FIN before/while being removed
    /// (`!fin_sent`). Callers should encode FIN for those ids, then treat them as
    /// gone from the map (this method already `remove`s + `close`s).
    ///
    /// Fully closed (`local && remote && fin_sent`) streams are removed with an
    /// empty contribution to the returned vec.
    pub fn reap_stale_streams(&self, linger: Duration) -> Vec<u32> {
        let mut streams = self.streams.lock();
        let mut need_fin = Vec::new();
        let mut to_remove = Vec::new();

        for (&id, s) in streams.iter() {
            let local = s.is_local_closed();
            let remote = s.is_remote_closed();
            let fin = s.is_fin_sent();

            if local && remote && fin {
                to_remove.push((id, false));
                continue;
            }

            if local && s.pending_send() == 0 {
                if let Some(elapsed) = s.local_closed_elapsed() {
                    if elapsed >= linger {
                        // Timed out waiting for peer FIN — force remove.
                        to_remove.push((id, !fin));
                    }
                }
            }
        }

        let removed_any = !to_remove.is_empty();
        let mut recycled = 0usize;
        for (id, wants_fin) in to_remove {
            if let Some(stream) = streams.remove(&id) {
                if wants_fin {
                    need_fin.push(id);
                }
                recycled += stream.recycle_tokens();
                stream.close();
            }
        }
        drop(streams);
        if removed_any {
            self.rebuild_snapshot();
        }
        // Reaped streams give their receive-window share back (Go's
        // `recycleTokens`).
        self.return_tokens(recycled);

        need_fin
    }

    /// Get the number of active streams.
    #[inline]
    pub fn stream_count(&self) -> usize {
        self.streams.lock().len()
    }

    /// Perform keepalive check — returns true if a ping should be sent.
    ///
    /// `keepalive_interval == 0` means keepalives are disabled (`Config::verify`
    /// allows `interval == 0` together with `timeout == 0`); without this guard
    /// the elapsed-time comparison is always true and every idle write-loop
    /// wake emits a NOP — with kcptun-common's 10 ms idle cadence both peers
    /// then ping-pong NOPs continuously (observed via frame tracing).
    pub fn check_keepalive(&self) -> bool {
        if self.config.keepalive_interval == 0 {
            return false;
        }
        let last = self.last_keepalive_ms.load(Ordering::Relaxed);
        let elapsed_ms = knet::mono_ms().saturating_sub(last);
        elapsed_ms >= self.keepalive_interval.as_millis() as u64
    }

    /// Update last inbound activity timestamp.
    pub fn update_activity(&self) {
        self.last_activity_ms
            .store(knet::mono_ms(), Ordering::Relaxed);
    }

    /// Mark that a keepalive NOP was just sent (resets the interval).
    pub fn mark_keepalive_sent(&self) {
        self.last_keepalive_ms
            .store(knet::mono_ms(), Ordering::Relaxed);
    }

    /// Returns true if no inbound activity within keepalive_timeout.
    ///
    /// Silence only counts as death while we are not the reason for it: once
    /// our receive window is drained the peer's writer is blocked on us and
    /// cannot send anything at all, so a quiet session is expected rather than
    /// dead. Go's `smux` keeps the same guard
    /// (`if atomic.LoadInt32(&s.bucket) > 0` — "recvLoop may block while bucket
    /// is 0, in this case, session should not be closed"); without it a slow
    /// local consumer tears down its own session exactly when a large download
    /// is buffering.
    pub fn is_keepalive_timeout(&self) -> bool {
        if self.config.keepalive_timeout == 0 {
            return false;
        }
        if !self.has_receive_capacity() {
            return false;
        }
        let last = self.last_activity_ms.load(Ordering::Relaxed);
        let elapsed_ms = knet::mono_ms().saturating_sub(last);
        elapsed_ms >= self.config.keepalive_timeout.saturating_mul(1000)
    }

    /// Configured keepalive timeout in seconds (0 = disabled).
    #[inline]
    pub fn keepalive_timeout_secs(&self) -> u64 {
        self.config.keepalive_timeout
    }

    /// Build a NOP keepalive frame (empty payload, stream id 0).
    pub fn keepalive_frame(&self) -> Frame {
        Frame::new(Cmd::Nop, 0, Bytes::new()).with_ver(self.config.version)
    }

    /// Prepare outbound SMUX frames by draining streams, encoding FINs for
    /// eligible closed streams, and appending any pending UPD frames.
    ///
    /// This is the unified outbound path for both client and server schedulers.
    /// It appends directly into the caller's `buf` (zero-copy from each stream's
    /// send buffer — one `extend_from_slice` per chunk on the flush path).
    ///
    /// - `max_bytes`: soft cap on total *payload* bytes to drain across streams.
    /// - `ver`: SMUX version (1 or 2) used for frame headers.
    ///
    /// Returns the stream IDs for which a FIN frame was encoded. The caller
    /// **must** call `mark_fin_sent(id)` on each of these **only after** the
    /// corresponding data has been successfully accepted by the transport
    /// (e.g., after `kcp.send` of the whole batch succeeds). This preserves the
    /// "can't lose FIN" invariant.
    ///
    /// The low-level `drain_send_max` / `emit_upd_frames` remain
    /// available for advanced integration; this method is the recommended
    /// single entry point for normal high-performance flush loops.
    pub fn prepare_outbound_into(&self, buf: &mut BytesMut, max_bytes: usize, ver: u8) -> Vec<u32> {
        self.prepare_outbound_into_controlled(buf, max_bytes, ver, true)
    }

    /// Prepare outbound frames, optionally deferring FIN emission.
    ///
    /// KCP-backed servers set `allow_fin=false` while previously drained KCP
    /// data is still unacknowledged, preventing FIN from overtaking echo tail
    /// data at Go clients.
    pub fn prepare_outbound_into_controlled(
        &self,
        buf: &mut BytesMut,
        max_bytes: usize,
        ver: u8,
        allow_fin: bool,
    ) -> Vec<u32> {
        let mut fin_streams = Vec::new();
        let mut drained_total = 0usize;

        // ── Lock-free COW snapshot traversal ──
        //
        // One `Mutex::lock().clone()` (a single atomic increment) gives us
        // a contiguous `Arc<[(u32, Arc<Stream>)]>` slice that we can iterate
        // without holding any lock.  This eliminates `process_data` ↔
        // `prepare_outbound` contention and replaces HashMap bucket
        // pointer-chasing (128+ L2/L3 misses for 64 streams) with linear,
        // prefetcher-friendly traversal (~8 L1 misses — one cache line
        // holds 8 entries at 8 bytes each).
        //
        // The snapshot is rebuilt by `rebuild_snapshot()` on every
        // open/close/remove — infrequent compared to flush cycles.
        let snapshot = self.stream_snapshot();

        // Per-stream share of this cycle's budget.
        //
        // Do NOT floor the share at `max_frame_size`: when `max_bytes <
        // n * max_frame_size` that lets the first stream in snapshot order
        // eat the entire budget and starve the rest. `drain_send_max` is
        // already capped per frame; the share only bounds how much one
        // stream may take across frames in this cycle.
        let share = if snapshot.is_empty() {
            max_bytes
        } else {
            (max_bytes / snapshot.len()).max(1)
        };

        // O12: the old four sequential passes (reclaim → UPD → drain → FIN)
        // walked this snapshot four times. All four are per-stream work with
        // only two ordering rules, both preserved here per stream:
        //   * UPD is emitted at the stream's turn, before its SYN/PSH
        //     (previously: all UPDs before any PSH — same per-stream order).
        //   * FIN is emitted only after that stream's own data drained
        //     (cross-stream frame order is not semantically significant:
        //     the peer routes frames by stream_id and only same-stream
        //     SYN→PSH→FIN ordering matters).
        // Budget exhaustion no longer aborts the walk ('outer break): the
        // remaining streams still do reclaim + UPD + FIN scanning, while
        // PSH/SYN emission for them is skipped — exactly what the old
        // pass-4 loop did after pass-3 broke out.
        let upd_frames = self.config.version >= 2;
        let mut reclaimed: usize = 0;
        let mut data_done = false;

        for &(id, ref s) in snapshot.iter() {
            // Pass 1 (reclaim): `take_return_tokens` is an atomic swap — no
            // per-stream lock needed. Session-level return happens once,
            // after the walk (nothing in between reads the session tokens).
            reclaimed += s.take_return_tokens();

            // Pass 2 (UPD, v2 only): same emission as
            // `emit_upd_frames_locked`, inlined for this walk.
            if upd_frames {
                if let Some((consumed, window)) = s.take_upd() {
                    Frame::encode_header_into(buf, ver, Cmd::Upd, id, 8);
                    buf.extend_from_slice(&consumed.to_le_bytes());
                    buf.extend_from_slice(&window.to_le_bytes());
                }
            }

            // Pass 3 (SYN + PSH drain), budget-limited.
            //
            // SYN is claimed from the same Stream object, in this same walk,
            // *before* any PSH for that stream. A session-level SYN queue
            // drained earlier raced with open_stream (insert+rebuild_snapshot
            // then queue_syn) and let PSH overtake SYN — the peer drops
            // unknown-stream data and KCP has already ACKed it.
            if !data_done {
                if s.syn_pending() && s.take_syn_pending() {
                    Frame::encode_header_into(buf, ver, Cmd::Syn, id, 0);
                }
                let mut drained_this_stream = 0usize;
                loop {
                    if drained_total >= max_bytes {
                        // Keep walking for reclaim/UPD/FIN, but no more
                        // PSH/SYN from here on (old: pass-3 'outer break).
                        data_done = true;
                        break;
                    }
                    if drained_this_stream >= share {
                        break;
                    }
                    // Cap each frame by both the configured frame size and this
                    // stream's remaining share / the cycle's remaining budget,
                    // so a small max_bytes cannot be overshot by a full frame.
                    let remaining_cycle = max_bytes - drained_total;
                    let remaining_share = share - drained_this_stream;
                    let cap = self
                        .config
                        .max_frame_size
                        .min(remaining_cycle)
                        .min(remaining_share);
                    if cap == 0 {
                        break;
                    }
                    let header_pos = buf.len();
                    Frame::encode_header_into(buf, ver, Cmd::Psh, id, 0);
                    let n = s.drain_send_max(buf, cap);
                    if n == 0 {
                        buf.truncate(header_pos);
                        break;
                    }
                    Frame::patch_header_length(buf, header_pos, n as u16);
                    drained_total += n;
                    drained_this_stream += n;
                }
            }

            // Pass 4 (FIN): after this stream's own data — same predicate and
            // SYN-before-FIN rule as the old separate pass.
            if allow_fin && s.is_local_closed() && s.pending_send() == 0 && !s.is_fin_sent() {
                // A stream that never got its SYN out this cycle (budget ran
                // out before reaching its drain) must not send FIN first.
                if s.syn_pending() && s.take_syn_pending() {
                    Frame::encode_header_into(buf, ver, Cmd::Syn, id, 0);
                }
                debug!("SMUX: prepare_outbound encoding FIN for stream {}", id);
                Frame::encode_header_into(buf, ver, Cmd::Fin, id, 0);
                fin_streams.push(id);
            }
        }

        if reclaimed > 0 {
            self.return_tokens(reclaimed);
        }

        fin_streams
    }

    /// Mark the given stream IDs as having had their FIN frame sent.
    ///
    /// Call this **after** the transport has accepted the bytes containing
    /// the corresponding FIN frames (e.g., after a successful `kcp.send` of
    /// the batch that included them). This is required to preserve the
    /// "can't lose FIN" rule and to allow proper linger/reap behavior.
    ///
    /// Unknown IDs are ignored.
    pub fn mark_fins_sent(&self, ids: &[u32]) {
        if ids.is_empty() {
            return;
        }
        let snapshot = self.stream_snapshot();
        // fin_ids is 0–2 entries per flush cycle; a linear scan over `ids`
        // (≤2 comparisons per stream) beats heap-allocating a HashSet.
        for &(stream_id, ref s) in snapshot.iter() {
            if ids.contains(&stream_id) {
                s.mark_fin_sent();
            }
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests;
