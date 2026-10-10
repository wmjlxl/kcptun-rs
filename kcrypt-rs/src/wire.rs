//! Zero-allocation encryption/decryption helpers for the kcptun wire format.
//!
//! # Authentication (P1-9 / M-11)
//!
//! Only `aes-128-gcm` is an AEAD. Every CFB-family cipher (aes, sm4, tea,
//! xtea, salsa20, blowfish, twofish, cast5, 3des) and `xor` are covered by a
//! CRC32 only — a network attacker can forge packets. `none`/`null` have no
//! protection at all. This matches Go kcptun's wire format and cannot change
//! without breaking compatibility. Prefer `aes-128-gcm` in production.
//!
//! The Go kcp-go v5 CFB wire format is:
//!   `[nonce 16B][CRC32 4B][ciphertext]`
//!
//! This module keeps the per-packet cost low with:
//! - A reusable `BytesMut` buffer (no heap allocation per packet)
//! - `Bytes` return type (reference-counted, zero-copy send to tokio tasks)
//! - [`crate::nonce::NonceGen`] for the 16-byte nonce: one AES block per
//!   packet, no syscall, same construction as Go's `nonceAES128`
//!
//! ## Nonce design
//!
//! The nonce is the output of a per-instance keyed PRF over a counter, so it
//! is unpredictable and non-repeating. It must be both: `salsa20` uses the
//! first eight nonce bytes as its stream nonce, and CFB chains the nonce
//! block into the keystream (`K₂ = E(nonce ⊕ E(IV))`). A plain counter — the
//! previous design — repeated across the two ends of a session, across
//! `--conn` channels and across restarts, which is keystream reuse for
//! salsa20 and deterministic ciphertext for CFB. See [`crate::nonce`].
//!
//! `session_id` is retained as the PRF domain separator (data path vs. ACK
//! path), not as the nonce's only source of diversity.

use bytes::{Bytes, BytesMut};

use crate::crypt::{AeadCrypt, BlockCrypt, CryptEngine};
use crate::nonce::NonceGen;

/// Crypto header size: `[nonce 16B][CRC32 4B]`.
pub const CRYPTO_HEADER_SIZE: usize = 20;
/// Nonce size.
pub const NONCE_SIZE: usize = 16;

/// Deprecated alias for [`CRYPTO_HEADER_SIZE`].
#[deprecated(note = "renamed to CRYPTO_HEADER_SIZE")]
pub const CRYPT_HDR: usize = CRYPTO_HEADER_SIZE;
/// Deprecated alias for [`NONCE_SIZE`].
#[deprecated(note = "renamed to NONCE_SIZE")]
pub const NONCE_SZ: usize = NONCE_SIZE;

/// Inbound CFB decrypt / header-strip failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InboundCryptError {
    /// Packet shorter than crypto header (or empty after probe).
    #[error("packet shorter than crypto header")]
    Short,
    /// CRC32 of payload does not match the header field.
    #[error("payload CRC32 does not match crypto header")]
    CrcMismatch,
}

/// Decrypt a CFB wire packet **in place** and return a slice of the plaintext body.
///
/// Layout after decrypt: `[nonce 16B][CRC32 4B][payload…]`. On success the
/// returned slice is `&buf[CRYPTO_HEADER_SIZE..]` — **no** heap alloc and **no** copy
/// into `CryptoBuf.enc_buf`.
///
/// `probe_header`: when `true` (server historical/compat path), if byte 4 is a
/// KCP cmd (`0x51..=0x54`) the whole buffer is treated as already-plaintext
/// with **no** crypto header. When `false` (client / normal Go CFB), always
/// require and strip the 20B header after CRC check.
///
/// Callers must finish using the returned slice (e.g. `KCP::input`) before
/// overwriting `buf` again.
#[inline]
pub fn decrypt_cfb_in_place<'a>(
    buf: &'a mut [u8],
    crypt: &CryptEngine,
    probe_header: bool,
) -> Result<&'a [u8], InboundCryptError> {
    if buf.is_empty() {
        return Err(InboundCryptError::Short);
    }
    crypt.decrypt(buf);
    strip_cfb_header_if_present(buf, probe_header)
}

/// After CFB decrypt (or for an already-plaintext buffer), optionally detect
/// and strip the 20B crypto header. See [`decrypt_cfb_in_place`].
#[inline]
pub fn strip_cfb_header_if_present(
    buf: &[u8],
    probe_header: bool,
) -> Result<&[u8], InboundCryptError> {
    if probe_header {
        if buf.len() > CRYPTO_HEADER_SIZE {
            let cmd = buf[4];
            let has_header = cmd != 0x51 && cmd != 0x52 && cmd != 0x53 && cmd != 0x54;
            if !has_header {
                return Ok(buf);
            }
        } else if buf.len() >= 5 {
            let cmd = buf[4];
            if cmd == 0x51 || cmd == 0x52 || cmd == 0x53 || cmd == 0x54 {
                return Ok(buf);
            }
            return Err(InboundCryptError::Short);
        } else {
            return Err(InboundCryptError::Short);
        }
    } else if buf.len() <= CRYPTO_HEADER_SIZE {
        return Err(InboundCryptError::Short);
    }

    let stored_crc = u32::from_le_bytes(
        buf[NONCE_SIZE..CRYPTO_HEADER_SIZE]
            .try_into()
            .map_err(|_| InboundCryptError::Short)?,
    );
    let computed_crc = crc32fast::hash(&buf[CRYPTO_HEADER_SIZE..]);
    if stored_crc != computed_crc {
        return Err(InboundCryptError::CrcMismatch);
    }
    Ok(&buf[CRYPTO_HEADER_SIZE..])
}

/// null cipher inbound view — identity slice (no crypto header).
#[inline]
pub fn inbound_null(buf: &[u8]) -> &[u8] {
    buf
}

/// A reusable encryption buffer with a monotonic nonce counter.
///
/// Designed to be held inside a `Mutex` or `parking_lot::Mutex` and called
/// from a single logical encryption path. The buffer is reused across
/// packets, eliminating per-packet `vec![]` allocation.
///
/// ## Buffer reuse strategy
/// - `enc_buf`: Used for CFB (encrypt_cfb / prepare_encrypt).
///   Capacity is retained across calls; we only clear + reserve, never shrink to zero.
/// - `aead_buf`: Dedicated AEAD seal buffer (seal_aead). Reused across flush cycles so
///   `seal_into` never allocates after warmup. One per `CryptoBuf` (i.e., per logical path).
pub struct CryptoBuf {
    /// Reusable encryption buffer — capacity is retained across calls.
    /// We only ever clear() + reserve(), never let it shrink to zero.
    /// See [`CryptoBuf`] documentation for the full reuse strategy.
    enc_buf: BytesMut,
    /// Reusable AEAD seal buffer — capacity retained across flush cycles.
    /// One buffer per `CryptoBuf` (i.e. per logical encrypt path) so `seal_into`
    /// never allocates after the first use. Used by [`CryptoBuf::seal_aead`].
    aead_buf: BytesMut,
    /// Per-instance nonce source (keyed PRF over a counter).
    nonce: NonceGen,
}

impl CryptoBuf {
    /// Create a new `CryptoBuf`.
    ///
    /// `session_id` separates the PRF domains of two paths that share a
    /// process (the data path and the ACK path); nonce unpredictability comes
    /// from the generator's random key, not from this value.
    pub fn new(session_id: u64) -> Self {
        CryptoBuf {
            enc_buf: BytesMut::with_capacity(2048),
            aead_buf: BytesMut::with_capacity(2048),
            nonce: NonceGen::new(session_id),
        }
    }

    /// Encrypt `data` using the CFB wire format, returning a `Bytes` that
    /// is reference-counted (zero-copy clone for tokio::spawn).
    ///
    /// Layout: `[nonce 16B][CRC32 4B][ciphertext]`
    ///
    /// This method reuses the internal buffer — no `vec![]` allocation
    /// occurs per packet. The returned `Bytes` shares the underlying
    /// allocation via reference counting.
    ///
    /// ## Implementation notes
    /// - Uses `extend_from_slice` (single O(n) write) instead of `resize(total, 0)`
    ///   followed by `copy_from_slice`. The zero-fill was immediately overwritten.
    /// - The nonce comes from [`NonceGen`]: one AES block per packet, as in
    ///   Go's `nonceAES128`. It must be unpredictable, not merely unique —
    ///   CFB feeds the nonce block into the keystream.
    #[inline]
    pub fn encrypt_cfb(&mut self, data: &[u8], crypt: &CryptEngine) -> Bytes {
        let total = CRYPTO_HEADER_SIZE + data.len();
        // Grow only when the remaining tail can't hold this packet. `split_to`
        // hands out the packet and leaves the tail for the next one, so a
        // large target amortizes mallocs across several packets.
        //
        // (bytes ≥1.12 `BytesMut::copy_to_bytes` is just `split_to().freeze()`;
        // the old per-packet malloc came from `reserve(total + SPARE)` after
        // `split_to` left only `SPARE` behind.)
        self.enc_buf.clear();
        if self.enc_buf.capacity() < total {
            self.enc_buf.reserve(total.saturating_mul(8).max(16384));
        }

        // Build via extend (one O(n) write) — avoid resize(total, 0) zero-fill
        // that would immediately be overwritten.
        self.enc_buf.extend_from_slice(&self.nonce.next());
        let crc = crc32fast::hash(data);
        self.enc_buf.extend_from_slice(&crc.to_le_bytes());
        self.enc_buf.extend_from_slice(data);
        debug_assert_eq!(self.enc_buf.len(), total);

        crypt.encrypt(&mut self.enc_buf[..total]);

        // split_to keeps the tail capacity on enc_buf; the returned Bytes only
        // holds [0, total) of the slab. Subsequent writes go into the tail and
        // never alias the frozen prefix.
        self.enc_buf.split_to(total).freeze()
    }

    /// Encrypt a batch of CFB packets into one reusable slab.
    ///
    /// One allocation for the whole batch: each packet is a `split_to` slice
    /// sharing the same backing store. Use this instead of looping
    /// [`Self::encrypt_cfb`] so a flush does not malloc per datagram.
    pub fn encrypt_cfb_batch(
        &mut self,
        packets: &[Bytes],
        crypt: &CryptEngine,
        out: &mut Vec<Bytes>,
    ) {
        const MIN_TAIL: usize = 4096;
        let total_wire: usize = packets.iter().map(|p| CRYPTO_HEADER_SIZE + p.len()).sum();
        self.enc_buf.clear();
        let needed = total_wire.saturating_add(MIN_TAIL);
        if self.enc_buf.capacity() < needed {
            self.enc_buf.reserve(needed);
        }
        for data in packets {
            let total = CRYPTO_HEADER_SIZE + data.len();
            self.enc_buf.extend_from_slice(&self.nonce.next());
            let crc = crc32fast::hash(data);
            self.enc_buf.extend_from_slice(&crc.to_le_bytes());
            self.enc_buf.extend_from_slice(data);
            debug_assert_eq!(self.enc_buf.len(), total);
            crypt.encrypt(&mut self.enc_buf[..total]);
            out.push(self.enc_buf.split_to(total).freeze());
        }
    }

    /// Encrypt one packet with the standard 20B CFB wire format.
    ///
    /// All BlockCrypt ciphers (including xor/salsa20) use the same wire layout
    /// as Go kcptun: `[nonce 16B][CRC32 4B][ciphertext]`.
    /// The cipher impl (xor/salsa20) handles the nonce offset internally —
    /// see [`Salsa20Crypt`] / [`SimpleXORCrypt`] for per-cipher details.
    ///
    /// AEAD ciphers (aes-128-gcm) should use [`seal_aead`] instead.
    #[inline]
    pub fn encrypt_packet(&mut self, data: &[u8], crypt: &CryptEngine) -> Bytes {
        self.encrypt_cfb(data, crypt)
    }

    /// AEAD seal using the session-reused `aead_buf` (no per-flush `BytesMut::new`).
    #[inline]
    pub fn seal_aead(&mut self, aead: &dyn AeadCrypt, data: &[u8]) -> Bytes {
        aead.seal_into(data, &mut self.aead_buf)
    }

    /// Prepare the encryption buffer (nonce + plaintext copy) WITHOUT CRC or
    /// encrypt. CRC is filled later by [`finalize_encrypt_packet`] so large
    /// batches can compute CRC + encrypt in parallel.
    ///
    /// Layout after prepare (CRC slot is zeroed placeholder):
    ///   `[nonce 16B][CRC placeholder 4B][plaintext]`
    ///
    /// This enables: prepare all packets serially (shared nonce counter), then
    /// CRC + encrypt in parallel across threads (both are stateless).
    #[inline]
    pub fn prepare_encrypt(&mut self, data: &[u8]) -> BytesMut {
        let total = CRYPTO_HEADER_SIZE + data.len();
        // Same amortized grow as `encrypt_cfb`: only when the tail can't hold
        // this packet. `split_to` leaves the tail on `enc_buf`.
        self.enc_buf.clear();
        if self.enc_buf.capacity() < total {
            self.enc_buf.reserve(total.saturating_mul(8).max(16384));
        }

        let n = self.nonce.next();
        self.enc_buf.extend_from_slice(&n);
        // CRC placeholder — filled by finalize_encrypt_packet before encrypt.
        self.enc_buf.extend_from_slice(&[0u8; 4]);
        self.enc_buf.extend_from_slice(data);
        debug_assert_eq!(self.enc_buf.len(), total);

        self.enc_buf.split_to(total)
    }

    /// Fill CRC32 of the plaintext payload then encrypt in place.
    ///
    /// Expects a buffer from [`prepare_encrypt`]:
    ///   `[nonce 16B][CRC placeholder 4B][plaintext…]`
    /// After this call the buffer is a ready-to-send wire packet.
    /// Wire-compatible with Go: CRC is over plaintext only (before encrypt).
    #[inline]
    pub fn finalize_encrypt_packet(buf: &mut [u8], crypt: &CryptEngine) {
        debug_assert!(buf.len() >= CRYPTO_HEADER_SIZE);
        let crc = crc32fast::hash(&buf[CRYPTO_HEADER_SIZE..]);
        buf[NONCE_SIZE..CRYPTO_HEADER_SIZE].copy_from_slice(&crc.to_le_bytes());
        crypt.encrypt(buf);
    }

    /// Decrypt `data` in place, verify CRC32, and **copy** the payload into
    /// this buffer's reusable `enc_buf`, returning it as `Bytes`.
    ///
    /// Prefer [`decrypt_cfb_in_place`] on the hot inbound path when the
    /// plaintext is only needed for a synchronous `KCP::input` — that path
    /// avoids this second payload copy. Keep this method when the caller
    /// needs an owned `Bytes` that outlives the receive buffer.
    ///
    /// On CRC mismatch or short data, returns `None`.
    #[inline]
    pub fn decrypt_cfb(&mut self, data: &mut [u8], crypt: &CryptEngine) -> Option<Bytes> {
        let body = decrypt_cfb_in_place(data, crypt, false).ok()?;
        let payload_len = body.len();
        self.enc_buf.clear();
        if self.enc_buf.capacity() < payload_len {
            self.enc_buf
                .reserve(payload_len.saturating_mul(8).max(16384));
        }
        self.enc_buf.extend_from_slice(body);
        Some(self.enc_buf.split_to(payload_len).freeze())
    }
}

/// `cpu_block` offload thresholds (scheduling only — not wire format).
///
/// Tokio is the sole runtime; thresholds are tuned for its multi-worker
/// scheduler. Env overrides still apply (see `KCPTUN_*` env vars).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffloadProfile {
    Tokio,
}

/// Optional env overrides. Non-profile keys cache via OnceLock.
/// Heavy8 keys: env if set, else **profile-dependent** default (not OnceLock'd
/// to the wrong profile's default).
///
/// | Env | Affects |
/// |-----|---------|
/// | `KCPTUN_COMPRESS_CPU_BLOCK_BYTES` | Snappy offload size (default 16KiB) |
/// | `KCPTUN_FAST_ENCRYPT_MIN_PKTS` / `_BYTES` | xor/salsa/AEAD |
/// | `KCPTUN_HEAVY8_ENCRYPT_MIN_PKTS` / `_BYTES` | cast5/3des/blowfish/tea/xtea |
fn env_usize(name: &'static str, default: usize) -> usize {
    match name {
        "KCPTUN_COMPRESS_CPU_BLOCK_BYTES" => {
            static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
            *V.get_or_init(|| parse_env_usize(name, default))
        }
        "KCPTUN_FAST_ENCRYPT_MIN_PKTS" => {
            static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
            *V.get_or_init(|| parse_env_usize(name, default))
        }
        "KCPTUN_FAST_ENCRYPT_MIN_BYTES" => {
            static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
            *V.get_or_init(|| parse_env_usize(name, default))
        }
        _ => parse_env_usize(name, default),
    }
}

/// Env override if present; otherwise `default` (may depend on profile).
fn env_or_profile(name: &str, default: usize) -> usize {
    static HEAVY_PKTS: std::sync::OnceLock<Option<usize>> = std::sync::OnceLock::new();
    static HEAVY_BYTES: std::sync::OnceLock<Option<usize>> = std::sync::OnceLock::new();
    let cached =
        match name {
            "KCPTUN_HEAVY8_ENCRYPT_MIN_PKTS" => HEAVY_PKTS
                .get_or_init(|| std::env::var(name).ok().and_then(|s| s.trim().parse().ok())),
            "KCPTUN_HEAVY8_ENCRYPT_MIN_BYTES" => HEAVY_BYTES
                .get_or_init(|| std::env::var(name).ok().and_then(|s| s.trim().parse().ok())),
            _ => &None,
        };
    cached.unwrap_or(default).max(1)
}

fn parse_env_usize(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(s) => s.trim().parse::<usize>().unwrap_or(default).max(1),
        Err(_) => default,
    }
}

/// Decide whether a batch encrypt should be offloaded to `cpu_block`.
///
/// Heavy-8 defaults depend on `profile`:
/// Tokio: 1 pkt / 512 B (protect multi-worker async from CFB-8).
/// Fast/AEAD and other CFB thresholds are shared. Env overrides still apply.
#[inline]
pub fn should_cpu_block_encrypt(
    has_encryption: bool,
    has_aead: bool,
    packet_count: usize,
    total_bytes: usize,
    crypt: &CryptEngine,
    _profile: OffloadProfile,
) -> bool {
    // Null cipher: the "encrypt" is `out.extend(packets)` — just moving Bytes
    // references (pointer copies). cpu_block dispatch costs more.
    if !has_encryption && !has_aead {
        return false;
    }
    let cname = crypt.name();

    // Fast ciphers / AEAD: offload only large batches to protect the async
    // worker from long inline encrypt + compress combinations.
    if matches!(cname, "xor" | "salsa20" | "salsa") || has_aead {
        let min_pkts = env_usize("KCPTUN_FAST_ENCRYPT_MIN_PKTS", 8);
        let min_bytes = env_usize("KCPTUN_FAST_ENCRYPT_MIN_BYTES", 8192);
        return packet_count >= min_pkts || total_bytes >= min_bytes;
    }

    // Heavy 8-byte CFB ciphers (cast5, 3des, blowfish, tea, xtea).
    if matches!(cname, "cast5" | "3des" | "blowfish" | "tea" | "xtea") {
        let (def_pkts, def_bytes) = (1, 512);
        let min_pkts = env_or_profile("KCPTUN_HEAVY8_ENCRYPT_MIN_PKTS", def_pkts);
        let min_bytes = env_or_profile("KCPTUN_HEAVY8_ENCRYPT_MIN_BYTES", def_bytes);
        return packet_count >= min_pkts || total_bytes >= min_bytes;
    }

    // Remaining CFB (AES software path, SM4, Twofish, ...): moderate threshold.
    packet_count >= 4 || total_bytes >= 4096
}

/// Decide whether inbound decrypt + KCP input + SMUX processing should be
/// offloaded to `cpu_block`.
///
/// - null/none: never
/// - AEAD: ≥4 KiB
/// - CFB: ≥512 B
#[inline]
pub fn should_cpu_block_decrypt(
    has_encryption: bool,
    has_aead: bool,
    data_len: usize,
    _profile: OffloadProfile,
) -> bool {
    if !has_encryption && !has_aead {
        false
    } else if has_aead {
        data_len >= 4096
    } else {
        let thr = 512;
        data_len >= thr
    }
}

/// Size threshold for session-level Snappy offload (default ≥16 KiB).
///
/// Previous threshold (64 KiB) was too high: inline Snappy + inline
/// encrypt ("double-inline") starves UDP/ACK processing on fast ciphers
/// (xor/salsa) + compression. Threshold sweep (2 MiB random, 3 runs):
///   16 KiB → +22.9% vs 64 KiB; null-comp control → +72.8%.
///
/// Override with `KCPTUN_COMPRESS_CPU_BLOCK_BYTES`.
#[inline]
pub fn should_cpu_block_compress(uncompressed_bytes: usize) -> bool {
    let thr = env_usize("KCPTUN_COMPRESS_CPU_BLOCK_BYTES", 16384);
    uncompressed_bytes >= thr
}

/// Encrypt a batch of raw KCP segments for the wire (P0.1 / P0.5).
///
/// Input packets are `Bytes` (reference-counted) — the KCP output callback
/// now hands ownership directly (P1.1 R2), avoiding a per-packet `Vec` alloc
/// + `extend_from_slice` copy in the output path.
///
/// - AEAD ([`CryptEngine::Aes128Gcm`]): `seal_into` each packet
/// - CFB (`has_encryption`): serial `encrypt_cfb` for cheap/small batches;
///   large heavy-cipher batches use serial `prepare_encrypt` then parallel
///   `finalize_encrypt_packet` (CRC + encrypt). `allow_parallel=false` when
///   already on a `cpu_block` worker (no nested fan-out)
/// - null (`!has_encryption`): move `Bytes` straight through (no crypto header)
///
/// `crypt` is concrete [`CryptEngine`] so encrypt/decrypt use enum match
/// dispatch (no `dyn` vtable on the hot path). `has_encryption` remains a
/// separate flag so `"none"` (header + identity cipher) still packs CFB
/// headers while `"null"` does not.
pub fn encrypt_batch(
    packets: Vec<Bytes>,
    crypt: &CryptEngine,
    crypto_buf: &parking_lot::Mutex<CryptoBuf>,
    has_encryption: bool,
    // When false, never spawn thread::scope workers (already on a cpu_block
    // pool thread — nested parallelism thrashes cores under multi-session load).
    allow_parallel: bool,
) -> Vec<Bytes> {
    let mut out = Vec::with_capacity(packets.len());
    encrypt_batch_ref_into(
        &packets,
        crypt,
        crypto_buf,
        has_encryption,
        allow_parallel,
        &mut out,
    );
    out
}

/// Whether in-process `thread::scope` parallel encrypt is worth it for this batch.
///
/// Spawn/join costs tens of µs. Cheap ciphers finish a whole flush faster than
/// that, so parallelism is a net loss (seen as tokio `none/comp` / medium xor
/// regressions). Heavy software CFB still benefits once the batch is large.
#[inline]
fn should_parallel_cfb_encrypt(
    crypt: &CryptEngine,
    allow_parallel: bool,
    packet_count: usize,
    total_bytes: usize,
) -> bool {
    if !allow_parallel || packet_count < 2 {
        return false;
    }
    let name = crypt.name();
    // Identity / stream XOR: encrypt is near-free; CRC+copy is the work.
    // Never pay thread spawn — always serial encrypt_cfb.
    if matches!(name, "none" | "xor" | "salsa20" | "salsa") {
        return false;
    }
    // Heavy pure-software CFB-8 / SM4 / Twofish: parallel helps, but only when
    // the batch amortizes spawn (not every 4-packet flush).
    if matches!(
        name,
        "cast5" | "3des" | "blowfish" | "tea" | "xtea" | "sm4" | "twofish"
    ) {
        return packet_count >= 8 || total_bytes >= 8192;
    }
    // AES CFB (and remaining): hardware block encrypt is fast; need a larger
    // batch than the old "≥4 packets" gate or we thrash cores for free.
    packet_count >= 8 || total_bytes >= 16384
}

/// Encrypt a batch into a caller-owned `out` buffer (cleared first).
///
/// Flush loops should reuse one `Vec<Bytes>` across cycles to avoid per-flush
/// allocation of the result vector (P0.4). Contents are `Bytes` (refcounted);
/// `out.clear()` drops refs but keeps the `Vec` capacity.
///
/// Takes `Vec<Bytes>` by value — the null path moves packets straight into
/// `out` without cloning. Use [`encrypt_batch_ref_into`] when you already have
/// a `&[Bytes]` slice and want to avoid the `to_vec()` allocation.
pub fn encrypt_batch_into(
    packets: Vec<Bytes>,
    crypt: &CryptEngine,
    crypto_buf: &parking_lot::Mutex<CryptoBuf>,
    has_encryption: bool,
    allow_parallel: bool,
    out: &mut Vec<Bytes>,
) {
    encrypt_batch_ref_into(
        &packets,
        crypt,
        crypto_buf,
        has_encryption,
        allow_parallel,
        out,
    );
}

/// Same as [`encrypt_batch_into`] but takes `&[Bytes]` instead of `Vec<Bytes>`,
/// avoiding the `to_vec()` allocation on the sync TX path (`encrypt_sync`).
///
/// In the null path, `Bytes` clones are cheap (refcount bumps) so the
/// difference vs `encrypt_batch_into` is negligible.
pub fn encrypt_batch_ref_into(
    packets: &[Bytes],
    crypt: &CryptEngine,
    crypto_buf: &parking_lot::Mutex<CryptoBuf>,
    has_encryption: bool,
    allow_parallel: bool,
    out: &mut Vec<Bytes>,
) {
    out.clear();
    if out.capacity() < packets.len() {
        out.reserve(packets.len() - out.capacity());
    }
    if let Some(aead) = crypt.as_aead() {
        // Reuse CryptoBuf.aead_buf across flush cycles (not just within a batch).
        let mut cb = crypto_buf.lock();
        for data in packets {
            out.push(cb.seal_aead(aead, data));
        }
    } else if has_encryption {
        // Standard CFB path for ALL BlockCrypt ciphers (including xor/salsa20):
        // wire format is [nonce 16B][CRC32 4B][ciphertext], matching Go kcptun.
        // The cipher impl handles the nonce offset internally — salsa20 reads
        // bytes 0-8 as the nonce, xor XORs the entire buffer.
        //
        // Parallelism is cipher- and size-gated (see should_parallel_cfb_encrypt):
        // cheap ciphers stay serial; expensive ones only fan out on large batches.
        let total_bytes: usize = packets.iter().map(|p| p.len()).sum();
        let use_parallel =
            should_parallel_cfb_encrypt(crypt, allow_parallel, packets.len(), total_bytes);
        if !use_parallel {
            let mut cb = crypto_buf.lock();
            cb.encrypt_cfb_batch(packets, crypt, out);
        } else {
            // Cap workers: more than 4 threads rarely helps one session's encrypt
            // and increases join latency on high-core hosts.
            let nthreads = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
                .min(packets.len())
                .clamp(2, 4);
            // Phase 1: Prepare all packets (serial — shared nonce counter only).
            // CRC is deferred to phase 2 so it can run in parallel with encrypt.
            let prepared: Vec<BytesMut> = {
                let mut cb = crypto_buf.lock();
                packets
                    .iter()
                    .map(|data| cb.prepare_encrypt(data))
                    .collect()
            };
            // Phase 2: CRC + encrypt in parallel (both are stateless / per-packet).
            //
            // NOTE: This path is rarely reached in production. For heavy CFB
            // ciphers, should_cpu_block_encrypt returns true at ≥1 packet / 512 B
            // (heavy 8-byte) or ≥8 packets / 8 KiB (fast/AES), routing through
            // cpu_block with allow_parallel=false. When allow_parallel=false,
            // should_parallel_cfb_encrypt always returns false, so the serial
            // path is used. This parallel path only triggers when the caller
            // explicitly sets allow_parallel=true (e.g. force_inline mode).
            let chunk_size = prepared.len().div_ceil(nthreads);
            let mut iter = prepared.into_iter();
            std::thread::scope(|s| {
                let mut handles = Vec::new();
                loop {
                    let chunk: Vec<BytesMut> = (&mut iter).take(chunk_size).collect();
                    if chunk.is_empty() {
                        break;
                    }
                    // CryptEngine enum dispatch (no dyn vtable).
                    // For AES CFB the encrypt call goes through
                    // aes::BlockEncrypt::encrypt_blocks for ILP on AES-NI/ARMv8.
                    handles.push(s.spawn(move || {
                        let mut r = Vec::with_capacity(chunk.len());
                        for mut buf in chunk {
                            // CRC over plaintext then encrypt — same wire layout as
                            // serial encrypt_cfb / Go postProcess.
                            CryptoBuf::finalize_encrypt_packet(&mut buf, crypt);
                            r.push(buf.freeze());
                        }
                        r
                    }));
                }
                // Use unwrap_or_default so a worker panic doesn't propagate
                // into the flush loop and kill the session.
                for h in handles {
                    if let Ok(chunk_out) = h.join() {
                        out.extend(chunk_out);
                    }
                }
            });
        }
    } else {
        // null: Bytes pass straight through (no crypto header, no copy).
        // Clone is cheap — Bytes is refcounted.
        out.extend(packets.iter().cloned());
    }
}

#[cfg(test)]
mod tests;
