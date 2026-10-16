//! TCP segment checksum (RFC 1071) for the raw-TCP transport.
//!
//! Lives in its own module rather than inside `tcpraw.rs` (Linux-only) so the
//! arithmetic — and its equivalence to the original allocate-then-sum
//! implementation — is testable on every platform:
//!
//! ```text
//! cargo test -p knet-rs tcp_pkt
//! ```
//!
//! The module is therefore compiled for Linux builds (where `tcpraw` uses it)
//! and for test builds everywhere; on a non-Linux release build it is absent.

/// IP protocol number for TCP (`IPPROTO_TCP`), used in the pseudo-header.
const IPPROTO_TCP_NUM: u16 = 6;

/// The original implementation summed one contiguous buffer:
/// `pseudo-header(12) ‖ segment ‖ zero-pad-to-even` through
/// [`ones_complement_sum`]. Kept as the test oracle for [`tcp_checksum`].
#[cfg(test)]
pub(crate) fn ones_complement_sum(data: &[u8]) -> u32 {
    let mut sum: u32 = 0;
    let chunks = data.chunks_exact(2);
    let remainder = chunks.remainder();
    for chunk in chunks {
        sum += u16::from_be_bytes([chunk[0], chunk[1]]) as u32;
    }
    if remainder.len() == 1 {
        sum += (remainder[0] as u32) << 8;
    }
    while sum >> 16 != 0 {
        sum = (sum & 0xFFFF) + (sum >> 16);
    }
    sum
}

/// Overflow-free reference: same byte sequence, summed with a wide accumulator.
///
/// The original `u32` accumulator overflows past ~128 KB of segment (debug:
/// panic, release: wrap), which is unreachable for MTU-bounded segments but
/// makes it unusable as an oracle at that size. This one is exact everywhere.
#[cfg(test)]
pub(crate) fn tcp_checksum_reference_wide(
    src_ip: &[u8; 4],
    dst_ip: &[u8; 4],
    tcp_segment: &[u8],
) -> u16 {
    let mut sum: u128 = 0;
    sum += u16::from_be_bytes([src_ip[0], src_ip[1]]) as u128;
    sum += u16::from_be_bytes([src_ip[2], src_ip[3]]) as u128;
    sum += u16::from_be_bytes([dst_ip[0], dst_ip[1]]) as u128;
    sum += u16::from_be_bytes([dst_ip[2], dst_ip[3]]) as u128;
    sum += IPPROTO_TCP_NUM as u128;
    sum += (tcp_segment.len() as u16) as u128;
    let mut chunks = tcp_segment.chunks_exact(2);
    for chunk in &mut chunks {
        sum += u16::from_be_bytes([chunk[0], chunk[1]]) as u128;
    }
    if let [last] = chunks.remainder() {
        sum += (*last as u128) << 8;
    }
    while sum >> 16 != 0 {
        sum = (sum & 0xFFFF) + (sum >> 16);
    }
    !(sum as u16)
}

/// Reference: build the exact byte sequence the old code built, then sum it.
#[cfg(test)]
pub(crate) fn tcp_checksum_reference(
    src_ip: &[u8; 4],
    dst_ip: &[u8; 4],
    tcp_segment: &[u8],
) -> u16 {
    let mut buf = Vec::with_capacity(12 + tcp_segment.len() + 1);
    buf.extend_from_slice(src_ip);
    buf.extend_from_slice(dst_ip);
    buf.push(0);
    buf.push(6); // TCP
    let tcp_len = tcp_segment.len() as u16;
    buf.extend_from_slice(&tcp_len.to_be_bytes());
    buf.extend_from_slice(tcp_segment);
    if buf.len() % 2 != 0 {
        buf.push(0);
    }
    !ones_complement_sum(&buf) as u16
}

/// One's-complement checksum of a TCP segment, including the IPv4 pseudo-header.
///
/// Sums the pseudo-header words and the segment words in place instead of
/// concatenating them into a temporary buffer (one allocation plus one copy of
/// the whole segment per sent packet). Measured on a 1350-byte segment:
/// 104.9 ns → 54.9 ns per call, and 36.2 ns → 3.8 ns for an empty-payload
/// segment (where the allocation dominated) — see
/// `docs/evidence/a05-checksum.txt`.
///
/// The accumulator is `u32` (a `u64` accumulator measured ~40% slower here) and
/// the hot loop is branch-free: it is folded only at the end, which is safe for
/// any segment up to `MAX_WORDS_PER_FOLD` words (120 KB). Longer inputs fall
/// back to a periodically folded loop, so the function stays correct for
/// arbitrary slices.
#[inline]
pub(crate) fn tcp_checksum(src_ip: &[u8; 4], dst_ip: &[u8; 4], tcp_segment: &[u8]) -> u16 {
    // 60_000 words of 0xFFFF still fit in u32 (60_000 * 65_535 < u32::MAX).
    const MAX_WORDS_PER_FOLD: usize = 60_000;

    let mut sum: u32 = 0;
    // Pseudo-header, as big-endian 16-bit words:
    // src(4) ‖ dst(4) ‖ zero(1) ‖ proto(1) ‖ tcp_length(2).
    sum += u16::from_be_bytes([src_ip[0], src_ip[1]]) as u32;
    sum += u16::from_be_bytes([src_ip[2], src_ip[3]]) as u32;
    sum += u16::from_be_bytes([dst_ip[0], dst_ip[1]]) as u32;
    sum += u16::from_be_bytes([dst_ip[2], dst_ip[3]]) as u32;
    sum += IPPROTO_TCP_NUM as u32;
    // The old code wrote `tcp_segment.len() as u16`; keep that truncation
    // (segments are bounded by the MTU, so it only matters for absurd inputs).
    sum += (tcp_segment.len() as u16) as u32;

    let mut chunks = tcp_segment.chunks_exact(2);
    if tcp_segment.len() / 2 <= MAX_WORDS_PER_FOLD {
        for chunk in &mut chunks {
            sum += u16::from_be_bytes([chunk[0], chunk[1]]) as u32;
        }
    } else {
        for (i, chunk) in chunks.by_ref().enumerate() {
            sum += u16::from_be_bytes([chunk[0], chunk[1]]) as u32;
            if i & 0x3FFF == 0x3FFF {
                while sum >> 16 != 0 {
                    sum = (sum & 0xFFFF) + (sum >> 16);
                }
            }
        }
    }
    // Odd tail: the lone byte is the high half of a zero-padded word.
    if let [last] = chunks.remainder() {
        sum += (*last as u32) << 8;
    }

    while sum >> 16 != 0 {
        sum = (sum & 0xFFFF) + (sum >> 16);
    }
    !(sum as u16)
}

#[cfg(test)]
mod tests;
