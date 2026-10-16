use super::*;

/// Deterministic pseudo-random bytes (xorshift), so failures are reproducible.
fn payload(len: usize, seed: u32) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            (x >> 24) as u8
        })
        .collect()
}

/// The no-alloc sum must reproduce the original build-a-buffer-then-sum result
/// bit for bit, for every length class: empty, single byte (odd), even, odd,
/// and a maximum-size 2048-byte datagram.
#[test]
fn matches_the_reference_implementation_for_odd_and_even_lengths() {
    let src = [192, 168, 1, 2];
    let dst = [192, 168, 1, 3];
    // MTU-range sizes must match the original implementation byte for byte.
    for len in 0..=2048 {
        let seg = payload(len, 0x1234_5678 ^ len as u32);
        assert_eq!(
            tcp_checksum(&src, &dst, &seg),
            tcp_checksum_reference(&src, &dst, &seg),
            "length {len} vs original implementation"
        );
    }
    // Larger sizes (including the periodically-folded branch above 120 KB) are
    // checked against the overflow-free reference only — the original `u32`
    // accumulator cannot represent them.
    for len in [2047, 2048, 4096, 120_000, 121_000, 400_000] {
        let seg = payload(len, 0x1234_5678 ^ len as u32);
        assert_eq!(
            tcp_checksum(&src, &dst, &seg),
            tcp_checksum_reference_wide(&src, &dst, &seg),
            "length {len} vs wide reference"
        );
    }
}

/// Golden values produced by an independent RFC 1071 implementation (Python,
/// written from the spec, not from this code). Pins the pseudo-header layout:
/// `src ‖ dst ‖ zero ‖ proto(6) ‖ length`, big-endian 16-bit words.
#[test]
fn matches_independently_computed_vectors() {
    let mut hdr = vec![0x30, 0x39, 0x1F, 0x90];
    hdr.extend_from_slice(&[0, 0, 0, 1, 0, 0, 0, 0]); // seq / ack
    hdr.extend_from_slice(&[0x80, 0x18]); // data offset 8, PSH|ACK
    hdr.extend_from_slice(&65535u16.to_be_bytes()); // window
    hdr.extend_from_slice(&[0, 0, 0, 0]); // checksum placeholder
    hdr.extend_from_slice(&[1, 1, 8, 10]); // NOP NOP Timestamp
    hdr.extend_from_slice(&1234567u32.to_be_bytes());
    hdr.extend_from_slice(&[0, 0, 0, 0]);
    assert_eq!(hdr.len(), 32);

    assert_eq!(
        tcp_checksum(&[192, 168, 1, 2], &[192, 168, 1, 3], &hdr),
        0xccfb
    );
    let body = payload(32, 7);
    assert_eq!(tcp_checksum(&[10, 0, 0, 1], &[10, 0, 0, 2], &body), 0x1beb);
    let mut odd = body.clone();
    odd.push(0xff);
    assert_eq!(tcp_checksum(&[10, 0, 0, 1], &[10, 0, 0, 2], &odd), 0x1ce9);
    assert_eq!(
        tcp_checksum(&[127, 0, 0, 1], &[127, 0, 0, 1], &[0xab]),
        0x56f5
    );
    assert_eq!(tcp_checksum(&[1, 2, 3, 4], &[5, 6, 7, 8], &[]), 0xefe5);
}

/// A checksum computed with a zeroed checksum field, inserted at bytes 16..18,
/// must make the receiver recompute zero — the property the kernel and the peer
/// rely on. (The field must be zero while summing; re-summing a segment that
/// carried a non-zero placeholder does not fold to zero.)
#[test]
fn inserted_checksum_verifies_to_zero() {
    let src = [172, 16, 0, 9];
    let dst = [172, 16, 0, 1];
    for len in [0usize, 1, 2, 31, 32, 33, 1326, 2048 - 32] {
        let mut seg = payload(32 + len, 99 + len as u32);
        seg[16..18].copy_from_slice(&[0, 0]);
        let csum = tcp_checksum(&src, &dst, &seg);
        seg[16..18].copy_from_slice(&csum.to_be_bytes());
        assert_eq!(
            tcp_checksum(&src, &dst, &seg),
            0,
            "length {len}: the inserted checksum must fold to zero"
        );
    }
}
