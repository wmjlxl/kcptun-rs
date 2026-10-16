use super::*;
use std::io::Write;

#[test]
fn decodes_a_valid_block_split_across_feeds() {
    let mut encoder = snap::write::FrameEncoder::new(Vec::new());
    encoder.write_all(&vec![b'a'; 64 * 1024]).unwrap();
    let wire = encoder.into_inner().unwrap();
    let mut decoder = SnappyStreamDecoder::new();
    let mut decoded = Vec::new();
    for chunk in wire.chunks(17) {
        decoded.extend(decoder.feed(chunk).unwrap());
    }
    assert_eq!(decoded, vec![b'a'; 64 * 1024]);
}

#[test]
fn accepts_highly_compressible_chunks_in_one_transport_read() {
    let plain = vec![0u8; 17 * MAX_BLOCK];
    let mut encoder = snap::write::FrameEncoder::new(Vec::new());
    encoder.write_all(&plain).unwrap();
    let wire = encoder.into_inner().unwrap();
    assert!(wire.len() < 64 * 1024);

    let mut decoder = SnappyStreamDecoder::new();
    assert_eq!(decoder.feed(&wire).unwrap(), plain);

    let mut decoder = SnappyStreamDecoder::new();
    let mut streamed = Vec::new();
    let mut chunks = 0;
    decoder
        .feed_chunks(&wire, |chunk| {
            assert!(chunk.len() <= MAX_BLOCK);
            streamed.extend_from_slice(chunk);
            chunks += 1;
        })
        .unwrap();
    assert_eq!(chunks, 17);
    assert_eq!(streamed, plain);
}

#[test]
fn rejects_corrupt_checksums_for_both_data_chunk_types() {
    for plain in [b"audit crc sample".as_slice(), &[0u8; MAX_BLOCK][..]] {
        let mut encoder = snap::write::FrameEncoder::new(Vec::new());
        encoder.write_all(plain).unwrap();
        let wire = encoder.into_inner().unwrap();
        assert!(matches!(wire[10], 0x00 | 0x01));
        let mut bad_checksum = wire.clone();
        bad_checksum[14] ^= 1;

        let mut decoder = SnappyStreamDecoder::new();
        assert_eq!(
            decoder.feed(&bad_checksum).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        if wire[10] == 0x01 {
            let mut bad_payload = wire;
            *bad_payload.last_mut().unwrap() ^= 1;
            let mut decoder = SnappyStreamDecoder::new();
            assert_eq!(
                decoder.feed(&bad_payload).unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
        }
    }
}

#[test]
fn rejects_oversized_data_chunk_before_buffering_body() {
    let mut decoder = SnappyStreamDecoder::new();
    decoder.feed(b"\xff\x06\x00\x00sNaPpY").unwrap();
    // Uncompressed chunks carry at most 64 KiB plus a 4-byte checksum.
    let oversized = 65_541u32.to_le_bytes();
    assert_eq!(
        decoder
            .feed(&[0x01, oversized[0], oversized[1], oversized[2]])
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
}

/// Two back-to-back maximum-size uncompressed chunks, split so that almost a
/// whole chunk is still buffered when the remainder arrives. A per-*feed* input
/// budget that ignores the retained prefix rejects this legal stream with
/// `InvalidData` and the session dies.
#[test]
fn accepts_two_max_chunks_split_across_feeds() {
    const BODY: usize = MAX_BLOCK + 4;
    let chunk = |byte: u8| {
        let mut c = vec![0x01, 0x04, 0x00, 0x01]; // type + 65_540 LE
        c.extend_from_slice(&masked_crc32c(&vec![byte; MAX_BLOCK]).to_le_bytes());
        c.extend(std::iter::repeat_n(byte, MAX_BLOCK));
        c
    };
    let mut wire = b"\xff\x06\x00\x00sNaPpY".to_vec();
    wire.extend(chunk(b'a'));
    wire.extend(chunk(b'b'));

    let mut decoder = SnappyStreamDecoder::new();
    // 10-byte stream id + 4-byte header + 65_539 body bytes: the first chunk is
    // still incomplete, so 65_543 bytes stay buffered.
    let split = 10 + 4 + (MAX_BLOCK - 1);
    let mut decoded = decoder.feed(&wire[..split]).unwrap();
    // The second feed carries the last body byte plus the whole second chunk:
    // retained + incoming exceeds any per-feed budget.
    assert_eq!(wire.len() - split, BODY - (MAX_BLOCK - 1) + 4 + BODY);
    decoded.extend(decoder.feed(&wire[split..]).unwrap());
    assert_eq!(decoded.len(), 2 * MAX_BLOCK);
    assert_eq!(&decoded[..MAX_BLOCK], &vec![b'a'; MAX_BLOCK][..]);
    assert_eq!(&decoded[MAX_BLOCK..], &vec![b'b'; MAX_BLOCK][..]);
    assert_eq!(decoder.buf.len() - decoder.pos, 0);
}

#[test]
fn skips_large_padding_without_retaining_it() {
    let mut decoder = SnappyStreamDecoder::new();
    decoder.feed(b"\xff\x06\x00\x00sNaPpY").unwrap();
    // A skippable chunk may use the full 24-bit length field.
    decoder.feed(&[0xfe, 0xff, 0xff, 0xff]).unwrap();
    for _ in 0..256 {
        decoder.feed(&[0u8; 65_535]).unwrap();
        assert!(decoder.buf.len() - decoder.pos <= MAX_RETAINED_INPUT);
    }
    decoder.feed(&[0u8; 255]).unwrap();
    assert_eq!(decoder.skip_remaining, 0);
}
