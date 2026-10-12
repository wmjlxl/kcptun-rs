use bytes::Bytes;
use kcp_rs::fec::{fec_kcp_from_recovered, FecDecoder, FecEncodeError, FecEncoder};
use kcp_rs::{snmp_enable, SNMP};
use std::sync::atomic::Ordering;

#[test]
fn accepted_256_shard_config_auto_tunes_and_recovers() {
    let mut encoder = FecEncoder::new(128, 128, 0).unwrap();
    let mut reference = FecDecoder::new(128, 128).unwrap();
    let mut auto = FecDecoder::new(3, 2).unwrap();
    let mut reference_recovered = Vec::new();
    let mut auto_recovered = Vec::new();
    for group in 0..4u8 {
        for slot in 0..128 {
            let (data, parity) = encoder.wrap_kcp_packet(&[group; 16], u32::MAX);
            if slot != 0 {
                reference_recovered.extend(reference.decode(&data));
                auto_recovered.extend(auto.decode(&data));
            }
            for packet in parity {
                reference_recovered.extend(reference.decode(&packet));
                auto_recovered.extend(auto.decode(&packet));
            }
        }
    }
    assert_eq!(reference_recovered.len(), 4);
    // Discovery needs an observed period: earlier dropped groups cannot be
    // recovered retroactively, but subsequent groups must use the learned FEC.
    assert!(!auto_recovered.is_empty());
    assert!(auto_recovered
        .iter()
        .all(|packet| reference_recovered.contains(packet)));
}

#[test]
fn tuned_256_shard_decoder_recovers_every_later_lost_packet() {
    let mut encoder = FecEncoder::new(128, 128, 0).unwrap();
    let mut reference = FecDecoder::new(128, 128).unwrap();
    let mut auto = FecDecoder::new(3, 2).unwrap();
    let mut expected = Vec::new();
    let mut actual = Vec::new();
    for group in 0..6u8 {
        for slot in 0..128 {
            let (data, parity) = encoder.wrap_kcp_packet(&[group; 16], u32::MAX);
            if group < 2 || slot != 0 {
                expected.extend(reference.decode(&data));
                actual.extend(auto.decode(&data));
            }
            for packet in parity {
                expected.extend(reference.decode(&packet));
                actual.extend(auto.decode(&packet));
            }
        }
    }
    assert_eq!(expected.len(), 4);
    assert_eq!(actual, expected);
}

#[test]
fn invalid_inputs_do_not_advance_sequence_or_partial_group() {
    let mut encoder = FecEncoder::new(2, 1, 0).unwrap();
    let (first, parity) = encoder.try_wrap_kcp_packet(&[0x11; 16], u32::MAX).unwrap();
    assert!(parity.is_empty());
    assert_eq!(u32::from_le_bytes(first[..4].try_into().unwrap()), 0);
    assert_eq!(
        encoder.try_encode(&mut [], u32::MAX),
        Err(FecEncodeError::ShortPacket)
    );
    for _ in 0..3 {
        assert!(encoder.encode(&mut [], u32::MAX).is_empty());
    }
    assert_eq!(
        encoder.try_wrap_kcp_packet(&vec![0x33; 65_534], u32::MAX),
        Err(FecEncodeError::PayloadTooLarge)
    );
    let (rejected, rejected_parity) = encoder.wrap_kcp_packet(&vec![0x33; 65_534], u32::MAX);
    assert!(rejected.is_empty() && rejected_parity.is_empty());
    let (second, parity) = encoder.try_wrap_kcp_packet(&[0x22; 16], u32::MAX).unwrap();
    assert_eq!(u32::from_le_bytes(second[..4].try_into().unwrap()), 1);
    assert_eq!(parity.len(), 1);
    let mut decoder = FecDecoder::new(2, 1).unwrap();
    assert!(decoder.decode(&second).is_empty());
    let recovered = decoder.decode(&parity[0]);
    assert_eq!(recovered.len(), 1);
    assert_eq!(fec_kcp_from_recovered(&recovered[0]), Some(&[0x11; 16][..]));
}

#[test]
fn size_field_boundary_recovers_without_truncation() {
    let mut encoder = FecEncoder::new(2, 1, 0).unwrap();
    let payload = vec![0x42; 65_533];
    let (first, _) = encoder.try_wrap_kcp_packet(&payload, u32::MAX).unwrap();
    assert_eq!(
        u16::from_le_bytes(first[6..8].try_into().unwrap()),
        u16::MAX
    );
    let (second, parity) = encoder.try_wrap_kcp_packet(&[7; 16], u32::MAX).unwrap();
    let mut decoder = FecDecoder::new(2, 1).unwrap();
    decoder.decode(&second);
    let recovered = decoder.decode(&parity[0]);
    assert_eq!(recovered.len(), 1);
    assert_eq!(
        fec_kcp_from_recovered(&recovered[0]),
        Some(payload.as_slice())
    );
}

#[test]
fn expansion_never_emits_invalid_empty_frame() {
    let mut encoder = FecEncoder::new(2, 1, 0).unwrap();
    let packets = [Bytes::from(vec![0; 65_534]), Bytes::from_static(b"valid")];
    let expanded = kcp_rs::fec_expand_packets(&mut encoder, &packets, u32::MAX);
    assert_eq!(expanded.len(), 1);
    assert_eq!(&expanded[0][8..], b"valid");
    assert_eq!(u32::from_le_bytes(expanded[0][..4].try_into().unwrap()), 0);
}

#[test]
fn impossible_dimensions_and_offset_are_rejected_without_overflow() {
    assert!(FecEncoder::new(usize::MAX, 1, 0).is_none());
    assert!(FecDecoder::new(usize::MAX, 1).is_none());
    assert!(FecEncoder::new(3, 2, usize::MAX).is_none());
    assert!(FecEncoder::new(128, 129, 0).is_none());
    assert!(FecDecoder::new(128, 129).is_none());
}

#[test]
fn resetting_live_gauge_and_unmatched_close_cannot_underflow() {
    snmp_enable();
    let counters = SNMP::new();
    counters.session_opened(true);
    counters.reset();
    assert_eq!(counters.curr_estab.load(Ordering::Relaxed), 1);
    assert_eq!(counters.max_conn.load(Ordering::Relaxed), 1);
    assert_eq!(counters.active_opens.load(Ordering::Relaxed), 0);
    counters.session_closed();
    counters.session_closed();
    assert_eq!(counters.curr_estab.load(Ordering::Relaxed), 0);
    assert_eq!(format!("{counters}"), counters.to_slice().join(","));
    assert_eq!(
        format!("{counters}").split(',').count(),
        SNMP::header().len()
    );
}
