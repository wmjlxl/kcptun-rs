use super::*;

fn encoded_group(encoder: &mut FecEncoder, group: u32) -> Vec<Vec<u8>> {
    let mut packets = Vec::new();
    let mut parity = Vec::new();
    for slot in 0..encoder.data_shards {
        let mut payload = vec![(slot % 251) as u8; 24 + slot % 17];
        payload[..4].copy_from_slice(&group.to_le_bytes());
        payload[4..8].copy_from_slice(&(slot as u32).to_le_bytes());
        let (frame, generated) = encoder.wrap_kcp_packet(&payload, u32::MAX);
        packets.push(frame);
        parity.extend(generated);
    }
    assert_eq!(parity.len(), encoder.parity_shards);
    packets.extend(parity);
    packets
}

fn original_payload(packet: &[u8]) -> &[u8] {
    fec_kcp_from_recovered(&packet[FEC_HEADER_SIZE..]).unwrap()
}

#[test]
fn all_bitmap_words_and_last_parity_slot_recover_exact_variable_payloads() {
    let mut encoder = FecEncoder::new(128, 128, 0).unwrap();
    let packets = encoded_group(&mut encoder, 7);
    let missing = [0, 63, 64, 127];
    let mut decoder = FecDecoder::new(128, 128).unwrap();
    for slot in (0..128).rev().filter(|slot| !missing.contains(slot)) {
        assert!(decoder.decode(&packets[slot]).is_empty());
    }
    // Parity indices touch both upper bitmap words and the final legal bit.
    for slot in [255, 192, 191] {
        assert!(decoder.decode(&packets[slot]).is_empty());
        assert!(decoder.decode(&packets[slot]).is_empty());
    }
    let recovered = decoder.decode(&packets[128]);
    assert_eq!(recovered.len(), missing.len());
    for (recovered, slot) in recovered.iter().zip(missing) {
        assert_eq!(
            fec_kcp_from_recovered(recovered).unwrap(),
            original_payload(&packets[slot])
        );
    }
    assert!(decoder.shard_set.is_empty());
    assert!(decoder.completed_shards.contains_key(&0));
    for packet in &packets {
        assert!(decoder.decode(packet).is_empty());
    }
    assert!(decoder.shard_set.is_empty());
}

#[test]
fn duplicate_payload_cannot_replace_first_shard_or_count_as_another_slot() {
    let mut encoder = FecEncoder::new(10, 3, 0).unwrap();
    let packets = encoded_group(&mut encoder, 3);
    let mut decoder = FecDecoder::new(10, 3).unwrap();
    assert!(decoder.decode(&packets[1]).is_empty());
    let mut changed_duplicate = packets[1].clone();
    changed_duplicate[FEC_HEADER_SIZE_PLUS_2..].fill(0xcc);
    for _ in 0..20 {
        assert!(decoder.decode(&changed_duplicate).is_empty());
    }
    for slot in 2..10 {
        assert!(decoder.decode(&packets[slot]).is_empty());
    }
    let recovered = decoder.decode(&packets[12]);
    assert_eq!(recovered.len(), 1);
    assert_eq!(
        fec_kcp_from_recovered(&recovered[0]).unwrap(),
        original_payload(&packets[0])
    );
    for packet in &packets[10..] {
        assert!(decoder.decode(packet).is_empty());
    }
}

#[test]
fn equal_slot_indices_in_interleaved_full_group_ids_stay_independent() {
    let mut encoder = FecEncoder::new(3, 2, 0).unwrap();
    let groups: Vec<_> = (0..5)
        .map(|group| encoded_group(&mut encoder, group))
        .collect();
    let mut decoder = FecDecoder::new(3, 2).unwrap();
    assert!(decoder.decode(&groups[0][1]).is_empty());
    assert!(decoder.decode(&groups[3][1]).is_empty());
    assert!(decoder.decode(&groups[0][2]).is_empty());
    assert!(decoder.decode(&groups[3][2]).is_empty());
    for group in [3, 0] {
        let recovered = decoder.decode(&groups[group][4]);
        assert_eq!(recovered.len(), 1);
        assert_eq!(
            fec_kcp_from_recovered(&recovered[0]).unwrap(),
            original_payload(&groups[group][0])
        );
    }
    assert!(decoder.completed_shards.contains_key(&0));
    assert!(decoder.completed_shards.contains_key(&3));
    decoder.decode(&groups[4][0]);
    assert!(!decoder.completed_shards.contains_key(&0));
    for packet in &groups[0] {
        assert!(decoder.decode(packet).is_empty());
    }
    assert!(!decoder.shard_set.contains_key(&0));
    assert!(decoder.shard_set.len() <= MAX_SHARD_SETS as usize + 1);
}

#[test]
fn real_packets_recover_on_both_sides_of_paws_wrap() {
    let mut encoder = FecEncoder::new(128, 128, 0).unwrap();
    encoder.next = encoder.paws - encoder.shard_size as u32;
    let last = encoded_group(&mut encoder, 8);
    let first = encoded_group(&mut encoder, 9);
    assert_eq!(parse_fec_header(&first[0]).unwrap().0, 0);
    let mut decoder = FecDecoder::new(128, 128).unwrap();
    for packets in [&last, &first] {
        for packet in packets.iter().take(128).skip(1).rev() {
            assert!(decoder.decode(packet).is_empty());
        }
    }
    // New group zero has arrived, but the final pre-wrap group is age one.
    for packets in [&last, &first] {
        let recovered = decoder.decode(&packets[255]);
        assert_eq!(recovered.len(), 1);
        assert_eq!(
            fec_kcp_from_recovered(&recovered[0]).unwrap(),
            original_payload(&packets[0])
        );
    }
    assert_eq!(decoder.newest_shard_id, Some(0));
    assert_eq!(decoder.completed_shards.len(), 2);
    assert!(decoder.shard_set.is_empty());
}

#[test]
fn retuning_discards_old_slot_dimensions_and_completed_groups() {
    let mut old_encoder = FecEncoder::new(3, 2, 0).unwrap();
    let old_complete = encoded_group(&mut old_encoder, 1);
    let old_partial = encoded_group(&mut old_encoder, 2);
    let mut decoder = FecDecoder::new(3, 2).unwrap();
    for packet in old_complete.iter().take(3) {
        decoder.decode(packet);
    }
    decoder.decode(&old_partial[0]);
    assert!(!decoder.shard_set.is_empty());
    assert!(!decoder.completed_shards.is_empty());
    let mut new_encoder = FecEncoder::new(128, 128, 0).unwrap();
    let mut tuned = false;
    for group in 0..4 {
        for packet in encoded_group(&mut new_encoder, group) {
            decoder.decode(&packet);
            if decoder.data_shards == 128 {
                assert_eq!(decoder.parity_shards, 128);
                assert_eq!(decoder.shard_size, 256);
                assert!(decoder.shard_set.is_empty());
                assert!(decoder.completed_shards.is_empty());
                assert_eq!(decoder.newest_shard_id, None);
                assert_eq!(decoder.decode_cache.len(), 256);
                assert_eq!(decoder.flag_cache.len(), 256);
                tuned = true;
                break;
            }
        }
        if tuned {
            break;
        }
    }
    assert!(tuned);
    let packets = encoded_group(&mut new_encoder, 19);
    for packet in packets.iter().take(128).skip(1) {
        decoder.decode(packet);
    }
    let recovered = decoder.decode(&packets[255]);
    assert_eq!(recovered.len(), 1);
    assert_eq!(
        fec_kcp_from_recovered(&recovered[0]).unwrap(),
        original_payload(&packets[0])
    );
}
