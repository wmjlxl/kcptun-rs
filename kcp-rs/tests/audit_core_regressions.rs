//! Correct-behavior regressions for the 2026-10-11 KCP core audit.
//! Historical tests under bugs/ intentionally retain their old assertions.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use kcp_rs::{
    kcp::KcpError,
    segment::{Command, Segment, KCP_MAX_FRAG, KCP_OVERHEAD},
    KcpConfig, KcpMode, KCP,
};

type Output = Arc<Mutex<Vec<Bytes>>>;

fn endpoint() -> (KCP, Output) {
    let output = Arc::new(Mutex::new(Vec::new()));
    let sink = output.clone();
    let mut kcp = KCP::new(1, 0, move |data| sink.lock().unwrap().push(data));
    kcp.set_nodelay(1, 10, 2, 1);
    assert!(kcp.set_mtu(50));
    (kcp, output)
}

fn transfer(output: &Output, peer: &mut KCP, reverse_and_duplicate: bool) {
    let mut packets = std::mem::take(&mut *output.lock().unwrap());
    if reverse_and_duplicate {
        packets.reverse();
    }
    for packet in packets {
        peer.input_no_flush(&packet, true).unwrap();
        if reverse_and_duplicate {
            peer.input_no_flush(&packet, true).unwrap();
        }
    }
}

fn exchange(messages: &[Vec<u8>], reorder: bool) -> Vec<Vec<u8>> {
    let (mut sender, tx) = endpoint();
    let (mut receiver, rx) = endpoint();
    for message in messages {
        sender.send(message).unwrap();
    }
    let now = sender.current_ms() as u32;
    let mut got = Vec::new();
    for tick in 0..1000 {
        let current = now.wrapping_add(tick * 10);
        sender.flush_with_current(current, true);
        transfer(&tx, &mut receiver, reorder);
        while let Ok(message) = receiver.recv_bytes() {
            got.push(message.to_vec());
        }
        receiver.flush_with_current(current, true);
        transfer(&rx, &mut sender, false);
        if got.len() == messages.len() && sender.wait_send() == 0 {
            assert_eq!(
                receiver.rcv_wnd(),
                32,
                "configured window remains unchanged"
            );
            return got;
        }
    }
    panic!(
        "exchange stalled: received {} of {} messages",
        got.len(),
        messages.len()
    );
}

fn push(sn: u32, frg: u8, data: &[u8]) -> Vec<u8> {
    let mut seg = Segment::with_capacity(data.len());
    seg.conv = 1;
    seg.cmd = Command::Push as u8;
    seg.wnd = 32;
    seg.sn = sn;
    seg.frg = frg;
    seg.len = data.len() as u32;
    seg.data.extend_from_slice(data);
    let mut wire = Vec::new();
    seg.encode(&mut wire);
    wire
}

#[test]
fn failed_stream_send_does_not_modify_queued_tail() {
    let (mut sender, output) = endpoint();
    sender.set_stream_mode(true);
    sender.send(b"a").unwrap();
    assert_eq!(
        sender.send(&vec![b'b'; 256 * 26 + 25]),
        Err(KcpError::TooManyFragments)
    );
    assert_eq!(sender.snd_queue_len(), 1);
    sender.send(b"c").unwrap();
    sender.flush_with_current(sender.current_ms() as u32, true);
    let (mut receiver, _) = endpoint();
    transfer(&output, &mut receiver, false);
    assert_eq!(&receiver.recv().unwrap()[..], b"ac");
    assert_eq!(receiver.recv(), Err(KcpError::NoData));
}

#[test]
fn stream_tail_append_and_maximum_fragment_boundary_still_work() {
    let (mut sender, _) = endpoint();
    sender.set_stream_mode(true);
    sender.send(b"a").unwrap();
    sender
        .send(&vec![b'b'; KCP_MAX_FRAG as usize * 26 + 25])
        .unwrap();
    assert_eq!(sender.snd_queue_len(), KCP_MAX_FRAG as usize + 1);
    assert_eq!(
        sender.send(&vec![0; 256 * 26]),
        Err(KcpError::TooManyFragments)
    );
}

#[test]
fn fragmented_messages_finish_beyond_the_configured_receive_window() {
    for fragments in [1, 32, 33, KCP_MAX_FRAG as usize] {
        let messages = vec![vec![fragments as u8; fragments * 26]];
        assert_eq!(exchange(&messages, false), messages);
    }
}

#[test]
fn back_to_back_messages_survive_reordering_duplicates_and_window_recovery() {
    let messages = vec![
        b"before".to_vec(),
        vec![1; 33 * 26],
        b"between".to_vec(),
        vec![2; 255 * 26],
        b"after".to_vec(),
    ];
    assert_eq!(exchange(&messages, true), messages);
}

#[test]
fn full_u8_fragment_range_can_be_reassembled_and_then_window_recovers() {
    let (mut receiver, output) = endpoint();
    let mut expected = Vec::new();
    for sn in 0..=u8::MAX {
        let payload = [sn];
        receiver
            .input_no_flush(&push(u32::from(sn), u8::MAX - sn, &payload), false)
            .unwrap();
        expected.push(sn);
    }
    assert_eq!(receiver.rcv_nxt(), 256);
    receiver.flush_acks_only();
    let before = output.lock().unwrap().pop().unwrap();
    assert_eq!(Segment::decode(&before).unwrap().0.wnd, 0);
    assert_eq!(&receiver.recv_bytes().unwrap()[..], expected);
    receiver.flush_with_current(receiver.current_ms() as u32, true);
    let after = output.lock().unwrap().pop().unwrap();
    assert_eq!(Segment::decode(&after).unwrap().0.wnd, 32);
    assert_eq!(receiver.recv(), Err(KcpError::NoData));
}

#[test]
fn spurious_head_fragment_cannot_grow_reassembly_without_bound() {
    let (mut receiver, output) = endpoint();
    for sn in 0..1000 {
        receiver
            .input_no_flush(&push(sn, u8::MAX, b"x"), false)
            .unwrap();
    }
    assert_eq!(receiver.rcv_nxt(), 256);
    assert!(
        receiver.peeksize().is_none(),
        "a message still needs its terminating fragment"
    );
    // 256 reassembly slots plus the configured 32-slot reorder buffer.
    assert!(receiver.pool().created() <= 288);
    receiver.flush_acks_only();
    let packet = output.lock().unwrap().pop().unwrap();
    assert_eq!(Segment::decode(&packet).unwrap().0.wnd, 0);
}

#[test]
fn mtu_shrink_rejects_queued_oversized_segment_without_mutation() {
    let (mut sender, output) = endpoint();
    assert!(sender.set_mtu(1400));
    sender.send(&[1; 1000]).unwrap();
    assert!(!sender.set_mtu(100));
    assert_eq!(sender.mtu(), 1400);
    assert_eq!(sender.mss(), 1376);
    sender.flush_with_current(sender.current_ms() as u32, true);
    assert_eq!(output.lock().unwrap()[0].len(), 1024);
}

#[test]
fn mtu_shrink_rejects_inflight_oversized_segment_until_acknowledged() {
    let (mut sender, output) = endpoint();
    assert!(sender.set_mtu(1400));
    sender.send(&[1; 1000]).unwrap();
    let now = sender.current_ms() as u32;
    sender.flush_with_current(now, true);
    output.lock().unwrap().clear(); // lose the first transmission
    assert_eq!(sender.snd_queue_len(), 0);
    assert!(!sender.set_mtu(100));
    sender.flush_with_current(now.wrapping_add(200), true);
    let (mut receiver, ack) = endpoint();
    transfer(&output, &mut receiver, false);
    receiver.flush_with_current(now.wrapping_add(200), true);
    transfer(&ack, &mut sender, false);
    assert_eq!(sender.wait_send(), 0);
    assert!(sender.set_mtu(100));
    sender.send(&[2; 76]).unwrap();
    sender.flush_with_current(now.wrapping_add(210), true);
    assert_eq!(output.lock().unwrap()[0].len(), 100);
}

#[test]
fn mtu_shrink_can_keep_compatible_queued_payloads() {
    let (mut sender, output) = endpoint();
    assert!(sender.set_mtu(1400));
    sender.send(&[1; 20]).unwrap();
    assert!(sender.set_mtu(50));
    sender.flush_with_current(sender.current_ms() as u32, true);
    assert_eq!(output.lock().unwrap()[0].len(), 20 + KCP_OVERHEAD);
}

#[test]
fn extreme_rtt_saturates_and_preserves_sampling_for_missed_acks() {
    // Leave headroom so ordinary test scheduling cannot push the timestamp
    // difference past the signed comparison limit between these two calls.
    for gap in [1_431_655_766, i32::MAX as u32 - 10_000] {
        let (mut receiver, _) = endpoint();
        let mut ack = Segment::with_capacity(0);
        ack.conv = 1;
        ack.cmd = Command::Ack as u8;
        ack.wnd = 32;
        ack.sn = u32::MAX; // no outstanding send; Go still samples this ACK
        ack.ts = (receiver.current_ms() as u32).wrapping_sub(gap);
        let mut wire = Vec::new();
        ack.encode(&mut wire);
        receiver.input_no_flush(&wire, false).unwrap();
        assert_eq!(receiver.rx_rto(), 60_000);
    }
}

#[test]
fn empty_final_fragment_is_consumed_by_both_receive_apis() {
    for bytes_api in [false, true] {
        for first in [&b"a"[..], &b""[..]] {
            let (mut receiver, _) = endpoint();
            let mut wire = push(0, 1, first);
            wire.extend(push(1, 0, b""));
            wire.extend(push(2, 0, b"next"));
            receiver.input_no_flush(&wire, false).unwrap();
            let message = if bytes_api {
                receiver.recv_bytes().unwrap().to_vec()
            } else {
                receiver.recv().unwrap().to_vec()
            };
            assert_eq!(message, first);
            assert_eq!(&receiver.recv_bytes().unwrap()[..], b"next");
            assert_eq!(receiver.recv(), Err(KcpError::NoData));
        }
    }
}

#[test]
fn default_features_expose_sync_configuration() {
    let (mut kcp, _) = endpoint();
    kcp.apply(&KcpConfig::default()).unwrap();
    assert_eq!(kcp.interval(), 10);
    kcp.set_mode(KcpMode::Normal, 0, 0, 0, 0);
    assert_eq!(kcp.interval(), 40);
}
