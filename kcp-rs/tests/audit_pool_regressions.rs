//! Exercise KCP's trusted pool entry points through the public receive APIs.
use kcp_rs::{
    segment::{Command, Segment, MTU},
    KCP,
};

fn push(sn: u32, frg: u8, payload: &[u8]) -> Vec<u8> {
    let mut segment = Segment::with_capacity(payload.len());
    segment.conv = 1;
    segment.cmd = Command::Push as u8;
    segment.wnd = 32;
    segment.sn = sn;
    segment.frg = frg;
    segment.len = payload.len() as u32;
    segment.data.extend_from_slice(payload);
    let mut wire = Vec::new();
    segment.encode(&mut wire);
    wire
}

#[test]
fn dropped_zero_copy_payload_reuses_whole_buffers_including_empty_tails() {
    for size in [1326, MTU] {
        let mut receiver = KCP::new(1, 0, |_| {});
        let expected = vec![0x45; size];
        for sn in 0..1000 {
            receiver
                .input_no_flush(&push(sn, 0, &expected), false)
                .unwrap();
            let payload = receiver.recv_bytes().unwrap();
            assert_eq!(payload.as_ref(), expected);
            drop(payload);
        }
        assert_eq!(receiver.pool().created(), 1);
        assert_eq!(receiver.pool().len(), 1);
    }
}

#[test]
fn a_live_zero_copy_payload_is_preserved_across_replacement_and_copy_receive() {
    let mut receiver = KCP::new(1, 0, |_| {});
    receiver
        .input_no_flush(&push(0, 0, &vec![0x45; MTU]), false)
        .unwrap();
    let first = receiver.recv_bytes().unwrap();
    receiver
        .input_no_flush(&push(1, 0, &vec![0x77; MTU]), false)
        .unwrap();
    assert_eq!(receiver.recv().unwrap().as_ref(), vec![0x77; MTU]);
    assert_eq!(first.as_ref(), vec![0x45; MTU]);
    assert_eq!(receiver.pool().created(), 2);
    drop(first);
    receiver
        .input_no_flush(&push(2, 0, b"next"), false)
        .unwrap();
    assert_eq!(receiver.recv_bytes().unwrap().as_ref(), b"next");
    assert_eq!(receiver.pool().created(), 2);
}

#[test]
fn merged_messages_return_owned_segments_for_subsequent_zero_copy_reuse() {
    let mut receiver = KCP::new(1, 0, |_| {});
    receiver.input_no_flush(&push(0, 1, b"a"), false).unwrap();
    receiver.input_no_flush(&push(1, 0, b"b"), false).unwrap();
    assert_eq!(receiver.recv_bytes().unwrap().as_ref(), b"ab");
    assert_eq!(receiver.pool().created(), 2);
    assert_eq!(receiver.pool().len(), 2);
    receiver.input_no_flush(&push(2, 0, b"c"), false).unwrap();
    assert_eq!(receiver.recv_bytes().unwrap().as_ref(), b"c");
    assert_eq!(receiver.pool().created(), 2);
}
