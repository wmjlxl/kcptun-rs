use super::*;

#[test]
fn read_buffer_tracks_bytes_and_entries() {
    let mut buffer = ReadBuffer::default();
    buffer.push_back(Bytes::from_static(b"abc"));
    buffer.push_back(Bytes::from_static(b"defgh"));
    assert_eq!(buffer.len(), 2);
    assert_eq!(buffer.bytes(), 8);

    let first = buffer.pop_front().unwrap();
    assert_eq!(first.as_ref(), b"abc");
    assert_eq!(buffer.len(), 1);
    assert_eq!(buffer.bytes(), 5);

    buffer.push_front(first.slice(1..));
    assert_eq!(buffer.len(), 2);
    assert_eq!(buffer.bytes(), 7);

    buffer.clear();
    assert!(buffer.is_empty());
    assert_eq!(buffer.bytes(), 0);
}

#[test]
fn config_defaults_fast3ish() {
    let c = KcpConfig::default();
    assert_eq!(c.mtu, 1350);
    assert_eq!(c.sndwnd, 128);
    assert_eq!(c.rcvwnd, 128);
    assert!(matches!(c.mode, KcpMode::Fast3));
    assert!(c.stream);
    assert!(c.acknodelay);
    assert_eq!(c.datashard, 0);
    assert_eq!(c.parityshard, 0);
}

#[test]
fn raw_packet_queue_recycles_capacity() {
    let mut q = RawPacketQueue::default();
    q.push(Bytes::from_static(b"a"));
    q.push(Bytes::from_static(b"b"));
    // drain hands out the batch with no lock-internal allocation.
    let batch = q.drain();
    assert_eq!(batch.len(), 2);
    assert!(q.is_empty());
    // recycle keeps the batch's capacity in the spare slot.
    q.recycle(batch);
    assert!(q.spare.capacity() >= 2);
    // The next burst accumulates into the recycled buffer (no re-grow).
    q.push(Bytes::from_static(b"c"));
    let batch2 = q.drain();
    assert_eq!(batch2.len(), 1);
    assert!(
        q.pending.capacity() >= 2,
        "recycled capacity should become the next accumulation target"
    );
    q.recycle(batch2);
}

#[test]
fn raw_packet_queue_caps_retained_batch() {
    let mut q = RawPacketQueue::default();
    // A pathologically large batch is not retained (bounded memory).
    let big = Vec::with_capacity(MAX_RETAINED_RAW_BATCH + 1);
    q.recycle(big);
    assert!(q.spare.is_empty());
}

#[test]
fn builder_sets_mtu_windows() {
    let b = KcpStream::connect("127.0.0.1:9")
        .mtu(1400)
        .sndwnd(256)
        .rcvwnd(64)
        .mode(KcpMode::Fast2)
        .stream(false)
        .acknodelay(false);
    assert_eq!(b.config.mtu, 1400);
    assert_eq!(b.config.sndwnd, 256);
    assert_eq!(b.config.rcvwnd, 64);
    assert!(matches!(b.config.mode, KcpMode::Fast2));
    assert!(!b.config.stream);
    assert!(!b.config.acknodelay);
}

#[test]
fn apply_mode_values() {
    let mut kcp = KCP::new(1, 0, |_| {});
    let cfg = KcpConfig {
        mode: KcpMode::Fast3,
        mtu: 1350,
        ..KcpConfig::default()
    };
    kcp.apply(&cfg).unwrap();
    assert_eq!(kcp.mtu(), 1350);
    assert_eq!(kcp.snd_wnd(), 128);
    assert_eq!(kcp.interval(), 10);
}

#[test]
fn builder_fec_sets_shards() {
    let b = KcpStream::connect("127.0.0.1:9").fec(10, 3);
    assert_eq!(b.config.datashard, 10);
    assert_eq!(b.config.parityshard, 3);
}
