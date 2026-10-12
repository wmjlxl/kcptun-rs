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

/// A plain UDP dial must get a fresh random conv per connection (Go kcp-go
/// `Dial` parity), while an explicit `.conv()` — even pinned to
/// [`DEFAULT_CONV`](crate::config::DEFAULT_CONV) — is honoured verbatim.
/// Random conv is what lets a listener replace (not alias into) a lingering
/// session when the peer re-dials from a reused source address
/// (bugs/BUGREPORT_KCP_LISTENER_STALE_SESSION_ALIASING.md).
#[tokio::test]
async fn udp_dial_randomizes_conv_unless_pinned() {
    let a = KcpStream::connect("127.0.0.1:9").build().await.unwrap();
    let b = KcpStream::connect("127.0.0.1:9").build().await.unwrap();
    let ca = a.shared.kcp.lock().conv();
    let cb = b.shared.kcp.lock().conv();
    assert_ne!(
        ca,
        crate::config::DEFAULT_CONV,
        "dial must not keep the fixed conv"
    );
    assert_ne!(cb, crate::config::DEFAULT_CONV);
    assert_ne!(ca, cb, "each dial gets its own conv");

    // Explicit pin wins, even for the default value.
    let pinned = KcpStream::connect("127.0.0.1:9")
        .conv(crate::config::DEFAULT_CONV)
        .build()
        .await
        .unwrap();
    assert_eq!(
        pinned.shared.kcp.lock().conv(),
        crate::config::DEFAULT_CONV,
        "explicit .conv() must not be randomised"
    );

    // A server-side (adopting) build never randomises: it learns the peer's
    // conv from the first inbound segment instead.
    assert!(!KcpStream::connect("127.0.0.1:9").conv_explicit);
    assert!(
        KcpStream::connect("127.0.0.1:9")
            .conv(crate::config::DEFAULT_CONV)
            .conv_explicit
    );
}

#[tokio::test]
async fn session_statistics_track_only_successful_build_and_unique_close() {
    let conn = KcpStream::connect("127.0.0.1:9")
        .background_input(false)
        .spawn_flush_loop(false)
        .build()
        .await
        .unwrap();
    assert!(conn.shared.session_tracked.load(Ordering::Acquire));
    let observer = conn.clone();
    let (read, write) = conn.into_split();
    drop(read);
    assert!(observer.shared.session_tracked.load(Ordering::Acquire));
    drop(write);
    assert!(!observer.shared.session_tracked.load(Ordering::Acquire));
    observer.close();
    assert!(!observer.shared.session_tracked.load(Ordering::Acquire));

    let pending = KcpStream::connect("127.0.0.1:9")
        .connect_timeout(Duration::from_millis(1))
        .build()
        .await;
    assert!(
        pending.is_err(),
        "failed builds must never register a session"
    );
}

#[tokio::test]
async fn inbound_clock_requires_valid_current_conv_input() {
    fn control(conv: u32, command: u8) -> Vec<u8> {
        let mut packet = vec![0u8; 24];
        packet[..4].copy_from_slice(&conv.to_le_bytes());
        packet[4] = command;
        packet[6..8].copy_from_slice(&128u16.to_le_bytes());
        packet
    }
    let conn = KcpStream::connect("127.0.0.1:9")
        .conv(42)
        .background_input(false)
        .spawn_flush_loop(false)
        .build()
        .await
        .unwrap();
    let original = conn.shared.last_rx_ms.load(Ordering::Relaxed);
    knet::sleep_ms(2).await;
    conn.feed_raw_batch(vec![control(43, 84)]); // valid header, wrong conv
    assert_eq!(conn.shared.last_rx_ms.load(Ordering::Relaxed), original);
    conn.feed_raw_batch(vec![control(42, 0)]); // current conv, invalid command
    assert_eq!(conn.shared.last_rx_ms.load(Ordering::Relaxed), original);
    conn.feed_raw_batch(vec![control(42, 84)]); // current conv, valid WINS
    assert!(conn.shared.last_rx_ms.load(Ordering::Relaxed) > original);

    let fec = KcpStream::connect("127.0.0.1:9")
        .conv(42)
        .background_input(false)
        .spawn_flush_loop(false)
        .fec(2, 1)
        .build()
        .await
        .unwrap();
    let original = fec.shared.last_rx_ms.load(Ordering::Relaxed);
    knet::sleep_ms(2).await;
    let mut parity = vec![0u8; 8];
    parity[..4].copy_from_slice(&2u32.to_le_bytes());
    parity[4..6].copy_from_slice(&FEC_TYPE_PARITY.to_le_bytes());
    fec.feed_raw_batch(vec![parity]); // too few shards to recover any KCP input
    assert_eq!(fec.shared.last_rx_ms.load(Ordering::Relaxed), original);
}
