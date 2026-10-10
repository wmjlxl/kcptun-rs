use super::*;
use knet::AsyncReadExt;
use knet::AsyncWriteExt;

#[derive(Default)]
struct PartialBatchTransport {
    try_limit: usize,
    sent: Mutex<Vec<Bytes>>,
    async_calls: AtomicUsize,
}

impl PartialBatchTransport {
    fn with_try_limit(try_limit: usize) -> Self {
        Self {
            try_limit,
            ..Self::default()
        }
    }

    fn record_prefix(&self, packets: &[Bytes]) -> usize {
        let sent = self.try_limit.min(packets.len());
        self.sent.lock().extend_from_slice(&packets[..sent]);
        sent
    }

    fn record_all(&self, packets: &[Bytes]) {
        self.async_calls.fetch_add(1, Ordering::Relaxed);
        self.sent.lock().extend_from_slice(packets);
    }
}

#[async_trait::async_trait]
impl PacketTransport for PartialBatchTransport {
    async fn recv(&self, _buf: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::from(io::ErrorKind::WouldBlock))
    }

    fn try_recv(&self, _buf: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::from(io::ErrorKind::WouldBlock))
    }

    async fn send_batch(&self, packets: &[Bytes]) -> io::Result<()> {
        self.record_all(packets);
        Ok(())
    }

    async fn send_batch_to(&self, packets: &[Bytes], _target: SocketAddr) -> io::Result<()> {
        self.record_all(packets);
        Ok(())
    }

    fn try_send_batch(&self, packets: &[Bytes]) -> io::Result<usize> {
        Ok(self.record_prefix(packets))
    }

    fn try_send_batch_to(&self, packets: &[Bytes], _target: SocketAddr) -> io::Result<usize> {
        Ok(self.record_prefix(packets))
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(SocketAddr::from(([127, 0, 0, 1], 0)))
    }
}

async fn conn_with_transport(transport: Arc<PartialBatchTransport>) -> KcpStream {
    KcpStream::with_transport(
        transport as Arc<dyn PacketTransport>,
        SocketAddr::from(([127, 0, 0, 1], 9)),
    )
    .connected(true)
    .background_input(false)
    .spawn_flush_loop(false)
    .build()
    .await
    .unwrap()
}

async fn wait_for_packets(transport: &PartialBatchTransport, count: usize) {
    knet::timeout(Duration::from_secs(1), async {
        while transport.sent.lock().len() < count {
            knet::yield_now().await;
        }
    })
    .await
    .expect("partial-send continuation timed out");
}

#[tokio::test]
async fn flush_tx_batch_sends_partial_suffix_once() {
    let transport = Arc::new(PartialBatchTransport::with_try_limit(1));
    let conn = conn_with_transport(transport.clone()).await;
    let packets = vec![
        Bytes::from_static(b"first"),
        Bytes::from_static(b"second"),
        Bytes::from_static(b"third"),
    ];

    let delivered = conn.shared.flush_tx_batch(&packets).await.unwrap();

    assert_eq!(delivered, packets.len());
    assert_eq!(*transport.sent.lock(), packets);
    assert_eq!(transport.async_calls.load(Ordering::Relaxed), 1);
}

/// `OutPkts` counts datagrams handed to the socket, so an FEC batch reports
/// the post-expansion wire count. Counting it in the KCP output callback
/// (where it used to live) reports segments instead and drops the parity
/// datagrams entirely.
#[tokio::test]
async fn out_pkts_counts_wire_datagrams_not_segments() {
    let transport = Arc::new(PartialBatchTransport::with_try_limit(usize::MAX));
    let conn = KcpStream::with_transport(
        transport.clone() as Arc<dyn PacketTransport>,
        SocketAddr::from(([127, 0, 0, 1], 9)),
    )
    .connected(true)
    .background_input(false)
    .spawn_flush_loop(false)
    .fec(10, 3)
    .build()
    .await
    .unwrap();

    let batch: Vec<Bytes> = (0..10).map(|i| Bytes::from(vec![i as u8; 32])).collect();
    crate::snmp::enable();
    let before = crate::snmp::DEFAULT_SNMP.out_pkts.load(Ordering::SeqCst);

    // The first group may or may not carry parity: the encoder skips it
    // when the previous packet is older than its RTO window, which depends
    // on process uptime.
    assert_eq!(
        conn.shared.flush_tx_batch(&batch).await.unwrap(),
        batch.len()
    );
    let after_first = transport.sent.lock().len();

    // Second group is continuous by construction: 10 data + 3 parity.
    assert_eq!(
        conn.shared.flush_tx_batch(&batch).await.unwrap(),
        batch.len()
    );
    let after_second = transport.sent.lock().len();
    assert_eq!(after_second - after_first, 13, "10 data shards + 3 parity");

    let after = crate::snmp::DEFAULT_SNMP.out_pkts.load(Ordering::SeqCst);
    // Lower bound rather than equality: DEFAULT_SNMP is process-global and
    // other tests in this binary increment it concurrently. Before the fix
    // this batch moved it by 0 (the callback it was counted in is not on
    // this path), so anything below the wire count is a regression.
    assert!(
        after - before >= after_second as u64,
        "out_pkts moved by {} for {} datagrams on the wire",
        after - before,
        after_second
    );
}

/// A send future dropped mid-flight — a stream closing while it was parked
/// in `flush_tx_batch` — must not keep the send token. Every sender checks
/// that token, so a leaked one leaves the session permanently mute: no
/// ACKs, no retransmits, no keepalives, until the peer's own timers tear
/// the session down. Seen live as `out_age_ms == write_blocked_ms` growing
/// past 131 s with the peer window open and an empty kernel send queue.
#[tokio::test]
async fn cancelled_send_releases_the_token() {
    struct ParkedBatchTransport;

    #[async_trait::async_trait]
    impl PacketTransport for ParkedBatchTransport {
        async fn recv(&self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        }
        fn try_recv(&self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        }
        /// Parks forever: the async send path never completes.
        async fn send_batch(&self, _packets: &[Bytes]) -> io::Result<()> {
            std::future::pending::<io::Result<()>>().await
        }
        async fn send_batch_to(&self, _packets: &[Bytes], _t: SocketAddr) -> io::Result<()> {
            std::future::pending::<io::Result<()>>().await
        }
        /// WouldBlock so the caller takes the parking async path above.
        fn try_send_batch(&self, _packets: &[Bytes]) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        }
        fn try_send_batch_to(&self, _packets: &[Bytes], _t: SocketAddr) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        }
        fn local_addr(&self) -> io::Result<SocketAddr> {
            Ok(SocketAddr::from(([127, 0, 0, 1], 0)))
        }
    }

    let conn = KcpStream::with_transport(
        Arc::new(ParkedBatchTransport) as Arc<dyn PacketTransport>,
        SocketAddr::from(([127, 0, 0, 1], 9)),
    )
    .connected(true)
    .background_input(false)
    .spawn_flush_loop(false)
    .build()
    .await
    .unwrap();

    // A packet must be pending, or the send path returns before the await.
    conn.shared
        .raw_packets
        .lock()
        .push(Bytes::from_static(b"parked"));

    // Drive the send, then cancel it the way a closing stream does.
    let _ = knet::timeout(Duration::from_millis(50), conn.shared.try_drain_and_send()).await;

    assert!(
        !conn.shared.is_sending.load(Ordering::Acquire),
        "send token leaked when the send future was dropped mid-flight"
    );
}

/// `try_drain_and_send` must bound the async flush. A transport whose
/// `send_batch` parks forever (downlink congestion that never drains)
/// used to hold `is_sending` for the life of the session and mute TX:
/// no ACKs, no retransmits. The bounded path returns, releases the token,
/// and — without FEC — requeues the pre-FEC batch for the next drain.
#[tokio::test]
async fn flush_tx_timeout_releases_token_and_requeues() {
    struct ParkedBatchTransport;

    #[async_trait::async_trait]
    impl PacketTransport for ParkedBatchTransport {
        async fn recv(&self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        }
        fn try_recv(&self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        }
        async fn send_batch(&self, _packets: &[Bytes]) -> io::Result<()> {
            std::future::pending::<io::Result<()>>().await
        }
        async fn send_batch_to(&self, _packets: &[Bytes], _t: SocketAddr) -> io::Result<()> {
            std::future::pending::<io::Result<()>>().await
        }
        fn try_send_batch(&self, _packets: &[Bytes]) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        }
        fn try_send_batch_to(&self, _packets: &[Bytes], _t: SocketAddr) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        }
        fn local_addr(&self) -> io::Result<SocketAddr> {
            Ok(SocketAddr::from(([127, 0, 0, 1], 0)))
        }
    }

    let conn = KcpStream::with_transport(
        Arc::new(ParkedBatchTransport) as Arc<dyn PacketTransport>,
        SocketAddr::from(([127, 0, 0, 1], 9)),
    )
    .connected(true)
    .background_input(false)
    .spawn_flush_loop(false)
    .build()
    .await
    .unwrap();

    conn.shared
        .raw_packets
        .lock()
        .push(Bytes::from_static(b"parked"));

    // No external cancellation: the internal TX_FLUSH_TIMEOUT must fire.
    let started = std::time::Instant::now();
    let acquired = conn.shared.try_drain_and_send().await;
    assert!(acquired, "try_drain_and_send should have taken the token");
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "bounded flush took {:?}; token would mute the session",
        started.elapsed()
    );
    assert!(
        !conn.shared.is_sending.load(Ordering::Acquire),
        "send token still held after bounded flush timeout"
    );
    assert!(
        !conn.shared.raw_packets.lock().is_empty(),
        "no-FEC timeout must requeue the pre-FEC batch for the next drain"
    );
}

/// A refused datagram before/while the peer is silent means the peer is
/// gone (cold port, server restart): close so the client accept-loop
/// redials instead of waiting for dead_link or the keepalive timeout.
#[tokio::test]
async fn note_io_error_closes_on_connection_refused() {
    let transport = Arc::new(PartialBatchTransport::with_try_limit(usize::MAX));
    let conn = conn_with_transport(transport).await;
    assert!(!conn.is_closed());

    conn.shared.note_io_error(io::Error::new(
        io::ErrorKind::ConnectionRefused,
        "peer down",
    ));
    assert!(conn.is_closed());
    assert!(conn.is_dead() || conn.is_closed());
}

/// The same error while the peer is actively streaming to us is spurious —
/// high-rate transfers surface isolated ICMP reports — and must not tear
/// the session down.
#[tokio::test]
async fn note_io_error_ignores_refused_while_peer_is_streaming() {
    let transport = Arc::new(PartialBatchTransport::with_try_limit(usize::MAX));
    let conn = conn_with_transport(transport).await;
    conn.shared.first_inbound.store(true, Ordering::Release);
    conn.shared
        .last_rx_ms
        .store(knet::mono_ms(), Ordering::Release);

    conn.shared.note_io_error(io::Error::new(
        io::ErrorKind::ConnectionRefused,
        "stray icmp",
    ));
    assert!(!conn.is_closed());
}

#[tokio::test]
async fn note_io_error_keeps_connection_on_transient_error() {
    let transport = Arc::new(PartialBatchTransport::with_try_limit(usize::MAX));
    let conn = conn_with_transport(transport).await;
    assert!(!conn.is_closed());

    conn.shared
        .note_io_error(io::Error::new(io::ErrorKind::WouldBlock, "transient"));
    assert!(!conn.is_closed());
}

#[tokio::test]
async fn inline_partial_send_preserves_fec_wire_batch() {
    let transport = Arc::new(PartialBatchTransport::with_try_limit(1));
    let conn = KcpStream::with_transport(
        transport.clone() as Arc<dyn PacketTransport>,
        SocketAddr::from(([127, 0, 0, 1], 9)),
    )
    .connected(true)
    .background_input(false)
    .spawn_flush_loop(false)
    .fec(2, 1)
    .build()
    .await
    .unwrap();
    let packets = vec![Bytes::from_static(b"alpha"), Bytes::from_static(b"beta")];
    conn.shared.raw_packets.lock().pending.extend(packets);

    assert!(conn.shared.drain_and_flush_tx());
    wait_for_packets(&transport, 3).await;
    knet::timeout(Duration::from_secs(1), async {
        while conn.shared.is_sending.load(Ordering::Acquire) {
            knet::yield_now().await;
        }
    })
    .await
    .expect("partial-send continuation did not release send token");

    let sent = transport.sent.lock();
    assert_eq!(sent.len(), 3, "2 data shards + 1 parity shard");
    assert_eq!(u16::from_le_bytes([sent[0][4], sent[0][5]]), FEC_TYPE_DATA);
    assert_eq!(u16::from_le_bytes([sent[1][4], sent[1][5]]), FEC_TYPE_DATA);
    assert_eq!(
        u16::from_le_bytes([sent[2][4], sent[2][5]]),
        FEC_TYPE_PARITY
    );
    assert_eq!(transport.async_calls.load(Ordering::Relaxed), 1);
}

/// Two KcpStream over localhost UDP, bidirectional integrity check (no FEC).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bidirectional_localhost() {
    let (mut conn_a, mut conn_b) = pair_conns(None).await;
    roundtrip(&mut conn_a, &mut conn_b, b"hello-kcp-conn-phase1").await;
    roundtrip(&mut conn_b, &mut conn_a, b"ping-pong-reverse").await;
    conn_a.close();
    conn_b.close();
}

/// Bidirectional integrity with FEC 10/3 (Go-compatible defaults).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bidirectional_localhost_fec_10_3() {
    let (mut conn_a, mut conn_b) = pair_conns(Some((10, 3))).await;

    // Multi-packet payload so FEC groups fill (parity generated).
    let mut payload = vec![0u8; 32 * 1024];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    roundtrip(&mut conn_a, &mut conn_b, &payload).await;

    let mut payload2 = vec![0u8; 16 * 1024];
    for (i, b) in payload2.iter_mut().enumerate() {
        *b = (255 - (i % 251)) as u8;
    }
    roundtrip(&mut conn_b, &mut conn_a, &payload2).await;

    conn_a.close();
    conn_b.close();
}

/// Smaller FEC group (2/1) still preserves integrity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bidirectional_localhost_fec_2_1() {
    let (mut conn_a, mut conn_b) = pair_conns(Some((2, 1))).await;
    let payload = b"fec-2-1-small-payload-integrity-check!!!!";
    roundtrip(&mut conn_a, &mut conn_b, payload).await;
    roundtrip(&mut conn_b, &mut conn_a, b"reverse-2-1").await;
    conn_a.close();
    conn_b.close();
}

/// The stale-session eviction guard counts WHOLE bursts: one foreign-conv
/// datagram inside a burst of fresh traffic must not mark the burst stale.
/// A live session whose ACKs are being lost retransmits segment 0 with
/// `una == 0`, which is indistinguishable from a re-dial unless the burst
/// context is honored. Regression for the session-task listener, where
/// per-datagram feeding made every datagram a one-packet burst and evicted
/// live sessions under load (32-connection request timeouts).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_guard_counts_whole_burst_not_single_datagrams() {
    use crate::segment::Command;

    let (mut conn_a, mut conn_b) = pair_conns(None).await;
    // Establish traffic so conn_b has moved past the first segments.
    roundtrip(&mut conn_a, &mut conn_b, b"establish").await;

    // KCP header: conv(4) cmd(1) frg(1) wnd(2) ts(4) sn(4) una(4) len(4).
    let header = |conv: u32, cmd: u8, sn: u32| {
        let mut d = vec![0u8; 24];
        d[0..4].copy_from_slice(&conv.to_le_bytes());
        d[4] = cmd;
        d[6..8].copy_from_slice(&32u16.to_le_bytes()); // advertised window
        d[12..16].copy_from_slice(&sn.to_le_bytes());
        d
    };
    let conv = 0xC0FFEE;
    let foreign = header(0xDEAD_BEEF, Command::Push as u8, 0);
    // An ACK for sn 0 with the correct conv: stale traffic (already acked
    // or a duplicate), so only the foreign datagram counts as stale.
    let stale_ack = header(conv, Command::Ack as u8, 0);

    let before = conn_b.stale_burst_count();
    conn_b.feed_raw_batch(vec![foreign, stale_ack.clone(), stale_ack]);
    assert_eq!(
        conn_b.stale_burst_count(),
        before,
        "one stale datagram in a burst of fresh traffic must not count the burst stale"
    );

    // A wholly-stale burst still counts — that is the genuine re-dial
    // signal the eviction path exists for.
    let before = conn_b.stale_burst_count();
    conn_b.feed_raw_batch(vec![
        header(0xDEAD_BEEF, Command::Push as u8, 0),
        header(0xDEAD_BEEF, Command::Push as u8, 0),
    ]);
    assert_eq!(
        conn_b.stale_burst_count(),
        before + 1,
        "a burst whose every datagram mismatches must count as stale"
    );
}

/// M-4 rework: a forged 20-byte sn=0/una=0 packet with a WRONG conv
/// must not increment `peer_restart` — it must first pass conv
/// validation, otherwise an attacker can trip the eviction counter
/// with arbitrary bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_restart_requires_conv_valid_packet() {
    use crate::segment::Command;

    let (mut conn_a, mut conn_b) = pair_conns(None).await;
    roundtrip(&mut conn_a, &mut conn_b, b"establish").await;

    let header = |conv: u32, cmd: u8, sn: u32, una: u32| {
        let mut d = vec![0u8; 24];
        d[0..4].copy_from_slice(&conv.to_le_bytes());
        d[4] = cmd;
        d[6..8].copy_from_slice(&32u16.to_le_bytes());
        d[12..16].copy_from_slice(&sn.to_le_bytes());
        d[16..20].copy_from_slice(&una.to_le_bytes());
        d
    };

    let before = conn_b.peer_restart_count();
    // Wrong conv, sn=0, una=0: forged restart, must NOT count.
    conn_b.feed_raw_batch(vec![header(0xDEAD_BEEF, Command::Push as u8, 0, 0)]);
    assert_eq!(
        conn_b.peer_restart_count(),
        before,
        "forged packet with wrong conv must not trip peer_restart"
    );

    // A burst of many forged packets still must not move the counter.
    let forged: Vec<Vec<u8>> = (0..10)
        .map(|_| header(0x1111_2222, Command::Push as u8, 0, 0))
        .collect();
    conn_b.feed_raw_batch(forged);
    assert_eq!(
        conn_b.peer_restart_count(),
        before,
        "forged burst must not accumulate peer_restart"
    );

    conn_a.close();
    conn_b.close();
}

/// P0-6 / M-4: eviction needs N *consecutive* all-stale bursts. One
/// all-stale burst can be an ACK-blackhole RTO of segment 0 (una=0), which
/// is not a re-dial. Any live burst resets the streak.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consecutive_stale_bursts_reset_on_live_traffic() {
    use crate::segment::Command;

    let (mut conn_a, mut conn_b) = pair_conns(None).await;
    roundtrip(&mut conn_a, &mut conn_b, b"establish").await;

    let header = |conv: u32, cmd: u8, sn: u32| {
        let mut d = vec![0u8; 24];
        d[0..4].copy_from_slice(&conv.to_le_bytes());
        d[4] = cmd;
        d[6..8].copy_from_slice(&32u16.to_le_bytes());
        d[12..16].copy_from_slice(&sn.to_le_bytes());
        d
    };
    let conv = 0xC0FFEE;
    let foreign = |sn: u32| header(0xDEAD_BEEF, Command::Push as u8, sn);
    let live = header(conv, Command::Ack as u8, 0);

    // One all-stale burst → consecutive = 1, not yet evictable at N=3.
    conn_b.feed_raw_batch(vec![foreign(0)]);
    assert_eq!(conn_b.consecutive_stale_burst_count(), 1);

    // Live traffic resets the streak.
    conn_b.feed_raw_batch(vec![live.clone()]);
    assert_eq!(
        conn_b.consecutive_stale_burst_count(),
        0,
        "a live burst must reset the consecutive stale streak"
    );

    // Two more all-stale bursts after the reset: still only 2.
    conn_b.feed_raw_batch(vec![foreign(1)]);
    conn_b.feed_raw_batch(vec![foreign(2)]);
    assert_eq!(conn_b.consecutive_stale_burst_count(), 2);

    // Third consecutive → reaches the default threshold of 3.
    conn_b.feed_raw_batch(vec![foreign(3)]);
    assert_eq!(conn_b.consecutive_stale_burst_count(), 3);

    conn_a.close();
    conn_b.close();
}

async fn pair_conns(fec: Option<(u32, u32)>) -> (KcpStream, KcpStream) {
    let a_tmp = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
    let b_tmp = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
    let addr_a = a_tmp.local_addr().unwrap();
    let addr_b = b_tmp.local_addr().unwrap();
    drop(a_tmp);
    drop(b_tmp);

    let sock_a = knet::UdpSocket::connect(addr_a, addr_b).unwrap();
    let sock_b = knet::UdpSocket::connect(addr_b, addr_a).unwrap();

    let mut ba = KcpStream::with_transport(
        Arc::new(knet::DatagramSocket::Udp(sock_a)) as Arc<dyn PacketTransport>,
        addr_b,
    )
    .connected(true)
    .conv(0xC0FFEE)
    .mode(KcpMode::Fast3)
    .mtu(1350)
    .sndwnd(128)
    .rcvwnd(128);
    let mut bb = KcpStream::with_transport(
        Arc::new(knet::DatagramSocket::Udp(sock_b)) as Arc<dyn PacketTransport>,
        addr_a,
    )
    .connected(true)
    .conv(0xC0FFEE)
    .mode(KcpMode::Fast3)
    .mtu(1350)
    .sndwnd(128)
    .rcvwnd(128);
    if let Some((d, p)) = fec {
        ba = ba.fec(d, p);
        bb = bb.fec(d, p);
    }
    let conn_a = ba.build().await.unwrap();
    let conn_b = bb.build().await.unwrap();
    (conn_a, conn_b)
}

async fn roundtrip(from: &mut KcpStream, to: &mut KcpStream, payload: &[u8]) {
    from.write_all(payload).await.unwrap();
    from.flush().await.unwrap();
    let mut got = vec![0u8; payload.len()];
    read_exact_timeout(to, &mut got, Duration::from_secs(5)).await;
    assert_eq!(&got[..], payload);
}

async fn read_exact_timeout(conn: &mut KcpStream, buf: &mut [u8], limit: Duration) {
    let deadline = std::time::Instant::now() + limit;
    let mut filled = 0usize;
    while filled < buf.len() {
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for data, got {}/{}", filled, buf.len());
        }
        match knet::timeout(Duration::from_millis(50), conn.read(&mut buf[filled..])).await {
            Ok(Ok(0)) => panic!("unexpected EOF at {}", filled),
            Ok(Ok(n)) => filled += n,
            Ok(Err(e)) => panic!("read error: {}", e),
            Err(_) => continue,
        }
    }
}

// ── BUG-1 / BUG-2 regression tests ───────────────────────────────────

/// `sendmmsg` accepts a prefix and the suffix then parks until the flush
/// timeout. The prefix is already in the kernel; requeueing it replays
/// those segments onto the congested path and the duplicate ACKs trip
/// `fastresend`. Only the suffix may come back.
#[tokio::test]
async fn flush_tx_timeout_requeues_only_the_unsent_suffix() {
    struct PrefixThenPark {
        try_limit: usize,
        sent: Mutex<Vec<Bytes>>,
    }

    #[async_trait::async_trait]
    impl PacketTransport for PrefixThenPark {
        async fn recv(&self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        }
        fn try_recv(&self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        }
        async fn send_batch(&self, _packets: &[Bytes]) -> io::Result<()> {
            std::future::pending::<io::Result<()>>().await
        }
        async fn send_batch_to(&self, _packets: &[Bytes], _t: SocketAddr) -> io::Result<()> {
            std::future::pending::<io::Result<()>>().await
        }
        fn try_send_batch(&self, packets: &[Bytes]) -> io::Result<usize> {
            let n = self.try_limit.min(packets.len());
            self.sent.lock().extend_from_slice(&packets[..n]);
            Ok(n)
        }
        fn try_send_batch_to(&self, packets: &[Bytes], _t: SocketAddr) -> io::Result<usize> {
            self.try_send_batch(packets)
        }
        fn local_addr(&self) -> io::Result<SocketAddr> {
            Ok(SocketAddr::from(([127, 0, 0, 1], 0)))
        }
    }

    let transport = Arc::new(PrefixThenPark {
        try_limit: 2,
        sent: Mutex::new(Vec::new()),
    });
    let conn = KcpStream::with_transport(
        transport.clone() as Arc<dyn PacketTransport>,
        SocketAddr::from(([127, 0, 0, 1], 9)),
    )
    .connected(true)
    .background_input(false)
    .spawn_flush_loop(false)
    .build()
    .await
    .unwrap();

    let batch = vec![
        Bytes::from_static(b"p0"),
        Bytes::from_static(b"p1"),
        Bytes::from_static(b"p2"),
        Bytes::from_static(b"p3"),
        Bytes::from_static(b"p4"),
    ];
    {
        let mut queue = conn.shared.raw_packets.lock();
        for packet in &batch {
            queue.push(packet.clone());
        }
    }

    let acquired = conn.shared.try_drain_and_send().await;
    assert!(acquired, "try_drain_and_send should have taken the token");
    assert!(
        !conn.shared.is_sending.load(Ordering::Acquire),
        "send token still held after the bounded flush timed out"
    );

    let requeued: Vec<Bytes> = conn.shared.raw_packets.lock().pending.clone();
    assert_eq!(
        requeued,
        batch[2..],
        "the prefix try_send handed to the kernel must not be requeued"
    );
    assert_eq!(*transport.sent.lock(), batch[..2]);
}

/// The sync worker hands a partial batch to `spawn_send_remainder`. When
/// that continuation times out, `Vec::split_off` used to be read
/// backwards: the unsent suffix was recycled and the delivered prefix was
/// requeued. The suffix is the part KCP will not retransmit until its RTO.
#[tokio::test]
async fn send_remainder_timeout_requeues_the_suffix_not_the_prefix() {
    struct ParkForever;

    #[async_trait::async_trait]
    impl PacketTransport for ParkForever {
        async fn recv(&self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        }
        fn try_recv(&self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        }
        async fn send_batch(&self, _packets: &[Bytes]) -> io::Result<()> {
            std::future::pending::<io::Result<()>>().await
        }
        async fn send_batch_to(&self, _packets: &[Bytes], _t: SocketAddr) -> io::Result<()> {
            std::future::pending::<io::Result<()>>().await
        }
        fn try_send_batch(&self, _packets: &[Bytes]) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        }
        fn try_send_batch_to(&self, _packets: &[Bytes], _t: SocketAddr) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        }
        fn local_addr(&self) -> io::Result<SocketAddr> {
            Ok(SocketAddr::from(([127, 0, 0, 1], 0)))
        }
    }

    let conn = KcpStream::with_transport(
        Arc::new(ParkForever) as Arc<dyn PacketTransport>,
        SocketAddr::from(([127, 0, 0, 1], 9)),
    )
    .connected(true)
    .background_input(false)
    .spawn_flush_loop(false)
    .build()
    .await
    .unwrap();

    let batch = vec![
        Bytes::from_static(b"p0"),
        Bytes::from_static(b"p1"),
        Bytes::from_static(b"p2"),
        Bytes::from_static(b"p3"),
        Bytes::from_static(b"p4"),
    ];
    // try_send already delivered the first two; the continuation owns the
    // token and only has the suffix left to send.
    conn.shared.is_sending.store(true, Ordering::Release);
    conn.shared.spawn_send_remainder(batch.clone(), None, 2);

    let requeued = knet::timeout(Duration::from_millis(500), async {
        loop {
            let queued = conn.shared.raw_packets.lock().pending.clone();
            if !queued.is_empty() {
                return queued;
            }
            knet::yield_now().await;
        }
    })
    .await
    .expect("timed-out remainder never requeued");

    assert_eq!(requeued, batch[2..]);
    assert!(
        !conn.shared.is_sending.load(Ordering::Acquire),
        "the continuation must release the send token when it times out"
    );
}
