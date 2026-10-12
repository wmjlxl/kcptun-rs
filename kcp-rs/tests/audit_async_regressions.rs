#![cfg(feature = "async")]
use bytes::Bytes;
use kcp_rs::{KcpListener, KcpStream, PacketTransport, WorkerPoolLimits};
use parking_lot::Mutex;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

#[derive(Default)]
struct RecordingTransport {
    sent: Mutex<Vec<Bytes>>,
}
#[async_trait::async_trait]
impl PacketTransport for RecordingTransport {
    async fn recv(&self, _: &mut [u8]) -> io::Result<usize> {
        std::future::pending().await
    }
    fn try_recv(&self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::ErrorKind::WouldBlock.into())
    }
    async fn send_batch(&self, packets: &[Bytes]) -> io::Result<()> {
        self.sent.lock().extend_from_slice(packets);
        Ok(())
    }
    async fn send_batch_to(&self, packets: &[Bytes], _: SocketAddr) -> io::Result<()> {
        self.send_batch(packets).await
    }
    fn try_send_batch(&self, packets: &[Bytes]) -> io::Result<usize> {
        self.sent.lock().extend_from_slice(packets);
        Ok(packets.len())
    }
    fn try_send_batch_to(&self, packets: &[Bytes], _: SocketAddr) -> io::Result<usize> {
        self.try_send_batch(packets)
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok("127.0.0.1:9000".parse().unwrap())
    }
}

async fn trait_write_then_drop(shutdown: bool) {
    let transport = Arc::new(RecordingTransport::default());
    let mut conn = KcpStream::with_transport(transport.clone(), "127.0.0.1:9001".parse().unwrap())
        .connected(true)
        .background_input(false)
        .build()
        .await
        .unwrap();
    // Explicit trait method: the inherent KcpStream::write_all has a different send path.
    knet::AsyncWriteExt::write_all(&mut conn, b"tail data")
        .await
        .unwrap();
    if shutdown {
        knet::AsyncWriteExt::shutdown(&mut conn).await.unwrap();
    } else {
        knet::AsyncWriteExt::flush(&mut conn).await.unwrap();
    }
    eprintln!(
        "after trait {} returned: sent={}",
        if shutdown { "shutdown" } else { "flush" },
        transport.sent.lock().len()
    );
    drop(conn);
    for _ in 0..10 {
        knet::yield_now().await;
    }
    assert!(
        !transport.sent.lock().is_empty(),
        "successful flush/shutdown followed by drop lost every queued packet"
    );
}
#[tokio::test(flavor = "current_thread")]
async fn trait_flush_must_deliver_queued_data_before_drop() {
    trait_write_then_drop(false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn trait_shutdown_must_deliver_queued_data_before_drop() {
    trait_write_then_drop(true).await;
}

fn packet(conv: u32, cmd: u8, sn: u32, data: &[u8]) -> Vec<u8> {
    let mut p = Vec::with_capacity(24 + data.len());
    p.extend_from_slice(&conv.to_le_bytes());
    p.push(cmd);
    p.push(0);
    p.extend_from_slice(&128u16.to_le_bytes());
    p.extend_from_slice(&0u32.to_le_bytes());
    p.extend_from_slice(&sn.to_le_bytes());
    p.extend_from_slice(&0u32.to_le_bytes());
    p.extend_from_slice(&(data.len() as u32).to_le_bytes());
    p.extend_from_slice(data);
    p
}
struct BurstTransport {
    next: AtomicUsize,
    sent: AtomicUsize,
    over_budget_before_ack: AtomicBool,
    batch: bool,
    first_packet: Option<Arc<knet::Notify>>,
}
impl BurstTransport {
    fn take(&self, buf: &mut [u8]) -> io::Result<usize> {
        let index = self.next.fetch_add(1, Ordering::SeqCst);
        if let (0, Some(gate)) = (index, self.first_packet.as_ref()) {
            gate.notify_one();
        }
        if index >= 128 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        if index >= 64 && self.sent.load(Ordering::SeqCst) == 0 {
            self.over_budget_before_ack.store(true, Ordering::SeqCst);
        }
        let p = packet(42, 81, index as u32, b"x");
        buf[..p.len()].copy_from_slice(&p);
        Ok(p.len())
    }
}
#[async_trait::async_trait]
impl PacketTransport for BurstTransport {
    async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        if self.next.load(Ordering::SeqCst) >= 128 {
            std::future::pending().await
        } else {
            self.take(buf)
        }
    }
    fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.take(buf)
    }
    async fn send_batch(&self, packets: &[Bytes]) -> io::Result<()> {
        self.sent.fetch_add(packets.len(), Ordering::SeqCst);
        Ok(())
    }
    async fn send_batch_to(&self, packets: &[Bytes], _: SocketAddr) -> io::Result<()> {
        self.send_batch(packets).await
    }
    fn try_send_batch(&self, packets: &[Bytes]) -> io::Result<usize> {
        self.sent.fetch_add(packets.len(), Ordering::SeqCst);
        Ok(packets.len())
    }
    fn try_send_batch_to(&self, packets: &[Bytes], _: SocketAddr) -> io::Result<usize> {
        self.try_send_batch(packets)
    }
    fn supports_recv_batch(&self) -> bool {
        self.batch
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok("127.0.0.1:9000".parse().unwrap())
    }
}
async fn burst_is_bounded(batch: bool) {
    let t = Arc::new(BurstTransport {
        next: AtomicUsize::new(0),
        sent: AtomicUsize::new(0),
        over_budget_before_ack: AtomicBool::new(false),
        batch,
        first_packet: None,
    });
    let conn = KcpStream::with_transport(t.clone(), "127.0.0.1:9001".parse().unwrap())
        .connected(true)
        .conv(42)
        .build()
        .await
        .unwrap();
    let mut b = [0; 1];
    knet::timeout(Duration::from_secs(1), conn.read(&mut b))
        .await
        .unwrap()
        .unwrap();
    assert!(
        !t.over_budget_before_ack.load(Ordering::SeqCst),
        "input consumed more than 64 datagrams before processing any ACK"
    );
}
#[tokio::test(flavor = "current_thread")]
async fn fallback_input_must_bound_burst() {
    burst_is_bounded(false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn batch_input_control_has_budget() {
    burst_is_bounded(true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_fec_configuration_must_be_rejected() {
    let conn = KcpStream::with_transport(
        Arc::new(RecordingTransport::default()),
        "127.0.0.1:9001".parse().unwrap(),
    )
    .background_input(false)
    .spawn_flush_loop(false)
    .fec(200, 100)
    .build()
    .await;
    assert!(
        matches!(conn,Err(ref e) if e.kind()==io::ErrorKind::InvalidInput),
        "FEC 200+100 was silently accepted"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn listener_inline_receive_must_refresh_rx_age() {
    let listener = KcpListener::bind("127.0.0.1:0").build().await.unwrap();
    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let probe = packet(42, 83, 0, &[]);
    client.send_to(&probe, addr).unwrap();
    let (conn, _) = knet::timeout(Duration::from_secs(2), listener.accept())
        .await
        .unwrap()
        .unwrap();
    knet::sleep_ms(20).await;
    for _ in 0..23 {
        client.send_to(&probe, addr).unwrap();
        knet::sleep_ms(100).await;
    }
    eprintln!("active listener rx_age={} ms", conn.rx_age_ms());
    assert!(
        conn.rx_age_ms() < 300,
        "ongoing inbound probes did not refresh inbound liveness timestamp"
    );
}
async fn mtu_transfer(mtu: u32) -> io::Result<usize> {
    let listener = KcpListener::bind("127.0.0.1:0")
        .mtu(mtu)
        .limits(WorkerPoolLimits::default())
        .build()
        .await?;
    let client = KcpStream::connect(listener.local_addr()?)
        .mtu(mtu)
        .conv(42)
        .build()
        .await?;
    client.write_all(&vec![0x5a; 3500]).await?;
    let (server, _) = knet::timeout(Duration::from_secs(2), listener.accept())
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
    server.set_read_timeout(Some(Duration::from_millis(500)))?;
    let mut received = 0;
    let mut b = vec![0; 3500];
    while received < 3500 {
        let n = server.read(&mut b[received..]).await?;
        if n == 0 {
            break;
        }
        received += n;
    }
    Ok(received)
}
#[tokio::test(flavor = "current_thread")]
async fn mtu_4096_must_be_rejected_before_receive_truncation() {
    assert_eq!(
        mtu_transfer(4096).await.unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    let conn = KcpStream::connect("127.0.0.1:9001").mtu(4096).build().await;
    assert!(matches!(conn, Err(e) if e.kind() == io::ErrorKind::InvalidInput));
}
#[tokio::test(flavor = "current_thread")]
async fn mtu_1400_control_transfers() {
    assert_eq!(mtu_transfer(1400).await.unwrap(), 3500);
}

struct BlockedTransport {
    sent: Mutex<Vec<Bytes>>,
    blocked: AtomicBool,
    prefix: AtomicUsize,
    fail_async: AtomicBool,
    gate: knet::Notify,
}

impl BlockedTransport {
    fn new(prefix: usize) -> Self {
        Self {
            sent: Mutex::new(Vec::new()),
            blocked: AtomicBool::new(true),
            prefix: AtomicUsize::new(prefix),
            fail_async: AtomicBool::new(false),
            gate: knet::Notify::new(),
        }
    }

    fn release(&self) {
        self.blocked.store(false, Ordering::Release);
        self.gate.notify_one();
    }
}

#[async_trait::async_trait]
impl PacketTransport for BlockedTransport {
    async fn recv(&self, _: &mut [u8]) -> io::Result<usize> {
        std::future::pending().await
    }
    fn try_recv(&self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::ErrorKind::WouldBlock.into())
    }
    async fn send_batch(&self, packets: &[Bytes]) -> io::Result<()> {
        while self.blocked.load(Ordering::Acquire) {
            self.gate.notified().await;
        }
        if self.fail_async.load(Ordering::Acquire) {
            return Err(io::Error::other("injected async TX failure"));
        }
        self.sent.lock().extend_from_slice(packets);
        Ok(())
    }
    async fn send_batch_to(&self, packets: &[Bytes], _: SocketAddr) -> io::Result<()> {
        self.send_batch(packets).await
    }
    fn try_send_batch(&self, packets: &[Bytes]) -> io::Result<usize> {
        if self.blocked.load(Ordering::Acquire) {
            let prefix = self.prefix.swap(0, Ordering::AcqRel).min(packets.len());
            if prefix == 0 {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            self.sent.lock().extend_from_slice(&packets[..prefix]);
            return Ok(prefix);
        }
        self.sent.lock().extend_from_slice(packets);
        Ok(packets.len())
    }
    fn try_send_batch_to(&self, packets: &[Bytes], _: SocketAddr) -> io::Result<usize> {
        self.try_send_batch(packets)
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok("127.0.0.1:9000".parse().unwrap())
    }
}

fn recorded_payload(packets: &[Bytes]) -> Vec<u8> {
    let mut segments = Vec::new();
    for packet in packets {
        let mut rest = packet.as_ref();
        while rest.len() >= 24 {
            let len = u32::from_le_bytes(rest[20..24].try_into().unwrap()) as usize;
            assert!(rest.len() >= 24 + len);
            if rest[4] == 81 {
                let sn = u32::from_le_bytes(rest[12..16].try_into().unwrap());
                segments.push((sn, rest[24..24 + len].to_vec()));
            }
            rest = &rest[24 + len..];
        }
        assert!(rest.is_empty());
    }
    segments.sort_by_key(|(sn, _)| *sn);
    segments.into_iter().flat_map(|(_, data)| data).collect()
}

#[tokio::test(flavor = "current_thread")]
async fn partial_flush_waits_for_wire_suffix_without_replaying_prefix_or_ack_wait() {
    let transport = Arc::new(BlockedTransport::new(1));
    let mut conn = KcpStream::with_transport(transport.clone(), "127.0.0.1:9001".parse().unwrap())
        .connected(true)
        .background_input(false)
        .spawn_flush_loop(false)
        .build()
        .await
        .unwrap();
    let payload = vec![0x5a; 3500];
    knet::AsyncWriteExt::write_all(&mut conn, &payload)
        .await
        .unwrap();
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(knet::AsyncWrite::poll_flush(std::pin::Pin::new(&mut conn), &mut cx).is_pending());
    assert_eq!(transport.sent.lock().len(), 1);
    transport.release();
    // There is no recv/ACK path in this transport: successful completion is
    // specifically local delivery, not remote acknowledgement.
    knet::timeout(
        Duration::from_secs(1),
        knet::AsyncWriteExt::flush(&mut conn),
    )
    .await
    .unwrap()
    .unwrap();
    drop(conn);
    assert_eq!(recorded_payload(&transport.sent.lock()), payload);
}

#[tokio::test(flavor = "current_thread")]
async fn blocked_borrowed_half_shutdown_drains_and_keeps_read_half_open() {
    let transport = Arc::new(BlockedTransport::new(0));
    let conn = KcpStream::with_transport(transport.clone(), "127.0.0.1:9001".parse().unwrap())
        .connected(true)
        .background_input(false)
        .spawn_flush_loop(false)
        .build()
        .await
        .unwrap();
    let (_read, mut write) = conn.split();
    knet::AsyncWriteExt::write_all(&mut write, b"borrowed tail")
        .await
        .unwrap();
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(knet::AsyncWrite::poll_shutdown(std::pin::Pin::new(&mut write), &mut cx).is_pending());
    let error = knet::AsyncWriteExt::write_all(&mut write, b"closed")
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    transport.release();
    knet::timeout(
        Duration::from_secs(1),
        knet::AsyncWriteExt::shutdown(&mut write),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!conn.is_closed(), "shutdown must preserve the read half");
    assert_eq!(recorded_payload(&transport.sent.lock()), b"borrowed tail");
}

#[tokio::test(flavor = "current_thread")]
async fn blocked_owned_half_flush_and_shutdown_keep_connection_until_last_half_drop() {
    let transport = Arc::new(BlockedTransport::new(0));
    let conn = KcpStream::with_transport(transport.clone(), "127.0.0.1:9001".parse().unwrap())
        .connected(true)
        .background_input(false)
        .spawn_flush_loop(false)
        .build()
        .await
        .unwrap();
    let observer = conn.clone();
    let (read, mut write) = conn.into_split();
    knet::AsyncWriteExt::write_all(&mut write, b"owned tail")
        .await
        .unwrap();
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(knet::AsyncWrite::poll_flush(std::pin::Pin::new(&mut write), &mut cx).is_pending());
    transport.release();
    knet::timeout(
        Duration::from_secs(1),
        knet::AsyncWriteExt::flush(&mut write),
    )
    .await
    .unwrap()
    .unwrap();
    knet::AsyncWriteExt::shutdown(&mut write).await.unwrap();
    assert!(!observer.is_closed());
    drop(write);
    assert!(
        !observer.is_closed(),
        "the surviving read half still owns the connection"
    );
    drop(read);
    assert!(observer.is_closed());
    assert_eq!(recorded_payload(&transport.sent.lock()), b"owned tail");
}

#[tokio::test(flavor = "current_thread")]
async fn discarded_fec_timeout_is_a_latched_flush_error_for_all_clones() {
    let transport = Arc::new(BlockedTransport::new(0));
    let mut conn = KcpStream::with_transport(transport.clone(), "127.0.0.1:9001".parse().unwrap())
        .connected(true)
        .background_input(false)
        .spawn_flush_loop(false)
        .fec(2, 1)
        .build()
        .await
        .unwrap();
    let mut observer = conn.clone();
    knet::AsyncWriteExt::write_all(&mut conn, b"FEC tail")
        .await
        .unwrap();
    let error = knet::timeout(
        Duration::from_secs(1),
        knet::AsyncWriteExt::flush(&mut conn),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    // Draining take_error and making transport writable must not allow another
    // flush caller to claim the discarded wire data was delivered.
    let _ = conn.take_error();
    transport.release();
    let repeated = knet::AsyncWriteExt::flush(&mut observer).await.unwrap_err();
    assert_eq!(repeated.kind(), io::ErrorKind::TimedOut);
    assert!(transport.sent.lock().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn fec_boundary_configuration_and_disabled_single_zero_are_explicit() {
    let addr = "127.0.0.1:9001".parse().unwrap();
    for (data, parity) in [(u32::MAX, 1), (200, 100)] {
        let result = KcpStream::with_transport(Arc::new(RecordingTransport::default()), addr)
            .background_input(false)
            .spawn_flush_loop(false)
            .fec(data, parity)
            .build()
            .await;
        assert!(matches!(result, Err(e) if e.kind() == io::ErrorKind::InvalidInput));
    }
    for (data, parity) in [(1, 255), (0, u32::MAX), (u32::MAX, 0)] {
        let conn = KcpStream::with_transport(Arc::new(RecordingTransport::default()), addr)
            .background_input(false)
            .spawn_flush_loop(false)
            .fec(data, parity)
            .build()
            .await
            .unwrap();
        assert_eq!(
            conn.kcp_mtu(),
            if data > 0 && parity > 0 { 1342 } else { 1350 }
        );
    }
    let too_small = KcpStream::with_transport(Arc::new(RecordingTransport::default()), addr)
        .background_input(false)
        .spawn_flush_loop(false)
        .fec(2, 1)
        .mtu(50)
        .build()
        .await;
    assert!(matches!(too_small, Err(e) if e.kind() == io::ErrorKind::InvalidInput));
}

#[tokio::test(flavor = "current_thread")]
async fn non_fec_timeout_keeps_flush_pending_until_requeued_suffix_is_sent() {
    let transport = Arc::new(BlockedTransport::new(1));
    let mut conn = KcpStream::with_transport(transport.clone(), "127.0.0.1:9001".parse().unwrap())
        .connected(true)
        .background_input(false)
        .spawn_flush_loop(false)
        .build()
        .await
        .unwrap();
    let payload = vec![0x39; 3500];
    knet::AsyncWriteExt::write_all(&mut conn, &payload)
        .await
        .unwrap();
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(knet::AsyncWrite::poll_flush(std::pin::Pin::new(&mut conn), &mut cx).is_pending());
    // Let the bounded sender give its unsent suffix back to the raw queue.
    knet::sleep_ms(80).await;
    assert!(knet::AsyncWrite::poll_flush(std::pin::Pin::new(&mut conn), &mut cx).is_pending());
    assert_eq!(
        transport.sent.lock().len(),
        1,
        "confirmed prefix must not replay"
    );
    transport.release();
    knet::timeout(
        Duration::from_secs(1),
        knet::AsyncWriteExt::flush(&mut conn),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(recorded_payload(&transport.sent.lock()), payload);
}

#[tokio::test(flavor = "current_thread")]
async fn closing_a_blocked_sender_wakes_flush_instead_of_reporting_success() {
    let transport = Arc::new(BlockedTransport::new(0));
    let owner = KcpStream::with_transport(transport.clone(), "127.0.0.1:9001".parse().unwrap())
        .connected(true)
        .background_input(false)
        .spawn_flush_loop(false)
        .build()
        .await
        .unwrap();
    let mut writer = owner.clone();
    knet::AsyncWriteExt::write_all(&mut writer, b"pending tail")
        .await
        .unwrap();
    let close = async {
        knet::yield_now().await;
        drop(owner);
    };
    let flush = async { knet::AsyncWriteExt::flush(&mut writer).await.unwrap_err() };
    let (_, error) = tokio::join!(close, flush);
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    assert!(transport.sent.lock().is_empty());
}

async fn cancelled_inline_send(fec: bool) {
    let transport = Arc::new(BlockedTransport::new(if fec { 0 } else { 1 }));
    let mut conn = KcpStream::with_transport(transport.clone(), "127.0.0.1:9001".parse().unwrap())
        .connected(true)
        .background_input(false)
        .spawn_flush_loop(false)
        .fec(if fec { 2 } else { 0 }, if fec { 1 } else { 0 })
        .build()
        .await
        .unwrap();
    let payload = vec![0x72; 3500];
    let mut write = Box::pin(conn.write_all(&payload));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(std::future::Future::poll(write.as_mut(), &mut cx).is_pending());
    drop(write); // cancels the inline send while its drained batch is owned
    if fec {
        let error = knet::AsyncWriteExt::flush(&mut conn).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        let _ = conn.take_error();
        let mut observer = conn.clone();
        transport.release();
        assert_eq!(
            knet::AsyncWriteExt::flush(&mut observer)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::Interrupted
        );
        assert!(transport.sent.lock().is_empty());
    } else {
        assert_eq!(
            transport.sent.lock().len(),
            1,
            "cancel must preserve the delivered prefix"
        );
        assert!(knet::AsyncWrite::poll_flush(std::pin::Pin::new(&mut conn), &mut cx).is_pending());
        transport.release();
        knet::timeout(
            Duration::from_secs(1),
            knet::AsyncWriteExt::flush(&mut conn),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(recorded_payload(&transport.sent.lock()), payload);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_inline_non_fec_send_requeues_only_unconfirmed_suffix() {
    cancelled_inline_send(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_inline_fec_send_latches_interrupted_instead_of_fake_flush_success() {
    cancelled_inline_send(true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn completed_async_send_failure_remains_a_terminal_flush_and_shutdown_error() {
    let transport = Arc::new(BlockedTransport::new(0));
    let mut conn = KcpStream::with_transport(transport.clone(), "127.0.0.1:9001".parse().unwrap())
        .connected(true)
        .background_input(false)
        .spawn_flush_loop(false)
        .build()
        .await
        .unwrap();
    knet::AsyncWriteExt::write_all(&mut conn, b"failed tail")
        .await
        .unwrap();
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(knet::AsyncWrite::poll_flush(std::pin::Pin::new(&mut conn), &mut cx).is_pending());
    transport.fail_async.store(true, Ordering::Release);
    transport.release();
    let error = knet::timeout(
        Duration::from_secs(1),
        knet::AsyncWriteExt::flush(&mut conn),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Other);
    // The current-thread sender has returned Err, dropped its batch and
    // released SendToken before this awaiting caller is polled again.
    assert!(
        !conn.is_closed(),
        "a non-fatal TX error need not close the read side"
    );
    let _ = conn.take_error();
    let mut observer = conn.clone();
    assert_eq!(
        knet::AsyncWriteExt::flush(&mut observer)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::Other
    );
    assert_eq!(
        knet::AsyncWriteExt::shutdown(&mut conn)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::Other
    );
    assert!(transport.sent.lock().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn input_burst_gives_other_runtime_tasks_a_turn_before_full_drain() {
    for batch in [false, true] {
        let first_packet = Arc::new(knet::Notify::new());
        let transport = Arc::new(BurstTransport {
            next: AtomicUsize::new(0),
            sent: AtomicUsize::new(0),
            over_budget_before_ack: AtomicBool::new(false),
            batch,
            first_packet: Some(first_packet.clone()),
        });
        let observer = transport.clone();
        let progress = tokio::spawn(async move {
            // This task can only become ready after input has started. Without
            // an explicit yield at the 64-packet budget, all 128 ready packets
            // are consumed before it gets a runtime turn.
            first_packet.notified().await;
            observer.next.load(Ordering::SeqCst)
        });
        let conn = KcpStream::with_transport(transport, "127.0.0.1:9001".parse().unwrap())
            .connected(true)
            .conv(42)
            .build()
            .await
            .unwrap();
        let observed = knet::timeout(Duration::from_secs(1), progress)
            .await
            .unwrap()
            .unwrap();
        assert!(
            observed > 0 && observed < 128,
            "batch={batch}: another task first ran after {observed} receives"
        );
        drop(conn);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn concurrent_flush_callers_with_distinct_wakers_both_complete() {
    let transport = Arc::new(BlockedTransport::new(0));
    let mut conn = KcpStream::with_transport(transport.clone(), "127.0.0.1:9001".parse().unwrap())
        .connected(true)
        .background_input(false)
        .spawn_flush_loop(false)
        .build()
        .await
        .unwrap();
    knet::AsyncWriteExt::write_all(&mut conn, b"shared flush tail")
        .await
        .unwrap();
    let mut a = conn.clone();
    let mut b = conn.clone();
    let entered = Arc::new(AtomicUsize::new(0));
    let entered_a = entered.clone();
    let entered_b = entered.clone();
    let first = tokio::spawn(async move {
        entered_a.fetch_add(1, Ordering::SeqCst);
        knet::AsyncWriteExt::flush(&mut a).await
    });
    let second = tokio::spawn(async move {
        entered_b.fetch_add(1, Ordering::SeqCst);
        knet::AsyncWriteExt::flush(&mut b).await
    });
    knet::timeout(Duration::from_secs(1), async {
        while entered.load(Ordering::SeqCst) < 2 {
            knet::yield_now().await;
        }
    })
    .await
    .unwrap();
    transport.release();
    knet::timeout(Duration::from_secs(1), first)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    knet::timeout(Duration::from_secs(1), second)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        recorded_payload(&transport.sent.lock()),
        b"shared flush tail"
    );
}
