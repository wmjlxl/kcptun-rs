//! Correct-behavior regressions for the 2026-10-11 listener audit.
#![cfg(feature = "async")]

use kcp_rs::{KcpListener, KcpStream, KcpTcpListener, WorkerPoolLimits};
use std::io;
use std::net::UdpSocket;
use std::time::{Duration, Instant};

fn packet(conv: u32, cmd: u8, payload: &[u8]) -> Vec<u8> {
    let mut p = vec![0u8; 24];
    p[..4].copy_from_slice(&conv.to_le_bytes());
    p[4] = cmd;
    p[6..8].copy_from_slice(&32u16.to_le_bytes());
    p[20..24].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    p.extend_from_slice(payload);
    p
}

fn raw_peer() -> UdpSocket {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket.set_nonblocking(true).unwrap();
    socket
}

async fn wait_until(mut ready: impl FnMut() -> bool, reason: &str) {
    let deadline = Instant::now() + Duration::from_secs(4);
    while !ready() {
        assert!(Instant::now() < deadline, "{reason}");
        knet::sleep_ms(5).await;
    }
}

async fn wait_for_reply(socket: &UdpSocket) {
    let mut buffer = [0u8; 2048];
    wait_until(
        || match socket.recv_from(&mut buffer) {
            Ok(_) => true,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => false,
            Err(e) => panic!("UDP reply: {e}"),
        },
        "initial probe did not reach KCP",
    )
    .await;
}

#[test]
fn remove_peer_cancels_building_peer() {
    knet::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0")
            .testing_build_delay(Duration::from_millis(300))
            .build()
            .await
            .unwrap();
        let socket = raw_peer();
        let peer = socket.local_addr().unwrap();
        socket
            .send_to(&packet(42, 83, &[]), listener.local_addr().unwrap())
            .unwrap();
        wait_until(|| listener.building_count() == 1, "build never started").await;
        assert!(
            listener.remove_peer(peer),
            "a removed build must return true"
        );
        assert_eq!(listener.building_count(), 0);
        assert!(
            !listener.remove_peer(peer),
            "an absent peer must return false"
        );
        knet::sleep_ms(400).await;
        assert_eq!(listener.session_count(), 0);
        assert!(listener.try_accept().unwrap().is_none());
        listener.close();
    });
}

#[test]
fn idle_reaper_removes_expired_pending_connections() {
    knet::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0")
            .limits(WorkerPoolLimits {
                max_sessions_per_worker: 1,
                per_ip_session_rate: 0,
                idle_timeout: Duration::from_millis(20),
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        let mut sockets = Vec::new();
        for _ in 0..3 {
            let socket = raw_peer();
            socket
                .send_to(&packet(42, 83, &[]), listener.local_addr().unwrap())
                .unwrap();
            sockets.push(socket);
            wait_until(|| listener.session_count() == 1, "session never published").await;
            wait_until(
                || listener.session_count() == 0 && listener.building_count() == 0,
                "idle session never reaped",
            )
            .await;
        }
        assert!(
            listener.try_accept().unwrap().is_none(),
            "expired connections must not accumulate beyond the cap or be accepted"
        );
        listener.close();
    });
}

#[test]
fn cancelled_build_cannot_remove_replacement_from_same_address() {
    knet::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0")
            .testing_build_delay(Duration::from_millis(300))
            .build()
            .await
            .unwrap();
        let socket = raw_peer();
        let peer = socket.local_addr().unwrap();
        let addr = listener.local_addr().unwrap();
        socket.send_to(&packet(42, 83, &[]), addr).unwrap();
        wait_until(|| listener.building_count() == 1, "old build never started").await;
        assert!(listener.remove_peer(peer));
        socket
            .send_to(&packet(43, 81, b"new generation"), addr)
            .unwrap();
        wait_until(
            || listener.building_count() == 1,
            "replacement build never started",
        )
        .await;
        knet::sleep_ms(400).await;
        let (conn, accepted_peer) = listener.try_accept().unwrap().expect("replacement lost");
        assert_eq!(accepted_peer, peer);
        assert!(!conn.is_closed());
        assert_eq!(listener.session_count(), 1);
        assert!(
            listener.try_accept().unwrap().is_none(),
            "old build was also published"
        );
        let mut data = [0u8; 14];
        let n = knet::timeout(Duration::from_secs(1), conn.read(&mut data))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&data[..n], b"new generation");
        listener.close();
    });
}

#[test]
fn stale_eviction_discards_old_backlog_and_accepts_only_new_generation() {
    knet::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0").build().await.unwrap();
        let socket = raw_peer();
        let addr = listener.local_addr().unwrap();
        socket.send_to(&packet(42, 83, &[]), addr).unwrap();
        wait_until(
            || listener.session_count() == 1,
            "old session never published",
        )
        .await;
        wait_for_reply(&socket).await;
        for _ in 0..3 {
            socket.send_to(&packet(43, 83, &[]), addr).unwrap();
            knet::sleep_ms(550).await;
        }
        wait_until(
            || listener.session_count() == 0,
            "old generation was not evicted",
        )
        .await;
        socket.send_to(&packet(43, 81, b"new"), addr).unwrap();
        let (conn, _) = knet::timeout(Duration::from_secs(2), listener.accept())
            .await
            .unwrap()
            .unwrap();
        assert!(!conn.is_closed(), "stale backlog connection was accepted");
        let mut data = [0u8; 3];
        let n = knet::timeout(Duration::from_secs(1), conn.read(&mut data))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&data[..n], b"new");
        assert!(listener.try_accept().unwrap().is_none());
        listener.close();
    });
}

#[test]
fn listener_close_preserves_already_accepted_connection() {
    knet::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0").build().await.unwrap();
        let client = KcpStream::connect(listener.local_addr().unwrap())
            .build()
            .await
            .unwrap();
        client.write_all(b"before").await.unwrap();
        let (server, _) = listener
            .accept_timeout(Duration::from_secs(2))
            .await
            .unwrap();
        let mut data = [0u8; 6];
        assert_eq!(server.read(&mut data).await.unwrap(), 6);
        assert_eq!(&data, b"before");
        listener.close();
        client.write_all(b"after").await.unwrap();
        let mut data = [0u8; 5];
        let n = knet::timeout(Duration::from_secs(2), server.read(&mut data))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&data[..n], b"after");
        assert!(!server.is_closed());
        assert!(
            matches!(listener.try_accept(), Err(e) if e.kind() == io::ErrorKind::ConnectionAborted)
        );
    });
}

#[test]
fn listener_rejects_invalid_async_configuration_before_binding() {
    knet::block_on(async {
        for (mtu, data, parity) in [(49, 0, 0), (4096, 0, 0), (50, 10, 3), (1400, 200, 100)] {
            let result = KcpListener::bind("127.0.0.1:0")
                .mtu(mtu)
                .fec(data, parity)
                .build()
                .await;
            assert!(matches!(result, Err(e) if e.kind() == io::ErrorKind::InvalidInput));
        }
    });
}

#[test]
fn raw_tcp_factory_rejects_invalid_configuration_before_platform_io() {
    for (mtu, data, parity) in [(4096, 0, 0), (50, 10, 3), (1400, 200, 100)] {
        let result = KcpTcpListener::bind("127.0.0.1:0")
            .mtu(mtu)
            .fec(data, parity)
            .build();
        assert!(matches!(result, Err(e) if e.kind() == io::ErrorKind::InvalidInput));
    }
}
