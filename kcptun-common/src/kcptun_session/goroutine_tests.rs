use super::*;
use knet::{AsyncReadExt, AsyncWriteExt};
use std::net::SocketAddr;

#[test]
fn fresh_stream_echoes_do_not_wait_for_idle_flush_tick() {
    const ROUNDS: usize = 32;
    knet::block_on(async {
        let a = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let b = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let addr_a = a.local_addr().unwrap();
        let addr_b = b.local_addr().unwrap();
        drop(a);
        drop(b);
        let socket_a = Arc::new(knet::DatagramSocket::Udp(
            knet::UdpSocket::connect(addr_a, addr_b).unwrap(),
        ));
        let socket_b = Arc::new(knet::DatagramSocket::Udp(
            knet::UdpSocket::connect(addr_b, addr_a).unwrap(),
        ));
        let session_config = KcptunConfig {
            kcp: kcp_rs::KcpConfig {
                conv: 0x51_55_58,
                mode: kcp_rs::KcpMode::Fast3,
                datashard: 0,
                parityshard: 0,
                ..Default::default()
            },
            smux: smux_rs::Config {
                keepalive_interval: 0,
                keepalive_timeout: 0,
                ..smux_rs::DEFAULT_CONFIG.clone()
            },
            nocomp: true,
            rate_limit: 0,
            offload_profile: OffloadProfile::Tokio,
            ack_stall_secs: 10,
        };
        let key = b"0123456789abcdef0123456789abcdef";
        let client = KcptunSession::connect(socket_a, addr_b, key, "null", &session_config)
            .await
            .unwrap();
        let server = Arc::new(
            KcptunSession::serve_transport(socket_b, addr_a, key, "null", &session_config)
                .await
                .unwrap(),
        );
        let server_task = {
            let server = server.clone();
            knet::spawn_task(async move {
                for _ in 0..ROUNDS {
                    let stream = server.accept().await.unwrap();
                    let mut stream = smux_rs::SmuxIo::new(stream, server.flush_notify());
                    let mut byte = [0u8; 1];
                    stream.read_exact(&mut byte).await.unwrap();
                    stream.write_all(&byte).await.unwrap();
                }
            })
        };

        let mut worst = Duration::ZERO;
        let mut samples: Vec<Duration> = Vec::with_capacity(ROUNDS);
        for value in 0..ROUNDS as u8 {
            let started = std::time::Instant::now();
            let stream = client.open_stream().unwrap();
            let mut stream = smux_rs::SmuxIo::new(stream, client.flush_notify());
            stream.write_all(&[value]).await.unwrap();
            let mut echoed = [0u8; 1];
            // Bounded read: an unbounded read_exact turned any lost-wake /
            // starvation bug into a >60 s test hang instead of a fast panic.
            knet::timeout(Duration::from_secs(3), stream.read_exact(&mut echoed))
                .await
                .expect("echo read timed out (stream write stalled)")
                .unwrap();
            assert_eq!(echoed, [value]);
            samples.push(started.elapsed());
            worst = worst.max(started.elapsed());
        }
        server_task.await.unwrap();
        client.close();
        server.close();
        // Regression guard for "writer waits for the idle flush tick":
        // the MEDIAN must stay in the sub-millisecond notify path. The
        // worst case on a loaded desktop can absorb one 10 ms idle tick
        // through scheduler jitter alone (IDLE_WAKE_MS), so a hard worst
        // bound only guards gross stalls, not the tick itself.
        samples.sort();
        let median = samples[samples.len() / 2];
        assert!(
            median < Duration::from_millis(5),
            "fresh stream echo median waited for idle flush tick: {median:?} (worst {worst:?})"
        );
        assert!(
            worst < Duration::from_millis(50),
            "fresh stream echo worst-case stall: {worst:?}"
        );
    });
}

/// Exercise the production forwarding shape end to end: a fresh local TCP
/// connection is piped to a fresh SMUX stream, the server side opens a
/// fresh target TCP connection and pipes it back to a TCP echo listener.
/// The lighter stream-only test above cannot catch a wake lost between the
/// two `copy_bidirectional` state machines.
#[test]
fn tcp_smux_tcp_proxy_does_not_wait_for_idle_ticks() {
    const ROUNDS: usize = 16;
    knet::block_on(async {
        let a = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let b = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let addr_a = a.local_addr().unwrap();
        let addr_b = b.local_addr().unwrap();
        drop(a);
        drop(b);
        let socket_a = Arc::new(knet::DatagramSocket::Udp(
            knet::UdpSocket::connect(addr_a, addr_b).unwrap(),
        ));
        let socket_b = Arc::new(knet::DatagramSocket::Udp(
            knet::UdpSocket::connect(addr_b, addr_a).unwrap(),
        ));
        let session_config = KcptunConfig {
            kcp: kcp_rs::KcpConfig {
                conv: 0x51_55_58,
                mode: kcp_rs::KcpMode::Fast3,
                datashard: 0,
                parityshard: 0,
                ..Default::default()
            },
            smux: smux_rs::Config {
                keepalive_interval: 0,
                keepalive_timeout: 0,
                ..smux_rs::DEFAULT_CONFIG.clone()
            },
            nocomp: true,
            rate_limit: 0,
            offload_profile: OffloadProfile::Tokio,
            ack_stall_secs: 10,
        };
        let key = b"0123456789abcdef0123456789abcdef";
        let client = Arc::new(
            KcptunSession::connect(socket_a, addr_b, key, "null", &session_config)
                .await
                .unwrap(),
        );
        let server = Arc::new(
            KcptunSession::serve_transport(socket_b, addr_a, key, "null", &session_config)
                .await
                .unwrap(),
        );

        let target_listener = knet::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let target_addr = target_listener.local_addr().unwrap();
        let target_task = knet::spawn_task(async move {
            let mut handlers = Vec::with_capacity(ROUNDS);
            for _ in 0..ROUNDS {
                let (mut target, _) = target_listener.accept().await.unwrap();
                handlers.push(knet::spawn_task(async move {
                    let mut byte = [0u8; 1];
                    target.read_exact(&mut byte).await.unwrap();
                    target.write_all(&byte).await.unwrap();
                }));
            }
            for handler in handlers {
                handler.await.unwrap();
            }
        });

        let server_task = {
            let server = server.clone();
            knet::spawn_task(async move {
                let mut handlers = Vec::with_capacity(ROUNDS);
                for _ in 0..ROUNDS {
                    let stream = server.accept().await.unwrap();
                    let notify = server.flush_notify();
                    handlers.push(knet::spawn_task(async move {
                        let mut target = knet::TcpStream::connect(target_addr.to_string())
                            .await
                            .unwrap();
                        let mut smux = smux_rs::SmuxIo::new(stream, notify);
                        crate::pipe(&mut target, &mut smux, 0).await.unwrap();
                    }));
                }
                for handler in handlers {
                    handler.await.unwrap();
                }
            })
        };

        let local_listener = knet::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let local_addr = local_listener.local_addr().unwrap();
        let client_task = {
            let client = client.clone();
            knet::spawn_task(async move {
                let mut handlers = Vec::with_capacity(ROUNDS);
                for _ in 0..ROUNDS {
                    let (mut local, _) = local_listener.accept().await.unwrap();
                    let stream = client.open_stream().unwrap();
                    let notify = client.flush_notify();
                    handlers.push(knet::spawn_task(async move {
                        let mut smux = smux_rs::SmuxIo::new(stream, notify);
                        crate::pipe(&mut local, &mut smux, 0).await.unwrap();
                    }));
                }
                for handler in handlers {
                    handler.await.unwrap();
                }
            })
        };

        let mut worst = Duration::ZERO;
        let mut samples: Vec<Duration> = Vec::with_capacity(ROUNDS);
        for value in 0..ROUNDS as u8 {
            let started = std::time::Instant::now();
            let mut local = knet::TcpStream::connect(local_addr.to_string())
                .await
                .unwrap();
            local.write_all(&[value]).await.unwrap();
            let mut echoed = [0u8; 1];
            local.read_exact(&mut echoed).await.unwrap();
            assert_eq!(echoed, [value]);
            samples.push(started.elapsed());
            worst = worst.max(started.elapsed());
        }
        client_task.await.unwrap();
        server_task.await.unwrap();
        target_task.await.unwrap();
        client.close();
        server.close();
        samples.sort();
        let median = samples[samples.len() / 2];
        assert!(
            median < Duration::from_millis(5),
            "full TCP/SMUX/TCP forwarding median waited for an idle tick: {median:?} (worst {worst:?})"
        );
        assert!(
            worst < Duration::from_millis(50),
            "full TCP/SMUX/TCP forwarding worst-case stall: {worst:?}"
        );
    });
}
