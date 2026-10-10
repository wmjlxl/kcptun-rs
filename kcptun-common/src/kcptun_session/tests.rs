use super::*;
use knet::{AsyncReadExt, AsyncWriteExt};
use std::net::SocketAddr;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_client_server_session_roundtrip() {
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
    let config = kcp_rs::KcpConfig {
        conv: 0x51_55_58,
        mode: kcp_rs::KcpMode::Fast3,
        datashard: 3,
        parityshard: 1,
        ..Default::default()
    };
    let session_config = KcptunConfig {
        kcp: config,
        smux: smux_rs::Config {
            keepalive_interval: 0,
            keepalive_timeout: 0,
            ..smux_rs::DEFAULT_CONFIG.clone()
        },
        nocomp: false,
        rate_limit: 0,
        offload_profile: OffloadProfile::Tokio,
        ack_stall_secs: 10,
    };
    let key = b"0123456789abcdef0123456789abcdef";
    let client = KcptunSession::connect(socket_a, addr_b, key, "aes", &session_config)
        .await
        .unwrap();
    let server = Arc::new(
        KcptunSession::serve_transport(socket_b, addr_a, key, "aes", &session_config)
            .await
            .unwrap(),
    );

    let server_task = {
        let server = server.clone();
        knet::spawn_task(async move {
            let stream = server.accept().await.unwrap();
            let mut stream = smux_rs::SmuxIo::new(stream, server.flush_notify());
            let mut input = [0u8; 14];
            stream.read_exact(&mut input).await.unwrap();
            assert_eq!(&input, b"common-session");
            stream.write_all(b"roundtrip-ok").await.unwrap();
        })
    };
    let stream = client.open_stream().unwrap();
    let mut stream = smux_rs::SmuxIo::new(stream, client.flush_notify());
    stream.write_all(b"common-session").await.unwrap();
    let mut output = [0u8; 12];
    knet::timeout(Duration::from_secs(5), stream.read_exact(&mut output))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&output, b"roundtrip-ok");
    server_task.await.unwrap();
    client.close();
    server.close();
}
