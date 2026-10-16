use super::*;
use kcp_rs::KcpConfig;
use knet::AsyncReadExt;
use knet::AsyncWriteExt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::kcp_transport::kcp_stream_with_socket;
use kcrypt_rs::OffloadProfile;

/// Pair of connected UDP sockets + null-crypt KcpStream.
async fn make_pair(cfg: KcpConfig) -> (kcp_rs::KcpStream, kcp_rs::KcpStream) {
    let a_tmp = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
    let b_tmp = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
    let addr_a = a_tmp.local_addr().unwrap();
    let addr_b = b_tmp.local_addr().unwrap();
    drop(a_tmp);
    drop(b_tmp);

    let sock_a = Arc::new(knet::DatagramSocket::Udp(
        knet::UdpSocket::connect(addr_a, addr_b).unwrap(),
    ));
    let sock_b = Arc::new(knet::DatagramSocket::Udp(
        knet::UdpSocket::connect(addr_b, addr_a).unwrap(),
    ));

    let key = b"0123456789abcdef0123456789abcdef";
    let a = kcp_stream_with_socket(
        sock_a,
        addr_b,
        key,
        "null",
        cfg.clone(),
        true,
        OffloadProfile::Tokio,
    )
    .await
    .unwrap();
    let b = kcp_stream_with_socket(
        sock_b,
        addr_a,
        key,
        "null",
        cfg,
        true,
        OffloadProfile::Tokio,
    )
    .await
    .unwrap();
    (a, b)
}

fn test_cfg() -> KcpConfig {
    KcpConfig {
        conv: 0x5A11_5EED,
        mode: kcp_rs::KcpMode::Fast3,
        sndwnd: 512,
        rcvwnd: 512,
        ..KcpConfig::default()
    }
}

async fn read_exact(conn: &mut (impl knet::AsyncRead + Unpin), buf: &mut [u8], limit: Duration) {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snappy_pipe_keeps_unfinished_chunk_within_one_read() {
    let (mut sender, receiver) = tokio::io::duplex(64 * 1024);
    let mut plain = vec![0u8; 16 * 1024];
    let mut state = 1u32;
    for byte in &mut plain {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        *byte = state as u8;
    }
    let mut encoder = snap::write::FrameEncoder::new(Vec::new());
    std::io::Write::write_all(&mut encoder, &plain).unwrap();
    let wire = encoder.into_inner().unwrap();
    assert!(wire.len() > 8 * 1024);
    sender.write_all(&wire).await.unwrap();

    let mut pipe = SnappyPipe::new(receiver, true);
    let mut got = vec![0u8; plain.len()];
    read_exact(&mut pipe, &mut got, Duration::from_secs(1)).await;
    assert_eq!(got, plain);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snappy_pipe_streams_many_compressed_chunks_from_one_read() {
    let (mut sender, receiver) = tokio::io::duplex(64 * 1024);
    let plain = vec![0u8; 17 * 64 * 1024];
    let mut encoder = snap::write::FrameEncoder::new(Vec::new());
    std::io::Write::write_all(&mut encoder, &plain).unwrap();
    let wire = encoder.into_inner().unwrap();
    assert!(wire.len() < 64 * 1024);
    sender.write_all(&wire).await.unwrap();

    let mut pipe = SnappyPipe::new(receiver, true);
    let mut got = vec![0u8; plain.len()];
    read_exact(&mut pipe, &mut got, Duration::from_secs(10)).await;
    assert_eq!(got, plain);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snappy_pipe_compressed_kcp_roundtrip() {
    let (a, b) = make_pair(test_cfg()).await;
    let mut a = SnappyPipe::new(a, true);
    let mut b = SnappyPipe::new(b, true);

    // Mixed payload: compressible prefix + varied tail, split across several
    // writes to exercise the persistent stream (single sNaPpY header).
    let mut payload = Vec::with_capacity(256 * 1024);
    payload.extend(std::iter::repeat_n(b'a', 128 * 1024));
    payload.extend((0u32..131072).map(|i| (i % 251) as u8));

    let mut written = 0usize;
    while written < payload.len() {
        let end = (written + 16 * 1024).min(payload.len());
        a.write_all(&payload[written..end]).await.unwrap();
        a.flush().await.unwrap();
        written = end;
    }

    let mut got = vec![0u8; payload.len()];
    read_exact(&mut b, &mut got, Duration::from_secs(10)).await;
    assert_eq!(got, payload);

    // Reverse direction too.
    let reply = b"snappy-pipe-reverse-reply!";
    b.write_all(reply).await.unwrap();
    b.flush().await.unwrap();
    let mut got_reply = vec![0u8; reply.len()];
    read_exact(&mut a, &mut got_reply, Duration::from_secs(10)).await;
    assert_eq!(&got_reply[..], reply);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snappy_pipe_passthrough_kcp_roundtrip() {
    let (a, b) = make_pair(test_cfg()).await;
    let mut a = SnappyPipe::new(a, false);
    let mut b = SnappyPipe::new(b, false);

    let payload = (0u32..65536).map(|i| (i % 251) as u8).collect::<Vec<_>>();
    a.write_all(&payload).await.unwrap();
    a.flush().await.unwrap();

    let mut got = vec![0u8; payload.len()];
    read_exact(&mut b, &mut got, Duration::from_secs(10)).await;
    assert_eq!(got, payload);
}
