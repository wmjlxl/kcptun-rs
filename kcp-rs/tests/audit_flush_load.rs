#![cfg(feature = "async")]

use kcp_rs::{KcpListener, KcpStream};
use std::{io, sync::Arc, time::Duration};

const CLIENTS: usize = 32;
const PAYLOAD_SIZE: usize = 128 * 1024;

fn payload(id: u32) -> Vec<u8> {
    let mut bytes: Vec<_> = (0..PAYLOAD_SIZE)
        .map(|offset| ((offset as u32).wrapping_mul(31) ^ id) as u8)
        .collect();
    bytes[..4].copy_from_slice(&id.to_le_bytes());
    bytes
}

fn checksum(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

async fn read_exact(stream: &KcpStream, buffer: &mut [u8]) -> io::Result<()> {
    let mut offset = 0;
    while offset < buffer.len() {
        let received = stream.read(&mut buffer[offset..]).await?;
        if received == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        offset += received;
    }
    Ok(())
}

async fn concurrent_flush_load(fec: bool) {
    knet::timeout(Duration::from_secs(20), async move {
        // Ephemeral loopback ports: this test neither launches binaries nor
        // signals processes owned by other agents in the original workspace.
        let listener = Arc::new(
            KcpListener::bind("127.0.0.1:0")
                .fec(if fec { 3 } else { 0 }, if fec { 2 } else { 0 })
                .build()
                .await
                .unwrap(),
        );
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn({
            let listener = listener.clone();
            async move {
                let mut workers = Vec::new();
                for _ in 0..CLIENTS {
                    let (mut connection, _) = listener.accept().await.unwrap();
                    workers.push(tokio::spawn(async move {
                        let mut received = vec![0; PAYLOAD_SIZE];
                        read_exact(&connection, &mut received).await.unwrap();
                        let id = u32::from_le_bytes(received[..4].try_into().unwrap());
                        assert_eq!(received, payload(id));
                        let reply = checksum(&received).to_le_bytes();
                        knet::AsyncWriteExt::write_all(&mut connection, &reply)
                            .await
                            .unwrap();
                        knet::AsyncWriteExt::flush(&mut connection).await.unwrap();
                        // Keep the sender alive until its transport receipt
                        // has been acknowledged; ordinary flush itself is not
                        // a promise of remote delivery.
                        let mut receipt = [0u8; 1];
                        read_exact(&connection, &mut receipt).await.unwrap();
                        assert_eq!(receipt[0], 1);
                    }));
                }
                for worker in workers {
                    worker.await.unwrap();
                }
            }
        });
        let mut clients = Vec::new();
        for id in 0..CLIENTS as u32 {
            clients.push(tokio::spawn(async move {
                let mut connection = KcpStream::connect(address)
                    .fec(if fec { 3 } else { 0 }, if fec { 2 } else { 0 })
                    .build()
                    .await
                    .unwrap();
                let bytes = payload(id);
                knet::AsyncWriteExt::write_all(&mut connection, &bytes)
                    .await
                    .unwrap();
                knet::AsyncWriteExt::flush(&mut connection).await.unwrap();
                let mut reply = [0u8; 8];
                read_exact(&connection, &mut reply).await.unwrap();
                assert_eq!(u64::from_le_bytes(reply), checksum(&bytes));
                knet::AsyncWriteExt::write_all(&mut connection, &[1])
                    .await
                    .unwrap();
                knet::AsyncWriteExt::flush(&mut connection).await.unwrap();
            }));
        }
        for client in clients {
            client.await.unwrap();
        }
        server.await.unwrap();
        listener.close();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_plaintext_trait_flush_preserves_all_payloads() {
    concurrent_flush_load(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_fec_trait_flush_preserves_all_payloads() {
    concurrent_flush_load(true).await;
}
