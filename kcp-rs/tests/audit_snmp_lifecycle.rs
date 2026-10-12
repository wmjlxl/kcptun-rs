#![cfg(feature = "async")]

use bytes::Bytes;
use kcp_rs::{snmp_enable, KcpStream, PacketTransport, DEFAULT_SNMP};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

struct QuietTransport;

#[async_trait::async_trait]
impl PacketTransport for QuietTransport {
    async fn recv(&self, _: &mut [u8]) -> io::Result<usize> {
        std::future::pending().await
    }
    fn try_recv(&self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::ErrorKind::WouldBlock.into())
    }
    async fn send_batch(&self, _: &[Bytes]) -> io::Result<()> {
        Ok(())
    }
    async fn send_batch_to(&self, packets: &[Bytes], _: SocketAddr) -> io::Result<()> {
        self.send_batch(packets).await
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok("127.0.0.1:9000".parse().unwrap())
    }
}

async fn connection(passive: bool) -> KcpStream {
    KcpStream::with_transport(Arc::new(QuietTransport), "127.0.0.1:9001".parse().unwrap())
        .connected(true)
        .background_input(false)
        .spawn_flush_loop(false)
        .adopt_conv(passive)
        .build()
        .await
        .unwrap()
}

// Kept in its own integration binary: process-wide SNMP enable cannot race
// unrelated tests and a late-enable gauge can be checked deterministically.
#[tokio::test(flavor = "current_thread")]
async fn lifecycle_tracks_unique_close_reset_split_and_late_enable() {
    assert_eq!(DEFAULT_SNMP.curr_estab.load(Ordering::Acquire), 0);
    let early = connection(false).await;
    assert_eq!(DEFAULT_SNMP.curr_estab.load(Ordering::Acquire), 1);
    assert_eq!(DEFAULT_SNMP.active_opens.load(Ordering::Acquire), 0);
    drop(early.clone());
    assert_eq!(DEFAULT_SNMP.curr_estab.load(Ordering::Acquire), 1);

    snmp_enable();
    DEFAULT_SNMP.reset();
    assert_eq!(DEFAULT_SNMP.curr_estab.load(Ordering::Acquire), 1);
    let (read, write) = early.into_split();
    drop(read);
    assert_eq!(DEFAULT_SNMP.curr_estab.load(Ordering::Acquire), 1);
    drop(write);
    assert_eq!(DEFAULT_SNMP.curr_estab.load(Ordering::Acquire), 0);

    let passive = connection(true).await;
    assert_eq!(DEFAULT_SNMP.passive_opens.load(Ordering::Acquire), 1);
    passive.close();
    passive.close();
    drop(passive);
    assert_eq!(DEFAULT_SNMP.curr_estab.load(Ordering::Acquire), 0);

    let active = connection(false).await;
    assert_eq!(DEFAULT_SNMP.active_opens.load(Ordering::Acquire), 1);
    DEFAULT_SNMP.reset();
    assert_eq!(DEFAULT_SNMP.curr_estab.load(Ordering::Acquire), 1);
    drop(active);
    assert_eq!(DEFAULT_SNMP.curr_estab.load(Ordering::Acquire), 0);

    let rejected =
        KcpStream::with_transport(Arc::new(QuietTransport), "127.0.0.1:9001".parse().unwrap())
            .fec(200, 100)
            .build()
            .await;
    assert!(matches!(rejected, Err(ref e) if e.kind() == io::ErrorKind::InvalidInput));
    assert_eq!(DEFAULT_SNMP.curr_estab.load(Ordering::Acquire), 0);
}
