//! Server socket creation: UDP socket with buffer size and DSCP.

use std::net::SocketAddr;

use anyhow::Result;
use log::warn;

/// Bind and tune a UDP fd without registering it with an async runtime.
///
/// Sharded server startup moves this raw fd into its dedicated local runtime
/// before calling `knet::UdpSocket::from_std`, preserving poller ownership.
///
/// Returns the buffers the kernel actually holds: `--sockbuf` is capped by
/// `net.core.{r,w}mem_max` (usually host-owned inside a container), and the
/// caller reports the granted value instead of the requested one.
pub(crate) fn create_udp_socket_std(
    addr: SocketAddr,
    sockbuf: u32,
    dscp: u32,
) -> Result<(std::net::UdpSocket, knet::SocketBuffers)> {
    build_udp(addr, sockbuf, dscp, false)
}

/// SO_REUSEPORT variant of [`create_udp_socket_std`].
pub(crate) fn create_udp_socket_shard_std(
    addr: SocketAddr,
    sockbuf: u32,
    dscp: u32,
) -> Result<(std::net::UdpSocket, knet::SocketBuffers)> {
    build_udp(addr, sockbuf, dscp, true)
}

fn build_udp(
    addr: SocketAddr,
    sockbuf: u32,
    dscp: u32,
    reuse_port: bool,
) -> Result<(std::net::UdpSocket, knet::SocketBuffers)> {
    let socket = socket2::Socket::new(
        if addr.is_ipv4() {
            socket2::Domain::IPV4
        } else {
            socket2::Domain::IPV6
        },
        socket2::Type::DGRAM,
        None,
    )?;
    let buffers = knet::set_socket_buffers(&socket, sockbuf as usize);
    if !buffers.granted() {
        warn!("sockbuf {}", knet::net::sockbuf::describe(&buffers));
    }
    if reuse_port {
        // SO_REUSEPORT: allow N sockets to bind the same addr:port; the kernel
        // distributes inbound datagrams across them (Linux: by connection hash).
        // Not available on Windows — skip silently.
        #[cfg(unix)]
        if let Err(e) = socket.set_reuse_port(true) {
            warn!("set_reuse_port failed: {}", e);
        }
    }
    if dscp > 0 {
        // Go: IPv4 → IP_TOS = dscp << 2; IPv6 → IPV6_TCLASS = dscp (no shift).
        // socket2::set_tos only sets IP_TOS; knet helper adds IPV6_TCLASS
        // for IPv6 sockets on Linux.
        knet::set_dscp_on_socket(&socket, dscp, addr.is_ipv6());
    }
    socket.bind(&addr.into())?;
    socket.set_nonblocking(true)?;
    Ok((socket.into(), buffers))
}
