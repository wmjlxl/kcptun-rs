//! Network socket wrappers.
//!
//! All sockets (TCP + UDP) are created via `socket2` for uniform buffer
//! tuning, SO_REUSEADDR, and non-blocking mode. The raw fd is then handed
//! to tokio's async wrapper.

use std::io;
use std::net::SocketAddr;

/// UDP recv/send buffer size (4 MB).
const SOCK_BUF: usize = 4 * 1024 * 1024;

/// Create a tuned, non-blocking `std::net::UdpSocket` via socket2.
///
/// - 4 MB recv/send buffer sizes
/// - SO_REUSEADDR
/// - non-blocking mode
///
/// If `remote_addr` is provided, the socket is `connect()`ed (client mode).
#[cfg(not(target_os = "windows"))]
pub(crate) fn raw_udp(
    bind_addr: SocketAddr,
    remote_addr: Option<SocketAddr>,
) -> io::Result<std::net::UdpSocket> {
    let domain = if bind_addr.is_ipv4() {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };
    let socket = socket2::Socket::new(domain, socket2::Type::DGRAM, None)?;

    let _ = socket.set_recv_buffer_size(SOCK_BUF);
    let _ = socket.set_send_buffer_size(SOCK_BUF);
    let _ = socket.set_reuse_address(true);

    socket.bind(&bind_addr.into())?;
    if let Some(remote) = remote_addr {
        socket.connect(&remote.into())?;
    }
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}

/// Windows fallback: `socket2` is not used for UDP on Windows; use the
/// standard library's `UdpSocket` directly. Buffer tuning is skipped because
/// the standard API doesn't expose per-socket buffer sizing.
#[cfg(target_os = "windows")]
pub(crate) fn raw_udp(
    bind_addr: SocketAddr,
    remote_addr: Option<SocketAddr>,
) -> io::Result<std::net::UdpSocket> {
    let std_sock = std::net::UdpSocket::bind(bind_addr)?;
    if let Some(remote) = remote_addr {
        std_sock.connect(remote)?;
    }
    std_sock.set_nonblocking(true)?;
    Ok(std_sock)
}

/// SO_REUSEPORT variant of [`raw_udp`] (Linux): binds a fresh socket that
/// joins the kernel's reuseport group for `bind_addr`. Every group member
/// receives the flows whose 4-tuple hash maps to it, so N sockets give N-way
/// receive fan-out with stable per-flow affinity and no user-space
/// demultiplexer. All members must be created with this constructor before
/// the group's first bind.
#[cfg(target_os = "linux")]
pub(crate) fn raw_udp_reuseport(bind_addr: SocketAddr) -> io::Result<std::net::UdpSocket> {
    let domain = if bind_addr.is_ipv4() {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };
    let socket = socket2::Socket::new(domain, socket2::Type::DGRAM, None)?;

    let _ = socket.set_recv_buffer_size(SOCK_BUF);
    let _ = socket.set_send_buffer_size(SOCK_BUF);
    let _ = socket.set_reuse_address(true);
    socket.set_reuse_port(true)?;

    socket.bind(&bind_addr.into())?;
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}

/// Create a tuned, non-blocking `std::net::TcpListener` via socket2.
pub(crate) fn raw_tcp_listener(addr: SocketAddr) -> io::Result<std::net::TcpListener> {
    let domain = if addr.is_ipv4() {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };
    let socket = socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;

    let _ = socket.set_recv_buffer_size(SOCK_BUF);
    let _ = socket.set_send_buffer_size(SOCK_BUF);
    socket.set_reuse_address(true)?;

    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}

// ─── Backend selection ────────────────────────────────────────────────────────
#[cfg(target_os = "linux")]
mod mmsg;

mod tokio;

pub mod sockbuf;

#[cfg(unix)]
pub use self::tokio::UnixListener;
pub use self::tokio::{TcpListener, TcpStream, UdpSocket};

// ─── TCP raw transport (Linux only) ──────────────────────────────────────────
#[cfg(target_os = "linux")]
mod tcpraw;

// Pure-arithmetic TCP checksum used by `tcpraw`; compiled for test builds on
// every platform so it can be verified without a Linux host.
#[cfg(any(target_os = "linux", test))]
mod tcp_pkt;

#[cfg(not(target_os = "linux"))]
mod tcpraw_stub;

#[cfg(target_os = "linux")]
pub use self::tcpraw::{dial as tcpraw_dial, listen as tcpraw_listen, TcpRawConn, TcpRawListener};

#[cfg(not(target_os = "linux"))]
pub use self::tcpraw_stub::{
    dial as tcpraw_dial, listen as tcpraw_listen, TcpRawConn, TcpRawListener,
};

// ─── DatagramSocket ──────────────────────────────────────────────────────────

/// Unified datagram socket over UDP or TCP raw transport.
///
/// Match-based dispatch — no trait/generic overhead, and the transport set
/// is closed (UDP + optionally TCP raw on Linux).
pub enum DatagramSocket {
    /// Standard UDP socket.
    Udp(UdpSocket),
    /// TCP raw socket (Linux only; compile-error on other platforms when
    /// constructed, but the variant exists for code coherence).
    TcpRaw(TcpRawConn),
}

impl DatagramSocket {
    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        match self {
            Self::Udp(s) => s.recv_from(buf).await,
            Self::TcpRaw(s) => s.recv_from(buf).await,
        }
    }

    pub async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        match self {
            Self::Udp(s) => s.send_to(buf, target).await,
            Self::TcpRaw(s) => s.send_to(buf, &target),
        }
    }

    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Udp(s) => s.send(buf).await,
            Self::TcpRaw(s) => s.send(buf),
        }
    }

    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Udp(s) => s.recv(buf).await,
            Self::TcpRaw(s) => s.recv(buf).await,
        }
    }

    pub fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Udp(s) => s.try_recv(buf),
            Self::TcpRaw(s) => s.try_recv(buf),
        }
    }

    /// Wait until the socket can accept another send. Shared by every session
    /// of a listener, so callers that find the kernel buffer full park here
    /// instead of polling. A raw-TCP transport has no readiness signal and
    /// returns immediately.
    pub async fn writable(&self) -> io::Result<()> {
        match self {
            Self::Udp(s) => s.writable().await,
            Self::TcpRaw(_) => Ok(()),
        }
    }

    pub fn try_recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        match self {
            Self::Udp(s) => s.try_recv_from(buf),
            Self::TcpRaw(s) => s.try_recv_from(buf),
        }
    }

    /// Try to send all `bufs` on a connected socket without blocking.
    ///
    /// Returns the number of packets actually sent (stops on `WouldBlock`).
    /// Returns `0` for transports that can't send synchronously (TcpRaw), so
    /// the caller falls back to the async path.
    pub fn try_send_batch<B: AsRef<[u8]>>(&self, bufs: &[B]) -> io::Result<usize> {
        match self {
            Self::Udp(s) => s.try_send_batch(bufs),
            Self::TcpRaw(_) => Ok(0),
        }
    }

    /// Try to send all `bufs` to an explicit peer (unconnected socket) without blocking.
    ///
    /// Returns the number of packets actually sent (stops on `WouldBlock`).
    /// Returns `0` for transports that can't send synchronously (TcpRaw), so
    /// the caller falls back to the async path.
    pub fn try_send_batch_to<B: AsRef<[u8]>>(
        &self,
        bufs: &[B],
        target: SocketAddr,
    ) -> io::Result<usize> {
        match self {
            Self::Udp(s) => s.try_send_batch_to(bufs, target),
            Self::TcpRaw(_) => Ok(0),
        }
    }

    pub async fn send_batch_to<B: AsRef<[u8]>>(
        &self,
        bufs: &[B],
        target: SocketAddr,
    ) -> io::Result<()> {
        match self {
            Self::Udp(s) => s.send_batch_to(bufs, target).await,
            Self::TcpRaw(s) => s.send_batch_to(bufs, &target).await,
        }
    }

    /// Send all `bufs` on a connected socket.
    pub async fn send_batch<B: AsRef<[u8]>>(&self, bufs: &[B]) -> io::Result<()> {
        match self {
            Self::Udp(s) => s.send_batch(bufs).await,
            Self::TcpRaw(s) => {
                // TCP raw is point-to-point; send each buffer individually.
                for buf in bufs {
                    s.send(buf.as_ref())?;
                }
                Ok(())
            }
        }
    }

    pub fn try_recv_batch_from(
        &self,
        packet_bufs: &mut [Vec<u8>],
        out: &mut Vec<(Vec<u8>, SocketAddr)>,
    ) -> io::Result<usize> {
        match self {
            Self::Udp(s) => s.try_recv_batch_from(packet_bufs, out),
            Self::TcpRaw(s) => s.try_recv_batch_from(packet_bufs, out),
        }
    }

    /// Allocation-free batch receive on an unconnected socket: fills
    /// `packet_bufs[..n]` with payloads in place and writes source addresses
    /// into `peers` (capacity reused). `TcpRaw` returns `0`.
    pub fn try_recv_batch_from_into(
        &self,
        packet_bufs: &mut [Vec<u8>],
        peers: &mut Vec<SocketAddr>,
    ) -> io::Result<usize> {
        match self {
            Self::Udp(s) => s.try_recv_batch_from_into(packet_bufs, peers),
            Self::TcpRaw(_) => Ok(0),
        }
    }

    /// Non-blocking batch receive on a connected socket into the caller's
    /// buffer pool. Linux: `recvmmsg` (one syscall). Others: one `try_recv`
    /// into `pool[0]`. Returns datagrams received.
    pub fn try_recv_batch(&self, pool: &mut [Vec<u8>]) -> io::Result<usize> {
        match self {
            Self::Udp(s) => s.try_recv_batch(pool),
            Self::TcpRaw(_) => Ok(0),
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Self::Udp(s) => s.local_addr(),
            Self::TcpRaw(s) => s.local_addr(),
        }
    }

    /// Set the TTL (hop limit) on the underlying socket.
    /// Only meaningful for UDP; TcpRaw returns `Unsupported`.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        match self {
            Self::Udp(s) => s.set_ttl(ttl),
            Self::TcpRaw(_) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "set_ttl not available on TcpRaw",
            )),
        }
    }

    /// Get the TTL (hop limit) from the underlying socket.
    /// Only meaningful for UDP; TcpRaw returns `Unsupported`.
    pub fn ttl(&self) -> io::Result<u32> {
        match self {
            Self::Udp(s) => s.ttl(),
            Self::TcpRaw(_) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "ttl not available on TcpRaw",
            )),
        }
    }
}

/// Set DSCP (Differentiated Services Code Point) on a `socket2::Socket`.
///
/// **DSCP is a 6-bit field (0–63)** per RFC 2474. Values > 63 must be rejected
/// by the caller before calling this.
///
/// Go kcptun v5.6.64 behaviour:
/// - IPv4: `IP_TOS = dscp << 2` (the kernel places DSCP in the DS field).
/// - IPv6: `IPV6_TCLASS = dscp` (no shift — Go passes the raw value).
///
/// `socket2::Socket::set_tos` only calls `setsockopt(IPPROTO_IP, IP_TOS)`,
/// which does **not** set `IPV6_TCLASS` on IPv6 sockets. This helper adds the
/// missing `IPV6_TCLASS` setsockopt for IPv6 on Unix.
pub fn set_dscp_on_socket(socket: &socket2::Socket, dscp: u32, is_ipv6: bool) {
    if dscp == 0 {
        return;
    }
    // IPv4: shift into the DS field. IPv6: pass raw (Go-compatible).
    let tos = if is_ipv6 { dscp } else { dscp << 2 };
    if let Err(e) = socket.set_tos(tos) {
        log::warn!("set_tos (DSCP {}) failed: {}", dscp, e);
    }
    #[cfg(unix)]
    if is_ipv6 {
        // socket2 does not expose setsockopt(IPPROTO_IPV6, IPV6_TCLASS).
        // Set it directly so the IPv6 traffic class actually takes effect.
        #[cfg(target_os = "linux")]
        unsafe {
            use std::os::fd::AsRawFd;

            let fd = socket.as_raw_fd();
            let val: libc::c_int = dscp as libc::c_int;
            let ret = libc::setsockopt(
                fd,
                libc::IPPROTO_IPV6,
                libc::IPV6_TCLASS,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of_val(&val) as libc::socklen_t,
            );
            if ret < 0 {
                log::warn!(
                    "setsockopt IPV6_TCLASS={} failed: {}",
                    dscp,
                    io::Error::last_os_error()
                );
            }
        }
    }
}

#[cfg(test)]
mod dscp_tests {
    use super::set_dscp_on_socket;

    fn udp_socket(ipv6: bool) -> socket2::Socket {
        let domain = if ipv6 {
            socket2::Domain::IPV6
        } else {
            socket2::Domain::IPV4
        };
        socket2::Socket::new(domain, socket2::Type::DGRAM, None).expect("create socket")
    }

    /// IPv4: Go writes `IP_TOS = dscp << 2`, and that is what the kernel must
    /// report back through `getsockopt`. Values 0/46/63 cover the 6-bit range
    /// (46 = EF, the value live configs use).
    #[test]
    fn ipv4_sets_ip_tos_shifted() {
        for dscp in [0u32, 46, 63] {
            let sock = udp_socket(false);
            set_dscp_on_socket(&sock, dscp, false);
            // dscp == 0 returns early (the default TOS is already 0).
            assert_eq!(
                sock.tos().expect("getsockopt IP_TOS"),
                dscp << 2,
                "dscp {dscp} must land in the DS field"
            );
        }
    }

    /// IPv6: Go writes the raw value into `IPV6_TCLASS` (no shift), which
    /// `socket2::set_tos` does not reach — the helper's own `setsockopt` does.
    #[cfg(target_os = "linux")]
    #[test]
    fn ipv6_sets_ipv6_tclass_unshifted() {
        for dscp in [46u32, 63] {
            let sock = udp_socket(true);
            set_dscp_on_socket(&sock, dscp, true);
            let mut val: libc::c_int = -1;
            let mut len = std::mem::size_of_val(&val) as libc::socklen_t;
            let ret = unsafe {
                libc::getsockopt(
                    std::os::fd::AsRawFd::as_raw_fd(&sock),
                    libc::IPPROTO_IPV6,
                    libc::IPV6_TCLASS,
                    &mut val as *mut _ as *mut libc::c_void,
                    &mut len,
                )
            };
            assert_eq!(
                ret,
                0,
                "getsockopt IPV6_TCLASS: {}",
                std::io::Error::last_os_error()
            );
            assert_eq!(val as u32, dscp, "IPv6 traffic class must not be shifted");
        }
    }
}
