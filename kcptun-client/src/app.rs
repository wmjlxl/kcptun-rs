//! Application lifecycle: async_main, configuration, and main accept loop.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use log::{debug, error, info, warn};

use crate::cli::{Cli, Config};
use crate::client::{self, ClientDialOptions};
use crate::socket;

/// Hard cap on `--conn` (P1-3 / M-8). Each conn is a session + task + socket.
pub(crate) const MAX_CONNS: u32 = 256;

/// Validate the `--conn` pool size.
pub(crate) fn validate_conn_count(conn: u32) -> Result<()> {
    anyhow::ensure!(conn > 0, "conn must be greater than 0 (got {conn})");
    anyhow::ensure!(
        conn <= MAX_CONNS,
        "conn {conn} exceeds the maximum of {MAX_CONNS} (each conn is a session + task + socket)"
    );
    Ok(())
}

enum LocalListener {
    Tcp(knet::TcpListener),
    #[cfg(unix)]
    Unix(knet::UnixListener),
}

impl LocalListener {
    async fn bind(addr: &str) -> Result<Self> {
        match kcptun_common::parse_multi_port(addr) {
            Ok(addrs) => {
                let mut addrs = addrs.into_iter();
                let listen_addr = addrs
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("invalid local address"))?;
                if let Some(extra) = addrs.next() {
                    anyhow::bail!(
                        "multiple local addresses are not supported; got [{listen_addr}, {extra}, …]"
                    );
                }
                Ok(Self::Tcp(knet::TcpListener::bind(listen_addr).await?))
            }
            Err(tcp_error) => {
                #[cfg(unix)]
                {
                    let listener = knet::UnixListener::bind(addr).await.map_err(|unix_error| {
                        anyhow::anyhow!(
                            "invalid TCP address ({tcp_error}); cannot bind unix socket {addr}: {unix_error}"
                        )
                    })?;
                    Ok(Self::Unix(listener))
                }
                #[cfg(not(unix))]
                Err(tcp_error)
            }
        }
    }

    async fn accept(&self) -> std::io::Result<(knet::TcpStream, String)> {
        match self {
            Self::Tcp(listener) => listener
                .accept()
                .await
                .map(|(stream, peer)| (stream, peer.to_string())),
            #[cfg(unix)]
            Self::Unix(listener) => listener.accept().await,
        }
    }

    fn try_accept(&self) -> std::io::Result<(knet::TcpStream, String)> {
        match self {
            Self::Tcp(listener) => listener
                .try_accept()
                .map(|(stream, peer)| (stream, peer.to_string())),
            #[cfg(unix)]
            Self::Unix(listener) => listener.try_accept(),
        }
    }
}

/// Main async entry point — configuration, session setup, accept loop, and
/// graceful shutdown.  Mirrors the Go kcptun client lifecycle.
/// Everything startup needs after parsing and validating the CLI/JSON
/// config: addresses, derived key, dial options, and pool tunables.
/// Resolved once by [`ClientSetup::resolve`] before any socket or task
/// is created.
#[derive(Clone)]
struct ClientSetup {
    local_addr: String,
    remote_addr: String,
    key: [u8; 32],
    session_cfg: ClientDialOptions,
    conn_count: u32,
    sockbuf: u32,
    dscp: u32,
    autoexpire: i64,
    scavengettl: i64,
    closewait: u64,
    ratelimit: u32,
    quiet: bool,
    tcp: bool,
    qpp_enabled: bool,
    qpp_count: u16,
}

impl ClientSetup {
    /// Apply CLI/JSON defaults, validate every tunable (crypt, DSCP, QPP,
    /// conn count, remote address), derive the encryption key, and build
    /// the dial options. All startup errors surface here, before the pool
    /// dials anything.
    fn resolve(cli: &Cli) -> Result<Self> {
        let local_addr = cli.localaddr.as_deref().unwrap_or(":12948");
        let remote_addr_str = cli.remoteaddr.as_deref().unwrap_or("vps:29900");

        let key_str = cli.key.as_deref().unwrap_or("it's a secrect");
        let crypt = cli.crypt.as_deref().unwrap_or("aes");
        // P2 / L-9: unknown --crypt used to silently fall back to aes.
        if let Err(e) = kcrypt_rs::validate_crypt_method(crypt) {
            anyhow::bail!("{e}");
        }
        let mode = cli.mode.as_deref().unwrap_or("fast");
        let conn_count = cli.conn.unwrap_or(1);
        // P1-3 / M-8: `--conn` used to be only `> 0`, so `--conn 1000000` opened
        // millions of tasks + fds and wedged the start-up loop.
        validate_conn_count(conn_count)?;
        let mtu = cli.mtu.unwrap_or(1350);
        kcptun_common::validate_encrypted_mtu(mtu, crypt)?;
        let sndwnd = cli.sndwnd.unwrap_or(128);
        let rcvwnd = cli.rcvwnd.unwrap_or(512);
        let datashard = cli.datashard.unwrap_or(10);
        let parityshard = cli.parityshard.unwrap_or(3);
        let nocomp = cli.nocomp;
        let quiet = cli.quiet;
        let acknodelay = cli.acknodelay;
        let nodelay = cli.nodelay.unwrap_or(0);
        let interval = cli.interval.unwrap_or(50);
        let resend = cli.resend.unwrap_or(0);
        let nc = cli.nc.unwrap_or(0);
        let smuxver = cli.smuxver.unwrap_or(2);
        let smuxbuf = cli.smuxbuf.unwrap_or(4 * 1024 * 1024);
        let streambuf = cli.streambuf.unwrap_or(2097152);
        let framesize = cli.framesize.unwrap_or(8192);
        let sockbuf = cli.sockbuf.unwrap_or(4 * 1024 * 1024);
        let keepalive = cli.keepalive.unwrap_or(10);
        let keepalivetimeout = cli.keepalivetimeout.unwrap_or(30).max(0) as u64;
        let ackstalltimeout = cli
            .ackstalltimeout
            .unwrap_or(kcptun_common::ACK_STALL_DEFAULT_SECS as i64)
            .max(0) as u64;
        let autoexpire = cli.autoexpire.unwrap_or(0);
        let scavengettl = cli.scavengettl.unwrap_or(600);
        let closewait = cli.closewait.unwrap_or(0).max(0) as u64;
        let ratelimit = cli.ratelimit;
        let dscp = cli.dscp.unwrap_or(0);
        // P2 / L-11: DSCP is a 6-bit field (0–63).
        anyhow::ensure!(dscp <= 63, "dscp {dscp} out of range (must be 0–63)");
        #[cfg(feature = "qpp")]
        let qpp_enabled = cli.qpp;
        #[cfg(not(feature = "qpp"))]
        let qpp_enabled = false;
        #[cfg(feature = "qpp")]
        let qpp_count = cli.qppcount.unwrap_or(61);
        #[cfg(not(feature = "qpp"))]
        let qpp_count = 0u16;

        kcptun_common::validate_qpp_config(qpp_enabled, qpp_count, key_str.as_bytes())?;

        // Derive encryption key
        let key = kcptun_common::derive_key(key_str);
        let session_cfg = ClientDialOptions {
            crypt: crypt.to_string(),
            mode: mode.to_string(),
            mtu,
            sndwnd,
            rcvwnd,
            datashard,
            parityshard,
            acknodelay,
            nodelay,
            interval,
            resend,
            nc,
            smuxver,
            smuxbuf,
            streambuf,
            framesize,
            keepalive: keepalive.max(0) as u64,
            keepalivetimeout,
            ackstalltimeout,
            nocomp,
            ratelimit,
        };

        kcptun_common::log_session_bootstrap(
            crypt,
            key.len(),
            session_cfg.ackstalltimeout,
            session_cfg.keepalive,
            session_cfg.keepalivetimeout,
        );
        // Validate the remote address once; each actual dial re-resolves DNS
        // and randomly selects a configured port, matching Go.
        kcptun_common::parse_multi_port(remote_addr_str)?;

        Ok(Self {
            local_addr: local_addr.to_string(),
            remote_addr: remote_addr_str.to_string(),
            key,
            session_cfg,
            conn_count,
            sockbuf,
            dscp,
            autoexpire,
            scavengettl,
            closewait,
            ratelimit,
            quiet,
            tcp: cli.tcp,
            qpp_enabled,
            qpp_count,
        })
    }
}

/// Dial the KCP connection pool: `conn_count` UDP (or raw-TCP) sessions,
/// tracked for auto-expire when `autoexpire > 0` (shared with the
/// scavenger — Go keeps every timed session in a separate list so a
/// replaced session stays closeable after its pool slot moves on).
async fn create_pool(setup: &ClientSetup) -> Result<(client::SessionPool, client::SessionPool)> {
    let ClientSetup {
        remote_addr,
        key,
        session_cfg,
        conn_count,
        sockbuf,
        dscp,
        autoexpire,
        tcp,
        ..
    } = setup;
    let conns: client::SessionPool = Arc::new(parking_lot::Mutex::new(Vec::with_capacity(
        *conn_count as usize,
    )));
    // Go keeps every timed session in a separate scavenger list.  Keeping Arc
    // references here ensures a session remains closeable after its pool slot
    // is replaced by a reconnect.
    let tracked_sessions: client::SessionPool = Arc::new(parking_lot::Mutex::new(Vec::new()));
    if *tcp {
        // Go maintains `conn` independently of the underlying UDP/tcpraw
        // transport. Build the requested number of tcpraw sessions so the
        // round-robin pool and its configured size cannot diverge.
        for i in 0..*conn_count as usize {
            let remote = kcptun_common::random_remote_addr(remote_addr)?;
            info!(
                "creating TCP raw KCP connection {}/{} -> {}",
                i + 1,
                conn_count,
                remote
            );
            let socket = socket::create_client_socket(remote, true, *sockbuf, *dscp)?;
            let conn = Arc::new(client::build_session(remote, key, session_cfg, socket).await?);
            conns.lock().push(conn.clone());
            if *autoexpire > 0 {
                tracked_sessions.lock().push(conn);
            }
        }
    } else {
        // UDP mode: create conn_count connections
        for i in 0..*conn_count as usize {
            let remote = kcptun_common::random_remote_addr(remote_addr)?;
            info!(
                "creating KCP connection {}/{} -> {}",
                i + 1,
                conn_count,
                remote
            );
            let socket = socket::create_client_udp_socket(remote, *sockbuf, *dscp)?;
            let socket = Arc::new(knet::DatagramSocket::Udp(socket));
            let conn = Arc::new(client::build_session(remote, key, session_cfg, socket).await?);
            conns.lock().push(conn.clone());
            if *autoexpire > 0 {
                tracked_sessions.lock().push(conn);
            }
        }
    }
    Ok((conns, tracked_sessions))
}

/// Start the pprof HTTP endpoint when `--pprof` is set (requires the
/// `pprof` feature; warns otherwise).
///
/// Heap sampling is only readable through the pprof HTTP endpoints, so without
/// `--pprof` it is switched off here: leaving the default 512 KiB sampling on
/// makes every allocation in a long-running proxy pay for a profile that
/// nothing can serve.
fn start_pprof(cli: &Cli, stop_flag: &Arc<AtomicBool>) {
    // Start pprof if configured (requires --features pprof)
    #[cfg(feature = "pprof")]
    if cli.pprof {
        let pprof_addr = cli.pprofaddr.clone();
        info!(
            "starting pprof HTTP server on {pprof_addr} (heap sampling: 1 per {} bytes)",
            kpprof::heap_sample_rate()
        );
        #[cfg(feature = "pprof-deadlock")]
        kpprof::start_deadlock_detector();
        let pprof_stop = stop_flag.clone();
        knet::spawn_task(async move {
            // P1-7 / M-14: bind loopback by default. `--pprofaddr` can
            // override (e.g. 0.0.0.0:6060 for Go parity).
            if let Err(e) = kpprof::run_pprof(&pprof_addr, pprof_stop).await {
                error!("pprof server error: {}", e);
            }
        });
    }
    #[cfg(feature = "pprof")]
    if !cli.pprof {
        let previous = kpprof::set_heap_sample_rate(0);
        info!("heap sampling disabled (was 1 sample per {previous} bytes): --pprof not enabled");
    }
    #[cfg(not(feature = "pprof"))]
    if cli.pprof {
        log::warn!("--pprof requested but binary built without `pprof` feature; rebuild with --features pprof");
    }
}

/// Spawn the auto-expire scavenger if enabled (matching Go client).
///
/// Go scavenger deadline = creation + autoexpire + scavengeTTL.
/// Uses absolute creation time, NOT last activity — keepalive does NOT
/// delay expiry.  The accept loop proactively replaces sessions at
/// `creation + autoexpire`; the scavenger retires any session still alive
/// past `creation + autoexpire + scavengeTTL` **and** done serving, so a
/// replaced session finishes the streams it was already carrying instead of
/// aborting them (see `client::should_retire_session`).
fn spawn_scavenger(
    setup: &ClientSetup,
    tracked_sessions: &client::SessionPool,
    stop_flag: Arc<AtomicBool>,
) {
    let autoexpire = setup.autoexpire;
    let scavengettl = setup.scavengettl;
    if autoexpire > 0 {
        let s = stop_flag;
        let scavenge_sessions = tracked_sessions.clone();
        let scavenge_autoexpire = autoexpire.max(0) as u64;
        let scavenge_ttl = scavengettl.max(0) as u64;
        knet::spawn_task(async move {
            info!(
                "scavenger started: autoexpire={}s, scavengettl={}s",
                scavenge_autoexpire, scavenge_ttl
            );
            loop {
                knet::sleep_ms(5000).await;
                if s.load(Ordering::Acquire) {
                    break;
                }
                // Decide under the pool lock, act outside it. `close()` reaches
                // into the SMUX session and KCP stream (their own locks), and the
                // final `Drop` runs the session teardown; neither belongs on the
                // pool mutex that the accept path also takes.
                let mut finished: Vec<(client::SessionRef, bool, usize)> = Vec::new();
                {
                    let mut pool = scavenge_sessions.lock();
                    pool.retain(|conn| {
                        if conn.is_dead() {
                            finished.push((conn.clone(), false, 0));
                            return false;
                        }
                        if client::should_retire_session(
                            conn,
                            scavenge_autoexpire,
                            scavenge_ttl,
                            knet::mono_ms(),
                        ) {
                            finished.push((conn.clone(), true, conn.active_stream_count()));
                            return false;
                        }
                        if client::is_session_scavenge_expired(
                            conn,
                            scavenge_autoexpire,
                            scavenge_ttl,
                        ) {
                            // Past the TTL but still serving: keep it until its
                            // streams finish.
                            log::debug!(
                                "scavenger: session past ttl, waiting for {} stream(s)",
                                conn.active_stream_count()
                            );
                        }
                        true
                    });
                }
                for (conn, retired, streams) in finished {
                    if retired {
                        info!(
                            "scavenger: session retired (ttl reached, {} stream(s) done)",
                            streams
                        );
                        conn.close();
                    } else {
                        info!("scavenger: session normally closed");
                    }
                    drop(conn);
                }
            }
        });
    }
}

pub(crate) async fn async_main(cli: Cli) -> Result<()> {
    // Ignore SIGPIPE to prevent crashes when writing to closed sockets.
    knet::ignore_sigpipe();
    // Install SIGUSR1 handler for SNMP stats dump (matching Go kcptun).
    knet::install_sigusr1_handler();
    kcp_rs::snmp_enable();

    if cli.version_flag {
        println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    // Load config file if specified
    let cli = if let Some(ref config_path) = cli.c {
        let config_str = knet::read_to_string(config_path.clone()).await?;
        let cfg: Config = serde_json::from_str(&config_str)?;
        Cli::merge(cli, cfg)
    } else {
        cli
    };

    // Logging: append to --log (rotated at 10 MiB) or stderr; default info.
    kcptun_common::init_logging(cli.log.as_deref())?;

    // Resolve + validate every tunable, derive key, build dial options.
    let setup = ClientSetup::resolve(&cli)?;

    if !setup.tcp {
        info!("using shared kcptun session stack");
    }

    // Create KCP connection pool (shared with scavenger for auto-expire)
    let (conns, tracked_sessions) = create_pool(&setup).await?;

    info!("established {} KCP connections", conns.lock().len());
    if setup.ratelimit > 0 {
        info!("ratelimit: {} bytes/sec", setup.ratelimit);
    }
    if setup.dscp > 0 {
        info!("dscp: {}", setup.dscp);
    }
    info!("sockbuf: requested={}", setup.sockbuf);

    // Stop flag + SIGUSR1 -> in-memory SNMP dump (matching Go kcptun).
    let stop_flag = Arc::new(AtomicBool::new(false));
    {
        let signal_stop = stop_flag.clone();
        knet::spawn_task(async move {
            kcptun_common::snmp_signal_logger(signal_stop).await;
        });
    }
    kcptun_common::spawn_snmp_logger(cli.snmplog.as_deref(), cli.snmpperiod, stop_flag.clone());
    start_pprof(&cli, &stop_flag);
    spawn_scavenger(&setup, &tracked_sessions, stop_flag.clone());

    // The accept loop works on plain locals; destructure the resolved
    // setup once instead of threading `setup.` through every retry branch.
    let ClientSetup {
        local_addr,
        remote_addr: remote_addr_str,
        key,
        session_cfg,
        sockbuf,
        dscp,
        autoexpire,
        closewait,
        quiet,
        qpp_enabled,
        qpp_count,
        ..
    } = setup;

    // Go accepts either a TCP address or a Unix-domain socket path here.
    let listener = LocalListener::bind(&local_addr).await?;
    info!("listening on {}", local_addr);
    kcptun_common::spawn_ctrl_c(stop_flag.clone());

    // Accept loop with round-robin across KCP connections
    let round_robin = Arc::new(AtomicUsize::new(0));
    let conn_count_usize = conns.lock().len();
    anyhow::ensure!(conn_count_usize > 0, "connection pool is empty");
    // Per-slot reconnect cooldown: while the server is down every TCP accept
    // would otherwise redial and thrash sockets/tasks. Match Go's lazy redial
    // by allowing at most one dial per slot per second.
    let mut last_redial_ms = vec![0u64; conn_count_usize];
    let mut idle_ticks = 0u32;

    loop {
        if stop_flag.load(Ordering::Relaxed) {
            info!("shutting down...");
            break;
        }

        let (local, peer) = match knet::timeout(Duration::from_millis(500), listener.accept()).await
        {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                error!("accept error: {}", e);
                continue;
            }
            Err(_) => {
                idle_ticks += 1;
                if idle_ticks >= 20 {
                    idle_ticks = 0;
                    debug!("accept loop idle; pool={}", conns.lock().len());
                }
                continue; // timeout, loop back to check stop_flag
            }
        };
        idle_ticks = 0;

        // Process the accepted connection, then drain any already-queued
        // connections in the same wakeup. Without this, a burst of concurrent
        // connections is serialized behind per-connection reactor wakeups
        // (measured ~90ms stall on the 2nd accept under the first burst).
        let mut pending = Some((local, peer));
        while let Some((local, peer)) = pending.take() {
            if stop_flag.load(Ordering::Relaxed) {
                info!("shutting down, rejecting new connection from {}", peer);
                break;
            }

            let idx = round_robin.fetch_add(1, Ordering::Relaxed) % conn_count_usize;

            // Ensure a live KCP/SMUX session (Go muxSession.Open auto-redial).
            // Go also proactively reconnects when `now > creation + autoexpire`
            // (absolute deadline, independent of keepalive activity).
            //
            // After a server restart every pool slot is dead; retry a couple
            // of times so a failed dial (server still binding) does not
            // silently accept the TCP conn into a black hole.
            let mut opened: Option<Arc<smux_rs::stream::Stream>> = None;
            for _attempt in 0..3 {
                let now_ms = knet::mono_ms();
                let needs_reconnect = {
                    let guard = conns.lock();
                    let dead = guard[idx].is_dead();
                    let expired = autoexpire > 0
                        && client::is_session_expired(&guard[idx], autoexpire.max(0) as u64);
                    // Cooldown: skip redial if we dialed this slot <1s ago
                    // (server still down, or the replacement died instantly).
                    let cooled_down = now_ms.saturating_sub(last_redial_ms[idx]) >= 1000;
                    if !dead && !expired && log::log_enabled!(log::Level::Debug) {
                        // `write_blocked_ms` / `out_idle_ms` separate a local
                        // writer stall from a silent peer: on the wire they
                        // both look like the session simply stopped talking.
                        log::debug!(
                            "conn {idx} alive: inbound_age_ms={} out_idle_ms={} write_blocked_ms={}",
                            guard[idx].inbound_idle_ms(),
                            guard[idx].out_idle_ms(),
                            guard[idx].write_blocked_ms()
                        );
                    }
                    (dead || expired) && cooled_down
                };
                if needs_reconnect {
                    last_redial_ms[idx] = now_ms;
                    let ok = client::reconnect_session(
                        &conns,
                        (autoexpire > 0).then_some(&tracked_sessions),
                        idx,
                        &remote_addr_str,
                        &key,
                        &session_cfg,
                        cli.tcp,
                        sockbuf,
                        dscp,
                    )
                    .await;
                    if !ok {
                        break;
                    }
                }

                let stream_result = {
                    let guard = conns.lock();
                    let c = &guard[idx];
                    // The replacement session may already be dead (e.g. the
                    // server is still down and the first UDP send got
                    // ECONNREFUSED). Do not open a stream on it — retry dial.
                    if c.is_dead() {
                        None
                    } else {
                        match c.open_stream() {
                            Ok(stream) => Some(stream),
                            Err(e) => {
                                error!("failed to open SMUX stream: {:?}", e);
                                c.close();
                                None
                            }
                        }
                    }
                };

                match stream_result {
                    Some(s) => {
                        opened = Some(s);
                        break;
                    }
                    None => continue,
                }
            }

            let smux_stream = match opened {
                Some(s) => s,
                None => continue,
            };

            let stream_id = smux_stream.id();
            if !quiet {
                info!("accepted connection from {} (stream {})", peer, stream_id);
            }

            let flush_notify_ref = {
                let guard = conns.lock();
                guard[idx].flush_notify()
            };

            let qpp_key = key.to_vec();
            knet::spawn_task(async move {
                // P1-2 / M-7: keep the handler visible to graceful shutdown
                // so an in-flight transfer can finish instead of being torn
                // down mid-pipe when the runtime goes away.
                let _inflight = knet::inflight::guard();
                if let Err(e) = client::handle_client(
                    local,
                    smux_stream,
                    qpp_enabled,
                    qpp_key,
                    qpp_count,
                    quiet,
                    flush_notify_ref,
                    closewait,
                )
                .await
                {
                    error!("client handler error (stream {}): {:?}", stream_id, e);
                }
                if !quiet {
                    info!("stream {} closed", stream_id);
                }
            });

            // Drain any other connections already queued in the same wakeup.
            pending = listener.try_accept().ok();
        }
    }

    // Graceful shutdown: stop accepting (loop already exited), then give
    // in-flight stream handlers a grace period to flush and close cleanly.
    info!("shutting down...");
    let drained = knet::inflight::wait_drain(Duration::from_secs(5)).await;
    if !drained {
        warn!(
            "graceful shutdown timed out with {} stream(s) still in flight",
            knet::inflight::active()
        );
    }
    info!("bye");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// P1-3 / M-8: `--conn` must reject absurd pool sizes at startup.
    #[test]
    fn conn_count_is_capped() {
        assert!(validate_conn_count(1).is_ok());
        assert!(validate_conn_count(4).is_ok());
        assert!(validate_conn_count(MAX_CONNS).is_ok());
        assert!(validate_conn_count(0).is_err());
        assert!(validate_conn_count(MAX_CONNS + 1).is_err());
        let err = validate_conn_count(100_000).unwrap_err().to_string();
        assert!(err.contains("exceeds the maximum"), "got: {err}");
    }
}
