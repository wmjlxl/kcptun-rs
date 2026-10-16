//! Application lifecycle: async_main, configuration, and server accept loop.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as AnyContext, Result};
use kcp_rs::PacketTransport;
use log::{error, info, warn};

use crate::cli::{Cli, Config};
use crate::server;
use crate::socket;

/// Hard cap on `--shards`. Each shard is one OS thread + one bound UDP socket;
/// past this the process dies in `thread::Builder::spawn` (and `panic=abort`
/// turns that into a full abort). 64 is well past any realistic core count.
pub(crate) const MAX_SHARDS: u32 = 64;

/// Validate an explicit `--shards` value (0 = platform default).
pub(crate) fn validate_shards(shards: u32) -> Result<()> {
    if shards > MAX_SHARDS {
        anyhow::bail!(
            "--shards {} exceeds the maximum of {MAX_SHARDS} (each shard is one OS thread + one bound UDP socket)",
            shards
        );
    }
    Ok(())
}

/// Everything startup needs after parsing and validating the CLI/JSON
/// config: listen addresses, derived key, tunables, and the fully built
/// session config. Resolved once by [`ServerSetup::resolve`] before any
/// listener or diagnostic task starts.
#[derive(Clone)]
struct ServerSetup {
    listen_addrs: Vec<std::net::SocketAddr>,
    target: String,
    key: [u8; 32],
    crypt_method: String,
    dscp: u32,
    sockbuf: u32,
    ratelimit: u32,
    quiet: bool,
    qpp_enabled: bool,
    qpp_count: u16,
    close_wait: u64,
    per_ip_rate: u32,
    max_per_ip: usize,
    session_config: kcptun_common::KcptunConfig,
}

impl ServerSetup {
    /// Apply CLI/JSON defaults, validate every tunable (crypt, DSCP, QPP,
    /// listen address), derive the encryption key, and build the session
    /// config. All startup errors surface here, before sockets are bound.
    fn resolve(cli: &Cli) -> Result<Self> {
        let listen = cli.listen.as_deref().unwrap_or(":29900");
        let target = cli.target.as_deref().unwrap_or("127.0.0.1:12948");

        let key_str = cli.key.as_deref().unwrap_or("it's a secrect");
        let crypt_method = cli.crypt.as_deref().unwrap_or("aes");
        // P2 / L-9: unknown --crypt used to silently fall back to aes.
        if let Err(e) = kcrypt_rs::validate_crypt_method(crypt_method) {
            anyhow::bail!("{e}");
        }
        let mode = cli.mode.as_deref().unwrap_or("fast");
        let mtu = cli.mtu.unwrap_or(1350);
        kcptun_common::validate_encrypted_mtu(mtu, crypt_method)?;
        let sndwnd = cli.sndwnd.unwrap_or(1024);
        let rcvwnd = cli.rcvwnd.unwrap_or(1024);
        let datashard = cli.datashard;
        let parityshard = cli.parityshard;
        let dscp_val = cli.dscp.unwrap_or(0);
        // P2 / L-11: DSCP is a 6-bit field (0–63). Higher values silently
        // overflow the TOS byte.
        anyhow::ensure!(
            dscp_val <= 63,
            "dscp {dscp_val} out of range (must be 0–63)"
        );
        let sockbuf = cli.sockbuf.unwrap_or(4 * 1024 * 1024);
        let nocomp = cli.nocomp;
        let acknodelay = cli.acknodelay;
        let nodelay = cli.nodelay.unwrap_or(0);
        let interval = cli.interval.unwrap_or(50);
        let resend = cli.resend.unwrap_or(0);
        let nc = cli.nc.unwrap_or(0);
        let smuxver = cli.smuxver.unwrap_or(2);
        let smuxbuf = cli.smuxbuf.unwrap_or(4 * 1024 * 1024);
        let streambuf = cli.streambuf;
        let framesize = cli.framesize;
        let keepalive = cli.keepalive.unwrap_or(10);
        let keepalivetimeout = cli.keepalivetimeout.unwrap_or(30).max(0) as u64;
        let ackstalltimeout = cli
            .ackstalltimeout
            .unwrap_or(kcptun_common::ACK_STALL_DEFAULT_SECS as i64)
            .max(0) as u64;
        let ratelimit_val = cli.ratelimit;
        let close_wait_val = cli.closewait.unwrap_or(30).max(0) as u64;
        let quiet = cli.quiet;
        #[cfg(feature = "qpp")]
        let qpp_enabled = cli.qpp;
        #[cfg(not(feature = "qpp"))]
        let qpp_enabled = false;
        #[cfg(feature = "qpp")]
        let qpp_count = cli.qppcount.unwrap_or(61);
        #[cfg(not(feature = "qpp"))]
        let qpp_count: u16 = 0;

        kcptun_common::validate_qpp_config(qpp_enabled, qpp_count, key_str.as_bytes())?;

        // Derive encryption key
        let key = kcptun_common::derive_key(key_str);
        kcptun_common::log_session_bootstrap(
            crypt_method,
            key.len(),
            ackstalltimeout,
            keepalive.max(0) as u64,
            keepalivetimeout,
        );
        // Bind listen address(es) — multi-port "host:min-max" matches Go ParseMultiPort.
        let listen_addrs =
            kcptun_common::parse_multi_port(listen).context("invalid listen address")?;
        let kcp_config = kcptun_common::KcpCliParams {
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
            conv: kcptun_common::DEFAULT_CONV,
            token: 0,
        }
        .to_kcp_config();
        let smux_config = smux_rs::Config {
            version: smuxver,
            max_receive_buffer: smuxbuf,
            max_stream_buffer: streambuf,
            max_frame_size: framesize,
            keepalive_interval: keepalive.max(0) as u64,
            // Go's BuildSmuxConfig changes only the interval; timeout remains 30s.
            keepalive_timeout: keepalivetimeout,
            ..smux_rs::DEFAULT_CONFIG.clone()
        };
        let session_config = kcptun_common::KcptunConfig {
            kcp: kcp_config,
            smux: smux_config,
            nocomp,
            rate_limit: ratelimit_val,
            offload_profile: kcrypt_rs::OffloadProfile::Tokio,
            ack_stall_secs: ackstalltimeout,
        };
        Ok(Self {
            listen_addrs,
            target: target.to_string(),
            key,
            crypt_method: crypt_method.to_string(),
            dscp: dscp_val,
            sockbuf,
            ratelimit: ratelimit_val,
            quiet,
            qpp_enabled,
            qpp_count,
            close_wait: close_wait_val,
            per_ip_rate: cli.peripsessionrate,
            max_per_ip: cli.maxsessionsperip,
            session_config,
        })
    }
}

/// Spawn raw-TCP KCP listeners when `--tcp` is set (Linux only: raw sockets
/// with TCP_REPAIR). Each accepted TCP conn becomes one dedicated KCP
/// session, matching Go: `--tcp` exposes a tcpraw listener alongside UDP.
fn spawn_tcp_listeners(setup: &ServerSetup, stop_flag: &Arc<AtomicBool>) {
    let ServerSetup {
        listen_addrs,
        target,
        key: key_arr,
        crypt_method,
        dscp: dscp_val,
        sockbuf,
        ratelimit: ratelimit_val,
        quiet,
        qpp_enabled,
        qpp_count,
        close_wait: close_wait_val,
        session_config,
        ..
    } = setup.clone();
    #[cfg(not(target_os = "linux"))]
    warn!("--tcp requires Linux (raw sockets + TCP_REPAIR) — serving UDP only");

    if cfg!(target_os = "linux") {
        let key = key_arr;
        for &addr in &listen_addrs {
            let listener = match knet::tcpraw_listen(&addr) {
                Ok(l) => l,
                Err(e) => {
                    warn!("tcpraw listen on {} failed: {}", addr, e);
                    continue;
                }
            };
            if dscp_val > 0 {
                if let Err(e) = listener.set_dscp(dscp_val) {
                    warn!("SetDSCP({}) failed on tcpraw listener: {}", dscp_val, e);
                }
            }
            info!("listening on {} for TCP raw KCP connections", addr);
            let crypt = crypt_method.to_string();
            let session_config = session_config.clone();
            let target_loop = target.to_string();
            let qpp_key_loop = key.to_vec();
            let stop_tcp = stop_flag.clone();
            knet::spawn_task(async move {
                loop {
                    if stop_tcp.load(Ordering::Relaxed) {
                        break;
                    }
                    // The parked accept cannot observe stop_flag, so
                    // bound the wait — this lets SIGTERM/Ctrl-C end the
                    // tcpraw loop (and, via listener drop, its accept
                    // threads) within ~500ms instead of hanging forever.
                    let (conn, peer) =
                        match knet::timeout(Duration::from_millis(500), listener.accept()).await {
                            Ok(Ok(c)) => c,
                            Ok(Err(e))
                                if e.kind() == std::io::ErrorKind::WouldBlock
                                    || e.kind() == std::io::ErrorKind::Interrupted =>
                            {
                                knet::sleep_ms(10).await;
                                continue;
                            }
                            Ok(Err(e)) => {
                                error!("TCP accept error on {}: {}", addr, e);
                                break;
                            }
                            Err(_) => continue, // timeout → re-check stop_flag
                        };
                    info!("TCP raw session from {}", peer);
                    let socket = Arc::new(knet::DatagramSocket::TcpRaw(conn));
                    let session = match kcptun_common::KcptunSession::serve_transport(
                        socket,
                        peer,
                        &key,
                        &crypt,
                        &session_config,
                    )
                    .await
                    {
                        Ok(session) => Arc::new(session),
                        Err(error) => {
                            warn!("failed to create TCP raw session from {}: {}", peer, error);
                            continue;
                        }
                    };
                    server::spawn_session_stream_loop(
                        session,
                        peer,
                        target_loop.clone(),
                        qpp_enabled,
                        qpp_key_loop.clone(),
                        qpp_count,
                        quiet,
                        close_wait_val,
                        None,
                    );
                }
            });
        }
        info!("forwarding to TCP target {}", target);
        if ratelimit_val > 0 {
            info!("ratelimit: {} bytes/sec", ratelimit_val);
        }
        info!("sockbuf: requested={}", sockbuf);
    }
}

/// Validate `--shards` and resolve the effective shard count.
///
/// SO_REUSEPORT shard count: 0 (default) is platform-aware — Linux binds
/// one shard per logical CPU (kernel hashes peers across the sockets →
/// parallel workers, no shared-fd send contention); non-Linux (Darwin does
/// not SO_REUSEPORT-distribute) defaults to a single socket. Explicit
/// `--shards N` overrides either default.
///
/// P0-7 / M-5: reject absurd values outright. Clamping silently would
/// hide the operator's mistake; an unbounded N used to reserve a giant
/// Vec, bind N sockets and spawn N OS threads, then abort on the first
/// spawn failure (`expect` + `panic=abort`).
fn resolve_shards(shards_cli: u32) -> Result<usize> {
    validate_shards(shards_cli)?;
    Ok(if shards_cli == 0 {
        #[cfg(target_os = "linux")]
        {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
                .min(MAX_SHARDS as usize)
        }
        #[cfg(not(target_os = "linux"))]
        {
            1
        }
    } else {
        shards_cli as usize
    })
}

/// Bind one UDP socket per shard on every listen address, logging the
/// granted socket buffers.
fn bind_udp_sockets(setup: &ServerSetup, shards: usize) -> Result<Vec<std::net::UdpSocket>> {
    let ServerSetup {
        listen_addrs,
        target,
        dscp: dscp_val,
        sockbuf,
        ratelimit: ratelimit_val,
        ..
    } = setup.clone();
    let mut udp_sockets: Vec<std::net::UdpSocket> = Vec::with_capacity(listen_addrs.len() * shards);
    let mut granted_buffers: Option<knet::SocketBuffers> = None;
    for addr in &listen_addrs {
        for s in 0..shards {
            let (socket, buffers) = if shards > 1 {
                socket::create_udp_socket_shard_std(*addr, sockbuf, dscp_val)?
            } else {
                socket::create_udp_socket_std(*addr, sockbuf, dscp_val)?
            };
            granted_buffers.get_or_insert(buffers);
            if shards > 1 {
                info!(
                    "listening on {} for KCP connections (shard {}/{})",
                    addr,
                    s + 1,
                    shards
                );
            } else {
                info!("listening on {} for KCP connections", addr);
            }
            udp_sockets.push(socket);
        }
    }
    info!("forwarding to TCP target {}", target);
    if ratelimit_val > 0 {
        info!("ratelimit: {} bytes/sec", ratelimit_val);
    }
    if dscp_val > 0 {
        info!("dscp: {}", dscp_val);
    }
    match granted_buffers {
        Some(ref b) => info!("sockbuf: {}", knet::net::sockbuf::describe(b)),
        None => info!("sockbuf: requested={}", sockbuf),
    }
    Ok(udp_sockets)
}

/// Start the SNMP CSV logger, the pprof endpoint, and the Ctrl-C stop hook.
fn start_diagnostics(cli: &Cli, stop_flag: &Arc<AtomicBool>) {
    kcptun_common::spawn_snmp_logger(cli.snmplog.as_deref(), cli.snmpperiod, stop_flag.clone());
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
            // P1-7 / M-14: bind loopback by default — the profile endpoint
            // exposes process internals and must not face the network.
            // `--pprofaddr` can override (e.g. 0.0.0.0:6060 for Go parity).
            if let Err(e) = kpprof::run_pprof(&pprof_addr, pprof_stop).await {
                error!("pprof server error: {}", e);
            }
        });
    }
    // Heap sampling can only be read back through the pprof HTTP endpoints, so
    // without `--pprof` it is switched off: leaving the default 512 KiB rate on
    // makes every allocation in a busy relay pay for an unreadable profile.
    #[cfg(feature = "pprof")]
    if !cli.pprof {
        let previous = kpprof::set_heap_sample_rate(0);
        info!("heap sampling disabled (was 1 sample per {previous} bytes): --pprof not enabled");
    }
    #[cfg(not(feature = "pprof"))]
    if cli.pprof {
        log::warn!("--pprof requested but binary built without `pprof` feature; rebuild with --features pprof");
    }
    kcptun_common::spawn_ctrl_c(stop_flag.clone());
}

/// Bind one `KcpListener` per UDP shard and serve it on its own worker
/// (single shard: the shared runtime; explicit multi-shard SO_REUSEPORT:
/// one dedicated thread per fd to avoid shared-socket send contention).
async fn spawn_udp_shards(
    setup: &ServerSetup,
    shards: usize,
    udp_sockets: Vec<std::net::UdpSocket>,
    stop_flag: Arc<AtomicBool>,
    kcp_listeners: &Arc<std::sync::Mutex<Vec<Arc<kcp_rs::KcpListener>>>>,
) -> Result<()> {
    let per_ip_rate = setup.per_ip_rate;
    let max_per_ip = setup.max_per_ip;
    let ServerSetup {
        key: key_arr,
        crypt_method,
        target: target_str,
        qpp_enabled,
        qpp_count,
        quiet,
        close_wait: close_wait_val,
        session_config,
        ..
    } = setup.clone();
    for udp in udp_sockets {
        // Encrypt each accepted peer's transport via kcp-rs' listener wrapper
        // (direct KcpListener use — no kcptun-common KcptunListener layer).
        let qpp_key = key_arr.to_vec();
        let key = Arc::<[u8]>::from(key_arr);
        let crypt = Arc::<str>::from(crypt_method.as_str());
        let offload = session_config.offload_profile;
        let rate_limit = session_config.rate_limit;
        let target = target_str.clone();
        let stop = stop_flag.clone();
        let shard_config = session_config.clone();

        // A single shard uses the shared tokio runtime directly.
        // A multi-shard deployment keeps one worker per SO_REUSEPORT fd,
        // preserving strict fd affinity and independent socket send queues.
        if shards == 1 {
            let udp = Arc::new(knet::DatagramSocket::Udp(knet::UdpSocket::from_std(udp)?));
            let listener = Arc::new(
                kcp_rs::KcpListener::from_socket(udp)
                    .config(shard_config.kcp.clone())
                    .limits(kcp_rs::WorkerPoolLimits {
                        per_ip_session_rate: per_ip_rate,
                        max_sessions_per_ip: max_per_ip,
                        ..Default::default()
                    })
                    .transport_wrapper(move |transport: Arc<dyn PacketTransport>, _peer| {
                        let mut ct = kcptun_common::CryptoTransport::with_transport(
                            transport,
                            key.as_ref(),
                            crypt.as_ref(),
                        );
                        ct.set_offload_profile(offload);
                        ct.set_rate_limit(rate_limit);
                        Arc::new(ct)
                    })
                    .build()
                    .await?,
            );
            kcp_listeners.lock().unwrap().push(listener.clone());
            knet::spawn_task(serve_udp_shard(
                listener,
                target,
                qpp_enabled,
                qpp_key,
                qpp_count,
                quiet,
                close_wait_val,
                shard_config,
                stop,
            ));
            continue;
        }

        let kcp_listeners_shard = kcp_listeners.clone();
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel::<Result<()>>(1);
        // Each shard runs on a dedicated OS thread + current-thread runtime:
        // this shard's fd is only touched by one worker → no shared-socket
        // send contention (Linux SO_REUSEPORT hashes peers across shards).
        match std::thread::Builder::new()
            .name("kcptun-shard".into())
            .spawn(move || {
                knet::block_on_local(async move {
                    // Register the already-bound fd only after entering this shard's P.
                    let udp = match knet::UdpSocket::from_std(udp) {
                        Ok(udp) => Arc::new(knet::DatagramSocket::Udp(udp)),
                        Err(error) => {
                            let _ = ready_tx.send(Err(error.into()));
                            return;
                        }
                    };
                    let listener = match kcp_rs::KcpListener::from_socket(udp)
                        .config(shard_config.kcp.clone())
                        .limits(kcp_rs::WorkerPoolLimits {
                            per_ip_session_rate: per_ip_rate,
                            max_sessions_per_ip: max_per_ip,
                            ..Default::default()
                        })
                        .transport_wrapper(move |transport: Arc<dyn PacketTransport>, _peer| {
                            let mut ct = kcptun_common::CryptoTransport::with_transport(
                                transport,
                                key.as_ref(),
                                crypt.as_ref(),
                            );
                            ct.set_offload_profile(offload);
                            ct.set_rate_limit(rate_limit);
                            Arc::new(ct)
                        })
                        .build()
                        .await
                    {
                        Ok(listener) => Arc::new(listener),
                        Err(error) => {
                            let _ = ready_tx.send(Err(error.into()));
                            return;
                        }
                    };
                    kcp_listeners_shard.lock().unwrap().push(listener.clone());
                    let _ = ready_tx.send(Ok(()));
                    serve_udp_shard(
                        listener,
                        target,
                        qpp_enabled,
                        qpp_key,
                        qpp_count,
                        quiet,
                        close_wait_val,
                        shard_config,
                        stop,
                    )
                    .await;
                })
            }) {
            Ok(_handle) => {}
            Err(error) => {
                // P0-7 / M-5: a spawn failure used to `expect` and abort the
                // whole process (release profile has `panic=abort`). Report
                // and unwind cleanly so the operator gets a real exit code.
                error!("failed to spawn shard worker: {error}");
                stop_flag.store(true, Ordering::Relaxed);
                return Err(error).context("spawn shard worker");
            }
        }
        ready_rx
            .recv()
            .context("UDP shard startup channel closed")??;
    }
    Ok(())
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

    // Logging: append to --log (rotated at 10 MiB) or stderr; default "info".
    kcptun_common::init_logging(cli.log.as_deref())?;

    // Resolve + validate every tunable, derive the key, build session config.
    let setup = ServerSetup::resolve(&cli)?;

    // Stop flag + SIGUSR1 -> in-memory SNMP dump (matching Go kcptun).
    let stop_flag = Arc::new(AtomicBool::new(false));
    {
        let signal_stop = stop_flag.clone();
        knet::spawn_task(async move {
            kcptun_common::snmp_signal_logger(signal_stop).await;
        });
    }

    // TCP mode: raw-TCP listener alongside the always-on UDP path.
    if cli.tcp {
        spawn_tcp_listeners(&setup, &stop_flag);
    }

    // SO_REUSEPORT shards: validate the CLI value, bind one socket per shard.
    let shards = resolve_shards(cli.shards)?;
    let udp_sockets = bind_udp_sockets(&setup, shards)?;

    // SNMP CSV logger, pprof endpoint, Ctrl-C handler.
    start_diagnostics(&cli, &stop_flag);

    info!("using shared kcptun server session stack");

    // Every KcpListener created for a UDP shard. Closed explicitly at
    // shutdown so the receive task and per-peer queues release immediately
    // (M-7 rework: the comment claiming "accept loops have already seen
    // stop" was wrong for the KCP path — listeners were never closed).
    let kcp_listeners: Arc<std::sync::Mutex<Vec<Arc<kcp_rs::KcpListener>>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    spawn_udp_shards(
        &setup,
        shards,
        udp_sockets,
        stop_flag.clone(),
        &kcp_listeners,
    )
    .await?;

    // Main task waits for stop signal (Ctrl-C).
    loop {
        knet::sleep_ms(500).await;
        if stop_flag.load(Ordering::Relaxed) {
            info!("received Ctrl+C, shutting down...");
            break;
        }
    }

    // Graceful shutdown (M-7 rework):
    // 1. Close every KcpListener so their receive tasks stop and pending
    //    accepts drain — previously they were left running and only the
    //    process teardown killed them.
    // 2. Then give in-flight stream handlers a grace period to flush and
    //    close cleanly (P1-2) instead of tearing them mid-pipe.
    info!("shutting down...");
    {
        let listeners: Vec<_> = kcp_listeners.lock().unwrap().drain(..).collect();
        for l in &listeners {
            l.close();
        }
        if !listeners.is_empty() {
            info!("closed {} KCP listener(s)", listeners.len());
        }
    }
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

/// Serve one UDP shard: accept KCP sessions off this shard's `kcp_rs::KcpListener`
/// and forward their SMUX streams to `target`. Single-shard builds run on the
/// shared tokio runtime; explicit multi-shard SO_REUSEPORT builds keep every fd
/// on one worker to avoid shared-socket send contention.
async fn serve_udp_shard(
    listener: Arc<kcp_rs::KcpListener>,
    target: String,
    qpp_enabled: bool,
    qpp_key: Vec<u8>,
    qpp_count: u16,
    quiet: bool,
    close_wait: u64,
    session_config: kcptun_common::KcptunConfig,
    stop: Arc<AtomicBool>,
) {
    loop {
        if stop.load(Ordering::Relaxed) {
            listener.close();
            break;
        }
        let (kcp, peer) = match listener.accept().await {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionAborted => break,
            Err(error) => {
                error!("KCP accept error: {}", error);
                knet::sleep_ms(10).await;
                continue;
            }
        };
        info!("new shared KCP session from {}", peer);
        let session =
            match kcptun_common::KcptunSession::server_with_limited_transport(kcp, &session_config)
            {
                Ok(s) => Arc::new(s),
                Err(e) => {
                    error!("session create failed for {}: {}", peer, e);
                    continue;
                }
            };
        server::spawn_session_stream_loop(
            session,
            peer,
            target.clone(),
            qpp_enabled,
            qpp_key.clone(),
            qpp_count,
            quiet,
            close_wait,
            Some(listener.clone()),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// P0-7 / M-5: `--shards` above the cap must fail fast, not abort later
    /// in `thread::Builder::spawn`.
    #[test]
    fn shards_above_cap_is_rejected() {
        assert!(validate_shards(0).is_ok(), "0 = platform default");
        assert!(validate_shards(1).is_ok());
        assert!(validate_shards(4).is_ok());
        assert!(validate_shards(MAX_SHARDS).is_ok());
        let err = validate_shards(MAX_SHARDS + 1).unwrap_err().to_string();
        assert!(err.contains("exceeds the maximum"), "got: {err}");
        let err = validate_shards(100_000).unwrap_err().to_string();
        assert!(err.contains("exceeds the maximum"), "got: {err}");
    }
}
