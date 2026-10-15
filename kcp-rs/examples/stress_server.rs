//! Stability-test echo server for the 1 h / 20-client kcp-rs stress run.
//!
//! Pure kcp-rs stack: one [`KcpListener`] accepts per-peer [`KcpStream`]s and
//! echoes every byte back until the peer goes idle (30 s read deadline) or the
//! connection dies; the accepted session is then dropped and explicitly
//! reaped via `remove_peer` so a burst-churn run cannot leak sessions.
//!
//! Also writes a Go-style SNMP CSV (`Unix,<SNMP::header…>` every 10 s from
//! [`DEFAULT_SNMP`]) and a periodic one-line stats log so the harness can
//! watch session counts / memory over a long run.
//!
//! ```text
//! # build (native macOS smoke)
//! cargo build --release -p kcp-rs --features async --example stress_server
//! # build (Linux musl for the remote host)
//! cargo build --release --target x86_64-unknown-linux-musl -p kcp-rs \
//!     --features async --example stress_server
//! ./stress_server --listen 0.0.0.0:29901 --snmp snmp_server.csv
//! ```

#[cfg(not(feature = "async"))]
fn main() {
    eprintln!("build with --features async");
    std::process::exit(2);
}

#[cfg(feature = "async")]
fn main() {
    // ── args ────────────────────────────────────────────────────────────────
    let mut listen = "0.0.0.0:29901".to_string();
    let mut snmp_path = String::new();
    let mut mode = "fast".to_string();
    let mut datashard = 10u32;
    let mut parityshard = 3u32;
    let mut sndwnd = 512u32;
    let mut rcvwnd = 512u32;
    let mut idle_secs = 30u64;
    let mut stats_secs = 10u64;

    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut val = || {
            it.next()
                .unwrap_or_else(|| panic!("missing value for {arg}"))
        };
        match arg.as_str() {
            "--listen" => listen = val(),
            "--snmp" => snmp_path = val(),
            "--mode" => mode = val(),
            "--datashard" => datashard = val().parse().expect("datashard"),
            "--parityshard" => parityshard = val().parse().expect("parityshard"),
            "--sndwnd" => sndwnd = val().parse().expect("sndwnd"),
            "--rcvwnd" => rcvwnd = val().parse().expect("rcvwnd"),
            "--idle" => idle_secs = val().parse().expect("idle"),
            "--stats-interval" => stats_secs = val().parse().expect("stats"),
            "--help" | "-h" => {
                eprintln!(
                    "usage: stress_server [--listen ADDR] [--snmp PATH] \
                     [--mode fast|fast2|fast3|normal] [--datashard N] \
                     [--parityshard N] [--sndwnd N] [--rcvwnd N] \
                     [--idle SECS] [--stats-interval SECS]"
                );
                return;
            }
            other => panic!("unknown arg: {other}"),
        }
    }

    let kcp_mode = match mode.as_str() {
        "fast" => kcp_rs::KcpMode::Fast,
        "fast2" => kcp_rs::KcpMode::Fast2,
        "fast3" => kcp_rs::KcpMode::Fast3,
        "normal" => kcp_rs::KcpMode::Normal,
        other => panic!("unknown mode: {other}"),
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let snmp = snmp_path.clone();
    rt.block_on(async move {
        use kcp_rs::{KcpListener, KcpStream};
        use std::net::SocketAddr;
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;
        use std::time::Duration;

        let listener = Arc::new(
            KcpListener::bind(listen.as_str())
                .mode(kcp_mode)
                .sndwnd(sndwnd)
                .rcvwnd(rcvwnd)
                .fec(datashard, parityshard)
                .build()
                .await
                .expect("bind listener"),
        );
        let local = listener.local_addr().expect("local_addr");
        println!(
            "t={} listening on {local} mode={mode} fec={datashard}/{parityshard} \
                  wnd={sndwnd}/{rcvwnd} idle={idle_secs}s",
            now_secs()
        );

        // ── SNMP CSV writer (same layout as kcptun-common::snmp_log) ────────
        if !snmp.is_empty() {
            // kcp-rs gates ALL counter updates behind this flag (zero-cost
            // no-ops when disabled) — without it every field stays 0.
            kcp_rs::snmp_enable();
            let path = snmp.clone();
            knet::spawn_task(async move {
                use std::io::Write;
                let header = format!("Unix,{}\n", kcp_rs::SNMP::header().join(","));
                // create/truncate + header
                if let Ok(mut f) = std::fs::File::create(&path) {
                    let _ = f.write_all(header.as_bytes());
                }
                loop {
                    knet::sleep_ms((stats_secs * 1000) as u64).await;
                    let line = format!(
                        "{},{}\n",
                        now_secs(),
                        kcp_rs::DEFAULT_SNMP.to_slice().join(",")
                    );
                    if let Ok(mut f) = std::fs::OpenOptions::new().append(true).open(&path) {
                        let _ = f.write_all(line.as_bytes());
                    }
                }
            });
        }

        // ── periodic stats line ────────────────────────────────────────────
        let accepts = Arc::new(AtomicU64::new(0));
        {
            let listener = Arc::clone(&listener);
            let accepts = Arc::clone(&accepts);
            knet::spawn_task(async move {
                loop {
                    knet::sleep_ms((stats_secs * 1000) as u64).await;
                    let s = kcp_rs::DEFAULT_SNMP.to_slice();
                    let g = |i: usize| s.get(i).cloned().unwrap_or_default();
                    // to_slice() excludes the leading "Unix" column — index the
                    // SNMP::header() order directly (0=BytesSent, 11=InSegs,
                    // 12=OutSegs, 15=RetransSegs, 18=LostSegs, 22=FECErrs,
                    // 23=FECRecovered, 8=KCPInErrors).
                    println!(
                        "t={} sessions={} building={} accepts={} bytes_sent={} bytes_rcv={} \
                         in_segs={} out_segs={} retrans={} lost={} fec_rec={} fec_errs={} \
                         kcp_in_errs={}",
                        now_secs(),
                        listener.session_count(),
                        listener.building_count(),
                        accepts.load(Ordering::Relaxed),
                        g(0),
                        g(1),
                        g(11),
                        g(12),
                        g(15),
                        g(18),
                        g(23),
                        g(22),
                        g(8),
                    );
                }
            });
        }

        // ── accept loop ─────────────────────────────────────────────────────
        let idle = Duration::from_secs(idle_secs);
        loop {
            let (stream, peer): (KcpStream, SocketAddr) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    println!("t={} accept error: {e}", now_secs());
                    knet::sleep_ms(100).await;
                    continue;
                }
            };
            let n = accepts.fetch_add(1, Ordering::Relaxed) + 1;
            if n <= 32 || n % 1000 == 0 {
                println!("t={} accept #{n} peer={peer}", now_secs());
            }
            let listener = Arc::clone(&listener);
            knet::spawn_task(async move {
                echo_session(stream, peer, listener, idle).await;
            });
        }
    });

    /// Read → echo until idle deadline / EOF / error, then reap the session.
    async fn echo_session(
        mut stream: kcp_rs::KcpStream,
        peer: std::net::SocketAddr,
        listener: std::sync::Arc<kcp_rs::KcpListener>,
        idle: std::time::Duration,
    ) {
        use knet::AsyncWriteExt;
        let mut buf = vec![0u8; 65536];
        // Running FNV-1a64 over everything this session echoed back: for a
        // one-exchange burst session it equals fnv(sent payload) — exactly
        // what stress_client logs on a mismatch — localising the corrupting
        // leg (client-TX/server-RX vs server-TX/client-RX).
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        let mut total: u64 = 0;
        loop {
            match knet::timeout(idle, stream.read(&mut buf)).await {
                Ok(Ok(0)) => break, // EOF
                Ok(Ok(n)) => {
                    for &b in &buf[..n] {
                        h ^= b as u64;
                        h = h.wrapping_mul(0x0000_0100_0000_01B3);
                    }
                    total += n as u64;
                    if stream.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                    if stream.flush().await.is_err() {
                        break;
                    }
                }
                Ok(Err(_)) => break,
                Err(_) => break, // idle deadline: peer vanished without FIN
            }
        }
        println!(
            "t={} echo_stat peer={} bytes={} fnv={:016x}",
            now_secs(),
            peer,
            total,
            h
        );
        drop(stream);
        listener.remove_peer(peer);
    }

    fn now_secs() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }
}
