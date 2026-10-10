//! O15 RSS bench: session-wide shared send pool vs per-stream pools.
//!
//! Phase A ("live"): one session, N open streams, each writes once — the
//! per-stream pools retain their 16 KiB buffers for the streams' lifetime.
//! With O15 the whole session shares ONE pool, so the delta collapses.
//! Phase B ("churn"): repeated open/write/remove cycles — shows the steady
//! RSS watermark under short connections.
//!
//! Usage:
//!   cargo run --release -p smux-rs --example churn_bench -- [live] [cycles] [per_cycle]
//!   defaults: live=1024, cycles=200, per_cycle=16
//!
//! VmRSS is Linux-only (`/proc/self/status`); on other platforms the absolute
//! numbers are reported as unavailable but the phases still run.

use smux_rs::{Config, Session, DEFAULT_CONFIG};

fn vmrss_kb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    line.split_whitespace().nth(1)?.parse().ok()
}

fn main() {
    let mut args = std::env::args().skip(1);
    let live: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(1024);
    let cycles: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(200);
    let per_cycle: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(16);

    let cfg = Config {
        version: 1,
        max_stream_buffer: 64 * 1024,
        ..DEFAULT_CONFIG.clone()
    };

    let rss0 = vmrss_kb();

    // ── Phase A: live streams ──
    let session = Session::new_client(&cfg).unwrap();
    let mut ids = Vec::with_capacity(live);
    for _ in 0..live {
        let s = session.open_stream().unwrap();
        let _ = s.write(&[0xABu8; 8 * 1024]);
        ids.push(s.id());
    }
    let rss_live = vmrss_kb();
    for id in &ids {
        let _ = session.remove_stream(*id);
    }
    drop(session);
    let rss_after_live = vmrss_kb();

    // ── Phase B: churn ──
    for _ in 0..cycles {
        let s = Session::new_client(&cfg).unwrap();
        let mut ids = Vec::with_capacity(per_cycle);
        for _ in 0..per_cycle {
            let st = s.open_stream().unwrap();
            let _ = st.write(&[0xCDu8; 8 * 1024]);
            ids.push(st.id());
        }
        for id in &ids {
            let _ = s.remove_stream(*id);
        }
        drop(s);
    }
    let rss_churn = vmrss_kb();

    let fmt = |r: Option<u64>| match r {
        Some(k) => format!("{k} KB"),
        None => "n/a (non-Linux)".to_string(),
    };
    println!(
        "churn_bench live={live} cycles={cycles} per_cycle={per_cycle} \
         rss_start={} rss_after_live={} rss_after_drop={} rss_after_churn={}",
        fmt(rss0),
        fmt(rss_live),
        fmt(rss_after_live),
        fmt(rss_churn),
    );
    if let (Some(a), Some(b)) = (rss0, rss_live) {
        println!(
            "live_phase_delta={} KB ({} KB/stream avg)",
            b as i64 - a as i64,
            (b as i64 - a as i64) / live as i64
        );
    }
}
