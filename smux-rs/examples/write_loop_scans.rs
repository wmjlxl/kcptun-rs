//! Isolates the per-flush-cycle scans of the session write loop, per the audit
//! review's B01 ("separate prepare / refill / reap / lock-wait measurements").
//!
//! The write loop in `kcptun-common/src/kcptun_session.rs` walks the stream set
//! several times per cycle:
//!   1. `any(pending_send() > 0)` — the "is there anything to flush" probe,
//!   2. `prepare_outbound_into_controlled` — builds the frame batch,
//!   3. `reap_stale_streams` — idle-stream reaping,
//!   4. the tail scan that decides whether to loop again (`more_data`/`fin_pending`).
//!
//! Reports ns per phase per cycle for a given stream count, idle and saturated,
//! so the "is the scanning itself a bottleneck" question is answered with
//! numbers instead of speculation.
//!
//! Usage:
//!   cargo run --release -p smux-rs --example write_loop_scans -- [streams] [iters]

use bytes::BytesMut;
use smux_rs::{Config, Session, DEFAULT_CONFIG};
use std::time::{Duration, Instant};

fn config() -> Config {
    Config {
        version: 2,
        max_frame_size: 16 * 1024,
        ..DEFAULT_CONFIG.clone()
    }
}

fn bench(label: &str, iters: usize, mut f: impl FnMut()) {
    for _ in 0..(iters / 10).max(1) {
        f();
    }
    let start = Instant::now();
    for _ in 0..iters {
        f();
    }
    let ns = start.elapsed().as_nanos() as f64 / iters as f64;
    println!("  {label:<34} {ns:10.1} ns/cycle");
}

fn main() {
    let streams: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);
    let iters: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_000);

    println!("streams={streams} iters={iters}");

    // ── Idle session: every stream open, nothing to send ────────────────────
    let idle = Session::new_client(&config()).unwrap();
    let handles: Vec<_> = (0..streams)
        .map(|_| {
            let s = idle.open_stream().unwrap();
            idle.queue_syn(s.id());
            s
        })
        .collect();
    let mut out = BytesMut::with_capacity(64 * 1024);

    println!(" idle (no pending data)");
    bench("1. any(pending_send > 0)", iters, || {
        let has = idle.streams().lock().values().any(|s| s.pending_send() > 0);
        std::hint::black_box(has);
    });
    bench("2. prepare_outbound (64 KiB budget)", iters, || {
        out.clear();
        let fins = idle.prepare_outbound_into_controlled(&mut out, 64 * 1024, 2, true);
        std::hint::black_box((fins, out.len()));
    });
    bench("3. reap_stale_streams(30s)", iters, || {
        let n = idle.reap_stale_streams(Duration::from_secs(30));
        std::hint::black_box(n);
    });
    bench("4. tail scan (more_data/fin_pending)", iters, || {
        let mut more_data = false;
        let mut fin_pending = false;
        for stream in idle.streams().lock().values() {
            if stream.pending_send() > 0 {
                more_data = true;
            }
            if stream.is_local_closed() && !stream.is_fin_sent() {
                fin_pending = true;
            }
        }
        std::hint::black_box((more_data, fin_pending));
    });
    bench("5. mark_fins_sent(1 id)", iters, || {
        let id = handles.first().map(|s| s.id()).unwrap_or(0);
        idle.mark_fins_sent(&[id]);
    });

    // ── Saturated: every stream has data queued (download-like) ─────────────
    let busy = Session::new_client(&config()).unwrap();
    let busy_handles: Vec<_> = (0..streams)
        .map(|_| {
            let s = busy.open_stream().unwrap();
            busy.queue_syn(s.id());
            let _ = s.write(&[0xABu8; 8 * 1024]);
            s
        })
        .collect();

    println!(" saturated (data queued on every stream)");
    bench("1. any(pending_send > 0)", iters, || {
        let has = busy.streams().lock().values().any(|s| s.pending_send() > 0);
        std::hint::black_box(has);
    });
    bench("2. prepare_outbound (64 KiB budget)", iters, || {
        out.clear();
        let fins = busy.prepare_outbound_into_controlled(&mut out, 64 * 1024, 2, true);
        // Refill what prepare drained so the next cycle is representative.
        for s in &busy_handles {
            if s.pending_send() < 64 * 1024 {
                let _ = s.write(&[0xABu8; 8 * 1024]);
            }
        }
        std::hint::black_box(fins);
    });
}
