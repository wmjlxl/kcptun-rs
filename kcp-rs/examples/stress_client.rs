//! Stability-test client for the 1 h / 20-process kcp-rs stress run.
//!
//! One process = one kcp-rs client. Two concurrent loops over independent
//! [`KcpStream`]s share one rate limiter:
//!
//!   * **burst** — every exchange dials a fresh `KcpStream` (peppers the
//!     listener accept/session-build path), sends a payload from a size
//!     schedule covering MTU/SMUX boundaries, reads the echo back and
//!     compares byte-for-byte;
//!   * **stream** — one long-lived `KcpStream` pushing/verifying 64 KiB
//!     blocks continuously (the long-haul data path + stall detection).
//!
//! Payloads are generated from a deterministic splitmix64 PRNG seeded by
//! `(worker_id, loop_tag, seq)` and the exact sent bytes are kept in memory —
//! the echo must compare `==`, so any corruption/reorder/dup/truncation is
//! caught and reported with the first differing offset.
//!
//! Output schema is byte-compatible with `stability_1h/report.py`:
//! `worker_<id>.jsonl` events, `worker_<id>.status.hist` throughput history,
//! `worker_<id>_summary.json` counters.
//!
//! Exit code: 0 clean · 2 at least one verify error · 1 fatal.
//!
//! ```text
//! cargo build --release -p kcp-rs --features async --example stress_client
//! ./stress_client --id 1 --addr 127.0.0.1:29901 --duration 3600 --logdir out/
//! ```

#[cfg(not(feature = "async"))]
fn main() {
    eprintln!("build with --features async");
    std::process::exit(2);
}

#[cfg(feature = "async")]
fn main() {
    // ── args ────────────────────────────────────────────────────────────────
    let mut id = 0u32;
    let mut addr = "127.0.0.1:29901".to_string();
    let mut rate = 262144u64; // bytes/sec, combined across both loops
    let mut duration = 3600u64;
    let mut logdir = ".".to_string();
    let mut mode = "fast".to_string();
    let mut datashard = 10u32;
    let mut parityshard = 3u32;
    let mut sndwnd = 512u32;
    let mut rcvwnd = 512u32;

    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut val = || {
            it.next()
                .unwrap_or_else(|| panic!("missing value for {arg}"))
        };
        match arg.as_str() {
            "--id" => id = val().parse().expect("id"),
            "--addr" => addr = val(),
            "--rate" => rate = val().parse().expect("rate"),
            "--duration" => duration = val().parse().expect("duration"),
            "--logdir" => logdir = val(),
            "--mode" => mode = val(),
            "--datashard" => datashard = val().parse().expect("datashard"),
            "--parityshard" => parityshard = val().parse().expect("parityshard"),
            "--sndwnd" => sndwnd = val().parse().expect("sndwnd"),
            "--rcvwnd" => rcvwnd = val().parse().expect("rcvwnd"),
            "--help" | "-h" => {
                eprintln!(
                    "usage: stress_client --id N [--addr HOST:PORT] [--rate B/s] \
                     [--duration SECS] [--logDIR DIR] [--mode fast|fast2|fast3|normal] \
                     [--datashard N] [--parityshard N] [--sndwnd N] [--rcvwnd N]"
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

    let code = imp::run(imp::Args {
        id,
        addr,
        rate,
        duration,
        logdir,
        mode: kcp_mode,
        datashard,
        parityshard,
        sndwnd,
        rcvwnd,
    });
    std::process::exit(code);
}

#[cfg(feature = "async")]
mod imp {
    use kcp_rs::{KcpMode, KcpStream};
    use knet::AsyncWriteExt;
    use std::fs::{File, OpenOptions};
    use std::io::Write;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Mutex;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    pub struct Args {
        pub id: u32,
        pub addr: String,
        pub rate: u64,
        pub duration: u64,
        pub logdir: String,
        pub mode: KcpMode,
        pub datashard: u32,
        pub parityshard: u32,
        pub sndwnd: u32,
        pub rcvwnd: u32,
    }

    /// Size schedule: 1 B, small, MTU 1350 ±1, SMUX framesize 8192 ±1,
    /// 64 KiB, 256 KiB, 512 KiB.
    const SIZES: [usize; 12] = [
        1, 64, 1400, 1349, 1350, 1351, 8191, 8192, 8193, 65536, 262144, 524288,
    ];
    const STREAM_BLOCK: usize = 65536;
    // Recalibrated 2026-10-12 (user-approved) after the RTO analysis
    // (bugs/ANALYSIS_KCP_RTO_INFLATION_TAIL_STALLS.md): under sustained
    // loss, KCP's retransmission slots stretch with the inflated `rx_rto`
    // up to the Go-identical `RTO_MAX = 60 s` cap, so a *self-healing* tail
    // stall can legitimately run to ~63 s (one cap slot) and ~120 s if that
    // slot's attempt is itself lost. The old 60 s thresholds counted those
    // expected stalls as failures (formal run: stalls=4, timeouts=396 with
    // zero verify errors and zero reconnects). 150 s sits above two cap
    // slots + margin: algorithm-expected stalls heal below it, while a real
    // wedge (never heals — the pre-fix failure mode) still trips both the
    // recv deadline and the watchdog, so the `stalls == 0` criterion stays
    // strict and only true hangs can fail it.
    const RECV_DEADLINE_SECS: u64 = 150;
    const CONNECT_DEADLINE_SECS: u64 = 30;
    const STALL_SECS: u64 = 150;
    const MAX_ERROR_SAMPLES: usize = 50;

    fn now_secs() -> f64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0)
    }

    /// splitmix64 → deterministic position-dependent payload per (id, tag, seq).
    fn fill_payload(seed: u64, out: &mut [u8]) {
        let mut s = seed;
        for chunk in out.chunks_mut(8) {
            s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = s;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            let b = z.to_le_bytes();
            for (i, v) in chunk.iter_mut().enumerate() {
                *v = b[i];
            }
        }
    }

    fn first_diff(a: &[u8], b: &[u8]) -> usize {
        a.iter()
            .zip(b.iter())
            .position(|(x, y)| x != y)
            .unwrap_or(a.len().min(b.len()))
    }

    /// Smooth send-rate limiter shared by both loops (leaky bucket, no credit).
    struct Pacer {
        rate: f64,
        next_t: Mutex<Instant>,
    }

    impl Pacer {
        fn new(rate: u64) -> Self {
            Self {
                rate: rate as f64,
                next_t: Mutex::new(Instant::now()),
            }
        }
        async fn wait(&self, n: usize) {
            if self.rate <= 0.0 {
                return;
            }
            let delay = {
                let mut next = self.next_t.lock().unwrap();
                let now = Instant::now();
                let start = (*next).max(now);
                *next = start + Duration::from_secs_f64(n as f64 / self.rate);
                start.saturating_duration_since(now)
            };
            if !delay.is_zero() {
                knet::sleep_ms(delay.as_millis() as u64).await;
            }
        }
    }

    #[derive(Default)]
    struct Counters {
        bytes_sent: AtomicU64,
        bytes_verified: AtomicU64,
        burst_conns: AtomicU64,
        stream_blocks: AtomicU64,
        verify_errors: AtomicU64,
        timeouts: AtomicU64,
        conn_errors: AtomicU64,
        stalls: AtomicU64,
        last_progress_ms: AtomicU64, // unix millis
        error_samples: Mutex<Vec<String>>,
    }

    impl Counters {
        fn progress_age_secs(&self) -> f64 {
            let last = self.last_progress_ms.load(Ordering::Relaxed);
            if last == 0 {
                return 0.0;
            }
            (now_secs() * 1000.0 - last as f64) / 1000.0
        }
        fn note_progress(&self) {
            self.last_progress_ms
                .store((now_secs() * 1000.0) as u64, Ordering::Relaxed);
        }
        fn add_verify_error(&self, sample: String) {
            self.verify_errors.fetch_add(1, Ordering::Relaxed);
            let mut v = self.error_samples.lock().unwrap();
            if v.len() < MAX_ERROR_SAMPLES {
                v.push(sample);
            }
        }
    }

    struct Ctx {
        args: Args,
        stop: AtomicBool,
        pacer: Pacer,
        c: Counters,
        ev: Mutex<File>,
        hist: Mutex<File>,
    }

    impl Ctx {
        fn emit(&self, ev: &str, extra: &str) {
            let mut f = self.ev.lock().unwrap();
            let _ = writeln!(
                f,
                "{{\"ts\":{:.3},\"ev\":\"{}\",\"worker\":{}{}}}",
                now_secs(),
                ev,
                self.args.id,
                extra
            );
            let _ = f.flush();
        }
    }

    /// Connect with retry inside CONNECT_DEADLINE; returns conn_errors on each
    /// failed attempt (mirrors the harness worker semantics).
    async fn connect(ctx: &Ctx) -> std::io::Result<KcpStream> {
        let deadline = Instant::now() + Duration::from_secs(CONNECT_DEADLINE_SECS);
        let mut last: Option<std::io::Error> = None;
        while Instant::now() < deadline && !ctx.stop.load(Ordering::Relaxed) {
            let res = KcpStream::connect(ctx.args.addr.as_str())
                .mode(ctx.args.mode)
                .sndwnd(ctx.args.sndwnd)
                .rcvwnd(ctx.args.rcvwnd)
                .fec(ctx.args.datashard, ctx.args.parityshard)
                .build()
                .await;
            match res {
                Ok(s) => return Ok(s),
                Err(e) => {
                    last = Some(e);
                    ctx.c.conn_errors.fetch_add(1, Ordering::Relaxed);
                    knet::sleep_ms(1000).await;
                }
            }
        }
        Err(last.unwrap_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::Interrupted, "connect aborted by stop")
        }))
    }

    /// FNV-1a64 — correlates a dumped payload with the server's per-session
    /// stream hash (stress_server logs it on session end).
    fn fnv1a(data: &[u8]) -> u64 {
        let mut h = 0xcbf2_9ce4_8422_2325u64;
        for &b in data {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01B3);
        }
        h
    }

    /// Read exactly `n` bytes before the deadline (1 s poll slices so the
    /// stop flag stays responsive). Returns short-read EOF as an error.
    async fn recv_exact(
        conn: &KcpStream,
        n: usize,
        deadline: Instant,
        stop: &AtomicBool,
    ) -> Result<Vec<u8>, &'static str> {
        let mut buf = vec![0u8; n];
        let mut filled = 0usize;
        while filled < n {
            if stop.load(Ordering::Relaxed) {
                return Err("stopped");
            }
            if Instant::now() >= deadline {
                return Err("timeout");
            }
            let slice = &mut buf[filled..];
            match knet::timeout(Duration::from_secs(1), conn.read(slice)).await {
                Ok(Ok(0)) => return Err("eof"),
                Ok(Ok(k)) => filled += k,
                Ok(Err(_)) => return Err("io"),
                Err(_) => continue, // 1 s poll slice, keep trying until deadline
            }
        }
        Ok(buf)
    }

    async fn paced_send(ctx: &Ctx, conn: &mut KcpStream, data: &[u8]) -> Result<(), &'static str> {
        const CHUNK: usize = 16384;
        let mut off = 0;
        while off < data.len() {
            if ctx.stop.load(Ordering::Relaxed) {
                return Err("stopped");
            }
            let end = (off + CHUNK).min(data.len());
            ctx.pacer.wait(end - off).await;
            if conn.write_all(&data[off..end]).await.is_err() {
                return Err("io");
            }
            off = end;
        }
        if conn.flush().await.is_err() {
            return Err("io");
        }
        Ok(())
    }

    fn record_failure(ctx: &Ctx, kind: &str, seq: usize, size: usize, r: Result<usize, &str>) {
        match r {
            Err("timeout") => {
                ctx.c.timeouts.fetch_add(1, Ordering::Relaxed);
                ctx.emit(
                    "timeout",
                    &format!(",\"seq\":{seq},\"size\":{size},\"kind\":\"{kind}\""),
                );
            }
            Err("eof") | Err("io") => {
                ctx.c.conn_errors.fetch_add(1, Ordering::Relaxed);
                ctx.emit(
                    "io_error",
                    &format!(",\"seq\":{seq},\"size\":{size},\"kind\":\"{kind}\""),
                );
            }
            Err(_) => {} // stopped: not an error
            Ok(_) => {}
        }
    }

    fn verify(ctx: &Ctx, payload: &[u8], got: &[u8], seq: usize, size: usize, loop_tag: &str) {
        if payload == got {
            ctx.c.bytes_sent.fetch_add(size as u64, Ordering::Relaxed);
            ctx.c
                .bytes_verified
                .fetch_add(size as u64, Ordering::Relaxed);
            ctx.c.note_progress();
        } else {
            let sample = format!(
                "{{\"kind\":\"mismatch\",\"sent_len\":{},\"recv_len\":{},\"first_diff\":{},\
                 \"seq\":{seq},\"size\":{size},\"loop\":\"{loop_tag}\",\
                 \"fnv_sent\":\"{:016x}\",\"fnv_got\":\"{:016x}\"}}",
                payload.len(),
                got.len(),
                first_diff(payload, got),
                fnv1a(payload),
                fnv1a(got),
            );
            // Keep both buffers: correlate fnv_sent with the server's
            // per-session stream hash to localise the corrupting leg
            // (client-TX/server-RX vs server-TX/client-RX).
            // bugs/BUGREPORT_KCP_SEGMENT_CONTENT_CORRUPTION.md
            let base = format!("{}/fail_{}_{}", ctx.args.logdir, ctx.args.id, seq);
            let _ = std::fs::write(format!("{base}.sent.bin"), payload);
            let _ = std::fs::write(format!("{base}.got.bin"), got);
            ctx.emit(
                "verify_fail",
                &format!(",\"sample\":{sample},\"dump\":\"{base}\""),
            );
            ctx.c.add_verify_error(sample);
            ctx.c.bytes_sent.fetch_add(size as u64, Ordering::Relaxed);
        }
    }

    /// Burst loop: fresh `KcpStream` per exchange, size schedule, byte-exact echo.
    async fn burst_loop(ctx: std::sync::Arc<Ctx>) {
        let mut seq = 0usize;
        while !ctx.stop.load(Ordering::Relaxed) {
            let size = SIZES[seq % SIZES.len()];
            let mut payload = vec![0u8; size];
            fill_payload(((ctx.args.id as u64) << 32) ^ seq as u64, &mut payload);

            let mut conn = match connect(&ctx).await {
                Ok(c) => c,
                Err(_) => {
                    if ctx.stop.load(Ordering::Relaxed) {
                        break;
                    }
                    ctx.emit("conn_fail", &format!(",\"seq\":{seq}"));
                    knet::sleep_ms(2000).await;
                    seq += 1;
                    continue;
                }
            };

            let deadline = Instant::now() + Duration::from_secs(RECV_DEADLINE_SECS);
            let send_res = paced_send(&ctx, &mut conn, &payload).await;
            let got = match send_res {
                Ok(()) => recv_exact(&conn, size, deadline, &ctx.stop).await,
                Err(e) => Err(e),
            };
            match got {
                Ok(got) => {
                    verify(&ctx, &payload, &got, seq, size, "burst");
                    ctx.c.burst_conns.fetch_add(1, Ordering::Relaxed);
                }
                Err("stopped") => break,
                Err(e) => record_failure(&ctx, "burst", seq, size, Err(e)),
            }
            drop(conn); // owner drop → close; server reaps via idle deadline
            seq += 1;
        }
        ctx.emit(
            "loop_exit",
            &format!(",\"loop\":\"burst\",\"exchanges\":{seq}"),
        );
    }

    /// Stream loop: one long-lived connection, 64 KiB blocks, reconnect on break.
    async fn stream_loop(ctx: std::sync::Arc<Ctx>) {
        let mut seq = 0usize;
        while !ctx.stop.load(Ordering::Relaxed) {
            let mut conn = match connect(&ctx).await {
                Ok(c) => c,
                Err(_) => {
                    if ctx.stop.load(Ordering::Relaxed) {
                        break;
                    }
                    ctx.emit("conn_fail", ",\"loop\":\"stream\"");
                    knet::sleep_ms(2000).await;
                    continue;
                }
            };
            let mut broke = false;
            while !ctx.stop.load(Ordering::Relaxed) && !broke {
                let mut payload = vec![0u8; STREAM_BLOCK];
                fill_payload(
                    ((ctx.args.id as u64) << 32) ^ (1u64 << 40) ^ seq as u64,
                    &mut payload,
                );
                let deadline = Instant::now() + Duration::from_secs(RECV_DEADLINE_SECS);
                let send_res = paced_send(&ctx, &mut conn, &payload).await;
                let got = match send_res {
                    Ok(()) => recv_exact(&conn, STREAM_BLOCK, deadline, &ctx.stop).await,
                    Err(e) => Err(e),
                };
                match got {
                    Ok(got) => {
                        verify(&ctx, &payload, &got, seq, STREAM_BLOCK, "stream");
                        ctx.c.stream_blocks.fetch_add(1, Ordering::Relaxed);
                        seq += 1;
                    }
                    Err("stopped") => break,
                    Err(e) => {
                        record_failure(&ctx, "stream", seq, STREAM_BLOCK, Err(e));
                        broke = true;
                    }
                }
            }
            drop(conn);
            if !ctx.stop.load(Ordering::Relaxed) {
                knet::sleep_ms(1000).await; // reconnect gap after any break
            }
        }
        ctx.emit(
            "loop_exit",
            &format!(",\"loop\":\"stream\",\"blocks\":{seq}"),
        );
    }

    /// Watchdog: flag ≥60 s without verified progress as a stall incident.
    async fn watchdog(ctx: std::sync::Arc<Ctx>) {
        let mut emitted = false;
        while !ctx.stop.load(Ordering::Relaxed) {
            knet::sleep_ms(5000).await;
            let age = ctx.c.progress_age_secs();
            if age > STALL_SECS as f64 {
                if !emitted {
                    ctx.c.stalls.fetch_add(1, Ordering::Relaxed);
                    ctx.emit("stall", &format!(",\"age_s\":{age:.1}"));
                    emitted = true;
                }
            } else {
                emitted = false;
            }
        }
    }

    /// 10 s throughput-history snapshots (report.py reads these).
    async fn status_loop(ctx: std::sync::Arc<Ctx>) {
        while !ctx.stop.load(Ordering::Relaxed) {
            knet::sleep_ms(10_000).await;
            let row = format!(
                "{{\"ts\":{:.3},\"worker\":{},\"bytes_sent\":{},\"bytes_verified\":{},\
                 \"burst_conns\":{},\"stream_blocks\":{},\"verify_errors\":{},\
                 \"timeouts\":{},\"conn_errors\":{},\"stalls\":{},\"progress_age_s\":{:.1}}}",
                now_secs(),
                ctx.args.id,
                ctx.c.bytes_sent.load(Ordering::Relaxed),
                ctx.c.bytes_verified.load(Ordering::Relaxed),
                ctx.c.burst_conns.load(Ordering::Relaxed),
                ctx.c.stream_blocks.load(Ordering::Relaxed),
                ctx.c.verify_errors.load(Ordering::Relaxed),
                ctx.c.timeouts.load(Ordering::Relaxed),
                ctx.c.conn_errors.load(Ordering::Relaxed),
                ctx.c.stalls.load(Ordering::Relaxed),
                ctx.c.progress_age_secs(),
            );
            let mut h = ctx.hist.lock().unwrap();
            let _ = writeln!(h, "{row}");
            let _ = h.flush();
        }
    }

    pub fn run(args: Args) -> i32 {
        std::fs::create_dir_all(&args.logdir).expect("logdir");
        let ev_path = format!("{}/worker_{}.jsonl", args.logdir, args.id);
        let hist_path = format!("{}/worker_{}.status.hist", args.logdir, args.id);
        let cur_path = format!("{}/worker_{}.status", args.logdir, args.id);
        let sum_path = format!("{}/worker_{}_summary.json", args.logdir, args.id);

        let ev = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&ev_path)
            .expect("open events");
        let hist = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&hist_path)
            .expect("open hist");

        let ctx = std::sync::Arc::new(Ctx {
            stop: AtomicBool::new(false),
            pacer: Pacer::new(args.rate),
            c: Counters::default(),
            ev: Mutex::new(ev),
            hist: Mutex::new(hist),
            args,
        });

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(3)
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            ctx.emit(
                "start",
                &format!(
                    ",\"addr\":\"{}\",\"rate\":{},\"duration_s\":{}",
                    ctx.args.addr, ctx.args.rate, ctx.args.duration
                ),
            );
            let h1 = knet::spawn_task(burst_loop(std::sync::Arc::clone(&ctx)));
            let h2 = knet::spawn_task(stream_loop(std::sync::Arc::clone(&ctx)));
            let h3 = knet::spawn_task(watchdog(std::sync::Arc::clone(&ctx)));
            let h4 = knet::spawn_task(status_loop(std::sync::Arc::clone(&ctx)));

            let deadline = Instant::now() + Duration::from_secs(ctx.args.duration);
            while Instant::now() < deadline {
                let left = deadline.saturating_duration_since(Instant::now());
                knet::sleep_ms(left.min(Duration::from_secs(5)).as_millis() as u64).await;
            }
            ctx.stop.store(true, Ordering::Relaxed);
            for h in [h1, h2, h3, h4] {
                let _ = h.await;
            }

            // final status row + summary (report.py schema)
            let (bs, bv, bc, sb, ve, to, ce, st) = (
                ctx.c.bytes_sent.load(Ordering::Relaxed),
                ctx.c.bytes_verified.load(Ordering::Relaxed),
                ctx.c.burst_conns.load(Ordering::Relaxed),
                ctx.c.stream_blocks.load(Ordering::Relaxed),
                ctx.c.verify_errors.load(Ordering::Relaxed),
                ctx.c.timeouts.load(Ordering::Relaxed),
                ctx.c.conn_errors.load(Ordering::Relaxed),
                ctx.c.stalls.load(Ordering::Relaxed),
            );
            let age = ctx.c.progress_age_secs();
            let samples = ctx.c.error_samples.lock().unwrap().join(",");
            let summary = format!(
                "{{\n  \"worker\": {},\n  \"duration_s\": {},\n  \"bytes_sent\": {},\n  \
                 \"bytes_verified\": {},\n  \"burst_conns\": {},\n  \"stream_blocks\": {},\n  \
                 \"verify_errors\": {},\n  \"stalls\": {},\n  \"timeouts\": {},\n  \
                 \"conn_errors\": {},\n  \"progress_age_s\": {:.1},\n  \
                 \"error_samples\": [{}]\n}}\n",
                ctx.args.id, ctx.args.duration, bs, bv, bc, sb, ve, st, to, ce, age, samples
            );
            if let Ok(mut f) = File::create(&sum_path) {
                let _ = f.write_all(summary.as_bytes());
            }
            if let Ok(mut f) = File::create(&cur_path) {
                let _ = f.write_all(summary.as_bytes());
            }
            ctx.emit(
                "finish",
                &format!(
                    ",\"bytes_sent\":{bs},\"bytes_verified\":{bv},\"verify_errors\":{ve},\
                     \"stalls\":{st},\"timeouts\":{to},\"conn_errors\":{ce}"
                ),
            );
            println!(
                "worker {}: sent={bs} verified={bv} verify_errors={ve} stalls={st} \
                 timeouts={to} conn_errors={ce}",
                ctx.args.id
            );
        });

        let ve = ctx.c.verify_errors.load(Ordering::Relaxed);
        let ce = ctx.c.conn_errors.load(Ordering::Relaxed);
        if ve > 0 {
            2
        } else if ce > 0 && ctx.c.bytes_verified.load(Ordering::Relaxed) == 0 {
            1 // connected nothing at all → fatal
        } else {
            0
        }
    }
}
