//! Corruption hunt harness for `BUGREPORT_KCP_SEGMENT_CONTENT_CORRUPTION.md`.
//!
//! In-process, high-throughput reproduction of the original 1h/20-client
//! stress run: one [`KcpListener`] + N client `KcpStream`s over localhost UDP,
//! Bernoulli loss injected on **both send legs** (client→server via a lossy
//! transport wrapper on the dial, server→client via the listener's
//! `transport_wrapper`), byte-exact request/response verification of every
//! exchange, and per-leg FNV localisation on mismatch.
//!
//! Unlike the WAN harness this runs millions of exchanges per hour with
//! `fail_{sid}_{seq}.sent.bin/.got.bin` dumps armed from exchange 0, and the
//! server-side echo records the FNV of exactly the bytes it read for the
//! failing exchange (`server_fnv`), so the corrupting leg self-identifies:
//!
//! * `server_fnv != expected_fnv` → corruption on the client→server leg
//!   (client send path, wire, or server receive path);
//! * `server_fnv == expected_fnv` but `got != expected` → server→client leg
//!   (server send path, wire, or client receive path).
//!
//! Loss is injected at *send* (the datagram is discarded before it reaches the
//! socket while the transport still reports it as handed off) — the same
//! visibility KCP has of real network loss: send succeeds, packet vanishes.
//! Both directions drop independently with probability `--loss`.
//!
//! ```text
//! cargo run -p kcp-rs --features async --release --example corruption_hunt -- \
//!     --sessions 16 --loss 0.30 --duration 3600
//! # exit code 1 + dumps under ./hunt_dumps/<ts>/ on any mismatch
//! ```

#[cfg(feature = "async")]
mod hunt {
    use std::collections::HashMap;
    use std::io::Write as _;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use bytes::Bytes;
    use kcp_rs::{KcpConfig, KcpListener, KcpMode, KcpStream, PacketTransport, DEFAULT_SNMP};

    // ─── lossy send wrapper ────────────────────────────────────────────────

    /// Drop-on-send Bernoulli wrapper. Survivors are forwarded to `inner`;
    /// dropped datagrams are still *reported* as handed off, exactly what the
    /// caller sees when the kernel accepts a datagram the network later
    /// discards. Recv-side is a pure pass-through (loss in the other direction
    /// is injected by a second wrapper on the peer's send leg).
    struct LossySendTransport {
        inner: Arc<dyn PacketTransport>,
        loss: f64,
        state: AtomicU64,
    }

    impl LossySendTransport {
        fn new(inner: Arc<dyn PacketTransport>, loss: f64) -> Arc<Self> {
            assert!((0.0..=0.9).contains(&loss), "loss must be in [0, 0.9]");
            Arc::new(Self {
                inner,
                loss,
                state: AtomicU64::new(0x853c_49e6_748f_ea9b),
            })
        }

        fn should_drop(&self) -> bool {
            let x = self
                .state
                .fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
            let mut z = x ^ (x >> 30);
            z = z.wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z ^= z >> 27;
            z = z.wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            let thr = (self.loss * 65_536.0) as u32;
            (z as u32 & 0xFFFF) < thr
        }

        fn filter(&self, packets: &[Bytes]) -> Vec<Bytes> {
            packets
                .iter()
                .filter(|_| !self.should_drop())
                .cloned()
                .collect()
        }

        /// Positional non-blocking send: every datagram (dropped or forwarded)
        /// counts toward the returned prefix, so the caller's `packets[sent..]`
        /// re-queue stays aligned and dropped datagrams behave as loss, not as
        /// re-queue. Stops at the first survivor the inner transport refuses.
        fn try_send_positional(
            &self,
            packets: &[Bytes],
            to: Option<SocketAddr>,
        ) -> std::io::Result<usize> {
            let mut sent = 0usize;
            for i in 0..packets.len() {
                if self.should_drop() {
                    sent = i + 1;
                    continue;
                }
                let single = &packets[i..i + 1];
                let r = match to {
                    None => self.inner.try_send_batch(single),
                    Some(t) => self.inner.try_send_batch_to(single, t),
                };
                match r {
                    Ok(k) if k >= 1 => sent = i + 1,
                    Ok(_) => break, // WouldBlock-equivalent: caller re-queues
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(e),
                }
            }
            Ok(sent)
        }
    }

    #[async_trait::async_trait]
    impl PacketTransport for LossySendTransport {
        async fn recv(&self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.inner.recv(buf).await
        }
        fn try_recv(&self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.inner.try_recv(buf)
        }
        fn decrypt_packet_in_place(&self, buf: &mut [u8], n: usize) -> usize {
            self.inner.decrypt_packet_in_place(buf, n)
        }
        async fn recv_vec(&self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
            self.inner.recv_vec(buf).await
        }
        fn try_recv_vec(&self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
            self.inner.try_recv_vec(buf)
        }
        fn try_recv_batch(&self, pool: &mut [Vec<u8>]) -> std::io::Result<usize> {
            self.inner.try_recv_batch(pool)
        }
        fn supports_recv_batch(&self) -> bool {
            self.inner.supports_recv_batch()
        }
        async fn send_batch(&self, packets: &[Bytes]) -> std::io::Result<()> {
            let kept = self.filter(packets);
            if kept.is_empty() {
                return Ok(());
            }
            self.inner.send_batch(&kept).await
        }
        async fn send_batch_to(
            &self,
            packets: &[Bytes],
            target: SocketAddr,
        ) -> std::io::Result<()> {
            let kept = self.filter(packets);
            if kept.is_empty() {
                return Ok(());
            }
            self.inner.send_batch_to(&kept, target).await
        }
        async fn send_urgent(&self, packets: &[Bytes]) -> std::io::Result<()> {
            let kept = self.filter(packets);
            if kept.is_empty() {
                return Ok(());
            }
            self.inner.send_urgent(&kept).await
        }
        async fn send_urgent_to(
            &self,
            packets: &[Bytes],
            target: SocketAddr,
        ) -> std::io::Result<()> {
            let kept = self.filter(packets);
            if kept.is_empty() {
                return Ok(());
            }
            self.inner.send_urgent_to(&kept, target).await
        }
        fn try_send_batch(&self, packets: &[Bytes]) -> std::io::Result<usize> {
            self.try_send_positional(packets, None)
        }
        fn try_send_batch_to(
            &self,
            packets: &[Bytes],
            target: SocketAddr,
        ) -> std::io::Result<usize> {
            self.try_send_positional(packets, Some(target))
        }
        fn local_addr(&self) -> std::io::Result<SocketAddr> {
            self.inner.local_addr()
        }
        fn set_ttl(&self, ttl: u32) -> std::io::Result<()> {
            self.inner.set_ttl(ttl)
        }
        fn ttl(&self) -> std::io::Result<u32> {
            self.inner.ttl()
        }
    }

    // ─── deterministic payload + hashing ───────────────────────────────────

    fn splitmix_next(z: u64) -> u64 {
        let mut x = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^ (x >> 31)
    }

    /// Byte-exact expected content for `(sid, seq)` — both ends can derive it,
    /// but only the client generates: the echo server hashes what it actually
    /// read, so a divergence on either leg is attributable.
    fn fill_payload(sid: u64, seq: u64, out: &mut [u8]) {
        let mut st = sid
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(seq.wrapping_mul(0xD1B5_4A32_D192_ED03))
            .wrapping_add(0x1234_5678_9ABC_DEF0);
        for ch in out.chunks_mut(8) {
            st = splitmix_next(st);
            let b = st.to_le_bytes();
            ch.copy_from_slice(&b[..ch.len()]);
        }
    }

    fn fnv1a(data: &[u8]) -> u64 {
        let mut h = 0xcbf2_9ce4_8422_2325u64;
        for &b in data {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01B3);
        }
        h
    }

    fn first_diff(a: &[u8], b: &[u8]) -> usize {
        a.iter()
            .zip(b.iter())
            .position(|(x, y)| x != y)
            .unwrap_or_else(|| a.len().min(b.len()))
    }

    // ─── shared state ──────────────────────────────────────────────────────

    #[derive(Default)]
    struct Counters {
        exchanges: AtomicU64,
        bytes: AtomicU64,
        mismatches: AtomicU64,
        timeouts: AtomicU64,
        session_errs: AtomicU64,
        echoes: AtomicU64,
        echo_errs: AtomicU64,
        dumps: AtomicU64,
        redials: AtomicU64,
        late_heals: AtomicU64,
    }

    /// Per accepted session: the FNV of the last exchange the echo fully
    /// read (strict request/response ⇒ that is the failing exchange), plus a
    /// handle to the server-side stream so any client alert can snapshot the
    /// peer's KCP state *live* — bracketing a stall with both ends' tx/rx
    /// counters is what separates "sender never emitted" from "lost after
    /// send".
    struct EchoInfo {
        last_fnv: AtomicU64,
        stream: Arc<KcpStream>,
    }

    struct Shared {
        counters: Counters,
        /// client local addr → echo-side info for that session.
        echo_info: Mutex<HashMap<SocketAddr, Arc<EchoInfo>>>,
        stop: AtomicBool,
        dump_dir: PathBuf,
        max_dumps: u64,
        alerts: Mutex<std::fs::File>,
    }

    impl Shared {
        fn alert(&self, line: &str) {
            // `t_ms` shares knet::mono_ms()'s origin with the state snapshots,
            // so alert/state timestamps compose into one timeline.
            let stamped = format!("{line} t_ms={}", knet::mono_ms());
            println!("{stamped}");
            let _ = writeln!(self.alerts.lock().unwrap(), "{stamped}");
            let _ = self.alerts.lock().unwrap().flush();
        }
    }

    // ─── read helpers ──────────────────────────────────────────────────────

    /// Phase-1 stall marker: crossing it without a complete exchange emits a
    /// diagnostic (and keeps waiting) instead of failing outright.
    const DEADLINE: Duration = Duration::from_secs(30);
    /// Hard deadline: past this the exchange is a confirmed wedge → redial.
    const HARD_DEADLINE: Duration = Duration::from_secs(90);

    enum Recv {
        /// `phase1_filled < n` ⇒ the exchange completed only after the 30 s
        /// marker — a self-healed stall; `elapsed_ms` is the heal timestamp
        /// and `phase1_state` the both-ends snapshot taken at the marker.
        /// `series` holds 5 s-interval dual-end samples collected while the
        /// read was blocked (empty for healthy exchanges) — the srv `tx`
        /// curve over the stall's first 30 s is the send-vs-lose discriminator.
        Complete {
            buf: Vec<u8>,
            phase1_filled: usize,
            elapsed_ms: u64,
            phase1_state: String,
            series: Vec<String>,
        },
        Stopped,
        Timeout {
            phase1_filled: usize,
            hard_filled: usize,
            phase1_state: String,
            series: Vec<String>,
        },
        Eof {
            filled: usize,
        },
        Io {
            filled: usize,
        },
    }

    /// Read exactly `n` bytes with a two-phase deadline and stop-flag
    /// responsiveness (1 s poll slices, mirroring `stress_client::recv_exact`).
    async fn recv_exact(
        conn: &KcpStream,
        n: usize,
        stop: &AtomicBool,
        srv_probe: &(dyn Fn() -> String + Sync),
    ) -> Recv {
        let mut buf = vec![0u8; n];
        let mut filled = 0usize;
        let start = Instant::now();
        let phase1 = start + DEADLINE;
        let hard = start + HARD_DEADLINE;
        let mut phase1_filled: Option<usize> = None;
        let mut phase1_state = String::new();
        let mut series: Vec<String> = Vec::new();
        // t+0 baseline (so every later frame is a delta), 1 s cadence for the
        // first 15 s of a stall (the unread window), then 5 s — a 90 s hard
        // timeout yields ≤30 frames.
        let sample = |filled: usize| -> String {
            format!(
                "t+{}ms f={} cli=[{}] srv=[{}]",
                start.elapsed().as_millis(),
                filled,
                state_of(conn),
                srv_probe(),
            )
        };
        series.push(sample(filled));
        let mut next_sample = start + Duration::from_secs(1);
        loop {
            if stop.load(Ordering::Relaxed) {
                return Recv::Stopped;
            }
            let now = Instant::now();
            if now >= next_sample {
                series.push(sample(filled));
                let step = if series.len() < 16 {
                    Duration::from_secs(1)
                } else {
                    Duration::from_secs(5)
                };
                next_sample = now + step;
            }
            if phase1_filled.is_none() && now >= phase1 {
                phase1_filled = Some(filled);
                phase1_state = format!("cli=[{}] srv=[{}]", state_of(conn), srv_probe());
            }
            if now >= hard {
                return Recv::Timeout {
                    phase1_filled: phase1_filled.unwrap_or(filled),
                    hard_filled: filled,
                    phase1_state,
                    series,
                };
            }
            if filled == n {
                return Recv::Complete {
                    buf,
                    phase1_filled: phase1_filled.unwrap_or(n),
                    elapsed_ms: start.elapsed().as_millis() as u64,
                    phase1_state,
                    series,
                };
            }
            match knet::timeout(Duration::from_secs(1), conn.read(&mut buf[filled..])).await {
                Ok(Ok(0)) => {
                    return Recv::Eof { filled };
                }
                Ok(Ok(k)) => filled += k,
                Ok(Err(_)) => {
                    return Recv::Io { filled };
                }
                Err(_) => continue,
            }
        }
    }

    // ─── server echo (framed 8191 → fnv → echo) ───────────────────────────

    async fn echo_session(stream: KcpStream, peer: SocketAddr, size: usize, shared: Arc<Shared>) {
        let stream = Arc::new(stream);
        let info = Arc::new(EchoInfo {
            last_fnv: AtomicU64::new(0),
            stream: stream.clone(),
        });
        shared.echo_info.lock().unwrap().insert(peer, info.clone());
        // Must outlive the client's 90 s hard deadline: a read timeout here
        // kills a session whose exchange is merely stalled, and the rebuilt
        // session cannot resume mid-stream (sn ≫ rcv_nxt is dropped forever).
        let _ = stream.set_read_timeout(Some(Duration::from_secs(120)));
        let _ = stream.set_write_timeout(Some(Duration::from_secs(120)));
        let mut buf = vec![0u8; size];
        let mut echoes = 0u64;
        loop {
            if shared.stop.load(Ordering::Relaxed) {
                break;
            }
            // read exactly one exchange (client is strict request/response)
            let mut filled = 0usize;
            let mut failure: Option<&'static str> = None;
            while filled < size {
                match stream.read(&mut buf[filled..]).await {
                    Ok(0) => {
                        failure = Some("eof");
                        break;
                    }
                    Ok(k) => filled += k,
                    Err(e) => {
                        failure = Some(if e.kind() == std::io::ErrorKind::TimedOut {
                            "timeout"
                        } else {
                            "io"
                        });
                        break;
                    }
                }
            }
            if failure.is_some() {
                // EOF normally means the client redialed/closed — alert so a
                // vanished session correlates with its client-side event
                // (a silent exit made `srv=[gone]` undiagnosable), except at
                // shutdown where every client closes at once.
                if !shared.stop.load(Ordering::Relaxed) {
                    if failure != Some("eof") {
                        shared.counters.echo_errs.fetch_add(1, Ordering::Relaxed);
                    }
                    shared.alert(&format!(
                        "ECHO_ERR peer={peer} kind={} echoes={} partial={} state={}",
                        failure.unwrap_or("?"),
                        echoes,
                        filled,
                        state_of(&stream),
                    ));
                }
                break;
            }
            // record BEFORE echoing: when the client fails this exchange,
            // its lookup can only happen after the echo byte stream completed,
            // hence after this store.
            info.last_fnv.store(fnv1a(&buf), Ordering::Release);
            if let Err(e) = stream.write_all(&buf).await {
                shared.counters.echo_errs.fetch_add(1, Ordering::Relaxed);
                shared.alert(&format!(
                    "ECHO_ERR peer={peer} kind=write echoes={echoes} err={e} state={}",
                    state_of(&stream),
                ));
                break;
            }
            echoes += 1;
            shared.counters.echoes.fetch_add(1, Ordering::Relaxed);
            shared
                .counters
                .bytes
                .fetch_add(size as u64, Ordering::Relaxed);
        }
        shared.echo_info.lock().unwrap().remove(&peer);
        stream.close();
    }

    // ─── client session ────────────────────────────────────────────────────

    /// One KCP state snapshot for failure alerts — the evidence that
    /// distinguishes "peer stopped answering" (`is_dead`, `rmt_wnd=0`,
    /// `snd_una` frozen) from "my send path wedged" (`wait_send` full,
    /// `last_activity_ms` growing while writes succeed).
    fn state_of(s: &KcpStream) -> String {
        format!(
            "dead={} closed={} snd_una={} snd_nxt={} sq={} rcv_nxt={} \
             wait_send={} rmt_wnd={} rto={} srtt={} var={} rx_age={}ms \
             tx={} rx={} send={} rawq={} ft={} kf={} rf={} conv_mismatch={} peer_restarts={} \
             err=[{}]",
            s.is_dead(),
            s.is_closed(),
            s.snd_una(),
            s.snd_nxt(),
            s.snd_queue_len(),
            s.rcv_nxt(),
            s.wait_send(),
            s.rmt_wnd(),
            s.rx_rto(),
            s.rx_srtt(),
            s.rx_rttvar(),
            s.rx_age_ms(),
            s.tx_pkts(),
            s.rx_pkts(),
            s.sending(),
            s.raw_queue_len(),
            s.flush_ticks(),
            s.kcp_flush_count(),
            s.rto_fires(),
            s.conv_mismatch_count(),
            s.peer_restart_count(),
            s.last_io_error().unwrap_or_else(|| "none".to_string()),
        )
    }

    /// Fresh ephemeral socket + lossy wrapper + built stream for one
    /// (re)dial. `None` means the dial failed (caller counts/alerts/redials).
    #[allow(clippy::too_many_arguments)]
    async fn dial_session(
        sid: u64,
        listener_addr: SocketAddr,
        loss: f64,
        mode: KcpMode,
        datashard: u32,
        parityshard: u32,
        sndwnd: u32,
        rcvwnd: u32,
    ) -> Option<KcpStream> {
        let probe = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).ok()?;
        let local = probe.local_addr().ok()?;
        drop(probe);
        let sock = knet::UdpSocket::connect(local, listener_addr).ok()?;
        let inner: Arc<dyn PacketTransport> = Arc::new(knet::DatagramSocket::Udp(sock));
        let transport = LossySendTransport::new(inner, loss);
        let conv = 0xC0DE_0000u32 ^ (sid as u32);
        let cfg = KcpConfig {
            mode,
            datashard,
            parityshard,
            sndwnd,
            rcvwnd,
            conv,
            ..KcpConfig::default()
        };
        KcpStream::with_transport(transport, listener_addr)
            .connected(true)
            .conv(conv)
            .config(cfg)
            .build()
            .await
            .ok()
    }

    /// Redial budget per session: a wedged session is closed, rebuilt and
    /// resumed at the same `seq`, so one stall never costs the remaining
    /// hours of exposure.
    const MAX_REDIALS: u32 = 8;

    #[allow(clippy::too_many_arguments)]
    async fn client_session(
        sid: u64,
        listener_addr: SocketAddr,
        loss: f64,
        size: usize,
        mode: KcpMode,
        datashard: u32,
        parityshard: u32,
        sndwnd: u32,
        rcvwnd: u32,
        shared: Arc<Shared>,
    ) {
        let mut seq: u64 = 0;
        let mut redials: u32 = 0;
        'redial: loop {
            if shared.stop.load(Ordering::Relaxed) {
                return;
            }
            let stream = match dial_session(
                sid,
                listener_addr,
                loss,
                mode,
                datashard,
                parityshard,
                sndwnd,
                rcvwnd,
            )
            .await
            {
                Some(s) => s,
                None => {
                    shared.counters.session_errs.fetch_add(1, Ordering::Relaxed);
                    shared.alert(&format!("SESSION_ERR sid={sid} seq={seq} dial failed"));
                    redials += 1;
                    if redials > MAX_REDIALS {
                        shared.alert(&format!("GIVE_UP sid={sid} redials={redials}"));
                        return;
                    }
                    knet::sleep_ms(500).await;
                    continue 'redial;
                }
            };
            let _ = stream.set_write_timeout(Some(Duration::from_secs(30)));
            let local = stream.local_addr().unwrap();
            let mut payload = vec![0u8; size];

            loop {
                if shared.stop.load(Ordering::Relaxed) {
                    stream.close();
                    return;
                }
                fill_payload(sid, seq, &mut payload);

                // (kind, detail) — set on any failure that should redial.
                let mut fail: Option<(&'static str, String)> = None;
                if let Err(e) = stream.write_all(&payload).await {
                    fail = Some(("write", e.to_string()));
                } else {
                    // (last echo FNV, live server-side KCP state) for this
                    // session's server stream — if the echo already exited,
                    // ("gone") and only client-side evidence remains.
                    let echo_snapshot = || -> (Option<u64>, String) {
                        let map = shared.echo_info.lock().unwrap();
                        match map.get(&local) {
                            Some(info) => (
                                Some(info.last_fnv.load(Ordering::Acquire)),
                                state_of(&info.stream),
                            ),
                            None => (None, "gone".to_string()),
                        }
                    };
                    let srv_probe = || echo_snapshot().1;
                    match recv_exact(&stream, size, &shared.stop, &srv_probe).await {
                        Recv::Complete {
                            buf: got,
                            phase1_filled,
                            elapsed_ms,
                            phase1_state,
                            series,
                        } => {
                            // Baseline + ≥2 samples ⇒ blocked ≥2 s (p45's
                            // healthy exchanges run ~1 s, so a 1-frame
                            // overrun is normal latency, not a stall).
                            if series.len() > 2 {
                                shared.counters.late_heals.fetch_add(1, Ordering::Relaxed);
                                let (_, srv_p2) = echo_snapshot();
                                shared.alert(&format!(
                                    "TIMEOUT_LATE sid={sid} seq={seq} local={local} \
                                     filled_at_30s={phase1_filled}/{size} \
                                     self_healed after_ms={elapsed_ms} \
                                     p1=[{phase1_state}] p2=[cli=[{}] srv=[{srv_p2}]] \
                                     series=[{}]",
                                    state_of(&stream),
                                    series.join(" | "),
                                ));
                            }
                            if got != payload {
                                let fd = first_diff(&payload, &got);
                                let expected_fnv = fnv1a(&payload);
                                let got_fnv = fnv1a(&got);
                                let (server_fnv, srv_state) = echo_snapshot();
                                let n =
                                    shared.counters.mismatches.fetch_add(1, Ordering::Relaxed) + 1;
                                let mut dump_note = String::new();
                                if shared.counters.dumps.fetch_add(1, Ordering::Relaxed)
                                    < shared.max_dumps
                                {
                                    let base =
                                        shared.dump_dir.join(format!("hunt_sid{sid}_seq{seq}"));
                                    std::fs::write(base.with_extension("sent.bin"), &payload).ok();
                                    std::fs::write(base.with_extension("got.bin"), &got).ok();
                                    dump_note = format!(" dumps={}", base.display());
                                }
                                shared.alert(&format!(
                                    "MISMATCH sid={sid} seq={seq} local={local} first_diff={fd} size={} \
                                     expected_fnv={expected_fnv:016x} got_fnv={got_fnv:016x} \
                                     server_fnv={} srv=[{srv_state}] count={n}{dump_note}",
                                    payload.len(),
                                    match server_fnv {
                                        Some(v) => format!("{v:016x}"),
                                        None => "none".to_string(),
                                    },
                                ));
                            }
                            shared.counters.exchanges.fetch_add(1, Ordering::Relaxed);
                            seq += 1;
                        }
                        Recv::Stopped => {
                            stream.close();
                            return;
                        }
                        Recv::Timeout {
                            phase1_filled,
                            hard_filled,
                            phase1_state,
                            series,
                        } => {
                            let (server_fnv, srv_state) = echo_snapshot();
                            fail = Some((
                                "timeout",
                                format!(
                                    "phase1_filled={phase1_filled}/{size} \
                                     hard_filled={hard_filled}/{size} \
                                     p1=[{phase1_state}] srv=[{srv_state}] \
                                     server_fnv={} series=[{}]",
                                    match server_fnv {
                                        Some(v) => format!("{v:016x}"),
                                        None => "none".to_string(),
                                    },
                                    series.join(" | "),
                                ),
                            ));
                        }
                        Recv::Eof { filled } => {
                            fail = Some(("read", format!("eof filled={filled}/{size}")));
                        }
                        Recv::Io { filled } => {
                            fail = Some(("read", format!("io filled={filled}/{size}")));
                        }
                    }
                }

                if let Some((kind, detail)) = fail {
                    if shared.stop.load(Ordering::Relaxed) {
                        stream.close();
                        return;
                    }
                    // Snapshot BEFORE close(): the state at stall time is the
                    // whole point of this alert.
                    let state = state_of(&stream);
                    if kind == "timeout" {
                        shared.counters.timeouts.fetch_add(1, Ordering::Relaxed);
                        shared.alert(&format!(
                            "TIMEOUT sid={sid} seq={seq} local={local} after={detail} state={state}"
                        ));
                    } else {
                        shared.counters.session_errs.fetch_add(1, Ordering::Relaxed);
                        shared.alert(&format!(
                            "SESSION_ERR sid={sid} seq={seq} local={local} {kind}: {detail} state={state}"
                        ));
                    }
                    stream.close();
                    redials += 1;
                    if redials > MAX_REDIALS {
                        shared.alert(&format!("GIVE_UP sid={sid} redials={redials}"));
                        return;
                    }
                    shared.counters.redials.fetch_add(1, Ordering::Relaxed);
                    knet::sleep_ms(500).await;
                    continue 'redial;
                }
            }
        }
    }

    // ─── driver ────────────────────────────────────────────────────────────

    pub struct Args {
        pub sessions: u64,
        pub loss: f64,
        pub duration: u64,
        pub size: usize,
        pub mode: String,
        pub datashard: u32,
        pub parityshard: u32,
        pub sndwnd: u32,
        pub rcvwnd: u32,
        pub dump_dir: Option<PathBuf>,
        pub max_dumps: u64,
    }

    pub fn run(args: Args) {
        // Counter gate: kcp-rs no-ops every SNMP update unless enabled, and
        // the progress line's fec_recovered is how we prove loss is landing.
        kcp_rs::snmp_enable();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let mode = match args.mode.as_str() {
                "fast" => KcpMode::Fast,
                "fast2" => KcpMode::Fast2,
                "fast3" => KcpMode::Fast3,
                "normal" => KcpMode::Normal,
                other => panic!("unknown mode {other}"),
            };
            let dump_dir = args.dump_dir.clone().unwrap_or_else(|| {
                let ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                PathBuf::from("hunt_dumps").join(format!("run_{ts}"))
            });
            std::fs::create_dir_all(&dump_dir).expect("create dump dir");
            let alerts =
                std::fs::File::create(dump_dir.join("alerts.log")).expect("create alerts.log");
            let shared = Arc::new(Shared {
                counters: Counters::default(),
                echo_info: Mutex::new(HashMap::new()),
                stop: AtomicBool::new(false),
                dump_dir: dump_dir.clone(),
                max_dumps: args.max_dumps,
                alerts: Mutex::new(alerts),
            });

            // Server: lossy send wrapper drops server→client datagrams.
            let loss = args.loss;
            let listener = KcpListener::bind("127.0.0.1:0")
                .mode(mode)
                .sndwnd(args.sndwnd)
                .rcvwnd(args.rcvwnd)
                .fec(args.datashard, args.parityshard)
                .transport_wrapper(move |inner, _peer| {
                    LossySendTransport::new(inner, loss) as Arc<dyn PacketTransport>
                })
                .build()
                .await
                .expect("bind listener");
            let addr = listener.local_addr().expect("listener local_addr");
            println!(
                "HUNT sessions={} loss={:.2} size={} mode={} fec={}/{} wnd={}/{} duration={}s \
                 dump_dir={}",
                args.sessions,
                args.loss,
                args.size,
                args.mode,
                args.datashard,
                args.parityshard,
                args.sndwnd,
                args.rcvwnd,
                args.duration,
                std::fs::canonicalize(&dump_dir)
                    .unwrap_or_else(|_| dump_dir.clone())
                    .display(),
            );

            // Accept loop → framed echo sessions.
            {
                let shared = Arc::clone(&shared);
                let size = args.size;
                knet::spawn_task(async move {
                    loop {
                        if shared.stop.load(Ordering::Relaxed) {
                            break;
                        }
                        match listener.accept().await {
                            Ok((stream, peer)) => {
                                let shared2 = Arc::clone(&shared);
                                knet::spawn_task(async move {
                                    echo_session(stream, peer, size, shared2).await;
                                });
                            }
                            Err(e) => {
                                if !shared.stop.load(Ordering::Relaxed) {
                                    shared.alert(&format!("ACCEPT_ERR {e}"));
                                }
                                break;
                            }
                        }
                    }
                    listener.close();
                });
            }

            // Progress line.
            {
                let shared = Arc::clone(&shared);
                knet::spawn_task(async move {
                    let start = Instant::now();
                    let mut last_exch = 0u64;
                    loop {
                        knet::sleep_ms(10_000).await;
                        if shared.stop.load(Ordering::Relaxed) {
                            break;
                        }
                        let c = &shared.counters;
                        let exch = c.exchanges.load(Ordering::Relaxed);
                        let g = |x: &AtomicU64| x.load(Ordering::Relaxed);
                        let rate = exch.saturating_sub(last_exch) / 10;
                        last_exch = exch;
                        println!(
                            "t={:4}s exchanges={} ({}/s) echoes={} mismatches={} timeouts={} \
                             session_errs={} echo_errs={} dumps={} redials={} late_heals={} \
                             bytes={} fec_recovered={} fec_errs={}",
                            start.elapsed().as_secs(),
                            exch,
                            rate,
                            g(&c.echoes),
                            g(&c.mismatches),
                            g(&c.timeouts),
                            g(&c.session_errs),
                            g(&c.echo_errs),
                            g(&c.dumps),
                            g(&c.redials),
                            g(&c.late_heals),
                            g(&c.bytes),
                            g(&DEFAULT_SNMP.fec_recovered),
                            g(&DEFAULT_SNMP.fec_errs),
                        );
                    }
                });
            }

            // Client sessions.
            let mut joins = Vec::new();
            for sid in 0..args.sessions {
                let shared = Arc::clone(&shared);
                joins.push(knet::spawn_task(client_session(
                    sid,
                    addr,
                    args.loss,
                    args.size,
                    mode,
                    args.datashard,
                    args.parityshard,
                    args.sndwnd,
                    args.rcvwnd,
                    shared,
                )));
            }

            // Duration.
            if args.duration > 0 {
                knet::sleep_ms(args.duration * 1000).await;
            } else {
                // run until Ctrl-C is impossible to detect cheaply here —
                // treat 0 as "24 h".
                knet::sleep_ms(24 * 3600 * 1000).await;
            }
            shared.stop.store(true, Ordering::Release);
            for j in joins {
                let _ = j.await;
            }
            knet::sleep_ms(500).await;

            let c = &shared.counters;
            let g = |x: &AtomicU64| x.load(Ordering::Relaxed);
            let mismatches = g(&c.mismatches);
            println!(
                "RESULT sessions={} loss={:.2} duration={} exchanges={} mismatches={} \
                 timeouts={} session_errs={} echo_errs={} echoes={} bytes={} dumps={} \
                 redials={} late_heals={} fec_recovered={} fec_errs={}",
                args.sessions,
                args.loss,
                args.duration,
                g(&c.exchanges),
                mismatches,
                g(&c.timeouts),
                g(&c.session_errs),
                g(&c.echo_errs),
                g(&c.echoes),
                g(&c.bytes),
                g(&c.dumps),
                g(&c.redials),
                g(&c.late_heals),
                g(&DEFAULT_SNMP.fec_recovered),
                g(&DEFAULT_SNMP.fec_errs),
            );
            if mismatches > 0 || g(&c.timeouts) > 0 {
                std::process::exit(1);
            }
        });
    }
}

fn main() {
    #[cfg(not(feature = "async"))]
    {
        eprintln!("build with --features async");
        std::process::exit(2);
    }
    #[cfg(feature = "async")]
    {
        use std::path::PathBuf;
        let mut sessions = 16u64;
        let mut loss = 0.30f64;
        let mut duration = 0u64;
        let mut size = 8191usize;
        let mut mode = "fast".to_string();
        let mut datashard = 10u32;
        let mut parityshard = 3u32;
        let mut sndwnd = 512u32;
        let mut rcvwnd = 512u32;
        let mut dump_dir: Option<PathBuf> = None;
        let mut max_dumps = 10u64;

        let mut it = std::env::args().skip(1);
        while let Some(arg) = it.next() {
            let mut val = || {
                it.next()
                    .unwrap_or_else(|| panic!("missing value for {arg}"))
            };
            match arg.as_str() {
                "--sessions" => sessions = val().parse().unwrap(),
                "--loss" => loss = val().parse().unwrap(),
                "--duration" => duration = val().parse().unwrap(),
                "--size" => size = val().parse().unwrap(),
                "--mode" => mode = val(),
                "--datashard" => datashard = val().parse().unwrap(),
                "--parityshard" => parityshard = val().parse().unwrap(),
                "--sndwnd" => sndwnd = val().parse().unwrap(),
                "--rcvwnd" => rcvwnd = val().parse().unwrap(),
                "--dump-dir" => dump_dir = Some(PathBuf::from(val())),
                "--max-dumps" => max_dumps = val().parse().unwrap(),
                other => panic!("unknown arg {other}"),
            }
        }

        hunt::run(hunt::Args {
            sessions,
            loss,
            duration,
            size,
            mode,
            datashard,
            parityshard,
            sndwnd,
            rcvwnd,
            dump_dir,
            max_dumps,
        });
    }
}
