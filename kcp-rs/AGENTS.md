<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-07-22 | Updated: 2026-08-16 (tokio-only refactor) -->

# kcp-rs

## Purpose

KCP ARQ (Automatic Repeat-reQuest) reliable UDP protocol state machine — port of Go `github.com/xtaci/kcp-go/v5`. Ordered, reliable delivery over UDP with congestion control, Reed-Solomon FEC, and atomic SNMP counters. **All crypto lives in `kcrypt-rs`** — kcp-rs has **no** dependency on it and **no** crypto re-exports; depend on `kcrypt-rs` directly for `BlockCrypt` / `CryptEngine` / `CryptoBuf` / wire packing.

Async surface (optional): `KcpStream` is a tokio-TCP-shaped `AsyncRead`/`AsyncWrite` over UDP with optional FEC. **No encryption inside KcpStream** — crypto is an external `PacketTransport` (see `kcptun-common::CryptoTransport`).

## Key Files

| File | Description |
|------|-------------|
| `Cargo.toml` | Deps: `bytes`, `parking_lot`, `crossbeam`, `reed-solomon-erasure`, `crc32fast`, `thiserror`; optional `knet-rs`. **No** `kcrypt-rs` dependency |
| `src/lib.rs` | Crate root; large intentional `#![allow(clippy::…)]` list — do not "fix" |
| `src/kcp.rs` | Core `KCP` state machine: windows, RTO, flush, input, NoDelay |
| `src/segment.rs` | 24-byte LE wire header, `Command` enum, `SegmentPool` (SegQueue) |
| `src/fec.rs` | `FecEncoder` / `FecDecoder` / `fec_expand_packets` / `fec_kcp_from_recovered`; header types `0x00f1` (data) / `0x00f2` (parity). Go also has `0x00f3` (OOB) but this implementation does not claim OOB support |
| ~~`src/crypto_buf.rs`~~ | **Removed** (B2) — moved to `kcrypt-rs::wire`; see `../kcrypt-rs/AGENTS.md` |
| `src/conn.rs` | (feature `async`) `KcpStream` + `SharedIoState` (crate-private) + builder + background loops + poll impls + split halves; exposes monotonic `last_activity_ms` for upper-layer session expiry; **TcpStream-aligned surface** (`set_nodelay(bool)`, `set_nonblocking(bool)` honored by `read`/`write_all`, `shutdown(Shutdown)`, read/write timeouts, `split`/`into_split`, `readable`/`writable`, `peek`, `take_error`, builder `.connect_timeout`) + post-construction `set_kcp_window_size`; builder raw knobs are `.set_kcp_nodelay(n,i,r,nc)` (not `.nodelay`). Shared builder setters use one internal macro (re-exported for both listener builders). TX path uses `flush_tx_batch` (non-blocking `sendmmsg` fast path → async `send_batch` fallback on `WouldBlock`); `feed_raw_batch`/`feed_batch`/`tick` are `pub(crate)` — the listener worker path only |
| `src/transport.rs` | (feature `async`) `PacketTransport` trait + impls (`knet::DatagramSocket`, per-peer `PeerTransport`) + `PeerQueue` (listener demux) + `TransportWrapper`; `MAX_DATAGRAM` bounds receive storage; exchanged buffers return to the shared listener pool |
| `src/listener.rs` | (feature `async`) `KcpTcpListener` only — a Linux raw-TCP **transport factory** (one accepted TCP conn → one `KcpStream`), NOT a demux listener. Builder shares the config setters and is `IntoFuture`. The UDP demux listener lives in `sharded.rs` |
| `src/sharded.rs` | (feature `async`) `KcpListener` + `KcpListenerBuilder` — the UDP demux listener, three topologies (direct single worker / Linux SO_REUSEPORT group / reader pipeline). Workers build `KcpStream` with `background_input(false)` and feed inbound via `feed_raw_batch` (decrypt + KCP + encrypt on the worker thread). Public entry points `KcpListener::bind` / `from_socket` (also `bind_listener` / `from_socket_listener`) |
| `src/config.rs` | **Always-on** `KcpConfig` / `KcpMode`; `KCP::apply` / `set_mode` (B1) |
| `src/snmp.rs` | Global `DEFAULT_SNMP` atomic counters; `snmp_enable` / `snmp_add` / `snmp_store` |
| `README.md` | User-facing usage guide: sync + async API, wire format, config, testing |
| `test.sh` | Standalone test runner: sync (default) + `async` |
| `tests/data_correctness.rs` | Sync reliability + FEC data-correctness over in-memory flaky channel |
| `tests/kcpstream_integrity.rs` | Async `KcpStream` integrity over localhost UDP |
| `tests/kcpstream_listener.rs` | Server listen / client connect: accept echo, multi-peer demux, serve-after-close |

## Features

| Feature | Effect |
|---------|--------|
| *(default)* | Sync KCP only — no async deps |
| `async` | `KcpStream` + `PacketTransport` via `knet-rs` (tokio) |

## Async API sketch (`async`)

```rust
// Raw UDP, optional FEC; crypto is NOT here
let conn = KcpStream::connect("1.2.3.4:29900")
    .mtu(1400)
    .fec(10, 3)
    .mode(KcpMode::Fast3)
    .connect_timeout(Duration::from_secs(3)) // first WINS/ACK response, else TimedOut
    .build()
    .await?;

// Or plug a custom PacketTransport (e.g. CryptoTransport from kcptun-common)
let conn = KcpStream::with_transport(transport, cfg).await?;
```

- **TcpStream-aligned surface** (learn-cost ≈ `tokio::net::TcpStream`):
  `set_nodelay(bool)`/`nodelay()`, `set_nonblocking(bool)` (honored by `read`/`write_all`),
  `set_read_timeout`/`set_write_timeout` (+ getters),
  `shutdown(std::net::Shutdown)` half-close, `peek()`, `take_error()`,
  `split()`/`into_split()` (owned halves close the connection on last-half drop),
  `readable()`/`writable()`. Post-construction KCP tuning is `set_kcp_window_size`;
  the raw knobs are builder-only via `.set_kcp_nodelay(n,i,r,nc)` so they don't
  collide with the bool `set_nodelay`. MTU / stream mode / acknodelay stay on the builder.
- `poll_shutdown`/`poll_close` = **write-half close** (tokio semantics), NOT full
  close — the production stack calls `KcpStream::close()` explicitly. KCP has **no wire
  FIN**, so peer-aware half-close lives at the SMUX/session layer.
- `connect_timeout` forces a `WASK` probe and waits for any conv-valid inbound
  (`WINS`/ACK); it proves reachability + conv match, **not** connection establishment
  (KCP has no handshake). Requires the background input loop.
- **conv default (post-aliasing-fix)**: a plain UDP dial (`connect`/`connect_timeout`)
  gets a **fresh random conv per dial** (Go kcp-go `Dial` parity; `kcp_rs::random_conv()`)
  unless pinned via `.conv()` — even pinning `DEFAULT_CONV` is honoured (builder tracks
  the explicit set). `with_transport` keeps the configured conv (callers opt in —
  kcptun-common randomises UDP dials in `kcp_transport.rs`, TCP dials stay fixed);
  server-side builds use `.adopt_conv(true)` and learn the peer's conv from the first
  inbound segment. Why: a re-dial from a reused UDP source address must replace (not
  alias into) the lingering session — see `bugs/BUGREPORT_KCP_LISTENER_STALE_SESSION_ALIASING.md`.
  New pub API: `kcp_rs::random_conv()` (kcp-rs minor bump at next publish).
- **Stall diagnostics (post-RTO-analysis)** — all `KcpStream` getters, one
  kcp-rs minor bump at next publish (with `random_conv`):
  `tx_pkts`/`rx_pkts` (per-endpoint wire-datagram counters),
  `rx_rto`/`rx_srtt`/`rx_rttvar` (live RTO ladder inputs; under sustained
  loss `rx_rto` inflates toward the 60 s `RTO_MAX`, which is what
  tail-segment stalls are — `bugs/ANALYSIS_KCP_RTO_INFLATION_TAIL_STALLS.md`,
  Go-identical behaviour),
  `snd_nxt`/`snd_queue_len` (live; distinguishes queued-vs-flushed-vs-sent —
  note `wait_send()` is the CACHED flush-loop value, not live),
  `sending`/`raw_queue_len` (`is_sending` token + drain backlog — flat `tx`
  with `sending=true` means a stuck token),
  `flush_ticks`/`kcp_flush_count`/`rto_fires` (forensic triple: flat while
  `snd_nxt-snd_una>0` = flush loop dead / continue-loop / KCP-internal
  suppression respectively; `rf` delta == `tx` delta = healthy ladder),
  `last_io_error` (what `note_io_error` recorded),
  and test seams `testing_mark_dead` (`#[doc(hidden)]`).
  Also: the sharded feed path stamps `mark_inbound()` (both liveness clocks) —
  listener-fed sessions previously left `rx_age_ms()` frozen at build time;
  the listener does NOT reap `is_dead()` sessions (Go never reads the state —
  `bugs/BUGREPORT_LISTENER_DEAD_LINK_SESSION_KILL.md`).
- `PacketTransport`: datagram send/receive plus optional batch operations;
  `recv_vec` / `try_recv_vec` let queue-backed transports transfer reusable
  owned packet storage without an extra copy.
- FEC: `.fec(datashard, parityshard)` on builder; encode on flush, decode on input.
- The auto-tune probe (`AutoTune::probe`) runs on every packet while `should_tune` is set, so it must stay
  allocation-free: one sorted snapshot into a reusable buffer (skipping the sort when the samples already
  arrive in order), no rotated copy, no modulo per element. It must **not** be throttled by sample count — a
  fixed stride aliases with the group period (stride 16 never realigns with a 128+128 stream), which silently
  breaks retuning. `fec::tune_tests` pins the cost against the pre-change algorithm on random and structured
  histories.
- FEC parity latency uses `FEC_ENCODE_LATENCY_MS` (500, mirrors Go kcp-go v5.6.64 `maxFECEncodeLatency`). It is a fixed pseudo-RTO, not a live `rx_rto` sample — do not "improve" it into one.
- `transport::MAX_DATAGRAM` (2048) is a cross-crate contract, not a local knob: every receive slot is sized from it and `kcptun-common::validate_encrypted_mtu` derives the maximum legal `--mtu` from it (`mtu + crypto overhead <= 2048`). Its value is pinned by `transport::tests::max_datagram_is_the_shared_receive_slot`.
- `KcpListener`: `bind` → `accept() -> (KcpStream, SocketAddr)`. One bound UDP socket; demux by source addr
  via per-peer queue-backed `PeerTransport`. Reconnect = fresh client session (KCP SN continuity blocks
  same-stream reuse after a server-side close).
- Production `kcptun-client` / `kcptun-server` use this `KcpStream` through the
  shared `kcptun_common::KcptunSession` stack. Server UDP uses the single-reader
  `KcpListener` demultiplexer before constructing per-peer connections.

## For AI Agents

### Working In This Directory

- **Wire compatibility with kcp-go v5 is the primary constraint.** Control flow mirrors Go; crate-level clippy allows exist for that reason.
- `KCP::input()` must queue ACKs for **every** received Push segment.
- `snd_buf` cleanup: ACKed segments removed from the **front** in `flush()` (Go `k.snd_buf = k.snd_buf[1:]`).
- Constants (`IKCP_RTO_*`, `IKCP_PROBE_*`, `KCP_DEFAULT_WND=32`, cmds 81–84) must match Go.
- Crypto is entirely in `kcrypt-rs` — depend on it directly. kcp-rs has no crypto API / re-exports.
- `kcrypt_rs::wire::CryptoBuf` nonce is **not** the CFB IV (IV is fixed `GO_CFB_IV`); nonce is `[counter 8B][session_id 8B]`.
- SNMP collection is **opt-in** (`snmp_enable`) so hot paths stay free when unused.
- **Do not put crypto inside `KcpStream`.** Use `PacketTransport` wrappers (`CryptoTransport`).
- Snappy stays **outside** KcpStream (session-level over KCP user data in binaries / common).
- **Cancelable recv**: the input loop (conn.rs) and listener reader (listener.rs) race their socket `recv` against a `knet::CancellationToken` via `knet::race` instead of a 100ms poll tick, so `close()` cancels the recv immediately. There is **no** 100ms close-polling tick on the recv path — do not reintroduce one. Both the conn and the listener own a `cancel_token` cancelled in `close()`.
- **Non-blocking TX fast path**: all send paths (`try_drain_and_send`, flush loop fast/second drain, `feed_batch`) use `flush_tx_batch` which tries `try_send_batch` (non-blocking `sendmmsg` on Linux) first, falling back to async `send_batch` only on `WouldBlock` or `Ok(0)`. This eliminates the reactor scheduling hop per burst when the kernel send buffer has room (pipeline §15: worker only `try_push` to TX, not `await send`). The external-worker path (`feed_batch`) uses the sync variant `drain_and_flush_tx` (pipeline §14: `drain` + `flush`).
- **Sharded worker pipeline** (`sharded.rs`): `ShardedKcpListener` implements the three-stage pipeline (RX → bounded channel → Worker → TX). The reader task does `recvmmsg` (`try_recv_batch_from_into`) and `try_push`es `(peer, Vec<u8>)` to the correct worker's bounded `async_channel` — it never decrypts, runs KCP, or awaits a send. Each worker owns a `SessionMap` (`HashMap<SocketAddr, KcpStream>`) and processes inbound via `feed_batch` (Mode B: decrypt + KCP + encrypt on the same task). Session affinity: `fast_hash_peer(peer) % worker_count` routes to a fixed worker. Worker event loop: `drain_udp_batch` → `process_session` → `flush_ready_sessions`, with adaptive spin→park when idle. Build is async (`knet::spawn_task`) so the worker doesn't block on `KcpStream::build().await`.
- **Listener admission + lifecycle** (`sharded.rs`): a server session is created from a *single* inbound datagram, so `process_session` runs a pre-admission integrity gate — it builds the `PacketTransport` (crypto wrapper included) and calls `decrypt_packet_in_place` on the first burst **before** `KcpStream::build()`. A peer that cannot produce a valid CRC32 / AEAD tag gets no `KcpStream`, no flush loop and no accept-backlog entry (`WorkerPoolStats::unauthenticated_drops`). With no crypto configured the default `decrypt_packet_in_place` is the identity, so the gate is a no-op — the `max_sessions_per_worker` cap is the only bound in that configuration.
- `WorkerPoolLimits` defaults are **bounded, not unlimited**: `max_sessions_per_worker = 4096`, `idle_timeout = 600s`, `building_timeout = 10s`. KCP has no keepalive of its own, so a peer that sends one datagram and vanishes never trips `is_dead()`; `idle_timeout` is what frees its slot. `last_activity_ms` is stamped by `SharedIoState::mark_activity()` from **both** the input loop and the `feed_batch`/`feed_single` paths — server sessions run `background_input(false)` and have no input loop, so dropping the feed-path stamp makes the reaper kill live sessions.
- **Reaping closes.** `reaper_sweep` and `KcpListener::remove_peer` call `KcpStream::close()` on what they remove. The session map holds a non-owning clone (`detach_owner`), so a bare `remove` leaks the flush-loop task and leaves anything blocked on the stream parked forever.
- The lifecycle sweep runs in the listener's receive task on the `SWEEP_INTERVAL` (1s) cadence — never per batch, so its O(sessions) scan cannot scale with packet rate. A listener that goes completely silent has no wakeups, so the sweep also runs on an idle timeout.
- `KcpListener` has **no** worker-count knob: it runs entirely on the caller's runtime and feeds every session inline. Parallelism comes from the caller (kcptun-server `--shards`); the old `KCPTUN_WORKER_THREADS` env var and `worker_count(n)` builder are gone.

### Testing Requirements

- Standalone runner: `bash kcp-rs/test.sh` (sync default → `async`)
- Sync data-correctness: `cargo test -p kcp-rs --test data_correctness`
- Async integrity: `cargo test -p kcp-rs --features async --test kcpstream_integrity`
- Listener / connect: `cargo test -p kcp-rs --features async --test kcpstream_listener`
- In-module unit tests where present
- Async: `cargo test -p kcp-rs --features async`
- Interop: `bash test_e2e.sh` after segment/KCP/FEC changes
- Stress: `make stress` for flush/lock behavior under load

### Common Patterns

- Output callback: `Box<dyn FnMut(bytes::Bytes) + Send>` on `KCP`
- NoDelay modes applied by binaries via `nodelay/interval/resend/nc`
- FEC optional at **session / KcpStream layer** (`FecEncoder`/`FecDecoder`); no core-KCP FEC API
- Recovered FEC payload: `fec_kcp_from_recovered` (Go `r[2:sz]`); reconstruct present-flag is `true` = present
- Public API: `KCP`, `KcpConfig`/`KcpMode`, FEC + SNMP helpers. Crypto types (`BlockCrypt`, `CryptEngine`, `CryptoBuf`, `encrypt_batch`, wire helpers) live in `kcrypt-rs` — **not** re-exported here.
- With `async`: also `KcpStream`, `KcpConfig`, `KcpMode`, `PacketTransport`
  (including reusable owned receive buffers), `KcpListener`, `ShardedKcpListener`
- Wire packing / encrypt / offload heuristics (`CryptoBuf`, `encrypt_batch`, `decrypt_cfb_in_place`, `should_cpu_block_*`, `OffloadProfile`, …): see `../kcrypt-rs/AGENTS.md` — all live in `kcrypt_rs::wire`.

## Dependencies

### Internal

- None — crypto lives in `kcrypt-rs` (not a dependency of this crate)

### External

- `bytes`, `crossbeam`, `parking_lot`, `reed-solomon-erasure`, `crc32fast`
- optional `knet-rs` via feature `async`

<!-- MANUAL: -->

### 2026-10-11 audit repair contracts

- Pure `KcpConfig`/`KcpMode` and `KCP::apply` are available without `async`.
- `KCP::send` errors never partially append data. Message reception uses a
  bounded head-message reassembly allowance of `max(rcv_wnd, head.frg + 1)`;
  the head allowance is at most 256 segments and normal stream windows are
  unchanged. Never infer a peer's maximum message size from dynamic `rmt_wnd`.
- `set_mtu` rejects a decrease incompatible with queued/in-flight unacked
  payloads. Async builders reject MTU outside 50..=2048 (58 minimum with FEC) and invalid enabled
  FEC dimensions before opening sockets. External wrappers must budget their
  own extra wire overhead inside receive capacity.
- Trait `poll_flush`/`poll_shutdown` wait for transport drain, not peer ACKs;
  synchronous write shutdown only requests flushing. Preserve partial-send
  ordering and wake all blocked flushers. Discarded TX batches cannot later
  be reported as successfully flushed. KCP still has no wire FIN.
- Listener evictions must clean same-generation pending accepts. Removal of
  a building peer invalidates publication. Use one consistent lifecycle lock
  order and keep a hard accept-backlog bound even if live sessions are unlimited.
- FEC checked helpers reject invalid SIZE/header layout without advancing
  state. The 256-shard boundary is legal; auto-tuning needs observed history
  and cannot retroactively recover groups discarded before dimensions were known.
- Connection lifecycle maintains CurrEstab/MaxConn independent of optional
  packet statistics. Track once after successful build and close once across
  owners/clones/split halves. `SNMP::reset` preserves those gauges; do not
  duplicate automatic stream counting with manual session callbacks.
- Extreme RTT intermediate arithmetic is widened before the existing clamp.
  Preserve RTT sampling for ACKs that miss snd_buf (root AGENTS L-4 constraint).

### 2026-10-12 retention and admission optimization contracts

- SegmentPool retains at most 8 MiB of payload capacity and at most 16 KiB
  per payload, in addition to its object-count limit. Acquired buffers must
  have exclusive ownership. Public release rejects unknown shared tails.
  KCP may retain a tail only with the full capacity captured before splitting
  an exclusive buffer; charge that capacity, not the tail view. Check ownership
  and reclaim at acquire, replacing still-shared tails before returning them. These limits exclude active segments and allocator RSS.
- FEC groups keep full group IDs in the bounded map and indexed payload slots
  with a `[u64; 4]` presence bitmap. Preserve first-packet duplicate semantics,
  completed-group tombstones, PAWS wrap and auto-tune state replacement; slot
  modulo indices alone cannot identify a group.
- Per-IP concurrent admission counts building and published entries exactly
  once. Only the lifecycle entry owns a reservation, which moves at publish;
  build tasks and cloned streams must not duplicate it. Recheck the cap while
  registering, and release on every entry-removal path, including stale builds.
- The IP new-session limiter retains at most 4096 IP windows and expiry records,
  and processes at most 32 expired records per admission. A full table rejects
  unknown IPs rather than resetting an active IP's rate window through eviction.
  Established traffic bypasses this limiter; rate=0 disables it.
