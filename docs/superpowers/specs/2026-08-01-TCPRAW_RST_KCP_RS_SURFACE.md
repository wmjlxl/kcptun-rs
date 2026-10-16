# tcpraw RST-Suppression & kcp-rs TCP Surface Design

**Date**: 2026-08-01
**Status**: implemented
**Implementation**: branch `feat-tcpraw-rst-kcp-rs`, 10 commits, plan `docs/superpowers/plans/2026-08-01-TCPRAW_RST_KCP_RS_SURFACE.md`
**Predecessor**: `2026-07-29-TCPRAW_TRANSPORT_DESIGN.md`（已删除） (tcpraw transport implementation; this spec supersedes its TCP_REPAIR-only fallback assumption)

## Context

`kio-rs/src/net/tcpraw.rs` already ships a working TCP raw transport: real TCP handshake → `TCP_REPAIR` silences the kernel TCP stack → KCP datagrams ride as raw `SOCK_RAW` + `IPPROTO_TCP` segments (`[20B TCP hdr + 12B TS opts][KCP payload]`), wire-matching Go `xtaci/tcpraw`. `--tcp` is wired into both binaries via `DatagramSocket::TcpRaw`.

This design closes two gaps:

1. **kcp-rs library TCP surface** — no ergonomic `KcpConn::connect_tcp` / server-side TCP accept in the library.
2. **RST suppression ("屏蔽 RST")** — currently accidental, not designed:
   - capture threads never parse the `rst` flag; RST seq/ack still flows into `update_flow_from_segment` (flow-corruption risk)
   - no clean-close path (kernel may emit RST when the real socket is dropped)
   - `TcpRawListener` never deregisters accepted connections (stale-flow leak)
   - no iptables-DROP fallback when `TCP_REPAIR` is unavailable

## Design Principles

- **TCP is a shell; KCP is the reliability layer.** The rawtcp transport never reports RST/death to KCP. KCP detects death via its own retransmission timeout; SMUX keepalive drives session close + client redial (existing fix, `BUGREPORT_NO_RECONNECT_ON_SERVER_RESTART.md`).
- **Wire-compatible with Go `xtaci/tcpraw`** (Rust↔Go interop both directions).
- **TCP_REPAIR primary; iptables TTL-DROP as fallback** for old kernels (< 3.5) / environments that block `TCP_REPAIR`.

## Four-Step Takeover

```
[client]                                              [server]
  1. standard kernel TCP handshake (SYN/SYN-ACK/ACK)       — kernel does the handshake
  2. takeover: TCP_REPAIR  (or iptables TTL-DROP)          — kernel stack isolated, seq/ack ours
  3. KCP over raw TCP segments — all inbound RST ignored   — KCP self-controls reliability
  4. drain RX + graceful close                             — kernel never emits RST
```

### Step 1 — handshake (unchanged)

Existing `TcpStream::connect` / `TcpListener::accept` complete the kernel 3-way handshake. Handshake packets use the default TTL, so they are never matched by the iptables TTL-DROP rule.

### Step 2 — takeover: `Takeover` enum

```rust
enum Takeover { Repair, Iptables }
```

**Repair (primary)**: `TCP_REPAIR` on → read seq/ack via `TCP_QUEUE_SEQ` (existing `capture_repair_state`). No external binary. Requires `CAP_NET_ADMIN`.

**Iptables (fallback, Go-faithful)**: OUTPUT-chain TTL-DROP:

```
client: iptables -t filter -A OUTPUT -m ttl --ttl-eq 1 -p tcp -s <local_ip> --sport <local_port> -d <peer_ip> --dport <peer_port> -j DROP
server: iptables -t filter -A OUTPUT -m ttl --ttl-eq 1 -p tcp --sport <listen_port> -j DROP
```

combined with `setsockopt(IP_TTL = 1)` on the real socket. Every packet the **kernel** sends for the connection (ACK / window update / **RST** / retransmit) is TTL=1 → dropped at OUTPUT → **the kernel's own RST never leaves the machine**. Raw-socket packets use TTL=64 (default) → pass. TTL distinguishes kernel output from raw output.

- seq/ack start at 0 and converge via the peer's ACK field (Go's model) — reuses existing `update_flow_from_segment`; no `TCP_INFO`, no handshake capture needed.
- Requires the `iptables` binary + `ttl` xtables match module (kernel-standard). No silent UDP fallback: if neither Repair nor Iptables can be established → clear error.

**Selection**: auto-probe (Repair → Iptables → error); `KCPTCP_TAKEOVER=repair|iptables` env override for tests / triage.

### Step 3 — inbound RST filtering

- `TcpSegmentView` gains `rst: bool` (parsed from `tcp[13] & 0x04`).
- Both capture threads (client + server): after port/peer filtering, `if seg.rst { continue; }` **before** `update_flow_from_segment` and before payload delivery.
- Effect: forged/middlebox RST can no longer corrupt flow seq/ack/ts_ecr, and never reaches KCP. The flow only dies from KCP's own timeout.

### Step 4 — clean close: `TcpRawConn::close()` (called by `Drop`)

**Repair path:**
1. Drain RX: `TCP_REPAIR_QUEUE = RECV_QUEUE`, non-blocking `recv` until `WouldBlock`.
2. `TCP_REPAIR` off (kernel view was frozen at capture time; raw-only sends keep it consistent).
3. `shutdown(SHUT_WR)` → kernel sends FIN with the correct seq.
4. Drain again + close (shrinks the FIN→close race window).

**Iptables path:**
1. `setTTL(real, 64)` (so the FIN passes the OUTPUT rule).
2. Delete the per-conn OUTPUT rule.
3. `shutdown(SHUT_WR)`; stop the drain thread (`shutdown(SHUT_RD)` unblocks it).
4. Close.

Both paths: the peer's capture sees the FIN (no PSH payload) → ignored; the peer's KCP times out and runs its own symmetric close. A residual race (peer keeps sending while we close → our close emits RST) is neutralized by Step 3 on the peer side. The two layers together close the "kernel throws RST to kill the connection" loop.

## kcp-rs Library Surface

In `kcp-rs/src/conn.rs` (behind `async-*` features):

```rust
impl KcpConn {
    /// Linux raw-TCP dial (tcpraw). Non-Linux → runtime io::Unsupported (stub).
    pub fn connect_tcp(addr: impl ToSocketAddrs) -> KcpConnBuilder;
    // build(): kio::tcpraw_dial(remote) → DatagramSocket::TcpRaw → with_transport(...).connected(true)
}

pub struct KcpTcpListener { listener: kio::TcpRawListener, config: KcpConfig, closed: AtomicBool }

impl KcpTcpListener {
    pub fn bind(addr: impl ToSocketAddrs) -> KcpTcpListenerBuilder;   // kio::tcpraw_listen
    pub async fn accept(&self) -> io::Result<(KcpConn, SocketAddr)>;
    //   TcpRawListener::accept (cpu_block) → DatagramSocket::TcpRaw(conn)
    //   → KcpConn::with_transport(...).connected(true).config(cfg).build().await
    pub fn local_addr(&self) -> io::Result<SocketAddr>;
    pub fn close(&self);   // stop new accepts; accepted KcpConns keep their raw_fd Arc and continue
}
```

- **1 TCP connection = 1 KCP session** (the TCP model). `KcpListener` (single-socket multi-peer UDP demux) is NOT extended; a new `KcpTcpListener` mirrors the `KcpListener`/`KcpListenerBuilder` shape instead.
- Non-Linux: `tcpraw_dial`/`tcpraw_listen` are stubs returning `Unsupported` at runtime — no `cfg` gating needed in kcp-rs, consistent with binary `--tcp`.
- Zero new abstractions on the data path; `PacketTransport for kio::DatagramSocket` already covers `TcpRaw`.

## Close / Reconnect Semantics

### Stale-flow deregistration (existing bug fix)

Accepted `TcpRawConn` holds `Arc`s to the listener's `channels`/`flows` maps plus its own `peer_addr`; `Drop` removes its entries with a **remove-if-still-mine** guard so a newer same-key connection's entry is never clobbered:

```rust
if let Some(existing) = flows.get(&peer) {
    if Arc::ptr_eq(existing, &self.flow) { flows.remove(&peer); }
}
// channels likewise
```

Fixes: unbounded map growth on abandoned connections, and dead-channel misrouting after peer reconnect.

### Drain thread lifecycle (iptables path)

Per-connection thread (client + accepted server conn): blocking `recv` → discard. Stop: `shutdown(SHUT_RD)` → EOF → exit. Prevents kernel recv-buffer growth and close-with-unread → RST.

### iptables rule ownership & SIGKILL self-healing

- Client: per-conn rule (full 4-tuple), deleted on conn drop.
- Server: one per-listen-port rule (covers all accepted conns — accepted sockets share the listen port as src), deleted on listener drop.
- Bind/dial: `-D` (ignore errors) then `-A` — self-heals rules leaked by `kill -9`.
- Leaked rules are low-harm (only drop TTL=1 packets from that port; raw data path is TTL=64) but are cleaned anyway.

### Death → reconnect

- `raw_send` never errors on peer death (`sendto` into the void still succeeds) → the transport never signals RST/death to KCP.
- Death chain: KCP retransmits without ACK → SMUX keepalive timeout → session close → client redial (fresh TCP conn, fresh raw flow, seq-0 convergence).
- Server restart: the session-layer fix (`BUGREPORT_NO_RECONNECT_ON_SERVER_RESTART.md`) already handles recovery; the transport only needs rule cleanup (above) and a working accept loop. TCP mode's fresh 4-tuple per reconnect avoids the UDP KCP SN-continuity conflict.

## Testing

1. **Pure-function unit tests** (Linux `cfg`, no root, `cargo test -p kio-rs`): segment build / parse / checksum against Go constants; flow update incl. **RST-skip assertion** (RST must not change seq/ack/ts_ecr).
2. **Root-gated integration tests** (`#[ignore]` / `KCPTCP_ROOT_TEST=1`, Linux): loopback round-trip on both takeover paths (`KCPTCP_TAKEOVER=repair|iptables`); forged-RST injection mid-transfer → flow survives; **sniffer asserts zero RST on the wire** for the flow across transfer + close; graceful FIN observed on close; `iptables -C` fails after close (rule cleaned); takeover probe/fallback.
3. **kcp-rs library tests** (Linux, root): `KcpConn::connect_tcp` ↔ `KcpTcpListener::accept` bidirectional + FEC 10/3 (mirrors existing UDP `bidirectional_localhost`); reconnect / stale-flow deregistration; RST-injection resilience on large transfer.
4. **Go interop**: `test_e2e.sh` gains `--tcp` cases — Rust client ↔ Go server and Go client ↔ Rust server (validates the TTL-DROP path against Go's iptables path).
5. **Gate**: non-Linux `make gate` clean (Linux suites `cfg`-gated); Linux container runs `cargo test -p kio-rs` + `cargo test -p kcp-rs --features async-tokio` + root-gated suite + e2e `--tcp`. Ties into the pending Linux-verify container plan (see kio-rs `net/AGENTS.md`).

## Permissions

- `TCP_REPAIR`: `CAP_NET_ADMIN`.
- Raw socket (`SOCK_RAW` + `IPPROTO_TCP`): `CAP_NET_RAW`.
- iptables fallback: `iptables` binary + `CAP_NET_ADMIN` + `ttl` match module.
- All match Go `xtaci/tcpraw`'s own requirements.

## File Change Map

- `kio-rs/src/net/tcpraw.rs` — `rst` flag parse + filter; `TcpRawConn::close()` (drain / repair-off / FIN); `Takeover` enum + probe/fallback; iptables TTL-DROP path (`setTTL`, rule insert/delete/self-heal, drain thread); accepted-conn deregistration.
- `kio-rs/src/net/mod.rs` — expose takeover knob as needed.
- `kcp-rs/src/conn.rs` — `KcpConn::connect_tcp`, `KcpTcpListener` (+ builder).
- `kio-rs` / `kcp-rs` tests — new Linux-gated suites.
- `docs/superpowers/specs/2026-07-29-TCPRAW_TRANSPORT_DESIGN.md`（已删除） — pointer note that this spec supersedes its TCP_REPAIR-only assumption.

## Non-Goals

- **IPv6**: out of scope (existing tcpraw is IPv4-only).
- **Direct netlink** (no external binary) takeover: out of scope — over-engineering for this project.
- **`nft` command fallback**: not implemented; `iptables` covers the target niche (old kernels ship it).

## Verification Gate

1. `cargo fmt --all -- --check` clean
2. `cargo test --workspace` clean (non-Linux; Linux-gated suites excluded on macOS)
3. `cargo clippy --workspace -- -D warnings` clean
4. Linux container: root-gated suite + `test_e2e.sh --tcp` both directions

## Implementation Record (2026-08-01)

Branch `feat-tcpraw-rst-kcp-rs`, 10 commits on top of `origin/master` (`114ffa70`):

```
3cce660a chore: add libc dev-dep lock + x86_64-linux-gnu linker config
97d3a072 docs: tcpraw RST/takeover + kcp-rs connect_tcp surface; e2e --tcp case
b8c59098 test(kio-rs,kcp-rs): root-gated tcpraw + connect_tcp integration tests
4bbeeee0 feat(kcp-rs): KcpConn::connect_tcp + KcpTcpListener (Linux raw-TCP surface)
3c4a09a7 feat(kio-rs): iptables server path + stale-flow deregistration (tcpraw)
fecc2dc6 feat(kio-rs): Takeover enum + iptables TTL-DROP client path (tcpraw)
ebd89791 feat(kio-rs): graceful close drains RX + repair-off + FIN (tcpraw)
22ce45b4 fix(kcp-rs): reset global SNMP counters in snmp_send_recv_counts_upper_bytes test
0f1528bc fix(kio-rs): restructure tcpraw test packet helper (clippy too_many_arguments)
5215e3b3 feat(kio-rs): filter inbound TCP RST before flow update (tcpraw)
```

**Files touched**: 10 files (+812/-32)
- `kio-rs/src/net/tcpraw.rs` — all transport changes (RST filter, Takeover, iptables, graceful close, deregistration, tests)
- `kio-rs/src/net/tcpraw_stub.rs` — `TcpRawListener::local_addr` stub parity
- `kio-rs/src/net/AGENTS.md` — tcpraw row updated, PENDING LINUX VERIFY note
- `kcp-rs/src/conn.rs` — `DialTransport`, `connect_tcp`, `KcpTcpListener` + builder
- `kcp-rs/src/lib.rs` — public exports for `KcpTcpListener`/`KcpTcpListenerBuilder`
- `kcp-rs/src/kcp.rs` — fix pre-existing SNMP test race
- `kcp-rs/Cargo.toml` — `libc` dev-dep for root-gated tests
- `kcp-rs/AGENTS.md` — `connect_tcp` + `KcpTcpListener` in Key Files
- `kcp-rs/tests/tcpconn_tcp.rs` — 3 integration tests (root+gated)
- `test_e2e.sh` — Linux+root `--tcp` transport case
- `.cargo/config.toml` — `x86_64-unknown-linux-gnu` linker config

**Gates (macOS)**: fmt clean, all tests pass, clippy zero warnings.

**Linux cross-check**: kio-rs and kcp-rs compile clean (per-crate). Workspace check needs `x86_64-linux-gnu-gcc` linker.

**Pending**: Linux container root execution of integration tests (`KCPTCP_ROOT_TEST=1`). All test code is compile-verified and runtime-gated.
