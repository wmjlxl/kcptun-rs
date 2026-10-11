# BUG: KcpListener stale-session aliasing on UDP port reuse corrupts/short-circuits new connections

**Date:** 2026-10-11
**Found by:** 1-hour / 20-client pure kcp-rs stability run
(`stability_1h/run_kcp_20261011_163143`)
**Severity:** Medium–High (data-integrity failure; requires rapid
connect/disconnect churn + ephemeral-port reuse within the server idle window)

## Summary

`kcp_rs::KcpListener` demultiplexes sessions **by source address only**.
When a client rapidly creates and destroys `KcpStream`s (one fresh ephemeral
UDP socket per `KcpStream::connect`), the OS reuses a port that the server
still associates with a *lingering* session (client `close()` puts nothing on
the wire, so the server session lives until its idle timeout — 15 s in this
run). The new connection's datagrams are then fed into the old session's KCP
state machine instead of building a fresh session.

The listener's restart detection (`sn=0/una=0` restart with **`rcv_nxt ≥ 16`**,
see `sharded.rs` / AGENTS "Stale-session eviction") does not fire for sessions
that received fewer than 16 segments — i.e. **short exchanges** — so the
aliasing stands and produces two failure modes:

1. **Wrong-bytes delivery (integrity failure).** The old session still holds
   un-ACKed echo segments (its peer closed without the final ACK landing);
   RTO retransmits go to the reused port and are read by the *new*
   `KcpStream` as if they were its own echo. `first_diff=0`, payload entirely
   wrong.
2. **Timeout.** The new connection's `sn=0` segments are below the old
   session's `rcv_nxt` → treated as duplicates and dropped → the echo never
   comes back → client read deadline fires.

## Evidence (1 h run, 20 clients, burst loop = fresh `KcpStream` per exchange)

| metric | value |
|---|---|
| total verified | 4,920,551,318 B (20/20 clients finished) |
| **byte-verification failures** | **22** |
| failures in the **stream loop** (one long-lived conn per client) | **0 / 43,047 blocks** |
| failures in the **burst loop** (fresh conn per exchange) | 22 / 28,765 |
| `first_diff == 0` | 22 / 22 (payload wrong from byte 0) |
| failed payload sizes | 1 B ×4, 64 B ×6, 1349 B ×5, 1350 B ×2, 1351 B ×1, 1400 B ×3, 8191 B ×1 — **21/22 ≤ 1400 B** |
| server accepts (full run) | 29,028 sessions, all reaped to `sessions=0` at end |
| KCP-level counters | `KCPInErrors=0`, `FECErrs=0` throughout |

Why the size skew is the signature of the eviction gate:

* A stale session built from a **small** exchange has `rcv_nxt < 16` →
  restart eviction never fires → aliasing happens (all observed failures).
* A stale session built from a **large** exchange (≥16 segments) is evicted
  cleanly on the next `sn=0/una=0` burst → no failure. Hence **zero**
  failures at ≥64 KiB payloads even though they churn through the same code.
* The stream loop never fails because its port is stable for the whole hour —
  no reuse, no aliasing.

Rough rate check: ephemeral range 16 384 ports, ~6.3 burst conns/s host-wide,
server sessions lingering 15 s ⇒ ~95 "hot" ports ⇒ ~0.6 % collision chance
per new conn; most collisions are cleaned by the `rcv_nxt ≥ 16` eviction or
surface as timeouts; 22 visible wrong-bytes events / 28 765 exchanges
(0.076 %) sits in the expected band.

## Why this is a kcp-rs robustness gap (not just a harness artifact)

* Creating a fresh `KcpStream` per exchange is legitimate API usage; the API
  gives the caller no way to pin the local port or signal the peer on close.
* `SharedIoState::close()` only cancels local tasks — **nothing is written to
  the wire**, so the server cannot distinguish "peer gone" from "peer
  restarting from the same address".
* All sessions default to `DEFAULT_CONV`, so the conv-mismatch eviction path
  cannot catch it either (same-conv aliasing is invisible to it).
* The restart-eviction machinery exists precisely for this scenario — it is
  simply gated too high (`rcv_nxt ≥ 16`) for short-lived sessions.

Real-world kcptun usage holds sessions for hours (`--conn` pool, SMUX), so
the trigger is rare in production; it becomes relevant for any
churn-heavy pattern (dial-per-request tunnels, tests, reconnect loops).

## Reproduction

```bash
cargo build --release -p kcp-rs --features async \
    --example stress_server --example stress_client
./stress_server --listen 127.0.0.1:29901 --idle 15 &
# burst loop (fresh KcpStream per exchange) + long-lived stream loop;
# byte-exact echo verification, splitmix64-seeded payloads
./stress_client --id 1 --addr 127.0.0.1:29901 \
    --duration 600 --logdir /tmp/repro
grep -c verify_fail /tmp/repro/worker_1.jsonl
```

On a lossless loopback the collision window still exists (see the run's local
smoke: 0 failures in 10 s is consistent with the low base rate) — the 1 h WAN
run at ~29 k sessions is the reliable reproducer. Speed-up knob: shrink the
server `--idle` window less (larger idle ⇒ more lingering sessions ⇒ higher
collision rate) or shrink the client's ephemeral range
(`net.inet.ip.portrange.first/last` on the client host).

## Go kcp-go comparison (decisive — done, 2026-10-11)

kcp-go v5.6.8 `sess.go` handles the same scenario **correctly**, via two
mechanisms kcp-rs lacks on the dial side:

```go
// DialWithOptions (sess.go:1050): a FRESH RANDOM conv per dial
var convid uint32
binary.Read(rand.Reader, binary.LittleEndian, &convid)

// Listener.packetInput (sess.go:829): same-addr + different conv + sn=0
if !convRecovered || conv == s.kcp.conv {
    s.kcpInput(data)              // conv matches → feed existing session
} else if sn == 0 {               // conv differs → replace IMMEDIATELY
    s.Close(); s = nil            // → fresh session built below
}
```

| | Go kcp-go | kcp-rs |
|---|---|---|
| conv on dial | **random per dial** | **fixed `0xDEAD_BEEF`** (`config.rs:16`) |
| same-addr + different conv | immediate replace (first sn=0 datagram) | conv-mismatch streak + span eviction (works, slower) |
| same-addr + **same** conv | never happens in practice (conv always differs) | restart detection — but gated `rcv_nxt ≥ RESTART_MIN_RCV_NXT` (`conn/endpoint.rs:1244`), **blind to short stale sessions** → observed corruption |

So Go avoids the aliasing by *construction* (random conv ⇒ the replace path
always fires), while kcp-rs pins the conv and relies on its own same-conv
restart machinery, whose `rcv_nxt ≥ 16` gate leaves exactly the hole this run
hit (stale sessions built from payloads < 16 segments). The `streak`/`span`
guards in `evict_stale` show the scenario was considered — the rcv_nxt gate
is the incomplete part.

Exposure beyond this harness: a **kcp-rs client (fixed conv) → Go server**
re-dial on a reused port also aliases on the Go side (Go's replace needs a
conv mismatch). And kcp-rs production churn paths (`--autoexpire` /
scavenger re-dials) hit the same window whenever an old session's socket has
been closed but the server-side session still lingers.

**Coupling audit for the fix:** `CryptoBuf`'s `session_id` seed is a *local
constant* (`kcptun-common/kcp_transport.rs:54,112` — `CryptoBuf::new(DEFAULT_CONV
as u64)`), independent of the wire conv field; `gate_new_session` performs
only the decrypt gate and does **not** check conv (the listener learns conv
from the first datagram, exactly like Go). Randomizing the dial conv
therefore touches neither the crypto layer nor the wire format.

## Suggested fixes (ranked, post Go-comparison)

1. **Random conv on dial — Go-parity root fix (recommended).** Make
   `KcpStream::connect` default to a fresh random conv per dial, mirroring
   kcp-go's `Dial` (`binary.Read(rand.Reader, …)`); keep `.conv()` as the
   explicit override. Effects: (a) the receiver rejects foreign-conv
   datagrams outright (`ConvMismatch`) so **wrong-bytes delivery becomes
   impossible**; (b) the existing conv-mismatch eviction path (streak + span,
   no rcv_nxt gate) rebuilds the session, so the timeout mode recovers the
   same way Go does — Go replaces on the first sn=0 mismatch. Verified
   non-goals: `gate_new_session` does not check conv (listener learns it from
   the first datagram, same as Go); `CryptoBuf`'s session_id seed is a local
   constant, not the wire conv; no wire-format change. This also removes the
   kcp-rs-client → Go-server aliasing exposure and covers kcptun churn paths
   (`--autoexpire` / scavenger re-dials) because they share the same builder.
2. **Close the restart-gate hole (defense in depth for fixed-conv dials).**
   Replace `rcv_nxt ≥ RESTART_MIN_RCV_NXT` in
   `conn/endpoint.rs::input_with_optional_conv` with a liveness judgement:
   count `sn=0/una=0` as a restart when the stale session has shown no
   input/flush progress for a grace period. The existing consecutive-burst
   streak + `EVICT_MIN_STALE_SPAN_MS` window already provide two independent
   guards against evicting a live session whose ACKs are being lost.
3. **Match Go's immediacy for conv-mismatch replaces (optional).** Go
   replaces on the first `conv≠, sn=0` datagram; kcp-rs waits for the streak
   + span thresholds, so recovery takes seconds of retransmit cycling. A fast
   path for `conv mismatch && sn==0` would close that latency gap (behaviour
   parity, not wire parity — eviction timing is receiver-local).
4. **Expose local-port pinning** on `KcpStream::connect` (bind to an explicit
   ephemeral port range) so callers that churn connections can avoid immediate
   reuse; document the hazard either way.

Pre-implementation audit (from this run): check callers/tests that rely on a
fixed dial conv (`adopt_conv` interaction, explicit `.conv()` users in
kcptun-common); kcp-rs is a published crate — default-conv semantics changing
belongs in the changelog. Validate by re-running
`stability_1h/run_kcp_test.sh` (harness ready): target `verify_errors = 0`
over the same 20 × 1 h workload.

Note for triage: compare against Go kcp-go's listener behaviour for the same
scenario before choosing (1) — wire/behaviour compatibility is the hard
constraint in this repo.

## Validation after the fix (2026-10-11)

Fix landed: random conv on dial (`kcp-rs/src/config.rs::random_conv`,
`conn.rs::build()` gate + `conv_setter!` pin flag, kcptun-common UDP-dial
opt-in in `kcp_transport.rs`). Full gates green: `cargo test --workspace`,
`make clippy -D warnings`, `cargo fmt --check`.

Re-ran the identical 20 × 1 h workload (`run_kcp_20261011_185634`):

| metric | before | after |
|---|---|---|
| alias-signature failures (`first_diff=0`) | **22** | **0** |
| total `verify_errors` | 22 | 1 (different defect → [[BUGREPORT_KCP_SEGMENT_CONTENT_CORRUPTION]]) |
| burst-loop timeouts (aliasing symptom) | 226 | 35 |
| all timeouts | 279 | 85 |
| sent / verified | 4.92 GB | 5.16 GB |
| stalls / conn_errors / KCPInErrors / FECErrs | 0/0/0/0 | 0/0/0/0 |

The failure mode this report describes is **gone**: every remaining failure
has a different fingerprint (segment-boundary `first_diff`, not whole-payload
foreign data).

## Environment

- Server: `stress_server` (kcp-rs example), musl static, remote Linux host
  (kernel 2.6.32-era OpenVZ, glibc 2.17), `--idle 15 --sndwnd 512 --rcvwnd 512
  --mode fast --datashard 10 --parityshard 3`.
- Clients: 20 × `stress_client` on macOS, 256 KiB/s each, 3600 s.
- Path: real WAN (cellular), 40.2 % segment retransmission rate — loss
  amplifies the timeout mode but is **not required** for the aliasing itself.
- Harness: `stability_1h/run_kcp_test.sh`.
