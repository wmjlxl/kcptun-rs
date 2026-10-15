# ANALYSIS — KCP tail-segment stalls under loss: RTO inflation (Go-identical, not a kcp-rs bug)

**Date:** 2026-10-12
**Status:** **CLOSED — mechanism confirmed live with `rx_rto` snapshots and
Go source parity.**
**Scope:** Stalls observed in `corruption_hunt` batches and the formal 1-hour
stability run. **Separate phenomenon from
[[BUGREPORT_KCP_SEGMENT_CONTENT_CORRUPTION]]** (content corruption ≠ timing
stall; that report stays open).

## Observed signature

Exchange-level stalls where the request/response tail is missing, everything
else byte-perfect, self-healing with no reconnect:

| where | heal time | missing bytes at 30 s marker |
|---|---|---|
| hunt p45 `sid=10 seq=257` | 62 855 ms | 5272/8191 (tail 3 segments) |
| hunt p45 `sid=6 seq=387` | 38 282 ms | 0/8191 |
| hunt p45 `sid=0 seq=474` | 37 589 ms | 0/8191 |
| hunt p45 `sid=16 seq=22` | 45 477 ms | 7908/8191 (tail 1) |
| hunt p45 `sid=21 seq=170` | 31 429 ms | 7908/8191 |
| hunt p30 `sid=12 seq=251` | 2 041 ms | 7908/8191 |
| hunt p45 startup ×3 (`seq=0`) | 2 209 ms | accept/build latency, not a stall |
| formal 1 h, 4 events | 60–62 s | (stress harness, no dumps) |

## Evidence chain

1. **Not stochastic loss.** nodelay half-linear backoff (+15 ms/attempt)
   needs ~70 attempts to cover 38 s; P(0.45^70) ≈ 10⁻²². The datagrams were
   not "all being lost" — retransmission slots were **spread seconds apart**.
2. **Dual-end bracket snapshots** (`p1`/`p2` in `TIMEOUT_LATE`):
   the heal window itself shows normal operation (srv `tx+3`, cli
   `rx+2, rcv_nxt+3` with FEC filling the last shard). The silence is
   *before* the first sample; a `wait_send>0` endpoint sits idle for whole
   seconds (`srv tx=2495` flat across `t+1000 → t+2001` while one segment
   was unacked).
3. **Heal times cluster at integer-ish seconds** (2.0, 2.2, 31.4, 37.6,
   38.3, 45.5, 62.9 s) — retransmission *slots*, not attempt storms. The
   62.9 s and the formal run's 60–62 s cluster at `RTO_MAX = 60 000 ms`.
4. **Tail-only selectivity.** Fast-retransmit (`fastack`) needs ACKs whose
   `sn` is *higher* than the missing segment. In strict request/response the
   exchange's last segments have no higher siblings until the echo
   completes, so the tail is **RTO-only**; mid-exchange losses are rescued
   in milliseconds by fastack. This matches the original corruption run's
   asymmetry (burst loop failures, stream loop clean) and explains why the
   stalls always show `filled_at_30s ∈ {0, 5272, 7908}` — 5272/7908 are the
   4-of-7 and 6-of-7 delivery fronts.

## Mechanism

`update_ack` (RFC6298 form) feeds every RTT sample — including samples
measured from *late* retransmissions — into `rx_srtt`/`rx_rttvar`:

```
rto = rx_srtt + max(interval, rx_rttvar << 2)
rx_rto = clamp(rto, rx_minrto, RTO_MAX=60000)
```

Under sustained 30–45 % loss, samples taken from retransmitted segments are
as large as the previous RTO, so `rx_srtt` walks upward: 30 → hundreds of ms
→ ~2 s (matches the 2 s heal cluster mid-session) → tens of seconds near the
60 s cap (matches the formal run). Each session's inflation tracks its own
loss history — `sid=12 seq=251` had 251 exchanges of lossy history and its
first retransmission slot sat at ≈2 s.

## Go parity — this is NOT a kcp-rs bug

kcp-go `master` `update_ack`, verbatim (fetched 2026-10-12):

```go
func (kcp *KCP) update_ack(rtt int32) {
	var rto uint32
	if kcp.rx_srtt == 0 {
		kcp.rx_srtt = rtt
		kcp.rx_rttvar = rtt >> 1
	} else {
		delta := rtt - kcp.rx_srtt
		kcp.rx_srtt += delta >> 3
		if delta < 0 { delta = -delta }
		if rtt < kcp.rx_srtt-kcp.rx_rttvar {
			kcp.rx_rttvar += (delta - kcp.rx_rttvar) >> 5
		} else {
			kcp.rx_rttvar += (delta - kcp.rx_rttvar) >> 2
		}
	}
	rto = uint32(kcp.rx_srtt) + max(kcp.interval, uint32(kcp.rx_rttvar)<<2)
	kcp.rx_rto = min(max(kcp.rx_minrto, rto), IKCP_RTO_MAX) // IKCP_RTO_MAX = 60000
}
```

kcp-rs `update_ack` (kcp.rs) is line-for-line the same computation, same
clamp, same `RTO_MAX = 60000`; the RTT sample source (`current - ack.ts`,
regular packets only, FEC-reconstructed excluded) is mirrored by
`input_no_flush_typed(regular=false)` skipping the RTT update. **A Go kcptun
pair under the same loss profile stalls identically.**

## Consequences

- **Formal 1 h run's `stalls=4 FAIL`** is a loss-induced, algorithm-expected
  condition, not a kcp-rs defect. The strict ≥60 s criterion measures the
  environment's loss + KCP's RTO ceiling; Go would fail it too. Criteria
  recalibration is a user decision — evidence recorded here.
- The **burst vs stream asymmetry** in the stress harness is structural:
  request/response tails are RTO-only, streams always have higher-sibling
  ACKs for fastack. Burst-loop tests under heavy loss will collect these
  stalls; stream loops will not.
- Mitigations (if ever wanted, all behavioural changes needing Go-parity
  review): none required for wire compatibility. Users already tune this via
  `--mode` (nodelay floors `rx_minrto` at 30 ms, interval 10 ms) — the
  inflation itself is in the sample domain, not the timer domain.

## Instrumentation (armed)

`corruption_hunt` `TIMEOUT_LATE`/hard-timeout alerts carry:
`rto/srtt/var` per sample (`state_of`), per-endpoint `tx/rx` datagram
counters, 1 s dual-end sample series from t+0 (baseline) through 15 s then
5 s cadence, `t_ms` aligned to `knet::mono_ms()`. New kcp-rs public
diagnostics: `KcpStream::{tx_pkts, rx_pkts, rx_rto, rx_srtt, rx_rttvar}`.

## Live confirmation (run `20261012_134539`, both predictions hit)

**Event 1 — `sid=30 seq=72`, healed at 6 038 ms.** Server `rto=907`
(`srtt=119 + max(interval, 4×var=788)` — the formula checks out exactly),
client `rto=148`. Server tx counter advances only in slot-sized steps and is
flat in between:

```
t+0     tx=690 wait_send=1
t+1001  tx=697   (initial echo burst)
t+2003  tx=698   (retrans slot)
t+3005  tx=698   flat
t+4006  tx=699   (retrans slot)
t+5007  tx=699   flat
t+6009  tx=699   → heal at 6038 (next slot's send succeeded)
```

Cumulative ladder from `seg.rto = 907` with `+rx_rto/2` growth:
0.9 + 1.4 + 1.8 + 2.3 ≈ 6.4 s ≈ the observed 6.04 s heal.

**Event 2 — `sid=1 seq=40`, healed at 8 743 ms.** Server `rto` evolves
1044 → 1338 → 1207 → 1083 across the stall; the tail segment wedges at
t+5.9 s (`wait_send=1`, `tx=378` flat for 3 s), next ladder slot lands at
≈8.7 s ≈ the observed 8.74 s heal (`tx` 378→379 exactly at p2).

Both events also show the asymmetry directly: **server `rto` ≈ 0.9–1.3 s
while client `rto` stays 0.15–0.23 s** — the echo leg's sends are the ones
being dropped (server-side wrapper, 30–45 %), so the server's RTT variance
inflates and its retransmission slots stretch; the tail segment has no
higher-sibling ACKs to fast-rescue it, so the stall *is* the ladder.

The earlier 37–62 s heals are the same curve further along (`rx_rto`
approaching `RTO_MAX=60 000`), which is also why the formal run's stalls
clustered at 60–62 s.

## Extreme event — the RTO_MAX cap, live (p45 `sid=17 seq=676`, hard timeout)

The full chain from one delayed ACK sample to a 60 s retransmission slot,
sampled every second:

1. t+0…t+20 s — the request's last 4 segments are stuck in *both*
   directions (`cli snd_una=4735` frozen, `srv rcv_nxt=4735` frozen);
   neither side's retransmissions get through.
2. ≈t+25 s — the stuck request finally lands. Its ACK echoes the send
   timestamp from ~25 s earlier ⇒ **one RTT sample of ~25 s** feeds
   `update_ack` with `rx_srtt==0` (this session had only sub-ms samples
   before) ⇒ `srtt=20967, var=12355` ⇒ `rto = 20967 + max(10, 49420) =
   70387 → clamped to 60000`.
3. The echo tail is now missing with `wait_send=1`; the server's only
   rescue is an RTO slot **60 s out**. Series shows exactly one send in the
   next 64 s (`tx 9490→9491` at t+85.8 s) — that attempt is lost (45 %) —
   the following slot would be t+145 s.
4. The client's 90 s hard deadline fires first ⇒ `TIMEOUT` + redial.

This is the formal run's 60–62 s stall signature in full: heals there are
the same event where the 60 s-slot attempt succeeded instead of being lost.

**Follow-on (closed):** a second hard timeout (`sid=14`-type) showed a
server-side echo session vanishing mid-stall (`srv=[gone]`). Root cause:
the listener reaped `is_dead()` sessions — dead_link (20 fast-ladder
retransmits ≈ 11.5 s at `rto=100`) fired on a *recoverable* storm stall and
the sweep/lookup closed a session whose peer was alive. Go never reads the
dead state (`sess.go` has zero references). Fixed by removing `is_dead`
from both listener kill sites —
[[BUGREPORT_LISTENER_DEAD_LINK_SESSION_KILL]].

## Two completed 4-hour hunt runs — closing statistics (2026-10-12)

- p30: **1,340,972 exchanges / 10.98 GB / 0 mismatches / 0 dumps**,
  18,220 self-healing stalls, 3 hard timeouts, 3 redials.
- p45: **402,773 exchanges / 3.30 GB / 0 mismatches / 0 dumps**,
  26,645 self-healing stalls, 2 hard timeouts, 2 redials.
- Every hard-timeout and stall alert carried `send=false rawq=0` on both
  ends — the `is_sending` token matrix ruled out a stuck drainer in every
  observed event (the token-leak hypothesis is dead).
- **Formal 1 h re-run `run_kcp_20261012_223738` under the recalibrated
  criteria (user-approved 150 s thresholds, binaries containing the
  dead-link fix): TOTAL PASS, all six checks green** — verify_errors=0 on
  5,047,380,714 B byte-verified, stalls=0, reconnects=0, conn_errors=0,
  memory stable both ends, no zero-throughput buckets. Previous run under
  60 s thresholds: FAIL (stalls=4, timeouts=396) — same workload, same
  algorithm; only the acceptance threshold changed, as analyzed.
- **Counter-verified stall (run `20261013_002447`, sid=12)**: the
  forensic triple (`flush_ticks` / `kcp_flush_count` / `rto_fires`) advanced
  +5350 / +5352 / +6 over the 90 s stall with `tx` +6 — flush loop alive
  (~11 ms cadence), KCP phase executing, RTO branch firing exactly per the
  inflated ladder (`rto=16764 ms` ⇒ ~6 slots in the window, `rf` == `tx`).
  No suppression, no dead loop, no stuck token — the previously puzzling
  "client tx frozen for 60 s" events are the same phenomenon: retransmission
  slots stretched to 10–20 s by `rx_rto` inflation, so long silences between
  sends are schedule, not failure. A genuine anomaly would now show
  `ft`/`kf`/`rf` flat while `snd_nxt - snd_una > 0`.

- Hard timeouts decompose into the same ladder math: inflated `rx_rto`
  base (0.9–7.4 s observed) × half-linear backoff
  (`seg.rto += rx_rto/2`, Go-identical) stretches retransmission slots
  until tail-segment recovery exceeds the 90 s harness deadline; server
  `tx` deltas over stall windows match the predicted slot times exactly
  (e.g. rto=2360 ⇒ exactly 4 sends in the 30–90 s window).
