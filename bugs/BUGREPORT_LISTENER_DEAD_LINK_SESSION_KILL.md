# BUG: Listener reaps `is_dead()` sessions — dead_link under a loss storm kills recoverable sessions (Go never reads the state)

**Date:** 2026-10-12
**Status:** **Fixed** (2026-10-12, this change) — `kcp-rs/src/sharded.rs`
`sweep()` + `lookup_established()`
**Found by:** `corruption_hunt` run `20261012_144631` (p45), live event with
full dual-end instrumentation
**Severity:** Medium — turns slow-but-healing stalls into hard session death
while the peer is alive; production layer has a related, separately-triaged
exposure (below)

## Observed event

```
ECHO_ERR peer=127.0.0.1:65216 kind=eof echoes=2910 partial=0
  state=dead=true closed=true ... wait_send=5 rx_age=11386ms err=[none] t_ms=2396130
TIMEOUT sid=7 seq=2909 local=127.0.0.1:65216   (client hard timeout, AFTER the EOF)
```

Timeline (all numbers from the alert series):

1. Exchange tail segments stall (loss storm, both directions).
2. Server retransmits climb the fast ladder (`rto=100`, half-linear
   `+rx_rto/2`): 20 unacked retransmits ≈ **11.5 s** ⇒ `xmit ≥ dead_link`
   ⇒ `state = DEAD_LINK_STATE`.
3. The listener's 1 s `sweep()` (or the next inbound lookup) sees
   `is_dead()` ⇒ **closes and removes the session**.
4. The client was alive the whole time; sibling sessions with identical
   stalls self-healed at 30–90 s (`late_heals` counters) — this session
   would have too. After the kill, no retransmissions can exist, so the
   client's pending exchange is guaranteed to hit its 90 s hard timeout.

Arithmetic check: `rx_age = 11386 ms` at EOF ≈ the 20th-ladder-slot time —
the session was reaped essentially the moment dead_link fired.

## Why this is wrong (Go parity)

- kcp-go `kcp.go` sets `state = -1` on `xmit >= dead_link` (identical
  constant, identical counter — kcp-rs mirrors it).
- kcp-go `sess.go`: **zero references to `state` / `State()` / dead** —
  nothing closes, errors, or fails a session because KCP declared the link
  dead. The state is purely informational; users may poll it.
- kcp-rs had TWO consumers of the state that Go does not have:
  1. `sweep()` — `is_dead() || is_closed()` ⇒ reap (every 1 s);
  2. `lookup_established()` — `!is_dead()` guard ⇒ next inbound datagram
     from the peer *removes* the session (`Established::Reaped`).
- The KCP core itself correctly keeps retransmitting after dead (no state
  guard in `flush_with_current`, matching Go) — only the listener layer
  diverged.

## Why dead_link fires on healthy-but-slow links

dead_link = 20 unacked transmissions of one segment. With an uninflated
fast-ladder `rto≈100 ms` that is ~11.5 s of non-delivery — trivially
reachable in a 30–45 % loss storm, with or without the peer being gone.
(RTO-inflated sessions, e.g. WAN `rto` in the seconds-to-60 s range, take
far *longer* to reach 20 xmits — which is why the formal 1 h WAN run never
triggered it and showed 0 reconnects; localhost hunts with many
`srtt=0, rto=100` sessions trigger it in minutes.)

## Fix

- `sweep()`: reap on `is_closed()` or idle only. A truly vanished peer is
  still covered: no inbound + no successful write ⇒ `idle_timeout`
  (default 600 s) reaps it, and the production stack additionally has
  SMUX keepalive liveness.
- `lookup_established()`: `!is_closed()` ⇒ `Live`. Dead sessions keep
  receiving inline feeds so a recovering link actually recovers the
  session (Go behaviour).

## Related exposure NOT changed here (documented for triage)

`kcptun-common/src/kcptun_session.rs:831` liveness uses
`kcp.is_dead() || smux.is_keepalive_timeout()` to declare a production
session dead. That is a documented kcp-rs liveness rule and the formal 1 h
run passed with 0 reconnects (WAN `rto` inflation keeps xmit counts below
20), but under low-RTT + heavy-loss conditions it can fire the same way.
Changing the production liveness rule needs its own evidence run — flagging,
not silently altering.

**User decision (2026-10-12): without evidence of production harm, do NOT
change it.** ("如果不能找到证据不该") — keep `kcp.is_dead()` in the
kcptun-session liveness rule as-is; if a future run ever shows a production
session killed by dead_link on a recoverable link (reconnect events
correlating with `kcp.is_dead()` in session-state logs), reopen with that
evidence.

## Tests

- **Regression test** `listener_serves_dead_link_sessions`
  (`kcp-rs/tests/kcpstream_listener.rs`): marks an accepted session
  `dead_link` via the `testing_mark_dead` seam, asserts (a) the 1 s sweep
  leaves it open after 2.5 s, and (b) a fresh inbound exchange still
  round-trips both directions with `session_count()==1`. **Mutation-verified**
  (2026-10-12): re-adding `|| conn.is_dead()` to `sweep()` and
  `&& !…is_dead()` to `lookup_established()` makes the test FAIL (line 822),
  removing them again makes it PASS — the test covers exactly this fix.
- `cargo test -p kcp-rs --features async` — all suites green (no prior test
  depended on dead-reap; `data_correctness` asserts the opposite:
  `!is_dead()` on healthy transfers).
- Live validation: corruption hunts re-run on the fixed binary; a repeat of
  the sid=7 pattern should now show the session surviving (dead=true in
  snapshots, no EOF, later `TIMEOUT_LATE` healing or an eventual idle reap).

See also: [[ANALYSIS_KCP_RTO_INFLATION_TAIL_STALLS]] (the stall family this
kill interrupted).
