# BUG: Cancelled/broadcast lost-wakeup — waiter parks forever on an already-cancelled token (found & fixed)

**Date:** 2026-10-11
**Found by:** `knet-rs` test `sync::cancel::tests::cancel_wakes_every_parked_waiter`
flaking ~10–20% per run (solo, no parallel interference), captured with
state diagnostics.
**Severity:** High — the same window in production lets a `close()` broadcast
fail to wake a parked recv loop: the connection's task stays parked on a
token that already reads `cancelled=true`, i.e. shutdown stalls until some
unrelated wakeup (mirrors the historical "input loop never exits" failure
class).
**Status:** **Fixed** (same day) — `knet-rs/src/sync/cancel.rs::Cancelled::poll`.

## Observed failure state

```
waiter did not complete — waiter 1: cancelled=true
flags=[true, false, false, false]   notify=(permits=0, waiter_count=0, list_len=3)
```

`cancelled=true` yet3 of 4 waiters never completed; permit consumed, waiters
still listed (the `(0,0,3)` triple is read non-atomically — it proves
registrations landed *after* the broadcast's take-all).

## Root cause — single shared backstop permit + early-return race

`Notify::notify_waiters` (the broadcast behind `CancellationToken::cancel`):

```rust
permits.store(1);                    // ONE shared backstop permit
if waiter_count.load() == 0 { return; }   // early-return: nobody registered
{ lock; waiter_count.store(0); take(waiters); }  // wake all taken
```

Interleaving (each arrow is SeqCst-ordered within its task):

1. Waiter0 re-polls: `Cancelled::poll` checks `is_cancelled` → **false**
   (the flag store hasn't landed yet), enters `notified.poll`, consumes the
   backstop permit via the fast path **just after `cancel()` stored it** →
   waiter0 deregisters and completes (its `flags[0]=true`).
2. `cancel()`'s `waiter_count` read then sees **0** (waiter0 deregistered,
   waiters1–3 not yet inserted) → **early-return, no wakes issued**.
3. Waiters1–3 insert their slots afterwards; both of their permit checks
   (fast path + post-registration re-read) see `0` (waiter0 ate it) →
   `Poll::Pending` forever. Nothing will ever wake them — the token is
   already cancelled, so no further broadcast comes.

The `Notify` registration protocol itself is sound (lock-published slot +
post-lock permit re-read, all SeqCst — verified by exhaustive interleaving
analysis); the hole is that **the terminal *condition* (the cancel flag) was
only checked before registration**, while the *permit* — which can be stolen
by an unrelated fast-path consumer — was the only thing checked after it.

## Fix

`Cancelled::poll` now re-checks `is_cancelled()` **after** the `notified`
registration attempt and returns `Ready` if it is set — the terminal flag is
the source of truth, the permit is only a wake-up optimization. The pending
`NotifyFuture` deregisters itself via `Drop`. This closes the window
regardless of who consumed the permit:

```rust
if this.token.is_cancelled() { return Ready(()); }   // pre-check
if notified.poll(cx).is_ready() { return Ready(()); }
if this.token.is_cancelled() { return Ready(()); }   // post-registration re-check  ← fix
Pending
```

`notify_waiters` docs now state the contract explicitly for plain-`Notify`
callers: gate on your own terminal flag after registering; do not rely on
the single permit (a fast-path re-poller can consume it).

## Secondary fix in the same run: `test_inflight_guard_tracks_and_drains` isolation

`inflight::COUNT` is process-global while the harness runs tests in
parallel; `test_inflight_wait_drain_times_out` holds a guard for its whole
~50 ms body, racing the sibling test's absolute `active() == 0` assertions
(observed `left:1 right:0`, 2/3 failures under load). Both inflight tests
now take a shared `static tokio::sync::Mutex` for their full body
(`INFLIGHT_TEST_LOCK` — an async mutex never deadlocks when a sibling test
is filtered out). This is a **test-isolation** fix; the counter itself was
correct.

## Regression evidence

- `cancel_wakes_every_parked_waiter`: **30/30 passes** after the fix
  (was ~10–20% flake; pre-fix diagnostics captured in this report).
- `knet-rs --lib` full suite: **5/5 runs green** (both fixes in).
- Failure-site diagnostics kept in the test (`notify=(permits, count, list)`)
  so any future recurrence distinguishes lost-wake from budget starvation.
- `cargo fmt --check` and `make clippy -D warnings` clean.

## Environment

macOS, rustc 1.99.0-nightly, tokio multi-thread global runtime
(`knet::block_on`). No wire/behaviour change: sync primitive + docs + tests
only.
