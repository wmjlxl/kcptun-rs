# BUG: `KcpListener::stats()` ABBA deadlock against the build-publish path (found & fixed)

**Date:** 2026-10-11
**Found by:** `session_cap_test` hanging >60s (repeatedly) during workspace
test runs on a loaded machine; isolated runs passed (low-probability window).
**Severity:** High (permanent hang of any thread calling `stats()` — including
production `kcptun-server`'s periodic stats logging — while a session build
is publishing; the test loop hangs forever, production would wedge the caller).
**Status:** **Fixed** (same day) — `kcp-rs/src/sharded.rs::stats`.

## Symptom

`cargo test -p kcp-rs --test session_cap_test` hangs indefinitely under CPU
load (two workspace runs reproduced, 21 min and >6 min before kill; two
isolated runs passed in 1.2s). The test's bounded deadline loop
(`BUILD_HOLD_MS + 800`) can never expire because the future never yields:
`listener.stats()` is parked on a mutex.

## Root cause — classic ABBA lock-order inversion

Both locks: `sessions: Mutex<HashMap<SocketAddr, Session>>` and
`building: Mutex<HashMap<SocketAddr, (Arc<PeerQueue>, Instant, u64)>>`.

**Path 1 — `stats()` (sharded.rs:446-449), order `sessions → building`:**

```rust
WorkerPoolStats {
    sessions: self.sessions.lock().len(),   // guard A held…
    building: self.building.lock().len(),   // …while waiting for guard B
    ...
}
```

Rust drops struct-literal temporaries only at the **end of the whole
statement**, so `sessions` was held across the `building.lock()` call.

**Path 2 — `spawn_build_task` publish (sharded.rs:1133-1144), order
`building → sessions`** (deliberate — the code documents "Hold the building
lock across the insert so the reaper cannot drop the slot in between", and
`admit_new_session`/`register_and_spawn_build` carry the same comment
"Lock order is `building` then `sessions`"):

```rust
let mut b = building.lock();
...
let mut live = sessions.lock();   // waits while stats() holds it
```

Interleaving: build task takes `building`, `stats()` takes `sessions`,
build task blocks on `sessions`, `stats()` blocks on `building` → **deadlock**.

## Evidence (lldb attach to the hung test process)

```
thread #2 (test main):
  session_cap_test.rs:79 → KcpListener::stats (sharded.rs:449)
  → Mutex<…(Arc<PeerQueue>, Instant, u64)…>::lock   ← waiting for `building`
  → parking_lot lock_slow → __psynch_cvwait   (forever)

thread #3 (tokio-rt-worker):
  spawn_build_task closure (sharded.rs:1144)
  → Mutex<…Session…>::lock                       ← waiting for `sessions`
  → parking_lot lock_slow → (parked)
```

The other dual-lock sites were audited in the same pass:
`admit_new_session` (954-955) and `register_and_spawn_build` (1060-1061)
both take `building → sessions` (matching the documented order, nested in one
scope); `sweep` takes them in separate non-nested scopes. **`stats()` was the
only inverter.**

## Fix

```rust
let sessions = self.sessions.lock().len(); // guard dropped at statement end
let building = self.building.lock().len(); // never held together
WorkerPoolStats { sessions, building, ... }
```

Each guard now dies at its own semicolon, so `stats()` never holds one lock
while waiting for the other; the publish path's `building → sessions` order
can no longer invert against it.

## Regression coverage

- `session_cap_test` (the reproducer) — passes standalone ×2 and in full
  workspace runs after the fix; under the same load profile that previously
  deadlocked it.
- No API/behaviour change: `stats()` still returns the same snapshot values
  (the two `len()` reads were never atomic-consistent anyway; per-field
  consistency is unchanged).

## Lesson for this codebase

`X.lock().field` inside a **struct literal / array / tuple expression**
extends the guard's lifetime to the end of the full statement — a silent
lock-hold extension. Any `Mutex::…` expression inside a composite literal
must be checked for cross-lock nesting. Consider a lint (clippy `held_lock`)
if it can be enabled for this workspace.
