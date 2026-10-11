# P99 test step 7 (go→kcp-rs smol server) intermittent hang (open)

> **状态**: **Obsolete**（2026-10-11 复核）— 该 bug 仅存在于 smol 后端
> （`latency_p99_smol` / go→kcp-rs smol server 步骤）。仓库已切换为
> **tokio-only** 后端（AGENTS.md "Single async backend: tokio"），smol 后端
> 及其测试目标已整体移除，报告中的触发路径不复存在；报告中同时记录
> tokio 路径 ~25 次从未失败。


## Status

**Open** — reproduced **once** (initial repro), then **not reproduced in 100 subsequent full-script
hunts (2026-08-01)**. Protocol-level mechanism confirmed; the exact server-side race line is not
yet pinned. Estimated rate ≈ 1 / ~117 full runs (~0.9%; one-sided 95% CI upper bound ≈ 2.5% for
0/100).

Related: `BUGREPORT_KCPCONN_INPUT_LOOP_LEAK.md` — confirmed socket leak in the same connection
path, **now fixed** (input-loop `recv` bounded by a 100ms timeout). That fix addresses closed-session
resource retention, not the live-connection ACK stall this hang is about.

## Symptom

`bench/run_p99.sh` prints `==> [7/7] kcp-go → kcp-rs(smol) (cross)` and then **never finishes**.
Both processes stay alive for minutes, each ~20% CPU, retransmitting:

```
kcp-go-latency client --addr 127.0.0.1:<port> --rps 200 --warmup 1 --duration 2 ...
latency_p99_smol --mode server --port <port> --size 1024
```

Reproduced with `RPS=200 WARMUP=1 DURATION=2` (short durations make it far more likely). Step 5
(go→kcp-rs **tokio** server) has never failed across ~25 runs; standalone go→smol passed 20/20.
So the trigger is the full-script context + the smol server.

## Mechanism (confirmed at the KCP protocol level)

1. If the smol server's echo/ACK path stalls, the Go client stops receiving ACKs.
2. kcp-go `WriteBuffers` (sess.go) only sends while `waitsnd < snd_wnd && waitsnd < rmt_wnd`.
   With no ACKs, `snd_buf` fills to `snd_wnd` (512) in ~2.5s at RPS=200 → `Write` **blocks forever**
   on `chWriteEvent` (no write deadline). The client can never reach `measure_end`.
3. The `updater` goroutine only re-fires `chWriteEvent` when `waitsnd < rmt_wnd` again; if the
   server advertised `wnd=0` (its `rcv_queue` filled to `rcv_wnd`), `rmt_wnd` stays 0 → **permanent
   deadlock** even if the server later recovers.
4. Both sides spin: server flush loop retransmits at `next_update=1` ms, client updater+readLoop
   process the retransmits.

Short durations (`DURATION=2`) hang because the window fills (2.56s) before `measure_end` (3s).
With `DURATION=60` the client has time to recover if the stall is transient — only a *permanent*
server-side break hangs the default run.

## Root-cause area

`kcp-rs` smol server, per-accepted-connection path. `KcpConn` internal locks were audited: no
AB-BA lock deadlock; notifies all have 2ms-poll fallbacks. The race is most likely in the smol
runtime's scheduling of the accepted-conn input / echo tasks (or the listener reader task) such that
the connection stops ACKing — not a mutex deadlock. Needs a stack (`sample`) of the hung processes
to pin down.

## Script mitigation (applied)

`bench/run_p99.sh` now wraps every measurement in a per-step SIGALRM timeout
(`STEP_TIMEOUT = WARMUP + DURATION + 30s`), so a stuck combo is reported as FAILED and the script
always terminates:

```bash
step_tmo() { perl -e 'alarm shift; exec @ARGV' "$STEP_TIMEOUT" "$@" 2>/dev/null | grep '^RESULT' || true; }
```

## Repro

```bash
RPS=200 WARMUP=1 DURATION=2 bash bench/run_p99.sh   # ~1/25 hangs at step 7
```

## Next steps

- Catch a hang with a `kcp-go-latency client` alive >12s watchdog, then `sample` both processes to
  identify the stalled server task.

## 100-run hunt log (2026-08-01)

Tried to reproduce with short params (`RPS=200 WARMUP=1 DURATION=2`), each run = the full 7-step
`bench/run_p99.sh`, a hung client watchdog (`kcp-go-latency client` alive >12s → `sample` both
sides), and a script-level `FAILED` check as the definitive signal:

| Hunt | Runs | Genuine hangs | Notes |
|------|------|---------------|-------|
| v1 (loose pgrep) | 56 | 0 | 4 false-positive detections — `pgrep -f "kcp-go-latency client"` matched a shell wrapper whose cmdline embedded the pattern (Claude Code tool shell). All scripts completed 7/7. |
| v2 (anchored pgrep) | 44 | 0 | 1 sampling event (a real kcp-go client alive >12s, idle in the Go runtime; script still completed 7/7 → not a genuine hang). |

Total: **100 full runs, 0 genuine hangs** since the initial reproduction.

Detector gotcha for future hunts: `pgrep -f "<binary> client"` matches any process whose *full
command line* contains the substring — including long-lived shell wrappers. Anchor it:
`pgrep -f '^[^ ]*/kcp-go-latency client'` (cmdline must start with the binary path), and verify
`ps -o comm=` is the Go binary. A script-level `FAILED: no RESULT within ...` line is the reliable
hang signal (the per-step timeout fires only when a measurement truly stalls).
