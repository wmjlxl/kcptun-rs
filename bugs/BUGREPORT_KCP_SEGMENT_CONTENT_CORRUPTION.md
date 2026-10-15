# BUG: Single segment-boundary-aligned byte corruption in raw (unencrypted) kcp-rs data path

**Date:** 2026-10-11
**Found by:** 1-hour / 20-client stability rerun after the conv-randomisation
fix (`stability_1h/run_kcp_20261011_185634`)
**Severity:** Medium (integrity failure; 1 occurrence in 5.16 GB — distinct
from [[BUGREPORT_KCP_LISTENER_STALE_SESSION_ALIASING]], whose signature is
fully absent from this run)

## Summary

One byte-verification failure out of 75,242 exchanges, with a signature that
cannot be produced by session aliasing and lands **exactly on a KCP segment
boundary**:

```json
{"ev":"verify_fail","worker":13,"sample":{
  "kind":"mismatch","sent_len":8191,"recv_len":8191,
  "first_diff":5272,"seq":1002,"size":8191,"loop":"burst"}}
```

- `first_diff = 5272 = 4 × 1318`, and `1318 = mtu 1350 − FEC 8 − KCP hdr 24`
  is precisely the FEC-adjusted KCP segment data size for this
  configuration (the client's first 4 segments are byte-perfect; the
  corruption begins at the first byte of segment 5).
- Lengths are equal (`recv_exact` filled all 8191 bytes) — content, not
  truncation.
- Alias signature (`first_diff=0`, whole payload foreign) occurred **0 times**
  in this run (22 times before the conv fix) — this is a different defect.
- Server counters stayed clean: `FECErrs=0`, `KCPInErrors=0`,
  `InCsumErrors=0`.
- Loop split: burst 1 failure / 29,927 exchanges; stream 0 / 45,315 (weak
  discriminator — see hypotheses).
- Path context: 41.2 % segment retransmission (congested WAN + harness
  without `--sockbuf`), FEC 10/3 active, **no crypto layer** (this stress
  stack is deliberately raw — no CRC32/AEAD covering the wire or buffers).

## Hypotheses (ranked)

1. **In-memory segment/buffer lifecycle race under retransmit churn**
   (`SegmentPool` / `read_buf` / shared buffer pools): a recycled buffer's
   contents are overwritten by another session between queue and read, which
   would replace a whole segment's payload — matching the exact-boundary
   `first_diff`. The 41 % retransmit load maximises pool turnover.
2. **Silent FEC mis-reconstruction**: Reed-Solomon decode "succeeds"
   (`FECErrs=0`) but emits wrong content for one shard (shard pairing /
   length handling under loss). Reconstruction ran 119,621 times this run.
3. **Post-validation kernel/memory corruption** (weakest): UDP checksums
   should drop wire bit-flips before userspace; corruption entering after
   validation would not be caught — and this raw stack has no second
   integrity layer (production kcptun adds CRC32/AEAD over the wire, which
   covers on-wire damage but *not* hypotheses 1–2 inside kcp-rs).

Not localised to a side: the harness does not retain the received buffer, so
client-egress vs server-ingress cannot be distinguished from this run's logs.

## Reproduction plan (for whoever picks this up)

**Implemented** as `kcp-rs/examples/corruption_hunt.rs` (2026-10-12): an
in-process `KcpListener` + N client `KcpStream`s over localhost UDP with
Bernoulli drop-on-send injected on *both* legs (client wrapper + listener
`transport_wrapper`), strict 8191-byte request/response framing, and
byte-exact verification of every exchange. On mismatch it dumps
`hunt_sid{N}_seq{N}.sent.bin/.got.bin` and prints the per-leg localisation
the original run lacked — the echo task FNVs exactly what it read for the
failing exchange (`server_fnv`):

- `server_fnv != expected_fnv` → client→server leg owns the corruption;
- `server_fnv == expected_fnv`, `got != expected` → server→client leg.

The generic tooling below still applies to any WAN rerun:

The repo already has the tool to localise the side: `examples/fec_loss.rs`
ships a `LossyTransport` that drops datagrams at a configurable rate while
keeping byte-exact verification. Combine it with a byte-exact echo and
capture at both ends:

1. Loopback pair with `LossyTransport` at 30–40 % loss (matching the run's
   retransmit profile) + FEC 10/3, run a burst+stream workload identical to
   `stress_client`;
2. Hex-dump every **write** on side A (pre-send) and every **read** on side B
   (post-recv) for the failing exchange — the first side whose dump diverges
   from the sender's bytes owns the bug;
3. If side B's *socket* bytes are already wrong → wire/kernel (hypothesis 3);
   if socket bytes are right but the app read is wrong → kcp-rs buffer path
   (1); if side A's own KCP segments are wrong before send → FEC encode /
   snd_buf path (2);
4. Repeat under `cargo test` with the pool/scavenge paths under contention
   (20+ sessions in one process).

A cheap additional probe: run the same workload **with crypto enabled**
(e.g. `aes-gcm`, which tags every datagram). If failures vanish while loss
stays equal, hypothesis 3 gains; if they persist, the corruption is inside
kcp-rs (1 or 2).

## Cumulative hunt statistics (2026-10-12, all with dump instrumentation armed)

| batch | conditions | exchanges | captures |
|---|---|---|---|
| original run (`run_kcp_20261011_185634`) | WAN, 20 clients ×1h, 41.2% retrans | ~75k | **1** (the report's event) |
| local loopback + 25%/dir loss proxy | 6 clients × 6×900s | ~3.8k | 0 |
| WAN batch1 | 20 clients × 3×1h (1st round clipped), timeouts 131–161/h vs original 85/h | ~157k | 0 |
| formal 1h (`run_kcp_20261012_103442`) | 20 clients ×1h, 43.4% retrans (harshest window yet) | ~45k | 0 |
| WAN batch2 (complete) | 20 clients × **4×1h**, 16,474 MB byte-verified | ~240k | 0 |
| WAN batch3 (complete) | 20 clients × **3×1h**, 13,887 MB byte-verified (2026-10-13) | ~202k | 0 |
| WAN batch4 (complete) | 20 clients × **3×1h**, 12,772 MB byte-verified (2026-10-15) | ~186k | 0 |
| in-process hunts (all generations) | `examples/corruption_hunt.rs`, p30+p45, localhost, bidirectional Bernoulli send-loss, FEC 10/3, full dual-end instrumentation (+ft/kf/rf counters) | **27,080,000** (~358 GB, **fifteen** 4 h local rounds + WAN batches 1-4) | 0 |
| **cumulative excluding the original event** | | **~27.08M** | **0** |

The in-process hunts are the statistical engine: ~90 exch/s (p30) with
dumps armed from exchange 0, `server_fnv` per-leg localisation, and — since
the RTO analysis — a side benefit of **44,865 recorded self-healing tail
stalls in the two completed 4-hour runs alone** (all `mismatches=0`), i.e.
the harness demonstrably survives the harshest delivery patterns while
verifying every byte. The two completed 4 h runs: p30 = 1,340,972 exchanges
(10.98 GB), p45 = 402,773 exchanges (3.30 GB), both `dumps=0`.

Under the original single-event density (1/75k) the expected count over
27.08M is ~360; **P(0 events) ≈ 4×10⁻¹⁵⁷**.

**Hunt status: paused 2026-10-15 by user decision** after 15 full 4-hour
local rounds (zero captures) — the bug stays OPEN at the evidence gate:
if it ever reproduces, `corruption_hunt` dumps (`sent.bin`/`got.bin` +
`server_fnv`) self-locate the corrupting leg from exchange 0. The n=1 density estimate is
statistically refuted: either the true density is orders of magnitude lower,
or the original event needed a condition none of ~20 independent batches
reproduced (specific host/kernel state on 2026-10-11, or an as-yet-unseen
timing interleaving). Every batch ran with dumps armed
(`fail_*.sent/got.bin` / `hunt_sid*_seq*.sent.bin/.got.bin` + `server_fnv`),
so the first capture self-locates the corrupting leg.

**Disjunction of scope:** the delivery *stalls* observed throughout these
batches are a separate, now-closed phenomenon (RTO inflation under loss,
Go-identical — see
[[ANALYSIS_KCP_RTO_INFLATION_TAIL_STALLS]]); they corrupt no bytes and must
not be conflated with this report's content-corruption signature.

## Audit closure — every static corruption path examined and ruled out

- **FEC encode/decode**: shard sets keyed by `shard_id` (no cross-set mixing);
  arrived-max padding is provably the true group max whenever recovery runs
  (parity packets always carry group-max length and are required to reach
  `data_shards`); `fec_kcp_from_recovered` strips via `r[2..sz]` matching Go;
  `FECErrs=0` in all runs.
- **SegmentPool**: per-KCP-instance field (`kcp.rs:141`), never crosses
  sessions; all acquire/release under the session's KCP mutex.
- **Send path**: `Segment::encode` copies `data[..len]` (bounds-checked);
  no freeze-and-empty hazard (the deferred-freeze comment is stale — encode
  copies directly).
- **Receive path**: input copies into segments synchronously
  (`extend_from_slice`), FEC decode copies (`Bytes::copy_from_slice`), the
  app-facing `recv_bytes` `split_to().freeze()` is `BytesMut`-safe
  (refcounted shared storage, no in-place mutation of the frozen half).
- **sharded bufpool / PeerQueue**: full ownership chain traced —
  listener rx → ownership transfer into `PeerQueue` (per-session spare pool,
  swap-based `pop_into`/`pop_batch`), `feed_raw_batch` recycles only AFTER
  `process_inbound_batch` + flush complete (synchronous, borrows ended);
  decrypt-fail recycles are terminal. **No path recycles a buffer while
  another session can still reference its contents.** Side note: the queue's
  `spare` index is write-only (pushed but never re-consumed) — a bounded,
  per-session capacity retention freed on session close; inefficiency, not a
  corruption source.
- **App harness**: `recv_exact` cancel-safety ruled out by 45,315 stream
  blocks with 0 failures (same read path as the failing burst loop).

**Remaining hypotheses (ranked):**
1. Post-UDP-validation kernel/host memory corruption (a bit flip after the
   kernel checksum check lands in the receive buffer) — a single event
   across all hardware touched fits; no software path found.
2. An undiscovered timing interleaving — requires a capture; all batches are
   instrumented.

## Signature refinement — tail-3-segments substitution (2026-10-12)

Re-reading `first_diff=5272` with the exchange layout (8191 B = 7 segments
of 1318/1318/1318/1318/**1318/1318/283**):

- segments 0–3 (5272 B) delivered correct, segments **4, 5 and 6 all
  wrong**, total length exactly preserved. The corruption is *contiguous
  from the first bad segment to the end* — not a lone bad byte.
- A random bit flip landing exactly on offset 5272 has probability
  ≈1/8191, and would not also corrupt the following segments. This
  signature is a **whole-segment (or whole-tail) substitution**, not a
  bit flip — which *weakens* hypothesis 1 (the current top rank) and
  strengthens a structured-data hypothesis.
- Lengths, headers, ordering and `frg`/`sn` were all correct (otherwise
  `recv_exact` would time out instead of returning 8191 wrong-tailed
  bytes). So whatever replaced the payload carried **valid KCP headers
  with wrong content**.
- Two candidate mechanisms survive, and the capture discriminates between
  them:
  1. **FEC recovery produced wrong shard payloads** — under loss, arrived
     shards 0–3 are direct copies (correct); shards 4–6 are the recovered
     ones (wrong), header bytes right / payload bytes wrong is exactly the
     pattern a length/padding error in RS reconstruction would produce
     while still passing `FECErrs` (which counts decode *failures*, not
     wrong-successful decodes). Audit ruled the math out on paper; the
     dump decides: compare `got.bin` tail against RS re-derivation from
     the arrived shards of that group.
  2. **Sender retransmitted stale/foreign content for sn 4–6** — snd_buf
     segment data mutated between first send and retransmit (audited as
     impossible under the KCP mutex, but a capture with both legs' FNV
     settles it: `server_fnv` mismatch puts the sender on the client leg).
- The in-process hunts (`examples/corruption_hunt.rs`, p30 + p45 batches)
  are armed to produce exactly this capture: `sent.bin`/`got.bin` plus
  `server_fnv` per failing exchange.


## Environment

- 20 × `stress_client` (macOS) ↔ `stress_server` (musl, remote Linux),
  FEC 10/3, `--mode fast`, sndwnd/rcvwnd 512, raw UDP (no `PacketTransport`
  wrapper), 3600 s, 5,163,019,862 B sent.
- Harness: `stability_1h/run_kcp_test.sh`; event logs under
  `run_kcp_20261011_185634/logs/worker_13.jsonl`.
