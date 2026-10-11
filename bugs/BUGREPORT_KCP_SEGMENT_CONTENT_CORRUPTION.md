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

## Environment

- 20 × `stress_client` (macOS) ↔ `stress_server` (musl, remote Linux),
  FEC 10/3, `--mode fast`, sndwnd/rcvwnd 512, raw UDP (no `PacketTransport`
  wrapper), 3600 s, 5,163,019,862 B sent.
- Harness: `stability_1h/run_kcp_test.sh`; event logs under
  `run_kcp_20261011_185634/logs/worker_13.jsonl`.
