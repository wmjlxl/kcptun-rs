# BUG: `--snmplog` time-layout expansion rewrites the whole path, including directories

**Date:** 2026-10-11
**Found by:** 1-hour / 20-process stability test (`stability_1h/`)
**Severity:** Low (operational footgun; no data-path impact)

## Summary

`snmp_log::expand_time_format` applies Go time-layout token replacement to
the **entire** `--snmplog` path, not just the filename. Any ancestor
directory (or the filename) containing digit runs that match Go layout tokens
gets rewritten to a different, usually nonexistent, path. The SNMP logger then
fails every period with `No such file or directory`.

## Reproduction

```bash
RUN=stability_1h/run_20261011_131816   # contains "02" (year 20**02**6?) and "01"
kcptun-client ... --snmplog "$RUN/logs/client_15_snmp.csv"
```

Observed (from `client_1.log`, 2026-10-11T05:20:24Z):

```
ERROR kcptun_common::snmp_log] Failed to open SNMP log file
'/Users/.../stability_1h/run_21161101_131816/logs/client_1_snmp.csv':
No such file or directory (os error 2)
```

Trace of the transformation of `run_20261011_131816` (local date 2026-10-11):

| token found | replaced with | reason |
|---|---|---|
| `02` (from `20**02**6...`) | `11` | Go layout `02` = day of month |
| `01` (from `26**10**11` → `1011` region) | `10` | Go layout `01` = month |

result: `run_20261011_...` → `run_21161101_...` (nonexistent dir).

Additionally `client_15_snmp.csv` → `client_13_snmp.csv` for **client 15**:
layout token `15` = hour, expanded to local hour `13`. This is visible in
`client_15.log`, which reports a failure for `client_13_snmp.csv`.

## Impact

- Any `--snmplog` path whose full string contains `2006|01|02|15|04|05`
  (or `%Y|%m|%d|%H|%M|%S`) — which includes every `YYYYMMDD` directory name
  and every `HH`-style id — is silently corrupted.
- Practically: **any timestamped run directory** breaks SNMP logging.
- No effect on the KCP data path, session behavior, or correctness.

## Suggested fix

Expand time tokens only in the **file name component**, leaving parent
directories intact; or require tokens to be explicitly enabled (e.g. only
expand when the path contains `%`-style tokens, keeping Go-style expansion
for backward compatibility only in the basename).

```rust
// sketch
let (dir, file) = path.rsplit_once('/').unwrap_or(("", path));
let file = expand_time_format(file);          // tokens only here
let path = if dir.is_empty() { file } else { format!("{dir}/{file}") };
```

## Workaround (used by this repo's harness)

Place `--snmplog` under a path with no digit runs matching layout tokens,
e.g. `stability_1h/snmp/client_1.csv` (ancestors `/Users/sean/Documents/
kcptun-rs/...` contain no `01|02|15|2006|...`).

## Environment

- 2026-10-11 stability run `stability_1h/run_20261011_131816`
- macOS client, Linux musl server; both binaries from this tree.
