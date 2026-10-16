<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-07-28 | Updated: 2026-08-16 (tokio-only refactor) -->

# kpprof-rs

## Purpose

Go-compatible pprof HTTP server (`lib` name: `kpprof`). Serves `/debug/pprof/*` endpoints matching Go's `net/http/pprof`, emitting CPU / heap / allocs profiles as **Go pprof protobuf** (application-level gzipped), analyzable directly with `go tool pprof`. Optional deadlock detection via `parking_lot`.

## Key Files

| File | Description |
|------|-------------|
| `Cargo.toml` | Deps: `knet-rs` (tokio); optional `deadlock`; `pprof` (protobuf-codec always; `frame-pointer` only on x86_64/aarch64/riscv64/loongarch64 — armv7-safe), `backtrace`, `flate2`, `mimalloc`, `parking_lot` |
| `src/lib.rs` | `run_pprof()` entry point; HTTP server loop; route dispatch for all `/debug/pprof/*` endpoints; `respond()`, `gzip_bytes()`, `empty_profile()`, `build_index_html()`, `dump_threads()` |
| `src/heap.rs` | `ProfilingAllocator` (wraps `mimalloc`); sampling logic; `build_heap_profile()` / `build_allocs_profile()` pprof protobuf builders |
| `src/deadlock.rs` | `start_deadlock_detector()` background thread; `dump_deadlocks()` on-demand check (requires `deadlock` feature) |

## For AI Agents

### Working In This Directory

- **Go pprof compatibility is the hard constraint.** All protobuf profiles use application-level gzip (inside the encoder, matching Go's `runtime/pprof`), NOT HTTP `Content-Encoding`. `go tool pprof` detects gzip by magic bytes — do **not** set `Content-Encoding: gzip`.
- `X-Content-Type-Options: nosniff` on all responses (matching Go). `Content-Disposition: attachment; filename="..."` on profile responses.
- CPU profiling uses `pprof::ProfilerGuardBuilder` at 997 Hz, offloaded via `knet::cpu_block`. The `blocklist` excludes `libc`, `libgcc`, `pthread`, `vdso` (on x86_64/aarch64/riscv64/loongarch64).
- **pprof `frame-pointer` is target-gated** in Cargo.toml (enabled only on x86_64/aarch64/riscv64/loongarch64). Enabling it globally breaks armv7 because `is_blocklisted` is cfg-gated off that arch, and RT_PKGS always builds this crate.
- Symbol endpoint (`/debug/pprof/symbol`) must support both GET (raw query `0xADDR+0xADDR`) and POST (body). Always returns `num_symbols: 1\n` first line (Go format). `backtrace::resolve()` for symbolization.
- Heap profiling: sampling rate 1 per 512 KB (`DEFAULT_SAMPLE_RATE = 524_288`, Go `MemProfileRate`-compatible). Changeable at runtime with `set_heap_sample_rate(n)` (`0` disables; `heap_sample_rate()` reads it back). Fast path = atomic counter; slow path = `backtrace::trace()` (raw addresses only, **no symbolization in allocator path** — avoids `addr2line` `OnceCell` reentrant init panics). Symbolization deferred to `build_profile()`. Both binaries call `set_heap_sample_rate(0)` at startup when `--pprof` is off, since heap samples are only readable through the HTTP endpoints.
- Re-entrance guard: thread-local `Cell<bool>` (`IN_SAMPLE`) prevents unbounded recursion when `backtrace::trace()` itself allocates.
- `ProfilingAllocator` with `sample_rate == 0` costs an atomic load plus a branch per allocation (no counter add) — cheap, not free. With a non-zero rate every allocation also does a TLS check and a shared `fetch_add`.
- The HTTP server handles each connection on its own task (`knet::spawn_task`) with a 64-connection cap (`503` past it), a 5 s total request deadline, an 8 KiB header cap and a 64 KiB body cap on the one route that reads a body (`/debug/pprof/symbol`). `PROFILE_BUSY` is released through a drop guard, so completion, failure and cancellation all release it.
- `empty_profile()` builds minimal valid pprof protobuf (0 samples) for Go runtime-only types (`block`, `mutex`, `threadcreate`, `goroutine` debug=0) so `go tool pprof` doesn't error.
- `dump_threads()`: Linux reads `/proc/self/task/{tid}/{comm,stack,syscall,status}`; non-Linux falls back to `parking_lot::deadlock::check_deadlock()` if `deadlock` feature is on.
- HTTP server is hand-rolled (no framework) — reads request headers in a loop, parses method/path/query, dispatches. `Connection: close` on every response.

### Testing Requirements

```bash
cargo test -p kpprof-rs
```

### Common Patterns

```rust
// In any binary that depends on kpprof-rs:
#[cfg(feature = "pprof")]
#[global_allocator]
static GLOBAL: kpprof::ProfilingAllocator = kpprof::ProfilingAllocator::new();

#[cfg(feature = "pprof")]
if let Some(ref addr) = pprof_addr {
    #[cfg(feature = "deadlock")]
    kpprof::start_deadlock_detector();
    let stop = stop_flag.clone();
    knet::spawn_task(async move {
        let _ = kpprof::run_pprof(&addr, stop).await;
    });
}
```

This crate can be used standalone — add `kpprof-rs` as a dependency, enable the `pprof` feature in the consuming binary, and call `run_pprof()`.

## Dependencies

### Internal

- `knet-rs` (tokio: `TcpListener`, `TcpStream`, `cpu_block`, `spawn_task`, `timeout`)

### External

- `pprof` (CPU profiling, protobuf-codec)
- `backtrace` (stack capture + symbol resolution)
- `flate2` (gzip compression, rust_backend)
- `mimalloc` (underlying allocator wrapped by `ProfilingAllocator`)
- `parking_lot` (mutex + optional deadlock detection)
- `anyhow`, `log`

<!-- MANUAL: -->
