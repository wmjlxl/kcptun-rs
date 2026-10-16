//! Cost of the heap-sampling hook with the default rate (1 sample per 512 KB)
//! versus sampling disabled (`set_heap_sample_rate(0)`), which is what both
//! binaries now do when `--pprof` is off.
//!
//! Runs the same allocation-bound workload both ways, **interleaved in one
//! process** and keeping the minimum over rounds, so the result is a ratio
//! rather than a load-sensitive absolute number. Single- and multi-threaded,
//! because the hook's shared `fetch_add` is what contention is about.
//!
//! Usage: cargo run --release -p kpprof-rs --example alloc_sampling_bench

use kpprof::{heap_sample_rate, set_heap_sample_rate, ProfilingAllocator};
use std::hint::black_box;
use std::time::Instant;

#[global_allocator]
static ALLOC: ProfilingAllocator = ProfilingAllocator::new();

/// Allocate/free `rounds` blocks of `size` bytes, touching each one.
fn workload(rounds: usize, size: usize) -> usize {
    let mut acc = 0usize;
    for i in 0..rounds {
        let mut v: Vec<u8> = Vec::with_capacity(size);
        v.resize(size, (i & 0xFF) as u8);
        acc = acc.wrapping_add(v[size / 2] as usize);
        black_box(&v);
    }
    acc
}

fn time(threads: usize, rounds: usize, size: usize) -> f64 {
    let start = Instant::now();
    if threads == 1 {
        black_box(workload(rounds, size));
    } else {
        std::thread::scope(|scope| {
            for _ in 0..threads {
                scope.spawn(|| black_box(workload(rounds, size)));
            }
        });
    }
    start.elapsed().as_secs_f64()
}

fn main() {
    let rounds: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000);
    println!(
        "default sample rate = {} bytes; workload = {} alloc/free rounds",
        heap_sample_rate(),
        rounds
    );

    for (label, threads, size) in [
        ("1 thread,   32 B blocks", 1usize, 32usize),
        ("1 thread, 4096 B blocks", 1, 4096),
        ("4 threads,  32 B blocks", 4, 32),
        ("4 threads, 4096 B blocks", 4, 4096),
    ] {
        let mut best_off = f64::MAX;
        let mut best_on = f64::MAX;
        for round in 0..6 {
            let previous = set_heap_sample_rate(0);
            let t_off = time(threads, rounds, size);
            set_heap_sample_rate(previous);
            let t_on = time(threads, rounds, size);
            if round > 0 {
                best_off = best_off.min(t_off);
                best_on = best_on.min(t_on);
            }
        }
        let off_ns = best_off * 1e9 / (rounds * threads) as f64;
        let on_ns = best_on * 1e9 / (rounds * threads) as f64;
        println!(
            "{label}: sampling on {on_ns:7.1} ns/op   off {off_ns:7.1} ns/op   hook costs {:.1}%",
            100.0 * (on_ns - off_ns) / on_ns
        );
    }
    // Restore the compiled-in default, as a binary does when --pprof is set.
    set_heap_sample_rate(524_288);
}
