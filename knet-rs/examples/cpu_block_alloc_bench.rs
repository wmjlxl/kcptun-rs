//! Allocation counter for `knet::cpu_block`.
//!
//! Counts heap allocations per `cpu_block` call using a counting global
//! allocator. Used to A/B the per-call `Box` + bounded-channel allocations
//! (O8: pooled channel pair should drop allocs/call).
//!
//! Usage:
//!   cargo run --release --example cpu_block_alloc_bench -- [calls]
//!   (default: 1000 calls after a 100-call pool warmup)

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountingAlloc;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static ALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(new_size, Ordering::Relaxed);
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static A: CountingAlloc = CountingAlloc;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let calls: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(1000);

    knet::block_on(async {
        // Warmup: initialize the blocking pool so its one-time allocations
        // (thread stacks, pool channels) are not attributed to the measurement.
        for _ in 0..100 {
            let v: u64 = knet::cpu_block(|| 7).await;
            assert_eq!(v, 7);
        }

        let a0 = ALLOCS.load(Ordering::Relaxed);
        let b0 = ALLOC_BYTES.load(Ordering::Relaxed);
        for _ in 0..calls {
            let v: u64 = knet::cpu_block(|| 7).await;
            assert_eq!(v, 7);
        }
        let da = ALLOCS.load(Ordering::Relaxed) - a0;
        let db = ALLOC_BYTES.load(Ordering::Relaxed) - b0;
        println!(
            "cpu_block calls={calls} allocs_total={da} allocs_per_call={:.2} bytes_total={db} bytes_per_call={:.1}",
            da as f64 / calls as f64,
            db as f64 / calls as f64
        );
    });
}
