//! kcptun-client -- KCP-based TCP stream accelerator.
//!
//! A Rust port of the Go kcptun client.
//! Listens locally, forwards connections over KCP/UDP multiplexed via SMUX.

#![allow(
    clippy::collapsible_match,
    clippy::question_mark,
    clippy::explicit_auto_deref
)]

#[cfg(not(feature = "pprof"))]
use mimalloc::MiMalloc;

#[cfg(not(feature = "pprof"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(feature = "pprof")]
#[global_allocator]
static GLOBAL: kpprof::ProfilingAllocator = kpprof::ProfilingAllocator::new();

mod app;
mod cli;
mod client;
mod socket;

use anyhow::Result;

/// Default KCP conversation ID.
const DEFAULT_CONV: u32 = 0xDEADBEEF;

fn main() -> Result<()> {
    // O16: the runtime is built lazily by the first `block_on`, but this
    // binary parses its clap `Cli` inside `async_main` — too late. Mirror
    // `--threads N` / `--threads=N` from the raw args here; full clap
    // validation still happens when `Cli` is parsed inside `async_main`.
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--threads" {
            if let Some(n) = args.next().and_then(|v| v.parse::<usize>().ok()) {
                knet::configure_worker_threads(n);
            }
        } else if let Some(n) = arg.strip_prefix("--threads=") {
            if let Ok(n) = n.parse::<usize>() {
                knet::configure_worker_threads(n);
            }
        }
    }
    knet::block_on(app::async_main())
}
