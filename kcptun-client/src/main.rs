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
    // Parse CLI once here so --threads is honoured before the runtime is built
    // (it is created lazily by the first `block_on`). Full clap validation —
    // including `--`, repeated args, Go `-ds/-ps` aliases — happens here, not
    // in a second raw-args scan.
    let cli = cli::Cli::parse_go_compatible();
    knet::configure_worker_threads(cli.threads);
    knet::block_on(app::async_main(cli))
}
