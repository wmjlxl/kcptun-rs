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
    knet::block_on(app::async_main())
}
