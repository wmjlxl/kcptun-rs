//! kcptun-server -- KCP-based TCP stream accelerator (server side).
//!
//! A Rust port of the Go kcptun server.
//! Listens on UDP for KCP connections, accepts SMUX streams, forwards to TCP targets.

#![allow(
    clippy::collapsible_match,
    clippy::question_mark,
    clippy::explicit_auto_deref,
    clippy::redundant_closure,
    clippy::too_many_arguments
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
mod server;
mod socket;

use anyhow::Result;

fn main() -> Result<()> {
    let cli = cli::Cli::parse_go_compatible();
    knet::block_on(app::async_main(cli))
}
