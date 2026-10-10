//! Per-packet CFB/GCM microbench for kcrypt-rs crypto primitives.
//!
//! Measures the raw per-packet cost of one packet-sized buffer:
//! - Block ciphers: `BlockCrypt::encrypt` / `decrypt` (bare cfb16/… over the
//!   slice — no wire header, no CRC; that layer lives in `wire.rs`).
//! - AEAD (`aes-128-gcm`): `AeadCrypt::seal` / `open` (includes the per-call
//!   Vec allocation of the returned packet — noted in the output).
//!
//! Used to (a) verify on x86_64 that AES-CFB runs on hardware (AES-NI vs
//! T-tables differ by an order of magnitude in ns/pkt) and (b) A/B individual
//! optimization items such as O9 (batched decrypt keystream).
//!
//! Usage:
//!   cargo run --release -p kcrypt-rs --example cfb_bench -- <method> <enc|dec> [iters] [size]
//!   e.g. cfb_bench aes dec 2000000 1326

use kcrypt_rs::{AeadCrypt, BlockCrypt, CryptEngine};
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let method = args.get(1).map(String::as_str).unwrap_or("aes");
    let mode = args.get(2).map(String::as_str).unwrap_or("dec");
    let iters: usize = args
        .get(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000);
    let size: usize = args
        .get(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1326);
    assert!(mode == "enc" || mode == "dec", "mode must be enc|dec");

    let (crypt, name) = CryptEngine::select(method, b"kcptun-bench-key");
    let is_aead = crypt.is_aead();
    let mut buf = vec![0xA5u8; size];

    let start;
    if is_aead {
        let aead = crypt.as_aead().expect("is_aead");
        // AEAD packets are [nonce][ciphertext+tag]; open() rejects garbage.
        let sealed = aead.seal(&buf);
        for _ in 0..20_000 {
            if mode == "enc" {
                let _ = aead.seal(&buf);
            } else {
                let _ = aead.open(&sealed).expect("open roundtrip");
            }
        }
        start = Instant::now();
        if mode == "enc" {
            for _ in 0..iters {
                let _ = aead.seal(&buf);
            }
        } else {
            for _ in 0..iters {
                let _ = aead.open(&sealed).expect("open roundtrip");
            }
        }
    } else {
        for _ in 0..20_000 {
            if mode == "enc" {
                crypt.encrypt(&mut buf);
            } else {
                crypt.decrypt(&mut buf);
            }
        }
        start = Instant::now();
        if mode == "enc" {
            for _ in 0..iters {
                crypt.encrypt(&mut buf);
            }
        } else {
            for _ in 0..iters {
                crypt.decrypt(&mut buf);
            }
        }
    }
    let elapsed = start.elapsed();

    let ns_per_pkt = elapsed.as_nanos() as f64 / iters as f64;
    let mbps = (size as f64 * iters as f64) / elapsed.as_secs_f64() / 1e6;
    let note = if is_aead { " (aead, incl. per-call Vec alloc)" } else { "" };
    println!(
        "method={name} mode={mode} size={size} iters={iters} total_s={:.3} ns/pkt={ns_per_pkt:.1} MB/s={mbps:.1}{note}",
        elapsed.as_secs_f64()
    );
}
