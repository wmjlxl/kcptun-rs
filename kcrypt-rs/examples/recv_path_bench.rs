//! Measures the two costs the audit review flagged on the *receive* path for a
//! single 1350-byte CFB datagram:
//!   * the CFB decrypt itself (`CryptEngine::decrypt` over the whole packet),
//!   * the `copy_within(20..n, 0)` memmove that moves the payload to the front.
//!
//! Usage: cargo run --release -p kcrypt-rs --example recv_path_bench [iters] [size]
use kcrypt_rs::{BlockCrypt, CryptEngine, CRYPTO_HEADER_SIZE};
use std::hint::black_box;
use std::time::Instant;

fn main() {
    let iters: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000);
    let size: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1350);

    let (crypt, name) = CryptEngine::select("aes", b"kcptun-bench-key");
    // A real wire packet: [nonce 16][CRC32 4][ciphertext ...].
    let mut sealed = vec![0u8; size];
    {
        // Fill with pseudo-random bytes so the CFB feedback is realistic.
        let mut x = 0x1234_5678u32;
        for b in sealed.iter_mut() {
            x = x.wrapping_mul(1664525).wrapping_add(1013904223);
            *b = (x >> 24) as u8;
        }
        // Encrypt the payload region in place so decrypt() has work to do.
        crypt.encrypt(&mut sealed[CRYPTO_HEADER_SIZE..]);
        let _ = crypt.name();
    }

    // 1. decrypt only
    let start = Instant::now();
    for _ in 0..iters {
        let mut buf = black_box(sealed.clone());
        crypt.decrypt(&mut buf);
        black_box(&buf);
    }
    let per_iter_clone = start.elapsed().as_secs_f64() * 1e9 / iters as f64;

    // Clone is part of the measurement above (a recv path reuses its buffer), so
    // measure the clone alone and subtract it.
    let start = Instant::now();
    for _ in 0..iters {
        black_box(black_box(&sealed).clone());
    }
    let clone_ns = start.elapsed().as_secs_f64() * 1e9 / iters as f64;

    // 2. memmove only (payload → front)
    let mut work = sealed.clone();
    let start = Instant::now();
    for _ in 0..iters {
        black_box(&mut work).copy_within(CRYPTO_HEADER_SIZE..size, 0);
        black_box(&work);
    }
    let move_ns = start.elapsed().as_secs_f64() * 1e9 / iters as f64;

    // 3. memmove long enough to defeat the cache for a "cold" upper bound.
    let mut big = vec![0u8; 2048];
    let start = Instant::now();
    for _ in 0..iters {
        black_box(&mut big).copy_within(CRYPTO_HEADER_SIZE..2048, 0);
        black_box(&big);
    }
    let move2048_ns = start.elapsed().as_secs_f64() * 1e9 / iters as f64;

    println!("crypt={name} size={size} iters={iters}");
    println!("decrypt(+clone) : {per_iter_clone:8.1} ns/pkt   (clone alone {clone_ns:.1})");
    println!("decrypt         : {:8.1} ns/pkt", per_iter_clone - clone_ns);
    println!("memmove {size:>4}B   : {move_ns:8.1} ns/pkt");
    println!("memmove 2048B   : {move2048_ns:8.1} ns/pkt");
    println!(
        "memmove share of decrypt+memmove: {:.1}%",
        100.0 * move_ns / (per_iter_clone - clone_ns + move_ns)
    );
}
