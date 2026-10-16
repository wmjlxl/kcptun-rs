//! FEC auto-tune probe: cost of the pre-optimization probe vs the current one.
//!
//! The probe runs on every FEC packet while `should_tune` is set, and a single
//! stale or injected packet sets it — so this was the review's "repeated
//! computation while FEC has not converged" item.
//!
//! Both algorithms are measured **interleaved in the same process**, on the same
//! sample histories, keeping the minimum over rounds: absolute numbers on a
//! shared machine are dominated by other load, the ratio is not. The old probe
//! is the pre-change algorithm verbatim (commit 3552b46c); it is also checked
//! against the new one by `fec::tune_tests::period_search_matches_*`.
//!
//! Usage: cargo run --release -p kcp-rs --example fec_tune_bench [iters]

use std::collections::VecDeque;
use std::hint::black_box;
use std::time::Instant;

/// Sample history used by both probes.
type Ring = VecDeque<(u32, bool)>;

fn periodic(len: usize, period: usize, data: usize) -> Ring {
    (0..len).map(|i| (i as u32, (i % period) < data)).collect()
}

/// Duplicate seqids carrying conflicting flags: unresolvable, and (because the
/// arrival order is not monotone) the sort cannot be skipped.
fn duplicated(len: usize) -> Ring {
    (0..len)
        .map(|i| {
            let seq = (i / 2) as u32;
            (seq, i % 2 == 0)
        })
        .collect()
}

// ─── OLD probe (commit 3552b46c, verbatim algorithm) ────────────────────────

fn old_find_period(pulses: &Ring, bit: bool, paws: u32) -> usize {
    if pulses.len() < 3 {
        return 0;
    }
    let mut sorted = pulses.clone();
    sorted.make_contiguous().sort_by_key(|&(seq, _)| seq);
    let mut largest_gap = 0;
    let mut largest_gap_at = 0;
    for i in 0..sorted.len() {
        let a = sorted[i].0;
        let b = sorted[(i + 1) % sorted.len()].0;
        let gap = if b >= a { b - a } else { paws - a + b };
        if gap > largest_gap {
            largest_gap = gap;
            largest_gap_at = i;
        }
    }
    let start = (largest_gap_at + 1) % sorted.len();
    let ordered: Vec<(u32, bool)> = (0..sorted.len())
        .map(|offset| sorted[(start + offset) % sorted.len()])
        .collect();
    let contiguous = |prev: u32, current: u32| -> bool {
        let diff = if current >= prev {
            current - prev
        } else {
            paws - prev + current
        };
        diff == 1
    };
    let mut left_edge = None;
    for i in 1..ordered.len() {
        if !contiguous(ordered[i - 1].0, ordered[i].0) {
            return 0;
        }
        if ordered[i - 1].1 != bit && ordered[i].1 == bit {
            left_edge = Some(i);
            break;
        }
    }
    let left = match left_edge {
        Some(i) => i,
        None => return 0,
    };
    for i in left + 1..ordered.len() {
        if !contiguous(ordered[i - 1].0, ordered[i].0) {
            return 0;
        }
        if ordered[i - 1].1 == bit && ordered[i].1 != bit {
            return i - left;
        }
    }
    0
}

fn old_infer(pulses: &Ring, paws: u32) -> Option<(usize, usize)> {
    let mut samples: Vec<_> = pulses.iter().copied().collect();
    samples.sort_unstable_by_key(|&(seq, _)| seq);
    if samples
        .windows(2)
        .any(|w| w[0].0 == w[1].0 && w[0].1 != w[1].1)
    {
        return None;
    }
    samples.dedup_by_key(|sample| sample.0);
    let edges: Vec<_> = samples
        .windows(2)
        .filter(|w| w[0].1 && !w[1].1 && w[1].0 - w[0].0 == 1)
        .map(|w| w[1].0)
        .collect();
    for pair in edges.windows(2) {
        let period = pair[1] - pair[0];
        if !(2..=256).contains(&period) {
            continue;
        }
        let data = pair[0] % period;
        if data == 0 {
            continue;
        }
        let observed = samples
            .iter()
            .filter(|&&(seq, _)| seq >= pair[0] && seq <= pair[1])
            .count();
        if observed < period as usize {
            continue;
        }
        if samples
            .iter()
            .all(|&(seq, is_data)| (seq % period < data) == is_data)
        {
            return Some((data as usize, (period - data) as usize));
        }
    }
    let _ = paws;
    None
}

fn old_probe(pulses: &Ring, paws: u32) -> (usize, usize) {
    let mut ds = old_find_period(pulses, true, paws);
    let mut ps = old_find_period(pulses, false, paws);
    if ds == 0 || ps == 0 {
        if let Some((d, p)) = old_infer(pulses, paws) {
            ds = d;
            ps = p;
        }
    }
    (ds, ps)
}

// ─── NEW probe (current implementation) ────────────────────────────────────

fn new_probe(
    pulses: &Ring,
    sorted: &mut Vec<(u32, bool)>,
    dedup: &mut Vec<(u32, bool)>,
    paws: u32,
) -> (usize, usize) {
    sorted.clear();
    sorted.extend(pulses.iter().copied());
    if !sorted.is_sorted_by_key(|&(seq, _)| seq) {
        sorted.sort_by_key(|&(seq, _)| seq);
    }
    let mut ds = new_find_period(sorted, true, paws);
    let mut ps = new_find_period(sorted, false, paws);
    if ds == 0 || ps == 0 {
        if let Some((d, p)) = new_infer(sorted, dedup, paws) {
            ds = d;
            ps = p;
        }
    }
    (ds, ps)
}

fn new_find_period(sorted: &[(u32, bool)], bit: bool, paws: u32) -> usize {
    if sorted.len() < 3 {
        return 0;
    }
    let len = sorted.len();
    let mut largest_gap = 0;
    let mut largest_gap_at = 0;
    for i in 0..len - 1 {
        let gap = sorted[i + 1].0 - sorted[i].0;
        if gap > largest_gap {
            largest_gap = gap;
            largest_gap_at = i;
        }
    }
    if paws
        .wrapping_sub(sorted[len - 1].0)
        .wrapping_add(sorted[0].0)
        > largest_gap
    {
        largest_gap_at = len - 1;
    }
    let start = (largest_gap_at + 1) % len;
    let (before, after) = sorted.split_at(start);
    let at = |i: usize| -> (u32, bool) {
        if i < after.len() {
            after[i]
        } else {
            before[i - after.len()]
        }
    };
    let contiguous = |prev: u32, current: u32| -> bool {
        let diff = if current >= prev {
            current - prev
        } else {
            paws.wrapping_sub(prev).wrapping_add(current)
        };
        diff == 1
    };
    let mut left_edge = None;
    for i in 1..len {
        if !contiguous(at(i - 1).0, at(i).0) {
            return 0;
        }
        if at(i - 1).1 != bit && at(i).1 == bit {
            left_edge = Some(i);
            break;
        }
    }
    let left = match left_edge {
        Some(i) => i,
        None => return 0,
    };
    for i in left + 1..len {
        if !contiguous(at(i - 1).0, at(i).0) {
            return 0;
        }
        if at(i - 1).1 == bit && at(i).1 != bit {
            return i - left;
        }
    }
    0
}

fn new_infer(
    sorted: &[(u32, bool)],
    dedup: &mut Vec<(u32, bool)>,
    paws: u32,
) -> Option<(usize, usize)> {
    dedup.clear();
    dedup.extend(sorted.iter().copied());
    if sorted
        .windows(2)
        .any(|w| w[0].0 == w[1].0 && w[0].1 != w[1].1)
    {
        return None;
    }
    dedup.dedup_by_key(|sample| sample.0);
    let samples = dedup;
    let edges: Vec<_> = samples
        .windows(2)
        .filter(|w| w[0].1 && !w[1].1 && w[1].0 - w[0].0 == 1)
        .map(|w| w[1].0)
        .collect();
    for pair in edges.windows(2) {
        let period = pair[1] - pair[0];
        if !(2..=256).contains(&period) {
            continue;
        }
        let data = pair[0] % period;
        if data == 0 {
            continue;
        }
        let observed = samples
            .iter()
            .filter(|&&(seq, _)| seq >= pair[0] && seq <= pair[1])
            .count();
        if observed < period as usize {
            continue;
        }
        if samples
            .iter()
            .all(|&(seq, is_data)| (seq % period < data) == is_data)
        {
            return Some((data as usize, (period - data) as usize));
        }
    }
    let _ = paws;
    None
}

fn main() {
    let iters: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000);
    let paws = u32::MAX;

    let cases: [(&str, Ring); 3] = [
        ("periodic  3+2  ", periodic(258, 5, 3)),
        ("periodic 128+128", periodic(258, 256, 128)),
        ("reordered/dup  ", duplicated(258)),
    ];

    for (label, ring) in cases {
        // Both probes must agree on this history.
        let mut sorted = Vec::with_capacity(258);
        let mut dedup = Vec::with_capacity(258);
        assert_eq!(
            old_probe(&ring, paws),
            new_probe(&ring, &mut sorted, &mut dedup, paws),
            "{label}: the optimized probe changed its answer"
        );

        let mut best_old = f64::MAX;
        let mut best_new = f64::MAX;
        for round in 0..6 {
            let t = Instant::now();
            for _ in 0..iters {
                black_box(old_probe(black_box(&ring), paws));
            }
            let old_ns = t.elapsed().as_secs_f64() * 1e9 / iters as f64;
            let t = Instant::now();
            for _ in 0..iters {
                black_box(new_probe(black_box(&ring), &mut sorted, &mut dedup, paws));
            }
            let new_ns = t.elapsed().as_secs_f64() * 1e9 / iters as f64;
            if round > 0 {
                best_old = best_old.min(old_ns);
                best_new = best_new.min(new_ns);
            }
        }
        println!(
            "{label}: old {best_old:8.1} ns/probe   new {best_new:8.1} ns/probe   {:.2}x",
            best_old / best_new
        );
    }
}
