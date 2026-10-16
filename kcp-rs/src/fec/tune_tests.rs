use super::*;

/// Verify that FEC auto-tune eventually converges when the initial dimensions
/// are wrong, and that `should_tune` is cleared after success.  The decoder
/// starts at 1/1 but the wire carries 3/2 groups (shard size 5).  Every packet
/// is fed in order; the probe runs on each one (log rate-limiting only, no
/// sample-based cooldown), so convergence happens as soon as enough pulses are
/// collected.
#[test]
fn unresolved_probe_keeps_collecting_and_later_converges() {
    let mut decoder = FecDecoder::new(1, 1).unwrap();
    for seq in 0..50u32 {
        let flag = if seq % 5 < 3 {
            FEC_TYPE_DATA
        } else {
            FEC_TYPE_PARITY
        };
        let mut packet = [0u8; FEC_HEADER_SIZE_PLUS_2];
        packet[..4].copy_from_slice(&seq.to_le_bytes());
        packet[4..6].copy_from_slice(&flag.to_le_bytes());
        decoder.decode(&packet);
    }
    assert!(!decoder.should_tune);
    assert_eq!((decoder.data_shards, decoder.parity_shards), (3, 2));
}

/// A period resolvable inside the window must still be found *after* a stuck
/// episode — the throttle must never latch the decoder into a dead state.
#[test]
fn converges_after_a_stuck_episode() {
    let mut decoder = FecDecoder::new(1, 1).unwrap();
    // First: 200 packets of an unresolvable pattern.
    for i in 0..200u32 {
        let seq = i / 2;
        let mut p = vec![0u8; FEC_HEADER_SIZE];
        p[..4].copy_from_slice(&seq.to_le_bytes());
        p[4..].copy_from_slice(
            &(if i % 2 == 0 {
                FEC_TYPE_DATA
            } else {
                FEC_TYPE_PARITY
            })
            .to_le_bytes(),
        );
        decoder.decode(&p);
    }
    assert!(decoder.should_tune);
    // Then: a real 3+2 pattern (shard size 5), starting past the stuck seqids.
    let mut seq = 1000u32;
    for _ in 0..80 {
        for k in 0..5u32 {
            let mut p = vec![0u8; FEC_HEADER_SIZE];
            p[..4].copy_from_slice(&seq.to_le_bytes());
            p[4..].copy_from_slice(
                &(if k < 3 {
                    FEC_TYPE_DATA
                } else {
                    FEC_TYPE_PARITY
                })
                .to_le_bytes(),
            );
            decoder.decode(&p);
            seq += 1;
        }
        if !decoder.should_tune {
            break;
        }
    }
    assert!(
        !decoder.should_tune,
        "a resolvable pattern must clear the flag after a stuck episode"
    );
    assert_eq!((decoder.data_shards, decoder.parity_shards), (3, 2));
}

/// The stuck probe must stay allocation-free and must not disturb the sample
/// ring: it re-sorts into its own reusable buffers. (Before this, every packet
/// in the stuck state built two fresh copies of the 258-entry ring and sorted
/// them twice — ~3.7 µs/packet against 41 ns for an aligned packet.)
#[test]
fn stuck_probe_reuses_scratch_and_keeps_the_sample_ring() {
    let mut decoder = FecDecoder::new(10, 3).unwrap();
    // Unresolvable: duplicate seqids with conflicting flags.
    let stuck: Vec<Vec<u8>> = (0..12u32)
        .flat_map(|i| {
            let seq = i / 2;
            [FEC_TYPE_DATA, FEC_TYPE_PARITY].map(|flag| {
                let mut p = vec![0u8; FEC_HEADER_SIZE];
                p[..4].copy_from_slice(&seq.to_le_bytes());
                p[4..].copy_from_slice(&flag.to_le_bytes());
                p
            })
        })
        .collect();
    for (i, p) in stuck.iter().enumerate() {
        decoder.decode(p);
        if i == stuck.len() - 1 {
            break;
        }
    }
    let cap_sorted = decoder.auto_tune.sorted.capacity();
    let cap_dedup = decoder.auto_tune.dedup.capacity();
    let probes_before = decoder.tune_probes;
    assert!(probes_before > 0, "the stuck probe must actually run");

    for i in 0..500 {
        decoder.decode(&stuck[i % stuck.len()]);
    }
    assert!(
        decoder.tune_probes > probes_before + 400,
        "the probe still runs per packet: {} probes",
        decoder.tune_probes
    );
    assert_eq!(
        decoder.auto_tune.sorted.capacity(),
        cap_sorted,
        "probe must reuse its sort buffer, not reallocate"
    );
    assert_eq!(
        decoder.auto_tune.dedup.capacity(),
        cap_dedup,
        "probe must reuse its dedup buffer"
    );
    // The ring still holds the newest samples only (max_samples), untouched by
    // probing.
    assert_eq!(
        decoder.auto_tune.pulses.len(),
        decoder.auto_tune.max_samples
    );
    assert!(decoder.should_tune);
}

// ─── Equivalence with the pre-optimization algorithm ────────────────────────
//
// `find_period_in`/`infer_dimensions_in` replaced a version that cloned the
// ring, sorted it, and rotated another copy of it. These references are that
// original code, verbatim from commit 3552b46c, so the rewrite is checked
// against it on randomized sample histories (reordered, duplicated, wrapped,
// sparse) instead of only on the hand-written cases.

fn find_period_reference(pulses: &VecDeque<(u32, bool)>, bit: bool, paws: u32) -> usize {
    if pulses.len() < 3 {
        return 0;
    }
    let mut sorted = pulses.clone();
    sorted.make_contiguous().sort_by_key(|&(seq, _)| seq);

    // Sequence IDs wrap at PAWS. Rotate the sorted history across the
    // largest cyclic gap, which is the seam between the oldest and newest
    // samples, so the window reads in linear sequence order.
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

    // Contiguity is checked while walking towards each edge, as in Go's
    // `autoTune.FindPeriod`: a gap only aborts the search if it is hit
    // before the edge being looked for. Requiring the whole 258-sample
    // window to be contiguous up front makes detection fail on any link
    // with loss — i.e. exactly the links auto-tune exists for.

    // Find left edge (transition from !bit to bit)
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

    // Find right edge (transition from bit to !bit)
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

fn infer_dimensions_reference(pulses: &VecDeque<(u32, bool)>, paws: u32) -> Option<(usize, usize)> {
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
        let inferred_paws = u32::MAX / period * period;
        if samples
            .iter()
            .all(|&(seq, is_data)| seq < inferred_paws && (seq % period < data) == is_data)
            && pair[1] < paws
        {
            // Losing a whole one-shard phase can hide an intermediate
            // edge (e.g. 1+1 masquerading as 1+3). Every compatible real
            // period divides the distance between these exact edges.
            // Refuse tuning until the observed phases select one period.
            let ambiguous = (2..period).any(|other_period| {
                if period % other_period != 0 {
                    return false;
                }
                let other_data = pair[0] % other_period;
                let other_paws = u32::MAX / other_period * other_period;
                other_data > 0
                    && samples.iter().all(|&(seq, is_data)| {
                        seq < other_paws && (seq % other_period < other_data) == is_data
                    })
            });
            if ambiguous {
                return None;
            }
            return Some((data as usize, (period - data) as usize));
        }
    }
    None
}

fn random_history(rng: &mut u64, len: usize, seq_space: u32) -> VecDeque<(u32, bool)> {
    let mut q = VecDeque::with_capacity(len);
    for _ in 0..len {
        *rng ^= *rng << 13;
        *rng ^= *rng >> 7;
        *rng ^= *rng << 17;
        let seq = (*rng % seq_space as u64) as u32;
        let is_data = (*rng >> 32) & 1 == 0;
        q.push_back((seq, is_data));
    }
    q
}

#[test]
fn period_search_matches_the_original_algorithm_on_random_histories() {
    let mut rng = 0x243F_6A88_85A3_08D3u64;
    for case in 0..400 {
        let len = (case * 7) % 300;
        // Mix dense (contiguous, periodic) and sparse seqid spaces, including
        // histories that wrap past the paws boundary.
        let seq_space = [8u32, 16, 64, 258, 300, 1_000_000]
            .get(case % 6)
            .copied()
            .unwrap();
        let history = random_history(&mut rng, len, seq_space);
        let mut tune = AutoTune::new();
        for &(seq, is_data) in history.iter() {
            tune.sample(is_data, seq);
        }
        let max_seq = tune.pulses.iter().map(|&(seq, _)| seq).max().unwrap_or(0);
        for paws in [u32::MAX, max_seq + 1, max_seq + 7] {
            for bit in [true, false] {
                assert_eq!(
                    tune.find_period(bit, paws),
                    find_period_reference(&tune.pulses, bit, paws),
                    "find_period bit={bit} paws={paws} case={case} len={len}"
                );
            }
            assert_eq!(
                tune.infer_dimensions(paws),
                infer_dimensions_reference(&tune.pulses, paws),
                "infer_dimensions paws={paws} case={case} len={len}"
            );
        }
    }
}

/// Periodic histories, i.e. what a real FEC stream looks like, including one
/// reordered packet and one dropped phase.
#[test]
fn period_search_matches_the_original_on_structured_histories() {
    for period in [2usize, 3, 5, 13, 256] {
        for data in 1..period {
            for len in [period, period * 3, 258, 300] {
                for perturb in 0..4 {
                    let mut tune = AutoTune::new();
                    let mut history = VecDeque::new();
                    for i in 0..len {
                        let seq = i as u32;
                        let is_data = (i % period) < data;
                        history.push_back((seq, is_data));
                    }
                    match perturb {
                        // one reordered pair
                        1 if history.len() > 4 => {
                            history.swap(1, 3);
                        }
                        // one missing sample
                        2 if history.len() > 4 => {
                            history.remove(2);
                        }
                        // one flipped flag
                        3 if !history.is_empty() => {
                            let last = history.len() - 1;
                            history[last].1 = !history[last].1;
                        }
                        _ => {}
                    }
                    for &(seq, is_data) in history.iter() {
                        tune.sample(is_data, seq);
                    }
                    // `paws` must exceed every sampled seqid — `decode()`
                    // rejects `seqid >= paws` before sampling, so the tuner
                    // never sees a history that stretches past it.
                    let max_seq = tune.pulses.iter().map(|&(seq, _)| seq).max().unwrap_or(0);
                    for paws in [u32::MAX, max_seq + 1, max_seq + 2] {
                        for bit in [true, false] {
                            assert_eq!(
                                tune.find_period(bit, paws),
                                find_period_reference(&tune.pulses, bit, paws),
                                "period={period} data={data} len={len} perturb={perturb} bit={bit}"
                            );
                        }
                        assert_eq!(
                            tune.infer_dimensions(paws),
                            infer_dimensions_reference(&tune.pulses, paws),
                            "period={period} data={data} len={len} perturb={perturb}"
                        );
                    }
                }
            }
        }
    }
}
