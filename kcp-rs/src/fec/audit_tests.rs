use super::AutoTune;

fn sampled_period(missing: &[u32], wrong_phase: Option<u32>) -> AutoTune {
    let mut tune = AutoTune::new();
    for seq in 127..=384 {
        if !missing.contains(&seq) {
            let is_data = seq % 256 < 128;
            tune.sample(
                if wrong_phase == Some(seq) {
                    !is_data
                } else {
                    is_data
                },
                seq,
            );
        }
    }
    tune
}

#[test]
fn boundary_loss_fallback_requires_a_consistent_nearly_complete_period() {
    let paws = u32::MAX / 256 * 256;
    let mut valid = sampled_period(&[256], None);
    assert_eq!(valid.infer_dimensions(paws), Some((128, 128)));
    valid.sample(true, 300); // a matching duplicate cannot invent another slot
    assert_eq!(valid.infer_dimensions(paws), Some((128, 128)));
    assert_eq!(
        sampled_period(&[200, 256], None).infer_dimensions(paws),
        None
    );
    assert_eq!(
        sampled_period(&[256], Some(200)).infer_dimensions(paws),
        None
    );
    let mut conflicting = sampled_period(&[256], None);
    conflicting.sample(false, 300);
    assert_eq!(conflicting.infer_dimensions(paws), None);
}

#[test]
fn wrap_history_is_conservative_then_resolves_a_fresh_period() {
    let paws = u32::MAX / 256 * 256;
    let mut tune = AutoTune::new();
    for seq in paws - 129..paws {
        tune.sample(seq % 256 < 128, seq);
    }
    for seq in 1..=128 {
        tune.sample(seq % 256 < 128, seq);
    }
    // The fallback never guesses a huge interval across the sequence seam.
    assert_eq!(tune.infer_dimensions(paws), None);
    for seq in 129..=384 {
        if seq != 256 {
            tune.sample(seq % 256 < 128, seq);
        }
    }
    assert_eq!(tune.infer_dimensions(paws), Some((128, 128)));
}

#[test]
fn a_lost_one_shard_phase_cannot_alias_larger_dimensions() {
    for samples in [
        vec![(0, true), (1, false), (3, false), (4, true), (5, false)],
        vec![
            (4, true),
            (5, false),
            (6, true),
            (7, true),
            (9, true),
            (10, true),
            (11, false),
        ],
    ] {
        let mut tune = AutoTune::new();
        for (seq, is_data) in samples {
            tune.sample(is_data, seq);
        }
        assert_eq!(tune.infer_dimensions(u32::MAX), None);
    }
    let mut tune = AutoTune::new();
    for (seq, is_data) in [
        (0, true),
        (1, false),
        (3, false),
        (4, true),
        (5, false),
        (6, true),
        (7, false),
    ] {
        tune.sample(is_data, seq);
    }
    assert_eq!(tune.infer_dimensions(u32::MAX), Some((1, 1)));
}
