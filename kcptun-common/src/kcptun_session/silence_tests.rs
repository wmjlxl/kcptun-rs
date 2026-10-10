use super::silence_is_fatal;

/// Nothing (no frames, no payload) for the whole window: a dead session.
#[test]
fn silence_past_the_timeout_is_fatal() {
    assert!(silence_is_fatal(90, 90_000, true));
    assert!(silence_is_fatal(90, 120_000, true));
}

/// Recent frames: alive, including just inside the edge.
#[test]
fn recent_frames_keep_the_session() {
    assert!(!silence_is_fatal(90, 400, true));
    assert!(!silence_is_fatal(90, 89_000, true));
    assert!(!silence_is_fatal(90, 0, true));
}

/// Our own receive window is drained: the peer's writer is blocked on us, so
/// its quiet is self-inflicted. Never fatal.
#[test]
fn drained_receive_window_keeps_the_session() {
    assert!(!silence_is_fatal(90, 120_000, false));
}

/// `--keepalivetimeout 0` disables the check entirely.
#[test]
fn disabled_timeout_keeps_the_session() {
    assert!(!silence_is_fatal(0, 600_000, true));
}
