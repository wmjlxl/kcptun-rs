use super::{stall_window_ms, AckStallDetector, ACK_STALL_DEFAULT_SECS, ACK_STALL_NO_RESTART_MS};

/// The window the tests exercise (the binaries default to this value).
const ACK_STALL_MS: u64 = ACK_STALL_DEFAULT_SECS * 1000;

/// Samples arrive every 500 ms in production; the peer is reachable.
const FRESH: u64 = 200;

#[test]
fn fires_after_the_stall_window() {
    let mut det = AckStallDetector::new(7);
    let mut now = 0;
    while now < ACK_STALL_MS {
        assert!(
            !det.sample(now, true, 7, FRESH, true, ACK_STALL_MS),
            "fired early at {now}ms"
        );
        now += 500;
    }
    assert!(det.sample(ACK_STALL_MS, true, 7, FRESH, true, ACK_STALL_MS));
}

#[test]
fn outage_between_peer_and_us_does_not_fire() {
    // Data in flight but the peer stopped talking to us as well: that is a
    // link outage, which KCP retransmission is meant to ride out. The
    // inbound age grows with the outage, so the reachability check fails
    // even after the stall window has elapsed.
    let mut det = AckStallDetector::new(7);
    assert!(!det.sample(0, true, 7, 100, true, ACK_STALL_MS));
    assert!(!det.sample(5_000, true, 7, 5_100, true, ACK_STALL_MS));
    assert!(!det.sample(ACK_STALL_MS, true, 7, 10_100, true, ACK_STALL_MS));
    assert!(!det.sample(20_000, true, 7, 20_100, true, ACK_STALL_MS));
}

#[test]
fn closed_peer_window_is_flow_control_not_desync() {
    // A peer that advertises no room is deliberately not taking data: the
    // frozen snd_una is backpressure (a slow consumer on the far side), so
    // the session must survive even though the peer keeps talking to us.
    let mut det = AckStallDetector::new(7);
    let mut now = 0;
    while now <= ACK_STALL_MS * 3 {
        assert!(
            !det.sample(now, true, 7, FRESH, false, ACK_STALL_MS),
            "fired on flow control at {now}ms"
        );
        now += 500;
    }
}

#[test]
fn acknowledgement_progress_restarts_the_clock() {
    let mut det = AckStallDetector::new(7);
    for now in (0..6_000).step_by(500) {
        assert!(
            !det.sample(now, true, 7, FRESH, true, ACK_STALL_MS),
            "fired early at {now}ms"
        );
    }
    // Peer acknowledged more data: the window starts over from here.
    assert!(!det.sample(6_000, true, 8, FRESH, true, ACK_STALL_MS));
    let mut now = 6_500;
    while now < 16_500 {
        assert!(
            !det.sample(now, true, 8, FRESH, true, ACK_STALL_MS),
            "fired early at {now}ms"
        );
        now += 500;
    }
    assert!(det.sample(16_500, true, 8, FRESH, true, ACK_STALL_MS));
}

#[test]
fn nothing_in_flight_never_fires() {
    let mut det = AckStallDetector::new(7);
    assert!(!det.sample(0, false, 7, FRESH, true, ACK_STALL_MS));
    assert!(!det.sample(600_000, false, 7, FRESH, true, ACK_STALL_MS));
}

/// The fast window applies only with restart evidence; without it the long
/// grace is used, so loss-induced stalls do not tear the session down.
#[test]
fn stall_window_requires_restart_evidence_for_the_fast_path() {
    assert_eq!(stall_window_ms(10_000, true), 10_000);
    assert_eq!(stall_window_ms(10_000, false), ACK_STALL_NO_RESTART_MS);
    // A configured value above the grace is respected either way.
    assert_eq!(stall_window_ms(90_000, false), 90_000);
    assert_eq!(stall_window_ms(90_000, true), 90_000);
    // 0 keeps the check disabled.
    assert_eq!(stall_window_ms(0, true), 0);
    assert_eq!(stall_window_ms(0, false), 0);
}
