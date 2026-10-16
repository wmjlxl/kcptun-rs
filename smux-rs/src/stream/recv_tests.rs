use super::*;
use std::sync::Barrier;

#[test]
fn recycled_stream_rejects_late_input_and_returns_tokens_once() {
    let stream = Arc::new(Stream::with_buffer(1, 8));
    stream
        .push_data_bytes(Bytes::from_static(b"first"))
        .unwrap();
    let gate = Arc::new(Barrier::new(2));
    let late_stream = stream.clone();
    let late_gate = gate.clone();
    let late = std::thread::spawn(move || {
        late_gate.wait();
        late_stream.push_data_bytes(Bytes::from_static(b"late"))
    });
    // Once the stream is removed, a previously fetched Arc cannot accept
    // another packet or create receive tokens that nobody can reclaim.
    assert_eq!(stream.recycle_tokens(), 5);
    gate.wait();
    assert_eq!(late.join().unwrap(), Err(StreamError::Closed));
    assert_eq!(stream.recycle_tokens(), 0);
    assert_eq!(stream.available(), 0);
}

#[test]
fn local_half_close_does_not_discard_inbound_tail() {
    let stream = Stream::with_buffer(1, 8);
    stream.mark_local_closed();
    stream
        .push_data_bytes(Bytes::from_static(b"reply"))
        .unwrap();
    let mut output = [0; 5];
    assert_eq!(stream.read(&mut output).unwrap().0, 5);
    assert_eq!(&output, b"reply");
    assert_eq!(stream.take_return_tokens(), 5);
}

/// A rejected push must not move the byte counter: the session charges the
/// bytes before the push and returns them on error, so a counter that counted
/// a rejected packet would make the receive window shrink for data that was
/// never buffered.
#[test]
fn overflow_push_is_not_charged() {
    let stream = Stream::with_buffer(1, 8);
    assert!(stream.push_data_bytes(Bytes::from_static(b"12345")).is_ok());
    // 6 more bytes would exceed the 8-byte window: rejected, counter untouched.
    assert_eq!(
        stream.push_data_bytes(Bytes::from_static(b"678901")),
        Err(StreamError::BufferOverflow)
    );
    // Exactly the remaining space is still accepted.
    assert!(stream.push_data_bytes(Bytes::from_static(b"678")).is_ok());
    assert_eq!(stream.available(), 8);
    assert_eq!(stream.recycle_tokens(), 8);
}

/// push ↔ read interleaving: the counter must equal what is queued at every
/// point, and reads must hand back exactly what they consumed (so
/// consumed == returned + still-buffered holds after the dust settles).
#[test]
fn concurrent_push_and_read_keep_the_counter_in_step() {
    const PACKETS: usize = 2_000;
    let stream = Arc::new(Stream::with_buffer(1, 64 * 1024));
    let producer = {
        let stream = stream.clone();
        std::thread::spawn(move || {
            for i in 0..PACKETS {
                let payload = Bytes::from(vec![(i % 251) as u8; 3]);
                // A full window is impossible here (max 6 KB queued), so any
                // error is a real failure rather than backpressure.
                stream.push_data_bytes(payload).expect("push");
            }
        })
    };

    let mut consumed = 0usize;
    let mut buf = [0u8; 7];
    let start = std::time::Instant::now();
    while consumed < PACKETS * 3 && start.elapsed() < std::time::Duration::from_secs(30) {
        match stream.read(&mut buf) {
            Ok((0, _)) => std::thread::yield_now(),
            Ok((n, _)) => consumed += n,
            Err(StreamError::WouldBlock) => std::thread::yield_now(),
            Err(e) => panic!("read failed: {e:?}"),
        }
    }
    producer.join().unwrap();
    assert_eq!(consumed, PACKETS * 3, "every pushed byte must be readable");

    // No byte is both returned and still queued: the remaining queue is empty
    // and the counter matches it exactly.
    assert_eq!(stream.available(), 0);
    let returned = stream.take_return_tokens();
    assert_eq!(returned, consumed, "returned exactly what was consumed");
    assert_eq!(stream.recycle_tokens(), 0, "nothing left to reclaim");
}

/// push ↔ close interleaving: `close()` clears the queue and moves the charge
/// to `tokens_to_return` under the same lock, so a push that lands before the
/// close is reclaimable exactly once and a push after it is rejected.
#[test]
fn close_during_push_returns_each_token_once() {
    for _ in 0..200 {
        let stream = Arc::new(Stream::with_buffer(1, 64));
        let gate = Arc::new(Barrier::new(2));
        let pusher = {
            let stream = stream.clone();
            let gate = gate.clone();
            std::thread::spawn(move || {
                gate.wait();
                stream.push_data_bytes(Bytes::from_static(b"0123456789"))
            })
        };
        gate.wait();
        stream.close();
        let pushed = pusher.join().unwrap();
        let dropped_by_close = stream.take_return_tokens();
        // Either the push won (10 buffered, returned by close) or the push
        // lost (Closed, nothing buffered) — never both.
        match pushed {
            Ok(()) => assert_eq!(dropped_by_close, 10),
            Err(StreamError::Closed) => assert_eq!(dropped_by_close, 0),
            Err(e) => panic!("unexpected push error: {e:?}"),
        }
        // The final reclaim path sees a zero counter either way.
        assert_eq!(stream.recycle_tokens(), 0);
        assert_eq!(stream.available(), 0);
    }
}
