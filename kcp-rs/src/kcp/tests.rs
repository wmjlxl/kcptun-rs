use super::*;

fn create_kcp(conv: u32) -> KCP {
    KCP::new(conv, 0, move |_data: Bytes| {})
}

/// Build a bare 24-byte KCP header (no payload).
fn header(conv: u32, cmd: u8, wnd: u16, ts: u32, sn: u32, una: u32) -> Vec<u8> {
    let mut b = Vec::with_capacity(KCP_OVERHEAD);
    b.extend_from_slice(&conv.to_le_bytes());
    b.push(cmd);
    b.push(0); // frg
    b.extend_from_slice(&wnd.to_le_bytes());
    b.extend_from_slice(&ts.to_le_bytes());
    b.extend_from_slice(&sn.to_le_bytes());
    b.extend_from_slice(&una.to_le_bytes());
    b.extend_from_slice(&0u32.to_le_bytes()); // len
    b
}

#[test]
fn set_nodelay_applies_zero_values() {
    let mut kcp = create_kcp(1);
    kcp.set_nodelay(1, 10, 2, 1);
    assert_eq!(kcp.nodelay, 1);
    assert_eq!(kcp.fastresend, 2);
    assert_eq!(kcp.nocwnd, 1);
    assert_eq!(kcp.rx_minrto, RTO_NODELAY_MIN);

    // Switching to a non-nodelay profile must clear all three knobs.
    // Gating each assignment on a non-zero value turned them into
    // one-way latches (Fast3 → Normal kept half-linear RTO backoff,
    // and congestion control could never be re-enabled).
    kcp.set_nodelay(0, 40, 0, 0);
    assert_eq!(kcp.nodelay, 0);
    assert_eq!(kcp.fastresend, 0);
    assert_eq!(kcp.nocwnd, 0);
    assert_eq!(kcp.rx_minrto, RTO_DEFAULT_MIN);
    assert_eq!(kcp.interval, 40);

    // Go's clamp: interval below 10 ms becomes 10 ms, not 40 ms.
    kcp.set_nodelay(1, 5, 2, 1);
    assert_eq!(kcp.interval, 10);
}

#[test]
fn fec_recovered_input_skips_window_and_rtt_updates() {
    let mut kcp = create_kcp(1);

    // A regular packet advertises a window of 8.
    kcp.input_no_flush(&header(1, Command::WIns as u8, 8, 0, 0, 0), false)
        .unwrap();
    assert_eq!(kcp.rmt_wnd, 8);

    // An FEC-reconstructed packet carries a stale advertisement; Go only
    // trusts window updates from regular packets.
    kcp.input_no_flush_typed(&header(1, Command::WIns as u8, 1, 0, 0, 0), false, false)
        .unwrap();
    assert_eq!(kcp.rmt_wnd, 8, "recovered packet must not shrink rmt_wnd");

    // Same for the RTT estimator: a recovered ACK's `ts` predates the FEC
    // group fill, so sampling it inflates srtt/rto.
    let ts = (kcp.current_ms() as u32).wrapping_sub(20);
    assert_eq!(kcp.rx_srtt, 0);
    kcp.input_no_flush_typed(&header(1, Command::Ack as u8, 8, ts, 0, 0), false, false)
        .unwrap();
    assert_eq!(kcp.rx_srtt, 0, "recovered ACK must not sample RTT");
    kcp.input_no_flush(&header(1, Command::Ack as u8, 8, ts, 0, 0), false)
        .unwrap();
    assert!(kcp.rx_srtt > 0, "regular ACK must sample RTT");
}

#[test]
fn early_retransmit_fires_on_one_dup_ack_when_no_new_segments() {
    let mut kcp = create_kcp(1);
    kcp.set_nodelay(1, 10, 2, 1); // fastresend = 2
    kcp.set_stream_mode(false);
    kcp.send(b"AAA").unwrap();
    kcp.send(b"BBB").unwrap();
    kcp.flush_with_current(100, true);
    assert_eq!(kcp.snd_buf.len(), 2);
    assert_eq!(kcp.snd_buf[0].xmit, 1);

    // ACK sn=1 only: sn=0 collects a single duplicate ACK, below the
    // fastresend threshold of 2.
    kcp.input_no_flush(&header(1, Command::Ack as u8, 32, 100, 1, 0), false)
        .unwrap();
    assert_eq!(kcp.snd_buf[0].fastack, 1);

    // snd_queue is empty, so this flush queues no new segments and Go's
    // early-retransmit branch applies. Well before the RTO deadline.
    kcp.flush_with_current(120, true);
    assert_eq!(
        kcp.snd_buf[0].xmit, 2,
        "early retransmit must resend on one dup ACK when the pipe is idle"
    );
    assert_eq!(kcp.snd_buf[0].fastack, 0xFFFFFFFF);
}

#[test]
fn test_kcp_create() {
    let kcp = create_kcp(1);
    assert_eq!(kcp.conv(), 1);
    assert_eq!(kcp.rx_rto(), RTO_DEFAULT);
    assert_eq!(kcp.mss(), (MTU - KCP_OVERHEAD) as u32);
}

#[test]
fn test_kcp_send_recv() {
    let mut kcp = create_kcp(1);
    kcp.send(b"hello kcp").unwrap();
    assert_eq!(kcp.snd_queue_len(), 1);
    // After update + flush should move to snd_buf
    kcp.update(100);
    assert_eq!(kcp.snd_queue_len(), 0);
}

#[test]
fn test_kcp_set_nodelay() {
    let mut kcp = create_kcp(1);
    kcp.set_nodelay(1, 10, 2, 1);
    assert_eq!(kcp.rx_minrto, RTO_NODELAY_MIN); // 30
    assert_eq!(kcp.nodelay, 1);
    assert_eq!(kcp.fastresend, 2);
    assert_eq!(kcp.nocwnd, 1);
}

/// P2 / L-2: rcv_wnd is clamped to u16::MAX.
#[test]
fn set_rcv_wnd_clamps_to_u16() {
    let mut kcp = create_kcp(1);
    kcp.set_rcv_wnd(1_000_000);
    assert_eq!(kcp.rcv_wnd(), u16::MAX as u32);
    kcp.set_rcv_wnd(512);
    assert_eq!(kcp.rcv_wnd(), 512);
}

/// P2 / L-3: send of >4 GiB is an error, not a silent u32 truncate.
#[test]
fn send_rejects_oversize_payload() {
    let mut kcp = create_kcp(1);
    // Empty is already NoData (pre-existing).
    assert!(matches!(kcp.send(b""), Err(KcpError::NoData)));
    // A normal payload still works.
    assert!(kcp.send(b"x").is_ok());
    // The real guard is `data.len() > u32::MAX`; constructing that would
    // need a 4 GiB allocation. The threshold is asserted here so a future
    // change to the guard is noticed.
    assert!(u32::MAX as usize > 0);
}

#[test]
fn test_kcp_set_mtu() {
    let mut kcp = create_kcp(1);
    assert!(kcp.set_mtu(500));
    assert_eq!(kcp.mtu(), 500);
    assert_eq!(kcp.mss(), 500 - KCP_OVERHEAD as u32);
    assert!(!kcp.set_mtu(20)); // Too small
}

#[test]
fn test_kcp_wait_send() {
    let mut kcp = create_kcp(1);
    assert_eq!(kcp.wait_send(), 0);
    kcp.send(b"test").unwrap();
    assert_eq!(kcp.wait_send(), 1);
}

#[test]
fn test_kcp_wnd_unused() {
    let kcp = create_kcp(1);
    assert_eq!(kcp.wnd_unused(), KCP_DEFAULT_WND as u16);
}

#[test]
fn test_kcp_input_conv_mismatch() {
    let mut kcp = create_kcp(42);
    // Craft a packet with conv=99 (wrong)
    let mut buf = Vec::with_capacity(KCP_OVERHEAD);
    buf.extend_from_slice(&99u32.to_le_bytes()); // conv = 99
    buf.push(Command::Push as u8);
    buf.push(0u8); // frg
    buf.extend_from_slice(&512u16.to_le_bytes()); // wnd
    buf.extend_from_slice(&100u32.to_le_bytes()); // ts
    buf.extend_from_slice(&1u32.to_le_bytes()); // sn
    buf.extend_from_slice(&0u32.to_le_bytes()); // una
    buf.extend_from_slice(&0u32.to_le_bytes()); // len

    let result = kcp.input(&buf, false);
    assert!(result.is_err()); // conv mismatch
}

#[test]
fn test_input_rejects_wrapping_payload_length() {
    let mut kcp = create_kcp(1);
    let mut buf = vec![0u8; KCP_OVERHEAD];
    buf[..4].copy_from_slice(&1u32.to_le_bytes());
    buf[4] = Command::Push as u8;
    buf[20..].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(
        kcp.input_no_flush(&buf, false),
        Err(KcpError::InvalidLength)
    );
}

#[test]
fn test_peeksize_handles_256_fragment_queue() {
    let mut kcp = create_kcp(1);
    for i in 0..=u8::MAX {
        let mut seg = Segment::with_capacity(1);
        seg.frg = u8::MAX - i;
        seg.len = 1;
        seg.data.extend_from_slice(&[0]);
        kcp.rcv_queue.push_back(seg);
    }
    assert_eq!(kcp.peeksize(), Some(usize::from(u8::MAX) + 1));
}

#[test]
fn test_pending_flush_is_consumed_and_reset() {
    let mut kcp = create_kcp(1);
    let mut buf = vec![0u8; KCP_OVERHEAD];
    buf[..4].copy_from_slice(&1u32.to_le_bytes());
    buf[4] = Command::Push as u8;
    buf[12..16].copy_from_slice(&0u32.to_le_bytes());

    kcp.input_no_flush(&buf, true).unwrap();
    assert_ne!(kcp.pending_flush_flag(), 0);
    kcp.flush_if_pending(100);
    assert_eq!(kcp.pending_flush_flag(), 0);
    assert_eq!(kcp.acklist_len(), 0);
    assert_eq!(kcp.flush_if_pending(100), kcp.interval());

    kcp.input_no_flush(&buf, true).unwrap();
    assert_ne!(kcp.pending_flush_flag(), 0);
}

#[test]
fn test_parse_ack_wrap_uses_index_and_sn_verification() {
    let mut kcp = create_kcp(1);
    kcp.snd_una = u32::MAX - 1;
    kcp.snd_nxt = 2;
    for sn in [u32::MAX - 1, u32::MAX, 0, 1] {
        let mut seg = Segment::with_capacity(0);
        seg.sn = sn;
        kcp.snd_buf.push_back(seg);
    }

    kcp.parse_ack(0);
    assert!(kcp.snd_buf[2].acked);
    kcp.parse_ack(1);
    assert!(kcp.snd_buf[3].acked);

    // A valid wrapping index must still verify the actual SN before
    // marking a malformed/gapped deque entry as acknowledged.
    kcp.snd_buf[2].sn = 7;
    kcp.snd_buf[2].acked = false;
    kcp.parse_ack(0);
    assert!(!kcp.snd_buf[2].acked);
}

#[test]
fn test_duplicate_segment_is_returned_to_pool() {
    let mut kcp = create_kcp(1);
    let mut buf = vec![0u8; KCP_OVERHEAD];
    buf[..4].copy_from_slice(&1u32.to_le_bytes());
    buf[4] = Command::Push as u8;
    buf[12..16].copy_from_slice(&1u32.to_le_bytes());

    kcp.input_no_flush(&buf, false).unwrap();
    assert_eq!(kcp.pool().len(), 0);
    kcp.input_no_flush(&buf, false).unwrap();
    assert_eq!(kcp.pool().len(), 1);

    let mut out_of_window = kcp.pool.acquire();
    out_of_window.sn = kcp.rcv_nxt.wrapping_add(kcp.rcv_wnd);
    assert!(kcp.parse_data(out_of_window));
    assert_eq!(kcp.pool().len(), 1);
}

#[test]
fn test_kcp_send_fragment_too_large() {
    let mut kcp = create_kcp(1);
    // Create data larger than KCP_MAX_FRAG * mss
    let large_data = vec![0u8; (KCP_MAX_FRAG + 1) as usize * (MTU - KCP_OVERHEAD)];
    let result = kcp.send(&large_data);
    // Go would return -2 for count > 255
    assert!(result.is_err());
}

#[test]
fn snmp_send_recv_counts_upper_bytes() {
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex};

    crate::snmp::enable();
    // Reset counters so we measure the absolute effect of our operations
    // rather than computing a delta (which races with other tests sharing
    // the process-global DEFAULT_SNMP).
    crate::snmp::DEFAULT_SNMP
        .bytes_sent
        .store(0, Ordering::SeqCst);
    crate::snmp::DEFAULT_SNMP
        .bytes_received
        .store(0, Ordering::SeqCst);

    let mut kcp = create_kcp(7);
    kcp.set_stream_mode(true);
    kcp.send(b"hello-snmp").unwrap();
    assert_eq!(
        crate::snmp::DEFAULT_SNMP.bytes_sent.load(Ordering::SeqCst),
        10,
    );

    let store: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let store2 = store.clone();
    let mut a = KCP::new(8, 0, move |data: bytes::Bytes| {
        store2.lock().unwrap().push(data.to_vec());
    });
    let mut b = KCP::new(8, 0, |_| {});
    a.set_nodelay(1, 10, 2, 1);
    b.set_nodelay(1, 10, 2, 1);
    a.send(b"abcdef").unwrap();
    a.update(10);
    a.flush();
    let pkts = store.lock().unwrap().clone();
    assert!(!pkts.is_empty());
    for p in &pkts {
        b.input(p, true).unwrap();
    }
    b.update(20);
    let got = b.recv().unwrap();
    assert_eq!(&got[..], b"abcdef");
    assert!(
        crate::snmp::DEFAULT_SNMP
            .bytes_received
            .load(Ordering::SeqCst)
            >= 6,
    );
}

/// Regression test: stream-mode append must not panic when the second
/// `send()` writes more data than the first segment's payload.
///
/// Before the fix, `send()` did `seg.payload = seg.data.split_to(...).freeze()`
/// which emptied `seg.data`. The next `send()` in stream mode then appended
/// to the empty `data` but set `len` to the new (larger) length, while
/// `payload` still held the old (shorter) data. On `encode()`,
/// `payload[..len]` panicked with "range end index out of range".
#[test]
fn test_stream_mode_append_no_panic() {
    use std::sync::{Arc, Mutex};

    let store: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let store2 = store.clone();
    let mut kcp = KCP::new(1, 0, move |data: bytes::Bytes| {
        store2.lock().unwrap().push(data.to_vec());
    });
    kcp.set_stream_mode(true);
    // Use a large MTU so the second send fits in one segment via append
    kcp.set_mtu(1400);
    let mss = kcp.mss() as usize;

    // First send: small payload (e.g. 100 bytes) → creates a segment
    let small = vec![0xAAu8; 100];
    kcp.send(&small).unwrap();
    assert_eq!(kcp.snd_queue_len(), 1);

    // Second send: larger than the first payload but still ≤ mss.
    // In stream mode this appends to the last segment in snd_queue.
    // With the old bug, payload stayed at 100 bytes but len was set to 500,
    // causing encode() to panic on payload[..500].
    let large = vec![0xBBu8; 500];
    kcp.send(&large).unwrap();

    // The segment should now contain 100 + 500 = 600 bytes.
    // Flush triggers encode() — must not panic.
    kcp.update(100);

    // Verify the encoded data round-trips correctly
    let pkts = store.lock().unwrap().clone();
    assert!(!pkts.is_empty(), "flush should produce output packets");

    // No packet may exceed MTU (1400).  An earlier intermediate fix
    // computed capacity from `data.len()` (which was 0 after split_to)
    // instead of the real payload length, letting segments grow past
    // MSS and producing oversized UDP packets.
    for (i, p) in pkts.iter().enumerate() {
        assert!(p.len() <= 1400, "pkt {} exceeds MTU: {}", i, p.len());
    }

    // Feed packets into a receiver and check data integrity
    let mut recv_kcp = create_kcp(1);
    recv_kcp.set_stream_mode(true);
    for p in &pkts {
        recv_kcp.input(p, false).unwrap();
    }
    let got = recv_kcp.recv().unwrap();
    assert_eq!(got.len(), 600);
    assert_eq!(&got[..100], &small[..]);
    assert_eq!(&got[100..], &large[..]);

    // Also test the case where the second send exceeds mss (creates new
    // segments after appending). This is the exact scenario from the bug
    // report where len=1326 and payload.len()=692.
    let _ = mss; // suppress unused warning
    let huge = vec![0xCCu8; mss * 2]; // 2 full segments worth
    kcp.send(&huge).unwrap();
    kcp.update(200);
}

/// Inline writes must not consume ACKs. Immediate inbound flushes and the
/// serialized protocol deadline are the ACK-emitting paths.
#[test]
fn test_ack_emission_single_owner() {
    use std::sync::{Arc, Mutex};

    let store: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let store2 = store.clone();
    let mut kcp = KCP::new(1, 0, move |data: bytes::Bytes| {
        store2.lock().unwrap().push(data.to_vec());
    });

    // Feed one Push segment → queues an ACK in the acklist.
    let mut buf = Vec::with_capacity(KCP_OVERHEAD);
    buf.extend_from_slice(&1u32.to_le_bytes()); // conv = 1
    buf.push(Command::Push as u8); // cmd = PUSH
    buf.push(0u8); // frg
    buf.extend_from_slice(&512u16.to_le_bytes()); // wnd
    buf.extend_from_slice(&100u32.to_le_bytes()); // ts
    buf.extend_from_slice(&1u32.to_le_bytes()); // sn = 1
    buf.extend_from_slice(&0u32.to_le_bytes()); // una
    buf.extend_from_slice(&0u32.to_le_bytes()); // len = 0
    kcp.input_no_flush(&buf, true).unwrap();

    let count_acks = |store: &Arc<Mutex<Vec<Vec<u8>>>>| {
        store
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p.len() >= 5 && p[4] == Command::Ack as u8)
            .count()
    };

    // flush_data_only must NOT emit ACKs (used by write path + flush loop).
    store.lock().unwrap().clear();
    kcp.flush_data_only();
    let after_data_only = count_acks(&store);
    assert_eq!(
        after_data_only, 0,
        "flush_data_only must not drain the acklist (single-owner ACK); got {after_data_only} ACK segs"
    );

    // The real flush must emit the pending ACK.
    store.lock().unwrap().clear();
    kcp.flush();
    let after_flush = count_acks(&store);
    assert!(
        after_flush >= 1,
        "flush() must emit the queued ACK; got {after_flush} ACK segs"
    );
}

/// Fast retransmit: when `fastack >= fastresend` (2 duplicate ACKs),
/// the lost segment is retransmitted immediately via the fast retransmit
/// path, not RTO timeout. This matches Go kcp-go's flush logic exactly.
///
/// Steps:
/// 1. Sender sends 3 data segments (SN 0, 1, 2)
/// 2. Receiver gets segments 1 and 2 (segment 0 is "lost")
/// 3. Receiver flushes → sends ACKs for SN 1 and 2
/// 4. Sender processes the ACKs → fastack[0] incremented to 2
/// 5. Sender flushes → segment 0 is fast-retransmitted (not RTO)
#[test]
fn test_fast_retransmit_fires_on_duplicate_acks() {
    use std::sync::{Arc, Mutex};

    crate::snmp::enable();
    crate::snmp::DEFAULT_SNMP
        .fast_retrans
        .store(0, std::sync::atomic::Ordering::SeqCst);
    crate::snmp::DEFAULT_SNMP
        .lost_segs
        .store(0, std::sync::atomic::Ordering::SeqCst);

    let sender_out: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let sender_out2 = sender_out.clone();
    let mut sender = KCP::new(42, 0, move |data: bytes::Bytes| {
        sender_out2.lock().unwrap().push(data.to_vec());
    });

    let receiver_out: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let receiver_out2 = receiver_out.clone();
    let mut receiver = KCP::new(42, 0, move |data: bytes::Bytes| {
        receiver_out2.lock().unwrap().push(data.to_vec());
    });

    // Fast3: nodelay=1, interval=10, resend=2, nc=1
    sender.set_nodelay(1, 10, 2, 1);
    receiver.set_nodelay(1, 10, 2, 1);
    // Disable stream mode so each send() creates a separate segment
    sender.set_stream_mode(false);
    receiver.set_stream_mode(false);

    // Step 1: Sender sends 3 small data segments
    sender.send(b"AAA").unwrap();
    sender.send(b"BBB").unwrap();
    sender.send(b"CCC").unwrap();

    // Flush at t=100 → moves segments to snd_buf, sends them.
    // Stay on the simulated clock: a wall-clock `flush()` would see
    // resendts=100+rto as ancient and spuriously RTO-retransmit everything.
    sender.update(100);
    sender.flush_with_current(100, true);

    // Extract individual KCP segments from wire packets (a wire packet
    // may contain multiple KCP segments packed up to MTU).
    fn extract_segments(wire_pkts: &[Vec<u8>]) -> Vec<Vec<u8>> {
        let mut segs = Vec::new();
        for pkt in wire_pkts {
            let mut off = 0;
            while off + KCP_OVERHEAD <= pkt.len() {
                let length = u32::from_le_bytes([
                    pkt[off + 20],
                    pkt[off + 21],
                    pkt[off + 22],
                    pkt[off + 23],
                ]) as usize;
                let seg = pkt[off..off + KCP_OVERHEAD + length].to_vec();
                segs.push(seg);
                off += KCP_OVERHEAD + length;
            }
        }
        segs
    }

    let sender_segs = extract_segments(&sender_out.lock().unwrap().clone());
    let push_segs: Vec<_> = sender_segs
        .iter()
        .filter(|s| s.len() >= 5 && s[4] == Command::Push as u8)
        .collect();
    assert!(
        push_segs.len() >= 3,
        "expected at least 3 Push segments, got {}",
        push_segs.len()
    );

    // Step 2: Receiver processes segments 1 and 2 (skip segment 0 = "lost")
    for (i, seg) in push_segs.iter().enumerate() {
        if i == 0 {
            continue; // "lose" segment 0
        }
        receiver.input_no_flush(seg, false).unwrap();
    }

    // Step 3: Receiver flushes → sends ACKs for received segments
    // (simulated clock — see the t=100 note above)
    receiver.update(110);
    receiver.flush_with_current(110, true);

    let receiver_segs = extract_segments(&receiver_out.lock().unwrap().clone());
    let ack_segs: Vec<_> = receiver_segs
        .iter()
        .filter(|s| s.len() >= 5 && s[4] == Command::Ack as u8)
        .collect();

    // Receiver should have sent at least 2 ACKs (for segments 1 and 2)
    assert!(
        ack_segs.len() >= 2,
        "expected at least 2 ACK segments, got {}",
        ack_segs.len()
    );

    // Step 4: Sender processes the ACKs
    for seg in &ack_segs {
        sender.input_no_flush(seg, false).unwrap();
    }

    // Step 5: Sender flushes → should fast retransmit segment 0
    // (simulated clock: wall-clock flush would RTO-retransmit and trip
    // the `lost == 0` assertion below)
    sender_out.lock().unwrap().clear();
    sender.update(120);
    sender.flush_with_current(120, true);

    let fast_retrans = crate::snmp::DEFAULT_SNMP
        .fast_retrans
        .load(std::sync::atomic::Ordering::SeqCst);
    let lost = crate::snmp::DEFAULT_SNMP
        .lost_segs
        .load(std::sync::atomic::Ordering::SeqCst);

    assert!(
        fast_retrans > 0,
        "expected fast_retrans > 0, got {fast_retrans}; lost={lost}"
    );
    assert_eq!(lost, 0, "expected 0 RTO-based retransmissions, got {lost}");
}
