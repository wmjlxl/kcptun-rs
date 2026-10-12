//! Lifecycle invariants and a bounded admission-only before/after model.
use super::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::atomic::AtomicUsize;

struct CountingAllocator;
static CAPTURE_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOC_CALLS: AtomicUsize = AtomicUsize::new(0);
static REALLOC_CALLS: AtomicUsize = AtomicUsize::new(0);
static REQUESTED_BYTES: AtomicUsize = AtomicUsize::new(0);
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if CAPTURE_ALLOCATIONS.load(Ordering::Relaxed) {
            ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
            REQUESTED_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        System.alloc(layout)
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if CAPTURE_ALLOCATIONS.load(Ordering::Relaxed) {
            ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
            REQUESTED_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        System.alloc_zeroed(layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if CAPTURE_ALLOCATIONS.load(Ordering::Relaxed) {
            REALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
            REQUESTED_BYTES.fetch_add(size, Ordering::Relaxed);
        }
        System.realloc(ptr, layout, size)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}

struct AllocationCapture;
impl Drop for AllocationCapture {
    fn drop(&mut self) {
        CAPTURE_ALLOCATIONS.store(false, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy)]
struct Measurement {
    ns: u128,
    allocations: usize,
    reallocations: usize,
    requested_bytes: usize,
}
fn measure<T>(work: impl FnOnce() -> T) -> (T, Measurement) {
    let guard = AllocationCapture;
    ALLOC_CALLS.store(0, Ordering::Relaxed);
    REALLOC_CALLS.store(0, Ordering::Relaxed);
    REQUESTED_BYTES.store(0, Ordering::Relaxed);
    CAPTURE_ALLOCATIONS.store(true, Ordering::Relaxed);
    let start = Instant::now();
    let value = work();
    let ns = start.elapsed().as_nanos();
    drop(guard); // Also disables capture if `work` unwinds.
    (
        value,
        Measurement {
            ns,
            allocations: ALLOC_CALLS.load(Ordering::Relaxed),
            reallocations: REALLOC_CALLS.load(Ordering::Relaxed),
            requested_bytes: REQUESTED_BYTES.load(Ordering::Relaxed),
        },
    )
}

fn ip(index: usize) -> IpAddr {
    IpAddr::V6(Ipv6Addr::new(
        0x2001,
        0xdb8,
        0,
        0,
        0,
        0,
        (index >> 16) as u16,
        index as u16,
    ))
}
fn counts() -> IpSessionCounts {
    Arc::new(Mutex::new(HashMap::default()))
}
fn args() -> RxArgs {
    RxArgs {
        socket: Arc::new(knet::DatagramSocket::Udp(
            knet::UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap(),
        )),
        sessions: Arc::new(Mutex::new(HashMap::default())),
        building: Arc::new(Mutex::new(HashMap::default())),
        build_gen: AtomicU64::new(0),
        pending: Arc::new(Mutex::new(VecDeque::new())),
        accept_notify: Arc::new(Notify::new()),
        closed: Arc::new(AtomicBool::new(false)),
        stop: knet::CancellationToken::new(),
        last_error: Arc::new(Mutex::new(None)),
        stats: Arc::new(ListenerStats::default()),
        send_lock: SharedSendLock::new(),
        config: KcpConfig::default(),
        transport_wrapper: None,
        limits: WorkerPoolLimits {
            per_ip_session_rate: 0,
            max_sessions_per_ip: 1,
            ..Default::default()
        },
        ip_limiter: Arc::new(Mutex::new(IpRateLimiter::default())),
        ip_counts: counts(),
        testing_build_delay: Duration::ZERO,
    }
}
fn listener(args: &RxArgs) -> KcpListener {
    KcpListener {
        socket: args.socket.clone(),
        sessions: args.sessions.clone(),
        building: args.building.clone(),
        pending: args.pending.clone(),
        accept_notify: args.accept_notify.clone(),
        closed: args.closed.clone(),
        stop: args.stop.clone(),
        last_error: args.last_error.clone(),
        stats: args.stats.clone(),
        _rx: knet::spawn_task(async {}),
    }
}
fn peer(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}
fn transport(
    args: &RxArgs,
    peer: SocketAddr,
    queue: &Arc<PeerQueue>,
) -> Arc<dyn crate::PacketTransport> {
    Arc::new(PeerTransport {
        queue: queue.clone(),
        socket: args.socket.clone(),
        peer,
        send_lock: Some(args.send_lock.clone()),
    })
}
fn probe() -> Vec<u8> {
    let mut p = vec![0; 24];
    p[..4].copy_from_slice(&42u32.to_le_bytes());
    p[4] = 83;
    p[6..8].copy_from_slice(&32u16.to_le_bytes());
    p
}
fn register(args: &RxArgs, peer: SocketAddr) {
    let queue = Arc::new(PeerQueue::new());
    register_and_spawn_build(
        args,
        peer,
        vec![probe()],
        8,
        queue.clone(),
        transport(args, peer, &queue),
    );
}
async fn wait(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !ready() {
        assert!(Instant::now() < deadline, "condition not reached");
        knet::sleep_ms(2).await;
    }
}
fn manual_build(
    args: &RxArgs,
    peer: SocketAddr,
    generation: u64,
    started: Instant,
) -> Arc<PeerQueue> {
    let queue = Arc::new(PeerQueue::new());
    args.building.lock().insert(
        peer,
        BuildingEntry {
            queue: queue.clone(),
            started,
            generation,
            ip_reservation: Some(
                IpSessionReservation::try_acquire(&args.ip_counts, peer.ip(), 1).unwrap(),
            ),
        },
    );
    queue
}
async fn inert_stream(args: &RxArgs, peer: SocketAddr) -> KcpStream {
    KcpStream::with_transport(args.socket.clone(), peer)
        .connected(false)
        .conv(42)
        .background_input(false)
        .spawn_flush_loop(false)
        .build()
        .await
        .unwrap()
}
fn manual_publish(args: &RxArgs, peer: SocketAddr, conn: KcpStream) {
    args.sessions.lock().insert(
        peer,
        Session {
            conn: conn.clone(),
            _ip_reservation: Some(
                IpSessionReservation::try_acquire(&args.ip_counts, peer.ip(), 1).unwrap(),
            ),
        },
    );
    args.pending.lock().push_back(PendingAccept { conn, peer });
}

#[test]
fn reservation_moves_and_releases_exactly_once() {
    let counts = counts();
    let a = ip(1);
    let b = ip(2);
    let one = IpSessionReservation::try_acquire(&counts, a, 2).unwrap();
    let two = IpSessionReservation::try_acquire(&counts, a, 2).unwrap();
    assert!(IpSessionReservation::try_acquire(&counts, a, 2).is_none());
    let other = IpSessionReservation::try_acquire(&counts, b, 1).unwrap();
    let moved = two;
    assert_eq!(session_count_for_ip(&counts, a), 2);
    drop(one);
    assert_eq!(session_count_for_ip(&counts, a), 1);
    drop(moved);
    assert_eq!(session_count_for_ip(&counts, a), 0);
    drop(other);
    assert!(counts.lock().is_empty());
}
#[test]
fn limiter_preserves_per_ip_windows_and_disabled_rate() {
    let mut limiter = IpRateLimiter::default();
    let now = Instant::now();
    let v4 = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let v6 = ip(1);
    for port in [10, 20] {
        assert!(limiter.allow(SocketAddr::new(v4, port).ip(), 2, now));
    }
    assert!(!limiter.allow(v4, 2, now + Duration::from_millis(999)));
    assert!(limiter.allow(v6, 2, now));
    assert!(limiter.allow(v4, 2, now + IP_LIMITER_WINDOW));
    let size = limiter.entries.len();
    assert!(limiter.allow(ip(99999), 0, now + IP_LIMITER_WINDOW));
    assert_eq!(limiter.entries.len(), size);
}
#[test]
fn limiter_capacity_flood_cannot_reset_an_active_ip_budget() {
    let mut limiter = IpRateLimiter::default();
    let now = Instant::now();
    for i in 0..IP_LIMITER_CAP + 5000 {
        assert_eq!(limiter.allow(ip(i), 1, now), i < IP_LIMITER_CAP);
        assert!(limiter.entries.len() <= IP_LIMITER_CAP);
        assert!(limiter.expiry.len() <= IP_LIMITER_CAP);
    }
    assert!(!limiter.allow(ip(0), 1, now));
    assert!(limiter.allow(ip(IP_LIMITER_CAP + 1), 1, now + IP_LIMITER_WINDOW));
    assert!(limiter.entries.len() <= IP_LIMITER_CAP);
}
#[test]
fn limiter_cleanup_is_budgeted_and_old_records_do_not_expire_new_windows() {
    let mut limiter = IpRateLimiter::default();
    let now = Instant::now();
    for i in 0..IP_LIMITER_CAP {
        assert!(limiter.allow(ip(i), 1, now));
    }
    let next = now + IP_LIMITER_WINDOW;
    assert!(limiter.allow(ip(IP_LIMITER_CAP - 1), 1, next));
    assert_eq!(
        limiter.entries.len(),
        IP_LIMITER_CAP - IP_LIMITER_CLEANUP_BUDGET
    );
    assert_eq!(
        limiter.expiry.len(),
        IP_LIMITER_CAP - IP_LIMITER_CLEANUP_BUDGET + 1
    );
    loop {
        let n = limiter.expire(next, IP_LIMITER_CLEANUP_BUDGET);
        assert!(n <= IP_LIMITER_CLEANUP_BUDGET);
        if n == 0 {
            break;
        }
    }
    assert_eq!(limiter.entries.len(), 1);
    assert_eq!(limiter.expiry.len(), 1);
    assert!(!limiter.allow(ip(IP_LIMITER_CAP - 1), 1, next));
    assert_eq!(
        limiter.expire(next + IP_LIMITER_WINDOW, IP_LIMITER_CLEANUP_BUDGET),
        1
    );
    assert!(limiter.entries.is_empty());
}
#[tokio::test]
async fn second_registration_check_rejects_a_newly_full_ip() {
    let args = args();
    assert!(admit_new_session(&args, peer(100)));
    manual_build(&args, peer(101), 1, Instant::now());
    register(&args, peer(100));
    assert_eq!(args.building.lock().len(), 1);
    assert_eq!(args.stats.session_drops.load(Ordering::Relaxed), 1);
    assert_eq!(session_count_for_ip(&args.ip_counts, peer(100).ip()), 1);
    assert!(listener(&args).remove_peer(peer(101)));
    assert!(args.ip_counts.lock().is_empty());
}
#[tokio::test]
async fn build_failure_releases_the_reserved_ip_slot() {
    let mut args = args();
    args.config.mtu = 49;
    register(&args, peer(100));
    wait(|| args.stats.build_failures.load(Ordering::Relaxed) == 1).await;
    assert!(args.building.lock().is_empty());
    assert!(args.ip_counts.lock().is_empty());
}
#[tokio::test]
async fn publish_remove_and_close_preserve_the_cached_count() {
    let args = args();
    register(&args, peer(100));
    wait(|| args.sessions.lock().len() == 1).await;
    assert!(args.building.lock().is_empty());
    assert_eq!(session_count_for_ip(&args.ip_counts, peer(100).ip()), 1);
    let listener = listener(&args);
    let (conn, _) = listener.try_accept().unwrap().unwrap();
    listener.close();
    assert!(!conn.is_closed());
    assert_eq!(session_count_for_ip(&args.ip_counts, peer(100).ip()), 1);
    assert!(listener.remove_peer(peer(100)));
    assert!(conn.is_closed());
    assert!(args.ip_counts.lock().is_empty());
}
#[tokio::test]
async fn timeout_and_late_failure_do_not_release_a_new_generation() {
    let mut args = args();
    args.limits.building_timeout = Duration::from_secs(1);
    let old = manual_build(&args, peer(100), 1, Instant::now() - Duration::from_secs(2));
    sweep(&args);
    assert!(old.is_closed());
    assert!(args.ip_counts.lock().is_empty());
    manual_build(&args, peer(100), 2, Instant::now());
    args.config.mtu = 49;
    spawn_build_task(
        &args,
        peer(100),
        1,
        old.clone(),
        transport(&args, peer(100), &old),
    );
    wait(|| args.stats.build_failures.load(Ordering::Relaxed) == 1).await;
    assert_eq!(args.building.lock()[&peer(100)].generation, 2);
    assert_eq!(session_count_for_ip(&args.ip_counts, peer(100).ip()), 1);
    listener(&args).remove_peer(peer(100));
    assert!(args.ip_counts.lock().is_empty());
}
#[tokio::test]
async fn cancelled_late_success_cannot_release_the_replacement_reservation() {
    let mut args = args();
    args.testing_build_delay = Duration::from_millis(30);
    let handle = listener(&args);
    let old = manual_build(&args, peer(100), 1, Instant::now());
    spawn_build_task(
        &args,
        peer(100),
        1,
        old.clone(),
        transport(&args, peer(100), &old),
    );
    assert!(handle.remove_peer(peer(100)));
    let new = manual_build(&args, peer(100), 2, Instant::now());
    new.push(probe(), 8);
    spawn_build_task(
        &args,
        peer(100),
        2,
        new.clone(),
        transport(&args, peer(100), &new),
    );
    wait(|| args.pending.lock().len() == 1).await;
    assert_eq!(args.sessions.lock().len(), 1);
    assert_eq!(session_count_for_ip(&args.ip_counts, peer(100).ip()), 1);
    handle.close();
    assert!(args.ip_counts.lock().is_empty());
}
#[tokio::test]
async fn idle_closed_and_stale_reaping_release_the_reservation() {
    for closed in [false, true] {
        let mut args = args();
        args.limits.idle_timeout = Duration::from_millis(1);
        let conn = inert_stream(&args, peer(100)).await;
        if closed {
            conn.close();
        }
        manual_publish(&args, peer(100), conn);
        knet::sleep_ms(3).await;
        sweep(&args);
        assert!(args.sessions.lock().is_empty());
        assert!(args.pending.lock().is_empty());
        assert!(args.ip_counts.lock().is_empty());
    }
    let args = args();
    let conn = inert_stream(&args, peer(100)).await;
    let observed = conn.clone();
    manual_publish(&args, peer(100), conn);
    knet::sleep_ms(2).await;
    let mut foreign = probe();
    foreign[..4].copy_from_slice(&43u32.to_le_bytes());
    for i in 0..3 {
        observed.feed_raw_batch(vec![foreign.clone()]);
        if i < 2 {
            knet::sleep_ms(550).await;
        }
    }
    evict_stale(&args, &mut vec![(peer(100), observed, 0)]);
    assert!(args.sessions.lock().is_empty());
    assert!(args.pending.lock().is_empty());
    assert!(args.ip_counts.lock().is_empty());
}
#[tokio::test]
async fn disabled_ip_cap_does_not_allocate_count_entries() {
    let mut args = args();
    args.limits.max_sessions_per_ip = 0;
    register(&args, peer(100));
    wait(|| args.pending.lock().len() == 1).await;
    assert!(args.ip_counts.lock().is_empty());
    listener(&args).close();
}

fn before_count(
    building: &Mutex<HashMap<SocketAddr, ()>>,
    sessions: &Mutex<HashMap<SocketAddr, ()>>,
    ip: IpAddr,
) -> usize {
    let building_count = building.lock().keys().filter(|p| p.ip() == ip).count();
    let session_count = sessions.lock().keys().filter(|p| p.ip() == ip).count();
    building_count + session_count
}
fn before_allow(
    map: &mut HashMap<IpAddr, (Instant, u32)>,
    ip: IpAddr,
    rate: u32,
    now: Instant,
    scanned: &mut usize,
) -> bool {
    if rate == 0 {
        return true;
    }
    if map.len() > 4096 {
        *scanned += map.len();
        map.retain(|_, (window, _)| now.saturating_duration_since(*window) < IP_LIMITER_WINDOW);
    }
    let entry = map.entry(ip).or_insert((now, 0));
    if now.saturating_duration_since(entry.0) >= IP_LIMITER_WINDOW {
        *entry = (now, 0);
    }
    if entry.1 >= rate {
        return false;
    }
    entry.1 += 1;
    true
}
fn row(
    case: &str,
    peers: usize,
    phase: &str,
    round: usize,
    operations: usize,
    m: Measurement,
    scans: usize,
    table: usize,
    heap: usize,
    accepted: usize,
) {
    println!("BENCH,{case},{peers},{phase},{round},{operations},{},{},{},{},{scans},{table},{heap},{accepted}",m.ns,m.allocations,m.reallocations,m.requested_bytes);
}

#[test]
#[ignore = "bounded admission microbenchmark; run alone in release with --test-threads=1"]
fn bounded_admission_before_after_benchmark() {
    println!("BENCH_HEADER,case,peers,phase,round,operations,elapsed_ns,alloc_calls,realloc_calls,requested_bytes,entry_scans,final_table_len,final_heap_len,accepted_or_checksum");
    const CHECKS: usize = 20_000;
    const NEW_IPS: usize = 4096;
    for peers in [1024usize, 4096] {
        // Pair order B/A, A/B, A/B, B/A avoids always warming up one side first.
        for round in 0..4 {
            let before_first = matches!(round, 0 | 3);
            println!(
                "BENCH_ORDER,{peers},{round},{}",
                if before_first {
                    "before-after"
                } else {
                    "after-before"
                }
            );
            let mut building = HashMap::default();
            let mut sessions = HashMap::default();
            let cached = counts();
            let mut reservations = Vec::with_capacity(peers);
            for n in 0..peers {
                let addr = SocketAddr::new(ip(n % 256), 40000 + (n / 256) as u16);
                if n % 2 == 0 {
                    building.insert(addr, ());
                } else {
                    sessions.insert(addr, ());
                }
                reservations.push(
                    IpSessionReservation::try_acquire(&cached, addr.ip(), usize::MAX).unwrap(),
                );
            }
            let building = Mutex::new(building);
            let sessions = Mutex::new(sessions);
            let run_before = || {
                measure(|| {
                    (0..CHECKS)
                        .map(|i| {
                            black_box(before_count(&building, &sessions, black_box(ip(i % 256))))
                        })
                        .sum::<usize>()
                })
            };
            let run_after = || {
                measure(|| {
                    (0..CHECKS)
                        .map(|i| black_box(session_count_for_ip(&cached, black_box(ip(i % 256)))))
                        .sum::<usize>()
                })
            };
            let ((before, b), (after, a)) = if before_first {
                (run_before(), run_after())
            } else {
                let after = run_after();
                (run_before(), after)
            };
            assert_eq!(before, after);
            row(
                "ip_count",
                peers,
                "before",
                round,
                CHECKS,
                b,
                CHECKS * peers,
                peers,
                0,
                before,
            );
            row(
                "ip_count",
                peers,
                "after",
                round,
                CHECKS,
                a,
                0,
                cached.lock().len(),
                0,
                after,
            );
            let (_, update) = measure(|| {
                for i in 0..CHECKS {
                    drop(
                        IpSessionReservation::try_acquire(
                            &cached,
                            black_box(ip(i % 256)),
                            usize::MAX,
                        )
                        .unwrap(),
                    );
                }
            });
            row(
                "ip_count_lease_update",
                peers,
                "after",
                round,
                CHECKS,
                update,
                0,
                cached.lock().len(),
                0,
                0,
            );
            drop(reservations);
            assert!(cached.lock().is_empty());

            let now = Instant::now();
            let mut old = HashMap::default();
            let mut new = IpRateLimiter::default();
            for i in 0..peers {
                old.insert(ip(i), (now, 1));
                assert!(new.allow(ip(i), 1, now));
            }
            let mut scans = 0;
            let mut run_before = || {
                measure(|| {
                    (peers..peers + NEW_IPS)
                        .filter(|&i| before_allow(&mut old, black_box(ip(i)), 1, now, &mut scans))
                        .count()
                })
            };
            let mut run_after = || {
                measure(|| {
                    (peers..peers + NEW_IPS)
                        .filter(|&i| new.allow(black_box(ip(i)), 1, now))
                        .count()
                })
            };
            let ((before, b), (after, a)) = if before_first {
                (run_before(), run_after())
            } else {
                let after = run_after();
                (run_before(), after)
            };
            row(
                "ip_limiter_fresh_flood",
                peers,
                "before",
                round,
                NEW_IPS,
                b,
                scans,
                old.len(),
                0,
                before,
            );
            row(
                "ip_limiter_fresh_flood",
                peers,
                "after",
                round,
                NEW_IPS,
                a,
                0,
                new.entries.len(),
                new.expiry.len(),
                after,
            );
            assert!(new.entries.len() <= IP_LIMITER_CAP);
            assert!(new.expiry.len() <= IP_LIMITER_CAP);
        }
    }
}
