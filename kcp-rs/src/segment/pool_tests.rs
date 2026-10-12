use super::{Segment, SegmentPool, MTU, POOL_MAX_BUFFER_CAPACITY, POOL_MAX_RETAINED_BYTES};
use bytes::BytesMut;

fn assert_accounted(pool: &SegmentPool) {
    assert_eq!(
        pool.retained_bytes,
        pool.inner
            .iter()
            .map(|entry| entry.charged_capacity)
            .sum::<usize>()
    );
    assert!(pool.retained_bytes <= POOL_MAX_RETAINED_BYTES);
    assert!(pool
        .inner
        .iter()
        .all(|entry| entry.charged_capacity <= POOL_MAX_BUFFER_CAPACITY
            && (entry.pending_tail || entry.segment.data.capacity() == entry.charged_capacity)));
}

#[test]
fn ordinary_mtu_full_default_pool_stays_reusable_without_new_segments() {
    let mut pool = SegmentPool::new(4096);
    let segments: Vec<_> = (0..4096).map(|_| pool.acquire()).collect();
    for mut seg in segments {
        seg.len = 1326;
        seg.sn = 45;
        seg.acked = true;
        seg.data.resize(1326, 0x5a);
        pool.release(seg);
    }
    assert_eq!(pool.len(), 4096);
    assert_eq!(pool.retained_bytes, 4096 * MTU);
    assert_accounted(&pool);
    for _ in 0..20_000 {
        let seg = pool.acquire();
        assert_eq!(seg.len, 0);
        assert_eq!(seg.sn, 0);
        assert!(!seg.acked);
        assert!(seg.data.is_empty());
        pool.release(seg);
    }
    assert_eq!(
        pool.created(),
        4096,
        "normal steady-state must keep reusing the pool"
    );
    assert_accounted(&pool);
}

#[test]
fn capacity_budget_does_not_confuse_empty_length_with_released_storage() {
    let mut pool = SegmentPool::new(1024);
    for _ in 0..1024 {
        // Data is empty, but its allocated capacity must still count.
        pool.release(Segment::with_capacity(POOL_MAX_BUFFER_CAPACITY));
    }
    assert_eq!(
        pool.len(),
        POOL_MAX_RETAINED_BYTES / POOL_MAX_BUFFER_CAPACITY
    );
    assert_eq!(pool.retained_bytes, POOL_MAX_RETAINED_BYTES);
    assert_accounted(&pool);
    let seg = pool.acquire();
    assert_eq!(
        pool.retained_bytes,
        POOL_MAX_RETAINED_BYTES - seg.data.capacity()
    );
    pool.release(seg);
    assert_eq!(pool.retained_bytes, POOL_MAX_RETAINED_BYTES);
    assert_accounted(&pool);
}

#[test]
fn oversized_buffers_are_dropped_and_exact_limit_is_reused() {
    let mut pool = SegmentPool::new(2);
    pool.release(Segment::with_capacity(POOL_MAX_BUFFER_CAPACITY + 1));
    assert!(pool.is_empty());
    assert_eq!(pool.retained_bytes, 0);
    pool.release(Segment::with_capacity(POOL_MAX_BUFFER_CAPACITY));
    assert_eq!(pool.len(), 1);
    let mut segment = pool.acquire();
    assert_eq!(segment.data.capacity(), POOL_MAX_BUFFER_CAPACITY);
    // A buffer resized after acquire must be accounted at its new capacity.
    segment.data = BytesMut::with_capacity(POOL_MAX_BUFFER_CAPACITY + 1);
    pool.release(segment);
    assert!(pool.is_empty());
    assert_accounted(&pool);
}

#[test]
fn zero_count_and_object_limit_are_independent_of_byte_limit() {
    let mut empty = SegmentPool::new(0);
    empty.release(Segment::with_capacity(0));
    assert!(empty.is_empty());
    assert_eq!(empty.retained_bytes, 0);
    let mut pool = SegmentPool::new(2);
    for _ in 0..3 {
        pool.release(Segment::with_capacity(MTU));
    }
    assert_eq!(pool.len(), 2);
    assert_eq!(pool.retained_bytes, 2 * MTU);
    assert_accounted(&pool);
}

#[test]
fn mixed_replacements_track_capacity_and_do_not_damage_shared_payload() {
    let mut pool = SegmentPool::new(16);
    let mut segment = Segment::with_capacity(64 * 1024);
    segment.data.resize(64 * 1024 - 8, 0x74);
    let payload = segment.data.split().freeze();
    assert_eq!(segment.data.capacity(), 8);
    pool.release(segment);
    assert!(
        pool.is_empty(),
        "a tiny shared tail must not pin the 64KiB backing"
    );
    assert_eq!(pool.retained_bytes, 0);
    assert_eq!(payload.len(), 64 * 1024 - 8);
    assert!(payload.iter().all(|byte| *byte == 0x74));
    let mut segment = pool.acquire();
    segment.data.extend_from_slice(&[0x45; 1400]);
    pool.release(segment);
    assert!(
        payload.iter().all(|byte| *byte == 0x74),
        "returned Bytes must remain unchanged"
    );
    assert_accounted(&pool);
    drop(payload);
    let mut segment = pool.acquire();
    segment.data = BytesMut::with_capacity(9000);
    pool.release(segment);
    assert_eq!(pool.retained_bytes, 9000);
    assert_accounted(&pool);
}

#[test]
fn unique_small_tail_reclaims_and_exposes_oversized_backing_before_retention() {
    let mut pool = SegmentPool::new(16);
    let mut segment = Segment::with_capacity(64 * 1024);
    segment.data.resize(64 * 1024 - 8, 0x74);
    let payload = segment.data.split().freeze();
    drop(payload);
    assert_eq!(segment.data.capacity(), 8);
    pool.release(segment);
    assert!(
        pool.is_empty(),
        "a unique tail must expose its oversized backing"
    );
    assert_eq!(pool.retained_bytes, 0);
}

#[test]
fn unique_prefix_and_ordinary_unique_tail_restore_the_full_allocation() {
    let mut pool = SegmentPool::new(16);
    let mut segment = Segment::with_capacity(64 * 1024);
    segment.data.resize(64 * 1024, 0x74);
    let mut prefix = Segment::with_capacity(0);
    prefix.data = segment.data.split_to(8);
    drop(segment);
    assert_eq!(prefix.data.capacity(), 8);
    pool.release(prefix);
    assert!(
        pool.is_empty(),
        "a unique small prefix must not hide an oversized allocation"
    );

    let mut segment = Segment::with_capacity(MTU);
    segment.data.resize(1326, 0x74);
    let payload = segment.data.split().freeze();
    drop(payload);
    assert_eq!(segment.data.capacity(), MTU - 1326);
    pool.release(segment);
    assert_eq!(pool.len(), 1);
    assert_eq!(pool.retained_bytes, MTU);
    assert_accounted(&pool);
}

#[test]
fn large_constructor_limit_and_created_counter_cannot_overflow_budget() {
    let mut pool = SegmentPool::new(usize::MAX);
    pool.created = u32::MAX;
    let segment = pool.acquire();
    assert_eq!(pool.created(), u32::MAX);
    pool.release(segment);
    assert_accounted(&pool);
}

#[test]
fn trusted_owned_path_reuses_default_pool_without_promoting_or_reallocating() {
    let mut pool = SegmentPool::new(4096);
    let segments: Vec<_> = (0..4096).map(|_| pool.acquire()).collect();
    for segment in segments {
        pool.release_unique(segment);
    }
    assert_eq!(pool.retained_bytes, 4096 * MTU);
    for _ in 0..20_000 {
        let mut segment = pool.acquire();
        segment.data.resize(1326, 0x45);
        segment.acked = true;
        pool.release_unique(segment);
    }
    assert_eq!(pool.created(), 4096);
    assert_eq!(pool.len(), 4096);
    assert_accounted(&pool);
}

#[test]
fn known_split_tails_reclaim_the_original_buffer_after_payload_drop() {
    for size in [0, 1326, MTU] {
        let mut pool = SegmentPool::new(1);
        for _ in 0..1000 {
            let mut segment = pool.acquire();
            let full_capacity = segment.data.capacity();
            let original_pointer = segment.data.as_ptr();
            segment.data.resize(size, 0x45);
            let payload = segment.data.split_to(size).freeze();
            assert_eq!(segment.data.capacity(), MTU - size);
            pool.release_after_split(segment, full_capacity);
            assert_eq!(pool.retained_bytes, MTU);
            assert_accounted(&pool);
            drop(payload);
            let segment = pool.acquire();
            assert!(segment.data.is_empty());
            assert_eq!(segment.data.capacity(), MTU);
            assert_eq!(segment.data.as_ptr(), original_pointer);
            assert_eq!(pool.retained_bytes, 0);
            pool.release_unique(segment);
        }
        assert_eq!(pool.created(), 1, "zero-capacity tails must also be reused");
    }
}

#[test]
fn held_payload_never_leaves_a_shared_tail_in_an_acquired_segment() {
    for size in [0, 1326, MTU] {
        let mut pool = SegmentPool::new(1);
        let mut segment = pool.acquire();
        let full_capacity = segment.data.capacity();
        segment.data.resize(size, 0x45);
        let payload = segment.data.split_to(size).freeze();
        let payload_pointer = payload.as_ptr();
        pool.release_after_split(segment, full_capacity);
        let mut replacement = pool.acquire();
        assert_eq!(pool.retained_bytes, 0);
        assert_eq!(pool.created(), 2);
        assert_eq!(replacement.data.capacity(), MTU);
        assert_ne!(replacement.data.as_ptr(), payload_pointer);
        replacement.data.resize(MTU, 0x77);
        pool.release_unique(replacement);
        assert_eq!(payload.as_ref(), vec![0x45; size]);
        assert_eq!(pool.retained_bytes, MTU);
        assert_accounted(&pool);
    }
}

#[test]
fn pending_tails_charge_full_backing_and_obey_byte_and_object_budgets() {
    let mut pool = SegmentPool::new(1024);
    let mut held = Vec::new();
    let count = POOL_MAX_RETAINED_BYTES / POOL_MAX_BUFFER_CAPACITY;
    for index in 0..=count {
        let mut segment = Segment::with_capacity(POOL_MAX_BUFFER_CAPACITY);
        let size = POOL_MAX_BUFFER_CAPACITY - if index % 2 == 0 { 0 } else { 8 };
        segment.data.resize(size, 0x45);
        held.push(segment.data.split_to(size).freeze());
        pool.release_after_split(segment, POOL_MAX_BUFFER_CAPACITY);
    }
    assert_eq!(pool.len(), count);
    assert_eq!(pool.retained_bytes, POOL_MAX_RETAINED_BYTES);
    assert_accounted(&pool);
    drop(held);
    for remaining in (0..count).rev() {
        let segment = pool.acquire();
        assert_eq!(segment.data.capacity(), POOL_MAX_BUFFER_CAPACITY);
        assert_eq!(pool.retained_bytes, remaining * POOL_MAX_BUFFER_CAPACITY);
    }
    assert!(pool.is_empty());

    let mut one = SegmentPool::new(1);
    for _ in 0..2 {
        let mut segment = Segment::with_capacity(MTU);
        segment.data.resize(MTU, 0x45);
        let payload = segment.data.split_to(MTU).freeze();
        one.release_after_split(segment, MTU);
        drop(payload);
    }
    assert_eq!(one.len(), 1);
    assert_eq!(one.retained_bytes, MTU);
}

#[test]
fn trusted_oversize_tails_are_rejected_even_when_view_capacity_is_tiny() {
    for keep_payload in [false, true] {
        let mut pool = SegmentPool::new(16);
        let mut segment = Segment::with_capacity(64 * 1024);
        segment.data.resize(64 * 1024 - 8, 0x45);
        let payload = segment.data.split().freeze();
        assert_eq!(segment.data.capacity(), 8);
        let held = keep_payload.then_some(payload);
        pool.release_after_split(segment, 64 * 1024);
        assert!(pool.is_empty());
        assert_eq!(pool.retained_bytes, 0);
        if let Some(payload) = held {
            assert!(payload.iter().all(|byte| *byte == 0x45));
        }
    }
}

#[test]
fn trusted_paths_respect_zero_pool_capacity_and_zero_backing() {
    let mut pool = SegmentPool::new(0);
    pool.release_unique(Segment::with_capacity(MTU));
    let mut segment = Segment::with_capacity(MTU);
    segment.data.resize(MTU, 0x45);
    let payload = segment.data.split().freeze();
    pool.release_after_split(segment, MTU);
    assert!(pool.is_empty());
    assert_eq!(payload.len(), MTU);
    let mut pool = SegmentPool::new(1);
    pool.release_after_split(Segment::with_capacity(0), 0);
    pool.release_unique(Segment::with_capacity(0));
    assert!(pool.is_empty());
    assert_accounted(&pool);
}
