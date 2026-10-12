//! Bounded FEC decoder comparison. Packet construction and codec construction
//! are outside the measured region; every recovered payload is checked.
use kcp_rs::fec::{fec_kcp_from_recovered, FecDecoder, FecEncoder};
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

struct CountedAllocator;
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(size as u64, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountedAllocator = CountedAllocator;

struct Fixture {
    packets: Vec<Vec<Vec<u8>>>,
    missing: Vec<Vec<Vec<u8>>>,
    packet_count: usize,
    missing_count: usize,
}

fn fixture(data: usize, parity: usize, groups: usize, case: &str) -> Fixture {
    let lost = if case.starts_with("loss") {
        (data / 8).max(1).min(parity)
    } else {
        0
    };
    let mut encoder = FecEncoder::new(data, parity, 0).unwrap();
    let mut packets = Vec::with_capacity(groups);
    let mut missing = Vec::with_capacity(groups);
    let mut packet_count = 0;
    for group in 0..groups {
        let mut frames = Vec::with_capacity(data + parity);
        let mut parity_frames = Vec::new();
        let mut expected = Vec::with_capacity(lost);
        for slot in 0..data {
            let mut payload = vec![((group * 17 + slot) % 251) as u8; 512 - slot % 31];
            payload[..8].copy_from_slice(&(group as u64).to_le_bytes());
            payload[8..12].copy_from_slice(&(slot as u32).to_le_bytes());
            let (frame, generated) = encoder.wrap_kcp_packet(&payload, u32::MAX);
            parity_frames.extend(generated);
            if slot < lost {
                expected.push(payload);
            } else {
                frames.push(frame);
            }
        }
        assert_eq!(parity_frames.len(), parity);
        if case != "ordered" {
            frames.reverse();
            parity_frames.reverse();
        }
        frames.extend(parity_frames);
        packet_count += frames.len();
        packets.push(frames);
        missing.push(expected);
    }
    Fixture {
        packets,
        missing,
        packet_count,
        missing_count: lost * groups,
    }
}

fn measure(label: &str, data: usize, parity: usize, case: &str, f: &Fixture, repeat: usize) {
    let mut decoder = FecDecoder::new(data, parity).unwrap();
    let allocs_before = ALLOCS.load(Ordering::Relaxed);
    let bytes_before = ALLOC_BYTES.load(Ordering::Relaxed);
    let started = Instant::now();
    let mut recovered_count = 0;
    for (group, packets) in f.packets.iter().enumerate() {
        let mut recovered_in_group = 0;
        for packet in packets {
            for recovered in decoder.decode(black_box(packet)) {
                let payload = fec_kcp_from_recovered(&recovered).unwrap();
                assert_eq!(payload, f.missing[group][recovered_in_group]);
                recovered_in_group += 1;
            }
        }
        assert_eq!(recovered_in_group, f.missing[group].len());
        recovered_count += recovered_in_group;
    }
    let elapsed = started.elapsed().as_nanos();
    let allocs = ALLOCS.load(Ordering::Relaxed) - allocs_before;
    let alloc_bytes = ALLOC_BYTES.load(Ordering::Relaxed) - bytes_before;
    assert_eq!(recovered_count, f.missing_count);
    println!("{{\"label\":\"{label}\",\"data\":{data},\"parity\":{parity},\"case\":\"{case}\",\"repeat\":{repeat},\"groups\":{},\"packets\":{},\"recovered\":{recovered_count},\"expected_recovered\":{},\"elapsed_ns\":{elapsed},\"allocs\":{allocs},\"allocated_bytes\":{alloc_bytes}}}", f.packets.len(), f.packet_count, f.missing_count);
}

fn main() {
    let label = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "current".to_owned());
    for (data, parity, groups) in [(10, 3, 2_000), (128, 128, 96)] {
        for case in ["ordered", "reordered", "loss_reordered"] {
            let f = fixture(data, parity, groups, case);
            // Untimed validation/warmup uses the same exact generated input.
            measure(&label, data, parity, case, &f, 0);
            for repeat in 1..=5 {
                measure(&label, data, parity, case, &f, repeat);
            }
        }
    }
}
