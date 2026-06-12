//! Tracking global allocator for benchmarks.
//!
//! Wraps [`std::alloc::System`] and maintains three counters with two
//! `Relaxed` atomic operations per allocation/deallocation:
//!
//! - **current** — live bytes right now,
//! - **peak** — high-water mark of `current` since the last [`reset_peak`],
//! - **total** — cumulative bytes allocated since the last [`reset_peak`]
//!   (deallocations don't decrease it; a measure of allocator churn).
//!
//! ## Usage in a bench
//!
//! ```ignore
//! use fluree_bench_alloc::TrackingAllocator;
//!
//! #[global_allocator]
//! static ALLOC: TrackingAllocator = TrackingAllocator::new();
//!
//! // around a scenario:
//! fluree_bench_alloc::reset_peak();
//! run_scenario();
//! let m = fluree_bench_alloc::snapshot();
//! eprintln!("peak={}B churn={}B", m.peak_bytes, m.total_allocated_bytes);
//! ```
//!
//! ## Measurement honesty
//!
//! The counters add a small constant overhead to every allocation. That
//! overhead is identical between a baseline run and a comparison run of the
//! same bench binary, so *deltas* remain valid; absolute numbers from a
//! tracking bench should not be compared against a non-tracking bench.
//! Benches that install this allocator say so in their module docs.
//!
//! Peak tracking uses a `Relaxed` compare-exchange loop; under multi-threaded
//! allocation the recorded peak can momentarily lag the true peak by the
//! window of a racing update, which is well inside bench noise. Counters are
//! process-wide: reset before each scenario and read after it, and don't run
//! scenarios concurrently (criterion doesn't).

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static TOTAL: AtomicUsize = AtomicUsize::new(0);
/// Live bytes at the last [`reset_peak`] — the ambient baseline a scenario
/// starts from. `scenario_peak_bytes = peak - this` isolates the memory a
/// scenario itself is responsible for, making the gated metric robust
/// against ambient process-heap shifts between binaries (a recompile can
/// move absolute peak by megabytes uniformly across every scenario).
static CURRENT_AT_RESET: AtomicUsize = AtomicUsize::new(0);

/// A `GlobalAlloc` wrapper around [`System`] that maintains the module's
/// current / peak / total counters.
pub struct TrackingAllocator;

impl TrackingAllocator {
    #[must_use]
    pub const fn new() -> Self {
        TrackingAllocator
    }
}

impl Default for TrackingAllocator {
    fn default() -> Self {
        Self::new()
    }
}

fn on_alloc(size: usize) {
    TOTAL.fetch_add(size, Ordering::Relaxed);
    let cur = CURRENT.fetch_add(size, Ordering::Relaxed) + size;
    // Lock-free high-water mark. Relaxed is fine: we only need the peak to
    // be observable after the scenario's allocations have happened-before
    // the read, which the bench harness's own synchronization provides.
    let mut peak = PEAK.load(Ordering::Relaxed);
    while cur > peak {
        match PEAK.compare_exchange_weak(peak, cur, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => peak = observed,
        }
    }
}

fn on_dealloc(size: usize) {
    CURRENT.fetch_sub(size, Ordering::Relaxed);
}

// SAFETY: delegates every allocation verbatim to `System`, which upholds the
// `GlobalAlloc` contract; the counter updates around the delegation never
// touch the returned memory and cannot panic (atomics only).
unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            on_alloc(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        on_dealloc(layout.size());
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);
        if !ptr.is_null() {
            on_alloc(layout.size());
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = System.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            // Model as dealloc(old) + alloc(new) so `current` stays exact.
            on_dealloc(layout.size());
            on_alloc(new_size);
        }
        new_ptr
    }
}

/// A point-in-time reading of the allocator counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllocSnapshot {
    /// Live bytes at the time of the snapshot.
    pub current_bytes: usize,
    /// High-water mark of live bytes since the last [`reset_peak`].
    /// Includes the ambient baseline live at reset time.
    pub peak_bytes: usize,
    /// `peak_bytes` minus live bytes at the last [`reset_peak`] — the
    /// scenario-attributable high-water mark. Comparable across binaries;
    /// prefer this for regression gating.
    pub scenario_peak_bytes: usize,
    /// Cumulative bytes allocated since the last [`reset_peak`] (churn).
    pub total_allocated_bytes: usize,
}

/// Reset `peak` to the current live size and `total` to zero. Call before
/// the measured region of a scenario.
pub fn reset_peak() {
    let cur = CURRENT.load(Ordering::Relaxed);
    PEAK.store(cur, Ordering::Relaxed);
    CURRENT_AT_RESET.store(cur, Ordering::Relaxed);
    TOTAL.store(0, Ordering::Relaxed);
}

/// Read the counters. Call after the measured region of a scenario.
#[must_use]
pub fn snapshot() -> AllocSnapshot {
    let peak = PEAK.load(Ordering::Relaxed);
    let at_reset = CURRENT_AT_RESET.load(Ordering::Relaxed);
    AllocSnapshot {
        current_bytes: CURRENT.load(Ordering::Relaxed),
        peak_bytes: peak,
        scenario_peak_bytes: peak.saturating_sub(at_reset),
        total_allocated_bytes: TOTAL.load(Ordering::Relaxed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // NOTE: these tests exercise the counter arithmetic directly rather than
    // installing the allocator (a test binary can't conditionally install a
    // global allocator, and installing it for the whole test binary would
    // make assertions racy against the test harness's own allocations).
    // The counters are process-global, so the tests serialize on a lock.

    static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn alloc_dealloc_counter_arithmetic() {
        let _guard = TEST_LOCK.lock().unwrap();
        reset_peak();
        let before = snapshot();
        on_alloc(1000);
        on_alloc(500);
        on_dealloc(500);
        let after = snapshot();
        assert_eq!(after.current_bytes, before.current_bytes + 1000);
        assert!(after.peak_bytes >= before.current_bytes + 1500);
        assert!(after.total_allocated_bytes >= 1500);
        on_dealloc(1000);
    }

    #[test]
    fn reset_peak_rebases_to_current() {
        let _guard = TEST_LOCK.lock().unwrap();
        on_alloc(2048);
        reset_peak();
        let s = snapshot();
        assert_eq!(s.peak_bytes, s.current_bytes);
        assert_eq!(s.total_allocated_bytes, 0);
        on_dealloc(2048);
    }
}
