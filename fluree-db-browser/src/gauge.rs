//! Write-behind gauge: bounded bytes between "fetched" and "persisted".
//!
//! Every block queued for the IndexedDB write-behind holds a
//! [`WriteBehindPermit`] sized to its byte length; the permit travels inside
//! the `CachePut` job and releases when the driver finishes (or fails, or
//! drops) the write. Fetch admission acquires the permit *before* enqueueing,
//! so when IndexedDB falls behind the network the fetch pipeline stalls
//! instead of queueing unbounded block clones in memory.
//!
//! A single block larger than the whole budget is admitted when nothing is
//! outstanding, so an oversized object degrades to serialized writes rather
//! than deadlocking.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// Shared accounting for in-flight write-behind bytes.
#[derive(Debug)]
pub struct WriteBehindGauge {
    budget: u64,
    outstanding: AtomicU64,
    peak: AtomicU64,
    released: Notify,
}

impl WriteBehindGauge {
    /// A gauge admitting at most `budget_bytes` of un-persisted blocks.
    pub fn new(budget_bytes: u64) -> Arc<Self> {
        Arc::new(Self {
            budget: budget_bytes.max(1),
            outstanding: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            released: Notify::new(),
        })
    }

    /// Bytes currently between fetch and persist.
    pub fn outstanding(&self) -> u64 {
        self.outstanding.load(Ordering::Relaxed)
    }

    /// High-water mark of [`outstanding`](Self::outstanding).
    pub fn peak(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }

    /// The configured budget.
    pub fn budget(&self) -> u64 {
        self.budget
    }

    fn try_acquire(self: &Arc<Self>, size: u64) -> Option<WriteBehindPermit> {
        loop {
            let current = self.outstanding.load(Ordering::Acquire);
            let admissible = current == 0 || current + size <= self.budget;
            if !admissible {
                return None;
            }
            if self
                .outstanding
                .compare_exchange(current, current + size, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.peak.fetch_max(current + size, Ordering::Relaxed);
                return Some(WriteBehindPermit {
                    gauge: Arc::clone(self),
                    size,
                });
            }
        }
    }

    /// Reserve `size` bytes, waiting while the queue is full.
    pub async fn acquire(self: &Arc<Self>, size: u64) -> WriteBehindPermit {
        loop {
            if let Some(permit) = self.try_acquire(size) {
                return permit;
            }
            // Register for the release notification BEFORE re-checking, so a
            // release between the check and the await cannot be missed.
            let released = self.released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            if let Some(permit) = self.try_acquire(size) {
                return permit;
            }
            released.await;
        }
    }
}

/// Reservation for one queued block; releasing (dropping) it credits the
/// gauge and wakes waiting fetches.
#[derive(Debug)]
pub struct WriteBehindPermit {
    gauge: Arc<WriteBehindGauge>,
    size: u64,
}

impl WriteBehindPermit {
    /// The reserved byte count.
    pub fn size(&self) -> u64 {
        self.size
    }
}

impl Drop for WriteBehindPermit {
    fn drop(&mut self) {
        self.gauge
            .outstanding
            .fetch_sub(self.size, Ordering::AcqRel);
        self.gauge.released.notify_waiters();
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn acquire_admits_within_budget_and_tracks_peak() {
        let gauge = WriteBehindGauge::new(100);
        let a = gauge.acquire(60).await;
        let b = gauge.acquire(40).await;
        assert_eq!(gauge.outstanding(), 100);
        assert_eq!(gauge.peak(), 100);
        drop(a);
        assert_eq!(gauge.outstanding(), 40);
        drop(b);
        assert_eq!(gauge.outstanding(), 0);
        assert_eq!(gauge.peak(), 100, "peak is a high-water mark");
    }

    #[tokio::test]
    async fn full_gauge_blocks_until_release() {
        let gauge = WriteBehindGauge::new(10);
        let first = gauge.acquire(10).await;
        let waiter = {
            let gauge = Arc::clone(&gauge);
            tokio::spawn(async move { gauge.acquire(10).await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "second acquire must wait");
        drop(first);
        let second = waiter.await.unwrap();
        assert_eq!(gauge.outstanding(), 10);
        drop(second);
    }

    #[tokio::test]
    async fn oversized_block_is_admitted_when_idle() {
        let gauge = WriteBehindGauge::new(10);
        let big = gauge.acquire(1000).await;
        assert_eq!(gauge.outstanding(), 1000);
        drop(big);
        assert_eq!(gauge.outstanding(), 0);
    }
}
