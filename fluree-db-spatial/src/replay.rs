//! Time-travel replay and novelty merge.
//!
//! This module provides iterators for:
//! - Merging sorted snapshot entries with novelty entries
//! - Replaying to a specific `to_t` (choosing the latest entry ≤ to_t per key)
//!
//! # Replay Semantics
//!
//! For each `(cell_id, subject_id)` key, we want the entry with the highest
//! `t` that is ≤ `to_t`. If that entry is a retract (op=0), the subject is
//! excluded; otherwise it's included.
//!
//! Because entries are sorted by `(cell_id, subject_id, t DESC, op ASC)`, we can
//! stream through and emit the first entry per key that satisfies `t <= to_t`.
//!
//! # Novelty Precedence
//!
//! When merging snapshot and novelty, on exact ties novelty (overlay) wins.
//! This ensures uncommitted changes override persisted state.

use crate::cell_index::CellEntry;
use std::cmp::Ordering;

/// Merge two sorted iterators of CellEntry.
///
/// Both iterators **must** be sorted by `cmp_index()` order. Caller is
/// responsible for ensuring this invariant.
///
/// # Tie-Break: Overlay Precedence
///
/// When entries compare equal, `iter2` (overlay/novelty) takes precedence.
/// This is the contract for merge-based overlays: uncommitted changes win.
///
/// Typical usage: `MergeSorted::new(snapshot_iter, novelty_iter)`
pub struct MergeSorted<I1, I2>
where
    I1: Iterator<Item = CellEntry>,
    I2: Iterator<Item = CellEntry>,
{
    iter1: std::iter::Peekable<I1>,
    iter2: std::iter::Peekable<I2>,
}

impl<I1, I2> MergeSorted<I1, I2>
where
    I1: Iterator<Item = CellEntry>,
    I2: Iterator<Item = CellEntry>,
{
    /// Create a new merge iterator.
    ///
    /// - `iter1`: Base/snapshot entries (sorted by `cmp_index`)
    /// - `iter2`: Overlay/novelty entries (sorted by `cmp_index`) - wins on ties
    pub fn new(iter1: I1, iter2: I2) -> Self {
        Self {
            iter1: iter1.peekable(),
            iter2: iter2.peekable(),
        }
    }
}

impl<I1, I2> Iterator for MergeSorted<I1, I2>
where
    I1: Iterator<Item = CellEntry>,
    I2: Iterator<Item = CellEntry>,
{
    type Item = CellEntry;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.iter1.peek(), self.iter2.peek()) {
            (Some(e1), Some(e2)) => {
                match e1.cmp_index(e2) {
                    Ordering::Less => self.iter1.next(),
                    // On equal or greater, prefer iter2 (novelty wins on ties)
                    Ordering::Equal | Ordering::Greater => self.iter2.next(),
                }
            }
            (Some(_), None) => self.iter1.next(),
            (None, Some(_)) => self.iter2.next(),
            (None, None) => None,
        }
    }
}

/// Replay resolver: collapses entries to the latest state at `to_t`.
///
/// For each `(cell_id, subject_id)` key, emits only the first entry
/// with `t <= to_t`. If that entry is a retract, it's filtered out
/// (subject is not present at `to_t`).
pub struct ReplayResolver<I>
where
    I: Iterator<Item = CellEntry>,
{
    inner: std::iter::Peekable<I>,
    to_t: i64,
    current_key: Option<(u64, u64)>, // (cell_id, subject_id)
}

impl<I> ReplayResolver<I>
where
    I: Iterator<Item = CellEntry>,
{
    /// Create a new replay resolver.
    pub fn new(inner: I, to_t: i64) -> Self {
        Self {
            inner: inner.peekable(),
            to_t,
            current_key: None,
        }
    }
}

impl<I> Iterator for ReplayResolver<I>
where
    I: Iterator<Item = CellEntry>,
{
    type Item = CellEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let entry = self.inner.next()?;

            let key = (entry.cell_id, entry.subject_id);

            // Skip if we've already emitted for this key
            if self.current_key == Some(key) {
                continue;
            }

            // Skip if entry is in the future
            if entry.t > self.to_t {
                // Don't update current_key yet - there may be an older entry for this key
                // that's within range
                continue;
            }

            // This is the first entry for this key with t <= to_t
            self.current_key = Some(key);

            // Skip retracts (subject not present at to_t)
            if entry.is_retract() {
                continue;
            }

            return Some(entry);
        }
    }
}

/// Replay resolver that doesn't filter retracts (for debugging/inspection).
#[allow(dead_code)]
pub struct ReplayResolverWithRetracts<I>
where
    I: Iterator<Item = CellEntry>,
{
    inner: std::iter::Peekable<I>,
    to_t: i64,
    current_key: Option<(u64, u64)>,
}

impl<I> ReplayResolverWithRetracts<I>
where
    I: Iterator<Item = CellEntry>,
{
    /// Create a new replay resolver that includes retracts.
    #[allow(dead_code)]
    pub fn new(inner: I, to_t: i64) -> Self {
        Self {
            inner: inner.peekable(),
            to_t,
            current_key: None,
        }
    }
}

impl<I> Iterator for ReplayResolverWithRetracts<I>
where
    I: Iterator<Item = CellEntry>,
{
    type Item = CellEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let entry = self.inner.next()?;

            let key = (entry.cell_id, entry.subject_id);

            // Skip if we've already emitted for this key
            if self.current_key == Some(key) {
                continue;
            }

            // Skip if entry is in the future
            if entry.t > self.to_t {
                continue;
            }

            // This is the first entry for this key with t <= to_t
            self.current_key = Some(key);

            return Some(entry);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(cell_id: u64, subject_id: u64, t: i64, op: u8) -> CellEntry {
        CellEntry::new(cell_id, subject_id, 0, t, op)
    }

    #[test]
    fn test_merge_sorted() {
        let v1 = vec![entry(100, 1, 20, 1), entry(200, 1, 10, 1)];
        let v2 = vec![entry(100, 1, 10, 1), entry(150, 1, 10, 1)];

        let merged: Vec<_> = MergeSorted::new(v1.into_iter(), v2.into_iter()).collect();

        assert_eq!(merged.len(), 4);
        // Check ordering
        assert_eq!(merged[0].cell_id, 100);
        assert_eq!(merged[0].t, 20); // First because t DESC
        assert_eq!(merged[1].cell_id, 100);
        assert_eq!(merged[1].t, 10);
        assert_eq!(merged[2].cell_id, 150);
        assert_eq!(merged[3].cell_id, 200);
    }

    #[test]
    fn test_replay_resolver_time_travel() {
        // Entry at t=20 should be visible at to_t=25 but not at to_t=15
        let entries = vec![
            entry(100, 1, 20, 1), // assert at t=20
            entry(100, 1, 10, 1), // assert at t=10
        ];

        // Query at t=25: should see t=20 entry
        let result: Vec<_> = ReplayResolver::new(entries.clone().into_iter(), 25).collect();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].t, 20);

        // Query at t=15: should see t=10 entry
        let result: Vec<_> = ReplayResolver::new(entries.clone().into_iter(), 15).collect();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].t, 10);

        // Query at t=5: should see nothing
        let result: Vec<_> = ReplayResolver::new(entries.into_iter(), 5).collect();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_replay_resolver_retract() {
        // Assert at t=10, retract at t=20
        let entries = vec![
            entry(100, 1, 20, 0), // retract at t=20
            entry(100, 1, 10, 1), // assert at t=10
        ];

        // Query at t=25: should see retract, so no result
        let result: Vec<_> = ReplayResolver::new(entries.clone().into_iter(), 25).collect();
        assert_eq!(result.len(), 0);

        // Query at t=15: should see assert at t=10
        let result: Vec<_> = ReplayResolver::new(entries.into_iter(), 15).collect();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].t, 10);
        assert!(result[0].is_assert());
    }

    #[test]
    fn test_replay_resolver_multiple_subjects() {
        let entries = vec![
            entry(100, 1, 20, 1),
            entry(100, 1, 10, 1),
            entry(100, 2, 15, 1),
            entry(100, 2, 5, 1),
        ];

        let result: Vec<_> = ReplayResolver::new(entries.into_iter(), 20).collect();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].subject_id, 1);
        assert_eq!(result[0].t, 20);
        assert_eq!(result[1].subject_id, 2);
        assert_eq!(result[1].t, 15);
    }
}
