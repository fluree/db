//! Global deduplication across S2 cells.
//!
//! A single geometry can appear in multiple S2 cells (its covering may have
//! multiple cells, and query coverings may overlap with multiple indexed cells).
//! This module provides deduplication to ensure each subject appears at most
//! once in query results.
//!
//! # Dedup Strategies
//!
//! - **KeepFirst**: Keep the first occurrence (arbitrary)
//! - **KeepMinDistance**: Keep the occurrence with minimum distance (for proximity queries)
//! - **KeepMaxDistance**: Keep the occurrence with maximum distance

use crate::cell_index::CellEntry;
use rustc_hash::FxHashMap;

/// Deduplication strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code, clippy::enum_variant_names)]
pub enum DedupStrategy {
    /// Keep the first occurrence encountered.
    KeepFirst,

    /// Keep the occurrence with minimum computed distance.
    /// Requires distance to be computed for each entry.
    KeepMinDistance,

    /// Keep the occurrence with maximum computed distance.
    KeepMaxDistance,
}

/// Entry with computed distance (for distance-based dedup).
#[derive(Debug, Clone)]
pub struct EntryWithDistance {
    pub entry: CellEntry,
    pub distance: f64,
}

/// Deduplicate entries by subject_id, keeping the first occurrence.
pub fn dedup_keep_first(entries: impl IntoIterator<Item = CellEntry>) -> Vec<CellEntry> {
    let mut seen: FxHashMap<u64, ()> = FxHashMap::default();
    let mut result = Vec::new();

    for entry in entries {
        if seen.insert(entry.subject_id, ()).is_none() {
            result.push(entry);
        }
    }

    result
}

/// Deduplicate entries by subject_id, keeping the one with minimum distance.
pub fn dedup_min_distance(
    entries: impl IntoIterator<Item = EntryWithDistance>,
) -> Vec<EntryWithDistance> {
    let mut best: FxHashMap<u64, EntryWithDistance> = FxHashMap::default();

    for item in entries {
        best.entry(item.entry.subject_id)
            .and_modify(|existing| {
                if item.distance < existing.distance {
                    *existing = item.clone();
                }
            })
            .or_insert(item);
    }

    best.into_values().collect()
}

/// Deduplicate entries by subject_id, keeping the one with maximum distance.
pub fn dedup_max_distance(
    entries: impl IntoIterator<Item = EntryWithDistance>,
) -> Vec<EntryWithDistance> {
    let mut best: FxHashMap<u64, EntryWithDistance> = FxHashMap::default();

    for item in entries {
        best.entry(item.entry.subject_id)
            .and_modify(|existing| {
                if item.distance > existing.distance {
                    *existing = item.clone();
                }
            })
            .or_insert(item);
    }

    best.into_values().collect()
}

/// Streaming deduplicator that yields entries as they become "final".
///
/// For streaming dedup, we can only emit an entry once we're sure we've
/// seen all occurrences of that subject. This requires knowing when we've
/// moved past all cells that could contain the subject.
///
/// For simplicity, this implementation collects all entries first, then
/// deduplicates. A more sophisticated implementation could use cell_id
/// ordering to emit earlier.
pub struct StreamingDedup {
    strategy: DedupStrategy,
    entries: Vec<EntryWithDistance>,
}

impl StreamingDedup {
    /// Create a new streaming deduplicator.
    pub fn new(strategy: DedupStrategy) -> Self {
        Self {
            strategy,
            entries: Vec::new(),
        }
    }

    /// Add an entry with its computed distance.
    pub fn push(&mut self, entry: CellEntry, distance: f64) {
        self.entries.push(EntryWithDistance { entry, distance });
    }

    /// Add an entry without distance (uses 0.0 as placeholder).
    #[allow(dead_code)]
    pub fn push_no_distance(&mut self, entry: CellEntry) {
        self.entries.push(EntryWithDistance {
            entry,
            distance: 0.0,
        });
    }

    /// Finalize and return deduplicated entries.
    pub fn finish(self) -> Vec<EntryWithDistance> {
        match self.strategy {
            DedupStrategy::KeepFirst => {
                let deduped = dedup_keep_first(self.entries.into_iter().map(|e| e.entry));
                deduped
                    .into_iter()
                    .map(|entry| EntryWithDistance {
                        entry,
                        distance: 0.0,
                    })
                    .collect()
            }
            DedupStrategy::KeepMinDistance => dedup_min_distance(self.entries),
            DedupStrategy::KeepMaxDistance => dedup_max_distance(self.entries),
        }
    }

    /// Finalize and return deduplicated entries sorted by distance (ascending).
    pub fn finish_sorted_by_distance(self) -> Vec<EntryWithDistance> {
        let mut result = self.finish();
        result.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(subject_id: u64) -> CellEntry {
        CellEntry::new(100, subject_id, 0, 10, 1)
    }

    #[test]
    fn test_dedup_keep_first() {
        let entries = vec![
            entry(1),
            entry(2),
            entry(1), // duplicate
            entry(3),
            entry(2), // duplicate
        ];

        let result = dedup_keep_first(entries);

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].subject_id, 1);
        assert_eq!(result[1].subject_id, 2);
        assert_eq!(result[2].subject_id, 3);
    }

    #[test]
    fn test_dedup_min_distance() {
        let entries = vec![
            EntryWithDistance {
                entry: entry(1),
                distance: 100.0,
            },
            EntryWithDistance {
                entry: entry(2),
                distance: 200.0,
            },
            EntryWithDistance {
                entry: entry(1),
                distance: 50.0,
            }, // closer
            EntryWithDistance {
                entry: entry(2),
                distance: 300.0,
            }, // farther
        ];

        let result = dedup_min_distance(entries);

        assert_eq!(result.len(), 2);

        let s1 = result.iter().find(|e| e.entry.subject_id == 1).unwrap();
        let s2 = result.iter().find(|e| e.entry.subject_id == 2).unwrap();

        assert_eq!(s1.distance, 50.0); // kept the closer one
        assert_eq!(s2.distance, 200.0); // kept the closer one
    }

    #[test]
    fn test_streaming_dedup() {
        let mut dedup = StreamingDedup::new(DedupStrategy::KeepMinDistance);

        dedup.push(entry(1), 100.0);
        dedup.push(entry(2), 200.0);
        dedup.push(entry(1), 50.0);

        let result = dedup.finish_sorted_by_distance();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].entry.subject_id, 1);
        assert_eq!(result[0].distance, 50.0);
        assert_eq!(result[1].entry.subject_id, 2);
        assert_eq!(result[1].distance, 200.0);
    }
}
