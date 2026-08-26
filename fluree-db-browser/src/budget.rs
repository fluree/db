//! Byte-budget LRU index for the persistent cache.
//!
//! IndexedDB has no eviction of its own, so the driver keeps this index of
//! `(key, size, last_access)` in memory — rebuilt from the `meta` object
//! store at open — and asks it which entries to delete when a write would
//! exceed the budget. Pure data structure: no I/O, no clock; natively
//! tested.

use std::collections::HashMap;

/// Metadata for one persisted block.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct IndexEntry {
    pub size: u64,
    /// Milliseconds since the epoch (JS `Date.now()` scale).
    pub last_access: f64,
}

/// In-memory index over the persisted blocks.
#[derive(Debug, Default, Clone)]
pub struct CacheIndex {
    entries: HashMap<String, IndexEntry>,
    total: u64,
}

impl CacheIndex {
    /// Empty index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuild from persisted metadata.
    pub fn load<I>(records: I) -> Self
    where
        I: IntoIterator<Item = (String, u64, f64)>,
    {
        let mut idx = Self::new();
        for (key, size, last_access) in records {
            idx.insert(key, size, last_access);
        }
        idx
    }

    /// Record a block. Re-inserting a key replaces its metadata.
    pub fn insert(&mut self, key: String, size: u64, now: f64) {
        if let Some(prev) = self.entries.insert(
            key,
            IndexEntry {
                size,
                last_access: now,
            },
        ) {
            self.total -= prev.size;
        }
        self.total += size;
    }

    /// Refresh a key's access time. Returns `false` for an unknown key.
    pub fn touch(&mut self, key: &str, now: f64) -> bool {
        match self.entries.get_mut(key) {
            Some(e) => {
                e.last_access = now;
                true
            }
            None => false,
        }
    }

    /// Forget a key, returning its size.
    pub fn remove(&mut self, key: &str) -> Option<u64> {
        let e = self.entries.remove(key)?;
        self.total -= e.size;
        Some(e.size)
    }

    /// Whether the key is indexed.
    pub fn contains(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }

    /// Metadata for a key.
    pub fn get(&self, key: &str) -> Option<IndexEntry> {
        self.entries.get(key).copied()
    }

    /// Number of indexed blocks.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Total persisted bytes.
    pub fn total_bytes(&self) -> u64 {
        self.total
    }

    /// Keys to delete so that, after removing them and adding `incoming`
    /// bytes, usage is at most `budget` — and, when eviction is needed at
    /// all, at most `low_water` (so one overflow does not cause a delete
    /// per subsequent write). Least-recently-used first. Empty when the
    /// incoming block already fits.
    pub fn plan_for_insert(&self, incoming: u64, budget: u64, low_water: u64) -> Vec<String> {
        if self.total + incoming <= budget {
            return Vec::new();
        }
        let target = low_water.min(budget).saturating_sub(incoming);
        let mut by_age: Vec<(&String, &IndexEntry)> = self.entries.iter().collect();
        by_age.sort_by(|a, b| {
            a.1.last_access
                .partial_cmp(&b.1.last_access)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(b.0))
        });
        let mut remaining = self.total;
        let mut victims = Vec::new();
        for (key, e) in by_age {
            if remaining <= target {
                break;
            }
            remaining -= e.size;
            victims.push(key.clone());
        }
        victims
    }

    /// Apply a planned eviction, returning the bytes freed.
    pub fn apply_eviction(&mut self, keys: &[String]) -> u64 {
        keys.iter().filter_map(|k| self.remove(k)).sum()
    }

    /// Iterate `(key, entry)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &IndexEntry)> {
        self.entries.iter()
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn insert_touch_remove_track_totals() {
        let mut idx = CacheIndex::new();
        idx.insert("a".into(), 10, 1.0);
        idx.insert("b".into(), 20, 2.0);
        assert_eq!(idx.total_bytes(), 30);
        assert_eq!(idx.len(), 2);
        // Replace a's size.
        idx.insert("a".into(), 15, 3.0);
        assert_eq!(idx.total_bytes(), 35);
        assert!(idx.touch("b", 9.0));
        assert!(!idx.touch("zzz", 9.0));
        assert_eq!(idx.get("b").unwrap().last_access, 9.0);
        assert_eq!(idx.remove("a"), Some(15));
        assert_eq!(idx.total_bytes(), 20);
        assert!(idx.remove("a").is_none());
    }

    #[test]
    fn no_eviction_when_the_incoming_block_fits() {
        let mut idx = CacheIndex::new();
        idx.insert("a".into(), 10, 1.0);
        assert!(idx.plan_for_insert(10, 20, 16).is_empty());
    }

    #[test]
    fn eviction_is_lru_and_drains_to_low_water() {
        let mut idx = CacheIndex::new();
        idx.insert("old".into(), 10, 1.0);
        idx.insert("mid".into(), 10, 2.0);
        idx.insert("new".into(), 10, 3.0);
        // Budget 30, low water 20, incoming 10: need total <= 10 after
        // eviction so 10 + 10 <= 20 → evict the two oldest.
        let victims = idx.plan_for_insert(10, 30, 20);
        assert_eq!(victims, vec!["old".to_string(), "mid".to_string()]);
        let freed = idx.apply_eviction(&victims);
        assert_eq!(freed, 20);
        assert_eq!(idx.total_bytes(), 10);
        assert!(idx.contains("new"));
    }

    #[test]
    fn rebuild_from_records_preserves_ordering_inputs() {
        let idx = CacheIndex::load(vec![
            ("x".to_string(), 5, 5.0),
            ("y".to_string(), 7, 1.0),
        ]);
        assert_eq!(idx.total_bytes(), 12);
        // y is older, so it goes first when over budget.
        let victims = idx.plan_for_insert(20, 20, 10);
        assert_eq!(victims[0], "y");
    }

    #[test]
    fn eviction_with_equal_timestamps_is_deterministic() {
        let mut idx = CacheIndex::new();
        idx.insert("b".into(), 10, 1.0);
        idx.insert("a".into(), 10, 1.0);
        let victims = idx.plan_for_insert(10, 20, 10);
        assert_eq!(victims, vec!["a".to_string(), "b".to_string()]);
    }
}
