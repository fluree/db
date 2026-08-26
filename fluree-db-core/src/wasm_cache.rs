//! Minimal `moka::sync::Cache` stand-in for wasm32.
//!
//! moka 0.12 constructs its eviction clock from `std::time::Instant::now()`
//! the moment a cache or builder is created (`Clock::default()` in
//! `moka::sync::CacheBuilder`), which aborts on wasm32-unknown-unknown. None
//! of the workspace's wasm-reachable caches use time-based expiry (they are
//! all `max_capacity` / `weigher` only), so a clock-free LRU is a faithful
//! replacement there.
//!
//! This module provides a byte-weighted LRU with the subset of moka's sync
//! API the workspace uses (`get`, `get_with`, `try_get_with`, `insert`,
//! `contains_key`, `invalidate_all`, `run_pending_tasks`, `weighted_size`,
//! `entry_count`, `policy().max_capacity()`), so wasm builds shadow-import it
//! in place of `moka::sync::Cache` with a one-line cfg'd `use` per seam and
//! native builds keep moka untouched.
//!
//! Semantics deliberately simplified for a single-threaded target:
//! - `get_with` / `try_get_with` are check → compute → insert, not
//!   single-flight. The init closure runs OUTSIDE the lock, so re-entrant
//!   cache use inside a decode closure cannot deadlock; with one thread there
//!   is no concurrent duplicate work to coalesce anyway.
//! - Eviction is exact LRU (not TinyLFU). Entries are evicted from the cold
//!   end until the weighted size fits the budget, but the most recently
//!   inserted entry is never evicted — a single entry larger than the budget
//!   stays usable rather than thrashing.
//!
//! Compiled on native only under `cfg(test)` so the unit tests run there;
//! nothing outside a wasm build links it.

use lru::LruCache;
use parking_lot::Mutex;
use std::borrow::Borrow;
use std::hash::Hash;
use std::sync::Arc;

type Weigher<K, V> = Box<dyn Fn(&K, &V) -> u32 + Send + Sync>;

/// Clock-free `moka::sync::Cache` look-alike (see module docs).
///
/// Cloning shares the underlying store, matching moka's handle semantics.
pub struct Cache<K, V> {
    shared: Arc<Shared<K, V>>,
}

impl<K, V> Clone for Cache<K, V> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

/// moka's `Cache` implements `Debug` (containing types derive it); mirror
/// that without printing entries or requiring `K: Debug` / `V: Debug`.
impl<K, V> std::fmt::Debug for Cache<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache").finish_non_exhaustive()
    }
}

struct Shared<K, V> {
    inner: Mutex<Inner<K, V>>,
    weigher: Option<Weigher<K, V>>,
    max_weight: u64,
}

struct Inner<K, V> {
    /// Unbounded LRU; the byte/entry budget is enforced manually so one
    /// mechanism serves both weighed (byte-budget) and unweighed
    /// (entry-count) caches.
    map: LruCache<K, (V, u32)>,
    /// Sum of the weights of all resident entries.
    weighted: u64,
}

/// Builder mirroring the `moka::sync::Cache::builder()` calls the workspace
/// makes (`max_capacity`, `weigher`, `build`).
pub struct CacheBuilder<K, V> {
    max_capacity: u64,
    weigher: Option<Weigher<K, V>>,
}

impl<K, V> Default for CacheBuilder<K, V> {
    fn default() -> Self {
        Self {
            max_capacity: u64::MAX,
            weigher: None,
        }
    }
}

impl<K: Hash + Eq, V: Clone> CacheBuilder<K, V> {
    pub fn max_capacity(mut self, max: u64) -> Self {
        self.max_capacity = max;
        self
    }

    pub fn weigher(mut self, weigher: impl Fn(&K, &V) -> u32 + Send + Sync + 'static) -> Self {
        self.weigher = Some(Box::new(weigher));
        self
    }

    pub fn build(self) -> Cache<K, V> {
        Cache {
            shared: Arc::new(Shared {
                inner: Mutex::new(Inner {
                    map: LruCache::unbounded(),
                    weighted: 0,
                }),
                weigher: self.weigher,
                max_weight: self.max_capacity,
            }),
        }
    }
}

/// Mirror of `moka::policy::Policy` for the single accessor the workspace
/// uses (`policy().max_capacity()`).
pub struct Policy {
    max_capacity: u64,
}

impl Policy {
    pub fn max_capacity(&self) -> Option<u64> {
        Some(self.max_capacity)
    }
}

impl<K: Hash + Eq, V: Clone> Cache<K, V> {
    /// Entry-count-bounded cache (weight 1 per entry), like
    /// `moka::sync::Cache::new`.
    pub fn new(max_capacity: u64) -> Self {
        Self::builder().max_capacity(max_capacity).build()
    }

    pub fn builder() -> CacheBuilder<K, V> {
        CacheBuilder::default()
    }

    /// Get a clone of the cached value, promoting the entry to
    /// most-recently-used.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut inner = self.shared.inner.lock();
        inner.map.get(key).map(|(v, _)| v.clone())
    }

    /// Presence check without promotion (moka's `contains_key` likewise does
    /// not touch recency).
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.shared.inner.lock().map.contains(key)
    }

    /// Get the value for `key`, computing and inserting it on a miss.
    ///
    /// NOT single-flight (see module docs); the closure runs outside the
    /// lock.
    pub fn get_with(&self, key: K, init: impl FnOnce() -> V) -> V {
        if let Some(v) = self.get(&key) {
            return v;
        }
        let v = init();
        self.insert(key, v.clone());
        v
    }

    /// Fallible `get_with`, matching moka's `Arc<E>` error type. A failed
    /// init inserts nothing.
    pub fn try_get_with<E>(
        &self,
        key: K,
        init: impl FnOnce() -> Result<V, E>,
    ) -> Result<V, Arc<E>> {
        if let Some(v) = self.get(&key) {
            return Ok(v);
        }
        match init() {
            Ok(v) => {
                self.insert(key, v.clone());
                Ok(v)
            }
            Err(e) => Err(Arc::new(e)),
        }
    }

    pub fn insert(&self, key: K, value: V) {
        let weight = self
            .shared
            .weigher
            .as_ref()
            .map(|w| w(&key, &value))
            .unwrap_or(1);
        let mut inner = self.shared.inner.lock();
        if let Some((_, old_weight)) = inner.map.put(key, (value, weight)) {
            inner.weighted -= u64::from(old_weight);
        }
        inner.weighted += u64::from(weight);
        // Evict cold entries until the budget fits, but never the entry just
        // inserted (len > 1 floor).
        while inner.weighted > self.shared.max_weight && inner.map.len() > 1 {
            if let Some((_, (_, w))) = inner.map.pop_lru() {
                inner.weighted -= u64::from(w);
            } else {
                break;
            }
        }
    }

    pub fn invalidate_all(&self) {
        let mut inner = self.shared.inner.lock();
        inner.map.clear();
        inner.weighted = 0;
    }

    /// moka runs deferred housekeeping here; this cache has none.
    pub fn run_pending_tasks(&self) {}

    pub fn weighted_size(&self) -> u64 {
        self.shared.inner.lock().weighted
    }

    pub fn entry_count(&self) -> u64 {
        self.shared.inner.lock().map.len() as u64
    }

    pub fn policy(&self) -> Policy {
        Policy {
            max_capacity: self.shared.max_weight,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_count_budget_evicts_lru() {
        let cache: Cache<u32, u32> = Cache::new(2);
        cache.insert(1, 10);
        cache.insert(2, 20);
        // Touch 1 so 2 becomes the LRU victim.
        assert_eq!(cache.get(&1), Some(10));
        cache.insert(3, 30);
        assert_eq!(cache.entry_count(), 2);
        assert_eq!(cache.get(&2), None);
        assert_eq!(cache.get(&1), Some(10));
        assert_eq!(cache.get(&3), Some(30));
    }

    #[test]
    fn weigher_budget_and_weighted_size() {
        let cache: Cache<u32, Vec<u8>> = Cache::builder()
            .weigher(|_k, v: &Vec<u8>| v.len() as u32)
            .max_capacity(100)
            .build();
        cache.insert(1, vec![0u8; 60]);
        cache.insert(2, vec![0u8; 30]);
        assert_eq!(cache.weighted_size(), 90);
        cache.insert(3, vec![0u8; 50]);
        // 1 (LRU, 60 bytes) must go to fit 30 + 50.
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.weighted_size(), 80);
        assert_eq!(cache.entry_count(), 2);
    }

    #[test]
    fn oversized_sole_entry_survives() {
        let cache: Cache<u32, Vec<u8>> = Cache::builder()
            .weigher(|_k, v: &Vec<u8>| v.len() as u32)
            .max_capacity(10)
            .build();
        cache.insert(1, vec![0u8; 100]);
        assert_eq!(cache.entry_count(), 1);
        assert!(cache.get(&1).is_some());
        // A second insert evicts the old oversized entry, keeping the new one.
        cache.insert(2, vec![0u8; 5]);
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&2), Some(vec![0u8; 5]));
    }

    #[test]
    fn reinsert_same_key_adjusts_weight() {
        let cache: Cache<u32, Vec<u8>> = Cache::builder()
            .weigher(|_k, v: &Vec<u8>| v.len() as u32)
            .max_capacity(100)
            .build();
        cache.insert(1, vec![0u8; 60]);
        cache.insert(1, vec![0u8; 20]);
        assert_eq!(cache.weighted_size(), 20);
        assert_eq!(cache.entry_count(), 1);
    }

    #[test]
    fn get_with_and_try_get_with() {
        let cache: Cache<u32, u32> = Cache::new(10);
        assert_eq!(cache.get_with(1, || 11), 11);
        // Hit: closure must not run.
        assert_eq!(cache.get_with(1, || unreachable!()), 11);

        let err: Result<u32, Arc<&str>> = cache.try_get_with(2, || Err("boom"));
        assert_eq!(*err.unwrap_err(), "boom");
        // Failure inserted nothing.
        assert_eq!(cache.get(&2), None);
        let ok: Result<u32, Arc<&str>> = cache.try_get_with(2, || Ok(22));
        assert_eq!(ok.unwrap(), 22);
        assert_eq!(cache.get(&2), Some(22));
    }

    #[test]
    fn policy_and_invalidate_all() {
        let cache: Cache<u32, u32> = Cache::new(7);
        cache.insert(1, 1);
        assert_eq!(cache.policy().max_capacity(), Some(7));
        cache.invalidate_all();
        assert_eq!(cache.entry_count(), 0);
        assert_eq!(cache.weighted_size(), 0);
        cache.run_pending_tasks();
    }
}
