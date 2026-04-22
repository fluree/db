//! Index cache with LRU eviction and TTL expiration.
//!
//! This module provides a simple cache for BM25 indexes with:
//! - Maximum entry count (LRU eviction)
//! - Time-to-live expiration per entry
//!
//! The cache is designed to be simple and safe, avoiding complex
//! eviction policies based on byte sizes.

use fluree_db_query::bm25::Bm25Index;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Cache key: (graph_source_id, index_t)
pub type CacheKey = (String, i64);

/// Cache entry with timestamp for TTL expiration.
struct CacheEntry {
    index: Arc<Bm25Index>,
    inserted_at: Instant,
}

/// LRU cache for BM25 indexes with TTL expiration.
pub struct IndexCache {
    /// Inner LRU cache protected by RwLock.
    inner: RwLock<LruCache<CacheKey, CacheEntry>>,
    /// TTL for cache entries.
    ttl: Duration,
}

impl IndexCache {
    /// Create a new cache with the specified capacity and TTL.
    ///
    /// # Arguments
    ///
    /// * `max_entries` - Maximum number of entries before LRU eviction
    /// * `ttl` - Time-to-live for cache entries
    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        let capacity = NonZeroUsize::new(max_entries.max(1)).expect("max_entries must be positive");
        Self {
            inner: RwLock::new(LruCache::new(capacity)),
            ttl,
        }
    }

    /// Get an index from the cache if present and not expired.
    ///
    /// Returns `None` if the entry doesn't exist or has expired.
    /// Expired entries are removed on access.
    pub fn get(&self, key: &CacheKey) -> Option<Arc<Bm25Index>> {
        let mut cache = self.inner.write().ok()?;

        // Check if entry exists
        if let Some(entry) = cache.get(key) {
            // Check TTL
            if entry.inserted_at.elapsed() < self.ttl {
                return Some(entry.index.clone());
            }
            // Entry expired - remove it
            cache.pop(key);
        }

        None
    }

    /// Insert an index into the cache.
    ///
    /// If the cache is at capacity, the least recently used entry
    /// will be evicted.
    pub fn insert(&self, key: CacheKey, index: Arc<Bm25Index>) {
        if let Ok(mut cache) = self.inner.write() {
            cache.put(
                key,
                CacheEntry {
                    index,
                    inserted_at: Instant::now(),
                },
            );
        }
    }

    /// Remove an entry from the cache.
    pub fn remove(&self, key: &CacheKey) -> Option<Arc<Bm25Index>> {
        self.inner.write().ok()?.pop(key).map(|entry| entry.index)
    }

    /// Clear all entries from the cache.
    pub fn clear(&self) {
        if let Ok(mut cache) = self.inner.write() {
            cache.clear();
        }
    }

    /// Get the current number of entries in the cache.
    pub fn len(&self) -> usize {
        self.inner.read().map(|c| c.len()).unwrap_or(0)
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl std::fmt::Debug for IndexCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexCache")
            .field("len", &self.len())
            .field("ttl", &self.ttl)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_cache_basic_operations() {
        let cache = IndexCache::new(10, Duration::from_secs(60));

        let key = ("test:main".to_string(), 100);
        let index = Arc::new(Bm25Index::new());

        // Initially empty
        assert!(cache.get(&key).is_none());

        // Insert and retrieve
        cache.insert(key.clone(), index.clone());
        assert!(cache.get(&key).is_some());

        // Remove
        assert!(cache.remove(&key).is_some());
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn test_cache_ttl_expiration() {
        let cache = IndexCache::new(10, Duration::from_millis(50));

        let key = ("test:main".to_string(), 100);
        let index = Arc::new(Bm25Index::new());

        cache.insert(key.clone(), index);
        assert!(cache.get(&key).is_some());

        // Wait for TTL to expire
        sleep(Duration::from_millis(100));

        // Entry should be expired
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn test_cache_lru_eviction() {
        let cache = IndexCache::new(2, Duration::from_secs(60));

        let key1 = ("vg1".to_string(), 100);
        let key2 = ("vg2".to_string(), 100);
        let key3 = ("vg3".to_string(), 100);

        cache.insert(key1.clone(), Arc::new(Bm25Index::new()));
        cache.insert(key2.clone(), Arc::new(Bm25Index::new()));

        assert_eq!(cache.len(), 2);

        // Access key1 to make it recently used
        cache.get(&key1);

        // Insert key3, should evict key2 (least recently used)
        cache.insert(key3.clone(), Arc::new(Bm25Index::new()));

        assert_eq!(cache.len(), 2);
        assert!(cache.get(&key1).is_some());
        assert!(cache.get(&key2).is_none()); // Evicted
        assert!(cache.get(&key3).is_some());
    }

    #[test]
    fn test_cache_clear() {
        let cache = IndexCache::new(10, Duration::from_secs(60));

        cache.insert(("vg1".to_string(), 100), Arc::new(Bm25Index::new()));
        cache.insert(("vg2".to_string(), 100), Arc::new(Bm25Index::new()));

        assert_eq!(cache.len(), 2);

        cache.clear();

        assert!(cache.is_empty());
    }
}
