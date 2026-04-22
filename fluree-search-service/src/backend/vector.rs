//! Vector search backend implementation.
//!
//! This module provides the vector backend that handles:
//! - Vector index loading from storage
//! - Caching with LRU eviction and TTL
//! - Similarity search execution
//! - Sync semantics (head-only; no time-travel)

use super::SearchBackend;
use crate::error::{Result, ServiceError};
use crate::sync::{wait_for_head, SyncConfig};
use async_trait::async_trait;
use fluree_db_query::vector::usearch::VectorIndex;
use fluree_search_protocol::{QueryVariant, SearchHit};
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// Configuration for the vector backend.
#[derive(Debug, Clone)]
pub struct VectorBackendConfig {
    /// Maximum number of cached indexes.
    pub cache_max_entries: usize,
    /// Time-to-live for cached indexes.
    pub cache_ttl_secs: u64,
    /// Maximum concurrent index loads.
    pub max_concurrent_loads: usize,
    /// Default timeout for requests.
    pub default_timeout_ms: u64,
    /// Sync polling configuration.
    pub sync_config: SyncConfig,
}

impl Default for VectorBackendConfig {
    fn default() -> Self {
        Self {
            cache_max_entries: 50,
            cache_ttl_secs: 300, // 5 minutes
            max_concurrent_loads: 4,
            default_timeout_ms: 30_000,
            sync_config: SyncConfig::default(),
        }
    }
}

/// Trait for loading vector indexes from storage.
///
/// This abstracts the storage layer so the backend can be used
/// with different storage implementations.
///
/// Vector indexes are **head-only**: they do not support time-travel queries.
/// The loader provides access to the latest snapshot only.
#[async_trait]
pub trait VectorIndexLoader: std::fmt::Debug + Send + Sync {
    /// Load the vector index for a graph source.
    ///
    /// Vector is head-only: implementations always load from the nameservice
    /// head pointer. The `index_t` parameter is used as a cache key by the
    /// backend, not for time-travel selection.
    ///
    /// # Arguments
    ///
    /// * `graph_source_id` - Graph source alias
    /// * `index_t` - Head transaction (used as cache key, not for snapshot selection)
    async fn load_index(&self, graph_source_id: &str, index_t: i64) -> Result<VectorIndex>;

    /// Get the latest available index transaction for a graph source.
    ///
    /// Returns `None` if no index has been built yet.
    async fn get_latest_index_t(&self, graph_source_id: &str) -> Result<Option<i64>>;

    /// Get the current nameservice index head for sync operations.
    ///
    /// This is the latest committed transaction that should be indexed.
    async fn get_index_head(&self, graph_source_id: &str) -> Result<Option<i64>>;
}

/// Cache key: (graph_source_id, index_t)
type VectorCacheKey = (String, i64);

/// Cache entry with timestamp for TTL expiration.
struct VectorCacheEntry {
    index: Arc<VectorIndex>,
    inserted_at: Instant,
}

/// LRU cache for vector indexes with TTL expiration.
struct VectorIndexCache {
    inner: RwLock<LruCache<VectorCacheKey, VectorCacheEntry>>,
    ttl: Duration,
}

impl VectorIndexCache {
    fn new(max_entries: usize, ttl: Duration) -> Self {
        let capacity = NonZeroUsize::new(max_entries.max(1)).expect("max_entries must be positive");
        Self {
            inner: RwLock::new(LruCache::new(capacity)),
            ttl,
        }
    }

    fn get(&self, key: &VectorCacheKey) -> Option<Arc<VectorIndex>> {
        let mut cache = self.inner.write().ok()?;
        if let Some(entry) = cache.get(key) {
            if entry.inserted_at.elapsed() < self.ttl {
                return Some(entry.index.clone());
            }
            cache.pop(key);
        }
        None
    }

    fn insert(&self, key: VectorCacheKey, index: Arc<VectorIndex>) {
        if let Ok(mut cache) = self.inner.write() {
            cache.put(
                key,
                VectorCacheEntry {
                    index,
                    inserted_at: Instant::now(),
                },
            );
        }
    }

    fn len(&self) -> usize {
        self.inner.read().map(|c| c.len()).unwrap_or(0)
    }
}

impl std::fmt::Debug for VectorIndexCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorIndexCache")
            .field("len", &self.len())
            .field("ttl", &self.ttl)
            .finish()
    }
}

/// Vector search backend.
///
/// Handles vector similarity search with caching and sync support.
/// Vector indexes are head-only and do not support time-travel queries (`as_of_t`).
pub struct VectorBackend<L: VectorIndexLoader> {
    /// Index loader (storage access).
    loader: L,
    /// Index cache.
    cache: VectorIndexCache,
    /// Semaphore to limit concurrent index loads.
    load_semaphore: Semaphore,
    /// Configuration.
    config: VectorBackendConfig,
}

impl<L: VectorIndexLoader> VectorBackend<L> {
    /// Create a new vector backend.
    pub fn new(loader: L, config: VectorBackendConfig) -> Self {
        Self {
            loader,
            cache: VectorIndexCache::new(
                config.cache_max_entries,
                Duration::from_secs(config.cache_ttl_secs),
            ),
            load_semaphore: Semaphore::new(config.max_concurrent_loads),
            config,
        }
    }

    /// Create a new vector backend with default configuration.
    pub fn with_defaults(loader: L) -> Self {
        Self::new(loader, VectorBackendConfig::default())
    }

    /// Get or load an index for the given graph source and transaction.
    async fn get_or_load_index(
        &self,
        graph_source_id: &str,
        index_t: i64,
    ) -> Result<Arc<VectorIndex>> {
        let cache_key = (graph_source_id.to_string(), index_t);

        // Check cache first
        if let Some(index) = self.cache.get(&cache_key) {
            tracing::debug!(graph_source_id, index_t, "Vector index cache hit");
            return Ok(index);
        }

        // Acquire semaphore to limit concurrent loads
        let _permit = self
            .load_semaphore
            .acquire()
            .await
            .map_err(|_| ServiceError::Internal {
                message: "failed to acquire load semaphore".to_string(),
            })?;

        // Double-check cache after acquiring semaphore
        if let Some(index) = self.cache.get(&cache_key) {
            return Ok(index);
        }

        // Load from storage
        tracing::debug!(
            graph_source_id,
            index_t,
            "Loading vector index from storage"
        );
        let index = self.loader.load_index(graph_source_id, index_t).await?;
        let index = Arc::new(index);

        // Insert into cache
        self.cache.insert(cache_key, index.clone());

        Ok(index)
    }

    /// Resolve the index transaction to use for a search request.
    ///
    /// Vector indexes are head-only: always returns the latest snapshot.
    /// If `sync` is true, waits for any index head to appear first.
    async fn resolve_index_t(
        &self,
        graph_source_id: &str,
        sync: bool,
        timeout: Duration,
    ) -> Result<i64> {
        if sync {
            wait_for_head(
                || async { self.loader.get_index_head(graph_source_id).await },
                None,
                timeout,
                &self.config.sync_config,
            )
            .await?;
        }

        self.loader
            .get_latest_index_t(graph_source_id)
            .await?
            .ok_or_else(|| ServiceError::IndexNotBuilt {
                address: graph_source_id.to_string(),
            })
    }
}

impl<L: VectorIndexLoader> std::fmt::Debug for VectorBackend<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorBackend")
            .field("loader", &self.loader)
            .field("cache", &self.cache)
            .field("config", &self.config)
            .finish()
    }
}

#[async_trait]
impl<L: VectorIndexLoader> SearchBackend for VectorBackend<L> {
    async fn search(
        &self,
        graph_source_id: &str,
        query: &QueryVariant,
        limit: usize,
        as_of_t: Option<i64>,
        sync: bool,
        timeout_ms: Option<u64>,
    ) -> Result<(i64, Vec<SearchHit>)> {
        // Vector indexes are head-only: reject as_of_t
        if as_of_t.is_some() {
            return Err(ServiceError::InvalidRequest {
                message: format!(
                    "Vector index '{graph_source_id}' does not support time-travel queries (as_of_t). \
                     Only the latest snapshot is available."
                ),
            });
        }

        // Extract query vector and optional metric
        let (query_vector, requested_metric) = match query {
            QueryVariant::Vector { vector, metric } => (vector, metric.as_deref()),
            _ => {
                return Err(ServiceError::InvalidRequest {
                    message: format!(
                        "Vector backend only supports Vector queries, got {:?}",
                        std::mem::discriminant(query)
                    ),
                })
            }
        };

        let timeout = Duration::from_millis(timeout_ms.unwrap_or(self.config.default_timeout_ms));

        // Resolve which index snapshot to use (always head)
        let index_t = self.resolve_index_t(graph_source_id, sync, timeout).await?;

        // Load the index (from cache or storage)
        let index = self.get_or_load_index(graph_source_id, index_t).await?;

        // Validate metric if the client specified one
        if let Some(metric_str) = requested_metric {
            use fluree_db_query::vector::DistanceMetric;
            let requested =
                DistanceMetric::parse(metric_str).ok_or_else(|| ServiceError::InvalidRequest {
                    message: format!("Unknown distance metric: '{metric_str}'"),
                })?;
            let index_metric = index.metric();
            if requested != index_metric {
                return Err(ServiceError::InvalidRequest {
                    message: format!(
                        "Metric mismatch for '{graph_source_id}': request asks for {requested} but index uses {index_metric}"
                    ),
                });
            }
        }

        // Execute vector search
        let results = index
            .search(query_vector, limit)
            .map_err(|e| ServiceError::Internal {
                message: format!("Vector search error: {e}"),
            })?;

        // Convert to SearchHit
        let hits: Vec<SearchHit> = results
            .into_iter()
            .map(|r| SearchHit::new(r.iri.to_string(), r.ledger_alias.to_string(), r.score))
            .collect();

        Ok((index_t, hits))
    }

    fn supports(&self, query: &QueryVariant) -> bool {
        // Only advertise support for Vector queries.
        // VectorSimilarTo requires server-side entity resolution (not yet implemented).
        matches!(query, QueryVariant::Vector { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_query::vector::usearch::VectorIndex;
    use fluree_db_query::vector::DistanceMetric;
    use std::collections::{HashMap, HashSet};
    use std::sync::RwLock as StdRwLock;

    /// Mock index loader for testing.
    ///
    /// Stores which (alias, t) pairs have indexes and rebuilds fresh indexes
    /// on each load (since `VectorIndex` doesn't implement `Clone`).
    #[derive(Debug, Default)]
    struct MockLoader {
        /// Set of (graph_source_id, index_t) pairs that have indexes.
        known_indexes: StdRwLock<HashSet<(String, i64)>>,
        latest_t: StdRwLock<HashMap<String, i64>>,
        head_t: StdRwLock<HashMap<String, i64>>,
    }

    impl MockLoader {
        fn add_index(&self, graph_source_id: &str, index_t: i64, _index: VectorIndex) {
            self.known_indexes
                .write()
                .unwrap()
                .insert((graph_source_id.to_string(), index_t));
            let mut latest = self.latest_t.write().unwrap();
            let current = latest.entry(graph_source_id.to_string()).or_insert(0);
            if index_t > *current {
                *current = index_t;
            }
        }

        fn set_head(&self, graph_source_id: &str, t: i64) {
            self.head_t
                .write()
                .unwrap()
                .insert(graph_source_id.to_string(), t);
        }
    }

    #[async_trait]
    impl VectorIndexLoader for MockLoader {
        async fn load_index(&self, graph_source_id: &str, index_t: i64) -> Result<VectorIndex> {
            if self
                .known_indexes
                .read()
                .unwrap()
                .contains(&(graph_source_id.to_string(), index_t))
            {
                // Rebuild a fresh test index (same content each time)
                Ok(build_test_index())
            } else {
                Err(ServiceError::IndexNotBuilt {
                    address: graph_source_id.to_string(),
                })
            }
        }

        async fn get_latest_index_t(&self, graph_source_id: &str) -> Result<Option<i64>> {
            Ok(self.latest_t.read().unwrap().get(graph_source_id).copied())
        }

        async fn get_index_head(&self, graph_source_id: &str) -> Result<Option<i64>> {
            Ok(self.head_t.read().unwrap().get(graph_source_id).copied())
        }
    }

    fn build_test_index() -> VectorIndex {
        let mut index = VectorIndex::new(3, DistanceMetric::Cosine).unwrap();
        index
            .add("ledger:main", "http://example.org/doc1", &[0.9, 0.1, 0.05])
            .unwrap();
        index
            .add("ledger:main", "http://example.org/doc2", &[0.1, 0.9, 0.05])
            .unwrap();
        index
    }

    #[tokio::test]
    async fn test_vector_backend_basic_search() {
        let loader = MockLoader::default();
        loader.add_index("embeddings:main", 100, build_test_index());
        loader.set_head("embeddings:main", 100);

        let backend = VectorBackend::with_defaults(loader);

        let (index_t, hits) = backend
            .search(
                "embeddings:main",
                &QueryVariant::Vector {
                    vector: vec![0.85, 0.1, 0.05],
                    metric: Some("cosine".to_string()),
                },
                10,
                None,
                false,
                None,
            )
            .await
            .unwrap();

        assert_eq!(index_t, 100);
        assert!(!hits.is_empty());
        // First result should be doc1 (most similar to query)
        assert_eq!(hits[0].iri, "http://example.org/doc1");
    }

    #[tokio::test]
    async fn test_vector_backend_rejects_as_of_t() {
        let loader = MockLoader::default();
        loader.add_index("search:main", 100, build_test_index());
        loader.set_head("search:main", 100);

        let backend = VectorBackend::with_defaults(loader);

        // Vector indexes are head-only: as_of_t must be rejected
        let result = backend
            .search(
                "search:main",
                &QueryVariant::Vector {
                    vector: vec![0.9, 0.1, 0.05],
                    metric: None,
                },
                10,
                Some(50),
                false,
                None,
            )
            .await;

        assert!(matches!(result, Err(ServiceError::InvalidRequest { .. })));
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("does not support time-travel"),
            "Error message should mention time-travel, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_vector_backend_index_not_built() {
        let loader = MockLoader::default();
        // No indexes registered

        let backend = VectorBackend::with_defaults(loader);

        // Query without any index should return IndexNotBuilt
        let result = backend
            .search(
                "search:main",
                &QueryVariant::Vector {
                    vector: vec![0.5, 0.5, 0.0],
                    metric: None,
                },
                10,
                None,
                false,
                None,
            )
            .await;

        assert!(matches!(result, Err(ServiceError::IndexNotBuilt { .. })));
    }

    #[tokio::test]
    async fn test_vector_backend_caching() {
        let loader = MockLoader::default();
        loader.add_index("search:main", 100, build_test_index());

        let backend = VectorBackend::with_defaults(loader);

        // First search loads from storage
        let _ = backend
            .search(
                "search:main",
                &QueryVariant::Vector {
                    vector: vec![0.9, 0.1, 0.05],
                    metric: None,
                },
                10,
                None,
                false,
                None,
            )
            .await
            .unwrap();

        // Second search should use cache
        let _ = backend
            .search(
                "search:main",
                &QueryVariant::Vector {
                    vector: vec![0.1, 0.9, 0.05],
                    metric: None,
                },
                10,
                None,
                false,
                None,
            )
            .await
            .unwrap();

        // Verify cache has the entry
        assert_eq!(backend.cache.len(), 1);
    }

    #[tokio::test]
    async fn test_vector_backend_supports() {
        let loader = MockLoader::default();
        let backend = VectorBackend::with_defaults(loader);

        assert!(backend.supports(&QueryVariant::Vector {
            vector: vec![1.0],
            metric: None
        }));
        // VectorSimilarTo is NOT supported (requires server-side entity resolution)
        assert!(!backend.supports(&QueryVariant::VectorSimilarTo {
            to_iri: "ex:item".to_string(),
            metric: None
        }));
        assert!(!backend.supports(&QueryVariant::Bm25 {
            text: "test".to_string()
        }));
    }

    #[tokio::test]
    async fn test_vector_backend_rejects_bm25_query() {
        let loader = MockLoader::default();
        loader.add_index("search:main", 100, build_test_index());

        let backend = VectorBackend::with_defaults(loader);

        let result = backend
            .search(
                "search:main",
                &QueryVariant::Bm25 {
                    text: "test".to_string(),
                },
                10,
                None,
                false,
                None,
            )
            .await;

        assert!(matches!(result, Err(ServiceError::InvalidRequest { .. })));
    }
}
