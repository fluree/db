//! BM25 search backend implementation.
//!
//! This module provides the BM25 backend that handles:
//! - Index loading from storage
//! - Caching with LRU eviction and TTL
//! - Query analysis and scoring
//! - Sync/time-travel semantics

use super::SearchBackend;
use crate::cache::IndexCache;
use crate::error::{Result, ServiceError};
use crate::sync::{wait_for_head, SyncConfig};
use async_trait::async_trait;
use fluree_db_query::bm25::{Analyzer, Bm25Index, Bm25Scorer};
use fluree_search_protocol::{QueryVariant, SearchHit};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

/// Configuration for the BM25 backend.
#[derive(Debug, Clone)]
pub struct Bm25BackendConfig {
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

impl Default for Bm25BackendConfig {
    fn default() -> Self {
        Self {
            cache_max_entries: 100,
            cache_ttl_secs: 300, // 5 minutes
            max_concurrent_loads: 4,
            default_timeout_ms: 30_000,
            sync_config: SyncConfig::default(),
        }
    }
}

/// Trait for loading BM25 indexes from storage.
///
/// This abstracts the storage layer so the backend can be used
/// with different storage implementations.
#[async_trait]
pub trait IndexLoader: std::fmt::Debug + Send + Sync {
    /// Load the BM25 index for a graph source at a specific transaction.
    ///
    /// # Arguments
    ///
    /// * `graph_source_id` - Graph source alias
    /// * `index_t` - Transaction number of the index snapshot
    ///
    /// # Returns
    ///
    /// The loaded BM25 index, or an error if not found.
    async fn load_index(&self, graph_source_id: &str, index_t: i64) -> Result<Bm25Index>;

    /// Get the latest available index transaction for a graph source.
    ///
    /// Returns `None` if no index has been built yet.
    async fn get_latest_index_t(&self, graph_source_id: &str) -> Result<Option<i64>>;

    /// Find the newest index snapshot with watermark <= target_t.
    ///
    /// Returns `None` if no suitable snapshot exists.
    async fn find_snapshot_for_t(
        &self,
        graph_source_id: &str,
        target_t: i64,
    ) -> Result<Option<i64>>;

    /// Get the current nameservice index head for sync operations.
    ///
    /// This is the latest committed transaction that should be indexed.
    async fn get_index_head(&self, graph_source_id: &str) -> Result<Option<i64>>;
}

/// BM25 search backend.
///
/// Handles BM25 text search with caching, sync, and time-travel support.
pub struct Bm25Backend<L: IndexLoader> {
    /// Index loader (storage access).
    loader: L,
    /// Index cache.
    cache: IndexCache,
    /// Semaphore to limit concurrent index loads.
    load_semaphore: Semaphore,
    /// Text analyzer for query processing.
    analyzer: Analyzer,
    /// Configuration.
    config: Bm25BackendConfig,
}

impl<L: IndexLoader> Bm25Backend<L> {
    /// Create a new BM25 backend.
    pub fn new(loader: L, config: Bm25BackendConfig) -> Self {
        Self {
            loader,
            cache: IndexCache::new(
                config.cache_max_entries,
                Duration::from_secs(config.cache_ttl_secs),
            ),
            load_semaphore: Semaphore::new(config.max_concurrent_loads),
            analyzer: Analyzer::english_default(),
            config,
        }
    }

    /// Create a new BM25 backend with default configuration.
    pub fn with_defaults(loader: L) -> Self {
        Self::new(loader, Bm25BackendConfig::default())
    }

    /// Get or load an index for the given graph source and transaction.
    async fn get_or_load_index(
        &self,
        graph_source_id: &str,
        index_t: i64,
    ) -> Result<Arc<Bm25Index>> {
        let cache_key = (graph_source_id.to_string(), index_t);

        // Check cache first
        if let Some(index) = self.cache.get(&cache_key) {
            tracing::debug!(graph_source_id, index_t, "BM25 index cache hit");
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
        tracing::debug!(graph_source_id, index_t, "Loading BM25 index from storage");
        let index = self.loader.load_index(graph_source_id, index_t).await?;
        let index = Arc::new(index);

        // Insert into cache
        self.cache.insert(cache_key, index.clone());

        Ok(index)
    }

    /// Resolve the index transaction to use for a search request.
    ///
    /// Handles sync and time-travel semantics:
    /// - If `as_of_t` is Some, find the newest snapshot <= target
    /// - If `sync` is true, wait for index to reach head first
    /// - Otherwise, use the latest available snapshot
    async fn resolve_index_t(
        &self,
        graph_source_id: &str,
        as_of_t: Option<i64>,
        sync: bool,
        timeout: Duration,
    ) -> Result<i64> {
        if sync {
            // Wait for index to reach head (or as_of_t if specified)
            let target_t = as_of_t;
            wait_for_head(
                || async { self.loader.get_index_head(graph_source_id).await },
                target_t,
                timeout,
                &self.config.sync_config,
            )
            .await?;
        }

        match as_of_t {
            Some(target_t) => {
                // Find newest snapshot <= target_t
                self.loader
                    .find_snapshot_for_t(graph_source_id, target_t)
                    .await?
                    .ok_or(ServiceError::NoSnapshotForAsOfT { as_of_t: target_t })
            }
            None => {
                // Use latest available
                self.loader
                    .get_latest_index_t(graph_source_id)
                    .await?
                    .ok_or_else(|| ServiceError::IndexNotBuilt {
                        address: graph_source_id.to_string(),
                    })
            }
        }
    }
}

impl<L: IndexLoader> std::fmt::Debug for Bm25Backend<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bm25Backend")
            .field("loader", &self.loader)
            .field("cache", &self.cache)
            .field("config", &self.config)
            .finish()
    }
}

#[async_trait]
impl<L: IndexLoader> SearchBackend for Bm25Backend<L> {
    async fn search(
        &self,
        graph_source_id: &str,
        query: &QueryVariant,
        limit: usize,
        as_of_t: Option<i64>,
        sync: bool,
        timeout_ms: Option<u64>,
    ) -> Result<(i64, Vec<SearchHit>)> {
        // Extract query text
        let query_text = match query {
            QueryVariant::Bm25 { text } => text,
            _ => {
                return Err(ServiceError::InvalidRequest {
                    message: "BM25 backend only supports Bm25 queries".to_string(),
                })
            }
        };

        let timeout = Duration::from_millis(timeout_ms.unwrap_or(self.config.default_timeout_ms));

        // Resolve which index snapshot to use
        let index_t = self
            .resolve_index_t(graph_source_id, as_of_t, sync, timeout)
            .await?;

        // Load the index (from cache or storage)
        let index = self.get_or_load_index(graph_source_id, index_t).await?;

        // Analyze query
        let terms = self.analyzer.analyze_to_strings(query_text);
        if terms.is_empty() {
            return Ok((index_t, vec![]));
        }

        // Score documents
        let term_refs: Vec<&str> = terms.iter().map(std::string::String::as_str).collect();
        let scorer = Bm25Scorer::new(&index, &term_refs);
        let scored = scorer.top_k(limit);

        // Convert to SearchHit
        let hits: Vec<SearchHit> = scored
            .into_iter()
            .map(|(doc_key, score)| {
                SearchHit::new(
                    doc_key.subject_iri.to_string(),
                    doc_key.ledger_alias.to_string(),
                    score,
                )
            })
            .collect();

        Ok((index_t, hits))
    }

    fn supports(&self, query: &QueryVariant) -> bool {
        matches!(query, QueryVariant::Bm25 { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_query::bm25::DocKey;
    use std::collections::HashMap;
    use std::sync::RwLock;

    /// Mock index loader for testing.
    #[derive(Debug, Default)]
    struct MockLoader {
        indexes: RwLock<HashMap<(String, i64), Bm25Index>>,
        latest_t: RwLock<HashMap<String, i64>>,
        head_t: RwLock<HashMap<String, i64>>,
    }

    impl MockLoader {
        fn add_index(&self, graph_source_id: &str, index_t: i64, index: Bm25Index) {
            self.indexes
                .write()
                .unwrap()
                .insert((graph_source_id.to_string(), index_t), index);
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
    impl IndexLoader for MockLoader {
        async fn load_index(&self, graph_source_id: &str, index_t: i64) -> Result<Bm25Index> {
            self.indexes
                .read()
                .unwrap()
                .get(&(graph_source_id.to_string(), index_t))
                .cloned()
                .ok_or_else(|| ServiceError::IndexNotBuilt {
                    address: graph_source_id.to_string(),
                })
        }

        async fn get_latest_index_t(&self, graph_source_id: &str) -> Result<Option<i64>> {
            Ok(self.latest_t.read().unwrap().get(graph_source_id).copied())
        }

        async fn find_snapshot_for_t(
            &self,
            graph_source_id: &str,
            target_t: i64,
        ) -> Result<Option<i64>> {
            // Find newest snapshot <= target_t
            let indexes = self.indexes.read().unwrap();
            let mut best: Option<i64> = None;
            for (key, _) in indexes.iter() {
                if key.0 == graph_source_id && key.1 <= target_t {
                    match best {
                        None => best = Some(key.1),
                        Some(b) if key.1 > b => best = Some(key.1),
                        _ => {}
                    }
                }
            }
            Ok(best)
        }

        async fn get_index_head(&self, graph_source_id: &str) -> Result<Option<i64>> {
            Ok(self.head_t.read().unwrap().get(graph_source_id).copied())
        }
    }

    fn build_test_index() -> Bm25Index {
        let mut index = Bm25Index::new();
        index.add_document(
            DocKey::new("ledger:main", "http://example.org/doc1"),
            [("wireless", 2), ("headphones", 1)].into_iter().collect(),
        );
        index.add_document(
            DocKey::new("ledger:main", "http://example.org/doc2"),
            [("wired", 1), ("headphones", 1)].into_iter().collect(),
        );
        index
    }

    #[tokio::test]
    async fn test_bm25_backend_basic_search() {
        let loader = MockLoader::default();
        loader.add_index("products:main", 100, build_test_index());
        loader.set_head("products:main", 100);

        let backend = Bm25Backend::with_defaults(loader);

        let (index_t, hits) = backend
            .search(
                "products:main",
                &QueryVariant::Bm25 {
                    text: "wireless".to_string(),
                },
                10,
                None,
                false,
                None,
            )
            .await
            .unwrap();

        assert_eq!(index_t, 100);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].iri, "http://example.org/doc1");
    }

    #[tokio::test]
    async fn test_bm25_backend_time_travel() {
        let loader = MockLoader::default();

        // Add indexes at different times
        let mut index_v1 = Bm25Index::new();
        index_v1.add_document(
            DocKey::new("ledger:main", "http://example.org/doc1"),
            [("old", 1)].into_iter().collect(),
        );
        loader.add_index("search:main", 100, index_v1);

        let mut index_v2 = Bm25Index::new();
        index_v2.add_document(
            DocKey::new("ledger:main", "http://example.org/doc1"),
            [("fresh", 1)].into_iter().collect(),
        );
        loader.add_index("search:main", 200, index_v2);

        let backend = Bm25Backend::with_defaults(loader);

        // Query with as_of_t=150 should return index at t=100
        let (index_t, _) = backend
            .search(
                "search:main",
                &QueryVariant::Bm25 {
                    text: "old".to_string(),
                },
                10,
                Some(150),
                false,
                None,
            )
            .await
            .unwrap();

        assert_eq!(index_t, 100);
    }

    #[tokio::test]
    async fn test_bm25_backend_empty_query() {
        let loader = MockLoader::default();
        loader.add_index("search:main", 100, build_test_index());

        let backend = Bm25Backend::with_defaults(loader);

        // Query with only stopwords should return empty results
        let (_, hits) = backend
            .search(
                "search:main",
                &QueryVariant::Bm25 {
                    text: "the a an".to_string(),
                },
                10,
                None,
                false,
                None,
            )
            .await
            .unwrap();

        assert!(hits.is_empty());
    }

    #[tokio::test]
    async fn test_bm25_backend_no_snapshot_error() {
        let loader = MockLoader::default();
        loader.add_index("search:main", 200, build_test_index());

        let backend = Bm25Backend::with_defaults(loader);

        // Query with as_of_t=100 should fail (no snapshot at or before t=100)
        let result = backend
            .search(
                "search:main",
                &QueryVariant::Bm25 {
                    text: "wireless".to_string(),
                },
                10,
                Some(100),
                false,
                None,
            )
            .await;

        assert!(matches!(
            result,
            Err(ServiceError::NoSnapshotForAsOfT { .. })
        ));
    }

    #[tokio::test]
    async fn test_bm25_backend_caching() {
        let loader = MockLoader::default();
        loader.add_index("search:main", 100, build_test_index());

        let backend = Bm25Backend::with_defaults(loader);

        // First search loads from storage
        let _ = backend
            .search(
                "search:main",
                &QueryVariant::Bm25 {
                    text: "wireless".to_string(),
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
                &QueryVariant::Bm25 {
                    text: "headphones".to_string(),
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
    async fn test_bm25_backend_supports() {
        let loader = MockLoader::default();
        let backend = Bm25Backend::with_defaults(loader);

        assert!(backend.supports(&QueryVariant::Bm25 {
            text: "test".to_string()
        }));
        assert!(!backend.supports(&QueryVariant::Vector {
            vector: vec![1.0],
            metric: None
        }));
    }
}
