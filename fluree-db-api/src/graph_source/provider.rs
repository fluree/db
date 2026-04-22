//! Index providers for query execution.
//!
//! This module provides implementations of index provider traits used by the
//! query engine to access BM25 indexes during query execution.
//!
//! # Architecture
//!
//! [`FlureeIndexProvider`] implements both [`Bm25IndexProvider`] (for legacy index access)
//! and [`Bm25SearchProvider`] (for search results). The search provider implementation
//! routes between embedded and remote modes based on graph source deployment configuration.
//!
//! # Deployment Modes
//!
//! - **Embedded** (default): Uses [`EmbeddedBm25SearchProvider`] to load the index locally
//!   and perform scoring in-process.
//! - **Remote** (feature-gated): Uses [`RemoteBm25SearchProvider`] to delegate search
//!   to a remote search service via HTTP.

use async_trait::async_trait;
use fluree_db_core::ContentStore;
use fluree_db_query::bm25::{Bm25Index, Bm25IndexProvider, Bm25SearchProvider, Bm25SearchResult};
use fluree_db_query::error::{QueryError, Result as QueryResult};
use std::sync::Arc;
use tracing::{debug, info};

use crate::search::{DeploymentMode, EmbeddedBm25SearchProvider, SearchDeploymentConfig};

#[cfg(feature = "search-remote-client")]
use crate::search::RemoteBm25SearchProvider;

// Used inside the VectorIndexProvider impl's Remote deployment arm.
// Needs both features: search-remote-client (for the HTTP client) and
// vector (for the VectorIndexProvider impl block).
#[cfg(all(feature = "search-remote-client", feature = "vector"))]
use crate::search::RemoteVectorSearchProvider;

#[cfg(feature = "vector")]
use fluree_db_query::vector::{VectorIndexProvider, VectorSearchHit, VectorSearchParams};

/// BM25 index provider for query execution.
///
/// This provider implements `Bm25IndexProvider` for use with the query engine's
/// `ExecutionContext`. It handles:
///
/// - Loading BM25 indexes from storage
/// - Time-travel semantics (checking watermarks against `as_of_t`)
/// - Automatic sync when requested
///
/// # Example
///
/// ```ignore
/// use fluree_db_api::FlureeIndexProvider;
///
/// let provider = FlureeIndexProvider::new(&fluree);
/// let mut ctx = ExecutionContext::new(&db, &vars);
/// ctx.bm25_provider = Some(&provider);
/// ```
pub struct FlureeIndexProvider<'a> {
    fluree: &'a crate::Fluree,
}

impl<'a> FlureeIndexProvider<'a> {
    /// Create a new index provider wrapping a Fluree instance.
    pub fn new(fluree: &'a crate::Fluree) -> Self {
        Self { fluree }
    }
}

impl std::fmt::Debug for FlureeIndexProvider<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlureeIndexProvider")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Bm25IndexProvider for FlureeIndexProvider<'_> {
    /// Load a BM25 index for query execution with time-travel support.
    ///
    /// This method implements full time-travel semantics for BM25 queries:
    ///
    /// 1. Looks up the snapshot history from nameservice
    /// 2. Selects the best snapshot (largest `index_t` <= `as_of_t`)
    /// 3. If no suitable snapshot and sync=true, syncs to target_t and retries
    /// 4. Loads and returns the selected snapshot
    ///
    /// # Arguments
    ///
    /// * `graph_source_id` - Graph source alias (e.g., "my-search:main")
    /// * `as_of_t` - Target transaction time for time-travel query
    /// * `sync` - Whether to sync if no suitable snapshot exists
    /// * `timeout_ms` - Optional timeout for sync operations
    async fn bm25_index(
        &self,
        graph_source_id: &str,
        as_of_t: Option<i64>,
        sync: bool,
        timeout_ms: Option<u64>,
    ) -> QueryResult<Arc<Bm25Index>> {
        // Load BM25 manifest from CAS
        let manifest = self
            .fluree
            .load_or_create_bm25_manifest(graph_source_id)
            .await
            .map_err(|e| QueryError::Internal(format!("Manifest load error: {e}")))?;

        // Try to select best snapshot for as_of_t.
        //
        // In dataset mode there is no meaningful dataset-level `t`, so callers may pass `None`
        // to mean "latest". Implement this by selecting with a very large bound.
        let effective_as_of_t = as_of_t.unwrap_or(i64::MAX);
        let as_of_label = as_of_t
            .map(|t| t.to_string())
            .unwrap_or_else(|| "latest".to_string());
        let selection = manifest.select_snapshot(effective_as_of_t);

        // If we have a suitable snapshot, load and return it
        if let Some(entry) = selection {
            let cs = self.fluree.content_store(graph_source_id);
            let bytes = cs
                .get(&entry.snapshot_id)
                .await
                .map_err(|e| QueryError::Internal(format!("Storage error: {e}")))?;

            let index = self
                .fluree
                .load_bm25_from_bytes(graph_source_id, &bytes)
                .await
                .map_err(|e| QueryError::Internal(format!("Deserialize error: {e}")))?;

            info!(
                graph_source_id = %graph_source_id,
                as_of_t = effective_as_of_t,
                snapshot_t = entry.index_t,
                "Loaded BM25 snapshot for time-travel query"
            );

            return Ok(Arc::new(index));
        }

        // No suitable snapshot found. Try to sync if requested.
        if sync {
            let _ = timeout_ms; // reserved for future commit-delta based sync with timeout

            // Sync to head (which will create a new snapshot in the manifest)
            self.fluree
                .sync_bm25_index(graph_source_id)
                .await
                .map_err(|e| QueryError::Internal(format!("Sync error: {e}")))?;

            // Re-load manifest after sync
            let manifest = self
                .fluree
                .load_or_create_bm25_manifest(graph_source_id)
                .await
                .map_err(|e| QueryError::Internal(format!("Manifest load error: {e}")))?;

            let selection = manifest.select_snapshot(effective_as_of_t);

            if let Some(entry) = selection {
                let cs = self.fluree.content_store(graph_source_id);
                let bytes = cs
                    .get(&entry.snapshot_id)
                    .await
                    .map_err(|e| QueryError::Internal(format!("Storage error: {e}")))?;

                let index = self
                    .fluree
                    .load_bm25_from_bytes(graph_source_id, &bytes)
                    .await
                    .map_err(|e| QueryError::Internal(format!("Deserialize error: {e}")))?;

                info!(
                    graph_source_id = %graph_source_id,
                    as_of_t = %as_of_label,
                    snapshot_t = entry.index_t,
                    "Loaded BM25 snapshot after sync"
                );

                return Ok(Arc::new(index));
            }

            return Err(QueryError::InvalidQuery(format!(
                "No BM25 snapshot available for {graph_source_id} at t={as_of_label}. The earliest snapshot may be later than requested."
            )));
        }

        // No sync requested and no suitable snapshot
        let available = if let Some(head) = manifest.head() {
            format!(
                " Available snapshots: earliest t={}, latest t={}.",
                manifest.snapshots.first().map(|s| s.index_t).unwrap_or(0),
                head.index_t
            )
        } else {
            " No snapshots available.".to_string()
        };

        Err(QueryError::InvalidQuery(format!(
            "No BM25 snapshot available for {graph_source_id} at t={as_of_label}.{available}"
        )))
    }
}

#[async_trait]
impl Bm25SearchProvider for FlureeIndexProvider<'_> {
    /// Execute a BM25 search with time-travel support.
    ///
    /// This implementation routes between embedded and remote modes based on
    /// the graph source's deployment configuration stored in the nameservice.
    ///
    /// # Deployment Modes
    ///
    /// - **Embedded** (default): Uses [`EmbeddedBm25SearchProvider`] to load the index
    ///   locally and perform scoring in-process.
    /// - **Remote**: Uses [`RemoteBm25SearchProvider`] to delegate search to a remote
    ///   search service via HTTP. Requires `search-remote-client` feature.
    ///
    /// # Arguments
    ///
    /// * `graph_source_id` - Graph source alias (e.g., "products-search:main")
    /// * `query_text` - The search query text
    /// * `limit` - Maximum number of results to return
    /// * `as_of_t` - Target transaction time for time-travel (None = latest)
    /// * `sync` - Whether to sync if index is stale
    /// * `timeout_ms` - Optional timeout for the operation
    async fn search_bm25(
        &self,
        graph_source_id: &str,
        query_text: &str,
        limit: usize,
        as_of_t: Option<i64>,
        sync: bool,
        timeout_ms: Option<u64>,
    ) -> QueryResult<Bm25SearchResult> {
        // Look up graph source record to get deployment configuration
        let deployment_config = self.get_deployment_config(graph_source_id).await?;

        match deployment_config.mode {
            DeploymentMode::Embedded if self.fluree.should_use_chunked_format() => {
                debug!(
                    graph_source_id = %graph_source_id,
                    "Using selective chunked search mode"
                );
                self.search_bm25_selective(
                    graph_source_id, query_text, limit, as_of_t, sync, timeout_ms,
                )
                .await
            }
            DeploymentMode::Embedded => {
                debug!(graph_source_id = %graph_source_id, "Using embedded search mode");
                let adapter = EmbeddedBm25SearchProvider::new(self);
                adapter
                    .search_bm25(graph_source_id, query_text, limit, as_of_t, sync, timeout_ms)
                    .await
            }
            #[cfg(feature = "search-remote-client")]
            DeploymentMode::Remote => {
                debug!(
                    graph_source_id = %graph_source_id,
                    endpoint = ?deployment_config.endpoint,
                    "Using remote search mode"
                );
                let client = RemoteBm25SearchProvider::from_config(&deployment_config)?;
                client
                    .search_bm25(graph_source_id, query_text, limit, as_of_t, sync, timeout_ms)
                    .await
            }
            #[cfg(not(feature = "search-remote-client"))]
            DeploymentMode::Remote => {
                Err(QueryError::InvalidQuery(format!(
                    "Remote search mode not available for graph source '{}': 'search-remote-client' feature not enabled",
                    graph_source_id
                )))
            }
        }
    }
}

impl FlureeIndexProvider<'_> {
    /// Get deployment configuration for a graph source.
    ///
    /// Looks up the graph source record from nameservice and parses the deployment
    /// configuration from its JSON config. Defaults to embedded mode if
    /// no deployment config is specified.
    async fn get_deployment_config(
        &self,
        graph_source_id: &str,
    ) -> QueryResult<SearchDeploymentConfig> {
        // Look up graph source record from nameservice
        let gs_record = self
            .fluree
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await
            .map_err(|e| QueryError::Internal(format!("Nameservice error: {e}")))?;

        let Some(record) = gs_record else {
            // Graph source not found - return default embedded mode
            // The actual search will fail later with a more specific error
            return Ok(SearchDeploymentConfig::default());
        };

        // Try to parse deployment config from the graph source's JSON config
        parse_deployment_from_gs_config(&record.config)
    }
}

impl FlureeIndexProvider<'_> {
    /// Selective search for v4 chunked snapshots.
    ///
    /// Handles the full lifecycle (manifest, snapshot selection, sync, selective
    /// leaflet loading, scoring) — same contract as `EmbeddedBm25SearchProvider`
    /// but fetches only the posting leaflets needed for the query terms.
    async fn search_bm25_selective(
        &self,
        graph_source_id: &str,
        query_text: &str,
        limit: usize,
        as_of_t: Option<i64>,
        sync: bool,
        timeout_ms: Option<u64>,
    ) -> QueryResult<Bm25SearchResult> {
        // Load manifest
        let manifest = self
            .fluree
            .load_or_create_bm25_manifest(graph_source_id)
            .await
            .map_err(|e| QueryError::Internal(format!("Manifest load error: {e}")))?;

        let effective_as_of_t = as_of_t.unwrap_or(i64::MAX);
        let selection = manifest.select_snapshot(effective_as_of_t);

        // Load snapshot bytes
        let snapshot_bytes = if let Some(entry) = selection {
            let cs = self.fluree.content_store(graph_source_id);
            cs.get(&entry.snapshot_id)
                .await
                .map_err(|e| QueryError::Internal(format!("Storage error: {e}")))?
        } else if sync {
            let _ = timeout_ms;
            self.fluree
                .sync_bm25_index(graph_source_id)
                .await
                .map_err(|e| QueryError::Internal(format!("Sync error: {e}")))?;

            let manifest = self
                .fluree
                .load_or_create_bm25_manifest(graph_source_id)
                .await
                .map_err(|e| QueryError::Internal(format!("Manifest load error: {e}")))?;

            let entry = manifest.select_snapshot(effective_as_of_t).ok_or_else(|| {
                QueryError::InvalidQuery(format!(
                    "No BM25 snapshot available for {graph_source_id} after sync"
                ))
            })?;

            let cs = self.fluree.content_store(graph_source_id);
            cs.get(&entry.snapshot_id)
                .await
                .map_err(|e| QueryError::Internal(format!("Storage error: {e}")))?
        } else {
            return Err(QueryError::InvalidQuery(format!(
                "No BM25 snapshot available for {graph_source_id}"
            )));
        };

        // Delegate to selective search (handles v4 detection internally)
        self.fluree
            .search_bm25_selective(graph_source_id, &snapshot_bytes, query_text, limit)
            .await
            .map_err(|e| QueryError::Internal(format!("Selective search error: {e}")))
    }
}

/// Parse deployment configuration from a graph source's JSON config string.
///
/// Looks for a "deployment" object in the config. If not present or
/// parsing fails, returns default (embedded mode).
fn parse_deployment_from_gs_config(config_json: &str) -> QueryResult<SearchDeploymentConfig> {
    // Empty config means embedded mode
    if config_json.trim().is_empty() {
        return Ok(SearchDeploymentConfig::default());
    }

    // Parse the full config JSON
    let config: serde_json::Value = serde_json::from_str(config_json).map_err(|e| {
        QueryError::Internal(format!("Failed to parse graph source config JSON: {e}"))
    })?;

    // Look for "deployment" field
    if let Some(deployment_value) = config.get("deployment") {
        // Parse deployment config
        let deployment: SearchDeploymentConfig = serde_json::from_value(deployment_value.clone())
            .map_err(|e| {
            QueryError::Internal(format!("Failed to parse deployment config: {e}"))
        })?;
        return Ok(deployment);
    }

    // No deployment config - use default (embedded)
    Ok(SearchDeploymentConfig::default())
}

// =============================================================================
// Vector Index Provider Implementation
// =============================================================================

#[cfg(feature = "vector")]
#[async_trait]
impl VectorIndexProvider for FlureeIndexProvider<'_> {
    /// Execute a vector similarity search with deployment routing and time-travel support.
    ///
    /// This implementation routes between embedded and remote modes based on
    /// the graph source's deployment configuration stored in the nameservice.
    ///
    /// # Deployment Modes
    ///
    /// - **Embedded** (default): Loads the vector index snapshot locally and performs
    ///   similarity search in-process using the embedded vector index.
    /// - **Remote**: Delegates search to a remote search service via HTTP.
    ///   Requires `search-remote-client` feature.
    ///
    /// # Arguments
    ///
    /// * `graph_source_id` - Graph source alias (e.g., "embeddings:main")
    /// * `params` - Search parameters (query vector, metric, limit, etc.)
    async fn search(
        &self,
        graph_source_id: &str,
        params: VectorSearchParams<'_>,
    ) -> QueryResult<Vec<VectorSearchHit>> {
        // Look up graph source record to get deployment configuration
        let deployment_config = self.get_deployment_config(graph_source_id).await?;

        match deployment_config.mode {
            DeploymentMode::Embedded => {
                debug!(graph_source_id = %graph_source_id, "Using embedded vector search mode");
                self.search_vector_embedded(graph_source_id, &params).await
            }
            #[cfg(feature = "search-remote-client")]
            DeploymentMode::Remote => {
                debug!(
                    graph_source_id = %graph_source_id,
                    endpoint = ?deployment_config.endpoint,
                    "Using remote vector search mode"
                );
                let client = RemoteVectorSearchProvider::from_config(&deployment_config)?;
                client
                    .search(graph_source_id, params)
                    .await
            }
            #[cfg(not(feature = "search-remote-client"))]
            DeploymentMode::Remote => {
                Err(QueryError::InvalidQuery(format!(
                    "Remote search mode not available for graph source '{}': 'search-remote-client' feature not enabled",
                    graph_source_id
                )))
            }
        }
    }

    /// Check if a vector index collection exists for the given graph source alias.
    async fn collection_exists(&self, graph_source_id: &str) -> QueryResult<bool> {
        let record = self
            .fluree
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await
            .map_err(|e| QueryError::Internal(format!("Nameservice error: {e}")))?;

        Ok(record.is_some() && record.map(|r| !r.retracted).unwrap_or(false))
    }
}

#[cfg(feature = "vector")]
impl FlureeIndexProvider<'_> {
    /// Embedded vector search implementation (head-only).
    ///
    /// Vector indexes do not support time-travel. Loads the head snapshot
    /// from nameservice and performs similarity search in-process.
    async fn search_vector_embedded(
        &self,
        graph_source_id: &str,
        params: &VectorSearchParams<'_>,
    ) -> QueryResult<Vec<VectorSearchHit>> {
        use fluree_db_query::vector::usearch::deserialize;

        let _ = params.timeout_ms; // Reserved for future use

        // Vector indexes are head-only -- reject as_of_t requests
        if params.as_of_t.is_some() {
            return Err(QueryError::InvalidQuery(format!(
                "Vector index '{graph_source_id}' does not support time-travel queries (as_of_t). \
                 Only the latest snapshot is available."
            )));
        }

        // Load head snapshot via nameservice head pointer
        let record = self
            .fluree
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await
            .map_err(|e| QueryError::Internal(format!("Nameservice error: {e}")))?;

        let record = record.ok_or_else(|| {
            QueryError::InvalidQuery(format!("Graph source not found: {graph_source_id}"))
        })?;

        let index_id = match &record.index_id {
            Some(id) => id.clone(),
            None => {
                // No index yet -- try to sync if requested
                if params.sync {
                    self.fluree
                        .sync_vector_index(graph_source_id)
                        .await
                        .map_err(|e| QueryError::Internal(format!("Sync error: {e}")))?;

                    // Re-lookup after sync
                    let record = self
                        .fluree
                        .nameservice()
                        .lookup_graph_source(graph_source_id)
                        .await
                        .map_err(|e| QueryError::Internal(format!("Nameservice error: {e}")))?
                        .ok_or_else(|| {
                            QueryError::Internal(format!(
                                "Graph source disappeared after sync: {graph_source_id}"
                            ))
                        })?;

                    record.index_id.ok_or_else(|| {
                        QueryError::InvalidQuery(format!(
                            "No vector index available for {graph_source_id} after sync"
                        ))
                    })?
                } else {
                    return Err(QueryError::InvalidQuery(format!(
                        "No vector index available for {graph_source_id}. Try syncing first."
                    )));
                }
            }
        };

        // Load and deserialize via content store
        let cs = self.fluree.content_store(graph_source_id);
        let bytes = cs
            .get(&index_id)
            .await
            .map_err(|e| QueryError::Internal(format!("Storage error: {e}")))?;

        let index = deserialize(&bytes)
            .map_err(|e| QueryError::Internal(format!("Deserialize error: {e}")))?;

        // Check metric compatibility
        if index.metadata.metric != params.metric {
            return Err(QueryError::InvalidQuery(format!(
                "Vector index '{}' uses {:?} metric, but query requested {:?}",
                graph_source_id, index.metadata.metric, params.metric
            )));
        }

        debug!(
            graph_source_id = %graph_source_id,
            index_t = record.index_t,
            limit = params.limit,
            "Executing vector search (head-only)"
        );

        let results = index
            .search(params.query_vector, params.limit)
            .map_err(|e| QueryError::Internal(format!("Vector search error: {e}")))?;

        Ok(results
            .into_iter()
            .map(|r| VectorSearchHit::new(r.iri, r.ledger_alias, r.score))
            .collect())
    }
}
