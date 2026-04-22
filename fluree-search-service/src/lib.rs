//! Search service for Fluree DB.
//!
//! This crate provides the search service implementation with BM25 backend
//! and caching support. It handles search requests according to the
//! `fluree-search-protocol` wire contract.
//!
//! # Architecture
//!
//! The service is composed of:
//!
//! - [`SearchService`]: Main entry point that handles protocol requests
//! - [`SearchBackend`]: Trait for search implementations (BM25, vector, etc.)
//! - [`IndexCache`]: LRU cache with TTL for index snapshots
//! - Sync utilities for waiting on index updates
//!
//! # Features
//!
//! - `bm25` (default): Enable BM25 text search backend
//! - `native`: Enable native storage backends
//!
//! # Example
//!
//! ```ignore
//! use fluree_search_service::{SearchService, ServiceConfig};
//!
//! let service = SearchService::new(storage, nameservice, ServiceConfig::default());
//!
//! let response = service.handle_request(request).await?;
//! ```

pub mod backend;
pub mod cache;
pub mod error;
pub mod sync;

pub use backend::SearchBackend;
#[cfg(feature = "bm25")]
pub use backend::{Bm25Backend, Bm25BackendConfig};
pub use cache::IndexCache;
pub use error::{Result, ServiceError};
pub use sync::{wait_for_head, SyncConfig};

use fluree_search_protocol::{
    Capabilities, ErrorCode, QueryVariant, SearchError, SearchRequest, SearchResponse,
    PROTOCOL_VERSION,
};
use std::sync::Arc;
use std::time::Instant;

/// Service configuration.
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    /// Maximum allowed limit for search results.
    pub max_limit: usize,
    /// Maximum allowed timeout in milliseconds.
    pub max_timeout_ms: u64,
    /// Default timeout in milliseconds.
    pub default_timeout_ms: u64,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            max_limit: 1000,
            max_timeout_ms: 300_000,    // 5 minutes
            default_timeout_ms: 30_000, // 30 seconds
        }
    }
}

/// Main search service.
///
/// Handles search requests and routes them to the appropriate backend.
pub struct SearchService<B: SearchBackend> {
    /// Search backend.
    backend: Arc<B>,
    /// Service configuration.
    config: ServiceConfig,
}

impl<B: SearchBackend> SearchService<B> {
    /// Create a new search service.
    pub fn new(backend: B, config: ServiceConfig) -> Self {
        Self {
            backend: Arc::new(backend),
            config,
        }
    }

    /// Create a new search service with default configuration.
    pub fn with_defaults(backend: B) -> Self {
        Self::new(backend, ServiceConfig::default())
    }

    /// Handle a search request.
    ///
    /// This is the main entry point for processing search requests.
    /// It validates the request, routes to the backend, and formats
    /// the response according to the protocol.
    pub async fn handle_request(
        &self,
        request: SearchRequest,
    ) -> std::result::Result<SearchResponse, SearchError> {
        let start = Instant::now();
        let request_id = request.request_id.clone();

        // Validate protocol version
        if request.protocol_version != PROTOCOL_VERSION {
            return Err(SearchError::new(
                PROTOCOL_VERSION,
                request_id,
                ErrorCode::UnsupportedProtocolVersion,
                format!(
                    "unsupported protocol version: {} (expected {})",
                    request.protocol_version, PROTOCOL_VERSION
                ),
            ));
        }

        // Validate limit
        let limit = request.limit.min(self.config.max_limit);
        if request.limit > self.config.max_limit {
            tracing::warn!(
                requested = request.limit,
                max = self.config.max_limit,
                "limit clamped to max"
            );
        }

        // Validate timeout
        let timeout_ms = request
            .timeout_ms
            .unwrap_or(self.config.default_timeout_ms)
            .min(self.config.max_timeout_ms);

        // Check if backend supports this query type
        if !self.backend.supports(&request.query) {
            return Err(SearchError::new(
                PROTOCOL_VERSION,
                request_id,
                ErrorCode::InvalidRequest,
                format!("unsupported query type: {:?}", query_kind(&request.query)),
            ));
        }

        // Execute search
        let (index_t, hits) = self
            .backend
            .search(
                &request.graph_source_id,
                &request.query,
                limit,
                request.as_of_t,
                request.sync,
                Some(timeout_ms),
            )
            .await
            .map_err(|e| {
                SearchError::new(
                    PROTOCOL_VERSION,
                    request_id.clone(),
                    e.error_code(),
                    e.to_string(),
                )
            })?;

        let took_ms = start.elapsed().as_millis() as u64;

        Ok(SearchResponse::new(
            PROTOCOL_VERSION.to_string(),
            request_id,
            index_t,
            hits,
            took_ms,
        ))
    }

    /// Get service capabilities.
    pub fn capabilities(&self) -> Capabilities {
        use fluree_search_protocol::BM25_ANALYZER_VERSION;

        let mut supported_kinds = vec![];

        // Check what queries the backend supports
        if self.backend.supports(&QueryVariant::Bm25 {
            text: String::new(),
        }) {
            supported_kinds.push("bm25".to_string());
        }
        if self.backend.supports(&QueryVariant::Vector {
            vector: vec![],
            metric: None,
        }) {
            supported_kinds.push("vector".to_string());
        }
        if self.backend.supports(&QueryVariant::VectorSimilarTo {
            to_iri: String::new(),
            metric: None,
        }) {
            supported_kinds.push("vector_similar_to".to_string());
        }

        Capabilities {
            protocol_version: PROTOCOL_VERSION.to_string(),
            bm25_analyzer_version: BM25_ANALYZER_VERSION.to_string(),
            supported_query_kinds: supported_kinds,
            max_limit: self.config.max_limit,
            max_timeout_ms: self.config.max_timeout_ms,
        }
    }
}

impl<B: SearchBackend> std::fmt::Debug for SearchService<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchService")
            .field("backend", &self.backend)
            .field("config", &self.config)
            .finish()
    }
}

/// Extract the kind string from a query variant.
fn query_kind(query: &QueryVariant) -> &'static str {
    match query {
        QueryVariant::Bm25 { .. } => "bm25",
        QueryVariant::Vector { .. } => "vector",
        QueryVariant::VectorSimilarTo { .. } => "vector_similar_to",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use fluree_search_protocol::SearchHit;

    /// Mock backend for testing.
    #[derive(Debug)]
    struct MockBackend {
        hits: Vec<SearchHit>,
        index_t: i64,
    }

    impl MockBackend {
        fn new() -> Self {
            Self {
                hits: vec![SearchHit::new(
                    "http://example.org/doc1".to_string(),
                    "ledger:main".to_string(),
                    1.5,
                )],
                index_t: 100,
            }
        }
    }

    #[async_trait]
    impl SearchBackend for MockBackend {
        async fn search(
            &self,
            _graph_source_id: &str,
            _query: &QueryVariant,
            _limit: usize,
            _as_of_t: Option<i64>,
            _sync: bool,
            _timeout_ms: Option<u64>,
        ) -> Result<(i64, Vec<SearchHit>)> {
            Ok((self.index_t, self.hits.clone()))
        }

        fn supports(&self, query: &QueryVariant) -> bool {
            matches!(query, QueryVariant::Bm25 { .. })
        }
    }

    #[tokio::test]
    async fn test_service_handle_request() {
        let service = SearchService::with_defaults(MockBackend::new());

        let request = SearchRequest::bm25("search:main", "test query", 10);
        let response = service.handle_request(request).await.unwrap();

        assert_eq!(response.index_t, 100);
        assert_eq!(response.hits.len(), 1);
        assert_eq!(response.protocol_version, PROTOCOL_VERSION);
    }

    #[tokio::test]
    async fn test_service_unsupported_protocol() {
        let service = SearchService::with_defaults(MockBackend::new());

        let mut request = SearchRequest::bm25("search:main", "test query", 10);
        request.protocol_version = "0.0".to_string();

        let result = service.handle_request(request).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.error.code, ErrorCode::UnsupportedProtocolVersion);
    }

    #[tokio::test]
    async fn test_service_unsupported_query_type() {
        let service = SearchService::with_defaults(MockBackend::new());

        let request = SearchRequest::vector("search:main", vec![1.0, 2.0], 10);
        let result = service.handle_request(request).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.error.code, ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    async fn test_service_capabilities() {
        let service = SearchService::with_defaults(MockBackend::new());

        let caps = service.capabilities();
        assert_eq!(caps.protocol_version, PROTOCOL_VERSION);
        assert!(caps.supported_query_kinds.contains(&"bm25".to_string()));
        assert!(!caps.supported_query_kinds.contains(&"vector".to_string()));
    }

    #[tokio::test]
    async fn test_service_limit_clamping() {
        let config = ServiceConfig {
            max_limit: 5,
            ..Default::default()
        };
        let service = SearchService::new(MockBackend::new(), config);

        // Request with limit > max should be clamped (no error)
        let request = SearchRequest::bm25("search:main", "test", 100);
        let result = service.handle_request(request).await;

        // Should succeed (limit is clamped, not rejected)
        assert!(result.is_ok());
    }
}
