//! End-to-end integration tests for fluree-search-httpd.
//!
//! These tests verify:
//! 1. The HTTP server correctly handles search requests
//! 2. Parity between embedded and remote search modes (same IRIs, ordering, scores within epsilon)
//! 3. Error handling for various edge cases

use axum::body::Body;
use fluree_db_api::{Bm25CreateConfig, FlureeBuilder};
use fluree_db_core::FileStorage;
use fluree_db_nameservice::file::FileNameService;
use fluree_db_query::bm25::{Analyzer, Bm25Scorer};
use fluree_search_protocol::{SearchHit, SearchRequest, SearchResponse, PROTOCOL_VERSION};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value as JsonValue};
use std::path::PathBuf;

// =============================================================================
// Test utilities
// =============================================================================

/// Parse JSON response body.
async fn json_body(resp: http::Response<Body>) -> (StatusCode, JsonValue) {
    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();
    let json: JsonValue = serde_json::from_slice(&bytes).expect("valid JSON response");
    (status, json)
}

// =============================================================================
// HTTP endpoint tests
// =============================================================================

mod http_tests {
    use super::*;
    use async_trait::async_trait;
    use axum::extract::State;
    use axum::response::IntoResponse;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use fluree_db_core::ContentStore;
    use fluree_db_nameservice::GraphSourceLookup;
    use fluree_db_query::bm25::{deserialize, Bm25Index, Bm25Manifest};
    use fluree_search_service::backend::{
        Bm25Backend, Bm25BackendConfig, IndexLoader, SearchBackend,
    };
    use fluree_search_service::error::{Result as ServiceResult, ServiceError};
    use fluree_search_service::sync::SyncConfig;
    use std::sync::Arc;
    use tower::ServiceExt;

    /// Build the search httpd router for testing.
    fn build_test_router(storage_path: PathBuf, ns_path: PathBuf) -> Router {
        // Create index loader
        let loader = TestIndexLoader::new(storage_path, ns_path);

        // Create backend
        let backend_config = Bm25BackendConfig {
            cache_max_entries: 100,
            cache_ttl_secs: 300,
            max_concurrent_loads: 4,
            default_timeout_ms: 30_000,
            sync_config: SyncConfig::default(),
        };
        let backend = Bm25Backend::new(loader, backend_config);

        // Create state
        let state = Arc::new(TestAppState {
            backend,
            max_limit: 1000,
            max_timeout_ms: 300_000,
        });

        // Build router
        Router::new()
            .route("/v1/search", post(handle_search))
            .route("/v1/capabilities", get(handle_capabilities))
            .route("/v1/health", get(handle_health))
            .with_state(state)
    }

    struct TestAppState {
        backend: Bm25Backend<TestIndexLoader>,
        max_limit: usize,
        max_timeout_ms: u64,
    }

    #[derive(Debug, Clone)]
    struct TestIndexLoader {
        storage: FileStorage,
        nameservice: FileNameService,
    }

    impl TestIndexLoader {
        fn new(storage_path: PathBuf, ns_path: PathBuf) -> Self {
            Self {
                storage: FileStorage::new(storage_path),
                nameservice: FileNameService::new(ns_path),
            }
        }

        /// Load the BM25 manifest from CAS via the nameservice head pointer.
        async fn load_manifest(&self, graph_source_id: &str) -> ServiceResult<Bm25Manifest> {
            let record = self
                .nameservice
                .lookup_graph_source(graph_source_id)
                .await
                .map_err(|e| ServiceError::Internal {
                    message: format!("Nameservice error: {e}"),
                })?;

            let record = match record {
                Some(r) => r,
                None => return Ok(Bm25Manifest::new(graph_source_id)),
            };

            let index_cid = match &record.index_id {
                Some(cid) => cid,
                None => return Ok(Bm25Manifest::new(graph_source_id)),
            };

            let cs = fluree_db_core::content_store_for(self.storage.clone(), graph_source_id);
            let bytes = cs
                .get(index_cid)
                .await
                .map_err(|e| ServiceError::Internal {
                    message: format!("Storage error loading manifest: {e}"),
                })?;

            let manifest: Bm25Manifest =
                serde_json::from_slice(&bytes).map_err(|e| ServiceError::Internal {
                    message: format!("Manifest deserialize error: {e}"),
                })?;

            Ok(manifest)
        }
    }

    #[async_trait]
    impl IndexLoader for TestIndexLoader {
        async fn load_index(
            &self,
            graph_source_id: &str,
            index_t: i64,
        ) -> ServiceResult<Bm25Index> {
            let manifest = self.load_manifest(graph_source_id).await?;

            let entry = manifest
                .snapshots
                .iter()
                .find(|e| e.index_t == index_t)
                .ok_or_else(|| ServiceError::Internal {
                    message: format!("No snapshot found for {graph_source_id} at t={index_t}"),
                })?;

            let cs = fluree_db_core::content_store_for(self.storage.clone(), graph_source_id);
            let bytes = cs
                .get(&entry.snapshot_id)
                .await
                .map_err(|e| ServiceError::Internal {
                    message: format!("Storage error: {e}"),
                })?;

            let index = deserialize(&bytes).map_err(|e| ServiceError::Internal {
                message: format!("Deserialize error: {e}"),
            })?;

            Ok(index)
        }

        async fn get_latest_index_t(&self, graph_source_id: &str) -> ServiceResult<Option<i64>> {
            let manifest = self.load_manifest(graph_source_id).await?;
            Ok(manifest.head().map(|e| e.index_t))
        }

        async fn find_snapshot_for_t(
            &self,
            graph_source_id: &str,
            target_t: i64,
        ) -> ServiceResult<Option<i64>> {
            let manifest = self.load_manifest(graph_source_id).await?;
            Ok(manifest.select_snapshot(target_t).map(|e| e.index_t))
        }

        async fn get_index_head(&self, graph_source_id: &str) -> ServiceResult<Option<i64>> {
            self.get_latest_index_t(graph_source_id).await
        }
    }

    async fn handle_search(
        State(state): State<Arc<TestAppState>>,
        Json(request): Json<SearchRequest>,
    ) -> impl IntoResponse {
        use fluree_search_protocol::SearchError;
        use std::time::Instant;

        let start = Instant::now();
        let request_id = request.request_id.clone();

        let limit = request.limit.min(state.max_limit);
        let timeout_ms = request
            .timeout_ms
            .map(|t| t.min(state.max_timeout_ms))
            .or(Some(30_000));

        let result = state
            .backend
            .search(
                &request.graph_source_id,
                &request.query,
                limit,
                request.as_of_t,
                request.sync,
                timeout_ms,
            )
            .await;

        let took_ms = start.elapsed().as_millis() as u64;

        match result {
            Ok((index_t, hits)) => {
                let response = SearchResponse::new(
                    PROTOCOL_VERSION.to_string(),
                    request_id,
                    index_t,
                    hits,
                    took_ms,
                );
                (StatusCode::OK, Json(response)).into_response()
            }
            Err(e) => {
                let status = match &e {
                    ServiceError::GraphSourceNotFound { .. }
                    | ServiceError::NoSnapshotForAsOfT { .. }
                    | ServiceError::IndexNotBuilt { .. } => StatusCode::NOT_FOUND,
                    ServiceError::SyncTimeout { .. } | ServiceError::Timeout { .. } => {
                        StatusCode::GATEWAY_TIMEOUT
                    }
                    ServiceError::InvalidRequest { .. }
                    | ServiceError::UnsupportedProtocolVersion { .. } => StatusCode::BAD_REQUEST,
                    ServiceError::StorageError { .. }
                    | ServiceError::NameserviceError { .. }
                    | ServiceError::Internal { .. } => StatusCode::INTERNAL_SERVER_ERROR,
                };

                let error = SearchError::new(
                    PROTOCOL_VERSION.to_string(),
                    request_id,
                    e.error_code(),
                    e.to_string(),
                );

                (status, Json(error)).into_response()
            }
        }
    }

    async fn handle_capabilities(State(state): State<Arc<TestAppState>>) -> impl IntoResponse {
        use fluree_search_protocol::Capabilities;

        let capabilities = Capabilities {
            protocol_version: PROTOCOL_VERSION.to_string(),
            bm25_analyzer_version: fluree_search_protocol::BM25_ANALYZER_VERSION.to_string(),
            supported_query_kinds: vec!["bm25".to_string()],
            max_limit: state.max_limit,
            max_timeout_ms: state.max_timeout_ms,
        };

        Json(capabilities)
    }

    async fn handle_health() -> impl IntoResponse {
        Json(json!({ "status": "ok", "version": env!("CARGO_PKG_VERSION") }))
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let storage_path = tmp.path().to_path_buf();
        let ns_path = tmp.path().to_path_buf();

        let app = build_test_router(storage_path, ns_path);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let (status, json) = json_body(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json.get("status").and_then(|v| v.as_str()), Some("ok"));
    }

    #[tokio::test]
    async fn test_capabilities_endpoint() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let storage_path = tmp.path().to_path_buf();
        let ns_path = tmp.path().to_path_buf();

        let app = build_test_router(storage_path, ns_path);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/capabilities")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let (status, json) = json_body(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            json.get("protocol_version").and_then(|v| v.as_str()),
            Some(PROTOCOL_VERSION)
        );
        assert!(json.get("bm25_analyzer_version").is_some());
        assert!(json.get("supported_query_kinds").is_some());
    }

    #[tokio::test]
    async fn test_search_graph_source_not_found() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let storage_path = tmp.path().to_path_buf();
        let ns_path = tmp.path().to_path_buf();

        let app = build_test_router(storage_path, ns_path);

        let request = SearchRequest::bm25("nonexistent:main", "test query", 10);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/search")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let (status, _json) = json_body(resp).await;
        // Should return NOT_FOUND or similar error status
        assert!(
            status == StatusCode::NOT_FOUND || status == StatusCode::INTERNAL_SERVER_ERROR,
            "Expected 404 or 500 for nonexistent graph source, got {status}"
        );
    }

    #[tokio::test]
    async fn test_search_with_real_data() {
        // Set up fluree with file storage
        let tmp = tempfile::tempdir().expect("tempdir");
        let data_path = tmp.path().to_string_lossy().to_string();

        let fluree = FlureeBuilder::file(&data_path)
            .build()
            .expect("build file fluree");

        // Create ledger and insert test data
        let ledger_alias = "search/test:main";
        let ledger = fluree
            .create_ledger(ledger_alias)
            .await
            .expect("create ledger");

        let tx = json!({
            "@context": { "ex": "http://example.org/" },
            "@graph": [
                { "@id": "ex:doc1", "@type": "ex:Article", "ex:title": "Rust programming language guide" },
                { "@id": "ex:doc2", "@type": "ex:Article", "ex:title": "Rust systems programming" },
                { "@id": "ex:doc3", "@type": "ex:Article", "ex:title": "Python programming tutorial" },
                { "@id": "ex:doc4", "@type": "ex:Article", "ex:title": "JavaScript web development" }
            ]
        });
        let _ledger = fluree.insert(ledger, &tx).await.expect("insert docs");

        // Create BM25 index
        let query = json!({
            "@context": { "ex": "http://example.org/" },
            "where": [{ "@id": "?x", "@type": "ex:Article", "ex:title": "?title" }],
            "select": { "?x": ["@id", "ex:title"] }
        });

        let cfg = Bm25CreateConfig::new("search-test", ledger_alias, query);
        let created = fluree
            .create_full_text_index(cfg)
            .await
            .expect("create bm25 index");
        let graph_source_id = created.graph_source_id;

        // Build router pointing at same storage (file storage stores both data and ns in same dir)
        let storage_path = tmp.path().to_path_buf();
        let ns_path = tmp.path().to_path_buf();
        let app = build_test_router(storage_path, ns_path);

        // Search for "rust programming"
        let request = SearchRequest::bm25(&graph_source_id, "rust programming", 10);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/search")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let (status, json) = json_body(resp).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "Search should succeed, got: {json:?}"
        );

        // Verify response structure
        assert!(json.get("protocol_version").is_some());
        assert!(json.get("index_t").is_some());
        assert!(json.get("hits").is_some());
        assert!(json.get("took_ms").is_some());

        // Verify we got results
        let hits = json.get("hits").and_then(|h| h.as_array()).unwrap();
        assert!(!hits.is_empty(), "Expected search hits");

        // Verify result structure
        let first_hit = &hits[0];
        assert!(first_hit.get("iri").is_some());
        assert!(first_hit.get("ledger_alias").is_some());
        assert!(first_hit.get("score").is_some());

        // Rust documents should rank higher than Python/JavaScript
        let iris: Vec<&str> = hits
            .iter()
            .map(|h| h.get("iri").and_then(|v| v.as_str()).unwrap())
            .collect();

        // doc1 and doc2 are about Rust, should be in top results
        let rust_in_top_2 = iris
            .iter()
            .take(2)
            .any(|iri| iri.contains("doc1") || iri.contains("doc2"));
        assert!(
            rust_in_top_2,
            "Rust documents should rank in top 2 for 'rust programming', got: {iris:?}"
        );
    }
}

// =============================================================================
// Parity tests (embedded vs service scoring)
// =============================================================================

mod parity_tests {
    use super::*;
    use fluree_db_api::FlureeIndexProvider;
    use fluree_db_query::bm25::Bm25SearchProvider;

    /// Score comparison epsilon for floating point parity.
    const SCORE_EPSILON: f64 = 1e-9;

    /// Compare two search results for parity.
    fn assert_results_parity(embedded: &[SearchHit], direct: &[SearchHit], context: &str) {
        assert_eq!(
            embedded.len(),
            direct.len(),
            "{}: result count mismatch (embedded={}, direct={})",
            context,
            embedded.len(),
            direct.len()
        );

        for (i, (e, d)) in embedded.iter().zip(direct.iter()).enumerate() {
            assert_eq!(
                e.iri, d.iri,
                "{}: IRI mismatch at position {} (embedded={}, direct={})",
                context, i, e.iri, d.iri
            );

            assert_eq!(
                e.ledger_alias, d.ledger_alias,
                "{}: ledger_alias mismatch at position {} (embedded={}, direct={})",
                context, i, e.ledger_alias, d.ledger_alias
            );

            let score_diff = (e.score - d.score).abs();
            assert!(
                score_diff < SCORE_EPSILON,
                "{}: score mismatch at position {} (embedded={}, direct={}, diff={})",
                context,
                i,
                e.score,
                d.score,
                score_diff
            );
        }
    }

    #[tokio::test]
    async fn test_embedded_vs_direct_scorer_parity() {
        // This test verifies that EmbeddedBm25SearchProvider produces the same
        // results as using the scorer directly (which is what the service does).
        let fluree = FlureeBuilder::memory().build_memory();

        // Create ledger and BM25 index using in-memory Fluree
        let alias = "parity/test:main";
        let ledger = fluree.create_ledger(alias).await.unwrap();

        let tx = json!({
            "@context": { "ex": "http://example.org/" },
            "@graph": [
                { "@id": "ex:doc1", "@type": "ex:Article", "ex:title": "Rust programming language" },
                { "@id": "ex:doc2", "@type": "ex:Article", "ex:title": "Rust systems programming" },
                { "@id": "ex:doc3", "@type": "ex:Article", "ex:title": "Python programming tutorial" }
            ]
        });
        let _ledger = fluree.insert(ledger, &tx).await.unwrap();

        let query = json!({
            "@context": { "ex": "http://example.org/" },
            "where": [{ "@id": "?x", "@type": "ex:Article", "ex:title": "?title" }],
            "select": { "?x": ["@id", "ex:title"] }
        });

        let cfg = Bm25CreateConfig::new("parity-test", alias, query);
        let created = fluree.create_full_text_index(cfg).await.unwrap();

        // Get results via direct scorer (what service backend does)
        let idx = fluree
            .load_bm25_index(&created.graph_source_id)
            .await
            .unwrap();
        let analyzer = Analyzer::english_default();
        let query_terms = analyzer.analyze_to_strings("rust programming");
        let term_refs: Vec<&str> = query_terms
            .iter()
            .map(std::string::String::as_str)
            .collect();
        let scorer = Bm25Scorer::new(&idx, &term_refs);
        let direct_results = scorer.top_k(10);

        // Convert to SearchHit for comparison
        let direct_hits: Vec<SearchHit> = direct_results
            .into_iter()
            .map(|(doc_key, score)| SearchHit {
                iri: doc_key.subject_iri.to_string(),
                ledger_alias: doc_key.ledger_alias.to_string(),
                score,
            })
            .collect();

        // Get results via Bm25SearchProvider (what the operator uses)
        let provider = FlureeIndexProvider::new(&fluree);
        let embedded_result = provider
            .search_bm25(
                &created.graph_source_id,
                "rust programming",
                10,
                None,
                false,
                None,
            )
            .await
            .unwrap();

        // Compare - should be identical
        assert_results_parity(
            &embedded_result.hits,
            &direct_hits,
            "embedded vs direct scorer",
        );
    }

    #[tokio::test]
    async fn test_empty_query_behavior() {
        // Empty/stopword-only queries should return empty results consistently
        let fluree = FlureeBuilder::memory().build_memory();

        let alias = "parity/empty:main";
        let ledger = fluree.create_ledger(alias).await.unwrap();

        let tx = json!({
            "@context": { "ex": "http://example.org/" },
            "@graph": [
                { "@id": "ex:doc1", "@type": "ex:Article", "ex:title": "Hello world" }
            ]
        });
        let _ledger = fluree.insert(ledger, &tx).await.unwrap();

        let query = json!({
            "@context": { "ex": "http://example.org/" },
            "where": [{ "@id": "?x", "@type": "ex:Article", "ex:title": "?title" }],
            "select": { "?x": ["@id", "ex:title"] }
        });

        let cfg = Bm25CreateConfig::new("empty-test", alias, query);
        let created = fluree.create_full_text_index(cfg).await.unwrap();

        // Empty query via provider
        let provider = FlureeIndexProvider::new(&fluree);
        let result = provider
            .search_bm25(&created.graph_source_id, "", 10, None, false, None)
            .await
            .unwrap();

        assert!(
            result.hits.is_empty(),
            "Empty query should return no results"
        );

        // Stopword-only query
        let result2 = provider
            .search_bm25(&created.graph_source_id, "the a an", 10, None, false, None)
            .await
            .unwrap();

        assert!(
            result2.hits.is_empty(),
            "Stopword-only query should return no results"
        );
    }

    #[tokio::test]
    async fn test_limit_respected() {
        // Verify limit is respected consistently
        let fluree = FlureeBuilder::memory().build_memory();

        let alias = "parity/limit:main";
        let ledger = fluree.create_ledger(alias).await.unwrap();

        // Create 10 documents
        let docs: Vec<_> = (1..=10)
            .map(|i| {
                json!({
                    "@id": format!("ex:doc{}", i),
                    "@type": "ex:Article",
                    "ex:title": format!("programming article number {}", i)
                })
            })
            .collect();

        let tx = json!({
            "@context": { "ex": "http://example.org/" },
            "@graph": docs
        });
        let _ledger = fluree.insert(ledger, &tx).await.unwrap();

        let query = json!({
            "@context": { "ex": "http://example.org/" },
            "where": [{ "@id": "?x", "@type": "ex:Article", "ex:title": "?title" }],
            "select": { "?x": ["@id", "ex:title"] }
        });

        let cfg = Bm25CreateConfig::new("limit-test", alias, query);
        let created = fluree.create_full_text_index(cfg).await.unwrap();

        let provider = FlureeIndexProvider::new(&fluree);

        // Test various limits
        for limit in [1, 3, 5, 10, 100] {
            let result = provider
                .search_bm25(
                    &created.graph_source_id,
                    "programming",
                    limit,
                    None,
                    false,
                    None,
                )
                .await
                .unwrap();

            let expected_count = limit.min(10); // We only have 10 docs
            assert_eq!(
                result.hits.len(),
                expected_count,
                "Limit {limit} should return {expected_count} results"
            );
        }
    }

    #[tokio::test]
    async fn test_ordering_consistency() {
        // Verify that results are consistently ordered by score descending
        let fluree = FlureeBuilder::memory().build_memory();

        let alias = "parity/order:main";
        let ledger = fluree.create_ledger(alias).await.unwrap();

        let tx = json!({
            "@context": { "ex": "http://example.org/" },
            "@graph": [
                { "@id": "ex:doc1", "@type": "ex:Article", "ex:title": "rust rust rust" },
                { "@id": "ex:doc2", "@type": "ex:Article", "ex:title": "rust rust" },
                { "@id": "ex:doc3", "@type": "ex:Article", "ex:title": "rust" }
            ]
        });
        let _ledger = fluree.insert(ledger, &tx).await.unwrap();

        let query = json!({
            "@context": { "ex": "http://example.org/" },
            "where": [{ "@id": "?x", "@type": "ex:Article", "ex:title": "?title" }],
            "select": { "?x": ["@id", "ex:title"] }
        });

        let cfg = Bm25CreateConfig::new("order-test", alias, query);
        let created = fluree.create_full_text_index(cfg).await.unwrap();

        let provider = FlureeIndexProvider::new(&fluree);
        let result = provider
            .search_bm25(&created.graph_source_id, "rust", 10, None, false, None)
            .await
            .unwrap();

        // Verify scores are in descending order
        let scores: Vec<f64> = result.hits.iter().map(|h| h.score).collect();
        for i in 1..scores.len() {
            assert!(
                scores[i - 1] >= scores[i],
                "Scores should be in descending order: {scores:?}"
            );
        }

        // Doc with most "rust" occurrences should be first
        assert!(
            result.hits[0].iri.contains("doc1"),
            "Doc1 (most 'rust' occurrences) should rank first"
        );
    }
}
