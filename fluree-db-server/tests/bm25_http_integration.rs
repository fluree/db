//! HTTP coverage for BM25 full-text index management.
//!
//! `POST /v1/fluree/bm25/create` is the only BM25-specific route: once an index
//! exists it is an ordinary graph source, so these tests also pin that the
//! generic `/ledgers` and `/info` fallbacks surface it without any BM25-specific
//! handling.

use axum::body::Body;
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

/// Indexes every `ex:Doc`'s `ex:title`. MUST select `@id`.
fn index_query() -> JsonValue {
    json!({
        "@context": {"ex": "http://example.org/"},
        "where": [{"@id": "?x", "@type": "ex:Doc", "ex:title": "?t"}],
        "select": {"?x": ["@id", "ex:title"]}
    })
}

async fn test_state() -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    (tmp, state)
}

async fn json_body(resp: http::Response<Body>) -> (StatusCode, JsonValue) {
    let status = resp.status();
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    let json = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| JsonValue::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, json)
}

/// A `docs:main` ledger holding one `ex:Doc`, created over HTTP.
async fn state_with_docs_ledger() -> (TempDir, Arc<AppState>) {
    let (tmp, state) = test_state().await;
    let app = build_router(state.clone());

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(json!({"ledger": "docs:main"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "ledger create");

    let doc = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:doc1",
        "@type": "ex:Doc",
        "ex:title": "Rust programming guide"
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/insert")
                .header("content-type", "application/json")
                .header("fluree-ledger", "docs:main")
                .body(Body::from(doc.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "insert doc");

    (tmp, state)
}

async fn create_index(state: &Arc<AppState>, body: JsonValue) -> (StatusCode, JsonValue) {
    let resp = build_router(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/bm25/create")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    json_body(resp).await
}

#[tokio::test]
async fn create_returns_201_with_index_stats() {
    let (_tmp, state) = state_with_docs_ledger().await;

    let (status, json) = create_index(
        &state,
        json!({
            "name": "docsearch",
            "ledger": "docs:main",
            "query": index_query()
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED, "body: {json}");
    assert_eq!(
        json.get("graph_source_id").and_then(JsonValue::as_str),
        Some("docsearch:main"),
        "branch defaults to main"
    );
    assert_eq!(
        json.get("doc_count").and_then(JsonValue::as_u64),
        Some(1),
        "the one ex:Doc should be indexed: {json}"
    );
    assert!(
        json.get("term_count")
            .and_then(JsonValue::as_u64)
            .is_some_and(|n| n > 0),
        "the title's terms should be indexed: {json}"
    );
    assert!(
        json.get("index_t")
            .and_then(JsonValue::as_i64)
            .is_some_and(|t| t >= 1),
        "index_t should be the source ledger's commit t: {json}"
    );
}

/// Once created, the index is an ordinary graph source: the generic
/// `/ledgers` and `/info` routes must surface it with no BM25-specific code.
#[tokio::test]
async fn created_index_is_visible_to_the_generic_graph_source_routes() {
    let (_tmp, state) = state_with_docs_ledger().await;
    let (status, _) = create_index(
        &state,
        json!({
            "name": "docsearch",
            "ledger": "docs:main",
            "query": index_query()
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let resp = build_router(state.clone())
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/ledgers")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        json.to_string().contains("BM25"),
        "the index should list with its BM25 type label: {json}"
    );

    let resp = build_router(state)
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/info/docsearch:main")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK, "body: {json}");
    assert_eq!(
        json.get("type").and_then(JsonValue::as_str),
        Some("BM25"),
        "body: {json}"
    );
    assert_eq!(
        json.get("dependencies").and_then(JsonValue::as_array),
        Some(&vec![JsonValue::String("docs:main".to_string())]),
        "info should name the source ledger: {json}"
    );
}

#[tokio::test]
async fn optional_k1_and_b_are_accepted() {
    let (_tmp, state) = state_with_docs_ledger().await;

    let (status, json) = create_index(
        &state,
        json!({
            "name": "docsearch",
            "ledger": "docs:main",
            "branch": "tuned",
            "query": index_query(),
            "k1": 1.5,
            "b": 0.4
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED, "body: {json}");
    assert_eq!(
        json.get("graph_source_id").and_then(JsonValue::as_str),
        Some("docsearch:tuned")
    );
}

/// Config validation runs before the build so a bad request is a 400, not an
/// indexing failure surfaced as a 500.
#[tokio::test]
async fn invalid_config_is_400() {
    let (_tmp, state) = state_with_docs_ledger().await;

    let (status, json) = create_index(
        &state,
        json!({
            "name": "has:colon",
            "ledger": "docs:main",
            "query": index_query()
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {json}");

    let (status, json) = create_index(
        &state,
        json!({
            "name": "docsearch",
            "ledger": "docs:main",
            "query": index_query(),
            "b": 5.0
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {json}");
}

#[tokio::test]
async fn creating_over_an_existing_index_is_rejected() {
    let (_tmp, state) = state_with_docs_ledger().await;
    let body = json!({
        "name": "docsearch",
        "ledger": "docs:main",
        "query": index_query()
    });

    let (status, _) = create_index(&state, body.clone()).await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, json) = create_index(&state, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {json}");
    assert!(json.to_string().contains("already exists"), "body: {json}");
}

#[tokio::test]
async fn missing_source_ledger_is_404() {
    let (_tmp, state) = test_state().await;

    let (status, json) = create_index(
        &state,
        json!({
            "name": "docsearch",
            "ledger": "nosuch:main",
            "query": index_query()
        }),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND, "body: {json}");
}
