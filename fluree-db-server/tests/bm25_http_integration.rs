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

    insert_doc(&state, "ex:doc1", "Rust programming guide").await;

    (tmp, state)
}

/// Insert one `ex:Doc` into `docs:main`, returning the resulting commit `t`.
async fn insert_doc(state: &Arc<AppState>, id: &str, title: &str) -> i64 {
    let doc = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": id,
        "@type": "ex:Doc",
        "ex:title": title
    });
    let resp = build_router(state.clone())
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
    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK, "insert {id}: {json}");
    json.get("t")
        .and_then(JsonValue::as_i64)
        .unwrap_or_else(|| panic!("insert should report t: {json}"))
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

// ============================================================================
// sync
// ============================================================================

async fn sync_index(state: &Arc<AppState>, uri: &str, index: &str) -> (StatusCode, JsonValue) {
    let resp = build_router(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(json!({"index": index}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    json_body(resp).await
}

/// `docs:main` with one doc, plus a `docsearch:main` index built over it.
async fn state_with_index() -> (TempDir, Arc<AppState>) {
    let (tmp, state) = state_with_docs_ledger().await;
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
    (tmp, state)
}

#[tokio::test]
async fn sync_without_t_catches_up_to_head() {
    let (_tmp, state) = state_with_index().await;
    insert_doc(&state, "ex:doc2", "Rust and WebAssembly").await;

    let (status, json) = sync_index(&state, "/v1/fluree/bm25/sync", "docsearch:main").await;

    assert_eq!(status, StatusCode::OK, "body: {json}");
    assert_eq!(
        json.get("graph_source_id").and_then(JsonValue::as_str),
        Some("docsearch:main")
    );
    let old = json.get("old_watermark").and_then(JsonValue::as_i64);
    let new = json.get("new_watermark").and_then(JsonValue::as_i64);
    assert!(new > old, "the new commit should advance it: {json}");
    assert!(
        json.get("upserted")
            .and_then(JsonValue::as_u64)
            .is_some_and(|n| n > 0),
        "the second doc should be indexed: {json}"
    );
}

#[tokio::test]
async fn sync_of_a_current_index_is_a_no_op() {
    let (_tmp, state) = state_with_index().await;

    let (status, json) = sync_index(&state, "/v1/fluree/bm25/sync", "docsearch:main").await;

    assert_eq!(status, StatusCode::OK, "body: {json}");
    assert_eq!(json.get("upserted").and_then(JsonValue::as_u64), Some(0));
    assert_eq!(json.get("removed").and_then(JsonValue::as_u64), Some(0));
    assert_eq!(
        json.get("old_watermark").and_then(JsonValue::as_i64),
        json.get("new_watermark").and_then(JsonValue::as_i64),
        "an already-current index should not move its watermark: {json}"
    );
}

/// The pinned form must stop at the requested `t`, not run on to the head.
#[tokio::test]
async fn sync_with_t_syncs_through_that_t_only() {
    let (_tmp, state) = state_with_index().await;
    let second = insert_doc(&state, "ex:doc2", "Rust and WebAssembly").await;
    let head = insert_doc(&state, "ex:doc3", "Rust concurrency patterns").await;
    assert!(head > second, "second insert should advance t");

    let (status, json) = sync_index(
        &state,
        &format!("/v1/fluree/bm25/sync?t={second}"),
        "docsearch:main",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body: {json}");
    assert_eq!(
        json.get("new_watermark").and_then(JsonValue::as_i64),
        Some(second),
        "watermark should land on the requested t, not head ({head}): {json}"
    );
}

/// `POST /drop` reaches the generic graph-source drop, which must sweep the
/// index's snapshot blobs rather than only retracting the record.
#[tokio::test]
async fn drop_reports_deleted_snapshots() {
    let (_tmp, state) = state_with_index().await;

    let resp = build_router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/drop")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"ledger": "docsearch", "hard": true}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK, "body: {json}");
    assert_eq!(
        json.get("status").and_then(JsonValue::as_str),
        Some("dropped"),
        "body: {json}"
    );
    assert!(
        json.get("files_deleted")
            .and_then(JsonValue::as_u64)
            .is_some_and(|n| n >= 1),
        "hard drop should report deleted snapshot blobs: {json}"
    );
}

/// Both sync forms must treat a dropped index the same way. The pinned form
/// used to skip the retraction check and write a fresh snapshot for an index
/// whose snapshots had just been deleted.
#[tokio::test]
async fn sync_of_a_dropped_index_is_refused_by_both_forms() {
    let (_tmp, state) = state_with_index().await;

    let resp = build_router(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/drop")
                .header("content-type", "application/json")
                .body(Body::from(json!({"ledger": "docsearch"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK, "drop: {json}");

    let (head_status, head_json) =
        sync_index(&state, "/v1/fluree/bm25/sync", "docsearch:main").await;
    let (pinned_status, pinned_json) =
        sync_index(&state, "/v1/fluree/bm25/sync?t=1", "docsearch:main").await;

    assert!(!head_status.is_success(), "head sync: {head_json}");
    assert!(
        !pinned_status.is_success(),
        "pinned sync must not resurrect a dropped index: {pinned_json}"
    );
    // Equality rather than a literal status: the refusal currently surfaces as
    // a 500 via the blanket `ApiError::Drop` arm, which is tracked separately.
    // What this pins is that the two forms agree.
    assert_eq!(
        head_status, pinned_status,
        "both forms should agree; head={head_json} pinned={pinned_json}"
    );
}

#[tokio::test]
async fn sync_of_an_unknown_index_is_404() {
    let (_tmp, state) = state_with_index().await;

    let (status, json) = sync_index(&state, "/v1/fluree/bm25/sync", "nosuch:main").await;

    assert_eq!(status, StatusCode::NOT_FOUND, "body: {json}");
}

/// An unparseable `t` must be rejected rather than silently falling back to a
/// head sync, which is the opposite of what the caller asked for.
#[tokio::test]
async fn sync_with_a_malformed_t_is_400() {
    let (_tmp, state) = state_with_index().await;

    let (status, json) = sync_index(&state, "/v1/fluree/bm25/sync?t=abc", "docsearch:main").await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {json}");
}

/// `t < 1` names no commit. Left to the API it succeeds with a rewound
/// watermark (`t=0` returns 200 with `new_watermark: 0`), silently discarding
/// the index's contents, so the route refuses it.
#[tokio::test]
async fn sync_with_a_non_positive_t_is_400() {
    let (_tmp, state) = state_with_index().await;

    for t in ["0", "-1"] {
        let (status, json) = sync_index(
            &state,
            &format!("/v1/fluree/bm25/sync?t={t}"),
            "docsearch:main",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "t={t} body: {json}");
    }

    // The index is untouched: a following head sync is still a no-op, and the
    // watermark is a real commit rather than the rewound 0 the API would leave.
    let (status, json) = sync_index(&state, "/v1/fluree/bm25/sync", "docsearch:main").await;
    assert_eq!(status, StatusCode::OK, "body: {json}");
    let old = json.get("old_watermark").and_then(JsonValue::as_i64);
    let new = json.get("new_watermark").and_then(JsonValue::as_i64);
    assert_eq!(old, new, "should still be current: {json}");
    assert!(
        new.is_some_and(|t| t >= 1),
        "watermark must not have been rewound: {json}"
    );
}
