//! HTTP-layer integration tests for connection-scoped multi-ledger JSON-LD
//! queries (`POST /v1/fluree/query`, no path ledger).
//!
//! Regression coverage for issue #1259, Issue 1 + Issue 3 layer 1: the
//! dispatcher's `get_ledger_id` only accepted a bare string `from`, so a
//! `from: [array]` (multi-default-graph union) or a `fromNamed`-only query was
//! rejected with `400 MissingLedger` before `requires_dataset_features` could
//! route it to the dataset execution path — even though the engine accepts
//! both. These tests drive the real Axum router end to end.

use axum::body::Body;
use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

async fn json_body(resp: http::Response<Body>) -> (StatusCode, JsonValue) {
    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();
    let json: JsonValue = serde_json::from_slice(&bytes).unwrap_or(JsonValue::Null);
    (status, json)
}

async fn server_state() -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState"));
    (tmp, state)
}

async fn create_ledger(state: &Arc<AppState>, ledger: &str) {
    let resp = build_router(Arc::clone(state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "ledger": ledger }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "create {ledger}");
}

async fn insert(state: &Arc<AppState>, ledger: &str, body: JsonValue) {
    let resp = build_router(Arc::clone(state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/insert/{ledger}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert!(status.is_success(), "insert into {ledger}: {status} {json}");
}

/// Seed two independent ledgers, each with one typed subject.
async fn seed_two_ledgers(state: &Arc<AppState>) {
    create_ledger(state, "test/people-a:main").await;
    create_ledger(state, "test/people-b:main").await;

    insert(
        state,
        "test/people-a:main",
        json!({
            "@context": {"schema": "http://schema.org/", "id": "@id", "type": "@type"},
            "@graph": [{"@id": "http://ex.org/alice", "@type": "schema:Person", "schema:name": "Alice"}]
        }),
    )
    .await;
    insert(
        state,
        "test/people-b:main",
        json!({
            "@context": {"schema": "http://schema.org/", "id": "@id", "type": "@type"},
            "@graph": [{"@id": "http://ex.org/bob", "@type": "schema:Person", "schema:name": "Bob"}]
        }),
    )
    .await;
}

async fn post_connection_query(state: &Arc<AppState>, query: JsonValue) -> (StatusCode, JsonValue) {
    let resp = build_router(Arc::clone(state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query")
                .header("content-type", "application/json")
                .body(Body::from(query.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    json_body(resp).await
}

/// Issue 1 — `from: [array]` (multi-default-graph union) over the
/// connection-scoped route must return both ledgers' subjects, not 400.
#[tokio::test]
async fn connection_query_from_array_unions_default_graphs() {
    let (_tmp, state) = server_state().await;
    seed_two_ledgers(&state).await;

    let (status, body) = post_connection_query(
        &state,
        json!({
            "@context": {"schema": "http://schema.org/", "id": "@id", "type": "@type"},
            "from": ["test/people-a:main", "test/people-b:main"],
            "select": {"?p": ["schema:name"]},
            "where": {"@id": "?p", "type": "schema:Person"}
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "from-array query body: {body}");
    let names: Vec<&str> = body
        .as_array()
        .expect("array result")
        .iter()
        .filter_map(|row| row.get("schema:name").and_then(|v| v.as_str()))
        .collect();
    assert!(names.contains(&"Alice"), "expected Alice in {body}");
    assert!(names.contains(&"Bob"), "expected Bob in {body}");
}

/// Issue 3 layer 1 — a `fromNamed`-only query over the connection-scoped route
/// must execute (not 400) and return the named graph's data via a GRAPH
/// pattern.
#[tokio::test]
async fn connection_query_from_named_only_executes() {
    let (_tmp, state) = server_state().await;
    seed_two_ledgers(&state).await;

    let (status, body) = post_connection_query(
        &state,
        json!({
            "@context": {"schema": "http://schema.org/", "id": "@id", "type": "@type"},
            "fromNamed": {"a": {"@id": "test/people-a:main"}},
            "select": {"?p": ["schema:name"]},
            "where": [["graph", "a", {"@id": "?p", "type": "schema:Person"}]]
        }),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::OK,
        "fromNamed-only must not be rejected with MissingLedger; body: {body}"
    );
    let names: Vec<&str> = body
        .as_array()
        .expect("array result")
        .iter()
        .filter_map(|row| row.get("schema:name").and_then(|v| v.as_str()))
        .collect();
    assert!(
        names.contains(&"Alice"),
        "expected Alice from named graph: {body}"
    );
}

/// A request with no `from`, `fromNamed`, header, or path ledger is still a
/// `400` — the relaxation must not swallow genuinely under-specified queries.
#[tokio::test]
async fn connection_query_without_any_ledger_still_400() {
    let (_tmp, state) = server_state().await;
    seed_two_ledgers(&state).await;

    let (status, _body) = post_connection_query(
        &state,
        json!({
            "@context": {"schema": "http://schema.org/", "id": "@id", "type": "@type"},
            "select": {"?p": ["schema:name"]},
            "where": {"@id": "?p", "type": "schema:Person"}
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}
