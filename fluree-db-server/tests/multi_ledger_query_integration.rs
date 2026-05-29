//! HTTP-layer integration tests for multi-ledger JSON-LD queries through
//! `/v1/fluree/query`.
//!
//! The engine-level repros in `fluree-db-api/tests/it_query_dataset.rs`
//! prove the query engine handles array-form `from`, object-form `from`,
//! and `fromNamed`-only correctly. These tests close the loop through
//! the HTTP dispatcher (`get_ledger_id`) which currently bails with
//! `MissingLedger` on any body whose `from` isn't a bare string. The
//! engine tests can't catch a regression at that layer.
//!
//! See fluree/db#1259.

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

/// Create a ledger via `/v1/fluree/create`, then seed it via
/// `/v1/fluree/insert/<ledger>`. Panics on any non-success status so the
/// test body stays focused on the query path under test.
async fn create_and_seed(state: &Arc<AppState>, ledger_id: &str, insert: JsonValue) {
    let create_resp = build_router(Arc::clone(state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(json!({"ledger": ledger_id}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        create_resp.status(),
        StatusCode::CREATED,
        "create {ledger_id}"
    );

    let insert_resp = build_router(Arc::clone(state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/insert/{ledger_id}"))
                .header("content-type", "application/json")
                .body(Body::from(insert.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let insert_status = insert_resp.status();
    let (_, insert_body) = json_body(insert_resp).await;
    assert!(
        insert_status.is_success(),
        "insert into {ledger_id} should succeed; got {insert_status}, body: {insert_body}"
    );
}

/// Seed two ledgers (`people:test` and `people2:test`) with two Person
/// entries each. Returns `(_tmp, state)` so the temp dir outlives the test.
async fn seed_two_people_ledgers() -> (TempDir, Arc<AppState>) {
    let (tmp, state) = server_state().await;

    create_and_seed(
        &state,
        "people:test",
        json!({
            "@context": {
                "ex": "http://example.org/ns/",
                "schema": "http://schema.org/"
            },
            "@graph": [
                {"@id": "ex:alice", "@type": "ex:Person", "schema:name": "Alice"},
                {"@id": "ex:bob",   "@type": "ex:Person", "schema:name": "Bob"}
            ]
        }),
    )
    .await;

    create_and_seed(
        &state,
        "people2:test",
        json!({
            "@context": {
                "ex": "http://example.org/ns/",
                "schema": "http://schema.org/"
            },
            "@graph": [
                {"@id": "ex:charlie", "@type": "ex:Person", "schema:name": "Charlie"},
                {"@id": "ex:diana",   "@type": "ex:Person", "schema:name": "Diana"}
            ]
        }),
    )
    .await;

    (tmp, state)
}

/// Issue 1: `from: ["a:test", "b:test"]` (array union) through the
/// connection-scoped `/v1/fluree/query` route must reach the engine.
///
/// The engine handles array `from` correctly (proven by
/// `dataset_from_array_union_via_connection`). The HTTP dispatcher's
/// `get_ledger_id` currently rejects with 400 `MissingLedger` because it
/// only accepts a string `from`.
#[tokio::test]
async fn query_with_from_array_union_through_dispatcher_returns_200() {
    let (_tmp, state) = seed_two_people_ledgers().await;

    let query = json!({
        "from": ["people:test", "people2:test"],
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": {"@id": "?p", "@type": "ex:Person", "schema:name": "?name"}
    });

    let resp = build_router(Arc::clone(&state))
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

    let (status, body) = json_body(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "array-form `from` must reach the engine, not get rejected at dispatcher. Body: {body}"
    );

    // Loosely assert union semantics: the response must contain all four names.
    let s = body.to_string();
    for name in &["Alice", "Bob", "Charlie", "Diana"] {
        assert!(s.contains(name), "expected {name} in response; got: {s}");
    }
}

/// Issue 1 variant: `from: {"@id": "ledger:test"}` (object form) through
/// the connection-scoped route.
#[tokio::test]
async fn query_with_from_object_through_dispatcher_returns_200() {
    let (_tmp, state) = seed_two_people_ledgers().await;

    let query = json!({
        "from": {"@id": "people:test"},
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": {"@id": "?p", "@type": "ex:Person", "schema:name": "?name"}
    });

    let resp = build_router(Arc::clone(&state))
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

    let (status, body) = json_body(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "object-form `from` must reach the engine, not get rejected at dispatcher. Body: {body}"
    );

    let s = body.to_string();
    assert!(s.contains("Alice"), "expected Alice in response; got: {s}");
    assert!(s.contains("Bob"), "expected Bob in response; got: {s}");
}

/// Issue 3: `fromNamed`-only (no `from`) through the connection-scoped route.
///
/// Two stacked bugs: the HTTP dispatcher rejects (Commit 3 unblocks), AND
/// `FromQueryBuilder::execute_formatted` errors `"No default graph for
/// formatting"` (Commit 4 unblocks). This test only goes green when both
/// land — un-ignored in Commit 4.
#[tokio::test]
async fn query_with_fromnamed_only_no_from_through_dispatcher_returns_200() {
    let (_tmp, state) = seed_two_people_ledgers().await;

    let query = json!({
        "fromNamed": {
            "g1": {"@id": "people:test"},
            "g2": {"@id": "people2:test"}
        },
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?g", "?name"],
        "where": [["graph", "?g", {"@id": "?p", "@type": "ex:Person", "schema:name": "?name"}]]
    });

    let resp = build_router(Arc::clone(&state))
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

    let (status, body) = json_body(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "fromNamed-only must reach the engine AND format successfully. Body: {body}"
    );

    let s = body.to_string();
    for name in &["Alice", "Bob", "Charlie", "Diana"] {
        assert!(s.contains(name), "expected {name} in response; got: {s}");
    }
}
