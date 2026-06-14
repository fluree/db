//! HTTP-layer coverage for the openCypher routes: `Content-Type:
//! application/cypher` on `POST /v1/fluree/query/<ledger>` (read) and
//! `POST /v1/fluree/update/<ledger>` (write). v1 is local execution with
//! JSON-LD output; the connection-scoped routes reject Cypher (no ledger).

use axum::body::Body;
use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

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

async fn insert(state: &Arc<AppState>, ledger: &str, body: serde_json::Value) {
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
    assert!(resp.status().is_success(), "insert into {ledger}");
}

/// POST a Cypher body to a route and return `(status, body_text)`.
async fn post_cypher(state: &Arc<AppState>, uri: &str, cypher: &str) -> (StatusCode, String) {
    let resp = build_router(Arc::clone(state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/cypher")
                .body(Body::from(cypher.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

#[tokio::test]
async fn cypher_http_read_write_round_trip() {
    let (_tmp, state) = server_state().await;
    create_ledger(&state, "cypherhttp").await;
    insert(
        &state,
        "cypherhttp",
        json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice", "ex:age": 30
        }),
    )
    .await;

    // Read via Cypher.
    let (status, body) = post_cypher(
        &state,
        "/v1/fluree/query/cypherhttp",
        "MATCH (n:Person) RETURN n.name",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "cypher read status; body={body}");
    assert!(body.contains("Alice"), "cypher read body: {body}");

    // Write via Cypher (MATCH … SET).
    let (status, body) = post_cypher(
        &state,
        "/v1/fluree/update/cypherhttp",
        r#"MATCH (n:Person {name: "Alice"}) SET n.age = 42"#,
    )
    .await;
    assert!(
        status.is_success(),
        "cypher write status={status}; body={body}"
    );

    // The new value is visible.
    let (status, body) = post_cypher(
        &state,
        "/v1/fluree/query/cypherhttp",
        "MATCH (n:Person) WHERE n.age > 40 RETURN n.name",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Alice"), "post-SET read body: {body}");

    // Write via Cypher CREATE.
    let (status, _body) = post_cypher(
        &state,
        "/v1/fluree/update/cypherhttp",
        r#"CREATE (n:Person {name: "Carol"})"#,
    )
    .await;
    assert!(status.is_success(), "cypher create status={status}");

    let (status, body) = post_cypher(
        &state,
        "/v1/fluree/query/cypherhttp",
        "MATCH (n:Person) RETURN n.name",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Carol"), "after-create read body: {body}");
}

#[tokio::test]
async fn cypher_parse_error_is_client_error() {
    let (_tmp, state) = server_state().await;
    create_ledger(&state, "cypherbad").await;
    let (status, _body) =
        post_cypher(&state, "/v1/fluree/query/cypherbad", "NOT VALID CYPHER {{").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "parse error → 400");
}

#[tokio::test]
async fn connection_scoped_cypher_is_rejected() {
    let (_tmp, state) = server_state().await;
    // No ledger in the path → 400 with a pointer to the ledger-scoped route.
    let (status, body) =
        post_cypher(&state, "/v1/fluree/query", "MATCH (n:Person) RETURN n").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body={body}");
    let (status, _body) = post_cypher(
        &state,
        "/v1/fluree/update",
        r#"CREATE (n:Person {name: "X"})"#,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
