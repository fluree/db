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
async fn cypher_http_json_envelope_with_params() {
    let (_tmp, state) = server_state().await;
    create_ledger(&state, "cypherparams").await;
    insert(
        &state,
        "cypherparams",
        json!({
            "@context": {"ex": "http://example.org/"},
            "@graph": [
                {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice"},
                {"@id": "ex:bob",   "@type": "ex:Person", "ex:name": "Bob"},
            ]
        }),
    )
    .await;

    // Parameterized write via the {cypher, params} envelope.
    let (status, body) = post_cypher(
        &state,
        "/v1/fluree/update/cypherparams",
        &json!({
            "cypher": "CREATE (n:Person {name: $name})",
            "params": {"name": "Carol"}
        })
        .to_string(),
    )
    .await;
    assert!(status.is_success(), "param write status={status}; body={body}");

    // Parameterized read via the envelope.
    let (status, body) = post_cypher(
        &state,
        "/v1/fluree/query/cypherparams",
        &json!({
            "cypher": "MATCH (n:Person {name: $name}) RETURN n.name",
            "params": {"name": "Carol"}
        })
        .to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "param read; body={body}");
    assert!(body.contains("Carol"), "param read body: {body}");
}

#[tokio::test]
async fn cypher_tx_id_reflects_parameters() {
    // Same statement, different params → distinct tx-ids (the tx-id hashes the
    // full envelope, not just the statement text).
    let (_tmp, state) = server_state().await;
    create_ledger(&state, "cyphertxid").await;

    let tx_id = |body: &str| -> String {
        serde_json::from_str::<serde_json::Value>(body)
            .ok()
            .and_then(|v| v.get("tx-id").and_then(|t| t.as_str()).map(String::from))
            .unwrap_or_default()
    };

    let (s1, b1) = post_cypher(
        &state,
        "/v1/fluree/update/cyphertxid",
        &json!({"cypher": "CREATE (n:Person {name: $name})", "params": {"name": "A"}}).to_string(),
    )
    .await;
    let (s2, b2) = post_cypher(
        &state,
        "/v1/fluree/update/cyphertxid",
        &json!({"cypher": "CREATE (n:Person {name: $name})", "params": {"name": "B"}}).to_string(),
    )
    .await;
    assert!(s1.is_success() && s2.is_success(), "{b1}\n{b2}");
    let (id1, id2) = (tx_id(&b1), tx_id(&b2));
    assert!(!id1.is_empty() && !id2.is_empty(), "tx-id present: {b1}\n{b2}");
    assert_ne!(id1, id2, "different params must yield different tx-ids");
}

#[tokio::test]
async fn cypher_http_merge_on_match_set_conditional() {
    // The conditional write (MERGE … ON MATCH SET) must resolve over the
    // server's cached-handle path: ON CREATE on the first POST, ON MATCH on
    // the second.
    let (_tmp, state) = server_state().await;
    create_ledger(&state, "cyphermerge").await;

    let stmt = r#"MERGE (n:Person {name: "Alice"})
                  ON CREATE SET n.origin = "created"
                  ON MATCH  SET n.origin = "matched""#;

    let (s1, b1) = post_cypher(&state, "/v1/fluree/update/cyphermerge", stmt).await;
    assert!(s1.is_success(), "first merge: {s1} {b1}");
    let (status, body) = post_cypher(
        &state,
        "/v1/fluree/query/cyphermerge",
        r#"MATCH (n:Person {origin: "created"}) RETURN n.name"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Alice"), "ON CREATE applied: {body}");

    let (s2, b2) = post_cypher(&state, "/v1/fluree/update/cyphermerge", stmt).await;
    assert!(s2.is_success(), "second merge: {s2} {b2}");
    let (status, body) = post_cypher(
        &state,
        "/v1/fluree/query/cyphermerge",
        r#"MATCH (n:Person {origin: "matched"}) RETURN n.name"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Alice"), "ON MATCH applied on second run: {body}");
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
