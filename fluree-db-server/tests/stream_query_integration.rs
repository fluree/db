//! HTTP integration tests for the streaming query endpoint
//! (`POST /v1/fluree/stream/query/*ledger`).
//!
//! These exercise the full server handler — content-type routing, the
//! Single-vs-Dataset decision, NDJSON framing, and the security/parity
//! refusals (policy headers, history `to`, ASK/CONSTRUCT) — which the
//! API-producer tests in `fluree-db-api/tests/it_stream_query.rs` bypass.

use axum::body::Body;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use ed25519_dalek::{Signer, SigningKey};
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use tower::ServiceExt;

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

async fn create_ledger(app: &axum::Router, ledger: &str) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(format!(r#"{{"ledger":"{ledger}"}}"#)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "create {ledger}");
}

async fn insert_name(app: &axum::Router, ledger: &str, id: &str, name: &str) {
    let body = json!({
        "@context": { "ex": "http://example.org/" },
        "insert":   { "@id": id, "ex:name": name }
    });
    let resp = app
        .clone()
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
    assert_eq!(resp.status(), StatusCode::OK, "insert into {ledger}");
}

/// Send a request and parse the response as NDJSON records (one per line).
async fn ndjson_records(
    resp: http::Response<Body>,
) -> (StatusCode, Option<String>, Vec<JsonValue>) {
    let status = resp.status();
    let content_type = resp
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("collect")
        .to_bytes();
    let text = String::from_utf8(bytes.to_vec()).expect("utf-8 body");
    let records = text
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str::<JsonValue>(l).expect("each NDJSON line is valid JSON"))
        .collect();
    (status, content_type, records)
}

async fn stream_jsonld(
    app: &axum::Router,
    ledger: &str,
    query: &JsonValue,
) -> http::Response<Body> {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/stream/query/{ledger}"))
                .header("content-type", "application/json")
                .body(Body::from(query.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn stream_sparql(
    app: &axum::Router,
    ledger: &str,
    sparql: &str,
    extra_header: Option<(&str, &str)>,
) -> http::Response<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri(format!("/v1/fluree/stream/query/{ledger}"))
        .header("content-type", "application/sparql-query");
    if let Some((k, v)) = extra_header {
        builder = builder.header(k, v);
    }
    app.clone()
        .oneshot(builder.body(Body::from(sparql.to_string())).unwrap())
        .await
        .unwrap()
}

async fn state_no_heartbeat() -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        stream_heartbeat_ms: 0, // disabled
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    (tmp, state)
}

#[tokio::test]
async fn heartbeat_disabled_still_streams() {
    let (_tmp, state) = state_no_heartbeat().await;
    let app = build_router(state);
    create_ledger(&app, "strm:nohb").await;
    insert_name(&app, "strm:nohb", "ex:x", "Xavier").await;

    let query = json!({
        "@context": { "ex": "http://example.org/" },
        "select": ["?name"],
        "where": { "@id": "?s", "ex:name": "?name" }
    });
    let resp = stream_jsonld(&app, "strm:nohb", &query).await;
    let (status, ct, records) = ndjson_records(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ct.as_deref(), Some("application/x-ndjson"));
    assert_eq!(records.first().unwrap()["type"], "head");
    assert_eq!(records.last().unwrap()["type"], "end");
    assert_eq!(records.last().unwrap()["rows"], 1);
    assert_eq!(
        records.iter().filter(|r| r["type"] == "heartbeat").count(),
        0,
        "no heartbeat records when disabled"
    );
}

#[tokio::test]
async fn jsonld_select_streams_ndjson() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "strm:sel").await;
    insert_name(&app, "strm:sel", "ex:x", "Xavier").await;
    insert_name(&app, "strm:sel", "ex:y", "Yolanda").await;

    let query = json!({
        "@context": { "ex": "http://example.org/" },
        "select": ["?name"],
        "where": { "@id": "?s", "ex:name": "?name" }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/stream/query/strm:sel")
                .header("content-type", "application/json")
                .body(Body::from(query.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, content_type, records) = ndjson_records(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(content_type.as_deref(), Some("application/x-ndjson"));

    assert_eq!(records.first().unwrap()["type"], "head");
    assert_eq!(records.last().unwrap()["type"], "end");
    assert_eq!(records.last().unwrap()["rows"], 2);
    assert_eq!(
        records.iter().filter(|r| r["type"] == "row").count(),
        2,
        "two row records"
    );
    // end carries fuel since the streaming endpoint tracks it.
    assert!(records.last().unwrap()["fuel"].as_f64().unwrap() >= 1.0);
}

#[tokio::test]
async fn sparql_select_streams_ndjson() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "strm:sparql").await;
    insert_name(&app, "strm:sparql", "ex:x", "Xavier").await;

    let resp = stream_sparql(
        &app,
        "strm:sparql",
        "SELECT ?name WHERE { ?s <http://example.org/name> ?name }",
        None,
    )
    .await;
    let (status, content_type, records) = ndjson_records(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(content_type.as_deref(), Some("application/x-ndjson"));
    assert_eq!(records.first().unwrap()["type"], "head");
    assert_eq!(records.last().unwrap()["type"], "end");
    assert_eq!(records.last().unwrap()["rows"], 1);
}

#[tokio::test]
async fn multi_ledger_from_streams_union() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "strm:a").await;
    create_ledger(&app, "strm:b").await;
    insert_name(&app, "strm:a", "ex:p", "Alice").await;
    insert_name(&app, "strm:b", "ex:q", "Bob").await;

    // `from: [a, b]` routes through the connection/dataset streaming path.
    let query = json!({
        "@context": { "ex": "http://example.org/" },
        "from": ["strm:a", "strm:b"],
        "select": ["?name"],
        "where": { "@id": "?s", "ex:name": "?name" }
    });
    let resp = stream_jsonld(&app, "strm:a", &query).await;
    let (status, _ct, records) = ndjson_records(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(records.last().unwrap()["type"], "end");

    let mut names: Vec<String> = records
        .iter()
        .filter(|r| r["type"] == "row")
        .map(|r| r["row"]["name"]["value"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["Alice", "Bob"], "union across both ledgers");
}

#[tokio::test]
async fn ask_query_rejected() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "strm:ask").await;

    let resp = stream_sparql(&app, "strm:ask", "ASK { ?s ?p ?o }", None).await;
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "ASK is rejected before the stream"
    );
}

#[tokio::test]
async fn construct_query_rejected() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "strm:con").await;

    let resp = stream_sparql(
        &app,
        "strm:con",
        "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "CONSTRUCT rejected");
}

#[tokio::test]
async fn history_to_query_rejected() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "strm:hist").await;
    insert_name(&app, "strm:hist", "ex:x", "Xavier").await;

    // Top-level `to` (history) must be rejected, not planned at current view.
    let query = json!({
        "@context": { "ex": "http://example.org/" },
        "from": "strm:hist",
        "to": 1,
        "select": ["?name"],
        "where": { "@id": "?s", "ex:name": "?name" }
    });
    let resp = stream_jsonld(&app, "strm:hist", &query).await;
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "history `to` is rejected on the streaming endpoint"
    );
}

/// A SPARQL `FROM` clause is no longer rejected — it routes through the
/// connection/dataset streaming path (FROM selects graphs within the ledger).
#[tokio::test]
async fn sparql_from_clause_streams() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "strm:from").await;
    insert_name(&app, "strm:from", "ex:x", "Xavier").await;
    insert_name(&app, "strm:from", "ex:y", "Yolanda").await;

    let resp = stream_sparql(
        &app,
        "strm:from",
        "SELECT ?name FROM <strm:from> WHERE { ?s <http://example.org/name> ?name }",
        None,
    )
    .await;
    let (status, _ct, records) = ndjson_records(resp).await;
    assert_eq!(status, StatusCode::OK, "SPARQL FROM streams (not refused)");
    assert_eq!(records.last().unwrap()["type"], "end");
    assert_eq!(
        records.iter().filter(|r| r["type"] == "row").count(),
        2,
        "both rows stream via the dataset path"
    );
}

/// A SPARQL request carrying a `Fluree-Policy-Class` header is enforced (not
/// refused): it routes through the connection/dataset streaming path with the
/// policy applied, so only rows the class permits are streamed.
#[tokio::test]
async fn sparql_policy_class_header_enforced() {
    let (_tmp, state) = policy_state().await;
    let app = build_router(state);
    setup_policy_ledger(&app, "strm:sparql-pol").await;

    let resp = stream_sparql(
        &app,
        "strm:sparql-pol",
        "SELECT ?name WHERE { ?d <http://example.org/name> ?name }",
        Some(("fluree-policy-class", "http://example.org/PublicClass")),
    )
    .await;
    let (status, _ct, records) = ndjson_records(resp).await;
    assert_eq!(status, StatusCode::OK, "SPARQL + policy header streams");
    let names: Vec<&str> = records
        .iter()
        .filter(|r| r["type"] == "row")
        .filter_map(|r| r["row"]["name"]["value"].as_str())
        .collect();
    assert_eq!(
        names,
        vec!["Public"],
        "policy-class header filters to the public doc"
    );
}

/// Build a state with data-auth Optional (so body `opts.identity` is honored)
/// and a ledger with three classification-tagged docs + per-class policies.
async fn policy_state() -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        data_auth_mode: fluree_db_server::config::DataAuthMode::Optional,
        data_auth_insecure_accept_any_issuer: true,
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    (tmp, state)
}

async fn insert(app: &axum::Router, ledger: &str, tx: &JsonValue) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/insert/{ledger}"))
                .header("content-type", "application/json")
                .body(Body::from(tx.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "insert into {ledger}");
}

/// Create a ledger with three classification-tagged docs and per-class
/// policies (`ex:PublicClass` → public only, `ex:ManagerClass` → f:allow true)
/// plus identity→class bindings. Shared by the JSON-LD and SPARQL policy tests.
async fn setup_policy_ledger(app: &axum::Router, ledger: &str) {
    create_ledger(app, ledger).await;
    insert(
        app,
        ledger,
        &json!({
            "@context": { "ex": "http://example.org/" },
            "insert": [
                {"@id": "ex:d1", "@type": "ex:Doc", "ex:name": "Public",  "ex:class": "public"},
                {"@id": "ex:d2", "@type": "ex:Doc", "ex:name": "Internal","ex:class": "internal"},
                {"@id": "ex:d3", "@type": "ex:Doc", "ex:name": "Secret",  "ex:class": "confidential"}
            ]
        }),
    )
    .await;
    insert(
        app,
        ledger,
        &json!({
            "@context": { "f": "https://ns.flur.ee/db#", "ex": "http://example.org/" },
            "insert": [
                {
                    "@id": "ex:public-policy",
                    "@type": ["f:AccessPolicy", "ex:PublicClass"],
                    "f:action": [{"@id": "f:view"}],
                    "f:query": {
                        "@type": "@json",
                        "@value": {
                            "@context": {"ex": "http://example.org/"},
                            "where": [{"@id": "?$this", "ex:class": "public"}]
                        }
                    }
                },
                {
                    "@id": "ex:manager-policy",
                    "@type": ["f:AccessPolicy", "ex:ManagerClass"],
                    "f:action": [{"@id": "f:view"}],
                    "f:allow": true
                },
                {"@id": "http://example.org/public-user", "f:policyClass": [{"@id": "ex:PublicClass"}]},
                {"@id": "http://example.org/manager-user", "f:policyClass": [{"@id": "ex:ManagerClass"}]}
            ]
        }),
    )
    .await;
}

/// Policy enforcement must apply on the streaming path: a query carrying
/// `opts.identity` upgrades to the connection/dataset streaming path, which
/// enforces the identity's policy class — so a restricted identity streams
/// fewer rows than an unrestricted one.
#[tokio::test]
async fn policy_identity_filters_streamed_rows() {
    let (_tmp, state) = policy_state().await;
    let app = build_router(state);
    setup_policy_ledger(&app, "strm:pol-enf").await;

    let docs_query = |identity: &str| {
        json!({
            "@context": { "ex": "http://example.org/" },
            "opts": { "identity": identity, "default-allow": false },
            "select": ["?name"],
            "where": [
                {"@id": "?d", "@type": "ex:Doc"},
                {"@id": "?d", "ex:name": "?name"}
            ]
        })
    };

    // Restricted identity → only the public doc streams.
    let resp = stream_jsonld(
        &app,
        "strm:pol-enf",
        &docs_query("http://example.org/public-user"),
    )
    .await;
    let (status, _ct, records) = ndjson_records(resp).await;
    assert_eq!(status, StatusCode::OK);
    let public_rows: Vec<&str> = records
        .iter()
        .filter(|r| r["type"] == "row")
        .filter_map(|r| r["row"]["name"]["value"].as_str())
        .collect();
    assert_eq!(
        public_rows,
        vec!["Public"],
        "public-user sees only the public doc"
    );

    // Manager identity (f:allow true) → all three stream.
    let resp = stream_jsonld(
        &app,
        "strm:pol-enf",
        &docs_query("http://example.org/manager-user"),
    )
    .await;
    let (_status, _ct, records) = ndjson_records(resp).await;
    let manager_count = records.iter().filter(|r| r["type"] == "row").count();
    assert_eq!(
        manager_count, 3,
        "manager sees all docs; policy upgrade enforced on stream"
    );
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn create_jws(claims: &JsonValue, key: &SigningKey) -> String {
    let pubkey_b64 = URL_SAFE_NO_PAD.encode(key.verifying_key().to_bytes());
    let header = json!({"alg": "EdDSA", "jwk": {"kty": "OKP", "crv": "Ed25519", "x": pubkey_b64}});
    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
    let payload_b64 = URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes());
    let signing_input = format!("{header_b64}.{payload_b64}");
    let sig_b64 = URL_SAFE_NO_PAD.encode(key.sign(signing_input.as_bytes()).to_bytes());
    format!("{header_b64}.{payload_b64}.{sig_b64}")
}

fn read_token(ledgers: &[&str], seed: u8) -> String {
    let key = SigningKey::from_bytes(&[seed; 32]);
    create_jws(
        &json!({
            "iss": fluree_db_credential::did_from_pubkey(&key.verifying_key().to_bytes()),
            "exp": now_secs() + 3600,
            "iat": now_secs(),
            "fluree.ledger.read.ledgers": ledgers,
            "fluree.ledger.write.ledgers": ledgers
        }),
        &key,
    )
}

async fn required_auth_state() -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        data_auth_mode: fluree_db_server::config::DataAuthMode::Required,
        data_auth_insecure_accept_any_issuer: true,
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    (tmp, state)
}

async fn stream_jsonld_token(
    app: &axum::Router,
    ledger: &str,
    query: &JsonValue,
    token: &str,
) -> http::Response<Body> {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/stream/query/{ledger}"))
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(query.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

/// A read token scoped to one ledger must not stream another via the streaming
/// endpoint — same bearer-scope enforcement as `/query` (404, no existence leak).
#[tokio::test]
async fn bearer_scope_enforced_on_stream() {
    let (_tmp, state) = required_auth_state().await;
    let app = build_router(state);
    create_ledger(&app, "strm:scopeA").await;
    create_ledger(&app, "strm:scopeB").await;

    let tok_a = read_token(&["strm:scopeA"], 7);
    let tok_b = read_token(&["strm:scopeB"], 8);

    // Seed B with its own token.
    let seed = json!({
        "@context": { "ex": "http://example.org/" },
        "insert": { "@id": "ex:x", "ex:name": "Bee" }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/insert/strm:scopeB")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {tok_b}"))
                .body(Body::from(seed.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "seed B");

    let query = json!({
        "@context": { "ex": "http://example.org/" },
        "select": ["?name"],
        "where": { "@id": "?s", "ex:name": "?name" }
    });

    // Token scoped to A streaming B → 404 (out of scope).
    let resp = stream_jsonld_token(&app, "strm:scopeB", &query, &tok_a).await;
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "A-scoped token must not stream B"
    );

    // Token scoped to B streaming B → 200 + rows.
    let resp = stream_jsonld_token(&app, "strm:scopeB", &query, &tok_b).await;
    let (status, _ct, records) = ndjson_records(resp).await;
    assert_eq!(status, StatusCode::OK, "B-scoped token streams B");
    assert_eq!(records.last().unwrap()["type"], "end");
    assert_eq!(records.last().unwrap()["rows"], 1);
}

/// A fuel overrun surfaces as a terminal `error` record carrying a
/// machine-readable `code`. A `max-fuel` below the 1.0 floor trips the floor
/// charge before any row, so the stream is a single error terminal (the 200 is
/// already committed when the body starts).
#[tokio::test]
async fn fuel_overrun_emits_error_code_terminal() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "strm:fuel").await;
    insert_name(&app, "strm:fuel", "ex:x", "Xavier").await;

    let query = json!({
        "@context": { "ex": "http://example.org/" },
        "opts": { "max-fuel": 0.5 },
        "select": ["?name"],
        "where": { "@id": "?s", "ex:name": "?name" }
    });
    let resp = stream_jsonld(&app, "strm:fuel", &query).await;
    let (status, _ct, records) = ndjson_records(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "stream is committed before execution"
    );

    let terminal = records.last().expect("a terminal record");
    assert_eq!(terminal["type"], "error");
    assert_eq!(terminal["error"]["code"], "fuel_exhausted");
}

#[tokio::test]
async fn unknown_ledger_streams_error_terminal_or_4xx() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);

    let query = json!({
        "@context": { "ex": "http://example.org/" },
        "select": ["?name"],
        "where": { "@id": "?s", "ex:name": "?name" }
    });
    let resp = stream_jsonld(&app, "strm:nope", &query).await;
    // Loading a missing ledger fails before the stream commits → 4xx.
    assert!(
        resp.status().is_client_error() || resp.status().is_server_error(),
        "missing ledger should not 200, got {}",
        resp.status()
    );
}
