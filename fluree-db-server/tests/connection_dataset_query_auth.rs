//! Bearer-scope enforcement for connection-scoped **dataset** JSON-LD queries
//! (`POST /v1/fluree/query` with a multi-default-graph `from: [array]` or a
//! `fromNamed` map).
//!
//! Review finding (PR #1267): the single-query handler derived one
//! representative ledger from `from`/`fromNamed` and only scope-checked that
//! one. A `from: ["a", "b"]` union therefore let a token scoped to `a` read
//! `b` by piggy-backing it onto the dataset. These tests assert the handler
//! now rejects the whole request (404, existence-leak-avoiding) when any
//! referenced ledger is outside the token's read scope — parity with the
//! multi-query envelope path in `multi_query_auth_integration.rs`.

use axum::body::Body;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use ed25519_dalek::{Signer, SigningKey};
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

async fn data_auth_state() -> (TempDir, Arc<AppState>) {
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

fn now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn create_jws(claims: &JsonValue, signing_key: &SigningKey) -> String {
    let pubkey = signing_key.verifying_key().to_bytes();
    let pubkey_b64 = URL_SAFE_NO_PAD.encode(pubkey);
    let header = json!({
        "alg": "EdDSA",
        "jwk": { "kty": "OKP", "crv": "Ed25519", "x": pubkey_b64 }
    });
    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
    let payload_b64 = URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes());
    let signing_input = format!("{header_b64}.{payload_b64}");
    let signature = signing_key.sign(signing_input.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());
    format!("{header_b64}.{payload_b64}.{sig_b64}")
}

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

async fn insert_one(app: &axum::Router, ledger: &str, id: &str, name: &str, token: &str) {
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
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "insert into {ledger}");
}

fn write_scoped_token(ledgers: &[&str], secret_seed: u8) -> String {
    let signing_key = SigningKey::from_bytes(&[secret_seed; 32]);
    let claims = json!({
        "iss": fluree_db_credential::did_from_pubkey(&signing_key.verifying_key().to_bytes()),
        "exp": now_secs() + 3600,
        "iat": now_secs(),
        "fluree.ledger.read.ledgers":  ledgers,
        "fluree.ledger.write.ledgers": ledgers
    });
    create_jws(&claims, &signing_key)
}

fn read_scoped_token(ledgers: &[&str], secret_seed: u8) -> String {
    let signing_key = SigningKey::from_bytes(&[secret_seed; 32]);
    let claims = json!({
        "iss": fluree_db_credential::did_from_pubkey(&signing_key.verifying_key().to_bytes()),
        "exp": now_secs() + 3600,
        "iat": now_secs(),
        "fluree.ledger.read.ledgers": ledgers
    });
    create_jws(&claims, &signing_key)
}

async fn post_query(app: &axum::Router, query: &JsonValue, token: &str) -> (StatusCode, JsonValue) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(query.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    json_body(resp).await
}

/// A `from: [in-scope, out-of-scope]` union must be rejected wholesale (404),
/// not silently allowed because the *first* ledger is in scope.
#[tokio::test]
async fn connection_from_array_outside_scope_returns_404() {
    let (_tmp, state) = data_auth_state().await;
    let app = build_router(state);
    create_ledger(&app, "dca:a").await;
    create_ledger(&app, "dca:b").await;
    let write_a = write_scoped_token(&["dca:a"], 31);
    insert_one(&app, "dca:a", "ex:p", "P", &write_a).await;
    let write_b = write_scoped_token(&["dca:b"], 32);
    insert_one(&app, "dca:b", "ex:q", "Q", &write_b).await;

    // Read token scoped to dca:a ONLY. dca:a is first in `from`, so the old
    // single-ledger check passed and dca:b leaked into the union.
    let read_a_only = read_scoped_token(&["dca:a"], 33);
    let query = json!({
        "@context": { "ex": "http://example.org/" },
        "from":   ["dca:a", "dca:b"],
        "select": ["?name"],
        "where":  { "@id": "?s", "ex:name": "?name" }
    });
    let (status, _body) = post_query(&app, &query, &read_a_only).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "out-of-scope ledger in a `from` union must 404"
    );
}

/// A token scoped to BOTH union ledgers succeeds — the check rejects only
/// genuinely out-of-scope ledgers, not every multi-ledger query.
#[tokio::test]
async fn connection_from_array_inside_scope_succeeds() {
    let (_tmp, state) = data_auth_state().await;
    let app = build_router(state);
    create_ledger(&app, "dca:c").await;
    create_ledger(&app, "dca:d").await;
    let write_c = write_scoped_token(&["dca:c"], 41);
    insert_one(&app, "dca:c", "ex:p", "P", &write_c).await;
    let write_d = write_scoped_token(&["dca:d"], 42);
    insert_one(&app, "dca:d", "ex:q", "Q", &write_d).await;

    let read_both = read_scoped_token(&["dca:c", "dca:d"], 43);
    let query = json!({
        "@context": { "ex": "http://example.org/" },
        "from":   ["dca:c", "dca:d"],
        "select": ["?name"],
        "where":  { "@id": "?s", "ex:name": "?name" }
    });
    let (status, body) = post_query(&app, &query, &read_both).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "in-scope union should succeed: {body}"
    );
}

/// Same gap via a `fromNamed` map: the out-of-scope ledger is reachable only
/// through a named-graph alias, never as the representative `from` id.
#[tokio::test]
async fn connection_from_named_outside_scope_returns_404() {
    let (_tmp, state) = data_auth_state().await;
    let app = build_router(state);
    create_ledger(&app, "dca:e").await;
    create_ledger(&app, "dca:f").await;
    let write_e = write_scoped_token(&["dca:e"], 51);
    insert_one(&app, "dca:e", "ex:p", "P", &write_e).await;
    let write_f = write_scoped_token(&["dca:f"], 52);
    insert_one(&app, "dca:f", "ex:q", "Q", &write_f).await;

    let read_e_only = read_scoped_token(&["dca:e"], 53);
    let query = json!({
        "@context": { "ex": "http://example.org/" },
        "from":      "dca:e",
        "fromNamed": { "other": { "@id": "dca:f" } },
        "select":    ["?name"],
        "where":     ["graph", "other", { "@id": "?s", "ex:name": "?name" }]
    });
    let (status, _body) = post_query(&app, &query, &read_e_only).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "out-of-scope ledger behind a fromNamed alias must 404"
    );
}
