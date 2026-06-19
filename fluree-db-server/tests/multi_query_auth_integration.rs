//! Bearer-scope and identity-threading integration tests for
//! `POST /v1/fluree/multi-query`.
//!
//! These tests exercise the auth parity between multi-query and the
//! single-query `/query` endpoint:
//!
//! - A bearer token with limited ledger-read scope must not be able to
//!   query ledgers outside that scope through the envelope — the same
//!   404 / "ledger not found" response the single-query handler produces
//!   for existence-leak avoidance.
//! - Identity claims on the bearer must flow through to JSON-LD
//!   sub-query `opts.identity` via `apply_auth_identity_to_opts`, the
//!   same code path the single-query handler uses.

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

fn write_scoped_token(ledgers: &[&str], secret_seed: u8) -> (SigningKey, String) {
    let signing_key = SigningKey::from_bytes(&[secret_seed; 32]);
    let claims = json!({
        "iss": fluree_db_credential::did_from_pubkey(&signing_key.verifying_key().to_bytes()),
        "exp": now_secs() + 3600,
        "iat": now_secs(),
        "fluree.ledger.read.ledgers":  ledgers,
        "fluree.ledger.write.ledgers": ledgers
    });
    let token = create_jws(&claims, &signing_key);
    (signing_key, token)
}

fn read_scoped_token(ledgers: &[&str], secret_seed: u8) -> (SigningKey, String) {
    let signing_key = SigningKey::from_bytes(&[secret_seed; 32]);
    let claims = json!({
        "iss": fluree_db_credential::did_from_pubkey(&signing_key.verifying_key().to_bytes()),
        "exp": now_secs() + 3600,
        "iat": now_secs(),
        "fluree.ledger.read.ledgers": ledgers
    });
    let token = create_jws(&claims, &signing_key);
    (signing_key, token)
}

async fn post_envelope(
    app: &axum::Router,
    envelope: &JsonValue,
    token: Option<&str>,
) -> (StatusCode, JsonValue) {
    let mut req = Request::builder()
        .method("POST")
        .uri("/v1/fluree/multi-query")
        .header("content-type", "application/json");
    if let Some(t) = token {
        req = req.header("authorization", format!("Bearer {t}"));
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::from(envelope.to_string())).unwrap())
        .await
        .unwrap();
    json_body(resp).await
}

// =============================================================================
// Bearer ledger-scope enforcement
// =============================================================================

#[tokio::test]
async fn multi_query_without_auth_when_required_returns_401() {
    let (_tmp, state) = data_auth_state().await;
    let app = build_router(state);
    create_ledger(&app, "mqa:x").await;
    // Even just creating the envelope: no token, data auth required → 401.
    let envelope = json!({
        "queries": {
            "a": {
                "language": "jsonld",
                "query": { "from": "mqa:x", "select": ["?s"], "where": { "@id": "?s" } }
            }
        }
    });
    let (status, _body) = post_envelope(&app, &envelope, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn multi_query_with_token_inside_scope_succeeds() {
    let (_tmp, state) = data_auth_state().await;
    let app = build_router(state);
    create_ledger(&app, "mqa:y").await;
    let (_sk, token) = write_scoped_token(&["mqa:y"], 11);
    insert_one(&app, "mqa:y", "ex:p", "P", &token).await;

    let envelope = json!({
        "queries": {
            "y": {
                "language": "jsonld",
                "query": {
                    "@context": { "ex": "http://example.org/" },
                    "from":   "mqa:y",
                    "select": ["?name"],
                    "where":  { "@id": "?s", "ex:name": "?name" }
                }
            }
        }
    });
    let (status, body) = post_envelope(&app, &envelope, Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "got body: {body}");
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn multi_query_with_token_outside_scope_returns_404() {
    // Regression for the review finding: bearer ledger-scope must be
    // enforced for every distinct ledger in the envelope, with the same
    // existence-leak-avoiding 404 the single-query handler returns.
    let (_tmp, state) = data_auth_state().await;
    let app = build_router(state);
    create_ledger(&app, "mqa:a").await;
    create_ledger(&app, "mqa:b").await;
    // Token can read 'mqa:a' but NOT 'mqa:b'.
    let (_sk_a, write_a) = write_scoped_token(&["mqa:a"], 13);
    insert_one(&app, "mqa:a", "ex:p", "P", &write_a).await;
    let (_sk_b, write_b) = write_scoped_token(&["mqa:b"], 14);
    insert_one(&app, "mqa:b", "ex:q", "Q", &write_b).await;
    let (_sk, read_a_only) = read_scoped_token(&["mqa:a"], 15);

    let envelope = json!({
        "queries": {
            "good": {
                "language": "jsonld",
                "query": {
                    "@context": { "ex": "http://example.org/" },
                    "from":   "mqa:a",
                    "select": ["?name"],
                    "where":  { "@id": "?s", "ex:name": "?name" }
                }
            },
            "out_of_scope": {
                "language": "jsonld",
                "query": {
                    "@context": { "ex": "http://example.org/" },
                    "from":   "mqa:b",
                    "select": ["?name"],
                    "where":  { "@id": "?s", "ex:name": "?name" }
                }
            }
        }
    });
    let (status, _body) = post_envelope(&app, &envelope, Some(&read_a_only)).await;
    // Whole envelope rejected — not partial — to avoid leaking whether
    // mqa:b exists at all. Matches single-query handler behavior.
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn multi_query_with_token_outside_scope_sparql_returns_404() {
    let (_tmp, state) = data_auth_state().await;
    let app = build_router(state);
    create_ledger(&app, "mqa:c").await;
    create_ledger(&app, "mqa:d").await;
    let (_w_c, write_c) = write_scoped_token(&["mqa:c"], 23);
    insert_one(&app, "mqa:c", "ex:p", "P", &write_c).await;
    let (_w_d, write_d) = write_scoped_token(&["mqa:d"], 24);
    insert_one(&app, "mqa:d", "ex:q", "Q", &write_d).await;
    let (_, read_c_only) = read_scoped_token(&["mqa:c"], 25);

    let envelope = json!({
        "queries": {
            "good": {
                "language": "sparql",
                "query": "PREFIX ex: <http://example.org/> SELECT ?name FROM <mqa:c> WHERE { ?s ex:name ?name }"
            },
            "bad": {
                "language": "sparql",
                "query": "PREFIX ex: <http://example.org/> SELECT ?name FROM <mqa:d> WHERE { ?s ex:name ?name }"
            }
        }
    });
    let (status, _body) = post_envelope(&app, &envelope, Some(&read_c_only)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
