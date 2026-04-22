use axum::body::Body;
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use ed25519_dalek::{Signer, SigningKey};
use serde_json::Value as JsonValue;

use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};

/// Helper to extract JSON response
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

fn create_jws(claims: &serde_json::Value, signing_key: &SigningKey) -> String {
    let pubkey = signing_key.verifying_key().to_bytes();
    let pubkey_b64 = URL_SAFE_NO_PAD.encode(pubkey);

    let header = serde_json::json!({
        "alg": "EdDSA",
        "jwk": {
            "kty": "OKP",
            "crv": "Ed25519",
            "x": pubkey_b64
        }
    });

    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
    let payload_b64 = URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes());

    let signing_input = format!("{header_b64}.{payload_b64}");
    let signature = signing_key.sign(signing_input.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

    format!("{header_b64}.{payload_b64}.{sig_b64}")
}

fn now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

async fn data_auth_state() -> (tempfile::TempDir, std::sync::Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        // Require data auth; use insecure issuer trust for tests
        data_auth_mode: fluree_db_server::config::DataAuthMode::Required,
        data_auth_insecure_accept_any_issuer: true,
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = std::sync::Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    (tmp, state)
}

#[tokio::test]
async fn data_auth_required_blocks_query_without_auth() {
    let (_tmp, state) = data_auth_state().await;
    let app = build_router(state);

    // Create ledger (no data auth on create)
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"ledger":"auth:test"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Query without auth should fail
    let query_body = serde_json::json!({
      "select": ["?s"],
      "where": { "@id": "?s" }
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query/auth:test")
                .header("content-type", "application/json")
                .body(Body::from(query_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn data_auth_bearer_allows_read_and_write_with_scopes() {
    let (_tmp, state) = data_auth_state().await;
    let app = build_router(state);

    // Create ledger
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"ledger":"auth2:test"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Create a token with read+write scope for this ledger.
    //
    // No `fluree.identity` / `sub` claim: this test exercises the bearer-scope
    // write path, not identity-based policy. Setting `fluree.identity` would
    // inject that identity into `opts.identity` on every request, which in turn
    // causes the server to build a PolicyContext. See `tests/policy_integration.rs`
    // for the identity + policy path and its impersonation semantics.
    let secret = [7u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);

    let claims = serde_json::json!({
      "iss": fluree_db_credential::did_from_pubkey(&signing_key.verifying_key().to_bytes()),
      "exp": now_secs() + 3600,
      "iat": now_secs(),
      "fluree.ledger.read.ledgers": ["auth2:test"],
      "fluree.ledger.write.ledgers": ["auth2:test"]
    });
    let token = create_jws(&claims, &signing_key);

    // Insert via /v1/fluree/insert/<ledger...>
    let insert_body = serde_json::json!({
      "@context": { "ex": "http://example.org/" },
      "insert": { "@id": "ex:alice", "ex:name": "Alice" }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/insert/auth2:test")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(insert_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Query should succeed
    let query_body = serde_json::json!({
      "@context": { "ex": "http://example.org/" },
      "select": ["?name"],
      "where": { "@id": "?s", "ex:name": "?name" }
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query/auth2:test")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(query_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, _json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn data_auth_denies_write_outside_scope_as_not_found() {
    let (_tmp, state) = data_auth_state().await;
    let app = build_router(state);

    // Create two ledgers
    for ledger in ["a:test", "b:test"] {
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
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    // Token only has write for a:test
    let secret = [9u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let claims = serde_json::json!({
      "iss": fluree_db_credential::did_from_pubkey(&signing_key.verifying_key().to_bytes()),
      "exp": now_secs() + 3600,
      "iat": now_secs(),
      "fluree.ledger.write.ledgers": ["a:test"]
    });
    let token = create_jws(&claims, &signing_key);

    let insert_body = serde_json::json!({
      "@context": { "ex": "http://example.org/" },
      "insert": { "@id": "ex:x", "ex:name": "X" }
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/insert/b:test")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(insert_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
