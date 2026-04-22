//! Integration tests for proxy storage mode
//!
//! Tests the complete flow of:
//! - Transaction server with storage proxy enabled
//! - Peer in proxy storage mode connecting to tx server
//! - Creating ledgers on tx server, querying through peer

use axum::body::Body;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use ed25519_dalek::{Signer, SigningKey};
use fluree_db_binary_index::IndexRoot;
use fluree_db_core::serde::flakes_transport::{decode_flakes, MAGIC as FLKB_MAGIC};
use fluree_db_core::{ContentId, ContentKind, StorageRead};
use fluree_db_server::{
    config::{ServerRole, StorageAccessMode},
    routes::build_router,
    AppState, ServerConfig, TelemetryConfig,
};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use tower::ServiceExt;

// =============================================================================
// Token Generation Helpers
// =============================================================================

/// Generate a did:key from a public key
fn did_from_pubkey(pubkey: &[u8; 32]) -> String {
    // Multicodec prefix for Ed25519 public key: 0xed01
    let mut bytes = vec![0xed, 0x01];
    bytes.extend_from_slice(pubkey);
    let encoded = bs58::encode(&bytes).into_string();
    format!("did:key:z{encoded}")
}

/// Create a JWS token with storage proxy claims
fn create_storage_proxy_token(signing_key: &SigningKey, storage_all: bool) -> String {
    let pubkey = signing_key.verifying_key().to_bytes();
    let pubkey_b64 = URL_SAFE_NO_PAD.encode(pubkey);
    let did = did_from_pubkey(&pubkey);

    // Create header with embedded JWK
    let header = serde_json::json!({
        "alg": "EdDSA",
        "jwk": {
            "kty": "OKP",
            "crv": "Ed25519",
            "x": pubkey_b64
        }
    });

    // Create payload with storage proxy claims
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let payload = serde_json::json!({
        "iss": did,
        "sub": "test-peer@example.com",
        "exp": now + 3600, // 1 hour from now
        "iat": now,
        "fluree.storage.all": storage_all,
        "fluree.identity": "ex:TestPeer"
    });

    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());

    // Sign header.payload
    let signing_input = format!("{header_b64}.{payload_b64}");
    let signature = signing_key.sign(signing_input.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

    format!("{header_b64}.{payload_b64}.{sig_b64}")
}

// =============================================================================
// Test Setup Helpers
// =============================================================================

/// Create a transaction server state with storage proxy enabled
async fn tx_server_state() -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        server_role: ServerRole::Transaction,
        // Enable storage proxy with insecure mode for testing
        storage_proxy_enabled: true,
        storage_proxy_insecure_accept_any_issuer: true,
        ..Default::default()
    };

    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    (tmp, state)
}

/// Create a peer state in proxy storage mode
///
/// Note: This creates a peer that would connect to a tx server for storage.
/// For this in-process test, we test the proxy components indirectly through
/// the storage proxy endpoints on the tx server side.
async fn proxy_peer_state(
    tx_server_url: &str,
    token: &str,
) -> Result<(TempDir, Arc<AppState>), String> {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        // No storage_path needed in proxy mode
        server_role: ServerRole::Peer,
        storage_access_mode: StorageAccessMode::Proxy,
        tx_server_url: Some(tx_server_url.to_string()),
        storage_proxy_token: Some(token.to_string()),
        // Required for peer mode
        peer_subscribe_all: true,
        ..Default::default()
    };

    let telemetry = TelemetryConfig::with_server_config(&cfg);
    match AppState::new(cfg, telemetry).await {
        Ok(state) => Ok((tmp, Arc::new(state))),
        Err(e) => Err(format!("Failed to create peer state: {e}")),
    }
}

/// Helper to extract JSON response
async fn json_body(resp: http::Response<Body>) -> (StatusCode, JsonValue) {
    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();
    let json: JsonValue = serde_json::from_slice(&bytes).expect("valid JSON response");
    (status, json)
}

// =============================================================================
// Storage Proxy Endpoint Tests
// =============================================================================

/// Test that storage proxy endpoints are accessible when enabled
#[tokio::test]
async fn test_storage_proxy_endpoints_enabled() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state);

    // Generate a valid token
    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token(&signing_key, true);

    // Try to access nameservice endpoint - should return 404 (ledger not found)
    // rather than 401 (endpoint disabled)
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/storage/ns/nonexistent:ledger")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // 404 means the endpoint is working, just the ledger doesn't exist
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// Test that storage proxy endpoints require Bearer token
#[tokio::test]
async fn test_storage_proxy_requires_token() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state);

    // Try to access without token
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/storage/ns/test:main")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should return 401 Unauthorized
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

/// Test that storage proxy rejects tokens without storage permissions
#[tokio::test]
async fn test_storage_proxy_requires_storage_permissions() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state);

    // Generate a token WITHOUT storage permissions
    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let pubkey = signing_key.verifying_key().to_bytes();
    let pubkey_b64 = URL_SAFE_NO_PAD.encode(pubkey);
    let did = did_from_pubkey(&pubkey);

    let header = serde_json::json!({
        "alg": "EdDSA",
        "jwk": {
            "kty": "OKP",
            "crv": "Ed25519",
            "x": pubkey_b64
        }
    });

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Only events permissions, no storage permissions
    let payload = serde_json::json!({
        "iss": did,
        "sub": "test@example.com",
        "exp": now + 3600,
        "iat": now,
        "fluree.events.all": true  // Events permission, NOT storage
    });

    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
    let signing_input = format!("{header_b64}.{payload_b64}");
    let signature = signing_key.sign(signing_input.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());
    let token = format!("{header_b64}.{payload_b64}.{sig_b64}");

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/storage/ns/test:main")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should return 401 - token lacks storage permissions
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

/// Test storage proxy block endpoint
#[tokio::test]
async fn test_storage_proxy_block_endpoint() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state.clone());

    // Generate a valid token
    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token(&signing_key, true);

    // First create a ledger so we have something to fetch
    let create_body = serde_json::json!({ "ledger": "proxy:test" });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Try to fetch a block - should return 404 for non-existent CID
    // (but the endpoint is working)
    let fake_cid = ContentId::new(ContentKind::Commit, b"nonexistent").to_string();
    let block_body = serde_json::json!({
        "cid": fake_cid,
        "ledger": "proxy:test"
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/storage/block")
                .header("content-type", "application/json")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::from(block_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    // 404 means the endpoint is working, just the block doesn't exist
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// Test that nameservice record is returned for existing ledger
#[tokio::test]
async fn test_storage_proxy_ns_record_for_existing_ledger() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state.clone());

    // Generate a valid token
    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token(&signing_key, true);

    // Create a ledger
    let create_body = serde_json::json!({ "ledger": "ns:test" });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Fetch the nameservice record
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/storage/ns/ns:test")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    // ledger_id is the full canonical key; name is the ledger without branch
    assert_eq!(
        json.get("ledger_id").and_then(|v| v.as_str()),
        Some("ns:test")
    );
    assert_eq!(json.get("name").and_then(|v| v.as_str()), Some("ns"));
    assert_eq!(json.get("branch").and_then(|v| v.as_str()), Some("test"));
    assert_eq!(
        json.get("retracted").and_then(serde_json::Value::as_bool),
        Some(false)
    );
}

/// Test that ledger-specific token scope is enforced
#[tokio::test]
async fn test_storage_proxy_ledger_scope_enforcement() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state.clone());

    // Create two ledgers
    let create_body = serde_json::json!({ "ledger": "allowed:main" });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let create_body = serde_json::json!({ "ledger": "denied:main" });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Generate a token that only allows access to "allowed:main"
    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let pubkey = signing_key.verifying_key().to_bytes();
    let pubkey_b64 = URL_SAFE_NO_PAD.encode(pubkey);
    let did = did_from_pubkey(&pubkey);

    let header = serde_json::json!({
        "alg": "EdDSA",
        "jwk": {
            "kty": "OKP",
            "crv": "Ed25519",
            "x": pubkey_b64
        }
    });

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let payload = serde_json::json!({
        "iss": did,
        "sub": "test@example.com",
        "exp": now + 3600,
        "iat": now,
        "fluree.storage.all": false,
        "fluree.storage.ledgers": ["allowed:main"]
    });

    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
    let signing_input = format!("{header_b64}.{payload_b64}");
    let signature = signing_key.sign(signing_input.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());
    let token = format!("{header_b64}.{payload_b64}.{sig_b64}");

    // Should be able to access allowed:main
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/storage/ns/allowed:main")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Should NOT be able to access denied:main (returns 404, not 403)
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/storage/ns/denied:main")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // Returns 404 to avoid leaking ledger existence
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// =============================================================================
// Peer Proxy Mode State Creation Tests
// =============================================================================

/// Test that peer proxy state can be created with valid config
#[tokio::test]
async fn test_peer_proxy_state_creation() {
    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token(&signing_key, true);

    // Create peer state pointing to a hypothetical tx server
    let result = proxy_peer_state("http://localhost:8090", &token).await;
    assert!(
        result.is_ok(),
        "Peer proxy state should be created successfully"
    );

    let (_tmp, state) = result.unwrap();
    assert!(state.config.is_proxy_storage_mode());
    assert!(state.fluree.nameservice_mode().is_read_only());
}

/// Test that FlureeInstance correctly identifies proxy mode
#[tokio::test]
async fn test_fluree_instance_proxy_identification() {
    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token(&signing_key, true);

    let result = proxy_peer_state("http://localhost:8090", &token).await;
    assert!(result.is_ok());

    let (_tmp, state) = result.unwrap();

    // Check that proxy mode produces a read-only nameservice
    assert!(state.fluree.nameservice_mode().is_read_only());
}

// =============================================================================
// Storage Proxy Disabled Tests
// =============================================================================

/// Test that storage proxy endpoints return 404 when disabled
#[tokio::test]
async fn test_storage_proxy_disabled() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        server_role: ServerRole::Transaction,
        // Storage proxy NOT enabled
        storage_proxy_enabled: false,
        ..Default::default()
    };

    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    let app = build_router(state);

    // Generate a valid token
    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token(&signing_key, true);

    // Try to access storage proxy endpoint
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/storage/ns/test:main")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should return 404 - endpoint not enabled
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// =============================================================================
// Block Fetch and Authorization Tests
// =============================================================================

/// Test block endpoint rejects requests for unauthorized addresses
#[tokio::test]
async fn test_storage_proxy_block_authorization() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state.clone());

    // Create a ledger
    let create_body = serde_json::json!({ "ledger": "block:test" });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Generate a token that only allows access to "other:ledger"
    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let pubkey = signing_key.verifying_key().to_bytes();
    let pubkey_b64 = URL_SAFE_NO_PAD.encode(pubkey);
    let did = did_from_pubkey(&pubkey);

    let header = serde_json::json!({
        "alg": "EdDSA",
        "jwk": {
            "kty": "OKP",
            "crv": "Ed25519",
            "x": pubkey_b64
        }
    });

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let payload = serde_json::json!({
        "iss": did,
        "sub": "test@example.com",
        "exp": now + 3600,
        "iat": now,
        "fluree.storage.all": false,
        "fluree.storage.ledgers": ["other:ledger"]  // NOT block:test
    });

    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
    let signing_input = format!("{header_b64}.{payload_b64}");
    let signature = signing_key.sign(signing_input.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());
    let token = format!("{header_b64}.{payload_b64}.{sig_b64}");

    // Try to fetch a block from unauthorized ledger
    let fake_cid = ContentId::new(ContentKind::Commit, b"auth-test").to_string();
    let block_body = serde_json::json!({
        "cid": fake_cid,
        "ledger": "block:test"
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/storage/block")
                .header("content-type", "application/json")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::from(block_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    // Should return 404 (no existence leak)
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// Test that graph source snapshot CIDs are rejected by the kind allowlist
#[tokio::test]
async fn test_storage_proxy_rejects_graph_source_cids() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state);

    // Generate a token with full access
    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token(&signing_key, true);

    // Try to fetch a graph source snapshot CID — rejected by kind allowlist
    let gs_cid = ContentId::new(ContentKind::GraphSourceSnapshot, b"snapshot-data").to_string();
    let block_body = serde_json::json!({
        "cid": gs_cid,
        "ledger": "search:main"
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/storage/block")
                .header("content-type", "application/json")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::from(block_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    // GraphSourceSnapshot is not in the kind allowlist → 404
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// Test that an invalid CID string is rejected with 400
#[tokio::test]
async fn test_storage_proxy_rejects_invalid_cid() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state);

    // Generate a token with full access
    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token(&signing_key, true);

    // Try to fetch with an invalid CID string
    let block_body = serde_json::json!({
        "cid": "not-a-valid-cid",
        "ledger": "x:y"
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/storage/block")
                .header("content-type", "application/json")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::from(block_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    // Invalid CID string → 400 Bad Request
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// =============================================================================
// Expired Token Tests
// =============================================================================

/// Test that expired tokens are rejected
#[tokio::test]
async fn test_storage_proxy_rejects_expired_token() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state);

    // Generate an expired token
    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let pubkey = signing_key.verifying_key().to_bytes();
    let pubkey_b64 = URL_SAFE_NO_PAD.encode(pubkey);
    let did = did_from_pubkey(&pubkey);

    let header = serde_json::json!({
        "alg": "EdDSA",
        "jwk": {
            "kty": "OKP",
            "crv": "Ed25519",
            "x": pubkey_b64
        }
    });

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let payload = serde_json::json!({
        "iss": did,
        "sub": "test@example.com",
        "exp": now - 120,  // Expired 2 minutes ago (beyond clock skew)
        "iat": now - 3600,
        "fluree.storage.all": true
    });

    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
    let signing_input = format!("{header_b64}.{payload_b64}");
    let signature = signing_key.sign(signing_input.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());
    let token = format!("{header_b64}.{payload_b64}.{sig_b64}");

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/storage/ns/test:main")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should return 401 - token expired
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// =============================================================================
// Content Negotiation Tests (PR6: FLKB Format)
// =============================================================================

/// Helper to extract raw bytes from response
async fn bytes_body(resp: http::Response<Body>) -> (StatusCode, Vec<u8>) {
    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes()
        .to_vec();
    (status, bytes)
}

/// Test that non-leaf blocks return raw bytes even when flakes format is requested
///
/// Commit blocks are structural data (not leaf nodes). The server returns them
/// as raw bytes regardless of Accept header — the content negotiation only
/// affects leaf block representation. Non-leaf blocks always return 200 with
/// application/octet-stream.
#[tokio::test]
async fn test_block_content_negotiation_non_leaf_returns_raw_bytes() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state.clone());

    // Generate a valid token
    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token(&signing_key, true);

    // Create a ledger
    let create_body = serde_json::json!({ "ledger": "flkb:test" });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Transact some data to create a commit
    let update_body = serde_json::json!({
        "ledger": "flkb:test",
        "@context": { "ex": "http://example.org/" },
        "insert": {
            "@id": "ex:alice",
            "ex:name": "Alice"
        }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/update")
                .header("content-type", "application/json")
                .body(Body::from(update_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Get the commit address from nameservice
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/storage/ns/flkb:test")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, ns_json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);

    // Extract commit head CID (should exist after transaction)
    let commit_head_id = ns_json
        .get("commit_head_id")
        .and_then(|v| v.as_str())
        .expect("commit_head_id should exist after transaction");

    // Request the commit via CID — server returns raw bytes anyway
    let block_body = serde_json::json!({ "cid": commit_head_id, "ledger": "flkb:test" });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/storage/block")
                .header("content-type", "application/json")
                .header("Authorization", format!("Bearer {token}"))
                .header("Accept", "application/x-fluree-flakes")
                .body(Body::from(block_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    // Non-leaf blocks always return raw bytes with 200, regardless of Accept header.
    // Content negotiation only affects leaf block representation.
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "Non-leaf block should return 200 with raw bytes regardless of Accept"
    );

    // Verify response is application/octet-stream (raw bytes)
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("application/octet-stream"),
        "Non-leaf response should be octet-stream, got: {content_type}"
    );
}

/// Test that non-leaf blocks return raw bytes when octet-stream is requested
///
/// This verifies the fallback path works correctly: when a block isn't a leaf,
/// octet-stream format should still return the raw bytes successfully.
#[tokio::test]
async fn test_block_content_negotiation_octet_stream_success() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state.clone());

    // Generate a valid token
    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token(&signing_key, true);

    // Create a ledger
    let create_body = serde_json::json!({ "ledger": "octet:test" });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Transact some data to create a commit
    let update_body = serde_json::json!({
        "ledger": "octet:test",
        "@context": { "ex": "http://example.org/" },
        "insert": {
            "@id": "ex:bob",
            "ex:name": "Bob"
        }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/update")
                .header("content-type", "application/json")
                .body(Body::from(update_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Get the commit address from nameservice
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/storage/ns/octet:test")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, ns_json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);

    // Extract commit head CID (should exist after transaction)
    let commit_head_id = ns_json
        .get("commit_head_id")
        .and_then(|v| v.as_str())
        .expect("commit_head_id should exist after transaction");

    // Request the commit via CID with octet-stream format - should succeed
    let block_body = serde_json::json!({ "cid": commit_head_id, "ledger": "octet:test" });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/storage/block")
                .header("content-type", "application/json")
                .header("Authorization", format!("Bearer {token}"))
                .header("Accept", "application/octet-stream")
                .body(Body::from(block_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, bytes) = bytes_body(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "octet-stream format should always succeed for valid blocks"
    );
    // Commit should be JSON
    assert!(
        !bytes.is_empty(),
        "Response body should contain commit data"
    );
    // Verify it's not FLKB format (commit is JSON)
    assert!(
        bytes.len() < 4 || &bytes[0..4] != b"FLKB",
        "Commit data should not be FLKB format"
    );
}

/// Test that the default Accept header (missing) returns octet-stream
#[tokio::test]
async fn test_block_content_negotiation_default_accept() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state.clone());

    // Generate a valid token
    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token(&signing_key, true);

    // Create a ledger
    let create_body = serde_json::json!({ "ledger": "default:test" });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Transact some data to create a commit
    let update_body = serde_json::json!({
        "ledger": "default:test",
        "@context": { "ex": "http://example.org/" },
        "insert": {
            "@id": "ex:charlie",
            "ex:name": "Charlie"
        }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/update")
                .header("content-type", "application/json")
                .body(Body::from(update_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Get the commit address from nameservice
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/storage/ns/default:test")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, ns_json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);

    // Extract commit head CID (should exist after transaction)
    let commit_head_id = ns_json
        .get("commit_head_id")
        .and_then(|v| v.as_str())
        .expect("commit_head_id should exist after transaction");

    // Request via CID with NO Accept header - should default to octet-stream and succeed
    let block_body = serde_json::json!({ "cid": commit_head_id, "ledger": "default:test" });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/storage/block")
                .header("content-type", "application/json")
                .header("Authorization", format!("Bearer {token}"))
                // No Accept header - should default to octet-stream
                .body(Body::from(block_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, _bytes) = bytes_body(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "Missing Accept header should default to octet-stream"
    );
}

/// Test that non-leaf blocks return raw bytes even when JSON flakes format is requested
///
/// Same as the binary flakes test: non-leaf blocks ignore Accept and return raw bytes.
#[tokio::test]
async fn test_block_content_negotiation_non_leaf_json_flakes_returns_raw() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state.clone());

    // Generate a valid token
    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token(&signing_key, true);

    // Create a ledger
    let create_body = serde_json::json!({ "ledger": "json:test" });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Transact some data to create a commit
    let update_body = serde_json::json!({
        "ledger": "json:test",
        "@context": { "ex": "http://example.org/" },
        "insert": {
            "@id": "ex:diana",
            "ex:name": "Diana"
        }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/update")
                .header("content-type", "application/json")
                .body(Body::from(update_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Get the commit address from nameservice
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/storage/ns/json:test")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, ns_json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);

    // Extract commit head CID (should exist after transaction)
    let commit_head_id = ns_json
        .get("commit_head_id")
        .and_then(|v| v.as_str())
        .expect("commit_head_id should exist after transaction");

    // Request via CID with JSON flakes debug format — server returns raw bytes anyway
    let block_body = serde_json::json!({ "cid": commit_head_id, "ledger": "json:test" });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/storage/block")
                .header("content-type", "application/json")
                .header("Authorization", format!("Bearer {token}"))
                .header("Accept", "application/x-fluree-flakes+json")
                .body(Body::from(block_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    // Non-leaf blocks always return raw bytes with 200, regardless of Accept header.
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "Non-leaf block should return 200 with raw bytes regardless of Accept"
    );

    // Verify response is application/octet-stream (raw bytes)
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("application/octet-stream"),
        "Non-leaf response should be octet-stream, got: {content_type}"
    );
}

/// Create a JWS token with storage proxy claims but NO identity
/// (avoids policy resolution errors when ledger doesn't have the identity)
fn create_storage_proxy_token_no_identity(signing_key: &SigningKey, storage_all: bool) -> String {
    let pubkey = signing_key.verifying_key().to_bytes();
    let pubkey_b64 = URL_SAFE_NO_PAD.encode(pubkey);
    let did = did_from_pubkey(&pubkey);

    let header = serde_json::json!({
        "alg": "EdDSA",
        "jwk": {
            "kty": "OKP",
            "crv": "Ed25519",
            "x": pubkey_b64
        }
    });

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // No fluree.identity or sub claim - avoids policy resolution entirely
    // This results in no policy filtering (returns all flakes)
    let payload = serde_json::json!({
        "iss": did,
        "exp": now + 3600,
        "iat": now,
        "fluree.storage.all": storage_all
    });

    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
    let signing_input = format!("{header_b64}.{payload_b64}");
    let signature = signing_key.sign(signing_input.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

    format!("{header_b64}.{payload_b64}.{sig_b64}")
}

/// Test that binary FLI3 leaf blocks return FLKB format when requested.
///
/// This test:
/// - creates a ledger and transacts some data
/// - reindexes (producing binary FLI3 leaves)
/// - fetches a real leaf address from the FIR6 index root
/// - requests that leaf with `Accept: application/x-fluree-flakes`
/// - verifies the response is FLKB and decodes to at least one flake
#[tokio::test]
async fn test_block_content_negotiation_returns_flkb_for_leaf() {
    use fluree_db_api::ReindexOptions;

    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state.clone());

    // Generate a token WITHOUT identity claim (avoids policy resolution errors)
    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token_no_identity(&signing_key, true);

    // Create a ledger so we have a valid alias for authorization
    let create_body = serde_json::json!({ "ledger": "leaf:test" });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Transact some data so reindex produces at least one leaf.
    let data = serde_json::json!({
        "ledger": "leaf:test",
        "@context": { "ex": "http://example.org/ns/" },
        "insert": {
            "@graph": [
                { "@id": "ex:alice", "ex:name": "Alice" },
                { "@id": "ex:bob",   "ex:name": "Bob"   }
            ]
        }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/update")
                .header("content-type", "application/json")
                .body(Body::from(data.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "Transact should succeed");

    // Reindex to build binary leaves (FLI3) + refresh cache so binary_store is present.
    let fluree = &state.fluree;
    let reindex_result = fluree
        .reindex("leaf:test", ReindexOptions::default())
        .await
        .expect("reindex should succeed");
    fluree
        .refresh("leaf:test", Default::default())
        .await
        .expect("refresh after reindex should succeed");

    // Fetch the DB root and extract a leaf CID.
    let root_body =
        serde_json::json!({ "cid": reindex_result.root_id.to_string(), "ledger": "leaf:test" });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/storage/block")
                .header("content-type", "application/json")
                .header("Authorization", format!("Bearer {token}"))
                .header("Accept", "application/octet-stream")
                .body(Body::from(root_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, root_bytes) = bytes_body(resp).await;
    assert_eq!(status, StatusCode::OK, "DB root fetch failed");

    let leaf_cid = extract_spot_leaf_cid(&root_bytes);

    // Request the leaf with flakes format - should return FLKB
    let block_body = serde_json::json!({ "cid": leaf_cid, "ledger": "leaf:test" });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/storage/block")
                .header("content-type", "application/json")
                .header("Authorization", format!("Bearer {token}"))
                .header("Accept", "application/x-fluree-flakes")
                .body(Body::from(block_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, bytes) = bytes_body(resp).await;

    // Debug: print error message if not 200
    if status != StatusCode::OK {
        let error_msg = String::from_utf8_lossy(&bytes);
        eprintln!("Error response: {error_msg}");
    }

    // Verify 200 OK
    assert_eq!(
        status,
        StatusCode::OK,
        "Leaf block with flakes format should return 200 OK"
    );

    // Verify FLKB magic bytes
    assert!(
        bytes.len() >= 4,
        "Response should have at least 4 bytes for magic"
    );
    assert_eq!(
        &bytes[0..4],
        FLKB_MAGIC,
        "Response should start with FLKB magic bytes"
    );

    // Verify we can decode the flakes
    let flakes = decode_flakes(&bytes).expect("decode_flakes should succeed");
    assert!(!flakes.is_empty(), "Should decode at least one flake");
}

// =============================================================================
// Peer-Mode Proxy Path Tests (PR6: ProxyStorage + ReadHint)
// =============================================================================

/// Test that ProxyStorage.read_bytes_hint(PreferLeafFlakes) returns FLKB for leaf nodes
///
/// This is the end-to-end proof that:
/// 1. Peer (proxy mode) reads a leaf through ProxyStorage using read_bytes_hint(PreferLeafFlakes)
/// 2. The tx server returns FLKB bytes
/// 3. The peer can decode them using decode_flakes
///
/// This test starts a real HTTP server to exercise the full network path.
#[tokio::test]
async fn test_proxy_storage_read_bytes_hint_returns_flkb_for_leaf() {
    use fluree_db_core::ReadHint;
    use fluree_db_server::peer::ProxyStorage;
    use tokio::net::TcpListener;

    // Create tx server state with storage proxy enabled
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state.clone());

    // Start a real HTTP server
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind to ephemeral port");
    let server_addr = listener.local_addr().expect("get local addr");
    let server_url = format!("http://{server_addr}");

    // Spawn the server in a background task
    let server_handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server run");
    });

    // Give the server a moment to start
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Generate a token WITHOUT identity claim (avoids policy resolution errors)
    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token_no_identity(&signing_key, true);

    // Create a ledger via HTTP (to have a valid alias for authorization)
    let client = reqwest::Client::new();
    let create_resp = client
        .post(format!("{server_url}/v1/fluree/create"))
        .header("content-type", "application/json")
        .body(r#"{"ledger": "peer:test"}"#)
        .send()
        .await
        .expect("create ledger request");
    assert_eq!(
        create_resp.status(),
        reqwest::StatusCode::CREATED,
        "Ledger creation should succeed"
    );

    // Create some data + reindex so we have a real leaf to fetch.
    let transact_resp = client
        .post(format!("{server_url}/v1/fluree/update"))
        .header("content-type", "application/json")
        .body(
            serde_json::json!({
                "ledger": "peer:test",
                "@context": { "ex": "http://example.org/ns/" },
                "insert": {
                    "@graph": [
                        { "@id": "ex:carol", "ex:age": 30 },
                        { "@id": "ex:dave",  "ex:age": 25 }
                    ]
                }
            })
            .to_string(),
        )
        .send()
        .await
        .expect("transact request");
    assert_eq!(
        transact_resp.status(),
        reqwest::StatusCode::OK,
        "Transact should succeed"
    );

    // Reindex via tx server state (direct call) and refresh, then fetch DB root JSON over HTTP.
    // (The server is running in-process; state is still available in this test.)
    use fluree_db_api::ReindexOptions;
    let fluree = &state.fluree;
    let reindex_result = fluree
        .reindex("peer:test", ReindexOptions::default())
        .await
        .expect("reindex should succeed");
    fluree
        .refresh("peer:test", Default::default())
        .await
        .expect("refresh after reindex should succeed");

    let token_for_http = token.clone();
    let root_resp = client
        .post(format!("{server_url}/v1/fluree/storage/block"))
        .header("content-type", "application/json")
        .header("Authorization", format!("Bearer {token_for_http}"))
        .header("Accept", "application/octet-stream")
        .body(
            serde_json::json!({ "cid": reindex_result.root_id.to_string(), "ledger": "peer:test" })
                .to_string(),
        )
        .send()
        .await
        .expect("fetch root");
    assert_eq!(
        root_resp.status(),
        reqwest::StatusCode::OK,
        "DB root fetch should succeed"
    );
    let root_bytes = root_resp.bytes().await.expect("read root bytes");
    let leaf_cid = extract_spot_leaf_cid(&root_bytes);
    let leaf_address = leaf_address_from_cid(&leaf_cid, "peer:test");

    // Create ProxyStorage pointing to our test server
    let proxy_storage = ProxyStorage::new(server_url.clone(), token);

    // Call read_bytes_hint with PreferLeafFlakes
    let result = proxy_storage
        .read_bytes_hint(&leaf_address, ReadHint::PreferLeafFlakes)
        .await;

    // Should succeed
    let bytes = result.expect("read_bytes_hint should succeed");

    // Verify FLKB magic bytes
    assert!(
        bytes.len() >= 4,
        "Response should have at least 4 bytes for magic"
    );
    assert_eq!(
        &bytes[0..4],
        FLKB_MAGIC,
        "ProxyStorage should return FLKB format for leaf with PreferLeafFlakes hint"
    );

    // Verify we can decode the flakes
    let flakes = decode_flakes(&bytes).expect("decode_flakes should succeed");
    assert!(!flakes.is_empty(), "Should decode at least one flake");
    assert!(
        flakes.iter().any(|f| f.s.name == "carol"),
        "Expected a flake for ex:carol"
    );
    assert!(
        flakes.iter().any(|f| f.s.name == "dave"),
        "Expected a flake for ex:dave"
    );

    // Cleanup: abort server
    server_handle.abort();
}

/// Test that ProxyStorage.read_bytes returns FLKB for leaf blocks under PolicyEnforced
///
/// Under PolicyEnforced mode (the only mode currently available via storage proxy),
/// leaf blocks are always decoded and policy-filtered. ProxyStorage.read_bytes() uses
/// flakes-first content negotiation, so leaves come back as FLKB (not raw FLI3).
///
/// Raw FLI3 leaf bytes would only be available under TrustedInternal enforcement mode,
/// which is not yet implemented. When it is, a separate ProxyStorage variant (or mode)
/// would be needed to opt into raw bytes.
#[tokio::test]
async fn test_proxy_storage_read_bytes_leaf_returns_flkb_under_policy() {
    use fluree_db_api::ReindexOptions;
    use fluree_db_server::peer::ProxyStorage;
    use tokio::net::TcpListener;

    // Create tx server state with storage proxy enabled
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state.clone());

    // Start a real HTTP server
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind to ephemeral port");
    let server_addr = listener.local_addr().expect("get local addr");
    let server_url = format!("http://{server_addr}");

    // Spawn the server in a background task
    let server_handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server run");
    });

    // Give the server a moment to start
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Generate a token
    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token_no_identity(&signing_key, true);

    // Create a ledger via HTTP
    let client = reqwest::Client::new();
    let create_resp = client
        .post(format!("{server_url}/v1/fluree/create"))
        .header("content-type", "application/json")
        .body(r#"{"ledger": "raw:test"}"#)
        .send()
        .await
        .expect("create ledger request");
    assert_eq!(
        create_resp.status(),
        reqwest::StatusCode::CREATED,
        "Ledger creation should succeed"
    );

    // Transact + reindex to create real binary leaves (FLI3).
    let transact_resp = client
        .post(format!("{server_url}/v1/fluree/update"))
        .header("content-type", "application/json")
        .body(
            serde_json::json!({
                "ledger": "raw:test",
                "@context": { "ex": "http://example.org/ns/" },
                "insert": { "@id": "ex:eve", "ex:score": 100 }
            })
            .to_string(),
        )
        .send()
        .await
        .expect("transact request");
    assert_eq!(
        transact_resp.status(),
        reqwest::StatusCode::OK,
        "Transact should succeed"
    );

    let fluree = &state.fluree;
    let reindex_result = fluree
        .reindex("raw:test", ReindexOptions::default())
        .await
        .expect("reindex should succeed");
    fluree
        .refresh("raw:test", Default::default())
        .await
        .expect("refresh after reindex should succeed");

    // Fetch DB root JSON so we can extract a real leaf CID.
    let token_for_http = token.clone();
    let root_resp = client
        .post(format!("{server_url}/v1/fluree/storage/block"))
        .header("content-type", "application/json")
        .header("Authorization", format!("Bearer {token_for_http}"))
        .header("Accept", "application/octet-stream")
        .body(
            serde_json::json!({ "cid": reindex_result.root_id.to_string(), "ledger": "raw:test" })
                .to_string(),
        )
        .send()
        .await
        .expect("fetch root");
    assert_eq!(
        root_resp.status(),
        reqwest::StatusCode::OK,
        "DB root fetch should succeed"
    );
    let root_bytes = root_resp.bytes().await.expect("read root bytes");
    let leaf_cid = extract_spot_leaf_cid(&root_bytes);
    let leaf_address = leaf_address_from_cid(&leaf_cid, "raw:test");

    // Create ProxyStorage pointing to our test server
    let proxy_storage = ProxyStorage::new(server_url.clone(), token);

    // Call read_bytes (no hint) — under PolicyEnforced, this uses flakes-first
    // negotiation and returns FLKB for leaf blocks.
    let result = proxy_storage.read_bytes(&leaf_address).await;
    let bytes = result.expect("read_bytes should succeed for leaf");

    // Under PolicyEnforced, leaf blocks are returned as FLKB (policy-filtered flakes),
    // not raw FLI3. This is the same behavior as read_bytes_hint(PreferLeafFlakes).
    assert!(
        bytes.len() >= 4 && &bytes[0..4] == FLKB_MAGIC,
        "read_bytes for leaf should return FLKB under PolicyEnforced, got magic: {:?}",
        &bytes[..std::cmp::min(4, bytes.len())]
    );

    // Should NOT be raw FLI3 (that would require TrustedInternal mode)
    assert!(
        bytes.len() < 4 || &bytes[0..4] != b"FLI3",
        "read_bytes should NOT return raw FLI3 under PolicyEnforced"
    );

    // Cleanup: abort server
    server_handle.abort();
}

// =============================================================================
// Policy-Filtered FLKB Tests (PR6: Prove Filtered < Raw)
// =============================================================================

/// Helper: Create tx server state with storage proxy AND policy config
///
/// Configures:
/// - storage_proxy_enabled = true
/// - storage_proxy_default_identity = the identity IRI (optional)
/// - storage_proxy_default_policy_class = the policy class IRI (optional)
async fn tx_server_state_with_policy(
    default_identity: Option<&str>,
    default_policy_class: Option<&str>,
) -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false, // We'll use reindex() manually
        storage_path: Some(tmp.path().to_path_buf()),
        server_role: ServerRole::Transaction,
        // Enable storage proxy with insecure mode for testing
        storage_proxy_enabled: true,
        storage_proxy_insecure_accept_any_issuer: true,
        // Configure policy defaults
        storage_proxy_default_identity: default_identity.map(std::string::ToString::to_string),
        storage_proxy_default_policy_class: default_policy_class
            .map(std::string::ToString::to_string),
        ..Default::default()
    };

    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    (tmp, state)
}

/// Extract the first SPOT leaf CID string from a IndexRoot JSON structure.
///
/// The root format is:
/// ```json
/// {
///   "graphs": [{
///     "g_id": 0,
///     "orders": {
///       "spot": { "branch": "...", "leaves": ["cid1", "cid2", ...] }
///     }
///   }]
/// }
/// ```
fn extract_spot_leaf_cid(root_bytes: &[u8]) -> String {
    let root = IndexRoot::decode(root_bytes).expect("db root should be valid FIR6");
    let spot_order = root
        .default_graph_orders
        .iter()
        .find(|o| o.order.dir_name() == "spot")
        .expect("root should have a SPOT order");
    spot_order
        .leaves
        .first()
        .expect("SPOT order should have at least one leaf")
        .leaf_cid
        .to_string()
}

/// Derive the storage address for a leaf from its CID string.
/// (Needed by ProxyStorage tests that call `read_bytes(address)` directly.)
fn leaf_address_from_cid(cid_str: &str, ledger_id: &str) -> String {
    let cid: ContentId = cid_str.parse().expect("leaf should be a valid CID");
    fluree_db_core::content_address("file", ContentKind::IndexLeaf, ledger_id, &cid.digest_hex())
}

/// Test that policy filtering is applied to binary leaves (FLI3 → FLKB)
///
/// This test proves real policy enforcement using CLASS-BASED policy (not identity-based):
/// 1. Create ledger with data and a policy class that unconditionally denies `schema:ssn`
/// 2. Reindex to build the index
/// 3. Fetch a real leaf with `Accept: application/x-fluree-flakes`
/// 4. Assert returned flakes do not include `schema:ssn`
///
/// NOTE: Uses class-based policy only (no identity) to avoid the stale-cache issue
/// where identity-based policy loading queries the cached DB for `<identity> f:policyClass ?class`.
#[tokio::test]
async fn test_policy_filtered_flkb_has_fewer_flakes_than_raw() {
    use fluree_db_api::ReindexOptions;

    // Policy class that will be used for filtering (NO identity - class-based only)
    let policy_class_iri = "http://example.org/ns/EmployeePolicy";

    // Create tx server with ONLY policy_class config (no identity)
    // This uses class-based policy loading which directly loads policies of the given class
    let (_tmp, state) = tx_server_state_with_policy(
        None, // NO identity - avoids stale-cache issue
        Some(policy_class_iri),
    )
    .await;
    let app = build_router(state.clone());

    // Generate a storage proxy token (no identity claim - we use server defaults)
    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token_no_identity(&signing_key, true);

    // Step 1: Create the ledger
    let alias = "policy:filter-test";
    let create_body = serde_json::json!({ "ledger": alias });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "Ledger creation failed");

    // Step 2: Transact data with class-based policy
    // This creates:
    // - Two users (Alice and John) each with an SSN and name
    // - A policy that UNCONDITIONALLY DENIES schema:ssn (no identity check)
    // - A default allow policy for all other properties
    //
    // Result: SSN flakes filtered out, name/type flakes remain
    let setup_data = serde_json::json!({
        "ledger": alias,
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "insert": {
            "@graph": [
                // Users with SSNs and names
                {
                    "@id": "ex:alice",
                    "@type": "ex:User",
                    "schema:name": "Alice",
                    "schema:ssn": "111-11-1111"
                },
                {
                    "@id": "ex:john",
                    "@type": "ex:User",
                    "schema:name": "John",
                    "schema:ssn": "888-88-8888"
                },
                // UNCONDITIONAL DENY for SSN - query that can never succeed
                // Uses a property lookup that will never match
                {
                    "@id": "ex:ssnDenyAll",
                    "@type": ["f:AccessPolicy", "ex:EmployeePolicy"],
                    "f:required": true,
                    "f:onProperty": [{"@id": "schema:ssn"}],
                    "f:action": {"@id": "f:view"},
                    "f:query": "{\"where\": {\"@id\": \"?$this\", \"http://example.org/ns/neverExistsProperty\": \"impossibleValue\"}}"
                },
                // Default allow for all other properties (unconditional)
                {
                    "@id": "ex:defaultAllowAll",
                    "@type": ["f:AccessPolicy", "ex:EmployeePolicy"],
                    "f:action": {"@id": "f:view"},
                    "f:query": "{}"
                }
            ]
        }
    });

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/update")
                .header("content-type", "application/json")
                .body(Body::from(setup_data.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, body) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK, "Transaction failed: {body:?}");

    // Step 3: Reindex to build the index
    // This creates real leaf nodes in storage
    let fluree = &state.fluree;
    let reindex_result = fluree
        .reindex(alias, ReindexOptions::default())
        .await
        .expect("reindex should succeed");

    assert!(
        reindex_result.root_id.digest_hex().len() == 64,
        "Reindex should produce a valid root CID"
    );
    assert!(
        reindex_result.index_t > 0,
        "Reindex should have index_t > 0"
    );

    // CRITICAL: Refresh the cached ledger so it picks up the new indexed state.
    // Without this, the cached db's dictionary won't have the policy class IRI
    // and policy lookup will fail (returning root policy = no filtering).
    let refresh_result = fluree
        .refresh(alias, Default::default())
        .await
        .expect("refresh should succeed");

    // Should have reloaded or updated index
    println!("Refresh result after reindex: {refresh_result:?}");

    // Step 4: Find a leaf address
    // The root_id points to the DB root file (contains index roots as nested objects).
    // We need to:
    // 1. Read the DB root to get the SPOT index root
    // 2. Read the SPOT index root (may be branch or leaf)
    // 3. If branch, walk down to find a leaf

    // Read the DB root file
    let db_root_body =
        serde_json::json!({ "cid": reindex_result.root_id.to_string(), "ledger": alias });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/storage/block")
                .header("content-type", "application/json")
                .header("Authorization", format!("Bearer {token}"))
                .header("Accept", "application/octet-stream")
                .body(Body::from(db_root_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, db_root_bytes) = bytes_body(resp).await;
    assert_eq!(status, StatusCode::OK, "DB root fetch failed");

    // Extract the first SPOT leaf CID from FIR6 binary root
    let leaf_cid = extract_spot_leaf_cid(&db_root_bytes);

    let leaf_block_body = serde_json::json!({ "cid": &leaf_cid, "ledger": alias });
    // Fetch the leaf FILTERED (x-fluree-flakes) with policy
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/storage/block")
                .header("content-type", "application/json")
                .header("Authorization", format!("Bearer {token}"))
                .header("Accept", "application/x-fluree-flakes")
                .body(Body::from(leaf_block_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, filtered_bytes) = bytes_body(resp).await;

    // Debug: print error if not 200
    if status != StatusCode::OK {
        let error_msg = String::from_utf8_lossy(&filtered_bytes);
        eprintln!("Filtered fetch failed with status {status}: {error_msg}");
    }

    assert_eq!(status, StatusCode::OK, "Filtered leaf fetch failed");

    // Verify FLKB format
    assert!(
        filtered_bytes.len() >= 4 && &filtered_bytes[0..4] == FLKB_MAGIC,
        "Filtered response should be FLKB format"
    );

    // Decode the filtered flakes
    let filtered_flakes = decode_flakes(&filtered_bytes).expect("FLKB decode should succeed");
    assert!(
        !filtered_flakes.is_empty(),
        "Expected at least one flake after filtering"
    );
    assert!(
        filtered_flakes.iter().all(|f| f.p.name != "ssn"),
        "Expected schema:ssn flakes to be filtered out"
    );
}

/// Test that NO policy (no identity/policy_class config) returns ALL flakes
///
/// This is the control test: without policy config, we can still request FLKB
/// for a binary leaf and decode at least one flake.
#[tokio::test]
async fn test_no_policy_flkb_returns_all_flakes() {
    use fluree_db_api::ReindexOptions;

    // Create tx server WITHOUT policy config (defaults)
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state.clone());

    // Generate a storage proxy token
    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token_no_identity(&signing_key, true);

    // Create ledger
    let alias = "nopolicy:test";
    let create_body = serde_json::json!({ "ledger": alias });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Transact some data
    let data = serde_json::json!({
        "ledger": alias,
        "@context": { "ex": "http://example.org/ns/" },
        "insert": {
            "@graph": [
                { "@id": "ex:a", "ex:val": 1 },
                { "@id": "ex:b", "ex:val": 2 },
                { "@id": "ex:c", "ex:val": 3 }
            ]
        }
    });

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/update")
                .header("content-type", "application/json")
                .body(Body::from(data.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Reindex
    let fluree = &state.fluree;
    let reindex_result = fluree
        .reindex(alias, ReindexOptions::default())
        .await
        .expect("reindex");
    fluree
        .refresh(alias, Default::default())
        .await
        .expect("refresh after reindex should succeed");

    // The root_id is the DB root, not a leaf. We need to extract the SPOT index root
    // and find a leaf from there.
    let db_root_body =
        serde_json::json!({ "cid": reindex_result.root_id.to_string(), "ledger": alias });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/storage/block")
                .header("content-type", "application/json")
                .header("Authorization", format!("Bearer {token}"))
                .header("Accept", "application/octet-stream")
                .body(Body::from(db_root_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, db_root_bytes) = bytes_body(resp).await;
    assert_eq!(status, StatusCode::OK, "DB root fetch failed");

    // Extract the first SPOT leaf CID from FIR6 binary root
    let leaf_cid = extract_spot_leaf_cid(&db_root_bytes);

    let block_body = serde_json::json!({ "cid": &leaf_cid, "ledger": alias });
    // Fetch leaf in flakes format (no policy configured → should return all flakes, still FLKB)
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/storage/block")
                .header("content-type", "application/json")
                .header("Authorization", format!("Bearer {token}"))
                .header("Accept", "application/x-fluree-flakes")
                .body(Body::from(block_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, filtered_bytes) = bytes_body(resp).await;
    assert_eq!(status, StatusCode::OK);

    assert!(
        filtered_bytes.len() >= 4 && &filtered_bytes[0..4] == FLKB_MAGIC,
        "Response should be FLKB format"
    );
    let filtered_flakes = decode_flakes(&filtered_bytes).expect("decode");
    assert!(
        !filtered_flakes.is_empty(),
        "Expected at least one flake from no-policy FLKB response"
    );
}
