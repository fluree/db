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
use fluree_db_core::{ContentAddressedWrite, ContentId, ContentKind, StorageRead};
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
    use fluree_db_server::peer::{ProxyReadMode, ProxyStorage};
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
    let proxy_storage = ProxyStorage::new(server_url.clone(), token, ProxyReadMode::Filtered);

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

/// Test that ProxyStorage.read_bytes returns FLKB for leaf blocks in Filtered mode
///
/// In `ProxyReadMode::Filtered`, leaf blocks fetched via `/storage/block` are
/// always decoded and policy-filtered. ProxyStorage.read_bytes() uses
/// flakes-first content negotiation, so leaves come back as FLKB (not raw FLI3).
///
/// Raw FLI3 leaf bytes are available via `ProxyReadMode::Raw`, which fetches
/// canonical CAS bytes through `/storage/objects/{cid}` instead.
#[tokio::test]
async fn test_proxy_storage_read_bytes_leaf_returns_flkb_under_policy() {
    use fluree_db_api::ReindexOptions;
    use fluree_db_server::peer::{ProxyReadMode, ProxyStorage};
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
    let proxy_storage = ProxyStorage::new(server_url.clone(), token, ProxyReadMode::Filtered);

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

/// Test that ProxyStorage in Raw mode returns canonical, CID-verified bytes
/// for both leaf and non-leaf blocks.
///
/// Raw mode fetches through `GET /storage/objects/{cid}` (full-access tier):
/// - leaf bytes must be byte-identical to what's in the origin's storage
///   (raw FLI3, NOT the FLKB transport format), so the binary index reader
///   can consume them directly
/// - non-leaf bytes (index root) must round-trip identically too
/// - an out-of-scope token must observe NotFound (no existence leak)
#[tokio::test]
async fn test_proxy_storage_raw_mode_returns_canonical_bytes() {
    use fluree_db_api::ReindexOptions;
    use fluree_db_server::peer::{ProxyReadMode, ProxyStorage};
    use tokio::net::TcpListener;

    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state.clone());

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind to ephemeral port");
    let server_addr = listener.local_addr().expect("get local addr");
    let server_url = format!("http://{server_addr}");
    let server_handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server run");
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token_no_identity(&signing_key, true);

    let client = reqwest::Client::new();
    let create_resp = client
        .post(format!("{server_url}/v1/fluree/create"))
        .header("content-type", "application/json")
        .body(r#"{"ledger": "rawmode:test"}"#)
        .send()
        .await
        .expect("create ledger request");
    assert_eq!(create_resp.status(), reqwest::StatusCode::CREATED);

    let transact_resp = client
        .post(format!("{server_url}/v1/fluree/update"))
        .header("content-type", "application/json")
        .body(
            serde_json::json!({
                "ledger": "rawmode:test",
                "@context": { "ex": "http://example.org/ns/" },
                "insert": {
                    "@graph": [
                        { "@id": "ex:frank", "ex:name": "Frank" },
                        { "@id": "ex:grace", "ex:name": "Grace" }
                    ]
                }
            })
            .to_string(),
        )
        .send()
        .await
        .expect("transact request");
    assert_eq!(transact_resp.status(), reqwest::StatusCode::OK);

    let fluree = &state.fluree;
    let reindex_result = fluree
        .reindex("rawmode:test", ReindexOptions::default())
        .await
        .expect("reindex should succeed");
    fluree
        .refresh("rawmode:test", Default::default())
        .await
        .expect("refresh after reindex should succeed");

    // Resolve a real leaf CID from the index root via direct storage access.
    let admin_storage = state
        .fluree
        .backend()
        .admin_storage_cloned()
        .expect("test backend has managed storage");
    let root_address = fluree_db_core::content_address(
        "file",
        ContentKind::IndexRoot,
        "rawmode:test",
        &reindex_result.root_id.digest_hex(),
    );
    let direct_root_bytes = admin_storage
        .read_bytes(&root_address)
        .await
        .expect("direct root read");
    let leaf_cid = extract_spot_leaf_cid(&direct_root_bytes);
    let leaf_address = leaf_address_from_cid(&leaf_cid, "rawmode:test");
    let direct_leaf_bytes = admin_storage
        .read_bytes(&leaf_address)
        .await
        .expect("direct leaf read");

    let proxy_storage = ProxyStorage::new(server_url.clone(), token, ProxyReadMode::Raw);

    // Leaf: canonical FLI3 bytes, identical to origin storage, not FLKB.
    let leaf_bytes = proxy_storage
        .read_bytes(&leaf_address)
        .await
        .expect("raw leaf read should succeed");
    assert_eq!(
        leaf_bytes, direct_leaf_bytes,
        "raw-mode leaf bytes must be byte-identical to origin storage"
    );
    assert!(
        leaf_bytes.len() < 4 || &leaf_bytes[0..4] != FLKB_MAGIC,
        "raw-mode leaf must not be FLKB transport format"
    );

    // Non-leaf (index root): identical round-trip through the objects endpoint.
    let root_bytes = proxy_storage
        .read_bytes(&root_address)
        .await
        .expect("raw root read should succeed");
    assert_eq!(
        root_bytes, direct_root_bytes,
        "raw-mode root bytes must be byte-identical to origin storage"
    );

    // Ranged reads: raw mode issues true HTTP Range requests (206) and the
    // slice must match the equivalent slice of the direct bytes.
    assert!(proxy_storage.supports_ranged_reads());
    let mid = (direct_leaf_bytes.len() / 2) as u64;
    let ranged = proxy_storage
        .read_byte_range(&leaf_address, 8..mid)
        .await
        .expect("ranged leaf read should succeed");
    assert_eq!(
        ranged,
        direct_leaf_bytes[8..mid as usize].to_vec(),
        "ranged bytes must match the direct slice"
    );
    // Range extending past the object is clamped.
    let tail = proxy_storage
        .read_byte_range(&leaf_address, mid..u64::MAX)
        .await
        .expect("tail range read should succeed");
    assert_eq!(tail, direct_leaf_bytes[mid as usize..].to_vec());
    // Range starting past the object length is empty (416 → empty slice).
    let beyond = proxy_storage
        .read_byte_range(
            &leaf_address,
            (direct_leaf_bytes.len() as u64 + 10)..u64::MAX,
        )
        .await
        .expect("out-of-range read should succeed as empty");
    assert!(beyond.is_empty());

    // Out-of-scope token: scoped to a different ledger → NotFound (no
    // existence leak). A token with no storage claims at all is rejected
    // earlier by the extractor with 401, so scope-to-another-ledger is the
    // case that exercises the per-ledger check.
    let pubkey = signing_key.verifying_key().to_bytes();
    let pubkey_b64 = URL_SAFE_NO_PAD.encode(pubkey);
    let header = serde_json::json!({
        "alg": "EdDSA",
        "jwk": { "kty": "OKP", "crv": "Ed25519", "x": pubkey_b64 }
    });
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let payload = serde_json::json!({
        "iss": did_from_pubkey(&pubkey),
        "exp": now + 3600,
        "iat": now,
        "fluree.storage.all": false,
        "fluree.storage.ledgers": ["other:ledger"]  // NOT rawmode:test
    });
    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
    let signing_input = format!("{header_b64}.{payload_b64}");
    let signature = signing_key.sign(signing_input.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());
    let scoped_token = format!("{header_b64}.{payload_b64}.{sig_b64}");

    let unauthorized_storage =
        ProxyStorage::new(server_url.clone(), scoped_token, ProxyReadMode::Raw);
    let err = unauthorized_storage
        .read_bytes(&leaf_address)
        .await
        .expect_err("out-of-scope read must fail");
    assert!(
        matches!(err, fluree_db_core::Error::NotFound(_)),
        "out-of-scope raw read should surface NotFound, got: {err:?}"
    );

    server_handle.abort();
}

/// End-to-end peer test: a proxy-mode `Fluree` — memory storage backed by
/// `ProxyStorage` in Raw mode plus `ProxyNameService`, the exact wiring of
/// `build_proxy_fluree` — executes a query against an INDEXED ledger served
/// by the tx server over HTTP.
///
/// This is the capability raw mode unlocks: the peer's binary index reader
/// consumes canonical FLI3 leaves/dicts fetched through `/storage/objects`.
/// (The filtered FLKB tier has no leaf decoder on the read path, so indexed
/// ledgers were previously unreadable through the proxy.)
///
/// Multi-thread runtime is required: lazy dict materialization bridges
/// sync→async (block_on) during query execution, which would starve the
/// in-process axum server on a current-thread runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_peer_proxy_fluree_queries_indexed_ledger() {
    use fluree_db_api::{FlureeBuilder, NameServiceMode, ReindexOptions};
    use fluree_db_server::peer::{ProxyNameService, ProxyReadMode, ProxyStorage};
    use tokio::net::TcpListener;

    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state.clone());

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind to ephemeral port");
    let server_addr = listener.local_addr().expect("get local addr");
    let server_url = format!("http://{server_addr}");
    let server_handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server run");
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token_no_identity(&signing_key, true);

    // Create + populate + index a ledger on the tx server.
    let client = reqwest::Client::new();
    let create_resp = client
        .post(format!("{server_url}/v1/fluree/create"))
        .header("content-type", "application/json")
        .body(r#"{"ledger": "peerquery:test"}"#)
        .send()
        .await
        .expect("create ledger request");
    assert_eq!(create_resp.status(), reqwest::StatusCode::CREATED);

    let transact_resp = client
        .post(format!("{server_url}/v1/fluree/update"))
        .header("content-type", "application/json")
        .body(
            serde_json::json!({
                "ledger": "peerquery:test",
                "@context": { "ex": "http://example.org/ns/" },
                "insert": {
                    "@graph": [
                        { "@id": "ex:hana", "ex:name": "Hana" },
                        { "@id": "ex:ivan", "ex:name": "Ivan" }
                    ]
                }
            })
            .to_string(),
        )
        .send()
        .await
        .expect("transact request");
    assert_eq!(transact_resp.status(), reqwest::StatusCode::OK);

    state
        .fluree
        .reindex("peerquery:test", ReindexOptions::default())
        .await
        .expect("reindex should succeed");
    state
        .fluree
        .refresh("peerquery:test", Default::default())
        .await
        .expect("refresh after reindex should succeed");

    // Build a peer exactly like build_proxy_fluree does.
    let peer_storage = ProxyStorage::new(server_url.clone(), token.clone(), ProxyReadMode::Raw);
    let peer_ns = ProxyNameService::new(server_url.clone(), token);
    let peer_fluree = FlureeBuilder::memory()
        .build_with(peer_storage, NameServiceMode::ReadOnly(Arc::new(peer_ns)));

    // Query through the peer — all index reads go over HTTP.
    let query = serde_json::json!({
        "@context": { "ex": "http://example.org/ns/" },
        "select": { "?s": ["ex:name"] },
        "where": { "@id": "?s", "ex:name": "?name" }
    });
    let result = peer_fluree
        .graph("peerquery:test")
        .query()
        .jsonld(&query)
        .execute_formatted()
        .await
        .expect("peer query should succeed");

    let rows = result.as_array().expect("query result should be an array");
    assert_eq!(
        rows.len(),
        2,
        "peer query should see both subjects: {result}"
    );
    let names: Vec<&str> = rows
        .iter()
        .filter_map(|r| r.get("ex:name").and_then(|v| v.as_str()))
        .collect();
    assert!(
        names.contains(&"Hana") && names.contains(&"Ivan"),
        "peer query should return indexed values, got: {result}"
    );

    server_handle.abort();
}

/// Remote mounts: a local Fluree (own ledgers, read-write) mounts a remote
/// Fluree's ledgers read-only under an alias prefix.
///
/// Exercises the full composition:
/// - `CompositeNameService` routes `acme/…` lookups to the remote's
///   `ProxyNameService` and localizes records
/// - `StorageBackend::Routed` sends `acme/…` CAS reads through the mount's
///   `ProxyStorage` (raw, CID-verified, prefix-stripped)
/// - a mounted indexed ledger is queryable next to a local ledger, including
///   in one mixed dataset query
/// - writes to mounted aliases are rejected with a clear error
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_remote_mount_queries_and_write_rejection() {
    use fluree_db_api::{FlureeBuilder, ReindexOptions, RemoteMountSpec};
    use fluree_db_server::peer::{ProxyNameService, ProxyReadMode, ProxyStorage};
    use tokio::net::TcpListener;

    // --- Remote origin: tx server with an indexed ledger ---
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind to ephemeral port");
    let server_addr = listener.local_addr().expect("get local addr");
    let server_url = format!("http://{server_addr}");
    let server_handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server run");
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token_no_identity(&signing_key, true);

    let client = reqwest::Client::new();
    let create_resp = client
        .post(format!("{server_url}/v1/fluree/create"))
        .header("content-type", "application/json")
        .body(r#"{"ledger": "inventory"}"#)
        .send()
        .await
        .expect("create ledger request");
    assert_eq!(create_resp.status(), reqwest::StatusCode::CREATED);
    let transact_resp = client
        .post(format!("{server_url}/v1/fluree/update"))
        .header("content-type", "application/json")
        .body(
            serde_json::json!({
                "ledger": "inventory",
                "@context": { "ex": "http://example.org/ns/" },
                "insert": { "@id": "ex:widget", "ex:name": "Widget", "ex:sku": "W-1" }
            })
            .to_string(),
        )
        .send()
        .await
        .expect("transact request");
    assert_eq!(transact_resp.status(), reqwest::StatusCode::OK);
    state
        .fluree
        .reindex("inventory:main", ReindexOptions::default())
        .await
        .expect("reindex should succeed");
    state
        .fluree
        .refresh("inventory:main", Default::default())
        .await
        .expect("refresh should succeed");

    // --- Local Fluree with the remote mounted under "acme" ---
    let mount_storage = ProxyStorage::new(server_url.clone(), token.clone(), ProxyReadMode::Raw)
        .with_local_prefix("acme");
    let mount_ns = Arc::new(ProxyNameService::new(server_url.clone(), token));
    let local = FlureeBuilder::memory()
        .with_remote_mount(RemoteMountSpec::new("acme", mount_ns, mount_storage))
        .build_memory();

    // Local ledger with its own data.
    local
        .create_ledger("books")
        .await
        .expect("create local ledger");
    local
        .graph("books:main")
        .transact()
        .insert(&serde_json::json!({
            "@context": { "ex": "http://example.org/ns/" },
            "@id": "ex:moby", "ex:name": "Moby Dick"
        }))
        .commit()
        .await
        .expect("local transact");

    // Query the mounted (remote, indexed) ledger by its local alias.
    let query = serde_json::json!({
        "@context": { "ex": "http://example.org/ns/" },
        "select": { "?s": ["ex:name"] },
        "where": { "@id": "?s", "ex:sku": "W-1" }
    });
    let result = local
        .graph("acme/inventory:main")
        .query()
        .jsonld(&query)
        .execute_formatted()
        .await
        .expect("mounted query should succeed");
    let rows = result.as_array().expect("array result");
    assert_eq!(rows.len(), 1, "mounted query result: {result}");
    assert_eq!(
        rows[0].get("ex:name").and_then(|v| v.as_str()),
        Some("Widget"),
        "mounted query should read remote index content: {result}"
    );

    // Mixed dataset: one query over the local ledger AND the mount.
    let mixed = serde_json::json!({
        "@context": { "ex": "http://example.org/ns/" },
        "from": ["books:main", "acme/inventory:main"],
        "select": ["?name"],
        "where": { "@id": "?s", "ex:name": "?name" }
    });
    let result = local
        .query_from()
        .jsonld(&mixed)
        .execute_formatted()
        .await
        .expect("mixed dataset query should succeed");
    // Rows are single-var tuples: [["Moby Dick"], ["Widget"]]
    let names: Vec<&str> = result
        .as_array()
        .expect("array result")
        .iter()
        .filter_map(|row| row.as_array()?.first()?.as_str())
        .collect();
    assert!(
        names.contains(&"Widget") && names.contains(&"Moby Dick"),
        "mixed dataset should span local + mounted ledgers, got: {result}"
    );

    // Writes to mounted aliases are rejected. The routed storage refuses
    // first (ProxyStorage is read-only); the CompositeNameService guard
    // backstops non-storage writes (create/branch/publish).
    let err = local
        .graph("acme/inventory:main")
        .transact()
        .insert(&serde_json::json!({
            "@context": { "ex": "http://example.org/ns/" },
            "@id": "ex:hack", "ex:name": "Nope"
        }))
        .commit()
        .await
        .expect_err("write to mount must fail");
    assert!(
        err.to_string().contains("read-only"),
        "unexpected write-rejection error: {err}"
    );
    // Creating a local ledger that would shadow the mount is also rejected,
    // at the nameservice layer.
    let err = local
        .create_ledger("acme/other")
        .await
        .expect_err("create under mount prefix must fail");
    assert!(
        err.to_string().contains("read-only remote mount"),
        "unexpected create-rejection error: {err}"
    );

    server_handle.abort();
}

/// Test per-ledger serving posture (`f:servingDefaults`) enforcement and
/// advertisement.
///
/// - `f:serveBlocks false` → block/objects/commits endpoints return 404,
///   queries still served, NS record advertises `["query"]`
/// - `f:serveQuery false` (blocks restored) → query route returns 403 with a
///   stable message, blocks served again, NS record advertises `["blocks"]`
///
/// Config changes are read novelty-inclusive, so no reindex is needed after
/// transacting the config graph.
#[tokio::test]
async fn test_serving_defaults_gate_and_advertisement() {
    use fluree_db_api::ReindexOptions;

    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state.clone());

    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token_no_identity(&signing_key, true);

    // Create + populate + index.
    let create_body = serde_json::json!({ "ledger": "serving:test" });
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

    let data = serde_json::json!({
        "ledger": "serving:test",
        "@context": { "ex": "http://example.org/ns/" },
        "insert": { "@id": "ex:judy", "ex:name": "Judy" }
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

    let reindex_result = state
        .fluree
        .reindex("serving:test", ReindexOptions::default())
        .await
        .expect("reindex should succeed");
    state
        .fluree
        .refresh("serving:test", Default::default())
        .await
        .expect("refresh should succeed");
    let root_cid = reindex_result.root_id.to_string();

    // Small helpers over the router.
    let fetch_block_status = |app: axum::Router, token: String, cid: String| async move {
        let body = serde_json::json!({ "cid": cid, "ledger": "serving:test" });
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/fluree/storage/block")
                    .header("content-type", "application/json")
                    .header("Authorization", format!("Bearer {token}"))
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        resp.status()
    };
    let query_status = |app: axum::Router| async move {
        let body = serde_json::json!({
            "@context": { "ex": "http://example.org/ns/" },
            "from": "serving:test",
            "select": ["?name"],
            "where": { "@id": "?s", "ex:name": "?name" }
        });
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/fluree/query")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        resp.status()
    };
    let ns_serving = |app: axum::Router, token: String| async move {
        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/fluree/storage/ns/serving:test")
                    .header("Authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, json) = json_body(resp).await;
        assert_eq!(status, StatusCode::OK, "NS record fetch failed: {json}");
        json.get("serving").cloned()
    };

    // Baseline: unconfigured ledger serves both tiers.
    assert_eq!(
        fetch_block_status(app.clone(), token.clone(), root_cid.clone()).await,
        StatusCode::OK
    );
    assert_eq!(query_status(app.clone()).await, StatusCode::OK);
    assert_eq!(
        ns_serving(app.clone(), token.clone()).await,
        Some(serde_json::json!(["query", "blocks"]))
    );

    // Disable block serving via the config graph.
    let cfg_trig = r"
        @prefix f:   <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <urn:fluree:serving:test#config> {
            <urn:cfg:main>    rdf:type          f:LedgerConfig .
            <urn:cfg:main>    f:servingDefaults <urn:cfg:serving> .
            <urn:cfg:serving> f:serveBlocks     false .
        }
    ";
    state
        .fluree
        .graph("serving:test")
        .transact()
        .upsert_turtle(cfg_trig)
        .commit()
        .await
        .expect("config transact should succeed");

    // Blocks tier refused across all raw-content endpoints; query unaffected.
    assert_eq!(
        fetch_block_status(app.clone(), token.clone(), root_cid.clone()).await,
        StatusCode::NOT_FOUND
    );
    let objects_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/v1/fluree/storage/objects/{root_cid}?ledger=serving:test"
                ))
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(objects_resp.status(), StatusCode::NOT_FOUND);
    let commits_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/commits/serving:test")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(commits_resp.status(), StatusCode::NOT_FOUND);
    assert_eq!(query_status(app.clone()).await, StatusCode::OK);
    assert_eq!(
        ns_serving(app.clone(), token.clone()).await,
        Some(serde_json::json!(["query"]))
    );

    // Flip: blocks on, query off.
    let cfg_trig = r"
        @prefix f: <https://ns.flur.ee/db#> .

        GRAPH <urn:fluree:serving:test#config> {
            <urn:cfg:serving> f:serveBlocks true .
            <urn:cfg:serving> f:serveQuery  false .
        }
    ";
    state
        .fluree
        .graph("serving:test")
        .transact()
        .upsert_turtle(cfg_trig)
        .commit()
        .await
        .expect("config transact should succeed");

    assert_eq!(
        fetch_block_status(app.clone(), token.clone(), root_cid.clone()).await,
        StatusCode::OK
    );
    assert_eq!(query_status(app.clone()).await, StatusCode::FORBIDDEN);
    assert_eq!(
        ns_serving(app.clone(), token.clone()).await,
        Some(serde_json::json!(["blocks"]))
    );
}

/// Vended credentials answer 404 when vending is not configured — the
/// consumer's signal to fall back to proxied block reads. (Positive-path
/// minting is covered by the LocalStack test in fluree-db-api.)
#[cfg(feature = "aws")]
#[tokio::test]
async fn test_vended_credentials_404_when_unconfigured() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state);

    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token_no_identity(&signing_key, true);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/storage/credentials?ledger=any:main")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "file-backed server without vend config must answer 404"
    );
}

/// Discovery advertises the coarse server-level serving capabilities.
#[tokio::test]
async fn test_discovery_advertises_serving_capabilities() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/.well-known/fluree.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json.get("serving"),
        Some(&serde_json::json!({ "query": true, "blocks": true })),
        "discovery should advertise serving capabilities: {json}"
    );
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

// =============================================================================
// Object endpoint: advanced index kinds (#1537)
// =============================================================================

/// The ledger-scoped advanced index kinds — planner statistics, spatial, and
/// history sidecars — must be served through `GET /storage/objects/{cid}`
/// byte-identically, exactly like the core index tree. This exercises the
/// endpoint whose allowlist #1537 widens (the mount E2E tests go through
/// `POST /storage/block`, whose allowlist is intentionally unchanged).
#[tokio::test]
async fn test_object_endpoint_serves_ledger_scoped_advanced_kinds() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state.clone());

    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token(&signing_key, true);

    // A real ledger, so `resolve_block_ledger` finds a nameservice record.
    let create_body = serde_json::json!({ "ledger": "advkinds:test" });
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

    let admin_storage = state
        .fluree
        .backend()
        .admin_storage_cloned()
        .expect("test backend has managed storage");

    for (kind, payload, expected_label) in [
        (
            ContentKind::StatsSketch,
            b"hll sketch bytes".as_slice(),
            "stats-sketch",
        ),
        (
            ContentKind::SpatialIndex,
            b"spatial index bytes".as_slice(),
            "spatial-index",
        ),
        (
            ContentKind::HistorySidecar,
            b"history sidecar bytes".as_slice(),
            "history-sidecar",
        ),
    ] {
        // Seed the artifact exactly where the writer would put it.
        let id = ContentId::new(kind, payload);
        let written = admin_storage
            .content_write_bytes(kind, "advkinds:test", payload)
            .await
            .unwrap_or_else(|e| panic!("seed {kind:?}: {e}"));
        assert_eq!(
            written.content_hash,
            id.digest_hex(),
            "seed hash must match the CID digest for {kind:?}"
        );

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!(
                        "/v1/fluree/storage/objects/{id}?ledger=advkinds:test"
                    ))
                    .header("Authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "{kind:?} must be served through the object endpoint"
        );
        let kind_header = resp
            .headers()
            .get("X-Fluree-Content-Kind")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        assert_eq!(kind_header, expected_label, "kind header for {kind:?}");
        let body = resp
            .into_body()
            .collect()
            .await
            .expect("collect body")
            .to_bytes();
        assert_eq!(
            body.as_ref(),
            payload,
            "{kind:?} bytes must round-trip identically"
        );
    }
}

/// Graph-source artifacts are NOT served through the object endpoint yet:
/// their addresses are keyed by graph_source_id (not a ledger id), the
/// nameservice deliberately skips graph-source records in `lookup()`, and the
/// token-scope semantics for multi-dependency graph sources are undecided —
/// tracked in #1539. Pin the 404 so enabling them is a deliberate act (this
/// test + the allowlist + #1539), not a drive-by.
#[tokio::test]
async fn test_object_endpoint_pins_graph_source_kinds_unserved() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state.clone());

    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token(&signing_key, true);

    // Real ledger, so the 404 provably comes from the kind gate rather than
    // from an unresolvable ledger param.
    let create_body = serde_json::json!({ "ledger": "gshost:test" });
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

    let admin_storage = state
        .fluree
        .backend()
        .admin_storage_cloned()
        .expect("test backend has managed storage");

    // Even with the snapshot bytes PRESENT in storage under its gs-style id
    // and a storage.all token, the endpoint must refuse.
    let payload = b"bm25 snapshot bytes".as_slice();
    let snap_id = ContentId::new(ContentKind::GraphSourceSnapshot, payload);
    admin_storage
        .content_write_bytes(ContentKind::GraphSourceSnapshot, "gsidx:main", payload)
        .await
        .expect("seed graph-source snapshot");

    for ledger_param in ["gsidx:main", "gshost:test"] {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!(
                        "/v1/fluree/storage/objects/{snap_id}?ledger={ledger_param}"
                    ))
                    .header("Authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "GraphSourceSnapshot must 404 (ledger={ledger_param}) until #1539"
        );
    }

    // Mapping kind: same gate, no seed needed — the kind check fires first.
    let map_id = ContentId::new(ContentKind::GraphSourceMapping, b"r2rml mapping");
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/v1/fluree/storage/objects/{map_id}?ledger=gshost:test"
                ))
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "GraphSourceMapping must 404 until #1539"
    );
}

/// The per-ledger SSE subscription form, over HTTP.
///
/// `GET /v1/fluree/events?ledger=<alias>` is what every peer opens — the
/// native peer builds it in `peer/subscription.rs`, the browser peer in
/// `fluree-db-browser`'s `events_url`, and the module docs advertise
/// `?ledger=a&ledger=b`. It nevertheless answered **400** for any value,
/// because `axum::extract::Query` deserializes with `serde_urlencoded`,
/// which cannot build a `Vec` from repeated keys. Every unit test built an
/// `EventsQuery` in process, so nothing caught it; only `?all=true`
/// (a plain bool) ever worked, which is what the peer test configs use.
#[tokio::test]
async fn test_events_accepts_per_ledger_subscription_over_http() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state);

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "ledger": "sse:main" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    // The body is an endless stream; only the response head is asserted (a
    // rejected query string never gets that far).
    for uri in [
        "/v1/fluree/events?ledger=sse:main",
        "/v1/fluree/events?ledger=sse%3Amain",
        "/v1/fluree/events?ledger=sse%3Amain&ledger=other%3Amain",
        "/v1/fluree/events?ledger=sse%3Amain&graph-source=gs%3Amain",
        "/v1/fluree/events?all=true",
        "/v1/fluree/events",
    ] {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(uri)
                    .header("Accept", "text/event-stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "GET {uri}");
        assert!(
            resp.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .starts_with("text/event-stream"),
            "GET {uri} must open an SSE stream"
        );
    }
}
// =============================================================================
// Browser-Readiness Header Tests (CORS, immutable caching, conditional GET)
// =============================================================================

/// Like `tx_server_state`, but with the CORS layer enabled so the
/// browser-facing header behavior is exercised.
async fn tx_server_state_with_cors() -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: true,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        server_role: ServerRole::Transaction,
        storage_proxy_enabled: true,
        storage_proxy_insecure_accept_any_issuer: true,
        ..Default::default()
    };

    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    (tmp, state)
}

fn header_str<'a>(resp: &'a http::Response<Body>, name: &str) -> &'a str {
    resp.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
}

/// CAS objects are immutable, so the object endpoint must serve them with an
/// `ETag` (the CID) and a forever/immutable `Cache-Control` — on both full
/// (200) and ranged (206) responses — and answer a matching `If-None-Match`
/// with 304 without touching storage.
#[tokio::test]
async fn test_object_endpoint_immutable_caching_and_conditional_get() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state.clone());

    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token(&signing_key, true);

    let create_body = serde_json::json!({ "ledger": "cachehdrs:test" });
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

    let admin_storage = state
        .fluree
        .backend()
        .admin_storage_cloned()
        .expect("test backend has managed storage");
    let payload = b"hll sketch bytes".as_slice();
    let id = ContentId::new(ContentKind::StatsSketch, payload);
    admin_storage
        .content_write_bytes(ContentKind::StatsSketch, "cachehdrs:test", payload)
        .await
        .expect("seed artifact");

    let uri = format!("/v1/fluree/storage/objects/{id}?ledger=cachehdrs:test");
    let expected_etag = format!("\"{id}\"");

    // Full 200: ETag + immutable Cache-Control.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&uri)
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(header_str(&resp, "etag"), expected_etag);
    assert_eq!(
        header_str(&resp, "cache-control"),
        "private, max-age=31536000, immutable"
    );
    assert_eq!(
        header_str(&resp, "vary"),
        "Authorization",
        "an immutable, never-revalidated body authorized by a bearer token must \
         key its cache entry on that token, or the next principal on this \
         browser profile is served it without the server seeing their token"
    );

    // Ranged 206: same caching headers ride along.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&uri)
                .header("Authorization", format!("Bearer {token}"))
                .header("Range", "bytes=0-3")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(header_str(&resp, "etag"), expected_etag);
    assert_eq!(
        header_str(&resp, "cache-control"),
        "private, max-age=31536000, immutable"
    );
    assert_eq!(
        header_str(&resp, "vary"),
        "Authorization",
        "an immutable, never-revalidated body authorized by a bearer token must \
         key its cache entry on that token, or the next principal on this \
         browser profile is served it without the server seeing their token"
    );
    assert_eq!(header_str(&resp, "content-range"), "bytes 0-3/16");

    // Conditional revalidation: matching If-None-Match → 304, empty body,
    // caching headers still present.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&uri)
                .header("Authorization", format!("Bearer {token}"))
                .header("If-None-Match", &expected_etag)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(header_str(&resp, "etag"), expected_etag);
    assert_eq!(
        header_str(&resp, "vary"),
        "Authorization",
        "the 304 refreshes the cached entry, so it must carry the same cache key"
    );
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert!(body.is_empty(), "304 must carry no body");

    // Weak-comparison and list forms also revalidate; `*` deliberately does
    // not (the object route answers before consulting storage).
    for candidate in [
        format!("W/{expected_etag}"),
        format!("\"other\", {expected_etag}"),
    ] {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(&uri)
                    .header("Authorization", format!("Bearer {token}"))
                    .header("If-None-Match", &candidate)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::NOT_MODIFIED,
            "If-None-Match {candidate:?} must revalidate"
        );
    }

    // Non-matching validator → full 200.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&uri)
                .header("Authorization", format!("Bearer {token}"))
                .header("If-None-Match", "\"stale\"")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // No-leak parity: an out-of-scope conditional request still 404s (a 304
    // must not become an existence oracle).
    let scoped = create_storage_proxy_token_scoped(&signing_key, &["other:ledger"]);
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&uri)
                .header("Authorization", format!("Bearer {scoped}"))
                .header("If-None-Match", &expected_etag)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// Create a JWS token scoped to specific ledgers (fluree.storage.ledgers).
fn create_storage_proxy_token_scoped(signing_key: &SigningKey, ledgers: &[&str]) -> String {
    let pubkey = signing_key.verifying_key().to_bytes();
    let pubkey_b64 = URL_SAFE_NO_PAD.encode(pubkey);
    let header = serde_json::json!({
        "alg": "EdDSA",
        "jwk": { "kty": "OKP", "crv": "Ed25519", "x": pubkey_b64 }
    });
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let payload = serde_json::json!({
        "iss": did_from_pubkey(&pubkey),
        "exp": now + 3600,
        "iat": now,
        "fluree.storage.all": false,
        "fluree.storage.ledgers": ledgers
    });
    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
    let signing_input = format!("{header_b64}.{payload_b64}");
    let signature = signing_key.sign(signing_input.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());
    format!("{header_b64}.{payload_b64}.{sig_b64}")
}

/// The nameservice record serves a content-digest `ETag` with
/// `Cache-Control: private, no-cache`, answers a matching `If-None-Match`
/// with 304, and the ETag changes when the head advances (making SSE-less
/// head polling a cheap 304 loop). The `*` form of `If-None-Match` is
/// deliberately not honored (see `if_none_match_matches` in
/// `routes/storage_proxy.rs`) — pinned below as a 200.
#[tokio::test]
async fn test_ns_record_etag_and_conditional_get() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state.clone());

    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token(&signing_key, true);

    let create_body = serde_json::json!({ "ledger": "nsetag:test" });
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

    let uri = "/v1/fluree/storage/ns/nsetag:test";

    // 200 carries the watermark ETag + no-cache, and the body still parses.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let etag = header_str(&resp, "etag").to_string();
    assert!(
        etag.len() > 2 && etag.starts_with('"') && etag.ends_with('"'),
        "NS record must carry a quoted ETag, got {etag:?}"
    );
    assert_eq!(header_str(&resp, "cache-control"), "private, no-cache");
    let (_, record) = json_body(resp).await;
    assert!(
        record["commit_t"].is_i64(),
        "body still parses as the record"
    );

    // Replaying the validator revalidates: 304, empty body, ETag echoed.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .header("Authorization", format!("Bearer {token}"))
                .header("If-None-Match", &etag)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(header_str(&resp, "etag"), etag);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert!(body.is_empty(), "304 must carry no body");

    // Advance the head; the stale validator now yields a full 200 with a
    // NEW ETag (monotonic watermarks).
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/update")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "ledger": "nsetag:test",
                        "@context": { "ex": "http://example.org/ns/" },
                        "insert": { "@id": "ex:a", "ex:name": "A" }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .header("Authorization", format!("Bearer {token}"))
                .header("If-None-Match", &etag)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "stale validator after head advance must return the full record"
    );
    let new_etag = header_str(&resp, "etag").to_string();
    assert_ne!(new_etag, etag, "ETag must change when the head advances");

    // `If-None-Match: *` is deliberately not honored (the route answers
    // before consulting storage, so `*`'s "any current representation"
    // semantics cannot be evaluated) — it must serve the full 200, never 304.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .header("Authorization", format!("Bearer {token}"))
                .header("If-None-Match", "*")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "the `*` If-None-Match form is documented as not honored"
    );
    assert_eq!(header_str(&resp, "etag"), new_etag);
}

/// Browser contract for the CORS layer: preflights must explicitly allow
/// `authorization` (the Fetch spec excludes it from the `*` wildcard, so the
/// old blanket allow-list silently broke bearer-token browser clients),
/// response metadata must be exposed to page JavaScript, and the
/// 404-no-existence-leak responses must still carry CORS headers so a browser
/// client can even *read* the 404.
#[tokio::test]
async fn test_cors_preflight_and_404_no_leak_keep_cors_headers() {
    let (_tmp, state) = tx_server_state_with_cors().await;
    let app = build_router(state.clone());

    // Preflight for a bearer-token object fetch.
    let some_cid = ContentId::new(ContentKind::Commit, b"cors probe");
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri(format!(
                    "/v1/fluree/storage/objects/{some_cid}?ledger=any:main"
                ))
                .header("Origin", "https://app.example.com")
                .header("Access-Control-Request-Method", "GET")
                .header("Access-Control-Request-Headers", "authorization, range")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "preflight must succeed");
    assert_eq!(header_str(&resp, "access-control-allow-origin"), "*");
    let allow_headers = header_str(&resp, "access-control-allow-headers").to_lowercase();
    assert!(
        allow_headers.contains("authorization"),
        "authorization must be explicitly allowed (wildcard does not cover it), got: {allow_headers}"
    );
    assert!(
        allow_headers.contains("range"),
        "range must be allowed for ranged CAS reads, got: {allow_headers}"
    );
    assert!(
        !header_str(&resp, "access-control-max-age").is_empty(),
        "preflight results must be cacheable"
    );

    // SSE preflight: last-event-id must be allowed for reconnects.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/v1/fluree/events?ledger=any:main")
                .header("Origin", "https://app.example.com")
                .header("Access-Control-Request-Method", "GET")
                .header("Access-Control-Request-Headers", "last-event-id")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(header_str(&resp, "access-control-allow-headers")
        .to_lowercase()
        .contains("last-event-id"));

    // Actual (non-preflight) response: metadata headers are exposed to JS.
    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token(&signing_key, true);
    let create_body = serde_json::json!({ "ledger": "corsdata:test" });
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

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/storage/ns/corsdata:test")
                .header("Origin", "https://app.example.com")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(header_str(&resp, "access-control-allow-origin"), "*");
    let exposed = header_str(&resp, "access-control-expose-headers").to_lowercase();
    for name in [
        "etag",
        "content-range",
        "x-fluree-content-kind",
        "x-fdb-time",
    ] {
        assert!(
            exposed.contains(name),
            "{name} must be exposed to browser JS, got: {exposed}"
        );
    }

    // 404-no-leak (out-of-scope token) still carries CORS headers — without
    // them a browser client sees an opaque network error instead of the 404.
    let scoped = create_storage_proxy_token_scoped(&signing_key, &["other:ledger"]);
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/storage/ns/corsdata:test")
                .header("Origin", "https://app.example.com")
                .header("Authorization", format!("Bearer {scoped}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        header_str(&resp, "access-control-allow-origin"),
        "*",
        "no-leak 404s must still be CORS-readable"
    );
}

/// The allow-list mirrors the preflight: every documented client-facing
/// request header — the `fluree-*` policy/tracking headers,
/// `Idempotency-Key`, and the `If-None-Match` this branch teaches clients
/// to send — must survive preflight, not just `authorization`.
#[tokio::test]
async fn test_cors_preflight_mirrors_every_requested_header() {
    let (_tmp, state) = tx_server_state_with_cors().await;
    let app = build_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/v1/fluree/query")
                .header("Origin", "https://app.example.com")
                .header("Access-Control-Request-Method", "POST")
                .header(
                    "Access-Control-Request-Headers",
                    "authorization, fluree-identity, fluree-track-fuel, idempotency-key, if-none-match, content-type",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "preflight must succeed");
    let allowed = header_str(&resp, "access-control-allow-headers").to_lowercase();
    for name in [
        "authorization",
        "fluree-identity",
        "fluree-track-fuel",
        "idempotency-key",
        "if-none-match",
        "content-type",
    ] {
        assert!(
            allowed.contains(name),
            "{name} must be allowed at preflight, got: {allowed}"
        );
    }
}

/// The NS ETag identifies the whole representation, not just the
/// watermarks: creating a child branch changes the parent's `branches`
/// count without a commit, and the previously-valid validator must then
/// yield a full 200 with a new ETag — otherwise a revalidating browser is
/// served a stale record indefinitely.
#[tokio::test]
async fn test_ns_record_etag_changes_without_a_commit() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state.clone());

    let secret = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let token = create_storage_proxy_token(&signing_key, true);

    for (uri, body) in [
        (
            "/v1/fluree/create",
            serde_json::json!({ "ledger": "nsbranch:main" }).to_string(),
        ),
        (
            "/v1/fluree/update",
            serde_json::json!({
                "ledger": "nsbranch:main",
                "@context": { "ex": "http://example.org/ns/" },
                "insert": { "@id": "ex:a", "ex:name": "A" }
            })
            .to_string(),
        ),
    ] {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            resp.status().is_success(),
            "{uri} must succeed, got {}",
            resp.status()
        );
    }

    let ns_uri = "/v1/fluree/storage/ns/nsbranch:main";
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(ns_uri)
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let etag = header_str(&resp, "etag").to_string();
    let (_, before) = json_body(resp).await;

    // Create a child branch: no commit on the parent, but its `branches`
    // count moves.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/branch")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "ledger": "nsbranch", "branch": "child" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "branch create must succeed, got {}",
        resp.status()
    );

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(ns_uri)
                .header("Authorization", format!("Bearer {token}"))
                .header("If-None-Match", &etag)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "stale validator must not revalidate after the record changed"
    );
    let new_etag = header_str(&resp, "etag").to_string();
    assert_ne!(new_etag, etag, "ETag must change with the representation");
    let (_, after) = json_body(resp).await;
    assert_eq!(before["commit_t"], after["commit_t"], "no commit happened");
    assert_ne!(before["branches"], after["branches"], "branch count moved");
}

/// The per-ledger SSE subscription form, over HTTP.
///
/// `GET /v1/fluree/events?ledger=<alias>` is what every peer opens — the
/// native peer builds it in `peer/subscription.rs`, the browser peer in
/// `fluree-db-browser`'s `events_url`, and the module docs advertise
/// `?ledger=a&ledger=b`. It nevertheless answered **400** for any value,
/// because `axum::extract::Query` deserializes with `serde_urlencoded`,
/// which cannot build a `Vec` from repeated keys. Every unit test built an
/// `EventsQuery` in process, so nothing caught it; only `?all=true`
/// (a plain bool) ever worked, which is what the peer test configs use.
#[tokio::test]
async fn test_events_accepts_per_ledger_subscription_over_http() {
    let (_tmp, state) = tx_server_state().await;
    let app = build_router(state);

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "ledger": "sse:main" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    // The body is an endless stream; only the response head is asserted (a
    // rejected query string never gets that far).
    for uri in [
        "/v1/fluree/events?ledger=sse:main",
        "/v1/fluree/events?ledger=sse%3Amain",
        "/v1/fluree/events?ledger=sse%3Amain&ledger=other%3Amain",
        "/v1/fluree/events?ledger=sse%3Amain&graph-source=gs%3Amain",
        "/v1/fluree/events?all=true",
        "/v1/fluree/events",
    ] {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(uri)
                    .header("Accept", "text/event-stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "GET {uri}");
        assert!(
            resp.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .starts_with("text/event-stream"),
            "GET {uri} must open an SSE stream"
        );
    }
}
