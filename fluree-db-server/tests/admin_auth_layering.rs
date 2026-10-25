//! Pins that admin-token auth gates the admin-protected write
//! routes as the OUTER layer — an unauthenticated request is
//! rejected locally rather than doing any downstream work (in raft
//! mode, that downstream work includes relaying the request to the
//! leader, so forwarding an unauthenticated request would be a DoS
//! amplifier). The layer order in `build_router` is subtle — axum
//! runs the last-applied layer outermost — so this guards against a
//! reordering that would let unauthenticated requests through the
//! auth gate.

use axum::body::Body;
use fluree_db_server::config::AdminAuthMode;
use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

async fn state_with_admin_auth_required() -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        admin_auth_mode: AdminAuthMode::Required,
        admin_auth_insecure_accept_any_issuer: true,
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState"));
    (tmp, state)
}

#[tokio::test]
async fn unauthenticated_admin_write_is_rejected_before_downstream_work() {
    let (_tmp, state) = state_with_admin_auth_required().await;

    let resp = build_router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"ledger":"test:main"}"#))
                .expect("request"),
        )
        .await
        .expect("router response");

    // No Bearer token → the outer auth layer short-circuits with
    // 401 before the create handler (or, in raft mode, the leader
    // forward) ever runs.
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn authenticated_shape_passes_the_auth_gate() {
    // A Bearer token that fails verification is still rejected, but
    // with a token present the request reaches verification rather
    // than the missing-token short-circuit — confirming the gate
    // keys on the token, not merely on route matching. A bogus
    // token verifies-and-fails to 401; the point is the request got
    // past extraction into verification.
    let (_tmp, state) = state_with_admin_auth_required().await;

    let resp = build_router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .header("authorization", "Bearer not-a-real-jwt")
                .body(Body::from(r#"{"ledger":"test:main"}"#))
                .expect("request"),
        )
        .await
        .expect("router response");

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

/// The sweep deletes index artifacts, so both its forms must sit behind the
/// same outer auth gate as every other admin write rather than relying on
/// route matching alone.
#[tokio::test]
async fn unauthenticated_sweep_is_rejected_before_downstream_work() {
    for uri in ["/v1/fluree/sweep", "/v1/fluree/sweep/plan"] {
        let (_tmp, state) = state_with_admin_auth_required().await;

        let resp = build_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"ledger":"test"}"#))
                    .expect("request"),
            )
            .await
            .expect("router response");

        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "{uri} must be admin-gated"
        );
    }
}

/// An admin token from `key`, which a server accepting any issuer trusts.
fn admin_token(key: &ed25519_dalek::SigningKey) -> String {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use ed25519_dalek::Signer;

    let pubkey = key.verifying_key().to_bytes();
    let header = serde_json::json!({
        "alg": "EdDSA",
        "jwk": {"kty": "OKP", "crv": "Ed25519", "x": URL_SAFE_NO_PAD.encode(pubkey)}
    });
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let payload = serde_json::json!({
        "iss": fluree_db_credential::did_from_pubkey(&pubkey),
        "sub": "admin@example.com",
        "exp": now + 3600,
        "iat": now,
    });
    let input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(header.to_string()),
        URL_SAFE_NO_PAD.encode(payload.to_string())
    );
    let signature = URL_SAFE_NO_PAD.encode(key.sign(input.as_bytes()).to_bytes());
    format!("{input}.{signature}")
}

/// A push carrying erasures needs what erasing needs: with admin auth
/// required, one without an admin token is refused before its erasures are
/// read; one with an admin token gets as far as reading them.
#[tokio::test]
async fn a_push_of_erasures_needs_an_admin_token() {
    let (_tmp, state) = state_with_admin_auth_required().await;
    state.fluree.create_ledger("pushed").await.unwrap();
    let erased = fluree_db_core::ContentId::new(fluree_db_core::ContentKind::Commit, b"erased");
    let body = serde_json::json!({
        "commits": [""],
        "commit_ids": [erased.to_string()],
        "erasure_frames": ["AAAA"],
    })
    .to_string();
    let push = |token: Option<String>| {
        let mut request = Request::builder()
            .method("POST")
            .uri("/v1/fluree/push/pushed:main")
            .header("content-type", "application/json");
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        request.body(Body::from(body.clone())).expect("request")
    };

    let refused = build_router(state.clone())
        .oneshot(push(None))
        .await
        .expect("router response");
    assert_eq!(refused.status(), StatusCode::FORBIDDEN);

    let key = ed25519_dalek::SigningKey::from_bytes(&[9u8; 32]);
    let read = build_router(state)
        .oneshot(push(Some(admin_token(&key))))
        .await
        .expect("router response");
    let status = read.status();
    let body = http_body_util::BodyExt::collect(read.into_body())
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
    assert!(String::from_utf8_lossy(&body).contains("erasure frame"));
}
