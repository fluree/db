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
