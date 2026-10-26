//! A graph source with no R2RML mapping, over HTTP: `POST /iceberg/map`
//! without `r2rml` is a 400 and stores nothing, so no surface can register a
//! source its queries could not be answered from.
#![cfg(feature = "iceberg")]

use axum::body::Body;
use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

async fn state() -> (TempDir, Arc<AppState>) {
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

async fn send(
    state: &Arc<AppState>,
    uri: &str,
    content_type: &str,
    body: String,
) -> (StatusCode, String) {
    let resp = build_router(Arc::clone(state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", content_type)
                .header("accept", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// `POST /iceberg/map` needs a mapping: without one it is a 400 naming the
/// field, and no source is stored.
#[tokio::test]
async fn iceberg_map_without_a_mapping_is_a_400_and_stores_nothing() {
    let (_tmp, state) = state().await;
    let body = json!({
        "name": "nomap",
        "mode": "direct",
        "table_location": "s3://bucket/warehouse/ns/t",
    });
    let (status, text) = send(
        &state,
        "/v1/fluree/iceberg/map",
        "application/json",
        body.to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
    assert!(text.contains("missing field `r2rml`"), "{text}");
    assert!(
        state
            .fluree
            .nameservice()
            .lookup_graph_source("nomap:main")
            .await
            .expect("lookup")
            .is_none(),
        "a refused registration stores no record"
    );
}
