//! `POST /index`: index maintenance a scheduler can call where no
//! long-running indexer runs. It indexes pending commits and folds the pooled
//! tree's delta into its base, and reports which it did.

use axum::body::Body;
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

async fn call(app: &axum::Router, uri: &str, ctype: &str, body: String) -> (StatusCode, Value) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", ctype)
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("router response");
    let status = resp.status();
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    (
        status,
        serde_json::from_slice(&bytes)
            .unwrap_or(Value::String(String::from_utf8_lossy(&bytes).into_owned())),
    )
}

async fn upsert(app: &axum::Router, trig: &str) {
    let (status, body) = call(
        app,
        "/v1/fluree/upsert/docs:main",
        "application/trig",
        trig.to_string(),
    )
    .await;
    assert!(status.is_success(), "upsert: {status} {body}");
}

async fn index(app: &axum::Router) -> Value {
    let (status, body) = call(
        app,
        "/v1/fluree/index",
        "application/json",
        json!({ "ledger": "docs:main" }).to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

#[tokio::test]
async fn index_route_builds_then_compacts_the_pooled_delta() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    let app = build_router(state);
    let (status, body) = call(
        &app,
        "/v1/fluree/create",
        "application/json",
        json!({ "ledger": "docs:main" }).to_string(),
    )
    .await;
    assert!(status.is_success(), "create: {status} {body}");

    upsert(
        &app,
        "@prefix ex: <http://example.org/> .\n\
         GRAPH ex:g1 { ex:a ex:val 1 . } GRAPH ex:g2 { ex:b ex:val 2 . }\n",
    )
    .await;
    let first = index(&app).await;
    assert_eq!(first["built"], true, "{first}");
    assert_eq!(first["compacted"], false, "{first}");

    // The next build writes the pooled graphs' change as a delta run; the
    // same call folds it into the base.
    upsert(
        &app,
        "@prefix ex: <http://example.org/> .\n\
         GRAPH ex:g1 { ex:c ex:val 3 . } GRAPH ex:g2 { ex:d ex:val 4 . }\n",
    )
    .await;
    let second = index(&app).await;
    assert_eq!(second["built"], true, "{second}");
    assert_eq!(second["compacted"], true, "{second}");
    assert!(
        second["index_t"].as_i64() > first["index_t"].as_i64(),
        "{second}"
    );

    let idle = index(&app).await;
    assert_eq!(idle["built"], false, "{idle}");
    assert_eq!(idle["compacted"], false, "{idle}");
    assert_eq!(idle["index_t"], second["index_t"], "{idle}");
    assert_eq!(idle["root_id"], second["root_id"], "{idle}");

    let (status, _) = call(
        &app,
        "/v1/fluree/index",
        "application/json",
        json!({ "ledger": "nope:main" }).to_string(),
    )
    .await;
    assert!(status.is_client_error(), "{status}");
}
