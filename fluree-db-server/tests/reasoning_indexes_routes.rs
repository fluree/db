//! The reasoning-index admin routes, driven through the real router: register a
//! program, refresh it, list the branch's indexes, and retire one.

use axum::body::Body;
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;
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

async fn post(app: &axum::Router, uri: &str, body: JsonValue) -> (StatusCode, JsonValue) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("router response");
    let status = resp.status();
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    let json = serde_json::from_slice(&bytes).unwrap_or(JsonValue::Null);
    (status, json)
}

#[tokio::test]
async fn register_refresh_list_and_retire_an_index() {
    let (_tmp, state) = test_state().await;
    let app = build_router(Arc::clone(&state));
    let ledger = "routes/indexes:main";
    let (status, _) = post(&app, "/v1/fluree/create", json!({"ledger": ledger})).await;
    assert!(status.is_success(), "{status}");
    let (status, _) = post(
        &app,
        "/v1/fluree/insert",
        json!({
            "ledger": ledger,
            "@context": {
                "ex": "http://example.org/",
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
            },
            "@graph": [
                {"@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}},
                {"@id": "ex:alice", "@type": "ex:Student"}
            ]
        }),
    )
    .await;
    assert!(status.is_success(), "{status}");

    let (status, registered) = post(
        &app,
        "/v1/fluree/reasoning-indexes/register",
        json!({"ledger": ledger, "program": {"reasoning": "owl2rl"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{registered}");
    let index_id = registered["indexId"].as_str().expect("indexId").to_string();

    let (status, refreshed) = post(
        &app,
        "/v1/fluree/reasoning-indexes/refresh",
        json!({"ledger": ledger}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{refreshed}");
    assert_eq!(refreshed["results"][0]["indexId"], json!(index_id));
    assert_eq!(refreshed["results"][0]["outcome"], json!("published"));
    assert_eq!(refreshed["results"][0]["generation"], json!(1));

    let (status, listed) = post(
        &app,
        "/v1/fluree/reasoning-indexes/list",
        json!({"ledger": ledger}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{listed}");
    assert_eq!(listed["indexes"][&index_id]["head"]["generation"], json!(1));

    let (status, _) = post(
        &app,
        "/v1/fluree/reasoning-indexes/retire",
        json!({"ledger": ledger, "indexId": index_id}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, listed) = post(
        &app,
        "/v1/fluree/reasoning-indexes/list",
        json!({"ledger": ledger}),
    )
    .await;
    assert_eq!(
        listed["indexes"][&index_id]["registration"]["state"]["state"],
        json!("retired")
    );

    let (status, _) = post(
        &app,
        "/v1/fluree/reasoning-indexes/retire",
        json!({"ledger": ledger, "indexId": "not-a-ulid"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
