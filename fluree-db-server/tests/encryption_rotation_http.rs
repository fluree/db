//! Key rotation over HTTP: key ids are reported, a rotation runs to
//! completion through the routes, verify stamps the record, and the
//! write endpoints sit behind the admin-token gate.

use axum::body::Body;
use fluree_db_server::config::AdminAuthMode;
use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use serde_json::{json, Value};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

const KEY1: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
const KEY2: &str = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=";

fn write_config(tmp: &TempDir, keys: &[(u32, &str)], current: u32) -> std::path::PathBuf {
    let keys: Vec<_> = keys
        .iter()
        .map(|(id, key)| json!({"keyId": id, "AES256Key": key}))
        .collect();
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {
                "@id": "storage",
                "@type": "Storage",
                "filePath": tmp.path().join("data").to_string_lossy(),
                "AES256Keys": keys,
                "AES256CurrentKey": current
            },
            {"@id": "connection", "@type": "Connection", "indexStorage": {"@id": "storage"}}
        ]
    });
    let path = tmp.path().join(format!("connection-{current}.jsonld"));
    std::fs::write(&path, config.to_string()).expect("write config");
    path
}

async fn state_for(
    tmp: &TempDir,
    keys: &[(u32, &str)],
    current: u32,
    admin_auth_mode: AdminAuthMode,
) -> Arc<AppState> {
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        connection_config: Some(write_config(tmp, keys, current)),
        admin_auth_mode,
        admin_auth_insecure_accept_any_issuer: true,
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    Arc::new(AppState::new(cfg, telemetry).await.expect("AppState"))
}

async fn call(
    state: &Arc<AppState>,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if body.is_none() {
        req = req.header("content-length", "0");
    }
    let req = req
        .body(Body::from(body.map(|b| b.to_string()).unwrap_or_default()))
        .expect("request");
    let resp = build_router(state.clone())
        .oneshot(req)
        .await
        .expect("router response");
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .expect("body");
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

#[tokio::test]
async fn rotation_runs_and_verifies_over_http() {
    let tmp = tempfile::tempdir().expect("tempdir");

    // Seed under key 1.
    {
        let state = state_for(&tmp, &[(1, KEY1)], 1, AdminAuthMode::None).await;
        let (status, body) = call(&state, "GET", "/v1/fluree/encryption", None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["encrypted"], json!(true));
        assert_eq!(body["key_ids"], json!([1]));
        let (status, body) = call(
            &state,
            "POST",
            "/v1/fluree/create",
            Some(json!({"ledger": "rot-http:main"})),
        )
        .await;
        assert!(status.is_success(), "{status} {body}");
        let (status, body) = call(
            &state,
            "POST",
            "/v1/fluree/insert/rot-http:main",
            Some(json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:alice",
                "ex:name": "Alice"
            })),
        )
        .await;
        assert!(status.is_success(), "{status} {body}");
    }

    // Key 2 current, key 1 held.
    let state = state_for(&tmp, &[(1, KEY1), (2, KEY2)], 2, AdminAuthMode::None).await;
    let (status, body) = call(&state, "GET", "/v1/fluree/encryption", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["key_ids"], json!([2, 1]));
    assert_eq!(body["current_key_id"], json!(2));

    let (status, body) = call(&state, "GET", "/v1/fluree/encryption/rotate/status", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["progress"], Value::Null, "no rotation yet");

    // Retiring the current key is refused.
    let (status, _) = call(
        &state,
        "POST",
        "/v1/fluree/encryption/rotate",
        Some(json!({"retire_key_id": 2})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, body) = call(
        &state,
        "POST",
        "/v1/fluree/encryption/rotate",
        Some(json!({"retire_key_id": 1})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["state"], json!("running"));
    assert!(body["holder"].as_str().is_some_and(|h| !h.is_empty()));

    let mut progress = Value::Null;
    for _ in 0..200 {
        let (status, body) = call(&state, "GET", "/v1/fluree/encryption/rotate/status", None).await;
        assert_eq!(status, StatusCode::OK);
        progress = body["progress"].clone();
        if progress["state"] == json!("completed") {
            assert_eq!(body["active_here"], json!(false));
            assert_eq!(body["stalled"], json!(false));
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(progress["state"], json!("completed"), "{progress}");
    assert!(progress["rewritten"].as_u64().unwrap() > 0);
    assert_eq!(progress["failed"], json!(0));
    assert_eq!(progress["completion"]["remaining_on_retired"], json!(0));

    let (status, body) = call(
        &state,
        "POST",
        "/v1/fluree/encryption/rotate/verify",
        Some(json!({"retire_key_id": 1})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["state"], json!("completed"));

    // Nothing runs now, so pause is a conflict.
    let (status, _) = call(&state, "POST", "/v1/fluree/encryption/rotate/pause", None).await;
    assert_eq!(status, StatusCode::CONFLICT);

    // The data reads back through a server holding key 2 alone.
    drop(state);
    let state = state_for(&tmp, &[(2, KEY2)], 2, AdminAuthMode::None).await;
    let (status, body) = call(
        &state,
        "POST",
        "/v1/fluree/query/rot-http:main",
        Some(json!({
            "@context": {"ex": "http://example.org/"},
            "select": ["?name"],
            "where": {"@id": "ex:alice", "ex:name": "?name"}
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, json!([["Alice"]]));
}

#[tokio::test]
async fn rotation_writes_are_admin_gated_and_reads_answer_unencrypted() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let state = state_for(&tmp, &[(1, KEY1)], 1, AdminAuthMode::Required).await;
    let (status, _) = call(
        &state,
        "POST",
        "/v1/fluree/encryption/rotate",
        Some(json!({"retire_key_id": 1})),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = call(&state, "GET", "/v1/fluree/encryption", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // An unencrypted server reports so rather than erroring.
    let plain = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(plain.path().to_path_buf()),
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState"));
    let (status, body) = call(&state, "GET", "/v1/fluree/encryption", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["encrypted"], json!(false));
    let (status, _) = call(
        &state,
        "POST",
        "/v1/fluree/encryption/rotate",
        Some(json!({"retire_key_id": 1})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
