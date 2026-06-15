//! HTTP integration test for `POST /v1/fluree/import/*ledger`.
//!
//! Populates a source ledger, archives it to a `.flpack` byte buffer via the
//! API, then POSTs the archive to the import endpoint under a *different* name
//! and verifies the restored ledger is queryable.

use axum::body::Body;
use fluree_db_api::{GraphDb, LedgerState, Novelty};
use fluree_db_core::LedgerSnapshot;
use fluree_db_server::config::ServerConfig;
use fluree_db_server::{routes::build_router, AppState, TelemetryConfig};
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

#[tokio::test]
async fn import_endpoint_restores_ledger_under_new_name() {
    let (_tmp, state) = test_state().await;

    let src_ledger = "imp-src/data:main";
    let dst_ledger = "imp-dst/data:main";

    // ── Populate the source ledger via the API ───────────────────────
    let insert = json!({
        "@context": { "ex": "http://example.org/ns/", "schema": "http://schema.org/" },
        "@graph": [
            { "@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice" },
            { "@id": "ex:bob", "@type": "ex:User", "schema:name": "Bob" }
        ]
    });
    let db = LedgerSnapshot::genesis(src_ledger);
    let ledger_state = LedgerState::new(db, Novelty::new(0));
    state
        .fluree
        .insert(ledger_state, &insert)
        .await
        .expect("insert source data");

    // ── Archive the source to a .flpack byte buffer ──────────────────
    let mut archive: Vec<u8> = Vec::new();
    state
        .fluree
        .archive_ledger(src_ledger, true, &mut archive)
        .await
        .expect("archive source ledger");
    assert!(archive.len() > 100, "archive should carry data");

    // ── POST the archive to the import endpoint under a new name ──────
    let app = build_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/import/{dst_ledger}"))
                .header("content-type", "application/x-fluree-pack")
                .body(Body::from(archive))
                .expect("build request"),
        )
        .await
        .expect("import request");

    let status = resp.status();
    let bytes = resp.into_body().collect().await.expect("collect").to_bytes();
    let summary: JsonValue = serde_json::from_slice(&bytes).expect("valid JSON summary");
    assert_eq!(
        status,
        StatusCode::CREATED,
        "import should succeed: {summary:?}"
    );
    assert_eq!(summary["ledger_id"], dst_ledger);
    assert_eq!(summary["commits"], 1, "one commit restored: {summary:?}");

    // ── Query the restored ledger to confirm the data is present ─────
    let query = json!({
        "select": ["?name"],
        "where": { "@id": "?s", "@type": "ex:User", "schema:name": "?name" },
        "orderBy": "?name",
        "@context": { "ex": "http://example.org/ns/", "schema": "http://schema.org/" }
    });
    let handle = state
        .fluree
        .ledger(dst_ledger)
        .await
        .expect("load restored ledger");
    let dst_db = GraphDb::from_ledger_state(&handle);
    let result = state
        .fluree
        .query(&dst_db, &query)
        .await
        .expect("query restored ledger")
        .to_jsonld(&handle.snapshot)
        .expect("to_jsonld");
    let rows = result.as_array().expect("array result");
    assert_eq!(rows.len(), 2, "restored ledger should hold both users");
}

#[tokio::test]
async fn import_endpoint_rejects_duplicate_name() {
    let (_tmp, state) = test_state().await;

    let src_ledger = "imp-dup/data:main";
    let insert = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id": "ex:x", "ex:v": "1"
    });
    let db = LedgerSnapshot::genesis(src_ledger);
    let ledger_state = LedgerState::new(db, Novelty::new(0));
    state
        .fluree
        .insert(ledger_state, &insert)
        .await
        .expect("insert");

    let mut archive: Vec<u8> = Vec::new();
    state
        .fluree
        .archive_ledger(src_ledger, true, &mut archive)
        .await
        .expect("archive");

    // Import onto the *same* name that already exists → must be rejected.
    let app = build_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/import/{src_ledger}"))
                .header("content-type", "application/x-fluree-pack")
                .body(Body::from(archive))
                .expect("build request"),
        )
        .await
        .expect("import request");

    assert_eq!(
        resp.status(),
        StatusCode::CONFLICT,
        "importing onto an existing ledger name should be a 409"
    );
}
