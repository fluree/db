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

/// Server with the negotiated presigned-upload flow enabled and a tiny direct
/// cap, so the handshake is exercised even for small test archives.
async fn presign_test_state() -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        import_presign_enabled: true,
        import_direct_max_bytes: 8,
        import_staging_dir: Some(tmp.path().join("staging")),
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    (tmp, state)
}

/// Server with the negotiated flow enabled, a tiny multipart threshold, and a
/// tiny part size — so even a small test archive is minted as a multi-part
/// upload and the part-split + assemble path is exercised end-to-end.
async fn multipart_test_state() -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        import_presign_enabled: true,
        import_direct_max_bytes: 8,
        import_staging_dir: Some(tmp.path().join("staging")),
        // Force multipart for any archive ≥ 8 bytes, in ~200-byte parts.
        import_multipart_threshold_bytes: 8,
        import_multipart_part_size_bytes: 200,
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    (tmp, state)
}

/// PUT one byte range of `archive` to a part URL; returns the response's ETag.
async fn put_part(
    state: &Arc<AppState>,
    url: &str,
    bytes: Vec<u8>,
) -> (StatusCode, Option<String>) {
    let resp = build_router(state.clone())
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(url)
                .header("content-type", "application/x-fluree-pack")
                .body(Body::from(bytes))
                .unwrap(),
        )
        .await
        .expect("part request");
    let status = resp.status();
    let etag = resp
        .headers()
        .get(http::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim_matches('"').to_string());
    (status, etag)
}

/// Build a `.flpack` archive of a freshly-populated source ledger.
async fn make_archive(state: &Arc<AppState>, src_ledger: &str) -> Vec<u8> {
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

    let mut archive: Vec<u8> = Vec::new();
    state
        .fluree
        .archive_ledger(src_ledger, true, &mut archive)
        .await
        .expect("archive source ledger");
    archive
}

async fn json_request(state: &Arc<AppState>, req: Request<Body>) -> (StatusCode, JsonValue) {
    let resp = build_router(state.clone())
        .oneshot(req)
        .await
        .expect("request");
    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("collect")
        .to_bytes();
    let json = if bytes.is_empty() {
        JsonValue::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(JsonValue::Null)
    };
    (status, json)
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
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("collect")
        .to_bytes();
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

#[tokio::test]
async fn discovery_advertises_presigned_when_enabled() {
    let (_tmp, state) = presign_test_state().await;
    let (status, doc) = json_request(
        &state,
        Request::builder()
            .method("GET")
            .uri("/.well-known/fluree.json")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let modes = doc["import"]["modes"].as_array().expect("import.modes");
    assert!(
        modes.iter().any(|m| m == "presigned-put"),
        "discovery should advertise presigned-put: {doc}"
    );
    assert_eq!(doc["import"]["direct_max_bytes"], 8);
}

#[tokio::test]
async fn negotiated_upload_round_trip() {
    let (_tmp, state) = presign_test_state().await;
    let archive = make_archive(&state, "neg-src/data:main").await;
    let dst = "neg-dst/data:main";

    // 1. Mint an upload slot.
    let (status, mint) = json_request(
        &state,
        Request::builder()
            .method("POST")
            .uri("/v1/fluree/import-upload")
            .header("content-type", "application/json")
            .body(Body::from(json!({ "ledger": dst }).to_string()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "mint: {mint:?}");
    let import_id = mint["import_id"].as_str().expect("import_id").to_string();
    let upload_url = mint["upload"]["url"]
        .as_str()
        .expect("upload.url")
        .to_string();
    assert!(
        upload_url.contains("token="),
        "url carries a token: {upload_url}"
    );

    // 2. PUT the archive to the minted (relative) URL.
    let (status, _) = json_request(
        &state,
        Request::builder()
            .method("PUT")
            .uri(&upload_url)
            .header("content-type", "application/x-fluree-pack")
            .body(Body::from(archive))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "blob upload should succeed");

    // 3. Complete → running.
    let (status, complete) = json_request(
        &state,
        Request::builder()
            .method("POST")
            .uri(format!("/v1/fluree/import-upload/{import_id}/complete"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "complete: {complete:?}");
    assert_eq!(complete["status"], "running");

    // 4. Poll status to a terminal state.
    let mut final_status = JsonValue::Null;
    for _ in 0..100 {
        let (status, body) = json_request(
            &state,
            Request::builder()
                .method("GET")
                .uri(format!("/v1/fluree/import-upload/{import_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        if matches!(body["status"].as_str(), Some("succeeded" | "failed")) {
            final_status = body;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        final_status["status"], "succeeded",
        "import should succeed: {final_status:?}"
    );
    assert_eq!(final_status["result"]["ledger_id"], dst);
    assert_eq!(final_status["result"]["commits"], 1);

    // 5. The restored ledger is queryable.
    let handle = state.fluree.ledger(dst).await.expect("load restored");
    let query = json!({
        "select": ["?name"],
        "where": { "@id": "?s", "@type": "ex:User", "schema:name": "?name" },
        "@context": { "ex": "http://example.org/ns/", "schema": "http://schema.org/" }
    });
    let db = GraphDb::from_ledger_state(&handle);
    let out = state
        .fluree
        .query(&db, &query)
        .await
        .expect("query")
        .to_jsonld(&handle.snapshot)
        .expect("to_jsonld");
    assert_eq!(out.as_array().expect("array").len(), 2);
}

#[tokio::test]
async fn discovery_advertises_multipart_with_hints() {
    let (_tmp, state) = multipart_test_state().await;
    let (status, doc) = json_request(
        &state,
        Request::builder()
            .method("GET")
            .uri("/.well-known/fluree.json")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let modes = doc["import"]["modes"].as_array().expect("import.modes");
    assert!(
        modes.iter().any(|m| m == "multipart-put"),
        "discovery should advertise multipart-put: {doc}"
    );
    assert_eq!(doc["import"]["multipart_threshold_bytes"], 8);
    assert_eq!(doc["import"]["multipart_part_size_bytes"], 200);
}

#[tokio::test]
async fn negotiated_multipart_round_trip() {
    let (_tmp, state) = multipart_test_state().await;
    let archive = make_archive(&state, "mp-src/data:main").await;
    let dst = "mp-dst/data:main";
    let archive_len = archive.len() as u64;

    // 1. Mint — declaring the size forces the multipart plan.
    let (status, mint) = json_request(
        &state,
        Request::builder()
            .method("POST")
            .uri("/v1/fluree/import-upload")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({ "ledger": dst, "size": archive_len }).to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "mint: {mint:?}");
    let import_id = mint["import_id"].as_str().expect("import_id").to_string();
    let mp = &mint["multipart"];
    let part_size = mp["part_size_bytes"].as_u64().expect("part_size_bytes");
    let parts = mp["parts"].as_array().expect("parts array");
    assert!(
        parts.len() >= 2,
        "tiny part size should split the archive into multiple parts (got {})",
        parts.len()
    );

    // 2. PUT each part's byte range; collect the reported ETags.
    let mut completed: Vec<serde_json::Value> = Vec::new();
    for part in parts {
        let part_number = part["part_number"].as_u64().expect("part_number");
        let url = part["url"].as_str().expect("part url");
        let offset = (part_number - 1) * part_size;
        let end = (offset + part_size).min(archive_len);
        let slice = archive[offset as usize..end as usize].to_vec();
        let (status, etag) = put_part(&state, url, slice).await;
        assert_eq!(status, StatusCode::OK, "part {part_number} upload");
        completed.push(json!({
            "part_number": part_number,
            "etag": etag.unwrap_or_default(),
        }));
    }

    // 3. Complete with the part list → running.
    let (status, complete) = json_request(
        &state,
        Request::builder()
            .method("POST")
            .uri(format!("/v1/fluree/import-upload/{import_id}/complete"))
            .header("content-type", "application/json")
            .body(Body::from(json!({ "parts": completed }).to_string()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "complete: {complete:?}");

    // 4. Poll to a terminal state.
    let mut final_status = JsonValue::Null;
    for _ in 0..100 {
        let (status, body) = json_request(
            &state,
            Request::builder()
                .method("GET")
                .uri(format!("/v1/fluree/import-upload/{import_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        if matches!(body["status"].as_str(), Some("succeeded" | "failed")) {
            final_status = body;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        final_status["status"], "succeeded",
        "multipart import should succeed: {final_status:?}"
    );
    assert_eq!(final_status["result"]["ledger_id"], dst);
    assert_eq!(final_status["result"]["commits"], 1);

    // 5. The reassembled archive restored a queryable ledger.
    let handle = state.fluree.ledger(dst).await.expect("load restored");
    let query = json!({
        "select": ["?name"],
        "where": { "@id": "?s", "@type": "ex:User", "schema:name": "?name" },
        "@context": { "ex": "http://example.org/ns/", "schema": "http://schema.org/" }
    });
    let db = GraphDb::from_ledger_state(&handle);
    let out = state
        .fluree
        .query(&db, &query)
        .await
        .expect("query")
        .to_jsonld(&handle.snapshot)
        .expect("to_jsonld");
    assert_eq!(out.as_array().expect("array").len(), 2);
}

#[tokio::test]
async fn multipart_complete_with_missing_part_is_rejected() {
    let (_tmp, state) = multipart_test_state().await;
    let archive = make_archive(&state, "mp-miss-src/data:main").await;
    let dst = "mp-miss-dst/data:main";
    let archive_len = archive.len() as u64;

    let (_status, mint) = json_request(
        &state,
        Request::builder()
            .method("POST")
            .uri("/v1/fluree/import-upload")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({ "ledger": dst, "size": archive_len }).to_string(),
            ))
            .unwrap(),
    )
    .await;
    let import_id = mint["import_id"].as_str().expect("import_id").to_string();
    let parts = mint["multipart"]["parts"].as_array().expect("parts");
    let part_size = mint["multipart"]["part_size_bytes"].as_u64().unwrap();
    assert!(parts.len() >= 2, "need ≥2 parts to drop one");

    // Upload all parts EXCEPT the last.
    for part in &parts[..parts.len() - 1] {
        let part_number = part["part_number"].as_u64().unwrap();
        let url = part["url"].as_str().unwrap();
        let offset = (part_number - 1) * part_size;
        let end = (offset + part_size).min(archive_len);
        let (status, _) =
            put_part(&state, url, archive[offset as usize..end as usize].to_vec()).await;
        assert_eq!(status, StatusCode::OK);
    }

    // Complete claiming ALL parts — the missing last part must be caught.
    let claimed: Vec<serde_json::Value> = parts
        .iter()
        .map(|p| json!({ "part_number": p["part_number"], "etag": "x" }))
        .collect();
    let (status, _) = json_request(
        &state,
        Request::builder()
            .method("POST")
            .uri(format!("/v1/fluree/import-upload/{import_id}/complete"))
            .header("content-type", "application/json")
            .body(Body::from(json!({ "parts": claimed }).to_string()))
            .unwrap(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "completing with a never-uploaded part must be rejected"
    );
}

#[tokio::test]
async fn complete_before_upload_is_rejected() {
    let (_tmp, state) = presign_test_state().await;

    let (_status, mint) = json_request(
        &state,
        Request::builder()
            .method("POST")
            .uri("/v1/fluree/import-upload")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({ "ledger": "no-blob/data:main" }).to_string(),
            ))
            .unwrap(),
    )
    .await;
    let import_id = mint["import_id"].as_str().expect("import_id");

    let (status, _) = json_request(
        &state,
        Request::builder()
            .method("POST")
            .uri(format!("/v1/fluree/import-upload/{import_id}/complete"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "completing with no uploaded archive must be rejected"
    );
}

#[tokio::test]
async fn negotiated_endpoints_404_when_presign_disabled() {
    let (_tmp, state) = test_state().await; // presign disabled
    let (status, _) = json_request(
        &state,
        Request::builder()
            .method("POST")
            .uri("/v1/fluree/import-upload")
            .header("content-type", "application/json")
            .body(Body::from(json!({ "ledger": "x:main" }).to_string()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
