//! The third annotation source, which needs its own process to reach.
//!
//! Export can answer annotations from a sealed arena, from the novelty
//! overlay, or from the **base-index scan**. Selecting the scan means setting
//! `FLUREE_EXPORT_ANNOTATION_SCAN`, which is process-global while these
//! assertions are per-test — so this is a standalone `[[test]]` target rather
//! than a `grp_http` member, for the same reason `telemetry_test` is.
//!
//! The scan used to be the odd one out: it read named-graph bundles fine but
//! the decoder rejected them, because the base-index reader does not put a
//! graph on the rows it decodes and `EdgeKey::from_reifies_facts` read the
//! resulting disagreement as a forged bundle (#1882). All three sources now
//! agree, which is what the kill switch exists to let you check, and this is
//! the wire-level guard on that. `export_omission_headers` holds the arena
//! and overlay halves.

use axum::body::Body;
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

const UNRESOLVED: &str = "x-fluree-export-annotations-unresolved";

/// An edge annotation written inside a named graph — the shape the three
/// annotation sources disagree about.
const ANNOTATED_NAMED_GRAPH: &str = "@prefix ex: <http://example.org/> .\n\
     GRAPH <http://example.org/g1> { ex:x ex:p ex:y ~ ex:cG {| ex:src ex:d |} . }\n";

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

async fn create_ledger(app: &axum::Router, ledger: &str) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "ledger": ledger }).to_string()))
                .expect("request"),
        )
        .await
        .expect("router response");
    assert!(
        resp.status().is_success(),
        "create {ledger}: {}",
        resp.status()
    );
}

/// Upsert TriG, which is the only HTTP surface that can place a named graph.
async fn upsert_trig(app: &axum::Router, ledger: &str, body: &str) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/upsert/{ledger}"))
                .header("content-type", "application/trig")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("router response");
    let status = resp.status();
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    assert!(
        status.is_success(),
        "upsert trig into {ledger}: {status} {}",
        String::from_utf8_lossy(&bytes)
    );
}

/// `POST /reindex` is synchronous, so a test can move a ledger from the
/// novelty overlay to a sealed index without racing a background indexer.
async fn reindex(app: &axum::Router, ledger: &str) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/reindex")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "ledger": ledger }).to_string()))
                .expect("request"),
        )
        .await
        .expect("router response");
    let status = resp.status();
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    assert!(
        status.is_success(),
        "reindex {ledger}: {status} {}",
        String::from_utf8_lossy(&bytes)
    );
}

/// `(status, headers, body)` from a real `POST /export`.
async fn export(
    app: &axum::Router,
    ledger: &str,
    body: serde_json::Value,
) -> (StatusCode, http::HeaderMap, String) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/export/{ledger}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("router response");
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    (
        status,
        headers,
        String::from_utf8_lossy(&bytes).into_owned(),
    )
}

fn header(headers: &http::HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .map(|v| v.to_str().expect("header is ascii").to_string())
}

#[tokio::test]
async fn the_base_index_scan_resolves_a_named_graph_annotation() {
    std::env::set_var("FLUREE_EXPORT_ANNOTATION_SCAN", "1");

    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "scan:main").await;
    upsert_trig(&app, "scan:main", ANNOTATED_NAMED_GRAPH).await;
    // Seal an index, so the scan has a base to read and the overlay — which is
    // *not* blind — is no longer the source that answers.
    reindex(&app, "scan:main").await;

    let (status, headers, body) = export(
        &app,
        "scan:main",
        json!({ "format": "trig", "all_graphs": true }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("http://example.org/g1"),
        "the graph itself must still export, or the header below would be \
         reporting on an export that produced nothing: {body}"
    );
    assert!(
        body.contains("~ <http://example.org/cG>"),
        "the scan must emit the marker, as the other two sources do: {body}"
    );
    assert!(
        body.contains("<http://example.org/src> <http://example.org/d>"),
        "and the reifier's own description must be in scope: {body}"
    );
    assert_eq!(
        header(&headers, UNRESOLVED),
        None,
        "nothing is dropped, so nothing to report; headers: {headers:?}"
    );

    // `raw_reifies` still emits the bundle verbatim for consumers pinned to
    // pre-4.2 bytes, and still reports nothing.
    let (status, headers, body) = export(
        &app,
        "scan:main",
        json!({ "format": "trig", "all_graphs": true, "raw_reifies": true }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("reifiesSubject"),
        "raw_reifies must emit the bundle verbatim: {body}"
    );
    assert_eq!(
        header(&headers, UNRESOLVED),
        None,
        "raw_reifies drops nothing, so reports nothing; headers: {headers:?}"
    );
}
