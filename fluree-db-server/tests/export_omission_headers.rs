//! `POST /export` must tell the client what it left out.
//!
//! #1847's finding was "nothing in the output to suggest anything is missing".
//! The CLI answers that on stderr; the HTTP surface answers it with response
//! headers, and *those* need a test that crosses the wire. Asserting the
//! headers exist in the code that builds them proves only that the code was
//! written — a rename, a reordering against `Response::builder`, or a routing
//! change would all still pass. These drive the real route through the real
//! router and read the real response.
//!
//! Every assertion about an absent header is paired with one about the body
//! that should be there: an export that produced nothing would satisfy all
//! three absence checks for entirely the wrong reason.
//!
//! ## Reachability of the three counters over HTTP
//!
//! - `x-fluree-export-named-graphs-omitted` — asserted both ways here.
//! - `x-fluree-export-annotations-unresolved` — asserted absent here, on both
//!   annotation sources an HTTP client can reach. Its non-zero case needs the
//!   base-index scan, which requires a process-global env var, so it lives in
//!   the standalone `export_scan_source` target.
//! - `x-fluree-export-annotations-out-of-scope` — emitted, and asserted
//!   absent, but **not asserted non-zero**. The counter is
//!   `named − in_scope`: a reifier a marker pointed at whose own bundle the
//!   export never saw. The write path co-locates the two — an annotation's
//!   `f:reifies*` rows are written into the same graph as the edge they
//!   describe (`EdgeKey::to_reifies_facts`) — so no selection of graphs can
//!   include the marker and exclude the bundle. Like the two below, it is a
//!   corruption-class guard rather than a reachable state.
//! - `x-fluree-export-rows-skipped` — asserted absent. Every site that
//!   increments it is a dictionary miss (a subject or predicate id with no
//!   IRI), which no well-formed request produces; reaching it over the wire
//!   would mean writing a corrupt store, so this file pins the clean case and
//!   leaves the counter itself to the export unit tests.

use axum::body::Body;
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

const OMITTED: &str = "x-fluree-export-named-graphs-omitted";
const UNRESOLVED: &str = "x-fluree-export-annotations-unresolved";
const SKIPPED: &str = "x-fluree-export-rows-skipped";
const OUT_OF_SCOPE: &str = "x-fluree-export-annotations-out-of-scope";

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

/// A dataset-format export that left the ledger's named graph behind must say
/// so on the response.
#[tokio::test]
async fn export_reports_named_graphs_it_omitted() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "omit:main").await;
    upsert_trig(
        &app,
        "omit:main",
        "@prefix ex: <http://example.org/> .\n\
         ex:top ex:p \"in-default\" .\n\
         GRAPH <http://example.org/g1> { ex:s ex:p \"in-g1\" . }\n",
    )
    .await;

    let (status, headers, body) = export(&app, "omit:main", json!({ "format": "trig" })).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("in-default"),
        "the default graph must export, or the header below would be \
         reporting on an export that produced nothing: {body}"
    );
    assert!(
        !body.contains("in-g1"),
        "the named graph must be left out: {body}"
    );
    assert_eq!(
        header(&headers, OMITTED).as_deref(),
        Some("1"),
        "the dropped named graph must be reported; headers: {headers:?}"
    );

    // With the graph actually included there is nothing to report.
    let (status, headers, body) = export(
        &app,
        "omit:main",
        json!({ "format": "trig", "all_graphs": true }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("in-g1"),
        "the named graph must export: {body}"
    );
    assert_eq!(
        header(&headers, OMITTED),
        None,
        "a complete export must carry no omission header; headers: {headers:?}"
    );
}

/// A clean export carries none of the three, so their presence is a signal
/// rather than noise.
#[tokio::test]
async fn a_complete_export_carries_no_omission_headers() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "clean:main").await;
    upsert_trig(
        &app,
        "clean:main",
        "@prefix ex: <http://example.org/> .\nex:a ex:p \"v\" .\n",
    )
    .await;

    let (status, headers, body) = export(&app, "clean:main", json!({ "format": "turtle" })).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("\"v\""),
        "the export must contain the data, or the absences below prove nothing: {body}"
    );
    for name in [OMITTED, UNRESOLVED, SKIPPED] {
        assert_eq!(
            header(&headers, name),
            None,
            "{name} must be absent on a complete export; headers: {headers:?}"
        );
    }
}

/// Both annotation sources an HTTP client can reach — the novelty overlay
/// before an index exists, and the sealed arena after `POST /reindex` — carry
/// annotations written inside a named graph. Neither drops anything, so
/// neither reports anything.
///
/// The pairing is the point. A test that only asserts the header is absent
/// passes on any fixture that never reached the code emitting it; this one
/// asserts the marker is present in the body for the same fixture, and its
/// counterpart in `export_scan_source` shows the header firing on the one
/// source that does drop it.
#[tokio::test]
async fn an_annotated_named_graph_resolves_from_both_reachable_sources() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "ann:main").await;
    upsert_trig(&app, "ann:main", ANNOTATED_NAMED_GRAPH).await;

    for stage in ["novelty overlay", "sealed arena"] {
        if stage == "sealed arena" {
            reindex(&app, "ann:main").await;
        }
        let (status, headers, body) = export(
            &app,
            "ann:main",
            json!({ "format": "trig", "all_graphs": true }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{stage}");
        assert!(
            body.contains("~ <http://example.org/cG>"),
            "{stage}: the marker must be emitted inline: {body}"
        );
        assert!(
            body.contains("<http://example.org/src> <http://example.org/d>"),
            "{stage}: the reifier's own description must be in scope: {body}"
        );
        assert_eq!(
            header(&headers, UNRESOLVED),
            None,
            "{stage}: nothing was dropped, so nothing to report; headers: {headers:?}"
        );
    }
}

/// A targeted single-graph export has omitted nothing.
///
/// `omitted_named_graph_count` only zeroed under `all_graphs`, so asking for
/// one graph by IRI reported every *other* user graph as omitted — a client
/// acting on that header saw a false positive on every targeted request.
/// The CLI gates the same warning on `all_graphs || graph.is_some()` and
/// correctly stays quiet; the two surfaces now agree.
#[tokio::test]
async fn a_targeted_single_graph_export_reports_no_omission() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "tgt:main").await;
    upsert_trig(
        &app,
        "tgt:main",
        "@prefix ex: <http://example.org/> .\n\
         GRAPH <http://example.org/g1> { ex:a ex:p \"in-g1\" . }\n\
         GRAPH <http://example.org/g2> { ex:b ex:p \"in-g2\" . }\n\
         GRAPH <http://example.org/g3> { ex:c ex:p \"in-g3\" . }\n",
    )
    .await;

    let (status, headers, body) = export(
        &app,
        "tgt:main",
        json!({ "format": "trig", "graph": "http://example.org/g1" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("in-g1"),
        "the requested graph must be in the body, or the absence below is vacuous: {body}"
    );
    assert!(!body.contains("in-g2"), "only the requested graph: {body}");
    assert_eq!(
        header(&headers, OMITTED),
        None,
        "a targeted export dropped nothing it was asked for; headers: {headers:?}"
    );

    // The untargeted default-graph-only export still reports, which is the
    // case the header exists for and the control that keeps this honest.
    let (_, headers, _) = export(&app, "tgt:main", json!({ "format": "trig" })).await;
    assert_eq!(
        header(&headers, OMITTED).as_deref(),
        Some("3"),
        "an unasked-for drop must still be reported; headers: {headers:?}"
    );
}
