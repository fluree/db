//! HTTP-layer coverage for the single-target graph-source query fallback in
//! `execute_query` (JSON-LD) and `execute_sparql_ledger` (SPARQL).
//!
//! Two properties, previously inconsistent between the two handlers:
//!   - A registered graph source requested in a delimited (CSV/TSV) format is
//!     rejected with an explicit `406 "… format not supported for graph source
//!     queries"`, not a misleading `404`.
//!   - A genuinely-missing ledger keeps its `404 NotFound` (it is not a graph
//!     source), rather than being reported as a format/dataset error.
//!
//! The graph source is registered with a bogus catalog: registration completes
//! before any catalog call, and these requests are rejected at format
//! negotiation before the source is ever scanned, so no live catalog is needed.
#![cfg(feature = "iceberg")]

use axum::body::Body;
use fluree_db_api::R2rmlCreateConfig;
use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

const MAPPING_TTL: &str = r#"
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.org/> .

<http://example.org/mapping#M> a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "openflights.airlines" ] ;
    rr:subjectMap [
        rr:template "http://example.org/airline/{id}" ;
        rr:class ex:Airline
    ] ;
    rr:predicateObjectMap [
        rr:predicate ex:name ;
        rr:objectMap [ rr:column "name" ]
    ] .
"#;

async fn state_with_graph_source() -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));

    state
        .fluree
        .create_r2rml_graph_source(
            R2rmlCreateConfig::new(
                "gs",
                "https://example.invalid",
                "openflights.airlines",
                MAPPING_TTL,
            )
            .with_mapping_media_type("text/turtle"),
        )
        .await
        .expect("graph source registration should succeed");

    (tmp, state)
}

async fn body_text(resp: http::Response<Body>) -> (StatusCode, String) {
    let status = resp.status();
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// JSON-LD: a graph source requested as CSV must be a 406 with the explicit
/// graph-source format message — previously this path returned a 404 because
/// the fallback was gated on `delimited.is_none()`.
#[tokio::test]
async fn jsonld_graph_source_delimited_is_406_not_404() {
    let (_tmp, state) = state_with_graph_source().await;

    let body = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?s"],
        "where": [["?s", "a", "ex:Airline"]]
    });
    let resp = build_router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query/gs:main")
                .header("content-type", "application/json")
                .header("accept", "text/csv")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, text) = body_text(resp).await;
    assert_eq!(
        status,
        StatusCode::NOT_ACCEPTABLE,
        "graph-source + CSV should be 406, got {status}: {text}"
    );
    assert!(
        text.contains("format not supported for graph source queries"),
        "expected the explicit graph-source format message, got: {text}"
    );
}

/// SPARQL sibling: same explicit 406 for a graph source requested as CSV.
#[tokio::test]
async fn sparql_graph_source_delimited_is_406_not_404() {
    let (_tmp, state) = state_with_graph_source().await;

    let resp = build_router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query/gs:main")
                .header("content-type", "application/sparql-query")
                .header("accept", "text/csv")
                .body(Body::from("SELECT ?s WHERE { ?s ?p ?o } LIMIT 1"))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, text) = body_text(resp).await;
    assert_eq!(
        status,
        StatusCode::NOT_ACCEPTABLE,
        "graph-source + CSV should be 406, got {status}: {text}"
    );
    assert!(
        text.contains("format not supported for graph source queries"),
        "expected the explicit graph-source format message, got: {text}"
    );
}

/// A genuinely-missing ledger requested as CSV must keep its 404 — it is not a
/// graph source, so it must not borrow the graph-source format message.
#[tokio::test]
async fn jsonld_missing_ledger_delimited_is_404() {
    let (_tmp, state) = state_with_graph_source().await;

    let body = json!({ "select": ["?s"], "where": [["?s", "?p", "?o"]] });
    let resp = build_router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query/doesnotexist:main")
                .header("content-type", "application/json")
                .header("accept", "text/csv")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, text) = body_text(resp).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "a missing ledger should be 404, got {status}: {text}"
    );
}
