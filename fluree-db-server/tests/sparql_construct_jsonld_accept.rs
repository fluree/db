//! HTTP-layer regression coverage for issue #1274: a SPARQL CONSTRUCT served
//! through `POST /v1/fluree/query/<ledger>` must return the constructed graph as
//! JSON-LD for the default (no `Accept`), `application/ld+json`, and
//! `application/json` cases — not a self-contradictory
//! `400 "CONSTRUCT queries only support JSON-LD output format"`. The explicit
//! `application/rdf+xml` path must keep working as the graph alternative.

use axum::body::Body;
use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

async fn server_state() -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState"));
    (tmp, state)
}

async fn create_ledger(state: &Arc<AppState>, ledger: &str) {
    let resp = build_router(Arc::clone(state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "ledger": ledger }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "create {ledger}");
}

async fn insert(state: &Arc<AppState>, ledger: &str, body: JsonValue) {
    let resp = build_router(Arc::clone(state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/insert/{ledger}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(resp.status().is_success(), "insert into {ledger}");
}

async fn seed(state: &Arc<AppState>, ledger: &str) {
    create_ledger(state, ledger).await;
    insert(
        state,
        ledger,
        json!({
            "@context": {"schema": "http://schema.org/", "id": "@id", "type": "@type"},
            "@graph": [{"@id": "http://ex.org/alice", "@type": "schema:Person", "schema:name": "Alice"}]
        }),
    )
    .await;
}

/// POST a raw SPARQL query to the ledger-scoped route with an optional `Accept`.
/// Returns `(status, content_type, raw_body_bytes)`.
async fn post_sparql(
    state: &Arc<AppState>,
    ledger: &str,
    sparql: &str,
    accept: Option<&str>,
) -> (StatusCode, String, Vec<u8>) {
    let mut builder = Request::builder()
        .method("POST")
        .uri(format!("/v1/fluree/query/{ledger}"))
        .header("content-type", "application/sparql-query");
    if let Some(a) = accept {
        builder = builder.header("accept", a);
    }
    let resp = build_router(Arc::clone(state))
        .oneshot(builder.body(Body::from(sparql.to_string())).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let content_type = resp
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    (status, content_type, bytes)
}

/// POST a raw SPARQL query to the connection-scoped route (`/v1/fluree/query`,
/// no path ledger) with an optional `Accept`. Returns `(status, content_type)`.
async fn post_connection_sparql(
    state: &Arc<AppState>,
    sparql: &str,
    accept: Option<&str>,
) -> (StatusCode, String, Vec<u8>) {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/v1/fluree/query")
        .header("content-type", "application/sparql-query");
    if let Some(a) = accept {
        builder = builder.header("accept", a);
    }
    let resp = build_router(Arc::clone(state))
        .oneshot(builder.body(Body::from(sparql.to_string())).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let content_type = resp
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    (status, content_type, bytes)
}

const CONSTRUCT: &str = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o } LIMIT 3";

/// Assert the body is a JSON-LD graph object (`{ "@graph": [ { "@id": ... } ] }`).
fn assert_jsonld_graph(bytes: &[u8]) {
    let body: JsonValue = serde_json::from_slice(bytes).expect("JSON body");
    let graph = body
        .get("@graph")
        .and_then(JsonValue::as_array)
        .unwrap_or_else(|| panic!("expected @graph array, got: {body}"));
    assert!(!graph.is_empty(), "expected constructed triples: {body}");
    assert!(
        graph.iter().all(|n| n.get("@id").is_some()),
        "each node carries an @id: {body}"
    );
}

#[tokio::test]
async fn construct_no_accept_returns_jsonld() {
    let (_tmp, state) = server_state().await;
    let ledger = "test/construct:main";
    seed(&state, ledger).await;

    let (status, content_type, body) = post_sparql(&state, ledger, CONSTRUCT, None).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "no-Accept CONSTRUCT must not 400: {}",
        String::from_utf8_lossy(&body)
    );
    assert!(
        content_type.contains("application/ld+json"),
        "graph response labelled JSON-LD, got: {content_type}"
    );
    assert_jsonld_graph(&body);
}

#[tokio::test]
async fn construct_accept_ld_json_returns_jsonld() {
    let (_tmp, state) = server_state().await;
    let ledger = "test/construct:main";
    seed(&state, ledger).await;

    let (status, content_type, body) =
        post_sparql(&state, ledger, CONSTRUCT, Some("application/ld+json")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "application/ld+json CONSTRUCT must succeed: {}",
        String::from_utf8_lossy(&body)
    );
    assert!(
        content_type.contains("application/ld+json"),
        "{content_type}"
    );
    assert_jsonld_graph(&body);
}

#[tokio::test]
async fn construct_accept_plain_json_returns_jsonld() {
    let (_tmp, state) = server_state().await;
    let ledger = "test/construct:main";
    seed(&state, ledger).await;

    let (status, _content_type, body) =
        post_sparql(&state, ledger, CONSTRUCT, Some("application/json")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "application/json CONSTRUCT must succeed (graph coerced to JSON-LD): {}",
        String::from_utf8_lossy(&body)
    );
    assert_jsonld_graph(&body);
}

#[tokio::test]
async fn construct_accept_rdf_xml_still_works() {
    let (_tmp, state) = server_state().await;
    let ledger = "test/construct:main";
    seed(&state, ledger).await;

    let (status, content_type, body) =
        post_sparql(&state, ledger, CONSTRUCT, Some("application/rdf+xml")).await;
    assert_eq!(status, StatusCode::OK, "rdf+xml CONSTRUCT must still work");
    assert!(
        content_type.contains("application/rdf+xml"),
        "{content_type}"
    );
    let xml = String::from_utf8_lossy(&body);
    assert!(
        xml.contains("<rdf:RDF") || xml.contains("rdf:RDF"),
        "RDF/XML body: {xml}"
    );
}

/// A SELECT must NOT be flipped to JSON-LD by a bare `application/json` Accept;
/// only `application/ld+json` opts a SELECT into JSON-LD. Bare json keeps the
/// SPARQL-results-JSON shape (`{ "head": ..., "results": ... }`).
#[tokio::test]
async fn select_plain_json_stays_sparql_results_json() {
    let (_tmp, state) = server_state().await;
    let ledger = "test/construct:main";
    seed(&state, ledger).await;

    let select = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 3";
    let (status, _ct, body) = post_sparql(&state, ledger, select, Some("application/json")).await;
    assert_eq!(status, StatusCode::OK);
    let json: JsonValue = serde_json::from_slice(&body).expect("JSON body");
    assert!(
        json.get("head").is_some() && json.get("results").is_some(),
        "SELECT under application/json stays SPARQL-results JSON, got: {json}"
    );
}

/// A graph query under `Accept: application/sparql-results+xml` has no
/// solution-table form — the route must reject it with `406 Not Acceptable`
/// (matching the documented negotiation matrix), not execute into a 400.
#[tokio::test]
async fn construct_accept_sparql_results_xml_is_406() {
    let (_tmp, state) = server_state().await;
    let ledger = "test/construct:main";
    seed(&state, ledger).await;

    let (status, _ct, _body) = post_sparql(
        &state,
        ledger,
        CONSTRUCT,
        Some("application/sparql-results+xml"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_ACCEPTABLE,
        "CONSTRUCT + SPARQL Results XML must be 406, not a format 400"
    );
}

/// CSV/TSV serialize a solution table; a graph query must be rejected with `406`,
/// not executed into a malformed body or a 500.
#[tokio::test]
async fn construct_accept_csv_is_406() {
    let (_tmp, state) = server_state().await;
    let ledger = "test/construct:main";
    seed(&state, ledger).await;

    let (status, _ct, _body) = post_sparql(&state, ledger, CONSTRUCT, Some("text/csv")).await;
    assert_eq!(
        status,
        StatusCode::NOT_ACCEPTABLE,
        "CONSTRUCT + CSV must be 406"
    );
}

/// AgentJson is a solution-table envelope; a graph query must be rejected with
/// `406`, not served as raw JSON-LD mislabelled `application/vnd.fluree.agent+json`.
#[tokio::test]
async fn construct_accept_agent_json_is_406() {
    let (_tmp, state) = server_state().await;
    let ledger = "test/construct:main";
    seed(&state, ledger).await;

    let (status, _ct, _body) = post_sparql(
        &state,
        ledger,
        CONSTRUCT,
        Some("application/vnd.fluree.agent+json"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_ACCEPTABLE,
        "CONSTRUCT + AgentJson must be 406, not a mislabelled JSON-LD graph"
    );
}

/// Connection-scoped `/v1/fluree/query` (SPARQL with FROM): CONSTRUCT still
/// returns a JSON-LD graph (the JSON-family columns of the matrix apply here).
#[tokio::test]
async fn connection_construct_returns_jsonld() {
    let (_tmp, state) = server_state().await;
    let ledger = "test/construct:main";
    seed(&state, ledger).await;

    let sparql = format!("CONSTRUCT {{ ?s ?p ?o }} FROM <{ledger}> WHERE {{ ?s ?p ?o }}");
    let (status, content_type, body) = post_connection_sparql(&state, &sparql, None).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "connection CONSTRUCT must not 400: {}",
        String::from_utf8_lossy(&body)
    );
    assert!(
        content_type.contains("application/ld+json"),
        "graph response labelled JSON-LD, got: {content_type}"
    );
    assert_jsonld_graph(&body);
}

/// Connection-scoped route does not negotiate byte formats — RDF/XML and
/// SPARQL-results XML are rejected with 406 (not silently downgraded to JSON),
/// pointing callers at the ledger-scoped route.
#[tokio::test]
async fn connection_byte_formats_are_406() {
    let (_tmp, state) = server_state().await;
    let ledger = "test/construct:main";
    seed(&state, ledger).await;

    let construct = format!("CONSTRUCT {{ ?s ?p ?o }} FROM <{ledger}> WHERE {{ ?s ?p ?o }}");
    let (status, _ct, _body) =
        post_connection_sparql(&state, &construct, Some("application/rdf+xml")).await;
    assert_eq!(
        status,
        StatusCode::NOT_ACCEPTABLE,
        "connection CONSTRUCT + rdf+xml must be 406 (use ledger-scoped route)"
    );

    let select = format!("SELECT ?s ?p ?o FROM <{ledger}> WHERE {{ ?s ?p ?o }}");
    let (status, _ct, _body) =
        post_connection_sparql(&state, &select, Some("application/sparql-results+xml")).await;
    assert_eq!(
        status,
        StatusCode::NOT_ACCEPTABLE,
        "connection SELECT + sparql-results+xml must be 406 (use ledger-scoped route)"
    );
}
