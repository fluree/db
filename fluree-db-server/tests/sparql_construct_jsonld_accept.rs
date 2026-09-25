//! HTTP-layer regression coverage for issue #1274: a SPARQL CONSTRUCT served
//! through `POST /v1/fluree/query/<ledger>` must return the constructed graph as
//! JSON-LD for the default (no `Accept`), `application/ld+json`, and
//! `application/json` cases — not a self-contradictory
//! `400 "CONSTRUCT queries only support JSON-LD output format"`. The explicit
//! `application/rdf+xml` path must keep working as the graph alternative, and
//! `text/turtle` / `application/n-triples` serve the graph as Turtle / N-Triples
//! on every ledger-scoped path that serves RDF/XML.

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
    post_sparql_with(state, ledger, sparql, accept, &[]).await
}

async fn post_sparql_with(
    state: &Arc<AppState>,
    ledger: &str,
    sparql: &str,
    accept: Option<&str>,
    headers: &[(&str, &str)],
) -> (StatusCode, String, Vec<u8>) {
    let mut builder = Request::builder()
        .method("POST")
        .uri(format!("/v1/fluree/query/{ledger}"))
        .header("content-type", "application/sparql-query");
    if let Some(a) = accept {
        builder = builder.header("accept", a);
    }
    for (name, value) in headers {
        builder = builder.header(*name, *value);
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

// ---------------------------------------------------------------------------
// Turtle and N-Triples
// ---------------------------------------------------------------------------

const PREFIXED_CONSTRUCT: &str =
    "PREFIX schema: <http://schema.org/> CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }";

/// Parse a Turtle or N-Triples body back into triples.
fn parse_rdf(body: &[u8]) -> fluree_graph_ir::Graph {
    let text = std::str::from_utf8(body).expect("UTF-8 body");
    let mut sink = fluree_graph_ir::GraphCollectorSink::new();
    fluree_graph_turtle::parse(text, &mut sink)
        .unwrap_or_else(|e| panic!("body must parse as Turtle: {e}\n{text}"));
    sink.into_graph()
}

#[tokio::test]
async fn construct_accept_turtle_and_ntriples() {
    let (_tmp, state) = server_state().await;
    let ledger = "test/construct-ttl:main";
    seed(&state, ledger).await;

    let (status, content_type, body) =
        post_sparql(&state, ledger, PREFIXED_CONSTRUCT, Some("text/turtle")).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(content_type, "text/turtle; charset=utf-8");
    assert_eq!(parse_rdf(&body).len(), 2, "type + name");
    let ttl = String::from_utf8(body).unwrap();
    assert!(
        ttl.starts_with("@prefix schema: <http://schema.org/> ."),
        "{ttl}"
    );
    assert!(
        ttl.contains("<http://ex.org/alice> a schema:Person ;\n    schema:name \"Alice\" ."),
        "{ttl}"
    );

    let (status, content_type, body) = post_sparql(
        &state,
        ledger,
        PREFIXED_CONSTRUCT,
        Some("application/n-triples"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(content_type, "application/n-triples; charset=utf-8");
    assert_eq!(parse_rdf(&body).len(), 2);
    let nt = String::from_utf8(body).unwrap();
    assert!(
        nt.lines()
            .any(|l| l == "<http://ex.org/alice> <http://schema.org/name> \"Alice\" ."),
        "{nt}"
    );
}

#[tokio::test]
async fn describe_accept_turtle() {
    let (_tmp, state) = server_state().await;
    let ledger = "test/describe-ttl:main";
    seed(&state, ledger).await;

    let (status, content_type, body) = post_sparql(
        &state,
        ledger,
        "DESCRIBE <http://ex.org/alice>",
        Some("text/turtle"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert!(content_type.starts_with("text/turtle"), "{content_type}");
    let graph = parse_rdf(&body);
    assert!(
        graph.iter().any(|t| t
            .object()
            .as_literal()
            .is_some_and(|(v, _, _)| v.lexical() == "Alice")),
        "{}",
        String::from_utf8_lossy(&body)
    );
}

/// The highest-`q` graph format wins; equal weights keep the header's order.
#[tokio::test]
async fn construct_accept_negotiates_by_q() {
    let (_tmp, state) = server_state().await;
    let ledger = "test/construct-q:main";
    seed(&state, ledger).await;

    for (accept, expected) in [
        ("application/ld+json;q=0.5, text/turtle", "text/turtle"),
        (
            "text/turtle;q=0.1, application/ld+json",
            "application/ld+json",
        ),
        (
            "application/n-triples, text/turtle",
            "application/n-triples",
        ),
        ("text/*", "text/turtle"),
    ] {
        let (status, content_type, body) =
            post_sparql(&state, ledger, CONSTRUCT, Some(accept)).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{accept}: {}",
            String::from_utf8_lossy(&body)
        );
        assert!(
            content_type.starts_with(expected),
            "{accept} → {content_type}"
        );
    }
}

/// A solution table has no Turtle form: a SELECT that accepts only graph
/// formats is a 406, while one that also accepts a results format is served.
#[tokio::test]
async fn select_accept_only_graph_formats_is_406() {
    let (_tmp, state) = server_state().await;
    let ledger = "test/select-ttl:main";
    seed(&state, ledger).await;
    let select = "SELECT ?s WHERE { ?s ?p ?o }";

    for accept in [
        "text/turtle",
        "application/n-triples",
        "text/turtle, application/rdf+xml",
    ] {
        let (status, _, _) = post_sparql(&state, ledger, select, Some(accept)).await;
        assert_eq!(status, StatusCode::NOT_ACCEPTABLE, "{accept}");
    }
    let (status, content_type, _) = post_sparql(
        &state,
        ledger,
        select,
        Some("application/sparql-results+json, text/turtle;q=0.1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        content_type.starts_with("application/json"),
        "{content_type}"
    );
}

/// Graph formats are served on the policy-scoped path (policy inputs, no
/// dataset clause) and the dataset path (FROM), not just the plain one.
#[tokio::test]
async fn construct_graph_formats_on_policy_and_dataset_paths() {
    let (_tmp, state) = server_state().await;
    let ledger = "test/construct-paths:main";
    seed(&state, ledger).await;

    for format in [
        "text/turtle",
        "application/n-triples",
        "application/rdf+xml",
    ] {
        let (status, content_type, body) = post_sparql_with(
            &state,
            ledger,
            CONSTRUCT,
            Some(format),
            &[("fluree-default-allow", "true")],
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "policy path, {format}: {}",
            String::from_utf8_lossy(&body)
        );
        assert!(
            content_type.starts_with(format),
            "{format} → {content_type}"
        );
    }

    let from = format!("CONSTRUCT {{ ?s ?p ?o }} FROM <{ledger}> WHERE {{ ?s ?p ?o }}");
    let (status, content_type, body) =
        post_sparql(&state, ledger, &from, Some("text/turtle")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "dataset path: {}",
        String::from_utf8_lossy(&body)
    );
    assert!(content_type.starts_with("text/turtle"), "{content_type}");
    assert_eq!(parse_rdf(&body).len(), 2);
}

#[tokio::test]
async fn connection_construct_turtle_is_406() {
    let (_tmp, state) = server_state().await;
    let ledger = "test/connection-ttl:main";
    seed(&state, ledger).await;

    let construct = format!("CONSTRUCT {{ ?s ?p ?o }} FROM <{ledger}> WHERE {{ ?s ?p ?o }}");
    let (status, _, _) = post_connection_sparql(&state, &construct, Some("text/turtle")).await;
    assert_eq!(status, StatusCode::NOT_ACCEPTABLE);
}

/// A template with a `GRAPH` block produces a dataset: TriG, N-Quads and
/// JSON-LD carry it, and an `Accept` that admits only triples formats is a 406.
#[tokio::test]
async fn construct_graph_template_negotiates_dataset_formats() {
    let (_tmp, state) = server_state().await;
    let ledger = "test/construct-dataset:main";
    seed(&state, ledger).await;
    let construct = "PREFIX schema: <http://schema.org/> \
                     CONSTRUCT { GRAPH <http://ex.org/names> { ?s schema:name ?n } } \
                     WHERE { ?s schema:name ?n }";

    let (status, content_type, body) =
        post_sparql(&state, ledger, construct, Some("application/n-quads")).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(content_type, "application/n-quads; charset=utf-8");
    assert_eq!(
        String::from_utf8(body).unwrap(),
        "<http://ex.org/alice> <http://schema.org/name> \"Alice\" <http://ex.org/names> .\n"
    );

    let (status, content_type, body) =
        post_sparql(&state, ledger, construct, Some("application/trig")).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(content_type, "application/trig; charset=utf-8");
    assert!(
        String::from_utf8_lossy(&body).contains("GRAPH <http://ex.org/names> {"),
        "{}",
        String::from_utf8_lossy(&body)
    );

    // Among the Accept ranges, only the dataset formats are candidates.
    let (status, content_type, _) = post_sparql(
        &state,
        ledger,
        construct,
        Some("text/turtle, application/n-quads;q=0.5"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        content_type.starts_with("application/n-quads"),
        "{content_type}"
    );

    let (status, content_type, body) = post_sparql(&state, ledger, construct, None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        content_type.starts_with("application/ld+json"),
        "{content_type}"
    );
    let doc: JsonValue = serde_json::from_slice(&body).unwrap();
    assert!(
        doc["@graph"]
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n["@id"] == "http://ex.org/names" && n["@graph"].is_array()),
        "{doc}"
    );

    for accept in [
        "text/turtle",
        "application/n-triples",
        "application/rdf+xml",
    ] {
        let (status, _, body) = post_sparql(&state, ledger, construct, Some(accept)).await;
        assert_eq!(
            status,
            StatusCode::NOT_ACCEPTABLE,
            "{accept}: {}",
            String::from_utf8_lossy(&body)
        );
    }
}
