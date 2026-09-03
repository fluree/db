//! Ledger-config defaults over the real HTTP ledger route
//! (`POST /v1/fluree/query/{ledger}`).
//!
//! `fluree/db#1577`: a ledger configured with `f:reasoningDefaults` served
//! non-entailed results until the reporter added `# PRAGMA reasoning: rdfs`
//! to every query. The cause was that the ledger route builds its view
//! straight from the loaded `LedgerState`, which carries no resolved config,
//! so nothing applied the ledger's defaults before the query ran.
//!
//! `it_config_graph` pins the same gap at the library level, against a view
//! built by `GraphDb::from_ledger_state`. These tests pin it over the real
//! HTTP route, which is where the reporter hit it.

use axum::body::Body;
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

const LEDGER: &str = "cfgdefaults:main";

/// `ex:childName rdfs:subPropertyOf ex:name`, so a query for `ex:name`
/// finds "Alice" only when RDFS reasoning is engaged.
const SEED_TRIG: &str = r#"
@prefix ex: <http://example.org/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:childName rdfs:subPropertyOf ex:name .
ex:alice ex:childName "Alice" .
"#;

/// The reporter's config: RDFS by default, with an `f:schemaSource`
/// GraphRef selecting the ledger's default graph.
const CONFIG_TRIG: &str = r"
@prefix f: <https://ns.flur.ee/db#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

GRAPH <urn:fluree:cfgdefaults:main#config> {
    <urn:cfgdefaults:config> rdf:type f:LedgerConfig .
    <urn:cfgdefaults:config> f:reasoningDefaults <urn:cfgdefaults:reasoning> .
    <urn:cfgdefaults:reasoning> f:reasoningModes f:RDFS .
    <urn:cfgdefaults:reasoning> f:schemaSource <urn:cfgdefaults:schema> .
    <urn:cfgdefaults:schema> rdf:type f:GraphRef .
    <urn:cfgdefaults:schema> f:graphSource <urn:cfgdefaults:schema:graph> .
    <urn:cfgdefaults:schema:graph> f:graphSelector f:defaultGraph .
}
";

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

async fn upsert_trig(app: &axum::Router, trig: &str) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/upsert/{LEDGER}"))
                .header("content-type", "application/trig")
                .body(Body::from(trig.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "trig upsert");
}

/// Create the ledger, seed the ontology + instance data, and write the
/// reasoning config into the config graph.
async fn seeded_app() -> (TempDir, axum::Router) {
    let (tmp, state) = test_state().await;
    let app = build_router(state);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "ledger": LEDGER }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "create ledger");

    upsert_trig(&app, SEED_TRIG).await;
    upsert_trig(&app, CONFIG_TRIG).await;

    (tmp, app)
}

async fn post(app: &axum::Router, content_type: &str, body: String) -> (StatusCode, JsonValue) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/query/{LEDGER}"))
                .header("content-type", content_type)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: JsonValue = serde_json::from_slice(&bytes).expect("valid JSON response");
    (status, json)
}

/// SELECT over this route defaults to SPARQL-results JSON.
fn bindings(json: &JsonValue) -> &Vec<JsonValue> {
    json.get("results")
        .and_then(|r| r.get("bindings"))
        .and_then(JsonValue::as_array)
        .unwrap_or_else(|| panic!("expected SPARQL-results bindings, got {json}"))
}

const ENTAILED_SELECT: &str = r"PREFIX ex: <http://example.org/>
SELECT ?v WHERE { ex:alice ex:name ?v }";

/// Control: the per-query pragma the reporter fell back to. Establishes that
/// the data, the ontology, and the reasoner all work over this route — the
/// only variable left is where the modes come from.
#[tokio::test]
async fn sparql_pragma_engages_reasoning_on_ledger_route() {
    let (_tmp, app) = seeded_app().await;

    let sparql = format!("# PRAGMA reasoning: rdfs\n{ENTAILED_SELECT}");
    let (status, json) = post(&app, "application/sparql-query", sparql).await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(bindings(&json).len(), 1, "pragma should entail: {json}");
}

/// The reporter's case: same query, modes from `f:reasoningDefaults` instead
/// of the pragma.
#[tokio::test]
async fn sparql_config_reasoning_defaults_apply_on_ledger_route() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = post(
        &app,
        "application/sparql-query",
        ENTAILED_SELECT.to_string(),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(
        bindings(&json).len(),
        1,
        "config reasoning defaults should engage without a pragma: {json}"
    );
}

/// The same gap on the JSON-LD front-end of the same route.
#[tokio::test]
async fn jsonld_config_reasoning_defaults_apply_on_ledger_route() {
    let (_tmp, app) = seeded_app().await;

    let query = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?v",
        "where": {"@id": "ex:alice", "ex:name": "?v"}
    });
    let (status, json) = post(&app, "application/json", query.to_string()).await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(
        json,
        serde_json::json!(["Alice"]),
        "config reasoning defaults should engage on the JSON-LD ledger route"
    );
}

// =============================================================================
// A malformed config graph is the operator's fault, not the caller's
// =============================================================================

/// `f:rulesSource` with `f:atT` is parsed but not yet supported, so config
/// resolution fails loudly rather than silently dropping a governance setting
/// the operator believes is in force. Since query preparation is where config
/// defaults are completed, that failure now reaches every query on the ledger.
const BAD_RULES_SOURCE_TRIG: &str = r"
@prefix f: <https://ns.flur.ee/db#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

GRAPH <urn:fluree:cfgdefaults:main#config> {
    <urn:cfgdefaults:config> rdf:type f:LedgerConfig .
    <urn:cfgdefaults:config> f:datalogDefaults <urn:cfgdefaults:datalog> .
    <urn:cfgdefaults:datalog> f:datalogEnabled true .
    <urn:cfgdefaults:datalog> f:rulesSource <urn:cfgdefaults:rules-ref> .
    <urn:cfgdefaults:rules-ref> rdf:type f:GraphRef ;
                                f:graphSource <urn:cfgdefaults:rules-src> .
    <urn:cfgdefaults:rules-src> f:graphSelector f:defaultGraph .
    <urn:cfgdefaults:rules-src> f:atT 1 .
}
";

/// The caller sent a perfectly good query. Answering with 400 points them at
/// their own request, which they cannot change to fix this; the fault is in
/// the ledger's config graph and only an operator can clear it.
#[tokio::test]
async fn malformed_ledger_config_is_not_reported_as_a_client_error() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "ledger": LEDGER }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "create ledger");

    upsert_trig(&app, SEED_TRIG).await;
    upsert_trig(&app, BAD_RULES_SOURCE_TRIG).await;

    let (status, json) = post(
        &app,
        "application/sparql-query",
        ENTAILED_SELECT.to_string(),
    )
    .await;

    assert_ne!(
        status,
        StatusCode::BAD_REQUEST,
        "a fault in the ledger's config graph must not read as a bad request: {json}"
    );
    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "expected a server-side status for a malformed config graph: {json}"
    );
}
