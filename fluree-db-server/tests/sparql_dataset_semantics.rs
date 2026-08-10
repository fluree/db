//! SPARQL dataset-clause semantics over the real HTTP ledger route
//! (`POST /v1/fluree/query/{ledger}`).
//!
//! The W3C `/dataset/` conformance family drives the *embedded* engine
//! (`testsuite-sparql/src/query_handler.rs` calls `fluree.query(&db, sparql)`),
//! so it was structurally blind to the HTTP route, which builds its own
//! `DatasetSpec`. That gap let azure-chat#50 ship: over HTTP, `FROM NAMED`
//! registered the ledger alias as a second named-graph key, doubling every
//! `GRAPH ?g` solution and resolving `GRAPH <ledger-alias>` to the wrong
//! graph's triples. These tests exercise the route itself.

use axum::body::Body;
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

const LEDGER: &str = "dsem:main";
const G1: &str = "http://ex.org/g1";
const G2: &str = "http://ex.org/g2";

/// Default graph carries one triple ("D"); `g1` carries two names plus one
/// `ex:knows` edge; `g2` carries one name. That makes "empty default graph",
/// "one solution per triple" and "one binding per declared graph" separately
/// discriminating.
const SEED_TRIG: &str = r#"
@prefix ex: <http://ex.org/> .

ex:d1 ex:name "D" .

<http://ex.org/g1> {
    ex:s1 ex:name "A" .
    ex:s2 ex:name "B" .
    ex:s1 ex:knows ex:s2 .
}

<http://ex.org/g2> {
    ex:s3 ex:name "C" .
}
"#;

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

/// Create the ledger and load `SEED_TRIG`, returning a router ready to query.
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

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/upsert/{LEDGER}"))
                .header("content-type", "application/trig")
                .body(Body::from(SEED_TRIG))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "trig upsert");

    (tmp, app)
}

/// POST SPARQL to the ledger route. Returns status, the `x-fdb-warning`
/// header if present, and the parsed body.
async fn query(app: &axum::Router, sparql: &str) -> (StatusCode, Option<String>, JsonValue) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/query/{LEDGER}"))
                .header("content-type", "application/sparql-query")
                .body(Body::from(sparql.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let warning = resp
        .headers()
        .get("x-fdb-warning")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: JsonValue = serde_json::from_slice(&bytes).expect("valid JSON response");
    (status, warning, json)
}

/// SELECT over this route defaults to SPARQL-results JSON.
fn bindings(json: &JsonValue) -> &Vec<JsonValue> {
    json.get("results")
        .and_then(|r| r.get("bindings"))
        .and_then(JsonValue::as_array)
        .unwrap_or_else(|| panic!("expected SPARQL-results bindings, got {json}"))
}

fn binding_value<'a>(row: &'a JsonValue, var: &str) -> &'a str {
    row.get(var)
        .and_then(|b| b.get("value"))
        .and_then(JsonValue::as_str)
        .unwrap_or_else(|| panic!("no ?{var} in {row}"))
}

/// The reporter's exact case. `FROM NAMED <g1>` makes `g1` the query's only
/// named graph, so `GRAPH ?g` yields one solution per matching triple with
/// `?g` bound to `g1`. Before the fix the ledger alias was a second key onto
/// the same view: 4 rows, half of them binding `?g` to `dsem:main`.
#[tokio::test]
async fn from_named_graph_var_binds_only_the_declared_graph() {
    let (_tmp, app) = seeded_app().await;

    let (status, warning, json) = query(
        &app,
        r"PREFIX ex: <http://ex.org/>
          SELECT ?g ?n
          FROM NAMED <http://ex.org/g1>
          WHERE { GRAPH ?g { ?s ex:name ?n } }",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    let rows = bindings(&json);
    assert_eq!(
        rows.len(),
        2,
        "one solution per triple, not one per key: {json}"
    );
    for row in rows {
        assert_eq!(
            binding_value(row, "g"),
            G1,
            "?g must bind only the declared graph"
        );
    }
    assert!(
        warning.is_none(),
        "every pattern is inside GRAPH, so nothing to warn about: {warning:?}"
    );
}

/// The reporter's property-path case (sq02-shaped): the single `ex:knows`
/// edge is one solution. Pre-fix it came back once per graph key (2).
#[tokio::test]
async fn from_named_property_path_is_not_doubled() {
    let (_tmp, app) = seeded_app().await;

    let (status, _warning, json) = query(
        &app,
        r"PREFIX ex: <http://ex.org/>
          SELECT ?x ?y
          FROM NAMED <http://ex.org/g1>
          WHERE { GRAPH ?g { ?x ex:knows+ ?y } }",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(bindings(&json).len(), 1, "{json}");
}

/// sq04-shaped: N `FROM NAMED` clauses give exactly N graph bindings. Pre-fix
/// gave N+1, the extra key aliasing whichever clause was processed last — which
/// is what the reporter measured as "expected 2, observed 3".
#[tokio::test]
async fn two_from_named_clauses_give_two_graph_bindings() {
    let (_tmp, app) = seeded_app().await;

    let (status, _warning, json) = query(
        &app,
        r"PREFIX ex: <http://ex.org/>
          SELECT DISTINCT ?g
          FROM NAMED <http://ex.org/g1>
          FROM NAMED <http://ex.org/g2>
          WHERE { GRAPH ?g { ?s ex:name ?n } }",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    let mut graphs: Vec<&str> = bindings(&json)
        .iter()
        .map(|r| binding_value(r, "g"))
        .collect();
    graphs.sort_unstable();
    assert_eq!(graphs, vec![G1, G2], "{json}");
}

/// SPARQL 1.1 §13.2: a dataset clause with `FROM NAMED` and no `FROM` has an
/// empty default graph, so a pattern outside `GRAPH { }` matches nothing. This
/// endpoint used to substitute the ledger's default graph, disagreeing with the
/// embedded engine on the same query text. Because the break is silent (200
/// with fewer rows) the response carries an advisory header.
#[tokio::test]
async fn from_named_only_has_an_empty_default_graph_and_warns() {
    let (_tmp, app) = seeded_app().await;

    let (status, warning, json) = query(
        &app,
        r"PREFIX ex: <http://ex.org/>
          SELECT ?n
          FROM NAMED <http://ex.org/g1>
          WHERE { ?s ex:name ?n }",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert!(
        bindings(&json).is_empty(),
        "the default-graph triple must not leak in: {json}"
    );
    let warning = warning.expect("a FROM NAMED-only query with a non-GRAPH pattern must warn");
    assert!(
        warning.contains("FROM NAMED") && warning.contains("default graph"),
        "unhelpful warning: {warning}"
    );
}

/// A query with no dataset clause at all is untouched by the §13.2 change: it
/// still reads the ledger's default graph, and there is nothing to warn about.
#[tokio::test]
async fn no_dataset_clause_still_reads_the_ledger_default_graph() {
    let (_tmp, app) = seeded_app().await;

    let (status, warning, json) = query(
        &app,
        r"PREFIX ex: <http://ex.org/>
          SELECT ?n WHERE { ?s ex:name ?n }",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(bindings(&json).len(), 1, "the default-graph triple: {json}");
    assert_eq!(binding_value(&bindings(&json)[0], "n"), "D");
    assert!(warning.is_none(), "{warning:?}");
}

/// The migration path for queries written against the old fallback: name the
/// default graph with `FROM`. Both halves then resolve and no warning fires.
#[tokio::test]
async fn from_default_plus_from_named_reads_both() {
    let (_tmp, app) = seeded_app().await;

    let (status, warning, json) = query(
        &app,
        r"PREFIX ex: <http://ex.org/>
          SELECT ?d ?a
          FROM <default>
          FROM NAMED <http://ex.org/g1>
          WHERE {
            ?s ex:name ?d .
            GRAPH <http://ex.org/g1> { ex:s1 ex:name ?a }
          }",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    let rows = bindings(&json);
    assert_eq!(rows.len(), 1, "{json}");
    assert_eq!(binding_value(&rows[0], "d"), "D");
    assert_eq!(binding_value(&rows[0], "a"), "A");
    assert!(warning.is_none(), "{warning:?}");
}

/// Under a dataset clause the ledger alias is not one of the query's graph
/// names, so `GRAPH <ledger-alias>` behaves like any unknown graph name: zero
/// rows, HTTP 200, no error. Pre-fix it resolved — and returned the *named*
/// graph's triples, never the default graph's own.
///
/// Whether a dataset clause should let `GRAPH <ledger-alias>` deliberately
/// address the ledger's default graph is an open product question (D-2 keeps
/// that spelling only on the no-dataset-clause path); this test pins today's
/// answer so a future change to it is a decision, not a side effect.
#[tokio::test]
async fn graph_ledger_alias_under_a_dataset_clause_is_an_unknown_graph() {
    let (_tmp, app) = seeded_app().await;

    let (status, _warning, json) = query(
        &app,
        r"PREFIX ex: <http://ex.org/>
          SELECT ?n
          FROM NAMED <http://ex.org/g1>
          WHERE { GRAPH <dsem:main> { ?s ex:name ?n } }",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert!(bindings(&json).is_empty(), "{json}");
}

/// `GRAPH ?g` with no dataset clause keeps enumerating the ledger's registered
/// user named graphs (decision D-2 keeps that Fluree extension) — and still
/// does not enumerate the ledger alias.
#[tokio::test]
async fn graph_var_without_dataset_clause_enumerates_user_graphs_only() {
    let (_tmp, app) = seeded_app().await;

    let (status, _warning, json) = query(
        &app,
        r"PREFIX ex: <http://ex.org/>
          SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ex:name ?n } }",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    let mut graphs: Vec<&str> = bindings(&json)
        .iter()
        .map(|r| binding_value(r, "g"))
        .collect();
    graphs.sort_unstable();
    assert_eq!(graphs, vec![G1, G2], "{json}");
}
