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

// ===========================================================================
// JSON-LD `fromNamed`, and cross-language parity
//
// The same dataset question asked in JSON-LD must get the same answer. Before
// this branch it did not: `execute_dataset_query` injected the endpoint's
// ledger as `from` whenever the body carried `fromNamed` but no `from`, so
// JSON-LD kept the default-graph fallback that SPARQL had just lost. On the
// connection endpoint the injected ledger was whichever `fromNamed` entry
// `get_ledger_id` picked first, which silently made one named graph the
// default graph.
// ===========================================================================

/// POST a JSON-LD body to the ledger route.
async fn jsonld(app: &axum::Router, body: JsonValue) -> (StatusCode, Option<String>, JsonValue) {
    post_jsonld(app, &format!("/v1/fluree/query/{LEDGER}"), body).await
}

/// POST a JSON-LD body to the connection route (no path ledger).
async fn jsonld_connection(
    app: &axum::Router,
    body: JsonValue,
) -> (StatusCode, Option<String>, JsonValue) {
    post_jsonld(app, "/v1/fluree/query", body).await
}

async fn post_jsonld(
    app: &axum::Router,
    uri: &str,
    body: JsonValue,
) -> (StatusCode, Option<String>, JsonValue) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
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

/// POST SPARQL to the connection route (no path ledger).
async fn sparql_connection(
    app: &axum::Router,
    sparql: &str,
) -> (StatusCode, Option<String>, JsonValue) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query")
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

/// JSON-LD SELECT returns a bare array of rows.
fn rows(json: &JsonValue) -> &Vec<JsonValue> {
    json.as_array()
        .unwrap_or_else(|| panic!("expected a JSON-LD row array, got {json}"))
}

/// Ledger endpoint, JSON-LD: `fromNamed` with no `from` leaves the default
/// graph empty, so a pattern outside `["graph", ...]` matches nothing — and
/// says so on the wire. Pre-branch this returned the ledger's "D" triple.
#[tokio::test]
async fn jsonld_from_named_only_has_empty_default_graph_and_warns() {
    let (_tmp, app) = seeded_app().await;

    let (status, warning, json) = jsonld(
        &app,
        serde_json::json!({
            "@context": {"ex": "http://ex.org/"},
            "fromNamed": {"g1": {"@id": LEDGER, "@graph": G1}},
            "select": ["?n"],
            "where": {"@id": "?s", "ex:name": "?n"}
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert!(
        rows(&json).is_empty(),
        "default graph must be empty: {json}"
    );
    let warning = warning.expect("fromNamed-only with a non-graph pattern must warn");
    assert!(
        warning.contains("fromNamed") && warning.contains("default graph"),
        "unhelpful warning: {warning}"
    );
}

/// The `["graph", ...]` half still resolves under the same body, and a body
/// whose every pattern is inside `graph` draws no warning.
#[tokio::test]
async fn jsonld_from_named_graph_pattern_resolves_without_warning() {
    let (_tmp, app) = seeded_app().await;

    let (status, warning, json) = jsonld(
        &app,
        serde_json::json!({
            "@context": {"ex": "http://ex.org/"},
            "fromNamed": {"g1": {"@id": LEDGER, "@graph": G1}},
            "select": ["?n"],
            "where": [["graph", "g1", {"@id": "?s", "ex:name": "?n"}]]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(rows(&json).len(), 2, "g1 carries two names: {json}");
    assert!(warning.is_none(), "{warning:?}");
}

/// A JSON-LD body with no dataset clause at all still reads the ledger's
/// default graph — the injection is preserved for exactly that case.
#[tokio::test]
async fn jsonld_no_dataset_clause_still_reads_ledger_default_graph() {
    let (_tmp, app) = seeded_app().await;

    let (status, warning, json) = jsonld(
        &app,
        serde_json::json!({
            "@context": {"ex": "http://ex.org/"},
            "select": ["?n"],
            "where": {"@id": "?s", "ex:name": "?n"}
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(rows(&json).len(), 1, "the default-graph triple: {json}");
    assert!(warning.is_none(), "{warning:?}");
}

/// Naming the default graph explicitly is the migration path, and it silences
/// the warning.
#[tokio::test]
async fn jsonld_explicit_from_plus_from_named_reads_both() {
    let (_tmp, app) = seeded_app().await;

    let (status, warning, json) = jsonld(
        &app,
        serde_json::json!({
            "@context": {"ex": "http://ex.org/"},
            "from": {"@id": LEDGER, "graph": "default"},
            "fromNamed": {"g1": {"@id": LEDGER, "@graph": G1}},
            "select": ["?n"],
            "where": {"@id": "?s", "ex:name": "?n"}
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(rows(&json).len(), 1, "{json}");
    assert!(warning.is_none(), "{warning:?}");
}

/// PARITY, ledger endpoint: byte-equivalent JSON-LD and SPARQL forms of the
/// same `fromNamed`-only question must agree — both on the rows and on whether
/// a warning fires. This is the assertion that would have caught the branch
/// shipping the SPARQL half alone.
#[tokio::test]
async fn ledger_endpoint_jsonld_and_sparql_agree_on_from_named_only() {
    let (_tmp, app) = seeded_app().await;

    let (sparql_status, sparql_warning, sparql_json) = query(
        &app,
        r"PREFIX ex: <http://ex.org/>
          SELECT ?n
          FROM NAMED <http://ex.org/g1>
          WHERE { ?s ex:name ?n }",
    )
    .await;
    let (jsonld_status, jsonld_warning, jsonld_json) = jsonld(
        &app,
        serde_json::json!({
            "@context": {"ex": "http://ex.org/"},
            "fromNamed": {"g1": {"@id": LEDGER, "@graph": G1}},
            "select": ["?n"],
            "where": {"@id": "?s", "ex:name": "?n"}
        }),
    )
    .await;

    assert_eq!(sparql_status, jsonld_status);
    assert_eq!(
        bindings(&sparql_json).len(),
        rows(&jsonld_json).len(),
        "row counts must match: sparql={sparql_json} jsonld={jsonld_json}"
    );
    assert!(bindings(&sparql_json).is_empty());
    assert_eq!(
        sparql_warning.is_some(),
        jsonld_warning.is_some(),
        "both surfaces must warn, or neither: {sparql_warning:?} vs {jsonld_warning:?}"
    );
}

/// PARITY, connection endpoint: the same `fromNamed`-only question with no
/// path ledger. Kept genuinely like-for-like — on the connection endpoint a
/// clause IRI names a LEDGER, so the SPARQL and JSON-LD forms both declare the
/// whole ledger as their one named graph.
#[tokio::test]
async fn connection_endpoint_jsonld_and_sparql_agree_on_from_named_only() {
    let (_tmp, app) = seeded_app().await;

    let (sparql_status, sparql_warning, sparql_json) = sparql_connection(
        &app,
        r"PREFIX ex: <http://ex.org/>
          SELECT ?n
          FROM NAMED <dsem:main>
          WHERE { ?s ex:name ?n }",
    )
    .await;
    let (jsonld_status, jsonld_warning, jsonld_json) = jsonld_connection(
        &app,
        serde_json::json!({
            "@context": {"ex": "http://ex.org/"},
            "fromNamed": {"a": {"@id": LEDGER}},
            "select": ["?n"],
            "where": {"@id": "?s", "ex:name": "?n"}
        }),
    )
    .await;

    assert_eq!(sparql_status, StatusCode::OK, "{sparql_json}");
    assert_eq!(jsonld_status, StatusCode::OK, "{jsonld_json}");
    assert!(
        bindings(&sparql_json).is_empty(),
        "connection SPARQL was already correct: {sparql_json}"
    );
    assert!(
        rows(&jsonld_json).is_empty(),
        "JSON-LD must now agree with it: {jsonld_json}"
    );
    assert_eq!(
        sparql_warning.is_some(),
        jsonld_warning.is_some(),
        "both surfaces must warn, or neither: {sparql_warning:?} vs {jsonld_warning:?}"
    );
    assert!(jsonld_warning.is_some(), "the JSON-LD form must warn");
}

/// Connection endpoint, the 2+ `fromNamed`-entry case specifically: previously
/// `get_ledger_id` picked the first entry and `execute_dataset_query` injected
/// it as `from`, so a pattern outside `["graph", ...]` silently read one
/// arbitrarily-chosen graph's triples and returned them under a 200. No entry
/// may be promoted to the default graph.
#[tokio::test]
async fn connection_jsonld_two_from_named_entries_promote_no_default_graph() {
    let (_tmp, app) = seeded_app().await;

    let (status, warning, json) = jsonld_connection(
        &app,
        serde_json::json!({
            "@context": {"ex": "http://ex.org/"},
            "fromNamed": {
                "g1": {"@id": LEDGER, "@graph": G1},
                "g2": {"@id": LEDGER, "@graph": G2}
            },
            "select": ["?n"],
            "where": {"@id": "?s", "ex:name": "?n"}
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert!(
        rows(&json).is_empty(),
        "no fromNamed entry may become the default graph: {json}"
    );
    assert!(warning.is_some(), "must warn");
}

/// Connection endpoint, JSON-LD: the named halves of that same two-entry body
/// still resolve, each under the alias the caller chose, with no doubling.
#[tokio::test]
async fn connection_jsonld_two_from_named_entries_resolve_by_alias() {
    let (_tmp, app) = seeded_app().await;

    let (status, _warning, json) = jsonld_connection(
        &app,
        serde_json::json!({
            "@context": {"ex": "http://ex.org/"},
            "fromNamed": {
                "g1": {"@id": LEDGER, "@graph": G1},
                "g2": {"@id": LEDGER, "@graph": G2}
            },
            "select": ["?g", "?n"],
            "where": [["graph", "?g", {"@id": "?s", "ex:name": "?n"}]]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    // g1 has two names, g2 one — three solutions over exactly two graph names.
    assert_eq!(rows(&json).len(), 3, "{json}");
    let all = serde_json::to_string(&json).expect("json");
    assert!(all.contains("\"g1\"") && all.contains("\"g2\""));
    assert!(
        !all.contains(LEDGER),
        "the ledger id must not appear as a graph name: {all}"
    );
}

/// Defect 10: the graph selector inside a `fromNamed` entry is accepted under
/// either spelling. `fromNamed` once read only `@graph` while the `from`
/// single-source form read only `graph`, and the wrong key was *silently
/// ignored* — the entry resolved to the whole ledger, so this query used to
/// return the default graph's "D" alongside g1's rows instead of erroring.
#[tokio::test]
async fn jsonld_from_named_accepts_either_graph_selector_spelling() {
    let (_tmp, app) = seeded_app().await;

    let ask = |key: &'static str| {
        let app = app.clone();
        async move {
            let (status, _warning, json) = jsonld(
                &app,
                serde_json::json!({
                    "@context": {"ex": "http://ex.org/"},
                    "fromNamed": {"g1": {"@id": LEDGER, key: G1}},
                    "select": ["?n"],
                    "where": [["graph", "g1", {"@id": "?s", "ex:name": "?n"}]]
                }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{json}");
            let mut names: Vec<String> = rows(&json)
                .iter()
                .map(|r| serde_json::to_string(r).expect("row json"))
                .collect();
            names.sort();
            names
        }
    };

    let with_at = ask("@graph").await;
    let without_at = ask("graph").await;

    // g1 holds exactly "A" and "B"; the ledger's default-graph "D" is not in it.
    assert_eq!(with_at.len(), 2, "@graph selector: {with_at:?}");
    assert_eq!(
        without_at, with_at,
        "both spellings must select the same graph"
    );
    assert!(
        !with_at.iter().any(|r| r.contains("\"D\"")),
        "the whole ledger must not be selected: {with_at:?}"
    );
}

/// Connection route: a named graph addressed as `ledger#graph`, the form
/// `docs/query/datasets.md` documents, reads that graph. v4.2.2 answered 400
/// "Invalid ID format ... branch cannot contain '#'", because the SQL lane's
/// graph-source probe looked the IRI up as a source id.
#[tokio::test]
async fn connection_route_reads_a_named_graph_addressed_by_ledger_fragment() {
    let (_tmp, app) = seeded_app().await;
    let g1 = format!("{LEDGER}#{G1}");

    let (status, _warning, json) = sparql_connection(
        &app,
        &format!(
            "PREFIX ex: <http://ex.org/> \
             SELECT ?n FROM NAMED <{g1}> WHERE {{ GRAPH <{g1}> {{ ?s ex:name ?n }} }}"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    let mut names: Vec<&str> = bindings(&json)
        .iter()
        .map(|row| binding_value(row, "n"))
        .collect();
    names.sort_unstable();
    assert_eq!(names, ["A", "B"], "{json}");

    // The reserved `#txn-meta` graph beside the default graph (datasets.md,
    // "Mixed Patterns").
    let (status, _warning, json) = sparql_connection(
        &app,
        &format!(
            "PREFIX ex: <http://ex.org/> \
             SELECT ?n ?t FROM <{LEDGER}> FROM NAMED <{LEDGER}#txn-meta> \
             WHERE {{ ?s ex:name ?n . \
                      GRAPH <{LEDGER}#txn-meta> {{ ?c <https://ns.flur.ee/db#t> ?t }} }}"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    let rows_nt: Vec<(&str, &str)> = bindings(&json)
        .iter()
        .map(|row| (binding_value(row, "n"), binding_value(row, "t")))
        .collect();
    assert_eq!(rows_nt, [("D", "1")], "{json}");

    // The JSON-LD twin.
    let (status, _warning, json) = jsonld_connection(
        &app,
        serde_json::json!({
            "@context": {"ex": "http://ex.org/"},
            "fromNamed": [g1],
            "select": ["?n"],
            "where": [["graph", g1, {"@id": "?s", "ex:name": "?n"}]]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    let mut names: Vec<&str> = rows(&json)
        .iter()
        .map(|row| row[0].as_str().unwrap_or_else(|| panic!("row {row}")))
        .collect();
    names.sort_unstable();
    assert_eq!(names, ["A", "B"], "{json}");
}

// =============================================================================
// One resolver for the ledger route's dataset references
// =============================================================================

const SCOPED_LEDGER: &str = "dscope:main";

/// A default-graph name ("D") and two graphs whose IRIs carry the characters
/// the old string classifiers read as ledger syntax: `#` and `@`.
const SCOPED_TRIG: &str = r#"
@prefix ex: <http://ex.org/> .

ex:d1 ex:name "D" .

<http://ex.org/vocab#products> {
    ex:p1 ex:name "P" .
}

<http://ex.org/@alice/g> {
    ex:a1 ex:name "A" .
}
"#;

async fn scoped_app() -> (TempDir, axum::Router) {
    let (tmp, state) = test_state().await;
    let app = build_router(state);
    let (status, _) = post(
        &app,
        "/v1/fluree/create",
        "application/json",
        serde_json::json!({ "ledger": SCOPED_LEDGER }).to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create ledger");
    let (status, _) = post(
        &app,
        &format!("/v1/fluree/upsert/{SCOPED_LEDGER}"),
        "application/trig",
        SCOPED_TRIG.to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "trig upsert");
    (tmp, app)
}

async fn post(
    app: &axum::Router,
    uri: &str,
    content_type: &str,
    body: String,
) -> (StatusCode, JsonValue) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", content_type)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: JsonValue = serde_json::from_slice(&bytes).unwrap_or(JsonValue::Null);
    (status, json)
}

/// The `?n` values of a SPARQL-results or JSON-LD `select ?n` response, sorted.
fn names(json: &JsonValue) -> Vec<String> {
    let mut out: Vec<String> = match json.get("results") {
        Some(results) => results["bindings"]
            .as_array()
            .unwrap_or_else(|| panic!("no bindings in {json}"))
            .iter()
            .map(|row| binding_value(row, "n").to_string())
            .collect(),
        None => json
            .as_array()
            .unwrap_or_else(|| panic!("no rows in {json}"))
            .iter()
            .map(|v| v.as_str().unwrap_or_default().to_string())
            .collect(),
    };
    out.sort();
    out
}

/// Every dataset reference on the ledger route resolves in the path's ledger
/// through one typed table: the ledger's own address in any spelling, on the
/// path or in the body, names it (#1982); a graph IRI containing `#` or `@` is
/// a graph, not ledger syntax; prefixed and BASE-relative clause IRIs expand
/// first; JSON-LD `from` / `fromNamed` read the same way as their SPARQL
/// twins. Another ledger is still refused.
#[tokio::test]
async fn ledger_route_dataset_references_resolve_in_the_paths_ledger() {
    let (_tmp, app) = scoped_app().await;
    let prefix = "PREFIX ex: <http://ex.org/> ";
    let sparql_cases: Vec<(&str, &str, String, Vec<&str>)> = vec![
        (
            "a graph IRI with '#'",
            SCOPED_LEDGER,
            format!(
                "{prefix}SELECT ?n FROM NAMED <http://ex.org/vocab#products> \
                 WHERE {{ GRAPH <http://ex.org/vocab#products> {{ ?s ex:name ?n }} }}"
            ),
            vec!["P"],
        ),
        (
            "a graph IRI with '@'",
            SCOPED_LEDGER,
            format!(
                "{prefix}SELECT ?n FROM NAMED <http://ex.org/@alice/g> \
                 WHERE {{ GRAPH <http://ex.org/@alice/g> {{ ?s ex:name ?n }} }}"
            ),
            vec!["A"],
        ),
        (
            "the config graph by its URN",
            SCOPED_LEDGER,
            format!("{prefix}SELECT ?n FROM <urn:fluree:{SCOPED_LEDGER}#config> WHERE {{ ?s ex:name ?n }}"),
            vec![],
        ),
        (
            "#1982: short path, full FROM",
            "dscope",
            format!("{prefix}SELECT ?n FROM <{SCOPED_LEDGER}> WHERE {{ ?s ex:name ?n }}"),
            vec!["D"],
        ),
        (
            "#1982: URN path",
            "urn:fluree:dscope:main",
            format!(
                "{prefix}SELECT ?n FROM <{SCOPED_LEDGER}> FROM NAMED <http://ex.org/vocab#products> \
                 WHERE {{ {{ ?s ex:name ?n }} UNION {{ GRAPH <http://ex.org/vocab#products> {{ ?s ex:name ?n }} }} }}"
            ),
            vec!["D", "P"],
        ),
        (
            "#1982: full path, short FROM",
            SCOPED_LEDGER,
            format!("{prefix}SELECT ?n FROM <dscope> WHERE {{ ?s ex:name ?n }}"),
            vec!["D"],
        ),
        (
            "a prefixed FROM NAMED",
            SCOPED_LEDGER,
            "PREFIX ex: <http://ex.org/> PREFIX al: <http://ex.org/@alice/> \
             SELECT ?n FROM NAMED al:g WHERE { GRAPH al:g { ?s ex:name ?n } }"
                .to_string(),
            vec!["A"],
        ),
    ];
    let mut failures = Vec::new();
    for (case, path, sparql, expected) in &sparql_cases {
        let (status, json) = post(
            &app,
            &format!("/v1/fluree/query/{path}"),
            "application/sparql-query",
            sparql.clone(),
        )
        .await;
        if status != StatusCode::OK || names(&json) != *expected {
            failures.push(format!("SPARQL {case}: {status} {json}"));
        }
    }

    let jsonld_cases: Vec<(&str, &str, JsonValue, Vec<&str>)> = vec![
        (
            "the short alias",
            SCOPED_LEDGER,
            serde_json::json!({"from": "dscope"}),
            vec!["D"],
        ),
        (
            "the URN",
            SCOPED_LEDGER,
            serde_json::json!({"from": "urn:fluree:dscope:main"}),
            vec!["D"],
        ),
        (
            "the address with a graph IRI",
            SCOPED_LEDGER,
            serde_json::json!({"from": "dscope:main#http://ex.org/vocab#products"}),
            vec!["P"],
        ),
        (
            "short path, full from",
            "dscope",
            serde_json::json!({"from": SCOPED_LEDGER}),
            vec!["D"],
        ),
    ];
    for (case, path, dataset, expected) in &jsonld_cases {
        let mut body = serde_json::json!({
            "@context": {"ex": "http://ex.org/"},
            "select": "?n",
            "where": {"@id": "?s", "ex:name": "?n"}
        });
        for (k, v) in dataset.as_object().unwrap() {
            body[k] = v.clone();
        }
        let (status, json) = post(
            &app,
            &format!("/v1/fluree/query/{path}"),
            "application/json",
            body.to_string(),
        )
        .await;
        if status != StatusCode::OK || names(&json) != *expected {
            failures.push(format!("JSON-LD {case}: {status} {json}"));
        }
    }

    // A graph IRI in `fromNamed`, read by its graph pattern.
    let body = serde_json::json!({
        "@context": {"ex": "http://ex.org/"},
        "fromNamed": ["http://ex.org/vocab#products"],
        "select": "?n",
        "where": [["graph", "http://ex.org/vocab#products", {"@id": "?s", "ex:name": "?n"}]]
    });
    let (status, json) = post(
        &app,
        &format!("/v1/fluree/query/{SCOPED_LEDGER}"),
        "application/json",
        body.to_string(),
    )
    .await;
    if status != StatusCode::OK || names(&json) != ["P"] {
        failures.push(format!("JSON-LD fromNamed graph IRI: {status} {json}"));
    }

    // A graph the ledger does not have is a typed 404.
    let (status, json) = post(
        &app,
        &format!("/v1/fluree/query/{SCOPED_LEDGER}"),
        "application/sparql-query",
        format!(
            "{prefix}SELECT ?n FROM NAMED <http://ex.org/nope> \
             WHERE {{ GRAPH <http://ex.org/nope> {{ ?s ex:name ?n }} }}"
        ),
    )
    .await;
    if status != StatusCode::NOT_FOUND || json["@type"] != "err:db/GraphNotFound" {
        failures.push(format!("SPARQL an unknown graph: {status} {json}"));
    }

    // Another ledger stays refused, in either language.
    let (status, json) = post(
        &app,
        &format!("/v1/fluree/query/{SCOPED_LEDGER}"),
        "application/sparql-query",
        format!("{prefix}SELECT ?n FROM <other-ledger:main> WHERE {{ ?s ex:name ?n }}"),
    )
    .await;
    if status != StatusCode::BAD_REQUEST || !json.to_string().contains("Ledger mismatch") {
        failures.push(format!("SPARQL another ledger: {status} {json}"));
    }
    let (status, json) = post(
        &app,
        &format!("/v1/fluree/query/{SCOPED_LEDGER}"),
        "application/json",
        serde_json::json!({"from": "other-ledger", "select": "?s", "where": {"@id": "?s"}})
            .to_string(),
    )
    .await;
    if status != StatusCode::BAD_REQUEST || !json.to_string().contains("Ledger mismatch") {
        failures.push(format!("JSON-LD another ledger: {status} {json}"));
    }

    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// On the ledger routes the ledger's own address with a reserved keyword
/// names that reserved graph, in writes and reads alike, as its `urn:fluree:`
/// form does: `L#config` writes and reads the config graph, a write to
/// `L#txn-meta` is refused, and no write registers a graph under either name.
/// SPARQL and JSON-LD.
#[tokio::test]
async fn ledger_route_reserved_keyword_addresses_name_the_reserved_graphs() {
    let (_tmp, app) = scoped_app().await;
    let update = format!("/v1/fluree/update/{SCOPED_LEDGER}");
    let query = format!("/v1/fluree/query/{SCOPED_LEDGER}");
    let prefix = "PREFIX ex: <http://ex.org/> ";
    let ctx = serde_json::json!({"ex": "http://ex.org/"});
    let mut failures = Vec::new();

    for (case, content_type, body) in [
        (
            "SPARQL write",
            "application/sparql-update",
            format!("{prefix}INSERT DATA {{ GRAPH <{SCOPED_LEDGER}#config> {{ ex:c1 ex:name \"C1\" }} }}"),
        ),
        (
            "JSON-LD write",
            "application/json",
            serde_json::json!({"@context": ctx, "graph": "dscope#config",
                               "insert": {"@id": "ex:c2", "ex:name": "C2"}})
            .to_string(),
        ),
    ] {
        let (status, json) = post(&app, &update, content_type, body).await;
        if status != StatusCode::OK {
            failures.push(format!("{case}: {status} {json}"));
        }
    }
    let config = ["C1", "C2"];
    for from in [
        format!("{SCOPED_LEDGER}#config"),
        format!("urn:fluree:{SCOPED_LEDGER}#config"),
    ] {
        let (status, json) = post(
            &app,
            &query,
            "application/sparql-query",
            format!("{prefix}SELECT ?n FROM <{from}> WHERE {{ ?s ex:name ?n }}"),
        )
        .await;
        if status != StatusCode::OK || names(&json) != config {
            failures.push(format!("SPARQL read {from}: {status} {json}"));
        }
        let (status, json) = post(
            &app,
            &query,
            "application/json",
            serde_json::json!({"@context": ctx, "from": from, "select": "?n",
                               "where": {"@id": "?s", "ex:name": "?n"}})
            .to_string(),
        )
        .await;
        if status != StatusCode::OK || names(&json) != config {
            failures.push(format!("JSON-LD read {from}: {status} {json}"));
        }
    }

    for (case, content_type, body) in [
        (
            "SPARQL txn-meta write",
            "application/sparql-update",
            format!("{prefix}INSERT DATA {{ GRAPH <{SCOPED_LEDGER}#txn-meta> {{ ex:t ex:name \"T\" }} }}"),
        ),
        (
            "JSON-LD txn-meta write",
            "application/json",
            serde_json::json!({"@context": ctx,
                               "insert": {"@id": "ex:t", "@graph": "dscope#txn-meta", "ex:name": "T"}})
            .to_string(),
        ),
    ] {
        let (status, json) = post(&app, &update, content_type, body).await;
        if status != StatusCode::BAD_REQUEST || !json.to_string().contains("reserved") {
            failures.push(format!("{case}: {status} {json}"));
        }
    }

    // No graph is registered under either keyword address.
    let (status, json) = post(
        &app,
        &query,
        "application/sparql-query",
        "SELECT DISTINCT ?n WHERE { GRAPH ?n { ?s ?p ?o } }".to_string(),
    )
    .await;
    let graphs = names(&json);
    if status != StatusCode::OK
        || graphs
            .iter()
            .any(|g| g.ends_with("#config") || g.ends_with("#txn-meta"))
    {
        failures.push(format!("graphs: {status} {graphs:?}"));
    }

    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}
