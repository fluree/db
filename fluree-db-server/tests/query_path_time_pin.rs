//! A time pin in the ledger path (`/v1/fluree/query/<ledger>@t:1`) reads the
//! whole ledger as of that point, named graphs included, on every query
//! surface the route serves.
//!
//! The fixture's second commit changes the default graph (a new value for
//! `ex:alice`, a new subject in a namespace first seen at t2) and the named
//! graphs (a new value in `g1`, a new graph `g2`), so each assertion at the
//! pin is checked against a head that would answer differently.

use axum::body::Body;
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

const LEDGER: &str = "pinned:main";
const G1: &str = "http://ex.org/g1";
const G2: &str = "http://ex.org/g2";

const T1_TRIG: &str = r#"
@prefix ex: <http://ex.org/> .
ex:alice ex:name "Alice-1" .
<http://ex.org/g1> { ex:s1 ex:label "g1-v1" . }
"#;

const T2_TRIG: &str = r#"
@prefix ex: <http://ex.org/> .
ex:alice ex:name "Alice-2" .
<http://later.example/bob> ex:name "Bob" .
<http://ex.org/g1> { ex:s1 ex:label "g1-v2" . }
<http://ex.org/g2> { ex:s2 ex:label "g2" . }
"#;

/// The first commit, as `/log` reports it.
struct Commit1 {
    time: String,
    id: String,
}

async fn app_with(config: impl FnOnce(&mut ServerConfig)) -> (TempDir, axum::Router) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    config(&mut cfg);
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    (tmp, build_router(state))
}

async fn send(
    app: &axum::Router,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
    body: impl Into<String>,
) -> (StatusCode, String) {
    let mut req = Request::builder().method(method).uri(uri);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::from(body.into())).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// Two commits: t1 = `T1_TRIG`, t2 = `T2_TRIG`.
async fn fixture_with(config: impl FnOnce(&mut ServerConfig)) -> (TempDir, axum::Router, Commit1) {
    let (tmp, app) = app_with(config).await;
    let create = json!({ "ledger": LEDGER }).to_string();
    let json_ct = [("content-type", "application/json")];
    let (status, body) = send(&app, "POST", "/v1/fluree/create", &json_ct, create).await;
    assert_eq!(status, StatusCode::CREATED, "create: {body}");
    let trig = [("content-type", "application/trig")];
    let upsert = format!("/v1/fluree/upsert/{LEDGER}");
    let (status, body) = send(&app, "POST", &upsert, &trig, T1_TRIG).await;
    assert_eq!(status, StatusCode::OK, "t1: {body}");
    // Keeps the two commit timestamps apart for the `@time:` pin.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let (status, body) = send(&app, "POST", &upsert, &trig, T2_TRIG).await;
    assert_eq!(status, StatusCode::OK, "t2: {body}");

    let (status, body) = send(&app, "GET", &format!("/v1/fluree/log/{LEDGER}"), &[], "").await;
    assert_eq!(status, StatusCode::OK, "log: {body}");
    let log: JsonValue = serde_json::from_str(&body).unwrap();
    let t1 = log["commits"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["t"] == 1)
        .unwrap_or_else(|| panic!("no t=1 commit in {log}"));
    let commit1 = Commit1 {
        time: t1["time"].as_str().unwrap().to_string(),
        id: t1["commit_id"].as_str().unwrap().to_string(),
    };
    (tmp, app, commit1)
}

async fn fixture() -> (TempDir, axum::Router, Commit1) {
    fixture_with(|_| {}).await
}

async fn sparql(app: &axum::Router, uri: &str, query: &str) -> (StatusCode, String) {
    send(
        app,
        "POST",
        uri,
        &[("content-type", "application/sparql-query")],
        query,
    )
    .await
}

async fn jsonld(app: &axum::Router, uri: &str, query: &JsonValue) -> (StatusCode, String) {
    send(
        app,
        "POST",
        uri,
        &[("content-type", "application/json")],
        query.to_string(),
    )
    .await
}

/// SPARQL-results JSON rows as the values of `vars`, sorted.
fn rows(status: StatusCode, body: &str, vars: &[&str]) -> Vec<Vec<String>> {
    assert_eq!(status, StatusCode::OK, "{body}");
    let json: JsonValue = serde_json::from_str(body).unwrap();
    let mut out: Vec<Vec<String>> = json["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("expected SPARQL-results bindings: {json}"))
        .iter()
        .map(|b| {
            vars.iter()
                .map(|v| b[*v]["value"].as_str().unwrap_or("").to_string())
                .collect()
        })
        .collect();
    out.sort();
    out
}

/// JSON-LD tuple rows, sorted.
fn jsonld_rows(status: StatusCode, body: &str) -> Vec<Vec<String>> {
    assert_eq!(status, StatusCode::OK, "{body}");
    let json: JsonValue = serde_json::from_str(body).unwrap();
    let mut out: Vec<Vec<String>> = json
        .as_array()
        .unwrap_or_else(|| panic!("expected JSON-LD rows: {json}"))
        .iter()
        .map(|row| {
            row.as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap_or_default().to_string())
                .collect()
        })
        .collect();
    out.sort();
    out
}

fn strs(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| r.iter().map(|s| (*s).to_string()).collect())
        .collect()
}

fn query_uri(pin: &str) -> String {
    format!("/v1/fluree/query/{LEDGER}{pin}")
}

const SPARQL_DEFAULT: &str = "PREFIX ex: <http://ex.org/> SELECT ?s ?n WHERE { ?s ex:name ?n }";
const SPARQL_GRAPHS: &str =
    "PREFIX ex: <http://ex.org/> SELECT ?g ?s ?l WHERE { GRAPH ?g { ?s ex:label ?l } }";

fn sparql_graph_iri(iri: &str) -> String {
    format!(
        "PREFIX ex: <http://ex.org/> SELECT ?s ?l WHERE {{ GRAPH <{iri}> {{ ?s ex:label ?l }} }}"
    )
}

fn jsonld_default() -> JsonValue {
    json!({
        "@context": { "ex": "http://ex.org/" },
        "select": ["?s", "?n"],
        "where": { "@id": "?s", "ex:name": "?n" }
    })
}

fn jsonld_graphs() -> JsonValue {
    json!({
        "@context": { "ex": "http://ex.org/" },
        "select": ["?g", "?s", "?l"],
        "where": [["graph", "?g", { "@id": "?s", "ex:label": "?l" }]]
    })
}

fn jsonld_graph_iri(iri: &str) -> JsonValue {
    json!({
        "@context": { "ex": "http://ex.org/" },
        "select": ["?s", "?l"],
        "where": [["graph", iri, { "@id": "?s", "ex:label": "?l" }]]
    })
}

fn t1_default_sparql() -> Vec<Vec<String>> {
    strs(&[&["http://ex.org/alice", "Alice-1"]])
}

fn with(mut query: JsonValue, key: &str, value: JsonValue) -> JsonValue {
    query[key] = value;
    query
}

#[tokio::test]
async fn sparql_path_pin_reads_default_and_named_graphs_as_of_the_pin() {
    let (_tmp, app, _) = fixture().await;

    // Head, for contrast: every assertion below would read differently here.
    let (status, body) = sparql(&app, &query_uri(""), SPARQL_DEFAULT).await;
    assert_eq!(
        rows(status, &body, &["s", "n"]),
        strs(&[
            &["http://ex.org/alice", "Alice-2"],
            &["http://later.example/bob", "Bob"]
        ])
    );
    let (status, body) = sparql(&app, &query_uri(""), SPARQL_GRAPHS).await;
    assert_eq!(
        rows(status, &body, &["g", "s", "l"]),
        strs(&[
            &[G1, "http://ex.org/s1", "g1-v2"],
            &[G2, "http://ex.org/s2", "g2"]
        ])
    );

    let pinned = query_uri("@t:1");
    let (status, body) = sparql(&app, &pinned, SPARQL_DEFAULT).await;
    assert_eq!(rows(status, &body, &["s", "n"]), t1_default_sparql());

    let (status, body) = sparql(&app, &pinned, SPARQL_GRAPHS).await;
    assert_eq!(
        rows(status, &body, &["g", "s", "l"]),
        strs(&[&[G1, "http://ex.org/s1", "g1-v1"]]),
        "GRAPH ?g at t1 sees g1 as it was, and not g2"
    );

    let (status, body) = sparql(&app, &pinned, &sparql_graph_iri(G1)).await;
    assert_eq!(
        rows(status, &body, &["s", "l"]),
        strs(&[&["http://ex.org/s1", "g1-v1"]])
    );
    let (status, body) = sparql(&app, &pinned, &sparql_graph_iri(G2)).await;
    assert!(rows(status, &body, &["s", "l"]).is_empty(), "{body}");

    // SPARQL Protocol GET reads at the pin too.
    let get = format!("{pinned}?query={}", urlencoding::encode(SPARQL_GRAPHS));
    let (status, body) = send(&app, "GET", &get, &[], "").await;
    assert_eq!(
        rows(status, &body, &["g", "s", "l"]),
        strs(&[&[G1, "http://ex.org/s1", "g1-v1"]])
    );
}

#[tokio::test]
async fn jsonld_path_pin_reads_default_and_named_graphs_as_of_the_pin() {
    let (_tmp, app, _) = fixture().await;

    let (status, body) = jsonld(&app, &query_uri(""), &jsonld_graphs()).await;
    assert_eq!(
        jsonld_rows(status, &body),
        strs(&[&[G1, "ex:s1", "g1-v2"], &[G2, "ex:s2", "g2"]]),
        "head, for contrast"
    );

    let pinned = query_uri("@t:1");
    let (status, body) = jsonld(&app, &pinned, &jsonld_default()).await;
    assert_eq!(
        jsonld_rows(status, &body),
        strs(&[&["ex:alice", "Alice-1"]])
    );

    let (status, body) = jsonld(&app, &pinned, &jsonld_graphs()).await;
    assert_eq!(jsonld_rows(status, &body), strs(&[&[G1, "ex:s1", "g1-v1"]]));

    let (status, body) = jsonld(&app, &pinned, &jsonld_graph_iri(G1)).await;
    assert_eq!(jsonld_rows(status, &body), strs(&[&["ex:s1", "g1-v1"]]));
    let (status, body) = jsonld(&app, &pinned, &jsonld_graph_iri(G2)).await;
    assert!(jsonld_rows(status, &body).is_empty(), "{body}");
}

#[tokio::test]
async fn time_and_commit_pins_select_the_same_state_as_t() {
    let (_tmp, app, commit1) = fixture().await;

    for pin in [
        format!("@time:{}", commit1.time),
        format!("@iso:{}", commit1.time),
        format!("@commit:{}", commit1.id),
    ] {
        let uri = query_uri(&pin);
        let (status, body) = sparql(&app, &uri, SPARQL_DEFAULT).await;
        assert_eq!(
            rows(status, &body, &["s", "n"]),
            t1_default_sparql(),
            "{pin}"
        );

        let (status, body) = jsonld(&app, &uri, &jsonld_graphs()).await;
        assert_eq!(
            jsonld_rows(status, &body),
            strs(&[&[G1, "ex:s1", "g1-v1"]]),
            "{pin}"
        );
    }
}

#[tokio::test]
async fn malformed_path_pin_is_a_400() {
    let (_tmp, app, _) = fixture().await;

    for pin in ["@t:abc", "@bogus:1", "@", "@commit:abc", "@t:1%23txn-meta"] {
        let uri = query_uri(pin);
        let (status, body) = sparql(&app, &uri, SPARQL_DEFAULT).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "SPARQL {pin}: {body}");
        let (status, body) = jsonld(&app, &uri, &jsonld_default()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "JSON-LD {pin}: {body}");
        let explain = format!("/v1/fluree/explain/{LEDGER}{pin}");
        let (status, body) = sparql(&app, &explain, SPARQL_DEFAULT).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "explain {pin}: {body}");
    }

    let (_, body) = sparql(&app, &query_uri("@t:abc"), SPARQL_DEFAULT).await;
    assert!(body.contains("Invalid time pin"), "{body}");
}

#[tokio::test]
async fn a_body_pin_must_agree_with_the_path_pin() {
    let (_tmp, app, _) = fixture().await;
    let pinned = query_uri("@t:1");

    let from = |iri: &str| {
        format!("PREFIX ex: <http://ex.org/> SELECT ?s ?n FROM <{iri}> WHERE {{ ?s ex:name ?n }}")
    };
    let (status, body) = sparql(&app, &pinned, &from(&format!("{LEDGER}@t:2"))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.contains("Time pin conflict"), "{body}");
    let (status, body) = sparql(&app, &pinned, &from(&format!("{LEDGER}@t:1"))).await;
    assert_eq!(rows(status, &body, &["s", "n"]), t1_default_sparql());

    for from in [
        json!(format!("{LEDGER}@t:2")),
        json!({ "@id": LEDGER, "t": 2 }),
        json!({ "@id": LEDGER, "at": "t:2" }),
    ] {
        let (status, body) =
            jsonld(&app, &pinned, &with(jsonld_default(), "from", from.clone())).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "from {from}: {body}");
        assert!(body.contains("Time pin conflict"), "{body}");
    }
    for from in [
        json!(format!("{LEDGER}@t:1")),
        json!({ "@id": LEDGER, "at": "t:1" }),
    ] {
        let (status, body) =
            jsonld(&app, &pinned, &with(jsonld_default(), "from", from.clone())).await;
        assert_eq!(
            jsonld_rows(status, &body),
            strs(&[&["ex:alice", "Alice-1"]]),
            "from {from}"
        );
    }

    // A history range names its own times.
    let history = with(
        with(jsonld_default(), "from", json!(format!("{LEDGER}@t:1"))),
        "to",
        json!(format!("{LEDGER}@t:latest")),
    );
    let (status, body) = jsonld(&app, &pinned, &history).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    // A dataset that never reads the pinned ledger would drop the pin.
    let elsewhere = with(jsonld_default(), "from", json!(["other:main"]));
    let (status, body) = jsonld(&app, &pinned, &elsewhere).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
}

#[tokio::test]
async fn the_path_pin_applies_to_graphs_the_query_selects() {
    let (_tmp, app, _) = fixture().await;
    let pinned = query_uri("@t:1");

    let (status, body) = sparql(
        &app,
        &pinned,
        &format!("PREFIX ex: <http://ex.org/> SELECT ?s ?l FROM <{G1}> WHERE {{ ?s ex:label ?l }}"),
    )
    .await;
    assert_eq!(
        rows(status, &body, &["s", "l"]),
        strs(&[&["http://ex.org/s1", "g1-v1"]])
    );
    let (status, body) = sparql(
        &app,
        &pinned,
        &format!(
            "PREFIX ex: <http://ex.org/> SELECT ?g ?l FROM NAMED <{G1}> \
             WHERE {{ GRAPH ?g {{ ?s ex:label ?l }} }}"
        ),
    )
    .await;
    assert_eq!(rows(status, &body, &["g", "l"]), strs(&[&[G1, "g1-v1"]]));

    let named = with(
        jsonld_graphs(),
        "fromNamed",
        json!({ "g1": { "@id": LEDGER, "@graph": G1 } }),
    );
    let (status, body) = jsonld(&app, &pinned, &named).await;
    assert_eq!(
        jsonld_rows(status, &body),
        strs(&[&["g1", "ex:s1", "g1-v1"]])
    );
}

#[tokio::test]
async fn sparql_output_formats_read_at_the_pin() {
    let (_tmp, app, _) = fixture().await;
    let pinned = query_uri("@t:1");
    let with_headers = |accept: &'static str, extra: Option<(&'static str, &'static str)>| {
        let mut h = vec![
            ("content-type", "application/sparql-query"),
            ("accept", accept),
        ];
        h.extend(extra);
        h
    };

    let (status, body) = send(
        &app,
        "POST",
        &pinned,
        &with_headers("application/sparql-results+xml", None),
        SPARQL_DEFAULT,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body.contains("Alice-1") && !body.contains("Alice-2"),
        "{body}"
    );

    let (status, body) = send(
        &app,
        "POST",
        &pinned,
        &with_headers("application/json", Some(("fluree-track-fuel", "true"))),
        SPARQL_DEFAULT,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body.contains("Alice-1") && !body.contains("Alice-2"),
        "{body}"
    );

    // Delimited output is not available on the dataset path a pin reads through.
    let (status, body) = send(
        &app,
        "POST",
        &pinned,
        &with_headers("text/csv", None),
        SPARQL_DEFAULT,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_ACCEPTABLE, "{body}");
}

#[tokio::test]
async fn pinned_sparql_honours_the_default_context() {
    let (_tmp, app, _) = fixture().await;
    let (status, body) = send(
        &app,
        "PUT",
        &format!("/v1/fluree/context/{LEDGER}"),
        &[("content-type", "application/json")],
        json!({ "ex": "http://ex.org/" }).to_string(),
    )
    .await;
    assert!(status.is_success(), "{body}");

    let no_prefix = "SELECT ?s ?n WHERE { ?s ex:name ?n }";
    let (status, body) = sparql(&app, &query_uri("@t:1?default-context=true"), no_prefix).await;
    assert_eq!(rows(status, &body, &["s", "n"]), t1_default_sparql());
}

#[tokio::test]
async fn a_path_pin_on_t_waits_for_the_ledger_to_reach_it() {
    let (_tmp, app, _) = fixture_with(|cfg| cfg.query_min_t_timeout_ms = 100).await;
    let ahead = query_uri("@t:9");

    let (status, body) = sparql(&app, &ahead, SPARQL_DEFAULT).await;
    assert_eq!(status, StatusCode::REQUEST_TIMEOUT, "{body}");
    let (status, body) = jsonld(&app, &ahead, &jsonld_default()).await;
    assert_eq!(status, StatusCode::REQUEST_TIMEOUT, "{body}");
}

/// The physical plan of an explain response, as text.
fn physical_plan(status: StatusCode, body: &str) -> String {
    assert_eq!(status, StatusCode::OK, "{body}");
    let json: JsonValue = serde_json::from_str(body).unwrap();
    json["plan"]["physical"].to_string()
}

/// Explain plans against the pinned snapshot. `later.example` is a namespace
/// the ledger first sees at t2, so only a t1 plan leaves the IRI unencoded.
#[tokio::test]
async fn explain_plans_against_the_pinned_snapshot() {
    let (_tmp, app, _) = fixture().await;
    let explain = |pin: &str| format!("/v1/fluree/explain/{LEDGER}{pin}");
    let unencoded = "<http://later.example/bob>";
    let q = "PREFIX ex: <http://ex.org/> SELECT ?n WHERE { <http://later.example/bob> ex:name ?n }";

    let (status, body) = sparql(&app, &explain(""), q).await;
    let head = physical_plan(status, &body);
    assert!(
        head.contains(":bob>") && !head.contains(unencoded),
        "{head}"
    );
    let (status, body) = sparql(&app, &explain("@t:1"), q).await;
    let plan = physical_plan(status, &body);
    assert!(plan.contains(unencoded), "{plan}");

    let jq = json!({
        "@context": { "ex": "http://ex.org/" },
        "select": ["?n"],
        "where": { "@id": "http://later.example/bob", "ex:name": "?n" }
    });
    let (status, body) = jsonld(&app, &explain(""), &jq).await;
    let head = physical_plan(status, &body);
    assert!(!head.contains(unencoded), "{head}");
    let (status, body) = jsonld(&app, &explain("@t:1"), &jq).await;
    let plan = physical_plan(status, &body);
    assert!(plan.contains(unencoded), "{plan}");

    // Explain plans a FROM's own snapshot, so it must repeat the path's pin.
    let from = |iri: &str| {
        format!(
            "PREFIX ex: <http://ex.org/> SELECT ?n FROM <{iri}> \
             WHERE {{ <http://later.example/bob> ex:name ?n }}"
        )
    };
    let (status, body) = sparql(&app, &explain("@t:1"), &from(&format!("{LEDGER}@t:1"))).await;
    let plan = physical_plan(status, &body);
    assert!(plan.contains(unencoded), "{plan}");
    for iri in [LEDGER.to_string(), format!("{LEDGER}@t:2")] {
        let (status, body) = sparql(&app, &explain("@t:1"), &from(&iri)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "FROM <{iri}>: {body}");
    }
}

#[tokio::test]
async fn cypher_reads_at_the_path_pin() {
    let (_tmp, app) = app_with(|_| {}).await;
    let json_ct = [("content-type", "application/json")];
    let (status, _) = send(
        &app,
        "POST",
        "/v1/fluree/create",
        &json_ct,
        json!({ "ledger": "cy" }).to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    for name in ["Alice", "Bob"] {
        let person =
            json!({ "@context": {}, "@id": name.to_lowercase(), "@type": "Person", "name": name });
        let (status, body) = send(
            &app,
            "POST",
            "/v1/fluree/insert/cy",
            &json_ct,
            person.to_string(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    let cypher = [("content-type", "application/cypher")];
    let match_people = "MATCH (n:Person) RETURN n.name";

    let (status, body) = send(&app, "POST", "/v1/fluree/query/cy", &cypher, match_people).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("Bob"), "head, for contrast: {body}");
    let (status, body) = send(
        &app,
        "POST",
        "/v1/fluree/query/cy@t:1",
        &cypher,
        match_people,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("Alice") && !body.contains("Bob"), "{body}");

    // Explain carries the pin to the loader: a commit that is not there fails.
    let (status, body) = send(
        &app,
        "POST",
        "/v1/fluree/explain/cy@t:1",
        &cypher,
        match_people,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = send(
        &app,
        "POST",
        "/v1/fluree/explain/cy@commit:ffffff",
        &cypher,
        match_people,
    )
    .await;
    assert!(!status.is_success(), "{body}");
    assert!(body.contains("No commit found"), "{body}");
}

#[tokio::test]
async fn graph_store_get_reads_at_the_path_pin() {
    let (_tmp, app, _) = fixture().await;
    let ntriples = [("accept", "application/n-triples")];
    let data = |rest: &str| format!("/v1/fluree/data/{LEDGER}{rest}");

    let (status, body) = send(
        &app,
        "GET",
        &data(&format!("@t:1?graph={G1}")),
        &ntriples,
        "",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("g1-v1") && !body.contains("g1-v2"), "{body}");

    let (status, body) = send(
        &app,
        "GET",
        &data(&format!("@t:1?graph={G2}")),
        &ntriples,
        "",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "g2 does not exist at t1: {body}"
    );

    let (status, body) = send(&app, "GET", &data("@t:1?default"), &ntriples, "").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("Alice-1") && !body.contains("Bob"), "{body}");
}

#[tokio::test]
async fn streaming_refuses_a_path_pin() {
    let (_tmp, app, _) = fixture().await;
    let (status, body) = sparql(
        &app,
        &format!("/v1/fluree/stream/query/{LEDGER}@t:1"),
        SPARQL_GRAPHS,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
}
