//! SPARQL query parameters over HTTP: one `$name=<RDF term>` parameter per
//! variable (the convention other SPARQL servers share), binding it for one
//! request, read from the URL or a form-encoded body on every SPARQL query
//! route — ledger (each response lane), connection, explain and stream.
//!
//! A route that dropped `params` would leave the variable unbound and widen
//! the match rather than fail, so each one is pinned here.

use axum::body::Body;
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

const LEDGER: &str = "qparams:main";

/// The default graph names `A`, `B` and `C`; graph `g1` names `D`.
const SEED_TRIG: &str = r#"
@prefix ex: <http://ex.org/> .

ex:s1 ex:name "A" .
ex:s2 ex:name "B" .
ex:s3 ex:name "C" .

<http://ex.org/g1> {
    ex:s4 ex:name "D" .
}
"#;

const BY_NAME: &str = "PREFIX ex: <http://ex.org/> SELECT ?s WHERE { ?s ex:name ?n }";
const BY_SUBJECT: &str = "PREFIX ex: <http://ex.org/> SELECT ?n WHERE { ?s ex:name ?n }";

const S1: &str = "http://ex.org/s1";
const S2: &str = "http://ex.org/s2";
const S3: &str = "http://ex.org/s3";

async fn seeded_app() -> (TempDir, axum::Router) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    let app = build_router(state);

    let (status, body) = send(
        &app,
        Request::builder()
            .method("POST")
            .uri("/v1/fluree/create")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({ "ledger": LEDGER }).to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create ledger: {body}");

    let (status, body) = send(
        &app,
        Request::builder()
            .method("POST")
            .uri(format!("/v1/fluree/upsert/{LEDGER}"))
            .header("content-type", "application/trig")
            .body(Body::from(SEED_TRIG))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "trig upsert: {body}");

    (tmp, app)
}

async fn send_raw(app: &axum::Router, request: Request<Body>) -> (StatusCode, String) {
    let resp = app.clone().oneshot(request).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

async fn send(app: &axum::Router, request: Request<Body>) -> (StatusCode, JsonValue) {
    let (status, text) = send_raw(app, request).await;
    let json = serde_json::from_str(&text).unwrap_or(JsonValue::String(text));
    (status, json)
}

fn enc(s: &str) -> String {
    urlencoding::encode(s).into_owned()
}

fn ledger_path() -> String {
    format!("/v1/fluree/query/{LEDGER}")
}

/// `$name=<term>` pairs, form- or URL-encoded (`$` as `%24`, as clients send it).
fn bindings(pairs: &[(&str, &str)]) -> String {
    pairs
        .iter()
        .map(|(name, term)| format!("{}={}", enc(&format!("${name}")), enc(term)))
        .collect::<Vec<_>>()
        .join("&")
}

/// `GET {path}?query=…&$name=…`.
async fn get_with_params(
    app: &axum::Router,
    path: &str,
    sparql: &str,
    params: &[(&str, &str)],
) -> (StatusCode, JsonValue) {
    send(
        app,
        Request::builder()
            .method("GET")
            .uri(format!("{path}?query={}&{}", enc(sparql), bindings(params)))
            .body(Body::empty())
            .unwrap(),
    )
    .await
}

/// `POST {path}?$name=…` with the query as an `application/sparql-query` body.
fn direct_post(path: &str, sparql: &str, params: &[(&str, &str)]) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(format!("{path}?{}", bindings(params)))
        .header("content-type", "application/sparql-query")
        .body(Body::from(sparql.to_string()))
        .unwrap()
}

async fn post_form(app: &axum::Router, path: &str, form: String) -> (StatusCode, JsonValue) {
    send(
        app,
        Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from(form))
            .unwrap(),
    )
    .await
}

/// Sorted values of `?var` from a SPARQL-results JSON body.
fn values(json: &JsonValue, var: &str) -> Vec<String> {
    let mut out: Vec<String> = json
        .pointer("/results/bindings")
        .and_then(JsonValue::as_array)
        .unwrap_or_else(|| panic!("expected SPARQL-results bindings, got {json}"))
        .iter()
        .map(|row| {
            row.pointer(&format!("/{var}/value"))
                .and_then(JsonValue::as_str)
                .unwrap_or_else(|| panic!("no ?{var} in {row}"))
                .to_string()
        })
        .collect();
    out.sort();
    out
}

// ---------------------------------------------------------------------------
// Transports
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_binds_a_literal_parameter() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = get_with_params(&app, &ledger_path(), BY_NAME, &[("n", r#""B""#)]).await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(values(&json, "s"), [S2], "{json}");
}

#[tokio::test]
async fn get_binds_an_iri_parameter() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = get_with_params(
        &app,
        &ledger_path(),
        BY_SUBJECT,
        &[("s", "<http://ex.org/s3>")],
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(values(&json, "n"), ["C"], "{json}");
}

#[tokio::test]
async fn form_encoded_post_takes_params_from_the_body() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = post_form(
        &app,
        &ledger_path(),
        format!("query={}&{}", enc(BY_NAME), bindings(&[("n", r#""A""#)])),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(values(&json, "s"), [S1], "{json}");
}

#[tokio::test]
async fn direct_post_takes_params_from_the_url() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = send(
        &app,
        direct_post(&ledger_path(), BY_NAME, &[("n", r#""B""#)]),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(values(&json, "s"), [S2], "{json}");
}

/// Clients send `$` raw or as `%24`; both name the binding.
#[tokio::test]
async fn a_raw_dollar_in_the_url_names_the_binding() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = send(
        &app,
        Request::builder()
            .method("GET")
            .uri(format!(
                "{}?query={}&$n={}",
                ledger_path(),
                enc(BY_NAME),
                enc(r#""C""#)
            ))
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(values(&json, "s"), [S3], "{json}");
}

/// A number matches its stored value, written bare or as a typed literal,
/// and keeps the datatype it would have written in the query: `30` is an
/// `xsd:integer`, `1.5` an `xsd:decimal`, `1.5e0` an `xsd:double`.
#[tokio::test]
async fn a_number_binds_with_its_datatype() {
    let (_tmp, app) = seeded_app().await;
    let (status, body) = send(
        &app,
        Request::builder()
            .method("POST")
            .uri(format!("/v1/fluree/upsert/{LEDGER}"))
            .header("content-type", "text/turtle")
            .body(Body::from(
                "@prefix ex: <http://ex.org/> . ex:s1 ex:age 30 ; ex:score 1.5 .",
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "numbers upsert: {body}");

    let by = |p: &str| format!("PREFIX ex: <http://ex.org/> SELECT ?s WHERE {{ ?s ex:{p} $v }}");
    for (p, term) in [
        ("age", "30"),
        ("age", r#""30"^^<http://www.w3.org/2001/XMLSchema#integer>"#),
        ("score", "1.5"),
        (
            "score",
            r#""1.5"^^<http://www.w3.org/2001/XMLSchema#decimal>"#,
        ),
    ] {
        let (status, json) = get_with_params(&app, &ledger_path(), &by(p), &[("v", term)]).await;
        assert_eq!(status, StatusCode::OK, "{p} {term}: {json}");
        assert_eq!(values(&json, "s"), [S1], "{p} {term}: {json}");
    }

    // Matching compares numbers by value, so the datatype shows in DATATYPE.
    let datatype = "SELECT ?dt WHERE { BIND(DATATYPE($v) AS ?dt) }";
    let xsd = |t: &str| format!("http://www.w3.org/2001/XMLSchema#{t}");
    for (term, expected) in [
        ("30", xsd("integer")),
        ("1.5", xsd("decimal")),
        ("1.5e0", xsd("double")),
    ] {
        let (status, json) = get_with_params(&app, &ledger_path(), datatype, &[("v", term)]).await;
        assert_eq!(status, StatusCode::OK, "{term}: {json}");
        assert_eq!(values(&json, "dt"), [expected], "{term}: {json}");
    }
}

/// One query text, run with two bindings and then none: each answer is its
/// own, so a binding never outlives its request (the parsed text is shared
/// across requests; the substitution is not).
#[tokio::test]
async fn one_text_answers_each_binding_and_the_unbound_query() {
    let (_tmp, app) = seeded_app().await;

    let (_, a) = get_with_params(&app, &ledger_path(), BY_NAME, &[("n", r#""A""#)]).await;
    let (_, c) = get_with_params(&app, &ledger_path(), BY_NAME, &[("n", r#""C""#)]).await;
    let (status, all) = send(
        &app,
        Request::builder()
            .method("GET")
            .uri(format!("{}?query={}", ledger_path(), enc(BY_NAME)))
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(values(&a, "s"), [S1], "{a}");
    assert_eq!(values(&c, "s"), [S3], "{c}");
    assert_eq!(status, StatusCode::OK, "{all}");
    assert_eq!(values(&all, "s"), [S1, S2, S3], "{all}");
}

// ---------------------------------------------------------------------------
// Routes and response lanes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sparql_results_xml_lane_binds_params() {
    let (_tmp, app) = seeded_app().await;
    let mut request = direct_post(&ledger_path(), BY_NAME, &[("n", r#""B""#)]);
    request
        .headers_mut()
        .insert("accept", "application/sparql-results+xml".parse().unwrap());

    let (status, xml) = send_raw(&app, request).await;

    assert_eq!(status, StatusCode::OK, "{xml}");
    assert!(xml.contains(S2), "{xml}");
    assert!(!xml.contains(S1) && !xml.contains(S3), "{xml}");
}

/// A `FROM` clause sends the ledger route through its dataset lane.
#[tokio::test]
async fn dataset_lane_binds_params() {
    let (_tmp, app) = seeded_app().await;
    let in_g1 =
        "PREFIX ex: <http://ex.org/> SELECT ?s FROM <http://ex.org/g1> WHERE { ?s ex:name ?n }";

    let (status, d) = get_with_params(&app, &ledger_path(), in_g1, &[("n", r#""D""#)]).await;
    let (_, a) = get_with_params(&app, &ledger_path(), in_g1, &[("n", r#""A""#)]).await;

    assert_eq!(status, StatusCode::OK, "{d}");
    assert_eq!(values(&d, "s"), ["http://ex.org/s4"], "{d}");
    assert!(values(&a, "s").is_empty(), "A is not in g1: {a}");
}

#[tokio::test]
async fn connection_route_binds_params() {
    let (_tmp, app) = seeded_app().await;
    let from_ledger =
        format!("PREFIX ex: <http://ex.org/> SELECT ?s FROM <{LEDGER}> WHERE {{ ?s ex:name ?n }}");

    let (status, json) =
        get_with_params(&app, "/v1/fluree/query", &from_ledger, &[("n", r#""C""#)]).await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(values(&json, "s"), [S3], "{json}");
}

/// Tracking headers send both routes through their tracked lanes.
#[tokio::test]
async fn tracked_lanes_bind_params() {
    let (_tmp, app) = seeded_app().await;
    let from_ledger =
        format!("PREFIX ex: <http://ex.org/> SELECT ?s FROM <{LEDGER}> WHERE {{ ?s ex:name ?n }}");

    for (path, sparql) in [
        (ledger_path(), BY_NAME.to_string()),
        ("/v1/fluree/query".to_string(), from_ledger),
    ] {
        let mut request = direct_post(&path, &sparql, &[("n", r#""B""#)]);
        request
            .headers_mut()
            .insert("fluree-track-fuel", "true".parse().unwrap());

        let (status, body) = send_raw(&app, request).await;

        assert_eq!(status, StatusCode::OK, "{path}: {body}");
        assert!(body.contains(S2), "{path}: {body}");
        assert!(!body.contains(S1) && !body.contains(S3), "{path}: {body}");
    }
}

#[tokio::test]
async fn explain_plans_the_bound_query() {
    let (_tmp, app) = seeded_app().await;

    let (status, plan) = send(
        &app,
        direct_post(
            &format!("/v1/fluree/explain/{LEDGER}"),
            BY_SUBJECT,
            &[("s", "<http://ex.org/s1>")],
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{plan}");
    assert!(plan.to_string().contains("s1"), "{plan}");
}

/// Each explain lane a `FROM` selects plans the bound query: the connection
/// route at the head, the connection route's dataset lane (a time-travel
/// `fluree-ledger` header), and the ledger route's connection lane.
#[tokio::test]
async fn explain_with_a_from_plans_the_bound_query() {
    let (_tmp, app) = seeded_app().await;
    let from_ledger =
        format!("PREFIX ex: <http://ex.org/> SELECT ?n FROM <{LEDGER}> WHERE {{ ?s ex:name ?n }}");
    let pinned = format!("{LEDGER}@t:1");

    for (path, ledger_header) in [
        ("/v1/fluree/explain".to_string(), None),
        ("/v1/fluree/explain".to_string(), Some(pinned.as_str())),
        (format!("/v1/fluree/explain/{LEDGER}"), None),
    ] {
        let mut request = direct_post(&path, &from_ledger, &[("s", "<http://ex.org/s1>")]);
        if let Some(ledger) = ledger_header {
            request
                .headers_mut()
                .insert("fluree-ledger", ledger.parse().unwrap());
        }
        let (status, plan) = send(&app, request).await;

        assert_eq!(status, StatusCode::OK, "{path} {ledger_header:?}: {plan}");
        assert!(
            plan.to_string().contains("s1"),
            "{path} {ledger_header:?}: {plan}"
        );
    }
}

/// The connection stream route, which names its ledger with `FROM`.
#[tokio::test]
async fn connection_stream_route_binds_params() {
    let (_tmp, app) = seeded_app().await;
    let from_ledger =
        format!("PREFIX ex: <http://ex.org/> SELECT ?s FROM <{LEDGER}> WHERE {{ ?s ex:name ?n }}");

    let (status, body) = send_raw(
        &app,
        direct_post("/v1/fluree/stream/query", &from_ledger, &[("n", r#""C""#)]),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains(S3), "{body}");
    assert!(!body.contains(S1) && !body.contains(S2), "{body}");
}

#[tokio::test]
async fn stream_route_binds_params() {
    let (_tmp, app) = seeded_app().await;

    let (status, body) = send_raw(
        &app,
        direct_post(
            &format!("/v1/fluree/stream/query/{LEDGER}"),
            BY_NAME,
            &[("n", r#""C""#)],
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains(S3), "{body}");
    assert!(!body.contains(S1) && !body.contains(S2), "{body}");
}

/// Categories for the AgentJson cases: two subjects in `ex:c1` and one in
/// `ex:c2`, in the default graph and in graph `g1`.
const CATEGORY_TRIG: &str = r"
@prefix ex: <http://ex.org/> .

ex:s1 ex:cat ex:c1 .
ex:s2 ex:cat ex:c1 .
ex:s3 ex:cat ex:c2 .

<http://ex.org/g1> {
    ex:s4 ex:cat ex:c1 .
    ex:s5 ex:cat ex:c1 .
    ex:s6 ex:cat ex:c2 .
}
";

/// The AgentJson lanes bind `$name` parameters, and a response the byte budget
/// cuts gives no resume query: one built from the text alone would run unbound
/// and answer a wider question on the next page. It says to re-send them.
#[tokio::test]
async fn agent_json_lanes_bind_params_and_withhold_the_resume_query() {
    let (_tmp, app) = seeded_app().await;
    let (status, body) = send(
        &app,
        Request::builder()
            .method("POST")
            .uri(format!("/v1/fluree/upsert/{LEDGER}"))
            .header("content-type", "application/trig")
            .body(Body::from(CATEGORY_TRIG))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "category upsert: {body}");

    let by_cat = |from: &str| {
        format!("PREFIX ex: <http://ex.org/> SELECT ?s {from} WHERE {{ ?s ex:cat ?c }} ORDER BY ?s")
    };
    // (lane, path, query, the bound query's subjects in order)
    let cases = [
        (
            "connection",
            "/v1/fluree/query".to_string(),
            by_cat(&format!("FROM <{LEDGER}>")),
            ["ex:s1", "ex:s2"],
        ),
        ("ledger", ledger_path(), by_cat(""), ["ex:s1", "ex:s2"]),
        (
            "ledger dataset",
            ledger_path(),
            by_cat("FROM <http://ex.org/g1>"),
            ["ex:s4", "ex:s5"],
        ),
    ];
    let subjects = |lane: &str, json: &JsonValue| -> Vec<String> {
        json["rows"]
            .as_array()
            .unwrap_or_else(|| panic!("{lane}: expected AgentJson rows, got {json}"))
            .iter()
            .map(|row| row["?s"].as_str().unwrap_or_default().to_string())
            .collect()
    };
    let agent_json = |path: &str, sparql: &str, tracked: bool, max_bytes: Option<&str>| {
        let mut request = Request::builder()
            .method("GET")
            .uri(format!(
                "{path}?query={}&{}",
                enc(sparql),
                bindings(&[("c", "<http://ex.org/c1>")])
            ))
            .header("accept", "application/vnd.fluree.agent+json");
        if tracked {
            request = request.header("fluree-track-fuel", "true");
        }
        if let Some(max_bytes) = max_bytes {
            request = request.header("fluree-max-bytes", max_bytes);
        }
        request.body(Body::empty()).unwrap()
    };

    // The connection route also has a tracked AgentJson lane; on the ledger
    // route a tracked request answers in the tracking envelope instead.
    let runs = cases
        .iter()
        .map(|case| (case, false))
        .chain(std::iter::once((&cases[0], true)));
    for ((lane, path, sparql, bound), tracked) in runs {
        let lane = format!("{lane}{}", if tracked { ", tracked" } else { "" });
        let (status, json) = send(&app, agent_json(path, sparql, tracked, None)).await;
        assert_eq!(status, StatusCode::OK, "{lane}: {json}");
        assert_eq!(subjects(&lane, &json), bound, "{lane}: {json}");

        let (status, json) = send(&app, agent_json(path, sparql, tracked, Some("1"))).await;
        assert_eq!(status, StatusCode::OK, "{lane}: {json}");
        assert_eq!(subjects(&lane, &json), bound[..1], "{lane}: {json}");
        assert_eq!(json["hasMore"], JsonValue::Bool(true), "{lane}: {json}");
        assert!(
            json.get("resume").is_none(),
            "{lane} gave an unbound resume query: {json}"
        );
        let message = json["message"].as_str().unwrap_or_default();
        assert!(
            message.contains("re-send this query with the same variable bindings"),
            "{lane}: {json}"
        );
    }
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_binding_must_be_one_full_rdf_term() {
    let (_tmp, app) = seeded_app().await;

    for (term, says) in [
        ("A", "one RDF term"),
        (r#""A" "B""#, "one RDF term"),
        ("ex:a", "prefixed name"),
    ] {
        let (status, json) = get_with_params(&app, &ledger_path(), BY_NAME, &[("n", term)]).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{term}: {json}");
        assert!(json.to_string().contains(says), "{term}: {json}");
    }
}

#[tokio::test]
async fn a_variable_may_be_bound_only_once() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = post_form(
        &app,
        &format!("{}?{}", ledger_path(), bindings(&[("n", r#""A""#)])),
        format!("query={}&{}", enc(BY_NAME), bindings(&[("n", r#""B""#)])),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
    assert!(json.to_string().contains("only once"), "{json}");
}

/// `params` is how a Cypher body spells its bindings; sent to a SPARQL
/// query it is refused with the spelling to use, rather than ignored.
#[tokio::test]
async fn the_params_spelling_is_refused_with_a_pointer() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = send(
        &app,
        Request::builder()
            .method("GET")
            .uri(format!(
                "{}?query={}&params={}",
                ledger_path(),
                enc(BY_NAME),
                enc(r#"{"n": "A"}"#)
            ))
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
    assert!(json.to_string().contains("$name"), "{json}");
}

/// A misspelt name must fail: left alone, its variable would stay unbound.
#[tokio::test]
async fn a_parameter_the_query_never_mentions_is_rejected() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) =
        get_with_params(&app, &ledger_path(), BY_NAME, &[("name", r#""A""#)]).await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
}

#[tokio::test]
async fn bindings_on_a_jsonld_query_are_rejected() {
    let (_tmp, app) = seeded_app().await;
    let query = serde_json::json!({
        "@context": { "ex": "http://ex.org/" },
        "select": ["?s"],
        "where": { "@id": "?s", "ex:name": "?n" }
    });

    let (status, json) = send(
        &app,
        Request::builder()
            .method("POST")
            .uri(format!(
                "{}?{}",
                ledger_path(),
                bindings(&[("n", r#""A""#)])
            ))
            .header("content-type", "application/json")
            .body(Body::from(query.to_string()))
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
    assert!(json.to_string().contains("only to SPARQL"), "{json}");
}

/// Cypher takes its parameters in its JSON body, never from `$name`.
#[tokio::test]
async fn bindings_on_a_cypher_query_are_rejected() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = send(
        &app,
        Request::builder()
            .method("POST")
            .uri(format!(
                "{}?{}",
                ledger_path(),
                bindings(&[("n", r#""A""#)])
            ))
            .header("content-type", "application/cypher")
            .body(Body::from("MATCH (n) RETURN n LIMIT 1"))
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
    assert!(json.to_string().contains("only to SPARQL"), "{json}");
}

/// The update endpoint takes no bindings. Ignoring one would leave its
/// variable unbound — this `DELETE WHERE` would delete every name — so the
/// request fails and the data is untouched, from the URL and from a form.
#[tokio::test]
async fn bindings_on_an_update_are_rejected_and_nothing_changes() {
    let (_tmp, app) = seeded_app().await;
    let delete = "DELETE WHERE { ?s <http://ex.org/name> ?n }";
    let update_path = format!("/v1/fluree/update/{LEDGER}");

    let (url_status, url_json) = send(
        &app,
        Request::builder()
            .method("POST")
            .uri(format!("{update_path}?{}", bindings(&[("n", r#""A""#)])))
            .header("content-type", "application/sparql-update")
            .body(Body::from(delete))
            .unwrap(),
    )
    .await;
    let (form_status, form_json) = post_form(
        &app,
        &update_path,
        format!("update={}&{}", enc(delete), bindings(&[("n", r#""A""#)])),
    )
    .await;
    let (_, all) = send(
        &app,
        Request::builder()
            .method("GET")
            .uri(format!("{}?query={}", ledger_path(), enc(BY_NAME)))
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(url_status, StatusCode::BAD_REQUEST, "{url_json}");
    assert_eq!(form_status, StatusCode::BAD_REQUEST, "{form_json}");
    assert!(
        url_json.to_string().contains("only by the SPARQL query"),
        "{url_json}"
    );
    assert_eq!(values(&all, "s"), [S1, S2, S3], "{all}");
}
