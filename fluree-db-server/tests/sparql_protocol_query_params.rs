//! SPARQL query parameters over HTTP: `params`, a JSON object binding
//! variables to values for one request, read from the URL or a form-encoded
//! body on every SPARQL query route — ledger (each response lane), connection,
//! explain and stream.
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

/// `GET {path}?query=…&params=…`.
async fn get_with_params(
    app: &axum::Router,
    path: &str,
    sparql: &str,
    params: &str,
) -> (StatusCode, JsonValue) {
    send(
        app,
        Request::builder()
            .method("GET")
            .uri(format!(
                "{path}?query={}&params={}",
                enc(sparql),
                enc(params)
            ))
            .body(Body::empty())
            .unwrap(),
    )
    .await
}

/// `POST {path}?params=…` with the query as an `application/sparql-query` body.
fn direct_post(path: &str, sparql: &str, params: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(format!("{path}?params={}", enc(params)))
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

    let (status, json) = get_with_params(&app, &ledger_path(), BY_NAME, r#"{"n": "B"}"#).await;

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
        r#"{"s": {"@id": "http://ex.org/s3"}}"#,
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
        format!("query={}&params={}", enc(BY_NAME), enc(r#"{"n": "A"}"#)),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(values(&json, "s"), [S1], "{json}");
}

#[tokio::test]
async fn direct_post_takes_params_from_the_url() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = send(&app, direct_post(&ledger_path(), BY_NAME, r#"{"n": "B"}"#)).await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(values(&json, "s"), [S2], "{json}");
}

/// One query text, run with two bindings and then none: each answer is its
/// own, so a binding never outlives its request (the parsed text is shared
/// across requests; the substitution is not).
#[tokio::test]
async fn one_text_answers_each_binding_and_the_unbound_query() {
    let (_tmp, app) = seeded_app().await;

    let (_, a) = get_with_params(&app, &ledger_path(), BY_NAME, r#"{"n": "A"}"#).await;
    let (_, c) = get_with_params(&app, &ledger_path(), BY_NAME, r#"{"n": "C"}"#).await;
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
    let mut request = direct_post(&ledger_path(), BY_NAME, r#"{"n": "B"}"#);
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

    let (status, d) = get_with_params(&app, &ledger_path(), in_g1, r#"{"n": "D"}"#).await;
    let (_, a) = get_with_params(&app, &ledger_path(), in_g1, r#"{"n": "A"}"#).await;

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
        get_with_params(&app, "/v1/fluree/query", &from_ledger, r#"{"n": "C"}"#).await;

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
        let mut request = direct_post(&path, &sparql, r#"{"n": "B"}"#);
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
            r#"{"s": {"@id": "http://ex.org/s1"}}"#,
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{plan}");
    assert!(plan.to_string().contains("s1"), "{plan}");
}

#[tokio::test]
async fn stream_route_binds_params() {
    let (_tmp, app) = seeded_app().await;

    let (status, body) = send_raw(
        &app,
        direct_post(
            &format!("/v1/fluree/stream/query/{LEDGER}"),
            BY_NAME,
            r#"{"n": "C"}"#,
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

/// The AgentJson lanes bind `params`, and a response the byte budget cuts
/// gives no resume query: one built from the text alone would run unbound and
/// answer a wider question on the next page. It says to re-send the `params`.
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
                "{path}?query={}&params={}",
                enc(sparql),
                enc(r#"{"c": {"@id": "http://ex.org/c1"}}"#)
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
            message.contains("re-send this query with the same params"),
            "{lane}: {json}"
        );
    }
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

#[tokio::test]
async fn params_must_be_a_json_object() {
    let (_tmp, app) = seeded_app().await;

    for params in [r#"["A"]"#, "n=A", r#""A""#] {
        let (status, json) = get_with_params(&app, &ledger_path(), BY_NAME, params).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{params}: {json}");
        assert!(json.to_string().contains("JSON object"), "{json}");
    }
}

#[tokio::test]
async fn params_may_appear_only_once() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = post_form(
        &app,
        &format!("{}?params={}", ledger_path(), enc(r#"{"n": "A"}"#)),
        format!("query={}&params={}", enc(BY_NAME), enc(r#"{"n": "B"}"#)),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
    assert!(json.to_string().contains("only once"), "{json}");
}

/// A misspelt name must fail: left alone, its variable would stay unbound.
#[tokio::test]
async fn a_parameter_the_query_never_mentions_is_rejected() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = get_with_params(&app, &ledger_path(), BY_NAME, r#"{"name": "A"}"#).await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
}

#[tokio::test]
async fn params_on_a_jsonld_query_are_rejected() {
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
            .uri(format!("{}?params={}", ledger_path(), enc(r#"{"n": "A"}"#)))
            .header("content-type", "application/json")
            .body(Body::from(query.to_string()))
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
    assert!(json.to_string().contains("only to SPARQL"), "{json}");
}

/// Cypher takes its parameters in its JSON body, never from `params`.
#[tokio::test]
async fn params_on_a_cypher_query_are_rejected() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = send(
        &app,
        Request::builder()
            .method("POST")
            .uri(format!("{}?params={}", ledger_path(), enc(r#"{"n": "A"}"#)))
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
async fn params_on_an_update_are_rejected_and_nothing_changes() {
    let (_tmp, app) = seeded_app().await;
    let delete = "DELETE WHERE { ?s <http://ex.org/name> ?n }";
    let update_path = format!("/v1/fluree/update/{LEDGER}");

    let (url_status, url_json) = send(
        &app,
        Request::builder()
            .method("POST")
            .uri(format!("{update_path}?params={}", enc(r#"{"n": "A"}"#)))
            .header("content-type", "application/sparql-update")
            .body(Body::from(delete))
            .unwrap(),
    )
    .await;
    let (form_status, form_json) = post_form(
        &app,
        &update_path,
        format!("update={}&params={}", enc(delete), enc(r#"{"n": "A"}"#)),
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
