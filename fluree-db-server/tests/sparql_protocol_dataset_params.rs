//! SPARQL 1.1 Protocol dataset parameters over HTTP: `default-graph-uri` and
//! `named-graph-uri` for queries (§2.1.4), `using-graph-uri` and
//! `using-named-graph-uri` for updates (§2.2.3).
//!
//! These were ignored (`default-graph-uri` was parsed and discarded), and a
//! repeated parameter — the normal way to name two graphs — was a 400
//! "duplicate field". On the update routes a repeated key silently discarded
//! every parameter, `ledger` included.

use axum::body::Body;
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

const LEDGER: &str = "protods:main";
const G1: &str = "http://ex.org/g1";
const G2: &str = "http://ex.org/g2";

/// The default graph names `D`; `g1` names `A` and `B`; `g2` names `C`. Every
/// expected answer below differs from the default graph's.
const SEED_TRIG: &str = r#"
@prefix ex: <http://ex.org/> .

ex:d1 ex:name "D" .

<http://ex.org/g1> {
    ex:s1 ex:name "A" .
    ex:s2 ex:name "B" .
}

<http://ex.org/g2> {
    ex:s3 ex:name "C" .
}
"#;

const NAMES: &str = "PREFIX ex: <http://ex.org/> SELECT ?n WHERE { ?s ex:name ?n }";

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

async fn send(app: &axum::Router, request: Request<Body>) -> (StatusCode, JsonValue) {
    let resp = app.clone().oneshot(request).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| JsonValue::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, json)
}

fn enc(s: &str) -> String {
    urlencoding::encode(s).into_owned()
}

/// `GET {path}?query=…&{extra}`.
async fn get_query(
    app: &axum::Router,
    path: &str,
    sparql: &str,
    extra: &str,
) -> (StatusCode, JsonValue) {
    send(
        app,
        Request::builder()
            .method("GET")
            .uri(format!("{path}?query={}&{extra}", enc(sparql)))
            .body(Body::empty())
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

fn ledger_path() -> String {
    format!("/v1/fluree/query/{LEDGER}")
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn default_graph_uri_sets_the_default_graph() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = get_query(
        &app,
        &ledger_path(),
        NAMES,
        &format!("default-graph-uri={}", enc(G1)),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(values(&json, "n"), ["A", "B"], "{json}");
}

/// Two graphs is two parameters. This was a 400 "duplicate field".
#[tokio::test]
async fn repeated_default_graph_uri_merges_the_graphs() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = get_query(
        &app,
        &ledger_path(),
        NAMES,
        &format!(
            "default-graph-uri={}&default-graph-uri={}",
            enc(G1),
            enc(G2)
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(values(&json, "n"), ["A", "B", "C"], "{json}");
}

#[tokio::test]
async fn named_graph_uri_sets_the_named_graphs() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = get_query(
        &app,
        &ledger_path(),
        "PREFIX ex: <http://ex.org/> SELECT ?g ?n WHERE { GRAPH ?g { ?s ex:name ?n } }",
        &format!("named-graph-uri={}", enc(G2)),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(values(&json, "g"), [G2], "{json}");
    assert_eq!(values(&json, "n"), ["C"], "{json}");
}

/// Protocol §2.1.4: the protocol dataset takes precedence over the query's.
#[tokio::test]
async fn protocol_dataset_overrides_the_query_from_clause() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = get_query(
        &app,
        &ledger_path(),
        "PREFIX ex: <http://ex.org/> SELECT ?n FROM <http://ex.org/g1> WHERE { ?s ex:name ?n }",
        &format!("default-graph-uri={}", enc(G2)),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(values(&json, "n"), ["C"], "{json}");
}

/// POST with the query as the body (`application/sparql-query`) takes the
/// dataset parameters from the URL.
#[tokio::test]
async fn post_direct_takes_dataset_parameters_from_the_url() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = send(
        &app,
        Request::builder()
            .method("POST")
            .uri(format!("{}?default-graph-uri={}", ledger_path(), enc(G2)))
            .header("content-type", "application/sparql-query")
            .body(Body::from(NAMES))
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(values(&json, "n"), ["C"], "{json}");
}

/// The connection-scoped route needs a dataset to find its ledger; a
/// `default-graph-uri` naming the ledger provides it, as `FROM` would.
#[tokio::test]
async fn connection_route_takes_its_ledger_from_default_graph_uri() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = get_query(
        &app,
        "/v1/fluree/query",
        NAMES,
        &format!("default-graph-uri={}", enc(LEDGER)),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(values(&json, "n"), ["D"], "{json}");
}

#[tokio::test]
async fn dataset_parameters_on_a_jsonld_query_are_rejected() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = send(
        &app,
        Request::builder()
            .method("POST")
            .uri(format!("{}?default-graph-uri={}", ledger_path(), enc(G1)))
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "@context": {"ex": "http://ex.org/"},
                    "select": ["?n"],
                    "where": {"@id": "?s", "ex:name": "?n"}
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
}

#[tokio::test]
async fn a_value_that_is_not_an_iri_is_rejected() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = get_query(
        &app,
        &ledger_path(),
        NAMES,
        &format!("default-graph-uri={}", enc("http://ex.org/a b")),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
}

/// Replacing a `FROM` that pins a time would silently turn a snapshot read
/// into a current-head read, so the protocol dataset refuses it.
#[tokio::test]
async fn protocol_dataset_does_not_replace_a_time_pinned_from() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = get_query(
        &app,
        &ledger_path(),
        &format!("PREFIX ex: <http://ex.org/> SELECT ?n FROM <{G1}@t:1> WHERE {{ ?s ex:name ?n }}"),
        &format!("default-graph-uri={}", enc(G2)),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
    assert!(json.to_string().contains("pins a time"), "{json}");
}

/// Protocol §2.1.2: query via POST with URL-encoded parameters in the body.
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

/// A form-encoded query body used to be taken as raw SPARQL (`query=SELECT…`)
/// and fail to parse.
#[tokio::test]
async fn form_encoded_query_post_is_accepted() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = post_form(&app, &ledger_path(), format!("query={}", enc(NAMES))).await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(values(&json, "n"), ["D"], "{json}");
}

#[tokio::test]
async fn form_encoded_query_takes_dataset_parameters_from_the_body() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = post_form(
        &app,
        &ledger_path(),
        format!(
            "query={}&default-graph-uri={}&default-graph-uri={}",
            enc(NAMES),
            enc(G1),
            enc(G2)
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(values(&json, "n"), ["A", "B", "C"], "{json}");
}

#[tokio::test]
async fn form_encoded_query_on_the_connection_route() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = post_form(
        &app,
        "/v1/fluree/query",
        format!("query={}&default-graph-uri={}", enc(NAMES), enc(LEDGER)),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(values(&json, "n"), ["D"], "{json}");
}

/// A form-encoded update belongs on the update endpoint — refused as an
/// update, not merely as an unparseable body.
#[tokio::test]
async fn form_encoded_update_on_the_query_endpoint_is_refused() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = post_form(
        &app,
        &ledger_path(),
        format!(
            "update={}",
            enc("PREFIX ex: <http://ex.org/> INSERT DATA { ex:x ex:name \"X\" }")
        ),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
    assert!(
        json.to_string().contains("/v1/fluree/update"),
        "the refusal must point at the update endpoint: {json}"
    );
    let (_, names) = get_query(&app, &ledger_path(), NAMES, "").await;
    assert_eq!(values(&names, "n"), ["D"], "nothing may be written");
}

// ---------------------------------------------------------------------------
// Updates
// ---------------------------------------------------------------------------

/// Marks every subject the WHERE clause matches. The mark lands in the
/// default graph, so which subjects carry it shows which graph WHERE read.
const MARK_NAMED: &str =
    "PREFIX ex: <http://ex.org/> INSERT { ?s ex:seen true } WHERE { ?s ex:name ?n }";

async fn update(
    app: &axum::Router,
    uri: &str,
    content_type: &str,
    body: String,
) -> (StatusCode, JsonValue) {
    send(
        app,
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", content_type)
            .body(Body::from(body))
            .unwrap(),
    )
    .await
}

async fn marked(app: &axum::Router) -> Vec<String> {
    let (status, json) = get_query(
        app,
        &ledger_path(),
        "PREFIX ex: <http://ex.org/> SELECT ?s WHERE { ?s ex:seen true }",
        "",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    values(&json, "s")
}

fn update_path() -> String {
    format!("/v1/fluree/update/{LEDGER}")
}

#[tokio::test]
async fn using_graph_uri_scopes_the_update_where_clause() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = update(
        &app,
        &format!("{}?using-graph-uri={}", update_path(), enc(G2)),
        "application/sparql-update",
        MARK_NAMED.to_string(),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(marked(&app).await, ["http://ex.org/s3"]);
}

/// Repeated `using-*` keys used to make the whole query string unparseable,
/// which silently discarded every parameter — `ledger` included — so this
/// request failed for want of a ledger.
#[tokio::test]
async fn repeated_using_graph_uri_keeps_the_ledger_parameter() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = update(
        &app,
        &format!(
            "/v1/fluree/update?ledger={}&using-graph-uri={}&using-graph-uri={}",
            enc(LEDGER),
            enc(G1),
            enc(G2)
        ),
        "application/sparql-update",
        MARK_NAMED.to_string(),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(
        marked(&app).await,
        ["http://ex.org/s1", "http://ex.org/s2", "http://ex.org/s3"]
    );
}

#[tokio::test]
async fn using_named_graph_uri_sets_the_update_named_graphs() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = update(
        &app,
        &format!("{}?using-named-graph-uri={}", update_path(), enc(G1)),
        "application/sparql-update",
        "PREFIX ex: <http://ex.org/> \
         INSERT { ?s ex:seen true } WHERE { GRAPH ?g { ?s ex:name ?n } }"
            .to_string(),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(marked(&app).await, ["http://ex.org/s1", "http://ex.org/s2"]);
}

/// The form-encoded transport carries the parameters in the body.
#[tokio::test]
async fn form_encoded_update_takes_using_graph_uri_from_the_body() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = update(
        &app,
        &update_path(),
        "application/x-www-form-urlencoded",
        format!("update={}&using-graph-uri={}", enc(MARK_NAMED), enc(G2)),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(marked(&app).await, ["http://ex.org/s3"]);
}

/// §2.2.3: an operation that names its own dataset may not also be given one
/// by the protocol.
#[tokio::test]
async fn using_graph_uri_conflicts_with_a_using_clause() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = update(
        &app,
        &format!("{}?using-graph-uri={}", update_path(), enc(G2)),
        "application/sparql-update",
        "PREFIX ex: <http://ex.org/> \
         INSERT { ?s ex:seen true } USING <http://ex.org/g1> WHERE { ?s ex:name ?n }"
            .to_string(),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
    assert!(marked(&app).await.is_empty(), "nothing may be written");
}

/// `DELETE WHERE` has no USING form. Running it unscoped would delete from the
/// wrong graph, so it is refused.
#[tokio::test]
async fn using_graph_uri_cannot_scope_delete_where() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = update(
        &app,
        &format!("{}?using-graph-uri={}", update_path(), enc(G2)),
        "application/sparql-update",
        "PREFIX ex: <http://ex.org/> DELETE WHERE { ?s ex:name ?n }".to_string(),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
    let (_, names) = get_query(&app, &ledger_path(), NAMES, "").await;
    assert_eq!(values(&names, "n"), ["D"], "the default graph is untouched");
}

/// The insert, upsert and sync routes take no SPARQL UPDATE, so a USING
/// parameter there is refused rather than ignored.
#[tokio::test]
async fn using_graph_uri_on_an_insert_route_is_refused() {
    let (_tmp, app) = seeded_app().await;

    let (status, json) = update(
        &app,
        &format!("/v1/fluree/insert/{LEDGER}?using-graph-uri={}", enc(G2)),
        "application/json",
        serde_json::json!({
            "@context": {"ex": "http://ex.org/"},
            "@id": "ex:x",
            "ex:name": "X"
        })
        .to_string(),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
    let (_, names) = get_query(&app, &ledger_path(), NAMES, "").await;
    assert_eq!(values(&names, "n"), ["D"], "nothing may be written");
}
