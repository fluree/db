//! SPARQL `# PRAGMA` request options over HTTP: the counterpart of a JSON-LD
//! body's `opts` and of the `fluree-*` headers. Each pragma is checked against
//! the header that already carries the same option. Policy pragmas are covered
//! in `policy_integration.rs`, and the `validation-mode` pragma in
//! `override_control_identity.rs`.

use axum::body::Body;
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, Response, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

const NAMES: &str = "PREFIX ex: <http://example.org/>\n\
                     SELECT ?name WHERE { ?s ex:name ?name }";

async fn seeded(ledger: &str) -> (TempDir, axum::Router) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        // A `min-t` the ledger never reaches fails fast.
        query_min_t_timeout_ms: 200,
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    let app = build_router(state);

    let (status, json) = send(
        &app,
        post("/v1/fluree/create", "application/json", &[])
            .body(Body::from(
                serde_json::json!({ "ledger": ledger }).to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{json}");
    let insert = serde_json::json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            { "@id": "ex:alice", "ex:name": "Alice" },
            { "@id": "ex:bob", "ex:name": "Bob" }
        ]
    });
    let (status, json) = send(
        &app,
        post(
            &format!("/v1/fluree/insert/{ledger}"),
            "application/json",
            &[],
        )
        .body(Body::from(insert.to_string()))
        .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    (tmp, app)
}

fn post(uri: &str, content_type: &str, headers: &[(&str, &str)]) -> http::request::Builder {
    let mut req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", content_type);
    for (name, value) in headers {
        req = req.header(*name, *value);
    }
    req
}

async fn send(app: &axum::Router, request: Request<Body>) -> (StatusCode, JsonValue) {
    let resp = app.clone().oneshot(request).await.unwrap();
    body(resp).await
}

async fn body(resp: Response<Body>) -> (StatusCode, JsonValue) {
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| JsonValue::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, json)
}

async fn query_raw(
    app: &axum::Router,
    ledger: &str,
    sparql: &str,
    headers: &[(&str, &str)],
) -> Response<Body> {
    app.clone()
        .oneshot(
            post(
                &format!("/v1/fluree/query/{ledger}"),
                "application/sparql-query",
                headers,
            )
            .body(Body::from(sparql.to_string()))
            .unwrap(),
        )
        .await
        .unwrap()
}

async fn query(
    app: &axum::Router,
    ledger: &str,
    sparql: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, JsonValue) {
    body(query_raw(app, ledger, sparql, headers).await).await
}

async fn update(
    app: &axum::Router,
    ledger: &str,
    sparql: &str,
    headers: &[(&str, &str)],
) -> Response<Body> {
    app.clone()
        .oneshot(
            post(
                &format!("/v1/fluree/update/{ledger}"),
                "application/sparql-update",
                headers,
            )
            .body(Body::from(sparql.to_string()))
            .unwrap(),
        )
        .await
        .unwrap()
}

/// Whether `json` is a fuel-limit error. Only its message counts: a tracked
/// success reports `fuel` too.
fn is_fuel_error(json: &JsonValue) -> bool {
    ["error", "message"]
        .iter()
        .filter_map(|key| json.get(*key).and_then(JsonValue::as_str))
        .any(|message| message.to_lowercase().contains("fuel"))
}

/// `# PRAGMA max-fuel` caps a query exactly as `fluree-max-fuel` does: a
/// budget below the per-query floor fails before execution.
#[tokio::test]
async fn max_fuel_pragma_caps_a_query_like_the_header() {
    let (_tmp, app) = seeded("prag:fuel").await;

    let (header_status, header_json) =
        query(&app, "prag:fuel", NAMES, &[("fluree-max-fuel", "0.5")]).await;
    assert!(is_fuel_error(&header_json), "{header_json}");

    let sparql = format!("# PRAGMA max-fuel: 0.5\n{NAMES}");
    let (status, json) = query(&app, "prag:fuel", &sparql, &[]).await;
    assert_eq!(status, header_status, "{json}");
    assert!(is_fuel_error(&json), "{json}");

    // The connection-scoped route takes it too.
    let sparql = "# PRAGMA max-fuel: 0.5\nPREFIX ex: <http://example.org/>\n\
                  SELECT ?name FROM <prag:fuel> WHERE { ?s ex:name ?name }";
    let (status, json) = send(
        &app,
        post("/v1/fluree/query", "application/sparql-query", &[])
            .body(Body::from(sparql))
            .unwrap(),
    )
    .await;
    assert_eq!(status, header_status, "{json}");
    assert!(is_fuel_error(&json), "{json}");
}

/// `max-fuel` is a cap, so a pragma tightens the header's but cannot lift it:
/// the header may be an application's and the text its end user's.
#[tokio::test]
async fn max_fuel_pragma_only_tightens_the_header() {
    let (_tmp, app) = seeded("prag:over").await;

    for (pragma, header) in [("100000", "0.5"), ("0.5", "100000")] {
        let sparql = format!("# PRAGMA max-fuel: {pragma}\n{NAMES}");
        let (status, json) =
            query(&app, "prag:over", &sparql, &[("fluree-max-fuel", header)]).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "pragma {pragma} with header {header}: {json}"
        );
        assert!(is_fuel_error(&json), "{json}");
    }

    let sparql = format!("# PRAGMA max-fuel: 100000\n{NAMES}");
    let (status, json) = query(&app, "prag:over", &sparql, &[("fluree-max-fuel", "100000")]).await;
    assert_eq!(status, StatusCode::OK, "control: {json}");
}

/// A multi-query alias's pragma holds to the alias's own `max-fuel` the same
/// way (an envelope cannot carry one).
#[tokio::test]
async fn multi_query_alias_max_fuel_pragma_only_tightens_its_opts() {
    let (_tmp, app) = seeded("prag:mq").await;
    let alias = |pragma: &str, max_fuel: f64| {
        let envelope = serde_json::json!({
            "queries": {
                "names": {
                    "language": "sparql",
                    "query": format!(
                        "{pragma}\nPREFIX ex: <http://example.org/>\n\
                         SELECT ?name FROM <prag:mq> WHERE {{ ?s ex:name ?name }}"
                    ),
                    "opts": {"max-fuel": max_fuel}
                }
            }
        });
        let app = app.clone();
        async move {
            let (status, json) = send(
                &app,
                post("/v1/fluree/multi-query", "application/json", &[])
                    .body(Body::from(envelope.to_string()))
                    .unwrap(),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{json}");
            json
        }
    };

    let json = alias("", 100_000.0).await;
    let rows = json.pointer("/results/names/results/bindings");
    assert_eq!(
        rows.and_then(JsonValue::as_array).map(Vec::len),
        Some(2),
        "control: {json}"
    );
    for (pragma, max_fuel) in [("100000", 0.5), ("0.5", 100_000.0)] {
        let json = alias(&format!("# PRAGMA max-fuel: {pragma}"), max_fuel).await;
        assert!(
            is_fuel_error(&json["errors"]["names"]),
            "pragma {pragma} with opts {max_fuel}: {json}"
        );
    }
}

/// A multi-query alias's `# PRAGMA meta` adds to the tracking its `opts` ask
/// for, as on a single query.
#[tokio::test]
async fn multi_query_alias_meta_pragma_adds_to_its_opts() {
    let (_tmp, app) = seeded("prag:mq-meta").await;
    let envelope = serde_json::json!({
        "queries": {
            "names": {
                "language": "sparql",
                "query": "# PRAGMA meta: time\nPREFIX ex: <http://example.org/>\n\
                          SELECT ?name FROM <prag:mq-meta> WHERE { ?s ex:name ?name }",
                "opts": {"meta": {"fuel": true}}
            }
        }
    });
    let (status, json) = send(
        &app,
        post("/v1/fluree/multi-query", "application/json", &[])
            .body(Body::from(envelope.to_string()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    let tracking = &json["tracking"]["names"];
    assert!(tracking.get("fuel").is_some(), "opts' fuel: {json}");
    assert!(tracking.get("time").is_some(), "pragma's time: {json}");
}

/// `# PRAGMA meta` reports tracking as `fluree-track-*` does.
#[tokio::test]
async fn meta_pragma_reports_tracking() {
    let (_tmp, app) = seeded("prag:meta").await;

    let resp = query_raw(&app, "prag:meta", NAMES, &[]).await;
    assert!(resp.headers().get("x-fdb-fuel").is_none());

    let sparql = format!("# PRAGMA meta: fuel, time\n{NAMES}");
    let resp = query_raw(&app, "prag:meta", &sparql, &[]).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers().get("x-fdb-fuel").is_some());
    assert!(resp.headers().get("x-fdb-time").is_some());
}

/// `# PRAGMA meta` adds to the tracking the headers ask for but cannot switch
/// it off: an application reading fuel from the response keeps it whatever
/// the text says.
#[tokio::test]
async fn meta_pragma_adds_to_header_tracking() {
    let (_tmp, app) = seeded("prag:meta-add").await;

    let sparql = format!("# PRAGMA meta: time\n{NAMES}");
    let resp = query_raw(
        &app,
        "prag:meta-add",
        &sparql,
        &[("fluree-track-fuel", "true")],
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers().get("x-fdb-fuel").is_some(), "header's fuel");
    assert!(resp.headers().get("x-fdb-time").is_some(), "pragma's time");

    let sparql = format!("# PRAGMA meta: false\n{NAMES}");
    let resp = query_raw(
        &app,
        "prag:meta-add",
        &sparql,
        &[("fluree-track-meta", "true")],
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers().get("x-fdb-fuel").is_some(), "header's fuel");
}

/// `# PRAGMA min-t` waits for the ledger as `fluree-min-t` does; a `t` it never
/// reaches times out the same way.
#[tokio::test]
async fn min_t_pragma_waits_like_the_header() {
    let (_tmp, app) = seeded("prag:mint").await;

    let (header_status, header_json) =
        query(&app, "prag:mint", NAMES, &[("fluree-min-t", "999")]).await;
    assert_eq!(header_status, StatusCode::REQUEST_TIMEOUT, "{header_json}");

    let sparql = format!("# PRAGMA min-t: 999\n{NAMES}");
    let (status, json) = query(&app, "prag:mint", &sparql, &[]).await;
    assert_eq!(status, StatusCode::REQUEST_TIMEOUT, "{json}");

    let sparql = format!("# PRAGMA min-t: 1\n{NAMES}");
    let (status, json) = query(&app, "prag:mint", &sparql, &[]).await;
    assert_eq!(status, StatusCode::OK, "{json}");
}

/// An unknown or malformed pragma is a 400, never an option silently dropped.
#[tokio::test]
async fn malformed_pragmas_are_rejected() {
    let (_tmp, app) = seeded("prag:bad").await;

    for (sparql, expected) in [
        (
            format!("# PRAGMA max-feul: 10\n{NAMES}"),
            "unknown pragma `max-feul`",
        ),
        (
            format!("# PRAGMA max-fuel: lots\n{NAMES}"),
            "non-negative number",
        ),
        (
            format!("# PRAGMA event-time: 2020-01-01T00:00:00Z\n{NAMES}"),
            "applies to updates, not queries",
        ),
    ] {
        let (status, json) = query(&app, "prag:bad", &sparql, &[]).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{sparql}: {json}");
        assert!(json.to_string().contains(expected), "{sparql}: {json}");
    }

    let resp = update(
        &app,
        "prag:bad",
        "# PRAGMA min-t: 1\nINSERT DATA { <urn:a> <urn:p> 1 }",
        &[],
    )
    .await;
    let (status, json) = body(resp).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
    assert!(
        json.to_string().contains("applies to queries, not updates"),
        "{json}"
    );
}

/// On SPARQL UPDATE, `max-fuel` and `meta` behave as their headers do.
#[tokio::test]
async fn update_fuel_and_meta_pragmas() {
    let (_tmp, app) = seeded("prag:upd").await;
    const INSERT: &str =
        "PREFIX ex: <http://example.org/>\nINSERT DATA { ex:carol ex:name \"Carol\" }";

    let resp = update(
        &app,
        "prag:upd",
        &format!("# PRAGMA meta: fuel\n{INSERT}"),
        &[],
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers().get("x-fdb-fuel").is_some());

    let resp = update(&app, "prag:upd", INSERT, &[("fluree-max-fuel", "0.5")]).await;
    let (header_status, header_json) = body(resp).await;
    assert!(is_fuel_error(&header_json), "{header_json}");

    let resp = update(
        &app,
        "prag:upd",
        &format!("# PRAGMA max-fuel: 0.5\n{INSERT}"),
        &[],
    )
    .await;
    let (status, json) = body(resp).await;
    assert_eq!(status, header_status, "{json}");
    assert!(is_fuel_error(&json), "{json}");
}

/// `# PRAGMA event-time` backdates the commit as `opts.eventTime` does, so a
/// read as of a later instant in that past sees the write.
#[tokio::test]
async fn event_time_pragma_backdates_the_commit() {
    let (_tmp, app) = seeded("prag:evt").await;
    // Backdated commits must not precede the ledger's head, so start fresh.
    let (status, json) = send(
        &app,
        post("/v1/fluree/create", "application/json", &[])
            .body(Body::from(
                serde_json::json!({ "ledger": "prag:dated" }).to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{json}");

    let resp = update(
        &app,
        "prag:dated",
        "# PRAGMA event-time: 2020-01-01T00:00:00Z\n\
         PREFIX ex: <http://example.org/>\nINSERT DATA { ex:dan ex:name \"Dan\" }",
        &[],
    )
    .await;
    let (status, json) = body(resp).await;
    assert_eq!(status, StatusCode::OK, "{json}");

    let as_of_2020 = "PREFIX ex: <http://example.org/>\n\
                      SELECT ?name FROM <prag:dated@iso:2020-06-01T00:00:00Z> \
                      WHERE { ?s ex:name ?name }";
    let (status, json) = send(
        &app,
        post("/v1/fluree/query", "application/sparql-query", &[])
            .body(Body::from(as_of_2020))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert!(json.to_string().contains("Dan"), "{json}");

    let resp = update(
        &app,
        "prag:dated",
        "# PRAGMA event-time: yesterday\nINSERT DATA { <urn:a> <urn:p> 1 }",
        &[],
    )
    .await;
    let (status, json) = body(resp).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
    assert!(json.to_string().contains("RFC 3339"), "{json}");
}
