//! W3C SPARQL 1.1 Graph Store HTTP Protocol over `/v1/fluree/data/<ledger>`:
//! `GET`/`HEAD` read a graph, `PUT` replaces it, `POST` adds to it, `DELETE`
//! removes it, with the protocol's status codes.

use axum::body::Body;
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

const LEDGER: &str = "gsp:main";
const TOOLS: &str = "urn:example:tools";
const OTHER: &str = "urn:example:other";

const SEED_TRIG: &str = r#"
@prefix ex: <http://example.org/> .
ex:seed ex:name "SeedDefault" .
<urn:example:other> { ex:zed ex:name "Zed" . }
"#;

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

    let create = serde_json::json!({ "ledger": LEDGER }).to_string();
    let (status, body) = send(
        &app,
        "POST",
        "/v1/fluree/create",
        Some("application/json"),
        &create,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create ledger: {body}");
    let (status, body) = send(
        &app,
        "POST",
        &format!("/v1/fluree/upsert/{LEDGER}"),
        Some("application/trig"),
        SEED_TRIG,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "seed: {body}");
    (tmp, app)
}

async fn send(
    app: &axum::Router,
    method: &str,
    uri: &str,
    content_type: Option<&str>,
    body: &str,
    accept: Option<&str>,
) -> (StatusCode, String) {
    let (status, _, body) = send_full(app, method, uri, content_type, body, accept).await;
    (status, body)
}

async fn send_full(
    app: &axum::Router,
    method: &str,
    uri: &str,
    content_type: Option<&str>,
    body: &str,
    accept: Option<&str>,
) -> (StatusCode, Option<String>, String) {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(ct) = content_type {
        req = req.header("content-type", ct);
    }
    if let Some(accept) = accept {
        req = req.header("accept", accept);
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, ct, String::from_utf8_lossy(&bytes).into_owned())
}

fn named(graph: &str) -> String {
    format!(
        "/v1/fluree/data/{LEDGER}?graph={}",
        urlencoding::encode(graph)
    )
}

fn default_graph() -> String {
    format!("/v1/fluree/data/{LEDGER}?default")
}

const TTL: Option<&str> = Some("text/turtle");
const JSON_LD: Option<&str> = Some("application/ld+json");

#[tokio::test]
async fn named_graph_lifecycle() {
    let (_tmp, app) = seeded_app().await;
    let tools = named(TOOLS);

    let (status, body) = send(&app, "GET", &tools, None, "", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "absent graph: {body}");

    let spec = "@prefix ex: <http://example.org/> .\n\
                ex:search ex:name \"search\" ; ex:param [ ex:name \"q\" ] .\n";
    let (status, body) = send(&app, "PUT", &tools, TTL, spec, None).await;
    assert_eq!(status, StatusCode::CREATED, "PUT creates: {body}");
    let t1 = serde_json::from_str::<serde_json::Value>(&body).unwrap()["t"].as_i64();

    // An identical PUT replaces nothing and commits nothing.
    let (status, body) = send(&app, "PUT", &tools, TTL, spec, None).await;
    assert_eq!(status, StatusCode::OK, "PUT replaces: {body}");
    let t2 = serde_json::from_str::<serde_json::Value>(&body).unwrap()["t"].as_i64();
    assert_eq!(t1, t2, "an identical PUT must not commit");

    let (status, ct, body) = send_full(&app, "GET", &tools, None, "", JSON_LD).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(ct.as_deref(), Some("application/ld+json"));
    assert!(body.contains("search") && body.contains("\"q\""), "{body}");
    assert!(
        !body.contains("Zed") && !body.contains("SeedDefault"),
        "only this graph: {body}"
    );

    let (status, ct, body) = send_full(
        &app,
        "GET",
        &tools,
        None,
        "",
        Some("text/turtle, application/rdf+xml;q=0.5"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(ct.as_deref(), Some("application/rdf+xml"));
    assert!(body.contains("search"), "{body}");

    let (status, body) = send(&app, "GET", &tools, None, "", Some("text/turtle")).await;
    assert_eq!(status, StatusCode::NOT_ACCEPTABLE, "{body}");

    let (status, body) = send(&app, "HEAD", &tools, None, "", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_empty(), "HEAD has no body: {body}");

    // POST adds; nothing already there is retracted.
    let more = "@prefix ex: <http://example.org/> .\nex:fetch ex:name \"fetch\" .\n";
    let (status, body) = send(&app, "POST", &tools, TTL, more, None).await;
    assert_eq!(status, StatusCode::OK, "POST adds: {body}");
    let (_, body) = send(&app, "GET", &tools, None, "", None).await;
    assert!(body.contains("fetch") && body.contains("search"), "{body}");

    // PUT with fewer triples retracts the rest.
    let smaller = "@prefix ex: <http://example.org/> .\nex:fetch ex:name \"fetch\" .\n";
    let (status, body) = send(&app, "PUT", &tools, TTL, smaller, None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, body) = send(&app, "GET", &tools, None, "", None).await;
    assert!(body.contains("fetch") && !body.contains("search"), "{body}");

    let (status, body) = send(&app, "DELETE", &tools, None, "", None).await;
    assert_eq!(status, StatusCode::OK, "DELETE: {body}");
    let (status, _) = send(&app, "GET", &tools, None, "", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "gone after DELETE");
    let (status, _) = send(&app, "DELETE", &tools, None, "", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "nothing left to DELETE");

    // Other graphs were never touched.
    let (status, body) = send(&app, "GET", &named(OTHER), None, "", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Zed"), "{body}");
}

#[tokio::test]
async fn empty_put_clears_the_graph() {
    let (_tmp, app) = seeded_app().await;
    let other = named(OTHER);

    // The protocol's PUT replaces the graph with the body, an empty one
    // included, with no allowEmpty opt-in.
    let (status, body) = send(&app, "PUT", &other, None, "", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, _) = send(&app, "GET", &other, None, "", None).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "an emptied named graph no longer exists"
    );
}

#[tokio::test]
async fn default_graph_lifecycle() {
    let (_tmp, app) = seeded_app().await;
    let default = default_graph();

    let (status, body) = send(&app, "GET", &default, None, "", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body.contains("SeedDefault") && !body.contains("Zed"),
        "default graph only: {body}"
    );

    let replacement = "@prefix ex: <http://example.org/> .\nex:new ex:name \"NewDefault\" .\n";
    let (status, body) = send(&app, "PUT", &default, TTL, replacement, None).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the default graph always exists: {body}"
    );
    let (_, body) = send(&app, "GET", &default, None, "", None).await;
    assert!(
        body.contains("NewDefault") && !body.contains("SeedDefault"),
        "{body}"
    );

    let (status, body) = send(&app, "DELETE", &default, None, "", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = send(&app, "GET", &default, None, "", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!body.contains("NewDefault"), "cleared: {body}");

    let (_, body) = send(&app, "GET", &named(OTHER), None, "", None).await;
    assert!(body.contains("Zed"), "named graphs untouched: {body}");
}

#[tokio::test]
async fn json_ld_bodies() {
    let (_tmp, app) = seeded_app().await;
    let tools = named(TOOLS);
    let doc = serde_json::json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [{ "@id": "ex:search", "ex:name": "search" }]
    })
    .to_string();
    let (status, body) = send(&app, "PUT", &tools, JSON_LD, &doc, None).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let add = serde_json::json!({
        "@context": { "ex": "http://example.org/" },
        "@id": "ex:fetch", "ex:name": "fetch"
    })
    .to_string();
    let (status, body) = send(&app, "POST", &tools, Some("application/json"), &add, None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, body) = send(&app, "GET", &tools, None, "", None).await;
    assert!(body.contains("fetch") && body.contains("search"), "{body}");
}

#[tokio::test]
async fn request_errors() {
    let (_tmp, app) = seeded_app().await;
    let base = format!("/v1/fluree/data/{LEDGER}");
    let spec = "@prefix ex: <http://example.org/> .\nex:a ex:p \"x\" .\n";

    for uri in [
        base.clone(),
        format!("{base}?graph={TOOLS}&default"),
        format!("{base}?graph=relative"),
        format!(
            "{base}?graph={}",
            urlencoding::encode("urn:fluree:gsp:main#txn-meta")
        ),
    ] {
        let (status, body) = send(&app, "GET", &uri, None, "", None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body}");
        let (status, body) = send(&app, "PUT", &uri, TTL, spec, None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body}");
    }

    let (status, body) = send(
        &app,
        "PUT",
        &named(TOOLS),
        Some("application/xml"),
        "<x/>",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE, "{body}");

    let other_block =
        format!("@prefix ex: <http://example.org/> .\nGRAPH <{OTHER}> {{ ex:a ex:p \"x\" . }}\n");
    let (status, body) = send(
        &app,
        "POST",
        &named(TOOLS),
        Some("application/trig"),
        &other_block,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    let (status, body) = send(&app, "POST", &named(TOOLS), TTL, "", None).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "an empty POST has nothing to add: {body}"
    );

    let (status, body) = send(
        &app,
        "GET",
        "/v1/fluree/data/nosuch:main?default",
        None,
        "",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
}
