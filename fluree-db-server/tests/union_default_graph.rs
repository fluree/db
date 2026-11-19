//! The union default graph over HTTP: the ledger's `f:unionDefaultGraph`
//! setting and the request's own switch reach every query route, and the
//! ledger endpoint's service description claims `sd:UnionDefaultGraph` when
//! the setting is on.

use axum::body::Body;
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, Response, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

const NAMES: &str = "PREFIX ex: <http://example.org/>\n\
                     SELECT ?name WHERE { ?s ex:name ?name }";

/// Alice in the default graph, Bob and Carol in named graphs.
async fn seeded(ledger: &str) -> (TempDir, axum::Router) {
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
        post("/v1/fluree/create", "application/json")
            .body(Body::from(json!({ "ledger": ledger }).to_string()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    update(
        &app,
        ledger,
        "PREFIX ex: <http://example.org/>
         INSERT DATA {
           ex:alice ex:name \"Alice\" .
           GRAPH <http://example.org/g1> { ex:bob ex:name \"Bob\" }
           GRAPH <http://example.org/g2> { ex:carol ex:name \"Carol\" }
         }",
    )
    .await;
    (tmp, app)
}

/// Switch the ledger's union default graph on, in its config graph.
async fn enable_union(app: &axum::Router, ledger: &str) {
    update(
        app,
        ledger,
        &format!(
            "PREFIX f: <https://ns.flur.ee/db#>
             INSERT DATA {{
               GRAPH <urn:fluree:{ledger}#config> {{
                 <urn:config:main> a f:LedgerConfig ;
                   f:queryDefaults <urn:config:query> .
                 <urn:config:query> f:unionDefaultGraph true .
               }}
             }}"
        ),
    )
    .await;
}

fn post(uri: &str, content_type: &str) -> http::request::Builder {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", content_type)
}

async fn body(resp: Response<Body>) -> (StatusCode, String) {
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

async fn send(app: &axum::Router, request: Request<Body>) -> (StatusCode, String) {
    body(app.clone().oneshot(request).await.unwrap()).await
}

async fn update(app: &axum::Router, ledger: &str, sparql: &str) {
    let (status, body) = send(
        app,
        post(
            &format!("/v1/fluree/update/{ledger}"),
            "application/sparql-update",
        )
        .body(Body::from(sparql.to_string()))
        .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// The seeded names a response carries, in any result format, sorted.
fn names(body: &str) -> Vec<String> {
    fn walk(v: &JsonValue, out: &mut Vec<String>) {
        match v {
            JsonValue::String(s) if ["Alice", "Bob", "Carol"].contains(&s.as_str()) => {
                out.push(s.clone());
            }
            JsonValue::Array(items) => items.iter().for_each(|i| walk(i, out)),
            JsonValue::Object(map) => map.values().for_each(|i| walk(i, out)),
            _ => {}
        }
    }
    let mut out = Vec::new();
    for line in body.lines() {
        if let Ok(v) = serde_json::from_str::<JsonValue>(line) {
            walk(&v, &mut out);
        }
    }
    if out.is_empty() {
        if let Ok(v) = serde_json::from_str::<JsonValue>(body) {
            walk(&v, &mut out);
        }
    }
    out.sort();
    out
}

fn all() -> Vec<String> {
    vec!["Alice".into(), "Bob".into(), "Carol".into()]
}

fn alice() -> Vec<String> {
    vec!["Alice".into()]
}

async fn sparql(app: &axum::Router, uri: &str, query: &str, headers: &[(&str, &str)]) -> String {
    let mut req = post(uri, "application/sparql-query");
    for (name, value) in headers {
        req = req.header(*name, *value);
    }
    let (status, body) = send(app, req.body(Body::from(query.to_string())).unwrap()).await;
    assert_eq!(status, StatusCode::OK, "{uri} {query}: {body}");
    body
}

async fn jsonld(app: &axum::Router, uri: &str, query: &JsonValue) -> String {
    let (status, body) = send(
        app,
        post(uri, "application/json")
            .body(Body::from(query.to_string()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{uri} {query}: {body}");
    body
}

/// The ledger endpoint reads the union once the ledger configures it, on the
/// plain, tracked, streaming, and time-pinned routes alike, and not before.
#[tokio::test]
async fn ledger_routes_read_the_union_the_ledger_configures() {
    let ledger = "udg:ledger";
    let (_tmp, app) = seeded(ledger).await;
    let query = format!("/v1/fluree/query/{ledger}");
    let stream = format!("/v1/fluree/stream/query/{ledger}");

    assert_eq!(names(&sparql(&app, &query, NAMES, &[]).await), alice());

    enable_union(&app, ledger).await;
    assert_eq!(names(&sparql(&app, &query, NAMES, &[]).await), all());
    assert_eq!(
        names(&sparql(&app, &query, NAMES, &[("fluree-track-fuel", "true")]).await),
        all()
    );
    assert_eq!(names(&sparql(&app, &stream, NAMES, &[]).await), all());

    // The seed committed at t=1 and the setting at t=2.
    let at = |t: u32| format!("/v1/fluree/query/{ledger}@t:{t}");
    assert_eq!(names(&sparql(&app, &at(1), NAMES, &[]).await), alice());
    assert_eq!(names(&sparql(&app, &at(2), NAMES, &[]).await), all());
}

/// The request's own switch reads the union on a ledger that does not
/// configure it: a SPARQL pragma, or JSON-LD `opts`.
#[tokio::test]
async fn request_switch_reads_the_union() {
    let ledger = "udg:request";
    let (_tmp, app) = seeded(ledger).await;

    let pragma = format!("# PRAGMA union-default-graph: true\n{NAMES}");
    assert_eq!(
        names(&sparql(&app, &format!("/v1/fluree/query/{ledger}"), &pragma, &[]).await),
        all()
    );

    let query = json!({
        "@context": { "ex": "http://example.org/" },
        "select": ["?name"],
        "where": { "@id": "?s", "ex:name": "?name" },
        "opts": { "unionDefaultGraph": true }
    });
    assert_eq!(
        names(&jsonld(&app, &format!("/v1/fluree/query/{ledger}"), &query).await),
        all()
    );
    let mut from = query.clone();
    from["from"] = json!(ledger);
    assert_eq!(names(&jsonld(&app, "/v1/fluree/query", &from).await), all());
}

/// On the connection endpoint, naming the ledger itself reads its default
/// graph, which the setting makes the union.
#[tokio::test]
async fn connection_endpoint_from_the_ledger_reads_the_union() {
    let ledger = "udg:connection";
    let (_tmp, app) = seeded(ledger).await;
    enable_union(&app, ledger).await;

    let from = format!(
        "PREFIX ex: <http://example.org/>\n\
         SELECT ?name FROM <{ledger}> WHERE {{ ?s ex:name ?name }}"
    );
    assert_eq!(
        names(&sparql(&app, "/v1/fluree/query", &from, &[]).await),
        all()
    );
}

/// The ledger endpoint's service description claims `sd:UnionDefaultGraph`
/// exactly when the ledger's setting is on; the connection endpoint, whose
/// default graph is whatever `FROM` names, never does.
#[tokio::test]
async fn service_description_claims_the_union_default_graph() {
    const FEATURE: &str = "<http://www.w3.org/ns/sparql-service-description#feature> \
                           <http://www.w3.org/ns/sparql-service-description#UnionDefaultGraph>";
    let ledger = "udg:describe";
    let (_tmp, app) = seeded(ledger).await;
    let describe = |uri: String| {
        let app = app.clone();
        async move {
            let (status, body) = send(
                &app,
                Request::builder()
                    .method("GET")
                    .uri(uri)
                    .header("accept", "application/n-triples")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            body
        }
    };

    let ledger_endpoint = format!("/v1/fluree/query/{ledger}");
    assert!(!describe(ledger_endpoint.clone()).await.contains(FEATURE));
    enable_union(&app, ledger).await;
    let body = describe(ledger_endpoint).await;
    assert!(body.contains(FEATURE), "{body}");
    assert!(!describe("/v1/fluree/query".to_string())
        .await
        .contains(FEATURE));
}
