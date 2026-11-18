//! Query-result cache over the ledger route (`POST /v1/fluree/query/{ledger}`):
//! `Fluree-Cache: true` opts in, `x-fdb-cache` reports what happened, and a
//! hit's body is byte-identical to the uncached response.

use axum::body::Body;
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

const LEDGER: &str = "resultcache:main";

async fn app() -> (TempDir, axum::Router) {
    app_with(None).await
}

async fn app_with(query_result_cache_max_mb: Option<usize>) -> (TempDir, axum::Router) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        query_result_cache_max_mb,
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    let app = build_router(state);
    let (status, _, _) = send(
        &app,
        "/v1/fluree/create",
        "application/json",
        None,
        serde_json::json!({ "ledger": LEDGER }).to_string(),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create ledger");
    upsert(&app, "ex:alice ex:name \"Alice\" ; ex:age 30 .").await;
    (tmp, app)
}

async fn upsert(app: &axum::Router, turtle: &str) {
    let (status, _, body) = send(
        app,
        &format!("/v1/fluree/upsert/{LEDGER}"),
        "text/turtle",
        None,
        format!("@prefix ex: <http://example.org/> .\n{turtle}"),
        &[],
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "upsert: {}",
        String::from_utf8_lossy(&body)
    );
}

async fn send(
    app: &axum::Router,
    uri: &str,
    content_type: &str,
    accept: Option<&str>,
    body: String,
    headers: &[(&str, &str)],
) -> (StatusCode, Option<String>, Vec<u8>) {
    let mut req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", content_type);
    if let Some(accept) = accept {
        req = req.header("accept", accept);
    }
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let cache = resp
        .headers()
        .get("x-fdb-cache")
        .map(|v| v.to_str().unwrap().to_string());
    let bytes = resp
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    (status, cache, bytes)
}

/// Run `query` uncached, then twice with `Fluree-Cache: true`: a miss, then a
/// hit, both with the uncached bytes.
async fn assert_miss_then_hit(
    app: &axum::Router,
    content_type: &str,
    accept: Option<&str>,
    query: &str,
) -> Vec<u8> {
    let uri = format!("/v1/fluree/query/{LEDGER}");
    let (status, cache, uncached) =
        send(app, &uri, content_type, accept, query.to_string(), &[]).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&uncached)
    );
    assert_eq!(cache, None, "a request that did not ask reports nothing");
    for expected in ["miss", "hit"] {
        let (status, cache, body) = send(
            app,
            &uri,
            content_type,
            accept,
            query.to_string(),
            &[("fluree-cache", "true")],
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(cache.as_deref(), Some(expected), "{query}");
        assert_eq!(body, uncached, "{query}: {expected} body");
    }
    uncached
}

#[tokio::test]
async fn sparql_select_hits_with_the_uncached_bytes() {
    let (_tmp, app) = app().await;
    let select = "PREFIX ex: <http://example.org/>\nSELECT ?n WHERE { ?s ex:name ?n }";
    let before = assert_miss_then_hit(
        &app,
        "application/sparql-query",
        Some("application/sparql-results+json"),
        select,
    )
    .await;

    upsert(&app, "ex:bob ex:name \"Bob\" .").await;
    let after = assert_miss_then_hit(
        &app,
        "application/sparql-query",
        Some("application/sparql-results+json"),
        select,
    )
    .await;
    assert_ne!(before, after, "the commit changed the answer");
    assert!(String::from_utf8_lossy(&after).contains("Bob"));
}

#[tokio::test]
async fn sparql_construct_and_xml_hit_with_the_uncached_bytes() {
    let (_tmp, app) = app().await;
    assert_miss_then_hit(
        &app,
        "application/sparql-query",
        Some("application/n-triples"),
        "PREFIX ex: <http://example.org/>\nCONSTRUCT { ?s ex:name ?n } WHERE { ?s ex:name ?n }",
    )
    .await;
    assert_miss_then_hit(
        &app,
        "application/sparql-query",
        Some("application/sparql-results+xml"),
        "PREFIX ex: <http://example.org/>\nSELECT ?n WHERE { ?s ex:name ?n }",
    )
    .await;
}

#[tokio::test]
async fn jsonld_header_and_opts_share_one_entry() {
    let (_tmp, app) = app().await;
    let query = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?n"],
        "where": {"@id": "?s", "ex:name": "?n"}
    });
    let uncached = assert_miss_then_hit(&app, "application/json", None, &query.to_string()).await;

    let mut in_opts = query.clone();
    in_opts["opts"] = serde_json::json!({"cache": true});
    let (status, cache, body) = send(
        &app,
        &format!("/v1/fluree/query/{LEDGER}"),
        "application/json",
        None,
        in_opts.to_string(),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cache.as_deref(), Some("hit"));
    assert_eq!(body, uncached);
}

#[tokio::test]
async fn volatile_sparql_reports_its_bypass() {
    let (_tmp, app) = app().await;
    let (status, cache, _) = send(
        &app,
        &format!("/v1/fluree/query/{LEDGER}"),
        "application/sparql-query",
        Some("application/sparql-results+json"),
        "SELECT (NOW() AS ?t) WHERE {}".to_string(),
        &[("fluree-cache", "true")],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cache.as_deref(), Some("bypass; reason=\"NOW()\""));
}

#[tokio::test]
async fn agent_json_reports_its_bypass() {
    let (_tmp, app) = app().await;
    let (status, cache, body) = send(
        &app,
        &format!("/v1/fluree/query/{LEDGER}"),
        "application/sparql-query",
        Some("application/vnd.fluree.agent+json"),
        "PREFIX ex: <http://example.org/>\nSELECT ?n WHERE { ?s ex:name ?n }".to_string(),
        &[("fluree-cache", "true")],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(cache.as_deref(), Some("bypass; reason=\"agent JSON\""));
}

#[tokio::test]
async fn a_zero_budget_reports_the_cache_disabled() {
    let (_tmp, app) = app_with(Some(0)).await;
    let (status, cache, _) = send(
        &app,
        &format!("/v1/fluree/query/{LEDGER}"),
        "application/sparql-query",
        Some("application/sparql-results+json"),
        "PREFIX ex: <http://example.org/>\nSELECT ?n WHERE { ?s ex:name ?n }".to_string(),
        &[("fluree-cache", "true")],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        cache.as_deref(),
        Some("bypass; reason=\"result cache disabled\"")
    );
}
