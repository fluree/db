//! A Delta graph source over HTTP: `POST /delta/map`, then queries — latest and
//! time-pinned — through the query routes, against the fixtures committed in
//! `fluree-db-delta`.
#![cfg(feature = "delta")]

use axum::body::Body;
use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

const MAPPING_TTL: &str = r#"
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<http://example.org/mapping#Item> a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "in_commit_time" ] ;
    rr:subjectMap [ rr:template "http://example.org/item/{id}" ; rr:class ex:Item ] ;
    rr:predicateObjectMap [
        rr:predicate ex:amount ;
        rr:objectMap [ rr:column "amount" ; rr:datatype xsd:integer ]
    ] .
"#;

fn fixtures() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../fluree-db-delta/tests/fixtures")
        .canonicalize()
        .expect("fixtures dir")
}

async fn server() -> (TempDir, Arc<AppState>) {
    // Read once per process; no other test in this harness sets it.
    std::env::set_var("FLUREE_ICEBERG_LOCAL_ROOTS", fixtures());
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

async fn send(state: &Arc<AppState>, request: Request<Body>) -> (StatusCode, String) {
    let resp = build_router(state.clone()).oneshot(request).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

async fn map(state: &Arc<AppState>, body: Value) -> (StatusCode, String) {
    send(
        state,
        Request::builder()
            .method("POST")
            .uri("/v1/fluree/delta/map")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
    )
    .await
}

async fn amounts(state: &Arc<AppState>, from: &str) -> (StatusCode, String) {
    let sparql = format!(
        "SELECT ?a FROM <{from}> WHERE {{ ?s <http://example.org/amount> ?a }} ORDER BY ?a"
    );
    send(
        state,
        Request::builder()
            .method("POST")
            .uri("/v1/fluree/query")
            .header("content-type", "application/sparql-query")
            .header("accept", "application/sparql-results+json")
            .body(Body::from(sparql))
            .unwrap(),
    )
    .await
}

fn values(body: &str) -> Vec<String> {
    let parsed: Value = serde_json::from_str(body).unwrap_or_else(|e| panic!("{e}: {body}"));
    parsed["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("bindings: {body}"))
        .iter()
        .map(|b| b["a"]["value"].as_str().expect("value").to_string())
        .collect()
}

#[tokio::test]
async fn a_mapped_delta_source_is_queried_over_http_latest_and_pinned() {
    let (_tmp, state) = server().await;

    let (status, text) = map(
        &state,
        json!({"name": "stock", "root": fixtures(), "r2rml": MAPPING_TTL}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{text}");
    let created: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(created["graph_source_id"], "stock:main");
    assert_eq!(created["table_versions"]["in_commit_time"], 2);
    assert!(created.get("table_warnings").is_none(), "{text}");

    // v0 = {100}, v1 = {100, 30000}, v2 = {30000}.
    let (status, text) = amounts(&state, "stock:main").await;
    assert_eq!(status, StatusCode::OK, "{text}");
    assert_eq!(values(&text), ["30000"], "{text}");

    let (status, text) = amounts(&state, "stock:main@snapshot:1").await;
    assert_eq!(status, StatusCode::OK, "{text}");
    assert_eq!(values(&text), ["100", "30000"]);

    // A version the table never had is the caller's error, not an empty result.
    let (status, text) = amounts(&state, "stock:main@snapshot:9").await;
    assert!(status.is_client_error(), "{status}: {text}");
    assert!(text.contains('9'), "{text}");

    // The JSON-LD twin of the connection query, then the single-source route.
    let from = json!({
        "@context": {"ex": "http://example.org/"},
        "from": "stock:main@snapshot:1",
        "select": ["?a"],
        "where": {"@id": "?s", "ex:amount": "?a"},
        "orderBy": "?a",
    });
    let (status, text) = send(
        &state,
        Request::builder()
            .method("POST")
            .uri("/v1/fluree/query")
            .header("content-type", "application/json")
            .body(Body::from(from.to_string()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{text}");
    assert_eq!(
        serde_json::from_str::<Value>(&text).unwrap(),
        json!([[100], [30000]])
    );

    // The same source through the single-source route, as JSON-LD.
    let jsonld = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?a"],
        "where": {"@id": "?s", "ex:amount": "?a"},
    });
    let (status, text) = send(
        &state,
        Request::builder()
            .method("POST")
            .uri("/v1/fluree/query/stock:main")
            .header("content-type", "application/json")
            .body(Body::from(jsonld.to_string()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{text}");
    assert_eq!(
        serde_json::from_str::<Value>(&text).unwrap(),
        json!([[30000]])
    );
}

#[tokio::test]
async fn a_map_request_the_server_cannot_honour_is_refused() {
    let (_tmp, state) = server().await;

    // Outside the local allowlist.
    let (status, text) = map(
        &state,
        json!({"name": "elsewhere", "root": "/etc", "r2rml": MAPPING_TTL}),
    )
    .await;
    assert!(status.is_client_error(), "{status}: {text}");

    // Half a service principal.
    let (status, text) = map(
        &state,
        json!({"name": "half", "root": fixtures(), "r2rml": MAPPING_TTL,
               "azure_tenant_id": "00000000-0000-0000-0000-000000000000"}),
    )
    .await;
    assert!(status.is_client_error(), "{status}: {text}");

    // An S3 endpoint override aimed at the instance metadata service.
    let (status, text) = map(
        &state,
        json!({"name": "ssrf", "root": "s3://bucket/lake", "r2rml": MAPPING_TTL,
               "s3_endpoint": "http://169.254.169.254/"}),
    )
    .await;
    assert!(status.is_client_error(), "{status}: {text}");

    // Unity Catalog: a workspace URL aimed at an internal host, a token URL
    // likewise, a secret in a variable the operator did not list, a catalog
    // beside a root, and a catalog with no way to authenticate.
    let unity = "https://workspace.example.com";
    for (name, extra) in [
        (
            "unity-ssrf",
            json!({"unity_uri": "http://169.254.169.254", "auth_bearer": "t"}),
        ),
        (
            "unity-token-ssrf",
            json!({"unity_uri": unity, "oauth2_client_id": "app", "oauth2_client_secret": "s",
                   "oauth2_token_url": "http://127.0.0.1/token"}),
        ),
        (
            "unity-env",
            json!({"unity_uri": unity, "auth_bearer_env": "AWS_SECRET_ACCESS_KEY"}),
        ),
        (
            "unity-root",
            json!({"unity_uri": unity, "auth_bearer": "t", "root": "s3://bucket/lake"}),
        ),
        ("unity-anonymous", json!({"unity_uri": unity})),
    ] {
        let mut body = json!({"name": name, "r2rml": MAPPING_TTL});
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let (status, text) = map(&state, body).await;
        assert!(status.is_client_error(), "{name}: {status}: {text}");
        let (status, text) = amounts(&state, &format!("{name}:main")).await;
        assert!(
            !status.is_success(),
            "{name} was registered: {status}: {text}"
        );
    }

    // Nothing above left a source behind.
    let (status, text) = amounts(&state, "elsewhere:main").await;
    assert!(!status.is_success(), "{status}: {text}");
}

async fn post(state: &Arc<AppState>, route: &str, body: Value) -> (StatusCode, Value) {
    let (status, text) = send(
        state,
        Request::builder()
            .method("POST")
            .uri(format!("/v1/fluree/delta/{route}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
    )
    .await;
    (
        status,
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
    )
}

fn codes(response: &Value) -> Vec<(&str, Option<&str>)> {
    let mut found: Vec<_> = response["diagnostics"]
        .as_array()
        .expect("diagnostics")
        .iter()
        .map(|d| (d["code"].as_str().unwrap(), d["column"].as_str()))
        .collect();
    found.sort_unstable();
    found
}

#[tokio::test]
async fn a_mapping_is_validated_against_its_tables_and_nothing_is_registered() {
    let (_tmp, state) = server().await;

    let (status, sound) = post(
        &state,
        "r2rml/validate",
        json!({"root": fixtures(), "r2rml": MAPPING_TTL}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{sound}");
    assert_eq!(sound["compiled_ok"], true);
    assert_eq!(sound["triples_map_count"], 1);
    assert_eq!(sound["table_names"], json!(["in_commit_time"]));
    // `id` and `amount` may be null in the fixture, which is said of the key.
    assert_eq!(codes(&sound), [("noSafeSubjectKey", Some("id"))]);
    // Asked here, while a source registered from this mapping would answer.
    let (status, text) = amounts(&state, "validate:main").await;
    assert!(!status.is_success(), "validate registered a source: {text}");

    let flawed = MAPPING_TTL
        .replace(r#"rr:column "amount""#, r#"rr:column "Amount""#)
        .replace("item/{id}", "item/{id}/{nope}");
    let (status, found) = post(
        &state,
        "r2rml/validate",
        json!({"name": "probe", "root": fixtures(), "r2rml": flawed}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{found}");
    assert_eq!(
        codes(&found),
        [
            ("casingMismatch", Some("Amount")),
            ("columnNotFound", Some("nope")),
            ("noSafeSubjectKey", Some("id")),
        ]
    );

    let absent = MAPPING_TTL.replace("in_commit_time", "no_such_table");
    let (status, found) = post(
        &state,
        "r2rml/validate",
        json!({"root": fixtures(), "r2rml": absent}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{found}");
    assert_eq!(codes(&found), [("tableNotFound", None)]);

    let (status, broken) = post(
        &state,
        "r2rml/validate",
        json!({"root": fixtures(), "r2rml": "not turtle"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{broken}");
    assert_eq!(broken["compiled_ok"], false);
}

#[tokio::test]
async fn a_catalog_request_the_server_cannot_honour_is_refused() {
    let (_tmp, state) = server().await;
    let unity = "https://workspace.example.com";
    let routes = [
        ("catalog/browse", json!({})),
        ("catalog/preview", json!({"table": "main.sales.orders"})),
        ("catalog/verify", json!({"table": "main.sales.orders"})),
        (
            "r2rml/generate",
            json!({"tables": ["main.sales.orders"], "base_namespace": "https://example.org/"}),
        ),
    ];
    let refused = [
        ("no catalog", json!({"auth_bearer": "t"})),
        (
            "internal host",
            json!({"unity_uri": "http://169.254.169.254", "auth_bearer": "t"}),
        ),
        (
            "internal token host",
            json!({"unity_uri": unity, "oauth2_client_id": "app", "oauth2_client_secret": "s",
                   "oauth2_token_url": "http://127.0.0.1/token"}),
        ),
        (
            "unlisted variable",
            json!({"unity_uri": unity, "auth_bearer_env": "AWS_SECRET_ACCESS_KEY"}),
        ),
        ("no way to authenticate", json!({"unity_uri": unity})),
    ];
    for (route, base) in &routes {
        for (case, connection) in &refused {
            let mut body = base.clone();
            body.as_object_mut()
                .unwrap()
                .extend(connection.as_object().unwrap().clone());
            let (status, text) = post(&state, route, body).await;
            assert!(
                status.is_client_error(),
                "{route}, {case}: {status}: {text}"
            );
        }
    }

    // An S3 endpoint is only verify's and validate's to aim.
    let (status, text) = post(
        &state,
        "catalog/verify",
        json!({"unity_uri": unity, "auth_bearer": "t", "table": "main.sales.orders",
               "s3_endpoint": "http://169.254.169.254/"}),
    )
    .await;
    assert!(status.is_client_error(), "{status}: {text}");
    let (status, text) = post(
        &state,
        "r2rml/validate",
        json!({"root": "s3://bucket/lake", "r2rml": MAPPING_TTL,
               "s3_endpoint": "http://169.254.169.254/"}),
    )
    .await;
    assert!(status.is_client_error(), "{status}: {text}");

    let (status, text) = post(
        &state,
        "r2rml/generate",
        json!({"unity_uri": unity, "auth_bearer": "t", "tables": [],
               "base_namespace": "https://example.org/"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
}

#[tokio::test]
async fn the_catalog_endpoints_are_the_administrators() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        admin_auth_mode: fluree_db_server::config::AdminAuthMode::Required,
        admin_auth_insecure_accept_any_issuer: true,
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    for route in [
        "catalog/browse",
        "catalog/preview",
        "catalog/verify",
        "r2rml/generate",
        "r2rml/validate",
    ] {
        let (status, text) = post(&state, route, json!({})).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{route}: {text}");
    }
}
