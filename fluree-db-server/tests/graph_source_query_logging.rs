//! A query that names a graph source is not a failed ledger lookup.
//!
//! The query and info routes try a name as a ledger first and as a graph source
//! second. The first step used to report its miss — an `ERROR` line and an
//! `error:NotFound` span code — before the second had been tried, so every
//! successful graph-source query read as a failure in logs and traces.
#![cfg(feature = "delta")]

use axum::body::Body;
use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::json;
use std::sync::{Arc, Mutex};
use tower::ServiceExt;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

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

/// `ERROR` events and `error_code` span records, as `"<what>: <text>"`.
#[derive(Clone, Default)]
struct Failures(Arc<Mutex<Vec<String>>>);

struct Text(String);

impl Visit for Text {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if matches!(field.name(), "message" | "error_code") {
            self.0 = format!("{value:?}");
        }
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        if matches!(field.name(), "message" | "error_code") {
            self.0 = value.to_string();
        }
    }
}

impl<S: tracing::Subscriber> Layer<S> for Failures {
    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        if *event.metadata().level() == tracing::Level::ERROR {
            let mut text = Text(String::new());
            event.record(&mut text);
            self.0.lock().unwrap().push(format!("event: {}", text.0));
        }
    }

    fn on_record(
        &self,
        _: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        _: Context<'_, S>,
    ) {
        let mut text = Text(String::new());
        values.record(&mut text);
        if !text.0.is_empty() {
            self.0.lock().unwrap().push(format!("span: {}", text.0));
        }
    }
}

impl Failures {
    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

async fn send(state: &Arc<AppState>, request: Request<Body>) -> (StatusCode, String) {
    let resp = build_router(state.clone()).oneshot(request).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

fn post(uri: &str, content_type: &str, body: String) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", content_type)
        .header(
            "accept",
            "application/sparql-results+json, application/json",
        )
        .body(Body::from(body))
        .unwrap()
}

/// One request per route that tries `name` as a ledger before a graph source.
fn requests(name: &str) -> Vec<(&'static str, Request<Body>)> {
    let target = format!("/v1/fluree/query/{name}");
    let jsonld = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?a"],
        "where": {"@id": "?s", "ex:amount": "?a"},
    });
    vec![
        (
            "JSON-LD",
            post(&target, "application/json", jsonld.to_string()),
        ),
        (
            "SPARQL",
            post(
                &target,
                "application/sparql-query",
                "SELECT ?a WHERE { ?s <http://example.org/amount> ?a }".to_string(),
            ),
        ),
        (
            "SPARQL with a dataset clause",
            post(
                &target,
                "application/sparql-query",
                format!("SELECT ?a FROM <{name}> WHERE {{ ?s <http://example.org/amount> ?a }}"),
            ),
        ),
        (
            "info",
            Request::builder()
                .method("GET")
                .uri(format!("/v1/fluree/info/{name}"))
                .body(Body::empty())
                .unwrap(),
        ),
    ]
}

#[tokio::test]
async fn a_graph_source_query_reports_no_missing_ledger_and_a_missing_name_still_does() {
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../fluree-db-delta/tests/fixtures")
        .canonicalize()
        .expect("fixtures dir");
    std::env::set_var("FLUREE_ICEBERG_LOCAL_ROOTS", &fixtures);
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));

    let failures = Failures::default();
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(failures.clone()));

    let (status, text) = send(
        &state,
        post(
            "/v1/fluree/delta/map",
            "application/json",
            json!({"name": "stock", "root": fixtures, "r2rml": MAPPING_TTL}).to_string(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{text}");
    failures.take();

    for (route, request) in requests("stock:main") {
        let (status, text) = send(&state, request).await;
        assert_eq!(status, StatusCode::OK, "{route}: {text}");
        assert_eq!(failures.take(), Vec::<String>::new(), "{route}");
    }

    // A name that is neither is still reported, on every one of those routes.
    for (route, request) in requests("nothing:main") {
        let (status, text) = send(&state, request).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{route}: {text}");
        let reported = failures.take();
        assert!(
            reported.iter().any(|f| f == "event: ledger not found"),
            "{route}: {reported:?}"
        );
        // Only the info route's span has an `error_code` field to carry it.
        assert_eq!(
            reported.iter().any(|f| f == "span: error:NotFound"),
            route == "info",
            "{route}: {reported:?}"
        );
    }
}
