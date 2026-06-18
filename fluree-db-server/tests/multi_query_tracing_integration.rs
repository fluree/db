//! Span-hierarchy acceptance test for `POST /v1/fluree/multi-query`.
//!
//! Verifies the envelope dispatcher emits the expected `sub_query`
//! child spans per alias, with the agreed attribute set (`alias`,
//! `language`, `effective_timeout_ms`, `result_status`). This is the
//! programmatic counterpart to the span tree documented in
//! `docs/operations/telemetry.md` and the multi-query subsection of
//! `.claude/skills/trace-{inspect,overview}/references/span-hierarchy.md`.

use axum::body::Body;
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use serde_json::{json, Value as JsonValue};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tower::ServiceExt;
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

// =============================================================================
// Inline span capture (minimal — just what we need to assert on)
// =============================================================================

#[derive(Debug, Clone, Default)]
struct CapturedSpan {
    name: &'static str,
    fields: HashMap<String, String>,
}

#[derive(Debug, Clone, Default)]
struct SpanStore(Arc<Mutex<Vec<CapturedSpan>>>);

impl SpanStore {
    fn find_spans(&self, name: &str) -> Vec<CapturedSpan> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.name == name)
            .cloned()
            .collect()
    }
}

struct CaptureLayer {
    store: SpanStore,
}

struct FieldVisitor(HashMap<String, String>);

impl tracing::field::Visit for FieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
}

struct SpanIndex(usize);

impl<S> Layer<S> for CaptureLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: Context<'_, S>,
    ) {
        let mut visitor = FieldVisitor(HashMap::new());
        attrs.record(&mut visitor);
        let captured = CapturedSpan {
            name: attrs.metadata().name(),
            fields: visitor.0,
        };
        let mut guard = self.store.0.lock().unwrap();
        guard.push(captured);
        let idx = guard.len() - 1;
        drop(guard);
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(SpanIndex(idx));
        }
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: Context<'_, S>,
    ) {
        if let Some(span) = ctx.span(id) {
            if let Some(SpanIndex(idx)) = span.extensions().get::<SpanIndex>() {
                let mut visitor = FieldVisitor(HashMap::new());
                values.record(&mut visitor);
                let mut guard = self.store.0.lock().unwrap();
                if let Some(captured) = guard.get_mut(*idx) {
                    for (k, v) in visitor.0 {
                        captured.fields.insert(k, v);
                    }
                }
            }
        }
    }
}

fn init_capture() -> (SpanStore, tracing::subscriber::DefaultGuard) {
    let store = SpanStore::default();
    let layer = CaptureLayer {
        store: store.clone(),
    };
    // Lower the filter to DEBUG so debug_span!s are captured. The
    // test harness's normal RUST_LOG might be info.
    let filter = tracing_subscriber::filter::LevelFilter::DEBUG;
    let subscriber = tracing_subscriber::registry().with(layer).with(filter);
    let guard = tracing::subscriber::set_default(subscriber);
    (store, guard)
}

// =============================================================================
// Fixture
// =============================================================================

async fn test_state() -> (TempDir, Arc<AppState>) {
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

async fn create_ledger(app: &axum::Router, ledger: &str) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(format!(r#"{{"ledger":"{ledger}"}}"#)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "create {ledger}");
}

async fn insert_one(app: &axum::Router, ledger: &str, id: &str, name: &str) {
    let body = json!({
        "@context": { "ex": "http://example.org/" },
        "insert":   { "@id": id, "ex:name": name }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/insert/{ledger}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "insert into {ledger}");
}

async fn post_envelope(app: &axum::Router, envelope: &JsonValue) -> StatusCode {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/multi-query")
                .header("content-type", "application/json")
                .body(Body::from(envelope.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    resp.status()
}

// =============================================================================
// Span hierarchy assertions
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn multi_query_emits_one_sub_query_span_per_alias() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "mqt:a").await;
    insert_one(&app, "mqt:a", "ex:p", "P").await;

    // Bring up tracing capture only for the multi-query call. Seeding
    // spans from create/insert above would otherwise add noise.
    let (store, _guard) = init_capture();

    let envelope = json!({
        "queries": {
            "by_jsonld": {
                "language": "jsonld",
                "query": {
                    "@context": { "ex": "http://example.org/" },
                    "from":   "mqt:a",
                    "select": ["?name"],
                    "where":  { "@id": "?s", "ex:name": "?name" }
                }
            },
            "by_sparql": {
                "language": "sparql",
                "query": "PREFIX ex: <http://example.org/> SELECT ?name FROM <mqt:a> WHERE { ?s ex:name ?name }"
            }
        }
    });

    let status = post_envelope(&app, &envelope).await;
    assert_eq!(status, StatusCode::OK);

    let sub_queries = store.find_spans("sub_query");
    assert_eq!(
        sub_queries.len(),
        2,
        "expected exactly two sub_query spans (one per alias); got {}",
        sub_queries.len()
    );

    // Each span must carry the agreed attribute set:
    //   alias, language, and (after task end) result_status. The
    //   effective_timeout_ms attribute is recorded as a u64 after
    //   permit acquisition.
    for sq in &sub_queries {
        let alias = sq
            .fields
            .get("alias")
            .expect("sub_query must record an alias");
        assert!(
            alias == "by_jsonld" || alias == "by_sparql",
            "unexpected alias on sub_query span: {alias}"
        );
        let language = sq
            .fields
            .get("language")
            .expect("sub_query must record a language");
        match alias.as_str() {
            "by_jsonld" => assert_eq!(language, "jsonld"),
            "by_sparql" => assert_eq!(language, "sparql"),
            _ => unreachable!(),
        }
        assert_eq!(
            sq.fields.get("result_status").map(String::as_str),
            Some("ok"),
            "successful sub_query should record result_status = ok, got fields: {:?}",
            sq.fields
        );
        assert!(
            sq.fields.contains_key("effective_timeout_ms"),
            "sub_query should record effective_timeout_ms after permit acquisition"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn sub_query_span_records_error_status_on_per_alias_failure() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "mqt:e").await;
    insert_one(&app, "mqt:e", "ex:p", "P").await;

    let (store, _guard) = init_capture();

    // The 'bad' alias parses fine at envelope validation (deferred to
    // downstream parser) but fails at execution — sub_query span must
    // record result_status = "error".
    let envelope = json!({
        "queries": {
            "good": {
                "language": "jsonld",
                "query": {
                    "@context": { "ex": "http://example.org/" },
                    "from":   "mqt:e",
                    "select": ["?name"],
                    "where":  { "@id": "?s", "ex:name": "?name" }
                }
            },
            "bad": {
                "language": "sparql",
                "query": "SELECT ?x FROM <mqt:e> WHERE { this is not valid SPARQL }"
            }
        }
    });
    let status = post_envelope(&app, &envelope).await;
    assert_eq!(status, StatusCode::OK);

    let sub_queries = store.find_spans("sub_query");
    assert_eq!(sub_queries.len(), 2);

    let bad = sub_queries
        .iter()
        .find(|s| s.fields.get("alias").map(String::as_str) == Some("bad"))
        .expect("bad sub_query span should exist");
    assert_eq!(
        bad.fields.get("result_status").map(String::as_str),
        Some("error"),
        "bad alias sub_query should record result_status = error, got: {:?}",
        bad.fields
    );

    let good = sub_queries
        .iter()
        .find(|s| s.fields.get("alias").map(String::as_str) == Some("good"))
        .expect("good sub_query span should exist");
    assert_eq!(
        good.fields.get("result_status").map(String::as_str),
        Some("ok"),
        "good alias sub_query should record result_status = ok"
    );
}
