//! Integration tests for telemetry functionality.
//!
//! These tests verify that `create_request_span` and `set_span_error_code` produce
//! the expected span metadata. They use a local `SpanCaptureLayer` (via `set_default`)
//! rather than `init_logging` (which sets a global subscriber) to avoid global state
//! conflicts between tests.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use fluree_db_server::telemetry::{create_request_span, extract_trace_id, set_span_error_code};
use http::{HeaderMap, HeaderValue};
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

// ---------------------------------------------------------------------------
// Minimal span capture (self-contained — server tests don't share fluree-db-api test support)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct CapturedSpan {
    name: &'static str,
    level: tracing::Level,
    fields: HashMap<String, String>,
}

struct SpanIdx(usize);

#[derive(Clone, Default)]
struct Store(Arc<Mutex<Vec<CapturedSpan>>>);

struct Capture(Store);

struct Vis(HashMap<String, String>);

impl tracing::field::Visit for Vis {
    fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
        self.0.insert(f.name().to_string(), format!("{v:?}"));
    }
    fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
        self.0.insert(f.name().to_string(), v.to_string());
    }
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: Context<'_, S>,
    ) {
        let mut vis = Vis(HashMap::new());
        attrs.record(&mut vis);
        let span_ref = ctx.span(id).expect("span exists");
        let meta = span_ref.metadata();
        let idx = {
            let mut store = self.0 .0.lock().unwrap();
            let idx = store.len();
            store.push(CapturedSpan {
                name: meta.name(),
                level: *meta.level(),
                fields: vis.0,
            });
            idx
        };
        span_ref.extensions_mut().insert(SpanIdx(idx));
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: Context<'_, S>,
    ) {
        if let Some(span_ref) = ctx.span(id) {
            if let Some(idx) = span_ref.extensions().get::<SpanIdx>() {
                let mut vis = Vis(HashMap::new());
                values.record(&mut vis);
                let mut store = self.0 .0.lock().unwrap();
                if let Some(cap) = store.get_mut(idx.0) {
                    cap.fields.extend(vis.0);
                }
            }
        }
    }
}

fn init_capture() -> (Store, tracing::subscriber::DefaultGuard) {
    let store = Store::default();
    let sub = tracing_subscriber::Registry::default().with(Capture(store.clone()));
    let guard = tracing::subscriber::set_default(sub);
    (store, guard)
}

fn headers_with_trace_id(trace_id: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("x-trace-id", HeaderValue::from_str(trace_id).unwrap());
    headers
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn request_span_has_expected_fields() {
    let (store, _guard) = init_capture();
    let headers = headers_with_trace_id("trace-456");

    let span = create_request_span(
        "query",
        Some("req-123"),
        extract_trace_id(&headers).as_deref(),
        Some("mydb:main"),
        None,
        Some("sparql"),
    );

    // Enter + exit the span so it's captured
    let _entered = span.enter();
    drop(_entered);

    let spans = store.0.lock().unwrap();
    let req = spans.iter().find(|s| s.name == "request");
    assert!(
        req.is_some(),
        "request span should be captured. Got: {:?}",
        spans.iter().map(|s| s.name).collect::<Vec<_>>()
    );

    let req = req.unwrap();
    assert_eq!(
        req.level,
        tracing::Level::INFO,
        "request span should be INFO (the one exception)"
    );
    assert_eq!(
        req.fields.get("operation").map(std::string::String::as_str),
        Some("query")
    );
    assert_eq!(
        req.fields
            .get("request_id")
            .map(std::string::String::as_str),
        Some("req-123")
    );
    assert_eq!(
        req.fields.get("trace_id").map(std::string::String::as_str),
        Some("trace-456")
    );
    assert_eq!(
        req.fields.get("ledger_id").map(std::string::String::as_str),
        Some("mydb:main")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn request_span_otel_name_includes_format() {
    let (store, _guard) = init_capture();
    let headers = HeaderMap::new();

    let span = create_request_span(
        "query",
        None,
        extract_trace_id(&headers).as_deref(),
        None,
        None,
        Some("sparql"),
    );
    let _entered = span.enter();
    drop(_entered);

    let spans = store.0.lock().unwrap();
    let req = spans.iter().find(|s| s.name == "request").unwrap();

    // otel.name should be "query:sparql"
    let otel_name = req
        .fields
        .get("otel.name")
        .expect("otel.name should be set");
    assert_eq!(
        otel_name, "query:sparql",
        "otel.name should combine operation:format"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn request_span_otel_name_without_format() {
    let (store, _guard) = init_capture();
    let headers = HeaderMap::new();

    let span = create_request_span(
        "ledger:create",
        None,
        extract_trace_id(&headers).as_deref(),
        None,
        None,
        None,
    );
    let _entered = span.enter();
    drop(_entered);

    let spans = store.0.lock().unwrap();
    let req = spans.iter().find(|s| s.name == "request").unwrap();

    let otel_name = req
        .fields
        .get("otel.name")
        .expect("otel.name should be set");
    assert_eq!(
        otel_name, "ledger:create",
        "otel.name should be just the operation when no format"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn set_error_code_records_on_span() {
    let (store, _guard) = init_capture();
    let headers = HeaderMap::new();

    let span = create_request_span(
        "transact",
        None,
        extract_trace_id(&headers).as_deref(),
        None,
        None,
        Some("json-ld"),
    );

    // Record error code on the span
    set_span_error_code(&span, "error:ParseError");

    // Enter briefly to ensure the span is in the registry
    let _entered = span.enter();
    drop(_entered);

    let spans = store.0.lock().unwrap();
    let req = spans.iter().find(|s| s.name == "request").unwrap();

    let error_code = req
        .fields
        .get("error_code")
        .expect("error_code should be recorded");
    assert_eq!(
        error_code, "error:ParseError",
        "error_code should match what was set"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn error_code_is_empty_on_success() {
    let (store, _guard) = init_capture();
    let headers = HeaderMap::new();

    // Create span but don't set error code (success path)
    let span = create_request_span(
        "query",
        None,
        extract_trace_id(&headers).as_deref(),
        None,
        None,
        Some("json-ld"),
    );
    let _entered = span.enter();
    drop(_entered);

    let spans = store.0.lock().unwrap();
    let req = spans.iter().find(|s| s.name == "request").unwrap();

    // error_code should not have a meaningful value — OTEL convention is to omit on success
    let error_code = req.fields.get("error_code");
    assert!(
        error_code.is_none() || error_code == Some(&String::new()),
        "error_code should be empty/absent on success path, got: {error_code:?}"
    );
}
