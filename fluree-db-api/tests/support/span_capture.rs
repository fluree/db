//! Span capture layer for programmatic verification of tracing instrumentation.
//!
//! Provides a custom `tracing_subscriber::Layer` that records span creation events
//! into a thread-safe `SpanStore`. Used by integration tests to verify span hierarchy,
//! levels, and field values without requiring Jaeger or any external OTEL backend.
//!
//! # Usage
//!
//! ```ignore
//! let (store, _guard) = init_test_tracing();
//! // ... run queries/transactions ...
//! assert!(store.has_span("query_prepare"));
//! let span = store.find_span("txn_stage").unwrap();
//! assert_eq!(span.level, tracing::Level::DEBUG);
//! ```
//!
//! Uses `tracing::subscriber::set_default()` (not `set_global_default`) for test
//! isolation — each test gets its own subscriber via the returned `DefaultGuard`.
//! Tests MUST use `#[tokio::test(flavor = "current_thread")]` so all async work
//! runs on the thread where the subscriber is installed.

// Kept for: test utilities — not all methods are used in every test file.
// Use when: writing new tracing acceptance tests in it_tracing_spans.rs.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

/// A captured span with its metadata and fields.
#[derive(Debug, Clone)]
pub struct CapturedSpan {
    pub name: &'static str,
    pub level: tracing::Level,
    pub fields: HashMap<String, String>,
    pub parent_name: Option<String>,
    /// True if the parent was set explicitly (via `parent:` on the span macro),
    /// false if determined from the current span context. When false, the parent
    /// is contextual — correct under `current_thread` runtime but may be wrong
    /// under multi-threaded runtimes.
    pub parent_is_explicit: bool,
    /// True once the span has been closed (guard dropped). False means the span
    /// was created but never closed — a potential span leak.
    pub closed: bool,
}

/// Index into the SpanStore vec, stored in span extensions for `on_record` and `on_close` updates.
struct SpanIndex(usize);

/// Thread-safe store of captured spans with query methods.
#[derive(Debug, Clone, Default)]
pub struct SpanStore(Arc<Mutex<Vec<CapturedSpan>>>);

impl SpanStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if any span with the given name was captured.
    pub fn has_span(&self, name: &str) -> bool {
        self.0.lock().unwrap().iter().any(|s| s.name == name)
    }

    /// Find the first span with the given name.
    pub fn find_span(&self, name: &str) -> Option<CapturedSpan> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .find(|s| s.name == name)
            .cloned()
    }

    /// Find all spans with the given name.
    pub fn find_spans(&self, name: &str) -> Vec<CapturedSpan> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.name == name)
            .cloned()
            .collect()
    }

    /// Return all captured span names (in order of creation).
    pub fn span_names(&self) -> Vec<&'static str> {
        self.0.lock().unwrap().iter().map(|s| s.name).collect()
    }

    /// Return all captured spans at DEBUG level.
    pub fn debug_spans(&self) -> Vec<CapturedSpan> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.level == tracing::Level::DEBUG)
            .cloned()
            .collect()
    }

    /// Return all captured spans at TRACE level.
    pub fn trace_spans(&self) -> Vec<CapturedSpan> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.level == tracing::Level::TRACE)
            .cloned()
            .collect()
    }

    /// Return all captured spans at INFO level.
    pub fn info_spans(&self) -> Vec<CapturedSpan> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.level == tracing::Level::INFO)
            .cloned()
            .collect()
    }

    /// Find spans that were created but never closed (potential leaks).
    pub fn unclosed_spans(&self) -> Vec<CapturedSpan> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|s| !s.closed)
            .cloned()
            .collect()
    }

    /// Find all children of a given parent span name.
    pub fn children_of(&self, parent_name: &str) -> Vec<CapturedSpan> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.parent_name.as_deref() == Some(parent_name))
            .cloned()
            .collect()
    }
}

/// Custom tracing layer that captures span creation, field updates, and close events.
pub struct SpanCaptureLayer {
    store: SpanStore,
}

impl SpanCaptureLayer {
    pub fn new(store: SpanStore) -> Self {
        Self { store }
    }
}

impl<S> Layer<S> for SpanCaptureLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: Context<'_, S>,
    ) {
        let mut fields = FieldVisitor(HashMap::new());
        attrs.record(&mut fields);

        // Determine parent span name.
        // First check explicit parent (set via `parent:` in span macro),
        // then fall back to contextual current span.
        let explicit_parent = attrs
            .parent()
            .and_then(|pid| ctx.span(pid))
            .map(|span| span.name().to_string());

        let (parent_name, parent_is_explicit) = match explicit_parent {
            Some(name) => (Some(name), true),
            None => {
                let contextual = ctx.lookup_current().map(|span| span.name().to_string());
                (contextual, false)
            }
        };

        let span_ref = ctx.span(id).expect("span should exist in registry");
        let meta = span_ref.metadata();

        let index = {
            let mut store = self.store.0.lock().unwrap();
            let index = store.len();
            store.push(CapturedSpan {
                name: meta.name(),
                level: *meta.level(),
                fields: fields.0,
                parent_name,
                parent_is_explicit,
                closed: false,
            });
            index
        };

        // Store index in span extensions for on_record and on_close lookups
        span_ref.extensions_mut().insert(SpanIndex(index));
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: Context<'_, S>,
    ) {
        if let Some(span_ref) = ctx.span(id) {
            if let Some(index) = span_ref.extensions().get::<SpanIndex>() {
                let mut visitor = FieldVisitor(HashMap::new());
                values.record(&mut visitor);
                let mut store = self.store.0.lock().unwrap();
                if let Some(captured) = store.get_mut(index.0) {
                    captured.fields.extend(visitor.0);
                }
            }
        }
    }

    fn on_close(&self, id: tracing::span::Id, ctx: Context<'_, S>) {
        if let Some(span_ref) = ctx.span(&id) {
            if let Some(index) = span_ref.extensions().get::<SpanIndex>() {
                let mut store = self.store.0.lock().unwrap();
                if let Some(captured) = store.get_mut(index.0) {
                    captured.closed = true;
                }
            }
        }
    }
}

/// Field visitor that extracts typed values into a string HashMap.
struct FieldVisitor(HashMap<String, String>);

impl tracing::field::Visit for FieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
}

/// Initialize test tracing that captures ALL span levels.
///
/// Returns the `SpanStore` for querying captured spans and a `DefaultGuard`
/// that restores the previous subscriber when dropped.
///
/// Must be used with `#[tokio::test(flavor = "current_thread")]`.
pub fn init_test_tracing() -> (SpanStore, tracing::subscriber::DefaultGuard) {
    let store = SpanStore::new();
    let layer = SpanCaptureLayer::new(store.clone());
    let subscriber = tracing_subscriber::Registry::default().with(layer);
    let guard = tracing::subscriber::set_default(subscriber);
    (store, guard)
}

/// Initialize test tracing that captures only INFO+ spans.
///
/// Use this to verify the "zero noise at default level" guarantee:
/// no debug/trace spans should appear in production-default logging.
///
/// Must be used with `#[tokio::test(flavor = "current_thread")]`.
pub fn init_info_only_tracing() -> (SpanStore, tracing::subscriber::DefaultGuard) {
    let store = SpanStore::new();
    let layer = SpanCaptureLayer::new(store.clone());
    let filter = tracing_subscriber::filter::LevelFilter::INFO;
    let subscriber = tracing_subscriber::Registry::default().with(layer.with_filter(filter));
    let guard = tracing::subscriber::set_default(subscriber);
    (store, guard)
}
