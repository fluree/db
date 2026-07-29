//! Bench-time tracing setup.
//!
//! Replaces the 18-line `init_tracing_for_bench()` block that was duplicated
//! verbatim across `insert_formats.rs`, `vector_query.rs`, and
//! `fulltext_query.rs` before this chassis existed. Same opt-in semantics:
//! tracing is **off** unless `FLUREE_BENCH_TRACING` is set, so PR-gated runs
//! keep wall-clock measurements clean.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use fluree_bench_support::init_tracing_for_bench;
//!
//! fn bench_main(c: &mut Criterion) {
//!     init_tracing_for_bench();
//!     // ... rest of the bench ...
//! }
//! ```
//!
//! ## Env vars recognized
//!
//! | Var | Effect |
//! |---|---|
//! | unset / not `1`+ | Tracing **off**. No subscriber installed. Zero overhead. |
//! | `FLUREE_BENCH_TRACING=1` | Install a stderr subscriber filtered by `RUST_LOG` (defaults to `info` if `RUST_LOG` is unset). |
//! | `FLUREE_BENCH_TRACING=file:./out.jsonl` | Install a file-only [`BenchSpanLayer`] streaming one JSON span record per line, optionally filtered by `FLUREE_BENCH_SPAN_ALLOWLIST` (comma-separated span names). No in-memory sink — safe for long-running processes with no `take()` caller. |
//!
//! The crate-level `Targets` filter from `fluree-db-server::telemetry` is not
//! invoked here because benches typically run only one or two crates at DEBUG
//! and the `RUST_LOG` env-filter shape is more flexible.

use std::sync::OnceLock;

static INIT: OnceLock<()> = OnceLock::new();

/// Install a tracing subscriber for the current bench process if
/// `FLUREE_BENCH_TRACING` is set. Idempotent — safe to call from every
/// `bench_*` entry point.
///
/// **Off by default.** Calling this without `FLUREE_BENCH_TRACING=1` is a
/// no-op so PR-gated wall-clock numbers are not polluted by tracing overhead.
pub fn init_tracing_for_bench() {
    INIT.get_or_init(|| {
        match std::env::var("FLUREE_BENCH_TRACING").ok().as_deref() {
            Some("1") => install_stderr_subscriber(),
            Some(other) if other.starts_with("file:") => {
                let path = &other["file:".len()..];
                if let Err(e) = install_file_span_capture(path) {
                    install_stderr_subscriber();
                    ::tracing::warn!(
                        %path,
                        error = %e,
                        "could not open BenchSpanLayer output file; \
                         falling back to the stderr subscriber"
                    );
                }
            }
            _ => { /* tracing off: zero overhead */ }
        }
    });
}

fn install_stderr_subscriber() {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    let filter = tracing_subscriber::EnvFilter::from_default_env();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_level(true)
        .try_init();
}

// ---------------------------------------------------------------------------
// BenchSpanLayer
// ---------------------------------------------------------------------------

use std::collections::HashSet;
use std::io::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// One captured span: name, parent span name, lifetime, and its recorded
/// fields (both creation-time and late `span.record(...)` values — the
/// `iceberg.scan_plan` span records `files_selected`/`files_pruned` after
/// creation, so `on_record` merging is load-bearing).
#[derive(Debug, Clone, serde::Serialize)]
pub struct SpanRecord {
    pub name: &'static str,
    pub parent: Option<&'static str>,
    /// Span creation → close. For `.instrument(fut)` spans (the codebase
    /// idiom) this is the future's whole life including await/idle time —
    /// which is what wall-clock attribution wants.
    pub elapsed_us: u64,
    pub fields: serde_json::Map<String, serde_json::Value>,
}

/// Cloneable handle owning the capture sink. Create one per bench process,
/// install `.layer()` on the subscriber, then `take()` the records after
/// each measured unit of work (e.g. one query).
#[derive(Debug, Clone, Default)]
pub struct BenchSpanCapture {
    records: Arc<Mutex<Vec<SpanRecord>>>,
}

impl BenchSpanCapture {
    pub fn new() -> Self {
        Self::default()
    }

    /// Drain and return everything captured since the last `take()`.
    pub fn take(&self) -> Vec<SpanRecord> {
        std::mem::take(&mut *self.records.lock().expect("span sink poisoned"))
    }

    /// Build the capture layer. `allowlist = None` captures every span that
    /// reaches the layer; prefer an explicit allowlist plus a per-layer
    /// filter (see [`BenchSpanLayer::filter`]) so unrelated DEBUG spans
    /// cost nothing.
    pub fn layer(&self, allowlist: Option<&[&str]>) -> BenchSpanLayer {
        BenchSpanLayer {
            allowlist: allowlist.map(|names| names.iter().map(|s| s.to_string()).collect()),
            sink: Some(Arc::clone(&self.records)),
            file: None,
        }
    }
}

/// `tracing_subscriber::Layer` that captures span open/close with monotonic
/// timestamps and recorded fields into a [`BenchSpanCapture`] sink and/or a
/// JSONL file (`FLUREE_BENCH_TRACING=file:./out.jsonl`).
///
/// The in-memory sink is optional: the file-only installation
/// ([`install_file_span_capture`]) leaves it `None`, because a sink nobody
/// drains via [`BenchSpanCapture::take`] grows without bound over a
/// long-running process.
pub struct BenchSpanLayer {
    allowlist: Option<HashSet<String>>,
    sink: Option<Arc<Mutex<Vec<SpanRecord>>>>,
    file: Option<Arc<Mutex<std::io::BufWriter<std::fs::File>>>>,
}

impl BenchSpanLayer {
    fn allowed(&self, name: &str) -> bool {
        self.allowlist
            .as_ref()
            .is_none_or(|names| names.contains(name))
    }
}

/// A per-layer filter matching a span-name allowlist (`None` = all spans),
/// for use as `layer.with_filter(span_name_filter(...))`. Per-layer so a
/// composed fmt/otel layer still sees everything it wants.
pub fn span_name_filter<S>(
    allowlist: Option<Vec<String>>,
) -> tracing_subscriber::filter::DynFilterFn<
    S,
    impl Fn(&tracing::Metadata<'_>, &Context<'_, S>) -> bool,
>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    tracing_subscriber::filter::dynamic_filter_fn(move |meta, _cx| {
        meta.is_span()
            && allowlist
                .as_ref()
                .is_none_or(|names| names.iter().any(|n| n == meta.name()))
    })
}

/// Span-extension payload while a span is in flight.
struct InFlight {
    start: Instant,
    record: SpanRecord,
}

struct JsonVisitor<'a>(&'a mut serde_json::Map<String, serde_json::Value>);

impl Visit for JsonVisitor<'_> {
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().into(), format!("{value:?}").into());
    }
}

impl<S> Layer<S> for BenchSpanLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let meta = attrs.metadata();
        if !self.allowed(meta.name()) {
            return;
        }
        let Some(span) = ctx.span(id) else { return };
        let parent = span.parent().map(|p| p.metadata().name());
        let mut fields = serde_json::Map::new();
        attrs.record(&mut JsonVisitor(&mut fields));
        span.extensions_mut().insert(InFlight {
            start: Instant::now(),
            record: SpanRecord {
                name: meta.name(),
                parent,
                elapsed_us: 0,
                fields,
            },
        });
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let mut ext = span.extensions_mut();
        if let Some(in_flight) = ext.get_mut::<InFlight>() {
            values.record(&mut JsonVisitor(&mut in_flight.record.fields));
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let Some(mut in_flight) = span.extensions_mut().remove::<InFlight>() else {
            return;
        };
        in_flight.record.elapsed_us =
            u64::try_from(in_flight.start.elapsed().as_micros()).unwrap_or(u64::MAX);
        if let Some(file) = &self.file {
            if let (Ok(mut w), Ok(line)) = (file.lock(), serde_json::to_string(&in_flight.record)) {
                let _ = writeln!(w, "{line}");
                let _ = w.flush();
            }
        }
        if let Some(sink) = &self.sink {
            sink.lock()
                .expect("span sink poisoned")
                .push(in_flight.record);
        }
    }
}

/// Install a registry + a **file-only** `BenchSpanLayer` writing JSONL to
/// `path` (the `FLUREE_BENCH_TRACING=file:...` mode). Optional allowlist from
/// `FLUREE_BENCH_SPAN_ALLOWLIST` (comma-separated span names; unset = all
/// spans). Deliberately installs **no in-memory sink**: this mode serves
/// externally-traced, possibly long-running processes where nothing ever
/// drains a sink, which would otherwise grow without bound. An in-process
/// consumer should build its own [`BenchSpanCapture`] + [`BenchSpanCapture::layer`]
/// instead.
pub fn install_file_span_capture(path: &str) -> std::io::Result<()> {
    use tracing_subscriber::prelude::*;
    let allowlist_env = std::env::var("FLUREE_BENCH_SPAN_ALLOWLIST").ok();
    let allowlist: Option<Vec<String>> = allowlist_env.map(|v| {
        v.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    });
    let layer = BenchSpanLayer {
        allowlist: allowlist.as_ref().map(|v| v.iter().cloned().collect()),
        sink: None,
        file: Some(Arc::new(Mutex::new(std::io::BufWriter::new(
            std::fs::File::create(path)?,
        )))),
    };
    let _ = tracing_subscriber::registry()
        .with(layer.with_filter(span_name_filter(allowlist)))
        .try_init();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::prelude::*;

    /// A file-only layer (the `FLUREE_BENCH_TRACING=file:` shape) streams
    /// JSONL and holds no in-memory sink, so a long-running externally-traced
    /// process cannot accumulate records nobody drains.
    #[test]
    fn file_only_layer_writes_jsonl_without_a_sink() {
        let path =
            std::env::temp_dir().join(format!("bench-span-file-only-{}.jsonl", std::process::id()));
        let layer = BenchSpanLayer {
            allowlist: None,
            sink: None,
            file: Some(Arc::new(Mutex::new(std::io::BufWriter::new(
                std::fs::File::create(&path).unwrap(),
            )))),
        };
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::callsite::rebuild_interest_cache();
            let span = tracing::debug_span!("bench.file_only", files = 7i64);
            let _g = span.enter();
        });
        let written = std::fs::read_to_string(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        let line = written.lines().next().expect("one span record written");
        let record: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(record["name"], "bench.file_only");
        assert_eq!(record["fields"]["files"], 7);
    }

    #[test]
    fn init_tracing_is_idempotent() {
        // Don't set the env var; this should be a no-op.
        init_tracing_for_bench();
        init_tracing_for_bench();
        init_tracing_for_bench();
        // No assertion — we're just confirming repeated calls don't panic.
    }

    /// Allowlisted spans are captured with creation fields, late-recorded
    /// fields (`span.record`), parents, and nonzero elapsed; non-allowlisted
    /// spans are not captured. Uses a thread-local `set_default` — these
    /// callsites are unique to this test fn, so the interest-cache pinning
    /// trap (sibling test hitting the same callsite under a no-op global
    /// dispatcher first) cannot bite here.
    #[test]
    fn captures_allowlisted_spans_with_fields() {
        let capture = BenchSpanCapture::new();
        let layer = capture.layer(Some(&["bench.outer", "bench.inner"]));
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::callsite::rebuild_interest_cache();
            let outer = tracing::debug_span!("bench.outer", files = 3i64);
            let _og = outer.enter();
            {
                let inner = tracing::debug_span!("bench.inner", rows = tracing::field::Empty);
                let _ig = inner.enter();
                inner.record("rows", 42i64);
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            {
                let skipped = tracing::debug_span!("bench.skipped");
                let _sg = skipped.enter();
            }
        });
        let mut records = capture.take();
        records.sort_by_key(|r| r.name);
        assert_eq!(
            records.iter().map(|r| r.name).collect::<Vec<_>>(),
            vec!["bench.inner", "bench.outer"],
            "allowlisted spans captured, bench.skipped excluded"
        );
        let inner = &records[0];
        assert_eq!(inner.parent, Some("bench.outer"));
        assert_eq!(inner.fields.get("rows"), Some(&42i64.into()));
        assert!(inner.elapsed_us >= 2_000, "elapsed covers the sleep");
        let outer = &records[1];
        assert_eq!(outer.fields.get("files"), Some(&3i64.into()));
        assert!(capture.take().is_empty(), "take() drains the sink");
    }
}
