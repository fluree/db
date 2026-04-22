# Adding Tracing Spans to New Code

When you add or modify code paths in Fluree, you should instrument them with tracing spans so that performance investigations can decompose wall-clock time into meaningful phases. This guide explains the conventions, patterns, and gotchas.

## The Two-Tier Span Strategy

Fluree uses a tiered approach so that tracing is **zero-overhead by default** but **deeply informative on demand**.

### The `request` span: `info_span!` (the one exception)

The HTTP `request` span in `telemetry.rs::create_request_span()` is the only `info_span!` in the codebase. It provides operators with HTTP request visibility at the production default `RUST_LOG=info`. All other operation spans are `debug_span!` — this guarantees true zero overhead when the `otel` feature is not compiled and `RUST_LOG` is at `info`.

### Tier 1: `debug_span!` -- operation and phase level

All operation spans (`query_execute`, `transact_execute`, `txn_stage`, `txn_commit`, `index_build`, `sort_blocking`, etc.) and their phases use `debug_span!`. They are visible when OTEL is enabled (the OTEL `Targets` filter registers interest at DEBUG for `fluree_*` crates) or when a developer sets `RUST_LOG=debug` or `RUST_LOG=info,fluree_db_query=debug`. Without either, `debug_span!` short-circuits to a single atomic load (~1-2ns, unmeasurable).

### Tier 2: `trace_span!` -- maximum detail

Per-operator, per-item, or per-iteration spans. Visible at `RUST_LOG=info,fluree_db_query=trace`. Use for fine-grained instrumentation in hot paths where you only want visibility during deep investigation. The OTEL `Targets` filter intentionally excludes TRACE to prevent flooding the batch processor.

### Decision guide

| You're adding... | Span level | Example |
|-------------------|-----------|---------|
| New top-level operation, phase, or operator | `debug_span!` | `query_execute`, `reasoning_prep`, `join`, `binary_open_leaf` |
| Detail or per-iteration instrumentation | `trace_span!` | `group_by`, `distinct`, `binary_cursor_next_leaf` |

**Do not use `info_span!`** for new operation spans. The `request` span is the sole exception.

## Code Patterns

### Sync phases (no `.await`)

Use `span.enter()` which creates a guard dropped at end of scope:

```rust
let span = tracing::debug_span!(
    "pattern_rewrite",
    patterns_before = patterns.len() as u64,
    patterns_after = tracing::field::Empty,  // recorded later
);
let _guard = span.enter();

// ... do the rewriting ...

span.record("patterns_after", rewritten.len() as u64);
// _guard dropped here, span ends
```

### Async phases (contains `.await`)

**Never** hold a `span.enter()` guard across an `.await` point. In tokio's multi-threaded runtime, `span.enter()` enters the span on the current thread. When the task yields at `.await`, the span remains "entered" on that thread. Other tasks polled on the same thread will then inherit this span as their parent, causing **cross-request trace contamination** — completely unrelated operations become nested under each other in Jaeger. This was the root cause of a critical trace corruption bug in the HTTP route handlers.

**Symptoms in Jaeger**: If you see sequential, independent requests nested as children of an earlier request (especially where child spans outlive their parents), the cause is almost certainly `span.enter()` held across `.await`.

Instead, use `.instrument(span)`:

```rust
let span = tracing::debug_span!(
    "format",
    output_format = %format_name,
    result_count = total_rows as u64,
);
format_results(batch, format).instrument(span).await
```

If you need to record deferred fields on a span that wraps an async block, use `Span::current()` inside the instrumented block:

```rust
let span = tracing::debug_span!(
    "txn_stage",
    insert_count = tracing::field::Empty,
    delete_count = tracing::field::Empty,
);
async {
    // ... do staging work ...
    let current = tracing::Span::current();
    current.record("insert_count", inserts as u64);
    current.record("delete_count", deletes as u64);
    Ok(result)
}.instrument(span).await
```

### HTTP route handlers (axum)

Route handlers are async and **must** use `.instrument()`. The standard pattern wraps the entire handler body in an `async move` block instrumented with the request span, then uses `Span::current()` inside:

```rust
pub async fn query(
    State(state): State<Arc<AppState>>,
    headers: FlureeHeaders,
) -> Result<impl IntoResponse> {
    let span = create_request_span("query", request_id.as_deref(), ...);

    async move {
        let span = tracing::Span::current(); // Same span, safe to .record() on
        tracing::info!(status = "start", "query request received");

        let alias = get_ledger_alias(...)?;
        span.record("ledger_alias", alias.as_str());

        execute_query(&state, &alias, &query_json).await
    }
    .instrument(span)
    .await
}
```

**Why `async move` + `Span::current()` instead of just `.instrument()`**: Route handlers need to record deferred fields (like `ledger_alias`, `error_code`) on the span after creation. By obtaining `Span::current()` inside the instrumented block, you get a handle to the same span that `.instrument()` entered, letting you call `.record()` and pass it to `set_span_error_code()`.

### spawn_blocking

For `tokio::task::spawn_blocking`, enter the span *inside* the closure:

```rust
let span = tracing::debug_span!("heavy_compute");
tokio::task::spawn_blocking(move || {
    let _guard = span.enter();
    // ... sync work ...
}).await
```

### std::thread::scope (parallel OS threads)

`std::thread::scope` spawned threads do NOT inherit `tracing` span context from the parent thread. Capture the current span before spawning and enter it inside each closure:

```rust
let parent_span = tracing::Span::current();

std::thread::scope(|s| {
    for item in &work_items {
        let thread_span = parent_span.clone();
        s.spawn(move || {
            let _guard = thread_span.enter();
            // ... work that creates child spans ...
        });
    }
});
```

This is safe because scoped threads are pure sync (no `.await`). The same pattern applies to any OS thread spawning (`std::thread::spawn`, `rayon`, etc.).

### Lightweight operators (hot path)

For simple operators that just need a span marker, use the terse `.entered()` pattern:

```rust
fn open(&mut self, ctx: &mut Context<S, C>) -> Result<()> {
    let _span = tracing::trace_span!("filter").entered();
    self.child.open(ctx)?;
    // ...
    Ok(())
}
```

## Deferred Fields

Declare fields as `tracing::field::Empty` at span creation, then record values later. This is essential for fields whose values aren't known until the operation completes.

```rust
let span = tracing::debug_span!(
    "plan",
    pattern_count = tracing::field::Empty,
);
let _guard = span.enter();

let plan = build_plan(&patterns)?;
span.record("pattern_count", plan.patterns.len() as u64);
```

**Gotcha:** `tracing::Span::current().record(...)` records on the *current innermost* span. If you've entered a child span, `.record()` targets the child, not the parent. Get a handle to the parent span before entering children:

```rust
let parent_span = tracing::debug_span!("outer", total = tracing::field::Empty);
let _parent_guard = parent_span.enter();

{
    let _child = tracing::trace_span!("inner").entered();
    // Span::current() is now "inner", NOT "outer"
}

// Back to "outer" scope -- safe to record on parent
parent_span.record("total", count as u64);
```

## `#[tracing::instrument]` vs Manual Spans

**Use `#[tracing::instrument]`** for simple functions where:
- You want span entry/exit to match the function boundary
- The function name is a good span name
- You don't need deferred field recording

Always use `skip_all` and explicitly list fields:

```rust
#[tracing::instrument(level = "debug", name = "parse", skip_all, fields(input_format, input_bytes))]
fn parse_query(input: &[u8], format: &str) -> Result<Query> {
    // ...
}
```

**Use manual spans** when:
- The span covers only *part* of a function
- You need a different name than the function
- You need deferred fields
- The function is a hot path (the `#[instrument]` macro captures all arguments by default unless you `skip_all`)

## Where to Add Spans

### New query feature

If you add a new phase to query execution (e.g., a new optimization pass):

1. Add a `debug_span!` in the code path
2. Add the span name to the hierarchy in `docs/operations/telemetry.md`
3. Add a test in `fluree-db-api/tests/it_tracing_spans.rs` verifying the span emits

### New operator

If you add a new query operator:

1. For core structural operators (scan, join, filter, project, sort), use `debug_span!` in `open()`
2. For detail operators (group_by, distinct, limit, offset, etc.), use `trace_span!` in `open()`
3. If it's a blocking/buffering operator (like sort), add a `debug_span!` timing span in `next_batch()`
4. Add a test verifying the span emits at the correct level

For lower-level remote storage diagnostics on the binary path, prefer short-lived `debug_span!` blocks around:
- leaf-open strategy selection (`binary_open_leaf`)
- remote leaf metadata reads (`binary_fetch_header_dir`)
- individual range reads (`binary_range_fetch`)
- leaflet cache hit/miss points (`binary_load_leaflet`)

These spans are intended for investigation of repeated remote I/O and cache effectiveness under query load.

### New transaction phase

If you add a new phase to transaction processing:

1. Add a `debug_span!` in the phase code
2. Record relevant counts/sizes as deferred fields
3. Add the span to the hierarchy in `docs/operations/telemetry.md`

### New background task

If you add a new background task (like indexing, garbage collection, compaction):

1. Add a `debug_span!` as the **trace root** (these are independent traces, not children of HTTP requests)
2. Add debug sub-spans for phases within the task
3. Ensure the crate target is listed in the OTEL `Targets` filter in `telemetry.rs`

## Testing Spans

All new spans should have at least one test verifying they emit with expected fields at the right level.

### Test utilities

The test infrastructure lives in `fluree-db-api/tests/support/span_capture.rs`:

```rust
mod support;
use support::span_capture;

#[tokio::test]
async fn my_new_span_emits_at_debug_level() {
    let (store, _guard) = span_capture::init_test_tracing(); // captures ALL levels

    // ... run the code that emits the span ...

    assert!(store.has_span("my_new_phase"));
    let span = store.find_span("my_new_phase").unwrap();
    assert_eq!(span.level, tracing::Level::DEBUG);
    assert!(span.fields.contains_key("some_field"));
}

#[tokio::test]
async fn my_new_span_not_visible_at_info() {
    let (store, _guard) = span_capture::init_info_only_tracing(); // captures only INFO+

    // ... run the code ...

    assert!(!store.has_span("my_new_phase")); // zero noise at info
}
```

### Test helpers available

- `span_capture::init_test_tracing()` -- captures all spans regardless of level (for verifying span existence)
- `span_capture::init_info_only_tracing()` -- captures only INFO+ (for verifying zero-noise at default level)
- `SpanStore::has_span(name)` -- check if a span was emitted
- `SpanStore::find_span(name)` -- get span details (level, fields, parent)
- `SpanStore::find_spans(name)` -- find all spans with a given name
- `SpanStore::span_names()` -- list all captured span names

### Where to put tests

- Tracing integration tests go in `fluree-db-api/tests/it_tracing_spans.rs`
- The test utilities are in `fluree-db-api/tests/support/span_capture.rs`

## OTEL Layer Configuration

If you add a new crate that emits spans that should be exported via OTEL, add it to the `Targets` filter in `fluree-db-server/src/telemetry.rs`:

```rust
let otel_filter = Targets::new()
    .with_target("fluree_db_server", Level::DEBUG)
    .with_target("fluree_db_api", Level::DEBUG)
    // ... existing targets ...
    .with_target("my_new_crate", Level::DEBUG);  // ADD THIS
```

Without this, spans from the new crate will appear in console logs but not in Jaeger/Tempo.

**Important:** All OTEL targets are set to DEBUG level. Do **not** set any target to TRACE in the OTEL filter — TRACE-level spans (e.g., `binary_cursor_next_leaf`, per-scan spans) can generate thousands of spans per query, overwhelming the `BatchSpanProcessor` queue and causing parent spans to be dropped. Users who need TRACE-level detail should use `RUST_LOG` for console output; the OTEL exporter intentionally excludes TRACE spans.

## Checklist for New Instrumentation

- [ ] Used `debug_span!` (not `info_span!`) for all new operation spans
- [ ] Used `span.enter()` only in sync code, `.instrument(span)` for async
- [ ] Propagated span context into spawned threads (`spawn_blocking`, `std::thread::scope`, etc.)
- [ ] Added deferred fields for values computed after span creation
- [ ] Tested span emission with `SpanCaptureLayer`
- [ ] Verified zero overhead at INFO level (no debug/trace spans appear without OTEL or `RUST_LOG=debug`)
- [ ] Updated span hierarchy in `docs/operations/telemetry.md` if adding spans
- [ ] Updated `.claude/skills/*/references/span-hierarchy.md` (both copies)
- [ ] Added new crate to OTEL `Targets` filter if applicable

## Common Gotchas

1. **`span.enter()` across `.await` causes cross-request contamination** -- This is the most dangerous tracing bug. In tokio's multi-threaded runtime, `span.enter()` sets the span on the current thread. When the task suspends at `.await`, the span stays "entered" on that thread. Other tasks polled on the same thread inherit it as their parent. **Result**: unrelated requests cascade into each other's traces in Jaeger, with child spans that outlive their parents. Always use `.instrument(span)` in async code. This was a real bug in the HTTP route handlers and took Jaeger analysis to identify.
2. **`Span::current().record()` targets the innermost span** -- not necessarily the one you intend. Hold a reference to the span you want to record on.
3. **OTEL exporter floods** -- if you set `RUST_LOG=debug` globally, third-party crates (hyper, tonic, h2) emit debug spans that overwhelm the OTEL batch processor. The `Targets` filter on the OTEL layer prevents this.
4. **Tower-HTTP `TraceLayer` removed** -- tower-http's `TraceLayer` was removed entirely because it created a duplicate `request` span that collided with Fluree's own `request` span in `create_request_span()`. If you re-add tower-http tracing, ensure it does not conflict.
5. **`set_global_default` in tests** -- can only be called once per process. Use `set_default()` which returns a guard scoped to the test.
6. **Compiler won't catch `span.enter()` across `.await`** -- Unlike what the tracing docs suggest, `Entered` may actually be `Send` (since `&Span` is `Send` when `Span: Sync`). The code compiles fine but produces incorrect traces at runtime. The only way to detect this is visual inspection in Jaeger. Grep for `span.enter()` in async functions as part of code review.
7. **`std::thread::scope` / `std::thread::spawn` drops span context** -- New OS threads start with empty thread-local span context, so any spans created on them become orphaned root traces. You must capture `Span::current()` and `.enter()` it inside the thread closure. This same issue applies to `tokio::task::spawn_blocking`, `rayon`, and any other thread-spawning API.

## Claude Code Trace Analysis Skills

Two Claude Code skills are available for analyzing Jaeger trace exports:

### `/trace-inspect`

Drills into a **single trace**: span tree visualization, timing breakdown, structural health checks. Use when you have a specific slow request and want to understand where time went.

```
/trace-inspect path/to/traces.json
```

### `/trace-overview`

Analyzes **all traces** in an export: aggregate statistics, anomaly detection across the corpus, comparison of query vs transaction patterns. Use when you want a high-level understanding of system behavior.

```
/trace-overview path/to/traces.json
```

### Exporting traces from Jaeger

1. Open Jaeger UI (default: `http://localhost:16686`)
2. Search for traces of interest (by service name, operation, duration, etc.)
3. Click the JSON download button on a trace or search result
4. Save to a file and pass to either skill

See the [OTEL dev harness](../../otel/README.md) for running a local Jaeger instance.

## Related Documentation

- [Performance Investigation](../troubleshooting/performance-tracing.md) -- How operators use deep tracing
- [Telemetry and Logging](../operations/telemetry.md) -- Configuration reference
- [Deep Tracing Playbook](../../dev-docs/deep-tracing-replay-playbook.md) -- Comprehensive implementation reference
