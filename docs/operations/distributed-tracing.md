# Distributed Tracing Integration

This guide explains how to correlate your application's traces and logs with Fluree's internal instrumentation, whether you use Fluree as an embedded Rust library (`fluree-db-api`) or as an HTTP server (`fluree-db-server`).

## Overview

Fluree instruments queries, transactions, and indexing with [tracing](https://docs.rs/tracing) spans. These spans can participate in your application's distributed traces so that a single trace shows the full picture: your application code, the Fluree call, and every internal phase (parsing, planning, execution, commit, etc.).

There are two integration paths depending on how you use Fluree:

| Integration mode | Mechanism | What you get |
|-----------------|-----------|--------------|
| **Rust library** (`fluree-db-api`) | Shared `tracing` subscriber | Fluree spans automatically nest under your application spans |
| **HTTP server** (`fluree-db-server`) | W3C Trace Context (`traceparent` header) | Fluree's request span becomes a child of your distributed trace |

## Rust Library Integration (`fluree-db-api`)

When you embed Fluree via `fluree-db-api`, trace correlation works automatically through the `tracing` crate's context propagation -- no special Fluree configuration required.

### How it works

The `tracing` crate uses task-local storage to track the "current span." When your code creates a span and then calls a Fluree API method, any spans Fluree creates internally become children of your span. This happens automatically as long as both your code and Fluree share the same `tracing` subscriber (which they do by default -- there's one global subscriber per process).

### Basic setup

```rust
use fluree_db_api::{FlureeBuilder, Result};
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing -- Fluree's spans will appear here too
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let fluree = FlureeBuilder::new()
        .with_storage_path("./data")
        .build()
        .await?;

    // Your application span wraps the Fluree call
    let span = tracing::info_span!("handle_request", user_id = %user_id);
    async {
        let db = fluree.db("my-ledger", None).await?;
        let result = fluree.query(&db, my_query).await?;
        Ok(result)
    }
    .instrument(span)
    .await
}
```

At the default `RUST_LOG=info`, Fluree's info-level log events appear within your span's context:

```
INFO handle_request{user_id=42}: fluree_db_api::view::query: parse_ms=0.12 plan_ms=0.45 exec_ms=3.21 query phases
```

With `RUST_LOG=info,fluree_db_query=debug`, you additionally see Fluree's operation spans nested under yours:

```
INFO  handle_request{user_id=42}: my_app: handling request
DEBUG handle_request{user_id=42}:query_execute: fluree_db_query: ...
DEBUG handle_request{user_id=42}:query_execute:query_prepare: fluree_db_query: ...
DEBUG handle_request{user_id=42}:query_execute:query_run: fluree_db_query: ...
INFO  handle_request{user_id=42}:query_execute: fluree_db_api: parse_ms=0.12 plan_ms=0.45 exec_ms=3.21 query phases
```

### With OpenTelemetry export

If your application exports traces to an OTEL backend (Jaeger, Tempo, Datadog, etc.), Fluree's spans appear in the same trace waterfall:

```rust
use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Registry};

fn init_tracing() {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint("http://localhost:4317")
        .build()
        .expect("OTLP exporter");

    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(exporter)
        .build();

    global::set_tracer_provider(provider);

    let otel_layer = OpenTelemetryLayer::new(global::tracer("my-app"));

    let subscriber = Registry::default()
        .with(otel_layer)
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer());

    tracing::subscriber::set_global_default(subscriber).unwrap();
}
```

In Jaeger/Tempo, you'll see a single trace containing both your application spans and Fluree's internal spans (`query_execute`, `query_prepare`, `query_run`, `scan`, `join`, etc.).

### Three tiers of visibility

Fluree uses a tiered logging strategy. At every tier, events and spans are correlated to your application's active span.

| Tier | `RUST_LOG` pattern | What you see from Fluree |
|------|-------------------|-------------------------|
| **Logs** | `info` (default) | Info-level log events: phase timings (`parse_ms`, `plan_ms`, `exec_ms`), commit summaries, errors. Zero span overhead. |
| **Operation spans** | `info,fluree_db_query=debug` | + `query_execute`, `query_prepare`, `query_run`, operator spans — timing waterfall in Jaeger/Tempo |
| **Deep tracing** | `info,fluree_db_query=trace` | + per-leaf, per-iteration detail (`binary_cursor_next_leaf`, `group_by`, etc.) |

At the default **INFO** level, you get Fluree's summary log events (timings, counts, errors) correlated inside your spans. This is sufficient for most production correlation needs.

At **DEBUG**, you additionally get the structured span hierarchy that produces the timing waterfall in OTEL backends. This is useful for performance investigation.

Useful `RUST_LOG` patterns:

| Pattern | Use case |
|---------|----------|
| `info` | Production: correlatable log events, zero span overhead |
| `info,fluree_db_query=debug` | Investigate slow queries |
| `info,fluree_db_transact=debug` | Investigate slow transactions |
| `info,fluree_db_query=debug,fluree_db_transact=debug` | Full operation visibility |
| `debug` | Everything, but includes third-party crate noise |

See [Telemetry and Logging](telemetry.md#span-hierarchy) for the full span hierarchy.

### Key span names and fields

These are the most useful spans and fields for application-level correlation:

| Span | Level | Key fields | When it appears |
|------|-------|-----------|-----------------|
| `query_execute` | DEBUG | `ledger_id` | Every query |
| `query_prepare` | DEBUG | `pattern_count` | Query planning phase |
| `query_run` | DEBUG | | Query execution phase |
| `transact_execute` | DEBUG | `ledger_id` | Every transaction |
| `txn_stage` | DEBUG | `insert_count`, `delete_count` | Transaction staging |
| `txn_commit` | DEBUG | `flake_count`, `delta_bytes` | Commit to storage |
| `format` | DEBUG | `output_format`, `result_count` | Result serialization |

### Adding your own context to Fluree spans

Since spans nest automatically, the simplest approach is to wrap Fluree calls with your own spans containing the context you need:

```rust
let span = tracing::info_span!(
    "api_query",
    user_id = %user_id,
    endpoint = %path,
    ledger = %ledger_alias,
);

let result = async {
    fluree.query(&db, query).await
}
.instrument(span)
.await?;
```

All of Fluree's internal spans inherit the `user_id`, `endpoint`, and `ledger` fields from the parent span in trace backends that support field inheritance.

## HTTP Server Integration (`fluree-db-server`)

When Fluree runs as a standalone HTTP server, your application connects over HTTP. Distributed trace correlation uses the [W3C Trace Context](https://www.w3.org/TR/trace-context/) standard.

### W3C `traceparent` header

When your application sends a `traceparent` header with an HTTP request, `fluree-db-server` automatically makes its `request` span a child of your trace. This requires the `otel` feature to be enabled on the server.

```
traceparent: 00-{trace-id}-{parent-span-id}-{trace-flags}
```

Example request:

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/json" \
  -H "traceparent: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" \
  -d '{"from": "my-ledger", "select": {"?s": ["*"]}, "where": [["?s", "rdf:type", "schema:Person"]]}'
```

The resulting trace in Jaeger/Tempo:

```
your-service: handle_request          ─────────────────────────────
  fluree-server: request (query:json-ld) ──────────────────────────
    query_execute                           ─────────────────────
      query_prepare                         ────
      query_run                                 ───────────────
        scan                                    ─────
        join                                         ─────────
      format                                                   ──
```

### Server requirements

W3C trace context propagation requires:

1. **`otel` feature enabled** at build time:
   ```bash
   cargo build -p fluree-db-server --features otel --release
   ```

2. **OTEL environment variables set** at runtime:
   ```bash
   OTEL_SERVICE_NAME=fluree-server \
   OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
   ./fluree-server
   ```

Without the `otel` feature, the `traceparent` header is still parsed and the trace ID is recorded as a log field for text-based correlation, but the span is not linked as a child in the OTEL trace.

For background indexing triggered by a transaction request, note the distinction between logs and traces:

- The later indexing work still runs in its own background task and appears as a separate trace/span tree.
- Fluree copies the triggering request's `request_id` and `trace_id` into the queued indexing job, so the background worker's log lines can still be correlated back to the originating request.
- If multiple requests coalesce onto one queued indexing job, the latest queued request metadata is the one retained on the worker logs.

### `X-Request-ID` header (non-OTEL correlation)

For simpler log correlation without full distributed tracing, send an `X-Request-ID` header:

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "X-Request-ID: abc-123-def-456" \
  -d '...'
```

The server logs and echoes back this ID in the response headers. All log lines for the request include the `request_id` field, so you can correlate with:

```bash
# In JSON log output:
grep '"request_id":"abc-123-def-456"' /var/log/fluree/server.log
```

This works without the `otel` feature and is useful for text-based log correlation. The same `request_id` is also copied onto background indexing logs when that request queues an index build, which helps connect the foreground transaction and later worker activity in plain log search.

### Client examples

#### Python (OpenTelemetry)

```python
from opentelemetry import trace
from opentelemetry.propagate import inject
import requests

tracer = trace.get_tracer("my-app")

with tracer.start_as_current_span("fluree_query") as span:
    headers = {"Content-Type": "application/json"}
    inject(headers)  # adds traceparent header automatically

    response = requests.post(
        "http://localhost:8090/v1/fluree/query",
        headers=headers,
        json={
            "from": "my-ledger",
            "select": {"?s": ["*"]},
            "where": [["?s", "rdf:type", "schema:Person"]],
        },
    )
```

#### JavaScript / TypeScript (OpenTelemetry)

```typescript
import { trace, context, propagation } from "@opentelemetry/api";

const tracer = trace.getTracer("my-app");

await tracer.startActiveSpan("fluree_query", async (span) => {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };
  propagation.inject(context.active(), headers);

  const response = await fetch("http://localhost:8090/v1/fluree/query", {
    method: "POST",
    headers,
    body: JSON.stringify({
      from: "my-ledger",
      select: { "?s": ["*"] },
      where: [["?s", "rdf:type", "schema:Person"]],
    }),
  });

  span.end();
  return response;
});
```

#### Rust (reqwest + tracing-opentelemetry)

```rust
use opentelemetry::global;
use opentelemetry::propagation::Injector;
use reqwest::header::HeaderMap;

struct HeaderInjector<'a>(&'a mut HeaderMap);
impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let Ok(name) = key.parse() {
            if let Ok(val) = value.parse() {
                self.0.insert(name, val);
            }
        }
    }
}

let span = tracing::info_span!("fluree_query", ledger = "my-ledger");
let _guard = span.enter();

let mut headers = HeaderMap::new();
global::get_text_map_propagator(|propagator| {
    propagator.inject(&mut HeaderInjector(&mut headers));
});

let response = reqwest::Client::new()
    .post("http://localhost:8090/v1/fluree/query")
    .headers(headers)
    .json(&query)
    .send()
    .await?;
```

## Correlation Strategy Summary

| Scenario | Mechanism | Setup required |
|----------|-----------|---------------|
| Rust app embedding `fluree-db-api` | Shared `tracing` subscriber | None -- automatic |
| Rust app embedding `fluree-db-api` with OTEL | Shared subscriber + OTEL layer | Add `OpenTelemetryLayer` to subscriber |
| HTTP client → `fluree-db-server` (OTEL) | `traceparent` header | Server built with `otel` feature + OTEL env vars |
| HTTP client → `fluree-db-server` (log only) | `X-Request-ID` header | None -- works out of the box |

## Related Documentation

- [Telemetry and Logging](telemetry.md) -- Server-side logging, OTEL export, span hierarchy
- [Adding Tracing Spans](../contributing/tracing-guide.md) -- Contributor guide for instrumenting new code
- [Performance Investigation](../troubleshooting/performance-tracing.md) -- Using traces to find bottlenecks
- [Using Fluree as a Rust Library](../getting-started/rust-api.md) -- General library usage guide
