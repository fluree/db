# Telemetry and Logging

Fluree provides comprehensive logging, metrics, and tracing capabilities for monitoring and debugging production deployments.

## Logging

### Log Levels

Configure log verbosity:

```bash
--log-level error|warn|info|debug|trace
```

**error:** Critical errors only
**warn:** Warnings and errors
**info:** Informational messages (default)
**debug:** Detailed debugging information
**trace:** Very detailed tracing

### Log Formats

#### JSON Format (Recommended)

```bash
--log-format json
```

Output:
```json
{
  "timestamp": "2024-01-22T10:30:00.123Z",
  "level": "INFO",
  "target": "fluree_db_server",
  "message": "Transaction committed",
  "fields": {
    "ledger": "mydb:main",
    "t": 42,
    "duration_ms": 45,
    "flakes_added": 3
  }
}
```

Benefits:
- Machine-parseable
- Easy to index (Elasticsearch, etc.)
- Structured fields
- JSON query tools work

#### Text Format

```bash
--log-format text
```

Output:
```text
2024-01-22T10:30:00.123Z INFO  fluree_db_server] Transaction committed ledger=mydb:main t=42 duration_ms=45
```

Benefits:
- Human-readable
- Compact
- Easy to grep

### Log Output

#### Standard Output (Default)

```bash
./fluree-db-server
```

Logs to stdout/stderr.

#### Log File

```bash
--log-file /var/log/fluree/server.log
```

```toml
[logging]
file = "/var/log/fluree/server.log"
```

#### Log Rotation

Use logrotate:

```bash
# /etc/logrotate.d/fluree
/var/log/fluree/*.log {
    daily
    rotate 14
    compress
    delaycompress
    notifempty
    create 0644 fluree fluree
    sharedscripts
    postrotate
        systemctl reload fluree
    endscript
}
```

### Structured Logging

Add context to logs:

```rust
// Rust code (for reference)
info!(
    ledger = %ledger,
    t = transaction_time,
    duration_ms = duration.as_millis(),
    "Transaction committed"
);
```

Output:
```json
{
  "message": "Transaction committed",
  "ledger": "mydb:main",
  "t": 42,
  "duration_ms": 45
}
```

## Metrics

> **Planned — not yet implemented.** The metrics below are a design target for a future PR. Prometheus metrics are not currently exposed by the server. The tracing/OTEL instrumentation described in the rest of this document is the current observability mechanism.

### Prometheus Metrics (planned)

```bash
curl http://localhost:8090/metrics
```

**Planned metrics:**
- `fluree_transactions_total` - Total transactions (counter)
- `fluree_transaction_duration_seconds` - Transaction latency (histogram)
- `fluree_queries_total` - Total queries (counter)
- `fluree_query_duration_seconds` - Query latency (histogram)
- `fluree_query_errors_total` - Query errors (counter)
- `fluree_indexing_lag_transactions` - Novelty count (gauge)
- `fluree_index_duration_seconds` - Indexing time (histogram)
- `fluree_uptime_seconds` - Server uptime (gauge)

### Prometheus Integration (planned)

Configure Prometheus to scrape Fluree:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'fluree'
    static_configs:
      - targets: ['localhost:8090']
    metrics_path: '/metrics'
    scrape_interval: 15s
```

## Distributed Tracing (OpenTelemetry)

Fluree supports OpenTelemetry (OTEL) distributed tracing, providing deep visibility into query, transaction, and indexing performance. Traces are exported to any OTLP-compatible backend (Jaeger, Grafana Tempo, AWS X-Ray, Datadog, etc.).

> **Integrating your application's traces with Fluree?** See [Distributed Tracing Integration](distributed-tracing.md) for how to correlate your spans with Fluree's -- both for the Rust library (`fluree-db-api`) and the HTTP server (`fluree-db-server` with W3C `traceparent`).

### Enabling OTEL

Build the server with the `otel` feature flag:

```bash
cargo build -p fluree-db-server --features otel --release
```

Then set environment variables to configure the OTLP exporter:

```bash
OTEL_SERVICE_NAME=fluree-server \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
OTEL_EXPORTER_OTLP_PROTOCOL=grpc \
RUST_LOG=info,fluree_db_query=debug,fluree_db_transact=debug \
./target/release/fluree-db-server --data-dir ./data
```

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `OTEL_SERVICE_NAME` | `fluree-db-server` | Service name in traces |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | OTLP receiver endpoint |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` | Protocol: `grpc` or `http/protobuf` |

### Quick Start with Jaeger

The repository includes a self-contained test harness in the `otel/` directory:

```bash
cd otel/
make all    # starts Jaeger, builds with --features otel, starts server, runs tests
make ui     # opens Jaeger UI at http://localhost:16686
```

See [Performance Investigation with Distributed Tracing](../troubleshooting/performance-tracing.md) for detailed usage.

### Dual-Layer Subscriber Architecture

The OTEL exporter uses its own `Targets` filter **independent of `RUST_LOG`**. This is a critical design choice: without it, enabling `RUST_LOG=debug` causes third-party crate spans (hyper, tonic, h2, tower-http) to flood the OTEL batch processor, which overwhelms the exporter and causes parent spans to be dropped.

```
┌──────────────────────────────────────────────────┐
│              tracing-subscriber registry          │
│                                                   │
│  ┌─────────────────────┐  ┌────────────────────┐ │
│  │   Console fmt layer  │  │   OTEL trace layer │ │
│  │   (EnvFilter from    │  │   (Targets filter: │ │
│  │    RUST_LOG)          │  │    fluree_* only)  │ │
│  └─────────────────────┘  └────────────────────┘ │
└──────────────────────────────────────────────────┘
```

- **Console layer:** Respects `RUST_LOG` as-is (all crates)
- **OTEL layer:** Exports only `fluree_*` crate targets at DEBUG level. Per-leaf-node TRACE spans (`binary_cursor_next_leaf`, `scan`) are excluded to prevent flooding the batch processor queue on large queries

This means `RUST_LOG=debug` produces verbose console output, but the OTEL exporter only receives Fluree spans -- no hyper/tonic/tower noise.

**Batch processor queue size:** The OTEL batch span processor queue is set to 1,000,000 spans. At ~200 bytes per span, this represents ~200MB of potential memory usage under sustained debug-level traffic. This is intentional to prevent span loss during investigation. At `RUST_LOG=info` without OTEL, no debug spans are created at all (true zero overhead). With OTEL enabled, the queue rarely exceeds a few thousand entries under normal operation.

### Shutdown

On server shutdown, the OTEL `SdkTracerProvider` is flushed and shut down to ensure all pending spans are exported. This is handled automatically by the server's shutdown hook.

### Dynamic Span Naming (otel.name)

Each HTTP request span is named dynamically via the `otel.name` field so that traces in Jaeger/Tempo show descriptive names instead of a generic `request`:

| Operation | otel.name examples |
|-----------|-------------------|
| Query | `query:json-ld`, `query:sparql`, `query:explain` |
| Transact | `transact:json-ld`, `transact:sparql-update`, `transact:turtle` |
| Insert | `insert:json-ld`, `insert:turtle` |
| Upsert | `upsert:json-ld`, `upsert:turtle`, `upsert:trig` |
| Ledger mgmt | `ledger:create`, `ledger:drop`, `ledger:info`, `ledger:exists` |

The `operation` span attribute retains the handler-specific name for precise filtering when needed.

### Span Hierarchy

Fluree instruments queries, transactions, and indexing with structured tracing spans at two tiers. The only `info_span!` in the codebase is `request` (the HTTP request span). All operation spans use `debug_span!`, guaranteeing true zero overhead when OTEL is not compiled and `RUST_LOG` is at `info`.

#### Tier 1: DEBUG (operation and phase level)

All operation, phase, and operator spans. Visible when OTEL is enabled or when `RUST_LOG` includes debug:

```bash
RUST_LOG=info,fluree_db_query=debug,fluree_db_transact=debug,fluree_db_indexer=debug
```

Spans: `query_execute`, `query_prepare`, `query_run`, `txn_stage`, `txn_commit`, `commit_*` sub-spans, `index_build`, `build_all_indexes`, `build_index`, `sort_blocking`, `groupby_blocking`, core operators (`scan`, `join`, `filter`, `project`, `sort`), `format`, `policy_enforce`, etc.

#### Tier 2: TRACE (maximum detail)

Per-operator detail for deep performance analysis:

```bash
RUST_LOG=info,fluree_db_query=trace
```

Additional spans: `binary_cursor_next_leaf`, `property_join`, `group_by`, `aggregate`, `group_aggregate`, `distinct`, `limit`, `offset`, `union`, `optional`, `subquery`, `having`

#### Span Tree (Query)

```
query_execute (debug)
├── query_prepare (debug)
│   ├── reasoning_prep (debug)
│   ├── pattern_rewrite (debug, patterns_before, patterns_after)
│   └── plan (debug, pattern_count)
├── query_run (debug)
│   ├── scan (debug)
│   ├── join (debug)
│   │   └── join_next_batch (debug, per iteration)
│   ├── filter (debug)
│   ├── project (debug)
│   ├── sort (debug)
│   ├── sort_blocking (debug, cross-thread via spawn_blocking)
│   └── ...
└── format (debug)
```

#### Span Tree (Transaction)

```
transact_execute (debug)
├── txn_stage (debug, insert_count, delete_count)
│   ├── where_exec (debug, pattern_count, binding_rows, retraction_count, assertion_count)
│   │   ├── delete_gen (debug, template_count, retraction_count)  ← per streaming-WHERE batch
│   │   └── insert_gen (debug, template_count, assertion_count)   ← per batch (mixed DELETE+INSERT only)
│   ├── cancellation (debug)        ← mixed DELETE+INSERT path
│   ├── dedup_retractions (debug)   ← pure-DELETE path (no INSERT templates, not Upsert)
│   └── policy_enforce (debug)
└── txn_commit (debug, flake_count, delta_bytes)
    ├── commit_nameservice_lookup (debug)
    ├── commit_verify_sequencing (debug)
    ├── commit_namespace_delta (debug)
    ├── commit_write_raw_txn (debug)  ← await of upload task spawned at pipeline entry
    ├── commit_build_record (debug)
    ├── commit_write_commit_blob (debug)
    ├── commit_publish_nameservice (debug)
    ├── commit_generate_metadata_flakes (debug)
    ├── commit_populate_dict_novelty (debug)
    └── commit_apply_to_novelty (debug)
```

#### Span Tree (Indexing)

Indexing runs as a **separate top-level trace** (not nested under an HTTP request). Each index refresh cycle starts its own trace root:

```
index_build (debug, ledger_id)
├── commit_chain_walk (debug)
├── commit_resolve (debug, per commit)
├── dict_merge_and_remap (debug)
├── build_all_indexes (debug)
│   └── build_index (debug, per order: SPOT, PSOT, POST, OPST) [cross-thread]
├── secondary_partition (debug)
├── upload_dicts (debug)
├── upload_indexes (debug)
├── build_index_root (debug)
└── BinaryIndexStore::load (debug) [cross-thread]
```

`index_gc` is a separate top-level trace (fire-and-forget `tokio::spawn`):
```
index_gc (debug, separate trace)
├── gc_walk_chain (debug)
└── gc_delete_entries (debug)
```

#### Span Tree (Bulk Import / fluree-ingest)

Bulk import runs as a **standalone top-level trace** under the `fluree-cli` service (no HTTP server involved). The import pipeline instruments all major phases:

```
bulk_import (debug, alias)
├── import_chunks (debug, total_chunks, parse_threads)
│   ├── [resolver thread: inherits parent context]
│   ├── [ttl-parser-N threads: inherit parent context]
│   └── commit + run generation log events
├── import_index_build (debug)
│   ├── build_all_indexes (debug)
│   │   └── build_index (debug, per order: SPOT, PSOT, POST, OPST) [cross-thread]
│   ├── import_cas_upload (debug)
│   └── import_publish (debug)
└── cleanup log events
```

The `import_chunks` span covers the parse+commit loop. Spawned threads (resolver, parse workers) and async tasks (dict upload, index build) inherit the parent span context so their work appears nested in the trace waterfall.

### Tracker-to-Span Bridge

When tracked queries or transactions are executed (via the `/query` or `/update` endpoints with tracking enabled), the `tracker_time` and `tracker_fuel` fields are recorded as deferred attributes on the `query_execute` and `transact_execute` spans. These values appear as span attributes in OTEL backends (Jaeger, Tempo, etc.), enabling correlation between the Tracker's fuel accounting and the span waterfall.

### RUST_LOG Quick Reference

| Goal | Pattern | What you see |
|------|---------|--------------|
| Production default | `info` | HTTP `request` spans only (zero operation spans) |
| Debug slow queries | `info,fluree_db_query=debug` | + `query_execute`, `query_prepare`, `query_run`, operators |
| Debug slow transactions | `info,fluree_db_transact=debug` | + `txn_stage`, `txn_commit`, commit sub-spans |
| Full phase decomposition | `info,fluree_db_query=debug,fluree_db_transact=debug,fluree_db_indexer=debug` | All debug spans |
| Per-operator detail | `info,fluree_db_query=trace` | + per-leaf: `binary_cursor_next_leaf`, etc. |
| Console firehose | `debug` | Everything (OTEL still filters to `fluree_*`) |

**Note:** When OTEL is enabled, the OTEL `Targets` filter always captures `fluree_*` spans at DEBUG regardless of `RUST_LOG`. The table above describes console output visibility only.

### Further Reading

- [Distributed Tracing Integration](distributed-tracing.md) -- How to correlate your application's traces with Fluree (library and HTTP)
- [Performance Investigation with Distributed Tracing](../troubleshooting/performance-tracing.md) -- How to use tracing to find bottlenecks, including AWS deployment patterns (ECS, Lambda, X-Ray, Tempo)
- [Adding Tracing Spans](../contributing/tracing-guide.md) -- How contributors should instrument new code
- [otel/ README](../../otel/README.md) -- OTEL validation harness reference

## Monitoring Integration

### Grafana Dashboards

Import Fluree dashboard:

```json
{
  "dashboard": {
    "title": "Fluree Monitoring",
    "panels": [
      {
        "title": "Query Rate",
        "targets": [
          {
            "expr": "rate(fluree_queries_total[5m])"
          }
        ]
      },
      {
        "title": "Query Latency (p95)",
        "targets": [
          {
            "expr": "histogram_quantile(0.95, fluree_query_duration_seconds)"
          }
        ]
      },
      {
        "title": "Indexing Lag",
        "targets": [
          {
            "expr": "fluree_indexing_lag_transactions"
          }
        ]
      }
    ]
  }
}
```

### Datadog Integration

Send logs to Datadog:

```bash
./fluree-db-server \
  --log-format json | \
  datadog-agent stream --service=fluree
```

### New Relic Integration

Use New Relic agent:

```bash
export NEW_RELIC_LICENSE_KEY=your-key
export NEW_RELIC_APP_NAME=fluree-prod

./fluree-db-server
```

### Elasticsearch/Kibana

Ship logs to Elasticsearch:

```bash
./fluree-db-server \
  --log-format json | \
  filebeat -e -c filebeat.yml
```

Filebeat config:
```yaml
filebeat.inputs:
  - type: stdin
    json.keys_under_root: true

output.elasticsearch:
  hosts: ["localhost:9200"]
  index: "fluree-logs-%{+yyyy.MM.dd}"
```

## Health Monitoring

### Health Check Endpoint

```bash
curl http://localhost:8090/health
```

Response (healthy):
```json
{
  "status": "healthy",
  "version": "0.1.0",
  "storage": "file",
  "uptime_ms": 3600000,
  "checks": {
    "storage": "healthy",
    "indexing": "healthy",
    "nameservice": "healthy"
  }
}
```

Response (unhealthy):
```json
{
  "status": "unhealthy",
  "checks": {
    "storage": "healthy",
    "indexing": "unhealthy",
    "nameservice": "healthy"
  },
  "errors": [
    {
      "component": "indexing",
      "message": "Indexing lag exceeds threshold"
    }
  ]
}
```

### Liveness Probe

For Kubernetes:

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8090
  initialDelaySeconds: 30
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 3
```

### Readiness Probe

```yaml
readinessProbe:
  httpGet:
    path: /ready
    port: 8090
  initialDelaySeconds: 10
  periodSeconds: 5
  timeoutSeconds: 3
```

## Alerting

### Alert Rules

Prometheus alert rules:

```yaml
groups:
  - name: fluree
    rules:
      - alert: HighQueryLatency
        expr: histogram_quantile(0.95, fluree_query_duration_seconds) > 1
        for: 5m
        annotations:
          summary: "High query latency"
          description: "95th percentile query latency is {{ $value }}s"
      
      - alert: HighIndexingLag
        expr: fluree_indexing_lag_transactions > 100
        for: 10m
        annotations:
          summary: "High indexing lag"
          description: "Indexing lag is {{ $value }} transactions"
      
      - alert: HighErrorRate
        expr: rate(fluree_query_errors_total[5m]) > 10
        for: 5m
        annotations:
          summary: "High query error rate"
          description: "Error rate is {{ $value }}/s"
```

### Alert Destinations

Configure alert routing:

```yaml
route:
  receiver: 'team-ops'
  group_by: ['alertname', 'ledger']
  routes:
    - match:
        severity: critical
      receiver: 'pagerduty'
    - match:
        severity: warning
      receiver: 'slack'

receivers:
  - name: 'pagerduty'
    pagerduty_configs:
      - service_key: 'your-key'
  
  - name: 'slack'
    slack_configs:
      - api_url: 'https://hooks.slack.com/...'
        channel: '#alerts'
```

## Performance Monitoring

### Key Metrics to Track

1. **Query Performance:**
   - p50, p95, p99 latency
   - Queries per second
   - Error rate

2. **Transaction Performance:**
   - Commit time
   - Transactions per second
   - Error rate

3. **Indexing:**
   - Novelty count
   - Index time
   - Indexing lag

4. **Resource Usage:**
   - CPU utilization
   - Memory usage
   - Disk I/O
   - Network I/O

5. **Storage:**
   - Storage used
   - Storage growth rate
   - S3 request rate (if AWS)

### Dashboards

Create operational dashboards:

**Overview Dashboard:**
- Request rate
- Error rate
- Response times
- Active connections

**Performance Dashboard:**
- Query latency percentiles
- Transaction latency
- Indexing performance
- Resource utilization

**Capacity Dashboard:**
- Storage usage and growth
- Memory usage trends
- Indexing lag trends
- Projection to capacity limits

## Logging Best Practices

### 1. Use Structured Logging

JSON format with consistent fields:

```json
{
  "timestamp": "2024-01-22T10:30:00Z",
  "level": "INFO",
  "ledger": "mydb:main",
  "operation": "query",
  "duration_ms": 45
}
```

### 2. Log Request IDs

Include request IDs for tracing:

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "X-Request-ID: abc-123-def-456" \
  -d '{...}'
```

### 3. Appropriate Log Levels

- Production: `info`
- Debugging: `debug`
- Development: `debug` or `trace`

### 4. Sample High-Volume Logs

For high-traffic deployments, sample logs:

```toml
[logging]
sample_rate = 0.1  # Log 10% of requests
```

### 5. Sensitive Data

Never log sensitive data:
- API keys
- Passwords
- Personal information
- Financial data

## Related Documentation

- [Configuration](configuration.md) - Configuration options
- [Admin and Health](admin-and-health.md) - Health monitoring
- [Troubleshooting](../troubleshooting/README.md) - Debugging guides
