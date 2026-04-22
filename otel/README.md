# OTEL Testing & Validation Infrastructure

Validate that OpenTelemetry tracing spans appear correctly in Jaeger. Provides a Makefile-driven workflow to start Jaeger, build with `--features otel`, run the server and CLI, and exercise all instrumented code paths.

## Prerequisites

- Docker (for Jaeger)
- Rust toolchain (for `cargo build`)
- curl
- bash

## Quick Start

```bash
cd otel/

# Full setup: start Jaeger, build binaries, init config, start server, run smoke tests
make all

# Open Jaeger UI to inspect traces
make ui
```

## Makefile Targets

### Infrastructure

| Target | Description |
|--------|-------------|
| `make up` | Start Jaeger via docker compose |
| `make down` | Stop Jaeger container |
| `make reset` | Restart Jaeger (clears trace data) |
| `make ui` | Open Jaeger UI in browser |
| `make build` | Build server + CLI with `--features otel` (release) |

### Project Initialization

| Target | Description |
|--------|-------------|
| `make init` | Initialize `.fluree/` project directory + apply config |
| `make config` | Apply OTEL-specific server configuration via `fluree config set` |
| `make stress-config` | Pre-configure high novelty limits (1GB) for stress testing |

### Server

| Target | Description |
|--------|-------------|
| `make server` | Start fluree-server in background with OTEL export |
| `make server-stop` | Stop the background server |
| `make server-logs` | Tail server stdout/stderr |

### Scenarios

| Target | What it exercises | Expected Jaeger spans |
|--------|-------------------|----------------------|
| `make transact` | Insert, upsert, update, Turtle, SPARQL UPDATE | `transact_execute` > `txn_stage` > `txn_commit` |
| `make query` | JSON-LD select/filter/sort, SPARQL basic/OPTIONAL/GROUP BY | `query_execute` > `query_prepare` > `query_run` > operators |
| `make index` | 500-entity burst to trigger background indexing | `index_build` > `build_all_indexes` > `build_index` |
| `make import` | Bulk import via CLI with OTEL tracing (no server) | `bulk_import` > `import_chunks` > commit spans |
| `make smoke` | Full cycle: seed + transact + query + index | End-to-end span waterfall |
| `make stress` | 50K inserts with backpressure + expensive query battery | Operator bottlenecks, `index_gc` with child spans, backpressure retries |
| `make stress-query` | Re-run only the query battery (no inserts, no server restart) | Quick iteration on query traces when stress data is already loaded |
| `make cycle` | 3x full cycle — triggers multiple index rebuilds | Sustained trace patterns |

### Data & Cleanup

| Target | Description |
|--------|-------------|
| `make generate` | Generate TTL data files for import |
| `make clean` | Remove `.fluree/` and `_data/` (all state + generated data) |
| `make clean-all` | `clean` + stop Docker |
| `make nuke` | `clean-all` + remove compiled binaries |

### Convenience

| Target | Description |
|--------|-------------|
| `make all` | `up` + `build` + `init` + `server` + `smoke` |
| `make fresh` | `reset` + `clean` + `build` + `init` + `server` + `smoke` |

## Configuration via fluree CLI

Server configuration is managed through `.fluree/config.toml` using the `fluree` CLI tool, instead of passing flags on every server start:

```bash
# Initialize project (one-time, idempotent)
make init

# View current config
fluree config list

# Change settings
fluree config set server.listen_addr "0.0.0.0:9090"
fluree config set server.indexing.reindex_max_bytes 1000000000

# The server reads .fluree/config.toml automatically
make server
```

`make init` runs `fluree init` (creates `.fluree/` with `config.toml` and `storage/`), then applies OTEL-specific settings via `fluree config set`. The server auto-discovers `config.toml` by walking up from the working directory.

### Make variable overrides

Make variables are applied to `.fluree/config.toml` via `make init`:

```bash
make server PORT=9090              # Custom port (written to config)
make smoke LEDGER=mytest:main      # Custom ledger name
make generate ENTITIES=500000      # More data for import
make server INDEXING=false         # Disable background indexing (written to config)
make server RUST_LOG=info,fluree_db_query=trace  # Custom log level (env var)
make stress STRESS_PRODUCTS=10000 STRESS_BATCH=200  # Smaller stress test
```

## RUST_LOG Patterns

| Level | Pattern | When to use |
|-------|---------|-------------|
| Default | `info` | Production; request logging only (operation spans are at debug) |
| Query debug | `info,fluree_db_query=debug` | Investigate slow queries |
| Txn debug | `info,fluree_db_transact=debug` | Investigate slow transactions |
| Full debug | `info,fluree_db_query=debug,fluree_db_transact=debug,fluree_db_indexer=debug` | Full phase decomposition (default for `make server`) |
| Operator trace | `info,fluree_db_query=trace` | Per-operator detail: scan, join, filter, sort |
| Everything | `debug` | Console firehose (OTEL layer still filters to `fluree_*`) |

## Directory Layout

```
otel/
├── Makefile                # All targets
├── README.md               # This file
├── docker-compose.yml      # Jaeger all-in-one
├── .gitignore              # Ignores .fluree/ and _data/
├── scripts/
│   ├── generate-data.sh    # TTL data generator
│   ├── wait-for-server.sh  # Health check poller
│   ├── seed-ledger.sh      # Create ledger + insert seed data
│   ├── transact-smoke.sh   # Transaction scenario
│   ├── query-smoke.sh      # Query scenario
│   ├── index-smoke.sh      # Indexing scenario
│   ├── import-smoke.sh     # Bulk import via CLI with OTEL
│   ├── stress-test.sh      # 50K inserts + backpressure + query battery
│   └── full-cycle.sh       # Combined scenario
├── .fluree/                # gitignored; created by 'make init'
│   ├── config.toml         # Server + CLI configuration
│   ├── storage/            # File-backed Fluree storage
│   ├── server.pid          # Background server PID
│   └── server.log          # Server stdout/stderr
└── _data/                  # gitignored; generated test artifacts
    └── generated/          # Generated TTL files
```

## What to Look for in Jaeger

After running scenarios, open Jaeger at `http://localhost:16686` and search for:
- **`fluree-server`** — server-side traces (query, transactions, index)
- **`fluree-cli`** — CLI import traces (bulk_import pipeline)

### Root span names (otel.name)

Traces are named via `otel.name` for easy identification in Jaeger's trace list:

| Operation | Span name examples |
|-----------|-------------------|
| Query | `query:json-ld`, `query:sparql`, `query:explain` |
| Update | `update:json-ld`, `update:sparql-update` |
| Insert | `insert:json-ld`, `insert:turtle` |
| Upsert | `upsert:json-ld`, `upsert:turtle` |
| Ledger mgmt | `ledger:create`, `ledger:drop`, `ledger:info`, `ledger:exists` |

The `operation` tag on each span retains the handler-specific name for filtering.

### Transaction traces

```
request (info, otel.name = update:json-ld)
  └─ transact_execute (debug)
       ├─ txn_stage (debug)
       │   ├─ where_exec (debug)
       │   ├─ delete_gen (debug)
       │   ├─ insert_gen (debug)
       │   ├─ cancellation (debug)
       │   └─ policy_enforce (debug)
       └─ txn_commit (debug)
            ├─ commit_nameservice_lookup (debug)
            ├─ commit_verify_sequencing (debug)
            ├─ commit_namespace_delta (debug)
            ├─ commit_write_raw_txn (debug)
            ├─ commit_build_record (debug)
            ├─ commit_write_commit_blob (debug)
            ├─ commit_publish_nameservice (debug)
            ├─ commit_generate_metadata_flakes (debug)
            ├─ commit_populate_dict_novelty (debug)
            └─ commit_apply_to_novelty (debug)
```

### Query traces

```
request (info, otel.name = query:sparql)
  └─ query_execute / sparql_execute (debug)
       ├─ query_prepare (debug)
       │   ├─ reasoning_prep (debug)
       │   ├─ pattern_rewrite (debug)
       │   └─ plan (debug)
       ├─ query_run (debug)
       │   ├─ scan (debug)
       │   ├─ join (debug)
       │   ├─ filter (debug)
       │   ├─ sort (debug)
       │   ├─ sort_blocking (debug, cross-thread)
       │   ├─ aggregate (debug)
       │   └─ project (debug)
       └─ format (debug)
```

### Index traces

```
index_build (debug, separate top-level trace)
  ├─ commit_chain_walk (debug)
  ├─ commit_resolve (debug, per commit)
  ├─ dict_merge_and_remap (debug)
  ├─ build_all_indexes (debug)
  │   └─ build_index (debug, per order: SPOT/PSOT/POST/OPST) [cross-thread]
  ├─ upload_dicts (debug)
  ├─ upload_indexes (debug)
  ├─ build_index_root (debug)
  └─ BinaryIndexStore::load (debug) [cross-thread]
```

### Import traces (CLI)

```
bulk_import (debug, service: fluree-cli)
  └─ import_chunks (debug)
       ├─ import_parse (debug, per chunk)
       └─ import_commit (debug, per chunk)
```

## Stress Test

The `make stress` target exercises high-volume insert throughput and expensive queries. It's designed to trigger multiple index cycles, backpressure retries, and generate traces with meaningful durations.

The stress target automatically pre-configures the server with a 1GB `reindex_max_bytes` via `fluree config set` before starting.

### What it does

1. **Seeds 20 categories** via a single insert
2. **Inserts 50,000 products** (configurable) in batches of 500, with exponential backoff on novelty-at-max backpressure (HTTP 400 with "Novelty at maximum size")
3. **Waits for indexing to settle**, then fires 2 additional bursts of 5,000 products each
4. **Runs 5 expensive SPARQL queries** (3 iterations each) exercising sort, join, filter, GROUP BY, and OPTIONAL (subquery Q5 is commented out pending parser support)

### Trace retention

The Jaeger instance defaults to `MEMORY_MAX_TRACES=5000`. During stress tests with 50K+ inserts, each producing a trace, older traces will be evicted. For large runs, override this in `docker-compose.yml` (e.g., `MEMORY_MAX_TRACES=50000`).

### Backpressure behavior

When the server's novelty buffer fills (default 1MB), transactions are rejected with HTTP 400. The stress script detects this and retries with exponential backoff (2s, 4s, 8s... capped at 30s). This is normal and expected -- it means indexing is working to drain the buffer.

### Configuration

```bash
make stress STRESS_PRODUCTS=10000   # Fewer products (faster)
make stress STRESS_BATCH=200        # Smaller batches
make stress STRESS_PRODUCTS=100000  # More products (triggers more index cycles)
```

### What to look for in Jaeger

- **Backpressure visibility**: Script output shows retry counts and wait times
- **index_build** traces with `gc_walk_chain` + `gc_delete_entries` child spans under `index_gc`
- **query:sparql** traces with `scan`, `join`, `filter`, `project`, `sort` operator spans under `query_run`
- **Query durations >100ms** indicating meaningful operator work on large datasets
- **Multiple index_build traces** showing sustained indexing activity

## Troubleshooting

**Server won't start:**
- Check `make server-logs` for errors
- Ensure port 8090 is free: `lsof -i :8090`
- Ensure binaries are built: `make build`

**No traces in Jaeger:**
- Verify Jaeger is running: `docker compose ps`
- Verify OTEL env vars: both server and CLI must have `OTEL_SERVICE_NAME` and `OTEL_EXPORTER_OTLP_ENDPOINT` set (handled by Makefile)
- Check that binaries were built with `--features otel`
- Traces batch-export with a slight delay; wait a few seconds after requests

**Stale server PID:**
- `make server-stop` handles stale PIDs gracefully
- Or manually: `rm .fluree/server.pid`
