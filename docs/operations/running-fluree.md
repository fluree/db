# Running & Interfacing with Fluree DB

This guide covers every way to run and interact with Fluree DB-R — from a local CLI to a distributed HTTP service to embedding it directly in a Rust application.

---

## At a Glance

| Mode | Best For | Persistence | Network Required |
|------|----------|-------------|------------------|
| **HTTP Server** | Production APIs, multi-client access | Memory / File / AWS S3 | Yes (serves HTTP) |
| **CLI** | Local dev, scripting, bulk import, replication | File (local), Remote (via push/pull) | No (local) / Yes (remotes) |
| **Rust Library** | Embedding in applications, tests | Memory / File / AWS S3 | No |
| **Offline / Embedded** | Air-gapped systems, single-process apps | Memory / File | No |

---

## 1. HTTP Server (`fluree-db-server`)

The primary production deployment. Exposes a full REST API over HTTP.

### Starting the Server

```bash
# Build from source
cargo build --release -p fluree-db-server

# Run with defaults (memory storage, port 8090)
./target/release/fluree-server

# Run with file-based persistence
fluree-server --storage-path /var/lib/fluree

# Run with debug logging
fluree-server --log-level debug
```

### Configuration

Configuration is resolved in this precedence order (highest wins):

1. **CLI flags** — `fluree-server --storage-path ./data --log-level debug`
2. **Environment variables** — All settings use a `FLUREE_` prefix (e.g., `FLUREE_LISTEN_ADDR`)
3. **Profile overrides** — Environment-specific sections in the config file
4. **Config file** — TOML, JSON, or JSON-LD (auto-discovered from `.fluree/config.toml` or `config.jsonld`)
5. **Defaults**

### Storage Backends

| Backend | Flag / Config | Use Case |
|---------|---------------|----------|
| **Memory** | (default) | Dev/testing — data lost on restart |
| **File** | `--storage-path /path` | Single-machine persistence |
| **AWS S3 + DynamoDB** | `--aws` feature + config | Distributed / cloud-native deployments |

### Server Roles

**Transaction Server** (default, `--server-role=transaction`):
- Handles reads and transactions
- Produces an event stream for replication
- Manages the nameservice (ledger metadata)

**Query Peer** (`--server-role=peer`):
- Read-only replica that subscribes to a transaction server
- Two storage access modes:
  - **Shared** — direct access to the same storage path
  - **Proxy** — fetches data through the transaction server's storage proxy API

```bash
# Transaction server (primary)
fluree-server \
  --storage-path /var/lib/fluree \
  --storage-proxy-enabled

# Query peer (shared storage)
fluree-server \
  --server-role peer \
  --tx-server-url http://primary:8090 \
  --storage-path /var/lib/fluree

# Query peer (proxy storage — no local data)
fluree-server \
  --server-role peer \
  --tx-server-url http://primary:8090 \
  --storage-access-mode proxy \
  --storage-proxy-token @/etc/fluree/proxy.jwt
```

### Key API Endpoints

**Data Operations:**
- `POST /query` — JSON-LD and SPARQL queries
- `POST /update` — Update transactions (WHERE/DELETE/INSERT JSON-LD or SPARQL UPDATE)
- `POST /insert` / `POST /upsert` — Direct insert or upsert

**Ledger Management:**
- `POST /fluree/create` — Create a new ledger
- `POST /fluree/drop` — Delete a ledger
- `GET /ledgers` — List all ledgers
- `GET /fluree/info` — Ledger metadata

**Admin:**
- `GET /health` — Health check
- `GET /status` — Server statistics
- `GET /version` — Version info
- `POST /admin/index` — Trigger manual indexing
- `POST /admin/compact` — Compact indexes

`POST /admin/index` triggers work and returns immediately. If you need to wait
for indexing in custom Rust code, use `trigger_index()` and set a timeout only
when your runtime has a hard ceiling, such as Lambda.

**Replication (Storage Proxy):**
- `GET /commits/{ledger}` — Paginated commit export
- `POST /pack/{ledger}` — Binary pack stream

See `docs/api/endpoints.md` for the complete endpoint reference.

### Authentication

Three scopes can each be set to `none`, `optional`, or `required`:

| Scope | Endpoints | Flag |
|-------|-----------|------|
| Data API | `/query`, `/update`, etc. | `--data-auth-mode` |
| Events | SSE stream | `--events-auth-mode` |
| Admin | `/fluree/create`, `/fluree/drop` | `--admin-auth-mode` |

Supported token types: **Ed25519 JWS** (`did:key` format) and **OIDC/JWKS RS256** (via `--jwks-issuer`).

---

## 2. CLI Tool (`fluree-db-cli`)

A command-line interface for local ledger management, querying, bulk import, and git-like replication.

### Installation

```bash
cargo build --release -p fluree-db-cli
# Binary: ./target/release/fluree
```

### Project Setup

```bash
fluree init              # Initialize .fluree/ directory in current project
fluree init --global     # Create global config (~/.config/fluree/)
```

### Ledger Management

```bash
fluree create mydb                          # Create a new ledger
fluree create mydb --from ./data.ttl        # Create and bulk-import Turtle data
fluree list                                 # List local ledgers
fluree info mydb                            # Show ledger metadata
fluree use mydb                             # Set active ledger (used as default)
fluree drop mydb --force                    # Delete a ledger
fluree export mydb --format turtle          # Export data as Turtle
```

### Querying

```bash
# JSON-LD query from a file
fluree query mydb query.jsonld

# SPARQL query inline
fluree query mydb --expr 'SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10' --sparql

# Output formats
fluree query mydb q.rq --format table
fluree query mydb q.rq --format csv

# Time-travel query (at commit t=5, or a specific ISO timestamp)
fluree query mydb q.rq --at 5
fluree query mydb q.rq --at 2024-06-15T00:00:00Z

# Benchmark a query
fluree query mydb q.rq --bench
```

### Transactions

```bash
fluree insert mydb data.jsonld      # Insert data from file
fluree upsert mydb data.jsonld      # Upsert data from file
fluree insert mydb --expr '{"@id": "ex:1", "ex:name": "Alice"}'  # Inline
```

### Bulk Import

The `create --from` command uses a high-throughput parallel import pipeline:

```bash
fluree create mydb --from ./large-dataset/ \
  --parallelism 8 \
  --memory-budget-mb 4096 \
  --chunk-size-mb 500
```

### History & Audit

```bash
fluree log mydb                          # Show commit log
fluree log mydb --oneline -n 20          # Brief format, last 20
fluree history ex:entity1                # Show all changes to an entity
fluree history ex:entity1 --from 1 --to 10  # Time-bounded
```

### Git-Like Replication

```bash
# Configure a remote server
fluree remote add prod https://fluree.example.com

# Clone a ledger from a remote
fluree clone prod mydb

# Pull latest changes
fluree pull mydb

# Push local changes to remote
fluree push mydb

# Fetch refs without merging
fluree fetch prod
```

### Shell Completions

```bash
fluree completions bash >> ~/.bashrc
fluree completions zsh >> ~/.zshrc
fluree completions fish > ~/.config/fish/completions/fluree.fish
```

---

## 3. Rust Library (`fluree-db-api`)

Embed Fluree directly in a Rust application with no server process required.

### Setup

```toml
[dependencies]
fluree-db-api = "0.1"              # default: file storage
# fluree-db-api = { version = "0.1", features = ["full"] }  # all features
tokio = { version = "1", features = ["full"] }
```

### Connecting

All construction goes through `FlureeBuilder`:

```rust
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;

// In-memory (no persistence) — typed
let fluree = FlureeBuilder::memory().build_memory();

// File-based persistence — typed
let fluree = FlureeBuilder::file("./data").build()?;

// AWS S3 (requires `aws` feature) — typed
let fluree = FlureeBuilder::s3("my-bucket", "https://s3.us-east-1.amazonaws.com")
    .build_s3().await?;

// From JSON-LD config — type-erased (FlureeClient)
let fluree = FlureeBuilder::from_json_ld(&json!({
    "@context": {"@base": "https://ns.flur.ee/config/connection/", "@vocab": "https://ns.flur.ee/system#"},
    "@graph": [
        {"@id": "storage", "@type": "Storage"},
        {"@id": "conn", "@type": "Connection", "indexStorage": {"@id": "storage"}, "cacheMaxMb": 2048}
    ]
}))?.build_client().await?;
```

Quick setup with typed builders:

```rust
use fluree_db_api::FlureeBuilder;

let fluree = FlureeBuilder::memory().build_memory();                               // In-memory
let fluree = FlureeBuilder::file("./data").build()?;                               // File-based
let fluree = FlureeBuilder::s3("bucket", "endpoint").build_client().await?;        // S3
```

### Creating & Querying Ledgers

```rust
// Create
fluree.create_ledger("mydb").await?;

// Insert data
fluree.graph("mydb:main")
    .transact()
    .insert(&json!({"@id": "ex:alice", "schema:name": "Alice"}))
    .commit()
    .await?;

// Query with JSON-LD
let result = fluree.graph("mydb:main")
    .query()
    .jsonld(&json!({"select": ["?name"], "where": [{"@id": "?s", "schema:name": "?name"}]}))
    .execute()
    .await?;

// Query with SPARQL
let result = fluree.graph("mydb:main")
    .query()
    .sparql("SELECT ?name WHERE { ?s <schema:name> ?name }")
    .execute()
    .await?;

// Time-travel
let result = fluree.graph_at("mydb:main", TimeSpec::AtT(42))
    .query()
    .sparql("SELECT * WHERE { ?s ?p ?o }")
    .execute()
    .await?;
```

### Feature Flags

| Flag | What It Enables |
|------|-----------------|
| `native` (default) | File storage, filesystem nameservice, moka cache |
| `credential` (default) | JWS / DID / VerifiableCredential support |
| `shacl` (default) | SHACL constraint validation |
| `iceberg` (default) | Apache Iceberg REST catalog graph sources |
| `aws` | S3 + DynamoDB storage backends |
| `ipfs` | IPFS-backed storage via Kubo HTTP RPC |
| `vector` | Embedded HNSW vector search |
| `oidc` | OIDC JWT verification via JWKS (RS256 tokens from external IdPs) |
| `swagger-ui` | Swagger UI endpoint at `/swagger-ui` |
| `otel` | OpenTelemetry tracing export |
| `full` (api only) | Convenience bundle: `native`, `credential`, `iceberg`, `shacl`, `ipfs` |

---

## 4. Offline / Embedded (No Network)

Fluree runs fully offline with zero network dependencies:

- **Memory mode** — All data in-process RAM. Ideal for tests or ephemeral workloads.
- **File mode** — Persists to a local directory. Survives restarts. No server needed.

Both the CLI and the Rust library support these modes without any network access. This makes Fluree suitable for:

- Air-gapped environments
- Edge / IoT devices
- Single-process embedded databases
- CI/CD pipelines and automated testing

```bash
# CLI: fully offline, file-persisted
fluree create mydb
fluree insert mydb data.jsonld
fluree query mydb query.rq
```

```rust
// Rust: fully offline, memory-only
let fluree = FlureeBuilder::memory().build_memory();
fluree.create_ledger("test").await?;
// ... use normally, no network calls
```

---

## 5. Helper Scripts

Located in the `scripts/` directory:

| Script | Purpose |
|--------|---------|
| `fluree_to_turtle.py` | Convert Fluree data to Turtle format |
| `split_ttl.py` | Split large Turtle files for chunked import |

---

## Summary of Data Format Support

| Format | Input | Output | Query Language |
|--------|-------|--------|----------------|
| **JSON-LD** | Insert/Upsert/Config | Query results | JSON-LD Query |
| **Turtle** (`.ttl`) | Import/Insert | Export | — |
| **TriG** (`.trig`) | Import | — | — |
| **SPARQL** (`.rq`, `.sparql`) | — | — | SELECT/CONSTRUCT/UPDATE |
| **CSV** | — | Query output | — |
| **Table** | — | Query output (CLI) | — |
