# Using Fluree as a Rust Library

This guide shows how to use Fluree programmatically in your Rust applications by depending on the `fluree-db-api` crate.

## Overview

Fluree can be embedded directly in Rust applications, giving you a powerful graph database without requiring a separate server process. This is ideal for:

- Desktop applications
- Edge computing
- Embedded systems
- Library/framework integration
- Testing and development

## Add Dependency

Add Fluree to your `Cargo.toml`:

```toml
[dependencies]
fluree-db-api = { path = "../fluree-db-api" }
tokio = { version = "1", features = ["full"] }
```

Note: Replace `path` with version when published to crates.io:
```toml
[dependencies]
fluree-db-api = "0.1"
```

### Features

Available feature flags:

- `native` (default) - File storage support
- `credential` (default in server/CLI) - DID/JWS/VerifiableCredential support for signed queries and transactions
- `shacl` (default in server/CLI) - SHACL constraint validation
- `iceberg` (default in server/CLI) - Apache Iceberg/R2RML graph source support
- `aws` - AWS-backed storage support (S3, storage-backed nameservice). Enables `FlureeBuilder::s3()` and S3-based JSON-LD configs.
- `ipfs` - IPFS-backed storage via Kubo HTTP RPC
- `vector` - Embedded vector similarity search (HNSW indexes via usearch)
- `search-remote-client` - Remote search service client (HTTP client for remote BM25 and vector search services)
- `aws-testcontainers` - Opt-in LocalStack-backed S3/DynamoDB tests (auto-start via testcontainers)
- `full` - Convenience bundle: `native`, `credential`, `iceberg`, `shacl`, `ipfs`

## Quick Start

### Basic Setup

```rust
use fluree_db_api::{FlureeBuilder, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Create a memory-backed Fluree instance
    let fluree = FlureeBuilder::memory().build_memory();

    // Create a new ledger
    let ledger = fluree.create_ledger("mydb").await?;

    println!("Ledger created at t={}", ledger.t());

    Ok(())
}
```

### With File Storage

```rust
use fluree_db_api::{FlureeBuilder, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Use file-backed storage for persistence
    let fluree = FlureeBuilder::file("./data").build()?;

    // Create a new ledger (or load an existing one)
    let ledger = fluree.create_ledger("mydb").await?;

    // Load an existing ledger by ID (`name:branch`)
    let ledger = fluree.ledger("mydb:main").await?;

    Ok(())
}
```

### Bulk import (high throughput)

For initial ledger bootstraps (large Turtle or JSON-LD datasets), Fluree exposes a bulk import
pipeline as a first-class Rust API:

```rust
use fluree_db_api::{FlureeBuilder, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::file("./data").build()?;

    // `chunks_dir` can be:
    // - a directory containing *.ttl, *.trig, or *.jsonld files (sorted lexicographically), OR
    // - a single .ttl or .jsonld file.
    // Directories must contain a single format (no mixing Turtle and JSON-LD).
    let result = fluree
        .create("dblp:main")
        .import("./chunks_dir")
        .threads(8)          // parallel TTL parsing; commits remain serial
        .build_index(true)   // write an index root and publish it
        .publish_every(50)   // nameservice checkpoints during long imports (0 disables)
        .cleanup(true)       // delete tmp import files on success
        .execute()
        .await?;

    println!(
        "import complete: t={}, flakes={}, root={:?}",
        result.t, result.flake_count, result.root_id
    );

    // Query normally after import (loads the published V2 root from CAS).
    let view = fluree.view("dblp:main").await?;
    let qr = fluree
        .query(&view, "SELECT * WHERE { ?s ?p ?o } LIMIT 10")
        .await?;
    println!("rows={}", qr.batches.iter().map(|b| b.len()).sum::<usize>());

    Ok(())
}
```

**Temporary files:** the bulk import pipeline uses a session-scoped `tmp_import/` directory and
removes it only on full success (unless `.cleanup(false)` is set). On failure, it keeps the
session directory and logs its path for debugging.

### With S3 Storage

Requires `fluree-db-api` feature `aws` and standard AWS credential/region configuration.

```rust
use fluree_db_api::{FlureeBuilder, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // LocalStack/MinIO: endpoint is required
    let fluree = FlureeBuilder::s3("my-bucket", "http://localhost:4566")
        .build_client()
        .await?;

    let ledger = fluree.create_ledger("mydb").await?;
    println!("Ledger created at t={}", ledger.t());
    Ok(())
}
```

**S3 Express One Zone note:** for directory buckets (`--x-s3` suffix), omit `s3Endpoint` in JSON-LD config and let the SDK handle it.

## Connection Configuration (JSON-LD)

For advanced configuration (tiered storage, address identifier routing, DynamoDB nameservice,
environment variable indirection), use `FlureeBuilder::from_json_ld()` to parse a JSON-LD config
and build from it. The typed builder methods (`build()`, `build_memory()`, `build_s3()`) and
the type-erased `build_client()` all share the same underlying construction logic.

See also: [JSON-LD connection configuration reference](../reference/connection-config-jsonld.md).

```rust
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = json!({
        "@context": {"@base": "https://ns.flur.ee/config/connection/", "@vocab": "https://ns.flur.ee/system#"},
        "@graph": [
            {"@id": "s3Index", "@type": "Storage", "s3Bucket": {"envVar": "INDEX_BUCKET"}, "s3Endpoint": {"envVar": "S3_ENDPOINT"}},
            {"@id": "conn", "@type": "Connection", "indexStorage": {"@id": "s3Index"}}
        ]
    });
    // from_json_ld parses the config into builder settings; build_client() constructs
    // a type-erased FlureeClient suitable for runtime-determined backends.
    let fluree = FlureeBuilder::from_json_ld(&cfg)?.build_client().await?;
    Ok(())
}
```

### Environment variables (`ConfigurationValue`)

Any string/number config value can be specified directly or via a `ConfigurationValue` object:

```json
{
  "s3Bucket": { "envVar": "FLUREE_S3_BUCKET", "defaultVal": "my-bucket" },
  "cacheMaxMb": { "envVar": "FLUREE_CACHE_MAX_MB", "defaultVal": "1024" }
}
```

### Supported JSON-LD fields (Rust)

Connection node:
- `parallelism`
- `cacheMaxMb`
- `indexStorage`, `commitStorage`
- `primaryPublisher` (publisher node)

Storage node:
- File: `filePath`, `AES256Key`
- S3: `s3Bucket`, `s3Prefix`, `s3Endpoint`, `s3ReadTimeoutMs`, `s3WriteTimeoutMs`, `s3ListTimeoutMs`, `s3MaxRetries`, `s3RetryBaseDelayMs`, `s3RetryMaxDelayMs`

Publisher node:
- DynamoDB nameservice: `dynamodbTable`, `dynamodbRegion`, `dynamodbEndpoint`, `dynamodbTimeoutMs`
- Storage-backed nameservice: `storage` (reference to a Storage node)

## Core Patterns

### The Graph API

The primary API revolves around `fluree.graph(graph_ref)`, which returns a lazy `Graph` handle.
No I/O occurs until a terminal method (`.execute()`, `.commit()`, `.load()`) is called.

Use `graph(...).query()` when the target may be a mapped graph source as well as a native ledger. If the query body itself carries `"from"` / `FROM`, use `query_from()`. The lower-level `fluree.db(...)` + `fluree.query(&view, ...)` path is for materialized native ledger snapshots, not graph source aliases.

**When I/O happens:**
- `.execute()` / `.execute_formatted()` / `.execute_tracked()` — loads the graph from storage, then runs the query (each call reloads)
- `.commit()` — loads the cached ledger handle, stages, and commits
- `.stage()` — loads the ledger and stages without committing
- `.load()` — loads the graph once, returning a `GraphSnapshot` for repeated queries without reloading

```rust
// Lazy query — loads graph and executes in one step
let result = fluree.graph("mydb:main")
    .query()
    .sparql("SELECT ?name WHERE { ?s <http://schema.org/name> ?name }")
    .execute()
    .await?;

// Lazy transact + commit
let out = fluree.graph("mydb:main")
    .transact()
    .insert(&data)
    .commit()
    .await?;

// Materialize for reuse (avoids reloading on each query)
let db = fluree.graph("mydb:main").load().await?;
let r1 = db.query().sparql("SELECT ...").execute().await?;
let r2 = db.query().jsonld(&q).execute().await?;

// Time travel
let result = fluree.graph_at("mydb:main", TimeSpec::AtT(42))
    .query()
    .jsonld(&q)
    .execute()
    .await?;
```

### Insert Data

```rust
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await?;

    // Insert JSON-LD data using the Graph API
    let data = json!({
        "@context": {
            "schema": "http://schema.org/",
            "ex": "http://example.org/ns/"
        },
        "@graph": [
            {
                "@id": "ex:alice",
                "@type": "schema:Person",
                "schema:name": "Alice",
                "schema:email": "alice@example.org",
                "schema:age": 30
            },
            {
                "@id": "ex:bob",
                "@type": "schema:Person",
                "schema:name": "Bob",
                "schema:email": "bob@example.org",
                "schema:age": 25
            }
        ]
    });

    let result = fluree.graph("mydb:main")
        .transact()
        .insert(&data)
        .commit()
        .await?;

    println!("Transaction committed");

    Ok(())
}
```

### Query Data with JSON-LD Query

```rust
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await?;

    // Insert test data first (see Insert Data above)
    // ...

    // Query with JSON-LD using the lazy Graph API
    let query = json!({
        "select": ["?name", "?email"],
        "where": [
            { "@id": "?person", "@type": "schema:Person" },
            { "@id": "?person", "schema:name": "?name" },
            { "@id": "?person", "schema:email": "?email" },
            { "@id": "?person", "schema:age": "?age" }
        ],
        "filter": "?age > 25"
    });

    let result = fluree.graph("mydb:main")
        .query()
        .jsonld(&query)
        .execute_formatted()
        .await?;

    println!("Query results: {}",
        serde_json::to_string_pretty(&result)?);

    Ok(())
}
```

### Query Data with SPARQL

```rust
use fluree_db_api::{FlureeBuilder, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await?;

    // Insert test data first (see Insert Data above)
    // ...

    // Query with SPARQL using the lazy Graph API
    let sparql = r#"
        PREFIX schema: <http://schema.org/>

        SELECT ?name ?email
        WHERE {
            ?person a schema:Person .
            ?person schema:name ?name .
            ?person schema:email ?email .
            ?person schema:age ?age .
            FILTER (?age > 25)
        }
        ORDER BY ?name
    "#;

    let result = fluree.graph("mydb:main")
        .query()
        .sparql(sparql)
        .execute_formatted()
        .await?;

    println!("Results: {}",
        serde_json::to_string_pretty(&result)?);

    Ok(())
}
```

### Update Data

```rust
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await?;

    // Update using WHERE/DELETE/INSERT pattern
    let update = json!({
        "@context": { "schema": "http://schema.org/" },
        "where": [
            { "@id": "?person", "schema:name": "Alice" },
            { "@id": "?person", "schema:age": "?oldAge" }
        ],
        "delete": [
            { "@id": "?person", "schema:age": "?oldAge" }
        ],
        "insert": [
            { "@id": "?person", "schema:age": 31 }
        ]
    });

    let result = fluree.graph("mydb:main")
        .transact()
        .update(&update)
        .commit()
        .await?;

    println!("Updated successfully");

    Ok(())
}
```

### SPARQL UPDATE

Use SPARQL UPDATE syntax for transactions:

```rust
use fluree_db_api::{
    FlureeBuilder, Result,
    parse_sparql, lower_sparql_update, NamespaceRegistry, TxnOpts,
    SparqlQueryBody,
};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::file("./data").build()?;

    // Get a cached ledger handle
    let handle = fluree.ledger_cached("mydb:main").await?;

    // SPARQL UPDATE string
    let sparql = r#"
        PREFIX ex: <http://example.org/ns/>

        DELETE {
            ?person ex:age ?oldAge .
        }
        INSERT {
            ?person ex:age 31 .
        }
        WHERE {
            ?person ex:name "Alice" .
            ?person ex:age ?oldAge .
        }
    "#;

    // Parse SPARQL
    let parse_output = parse_sparql(sparql);
    if parse_output.has_errors() {
        // Handle parse errors
        for diag in parse_output.diagnostics.iter().filter(|d| d.is_error()) {
            eprintln!("Parse error: {}", diag.message);
        }
        return Err(fluree_db_api::ApiError::Internal("SPARQL parse error".into()));
    }

    let ast = parse_output.ast.unwrap();

    // Extract the UPDATE operation
    let update_op = match &ast.body {
        SparqlQueryBody::Update(op) => op,
        _ => return Err(fluree_db_api::ApiError::Internal("Expected SPARQL UPDATE".into())),
    };

    // Get namespace registry from the ledger
    let snapshot = handle.snapshot().await;
    let mut ns = NamespaceRegistry::from_db(&snapshot.snapshot);

    // Lower SPARQL UPDATE to Txn IR
    let txn = lower_sparql_update(update_op, &ast.prologue, &mut ns, TxnOpts::default())?;

    // Execute the transaction
    let result = fluree.stage(&handle)
        .txn(txn)
        .execute()
        .await?;

    println!("SPARQL UPDATE committed at t={}", result.receipt.t);

    Ok(())
}
```

**Supported SPARQL UPDATE operations:**
- `INSERT DATA` - Insert ground triples
- `DELETE DATA` - Delete specific triples
- `DELETE WHERE` - Delete matching patterns
- `DELETE/INSERT WHERE` - Full update with patterns

See [SPARQL UPDATE](../query/sparql.md#sparql-update) for syntax details.

### Stage and Preview Changes

```rust
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await?;

    let data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });

    // Stage without committing
    let staged = fluree.graph("mydb:main")
        .transact()
        .insert(&data)
        .stage()
        .await?;

    // Query the staged state to preview changes
    let preview_query = json!({
        "select": ["?name"],
        "where": [{"@id": "ex:alice", "ex:name": "?name"}]
    });

    let preview = staged.query()
        .jsonld(&preview_query)
        .execute()
        .await?;

    println!("Preview: {} rows", preview.row_count());

    Ok(())
}
```

**Note:** `StagedGraph` currently supports querying only. Staging on top of a staged transaction and committing from a `StagedGraph` are not yet supported.

### Export Data

Stream ledger data as Turtle, N-Triples, N-Quads, TriG, or JSON-LD using the builder API:

```rust
use fluree_db_api::{FlureeBuilder, Result};
use fluree_db_api::export::ExportFormat;
use std::io::BufWriter;
use std::fs::File;

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::file("./data").build()?;

    // Export as Turtle to a file
    let file = File::create("backup.ttl").unwrap();
    let mut writer = BufWriter::new(file);
    let stats = fluree.export("mydb")
        .format(ExportFormat::Turtle)
        .write_to(&mut writer)
        .await?;
    println!("Exported {} triples", stats.triples_written);

    // Export as JSON-LD with custom prefixes
    let mut buf = Vec::new();
    let stats = fluree.export("mydb")
        .format(ExportFormat::JsonLd)
        .context(&serde_json::json!({"ex": "http://example.org/"}))
        .write_to(&mut buf)
        .await?;

    // Export all graphs as N-Quads (dataset export)
    let stats = fluree.export("mydb")
        .format(ExportFormat::NQuads)
        .all_graphs()
        .to_stdout()
        .await?;

    Ok(())
}
```

All formats stream directly from the binary SPOT index. Memory usage is O(leaflet size) for line-oriented formats and O(largest subject) for JSON-LD, regardless of dataset size.

**Builder methods:**
- `.format(ExportFormat)` — output format (default: Turtle)
- `.all_graphs()` — include all named graphs including system graphs (requires TriG or NQuads)
- `.graph("iri")` — export a specific named graph by IRI
- `.as_of(TimeSpec)` — time-travel export (transaction number, ISO-8601 datetime, or commit CID prefix)
- `.context(&json)` — override prefix map (default: ledger's context from nameservice)
- `.write_to(&mut writer)` — stream to any `Write` sink
- `.to_stdout()` — convenience for stdout output

See also: [CLI export](../cli/export.md) for command-line usage.

### Materialize for Reuse

When you need to run multiple queries against the same snapshot, materialize a `GraphSnapshot` once:

```rust
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();

    // Load once, query many times
    let db = fluree.graph("mydb:main").load().await?;

    let r1 = db.query()
        .sparql("SELECT ?name WHERE { ?s <http://schema.org/name> ?name }")
        .execute()
        .await?;

    let q2 = json!({
        "select": ["?email"],
        "where": [{"@id": "?s", "schema:email": "?email"}]
    });
    let r2 = db.query()
        .jsonld(&q2)
        .execute()
        .await?;

    // Access the underlying view if needed
    let view = db.view();

    Ok(())
}
```

## Advanced Usage

### Ledger Caching

Ledger caching is enabled by default on all `FlureeBuilder` constructors. When caching is active, `fluree.ledger()` returns a cached handle and subsequent calls avoid reloading from storage:

```rust
use fluree_db_api::{FlureeBuilder, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Caching is on by default — no extra call needed
    let fluree = FlureeBuilder::file("./data").build()?;

    // First call loads from storage
    let ledger = fluree.ledger("mydb:main").await?;

    // Subsequent calls return cached state (fast)
    let ledger2 = fluree.ledger("mydb:main").await?;

    Ok(())
}
```

To **disable** caching (e.g., for a CLI tool that runs once and exits):

```rust
let fluree = FlureeBuilder::file("./data")
    .without_ledger_caching()
    .build()?;
```

#### Disconnecting Ledgers

Use `disconnect_ledger` to release a ledger from the connection cache. This forces a fresh load on the next access:

```rust
use fluree_db_api::{FlureeBuilder, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::file("./data").build()?;

    // Load and use ledger
    let ledger = fluree.ledger("mydb:main").await?;
    println!("Ledger at t={}", ledger.t());

    // Release cached state
    fluree.disconnect_ledger("mydb:main").await;

    // Next access will reload from storage
    let ledger = fluree.ledger("mydb:main").await?;

    Ok(())
}
```

**When to use `disconnect_ledger`:**

- **Force fresh load**: After external changes to the ledger (e.g., another process wrote data)
- **Free memory**: Release memory for ledgers you no longer need
- **Clean shutdown**: Release resources before application exit
- **Testing**: Reset state between test cases

**Note:** If caching is disabled (via `without_ledger_caching()` on builder), `disconnect_ledger` is a no-op.

#### Checking Ledger Existence

Use `ledger_exists` to check if a ledger is registered in the nameservice without loading it:

```rust
use fluree_db_api::{FlureeBuilder, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::file("./data").build()?;

    // Check if ledger exists (lightweight nameservice lookup)
    if fluree.ledger_exists("mydb:main").await? {
        // Ledger exists - load it
        let ledger = fluree.ledger("mydb:main").await?;
        println!("Loaded ledger at t={}", ledger.t());
    } else {
        // Ledger doesn't exist - create it
        let ledger = fluree.create_ledger("mydb").await?;
        println!("Created new ledger");
    }

    Ok(())
}
```

**When to use `ledger_exists`:**

- **Conditional create-or-load**: Check before deciding whether to create or load
- **Validation**: Verify ledger IDs exist before operations
- **Defensive programming**: Avoid `NotFound` errors in application logic

**Performance note:** This is a lightweight check that only queries the nameservice - it does NOT load the ledger data, indexes, or novelty. Much faster than attempting to load and catching `NotFound` errors.

#### Dropping Ledgers

Use `drop_ledger` to retract a ledger or to permanently remove its managed storage artifacts:

```rust
use fluree_db_api::{FlureeBuilder, DropMode, DropStatus, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::file("./data").build()?;

    // Soft drop: retract from nameservice, preserve storage artifacts
    let report = fluree.drop_ledger("mydb:main", DropMode::Soft).await?;
    match report.status {
        DropStatus::Dropped => println!("Ledger dropped"),
        DropStatus::AlreadyRetracted => println!("Already dropped"),
        DropStatus::NotFound => println!("Ledger not found"),
    }

    // Hard drop: delete managed storage artifacts (IRREVERSIBLE)
    let report = fluree.drop_ledger("mydb:main", DropMode::Hard).await?;
    println!("Deleted {} storage artifacts", report.artifacts_deleted);

    Ok(())
}
```

**Drop Modes:**

| Mode | Behavior | Reversible |
|------|----------|------------|
| `DropMode::Soft` (default) | Marks the ledger retracted in the nameservice; artifacts remain and the alias stays reserved | Partially; requires administrative recovery |
| `DropMode::Hard` | Deletes managed storage artifacts and purges the nameservice record where supported | **No** for deleted artifacts |

**Drop Sequence:**

1. Normalizes the ledger ID (ensures `:main` suffix)
2. Cancels any pending background indexing
3. Waits for in-progress indexing to complete
4. In hard mode: deletes managed storage artifacts (commits, txns, indexes, config/context blobs, and related content)
5. In soft mode: retracts from nameservice; in hard mode: purges the nameservice record where supported
6. Disconnects from ledger cache (if caching enabled)

**When to use `drop_ledger`:**

- **Cleanup**: Remove test ledgers or unused data
- **Data lifecycle**: Permanently delete ledgers that are no longer needed
- **Admin operations**: Clean up after migrations or failures

**Idempotency:**

Safe to call multiple times:
- Returns `DropStatus::AlreadyRetracted` if previously dropped
- Hard mode still attempts deletion for `NotFound`/`AlreadyRetracted` (useful for admin cleanup)

**Warnings:**

The `DropReport` includes a `warnings` field for any non-fatal errors encountered during the operation (e.g., failed to delete a specific file). Always check this for hard drops:

```rust
let report = fluree.drop_ledger("mydb:main", DropMode::Hard).await?;
if !report.warnings.is_empty() {
    for warning in &report.warnings {
        eprintln!("Warning: {}", warning);
    }
}
```

#### Refreshing Cached Ledgers

Use `refresh` to poll-check whether a cached ledger is stale and update it if needed.
`refresh` returns a `RefreshResult` containing the ledger's `t` after the operation
and what action was taken:

```rust
use fluree_db_api::{FlureeBuilder, NotifyResult, RefreshOpts, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::file("./data").build()?;

    // Load ledger into cache
    let _ledger = fluree.ledger_cached("mydb:main").await?;

    // Later, check if the cached state is still fresh
    match fluree.refresh("mydb:main", Default::default()).await? {
        Some(r) => {
            println!("Ledger at t={}, action: {:?}", r.t, r.action);
            match r.action {
                NotifyResult::Current => println!("Already up to date"),
                NotifyResult::Reloaded => println!("Reloaded from storage"),
                NotifyResult::IndexUpdated => println!("Index was updated"),
                NotifyResult::CommitsApplied { count } => {
                    println!("{count} commits applied incrementally");
                }
                NotifyResult::NotLoaded => println!("Not in cache"),
            }
        }
        None => println!("Ledger not found in nameservice"),
    }

    Ok(())
}
```

**Key behaviors:**

- **Does NOT cold-load**: If the ledger isn't already cached, returns `NotLoaded` (no-op)
- **Returns `None`**: If the ledger doesn't exist in the nameservice
- **Alias resolution**: Supports short aliases (`mydb` resolves to `mydb:main`)
- **No-op without caching**: If caching is disabled, returns `NotLoaded`
- **Returns `t`**: The `RefreshResult.t` field always tells you the ledger's current transaction time

**When to use `refresh`:**

- **Poll-based freshness**: When you can't use SSE events but need periodic freshness checks
- **Before critical reads**: Ensure you have the latest state before important queries
- **Peer mode**: Check if the local cache is behind the transaction server

**`refresh` vs `disconnect_ledger`:**

| Behavior | `refresh` | `disconnect_ledger` |
|----------|-----------|---------------------|
| Checks freshness | Yes | No |
| Updates in place | Yes | No (forces full reload on next access) |
| Handles not-cached | Returns `NotLoaded` | No-op |
| Use case | Poll-based updates | Force full reload |

#### Read-After-Write Consistency

Fluree's query engine is **eventually consistent**: when one process writes data and
another (or the same process on a warm cache) queries it, the query may not yet see
the latest commit. The `t` value returned from a transaction is the key to bridging
this gap.

Pass `RefreshOpts { min_t: Some(t) }` to `refresh()` to assert that the cached
ledger has reached at least that transaction time. If it hasn't after pulling the
latest state from the nameservice, `refresh` returns `ApiError::AwaitTNotReached`
with both the `requested` and `current` `t` values. Your code owns retry timing
and timeout policy.

**Basic usage:**

```rust
use fluree_db_api::{FlureeBuilder, RefreshOpts, ApiError, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::file("./data").build()?;
    let handle = fluree.ledger_cached("mydb:main").await?;

    // Transaction returns the commit's t value
    let receipt = fluree.stage(&handle)
        .insert(&json!({"@id": "ex:item", "ex:count": 42}))
        .commit()
        .await?;
    let committed_t = receipt.t;

    // Ensure the cache reflects at least this t before querying
    let opts = RefreshOpts { min_t: Some(committed_t) };
    let result = fluree.refresh("mydb:main", opts).await?;
    // result.unwrap().t >= committed_t is guaranteed here

    Ok(())
}
```

**Serverless / Lambda pattern (retry with backoff):**

In a serverless environment, the transacting process and the querying process may be
different Lambda invocations. The querying invocation receives `t` (e.g., via an
event payload or API parameter) and must wait for that commit to be visible:

```rust
use fluree_db_api::{RefreshOpts, ApiError};
use std::time::{Duration, Instant};

async fn wait_for_t(
    fluree: &Fluree<impl Storage, impl NameService>,
    ledger_id: &str,
    min_t: i64,
    timeout: Duration,
) -> Result<i64, ApiError> {
    let deadline = Instant::now() + timeout;
    let opts = RefreshOpts { min_t: Some(min_t) };

    loop {
        match fluree.refresh(ledger_id, opts.clone()).await {
            Ok(Some(r)) => return Ok(r.t),   // reached min_t
            Ok(None) => return Err(ApiError::NotFound(
                format!("ledger {ledger_id} not in nameservice"),
            )),
            Err(ApiError::AwaitTNotReached { current, .. }) => {
                if Instant::now() >= deadline {
                    return Err(ApiError::AwaitTNotReached {
                        requested: min_t,
                        current,
                    });
                }
                // Back off before retrying
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => return Err(e),
        }
    }
}
```

**How it works internally:**

1. **Fast path**: If the cached `t` already satisfies `min_t`, returns immediately
   without hitting the nameservice at all.
2. **Pull**: Queries the nameservice for the latest commit/index pointers and applies
   any new commits incrementally (or reloads if the gap is large).
3. **Check**: If `t` is still below `min_t` after the pull, returns
   `ApiError::AwaitTNotReached` so you can retry.

This design keeps retry/timeout policy out of the database layer. Different
deployment contexts (Lambda with 100ms backoff, HTTP handler with 5s deadline,
integration test with immediate assertion) each wrap the same primitive differently.

### Branch Diff (Merge Preview)

`Fluree::merge_preview` returns the rich diff between two branches —
ahead/behind commit summaries, the common ancestor, conflict keys, and
fast-forward eligibility — **without mutating any state**. It uses the
same primitives as `merge_branch` but skips the publish/copy steps,
making it cheap enough to call on every UI render.

```rust
use fluree_db_api::{FlureeBuilder, MergePreviewOpts, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();

    // ... create ledger, branch, transact on dev, etc.

    // Default: previewing dev → main with the spec defaults
    // (cap each commit list at 500, conflict keys at 200, run conflicts).
    let preview = fluree.merge_preview("mydb", "dev", None).await?;

    println!(
        "{} ahead, {} behind, fast-forward: {}",
        preview.ahead.count, preview.behind.count, preview.fast_forward,
    );

    if preview.fast_forward {
        println!("merge would advance {} → {}", preview.source, preview.target);
    } else {
        println!("merge has {} conflict(s)", preview.conflicts.count);
        for k in &preview.conflicts.keys {
            println!("  - s={} p={}", k.s, k.p);
        }
    }
    Ok(())
}
```

#### Tuning the preview

`merge_preview_with` takes a `MergePreviewOpts` for callers that need
control over response size or want to skip the conflict computation:

```rust
use fluree_db_api::{FlureeBuilder, MergePreviewOpts, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();

    // Cheap preview: counts only, no conflict walks.
    let counts = fluree
        .merge_preview_with(
            "mydb",
            "dev",
            Some("main"),
            MergePreviewOpts {
                max_commits: Some(0),       // counts only — no commit summaries
                max_conflict_keys: Some(0),
                include_conflicts: false,
            },
        )
        .await?;

    // Direct Rust callers can opt in to **unbounded** results — useful for
    // tooling that needs the full divergence. The HTTP layer always supplies
    // a bound, so this is a Rust-only escape hatch.
    let full = fluree
        .merge_preview_with(
            "mydb",
            "dev",
            None,
            MergePreviewOpts {
                max_commits: None,
                max_conflict_keys: None,
                include_conflicts: true,
            },
        )
        .await?;

    Ok(())
}
```

#### What the caps do (and don't) control

`max_commits` and `max_conflict_keys` cap the **size of the returned
lists**, not the cost of computing them:

- `BranchDelta::count` on each side reflects the full unbounded
  divergence — computed by walking every commit envelope between HEAD and
  the common ancestor — regardless of `max_commits`.
- When `include_conflicts: true`, both `compute_delta_keys` walks scan
  the full per-side delta regardless of `max_conflict_keys`.
- When `include_conflict_details: true`, value details are collected only
  for the returned `conflicts.keys` after the `max_conflict_keys` cap is
  applied.
- Set `include_conflicts: false` for a cheap preview on heavily diverged
  branches; you still get accurate `ahead.count` / `behind.count`.

#### Response shape

| Type | Notable fields |
|------|----------------|
| `MergePreview` | `source`, `target`, `ancestor: Option<AncestorRef>`, `ahead`, `behind`, `fast_forward`, `conflicts`, `mergeable` |
| `BranchDelta` | `count` (unbounded), `commits: Vec<CommitSummary>` (newest-first, capped), `truncated` |
| `CommitSummary` | `t`, `commit_id`, `time`, `asserts`, `retracts`, `flake_count`, `message: Option<String>` (extracted from the `f:message` `txn_meta` entry when present) |
| `ConflictSummary` | `count` (unbounded), `keys: Vec<ConflictKey>` (sorted, capped), `truncated`, `strategy`, `details` |
| `ConflictDetail` | `key`, `source_values`, `target_values`, `resolution` (values are the current asserted values at each branch HEAD) |
| `ConflictKey` | `s: Sid`, `p: Sid`, `g: Option<Sid>` |

`mergeable` only reflects whether the selected strategy would abort due to
detected conflicts; it is not full validation of every constraint the eventual
merge commit may encounter. `mergeable=true` does not guarantee a subsequent
merge will succeed; it only reflects the conflict/strategy interaction at
preview time.

All types derive `Serialize` so the response is wire-stable; the HTTP
endpoint at `GET /v1/fluree/merge-preview/{ledger...}` returns the same struct.
See `docs/api/endpoints.md` and `docs/cli/server-integration.md` for the
HTTP contract.

#### Reusable primitives in `fluree-db-core`

The per-commit summary types and DAG walker are factored into core for
reuse outside the merge-preview flow (e.g., git-log-style commit history
viewers, indexer integration). Re-exported from `fluree-db-api`:

- `walk_commit_summaries(store, head, stop_at_t, max) -> Result<(Vec<CommitSummary>, usize)>`
  — newest-first walk that returns both the (capped) summary list and the
  unbounded total count.
- `commit_to_summary(commit) -> CommitSummary` — pure function, no I/O.
- `find_common_ancestor(store, head_a, head_b)` — dual-frontier BFS.

### Time Travel Queries

```rust
use fluree_db_api::{FlureeBuilder, TimeSpec, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();

    // Query at a specific point in time
    let result = fluree.graph_at("mydb:main", TimeSpec::AtT(100))
        .query()
        .sparql("SELECT * WHERE { ?s ?p ?o } LIMIT 10")
        .execute()
        .await?;

    println!("Results at t=100: {:?}", result.row_count());

    Ok(())
}
```

### Multi-Ledger Queries

```rust
use fluree_db_api::{FlureeBuilder, DataSetDb, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();

    // Load views from multiple ledgers
    let customers = fluree.view("customers:main").await?;
    let orders = fluree.view("orders:main").await?;

    // Compose a dataset from multiple graphs
    let dataset = DataSetDb::new()
        .with_default(customers)
        .with_named("orders:main", orders);

    // Query across ledgers using the dataset builder
    let query = r#"
        SELECT ?customerName ?orderTotal
        WHERE {
            ?customer schema:name ?customerName .
            ?customer ex:customerId ?cid .

            GRAPH <orders:main> {
                ?order ex:customerId ?cid .
                ?order ex:total ?orderTotal .
            }
        }
    "#;

    let result = dataset.query(&fluree)
        .sparql(query)
        .execute()
        .await?;

    Ok(())
}
```

### Remote Federation

Query ledgers on remote Fluree servers using SPARQL `SERVICE` with the `fluree:remote:` scheme. Register remote connections at build time — each maps a name to a server URL and optional bearer token:

```rust
use fluree_db_api::{FlureeBuilder, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::file("./data")
        .remote_connection(
            "acme",
            "https://acme-fluree.example.com",
            Some("eyJhbG...".to_string()),
        )
        .build()?;

    let db = fluree.view("local-ledger:main").await?;

    // Join local data with a ledger on the remote server
    let result = fluree.query(&db, r#"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?name ?email
        WHERE {
          ?person ex:name ?name .
          SERVICE <fluree:remote:acme/customers:main> {
            ?person ex:email ?email .
          }
        }
    "#).await?;

    Ok(())
}
```

The connection name (`acme`) maps to the server URL. The ledger path (`customers:main`) is appended to form the request URL: `POST https://acme-fluree.example.com/v1/fluree/query/customers:main`. The bearer token is sent as `Authorization: Bearer <token>` on every request.

Multiple ledgers on the same remote server use the same connection name — you register the server once and can query any ledger your token is authorized for.

See [Configuration: Remote connections](../operations/configuration.md#remote-connections) for details and [SPARQL: Remote Fluree Federation](../query/sparql.md#remote-fluree-federation) for full query syntax.

### FROM-Driven Queries (Connection Queries)

When the query body itself specifies which ledgers to target (via `"from"` in JSON-LD or `FROM` in SPARQL), use `query_from()`:

```rust
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();

    // Query where the "from" is embedded in the query body
    let query = json!({
        "from": "mydb:main",
        "select": ["?name"],
        "where": { "@id": "?s", "schema:name": "?name" }
    });

    let result = fluree.query_from()
        .jsonld(&query)
        .execute_formatted()
        .await?;

    // SPARQL with FROM clause
    let result = fluree.query_from()
        .sparql("SELECT ?name FROM <mydb:main> WHERE { ?s <http://schema.org/name> ?name }")
        .execute_formatted()
        .await?;

    Ok(())
}
```

### Background Indexing

```rust
use fluree_db_api::{FlureeBuilder, BackgroundIndexerWorker, Result};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = Arc::new(FlureeBuilder::file("./data").build()?);

    // Start background indexer
    let indexer = BackgroundIndexerWorker::new(
        fluree.clone(),
        Duration::from_secs(5), // Index interval
    );

    let indexer_handle = indexer.start();

    // Application logic
    let ledger = fluree.create_ledger("mydb").await?;

    // Transactions will be indexed automatically in background
    for i in 0..100 {
        let txn = json!({
            "@context": {"ex": "http://example.org/ns/"},
            "@graph": [{"@id": format!("ex:item{}", i), "ex:value": i}]
        });

        fluree.graph("mydb:main")
            .transact()
            .insert(&txn)
            .commit()
            .await?;
    }

    // Wait for indexing to complete
    sleep(Duration::from_secs(10)).await;

    // Shutdown indexer
    indexer_handle.shutdown().await?;

    Ok(())
}
```

### BM25 Full-Text Search

```rust
use fluree_db_api::{
    FlureeBuilder, Bm25CreateConfig, Bm25FieldConfig, Result
};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await?;

    // Insert searchable data and create BM25 index
    // ...

    // Query with full-text search using JSON-LD and the f:graphSource pattern
    let search_query = json!({
        "@context": {
            "schema": "http://schema.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "from": "mydb:main",
        "select": ["?product", "?score", "?name"],
        "where": [
            {
                "f:graphSource": "products-search:main",
                "f:searchText": "laptop",
                "f:searchLimit": 10,
                "f:searchResult": { "f:resultId": "?product", "f:resultScore": "?score" }
            },
            { "@id": "?product", "schema:name": "?name" }
        ],
        "orderBy": [["desc", "?score"]],
        "limit": 10
    });

    let result = fluree.query_from()
        .jsonld(&search_query)
        .execute()
        .await?;

    println!("Found {} matching products", result.row_count());

    Ok(())
}
```

## Configuration

### Builder Options

```rust
use fluree_db_api::{FlureeBuilder, ConnectionConfig, IndexConfig, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let config = ConnectionConfig {
        storage_path: "./data".into(),
        index_config: IndexConfig {
            interval_ms: 5000,
            batch_size: 10,
            memory_mb: 2048,
            threads: 4,
        },
        ..Default::default()
    };

    let fluree = FlureeBuilder::with_config(config).build()?;

    Ok(())
}
```

### Custom Storage Backend

```rust
use fluree_db_api::{
    FlureeBuilder, Storage, StorageWrite, Result
};
use async_trait::async_trait;

// Implement custom storage
struct MyStorage;

#[async_trait]
impl Storage for MyStorage {
    async fn read(&self, address: &str) -> Result<Vec<u8>> {
        // Custom implementation
        todo!()
    }
}

#[async_trait]
impl StorageWrite for MyStorage {
    async fn write(&self, address: &str, data: &[u8]) -> Result<()> {
        // Custom implementation
        todo!()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let storage = MyStorage;
    let fluree = FlureeBuilder::custom(storage).build()?;

    Ok(())
}
```

If you need full control over both storage and nameservice (e.g., for proxy mode or custom backends), use `build_with()`:

```rust
let storage = MyStorage;
let nameservice = MyNameService;

let fluree = FlureeBuilder::memory()
    .build_with(storage, nameservice);
```

`build_with()` respects the builder's caching configuration — caching is on by default, or call `.without_ledger_caching()` before `build_with()` to disable it.

## Error Handling

```rust
use fluree_db_api::{FlureeBuilder, ApiError, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create a ledger — handles duplicates gracefully
    match fluree.create_ledger("mydb").await {
        Ok(ledger) => {
            println!("Ledger created at t={}", ledger.t());
        }
        Err(ApiError::LedgerExists(ledger_id)) => {
            println!("Ledger {} already exists, loading...", ledger_id);
            let ledger = fluree.ledger("mydb:main").await?;
            println!("Loaded at t={}", ledger.t());
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            return Err(e);
        }
    }

    Ok(())
}
```

## Testing

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use fluree_db_api::{FlureeBuilder, Result};
    use serde_json::json;

    #[tokio::test]
    async fn test_insert_and_query() -> Result<()> {
        // Use memory storage for tests
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("test").await?;

        // Insert data
        let data = json!({
            "@context": {"ex": "http://example.org/ns/"},
            "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
        });

        fluree.graph("test:main")
            .transact()
            .insert(&data)
            .commit()
            .await?;

        // Query data
        let query = json!({
            "select": ["?name"],
            "where": [{"@id": "ex:alice", "ex:name": "?name"}]
        });

        let result = fluree.graph("test:main")
            .query()
            .jsonld(&query)
            .execute()
            .await?;

        assert_eq!(result.row_count(), 1);

        Ok(())
    }
}
```

### Integration Tests

```rust
// tests/integration_test.rs
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;
use tempfile::TempDir;

#[tokio::test]
async fn test_persistence() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().to_str().unwrap();

    // Create ledger and write data
    {
        let fluree = FlureeBuilder::file(path).build()?;
        let ledger = fluree.create_ledger("test").await?;

        let data = json!({"@context": {}, "@graph": [{"@id": "ex:test"}]});
        fluree.graph("test:main")
            .transact()
            .insert(&data)
            .commit()
            .await?;
    }

    // Verify persistence by reopening
    {
        let fluree = FlureeBuilder::file(path).build()?;
        let ledger = fluree.ledger("test:main").await?;

        assert!(ledger.t() > 0);
    }

    Ok(())
}
```

## Performance Tips

### Batch Transactions

```rust
// Good: Batch related changes
let batch_data = json!({
    "@graph": [
        {"@id": "ex:item1", "ex:value": 1},
        {"@id": "ex:item2", "ex:value": 2},
        {"@id": "ex:item3", "ex:value": 3}
    ]
});
let result = fluree.graph("mydb:main")
    .transact()
    .insert(&batch_data)
    .commit()
    .await?;

// Bad: Individual transactions (more overhead per commit)
for i in 1..=3 {
    let txn = json!({"@graph": [{"@id": format!("ex:item{}", i), "ex:value": i}]});
    fluree.graph("mydb:main")
        .transact()
        .insert(&txn)
        .commit()
        .await?;
}
```

### Use Appropriate Storage

- **Memory**: Fastest, no persistence (tests, temporary data)
- **File**: Good balance (single server, local development)
- **AWS**: Distributed, durable (production, multi-server)

### Query Optimization

```rust
// Good: Specific patterns
let query = json!({
    "select": ["?name"],
    "where": [
        {"@id": "ex:alice", "schema:name": "?name"}
    ]
});

// Bad: Broad patterns
let query = json!({
    "select": ["?s", "?p", "?o"],
    "where": [
        {"@id": "?s", "?p": "?o"}
    ]
});
```

### Enable Query Tracking

```rust
use fluree_db_api::{FlureeBuilder, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();

    // Use execute_tracked() for fuel/time/policy tracking
    let tracked = fluree.graph("mydb:main")
        .query()
        .sparql("SELECT * WHERE { ?s ?p ?o }")
        .execute_tracked()
        .await?;

    println!("Query used {} fuel", tracked.fuel().unwrap_or(0));

    Ok(())
}
```

## Graph API Reference

The Graph API follows a **lazy-handle** pattern: `fluree.graph(graph_ref)` returns a lightweight handle, and all I/O is deferred to terminal methods.

### Getting a Graph Handle

```rust
// Lazy handle to the current (head) state
let graph = fluree.graph("mydb:main");

// Lazy handle at a specific point in time
let graph = fluree.graph_at("mydb:main", TimeSpec::AtT(100));
```

### Querying

```rust
// JSON-LD query (lazy — loads graph at execution time)
let result = fluree.graph("mydb:main")
    .query()
    .jsonld(&query_json)
    .execute().await?;

// SPARQL query
let result = fluree.graph("mydb:main")
    .query()
    .sparql("SELECT ?s WHERE { ?s a <ex:Person> }")
    .execute().await?;

// Formatted output (JSON-LD or SPARQL JSON based on query type)
let json = fluree.graph("mydb:main")
    .query()
    .jsonld(&query_json)
    .execute_formatted().await?;

// Tracked query (fuel/time/policy metrics)
let tracked = fluree.graph("mydb:main")
    .query()
    .sparql("SELECT * WHERE { ?s ?p ?o }")
    .execute_tracked().await?;
```

### Materializing a GraphSnapshot

```rust
// Load once, query many times (avoids reloading)
let db = fluree.graph("mydb:main").load().await?;

let r1 = db.query().sparql("...").execute().await?;
let r2 = db.query().jsonld(&q).execute().await?;

// Access the underlying GraphDb
let view = db.view();
```

### Transacting

```rust
// Insert and commit
let result = fluree.graph("mydb:main")
    .transact()
    .insert(&data)
    .commit().await?;

// Upsert with options. f:identity is system-controlled (signed DID,
// opts.identity, or CommitOpts::identity). f:message and f:author are
// pure user claims — supply them in the transaction body just like any
// other txn-meta property.
let data = serde_json::json!({
    "@context": {
        "ex": "http://example.org/",
        "f": "https://ns.flur.ee/db#"
    },
    "@graph": [{ "@id": "ex:alice", "ex:name": "Alice" }],
    "f:message": "admin update",
    "f:author": "did:admin"
});

let result = fluree.graph("mydb:main")
    .transact()
    .upsert(&data)
    .commit_opts(CommitOpts::default().identity("did:admin"))
    .commit().await?;

// Stage without committing (preview changes)
let staged = fluree.graph("mydb:main")
    .transact()
    .insert(&data)
    .stage().await?;

// Query staged state
let preview = staged.query()
    .jsonld(&validation_query)
    .execute().await?;
```

### Commit Inspection

Decode and display the contents of a commit — assertions and retractions with IRIs resolved to compact form. Similar to `git show` for individual commits.

```rust
// By exact CID
let detail = fluree.graph("mydb:main")
    .commit(&commit_id)
    .execute().await?;

// By transaction number
let detail = fluree.graph("mydb:main")
    .commit_t(5)
    .execute().await?;

// By hex-digest prefix (min 6 chars, like abbreviated git hashes)
let detail = fluree.graph("mydb:main")
    .commit_prefix("3dd028")
    .execute().await?;

// With a custom @context for IRI compaction
let detail = fluree.graph("mydb:main")
    .commit_prefix("3dd028")
    .context(my_parsed_context)
    .execute().await?;

// Access the result
println!("t={}, +{} -{}", detail.t, detail.asserts, detail.retracts);
for flake in &detail.flakes {
    let op = if flake.op { "+" } else { "-" };
    println!("{} {} {} {} [{}]", op, flake.s, flake.p, flake.o, flake.dt);
}
```

The returned `CommitDetail` contains:
- **Metadata**: `id`, `t`, `time`, `size`, `previous`, `signer`, `asserts`, `retracts`
- **`context`**: prefix → IRI map derived from the ledger's namespace codes
- **`flakes`**: flat list in SPOT order, each with resolved compact IRIs

`CommitDetail` implements `Serialize` — flakes serialize as `[s, p, o, dt, op]` tuples (with an optional 6th metadata element for language tags, list indices, or named graphs).

### Terminal Operations

| Method | Returns | Description |
|--------|---------|-------------|
| `.execute()` | `Result<QueryResult>` | Raw query result |
| `.execute_formatted()` | `Result<JsonValue>` | Formatted JSON output (JSON-LD for `.jsonld()`, SPARQL JSON for `.sparql()`) |
| `.execute_tracked()` | `Result<TrackedQueryResponse>` | Result with fuel/time/policy tracking |
| `.commit()` | `Result<TransactResultRef>` | Stage + commit transaction |
| `.stage()` | `Result<StagedGraph>` | Stage without committing |
| `.load()` | `Result<GraphSnapshot>` | Materialize snapshot for reuse |

### Format Override

```rust
use fluree_db_api::FormatterConfig;

// Force JSON-LD format for a SPARQL query
let result = fluree.graph("mydb:main")
    .query()
    .sparql("SELECT ?name WHERE { ?s <schema:name> ?name }")
    .format(FormatterConfig::jsonld())
    .execute_formatted()
    .await?;
```

### Multi-Ledger Queries (Dataset)

For multi-ledger queries, use `GraphDb` directly:

```rust
let customers = fluree.view("customers:main").await?;
let orders = fluree.view("orders:main").await?;

let dataset = DataSetDb::new()
    .with_default(customers)
    .with_named("orders:main", orders);

let result = dataset.query(&fluree)
    .sparql(query)
    .execute().await?;
```

### FROM-Driven Queries (Connection Queries)

```rust
let result = fluree.query_from()
    .jsonld(&query_with_from)
    .execute().await?;
```

## Transaction Builder API Reference

There are two transaction builder patterns, each suited for different use cases:

### `stage(&handle)` — Server/Application Pattern (Recommended)

Use `stage(&handle)` when building servers or applications with ledger caching enabled. The handle is borrowed and updated in-place on successful commit, ensuring concurrent readers see the update.

```rust
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    // Caching is on by default (required for stage)
    let fluree = FlureeBuilder::file("./data").build()?;

    // Get a cached handle
    let handle = fluree.ledger_cached("mydb:main").await?;

    // Transaction via builder — handle updated in-place
    let data = json!({"@graph": [{"@id": "ex:test", "ex:name": "Test"}]});
    let result = fluree.stage(&handle)
        .insert(&data)
        .execute()
        .await?;

    println!("Committed at t={}", result.receipt.t);

    // Handle now reflects the new state
    let snapshot = handle.snapshot().await;
    assert_eq!(snapshot.t, result.receipt.t);

    Ok(())
}
```

**Why use `stage(&handle)`:**
- **Concurrent safety**: Multiple requests share the same handle; updates are atomic
- **No ownership dance**: You don't need to track and pass around `LedgerState` values
- **Server-friendly**: Matches how the HTTP server handles transactions internally

### `stage_owned(ledger)` — CLI/Script/Test Pattern

Use `stage_owned(ledger)` when you manage your own `LedgerState` directly. This is typical for CLI tools, scripts, and tests where you don't need ledger caching.

```rust
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();

    // You own the ledger state
    let ledger = fluree.create_ledger("mydb").await?;

    // Transaction consumes ledger, returns updated state
    let data = json!({"@graph": [{"@id": "ex:test", "ex:name": "Test"}]});
    let result = fluree.stage_owned(ledger)
        .insert(&data)
        .execute()
        .await?;

    // Get the updated ledger from the result
    let ledger = result.ledger;
    println!("Now at t={}", ledger.t());

    Ok(())
}
```

**Why use `stage_owned(ledger)`:**
- **Simple ownership**: Good for linear workflows (load → transact → done)
- **No caching required**: Works even with `without_ledger_caching()`
- **Test-friendly**: Each test manages its own state

### Choosing Between Them

| Use Case | Pattern | Why |
|----------|---------|-----|
| HTTP server | `stage(&handle)` | Shared handles, atomic updates |
| Long-running app | `stage(&handle)` | Concurrent access to same ledger |
| CLI tool | `stage_owned(ledger)` | Simple, no caching needed |
| Integration test | `stage_owned(ledger)` | Isolated state per test |
| Script/batch job | `stage_owned(ledger)` | Linear workflow |

### Builder Methods (Both Patterns)

Both `stage(&handle)` and `stage_owned(ledger)` return a builder with identical methods:

```rust
let result = fluree.stage(&handle)  // or stage_owned(ledger)
    .insert(&data)                   // or .upsert(&data), .update(&data)
    .commit_opts(CommitOpts::default().identity("did:admin"))
    .execute()
    .await?;
// (Include `f:message` / `f:author` directly in `data` for user-claim provenance.)
```

| Method | Description |
|--------|-------------|
| `.insert(&json)` | Insert JSON-LD data |
| `.upsert(&json)` | Upsert JSON-LD data |
| `.update(&json)` | Update with WHERE/DELETE/INSERT |
| `.insert_turtle(&ttl)` | Insert Turtle data |
| `.upsert_turtle(&ttl)` | Upsert Turtle data |
| `.txn_opts(opts)` | Set transaction options (branch, context) |
| `.commit_opts(opts)` | Set commit options (identity, raw_txn) |
| `.policy(ctx)` | Set policy enforcement |
| `.execute()` | Stage + commit |
| `.stage()` | Stage without committing (returns `Staged`) |
| `.validate()` | Check configuration without executing |

### Graph API Transactions

The Graph API (`fluree.graph(graph_ref).transact()`) is built on top of `stage(&handle)` internally:

```rust
// Graph API (convenient, uses caching internally)
let result = fluree.graph("mydb:main")
    .transact()
    .insert(&data)
    .commit()
    .await?;

// Equivalent to:
let handle = fluree.ledger_cached("mydb:main").await?;
let result = fluree.stage(&handle)
    .insert(&data)
    .execute()
    .await?;
```

## Ledger Info API

Get comprehensive metadata about a ledger using the `ledger_info()` builder:

```rust
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::file("./data").build()?;

    // Get ledger info with optional context for IRI compaction
    let context = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.org/ns/"
    });

    let info = fluree
        .ledger_info("mydb:main")
        .with_context(&context)
        // Optional: include datatype breakdowns under stats.properties[*]
        // .with_property_datatypes(true)
        // Optional: make property datatype details novelty-aware (real-time)
        // .with_realtime_property_details(true)
        .execute()
        .await?;

    // Access metadata sections
    println!("Commit: {}", info["commit"]);
    println!("Nameservice: {}", info["nameservice"]);
    println!("Namespace codes: {}", info["namespace-codes"]);
    println!("Stats: {}", info["stats"]);
    println!("Index: {}", info["index"]);

    Ok(())
}
```

### Ledger Info Response

The response includes:

| Section | Description |
|---------|-------------|
| `commit` | Commit info in JSON-LD format |
| `nameservice` | NsRecord in JSON-LD format |
| `namespace-codes` | Inverted mapping (prefix → code) for IRI expansion |
| `stats` | Flake counts, size, property/class statistics with selectivity |
| `index` | Index metadata (`t`, ContentId, index ID) |

#### Stats freshness (real-time vs indexed)

The `stats` section now uses layered runtime stats assembly:

- Default `ledger_info()` uses the full novelty-aware path, including lookup-backed class/ref enrichment.
- `with_realtime_property_details(false)` downgrades to the lighter fast novelty-aware merge (`Indexed` + novelty deltas, no extra lookups).
- HLL / NDV fields remain index-derived, so they are omitted by default and only included via `with_property_estimates(true)`.

That means the payload still mixes **real-time** values (indexed + novelty deltas) with values that are only available **as-of the last index**.

- **Real-time (includes novelty)**:
  - `stats.flakes`, `stats.size`
  - `stats.properties[*].count` (but not NDV)
  - `stats.properties[*].datatypes` by default
  - `stats.classes[*].count`
  - `stats.classes[*].property-list` and `stats.classes[*].properties` (property presence)
  - `stats.classes[*].properties[*].refs` by default

- **As-of last index**:
  - `stats.indexed` (the index \(t\))
  - `stats.properties[*].ndv-values`, `stats.properties[*].ndv-subjects` when explicitly included via `with_property_estimates(true)`
  - Any selectivity derived from NDV values
  - `stats.classes[*].properties[*].refs` only when callers explicitly disable full detail with `with_realtime_property_details(false)`

## Nameservice Query API

Query metadata about all ledgers and graph sources using the `nameservice_query()` builder:

```rust
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::file("./data").build()?;

    // Find all ledgers on main branch
    let query = json!({
        "@context": {"f": "https://ns.flur.ee/db#"},
        "select": ["?ledger", "?t"],
        "where": [{"@id": "?ns", "@type": "f:LedgerSource", "f:ledger": "?ledger", "f:branch": "main", "f:t": "?t"}],
        "orderBy": [{"var": "?t", "desc": true}]
    });

    let results = fluree.nameservice_query()
        .jsonld(&query)
        .execute_formatted()
        .await?;

    println!("Ledgers: {}", serde_json::to_string_pretty(&results)?);

    // SPARQL query
    let results = fluree.nameservice_query()
        .sparql("PREFIX f: <https://ns.flur.ee/db#>
                 SELECT ?ledger ?t WHERE { ?ns a f:LedgerSource ; f:ledger ?ledger ; f:t ?t }")
        .execute_formatted()
        .await?;

    println!("SPARQL results: {}", serde_json::to_string_pretty(&results)?);

    // Convenience method (equivalent to builder with defaults)
    let results = fluree.query_nameservice(&query).await?;

    Ok(())
}
```

### Available Properties

**Ledger Records** (`@type: "f:LedgerSource"`):

| Property | Description |
|----------|-------------|
| `f:ledger` | Ledger name (without branch suffix) |
| `f:branch` | Branch name |
| `f:t` | Current transaction number |
| `f:status` | Status: "ready" or "retracted" |
| `f:ledgerCommit` | Reference to latest commit ContentId |
| `f:ledgerIndex` | Index info with `@id` and `f:t` |

**Graph Source Records** (`@type: "f:GraphSourceDatabase"`):

| Property | Description |
|----------|-------------|
| `f:name` | Graph source name |
| `f:branch` | Branch name |
| `f:config` | Configuration JSON |
| `f:dependencies` | Source ledger dependencies |
| `f:indexAddress` | Index ContentId |
| `f:indexT` | Index transaction number |

### Builder Methods

| Method | Description |
|--------|-------------|
| `.jsonld(&query)` | Set JSON-LD query input |
| `.sparql(query)` | Set SPARQL query input |
| `.format(config)` | Override output format |
| `.execute_formatted()` | Execute and return formatted JSON |
| `.execute()` | Execute with default formatting |
| `.validate()` | Validate without executing |

### Example Queries

```rust
// Find ledgers with t > 100
let query = json!({
    "@context": {"f": "https://ns.flur.ee/db#"},
    "select": ["?ledger", "?t"],
    "where": [{"@id": "?ns", "f:ledger": "?ledger", "f:t": "?t"}],
    "filter": ["(> ?t 100)"]
});

// Find all BM25 graph sources
let query = json!({
    "@context": {"f": "https://ns.flur.ee/db#"},
    "select": ["?name", "?deps"],
    "where": [{"@id": "?gs", "@type": "f:Bm25Index", "f:name": "?name", "f:dependencies": "?deps"}]
});
```

## Examples

See complete examples in `fluree-db-api/examples/`:

- `benchmark_aj_query_1.rs` - Basic query patterns
- `benchmark_aj_query_2.rs` - Complex queries
- `benchmark_aj_query_3.rs` - Aggregations
- `benchmark_aj_query_4.rs` - Time travel queries

Run examples:

```bash
cargo run --example benchmark_aj_query_1 --release
```

## API Reference

For detailed API documentation, see:

```bash
cargo doc --open -p fluree-db-api
```

## Related Documentation

- [Getting Started](README.md) - Overview
- [HTTP API](../api/README.md) - Server-based usage
- [Distributed Tracing Integration](../operations/distributed-tracing.md) - Correlating your app's traces with Fluree
- [Query](../query/README.md) - Query documentation
- [Transactions](../transactions/README.md) - Write operations
- [Crate Map](../reference/crate-map.md) - Architecture overview
- [Dev Setup](../contributing/dev-setup.md) - Development guide
