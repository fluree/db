# Reindex API

The Reindex API provides full rebuilds of ledger indexes from the commit chain. Use this when you need to rebuild indexes from scratch, such as after suspected corruption or index configuration changes.

## Overview

Unlike [background indexing](background-indexing.md) which incrementally updates indexes as transactions commit, reindexing rebuilds the entire binary columnar index from the commit history.

Reindex publishes the new index root via `publish_index_allow_equal`, which means a reindex can produce a **new index root CID** even when `index_t` stays the same (same logical snapshot, different physical layout/config).

## When to Reindex

### Common Use Cases

1. **Index corruption** - Query errors or unexpected results suggest corrupted indexes
2. **Configuration changes** - Changing index parameters (leaf size, branch size)
3. **Storage backend changes** - If you move a deployment between storage backends or adopt a new index strategy/type.

### Before You Reindex

Consider these factors:

- **Duration**: Full reindex scales with ledger size; large ledgers may take hours
- **Resources**: Ensure adequate memory and storage during the operation
- **Availability**: Queries remain available during reindex, but may be slower
- **Backup**: Be sure to back up data before major reindex operations

## Rust API

The reindex API is exposed through the `Fluree` type in `fluree-db-api`. `Fluree` owns the storage backend, node cache, nameservice, and provides all ledger operations including queries, transactions, and admin functions like reindex.

### Basic Reindex

```rust
use fluree_db_api::{FlureeBuilder, ReindexOptions, ReindexResult};

// Create Fluree instance
let fluree = FlureeBuilder::file("/path/to/data")
    .build()
    .await?;

// Reindex with default options
let result: ReindexResult = fluree.reindex("mydb:main", ReindexOptions::default()).await?;

println!("Reindexed to t={}", result.index_t);
println!("Root ID: {}", result.root_id);
```

### Reindex with Custom Options

```rust
use fluree_db_api::{FlureeBuilder, ReindexOptions};
use fluree_db_indexer::IndexerConfig;

let fluree = FlureeBuilder::file("/path/to/data").build().await?;

let result = fluree.reindex("mydb:main", ReindexOptions::default()
    // Use custom index node sizes
    .with_indexer_config(IndexerConfig::large())
).await?;
```

## ReindexOptions Reference

| Option | Default | Description |
|--------|---------|-------------|
| `indexer_config` | `IndexerConfig::default()` | Controls output index structure (leaf/branch sizes, GC settings, memory budget) |

### `indexer_config`

Controls the output index structure and rebuild resources:

```rust
use fluree_db_indexer::IndexerConfig;

// For small datasets (< 100k flakes)
ReindexOptions::default()
    .with_indexer_config(IndexerConfig::small())

// For large datasets (> 10M flakes)
ReindexOptions::default()
    .with_indexer_config(IndexerConfig::large())

// Custom configuration
let config = IndexerConfig::default()
    .with_gc_max_old_indexes(10)       // Keep more old index versions
    .with_gc_min_time_mins(60)         // Retain for at least 60 minutes
    .with_run_budget_bytes(1 << 30)    // 1 GB memory budget for sort buffers
    .with_data_dir("/data/fluree");    // Directory for index artifacts

ReindexOptions::default()
    .with_indexer_config(config)
```

Key `IndexerConfig` fields:

| Field | Default | Description |
|-------|---------|-------------|
| `leaf_target_bytes` | 187,500 | Target bytes per leaf node |
| `leaf_max_bytes` | 375,000 | Maximum bytes per leaf node (triggers split) |
| `branch_target_children` | 100 | Target children per branch node |
| `branch_max_children` | 200 | Maximum children per branch node |
| `gc_max_old_indexes` | 5 | Old index versions to retain before GC |
| `gc_min_time_mins` | 30 | Minimum age (minutes) before an index can be GC'd |
| `run_budget_bytes` | 256 MB | Memory budget for sort buffers (split across all sort orders) |
| `data_dir` | System temp dir | Base directory for index artifacts |
| `incremental_enabled` | true | Background indexing: attempt incremental updates before full rebuild |
| `incremental_max_commits` | 10,000 | Background indexing: max commit window for incremental indexing |
| `incremental_max_concurrency` | 4 | Background indexing: max concurrent (graph, order) branch updates |

Note: Reindex is a full rebuild. The `incremental_*` fields are used by background indexing and are not relevant to the semantics of a reindex operation.

## ReindexResult

The reindex operation returns:

```rust
pub struct ReindexResult {
    /// Ledger ID
    pub ledger_id: String,
    /// Transaction time the index was built to
    pub index_t: i64,
    /// ContentId of the new index root
    pub root_id: ContentId,
    /// Index build statistics
    pub stats: IndexStats,
}
```

## Error Handling

### Common Errors

```rust
use fluree_db_api::ApiError;

match fluree.reindex("mydb:main", opts).await {
    Ok(result) => println!("Success: t={}", result.index_t),
    Err(ApiError::NotFound(msg)) => {
        // Ledger doesn't exist or has no commits
        println!("Ledger not found: {}", msg);
    }
    Err(ApiError::ReindexConflict { expected, found }) => {
        // Ledger advanced during reindex (new commits arrived)
        println!("Conflict: expected t={}, found t={}", expected, found);
    }
    Err(e) => {
        // Storage, indexing, or other errors
        println!("Reindex failed: {}", e);
    }
}
```

## How It Works

The reindex operation:

1. **Looks up** the current ledger state and captures `commit_t` for conflict detection
2. **Cancels** any active background indexing for the ledger
3. **Rebuilds** a fresh binary columnar index from the full commit chain using `rebuild_index_from_commits`:
   - **Phase A**: Walks the commit DAG once, reading only the envelope header of each commit via byte-range requests (`ContentStore::get_range`). Returns the chronological CID list plus the genesis-most `NsSplitMode` in a single pass, so per-commit bandwidth on remote storage is ~128 KiB rather than the full commit blob.
   - **Phase B**: Resolves commits into batched chunks with chunk-local dictionaries (subjects, strings) and shared global dictionaries (predicates, datatypes, graphs, languages, numbigs, vectors). Commit blobs are pre-fetched concurrently (`buffered(K)`, default `K=3`, env-tunable via `FLUREE_REBUILD_FETCH_CONCURRENCY`) so S3 round-trip latency overlaps with local decode cost.
   - **Phase C**: Merges per-chunk dictionaries into global dictionaries with remap tables
   - **Phase D**: Builds SPOT indexes from sorted commit files via k-way merge with graph-aware partitioning
   - **Phase E**: Builds secondary indexes (PSOT, POST, OPST) per-graph from partitioned run files
   - **Phase F**: Uploads dictionaries and index artifacts to CAS, creates `IndexRoot` (FIR6)
4. **Validates** that no new commits arrived during the build (conflict detection)
5. **Publishes** the new index root via `publish_index_allow_equal`
6. **Spawns** async garbage collection to clean up old index versions

The rebuilt index preserves full time-travel history: retract-winner events and their preceding asserts are stored in Region 3 (history) of leaf nodes, enabling `as-of` queries at any past transaction time.

## Best Practices

### 1. Schedule During Low-Traffic Periods

While queries continue to work during reindex, performance may be impacted. Schedule large reindex operations during maintenance windows when possible.

### 2. Tune Memory Budget for Large Ledgers

For ledgers with millions of flakes, increasing `run_budget_bytes` reduces the number of spill files and speeds up the merge phase:

```rust
let config = IndexerConfig::default()
    .with_run_budget_bytes(2 * 1024 * 1024 * 1024); // 2 GB
```

### 3. Tune Phase B Fetch Concurrency for Remote Storage

When reindexing from remote storage (S3) on latency-bound platforms like AWS Lambda, Phase B benefits from fetching several commit blobs in parallel so S3 round-trip latency (25–50 ms) overlaps with local decode cost.

```bash
# Default: 3. Increase for high-latency links; pin to 1 for strict serial behavior.
export FLUREE_REBUILD_FETCH_CONCURRENCY=4
```

In-flight memory is bounded by `K × avg_commit_blob_size`. For typical commits (< 1 MB) and `K=3`, the overhead is negligible against the `run_budget_bytes` pool. Pathologically large commits (hundreds of MB) should set `K=1` to avoid transient memory spikes.

### 4. Verify After Reindex

After reindex, verify the results:

```rust
// Get ledger info to check state
let info = fluree.ledger_info(ledger_id).execute().await?;
println!("Index rebuilt to t={}", info["index"]["t"]);

// Run a sample query to verify correctness
let db = fluree_db_api::GraphDb::from_ledger_state(&ledger);
let query_result = fluree.query(&db, &sample_query).await?;
```

### 5. Concurrent Operations

During reindex:
- Queries continue to work (using old index + novelty)
- Transactions continue to work (writes to novelty)
- Background indexing is paused for this ledger

## Related Documentation

- [Background Indexing](background-indexing.md) - Automatic incremental indexing
- [Admin and Health](../operations/admin-and-health.md) - Admin operations
- [Rust API](../getting-started/rust-api.md) - Using Fluree as a library
- [Storage](../operations/storage.md) - Storage configuration
