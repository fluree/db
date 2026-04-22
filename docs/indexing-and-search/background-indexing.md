# Background Indexing

Fluree maintains query-optimized indexes through a background indexing process. This document covers the indexing architecture, configuration, and monitoring.

## Index Architecture

Fluree maintains four index permutations for efficient query execution:

### SPOT (Subject-Predicate-Object-Time)

Organized by subject first:

```text
ex:alice → schema:name → "Alice" → [t=1, t=5]
ex:alice → schema:age → 30 → [t=1]
ex:alice → schema:age → 31 → [t=10]
```

**Optimized for:** "Give me all properties of this subject"

### POST (Predicate-Object-Subject-Time)

Organized by predicate first:

```text
schema:name → "Alice" → ex:alice → [t=1, t=5]
schema:age → 30 → ex:alice → [t=1]
schema:age → 31 → ex:alice → [t=10]
```

**Optimized for:** "Find all subjects with this property/value"

### OPST (Object-Predicate-Subject-Time)

Organized by object first:

```text
"Alice" → schema:name → ex:alice → [t=1, t=5]
30 → schema:age → ex:alice → [t=1]
31 → schema:age → ex:alice → [t=10]
```

**Optimized for:** "Find subjects with this object value"

### PSOT (Predicate-Subject-Object-Time)

Organized by predicate, then subject:

```text
schema:name → ex:alice → "Alice" → [t=1, t=5]
schema:age → ex:alice → 30 → [t=1]
schema:age → ex:alice → 31 → [t=10]
```

**Optimized for:** "Get all values for this predicate"

## Indexing Process

### 1. Transaction Commit

```text
t=42: Transaction committed
  - Flakes written to append-only log
  - Commit metadata created
  - Commit published to nameservice (commit_t=42)
```

### 2. Indexer Detection

Background indexing is triggered when the ledger’s novelty exceeds the configured threshold (see Configuration below):

```text
Indexer checks: commit_t=42, index_t=40
Indexer: Need to index t=41, t=42
```

### 3. Index Building

Background indexing builds a new index snapshot up to a specific `to_t` (typically the current `commit_t` when the job starts). During the job, new commits may arrive; those remain in novelty for the next cycle.

```text
Incremental indexing (default path):
  - Load the existing index root (CAS CID) from nameservice
  - Resolve only commits with t in (index_t, to_t]
  - Merge resolved novelty into only the affected leaf blobs (Copy-on-Write)
  - Update dictionaries (forward packs + reverse trees)
  - Assemble a new root referencing mostly-unchanged CAS artifacts

Fallback:
  - If incremental indexing cannot safely proceed, fall back to a full rebuild
```

### 4. Index Publishing

When complete:

```text
  - Upload new CAS blobs (leaves, branches, dict blobs) as needed
  - Upload the new index root (CAS CID)
  - Publish index_head_id to nameservice (atomic “commit point”)
  - Update index_t to to_t
```

## Novelty Layer

The **novelty layer** consists of transactions committed but not yet indexed:

```text
Current State:
  commit_t = 150
  index_t = 145
  novelty = [t=146, t=147, t=148, t=149, t=150]
```

### Query Execution with Novelty

Queries combine indexed data with novelty:

```text
Query for ex:alice's properties:

1. Check SPOT index (up to t=145)
2. Apply novelty layer (t=146 to t=150)
3. Combine results
```

### Impact of Large Novelty

**Small novelty** (< 10 transactions):
- Minimal query overhead
- Fast query execution

**Large novelty** (> 100 transactions):
- Significant query overhead
- Slower query execution
- Higher memory usage

## Configuration

Background indexing is enabled at the server level, and indexing is triggered based on novelty size thresholds:

- Enable/disable background indexing: `--indexing-enabled` / `FLUREE_INDEXING_ENABLED`
- Trigger threshold (soft): `--reindex-min-bytes` / `FLUREE_REINDEX_MIN_BYTES`
- Backpressure threshold (hard): `--reindex-max-bytes` / `FLUREE_REINDEX_MAX_BYTES`

See [Operations: Configuration](../operations/configuration.md#background-indexing) for the canonical flag/env/config-file reference.

### Incremental parallelism (per ledger)

Within a single incremental indexing job, Fluree can update multiple `(graph, index-order)` branches concurrently. This is bounded by:

- `IndexerConfig.incremental_max_concurrency` (default: 4)

This setting is part of the Rust `IndexerConfig` used by the indexer pipeline; it is not a server CLI flag. Increasing it can improve throughput on multi-graph ledgers and can run the four main index orders (SPOT/PSOT/POST/OPST) in parallel, at the cost of higher peak memory.

## Monitoring

### Check Index Status

```bash
curl http://localhost:8090/ledgers/mydb:main
```

Response:
```json
{
  "ledger_id": "mydb:main",
  "branch": "main",
  "commit_t": 150,
  "index_t": 145,
  "commit_id": "bafy...headCommit",
  "index_id": "bafy...indexRoot"
}
```

**Key Metrics:**
- **index lag (txns)**: `commit_t - index_t`

For byte-level novelty size and indexing trigger decisions, see the `indexing` block returned by transaction and replication endpoints (e.g. `POST /push/<ledger>`), documented in [API Endpoints](../api/endpoints.md).

### Key Log Messages

At `INFO`, background indexing now emits coarse-grained progress logs that make it easier to distinguish:

- request queued vs. worker started
- current wait status while `trigger_index()` is blocked
- incremental vs. rebuild path selection
- commit-chain walking progress
- commit resolution progress and phase completion

When background indexing is queued by an HTTP transaction request, the worker logs also include copied `request_id` and `trace_id` fields from the triggering request. This provides log-level correlation between the foreground request and the later background build without making the index build part of the original request trace.

At `DEBUG`, the same wait and commit-walk paths emit more frequent progress updates for incident debugging without changing behavior.

When you call indexing through the Rust API with `trigger_index()`, wait timeout
is optional and should generally be chosen by the caller. Leave
`TriggerIndexOptions.timeout_ms` unset to wait until completion, or set it
explicitly for bounded environments such as Lambda jobs, HTTP gateways, or
other workers with a fixed maximum runtime.

### Health Indicators

**Healthy:**
```text
index_lag: 0-10 transactions
index_rate > transaction_rate
```

**Warning:**
```text
index_lag: 10-50 transactions
index_rate ≈ transaction_rate
```

**Critical:**
```text
index_lag: > 50 transactions
index_rate < transaction_rate
```

## Performance Tuning

### Optimize for Write-Heavy Loads

```bash
fluree-server \
  --indexing-enabled \
  --reindex-min-bytes 200000 \
  --reindex-max-bytes 2000000
```

Larger thresholds reduce indexing frequency (more novelty accumulation), trading some query-time overlay cost for reduced background indexing activity.

### Optimize for Read-Heavy Loads

```bash
fluree-server \
  --indexing-enabled \
  --reindex-min-bytes 50000
```

Smaller `reindex-min-bytes` keeps novelty smaller (better query performance) at the cost of more frequent background indexing cycles.

## Index Storage

### Index Snapshots

Indexes are stored as immutable, content-addressed snapshots:

```text
  - Leaf blobs (FLI3) and branch manifests (FBR3)
  - Dictionary blobs (forward packs, reverse tree leaves/branches)
  - An index root blob (FIR6) that references everything needed for queries
```

The nameservice stores the current index root CID (`index_head_id`) and its watermark (`index_t`). Peers fetch only the CAS objects they need on demand.

### Index Retention

Old index snapshots are retained for time-travel safety and concurrent query safety. Cleanup is performed by the binary index garbage collector, governed by:

- `IndexerConfig.gc_max_old_indexes`
- `IndexerConfig.gc_min_time_mins`

You can also trigger cleanup via the admin endpoint `POST /admin/compact?ledger=...` (see [API Endpoints](../api/endpoints.md#admin-endpoints)).

## Troubleshooting

### High indexing lag

**Symptom:** `commit_t - index_t` grows continuously

**Causes:**
- Transaction rate exceeds indexing capacity
- Large transactions
- Insufficient resources

**Solutions:**
1. Reduce `reindex-min-bytes` so indexing triggers sooner
2. Increase resources for the indexer (CPU/memory and storage throughput)
3. Consider running a dedicated indexer process (separate from the transactor)
4. For incremental indexing, consider increasing `IndexerConfig.incremental_max_concurrency`

### Slow Indexing

**Symptom:** `index_t` advances slowly (or stops advancing)

**Causes:**
- Disk I/O bottleneck
- CPU bottleneck
- Large index size
- Storage backend latency

**Solutions:**
1. Use faster storage (SSD)
2. Increase CPU allocation
3. Optimize transaction patterns
4. Use local storage vs network storage

### Index Corruption

**Symptom:** Query errors, unexpected results

**Recovery:** Use the [Reindex API](reindex.md) to rebuild indexes from scratch if you suspect corruption or need to change index structure parameters.

## Best Practices

### 1. Monitor Novelty

```javascript
setInterval(async () => {
  const status = await fetch('http://localhost:8090/ledgers/mydb:main')
    .then(r => r.json());
  
  const lag = status.commit_t - status.index_t;
  if (lag > 50) {
    console.warn(`High indexing lag: ${lag} transactions`);
  }
}, 30000);  // Check every 30 seconds
```

### 2. Tune for Workload

Match configuration to workload pattern:
- Write-heavy: Larger `reindex-min-bytes` (fewer indexing cycles)
- Read-heavy: Smaller `reindex-min-bytes` (less novelty overlay)
- Balanced: Default settings

### 3. Capacity Planning

Estimate indexing capacity:

```text
Transaction rate: 10 txn/second
Avg flakes per txn: 100
Total flakes: 1,000 flakes/second

Indexing capacity: 2,000 flakes/second (2× margin)
```

### 4. Alert on Lag

Set up alerting:

```javascript
const lag = status.commit_t - status.index_t;
if (lag > 100) {
  alertOps('Critical: Indexing lag > 100 transactions');
}
```

### 5. Scheduled Compaction

Run compaction during off-peak hours:

```bash
# Cron job
0 2 * * * curl -X POST http://localhost:8090/admin/compact
```

## Related Documentation

- [Reindex API](reindex.md) - Manual index rebuilding and recovery
- [Indexing Side-Effects](../transactions/indexing-side-effects.md) - Transaction impact on indexing
- [Query Performance](../query/explain.md) - Query optimization
- [BM25](bm25.md) - Full-text search indexing
- [Vector Search](vector-search.md) - Vector indexing
