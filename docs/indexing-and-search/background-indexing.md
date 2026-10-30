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

Background indexing is **on by default**. Indexing is triggered based on novelty size thresholds:

- Enable/disable background indexing: `--indexing-enabled` / `FLUREE_INDEXING_ENABLED` (default `true`; disable only when a peer/indexer process owns this storage)
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
curl http://localhost:8090/v1/fluree/info/mydb:main
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
- index garbage collection: `Garbage collection complete` or `Garbage collection released nothing`, with how many versions each pass left (see [Monitoring](#monitoring))

When background indexing is queued by an HTTP transaction request, the worker logs also include copied `request_id` and `trace_id` fields from the triggering request. This provides log-level correlation between the foreground request and the later background build without making the index build part of the original request trace.

At `DEBUG`, the same wait and commit-walk paths emit more frequent progress updates for incident debugging without changing behavior.

When you call indexing through the Rust API with `trigger_index()`, wait timeout
is optional and should generally be chosen by the caller. Leave
`TriggerIndexOptions.timeout_ms` unset to wait until completion, or set it
explicitly for bounded environments such as Lambda jobs, HTTP gateways, or
other workers with a fixed maximum runtime.

`TriggerIndexResult` carries `fuel: Option<f64>` — the total fuel charged for
the build that satisfied the trigger. The background indexer always meters
each build, so callers see `Some(N)` for a real build, `Some(0.0)` when the
requested t was already indexed (no work was needed), and the same value for
all coalesced trigger callers satisfied by a single underlying build. See
[Tracking and Fuel](../query/tracking-and-fuel.md#indexing-fuel) for the
cost-ladder and rate semantics.

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
fluree server run -- \
  --indexing-enabled \
  --reindex-min-bytes 200000 \
  --reindex-max-bytes 2000000
```

Larger thresholds reduce indexing frequency (more novelty accumulation), trading some query-time overlay cost for reduced background indexing activity.

### Optimize for Read-Heavy Loads

```bash
fluree server run -- \
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

Each index build publishes a new version of the ledger's index and supersedes the previous one. New queries read the newest version. A query already running keeps reading the version it started on, whether it runs on the server, on a [query peer](../operations/query-peers.md), or in an application that embeds Fluree as a library. So a superseded version stays in storage until the garbage collector releases it. Superseded versions are not needed for [time travel](../concepts/time-travel.md): a historical query reads history from the current index.

#### Settings

The age guard decides when a superseded version goes, and a ceiling backs it up for ledgers that publish fast:

| Setting | Default | Meaning |
|---------|---------|---------|
| Age guard | 15 minutes | A superseded version is kept this long, so a query that started on it can finish. |
| Ceiling | 10 | Past this many old versions, a version is collected even inside the age guard. |
| Floor | 0 | Optional. The newest this many old versions are always kept, whatever their age. |

Where each one is set:

| | Age guard | Ceiling | Floor |
|---|---|---|---|
| Server flag | `--gc-min-time-mins` | `--gc-hard-max-old-indexes` | `--gc-max-old-indexes` |
| Environment | `FLUREE_GC_MIN_TIME_MINS` | `FLUREE_GC_HARD_MAX_OLD_INDEXES` | `FLUREE_GC_MAX_OLD_INDEXES` |
| Config file, `[server.indexing]` | `gc_min_time_mins` | `gc_hard_max_old_indexes` | `gc_max_old_indexes` |
| [JSON-LD connection config](../reference/connection-config-jsonld.md), `defaults.indexing` | `gcMinTimeMins` | `gcHardMaxOldIndexes` | `maxOldIndexes` |
| Rust, `IndexerConfig` | `gc_min_time_mins` | `gc_hard_max_old_indexes` | `gc_max_old_indexes` |

From Rust, `FlureeBuilder::with_gc_settings(floor, age_guard, ceiling)` sets them on the builder; `None` leaves a setting at its default. See [Index retention](../getting-started/rust-api.md#index-retention).

The process that runs the indexer collects garbage, so its settings are the ones that apply. A query peer, or a server with indexing disabled, collects nothing: set retention on the server that indexes.

#### How the settings combine

The collector keeps each old version until it has been superseded for longer than the age guard, and collects everything past the ceiling whatever its age. A floor, if set, is kept regardless of either, and a ceiling at or below the floor is raised to it: the floor's versions are kept, and every version past them goes whatever its age. With the defaults:

| Publish rate | Old versions kept | Decided by |
|--------------|-------------------|------------|
| Once an hour | 1, then 0 | The age guard: the previous version is kept for 15 minutes after each publish, then collected. |
| Every 2 minutes | 7 or 8 | The age guard: the versions superseded in the last 15 minutes. |
| Every 10 seconds | 10 | The ceiling: the age guard alone would keep about 90. |

The collector runs after every index publish, and also on the worker's periodic tick (`IndexerConfig.catchup_interval`, default 300 s), including once at start-up. So a version can outlive its age guard until the next pass. The periodic pass is what reaches a ledger that stops publishing: a load-once dataset whose final burst left versions inside the age guard would otherwise keep them until its next write, which may never come. Passes are serialised per ledger name, the tick runs them one at a time, and releases go out in batches — one `DeleteObjects` request per thousand artifacts on S3, a few in flight — so a large pass does not take the storage request cap away from readers.

#### Sizing

- **Age guard.** Keep it above your longest query. A server stops a query at `query_timeout_ms`, which defaults to 15 minutes, the same as the guard. If you raise that timeout, or set it to `0` so nothing bounds a query, raise the guard to match. An embedding application should size the guard from the longest query it runs.
- **Ceiling.** Keep it above the number of versions the ledger publishes during your longest query. Past the ceiling, a query still reading a collected version fails or reads a torn version. At the default of 10, a ledger that publishes every 6 seconds can collect a version a query started on a minute earlier. To let the age guard alone decide, raise the ceiling; an embedding application can also remove it with `IndexerConfig::with_gc_hard_max_old_indexes(None)`.
- **Disk.** The ceiling bounds versions, not bytes. What a retained version costs varies by orders of magnitude between ledgers — one deployment measured ~7.7 GiB per version on one ledger and ~3.3 GiB on another — so size the ceiling from the per-version disk use you observe, and expect `objects/history` to hold roughly `versions × per-version bytes` at the ceiling.

#### Monitoring

Each pass logs what it left at `INFO`. A pass that releases something logs `Garbage collection complete`. A pass that releases nothing while the age guard is holding old versions logs `Garbage collection released nothing`, so the versions a ledger keeps are visible either way. Both carry:

- `retained`: the versions left on the chain, the current one included.
- `retention_target`: the floor, counting the current version (`1 + gc_max_old_indexes`, so `1` by default).
- `past_target`: the retained versions beyond the floor, which the age guard is holding. With the default floor of 0, that is every old version kept. With the ceiling set, it stays at or below `gc_hard_max_old_indexes − gc_max_old_indexes`, and at 0 once the floor reaches the ceiling, unless a garbage manifest could not be read.
- `stopped_by`: present when the pass stopped before the oldest version. `age_guard` means the next version was inside `gc_min_time_mins`. `unreadable_manifest` means a garbage manifest could not be read.

The completion line also carries `age_guard_overridden`: the versions collected past the ceiling while still inside the age guard. A non-zero value means the ceiling, not the age guard, is limiting retention, and a query still reading one of those versions may have lost it.

#### What the collector reclaims

The collector reclaims artifacts by *name*. Each index root carries a garbage manifest listing what the previous version replaced, and the collector walks the prev-index chain releasing exactly those. It therefore reaches only artifacts that some manifest records.

Dictionary blobs live in the ledger-wide `@shared/dicts/` namespace, shared by every branch: a branch created from another starts out reading the source's dictionaries rather than a copy, so the source's manifest can name a blob the fork still reads. The collector releases a dictionary blob only when no other branch of the ledger — retracted branches included — still reaches it through its own index chain. A blob a sibling still reaches is deferred; when that sibling replaces it, its own manifest names it and the sibling's pass releases it. Whichever branch drops a blob last deletes it, and dropping a branch releases the blobs only that branch referenced. A ledger with a single branch releases every blob its manifests name.

A manifest's word is not final. Content addressing gives byte-identical output the same name, so a later build can recreate a blob an earlier manifest recorded as replaced — reverse-dictionary leaves do this routinely under monotonically increasing keys (ULIDs, UUIDv7, sequential ids). The collector therefore never releases anything a retained root still references, whatever a manifest says. And because a build can recreate such a blob at any moment, a pass plans against a snapshot and then releases inside a short window during which no branch of the ledger builds: it waits for a build in flight, defers new ones, re-reads the current index head of the branch and of each of its siblings, and only then releases. Builds resume when the window closes. A pass that has nothing to release never opens one, and a pass that finds a branch held by a reindex or sweep releases nothing and leaves the work to a later pass. Which branches are siblings comes from the worker's branch listing, refreshed every `catchup_interval`, plus any branch the worker has built since. A pass that collects a version past the ceiling, inside the age guard, takes the listing fresh, as does every pass under an age guard shorter than ten minutes. All of this behaves the same on every storage and nameservice backend: it needs only a consistent single-record lookup, which the file, S3 and DynamoDB nameservices all provide. It covers the indexer in one process; two processes indexing the same storage are not coordinated.

### Reclaiming orphaned artifacts

Artifacts orphaned outside that chain are invisible to the collector. The main source is a reindex published by Fluree **4.1.4 or earlier**: it severed the prev-index chain, leaving every earlier root — and the blobs only those roots referenced — unreachable from any walk. Retention could never truncate past it, so a ledger that had been reindexed accumulated index artifacts without bound.

`POST /v1/fluree/sweep` reclaims them, and `fluree sweep <ledger>` does the same from the CLI. A sweep enumerates what storage holds, subtracts everything reachable from a live index chain, and releases the remainder. Use `--dry-run` (or `POST /v1/fluree/sweep/plan`) to see what would be released first.

A sweep is also what reclaims what the collector cannot reach any more: dictionary blobs whose manifests were consumed on Fluree **4.2.0 or earlier**, which left every dictionary blob to the sweep, blobs a branch drop could not attribute, and anything orphaned off a chain. A ledger that accumulated dictionaries under one of those versions can hold many times its live index in `@shared/dicts/`; one sweep clears the backlog, and afterwards the collector keeps up. Running one when nothing is reclaimable is a safe no-op.

A sweep is ledger-wide rather than per-branch, because dictionary blobs are shared across a ledger's branches. It touches only index artifacts — commits, transactions, and config blobs are reachable through the commit chain rather than the index chain, so a sweep cannot establish that they are unreferenced and never considers them.

> **Single-process deployments only.** The hold that keeps index builds from writing during a sweep excludes the server's own indexer. An external or second-process indexer writing to the same storage is not excluded, and its in-flight artifacts would be indistinguishable from orphans.

`POST /v1/fluree/reindex` rebuilds the index but does not reclaim anything by itself. A reindex now participates in the GC chain like any incremental build, so ordinary retention applies to the roots it supersedes; a sweep is only needed for artifacts orphaned by an earlier version.

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

### Index storage keeps growing

**Symptom:** the ledger's index storage (`objects/history` on disk, or its prefix in a bucket) grows much faster than its data.

**Check** the garbage collection log lines (see [Monitoring](#monitoring)):
- `past_target` near `gc_hard_max_old_indexes − gc_max_old_indexes` (10 by default): the age guard is holding versions up to the ceiling, which is retention working as configured. Lower the ceiling, or the age guard, if your queries allow it (see [Sizing](#sizing)).
- `past_target` above that, with `stopped_by=unreadable_manifest`: a garbage manifest could not be read, and the collector cannot pass it. Look for storage errors on the manifest named at `DEBUG`.
- `retained` small but storage still large: the artifacts are outside the chain the collector walks. Run a [sweep](#reclaiming-orphaned-artifacts).
- No collector lines at all: either the ledger keeps no old versions, or this process does not run the indexer. Retention is decided where indexing runs.

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
  const status = await fetch('http://localhost:8090/v1/fluree/info/mydb:main')
    .then(r => r.json());
  
  const lag = status.t - status.index.t;
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

### 5. Scheduled Reindex

Run a full reindex during off-peak hours when you need to rebuild indexes:

```bash
# Cron job
0 2 * * * curl -X POST http://localhost:8090/v1/fluree/reindex -H "Content-Type: application/json" -d '{"ledger":"mydb:main"}'
```

## Related Documentation

- [Reindex API](reindex.md) - Manual index rebuilding and recovery
- [Indexing Side-Effects](../transactions/indexing-side-effects.md) - Transaction impact on indexing
- [Query Performance](../query/explain.md) - Query optimization
- [BM25](bm25.md) - Full-text search indexing
- [Vector Search](vector-search.md) - Vector indexing
