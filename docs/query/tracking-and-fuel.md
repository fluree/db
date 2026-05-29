# Tracking and Fuel Limits

Fluree provides query tracking and fuel limits to monitor and control query execution, ensuring system stability and performance.

## Query Tracking

Query tracking provides visibility into query execution, helping you understand query behavior and performance.

### Enable Tracking

Enable tracking via the `opts` object. Use `"meta": true` to enable all tracking, or selectively enable specific metrics:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "select": ["?name"],
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ],
  "opts": { "meta": true }
}
```

Or enable specific metrics:

```json
{
  "opts": {
    "meta": {
      "time": true,
      "fuel": true,
      "policy": true
    }
  }
}
```

### Tracked Information

Tracking provides:
- **time**: Query execution duration (formatted as "12.34ms")
- **fuel**: Total cost as a decimal value (rounded to 3 places)
- **policy**: Policy evaluation statistics (`{policy-id: {executed: N, allowed: M}}`)

## Fuel Limits

Fuel limits control resource consumption, preventing runaway queries from consuming excessive resources.

### What Is Fuel?

Fuel is a decimal measure of query/transaction cost. Internally it is accumulated as **micro-fuel** (1 fuel = 1000 micro-fuel) and reported back rounded to 3 decimal places. Costs reflect actual work — primarily I/O — rather than output cardinality.

Cost ladder (per event):

| Event | Cost (fuel) |
|---|---|
| Query floor (once per query, charged at entry before parsing) | 1.000 |
| Index leaflet touched (per scan batch, regardless of cache state) | 0.010 |
| Forward-dict touch (per dict-backed value resolved during result materialization) | 0.010 |
| History-scan leaflet base (per leaflet; per-row costs below add on top) | 0.010 |
| Flake returned from a `db.range` call (e.g. SHACL graph reads, graph crawl) | 0.001 |
| Overlay/novelty row materialized | 0.001 |
| History row scanned (base + in-range sidecar rows) | 0.001 |
| R2RML row emitted (Iceberg/Parquet) | 0.001 |
| Transaction commit baseline (once per commit, including each bulk-import chunk) | 10.000 |
| Staged flake (per flake in a transaction or bulk-import chunk) | 0.001 |
| Indexer CAS write (per successful `ContentStore::put` / `put_with_id` made by an index build) | 1.000 |
| Re-encoded leaflet inside an FLI3 leaf write (passthrough leaflets are not charged) | 1.000 |
| `REGEX` / `REPLACE` evaluation | 0.001 |
| Hash function (`MD5`, `SHA1`, `SHA256`, `SHA384`, `SHA512`) | 0.001 |
| `UUID` / `STRUUID` | 0.001 |
| `geof:distance` | 0.001 |
| Vector similarity (`DotProduct`, `CosineSimilarity`, `EuclideanDistance`) | 0.002 |
| `Fulltext` (per-row BM25 scoring) | 0.005 |

Cheap operations (comparisons, arithmetic, type checks, simple string ops, datetime extraction, etc.) cost zero — instrumentation overhead would dwarf the actual cost.

The **query floor** guarantees every fuel-tracked query reports at least `1.000` fuel: a query touching no persisted data still costs the floor, and a query that errors during parsing/planning still reports it. I/O "touches" cost `0.010` each, so a scan-dominated query reports roughly `1.000 + 0.010 × (leaflet/dict touches)`. The fuel schedule above is defined in one place — `fluree-db-core/src/tracking.rs` (`tracking::schedule`).

### Indexing Fuel

Indexer CAS writes are billed through a `MeteredContentStore` wrapper that the build entry points install around the caller-supplied content store. Every successful `ContentStore::put` / `put_with_id` charges the base **1.000 fuel** rate — including index leaves, branch manifests, root manifests, dict packs / reverse-tree nodes, history sidecars, garbage records, stats sketches, and (incremental) spatial / fulltext arenas. The lower-level `Storage::content_write_bytes` is **not** currently wrapped; the only indexer code path that uses it is the dead-code spatial rebuild helper. If that path is wired up, add a storage-level wrapper first (or migrate it to `ContentStore::put`).

FLI3 leaf writes carry an additional per-leaflet charge of **1.000 fuel per re-encoded leaflet**. Passthrough leaflets (byte-copies carried forward from a prior leaf during an incremental update) are **not** charged because no zstd encoding work was performed — so a 100-leaflet leaf where only two leaflets were touched by novelty bills `1 + 2 = 3` fuel, not `1 + 100`. The two FLI3 leaf upload sites (`build::upload::upload_indexes_to_cas` for full rebuild, `build::incremental::upload_leaf_blobs` for incremental) compute the count from `LeafInfo::re_encoded_leaflet_count`.

Indexing fuel is **measurement only**: indexer trackers are no-limit. A partial index is worse than a slow one, so the indexer never aborts mid-build on a fuel limit. The plain public entry points (`build_index_for_record`, `rebuild_index_from_commits`) pass a disabled tracker and report `fuel: None`. The `*_with_tracker` variants wrap the store, propagate a fuel-enabled tracker, and stamp the final tally on `IndexResult::fuel` — `Some(0.0)` for an already-current build, `Some(N)` for one that did real work.

Where fuel surfaces depends on who initiated the build:

- **`/reindex` (standalone, user-triggered):** the API creates a per-request fuel tracker, returns `ReindexResponse.fuel`. CLI prints `Fuel: N.NNN`.
- **`trigger_index` (standalone, waits for background completion):** the orchestrator creates a per-build tracker; `TriggerIndexResult.fuel` carries the result. **Coalesced trigger callers** all receive the fuel of the single build that satisfied them.
- **Background indexer (no caller waiting):** the orchestrator still creates a per-build tracker and logs the tally on the completion `info!` line (`fuel = ...`), but does not return it anywhere.
- **Combined transactor + post-commit indexing (`maybe_refresh_after_commit` / `require_refresh_before_commit`):** measured with a per-build tracker and logged on the completion line; intentionally **not** attributed to the transaction's tracking response.

`IndexOutcome::Completed` carries `fuel: Option<f64>` so `wait()` callers and waiter handles see the same value. Already-satisfied early-returns (waiter satisfied by a previously-published index) report `Some(0.0)` so callers can distinguish "no work" from "not tracked".

### Setting Fuel Limits

Set fuel limits via `opts.max-fuel` (decimal allowed). Setting a fuel limit implicitly enables fuel tracking:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "select": ["?name"],
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ],
  "opts": { "max-fuel": 10000 }
}
```

You can also use `"maxFuel"` or `"max_fuel"` as alternative key names. The HTTP equivalent is the `fluree-max-fuel` header.

Because the `1.000` query floor is charged before execution and counts toward the limit, `max-fuel` must leave room for it:

- `max-fuel: 1` permits exactly the floor — a query that needs even one persisted touch is rejected.
- `max-fuel: 1.01` permits the floor plus one persisted touch.
- `max-fuel` below `1.0` (e.g. `0.5`) is rejected up front, before parsing.

### Fuel Limit Behavior

When fuel limit is exceeded:
- Query execution stops
- Error returned to client
- Partial results not returned

## Response Format

When tracking is enabled, the response includes tracking information as top-level siblings:

```json
{
  "status": 200,
  "result": [...],
  "time": "12.34ms",
  "fuel": 42.317,
  "policy": {
    "http://example.org/myPolicy": {
      "executed": 10,
      "allowed": 8
    }
  }
}
```

The `fuel` value is decimal with up to 3 places of precision. The HTTP `x-fdb-fuel` response header carries the same value.

Tracked transaction responses (`/insert`, `/upsert`, `/update`, including Turtle/TriG and SPARQL UPDATE when tracking headers are used) expose the same top-level `time`, `fuel`, and `policy` fields when present, alongside the transaction receipt fields.

## Best Practices

### Tracking

1. **Enable for Debugging**: Use `"opts": {"meta": true}` to debug slow queries
2. **Monitor Performance**: Track query performance over time
3. **Identify Bottlenecks**: Use tracking to identify performance bottlenecks

### Fuel Limits

1. **Set Appropriate Limits**: Set fuel limits based on expected query complexity
2. **Monitor Fuel Usage**: Track fuel usage to optimize queries
3. **Prevent Runaway Queries**: Use fuel limits to prevent resource exhaustion

## Related Documentation

- [JSON-LD Query](jsonld-query.md): JSON-LD Query syntax
- [SPARQL](sparql.md): SPARQL syntax
- [Explain Plans](explain.md): Query execution plans
