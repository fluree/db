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
| `REGEX` / `REPLACE` evaluation | 0.001 |
| Hash function (`MD5`, `SHA1`, `SHA256`, `SHA384`, `SHA512`) | 0.001 |
| `UUID` / `STRUUID` | 0.001 |
| `geof:distance` | 0.001 |
| Vector similarity (`DotProduct`, `CosineSimilarity`, `EuclideanDistance`) | 0.002 |
| `Fulltext` (per-row BM25 scoring) | 0.005 |

Cheap operations (comparisons, arithmetic, type checks, simple string ops, datetime extraction, etc.) cost zero — instrumentation overhead would dwarf the actual cost.

The **query floor** guarantees every fuel-tracked query reports at least `1.000` fuel: a query touching no persisted data still costs the floor, and a query that errors during parsing/planning still reports it. I/O "touches" cost `0.010` each, so a scan-dominated query reports roughly `1.000 + 0.010 × (leaflet/dict touches)`. The fuel schedule above is defined in one place — `fluree-db-core/src/tracking.rs` (`tracking::schedule`).

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
