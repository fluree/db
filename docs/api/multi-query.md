# Multi-query envelope

> **Endpoint:** `POST /v1/fluree/multi-query`
> **Status:** envelope contract stable; some features explicitly out of scope — see [Limitations](#limitations).

Bundle multiple independent queries — JSON-LD, SPARQL, or both — into a single
HTTP request that runs them in parallel against one shared snapshot moment.
The response carries per-alias results, per-alias errors, and an explicit
record of the per-ledger transaction (`t`) every sub-query observed.

## When to use it

- You want N queries to see the **same data** without coordinating two round
  trips ("get `t`, then issue N pinned queries").
- You want **parallel execution** without standing up N concurrent HTTP
  requests (in-process cache sharing wins over N independent connections).
- You want a **single signed-request boundary** when working with JWS/VC.

## When *not* to use it

- The queries don't need to share a snapshot — N concurrent single-query
  requests are simpler and have looser per-query bounds.
- You need **history range queries** (`FROM <a@t:1> TO <a@t:latest>` or
  JSON-LD `to`) — those are rejected in the envelope (see
  [Limitations](#limitations)).
- You need a **shared envelope fuel budget** — currently each sub-query
  carries its own `max-fuel` (see [Limitations](#limitations)).

---

## Request envelope

```json
{
  "@context": { "schema": "http://schema.org/" },
  "asOf":     "2024-01-01T12:00:00Z",
  "opts":     { "meta": true, "timeoutMs": 30000, "maxConcurrency": 8 },
  "queries": {
    "alice": {
      "language": "jsonld",
      "query": {
        "from":   "myledger:main",
        "select": ["?name"],
        "where":  { "@id": "?p", "schema:name": "?name" }
      }
    },
    "bob": {
      "language": "sparql",
      "query":    "SELECT ?name FROM <other:main> WHERE { ?p schema:name ?name }"
    }
  }
}
```

### Top-level fields

| Field | Required | Type | Purpose |
|-------|----------|------|---------|
| `queries` | **yes** | object (alias → sub-query) | Independent sub-queries to run. Alias keys become keys in `results` / `errors`. |
| `@context` | no | object | Envelope-level JSON-LD context. Merged into each sub-query (see [Merge rules](#merge-rules)). |
| `asOf` | no | integer or ISO 8601 string | Shared snapshot pin (see [Snapshot semantics](#snapshot-semantics)). |
| `opts` | no | object | Envelope-default opts. Merged into each sub-query (see [Merge rules](#merge-rules)). |

### Per-sub-query fields

| Field | Required | Type | Purpose |
|-------|----------|------|---------|
| `language` | **yes** | `"jsonld"` or `"sparql"` (alias `"json-ld"` accepted) | Selects the parser. |
| `query` | **yes** | object (jsonld) or string (sparql) | Query body. Carries its own `from` — each sub-query specifies its own dataset. |
| `opts` | no | object | Per-sub-query overrides. Merged onto envelope opts with sub-query winning on conflict. `opts.t` is **not allowed** here — pin time via `from` or envelope `asOf`. |

---

## Merge rules

Two rules cover the entire "what shadows what" question:

1. **Mergeable** (`@context`, `opts`): **shallow merge**, sub-query wins on
   key conflict. Envelope serves as the default.
2. **Temporal pin** (`asOf` vs any inner `@t:` / `t` field / SPARQL
   `FROM <ledger@t:...>`): **collision is an error**, never a silent
   override.

### `@context` inheritance

- Sub-query has no `@context`: inherits the envelope context unchanged.
- Sub-query has an `@context` object: shallow-merged onto the envelope
  context. Sub-query keys win on conflict.
- Sub-query has `@context: null`: explicit reset — that sub-query runs
  with **no** context.

For **SPARQL** sub-queries, the envelope context contributes to the query
in two ways:

- **`PREFIX` injection** — if the SPARQL query has *zero* `PREFIX`
  declarations of its own, every prefix-shaped entry of the merged
  envelope context is injected as a `PREFIX` declaration. The injection
  is per-directive-class and all-or-nothing: declaring even a single
  `PREFIX` in your SPARQL turns off envelope `PREFIX` injection (but not
  `BASE` injection — those are decided independently).
- **`BASE` injection** — same all-or-nothing rule applied to the
  envelope's `@base`.

Only prefix-shaped entries (key is a valid SPARQL `PN_PREFIX`, value is a
namespace IRI ending with `#` or `/` or containing `://`) are eligible.
JSON-LD term aliases like `"name": "schema:name"` are not — they would
produce invalid SPARQL.

### `opts` merge

Opts come from three layers, merged shallowly with **the most specific layer winning** on key conflict:

| Layer | Where it lives | Precedence |
|-------|----------------|------------|
| 1. Envelope opts | top-level `opts` on the envelope | lowest — applies as defaults to every sub-query |
| 2. Sub-query opts | `opts` on each entry of the `queries` map (the wrapper around `language` + `query`) | middle — wins over envelope |
| 3. Query body opts | `opts` *inside* the JSON-LD query body (`sub.query["opts"]`) | **highest — wins over both** |

This precedence is security-relevant. The HTTP server's bearer/identity gate runs against the **pre-merged** opts and writes its decision into the body layer (layer 3), where the dispatcher's merge then makes it the final word. Inverting the order would let an envelope-level or sub-query-level `opts.identity` clobber the gate's decision.

- Per-sub-query overrides recognised at any layer: `meta`, `policy`, `policy-class`, `policy-values`, `identity`, `default-allow`, `timeoutMs`, `max-fuel`, `min-t` / `minT`.
- `opts.min-t` / `opts.minT` requests read-after-write freshness before the envelope snapshot is resolved. Envelope-level values apply to every referenced ledger; sub-query values apply to that sub-query's ledger(s). The `Fluree-Min-T` header provides the same guarantee as an envelope default.
- `opts.t` is **rejected at every layer** inside a multi-query envelope (envelope opts, sub-query opts, AND body opts). Pin time via `from` (per sub-query) or envelope `asOf`.
- Envelope-level `opts.max-fuel` is rejected up-front (see [Limitations](#limitations)); per-sub-query `opts.max-fuel` (in either layer 2 or layer 3) is honoured.

---

## Snapshot semantics

When the server processes an envelope, it resolves the snapshot **once**
at envelope entry, then uses that resolved per-ledger `t` map for every
unpinned sub-query.

| `asOf` value | Behavior |
|--------------|----------|
| omitted | Server captures wall-clock "now" once at envelope entry, then pins each distinct ledger to its current `t`. The captured moment is echoed back in `snapshot.asOf` so the request is reproducible. |
| integer (e.g. `"asOf": 42`) | The envelope must reference **exactly one ledger** — the integer pin is applied directly. Multi-ledger envelopes paired with an integer `asOf` are rejected with `400 Bad Request`. |
| ISO 8601 string (e.g. `"asOf": "2024-01-01T12:00:00Z"`) | Each distinct ledger resolves to its latest commit at or before that moment. Different ledgers may receive different numeric `t` values. |

> **Atomicity caveat.** `asOf` provides **shared time resolution**, not
> distributed atomicity. Different ledgers commit on independent clocks,
> so two ledgers' "as of this moment" `t` values reflect each ledger's
> latest commit independently — they are not synchronized across ledgers.
> The response's `snapshot.ledgers` map exposes exactly what each
> sub-query observed.

### Temporal collision

When `asOf` is set, no sub-query may carry its own temporal pin. The
following are all collision errors and reject the envelope with
`400 Bad Request`:

- JSON-LD `from: "ledger@t:42"`
- JSON-LD `from: { "@id": "ledger", "t": 42 }`
- JSON-LD `from: { "@id": "ledger", "at": "commit:abc123" }`
- JSON-LD body with an inner `t` field
- SPARQL `FROM <ledger@t:42>` / `FROM <ledger@iso:...>` /
  `FROM <ledger@commit:...>`

This rule is intentional: the alternative (silent inner-wins override)
made it too easy to query a different snapshot than you asked for.

When `asOf` is **omitted**, inner temporal pins are allowed and the
envelope is still atomic per the rule above (snapshot resolved once at
envelope entry).

---

## Response shape

```json
{
  "status":  "ok",
  "snapshot": {
    "asOf":    "2024-01-01T12:00:00.000Z",
    "ledgers": { "myledger:main": 1042, "other:main": 87 }
  },
  "results": {
    "alice": [ { "name": "Alice" } ],
    "bob":   { "head": { "vars": ["name"] }, "results": { "bindings": [...] } }
  },
  "tracking": {
    "alice": { "time": "5ms", "fuel": 1024.0 },
    "bob":   { "time": "3ms", "fuel": 210.5 }
  },
  "meta": { "fuel_total": 1234.5, "elapsed_ms": 87 }
}
```

> **Field omission:** `errors` is omitted from the response when no sub-query failed (zero entries → field skipped, not emitted as `{}`). `tracking` is omitted when no sub-query ran with tracking enabled. `meta` is omitted when `opts.meta` isn't set at the envelope level. `snapshot.asOf` is omitted when the envelope used an integer `asOf` (no shared wall-clock interpretation). Examples below show only the fields that would appear in each scenario.

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `status` | `"ok"` \| `"partial"` \| `"all_failed"` | Aggregate over per-alias outcomes. **Clients should branch on this**, not on HTTP status. |
| `snapshot.asOf` | string \| absent | ISO 8601 moment used for resolution. Echoes envelope `asOf` (ISO form) or the server's wall-clock at envelope entry. Absent when envelope used integer `asOf`. |
| `snapshot.ledgers` | object (ledger → integer) | Per-ledger numeric `t` every sub-query observed. **Each value is independent** — see the atomicity caveat above. |
| `results` | object (alias → query result) | Successful sub-queries, keyed by alias. JSON-LD aliases get the JSON-LD query result shape; SPARQL aliases get SPARQL Results JSON. Aliases that errored are absent here. |
| `errors` | object (alias → error entry) | Failed or timed-out sub-queries. Each entry has `code`, `message`, and (for timeouts) `effective_timeout_ms`. Omitted when empty. |
| `tracking` | object (alias → tally) | Per-alias telemetry (`time`, `fuel`, `policy`) for each sub-query that ran with tracking enabled. Mirrors single-query `/query`'s tracked-response shape, one entry per alias. Indexed by alias and ordered to match `results`. Omitted when no sub-query tracked. |
| `meta.fuel_total` | number \| absent | Envelope-level rollup of per-alias fuel. Sum across every sub-query that tracked. Included when envelope `opts.meta` is enabled. |
| `meta.elapsed_ms` | number \| absent | Envelope wall-clock duration (entry → response assembly). Included when envelope `opts.meta` is enabled. Note: this is the envelope wall-clock, not a sum across parallel sub-queries — sub-queries run concurrently, so summing per-alias `time` would over-count. |

#### Tracking detail

Each entry in `tracking` mirrors the single-query [`TrackedQueryResponse`](endpoints.md#post-query) shape with three optional siblings:

- `time` — formatted execution time string (e.g., `"5ms"`).
- `fuel` — decimal fuel consumed.
- `policy` — per-policy `{ executed, allowed }` stats when policy tracking is requested.

Which siblings populate depends on what the sub-query's merged opts requested (`opts.meta: true` enables all three; selective `opts.meta: { time: true, fuel: true }` enables a subset; `opts.max-fuel: N` implicitly enables fuel tracking).

A sub-query whose opts didn't enable tracking will not appear in the `tracking` map at all — making it easy for tracking-unaware clients to ignore the field entirely.

### HTTP status mapping

| HTTP code | Meaning |
|-----------|---------|
| `200` | Envelope parsed, validated, executed. Body's `status` reports the aggregate (`ok` / `partial` / `all_failed`). Per-alias errors and timeouts live inside `errors`. |
| `400` | Envelope validation failed (bounds violation, `asOf` collision, missing `from`, malformed body, history query, envelope `max-fuel`, `maxConcurrency: 0`, etc.). No `results` / `errors` keys — the body is the standard error shape. |
| `401` | Authentication required and missing. |
| `500` | Envelope infrastructure failed: snapshot resolution couldn't load a ledger, response would exceed the configured size cap during assembly, server-side panic. |

---

## Bounds

The server enforces several limits per envelope.

> The "Override surface" column below lists the request-side knobs that already work (`opts.maxConcurrency`, `opts.timeoutMs`). The other limits are compile-time defaults (`MultiQueryBounds::DEFAULT`) and not server-tunable today.

| Bound | Value | Override surface |
|-------|-------|------------------|
| Max sub-queries / envelope | 64 | request cannot override |
| Max distinct ledgers / envelope | 8 | request cannot override |
| Max concurrent sub-queries | 16 | `opts.maxConcurrency` (clamped to 16) |
| Envelope wall deadline | 60_000 ms | `opts.timeoutMs` (clamped to 60_000) |
| Per-sub-query timeout | `min(opts.timeoutMs, remaining envelope budget)` | `opts.timeoutMs` per-sub-query or per-envelope |
| Response size | 64 MiB | request cannot override |

**Per-sub-query effective timeout** is computed when the sub-query
acquires its concurrency permit, not at envelope entry. A sub-query that
waits 30 s in the permit queue on a 60 s envelope gets ≤30 s of execution
regardless of its own `opts.timeoutMs`. The total wall-clock budget is
the envelope's promise.

When the envelope deadline fires, in-flight sub-queries are cancelled
and reported with `code: "timeout"` in the per-alias errors map. Already
completed sub-queries land in `results` normally.

---

## Output formatting

By default, each alias formats with its language's natural shape: JSON-LD
sub-queries emit JSON-LD, SPARQL sub-queries emit SPARQL 1.1 Results JSON.

### In-process (Rust builder)

The builder accepts an envelope-wide `FormatterConfig` via `.format(...)`
matching the single-query `fluree.query_from().format(...)` vocabulary.

```rust
use fluree_db_api::FormatterConfig;

let response = fluree
    .multi_query()
    .envelope(envelope)
    .format(FormatterConfig::typed_json().with_normalize_arrays())
    .execute()
    .await?;
```

The envelope's `results` map is always JSON, so only JSON-producing
formats are accepted. Their treatment differs by design — cross-language
shapes apply to every alias, JSON-LD applies only to JSON-LD aliases,
non-JSON formats are rejected up front:

| Format | Builder method | JSON-LD aliases | SPARQL aliases |
|--------|----------------|-----------------|----------------|
| Typed JSON | `FormatterConfig::typed_json()` | applies | **applies** (cross-language typed shape) |
| SPARQL Results JSON | `FormatterConfig::sparql_json()` | applies | **applies** (cross-language SPARQL Results shape) |
| Agent JSON | `FormatterConfig::agent_json()` | applies | **applies** (cross-language agent envelope; honours `with_max_bytes(...)`) |
| JSON-LD | `FormatterConfig::jsonld()` | applies | **skipped** — SPARQL Results JSON default kept |
| TSV / CSV / SPARQL XML / RDF/XML | (any non-JSON `OutputFormat`) | rejected at `.execute()` with `MultiQueryError::UnsupportedFormat` | rejected |

The "JSON-LD applies only to JSON-LD aliases" rule keeps `--normalize-arrays`
(which builds `FormatterConfig::jsonld().with_normalize_arrays()`) from
silently coercing SPARQL `SELECT` results out of SPARQL Results JSON. If
you want a unified shape across both languages, pick `TypedJson`,
`SparqlJson`, or `AgentJson` instead.

Non-JSON formats (TSV, CSV, SPARQL XML, RDF/XML) are rejected at
`.execute()` time with `MultiQueryError::UnsupportedFormat` — a multi-query
envelope can't embed byte/string payloads inside its JSON `results` map.

### Over HTTP

The HTTP handler picks a format from request headers in this precedence
order (most specific wins):

1. **`Fluree-Output-Format` header** — explicit per-alias selector.
   `json` keeps per-language defaults; `typed-json` selects
   [`FormatterConfig::typed_json`]. Unknown values return `400 Bad
   Request`. This is what the CLI's `--format` flag sends on the wire.
2. **`Fluree-Normalize-Arrays: true`** — layers array normalization on
   the chosen format (or on the default JSON-LD shape when no
   `Fluree-Output-Format` is set). Matches the CLI's
   `--normalize-arrays`.
3. **`Accept` header** — standard content negotiation.
   `application/vnd.fluree.agent+json` selects
   [`FormatterConfig::agent_json`] (honouring `Fluree-Max-Bytes` if set).

`Accept` values that produce byte/string payloads — `text/tab-separated-values`,
`text/csv`, `application/sparql-results+xml`, `application/rdf+xml` —
are rejected with **406 Not Acceptable** since they can't be embedded
inside the envelope's JSON `results` map.

| Header configuration | Effect |
|----------------------|--------|
| (none) | Per-language defaults (JSON-LD aliases → JSON-LD, SPARQL aliases → SPARQL JSON). |
| `Fluree-Output-Format: json` | Same as default. |
| `Fluree-Output-Format: typed-json` | All aliases format as typed JSON. |
| `Fluree-Normalize-Arrays: true` | Default JSON-LD shape with single-value properties wrapped in arrays (applies to JSON-LD aliases). |
| `Fluree-Output-Format: typed-json` + `Fluree-Normalize-Arrays: true` | Typed JSON with array normalization. |
| `Accept: application/vnd.fluree.agent+json` | All aliases format as Agent JSON. `Fluree-Max-Bytes` sets per-alias byte budget. |
| `Accept: text/csv`, `Accept: application/rdf+xml`, etc. | **406 Not Acceptable**. |

---

## Examples

### Minimal — two JSON-LD queries, no envelope settings

```json
{
  "queries": {
    "all_people": {
      "language": "jsonld",
      "query": {
        "@context": { "ex": "http://example.org/" },
        "from":     "myledger",
        "select":   ["?id", "?name"],
        "where":    { "@id": "?id", "ex:type": "ex:Person", "ex:name": "?name" }
      }
    },
    "all_orders": {
      "language": "jsonld",
      "query": {
        "@context": { "ex": "http://example.org/" },
        "from":     "myledger",
        "select":   ["?id", "?total"],
        "where":    { "@id": "?id", "ex:type": "ex:Order",  "ex:total": "?total" }
      }
    }
  }
}
```

### Shared `@context` lifted to envelope

```json
{
  "@context": { "ex": "http://example.org/" },
  "queries": {
    "all_people": {
      "language": "jsonld",
      "query": {
        "from":   "myledger",
        "select": ["?id", "?name"],
        "where":  { "@id": "?id", "ex:type": "ex:Person", "ex:name": "?name" }
      }
    },
    "all_orders": {
      "language": "jsonld",
      "query": {
        "from":   "myledger",
        "select": ["?id", "?total"],
        "where":  { "@id": "?id", "ex:type": "ex:Order", "ex:total": "?total" }
      }
    }
  }
}
```

### Mixed-language with envelope `@context`

The envelope `@context` lifts to **both** JSON-LD sub-queries (as JSON-LD
context inheritance) and SPARQL sub-queries (as `PREFIX` injection — the
SPARQL query below has no `PREFIX` of its own, so envelope prefixes are
injected).

```json
{
  "@context": { "ex": "http://example.org/" },
  "queries": {
    "by_jsonld": {
      "language": "jsonld",
      "query": {
        "from": "myledger",
        "select": ["?name"],
        "where": { "@id": "?p", "ex:name": "?name" }
      }
    },
    "by_sparql": {
      "language": "sparql",
      "query": "SELECT ?name FROM <myledger> WHERE { ?p ex:name ?name }"
    }
  }
}
```

### Per-sub-query opts override

`opts.meta` is enabled at the envelope so every sub-query reports
tracking, but `bob` overrides with a tighter per-sub-query timeout.

```json
{
  "opts": { "meta": true, "timeoutMs": 30000 },
  "queries": {
    "alice": {
      "language": "jsonld",
      "query": { "from": "myledger", "select": ["?name"],
                 "where": { "@id": "?p", "ex:name": "?name" } }
    },
    "bob": {
      "language": "jsonld",
      "query": { "from": "myledger", "select": ["?age"],
                 "where": { "@id": "?p", "ex:age": "?age" } },
      "opts": { "timeoutMs": 5000 }
    }
  }
}
```

### Pinning to a wall-clock moment across ledgers

```json
{
  "asOf": "2024-01-01T12:00:00Z",
  "queries": {
    "users": {
      "language": "jsonld",
      "query": { "from": "users:main",
                 "select": ["?name"],
                 "where": { "@id": "?u", "ex:name": "?name" } }
    },
    "orders": {
      "language": "jsonld",
      "query": { "from": "orders:main",
                 "select": ["?id"],
                 "where": { "@id": "?o", "ex:orderId": "?id" } }
    }
  }
}
```

Response (abbreviated):

```json
{
  "status": "ok",
  "snapshot": {
    "asOf":    "2024-01-01T12:00:00Z",
    "ledgers": { "users:main": 4108, "orders:main": 9421 }
  },
  "results": { "users": [ ... ], "orders": [ ... ] }
}
```

Each ledger's `t` is its latest commit at or before `2024-01-01T12:00:00Z`
— neither value is "the same `t`" because that's not a meaningful
concept across ledgers.

### Partial failure

```json
{
  "queries": {
    "good": {
      "language": "jsonld",
      "query": { "from": "myledger", "select": ["?name"],
                 "where": { "@id": "?p", "ex:name": "?name" } }
    },
    "bad": {
      "language": "sparql",
      "query":    "SELECT ?x FROM <myledger> WHERE { this is not SPARQL }"
    }
  }
}
```

Response (HTTP 200):

```json
{
  "status":   "partial",
  "snapshot": { "asOf": "...", "ledgers": { "myledger": 42 } },
  "results":  { "good": [ ... ] },
  "errors":   {
    "bad": { "code": "api_error", "message": "SPARQL parse error: ..." }
  }
}
```

---

## Limitations

The following are not supported and produce documented behaviour rather than silent partial success.

- **History queries are rejected.** A JSON-LD body with a `to` field or a SPARQL query with `FROM <a@t:1> TO <a@t:latest>` is rejected with `400 Bad Request`. History spans a `t`-range; the envelope's shared-snapshot contract has no meaning over a range. Use single queries against `/query` for history.
- **Envelope-level fuel budget is rejected.** `opts.max-fuel` / `max_fuel` / `maxFuel` at the envelope level is rejected with `400 Bad Request` because the dispatcher does not currently share a fuel budget across parallel sub-queries. Per-sub-query `opts.max-fuel` works unchanged.
- **Cancellation latency is bounded by in-flight storage reads.** When the envelope deadline fires, the dispatcher aborts the `JoinSet`; in-flight blocking storage reads still complete before the dropped future is observed (cache hit: under 5 ms; remote S3 range read: up to a few hundred ms). No new work starts after cancellation.
- **`opts.t` is rejected at every level inside the envelope.** Pin time via `from` (e.g., `from: "ledger@t:42"`) or envelope `asOf`.
- **Response size cap is enforced at assembly, not throughout dispatch.** Each sub-query result is checked against the per-sub-query cap once after it returns, and the assembler enforces the envelope-level cap as it stitches the response. Peak memory during dispatch is bounded by `max_concurrency × max_subquery_response_bytes`, which can exceed the envelope cap while individual sub-queries are running.
- **SPARQL sub-queries do not consume merged policy opts (identity, policy-class, policy, policy-values, default-allow).** The headers ride through the transport, the server folds them into the envelope's top-level `opts`, and the envelope → sub-query opts merge carries them into each sub-query's opts — but the connection-scoped SPARQL dispatch path (`query_from().sparql()`) does not read body opts. JSON-LD sub-queries get full policy threading via `apply_auth_identity_to_opts`; SPARQL sub-queries observe bearer ledger-scope only. This is the same gap that exists for single-query connection-scoped SPARQL (`POST /query` with `Content-Type: application/sparql-query` and an inline `FROM`).
- **Output formats are limited to JSON-producing shapes.** The envelope always assembles a JSON response body, so TSV, CSV, SPARQL Results XML, and RDF/XML are not available per-alias. Use single queries against `/query` when you need a byte/string payload.

---

## See also

- [Query endpoint reference](endpoints.md#post-query) — single-query
  `POST /query`
- [`fluree multi-query` CLI](../cli/multi-query.md)
- [Headers and content types](headers.md)
- [Signed requests (JWS/VC)](signed-requests.md)
- [Errors and status codes](errors.md)
- [SPARQL compliance notes](../contributing/sparql-compliance.md)
