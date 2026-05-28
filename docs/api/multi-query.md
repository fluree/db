# Multi-query envelope

> **Endpoint:** `POST /v1/fluree/multi-query`
> **Status:** v1 (envelope contract stable; some features deferred — see [Limitations](#limitations))

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

- Sub-query keys win on conflict.
- Per-sub-query overrides supported in v1: `meta`, `policy`, `identity`,
  `timeoutMs`, `max-fuel`.
- `opts.t` is **rejected inside a multi-query envelope** at any level.
  Pin time via `from` (sub-query level) or `asOf` (envelope level).

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
  "errors": {},
  "meta":   { "fuel_total": 1234.5, "elapsed_ms": 87 }
}
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `status` | `"ok"` \| `"partial"` \| `"all_failed"` | Aggregate over per-alias outcomes. **Clients should branch on this**, not on HTTP status. |
| `snapshot.asOf` | string \| absent | ISO 8601 moment used for resolution. Echoes envelope `asOf` (ISO form) or the server's wall-clock at envelope entry. Absent when envelope used integer `asOf`. |
| `snapshot.ledgers` | object (ledger → integer) | Per-ledger numeric `t` every sub-query observed. **Each value is independent** — see the atomicity caveat above. |
| `results` | object (alias → query result) | Successful sub-queries, keyed by alias. JSON-LD aliases get the JSON-LD query result shape; SPARQL aliases get SPARQL Results JSON. Aliases that errored are absent here. |
| `errors` | object (alias → error entry) | Failed or timed-out sub-queries. Each entry has `code`, `message`, and (for timeouts) `effective_timeout_ms`. Omitted when empty. |
| `meta` | object \| absent | Aggregate fuel / wall-clock elapsed. Included when `opts.meta` is enabled at the envelope level. |

### HTTP status mapping

| HTTP code | Meaning |
|-----------|---------|
| `200` | Envelope parsed, validated, executed. Body's `status` reports the aggregate (`ok` / `partial` / `all_failed`). Per-alias errors and timeouts live inside `errors`. |
| `400` | Envelope validation failed (bounds violation, `asOf` collision, missing `from`, malformed body, history query, envelope `max-fuel`, `maxConcurrency: 0`, etc.). No `results` / `errors` keys — the body is the standard error shape. |
| `401` | Authentication required and missing. |
| `500` | Envelope infrastructure failed: snapshot resolution couldn't load a ledger, response would exceed the configured size cap during assembly, server-side panic. |

---

## Bounds

The server enforces several limits per envelope. The defaults are the
single-tenant server config; production deployments tune these via server
settings.

| Bound | Default | Override surface |
|-------|---------|------------------|
| Max sub-queries / envelope | 64 | static server config only |
| Max distinct ledgers / envelope | 8 | static server config only |
| Max concurrent sub-queries | 16 | static + `opts.maxConcurrency` (clamped to static) |
| Envelope wall deadline | 60_000 ms | static + `opts.timeoutMs` (clamped to static) |
| Per-sub-query timeout | `min(opts.timeoutMs, remaining envelope budget)` | `opts.timeoutMs` per-sub-query or per-envelope |
| Response size | 64 MiB | static server config only |

**Per-sub-query effective timeout** is computed when the sub-query
acquires its concurrency permit, not at envelope entry. A sub-query that
waits 30 s in the permit queue on a 60 s envelope gets ≤30 s of execution
regardless of its own `opts.timeoutMs`. The total wall-clock budget is
the envelope's promise.

When the envelope deadline fires, in-flight sub-queries are cancelled
and reported with `code: "timeout"` in the per-alias errors map. Already
completed sub-queries land in `results` normally.

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

These are explicit v1 scope-cuts. Each has an issue tracking the lift.

- **History queries are not supported inside envelopes.** A JSON-LD body
  with a `to` field or a SPARQL query with `FROM <a@t:1> TO <a@t:latest>`
  is rejected with `400 Bad Request`. History queries span a `t`-range
  rather than a single snapshot, so the envelope's shared-snapshot
  contract doesn't compose meaningfully. Run them as single queries via
  `/query`.
- **Envelope-level fuel budget is not enforced.** `opts.max-fuel` /
  `max_fuel` / `maxFuel` at the envelope level is rejected with
  `400 Bad Request`. Per-sub-query `opts.max-fuel` works unchanged.
  Shared-atomic envelope budget needs a thread-safe budget tracker
  plumbed through every fuel-charge site in the query engine — planned
  for a future release.
- **Cancellation is "Tier B".** When the envelope deadline fires,
  in-flight blocking storage reads complete within a few hundred
  milliseconds (cache hit: <5 ms; remote S3 range read: 50–200 ms)
  before the future is observed as cancelled. No new work starts after
  cancellation. Sub-millisecond cancellation latency requires injecting
  a `CancellationToken` into the index store — planned for a future
  release.
- **`opts.t` is not accepted at any level inside the envelope.** Pin
  time via `from` (e.g., `from: "ledger@t:42"`) or envelope `asOf`.
- **Response size cap is best-effort, not an OOM guard.** Each
  sub-query result is bounded by a per-sub-query post-format check that
  catches a single runaway alias before it contributes to assembly,
  and the assembler enforces the envelope-level cap when stitching
  per-alias results together. But each sub-query still fully
  materializes its result in memory before the per-sub-query check
  fires — peak memory during dispatch is therefore bounded by
  `max_concurrency × max_subquery_response_bytes`, not by the envelope
  cap alone. Per-sub-query streaming serialization with byte-level
  budget back-pressure is planned for a future release.
- **SPARQL sub-queries don't thread bearer identity / server-default
  policy-class.** JSON-LD sub-queries pick these up via
  `apply_auth_identity_to_opts` (same path as single-query `/query`).
  SPARQL sub-queries currently match the single-query connection-scoped
  SPARQL path, which also doesn't thread identity. Identity threading
  for connection-scoped SPARQL will land on both endpoints together so
  the parity stays clean.

---

## See also

- [Query endpoint reference](endpoints.md#post-query) — single-query
  `POST /query`
- [Headers and content types](headers.md)
- [Signed requests (JWS/VC)](signed-requests.md)
- [Errors and status codes](errors.md)
- [SPARQL compliance notes](../contributing/sparql-compliance.md)
