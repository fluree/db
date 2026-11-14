# Query Result Cache

A query can ask to be answered from the result cache. The first run stores its formatted answer;
a later identical request against the same ledger state returns it without executing.

A hit is never stale. The cache key includes the state of the ledger the query reads, so any
commit, index build, or a ledger dropped and created again under the same name produces a new
key. A cached answer is always the one the query would return if it ran now. On a query peer,
that means the answer for the state the peer has replicated.

## Asking for the cache

Caching is per request; nothing is cached unless the request asks.

| Request | How |
|---------|-----|
| HTTP, any query language | `Fluree-Cache: true` header |
| JSON-LD | `"opts": {"cache": true}` |
| Rust API | `.result_cache(&outcome)` on `GraphDb::query()`, then `outcome.status()` |

A request that asks gets an `x-fdb-cache` response header:

| Value | Meaning |
|-------|---------|
| `hit` | Answered from the cache |
| `miss` | Executed; the answer is now cached |
| `bypass; reason="…"` | Executed and not cached, for the reason given |

A hit's body is byte-identical to the uncached response.

## What the key covers

- The query text: SPARQL as sent, JSON-LD as sent apart from `opts.cache`, `opts.meta` and
  `opts.max-fuel`. SPARQL parameters are included.
- The response format: the negotiated output format and its options.
- The ledger state read: ledger, branch, `t`, index, and uncommitted novelty.
- The ledger's default context.
- The effective policy: identity, policy classes, `default-allow`, policy values and the policy
  rules themselves. Two identities share an entry only when the same policy applies to both.

## What bypasses the cache

These queries always execute and report `bypass`:

- Queries that call `NOW()`, `RAND()`, `UUID()`, `STRUUID()` or `BNODE()`, anywhere in the query.
- `SERVICE` to a remote endpoint or a variable endpoint.
- Graph sources (Iceberg, Delta, R2RML/SQL), full-text (BM25) search and vector search.
- Reasoning (`"reasoning"` / `# PRAGMA reasoning` or ledger reasoning defaults).
- Policies with an `f:query` restriction, or policy sourced from another ledger.
- Agent JSON output (`application/vnd.fluree.agent+json`), whose envelope includes the time it
  was written.
- SPARQL `FROM` / `FROM NAMED`.
- Answers too large to cache. The limit is 1/32 of the cache budget.

Tracked requests (`fluree-track-*`, `opts.meta`) and CSV/TSV output are not served from the cache.
Neither are connection-scoped queries (`/v1/fluree/query` without a ledger in the path),
multi-query, GraphQL or Cypher. Those requests get no `x-fdb-cache` header.

## Memory

Cached answers share the server's cache budget (`--cache-max-mb`) with decoded index data. Answers
for a ledger state that later queries no longer read are dropped once newer states replace them.

A hit does no work, so `max-fuel` is not charged for it.
