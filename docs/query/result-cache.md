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
  rules themselves. Each identity has entries of its own; requests without an identity share
  entries when the same policy applies to them.

## What bypasses the cache

These queries always execute, and report `bypass` where the request reaches the cache (see
[below](#not-served-from-the-cache) for HTTP requests that do not):

- Queries that call `NOW()`, `RAND()`, `UUID()`, `STRUUID()` or `BNODE()`, anywhere in the query.
- `SERVICE` to a remote endpoint or a variable endpoint.
- Graph sources (Iceberg, Delta, R2RML/SQL), full-text (BM25) search and vector search.
- Reasoning (`"reasoning"` / `# PRAGMA reasoning` or ledger reasoning defaults).
- Policies with an `f:query` restriction, or policy sourced from another ledger.
- Agent JSON output (`application/vnd.fluree.agent+json`), whose envelope includes the time it
  was written.
- SPARQL `FROM` / `FROM NAMED`.
- Answers too large to cache. One answer may take at most 1/16 of the
  [result-cache budget](#memory).
- Any request, when the result cache is disabled (`query_result_cache_max_mb = 0`).

## Not served from the cache

These requests execute without consulting the cache and get no `x-fdb-cache` header:

- Tracked requests (`fluree-track-*`, `opts.meta`) and CSV/TSV output.
- Connection-scoped queries (`/v1/fluree/query` without a ledger in the path), multi-query, GraphQL
  and Cypher.
- Over HTTP, requests that carry an identity or policy inputs (a verified identity,
  `fluree-identity`, `fluree-policy*` headers, or `opts.identity` / `opts.policy*`). The
  identity-keyed entries above are reachable only from the Rust API for now.
- Over HTTP, SPARQL `FROM` / `FROM NAMED` and queries against a graph-source alias.

A query peer in proxy-storage mode does not report the cache yet: a JSON-LD request that asks is
served from the cache with no `x-fdb-cache` header, and a SPARQL request is not cached.

## Memory

Cached answers have a budget of their own, `query_result_cache_max_mb`. It is separate from the
index cache budget (`cache_max_mb`) and in addition to it, so caching answers never shrinks the
cache that ordinary queries read index data from.

The budget is a ceiling, not a reservation. The pool that holds answers is created by the first
request that opts in, and it grows only as answers are stored. A server where no request ever
opts in allocates nothing for it, whatever the budget is set to.

The default is 10% of `cache_max_mb`. When sizing a server against a memory limit, count both:
the process can hold up to `cache_max_mb` of index data plus `query_result_cache_max_mb` of
answers. Set the budget to `0` to turn result caching off; requests that ask for it then report
`bypass; reason="result cache disabled"`.

When the pool is full, the least recently used answers are dropped first. An answer for a ledger
state that queries no longer read is never looked up again, so it is among the first to go.

| Where | Setting |
|-------|---------|
| Server flag | `--query-result-cache-max-mb 200` |
| Environment | `FLUREE_QUERY_RESULT_CACHE_MAX_MB=200` |
| Config file (`[server]`) | `query_result_cache_max_mb = 200` |
| JSON-LD connection config | `"queryResultCacheMaxMb": 200` on the `Connection` node (accepts `ConfigurationValue`) |
| Rust | `FlureeBuilder::query_result_cache_max_mb(200)` |

Server flags, environment variables and the config file override the connection config, as they
do for `cache_max_mb`. See [Configuration](../operations/configuration.md#cache-size).

Once a request has opted in, the server's once-a-minute cache stats log gains a
`Query result cache stats` line with the entry count, the bytes held and the budget.

A hit does no work, so `max-fuel` is not charged for it.
