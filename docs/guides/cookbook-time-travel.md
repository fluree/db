# Cookbook: Time Travel

Every transaction in Fluree is immutable. The database preserves complete history automatically — no audit tables, no trigger-based logging, no slowly-changing dimensions.

There are two distinct capabilities, and it's worth keeping them straight:

| | **Time travel** | **History queries** |
|---|---|---|
| Question | "What did the graph look like *at* time T?" | "What *changed* between T1 and T2?" |
| Result | An ordinary graph, as of one moment | A change log: value + `t` + assert/retract |
| Syntax | `@t:` / `@iso:` / `@recorded:` / `@commit:` on a ledger reference | `from` **and** `to`, plus per-value metadata bindings |

Time travel needs nothing special in the query body — you write a normal query and pin the snapshot. History queries need explicit metadata bindings, covered under [History queries](#history-queries) below.

## Time travel

### Query by transaction number

Every transaction increments a counter (`t`). Query data as it was after any transaction:

```bash
# Current state
fluree query 'SELECT ?name ?salary WHERE { ?p schema:name ?name ; ex:salary ?salary }'

# State after transaction 5
fluree query --at 5 'SELECT ?name ?salary WHERE { ?p schema:name ?name ; ex:salary ?salary }'

# State after the very first transaction
fluree query --at 1 'SELECT ?s ?p ?o WHERE { ?s ?p ?o }'
```

### Query by ISO timestamp

Use a timestamp to query the state at a specific moment:

```bash
fluree query --at 2025-01-15T00:00:00Z \
  'SELECT ?name ?email WHERE { ?p schema:name ?name ; schema:email ?email }'
```

Fluree finds the most recent transaction at or before the given timestamp.

This resolves along the **event** axis — the time each commit's changes are *about*, which for backdated imports is not the same as when they were loaded. To ask "what had been loaded by this time?" instead, use `@recorded:` (see [Time Travel Concepts](../concepts/time-travel.md#recorded-time-the-audit-axis-recorded)).

### Query by commit ID

Every commit has a content-addressed ID (CID). Query by exact commit:

```bash
fluree query --at bafyreif... \
  'SELECT ?s ?p ?o WHERE { ?s ?p ?o }'
```

### JSON-LD query with time specifier

In a query body, the time specifier is a suffix on the ledger reference in `from`:

```json
{
  "from": "mydb:main@t:5",
  "select": ["?name"],
  "where": [{"@id": "?p", "schema:name": "?name"}]
}
```

The structured form is equivalent and composes with named-graph selection:

```json
{
  "from": {"@id": "mydb:main", "t": 5},
  "select": ["?name"],
  "where": [{"@id": "?p", "schema:name": "?name"}]
}
```

The suffix forms are `@t:5`, `@t:latest`, `@iso:2025-01-15T00:00:00Z`, `@recorded:2025-01-15T00:00:00Z`, and `@commit:bafyreif...`.

### SPARQL with a time specifier

The `@t:` suffix is parsed on ledger references inside a `FROM` clause, which means SPARQL time travel goes through the **connection-scoped** endpoint:

```sparql
PREFIX schema: <http://schema.org/>

SELECT ?name
FROM <mydb:main@t:5>
WHERE { ?p schema:name ?name }
```

### HTTP API

Ledger-scoped queries put the ledger in the path; time travel rides on the `FROM` clause (SPARQL) or the `from` key (JSON-LD) at the connection-scoped endpoint.

```bash
# Ledger-scoped, current state
curl -X POST 'http://localhost:8090/v1/fluree/query/mydb:main' \
  -H 'Content-Type: application/sparql-query' \
  -d 'SELECT ?s ?p ?o WHERE { ?s ?p ?o }'

# Connection-scoped, pinned to t=5
curl -X POST 'http://localhost:8090/v1/fluree/query' \
  -H 'Content-Type: application/sparql-query' \
  -d 'SELECT ?s ?p ?o FROM <mydb:main@t:5> WHERE { ?s ?p ?o }'

# Connection-scoped JSON-LD, pinned to a timestamp
curl -X POST 'http://localhost:8090/v1/fluree/query' \
  -H 'Content-Type: application/json' \
  -d '{
        "from": "mydb:main@iso:2025-01-15T00:00:00Z",
        "select": ["?s", "?p", "?o"],
        "where": [{"@id": "?s", "?p": "?o"}]
      }'
```

There is no `?t=` URL parameter — a numeric pin like `from: "mydb:main@t:42"` also makes the server observe at least `t=42` before executing, giving read-after-write on a pinned snapshot. See [Time Travel Concepts](../concepts/time-travel.md#consistency-and-read-after-write) for the `fluree-min-t` header.

## History queries

A history query is one with **both** `from` and `to`. It returns a change log rather than a snapshot: every assert and retract in the range, each carrying the transaction time and the operation.

Two things must be bound explicitly, or you get rows with no metadata:

- **`@t`** — the transaction number (integer)
- **`@op`** — a **boolean**: `true` = assert, `false` = retract

### Entity history (CLI)

`fluree history` takes an **entity IRI**, not a query — it builds the history query for you:

```bash
# All changes to an entity
fluree history ex:alice

# Filter to a single predicate
fluree history ex:alice -p ex:salary

# Bound the range
fluree history ex:alice --from 1 --to 5

# Machine-readable
fluree history ex:alice --format json
```

Output (table format renders `op` as `+` / `-`):

```
┌───┬────┬─────────────────────────────┬─────────┐
│ t │ op │ predicate                   │ value   │
├───┼────┼─────────────────────────────┼─────────┤
│ 1 │ +  │ http://example.org/salary   │ 85000   │
│ 4 │ -  │ http://example.org/salary   │ 85000   │
│ 4 │ +  │ http://example.org/salary   │ 95000   │
└───┴────┴─────────────────────────────┴─────────┘
```

Each update produces a retract/assert pair at the same `t`.

For anything beyond a single entity, write the query yourself with `fluree query`.

### Entity history (JSON-LD)

Bind `@t` and `@op` on the value object:

```json
{
  "@context": {"ex": "http://example.org/"},
  "from": "mydb:main@t:1",
  "to": "mydb:main@t:latest",
  "select": ["?prop", "?value", "?t", "?op"],
  "where": [
    {"@id": "ex:alice", "?prop": {"@value": "?value", "@t": "?t", "@op": "?op"}}
  ],
  "orderBy": ["?t"]
}
```

### Entity history (SPARQL)

SPARQL uses the `FROM ... TO ...` range plus RDF-star quoted triples to bind the metadata. `f:t` and `f:op` on a quoted triple extract the flake metadata for that triple's object:

```sparql
PREFIX ex: <http://example.org/>
PREFIX f: <https://ns.flur.ee/db#>

SELECT ?value ?t ?op
FROM <mydb:main@t:1>
TO <mydb:main@t:latest>
WHERE {
  << ex:alice ex:salary ?value >> f:t ?t ; f:op ?op .
}
ORDER BY ?t
```

Results:

```
?value   ?t  ?op
85000    1   true      ← initial salary
85000    4   false     ← old value retracted
95000    4   true      ← new value asserted
```

### Find when a value changed

```bash
fluree history ex:alice -p ex:salary
```

or, across every subject with that property:

```json
{
  "@context": {"ex": "http://example.org/"},
  "from": "mydb:main@t:1",
  "to": "mydb:main@t:latest",
  "select": ["?person", "?salary", "?t", "?op"],
  "where": [
    {"@id": "?person", "ex:salary": {"@value": "?salary", "@t": "?t", "@op": "?op"}}
  ],
  "orderBy": ["?t"]
}
```

### Show only retractions

`@op` is a boolean. The constant shorthand filters without binding a variable:

```json
{
  "@context": {"ex": "http://example.org/"},
  "from": "mydb:main@t:1",
  "to": "mydb:main@t:latest",
  "select": ["?salary", "?t"],
  "where": [
    {"@id": "ex:alice", "ex:salary": {"@value": "?salary", "@t": "?t", "@op": false}}
  ]
}
```

In SPARQL, filter the bound variable:

```sparql
FILTER(?op = false)
```

### Scope a range

Bound the range with `from` / `to` rather than filtering on `?t` after the fact — the endpoints are what drive the scan:

```json
{
  "@context": {"ex": "http://example.org/"},
  "from": "mydb:main@t:10",
  "to": "mydb:main@t:15",
  "select": ["?s", "?p", "?v", "?t", "?op"],
  "where": [
    {"@id": "?s", "?p": {"@value": "?v", "@t": "?t", "@op": "?op"}}
  ],
  "orderBy": ["?t"]
}
```

ISO timestamps work as endpoints too: `"from": "mydb:main@iso:2025-01-01T00:00:00Z"`.

## Patterns

### Point-in-time comparison

```bash
# Before the change (t=5)
fluree query --at 5 'SELECT ?salary WHERE { ex:alice ex:salary ?salary }'

# After the change (t=6)
fluree query --at 6 'SELECT ?salary WHERE { ex:alice ex:salary ?salary }'
```

### Compliance snapshot

Generate a report of all data as it existed on a specific date:

```bash
fluree query --at 2025-06-30T23:59:59Z --format csv \
  'PREFIX schema: <http://schema.org/>
   PREFIX ex: <http://example.org/>

   SELECT ?name ?department ?role
   WHERE {
     ?person a schema:Person ;
             schema:name ?name ;
             ex:department ?department ;
             ex:role ?role .
   }
   ORDER BY ?department ?name' > compliance-report-2025-Q2.csv
```

This is a reproducible snapshot — the same query with the same timestamp always returns the same results.

### Recover deleted data

Data that was retracted still exists in history:

```bash
# Carol was deleted at t=8. Recover her data from t=7:
fluree query --at 7 'SELECT ?prop ?value WHERE { ex:carol ?prop ?value }'
```

To restore, re-insert the data from the historical query.

### Multi-ledger time travel

Each ledger reference in `from` carries its own time specifier, so you can join two ledgers at different moments — useful for price-at-time-of-purchase analysis:

```json
{
  "@context": {"ex": "http://example.org/", "schema": "http://schema.org/"},
  "from": ["catalog:main@t:10", "orders:main@t:25"],
  "select": ["?product", "?price", "?qty"],
  "where": [
    {"@id": "?order", "ex:product": "?p", "ex:quantity": "?qty"},
    {"@id": "?p", "schema:name": "?product", "schema:price": "?price"}
  ]
}
```

For named-graph scoping alongside a time pin, use the structured form — `{"@id": "catalog:main", "t": 10, "graph": "..."}`. See [Datasets and named graphs](../query/datasets.md).

### Transaction metadata

Every commit records metadata in the built-in `txn-meta` named graph, addressed with a `#txn-meta` fragment on the ledger reference:

```sparql
PREFIX f: <https://ns.flur.ee/db#>

SELECT ?t ?timestamp ?author
FROM <mydb:main#txn-meta>
WHERE {
  ?commit f:t ?t ;
          f:time ?timestamp .
  OPTIONAL { ?commit f:author ?author }
}
ORDER BY DESC(?t)
LIMIT 10
```

`f:receivedAt` is also present on ledgers that dual-stamp (see [Recorded time](../concepts/time-travel.md#recorded-time-the-audit-axis-recorded)).

## Gotchas

**Selecting `?t` / `?op` without binding them silently returns nulls.**
This is the most common mistake. A history query that selects `?t` and `?op` but never binds them via `@t`/`@op` (JSON-LD) or `f:t`/`f:op` (SPARQL) succeeds, returns history rows, and leaves both columns null — so asserts and retracts are indistinguishable. There is no error or warning. Always bind the metadata.

**`@op` / `f:op` is a boolean, not a string.**
`true` = assert, `false` = retract. `"assert"` and `"retract"` are not accepted and match nothing. The CLI's *table* renderer displays `+` / `-`, but the underlying value in JSON output is `true` / `false`.

**A history query needs both `from` and `to`.**
With only `from`, you get a point-in-time snapshot, and the metadata bindings have nothing to report.

**SPARQL `FROM ... TO ...` is connection-scoped only.**
It is rejected on `/v1/fluree/query/{ledger}` and on the streaming endpoint `/v1/fluree/stream/query`. Post to `/v1/fluree/query` instead.

**The quoted triple's object must be a variable.**
`<< ex:alice ex:name ?name >> f:t ?t` is valid; `<< ex:alice ex:name "Alice" >> f:t ?t` is rejected — there is no object binding to attach metadata to.

**`<< s p o >>` and `<<( s p o )>>` are different things.**
The bare form is this Fluree-specific flake-metadata construct. The parenthesized triple term is RDF 1.2 reification, valid only as the object of `rdf:reifies`. They do not compose — see [Edge annotations](../concepts/edge-annotations.md).

**Reasoning is rejected in history mode.**
A history-range query that requests reasoning returns an error rather than silently dropping derived facts.

**History is not available locally for tracked ledgers.**
A tracked ledger has no local commit chain; use `--remote <name>` to query the upstream.

## Common questions

**Is time travel expensive?**
No. Querying a historical state uses the same indexes as querying the current state. The cost is O(log n) for index lookups. `@t:` is cheapest (no resolution); `@iso:` / `@recorded:` cost a binary search over commit timestamps; `@commit:` costs a bounded scan.

**Does old data use extra storage?**
Yes — immutability means retracted values are preserved. Storage grows with the number of changes, not just the current state size. For most workloads this is negligible.

**Can I delete history?**
No. Immutability is a core guarantee. If you need to remove data for compliance (e.g., GDPR right to erasure), contact the Fluree team about data compaction options.

## Related documentation

- [Time Travel Concepts](../concepts/time-travel.md) — Architecture, event vs. recorded axis, read-after-write
- [SPARQL Reference](../query/sparql.md#history-queries) — RDF-star history syntax
- [JSON-LD Query](../query/jsonld-query.md) — `@t` / `@op` annotations
- [Datasets and named graphs](../query/datasets.md) — Structured `from`, `txn-meta`
- [fluree history](../cli/history.md) — CLI reference
