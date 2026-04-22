# Cookbook: Time Travel

Every transaction in Fluree is immutable. The database preserves complete history automatically — no audit tables, no trigger-based logging, no slowly-changing dimensions. This guide covers practical patterns for using time travel.

## Basics

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

### Query by commit ID

Every commit has a content-addressed ID (CID). Query by exact commit:

```bash
fluree query --at bafyreif... \
  'SELECT ?s ?p ?o WHERE { ?s ?p ?o }'
```

### HTTP API

```bash
# By transaction number
curl -X POST 'http://localhost:8090/v1/fluree/query?ledger=mydb:main&t=5' \
  -H "Content-Type: application/sparql-query" \
  -d 'SELECT ?s ?p ?o WHERE { ?s ?p ?o }'

# By timestamp (URL-encoded)
curl -X POST 'http://localhost:8090/v1/fluree/query?ledger=mydb:main&t=2025-01-15T00%3A00%3A00Z' \
  -H "Content-Type: application/sparql-query" \
  -d 'SELECT ?s ?p ?o WHERE { ?s ?p ?o }'
```

### JSON-LD query with time specifier

```json
{
  "from": "mydb:main@t:5",
  "select": ["?name"],
  "where": [{"@id": "?p", "schema:name": "?name"}]
}
```

## Patterns

### Audit trail: who changed what

View the history of changes to a specific entity:

```bash
fluree history 'PREFIX schema: <http://schema.org/>
PREFIX ex: <http://example.org/>

SELECT ?prop ?value ?t ?op WHERE {
  ex:alice ?prop ?value .
}'
```

Each result includes:
- `?t` — the transaction number
- `?op` — `assert` (added) or `retract` (removed)

### Point-in-time comparison

Compare an entity before and after a change:

```bash
# Before the change (t=5)
fluree query --at 5 'SELECT ?salary WHERE { ex:alice ex:salary ?salary }'

# After the change (t=6)
fluree query --at 6 'SELECT ?salary WHERE { ex:alice ex:salary ?salary }'
```

### Find when a value changed

Track salary history:

```bash
fluree history 'SELECT ?salary ?t ?op WHERE { ex:alice ex:salary ?salary }'
```

Output:
```
?salary  ?t  ?op
85000    1   assert      ← Initial salary
85000    4   retract     ← Old value removed
95000    4   assert      ← New value added
95000    7   retract
110000   7   assert      ← Another raise
```

Each update produces a retract/assert pair at the same `t`.

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

This is a reproducible snapshot — running the same query with the same timestamp always returns the same results.

### Debugging: find what changed between two points

Compare entity states across a range:

```bash
# What was added or removed between t=10 and t=15?
fluree history 'SELECT ?s ?p ?o ?t ?op WHERE {
  ?s ?p ?o .
  FILTER(?t >= 10 && ?t <= 15)
}'
```

### Recover deleted data

Data that was retracted still exists in history:

```bash
# Carol was deleted at t=8. Recover her data from t=7:
fluree query --at 7 'SELECT ?prop ?value WHERE { ex:carol ?prop ?value }'
```

To restore, simply re-insert the data from the historical query.

### Multi-ledger time travel

Query two ledgers at different points in time:

```json
{
  "from": {
    "products": {"ledger": "catalog:main", "t": 10},
    "orders": {"ledger": "orders:main", "t": 25}
  },
  "select": ["?product", "?price", "?qty"],
  "where": [
    {"@id": "?order", "ex:product": "?p", "ex:quantity": "?qty", "@graph": "orders"},
    {"@id": "?p", "schema:name": "?product", "schema:price": "?price", "@graph": "products"}
  ]
}
```

This joins product data from `t=10` with order data from `t=25` — useful for price-at-time-of-purchase analysis.

### Temporal aggregation

Track how a metric changed over time:

```bash
fluree history 'SELECT ?count ?t ?op WHERE {
  ex:dashboard ex:activeUsers ?count
}'
```

### Transaction metadata

Every commit records metadata. Query it via the `txn-meta` graph:

```sparql
PREFIX f: <https://ns.flur.ee/db#>

SELECT ?t ?timestamp ?author
FROM <urn:fluree:knowledge-base:main#txn-meta>
WHERE {
  ?commit f:t ?t ;
          f:time ?timestamp .
  OPTIONAL { ?commit f:author ?author }
}
ORDER BY DESC(?t)
LIMIT 10
```

## Common questions

**Is time travel expensive?**
No. Querying a historical state uses the same indexes as querying the current state. The cost is O(log n) for index lookups.

**Does old data use extra storage?**
Yes — immutability means retracted values are preserved. Storage grows with the number of changes, not just the current state size. For most workloads this is negligible.

**Can I query "between" two points?**
History queries return all changes with their transaction numbers. Use `FILTER` on `?t` to scope to a range.

**Can I delete history?**
No. Immutability is a core guarantee. If you need to remove data for compliance (e.g., GDPR right to erasure), contact the Fluree team about data compaction options.

## Related documentation

- [Time Travel Concepts](../concepts/time-travel.md) — Architecture and design
- [SPARQL Reference](../query/sparql.md) — History query syntax
- [JSON-LD Query](../query/jsonld-query.md) — Time specifiers in JSON-LD
- [Commit Receipts](../transactions/commit-receipts.md) — Transaction metadata
