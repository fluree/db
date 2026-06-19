# Retractions

Retractions remove data from Fluree. While data is never truly deleted (it remains in history), retractions mark triples as no longer current.

## What is a Retraction?

A **retraction** removes a triple from the current state:
- The triple existed at some point (was asserted)
- The retraction marks it as no longer true
- Historical queries can still see the triple
- Current queries don't see the triple

## Basic Retraction

Remove a specific triple:

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "schema": "http://schema.org/"
  },
  "where": [
    { "@id": "ex:alice", "schema:age": "?age" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:age": "?age" }
  ]
}
```

This removes the age property from ex:alice.

## Retract Specific Property

Remove a specific property value:

```bash
curl -X POST "http://localhost:8090/v1/fluree/update?ledger=mydb:main" \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "ex": "http://example.org/ns/",
      "schema": "http://schema.org/"
    },
    "where": [
      { "@id": "ex:alice", "schema:email": "alice.old@example.org" }
    ],
    "delete": [
      { "@id": "ex:alice", "schema:email": "alice.old@example.org" }
    ]
  }'
```

## Retract All Values of a Property

Remove all values for a property:

```json
{
  "where": [
    { "@id": "ex:alice", "schema:telephone": "?phone" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:telephone": "?phone" }
  ]
}
```

If ex:alice has multiple phone numbers, this removes them all.

## Retract Multiple Properties

Remove several properties at once:

```json
{
  "where": [
    { "@id": "ex:alice", "schema:email": "?email" },
    { "@id": "ex:alice", "schema:telephone": "?phone" },
    { "@id": "ex:alice", "ex:preferences": "?prefs" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:email": "?email" },
    { "@id": "ex:alice", "schema:telephone": "?phone" },
    { "@id": "ex:alice", "ex:preferences": "?prefs" }
  ]
}
```

## Retract Entire Entity

Remove all triples for an entity:

```json
{
  "where": [
    { "@id": "ex:alice", "?predicate": "?value" }
  ],
  "delete": [
    { "@id": "ex:alice", "?predicate": "?value" }
  ]
}
```

This finds all triples where ex:alice is the subject and retracts them all.

**Result:** Entity is "deleted" from current state (but remains in history).

## Conditional Retractions

Retract only if conditions are met:

```json
{
  "where": [
    { "@id": "?user", "@type": "schema:Person" },
    { "@id": "?user", "ex:lastLogin": "?lastLogin" },
    { "@id": "?user", "ex:status": "?status" }
  ],
  "filter": "?lastLogin < '2023-01-01' && ?status == 'inactive'",
  "delete": [
    { "@id": "?user", "?predicate": "?value" }
  ],
  "where": [
    { "@id": "?user", "?predicate": "?value" }
  ]
}
```

Removes all inactive users who haven't logged in since 2023.

## Retract Relationships

### Remove Single Relationship

```json
{
  "where": [
    { "@id": "ex:alice", "schema:knows": "ex:bob" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:knows": "ex:bob" }
  ]
}
```

### Remove All Relationships of a Type

```json
{
  "where": [
    { "@id": "ex:alice", "schema:knows": "?person" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:knows": "?person" }
  ]
}
```

### Bidirectional Relationship Removal

Remove relationship in both directions:

```json
{
  "where": [
    { "@id": "ex:alice", "schema:knows": "ex:bob" },
    { "@id": "ex:bob", "schema:knows": "ex:alice" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:knows": "ex:bob" },
    { "@id": "ex:bob", "schema:knows": "ex:alice" }
  ]
}
```

## Cascading Retractions

Retract an entity and all related entities:

```json
{
  "where": [
    { "@id": "ex:order-123", "ex:items": "?item" },
    { "@id": "?item", "?itemPred": "?itemVal" },
    { "@id": "ex:order-123", "?orderPred": "?orderVal" }
  ],
  "delete": [
    { "@id": "?item", "?itemPred": "?itemVal" },
    { "@id": "ex:order-123", "?orderPred": "?orderVal" }
  ]
}
```

Deletes order and all its items.

## Edge-Annotation Cascade

When a transaction retracts a base edge that has annotations attached (see [Insert: Edge Annotations](insert.md#edge-annotations)), the transactor automatically retracts the `f:reifies*` system bundle that pinned each annotation to that edge. Without this cascade, the durable encoding would carry orphaned attachment pointers and `@reifies` queries would still surface annotations for retracted edges.

**Base-edge retract** — fires on every annotated retract:

- The `f:reifies*` bundle for each currently-asserted annotation on the edge is retracted in the same transaction.
- Anonymous (blank-node) annotation subjects also have their body metadata retracted, since the synthetic SID is unaddressable once the bundle is gone.
- Explicit-IRI annotation subjects keep their body metadata as ordinary RDF on the named subject (default RDF mode). To extend cleanup to explicit-IRI annotations as well, set `opts.lpgEdgeLifecycle: true` on the transaction — this matches the property-graph relationship lifecycle.

**Metadata-only retract** — fires when the user retracts every body fact of an annotation subject without touching the base edge:

- The `f:reifies*` bundle is also retracted, so the annotation is fully disposed of and inline `@annotation` queries no longer surface it.
- Same-transaction replacements (delete one body fact, insert another on the same annotation in a single update) preserve the bundle — the post-transaction metadata set is non-empty, so the cascade reads "the user is updating, not removing."
- Partial retracts (some body facts gone, others still asserted) preserve the bundle — the annotation is still meaningful.

The cascade is graph-aware: named-graph annotations are retracted in the same named graph as the edge they reify, never by mismatched-graph retracts.

User-authored mention of `https://ns.flur.ee/db#reifies*` IRIs (the system predicates underlying the bundle) is rejected at parse time on every write surface (insert, upsert, update, Turtle ingest, raw transaction upload). Use `@annotation` and the cascade described above to manage annotation lifecycle.

## Soft Delete vs Hard Retraction

### Soft Delete (Recommended)

Mark as deleted without retracting:

```json
{
  "where": [
    { "@id": "ex:alice", "ex:status": "?status" }
  ],
  "delete": [
    { "@id": "ex:alice", "ex:status": "?status" }
  ],
  "insert": [
    { "@id": "ex:alice", "ex:status": "deleted" },
    { "@id": "ex:alice", "ex:deletedAt": "2024-01-22T10:30:00Z" }
  ]
}
```

**Benefits:**
- Easy to "undelete"
- Audit trail of deletion
- Can query deleted entities
- Less impact on indexes

### Hard Retraction

Retract all data:

```json
{
  "where": [
    { "@id": "ex:alice", "?predicate": "?value" }
  ],
  "delete": [
    { "@id": "ex:alice", "?predicate": "?value" }
  ]
}
```

**When to use:**
- Legal requirement to remove data
- Sensitive data that must be removed
- Test data cleanup

**Note:** Data still exists in history. For true deletion, see data purging operations.

## Pattern-Based Retractions

### Retract by Type

Remove all entities of a type:

```json
{
  "where": [
    { "@id": "?entity", "@type": "ex:TempData" },
    { "@id": "?entity", "?predicate": "?value" }
  ],
  "delete": [
    { "@id": "?entity", "?predicate": "?value" }
  ]
}
```

### Retract by Property Value

Remove entities with specific property:

```json
{
  "where": [
    { "@id": "?entity", "ex:expired": true },
    { "@id": "?entity", "?predicate": "?value" }
  ],
  "delete": [
    { "@id": "?entity", "?predicate": "?value" }
  ]
}
```

## Retraction Semantics

### Idempotent

Retracting a non-existent triple is a no-op:

```text
t=1: No triple exists
t=2: DELETE { ex:alice schema:age 30 }
     Result: No change (triple didn't exist)
```

### No Cascading by Default

Retracting an entity doesn't automatically retract references to it:

```text
t=1: ex:alice schema:worksFor ex:company-a
     ex:company-a schema:name "Acme"

t=2: DELETE all triples for ex:company-a

Result:
- ex:company-a properties are gone
- ex:alice schema:worksFor ex:company-a REMAINS
- Reference is now "dangling"
```

To cascade, explicitly match and delete references.

## Time Travel and Retractions

### Historical Queries See Retracted Data

```bash
# Current query (after retraction at t=5)
curl -X POST http://localhost:8090/v1/fluree/query \
  -d '{"from": "mydb:main", "select": ["?name"], ...}'
# Returns: [] (no results)

# Historical query (before retraction)
curl -X POST http://localhost:8090/v1/fluree/query \
  -d '{"from": "mydb:main@t:3", "select": ["?name"], ...}'
# Returns: [{"name": "Alice"}] (data visible)
```

### History Shows Retractions

Query the history to see both assertions and retractions:

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -d '{
    "@context": { "schema": "http://schema.org/" },
    "from": "mydb:main@t:1",
    "to": "mydb:main@t:latest",
    "select": ["?name", "?t", "?op"],
    "where": [
      { "@id": "ex:alice", "schema:name": { "@value": "?name", "@t": "?t", "@op": "?op" } }
    ],
    "orderBy": "?t"
  }'
```

Response:
```json
[
  ["Alice", 1, true],
  ["Alice", 5, false]
]
```

The `@t` annotation captures the transaction time and `@op` binds a boolean — `true` for assertions, `false` for retractions (mirroring `Flake.op` on disk).

## Error Handling

### Common Errors

**No Match (Not an Error):**
```json
{
  "where": [{ "@id": "ex:nonexistent", "schema:name": "?name" }],
  "delete": [{ "@id": "ex:nonexistent", "schema:name": "?name" }]
}
```
Result: No changes, no error.

**Invalid Pattern:**
```json
{
  "error": "QueryError",
  "message": "Invalid WHERE pattern",
  "code": "INVALID_PATTERN"
}
```

## Performance Considerations

### Index Updates

Retractions update all indexes:
- Each retracted triple updates SPOT, POST, OPST, PSOT
- Large retractions can impact performance
- Consider batch size for bulk deletions

### Indexing Lag

Large retractions may increase indexing lag:
- Monitor `commit_t - index_t`
- Allow time for indexing between large retractions
- Consider scheduling during low-traffic periods

### Vacuum/Compaction

Eventually, consider compaction to reclaim space from retracted data (implementation-specific).

## Best Practices

### 1. Use Soft Deletes

Prefer marking as deleted:

Good:
```json
{
  "insert": [{ "@id": "ex:alice", "ex:status": "deleted" }]
}
```

Over:
```json
{
  "delete": [{ "@id": "ex:alice", "?pred": "?val" }]
}
```

### 2. Add Audit Metadata

Include deletion metadata:

```json
{
  "insert": [
    { "@id": "ex:alice", "ex:status": "deleted" },
    { "@id": "ex:alice", "ex:deletedAt": "2024-01-22T10:30:00Z" },
    { "@id": "ex:alice", "ex:deletedBy": "user-admin" },
    { "@id": "ex:alice", "ex:deleteReason": "User request" }
  ]
}
```

### 3. Be Specific in WHERE

Avoid accidentally retracting too much:

Good:
```json
{
  "where": [{ "@id": "ex:alice", "schema:age": "?age" }],
  "delete": [{ "@id": "ex:alice", "schema:age": "?age" }]
}
```

Dangerous:
```json
{
  "where": [{ "@id": "?entity", "schema:age": "?age" }],
  "delete": [{ "@id": "?entity", "?pred": "?val" }]
}
```

### 4. Test Retractions

Test on development data:

```javascript
// Count before
const countBefore = await query('SELECT (COUNT(?e) as ?count) WHERE { ... }');

// Retract
await transact(retractionQuery);

// Count after
const countAfter = await query('SELECT (COUNT(?e) as ?count) WHERE { ... }');

console.log(`Retracted ${countBefore - countAfter} entities`);
```

### 5. Handle Cascading Explicitly

Don't rely on cascading—make it explicit:

```json
{
  "where": [
    { "@id": "ex:order-123", "?pred": "?val" },
    { "@id": "?item", "ex:orderId": "ex:order-123" },
    { "@id": "?item", "?itemPred": "?itemVal" }
  ],
  "delete": [
    { "@id": "ex:order-123", "?pred": "?val" },
    { "@id": "?item", "?itemPred": "?itemVal" }
  ]
}
```

### 6. Document Deletion Logic

Comment deletion logic:

```javascript
// Hard delete expired sessions older than 30 days
// - Finds all sessions with expired=true and oldDate
// - Retracts all properties
// - Logs count of deleted sessions
await retractExpiredSessions();
```

### 7. Monitor Impact

Track retraction metrics:
- Count of retractions
- Entities affected
- Indexing lag after large retractions
- Query performance impact

## Data Privacy Compliance

### GDPR "Right to be Forgotten"

For compliance, consider:

1. **Soft delete first** (marks as deleted)
2. **Schedule purge** (actual removal from history)
3. **Anonymize references** (replace with pseudonymous ID)

Example:
```json
{
  "where": [{ "@id": "ex:user-123", "?pred": "?val" }],
  "delete": [{ "@id": "ex:user-123", "?pred": "?val" }],
  "insert": [{
    "@id": "ex:user-123",
    "ex:anonymized": true,
    "ex:anonymizedAt": "2024-01-22T10:30:00Z"
  }]
}
```

Note: True purging from history requires administrative operations beyond standard retractions.

## Related Documentation

- [Insert](insert.md) - Adding data
- [Update](update-where-delete-insert.md) - Updating data
- [Time Travel](../concepts/time-travel.md) - Historical queries
- [History Queries](../query/jsonld-query.md) - Viewing changes over time
