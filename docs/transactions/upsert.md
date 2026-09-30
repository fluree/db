# Upsert

Upsert operations provide idempotent transactions by **replacing the values of the predicates you supply** for an entity (matched by `@id`).

## What is Upsert?

**Upsert** = Update or Insert:
- If the entity exists: for each predicate present in your payload, retract existing values for that predicate and assert the new value(s)
- If the entity doesn’t exist: create it with the supplied triples

This makes upserts safe to retry: sending the same upsert repeatedly produces the same current-state values for those predicates, with one exception for values stored with less detail than they were written (see [Idempotency](#idempotency)).

## HTTP Endpoint

Use the dedicated upsert endpoint:

```bash
curl -X POST "http://localhost:8090/v1/fluree/upsert?ledger=mydb:main" \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "ex": "http://example.org/ns/",
      "schema": "http://schema.org/"
    },
    "@graph": [
      {
        "@id": "ex:alice",
        "@type": "schema:Person",
        "schema:name": "Alice Smith",
        "schema:email": "alice.smith@example.org"
      }
    ]
  }'
```

## Upsert Behavior

### First Transaction (Entity Doesn't Exist)

```json
{
  "@graph": [
    {
      "@id": "ex:alice",
      "@type": "schema:Person",
      "schema:name": "Alice",
      "schema:email": "alice@example.org"
    }
  ]
}
```

**Result:** Entity created with specified properties.

**Triples After t=1:**
```
ex:alice rdf:type schema:Person
ex:alice schema:name "Alice"
ex:alice schema:email "alice@example.org"
```

### Second Transaction (Entity Exists)

```json
{
  "@graph": [
    {
      "@id": "ex:alice",
      "@type": "schema:Person",
      "schema:name": "Alice Smith",
      "schema:email": "alice.smith@example.org",
      "schema:age": 30
    }
  ]
}
```

**Operations:**
1. For each predicate in the payload (`rdf:type`, `schema:name`, `schema:email`,
   `schema:age`), retract every value ex:alice currently holds for it
2. Assert the payload's values

A value the payload restates cancels its own retraction, so it does not
appear in the commit.

**Flakes:**
```
# Retractions (t=2)
ex:alice schema:name "Alice" (retract)
ex:alice schema:email "alice@example.org" (retract)

# Assertions (t=2)
ex:alice schema:name "Alice Smith" (assert)
ex:alice schema:email "alice.smith@example.org" (assert)
ex:alice schema:age 30 (assert)
```

**Triples After t=2:**
```
ex:alice rdf:type schema:Person
ex:alice schema:name "Alice Smith"
ex:alice schema:email "alice.smith@example.org"
ex:alice schema:age 30
```

`rdf:type schema:Person` is unchanged: it is retracted and re-asserted in the
same transaction, and the two cancel.

### Language Tags, Lists, and Blank Nodes

The unit of replacement is the whole `(graph, subject, predicate)`: every
current value is retracted exactly as it is stored.

- **Language tags.** All languages of a predicate are replaced together. An
  upsert of `{"schema:name": {"@value": "Alicia", "@language": "es"}}` over
  `"Alice"@en` and `"Alice"@fr` leaves only `"Alicia"@es`. To change one
  language and keep the others, use
  [WHERE/DELETE/INSERT](update-where-delete-insert.md) with a `FILTER` on
  `lang(?v)`, or include every language in the payload.
- **Lists.** A `@list` value is replaced as a whole: every stored position is
  retracted, including repeated values, and positions the new list restates
  cancel. Replacing a list with plain values (or the reverse) works the same
  way.
- **Blank nodes.** A blank node in an upsert payload is identified by the
  payload itself, so upserting the same document again addresses the same
  node, and its values are replaced like any other subject's. An identical
  re-upsert of a document with blank nodes commits nothing. A changed
  document mints new blank nodes; the old ones stay (see
  [Stable blank-node ids](update-where-delete-insert.md#editing-blank-node-structures-stable-_fdb--ids)
  for editing a stored blank node in place).
- **Named graphs.** The graph is part of the unit: an upsert into a named
  graph replaces that graph's values and leaves the same predicate's values
  in other graphs alone.

## Idempotency

Replace mode is idempotent—repeated submissions produce the same result:

**First Submission (t=1):**
```json
{"@id": "ex:alice", "schema:name": "Alice", "schema:age": 30}
```
Result: Entity created.

**Second Submission:**
```json
{"@id": "ex:alice", "schema:name": "Alice", "schema:age": 30}
```
Result: nothing is committed. Each value's retraction and re-assertion cancel,
so the transaction stages no flakes and the ledger stays at t=1. This holds
for language-tagged values, lists, and the payload's blank nodes too.

This makes upserts safe to retry, with one exception. A value stored with
less detail than it was written is not re-upserted as a no-op: an integer too
large for 64 bits written with an XSD subtype such as `xsd:nonNegativeInteger`
(the index keeps no subtype), or a `dateTime` or `time` with digits past the
microsecond (storage keeps six). Re-upserting such a value as written commits
a retraction and an assertion of the same stored fact, and the value can be
lost. Write these values at the precision they are stored with: `xsd:integer`
for big integers, and at most six fractional digits for temporals. Graph sync
behaves the same way.

## Comparison: Insert vs Update vs Upsert

### Insert

```bash
POST /insert?ledger=mydb:main
```

**Behavior:**
- Additive: asserts the triples you submit
- Does not retract existing values automatically

**Example:**
```text
t=1: INSERT { ex:alice schema:name "Alice", schema:age 30 }
t=2: INSERT { ex:alice schema:email "alice@example.org" }

Result: ex:alice has name, age, AND email (all three)
```

### Update (WHERE/DELETE/INSERT)

```bash
POST /update?ledger=mydb:main
```

**Behavior:**
- Explicit: you retract exactly what you match in `where`/`delete`, then assert `insert`
- Most flexible (conditional updates, partial updates, computed values)

**Example:**
```text
t=1: INSERT { ex:alice schema:name "Alice", schema:age 30 }
t=2: UPDATE { DELETE { ex:alice schema:age 30 } INSERT { ex:alice schema:age 31 } WHERE { ex:alice schema:age 30 } }

Result: ex:alice has name "Alice", age 31
```

### Upsert

```bash
POST /upsert?ledger=mydb:main
```

**Behavior:**
- Replaces values **for the predicates you supply** (per subject)
- Leaves other predicates unchanged
- Retry-safe/idempotent for the supplied predicates

## Use Cases

### 1. Synchronization from External Systems

Sync data from external database:

```javascript
async function syncUser(externalUser) {
  await fetch('http://localhost:8090/v1/fluree/upsert?ledger=mydb:main', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      "@graph": [{
        "@id": `ex:user-${externalUser.id}`,
        "@type": "schema:Person",
        "schema:name": externalUser.name,
        "schema:email": externalUser.email,
        "schema:telephone": externalUser.phone
      }]
    })
  })
}

// Safe to call repeatedly—always matches external state
await syncUser(fetchUserFromDB(123));
```

### 2. Idempotent API Operations

Make API operations retry-safe:

```javascript
// Safe to retry on failure
async function updateProduct(productId, productData) {
  return await fetch('http://localhost:8090/v1/fluree/upsert?ledger=mydb:main', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      "@graph": [{
        "@id": `ex:product-${productId}`,
        ...productData
      }]
    })
  })
}
```

### 3. Configuration Management

Update configuration atomically:

```json
{
  "@graph": [
    {
      "@id": "ex:config",
      "@type": "ex:Configuration",
      "ex:apiEndpoint": "https://api.example.com",
      "ex:timeout": 30000,
      "ex:retries": 3,
      "ex:enabled": true
    }
  ]
}
```

Each update replaces entire configuration—no orphaned settings.

### 4. State Machine Transitions

Model state machines where entity has well-defined state:

```json
{
  "@graph": [
    {
      "@id": "ex:order-123",
      "@type": "ex:Order",
      "ex:status": "shipped",
      "ex:shippedAt": "2024-01-22T10:30:00Z",
      "ex:carrier": "FedEx",
      "ex:trackingNumber": "123456789"
    }
  ]
}
```

## Batch Upserts

Upsert multiple entities:

```bash
POST /upsert?ledger=mydb:main
```

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "schema": "http://schema.org/"
  },
  "@graph": [
    {
      "@id": "ex:user-1",
      "@type": "schema:Person",
      "schema:name": "Alice"
    },
    {
      "@id": "ex:user-2",
      "@type": "schema:Person",
      "schema:name": "Bob"
    },
    {
      "@id": "ex:user-3",
      "@type": "schema:Person",
      "schema:name": "Carol"
    }
  ]
}
```

Each entity is replaced independently.

## Type Handling

### Types are Preserved

Upsert preserves existing `@type` values unless you explicitly include `@type` in the upsert payload (in which case `rdf:type` is treated like any other predicate and its values are replaced for that subject).

```json
{
  "@graph": [
    {
      "@id": "ex:alice",
      "@type": "schema:Person",
      "schema:name": "Alice"
    }
  ]
}
```

The `@type` is always asserted, even if it existed before.

### Multiple Types

Entities can have multiple types:

```json
{
  "@graph": [
    {
      "@id": "ex:alice",
      "@type": ["schema:Person", "ex:Employee"],
      "schema:name": "Alice"
    }
  ]
}
```

All types are replaced together.

## Edge Cases

### Predicates Not in the Payload

Only the predicates the payload names are replaced:

**Before (t=1):**
```json
{
  "@id": "ex:alice",
  "schema:name": "Alice",
  "schema:email": "alice@example.org",
  "schema:age": 30
}
```

**Upsert (t=2):**
```json
{
  "@id": "ex:alice",
  "schema:name": "Alice Smith"
}
```

**After t=2:**
```json
{
  "@id": "ex:alice",
  "schema:name": "Alice Smith",
  "schema:email": "alice@example.org",
  "schema:age": 30
}
```

To remove a predicate's values, retract them with
[WHERE/DELETE/INSERT](update-where-delete-insert.md), or use
[graph sync](sync.md) to make a whole graph equal a document.

## Error Handling

### Same Errors as Default Mode

Replace mode has same validation errors:

```json
{
  "error": "ValidationError",
  "message": "Invalid IRI format",
  "code": "INVALID_IRI"
}
```

### No Special Errors

Replace mode doesn't introduce new error types—it's just different semantics for the same operations.

## Performance Considerations

### Retraction Overhead

Replace mode may retract many triples:

```text
Entity with 50 properties:
- 50 retractions
- 50 assertions
= 100 flakes per entity
```

For entities with many properties, this can be expensive.

### Indexing Impact

Each retraction and assertion updates indexes:
- More work for indexing process
- May increase indexing lag
- Consider batch size for large replacements

## Best Practices

### 1. Use for Idempotent Operations

Good use:
```javascript
// Idempotent sync
await upsertUser(userId, userData);
await upsertUser(userId, userData); // Safe to retry
```

### 2. Include All Required Properties

Always include all properties entity should have:

Good:
```json
{
  "@id": "ex:user-123",
  "@type": "schema:Person",
  "schema:name": "Alice",
  "schema:email": "alice@example.org",
  "ex:status": "active"
}
```

Bad (incomplete):
```json
{
  "@id": "ex:user-123",
  "schema:name": "Alice"
}
```

### 3. Use Consistent Schema

Define entity schema and always include all fields:

```javascript
function createUserTransaction(user) {
  return {
    "@id": `ex:user-${user.id}`,
    "@type": "schema:Person",
    "schema:name": user.name || null,
    "schema:email": user.email || null,
    "schema:telephone": user.phone || null,
    "ex:status": user.status || "active"
  };
}
```

### 4. Document Upsert Usage

Comment when using upsert for idempotent sync:

```javascript
// Upsert for idempotent sync with external API
await fetch('http://localhost:8090/v1/fluree/upsert?ledger=users:main', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(userPayload),
});
```

### 5. Test Idempotency

Verify operations are truly idempotent:

```javascript
const result1 = await upsert(data);
const result2 = await upsert(data);
// Should produce same final state
```

### 6. Monitor Performance

Track metrics for replace operations:
- Flakes retracted
- Flakes asserted
- Commit time
- Indexing lag

### 7. Consider Alternatives

For partial updates, use WHERE/DELETE/INSERT:

```json
{
  "where": [{ "@id": "ex:alice", "schema:age": "?oldAge" }],
  "delete": [{ "@id": "ex:alice", "schema:age": "?oldAge" }],
  "insert": [{ "@id": "ex:alice", "schema:age": 31 }]
}
```

## Comparison Table

| Feature | Default Mode | Replace Mode |
|---------|--------------|--------------|
| **Behavior** | Additive | Replaces each named predicate's values |
| **Predicates not in the payload** | Preserved | Preserved |
| **Idempotent** | No | Yes |
| **Partial updates** | Yes (with WHERE/DELETE/INSERT) | Per predicate |
| **Use case** | Adding data | Synchronization |
| **Retry safety** | Requires care | Safe by default |
| **Performance** | Fewer operations | More operations |

## Related Documentation

- [Insert](insert.md) - Adding new data
- [Update](update-where-delete-insert.md) - Partial updates
- [Overview](overview.md) - Transaction overview
- [API Endpoints](../api/endpoints.md) - HTTP API details
