# Transactions

Transactions are how you write data to Fluree. This section covers all transaction patterns, formats, and behaviors.

## Transaction Patterns

### [Overview](overview.md)

High-level introduction to Fluree transactions:
- Transaction lifecycle
- Commit process
- Indexing pipeline
- Transaction semantics

### [Insert](insert.md)

Adding new data to the database:
- Basic inserts
- Batch inserts
- Entity creation
- Relationship creation

### [Upsert](upsert.md)

Idempotent transactions that replace values for supplied predicates:
- Upsert semantics
- Use cases for upsert
- Idempotent operations
- Synchronization patterns

### [Update (WHERE/DELETE/INSERT)](update-where-delete-insert.md)

Targeted updates to existing data:
- WHERE clause patterns
- DELETE operations
- INSERT operations
- Conditional updates
- Partial updates

### [Retractions](retractions.md)

Removing data from the database:
- Retract specific triples
- Retract entire entities
- Retraction semantics
- Time travel and retractions

## Transaction Formats

### [Turtle Ingest](turtle.md)

Import RDF data in Turtle format:
- Turtle syntax
- Bulk imports
- File uploads
- Format conversion

### [Signed / Credentialed Transactions](signed-transactions.md)

Cryptographically signed transactions:
- JWS signed transactions
- Verifiable Credentials
- Identity-based transactions
- Audit trails

## Transaction Metadata

### [Commit Receipts and tx-id](commit-receipts.md)

Understanding transaction receipts:
- Receipt structure
- Transaction ID (t)
- Commit ID
- Timestamps
- Flake counts

### [Indexing Side-Effects](indexing-side-effects.md)

How transactions affect indexing:
- Background indexing
- Novelty layer
- Index triggers
- Performance considerations

## Transaction Concepts

### Immutability

Once committed, transactions are immutable:
- Changes are represented as new assertions and retractions
- Historical data is never modified
- Complete audit trail preserved
- Time travel enabled by immutability

### Atomicity

Transactions are atomic:
- All changes succeed or all fail
- No partial commits
- Consistent state guaranteed
- Validation before commit

### Transaction Time

Every transaction receives a unique transaction time:
- Monotonically increasing integer (t)
- Unique across all ledgers in instance
- Used for time travel queries
- Basis for temporal ordering

### Assertions and Retractions

Transactions consist of two operations:
- **Assertions**: Add new triples
- **Retractions**: Remove existing triples

Updates are represented as retraction + assertion pairs.

## Common Transaction Patterns

### Create Entity

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "schema": "http://schema.org/"
  },
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

### Update Property

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "schema": "http://schema.org/"
  },
  "where": [
    { "@id": "ex:alice", "schema:age": "?oldAge" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:age": "?oldAge" }
  ],
  "insert": [
    { "@id": "ex:alice", "schema:age": 31 }
  ]
}
```

### Add Relationship

```json
{
  "@graph": [
    {
      "@id": "ex:alice",
      "schema:worksFor": { "@id": "ex:company-a" }
    }
  ]
}
```

### Remove Property

```json
{
  "delete": [
    { "@id": "ex:alice", "schema:telephone": "?phone" }
  ],
  "where": [
    { "@id": "ex:alice", "schema:telephone": "?phone" }
  ]
}
```

### Replace Entity (Upsert)

```bash
POST /upsert?ledger=mydb:main
```

```json
{
  "@graph": [
    {
      "@id": "ex:alice",
      "@type": "schema:Person",
      "schema:name": "Alice Smith",
      "schema:email": "alice.smith@example.org"
    }
  ]
}
```

## Transaction Types

- **Insert** (`POST /insert`) — add triples (JSON-LD or Turtle)
- **Update** (`POST /update`) — WHERE/DELETE/INSERT (JSON-LD) or SPARQL UPDATE
- **Upsert** (`POST /upsert`) — replace values for the predicates you supply (JSON-LD, Turtle, TriG)

## Transaction Validation

Before commit, transactions are validated:

**Syntax Validation:**
- Valid JSON/JSON-LD syntax
- Well-formed IRIs
- Correct datatype formats

**Semantic Validation:**
- Type compatibility
- Constraint adherence
- Reference integrity (optional)

**Policy Validation:**
- Authorization checks
- Access control enforcement
- Data-level permissions

Validation failures result in transaction rejection with detailed error messages.

## Transaction Size Limits

**Default Limits:**
- Transaction size: 10 MB
- Triple count: 10,000 triples
- Configurable per deployment

**Large Transactions:**
- Split into batches for large imports
- Use streaming for bulk data
- Monitor indexing lag

See [Indexing Side-Effects](indexing-side-effects.md) for performance considerations.

## Error Handling

### Transaction Errors

Common errors:
- `PARSE_ERROR` - Invalid JSON-LD
- `INVALID_IRI` - Malformed IRI
- `TYPE_ERROR` - Type mismatch
- `CONSTRAINT_VIOLATION` - Constraint violated
- `POLICY_DENIED` - Not authorized

### Retry Logic

Implement retry for transient errors:
- Network errors: Retry with backoff
- Conflicts: Retry with updated data
- Timeouts: Retry after delay
- Server errors: Retry with backoff

### Idempotency

For idempotent transactions:
- Use replace mode
- Include unique identifiers
- Design for retry safety
- Use deterministic IRIs

## Best Practices

### 1. Use Meaningful IRIs

Good:
```json
{"@id": "ex:user-alice-123"}
```

Bad:
```json
{"@id": "ex:1"}
```

### 2. Batch Related Changes

Combine related entities in single transaction:

```json
{
  "@graph": [
    { "@id": "ex:order-123", "ex:customer": { "@id": "ex:alice" } },
    { "@id": "ex:order-123", "ex:product": { "@id": "ex:widget" } },
    { "@id": "ex:order-123", "ex:total": 99.99 }
  ]
}
```

### 3. Use Appropriate Mode

- Default mode: For additive operations
- Replace mode: For complete replacements, sync operations

### 4. Include Types

Always specify entity types:

```json
{
  "@id": "ex:alice",
  "@type": "schema:Person"
}
```

### 5. Use Typed Literals

Be explicit about types:

```json
{
  "schema:birthDate": {
    "@value": "1990-05-15",
    "@type": "xsd:date"
  }
}
```

### 6. Design for History

Consider how data will look in historical queries:
- Use descriptive property names
- Include relevant metadata
- Design for temporal queries

### 7. Monitor Performance

Track transaction metrics:
- Commit time
- Indexing lag
- Error rates
- Transaction size

## Related Documentation

- [Getting Started: Write Data](../getting-started/quickstart-write.md) - Quickstart guide
- [Concepts: Time Travel](../concepts/time-travel.md) - Temporal semantics
- [API: POST /update](../api/endpoints.md#post-update) - HTTP endpoint details
- [Indexing](../indexing-and-search/README.md) - Indexing and search
