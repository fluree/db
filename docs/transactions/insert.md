# Insert

Insert operations add new data to Fluree. This is the most common transaction type for creating new entities and relationships.

## Basic Insert

### Single Entity

Insert a single entity with properties:

```bash
curl -X POST "http://localhost:8090/v1/fluree/insert?ledger=mydb:main" \
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
        "schema:name": "Alice",
        "schema:email": "alice@example.org",
        "schema:age": 30
      }
    ]
  }'
```

**Result:**
```json
{
  "t": 1,
  "timestamp": "2024-01-22T10:30:00.000Z",
  "commit_id": "bafybeig...commitT1",
  "flakes_added": 4,
  "flakes_retracted": 0
}
```

This creates 4 triples:
```
ex:alice rdf:type schema:Person
ex:alice schema:name "Alice"
ex:alice schema:email "alice@example.org"
ex:alice schema:age 30
```

## Multiple Entities

Insert multiple entities in one transaction:

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
      "schema:name": "Alice"
    },
    {
      "@id": "ex:bob",
      "@type": "schema:Person",
      "schema:name": "Bob"
    },
    {
      "@id": "ex:carol",
      "@type": "schema:Person",
      "schema:name": "Carol"
    }
  ]
}
```

**Benefits:**
- Atomic: All entities created or none
- Efficient: Single commit, single index update
- Consistent: All entities at same transaction time

## Insert with Relationships

Create entities with relationships:

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "schema": "http://schema.org/"
  },
  "@graph": [
    {
      "@id": "ex:company-a",
      "@type": "schema:Organization",
      "schema:name": "Acme Corp"
    },
    {
      "@id": "ex:alice",
      "@type": "schema:Person",
      "schema:name": "Alice",
      "schema:worksFor": { "@id": "ex:company-a" }
    }
  ]
}
```

This creates:
```
ex:company-a rdf:type schema:Organization
ex:company-a schema:name "Acme Corp"
ex:alice rdf:type schema:Person
ex:alice schema:name "Alice"
ex:alice schema:worksFor ex:company-a
```

## Nested Objects

Create nested structures:

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
      "schema:address": {
        "@id": "ex:alice-address",
        "@type": "schema:PostalAddress",
        "schema:streetAddress": "123 Main St",
        "schema:addressLocality": "Springfield",
        "schema:postalCode": "12345"
      }
    }
  ]
}
```

This creates two entities (alice and alice-address) linked by schema:address.

## Multi-Valued Properties

Add multiple values for a property:

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
      "schema:email": ["alice@example.org", "alice@work.com"],
      "schema:telephone": ["+1-555-0100", "+1-555-0101"]
    }
  ]
}
```

Creates separate triples for each value:
```
ex:alice schema:email "alice@example.org"
ex:alice schema:email "alice@work.com"
ex:alice schema:telephone "+1-555-0100"
ex:alice schema:telephone "+1-555-0101"
```

## Typed Literals

### Dates

```json
{
  "@id": "ex:alice",
  "schema:birthDate": {
    "@value": "1994-05-15",
    "@type": "xsd:date"
  }
}
```

### Timestamps

```json
{
  "@id": "ex:event",
  "schema:startDate": {
    "@value": "2024-01-22T10:30:00Z",
    "@type": "xsd:dateTime"
  }
}
```

### Numbers

```json
{
  "@id": "ex:product",
  "schema:price": {
    "@value": "29.99",
    "@type": "xsd:decimal"
  }
}
```

### Booleans

```json
{
  "@id": "ex:alice",
  "schema:active": {
    "@value": "true",
    "@type": "xsd:boolean"
  }
}
```

Or use native JSON boolean:
```json
{
  "@id": "ex:alice",
  "schema:active": true
}
```

## Language Tags

Add language-tagged strings:

```json
{
  "@id": "ex:alice",
  "schema:name": {
    "@value": "Alice",
    "@language": "en"
  },
  "schema:description": [
    { "@value": "Software engineer", "@language": "en" },
    { "@value": "Ingénieure logicielle", "@language": "fr" },
    { "@value": "Softwareingenieurin", "@language": "de" }
  ]
}
```

## Blank Nodes

Create entities without explicit IRIs:

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "schema": "http://schema.org/"
  },
  "@graph": [
    {
      "@id": "ex:alice",
      "schema:address": {
        "@type": "schema:PostalAddress",
        "schema:streetAddress": "123 Main St"
      }
    }
  ]
}
```

Fluree generates a unique IRI for the blank node address.

## Adding to Existing Entities

Add properties to existing entities:

**Initial Insert (t=1):**
```json
{
  "@graph": [
    {
      "@id": "ex:alice",
      "schema:name": "Alice"
    }
  ]
}
```

**Add Email (t=2):**
```json
{
  "@graph": [
    {
      "@id": "ex:alice",
      "schema:email": "alice@example.org"
    }
  ]
}
```

After t=2, ex:alice has both name and email.

## Insert Semantics

### Additive by Default

Inserts are additive—they don't remove existing data:

```text
t=1: INSERT { ex:alice schema:name "Alice" }
     Result: ex:alice has name "Alice"

t=2: INSERT { ex:alice schema:age 30 }
     Result: ex:alice has name "Alice" AND age 30
```

### Duplicate Prevention

Inserting the same triple again is a no-op:

```text
t=1: INSERT { ex:alice schema:name "Alice" }
t=2: INSERT { ex:alice schema:name "Alice" }
     (No change—triple already exists)
```

### Multi-Value Handling

Multiple values create multiple triples:

```text
t=1: INSERT { ex:alice schema:email "alice@example.org" }
t=2: INSERT { ex:alice schema:email "alice@work.com" }
     Result: ex:alice has TWO email values
```

## IRI Generation

### Explicit IRIs

Specify IRIs explicitly:

```json
{
  "@id": "ex:user-12345",
  "schema:name": "Alice"
}
```

### UUID-Based IRIs

Generate UUIDs for unique IRIs:

```javascript
const uuid = crypto.randomUUID();
const entity = {
  "@id": `ex:user-${uuid}`,
  "schema:name": "Alice"
};
```

### Content-Addressable IRIs

Use content hashing for deterministic IRIs:

```javascript
const hash = sha256(JSON.stringify(data));
const entity = {
  "@id": `ex:entity-${hash}`,
  ...data
};
```

## Batch Inserts

### Small Batches (Recommended)

```json
{
  "@graph": [
    { "@id": "ex:user-1", "schema:name": "Alice" },
    { "@id": "ex:user-2", "schema:name": "Bob" },
    { "@id": "ex:user-3", "schema:name": "Carol" }
    // ... 100-1000 entities
  ]
}
```

### Large Imports

For very large imports:

```javascript
const batchSize = 1000;
for (let i = 0; i < entities.length; i += batchSize) {
  const batch = entities.slice(i, i + batchSize);
  await transact({ "@graph": batch });
  
  // Optional: wait for indexing
  await sleep(1000);
}
```

## Error Handling

### Common Insert Errors

**Invalid IRI:**
```json
{
  "error": "ValidationError",
  "message": "Invalid IRI format",
  "code": "INVALID_IRI"
}
```

**Type Mismatch:**
```json
{
  "error": "TypeError",
  "message": "Expected number, got string",
  "code": "TYPE_ERROR"
}
```

**Constraint Violation:**
```json
{
  "error": "ConstraintViolation",
  "message": "Unique constraint violated",
  "code": "CONSTRAINT_VIOLATION"
}
```

### Validation Before Insert

Validate data before inserting:

```javascript
function validateEntity(entity) {
  if (!entity['@id']) {
    throw new Error('Entity must have @id');
  }
  if (!isValidIRI(entity['@id'])) {
    throw new Error('Invalid IRI format');
  }
  // Additional validation...
}
```

## Best Practices

### 1. Use Meaningful IRIs

Good:
```json
{ "@id": "ex:user-alice-12345" }
```

Bad:
```json
{ "@id": "ex:1" }
```

### 2. Always Include Type

```json
{
  "@id": "ex:alice",
  "@type": "schema:Person"
}
```

### 3. Use Appropriate Datatypes

```json
{
  "schema:age": 30,
  "schema:price": 29.99,
  "schema:active": true,
  "schema:birthDate": { "@value": "1994-05-15", "@type": "xsd:date" }
}
```

### 4. Batch Related Entities

Insert related entities in same transaction:

```json
{
  "@graph": [
    { "@id": "ex:order-123", ... },
    { "@id": "ex:order-item-1", ... },
    { "@id": "ex:order-item-2", ... }
  ]
}
```

### 5. Use Consistent Namespaces

Define and use consistent namespace prefixes:

```json
{
  "@context": {
    "app": "https://myapp.com/ns/",
    "schema": "http://schema.org/"
  }
}
```

### 6. Include Metadata

Add creation metadata:

```json
{
  "@id": "ex:alice",
  "schema:name": "Alice",
  "app:createdAt": "2024-01-22T10:00:00Z",
  "app:createdBy": "user-admin"
}
```

### 7. Validate Before Insert

Always validate:
- JSON-LD syntax
- IRI formats
- Required fields
- Type constraints

## Performance Tips

### 1. Batch Appropriately

- Recommended: 100-1000 entities per batch
- Too small: Many commits, slow
- Too large: Memory pressure, long commits

### 2. Monitor Indexing

Track indexing lag after large inserts:

```bash
curl http://localhost:8090/v1/fluree/info/mydb:main
# Check: t - index.t
```

### 3. Use Efficient IRIs

Short IRIs are more efficient:

Good: `ex:user-123`
Less efficient: `https://example.org/very/long/path/user-123`

### 4. Minimize Context Size

Use compact contexts:

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "schema": "http://schema.org/"
  }
}
```

## Related Documentation

- [Overview](overview.md) - Transaction overview
- [Upsert](upsert.md) - Replace mode inserts
- [Update](update-where-delete-insert.md) - Updating existing data
- [Data Types](../concepts/datatypes.md) - Supported datatypes
- [API Endpoints](../api/endpoints.md) - HTTP API details
