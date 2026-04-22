# Quickstart: Query Data

This guide introduces you to querying data in Fluree using both JSON-LD Query and SPARQL.

## Prerequisites

- Fluree server running with data (complete previous quickstarts)
- Sample data from [Write Data](quickstart-write.md) guide

## Query Languages

Fluree supports two query languages:

- **JSON-LD Query**: Fluree's native JSON-based query language
- **SPARQL**: W3C standard RDF query language

Both provide access to the same data and features.

## JSON-LD Query

### Basic SELECT Query

Retrieve all person names:

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "schema": "http://schema.org/"
    },
    "from": "mydb:main",
    "select": ["?name"],
    "where": [
      { "@id": "?person", "schema:name": "?name" }
    ]
  }'
```

Response:

```json
[
  { "name": "Alice" },
  { "name": "Bob" },
  { "name": "Carol" }
]
```

### Query Multiple Properties

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "schema": "http://schema.org/"
    },
    "from": "mydb:main",
    "select": ["?name", "?email"],
    "where": [
      { "@id": "?person", "schema:name": "?name" },
      { "@id": "?person", "schema:email": "?email" }
    ]
  }'
```

Response:

```json
[
  { "name": "Alice", "email": "alice@example.org" },
  { "name": "Bob", "email": "bob@example.org" },
  { "name": "Carol", "email": "carol@example.org" }
]
```

### Filter Results

Query with a specific filter:

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "schema": "http://schema.org/"
    },
    "from": "mydb:main",
    "select": ["?name", "?age"],
    "where": [
      { "@id": "?person", "schema:name": "?name" },
      { "@id": "?person", "schema:age": "?age" }
    ],
    "filter": "?age > 25"
  }'
```

### Query Specific Entity

Query a specific entity by IRI:

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "ex": "http://example.org/ns/",
      "schema": "http://schema.org/"
    },
    "from": "mydb:main",
    "select": ["?name", "?email", "?age"],
    "where": [
      { "@id": "ex:alice", "schema:name": "?name" },
      { "@id": "ex:alice", "schema:email": "?email" },
      { "@id": "ex:alice", "schema:age": "?age" }
    ]
  }'
```

### Query with Relationships

Follow links between entities:

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "schema": "http://schema.org/"
    },
    "from": "mydb:main",
    "select": ["?personName", "?companyName"],
    "where": [
      { "@id": "?person", "schema:name": "?personName" },
      { "@id": "?person", "schema:worksFor": "?company" },
      { "@id": "?company", "schema:name": "?companyName" }
    ]
  }'
```

Response:

```json
[
  { "personName": "Alice", "companyName": "Acme Corp" }
]
```

## SPARQL

### Basic SELECT Query

The same queries in SPARQL syntax:

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/sparql-query" \
  -d '
    PREFIX schema: <http://schema.org/>
    
    SELECT ?name
    FROM <mydb:main>
    WHERE {
      ?person schema:name ?name .
    }
  '
```

### Query Multiple Properties

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/sparql-query" \
  -d '
    PREFIX schema: <http://schema.org/>
    
    SELECT ?name ?email
    FROM <mydb:main>
    WHERE {
      ?person schema:name ?name .
      ?person schema:email ?email .
    }
  '
```

### Filter Results

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/sparql-query" \
  -d '
    PREFIX schema: <http://schema.org/>
    
    SELECT ?name ?age
    FROM <mydb:main>
    WHERE {
      ?person schema:name ?name .
      ?person schema:age ?age .
      FILTER (?age > 25)
    }
  '
```

### Query with Relationships

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/sparql-query" \
  -d '
    PREFIX schema: <http://schema.org/>
    
    SELECT ?personName ?companyName
    FROM <mydb:main>
    WHERE {
      ?person schema:name ?personName .
      ?person schema:worksFor ?company .
      ?company schema:name ?companyName .
    }
  '
```

## Time Travel Queries

Query historical data using time specifiers.

### Query at Specific Transaction

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "schema": "http://schema.org/"
    },
    "from": "mydb:main@t:1",
    "select": ["?name"],
    "where": [
      { "@id": "?person", "schema:name": "?name" }
    ]
  }'
```

This shows data as it existed at transaction 1.

### Query at ISO Timestamp

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "schema": "http://schema.org/"
    },
    "from": "mydb:main@iso:2024-01-22T10:00:00Z",
    "select": ["?name"],
    "where": [
      { "@id": "?person", "schema:name": "?name" }
    ]
  }'
```

### Query at Commit ContentId

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "schema": "http://schema.org/"
    },
    "from": "mydb:main@commit:bafybeig...",
    "select": ["?name"],
    "where": [
      { "@id": "?person", "schema:name": "?name" }
    ]
  }'
```

See [Time Travel](../concepts/time-travel.md) for comprehensive details.

## History Queries

Track changes to entities over time by specifying a time range in the `from` clause.

### Entity History

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "ex": "http://example.org/ns/",
      "schema": "http://schema.org/"
    },
    "from": "mydb:main@t:1",
    "to": "mydb:main@t:latest",
    "select": ["?name", "?age", "?t", "?op"],
    "where": [
      { "@id": "ex:alice", "schema:name": { "@value": "?name", "@t": "?t", "@op": "?op" } },
      { "@id": "ex:alice", "schema:age": "?age" }
    ],
    "orderBy": "?t"
  }'
```

The `@t` annotation binds the transaction time and `@op` shows the operation type (`"assert"` or `"retract"`).

Response shows all changes:

```json
[
  ["Alice", 30, 1, "assert"],
  ["Alice", 30, 5, "retract"],
  ["Alicia", 31, 5, "assert"]
]
```

### Property History

Track changes to a specific property:

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "ex": "http://example.org/ns/",
      "schema": "http://schema.org/"
    },
    "from": "mydb:main@t:1",
    "to": "mydb:main@t:latest",
    "select": ["?age", "?t", "?op"],
    "where": [
      { "@id": "ex:alice", "schema:age": { "@value": "?age", "@t": "?t", "@op": "?op" } }
    ],
    "orderBy": "?t"
  }'
```

Response:

```json
[
  [30, 1, "assert"],
  [30, 5, "retract"],
  [31, 5, "assert"]
]
```

## Aggregations

### Count Results

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/sparql-query" \
  -d '
    PREFIX schema: <http://schema.org/>
    
    SELECT (COUNT(?person) AS ?count)
    FROM <mydb:main>
    WHERE {
      ?person schema:name ?name .
    }
  '
```

### Average, Min, Max

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/sparql-query" \
  -d '
    PREFIX schema: <http://schema.org/>
    
    SELECT (AVG(?age) AS ?avgAge) (MIN(?age) AS ?minAge) (MAX(?age) AS ?maxAge)
    FROM <mydb:main>
    WHERE {
      ?person schema:age ?age .
    }
  '
```

## Limiting Results

### JSON-LD Query Limit

```json
{
  "@context": {
    "schema": "http://schema.org/"
  },
  "from": "mydb:main",
  "select": ["?name"],
  "where": [
    { "@id": "?person", "schema:name": "?name" }
  ],
  "limit": 10
}
```

### SPARQL Limit and Offset

```sparql
PREFIX schema: <http://schema.org/>

SELECT ?name
FROM <mydb:main>
WHERE {
  ?person schema:name ?name .
}
ORDER BY ?name
LIMIT 10
OFFSET 20
```

## Ordering Results

### JSON-LD Query Order

```json
{
  "@context": {
    "schema": "http://schema.org/"
  },
  "from": "mydb:main",
  "select": ["?name", "?age"],
  "where": [
    { "@id": "?person", "schema:name": "?name" },
    { "@id": "?person", "schema:age": "?age" }
  ],
  "orderBy": ["?age"]
}
```

### SPARQL Order

```sparql
PREFIX schema: <http://schema.org/>

SELECT ?name ?age
FROM <mydb:main>
WHERE {
  ?person schema:name ?name .
  ?person schema:age ?age .
}
ORDER BY DESC(?age)
```

## Multi-Ledger Queries

Query across multiple ledgers:

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "schema": "http://schema.org/"
    },
    "from": ["customers:main", "orders:main"],
    "select": ["?customerName", "?orderTotal"],
    "where": [
      { "@id": "?customer", "schema:name": "?customerName" },
      { "@id": "?order", "schema:customer": "?customer" },
      { "@id": "?order", "schema:totalPrice": "?orderTotal" }
    ]
  }'
```

See [Datasets](../query/datasets.md) for comprehensive multi-graph query documentation.

## Understanding Query Results

### JSON-LD Query Results

Results are returned as an array of objects:

```json
[
  { "name": "Alice", "age": 30 },
  { "name": "Bob", "age": 25 }
]
```

### SPARQL Results

SPARQL returns results in SPARQL JSON format:

```json
{
  "head": {
    "vars": ["name", "age"]
  },
  "results": {
    "bindings": [
      {
        "name": { "type": "literal", "value": "Alice" },
        "age": { "type": "literal", "value": "30", "datatype": "http://www.w3.org/2001/XMLSchema#integer" }
      },
      {
        "name": { "type": "literal", "value": "Bob" },
        "age": { "type": "literal", "value": "25", "datatype": "http://www.w3.org/2001/XMLSchema#integer" }
      }
    ]
  }
}
```

See [Output Formats](../query/output-formats.md) for format details.

## Query Performance Tips

### 1. Use Specific Patterns

More specific patterns are faster:

Good:
```json
{ "@id": "ex:alice", "schema:name": "?name" }
```

Less efficient:
```json
{ "@id": "?person", "?predicate": "?value" }
```

### 2. Filter Early

Apply filters in WHERE clauses when possible:

```json
"where": [
  { "@id": "?person", "schema:age": "?age" }
],
"filter": "?age > 25"
```

### 3. Limit Results

Always use LIMIT for large result sets:

```json
"limit": 100
```

### 4. Use Indexes

Queries leverage automatic indexes. Structure queries to take advantage:
- Subject-based lookups are fast
- Predicate-based lookups are fast
- Complex graph patterns may be slower

See [Explain Plans](../query/explain.md) for query optimization.

## Common Query Patterns

### Find All Types

```sparql
SELECT DISTINCT ?type
FROM <mydb:main>
WHERE {
  ?entity a ?type .
}
```

### Find All Predicates

```sparql
SELECT DISTINCT ?predicate
FROM <mydb:main>
WHERE {
  ?subject ?predicate ?object .
}
```

### Inverse Relationships

Find what points to an entity:

```sparql
SELECT ?source ?predicate
FROM <mydb:main>
WHERE {
  ?source ?predicate <http://example.org/ns/alice> .
}
```

### Optional Properties

Query with optional values:

```sparql
PREFIX schema: <http://schema.org/>

SELECT ?name ?email ?phone
FROM <mydb:main>
WHERE {
  ?person schema:name ?name .
  ?person schema:email ?email .
  OPTIONAL { ?person schema:telephone ?phone }
}
```

## Error Handling

### Query Errors

Common query errors:

```json
{
  "error": "QueryError",
  "message": "Ledger not found: mydb:main",
  "code": "LEDGER_NOT_FOUND"
}
```

```json
{
  "error": "ParseError",
  "message": "Invalid JSON-LD: unexpected token",
  "code": "PARSE_ERROR"
}
```

### Empty Results

Empty result set (not an error):

```json
[]
```

## Next Steps

Now that you can query data:

1. **Learn Advanced Queries**: Explore [JSON-LD Query](../query/jsonld-query.md) and [SPARQL](../query/sparql.md) documentation
2. **Understand Time Travel**: Deep dive into [Time Travel](../concepts/time-travel.md)
3. **Optimize Queries**: Read about [Explain Plans](../query/explain.md)
4. **Multi-Graph Queries**: Learn about [Datasets](../query/datasets.md)

## Related Documentation

- [JSON-LD Query](../query/jsonld-query.md) - Complete JSON-LD query reference
- [SPARQL](../query/sparql.md) - Complete SPARQL reference
- [Output Formats](../query/output-formats.md) - Result format options
- [Time Travel](../concepts/time-travel.md) - Historical queries
- [Graph Crawl](../query/graph-crawl.md) - Graph traversal
