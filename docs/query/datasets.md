# Datasets and Multi-Graph Execution

Fluree supports SPARQL datasets, enabling queries across multiple graphs and ledgers simultaneously. This provides powerful data integration capabilities for complex applications.

## SPARQL Datasets

A **dataset** in SPARQL is a collection of graphs used for query execution:

- **Default Graph**: The primary graph for triple patterns without GRAPH clauses
- **Named Graphs**: Additional graphs identified by IRIs, accessible via GRAPH clauses

## FROM Clauses

### Single Default Graph

Specify a single default graph:

**JSON-LD Query:**

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": "mydb:main",
  "select": ["?name"],
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ]
}
```

**SPARQL:**

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?name
FROM <mydb:main>
WHERE {
  ?person ex:name ?name .
}
```

### Multiple Default Graphs

Specify multiple default graphs (union semantics):

**JSON-LD Query:**

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": ["mydb:main", "otherdb:main"],
  "select": ["?name"],
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ]
}
```

**SPARQL:**

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?name
FROM <mydb:main>
FROM <otherdb:main>
WHERE {
  ?person ex:name ?name .
}
```

## FROM NAMED Clauses

### Named graph sources (datasets)

In SPARQL, `FROM NAMED` identifies **named graphs in the dataset**. In Fluree, these are often *graph sources* such as:
- another ledger (federation / multi-ledger queries), or
- a graph source (search, tabular mapping, etc.).

Note: On the **ledger-scoped HTTP query endpoint** (`POST /query/{ledger}`), `FROM` / `FROM NAMED` is also supported, but is interpreted as selecting **named graphs inside that same ledger**. Use the connection-scoped endpoint (`POST /query`) when you want a dataset that spans multiple ledgers.

Query across multiple named graph sources:

**JSON-LD Query:**

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "fromNamed": {
    "mydb": { "@id": "mydb:main" },
    "otherdb": { "@id": "otherdb:main" }
  },
  "select": ["?graph", "?name"],
  "where": [
    ["graph", "?graph", { "@id": "?person", "ex:name": "?name" }]
  ]
}
```

**SPARQL:**

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?graph ?name
FROM NAMED <mydb:main>
FROM NAMED <otherdb:main>
WHERE {
  GRAPH ?graph {
    ?person ex:name ?name .
  }
}
```

### Specific Named Graph

Query a specific named graph:

**SPARQL:**

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?name
FROM NAMED <mydb:main>
WHERE {
  GRAPH <mydb:main> {
    ?person ex:name ?name .
  }
}
```

### Ledger named graph: `txn-meta`

Fluree provides a built-in named graph inside each ledger for transactional / commit metadata: **`txn-meta`**.

Use the `#txn-meta` fragment on a ledger reference:
- `mydb:main#txn-meta`
- `mydb:main@t:100#txn-meta` (time pinned)

**JSON-LD Query (txn-meta as the default graph):**

```json
{
  "@context": {
    "f": "https://ns.flur.ee/db#",
    "ex": "http://example.org/ns/"
  },
  "from": "mydb:main#txn-meta",
  "select": ["?commit", "?t", "?machine"],
  "where": [
    { "@id": "?commit", "f:t": "?t" },
    { "@id": "?commit", "ex:machine": "?machine" }
  ]
}
```

**SPARQL Query:**

```sparql
PREFIX f: <https://ns.flur.ee/db#>
PREFIX ex: <http://example.org/ns/>

SELECT ?commit ?t ?machine
FROM <mydb:main#txn-meta>
WHERE {
  ?commit f:t ?t .
  OPTIONAL { ?commit ex:machine ?machine }
}
```

### User-Defined Named Graphs

Fluree supports user-defined named graphs ingested via TriG format. These graphs are queryable using the structured `from` object syntax with a `graph` field.

For the ledger-scoped HTTP endpoint (`POST /query/{ledger}`), the server also accepts a convenient shorthand:
- `"from": "txn-meta"` / `"from": "default"` / `"from": "<graph IRI>"`
to select a graph **within** the ledger in the URL.

**Ingesting data with named graphs (TriG):**

```bash
curl -X POST "http://localhost:8090/v1/fluree/upsert?ledger=mydb:main" \
  -H "Content-Type: application/trig" \
  -d '@prefix ex: <http://example.org/ns/> .

      GRAPH <http://example.org/graphs/products> {
          ex:widget ex:name "Widget" ;
                    ex:price "29.99"^^xsd:decimal .
      }'
```

**Querying the named graph (JSON-LD):**

Use the structured `from` object with a `graph` field specifying the graph IRI:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": {
    "@id": "mydb:main",
    "graph": "http://example.org/graphs/products"
  },
  "select": ["?name", "?price"],
  "where": [
    { "@id": "?product", "ex:name": "?name" },
    { "@id": "?product", "ex:price": "?price" }
  ]
}
```

**With time-travel:**

```json
{
  "from": {
    "@id": "mydb:main",
    "t": 100,
    "graph": "http://example.org/graphs/products"
  },
  "select": ["?name", "?price"],
  "where": [...]
}
```

**Combining multiple graphs (JSON-LD):**

Query across the default graph and user-defined named graphs:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": "mydb:main",
  "fromNamed": {
    "products": {
      "@id": "mydb:main",
      "@graph": "http://example.org/graphs/products"
    }
  },
  "select": ["?company", "?product", "?price"],
  "where": [
    { "@id": "?company", "@type": "ex:Company" },
    ["graph", "products", { "@id": "?product", "ex:name": "?productName", "ex:price": "?price" }]
  ]
}
```

**Notes:**
- Named graphs are queryable after indexing completes
- The `@graph` field accepts the full graph IRI (no URL-encoding required)
- Time-travel is specified via the `t`, `iso`, or `sha` field in the object form
- Object keys in `fromNamed` serve as dataset-local aliases for use in GRAPH patterns

## Graph Source Object Schema

### `fromNamed` (named graphs) — preferred format

`fromNamed` is an object whose keys are dataset-local aliases. Each value has:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `@id` | string | Yes | Ledger reference (e.g., `mydb:main`, `mydb:main@t:100`) |
| `@graph` | string | No | Graph selector: `"default"`, `"txn-meta"`, or full IRI |
| `t` | integer | No | Time-travel: specific transaction number |
| `at` | string | No | Time-travel: ISO-8601 timestamp or `commit:<hash>` |
| `policy` | object | No | Per-source policy override (see below) |

### `from` (default graphs) — object syntax

When using object syntax for `from`, the following fields are available:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `@id` | string | Yes | Ledger reference (e.g., `mydb:main`, `mydb:main@t:100`) |
| `alias` | string | No | Dataset-local alias for GRAPH pattern reference |
| `graph` | string | No | Graph selector: `"default"`, `"txn-meta"`, or full IRI |
| `t` | integer | No | Time-travel: specific transaction number |
| `iso` | string | No | Time-travel: ISO-8601 timestamp |
| `commit_id` | string | No | Time-travel: commit ContentId |
| `policy` | object | No | Per-source policy override (see below) |

> **Legacy format:** The array format `"from-named": [...]` with `"alias"` and `"graph"` fields is still accepted for backward compatibility. The `"fromNamed"` object format is preferred.

### Dataset-Local Aliases

Aliases provide short names for referencing graphs in query patterns. They are especially useful when:

1. **Same graph IRI exists in multiple ledgers** - Use distinct aliases to disambiguate
2. **Complex IRIs** - Use short aliases instead of repeating long IRIs

**Example: Disambiguating same graph IRI across ledgers**

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "fromNamed": {
    "salesProducts": {
      "@id": "sales:main",
      "@graph": "http://example.org/vocab#products"
    },
    "inventoryProducts": {
      "@id": "inventory:main",
      "@graph": "http://example.org/vocab#products"
    }
  },
  "select": ["?g", "?sku", "?data"],
  "where": [
    ["graph", "?g", { "@id": "?sku", "ex:data": "?data" }]
  ]
}
```

In this example, both ledgers have a graph with the same IRI (`http://example.org/vocab#products`). The aliases `salesProducts` and `inventoryProducts` (the object keys) allow you to reference them distinctly.

**Validation Rules:**
- Aliases must be unique across the entire dataset (both `from` and `fromNamed`)
- Aliases cannot collide with identifiers (the `@id` values)
- Duplicate aliases will cause an error

### Graph Selector Values

The `graph` field accepts three types of values:

| Value | Meaning |
|-------|---------|
| `"default"` | Explicitly select the ledger's default graph |
| `"txn-meta"` | Select the built-in transaction metadata graph (`urn:fluree:{ledger_id}#txn-meta`) |
| `"<full-iri>"` | Select a user-defined named graph by its full IRI |

**Note:** If using `#txn-meta` fragment syntax in `@id`, do not also specify `graph: "txn-meta"`. This is considered ambiguous and will return an error.

### Per-Source Policy Override

Each graph source can have its own policy, enabling fine-grained access control where different graphs in the same query use different policies.

**Policy object fields:**
- `identity`: Identity IRI string
- `policy-class`: Policy class IRI or array of IRIs
- `policy`: Inline policy JSON
- `policy-values`: Policy parameter values
- `default-allow`: Boolean (default: false). Governs access when no policies match. Ignored (forced false) if `identity` is specified but has no subject node in the ledger.

**Example:**

```json
{
  "from": [
    {
      "@id": "public:main",
      "policy": {
        "default-allow": true
      }
    },
    {
      "@id": "sensitive:main",
      "policy": {
        "identity": "did:fluree:alice",
        "policy-class": ["ex:EmployeePolicy"],
        "default-allow": false
      }
    }
  ],
  "select": ["?data"],
  "where": [{ "@id": "?s", "ex:data": "?data" }]
}
```

**Policy Precedence:**
- Per-source `policy` takes precedence over global `opts` policy
- If a source has no `policy` field, the global policy (if any) applies

## Multi-Ledger Queries

Query across different ledgers:

**JSON-LD Query:**

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": ["customers:main", "orders:main"],
  "select": ["?customer", "?order"],
  "where": [
    { "@id": "?customer", "ex:name": "Alice" },
    { "@id": "?order", "ex:customer": "?customer" }
  ]
}
```

**SPARQL:**

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?customer ?order
FROM <customers:main>
FROM <orders:main>
WHERE {
  ?customer ex:name "Alice" .
  ?order ex:customer ?customer .
}
```

## Time-Aware Datasets

Query graphs at different time points:

**JSON-LD Query:**

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": ["ledger1:main@t:100", "ledger2:main@t:200"],
  "select": ["?data"],
  "where": [
    { "@id": "?entity", "ex:data": "?data" }
  ]
}
```

**SPARQL:**

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?data
FROM <ledger1:main@t:100>
FROM <ledger2:main@t:200>
WHERE {
  ?entity ex:data ?data .
}
```

## Graph Patterns

### Default Graph Only

Query only the default graph:

**SPARQL:**

```sparql
SELECT ?name
FROM <mydb:main>
WHERE {
  ?person ex:name ?name .
  # Matches triples in default graph only
}
```

### Named Graph Only

Query only named graphs:

**SPARQL:**

```sparql
SELECT ?name
FROM NAMED <mydb:main>
WHERE {
  GRAPH <mydb:main> {
    ?person ex:name ?name .
  }
}
```

### Mixed Patterns

Combine default and named graph patterns:

**SPARQL:**

```sparql
PREFIX f: <https://ns.flur.ee/db#>
PREFIX ex: <http://example.org/ns/>

SELECT ?name ?commit ?t
FROM <mydb:main>
FROM NAMED <mydb:main#txn-meta>
WHERE {
  ?person ex:name ?name .
  GRAPH <mydb:main#txn-meta> {
    ?commit f:t ?t .
  }
}
```

## Use Cases

### Data Integration

Combine data from multiple sources:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": ["customers:main", "products:main", "orders:main"],
  "select": ["?customer", "?product", "?order"],
  "where": [
    { "@id": "?customer", "ex:name": "Alice" },
    { "@id": "?order", "ex:customer": "?customer" },
    { "@id": "?order", "ex:product": "?product" }
  ]
}
```

### Cross-Ledger Joins

Join data across different ledgers:

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?customer ?order ?product
FROM <customers:main>
FROM <orders:main>
FROM <products:main>
WHERE {
  ?customer ex:name "Alice" .
  ?order ex:customer ?customer .
  ?order ex:product ?product .
}
```

### SERVICE for Cross-Ledger Queries

Use SPARQL SERVICE to explicitly target specific ledgers within a query:

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?customerName ?productName ?quantity
FROM <customers:main>
FROM NAMED <orders:main>
FROM NAMED <products:main>
WHERE {
  # Get customer from default graph
  ?customer ex:name ?customerName .

  # Get orders from orders ledger
  SERVICE <fluree:ledger:orders:main> {
    ?order ex:customer ?customer ;
           ex:product ?product ;
           ex:quantity ?quantity .
  }

  # Get product details from products ledger
  SERVICE <fluree:ledger:products:main> {
    ?product ex:name ?productName .
  }
}
```

SERVICE provides explicit control over which ledger each pattern executes against, enabling complex cross-ledger joins with clear data provenance.

See [SPARQL Service Queries](sparql.md#service-queries) for full documentation.

### Time-Consistent Queries

Query multiple ledgers at the same point in time:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": [
    "products:main@t:1000",
    "inventory:main@t:1000",
    "pricing:main@t:1000"
  ],
  "select": ["?product", "?stock", "?price"],
  "where": [
    { "@id": "?product", "ex:stockLevel": "?stock" },
    { "@id": "?product", "ex:price": "?price" }
  ]
}
```

## Error Handling

### Common Dataset Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| Duplicate alias | Same `alias` used twice in dataset spec | Use unique aliases for each source |
| Alias collision | `alias` matches an existing `@id` | Choose a different alias name |
| Ambiguous graph selector | Both `#txn-meta` fragment AND `graph` field specified | Use only one method |
| Unknown ledger | Ledger reference not found | Verify ledger exists and is accessible |
| Unknown graph IRI | Graph IRI not found in ledger | Verify graph was ingested and indexed |
| Binary index required | Named graph query requires binary index | Ensure ledger has been indexed |

### Example Error Messages

**Duplicate alias:**
```json
{
  "error": "Duplicate dataset-local alias: 'products' appears multiple times"
}
```

**Ambiguous graph selector:**
```json
{
  "error": "Ambiguous graph selector: cannot specify both #txn-meta fragment and graph field"
}
```

## SPARQL Execution Modes

Fluree supports two SPARQL execution modes:

### Ledger-Bound Mode

When a query targets a single ledger (via endpoint or single FROM clause), GRAPH patterns reference **named graphs within that ledger**:

```sparql
-- Ledger-bound: GRAPH references graphs inside mydb:main
SELECT ?name ?price
FROM <mydb:main>
WHERE {
  GRAPH <http://example.org/graphs/products> {
    ?product ex:name ?name ;
             ex:price ?price .
  }
}
```

### Connection-Bound Mode

When querying across multiple ledgers, use SERVICE to select which ledger each pattern executes against:

```sparql
-- Connection-bound: SERVICE selects the target ledger
SELECT ?name ?stock
WHERE {
  SERVICE <fluree:ledger:sales:main> {
    GRAPH <http://example.org/graphs/products> {
      ?product ex:name ?name .
    }
  }
  SERVICE <fluree:ledger:inventory:main> {
    ?product ex:stock ?stock .
  }
}
```

**When to use each mode:**
- **Ledger-bound**: Single ledger queries, standard SPARQL datasets within one ledger
- **Connection-bound**: Multi-ledger queries, explicit control over data provenance

## Best Practices

1. **Consistent Time Points**: Use the same time specifier for all graphs in a query
2. **Graph Selection**: Use FROM NAMED when you need to identify the source graph
3. **Use Aliases**: Create meaningful aliases for complex graph IRIs or disambiguation
4. **Performance**: Queries across multiple ledgers may be slower
5. **Data Locality**: Consider data locality when designing multi-ledger queries
6. **Policy Granularity**: Use per-source policy when different graphs need different access control

## Related Documentation

- [JSON-LD Query](jsonld-query.md): JSON-LD Query syntax
- [SPARQL](sparql.md): SPARQL syntax
- [Time Travel](../concepts/time-travel.md): Historical queries
- [Datasets and Named Graphs](../concepts/datasets-and-named-graphs.md): Concept documentation
