# Query

Fluree supports two powerful query languages for querying graph data: **JSON-LD Query** (Fluree's native query language) and **SPARQL** (the W3C standard). Both languages provide access to Fluree's unique features including time travel, graph sources, and policy enforcement.

## Query Languages

### [JSON-LD Query](jsonld-query.md)

Fluree's native query language that uses JSON-LD syntax. JSON-LD Query provides a natural, JSON-based interface for querying graph data, making it easy to integrate with modern applications.

**Key Features:**
- JSON-based syntax (no string parsing)
- Full support for time travel (`@t:`, `@iso:`, `@commit:`)
- Graph source integration
- Policy enforcement
- History queries

### [SPARQL](sparql.md)

Industry-standard SPARQL 1.1 query language. Fluree provides full SPARQL support, enabling compatibility with existing RDF tools and knowledge graphs.

**Key Features:**
- W3C SPARQL 1.1 compliant
- FROM and FROM NAMED clauses
- CONSTRUCT queries
- Time travel support (planned)
- Standard SPARQL functions

## Query Features

### [Output Formats](output-formats.md)

Fluree supports multiple output formats for query results:
- **JSON-LD**: Compact, context-aware JSON with IRI expansion/compaction
- **SPARQL JSON**: Standard SPARQL result format
- **Typed JSON**: Type-preserving JSON with datatype information

### [Datasets and Multi-Graph Execution](datasets.md)

Query across multiple graphs and ledgers:
- **FROM clauses**: Specify default graphs
- **FROM NAMED**: Query named graphs
- **Multi-ledger queries**: Query across different ledgers
- **Time-aware datasets**: Query graphs at different time points

### [CONSTRUCT Queries](construct.md)

Generate RDF graphs from query results:
- Transform query results into RDF
- Create new graph structures
- Extract subgraphs

### [Graph Crawl](graph-crawl.md)

Traverse graph relationships:
- Follow links between entities
- Recursive graph traversal
- Depth-limited crawling

### [Explain Plans](explain.md)

Understand query execution:
- View query plans
- Analyze index usage
- Optimize query performance

### [Tracking and Fuel Limits](tracking-and-fuel.md)

Monitor and control query execution:
- Query tracking and debugging
- Fuel limits for resource control
- Performance monitoring

### Nameservice Queries

Query metadata about all ledgers and graph sources in the system. The nameservice stores information about every database including commit state, index state, and configuration.

**JSON-LD Query:**
```json
{
  "@context": {"f": "https://ns.flur.ee/db#"},
  "select": ["?ledger", "?t"],
  "where": [
    { "@id": "?ns", "@type": "f:LedgerSource", "f:ledger": "?ledger", "f:t": "?t" }
  ]
}
```

**SPARQL:**
```sparql
PREFIX f: <https://ns.flur.ee/db#>
SELECT ?ledger ?t WHERE { ?ns a f:LedgerSource ; f:ledger ?ledger ; f:t ?t }
```

See the [Ledgers and Nameservice](../concepts/ledgers-and-nameservice.md) concept documentation for details.

## Time Travel in Queries

Fluree supports querying historical data using time specifiers in ledger references:

**Transaction Number:**
```
ledger:main@t:100
```

**ISO 8601 Timestamp:**
```
ledger:main@iso:2024-01-15T10:30:00Z
```

**Commit ContentId:**
```
ledger:main@commit:bafybeig...
```

See the [Time Travel](../concepts/time-travel.md) concept documentation for details.

## Graph Source Queries

Query graph sources (BM25, Vector, Iceberg, R2RML) using the same syntax as regular ledgers:

```json
{
  "@context": {
    "f": "https://ns.flur.ee/db#"
  },
  "from": "products:main",
  "select": ["?product"],
  "where": [
    {
      "f:graphSource": "products-search:main",
      "f:searchText": "laptop",
      "f:searchLimit": 20,
      "f:searchResult": { "f:resultId": "?product" }
    }
  ]
}
```

See the [Graph Sources](../concepts/graph-sources.md) concept documentation for details.

## Policy Enforcement

Policies are automatically enforced during query execution, ensuring users only see data they're authorized to access. No special syntax is required—policies are applied transparently.

See the [Policy Enforcement](../concepts/policy-enforcement.md) concept documentation for details.

## Getting Started

### Basic JSON-LD Query

```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "select": ["?name"],
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ]
}
```

### Basic SPARQL Query

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?name
WHERE {
  ?person ex:name ?name .
}
```

### Query with Time Travel

```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "from": "ledger:main@t:100",
  "select": ["?name"],
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ]
}
```

## Query Performance

Fluree's query engine is optimized for:
- **Automatic Join Ordering**: The planner reorders all WHERE-clause patterns
  (triples, UNION, OPTIONAL, MINUS, search patterns, and more) using
  statistics-driven cardinality estimates. When database statistics are
  available, it uses HLL-derived property counts; otherwise it falls back to
  heuristic constants. Estimates are context-aware — the planner tracks which
  variables are already bound and adjusts costs accordingly, so a triple
  whose subject is bound from an earlier pattern is scored as a cheap
  per-subject lookup rather than a full scan.
- **Index Selection**: Automatically chooses optimal indexes (SPOT, POST,
  OPST, PSOT) based on which triple components are bound.
- **Filter Optimization**: Filters are automatically applied as soon as their
  required variables are bound, regardless of where they appear in the query.
  Range-safe filters are pushed down to index scans, and filters are
  evaluated inline during joins when possible.
- **Streaming Execution**: Results stream as they're computed
- **Parallel Processing**: Parallel execution where possible

## Best Practices

1. **Use Appropriate Indexes**: Structure queries to leverage indexes
2. **Limit Result Sets**: Use LIMIT clauses for large result sets
3. **Time Travel Efficiency**: Use `@t:` when transaction numbers are known
4. **Graph Source Selection**: Choose appropriate graph sources for query patterns
5. **Policy Awareness**: Understand how policies affect query results

## Related Documentation

- [Concepts](../concepts/README.md): Core concepts including time travel, graph sources, and policy
- [Transactions](../transactions/README.md): Writing data to Fluree
- [Security and Policy](../security/README.md): Policy configuration and management
