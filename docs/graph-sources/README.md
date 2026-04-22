# Graph Sources and Integrations

Graph sources extend Fluree's query capabilities by integrating specialized indexes and external data sources. Graph sources appear as queryable ledgers but are backed by different storage and indexing systems.

## Graph Source Types

### [Overview](overview.md)

Introduction to graph sources:
- What are graph sources
- Architecture and design
- Use cases
- Performance characteristics
- Creating and managing graph sources

### [Iceberg / Parquet](iceberg.md)

Apache Iceberg data lake integration:
- Querying Iceberg tables
- Parquet file support
- Schema mapping
- Partition pruning
- Performance optimization

### [R2RML](r2rml.md)

Relational database mapping:
- R2RML standard
- Mapping relational data to RDF
- SQL query generation
- Join optimization
- Supported databases (PostgreSQL, MySQL, etc.)

### [BM25 Graph Source](bm25.md)

Full-text search as graph source:
- BM25 index as queryable ledger
- Search predicates
- Combining with structured queries
- Real-time index updates

## What are Graph Sources?

Graph sources are queryable data sources that appear as Fluree ledgers but are backed by specialized storage:

**Standard Ledger:**
```text
mydb:main → RDF triple store → SPOT/POST/OPST/PSOT indexes
```

**Graph Source:**
```text
products-search:main → BM25 index → Inverted text index
products-vector:main → HNSW → Vector similarity index
warehouse-data:main → Iceberg → Parquet files
sql-db:main → R2RML → PostgreSQL tables
```

## Query Transparency

Graph sources are queried like regular ledgers:

```json
{
  "@context": {"f": "https://ns.flur.ee/db#"},
  "from": "products:main",
  "select": ["?product", "?score"],
  "where": [
    {
      "f:graphSource": "products-search:main",
      "f:searchText": "laptop",
      "f:searchLimit": 20,
      "f:searchResult": { "f:resultId": "?product", "f:resultScore": "?score" }
    }
  ]
}
```

> **Note:** SPARQL queries use the same `f:` namespace pattern (`f:graphSource`, `f:searchText`, etc.) within JSON-LD query syntax.

## Multi-Graph Queries

Combine regular ledgers with graph sources:

```json
{
  "@context": {"f": "https://ns.flur.ee/db#"},
  "from": "products:main",
  "select": ["?product", "?name", "?price", "?score"],
  "where": [
    {
      "f:graphSource": "products-search:main",
      "f:searchText": "laptop",
      "f:searchLimit": 20,
      "f:searchResult": { "f:resultId": "?product", "f:resultScore": "?score" }
    },
    { "@id": "?product", "schema:name": "?name" },
    { "@id": "?product", "schema:price": "?price" }
  ],
  "orderBy": ["-?score"]
}
```

Joins structured data from products:main with search results from the products-search:main graph source.

## Graph Source Lifecycle

### 1. Create Graph Source

Define mapping/configuration:

```bash
curl -X POST http://localhost:8090/index/bm25?ledger=mydb:main \
  -d '{"name": "products-search", "fields": [...]}'
```

### 2. Initial Indexing

Build index from source data:
- Load data from source ledger
- Transform to target format
- Build specialized index
- Publish to nameservice

### 3. Incremental Updates

Keep synchronized with source:
- Monitor source ledger for changes
- Update graph source incrementally
- Maintain consistency

### 4. Query Execution

Execute queries against graph source:
- Parse query
- Route to appropriate backend
- Execute specialized query
- Return results

## Supported Graph Sources

### BM25 Full-Text Search

**Purpose:** Keyword search with relevance ranking

**Backend:** Inverted index

**Use Cases:**
- E-commerce product search
- Document search
- Knowledge base search

**Example:**
```json
{
  "@context": {"f": "https://ns.flur.ee/db#"},
  "from": "docs:main",
  "where": [
    {
      "f:graphSource": "docs-search:main",
      "f:searchText": "quarterly report",
      "f:searchLimit": 20,
      "f:searchResult": { "f:resultId": "?doc" }
    }
  ]
}
```

See [BM25 Graph Source](bm25.md) and [BM25 Indexing](../indexing-and-search/bm25.md).

### Vector Similarity Search

**Purpose:** Semantic search using embeddings

**Backend:** HNSW index (embedded or remote)

**Use Cases:**
- Semantic search
- Recommendations
- Image similarity
- Clustering

See [Vector Search](../indexing-and-search/vector-search.md) for details.

### Apache Iceberg

**Purpose:** Query data lake tables

**Backend:** Apache Iceberg / Parquet files

**Use Cases:**
- Analytics on historical data
- Data warehouse integration
- Large-scale batch data

**Example:**
```json
{
  "from": "warehouse-sales:main",
  "select": ["?date", "?revenue"],
  "where": [
    { "@id": "?sale", "warehouse:date": "?date" },
    { "@id": "?sale", "warehouse:revenue": "?revenue" }
  ],
  "filter": "?date >= '2024-01-01'"
}
```

See [Iceberg / Parquet](iceberg.md).

### R2RML (Relational Databases)

**Purpose:** Query relational databases as RDF

**Backend:** SQL databases (PostgreSQL, MySQL, etc.)

**Use Cases:**
- Existing database integration
- Incremental adoption of graph queries
- Unified queries across systems

**Example:**
```json
{
  "from": "sql-customers:main",
  "select": ["?name", "?email"],
  "where": [
    { "@id": "?customer", "schema:name": "?name" },
    { "@id": "?customer", "schema:email": "?email" }
  ]
}
```

See [R2RML](r2rml.md).

## Architecture

### Graph Source Registry

Graph sources registered in nameservice:

```json
{
  "graph_source_id": "products-search:main",
  "type": "bm25",
  "source": "products:main",
  "backend": "inverted_index",
  "status": "ready"
}
```

### Query Routing

Query engine routes to appropriate backend:

```text
Query: FROM <products-search:main>
  ↓
Nameservice lookup: type=bm25
  ↓
Route to BM25 query engine
  ↓
Execute against inverted index
  ↓
Return results
```

### Result Integration

Results from graph sources join with regular graphs:

```text
FROM <products:main>, <products-search:main>
  ↓
Execute subquery on products:main → Results A
Execute subquery on products-search:main → Results B
  ↓
Join Results A + B on ?product
  ↓
Return combined results
```

## Performance Considerations

### Query Planning

Graph sources affect query optimization:
- Specialized indexes enable efficient filtering
- Push filters down to graph source when possible
- Minimize data transfer between graphs

### Data Transfer

Minimize data movement:
- Filter in graph source before joining
- Use selective projections
- Leverage graph source's native capabilities

### Caching

Some graph source backends support caching:
- BM25: Results cacheable
- Vector: Similar queries share computation
- Iceberg: Parquet file caching
- R2RML: SQL query plan caching

## Best Practices

### 1. Choose Appropriate Graph Source Type

Match graph source to use case:
- Keyword search → BM25
- Semantic search → Vector
- Analytics → Iceberg
- Relational database integration → R2RML

### 2. Filter Early

Push filters to graph sources:

Good:
```json
{
  "@context": {"f": "https://ns.flur.ee/db#"},
  "from": "products:main",
  "where": [
    {
      "f:graphSource": "products-search:main",
      "f:searchText": "laptop",
      "f:searchLimit": 50,
      "f:searchResult": { "f:resultId": "?p" }
    },
    { "@id": "?p", "schema:price": "?price" }
  ],
  "filter": "?price < 1000"
}
```

### 3. Monitor Graph Source Lag

Check synchronization status:

```bash
curl http://localhost:8090/index/status/products-search:main
```

### 4. Use Appropriate Limits

Limit results from graph sources:

```json
{
  "@context": {"f": "https://ns.flur.ee/db#"},
  "from": "products:main",
  "where": [
    {
      "f:graphSource": "products-search:main",
      "f:searchText": "query",
      "f:searchLimit": 100,
      "f:searchResult": { "f:resultId": "?p" }
    }
  ]
}
```

### 5. Test Performance

Profile queries combining graph sources:

```bash
curl -X POST http://localhost:8090/v1/fluree/explain \
  -d '{...}'
```

## Troubleshooting

### Graph Source Not Found

```json
{
  "error": "GraphSourceNotFound",
  "message": "Graph source not found: products-search:main"
}
```

**Solution:** Create graph source or check name spelling.

### Synchronization Lag

Graph source out of sync with source:

```bash
# Check status
curl http://localhost:8090/index/status/products-search:main

# Trigger rebuild
curl -X POST http://localhost:8090/index/rebuild/products-search:main
```

### Poor Performance

Query combining graph sources is slow:

1. Check explain plan
2. Add filters to reduce result set
3. Ensure indexes are up-to-date
4. Consider query rewrite

## Related Documentation

- [Overview](overview.md) - Graph source concepts
- [BM25](bm25.md) - Full-text search
- [Vector Search](../indexing-and-search/vector-search.md) - Similarity search
- [Iceberg](iceberg.md) - Data lake integration
- [R2RML](r2rml.md) - Relational mapping
- [Query Datasets](../query/datasets.md) - Multi-graph queries
