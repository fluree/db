# Graph Sources

**Differentiator**: Graph sources are one of Fluree's most powerful features, enabling seamless integration of specialized indexes and external data sources directly into graph queries. Unlike traditional databases that require separate systems for full-text search, vector similarity, or data lake access, Fluree makes these capabilities first-class citizens in the query language.

## What Are Graph Sources?

A **graph source** is anything you can address by a graph name/IRI in Fluree query execution. Graph sources may be backed by:
- **Ledger graphs** (default graph and named graphs stored as RDF triples)
- **Index graph sources** (BM25 and vector/HNSW indexes)
- **Mapped graph sources** (R2RML and Iceberg-backed mappings)

### Key Characteristics

- **Query integration**: Graph sources can be queried using the same SPARQL and JSON-LD Query interfaces
- **Transparent access**: Applications don't need to know whether data comes from a ledger graph source or a non-ledger graph source
- **Specialization**: Each graph source type is optimized for specific query patterns
- **Time travel (type-specific)**: Some graph sources support time-travel queries, but support is not uniform across all types. Time-travel is implemented by each graph source type (not by the nameservice).

## Graph Source Types

### BM25 Full-Text Search

**Differentiator**: Fluree includes built-in BM25 full-text search indexing, eliminating the need for separate search systems like Elasticsearch.

**Use Cases:**
- Product search with relevance ranking
- Document search with keyword matching
- Content discovery with fuzzy matching

**Example:**

```json
{
  "@context": {
    "f": "https://ns.flur.ee/db#"
  },
  "from": "products:main",
  "select": ["?product", "?score"],
  "where": [
    {
      "f:graphSource": "products-search:main",
      "f:searchText": "laptop",
      "f:searchLimit": 10,
      "f:searchResult": { "f:resultId": "?product", "f:resultScore": "?score" }
    }
  ],
  "orderBy": [["desc", "?score"]],
  "limit": 10
}
```

**Key Features:**
- Relevance scoring (BM25 algorithm)
- Configurable parameters (k1, b)
- Language-aware search
- Optional time-travel support (BM25-owned manifest; see “Time Travel” below)

See the [BM25 documentation](../indexing-and-search/bm25.md) for details.

### Vector Similarity Search (ANN)

**Differentiator**: Native support for approximate nearest neighbor (ANN) queries via embedded HNSW indexes, enabling semantic search and similarity queries. Can run embedded (in-process) or via a dedicated remote search service.

**Use Cases:**
- Semantic search (find similar documents)
- Recommendation systems
- Image similarity search
- Embedding-based queries

**Key Features:**
- Approximate nearest neighbor search (HNSW algorithm)
- Configurable distance metrics (cosine, euclidean, dot product)
- Embedded indexes (no external service required) or remote mode via `fluree-search-httpd`
- Support for high-dimensional vectors
- Snapshot-based persistence with watermarks (head-only in v1; time-travel not supported)

See the [Vector Search documentation](../indexing-and-search/vector-search.md) for details.

### Apache Iceberg Integration

**Differentiator**: Query Apache Iceberg tables and Parquet files directly as graph sources, enabling seamless integration with data lake architectures.

**Use Cases:**
- Query data lake formats without ETL
- Combine graph data with tabular data
- Analytics queries over large datasets
- Integration with existing data pipelines

**Example:**

```sparql
# Query Iceberg table as graph source
SELECT ?customer ?order ?amount
FROM <iceberg:sales:main>
WHERE {
  ?order ex:customer ?customer .
  ?order ex:amount ?amount .
  FILTER(?amount > 1000)
}
```

**Key Features:**
- Direct querying of Iceberg tables
- Parquet file support
- R2RML mapping for tabular data (Iceberg-backed)
- Time-travel via Iceberg snapshots
- **Direct S3 mode**: bypass REST catalog servers for `iceberg-rust` / self-managed tables — reads `version-hint.text` for automatic version discovery

See the [Iceberg documentation](../graph-sources/iceberg.md) for details.

### R2RML Relational Mapping

**Differentiator**: Map relational databases to RDF using R2RML (R2RML Mapping Language), enabling graph queries over SQL databases.

**Use Cases:**
- Adopt graph queries alongside SQL data sources
- Query SQL databases using SPARQL
- Integrate existing systems
- Unified query interface across data sources

**Example:**

```sparql
# Query relational database via R2RML mapping
SELECT ?customer ?order
FROM <r2rml:orders:main>
WHERE {
  ?customer ex:hasOrder ?order .
  ?order ex:status "pending" .
}
```

**Key Features:**
- R2RML standard compliance
- Automatic RDF mapping from SQL schemas
- Read-only access to source databases
- Support for complex joins and transformations

See the [R2RML documentation](../graph-sources/r2rml.md) for details.

## Graph Source Lifecycle

### Creation

Graph sources are created through administrative operations, specifying:
- **Type**: BM25, Vector, Iceberg, or R2RML
- **Configuration**: Type-specific settings
- **Dependencies**: Source ledgers or data sources
- **Branch**: Graph sources support branching like ledgers

**Example BM25 Graph Source Creation:**

```json
{
  "@type": "f:Bm25Index",
  "f:name": "products-search",
  "f:branch": "main",
  "f:sourceLedger": "products:main",
  "f:config": {
    "k1": 1.2,
    "b": 0.75,
    "fields": ["name", "description"]
  }
}
```

### Indexing

Graph sources maintain their own indexes:
- **BM25**: Full-text indexes are built from source ledger data
- **Vector**: Embeddings stored in HNSW indexes (embedded or remote)
- **Iceberg**: Metadata is cached for efficient querying
- **R2RML**: Mapping rules are applied to generate RDF

### Querying

Graph sources are queried like regular ledgers:

```sparql
# Query any graph source
SELECT ?result
FROM <graph-source-name:branch>
WHERE {
  # Query patterns specific to graph source type
}
```

### Time Travel

Some graph sources support historical queries using the `@t:` syntax in the ledger reference, but the behavior is **graph-source-type specific**:

```json
{
  "@context": { "f": "https://ns.flur.ee/db#" },
  "from": "products:main@t:1000",
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

#### BM25

BM25 can support time travel by maintaining a **BM25-owned manifest** in storage that maps transaction watermarks (`t`) to index snapshot addresses. The nameservice stores only a **head pointer** (an opaque address to the latest BM25 manifest/root) and does not store snapshot history.

#### Vector

Vector search is **head-only** in v1. If a query requests an `@t:` (or otherwise requests an historical view), vector search rejects the request with a clear “time-travel not supported” error.

#### Iceberg

Iceberg time travel (when used) is handled by **Iceberg’s own snapshot/metadata model**, not by nameservice-managed snapshot history.

## Graph Source Architecture

### Nameservice Integration

Graph sources are tracked in the nameservice alongside ledgers:

- **Discovery**: List all graph sources via nameservice
- **Metadata**: Configuration and status stored in nameservice
- **Coordination**: Index state tracked separately from source ledgers

**Important**: for graph sources, the nameservice stores only **configuration** and a **head pointer** (as a ContentId) to the graph source's latest index root/manifest. Snapshot history (if any) lives in graph-source-owned manifests in the content store.

### Query Execution

When querying a graph source:

1. **Resolution**: Query engine resolves graph source from nameservice
2. **Type Detection**: Determines graph source type (BM25, Vector, etc.)
3. **Specialized Execution**: Routes to type-specific query handler
4. **Result Integration**: Results integrated with regular graph queries

### Performance Characteristics

Each graph source type has different performance characteristics:

- **BM25**: Fast keyword search, relevance scoring
- **Vector**: Approximate similarity search, configurable accuracy/speed tradeoff
- **Iceberg**: Columnar storage, efficient for analytical queries
- **R2RML**: Depends on source database performance

## Use Cases

### Multi-Modal Search

Combine full-text search, vector similarity, and graph queries:

```json
{
  "@context": {
    "ex": "http://example.org/",
    "f": "https://ns.flur.ee/db#"
  },
  "from": "products:main",
  "select": ["?product", "?textScore", "?vectorScore"],
  "values": [
    ["?queryVec"],
    [{"@value": [0.1, 0.2, 0.3], "@type": "https://ns.flur.ee/db#embeddingVector"}]
  ],
  "where": [
    { "@id": "?product", "ex:category": "electronics" },
    {
      "f:graphSource": "products-search:main",
      "f:searchText": "wireless",
      "f:searchLimit": 20,
      "f:searchResult": { "f:resultId": "?product", "f:resultScore": "?textScore" }
    },
    {
      "f:graphSource": "products-vector:main",
      "f:queryVector": "?queryVec",
      "f:searchLimit": 10,
      "f:searchResult": { "f:resultId": "?product", "f:resultScore": "?vectorScore" }
    }
  ],
  "orderBy": [["desc", "(?textScore + ?vectorScore)"]]
}
```

Vector/HNSW graph sources are currently queried via JSON-LD Query using `f:*` patterns (e.g. `f:graphSource`, `f:queryVector`, `f:searchResult`). SPARQL query syntax for HNSW vector indexes is not currently available.

### Data Lake Integration

Query both graph and tabular data:

```sparql
SELECT ?customer ?graphData ?lakeData
FROM <customers:main>           # Graph ledger
FROM <iceberg:sales:main>        # Iceberg graph source
WHERE {
  # Graph data
  ?customer ex:preferences ?graphData .
  
  # Data lake data
  GRAPH <iceberg:sales:main> {
    ?sale ex:customer ?customer .
    ?sale ex:total ?lakeData .
  }
}
```

### Hybrid Search

Combine semantic and keyword search:

```json
{
  "@context": {
    "f": "https://ns.flur.ee/db#"
  },
  "from": "documents:main",
  "select": ["?document"],
  "where": [
    {
      "f:graphSource": "documents-search:main",
      "f:searchText": "machine learning",
      "f:searchLimit": 20,
      "f:searchResult": { "f:resultId": "?document" }
    }
  ]
}
```

Semantic similarity via HNSW vector indexes is also queried via JSON-LD Query using `f:*` patterns. SPARQL syntax for BM25 and vector index search is not currently available.

## Best Practices

### Graph Source Design

1. **Choose Appropriate Type**: Match graph source type to query patterns
   - Keyword search → BM25
   - Semantic search → Vector
   - Analytics → Iceberg
   - SQL integration → R2RML

2. **Configuration Tuning**: Optimize graph source parameters
   - BM25: Tune k1 and b for relevance
   - Vector: Choose appropriate distance metric
   - Iceberg: Optimize partition strategy

3. **Dependency Management**: Understand source data dependencies
   - BM25/Vector: Keep in sync with source ledger
   - Iceberg: Handle schema evolution
   - R2RML: Map schema changes

### Performance Optimization

1. **Index Maintenance**: Keep graph source indexes up-to-date
   - Monitor indexing lag
   - Tune indexing frequency
   - Handle large data volumes

2. **Query Planning**: Optimize queries using graph sources
   - Use graph sources for appropriate query patterns
   - Combine with graph queries efficiently
   - Consider cost of graph source queries

3. **Caching**: Cache frequently accessed graph source results
   - Cache query results when appropriate
   - Consider graph source snapshot caching
   - Balance freshness vs performance

### Operational Considerations

1. **Monitoring**: Track graph source health
   - Index build status
   - Query performance
   - Storage usage

2. **Backup**: Include graph sources in backup strategy
   - BM25 indexes can be rebuilt (or restored from stored snapshots/manifests, depending on configuration)
   - Vector indexes are stored as head snapshots (time-travel not supported in v1)
   - Iceberg metadata in nameservice

3. **Scaling**: Plan for graph source scaling
   - BM25: Scale with source ledger size
   - Vector: Scale with embedding count
   - Iceberg: Leverage Iceberg partitioning

## Comparison with Traditional Approaches

### Traditional Architecture

```
Application
    ├── Graph Database (Neo4j, etc.)
    ├── Search Engine (Elasticsearch)
    ├── Vector DB (Pinecone, etc.)
    └── Data Lake (Spark, Presto)
```

**Challenges:**
- Multiple systems to manage
- Data synchronization complexity
- Different query languages
- Separate authentication/authorization

### Fluree Graph Source Architecture

```
Application
    └── Fluree
        ├── Graph Ledgers
        ├── BM25 Graph Sources (built-in)
        ├── Vector Graph Sources
        └── Iceberg Graph Sources
```

**Benefits:**
- Single query interface (SPARQL/JSON-LD Query)
- Unified access control (policy enforcement)
- Consistent time-travel across all data
- Simplified operations and deployment

Graph sources make Fluree a unified platform for graph, search, vector, and data lake queries, eliminating the complexity of managing multiple specialized systems.
