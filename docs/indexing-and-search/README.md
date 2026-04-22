# Indexing and Search

Fluree provides powerful indexing and search capabilities beyond standard graph queries. This section covers background indexing, full-text search, and vector similarity search.

## Index Types

### [Background Indexing](background-indexing.md)

Core database indexing for query performance:
- SPOT, POST, OPST, PSOT indexes
- Automatic index maintenance
- Indexing configuration
- Performance tuning
- Monitoring and metrics

### [Reindex API](reindex.md)

Manual index rebuilding for recovery and maintenance:
- Memory-bounded batched processing
- Checkpointing for resumable operations
- Progress monitoring with callbacks
- Resume after interruption
- Index configuration options

### [Inline Fulltext Search](fulltext.md)

Inline BM25-ranked text scoring. Two entry points, same query surface:
- **`@fulltext` datatype** — per-value annotation (analogous to `@vector`), always English, zero config
- **`f:fullTextDefaults` config** — declare properties + language once at the ledger level; supports 18 languages with Snowball stemming and per-graph overrides for multilingual setups
- `fulltext(?var, "query")` scoring function in `bind` expressions (same for both paths)
- Automatic per-(graph, property, language) fulltext arena construction during background indexing
- Unified scoring across indexed and novelty documents
- Works immediately (no-index fallback) with optimal performance after indexing

### [BM25 Full-Text Search](bm25.md)

Dedicated full-text search indexes using BM25 ranking (for large-scale corpora):
- Creating BM25 indexes via Rust API
- Query-based field selection (indexing query defines what to index)
- BM25 scoring with configurable k1/b parameters
- Block-Max WAND for efficient top-k queries
- Incremental index updates via property-dependency tracking

### [Vector Search](vector-search.md)

Approximate nearest neighbor (ANN) search for embeddings:
- Vector index configuration
- Embedded HNSW indexes (in-process) or remote via dedicated search service
- Embedding storage with `@vector` datatype (resolves to `https://ns.flur.ee/db#embeddingVector`)
- Similarity queries via `f:*` syntax
- Deployment modes (embedded / remote)
- Use cases (semantic search, recommendations)

### [Geospatial](geospatial.md)

Geographic point data with native binary encoding:
- `geo:wktLiteral` datatype support (OGC GeoSPARQL)
- Automatic POINT geometry detection and optimization
- Packed 60-bit lat/lng encoding (~0.3mm precision)
- Foundation for proximity queries (latitude-band index scans)

## Indexing Architecture

Fluree maintains multiple index types for different query patterns:

**Core Indexes (automatic):**
- SPOT: Subject-Predicate-Object-Time
- POST: Predicate-Object-Subject-Time
- OPST: Object-Predicate-Subject-Time
- PSOT: Predicate-Subject-Object-Time

**Graph Source Indexes (explicit):**
- BM25: Full-text search indexes
- Vector: Embedding similarity indexes
- R2RML: Relational database views
- Iceberg: Data lake integrations

## Background Indexing

Core database indexing happens automatically:

```text
Transaction → Commit → Background Indexer → Index Published
```

**Process:**
1. Transaction committed (t assigned)
2. Commit published to nameservice
3. Background indexer detects new commit
4. Indexes updated (SPOT, POST, OPST, PSOT)
5. Index snapshot published

**Novelty Layer:**
- Gap between latest commit and latest index
- Queries combine indexed data + novelty
- Monitored via `commit_t - index_t`

See [Background Indexing](background-indexing.md) for details.

## Inline Fulltext Search

For small-to-medium corpora (up to hundreds of thousands of documents per predicate), inline fulltext search provides BM25-ranked scoring with zero configuration:

**Annotate data:**
```json
{
  "@id": "ex:article-1",
  "ex:content": {
    "@value": "Rust is a systems programming language focused on safety",
    "@type": "@fulltext"
  }
}
```

**Query with scoring:**
```json
{
  "select": ["?title", "?score"],
  "where": [
    { "@id": "?doc", "ex:content": "?content", "ex:title": "?title" },
    ["bind", "?score", "(fulltext ?content \"Rust programming\")"],
    ["filter", "(> ?score 0)"]
  ],
  "orderBy": [["desc", "?score"]],
  "limit": 10
}
```

See [Inline Fulltext Search](fulltext.md) for details.

## Full-Text Search (BM25 Graph Source)

For larger corpora (1M+ documents) with strict latency requirements, the BM25 graph source pipeline provides WAND-based top-k pruning, chunked posting lists, and incremental updates:

BM25 provides ranked full-text search:

**Creating Index (Rust API):**
```rust
use fluree_db_api::Bm25CreateConfig;
use serde_json::json;

let query = json!({
    "@context": { "schema": "http://schema.org/" },
    "where": [{ "@id": "?x", "@type": "schema:Product", "schema:name": "?name" }],
    "select": { "?x": ["@id", "schema:name", "schema:description"] }
});
let config = Bm25CreateConfig::new("products-search", "mydb:main", query);
let result = fluree.create_full_text_index(config).await?;
```

There are no HTTP endpoints for index management yet — indexes are managed via the Rust API.

**Searching:**
```json
{
  "@context": {"f": "https://ns.flur.ee/db#"},
  "from": "mydb:main",
  "select": ["?product", "?score"],
  "where": [
    {
      "f:graphSource": "products-search:main",
      "f:searchText": "laptop computer",
      "f:searchLimit": 20,
      "f:searchResult": { "f:resultId": "?product", "f:resultScore": "?score" }
    }
  ],
  "orderBy": ["-?score"]
}
```

See [BM25](bm25.md) for details.

## Vector Search

Similarity search using vector embeddings via HNSW indexes (embedded or remote).

**Important**: Embeddings must be stored with the vector datatype (`@type: "@vector"`, `@type: "f:embeddingVector"`, or full IRI `https://ns.flur.ee/db#embeddingVector`) to preserve array structure.

**Creating Index (Rust API):**
```rust
let config = VectorCreateConfig::new(
    "products-vector", "mydb:main", query, "ex:embedding", 384
);
fluree.create_vector_index(config).await?;
```

**Searching:**
```json
{
  "from": "mydb:main",
  "select": ["?product", "?score"],
  "where": [
    {
      "f:graphSource": "products-vector:main",
      "f:queryVector": [0.1, 0.2, ..., 0.9],
      "f:searchLimit": 10,
      "f:searchResult": {
        "f:resultId": "?product",
        "f:resultScore": "?score"
      }
    }
  ]
}
```

See [Vector Search](vector-search.md) for details.

## Index as Graph Sources

Search indexes are exposed as graph sources:

**Graph Source Names:**
- `products-search:main` - BM25 index
- `products-vector:main` - Vector index

**Query Like Regular Ledgers:**
```json
{
  "@context": {"f": "https://ns.flur.ee/db#"},
  "from": "mydb:main",
  "select": ["?product", "?name", "?score"],
  "where": [
    {
      "f:graphSource": "products-search:main",
      "f:searchText": "laptop",
      "f:searchLimit": 20,
      "f:searchResult": { "f:resultId": "?product", "f:resultScore": "?score" }
    },
    { "@id": "?product", "schema:name": "?name" }
  ]
}
```

Combines structured data with search results via the `f:graphSource` pattern.

## Index Management

### Creating Indexes

BM25 and vector indexes are created via the Rust API. See [BM25](bm25.md) and [Vector Search](vector-search.md) for details.

### Updating Indexes

BM25 indexes are **not** automatically updated when the source ledger changes. They must be explicitly synced:

```rust
// Incremental sync (detects changes since last watermark)
let result = fluree.sync_bm25_index("products-search:main").await?;

// Or use the Bm25MaintenanceWorker for automatic background syncing
```

The `Bm25MaintenanceWorker` can be configured to watch for ledger commits and sync automatically.

### Deleting Indexes

```rust
let result = fluree.drop_full_text_index("products-search:main").await?;
```

## Performance Characteristics

### Inline Fulltext Search

- **Indexed throughput**: ~625,000 docs/sec (50K paragraph-length docs in 80ms)
- **Novelty throughput**: ~85,000 docs/sec (50K docs in ~600ms, no index required)
- **Indexed speedup**: 7-7.5x faster than novelty-only
- **Scaling**: Near-linear; ~625K docs within a 1-second query budget
- **Arena build**: Adds minimal overhead to the normal binary index build

### BM25 Search

- **Index Build Time**: O(n) for n documents
- **Top-k Query Time**: Sub-linear via Block-Max WAND — skips posting list segments that cannot contribute to the top-k, with early termination. Falls back to O(total matching postings) when k approaches corpus size.
- **Space**: ~2-3x document size
- **Updates**: Incremental via property-dependency tracking, O(changed docs)

### Vector Search

- **Flat scan (inline functions)**: O(n) brute-force, viable up to ~100K vectors with binary indexing; binary index provides ~6x speedup over novelty-only scans and ~25x for filtered queries
- **HNSW index**: O(log n) approximate nearest neighbor, recommended for 100K+ vectors or strict latency requirements
- **Space**: ~1.5x embedding size
- **Updates**: Incremental, O(1) per vector
- See [Vector Search -- Performance and Scaling](vector-search.md#performance-and-scaling) for benchmark data and guidance on when to adopt HNSW

### Combined Queries

Combine search with graph queries:

```json
{
  "@context": {"f": "https://ns.flur.ee/db#"},
  "from": "mydb:main",
  "select": ["?product", "?category"],
  "where": [
    {
      "f:graphSource": "products-search:main",
      "f:searchText": "laptop",
      "f:searchLimit": 20,
      "f:searchResult": { "f:resultId": "?product" }
    },
    { "@id": "?product", "schema:category": "?category" }
  ]
}
```

Query optimizer handles joins between the search graph source and structured data efficiently.

## Use Cases

### Full-Text Search

**E-commerce Product Search:**
```json
{
  "@context": {"f": "https://ns.flur.ee/db#"},
  "from": "products:main",
  "select": ["?product", "?score"],
  "where": [
    {
      "f:graphSource": "products-search:main",
      "f:searchText": "wireless headphones",
      "f:searchLimit": 20,
      "f:searchResult": { "f:resultId": "?product", "f:resultScore": "?score" }
    }
  ],
  "orderBy": ["-?score"]
}
```

**Document Management:**
```json
{
  "@context": {"f": "https://ns.flur.ee/db#"},
  "from": "documents:main",
  "where": [
    {
      "f:graphSource": "documents-search:main",
      "f:searchText": "quarterly report 2024",
      "f:searchLimit": 20,
      "f:searchResult": { "f:resultId": "?doc" }
    },
    { "@id": "?doc", "ex:department": "finance" }
  ]
}
```

### Vector Similarity

**Semantic Search:**
```json
{
  "from": "articles:main",
  "values": [
    ["?queryVec"],
    [{"@value": [0.1, 0.2, 0.3], "@type": "https://ns.flur.ee/db#embeddingVector"}]
  ],
  "where": [
    {
      "f:graphSource": "articles-vector:main",
      "f:queryVector": "?queryVec",
      "f:searchLimit": 10,
      "f:searchResult": {
        "f:resultId": "?article",
        "f:resultScore": "?vecScore"
      }
    }
  ],
  "select": ["?article", "?vecScore"],
  "orderBy": [["desc", "?vecScore"]]
}
```

**Recommendation Engine:**
```json
{
  "from": "products:main",
  "where": [
    {
      "@id": "ex:product-123",
      "ex:embedding": "?queryVec"
    },
    {
      "f:graphSource": "products-vector:main",
      "f:queryVector": "?queryVec",
      "f:searchLimit": 5,
      "f:searchResult": { "f:resultId": "?similar", "f:resultScore": "?vecScore" }
    }
  ],
  "select": ["?similar", "?vecScore"],
  "orderBy": [["desc", "?vecScore"]]
}
```

### Hybrid Search

Combine text and vector search:

```json
{
  "@context": {"f": "https://ns.flur.ee/db#"},
  "from": "products:main",
  "values": [
    ["?queryVec"],
    [{"@value": [0.1, 0.2, 0.3], "@type": "https://ns.flur.ee/db#embeddingVector"}]
  ],
  "where": [
    {
      "f:graphSource": "products-search:main",
      "f:searchText": "laptop",
      "f:searchLimit": 100,
      "f:searchResult": { "f:resultId": "?product", "f:resultScore": "?textScore" }
    },
    {
      "f:graphSource": "products-vector:main",
      "f:queryVector": "?queryVec",
      "f:searchLimit": 100,
      "f:searchResult": { "f:resultId": "?product", "f:resultScore": "?vecScore" }
    }
  ],
  "bind": {
    "?finalScore": "(?textScore * 0.6) + (?vecScore * 0.4)"
  },
  "orderBy": ["-?finalScore"]
}
```

## Monitoring

### Check BM25 Staleness

Check whether a BM25 index is behind its source ledger:

```rust
let check = fluree.check_bm25_staleness("products-search:main").await?;
println!("Index at t={}, ledger at t={}, stale: {}, lag: {}",
    check.index_t, check.ledger_t, check.is_stale, check.lag);
```

### Background Maintenance

The `Bm25MaintenanceWorker` watches for source ledger commits and syncs indexes automatically:
- Debounces rapid commits (configurable interval)
- Bounded concurrency for concurrent sync operations
- Registers/unregisters graph sources dynamically

## Best Practices

### 1. Choose Appropriate Index Type

- **Structured queries**: Use core graph indexes
- **Keyword search (< 500K docs)**: Use inline `@fulltext` for zero-config BM25 scoring
- **Keyword search (1M+ docs)**: Use the BM25 graph source for WAND-optimized top-k retrieval
- **Semantic similarity**: Use vector search
- **Hybrid**: Combine multiple indexes

### 2. Tune BM25 Parameters

Adjust k1 and b for your corpus:

```rust
let config = Bm25CreateConfig::new("search", "docs:main", query)
    .with_k1(1.5)  // Higher = more weight to term frequency (default: 1.2)
    .with_b(0.5);   // Lower = less document length normalization (default: 0.75)
```

The indexing query controls **which properties** are indexed — all selected text properties contribute to the document's searchable content.

### 3. Monitor Index Staleness

Check staleness after bulk operations:

```rust
let check = fluree.check_bm25_staleness("search:main").await?;
if check.is_stale {
    fluree.sync_bm25_index("search:main").await?;
}
```

### 4. Sync After Bulk Updates

BM25 indexes require explicit sync. After bulk inserts, sync once at the end:

```rust
// Insert many documents...
for batch in batches {
    fluree.insert(ledger.clone(), &batch).await?;
}
// Sync the BM25 index once after all inserts
fluree.sync_bm25_index("products-search:main").await?;
```

### 5. Use Appropriate Limits

Limit results for performance:

```json
{
  "@context": {"f": "https://ns.flur.ee/db#"},
  "from": "docs:main",
  "where": [
    {
      "f:graphSource": "docs-search:main",
      "f:searchText": "search query",
      "f:searchLimit": 100,
      "f:searchResult": { "f:resultId": "?doc" }
    }
  ]
}
```

## Related Documentation

- [Background Indexing](background-indexing.md) - Core index details
- [Inline Fulltext Search](fulltext.md) - `@fulltext` datatype and `fulltext()` scoring
- [BM25](bm25.md) - Dedicated full-text search graph source
- [Vector Search](vector-search.md) - Similarity search
- [Graph Sources](../graph-sources/README.md) - Graph source concepts
- [Query](../query/README.md) - Query syntax
