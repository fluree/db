# Vector Search

Vector search enables similarity search using embedding vectors, supporting use cases like:

- **Semantic search**: Find similar meanings, not just keywords
- **Recommendations**: Find similar products, content, users
- **Image search**: Find similar images by visual features
- **Anomaly detection**: Find unusual patterns

Fluree supports two complementary approaches:

1. **Inline similarity functions** -- compute `dotProduct`, `cosineSimilarity`, or `euclideanDistance` directly in queries using `bind`. No external index required.
2. **HNSW vector indexes** -- build dedicated approximate-nearest-neighbor (ANN) indexes for large-scale similarity search using the `f:*` query pattern.

## The `@vector` Datatype

### Why a dedicated datatype?

In RDF, a plain JSON array like `[0.5, 0.5, 0.0]` is decomposed into individual values. Duplicate elements can be deduplicated, and ordering is not guaranteed. This breaks embedding vectors. The `@vector` datatype tells Fluree to store the array as a single, ordered, fixed-length vector.

`@vector` is a shorthand for the full IRI `https://ns.flur.ee/db#embeddingVector`, which can also be written as `f:embeddingVector` when the Fluree namespace prefix is declared in your `@context`.

### Storage: f32 precision contract

All `@vector` values are stored as **IEEE-754 binary32 (f32)** arrays. This means:

- Each element in your JSON array is quantized to f32 at ingest time
- Values that are not representable as finite f32 (NaN, Infinity, values exceeding f32 range) are rejected
- Round-trip reads return the f32-quantized values (e.g., `0.1` in JSON becomes `0.10000000149011612` after f32 quantization)
- This provides a compact, cache-friendly representation optimized for SIMD similarity computation

If you need higher precision (f64) or different vector formats (sparse, integer), store them as a custom RDF datatype string.

### Inserting vectors (JSON-LD)

Use `"@type": "@vector"` to annotate a numeric array as a vector:

```json
{
  "@context": {
    "ex": "http://example.org/"
  },
  "@graph": [
    {
      "@id": "ex:doc1",
      "@type": "ex:Document",
      "ex:embedding": {
        "@value": [0.1, 0.2, 0.3, 0.4],
        "@type": "@vector"
      }
    }
  ]
}
```

You can also use the full IRI or the `f:` prefix form, which is equivalent:

```json
{
  "@context": {
    "ex": "http://example.org/",
    "f": "https://ns.flur.ee/db#"
  },
  "@graph": [
    {
      "@id": "ex:doc1",
      "ex:embedding": {
        "@value": [0.1, 0.2, 0.3, 0.4],
        "@type": "f:embeddingVector"
      }
    }
  ]
}
```

**Incorrect -- plain array (will not work for similarity):**

```json
{
  "@id": "ex:doc1",
  "ex:embedding": [0.1, 0.2, 0.3, 0.4]
}
```

Plain arrays are decomposed into individual RDF values where duplicates may be removed and order is lost.

### Inserting vectors (Turtle / SPARQL UPDATE)

In Turtle and SPARQL UPDATE, the `@vector` shorthand is not available. Use the `f:embeddingVector` datatype IRI with the standard `^^` typed-literal syntax:

```sparql
PREFIX ex: <http://example.org/>
PREFIX f: <https://ns.flur.ee/db#>

INSERT DATA {
  ex:doc1 ex:embedding "[0.1, 0.2, 0.3, 0.4]"^^f:embeddingVector .
}
```

The vector is represented as a JSON array string with the `^^f:embeddingVector` datatype annotation.

### Multiple vectors per entity

An entity can have multiple vectors on the same property:

```json
{
  "@id": "ex:doc1",
  "ex:embedding": [
    {"@value": [0.1, 0.9], "@type": "@vector"},
    {"@value": [0.2, 0.8], "@type": "@vector"}
  ]
}
```

Each vector produces separate rows in query results.

### Vector literals in query VALUES clauses

When passing a vector literal in a query `values` clause, use the full IRI or the `f:` prefix form -- the `@vector` shorthand is only resolved in the transaction parser:

```json
"values": [
  ["?queryVec"],
  [{"@value": [0.7, 0.6], "@type": "f:embeddingVector"}]
]
```

Or with the full IRI:

```json
"values": [
  ["?queryVec"],
  [{"@value": [0.7, 0.6], "@type": "https://ns.flur.ee/db#embeddingVector"}]
]
```

## Inline Similarity Functions (JSON-LD Query)

Fluree provides three vector similarity functions that can be used in `bind` expressions within JSON-LD queries. These compute similarity scores directly during query execution without requiring a pre-built index.

Function names are case-insensitive; `dotProduct`, `dotproduct`, and `dot_product` are all equivalent.

### dotProduct

Computes the dot product (inner product) of two vectors. Higher scores indicate greater similarity when vectors represent aligned directions.

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "f": "https://ns.flur.ee/db#"
  },
  "select": ["?doc", "?score"],
  "values": [
    ["?queryVec"],
    [{"@value": [0.7, 0.6], "@type": "f:embeddingVector"}]
  ],
  "where": [
    {"@id": "?doc", "ex:embedding": "?vec"},
    ["bind", "?score", "(dotProduct ?vec ?queryVec)"]
  ],
  "orderBy": [["desc", "?score"]],
  "limit": 10
}
```

**Score range**: (-inf, +inf). Best when vector magnitude encodes importance.

### cosineSimilarity

Computes the cosine of the angle between two vectors. Ignores magnitude, focusing purely on directional similarity.

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "f": "https://ns.flur.ee/db#"
  },
  "select": ["?doc", "?score"],
  "values": [
    ["?queryVec"],
    [{"@value": [0.7, 0.6], "@type": "f:embeddingVector"}]
  ],
  "where": [
    {"@id": "?doc", "ex:embedding": "?vec"},
    ["bind", "?score", "(cosineSimilarity ?vec ?queryVec)"]
  ],
  "orderBy": [["desc", "?score"]],
  "limit": 10
}
```

**Score range**: [-1, 1] (1 = identical direction, 0 = orthogonal, -1 = opposite). Returns `null` if either vector has zero magnitude. Best for text embeddings and normalized vectors.

### euclideanDistance

Computes the L2 (straight-line) distance between two vectors. Lower scores indicate greater similarity.

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "f": "https://ns.flur.ee/db#"
  },
  "select": ["?doc", "?distance"],
  "values": [
    ["?queryVec"],
    [{"@value": [0.7, 0.6], "@type": "f:embeddingVector"}]
  ],
  "where": [
    {"@id": "?doc", "ex:embedding": "?vec"},
    ["bind", "?distance", "(euclideanDistance ?vec ?queryVec)"]
  ],
  "orderBy": "?distance",
  "limit": 10
}
```

**Score range**: [0, +inf) (0 = identical). Best for geometric similarity and when absolute position matters.

### Alternative array syntax

The similarity functions also accept array form instead of the S-expression string:

```json
["bind", "?score", ["dotProduct", "?vec", "?queryVec"]]
```

This is equivalent to:

```json
["bind", "?score", "(dotProduct ?vec ?queryVec)"]
```

### Filtering by score threshold

Combine `bind` with `filter` to return only results above a similarity threshold:

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "f": "https://ns.flur.ee/db#"
  },
  "select": ["?doc", "?score"],
  "values": [
    ["?queryVec"],
    [{"@value": [0.7, 0.6], "@type": "f:embeddingVector"}]
  ],
  "where": [
    {"@id": "?doc", "ex:embedding": "?vec"},
    ["bind", "?score", "(dotProduct ?vec ?queryVec)"],
    ["filter", "(> ?score 0.7)"]
  ]
}
```

### Combining with graph patterns

Vector similarity can be combined with standard graph patterns to filter by type, property values, or relationships:

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "f": "https://ns.flur.ee/db#"
  },
  "select": ["?doc", "?title", "?score"],
  "values": [
    ["?queryVec"],
    [{"@value": [0.9, 0.1, 0.05], "@type": "f:embeddingVector"}]
  ],
  "where": [
    {"@id": "?doc", "@type": "ex:Article", "ex:title": "?title", "ex:embedding": "?vec"},
    ["bind", "?score", "(cosineSimilarity ?vec ?queryVec)"],
    ["filter", "(> ?score 0.5)"]
  ],
  "orderBy": [["desc", "?score"]],
  "limit": 5
}
```

### Using a stored vector as the query vector

Instead of providing a literal vector, you can use a stored entity's vector:

```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "select": ["?similar", "?score"],
  "where": [
    {"@id": "ex:reference-doc", "ex:embedding": "?queryVec"},
    {"@id": "?similar", "ex:embedding": "?vec"},
    ["filter", "(!= ?similar ex:reference-doc)"],
    ["bind", "?score", "(cosineSimilarity ?vec ?queryVec)"]
  ],
  "orderBy": [["desc", "?score"]],
  "limit": 10
}
```

### Mixed datatypes

If a property contains both vector and non-vector values, the similarity functions return `null` for non-vector bindings:

```json
{
  "@graph": [
    {"@id": "ex:a", "ex:data": {"@value": [0.6, 0.5], "@type": "@vector"}},
    {"@id": "ex:b", "ex:data": "Not a vector"}
  ]
}
```

Querying with `dotProduct` on `?data` will return a numeric score for `ex:a` and `null` for `ex:b`.

### SPARQL support

Inline vector similarity functions (`dotProduct`, `cosineSimilarity`, `euclideanDistance`) are available in both JSON-LD Query and SPARQL. In SPARQL, use them as built-in function calls within `BIND` expressions:

#### dotProduct (SPARQL)

```sparql
PREFIX ex: <http://example.org/ns/>
PREFIX f: <https://ns.flur.ee/db#>

SELECT ?doc ?score
WHERE {
  VALUES ?queryVec { "[0.7, 0.6]"^^f:embeddingVector }
  ?doc ex:embedding ?vec ;
       ex:title ?title .
  BIND(dotProduct(?vec, ?queryVec) AS ?score)
}
ORDER BY DESC(?score)
LIMIT 10
```

#### cosineSimilarity (SPARQL)

```sparql
PREFIX ex: <http://example.org/ns/>
PREFIX f: <https://ns.flur.ee/db#>

SELECT ?doc ?score
WHERE {
  VALUES ?queryVec { "[0.88, 0.12, 0.08]"^^f:embeddingVector }
  ?doc a ex:Article ;
       ex:embedding ?vec ;
       ex:title ?title .
  BIND(cosineSimilarity(?vec, ?queryVec) AS ?score)
  FILTER(?score > 0.5)
}
ORDER BY DESC(?score)
LIMIT 5
```

#### euclideanDistance (SPARQL)

```sparql
PREFIX ex: <http://example.org/ns/>
PREFIX f: <https://ns.flur.ee/db#>

SELECT ?doc ?distance
WHERE {
  VALUES ?queryVec { "[0.7, 0.6]"^^f:embeddingVector }
  ?doc ex:embedding ?vec .
  BIND(euclideanDistance(?vec, ?queryVec) AS ?distance)
}
ORDER BY ?distance
LIMIT 10
```

#### Vector literals in SPARQL

In SPARQL, vectors are passed as JSON array strings with the `^^f:embeddingVector` typed literal syntax:

```sparql
VALUES ?queryVec { "[0.1, 0.2, 0.3]"^^f:embeddingVector }
```

Or with the full IRI:

```sparql
VALUES ?queryVec { "[0.1, 0.2, 0.3]"^^<https://ns.flur.ee/db#embeddingVector> }
```

#### Function name variants

Function names are case-insensitive in SPARQL. All of these are equivalent:

- `dotProduct`, `DOTPRODUCT`, `dot_product`
- `cosineSimilarity`, `COSINESIMILARITY`, `cosine_similarity`
- `euclideanDistance`, `EUCLIDEANDISTANCE`, `euclidean_distance`

## HNSW Vector Indexes

For large-scale similarity search, Fluree provides dedicated HNSW (Hierarchical Navigable Small World) vector indexes. These are approximate nearest-neighbor (ANN) indexes that trade exact results for dramatically faster query times on large datasets.

Vector indexes are implemented using embedded [usearch](https://github.com/unum-cloud/usearch) following the same architecture as BM25:

- Embedded in-process HNSW indexes (no external service required)
- Remote mode via dedicated search service (`fluree-search-httpd`)
- Snapshot-based persistence with watermarks
- Incremental sync for efficient updates
- Feature-gated via `vector` feature flag

**v1 limitation**: HNSW vector search is **head-only**. Time-travel queries (e.g. `@t:`) are not supported.

### Creating Vector Indexes

#### Rust API

```rust
use fluree_db_api::{FlureeBuilder, VectorCreateConfig};
use fluree_db_query::vector::DistanceMetric;

let fluree = FlureeBuilder::memory().build_memory();

// Create indexing query to select documents with embeddings
let indexing_query = json!({
    "@context": { "ex": "http://example.org/" },
    "where": [{ "@id": "?x", "@type": "ex:Document" }],
    "select": { "?x": ["@id", "ex:embedding"] }
});

// Create vector index
let config = VectorCreateConfig::new(
    "doc-embeddings",           // index name
    "mydb:main",                // source ledger
    indexing_query,             // what to index
    "ex:embedding",             // embedding property
    768                         // dimensions
)
.with_metric(DistanceMetric::Cosine);

let result = fluree.create_vector_index(config).await?;
println!("Indexed {} vectors", result.vector_count);
```

#### Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `name` | Index name (creates graph source ID `name:branch`) | Required |
| `ledger` | Source ledger ID (`name:branch`) | Required |
| `query` | JSON-LD query selecting documents | Required |
| `embedding_property` | Property containing embeddings | Required |
| `dimensions` | Vector dimensions | Required |
| `metric` | Distance metric (Cosine, Dot, Euclidean) | Cosine |
| `connectivity` | HNSW M parameter | 16 |
| `expansion_add` | efConstruction parameter | 128 |
| `expansion_search` | efSearch parameter | 64 |

### Query Syntax

Vector index search uses the `f:*` pattern syntax in WHERE clauses:

```json
{
  "@context": {
    "ex": "http://example.org/",
    "f": "https://ns.flur.ee/db#"
  },
  "from": "mydb:main",
  "where": [
    {
      "f:graphSource": "doc-embeddings:main",
      "f:queryVector": [0.1, 0.2, 0.3],
      "f:distanceMetric": "cosine",
      "f:searchLimit": 10,
      "f:searchResult": {
        "f:resultId": "?doc",
        "f:resultScore": "?score"
      }
    }
  ],
  "select": ["?doc", "?score"]
}
```

#### Query Parameters

| Parameter | Description | Required |
|-----------|-------------|----------|
| `f:graphSource` | Vector index alias | Yes |
| `f:queryVector` | Query vector (array or variable) | Yes |
| `f:distanceMetric` | Distance metric ("cosine", "dot", "euclidean") | No (uses index default) |
| `f:searchLimit` | Maximum results | No |
| `f:searchResult` | Result binding (variable or object) | Yes |
| `f:syncBeforeQuery` | Wait for index sync before query | No (default: false) |
| `f:timeoutMs` | Query timeout in ms | No |

#### Result Binding

Simple variable binding:
```json
"f:searchResult": "?doc"
```

Structured binding with score and ledger:
```json
"f:searchResult": {
  "f:resultId": "?doc",
  "f:resultScore": "?similarity",
  "f:resultLedger": "?source"
}
```

#### Variable Query Vectors

Query vector can be a variable bound earlier:
```json
{
  "where": [
    { "@id": "ex:reference-doc", "ex:embedding": "?queryVec" },
    {
      "f:graphSource": "embeddings:main",
      "f:queryVector": "?queryVec",
      "f:searchLimit": 5,
      "f:searchResult": "?similar"
    }
  ]
}
```

### Index Maintenance

#### Sync Updates

After committing new data, sync the vector index:

```rust
let sync_result = fluree.sync_vector_index("doc-embeddings:main").await?;
println!("Upserted: {}, Removed: {}", sync_result.upserted, sync_result.removed);
```

#### Full Resync

Rebuild the entire index from scratch:

```rust
let resync_result = fluree.resync_vector_index("doc-embeddings:main").await?;
```

#### Check Staleness

```rust
let check = fluree.check_vector_staleness("doc-embeddings:main").await?;
if check.is_stale {
    println!("Index is {} commits behind", check.commits_behind);
}
```

#### Drop Index

```rust
fluree.drop_vector_index("doc-embeddings:main").await?;
```

## Distance Metrics

### Cosine (Default)

Measures angle between vectors. Best for:
- Text embeddings (e.g., sentence transformers)
- Normalized vectors
- When magnitude doesn't matter

Score range: [-1, 1] (1 = identical, 0 = orthogonal, -1 = opposite)

For unit-normalized vectors, cosine similarity equals dot product. Fluree's SIMD kernels exploit this for faster computation when vectors are pre-normalized.

### Dot Product

Measures alignment and magnitude. Best for:
- Maximum inner product search (MIPS)
- When vector magnitude encodes importance

Score range: (-inf, +inf)

### Euclidean (L2)

Measures straight-line distance. Best for:
- Geometric similarity
- Image feature vectors
- When absolute position matters

Raw score range: [0, +inf). In HNSW index results, normalized to (0, 1] via `1 / (1 + distance)`.

**Note**: In HNSW index results (`f:*` queries), all metrics are normalized to "higher is better". In inline similarity functions, `euclideanDistance` returns the raw L2 distance (lower = more similar).

## Deployment Modes

Vector indexes support two deployment modes: **embedded** (default) and **remote**. This mirrors the BM25 deployment architecture.

### Embedded Mode (Default)

In embedded mode, the vector index is loaded and searched within the same process as Fluree:

```json
{
  "deployment": {
    "mode": "embedded"
  }
}
```

**Advantages:** No network latency, simpler deployment, no additional services.

### Remote Mode

In remote mode, vector search queries are delegated to a dedicated search service:

```json
{
  "deployment": {
    "mode": "remote",
    "endpoint": "http://search.example.com:9090/v1/search",
    "auth_token": "your-secret-token",
    "request_timeout_ms": 10000
  }
}
```

**Configuration options:**
- `mode`: `"remote"` to enable remote search
- `endpoint`: URL of the search service (required)
- `auth_token`: Bearer token for authentication (optional)
- `connect_timeout_ms`: Connection timeout in milliseconds (default: 5000)
- `request_timeout_ms`: Request timeout in milliseconds (default: 30000)

**Advantages:** Scales independently, dedicated memory for large indexes, shared across instances.

### Running the Search Service

The `fluree-search-httpd` binary provides a standalone HTTP server for remote search:

```bash
fluree-search-httpd \
  --storage-root file:///var/fluree/data \
  --nameservice-path file:///var/fluree/ns \
  --listen 0.0.0.0:9090
```

Both embedded and remote modes use identical distance metric computation, score normalization, and snapshot serialization -- ensuring identical results regardless of deployment mode.

## Performance and Scaling

### The importance of binary indexing

Fluree's binary columnar index dramatically accelerates vector queries. Queries against novelty-only (unindexed) data perform a linear scan through the in-memory commit log, while indexed queries read pre-sorted, cache-friendly columnar data. **Ensure background indexing is running** for production workloads -- the difference is substantial.

The following benchmarks use 768-dimensional vectors (typical for transformer embeddings like sentence-transformers or OpenAI `text-embedding-3-small`) on Apple M-series hardware:

#### Novelty-only (no binary index)

| Scenario | Vectors | Query time | Throughput |
|----------|---------|-----------|------------|
| Scan all | 1,000 | 9.9 ms | ~101K vec/s |
| Scan all | 5,000 | 45.1 ms | ~111K vec/s |
| Filtered + score | 1,000 (75 pass filter) | 13.5 ms | ~5.5K vec/s |
| Filtered + score | 5,000 (402 pass filter) | 62.1 ms | ~6.5K vec/s |

#### With binary index

| Scenario | Vectors | Query time | Throughput | Speedup vs novelty |
|----------|---------|-----------|------------|-------------------|
| Scan all | 1,000 | 1.68 ms | ~595K vec/s | 5.9x |
| Scan all | 5,000 | 7.69 ms | ~650K vec/s | 5.9x |
| Filtered + score | 1,000 (75 pass filter) | 533 us | ~141K vec/s | 25x |
| Filtered + score | 5,000 (402 pass filter) | 2.40 ms | ~168K vec/s | 26x |

Key takeaways:

- **Unfiltered scans** are ~6x faster with the binary index
- **Filtered queries** (where graph patterns reduce the candidate set before scoring) are ~25x faster -- the index enables efficient predicate-first access that avoids loading irrelevant vectors entirely
- At 5,000 vectors, a filtered indexed query completes in **2.4 ms** -- well within interactive latency budgets

### Inline similarity functions (flat scan)

- **Best for**: Small to medium datasets, ad-hoc similarity queries, prototyping
- **Complexity**: O(n) linear scan -- computes similarity against every matching vector
- **Advantage**: No index setup required, works immediately after insert
- **SIMD acceleration**: Fluree uses runtime-detected SIMD kernels (SSE2/AVX on x86_64, NEON on ARM) for vectorized dot/cosine/L2 computation
- **Normalized embedding optimization**: For unit-normalized vectors (most transformer embeddings), cosine similarity reduces to a dot product, avoiding magnitude computation entirely

### When to consider HNSW

Inline similarity functions perform a brute-force scan over all candidate vectors. This scales linearly and remains fast for moderate datasets, but at larger scales an HNSW index provides O(log n) approximate nearest-neighbor search.

**Rule of thumb:**

| Vector count (per property) | Recommendation |
|----------------------------|----------------|
| < 100K | Flat scan works well, especially with binary indexing. Sub-100ms queries typical. |
| 100K -- 1M | **Start evaluating HNSW.** Flat scan may still be acceptable depending on latency target and hardware, but HNSW will provide more consistent low-latency results. |
| 1M -- 10M | HNSW strongly recommended for interactive latency. Flat scan can work if vectors are memory-resident and you can tolerate ~1-2 second queries. |
| > 10M | HNSW (or other ANN index) is the default recommendation. Flat scan becomes I/O- and cache-bound for low-latency use cases. |

Factors that shift the crossover:

- **Hardware**: Fast NVMe / large RAM pushes the threshold higher; object storage (S3) pulls it lower
- **Latency target**: A 50 ms budget favors HNSW earlier than a 2-second budget
- **Filter selectivity**: If graph patterns reduce candidates to a small fraction before scoring, flat scan remains viable at higher counts
- **Normalized embeddings**: Cosine-as-dot-product is faster, pushing the threshold higher
- **Binary indexing**: An indexed dataset scans ~6x faster than novelty-only, effectively raising the flat-scan ceiling

### HNSW vector indexes

- **Best for**: Large datasets (100K+ vectors), production similarity search with strict latency requirements
- **Complexity**: O(log n) approximate nearest neighbor
- **Space**: ~1.5x embedding size + IRI mapping overhead
- **Updates**: Incremental via affected-subject tracking

#### Tuning parameters

| Parameter | Effect | Trade-off |
|-----------|--------|-----------|
| `connectivity` (M) | Graph connectivity | Higher = better recall, more memory |
| `expansion_add` (efConstruction) | Build-time search width | Higher = better index quality, slower build |
| `expansion_search` (efSearch) | Query-time search width | Higher = better recall, slower queries |

## Feature Flag

The HNSW vector index functionality requires the `vector` feature:

```toml
[dependencies]
fluree-db-api = { version = "0.1", features = ["vector"] }
```

Inline similarity functions (`dotProduct`, `cosineSimilarity`, `euclideanDistance`) and the `@vector` datatype are available without feature flags.

## Complete Example: Semantic Search

**1. Insert documents with embeddings:**

```json
{
  "@context": {
    "ex": "http://example.org/",
    "f": "https://ns.flur.ee/db#"
  },
  "@graph": [
    {
      "@id": "ex:doc1",
      "@type": "ex:Article",
      "ex:title": "Introduction to Machine Learning",
      "ex:embedding": {"@value": [0.9, 0.1, 0.05], "@type": "@vector"}
    },
    {
      "@id": "ex:doc2",
      "@type": "ex:Article",
      "ex:title": "Database Design Patterns",
      "ex:embedding": {"@value": [0.1, 0.8, 0.1], "@type": "@vector"}
    },
    {
      "@id": "ex:doc3",
      "@type": "ex:Article",
      "ex:title": "Neural Network Architectures",
      "ex:embedding": {"@value": [0.85, 0.15, 0.1], "@type": "@vector"}
    }
  ]
}
```

**2. Query -- find articles similar to a "machine learning" embedding:**

```json
{
  "@context": {
    "ex": "http://example.org/",
    "f": "https://ns.flur.ee/db#"
  },
  "select": ["?title", "?score"],
  "values": [
    ["?queryVec"],
    [{"@value": [0.88, 0.12, 0.08], "@type": "f:embeddingVector"}]
  ],
  "where": [
    {"@id": "?doc", "@type": "ex:Article", "ex:title": "?title", "ex:embedding": "?vec"},
    ["bind", "?score", "(cosineSimilarity ?vec ?queryVec)"]
  ],
  "orderBy": [["desc", "?score"]],
  "limit": 5
}
```

Expected results (ordered by similarity):
1. "Introduction to Machine Learning" -- highest cosine similarity
2. "Neural Network Architectures" -- similar domain
3. "Database Design Patterns" -- different domain, lower score

## Related Documentation

- [Datatypes and Typed Values](../concepts/datatypes.md) - All supported datatypes including `@vector`
- [JSON-LD Query](../query/jsonld-query.md) - Full query language reference
- [BM25](bm25.md) - Full-text search
- [Background Indexing](background-indexing.md) - Core indexing
- [Graph Sources](../graph-sources/README.md) - Graph source concepts
