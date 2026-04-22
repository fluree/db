# Cookbook: Full-Text and Vector Search

Fluree integrates BM25 full-text search and vector similarity directly into the query engine. Search results participate in joins, filters, and aggregations like any other graph pattern — no external search service needed.

This guide covers practical patterns for both approaches.

## Quick start: full-text search

### 1. Insert searchable data

Annotate string values with `@fulltext` to make them searchable:

```bash
fluree insert '{
  "@context": {"ex": "http://example.org/"},
  "@graph": [
    {
      "@id": "ex:doc1",
      "@type": "ex:Article",
      "ex:title": "Introduction to Graph Databases",
      "ex:body": {
        "@value": "Graph databases model data as nodes and edges, making relationship queries fast and intuitive. Unlike relational databases, graph databases traverse relationships without expensive joins.",
        "@type": "@fulltext"
      }
    },
    {
      "@id": "ex:doc2",
      "@type": "ex:Article",
      "ex:title": "Time Series vs Graph: When to Use Which",
      "ex:body": {
        "@value": "Time series databases excel at ordered, append-only data. Graph databases shine when relationships between entities matter more than temporal ordering.",
        "@type": "@fulltext"
      }
    },
    {
      "@id": "ex:doc3",
      "@type": "ex:Article",
      "ex:title": "Building REST APIs with Rust",
      "ex:body": {
        "@value": "Rust provides memory safety without garbage collection, making it ideal for high-performance API servers. Popular frameworks include Actix and Axum.",
        "@type": "@fulltext"
      }
    }
  ]
}'
```

In Turtle, use `^^f:fullText`:

```bash
fluree insert '
@prefix ex: <http://example.org/> .
@prefix f:  <https://ns.flur.ee/db#> .

ex:doc4 a ex:Article ;
  ex:title "SPARQL Query Optimization" ;
  ex:body  "Optimizing SPARQL queries requires understanding triple patterns, join ordering, and index selection. The query planner reorders patterns based on estimated cardinality."^^f:fullText .
'
```

### 2. Search with relevance scoring

```bash
fluree query '{
  "@context": {"ex": "http://example.org/"},
  "select": ["?title", "?score"],
  "where": [
    {"@id": "?doc", "@type": "ex:Article", "ex:body": "?body", "ex:title": "?title"},
    ["bind", "?score", "(fulltext ?body \"graph database relationships\")"],
    ["filter", "(> ?score 0)"]
  ],
  "orderBy": [["desc", "?score"]],
  "limit": 10
}'
```

The `fulltext()` function returns a BM25 relevance score. Higher scores mean better matches. Documents with none of the search terms score 0.

### 3. Combine search with graph filters

Search only within a specific category:

```bash
fluree query '{
  "@context": {"ex": "http://example.org/"},
  "select": ["?title", "?score"],
  "where": [
    {
      "@id": "?doc", "@type": "ex:Article",
      "ex:body": "?body", "ex:title": "?title",
      "ex:category": "databases"
    },
    ["bind", "?score", "(fulltext ?body \"query optimization\")"],
    ["filter", "(> ?score 0)"]
  ],
  "orderBy": [["desc", "?score"]]
}'
```

Place graph filters **before** the `fulltext()` bind to reduce the number of documents scored.

## Patterns

### Search across multiple properties

If both `title` and `body` are `@fulltext`, score them separately and combine:

```bash
fluree query '{
  "@context": {"ex": "http://example.org/"},
  "select": ["?title", "?combined"],
  "where": [
    {"@id": "?doc", "ex:ftTitle": "?ft", "ex:body": "?body", "ex:title": "?title"},
    ["bind", "?titleScore", "(fulltext ?ft \"graph databases\")"],
    ["bind", "?bodyScore", "(fulltext ?body \"graph databases\")"],
    ["bind", "?combined", "(+ (* ?titleScore 2.0) ?bodyScore)"],
    ["filter", "(> ?combined 0)"]
  ],
  "orderBy": [["desc", "?combined"]]
}'
```

This weights title matches 2x higher than body matches.

### Search with time travel

Search the knowledge base as it existed at a previous point in time:

```bash
fluree query '{
  "@context": {"ex": "http://example.org/"},
  "from": "mydb:main@t:5",
  "select": ["?title", "?score"],
  "where": [
    {"@id": "?doc", "ex:body": "?body", "ex:title": "?title"},
    ["bind", "?score", "(fulltext ?body \"deployment\")"],
    ["filter", "(> ?score 0)"]
  ],
  "orderBy": [["desc", "?score"]]
}'
```

### Search with aggregation

Count matches by category:

```bash
fluree query '{
  "@context": {"ex": "http://example.org/"},
  "select": ["?category", "?count"],
  "where": [
    {"@id": "?doc", "ex:body": "?body", "ex:category": "?category"},
    ["bind", "?score", "(fulltext ?body \"database\")"],
    ["filter", "(> ?score 0)"]
  ],
  "groupBy": "?category",
  "aggregate": {"?count": ["count", "?doc"]}
}'
```

## Quick start: vector search

### 1. Insert vector embeddings

Annotate arrays with `@vector`:

```bash
fluree insert '{
  "@context": {"ex": "http://example.org/"},
  "@graph": [
    {
      "@id": "ex:product1",
      "@type": "ex:Product",
      "ex:name": "Wireless Headphones",
      "ex:embedding": {"@value": [0.82, 0.15, 0.91, 0.23], "@type": "@vector"}
    },
    {
      "@id": "ex:product2",
      "@type": "ex:Product",
      "ex:name": "Bluetooth Speaker",
      "ex:embedding": {"@value": [0.78, 0.12, 0.88, 0.31], "@type": "@vector"}
    },
    {
      "@id": "ex:product3",
      "@type": "ex:Product",
      "ex:name": "Running Shoes",
      "ex:embedding": {"@value": [0.11, 0.95, 0.05, 0.87], "@type": "@vector"}
    }
  ]
}'
```

Vectors are stored as f32. Values are quantized at ingest time.

### 2. Find similar items

Use `cosineSimilarity` (or `dotProduct`, `euclideanDistance`) to rank by similarity:

```bash
fluree query '{
  "@context": {"ex": "http://example.org/"},
  "select": ["?name", "?sim"],
  "where": [
    {"@id": "?product", "@type": "ex:Product", "ex:name": "?name", "ex:embedding": "?vec"},
    ["bind", "?sim", "(cosineSimilarity ?vec [0.80, 0.14, 0.90, 0.25])"],
    ["filter", "(> ?sim 0.9)"]
  ],
  "orderBy": [["desc", "?sim"]],
  "limit": 5
}'
```

### 3. Combine vector search with graph patterns

Find products similar to a query vector, but only in a specific category:

```bash
fluree query '{
  "@context": {"ex": "http://example.org/"},
  "select": ["?name", "?sim"],
  "where": [
    {
      "@id": "?product", "@type": "ex:Product",
      "ex:name": "?name", "ex:embedding": "?vec",
      "ex:category": "electronics"
    },
    ["bind", "?sim", "(cosineSimilarity ?vec [0.80, 0.14, 0.90, 0.25])"]
  ],
  "orderBy": [["desc", "?sim"]],
  "limit": 10
}'
```

## Hybrid search: text + vector

Combine BM25 keyword relevance with vector semantic similarity for the best of both:

```bash
fluree query '{
  "@context": {"ex": "http://example.org/"},
  "select": ["?name", "?hybrid"],
  "where": [
    {
      "@id": "?doc", "ex:name": "?name",
      "ex:description": "?desc", "ex:embedding": "?vec"
    },
    ["bind", "?textScore", "(fulltext ?desc \"wireless audio\")"],
    ["bind", "?vecScore", "(cosineSimilarity ?vec [0.80, 0.14, 0.90, 0.25])"],
    ["bind", "?hybrid", "(+ (* ?textScore 0.4) (* ?vecScore 0.6))"],
    ["filter", "(> ?hybrid 0)"]
  ],
  "orderBy": [["desc", "?hybrid"]],
  "limit": 10
}'
```

Adjust the weights (0.4 text, 0.6 vector) based on your use case. Keyword search is better for exact term matching; vector search is better for semantic similarity.

## When to use which

| Approach | Best for | Scale |
|---|---|---|
| Inline `@fulltext` | Keyword search, document ranking | Up to ~500K documents per property |
| [BM25 graph source](../indexing-and-search/bm25.md) | Large-scale text search with WAND pruning | 1M+ documents |
| Inline `@vector` + similarity | Small-to-medium similarity search | Up to ~100K vectors |
| [HNSW index](../indexing-and-search/vector-search.md) | Large-scale approximate nearest neighbor | 100K+ vectors |

## Performance tips

1. **Place graph filters before search** — Reduce the candidate set before scoring
2. **Use `limit`** — BM25 and similarity scoring are per-document operations
3. **Wait for indexing** — Inline `@fulltext` works without an index (novelty fallback) but is 7x faster with a built index
4. **Choose the right scale** — Inline functions work well up to hundreds of thousands of documents. For millions, use the dedicated graph source pipeline

## Related documentation

- [Inline Fulltext Search](../indexing-and-search/fulltext.md) — `@fulltext` datatype reference
- [BM25 Graph Source](../indexing-and-search/bm25.md) — Large-scale text search pipeline
- [Vector Search](../indexing-and-search/vector-search.md) — `@vector` datatype and HNSW indexes
- [JSON-LD Query](../query/jsonld-query.md) — Full query language reference
