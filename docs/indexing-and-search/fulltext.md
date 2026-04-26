# Inline Fulltext Search

Inline fulltext search enables BM25-ranked text scoring directly in queries, using the `@fulltext` datatype (or a ledger-level `f:fullTextDefaults` config) and the `fulltext()` scoring function. This follows the same pattern as `@vector` and inline similarity functions: declare what to index, persist as normal commits, and query with a scoring function in `bind` expressions. No external services, no separate ingestion pipeline.

Two ways to enable fulltext scoring on a property:

- **Per-value annotation** (`@fulltext` datatype) — zero-config, always English. Tag individual literal values at insert time. Good for a handful of obviously-fulltext fields where English is fine.
- **Property-level configuration** (`f:fullTextDefaults`) — declare once in the ledger's config graph which properties should be full-text indexed, and optionally which language to analyze them in. Plain-string values on those properties get indexed automatically — no `@type` annotation needed at insert time. Required when you want non-English stemming/stopwords, or when you want every value of a property indexed by default.

Both paths produce the same on-disk BM25 arenas and are queried with the same `fulltext(?var, "query")` function.

Use cases:

- **Document ranking**: Score and rank articles, product descriptions, or knowledge base entries by keyword relevance
- **Content discovery**: Find the most relevant documents for a natural language query
- **Faceted search**: Combine fulltext scoring with graph pattern filters (e.g., score only documents in a specific category)
- **Multilingual catalogs**: Index product descriptions in Spanish on one graph and English on another, with the right stemmer picked automatically per-language

## The `@fulltext` Datatype

### Why a dedicated datatype?

Plain strings in Fluree are stored as `xsd:string` values. They are indexed for exact matching and prefix queries, but not for full-text search. The `@fulltext` datatype tells Fluree that a string value should be analyzed (tokenized, stemmed, stopword-filtered) and indexed for relevance scoring.

`@fulltext` is a JSON-LD shorthand that resolves to the full IRI `https://ns.flur.ee/db#fullText`, which can also be written as `f:fullText` when the Fluree namespace prefix is declared in your `@context`.

### Inserting fulltext values (JSON-LD)

Use `"@type": "@fulltext"` to annotate a string as fulltext-searchable:

```json
{
  "@context": {
    "ex": "http://example.org/"
  },
  "@graph": [
    {
      "@id": "ex:article-1",
      "@type": "ex:Article",
      "ex:title": "Rust Programming",
      "ex:content": {
        "@value": "Rust is a systems programming language focused on safety and performance",
        "@type": "@fulltext"
      }
    }
  ]
}
```

You can also use the full IRI or `f:` prefix form:

```json
{
  "@context": {
    "ex": "http://example.org/",
    "f": "https://ns.flur.ee/db#"
  },
  "@graph": [
    {
      "@id": "ex:article-1",
      "ex:content": {
        "@value": "Rust is a systems programming language...",
        "@type": "f:fullText"
      }
    }
  ]
}
```

### Inserting fulltext values (Turtle / SPARQL UPDATE)

In Turtle and SPARQL UPDATE, the `@fulltext` shorthand is not available. Use the `f:fullText` datatype IRI with the standard `^^` typed-literal syntax.

**Turtle data file:**

```turtle
@prefix ex: <http://example.org/> .
@prefix f: <https://ns.flur.ee/db#> .

ex:article-1
  a ex:Article ;
  ex:title "Introduction to Rust" ;
  ex:content "Rust is a systems programming language focused on safety and performance"^^f:fullText .

ex:article-2
  a ex:Article ;
  ex:title "Database Design Patterns" ;
  ex:content "Modern database systems use columnar storage and immutable ledgers"^^f:fullText .
```

**SPARQL UPDATE:**

```sparql
PREFIX ex: <http://example.org/>
PREFIX f: <https://ns.flur.ee/db#>

INSERT DATA {
  ex:article-1 a ex:Article ;
    ex:title "Introduction to Rust" ;
    ex:content "Rust is a systems programming language focused on safety"^^f:fullText .
}
```

The `^^f:fullText` annotation is the Turtle/SPARQL equivalent of `"@type": "@fulltext"` in JSON-LD. Without it, the string is stored as a plain `xsd:string`.

### Multiple fulltext properties per entity

An entity can have `@fulltext` on multiple different properties:

```json
{
  "@id": "ex:article-1",
  "ex:title": {
    "@value": "Rust Programming Guide",
    "@type": "@fulltext"
  },
  "ex:content": {
    "@value": "Rust is a systems programming language focused on safety...",
    "@type": "@fulltext"
  }
}
```

Each property produces an independent fulltext index (arena). When you query with `fulltext()`, the function automatically uses the arena for the property bound to the variable.

### Portability

`@fulltext` annotations are fully portable across Fluree's data distribution pipeline. Import, export, push, and pull all preserve `@fulltext` type annotations, and indexes are rebuilt transparently on the receiving side.

## Configured Full-Text Properties (`f:fullTextDefaults`)

The `@fulltext` datatype is a per-value shortcut — you decide at insert time, one triple at a time, whether a string gets full-text indexed, and English is the only supported language. For many real-world workloads that's not what you want. You want to say once, at the ledger level, "index every value of `ex:title`", or "index `ex:productName` in the product catalog graph in Spanish." That's what `f:fullTextDefaults` gives you.

When a property is declared in `f:fullTextDefaults`, any plain `xsd:string` or `rdf:langString` value on that property gets full-text indexed — no `@type: @fulltext` needed on individual values. Language-tagged (`rdf:langString`) values automatically route to a per-language arena (French stemmer for `"fr"`, Spanish stopwords for `"es"`, and so on). Untagged plain strings fall back to the configured default language.

The `@fulltext` datatype continues to work exactly as before: any value tagged `@fulltext` is always indexed as English, regardless of what `f:fullTextDefaults` says about its property. You can mix both paths on the same property; English content from either path lands in a single shared arena.

### When to use which

| Need | Use |
|------|-----|
| English-only, a few obviously-fulltext fields, want the choice per-value | `@fulltext` datatype |
| Non-English (or mixed languages) | `f:fullTextDefaults` with `f:defaultLanguage` |
| Every value of a property should be searchable, no per-value opt-in | `f:fullTextDefaults` |
| Different languages per graph (e.g. multilingual catalog) | `f:fullTextDefaults` with per-graph overrides |
| Zero config, just works | `@fulltext` datatype |

### Setting it up

Write configuration into the ledger's `#config` named graph, alongside any other config groups (policy, SHACL, reasoning, etc.). The config is itself a transaction — it's versioned and auditable like any other data.

**Minimal — index `ex:title` and `ex:body`, English by default:**

```trig
@prefix f: <https://ns.flur.ee/db#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix ex: <http://example.org/> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
    f:fullTextDefaults [
      a f:FullTextDefaults ;
      f:defaultLanguage "en" ;
      f:property [ a f:FullTextProperty ; f:target ex:title ] ,
                 [ a f:FullTextProperty ; f:target ex:body ]
    ] .
}
```

Or as JSON-LD:

```json
{
  "@context": {
    "f": "https://ns.flur.ee/db#",
    "ex": "http://example.org/"
  },
  "@graph": [
    {
      "@id": "urn:fluree:mydb:main:config:ledger",
      "@type": "f:LedgerConfig",
      "@graph": "urn:fluree:mydb:main#config",
      "f:fullTextDefaults": {
        "@type": "f:FullTextDefaults",
        "f:defaultLanguage": "en",
        "f:property": [
          { "@type": "f:FullTextProperty", "f:target": { "@id": "ex:title" } },
          { "@type": "f:FullTextProperty", "f:target": { "@id": "ex:body" } }
        ]
      }
    }
  ]
}
```

After writing config, trigger a reindex so existing values on `ex:title` and `ex:body` get indexed. See [Reindexing after a config change](#reindexing-after-a-config-change) below.

**Data writes don't change.** Once config is in place and the reindex has run, just insert plain strings the way you always would:

```json
{
  "@id": "ex:doc1",
  "ex:title": "Rust programming language guide",
  "ex:body": "Rust is a systems programming language..."
}
```

Both values flow into BM25 arenas automatically.

### Multiple languages

Fluree ships Snowball stemmers and curated stopwords for 18 languages. Pick one as your ledger default via `f:defaultLanguage`; any BCP-47 tag in the list below works.

| Tag | Language |
|-----|----------|
| `ar` | Arabic |
| `da` | Danish |
| `de` | German |
| `el` | Greek |
| `en` | English |
| `es` | Spanish |
| `fi` | Finnish |
| `fr` | French |
| `hu` | Hungarian |
| `it` | Italian |
| `nl` | Dutch |
| `no` (or `nb`, `nn`) | Norwegian |
| `pt` | Portuguese |
| `ro` | Romanian |
| `ru` | Russian |
| `sv` | Swedish |
| `ta` | Tamil |
| `tr` | Turkish |

A BCP-47 tag that isn't on this list still works — it just skips stemming and stopword removal (tokenize + lowercase only). Index and query sides agree on that behavior so scores remain consistent.

**Per-value language tagging via `rdf:langString`.** If a single property holds values in different languages, tag them with `@language` (JSON-LD) or `@lang` (Turtle):

```json
{
  "@id": "ex:doc1",
  "ex:title": [
    { "@value": "Rust programming", "@language": "en" },
    { "@value": "Programmation Rust", "@language": "fr" }
  ]
}
```

Fluree automatically builds per-language arenas (`ex:title` in English, `ex:title` in French) and queries against the arena whose language matches the row's tag. Untagged values fall back to the ledger's `f:defaultLanguage`.

### Per-graph overrides

Different graphs can have different full-text configuration. For example, a product catalog graph might index `ex:productName` in Spanish while the rest of the ledger uses English:

```trig
@prefix f: <https://ns.flur.ee/db#> .
@prefix ex: <http://example.org/> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
    # Ledger-wide: English, index ex:title everywhere.
    f:fullTextDefaults [
      a f:FullTextDefaults ;
      f:defaultLanguage "en" ;
      f:property [ a f:FullTextProperty ; f:target ex:title ]
    ] ;
    # Catalog graph: also index ex:productName, default Spanish.
    f:graphOverrides [
      a f:GraphConfig ;
      f:targetGraph <urn:example:productCatalog> ;
      f:fullTextDefaults [
        a f:FullTextDefaults ;
        f:defaultLanguage "es" ;
        f:property [ a f:FullTextProperty ; f:target ex:productName ]
      ]
    ] .
}
```

The merge is **additive**: every property in the ledger-wide list applies to every graph (including `productCatalog`), and the per-graph override *adds* `ex:productName` on top of `ex:title`. The override's `f:defaultLanguage` shadows the ledger-wide language only for untagged plain strings on that specific graph.

**Targeting the default graph or txn-meta explicitly.** Use the `f:defaultGraph` sentinel to target only the default graph (`g_id = 0`), or `f:txnMetaGraph` for the ledger's txn-meta graph:

```trig
f:graphOverrides [
  a f:GraphConfig ;
  f:targetGraph f:defaultGraph ;
  f:fullTextDefaults [
    a f:FullTextDefaults ;
    f:property [ a f:FullTextProperty ; f:target ex:note ]
  ]
]
```

### Locking config (`f:overrideControl`)

If you want to prevent per-graph overrides from modifying the ledger-wide full-text defaults, set `f:overrideControl` to `f:OverrideNone` on the ledger-wide group:

```trig
<urn:fluree:mydb:main:config:ledger> f:fullTextDefaults [
  a f:FullTextDefaults ;
  f:defaultLanguage "en" ;
  f:overrideControl f:OverrideNone ;
  f:property [ a f:FullTextProperty ; f:target ex:title ]
] .
```

With `f:OverrideNone`, any `f:graphOverrides` entry targeting `f:fullTextDefaults` is ignored at resolution time — the ledger-wide group is final. See [Override control](../ledger-config/override-control.md) for the full model.

### Reindexing after a config change

Writing or editing `f:fullTextDefaults` does **not** automatically rebuild any arenas. You control when reindexing happens.

**What you need to know:**

1. **New commits after the config change** pick up the new config automatically during the next incremental index build — newly inserted values on configured properties flow into arenas as expected.
2. **Existing values** that were committed *before* the config change are not retroactively indexed until you run a full reindex.
3. **Removing or renaming a property** from `f:fullTextDefaults` drops it from the configured set for new commits, but the existing arena stays until you reindex.
4. **Changing `f:defaultLanguage`** doesn't rewrite existing arenas — they keep whatever language they were built with. New values get the new language; scores may be temporarily inconsistent across the old/new boundary until a reindex.

To force the full picture — pick up config changes for *all* existing data — run a manual reindex:

```bash
# CLI
fluree reindex mydb:main

# Or via the admin API
curl -X POST https://<fluree-server>/v1/fluree/reindex \
  -H 'Content-Type: application/json' \
  -d '{"ledger": "mydb:main"}'
```

The reindex reads the current `f:fullTextDefaults`, walks the entire commit chain, and rebuilds arenas with the new configuration applied consistently.

> **Note on concurrent reindex + config write.** A reindex already in progress operates on a point-in-time snapshot and will NOT pick up a config change committed during its run. If you change config during a reindex, wait for it to finish, then trigger another reindex. See [Reindex](reindex.md) for full semantics.

### How config-path and `@fulltext`-datatype coexist

If a value's datatype is `@fulltext`, the datatype wins: that value is indexed as English, even if the property is listed in `f:fullTextDefaults` with a different `f:defaultLanguage`. This keeps the `@fulltext` contract stable ("I tagged this value English, index it now") and guarantees no double-indexing.

In practice, a single property can mix:

- `@fulltext`-datatype values → English arena
- `rdf:langString` values tagged `"fr"` → French arena
- Plain `xsd:string` values → arena for the configured `f:defaultLanguage`

Each language becomes its own arena; queries automatically look up the right one based on the row's language tag (with English as the fallback). Ledger-wide English content from both paths shares a single arena — no wasted duplication.

## The `fulltext()` Scoring Function

The `fulltext()` function computes a BM25 relevance score for a bound text value against a query string. Use it in `bind` expressions within JSON-LD queries.

### Basic usage

```json
{
  "@context": {
    "ex": "http://example.org/"
  },
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

**Arguments:**
- First argument: a variable bound to a `@fulltext`-typed value
- Second argument: the search query string (natural language)

**Returns:** A numeric score (`xsd:double`). Higher scores indicate greater relevance. Returns `0.0` when the document contains none of the query terms.

### Alternative array syntax

The function also accepts array form:

```json
["bind", "?score", ["fulltext", "?content", "Rust programming"]]
```

This is equivalent to the S-expression string form.

### Filtering by score

Combine `bind` with `filter` to exclude non-matching documents:

```json
["bind", "?score", "(fulltext ?content \"search terms\")"],
["filter", "(> ?score 0)"]
```

### Combining with graph patterns

Fulltext scoring works naturally with standard graph patterns. Filter by type, category, or relationships before or after scoring:

```json
{
  "@context": {
    "ex": "http://example.org/"
  },
  "select": ["?title", "?score"],
  "where": [
    {
      "@id": "?doc",
      "@type": "ex:Article",
      "ex:content": "?content",
      "ex:title": "?title",
      "ex:category": "?cat"
    },
    ["filter", "(= ?cat \"technology\")"],
    ["bind", "?score", "(fulltext ?content \"distributed database systems\")"],
    ["filter", "(> ?score 0)"]
  ],
  "orderBy": [["desc", "?score"]],
  "limit": 10
}
```

Placing the category filter before the `fulltext()` bind reduces the number of documents scored, improving query performance.

## How Scoring Works

The `fulltext()` function uses **BM25** (Best Match 25), the standard information retrieval scoring algorithm used by search engines.

### BM25 formula

For each query term *t* in document *d*:

```
IDF(t)     = ln((N - df(t) + 0.5) / (df(t) + 0.5) + 1)
TF_norm(t) = tf(t,d) * (k1 + 1) / (tf(t,d) + k1 * (1 - b + b * |d| / avgdl))
score(q,d) = SUM( IDF(t) * TF_norm(t) )  for each query term t
```

### What makes the scoring effective

- **IDF (Inverse Document Frequency)** -- Downweights common terms ("the", "is") and boosts rare, discriminative terms. A query for "distributed database" gives more weight to "distributed" (rarer) than "database" (common in a tech corpus).

- **Document length normalization** -- Prevents long documents from dominating purely due to having more words. Controlled by parameter *b* (default 0.75). A 50-word abstract mentioning "database" twice scores comparably to a 500-word article mentioning it twice.

- **Term frequency saturation** -- Diminishing returns for repeated terms, controlled by parameter *k1* (default 1.2). The 5th occurrence of "database" in a document contributes less than the 1st.

- **Corpus-wide average document length** (`avgdl`) -- Anchors the length normalization across the entire collection.

### Text analysis pipeline

Both documents and queries go through the same analysis pipeline, and the index and query sides always use the same analyzer for a given arena — so query stems match document stems:

1. **Tokenization** -- Split text on whitespace and punctuation (Unicode-aware)
2. **Lowercasing** -- Normalize to lowercase
3. **Stopword removal** -- Remove common stopwords for the bucket's language ("the", "is", "and" in English; "le", "la", "et" in French; etc.)
4. **Stemming** -- Reduce words to stems using the Snowball stemmer for the bucket's language

This means a query for "programming" against an English arena matches documents containing "programmed", "programs", or "programmer". A French-language arena stems French word forms instead ("chantait" → "chant", matching "chanter", "chantons", and so on).

For the `@fulltext` datatype, the analyzer is always English. For properties declared in `f:fullTextDefaults`, the analyzer matches the arena's language (row's `rdf:langString` tag, or the configured `f:defaultLanguage`). An unrecognized BCP-47 tag skips steps 3 and 4 — tokenize + lowercase only — consistently on both sides.

## Indexing

### Automatic arena construction

During background binary index builds, Fluree automatically constructs a **FulltextArena** (FTA1 format) for each `(graph, predicate)` combination that has `@fulltext` values. Each arena stores:

- A sorted **term dictionary** of stemmed tokens
- Per-document **bag-of-words** (BoW) entries: `(term_id, tf)` pairs sorted by term ID
- **Corpus-level statistics**: document count (*N*), sum of document lengths (*sum_dl*), and per-term document frequency (*df*)

This precomputed representation enables fast scoring at query time -- the indexed path avoids per-row text analysis entirely, reading precomputed BoW entries via binary search.

### No-index fallback

If no binary index has been built yet (e.g., immediately after ledger creation), `fulltext()` still works using an on-the-fly analysis fallback. Documents are tokenized and scored using TF-saturation (a simplified scoring model). This is slower but ensures the feature works before background indexing catches up.

### Novelty overlay

Documents committed after the last index build (in the "novelty" layer) are automatically included in query results with consistent BM25 scores. Fluree computes effective corpus statistics by merging the persisted arena stats with a novelty delta:

- `N' = N_arena + delta_N_novelty`
- `avgdl' = (sum_dl_arena + delta_sum_dl_novelty) / N'`
- `df'(t) = df_arena(t) + delta_df_novelty(t)`

This ensures that indexed documents and novelty documents produce comparable, consistent scores in the same query.

### Retraction handling

When a `@fulltext` value is retracted, it is removed from the arena at the next index build. The retracted document no longer appears in fulltext query results and its statistics are excluded from corpus-level calculations.

## Performance

### Query-time benchmarks

All benchmarks measure the full end-to-end query path: JSON-LD parse, query plan, scan, BM25 score, sort, and limit 10. Documents are paragraph-length (~30-60 words), representative of article abstracts, product descriptions, or knowledge base entries.

| Documents | Novelty (no index) | Indexed (arena BM25) | Speedup |
|----------:|:------------------:|:--------------------:|:-------:|
| 1,000 | 11.6 ms | 1.7 ms | 6.7x |
| 5,000 | 57.0 ms | 7.9 ms | 7.2x |
| 10,000 | 115.8 ms | 15.5 ms | 7.5x |
| 50,000 | 601.9 ms | 80.2 ms | 7.5x |

**Indexed throughput: ~625,000 docs/sec** -- 50K documents scored and ranked in 80ms.

**Novelty throughput: ~85,000 docs/sec** -- 50K documents in ~600ms (no index required).

The indexed path is 7-7.5x faster because it reads precomputed BoW entries via binary search on sorted `(term_id, tf)` arrays, avoiding per-row text analysis and HashMap allocation.

Scaling is near-linear. Extrapolating, the indexed path handles approximately 625K documents within a 1-second query budget.

### When to consider the BM25 graph source pipeline

Inline `@fulltext` works well for **tens to hundreds of thousands of documents** per predicate. For larger corpora (1M+ documents), consider the dedicated [BM25 graph source pipeline](bm25.md), which provides:

- **WAND (Weak AND) top-k pruning** -- Skips documents that provably cannot enter the top-k results, critical for large corpora where scanning every document is prohibitive
- **Chunked posting list storage** -- Compressed, seekable posting lists with skip pointers for efficient I/O at scale
- **Incremental index updates** -- Updates posting lists in place without rebuilding the full index
- **Cross-property dependency tracking** -- BM25 scores can depend on fields from other properties
- **Configurable analyzers per property** -- Language-specific tokenizers, stemmers, and stopword lists
- **Multi-term query optimization** -- Term-at-a-time vs document-at-a-time evaluation strategies

| Corpus size | Recommendation |
|-------------|----------------|
| < 100K docs | Inline `@fulltext` works well, especially with binary indexing |
| 100K - 500K | Inline `@fulltext` remains viable; query times scale linearly |
| 500K - 1M | Evaluate based on latency requirements; WAND pruning may help |
| 1M+ | Use the [BM25 graph source](bm25.md) for production workloads |

## Comparison with `@vector`

Both `@fulltext` and `@vector` follow the same architectural pattern: annotate, commit, index, query.

| | `@vector` | `@fulltext` |
|---|---|---|
| **Annotation** | `"@type": "@vector"` | `"@type": "@fulltext"` |
| **Index artifact** | VAS1 arena (raw vectors) | FTA1 arena (BoW + corpus stats) |
| **Scoring function** | `dotProduct`, `cosineSimilarity`, `euclideanDistance` | `fulltext(?var, "query")` |
| **Query input** | Vector literal | Natural language string |
| **Per-row cost** | O(dims) float math | O(query_terms) integer lookups |
| **Portability** | Push/pull/import/export preserves `@vector` | Push/pull/import/export preserves `@fulltext` |

## Complete Example

**1. Insert documents with fulltext content:**

```json
{
  "@context": {
    "ex": "http://example.org/"
  },
  "@graph": [
    {
      "@id": "ex:article-1",
      "@type": "ex:Article",
      "ex:title": "Introduction to Rust",
      "ex:content": {
        "@value": "Rust is a systems programming language focused on safety, speed, and concurrency. It prevents segfaults and guarantees thread safety.",
        "@type": "@fulltext"
      }
    },
    {
      "@id": "ex:article-2",
      "@type": "ex:Article",
      "ex:title": "Database Design Patterns",
      "ex:content": {
        "@value": "Modern database systems use columnar storage and immutable ledgers. Graph databases model relationships as first-class citizens.",
        "@type": "@fulltext"
      }
    },
    {
      "@id": "ex:article-3",
      "@type": "ex:Article",
      "ex:title": "Rust for Systems Programming",
      "ex:content": {
        "@value": "Building high-performance systems in Rust requires understanding ownership, borrowing, and lifetime semantics. Rust's type system catches bugs at compile time.",
        "@type": "@fulltext"
      }
    }
  ]
}
```

**2. Query -- find articles about "Rust systems programming", ranked by relevance:**

```json
{
  "@context": {
    "ex": "http://example.org/"
  },
  "select": ["?title", "?score"],
  "where": [
    {
      "@id": "?doc",
      "@type": "ex:Article",
      "ex:content": "?content",
      "ex:title": "?title"
    },
    ["bind", "?score", "(fulltext ?content \"Rust systems programming\")"],
    ["filter", "(> ?score 0)"]
  ],
  "orderBy": [["desc", "?score"]],
  "limit": 10
}
```

Expected results (ordered by relevance):
1. "Rust for Systems Programming" -- highest score (most query terms, multiple occurrences)
2. "Introduction to Rust" -- mentions Rust and systems programming
3. "Database Design Patterns" -- excluded by `> 0` filter (no matching terms)

## SPARQL Support

### Inserting data

Fulltext annotation works in SPARQL UPDATE today using the `^^f:fullText` typed literal syntax (see the Turtle/SPARQL insertion examples above).

### Querying

The `fulltext()` scoring function is currently available in **JSON-LD Query only**. SPARQL query support is planned for a future release, with anticipated syntax like:

```sparql
PREFIX ex: <http://example.org/>
PREFIX f: <https://ns.flur.ee/db#>

SELECT ?title ?score
WHERE {
  ?doc a ex:Article ;
       ex:content ?content ;
       ex:title ?title .
  BIND(f:fulltext(?content, "Rust programming") AS ?score)
  FILTER(?score > 0)
}
ORDER BY DESC(?score)
LIMIT 10
```

This mirrors the pattern established by inline vector similarity functions (`dotProduct`, `cosineSimilarity`, `euclideanDistance`), which also support JSON-LD Query today with SPARQL planned.

## Related Documentation

- [Datatypes and Typed Values](../concepts/datatypes.md) -- All supported datatypes including `@fulltext`
- [Setting Groups](../ledger-config/setting-groups.md#full-text-defaults) -- Full reference for the `f:fullTextDefaults` schema (fields, additive merge, override control)
- [Override control](../ledger-config/override-control.md) -- Locking ledger-wide config against per-graph overrides
- [Reindex](reindex.md) -- When and how to reindex (required to pick up config changes for existing data)
- [JSON-LD Query](../query/jsonld-query.md) -- Full query language reference
- [BM25 Graph Source](bm25.md) -- Dedicated BM25 full-text search for large-scale corpora
- [Vector Search](vector-search.md) -- Inline similarity search with `@vector`
- [Background Indexing](background-indexing.md) -- How background indexing works
