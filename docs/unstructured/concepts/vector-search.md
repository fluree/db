# Vector search

`fluree doc search --mode vector` embeds your query with the configured
embedding slot and ranks chunks by cosine similarity against their
`doc:embedding` values.

It does that with an exact scan — every chunk is scored — not an approximate
index. That is a deliberate choice, and it has a boundary worth knowing.

## Why an exact scan

An embedding is ordinary ledger data: a `@vector` literal on the chunk,
committed with everything else, time-travelled with everything else. Scoring it
needs no index and no extra library, just the engine's `cosineSimilarity`
function:

```json
{
  "where": [
    { "@id": "?c", "doc:embedding": "?v" },
    ["bind", "?score", ["cosineSimilarity", "?v", "?q"]]
  ],
  "select": ["?score", "?c"],
  "orderBy": [["desc", "?score"]],
  "limit": 10
}
```

That is exactly what `doc search` runs. The consequences are all good ones at
the scale the CLI is built for:

- **Exact.** An approximate index trades recall for speed; a scan has no recall
  loss, so the top result is the top result.
- **Nothing to build, sync or rebuild.** New documents are searchable the moment
  they commit. Changing embedding models re-embeds and you are done — there is
  no index built for the old width to drop.
- **Time travel works.** `--at` on a vector search is an ordinary query against
  an ordinary property.
- **No ANN library in the CLI.** The `fluree` binary links no C++ vector index,
  so it installs from source with no extra toolchain.

## Where the boundary is

Cost is linear in the number of chunks. For a folder, a team's handbook, a
contract set — thousands to tens of thousands of chunks — a scan is the right
answer and the index would be overhead. Past that, linear stops being free, and
the answer is one of:

- **`--mode text`.** BM25 is indexed and stays fast at any size. For queries
  that name their terms it is often the better retrieval anyway.
- **An HNSW index on a server.** Approximate nearest-neighbour indexes are a
  `fluree server` capability — built into the server with its `vector` feature,
  managed as a graph source, and queried with an `f:queryVector` pattern. This
  is where a corpus goes when it outgrows a scan.

The split is deliberate: the CLI is a local tool over a folder, and a server is
where a corpus with an index-shaped workload belongs.

## Mixing models

A query vector is only comparable to chunks embedded by the same model. After
changing `[doc.embedding]`, re-ingest the whole folder rather than part of it,
or the ranking silently mixes two vector spaces.
