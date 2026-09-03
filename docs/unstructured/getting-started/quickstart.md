# Quickstart

## 1. Ingest a folder

```bash
fluree doc ingest ./contracts -l contracts
```

```
ingest 3 document(s) → contracts
  parser     fluree-doc-parse 407daa0
  escalation none — deterministic tier only; set [doc.vlm] to read scanned pages
  embedding  none — set [doc.embedding] to enable vector search
→ created ledger contracts
  ✓ msa-2024.pdf  parsed: 41p, 512 elements, 138 chunks  t=1
  ✓ nda.docx  parsed: 19 elements, 4 chunks  t=2
  ✓ sow/q3.pdf  parsed: 12p, 96 elements, 27 chunks  t=3
  + full-text index contracts-text:main: 169 chunk(s), 1631 terms

done: 3 ingested, 0 unchanged, 0 failed — 169 chunks, 53 pages, 0 crop(s) read, 0 parse(s) from cache, 6.1s
```

The ledger was created, each document landed as one commit, and a full-text index was built over the chunks. Two lines say what is not configured yet: no vision model, so scanned pages would stay unread, and no embedding model, so there is no vector index.

## 2. Search it

```bash
fluree doc search "termination notice period" -l contracts -n 3
```

```
 1. 12.687  msa-2024.pdf  Master Services Agreement / 12. Term and Termination
    Either party may terminate this Agreement for convenience on ninety (90) days' written notice…
    urn:fluree:doc:msa-2024.pdf/chunk/97
 2. 6.801   nda.docx  Term
    This Agreement terminates two years after the Effective Date unless…
    urn:fluree:doc:nda.docx/chunk/2
(2 result(s), text, 41 ms)
```

Each hit is a score, the file, the section path the chunk sits under, a snippet, and the chunk's IRI. With no embedding model this is BM25 full-text search.

## 3. Add embeddings

Point the `embedding` slot at any OpenAI-compatible endpoint. A local Ollama works:

```bash
ollama pull nomic-embed-text
fluree config set doc.embedding.url http://localhost:11434/v1
fluree config set doc.embedding.model nomic-embed-text
fluree doc ingest ./contracts -l contracts
```

The documents are re-ingested — the embedding model changed, so they are no longer "unchanged" — and a vector index is built. `fluree doc search` now runs both indexes and fuses them (`--mode hybrid`, the default once both exist); `--mode vector` and `--mode text` give you one or the other.

Or skip model configuration entirely by [connecting a Fluree AI account](fluree-ai.md).

## 4. Look at the graph

A chunk is a node. Its `doc:sourceElement` values are the paragraphs and table cells it was built from, and those carry offsets, page and box:

```bash
fluree query contracts -e '{
  "@context": {"doc": "https://ns.flur.ee/doc#", "nif": "http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#"},
  "where": [
    {"@id": "urn:fluree:doc:msa-2024.pdf/chunk/97", "doc:sourceElement": "?el"},
    {"@id": "?el", "doc:pageIndex": "?page", "doc:bbox": "?box", "nif:beginIndex": "?start"}
  ],
  "select": ["?el", "?page", "?box", "?start"]
}'
```

From there the whole graph is open: the section containing the element, the document node with the file's hash and ingest time, or the same passage at an earlier commit. See [Querying the graph](../guides/querying-the-graph.md).

## 5. Say what it is about

Point the ingest at the entities you already have and, if you want relations, at an ontology:

```bash
fluree doc ingest ./contracts -l contracts --entities counterparties.ttl --model ./contracts-ontology.ttl
```

Every mention of a counterparty is written against the IRI it has in `counterparties.ttl`, so a query across that graph and the contracts joins on the same node. The ontology needs a language model in the `llm` slot or a Fluree AI account; the mentions alone need nothing. See [Entities and relations](../concepts/entities-and-relations.md).
