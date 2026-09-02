# fluree doc ingest

Parse documents into a ledger: structure graph, retrieval chunks, embeddings, and the vector and full-text indexes over them.

```bash
fluree doc ingest <PATH>... [-l <LEDGER>] [OPTIONS]
```

## Arguments

| Argument | Description |
|---|---|
| `<PATH>...` | Files or directories. Directories are walked recursively; hidden entries are skipped. PDF, Markdown, HTML, DOCX, PPTX, PNG, JPEG. |

## Options

| Option | Description |
|---|---|
| `-l, --ledger <LEDGER>` | Target ledger (default: the active ledger). Created when it does not exist. |
| `--base-iri <IRI>` | Prefix documents are minted under (default `urn:fluree:doc:`). The path relative to the ingested directory is appended, so a document keeps its IRI across runs. |
| `--no-embed` | Skip embeddings even when an embedding slot is configured. |
| `--no-escalate` | Never call a vision model, whatever is configured. |
| `--no-index` | Skip building or syncing the vector and full-text indexes. |
| `--no-cache` | Neither read nor write the parse and reading caches. |
| `--force` | Re-ingest documents the ledger already holds with the same content, parser and embedding model. |
| `--min-chars <N>` | Emit a chunk once its buffer reaches this many characters (default `800`). |
| `--max-chars <N>` | Split a single element longer than this many characters (default `2000`). |
| `--max-crops <N>` | Most crops one document may send to the vision model (default `70`). A document asking for more lands with the deterministic tier only and is flagged. |
| `--dry-run` | Parse, chunk and embed, then report what would be written. Nothing is written. |
| `--out-dir <DIR>` | Also write each document's transaction as `<relative-path>.jsonld` here. |

## What a run does

For each document, in path order:

1. **Skip if unchanged** — same SHA-256, parser revision and embedding model as the document node in the ledger. `--force` overrides.
2. **Parse**, from the parse cache when possible. Pages the deterministic pass cannot read are sent to the vision slot as crops, and the readings are spliced back under the engine's arbitration rules.
3. **Chunk** along the structure graph.
4. **Embed** each chunk, prefixed with its section path, against the embedding slot.
5. **Retract** the previous extraction of the same document IRI, if any, then **insert** structure, chunks and document node as one commit.

Then the full-text index `<ledger>-text` is created or synced, and the vector index `<ledger>-vectors` likewise when embeddings were produced; an index built for a different vector width is rebuilt.

## Output

```
ingest 3 document(s) → contracts
  account    acct (Fluree AI gateway supplies unset model slots)
  parser     fluree-doc-parse 407daa0
  escalation auto (crops the parser cannot read)
  embedding  text-embedding-3-small
→ created ledger contracts
  ✓ msa-2024.pdf  parsed: 41p, 512 elements, 138 chunks, 2 crop(s) read, embedded  t=1
  ✓ nda.docx  parsed: 19 elements, 4 chunks, embedded  t=2
  = sow/q3.pdf  unchanged
  + full-text index contracts-text:main: 142 chunk(s), 1631 terms
  + vector index contracts-vectors:main: 142 vector(s), 1536 dims

done: 2 ingested, 1 unchanged, 0 failed — 142 chunks, 41 pages, 2 crop(s) read, 0 parse(s) from cache, 14.2s
```

`parsed` versus `cached` says whether the parse cache answered. A document that failed is marked `✗` with the reason, counted, and the command exits non-zero after finishing the rest.
