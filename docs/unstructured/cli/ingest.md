# fluree doc ingest

Parse documents into a ledger: structure graph, retrieval chunks, embeddings, the vector and full-text indexes over them, and, given an ontology and known entities, mentions and relations.

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
| `--model <LEDGER\|FILE>` | Ontology the language model extracts against: classes and properties from a ledger or a `.ttl` / `.jsonld` / `.nt` / `.json` file. Needs the `llm` slot or a Fluree AI account. |
| `--entities <LEDGER\|FILE[#CLASS]>` | Known entities to find by label (`skos:prefLabel`, `skos:altLabel`, `skos:hiddenLabel`, `rdfs:label`, `schema:name`, `schema:alternateName`). Repeatable; `#Class` keeps only subjects of that type. A mention keeps the entity's own IRI. Works without `--model` and without any model. |
| `--relations <MODE>` | `direct` (default) writes an edge for every predicate the ontology admits, plus the reified node; `reified` writes the nodes only; `off` extracts entities alone. |
| `--guidance <FILE>` | Project priorities placed in the extraction prompt. Config: `doc.extraction.guidance`. |
| `--system-prompt <FILE>` | Replaces the extraction system prompt; keep the `{model}` and `{guidance}` slots. Config: `doc.extraction.system_prompt`. |
| `--user-prompt <FILE>` | Replaces the extraction user prompt; keep the `{existing}` and `{document}` slots. Config: `doc.extraction.user_prompt`. |
| `--concurrency <N>` | Chunks sent to the language model at once (default `4`). Config: `doc.extraction.concurrency`. |
| `--drop-off-model` | Drop new entities whose class is not in the ontology instead of keeping them flagged `doc:offModel`. Config: `doc.extraction.drop_off_model`. |
| `--lang <CODE>` | Language for stemming in the entity scan (default `en`). |
| `--no-extract` | Skip extraction even when `--model` or `--entities` is given. |
| `--dry-run` | Parse, chunk and embed, then report what would be written. Nothing is written. |
| `--out-dir <DIR>` | Also write each document's transaction as `<relative-path>.jsonld` here. |

## What a run does

For each document, in path order:

1. **Skip if unchanged** — same SHA-256, parser revision, embedding model and extraction fingerprint as the document node in the ledger. `--force` overrides.
2. **Parse**, from the parse cache when possible. Pages the deterministic pass cannot read are sent to the vision slot as crops, and the readings are spliced back under the engine's arbitration rules.
3. **Chunk** along the structure graph.
4. **Embed** each chunk, prefixed with its section path, against the embedding slot.
5. **Extract**, when asked: scan each chunk for the `--entities` labels, then ask the `llm` slot about the chunk with the ontology and the entities found, and gate what comes back. See [Entities and relations](../concepts/entities-and-relations.md).
6. **Retract** the previous extraction of the same document IRI, if any, then **insert** structure, chunks, document node, mentions, relations and new entities as one commit.

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

With extraction on, the header names the ontology and the gazetteer, each document line adds its mentions, entities and relations, and a final line totals them:

```
  model      ./ontology.ttl (14 classes, 22 properties)
  extraction auto (relations: reified + direct edges; off-model entities: kept, flagged; 4 chunk(s) at once)
  entities   people: 412 label(s)
  entities   orgs.ttl#schema:Organization: 38 label(s)
  gazetteer  187 entities
  ✓ msa-2024.pdf  parsed: 41p, 512 elements, 138 chunks, embedded, 96 mention(s) of 31 entities (9 new, 3 off-model), 44 relation(s) (3 rejected)  t=1
  …
  extraction: 131 mention(s), 12 new entities (4 off-model), 58 relation(s) (5 rejected), 2 dropped (2 hallucinated, 0 off-model), 0 chunk(s) from cache
```

A chunk whose model call failed is reported on the document line as `not extracted` and the document is not stamped, so the next run asks about it again:

```
  ✓ nda.docx  parsed: 19 elements, 4 chunks, embedded, 6 mention(s) of 3 entities (1 new), 1 chunk(s) not extracted  t=2
    ! extraction incomplete, will be retried next run: chunk 2: model endpoint: https://…/responses: 503: …
```

`parsed` versus `cached` says whether the parse cache answered. A document that failed is marked `✗` with the reason, counted, and the command exits non-zero after finishing the rest.
