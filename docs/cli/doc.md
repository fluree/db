# fluree doc

Turn a folder of documents into a searchable graph, and search it.

`fluree doc ingest` reads PDF, Markdown, HTML, DOCX, PPTX, XLSX, WebVTT and SubRip transcripts, email (`.eml`, `.msg`) and image files and writes, per document, one commit holding:

- a **DoCO structure graph** from the [fluree-doc-parse](https://github.com/fluree/fluree-doc-parse) engine: sections, paragraphs, lists, tables and their cells, each with character offsets into the text projection and, for PDFs, page index and bounding box;
- **retrieval chunks** cut along that structure, each citing the elements it was built from and carrying its section path;
- an **embedding** per chunk, when an embedding endpoint is configured;
- a **document node** recording the file, its content hash, the parser revision and the embedding model;
- with `--entities` and `--ontology`, **mentions** of known entities under their own IRIs, **relations** the text states (reified with evidence and verdict, and as edges when the ontology admits them), and any **new entities** the language model found.

It then creates or syncs a BM25 full-text index over the chunks. `fluree doc search` joins each hit back to its chunk text, section path and source file.

There is no vector index: `--mode vector` scores every chunk's `doc:embedding` exactly with `cosineSimilarity`, so no ANN library is linked into the CLI and nothing has to be built, synced or rebuilt. An approximate HNSW index for corpora past the point where a scan is cheap is a `fluree server` capability.

Everything runs in-process against local storage: the ledger written is always a local one, and Fluree AI, when connected, supplies models only. Parsing is deterministic and makes no network connection unless a model endpoint is configured.

> This page is the CLI command reference. For the walkthrough — the tiers, connecting a Fluree AI account, what gets built, which model calls are made, and publishing the result — see the [Fluree Unstructured section](../unstructured/README.md) of the docs.

## Subcommands

| Subcommand | Description |
|------------|-------------|
| `ingest` | Parse documents into a ledger and build the indexes over them |
| `search` | Search a ledger's chunks by meaning (vector) or by words (full-text) |
| `parse` | Parse one document and print its DoCO graph, without writing to a ledger |

## fluree doc ingest

### Usage

```bash
fluree doc ingest <PATH>... [-l <LEDGER>] [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<PATH>...` | Files or directories. Directories are walked recursively; hidden entries are skipped. Read by extension: `.pdf`; `.md`, `.txt`; `.html`; `.docx`; `.pptx`; `.xlsx`; `.vtt`, `.srt`; `.eml`, `.msg`; `.png`, `.jpg`, `.gif`, `.webp` (images need a vision model). A `.txt` holding a transcript or a saved email is read as one. |

### Options

| Option | Description |
|--------|-------------|
| `-l, --ledger <LEDGER>` | Target ledger (default: the active ledger). Created when it does not exist. |
| `--base-iri <IRI>` | Prefix documents are minted under (default `urn:fluree:doc:`). The path relative to the ingested directory is appended, so a document keeps its IRI across runs. |
| `--no-embed` | Skip embeddings even when `[doc.embedding]` is configured. |
| `--no-escalate` | Never call a vision model, whatever `[doc.vlm]` says. |
| `--no-index` | Skip indexing after the run: the ledger's own binary index, and the full-text index. |
| `--no-cache` | Neither read nor write the parse and reading caches. |
| `--force` | Re-ingest documents the ledger already holds with the same content, parser and embedding model. |
| `--min-chars <N>` | Emit a chunk once its buffer reaches this many characters (default `1500`). |
| `--max-chars <N>` | Split a single element longer than this many characters (default `4000`). |
| `--max-crops <N>` | Most crops one document may send to the vision model (default `70`). A document asking for more is parsed with the deterministic tier only and flagged in the output; nothing is sent. |
| `--ontology <LEDGER\|FILE>` | Ontology extraction reads against: a ledger, or a `.ttl` / `.nt` / `.jsonld` / `.json` file. Needs a language model (`[doc.llm]` or `doc.remote`) or a reader (`--model`). |
| `--model <ID>` | The extraction model. An id a [`[doc.reader]`](#an-http-reader) lists in `models` reads each document whole with that reader; any other id names the language model `[doc.llm]` runs. Default: `doc.model`, else `[doc.llm] model`. |
| `--cache <ontology\|catalog>` | For a reader: what it may keep for the next request that shares it, the ontology alone or the ontology and catalog. Config: `doc.reader.cache`. |
| `--entities <LEDGER\|FILE[#CLASS]>` (alias `--catalog`) | Known entities (a catalog): a ledger, a graph source mapped over a table (`fluree iceberg\|sql\|delta map`), or a `.ttl` / `.nt` / `.jsonld` / `.json` file. Found by `skos:prefLabel`, `skos:altLabel`, `skos:hiddenLabel`, `rdfs:label`, `schema:name`, `schema:legalName` and `schema:alternateName`. Repeatable; `#Class` scopes to one type. A mention keeps the entity's IRI. Needs no model. |
| `--relations <direct\|reified\|off>` | `direct` (default): reified nodes plus an edge per admitted predicate; `reified`: nodes only; `off`: entities alone. |
| `--guidance <FILE>` | Project priorities placed in the extraction prompt. Config: `doc.extraction.guidance`. |
| `--system-prompt <FILE>` | Replaces the extraction system prompt; keep the `{model}` and `{guidance}` slots. Config: `doc.extraction.system_prompt`. |
| `--user-prompt <FILE>` | Replaces the extraction user prompt; keep the `{existing}` and `{document}` slots. Config: `doc.extraction.user_prompt`. |
| `--concurrency <N>` | Chunks sent to the language model at once (default `4`). Config: `doc.extraction.concurrency`. |
| `--drop-off-model` | Drop new entities whose class is not in the ontology instead of keeping them flagged `doc:offModel`. Config: `doc.extraction.drop_off_model`. |
| `--lang <CODE>` | Language for stemming in the entity scan (default `en`). |
| `--no-extract` | Skip extraction even when `--ontology` or `--entities` is given. |
| `--dry-run` | Parse, chunk and embed, then report what would be written. Nothing is written. |
| `--out-dir <DIR>` | Also write each document's transaction as JSON-LD into this directory. |

### What a run does

For each document, in path order:

1. **Skip if unchanged.** The ledger's document node records the file's SHA-256, the parser revision, the embedding model and the extraction fingerprint (ontology, entity sources, model, guidance, relation mode). When all match, the document is reported as `unchanged` and skipped. `--force` overrides this.
2. **Parse**, served from the parse cache when the same bytes were parsed with the same settings before. PDF structure is inferred from glyph geometry; the other formats declare their structure. Pages the deterministic pass cannot read (scans, pixel-only regions, tables whose structure it doubts) are sent to the configured vision model as crops, and the readings are spliced back in under the engine's arbitration rules.
3. **Chunk** along the structure graph. A chunk closes once it reaches `--min-chars`, or at a heading once it is at least half full. Table cells carry their row and column headers.
4. **Embed** each chunk's text, prefixed with its section path, against `[doc.embedding]`.
5. **Extract**, when `--entities` or `--ontology` is given. Each chunk is scanned for every label of every known entity (longest whole-word match, case-folded, plus Snowball stems), and each match becomes a mention under the entity's IRI. With `--ontology`, the language model is then asked about the chunk with the ontology as its system prompt and the entities found as known names. Its entities are resolved to known IRIs where a label matches and minted otherwise; an entity whose excerpt is not in the chunk is dropped; a new entity typed outside the ontology is kept flagged `doc:offModel` (or dropped with `--drop-off-model`); each relation's predicate is judged against the ontology as `valid`, `repaired` (an unambiguous label, local name or class-to-property fix) or `rejected`, and written reified with that verdict, a relation whose object names no entity carrying it as a literal. In `direct` mode an admitted relation with an entity object is also written as an edge. Answers are cached per chunk and asked for several chunks at once; a chunk whose call fails keeps its gazetteer mentions, and the document is not stamped as extracted so the next run retries it.
6. **Retract the previous extraction** of the same document IRI, if any, then insert the structure graph, the chunks, the document node and the extraction as one commit. The earlier extraction remains queryable at its commit. Entity nodes are shared across documents and not retracted; an edge the earlier extraction asserted is dropped only when no remaining relation supports it.

After the documents, the ledger's binary index is brought up to the new head, so later invocations read it rather than replaying the commits just written (a CLI process never runs the background indexer that a server would). Then the full-text index `<ledger>-text` is created or synced. Embeddings need no index step — they are committed with the chunks and scored at search time.

### Examples

```bash
# Deterministic, offline: structure graph + chunks + full-text index
fluree doc ingest ./contracts -l contracts

# Configure an embedding endpoint once, then ingest with vectors
fluree config set doc.embedding.url http://localhost:11434/v1
fluree config set doc.embedding.model nomic-embed-text
fluree doc ingest ./contracts -l contracts

# See what a folder would produce without writing anything
fluree doc ingest ./drafts -l contracts --dry-run --out-dir ./inspect

# Find the counterparties you already track, under their own IRIs; no model needed
fluree doc ingest ./contracts -l contracts --entities counterparties.ttl

# Add an ontology and a language model: new entities and relations, gated by the ontology
fluree config set doc.llm.url http://localhost:11434/v1
fluree config set doc.llm.model qwen3
fluree doc ingest ./contracts -l contracts --entities counterparties.ttl --ontology ./contracts-ontology.ttl
```

Output:

```
ingest 3 document(s) → contracts
  parser     fluree-doc-parse 8765b40
  escalation none — deterministic tier only; set [doc.vlm] to read scanned pages
  embedding  nomic-embed-text
→ created ledger contracts
  ✓ msa-2024.pdf  parsed: 41p, 512 elements, 138 chunks, embedded  t=1
  ✓ nda.docx  parsed: 19 elements, 4 chunks, embedded  t=2
  = sow/q3.pdf  unchanged
  ⟳ ledger index contracts: t=2
  + full-text index contracts-text:main: 142 chunk(s), 1631 terms

done: 2 ingested, 1 unchanged, 0 failed — 142 chunks, 41 pages, 0 crop(s) read, 0 parse(s) from cache, 9.4s
```

## fluree doc search

### Usage

```bash
fluree doc search <QUERY> [-l <LEDGER>] [-n <N>] [--mode auto|hybrid|vector|text] [--json]
```

### Options

| Option | Description |
|--------|-------------|
| `-l, --ledger <LEDGER>` | Ledger to search (default: the active ledger). |
| `-n, --limit <N>` | Results to return (default `10`). |
| `--mode <MODE>` | `text` runs BM25 over the full-text index; `vector` embeds the query with `[doc.embedding]` and ranks every chunk by cosine similarity; `hybrid` runs both, each for three times the limit, and fuses them on calibrated scores: the cosine as it is, BM25 as `s / (s + 10)`, averaged over both methods with a missing method counting 0; `auto` (default) picks `hybrid` when the chunks carry embeddings, the full-text index exists and the query can be embedded, else whichever of the two is available. |
| `--json` | Print the hits as JSON objects: `score`, `chunk`, `document`, `file`, `path`, `text`, and `ranks` (`vector 0.53 #10 · text 11.1 #1`) for a fused hit. |

### Example

```bash
fluree doc search "termination notice period" -l contracts -n 3
```

```
 1. 0.812  msa-2024.pdf  Master Services Agreement / 12. Term and Termination
    Either party may terminate this Agreement for convenience on ninety (90) days' written notice…
    urn:fluree:doc:msa-2024.pdf-chunk-97
 2. 0.774  nda.docx  Term
    This Agreement terminates two years after the Effective Date unless…
    urn:fluree:doc:nda.docx-chunk-2
(2 result(s), vector, 41 ms)
```

A fused hit shows each method's own score and rank, which is also the diagnostic for when one method fails: a query that names the answer's terms but not its meaning can rank tenth by vector and first by text, and lands first fused. The scales differ, a cosine of 0.9 being very high while BM25 reaches 35 and 3 is weak, which is why fusion calibrates before it averages.

```
 1. 0.527  fin_fed_stmts.pdf  … / l. Federal Reserve Notes  vector 0.53 #10 · text 11.1 #1
```

The chunk IRI is a real node: follow `doc:sourceElement` to the paragraphs and cells it was built from, and from those `nif:beginIndex`, `doc:pageIndex` and `doc:bbox` locate the passage on the page.

## fluree doc parse

Prints the structure graph `fluree doc ingest` would write for one file, as JSON-LD, or the plain-text projection its offsets index into. No ledger is written or needed, so it runs outside a project too. Where there is a project (or a global config), its `[doc.vlm]` slot reads what the deterministic pass could not and the parse is cached there, exactly as for `ingest`. `FLUREE_DOC_VLM_*` / `FLUREE_DOC_LLM_*` configure a model anywhere; with none of these, or with `--no-escalate`, no model is called.

### Usage

```bash
fluree doc parse <FILE> [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `--format <FORMAT>` | `doco` (default): the DoCO JSON-LD graph. `text`: the plain-text projection that `nif:beginIndex` / `nif:endIndex` index into. |
| `-o, --output <FILE>` | Write to this file instead of stdout. |
| `--base-iri <IRI>` | Prefix the file name is appended to, forming the document IRI (default `urn:fluree:doc:`), as `ingest` does for a file named on its command line. |
| `--doc-iri <IRI>` | The document IRI in full, in place of `--base-iri` and the file name. Every node is minted under it and stamped with it. |
| `--no-escalate` | Never call a vision model, whatever `[doc.vlm]` says. |
| `--max-crops <N>` | Most crops the document may send to the vision model (default `70`). |
| `--no-cache` | Neither read nor write the parse and reading caches. |

Warnings go to stderr: pages nothing could read, crops over the cap, an email's attachments (described in the graph, not read).

### Examples

```bash
fluree doc parse report.pdf > report.jsonld
fluree doc parse call.vtt --format text
fluree doc parse memo.docx --doc-iri https://example.org/doc/memo -o memo.jsonld
```

The output is the engine's own JSON-LD, insertable as it is: `fluree insert docs -f memo.jsonld` writes the structure graph alone, without chunks, embeddings or a document node.

## Configuration

Model endpoints live in the `[doc]` table of the project's `config.toml`, set with `fluree config set`:

```toml
[doc.embedding]
url = "http://localhost:11434/v1"      # any OpenAI-compatible base URL
model = "nomic-embed-text"
# api_key = "$OPENAI_API_KEY"           # a `$NAME` value reads that environment variable
# dimensions = 768                      # for models that accept a `dimensions` parameter

[doc.vlm]                               # reads document crops; falls back to [doc.llm]
url = "https://api.openai.com/v1"
model = "gpt-5-mini"
api_key = "$OPENAI_API_KEY"
# api = "chat"                          # `chat` (default) or `responses`

[doc.llm]                               # entity and relation extraction (--ontology)
url = "https://api.openai.com/v1"
model = "gpt-5-mini"
api_key = "$OPENAI_API_KEY"

[doc.extraction]                        # how extraction is asked, project-wide
guidance = "prompts/guidance.md"        # priorities placed in the prompt
# system_prompt = "prompts/system.txt"  # replaces the system prompt; keep {model} and {guidance}
# user_prompt = "prompts/user.txt"      # replaces the user prompt; keep {existing} and {document}
# concurrency = 4
# drop_off_model = false
```

All three slots speak the OpenAI wire shape, so OpenAI, Ollama, vLLM and LM Studio are configured the same way. Each slot can be overridden by environment variables `FLUREE_DOC_{EMBEDDING,LLM,VLM}_{URL,MODEL,API_KEY,DIMENSIONS,API}`.

### An HTTP reader

Some extraction models read a whole document at once rather than a chunk at a time, and take their inputs as RDF. `[doc.reader]` names one:

```toml
[doc.reader]
url = "https://reader.example/extract"   # the URL each request is POSTed to
api_key = "$READER_API_KEY"
# auth_header = "X-Api-Key"              # default: Authorization: Bearer <key>
models = ["reader-small", "reader-large"]
# cache = "catalog"                      # what it may keep between requests (--cache)

[doc]
# model = "reader-small"                 # what --model defaults to
```

Then `fluree doc ingest ./docs -l docs --ontology onto.ttl --catalog people --model reader-small` reads each document with it. Each request is one RDF dataset (TriG) whose named graphs give the inputs their roles, under `https://ns.flur.ee/extract#`: `document` (the document node with its full text as `nif:isString`, and the parsed structure's elements with their offsets), `ontology` (the `--ontology` source as it is), `instances` (the catalog: each `--catalog` record's types, labels and short description), and, when the text needs it, `request` (the windows it is read in). The model id goes in `X-Fluree-Model` and `--cache` in `X-Fluree-Cache`; the answer is asked for as JSON-LD.

- **Long documents.** A request carries at most 48,000 characters, so a longer document is read in several, cut between elements, never inside one. A request whose text would not fit one window beside the ontology and catalog is read in up to 16 windows, cut between leaf elements.
- **What is kept.** The reader's answer is a dataset: the document graph it was sent, echoed, and the reading in graphs of its own. The echo is dropped. The reading goes into the document's commit with its offsets moved back into the document's text, every node stamped `doc:sourceDocument`, and each reified `rdf:Statement` typed `doc:Relation` (and `doc:asserted` when the reading also states its triple with an IRI object). A re-ingest retracts it like any extraction, and every triple on a node the reader minted under the document's IRI (`<doc>/…`, `<doc>#…`).
- **What is billed.** Each document line reports the tokens the reader billed (`X-Fluree-Serialized-Tokens`), prompt-cache reads and writes, and fuel when the endpoint meters it; the run's last lines total them, and say what the reader set aside or adapted in the ontology.
- **Resends.** A request is resent only when the reader answers 503 with `Retry-After` (no capacity yet, such as a model still starting), after the wait it names, up to four attempts and five minutes of waiting, each wait shown as it starts. Nothing else is resent, because any other failure may follow a read that ran and was billed.
- **Caching.** Answers are kept in the reading cache by request body, so a rerun that sends the same request (`--force` with nothing changed) is not read again; `--no-cache` reads afresh.

Environment variables `FLUREE_DOC_READER_{URL,API_KEY,AUTH_HEADER,MODELS,CACHE}` and `FLUREE_DOC_MODEL` override the file.

### Using a Fluree AI account

A Fluree AI stack serves the same routes and holds the model keys, so with an account nothing else needs configuring locally. Register the stack as a remote, log in once, and name it:

```bash
fluree remote add acct https://<your-stack>/v1/fluree     # OIDC is auto-discovered
fluree auth login --remote acct                          # browser / device login
fluree config set doc.remote acct
```

`doc.remote` fills every slot you have not set explicitly: embeddings through the gateway's `/v1/embeddings` (default model `text-embedding-3-small`), and crop reading and extraction through `/v1/responses` with `model = "auto"`, which lets the gateway route each intent to the account's vision or language provider. The remote is looked up in the project config first and the global config second, so one login from your home directory serves every project. An expired login is refreshed before the run.

Any slot set explicitly wins, so a local embedding model can be combined with the account's vision model:

```toml
[doc]
remote = "acct"

[doc.embedding]
url = "http://localhost:11434/v1"
model = "nomic-embed-text"
```

Without `doc.remote`, the same wiring can be spelled out by hand with an `flr_…` API key from Settings → API keys, using `api = "responses"` and `model = "auto"` on the `vlm` and `llm` slots.

The gateway's embeddings route forwards to the account's OpenAI-type provider, so vector search through the stack needs one configured there.

With no slot configured the command never reaches the network: it parses deterministically, writes structure and chunks, and builds the full-text index. Raster images are the one input that cannot be read without a vision model.

## Caches

Three caches under `.fluree/cache/doc/` make re-runs cheap:

- **Parse cache** — keyed on the file's content hash plus the parser revision, the document IRI and the escalation model. A re-run over an unchanged folder parses nothing.
- **Reading cache** — keyed on the crop's pixels, the prompt and the model. A parser upgrade re-routes pages, but a crop whose pixels did not change is answered without a model call.
- **Extraction cache** — keyed on the model, the ontology and guidance, the known entities in the chunk, and the chunk's text.

`--no-cache` bypasses all of them. Deleting the directory is always safe.

## The graph

Structure elements use DoCO, NIF, the pattern ontology, Dublin Core and `foaf:Document`, with Fluree's own terms under `doc:` (`https://ns.flur.ee/doc#`), as documented in fluree-doc-parse's [vocabulary reference](https://github.com/fluree/fluree-doc-parse/blob/main/docs/reference/vocabulary.md); [What gets built](../unstructured/concepts/what-gets-built.md#the-structure-graph) summarises them. Chunks and document nodes extend the same namespace:

| Node / property | Meaning |
|---|---|
| `doc:SourceDocument` | The file. `doc:fileName`, `doc:relativePath`, `doc:sha256`, `doc:mediaType`, `doc:byteSize`, `doc:pageCount`, `doc:escalatedCrops`, `doc:parserRevision`, `doc:chunkCount`, `doc:embeddingModel`, `doc:embeddingDimensions`, `doc:ingestedAt`. |
| `doc:Chunk` | A retrieval unit. `doc:chunkIndex`, `doc:text`, `doc:headerPath`, `doc:sourceElement` (the DoCO elements it was built from), `doc:sourceDocument`, `doc:embedding` (an `@vector`). |
| `doc:sourceDocument` | On every structure node, chunk, mention and relation: the document it came from, and the tag a re-ingest retracts by. A table cell carries none; it is retracted through the table that contains it. |
| `doc:Mention` | A span of a chunk naming an entity: `nif:beginIndex`, `nif:endIndex`, `nif:anchorOf`, `nif:referenceContext` (the chunk), `nif:entity`, `doc:sourceElement`, `doc:extractedBy` (`gazetteer` or `llm`). |
| `doc:Relation` | A reified statement: `rdf:subject`, `rdf:predicate`, `rdf:object`, `doc:excerpt`, `doc:verdict` (`valid`, `repaired`, `rejected`), `doc:asserted`, `doc:originalPredicate`, `doc:repairNote`, `doc:rejectionReason`, `doc:sourceChunk`. |
| `doc:Entity` | An entity minted because no `--entities` source knew it: `schema:name`, `skos:altLabel`, `doc:nerLabel`, plus attributes the ontology admits, and `doc:offModel true` when its class was not in the ontology. Known entities keep their IRI and get no node. |

Document IRIs are `<base-iri><relative path>`, structure nodes `<document>-element-<n>` (and `-section-`, `-table-`, `-cell-`…), chunks `<document>-chunk-<n>`, mentions `<chunk>-mention-<n>`, relations `<document>-relation-<n>`, minted entities `<base-iri>entity/<hash of the name>`. Nothing minted adds a `/`, `#` or `:` to the document IRI, so a document's nodes cost no namespace of their own — see [IRIs](../unstructured/concepts/what-gets-built.md#iris).

The full walkthrough, including a cross-ledger query over the entities graph and the documents graph, is in [Entities and relations](../unstructured/concepts/entities-and-relations.md).
