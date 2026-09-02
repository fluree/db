# fluree doc

Turn a folder of documents into a searchable graph, and search it.

`fluree doc ingest` reads PDF, Markdown, HTML, DOCX, PPTX and image files and writes, per document, one commit holding:

- a **DoCO structure graph** from the [fluree-doc-parse](https://github.com/fluree/fluree-doc-parse) engine: sections, paragraphs, lists, captions and table cells, each with character offsets into the text projection and, for PDFs, page index and bounding box;
- **retrieval chunks** cut along that structure, each citing the elements it was built from and carrying its section path;
- an **embedding** per chunk, when an embedding endpoint is configured;
- a **document node** recording the file, its content hash, the parser revision and the embedding model.

It then creates or syncs a BM25 full-text index and, when embeddings were produced, an HNSW vector index over the chunks. `fluree doc search` queries those indexes and joins each hit back to its chunk text, section path and source file.

Everything runs in-process against local storage. Parsing is deterministic and makes no network connection unless a model endpoint is configured.

## Subcommands

| Subcommand | Description |
|------------|-------------|
| `ingest` | Parse documents into a ledger and build the indexes over them |
| `search` | Search a ledger's chunks by meaning (vector) or by words (full-text) |

## fluree doc ingest

### Usage

```bash
fluree doc ingest <PATH>... [-l <LEDGER>] [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<PATH>...` | Files or directories. Directories are walked recursively; hidden entries are skipped. |

### Options

| Option | Description |
|--------|-------------|
| `-l, --ledger <LEDGER>` | Target ledger (default: the active ledger). Created when it does not exist. |
| `--base-iri <IRI>` | Prefix documents are minted under (default `urn:fluree:doc:`). The path relative to the ingested directory is appended, so a document keeps its IRI across runs. |
| `--no-embed` | Skip embeddings even when `[doc.embedding]` is configured. |
| `--no-escalate` | Never call a vision model, whatever `[doc.vlm]` says. |
| `--no-index` | Skip building or syncing the vector and full-text indexes. |
| `--no-cache` | Neither read nor write the parse and reading caches. |
| `--force` | Re-ingest documents the ledger already holds with the same content, parser and embedding model. |
| `--min-chars <N>` | Emit a chunk once its buffer reaches this many characters (default `800`). |
| `--max-chars <N>` | Split a single element longer than this many characters (default `2000`). |
| `--max-crops <N>` | Most crops one document may send to the vision model (default `70`). |
| `--dry-run` | Parse, chunk and embed, then report what would be written. Nothing is written. |
| `--out-dir <DIR>` | Also write each document's transaction as JSON-LD into this directory. |

### What a run does

For each document, in path order:

1. **Skip if unchanged.** The ledger's document node records the file's SHA-256, the parser revision and the embedding model. When all three match, the document is reported as `unchanged` and skipped. `--force` overrides this.
2. **Parse**, served from the parse cache when the same bytes were parsed with the same settings before. PDF structure is inferred from glyph geometry; the other formats declare their structure. Pages the deterministic pass cannot read (scans, pixel-only regions, tables whose structure it doubts) are sent to the configured vision model as crops, and the readings are spliced back in under the engine's arbitration rules.
3. **Chunk** along the structure graph. A chunk closes once it reaches `--min-chars`, or at a heading once it is at least half full. Table cells carry their row and column headers.
4. **Embed** each chunk's text, prefixed with its section path, against `[doc.embedding]`.
5. **Retract the previous extraction** of the same document IRI, if any, then insert the structure graph, the chunks and the document node as one commit. The earlier extraction remains queryable at its commit.

After the documents, the full-text index `<ledger>-text` is created or synced, and the vector index `<ledger>-vectors` likewise when embeddings were produced.

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
  + full-text index contracts-text:main: 142 chunk(s), 1631 terms
  + vector index contracts-vectors:main: 142 vector(s), 768 dims

done: 2 ingested, 1 unchanged, 0 failed — 142 chunks, 41 pages, 0 crop(s) read, 0 parse(s) from cache, 9.4s
```

## fluree doc search

### Usage

```bash
fluree doc search <QUERY> [-l <LEDGER>] [-n <N>] [--mode auto|vector|text] [--json]
```

### Options

| Option | Description |
|--------|-------------|
| `-l, --ledger <LEDGER>` | Ledger to search (default: the active ledger). |
| `-n, --limit <N>` | Results to return (default `10`). |
| `--mode <MODE>` | `vector` embeds the query with `[doc.embedding]` and searches the HNSW index; `text` runs BM25; `auto` (default) picks `vector` when an embedding endpoint is configured. |
| `--json` | Print the rows as JSON: `[score, chunk, document, file, section path, text]`. |

### Example

```bash
fluree doc search "termination notice period" -l contracts -n 3
```

```
 1. 0.812  msa-2024.pdf  Master Services Agreement / 12. Term and Termination
    Either party may terminate this Agreement for convenience on ninety (90) days' written notice…
    urn:fluree:doc:msa-2024.pdf/chunk/97
 2. 0.774  nda.docx  Term
    This Agreement terminates two years after the Effective Date unless…
    urn:fluree:doc:nda.docx/chunk/2
(2 result(s), vector, 41 ms)
```

The chunk IRI is a real node: follow `doc:sourceElement` to the paragraphs and cells it was built from, and from those `nif:beginIndex`, `doc:pageIndex` and `doc:bbox` locate the passage on the page.

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

[doc.llm]                               # reserved for entity and relation extraction
url = "https://api.openai.com/v1"
model = "gpt-5-mini"
api_key = "$OPENAI_API_KEY"
```

All three slots speak the OpenAI wire shape, so OpenAI, Ollama, vLLM and LM Studio are configured the same way. Each slot can be overridden by environment variables `FLUREE_DOC_{EMBEDDING,LLM,VLM}_{URL,MODEL,API_KEY,DIMENSIONS,API}`.

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

Two caches under `.fluree/cache/doc/` make re-runs cheap:

- **Parse cache** — keyed on the file's content hash plus the parser revision, the document IRI and the escalation model. A re-run over an unchanged folder parses nothing.
- **Reading cache** — keyed on the crop's pixels, the prompt and the model. A parser upgrade re-routes pages, but a crop whose pixels did not change is answered without a model call.

`--no-cache` bypasses both. Deleting the directory is always safe.

## The graph

Structure elements use the DoCO, NIF and pattern ontologies under `doc:` (`https://ns.flur.ee/doc#`) as documented by fluree-doc-parse. Chunks and document nodes extend the same namespace:

| Node / property | Meaning |
|---|---|
| `doc:SourceDocument` | The file. `doc:fileName`, `doc:relativePath`, `doc:sha256`, `doc:mediaType`, `doc:byteSize`, `doc:pageCount`, `doc:escalatedCrops`, `doc:parserRevision`, `doc:chunkCount`, `doc:embeddingModel`, `doc:embeddingDimensions`, `doc:ingestedAt`. |
| `doc:Chunk` | A retrieval unit. `doc:chunkIndex`, `doc:text`, `doc:headerPath`, `doc:sourceElement` (the DoCO elements it was built from), `doc:sourceDocument`, `doc:embedding` (an `@vector`). |
| `doc:sourceDocument` | On every element and chunk: the document it came from, and the tag a re-ingest retracts by. |

Document IRIs are `<base-iri><relative path>`, elements `<document>/element/<n>`, chunks `<document>/chunk/<n>`.
