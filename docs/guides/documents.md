# Documents into a graph with `fluree doc`

A folder of PDFs, Word and PowerPoint files, Markdown, HTML and scans goes in. What comes out is a ledger you can search by meaning and by words, where every hit is a citation into the structure of the document it came from: the section it sits under, the paragraphs and table cells it was built from, and for PDFs the page and bounding box. That ledger is ordinary Fluree data, so it can be branched, time-travelled, queried with SPARQL or JSON-LD, joined to whatever else you hold, and published to a Fluree AI account.

This guide is the narrative: what gets built, the three ways to run it, and how it fits with Fluree AI. The flag-by-flag reference is [`fluree doc`](../cli/doc.md).

## What one run builds

```bash
fluree doc ingest ./contracts -l contracts
fluree doc search "termination notice period" -l contracts
```

For every document, one commit containing:

| Layer | What it holds | Where it comes from |
|---|---|---|
| **Structure graph** | Sections, paragraphs, lists, captions, tables cell by cell, each with character offsets into the text projection and, for PDFs, page index and bounding box. DoCO and NIF vocabularies under `doc:`. | The [fluree-doc-parse](https://github.com/fluree/fluree-doc-parse) engine, in-process. Deterministic: the same bytes give the same graph. |
| **Chunks** | Retrieval units cut along that structure, never mid-paragraph by character count alone. Each carries its section path and cites the elements it was built from. | The chunker, in-process. |
| **Embeddings** | One vector per chunk, stored as an `@vector` literal. | An embedding model, if one is configured. |
| **Document node** | The file's name, path, hash, page count, the parser revision and embedding model used, and when. | The run itself. |

Then two indexes over the chunks: a BM25 full-text index and, when embeddings were produced, an HNSW vector index. `fluree doc search` queries either and joins the hit back to its chunk, section path and file.

Re-running over the same folder is cheap and safe. A document whose bytes, parser and embedding model are unchanged is skipped. A changed one is retracted by its `doc:sourceDocument` stamp and re-inserted, and the earlier extraction stays queryable at its commit. Parses and model readings are cached under `.fluree/cache/doc/`.

## The three tiers

The pipeline is the same in every tier. What changes is where the models come from and how much of the work runs on your machine.

### Tier 1: local only

Nothing configured beyond a Fluree project. Parsing is deterministic and the command makes no network connection. You get the structure graph, chunks and full-text search. What you do not get:

- vector search, because nothing produced embeddings;
- scanned pages and pixel-only regions, because nothing can read them. Such pages are reported as unread rather than silently dropped.

Add models you run or pay for yourself, each as an OpenAI-compatible endpoint:

```bash
fluree config set doc.embedding.url http://localhost:11434/v1     # Ollama, vLLM, LM Studio, OpenAI…
fluree config set doc.embedding.model nomic-embed-text
fluree config set doc.vlm.url https://api.openai.com/v1            # reads crops the parser cannot
fluree config set doc.vlm.model gpt-5-mini
fluree config set doc.vlm.api_key '$OPENAI_API_KEY'                # a $NAME value reads the environment
```

Three slots exist: `embedding`, `vlm` for reading document crops, and `llm` for entity and relation extraction. `vlm` falls back to `llm`, since one multimodal model is often all a machine has. This tier is the most setup and the fewest features, and it is fully yours: no account, no data leaving the machine unless you point a slot at a hosted API.

### Tier 2: Fluree AI as your model gateway

A Fluree AI account already holds model access, so with an account you configure nothing model-related locally:

```bash
fluree remote add acct https://<your-stack>/v1/fluree     # once
fluree auth login --remote acct                          # once; browser or device login
fluree config set doc.remote acct                        # per project
fluree doc ingest ./contracts -l contracts
```

`doc.remote` fills every model slot you have not set yourself with the account's gateway and your login. The pipeline still runs on your machine and the ledger is still local. Only the model calls go to Fluree AI, which routes each one to the provider behind your account, whether that is a key your organisation configured on the stack or Fluree's own proxy. Any slot you set explicitly still wins, so a local embedding model can be paired with the account's vision model.

Nothing is stored in Fluree AI in this tier. Your documents are not uploaded; crops of pages the parser could not read and chunk text for embedding are sent to the gateway and answered, exactly as a direct call to a model provider would be.

### Tier 3: Fluree AI hosted extraction

Fluree AI runs a larger extraction pipeline on its side: trained entity rankers with a feedback loop, relation extraction with review, entity resolution across documents, and a parse cache shared by everyone on the account. That tier is used from the Fluree AI application today. Handing a folder to it from `fluree doc` is planned and not yet built; the sections below on publishing cover what works now.

## Getting a Fluree AI account and logging the CLI in

A Fluree AI stack has a URL of its own. Open it, and either sign in with your organisation's single sign-on or use **Create a new account** on the sign-in page. If someone invited you, the invitation carries the URL.

Register the stack as a remote using its CLI base, which is the stack URL plus `/v1/fluree`. The CLI discovers the stack's OpenID Connect configuration on its own:

```bash
fluree remote add acct https://dk8lm4bi7xr8j.cloudfront.net/v1/fluree
  info: auto-discovered OIDC auth from server
  Run `fluree auth login --remote acct` to authenticate
```

Log in. The CLI opens your browser at the stack's activation page with a device code and waits; approve it there and the token is stored in the config file that holds the remote:

```bash
fluree auth login --remote acct
fluree auth status --remote acct
```

Run those inside the project: the remote and its login are stored in the nearest `.fluree/config.toml`, and the sync commands (`publish`, `push`, `query <remote>/<ledger>`) look only there. `doc.remote` is the one setting that also finds a remote registered from your home directory in `~/.fluree/config.toml`, so a single login can serve model access for every project even where publishing is set up per project. Logins expire after a few hours and are refreshed automatically when a command needs them; when the refresh token itself has lapsed, `fluree auth login` again.

## Where the data lives, and which calls are made

Whichever tier you use, `fluree doc ingest` writes to a **local ledger** in the project's `.fluree/storage`. It creates the ledger if it does not exist. Fluree AI is never the write target of an ingest.

Every model interaction is a separate call, and there are three kinds:

| Step | Route on the gateway or endpoint | Slot | When |
|---|---|---|---|
| Read a crop the parser could not | `POST /v1/responses` with `input_image`, intent `doc-parse` | `vlm` (falls back to `llm`) | Per crop, only for pages the deterministic pass flagged. Cached on the crop's pixels. |
| Embed a chunk batch | `POST /v1/embeddings` | `embedding` | Per document, batches of chunks. |
| Extract entities and relations | `POST /v1/responses`, intent `extraction` | `llm` | Per chunk, when extraction is enabled (in progress). |

So the vision read and the language-model extraction are distinct calls with distinct intents, and they can be different models. Against Fluree AI both go to the same route and the gateway picks the provider per intent. Against endpoints you configure yourself, `vlm` and `llm` are simply two configurations, which may point at the same model.

Costs to keep in mind. The deterministic parse is milliseconds per page. A vision read is seconds per crop, so `--max-crops` caps what one document may ask for; a document over the cap lands with the deterministic tier only and says so. Embeddings are cheap per chunk but there are many chunks.

## Getting the result into Fluree AI

The ledger you built is a normal ledger, so the remote-sync commands move it:

```bash
fluree publish acct contracts       # creates contracts on the stack and pushes every commit
fluree push contracts               # later runs: push what is new
fluree query acct/contracts -e '…'  # query the hosted copy
```

The remote must be registered in this project's `.fluree/` for `publish` to find it (see the login section above).

That carries the structure graph, the chunks, the embeddings and the document nodes. It does not carry the two indexes, which are separate graph sources on your machine:

- the full-text index can be rebuilt on the stack with `fluree bm25 create --remote acct --ledger contracts:main …` using the same indexing query `fluree doc ingest` used, which selects `doc:Chunk` nodes with `doc:text` and `doc:headerPath`;
- the vector index has no remote creation route yet. On the stack, vector similarity over the published `doc:embedding` values is available through the inline `cosineSimilarity` functions in a query, and through Fluree AI's own retrieval.

Parsing documents **directly into a ledger that lives on Fluree AI** is not supported by `fluree doc ingest` today: it runs in-process against local storage. Two workable paths in the meantime:

1. ingest locally, then `fluree publish` as above; or
2. ingest with `--out-dir`, which writes each document's transaction as JSON-LD, and `fluree insert --remote acct` those files into the hosted ledger. You lose the automatic retract-on-rerun, so treat it as a one-time load.

## Troubleshooting

| Symptom | Meaning |
|---|---|
| `escalation none — deterministic tier only` | No `vlm` or `llm` slot. Scans stay unread. Configure a slot or `doc.remote`. |
| `doc.remote 'acct': authentication failed (401)` | The stored login has fully expired. `fluree auth login --remote acct`. |
| `NoEmbeddingProvider` from the gateway | The stack has neither an OpenAI-type provider key nor a proxy connection that serves embeddings. Configure one in the stack's settings, or set `doc.embedding` to an endpoint of your own. |
| `deterministic tier only: N crop(s) … past the cap` | The document asks the vision model for more crops than `--max-crops`. Raise it deliberately; the cap exists because each crop is a model call. |
| A page reported as unread | The parser found pixels it could not read and no vision model was available. |
| `[doc] section: …` on startup | The `[doc]` table in `config.toml` is malformed. A present but broken section is an error rather than a silent fallback, because running without the model you configured would look like success. |
