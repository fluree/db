# Fluree Unstructured

Unstructured documents in, a knowledge graph out — from the command line, with nothing to run but the Fluree CLI.

`fluree doc ingest ./contracts` reads PDFs, Word and PowerPoint files, Markdown, HTML and scans, and writes each one into a Fluree ledger as three connected layers: the document's **structure** (sections, paragraphs, lists, tables cell by cell, each with character offsets and, for PDFs, page and bounding box), **retrieval chunks** cut along that structure, and an **embedding** per chunk. `fluree doc search` then answers a question with chunks that carry their section path, their source file and the exact elements they came from.

That is the difference from a document-to-text converter with a vector database bolted on. The output is a knowledge graph in a database with time travel, branching, policies and SPARQL, so a retrieved passage is a node you can walk: to the paragraph and table cells it was built from, to the page and rectangle they occupy, to the section heading above them, and to whatever else in your graph mentions the same things.

## Three ways to run it

| Tier | What you need | What you get |
|---|---|---|
| **Local only** | The CLI. Nothing else. | Structure graph, chunks, full-text search. Deterministic, offline. Scanned pages stay unread. |
| **Local, your models** | Any OpenAI-compatible endpoints: Ollama, vLLM, OpenAI… | Adds embeddings and vector search, and a vision model for scans and unreadable regions. |
| **With a Fluree AI account** | `fluree auth login` once. | Same pipeline, nothing model-related to configure: your account's gateway supplies the models. The ledger stays local unless you publish it. |

A fourth, Fluree AI's hosted extraction pipeline — trained entity rankers, relation extraction with review, entity resolution across documents — is used from the Fluree AI application today, and will be reachable from `fluree doc` later.

## Design philosophy

- **Structure first, models second.** The parsing engine is deterministic and places third of seventeen engines on a public benchmark with no model at all. Models are asked only about what the deterministic pass could not read, per page and per region, and their answers are arbitrated against what the page says rather than trusted outright.
- **Provenance is the product.** Every chunk cites the elements it was built from; every element carries character offsets and, for PDFs, its page and box. A highlight on the page and the text a model reasons over come from one parse, so they agree by construction.
- **Re-runs cost only what changed.** Parses are cached on content, model readings on the crop's pixels. A document whose bytes, parser and embedding model are unchanged is skipped; a changed one is retracted and replaced, never diffed, and the previous extraction remains queryable at its commit.
- **Yours by default.** A local ledger, local caches, and no network connection until you point a slot at a model. Connecting an account changes where model calls go, not where your data lives.

## Where to start

- [Quickstart](getting-started/quickstart.md) — a folder in, a search out, in five minutes.
- [Connect a Fluree AI account](getting-started/fluree-ai.md) — register, log the CLI in, and stop configuring models.
- [What gets built](concepts/what-gets-built.md) — the three layers and the vocabulary they use.
- [Models and calls](concepts/models-and-calls.md) — the three slots, which calls are made, and where data goes.
- [Publish to Fluree AI](guides/publish-to-fluree-ai.md) — move a built ledger to your account.
- [CLI reference](cli/README.md) — `ingest` and `search`, flag by flag.
