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

## How well does it read?

The parsing engine is scored on [opendataloader-bench](https://github.com/opendataloader-project/opendataloader-bench): 200 public PDFs with hand-checked ground truth, three metrics (NID for reading order and text, TEDS for table structure, MHS for heading structure), and a harness neither written nor tuned by Fluree. Measured 2026-08-01, the top 8 of the 17 engines scored:

| # | engine | overall | NID | TEDS | MHS | s/doc |
|---|---|---|---|---|---|---|
| 1 | **fluree-doc-parse** (cascade) | **0.933** | 0.948 | 0.944 | 0.876 | ~1.5 |
| 2 | opendataloader-hybrid | 0.907 | 0.934 | 0.928 | 0.821 | 0.463 |
| 3 | **fluree-doc-parse** (deterministic) | **0.892** | 0.923 | 0.847 | 0.813 | **~0.009** |
| 4 | nutrient | 0.885 | 0.925 | 0.708 | 0.819 | 0.008 |
| 5 | docling | 0.882 | 0.898 | 0.887 | 0.824 | 0.762 |
| 6 | opendataloader-hybrid-hydrogen | 0.877 | 0.926 | 0.796 | 0.769 | 5.068 |
| 7 | pdf-inspector | 0.875 | 0.915 | 0.814 | 0.788 | 0.006 |
| 8 | marker | 0.861 | 0.890 | 0.808 | 0.796 | 53.932 |

Two rows matter for the tiers above.

- The **deterministic** engine — no model, no GPU, no API key, about 8 ms per document on a CPU — is what the local tier runs. It is the best model-free engine on the board: ahead of every other engine that runs without a model (nutrient and pdf-inspector, the other two under 10 ms per document), and ahead of most of the model-assisted ones too, including docling and marker. Only one model-assisted engine scores above it.
- The **cascade**, which adds a vision model for just the pages and regions the deterministic pass could not read, is what a `vlm` slot or a Fluree AI account gives you. It places first, ahead of every engine scored, model-assisted or not, on every one of the three metrics.

Across that corpus 113 of the 200 documents never needed the model at all; of the 87 that did, the median cost 1.7 s and the worst 18.9 s.

Every score reproduces from committed model-output caches without a GPU or a key, and the engine's own accounting of [where its output is better than the reference and scores lower for it](https://github.com/fluree/fluree-doc-parse/blob/main/docs/benchmarks/where-we-differ.md) is published alongside. Full detail: [the benchmarks page](https://github.com/fluree/fluree-doc-parse/blob/main/docs/benchmarks/README.md).

## Design philosophy

- **Structure first, models second.** The parsing engine is deterministic and places third of seventeen engines on the public benchmark above with no model at all. Models are asked only about what the deterministic pass could not read, per page and per region, and their answers are arbitrated against what the page says rather than trusted outright.
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
