# Tiers

The pipeline is the same everywhere: parse, chunk, embed, write, index. What a tier changes is where the models come from and how much of the work runs on your machine.

What each tier reads, on the [public benchmark](../README.md#how-well-does-it-read) the parsing engine is scored on (200 PDFs, 2026-08-01):

| Reading | Overall | Typical cost per document |
|---|---|---|
| Deterministic only — the local tier | 0.892 (third of seventeen engines) | ~8 ms, CPU |
| With a vision model for pages and regions the deterministic pass could not read — a `vlm` slot or a Fluree AI account | 0.933 (first) | ~1.5 s averaged over the corpus; 113 of 200 documents never call the model |

## Local only

Nothing configured beyond a Fluree project. Parsing is deterministic and the command makes no network connection. You get the structure graph, chunks and full-text search, and, with `--entities`, every mention of the entities you already have, under their own IRIs: the [gazetteer scan](entities-and-relations.md) needs no model either.

What you do not get: vector search, because nothing produced embeddings; and scanned pages or pixel-only regions, because nothing can read them. Such pages are reported as unread rather than silently dropped. The deterministic engine alone still places third of seventeen on the public benchmark, and most documents in that corpus never needed more.

## Local, with models you run or pay for

Three slots, each an OpenAI-compatible endpoint you point at anything: Ollama, vLLM, LM Studio, OpenAI, Voyage's compatible route.

| Slot | Used for | Falls back to |
|---|---|---|
| `embedding` | one vector per chunk; enables vector search | — |
| `vlm` | reading crops of pages and regions the parser could not | `llm` |
| `llm` | entity and relation extraction against `--model` | — |

This is the most setup and the fewest features, and it is fully yours: no account, and no data leaves the machine unless a slot points at a hosted API. See [Local models](../guides/local-models.md).

## With a Fluree AI account

One `fluree auth login`, then `doc.remote = "<remote>"` fills every slot you did not set yourself with the account's gateway. The pipeline still runs locally and the ledger is still local; only model calls go to the account, which routes each to the provider behind it. Explicit slots win, so you can mix a local embedding model with the account's vision model. See [Connect a Fluree AI account](../getting-started/fluree-ai.md).

## Fluree AI hosted extraction

Fluree AI runs a larger pipeline on its side: trained entity rankers with a feedback loop, relation extraction with review, entity resolution across documents, a parse cache shared across the account. It is used from the Fluree AI application today. Handing a local folder to it from `fluree doc` is planned; until then, a ledger built locally can be [published](../guides/publish-to-fluree-ai.md) to the account.
