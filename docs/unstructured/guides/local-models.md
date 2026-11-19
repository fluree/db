# Local models

Every slot speaks the OpenAI wire shape, so anything that serves it works.

## Ollama

```bash
ollama pull nomic-embed-text
ollama pull qwen2.5vl                   # a vision model, for scans
fluree config set doc.embedding.url http://localhost:11434/v1
fluree config set doc.embedding.model nomic-embed-text
fluree config set doc.vlm.url http://localhost:11434/v1
fluree config set doc.vlm.model qwen2.5vl
```

No key. Ollama's `/v1` route serves both embeddings and chat completions with images.

## vLLM or LM Studio

Same shape: the server's `/v1` base as `url`, the served model name as `model`. LM Studio accepts any `api_key`; vLLM uses whatever you started it with.

## OpenAI, or any hosted OpenAI-compatible API

```bash
fluree config set doc.embedding.url https://api.openai.com/v1
fluree config set doc.embedding.model text-embedding-3-small
fluree config set doc.embedding.api_key '$OPENAI_API_KEY'
fluree config set doc.vlm.url https://api.openai.com/v1
fluree config set doc.vlm.model gpt-5-mini
fluree config set doc.vlm.api_key '$OPENAI_API_KEY'
```

A `$NAME` value reads that environment variable at run time, so the key never sits in the config file. `fluree config list` redacts key values either way.

## One model for both

Set only `llm`; `vlm` falls back to it. One multimodal model then reads crops and, when extraction is enabled, extracts entities.

## Choosing an embedding size

Embeddings are stored at whatever width the model returns. Models that accept a `dimensions` parameter can be pinned with `dimensions = 768` in the slot — smaller vectors are cheaper to store and to scan. Changing to another model of the same width later is fine: the documents are re-embedded because the model changed, and search reads the new vectors. The width itself is fixed by the first ingest: a ledger keeps one [vector length](../../indexing-and-search/vector-search.md#length) for the chunks' embeddings, even after the old ones are retracted, so a model of another width is refused with an `Invalid vector` error. To change width, ingest into a new ledger, or pin `dimensions` to the width the ledger already holds.

A query is only comparable to chunks embedded by the same model, so a corpus part-way through a model switch mixes widths. Re-ingest the whole folder after a change rather than a subset.
