# Models and calls

## Where the data lives

`fluree doc ingest` always writes to a **local ledger** under the project's `.fluree/storage`, creating it if needed. Fluree AI is never the write target of an ingest; it supplies models. The two indexes are local graph sources next to the ledger, and the caches are local files.

## The three slots

Model access is configured in the `[doc]` table of the project's `config.toml`, by `fluree config set` or by hand:

```toml
[doc.embedding]
url = "http://localhost:11434/v1"      # any OpenAI-compatible base URL, up to /v1
model = "nomic-embed-text"
# api_key = "$OPENAI_API_KEY"           # a $NAME value reads that environment variable
# dimensions = 768                      # for models that accept a dimensions parameter

[doc.vlm]                               # reads document crops; falls back to [doc.llm]
url = "https://api.openai.com/v1"
model = "gpt-5-mini"
api_key = "$OPENAI_API_KEY"
# api = "chat"                          # chat (default) or responses

[doc.llm]                               # entity and relation extraction
url = "https://api.openai.com/v1"
model = "gpt-5-mini"
api_key = "$OPENAI_API_KEY"
```

`api` selects the wire shape for generation calls: `chat` is `/chat/completions` with `image_url` parts, what OpenAI, Ollama, vLLM and LM Studio serve; `responses` is the Responses API with `input_image` parts, what the Fluree AI gateway serves. Embeddings use `/embeddings` either way. Each field can be overridden by `FLUREE_DOC_{EMBEDDING,LLM,VLM}_{URL,MODEL,API_KEY,DIMENSIONS,API}`.

With `doc.remote = "<remote>"`, any slot not set gets the remote's gateway URL, the stored login as its bearer token, `api = "responses"`, and a model of `auto` for `vlm` and `llm` (the gateway picks by intent) or `text-embedding-3-small` for embeddings.

## Which calls are made

Every model interaction is a separate call. There are three kinds, and they are independent: the vision read and the extraction are distinct calls with distinct intents and may be different models.

| Step | Call | Slot | When |
|---|---|---|---|
| Read a crop | `POST …/responses` (or `…/chat/completions`) with the crop image and a transcription prompt, intent `doc-parse` | `vlm`, else `llm` | Per crop, only for pages and regions the deterministic pass flagged. Answered from the reading cache when the pixels were seen before. |
| Embed chunks | `POST …/embeddings` | `embedding` | Per document, in batches. |
| Extract entities and relations | `POST …/responses`, intent `extraction` | `llm` | Per chunk, when extraction is enabled. In progress. |

Against a Fluree AI gateway, the first and third go to the same route and the gateway routes each intent to the account's provider for it. Against your own endpoints, `vlm` and `llm` are simply two configurations, which may name the same model.

## What leaves the machine

- For a crop read: a PNG of the page or region and a short prompt. Only pages the parser could not read are cropped; a document that reads deterministically sends nothing.
- For embeddings: each chunk's text with its section path prefixed.
- For extraction: each chunk's text plus the ontology and known entities you supplied.

Never the file itself, never the ledger. The parse cache and reading cache are local files under `.fluree/cache/doc/`.

## Costs to expect

The deterministic parse is milliseconds per page. A vision read is seconds per crop, so `--max-crops` (default 70) caps what one document may ask for; a document over the cap lands with the deterministic tier only and says so in the output. Embeddings are cheap per chunk, but a long report is hundreds of chunks.
