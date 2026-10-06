# Configuration

All settings live in the `[doc]` table of the project's `.fluree/config.toml`, set with `fluree config set doc.<key> <value>` or edited by hand.

```toml
[doc]
remote = "acct"                         # a CLI remote whose gateway and login fill unset slots

[doc.embedding]
url = "http://localhost:11434/v1"
model = "nomic-embed-text"
api_key = "$OPENAI_API_KEY"             # optional; $NAME reads the environment
dimensions = 768                        # optional; for models that accept it

[doc.vlm]                               # optional; falls back to [doc.llm]
url = "https://api.openai.com/v1"
model = "gpt-5-mini"
api_key = "$OPENAI_API_KEY"
api = "chat"                            # chat (default) | responses

[doc.llm]                               # optional
url = "https://api.openai.com/v1"
model = "gpt-5-mini"
api_key = "$OPENAI_API_KEY"
api = "chat"
```

| Key | Meaning |
|---|---|
| `remote` | Name of a remote registered with `fluree remote add`, looked up in the project config, then `~/.fluree/config.toml`, then the platform config directory. Its URL (with `/fluree` stripped) becomes the gateway base and its stored login the bearer token for every slot not set explicitly. The login is refreshed before use. |
| `<slot>.url` | OpenAI-compatible base URL, up to and including `/v1`. |
| `<slot>.model` | Model name, passed through unchanged. `auto` on a Responses-API gateway lets it choose by intent. |
| `<slot>.api_key` | Bearer token. A value starting with `$` names an environment variable holding it. |
| `<slot>.dimensions` | Embedding width to request, for models that accept `dimensions`. |
| `<slot>.api` | `chat` for `/chat/completions`, `responses` for `/responses`. Embeddings always use `/embeddings`. |

Environment variables override the file per field: `FLUREE_DOC_{EMBEDDING,LLM,VLM}_{URL,MODEL,API_KEY,DIMENSIONS,API}`.

There are deliberately **no per-endpoint capability settings** — nothing to declare about which JSON mode, token-limit field or temperature a model takes. See [What is sent on the wire, and what gets withdrawn](#what-is-sent-on-the-wire-and-what-gets-withdrawn).

An absent `[doc]` table means unconfigured: the pipeline runs deterministic and offline. A present but malformed one is an error.

### What is sent on the wire, and what gets withdrawn

A generation request carries the model, the messages, a sampling temperature, a JSON-mode request and an output budget:

```json
{ "model": "gpt-5-mini",
  "messages": [ … ],
  "temperature": 0,
  "response_format": { "type": "json_object" },
  "max_completion_tokens": 8000 }
```

Three of those are fields that *some* current model refuses, and they are sent optimistically rather than withheld:

| Field | Why it is sent | Who refuses it |
|---|---|---|
| `temperature: 0` | Extraction is transcription, not composition. Near-greedy sampling is what makes a re-run over the same corpus produce the same graph, which the extraction cache and `doc:extractionFingerprint` both assume. | gpt-5 and the o-series accept only their own default. |
| `response_format` | `json_object` genuinely improves how reliably a model returns parseable JSON. | Anthropic's OpenAI-compatible route, today. |
| `max_completion_tokens` | The spelling Chat Completions takes across its current range. | Servers predating it, which want `max_tokens`. |

**A refusal is the endpoint describing its own dialect, and it is taken at its word.** When a request comes back 400 naming one of those fields, that field is withdrawn — or, for the budget, renamed to `max_tokens` — and the request is resent immediately. No backoff, and it does not consume one of the three retries that exist for transient failures. **The refusal is then remembered for the rest of the run**, so an endpoint that rejects a field costs one extra round trip in total rather than one per chunk. The run reports how many calls were adjusted.

Only the three fields above are handled this way. Any other 400 is reported to you verbatim rather than silently rewritten.

**None of this is configurable, on purpose.** A per-endpoint capability setting is only right until you repoint `url` at a different server, has to be re-derived by hand whenever the endpoint or model changes, and can only be as accurate as the vendor table someone transcribed it from — which in the Anthropic case above is wrong today: its own compatibility page documents `response_format` as ignored while the deployed route rejects it. Asking the endpoint is more reliable than asking the documentation.

Determinism is worth one caveat: `temperature: 0` makes extraction near-greedy, not reproducible. No `seed` is sent and no provider guarantees identical output. On an endpoint that refuses the field you lose even that, and two cold runs over one corpus can differ while the fingerprint stays the same.

The gateway route (`api = "responses"`) carries none of these fields, and never has.

## Extraction

```toml
[doc.extraction]
guidance = "prompts/guidance.md"          # priorities placed in the extraction prompt
# system_prompt = "prompts/system.txt"    # replaces the system prompt; keep {model} and {guidance}
# user_prompt = "prompts/user.txt"        # replaces the user prompt; keep {existing} and {document}
# concurrency = 4                         # chunks sent to the language model at once
# drop_off_model = false                  # drop new entities typed outside the ontology
```

Paths are relative to the project. Each value has a flag of the same name on `fluree doc ingest` that overrides it for one run. See [Entities and relations](../concepts/entities-and-relations.md#guidance-and-custom-prompts).

## Defaults supplied by `remote`

| Slot | Model | API |
|---|---|---|
| `embedding` | `text-embedding-3-small` | embeddings |
| `vlm` | `auto` | `responses` |
| `llm` | `auto` | `responses` |

## Caches

`.fluree/cache/doc/parse/` holds parses keyed on content hash and settings; `.fluree/cache/doc/readings/` holds vision-model readings keyed on crop pixels, prompt and model. Both are safe to delete.
