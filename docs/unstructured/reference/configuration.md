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

An absent `[doc]` table means unconfigured: the pipeline runs deterministic and offline. A present but malformed one is an error.

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
