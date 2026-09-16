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

There are deliberately **no per-endpoint capability settings** — nothing to declare about which JSON mode, token-limit field or temperature a model takes. See [What is sent on the wire](#what-is-sent-on-the-wire).

An absent `[doc]` table means unconfigured: the pipeline runs deterministic and offline. A present but malformed one is an error.

### What is sent on the wire

A generation request carries the model, the messages and an output budget, and nothing else:

```json
{ "model": "gpt-5-mini",
  "messages": [ … ],
  "max_completion_tokens": 8000 }
```

That is the whole body, and the omissions are the point. Every optional field is a field some current model refuses:

| Not sent | Why |
|---|---|
| `temperature` | gpt-5 and the o-series reject any value but their own default. `0` — what this used to send for determinism — is already the default on every endpoint that accepts it, so the field bought nothing where it worked and broke the call where it did not. |
| `response_format` | `json_object` mode only guarantees syntactic JSON, not a schema. The prompt already asks for JSON and the parser already tolerates fences and prose around it. Anthropic's OpenAI-compatible route returns a 400 on the field today, against its own published table saying it is ignored. |
| `max_tokens` | The deprecated spelling of the budget, and the one gpt-5 and the o-series reject. `max_completion_tokens` is accepted across the Chat Completions range. |

**If an endpoint refuses something anyway, it is asked again.** A 400 naming a field is the endpoint describing its own dialect, which is better evidence than any table shipped in a binary: a server too old to know `max_completion_tokens` says so, and the request is retried once with `max_tokens` instead. Only known refusals are handled — an unrecognised 400 is reported to you verbatim rather than silently rewritten.

Nothing here is configurable, on purpose. A per-endpoint capability setting is only right until you repoint `url` at a different server, has to be re-derived by hand each time the endpoint or model changes, and can only be as accurate as the vendor table someone transcribed it from — which, in the Anthropic case above, is wrong today.

The gateway route (`api = "responses"`) has always sent this minimal shape and is unchanged.

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
