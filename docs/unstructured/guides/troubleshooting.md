# Troubleshooting

| Message | Meaning |
|---|---|
| `escalation none — deterministic tier only` | No `vlm` or `llm` slot and no `doc.remote`. Scanned pages and unreadable regions stay unread. |
| `A page reported as unread` | The parser found pixels it could not read and no vision model was available, or the reading did not complete. |
| `deterministic tier only: N crop(s) … past the cap` | The document asks the vision model for more crops than `--max-crops`. It landed without them; raise the cap deliberately, because each crop is a model call. |
| `doc.remote 'acct': authentication failed (401)` | The stored login has fully expired. `fluree auth login --remote acct`. |
| `doc.remote 'acct': no such remote` | Register it with `fluree remote add`, in the project or from your home directory. |
| `NoEmbeddingProvider` from the gateway | The stack has neither an OpenAI-type provider key nor a proxy connection serving embeddings. Configure one in the stack's settings, or point `doc.embedding` at an endpoint of your own. |
| `[doc] section: …` at startup | The `[doc]` table in `config.toml` is malformed. A present but broken section is an error rather than a silent fallback, because running without the model you configured would look like success. |
| `an image has no text layer; configure [doc.vlm]` | A PNG or JPEG can only be read by a vision model. |
| `Vector index … does not support time-travel queries` | `--at` on a vector search asks for a state older than the index. The vector index is head-only; full-text and plain queries time-travel. |
