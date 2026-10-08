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
| A vector search feels slow on a large corpus | `--mode vector` scores every chunk exactly. That is fast for a folder of documents and linear past it; `--mode text` stays indexed at any size, and an approximate index is a [server capability](../concepts/vector-search.md). |
