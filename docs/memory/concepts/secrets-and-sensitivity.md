# Secrets and sensitivity

Memory is meant to be written freely and committed to git. That only works if secrets never land in there.

## Automatic redaction

Every `memory_add` / `fluree memory add` runs the input through a secret detector before storage. If the content matches patterns for API keys, passwords, tokens, or connection strings, the sensitive substrings are replaced with `[REDACTED]` and a warning is printed:

```
  warning: secrets detected in content — storing redacted version.
  Original content contained sensitive data that was replaced with [REDACTED].
Stored memory: mem:fact-01JDXYZ...
```

Patterns covered include:

- AWS access key IDs (`AKIA…`)
- GitHub personal access tokens (`ghp_…`, `gho_…`, `ghu_…`, `ghs_…`, `ghr_…`)
- OpenAI keys (`sk-…`) and Anthropic keys (`sk-ant-…`)
- Fluree API keys (`flk_…`)
- Generic `api_key=…` / `apikey: …` assignments
- `password=…` / `passwd: …` assignments
- Connection strings with inline credentials (`postgres://`, `mysql://`, `mongodb://`, `redis://`, `amqp://` containing `user:pass@host`)
- PEM private keys (`-----BEGIN … PRIVATE KEY-----`)
- Bearer tokens (`Bearer eyJ…`)
- JWT tokens (three base64 segments separated by dots)

Redaction preserves enough context that the memory still makes sense (e.g. "Use the API key `[REDACTED]` from 1Password") while the actual value never reaches the TTL file.

The detector is pattern-based, not entropy-based — well-disguised secrets outside these patterns can still slip through. Treat redaction as a safety net, not a guarantee.

## Scope as the privacy boundary

Memory visibility is controlled by `scope` (`repo` or `user`), not by a separate sensitivity level. Repo-scoped memories live in `.fluree-memory/repo.ttl` and are committed to git, so they're visible to anyone with repo access. User-scoped memories live in `.fluree-memory/.local/user.ttl`, which is gitignored.

If something is client-specific or team-internal, put it in `user` scope or use a private sub-repo. The scope mechanism plus secret-detection on ingest handles what a separate sensitivity field used to.

## What if I slip?

If something slipped past the detector and into `repo.ttl` before you noticed:

1. `fluree memory forget <id>` — retracts the memory.
2. Run `git log -p .fluree-memory/repo.ttl` and use `git filter-repo` (or the BFG) to scrub the history if the value leaked there too.
3. Rotate the credential at the source. Redaction in memory doesn't rotate keys.

Treat this the same way you'd treat accidentally committing `.env` — the git history is the hard part, the file is the easy part.
