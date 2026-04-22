# fluree memory export / import

Round-trip memories as JSON.

## export

Write all memories to stdout as a JSON array.

```bash
fluree memory export > memories.json
```

`export` takes no options — it emits every memory, both scopes included. To get a single scope, filter with `jq` or use `recall` with `--scope` and a permissive limit.

Output is a flat array of full memory objects:

```json
[
  {
    "id": "mem:fact-01JDXYZ...",
    "kind": "fact",
    "content": "Tests use cargo nextest",
    "tags": ["testing", "cargo"],
    "scope": "repo",
    "severity": null,
    "artifact_refs": [],
    "branch": "main",
    "created_at": "2026-02-22T14:00:00Z"
  }
]
```

## import

Load memories from a JSON file produced by `export` (or a hand-crafted array of the same shape).

```bash
fluree memory import memories.json
```

Import is additive — every entry in the file is re-transacted into the ledger, with secret-detection applied to `content`, `rationale`, and `alternatives`. IDs and timestamps from the source file are preserved. There is no dedup step, so importing the same file twice will double-insert; `forget` the existing entries first (or import into a freshly-initialized store) if that's not what you want.

## When to use

- **Backup / portability** — export before a risky refactor.
- **Bootstrapping a new repo** from another project's knowledge.
- **Sharing a slice** of memory out-of-band (e.g. into an issue or wiki).

For normal team sharing, you don't need export/import — `.fluree-memory/repo.ttl` is committed to git and everyone who clones + runs `fluree memory init` picks it up automatically. See [Repo vs user memory](../concepts/repo-vs-user.md).
