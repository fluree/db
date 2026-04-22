# fluree memory update

Update an existing memory in place. The memory keeps the same ID — only the changed fields are modified. History is tracked via git.

```bash
fluree memory update <ID> [OPTIONS]
```

## Options

| Option | Description |
|---|---|
| `--text <TEXT>` | New content text |
| `--tags <T1,T2>` | New tags (replaces all existing) |
| `--refs <R1,R2>` | New artifact refs (replaces all existing) |
| `--format <FMT>` | `text` (default) or `json` |

## Example

```bash
fluree memory update mem:fact-01JDXYZ... \
  --text "Tests use cargo nextest with --no-fail-fast"
```

Output:

```
Updated: mem:fact-01JDXYZ...
```

The TTL file is rewritten with the updated content. Use `git diff` to see what changed, or `git log -p .fluree-memory/repo.ttl` to review the full history.

## See also

- [`forget`](forget.md) — retract instead of update
- [Updates and forgetting](../concepts/supersession.md) — the update model
