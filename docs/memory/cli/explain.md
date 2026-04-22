# Memory history via git

The `fluree memory explain` command has been removed. Memory history is now tracked via git.

## Viewing history

Since updates modify memories in place and the TTL file is rewritten on each change, `git log` shows the full history:

```bash
# Full history of all memory changes
git log -p .fluree-memory/repo.ttl

# Search for changes to a specific memory ID
git log -p -S "mem:fact-01JDXYZ" .fluree-memory/repo.ttl

# Compact one-line summary
git log --oneline .fluree-memory/repo.ttl
```

## Time-travel via Fluree

For richer querying over memory history, import your git history into a Fluree ledger:

```bash
fluree create my-memory-ledger --memory
```

Each git commit becomes a Fluree transaction, enabling time-travel queries over the full evolution of your project's memory.

See [Updates and forgetting](../concepts/supersession.md) for the update model.
