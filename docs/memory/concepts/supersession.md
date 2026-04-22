# Updates and forgetting

Memories are updated **in place**. When you `update` a memory, the same ID is kept and only the changed fields are modified. History is tracked via git, not via internal versioning.

## `update` modifies in place

```bash
fluree memory update mem:fact-01JDXYZ... --text "Tests use cargo nextest with --no-fail-fast"
```

Output:

```
Updated: mem:fact-01JDXYZ...
```

The memory keeps its original ID. The TTL file is rewritten with the new content, and git records what changed:

```bash
git diff .fluree-memory/repo.ttl
```

```diff
 mem:fact-01JDXYZ a mem:Fact ;
-    mem:content "Tests use cargo nextest" ;
+    mem:content "Tests use cargo nextest with --no-fail-fast" ;
     mem:tag "cargo" ;
```

## `forget` retracts

`forget` is different from `update`. It **retracts** the memory's triples — the memory stops existing entirely.

```bash
fluree memory forget mem:fact-01JDXYZ...
```

```
Forgotten: mem:fact-01JDXYZ...
```

Rule of thumb:

| You think... | Use |
|---|---|
| "This was wrong from the start" | `forget` |
| "This was right but the world changed" | `update` |
| "I never want anyone to see this again" | `forget` |

## History via git

Both `update` and `forget` rewrite the TTL file, and git tracks the full history. To see how a memory evolved:

```bash
git log -p .fluree-memory/repo.ttl
```

This shows every change — what was added, updated, or forgotten, and when.

## Time-travel over memory history

If you want to query memory history with Fluree's time-travel capabilities, you can import your git-tracked memory history into a Fluree ledger:

```bash
fluree create my-memory-ledger --memory
```

This replays each git commit to `.fluree-memory/repo.ttl` as a Fluree transaction, giving you a full time-travel-capable ledger over your memory history. Use `--no-user` to exclude `user.ttl` from the import.
