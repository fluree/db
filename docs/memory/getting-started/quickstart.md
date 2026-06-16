# Quickstart

2 minutes to your first memory.

## 1. Initialize the memory store

From the root of a project you'd like to give memory to:

```bash
cd my-project
fluree memory init
```

This creates:

- `.fluree-memory/repo.ttl` — **team memories**, meant to be committed to git
- `.fluree-memory/.local/user.ttl` — **your personal memories**, gitignored
- `.fluree-memory/.gitignore` — pre-configured to ignore `.local/` (which holds your user scope plus the MCP log)
- The `__memory` ledger inside your project's `.fluree/` store

`init` is idempotent; running it again is safe.

It will also detect any installed AI coding tools (Claude Code, Cursor, VS Code, Windsurf, Zed) and offer to wire up MCP. You can say no here and run [`fluree memory mcp-install`](../cli/mcp-install.md) later.

## 2. Add a memory

```bash
fluree memory add --kind fact \
  --text "Tests use cargo nextest, not cargo test" \
  --tags testing
```

Output:

```
Stored memory: mem:fact-01JDXYZ5A2B3C4D5E6F7G8H9J0
```

The ID is a ULID — sortable by creation time and unique across the store.

## 3. Recall it

```bash
fluree memory recall "how do I run tests"
```

Output:

```
Recall: "how do I run tests" (1 match)

1. [score: 13.0] mem:fact-01JDXYZ5A2B3C4D5E6F7G8H9J0
   Tests use cargo nextest, not cargo test
   Tags: testing
```

Recall is BM25-ranked over the memory content and tags. No embeddings, no network — fast and deterministic.

## 4. Check status

```bash
fluree memory status
```

```
Memory Store Status
  Total memories: 1
  Total tags:     1
  By kind:
    fact: 1
```

## That's the loop

Add memories as you learn things. Recall them when you need them. Commit `.fluree-memory/repo.ttl` to share team knowledge.

## Next

- **Wire to your AI tool** — [Claude Code](claude-code.md), [Cursor](cursor.md), or others — so the agent does this for you.
- **Learn the memory kinds** — [What is a memory?](../concepts/what-is-a-memory.md)
- **Understand scope** — [Repo vs user memory](../concepts/repo-vs-user.md)
