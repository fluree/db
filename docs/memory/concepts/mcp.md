# MCP server

Fluree Memory exposes its functionality over [Model Context Protocol](https://modelcontextprotocol.io) so AI coding agents can use it natively. The MCP server is bundled with the `fluree` CLI — no separate install.

## Start it manually

```bash
fluree mcp serve --transport stdio
```

In practice you never start it manually — your IDE launches it. `fluree memory mcp-install` writes the IDE-specific config that does the spawning. See [mcp-install](../cli/mcp-install.md) for the per-IDE details.

## Tools exposed

The server exposes these tools to the agent:

### `memory_recall`

Search for relevant memories.

```json
{
  "name": "memory_recall",
  "arguments": {
    "query": "how do I run tests",
    "limit": 5,
    "offset": 0,
    "kind": "fact",
    "tags": ["testing"],
    "scope": "repo"
  }
}
```

Returns XML context-formatted output (see [Recall and ranking](recall-and-ranking.md)).

### `memory_add`

Store a new memory. The content field is named `content` (not `text`).

```json
{
  "name": "memory_add",
  "arguments": {
    "kind": "fact",
    "content": "Tests use cargo nextest, not cargo test",
    "tags": ["testing"],
    "scope": "repo"
  }
}
```

Other optional arguments: `refs`, `severity`, `rationale`, `alternatives`. Returns the new memory ID.

### `memory_update`

Patch an existing memory in place. The memory keeps its ID; only the fields you pass are changed. Use `content` (not `text`) for the new body.

```json
{
  "name": "memory_update",
  "arguments": {
    "id": "mem:fact-01JDXYZ...",
    "content": "Tests use cargo nextest with --no-fail-fast"
  }
}
```

Also accepts `tags`, `refs`, `rationale`, `alternatives`.

### `memory_forget`

Retract a memory permanently.

```json
{
  "name": "memory_forget",
  "arguments": { "id": "mem:fact-01JDXYZ..." }
}
```

### `memory_status`

Return a summary of the store — totals by kind and a preview of recent memories. Agents are encouraged to call this first to discover what topics to query.

### `kg_query`

Run a raw SPARQL SELECT against the `__memory` ledger. Advanced escape hatch — prefer `memory_recall` for ranked search.

```json
{
  "name": "kg_query",
  "arguments": {
    "query": "PREFIX mem: <https://ns.flur.ee/memory#> SELECT ?id ?content WHERE { ?id a mem:Constraint ; mem:content ?content } LIMIT 20"
  }
}
```

## Where the store lives

When the MCP server starts, it picks its Fluree directory the same way the CLI does:

1. If `$FLUREE_HOME` is set, that directory is used (unified mode).
2. Otherwise it walks up from the spawn CWD looking for an existing `.fluree/`.
3. If neither is found, it falls back to the platform's global config/data directories.

When the server is in unified mode (cases 1 and 2), the memory store lives in `<dir>/../.fluree-memory/` and is shared with the CLI. In global mode, file-based sync is disabled and memories live only in the global ledger.

This matters for IDE integrations: the **Cursor** config that `mcp-install` writes explicitly sets `FLUREE_HOME=${workspaceFolder}/.fluree` so memory stays scoped to the current repo regardless of Cursor's CWD. The other supported IDEs (Claude Code, VS Code, Windsurf, Zed) rely on the spawn CWD plus the walk-up behavior — which normally works, but can land in a global store if the IDE spawns the MCP server from outside the repo. If you see that, set `FLUREE_HOME` manually in the MCP config or re-run `mcp-install` from inside the repo root.

## The rules file

Alongside the MCP server, `mcp-install` writes (or appends to) a short rules file for IDEs that support one:

| IDE | Rules file |
|---|---|
| Claude Code | Short section appended to `<repo>/CLAUDE.md` |
| Cursor | `<repo>/.cursor/rules/fluree_rules.md` |
| VS Code | `<repo>/.vscode/fluree_rules.md` |
| Windsurf, Zed | None written — you can add your own guidance manually |

The file tells the agent *when* to reach for memory tools — e.g. at the start of a task (`memory_recall` first), after capturing something reusable (`memory_add`), and not to re-ask the user for things already memorized. You can edit it to customize the agent's instincts; see [Customizing the rules file](../guides/rules-file.md).
