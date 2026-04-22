# CLI reference

The `fluree memory` subcommands, alphabetically-ish.

| Command | Purpose |
|---|---|
| [`init`](init.md) | Create the memory store and optionally configure MCP for detected AI tools |
| [`add`](add.md) | Store a new memory |
| [`recall`](recall.md) | Search and rank relevant memories |
| [`update`](update.md) | Update an existing memory in place |
| [`forget`](forget.md) | Retract a memory permanently |
| [`status`](status.md) | Summary of the store (totals, tags, kinds) |
| [`export` / `import`](export-import.md) | Round-trip memories as JSON |
| [`mcp-install`](mcp-install.md) | Install MCP config for an IDE |

Several subcommands take a `--format` flag (`text` for humans, `json` for scripts, and `context` on `recall` for XML intended for LLM injection). The default is always `text`.

## The common options

A few flags show up across many subcommands:

| Flag | Default | Where |
|---|---|---|
| `--scope <repo\|user>` | `repo` | `add`; filter on `recall` |
| `--tags <t1,t2>` | none | `add`, `update`; filter on `recall` |
| `--kind <kind>` | `fact` on `add` | `add`; filter on `recall` |
| `--format <text\|json\|context>` | `text` | `add`, `recall`, `update` |

See [What is a memory?](../concepts/what-is-a-memory.md) for the kind taxonomy.

## Environment

| Variable | Effect |
|---|---|
| `FLUREE_HOME` | When set, the CLI and MCP server use this path as the unified Fluree directory. If unset, both walk up from CWD looking for an existing `.fluree/`; if none is found, they fall back to a platform-global config/data directory. |

Set `FLUREE_HOME=<repo>/.fluree` if you need to force repo-scoped operation from a shell that starts elsewhere. Among the IDE integrations, only the Cursor MCP config sets this automatically via `${workspaceFolder}`; the others rely on the walk-up behavior from spawn CWD.
