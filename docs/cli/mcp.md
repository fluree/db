# fluree mcp

Model Context Protocol (MCP) server for IDE agent integration.

One `fluree` MCP server exposes a selectable set of **toolsets** over a single
stdio transport, instead of one server per feature:

| Toolset | Tools | Notes |
|---------|-------|-------|
| `memory` | `memory_add`, `memory_recall`, `memory_update`, `memory_forget`, `memory_status`, `kg_query` | Developer-memory store; created lazily on first use |
| `docs` | `docs_search`, `docs_get`, `docs_examples`, `docs_tree` | Embedded, version-pinned documentation; stateless |

## Usage

```bash
fluree mcp <COMMAND>
```

## Subcommands

| Command | Description |
|---------|-------------|
| `init` | Register the Fluree MCP server (with selected toolsets) in an IDE's config |
| `serve` | Start the MCP server (spawned by the IDE) |
| `status` | Show which toolsets are installed for each detected IDE |

## fluree mcp init

Write an IDE's MCP config so its agent spawns `fluree mcp serve` on demand.
Needs no `.fluree/` directory — the server lazy-inits its store and the docs
toolset is stateless.

```bash
fluree mcp init [--ide <IDE>] [--toolsets <TOOLSETS>]
```

### Options

| Option | Description |
|--------|-------------|
| `--ide <IDE>` | Target IDE: `claude-code`, `vscode`, `cursor`, `windsurf`, `zed` (auto-detected if omitted) |
| `--toolsets <TOOLSETS>` | Which toolset(s) to enable: `memory`, `docs`, a comma-separated list, or `all` (default) |

```bash
# Register both toolsets with the auto-detected IDE
fluree mcp init

# Docs only (handy in a project that doesn't use Fluree Memory)
fluree mcp init --ide cursor --toolsets docs
```

`init` writes a single MCP entry named `fluree` whose `--toolsets` arg records
the selection, and removes any legacy per-feature `fluree-memory` / `fluree-docs`
entries from a previous install. After it runs, **reload your editor** to
activate the tools.

> The store is created lazily on first tool call, so there is no separate
> initialization step. To git-track repo memories, commit `.fluree-memory/`
> after first use.

## fluree mcp serve

Start the MCP server. Normally the IDE spawns this; run it by hand only to
debug.

```bash
fluree mcp serve [--transport <TRANSPORT>] [--toolsets <TOOLSETS>]
```

### Options

| Option | Description |
|--------|-------------|
| `--transport <TRANSPORT>` | Transport protocol: `stdio` (default) |
| `--toolsets <TOOLSETS>` | Which toolset(s) to expose. Defaults to `memory` for back-compat when omitted; `init` always writes an explicit `--toolsets`. |

The `stdio` transport reads JSON-RPC requests from stdin and writes responses to
stdout. CLI tracing is disabled (or written to `.fluree-memory/.local/mcp.log`
when the memory toolset is active) so nothing pollutes the JSON-RPC stream on
stdout/stderr.

A docs-only server (`--toolsets docs`) is fully stateless and requires no
`.fluree/` directory.

### Manual IDE configuration

`fluree mcp init` writes this for you, but to add it by hand:

```json
{
  "mcpServers": {
    "fluree": {
      "type": "stdio",
      "command": "/path/to/fluree",
      "args": ["mcp", "serve", "--transport", "stdio", "--toolsets", "memory,docs"],
      "env": {
        "FLUREE_HOME": "${workspaceFolder}/.fluree"
      }
    }
  }
}
```

The `env.FLUREE_HOME` is only needed when the `memory` toolset is enabled (it
pins the store to the workspace). For the per-IDE config file paths and keys,
see [Memory: IDE support matrix](../memory/reference/ide-matrix.md).

### Testing with JSON-RPC

```bash
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke","version":"0.0"}}}' \
  '{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' \
  | fluree mcp serve --transport stdio --toolsets all
```

## fluree mcp status

Report, per detected IDE, whether the `fluree` server is installed and which
toolsets it exposes.

```bash
fluree mcp status
```

## Back-compat

These older commands still work as hidden aliases:

| Old | Now equivalent to |
|-----|-------------------|
| `fluree mcp install [--ide …] [--toolsets …]` | `fluree mcp init …` |
| `fluree memory mcp-install [--ide …]` | `fluree mcp init --toolsets memory` |
| `fluree memory init` | `fluree mcp init --toolsets memory` (store is no longer created up front) |

## See Also

- [docs](docs.md) — the human `fluree docs` CLI over the same corpus the `docs` toolset serves
- [memory](memory.md) — CLI commands for memory management
- [Memory: MCP server](../memory/concepts/mcp.md) — what the memory tools expose and how agents use them
- [Memory IDE support matrix](../memory/reference/ide-matrix.md) — config paths and supported features per IDE
