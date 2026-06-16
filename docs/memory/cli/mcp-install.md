# fluree mcp install

Install MCP configuration for an IDE so its agent can use Fluree's MCP servers.
By default this registers **two** servers in one step:

- **`fluree-memory`** — the developer-memory tools (`fluree mcp serve`)
- **`fluree-docs`** — version-pinned documentation lookup (`fluree docs serve`,
  see [fluree docs](../../cli/docs.md))

```bash
fluree mcp install [--ide <IDE>] [--server <SERVER>]
```

> `fluree memory mcp-install` is a back-compat alias for this command (it always
> installs both servers).

## Options

| Option | Description |
|---|---|
| `--ide <IDE>` | Target IDE (auto-detected if omitted) |
| `--server <SERVER>` | Which server(s) to register: `memory`, `docs`, or `all` (default) |

Use `--server docs` to register only the documentation server — it's stateless,
so it works in any project, including ones that don't use Fluree (no `.fluree/`
directory required).

## Supported IDEs

| Value | Config written | Extras |
|---|---|---|
| `claude-code` | `claude mcp add` → `~/.claude.json` (local scope) | Appends to `CLAUDE.md` |
| `vscode` | `<repo>/.vscode/mcp.json` (key: `servers`) | `.vscode/fluree_rules.md` |
| `cursor` | `<repo>/.cursor/mcp.json` (key: `mcpServers`) | `.cursor/rules/fluree_rules.md` |
| `windsurf` | `~/.codeium/windsurf/mcp_config.json` (global) | — |
| `zed` | `<repo>/.zed/settings.json` (key: `context_servers`) | Skips if JSONC detected |

If an existing target file isn't valid JSON (JSONC with comments, hand-edited corruption, etc.), `mcp-install` refuses to overwrite it and prints a snippet you can paste in manually. This applies to all per-repo configs above, plus Windsurf's global file.

Legacy aliases: `claude-vscode` and `github-copilot` both map to `vscode`.

When `--ide` is omitted, the first **unconfigured** detected tool is used; defaults to `claude-code` if nothing's detected.

## Example

```bash
fluree mcp install --ide cursor
```

Output:

```
  Installed: .cursor/mcp.json (fluree-memory, fluree-docs)
  Installed: .cursor/rules/fluree_rules.md
```

## Per-IDE config shape

The JSON `mcp-install` writes differs per IDE:

Both servers are written together. Only the `fluree-memory` entry takes a
`FLUREE_HOME` env (it has a per-repo store); `fluree-docs` is stateless (the
corpus is embedded), so it never needs one.

**Cursor** (`.cursor/mcp.json`) is the only target that sets `FLUREE_HOME` by default. It uses `${workspaceFolder}` interpolation to pin the memory store to the current workspace regardless of where Cursor spawns the process from:

```json
{
  "mcpServers": {
    "fluree-memory": {
      "type": "stdio",
      "command": "fluree",
      "args": ["mcp", "serve", "--transport", "stdio"],
      "env": { "FLUREE_HOME": "${workspaceFolder}/.fluree" }
    },
    "fluree-docs": {
      "type": "stdio",
      "command": "fluree",
      "args": ["docs", "serve", "--transport", "stdio"]
    }
  }
}
```

**VS Code, Windsurf, Zed, Claude Code** get simpler entries with no `env` — for example:

```json
{
  "fluree-memory": { "command": "fluree", "args": ["mcp", "serve", "--transport", "stdio"] },
  "fluree-docs":   { "command": "fluree", "args": ["docs", "serve", "--transport", "stdio"] }
}
```

(The top-level wrapper key differs — `servers` for VS Code, `mcpServers` for Windsurf, `context_servers` for Zed. Claude Code's entries are registered globally via `claude mcp add`.)

These rely on the MCP server's walk-up behavior: on start, it looks for `.fluree/` beginning at its spawn CWD. That's usually the workspace, but if the IDE starts it elsewhere memory may land in a global store. See the troubleshooting section below.

## Troubleshooting: repo vs global memory

**Repo-scoped (the goal):**

- Memories: `<repo>/.fluree-memory/repo.ttl`
- MCP log: `<repo>/.fluree-memory/.local/mcp.log` (truncated on each server start — tail it while reproducing the issue)

**Global (something's wrong):**

- Memories under the platform default, e.g. `~/Library/Application Support/fluree/` on macOS
- **Fix:** add an explicit absolute `FLUREE_HOME` to the MCP config entry, pointing at your repo's `.fluree/`, and fully restart (not just reload) the IDE. For Cursor, the `${workspaceFolder}`-based default should already be in place — re-run `mcp-install` from inside the repo if it's missing.

## See also

- [Concepts: MCP server](../concepts/mcp.md) — what tools are exposed
- [Getting started: Claude Code / Cursor / VS Code / Windsurf / Zed](../getting-started/README.md) — per-IDE walkthroughs
