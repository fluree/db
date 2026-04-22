# IDE support matrix

Where each supported AI coding tool stores its MCP config and its rules file, and whether the config is scoped per-repo or global.

| IDE | MCP config | Config scope | `FLUREE_HOME` set? | Rules file | `mcp-install` value |
|---|---|---|---|---|---|
| Claude Code | `~/.claude.json` (via `claude mcp add`) | user (local) | no | section appended to `<repo>/CLAUDE.md` | `claude-code` |
| Cursor | `<repo>/.cursor/mcp.json` | **repo** | yes — `${workspaceFolder}/.fluree` | `<repo>/.cursor/rules/fluree_rules.md` | `cursor` |
| VS Code (Copilot) | `<repo>/.vscode/mcp.json` | **repo** | no | `<repo>/.vscode/fluree_rules.md` | `vscode` |
| Windsurf | `~/.codeium/windsurf/mcp_config.json` | global | no | none | `windsurf` |
| Zed | `<repo>/.zed/settings.json` | **repo** | no | none (skipped if JSONC) | `zed` |

Legacy aliases:
- `claude-vscode` → `vscode`
- `github-copilot` → `vscode`

## `FLUREE_HOME` and repo scoping

Only the Cursor config sets `FLUREE_HOME` automatically. For the other IDEs, the MCP server figures out which repo it's serving by walking up from its spawn CWD until it finds a `.fluree/` directory. In normal use the IDE spawns the server from the workspace root, so this works without extra configuration.

If memory ends up in a platform-global store instead of `<repo>/.fluree-memory/`, the fix is to add `FLUREE_HOME` manually to the relevant MCP config, pointing at an absolute path (or a variable the IDE interpolates — Cursor supports `${workspaceFolder}`; other IDEs' support varies). Then restart the IDE.

## Known gotchas

- **Zed + JSONC**: If `.zed/settings.json` contains `//` comments, `mcp-install` refuses to write to avoid corrupting your settings. Paste the snippet yourself or strip comments first.
- **Windsurf globals**: Windsurf's MCP config is user-global, not per-repo. If you work across multiple repos, you likely need to leave `FLUREE_HOME` unset and rely on walk-up — or switch the env var per project manually.
- **Cursor restarts**: Cursor caches MCP servers aggressively. If a change to `.cursor/mcp.json` doesn't take effect, fully quit Cursor (Cmd-Q on macOS) rather than just reloading the window.
- **Claude Code CLAUDE.md**: The rules section is appended at the end of `CLAUDE.md` (only if one doesn't already mention `fluree memory` or `memory_recall`). If you have a large existing CLAUDE.md, make sure the agent is actually reading to the end.
