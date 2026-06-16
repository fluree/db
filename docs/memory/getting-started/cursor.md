# Set up Cursor

Wire Fluree Memory into [Cursor](https://cursor.com) so its agent mode saves and recalls memories for you.

## Automatic setup

From your project root:

```bash
cd my-project
fluree memory init
```

Accept the Cursor prompt:

```
Install MCP config for Cursor? [Y/n]
```

Or, at any time:

```bash
fluree memory mcp-install --ide cursor
```

## What gets written

- `<repo>/.cursor/mcp.json` — repo-scoped MCP server config
- `<repo>/.cursor/rules/fluree_rules.md` — a short rules file telling Cursor when to reach for `memory_recall`

```json
{
  "mcpServers": {
    "fluree-memory": {
      "type": "stdio",
      "command": "fluree",
      "args": ["mcp", "serve", "--transport", "stdio"],
      "env": {
        "FLUREE_HOME": "${workspaceFolder}/.fluree"
      }
    }
  }
}
```

`${workspaceFolder}` is a Cursor config-interpolation token — the MCP server is always launched with `FLUREE_HOME` pointing at the current project, so memories stay scoped to the repo even if Cursor spawns the process from a different working directory.

## Verify

Fully restart Cursor (Cmd-Q on macOS, not just reload window). Open the project and ask the agent:

> Recall project memories for testing.

The agent should call `memory_recall` with the tag `testing` and return what's in `.fluree-memory/repo.ttl`.

## Troubleshooting

**MCP isn't connecting.** Tail the MCP log:

```bash
tail -f .fluree-memory/.local/mcp.log
```

You should see a `client initialized` line within a few seconds of Cursor startup. If not, check `.cursor/mcp.json` exists and is valid JSON, then restart Cursor.

**Memories going to a global store on macOS.** If you see memories landing in `~/Library/Application Support/.fluree-memory/` instead of `<repo>/.fluree-memory/`, `FLUREE_HOME` isn't being honored. Re-run `fluree memory mcp-install --ide cursor` from inside the repo and restart Cursor fully.

**Rules file ignored.** Cursor picks up `.cursor/rules/*.md` on project open. After editing, reload the window.
