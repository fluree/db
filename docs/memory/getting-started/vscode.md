# Set up VS Code (Copilot)

Wire Fluree Memory into [VS Code](https://code.visualstudio.com) with GitHub Copilot Chat so it can save and recall memories through MCP.

## Automatic setup

From your project root:

```bash
fluree memory init
```

Accept the VS Code prompt, or run:

```bash
fluree memory mcp-install --ide vscode
```

## What gets written

- `<repo>/.vscode/mcp.json` — repo-scoped MCP server config (key: `servers`)
- `<repo>/.vscode/fluree_rules.md` — rules file you can reference from your prompts

```json
{
  "servers": {
    "fluree-memory": {
      "type": "stdio",
      "command": "fluree",
      "args": ["mcp", "serve", "--transport", "stdio"]
    }
  }
}
```

Unlike the Cursor config, this entry does not set `FLUREE_HOME` — VS Code normally spawns the server from the workspace root, so the walk-up logic in `fluree mcp serve` finds `.fluree/` on its own. If you need to pin the location explicitly (e.g. the server is ending up in a global store), add an `env` block pointing at the absolute path to `<repo>/.fluree/`.

## Verify

Open the project in VS Code with Copilot Chat enabled. In chat (agent mode), ask:

> Call memory_recall for "testing".

Copilot should invoke the tool and return matching memories. On first use VS Code may prompt to allow the MCP server — approve it.

## Troubleshooting

Tail `.fluree-memory/.local/mcp.log` and fully restart VS Code if something's off. If memory is landing in a global store rather than the repo, add an explicit `env.FLUREE_HOME` pointing at `<repo>/.fluree/` in `.vscode/mcp.json` and restart.
