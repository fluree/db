# Set up Windsurf

Wire Fluree Memory into [Windsurf](https://codeium.com/windsurf) (Codeium's IDE).

## Automatic setup

```bash
fluree memory init
```

Accept the Windsurf prompt, or run:

```bash
fluree memory mcp-install --ide windsurf
```

## What gets written

Windsurf uses a **global** MCP config:

- `~/.codeium/windsurf/mcp_config.json` — a `fluree-memory` entry is merged under `mcpServers`

```json
{
  "mcpServers": {
    "fluree-memory": {
      "command": "fluree",
      "args": ["mcp", "serve", "--transport", "stdio"]
    }
  }
}
```

Because the config is global, it's wired once and every Windsurf project can use it. The MCP server figures out which repo it's serving by walking up from its spawn CWD until it finds a `.fluree/` directory; in normal use Windsurf spawns it from the workspace root so this works without extra configuration. No `FLUREE_HOME` is set by default.

## Verify

Restart Windsurf and open your project. In Cascade (Windsurf's agent chat):

> Use memory_recall to find testing patterns.

The agent should invoke the tool.

## Troubleshooting

If memories end up in a global store instead of `<repo>/.fluree-memory/`, Windsurf is likely spawning the server from outside the workspace. Edit `~/.codeium/windsurf/mcp_config.json` and add an explicit absolute path:

```json
"env": { "FLUREE_HOME": "/absolute/path/to/repo/.fluree" }
```

`${workspaceFolder}` interpolation is not guaranteed in all Windsurf versions — when in doubt, use an absolute path and switch it per project.
