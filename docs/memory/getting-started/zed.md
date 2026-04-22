# Set up Zed

Wire Fluree Memory into [Zed](https://zed.dev)'s agent via MCP.

## Automatic setup

```bash
fluree memory init
```

Accept the Zed prompt, or run:

```bash
fluree memory mcp-install --ide zed
```

## What gets written

- `<repo>/.zed/settings.json` — the `context_servers` key gets a `fluree-memory` entry

```json
{
  "context_servers": {
    "fluree-memory": {
      "command": "fluree",
      "args": ["mcp", "serve", "--transport", "stdio"]
    }
  }
}
```

No `FLUREE_HOME` is set by default — the MCP server walks up from Zed's spawn CWD to find the workspace's `.fluree/`. If you need to pin it explicitly, add an `env` block alongside `command`/`args` with an absolute path.

## Caveat: JSONC

Zed's `settings.json` often contains `//` comments (JSONC). `mcp-install` detects this and will skip the automatic write rather than risk corrupting your settings — it prints a hint telling you to add the block by hand.

If you'd like to pre-empt that, strip comments from `.zed/settings.json` before running `mcp-install`, or paste the block yourself.

## Verify

Restart Zed. In the agent panel:

> Recall project memories about testing.

The agent should call `memory_recall` via the `fluree-memory` context server.
