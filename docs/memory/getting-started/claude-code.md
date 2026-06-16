# Set up Claude Code

Wire Fluree Memory into [Claude Code](https://claude.com/claude-code) so it saves and recalls memories for you.

## Automatic setup

Easiest path: run `init` from your project root and accept the Claude Code prompt.

```bash
cd my-project
fluree memory init
```

When you see:

```
Detected AI coding tools:
  - Claude Code

Install MCP config for Claude Code? [Y/n]
```

…press `Y`. This runs `claude mcp add` under the hood to register the Fluree Memory MCP server at local (user) scope, and appends a short section to your `CLAUDE.md` telling Claude when to use it.

If you already ran `init` and skipped it:

```bash
fluree mcp install --ide claude-code
```

## What gets added

- **Two MCP servers** registered in `~/.claude.json` — scope `local`
  - `fluree-memory` → `fluree mcp serve --transport stdio`
  - `fluree-docs` → `fluree docs serve --transport stdio` (version-pinned documentation lookup — `docs_search` / `docs_get` / `docs_examples`; see [fluree docs](../../cli/docs.md))
- **Project instructions** in `<repo>/CLAUDE.md` — a short block explaining the memory and docs tools

## Verify

Restart Claude Code and start a session in the project. Ask:

> What project memories do you have?

Claude should call `memory_recall` and return whatever you've added (initially nothing).

Try:

> Remember: we use `cargo nextest` for tests, not `cargo test`.

Claude should call `memory_add` and report the stored ID.

## Troubleshooting

**The tool doesn't appear.** Confirm Claude Code sees the MCP server:

```bash
claude mcp list
```

You should see `fluree-memory` and `fluree-docs` entries. If not, re-run `fluree mcp install --ide claude-code`.

**Memories aren't scoped to the repo.** The Claude Code MCP entry doesn't set `FLUREE_HOME` — the server walks up from its spawn CWD looking for a `.fluree/` directory. In normal use this matches the workspace, but if Claude Code launched the server from outside your repo, memories can land in a global store. Fix by editing `~/.claude.json` and adding an `env` block to the `fluree-memory` server entry:

```json
"env": { "FLUREE_HOME": "/absolute/path/to/your/repo/.fluree" }
```

Then restart Claude Code.

**The MCP log.** The MCP server logs to `<repo>/.fluree-memory/.local/mcp.log` (the file is truncated on each server start). Tail it if something's off:

```bash
tail -f .fluree-memory/.local/mcp.log
```
