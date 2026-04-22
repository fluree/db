# fluree memory init

Initialize the memory store and optionally configure MCP for detected AI coding tools. Idempotent — safe to run repeatedly.

```bash
fluree memory init [OPTIONS]
```

## Options

| Option | Description |
|---|---|
| `--yes`, `-y` | Auto-confirm all MCP installations (non-interactive) |
| `--no-mcp` | Skip AI tool detection and MCP configuration entirely |

## What init does

1. **Creates the `__memory` ledger** inside `<repo>/.fluree/` and transacts the memory schema.
2. **Creates `.fluree-memory/`** at the project root:
   - `repo.ttl` — team memories (empty to start; meant to be committed)
   - `.local/user.ttl` — your personal memories (gitignored)
   - `.gitignore` — pre-configured with `.local/` (which holds your user scope and the MCP log)
3. **Migrates existing memories** — if the ledger already has memories (e.g. from before the TTL file layout), they're exported into the appropriate `.ttl` file.
4. **Detects AI coding tools** (Claude Code, Cursor, VS Code, Windsurf, Zed) and offers to install MCP for each.

## Example

```bash
$ fluree memory init

Memory store initialized at /path/to/project/.fluree-memory

Repo memories are stored in .fluree-memory/repo.ttl (git-tracked).
Commit this directory to share project knowledge with your team.

Detected AI coding tools:
  - Claude Code (already configured)
  - Cursor
  - VS Code (Copilot) (already configured)

Install MCP config for Cursor? [Y/n] Y
  Installed: .cursor/mcp.json
  Installed: .cursor/rules/fluree_rules.md

Configured 1 tool.
```

With `--yes`: auto-confirms all installations without prompting. In a non-interactive shell (piped stdin) without `--yes`, MCP installation is skipped with a message.

## Re-running

`init` is safe to run again. It won't re-create or overwrite files that already exist; it just:

- Checks that the ledger and schema are current (migrating if not).
- Detects IDEs you've since installed and offers to configure them.
- Leaves existing memories untouched.

Run it again after:
- Installing a new AI tool you want to wire up.
- Cloning a repo someone else set up — `init` will pick up the committed `repo.ttl` into the ledger automatically.
