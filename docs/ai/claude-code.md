# Claude Code and the Fluree CLI

How an AI coding agent — Claude Code in particular, but the pattern generalizes — should drive the `fluree` CLI. This page ships inside the binary (`fluree docs get ai/claude-code`), so what it says is true for the exact version you are running; agent-side packaging (the `fluree-cli` Claude Code plugin, a stack's generated build prompt) deliberately defers to it rather than copying it.

## Setup

Register the CLI's MCP server with your agent:

```bash
fluree mcp init --ide claude-code            # or cursor / vscode / windsurf / zed
```

This wires `fluree mcp serve` with the `docs` toolset (ranked search over this documentation, version-exact) and the `memory` toolset (persistent project memory). Use `--toolsets docs` to skip memory. Without MCP, the same corpus is available as plain commands: `fluree docs search`, `fluree docs get`, `fluree docs examples`, `fluree docs tree`.

## Probe, don't assume

The CLI's surface varies by version and by compiled features — and an agent's trained knowledge of it is always stale. Before composing a nontrivial invocation:

- **Check the docs or `--help` first.** `fluree docs search "<topic>"` / `fluree <cmd> --help` are authoritative for the running binary.
- **A missing command usually means a feature-gated build, not a typo.** `validate` (SHACL) and `cluster` (server) vanish entirely from `--help` in builds without their features. The hidden `fluree manifest` command emits a machine-readable JSON of the full surface, including a `features` array that distinguishes "not compiled in" from "does not exist".
- **Check the version when a server is involved.** `fluree --version`; a stack that advertises `cli.min_version` in its `/.well-known/fluree.json` triggers a warning at `fluree remote add` when your binary is older than what its docs teach (governance `fluree model` commands exist only in 4.1.3+).

## Machine-readable output

Support is per-command, not global:

- `query` and `multi-query` take `--format json` (plus CSV/TSV/NDJSON variants); `--envelope` adds a self-describing wrapper.
- `list`, `graph list`, `branch diff`/`merge --preview`, and the four `docs` subcommands take boolean `--json`.
- `info` and `show` are human-formatted only (as of 4.1.x).
- Errors are human text; the only machine signal is the exit code (`0` ok, `1` error, `2` usage). Parse stderr only as a last resort.

## Working against a remote (Fluree Solo / Fluree AI stacks)

- `--remote <name>` takes a **configured alias**, never a URL. Set one up with `fluree remote add <name> <url>`; the compound positional `name/ledger` form is equivalent on data commands.
- Passing the stack's API base (`https://<stack>/v1/fluree`) as `<url>` is robust: discovery of `/.well-known/fluree.json` ignores the input path, and if discovery is unreachable an input already ending in `/fluree` is stored as-is.
- **`fluree auth login` needs the human.** It prints a device code and opens the stack's `/activate` page — the *user* approves it in their browser while the CLI polls. Run the command, then tell the user to approve; continue only after `fluree auth status` shows a configured token. There are no environment-variable credentials.
- **Scripting a token:** `fluree auth token` prints exactly the access token (for `.env` files, `curl`). `fluree config list` masks credentials as `[redacted]`; `--reveal` prints them raw and its output must never be pasted into logs, commits, or chat.
- `.fluree/` contains the remote config **including live access and refresh tokens** — it must be gitignored in any project where an agent runs `fluree init`.

## Destructive operations — confirm with the user first

- `fluree drop --force` is a **hard** delete (storage removed, not recoverable), and `fluree branch drop` has **no confirmation flag at all** — on a leaf branch it permanently deletes storage and cascades into retracted ancestors. `main` has no special protection. Name the exact target and get explicit user confirmation before running either.
- Never drop and recreate a ledger underneath a running server — the server keeps stale index pointers and fails *partially* (some queries work, others 404 on leaf files). Stop the server first.

## Resource limits on shared machines

`--memory-budget-mb 0` (the default) auto-sizes to **80% of system RAM** and assumes it owns the box; a large `create --from` import on a machine also running an IDE, Docker, or a browser can OOM the machine. On anything but a dedicated host, pass an explicit budget and modest `--parallelism` (2–4). Note `FLUREE_IMPORT_THREADS` silently overrides `--parallelism`.

## Policy work

Author policy as a single `where` (object, or array of patterns correlated by shared variables) that constrains `?$this`; unknown keys in `f:query` are **silently ignored**, and a policy whose `where` never touches `?$this` matches everything — silently allowing what it was meant to restrict. Verify with `--track-policy` on direct/local execution (its `allowed/evaluated` counts expose an uncorrelated policy immediately); server-routed `--track-policy` is unreliable. See the [policy cookbook](../guides/cookbook-policies.md).

## Query hygiene for agents

- Prefer `--explain` to inspect a plan without executing.
- `--at` time travel on server-routed queries injects a `FROM` clause by scanning for the literal ` where ` substring — keep `WHERE` on the same line as `SELECT`, and never combine `--at` with an explicit `FROM`.
- Auto-routing silently targets a detected local server (a stderr notice is the only sign); `--direct` forces local execution when behavior differs.
