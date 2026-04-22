# fluree memory

Developer memory — store and recall facts, decisions, and constraints.

> This page is the CLI command reference. For conceptual background, IDE setup, team workflows, and the full schema, see the [Memory section](../memory/README.md) of the docs.

## Usage

```bash
fluree memory <COMMAND>
```

## Subcommands

| Command | Description |
|---------|-------------|
| `init` | Initialize the memory store (creates `__memory` ledger) |
| `add` | Store a new memory |
| `recall` | Search and rank relevant memories |
| `update <ID>` | Update a memory in place |
| `forget <ID>` | Delete a memory |
| `status` | Show memory store status |
| `export` | Export all current memories as JSON |
| `import <FILE>` | Import memories from a JSON file |
| `mcp-install` | Install MCP configuration for an IDE |

## Description

The memory system stores project knowledge as RDF triples in a dedicated `__memory` Fluree ledger. Memories persist across sessions and are searchable by keyword-scored recall.

Run `fluree memory init` before using other memory commands. The MCP server auto-initializes on first tool call.

## fluree memory init

Initialize the memory store and optionally configure MCP for detected AI coding tools. Idempotent — safe to run multiple times.

```bash
fluree memory init [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `--yes, -y` | Auto-confirm all MCP installations (non-interactive) |
| `--no-mcp` | Skip AI tool detection and MCP configuration entirely |

### What init does

1. **Creates the `__memory` ledger** and transacts the memory schema.
2. **Creates `.fluree-memory/`** at the project root with `repo.ttl`, `.gitignore`, and `.local/user.ttl`.
3. **Migrates existing memories** — if the ledger already has memories (e.g., from a pre-TTL version), they are exported to the appropriate `.ttl` files.
4. **Detects AI coding tools** (Claude Code, Cursor, VS Code, Windsurf, Zed) and offers to install MCP config for each.

### Example

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

## fluree memory add

Store a new memory.

```bash
fluree memory add [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `--kind <KIND>` | Memory kind: `fact`, `decision`, `constraint` (default: `fact`) |
| `--text <TEXT>` | Content text (or provide via stdin) |
| `--tags <T1,T2>` | **Required.** Comma-separated tags for categorization — the primary recall signal |
| `--refs <R1,R2>` | Comma-separated file/artifact references |
| `--severity <SEV>` | For constraints: `must`, `should`, `prefer` |
| `--scope <SCOPE>` | Scope: `repo` (default) or `user` |
| `--rationale <TEXT>` | Why this memory exists (available on any kind) |
| `--alternatives <TEXT>` | Alternatives considered (comma-separated) |
| `--format <FMT>` | Output format: `text` (default) or `json` |

### Examples

```bash
# Store a fact
fluree memory add --kind fact --text "Tests use cargo nextest" --tags testing,cargo

# Store a constraint with severity
fluree memory add --kind constraint --text "Never suppress dead code with underscore prefix" \
  --tags code-style --severity must

# Store from stdin
echo "The index format uses postcard encoding" | fluree memory add --kind fact --tags indexer

# Store a decision with rationale and alternatives
fluree memory add --kind decision --text "Use postcard for compact index encoding" \
  --rationale "no_std compatible, smaller output than bincode" \
  --alternatives "bincode, CBOR, MessagePack" --refs fluree-db-indexer/

# Store a fact with rationale
fluree memory add --kind fact --text "PSOT queries return supersets — post-filter required" \
  --rationale "B-tree range scan can't filter on non-key predicates" --tags query,index
```

Output (text):
```
Stored memory: mem:fact-01JDXYZ5A2B3C4D5E6F7G8H9J0
```

### Secret detection

If the content contains secrets (API keys, passwords, tokens, connection strings), they are automatically redacted and a warning is printed:

```
  warning: secrets detected in content — storing redacted version.
  Original content contained sensitive data that was replaced with [REDACTED].
Stored memory: mem:fact-01JDXYZ...
```

## fluree memory recall

Search and retrieve relevant memories ranked by score.

```bash
fluree memory recall <QUERY> [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<QUERY>` | Natural language search query |

### Options

| Option | Description |
|--------|-------------|
| `-n, --limit <N>` | Maximum results per page (default: 3) |
| `--offset <N>` | Skip the first N results — use for pagination (default: 0) |
| `--kind <KIND>` | Filter to a specific memory kind |
| `--tags <T1,T2>` | Filter to memories with these tags |
| `--scope <SCOPE>` | Filter by scope: `repo` or `user` |
| `--format <FMT>` | Output: `text` (default), `json`, or `context` (XML for LLM) |

### Examples

```bash
# Basic recall (returns top 3)
fluree memory recall "how to run tests"

# Get the next page
fluree memory recall "how to run tests" --offset 3

# Return up to 10 results
fluree memory recall "error handling" -n 10

# Filter by kind and tags
fluree memory recall "error handling" --kind constraint --tags errors

# Output as XML context (for LLM injection)
fluree memory recall "testing patterns" --format context
```

Output (text):
```
Recall: "how to run tests" (2 matches)

1. [score: 13.0] mem:fact-01JDXYZ...
   Tests use cargo nextest
   Tags: testing, cargo

2. [score: 8.0] mem:fact-01JDABC...
   Integration tests use assert_cmd + predicates
   Tags: testing

  (showing results 1–3; use --offset 3 for more)
```

Output (context):
```xml
<memory-context>
  <memory id="mem:fact-01JDXYZ..." kind="fact" score="13.0">
    <content>Tests use cargo nextest</content>
    <tags>testing, cargo</tags>
  </memory>
  <pagination shown="1" offset="0" total_in_store="13" />
</memory-context>
```

When results are cut off, the pagination element includes a hint:

```xml
  <pagination shown="3" offset="0" limit="3" total_in_store="13">Results 1–3. Use offset=3 to retrieve more.</pagination>
```

## fluree memory update

Update a memory in place. Only the fields you provide are changed — the ID stays the same. History is tracked via git.

```bash
fluree memory update <ID> [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `--text <TEXT>` | New content text |
| `--tags <T1,T2>` | New tags (replaces all existing) |
| `--refs <R1,R2>` | New artifact refs (replaces all existing) |
| `--format <FMT>` | Output: `text` or `json` |

### Example

```bash
fluree memory update mem:fact-01JDXYZ... --text "Tests use cargo nextest with --no-fail-fast"
```

Output:
```
Updated: mem:fact-01JDXYZ...
```

## fluree memory forget

Delete a memory by retracting all its triples.

```bash
fluree memory forget <ID>
```

Output:
```
Forgotten: mem:fact-01JDXYZ...
```

## fluree memory status

Show a summary of the memory store.

```bash
fluree memory status
```

Output:
```
Memory Store Status
  Total memories: 12
  Total tags:     25
  By kind:
    fact: 7
    decision: 2
    constraint: 3
```

## fluree memory export / import

Export all current (non-superseded) memories as JSON, or import from a file.

```bash
fluree memory export > memories.json
fluree memory import memories.json
```

## fluree memory mcp-install

Install MCP configuration for an IDE so agents can use memory tools.

```bash
fluree memory mcp-install [--ide <IDE>]
```

### Options

| Option | Description |
|--------|-------------|
| `--ide <IDE>` | Target IDE (auto-detected if omitted) |

Supported IDE values:

| Value | Config written | Notes |
|-------|----------------|-------|
| `claude-code` | `claude mcp add` (local scope → `~/.claude.json`) | Also appends to `CLAUDE.md` |
| `vscode` | `.vscode/mcp.json` (key: `servers`) | Also installs `.vscode/fluree_rules.md` |
| `cursor` | `.cursor/mcp.json` (key: `mcpServers`) | Also installs `.cursor/rules/fluree_rules.md` |
| `windsurf` | `~/.codeium/windsurf/mcp_config.json` (global) | — |
| `zed` | `.zed/settings.json` (key: `context_servers`) | Skips if JSONC (comments) detected |

Legacy aliases: `claude-vscode` and `github-copilot` map to `vscode`.

When `--ide` is omitted, the first unconfigured detected tool is used; defaults to `claude-code` if none detected.

### Example

```bash
fluree memory mcp-install --ide cursor
```

Output:
```
  Installed: .cursor/mcp.json
  Installed: .cursor/rules/fluree_rules.md
```

### Cursor notes (recommended config)

Cursor’s MCP configuration supports stdio servers with a `type` field and config interpolation like `${workspaceFolder}`. A portable repo-scoped setup looks like:

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

Setting `FLUREE_HOME` ensures the MCP server uses the current workspace’s `.fluree/` directory even if Cursor spawns the process from a different working directory. That keeps repo memory/logs under `<repo>/.fluree-memory/` instead of a global location.

### Troubleshooting: repo vs global memory

- **Repo-scoped expected**:
  - Memories: `<repo>/.fluree-memory/repo.ttl`
  - MCP log: `<repo>/.fluree-memory/.local/mcp.log` (should show `client initialized` after a full Cursor restart)
- **If it’s using global dirs on macOS**:
  - Memories/log: `~/Library/Application Support/.fluree-memory/...`
  - Fix: ensure your Cursor config sets `env.FLUREE_HOME = "${workspaceFolder}/.fluree"` and restart Cursor fully.

## See Also

- [Memory overview](../memory/README.md) — what it is, when to use it, how it fits into your workflow
- [Memory getting started](../memory/getting-started/README.md) — install, quickstart, and per-IDE setup guides
- [Memory concepts](../memory/concepts/README.md) — repo vs user memory, supersession, recall ranking, secrets
- [Memory guides](../memory/guides/README.md) — team workflows, rules-file customization, migrating from plain markdown
- [Memory reference](../memory/reference/README.md) — IDE support matrix, `mem:` schema, TTL file format
- [mcp](mcp.md) — MCP server for IDE agent integration
