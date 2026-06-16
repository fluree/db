# fluree docs

Search and read the Fluree documentation from the command line, and serve it to
IDE agents over MCP. The docs are **embedded in the `fluree` binary**, so lookups
work offline and are **version-exact for this build** — they match the Fluree
you're developing against, with no "latest"-vs-installed drift.

## Usage

```bash
fluree docs <COMMAND>
```

## Subcommands

| Command | Description |
|---------|-------------|
| `search` | Ranked, section-level search |
| `get` | Print a page, or one heading section, as markdown |
| `examples` | Extract code examples for a topic |
| `tree` | Print the documentation table of contents |
| `serve` | Start the standalone `fluree-docs` MCP server |

No `.fluree/` directory or running database is required — these work in any
directory.

## fluree docs search

```bash
fluree docs search <QUERY> [--limit <N>] [--json]
```

Returns ranked hits at heading-section granularity — each with the page path,
heading anchor, title, a snippet, and a relevance score (BM25 over titles,
headings, and body, with stemming).

| Option | Description |
|--------|-------------|
| `--limit <N>` | Max hits (default: 10) |
| `--json` | Emit JSON instead of human-readable text |

```bash
fluree docs search "property paths"
```

## fluree docs get

```bash
fluree docs get <PATH> [--anchor <ANCHOR>] [--json]
```

Prints a whole page as markdown, or just one section when `--anchor` is given.
Use the `path` and `anchor` from a `search` hit.

| Option | Description |
|--------|-------------|
| `--anchor <ANCHOR>` | Return only the section with this heading anchor |
| `--json` | Emit JSON (`{path, title, anchor, content, version}`) |

```bash
fluree docs get query/sparql.md --anchor property-paths
```

## fluree docs examples

```bash
fluree docs examples <QUERY> [--lang <LANG>] [--limit <N>] [--json]
```

Extracts fenced code blocks from the sections most relevant to the query — often
all you need to get the syntax right in one shot.

| Option | Description |
|--------|-------------|
| `--lang <LANG>` | Only return code blocks in this language (e.g. `json`, `sparql`) |
| `--limit <N>` | Max examples (default: 10) |
| `--json` | Emit JSON |

```bash
fluree docs examples "insert transaction" --lang json
```

## fluree docs tree

```bash
fluree docs tree [--json]
```

Print the documentation table of contents — the curated `SUMMARY.md` hierarchy
of titles and page paths. Use it to orient (see what topics exist) and grab a
page path to feed `get`, rather than inferring structure from search results.

| Option | Description |
|--------|-------------|
| `--json` | Emit JSON (`{nodes: [{title, path, children}], version}`) instead of an indented tree |

```bash
fluree docs tree
```

Only pages listed in `SUMMARY.md` appear; a markdown file not in the TOC is still
searchable and retrievable by path, but won't show in the tree.

## fluree docs serve

Start the standalone **`fluree-docs`** MCP server, which exposes the same lookup
to IDE agents. It is read-only over static, embedded content — safe to
auto-allow with no permission friction — and is **separate** from the
developer-memory server ([`fluree mcp serve`](mcp.md)).

```bash
fluree docs serve [--transport <TRANSPORT>]
```

| Option | Description |
|--------|-------------|
| `--transport <TRANSPORT>` | Transport protocol: `stdio` (default) |

### Tools

| Tool | Description |
|------|-------------|
| `docs_search` | Ranked, section-level hits (`query`, `limit?`) |
| `docs_get` | A page or one heading section (with its subtree) as markdown (`path`, `anchor?`) |
| `docs_examples` | Code examples for a topic (`query`, `lang?`, `limit?`) |
| `docs_tree` | The documentation table of contents, for browse/orientation (no args) |

Every result carries a `version` field matching the binary, so the agent can
trust it over training-data recall.

### IDE configuration

`fluree mcp install` registers **both** the memory and docs servers in one step
(see [mcp install](mcp.md#fluree-mcp-install)). To add only the docs server —
e.g. in a project that doesn't use Fluree Memory — scope it with `--server`:

```bash
fluree mcp install --server docs
```

Or register it manually with your agent:

```bash
claude mcp add -t stdio fluree-docs -- fluree docs serve --transport stdio
```

Or add it to your IDE's MCP config:

```json
{
  "mcpServers": {
    "fluree-docs": {
      "type": "stdio",
      "command": "/path/to/fluree",
      "args": ["docs", "serve", "--transport", "stdio"]
    }
  }
}
```

### Testing with JSON-RPC

```bash
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke","version":"0.0"}}}' \
  '{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' \
  | fluree docs serve --transport stdio
```

## See Also

- [mcp](mcp.md) — the developer-memory MCP server (`fluree mcp serve`) and `fluree mcp install`
- [Memory: IDE support matrix](../memory/reference/ide-matrix.md) — per-IDE MCP config file paths and keys
