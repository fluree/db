# fluree docs

Search and read the Fluree documentation from the command line. The docs are
**embedded in the `fluree` binary**, so lookups work offline and are
**version-exact for this build** — they match the Fluree you're developing
against, with no "latest"-vs-installed drift.

This is the human-facing CLI. The same corpus is exposed to IDE agents as the
`docs` toolset of the MCP server — see [`fluree mcp`](mcp.md).

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

## Docs for IDE agents (MCP)

The same lookup is exposed to IDE agents as the **`docs` toolset** of the
unified Fluree MCP server — there is no separate docs server. It is read-only
over static, embedded content (safe to auto-allow), and surfaces four tools:

| Tool | Description |
|------|-------------|
| `docs_search` | Ranked, section-level hits (`query`, `limit?`) |
| `docs_get` | A page or one heading section (with its subtree) as markdown (`path`, `anchor?`) |
| `docs_examples` | Code examples for a topic (`query`, `lang?`, `limit?`) |
| `docs_tree` | The documentation table of contents, for browse/orientation (no args) |

Every result carries a `version` field matching the binary, so the agent can
trust it over training-data recall.

Register it with your IDE (docs only, no `.fluree/` directory needed):

```bash
fluree mcp init --toolsets docs
```

See [`fluree mcp`](mcp.md) for the full command surface, manual config, and
combining `docs` with the `memory` toolset.

## See Also

- [mcp](mcp.md) — the unified MCP server (`fluree mcp init` / `serve` / `status`) and its toolsets
- [Memory: IDE support matrix](../memory/reference/ide-matrix.md) — per-IDE MCP config file paths and keys
