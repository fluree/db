# fluree query

Query a ledger.

## Usage

```bash
fluree query [LEDGER] [QUERY] [OPTIONS]
```

## Arguments

| Arguments | Behavior |
|-----------|----------|
| (none) | Active ledger; provide query via `-e`, `-f`, or stdin |
| `<arg>` | Auto-detected: if it looks like a query, uses it inline with the active ledger; if it's an existing file, reads from it; otherwise treats it as a ledger name |
| `<ledger> <query>` | Specified ledger + inline query |

## Options

| Option | Description |
|--------|-------------|
| `-e, --expr <EXPR>` | Inline query expression (alternative to positional) |
| `-f, --file <FILE>` | Read query from a file |
| `--format <FORMAT>` | Output format: `json`, `typed-json`, `table`, `csv`, `tsv`, or `ndjson` (default: `table`) |
| `--envelope` | With `--format ndjson`, emit the full streaming record protocol verbatim instead of bare binding objects |
| `--sparql` | Force SPARQL query format |
| `--jsonld` | Force JSON-LD query format |
| `--at <TIME>` | Query at a specific point in time |
| `--normalize-arrays` | Always wrap multi-value properties in arrays (graph-crawl JSON-LD queries only) |
| `--bench` | Benchmark mode: time execution only and print the first 5 rows as a table (no full-result JSON formatting) |
| `--explain` | Print the query plan without executing it |
| `--remote <NAME>` | Execute against a remote server (by remote name, e.g., `origin`) |

## Description

Executes a query against a ledger. Supports both SPARQL and JSON-LD query formats.

## Query Formats

### SPARQL

```bash
fluree query 'SELECT ?name WHERE { ?s <http://example.org/name> ?name }'
```

### JSON-LD Query

```bash
fluree query '{"select": ["?name"], "where": {"http://example.org/name": "?name"}}'
```

Format is auto-detected if not specified:
- Contains `SELECT`, `CONSTRUCT`, `ASK`, or `DESCRIBE` → SPARQL
- Otherwise → JSON-LD

## Output Formats

### JSON (default)

```bash
fluree query 'SELECT ?name WHERE { ?s <http://example.org/name> ?name }'
```
```json
{
  "head": {"vars": ["name"]},
  "results": {"bindings": [{"name": {"type": "literal", "value": "Alice"}}]}
}
```

### Table

```bash
fluree query --format table 'SELECT ?name WHERE { ?s <http://example.org/name> ?name }'
```
```
┌───────┐
│ name  │
├───────┤
│ Alice │
│ Bob   │
└───────┘
```

### CSV

```bash
fluree query --format csv 'SELECT ?name WHERE { ?s <http://example.org/name> ?name }'
```
```
name
Alice
Bob
```

Note: `--format csv` (and `--format tsv`) are only supported for **local** ledgers. Tracked/remote ledgers support `json` and `table` output.

### NDJSON (streaming)

`--format ndjson` streams SELECT results **incrementally** as newline-delimited
JSON instead of buffering the whole result set — use it for large result sets
and unix pipelines (`| jq`, `| while read`, etc.). Rows are flushed as the query
produces them, so the CLI never holds the full result in memory.

By default each line is a bare SPARQL-JSON binding object (the same term shapes
as the `json` format's `results.bindings` entries), one per result row:

```bash
fluree query --format ndjson 'SELECT ?name WHERE { ?s <http://example.org/name> ?name }'
```
```
{"name":{"type":"literal","value":"Alice"}}
{"name":{"type":"literal","value":"Bob"}}
```

Add `--envelope` to emit the full streaming record protocol verbatim — the same
bytes the server's [streaming query endpoint](../api/streaming-query.md) produces
(`head` / `row` / `heartbeat` / `end` / `error`). This is useful for debugging
and for detecting truncation (a stream that ends without a terminal record):

```bash
fluree query --format ndjson --envelope 'SELECT ?name WHERE { ?s <http://example.org/name> ?name }'
```
```
{"type":"head","vars":["name"]}
{"type":"row","row":{"name":{"type":"literal","value":"Alice"}}}
{"type":"row","row":{"name":{"type":"literal","value":"Bob"}}}
{"type":"end","rows":2}
```

In bare mode the CLI consumes the terminal record internally and exits **non-zero**
if the stream carried an `error` or ended without a terminal (truncated /
dropped connection). A closed downstream pipe (e.g. `| head`) ends the stream
cleanly with exit 0.

Scope (mirrors the server [streaming endpoint](../api/streaming-query.md)):
NDJSON streaming applies to **SELECT** queries only. `ASK`, `CONSTRUCT`/`DESCRIBE`,
`selectOne`, hydration, and history-range queries are rejected — use a buffered
`--format`. On **local** ledgers, `--at` (time travel) and per-request `--policy`
are not supported with `--format ndjson`; use `--remote` (where they route
through the server's dataset path) or a buffered format instead. `--bench` and
`--explain` are not compatible with `--format ndjson`.

## Time Travel

Query historical states with `--at`:

```bash
# Query at transaction 5
fluree query --at 5 'SELECT * WHERE { ?s ?p ?o }'

# Query at specific commit
fluree query --at abc123def 'SELECT * WHERE { ?s ?p ?o }'

# Query at ISO-8601 timestamp
fluree query --at 2024-01-15T10:30:00Z 'SELECT * WHERE { ?s ?p ?o }'
```

Tracked/remote ledgers also support `--at`. The CLI will translate `--at` into the appropriate dataset/time-travel form when forwarding the query to the remote server.

SPARQL note (remote): if your SPARQL already includes `FROM` / `FROM NAMED`, the CLI will **not** rewrite it for `--at`. In that case, encode time travel directly in the `FROM` IRI (e.g., `FROM <myledger:main@t:5>`).

## Examples

```bash
# Inline SPARQL query (most common)
fluree query 'SELECT ?name WHERE { ?s <http://example.org/name> ?name }'

# JSON-LD query inline
fluree query '{"select": {"?s": ["*"]}, "where": {"@id": "?s"}}'

# Query specific ledger with CSV output
fluree query production --format csv 'SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10'

# SPARQL query from file
fluree query -f query.rq

# Time travel query
fluree query --at 3 'SELECT * WHERE { ?s ?p ?o }'

# Pipe from stdin
cat query.rq | fluree query
```

## See Also

- [history](history.md) - View entity change history
- [export](export.md) - Export all data
