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
| `--format <FORMAT>` | Output format: `json`, `typed-json`, `table`, `csv`, or `tsv` (default: `table`) |
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
