# fluree insert

Insert data into a ledger.

## Usage

```bash
fluree insert [LEDGER] [DATA] [OPTIONS]
```

## Arguments

| Arguments | Behavior |
|-----------|----------|
| (none) | Active ledger; provide data via `-e`, `-f`, or stdin |
| `<arg>` | Auto-detected: if it looks like data (JSON, Turtle), uses it inline with the active ledger; if it's an existing file, reads from it; otherwise treats it as a ledger name |
| `<ledger> <data>` | Specified ledger + inline data |

## Options

| Option | Description |
|--------|-------------|
| `-l, --ledger <LEDGER>` | Ledger name (defaults to active ledger); explicit alternative to the positional ledger argument |
| `-e, --expr <EXPR>` | Inline data expression (alternative to positional) |
| `-f, --file <FILE>` | Read data from a file |
| `-m, --message <MSG>` | Commit message |
| `--format <FORMAT>` | Data format: `turtle` (`nt` for N-Triples), `trig` or `jsonld` (auto-detected if omitted) |
| `--remote <NAME>` | Execute against a remote server (by remote name, e.g., `origin`) |

## Description

Inserts RDF data into a ledger. Supports Turtle, TriG and JSON-LD. TriG `GRAPH <iri> { ... }` blocks (and the compact `<iri> { ... }` form) land in their named graphs, and a `GRAPH <#txn-meta> { ... }` block becomes commit metadata. Data can come from:
- A positional argument (inline data)
- `-e` flag (inline expression)
- `-f` flag (file)
- Standard input (pipe)

## Examples

```bash
# Insert inline Turtle
fluree insert '@prefix ex: <http://example.org/> .
ex:alice a ex:Person ; ex:name "Alice" .'

# Insert inline JSON-LD
fluree insert '{"@id": "ex:bob", "ex:name": "Bob"}'

# Insert from file
fluree insert -f data.ttl

# Insert a TriG file: its graph blocks land in their named graphs
fluree insert -f dataset.trig

# Insert with commit message
fluree insert -f data.ttl -m "Added initial users"

# Insert into specific ledger
fluree insert production '<http://example.org/x> a <http://example.org/Thing> .'

# Pipe from stdin
cat data.ttl | fluree insert
```

## Output

```
Committed t=1 (42 flakes)
```

With verbose mode:
```
Committed t=1 (42 flakes)
Commit ID: bafybeig...
```

## Data Format Detection

The format comes from `--format` when given, then from the file extension, then from the content:
- `.ttl` or `.nt` → Turtle (N-Triples is a subset of Turtle and uses the same parser)
- `.trig` → TriG
- `.json` or `.jsonld` → JSON-LD
- No extension, or inline or piped data: content that parses as JSON is JSON-LD, and anything else is Turtle

Turtle input that turns out to contain graph blocks is read as TriG, so a TriG body needs no flag. Override detection with `--format turtle` (or `ttl`, or `nt`), `--format trig` or `--format jsonld`.

N-Quads (`.nq`) is not read by `insert`; import it into a new ledger with `fluree create <ledger> --from <file>.nq`.

## See Also

- [upsert](upsert.md) - Insert or update existing data
- [update](update.md) - Full WHERE/DELETE/INSERT updates
- [query](query.md) - Query the inserted data
- [export](export.md) - Export all data
