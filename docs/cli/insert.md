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
| `-e, --expr <EXPR>` | Inline data expression (alternative to positional) |
| `-f, --file <FILE>` | Read data from a file |
| `-m, --message <MSG>` | Commit message |
| `--format <FORMAT>` | Data format: `turtle` or `jsonld` (auto-detected if omitted) |
| `--remote <NAME>` | Execute against a remote server (by remote name, e.g., `origin`) |

## Description

Inserts RDF data into a ledger. Supports both Turtle and JSON-LD formats. Data can come from:
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

The format is auto-detected:
- `@prefix` or `@base` at line start → Turtle
- Starts with `{` or `[` → JSON-LD
- `.ttl` file extension → Turtle
- `.json` or `.jsonld` extension → JSON-LD

Override with `--format turtle` or `--format jsonld`.

## See Also

- [upsert](upsert.md) - Insert or update existing data
- [update](update.md) - Full WHERE/DELETE/INSERT updates
- [query](query.md) - Query the inserted data
- [export](export.md) - Export all data
