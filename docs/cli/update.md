# fluree update

Update data with full WHERE/DELETE/INSERT semantics.

## Usage

```bash
fluree update [LEDGER] [DATA] [OPTIONS]
```

## Arguments

| Arguments | Behavior |
|-----------|----------|
| (none) | Active ledger; provide data via `-e`, `-f`, or stdin |
| `<arg>` | Auto-detected: if it looks like data (JSON or SPARQL UPDATE), uses it inline with the active ledger; if it's an existing file, reads from it; otherwise treats it as a ledger name |
| `<ledger> <data>` | Specified ledger + inline data |

## Options

| Option | Description |
|--------|-------------|
| `-e, --expr <EXPR>` | Inline data expression (alternative to positional) |
| `-f, --file <FILE>` | Read data from a file |
| `-m, --message <MSG>` | Commit message |
| `--format <FORMAT>` | Data format: `jsonld` or `sparql` (auto-detected if omitted) |
| `--remote <NAME>` | Execute against a remote server (by remote name) |
| `--direct` | Bypass auto-routing through a local server (global flag; see note on SPARQL UPDATE below) |

## Description

Executes a WHERE/DELETE/INSERT transaction against a ledger. Unlike `insert` (which only adds data) and `upsert` (which replaces by subject+predicate), `update` supports the full WHERE/DELETE/INSERT pattern, enabling:

- **Conditional deletes**: delete triples matching a WHERE pattern
- **Conditional updates**: delete old values and insert new ones based on WHERE matches
- **Computed updates**: use variables from WHERE to derive new values via `bind`
- **Delete-only operations**: WHERE + DELETE without INSERT
- **Insert-only operations**: equivalent to `insert` but using the update command

### Supported Formats

- **JSON-LD** (default): transaction body with `where`, `delete`, and/or `insert` keys
- **SPARQL UPDATE**: standard `INSERT DATA`, `DELETE DATA`, `DELETE/INSERT WHERE` syntax

### SPARQL UPDATE Note

SPARQL UPDATE requires the server's parsing pipeline. It works automatically when:
- A local server is running (the CLI auto-routes through it by default)
- Using `--remote` to target a remote server

For direct local mode (`--direct`), use JSON-LD format instead.

## Examples

### Conditional Property Update (JSON-LD)

```bash
# Update Alice's age: find old value, delete it, insert new one
fluree update '{
  "@context": {"ex": "http://example.org/"},
  "where": [{"@id": "ex:alice", "ex:age": "?oldAge"}],
  "delete": [{"@id": "ex:alice", "ex:age": "?oldAge"}],
  "insert": [{"@id": "ex:alice", "ex:age": 31}]
}'
```

### Delete-Only

```bash
# Remove all email addresses for alice
fluree update '{
  "@context": {"ex": "http://example.org/"},
  "where": [{"@id": "ex:alice", "ex:email": "?email"}],
  "delete": [{"@id": "ex:alice", "ex:email": "?email"}]
}'
```

### Bulk Conditional Update

```bash
# Set all "pending" users to "active"
fluree update '{
  "@context": {"ex": "http://example.org/"},
  "where": [{"@id": "?person", "ex:status": "pending"}],
  "delete": [{"@id": "?person", "ex:status": "pending"}],
  "insert": [{"@id": "?person", "ex:status": "active"}]
}'
```

### From File

```bash
fluree update -f update.json
fluree update -f update.json -m "Updated user statuses"
```

### SPARQL UPDATE (via server)

```bash
# Requires a running server (fluree server start)
fluree update -e 'PREFIX ex: <http://example.org/>
DELETE { ex:alice ex:age ?oldAge }
INSERT { ex:alice ex:age 31 }
WHERE { ex:alice ex:age ?oldAge }'
```

### Pipe from stdin

```bash
cat update.json | fluree update
```

### Target a specific ledger

```bash
fluree update production -f migration.json
```

## Output

```
Committed t=3, 4 flakes
```

With remote mode, the full server response is printed as JSON.

## Format Detection

The format is auto-detected using this priority:

1. **Explicit flag** (`--format`) — always wins
2. **File extension** (when using `-f` or a positional file path):
   - `.json`, `.jsonld` → JSON-LD
   - `.rq`, `.ru`, `.sparql` → SPARQL UPDATE
3. **Content sniffing**:
   - Valid JSON (full parse, not just first character) → JSON-LD
   - Starts with `INSERT`, `DELETE`, `PREFIX`, or `BASE` → SPARQL UPDATE

Override with `--format jsonld` or `--format sparql`.

## Comparison with Insert and Upsert

| Operation | WHERE clause | DELETE | Conditional | Use case |
|-----------|-------------|--------|-------------|----------|
| `insert` | No | No | No | Add new data |
| `upsert` | No | Auto (per subject+predicate) | No | Replace values for known entities |
| `update` | Yes | Explicit | Yes | Targeted updates, deletes, complex transformations |

## See Also

- [insert](insert.md) - Insert new data
- [upsert](upsert.md) - Insert or replace existing data
- [Update (WHERE/DELETE/INSERT)](../transactions/update-where-delete-insert.md) - Full transaction syntax guide
- [query](query.md) - Query data
