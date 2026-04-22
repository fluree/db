# fluree upsert

Upsert data into a ledger (insert or update existing).

## Usage

```bash
fluree upsert [LEDGER] [DATA] [OPTIONS]
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

Upserts RDF data into a ledger. Unlike `insert`, upsert will:
- Insert new entities
- Replace existing values for entities that already exist (matched by `@id`)

This is useful for updating data without needing to know whether it exists.

## Examples

```bash
# Update or insert a user
fluree upsert '@prefix ex: <http://example.org/> .
ex:alice ex:name "Alice Smith" ; ex:age 31 .'

# Upsert from file
fluree upsert -f updates.ttl

# Upsert with commit message
fluree upsert '{"@id": "ex:alice", "ex:status": "active"}' -m "Updated Alice status"
```

## Output

```
Committed t=2 (3 flakes)
```

## Difference from Insert

| Operation | Existing Entity | New Entity |
|-----------|-----------------|------------|
| `insert` | Adds new triples (may create duplicates) | Creates entity |
| `upsert` | Replaces values for given predicates | Creates entity |

## See Also

- [insert](insert.md) - Insert without replacement
- [update](update.md) - Full WHERE/DELETE/INSERT updates
- [query](query.md) - Query data
- [history](history.md) - View change history
