# fluree history

Show change history for an entity.

## Usage

```bash
fluree history <ENTITY> [OPTIONS]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `<ENTITY>` | Entity IRI (compact or full) |

## Options

| Option | Description |
|--------|-------------|
| `--ledger <LEDGER>` | Ledger name (defaults to active ledger) |
| `--from <TIME>` | Start of time range (default: `1`) |
| `--to <TIME>` | End of time range (default: `latest`) |
| `-p, --predicate <PRED>` | Filter to specific predicate |
| `--format <FORMAT>` | Output format: `json`, `table`, or `csv` (default: `table`) |

## Description

Shows the change history for a specific entity across transactions. Each change shows:
- `t` - Transaction number
- `op` - Operation: `+` (assert) or `-` (retract)
- `predicate` - The property that changed (if not filtered)
- `value` - The value asserted or retracted

## Prefix Expansion

Entity IRIs can use stored prefixes:

```bash
# First, add a prefix
fluree prefix add ex http://example.org/

# Then use compact IRI
fluree history ex:alice
```

Or use the full IRI:
```bash
fluree history http://example.org/alice
```

## Examples

```bash
# Show all changes to an entity
fluree history ex:alice

# Show changes in JSON format
fluree history ex:alice --format json

# Filter to specific predicate
fluree history ex:alice -p ex:name

# Show changes in a time range
fluree history ex:alice --from 1 --to 5

# Query specific ledger
fluree history ex:alice --ledger production
```

## Output

### Table (default)

```
┌───┬────┬─────────────────────────────────┬─────────────┐
│ t │ op │ predicate                       │ value       │
├───┼────┼─────────────────────────────────┼─────────────┤
│ 1 │ +  │ http://example.org/name         │ Alice       │
│ 1 │ +  │ http://example.org/age          │ 30          │
│ 2 │ -  │ http://example.org/name         │ Alice       │
│ 2 │ +  │ http://example.org/name         │ Alice Smith │
└───┴────┴─────────────────────────────────┴─────────────┘
```

### JSON

```json
[
  {"?t": 1, "?op": true, "?p": "http://example.org/name", "?v": "Alice"},
  {"?t": 1, "?op": true, "?p": "http://example.org/age", "?v": 30},
  {"?t": 2, "?op": false, "?p": "http://example.org/name", "?v": "Alice"},
  {"?t": 2, "?op": true, "?p": "http://example.org/name", "?v": "Alice Smith"}
]
```

### CSV

```
t,op,predicate,value
1,+,http://example.org/name,Alice
1,+,http://example.org/age,30
2,-,http://example.org/name,Alice
2,+,http://example.org/name,Alice Smith
```

## See Also

- [prefix](prefix.md) - Manage prefix mappings
- [log](log.md) - Show commit history
- [query](query.md) - Run custom queries
