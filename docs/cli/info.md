# fluree info

Show detailed information about a ledger or graph source.

## Usage

```bash
fluree info [NAME] [--remote <name>] [--graph <name|IRI>]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `[NAME]` | Ledger or graph source name (defaults to active ledger) |

## Options

| Option | Description |
|--------|-------------|
| `--remote <name>` | Query a remote server (e.g., `origin`) instead of the local installation. |
| `--graph <name\|IRI>` | Scope the `stats` block to a single named graph within the ledger. Accepts a well-known name (`default`, `txn-meta`) or a graph IRI. Not applicable to graph sources. |

## Description

Displays detailed information about a ledger or graph source. The command first checks for a matching ledger; if none is found, it checks for a graph source with the same name.

For ledgers, displays:
- Ledger ID, branch, and type
- Current transaction number (t)
- Commit and index details

For graph sources (Iceberg, R2RML, BM25, etc.), displays:
- Name, branch, and type
- Graph source ID
- Index status
- Dependencies
- Configuration (catalog URI, table, mapping, etc.)

## Examples

```bash
# Info for active ledger
fluree info

# Info for specific ledger
fluree info production

# Info for a graph source
fluree info warehouse-orders

# Query a remote server
fluree info production --remote origin

# Scope stats to the default graph
fluree info mydb --graph default

# Scope stats to the transaction-metadata graph
fluree info mydb --graph txn-meta

# Scope stats to a specific named graph by IRI
fluree info mydb --graph https://example.org/graphs/inventory
```

When `--graph` is set, the command prints the full `ledger-info` JSON response
with the `stats` block scoped to the selected graph (properties, classes,
flakes, size).

## Output

Ledger:
```
Ledger:         mydb
Branch:         main
Type:           Ledger
Ledger ID:      mydb:main
Commit t:       5
Commit ID:      bafybeig...
Index t:        5
Index ID:       bafybeig...
```

Graph source (Iceberg):
```
Name:           warehouse-orders
Branch:         main
Type:           Iceberg
ID:             warehouse-orders:main
Retracted:      false
Index t:        0
Index ID:       (none)

Configuration:
{
  "catalog": {
    "type": "rest",
    "uri": "https://polaris.example.com/api/catalog"
  },
  "table": "sales.orders",
  ...
}
```

## See Also

- [list](list.md) - List all ledgers and graph sources
- [iceberg](iceberg.md) - Map Iceberg tables as graph sources
- [log](log.md) - Show commit history
