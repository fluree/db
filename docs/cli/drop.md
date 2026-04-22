# fluree drop

Drop (delete) a ledger or graph source.

## Usage

```bash
fluree drop <NAME> --force
```

## Arguments

| Argument | Description |
|----------|-------------|
| `<NAME>` | Ledger or graph source name to drop |

## Options

| Option | Description |
|--------|-------------|
| `--force` | Required flag to confirm deletion |

## Description

Permanently deletes a ledger or graph source. The `--force` flag is required to prevent accidental deletion.

The command first tries to drop the name as a ledger. If no ledger is found, it tries to drop it as a graph source. This means `fluree drop` works uniformly for both ledgers and graph sources like Iceberg mappings.

## Examples

```bash
# Delete a ledger
fluree drop oldledger --force

# Delete a graph source (Iceberg mapping)
fluree drop warehouse-orders --force
```

## Output

Ledger:
```
Dropped ledger 'oldledger'
```

Graph source:
```
Dropped graph source 'warehouse-orders:main'
```

## Errors

Without `--force`:
```
error: use --force to confirm deletion of 'oldledger'
```

## See Also

- [create](create.md) - Create a new ledger
- [iceberg](iceberg.md) - Map Iceberg tables as graph sources
- [list](list.md) - List all ledgers and graph sources
