# fluree drop

Hard-drop a ledger or graph source.

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

Deletes a ledger using the same hard-drop mode as `POST /drop` with `"hard": true`. For managed storage backends, Fluree deletes storage artifacts and removes the nameservice record where the backend supports purge, allowing the alias to be reused. Deleted artifacts are irreversible.

The command first tries to drop the name as a ledger. If no ledger is found, it tries to drop it as a graph source. This means `fluree drop` works uniformly for both ledgers and graph sources like Iceberg mappings.

The `--force` flag is required to prevent accidental deletion. There is no CLI soft-drop flag; use the HTTP or Rust API if you need to retract a ledger while preserving artifacts.

Graph source cleanup is implementation-specific. The command retracts the graph source record and performs any available hard-drop cleanup for that graph source type; warnings are printed when cleanup is partial.

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

Ledger with artifact cleanup:
```
Dropped ledger 'oldledger' (deleted 23 artifacts)
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
