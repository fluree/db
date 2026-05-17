# fluree drop

Hard-drop an entire ledger (every branch under the name) or a graph source.

## Usage

```bash
fluree drop <NAME> --force
```

## Arguments

| Argument | Description |
|----------|-------------|
| `<NAME>` | Ledger name (bare, e.g. `mydb`) or graph source name. Branch-qualified ledger ids like `mydb:main` are accepted with a warning; non-default suffixes like `mydb:dev` are rejected — use `fluree branch drop dev --ledger mydb` to drop a single branch. |

## Options

| Option | Description |
|--------|-------------|
| `--force` | Required flag to confirm deletion |

## Description

Hard-drops a **whole ledger** — every branch under the name, including any retracted-but-not-purged branches, plus the cross-branch `@shared/dicts/` namespace. Branches are dropped leaf-first so partial failure leaves orphan parents rather than dangling children. Equivalent to `POST /drop` with `"hard": true`. Deleted artifacts are irreversible.

The command first tries to drop the name as a ledger. If no nameservice record exists for the name, it tries to drop it as a graph source. This means `fluree drop` works uniformly for both ledgers and graph sources like Iceberg mappings.

The `--force` flag is required to prevent accidental deletion. There is no CLI soft-drop flag; use the HTTP or Rust API if you need to retract a ledger while preserving artifacts. To remove a single branch (not the whole ledger), use `fluree branch drop`.

Graph source cleanup is implementation-specific. The command retracts the graph source record and performs any available hard-drop cleanup for that graph source type; warnings are printed when cleanup is partial.

## Examples

```bash
# Drop the whole "oldledger" ledger (all branches + @shared/dicts/)
fluree drop oldledger --force

# Drop a graph source (Iceberg mapping)
fluree drop warehouse-orders --force
```

## Output

Ledger:
```
Dropped ledger 'oldledger'
```

Ledger with artifact cleanup:
```
Dropped ledger 'oldledger' (deleted 73 artifacts across 3 branches)
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

Branch-qualified input with a non-default suffix:
```
error: drop_ledger drops the whole ledger and does not accept a non-default
       branch suffix 'dev'. Use drop_branch("mydb", "dev") to drop a single
       branch, or pass "mydb" to drop the whole ledger.
```

## See Also

- [create](create.md) - Create a new ledger
- [iceberg](iceberg.md) - Map Iceberg tables as graph sources
- [list](list.md) - List all ledgers and graph sources
