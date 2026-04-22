# fluree list

List all ledgers and graph sources.

## Usage

```bash
fluree list
```

## Description

Lists all ledgers and graph sources (Iceberg, R2RML, BM25, Vector, etc.) in the current Fluree directory. The active ledger is marked with an asterisk (`*`).

When graph sources are present, a TYPE column is shown to distinguish ledgers from graph sources.

## Examples

```bash
fluree list
```

## Output

When only ledgers exist:
```
   LEDGER      BRANCH  T
 * mydb        main    5
   production  main    12
```

When graph sources are also present:
```
   NAME              BRANCH  TYPE     T
 * mydb              main    Ledger   5
   production        main    Ledger   12
   warehouse-orders  main    Iceberg  -
   my-search         main    BM25     5
```

If nothing exists:
```
No ledgers found. Run 'fluree create <name>' to create one.
```

## See Also

- [create](create.md) - Create a new ledger
- [iceberg](iceberg.md) - Map Iceberg tables as graph sources
- [info](info.md) - Show detailed ledger or graph source information
- [use](use.md) - Switch active ledger
