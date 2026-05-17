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

## Dropping a single named graph

To drop just one **named graph** inside a ledger (without removing the
ledger, the branch, or any other graph), use `fluree graph drop`:

```bash
# Drop one named graph (active ledger, default branch)
fluree graph drop urn:example:org/payroll

# Drop on a specific ledger / branch
fluree graph drop urn:example:org/payroll --ledger mydb
fluree graph drop http://example.org/graphs/scratch --ledger mydb:feature-x

# Drop via a tracked remote server
fluree graph drop urn:example:org/payroll --ledger mydb --remote origin
```

Unlike `fluree drop`, `fluree graph drop` is **transactional and history-
preserving**: it produces a normal commit at `t = current + 1` whose
flakes retract every triple currently asserted in the graph, leaves the
graph IRI registered (so subsequent inserts land in the same graph slot),
and lets queries `as-of` an earlier `t` still see the dropped data.

The graph IRI must be an absolute IRI (e.g. `urn:...`, `http://...`).
The default graph and the system graphs (`urn:fluree:{ledger_id}#txn-meta`
and `urn:fluree:{ledger_id}#config`) cannot be dropped.

To see what graphs exist on a ledger, use `fluree graph list` (or look
at the `named-graphs` section of `fluree info <ledger>`).

## See Also

- [create](create.md) - Create a new ledger
- [iceberg](iceberg.md) - Map Iceberg tables as graph sources
- [list](list.md) - List all ledgers and graph sources
