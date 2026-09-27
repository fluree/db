# fluree drop

Drop an entire ledger (every branch under the name) or a graph source.

## Usage

```bash
fluree drop <NAME> [--hard --force]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `<NAME>` | Ledger name (bare, e.g. `mydb`) or graph source name. Branch-qualified ledger ids (including `mydb:main`) are rejected — use `fluree branch drop <branch> --ledger mydb` to drop a single branch. |

## Options

| Option | Description |
|--------|-------------|
| `--hard` | Delete the data now instead of keeping it restorable. Irreversible. |
| `--force` | Confirms permanent deletion. Required with `--hard`. |
| `--remote <NAME>` | Run against a configured remote server. |

## Description

Drops a **whole ledger**: every branch under the name. The name is free straight away, so `fluree create` can make a new ledger under it, and writers that loaded the old ledger are refused.

By default the data is kept. The dropped ledger moves to a list of dropped ledgers, keyed by an **instance id** that `fluree drop` prints; [`fluree dropped`](dropped.md) lists, restores and purges them.

`--hard` deletes the data now: commits, indexes, dictionaries and the cross-branch `@shared/` namespace. Equivalent to `POST /drop` with `"hard": true`.

A ledger created before name bindings (by an earlier release) has no instance id. Dropping one keeps its data and its name stays reserved; `fluree drop <name> --hard --force` deletes it.

The command first tries to drop the name as a ledger. If no ledger holds the name, it tries to drop it as a graph source, so `fluree drop` works uniformly for both ledgers and graph sources like Iceberg mappings. Graph sources cannot be restored; a soft drop retracts the graph source and keeps its files, and `--hard` deletes them. Graph source cleanup is implementation-specific, and warnings are printed when it is partial.

To remove a single branch (not the whole ledger), use `fluree branch drop`.

## Examples

```bash
# Drop "oldledger", keeping its data
fluree drop oldledger

# Bring it back, or delete its data
fluree dropped restore 01J9Z6Q8W2M4T7XK3B5N1C0D9E
fluree dropped purge 01J9Z6Q8W2M4T7XK3B5N1C0D9E --force

# Drop and delete at once
fluree drop oldledger --hard --force

# Drop a graph source (Iceberg mapping)
fluree drop warehouse-orders
```

## Output

Soft drop:
```
Dropped ledger 'oldledger'
Its data is kept: restore it with `fluree dropped restore 01J9Z6Q8W2M4T7XK3B5N1C0D9E`, or delete it with `fluree dropped purge 01J9Z6Q8W2M4T7XK3B5N1C0D9E --force`.
```

Hard drop:
```
Dropped ledger 'oldledger' (deleted 73 artifacts across 3 branches)
```

Graph source:
```
Dropped graph source 'warehouse-orders:main'
```

## Errors

`--hard` without `--force`:
```
error: use --force with --hard to confirm permanent deletion of 'oldledger'
```

A name no ledger or graph source holds, including one already dropped:
```
error: 'oldledger' not found; `fluree dropped list` shows dropped ledgers
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

- [dropped](dropped.md) - List, restore or purge dropped ledgers
- [create](create.md) - Create a new ledger
- [iceberg](iceberg.md) - Map Iceberg tables as graph sources
- [list](list.md) - List all ledgers and graph sources
