# fluree dropped

List, restore or purge ledgers dropped with [`fluree drop`](drop.md).

## Usage

```bash
fluree dropped list
fluree dropped restore <INSTANCE>
fluree dropped purge <INSTANCE> --force
```

## Subcommands

| Command | Description |
|---------|-------------|
| `fluree dropped list` | Lists dropped ledgers, most recently dropped first. |
| `fluree dropped restore <INSTANCE>` | Restores a dropped ledger under the name it was dropped under. |
| `fluree dropped purge <INSTANCE> --force` | Deletes a dropped ledger's data. Irreversible. |

## Options

| Option | Description |
|--------|-------------|
| `--force` | Confirms deletion. Required by `purge`. |
| `--remote <NAME>` | Run against a configured remote server. |

## Description

`fluree drop` keeps a ledger's data by default. The dropped ledger is held in a list of dropped ledgers until it is restored or purged.

A dropped ledger is named by its **instance id**, not its name, because a new ledger may already hold the name. `fluree drop` prints the instance id, and `fluree dropped list` shows it.

- **Restore** brings the ledger back under its name, with the branches and data it had. A branch dropped before the ledger stays dropped. Writers that loaded the ledger before it was dropped stay refused and must reload it. Restore fails if another ledger now holds the name; drop or rename that one first.
- **Purge** deletes the data and removes the entry.

A state of `restoring` or `purging` means that operation was interrupted; running it again finishes it.

Without `--remote`, the commands go to a locally running server when there is one (see [server integration](server-integration.md)), and otherwise to the local store. Pass `--direct` to skip the server.

## Examples

```bash
fluree drop oldledger
fluree dropped list
fluree dropped restore 01J9Z6Q8W2M4T7XK3B5N1C0D9E

fluree drop oldledger
fluree dropped purge 01J9Z6Q8W2M4T7XK3B5N1C0D9E --force
```

## Output

List:
```
+----------------------------+-----------+-------------------------+---------+----------+
| INSTANCE                   | NAME      | DROPPED                 | STATE   | BRANCHES |
+----------------------------+-----------+-------------------------+---------+----------+
| 01J9Z6Q8W2M4T7XK3B5N1C0D9E | oldledger | 2026-09-27 14:02:11 UTC | dropped | main     |
+----------------------------+-----------+-------------------------+---------+----------+
```

Restore:
```
Restored ledger 'oldledger'
```

Purge:
```
Purged dropped ledger 'oldledger' (deleted 73 artifacts)
```

## Errors

Another ledger holds the name:
```
error: Ledger already exists: oldledger
```

No dropped ledger has the instance id:
```
error: Dropped ledger not found: 01J9Z6Q8W2M4T7XK3B5N1C0D9E
```

## See Also

- [drop](drop.md) - Drop a ledger or graph source
- [list](list.md) - List ledgers and graph sources
