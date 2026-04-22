# fluree track

Track a remote ledger without storing local data. Tracked ledgers route reads and writes to the configured remote server while keeping a lightweight record locally so you can use short aliases and the active-ledger shortcut.

## Usage

```bash
fluree track <SUBCOMMAND>
```

## Subcommands

### fluree track add

Start tracking a remote ledger under a local alias.

**Usage:**

```bash
fluree track add <LEDGER> [--remote <NAME>] [--remote-alias <NAME>]
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `<LEDGER>` | Local alias for the tracked ledger |

**Options:**

| Option | Description |
|--------|-------------|
| `--remote <NAME>` | Remote name (e.g., `origin`). Defaults to the only configured remote if unambiguous. |
| `--remote-alias <NAME>` | Alias on the remote (defaults to the local alias) |

**Examples:**

```bash
# Track a remote ledger using the same name locally
fluree track add production --remote origin

# Use a different local alias
fluree track add prod --remote origin --remote-alias production
```

### fluree track remove

Stop tracking a remote ledger. Local data is not affected (tracked ledgers have none).

**Usage:**

```bash
fluree track remove <LEDGER>
```

| Argument | Description |
|----------|-------------|
| `<LEDGER>` | Local alias to stop tracking |

### fluree track list

List all currently tracked ledgers and the remote each resolves to.

**Usage:**

```bash
fluree track list
```

### fluree track status

Show status of tracked ledger(s) by querying the configured remote for each — commit t, index t, and head IDs.

**Usage:**

```bash
fluree track status [LEDGER]
```

| Argument | Description |
|----------|-------------|
| `[LEDGER]` | Local alias (shows all tracked ledgers if omitted) |

**Examples:**

```bash
# Status of all tracked ledgers
fluree track status

# Status for a single tracked ledger
fluree track status production
```

## Description

A tracked ledger is a local pointer to a remote ledger. Queries, transactions, and most administrative commands against a tracked alias are transparently forwarded to the remote. This lets you work against a hosted ledger using the same CLI flow as a local ledger — including the active-ledger shortcut (`fluree use`), without syncing commit/index data to disk.

Use `fluree clone` instead when you need a full local copy of a remote ledger's data.

## See Also

- [remote](remote.md) - Manage named remote servers
- [clone](clone.md) - Clone a remote ledger locally (with data)
- [use](use.md) - Switch active ledger
- [list](list.md) - List local and tracked ledgers
