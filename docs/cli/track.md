# fluree track

Track a remote ledger without storing local data. Tracked ledgers keep a lightweight record locally so you can use short aliases and the active-ledger shortcut. Two query modes are available:

- **proxy** (default): reads and writes route to the remote server over HTTP — the remote executes your queries (its compute, row-level policy applied).
- **peer**: queries execute **locally** against index blocks fetched on demand from the remote's raw storage tier, CID-verified and cached on disk per remote (see `fluree cache`). Writes still forward to the remote over HTTP. Requires a bearer token with `fluree.storage.*` scope for the ledger — the remote serves its full contents, so this mode is only offered for ledgers you may read in full (see [Remote mounts and serving tiers](../design/remote-mounts.md)). When the remote vends S3 credentials (`GET /storage/credentials`), peer reads go directly to S3 automatically; otherwise blocks proxy through the remote's HTTP endpoints.

Use `fluree remote ledgers <name>` to see which ledgers your token can access and which serving tiers (`query`, `blocks`) each offers.

## Usage

```bash
fluree track <SUBCOMMAND>
```

## Subcommands

### fluree track add

Start tracking a remote ledger under a local alias.

**Usage:**

```bash
fluree track add <LEDGER> [--remote <NAME>] [--remote-alias <NAME>] [--mode <proxy|peer>]
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
| `--mode <proxy\|peer>` | Query execution mode (default `proxy`; see above) |

**Examples:**

```bash
# Track a remote ledger using the same name locally
fluree track add production --remote origin

# Use a different local alias
fluree track add prod --remote origin --remote-alias production

# Peer mode: local query execution over remotely-served index blocks
fluree track add analytics --remote origin --mode peer
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
