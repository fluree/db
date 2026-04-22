# fluree upstream

Manage upstream tracking configuration for ledgers.

Upstream configuration links a local ledger to a remote ledger, enabling `pull` and `push` operations.

## Subcommands

| Subcommand | Description |
|------------|-------------|
| `set` | Set upstream tracking for a ledger |
| `remove` | Remove upstream tracking |
| `list` | List all upstream configurations |

---

## fluree upstream set

Configure a local ledger to track a remote ledger.

### Usage

```bash
fluree upstream set <LOCAL> <REMOTE> [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<LOCAL>` | Local ledger ID (e.g., `mydb` or `mydb:main`) |
| `<REMOTE>` | Remote name (e.g., `origin`) |

### Options

| Option | Description |
|--------|-------------|
| `--remote-alias <ALIAS>` | Remote ledger ID (defaults to local ledger ID) |
| `--auto-pull` | Automatically pull on fetch |

### Examples

```bash
# Track remote ledger with same name
fluree upstream set mydb origin

# Track a differently-named remote ledger
fluree upstream set mydb origin --remote-alias production-db

# Enable auto-pull on fetch
fluree upstream set mydb origin --auto-pull
```

---

## fluree upstream remove

Remove upstream tracking for a ledger.

### Usage

```bash
fluree upstream remove <LOCAL>
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<LOCAL>` | Local ledger ID |

### Examples

```bash
fluree upstream remove mydb
```

---

## fluree upstream list

List all configured upstream tracking relationships.

### Usage

```bash
fluree upstream list
```

### Output

```
┌────────────┬─────────┬────────────────┬───────────┐
│ Local      │ Remote  │ Remote Alias   │ Auto-Pull │
├────────────┼─────────┼────────────────┼───────────┤
│ mydb:main  │ origin  │ mydb           │ no        │
│ test:main  │ staging │ test-ledger    │ yes       │
└────────────┴─────────┴────────────────┴───────────┘
```

## See Also

- [remote](remote.md) - Configure remote servers
- [clone](clone.md) - Clone a ledger from a remote
- [pull](pull.md) - Pull from upstream
- [push](push.md) - Push to upstream
