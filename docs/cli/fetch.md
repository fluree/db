# fluree fetch

Fetch refs from a remote server (similar to `git fetch`).

## Usage

```bash
fluree fetch <REMOTE>
```

## Arguments

| Argument | Description |
|----------|-------------|
| `<REMOTE>` | Remote name (e.g., `origin`) |

## Description

Fetches ledger references from a remote server and updates local tracking data. This does **not** modify your local ledgers - it only updates what the CLI knows about the remote's state.

This is a **replication** operation. It requires a Bearer token with **root / storage-proxy** permissions (`fluree.storage.*`). If you only have permissioned/query access to a ledger, you should use `fluree track` (or `--remote`) and run queries/transactions against the remote instead.

After fetching, you can use `pull` to download and apply new commits to your local ledger.

## Examples

```bash
# Fetch from origin
fluree fetch origin

# Typical workflow
fluree fetch origin
fluree pull mydb
```

## Output

```
Fetching from 'origin'...
Updated:
  mydb -> t=42
  testdb -> t=15
Already up to date: 2 ledger(s) unchanged
```

If no ledgers are found:
```
Fetching from 'origin'...
No ledgers found on remote.
```

## See Also

- [remote](remote.md) - Configure remote servers
- [clone](clone.md) - Clone a ledger from a remote
- [pull](pull.md) - Pull commits from upstream
- [push](push.md) - Push to upstream
