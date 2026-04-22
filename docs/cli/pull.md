# fluree pull

Pull commits from upstream and apply them to the local ledger, similar to `git pull`.

## Usage

```bash
fluree pull [OPTIONS] [LEDGER]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `[LEDGER]` | Ledger name (defaults to active ledger) |
| `--no-indexes` | Skip pulling binary index data; only transfer new commits and txn blobs (local index may lag until you run `fluree reindex`) |

## Description

Downloads new commits from the configured upstream and applies them to the local ledger:

1. Queries the remote for its current head (`t` and commit ContentId)
2. Compares with the local head; exits early if already up to date
3. Attempts bulk download of missing commits (and by default **index artifacts**) via the **pack protocol** (single streaming request)
4. Falls back to paginated JSON export if the server does not support pack
5. Stores all commit and transaction blobs to local CAS
6. When index data is requested and transferred, advances the local index head to match the remote
7. Advances the local commit head to the remote head

### Index transfer

As with [clone](clone.md#index-transfer), pull uses the pack protocol to request **index artifacts** by default when the remote has an index. Use **`--no-indexes`** to transfer only new commits and txn blobs. For large estimated transfers (~1 GiB or more), the CLI prompts for confirmation before streaming.

### Transport

Pull uses the same pack protocol as clone -- see [clone: Transport](clone.md#transport) for details.

### Origin-based pull

When no upstream remote is configured, pull falls back to **origin-based fetching** if a LedgerConfig with origins is set on the ledger (see `fluree config set-origins`). This uses the same pack-first / CID-walk-fallback transport as `fluree clone --origin`.

This is a **replication** operation. It requires a Bearer token with **root / storage-proxy** permissions (`fluree.storage.*`). If you only have permissioned/query access to a ledger, you should use `fluree track` (or `--remote`) and run queries/transactions against the remote instead.

The ledger must have an upstream configured (see `fluree upstream set`), **or** a LedgerConfig with origins (see `fluree config set-origins`).

**Restart safety:** If interrupted, the local head reflects the last successful import. The next pull resumes from the local head automatically.

## Examples

```bash
# Pull changes for active ledger
fluree pull

# Pull changes for specific ledger
fluree pull mydb

# Pull commits only (skip index transfer)
fluree pull --no-indexes mydb
```

## Output

Successful pull (with index data when remote has an index):
```
Pulling 'mydb:main' from 'origin' (local t=10, remote t=42)...
✓ 'mydb:main' pulled 32 commit(s) via pack (new head t=42)
```

With `--no-indexes`, only commits (and referenced txn blobs) are transferred; the message does not include index artifact counts.

Already up to date:
```
✓ 'mydb:main' is already up to date
```

No upstream configured:
```
error: no upstream configured for 'mydb:main'
  hint: fluree upstream set mydb:main <remote>
```

## Errors

| Error | Description |
|-------|-------------|
| No upstream configured | Run `fluree upstream set <ledger> <remote>` first, or configure origins via `fluree config set-origins` |
| Ancestry mismatch | Remote chain does not descend from local head (histories diverged) |
| Import validation failure | Commit chain or retraction invariant violation |

## Limitations

- **Index head vs commit head:** When you use `--no-indexes`, the local index head is not updated. Queries still work but may replay more novelty; run `fluree reindex` to bring the index up to the current commit head.
- **Graph source indexes not replicated:** Graph source snapshots (BM25/vector/geo, etc.) are not replicated by `fluree pull` yet. Rebuild graph source indexes in the target environment as needed.

## See Also

- [clone](clone.md) - Clone a ledger from a remote server
- [upstream](upstream.md) - Configure upstream tracking
- [fetch](fetch.md) - Fetch refs without modifying local ledger
- [push](push.md) - Push local changes to upstream
