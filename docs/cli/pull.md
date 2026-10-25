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
| `--erasure-authority` | For a pull from the ledger's origins: accept the permanent erasures they carry, for this pull. With a named upstream it is refused; designate the remote with `fluree remote erasure-authority` instead. |

## Description

Downloads new commits from the configured upstream and applies them to the local ledger:

1. Queries the remote for its current head (`t` and commit ContentId)
2. Compares with the local head. When the local ledger already has every commit, it still asks the pack protocol for what the remote gained without a commit (its removal journal, and erasures of history the ledger holds), then exits
3. Attempts bulk download of missing commits (and by default **index artifacts**) via the **pack protocol** (single streaming request)
4. Falls back to paginated JSON export if the server does not support pack. The export sends the branch's first-parent line and, separately, the commits its merges brought in. A server too old to export that way can still serve a history without merges.
5. Stores all commit and transaction blobs to local CAS
6. When index data is requested and transferred, advances the local index head to match the remote
7. Advances the local commit head to the remote head

### Index transfer

As with [clone](clone.md#index-transfer), pull uses the pack protocol to request **index artifacts** by default when the remote has an index. Use **`--no-indexes`** to transfer only new commits and txn blobs. For large estimated transfers (~1 GiB or more), the CLI prompts for confirmation before streaming.

### Transport

Pull uses the same pack protocol as clone -- see [clone: Transport](clone.md#transport) for details.

### Erased history

When the remote's history holds [permanently erased](../transactions/permanent-erasure.md#clones-and-pulls)
commits, pull goes through the pack protocol only, and only from a remote designated an erasure
authority (`fluree remote erasure-authority <name> true`). The request lists the erasures the local
storage already holds; the remote sends every other erasure of the history, including those of
commits this ledger already has. Before the local head moves, the ledger (and any other ledger here
holding the originals) reads as the erasures do: the values leave its index and history, with nothing
committed to it, and its purge later deletes the originals. A pull that stops partway completes at the
next clone or pull, or when a server on this storage next runs its retention worker. An erasure
published after the ledger has every commit arrives with the next pull all the same, since a pull with
no new commits still asks for one. The paginated export and an origin-based pull refuse erased
history. The ledger's removal journal arrives with any pack, an origin's included; the object-by-object
fallback of an origin-based pull carries none.

An origin-based pull takes erased history only with `--erasure-authority`, which designates the
ledger's origins an erasure authority for that pull, and only through the pack: the object-by-object
fallback walks only the new commits, so it cannot bring the erasures of commits this ledger already
has, and it refuses an erased commit. With a named upstream, the flag is refused; designate the remote
instead.

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
