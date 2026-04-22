# fluree clone

Clone a ledger from a remote server, similar to `git clone`.

## Usage

```bash
# Named-remote clone
fluree clone [OPTIONS] <REMOTE> <LEDGER>

# Origin-based clone (no pre-configured remote)
fluree clone --origin <URI> [--token <TOKEN>] [OPTIONS] <LEDGER>
```

## Arguments

| Argument | Description |
|----------|-------------|
| `<REMOTE>` | Remote name (configured via `fluree remote add`) |
| `<LEDGER>` | Ledger name on the remote server |
| `--origin <URI>` | Bootstrap URI for CID-based clone (replaces `<REMOTE>`) |
| `--token <TOKEN>` | Auth token for origin server (with `--origin` only) |
| `--no-indexes` | Skip pulling binary index data; only transfer commits and txn blobs (queries will replay from commits until you run `fluree reindex`) |
| `--no-txns` | Skip pulling original transaction payloads. Commits still transfer (chain remains valid and verifiable), but the raw JSON-LD / SPARQL requests that produced each commit are not downloaded. Use for read-only clones of large ledgers. See [Transaction transfer](#transaction-transfer). |

## Description

Downloads all commits from a remote ledger and creates a local copy:

1. Verifies the remote ledger exists and has commits
2. Creates a local ledger with the same name as on the remote
3. Attempts bulk download via the **pack protocol** (single streaming request)
4. Falls back to paginated JSON export if the server does not support pack
5. Stores all commit and transaction blobs to local CAS
6. By default, also transfers **binary index** artifacts when the remote has an index (see [Index transfer](#index-transfer))
7. Sets the local commit head (and index head when index data was transferred) to match the remote
8. Configures the remote as upstream for future `pull`/`push` (named-remote only)

### Index transfer

When using the pack protocol, the CLI requests **index artifacts** by default so the local ledger is query-ready without a full reindex. The server sends missing commit blobs, txn blobs, and binary index artifacts (dictionaries, branches, leaves) in one stream.

- Use **`--no-indexes`** to transfer only commits and txn blobs. This reduces transfer size and time; afterward, run `fluree reindex` to build the index locally if needed.
- For large transfers (estimated size above ~1 GiB), the CLI prompts: *"Estimated transfer size: ~X. This may take several minutes. Continue? [Y/n]"*. Answer `n` to abort or to re-request without index data (commits-only).
- If the remote has no index yet (e.g. a fresh ledger), only commits and txns are transferred regardless of the flag.

### Transaction transfer

Every commit references an **original transaction blob** — the raw request (JSON-LD insert/update or SPARQL Update) that produced the commit. By default, `fluree clone` downloads these so the local ledger has a complete audit trail of the original payloads.

- Use **`--no-txns`** to skip transaction blobs entirely. The commit chain is still cloned and remains valid and verifiable; only the original request payloads are missing.
- The materialized ledger state (what queries return) is reconstructable from commits + indexes alone — transactions are not needed for query answering.
- With `--no-txns`, operations that need the original request payload (e.g., `fluree show --flakes` for transaction-level inspection, or re-running a transaction against a branch) will fail locally for those transactions. Anything that only reads materialized state is unaffected.
- Combine with `--no-indexes` for the smallest possible clone (`fluree clone --no-indexes --no-txns origin mydb`), useful for minimal verification / auditing of the commit chain only.

### Transport

The CLI uses the **pack protocol** (`fluree-pack-v1`) as the primary transport for clone and pull. Pack transfers all missing CAS objects (commits + txn blobs, and by default index artifacts) in a single streaming HTTP request, avoiding per-object round-trips.

If the remote server does not support the pack endpoint (returns 404, 405, 406, or 501), the CLI automatically falls back to:
- **Named-remote mode**: paginated JSON export via `GET /commits/{ledger}` (500 commits per page)
- **Origin mode**: CID chain walk via `GET /storage/objects/{cid}` (one round-trip per commit)

This fallback is transparent -- no user action is required.

### Origin-based clone

The `--origin` flag enables CID-based clone from a server URL without pre-configuring a named remote:

```bash
fluree clone --origin http://localhost:8090 mydb
fluree clone --origin https://api.example.com --token @~/.fluree/token mydb
```

This mode:
1. Fetches the NsRecord from the origin to discover the head commit CID
2. Optionally upgrades to a multi-origin fetcher if a LedgerConfig is advertised
3. Downloads commits via pack (or CID chain walk as fallback)
4. Stores the LedgerConfig locally for future origin-based `pull`
5. Does **not** configure upstream tracking (use `fluree upstream set` manually)

This is a **replication** operation. It requires a Bearer token with **root / storage-proxy** permissions (`fluree.storage.*`). If you only have permissioned/query access to a ledger, you should use `fluree track` (or `--remote`) and run queries/transactions against the remote instead.

**Idempotent CAS writes:** If interrupted mid-clone, CAS blob writes are idempotent. Re-running the clone command will re-fetch all pages (duplicate writes are harmless). The local head is only set after all data is downloaded.

## Examples

```bash
# Clone a ledger from a configured remote
fluree clone origin mydb

# Full workflow: add remote, then clone
fluree remote add production https://api.example.com --token @~/.fluree/token
fluree clone production customers

# Origin-based clone (no remote setup needed)
fluree clone --origin http://localhost:8090 mydb

# Origin-based clone with auth
fluree clone --origin https://api.example.com --token @~/.fluree/token mydb

# Clone without index data (faster; run fluree reindex afterward if needed)
fluree clone --no-indexes origin mydb

# Clone commits + indexes but skip original transaction payloads
fluree clone --no-txns origin mydb

# Smallest possible clone — commits only (no indexes, no transactions)
fluree clone --no-indexes --no-txns origin mydb
```

## Output

Successful clone (via pack, with index data):
```
Cloning 'mydb:main' from 'origin' (remote t=1042)...
  fetched 2084 object(s) via pack
✓ Cloned 'mydb:main' (1042 commits, head t=1042)
  → upstream set to 'origin/mydb:main'
```

With `--no-indexes` (commits and txns only), the object count will be lower and the local index head is not set until you run `fluree reindex`.

Successful clone (fallback to paginated export):
```
Cloning 'mydb:main' from 'origin' (remote t=1042)...
  fetched 500 commits...
  fetched 1000 commits...
  fetched 1042 commits...
✓ Cloned 'mydb:main' (1042 commits, head t=1042)
  → upstream set to 'origin/mydb:main'
```

Origin-based clone:
```
Cloning 'mydb:main' from 'http://localhost:8090' (remote t=50)...
  fetched 100 object(s) via pack
✓ Cloned 'mydb:main' (50 commit(s), head t=50)
```

Remote ledger has no commits:
```
Remote ledger 'mydb:main' has no commits (t=0), nothing to clone.
```

## Errors

| Error | Description |
|-------|-------------|
| Remote not configured | Run `fluree remote add <name> <url>` first |
| Ledger not found on remote | Verify the ledger name matches the remote server |
| Auth failure | Token missing or lacks `fluree.storage.*` permissions |
| Local ledger already exists | Drop the existing ledger first |

## Limitations

- **Post-clone indexing:** If you used `--no-indexes`, run `fluree reindex` to build a binary index locally. Without an index, queries replay from commits and can be slow for large ledgers. When index data is transferred by default (no `--no-indexes`), the local index head is set and no reindex is needed for the core ledger.
- **Missing transactions:** If you used `--no-txns`, the original transaction payloads for historical commits are permanently unavailable on the local clone (re-pull will not fetch them unless you explicitly re-clone without the flag). The ledger state remains queryable; only transaction-level inspection and replay are affected.
- **Graph source indexes not replicated:** Graph source snapshots (BM25/vector/geo, etc.) are not replicated by `fluree clone` yet. After cloning, rebuild graph source indexes in the target environment as needed.

## See Also

- [pull](pull.md) - Pull new commits from upstream
- [push](push.md) - Push local commits to upstream
- [remote](remote.md) - Configure remote servers
- [upstream](upstream.md) - Configure upstream tracking
