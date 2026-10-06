# fluree cache

Inspect or clear the peer-mode index artifact cache.

When a tracked ledger uses `--mode peer` (see [track](track.md)), queries
execute locally against index blocks fetched from the remote's raw storage
tier. Fetched artifacts are cached on disk, keyed by remote, under the OS
cache directory (`<cache-dir>/fluree/peer-cache/<remote>/`). Everything in
the cache is content-addressed and immutable — entries never go stale and
clearing is always safe (artifacts re-fetch on demand).

## Subcommands

### fluree cache status

Show per-remote disk usage.

```bash
fluree cache status
```

```
Peer cache: /Users/me/Library/Caches/fluree/peer-cache
  origin                   412.3 MiB
  analytics                88.1 MiB
  total                    500.4 MiB
```

### fluree cache clear

Delete cached artifacts — everything, or one remote's with `--remote`.

```bash
fluree cache clear                    # all remotes
fluree cache clear --remote origin    # one remote
```

## See Also

- [track](track.md) - Track a remote ledger (peer mode)
- [remote](remote.md) - Manage remotes and list accessible ledgers
