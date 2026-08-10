# fluree sweep

Reclaim index artifacts that no index chain references.

## Usage

```bash
fluree sweep [LEDGER] [--dry-run] [--remote <NAME>]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `[LEDGER]` | Ledger **name**, without a branch suffix (defaults to active ledger) |

## Options

| Option | Description |
|--------|-------------|
| `--dry-run` | Report what would be reclaimed without deleting anything |
| `--remote <NAME>` | Execute against a remote server (by remote name, e.g. `origin`) |

## Description

Index builds leave superseded artifacts behind. The garbage collector normally reclaims them: each index root carries a manifest naming what the previous version replaced, and the collector releases exactly those. That only reaches artifacts some manifest records, and only **branch-local** ones — index roots, leaves, and branch manifests.

A sweep finds the rest, and it is the only thing that reclaims **dictionary blobs**. It enumerates what storage actually holds, subtracts everything reachable from a live index chain, and releases the remainder. The usual source of such artifacts is a reindex published by Fluree **4.1.4 or earlier**, which severed the chain and left every earlier index version unreachable — so a ledger reindexed on one of those versions can hold a large volume of blobs no retention policy can ever truncate.

A sweep covers **every branch** of a ledger, which is why `LEDGER` names the ledger rather than a branch — a branch-qualified alias is rejected. Dictionary blobs live in a namespace shared by all of a ledger's branches, so releasing one is only safe with every branch's index accounted for. That is exactly why background GC leaves dictionaries alone: it walks a single branch and holds no exclusion over the others, whereas a sweep holds every branch and unions their reachable sets. Soft-dropped branches count as live, so dropping a branch without purging it stays reversible.

Only index artifacts are considered. Commits, transactions, and config blobs are reachable through the commit chain rather than the index chain, so a sweep cannot establish that they are unreferenced and never touches them.

Index builds are held off for the duration. If any index root cannot be read, or any storage prefix cannot be listed, the sweep aborts without deleting anything — an incomplete picture of what is reachable would classify live artifacts as orphans.

> **Single-process deployments only.** The hold that keeps index builds from writing during a sweep excludes the server's own indexer. An external or second-process indexer writing to the same storage is not excluded, and its in-flight artifacts would be indistinguishable from orphans.

## Examples

```bash
# See what would be reclaimed, without deleting
fluree sweep mydb --dry-run

# Reclaim them
fluree sweep mydb

# Against a remote server
fluree sweep mydb --remote origin
```

## Output

```
mydb would reclaim 2847 of 5216 index artifacts (2369 still referenced)
  fluree:file://mydb/main/index/roots/3f2a....fir6
  fluree:file://mydb/@shared/dicts/9c1b....dict
  ...

Run without --dry-run to reclaim them.
```

```
Reclaimed 2847 artifacts from mydb
```

Artifacts that resist deletion are reported rather than treated as failures — they stay in storage and the next sweep retries them.

## When to Use

- **Disk usage far exceeds the data** — the ledger directory is many times the size of its commits.
- **After reindexing on Fluree 4.1.4 or earlier** — those reindexes orphaned every earlier index version, and only a sweep reclaims them.
- **Periodically on write-heavy ledgers** — dictionaries are reclaimed only by a sweep and are often the largest part of index storage, so this is routine maintenance rather than a one-off repair. Running it when nothing is reclaimable is a safe no-op.

## See Also

- [reindex](reindex.md) - Full rebuild from commit history (rebuilds; does not reclaim)
- [index](index.md) - Incremental index build
- [Background indexing](../indexing-and-search/background-indexing.md#reclaiming-orphaned-artifacts) - Retention and reclamation in the server
