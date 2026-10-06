# fluree reindex

Full reindex from commit history.

## Usage

```bash
fluree reindex [LEDGER]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `[LEDGER]` | Ledger name (defaults to active ledger) |

## Description

Rebuilds the binary index from scratch by replaying all commits in order. This is a heavier operation than `index` — use it when the index is corrupted, missing, or you want a guaranteed clean rebuild.

For routine indexing after transactions, prefer [`index`](index.md).

## Examples

```bash
# Reindex the active ledger
fluree reindex

# Reindex a specific ledger
fluree reindex mydb
```

## Output

```
Reindexed mydb to t=15 (root: bafyreig...)
```

## When to Use

- **Suspected index corruption** — query results seem wrong or incomplete.
- **After schema or configuration changes** that affect index structure.
- **Clean slate** — you want to guarantee the index matches the commit history exactly.
- **Merged on an older build** — a general merge landed while the ledger was indexed by a build from before branch operations stamped their flakes on the target's clock. Head reads are correct; `--at` reads served from that index stay wrong until a rebuild.

For incremental indexing (faster, merges only new commits), use [`index`](index.md) instead.

## See Also

- [index](index.md) - Incremental index build
- [Background indexing](../indexing-and-search/background-indexing.md) - Automatic indexing in the server
- [Reindex API](../indexing-and-search/reindex.md) - Rust API reference
