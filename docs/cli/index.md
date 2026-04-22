# fluree index

Build or update the binary index for a ledger.

## Usage

```bash
fluree index [LEDGER]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `[LEDGER]` | Ledger name (defaults to active ledger) |

## Description

Performs incremental indexing when possible — merges only new commits into the existing index. Falls back to a full rebuild if incremental indexing isn't possible (e.g., no prior index exists).

Run this after transactions to clear the novelty layer and speed up queries. For routine use this is preferred over `reindex`, which always rebuilds from scratch.

## Examples

```bash
# Index the active ledger
fluree index

# Index a specific ledger
fluree index mydb
```

## Output

```
Indexed mydb to t=15 (root: bafyreig...)
```

## When to Use

- **After bulk transactions** — clears accumulated novelty so queries hit the optimized binary index instead of scanning in-memory flakes.
- **Routine maintenance** — keeps query performance consistent as data grows.
- **After `clone --no-indexes` or `pull --no-indexes`** — builds the local index that was skipped during transfer.

For a clean rebuild from commit history (e.g., suspected corruption), use [`reindex`](reindex.md) instead.

## See Also

- [reindex](reindex.md) - Full rebuild from commit history
- [Background indexing](../indexing-and-search/background-indexing.md) - Automatic indexing in the server
- [Reindex API](../indexing-and-search/reindex.md) - Rust API reference
