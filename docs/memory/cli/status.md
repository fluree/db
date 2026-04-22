# fluree memory status

Show a summary of the memory store.

```bash
fluree memory status
```

Output:

```
  Directory: /path/to/project/.fluree-memory
Memory Store: 12 memories, 25 tags
  Kinds: 7 fact, 2 decision, 3 constraint

Recent memories:
  - [fact] Tests use cargo nextest, not cargo test [cargo, testing]
    ID: mem:fact-01JDXYZ...
  - [decision] Use postcard for compact index encoding [encoding, indexer]
    ID: mem:decision-01JDABC...

Use memory_recall with specific keywords from above to search.
```

`status` counts all memories in the store. The "Recent memories" list is included to help agents (or you) pick good keywords for `memory_recall`.

## When it's useful

- Confirming init worked and the store is live.
- Sanity-checking after an import.
- Quick "how much does this project remember?" check.

For per-memory detail, use [`recall`](recall.md) with a broad query (e.g. `fluree memory recall "" -n 100`) or [`export`](export-import.md).
