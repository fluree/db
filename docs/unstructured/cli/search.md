# fluree doc search

Search a ledger's chunks by meaning or by words, and get back citations.

```bash
fluree doc search <QUERY> [-l <LEDGER>] [-n <N>] [--mode auto|vector|text] [--json]
```

## Options

| Option | Description |
|---|---|
| `-l, --ledger <LEDGER>` | Ledger to search (default: the active ledger). |
| `-n, --limit <N>` | Results to return (default `10`). |
| `--mode <MODE>` | `vector` embeds the query with the embedding slot and searches the HNSW index; `text` runs BM25; `auto` (default) picks `vector` when an embedding slot is configured. |
| `--json` | Print the rows as JSON: `[score, chunk, document, file, section path, text]`. |

## Output

```
 1. 0.812  msa-2024.pdf  Master Services Agreement / 12. Term and Termination
    Either party may terminate this Agreement for convenience on ninety (90) days' written notice…
    urn:fluree:doc:msa-2024.pdf/chunk/97
(1 result(s), vector, 41 ms)
```

Rank and score, the file, the section path, a snippet, and the chunk IRI. The IRI is a node: see [Querying the graph](../guides/querying-the-graph.md) for following it to elements, pages and boxes, and for writing the search yourself.
