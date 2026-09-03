# fluree doc search

Search a ledger's chunks by meaning, by words, or by both, and get back citations.

```bash
fluree doc search <QUERY> [-l <LEDGER>] [-n <N>] [--mode auto|hybrid|vector|text] [--json]
```

## Options

| Option | Description |
|---|---|
| `-l, --ledger <LEDGER>` | Ledger to search (default: the active ledger). |
| `-n, --limit <N>` | Results to return (default `10`). |
| `--mode <MODE>` | Which index answers; see below. `auto` (default) uses everything the ledger has. |
| `--json` | Print the hits as JSON objects: `score`, `chunk`, `document`, `file`, `path`, `text`, and for a fused hit `ranks`. |

## Modes

Two indexes are built by `ingest`: BM25 over the chunks' text, always, and HNSW over their embeddings when an embedding slot was configured. `search` can use either or both.

| Mode | What runs | Best for |
|---|---|---|
| `text` | BM25 over the full-text index. No model call. | Exact terms, identifiers, part numbers, names; a ledger built without embeddings. |
| `vector` | The query is embedded with the embedding slot and the HNSW index is searched by cosine distance. | Paraphrase: a question worded nothing like the passage that answers it. |
| `hybrid` | Both, each asked for three times the limit, then fused on calibrated scores (below). | The default when both indexes exist. Agreement between meaning and words outranks either alone, and a strong hit only one method finds still surfaces above a weak agreed one. |
| `auto` | `hybrid` when both indexes exist and the query can be embedded; otherwise `vector` or `text`, whichever the ledger has. | Not thinking about it. |

### How hybrid scores

The two methods do not score alike. A vector score is a cosine similarity in 0 to 1, where 0.9 is very high and a relevant passage often sits around 0.5. A BM25 score is a sum of term weights with no ceiling that grows with the query and the corpus, where 3 is weak and 35 is strong. Fusing by rank alone would credit a weak first place as much as a strong one, so each score is first put on one 0 to 1 confidence scale:

| Method | Confidence | So that |
|---|---|---|
| vector | the cosine similarity, as it is | 0.9 stays very high, 0.5 middling |
| text | `s / (s + 10)` | 3 → 0.23, 10 → 0.50, 24 → 0.71, 35 → 0.78 |

A chunk's fused score is the mean of its confidences over both methods, a method that did not return it counting as 0. Agreement wins, a strong single-method hit ranks above a weak agreed one, and the raw score and rank from each method are printed so the fusion is never a mystery:

```
$ fluree doc search "paramount lien on Reserve Bank assets" -l fed -n 2
 1. 0.527  fin_fed_stmts.pdf  … / l. Federal Reserve Notes  vector 0.53 #10 · text 11.1 #1
    Federal Reserve notes are the circulating currency of the United States. These notes … must be fully collateralized…
    urn:fluree:doc:fin_fed_stmts.pdf/chunk/66
 2. 0.282  fin_fed_stmts.pdf  … / i. Allowance for Credit Losses  vector 0.56 #1
    FASB ASC 326, Financial Instruments – Credit Losses provides the updated methodology…
    urn:fluree:doc:fin_fed_stmts.pdf/chunk/61
(2 result(s), hybrid, 1913 ms)
```

This query names the answer's words but not its meaning: vector search alone had the right chunk tenth, BM25 had it first with a solid score, and fused it leads by a wide margin. In `vector` and `text` modes the score shown is the method's own.

## Output

```
 1. 0.812  msa-2024.pdf  Master Services Agreement / 12. Term and Termination
    Either party may terminate this Agreement for convenience on ninety (90) days' written notice…
    urn:fluree:doc:msa-2024.pdf/chunk/97
(1 result(s), vector, 41 ms)
```

Rank and score, the file, the section path, a snippet, and the chunk IRI. The IRI is a node: see [Querying the graph](../guides/querying-the-graph.md) for following it to elements, pages, boxes, mentions and relations, and for writing the search yourself.

## What the command does not do

It searches chunks. Filtering by document, section, or the entities a chunk mentions is a query away, and the guide shows the patterns; the command is the fast path, not the only one.
