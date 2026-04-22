# Recall and ranking

`recall` is how you get memories out. It's a keyword query against an inverted index with BM25 scoring — fast, local, and deterministic.

## The basics

```bash
fluree memory recall "how do I run tests"
```

The query string is tokenized and matched against each memory's **content** via a BM25-scored fulltext index. Tags, artifact refs, kind, branch, and recency contribute as **re-rank bonuses** on top of the BM25 score — they're not part of the fulltext match itself. Results are sorted by combined score (higher = better) and capped at `--limit` (default: 3).

```
Recall: "how do I run tests" (2 matches)

1. [score: 13.0] mem:fact-01JDXYZ...
   Tests use cargo nextest, not cargo test
   Tags: testing

2. [score: 8.0] mem:fact-01JDABC...
   Integration tests use assert_cmd + predicates
   Tags: testing
```

## What BM25 rewards

BM25 scores a memory's content higher when:

- **Query terms appear** in the content.
- Those terms are **rare in the overall store** — a match on "postcard" beats a match on "the".
- The matched terms are in a **shorter** memory — density matters.
- Multiple distinct terms from the query match (not the same term repeated).

There are no embeddings, no semantic matching — just lexical overlap with smart weighting. If you mean "tests" but phrase it as "unit tests" or "testing", BM25 catches that because the stems overlap; it won't catch "QA" unless the content mentions it.

## Re-rank bonuses

After BM25 produces content scores, Fluree Memory adds small bonuses:

- **Tag hit**: +10 per tag that contains a query word.
- **Artifact ref hit**: +8 per ref path that contains a query word.
- **Kind word in query**: +6 if the query mentions the memory's kind ("constraint", "decision", etc.).
- **Branch match**: +3 if the memory was captured on the current git branch.
- **Recency**: +2 for memories <7 days old, +1 for <30 days.

If BM25 returns no hits, recall falls back to metadata-only scoring using these same bonuses so a well-tagged memory can still surface on a content miss.

## Filters

Filters narrow the candidate set *before* scoring:

```bash
# Only constraints tagged "errors"
fluree memory recall "handling" --kind constraint --tags errors

# Only repo-scoped memories
fluree memory recall "deployment" --scope repo

# Page through results
fluree memory recall "tests" --limit 10 --offset 10
```

Common filter recipes:

| You want… | Flags |
|---|---|
| Team-only (ignore personal) | `--scope repo` |
| Just the hard rules | `--kind constraint` |
| Just the decisions with reasoning | `--kind decision` |
| Pointers to code | `--kind fact --tags <domain>` (with `--refs`) |

## Output formats

```bash
fluree memory recall "tests"                 # text — for humans
fluree memory recall "tests" --format json   # JSON — for scripts
fluree memory recall "tests" --format context  # XML — for LLM injection
```

The `context` format produces a compact XML block designed to be pasted into an agent's context window:

```xml
<memory-context>
  <memory id="mem:fact-01JDXYZ..." kind="fact" score="13.0">
    <content>Tests use cargo nextest, not cargo test</content>
    <tags>testing</tags>
  </memory>
  <pagination shown="1" offset="0" total_in_store="13" />
</memory-context>
```

When results are cut off, the pagination element embeds a human-readable hint telling the agent how to get more:

```xml
<pagination shown="3" offset="0" limit="3" total_in_store="13">
  Results 1–3. Use offset=3 to retrieve more.
</pagination>
```

This pattern is why Fluree Memory is practical to use with an agent: a small, ranked slice goes into context, and the agent can ask for more if the top hits aren't enough.

## How this compares to other approaches

| Approach | Cost | Quality | Works offline |
|---|---|---|---|
| BM25 (Fluree Memory) | free, instant | high for keyword overlap | yes |
| Embedding search | paid + latency | high for paraphrase | usually no |
| Stuff-it-all-in-CLAUDE.md | free | context blow-up | yes |

For developer memory — where the agent knows the words for what it's looking for — BM25 is a very good fit. If you later want semantic recall, Fluree DB itself ships a [vector search](../../docs/indexing-and-search/vector-search.md) feature that the memory store could layer on.
