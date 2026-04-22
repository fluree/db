# fluree memory recall

Search and retrieve relevant memories ranked by BM25 score.

```bash
fluree memory recall <QUERY> [OPTIONS]
```

## Arguments

| Argument | Description |
|---|---|
| `<QUERY>` | Natural-language search query (keyword-matched, not semantic) |

## Options

| Option | Description |
|---|---|
| `-n`, `--limit <N>` | Max results per page (default: 3) |
| `--offset <N>` | Skip the first N results — use for pagination (default: 0) |
| `--kind <KIND>` | Filter to a specific memory kind |
| `--tags <T1,T2>` | Filter to memories with these tags |
| `--scope <SCOPE>` | Filter by `repo` or `user` |
| `--format <FMT>` | `text` (default), `json`, or `context` (XML for LLM) |

## Examples

```bash
# Basic recall — returns top 3
fluree memory recall "how to run tests"

# Page through a longer result set
fluree memory recall "how to run tests" --offset 3
fluree memory recall "error handling" -n 10

# Narrow with filters
fluree memory recall "error handling" --kind constraint --tags errors
fluree memory recall "deployment" --scope repo

# XML output designed for LLM context injection
fluree memory recall "testing patterns" --format context
```

## Output

### `text`

```
Recall: "how to run tests" (2 matches)

1. [score: 13.0] mem:fact-01JDXYZ...
   Tests use cargo nextest
   Tags: testing, cargo

2. [score: 8.0] mem:fact-01JDABC...
   Integration tests use assert_cmd + predicates
   Tags: testing

  (showing results 1–3; use --offset 3 for more)
```

### `json`

```json
{
  "query": "how to run tests",
  "memories": [
    {
      "memory": {
        "id": "mem:fact-01JDXYZ...",
        "kind": "fact",
        "content": "Tests use cargo nextest",
        "tags": ["testing", "cargo"],
        "scope": "repo",
        "created_at": "2026-02-22T14:00:00Z"
      },
      "score": 13.0
    }
  ],
  "total_count": 13
}
```

`total_count` is the total number of memories in the store, not the number of matches — useful for UI context but not for pagination math.

### `context` (XML for LLM injection)

```xml
<memory-context>
  <memory id="mem:fact-01JDXYZ..." kind="fact" score="13.0">
    <content>Tests use cargo nextest</content>
    <tags>testing, cargo</tags>
  </memory>
  <pagination shown="1" offset="0" total_in_store="13" />
</memory-context>
```

When results are cut off, the pagination element embeds a hint:

```xml
<pagination shown="3" offset="0" limit="3" total_in_store="13">
  Results 1–3. Use offset=3 to retrieve more.
</pagination>
```

## How ranking works

See [Recall and ranking](../concepts/recall-and-ranking.md) for the full story — BM25 over content plus metadata bonuses for tag, ref, kind, branch, and recency matches. Filters (`--kind`, `--tags`, `--scope`) narrow the candidate set. All local, deterministic, and offline.

## See also

- [`add`](add.md) — store a new memory
- [`status`](status.md) — store summary
- [Recall and ranking](../concepts/recall-and-ranking.md)
