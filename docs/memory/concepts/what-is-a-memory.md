# What is a memory?

A memory is a single structured record of something worth remembering about a project. Every memory has:

- **Content** — the text itself ("Tests use cargo nextest, not cargo test")
- **Kind** — what *sort* of thing it is
- **Tags** — free-form keywords for filtering
- **Scope** — repo (shared) or user (yours)
- **Refs** — optional file or artifact pointers
- **Timestamps** — when it was created

Everything else (severity, rationale, alternatives) is optional metadata that can appear on any kind.

## The three kinds

Memories are typed. The kind tells future-you (and future-agents) how to interpret the content.

### `fact`

Something that is objectively true about the project.

> "The indexer uses postcard encoding for on-disk format."
> "We run PostgreSQL 16 in production."
> "The BM25 code lives in `fluree-db-indexer/src/bm25.rs`."
> "Error pattern defined here -> `fluree-db-core/src/error.rs`"

Use facts liberally. They're the default and make up the bulk of a typical memory store. Use tags to categorize them (e.g. `architecture`, `dependency`, `configuration`). Facts can carry `--rationale` and `--alternatives` when you want to explain *why* something is the way it is.

### `decision`

A choice the team made, ideally with *why* and *what was considered*.

> "Use postcard for compact index encoding. **Why:** no_std compatible, smaller than bincode. **Alternatives:** bincode, CBOR, MessagePack."

Decisions are what distinguishes a project with institutional knowledge from one where people keep re-litigating settled choices. Capture them with `--rationale` and `--alternatives`:

```bash
fluree memory add --kind decision \
  --text "Use postcard for compact index encoding" \
  --rationale "no_std compatible, smaller output than bincode" \
  --alternatives "bincode, CBOR, MessagePack" \
  --refs fluree-db-indexer/
```

### `constraint`

A rule — something that *must*, *should*, or is *preferred*. Constraints carry a severity.

> **must** "Never commit secrets; use environment variables."
> **should** "Integration tests run in a real Postgres, not SQLite."
> **prefer** "Name errors with the module prefix (`QueryError`, not `Error`)."

```bash
fluree memory add --kind constraint \
  --text "Never suppress dead code with _underscore prefix; delete it" \
  --severity must \
  --tags code-style \
  --rationale "Underscore-prefixed names hide code from future discovery"
```

When an agent is about to do something, constraints are the first thing it should recall. Like facts and decisions, constraints can carry `--rationale` and `--alternatives` to explain the reasoning behind the rule.

## Which kind should I use?

| You have... | Use kind |
|---|---|
| A verifiable truth | `fact` |
| A choice and its reasoning | `decision` |
| A rule that must/should be followed | `constraint` |
| A pointer to code / a file | `fact` (with `--refs`) |
| A soft taste or convention | `fact` or `constraint --severity prefer` |

When in doubt: `fact`. The kind can always be refined later via `update`. All three kinds support `--rationale` and `--alternatives` for capturing the *why*.
