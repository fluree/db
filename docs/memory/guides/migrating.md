# Migrating from plain-markdown memory

Many teams start with one big markdown file that their AI tool reads on every session — `CLAUDE.md`, `AGENTS.md`, `.cursorrules`, `.windsurfrules`, or a section in `README.md`. These files work until they don't: they bloat context, mix levels (architectural rules next to "the CI flag is --all-features"), and rot silently.

Here's a pragmatic migration from that world to structured memory.

## Phase 1: leave the markdown alone

You don't have to delete anything to start using Fluree Memory. Add memories for **new** things you learn while keeping the old file around. After a week or two of active use, you'll have a sense of which things belong where.

```bash
fluree memory init
# ...work, capture things as they come up...
fluree memory add --kind constraint --severity must \
  --text "All public fns must have doc comments" --tags code-style
```

## Phase 2: categorize the markdown file

Open the old file and go paragraph by paragraph. For each chunk, ask:

| Chunk type | Where it goes |
|---|---|
| High-level overview / architecture prose | Stays in markdown (README, ARCHITECTURE.md) |
| Rules ("do this", "don't do that") | → `constraint` memories with `--severity` |
| Choices + reasoning | → `decision` memories with `--rationale` |
| Named quirks / gotchas | → `fact` memories |
| "Look here for X" | → `fact` memories with `--refs` |
| Personal preferences | → `fact` memories (`--scope user` usually) |

The markdown file that's left after this should be genuinely about framing — the 30-second project tour — not a knowledge base.

## Phase 3: move the categorized chunks

Turn each chunk into a `memory add` call. Tag consistently so things group later:

```bash
fluree memory add --kind constraint --severity must \
  --text "Never commit secrets; use environment variables" \
  --tags security,secrets

fluree memory add --kind decision \
  --text "Use postcard for index encoding" \
  --rationale "no_std compatible, smaller than bincode" \
  --alternatives "bincode, CBOR, MessagePack" \
  --refs fluree-db-indexer/ \
  --tags indexer,encoding

fluree memory add --kind fact \
  --text "Error pattern defined here" \
  --refs fluree-db-core/src/error.rs \
  --tags errors
```

If you want to script it, pipe content into `fluree memory add` on stdin (with `--kind` / `--tags` set per-line). `add` reads stdin when `--text` is omitted:

```bash
echo "The index format uses postcard encoding" \
  | fluree memory add --kind fact --tags indexer
```

## Phase 4: trim the old file

Once the chunks are in memory, delete them from the markdown. What's left is your high-level orientation doc, which is fine.

**Leave a pointer at the top:**

```markdown
> Detailed conventions, rules, and decisions are in Fluree Memory.
> Use `memory_recall` from an MCP-enabled IDE, or `fluree memory recall "..."` from the shell.
```

## Phase 5: review

Run `fluree memory status` and `fluree memory recall "" -n 50` to eyeball everything. Look for:

- Duplicates — memories that say nearly the same thing with different wording.
- Mis-categorized kinds — a "decision" with no rationale is really a `fact`.
- Over-long content — memories should be paragraphs at most, not pages. Break up if needed.

## Why this is worth doing

| Plain markdown | Structured memory |
|---|---|
| Entire file loaded every session | Only relevant matches loaded |
| No filtering | Filter by kind, tag, scope |
| No history | Full history via `git log -p` |
| Hard to share a slice | `export` + `jq` / curated `recall` |
| Drifts silently | `status` visibility + curation flow |

You get a knowledge base the team can actually maintain — and that costs fewer tokens per session than the markdown file it replaces.
