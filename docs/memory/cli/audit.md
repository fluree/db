# fluree memory audit

Audit the store against the [hygiene rubric](../guides/hygiene-and-auditing.md). Read-only — it reports, you decide.

```bash
fluree memory audit [OPTIONS]
```

## Options

| Option | Description |
|---|---|
| `--all` | Audit every memory instead of just this branch's |
| `--base <REF>` | Base ref the branch is compared against (default: `main`) |
| `--format <FMT>` | `text` (default) or `json` |

By default the audit is branch-scoped: memories captured on the current branch, plus memories from any branch that reference a file this branch changed — the ones this branch can invalidate.

## Example

```bash
fluree memory audit --base main
```

Output:

```
## Memory audit — branch `feat/iceberg` vs `main` (12 of 214 memories in scope)
T1 durable — true at HEAD, not progress or status.
T2 shared — any contributor is the audience; no personal or session state.
T3 non-derivable — beats grep, git log, and the docs.
T4 actionable — a reader does something differently.
T5 well-formed — one insight, within 750 chars, refs that resolve, lowercase tags.
Act with memory_update (same id; an update that changes no field means "re-verified at HEAD"), memory_forget, or memory_add. Flags are signals, not verdicts — read the memory first.

### Memories — 3 of 12 flagged, 12 listed (flagged first)
- mem:fact-01JDXYZ... [narration:SHIPPED,iso-date] refs: 2 ok
- mem:fact-01JDABC... refs: 1 ok, MISSING src/moved.rs
- mem:decision-01JDDEF... refs: 3 ok, 1 churned since last verified (fluree-db-query/src/join.rs 2026-07-29)

### Changed on this branch with no memory coverage — capture only what code/docs can't show:
- fluree-db-iceberg/src/manifest.rs
```

## What the flags mean

| Flag | Signal |
|---|---|
| `over-cap:N` | Content exceeds the 750-character cap — split or tighten it |
| `narration:...` | Progress/status language (`SHIPPED`, `awaiting`, a PR number, an ISO date) — this reads as news about an effort, not a fact about the code |
| `portability:...` | An absolute path or a person-ish `@handle` — it won't travel to another contributor's checkout |
| `tags:...` | A tag that isn't a lowercase single-word recall key |
| `MISSING <path>` | A ref that no longer resolves at HEAD |
| `N churned since last verified` | The ref still resolves, but its file has commits newer than the memory's last write or re-verification — the claim may have rotted |

Flags are mechanical signals, not verdicts. The five tests in the header are the actual standard, and applying them is a judgment call the audit deliberately leaves to you.

## Clearing a churn flag

Churn compares each ref's last commit against the memory's `updatedAt` (or `createdAt` if it was never updated). On a store that has never been audited, most memories flag — nothing has been re-verified yet. Read the memory, check the claim against HEAD, and then:

```bash
fluree memory update mem:fact-01JDXYZ...
```

An update that changes no field still stamps `updatedAt`, which is exactly how you record "I re-verified this and it still holds". If the claim no longer holds, `--text` it into shape or [`forget`](forget.md) it.

## See also

- [Memory hygiene and auditing](../guides/hygiene-and-auditing.md) — the five tests and the full audit procedure
- [`update`](update.md) — fix or re-verify a memory
- [`forget`](forget.md) — retract one with no durable core
