# Fluree Developer Memory — Agent Rules

You have access to a persistent project memory system via MCP tools. Use it to maintain context across sessions.

## When to Recall

Call `memory_recall` at the start of each task with a query describing what you're about to do. This surfaces relevant facts, decisions, and constraints from previous sessions.

Examples:
- Starting a new feature: `memory_recall("building <feature name>")`
- Debugging: `memory_recall("error in <module> <symptom>")`
- Refactoring: `memory_recall("architecture of <component>")`

## When to Store

Call `memory_add` when you discover or decide something worth remembering:

| Kind | When to use | Example |
|------|------------|---------|
| `fact` | You learn how something works | "The index format uses postcard encoding with delta compression" |
| `decision` | A design choice is made (with rationale) | "Chose keyword matching over embeddings for Phase 1 to avoid cloud dependency" |
| `constraint` | A rule that must always be followed | "Never suppress dead code warnings with underscore prefix" |

Use `rationale` on any kind to explain *why* something is true, was decided, or must be followed.

## Tag Conventions

**Tags are required.** Every memory must include at least one tag. Tags are the primary recall signal — memories without tags are hard to surface later.

Use consistent, lowercase tags. Common tags:
- Module names: `indexer`, `query`, `transact`, `api`, `cli`, `memory`
- Topics: `testing`, `errors`, `performance`, `storage`, `schema`
- Actions: `debugging`, `refactoring`, `migration`

## When to Update

Call `memory_update` when a previously stored fact or decision changes. Only provide the fields you want to change — the memory keeps its ID. History is tracked via git.

## When to Forget

Call `memory_forget` only when a memory is clearly incorrect or permanently obsolete. Prefer `memory_update` for evolving information.

## Severity for Constraints

When storing constraints, set severity:
- `must` — Violation is a bug. Example: "Must use thiserror, not anyhow"
- `should` — Strong preference. Example: "Should keep functions under 50 lines"
- `prefer` — Soft preference. Example: "Prefer impl Trait over Box<dyn>"

## What NOT to Store (repo scope)

Repo-scoped memories are a shared team asset committed to git. Before `memory_add` with repo scope, check the content against these rules — they are enforced by review and by a lint in CI:

- **No progress or status.** "SHIPPED as PR #N", "awaiting review", "next step", branch names, commit hashes, dates-as-status. Git already records what happened when. Store the durable *residue* of the work — the invariant, the gotcha, the decision rationale — phrased as a fact about the code, not news about the effort.
- **No session or personal state.** Hand-off notes, resume pointers, schedules, named-person state, absolute paths into anyone's home directory. Use `--scope user` for personal workflow notes.
- **Nothing grep or git-log answers.** Diff narration ("added X to Y") and restatements of what a file plainly shows dilute recall ranking for every other memory.
- **Refs must resolve.** `refs` are repo-relative paths that exist at HEAD. When code moves, `memory_update` the ref.
- **One insight, within the cap.** Split bundles; tighten prose. Tags are lowercase keywords from stable vocabulary (crate/module names, topics) — not effort codenames.

When an effort ends, ask: "what did we learn that the code and docs can't show?" Store only that. See `docs/memory/guides/hygiene-and-auditing.md` for the full rubric and the audit procedure.
