# Fluree Developer Memory — Agent Rules

You have access to a persistent project memory system via MCP tools. Use it to maintain context across sessions.

## When to Recall

Call `memory_recall` at the start of each task with a query describing what you're about to do. This surfaces relevant facts, decisions, constraints, and preferences from previous sessions.

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
| `preference` | The user prefers something done a certain way | "Use thiserror for error types, not anyhow" |
| `artifact` | An important file or resource | "fluree-db-core/src/error.rs defines the base error pattern for all crates" |

## Tag Conventions

Use consistent, lowercase tags. Common tags:
- Module names: `indexer`, `query`, `transact`, `api`, `cli`, `memory`
- Topics: `testing`, `errors`, `performance`, `storage`, `schema`
- Actions: `debugging`, `refactoring`, `migration`

## When to Update

Call `memory_update` when a previously stored fact or decision changes. This creates a new version that supersedes the old one, preserving audit history.

## When to Forget

Call `memory_forget` only when a memory is clearly incorrect or permanently obsolete. Prefer `memory_update` for evolving information.

## Severity for Constraints

When storing constraints, set severity:
- `must` — Violation is a bug. Example: "Must use thiserror, not anyhow"
- `should` — Strong preference. Example: "Should keep functions under 50 lines"
- `prefer` — Soft preference. Example: "Prefer impl Trait over Box<dyn>"
