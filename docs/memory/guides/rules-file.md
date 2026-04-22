# Customizing the rules file

When you run `fluree memory mcp-install`, a short "rules file" gets written alongside the MCP server config. This file tells your AI tool *when and how* to use the memory tools — things the tool definitions alone don't express.

## Where it lives

| IDE | Rules file |
|---|---|
| Claude Code | Section appended to `<repo>/CLAUDE.md` |
| Cursor | `<repo>/.cursor/rules/fluree_rules.md` |
| VS Code | `<repo>/.vscode/fluree_rules.md` |
| Windsurf | Not written — add your own guidance to Windsurf's memory / rules UI |
| Zed | Not written — add your own guidance via Zed's assistant settings |

The canonical source for the default text lives in `fluree-db-memory/rules/fluree_rules.md` in the repo; the Cursor and VS Code installers copy it verbatim. The Claude Code installer appends a short variant directly to `CLAUDE.md`. Windsurf and Zed don't have a conventional per-project rules-file slot that `mcp-install` targets automatically — the paragraph below is a reasonable starting point if you want to paste one in yourself.

## What the default says

A minimal set of instructions along these lines:

> **Before starting a task:** call `memory_recall` with a query describing what you're about to do. Review the top matches for constraints, decisions, and relevant facts.
>
> **After learning something reusable:** call `memory_add` with the appropriate kind:
> - `fact` — verifiable truths about the codebase (use `--refs` for file pointers)
> - `decision` — choices with rationale (use `--rationale`)
> - `constraint` — rules with severity (use `--severity must/should/prefer`)
>
> **Don't re-ask the user** for things that are already in memory.

## Customizing

Edit the file freely. Common tweaks:

- **Add domain-specific guidance**: "When working on the indexer, always recall with the `indexer` tag first."
- **Tighten the defaults**: "Only call `memory_add` for memories that will apply in future sessions — not for task-specific scratch."
- **Shape the kinds**: "Use `fact` with `--refs` when the memory is really a pointer to a file or symbol."

## Reloading

- **Cursor / VS Code**: reload the window after editing.
- **Claude Code**: appending to `CLAUDE.md` takes effect on the next session.
- **Zed**: agent reads settings on connection — reload.

## Keeping team customizations shared

If you edit the rules file and like what you got, commit it. Teammates get your tuning automatically on their next pull. The rules file is just markdown — treat it like any other piece of team guidance.
