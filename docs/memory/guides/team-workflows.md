# Team workflows: sharing memory via git

The whole point of `repo.ttl` is that memory becomes a team asset — captured once by whoever learns it, available to every teammate and every AI agent forever.

## The happy path

1. **Someone runs `fluree memory init`** in the repo and commits `.fluree-memory/` (minus the gitignored `.local/`).
2. **Teammates pull** and run `fluree memory init` once. The init picks up the committed `repo.ttl` and populates the ledger from it. No manual import.
3. **As people add memories**, `.fluree-memory/repo.ttl` changes in the working tree. Commit it like any other file.
4. **Pulls bring in new memories automatically** — `fluree memory recall` (and the MCP server) read the ledger, which stays in sync with the TTL file.

That's it. No server, no sync daemon, no API tokens. Git is the sync mechanism.

## What to commit

✅ Commit:
- `.fluree-memory/repo.ttl`
- `.fluree-memory/.gitignore`
- Any IDE config MCP-install created: `.cursor/mcp.json`, `.cursor/rules/fluree_rules.md`, `.vscode/mcp.json`, `.vscode/fluree_rules.md`, `.zed/settings.json`

❌ Don't commit (the `.fluree-memory/.gitignore` already handles this):
- `.fluree-memory/.local/user.ttl` — your personal memories
- `.fluree-memory/.local/mcp.log` — noisy and personal
- `.fluree/` — the Fluree storage dir (can be re-hydrated from `repo.ttl`)

## Reviewing memory in PRs

Treat `repo.ttl` changes like documentation changes in code review:

- **New memory?** Is the kind right? Is the wording accurate? Are the tags useful?
- **Updated memory?** Is the new content better (not just different)?
- **Forgot memory?** Was that really wrong, or should it have been updated instead?

Memories are serialized as subject blocks with one predicate per line, so most diffs are readable.

## Merge conflicts

Memories in `repo.ttl` are sorted by `(branch, id)` — memories from the same git branch cluster together, and different branches land in different regions of the file. This means two feature branches that each add memories will almost never conflict, because their blocks insert at different positions in the file.

The branch name is captured automatically when a memory is created, so memories from `feature/auth` sort separately from memories created on `feature/indexer`. Within each branch group, memories are ordered chronologically (ULID encodes creation time).

When conflicts do occur, they're usually because two branches modified the same existing memory (via `update`) or both worked on the same branch. These are typically clean to resolve:

```ttl
<<<<<<< HEAD
mem:fact-01JD... a mem:Fact ;
    mem:content "Tests use cargo nextest" ;
    mem:tag "cargo" ;
    mem:tag "testing" ;
    ...
=======
mem:fact-01JD... a mem:Fact ;
    mem:content "Tests use cargo nextest with --no-fail-fast" ;
    mem:tag "testing" ;
    ...
>>>>>>> their-branch
```

Pick the version you want or combine them, then re-run `fluree memory status` to make sure the store parses cleanly. If the merged file is genuinely messy, a cleaner path is to accept one side wholesale and then apply the other side's changes via `fluree memory add` / `update` on top.

## Onboarding a new teammate

When someone new clones the repo:

```bash
git clone git@github.com:team/project
cd project
fluree memory init
```

After init, they can immediately:

```bash
fluree memory recall "testing" -n 10
```

…and get everything the team has captured. No setup beyond installing `fluree`.

## Going further

- Keep PR review short by tagging memories with their **domain** (`auth`, `indexer`, `docs`, etc.) so reviewers can filter.
- Use `constraint --severity must` sparingly — they're the "policy layer" of memory. Prefer `should` or `prefer` for taste.
- Periodically `fluree memory status` and prune stale memories with `forget`. The store should feel curated, not a dumping ground.
