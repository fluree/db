# Repo vs user memory

Fluree Memory has two scopes, and they live in separate files:

| Scope | File | Git | Visible to |
|---|---|---|---|
| **repo** | `.fluree-memory/repo.ttl` | ✅ commit it | the whole team |
| **user** | `.fluree-memory/.local/user.ttl` | ❌ gitignored | just you |

Scope is set at write time (`--scope repo` or `--scope user`) and defaults to `repo`. Once set, it determines which TTL file the memory is written to.

## Layout

After `fluree memory init` inside a project:

```
my-project/
├── .fluree/                      # Fluree DB storage for the __memory ledger
├── .fluree-memory/
│   ├── .gitignore                # contents: ".local/"
│   ├── repo.ttl                  # team memories — COMMIT THIS
│   └── .local/                   # ignored by the .gitignore above
│       ├── user.ttl              # your personal memories
│       ├── mcp.log               # MCP server log
│       └── build-hash            # content hash used to detect external TTL edits
└── (your code)
```

The `.fluree-memory/.gitignore` is written by `init` and handles the split for you. Commit the whole `.fluree-memory/` directory; git will skip `.local/` automatically.

## When to use which

**Repo scope (default):**
- Facts about the codebase ("tests use cargo nextest")
- Team decisions with rationale
- Constraints everyone must follow
- File/symbol pointers via `--refs` ("X lives at Y")

**User scope:**
- Your IDE quirks
- Personal conventions the team hasn't agreed on
- Scratch notes while you're exploring
- Anything you'd be embarrassed to commit

## Changing scope after the fact

You can't move a memory between scopes directly. If you stored something as repo that should be user-only:

```bash
fluree memory forget <id>
fluree memory add --scope user --kind <kind> --text "..."
```

## Recall sees both

By default, `fluree memory recall` and the `memory_recall` MCP tool return matches from **both** scopes — your personal notes and the team's are merged in the result set. Filter with `--scope repo` or `--scope user` if you need to isolate one.

## Sharing with the team

Memory becomes a shared asset as soon as you commit `.fluree-memory/repo.ttl`. A teammate who clones the repo and runs `fluree memory init` gets the ledger populated from the committed TTL automatically — no manual import step.

Conflicts on `repo.ttl` resolve like any other text file. TTL is line-oriented per-triple, so most merges are clean; occasionally you'll see a merge mark in the middle of a memory's fields and need to pick one side.

See [Team workflows](../guides/team-workflows.md) for the full story.
