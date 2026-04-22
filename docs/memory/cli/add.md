# fluree memory add

Store a new memory.

```bash
fluree memory add [OPTIONS]
```

## Options

| Option | Description |
|---|---|
| `--kind <KIND>` | `fact` (default), `decision`, `constraint` |
| `--text <TEXT>` | Content text (or provide via stdin) |
| `--tags <T1,T2>` | **Required.** Comma-separated tags — the primary recall signal |
| `--refs <R1,R2>` | Comma-separated file/artifact references |
| `--severity <SEV>` | For constraints: `must`, `should`, `prefer` |
| `--scope <SCOPE>` | `repo` (default) or `user` |
| `--rationale <TEXT>` | Why — the reasoning behind this memory (any kind) |
| `--alternatives <TEXT>` | Alternatives considered (any kind) |
| `--format <FMT>` | `text` (default) or `json` |

## Examples

```bash
# A simple fact
fluree memory add --kind fact \
  --text "Tests use cargo nextest" \
  --tags testing,cargo

# A hard constraint with rationale
fluree memory add --kind constraint \
  --text "Never suppress dead code with an underscore prefix" \
  --tags code-style \
  --severity must \
  --rationale "Underscore-prefixed names hide code from future discovery"

# From stdin (useful for piping from other tools)
echo "The index format uses postcard encoding" \
  | fluree memory add --kind fact --tags indexer

# A decision with full context
fluree memory add --kind decision \
  --text "Use postcard for compact index encoding" \
  --rationale "no_std compatible, smaller output than bincode" \
  --alternatives "bincode, CBOR, MessagePack" \
  --refs fluree-db-indexer/

# A fact pointing to a file (use --refs for artifact pointers)
fluree memory add --kind fact \
  --text "Error pattern defined here" \
  --refs fluree-db-core/src/error.rs \
  --tags errors

# A personal convention, user-scoped
fluree memory add --kind fact \
  --text "Always run clippy with --all-features" \
  --scope user \
  --tags code-style
```

## Output

Default (`text`):

```
Stored memory: mem:fact-01JDXYZ5A2B3C4D5E6F7G8H9J0
```

`json`:

```json
{
  "id": "mem:fact-01JDXYZ5A2B3C4D5E6F7G8H9J0",
  "kind": "fact",
  "scope": "repo",
  "created_at": "2026-04-14T16:45:12Z"
}
```

## Secret detection

If the content matches a known secret pattern (AWS keys, GitHub tokens, password-bearing URLs, etc.), the sensitive portions are replaced with `[REDACTED]` before storage and a warning is printed. See [Secrets and sensitivity](../concepts/secrets-and-sensitivity.md).

## Scope and file placement

| `--scope repo` (default) | Writes to `.fluree-memory/repo.ttl` — committable |
| `--scope user` | Writes to `.fluree-memory/.local/user.ttl` — gitignored |

See [Repo vs user memory](../concepts/repo-vs-user.md).

## See also

- [`recall`](recall.md) — search stored memories
- [`update`](update.md) — update an existing memory in place
- [What is a memory?](../concepts/what-is-a-memory.md) — choosing the right kind
