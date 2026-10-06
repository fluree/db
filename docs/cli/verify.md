# fluree verify

Verify a ledger's commit chain and the objects it references.

## Usage

```bash
fluree verify [LEDGER] [OPTIONS]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `[LEDGER]` | Ledger name (defaults to active ledger) |

## Options

| Option | Description |
|--------|-------------|
| `--limit <N>` | Stop after checking N commits, newest first — the walk follows the primary-parent lineage before any merge lineage |
| `--json` | Emit the report as JSON |

## Description

Walks the commit DAG from the head and checks that:

- every commit blob exists and decodes;
- every parent named by a commit exists (a missing parent breaks the chain);
- `t` is contiguous along primary-parent edges;
- every raw-transaction blob referenced by a commit (`store_raw_txn`) exists;
- the index root the nameservice points at exists.

The command is read-only.

A **missing txn blob** does not affect ledger state — flakes live in the
commit itself — but it means provenance for that commit is lost. Replication
paths (`clone`, `pack`, `push`, `merge`) tolerate the gap with a warning; this
command is how you find out the gap exists and which commit carries it.

## Exit codes

Automation can gate on *what* broke, not just that something did — a
provenance gap still clones, a broken chain does not.

| Code | Meaning |
|------|---------|
| `0` | No problems found |
| `1` | Verification could not run (ledger not found, bad `--limit`, storage error) |
| `3` | Provenance problems only — a missing txn blob. State and every replication path are intact |
| `4` | The commit chain or the index root is broken — a missing/unreadable commit, a `t` gap, or a missing index root |

```bash
# Clone only if replication paths will work; a provenance gap is not a blocker.
fluree verify production; [ $? -lt 4 ] && fluree clone ...
```

With `--json`, the same classification is on the report as
`"severity": "healthy" | "provenance" | "chain"`.

## Examples

```bash
# Verify the active ledger
fluree verify

# Verify a specific ledger, machine-readable
fluree verify production --json

# Spot-check only the newest 1000 commits of a long chain
fluree verify production --limit 1000
```

## Output

```
Ledger:   production:main
Head:     t=124 bagaybqabciq...
Index:    t=124 baghybqabciq...
Checked:  124 commit(s), 124 txn reference(s)
Result:   1 problem(s)
  - missing txn blob bagbibqabciq... referenced by commit t=47 bagaybqabciq...; state is intact, provenance for this commit is lost
```

Exit code `3` here: provenance only.


## See Also

- [log](log.md) - Show commit log
- [info](info.md) - Show ledger details
- [clone](clone.md) - Clone a ledger from a remote
