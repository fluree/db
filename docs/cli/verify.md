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
| `--limit <N>` | Stop after checking N commits (newest first) |
| `--json` | Emit the report as JSON |

## Description

Walks the commit DAG from the head and checks that:

- every commit blob exists and decodes;
- every parent named by a commit exists (a missing parent breaks the chain);
- `t` is contiguous along primary-parent edges;
- every raw-transaction blob referenced by a commit (`store_raw_txn`) exists;
- the index root the nameservice points at exists.

The command is read-only. It exits non-zero when any problem is found, so it
can gate automation (backups, clones, promotions).

A **missing txn blob** does not affect ledger state — flakes live in the
commit itself — but it means provenance for that commit is lost. Replication
paths (`clone`, `pack`, `push`, `merge`) tolerate the gap with a warning; this
command is how you find out the gap exists and which commit carries it.

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

## See Also

- [log](log.md) - Show commit log
- [info](info.md) - Show ledger details
- [clone](clone.md) - Clone a ledger from a remote
