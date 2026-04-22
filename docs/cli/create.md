# fluree create

Create a new ledger.

## Usage

```bash
fluree create <LEDGER> [OPTIONS]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `<LEDGER>` | Name for the new ledger |

## Options

| Option | Description |
|--------|-------------|
| `--from <PATH>` | Import data from a file (Turtle or JSON-LD) |
| `--memory [PATH]` | Import memory history from a git-tracked `.fluree-memory/` directory. Defaults to the current repo if no path is given. Mutually exclusive with `--from`. |
| `--no-user` | Exclude user-scoped memories (`.local/user.ttl`) from `--memory` import |
| `--chunk-size-mb <MB>` | Chunk size in MB for splitting large Turtle files (0 = derive from memory budget). Only used when `--from` points to a `.ttl` file. |
| `--leaflet-rows <N>` | Rows per leaflet in the binary index (default: 25000). Larger values produce fewer, bigger leaflets — less I/O per scan, more memory per read. |
| `--leaflets-per-leaf <N>` | Leaflets per leaf file (default: 10). Larger values produce fewer leaf files — shallower tree, bigger reads. |

**Global flags** that affect bulk import when using `--from` (see [CLI README](README.md#global-options)):

- `--memory-budget-mb <MB>` — Memory budget in MB (0 = auto: 75% of system RAM). Drives chunk size, concurrency, and indexer run budget.
- `--parallelism <N>` — Number of parallel parse threads (0 = auto: system cores, cap 6).

## Description

Creates a new empty ledger with the given name and sets it as the active ledger. The ledger is stored in `.fluree/storage/`.

Use `--from` to create a ledger pre-populated with data from a Turtle or JSON-LD file. For large Turtle files, the CLI splits work into chunks and runs parallel parse threads; tune with `--memory-budget-mb` and `--parallelism` if needed.

Use `--memory` to import your project's developer memory history into a time-travel-capable Fluree ledger. Each git commit that touched `.fluree-memory/repo.ttl` (and `.local/user.ttl` unless `--no-user` is set) becomes a Fluree transaction. The git commit message, SHA, and author date are stored as transaction metadata, so you can correlate Fluree `t` values with git history.

## Examples

```bash
# Create an empty ledger
fluree create mydb

# Create with initial data
fluree create mydb --from seed-data.ttl

# Create from JSON-LD
fluree create mydb --from initial.jsonld

# Create with explicit memory and parallelism for a large Turtle file
fluree create mydb --from large.ttl --memory-budget-mb 4096 --parallelism 8

# Import memory history from the current repo
fluree create memories --memory

# Import memory history from another repo, excluding user memories
fluree create memories --memory /path/to/other/repo --no-user
```

## Output

```
Created ledger 'mydb'
Set 'mydb' as active ledger
```

With `--from`:
```
Created ledger 'mydb'
Committed t=1 (42 flakes)
Set 'mydb' as active ledger
```

With `--memory`:
```
Created ledger 'memories' with 42 commits (t=1..43)
  Earliest: bf803255 — initial memory store
  Latest:   9865e5cd — prevent overrides of fluree txn-meta

Query with time travel:
  fluree query memories 'SELECT ?id ?content WHERE { ?id a mem:Fact ; mem:content ?content } LIMIT 5'
  fluree query memories --at-t 2 'SELECT ...'   # state at first commit
```

## See Also

- [list](list.md) - List all ledgers
- [use](use.md) - Switch active ledger
- [drop](drop.md) - Delete a ledger
