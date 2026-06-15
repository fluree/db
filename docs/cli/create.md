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
| `--from <PATH>` | Import data from a file (Turtle, N-Triples, N-Quads, TriG, or JSON-LD), optionally `.gz`- or `.zst`-compressed. N-Triples (`.nt`) parses as Turtle; N-Quads (`.nq`) converts to TriG (named graphs supported). A `.flpack` archive (see [export](export.md)) is restored wholesale instead — full ledger including its prebuilt index. |
| `--remote <NAME>` | Create on a remote server instead of locally. With no `--from`, creates an empty ledger. With `--from <archive>.flpack`, streams the archive to the server's import endpoint to restore the ledger remotely. Other `--from` formats are not supported remotely — export to `.flpack` first, or create locally then [publish](publish.md). |
| `--memory [PATH]` | Import memory history from a git-tracked `.fluree-memory/` directory. Defaults to the current repo if no path is given. Mutually exclusive with `--from`. |
| `--no-user` | Exclude user-scoped memories (`.local/user.ttl`) from `--memory` import |
| `--chunk-size-mb <MB>` | Chunk size in MB for splitting large Turtle files (0 = derive from memory budget). Only used when `--from` points to a `.ttl` or `.nt` file. |
| `--leaflet-rows <N>` | Rows per leaflet in the binary index (default: 25000). Larger values produce fewer, bigger leaflets — less I/O per scan, more memory per read. |
| `--leaflets-per-leaf <N>` | Leaflets per leaf file (default: 10). Larger values produce fewer leaf files — shallower tree, bigger reads. |

**Global flags** that affect bulk import when using `--from` (see [CLI README](README.md#global-options)):

- `--memory-budget-mb <MB>` — Memory budget in MB (0 = auto: 60% of system RAM). Drives chunk size, concurrency, and indexer run budget. Set this to cap how much memory the import uses; auto-detected thread count shrinks to fit it.
- `--parallelism <N>` — Number of parallel parse threads (0 = auto: most logical cores, capped to fit the memory budget; explicit values honored as-is, floored at 1).

## Description

Creates a new empty ledger with the given name and sets it as the active ledger. The ledger is stored in `.fluree/storage/`.

Use `--from` to create a ledger pre-populated with data from a Turtle, N-Triples, N-Quads, TriG, or JSON-LD file (or a directory of same-format files). Any input may be gzip- or zstd-compressed and is decoded transparently (`data.ttl.gz`, `dump.nq.zst`, mixed directories — the underlying RDF extension classifies the file). N-Triples (`.nt`) is a strict subset of Turtle and is parsed by the same parser. N-Quads (`.nq`) and TriG (`.trig`) support named graphs — queryable after import via the `#<graph-iri>` fragment. For large Turtle/N-Triples files (including `.ttl.gz`/`.nt.gz`), the CLI splits work into chunks and runs parallel parse threads — though compressed inputs decode single-threaded; TriG/N-Quads/JSON-LD use a serial path. Tune with `--memory-budget-mb` and `--parallelism` if needed.

**Directory imports (`.ttl`/`.nt`)** are *rechunked by bytes* rather than one-chunk-per-file: large files are sub-split at statement boundaries and many small files are coalesced into `~chunk_size` work items. This keeps the import fully parallel and bounds the number of commits and sorted index runs regardless of how the data is packaged — so a directory of one big file, or of hundreds of tiny shards, both import at full speed. Coalescing engages automatically once a directory holds more than 64 sub-`chunk_size` files; a file containing a labeled blank node (`_:`) or an `@base` directive is never coalesced (it would change RDF document scope) and is imported as its own chunk. Set `FLUREE_IMPORT_COALESCE_THRESHOLD=<n>` to change the gate (`0` disables coalescing — every file becomes its own commit, the legacy behavior). Directories containing any `.trig`/`.nq`/`.jsonld` continue to use the per-file serial path.

Use `--memory` to import your project's developer memory history into a time-travel-capable Fluree ledger. Each git commit that touched `.fluree-memory/repo.ttl` (and `.local/user.ttl` unless `--no-user` is set) becomes a Fluree transaction. The git commit message, SHA, and author date are stored as transaction metadata, so you can correlate Fluree `t` values with git history.

### Restoring from a `.flpack` archive

When `--from` points at a `.flpack` file (produced by `fluree export <ledger> --format ledger`), the ledger is restored *wholesale* rather than bulk-imported: every commit, transaction blob, and prebuilt index artifact is streamed straight into storage and the heads are set from the archive — the restored ledger is byte-for-byte identical and immediately queryable, under whatever name you choose.

Add `--remote <name>` to restore onto a server instead of locally; the archive streams to the server's import endpoint, so no local staging instance is needed. This makes `.flpack` the universal way to move any data onto a server — build a ledger locally in any format, export it, then import it remotely. See [pack archive & restore](../operations/pack-archive-restore.md) for the full workflow.

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

# Restore a .flpack archive into a new local ledger (any name)
fluree create restored-db --from mydb.flpack

# Restore a .flpack archive onto a remote server
fluree create restored-db --remote origin --from mydb.flpack

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
