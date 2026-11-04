# fluree graph

Manage **named graphs** within a single branch of a ledger.

Named graphs are created implicitly: the first time you transact a triple under a new graph IRI (via TriG `GRAPH <iri> { ... }`, JSON-LD `@graph` with an `@id`, or SPARQL `INSERT DATA { GRAPH <iri> { ... } }`), the graph is registered and assigned a deterministic `g_id`. There is no separate "create graph" command — `fluree graph` only exposes the operations that don't fall out of `insert` / `upsert` / `update` for free: listing graphs, dropping one, and bulk-loading a new one from files with `fluree graph import`.

## Subcommands

### fluree graph list

List the user-defined named graphs registered on a branch.

**Usage:**

```bash
fluree graph list [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-l, --ledger <LEDGER>` | Ledger identifier (e.g. `mydb` or `mydb:feature-x`). Defaults to the active ledger. |
| `--remote <REMOTE>` | List graphs on a remote server by remote name (e.g. `origin`). |
| `--json` | Emit the filtered `named-graphs` JSON array instead of a table. |
| `--include-system` | Include the default graph and the system `txn-meta` / `config` graphs. Off by default. |

**Description:**

Reads the `named-graphs` section of the standard `info` payload for the targeted branch — no new endpoint is required. By default, the output is restricted to user-defined graphs (`g_id >= 3`); pass `--include-system` to also surface the default graph (`g_id 0`), `txn-meta` (`g_id 1`), and `config` (`g_id 2`).

Each row reports the graph IRI, kind (`user`, `default`, `system:txn-meta`, `system:config`), `g_id`, current flake count, and on-disk size.

**Auto-routing:** like other read commands, `fluree graph list` auto-routes through a running local `fluree server` if one is up, or through the tracked remote when the active ledger has a `track` config. Pass `--direct` to bypass auto-routing and read from the local store, or `--remote <name>` to force a specific remote.

**Examples:**

```bash
# Active ledger, user graphs only
fluree graph list

# Specific ledger / branch
fluree graph list --ledger mydb:feature-x

# Remote
fluree graph list --ledger mydb --remote origin

# JSON output including system graphs
fluree graph list --ledger mydb --include-system --json
```

### fluree graph import

Bulk-import files into a new named graph of an existing ledger.

**Usage:**

```bash
fluree graph import <IRI> --from <PATH> [OPTIONS]
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `<IRI>` | Full absolute IRI of the new named graph (e.g. `http://example.org/graph/products`). |

**Options:**

| Option | Description |
|--------|-------------|
| `--from <PATH>` | File or directory to import. |
| `-l, --ledger <LEDGER>` | Ledger identifier (e.g. `mydb` or `mydb:feature-x`). Defaults to the active ledger. |
| `--remote <REMOTE>` | Run the import on a remote server by remote name. `--from` must be a single file. |
| `--memory-budget-mb`, `--parallelism`, `--chunk-size-mb`, `--skolem-namespace`, `--leaflet-rows`, `--leaflets-per-leaf` | Bulk-import tuning, as for [`fluree create --from`](create.md). |
| `--edge-properties`, `--base-iri` | CSV/Cypher conversion, as for [`fluree create --from`](create.md#property-graph-imports-csv--cypher). |

**Description:**

`fluree graph import` runs the bulk-import pipeline of `fluree create --from` over a ledger that already exists:

- **The graph must be new.** Every statement in the source lands in `<IRI>`, which the ledger must not have registered. A graph that holds data is refused, and so is one emptied by `fluree graph drop`, which keeps its registration; import into a new IRI instead.
- **The ledger's history continues.** The import writes commits after the ledger's head, `t = head + 1` onward, so a query `as-of` an earlier `t` does not see the graph, and a full reindex from the commits rebuilds it.
- **Terms are shared.** An IRI or string the ledger already holds keeps its identity, so the graph joins with the default graph and other graphs through the subjects they have in common.
- **The index is extended, not rebuilt.** The new graph's index is built in bulk and added to the ledger's existing index; the rest of the index is reused as it is. A ledger with commits after its index is indexed first.
- **Writes wait.** Transactions that go through the connection running the import (the server the CLI routes it to, or the program that embeds Fluree) wait until it finishes; queries go on against the head the import started from. A writer that does not share that connection, such as a second process over the same storage, is not held: if it commits during the import, the import fails at the end with nothing published.

The source holds default-graph data: Turtle, N-Triples, JSON-LD, or JSONL/NDJSON files (optionally `.gz`/`.zst`), or CSV / Cypher inputs, as a single file or a directory. TriG, N-Quads, and JSON-LD that names graphs of its own are refused, since their statements belong to other graphs. Blank nodes are minted from the ledger id and the graph IRI unless `--skolem-namespace` is given, so importing one file into two graphs gives each its own blank nodes. A typed literal whose lexical form does not fit its datatype (`"abc"^^xsd:integer`) is kept as text under its datatype, as [`fluree create --from`](create.md) keeps one, where a transaction refuses it.

If the source holds statement annotations (RDF 1.2 / `f:reifies`), the ledger's annotation index is set aside: annotation queries stay correct but read by scan until [`fluree reindex`](reindex.md) seals it again. The CLI prints a note when this happens.

The following are refused with nothing written: a graph the ledger already has registered, the default graph (`urn:default`), the system `txn-meta` / `config` graphs, a ledger that does not exist, and the source formats above.

**If an import fails.** Until its last step an import publishes nothing, so a failure leaves the ledger as it was, including when another writer moved the ledger's head during the import. The last step publishes the import's commits and then its index. If the index does not publish, because publishing it fails or the process stops in between, the import is in the ledger all the same: the error says it is published through `t=…`, and queries see the graph, read from the import's commits until an index covers them. Every load of the ledger reads those commits into memory until then, so after a large import run [`fluree reindex`](reindex.md). The log records each import's commit head and index root (`publishing graph import`) before it publishes them.

**Routing:** with `--remote <name>`, or when the active ledger is tracked, the file is uploaded to the server and imported there; the server must advertise the `source-upload-graph` import mode, and `source-upload-skolem` when `--skolem-namespace` is given (see [server-integration](server-integration.md#into-a-new-graph-of-an-existing-ledger)). With a local `fluree server` running against the same storage, the import is routed through it in the same way, as a single file. Pass `--direct` only when no server holds the storage.

**Examples:**

```bash
# A directory of Turtle files into a new graph of the active ledger
fluree graph import http://example.org/graph/products --from products/

# A compressed file into a specific ledger
fluree graph import urn:example:payroll --from payroll.ttl.gz --ledger mydb

# On a remote server
fluree graph import urn:example:payroll --from payroll.ttl --ledger mydb --remote origin
```

**Output:**

```
Imported 1.2M flakes into graph <http://example.org/graph/products> of 'mydb:main' in 14.20s (0.08 M flakes/s) across 3 commits (t=18..20)
```

### fluree graph drop

Drop a single named graph from one branch by transactionally retracting every triple currently asserted in it.

**Usage:**

```bash
fluree graph drop <IRI> [OPTIONS]
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `<IRI>` | Full absolute IRI of the named graph to drop (e.g. `urn:example:org/payroll`). |

**Options:**

| Option | Description |
|--------|-------------|
| `-l, --ledger <LEDGER>` | Ledger identifier. Defaults to the active ledger. |
| `--remote <REMOTE>` | Execute against a remote server by remote name. |

**Description:**

`fluree graph drop` is **transactional and history-preserving**:

- The drop produces one new commit at `t = current + 1` whose flakes are retractions of every triple currently asserted under the target graph IRI.
- A query `as-of` an earlier `t` (e.g. via `--at t:<N>`, `--at-time`, or a dataset `TimeSpec`) still sees the graph populated.
- The graph IRI keeps its `g_id`; a subsequent insert into the same IRI lands in the same logical graph rather than a new slot.
- Drops are per-branch — sibling branches that share the same graph IRI are not touched.
- Idempotent: re-running the drop on an already-empty graph reports `committed: false`, `retracted: 0` and produces no new commit.
- **One commit, sized by the graph.** Every retraction lands in that commit, so a graph holding more facts than `FLUREE_MAX_GRAPH_SCAN_FLAKES` (default 10,000,000; `0` disables) is refused before it is read. A drop that fits that limit but not the ledger's novelty limit (`--reindex-max-bytes`) is refused at commit. Drop a larger graph in batches, e.g. `DELETE { GRAPH <g> { ?s ?p ?o } } WHERE { GRAPH <g> { VALUES ?s { … } ?s ?p ?o } }`.

The following are rejected with a clear error (no commit is produced):

- The **default graph** — refuses an empty IRI, since the default graph cannot be dropped.
- The **system graphs** — `urn:fluree:{ledger_id}#txn-meta` and `urn:fluree:{ledger_id}#config`.
- **Relative references** — `<IRI>` must be an absolute IRI with a valid `<scheme>:<rest>` head (e.g. `urn:...`, `http://...`).
- **Whitespace / control characters / RFC 3987-excluded characters** — leading/trailing whitespace is rejected, not silently trimmed.
- **Unknown graph IRIs** — return a 404-shaped "not registered" error; the registry is never silently extended by a drop.

**Examples:**

```bash
# Drop on the active ledger
fluree graph drop urn:example:org/payroll

# Specific ledger or branch
fluree graph drop urn:example:org/payroll --ledger mydb
fluree graph drop http://example.org/graphs/scratch --ledger mydb:feature-x

# Via a tracked remote server
fluree graph drop urn:example:org/payroll --ledger mydb --remote origin
```

**Output (committed):**

```
Dropped graph <urn:example:org/payroll> from 'mydb:main' — retracted 42 flakes (t=18).
```

**Output (no-op):**

```
Graph <urn:example:org/payroll> in 'mydb:main' was already empty — no commit produced (t=17).
```

## See Also

- [create](create.md) — create a ledger, optionally bulk-loading it from files
- [drop](drop.md) — drop a whole ledger or graph source
- [branch](branch.md) — branch management (drop, list, rebase, merge, diff)
- [info](info.md) — full ledger info, including the underlying `named-graphs` payload
- [Datasets and named graphs](../concepts/datasets-and-named-graphs.md) — concept doc
- [server-integration](server-integration.md) — wire contract for custom servers (`POST /drop-graph`, `GET /info`)
