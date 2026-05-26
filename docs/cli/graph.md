# fluree graph

Manage **named graphs** within a single branch of a ledger.

Named graphs are created implicitly: the first time you transact a triple under a new graph IRI (via TriG `GRAPH <iri> { ... }`, JSON-LD `@graph` with an `@id`, or SPARQL `INSERT DATA { GRAPH <iri> { ... } }`), the graph is registered and assigned a deterministic `g_id`. There is no separate "create graph" command — `fluree graph` only exposes the operations that don't fall out of `insert` / `upsert` / `update` for free.

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
| `--ledger <LEDGER>` | Ledger identifier (e.g. `mydb` or `mydb:feature-x`). Defaults to the active ledger. |
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
| `--ledger <LEDGER>` | Ledger identifier. Defaults to the active ledger. |
| `--remote <REMOTE>` | Execute against a remote server by remote name. |

**Description:**

`fluree graph drop` is **transactional and history-preserving**:

- The drop produces one new commit at `t = current + 1` whose flakes are retractions of every triple currently asserted under the target graph IRI.
- A query `as-of` an earlier `t` (e.g. via `--at t:<N>`, `--at-time`, or a dataset `TimeSpec`) still sees the graph populated.
- The graph IRI keeps its `g_id`; a subsequent insert into the same IRI lands in the same logical graph rather than a new slot.
- Drops are per-branch — sibling branches that share the same graph IRI are not touched.
- Idempotent: re-running the drop on an already-empty graph reports `committed: false`, `retracted: 0` and produces no new commit.

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

- [drop](drop.md) — drop a whole ledger or graph source
- [branch](branch.md) — branch management (drop, list, rebase, merge, diff)
- [info](info.md) — full ledger info, including the underlying `named-graphs` payload
- [Datasets and named graphs](../concepts/datasets-and-named-graphs.md) — concept doc
- [server-integration](server-integration.md) — wire contract for custom servers (`POST /drop-graph`, `GET /info`)
