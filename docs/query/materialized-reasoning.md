# Materialized Reasoning

Query-time reasoning materializes a program's derived facts when a query asks
for them, and again after every commit. **Materialized reasoning** persists
that work. You register a reasoning program on a branch; a **reasoning index**
holds the branch's asserted facts together with the facts the program derives;
and reasoning queries that match the program are answered from the index as
ordinary queries. Nothing is re-derived when the server restarts, and a
reasoning budget never caps an answer read from an index.

An index is brought up to date by a refresh: one you ask for, or one a
background worker runs after each commit when the server enables it.
Query-time reasoning stays the fallback. A query no index can answer exactly
reasons at query time, as before.

## Terms

| Term | Meaning |
|---|---|
| Reasoning program | The registered rules and options. |
| Reasoning index | What a branch maintains for one program. |
| Generation | One immutable build of an index, computed at one commit (its basis). |
| Head | The pointer to an index's currently published generation. |

## Registering a program

A program carries the reasoning keys a query would, plus an optional `graph`,
the IRI of the graph it reasons against (the default graph when omitted):

```bash
curl -X POST http://localhost:8090/v1/fluree/reasoning-indexes/register \
  -H 'Content-Type: application/json' \
  -d '{"ledger": "mydb:main", "program": {"reasoning": "owl2rl"}}'
# => {"ledger": "mydb:main", "indexId": "01JC..."}
```

Program keys: `reasoning`, `rules`, `ontology`, `reasoningTime`,
`reasoningBudget`, `@context`, `graph`. Registering a program the branch
already has an active index for returns that index. A branch takes at most
eight active indexes.

Registering builds nothing. The index answers queries once a refresh has
published its first generation.

From the CLI (a local ledger, or a server with `--remote`):

```bash
fluree reasoning-indexes register mydb --reasoning owl2rl
fluree reasoning-indexes register mydb --program '{"reasoning": "datalog", "rules": [...]}'
fluree reasoning-indexes refresh mydb
fluree reasoning-indexes list mydb
fluree reasoning-indexes retire 01JC... -l mydb
```

From Rust:

```rust
let index_id = fluree
    .register_reasoning_index("mydb:main", &json!({"reasoning": "owl2rl"}))
    .await?;
fluree.refresh_reasoning_index("mydb:main", &index_id).await?;
```

`/reasoning-indexes/list` returns the branch's indexes and their heads.
`/reasoning-indexes/retire` with `{"ledger", "indexId"}` stops an index from
being served or refreshed. Its files are deleted once the retention window has
passed, by the worker or by the next refresh of the branch. All four routes
are admin routes; see the
[API reference](../api/endpoints.md#reasoning-indexes).

## Keeping an index current

### Refreshing

A refresh brings an index to the branch's current commit: it computes the
program there, writes a new generation and publishes it.

```bash
curl -X POST http://localhost:8090/v1/fluree/reasoning-indexes/refresh \
  -H 'Content-Type: application/json' -d '{"ledger": "mydb:main"}'
```

Without `indexId` it refreshes every active index of the branch, and deletes
what indexes retired longer ago than the retention window left. Each result
names what the refresh did in `outcome`:

| Outcome | Meaning |
|---|---|
| `published` | A new generation was published. Carries `generation`, `basisT`, `kind` and `stats`. |
| `current` | The published generation is already at the branch's commit. |
| `failed` | The program could not be computed completely (`reason` says why), so nothing was published. |
| `discarded` | The result could not be published: while it was built the index was retired, an erasure reached the branch, or a newer version of Fluree published. |
| `throttled` | The index already holds as many superseded generations inside the retention window as it may (`inGuard`). Publication waits for them to age out. |
| `deferred` | Left for a later refresh (`reason` says why): a storage sweep or collection of the ledger is running, the branch moved on while it was computed, or a failed refresh is waiting to be retried. |
| `inFlight` | Another refresh of this index is under way. |
| `newerEngine` | The index was published by a newer version of Fluree. See [Upgrades](#upgrades). |

### Automatic maintenance

A server refreshes indexes on its own when started with
`--reasoning-index-auto-refresh`, `FLUREE_REASONING_INDEX_AUTO_REFRESH=true`,
or `reasoning_index_auto_refresh = true` under `[server.indexing]` in its
configuration file. It is off by default, so a server with no registered index
does no work looking for one. Explicit refresh works either way. The worker
runs on a standalone write server or on the Raft leader, never on a peer. Rust
embedders can run `ReasoningIndexWorker::new(fluree).run()` themselves.

The worker builds an index's first generation, and a new one after each
commit. Ledgers share one queue, and commits to a ledger already queued
coalesce into one refresh. Registrations made through the same server wake the
worker at once; a scan at startup and every 15 minutes finds registrations
made by other processes.

Every minute the worker also checks known indexes against what their program
settles to now. A schema or rules source in another ledger can change without
the branch moving, and the index is then rebuilt at the same commit. Until it
is, queries no longer match it and reason at query time.

### Incremental and full builds

A new generation of an OWL 2 RL program is computed from its predecessor when
the commits since only inserted data: only what those facts add to the closure
is derived. The closure is computed in full instead:

- for the first generation;
- after a retraction;
- when a commit inserted RDFS or OWL vocabulary or RDF lists, which can change
  the program itself;
- for programs with datalog rules;
- for programs using `owl:sameAs` or the rules that derive it (functional,
  inverse-functional and key properties, maximum cardinality).

A refresh reports which it did as `stats.closure` (`full` or `incremental`).

### Budgets and memory

A refresh computes its closure under the same resolved
[reasoning budget](reasoning.md#materialization-budget) as a query: built-in
limits, environment defaults, then the ledger and program settings that
override control permits. Without overrides the limits are one million derived
facts, 30 seconds, and a memory ceiling derived from the fact limit.

An index publishes only complete results. A program whose closure hits its
budget, or whose OWL 2 RL pass fails, publishes nothing, and the refresh
reports why. Set `reasoningBudget` in the program for a closure the defaults
do not cover.

A full build holds the closure in memory while it runs, so persisting a
program moves that memory from queries to maintenance rather than removing it.
Maintenance does not leave the closure in the query-time reasoning cache.
Generations a server has loaded to answer queries are kept in a 16-entry LRU,
which embedders can size with `set_reasoning_index_cache_capacity`.

### Failures and retries

A failed automatic refresh is retried with exponential backoff, from 5 seconds
to 5 minutes, on the worker's one-minute tick. A new commit does not reset the
backoff, so a busy failing ledger cannot monopolize maintenance. A failure in
one index does not keep the branch's other indexes from refreshing.

A failure that follows from what the closure is computed from (a closure over
its budget, a program whose output an index cannot hold) is not computed again
when its backoff ends. Maintenance waits until the branch commits, the
registration changes, or what the program settles to changes.

An explicit refresh retries at once in both cases, as does a restart. Use one
after raising a budget through the environment, which changes none of those.

`/reasoning-indexes/list` includes `maintenanceFailures`: for each index this
server failed to refresh, the reason, attempt count, failure time and retry
time (Unix milliseconds). They are local to the process, clear after a
successful refresh, and do not survive a restart. Rust callers read
`reasoning_index_failures`.

## Which queries an index answers

A query is answered from an index when all of these hold:

- its reasoning, settled with the ledger's configuration defaults, rule
  sources and schema the same way the index's program was, is the same
  reasoning: modes, rules, ontology, evaluation time and reasoned graph;
- the index's head generation was computed at exactly the commit the query
  reads, and the query reads the branch's committed state at that commit. A
  staged (uncommitted) view, or a time-travel view of an earlier commit,
  reasons at query time; time travel to the current `t` is served;
- the index's head was computed under the engine semantics version the
  answering instance runs (see [Upgrades](#upgrades)).

Reasoning budgets are not part of the match. An index holds only complete
results, so a query that would have hit its budget at query time gets the
complete answer from an index.

`rdfs` and `owl2ql` widen a query's patterns rather than materialize facts,
so an index holds only what the program's other modes derive. A query that
mixes them, such as `["rdfs", "datalog"]`, is served with the rewrite still
applied over the index. Query-time `rdfs` reads the class and property
hierarchy from asserted facts only, so a program that derives
`rdfs:subClassOf` or `rdfs:subPropertyOf` facts under `rdfs` or `owl2ql`
(without `owl2rl`) publishes nothing.

JSON-LD, SPARQL, Cypher and streaming queries are served from indexes, and so
are dataset queries whose graphs all belong to the one ledger. A dataset that
reaches other ledgers reasons at query time.

An instance that has found a ledger with no indexes does not look again for 30
seconds. An index registered through another server instance therefore starts
answering that instance's queries within that time; one registered through the
same instance answers at once.

Tracked queries report the index and generation that answered in the
`reasoning` block (and the `x-fdb-reasoning` header):

```json
"reasoning": {"index": {"indexId": "01JC...", "generation": 4, "basisT": 12, "basisCommit": "bafy..."}}
```

### Under a view policy

An index is computed without policy, over all of the ledger's data. A caller's
view policy applies when rows are read, as it does to query-time reasoning:
each fact is judged by its own subject, predicate and object, a class policy by
the subject's classes including derived ones, and an `f:query` condition
against the derived facts the query reads. A restricted caller therefore gets
the same answer from an index as from query-time reasoning; see
[Reasoning under access policy](reasoning.md#reasoning-under-access-policy).

A restricted caller is served only by a generation at exactly the commit it
reads. Under `asOf` it reasons at query time when there is none, rather than
read an older generation, which would also take its policy's class lookups and
conditions back to that commit. A dataset is restricted when any of its graphs
carries a policy.

Query-supplied `rules` are stripped from a restricted request before an index
is chosen, as they are at query time, so only stored and configured rules can
match.

A single-graph crawl (`select {"?s": ["*"]}`) renders from the snapshot,
derived facts and policy its rows were read under, at query time and when
served alike. A restricted reasoning crawl over a dataset of several graphs,
`unionDefaultGraph` included, is refused; use a single-graph view or select
variables.

## Freshness

`reasoningFreshness` (SPARQL: `# PRAGMA reasoning-freshness: <mode>`) chooses
what may answer:

| Mode | Answer |
|---|---|
| `default` | An index whose head is at exactly the queried commit; query-time reasoning otherwise. |
| `exact` | As `default`, but a query-time run that hits a budget, or whose OWL 2 RL pass fails, is an error rather than an incomplete result. |
| `strict` | Only an index whose head is at exactly the queried commit. Waits up to 10 seconds for one, then fails (408). Never reasons at query time. |
| `asOf` | For a query of the branch's current state, the newest eligible retained generation at or before its commit. The **whole query** reads that generation's commit, and the response reports it (`basisT`, `basisCommit`). Falls back to `default` when there is none, and for a policy-restricted caller when the newest is older than its commit. |

Under `asOf` a generation is eligible when no graph was dropped since it was
built and no [targeted removal](../transactions/history-retention.md#targeted-removal)
covers its commit. A drop takes a graph out as of every earlier `t`, and a
removal takes a value out of every read of a commit it was current at; a
generation holds what the branch held at its basis, so answering from one
would read either back.

Rust embedders can change how long `strict` waits with
`set_reasoning_index_strict_wait`.

## Storage

An index lives inside its branch, under `{branch}/@reasoning/{indexId}/`, and
shares the ledger's dictionaries. Reasoning indexes are derived state: they
are not transferred by clone, pull or export, and are rebuilt where they are
registered. Garbage collection, sweeps, branch drops and ledger drop and
restore all account for them.

### Cost

A reasoning index is not a compact cache of inferred triples. Each generation
is a complete index of the branch, every graph, merged with the program's
closure, with a separate set of the derived-only facts beside it that later
generations are maintained from. The derived facts are therefore stored twice.

Storage depends on closure size, index layout, shared dictionaries and how
many generations are retained; it can be several times the size of the
branch's own index. Each of a branch's active programs incurs that cost, and
does maintenance work after commits.

### Retention

A superseded generation is kept for 15 minutes after its successor is
published, so queries already reading it finish and `asOf` has earlier
generations to answer from. An index may hold 32 superseded generations inside
that window; past that, a refresh reports `throttled` and publication waits
for one to age out, so an index on a branch committing faster than that falls
behind its head. `set_reasoning_index_retention` changes both from Rust.

### Durability and repair

A generation's manifest and files are synced before its head is published,
publication retries included.

A generation that fails to read falls back to query-time reasoning and logs a
rate-limited warning; when it is the head, maintenance is asked to look at it.
A refresh checks the head before declaring it current:

- a head whose manifest or root is absent, or does not decode, is repaired at
  once;
- a head that failed a read is read again and repaired only if that fails too,
  so a storage timeout does not cost a recompute;
- a refresh that cannot read a file still in place fails and is retried.

A repair rebuilds at the same commit and can detach a broken chain of earlier
generations. The files of a detached chain are kept from collection for the
retention window, across restarts too, since readers may still hold them.

## Erasure and removal

Each index keeps an erasure floor. A generation records the floor its build
began under, and one below its index's floor is neither published nor served,
whether current or retained.

The purge of a [permanent erasure](../transactions/permanent-erasure.md#purge)
raises the floor of every index of each branch that holds an erased commit,
unpublishes its head and deletes its generations, with the dictionaries only
they reached. A build begun before the purge publishes nothing. The next
refresh builds each index again from the branch as it now reads; until then
reasoning queries on that branch reason at query time.

A [targeted removal](../transactions/history-retention.md#targeted-removal)
discards nothing. A reasoning query of an earlier point counts as reading
every property, so it is refused when a removal covers that point, whichever
generation would have answered it. A query of the current state under `asOf`
is not answered from a generation built at a commit a removal covers; it
reasons at query time until a refresh publishes a generation after the
removal.

## Upgrades

Each generation records the engine semantics version it was computed under
(`engine` in `/reasoning-indexes/list`). A release that changes what the
engine derives, or how an index is stored or read, raises it. The worker then
rebuilds every index at the commit it was at, with no transaction needed, and
queries reason at query time until the rebuild is published. Where automatic
maintenance is off, refresh after such an upgrade.

While instances of two versions share a ledger, as during a rolling upgrade,
each reasons at query time over indexes the other version computed, and only
the newer version refreshes them. A refresh by the older one does nothing and
reports `newerEngine`. After a downgrade, an index the newer version published
stays unrefreshed: retire it and register its program again.

## Rust API changes

Materialized reasoning changes the nameservice interfaces: custom
implementations must implement reasoning-index reads and fenced
compare-and-swap publication. No-op implementations cannot provide persistence
or fencing guarantees.

`run_stream_query` no longer takes its former `LedgerState` argument.
`QueryResult`, `ReasoningTally` and `ReasoningManifest` have new public
fields; code constructing these structs directly must initialize them.
`QueryResult::read_view` retains the snapshot, overlay and policy a
single-graph query read, as a `ReadView`.

The async formatters (`format_results_async`, `format_results_string_async`
and the `QueryResult` methods over them, such as `to_jsonld_async` and
`to_jsonld_async_with_policy`) format a result that carries a read view from
that state, under the policy that judged its rows. The `db` and `policy`
arguments no longer decide anything for such a result: a crawl formatted with
no policy is filtered by the query's, and a different policy passed for a
result whose query ran under one is not applied. A `policy` argument still
applies to a result whose query ran under none. `format_results_async_dataset`
formats a one-graph dataset's result the same way. Prefer
`format_async_for_view` when formatting API query results.

The incremental reasoner entry points take both the closed state and the
assertion-only view at the same commit. Pass external schema in the assertion
view as well; derived schema in the closed state must not become new rules.
