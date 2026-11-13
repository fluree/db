# Materialized Reasoning

Query-time reasoning materializes a program's derived facts when a query asks
for them, and again after every commit. **Materialized reasoning** persists
that work: you register a reasoning program on a branch, an optional background worker
keeps a **reasoning index** for it up to date, and reasoning queries that
match the program are answered from that index as ordinary queries. Nothing is
re-derived when the server restarts, and a reasoning budget never caps the
answer.

A reasoning index contains the branch's asserted facts together with the
program's derived facts.

Query-time reasoning stays the fallback: a query the index cannot answer
exactly reasons at query time, as before.

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
already has an active index for returns that index.

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
```

Enable automatic maintenance with `--reasoning-index-auto-refresh`,
`FLUREE_REASONING_INDEX_AUTO_REFRESH=true`, or
`server.indexing.reasoning_index_auto_refresh = true` in the server configuration.
It is off by default; explicit refresh works in either mode. The worker runs
on standalone write servers or the Raft leader, never on peers. Rust embedders
can run `ReasoningIndexWorker::new(fluree).run()` themselves.

The background worker builds the index's first generation, and a new
one after each commit. Every minute it also checks known indexes against what their
program settles to now: a schema or rules source in another ledger can change
without the branch moving, and the index is then rebuilt at the same commit.
Until it is, queries no longer match it and reason at query time. A refresh
that finds a storage sweep of the ledger running defers to the next one.
Local registrations wake the worker immediately. A full discovery scan runs
at startup and every 15 minutes to find registrations made by other processes.
Ledgers share a FIFO queue, and commits for a queued ledger coalesce.

Failed refreshes retry with exponential backoff from 5 seconds to 5 minutes,
subject to the worker's one-minute retry tick. A new commit does not reset
backoff, so a busy failing ledger cannot monopolize maintenance. Explicit
refresh bypasses backoff, including after changing a budget or configuration.
A failure that follows from what the closure is computed from (a closure over
its budget, a program whose output an index cannot hold) is not computed again
once its backoff ends: maintenance waits until the branch commits, the
registration changes, or what the program settles to changes. An explicit
refresh retries it at once, as does a restart; use one after raising a budget
through the environment, which changes none of those.
A failure in one registration does not prevent other indexes from refreshing.
The list endpoint includes `maintenanceFailures` with each locally observed
failure's reason, attempt count, failure time and retry time (Unix milliseconds).
These diagnostics are process-local, clear after a successful refresh, and do
not survive a restart. Rust callers can read `reasoning_index_failures`.

Maintenance uses the same resolved reasoning budgets as queries: built-in
limits, environment defaults, then the ledger/program settings permitted by
override control. Without overrides, the defaults are one million facts,
30 seconds, and a memory ceiling derived from the fact limit. Maintenance does
not retain OWL or Datalog closures in the query cache. Loaded generations use
a 16-entry LRU; embedders can adjust it with `set_reasoning_index_cache_capacity`.

A new generation of an OWL 2 RL program is computed from its predecessor
when the commits since only inserted data: only what those facts add to the
closure is derived. The closure is computed in full instead for the first
generation, after a retraction, when a commit inserted RDFS or OWL
vocabulary or RDF lists (which can change the program itself), for programs
with datalog rules, and for programs using `owl:sameAs` or the rules that
derive it (functional, inverse-functional and key properties, maximum
cardinality). A refresh reports which it did as `stats.closure` (`full` or
`incremental`). To refresh now:

```bash
curl -X POST http://localhost:8090/v1/fluree/reasoning-indexes/refresh \
  -H 'Content-Type: application/json' -d '{"ledger": "mydb:main"}'
```

`/reasoning-indexes/list` returns the branch's indexes and their heads;
`/reasoning-indexes/retire` with `{"ledger", "indexId"}` stops an index from
being served or refreshed, and its files are deleted once the retention window
has passed. A branch takes at most eight active indexes. All four routes are
admin routes.

## Which queries an index answers

A query is answered from an index when all of these hold:

- its reasoning, settled with the ledger's configuration defaults, rule
  sources and schema the same way the index's program was, is the same
  reasoning — modes, rules, ontology, evaluation time and reasoned graph;
- the index's head generation was computed at exactly the commit the query
  reads, and the query reads the branch's committed state at that commit. A
  staged (uncommitted) view, or a time-travel view of an earlier commit,
  reasons at query time; time travel to the current `t` is served;
- the index's head was computed under the engine semantics version the
  answering instance runs (see [Upgrades](#upgrades)).

`rdfs` and `owl2ql` widen a query's patterns rather than materialize facts,
so an index holds only what the program's other modes derive. A query that
mixes them, such as `["rdfs", "datalog"]`, is served with the rewrite still
applied over the index. Query-time `rdfs` reads the class and property
hierarchy from asserted facts only, so a program that derives
`rdfs:subClassOf` or `rdfs:subPropertyOf` facts under `rdfs` or `owl2ql`
(without `owl2rl`) publishes nothing.

An index publishes only complete results. A program whose closure hits its
budget, or whose OWL 2 RL pass fails, publishes nothing, and the refresh
reports why.

An instance that has found a ledger with no indexes does not look again for 30
seconds, so an index registered through another server instance starts
answering that instance's queries within that time; one registered through the
same instance answers at once.

Reasoning budgets are not part of the match: an index holds only complete
results, so a query that would have hit its budget at query time gets the
complete answer from an index.

Tracked queries report the index and generation that answered in the
`reasoning` block (and the `x-fdb-reasoning` header):

```json
"reasoning": {"index": {"indexId": "01JC...", "generation": 4, "basisT": 12, "basisCommit": "bafy..."}}
```

JSON-LD, SPARQL, Cypher and streaming queries are served from indexes, and so are
dataset queries whose graphs all belong to the one ledger. A dataset that
reaches other ledgers reasons at query time.

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
carries a policy. Query-supplied `rules` are stripped from a restricted request
before an index is chosen, as they are at query time, so only stored and
configured rules can match. Single-graph crawls retain the same snapshot,
derived facts and policy as execution, both at query time and when served.
Restricted reasoning crawls over datasets with multiple graphs, including
`unionDefaultGraph`, remain unsupported; use a single-graph view or select variables.

## Freshness

`reasoningFreshness` (SPARQL: `# PRAGMA reasoning-freshness: <mode>`) chooses
what may answer:

| Mode | Answer |
|---|---|
| `default` | An index whose head is at exactly the queried commit; query-time reasoning otherwise. |
| `exact` | As `default`, but a query-time run that hits a budget, or whose OWL 2 RL pass fails, is an error rather than an incomplete result. |
| `strict` | Only an index whose head is at exactly the queried commit. Waits briefly for one, then fails (408). Never reasons at query time. |
| `asOf` | For a query of the branch's current state, the newest retained generation at or before its commit, computed before no graph drop since (a drop takes a graph out as of every earlier `t`, and a generation holds what the graph held) and at no commit a [targeted removal](../transactions/history-retention.md#targeted-removal) covers. The **whole query** reads that generation's commit, and the response reports it (`basisT`, `basisCommit`). Falls back to `default` when there is none, and for a policy-restricted caller when the newest is older than its commit. |

## Storage and retention

An index lives inside its branch, under `{branch}/@reasoning/{indexId}/`, and
shares the ledger's dictionaries. Each generation is a complete index of the
branch — every graph — plus the derived facts, so an index costs about one
more index of the branch. A superseded generation is kept for 15 minutes
after its successor is published, so queries already reading it finish
(`set_reasoning_index_retention` changes this from Rust). Garbage collection,
sweeps, branch drops and ledger drop/restore all account for indexes.

Reasoning indexes are derived state: they are not transferred by clone, pull
or export, and are rebuilt where they are registered.

## Upgrades

Each generation records the engine semantics version it was computed under
(`engine` in `/reasoning-indexes/list`). A release that changes what the
engine derives, or how an index is stored or read, raises it. The worker then
rebuilds every index at the commit it was at, with no transaction needed, and
queries reason at query time until the rebuild is published.

While instances of two versions share a ledger, as during a rolling upgrade,
each reasons at query time over indexes the other version computed, and only
the newer version refreshes them. A refresh by the older one does nothing and
reports `newerEngine`. After a downgrade, an index the newer version published
stays unrefreshed: retire it and register its program again.

## Storage and durability

Each reasoning index stores a merged index of the entire asserted branch plus
its closure, as well as a separate derived-only set used to maintain later
generations. It is not just a compact cache of inferred triples. Storage cost
depends on closure size, index layout, shared dictionaries and generation
retention; it can be several times the asserted branch size. Each of the up to
eight active programs can incur this cost and maintenance work after commits.

A generation's manifest and artifacts are synced before its head is published,
including publication retries. A generation that fails to read falls back to
query-time reasoning and emits a rate-limited warning; when it is the head,
maintenance is asked to look at it. A refresh checks the head before declaring
it current. A head whose manifest or root is absent or does not decode is
repaired at once. One that failed a read is read again and repaired only if
that fails too, so a storage timeout does not cost a recompute; a refresh that
cannot read a file still in place fails and is retried.
Repair rebuilds at the same commit and can detach a broken manifest chain;
a persisted retention deadline defers both sweeping and shared-dictionary
reclamation after detaching the chain, including across restarts. This protects
files that readers may still hold before they become eligible for collection.

Each index keeps an erasure floor. A generation records the floor its build
began under, and one below its index's floor is neither published nor served,
current or retained (`asOf`). The purge of a
[permanent erasure](../transactions/permanent-erasure.md#purge) raises the floor
of every index of each branch that holds an erased commit, unpublishes its
head and deletes its generations, with the dictionaries only they reached. A
build begun before the purge publishes nothing. The next refresh builds each
index again from the branch as it now reads; until then reasoning queries on
that branch reason at query time.

A [targeted removal](../transactions/history-retention.md#targeted-removal)
discards nothing. A reasoning query of an earlier point counts as reading
every property, so it is refused when a removal covers that point, whichever
generation would have answered it. A query of the current state under `asOf`
is not answered from a generation built at a commit a removal covers, since
the generation holds the removed value as current: it reasons at query time
until a refresh publishes a generation after the removal.

Engine semantics version 4 rebuilds generations from earlier engines, including
ones whose incremental closure could have treated derived schema as new rules.

## Rust API migration

This feature changes the nameservice interfaces: custom implementations must
implement reasoning-index reads and fenced compare-and-swap publication.
No-op implementations cannot provide persistence or fencing guarantees.

`run_stream_query` no longer takes its former `LedgerState` argument.
`QueryResult`, `ReasoningTally`, and `ReasoningManifest` have new public fields; code constructing
these structs directly must initialize them. `QueryResult::read_view` retains
the snapshot, overlay and policy a single-graph query read, as a `ReadView`.

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

When integrating PR #2271, preserve the reasoning pair-probe lane's eager
materialization admission checks and clear `batched_pair_objects` whenever a
batch is rerouted to per-row execution. Test both eager and fallback paths
against the per-row oracle before enabling the combined optimization.
