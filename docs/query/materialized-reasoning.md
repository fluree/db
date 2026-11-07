# Materialized Reasoning

Query-time reasoning materializes a program's derived facts when a query asks
for them, and again after every commit. **Materialized reasoning** persists
that work: you register a reasoning program on a branch, a background worker
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

The server's background worker builds the index's first generation, and a new
one after each commit. Every minute it also checks each index against what its
program settles to now: a schema or rules source in another ledger can change
without the branch moving, and the index is then rebuilt at the same commit.
Until it is, queries no longer match it and reason at query time. A refresh
that finds a storage sweep of the ledger running defers to the next one. To
refresh now:

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
- the query runs without a restricting policy (an index is computed without
  policy, so a policy-restricted view always reasons at query time).

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

JSON-LD, SPARQL and streaming queries are served from indexes, and so are
dataset queries whose graphs all belong to the one ledger. A dataset that
reaches other ledgers reasons at query time.

## Freshness

`reasoningFreshness` (SPARQL: `# PRAGMA reasoning-freshness: <mode>`) chooses
what may answer:

| Mode | Answer |
|---|---|
| `default` | An index whose head is at exactly the queried commit; query-time reasoning otherwise. |
| `exact` | As `default`, but a query-time run that hits a budget, or whose OWL 2 RL pass fails, is an error rather than an incomplete result. |
| `strict` | Only an index whose head is at exactly the queried commit. Waits briefly for one, then fails (408). Never reasons at query time. |
| `asOf` | For a query of the branch's current state, the newest retained generation at or before its commit. The **whole query** reads that generation's commit, and the response reports it (`basisT`, `basisCommit`). Falls back to `default` when there is none. |

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
