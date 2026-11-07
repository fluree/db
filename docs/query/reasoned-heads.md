# Persisted Reasoning (Reasoned Heads)

Query-time reasoning materializes a program's derived facts when a query asks
for them, and again after every commit. A **reasoned head** persists that work:
you register a reasoning program on a branch, a background worker keeps a
merged index of the branch's data and the program's derived facts up to date,
and reasoning queries that match the program are answered from that index as
ordinary queries. Nothing is re-derived when the server restarts, and a
reasoning budget never caps the answer.

Query-time reasoning stays the fallback: a query the head cannot answer
exactly reasons at query time, as before.

## Registering a program

A program carries the reasoning keys a query would, plus an optional `graph`,
the IRI of the graph it reasons against (the default graph when omitted):

```bash
curl -X POST http://localhost:8090/v1/fluree/reasoned-heads/register \
  -H 'Content-Type: application/json' \
  -d '{"ledger": "mydb:main", "program": {"reasoning": "owl2rl"}}'
# => {"ledger": "mydb:main", "headId": "01JC..."}
```

Program keys: `reasoning`, `rules`, `ontology`, `reasoningTime`,
`reasoningBudget`, `@context`, `graph`. Registering a program the branch
already has active returns its existing head.

From the CLI (a local ledger, or a server with `--remote`):

```bash
fluree reasoned-heads register mydb --reasoning owl2rl
fluree reasoned-heads register mydb --program '{"reasoning": "datalog", "rules": [...]}'
fluree reasoned-heads refresh mydb
fluree reasoned-heads list mydb
fluree reasoned-heads retire 01JC... -l mydb
```

From Rust:

```rust
let head = fluree
    .register_reasoned_head("mydb:main", &json!({"reasoning": "owl2rl"}))
    .await?;
```

The server's background worker fills the head and refreshes it after each
commit, and every minute checks each head against what its program settles
to now: a schema or rules source in another ledger can change without the
branch moving, and the head is then rebuilt at the same commit. Until it
is, queries no longer match it and reason at query time. A refresh that
finds a storage sweep of the ledger running defers to the next one. To
refresh now:

```bash
curl -X POST http://localhost:8090/v1/fluree/reasoned-heads/refresh \
  -H 'Content-Type: application/json' -d '{"ledger": "mydb:main"}'
```

`/reasoned-heads/list` returns the branch's heads; `/reasoned-heads/retire`
with `{"ledger", "headId"}` stops a head from being served or refreshed, and
its files are deleted once the retention window has passed. A branch takes at
most eight active heads. All four routes are admin routes.

## Which queries a head answers

A query is answered from a head when all of these hold:

- its reasoning, settled with the ledger's configuration defaults, rule
  sources and schema the same way the head's program was, is the same
  reasoning — modes, rules, ontology, evaluation time and reasoned graph;
- the head's current generation was computed at exactly the commit the query
  reads, and the query reads the branch's committed state at that commit. A
  staged (uncommitted) view, or a time-travel view of an earlier commit,
  reasons at query time; time travel to the current `t` is served;
- the query runs without a restricting policy (a head is computed without
  policy, so a policy-restricted view always reasons at query time).

`rdfs` and `owl2ql` widen a query's patterns rather than materialize facts,
so a head holds only what the program's other modes derive. A query that
mixes them, such as `["rdfs", "datalog"]`, is served with the rewrite still
applied over the head. Query-time `rdfs` reads the class and property
hierarchy from asserted facts only, so a program that derives
`rdfs:subClassOf` or `rdfs:subPropertyOf` facts under `rdfs` or `owl2ql`
(without `owl2rl`) publishes nothing.

A head publishes only a complete result. A program whose closure hits its
budget, or whose OWL 2 RL pass fails, publishes nothing, and the refresh
reports why.

An instance that has found a ledger with no heads does not look again for 30
seconds, so a head registered through another server instance starts
answering that instance's queries within that time; one registered through the
same instance answers at once.

Reasoning budgets are not part of the match: a head holds only complete
results, so a query that would have hit its budget at query time gets the
complete answer from a head.

Tracked queries report the head that answered in the `reasoning` block (and
the `x-fdb-reasoning` header):

```json
"reasoning": {"head": {"headId": "01JC...", "generation": 4, "basisT": 12, "basisCommit": "bafy..."}}
```

JSON-LD, SPARQL and streaming queries are served from heads, and so are
dataset queries whose graphs all belong to the one ledger. A dataset that
reaches other ledgers reasons at query time.

## Freshness

`reasoningFreshness` (SPARQL: `# PRAGMA reasoning-freshness: <mode>`) chooses
what may answer:

| Mode | Answer |
|---|---|
| `default` | A head at exactly the queried commit; query-time reasoning otherwise. |
| `exact` | As `default`, but a query-time run that hits a budget is an error rather than a capped result. |
| `strict` | Only a head at exactly the queried commit. Waits briefly for one, then fails (408). Never reasons at query time. |
| `asOf` | For a query of the branch's current state, the newest head generation at or before its commit. The **whole query** reads that generation's commit, and the response reports it (`basisT`, `basisCommit`). Falls back to `default` when there is none. |

## Storage and retention

A head lives inside its branch, under `{branch}/@reasoned/{headId}/`, and
shares the ledger's dictionaries. It is a complete index of the branch — every
graph — plus the derived facts, so it costs about one more index of the
branch. Superseded generations are kept for the indexer's garbage-collection
window (`gc_min_time_mins`) so queries already reading them finish; garbage
collection, sweeps, branch drops and ledger drop/restore all account for
heads.

Heads are derived state: they are not transferred by clone, pull or export,
and are rebuilt where they are registered.
