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

From Rust:

```rust
let head = fluree
    .register_reasoned_head("mydb:main", &json!({"reasoning": "owl2rl"}))
    .await?;
```

The server's background worker fills the head and refreshes it after each
commit. To refresh now:

```bash
curl -X POST http://localhost:8090/v1/fluree/reasoned-heads/refresh \
  -H 'Content-Type: application/json' -d '{"ledger": "mydb:main"}'
```

`/reasoned-heads/list` returns the branch's heads; `/reasoned-heads/retire`
with `{"ledger", "headId"}` stops a head from being served or refreshed. All
four routes are admin routes.

## Which queries a head answers

A query is answered from a head when all of these hold:

- its reasoning, settled with the ledger's configuration defaults, rule
  sources and schema the same way the head's program was, is the same
  reasoning — modes, rules, ontology, evaluation time and reasoned graph;
- the head's current generation was computed at exactly the commit the query
  reads;
- the query runs without a restricting policy (a head is computed without
  policy, so a policy-restricted view always reasons at query time).

Reasoning budgets are not part of the match: a head holds only complete
results, so a query that would have hit its budget at query time gets the
complete answer from a head.

Tracked queries report the head that answered in the `reasoning` block (and
the `x-fdb-reasoning` header):

```json
"reasoning": {"head": {"headId": "01JC...", "generation": 4, "basisT": 12, "basisCommit": "bafy..."}}
```

JSON-LD, SPARQL and streaming queries are served from heads. Dataset queries
reason at query time.

## Freshness

`reasoningFreshness` (SPARQL: `# PRAGMA reasoning-freshness: <mode>`) chooses
what may answer:

| Mode | Answer |
|---|---|
| `default` | A head at exactly the queried commit; query-time reasoning otherwise. |
| `exact` | As `default`, but a query-time run that hits a budget is an error rather than a capped result. |
| `strict` | Only a head at exactly the queried commit. Waits briefly for one, then fails (408). Never reasons at query time. |
| `asOf` | The newest head generation at or before the queried commit. The **whole query** reads that generation's commit, and the response reports it (`basisT`, `basisCommit`). Falls back to `default` when there is none. |

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
