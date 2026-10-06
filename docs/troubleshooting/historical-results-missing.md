# Missing results at a historical `t`

A query against a past `t` returns nothing (or returns rows with unbound
values) for data that demonstrably existed at that `t`, while the same query
against current state is correct.

This happens when the ledger's published index was built by a version
predating [fluree/db#1624](https://github.com/fluree/db/pull/1624). The commit
history is complete and undamaged; only the index artifact is missing the
entries needed to reconstruct those facts. Reindexing rebuilds it.

## Affected data

Two shapes, both of which leave nothing live to anchor the index entry:

- A **property whose every value was retracted**. Nothing currently uses the
  property anywhere in the ledger.
- An **entity that was deleted outright**, i.e. every triple about that
  subject was retracted.

Data that was only partially retracted is not affected — surviving values keep
the index entry alive, so history reads through it correctly.

## Symptoms

```sparql
# Returns 0 rows, though every invoice carried the flag at t=1
SELECT ?inv FROM <mydb:main@t:1> WHERE { ?inv ex:legacyFlag "true" }
```

`OPTIONAL` is the easiest form to miss, because it returns a full result set
rather than an empty one:

```sparql
# Returns every invoice with ?flag unbound, which reads as
# "the property did not exist at t=1"
SELECT ?inv ?flag FROM <mydb:main@t:1>
WHERE { ?inv a ex:Invoice . OPTIONAL { ?inv ex:legacyFlag ?flag } }
```

`COUNT`, `FILTER EXISTS`, and history-range queries over the same property
report the same absence.

## Confirming the cause

Compare a historical read against the same read on an unindexed view of the
data. A ledger serving from novelty (not yet indexed) reconstructs these facts
correctly, so a disagreement between the two points at the index.

The cheapest confirmation is to run the remediation below on a copy and see
whether the historical result changes.

## Remediation

Reindex the ledger. Nothing triggers this automatically — neither ordinary
writes nor incremental index builds repair an index that is already missing
the entries.

```bash
# CLI
fluree reindex mydb:main

# Or via the admin API
curl -X POST https://<fluree-server>/v1/fluree/reindex \
  -H 'Content-Type: application/json' \
  -d '{"ledger": "mydb:main"}'
```

A reindex rebuilds from the commit chain starting at genesis and does not
carry anything forward from the previous index, so it restores the missing
entries regardless of which version wrote the index being replaced. Reindexing
is safe to repeat.

Plan for it as a full index build: cost scales with total history, not with
the amount of affected data.

## After reindexing

Indexes built by a version that includes the fix are not affected, and stay
correct across subsequent incremental index builds. No further action is
needed once a ledger has been reindexed once.

## Related documentation

- [Background indexing](../indexing-and-search/background-indexing.md) —
  novelty and reindex thresholds
- [Debugging queries](debugging-queries.md) — EXPLAIN plans and query tracing
- [Time travel](../concepts/time-travel.md) — querying at a past `t`
