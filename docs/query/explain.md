# Explain Plans

Explain plans provide insight into how the query planner reorders WHERE-clause
patterns, helping you understand optimization decisions and diagnose
performance issues.

## Overview

Explain plans show:
- Whether patterns were reordered and why
- Whether database statistics were available for optimization
- The cardinality category and cost estimate assigned to each pattern
- The original vs. optimized pattern order
- Execution strategy hints for special fast paths such as fused property-join stars

## Requesting Explain Plans

### JSON-LD Query

Use the `/fluree/explain` endpoint (or the CLI `fluree query --explain ...`) to get a plan without executing.
For JSON-LD, the explain request body is the same as a normal JSON-LD query body.

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "select": ["?name"],
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ],
  "from": "mydb:main"
}
```

### SPARQL

Use the explain endpoint with SPARQL content type:

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?name
WHERE {
  ?person ex:name ?name .
}
```

## How the Query Planner Works

The query planner reorders WHERE-clause patterns to minimize the number of
intermediate rows flowing through the execution pipeline. It uses a greedy
algorithm that places patterns one at a time, choosing the cheapest eligible
pattern at each step.

### Pattern Categories

Every pattern is classified into one of four cardinality categories:

| Category     | Meaning                                | Patterns                                                                          |
| ------------ | -------------------------------------- | --------------------------------------------------------------------------------- |
| **Source**   | Produces rows (estimated row count)    | Triple, VALUES, UNION, Subquery, IndexSearch, VectorSearch, GeoSearch, S2Search, Graph, PropertyPath, R2rml, Service |
| **Reducer**  | Shrinks the stream (multiplier < 1.0)  | MINUS, EXISTS, NOT EXISTS                                                         |
| **Expander** | Grows the stream (multiplier >= 1.0)   | OPTIONAL                                                                          |
| **Deferred** | No cardinality effect                  | FILTER, BIND                                                                      |

### Placement Priority

The greedy loop places patterns in this priority order:

1. **Eligible reducers** (lowest multiplier first) — shrink the stream as
   early as possible.
2. **Sources** (lowest row count first, preferring patterns that join on
   already-bound variables) — most selective first.
3. **Eligible expanders** (lowest multiplier first) — defer row expansion
   until prerequisite variables are bound.

A reducer or expander is "eligible" when at least one of its variables is
already bound by a previously placed pattern.

FILTER and BIND patterns are integrated into the greedy loop: after each
source, reducer, or expander is placed, any deferred patterns whose input
variables are now satisfied are drained in original-position order. For BIND
patterns, only the expression's input variables must be bound — the target
variable is an output that feeds back into `bound_vars`, potentially enabling
further deferred patterns to be placed immediately (cascading placement).

### Compound Pattern Nesting

When a deferred pattern (FILTER or BIND) becomes ready and the last placed
pattern is a compound pattern (UNION, Graph, or Service), the planner nests
the deferred pattern *into* the compound pattern's inner lists instead of
appending it after. This enables the deferred pattern to participate in the
compound pattern's inner `reorder_patterns` pipeline, unlocking:

- Optimal placement after the specific triple that binds its variable
- Range-safe filter pushdown to index scans
- Inline evaluation during joins

Nesting occurs only when the compound pattern **guarantees** the deferred
pattern's required variable is bound:

| Compound     | Nest? | Guarantee                                              |
| ------------ | ----- | ------------------------------------------------------ |
| **UNION**    | Yes   | Variable must appear in the intersection of all branches |
| **Graph**    | Yes   | Variable is in inner patterns or is the graph name variable |
| **Service**  | Yes   | Variable is in inner patterns or is the endpoint variable |
| OPTIONAL     | No    | Left-join: inner vars may be Unbound                   |
| MINUS        | No    | Anti-join: inner vars not exported to outer scope      |
| EXISTS       | No    | Filter-only: inner vars not exported                   |
| NOT EXISTS   | No    | Filter-only: inner vars not exported                   |

For UNION, the deferred pattern is cloned into every branch. For Graph and
Service, it is appended to the inner pattern list. Recursion is handled
naturally: when a nested filter lands inside a branch containing another
compound pattern, the branch's `reorder_patterns` call applies the same logic.

### Bound-Variable-Aware Estimation

The planner tracks which variables become bound as each pattern is placed.
This significantly affects estimates for subsequent patterns:

- A triple `?s :name ?name` with `?s` **unbound** is a property scan —
  estimated at the full property count (or a 1000-row fallback).
- The same triple with `?s` **already bound** from an earlier pattern is a
  per-subject lookup — estimated at `count / ndv_subjects` (typically ~10
  rows).

This context-aware scoring also applies inside compound patterns: UNION
branches and subqueries receive database statistics and use the same
selectivity model for their inner patterns.

### Statistics-Based vs. Fallback Scoring

When a `StatsView` is available (after at least one indexing cycle), the
planner uses HLL-derived property statistics:

- **count**: total number of triples for this predicate
- **ndv_subjects**: number of distinct subjects
- **ndv_values**: number of distinct objects

Without statistics, the planner falls back to heuristic constants:

| Pattern Type   | Fallback Estimate |
| -------------- | ----------------: |
| ExactMatch     |                 1 |
| BoundSubject   |                10 |
| BoundObject    |             1,000 |
| PropertyScan   |             1,000 |
| FullScan       |            1e12   |

### Search and Graph Source Estimates

Search patterns (IndexSearch, VectorSearch, GeoSearch, S2Search) use their
`limit` field when present. Without an explicit limit, the planner assumes a
default of 100 rows. Graph patterns recursively estimate their inner
patterns. Service patterns use a very high estimate (1e12) so they are placed
last among sources, minimizing data sent to the remote endpoint.

## Reading Explain Output

The explain plan shows two sections: the original pattern order and the
optimized order. Each pattern is annotated with its category and estimate.

Example output for a multi-pattern query:

```
=== Query Optimization Explain (Generalized) ===

Statistics available: yes
Optimization: patterns reordered

--- Original Pattern Order ---
  [1] ?s :age ?age | category=Source row_count=5000
  [2] ?s :name ?name | category=Source row_count=10000
  [3] FILTER((> ?age 25)) | category=Deferred
  [4] OPTIONAL { ?s :email ?email } | category=Expander multiplier=1.00

--- Optimized Pattern Order ---
  [1] ?s :age ?age | category=Source row_count=5000
  [2] FILTER((> ?age 25)) | category=Deferred
  [3] ?s :name ?name | category=Source row_count=10000
  [4] OPTIONAL { ?s :email ?email } | category=Expander multiplier=1.00
```

Key things to look for:

- **Source row_count**: Lower values are placed first. If a pattern with a
  high row count appears early, it may indicate missing statistics or an
  inherently broad pattern.
- **Reducer multiplier**: Values below 1.0 indicate the fraction of rows
  that survive. A MINUS with multiplier 0.90 removes ~10% of rows.
- **Deferred placement**: FILTERs and BINDs appear immediately after all of
  their input variables become bound. BIND outputs cascade — a BIND placed
  early can enable subsequent FILTERs or BINDs that depend on its target
  variable. If a FILTER appears late, check whether its variables could be
  bound sooner.
- **Statistics available: no**: Without statistics, the planner uses
  conservative heuristics. Run at least one indexing cycle to enable
  statistics-based optimization.

### Execution Hints

Explain responses may also include an `execution-hints` array. These are not
generic cardinality estimates; they describe when the executor expects to use a
specialized path after planning.

For the star-join work, look for:

- `property_join`: the planner chose the same-subject property-join path
- `property_join_fused_star`: the planner chose property join and also fused
  trailing same-subject single-triple `OPTIONAL`s plus eligible trailing
  `FILTER`/`BIND` patterns into the same star operator

Typical fields include:

- `required_triples`: number of required star predicates
- `fused_optional_triples`: number of fused trailing `OPTIONAL` triples
- `fused_filters`: number of trailing filters evaluated inside the star path
- `fused_binds`: number of trailing binds evaluated inside the star path
- `width_score`: weighted star width used by the property-join gate
- `optional_bonus`: how much of the width score came from trailing optionals

This is the clearest signal that a query like:

```sparql
?deal a crm:Deal ;
      crm:name ?name ;
      crm:amount ?amount ;
      crm:stage ?stage .
OPTIONAL { ?deal crm:probability ?probability }
OPTIONAL { ?deal crm:closedAt ?closedAt }
FILTER (!STRSTARTS(STR(?stage), "Closed"))
```

is using the fused two-pass star path rather than falling back to separate
OPTIONAL and FILTER operators.

## Indexes

Scan operations use one of four index permutations depending on which
components of the triple pattern are bound:

- **SPOT**: Subject-Predicate-Object-Time — used when the subject is bound
- **POST**: Predicate-Object-Subject-Time — used for predicate+object lookups
- **OPST**: Object-Predicate-Subject-Time — used for object-based lookups
- **PSOT**: Predicate-Subject-Object-Time — used for full predicate scans

## Filter Optimization

Filters are automatically optimized by the query engine in three ways:

- **Dependency-based placement**: Filters and BINDs are placed as soon as all
  their input variables are bound, as part of the greedy reordering loop.
  BIND target variables feed back into the bound set, enabling cascading
  placement of dependent patterns.
- **Index pushdown**: Range-safe filters (comparisons like `>`, `<`, `>=`,
  `<=` on indexed properties) are pushed down to the index scan, reducing the
  number of rows read.
- **Inline evaluation**: Filters whose variables are all bound by a join are
  evaluated inside the join operator itself, avoiding the overhead of a
  separate filter pass.
- **BIND filter fusion**: When a FILTER's last required variable is the output
  of a BIND, the filter is fused into the BindOperator and evaluated inline
  after computing each row's BIND value. Failing rows are dropped before
  materialization, eliminating a separate FilterOperator pass.

## Best Practices

1. **Review plans for new queries**: Use explain to verify that the planner
   chose a reasonable order, especially for queries with many patterns.
2. **Ensure statistics are available**: Statistics enable much better estimates.
   If explain shows "Statistics available: no", check that at least one
   indexing cycle has completed.
3. **Check for high row counts early in the plan**: A source with a very high
   row count placed first can indicate a missing join variable or an overly
   broad pattern.
4. **Use LIMIT on search patterns**: IndexSearch, VectorSearch, GeoSearch, and
   S2Search patterns use their `limit` field for cost estimation. Providing an
   explicit limit helps the planner place them more accurately.

## Related Documentation

- [JSON-LD Query](jsonld-query.md): JSON-LD Query syntax
- [SPARQL](sparql.md): SPARQL syntax
- [Indexing and Search](../indexing-and-search/README.md): Index details
- [Debugging Queries](../troubleshooting/debugging-queries.md): Troubleshooting guide
