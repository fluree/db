# Cypher (openCypher 9 subset)

> **Status: v1 (preview).** This page describes the Cypher surface
> exposed by Fluree DB as of the first delivery. Many of the features
> users expect from Neo4j are explicitly out of scope for v1 and are
> called out below. See `GQL_CYPHER_SUPPORT.md` at the repository root
> for the design and roadmap.

Fluree DB accepts a useful subset of [openCypher 9][opencypher] on top
of the same query IR and transaction pipeline as JSON-LD and SPARQL.
Cypher relationship-with-properties — `(a)-[:WORKS_FOR {role: "..."}]->(b)`
— maps to Fluree's edge-annotation primitive
([concept](edge-annotations.md), [internals](../design/edge-annotations.md)).

[opencypher]: https://opencypher.org/resources/

## Why Cypher in an RDF database?

- **LPG ergonomics for property-graph users.** No need to model
  relationship properties by hand with intermediate nodes.
- **One query engine.** SPARQL, JSON-LD, and Cypher all lower to the
  same `Query` IR — the planner, executor, and result formatter are
  shared.
- **Round-trip with the other surfaces.** Data inserted via JSON-LD
  `@annotation` or SPARQL `{| ... |}` is visible to Cypher reads, and
  vice versa.

## Quick start

```cypher
MATCH (p:Person {name: "Alice"})-[:WORKS_FOR {role: "Engineer"}]->(o:Organization)
RETURN p, o
```

```rust
let result = fluree.query_cypher(&db, cypher).await?;
```

## How Cypher maps to RDF

| Cypher concept | Fluree representation |
|---|---|
| Node `(n:Label)` | Subject with `rdf:type <Label>`. |
| Multiple labels `(n:L1:L2)` | Multiple `rdf:type` triples about `n`. |
| Node properties `(n {key: val})` | Ordinary triples about `n`. |
| Relationship `(a)-[:TYPE]->(b)` | Base triple `(a, <TYPE>, b)`. |
| Relationship with `var` `(a)-[r:TYPE]->(b)` | Base triple + an `f:reifies*` reifier bundle; `r` binds the reifier subject. |
| Relationship properties `[:T {p:v}]` | Reifier bundle plus an annotation-body triple `(_:r, p, v)`. |
| Parallel relationships | Multiple reifier subjects attached to the same base edge. |

### IRI mapping for bare identifiers

Cypher uses bare names like `Person`, `WORKS_FOR`, `name`. Fluree
resolves them via:

1. **The ledger's default `@context`** (the same context that applies
   to JSON-LD queries against the same ledger).
   - `@vocab` supplies the fallback namespace.
   - Full-term mappings (e.g. `"Person": "http://example.org/Person"`)
     act as overrides.
2. **Fallback default:** `http://example.org/` when no context is
   configured. Useful in tests; not appropriate for production data.

The mapping is **case-preserving**: `WORKS_FOR` becomes
`<vocab>WORKS_FOR`, not `<vocab>worksFor`. Put any case-normalizing
aliases in the context.

## Relationship lowering — three shapes, three behaviors

The Cypher → IR rule depends on whether you bind the relationship and
whether you filter on relationship properties.

| Pattern | Lowers to | Cardinality | Sees plain RDF? |
|---|---|---|---|
| `(a)-[:T]->(b)` | Plain triple `(a, <T>, b)` | Set | Yes |
| `(a)-[r:T]->(b)` | `EdgeAnnotation { edge, annotation: ?r, body: [] }` | Bag | No — only reifier-bundled edges |
| `(a)-[:T {p:v}]->(b)` | `EdgeAnnotation { edge, annotation: ?#__anon, body: [(?#__anon, p, v)] }` | Bag | No |

**Consequence.** If your data was loaded via JSON-LD without
`@annotation` (or any other path that doesn't produce reifier
bundles), `MATCH (a)-[r:T]->(b)` returns zero rows even though the
base triples exist. Drop the `r` to get plain-RDF-visible set
semantics:

```cypher
-- bag semantics, requires reifier bundles
MATCH (a:Person)-[r:WORKS_FOR]->(o:Organization) RETURN a, r, o

-- set semantics, sees all base edges
MATCH (a:Person)-[:WORKS_FOR]->(o:Organization) RETURN a, o
```

## Cardinality

Cypher's default is **bag semantics**; SPARQL's default is set
semantics. The cardinality contract:

- Bare `(a)-[:T]->(b)` returns one row per distinct `(s, p, o)` —
  matches SPARQL.
- Binding `r` or matching on relationship properties shifts to one
  row per occurrence — matches Cypher.
- `RETURN DISTINCT` always falls back to set semantics.

## v1 supported surface

### Reads

```text
MATCH / OPTIONAL MATCH / WHERE / RETURN [DISTINCT]
ORDER BY / SKIP / LIMIT
```

- Node patterns with labels and/or inline properties.
- Directed typed relationships and type alternatives (`[:T1|T2]`,
  lowered to a `Union` of concrete-predicate triples).
- Inverse direction (`<-[:T]-`).
- Untyped relationships (`[r]`) — predicate is variable, system
  facts hidden via the existing `include_system_facts = false`
  filter.
- `WHERE` expressions: comparison, AND/OR/NOT, arithmetic +/-/*//,
  STARTS WITH / ENDS WITH / CONTAINS, IS NULL / IS NOT NULL,
  `expr IN [a, b, ...]`, `CASE WHEN ... THEN ... END` (simple and
  subject forms), `EXISTS { pattern }`.
- ORDER BY (variable keys only), SKIP, LIMIT.
- `UNWIND [literals] AS x` — inline list literal unwinding.
- Aggregates: `count(*)`, `count(x)`, `count(DISTINCT x)`,
  `sum(x)`, `avg(x)`, `min(x)`, `max(x)` (bare-variable arguments
  only in v1). Mixed projections (`RETURN n, count(*) AS c`)
  implicitly group by the non-aggregate projections.
- `WITH ... [WHERE/ORDER BY/SKIP/LIMIT/DISTINCT]` — subquery
  boundary. WHERE that references aggregate aliases lowers to HAVING
  rather than a pre-aggregation Filter. Nested WITHs nest Subqueries.
- `RETURN n`, `RETURN n, m`, `RETURN *`, `RETURN DISTINCT ...`,
  `RETURN expr AS alias` (lowered via `Bind`).
- `UNION` and `UNION ALL` at the RETURN boundary. Every branch must
  project the same VarIds in the same order; mixing `UNION` and
  `UNION ALL` in one chain is rejected (matches the openCypher
  spec). `RETURN *` is also rejected in UNION branches because its
  projected-vars list is opaque at lower time.

### Writes

```text
CREATE (a:Label {p:v})-[:T {q:w}]->(b:Label2)
```

```rust
let result = fluree.transact_cypher(ledger, cypher).await?;
```

- Directed typed relationships emit base triple + reifier bundle
  (LPG-mode default for Cypher).
- Multiple parallel relationships in one `CREATE` mint distinct
  annotation subjects automatically.

## v1 deferred — what to expect later

These produce a clear error today and land in follow-on slices.

- `MATCH (n)` without label/property/relationship constraint.
- Undirected relationships `-[r]-`.
- Variable-length paths `-[*N..M]->`.
- Path values `MATCH p = (...)`.
- `shortestPath`, `allShortestPaths`.
- `UNWIND $param AS x` (parameter-bound list — needs API-layer
  parameter substitution).
- `collect(x)` aggregate (needs list-valued bindings/result format).
- Expression-valued aggregate arguments (`sum(n + 1)`) — needs a
  pre-aggregation `Bind`.
- `collect(...)`, `labels(...)`, `keys(...)`, `properties(...)`,
  `type(r)`, `id(...)`, list/map functions generally.
- `SET / REMOVE / DELETE / DETACH DELETE`.
- `MERGE` — Cypher's find-or-create needs a search-first phase that
  the existing `TxnType` variants don't model. A v1.1 implementation
  can layer it at the API level: snapshot-query for the identifying
  pattern, then conditionally stage either a CREATE-shape transaction
  or an ON MATCH SET update.
- `MATCH ... CREATE ...` (WHERE-driven write templates).
- `CALL` with side effects, stored procedures.
- `LOAD CSV`, `FOREACH`, schema DDL.
- Multi-statement scripts (one statement per request).
- `%`, `^`, `XOR`.

## See also

- [Edge annotations (concept)](edge-annotations.md) — the storage
  primitive Cypher relationships sit on top of.
- [Edge annotations (storage internals)](../design/edge-annotations.md)
  — the `f:reifies*` durable encoding.
- [SPARQL](../query/sparql.md) — the parallel surface for the same IR.
- `GQL_CYPHER_SUPPORT.md` at the repo root — the design plan and v1
  contract.
