# openCypher Support Matrix

A tracked feature matrix for Fluree's [openCypher](../query/cypher.md)
surface, against **openCypher 9** (the Cypher 9 language reference the
[openCypher TCK](https://github.com/opencypher/openCypher) exercises). For
syntax and semantics see [Cypher (concept)](../query/cypher.md); for recipes
see the [Cypher cookbook](../guides/cookbook-cypher.md).

## Legend

| Mark | Meaning |
|------|---------|
| ✅ | **Supported** — works as openCypher 9 specifies. |
| ◑ | **Partial** — a common subset works; specific forms are deferred (noted). |
| ⟂ | **Divergent by design** — intentionally different because Fluree is an RDF / immutable graph, not an LPG store. Rejected-or-adapted, never silently wrong. |
| ⏳ | **Deferred** — not yet implemented; rejected with a clear error. |

**Guiding invariant:** an unsupported construct produces a *clear error*, never a
silently wrong result. Divergences (⟂) are where openCypher's LPG assumptions
meet Fluree's RDF model.

## Core model divergences (⟂)

These shape everything below; read them first.

- **Nodes are IRIs.** A node is an RDF subject (an IRI/blank node), not an opaque
  LPG node. `labels(n)` are `rdf:type` assertions; node identity is the IRI.
- **Relationships are edge annotations.** A relationship is the base triple
  `(s, p, o)`; binding `-[r:T]->` reifies it into an `f:reifies*` annotation node
  (the LPG edge identity). Fluree does **not** implement RDF-star triple terms —
  see [Edge annotations](../concepts/edge-annotations.md).
- **`id(n)` / `elementId(n)`** return the node/relationship **IRI string** — there
  is no integer element id.
- **No implicit per-statement transaction id** semantics; immutability/time-travel
  replace it (`f:t`, history queries).

## Clauses

| Clause | Status | Notes |
|--------|:------:|-------|
| `MATCH` | ✅ | Node/relationship patterns, `WHERE`. |
| `OPTIONAL MATCH` | ✅ | Nullable bindings; poisoned-binding semantics. |
| `WITH` | ✅ | Projection boundary; `WHERE`→HAVING when it references aggregates; `DISTINCT`/`ORDER BY`/`SKIP`/`LIMIT`; `collect()` carries forward as a list. |
| `UNWIND` | ✅ | Inline lists and runtime list expressions; `$param` lists via API substitution. |
| `RETURN` | ✅ | `*`, aliases, `DISTINCT`, `ORDER BY`/`SKIP`/`LIMIT`. |
| `UNION` / `UNION ALL` | ✅ | Column-name-match + uniform-variant rules enforced. |
| `CALL { … }` (subquery) | ✅ | Imports `(a,b)` / `(*)`, uncorrelated broadcast, inner `UNION`, nesting, strict scope/shadowing, correlated-aggregate soundness. |
| `CREATE` | ✅ | Nodes + relationships (relationships reify). |
| `MERGE` (node) | ✅ | `ON CREATE SET` / `ON MATCH SET`. |
| `MERGE` (relationship) | ◑ | Standalone + bound-endpoint forms; `ON CREATE SET`. Deferred: property-bearing rel MERGE (`-[:T {p:v}]->`), `ON MATCH SET` on a rel MERGE, multi-hop/multi-part MERGE. |
| `SET` / `REMOVE` | ✅ | Properties, `+=` map merge, labels. |
| `DELETE` / `DETACH DELETE` | ✅ | |
| `FOREACH` | ⏳ | |
| `CALL proc(...) YIELD` | ⏳ | Stored/builtin procedures (distinct from `CALL { … }`). |
| `LOAD CSV` | ⏳ | Bulk CSV import exists via the CLI, not the `LOAD CSV` clause. |
| Multi-statement (`;`) | ⏳ | One statement per request. |

## Patterns & paths

| Feature | Status | Notes |
|---------|:------:|-------|
| Node pattern (labels, inline props) | ✅ | |
| Directed typed relationship `-[:T]->`, `<-[:T]-` | ✅ | |
| Type alternation `-[:A\|B]->` | ✅ | `Union` of concrete predicates. |
| Undirected `-[:T]-` | ✅ | Forward ∪ reverse `Union`. |
| Untyped relationship `-[r]->` | ✅ | Variable predicate; system facts hidden. |
| Bounded var-length `-[:T*m..n]->` | ✅ | **Enumerates trails** (one row per path, relationship-uniqueness). |
| Unbounded var-length `-[:T*]->` | ⟂ | **Reachability** (one row per reachable endpoint), not path enumeration. |
| Untyped var-length `-[*m..n]->` | ⟂ | Wildcard reachability over node→node edges; excludes `rdf:type`/`f:reifies*`. |
| Bounded var-length **binding** `-[r:T*m..n]->` / `p = …` | ✅ | `r` = rel list, `p` = path; via per-branch construction. |
| Unbounded var-length binding | ⏳ | Needs a path-enumeration operator. |
| `shortestPath` / `allShortestPaths` | ✅ | Anchored, single typed predicate; `All` emits one row per minimal path. |
| `relationships(p)` / `nodes(p)` / `pathPairs(p)` / `length(p)` | ✅ | `relationships(p)` carries the stored edge orientation. |
| Bounded type-alternation var-length `-[:A\|B*1..3]->` | ⏳ | Use the unbounded form. |
| Undirected **unbounded** path `-[:T*]-` | ⏳ | |

## Expressions & operators

| Feature | Status | Notes |
|---------|:------:|-------|
| Arithmetic `+ - * / %`, unary `-` | ✅ | `/` of integers → `xsd:decimal` (rendered as a string for precision). |
| Exponentiation `^` | ✅ | Right-associative. |
| Comparison `= <> < <= > >=` | ✅ | |
| Boolean `AND` / `OR` / `XOR` / `NOT` | ✅ | |
| `STARTS WITH` / `ENDS WITH` / `CONTAINS` | ✅ | |
| `x IN [ … ]` | ✅ | |
| `IS NULL` / `IS NOT NULL` | ✅ | |
| `CASE` (simple + generic) | ✅ | Aggregates inside `CASE` deferred. |
| Property access `n.prop` | ◑ | Bare-variable target; chained `n.a.b` deferred. |
| List literal / indexing `[a,b]`, `list[i]` | ✅ | Negative index from end. |
| Map literal `{k: v}` | ✅ | Key-order-insensitive identity (⟂ vs strict insertion order for equality). |
| Map projection `n{.k, .*, x: e}` | ◑ | Mixing `.*` with other selectors deferred. |
| List comprehension / `reduce` / `all·any·none·single` | ✅ | Loop-local property access supported. |
| Pattern comprehension `[(a)-->(b) \| e]` | ✅ | Correlated; reuses the EXISTS path. |
| `EXISTS { … }` (predicate + value) | ✅ | Incl. inside map/projection entries. |
| Parameters `$p` | ✅ | Scalars, lists, maps; substituted everywhere incl. inside `CALL`/patterns. |

## Functions

| Group | Status | Notes |
|-------|:------:|-------|
| Casts: `toString` `toInteger` `toFloat` | ✅ | `toFloat`→xsd:double. |
| String: `toUpper` `toLower` `substring` `left` `right` `trim` `ltrim` `rtrim` `replace` `split` `reverse` | ✅ | `substring` 0-indexed; `replace` literal. |
| Math: `abs` `round` `floor` `ceil` `sign` `sqrt` `log` `rand` | ✅ | `log` = natural log. |
| `coalesce` | ✅ | |
| Aggregates: `count` `sum` `avg` `min` `max` `collect` (+ `DISTINCT`) | ✅ | Implicit grouping by non-aggregate projections; HAVING via `WITH`. |
| List: `size` `head` `last` `tail` `reverse` `range` | ✅ | |
| Path/metadata: `length` `nodes` `relationships` `pathPairs` `labels` `type` `startNode` `endNode` `keys` `properties` | ✅ | |
| `id` / `elementId` | ⟂ | Returns the IRI string. |
| Temporal accessors `<date>.year/.month/.day/.hour/.minute/.second` | ✅ | |
| Temporal constructors `date()` `datetime()` `duration()` | ⏳ | Use XSD-typed literals. |
| Spatial `point()` / `distance()` | ⏳ | |

## Null & type semantics

| Aspect | Status | Notes |
|--------|:------:|-------|
| Three-valued logic in `WHERE` / filters | ✅ | Unbound comparison → filter-false. |
| Null propagation through arithmetic / functions | ✅ | |
| `IS NULL` for absent property (nullable accessor) | ✅ | `OPTIONAL`-wrapped accessor. |
| Mixed-representation equality (encoded vs decoded) | ✅ | Normalized at DISTINCT/GROUP BY/join/MINUS/VALUES. |
| `xsd:float` string-backed numeric coercion | ✅ | In SUM/AVG, comparisons, math. |
| List / map ordering in `ORDER BY` | ⏳ | `ORDER BY <list/map>` rejected (defensive total order internally). |

## Maintaining this matrix

This is a hand-maintained matrix, not yet a TCK-driven report. When a Cypher
feature lands or a divergence changes:

1. Update the relevant row here **and** the [concept doc](../query/cypher.md).
2. Prefer ⟂ over ⏳ when the divergence is an intentional RDF-model choice —
   and record *why* in the Notes column.

A future step is to wire a subset of the openCypher TCK `.feature` scenarios as
executable tests and generate the supported/deferred columns from their pass/fail
results, replacing the hand-maintained status marks.
