# Cypher (openCypher 9 subset)

Fluree accepts a subset of [openCypher 9][opencypher] on top of the same query
IR and transaction pipeline as JSON-LD and SPARQL — the planner, executor, and
result formatter are shared across all three surfaces. A Cypher
relationship-with-properties — `(a)-[:WORKS_FOR {role: "..."}]->(b)` — maps to
Fluree's edge-annotation primitive ([concept](edge-annotations.md),
[internals](../design/edge-annotations.md)), so property-graph edges and RDF
quoted-triple annotations are the same data read from two angles.

It is a *subset*: the constructs Fluree does not yet accept are listed under
[Not yet supported](#not-yet-supported), and each produces a clear error rather
than silently misbehaving.

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

## Running Cypher

Cypher is read/write — reads go through the query path, writes through the
transaction path. Both require a target ledger (Cypher has no `FROM`/dataset
clause). Read and write are split into separate endpoints/methods for parity
with SPARQL, but the *statement* determines what runs.

**Rust API**

```rust
// read
let result = fluree.query_cypher(&db, "MATCH (n:Person) RETURN n.name").await?;
// write
let committed = fluree.transact_cypher(ledger, "CREATE (n:Person {name: \"Alice\"})").await?;
```

Parameterized forms (`$param`) are available via `query_cypher_with_params` /
`transact_cypher_with_params`.

**CLI** — Cypher is auto-detected from a `.cypher`/`.cyp`/`.cql` file extension
or a leading `MATCH`/`CREATE`/`MERGE`/…; force it with `--cypher` (query) or
`--format cypher` (update):

```bash
fluree query my/ledger -e 'MATCH (n:Person) RETURN n.name' --cypher
fluree update my/ledger -f create.cypher
```

Cypher results default to **cypher-json** (a Neo4j-compatible tabular envelope
with native scalars); pass `--format jsonld` for the RDF JSON-LD form.

**HTTP** — send the statement with `Content-Type: application/cypher` to the
ledger-scoped query/update endpoints:

```bash
curl -X POST http://localhost:8090/v1/fluree/query/my/ledger \
  -H 'Content-Type: application/cypher' \
  --data 'MATCH (n:Person) RETURN n.name'

curl -X POST http://localhost:8090/v1/fluree/update/my/ledger \
  -H 'Content-Type: application/cypher' \
  --data 'CREATE (n:Person {name: "Alice"})'
```

The body may be raw Cypher, or a JSON envelope `{"cypher": "...", "params": {...}}`
(the Neo4j-HTTP shape). Responses are cypher-json; request RDF JSON-LD with
`Accept: application/ld+json`.

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

## Supported surface

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
- Variable-length paths `-[:T*]->`, `-[:T*m..n]->` (unbounded reuses
  the transitive `PropertyPath` operator; bounded ranges expand to a
  `Union` of fixed-length chains with relationship-uniqueness filters).
  An **unbounded** range may name a type *alternation*
  (`-[:A|B*]->`, `-[:A|B*0..]->`): the closure follows an edge of any
  listed type per hop (LDBC IC12's
  `[:HAS_TYPE|IS_SUBCLASS_OF*0..]`). Bounded alternation
  (`-[:A|B*1..3]->`) is still deferred — use the unbounded form.
- **Untyped** variable-length paths `-[*]->`, `-[*m..n]->` (no relationship
  type): a *wildcard* transitive path that follows **any** node→node edge per
  hop — excluding `rdf:type` (its object is a class, not a node) and the
  `f:reifies*` reifier bundle, and ignoring data properties (only node-valued
  edges are followed). Bounds become the path's `min_hops`/`max_hops`; a
  **bounded** range runs a layered (node, depth) BFS, so a node reachable in
  range is found even when a shorter path to it also exists (`-[*2..2]->` finds
  the length-2 path past a 1-hop edge), and the bound-bound and bound-unbound
  forms agree. These use **reachability** semantics (each in-range node once, not
  path enumeration or trail semantics). A direction is required; undirected
  untyped (`-[*]-`) is deferred, as is an **unbounded** lower bound above 1
  (`-[*2..]->` — give an upper bound or name a type).
- Undirected relationships `-[:T]-` (forward ∪ reverse `Union`).
- Path finding: `MATCH p = shortestPath((a)-[:T*]-(b))` and
  `allShortestPaths(...)`. Anchored (both endpoints bound by a
  preceding MATCH); unweighted bidirectional BFS over a single typed
  predicate, lowered to `Pattern::ShortestPath` and executed by
  `ShortestPathOperator`. `Single` mode binds one shortest path per
  input row; `All` mode emits one row per minimal-length path. The
  path binds to a `Binding::Path` (node sequence); `length(p)` is its
  hop count and `p IS NULL` (under `OPTIONAL MATCH`) detects "no path"
  — the IC13 shape. `nodes(p)` returns the node sequence and
  `pathPairs(p)` the consecutive node pairs (both list-valued, for
  `UNWIND`); `relationships(p)` (edge identities) is deferred.
- Scalar functions: `toString`, `toInteger`, `toFloat`, `coalesce`, `abs`,
  `toUpper`, `toLower`, `round`, `floor`, `ceil`/`ceiling`, `rand`. (`substring`
  and `replace` are deferred — they differ from the engine's 1-based `SUBSTR` /
  regex `REPLACE`; `sqrt`/`sign`/`split`/`trim`/`^` need new evaluators.)
- `WHERE` expressions: comparison, AND/OR/NOT, arithmetic +/-/*//,
  STARTS WITH / ENDS WITH / CONTAINS, IS NULL / IS NOT NULL,
  `expr IN [a, b, ...]`, `CASE WHEN ... THEN ... END` (simple and
  subject forms), `EXISTS { pattern }` and the subquery form
  `EXISTS { MATCH pattern WHERE expr }` (the inner `WHERE` is ANDed into
  the correlated existence test; outer-scope variables stay visible).
- Property accessors `n.prop` in expression position. Lowered by
  emitting an auxiliary `Optional((n, <prop IRI>, ?#__prop_n_prop))`
  before the consuming Filter/Bind/aggregate. The Optional wrap
  preserves Cypher's nullable property-access semantics: when `n`
  has no value for the key, the accessor evaluates to null instead
  of dropping the row. This makes `WHERE n.missing IS NULL`
  return nodes lacking the property, `RETURN n.name` return one
  row per matched node (with null where the property is absent),
  and `avg(n.age)` average across all matched nodes — skipping
  nulls — as Cypher users expect. `WHERE n.age > 30` continues to
  filter to age-bearing nodes above 30 (the `>` comparison on an
  unbound binding yields filter-context false). Bare-variable
  target only in v1; chained accessors (`n.a.b`) are rejected.
- ORDER BY (variable, property-accessor, or general expression keys —
  e.g. `ORDER BY toInteger(n.id)`), SKIP, LIMIT.
- `UNWIND [literals] AS x` — inline list literal unwinding, and
  `UNWIND <expr> AS x` over a runtime list (`UNWIND nodes(path) AS n`,
  `UNWIND range(1,5) AS i`) — a correlated operator fans each input row
  out over the list elements; a property accessor on the element
  correlates (`n.name`).
- List functions over a `collect()` list: `size`, `head`, `last`,
  `tail`, `reverse` (and `size`/`reverse` over a string). Usable in the
  final `RETURN` wrapping a collect, e.g. `RETURN size(collect(f.name))`;
  `collect()` nested in arithmetic is rejected.
- List literals `[a, b, …]` and structured `collect([a, b])` — collect
  per-row tuples into a list of lists (e.g.
  `RETURN collect([n.id, n.name])`).
- List indexing `list[i]` — 0-based element access; a negative index
  counts from the end (`list[-1]` is the last element). Out-of-range,
  non-integer index, or non-list yields null. An indexed element that is
  itself a node ref correlates downstream (`WITH pair[0] AS x ... x.name`).
- **List iteration** — list comprehensions `[x IN list WHERE pred | expr]`,
  `reduce(acc = init, x IN list | expr)`, and the list predicates
  `all/any/none/single(x IN list WHERE pred)`. The loop variable is scoped to
  the body and bound per element via a shared overlay; **property access on it
  works** (`[x IN nodes(p) | x.name]`, `[row IN $people | row.email]`,
  `reduce(s = 0, x IN xs | s + x.score)`) — a map element looks the key up, a
  node element scans the property at eval time. The list position may aggregate
  (`[x IN collect(p) | x.name]`). A null / non-list input yields null (not an
  empty list); empty-list identities are `all`/`none` = true, `any`/`single` =
  false. (`EXISTS { … }` inside a list-iteration body is rejected — it would
  need per-element async subquery evaluation; write-side `MATCH … WHERE` doesn't
  accept these forms either.)
- **Pattern comprehension** `[(a)-[:KNOWS]->(b) WHERE b.age > 30 | b.name]` — a
  correlated subquery that collects a projection over each match into a list.
  The inner pattern's existing variables (e.g. `a`) correlate with the outer
  row; new ones (`b`) are introduced in the subquery. Resolved asynchronously
  per outer row on the same machinery as `EXISTS`, so it can appear as a value
  anywhere a projection expression can — including nested
  (`size([(a)-->(b) | b])`). Write-side `MATCH … WHERE` doesn't accept it.
- Metadata functions: `labels(n)` returns the node's Cypher label strings
  (from live `rdf:type` assertions, overlay-aware); `type(r)` returns the
  relationship type string for a named relationship variable (from
  `f:reifiesPredicate` on the reifier). Unbound or non-node/non-rel
  arguments yield null.
- `pathPairs(p)` — the consecutive node pairs of a path value
  (`[[a,b],[b,c],…]`, each pair a two-element list). With `UNWIND`, this
  drives per-edge aggregation: `UNWIND pathPairs(p) AS pair` then
  `pair[0]` / `pair[1]` as the edge endpoints. The building block for
  IC14-style weighted path scoring — `reduce` over per-edge interaction
  counts becomes unwind-pairs → OPTIONAL MATCH → `count` → `sum`, grouped
  by the carried path.
- Map **values**: a map literal `{k: expr, …}` in expression position
  (`RETURN {name: n.name, age: n.age} AS person`), `properties(n)` (all of a
  node's data properties as a map — excluding labels, relationships, and the
  reifier sidecar; a multi-valued property becomes a list), `keys(n)` (the
  property names as a sorted list), and object `$params`
  (`$filter = {city: "NYC"}`). A map carries in a `Binding::Map` and renders as
  a JSON object — native (`{"name": "Alice"}`) in cypher-json. Map identity
  (DISTINCT / grouping) is key-order-insensitive; display preserves insertion
  order; duplicate literal keys resolve last-wins. Maps are projection/value
  constructs only — not RDF terms, so they can't be matched, indexed, or stored
  via `SET n.prop = {…}`. A computed entry may itself be an async subquery
  (`{ok: EXISTS { (p)-[:KNOWS]->(:Person) }}`) — it is resolved per row on the
  same machinery as a bare `EXISTS`.
- **Map projection** `n{.name, .age, computed: n.age + 1}` — build a map from a
  node variable: `.key` selectors desugar to `key: n.key`, `key: expr` adds an
  explicit entry, and `n{.*}` projects every data property (equivalent to
  `properties(n)`). Mixing `.*` with other selectors is deferred (use
  `properties(n)` or list the keys).
- Aggregates: `count(*)`, `count(x)`, `count(DISTINCT x)`,
  `sum(x)`, `avg(x)`, `min(x)`, `max(x)`. Arguments may be a bare
  variable (`count(n)`), a property accessor (`avg(n.age)`), a list
  literal (`collect([n.id, n.name])`), or a scalar expression
  (`sum(n.age * 2)`, lowered through a pre-aggregation `Bind`). Mixed projections
  (`RETURN n, count(*) AS c`) implicitly group by the non-aggregate
  projections.
- `WITH ... [WHERE/ORDER BY/SKIP/LIMIT/DISTINCT]` and `WITH *` — subquery
  boundary. WHERE that references aggregate aliases lowers to HAVING
  rather than a pre-aggregation Filter. Nested WITHs nest Subqueries.
- `CALL [(a, b)] { … }` — a read-only subquery clause in the pipeline. The
  scope clause `(a, b)` imports outer variables (the subquery is correlated on
  them); `CALL { … }` with no scope clause runs once and broadcasts its result.
  The body is `MATCH` / `OPTIONAL MATCH` / `WITH` / `UNWIND` / nested `CALL`
  ending in `RETURN` (explicit columns, not `*`); outer rows flow in and the
  RETURN columns continue downstream. The body may be a `UNION` / `UNION ALL`
  of branches with a common column shape (`UNION` dedups per correlation group;
  every branch references the same imports and projects the same columns).
  A correlated aggregating CALL (`CALL (p) { … RETURN count(f) }`) is grouped
  per import, so an import with **zero inner matches yields no row** — wrap the
  inner `MATCH` in `OPTIONAL MATCH` to retain it as a `0`. **Scope is strict:**
  every import must already be bound outside, a RETURN may not re-bind any
  outer name, and the body may not reuse an outer variable's name internally
  without importing it (rename it, or add it to the scope clause). Deferred:
  writes inside `CALL` and `CALL (*)` (import-all).
- `RETURN n`, `RETURN n, m`, `RETURN *`, `RETURN DISTINCT ...`,
  `RETURN expr AS alias` (lowered via `Bind`).
- `UNION` and `UNION ALL` at the RETURN boundary. Every branch must
  project the same VarIds in the same order; mixing `UNION` and
  `UNION ALL` in one chain is rejected (matches the openCypher
  spec). `RETURN *` is also rejected in UNION branches because its
  projected-vars list is opaque at lower time.

### Writes

- **`CREATE`** — nodes and relationships. Directed typed relationships emit a
  base triple plus a reifier bundle (LPG-mode default for Cypher); multiple
  parallel relationships in one `CREATE` mint distinct annotation subjects
  automatically.
  ```cypher
  CREATE (a:Person {name: "Alice"})-[:WORKS_FOR {role: "Engineer"}]->(b:Org {name: "Acme"})
  ```
- **`SET`** — set/overwrite a property (`SET n.age = 30`), merge a map
  (`SET n += {age: 30, city: "X"}`), replace scalar node properties with a map
  (`SET n = {name: "Alice"}`), or add a label (`SET n:Admin`). Map replace
  removes prior scalar node properties while preserving labels, relationships,
  and relationship sidecar metadata.
- **`REMOVE`** — remove a property (`REMOVE n.age`) or a label (`REMOVE n:Admin`).
- **`DELETE` / `DETACH DELETE`** — delete nodes/relationships. `DETACH DELETE`
  removes a node together with its relationships.
- **`MERGE`** — find-or-create for a single node
  (`MERGE (n:Person {name: "Alice"})`) or a single relationship path, in two
  forms:
  - **Standalone** — the whole pattern is the match key, treated atomically:
    `MERGE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})`. If
    *no* matching path exists, the entire path is created with **fresh** nodes
    for both endpoints — even if a node matching one endpoint already exists.
    (To reuse existing endpoints, bind them with a leading `MATCH` — the
    per-row form below.)
  - **Per-row (leading `MATCH` binds the endpoints)** — find-or-create the edge
    for each matched pair, reusing the bound nodes:
    `MATCH (a:Person), (b:Person) WHERE a.name <> b.name MERGE (a)-[:KNOWS]->(b)`.
    The edge is created only for pairs that don't already have it. An endpoint
    *introduced* by the `MERGE` (not bound by the `MATCH`) is created per row —
    e.g. `MATCH (a:Person) MERGE (a)-[:HAS_PET]->(p:Pet {name: "Rex"})` creates
    one `Pet` per matched `a`.

  > **Cartesian-product warning:** a per-row `MERGE` over an unfiltered
  > multi-node `MATCH` (`MATCH (a:Person), (b:Person) MERGE (a)-[:KNOWS]->(b)`)
  > considers every ordered pair — O(n²) candidate edges. Add a selective
  > `WHERE` (as above) unless a full cross-product is intended.

  `ON CREATE SET` is supported on both forms (and may target either endpoint
  node variable). `ON MATCH SET` is supported on **single-node** `MERGE` only
  (deferred on a relationship `MERGE`). Resolved by probing the current writer
  state, then staging either a create or an update.

  Style note: write bound endpoints **bare** in the `MERGE` pattern
  (`MATCH (a:Person) MERGE (a)-[:T]->(b)`). Repeating a label on a bound
  endpoint (`MERGE (a:Person)-[:T]->(b)`) re-asserts its `rdf:type` triple when
  the edge is inserted — idempotent in RDF, but redundant.
- **`MATCH … CREATE/SET/REMOVE/DELETE`** — pattern-driven write templates (find
  rows, then write per match). Write-side `MATCH` supports labels, inline
  property filters, directed single-typed relationships, and scalar `WHERE`
  filters over the same comparison/boolean/string/property-accessor expression
  surface used by reads. `CASE` / `EXISTS` inside write-side `WHERE` are still
  deferred.
- **`MATCH … WITH … <write>`** — a `WITH` between the match and the write,
  limited to the *horizon subset*: pass-through variables (`WITH a, b`), renames
  (`WITH a AS p`), computed (non-aggregate) aliases carried into the write
  (`WITH a, a.birthYear + 30 AS adultAt SET a.adultAt = adultAt`), and a
  post-projection `WHERE` that gates which rows are written
  (`WITH p, p.age AS age WHERE age >= 30 SET p.adult = true`). `WITH` applies
  Cypher scoping — only projected names are visible to the write. Works before
  `CREATE` / `SET` / `REMOVE`; `WITH` before `DELETE` is rejected (delete
  resolution keys off the raw MATCH variables and can't honor a rename/horizon —
  `DELETE` directly off the MATCH variables). Aggregation, `DISTINCT`, and
  `ORDER BY` / `SKIP` / `LIMIT` on a write-side `WITH` are deferred.

```rust
let committed = fluree.transact_cypher(ledger, cypher).await?;
```

Writes default to LPG mode, where every relationship reifies (carries an
annotation identity). See [Edge annotations](edge-annotations.md) for the RDF
vs. LPG modes and the retraction semantics that follow from them.

## Not yet supported

These constructs are part of openCypher but Fluree does not yet accept them; each
produces a clear error rather than a silent wrong answer.

**Patterns and paths**

- Bare `MATCH (n)` — a node must be constrained by a label, a property, or a
  relationship.
- Free path values `MATCH p = (...)` without a `shortestPath` /
  `allShortestPaths` wrapper.
- Binding a relationship variable to a variable-length path (`-[r:T*]->`).
- Undirected untyped variable-length paths (`-[*m..n]-` — give a direction);
  unbounded untyped paths with a lower bound above 1 (`-[*2..]->` — add an upper
  bound or name a type); zero-length *typed* bounded paths (`-[:T*0..M]->` — use
  `*1..M`); bounded type alternation (`-[:A|B*1..3]->` — use the unbounded form);
  property filters on a variable-length or shortestPath relationship.

**Functions**

- `relationships(p)`, `id(x)`, `point`, `distance`.
  (`labels(n)`, `type(r)`, `nodes(p)`, `pathPairs(p)`, `keys(n)`,
  `properties(n)`, map literals (`{k: v}`), object `$params`, and the list
  functions *are* supported — see above.)

**Expressions**

- `^` (exponent).
- Chained property accessors (`n.a.b` — bind an intermediate via `WITH`).
- `NULL` literals; aggregates inside `CASE` / `EXISTS`.

**Clauses and structure**

- Non-literal `SKIP`/`LIMIT`; `ORDER BY` on a `collect()` list.
- `CASE` / `EXISTS` inside a write-statement `MATCH ... WHERE`. Aggregation,
  `DISTINCT`, and `ORDER BY` / `SKIP` / `LIMIT` on a `WITH` before a write clause,
  and `WITH` before `DELETE` (the pass-through / rename / computed-alias / filter
  subset before `CREATE` / `SET` / `REMOVE` *is* supported — see above).
- `MERGE` on a property-bearing relationship (`-[:KNOWS {since: 2020}]->`),
  multi-hop or multi-part (comma-separated) `MERGE`, multiple `MERGE` clauses,
  `ON MATCH SET` on a relationship `MERGE`, and `MERGE` combined with another
  write clause in the same statement.
- `CALL proc(...)` stored/builtin procedures (the `CALL { … }` read subquery
  *is* supported — see above), `LOAD CSV`, `FOREACH`, schema DDL.
- Multi-statement scripts — submit one statement per request.

## See also

- [Edge annotations (concept)](edge-annotations.md) — the storage primitive
  Cypher relationships sit on top of.
- [Edge annotations (storage internals)](../design/edge-annotations.md) — the
  `f:reifies*` durable encoding.
- [SPARQL](../query/sparql.md) and [JSON-LD Query](../query/jsonld-query.md) —
  the parallel surfaces over the same IR.
