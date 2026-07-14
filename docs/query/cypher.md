# Cypher

Fluree supports [openCypher 9][opencypher] queries. A Cypher
relationship-with-properties — `(a)-[:WORKS_FOR {role: "..."}]->(b)` — maps to
Fluree's edge-annotation primitive ([concept](../concepts/edge-annotations.md),
[internals](../design/edge-annotations.md)), so property-graph edges and RDF
quoted-triple annotations are the same data read from two angles.

The same database is queryable through JSON-LD, SPARQL, and Cypher at once — one
underlying store, no separate copy or sync step, so data written through any
surface is immediately visible to the others. Because each surface is its own
query language over that shared data, there are some subtle differences to be
aware of — see [Differences from Neo4j](#differences-from-neo4j) at the end for
the model divergences and the handful of deferred forms.

[opencypher]: https://opencypher.org/resources/

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

**Bulk loading** — a `.cypher` dump of `CREATE` / `MATCH … CREATE` statements
(the Neo4j/Memgraph export idiom) should not be replayed statement-by-statement.
`fluree create <ledger> --from dump.cypher` converts it on the fly and loads it
through the chunked bulk-import pipeline; see
[the create command](../cli/create.md#property-graph-imports-csv--cypher).

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
- **Variable-length relationship / path binding.** A var-length relationship
  may bind a variable: `-[r:T*…]->` binds `r` to the list of relationship
  values per match, and `MATCH p = (a)-[:T*…]->(b)` binds `p` to a path value.
  Two execution strategies, chosen automatically:
  - **Bounded typed directed** ranges expand to fixed-length chain branches,
    each constructing the value from its nodes.
  - Everything else — **unbounded** (`-[r:T*]->`, `p = (a)-[:T*]->(b)`),
    **untyped/wildcard**, **undirected**, zero lower bounds (`*0..`), and
    lower bounds above 1 — runs the **path-enumeration** search: a DFS from
    the (anchored) start emitting one row per path whose hop count is in range.
    The end node binds per path when free, or filters the enumeration when
    already bound. The search enforces Cypher **relationship-uniqueness** (trail
    semantics — no edge is traversed twice, but a node may be revisited via a
    different edge; e.g. the triangle closure `a→b→c→a` is a valid 3-hop path),
    matching Neo4j. It is guarded by visited/path caps that **error** rather than
    silently truncate — narrow dense patterns with hop bounds, a bound end, or a
    type.

  A **fixed** single hop also takes a path variable
  (`MATCH p = (a)-[:T]->(b)` is a `*1..1` path), as does a **multi-hop chain**
  of fixed single-typed directed hops (`p = (a)-[:R1]->(b)<-[:R2]-(c)`) — the
  path value is built from the bound nodes and per-hop relationship values.
  Deferred: a variable-length or undirected segment inside a multi-hop path
  value, binding over a type alternation, and property filters on a
  var-length relationship.
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
  — the IC13 shape. `nodes(p)` returns the node sequence,
  `pathPairs(p)` the consecutive node pairs, and `relationships(p)` the
  per-hop relationship values (all list-valued, for `UNWIND` / list functions).
- **Relationship values.** A relationship is a value carrying its start node,
  type, and end node (`Binding::Rel`); it comes from a bound `-[r:T]->` (the
  reified edge), `relationships(p)`, or a bound var-length relationship.
  `type(r)` is the relationship type string, `startNode(r)` / `endNode(r)` its
  endpoints, `properties(r)` / `r.prop` its edge properties (present only for a
  reified/annotated edge — a plain path edge has none). Rendered as a
  `{start, type, end}` object.
- Scalar functions:
  - **Casts / general:** `toString`, `toInteger`, `toFloat`, `coalesce`.
  - **String:** `toUpper`, `toLower`, `substring` (0-indexed; 2- and 3-arg),
    `left`, `right`, `trim`, `ltrim`, `rtrim`, `replace` (literal replace-all),
    `split` (→ list).
  - **Math:** `abs`, `round`, `floor`, `ceil`/`ceiling`, `rand`, `sqrt`,
    `sign`, `log` (natural logarithm), and the `^` exponent operator
    (right-associative).
  - **Identity:** `id(n)` / `elementId(n)` return the node/relationship's
    stable **identity string** (the stored name — a full IRI in `@vocab`
    mode; see [names](#names-and-opting-into-iris)). Fluree has no integer
    element id (differs from Neo4j's integer `id`).
- `WHERE` expressions: comparison, AND/OR/XOR/NOT, arithmetic `+ - * / %`, `^`,
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
- Temporal component accessors — `d.year`, `d.month`, `d.day`, `d.hour`,
  `d.minute`, `d.second` extract a component of a date/dateTime-valued property
  as an integer (e.g. `WHERE p.birthDate.year < 1990`). This is the one
  property-accessor chain that is *not* rejected.
- Temporal constructors — `date('2024-01-15')`, `datetime('2024-01-15T10:00:00Z')`,
  `time('10:00:00')`, `duration('P1D')` fold a constant lexical argument to a
  typed value, usable in comparisons (`WHERE e.at > datetime('…')`) and as
  write property values (`SET n.created = datetime()`). Zero-arg `datetime()` /
  `date()` are the current instant / date; in a write statement every zero-arg
  constructor sees the same instant. Component maps (`date({year: 2024})`),
  non-constant arguments, and duration arithmetic are deferred.
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
  rather than a pre-aggregation Filter. Nested WITHs nest Subqueries. A
  `collect()` projected by a `WITH` carries forward as a real list to the next
  stage (`WITH p, collect(f) AS fs … RETURN size(fs)` / `UNWIND fs …`); only
  `ORDER BY` directly on a collected list is rejected (sorting a list value is
  unsupported in v1).
- `CALL [(a, b) | (*)] { … }` — a read-only subquery clause in the pipeline.
  The scope clause `(a, b)` imports those outer variables (the subquery is
  correlated on them), `(*)` imports the whole visible outer scope, and
  `CALL { … }` with no scope clause runs once and broadcasts its result.
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
  without importing it (rename it, or add it to the scope clause, or use
  `CALL (*)`). Deferred: writes inside `CALL`.
- `RETURN n`, `RETURN n, m`, `RETURN *`, `RETURN DISTINCT ...`,
  `RETURN expr AS alias` (lowered via `Bind`).
- `UNION` and `UNION ALL` at the RETURN boundary. Every branch must
  project the same VarIds in the same order; mixing `UNION` and
  `UNION ALL` in one chain is rejected (matches the openCypher
  spec). `RETURN *` is also rejected in UNION branches because its
  projected-vars list is opaque at lower time.

### Procedures (introspection shims)

Graph tooling — Neo4j Browser, LangChain, driver smoke tests — introspects the
database through built-in procedures before it issues real queries. Fluree
answers the common ones directly from ledger statistics (novelty-merged, no
scan), so they are instant even on large ledgers:

| Procedure | Answers |
|---|---|
| `CALL db.labels()` | Distinct node labels (classes), sorted. |
| `CALL db.relationshipTypes()` | Distinct relationship types (predicates whose objects are nodes; `rdf:type` excluded). |
| `CALL db.propertyKeys()` | Distinct property keys (predicates with literal values). |
| `CALL db.schema.visualization()` | One row: `nodes` / `relationships` summary lists (best effort). |
| `CALL dbms.components()` | Compatibility identity (mirrors the Bolt handshake's `Neo4j/<version> (compatible; Fluree/…)`). |
| `CALL apoc.meta.data()` | Per-(label, property) schema rows — node properties with meta types (`STRING`/`INTEGER`/…) and outgoing relationships (`type: "RELATIONSHIP"`, `other` = end labels). Covers the LangChain `Neo4jGraph` schema queries verbatim. |

The full call form composes like any read — after the `YIELD` the statement
continues with ordinary read clauses
(`CALL proc() [YIELD * | col [AS alias], … [WHERE …]] [WITH/UNWIND/MATCH …] [RETURN …]`):

```cypher
CALL db.labels() YIELD label WHERE label STARTS WITH "P" RETURN label ORDER BY label
```

```cypher
CALL apoc.meta.data()
YIELD label, other, elementType, type, property
WHERE type = "RELATIONSHIP" AND elementType = "node"
UNWIND other AS other_node
RETURN {start: label, type: property, end: toString(other_node)} AS output
```

Names render through the ledger's default context (`@vocab` stripped, term
overrides reversed), so `db.labels()` returns the identifiers you would write
in a `MATCH`. Like Neo4j's own catalog procedures, answers are lenient about
tombstones: a label or key whose facts were all retracted may keep appearing
until a reindex. A procedure call stands alone as its own statement (it can't
follow a `MATCH`), and unsupported procedures (e.g. `apoc.*`) fail with an
error listing the supported set.

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
- **`FOREACH`** — unroll a write over a **constant** list (inline literal,
  constant `range()`, or a `$param` array), running a `CREATE` / `SET` /
  `REMOVE` body per element:
  ```cypher
  FOREACH (n IN range(1, 3) | CREATE (:Ping {n: n}))
  ```
  Bodies unroll at parse time (≤ 10000 iterations; same-property `SET` is
  last-wins). Deferred: runtime lists (e.g. a collected list) and
  `MERGE` / `DELETE` / nested `FOREACH` bodies.
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

  A property-bearing relationship pattern (`MERGE (a)-[:IN {since: 2020}]->(b)`)
  matches only an edge whose properties carry those values — a different value
  creates a parallel edge, per Cypher.

  `ON CREATE SET` is supported on both forms and may target endpoint node
  variables or the relationship variable (`ON CREATE SET r.checks = 1`).
  `ON MATCH SET` is supported on single-node `MERGE`, on **standalone**
  relationship `MERGE` (resolved by probing the current writer state, then
  staging either branch), and on the **per-row** relationship form (leading
  `MATCH`). The per-row form decomposes into two branches over the same
  leading `MATCH` — an `ON MATCH SET` over the rows whose edge already exists,
  then a create (`ON CREATE SET`) over the rows whose edge is absent — staged
  into a **single atomic commit** (either both branches publish, or an error
  returns with nothing committed). `ON MATCH SET` on a per-row *node* MERGE
  (leading `MATCH` before a node `MERGE`) stays deferred — there is no
  relationship to partition the rows on.

  A single-node or standalone relationship `MERGE` also takes trailing `SET`
  clauses, which apply on *both* branches — the standard upsert idiom:

  ```cypher
  MERGE (n:User {id: $id}) SET n += $props
  ```

  The map side of `SET n = …` / `SET n += …` may be a whole-map parameter
  (`$props` above) or an inline `{k: v}` literal.

  Style note: write bound endpoints **bare** in the `MERGE` pattern
  (`MATCH (a:Person) MERGE (a)-[:T]->(b)`). Repeating a label on a bound
  endpoint (`MERGE (a:Person)-[:T]->(b)`) re-asserts its `rdf:type` triple when
  the edge is inserted — idempotent in RDF, but redundant.
- **`MATCH … CREATE/SET/REMOVE/DELETE`** — pattern-driven write templates (find
  rows, then write per match). Write-side `MATCH` supports labels, inline
  property filters (on nodes and relationships — `-[r:T {w: 3}]->` filters on
  the relationship's properties), directed single-typed relationships, and
  scalar `WHERE` filters over the same comparison/boolean/string/property-
  accessor expression surface used by reads. `CASE` / `EXISTS` inside write-side `WHERE` are still
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
- **`… RETURN <created>`** — a trailing `RETURN` of *created* entities:
  `CREATE (n:Person {name: "Alice"}) RETURN n`,
  `MATCH (a), (b) CREATE (a)-[e:KNOWS]->(b) RETURN e` (one row per matched
  pair). Answered as the read path's Cypher-JSON tabular envelope; each entity
  serializes as its identifier string. v1 surface: bare variables (optionally
  aliased) naming a fresh `CREATE` node or relationship variable. Deferred:
  expressions, `RETURN` modifiers, `MATCH`-bound variables, and `RETURN` with
  `MERGE` (the matched branch's node isn't a created entity).

```rust
let committed = fluree.transact_cypher(ledger, cypher).await?;
// or, when the statement ends in RETURN:
let (committed, rows) = fluree.transact_cypher_returning(ledger, cypher, None).await?;
```

The transact API also accepts a semicolon-separated **script** of write
statements, executed sequentially with one commit per statement — later
statements see earlier ones' effects, and only the final statement may carry
a `RETURN` (cypher-shell autocommit semantics; a failure aborts the remainder
but keeps prior commits — use an explicit Bolt transaction for atomicity):

```rust
let committed = fluree
    .transact_cypher(
        ledger,
        r#"CREATE (:Person {name: "Alice"});
           CREATE (:Person {name: "Bob"});
           MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"})
           CREATE (a)-[:KNOWS]->(b);"#,
    )
    .await?;
```

To **bulk-load a CSV**, use the CLI's [`fluree load`](../cli/load.md) — Fluree's
`LOAD CSV` analog. It reads the file client-side and streams it as batched
per-row upserts, one commit per batch. Each row binds as `row` inside
`UNWIND $batch AS row …` (`--cypher`), or the batch is injected as an update's
`values` clause (`--jsonld`):

```bash
fluree load people --from people.csv \
  --cypher 'MERGE (n:Person {id: row.id}) SET n.name = row.name'
```

The same per-row upsert shape works directly on the transact API:
`UNWIND $batch AS row MERGE (n:Person {id: row.id}) SET n.name = row.name`, with
`$batch` a parameter array of row maps.

Writes default to LPG mode, where every relationship reifies (carries an
annotation identity). See [Edge annotations](../concepts/edge-annotations.md) for the RDF
vs. LPG modes and the retraction semantics that follow from them.

## How Cypher maps to RDF

You don't need this to write Cypher — it's here for when you want to see how a
statement lands in Fluree's store, or cross-reference data written through
JSON-LD or SPARQL.

| Cypher concept | Fluree representation |
|---|---|
| Node `(n:Label)` | Subject with `rdf:type <Label>`. |
| Multiple labels `(n:L1:L2)` | Multiple `rdf:type` triples about `n`. |
| Node properties `(n {key: val})` | Ordinary triples about `n`. |
| Relationship `(a)-[:TYPE]->(b)` | Base triple `(a, <TYPE>, b)`. |
| Relationship with `var` `(a)-[r:TYPE]->(b)` | Base triple + an `f:reifies*` reifier bundle; `r` binds the reifier subject. |
| Relationship properties `[:T {p:v}]` | Reifier bundle plus an annotation-body triple `(_:r, p, v)`. |
| Parallel relationships | Multiple reifier subjects attached to the same base edge. |

### Relationship lowering — three shapes, three behaviors

How a relationship lowers depends on whether you bind it and whether you filter
on its properties — which in turn decides the cardinality and whether plain
(un-annotated) base edges are visible.

| Pattern | Lowers to | Cardinality | Sees plain RDF? |
|---|---|---|---|
| `(a)-[:T]->(b)` | Plain triple `(a, <T>, b)` | Set | Yes |
| `(a)-[r:T]->(b)`, `r` **value-only** | Plain triple + `OPTIONAL { EdgeAnnotation }` + `r = coalesce(annotation, MakeRel(a, T, b))` | Bag over annotations; one row for an unreified edge | Yes |
| `(a)-[r:T]->(b)`, `r` **property-read** | `EdgeAnnotation { edge, annotation: ?r, body: [] }` | Bag | No — only reifier-bundled edges |
| `(a)-[:T {p:v}]->(b)` | `EdgeAnnotation { edge, annotation: ?#__anon, body: [(?#__anon, p, v)] }` | Bag | No |

A bound relationship variable is **value-only** when the statement never reads
its properties — no `r.prop`, `properties(r)`, `keys(r)`, or map projection
`r{…}` anywhere (a statement-wide scan decides this at lowering). Value-only
uses (`RETURN r`, `type(r)`, `startNode(r)` / `endNode(r)`, comparisons,
`collect(r)`) are satisfied by a relationship value synthesized from the base
triple, so plain (un-annotated) RDF edges match too; edges that *do* carry
reifier bundles still bind one row per annotation (parallel relationships stay
distinct).

**Consequence.** Only a *property-reading* relationship variable requires
reifier bundles. If your data was loaded via JSON-LD without `@annotation`
(or any other path that doesn't produce reifier bundles),
`MATCH (a)-[r:T]->(b) RETURN r.since` returns zero rows even though the base
triples exist — there is no annotation node to read `since` from:

```cypher
-- value-only r: sees all base edges, plus per-annotation rows where reified
MATCH (a:Person)-[r:WORKS_FOR]->(o:Organization) RETURN a, type(r), o

-- property read on r: requires reifier bundles
MATCH (a:Person)-[r:WORKS_FOR]->(o:Organization) RETURN a, r.since, o

-- set semantics, sees all base edges
MATCH (a:Person)-[:WORKS_FOR]->(o:Organization) RETURN a, o
```

### Names, and opting into IRIs

Cypher uses bare names like `Person`, `WORKS_FOR`, `name`. **By
default they are just names** — no IRI prefix is invented for them.
Internally they live under namespace code 0 (the empty prefix), so a
label written as `Person` reads back as `Person` on every surface:
`labels(n)`, Bolt, and even JSON-LD/SPARQL queries against the same
ledger see the same bare (relative) name. A pure-Cypher user never
sees or configures a namespace.

To interoperate with RDF-style data (full IRIs), configure the
ledger's default `@context` — the same context that applies to JSON-LD
queries against that ledger:

- `@vocab` supplies the namespace prefix: `Person` then resolves to
  `<vocab>Person`, matching data whose IRIs live under that vocab.
- Full-term mappings (e.g. `"Person": "http://schema.org/Person"`) act
  as per-name overrides (they work with or without `@vocab`).

The mapping is **case-preserving**: `WORKS_FOR` becomes
`<vocab>WORKS_FOR`, not `<vocab>worksFor`. Put any case-normalizing
aliases in the context.

The placement rule for a name (without `@vocab`) is: **no colon →
the whole name, verbatim**. Backticked names containing `/`, `#`,
spaces, or `@` (`` `a/b` ``, `` `my prop` ``, `` `user@host` ``) are
never split into namespaces — they round-trip intact. A backticked
name that *does* contain a colon is treated as an RDF identifier
(prefixed name or full IRI): `` `ex:code` `` or
`` `http://schema.org/name` `` registers its namespace and
interoperates with SPARQL/JSON-LD views of the same data.

Note the two modes address different data: bare names and
vocab-resolved IRIs are different identifiers. Adding `@vocab` to a
ledger whose data was written bare (or vice versa) changes what Cypher
statements match.

### Identifiers and keywords

Keyword tokens (`count`, `end`, `order`, `limit`, `all`, …) are accepted
as binding names — both as aliases (`RETURN n.name AS end`,
`WITH count(*) AS count`, `UNWIND xs AS end`, `YIELD col AS type`) and
when referenced downstream as plain variables
(`WITH count(*) AS count WHERE count > 5 RETURN count`). This is a
deliberate leniency over strict openCypher, which reserves these words
and requires backticking (`` AS `count` ``) — backticked identifiers are
also accepted. The dedicated meanings win where a keyword is followed by
its delimiter: `count(*)`, `exists { … }`, and `all(x IN … )` still
parse as their constructs.

## Differences from Neo4j

Fluree implements the openCypher 9 surface faithfully — the common clause,
pattern, and expression set works as specified, and anything unsupported
returns a **clear error, never a silently wrong result**. A few differences are
worth knowing, and they fall into two kinds.

**Divergent by design** — inherent to running Cypher over an RDF store, and
here to stay:

- **Nodes are durable subjects, not opaque LPG nodes.** `labels(n)` are
  `rdf:type` assertions; node identity is the subject's stored name (a plain
  name by default, a full IRI in `@vocab` mode — see
  [Names, and opting into IRIs](#names-and-opting-into-iris)).
- **Relationships are edge annotations.** `-[r:T]->` reifies the base triple
  `(s, p, o)` into a reifier node (the edge identity) — the same RDF 1.2 model
  SPARQL exposes via the `{| … |}` annotation tail and `rdf:reifies <<( s p o )>>`
  triple terms. So an edge is a *reifier over* a triple, not a triple term
  stored as a value (triple terms are supported as the object of `rdf:reifies`,
  not free-standing). See [How Cypher maps to RDF](#how-cypher-maps-to-rdf) and
  [Edge annotations](../concepts/edge-annotations.md).
- **`id(n)` / `elementId(n)` return the identity string**, not an integer — RDF
  subjects have no integer element id. Over Bolt, `xsd:decimal` renders as
  Float (Neo4j parity, precision loss); integer division yields decimals, so
  this shows on ordinary `a / b`.
- **No implicit per-statement transaction id.** Immutability and time-travel
  (`f:t`, history queries) replace those semantics.

**Deferred (fringe / on request)** — rejected with a clear error until a use
case pulls them in; each has a workaround:

- Bounded type-alternation var-length `-[:A|B*1..3]->` — use the unbounded form
  `-[:A|B*]->`.
- Spatial `point()` / `distance()`, and `duration` arithmetic (`date + duration`).
- Chained property access `n.a.b` (except temporal field chains like
  `x.date.month`) and mixing `.*` with named selectors in a map projection.
- `ORDER BY` over a list/map value, and `neo4j://` cluster routing (use
  `bolt://` direct).

Everything else — the full clause/pattern/expression surface, the write path,
procedures, and Bolt driver support — works; when in doubt, try it and read the
error, which names the unsupported form and its workaround.

## See also

- [Edge annotations (concept)](../concepts/edge-annotations.md) — the storage primitive
  Cypher relationships sit on top of.
- [Edge annotations (storage internals)](../design/edge-annotations.md) — the
  `f:reifies*` durable encoding.
- [SPARQL](sparql.md) and [JSON-LD Query](jsonld-query.md) —
  the parallel surfaces over the same IR.
