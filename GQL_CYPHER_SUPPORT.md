# GQL / Cypher Support — Implementation Plan

> **Implementation status — updated 2026-06-14.** The openCypher v1 work
> (the `fluree-db-cypher` crate + write-path lowering + `fluree-db-api`
> entrypoints) has been merged onto `feature/edge-annotations`. The
> sections below are the original *plan* (the target surface); this
> banner and the **Implementation status** section immediately following
> record what is actually built versus deferred, and why. When the two
> disagree, the status section wins.

## Implementation status

**Reachable three ways:** the Rust library API (`Fluree::query_cypher`,
`Fluree::transact_cypher`), the CLI (`fluree query --cypher …`,
`fluree update --format cypher …`, plus auto-detection by lead keyword /
`.cypher` extension), and **HTTP** — `POST /v1/fluree/query/<ledger>` and
`POST /v1/fluree/update/<ledger>` with `Content-Type: application/cypher`
(read returns JSON-LD; write returns the standard transact response). The
connection-scoped (no-ledger) routes reject Cypher with a pointer to the
ledger-scoped endpoint. Server writes go through the same cached-handle
commit path as SPARQL UPDATE so the in-memory cache stays current.

### Done

| Area | Shipped | Notes |
|---|---|---|
| Read path | `MATCH` (labels, multi-label, inline `{prop}`), directed/typed/untyped/alternation relationships, anonymous vs named (EdgeAnnotation), `OPTIONAL MATCH`, `WHERE` (comparisons, boolean, `STARTS/ENDS/CONTAINS`, `IN`, `IS NULL`, `CASE`, `coalesce/length/toString/toInteger/toFloat/abs`, `n.prop`), `WITH … WHERE/ORDER BY/SKIP/LIMIT`, `UNWIND [literal]`, `RETURN`/`DISTINCT`/`AS`, aggregates `count/sum/avg/min/max`, `ORDER BY/SKIP/LIMIT`, `UNION`/`UNION ALL`, `CALL { subquery }`, `EXISTS` | Lowers to the shared query IR. |
| Write — `CREATE` | Pure pattern CREATE (nodes + directed typed relationships + reifier bundle under LPG default) | `TxnType::Insert`. |
| Write — `MATCH … SET` | `SET n.prop = lit`, `SET n += {…}`, `SET n:Label` | `TxnType::Update`; property forms use an OPTIONAL old-value WHERE binding so the prior value is retracted and the new one asserted (single-valued replace; absent property skips the delete). Labels are additive. |
| Write — `MATCH … REMOVE` | `REMOVE n.prop`, `REMOVE n:Label` | `TxnType::Update`. |
| Write — `SET r.prop` / `REMOVE r.prop` | A **named** relationship in a write MATCH (`-[r:T]->`) binds `r` to the annotation SID via an `EdgeAnnotation` pattern, so SET/REMOVE on `r` update the relationship's metadata | Only reifier-bundled edges match (every LPG/Cypher-written relationship). |
| Write — `MATCH … CREATE` | Template-driven writes: CREATE nodes bound by MATCH reference the matched node; unbound vars mint new nodes per solution | `TxnType::Update`. |
| Write — `DETACH DELETE n` | Retracts every triple touching `n` in both directions (outbound + inbound var-predicate OPTIONAL scans → delete templates); the `f:reifies*` cascade auto-removes reifier bundles | Pure lowering, no probe. Var-predicate scans run with `include_system_facts = false` (write-WHERE default) so the inbound scan never touches reserved predicates. Sets `lpg_edge_lifecycle = true` so relationship body metadata is cascaded too. Tested on memory/overlay **and** indexed paths. |
| Write — single-node `MERGE` | `MERGE (n:Label {props}) [ON CREATE SET …]` — find-or-create as a **single Txn**: the identifying pattern becomes a `NOT EXISTS` guard, so a fresh node + `ON CREATE SET` inserts fire only when no match exists; a match makes it a no-op | Depends on the SPARQL-UPDATE staging fix (zero-row WHERE = no-op). |
| Write — `MERGE … ON MATCH SET` | Resolved as a **conditional write** (`WritePlan`): probe the identifying pattern, then stage either the create branch (absent) or `MATCH (pattern) SET <on_match>` (present) | On the server (cached-handle) path the conditional resolves **under the write lock**, so the branch-choosing probe and the staged branch see the same writer state (probe + stage atomic; no in-place retry — same as the SPARQL-UPDATE prebuilt-`Txn` path). The owned-ledger path probes and stages against the same `LedgerState` value. The MERGE node must have a variable. Relationship MERGE / `MATCH … MERGE` deferred. |
| Write — bare `DELETE n` | Conditional write: probe each matched node for a (reified) relationship in either direction; **error** if any, otherwise stage the node retraction (via the `DETACH DELETE` lowering — equivalent with no relationships) | Same under-lock resolution as MERGE on the server path. Only **mandatorily-bound** targets are accepted — an `OPTIONAL MATCH`-only target is rejected (it can be unbound on some rows, where the probe could bind an unrelated node). Probes **reified** relationships only — that is exactly the Cypher relationship contract here (named `-[r:T]->` lowers to an `EdgeAnnotation`; anonymous `-[:T]->` is plain RDF). |
| Write — `DELETE r` | Conditional write: for each matched relationship variable, probe whether its base `(a)-[:T]->(b)` triple backs a **parallel sibling** (`WITH a, b, count(DISTINCT r) … HAVING count > 1`); **error** if so, otherwise lower to a base-edge retraction template — the `f:reifies*` cascade clears the bundle | Both endpoints must be named so the probe can group by them (anonymous endpoint → error). `count(DISTINCT r)` counts relationship identities, not solution rows, so unrelated MATCH multiplicity can't false-trip the guard. Retracting the shared base edge would disturb parallel siblings, hence the guard; per-occurrence DELETE of a parallel relationship is **deferred** (needs property-filtered relationship MATCH). A relationship variable may bind only one edge per MATCH (reuse rejected). Same under-lock resolution as MERGE/`DELETE n` on the server path. |
| Parameters (`$param`) | Scalar and flat-list (`UNWIND $ids`) parameters, on read and write, across API / CLI / HTTP | AST pre-substitution (`fluree_db_cypher::substitute_params`) replaces `$name` with the supplied value before lowering. Supplied as a JSON map; over HTTP/CLI via the `{"cypher": "...", "params": {...}}` envelope (raw Cypher still works). Missing param → clear error. |
| Batched node insert — `UNWIND $batch AS row CREATE (…)` | The idiomatic driver batched insert: one list-of-maps parameter, one node per element, committed once | Compile-time **unroll** (`expand_unwind_create` in `params.rs`): each element becomes a literal CREATE with `row.field` → the map's value (missing → null) and node/rel vars suffixed per row so elements create **distinct** nodes. Scalar lists (`UNWIND $ids AS id CREATE (n {ref: id})`) work too. Pure-CREATE only. Whole-element-as-value (`{data: row}`) and nested field values deferred. Empty `$batch` currently errors (`EmptyTransaction`) rather than a no-op — follow-up. |
| Batched **edge** insert — `UNWIND $pairs AS p MATCH (a {id:p.from}),(b {id:p.to}) CREATE (a)-[:T {prop: p.x}]->(b)` | The edge-loading idiom (the scale lever past one request); one row per pair, missing endpoints drop only their row, optional per-row edge properties | VALUES **desugar** (`expand_unwind_match` → internal `ReadClause::InlineRows` → `UnresolvedPattern::Values`): `p.field` accesses become VALUES columns joined against the id lookups; the MATCH…CREATE edge template fires per row. Edge **properties** work too: CREATE property maps now accept bound variables (`expr_to_object`), and the per-row reifier is a **fresh-per-solution blank node** (see the SPARQL §3.1.3 fix below). The MATCH must be **mandatory** — `OPTIONAL MATCH … CREATE` is rejected (an unbound endpoint could assert a partial reifier bundle). |

The MATCH→WHERE foundation lowers leading `MATCH`/`OPTIONAL MATCH`
(node labels + inline property filters + directed single-typed
relationships) into `Txn.where_patterns`, with the WHERE side and the
DELETE/INSERT templates sharing variable ids via the shared
`VarRegistry` (`?name` interning) — the same linkage SPARQL UPDATE uses.

### Deferred (with reasons)

| Feature | Why deferred |
|---|---|
| Relationship MERGE / `MATCH … MERGE` | Their own follow-ups (multi-pattern / bound-context find-or-create). |
| `SET n = {…}` (bounded replace) | Needs a predicate-**variable** scan with a literal-object + non-`rdf:type` + non-`f:*` filter to bound the retract scope safely. Deferred to land with the DELETE slice (same predicate-var machinery). `SET n += {…}` covers the common per-key case. |
| `WHERE` filter expressions in a **write** MATCH | Requires lowering Cypher `Expr` → `UnresolvedExpression`. Inline property filters (`(n:Label {key: val})`) cover find-by-key today; explicit `WHERE n.x > 1 SET …` is the follow-up. |
| Untyped / alternation / property-filtered relationships in a **write** MATCH | Write MATCH supports directed single-typed relationships, anonymous or named (named binds `r` for `SET r.prop`). Untyped (`-[r]->`), alternation (`-[:A\|B]->`), and inline relationship property filters in a write MATCH are still deferred. |
| Whole-map node params (`(n $props)`); `$map.field` outside UNWIND; nested/map field values | Standalone map-valued params (not via `UNWIND … CREATE`/`MATCH`) still need a map-valued `Expr` the AST doesn't carry; deferred. |
| HTTP tracking (reads) / delimited / agent-json for Cypher | The Cypher HTTP routes return JSON-LD only and don't negotiate delimited (TSV/CSV) or agent-json output, and read-side tracking isn't surfaced yet. **Policy/identity enforcement IS applied** (resolved identity + header policy fields → `wrap_policy` on reads, `PolicyContext` on writes), and write-side tracking headers are honored. Remaining gap is output-format negotiation + read tracking. |
| Free path values (`p = (...)` without `shortestPath`), `relationships()`, reflection (`labels/type/keys/properties/id`), list comprehensions, bare `(n)`, `LOAD CSV`, `FOREACH`, `CALL proc`, schema DDL, multi-statement | Per the original plan's deferral list below (engine work or product decisions). Undirected, variable-length paths, `collect()`, **`shortestPath`/`allShortestPaths` + `length(p)`**, the **list functions `size`/`head`/`last`/`tail`/`reverse`**, **`nodes(p)`/`pathPairs(p)` + list indexing `list[i]`**, `range()`, and **IC14 weighted path scoring** have since landed (see status table). |

---

Builds on the edge-annotations storage primitive documented at
[`docs/concepts/edge-annotations.md`](docs/concepts/edge-annotations.md)
(user-facing semantics) and
[`docs/design/edge-annotations.md`](docs/design/edge-annotations.md)
(durable `f:reifies*` encoding, arena, and indexer state machine). An
LPG edge `(:Person)-[:WORKS_FOR {role:...}]->(:Org)` lowers to a base
triple plus an `f:reifies*` reifier bundle, identical to what JSON-LD
`@annotation` and SPARQL 1.2 `{| ... |}` produce.

This plan adds a **property-graph query surface** — initially
openCypher 9, with a forward path to ISO GQL — on top of the same
shared IR (`fluree_db_query::ir::Query`) and the same transaction
staging pipeline (`fluree_db_transact::Txn`) used by JSON-LD and SPARQL.

**Storage is unchanged.** The IR/executor are mostly reused, with two
narrow exceptions called out in the milestones below: (a) reusing the
existing `include_system_facts = false` filter for Cypher untyped
relationship matches, and (b) any new IR variants ride the existing
operator infrastructure. List-valued aggregation (`collect()`) landed
by reusing the existing `Binding::Grouped` carrier — no new operator.
Anchored path *search* (`shortestPath`/`allShortestPaths`) landed as a
dedicated `ShortestPathOperator` (bidirectional BFS) producing a new
`Binding::Path` value, with `length(p)` as `Function::PathLength`.
First-class *free* path values (`p = (...)` without a search wrapper)
and `nodes()/relationships()` still need general path-value IR and are
deferred.

## Why now

The edge-annotations primitive is the missing piece that makes Cypher
expressible without forcing users to model relationship-with-properties
by hand. With JSON-LD `@annotation` and SPARQL `{| ... |}` shipped on
that primitive, Cypher is the next natural surface to expose — the
language work is the only remaining track.

## Dialect choice: openCypher 9 first, GQL later

openCypher and ISO GQL are not the same language. They share most of
their pattern syntax, but they differ on session model, return shape,
schema DDL, and many surface details:

| Aspect | openCypher 9 (Neo4j-lineage) | ISO GQL (ISO/IEC 39075:2024) |
|---|---|---|
| Maturity | 10+ years of ecosystem (drivers, training, examples). | Standard published 2024; ecosystem nascent. |
| Read core | `MATCH ... WHERE ... RETURN`. | `MATCH ... FILTER ... RETURN` (similar shape). |
| Write core | `CREATE / MERGE / SET / DELETE / REMOVE`. | `INSERT / UPDATE / DELETE` (different keywords, similar semantics). |
| Path values | First-class. | First-class. |
| Schema | Implicit. | Explicit graph types and node/edge types. |
| Bag semantics | Default. | Default. |
| Session graphs | Implicit current graph. | First-class session graph / home graph. |

**Decision: v1 implements an openCypher 9 subset.** Rationale: the
existing ecosystem (Neo4j drivers, awesome-cypher docs, BSBM-style
benchmarks) and the existence of an openCypher reference grammar
(`Cypher.g4`) make this the cheaper, more useful first deliverable.
GQL alignment is a Phase 2 concern — the AST will be structured so that
adding GQL keyword surfaces (`INSERT` alias for `CREATE`, `FILTER`
alias for `WHERE`) is parser-only work.

Throughout this plan, "Cypher" means "openCypher 9 subset". When GQL
diverges materially the difference is called out.

## Frozen contract

The contract this plan implements is:

1. **The edge-annotations user contract** at
   `docs/concepts/edge-annotations.md` — mode defaults (RDF vs LPG),
   cascade behavior, empty-annotation semantics, multiplicity rules,
   parallel-edge requirements, and the deferred/out-of-scope list.
2. **The durable encoding and arena contract** at
   `docs/design/edge-annotations.md` — `f:reifies*` bundle shape,
   reserved-predicate firewall, `EdgeKey` definition, and the
   forward/reverse attachment indexes that back lookup.
3. **The multiplicity contract**: a bare `Triple(?s, p, ?o)` returns
   one row per distinct `(s, p, o)`; binding an annotation variable
   (or matching a relationship-property body) is what introduces
   per-occurrence cardinality. Cypher's bag semantics match this rule
   with one extra lowering choice (below) for the
   bind-relationship-implicitly case.

Any divergence between Cypher behavior and these contracts is a bug in
this plan.

## Semantic model — Cypher → Fluree

| Cypher concept | Fluree mapping |
|---|---|
| Node `(n:Label)` | Subject with `rdf:type <Label IRI>`. Multiple labels = multiple `rdf:type` triples. |
| Node `(n {key: val})` | Ordinary triples about `n`. Properties are open-world; no schema check unless SHACL is wired. |
| Bare `(n)` with no label/prop/relationship constraint | **Rejected in v1** — see "Node existence model" below. |
| Relationship `-[r:TYPE]->` | Base triple `(start, <TYPE IRI>, end)`. Direction is captured by triple direction. |
| Relationship properties `-[:T {p: v}]->` | Base triple **plus** an edge-annotation reifier bundle (`f:reifies*` system encoding, see `docs/design/edge-annotations.md`). One annotation SID per relationship occurrence. |
| Relationship variable `-[r]->` binds | The annotation SID. Only matches relationships that have a reifier bundle — see "Relationship lowering rule" below. |
| Parallel relationships | Two annotation SIDs attached to the same `(s, p, o)` edge key. Already supported (multimap forward attachment index). |
| Undirected `-[r]-` | **Landed** — forward∪reverse `Union` (reverse via the `Opst` index). |
| Variable-length `-[:T*1..5]->` | **Landed** (anonymous, single-typed) — see "Variable-length paths" below. **Bounded** ranges enforce Cypher **relationship-uniqueness** (no reused edge; a node may be revisited via different edges — triangle closures allowed, edge-reuse out-and-backs excluded; matches Neo4j on cyclic graphs). Binding a variable to the path (`-[r:T*1..5]->`, a relationship *list*) is still deferred. |
| Path value `p = (a)-[r]->(b)-[r2]->(c)` | Free path object. **Deferred** (only `shortestPath`/`allShortestPaths`-wrapped paths bind today). |
| `p = shortestPath((a)-[:T*]-(b))` / `allShortestPaths(...)` | **Landed** — anchored bidirectional-BFS `ShortestPathOperator` binds `p` to a `Binding::Path` node sequence; `length(p)` + `p IS NULL` supported. See "Shortest paths" below. |
| `RETURN n, r, m` | SELECT projection. Bag semantics by default (no DISTINCT). |
| `RETURN DISTINCT` | Lower to existing DISTINCT modifier. |
| `WHERE` predicate | Lower to existing `Filter` patterns. |
| `WITH ... [WHERE]` | Lower to subquery boundary (correlated subquery in IR). |
| `UNWIND $list AS x` | Lower to existing list-binding shape. v1 supports parameter-bound lists of scalars or shallow maps; expression-built lists deferred. |
| `ORDER BY / SKIP / LIMIT` | Existing solution modifiers. |
| `CREATE (a:Label {p:v})-[:T {q:w}]->(b)` | Insert pipeline: base triples + reifier bundle, identical to JSON-LD `@annotation` inserts. |
| Bare `CREATE (n)` with no label/prop/relationship | **Rejected in v1** — see "Node existence model" below. |
| `SET n.prop = expr` | DELETE+INSERT staging on `(n, prop, *)`. See "SET property/relationship boundary" for the precise rule. |
| `SET n = {p:v}` / `SET n += {p:v}` | Bounded data-property replace/merge — see "SET property/relationship boundary". |
| `REMOVE n:Label` / `REMOVE n.prop` | DELETE staging. |
| `DELETE r` | Retract attachment row + cascade owned annotation facts. LPG mode (see `docs/concepts/edge-annotations.md`). |
| `DELETE n` | See "DELETE / DETACH DELETE in RDF" below — refuses to retract if `n` has any remaining outbound or inbound ref-typed triples. |
| `DETACH DELETE n` | Retract all ref-typed triples involving `n` (with reifier bundles) plus all triples about `n`. |
| `MERGE (n:Label {p:v})` | v1 supports single-node-only MERGE; relationship-MERGE deferred — see M5.5. |

### IRI mapping for Cypher identifiers

Cypher labels, relationship types, and property keys are bare
identifiers (e.g., `Person`, `WORKS_FOR`, `name`). Fluree needs IRIs.
The mapping is:

1. **Ledger default context wins.** The ledger's `f:context` config
   (the same context that applies to JSON-LD queries against the
   same ledger) supplies `@vocab` and named prefixes. A Cypher label
   `Person` resolves the same way a JSON-LD compact IRI `Person`
   would: try prefixes first, then `@vocab` fallback.
2. **Request-envelope override.** When Cypher is sent via the JSON
   envelope (`{"cypher": "...", "params": {...}, "context": {...}}`),
   the request `context` field overrides the ledger default for that
   request only. This mirrors `opts.@context` in JSON-LD transactions.
3. **Plain-text bodies** (`Content-Type: application/cypher` with a
   raw query) get the ledger default context. No way to override
   per-request without the JSON envelope.
4. **Snake_case to camelCase or kebab-case is not done.** `WORKS_FOR`
   maps to the IRI `<vocab>WORKS_FOR`, not `<vocab>worksFor`. If the
   user wants the JSON-LD-style camelCase name, they put that mapping
   in the context.
5. **`rdf:type` is the only reserved Cypher → IRI mapping.** Labels
   become `rdf:type` objects regardless of context.
6. **Property keys follow the same rules** but use the context's
   property-position resolution. v1 accepts a single mapping per key;
   per-position aliasing (term has a different meaning as predicate vs
   subject) is not exposed through the Cypher surface.

Rejection rules at parse/lower time:
- Identifier that resolves to no IRI under any rule → clear error.
- Identifier that resolves to a reserved system predicate (`f:reifies*`
  family) at lowering time → reject with reserved-predicate firewall
  message, matching the SPARQL/JSON-LD behavior.

### Node existence model — bare `(n)` is rejected in v1

In Fluree, a node with no labels and no data triples has no facts
asserted about it. There is no implicit marker fact ("this SID exists
as a node") today.

Consequences for Cypher:
- `MATCH (n) RETURN n` is asking for "every node in the graph". In
  RDF terms, that's "every subject of any triple". v1 rejects this
  pattern at lower time with a clear error pointing at `MATCH (n:Label)`
  or `MATCH (n) WHERE ...` as the v1 alternative. Reason: a literal
  whole-graph subject scan with no predicate constraint is rarely
  what users want and is expensive enough to be a footgun.
- `MATCH (n)-[]->()` is fine — the relationship constraint anchors
  `n`'s lower bound (it must be a subject of some triple).
- `CREATE (n)` with no labels and no properties is also rejected in
  v1. It would mint a SID that no query can address, and there's no
  way to round-trip the result back to the user. Users wanting a
  bare anonymous node should provide at least one identifying fact
  (a label or a property).
- `MATCH (n {prop:val})` (no label, with property) is accepted; the
  property triple constrains `n`.

A future v1.1 may introduce an explicit node-marker predicate
(`f:Node` or similar) to support `MATCH (n)` without a whole-graph
scan, but that is its own design decision and not part of this plan.

### Relationship lowering rule

This is the rule that determines whether a Cypher relationship pattern
sees plain RDF triples or only annotated ones. It governs the
"impedance mismatch" between Cypher's LPG worldview and Fluree's
RDF substrate.

The three input shapes and their lowering targets:

```text
1. (a)-[:T]->(b)                    (anonymous, no property filter)
   → Triple(?a, <T>, ?b)
   Matches ALL base edges with predicate <T>, including those that
   have no reifier bundle. Set semantics (one row per (s, p, o)) —
   matches SPARQL's bare-triple pattern.

2. (a)-[r:T]->(b)                   (named, no property filter)
   → EdgeAnnotation { edge: Triple(?a, <T>, ?b), annotation: ?r,
                      body: vec![] }
   Matches ONLY edges that have at least one reifier bundle. ?r
   binds to the annotation SID. Bag semantics (one row per occurrence,
   including parallels).

3. (a)-[:T {prop:val}]->(b)         (anonymous OR named, with filter)
   → EdgeAnnotation { edge: Triple(?a, <T>, ?b),
                      annotation: fresh non-distinguished Var,
                      body: vec![Triple(?annotation, prop, val)] }
   Matches reifier-bundled edges whose annotation body satisfies the
   filter. Bag semantics.
```

Trade-off acknowledged: shapes 2 and 3 cannot see plain RDF triples
without a reifier. A user who writes `MATCH (a)-[r:T]->(b)` against
non-Cypher-written data will get zero rows even though base triples
exist. The Cypher docs page will explain this rule explicitly:

> "Binding a relationship variable, or matching on relationship
> properties, requires the edge to have a reifier. Plain RDF triples
> inserted via JSON-LD or SPARQL without `@annotation` / `{| |}` are
> not visible to these patterns. Use the anonymous form
> `(a)-[:T]->(b)` to match across both worlds at set cardinality."

For LPG-native workflows where all writes come through Cypher (and
therefore every relationship has a reifier under LPG-default mode),
the rule is transparent: bag semantics work as Cypher users expect.

### Cardinality summary

| Pattern shape | Cardinality | Sees plain RDF? |
|---|---|---|
| `(a)-[:T]->(b)` | Set (one per `(s,p,o)`) | Yes |
| `(a)-[r:T]->(b)` | Bag (per occurrence) | No |
| `(a)-[:T {p:v}]->(b)` | Bag (per occurrence) | No |
| `MATCH ... RETURN DISTINCT a, b` | Set | (irrelevant — DISTINCT) |

### Variable-length paths — landed

Anonymous variable-length relationships are lowered (2026-06-14):

- **Bounded** `-[:T*m..n]->` expands to a `Union` of fixed-length join
  chains (one chain per length, each with `k-1` fresh intermediate
  nodes), honoring direction — undirected hops emit a forward∪reverse
  `Union`. Capped at 16 hops. For `k ≥ 2` each chain carries a
  **relationship-uniqueness `Filter`** so the walk can't reuse an edge.
- **Unbounded** `-[:T*]->` / `-[:T*0..]->` reuses the existing transitive
  `PropertyPathPattern` (`*`→`OneOrMore`, `*0..`→`ZeroOrMore`). Directed
  only; unbounded-undirected and `*N..` (N>1) remain deferred.

**Uniqueness (bounded).** The filter enforces Cypher's actual
**relationship-uniqueness** rule (no edge traversed twice; a node *may*
be revisited via different edges), comparing consecutive-node *pairs*
(edges) rather than individual nodes — for an undirected hop an edge is
the unordered pair, so the reverse orientation is forbidden too. This
matches Neo4j on cyclic graphs:

- A triangle closure `a-b-c-a` is **allowed** (three distinct edges) —
  node-uniqueness wrongly excluded it.
- An out-and-back `a-b-a` over one edge is **excluded** (the edge would
  be reused), including the same-endpoint case `(a)-[:T*2]-(a)`.

`nodes[i] != nodes[j]` evaluates at runtime, so a comparison over the
same variable is simply `false` — no static analysis. Caveat:
**Unbounded** paths (via `PropertyPathPattern`) still enforce no
uniqueness.

**Scope vs. full Cypher.** These produce **endpoint bindings**, not
first-class path values. Binding a variable to the path (`-[r:T*]->`, a
relationship *list*) and free path values (`p = (...)`,
`nodes()/relationships()`) still need list/path-valued bindings —
deferred. Anchored `shortestPath`/`allShortestPaths` *do* bind a path
(see below).

A relationship *type* whose namespace isn't registered in the ledger
encodes to no predicate; such a path yields **zero rows**, not an error
(the same as an absent label). Bound rel-var var-length, unbounded
undirected, and unbounded `*N..` (N>1) are still rejected at lower time
with a clear message.

### Shortest paths — landed

`MATCH p = shortestPath((a)-[:T*]-(b))` and `allShortestPaths(...)`
lower to `Pattern::ShortestPath` and execute on a dedicated
`ShortestPathOperator` (`fluree-db-query/src/shortest_path.rs`):

- **Anchored only (v1):** both endpoints must be bound by a preceding
  mandatory MATCH. The planner classifies `ShortestPath` as `Deferred`
  on its endpoint vars (like a correlated subquery), so it always runs
  after they are bound. An unresolved endpoint yields zero rows.
- **`Single` mode** (`shortestPath`) runs **bidirectional BFS** —
  frontiers expand from both endpoints, alternating the smaller, until
  they meet — and reconstructs one shortest path from the predecessor
  maps. **`All` mode** (`allShortestPaths`) runs a layered forward BFS
  recording the full predecessor set, then enumerates every
  minimal-length path (one output row each, capped).
- Neighbour expansion reuses the `property_path` index access: `Spot`
  (subject→object) and `Post` (object→subject) range scans, ref-only
  edges, single active graph. Direction maps to which index(es) to
  probe per frontier (`Either` = both → undirected, the IC13 `KNOWS`
  case). Safety caps: `DEFAULT_MAX_VISITED` (100k nodes), `DEFAULT_MAX_PATHS`
  (1k paths, `All` mode).
- The path binds to a new `Binding::Path(Vec<Sid>)` (node sequence,
  start→end). `length(p)` (`Function::PathLength`) is its hop count
  (`nodes − 1`); `p IS NULL` under `OPTIONAL MATCH` detects "no path"
  — together these are the IC13 shape
  (`CASE WHEN p IS NULL THEN -1 ELSE length(p) END`). No path under a
  *mandatory* MATCH drops the row.
- A lower hop bound > 1 (`*2..`) can require a *longer* path than the
  unconstrained shortest one — which distance-finalizing BFS cannot
  discover (it pins each node at its minimal distance, so a length-1
  `A→D` hides the length-2 `A→B→D`). For `min_hops > 1` the operator
  switches to an iterative-deepening node-distinct DFS that returns the
  path(s) at the first qualifying length (unbounded `*2..` capped at
  `UNBOUNDED_DEPTH_CAP = 15` hops).
- `allShortestPaths` returning more than `DEFAULT_MAX_PATHS` (1000) is a
  hard `ResourceLimit` error, not a silent truncation — a quietly-capped
  result on a high-fan-out lattice would look complete while dropping
  paths.
- Inner pattern must be node–relationship–node over a **single typed**
  predicate, anonymous rel (no rel var / property filter). `nodes(p)` and
  `pathPairs(p)` are supported; `relationships(p)` (edge identities) and
  free (unwrapped) path values remain deferred. `Binding::Path` renders as
  a node-IRI array in JSON-LD/typed output; SPARQL/CONSTRUCT formatters
  reject it (Cypher-only type).

### LPG mode is the default for Cypher writes

Per `docs/concepts/edge-annotations.md`, `opts.lpgEdgeLifecycle: true`
is opt-in for JSON-LD/SPARQL and default-on for Cypher imports. The
Cypher lowering threads this through automatically:

- CREATE without relationship properties still mints an annotation SID
  (relationship has identity).
- DELETE on a relationship cascades attachment + owned metadata in
  LPG mode (matches `MATCH ()-[r]->() DELETE r` semantics).
- Empty relationship property maps `{}` mint a fresh SID (matching the
  empty-annotation rule in the concept doc).

Users do not opt into LPG mode for Cypher writes; it is the contract.

### SET property/relationship boundary

Cypher distinguishes node *properties* from *labels* and
*relationships*. Fluree stores everything as triples, so the lowering
must decide which triples are in scope for each `SET` shape:

| `SET` shape | Retract scope | Insert |
|---|---|---|
| `SET n.prop = expr` | All `(n, <prop>, *)` flakes. | `(n, <prop>, expr-value)`. |
| `SET r.prop = expr` (r is a reifier SID) | All `(r, <prop>, *)` flakes. | `(r, <prop>, expr-value)`. |
| `SET n += {p:v, q:w}` | For each key `k` in the map, all `(n, <k>, *)`. | One triple per key. |
| `SET n = {p:v, q:w}` | **All `(n, ?p, ?o)` where `?o` is a literal AND `?p` is not `rdf:type` AND `?p` is not in the `f:*` reserved namespace.** Then the keys in the map. | One triple per key. |
| `SET n:NewLabel` | (none — additive) | `(n, rdf:type, <NewLabel>)`. |

The `SET n = {...}` "replace all properties" rule is the tricky one.
The bounded retract scope ensures we don't:
- Wipe labels (Cypher's `SET n = {}` keeps labels — match Neo4j).
- Wipe outbound relationships (a ref-valued triple is a relationship,
  not a property).
- Wipe inbound relationships (those have `n` in the object position).
- Wipe reifier bundles attached to relationships originating from `n`.
- Wipe `f:*` system facts (config, policy).

Users who want to clear relationships and properties together use
`DETACH DELETE` then `CREATE`.

### DELETE / DETACH DELETE in RDF

Cypher requires `DELETE n` to fail when `n` has remaining
relationships. In RDF terms, the staging-time check is:

- A "relationship" is any triple `(n, p, o)` or `(s, p, n)` where the
  object position is a **ref-typed value** (IRI or blank node, not
  a literal), and `p` is not `rdf:type`, and `p` is not in the
  `f:*` system namespace.

If any such triple exists at staging time, `DELETE n` errors with a
message listing the count of remaining relationships and pointing at
`DETACH DELETE n`.

`DETACH DELETE n` cascade:
1. Find all outbound relationship triples for `n` (per above
   definition). For each, find its reifier bundle (if any) and retract
   the bundle.
2. Find all inbound relationship triples for `n`. For each, find its
   reifier bundle (if any) and retract the bundle.
3. Retract all triples where `n` is subject or object (covers labels,
   data properties, both directions of relationships).

This is more work than Neo4j's `DETACH DELETE` (which only sees
outbound + inbound from the node's adjacency list), but the RDF
substrate has no adjacency list — we scan SPO and OSP. The cost
follows the existing index lookup machinery.

### LPG mode is the default for Cypher writes

Per `docs/concepts/edge-annotations.md`, `opts.lpgEdgeLifecycle: true`
is opt-in for JSON-LD/SPARQL and default-on for Cypher imports. The
Cypher lowering threads this through automatically:

- CREATE without relationship properties still mints an annotation SID
  (relationship has identity).
- DELETE on a relationship cascades attachment + owned metadata in
  LPG mode (matches `MATCH ()-[r]->() DELETE r` semantics).
- Empty relationship property maps `{}` mint a fresh SID (matching the
  empty-annotation rule in the concept doc).

Users do not opt into LPG mode for Cypher writes; it is the contract.

## Architecture

A new sibling crate, `fluree-db-cypher/`, mirroring `fluree-db-sparql`:

```
fluree-db-cypher/
  Cargo.toml
  src/
    lib.rs           — public surface: parse_cypher, lower_cypher
    lex/             — winnow lexer; hand-written, same style as SPARQL
    ast/             — pure AST types; no DB access
    parse/           — token → AST
    lower/           — AST → fluree_db_query::ir::Query
    validate/        — capability-driven rejection of unsupported shapes
    span.rs          — source spans, identical layout to SPARQL crate
```

Feature flags identical in shape to `fluree-db-sparql`:

```toml
[features]
default = ["lowering"]
lowering = ["dep:fluree-db-query", "dep:fluree-db-core",
            "dep:fluree-vocab", "dep:fluree-graph-json-ld"]
```

Reasons for a separate crate rather than module-inside-sparql:

- **Independent grammar.** Cypher and SPARQL share zero token shapes;
  one lexer doing both would be a maintenance hazard.
- **Independent test surface.** Cypher test fixtures want to be next
  to Cypher source, not pasted into the SPARQL crate.
- **WASM/Lambda parity.** Same `lowering` feature flag lets future
  edge deployments include or exclude Cypher independently.
- **Shared lowering target.** Both crates lower into the same shared
  IR. There is one execution engine; that doesn't change.

### Where the writes go

Cypher write statements (`CREATE / SET / MERGE / DELETE`) lower into
`fluree_db_transact::Txn` via a new file:

```
fluree-db-transact/src/lower_cypher_update.rs
```

analogous to `lower_sparql_update.rs`. Output staging records (the
`f:reifies*` bundle for relationships, ordinary triples for node
properties) are bit-for-bit identical to what the JSON-LD and SPARQL
paths produce. Cascade, policy, firewall, and reserved-predicate
machinery from the edge-annotations storage layer cover Cypher writes
for free.

## Surface — what's in v1

Scope is "useful Cypher subset that round-trips with JSON-LD and SPARQL
on the edge-annotation primitive". The table below is the contract;
anything not in this list is **rejected with a clear
`UnsupportedFeature` error** that names the feature and points at this
document.

### v1 read surface

The narrowed v1 read surface is: **labeled-or-constrained nodes plus
directed, typed relationships (with optional property filters), under
standard solution modifiers and a conservative expression sublanguage.**

| Feature | v1? | Notes |
|---|---|---|
| `MATCH (n)` (bare node) | ❌ | Rejected — no node-existence model in v1. See "Node existence model". |
| `MATCH (n:Label)` | ✅ | One label via `rdf:type`. |
| `MATCH (n:L1:L2)` | ✅ | AND across labels (multiple `rdf:type` triples). |
| `MATCH (n {p:v})` | ✅ | Inline property filters; `n` is anchored by the property triple. |
| `MATCH (a)-[:T]->(b)` | ✅ | Anonymous typed relationship — lowers to plain `Triple`, set semantics, sees plain RDF. |
| `MATCH (a)-[r:T]->(b)` | ✅ | Named typed relationship — lowers to `EdgeAnnotation`, bag semantics, sees only reified edges. |
| `MATCH (a)-[r:T {p:v}]->(b)` | ✅ | Relationship property filter — same as above plus body. |
| `MATCH (a)-[r]->(b)` (untyped) | ✅ | Predicate is a Var. Implicitly filtered to exclude `f:reifies*` and other system predicates (reuses existing `include_system_facts = false`). |
| `MATCH (a)-[r:T1\|T2]->(b)` | ✅ | Type alternatives via `Union`. |
| `MATCH (a)<-[r:T]-(b)` | ✅ | Inverse direction (swap subject/object). |
| `MATCH (a)-[:T]-(b)` (undirected) | ✅ | Forward∪reverse `Union` (reverse via the `Opst` object index). A bound rel var works for single-hop undirected. |
| `MATCH (a)-[:T*m..n]->(b)` (bounded var-length) | ✅ | Expands to a `Union` of fixed-length join chains; honors direction incl. undirected hops. Anonymous rel only (a bound rel var binds a *list* — deferred). Each `k≥2` chain carries a **relationship-uniqueness filter** (no reused edge — compares consecutive-node pairs; undirected forbids the reverse orientation too), matching Neo4j on cyclic graphs (triangle closures allowed, edge-reuse out-and-backs excluded). |
| `MATCH (a)-[:T*]->(b)` / `*0..` (unbounded var-length) | ✅ | Reuses the transitive `PropertyPathPattern` (`*`→OneOrMore, `*0..`→ZeroOrMore). Directed only; unbounded-undirected and `*N..` (N>1) deferred. |
| `MATCH p = shortestPath((a)-[:T*]-(b))` / `allShortestPaths(...)` | ✅ | Anchored bidirectional-BFS path search → `Pattern::ShortestPath` / `ShortestPathOperator`. Both endpoints must be bound by a preceding MATCH; single typed predicate; directed/undirected. `Single` binds one shortest path per row, `All` one row per minimal-length path. Binds `Binding::Path` (node sequence). |
| `length(p)` | ✅ | Hop count of a path value (`Function::PathLength`); `p IS NULL` under `OPTIONAL MATCH` detects "no path" (IC13). |
| `MATCH p = ...` (free path value, no `shortestPath` wrapper), `nodes()/relationships()` | ❌ | Deferred — needs general path-value IR + list bindings. |
| `OPTIONAL MATCH` | ✅ | Lowers to `Optional`. |
| `WHERE expr` | ✅ | Conservative expression sublanguage — see "Expressions in v1" below. |
| `WITH ... AS ...` | ✅ | Lowers to subquery boundary. |
| `WITH ... WHERE ...` | ✅ | Subquery + outer filter. |
| `WITH ... ORDER BY / SKIP / LIMIT` | ✅ | Modifiers inside subquery. |
| `UNWIND $list AS x` | ✅ | Parameter-bound lists of scalars or shallow maps. Expression-built lists deferred. |
| `UNWIND [literal, ...] AS x` | ✅ | Inline literal list (lowers to `Values`). |
| `UNWIND <expr> AS x` (runtime list) | ✅ | A non-constant list — `UNWIND nodes(path) AS n`, `UNWIND range(1,5) AS i` — lowers to `Pattern::Unwind`, a correlated operator that fans each input row out over the list elements (empty/null → drops the row). A property accessor on the unwound element (`n.name`) correlates correctly. Constant lists still take the `Values` fast path. |
| IC14 connection paths as person lists | ✅ | `MATCH p = allShortestPaths((a)-[:KNOWS*]-(b)) UNWIND nodes(p) AS pn RETURN p, collect(pn.id)` — every shortest path, exploded and re-collected per path (a path is a first-class GROUP BY / `collect` key via the `Seq` group key). |
| IC14 **weighted** path scoring | ✅ | The per-edge `reduce` folding pattern-match counts between path-adjacent nodes — which would need runtime pattern execution inside a fold — is decomposed instead into `UNWIND pathPairs(p) AS pair` → `OPTIONAL MATCH` the interaction between `pair[0]`/`pair[1]` → `count` → `sum`, grouped by the carried path. IC14's weight is additive over consecutive pairs, so unwind + aggregate computes it exactly. The path `p` is carried through the WITH stages (a node sequence survives projection) and the final id list is a *terminal* `collect` grouped by that path — together sidestepping the `collect`-in-`WITH` limitation entirely. No `reduce`, pattern comprehension, or async property access needed. |
| `pathPairs(p)` + list indexing `list[i]` | ✅ | `pathPairs(p)` returns a path's consecutive node pairs (`[[a,b],[b,c],…]`, each a two-element list) for `UNWIND`; `list[i]` is 0-based element access (negative indexes from the end, out-of-range → null). An indexed node-ref element correlates downstream as a MATCH endpoint / property target. The two engine pieces under IC14 weighted scoring. |
| `RETURN ...` | ✅ | Default bag semantics. |
| `RETURN DISTINCT` | ✅ | Set semantics. |
| `RETURN ... AS alias` | ✅ | Existing projection alias support. |
| `RETURN count(*) / count(x) / sum(x) / avg(x) / min(x) / max(x)` | ✅ | Existing aggregate operators. |
| Aggregates composed into expressions — `count(a) + count(b)`, `count(m) + 1`, `sum(a) / count(b)` | ✅ | Each aggregate sub-expression is lifted to its own spec; the surrounding expression becomes a post-aggregation bind (LDBC IC3 total / IC10 score). Combine aggregates with literals and each other; referencing a *grouping key* inside the expression is deferred (project it separately and use its alias). |
| `RETURN collect(x)` / `collect(DISTINCT x)` | ✅ | `AggregateFn::Collect` gathers non-null values into a list (Cypher semantics: nulls dropped, empty → `[]`; an implicit aggregation over zero matched rows still yields one row with `[]`). Produces a first-class `Binding::List` (distinct from the transient `Binding::Grouped`), rendered as a JSON array by the JSON-LD / typed formatters (v1 Cypher output). A collect list **cannot be an `ORDER BY` key**, and `collect()` in `WITH` is still deferred (projecting the raw list through the subquery boundary nulls it — a separate fix); use it in the final `RETURN`. |
| List functions `size` / `head` / `last` / `tail` / `reverse` | ✅ | Over a `collect()` list (and `size`/`reverse` also over a string). `size`/`head`/`last` return scalars; `tail`/`reverse` return lists (via the binding-producing eval path). Usable in the final `RETURN` wrapping a collect — `RETURN size(collect(f.name))` — extracted as a list-valued aggregate with the list function as a post-aggregation bind. A `collect()` nested in arithmetic/comparison (`collect(x) + 1`) is still rejected (it would evaluate to null). |
| List literal `[a, b, …]` and structured `collect([a, b])` | ✅ | A list literal builds a `Binding::List` via the `MakeList` constructor (`Function::MakeList`); mixed element types allowed. `collect([n.id, n.name])` (IC1's tuple-collect tier) lowers the list literal to a pre-aggregation Bind and gathers the per-row tuples into a list of lists. `range(...)` / list comprehensions still deferred. |
| `ORDER BY / SKIP / LIMIT` | ✅ | Sort keys may be a variable, a property accessor, **or a general expression** (`ORDER BY toInteger(n.id)`, arithmetic) — an expression key lowers to a synthetic pre-sort Bind. An aggregate key must reference its projected alias. |
| `toInteger(x)` / `toFloat(x)` | ✅ | Cypher numeric casts (LDBC orders string ids numerically via `toInteger`), mapped to the XSD cast functions. |
| IC1 distance ranking — `length(shortestPath((p)-[:KNOWS*1..3]-(friend)))` | ✅ | The "BFS-distance rider": anchored `shortestPath` (both endpoints bound) with a bounded `*1..3` plus `length(path)` gives each friend's shortest hop-distance; rank with `ORDER BY distance, …, toInteger(friend.id)`. No fixed-length UNION workaround needed. |
| `UNION` / `UNION ALL` | ✅ | Lowers to existing `Union` pattern. |
| `CALL { subquery }` (read-only) | ✅ | Lowers to `Subquery`. |
| `CALL procedureName(...)` | ❌ | No procedure namespace yet. Reject. |
| `shortestPath / allShortestPaths` | ❌ | Deferred — needs path-aware planner extension. |
| `EXISTS { ... }` | ✅ | Bare pattern **and** the subquery form `EXISTS { MATCH … WHERE … }` (inner WHERE ANDed into the test; outer vars visible). Lowers to the shared `Exists`. |
| `CASE ... WHEN ... END` | ✅ | Standard expression. |

### v1 write surface

| Feature | v1? | Notes |
|---|---|---|
| `CREATE (n)` (bare, no labels/props) | ❌ | Rejected — see "Node existence model". |
| `CREATE (n:Label {p:v})` | ✅ | Node creation; at least one label or property required. A **null** property value (`{x: null}`, or an UNWIND row's missing field) means "no property" — it is **skipped**, not stored as a null. |
| List-valued property (`{prop: [a, b, …]}`) | ✅ | A list-valued literal property (IU1's `email[]` / `language[]`) becomes a **multi-valued RDF predicate** — one flake per element — across **all write ops**: `CREATE`, `SET n.prop = [...]` / `SET n += {…}` (replace: retract existing values, assert the new set), `MERGE … ON CREATE SET`, and the batched `UNWIND $batch AS row CREATE (n {prop: row.prop})` load (a JSON-array element field). An empty list `[]` stores nothing (like null). Nested lists/maps as elements are rejected. **Read divergence:** on read a multi-valued predicate projects as one row per value (RDF set semantics), not Neo4j's single-array value — benign for the LDBC read workload (it doesn't read array properties); a Neo4j-faithful single-array read would need first-class list-valued read bindings. |
| `CREATE (a)-[r:T {p:v}]->(b)` | ✅ | Directed typed relationship. **Every Cypher relationship reifies** (gets a `f:reifies*` bundle / identity) — including anonymous, property-less `CREATE (a)-[:T]->(b)` — so it's visible to named reads (`-[r:T]->`), deletable by `DELETE r`, guarded by bare `DELETE n`, and not collapsed with a parallel edge. The base triple makes it visible to anonymous (plain-RDF) reads too. Multi-solution CREATE (e.g. batched edges) mints a fresh reifier per solution (SPARQL §3.1.3 blank-node semantics, fixed engine-wide). |
| `CREATE` chains and patterns | ✅ | `CREATE (a)-[:T]->(b)-[:T2]->(c)`. |
| `MATCH ... CREATE ...` | ✅ | WHERE-bound bindings drive template. |
| `MATCH ... SET n.prop = expr` | ✅ | Single-property update (DELETE+INSERT). |
| `MATCH ... SET r.prop = expr` | ✅ | Relationship-property update (operates on annotation SID). |
| `MATCH ... SET n += {p:v}` | ✅ | Merge map keys into node — per-key DELETE+INSERT. |
| `MATCH ... SET n = {p:v}` | ✅ | Bounded replace — see "SET property/relationship boundary". |
| `MATCH ... SET n:NewLabel` | ✅ | Additive — INSERT `(n, rdf:type, NewLabel)`. |
| `MATCH ... REMOVE n.prop` | ✅ | DELETE staging on `(n, prop, *)`. |
| `MATCH ... REMOVE n:Label` | ✅ | DELETE `(n, rdf:type, Label)`. |
| `MATCH ... DELETE r` | ✅ | Retract attachment + cascade owned annotation facts (LPG mode). |
| `MATCH ... DELETE n` | ✅ | Reject if `n` has remaining relationships — see "DELETE / DETACH DELETE in RDF". |
| `MATCH ... DETACH DELETE n` | ✅ | Cascade all relationships + node. |
| `MERGE (n:Label {p:v})` | ✅ | Single-node MERGE only in v1; see M5.5. |
| `MERGE` with `ON CREATE SET` / `ON MATCH SET` | ✅ | For the single-node form. |
| `MERGE (a)-[:T]->(b)` (relationship MERGE) | ❌ | Deferred to v1.1 — see M5.5. |
| `LOAD CSV` | ❌ | Out of scope; use the existing import pipeline. |
| `FOREACH` | ❌ | Imperative; deferred. |
| Multi-statement scripts (`;`-separated) | ❌ | v1 = one statement per request, matching existing transact endpoint contract. |
| Schema DDL (`CREATE INDEX / CREATE CONSTRAINT`) | ❌ | Fluree's schema lives in SHACL config graph; mapping is its own decision. |

### Cypher expressions in v1

The expression sublanguage is intentionally conservative: only Cypher
operators and functions that map cleanly to an existing
`fluree_db_query::ir::Expression` variant are in v1. Anything that
would need new IR (list-valued bindings, dynamic introspection of node
labels/properties, predicate-name reverse lookup) is deferred.

**Included** (each maps to an existing IR variant):

- Comparison: `=, <>, <, <=, >, >=`
- Boolean: `AND, OR, NOT`
- Arithmetic: `+, -, *, /` (the operators the IR already exposes;
  `%` modulus and `^` exponent are deferred pending IR/op support
  confirmation)
- String: `STARTS WITH, ENDS WITH, CONTAINS`, string `+` concat
- Null tests: `IS NULL, IS NOT NULL`
- List membership: `IN` over inline literal lists or
  parameter-bound scalar lists
- `coalesce(...)`, `length(string)`, `toString(...)`, `toInteger(...)`,
  `toFloat(...)`, `abs(...)` — these map directly to existing
  scalar expression variants
- Aggregations that already exist: `count(*)`, `count(x)`, `sum(x)`,
  `avg(x)`, `min(x)`, `max(x)`
- `CASE ... WHEN ... THEN ... ELSE ... END`
- Parameter references: `$name` (literal substitution at lower time)

**Deferred** (each would need new IR or runtime work):

- `XOR` — no direct IR variant; users write `(a OR b) AND NOT (a AND b)`.
- `%` modulus, `^` exponent — pending IR confirmation.
- `size`, `head`, `last`, `tail`, `reverse` over a `collect()` list have
  **landed** (consuming `Binding::List`). `range` and the remaining list
  builders/comprehensions are still deferred.
- `labels(n)`, `keys(n)`, `properties(n)`, `type(r)` — dynamic
  reflection over a node/relationship's facts; needs snapshot-time
  lookup expressions.
- `id(n)` / `id(r)` — exposing SIDs has cross-import stability and
  policy implications that need a separate decision. Users wanting
  stable identity should project the IRI directly.
- Map literals beyond their use as inline property filters in
  patterns (`{p:v, q:w}` is supported in `MATCH (n {p:v})` and
  `SET n = {p:v}` but is not a first-class expression value).
- Map projection (`n {.prop1, .prop2}`).
- `point()`, `distance()`, geo/temporal beyond `xsd:date`/`xsd:dateTime`.

Functions not in the included list raise a clear deferred-feature
error at parse/lower time. Adding a function later that maps to an
existing IR variant is small; adding one that needs new IR is a
follow-up project.

## Out of scope (deferred to v1.1+)

These produce a clear `UnsupportedFeature` error in v1 with a message
pointing at this document. Error message templates live next to the
SPARQL/JSON-LD deferred error catalog so the user sees one vocabulary
across all three surfaces.

- Path values (`MATCH p = (a)-->(b)`) and path-typed RETURN.
- `shortestPath` / `allShortestPaths`.
- `LOAD CSV`, `FOREACH`, `CALL` with side effects.
- Stored procedures (`CALL apoc.*` etc.).
- Schema DDL (`CREATE INDEX`, `CREATE CONSTRAINT`).
- Multi-statement scripts (`;`-separated).
- Map projections beyond trivial property selection.
- Implicit query parameters via session state (Bolt protocol). v1
  accepts `$param` syntax in queries but expects parameter values in a
  separate JSON body field, same as Neo4j HTTP API.
- Geospatial / temporal types beyond `xsd:date / xsd:dateTime`.
- Cypher's `point()`, `distance()`, etc.
- Schema-aware MERGE (where uniqueness constraints choose which
  property matches).
- GQL session-graph semantics. The default graph is the ledger the
  request targets; Cypher's `USE` is rejected.
- GQL-only keywords (`INSERT` as alias for `CREATE`, etc.). Parser may
  accept them as future-compat sugar in v1.1.

## Milestone overview

Each milestone is one PR. Lex and parse may combine if the diff stays
reviewable.

| ID | Scope | Status |
|----|-------|--------|
| M5.0 | Crate scaffolding, workspace wiring, feature flags | ✅ Done |
| M5.1 | Lex (Cypher tokens) | ✅ Done |
| M5.2 | AST + parser (v1 read + write surface) | ✅ Done |
| M5.3 | Query-path lower → shared IR; first round-trip with JSON-LD | ✅ Done |
| M5.4 | Write surface — CREATE / SET / REMOVE / MATCH…CREATE → `Txn` | 🟡 Partial — CREATE, MATCH…SET, MATCH…REMOVE, MATCH…CREATE done; DELETE / DETACH DELETE and `SET n = {…}` deferred (see Implementation status) |
| M5.5 | Single-node MERGE + ON CREATE / ON MATCH | 🟡 Partial — single-node MERGE + ON CREATE SET ship as a single Txn (NOT EXISTS guard); ON MATCH SET / relationship MERGE deferred |
| M5.6 | HTTP + CLI wiring, content negotiation, parameter passing | 🟡 Partial — CLI (read + write, auto-detect), HTTP ledger-scoped routes (`application/cypher` on query/update), and parameter passing (scalar/list, `{cypher,params}` envelope) all wired; remaining: HTTP content-negotiation parity (tracking/delimited/agent-json/policy) |
| M5.7 | Tests, docs, openCypher TCK subset | 🟡 Partial — lowering + end-to-end round-trip tests for the shipped surface; TCK subset not yet |

Variable-length paths have since **landed** (anonymous, single-typed
`-[:T*m..n]->` / `-[:T*]->`); **bounded** ranges are now
relationship-uniqueness compliant (Neo4j parity on cyclic graphs),
unbounded is endpoint-reachability — see "Variable-length paths —
landed" in the semantic model. Bound rel-var var-length and first-class
path values remain deferred.

---

## M5.0 — Crate scaffolding

**Goal:** new `fluree-db-cypher/` crate compiles in the workspace and
re-exports a stub `parse_cypher(input: &str) -> ParseOutput` that
returns "not implemented". Nothing surfaced to users yet.

### Files

- `Cargo.toml` (workspace) — add `fluree-db-cypher` to members.
- `fluree-db-cypher/Cargo.toml` — mirror the SPARQL crate's shape;
  `winnow`, `thiserror`, `tracing`, `serde`/`serde_json`, feature-gated
  internal deps.
- `fluree-db-cypher/src/lib.rs` — public surface stubs:
  ```rust
  pub fn parse_cypher(input: &str) -> ParseOutput { /* todo */ }
  #[cfg(feature = "lowering")]
  pub fn lower_cypher<E: IriEncoder>(
      ast: &CypherAst, encoder: &E, vars: &mut VarRegistry,
  ) -> Result<Query> { /* todo */ }
  ```
- `fluree-db-cypher/src/{lex,ast,parse,lower,validate,span.rs}` —
  empty modules, doc-only.

### Definition of done

- `cargo check -p fluree-db-cypher` passes.
- `cargo check --workspace --all-features --all-targets` passes.
- Empty test file `fluree-db-cypher/tests/it_smoke.rs` runs.

---

## M5.1 — Lex

**Goal:** lexer recognizes all Cypher v1 tokens with correct
longest-match precedence. No parse or AST work yet.

### Token shape

Cypher tokens fall into these groups:

- **Keywords (case-insensitive per Cypher convention)**: `MATCH`,
  `OPTIONAL`, `WHERE`, `RETURN`, `DISTINCT`, `AS`, `AND`, `OR`, `XOR`,
  `NOT`, `IN`, `IS`, `NULL`, `TRUE`, `FALSE`, `ORDER`, `BY`, `ASC`,
  `DESC`, `SKIP`, `LIMIT`, `UNION`, `ALL`, `WITH`, `UNWIND`, `CREATE`,
  `MERGE`, `ON`, `SET`, `REMOVE`, `DELETE`, `DETACH`, `CASE`, `WHEN`,
  `THEN`, `ELSE`, `END`, `STARTS`, `ENDS`, `CONTAINS`, `CALL`, `YIELD`,
  `EXISTS`, `COUNT`, `COLLECT`, `SUM`, `AVG`, `MIN`, `MAX`.
- **Identifiers**: `[A-Za-z_][A-Za-z0-9_]*` and backtick-quoted
  ``` `weird name` ```.
- **Numbers**: integer, decimal, scientific, hex (`0x...`), octal
  (`0o...`).
- **Strings**: single- and double-quoted, with `\` escapes including
  `\u{...}`.
- **Parameters**: `$name` and `$0`, `$1`, etc.
- **Punctuation**: `(`, `)`, `[`, `]`, `{`, `}`, `,`, `;`, `.`, `..`
  (range), `:`, `::` (type cast — reject in v1), `=`, `<>`, `<`, `<=`,
  `>`, `>=`, `+`, `-`, `*`, `/`, `%`, `^`, `+=`, `|`.
- **Relationship arrows**: `->`, `<-`, `--`, `-`, with the bracketed
  forms `-[`, `]->`, `]-`, `<-[`, `]-(` parsed as token pairs by the
  parser (the lexer emits individual punctuation).
- **Comments**: `//` line comments and `/* ... */` block comments.

### Precedence rules

Most are obvious; the non-obvious ones:

- `<>` must be tried before `<` and `>`.
- `<=`, `>=`, `+=` before single-char.
- `..` (range) before `.`.
- `::` (reject) before `:`.

### Files

- `fluree-db-cypher/src/lex/token.rs` — `TokenKind` enum + `Display`.
- `fluree-db-cypher/src/lex/lexer.rs` — winnow parsers; same style as
  `fluree-db-sparql/src/lex/lexer.rs`.
- `fluree-db-cypher/src/lex/chars.rs` — identifier/digit character
  predicates.

### Tests

- One unit test per token kind.
- Negative tests for `::` cast, `<<`, `>>` (Cypher does not use them;
  ensure they don't tokenize to anything special).
- Comment-skipping test.
- Keyword case-insensitivity test (`MATCH` vs `match` vs `Match`).

### Definition of done

- [ ] All tokens produced for the canonical examples in this doc.
- [ ] No regression in SPARQL lexer tests (Cypher crate doesn't touch
      SPARQL code).

---

## M5.2 — AST + parser

**Goal:** parse the v1 read surface and the v1 write surface into a
stable AST. Lowering is the next slice.

### Parser strategy

Pratt-style precedence-climbing for expressions, recursive descent for
statements and patterns. Same approach as `fluree-db-sparql/src/parse/`.

### AST sketch

```rust
// fluree-db-cypher/src/ast/mod.rs

pub struct CypherAst {
    pub statement: Statement,
    pub span: SourceSpan,
}

pub enum Statement {
    Query(QueryStmt),       // MATCH ... RETURN ...
    Update(UpdateStmt),     // CREATE / SET / DELETE / MERGE
}

pub struct QueryStmt {
    pub clauses: Vec<Clause>,  // Match, With, Unwind, Where, Return
}

pub enum Clause {
    Match { optional: bool, pattern: Pattern, where_: Option<Expr> },
    With  { items: Vec<ProjectionItem>, where_: Option<Expr>,
            order_by: Vec<OrderItem>, skip: Option<Expr>,
            limit: Option<Expr>, distinct: bool },
    Unwind { expr: Expr, alias: Variable },
    Return { items: Vec<ProjectionItem>, distinct: bool,
             order_by: Vec<OrderItem>, skip: Option<Expr>,
             limit: Option<Expr> },
    Create  { pattern: Pattern },
    Merge   { pattern: Pattern, on_create: Vec<SetItem>,
              on_match: Vec<SetItem> },
    Set     { items: Vec<SetItem> },
    Remove  { items: Vec<RemoveItem> },
    Delete  { detach: bool, exprs: Vec<Expr> },
}

pub struct Pattern { pub parts: Vec<PatternPart> }
pub enum PatternPart { Node(NodePat), Rel(RelPat) }  // alternating

pub struct NodePat {
    pub var: Option<Variable>,
    pub labels: Vec<Label>,
    pub props: Option<MapLit>,   // {k: v, ...}
}

pub struct RelPat {
    pub var: Option<Variable>,
    pub direction: Direction,    // Out, In, Either
    pub types: Vec<RelType>,     // multiple via |
    pub length: Option<LengthRange>,  // *, *N, *N..M
    pub props: Option<MapLit>,
}

pub enum Direction { Out, In, Either }

pub struct LengthRange { pub min: Option<u32>, pub max: Option<u32> }

pub enum Expr {
    Var(Variable),
    Lit(Literal),
    Param(String),
    Prop(Box<Expr>, String),       // x.prop
    BinOp(BinOpKind, Box<Expr>, Box<Expr>),
    UnaryOp(UnaryOpKind, Box<Expr>),
    Func(String, Vec<Expr>),
    Case(CaseExpr),
    Exists(Box<Pattern>),
    ListLit(Vec<Expr>),
    MapLit(MapLit),
    // ... etc.
}
```

### Property-path interaction

Cypher's variable-length `-[:T*1..5]->` becomes a `RelPat` with
`length: Some(LengthRange { min: Some(1), max: Some(5) })`. The
parser does not flatten it into multiple `RelPat` instances; that's
the lowering's job. The parser also accepts a bound rel-var form
(`-[r:T*1..5]->`) syntactically, but lowering rejects it — binding a
variable to a variable-length path needs list-valued bindings (deferred).

### Failure modes the parser rejects

- `MATCH (a)-[r]-(b)` with type list and relationship var when the
  type alternatives include both inbound and outbound semantics (rare
  edge case; emit a clear error).
- Multiple statements separated by `;` — point at the multi-statement
  deferred feature.
- `CALL` with anything other than a parenthesized subquery —
  procedure invocation is rejected.
- `::` cast operator — point at "type system not surfaced in v1".

### Tests

- Round-trip parse tests for every shape in the v1 surface tables.
- Negative tests for each deferred shape; error message contains the
  feature name.

### Definition of done

- [ ] All v1 read and write shapes parse without error.
- [ ] All deferred shapes produce the documented error message and
      span.
- [ ] No SPARQL crate regressions.

---

## M5.3 — Lower (read path)

**Goal:** Cypher `MATCH ... RETURN` queries execute against the shared
IR. Once this slice lands, JSON-LD inserts of edge-annotation data
round-trip cleanly with Cypher reads.

### Files

- `fluree-db-cypher/src/lower/mod.rs` — top-level dispatch
  (`lower_cypher`).
- `fluree-db-cypher/src/lower/pattern.rs` — Cypher pattern →
  `Vec<Pattern>` for the WHERE clause.
- `fluree-db-cypher/src/lower/expr.rs` — Cypher `Expr` → IR
  `Expression`. Most operators map 1:1.
- `fluree-db-cypher/src/lower/projection.rs` — RETURN items →
  `QueryOutput::Select` projections.

### Rule 1 — Node pattern lowering

```text
NodePat { var: Some(v), labels: [L1, L2], props: {k1:lit1} }

  ===>

Triple(?v, rdf:type, <L1 IRI>)
Triple(?v, rdf:type, <L2 IRI>)
Triple(?v, <k1 IRI>, lit1)
```

Anonymous node patterns (no `var`) get a fresh non-distinguished Var
using the `?#__cy_<n>` convention — the `?#` prefix is uncollidable
(`#` is comment-start in SPARQL var lex) and hidden from `RETURN *`.

Bare `(n)` patterns are rejected at lower time, per "Node existence
model". The pattern parser produces an AST; the lower step checks
that the node carries at least one of: a label, a property filter, or
a participating relationship in the same `MATCH` clause.

### Rule 2 — Relationship pattern lowering (the core mapping)

Three input shapes, three lowering targets, as per "Relationship
lowering rule" in the semantic model.

**Shape 1 — anonymous, no property filter:**

```text
NodePat(a) -- RelPat(var=None, type=T, dir=Out, props=None) --> NodePat(b)

  ===>

Triple(?a, <T IRI>, ?b)
```

Set semantics. Matches plain RDF triples.

**Shape 2 — named relationship (`-[r:T]->`):**

```text
NodePat(a) -- RelPat(var=Some(r), type=T, dir=Out, props=None) --> NodePat(b)

  ===>

EdgeAnnotation {
    edge: Triple(?a, <T IRI>, ?b),
    annotation: ?r,
    body: vec![],
}
```

Bag semantics. Matches only edges that have a reifier bundle. `?r`
binds to the annotation SID.

**Shape 3 — relationship property filter (`-[:T {p:v}]->` or
`-[r:T {p:v}]->`):**

```text
NodePat(a) -- RelPat(var=v?, type=T, dir=Out, props={p:lit}) --> NodePat(b)

  ===>

EdgeAnnotation {
    edge: Triple(?a, <T IRI>, ?b),
    annotation: ?r (if named) or fresh non-distinguished Var,
    body: vec![Triple(?annotation, <p IRI>, lit)],
}
```

Bag semantics. Matches only edges with a reifier bundle whose body
satisfies the property filter.

**Direction handling:**
- `Out` (`-[]->`): subject = a, object = b as shown.
- `In` (`<-[]-`): subject = b, object = a (swap).
- `Either` (`-[]-`): **forward∪reverse `Union`** — the reverse branch finds the edge via the `Opst` object index (landed 2026-06-14).

**Multiple types `[:T1|T2]`** (for shapes 1 and 2):

```text
Shape 1, types=[T1, T2]:
  Triple(?a, ?__pred, ?b)
  Filter(?__pred IN [<T1 IRI>, <T2 IRI>])

Shape 2 with name r:
  EdgeAnnotation { edge: Triple(?a, ?__pred, ?b), annotation: ?r, body: [] }
  Filter(?__pred IN [<T1 IRI>, <T2 IRI>])
```

The predicate-Var form is preferable to a `Union` of two
`EdgeAnnotation` patterns because the executor's reverse-attachment
lookup can fan out across an IN-list more cheaply than across separate
branches.

**Untyped relationship `-[r]->`** (no type given):

```text
Shape 1 untyped:
  Triple(?a, ?__pred, ?b)
  Filter (existing include_system_facts=false equivalent)

Shape 2 untyped with name r:
  EdgeAnnotation { edge: Triple(?a, ?__pred, ?b), annotation: ?r, body: [] }
  Filter (system-predicate exclusion)
```

The system-predicate exclusion reuses the same machinery as
`include_system_facts = false` in the existing query path. This
prevents `?__pred` from binding to `f:reifiesSubject` etc. Open Q3
discusses this.

### Rule 3 — OPTIONAL MATCH

```text
OPTIONAL MATCH <patterns>

  ===>

Optional(<inner-lowered patterns>)
```

Maps 1:1 to existing `Pattern::Optional`.

### Rule 4 — WITH ... WHERE ... RETURN ...

`WITH` introduces a subquery boundary:

```text
MATCH X
WITH a, count(*) AS c WHERE c > 5
MATCH Y
RETURN ...

  ===>

Subquery {
    inner: Query {
        patterns: [X],
        output: Select(a, count(*)) bind c,
        ... filter c > 5
    },
    binds: [a, c],
}
+ patterns Y
+ projection
```

The existing `Pattern::Subquery` covers this; the lowering's job is
chopping the clause list at `WITH` boundaries and emitting a
correlated subquery.

### Rule 5 — UNWIND

`UNWIND $list AS x` and `UNWIND [lit1, lit2] AS x` lower to whatever
list-binding pattern the IR already exposes (SPARQL `VALUES` is the
closest shape — confirm during M5.3 whether `Pattern::Values` is
directly usable, or whether a thin wrapper is needed). v1 list
elements are scalars or shallow maps; arbitrary expression-built lists
are deferred.

### Cardinality contract enforced here

The Cypher MATCH lowering selects the per-relationship IR variant
that produces Cypher's expected cardinality:

- Anonymous, no property filter → plain `Triple`, set semantics, sees
  plain RDF. Same cardinality as SPARQL bare-triple.
- Named or property-filtered → `EdgeAnnotation`, bag semantics
  (per-occurrence), only sees reifier-bundled edges.
- `RETURN DISTINCT` always falls back to set semantics regardless.

This deliberately surfaces a Cypher-vs-RDF impedance to users at the
syntax level: the `r` you wrote means "I want relationship identity",
which only exists for reifier-bundled edges. The Cypher docs page
makes this rule explicit.

### Definition of done

- [ ] All v1 read shapes execute against in-memory + indexed snapshots.
- [ ] Bare `MATCH (n)` and bare `CREATE (n)` are rejected at lower
      time with the documented error.
- [ ] Shape-1 anonymous relationship reads plain RDF triples (proven
      by inserting via JSON-LD without `@annotation`, then matching
      via Cypher).
- [ ] Shape-2 named relationship reads only reifier-bundled edges
      (proven by inserting via JSON-LD `@annotation`, then matching
      via Cypher with `r`).
- [ ] Round-trip parity: Cypher write → JSON-LD read returns identical
      bindings to JSON-LD write → JSON-LD read of the same logical
      data.
- [ ] Untyped `-[r]->` does not bind `?__pred` to `f:reifies*`.

---

## M5.4 — Lower (write path)

**Goal:** Cypher writes route through the same staging pipeline as
JSON-LD and SPARQL.

### Files

- `fluree-db-transact/src/lower_cypher_update.rs` (new) — analogous to
  `lower_sparql_update.rs`. Consumes `CypherAst::Update(UpdateStmt)`,
  produces `Txn`.
- `fluree-db-transact/src/parse/edge_annotations.rs` — no changes;
  Cypher writes funnel into the same `f:reifies*` emitter.

### Per-clause rules

#### CREATE

```text
CREATE (a:L {p:v})-[:T {q:w}]->(b:L2)
```

For each new node:
- Mint a subject SID (unless `@id`-bound via parameter; v1 also
  accepts `MERGE`-style identifying keys via M5.5).
- Emit `(?a, rdf:type, L)`, `(?a, p, v)`.

For each relationship:
- Emit base triple `(?a, ex:T, ?b)`.
- Emit the standard reifier bundle (`f:reifies*`) with a fresh
  annotation SID — **always**, per LPG-mode default for Cypher.
- Emit `(ann, q, w)` for relationship properties.

If `MATCH ... CREATE ...`, bindings from WHERE drive the template per
SPARQL Update §4.1.3 semantics (per-solution fresh blank nodes for
unbound parts).

#### SET — bounded scopes per the property/relationship boundary

Per "SET property/relationship boundary" in the semantic model:

```text
SET n.prop = expr      → DELETE (n, <prop>, *), INSERT (n, <prop>, expr)
SET n += {p:v, q:w}    → For each key k: DELETE (n, <k>, *), INSERT (n, <k>, val).
SET n = {p:v, q:w}     → DELETE (n, ?p, ?o) where:
                            ?o is a literal, AND
                            ?p is not rdf:type, AND
                            ?p is not in the f:* system namespace.
                         Then INSERT one triple per key in the map.
SET r.prop = expr      → SET on annotation SID r. Touches the
                         annotation body, not the base edge or the
                         reifier system predicates.
SET n:Label            → INSERT (n, rdf:type, <Label>). Additive.
```

The bounded retract scope for `SET n = {...}` is the critical safety
rule: it does not wipe labels, outbound or inbound relationships, or
system facts.

#### REMOVE

```text
REMOVE n.prop          → DELETE (n, <prop>, *)
REMOVE n:Label         → DELETE (n, rdf:type, <Label>)
```

#### DELETE / DETACH DELETE

Per "DELETE / DETACH DELETE in RDF" in the semantic model:

```text
DELETE r               → Retract attachment row for r + cascade
                         owned annotation facts (LPG mode).

DELETE n               → Staging-time check: count (n, p, o) and
                         (s, p, n) where the non-n position is a ref
                         (IRI/blank), p ∉ rdf:type, p ∉ f:* namespace.
                         If count > 0: error pointing at DETACH DELETE.
                         If count == 0: retract all triples about n.

DETACH DELETE n        → For each outbound relationship (n, p, o):
                            retract reifier bundle if present,
                            retract base triple.
                         For each inbound relationship (s, p, n):
                            retract reifier bundle if present,
                            retract base triple.
                         Retract all remaining (n, p, o) and (s, p, n).
```

The relationship-detection check is two index probes (SPOT subject
fan-out and OPST object fan-out) with the predicate filter applied
on the fly.

### Cascade and policy

All inherited from the edge-annotations storage layer:
- Plain-edge DELETE cascades the bundle. In LPG mode (Cypher default),
  explicit-IRI annotation subjects also cascade.
- Reserved-predicate firewall covers `f:reifies*` system predicates.
- Policy + history work without change.

### Parameter passing

```text
CREATE (n:Person {name: $name, age: $age})
```

Parameters arrive in the HTTP request body as a separate `params`
object:

```json
{
  "cypher": "CREATE (n:Person {name: $name})",
  "params": {"name": "Alice"}
}
```

Substitution happens at lowering time before staging. v1 supports
literal substitution only; computed-parameter values (e.g.,
`$people` as a list driving `UNWIND $people AS p CREATE (n:Person {name: p.name})`)
are part of M5.4.

### Definition of done

- [ ] Insert via Cypher CREATE, query via JSON-LD: identical results.
- [ ] Insert via Cypher CREATE, query via SPARQL: identical results.
- [ ] DELETE r via Cypher triggers the same cascade as SPARQL UPDATE
      DELETE of a reifier bundle.
- [ ] DETACH DELETE n correctly retracts a node with N relationships.
- [ ] `UNWIND $list AS x CREATE ...` round-trips for list-of-maps.

---

## M5.5 — MERGE

**Goal:** Cypher's find-or-create semantics. The semantically loaded
clause; deserves its own slice.

### Surface

```cypher
MERGE (n:Person {name: "Alice"})
ON CREATE SET n.created = timestamp()
ON MATCH  SET n.lastSeen = timestamp()
```

```cypher
MATCH (a:Person {name: "Alice"})
MERGE (a)-[:KNOWS]->(b:Person {name: "Bob"})
ON CREATE SET b.created = timestamp()
```

### Semantics

`MERGE` is *find a pattern, otherwise create it*. The identifying
property bag is the entire inline `{...}` map. Cypher considers
**every property in the map** as identifying — there is no
"identifying subset" without a uniqueness constraint.

Lowering proceeds in three steps at staging time:

1. **Search phase**: build a WHERE-equivalent pattern from the MERGE
   shape. Run it against the snapshot.
2. **Branch**:
   - At least one binding: apply `ON MATCH` SET items to each binding.
   - Zero bindings: apply CREATE template, then apply `ON CREATE` SET
     items.

The "every property is identifying" rule lets the search step reuse
the same lowering as `MATCH` directly — `MERGE (n {p:v, q:w})` lowers
to a search for nodes with both `(n, p, v)` and `(n, q, w)` asserted.

### v1 supported shapes

- Single-node MERGE only: `MERGE (n:Label {p:v})`.
- ON CREATE / ON MATCH SET clauses (any combination, including none).

### v1 deferred

- **Single-edge MERGE** (`MERGE (a)-[:T]->(b)`) — pushed to v1.1.
  Reason: the matching semantics interact with the relationship-
  lowering rule (only sees reifier-bundled edges when the relationship
  variable participates), and getting this right requires more design.
- MERGE on patterns longer than one relationship.
- MERGE with relationship properties as identifying keys.
- Schema-aware MERGE (where a uniqueness constraint chooses which
  property is identifying).

### Atomicity

MERGE must be atomic within the transaction. Search and create-or-set
happen in the same `Txn`. The staging pipeline already runs `WHERE`
against a frozen snapshot before applying any inserts, so the search
phase sees a consistent view. No new atomicity machinery needed.

### Race conditions

Two concurrent MERGEs of the same identifying pattern can both
observe "not present" and both create. Neo4j relies on uniqueness
constraints to prevent this; without them, the same outcome can
happen there. Fluree's SHACL `f:enforceUnique` is the natural
parallel — when a SHACL constraint is in place, the optimistic
concurrency check at commit time will reject the duplicate. Document
this clearly.

### Definition of done

- [ ] `MERGE (n:Person {name: "Alice"})` creates exactly one node on
      first run, finds it on second.
- [ ] `ON CREATE SET` and `ON MATCH SET` fire on the correct branch.
- [ ] Single-relationship MERGE with a previously-MATCHed left side
      creates the edge + reifier exactly once.
- [ ] Test against a SHACL `f:enforceUnique` constraint that the
      duplicate-create race is caught by validation.

---

## M5.6 — HTTP + CLI wiring

**Goal:** Cypher reaches users.

### HTTP

In `fluree-db-server/src/routes/query.rs`:

- Accept `Content-Type: application/cypher` (de facto Neo4j MIME) and
  `application/openCypher` for the read endpoint.
- Accept the same for the write endpoint (`/transact`).
- JSON body envelope accepted for both:
  ```json
  {
    "cypher": "MATCH (n:Person) RETURN n",
    "params": {"limit": 10}
  }
  ```
- Plain-text body accepted for parameter-less queries (matches Neo4j
  HTTP API convention).

Reuse the SPARQL routes' content-negotiation pattern. Add:
- `FlureeHeaders::wants_cypher()` predicate.
- Format selection: Cypher queries return JSON-LD by default. SPARQL
  Results JSON is also valid for SELECT-shape queries. Neo4j's
  result-row format is rejected in v1 — document the deviation.

### CLI

In `fluree-db-cli/src/commands/query.rs` and `update.rs`:

- New `--cypher` flag, mutually exclusive with `--sparql`.
- Auto-detection via `detect_query_format()` — Cypher is detected by
  presence of `MATCH`, `CREATE`, `MERGE` keywords near the top.
  Fall through to JSON-LD if uncertain.
- Remote client gains `query_cypher_accept_bytes()` and `update_cypher()`.
- `--explain --cypher ...` prints the plan as JSON without executing.

### Library

In `fluree-db-api/src/lib.rs`:

- Re-export `parse_cypher`, `lower_cypher` from `fluree-db-cypher`.
- Re-export `lower_cypher_update` from `fluree-db-transact`.
- Add `GraphQueryBuilder::cypher(&str)` method, parallel to existing
  `.sparql(&str)`.
- Add `GraphTransactBuilder::cypher(&str)` method.

### Definition of done

- [ ] `curl -H 'Content-Type: application/cypher' --data 'MATCH (n) RETURN n LIMIT 5' .../query` works.
- [ ] `fluree query --cypher 'MATCH (n) RETURN n LIMIT 5'` works.
- [ ] `fluree update --cypher 'CREATE (n:Person {name: "Alice"})'` works.
- [ ] `fluree.graph("mydb").cypher("MATCH (n) RETURN n").await` works
      from Rust.

---

## M5.7 — Tests, docs, TCK subset

### Tests

`fluree-db-api/tests/it_query_cypher.rs` — read-path tests:

- Node patterns with labels and property filters.
- Relationship patterns: anonymous, named, typed, untyped, alternative
  types, both directions, undirected (if shipped).
- Property filter on relationships via `EdgeAnnotation` body.
- Variable-length paths: bounded, unbounded, reflexive.
- OPTIONAL MATCH.
- WHERE expressions (comparison, IS NULL, IN, list functions).
- WITH ... WHERE ... boundary.
- UNWIND of literal list and bound list.
- Aggregations: count, sum, avg, collect.
- ORDER BY, SKIP, LIMIT.
- UNION / UNION ALL.
- Parallel relationships return one row per occurrence.
- Cross-surface parity: JSON-LD insert → Cypher read returns same
  bindings as SPARQL read of same data.

`fluree-db-api/tests/it_transact_cypher.rs` — write-path tests:

- CREATE node, relationship, mixed patterns.
- SET node prop, relationship prop, += map merge, = map replace.
- REMOVE prop, REMOVE label.
- DELETE r cascades reifier bundle.
- DELETE n rejects when relationships remain.
- DETACH DELETE n cascades.
- MERGE node: create on first, match on second.
- MERGE edge: create and match branches.
- ON CREATE / ON MATCH SET branches.
- Parameter substitution: literal, list of literals, list of maps.
- LPG-mode cascade verified.
- Reserved-predicate firewall: CREATE that mentions `f:reifiesSubject`
  directly is rejected.

`fluree-db-api/tests/it_query_cypher_indexed.rs`:
- Reindex between insert and query; arena read path works through
  Cypher surface.

### openCypher TCK subset

The openCypher project ships a Technology Compatibility Kit (TCK) as
Cucumber feature files. v1 doesn't run the full TCK; instead, port
the **Read1, Match1, Match2, Match3, Comparison1** TCK groups as
manually-translated Rust integration tests. These exercise the read
surface end-to-end against canonical Cypher semantics.

Full TCK harness integration is a follow-up.

### Docs

- `docs/concepts/cypher.md` (new) — user-facing overview, mapping to
  RDF/JSON-LD, what's supported, what's deferred. Cross-link from
  `docs/concepts/edge-annotations.md` since Cypher relationship
  properties surface the same primitive.
- `docs/getting-started/cypher.md` (new) — quickstart with examples.
- `docs/query/cypher.md` (new) — full v1 reference.
- `docs/transactions/cypher.md` (new) — write surface.
- `docs/SUMMARY.md` — link the four new pages.
- `CLAUDE.md` — add Cypher to the "Patterns to Follow" or feature
  table.

### Definition of done

- [ ] All tests pass on memory + file storage.
- [ ] `cargo nextest run --workspace --all-features --no-fail-fast`
      passes.
- [ ] TCK subset tests pass.
- [ ] Docs updated; SUMMARY.md links new pages.

---

## Open questions

These need answers before or during M5.2, but don't block M5.0/M5.1.

### Q1 — Undirected relationships `(a)-[r]-(b)`

Cypher returns each match twice (once per direction) when the
relationship is undirected. Two options:

- **(a)** Lower to `Union` of both directions. Correct semantics; may
  double-count for symmetric data.
- **(b)** Reject `-[r]-` in v1 with a clear error message pointing
  users at the explicit `<-[r]-` / `-[r]->` forms.

Recommendation: **(b)** for v1. Most real Cypher queries are directed;
the syntactic ambiguity is a footgun. Revisit when the user need
becomes concrete.

### Q2 — `id(n)` semantics

Neo4j's `id(n)` returns an internal unstable integer. Fluree's SIDs
are also internal unstable integers. Options:

- **(a)** Return the SID directly. Faithful but exposes encoding.
- **(b)** Return the IRI string. Stable but type-incompatible with
  Neo4j queries that use `id(n)` numerically.

Recommendation: **(a)** with the same caveat Neo4j docs carry ("don't
rely on it across imports"). Add a separate `iri(n)` function for the
stable-identity case.

### Q3 — Cypher's untyped relationships and system predicates

`MATCH (a)-[r]->(b)` (no type) lowers to a triple with a variable
predicate. Without filtering, `?r` would bind to system predicates
including `f:reifies*` and other `f:` namespace items — wrong from
a user's standpoint.

Recommendation: **reuse the existing `include_system_facts = false`
filter** that the query path already exposes (used elsewhere for
hiding system facts in default subject-expansion). The Cypher lower
sets that flag (or the equivalent IR-level predicate exclusion) on
any pattern where the predicate position is a Var. Adding Cypher-
specific tests verifies the filter is hit. No new firewall machinery
is invented.

### Q4 — Multi-label MERGE

`MERGE (n:L1:L2 {p:v})` — is the identifying key the property bag
alone, or property bag + label set? Cypher spec says label is part of
the pattern, so the search must find a node with both labels.
Confirmation: yes, the search lowers to:

```text
Triple(?n, rdf:type, L1)
Triple(?n, rdf:type, L2)
Triple(?n, p, v)
```

If zero results, create with all three. Document.

### Q5 — Should v1 ship GQL keyword aliases?

`INSERT` for `CREATE`, `FILTER` for `WHERE`, etc. Cost is parser-only.

Recommendation: **no** — adds surface area for v1 without buying any
user-facing value (no GQL drivers in the wild yet). Add when there's
a concrete GQL use case to validate against.

---

## Cross-cutting concerns

- **Tracing.** No new spans; existing `parse / lower / execute` spans
  cover the Cypher path the same way they cover SPARQL. If the lowering
  becomes expensive enough to want a span, add `debug_span!("cypher.lower")`
  scoped to the lowering call.
- **Policy / history / cascade.** All inherited from the
  edge-annotations storage layer. Cypher writes are indistinguishable
  from SPARQL writes once they reach the staging pipeline.
- **Reserved-predicate firewall.** Existing wiring covers Cypher writes
  via the shared staging path. Query-side firewall extension (Q3) is
  net new.
- **W3C testsuite.** `cd testsuite-sparql/ && make count-eval` must
  remain unchanged after each Cypher milestone. Cypher and SPARQL
  share IR; if a Cypher slice ever changes the IR, SPARQL semantics
  must be unaffected.
- **Edge-annotation tests.** The annotation round-trip tests
  (`it_query_sparql_annotations.rs`, `it_edge_annotations.rs`) must
  keep passing after each Cypher slice. Cypher inserts that round-trip
  to JSON-LD/SPARQL reads are the canonical cross-surface parity test.
- **Lambda/WASM builds.** Cypher follows SPARQL's pattern: the
  `lowering` feature is default-on for native builds and opt-out for
  reduced-size deployments. Parser-only Cypher (no execution) is a
  valid build target.

## Success criteria

- Ordinary RDF/JSON-LD queries are completely unaffected.
- Ordinary SPARQL queries (including edge-annotation queries shipped
  in M4) are completely unaffected.
- Cypher round-trips data inserted via JSON-LD `@annotation` and
  SPARQL `{| ... |}` to byte-identical results.
- Cypher `CREATE (a)-[r:T {p:v}]->(b)` writes identical staging
  records to the SPARQL `~ ?ann {| ... |}` form on the same input.
- Parallel relationships, named relationships, and property-less
  relationships preserve identity across all three surfaces.
- A user can write Cypher in the CLI, HTTP, and Rust API.
- `cd testsuite-sparql && make count-eval` shows no regression after
  any Cypher milestone.

## What I'd open as PR #1

M5.0 (crate scaffolding) is the cleanest first PR. It's small, it
proves workspace wiring, and it gives subsequent slices a place to
live. Concretely:

1. Add `fluree-db-cypher` to workspace `Cargo.toml`.
2. Mirror `fluree-db-sparql/Cargo.toml` for the new crate's
   manifest and feature shape.
3. Skeleton `src/lib.rs` with `parse_cypher` stub returning
   "not implemented".
4. Empty `lex/`, `ast/`, `parse/`, `lower/`, `validate/` modules.
5. `cargo check --workspace --all-features --all-targets` green.

M5.1 (lex) follows directly.
