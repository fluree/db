# GraphQL on Fluree — implementation plan

Status: **Done — Phases 0–4.** All three tiers work end to end. Point a GraphQL client at any
existing ledger and it introspects, filters, paginates and queries with no configuration; write
SHACL shapes and the schema gains cardinality, enums, documentation, reverse fields and closed
types; add a `graphql:Schema` and it decides what is published, names it, marks abstract classes as
interfaces, and — only if asked — opens a write surface. Nothing GraphQL-specific is ever written
outside the ledger itself.

Tests: 71 in `fluree-db-graphql`, 40 end-to-end in `fluree-db-api`, 12 over HTTP in
`fluree-db-server`, plus 6 for the JSON-LD nested-selection syntax this added. Benchmarks in
`fluree-db-api/benches/graphql_schema.rs`.

Known gaps, each with its reasoning below: nested `where`, nested arguments on a reverse field,
several `graphql:Schema` instances in one ledger, `::n` intra-mutation references, subscriptions,
and federation.

## Goal

A GraphQL endpoint on every ledger with three tiers of trust, each a strict superset
of the one below, selected by what metadata the ledger contains — not by a config file:

| Tier | Trigger | Schema source | Exposure | Mutations |
|---|---|---|---|---|
| 1 Inferred | nothing | HEAD stats (`IndexStats.classes`) | every class, every observed property, all nullable lists | off |
| 2 Shaped | `sh:NodeShape` with `sh:targetClass` present | shapes and stats build one model together; a shape wins where they overlap, and `sh:closed` decides whether observed-but-undeclared properties survive | as tier 1 | off |
| 3 Curated | a `graphql:Schema` instance present | only shapes it lists; no stats fallback | `graphql:publicShape` / `protectedShape` / `privateShape` | opt-in |

Design decisions already made:

- The customization layer is **SHACL in the ledger** plus the `http://datashapes.org/graphql#`
  vocabulary (shared by TopBraid EDG and GraphDB 11). No `.graphqls` upload, no YAML. One artifact
  drives validation and the API contract, versioned with the data, time-travels with `t`.
- Execution targets **JSON-LD queries** (`fluree-db-query`), not SPARQL text — and, as built, the
  JSON-LD query *document* rather than the resolved IR, so the engine's own parser validates and
  encodes it. A GraphQL selection set *is* a select-tree; root arguments lower to WHERE patterns.
- Tier 1 is deliberately lossy, in three specific ways, each because statistics cannot support the
  stronger claim:
  - **Nullable list fields everywhere.** No "max observed count = 1 → single" heuristic; it would
    break clients the first time someone writes a second value.
  - **No interfaces** (revised during implementation; the original plan derived them from
    `rdfs:subClassOf`). An RDF class with subclasses is usually still instantiable, so it would have
    to be both an interface and an object type, and one of the two would need an invented name —
    a naming convention that is hard to change once clients depend on it. A reference whose observed
    targets span several classes becomes a **union** instead, which needs no such convention.
    Interfaces arrive in tier 2/3, where `graphql:isInterface` says which classes are abstract.
    Tier 1 therefore does not read `SchemaHierarchy` at all.
  - **No reverse fields.** Stats record a reference from the subject's side; naming the other
    direction is guesswork. `sh:inversePath` is the explicit route (tier 2).
- Range fidelity beats familiar scalar names: `xsd:integer`/`long` map to a custom `Long`, not to
  GraphQL's 32-bit `Int`, and `xsd:decimal` to a custom `Decimal` rather than rounding into `Float`.
- Policy is Fluree policy, applied by pruning the generated schema (same as
  `cypher_procedures.rs` `class_denied` / `predicate_denied`). We do not model SOML-style RBAC.
- Mutations only in tier 3, and only when the `graphql:Schema` enables them.

## Prior art (for reference)

- Stardog: schema-less by default (`Type { pred }`, `prefix_local` naming, `@type/@optional/@bind/@filter/@hide`),
  optional registered `.graphqls`, `graphql.auto.schema` from RDFS/OWL/SHACL. Schema-less has no real introspection.
- GraphDB 11 / TopBraid: `graphql:Schema` + public/protected/private shapes, `graphql:name`, `graphql:isIDField`,
  `graphql:isInterface`; SHACL core for types/fields/cardinality/enums/unions; `where:{f:{EQ|IN|RE|LT..},AND,OR}`,
  `orderBy`, `limit/offset`, `type_count`, `create_/update_/delete_Type`, language-spec strings for langStrings.
- GraphQL-LD: JSON-LD-context-driven, no schema.

We copy GraphDB's query/mutation *conventions* (they are what LangChain/Apollo users already know) and the
datashapes vocabulary; we do not copy SOML.

## Architecture

```
POST /v1/fluree/graphql/{ledger}[/{schema}]        (server route, mirrors execute_cypher_ledger)
        │
        ▼
fluree-db-graphql (new crate)
  ├── schema/          SchemaModel: language-neutral type/field model + its 3 builders
  │     ├── inferred.rs   IndexStats.classes + SchemaHierarchy  → SchemaModel   (tier 1)
  │     ├── shaped.rs     CompiledShape overlay                  → SchemaModel   (tier 2)
  │     └── curated.rs    graphql:Schema instance                 → SchemaModel   (tier 3)
  ├── runtime.rs       SchemaModel → executable async-graphql dynamic schema (+ generated inputs)
  ├── sdl.rs           SchemaModel → GraphQL SDL (via the same registered schema)
  ├── selection.rs     parsed document + variables → owned selection tree (keeps fragment conditions)
  ├── lower/           selection tree + arguments → fluree_db_query::Query (select-tree + where)
  ├── mutate/          create_/update_/delete_ → JSON-LD transaction (tier 3 only)
  └── naming.rs        IRI ⇄ GraphQL identifier (prefix rules, collisions, reserved words)
        │
        ▼
fluree-db-api  (GraphQlSchemaCache on LedgerState, policy pruning, execution, formatting)
```

Crate placement follows `fluree-db-cypher`: parser/lowering crate depends on `fluree-db-query`,
`fluree-db-core`, `fluree-vocab`; the API crate owns caching, policy, and the ledger handle. SHACL
dependency is feature-gated like `fluree-db-api`'s `shacl` feature (tier 2/3 need it; tier 1 does not).

### GraphQL runtime library — decided (Phase 0)

**`async-graphql` 7.2 with the `dynamic` schema API**, `default-features = false, features = ["dynamic-schema"]`
(the default set also pulls `tempfile` for multipart uploads and an `askama`-templated GraphiQL page,
neither of which this repo serves). `apollo-compiler` is not needed: the spike
(`fluree-db-graphql/tests/it_dynamic_schema.rs`) registers interfaces, unions, custom scalars, and
*recursive* input objects, and gets spec-compliant validation, introspection, and SDL for free.
`Schema::sdl()` renders from the same registered schema that executes, so the printed and executable
schemas cannot drift.

The root resolver compiles the **entire** selection set to one Fluree query and returns hydrated JSON;
nested fields use one generic pass-through resolver. No N+1, and no per-field I/O.

Two things the spike settled that constrain the rest of the design:

- **The selection tree must come from the parsed document, not from the resolver.** async-graphql's
  resolver-facing selection APIs — `SelectionField::selection_set()` and `Lookahead` — flatten fragment
  spreads and inline fragments into the parent selection and **discard the type condition**
  (`context.rs` `SelectionFieldsIter`, `look_ahead.rs` `filter`). That makes `... on Person { name }`
  indistinguishable from a plain field, so unions and interfaces would be unlowerable. `selection.rs`
  therefore walks `ExecutableDocument` itself, keeping the condition on every node and substituting
  variables (including variable defaults). The caller parses once, extracts the tree, and attaches it to
  the request as data; the root resolver looks its own subtree up by response key. Validation still runs
  first — async-graphql rejects an invalid document before any resolver fires.
- **The executor's JSON is keyed by response key, not field name**, since the executor is what saw the
  aliases. At an interface or union position each object must carry `__typename`; the pass-through
  resolver uses it for `FieldValue::with_type`. Nested pass-throughs *borrow* out of the root's owned
  JSON (async-graphql keeps a parent `FieldValue` alive across its children), so only leaf values copy.

### Resource bounds

Every other read surface is bounded by the server's query timeout and its
client-disconnect cancellation, both installed by `run_query_task` and carried
in a `QueryExecutionOptions` the route builds. GraphQL has to join that scope
rather than invent its own, and two properties make it awkward:

- **The handle has to be shared across a whole request.** async-graphql resolves
  root fields concurrently, so one document is N queries. `LedgerExecutor` holds
  the options and hands the same clone to every `query_with_options` — one
  timeout, one disconnect, all N cancelled together. A per-field handle would
  bound each query and still let the fan-out run unbounded in aggregate.
- **The document's own cost is bounded separately.** A timeout limits how long
  one query runs; it does not limit how many a document launches, and it does
  nothing about recursion that happens before execution. A derived schema is
  cyclic wherever one class references another, so depth is the caller's choice.

Hence two limits, in `limits.rs`, defaulting to depth 15 and 1000 fields and
configurable as `graphql_max_depth` / `graphql_max_complexity`:

- **Depth is checked twice.** `Schema::build` gets `limit_depth`, but
  `parse_query` and `selection::extract` run *before* `schema.execute()` — a
  schema-level limit alone would let a deep document recurse through the
  extraction walk first. `selection::walk` therefore carries its own counter.
  It counts the way async-graphql's `DepthCalculate` does (a field is a level, a
  fragment is not), so the two agree on which documents are refusable.
- **Complexity is checked by the schema.** It bounds total fields across aliases
  and fragments, which is what caps the fan-out. The pre-execution walk does not
  duplicate it: the parse it would protect is already bounded by the body limit.

The limits are baked into the registered schema, which is cached per ledger
version — so they are part of `RegisteredKey`. Without that the first request
through would fix the ceiling for every later one.

## Mapping specification

### Types

| Source | GraphQL |
|---|---|
| class (stats or `sh:targetClass`) | `type` |
| class flagged `graphql:isInterface` / `dash:abstract` (tier 2/3 only) | `interface` + concrete `type`s implementing it |
| every type | `id: ID!` (the IRI, compacted with the ledger context), `__typename` |
| property observed with several `ref_classes`, or `sh:or` of node shapes | `union` (`so:useUnions` default true) |
| object-valued field with no exposed target type | built-in `Node { id: ID! }` (TopBraid's `_Resource`) |

### Fields

| Source | GraphQL |
|---|---|
| observed property / `sh:property` with `sh:path <iri>` | field |
| `sh:path [sh:inversePath <iri>]` | reverse field (lowers to `@reverse`) |
| `sh:datatype` / stats datatype tag | scalar: `xsd:string→String`, `int/short/byte→Int`, `integer/long/unsigned*→Long` (custom), `decimal→Decimal` (custom), `double/float→Float`, `boolean→Boolean`, `dateTime/date/time→DateTime/Date/Time` (custom), `anyURI→ID`, `rdf:langString→String` with `@lang(spec:)` arg, `rdf:JSON→JSON`, unknown→`String`. One table, in `schema/datatype.rs`; the IRI path resolves to a `ValueTypeTag` first so the tier-1 and tier-2 mappings cannot disagree. |
| mixed datatypes observed | `String` (rendered lexical form), except integer widths, which widen to `Long` — they are one number line |
| a property observed with both references and literals | `String`, plus a model warning: no GraphQL type covers both, and an IRI at least renders as its own lexical form |
| `sh:class` / `sh:node` / stats `ref_classes` | object type / union |
| `sh:maxCount 1` | single value; otherwise `[T]` |
| `sh:minCount ≥ 1` | `!` |
| `sh:in` (all IRIs or all strings) | `enum` |
| `sh:name`, `sh:description` | field description (docstring) — **`sh:description` is not compiled today, see Phase 2** |
| `sh:order` | field order in SDL |
| `sh:deactivated true` | omitted |
| `so:privateProperty` / `so:protectedProperty` | omitted / typed as `Node` |
| `sh:closed true` on a shape | unlisted observed properties omitted for that type |
| tier-1 property with no shape | `[Scalar]` or `[Type]`, nullable |

### Naming

1. `graphql:name` if present.
2. Otherwise local name of the IRI (after the ledger `@context` / default vocab), with `-` and `.`
   → `_`; leading digit → `_` prefix.
3. On collision within a schema, prefix with the context prefix: `foaf_name`. Deterministic: sorted by IRI.
4. Reserved: `id`, `__*`, root query names. A property literally named `id` becomes `pfx_id`.
5. Root fields: `person` (single by id), `persons` (list) and `persons_count`. Pluralisation is naive
   (`+s`, `y→ies`); `graphql:pluralName` overrides. Tier 3 may set `graphql:queryPrefix`.

### Query conventions (GraphDB-compatible)

```graphql
persons(
  where: { name: { RE: "^A" }, AND: [{ age: { GTE: 18 } }], friend: { name: { EQ: "Bob" } } }
  orderBy: { age: DESC }
  limit: 20, offset: 40
) { id name friend(limit: 5, orderBy: { name: ASC }) { id name } }
person(id: "ex:alice") { name }
persons_count(where: { age: { GT: 30 } })
```

Filter operators: `EQ NEQ IN NIN LT LTE GT GTE RE IRE NRE NIRE ID EXISTS`, combinators `AND OR NOT`,
nested-object filters recurse into a joined pattern. Multi-valued semantics: a filter on a list field
is `ANY` (exists a value satisfying); `ALL` is a follow-up.

Per-request variables (request envelope, not the document): `t` / `asOf` for time travel,
`schema` to pick a `graphql:Schema` when several exist, `lang` default language spec.

### Mutations (tier 3 only)

`create_Person(input: PersonInput!): Person`, `update_Person(ids: [ID!]!, set: PersonPatch!): {affected_count, affected_objects}`,
`delete_Person(ids: [ID!]!)`. Input types mirror the output types with object refs as `ID` or nested
`create`. New IRIs minted under `graphql:iriBase` (required when mutations enabled; no default base).
Lowered to the existing JSON-LD `insert` / `upsert` / `delete` paths so SHACL validation and policy apply
unchanged.

## Phases

Each phase ends mergeable and behind the `graphql` feature. Order is chosen so a demo works after Phase 1
on any existing ledger.

### Phase 0 — Spike ✅ done

`fluree-db-graphql` exists as a workspace member with `async-graphql` 7.2 wired in, and
`tests/it_dynamic_schema.rs` (10 tests) drives a hand-built `SchemaModel` end to end: SDL for every
construct, root resolver receiving the whole subtree with aliases and nested args, fragment type
conditions surviving extraction, pass-through resolution of nested lists and objects, union dispatch on
`__typename`, count/single roots, variable substitution with defaults, introspection, and the two
extraction errors (cyclic fragment, undefined variable). Runtime decision and its consequences are in
*GraphQL runtime library* above.

Landed already, ahead of where the phase list put them, because the spike needed them:

- `schema/model.rs` — the `SchemaModel` of Phase 1 step 1, complete.
- `runtime.rs` — model → executable schema, including the generated `where` / `orderBy` input types and
  the per-level `where`/`limit`/`offset`/`orderBy` arguments on nested list fields. The *schema surface*
  for nested args therefore exists; honouring them still needs the `NestedSelectSpec` change in Phase 1.
- `selection.rs` — the owned selection tree.
- `sdl.rs` — SDL via `Schema::sdl()`.

`naming.rs`, the three builders, and `lower/` are still to come.

### Phase 1 — Tier 1 read path (inferred schema) — done

1. ✅ **`SchemaModel`** (`schema/model.rs`).
2. ✅ **Inferred builder** (`schema/inferred.rs`) — done, but taking plain-IRI `ClassObservation`s
   rather than `IndexStats` directly. The SID→IRI resolution, the Fluree-system-vocabulary filter and
   the policy pruning all need a `LedgerSnapshot`, so they belong on the `fluree-db-api` side of the
   seam; the graphql crate then stays testable without ledger fixtures, and tier 2's `sh:datatype`
   IRIs feed the same code path. **Still to do in `fluree-db-api`:** build those observations from
   `assemble_fast_stats` with `NoveltyMerge::Reconciled` (site label `GRAPHQL_SCHEMA`, add to
   `runtime_stats.rs:70-81`). Note `StatsView` does not carry class→property; use
   `IndexStats.classes` / `union_per_graph_classes`.
3. ✅ **Naming** (`naming.rs`) per the rules above. The `Namer` is constructed from the ledger's
   default context (`db_with_default_context`) by the caller.
4. ✅ **Lowering** (`lower/`) — but to a **JSON-LD query document**, not to
   `fluree_db_query::Query`. The document is what the engine's own parser validates and encodes, so
   the crate inherits that instead of restating it; it is also directly reviewable, which is what
   makes `explain` worth having. The cost is one JSON parse per request, against a query about to
   touch the index. Every lowered query is written `"@context": {}` — no compaction — so result keys
   are exactly the IRIs requested and `lower/reshape.rs` can match them without reimplementing
   JSON-LD term selection; IRIs are compacted for the client afterwards by `Namer::compact`.

   What was originally planned:
   - root list field → `where: [{"@id":"?x","@type": C}]` + filters + `orderBy/limit/offset`;
     `*_count` → `select: ["(as (count-distinct ?x) ?count)"]`.
   - selection set → the JSON-LD select tree; aliases live in the shape tree, not the query.
   - `where` → JSON-LD filter patterns; nested-object filters → additional triple patterns.
   - **Nested field args (`friend(limit:, orderBy:, offset:)`)**: done. `ForwardItem::Property`
     gained an optional `NestedModifiers { order, offset, limit }`, honoured in `format_subject`,
     with a JSON-LD surface syntax to match — a nested selection's value may be an object of
     `select`/`orderBy`/`limit`/`offset` instead of a bare array. This closed the same gap for
     JSON-LD queries, which is documented in `docs/query/jsonld-query.md`.

     Three things this settled:
     - **Nested ordering takes a different input type from root ordering.** A root `orderBy` orders
       the query's *solutions*, so a multi-valued key would repeat subjects; a nested one sorts the
       values already fetched for one subject, so a multi-valued key is fine and sorts by its first
       value. Hence `PersonOrder` (strict) at the root and `PersonNestedOrder` (permissive) below.
     - **Modifiers are part of the hydration cache key.** Two levels selecting one predicate with
       different `limit`s produce different output; hashing only the selection made the second
       borrow the first's answer. Pinned by a test that fails if the hashing is removed.
     - **Nested `where` is still refused**, with a reason. Filtering a nested level means evaluating
       a predicate over already-materialized values, which is a different engine from the one that
       answers the WHERE clause. Ordering and paging act on the values as they stand.

   Corrections the implementation forced, each found by a failing test:

   - **`person(id:)` is a filtered list, not an IRI-constant expansion.** `"select": {"<iri>": [...]}`
     returns a bare `{"@id": ...}` stub for a subject that does not exist and does not check the
     subject's type, so `person(id: <a Company>)` would have returned a Person-shaped stub. A typed
     variable plus a `values` constraint yields no row in both cases — the `null` GraphQL expects.
   - **`count-distinct`, not `count`.** A filter binds a multi-valued field, so a plain count charges
     one subject once per matching value.
   - **JSON-LD collapses a single-element array to a bare value**, so `reshape` must re-expand every
     list field; a subject with no value for a selected list field gets `[]`, not `null`.
   - **`values` takes one `[vars, rows]` argument** and `not-exists` *spreads* its patterns.
     `docs/query/jsonld-query.md` documented the wrong `values` arity; fixed in the same change, with
     the multi-variable form added and both forms verified against the engine.
   - **`EXISTS` belongs on every `<T>Filter`**, since a reference field's filter is the target type's
     filter; a reference filter is split into operators on the reference and constraints on the
     referenced subject before either is lowered. At the top of a `where` there is nothing for
     `EXISTS` to test, and lowering says so rather than ignoring it.
   - **Ordering is `id`-only in tier 1**, and falls out of the model rather than being special-cased:
     `is_orderable` requires a single-valued field, and tier 1 has none but `id`. Ordering by a
     multi-valued property would multiply subjects rather than order them.
   - **IRI operands cannot go in an S-expression** (the filter language has no IRI atom), so `id` and
     reference constraints lower to `values` patterns. That covers `EQ`/`IN`/`EXISTS`; `NEQ`/`NIN`
     and ordering on IRI-valued fields are refused with a reason rather than mis-lowered.
   - **`xsd:integer` maps to a custom `Long` scalar, so most Fluree integers surface as `Long`, not
     `Int`.** Fluree stores a plain JSON integer as `xsd:integer`, which is unbounded; GraphQL's
     `Int` is 32-bit and would fail at read time on a large value. Worth knowing, since it is what
     every generated schema will show.
5. ⚠️ **API** (`fluree-db-api/src/graphql.rs`) — done except caching. `Fluree::graphql(db, request)`
   returns the GraphQL envelope (a GraphQL error is a response body, not a transport failure);
   `schema_model` / `schema_sdl` derive the schema, applying policy by pruning, reusing
   `class_denied` / `predicate_denied` (now `pub(crate)` in `cypher_procedures.rs`). Stats come
   through `assemble_fast_stats_with` with `NoveltyMerge::Reconciled` under the new
   `stats_merge_site::GRAPHQL_SCHEMA`. Behind a `graphql` feature (in `full`), since `async-graphql`
   is a heavy dependency.
   **Caching** (done): `derive_schema` is served from a process-wide LRU keyed on
   `(ledger, index t, as-of t, overlay content version, default-context hash)`. Two cases decline
   and derive afresh: an overlay with no `content_version` makes no uniqueness guarantee, and a
   policy-bearing view prunes by identity — reducing an enforcer to a comparable fingerprint is not
   something this module can do correctly, and getting it wrong would leak one identity's schema to
   another. The as-of time is in the key because the novelty half of the stats merge honours it; the
   indexed half cannot (`IndexStats` has no historical form), so a time-traveled schema is a
   superset.
6. ✅ **Server** (`routes/graphql.rs`): `POST /v1/fluree/graphql/*ledger` accepting
   `application/json` `{query, variables, operationName}` and `application/graphql`, plus `GET` with
   `?query=` for GraphiQL. Identity and policy come from headers, as for SPARQL and Cypher. Errors
   ride the GraphQL envelope with a **200**, since every standard client reads `errors` from the
   body and a 4xx for an unknown field would break them.
   SDL is `GET /v1/fluree/graphql-schema/*ledger` — a separate path, not a `/schema` suffix: the
   ledger tail is greedy and ledger names may contain `/`.
7. ✅ **CLI**: `fluree graphql [ledger] '<document>'`, `-e`/`-f`/stdin, `--variables`,
   `--operation`, and `--schema` for the SDL dump. A response carrying `errors` exits non-zero.
   Docs at `docs/cli/graphql.md`.
8. ⚠️ **Tests** — the differential and end-to-end layers exist; a golden-SDL snapshot does not.
   `fluree-db-graphql/tests/it_lowering.rs` asserts the lowered **document** equals the JSON-LD
   query a user would have written by hand (17 tests); `it_inferred_schema.rs` covers the tier-1
   mapping; `fluree-db-api/tests/it_graphql.rs` runs 9 queries end to end against a seeded ledger.
   **Still to do:** a golden SDL snapshot for a fixture ledger, and an introspection round-trip
   through a real client library.

Acceptance: point GraphiQL at any existing ledger, autocomplete works, filters/order/pagination work,
policy-restricted classes disappear from introspection. **Met**, with the caveats above: `orderBy`
takes only `id` until tier 2 supplies cardinality, and nested field arguments are refused.

One thing found on the way that was not GraphQL's: `docs/query/jsonld-query.md` documented `values`
with the wrong arity (`["values", "?x", [...]]` rather than `["values", ["?x", [...]]]`). Corrected,
with the multi-variable form added; both forms verified against the engine.

### Phase 2 — Tier 2 (shape overlay) — done

1. ✅ **SHACL compiler additions**: `sh:description`, `sh:order` and `sh:defaultValue` are compiled
   onto `PropertyShape` (and `sh:description` onto `CompiledShape`). They constrain nothing —
   validation never reads them — but they are what a schema generator has to work with.
   `sh:defaultValue` is carried and deliberately never materialized: a default is a statement about
   presentation, not about what the graph holds, and inventing the triple would make validation
   self-fulfilling.
2. ✅ **Read-path shape registry** — but a process-wide cache in `fluree-db-api::graphql`, not the
   `LedgerState` slot. A read path has a `GraphDb`, not a `LedgerState`, so the write path's
   type-erased slot is unreachable from here. The key is `(ledger, index t, shacl_epoch)` — the same
   epoch the write path keys its own compile cache on, and exactly "a shape-affecting flake was
   committed". Shapes that fail to compile fall back to the inferred schema with a warning rather
   than failing the request: a ledger whose shapes are broken still has a schema.
3. ✅ **Shaped builder** — but the two tiers build in **one pass** (`schema/build.rs`), not as an
   overlay. Naming is global: names are allocated once over the union of observed and shaped
   classes, so a shaped class and an inferred one cannot both claim `Person` and then need
   reconciling. `inferred::build` is now a thin wrapper for the shapes-empty case.
4. ✅ Bootstrap — as `fluree graphql --bootstrap`, not `fluree model bootstrap`. It sits next to
   `--schema` because the two are halves of one workflow: `--schema` shows what you have,
   `--bootstrap` gives you the SHACL to sharpen it. Output is deliberately weak (paths and value
   types; no cardinalities, no `sh:closed`, no `sh:in`) and is **printed, never transacted** —
   shapes activate SHACL validation for their class, so applying them is the author's decision after
   editing. Round-trip verified: bootstrap → edit → `fluree insert` → the schema sharpens.
5. ✅ Tests: 14 builder tests in `fluree-db-graphql/tests/it_shaped_schema.rs`, 6 end-to-end in
   `fluree-db-api/tests/it_graphql_shapes.rs` (cardinality, documentation, `sh:closed`, enums
   queried *and* filtered by member name, an inverse path traversed, a shaped class with no
   instances, and shape-edit invalidation).

What tier 2 actually contributes, and the decisions behind it:

- **Cardinality**: `sh:maxCount 1` → a single value, `sh:minCount ≥ 1` → `!`. This is the only way a
  field escapes tier 1's nullable list, and it is why the tiers exist.
- **A shaped class appears before its first instance.** A shape describes what the ledger is *meant*
  to hold; waiting for an instance would mean a client cannot see the schema it is writing against.
- **`sh:closed` drops observed-but-undeclared properties**; an open shape keeps them as inferred
  nullable lists, so a partial shape is genuinely partial rather than a whitelist.
- **A partial property shape falls back to the observed type.** Declaring `sh:maxCount` without
  `sh:datatype` gets the cardinality from the shape and the type from the data.
- **`sh:in` becomes an enum** when every member yields a usable name and they are all IRIs or all
  literals. A mixed set, an unusable name, or two members that sanitize alike keeps the datatype and
  records a warning — a silently shrunken domain would be worse than no enum. The underlying value
  travels with the member, so a filter written as `status: { EQ: Retired }` lowers to the IRI.
- **`sh:inversePath` becomes a reverse field**, and the lowered query gets a generated
  `{"@reverse": <iri>}` context term for it. Without the term a reverse selection comes back under
  the plain predicate IRI — the same key a forward selection uses — so an edge read in both
  directions would collide. Reverse fields take no nested arguments: the hydration IR carries
  per-value modifiers on forward properties only, and advertising arguments that then error would be
  worse than not offering them.
- **Only a single predicate, forwards or backwards, becomes a field.** A sequence or alternative
  path names no one predicate to read or write, so those property shapes are skipped.
- **Declared names are allocated before derived ones**, in two passes. A single pass let a name
  derived from an IRI take a `sh:name` that a later shape had declared — which class won depended on
  IRI sort order. Only a declaration can now lose to another declaration, and that is reported.
- Schema derivation is **async** throughout, because shape compilation reads the index. Blocking a
  runtime worker on it would risk starving the executor those reads need.

### Phase 3 — Tier 3 (curated schema) + mutations — done

1. ✅ **Vocabulary** in `fluree-vocab::graphql`: the `datashapes.org/graphql#` terms
   (`Schema`, `publicShape`/`protectedShape`/`privateShape`, `name`, `isInterface`, `isIDField`),
   plus three Fluree extensions under `https://ns.flur.ee/db#` — `graphqlPluralName`,
   `graphqlEnableMutations`, `graphqlIriBase`. The shared vocabulary defines no equivalent for
   those, and minting terms in a namespace we do not own would make shapes only Fluree can read
   look portable.

   ⚠️ **`is_shacl_affecting_flake` was deliberately not touched.** It keys on a registered
   namespace *code*, and `graphql:` has none — adding one risks colliding with the dynamic code an
   existing ledger already allocated for that IRI, which would silently mis-key its stored flakes.
   No change was needed anyway: the derivation cache keys on the overlay's content version, which
   any write moves, so editing a curated schema invalidates it. (Pinned by
   `editing_the_curated_schema_reshapes_the_endpoint`.)
2. ✅ **Curated builder** — a filter and an overlay over the same one-pass build, not a third source
   of types. It decides which classes are published and how they are named; a type's *shape* still
   comes from its SHACL shape and the data.
   - **Exposure**: `publicShape` → a type plus root fields; `protectedShape` → a type reachable only
     through a reference; `privateShape` (and any unlisted class) → absent, with references to it
     degrading to `Node` so the edge stays visible as an IRI without naming an unqueryable type.
   - **`graphql:isInterface`** makes a class an *interface* rather than an object: nothing is a
     direct instance of an abstract class, so a concrete type would only ever be empty. Implementors
     come from the RDFS hierarchy — and from a **novelty-aware** one, since
     `LedgerSnapshot::schema_hierarchy` reflects the last index build only and would report no
     implementors on an unindexed ledger. A class missing one of the interface's fields does not
     claim it: GraphQL requires implementors to declare every field, and a schema that will not
     register is worse than one missing edge. (The same fix now feeds SHACL compilation, so subclass
     targeting also sees edges written since the last index build; `schema_epoch` joined that
     cache's key as a result.)
   - **Several `graphql:Schema` instances fall back to tier 2** rather than guessing which to serve.
     Multi-schema routing (`/{ledger}/{schemaName}`) is a follow-up.
   - `graphql:name` beats `sh:name`; `f:graphqlPluralName` beats the naive pluralisation.
3. ✅ **Mutations** (`mutate/`) — `create_<T>(input:)`, `update_<T>(ids:, set:)`,
   `delete_<T>(ids:)`, each lowering to an ordinary JSON-LD transaction, so SHACL, policy and commit
   semantics apply unchanged. Decisions worth knowing:
   - **Off unless `f:graphqlEnableMutations` says otherwise**, and then only in tier 3. A schema
     derived from whatever a ledger happens to contain must never become a write surface by accident.
   - **The read entry point cannot write, structurally**: writing needs a `LedgerState`, which a
     read view does not carry. `Fluree::graphql_transact` is the write path, and mutation fields are
     registered *only* there — so the SDL a read endpoint serves matches what it can answer.
   - **`f:graphqlIriBase` is required to mint an IRI**, with no default: a wrong guess writes
     identifiers that cannot be un-minted. An explicit `id` works without one.
   - **`update` lowers to `where`/`delete`/`insert`, not upsert.** A property set to `null` must be
     retracted with nothing put back, which an upsert cannot express (a node with only `@id` is not
     valid), and this keeps every property and subject in one atomic transaction. Both `update` and
     `delete` anchor on `@type`, so `delete_Person` on a Company's IRI is a no-op rather than a wipe.
   - **A refused write leaves the caller where it started.** The write APIs consume the ledger and a
     rejection does not hand it back; `LedgerState` is `Arc`-backed, so a copy is kept and restored.
   - **References are written as the target's `id`, never as a nested node** — creating a subject as
     a side effect of linking to it would write an object the caller never named. Reverse fields and
     abstract-typed fields are not writable at all.
   - **Every input field is nullable** regardless of the output's `!`, or a partial `update` would
     be inexpressible. `id` cannot be changed: renaming a subject is a create and a delete.
   - Mutations run **serially** (as the spec requires), each committing before the next, and the
     result is read back through the ordinary query path — so what a mutation returns is what a
     query would have returned.
   - ⬜ `::n` intra-mutation references are **not** implemented; link by `id` across two mutations.
4. ✅ Tests: 8 curated-builder + 10 mutation end-to-end + 6 curated end-to-end + 4 HTTP, covering
   the exposure matrix, interfaces, `graphql:name`, ambiguity fallback, invalidation, IRI minting,
   null-clearing, type-scoped deletes, serial execution, and a SHACL violation surfacing as a
   GraphQL error that writes nothing.

### Phase 4 — Polish — done

- ✅ **`explain`**: `?explain=true`, `extensions: {"explain": true}` in the request body, or
  `--explain` on the CLI, returns `extensions.explain` with the tier, the model's warnings, and per
  root field the JSON-LD query it lowered to (or, for a mutation, the transaction that was
  committed). It reports what **ran** — a mutation still writes. Silently not writing when the
  caller asked to see the plan would be the more surprising behaviour, so that is documented rather
  than hidden.
- ✅ **Language selection**: a field whose values carry language tags takes `lang:`.
  `"en,fr"` is a preference list — the English values if there are any, else the French ones, not
  both — and `"*"` means every value whatever its tag, which is also the default. The tag is not
  returned; the field is declared `String`.

  This uncovered a real bug: a langString field was returning
  `{"@value": …, "@language": …}` objects even with no `lang` argument, because that is how the
  hydration renders a tagged literal — so the declared `String` type was a lie. Language-tagged
  fields now always unwrap.
- ⬜ **Persisted queries / `@cacheControl`** — deliberately not done. `@cacheControl` is an Apollo
  *server* schema directive, not something clients send, and registering it as a no-op would mean
  accepting a caching hint we ignore. Persisted queries are a protocol needing a real store. Neither
  is a shim worth shipping; revisit if a client actually needs them.
- ✅ **Docs**: `docs/query/graphql.md` (the reference), `docs/cli/graphql.md` (the CLI, with the full
  SHACL and curation tables), and a *GraphQL schema vocabulary* section in
  `docs/reference/vocabulary.md`. Both are in `SUMMARY.md`; the CLI page satisfies the
  `docs_coverage` gate.
- ✅ **Benchmarks** (`fluree-db-api/benches/graphql_schema.rs`), which found a real problem and
  measured the fix:

  | | before | after |
  |---|---|---|
  | derivation, warm | — | ~256 ns |
  | derivation, cold | — | ~250 µs (10 classes) |
  | registration | ~274 µs (10 classes), ~2.5 ms (100) | cached |
  | one query, GraphQL | ~332 µs | **~84 µs** |
  | the same query, hand-written JSON-LD | ~39 µs | ~39 µs |

  **Registering the executable schema per request was the entire GraphQL overhead.** The Phase-1
  note that caching it was "a second-order win" was wrong: at a hundred classes it is 2.5 ms on
  every request. The fix was to make the resolvers executor-agnostic — they read the per-request
  executor from the request's `data` instead of capturing it — so one registration serves every
  request against a ledger version. Keyed on the derivation's key plus whether mutations are
  registered, since a read schema deliberately has no `Mutation` type.

## Follow-ups

Each of these was reached and deliberately left, not overlooked.

- **`where` on a nested field.** Filtering a nested level means evaluating a predicate over values
  the hydration has already materialized — a different engine from the one that answers the WHERE
  clause. Refused with a reason that points at filtering from a root field instead.
- **Nested arguments on a reverse field.** `ForwardItem::Property` carries the per-value modifiers;
  a reverse selection is `HashMap<Sid, Option<Box<NestedSelectSpec>>>` with nowhere to put them.
  The schema does not offer the arguments rather than offering and refusing them.
- **Several `graphql:Schema` instances in one ledger.** Falls back to the shaped schema with a
  warning. Routing them as `/{ledger}/{schemaName}` is the intended shape.
- **`::n` intra-mutation references.** Link by `id` across two mutations.
- **Ordering a root list by a multi-valued field**, which would need the engine to sort solutions by
  an aggregate. Not expressible in JSON-LD `orderBy` today.
- **`NEQ` / `NIN` / ordering on IRI-valued fields.** The S-expression filter language has no IRI
  atom, so those lower to `values` patterns, which express membership but not exclusion.
- **Dataset / multi-graph queries**: tier 1 unions per-graph class stats; a `graph:` root argument
  is a follow-up.
- **Subscriptions and federation** — see Non-goals.
- **Fuel accounting.** The lowered query carries no `opts.maxFuel`, so
  `tracker_for_limits` runs it untracked; the timeout is the only ceiling on a
  single query's cost. Exposing a fuel budget on the GraphQL surface would give
  a second, work-proportional bound.

Two open questions from the original plan, now answered:

- *Cypher-imported ledgers use namespace-0 bare names — do they collide with the `Node` built-in?*
  No: `Node` is claimed in the type scope before any class is assigned, so a class named `Node`
  gets the collision treatment like any other.
- *Should a wildcard-less `depth` be allowed?* Moot, as suspected: `{ person { } }` is invalid
  GraphQL, so there is no way to ask.

## Non-goals

- GraphQL subscriptions (SSE exists separately; revisit after Phase 3).
- Federation (`@key` / `_entities`) — plausible later since every type has `id`.
- Reasoning-driven schema (owl restrictions); tier 2 shapes are the explicit route.
