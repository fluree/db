# Residual 1.1 Eval Mismatches — Deep Audit (burn-down)

**Scope:** the residual SPARQL 1.1 query-evaluation failures assigned to this
cluster — CONSTRUCT (`constructwhere04`, `constructlist`), SUBQUERY
(`subquery02/04/12`), EXISTS (`exists03`), PROJECT_EXPRESSION (`projexp05`),
AGGREGATES (`agg02`, `agg-err-01`, `agg-empty-group-count-graph`,
`agg-count-rows-distinct`), PROPERTY_PATH (`pp16`, `pp34`, `pp35`, `pp36`), and
the ENTAILMENT framing. Companion to
`docs/audit/2026-07-sparql-testsuite-audit.md` (§4.2, §6). No source was
modified producing this document; every root cause below was verified by reading
engine code and/or reproduced against a live in-memory ledger via the CLI.

Baseline: branch `test/sparql-testsuite-full-coverage`, rdf-tests submodule
`efccbc6b8`.

## 1. Executive summary

The 15 residual tests decompose into **four** root-cause families, not the
"per-category" grouping the register implies. The single most important finding
is a **reframing of the property-path work**: the audit (§6 item 3) frames
`pp16/pp34/pp35/pp36` as "path multiplicity semantics" needing a "plan-time
bag-vs-set selection." That is not what these tests need. Sequence-path
multiplicity is **already correct and already regression-guarded**
(`fluree-db-api/tests/it_query_seq_path_count_repro.rs`); no plan-time
bag/set switch is required, and the `*`/`+` distinct fast path is **untouched**
by any fix here. The real property-path defects are (a) an incomplete
zero-length node set (`pp16`) and (b) an empty-schema batch that drops the unit
row (`pp36`); `pp34/pp35` are not property-path bugs at all — they are blocked by
named-graph IRI resolution and belong to the graph cluster.

Family map (detail in §3):

| Family | Tests | Owner PR |
|---|---|---|
| **A. Named-graph resolution / GRAPH-var semantics** | pp34, pp35, exists03, subquery02, subquery04, agg-empty-group-count-graph | Graph PR (audit C4) — shared with the graph cluster |
| **B. Property-path operator** | pp16, pp36 | Property-path PR (audit C3) |
| **C. Expression / aggregate semantics** | projexp05, agg02, agg-err-01, agg-count-rows-distinct, subquery12 | Expression-semantics PR (audit C1/C2) |
| **D. Parser / dataset gaps** | constructlist, constructwhere04 | constructlist → syntax PR (B3); constructwhere04 → dataset PR (C4/§5.3) |

Of the 15, **6 are already latent passes** waiting only on the graph fix
(Family A), **2 are the genuine property-path centerpiece** (Family B), **5 are
expression/aggregate fixes** (Family C), and **2 are parser/dataset** (Family D).

---

## 2. The property-path reframing (centerpiece)

The team brief asked for a "plan-time selection of bag-semantics path counting"
that "preserves the `*`/`+` distinct fast path." I verified this is **not
needed** for any test in scope, and documenting why is the most load-bearing
result here.

### 2.1 Sequence multiplicity is already correct

A sequence path `?s a/b+ ?o` lowers to a **BGP chain joined by fresh
path-internal variables** — not to a single distinct-pair path operator.
`fluree-db-sparql/src/lower/path.rs:362-426` (`lower_sequence_chain`) emits
`Triple(?s, a, ?__pp0)` + `PropertyPath(?__pp0, b+, ?o)`, i.e. the intermediate
`?__pp0` is a real join variable. Per SPARQL 1.1 §18.2.2.4 the join over that
variable **preserves multiplicity**: an `?o` reachable through two intermediates
is counted twice. This is exactly the spec-correct "bind-`?mid`" cardinality.

This is not theoretical — it is pinned by a dedicated regression test:
`fluree-db-api/tests/it_query_seq_path_count_repro.rs` asserts `COUNT(*)` equals
the bind-`?mid` join cardinality (e.g. `ex:s ex:p1/ex:p2+ ?o` = 4, not the
distinct-pair 3) on **both** the in-memory generic operator and the indexed
fast path (the file documents "Defect 1"/"Defect 2" that were already fixed).

**Consequence:** there is no bag-vs-set decision to make at plan time. The
`*`/`+` transitive operator correctly emits **distinct nodes** per source
(`fluree-db-query/src/property_path.rs`, BFS with a `visited` set), and sequence
multiplicity comes from the surrounding BGP join, already in place. None of the
fixes below alters this operator's traversal or its distinct semantics, so there
is **zero perf change for `*`/`+`** and no new plan-time branch.

### 2.2 What pp34/pp35 actually are

`pp34` = `GRAPH <ng-01.ttl> { ?s :p1* ?t }`; `pp35` = `GRAPH ?g { ?s :p1* ?t }
FILTER(?g = <ng-01.ttl>)`. Both currently return **0 rows** (expected `[a,b,b]`).
The named graph `ng-01.ttl` **is** loaded (via `qt:graphData`), so the `[a,b,b]`
result is producible today *if the GRAPH block matched*: within `ng-01`
(`:a :p1 :b`) the closure emits the distinct pairs `(a,a),(b,b),(a,b)`, and
projecting `?t` (no `DISTINCT`) preserves the bag → `[a,b,b]`. The multiplicity
is therefore **already correct**; the blocker is that `GRAPH <ng-01.ttl>` never
resolves to the loaded graph (Family A, §3.1). These are graph-cluster tests
mis-filed under property-path. After the graph fix they should pass with no
property-path change — a claim to **verify** post-fix, not assume.

---

## 3. Per-test root causes (with evidence)

### 3.1 Family A — named-graph resolution / GRAPH-var semantics

All five share one or more of three engine defects in
`fluree-db-query/src/graph.rs` (confirmed by code read). These are the **graph
cluster's** fixes; listed here because these tests can't pass without them and
because the audit filed them under my categories.

**Defect A1 — constant graph IRI never matches the loaded graph.**
Lowering base-expands `GRAPH <ng-01.ttl>` by naive string concat
(`fluree-db-sparql/src/lower/term.rs:416-420`, via `lower/pattern.rs:232`) into
`IrGraphName::Iri("{base}ng-01.ttl")`. Runtime does an **exact** `HashMap`
key match (`graph.rs:587`/`633` → `dataset.rs:218-219`
`named_graphs.contains_key`, no IRI normalization). The loader registers the
named graph under the file URL from `qt:graphData`; the query's base-expanded
string differs, so the lookup misses and the block silently returns empty
(`graph.rs:597`).
→ **Blocks pp34, pp35, exists03** (all use `GRAPH <relative.ttl>`).

**Defect A2 — GRAPH graph-variable bound as a plain literal, not an IRI.**
`GraphOperator::execute_in_graph` binds `?g` as
`Binding::Lit { val: String(graph_iri), dtc: Explicit(xsd:string) }`
(`graph.rs:314-323`; the wrong behavior is even codified in the module doc at
`graph.rs:10`). Per SPARQL `?g` must be an IRI term.
→ Contributes to **agg-empty-group-count-graph** (result shows
`g: Literal("…singleton.ttl")` instead of `Iri(...)`).

**Defect A3 — default graph leaks into `GRAPH ?g` enumeration (single-db).**
The unbound-`?g` fan-out appends the ledger alias (== default graph) to the
enumerated set (`graph.rs:700-709`); dataset mode is correct (`graph.rs:675-685`
iterates only `named_graph_iris()`).
→ **Blocks subquery04** ("default graph does not apply"): the extra solution
`x=instance#no` comes from `sq04.rdf`, which is loaded into the **default**
graph and must not appear under `GRAPH ?g`.

**Defect A4 — GRAPH graph-var not correlated with a subquery projecting the
same variable.** For `GRAPH ?g { { SELECT * WHERE { ?x ?p ?g } } }` the inner
subquery is seeded from the parent row only, which does **not** contain `?g`
(`graph.rs:258-266`); `SubqueryOperator` therefore computes empty
`correlation_vars` for `?g` (`subquery.rs:112-124`) and runs uncorrelated, and
`GraphOperator` then **overwrites** `?g` with the graph IRI instead of joining
on it (`graph.rs:312-323`). Result: every triple in the graph survives with `?g`
stamped over it.
→ **Blocks subquery02**: expected 1 row (`x=c`, the only triple whose object
equals the graph name); Fluree returns 2 (`x=a` and `x=c`) because the
`?g == graph-name` join is never applied.

**exists03 — additional sub-check.** Beyond A1, `exists03` specifically tests
that `FILTER EXISTS { ?s ?p ex:o2 }` is evaluated **within** the enclosing named
graph (`mf:name` = "Exists within graph pattern"). Even after A1 is fixed, the
EXISTS sub-plan must inherit the active named graph, not fall back to the default
graph. This is a distinct correctness point to verify once GRAPH resolution
lands; it is not separately reproducible until then.

**agg-empty-group-count-graph** additionally needs empty named graphs to be
enumerable: expected `(empty.ttl, 0)` requires the empty named graph to yield a
`count=0` row, but Fluree does not track empty named graphs as first-class
graph-store entries (noted in `query_handler.rs:441-446`). This is a
graph-model limitation on top of A2/A3; likely deferred with a register entry
even after the core graph fix.

### 3.2 Family B — property-path operator (the genuine fixes)

**pp16 — zero-length node set is incomplete.** Query `?X foaf:knows* ?Y`;
expected 15, got 13. Missing exactly the two reflexive pairs `(h,h)` and
`("test","test")`. Root cause: the both-variables `*`/`?` closure builds its node
set **only from edges of the path predicate** and **only from ref objects**:

```
// fluree-db-query/src/property_path.rs:616-623 (compute_closure)
let mut ingest = |flake| {
    if let FlakeValue::Ref(o) = flake.o {      // literal objects skipped
        nodes.insert(flake.s); nodes.insert(o);
        adj.entry(flake.s).or_default().push(o);
    }
};
// ...scan is restricted to self.pattern.predicates (foaf:knows) — :644-660
```

The reflexive pairs are emitted per node in `nodes` (`property_path.rs:733-735`).
So `h` (object of `foaf:homepage`, a different predicate) and the literal
`"test"` (object of `foaf:name`) never enter `nodes`, and their `(n,n)` pairs are
lost. Per SPARQL 1.1 §18/§9.3, a zero-length path with both endpoints variable
ranges over **every term in subject or object position of the active graph**,
regardless of predicate, **including literals**. This is a distinct-node
completeness bug, **not** a multiplicity bug.

**pp36 — matching unit row dropped by empty-schema batch.** Query
`:a0 (:p)* :a1` (both endpoints constant); expected 1 empty solution (ASK-true),
got 0. The both-constants arm correctly computes reachability and stores a dummy
pair to signal "1 row" (`property_path.rs:954-968`), but `next_batch` then
materializes columns from the **empty** `in_schema` and calls `Batch::new`,
which infers `len` from the (absent) first column and collapses to `len = 0`:

```
// property_path.rs:1184-1213 (unseeded) and :1259-1272 (correlated)
let columns = self.in_schema.iter().map(...).collect(); // [] — zero columns
let batch = Batch::new(self.in_schema.clone(), columns)?; // empty schema => len 0
```

`Batch::new`'s len-loss on empty schema is a pinned invariant
(`fluree-db-query/src/binding.rs`, test `test_batch_new_loses_len_for_empty_schema`).
The correct idiom already exists and is used by sibling correlated operators:
`GraphOperator::drain_buffer` (`graph.rs:482-489`) and
`SubqueryOperator::drain_buffer` (`subquery.rs:488-495`) guard `num_cols == 0`
and return `Batch::empty_schema_with_len(n)` (`binding.rs:1265`). The
property-path operator lacks that guard. This is the "zero-variable `SELECT *`
projection" symptom in the brief, but the drop happens **inside the path
operator**, not in projection — projection of a wildcard with zero vars is a
no-op (`ir/projection.rs:230-232`; `execute/operator_tree.rs:3351-3361` builds no
`ProjectOperator`) and would preserve a `len=1` empty-schema batch if one reached
it.

### 3.3 Family C — expression / aggregate semantics

**projexp05 — `DATATYPE()` on an IRI returns `@id` instead of erroring.**
Query `SELECT ?x ?l (datatype(?l) AS ?dt)` over data where `?l` binds to both an
integer and an IRI. Expected: for the IRI row, `?dt` **unbound** (type error).
Actual (reproduced): `?dt = @id`. Source: `eval_datatype`'s deliberate Fluree
extension for IRI/ref arguments:

```
// fluree-db-query/src/eval/rdf.rs:89-92
// Fluree extension: DATATYPE of an IRI/ref reports the `@id` ref type.
Binding::Sid { .. } | Binding::IriMatch { .. } | Binding::Iri(_)
    => Ok(Some(ComparableValue::Sid(WELL_KNOWN_DATATYPES.id_type.clone()))),
```

Returning `Ok(Some(@id))` binds `?dt` in the project-expression; the spec
requires a type error (unbound). The very next arm (`rdf.rs:94-96`) already
raises `InvalidExpression` for other non-literals — the IRI case was carved out
above it. **Reproduced** via CLI: `datatype(<iri>)` → `{"@id":"@id"}`;
`datatype(1)` → `xsd:integer`.

> **This is a deliberate divergence, not an accident.** Fixing it is a
> SPARQL-conformance vs Fluree-extension decision (see §6 blast radius): the
> `@id` return is used by JSON-LD-surface queries. The fix must be
> **SPARQL-mode-scoped** (error only under SPARQL semantics) or the extension
> must be dropped and documented.

**agg02 — grouped `COUNT(?var)` is typed `xsd:long`, must be `xsd:integer`.**
Query `SELECT ?P (COUNT(?O) AS ?C) … GROUP BY ?P`. The SPARQL-JSON formatter is
datatype-driven (`fluree-db-api/src/format/sparql.rs:333-335` decodes
`dtc.datatype()`), so the binding's `dtc` really is `xsd:long`. **Isolated by
reproduction** (CLI, in-memory ledger):

| query | datatype |
|---|---|
| `SELECT (COUNT(*) AS ?C) …` (scalar) | `xsd:integer` ✓ |
| `SELECT ?P (COUNT(*) AS ?C) … GROUP BY ?P` (streaming) | `xsd:integer` ✓ |
| `SELECT ?P (COUNT(?O) AS ?C) … GROUP BY ?P` (streaming) | **`xsd:long`** ✗ |
| same + a non-streamable agg → non-streaming path | `xsd:integer` ✓ |

The plan for the failing case is `ProjectOperator > GroupAggregateOperator`
(streaming). Every *visible* count-finalization site specifies `xsd:integer`
(`group_aggregate.rs:340` `AggState::Count`; `aggregate.rs:715`; the streaming
`CountAll` pushdown `group_aggregate.rs:797`), and the projection layer does not
re-type (`project.rs` has no datatype logic). The only production `xsd:long`
count site is the indexed top-K path (`fast_group_count_firsts.rs:1548/1588`),
which this query cannot reach (it needs `LIMIT` + `ORDER BY DESC`,
`operator_tree.rs:735-744`). **The defect is therefore localized to the streaming
`GroupAggregateOperator` variable-count path (`input_col = Some`) —**
`COUNT(*)` (input_col `None`) is correct through the same operator, and the
non-streaming operator is correct for `COUNT(?var)`. Fix target:
`fluree-db-query/src/group_aggregate.rs` streaming Count-of-variable
finalization must emit `xsd:integer` to match the other three paths. (The
finalize at `:340` reads `xsd_integer`; because the reproduction contradicts a
purely static read, the implementer should confirm the exact re-typing site with
a one-line probe before patching — the observable contract and the trivial fix
are unambiguous.)

**agg-err-01 — numeric aggregate over a non-numeric member must error → unbound.**
Query groups `?g :p ?p` and computes `AVG(?p)` / `(MIN+MAX)/2`. Group `#y`
contains a blank node among the numbers; expected: `?avg`/`?c` **unbound** for
`#y`. Actual: `?avg = 2.666…` (computed over the numeric members only). Source:
the numeric aggregates **skip** non-numerics rather than erroring —
`aggregate.rs:737-742` (SUM) and `:750-755` (AVG) iterate
`values.iter().filter_map(binding_to_numeric)`, and `binding_to_numeric` returns
`None` for a blank node / IRI / string (`aggregate.rs:620-640`, final `_ => None`).
The module doc even states the behavior: `aggregate.rs:13` "Numeric aggregates …
skip non-numeric values." MIN/MAX likewise never error (they compare by term
order, `aggregate.rs:759-786`). SPARQL 1.1 §18.5: a type error in an aggregated
expression makes the aggregate an error → the variable is unbound. The streaming
path mirrors the same skip (`group_aggregate.rs:267-273`).

**agg-count-rows-distinct — `COUNT(DISTINCT *)` lowering not implemented.**
Explicit bail: `fluree-db-sparql/src/lower/aggregate.rs:232`
(`return Err(LowerError::not_implemented("COUNT(DISTINCT *)", …))`) in the
`(Count, None)` arm — `input_var` is `None` iff the argument was `*`. Plain
`COUNT(*)` and `COUNT(DISTINCT ?v)` both work (`aggregate.rs:228-249`).
`COUNT(DISTINCT *)` counts distinct whole solution rows, so it needs a new IR
aggregate that dedups over the full in-scope tuple (not a single column).

**subquery12 — CONSTRUCT over a sub-SELECT with `CONCAT` yields 0 triples.**
Query: `CONSTRUCT { ?P foaf:name ?FullName } WHERE { SELECT ?P (CONCAT(?F," ",?L)
AS ?FullName) WHERE { ?P foaf:firstName ?F ; foaf:lastName ?L } }`. Expected 1
triple, got 0. No GRAPH involved — this is a CONSTRUCT-template + subquery-alias
interaction: the outer template references `?FullName`, which is produced only by
the inner sub-SELECT's project-expression. The likely cause is that the sub-SELECT
alias `?FullName` is not visible to / not joined into the CONSTRUCT template
binding (analogous to A4's subquery-projection scoping, but on the CONSTRUCT
path). This one I could not fully isolate by static read; **flag for a focused
repro** during the expression-semantics PR (build the two-column sub-SELECT, dump
the pre-template solution to confirm `?FullName` is bound before template
instantiation).

### 3.4 Family D — parser / dataset gaps

**constructlist — RDF collection `( … )` syntax unsupported.** Query
`CONSTRUCT { (?s ?o) :prop ?p } WHERE { ?s ?p ?o }`. The `(?s ?o)` collection in
subject position is rejected at parse time:
`fluree-db-sparql/src/parse/query/term.rs:87-94` (subject) and `:220-227`
(object) emit "RDF collection (list) syntax is not yet supported" and
`skip_collection` (`term.rs:483-500`). The tokens (`LParen`/`Nil`) are
recognized but there is no expansion to `rdf:first`/`rdf:rest`/`rdf:nil`. Affects
both CONSTRUCT templates and WHERE clauses.

**constructwhere04 — `FROM` on a single-ledger GraphDb.** Query
`CONSTRUCT FROM <data.ttl> WHERE { ?s ?p ?o }`. Fails with "SPARQL FROM/FROM NAMED
clauses are not supported on a single-ledger GraphDb." This is the dataset
(`FROM`/`FROM NAMED`) gap (audit §5.3); folds into the dataset PR, not a
CONSTRUCT-specific fix.

---

## 4. Hot-path classification per fix (perf-safety, audit §6)

| Fix | Phase | Per-row hot path? | Perf note |
|---|---|---|---|
| A1 graph-name resolution | prepare (lowering + lookup) | No | Off hot path; string/normalization at plan/lookup time |
| A2 GRAPH-var IRI binding | per-graph (once per enumerated graph) | No | Constant work per graph, not per row |
| A3 default-graph exclusion | per-graph enumeration | No | Removes one graph from a set; strictly cheaper |
| A4 subquery↔GRAPH-var join | prepare (seed schema) + per-row join | Correlated join only | Adds `?g` to the subquery seed; join already exists for other correlated vars |
| **B/pp16 zero-length node set** | per-query (both-var `*`/`?` closure) | No (not `+`, not bound-endpoint) | Adds a full-graph node scan **only** for both-variable `*`/`?`, already the guarded expensive path (`DEFAULT_MAX_VISITED`, `property_path.rs:49`). `*`/`+`/`?` with a bound endpoint and all `+` closures are byte-identical. |
| **B/pp36 empty-schema batch guard** | operator emit | No | One `num_cols == 0` branch at batch build; mirrors existing `graph.rs:482`/`subquery.rs:488` |
| C/projexp05 DATATYPE | per-row (FILTER/expr eval) | Only the IRI-arg arm | Changes one match arm in a builtin; the literal fast path is unchanged |
| C/agg02 COUNT datatype | per-group finalize | No | Result-literal datatype tag; once per group |
| C/agg-err-01 numeric-agg error | per-row accumulation | Only the non-numeric arm | Replace "skip" with "poison the accumulator"; numeric inputs unchanged |
| C/agg-count-rows-distinct | lowering + per-row dedup | New aggregate only | No impact on existing aggregates |
| D/constructlist | parse time | No | Off hot path entirely |
| D/constructwhere04 | prepare (dataset) | No | Dataset construction, once per query |

**Bench guardrails.** No existing bench exercises property-path closures — the
hot benches (`query_hot_bsbm`, `query_hot_bsbm_bi`, `insert_formats`,
`import_bulk`, `vector_query`, `fulltext_query` in `regression-budget.json`) do
not touch `PropertyPathOperator`. The pp16/pp36 changes are consequently invisible
to the regression budget and are off the BSBM join/filter hot paths by
construction. **Recommendation:** add a small `query_hot_property_path` bench
(bound-subject `+` closure + a both-variable `*` closure) so the zero-length
node-set change is measured; gate the property-path PR on it and on
`query_hot_bsbm`/`query_hot_bsbm_bi` staying within budget. The expression/aggregate
fixes should gate on `query_hot_bsbm`/`query_hot_bsbm_bi` (FILTER + GROUP-BY hot
paths).

---

## 5. Query-surface parity (SPARQL + JSON-LD)

Per `docs/contributing/sparql-compliance.md` ("Query Surface Parity"), each fix
must be classified and JSON-LD regression coverage authored where the semantics
are expressible. All fixes here are **IR/engine-level** (they change shared
operators/eval, not SPARQL-only syntax), so they fix the JSON-LD surface
implicitly — but nothing guards it unless we write the test.

**Cypher is out of scope** for this burn-down: openCypher owns that grammar and
we add no custom syntax, so parity coverage below is SPARQL + JSON-LD only.

| Fix | JSON-LD expressible? | Regression test to author |
|---|---|---|
| A4 subquery↔GRAPH-var correlation | Yes (JSON-LD sub-selects + `graph`/`from`) | `it_query.rs`: named-graph sub-select whose projected var equals the graph var; assert the join |
| A3 default-graph exclusion | Yes | `it_query.rs`: default + named data, `graph` query must exclude default |
| pp16 zero-length node set | **SPARQL-only** surface (property-path syntax) | Engine fix; no JSON-LD path syntax, so no JSON-LD parity test |
| pp36 zero-var path match | **SPARQL-only** | As above; consider whether JSON-LD should express "path exists between two constants" and record the decision |
| projexp05 DATATYPE | Yes (`datatype()` in JSON-LD) | `it_query.rs`: `datatype(?l)` where `?l` is an IRI → var unbound **under SPARQL mode**; JSON-LD may keep the `@id` extension (record the mode split) |
| agg02 COUNT type | Yes | `it_query_grouping.rs`: grouped `COUNT(?v)` asserts `xsd:integer` (this is the surface that currently regresses — the JSON-LD `count` path shares the operator) |
| agg-err-01 numeric-agg error | Yes | `it_query_grouping.rs`: `avg`/`min`/`max` over a group with a non-numeric member → unbound |
| agg-count-rows-distinct | Yes | `it_query_grouping.rs`: JSON-LD `countDistinct` over `*` / all-vars |
| subquery12 CONSTRUCT-from-subselect | Partial | SPARQL `it_query_sparql.rs`; JSON-LD has no CONSTRUCT — the shared bit is sub-select alias visibility, coverable in `it_query.rs` |
| constructlist | SPARQL-only (RDF collection syntax) | SPARQL only |
| constructwhere04 (dataset) | SPARQL-only (`FROM`) | Dataset PR owns it |

Files: SPARQL → `fluree-db-api/tests/it_query_sparql.rs`; JSON-LD →
`it_query.rs` / `it_query_grouping.rs` / `it_query_analytical.rs`.

---

## 6. Entailment framing verdict + candidate wins

**Verdict on the 20/50 split: confirmed, with a sharper mechanism than "declares
no regime."** The 20 passing tests are answerable by matching **asserted**
triples — no materialized inference required — even when they *declare* an RDFS/D
regime. Verified examples: `rdfs08` (regime `ent:RDFS ent:D`) passes because it
queries `ex:d rdfs:range ?x` and the `rdfs:range` triple is asserted verbatim;
`parent2` passes because it queries the asserted `:hasChild` property. The
`bind0x` group is BIND-driven (no inference). So "simple-entailment-answerable"
is correct; the accurate phrasing is "the expected answer coincides with the
no-reasoning answer."

**The 50 failing genuinely need materialized entailment** (spot-checked 5):
`rdfs04` (subClassOf), `rdfs09` (3-level subClassOf transitivity), `rdfs03`
(subPropertyOf + domain), `parent3` (owl:Restriction someValuesFrom), plus the
RIF/OWL-DL families. Each requires triples that are not asserted.

**Candidate wins — YES, but gated.** Fluree ships an **OWL2-RL forward-chaining
materializer** (`fluree-db-reasoner/`) that already implements the rules these
tests need: **cax-sco** (subClassOf, `execute/class_rules.rs:21`, transitive via
fixpoint), **prp-spo1** (subPropertyOf), **prp-dom** (domain), **prp-rng**
(range), plus cls-\* restriction rules and eq-\* sameAs (`compile.rs:184-339`).
Reasoning is opt-in and can be enabled on the **same** `Fluree::query(&db,
sparql)` path the harness uses — via a `# PRAGMA reasoning: owl2rl` comment in
the query text (`fluree-db-sparql/src/parse/query/mod.rs:86-116`), a
`.with_reasoning` view wrapper, or a ledger-config default. `owl2rl` mode does
query-time **materialization** (a derived-facts overlay,
`reasoning_prep.rs:232-296`), so a generic SELECT sees the inferred triples.

Concrete candidate wins with the **existing** reasoner (no reasoner code):
`rdfs04`, `rdfs09`, `rdfs03`, and — pending per-test check — `rdfs05/06/07`
(subproperty/domain/range family), `rdfs10/11` (subClassOf variants), and the
`parent*` restriction cases (via cls-svf/hv).

**Three hard caveats:**
1. **A harness change is required** — inject `# PRAGMA reasoning: owl2rl` per
   test (or wrap the db) in `query_handler.rs:342-349`, which today does zero
   reasoning. This is harness plumbing, not reasoner code, but it is not
   zero-touch.
2. **Use `owl2rl`, not `rdfs`** — the `rdfs` mode only *rewrites* `rdf:type`/
   subproperty patterns (`rewrite.rs`) and produces nothing for a generic
   `?s ?p ?o` scan; domain/range live only in `owl2rl`.
3. **OWL2-RL ≠ full RDFS entailment** — the materializer does not emit RDFS
   axiomatic-closure triples (reflexive subClassOf, `rdf:type rdfs:range
   rdfs:Class`, container-membership axioms). A test expecting the full RDFS
   closure can still fail with `owl2rl` on, and reasoning **cannot be enabled
   globally** — it would add inferred rows that break some of the 20 currently
   passing. The realistic plan: keep the 50-test register, but pull a **named
   subset** (start with `rdfs04`, `rdfs09`, `rdfs03`) into a small
   reasoning-enabled harness variant that injects the pragma per test, and
   burn them down there. **Not winnable at all:** RIF (`rif01/03/04/06`) and
   full OWL-DL (`paper-sparqldl-*`, `sparqldl-*`) — out of the OWL2-RL profile.

---

## 7. Blast radius + PR mapping

**Graph PR (audit C4) — Family A (pp34, pp35, exists03, subquery02,
subquery04, agg-empty-group-count-graph).** High blast radius: A1–A4 touch every
named-graph query. Owned by the graph cluster; this cluster contributes the
**test-level acceptance criteria** and the two extra sub-checks (EXISTS
active-graph scoping for `exists03`; empty-named-graph enumeration for
`agg-empty-group-count-graph`). After the graph fix, **re-verify** pp34/pp35
yield `[a,b,b]` (multiplicity is already correct, §2.2) and that subquery02
drops the uncorrelated row. Register: move pp34/pp35 out of the property-path
register into the graph register (they are graph tests).

**Property-path PR (audit C3) — Family B (pp16, pp36).** Contained blast radius:
touches only `PropertyPathOperator` (`property_path.rs`) — pp16 the both-variable
`*`/`?` closure node set (`:616-661`, `:733`) and the correlated both-var path;
pp36 the two empty-schema batch builds (`:1212`, `:1271`). No change to `+`, to
bound-endpoint traversal, to sequence lowering, or to the distinct fast path.
Add `query_hot_property_path` and gate on it. **The centerpiece needs no
plan-time bag/set selection** — that framing is retired (§2).

**Expression-semantics PR (audit C1/C2) — Family C (projexp05, agg02,
agg-err-01, agg-count-rows-distinct, subquery12).**
- projexp05: **decision required** — SPARQL-conformant type error vs. Fluree's
  `@id` extension. Recommend a SPARQL-mode-scoped error so the JSON-LD extension
  survives; blast radius otherwise touches every JSON-LD query relying on
  `datatype(<iri>) = @id`.
- agg02: low blast radius — one datatype tag on the streaming variable-count
  path; other three count paths already correct.
- agg-err-01: medium — changes results for any group mixing numeric and
  non-numeric values; poison-on-non-numeric in SUM/AVG (and MIN/MAX
  arithmetic). Confine to numeric aggregates.
- agg-count-rows-distinct: additive (new IR aggregate + lowering); no impact on
  existing aggregates.
- subquery12: needs a focused repro first (§3.3); likely the same
  subquery-alias-visibility class as A4 but on the CONSTRUCT path.

**Syntax PR (audit B3) — constructlist.** Parser feature (RDF collection
expansion to `rdf:first`/`rest`/`nil`); moderate parser blast radius, off hot
path.

**Dataset PR (audit C4/§5.3) — constructwhere04.** Folds into the
`FROM`/`FROM NAMED` dataset-strategy work.

---

## 8. Appendix — reproductions (CLI, in-memory ledger)

All against `target/debug/fluree` on a fresh in-memory ledger.

- **agg02 datatype isolation** (data `:s :p1 :o1,:o2,:o3; :s :p2 :o1,:o2`):
  `SELECT (COUNT(*) AS ?C)` → `xsd:integer`;
  `SELECT ?P (COUNT(*) AS ?C) … GROUP BY ?P` → `xsd:integer`;
  `SELECT ?P (COUNT(?O) AS ?C) … GROUP BY ?P` → **`xsd:long`**;
  same + `GROUP_CONCAT` (forces non-streaming) → `xsd:integer`. Plan of the
  failing case: `ProjectOperator > GroupAggregateOperator > DatasetOperator`.
- **projexp05 DATATYPE** (data `in:a ex:p 1 ; ex:p ex:a`):
  `SELECT (datatype(?l) AS ?dt) WHERE { ?x ex:p ?l }` →
  IRI row `{"@id":"@id"}`, integer row `xsd:integer`.
- **pp16 / pp34 / pp35 / pp36 / sq02 / sq04 / exists03**: root-caused by code
  read against `property_path.rs`, `graph.rs`, `subquery.rs`, and the W3C test
  data/manifests; pp34/35 multiplicity confirmed by hand-tracing the closure
  over `ng-01.ttl` and the existing `it_query_seq_path_count_repro.rs` guard.
