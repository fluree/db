# Algebra/OPTIONAL-scope + Output/Serialization — Deep Audit (burn-down W-1/W-2)

**Cluster owner deliverable — pre-implementation deep audit. No source was
modified (only this document).** This is the ninth cluster doc, owning the 14
register entries the Stage-2 cross-check found unowned (ROADMAP §2 work-streams
**W-1** algebra/OPTIONAL-scope (9) and **W-2** output/serialization (5)). Parent
context: `docs/audit/burn-down/ROADMAP.md` §2/§3/§6.1; the reassignment origin
in `docs/audit/burn-down/expression-semantics.md` §6; perf discipline in
`docs/audit/2026-07-sparql-testsuite-audit.md` §6; parity rules in
`docs/contributing/sparql-compliance.md` § Query Surface Parity.

**Baseline:** branch `test/sparql-testsuite-full-coverage` @ `0b84e6e24` (=
current HEAD = branch tip). rdf-tests submodule `efccbc6b8`. **Every** root
cause below was reproduced against the live engine — either through the W3C
harness (`W3C_REPORT_JSON` capture of the `sparql10_query_eval_tests` and
`sparql11_construct` suites) or by driving `target/debug/fluree query` on
fresh in-memory ledgers loaded with each test's own `.ttl`. Actual output
quoted below is real engine output.

**PR-X1 composition note.** The brief mandated designing against PR-X1's landed
D1 changes (`git diff 0b84e6e24..burndown/pr-x1 -- fluree-db-query`). I read
that diff. D1 lives entirely in `planner.rs::reorder_patterns` (pins var-free
FILTERs / BNODE-binds after all preceding `produced_vars`, `:1286-1326`) and
`filter.rs` (a zero-column batch carries its row count via
`empty_schema_with_len`, `:64-72`, `:594-599`). **None of the W-1 root causes
is in that machinery** (see §0.1 correction C1). The two touch different
compilation stages and compose without conflict; §2 states the one place D1's
`filter.rs` fix strengthens the general case.

---

## 0. Executive summary

The 14 entries are **not two clusters with one root cause each** — they are
**six** distinct defects. The register/roadmap groupings are partly wrong; the
five corrections below are load-bearing and change PR ownership.

| Family | Tests | Root cause | Owner |
|---|---|---|---|
| **A. FILTER/BIND scope leak** | filter-nested-2, dawg-optional-filter-005 | parser unwraps single-child `{ }` groups (`pattern.rs:228`) + lowerer flattens a group's own FILTER/BIND, so the scope boundary is lost and the FILTER sees an enclosing-scope variable | **PR-W1** (this cluster) |
| **B. Subquery merge of an optionally-produced correlation var** | join-scope-1 | `subquery.rs::self_produced_vars` (`:694`) excludes OPTIONAL-bound vars, so a correlation var bound only via OPTIONAL is not a join key; the merge keeps the parent's value and silently drops the subquery's conflicting binding | **PR-W1** (this cluster) |
| **C. Correlated-OPTIONAL executor** | nested-opt-1, nested-opt-2 | OPTIONAL is executed as a per-row **correlated** left-join (`optional.rs`, `SeedOperator`); SPARQL requires the body evaluated as an independent scope then left-joined. Divergence appears when the body rebinds a shared variable (through a nested OPTIONAL, C-1) or when a shared var appears in both the left and a later sibling OPTIONAL body (C-2) | **PR-W1-OPT** (separate, higher-risk) |
| **D. GRAPH-var dependent** | join-combo-2, dawg-optional-complex-2/3/4 | all four use a `GRAPH ?var` block; blocked by the GRAPH-var-as-literal + default-graph-enumeration defects | **PR-G1** (verify after) |
| **E. CONSTRUCT template blank nodes** | construct-3, construct-4, constructlist | template bnodes (`[ ]`, `_:a`, and collection first/rest) lower to `_:`-named `Var`s the WHERE never binds; `construct.rs::instantiate_row` (`:104`) skips any triple with an unbound term → empty graph | **PR-W2** (this cluster) |
| **F. Pattern-object datatype-drop (NOT escaping)** | quotes-3, quotes-4 | a plain-string pattern object matches a `:someType`-typed literal of the same lexeme and vice-versa — the scan-path datatype-drop of open-eq-02 / D5b, not string-escape serialization | **PR-X2** (reclassified out) |

### 0.1 Corrections to the roadmap / register (Stage-2 discipline)

- **C1 — W-1's fix territory is NOT `where_plan.rs` filter partitioning.**
  ROADMAP §2 W-1 and §3's dependency map assert W-1 shares "the same
  `where_plan.rs` filter machinery PR-X1's D1 touches" and therefore must land
  after PR-X1. Refuted empirically and by code read: the FILTER-scope fix is in
  the **parser** (`parse/query/pattern.rs:228`) and **lowerer**
  (`lower/pattern.rs`); the join-scope fix is in `subquery.rs`; the nested-opt
  fix is in `optional.rs`. `where_plan.rs::partition_eligible_filters` is not
  implicated in any of the nine. **W-1 has no dependency on PR-X1** and is
  parallel-safe with it (they touch disjoint files and stages). The "land PR-X1
  first" sequencing note for W-1 is void.

- **C2 — quotes-3/4 are datatype-matching, not "string-escape serialization".**
  ROADMAP §2 W-2 and `expression-semantics.md` §6 file quotes-3/4 as
  "string-escape serialization". Refuted: the triple-quote/newline escaping
  works (both queries produce a result); the bug is that a triple-pattern
  **object** ignores the datatype for unknown-datatype string literals —
  identical to `open-eq-02` (D5b). They belong in **PR-X2's D5b carve-out**, not
  W-2. This removes 2 entries from W-2 (W-2 is 3 CONSTRUCT tests, not 5).

- **C3 — W-1 is four defects, not one "FILTER-scope" cluster.** Only
  filter-nested-2 and dawg-optional-filter-005 are the FILTER-scope class named
  by `expression-semantics.md` §6. join-scope-1 is a subquery-merge bug,
  nested-opt-1/2 are OPTIONAL-executor bugs, and four are GRAPH-var-gated.

- **C4 — all three `optional-complex-*` tests are GRAPH-var-dependent, not just
  complex-2.** `expression-semantics.md` §6 flags only complex-2 as using
  `GRAPH ?x`. In fact complex-2 and complex-3 both use `GRAPH ?x { [] … }` and
  complex-4 uses `GRAPH ?g { [] … }` inside an OPTIONAL. All three are gated on
  PR-G1 (register comment at `mod.rs:492` should be widened).

- **C5 — construct-3/4 need generic per-solution bnode instantiation, not
  "reification-vocabulary" handling.** The register/roadmap frame construct-3/4
  as "reification output" (distinct from constructlist's "bnode instantiation").
  Empirically construct-3 (`[ … ]`), construct-4 (`_:a`), and constructlist
  (collection `( … )`) fail for the **same** reason (template bnode → never-bound
  var → dropped). One fix greens all three (constructlist additionally needs
  PR-1's collection desugaring). No reification-specific code is required.

---

## 1. Per-test root cause (with file:line evidence + empirical reproduction)

### Family A — FILTER/BIND scope leak

The IR `Pattern` enum has **no group variant** (`ir/pattern.rs:297-366`):
`Filter`, `Optional`, `Union`, `Bind`, … are siblings in a flat `Vec<Pattern>`.
Group scope is reconstructed only by the lowerer wrapping *nested-Group
children* of a plain Group as uncorrelated subqueries
(`lower/pattern.rs:47-71`). Two holes in that reconstruction leak scope.

**filter-nested-2** — `SELECT ?v { :x :p ?v . { FILTER(?v = 1) } }`, data
`:x :p 1,2,3,4`. Expected **0**; actual **1** (`{v:1}`).
- SPARQL algebra §18.2.2: the inner `{ FILTER(?v=1) }` = `Filter(?v=1, Z)` over
  the empty-pattern solution `Z`, where `?v` is **unbound** → EBV type error →
  false → 0 solutions; `Join(BGP(:x :p ?v), ∅) = ∅`.
- Mechanism: the inner group has one element, so the parser's single-pattern
  simplification returns the bare `Filter` **unwrapped**
  (`parse/query/pattern.rs:228-232`), never a `Group`. The outer group's
  children become `[Bgp, Filter]`; the lowerer's Group arm flattens both (only
  *Group* children get the subquery treatment, `lower/pattern.rs:49`), so the
  FILTER lands in the same scope as `:x :p ?v` and sees `?v` bound → keeps
  `v=1`. The sibling `filter-nested-1` (`{ :x :p ?v . FILTER(?v = 1) }`, filter
  *in* the same group) is **not** registered — it correctly returns `{v:1}`, so
  the defect is precisely the lost `{ }` boundary.
- Reproduced (CLI): `{ :x :p ?v . { FILTER(?v=1) } }` → 1 row; the equivalent
  multi-pattern nested group `{ :x :p ?v . { ?y :q ?w . FILTER(?v=1) } }`
  (which *does* stay a Group → subquery) → **0 rows** (correct). This proves the
  existing uncorrelated-subquery path already drops rows when a FILTER
  references an unbound var — the fix only has to route the single-element case
  through it.

**dawg-optional-filter-005-not-simplified** —
`{ ?book dc:title ?title . OPTIONAL { { ?book x:price ?price . FILTER(?title = "TITLE 2") } } }`.
Expected **3** rows, all title-only; actual 3 rows but one carries `price=20`.
- SPARQL: the FILTER sits in the doubly-nested group `{ ?book x:price ?price .
  FILTER(?title=…) }` where `?title` is **not** locally bound; evaluated as an
  independent scope, `FILTER(unbound = "TITLE 2")` errors → false → the group is
  empty → the OPTIONAL adds nothing → no row gets a price.
- Mechanism: `OPTIONAL { { … } }` — the OPTIONAL's outer group has one child
  (the inner Group), so the parser unwraps it (`pattern.rs:228`), making the
  inner `Group([triple, filter])` the OPTIONAL's direct pattern. Lowering the
  OPTIONAL (`lower/pattern.rs:74-79`) lowers that Group via the Group arm, which
  flattens its FILTER child into the OPTIONAL body. Fluree's OPTIONAL is a
  **correlated** left-join (§Family C), so the flattened FILTER is evaluated
  against the joined row where `?title` **is** bound → keeps price for TITLE 2.
- Reproduced (harness report): actual row `[1] {title:"TITLE 2", price:20}`
  where expected is `{title:"TITLE 2"}`.

### Family B — subquery merge of an optionally-produced correlation var

**join-scope-1** (`var-scope-join-1.rq`) —
`SELECT * { ?X :name "paul" { ?Y :name "george" . OPTIONAL { ?X :email ?Z } } }`.
Expected **0**; actual **2** (`{X:B1(paul), Y:B3, Z:john}`, `{X:B1, Y:B3,
Z:ringo}`).
- SPARQL: the inner group produces `{Y:B3, X:B2, Z:john}`, `{Y:B3, X:B4,
  Z:ringo}` (X/Z from the OPTIONAL over email triples, B2/B4); `Join` with the
  outer `?X :name "paul"` (X=B1) on `?X` → B1∉{B2,B4} → 0.
- Mechanism: the inner group is multi-pattern → wrapped as an uncorrelated
  subquery projecting `{Y,X,Z}` (`collect_bound_variables` descends into
  OPTIONAL, `lower/pattern.rs:340-342`). At `subquery.rs:119-124`
  `correlation_vars = {X}` (X ∈ parent schema ∩ subquery SELECT). But
  `join_keys` is filtered to vars in `self_produced_vars`
  (`subquery.rs:150-155`), and `self_produced_vars` (`:694-708`) counts only
  `Triple`/`PropertyPath`/non-sliced `Subquery` — **it does not descend into
  `Optional`/`Union`/`Bind`**. So X (bound only by the OPTIONAL) is a
  correlation var but **not** a join key; the merge appends the subquery's
  *new* vars `{Y,Z}` to the parent row and never checks X, keeping the parent's
  X=B1 while dropping the subquery's conflicting X=B2/B4.
- Isolation probes (CLI, var-scope data):
  `{ ?X :name "paul" { ?Y :name "george" . ?X :email ?Z } }` (X via a **required
  triple** inside) → **0** (X is self-produced → join key → correct);
  join-scope-1 (X via OPTIONAL) → **2** (wrong). The OPTIONAL is the trigger.

### Family C — correlated-OPTIONAL executor (highest risk)

Fluree's OPTIONAL is built **per required row**, seeding the optional side with
that row's bindings (`optional.rs:10` "the optional side is built per-row",
`:42` `SeedOperator`, `:60-92` `OptionalBuilder`). This equals the SPARQL
left-join for *well-designed* patterns but diverges when the body rebinds a
shared variable.

**nested-opt-1** (`two-nested-opt.rq`) —
`{ :x1 :p ?v . OPTIONAL { :x3 :q ?w . OPTIONAL { :x2 :p ?v } } }`, data
`:x1 :p 1 . :x2 :p 2 . :x3 :q 3,4`. Expected **1** (`{v:1}`, w unbound); actual
**2** (`{v:1,w:3}`,`{v:1,w:4}`).
- SPARQL: the outer OPTIONAL body evaluated **independently** =
  `LeftJoin(:x3 :q ?w, :x2 :p ?v)` = `{w:3,v:2}`,`{w:4,v:2}`; outer
  `LeftJoin({v:1}, …)` on `?v` → 1≠2 → keep `{v:1}` (w discarded).
- Mechanism: correlated seeding pins `?v=1` into the body, so the inner
  `OPTIONAL { :x2 :p ?v }` looks for `:x2 :p 1` (absent), binds nothing, and the
  outer optional keeps `w=3/4`. The independent evaluation would have bound
  `v=2`, forcing the mismatch that drops `w`.

**nested-opt-2** (`two-nested-opt-alt.rq`) —
`{ :x1 :p ?v . OPTIONAL { :x3 :q ?w } OPTIONAL { :x3 :q ?w . :x2 :p ?v } }`.
Expected **2** (`{v:1,w:3}`,`{v:1,w:4}`); actual **1** (`{v:1}`, w unbound).
- Isolation probes: the first OPTIONAL alone → `{v:1,w:3}`,`{v:1,w:4}` (correct);
  the second OPTIONAL alone → `{v:1}` (correct); **both together → `{v:1}`** —
  the second OPTIONAL, seeded with `w=3/4` from the first, fails its
  `:x2 :p ?v` match and returns a row with `?w` **unbound**, clobbering the
  `w=3/4` the first optional had bound. This is a distinct left-join merge
  defect: a shared variable (`?w` appears in the left *and* the failing
  optional's body) is reset to unbound on optional-miss instead of preserving
  the left value.

### Family D — GRAPH-var dependent (gated on PR-G1)

All four use a `GRAPH ?var` block and fail on the GRAPH-var-as-literal +
default-graph-enumeration defects documented in `named-graph-dataset.md` §1/§3
(BUG-2/BUG-5) and `residual-eval.md` §3.1 (A1-A4). `burndown/pr-g1` is an empty
placeholder at `0b84e6e24` (no engine work landed yet), so a graph-fixed
build cannot be run; these are classified, not reproduced-post-fix.

- **join-combo-2** — `{ GRAPH ?g { ?x ?p 1 } { ?x :p ?y } UNION { ?p a ?z } }`.
  Expected 1, actual 4. The `GRAPH ?g` block mis-binds `?x` (as a literal, and
  the default graph is enumerated) → spurious `?x` rows feed the UNION join.
- **dawg-optional-complex-2** — `GRAPH ?x { [] foaf:name ?name; foaf:nick ?nick }`
  + OPTIONAL/UNION. Expected 2, actual 6.
- **dawg-optional-complex-3** — `GRAPH ?x { [] … }` + nested OPTIONAL. Expected 2,
  actual 6.
- **dawg-optional-complex-4** — `GRAPH ?g { [] … }` **inside** an OPTIONAL.
  Expected 5, actual 5 but non-isomorphic bindings.

Acceptance: after PR-G1 lands, **re-run all four**; complex-3/4 (which combine
GRAPH with nested OPTIONAL) may retain a Family-C residual and must be verified,
not assumed green.

### Family E — CONSTRUCT template blank nodes

**construct-3** (`[ rdf:subject ?s ; rdf:predicate ?p ; rdf:object ?o ]`),
**construct-4** (`_:a rdf:subject ?s ; …`): expected **24** triples, actual
**0**. **constructlist** (`CONSTRUCT { (?s ?o) :prop ?p }`): today a parse error
("RDF collection (list) syntax is not yet supported"); after PR-1 desugars the
collection to `_:l0 rdf:first ?s ; rdf:rest _:l1 . _:l1 rdf:first ?o ; rdf:rest
rdf:nil . _:l0 :prop ?p`, it hits the same bnode gap.
- Mechanism: `Term`/`Ref` have **no blank-node variant** (`ir/triple.rs:18-30`,
  `:117-129`) — a template bnode lowers to a `Var` whose name keeps the `_:`
  prefix (`lower/term.rs:75-93`: `_:a` → `vars.get_or_insert("_:a")`, `[ ]` →
  `vars.get_or_insert("_:b{n}")`). That var is not produced by the WHERE, so
  `instantiate_row` (`format/construct.rs:97-109`) resolves it to `None`
  (`resolve_subject_term`, `:157` Unbound / `:173` absent) and the guard at
  `:104` (`let (Some(s),Some(p),Some(o)) = … else { continue }`) **skips every
  template triple** → empty graph.
- Reproduced (CLI): a bound-subject template `{ ?s :tagged :Yes }` emits
  triples; the bnode-subject template `{ [ rdf:subject ?s ; rdf:object ?o ] }`
  yields `@graph: []`. The output path already emits blank nodes for `_:`-
  prefixed bindings (`construct.rs:143-155`) — only per-solution *instantiation*
  is missing.

### Family F — pattern-object datatype-drop (reclassified to PR-X2)

**quotes-3** (`?x ?p '''x\ny'''`, plain string) and **quotes-4** (`?x ?p
"""x\ny"""^^:someType`, typed), data `:x2 :p2 "x\ny"`, `:x3 :p3
"x\ny"^^:someType`. quotes-3 expects only `x2`, quotes-4 only `x3`; both queries
return **both `x2` and `x3`** (reproduced via CLI). The `x\ny` triple-quote
value parses fine on both sides — the escaping is correct. The defect is that
the triple-pattern object match ignores the datatype for unknown-datatype string
literals: plain `"x\ny"` matches `"x\ny"^^:someType` and vice-versa. This is
byte-for-byte the `open-eq-02` scan-path defect (**D5b**,
`expression-semantics.md` §1 D5b, register `mod.rs:263`). **Move quotes-3/4 to
PR-X2's D5b carve-out**; they are removed by the same per-flake datatype
constraint fix and need no W-2 work.

---

## 2. Fix designs (composing with D1)

### PR-W1 — group scope & correlation (Families A + B; greens 3)

**A. Preserve the scope boundary for scope-sensitive single-child groups.**
Root the fix in the parser, because once `pattern.rs:228` unwraps the group the
boundary is unrecoverable. Change the simplification so a single-child group is
returned bare **only when the child is not scope-sensitive**; keep it as a
`Group` when the child is a `Filter`, `Bind`, or a nested `Group`:

```
// parse/query/pattern.rs (~:228) — sketch
if patterns.len() == 1 && !is_scope_sensitive(&patterns[0]) {
    Some(patterns.remove(0))          // Triple/Optional/Union/Path/… : safe to unwrap
} else {
    Some(GraphPattern::group(patterns, span))  // Filter/Bind/Group : keep the boundary
}
```

Then the **existing** lowerer path (`lower/pattern.rs:49-65`) wraps that Group
child as an uncorrelated subquery — the mechanism already proven correct for
multi-pattern groups. Relax the `debug_assert!(… inner.len() >= 2 …)`
(`lower/pattern.rs:34-45`) to admit single-element FILTER/BIND groups, and
update its comment. This one change fixes **both** filter-nested-2 (single
`{ FILTER }`) and dawg-optional-filter-005 (the `OPTIONAL { { … } }` double
brace now keeps the inner Group, which the OPTIONAL-body lowering wraps as a
subquery, so the FILTER is evaluated in an independent scope where `?title` is
unbound).

*Why this is correct against the algebra:* wrapping `{ FILTER(?v=1) }` as an
uncorrelated subquery with body `[Filter(?v=1)]` and `select = {}` yields
`Filter(?v=1, Z)` — the filter runs over the unit solution with `?v` unbound →
0 rows → cross-join with the parent → 0. Verified: `{ { FILTER(?v=1) } }` → 0
today, and the multi-pattern analogue → 0.

*Composition with D1:* independent — the change is parse/lower-time; D1 is
`planner.rs`/`filter.rs`. For the **target tests** (`?v=1`, `?title="TITLE 2"`,
both var-referencing) the fix works with or without D1. D1's `filter.rs`
zero-column-batch fix (`:64-72`) additionally guarantees the *general* case
`{ FILTER(true) }`-as-subquery keeps its unit row instead of the pre-D1
row-wipe (#1439); land W-1 on top of PR-X1 to inherit that (no hard dependency,
but it removes a latent regression for constant-filter nested groups).

**B. Reconcile optionally-produced correlation vars in the subquery merge.**
In `subquery.rs`, a correlation var that the subquery *may* bind (via
OPTIONAL/UNION/BIND) but is not a hash `join_key` must still be checked at merge
time: for each subquery row where that var is bound to a value **≠** the parent
binding, reject the row; where it is unbound in the subquery row, keep the
parent binding (unbound-compatible natural join, SPARQL §18.4). Concretely,
partition `correlation_vars` into `join_keys` (self-produced — unchanged hash
path) and `reconcile_vars` (produced only via OPTIONAL/UNION — the set
`self_produced_vars` currently omits), and add an equality check on
`reconcile_vars` in the per-row merge. `self_produced_vars` (`:694`) stays as-is
for the hash-key decision (an optional var is not a safe hash key); the new
reconciliation is a post-merge filter, not a key.

*Composition with D1:* none — different file.

### PR-W1-OPT — correlated-OPTIONAL independence (Family C; greens 2; higher risk)

nested-opt-1/2 require the OPTIONAL body to be evaluated as an **independent
scope** when it is *not well-designed* w.r.t. the outer scope. Selected at
**prepare time** (never per-row): a body is non-well-designed iff a variable
bound in the outer/required scope is also (re)bound inside a **nested**
OPTIONAL/UNION of the body (nested-opt-1), or a variable appears in both the
left of the left-join and a later sibling OPTIONAL body (nested-opt-2). For
well-designed bodies (the overwhelming common case, incl. BSBM Q3) keep the fast
correlated `SeedOperator` path **byte-identical**. For the flagged shapes, route
the body through the uncorrelated-subquery evaluation (independent, then
left-join on shared vars) — the same primitive PR-W1 uses. nested-opt-2 also
needs the left-join merge to **preserve** a left-bound shared variable when the
optional misses (don't reset `?w` to unbound). This is the deepest, hottest
change in the cluster; isolate it so PR-W1 stays low-risk and bisectable.

### PR-W2 — per-solution CONSTRUCT bnode instantiation (Family E; greens 2 now + constructlist after PR-1)

1. **Record template blank-node vars.** At CONSTRUCT lowering
   (`lower/construct.rs::lower_construct_template`), collect the set of template
   vars that originated as blank nodes (identifiable today by the `_:` name
   prefix the lowerer already assigns, `lower/term.rs:87/:91`) into a new
   `ConstructTemplate.bnode_vars: HashSet<VarId>` (`ir/query.rs:31-41`).
   Recording it explicitly beats re-sniffing names in the formatter.
2. **Instantiate per solution row.** In `instantiate_row`
   (`format/construct.rs:89-112`), before resolving terms, build a per-row map
   `bnode_var → BlankId`, minting a fresh label per (var, row) — same label for
   the same template bnode within one row (so `[ rdf:subject ?s ; rdf:object ?o
   ]` links both triples to one node) and distinct across rows (a running
   counter). `resolve_subject_term`/`resolve_object_term` consult this map when
   the `Ref::Var` is a template bnode var, instead of `batch.get`. The output
   side already renders `IrTerm::BlankNode` (`construct.rs:144/:152`), so only
   the source of the label is new.
3. **constructlist** additionally depends on **PR-1** (collection desugaring in
   `parse/query/term.rs`); its first/rest bnodes then flow through the same
   instantiation. Keep constructlist registered until *both* land (ROADMAP
   §1.1-1 already requires this).

*Composition with D1:* none — CONSTRUCT output path.

### Reclassifications (no W-1/W-2 code)

- **quotes-3/4 → PR-X2** (D5b scan-path datatype constraint). Remove from W-2.
- **join-combo-2, optional-complex-2/3/4 → PR-G1**; re-verify complex-3/4 for a
  Family-C residual afterward.

---

## 3. Hot-path classification + bench gates

| Fix | Stage | Per-row hot? | Notes / gate |
|---|---|---|---|
| **A** parser/lowerer scope boundary | parse + prepare | No | Only rare shapes (single-`{FILTER}`, `{ { } }`) now execute as uncorrelated subqueries — a mechanism already paid for multi-pattern nested groups; common BGP/OPTIONAL unchanged. Gate `query_hot_bsbm`/`query_hot_bsbm_bi` for no-op sanity. |
| **B** subquery merge reconcile | prepare (partition) + per-row (correlated subq only) | Only correlated-subquery merge | Added check runs only for `reconcile_vars` (optional-produced correlation vars — uncommon); hash `join_key` path byte-identical. Gate both BSBM benches. |
| **C** OPTIONAL independence | **prepare-time selection** + per-row (flagged shapes only) | **OPTIONAL is hot** | Highest risk. Well-designed detection is a plan-build predicate; the correlated `SeedOperator` fast path (incl. BSBM Q3) must stay byte-identical. **Gate `query_hot_bsbm` AND `query_hot_bsbm_bi` every commit**; the BI variant exercises OPTIONAL-heavy shapes. |
| **E** CONSTRUCT bnode instantiation | per-solution-row (output) | No (not scan-hot) | CONSTRUCT is not in any `regression-budget.json` bench; a per-row bnode map is a small alloc bounded by template bnode count. Standard gates only; optionally add a CONSTRUCT micro-bench when the bench backlog is next touched. |

No fix here is *forced* onto the scan/join hot loop except C, and C is
prepare-time-selected so its common case is unchanged. Per §6, correctness must
not buy a hot-path regression: PR-W1-OPT's acceptance bar is both BSBM benches
within budget with the fast path unaltered.

---

## 4. Query-surface parity (SPARQL + JSON-LD; Cypher excluded per compliance doc)

Families A/B/C are **IR/engine-level** (lowerer, `subquery.rs`, `optional.rs`
are shared by the JSON-LD FQL surface), so they fix JSON-LD implicitly — but
nothing guards it without a test. Family E (CONSTRUCT) is a **SPARQL-only**
surface. Family F is handled by PR-X2's D5 JSON-LD tests.

| Fix | JSON-LD expressible? | Regression test to author |
|---|---|---|
| A — nested-group FILTER scope | Yes (a `filter` inside a nested `optional`/`union` block whose vars are enclosing-scope) | `it_query.rs`: nested-scope filter must not see an enclosing var; and an OPTIONAL-with-inner-filter-referencing-outer-var must not bind (JSON-LD analogue of optional-filter-005) |
| B — subquery optional-var correlation | Yes (JSON-LD sub-select projecting an optional-bound var that equals a parent var) | `it_query.rs`: nested sub-select whose OPTIONAL-bound var equals the parent var → join reconciles (0 rows for the join-scope shape) |
| C — nested-OPTIONAL independence | Yes (nested `optional` inside `optional`; sibling `optional`s sharing a var) | `it_query.rs`: nested-optional rebinding an outer var drops the outer optional's binding; sibling optionals preserve the first's binding |
| E — CONSTRUCT bnode instantiation | **No** (JSON-LD query has no CONSTRUCT form) | SPARQL-only: `it_query_sparql.rs` bnode-template emits fresh bnodes per solution (24 triples; same bnode within a solution). Record the surface-only classification in the PR description per compliance §Query Surface Parity. If a JSON-LD "construct"/graph-projection capability is later added it must reuse this path. |

Files: SPARQL → `fluree-db-api/tests/it_query_sparql.rs`; JSON-LD →
`it_query.rs`. Run via `grp_query` / `grp_query_sparql`.

---

## 5. Blast radius, PR composition, risks, open questions

**Blast radius.** A (parser+lowerer) is contained but touches a load-bearing
parser invariant (single-child unwrap) consumed by the lowerer's scope logic —
full parser + SPARQL/JSON-LD query suites must rerun. B is contained to
`subquery.rs`. C is the widest and hottest (`optional.rs` + left-join merge) —
every OPTIONAL query is in scope. E is contained to `format/construct.rs` +
`ir/query.rs` + `lower/construct.rs`, SPARQL-reachable only.

**Recommended composition (two owned PRs + one carve-out):**

- **PR-W1 — algebra scope & correlation** (Families A + B). Coherent theme
  (group-scope correctness), low-medium risk, greens **filter-nested-2,
  dawg-optional-filter-005, join-scope-1**. Parser + lowerer + `subquery.rs`.
  Standard gates + the three JSON-LD parity tests above. **No PR-X1 dependency**
  (C1); land any time, ideally after PR-X1 to inherit the `filter.rs`
  constant-filter fix for the general case.
- **PR-W2 — CONSTRUCT bnode instantiation** (Family E). Low risk, SPARQL-only,
  greens **construct-3, construct-4**, and **constructlist** once PR-1 lands
  (keep constructlist registered until then). `format/construct.rs` +
  `ir/query.rs` + `lower/construct.rs`.
- **PR-W1-OPT — correlated-OPTIONAL independence** (Family C, **separate**).
  Highest risk in the cluster; greens **nested-opt-1, nested-opt-2**. Isolate
  for bench sign-off and bisectability; do not bundle with PR-W1. Deferrable
  with the two entries kept registered if the prepare-time
  well-designed detection can't be made bench-clean.

**Not owned here:** join-combo-2 + optional-complex-2/3/4 (PR-G1, verify after);
quotes-3/4 (PR-X2 D5b).

**Register accounting (this cluster's 14):** PR-W1 −3, PR-W2 −2 now (+
constructlist −1 jointly with PR-1), PR-W1-OPT −2, PR-G1 −4 (verify), PR-X2 −2
(reclassified). Full green requires PR-1 (constructlist), PR-G1 (4), PR-X2 (2),
plus the three owned efforts.

**Risks.**
- A changes a parser invariant; a mis-scoped unwrap predicate could wrap
  benign single-triple groups as subqueries (perf) or miss a scope-sensitive
  child (correctness). Enumerate the scope-sensitive set precisely
  (Filter/Bind/Group) and keep Triple/Optional/Union/Path unwrapping unchanged.
- C is a behavior change for non-well-designed OPTIONAL patterns; some users may
  rely on the current (wrong) correlated results. Changelog note.
- B: confirm the reconcile-vars post-filter does not double-count when a var is
  *both* a hash key and optional-produced (it can't be, by the partition, but
  assert it).

**Open questions.**
1. C's well-designed predicate: is "shared var rebound inside a nested
   OPTIONAL/UNION of the body" the exact necessary-and-sufficient trigger, or
   are there sibling-scope cases (nested-opt-2) needing a broader rule? Settle
   by enumerating the DAWG algebra tests against the predicate before coding.
2. E: should Fluree ever expose a JSON-LD graph-projection ("construct") form?
   If not, record CONSTRUCT as permanently SPARQL-only surface (no parity
   obligation) — recommended.
3. E edge case (not in the test set): a blank-node label used in **both** the
   WHERE and the template lowers to one `_:`-named var today, conflating
   template-scoped and WHERE-scoped bnodes (SPARQL keeps them separate). Flag as
   a latent follow-up; the per-solution instantiation should key on
   template-only bnode vars to avoid regressing WHERE bnodes.
