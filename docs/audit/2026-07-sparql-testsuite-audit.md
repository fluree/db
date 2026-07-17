# W3C SPARQL Testsuite — Audit & 100% Coverage Plan (2026-07)

**Status:** audit complete; Phases A (harness completeness) and E (CI
enforcement) are implemented on branch `test/sparql-testsuite-full-coverage`.
`cargo test` in `testsuite-sparql/` runs all 36 suites green (~15 s wall,
parallel): 1,420 tests — 816 passing, 541 registered engine/feature gaps, 63
registered not-applicable — with the register enforced in both directions in
CI. Phases B/C/D burn the registers down from here. (Corrected 2026-07-06 by
the Stage-2 burn-down cross-check: the original 815/538/67 split was stale —
pp06 passes and was deregistered, and 4 entries were reclassified from
not-applicable to gap; see `docs/audit/burn-down/ROADMAP.md`.)
**Baseline:** commit `15e2a4b95` (origin/main), rdf-tests submodule `efccbc6b8` (`sparql-mixed-rdf-version-tests-228-gefccbc6`).

## 1. Executive summary

The `testsuite-sparql` harness is architecturally sound and honors its design
contract (~4.2k lines of Rust drive ~1,420 W3C test cases discovered from
manifests; zero hand-written test cases). But **CI enforces none of it**: the
GitHub Actions job runs only `fmt`, `clippy`, and `cargo test --no-run`. The
Makefile/docs claim plain `cargo test` is "CI-safe"; it is not — the default
(non-ignored) suite currently fails **85 syntax tests**, which is presumably
why the CI run step was never enabled.

Ground truth across every registered manifest plus the unregistered SPARQL 1.2
suite: **~1,420 tests, 595 failures**. The failures cluster into a small number
of root causes, and a large fraction are **harness gaps, not engine gaps** —
the engine has since grown capabilities (SPARQL UPDATE execution, TriG/named
graphs, connection-level datasets) that the harness never adopted.

Definition of done ("100% green light coverage"):

1. **Every** manifest in the submodule is registered and executed in CI —
   nothing is silently un-run.
2. Every test either passes or sits in an explicit, per-test, commented,
   reviewed skip register (the same `ignored_tests` mechanism that exists
   today, kept accurate by CI).
3. CI fails on any unexpected failure **and any unexpected pass** (stale skip
   entries are errors), so the register shrinks monotonically as engine gaps
   close.
4. Engine fixes land under the repo's perf-safety discipline (§6): no hot-path
   regressions to buy correctness.

## 2. Current state

### 2.1 Harness (contract: intact)

```
testsuite-sparql/            (excluded from workspace; own Cargo.lock)
├── src/manifest.rs          manifest.ttl → Test iterator (uses Fluree's own Turtle parser)
├── src/evaluator.rs         test-type IRI → handler dispatch
├── src/sparql_handlers.rs   handler registration
├── src/query_handler.rs     QueryEvaluationTest: ledger + load + query + compare
├── src/subprocess.rs        per-test subprocess isolation (5 s syntax / 10 s eval timeouts)
├── src/result_format.rs     .srx/.srj/.ttl(DAWG)/CONSTRUCT-graph parsing
├── src/result_comparison.rs isomorphic comparison incl. bnode mapping
├── src/report.rs            JSON report (W3C_REPORT_JSON)
└── tests/w3c_sparql.rs      registry: 28 test fns = manifest URL + skip list
```

This is the Oxigraph-style pattern and it has held: adding a whole new suite is
a ~10-line test fn. The refactor risk the team worried about ("too much custom
code") has **not** materialized; the fixes below are additive handlers, not a
rewrite.

### 2.2 What CI enforces today

`.github/workflows/ci.yml` job `testsuite-sparql`: fmt + clippy + `cargo test
--no-run`. **No test executes in CI.** There is no durable adherence to any
pass rate today.

### 2.3 Registry state

- Syntax suites + property-path run by default; 12 eval categories, 1.0 eval,
  update, json-res, csv-tsv, service, protocol, service-description,
  http-rdf-update, entailment are all `#[ignore]`d.
- `sparql11_property_path` is the model citizen: green with a curated,
  feature-grouped 18-test skip list. **13 of those 18 now pass** (stale — the
  features landed); only `pp06 pp16 pp34 pp35 pp36` still fail.
- The whole `rdf-tests/sparql/sparql12/` tree (SPARQL 1.2 / RDF-star,
  254 tests) is **unregistered**.

## 3. Ground truth (2026-07-06 baseline, this branch)

| Suite | Total | Pass | Fail | Notes |
|---|---|---|---|---|
| 1.1 syntax-query | 94 | 81 | 13 | 7 pos-rejected, 6 neg-accepted |
| 1.1 syntax-update-1 | 54 | 23 | 31 | graph-mgmt ops missing from parser |
| 1.1 syntax-update-2 | 1 | 1 | 0 | |
| 1.0 syntax | 199 | 158 | 41 | 17 pos (lists/forms/qname), 24 neg |
| 1.1 syntax-fed | 3 | 3 | 0 | |
| 1.1 aggregates | 46 | 37 | 9 | 5 neg-accepted (GROUP BY projection validation) |
| 1.1 bind / cast / negation | 10/6/12 | all | 0 | 100% |
| 1.1 bindings | 11 | 10 | 1 | `graph` (named-graph) |
| 1.1 construct | 7 | 5 | 2 | constructwhere04, constructlist |
| 1.1 exists | 6 | 4 | 2 | exists03, exists-graph-variable |
| 1.1 functions | 75 | 69 | 6 | strlang03-rdf11, concat02, bnode01, in01, notin01, iri01 |
| 1.1 grouping | 6 | 4 | 2 | group06/07 neg-accepted |
| 1.1 project-expression | 7 | 6 | 1 | projexp05 |
| 1.1 property-path | 33 | 28 | 5* | *in skip list; 13 skip entries stale |
| 1.1 subquery | 14 | 11 | 3 | subquery02/04/12 |
| 1.0 eval | 282 | 154 | 128 | see §4 clusters |
| 1.1 update (syntax+eval) | 157 | 24 | 133 | 94 eval "not implemented" + 39 syntax |
| 1.1 json-res | 4 | 0 | 4 | Turtle lexer: `_:o6.` (bnode label + dot) |
| 1.1 csv-tsv-res | 6 | 0 | 6 | comparison not implemented |
| 1.1 service | 7 | 0 | 7* | *skip-listed: needs endpoints |
| 1.1 protocol / GSP / svc-desc | 34/19/3 | 0 | all* | *skip-listed: not applicable (HTTP) |
| 1.1 entailment | 70 | 20 | 50 | needs RDFS/OWL regimes (out of scope) |
| **1.2 (unregistered probe)** | 254 | 91 | 163 | see §4.3 |

**Totals: ~1,420 registered+probe tests, 595 failing today.**

## 4. Failure taxonomy → root causes

### 4.1 Harness/registration gaps (no engine risk; largest, cheapest wins)

| Gap | Failing tests affected | Fix |
|---|---|---|
| UPDATE evaluation handler is a `bail!` stub, but the API now supports UPDATE (`GraphTransactBuilder::sparql_update`, issues #509/#1288 closed) | 94 (+3 in 1.2) | Implement `UpdateEvaluationTest`: build ledger from `ut:data`/`ut:graphData`, apply update, compare result graph(s) vs `ut:result` (isomorphic, per-graph) |
| Named-graph data loaded into the **default** graph (comment says "until TriG is supported" — TriG landed, #1278/#1279 closed) | ~14 (1.0 graph) + 1 bindings + several 1.0 dataset/1.1 exists | Load `qt:graphData` as real named graphs (wrap each file in `GRAPH <name> { }` TriG or use the named-graph insert API) |
| `FROM`/`FROM NAMED` rejected on single-ledger `GraphDb` | 12 (1.0 dataset) + part of graph | Design decision §5.3: within-ledger dataset construction (preferred) or harness-level graph→ledger mapping via `query_connection_sparql` |
| CSV/TSV result comparison stub | 6 | Implement CSV/TSV encoders/comparison (formatters may already exist in `fluree-db-api::format`) |
| SPARQL 1.2 suites unregistered | 254 | Register per-category fns like every other suite |
| `mf:PositiveUpdateSyntaxTest`/`mf:NegativeUpdateSyntaxTest` (1.2 non-`11` type IRIs) unhandled | 20 | Two `evaluator.register` lines |
| Labeled `qt:graphData` blank nodes silently dropped (`manifest.rs::get_graph_data` calls `term_to_string` on a blank node → `None`; never reads `qt:graph`) | latent (drops test data) | Read `qt:graph` off the blank node |
| Property-path skip list stale (13/18 now pass) | 0 (hides regressions) | Prune to `pp06 pp16 pp34 pp35 pp36`; CI's unexpected-pass check prevents recurrence |
| Combined suites (`sparql11_query_w3c_testsuite`, `sparql11_all`) duplicate per-category runs | n/a | Exclude from CI matrix (keep for local use) |

### 4.2 Engine gaps (perf-safety analysis required, §6)

Ordered roughly by breadth of impact:

1. **UPDATE grammar: graph-management ops absent.** `UpdateOperation` has only
   `InsertData/DeleteData/DeleteWhere/Modify`. Missing: `LOAD`, `CLEAR`,
   `CREATE`, `DROP`, `COPY`, `MOVE`, `ADD` (+ `SILENT`), and whatever `Modify`
   lacks (`WITH`/`USING` coverage TBD). Explains 27+27 positive-syntax
   rejections (1.1 update suites) and blocks a slice of update-eval. Parser
   work is off hot path entirely; execution maps to existing transact/ledger
   ops.
2. **Value/type semantics cluster (~76 in 1.0 eval + ~6 1.1 functions).**
   `type-promotion` (22), `open-world` (11), `expr-builtin` (14), `expr-ops`
   (7), `expr-equals` (7), `boolean-effective-value` (7), `cast` (7) + 1.1
   functions (6). Root causes: XSD derived-type promotion in comparisons,
   open-world equality for unknown datatypes, assorted builtin edge semantics.
   **Hot-path sensitive** — this is FILTER evaluation.
3. **Negative-syntax validation (56).** Parser accepts invalid queries:
   aggregate/GROUP BY projection scope (agg08-12, group06/07), plus 24 in 1.0
   and 6 in 1.1. Validation passes run at parse time only — off query hot
   path by construction.
4. **Positive-syntax parser gaps (24 in 1.0/1.1 query).** Collections `(...)`
   in patterns (syntax-lists-*), blank-node property list forms
   (syntax-forms), `syntax-qname-05`, `syntax-order-07`, test_21/23/35a/36a/
   63/64, `test_pp_coll`.
5. **Turtle lexer: bnode label followed by `.` without whitespace** (`_:o6.`)
   fails to lex — blocks all 4 json-res tests' data load and any user data of
   this shape. PN_LOCAL/label termination rule; perf-neutral char-class fix,
   but lexing is import-hot → bench guardrail (`insert_formats`,
   `import_bulk`).
6. **SPARQL parser: `BASE` + relative `PREFIX` IRI resolution**
   (`base-prefix-1`: "expected IRI after prefix namespace") — several 1.0
   `basic` failures.
7. **Property-path residuals** (pp16, pp34-36; pp06 passes and is already
   deregistered): pp16 zero-length-path node-universe completeness and pp36
   zero-var projection stay path-operator work; the burn-down verification
   reattributed pp34/pp35 to the graph cluster (constant-GRAPH-IRI base
   expansion + `?g`-as-literal), not path cardinality. Deep operator
   semantics in a hot operator — needs the §6 pattern (common shapes keep
   current code path).
8. **RDF-star / SPARQL 1.2** — see §4.3.
9. Misc: `agg-count-rows-distinct` execution error, constructwhere04/
   constructlist execution errors, subquery02/04/12, exists03/
   exists-graph-variable, xsd:long-vs-integer VALUES issue (#1319, open).

### 4.3 SPARQL 1.2 probe breakdown (254 tests, 91 pass today)

| Sub-suite | Pass | Fail | Dominant cause |
|---|---|---|---|
| syntax-triple-terms-positive | 19 | 94 | parser lacks `<<( )>>`/triple-term syntax |
| syntax-triple-terms-negative | 63 | 2 | pass "for free" (parser rejects everything star) |
| eval-triple-terms | 0 | 41 | Turtle-star data won't load + engine support |
| lang-basedir | 1 | 10 | base-direction literals (`@en--ltr`) |
| codepoint-escapes | 2 | 6 | `\u`/codepoint escape handling |
| version | 6 | 3 | `VERSION` declaration |
| rdf11 / expression / grouping / syntax | 0/0/0/0 | 3/1/1/2 | mixed |

RDF-star engine support state (code-level sub-audit, confirmed):

Fluree implements RDF 1.2 via a deliberate **LPG-style edge-annotation /
reified-edge model**, not first-class triple terms. Much more exists than
the raw pass counts suggest:

- **SPARQL parser: substantial support.** `<< >>` tokens exist (legacy
  Fluree history form, `lower/rdf_star.rs`); RDF 1.2 triple-term `<<( )>>`
  tokens are lexed and parsed (`parse/query/term.rs:690`) but deliberately
  restricted to object-of-`rdf:reifies`; annotation `{| |}` and `~` lower to
  `Pattern::EdgeAnnotation`/`AnnotationTarget` (`lower/annotation.rs`).
- **Engine + storage: end-to-end.** Annotations execute through the normal
  scan/join/policy pipeline (`execute/where_plan.rs:74`) with an arena
  fast-path (`annotation_edge_probe.rs`); durable storage is ordinary flakes
  under 7 reserved `f:reifies*` system predicates, novelty-overlaid and
  sealed into an on-disk annotation arena. ~519 test fns cover this. No
  feature flag — always on.
- **The actual 1.2-suite gaps** (what the 84 positive-syntax + 41 eval
  failures decompose into):
  1. `TRIPLE`/`SUBJECT`/`PREDICATE`/`OBJECT`/`isTRIPLE` functions — no
     tokens, no AST (deliberately deferred);
  2. bare triple terms as values (outside `rdf:reifies` object position) —
     conflicts with the no-first-class-triple-terms design choice; needs an
     explicit decision: implement vs. register as documented divergence;
  3. RDF-star **Turtle/N-Triples ingest** — `fluree-graph-turtle` has zero
     star tokens/grammar (`lex/token.rs:47-178`), and `fluree-graph-ir::Term`
     is `Iri|BlankNode|Literal` only; blocks all 41 eval-triple-terms tests
     at data load. The JSON-LD `@annotation` path IS fully handled — but in
     `fluree-db-transact/parse/edge_annotations.rs`, not the generic parser;
     Turtle-star ingest should map onto that same reifier pipeline;
  4. SPARQL `CONSTRUCT` projecting annotation metadata →
     `UnsupportedFeature` (`lower/construct.rs:91-96`) — output
     serialization only, matching works.
- Independent mini-features in the 1.2 suite: `VERSION` declaration (3),
  base-direction literals `@en--ltr` (10), codepoint escapes (6) —
  parser-local and cheap.

Related confirmations: UPDATE `Modify` **does** parse `WITH`/`USING`
(`lower_sparql_update.rs:653`); the graph-management verbs are lexed as
reserved keywords but have no AST (only 4 ops in `ast/update.rs:20-32`).
SERVICE is parsed and executed but **Fluree-only** (`fluree:ledger:` /
`fluree:remote:` — external endpoints explicitly rejected,
`fluree-db-query/src/service.rs:487-491`), which is the durable rationale
for the service-suite register.

### 4.4 Legitimately not-applicable (durable skip register with rationale)

- protocol (34), http-rdf-update/GSP (19), service-description (3): require
  HTTP client/server conformance testing; not a database-engine property.
  Already skip-listed with rationale. Keep; re-home if a server-level
  conformance harness ever exists.
- service (7): requires live external SPARQL endpoints. Future option: mock
  endpoint in-process; until then skip-listed.
- entailment (50 of 70): requires RDFS/OWL/RIF entailment regimes — a
  deliberate non-goal. **But 20 pass today** (simple-entailment-answerable);
  pin those green so regressions surface, skip-list the rest with rationale.

## 5. Target architecture decisions

### 5.1 CI enforcement design

Replace the compile-only step with a real run:

- `cargo test` (default set) must be green → becomes the gate. Everything
  reachable is registered **non-ignored** with explicit per-test skip lists.
- The `#[ignore]` attribute remains only for: combined/duplicate suites and
  suites whose runtime or infra needs make them local-only (none expected).
- Add **unexpected-pass detection** to `check_testsuite`: a test in
  `ignored_tests` that *passes* fails the suite with "stale skip entry —
  remove it". This is what keeps the register honest (it already rotted once:
  13/18 property-path entries).
- Keep per-test subprocess isolation (existing) so one hang cannot eat the job;
  job-level timeout as backstop.
- Nightly (not per-PR) full JSON report artifact for trend visibility.

### 5.2 Skip-register policy (unchanged from docs, now enforced)

Every entry: test IRI + comment (root cause, spec link, tracking issue) —
grouped by feature exactly like `sparql11_property_path` does today. CI
enforces both directions. Target end-state register after this effort:
protocol/GSP/service-desc/service/entailment (§4.4) + enumerated engine-gap
tests from §4.2 that don't land in the first wave, each tied to a GitHub
issue.

### 5.3 Dataset (`FROM`/`FROM NAMED`) strategy — needs a design call

W3C dataset tests select graphs from the test document set. Options:

- **(a) Within-ledger datasets** (semantic match): allow dataset clauses on a
  single ledger to restrict/compose from its named graphs. #1279's fix
  suggests partial infrastructure. Engine change — needs planner review.
- **(b) Harness maps graph URLs → ledgers** and uses
  `query_connection_sparql`: zero engine change, but ledger aliases must
  admit arbitrary IRIs and semantics must match (default-graph union etc.).

Decision deferred to implementation of the dataset category; everything else
is independent of it.

### 5.4 Net-zero vacuous passes (oracle limitation — PR #1437 review)

The both-way register catches a registered test that starts passing (stale
entry) and an unregistered test that fails (regression). It is blind, by
construction, to an *unregistered test that passes for the wrong reason* — a
false-pass. `UpdateEvaluationTest` has one structural false-pass class: a
**net-zero** update, whose expected end-state is isomorphic to its initial
state. Such a test passes whether the engine executed the update correctly or
did nothing at all (a total no-op — including an `is_empty_transaction` swallow
in `query_handler.rs::run_update_eval_test` — yields the same store). The
harness verifies the unchanged end-state but cannot confirm the engine no-op'd
*for the right reason*.

Enumerated 2026-07-08 (rdflib graph-isomorphism of each test's
`ut:data`/`ut:graphData` initial state vs its `mf:result` state, intersected
with the live pass list from `sparql11_update_tests`): **32 of 94
UpdateEvaluationTests are net-zero; 20 are already registered (known failures),
leaving 12 that currently pass vacuously**:

| Suite | Test | Shape |
|---|---|---|
| basic-update | `insert-data-spo-named3` | INSERT DATA of an already-present triple (idempotent-insert) |
| delete | `dawg-delete-03`, `dawg-delete-04`, `dawg-delete-07` | DELETE whose WHERE matches nothing |
| delete | `dawg-delete-using-03`, `dawg-delete-using-04` | USING-scoped DELETE, no match |
| delete | `dawg-delete-with-03`, `dawg-delete-with-04` | WITH-scoped DELETE of a non-existing triple |
| delete-data | `dawg-delete-data-03`, `dawg-delete-data-04` | DELETE DATA of absent triples |
| delete-insert | `dawg-delete-insert-06b` | DELETE/INSERT netting to zero |
| delete-where | `dawg-delete-where-03` | DELETE WHERE, no match |

All 12 are non-empty-store *invariance* tests (not trivial empty→empty). They
are **not** register candidates — they return the correct end-state and would
trip the stale-entry gate if listed. The resolution is vigilance, not
enforcement: the WITH/USING no-op cases (`dawg-delete-with-03/04`,
`dawg-delete-using-03/04`) are the ones to watch, because sibling tests in
those families (`dawg-delete-using-02a`, `dawg-delete-using-06a`) are
registered as producing the *wrong* graph-store state — so a green net-zero
sibling can give false confidence that WITH/USING scoping is correct when it is
only untested for that case. When the expression/scoping burn-down PRs touch
those families, re-run this enumeration and confirm the no-op cases fail for
the intended reason under a deliberately-mutating variant. Recorded in the
`tests/registers/mod.rs` header.

## 6. Perf-safety discipline (non-negotiable)

Repo priority is speed first, memory second. Correctness fixes must follow the
established off-hot-path pattern (cf. sibling-OPTIONAL fast path retained while
general OPTIONAL semantics were fixed; fulltext context setup skipped unless
the query uses `fulltext(...)`):

1. **Classify** every engine fix as parse-time, prepare-time, or per-row.
   Parse/prepare-time fixes (validation passes, grammar, update ops, promotion
   *table construction*) are inherently safe — do them there whenever
   possible.
2. **Per-row changes must preserve the common-type fast path.** e.g. type
   promotion: keep the existing same-type / integer-integer / string-string
   comparisons byte-identical; route only *mixed derived-numeric* and
   *unknown-datatype* comparisons through a new slow path selected at
   prepare time (per-predicate/per-expression, not per-row branching where
   avoidable).
3. **Property-path multiplicity**: preserve current traversal for `*`/`+`
   (distinct semantics are correct there); sequence-path counting only where
   the plan shape requires it, chosen at plan time.
4. **Bench guardrails per PR**: `query_hot_bsbm`, `query_hot_bsbm_bi` (filter/
   join hot paths), `insert_formats` + `import_bulk` (Turtle lexer), within
   `regression-budget.json` budgets; nightly bench workflow is the backstop.
5. Harness-only changes (Phase A) carry zero engine risk by definition.
6. **JSON-LD parity (team guideline).** SPARQL, JSON-LD query, and Cypher
   share the IR and engine. Every burn-down fix must classify as
   IR/engine-level (fixes all surfaces implicitly — still add a JSON-LD
   regression test) or surface-syntax addition (anything newly possible in
   SPARQL must be made possible in JSON-LD in the same effort, with tests —
   we own the JSON-LD query syntax). The W3C submodule only guards the
   SPARQL surface — JSON-LD regression coverage must be authored alongside
   each fix. Cypher is deliberately excluded: openCypher's grammar isn't
   ours to extend; it benefits from IR/engine fixes automatically. See
   `docs/contributing/sparql-compliance.md` § "Query Surface Parity".

## 7. Phased implementation plan

**Phase A — harness completeness (no engine changes):**
A1 fix `get_graph_data` labeled-graph bug; A2 named-graph loading via TriG;
A3 UPDATE-eval handler over `sparql_update`; A4 register 1.2 suites +
non-`11` update-syntax types; A5 CSV/TSV comparison; A6 prune property-path
skip list; A7 unexpected-pass detection; A8 entailment: pin 20 green, skip
rest with rationale. Re-baseline after A — expected: update-eval failures
collapse to real engine gaps; graph/bindings/exists partially green;
1.2 counts become accurate.

**Phase B — off-hot-path engine fixes:**
B1 UPDATE grammar graph-mgmt ops (+ exec mapping); B2 negative-syntax
validation passes; B3 positive-syntax parser gaps (lists/forms/qname/order);
B4 Turtle lexer bnode-dot fix (bench-guarded); B5 BASE/PREFIX resolution fix.

**Phase C — semantics fixes (bench-guarded, §6 pattern):**
C1 type-promotion + open-world equality + expr builtins cluster;
C2 remaining eval mismatches (functions/subquery/exists/construct/aggregates);
C3 property-path multiplicity; C4 dataset strategy (§5.3) + graph category.

**Phase D — RDF-star/1.2** (scope per the §4.3 sub-audit): Fluree's
edge-annotation model already covers parse→IR→match→storage. Work items:
D1 Turtle/TriG-star ingest mapped onto the existing reifier pipeline (the
same rewrite `edge_annotations.rs` does for JSON-LD `@annotation`);
D2 triple-term functions (TRIPLE/SUBJECT/PREDICATE/OBJECT/isTRIPLE);
D3 decision: bare triple-terms-as-values — implement vs. documented
divergence register (design conflict with the no-first-class-terms model);
D4 CONSTRUCT annotation projection; D5 mini-features (VERSION,
lang-basedir, codepoint escapes).

**Phase E — CI switch-on:** replace `--no-run` with the real run + both-way
enforcement; docs update (`sparql-compliance.md`, Makefile "CI-safe" claim);
nightly report artifact.

Sequencing: A → E can land immediately after A (CI enforces the accurate
baseline with explicit registers); B/C/D burn the register down in follow-up
PRs, each shrinking skip lists in the same change that fixes the engine.

## 8. Implementation status (updated 2026-07-06)

**Done (this branch):**
- Phase A complete: labeled `ut:graphData` parsing fixed; named graphs load
  as real named graphs (TriG via transact builder); `UpdateEvaluationTest`
  implemented end-to-end over the public `sparql_update` surface (24→67
  passing); CSV/TSV result comparison (0→5 of 6); SPARQL 1.2 suites
  registered per-category; non-`11` update-syntax test types handled;
  property-path register pruned (14 stale entries removed, `pp06`
  additionally flagged by the new stale-skip detection);
  unexpected-pass policing added to `check_testsuite`.
- Phase E complete: CI job now runs the full suite (was compile-only);
  Makefile/docs rewritten to match the registers model
  (`tests/registers/mod.rs`).

**New engine findings from Phase A runs (added to registers):**
- `GRAPH ?g` exposes the default graph as a graph named by the ledger alias,
  and binds `?g` as a *plain literal* rather than an IRI — breaks most
  `sparql10/graph` tests even though named-graph data now loads correctly.
- `GRAPH` blocks inside `DELETE WHERE` are rejected at lowering
  ("not yet supported").
- Multi-operation (`;`) UPDATE requests silently execute only the first
  operation (#1438) — this, not named-graph INSERT loss or combined
  DELETE/INSERT skew, is the verified root cause behind basic-update
  `insert-05a` / `insert-*-same-bnode*` and `dawg-delete-insert-01c`: each
  individual operation works, everything after the first `;` is discarded.
  (Corrected 2026-07-06 by the burn-down verification; the two earlier
  bullets "INSERT into a not-yet-existing named graph silently loses the
  triples" and "combined DELETE/INSERT WHERE applies inserts without the
  deletes" are refuted.)
- `USING` clause semantics incomplete (`dawg-delete-using-*`).
- CSV output does not use canonical `xsd:double` lexical form (`csv03`).

**Open items:**
- [ ] Phase B/C/D engine fixes per §4.2/§4.3 (each PR must shrink the
      registers it addresses; bench guardrails per §6).
- [ ] Decide §5.3 dataset strategy when Phase C4 starts.
- [ ] File tracking issues per §4.2 cluster (audit A2-style batch).
- [x] Deep code-level RDF-star audit — done, folded into §4.3 (2026-07-06).
- [ ] Phase D3 design decision: bare triple-terms-as-values vs. documented
      divergence (owner: engine team; blocks part of
      syntax-triple-terms-positive / eval-triple-terms registers).
