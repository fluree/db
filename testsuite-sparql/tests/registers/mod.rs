//! Per-suite skip registers for the W3C SPARQL test suites.
//!
//! Each entry is a test that currently FAILS for a known reason. The suites
//! themselves always run in CI; `check_testsuite` enforces this register in
//! BOTH directions:
//!
//! - a test that fails and is not listed here fails the suite (regression);
//! - a test that passes but IS listed here fails the suite (stale entry —
//!   remove it in the same change that fixes the feature).
//!
//! LIMITATION — what this register CANNOT catch: an *unregistered* test that
//! PASSES for the wrong reason. The check polices registered-pass (stale) and
//! unregistered-fail (regression); a false-pass is invisible to it by
//! construction. Two harness leniencies open that blind spot and need manual
//! vigilance, not register enforcement:
//!
//! - Empty named graphs are not tracked, so CLEAR (graph stays, empty), DROP
//!   (graph removed) and CREATE (empty graph exists) end-states are partially
//!   unobservable (see query_handler.rs `run_update_eval_test` step 5). Every
//!   such test is currently registered below (the grammar does not parse the
//!   graph-management ops yet). When that grammar lands, they must gain
//!   explicit end-state assertions or STAY registered — do NOT un-register them
//!   just because `check_testsuite` reports an "unexpected pass".
//!
//! - Net-zero updates (expected end-state == initial state) pass whether or not
//!   the engine actually ran the update; on their own they confirm end-state
//!   invariance, not execution. 12 currently-passing update-eval tests are
//!   net-zero (enumerated in docs/audit/2026-07-sparql-testsuite-audit.md
//!   §"Net-zero vacuous passes"); the WITH/USING no-op cases
//!   (dawg-delete-with-03/04, dawg-delete-using-03/04) warrant a manual check,
//!   since sibling tests in those families are known-broken and registered.
//!
//! Grouping comments name the root cause. The full failure taxonomy, root
//! causes, and burn-down plan live in
//! `docs/audit/2026-07-sparql-testsuite-audit.md`; policy for adding entries
//! is in `docs/contributing/sparql-compliance.md` ("Managing the Skip List").
//! Baseline: rdf-tests submodule @ efccbc6b8, 2026-07-06.

pub const SPARQL11_SYNTAX_QUERY: &[&str] = &[
    // fully green: PR-1 (accept-more) + PR-2 (V3-V6 validation) + main's #1436
];

// Dominated by the missing UPDATE graph-management grammar
// (LOAD/CLEAR/CREATE/DROP/COPY/MOVE/ADD, SILENT variants) — audit §4.2.1.
// NOTE: every test here is double-registered (also inside SPARQL11_UPDATE);
// a fix PR must delete BOTH register lines per test.
pub const SPARQL11_SYNTAX_UPDATE_1: &[&str] = &[
    // parser rejects valid input: graph-management grammar (23) — PR-U3
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_1",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_10",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_11",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_12",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_13",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_14",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_15",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_16",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_17",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_18",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_19",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_2",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_20",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_21",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_22",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_3",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_37",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_4",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_5",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_6",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_7",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_8",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_9",
    // parser rejects valid input: validator rejects GRAPH inside DELETE WHERE
    // (update class D, NOT graph-management grammar) (1) — PR-U1
    // parser rejects valid input: empty / prologue-only request must be a
    // valid no-op (update class C, NOT graph-management grammar) (3) — PR-U2
    // parser accepts invalid input (missing validation) (3) — PR-U1
    // parser accepts invalid input: cross-operation blank-node label reuse in
    // a multi-op `;` request (update class B validation) (1) — PR-U2
];

pub const SPARQL10_SYNTAX: &[&str] = &[
    // parser accepts invalid input (missing validation) (13)
    // All remaining entries are V1 (load-bearing dots) / V2 (FILTER
    // Constraint) grammar-tightening territory — burn-down PR-3, not the
    // PR-2 semantic-validation passes (which cleared the V3 blabel/
    // breaks-BGP class).
];

pub const SPARQL11_AGGREGATES: &[&str] = &[
    // graph cluster: COUNT over an (empty) named graph — needs enumerable
    // empty named graphs; gated on decision D-6, expected to remain
    // registered after PR-G1 (1)
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#agg-empty-group-count-graph",
    // expression/aggregate cluster (2) — PR-X2: agg-err-01 = aggregate must
    // poison (unbind) on non-numeric group members; agg02 = COUNT(?var)
    // re-typed as xsd:int — fix site UNCONFIRMED (it is NOT the
    // group_aggregate finalize; probe before fixing)
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#agg-err-01",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#agg02",
    // expression/aggregate cluster: COUNT(DISTINCT *) needs a
    // rows-distinct IR aggregate (1) — PR-X2
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#agg-count-rows-distinct",
];

pub const SPARQL11_BINDINGS: &[&str] = &[
    // graph cluster: requires enumerable empty named graphs; stays
    // registered after PR-G1, gated on decision D-6 (1)
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/bindings/manifest#graph",
];

pub const SPARQL11_CONSTRUCT: &[&str] = &[
    // dataset: FROM/FROM NAMED unsupported on single-ledger GraphDb (1) — PR-G2
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/construct/manifest#constructwhere04",
    // parse-time rejection of RDF collection syntax in the CONSTRUCT
    // template (NOT a query-execution error); once PR-1 lands collections it
    // becomes the CONSTRUCT-template blank-node instantiation gap — W-2
    // serialization cluster; stays registered through PR-1 (1)
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/construct/manifest#constructlist",
];

pub const SPARQL11_EXISTS: &[&str] = &[
    // result mismatch (1)
    // graph cluster: BASE-relative graph IRI resolution + EXISTS
    // active-graph inheritance (1) — PR-BASE + PR-G1 (conditional)
];

pub const SPARQL11_FUNCTIONS: &[&str] = &[
    // result mismatch (3)
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/functions/manifest#concat02",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/functions/manifest#strlang03-rdf11",
];

pub const SPARQL11_GROUPING: &[&str] = &[];

pub const SPARQL11_PROJECT_EXPRESSION: &[&str] = &[];

pub const SPARQL11_SUBQUERY: &[&str] = &[
    // graph cluster (2) — PR-G1: subquery02 = GRAPH ?g correlation into the
    // subquery; subquery04 = default graph leaks into a GRAPH-scoped subquery
    // (subquery12 — CONSTRUCT ↔ sub-SELECT alias visibility — was greened by
    // main's sub-SELECT set-operand fix, PR #1436)
];

// Largest clusters: XSD type-promotion comparisons, open-world equality,
// expression builtins, FROM/FROM NAMED dataset construction — audit §4.2.
// (the "GRAPH ?g binding typed as literal" cluster was fixed by #1442/#1443)
pub const SPARQL10_QUERY_EVAL: &[&str] = &[
    // result mismatch (83); subgroup ownership per
    // docs/audit/burn-down/ROADMAP.md §6.1
    //
    // W-1 algebra cluster: nested-group FILTER sees enclosing-scope
    // variables; nested-OPTIONAL / join variable-scope semantics (5)
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#filter-nested-2",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#join-combo-2",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#join-scope-1",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#nested-opt-1",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#nested-opt-2",
    // PR-BASE: relative-IRI/BASE resolution in query output (2)
    // list-1..4: the SPARQL parser now desugars `( ... )` patterns to
    // rdf:first/rest/nil triples, but Fluree's Turtle ingest emits
    // OBJECT-position collections as Fluree list_index items (and drops
    // `()` objects entirely) — `parse_collection_as_list`,
    // fluree-graph-turtle/src/parser.rs — so the first/rest triples the
    // query needs are never stored. Eval-side ingest/model gap, not parser.
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/basic/manifest#list-1",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/basic/manifest#list-2",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/basic/manifest#list-3",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/basic/manifest#list-4",
    // W-2 serialization cluster: string-escape serialization (2)
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/basic/manifest#quotes-3",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/basic/manifest#quotes-4",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/boolean-effective-value/manifest#dawg-bev-1",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/boolean-effective-value/manifest#dawg-bev-2",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/boolean-effective-value/manifest#dawg-bev-3",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/boolean-effective-value/manifest#dawg-bev-4",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/boolean-effective-value/manifest#dawg-bev-5",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/boolean-effective-value/manifest#dawg-bev-6",
    // W-2 serialization cluster: reification output in CONSTRUCT (2)
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/construct/manifest#construct-3",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/construct/manifest#construct-4",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/distinct/manifest#distinct-1",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/distinct/manifest#distinct-9",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-lang-3",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-langMatches-1",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-langMatches-2",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-langMatches-3",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-langMatches-4",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-langMatches-basic",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-str-1",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-str-2",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#sameTerm-eq",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#sameTerm-not-eq",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#sameTerm-simple",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-equals/manifest#eq-2-1",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-equals/manifest#eq-2-2",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-equals/manifest#eq-4",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-equals/manifest#eq-dateTime",
    // joint PR-G1 (GRAPH-variable semantics) + PR-X2 (D5 value equality);
    // the second lander removes these (3)
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-equals/manifest#eq-graph-1",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-equals/manifest#eq-graph-2",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-equals/manifest#eq-graph-4",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-ops/manifest#add-numbers-cast",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-ops/manifest#divide-numbers-cast",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-ops/manifest#multiply-numbers-cast",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-ops/manifest#subtract-numbers-cast",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-ops/manifest#unminus-2",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-ops/manifest#unplus-2",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#date-1",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-01",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-02",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-04",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-05",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-06",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-07",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-08",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-10",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-11",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-12",
    // W-1 algebra cluster: FILTER-scope / nested-OPTIONAL semantics;
    // optional-complex-2 is additionally gated on PR-G1 (GRAPH ?x) (4)
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional-filter/manifest#dawg-optional-filter-005-not-simplified",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional/manifest#dawg-optional-complex-2",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional/manifest#dawg-optional-complex-3",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional/manifest#dawg-optional-complex-4",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-03",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-04",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-05",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-21",
    // tP-29/30 expect ASK=false and previously "passed" only because
    // DATATYPE(expr) errored; evaluating expression arguments unmasks the
    // D4 numeric-promotion defect (double/decimal, float) — PR-X2.
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-29",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-30",
    // dataset: FROM/FROM NAMED unsupported on single-ledger GraphDb (12)
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/dataset/manifest#dawg-dataset-01",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/dataset/manifest#dawg-dataset-02",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/dataset/manifest#dawg-dataset-03",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/dataset/manifest#dawg-dataset-04",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/dataset/manifest#dawg-dataset-05",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/dataset/manifest#dawg-dataset-06",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/dataset/manifest#dawg-dataset-07",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/dataset/manifest#dawg-dataset-08",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/dataset/manifest#dawg-dataset-09b",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/dataset/manifest#dawg-dataset-10b",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/dataset/manifest#dawg-dataset-11",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/dataset/manifest#dawg-dataset-12b",
    // query execution error: PR-BASE (relative-IRI/BASE resolution at lower
    // time); its lexer half lands in PR-1 (1)
];

// Clusters: graph-management ops missing from the UPDATE grammar,
// GRAPH blocks in DELETE WHERE, USING + GRAPH scoping, and multi-operation
// `;` requests silently executing only their first operation (#1438) —
// audit §4.2.1, burn-down update-completeness.md. The syntax-update-1 tests
// are double-registered (see SPARQL11_SYNTAX_UPDATE_1); fix PRs must delete
// both register lines per test.
pub const SPARQL11_UPDATE: &[&str] = &[
    // update grammar: graph-management op not parsed (41)
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/add/manifest#add01",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/add/manifest#add02",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/add/manifest#add03",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/add/manifest#add04",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/add/manifest#add05",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/add/manifest#add06",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/add/manifest#add07",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/add/manifest#add08",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/clear/manifest#dawg-clear-all-01",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/clear/manifest#dawg-clear-default-01",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/clear/manifest#dawg-clear-graph-01",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/clear/manifest#dawg-clear-named-01",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/copy/manifest#copy01",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/copy/manifest#copy02",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/copy/manifest#copy03",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/copy/manifest#copy04",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/copy/manifest#copy06",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/copy/manifest#copy07",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/drop/manifest#dawg-drop-all-01",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/drop/manifest#dawg-drop-default-01",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/drop/manifest#dawg-drop-graph-01",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/drop/manifest#dawg-drop-named-01",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/move/manifest#move01",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/move/manifest#move02",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/move/manifest#move03",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/move/manifest#move04",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/move/manifest#move06",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/move/manifest#move07",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/update-silent/manifest#add-silent",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/update-silent/manifest#add-to-default-silent",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/update-silent/manifest#clear-default-silent",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/update-silent/manifest#clear-silent",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/update-silent/manifest#copy-silent",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/update-silent/manifest#copy-to-default-silent",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/update-silent/manifest#create-silent",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/update-silent/manifest#drop-default-silent",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/update-silent/manifest#drop-silent",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/update-silent/manifest#load-into-silent",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/update-silent/manifest#load-silent",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/update-silent/manifest#move-silent",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/update-silent/manifest#move-to-default-silent",
    // parser rejects valid input: graph-management grammar (23) — PR-U3
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_1",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_10",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_11",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_12",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_13",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_14",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_15",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_16",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_17",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_18",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_19",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_2",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_20",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_21",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_22",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_3",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_37",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_4",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_5",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_6",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_7",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_8",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_9",
    // parser rejects valid input: validator rejects GRAPH inside DELETE WHERE
    // (update class D, NOT graph-management grammar) (1) — PR-U1
    // parser rejects valid input: empty / prologue-only request must be a
    // valid no-op (update class C, NOT graph-management grammar) (3) — PR-U2
    // parser accepts invalid input (missing validation) (11) — PR-U1
    // parser accepts invalid input: cross-operation blank-node label reuse in
    // a multi-op `;` request (update class B validation) (1) — PR-U2
    // update eval: multi-operation `;` request executes only its first
    // operation (class B truncation, #1438) — NOT "INSERT into a
    // not-yet-existing named graph loses triples" and NOT "combined
    // DELETE/INSERT WHERE skips the deletes"; each single operation works
    // (5) — PR-U2 (+PR-U3 for the graph-management ops the same-bnode
    // requests also contain)
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/basic-update/manifest#insert-05a",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/basic-update/manifest#insert-data-same-bnode",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/basic-update/manifest#insert-where-same-bnode",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/basic-update/manifest#insert-where-same-bnode2",
    // update eval: USING + explicit GRAPH over-deletes from the default
    // graph (class F scoping) (2) — PR-U6, sequenced after PR-G1
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/delete/manifest#dawg-delete-using-02a",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/delete/manifest#dawg-delete-using-06a",
];

// Empty: jsonres01-04 went green once the Turtle lexer treated a trailing
// '.' after a blank-node label (`_:o6.`) as the statement terminator
// (#1444, roadmap PR-L1).
pub const SPARQL11_JSON_RES: &[&str] = &[];

// Fully green: csv03 was fixed by emitting the canonical xsd:double
// lexical form (1.0E6) from every RDF-lexical serializer (issue #1445).
pub const SPARQL11_CSV_TSV: &[&str] = &[];

// Requires RDFS/OWL/RIF entailment regimes — a deliberate non-goal
// (audit §4.4). The 21 simple-entailment-answerable tests that pass
// today stay enforced (owlds02 joined them once the bnode-dot lexer fix
// let its data load — #1444); revisit this register if reasoning support
// lands.
pub const SPARQL11_ENTAILMENT: &[&str] = &[
    // result mismatch (45)
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#bind07",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#lang",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#paper-sparqldl-Q1",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#paper-sparqldl-Q1-rdfs",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#paper-sparqldl-Q2",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#paper-sparqldl-Q3",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#paper-sparqldl-Q4",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#parent10",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#parent3",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#parent4",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#parent5",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#parent6",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#parent7",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#parent8",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#parent9",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#plainLit",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#rdf01",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#rdfs01",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#rdfs02",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#rdfs03",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#rdfs04",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#rdfs05",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#rdfs06",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#rdfs07",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#rdfs09",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#rdfs10",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#rdfs11",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#rif01",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#rif03",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#rif04",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#rif06",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#simple1",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#simple2",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#simple3",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#simple4",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#simple5",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#simple6",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#simple7",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#simple8",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#sparqldl-02",
    // sparqldl-03's data now loads (bnode-dot lexer fix, #1444) but the
    // expected solution requires OWL entailment: 1 expected, 0 found.
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#sparqldl-03",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#sparqldl-10",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#sparqldl-11",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#sparqldl-12",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#sparqldl-13",
    // query execution error (4)
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#sparqldl-06",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#sparqldl-07",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#sparqldl-08",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/entailment/manifest#sparqldl-09",
];

pub const SPARQL12_GROUPING: &[&str] = &[
    // result mismatch (1)
    "https://w3c.github.io/rdf-tests/sparql/sparql12/grouping/manifest#group01",
];

pub const SPARQL12_CODEPOINT_ESCAPES: &[&str] = &[];

pub const SPARQL12_SYNTAX_TRIPLE_TERMS_NEGATIVE: &[&str] = &[];

// SPARQL 1.2 triple-term *value* syntax (bare `<<( )>>` outside the
// object-of-rdf:reifies position, and the TRIPLE/SUBJECT/PREDICATE/
// OBJECT/isTRIPLE builtins). The reifier forms (buckets A/D) were
// greened by burn-down PR-W2A; the 27 entries below are buckets B/C,
// owned by sibling PR-W2BC under decision D-1 (accept-then-defer) —
// see docs/audit/burn-down/ROADMAP.md §2/§4 and
// docs/audit/burn-down/sparql12-wave2-triple-terms.md §1.2/§1.3.
pub const SPARQL12_SYNTAX_TRIPLE_TERMS_POSITIVE: &[&str] = &[
    // bucket B — triple-term builtins not in the lexer/AST (3)
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#expr-tripleterm-03",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#expr-tripleterm-04",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#expr-tripleterm-05",
    // bucket C — parser rejects bare triple-term values (24)
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#basic-tripleterm-01",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#basic-tripleterm-02",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#basic-tripleterm-03",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#basic-tripleterm-04",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#basic-tripleterm-05",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#basic-tripleterm-06",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#basic-tripleterm-07",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#bnode-tripleterm-01",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#bnode-tripleterm-02",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#bnode-tripleterm-03",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#compound-all",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#compound-tripleterm",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#compound-tripleterm-subject",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#expr-tripleterm-01",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#expr-tripleterm-06",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#inside-tripleterm-01",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#inside-tripleterm-02",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#nested-tripleterm-01",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#nested-tripleterm-02",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#subject-tripleterm",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#update-tripleterm-01",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#update-tripleterm-03",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#update-tripleterm-04",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest#update-tripleterm-05",
];

// Blocked on Turtle-star data loading and engine triple-term support —
// audit §4.3 / Phase D. Re-baseline this whole register after Turtle-star
// ingest (PR-W15) lands: the residual blockers are wave-2 query syntax /
// triple-term functions / CONSTRUCT projection / result serialization,
// scoped by the Option-1 first-class-triple-term epic (ROADMAP §2).
// Each test appears in EXACTLY ONE reason cluster (PR-1454 review found 10
// entries double-counted across clusters); attribution below re-verified
// empirically by unregistering and reading the harness's failure reasons.
pub const SPARQL12_EVAL_TRIPLE_TERMS: &[&str] = &[
    // data load: Turtle-star data won't parse (11)
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#basic-2",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#basic-3",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#basic-4",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#construct-1",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#construct-2",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#construct-3",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#pattern-1",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#pattern-2",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#pattern-7",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#pattern-8-nomatch",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#pattern-9",
    // data load: blocked on TriG GRAPH-block parsing, orthogonal to star
    // (D-8) — 'expected subject, found KwGraph' on data-4.trig (3)
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#expr-1",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#graphs-1",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#graphs-2",
    // data load: blocked on TriG GRAPH-block parsing, orthogonal to star
    // (D-8) (2)
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#update-1",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#update-2",
    // D-1 triple-terms-as-values: qt:data uses `<<( … )>>`, rejected by
    // ingest with the specific deferred error — Option-1 epic (10)
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#basic-8",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#basic-9",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#op-1",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#op-2",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#order-1",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#order-2",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#pattern-10",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#pattern-11",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#results-tripleterms-1j",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#results-tripleterms-1x",
    // query execution error (5)
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#basic-7",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#pattern-5",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#results-reifiedtriples-1j",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#results-reifiedtriples-1x",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#expr-2",
    // D-4 CONSTRUCT annotation projection: lowering returns
    // "CONSTRUCT projection of edge-annotation metadata" (1)
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#construct-4",
    // harness EXPECTED-graph parsing: the update/construct executes but
    // the expected file embeds star constructs the harness's collector
    // sink can't parse — construct-5 (`<<( )>>` in expected .ttl),
    // update-3 (TriG-star in expected .trig; its `{| |}` INSERT DATA
    // already executes — Option-1 epic scoping intel, roadmap §1.1-11) (2)
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#construct-5",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#update-3",
];

pub const SPARQL12_EXPRESSION: &[&str] = &[
    // result mismatch (1)
    "https://w3c.github.io/rdf-tests/sparql/sparql12/expression/manifest#not-not",
];

// Base-direction language literals (`@en--ltr`) — audit §4.3.
pub const SPARQL12_LANG_BASEDIR: &[&str] = &[
    // query execution error (8)
    "https://w3c.github.io/rdf-tests/sparql/sparql12/lang-basedir/manifest#concat",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/lang-basedir/manifest#contains",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/lang-basedir/manifest#datatype",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/lang-basedir/manifest#haslang",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/lang-basedir/manifest#haslangdir",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/lang-basedir/manifest#langdir",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/lang-basedir/manifest#langdir-literal",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/lang-basedir/manifest#strlangdir",
    // result mismatch (2)
    "https://w3c.github.io/rdf-tests/sparql/sparql12/lang-basedir/manifest#lang",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/lang-basedir/manifest#strlang",
];

pub const SPARQL12_RDF11: &[&str] = &[];

pub const SPARQL12_SYNTAX: &[&str] = &[];

// VERSION itself already lexes and parses; these three fail on the bare
// `<< >>` reifier patterns in their query bodies (wave-2 triple-term
// syntax) — PR-W2A, not a VERSION-declaration gap.
pub const SPARQL12_VERSION: &[&str] = &[
    // parser rejects valid input (3)
];

// Not path-cardinality defects: pp34/pp35 are graph-cluster tests mis-filed
// under property-path (`GRAPH <ng-01.ttl> { ?s :p1* ?t }` — constant GRAPH IRI
// base-expansion vs exact-key registry miss, and `?g` bound as a literal).
// Owner: PR-BASE + PR-G1 (burn-down ROADMAP §6.1); the path closure itself
// already produces the expected `[a,b,b]` bag once the GRAPH block matches
// (residual-eval.md §2.2).
pub const SPARQL11_PROPERTY_PATH: &[&str] =
    &["http://www.w3.org/2009/sparql/docs/tests/data-sparql11/property-path/manifest#pp35"];

// SERVICE evaluation requires live external SPARQL endpoints, which a unit
// test environment cannot provide. Revisit with an in-process mock endpoint
// when federation execution lands (audit §4.4).
pub const SPARQL11_SERVICE: &[&str] = &[
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service/manifest#service1",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service/manifest#service2",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service/manifest#service3",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service/manifest#service4a",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service/manifest#service5",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service/manifest#service6",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service/manifest#service7",
];

// SPARQL Protocol conformance requires an HTTP client/server harness —
// not applicable to database-engine unit testing (audit §4.4).
pub const SPARQL11_PROTOCOL: &[&str] = &[
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_multiple_queries",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_multiple_updates",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_query_method",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_query_missing_direct_type",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_query_missing_form_type",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_query_non_utf8",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_query_syntax",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_query_wrong_media_type",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_update_dataset_conflict",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_update_get",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_update_missing_form_type",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_update_non_utf8",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_update_syntax",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_update_wrong_media_type",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_content_type_ask",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_content_type_construct",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_content_type_describe",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_content_type_select",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_dataset_default_graphs_get",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_dataset_default_graphs_post",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_dataset_full",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_dataset_named_graphs_get",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_dataset_named_graphs_post",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_get",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_multiple_dataset",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_post_direct",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_post_form",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#update_base_uri",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#update_dataset_default_graph",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#update_dataset_default_graphs",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#update_dataset_full",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#update_dataset_named_graphs",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#update_post_direct",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#update_post_form",
];

// Requires a running SPARQL server to introspect (audit §4.4).
pub const SPARQL11_SERVICE_DESCRIPTION: &[&str] = &[
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service-description/manifest#conforms-to-schema",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service-description/manifest#has-endpoint-triple",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service-description/manifest#returns-rdf",
];

// Graph Store Protocol requires HTTP endpoint infrastructure (audit §4.4).
pub const SPARQL11_HTTP_RDF_UPDATE: &[&str] = &[
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#delete__existing_graph",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#delete__nonexistent_graph",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#get_of_delete__existing_graph",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#get_of_post__after_noop",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#get_of_post__create__new_graph",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#get_of_post__existing_graph",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#get_of_post__multipart_formdata",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#get_of_put__default_graph",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#get_of_put__graph_already_in_store",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#get_of_put__initial_state",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#head_on_a_nonexisting_graph",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#head_on_an_existing_graph",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#post__create__new_graph",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#post__existing_graph",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#post__multipart_formdata",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#put__default_graph",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#put__graph_already_in_store",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#put__initial_state",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#put__mismatched_payload",
];
