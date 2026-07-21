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

// SPARQL 1.1 UPDATE syntax (syntax-update-1). Fully green:
// - the graph-management grammar (LOAD/CLEAR/CREATE/DROP/COPY/MOVE/ADD +
//   SILENT/INTO/DEFAULT/NAMED/ALL) now parses (PR-U3);
// - GRAPH-in-DELETE-WHERE (class D) was fixed by PR-U1;
// - empty / prologue-only requests (class C) and cross-operation blank-node
//   scope (class B) were fixed by PR-U2.
// These 23 graph-management tests were double-registered (also inside
// SPARQL11_UPDATE); PR-U3 removed both copies.
pub const SPARQL11_SYNTAX_UPDATE_1: &[&str] = &[];

pub const SPARQL10_SYNTAX: &[&str] = &[
    // parser accepts invalid input (missing validation) (13)
    // All remaining entries are V1 (load-bearing dots) / V2 (FILTER
    // Constraint) grammar-tightening territory — burn-down PR-3, not the
    // PR-2 semantic-validation passes (which cleared the V3 blabel/
    // breaks-BGP class).
];

pub const SPARQL11_AGGREGATES: &[&str] = &[
    // DOCUMENTED DIVERGENCE (decision D-6, second half — resolved by PR-U3):
    // this test requires enumerating a named graph that has ZERO triples
    // (`GRAPH ?g { SELECT (COUNT(*)...) }` must bind the empty `<empty.ttl>`
    // graph and return count 0). Fluree models a graph as a reserved g_id that
    // exists iff at least one flake carries it, so an empty named graph is
    // unrepresentable and non-enumerable — the same model fact that makes
    // DROP ≡ CLEAR harness-indistinguishable. Making empty graphs enumerable
    // would require a graph-existence record decoupled from flakes across
    // storage/commit/query-enumeration; deliberately NOT built (a query/
    // dataset-model concern orthogonal to the UPDATE verbs). Permanent
    // divergence. (1)
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#agg-empty-group-count-graph",
    // COUNT(DISTINCT *): DEFERRED — the parser accepts it but the lowerer
    // rejects it (lower/aggregate.rs); greening needs a new CountDistinctAll IR
    // variant + whole-row group-operator plumbing (the operators feed each
    // aggregate one input-var column, not the whole solution). Perf-neutral
    // (per-group, off the per-row hot path); a standalone post-wave-3 follow-up,
    // NOT X3 — PR-X2 (decision-owner).
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#agg-count-rows-distinct",
];

pub const SPARQL11_BINDINGS: &[&str] = &[
    // DOCUMENTED DIVERGENCE (decision D-6, second half — resolved by PR-U3):
    // requires enumerable empty named graphs, which Fluree does not model (a
    // graph exists iff a flake carries its g_id). See the fuller rationale on
    // `agg-empty-group-count-graph` in SPARQL11_AGGREGATES above. Permanent
    // divergence. (1)
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/bindings/manifest#graph",
];

pub const SPARQL11_CONSTRUCT: &[&str] = &[];

pub const SPARQL11_EXISTS: &[&str] = &[
    // result mismatch (1)
    // graph cluster: BASE-relative graph IRI resolution + EXISTS
    // active-graph inheritance (1) — PR-BASE + PR-G1 (conditional)
];

pub const SPARQL11_FUNCTIONS: &[&str] = &[
    // fully green: concat02 (CONCAT type-errors on a non-string argument) and
    // strlang03-rdf11 (case-insensitive language-tag comparison) — PR-X2
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
    // result-mismatch cluster; subgroup ownership + per-subgroup counts per
    // docs/audit/burn-down/ROADMAP.md §6.1 (absolute count omitted — it went
    // stale after the wave-2 register prune; the subgroup counts below are live)
    //
    // W-1 algebra cluster (3): join-combo-2 = GRAPH ?g default-graph
    // enumeration (PR-G1); nested-opt-1/2 = correlated-OPTIONAL independence
    // (PR-W1-OPT). filter-nested-2 (nested-group FILTER scope) and join-scope-1
    // (sub-SELECT merge of an OPTIONAL-produced correlation var) are fixed
    // (PR-W1 Families A/B).
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#join-combo-2",
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
    // quotes-3/4: D5b scan-path — pattern-object datatype drop (ninth-audit
    // reclassified these here from serialization); deferred with the scan-path
    // carve-out (open-eq-02 / eq-graph / dawg-lang-3) — PR-X2.
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/basic/manifest#quotes-3",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/basic/manifest#quotes-4",
    // dawg-bev-1..6: greened by the datatype-aware, fallible bare-variable EBV
    // (numeric-zero/empty-string falsy; ill-typed/lang/IRI/unbound → type error
    // excluding the row) — D-EBV, PR-X2
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/distinct/manifest#distinct-1",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/distinct/manifest#distinct-9",
    // dawg-lang-3: pattern-object language tag dropped on the scan path
    // (`?x :p "string"@EN` matches every lexical "string" regardless of @lang)
    // — D5b scan-path family, PR-X2 (owned with open-eq-02 / quotes-3/4)
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-lang-3",
    // dawg-langMatches-4: `!langMatches(lang(?v),"*")` where ?v is an IRI —
    // LANG of a non-literal must raise a type error that excludes the row (the
    // negation of an error is an error), not evaluate to "" — PR-X2 follow-up
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-langMatches-4",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-str-1",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-str-2",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#sameTerm-eq",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#sameTerm-not-eq",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#sameTerm-simple",
    // D5 datatype-aware `=`/`!=` (rdf_term_equal) greened eq-2-1/eq-2-2 (numeric
    // promotion + foreign-datatype distinctness) and open-eq-04. eq-4 remains:
    // its foreign literal arrives as a late-materialized EncodedLit (scan+filter
    // path), whose datatype-aware carry is on the binary-index hot path
    // (bench-sensitive, D5b class); the join-materialized Lit path is already
    // fixed — the same "zzz"^^:myType is correctly distinct in eq-2-1 — PR-X2.
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-equals/manifest#eq-4",
    // eq-dateTime: temporal `=` — a plain string vs xsd:dateTime and timezone-
    // instant handling; needs temporal value semantics beyond the filter lattice
    // — PR-X2 (temporal, deferred).
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-equals/manifest#eq-dateTime",
    // eq-graph-1/2/4: NOT filter equality — each is a bare BGP `{ ?x :p <const> }`
    // (no GRAPH keyword, no FILTER), so the constant OBJECT is matched on the scan
    // path, which ignores the exact term (`:p 1` also matches "01"/1.0e0). Same
    // D5b scan-path class as open-eq-02; the earlier "GRAPH-var / pr-g1" note was
    // a misnomer — PR-X2 (scan-path carve-out, deferred).
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-equals/manifest#eq-graph-1",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-equals/manifest#eq-graph-2",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-equals/manifest#eq-graph-4",
    // expr-ops {add,subtract,multiply,divide}-numbers-cast + unplus-2/unminus-2:
    // greened by D4 numeric promotion (xsd:float first-class, double∘decimal→
    // double) — PR-X2
    // date-1: xsd:date `=` — Fluree drops the timezone, so "2006-08-23" ≡
    // "2006-08-23Z" ≡ "2006-08-23+00:00"; needs temporal value semantics — PR-X2
    // (temporal, deferred).
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#date-1",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-01",
    // open-eq-02: D5b scan-path — the BGP object `"a"^^t:type1` matches
    // `"a"^^t:type2`; a deliberately-disabled per-flake scan datatype constraint,
    // deferred to protect the bench budget (spec-sanctioned carve-out) — PR-X2.
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-02",
    // open-eq-04 greened (D5 datatype-aware `=`/`!=`). open-eq-05/06 need BOTH the
    // scan-path EncodedLit datatype-carry (bench-sensitive, D5b class) AND typed-
    // literal *constants* to carry their datatype (lower_typed_literal drops it);
    // open-eq-07/08/10/11/12 now select the correct 12/42/52/52/10-row set but
    // stay non-isomorphic on blank-node OUTPUT identity (the same object bnode is
    // re-minted per binding) — orthogonal to the equality lattice.
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-05",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-06",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-07",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-08",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-10",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-11",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-12",
    // optional cluster (3): all three optional-complex-* use GRAPH ?x/?g and
    // are gated on PR-G1 (GRAPH-variable semantics). dawg-optional-filter-005's
    // doubly-nested `OPTIONAL { { ... FILTER } }` scope leak is fixed (PR-W1
    // Family A).
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional/manifest#dawg-optional-complex-2",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional/manifest#dawg-optional-complex-3",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional/manifest#dawg-optional-complex-4",
    // type-promotion-03/04/05/21 (double∘decimal, float-float/decimal/short
    // promotion) and the tP-29/30 "-fail" ASKs (double+decimal is double not
    // decimal; float+decimal is float not decimal) — D4 numeric promotion, PR-X2
    // query execution error: PR-BASE (relative-IRI/BASE resolution at lower
    // time); its lexer half lands in PR-1 (1)
];

// Fully green: the last cluster — USING + explicit GRAPH scoping (class F) —
// was greened by PR-U6. A `USING`/`USING NAMED` clause now defines the WHERE
// dataset exactly, so an explicit `GRAPH <g>` block matches nothing unless `g`
// is in the `USING NAMED` set (dawg-delete-using-02a/06a, #1441 — the block no
// longer over-reaches into a named graph scoped out by `USING`). The
// graph-management grammar (LOAD/CLEAR/CREATE/DROP/COPY/MOVE/ADD + SILENT), the
// same-bnode multi-op requests, and the double-registered syntax-update-1
// tests were greened earlier by PR-U3 (building on PR-U1's DELETE-WHERE-GRAPH
// and PR-U2's multi-operation `;` support) — audit §4.2.1, burn-down
// update-completeness.md.
pub const SPARQL11_UPDATE: &[&str] = &[];

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

// SPARQL 1.2 triple-term syntax is fully accepted (accept-then-defer,
// decision D-1): reifier forms (buckets A/D) by PR-W2A; the
// TRIPLE/SUBJECT/PREDICATE/OBJECT/isTRIPLE builtins (bucket B) and bare
// `<<( )>>` triple-term values (bucket C) by PR-W2BC. All parse + validate;
// evaluation is deferred to the first-class-triple-term epic (ROADMAP §2/§4,
// docs/audit/burn-down/sparql12-wave2-triple-terms.md §1.2/§1.3), so the
// sibling SPARQL12_EVAL_TRIPLE_TERMS register still stands.
pub const SPARQL12_SYNTAX_TRIPLE_TERMS_POSITIVE: &[&str] = &[];

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
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#op-1",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#op-2",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#order-1",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#order-2",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#basic-8",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#basic-9",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#pattern-10",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#pattern-11",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#results-tripleterms-1j",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#results-tripleterms-1x",
    // harness: the expected-result TriG-star graph fails to parse — the
    // `{| |}` INSERT DATA itself already executes (1)
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#update-3",
    // query execution error (5). (expr-1/graphs-1/graphs-2 also error at
    // execution, but each is registered once, above, under its data-load blocker.)
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#basic-7",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#pattern-5",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#results-reifiedtriples-1j",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#results-reifiedtriples-1x",
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#expr-2",
    // D-4 CONSTRUCT annotation projection: lowering returns "CONSTRUCT
    // projection of edge-annotation metadata is not supported in v1" (1)
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#construct-4",
    // harness EXPECTED-graph parsing: the expected .ttl embeds `<<( … )>>`
    // triple-term values the collector sink can't parse (secondary blockers
    // for update-3 above are the same class). Option-1 epic scoping intel,
    // roadmap §1.1-11. (1)
    "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest#construct-5",
];

pub const SPARQL12_EXPRESSION: &[&str] = &[
    // not-not: the D-EBV fix makes !!?v unbind for the language-tagged,
    // xsd:dateTime and IRI VALUES rows, but the `"z"^^xsd:boolean` row is stored
    // coerced to Boolean(false) — an ill-typed literal is canonicalized, losing
    // both its lexical form and its ill-typedness — so its EBV is (wrongly)
    // false, not a type error. Blocked on ill-typed-literal preservation (D6 /
    // PR-X3, decision D-11), not on the EBV logic.
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
