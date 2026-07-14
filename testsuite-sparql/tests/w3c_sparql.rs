//! W3C SPARQL test suite registration.
//!
//! Every manifest in the rdf-tests submodule is registered here and runs
//! unconditionally (`cargo test` — no `#[ignore]`). A suite is green when
//! each of its tests either passes or appears in that suite's skip register
//! (`tests/registers/mod.rs`), which `check_testsuite` polices in both
//! directions: unexpected failures AND stale register entries fail the suite.
//!
//! See `docs/contributing/sparql-compliance.md` for the workflow and
//! `docs/audit/2026-07-sparql-testsuite-audit.md` for the failure taxonomy
//! and burn-down plan.

use anyhow::Result;
use testsuite_sparql::check_testsuite;

mod registers;
use registers as reg;

// =============================================================================
// SPARQL 1.1 Syntax Tests (Query)
// =============================================================================

/// W3C SPARQL 1.1 syntax tests (positive + negative).
///
/// Tests only the parser — no query execution, no data loading.
#[test]
fn sparql11_syntax_query_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-query/manifest.ttl",
        reg::SPARQL11_SYNTAX_QUERY,
    )
}

// =============================================================================
// SPARQL 1.1 Syntax Tests (Update)
// =============================================================================

/// W3C SPARQL 1.1 update syntax tests (positive + negative).
#[test]
fn sparql11_syntax_update_1_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-update-1/manifest.ttl",
        reg::SPARQL11_SYNTAX_UPDATE_1,
    )
}

#[test]
fn sparql11_syntax_update_2_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-update-2/manifest.ttl",
        &[],
    )
}

// =============================================================================
// SPARQL 1.0 Syntax Tests
// =============================================================================

/// W3C SPARQL 1.0 syntax tests.
#[test]
fn sparql10_syntax_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-syntax.ttl",
        reg::SPARQL10_SYNTAX,
    )
}

// =============================================================================
// SPARQL 1.1 Federation Syntax Tests
// =============================================================================

/// W3C SPARQL 1.1 federation syntax tests (SERVICE keyword parsing).
#[test]
fn sparql11_federation_syntax_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-fed/manifest.ttl",
        &[],
    )
}

// =============================================================================
// SPARQL 1.1 Query Evaluation Tests — Per-Category
//
// Each category runs against an in-memory Fluree ledger through the public
// API surface (data load, query, result comparison).
// =============================================================================

#[test]
fn sparql11_aggregates() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/aggregates/manifest.ttl",
        reg::SPARQL11_AGGREGATES,
    )
}

#[test]
fn sparql11_bind() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/bind/manifest.ttl",
        &[],
    )
}

#[test]
fn sparql11_bindings() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/bindings/manifest.ttl",
        reg::SPARQL11_BINDINGS,
    )
}

#[test]
fn sparql11_cast() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/cast/manifest.ttl",
        &[],
    )
}

#[test]
fn sparql11_construct() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/construct/manifest.ttl",
        reg::SPARQL11_CONSTRUCT,
    )
}

#[test]
fn sparql11_exists() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/exists/manifest.ttl",
        reg::SPARQL11_EXISTS,
    )
}

#[test]
fn sparql11_functions() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/functions/manifest.ttl",
        reg::SPARQL11_FUNCTIONS,
    )
}

#[test]
fn sparql11_grouping() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/grouping/manifest.ttl",
        reg::SPARQL11_GROUPING,
    )
}

#[test]
fn sparql11_negation() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/negation/manifest.ttl",
        &[],
    )
}

#[test]
fn sparql11_project_expression() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/project-expression/manifest.ttl",
        reg::SPARQL11_PROJECT_EXPRESSION,
    )
}

#[test]
fn sparql11_property_path() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/property-path/manifest.ttl",
        reg::SPARQL11_PROPERTY_PATH,
    )
}

#[test]
fn sparql11_subquery() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/subquery/manifest.ttl",
        reg::SPARQL11_SUBQUERY,
    )
}

// =============================================================================
// SPARQL 1.0 Query Evaluation Tests
// =============================================================================

/// All SPARQL 1.0 query evaluation tests (24 categories).
///
/// Categories: basic, triple-match, open-world, algebra, bnode-coreference,
/// optional, optional-filter, graph, dataset, type-promotion, cast,
/// boolean-effective-value, bound, expr-builtin, expr-ops, expr-equals,
/// regex, i18n, construct, ask, distinct, sort, solution-seq, reduced.
#[test]
fn sparql10_query_eval_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-evaluation.ttl",
        reg::SPARQL10_QUERY_EVAL,
    )
}

// =============================================================================
// SPARQL 1.1 Update Tests (syntax + evaluation, all 13 categories)
// =============================================================================

/// SPARQL 1.1 update tests: add, basic-update, clear, copy, delete-data,
/// delete-insert, delete-where, delete, drop, move, syntax-update-1,
/// syntax-update-2, update-silent.
#[test]
fn sparql11_update_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-update.ttl",
        reg::SPARQL11_UPDATE,
    )
}

// =============================================================================
// SPARQL 1.1 Result Format Tests
// =============================================================================

/// SPARQL 1.1 JSON result format tests (.srj expected results).
#[test]
fn sparql11_json_result_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/json-res/manifest.ttl",
        reg::SPARQL11_JSON_RES,
    )
}

/// SPARQL 1.1 CSV/TSV result format tests.
#[test]
fn sparql11_csv_tsv_result_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/csv-tsv-res/manifest.ttl",
        reg::SPARQL11_CSV_TSV,
    )
}

// =============================================================================
// SPARQL 1.1 Federation SERVICE Tests
// =============================================================================

/// SPARQL 1.1 SERVICE federation evaluation tests.
///
/// All entries are registered as skips: they require external SPARQL
/// endpoints. Enable incrementally (mock endpoint) when federation
/// execution support is added.
#[test]
fn sparql11_service_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/service/manifest.ttl",
        reg::SPARQL11_SERVICE,
    )
}

// =============================================================================
// SPARQL 1.1 Protocol / Service Description / Graph Store Protocol Tests
//
// Not applicable to a database engine (they exercise HTTP conformance).
// Registered so the suite inventory is complete; every test is in the
// register with that rationale.
// =============================================================================

#[test]
fn sparql11_protocol_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/protocol/manifest.ttl",
        reg::SPARQL11_PROTOCOL,
    )
}

#[test]
fn sparql11_service_description_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/service-description/manifest.ttl",
        reg::SPARQL11_SERVICE_DESCRIPTION,
    )
}

#[test]
fn sparql11_http_rdf_update_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/http-rdf-update/manifest.ttl",
        reg::SPARQL11_HTTP_RDF_UPDATE,
    )
}

// =============================================================================
// SPARQL 1.1 Entailment Tests
// =============================================================================

/// SPARQL 1.1 entailment regime tests.
///
/// Fluree does not implement RDFS/OWL/RIF entailment regimes (deliberate
/// non-goal). The subset answerable under simple entailment passes and is
/// enforced; the rest is registered.
#[test]
fn sparql11_entailment_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/entailment/manifest.ttl",
        reg::SPARQL11_ENTAILMENT,
    )
}

// =============================================================================
// SPARQL 1.2 Test Suite (RDF-star / triple terms and other 1.2 features)
// =============================================================================

#[test]
fn sparql12_grouping() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql12/grouping/manifest.ttl",
        reg::SPARQL12_GROUPING,
    )
}

#[test]
fn sparql12_codepoint_escapes() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql12/codepoint-escapes/manifest.ttl",
        reg::SPARQL12_CODEPOINT_ESCAPES,
    )
}

#[test]
fn sparql12_syntax_triple_terms_negative() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-negative/manifest.ttl",
        reg::SPARQL12_SYNTAX_TRIPLE_TERMS_NEGATIVE,
    )
}

#[test]
fn sparql12_syntax_triple_terms_positive() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax-triple-terms-positive/manifest.ttl",
        reg::SPARQL12_SYNTAX_TRIPLE_TERMS_POSITIVE,
    )
}

#[test]
fn sparql12_eval_triple_terms() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql12/eval-triple-terms/manifest.ttl",
        reg::SPARQL12_EVAL_TRIPLE_TERMS,
    )
}

#[test]
fn sparql12_expression() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql12/expression/manifest.ttl",
        reg::SPARQL12_EXPRESSION,
    )
}

#[test]
fn sparql12_lang_basedir() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql12/lang-basedir/manifest.ttl",
        reg::SPARQL12_LANG_BASEDIR,
    )
}

#[test]
fn sparql12_rdf11() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql12/rdf11/manifest.ttl",
        reg::SPARQL12_RDF11,
    )
}

#[test]
fn sparql12_syntax() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql12/syntax/manifest.ttl",
        reg::SPARQL12_SYNTAX,
    )
}

#[test]
fn sparql12_version() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql12/version/manifest.ttl",
        reg::SPARQL12_VERSION,
    )
}
