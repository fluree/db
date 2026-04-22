use anyhow::Result;
use testsuite_sparql::check_testsuite;

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
        &[],
    )
}

// =============================================================================
// SPARQL 1.1 Syntax Tests (Update)
// =============================================================================

/// W3C SPARQL 1.1 update syntax tests (positive + negative).
///
/// Update syntax uses the same SPARQL parser; this exercises .ru files.
/// Note: The update manifests also include UpdateEvaluationTest entries,
/// which will fail with "not yet implemented" — those are listed in ignored_tests.
#[test]
fn sparql11_syntax_update_1_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-update-1/manifest.ttl",
        &[],
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
        &[],
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
// Each runs the eval tests for one W3C category against an in-memory Fluree
// ledger. These are #[ignore]'d because many tests fail (unsupported features,
// missing functions, etc.). They will NOT block CI.
//
// Run individually:
//   cargo test -p testsuite-sparql sparql11_<category> -- --nocapture --include-ignored
//
// Run all eval tests:
//   cargo test -p testsuite-sparql -- --nocapture --include-ignored
//   make test-all    (from testsuite-sparql/)
// =============================================================================

#[test]
#[ignore = "eval: many failures expected — run with --include-ignored"]
fn sparql11_aggregates() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/aggregates/manifest.ttl",
        &[],
    )
}

#[test]
#[ignore = "eval: many failures expected — run with --include-ignored"]
fn sparql11_bind() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/bind/manifest.ttl",
        &[],
    )
}

#[test]
#[ignore = "eval: many failures expected — run with --include-ignored"]
fn sparql11_bindings() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/bindings/manifest.ttl",
        &[],
    )
}

#[test]
#[ignore = "eval: many failures expected — run with --include-ignored"]
fn sparql11_cast() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/cast/manifest.ttl",
        &[],
    )
}

#[test]
#[ignore = "eval: many failures expected — run with --include-ignored"]
fn sparql11_construct() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/construct/manifest.ttl",
        &[],
    )
}

#[test]
#[ignore = "eval: many failures expected — run with --include-ignored"]
fn sparql11_exists() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/exists/manifest.ttl",
        &[],
    )
}

#[test]
#[ignore = "eval: many failures expected — run with --include-ignored"]
fn sparql11_functions() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/functions/manifest.ttl",
        &[],
    )
}

#[test]
#[ignore = "eval: many failures expected — run with --include-ignored"]
fn sparql11_grouping() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/grouping/manifest.ttl",
        &[],
    )
}

#[test]
#[ignore = "eval: many failures expected — run with --include-ignored"]
fn sparql11_negation() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/negation/manifest.ttl",
        &[],
    )
}

#[test]
#[ignore = "eval: many failures expected — run with --include-ignored"]
fn sparql11_project_expression() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/project-expression/manifest.ttl",
        &[],
    )
}

#[test]
#[ignore = "eval: many failures expected — run with --include-ignored"]
fn sparql11_property_path() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/property-path/manifest.ttl",
        &[],
    )
}

#[test]
#[ignore = "eval: many failures expected — run with --include-ignored"]
fn sparql11_subquery() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/subquery/manifest.ttl",
        &[],
    )
}

// =============================================================================
// SPARQL 1.1 Full Query Evaluation Test Suite
// =============================================================================

/// All 12 SPARQL 1.1 query evaluation categories in a single run.
///
/// This is the top-level manifest that includes aggregates, bind, bindings,
/// cast, construct, exists, functions, grouping, negation, project-expression,
/// property-path, subquery, and syntax-query.
#[test]
#[ignore = "Full 1.1 eval suite (~5 min); use per-category tests or --include-ignored"]
fn sparql11_query_w3c_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-query.ttl",
        &[],
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
#[ignore = "eval: many failures expected — run with --include-ignored"]
fn sparql10_query_eval_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-evaluation.ttl",
        &[],
    )
}

// =============================================================================
// SPARQL 1.1 Update Tests
//
// Includes both update syntax tests (PositiveUpdateSyntaxTest11 /
// NegativeUpdateSyntaxTest11) and update evaluation tests
// (UpdateEvaluationTest). Syntax tests use the existing parser handlers.
// Evaluation tests fail with "not yet implemented" — this is expected.
// =============================================================================

/// SPARQL 1.1 update tests: all 13 categories via top-level manifest.
///
/// Categories: add, basic-update, clear, copy, delete-data, delete-insert,
/// delete-where, delete, drop, move, syntax-update-1, syntax-update-2,
/// update-silent.
#[test]
#[ignore = "update eval not yet implemented — run with --include-ignored"]
fn sparql11_update_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-update.ttl",
        &[],
    )
}

// =============================================================================
// SPARQL 1.1 Result Format Tests
// =============================================================================

/// SPARQL 1.1 JSON result format tests.
///
/// These are QueryEvaluationTest entries with .srj expected results.
#[test]
#[ignore = "eval: failures expected — run with --include-ignored"]
fn sparql11_json_result_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/json-res/manifest.ttl",
        &[],
    )
}

/// SPARQL 1.1 CSV/TSV result format tests.
///
/// Includes CSVResultFormatTest (not yet implemented) and QueryEvaluationTest
/// with .csv/.tsv expected results (not yet supported).
#[test]
#[ignore = "CSV/TSV comparison not yet implemented — run with --include-ignored"]
fn sparql11_csv_tsv_result_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/csv-tsv-res/manifest.ttl",
        &[],
    )
}

// =============================================================================
// SPARQL 1.1 Federation SERVICE Tests
//
// Deliberately ignored: these require external SPARQL endpoints (qt:serviceData
// / qt:endpoint) which cannot be provided in a unit test environment.
// All test IDs are listed in ignored_tests to prevent false failures.
// =============================================================================

/// SPARQL 1.1 SERVICE federation evaluation tests.
///
/// All tests are ignored because they require external SPARQL endpoints.
/// When federation support is added, tests should be enabled incrementally
/// with a mock endpoint or integration test environment.
#[test]
#[ignore = "requires external SPARQL endpoints — run with --include-ignored"]
fn sparql11_service_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/service/manifest.ttl",
        &[
            // All SERVICE tests require external SPARQL endpoints.
            // Enable incrementally when federation support is added.
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service/manifest#service1",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service/manifest#service2",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service/manifest#service3",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service/manifest#service4a",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service/manifest#service5",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service/manifest#service6",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service/manifest#service7",
        ],
    )
}

// =============================================================================
// SPARQL 1.1 Protocol / Service Description / Graph Store Protocol Tests
//
// These require HTTP server infrastructure and cannot run as unit tests.
// Registered for completeness — all tests are in ignored_tests.
// =============================================================================

/// SPARQL 1.1 protocol tests.
///
/// Require HTTP server and client infrastructure. Not applicable to
/// database engine unit testing.
#[test]
#[ignore = "requires HTTP protocol infrastructure"]
fn sparql11_protocol_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/protocol/manifest.ttl",
        &[
            // All protocol tests require HTTP server — not applicable to unit tests.
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_post_form",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_dataset_default_graphs_get",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_dataset_default_graphs_post",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_dataset_named_graphs_post",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_dataset_named_graphs_get",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_dataset_full",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_multiple_dataset",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_get",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_content_type_select",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_content_type_ask",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_content_type_describe",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_content_type_construct",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#update_dataset_default_graph",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#update_dataset_default_graphs",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#update_dataset_named_graphs",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#update_dataset_full",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#update_post_form",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#update_post_direct",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#update_base_uri",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#query_post_direct",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_query_method",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_multiple_queries",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_query_wrong_media_type",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_query_missing_form_type",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_query_missing_direct_type",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_query_non_utf8",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_query_syntax",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_update_get",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_multiple_updates",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_update_wrong_media_type",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_update_missing_form_type",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_update_non_utf8",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_update_syntax",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/protocol/manifest#bad_update_dataset_conflict",
        ],
    )
}

/// SPARQL 1.1 service description tests.
///
/// Require a running SPARQL server to introspect.
#[test]
#[ignore = "requires running SPARQL server"]
fn sparql11_service_description_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/service-description/manifest.ttl",
        &[
            // All service description tests require a running server.
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service-description/manifest#returns-rdf",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service-description/manifest#has-endpoint-triple",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service-description/manifest#conforms-to-schema",
        ],
    )
}

/// SPARQL 1.1 HTTP graph store protocol tests.
///
/// Require HTTP graph store endpoint infrastructure.
#[test]
#[ignore = "requires HTTP graph store infrastructure"]
fn sparql11_http_rdf_update_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/http-rdf-update/manifest.ttl",
        &[
            // All graph store protocol tests require HTTP infrastructure.
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#put__initial_state",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#get_of_put__initial_state",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#put__graph_already_in_store",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#get_of_put__graph_already_in_store",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#put__default_graph",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#get_of_put__default_graph",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#put__mismatched_payload",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#delete__existing_graph",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#get_of_delete__existing_graph",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#delete__nonexistent_graph",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#post__existing_graph",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#get_of_post__existing_graph",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#post__multipart_formdata",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#get_of_post__multipart_formdata",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#post__create__new_graph",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#get_of_post__create__new_graph",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#get_of_post__after_noop",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#head_on_an_existing_graph",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/http-rdf-update/manifest#head_on_a_nonexisting_graph",
        ],
    )
}

// =============================================================================
// SPARQL 1.1 Entailment Tests
//
// Require entailment regime support (RDFS, OWL, RIF) which Fluree does not
// implement. Registered for completeness.
// =============================================================================

/// SPARQL 1.1 entailment regime tests.
///
/// Require RDFS/OWL/RIF entailment reasoning. Fluree does not implement
/// entailment regimes. These tests are registered for completeness and
/// to track potential future support.
#[test]
#[ignore = "requires entailment regime support (RDFS/OWL/RIF)"]
fn sparql11_entailment_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/entailment/manifest.ttl",
        &[],
    )
}

// =============================================================================
// SPARQL 1.1 Complete Suite (all manifests)
// =============================================================================

/// Run the complete SPARQL 1.1 test suite: all manifests combined.
///
/// This is the ultimate compliance check — includes query, update,
/// federation, result formats, and all infrastructure tests.
/// Takes ~10 minutes. Use per-category tests for incremental work.
#[test]
#[ignore = "Complete 1.1 suite (~10 min); use per-category tests"]
fn sparql11_all() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-all.ttl",
        &[],
    )
}
