//! SPARQL parse errors must be authoritative at the API seam.
//!
//! The parser recovers from many syntax errors and still produces an AST;
//! `fluree.query` previously executed that recovered AST, silently answering
//! a different question than the user asked (ROADMAP §1 addendum: the API
//! swallowed error-severity diagnostics whenever an AST survived recovery).
//! These tests pin the fix: any error-severity parse diagnostic rejects the
//! query through the public API — for the V1 dot-structure and V2
//! FILTER-Constraint tightenings and for trailing tokens after a complete
//! query.

use crate::support;
use crate::support::{assert_index_defaults, genesis_ledger, MemoryFluree, MemoryLedger};
use fluree_db_api::FlureeBuilder;
use serde_json::json;

async fn seed_one_triple(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:s", "ex:p": "o"}]
    });
    fluree.insert(ledger0, &insert).await.expect("seed").ledger
}

/// Sanity: the well-formed spelling of the queries below executes.
#[tokio::test]
async fn sparql_well_formed_query_still_executes() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_one_triple(&fluree, "parse-errors:sanity").await;

    let result = support::query_sparql(
        &fluree,
        &ledger,
        "SELECT * WHERE { ?s ?p ?o . FILTER(isLiteral(?o)) }",
    )
    .await
    .expect("well-formed query should execute");
    let rows = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(rows.as_array().map(Vec::len), Some(1));
}

/// V1: a doubled dot is an error-severity diagnostic; the query must be
/// rejected even though parser recovery produces an AST for it.
#[tokio::test]
async fn sparql_v1_stray_dot_rejected_at_api() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_one_triple(&fluree, "parse-errors:v1-stray").await;

    let err = support::query_sparql(&fluree, &ledger, "SELECT * WHERE { ?s ?p ?o .. }")
        .await
        .expect_err("stray-dot query must be rejected, not executed");
    assert!(
        err.to_string().contains("unexpected '.'"),
        "error should carry the parser diagnostic, got: {err}"
    );
}

/// V1: a missing dot between two triple patterns is rejected.
#[tokio::test]
async fn sparql_v1_missing_dot_rejected_at_api() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_one_triple(&fluree, "parse-errors:v1-missing").await;

    let err = support::query_sparql(&fluree, &ledger, "SELECT * WHERE { ?s ?p ?o ?a ?b ?c }")
        .await
        .expect_err("missing-dot query must be rejected, not executed");
    assert!(
        err.to_string().contains("expected '.'"),
        "error should carry the parser diagnostic, got: {err}"
    );
}

/// V2: FILTER with a bare term (not a Constraint) is rejected.
#[tokio::test]
async fn sparql_v2_bare_filter_term_rejected_at_api() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_one_triple(&fluree, "parse-errors:v2").await;

    let err = support::query_sparql(&fluree, &ledger, "SELECT * WHERE { ?s ?p ?o FILTER ?o }")
        .await
        .expect_err("bare-FILTER query must be rejected, not executed");
    assert!(
        err.to_string().contains("FILTER requires"),
        "error should carry the parser diagnostic, got: {err}"
    );
}
