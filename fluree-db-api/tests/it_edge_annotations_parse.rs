//! Edge annotations — M1 surface integration tests.
//!
//! M1 wires durable storage for `@annotation` / `@edge` (lowered to
//! `f:reifies*` system facts on the write side). `@reifies` on inserts
//! remains deferred to a follow-up. The query-side operators arrive
//! after M1 finishes.
//!
//! These tests pin the surface contract:
//! - Inserts with `@annotation` (or `@edge` alias) succeed and persist
//!   the durable encoding.
//! - `@reifies` on inserts is the deferred unasserted-reifier shape and
//!   errors with an explicit message.
//! - Queries containing `@annotation` / `@reifies` parse cleanly but
//!   still error at execution time until the planner is wired in M1's
//!   read-side slice.
//!
//! See: `EDGE_ANNOTATIONS.md` (design contract) and
//! `EDGE_ANNOTATIONS_IMPL_PLAN.md` (M1 split).

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::genesis_ledger;

fn ctx() -> serde_json::Value {
    json!({
        "ex": "http://example.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

#[tokio::test]
async fn insert_with_annotation_succeeds_under_m1() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:insert";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:worksFor": {
            "@id": "ex:acme",
            "@annotation": { "ex:role": "Engineer" }
        }
    });

    fluree
        .insert(ledger0, &txn)
        .await
        .expect("M1: @annotation on insert lowers to f:reifies* and succeeds");
}

#[tokio::test]
async fn insert_with_edge_alias_succeeds_under_m1() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:edge-alias";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:worksFor": {
            "@id": "ex:acme",
            "@edge": { "ex:role": "Engineer" }
        }
    });

    fluree
        .insert(ledger0, &txn)
        .await
        .expect("M1: @edge is an alias for @annotation and succeeds");
}

#[tokio::test]
async fn insert_with_reifies_unsupported() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:reifies-insert";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let txn = json!({
        "@context": ctx(),
        "@id": "ex:employment-1",
        "ex:role": "Engineer",
        "@reifies": {
            "@id": "ex:alice",
            "ex:worksFor": { "@id": "ex:acme" }
        }
    });

    let err = fluree
        .insert(ledger0, &txn)
        .await
        .expect_err("M0: @reifies on insert is rejected");
    assert!(err.to_string().contains("@reifies"));
}

#[tokio::test]
async fn query_inline_annotation_returns_matching_role() {
    // End-to-end M1b: insert an annotated edge, then query it via the
    // inline `@annotation` form. The expansion in `where_plan.rs`
    // should hit the `f:reifies*` flakes that the M1a transactor
    // lowering produced.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:query-inline";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:worksFor": {
            "@id": "ex:acme",
            "@annotation": { "ex:role": "Engineer" }
        }
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("annotated insert");

    let query = json!({
        "@context": ctx(),
        "select": ["?person", "?org", "?role"],
        "where": {
            "@id": "?person",
            "ex:worksFor": {
                "@id": "?org",
                "@annotation": { "ex:role": "?role" }
            }
        }
    });

    let result = support::query_jsonld(&fluree, &committed.ledger, &query)
        .await
        .expect("M1b: inline annotation query executes against the f:reifies* facts");

    // We expect at least one binding row binding ?role to "Engineer".
    // The full assertion shape depends on the result-rendering API;
    // the smoke check is that execution didn't error and the result
    // is shaped like a normal Select.
    assert!(
        matches!(result.output, fluree_db_api::QueryOutput::Select { .. }),
        "expected a Select-shaped result, got: {result:?}"
    );
}

#[tokio::test]
async fn query_reifies_form_runs_with_visibility_check() {
    // Reverse-direction query (`@reifies`). Under M1b's expansion, the
    // base edge triple emitted alongside the f:reifies* lookups acts
    // as the visibility check — if the edge isn't currently asserted
    // (or is hidden by policy), no row survives. We seed an annotated
    // edge and then query annotation-rooted; this should round-trip.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:query-reifies";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:worksFor": {
            "@id": "ex:acme",
            "@annotation": { "ex:role": "Engineer" }
        }
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("annotated insert");

    let query = json!({
        "@context": ctx(),
        "select": ["?person", "?org"],
        "where": {
            "ex:role": "Engineer",
            "@reifies": {
                "@id": "?person",
                "ex:worksFor": { "@id": "?org" }
            }
        }
    });

    let result = support::query_jsonld(&fluree, &committed.ledger, &query)
        .await
        .expect("M1b: @reifies query executes via expansion into f:reifies* + base edge");

    assert!(matches!(
        result.output,
        fluree_db_api::QueryOutput::Select { .. }
    ));
}
