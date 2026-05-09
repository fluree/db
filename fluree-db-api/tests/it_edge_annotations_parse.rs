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
async fn query_with_annotation_parses_then_fails_at_execution() {
    // Seed a ledger with plain (non-annotated) data so the query has a
    // schema to compile against. The query should still fail at the
    // operator-tree assembly step, not at parse.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:query-inline";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let seed = json!({
        "@context": ctx(),
        "@graph": [
            { "@id": "ex:alice", "ex:worksFor": { "@id": "ex:acme" } }
        ]
    });
    let committed = fluree.insert(ledger0, &seed).await.expect("seed insert");

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

    let err = support::query_jsonld(&fluree, &committed.ledger, &query)
        .await
        .expect_err("M0: edge-annotation queries error at exec until M1");
    let msg = err.to_string();
    assert!(
        msg.contains("edge annotations") || msg.contains("Unsupported feature"),
        "error should mark the feature as deferred at the operator layer: {msg}"
    );
}

#[tokio::test]
async fn query_with_reifies_parses_then_fails_at_execution() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:query-reifies";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let seed = json!({
        "@context": ctx(),
        "@graph": [
            { "@id": "ex:alice", "ex:worksFor": { "@id": "ex:acme" } }
        ]
    });
    let committed = fluree.insert(ledger0, &seed).await.expect("seed insert");

    let query = json!({
        "@context": ctx(),
        "select": ["?person", "?org", "?since"],
        "where": {
            "ex:role": "Engineer",
            "ex:since": "?since",
            "@reifies": {
                "@id": "?person",
                "ex:worksFor": { "@id": "?org" }
            }
        }
    });

    let err = support::query_jsonld(&fluree, &committed.ledger, &query)
        .await
        .expect_err("M0: @reifies queries error at exec until M1");
    let msg = err.to_string();
    assert!(
        msg.contains("edge annotations") || msg.contains("Unsupported feature"),
        "error should mark the feature as deferred: {msg}"
    );
}
