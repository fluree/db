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
    // inline `@annotation` form. Verify the actual binding values, not
    // just that the result executes — a zero-row regression must
    // fail this test.
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
    let committed = fluree
        .insert(ledger0, &txn)
        .await
        .expect("annotated insert");

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

    let rows = support::query_jsonld_formatted(&fluree, &committed.ledger, &query)
        .await
        .expect("M1b: inline annotation query executes against the f:reifies* facts");
    let arr = rows
        .as_array()
        .expect("Select result should render as a JSON array of row tuples");
    assert_eq!(
        arr.len(),
        1,
        "expected exactly one row binding (?person, ?org, ?role) for the annotated edge, got: {arr:#?}"
    );
    let row = arr[0]
        .as_array()
        .expect("each row is a tuple [?person, ?org, ?role]");
    assert_eq!(row.len(), 3, "row shape: {row:#?}");
    // Project format is: IRI columns render as raw strings; literal
    // columns render as their JSON-typed value. Match either string or
    // {"@id": ...} shape so the test is robust to JSON-LD formatter
    // changes.
    assert!(
        row_iri_matches(&row[0], "ex:alice", "http://example.org/alice"),
        "?person should bind to ex:alice, got: {:?}",
        row[0]
    );
    assert!(
        row_iri_matches(&row[1], "ex:acme", "http://example.org/acme"),
        "?org should bind to ex:acme, got: {:?}",
        row[1]
    );
    assert_eq!(
        row[2].as_str(),
        Some("Engineer"),
        "?role should bind to the annotation's ex:role string"
    );
}

/// Match a row column against either the compact IRI form (rendered
/// when the query's `@context` covers the namespace) or the full
/// expanded IRI, in either bare-string or `{"@id": "..."}` shape.
fn row_iri_matches(value: &serde_json::Value, compact: &str, expanded: &str) -> bool {
    let candidates = [compact, expanded];
    candidates.iter().any(|expect| {
        value.as_str() == Some(*expect)
            || value.get("@id").and_then(|v| v.as_str()) == Some(*expect)
    })
}

#[tokio::test]
async fn query_reifies_form_runs_with_visibility_check() {
    // Reverse-direction query (`@reifies`). Same row-content
    // verification as the inline form — the base edge triple emitted
    // alongside the f:reifies* lookups acts as the visibility check.
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
    let committed = fluree
        .insert(ledger0, &txn)
        .await
        .expect("annotated insert");

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

    let rows = support::query_jsonld_formatted(&fluree, &committed.ledger, &query)
        .await
        .expect("M1b: @reifies query executes via expansion into f:reifies* + base edge");
    let arr = rows.as_array().expect("Select result is an array");
    assert_eq!(arr.len(), 1, "expected one row, got: {arr:#?}");
    let row = arr[0]
        .as_array()
        .expect("each row is a tuple [?person, ?org]");
    assert_eq!(row.len(), 2);
    assert!(row_iri_matches(
        &row[0],
        "ex:alice",
        "http://example.org/alice"
    ));
    assert!(row_iri_matches(
        &row[1],
        "ex:acme",
        "http://example.org/acme"
    ));
}
