//! Regression: retracting a `@list` container must hydrate each
//! retraction's list-index metadata BEFORE any dedup/cancellation runs.
//!
//! ## Why this test exists
//!
//! `FlakeMeta.i` holds the list index for `@list` entries, and `Flake`'s
//! `Eq`/`Hash` includes `m`. So the asserted `(s, p, o, dt, m={i:k})`
//! flake at list position `k` is not equal to the same triple at list
//! position `k+1`. A retraction generated from a DELETE template that
//! doesn't specify the list index comes out with `m = None` — which
//! matches *nothing* in the index until
//! `hydrate_list_index_meta_for_retractions` looks up the asserted flake's
//! actual `m` and copies it onto the retraction.
//!
//! Hydration MUST run before any step that treats `Flake` identity as a
//! dedup/cancellation key (the mixed-mode `FlakeAccumulator`, the
//! pure-delete `FlakeAccumulator`, any downstream novelty apply step).
//! If hydration is deferred until after finalization, N raw retractions
//! with `m = None` all collapse to one survivor, and only one list entry
//! gets retracted.
//!
//! This test pins the correct timing: a wildcard DELETE WHERE over a
//! three-element `@list` retracts all three entries. It's the regression
//! target for the forthcoming streaming-WHERE refactor — any version of
//! `stage()` that loses the "hydrate before accumulate" guarantee will
//! fail this test.
//!
//! ## Note on duplicate-value list positions
//!
//! A diagnostic run against `["a","a","a"]` shows that today's JSON-LD
//! insert path collapses identical-value list items to a single flake.
//! That means the "hydrate-after-finalize collapses distinct list
//! positions with identical values" hazard is not currently reachable
//! through JSON-LD insert — but the architectural concern still holds
//! for any other code path that may produce such flakes (raw flake sink,
//! import pipeline, future insert semantics). Pre-hydration is the
//! correct design regardless of whether today's insert exposes the gap.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};

fn ctx() -> JsonValue {
    json!({
        "ex": "http://example.org/",
        "ex:items": { "@container": "@list" }
    })
}

async fn count_items(fluree: &fluree_db_api::Fluree, ledger: &fluree_db_api::LedgerState) -> usize {
    // SPARQL `SELECT (COUNT(*) AS ?c)` counts binding rows — each distinct
    // flake matching the pattern contributes one row, so list entries at
    // distinct positions with distinct values each get counted.
    let sparql = "\
        PREFIX ex: <http://example.org/> \
        SELECT (COUNT(*) AS ?c) WHERE { ex:alice ex:items ?o }";
    let result = support::query_sparql(fluree, ledger, sparql)
        .await
        .expect("sparql count");
    let jsonld = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");
    let arr = jsonld.as_array().expect("array result");
    if arr.is_empty() {
        return 0;
    }
    arr[0].as_u64().map(|v| v as usize).unwrap_or(0)
}

/// Three distinct `@list` entries, wildcard DELETE WHERE.
///
/// Each asserted flake carries a distinct `FlakeMeta.i` (0, 1, 2). The
/// DELETE template doesn't specify the list index, so raw retractions
/// come out with `m = None`. Hydration must fill each one's `m` from the
/// matching asserted flake BEFORE dedup/cancellation, otherwise the
/// retractions would fail to match the indexed assertions and the list
/// entries would silently remain.
#[tokio::test]
async fn wildcard_delete_retracts_all_distinct_list_entries() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/list-retract-distinct:main")
        .await
        .expect("create");

    let insert = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:items": ["a", "b", "c"]
    });
    let receipt = fluree.insert(ledger0, &insert).await.expect("insert");

    assert_eq!(
        count_items(&fluree, &receipt.ledger).await,
        3,
        "precondition: three distinct list entries asserted"
    );

    // Wildcard-shaped DELETE that omits the list index. Every retraction
    // comes out of `generate_retractions` with `m = None` — the only way
    // these match the asserted flakes (which have `m.i` set) is if
    // hydration runs before the retractions are consumed by the
    // accumulator / cancellation path.
    let delete_txn = json!({
        "@context": { "ex": "http://example.org/" },
        "where":  { "@id": "ex:alice", "ex:items": "?o" },
        "delete": { "@id": "ex:alice", "ex:items": "?o" }
    });
    let out = fluree
        .update(receipt.ledger, &delete_txn)
        .await
        .expect("wildcard retract");

    assert_eq!(
        count_items(&fluree, &out.ledger).await,
        0,
        "all three list entries must be retracted — surviving entries \
         indicate that retractions failed to match asserted flakes, \
         which happens when hydration doesn't populate `m.i` before \
         the retractions enter the dedup/novelty path"
    );
}

/// Companion to the three-entry case: retracting a single-entry `@list`
/// where the asserted flake has `m.i = 0`. Pins the hydration behavior
/// for the simplest case.
#[tokio::test]
async fn wildcard_delete_retracts_single_list_entry() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/list-retract-single:main")
        .await
        .expect("create");

    let insert = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:items": ["only"]
    });
    let receipt = fluree.insert(ledger0, &insert).await.expect("insert");
    assert_eq!(count_items(&fluree, &receipt.ledger).await, 1);

    let delete_txn = json!({
        "@context": { "ex": "http://example.org/" },
        "where":  { "@id": "ex:alice", "ex:items": "?o" },
        "delete": { "@id": "ex:alice", "ex:items": "?o" }
    });
    let out = fluree
        .update(receipt.ledger, &delete_txn)
        .await
        .expect("wildcard retract");

    assert_eq!(
        count_items(&fluree, &out.ledger).await,
        0,
        "single list entry must be retracted via wildcard DELETE"
    );
}
