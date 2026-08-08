//! Storage-sweep entry points on `Fluree`.

use fluree_db_api::FlureeBuilder;
use serde_json::json;

/// A ledger whose index chain is intact has nothing to reclaim, and planning
/// says so without touching storage.
#[tokio::test]
async fn planning_a_healthy_ledger_finds_no_orphans() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger("sweeptest").await.expect("create");
    let cached = fluree.ledger_cached("sweeptest").await.expect("cache");
    fluree
        .stage(&cached)
        .insert(&json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:a", "ex:v": 1}))
        .execute()
        .await
        .expect("insert");

    let plan = fluree
        .plan_index_sweep("sweeptest")
        .await
        .expect("planning succeeds");

    assert!(
        plan.orphans.is_empty(),
        "an intact ledger has nothing orphaned: {:?}",
        plan.orphans
    );
}

/// Sweeping names a ledger, not a branch. A `name:branch` argument would
/// silently sweep nothing, so it must not be mistaken for a valid target.
#[tokio::test]
async fn sweeping_an_unknown_ledger_reports_not_found() {
    let fluree = FlureeBuilder::memory().build_memory();

    let err = fluree
        .plan_index_sweep("nosuchledger")
        .await
        .expect_err("an unknown ledger has no branches to hold");

    assert!(
        err.to_string().contains("Not found"),
        "expected a not-found error, got: {err}"
    );
}

/// Reclaiming a healthy ledger is a no-op rather than an error, so operators
/// can run it on a schedule without special-casing the nothing-to-do case.
#[tokio::test]
async fn sweeping_a_healthy_ledger_reclaims_nothing() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger("sweeptest").await.expect("create");
    let cached = fluree.ledger_cached("sweeptest").await.expect("cache");
    fluree
        .stage(&cached)
        .insert(&json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:a", "ex:v": 1}))
        .execute()
        .await
        .expect("insert");

    let result = fluree
        .sweep_index_storage("sweeptest")
        .await
        .expect("sweeping succeeds");

    assert_eq!(result.reclaimed, 0);
    assert!(result.failures.is_empty(), "{:?}", result.failures);
}
