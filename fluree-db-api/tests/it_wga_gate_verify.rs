//! Regression pin for the `fast_whole_graph_agg` overlay-lane gate
//! (`overlay_lane_eligible`) — PR-1 L4.
//!
//! The overlay lane reconciles an `index_t`-pinned base count with a
//! `to_t`-bounded overlay delta. That sum is exact only when `to_t >= index_t`.
//! `overlay_lane_eligible` now enforces that relation (`to_t >= store.max_t()`);
//! before the fix it did not, so a `from_t = None` read whose `to_t` was pushed
//! below `index_t` while a live overlay stayed attached returned the `index_t`
//! snapshot count instead of the `to_t` count.
//!
//! Production time-travel goes through `load_graph_db_at_t`, which only attaches
//! a non-empty overlay when `target_t > index_t`, so the invariant already held
//! there. `GraphDb::as_of(t)` bypasses that loader — it overrides `to_t` on a
//! current-state view WITHOUT reloading the index or detaching the live overlay
//! — which is how this test reaches the gate below `index_t`. With the bound in
//! place the lane declines and the query falls through to the general pipeline,
//! so the fast fold AGREES with the barrier pipeline (both 2) at `to_t = 2`.

#![cfg(feature = "native")]
#![allow(clippy::needless_raw_string_hashes)]

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{genesis_ledger, rebuild_and_publish_index};

/// Count of distinct nodes via the fast fold vs. the `WITH n`-barrier general
/// pipeline, both against the SAME view.
async fn fold_and_barrier_count(
    fluree: &fluree_db_api::Fluree,
    db: &fluree_db_api::GraphDb,
) -> (i64, i64) {
    let fold_cj = fluree
        .query_cypher(db, "MATCH (n) RETURN count(n) AS c")
        .await
        .expect("fold count")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("fold cypher json");
    let truth_cj = fluree
        .query_cypher(db, "MATCH (n) WITH n RETURN count(n) AS c")
        .await
        .expect("barrier count")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("barrier cypher json");
    let fold = fold_cj["results"][0]["data"][0]["row"][0]
        .as_i64()
        .expect("fold i64");
    let truth = truth_cj["results"][0]["data"][0]["row"][0]
        .as_i64()
        .expect("truth i64");
    (fold, truth)
}

#[tokio::test]
async fn wga_overlay_lane_declines_below_index_t() {
    std::env::set_var("FLUREE_CYPHER_ALLOW_FULL_SCAN", "1");

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/wga:as-of-below-index";

    // One subject per commit so each lands at a distinct t:
    //   s1 @ t=1, s2 @ t=2, s3 @ t=3.
    let mut ledger = genesis_ledger(&fluree, ledger_id);
    for i in 1..=3u32 {
        ledger = fluree
            .insert(
                ledger,
                &json!({ "@graph": [ { "@id": format!("s{i}"), "@type": "Thing" } ] }),
            )
            .await
            .expect("seed insert")
            .ledger;
    }

    // Index the whole chain: index_t = 3, index holds s1, s2, s3.
    rebuild_and_publish_index(&fluree, ledger_id).await;

    // Post-index novelty: s4 @ t=4 stays in the live overlay (no reindex).
    let indexed = fluree.ledger(ledger_id).await.expect("indexed ledger");
    fluree
        .insert(
            indexed,
            &json!({ "@graph": [ { "@id": "s4", "@type": "Thing" } ] }),
        )
        .await
        .expect("novelty insert");

    // Current-state view: to_t = 4 (head), binary index at index_t = 3, live
    // overlay = { s4 @ t=4 }.
    let head_db = fluree.db(ledger_id).await.expect("head view");

    // Sanity: at HEAD the fold agrees with the pipeline (4 subjects), and the
    // overlay lane is genuinely exercised (index_t < head, novelty present, and
    // to_t = 4 >= max_t = 3 so the L4 bound holds and the lane runs).
    let (fold_head, truth_head) = fold_and_barrier_count(&fluree, &head_db).await;
    assert_eq!(truth_head, 4, "head pipeline: s1..s4");
    assert_eq!(fold_head, 4, "head overlay fold agrees at to_t >= index_t");

    // Time-travel BELOW the index via `as_of`: to_t = 2 < index_t = 3, keeping
    // the live overlay attached. Correct answer at t=2 is {s1, s2} = 2.
    let past_db = fluree.db(ledger_id).await.expect("past view").as_of(2);
    let (fold_past, truth_past) = fold_and_barrier_count(&fluree, &past_db).await;

    println!(
        "as_of(2), index_t=3: overlay-fold count(n) = {fold_past}, \
         general-pipeline count(n) = {truth_past} (correct = 2)"
    );

    // The general pipeline honors to_t = 2 and returns the correct 2.
    assert_eq!(
        truth_past, 2,
        "general pipeline (WITH n barrier) is correct at t=2"
    );

    // PR-1 L4 regression pin: the overlay lane now declines when to_t < index_t
    // (overlay_lane_eligible requires to_t >= store.max_t()), so the query falls
    // through to the general pipeline and the fold AGREES with the barrier —
    // before the fix it returned the index_t=3 snapshot count here.
    assert_eq!(
        fold_past, truth_past,
        "L4: overlay lane must decline below index_t so the fold matches the pipeline"
    );
    assert_eq!(
        fold_past, 2,
        "at to_t = 2 < index_t = 3 the count is {{s1, s2}} = 2, not the index_t=3 snapshot"
    );
}
