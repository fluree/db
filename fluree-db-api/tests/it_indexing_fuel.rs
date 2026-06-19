//! End-to-end checks that indexer CAS writes are billed against a fuel tracker
//! and that the resulting fuel surfaces through `IndexResult`.
//!
//! These tests cover the wrapper-based fuel pipeline added on top of #1255:
//! `MeteredContentStore` charges each CAS write, the two FLI3 leaf upload
//! sites add the per-leaflet extra charge, and the entry-point stamp surfaces
//! the tally on the returned `IndexResult`.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{Fluree, FlureeBuilder, LedgerState, Novelty};
use fluree_db_core::tracking::{Tracker, TrackingOptions};
use fluree_db_core::LedgerSnapshot;
use fluree_db_indexer::{
    build_index_for_record_with_tracker, rebuild_index_from_commits_with_tracker, IndexerConfig,
};
use serde_json::json;

async fn insert_one(fluree: &Fluree, ledger: LedgerState, tx: serde_json::Value) -> LedgerState {
    let outcome = fluree.insert(ledger, &tx).await.expect("insert");
    outcome.ledger
}

fn enabled_tracker() -> Tracker {
    Tracker::new(TrackingOptions {
        track_fuel: true,
        ..Default::default()
    })
}

#[tokio::test]
async fn reindex_with_tracker_reports_positive_fuel() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/indexing-fuel:main";

    let db0 = LedgerSnapshot::genesis(ledger_id);
    let mut state = LedgerState::new(db0, Novelty::new(0));

    // Seed a couple of small commits so the rebuild has real CAS work.
    state = insert_one(
        &fluree,
        state,
        json!({
            "@context": {"ex": "http://example.org/"},
            "@graph": [
                {"@id": "ex:a", "@type": "ex:Person", "ex:name": "Alice"},
                {"@id": "ex:b", "@type": "ex:Person", "ex:name": "Bob"},
            ]
        }),
    )
    .await;
    let _ = insert_one(
        &fluree,
        state,
        json!({
            "@context": {"ex": "http://example.org/"},
            "@graph": [
                {"@id": "ex:c", "@type": "ex:Person", "ex:name": "Carol"},
            ]
        }),
    )
    .await;

    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("ns lookup")
        .expect("record");

    let tracker = enabled_tracker();
    let result = rebuild_index_from_commits_with_tracker(
        fluree.content_store(ledger_id),
        tracker,
        ledger_id,
        &record,
        IndexerConfig::default(),
    )
    .await
    .expect("rebuild succeeds");

    let fuel = result.fuel.expect("tracker was enabled");
    assert!(
        fuel > 0.0,
        "rebuild over real commits must charge non-zero fuel; got {fuel}"
    );
}

#[tokio::test]
async fn build_index_for_record_already_current_reports_zero_fuel() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/indexing-fuel-current:main";

    let db0 = LedgerSnapshot::genesis(ledger_id);
    let mut state = LedgerState::new(db0, Novelty::new(0));
    state = insert_one(
        &fluree,
        state,
        json!({
            "@context": {"ex": "http://example.org/"},
            "@graph": [{"@id": "ex:a", "@type": "ex:Person"}]
        }),
    )
    .await;
    let _ = state;

    // First reindex to make the index current.
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("ns lookup")
        .expect("record");
    let result = rebuild_index_from_commits_with_tracker(
        fluree.content_store(ledger_id),
        enabled_tracker(),
        ledger_id,
        &record,
        IndexerConfig::default(),
    )
    .await
    .expect("rebuild");
    fluree
        .publisher()
        .expect("publisher")
        .publish_index(ledger_id, result.index_t, &result.root_id)
        .await
        .expect("publish");

    // Now go through `build_index_for_record_with_tracker` — it should hit the
    // already-current early return and report Some(0.0) (no CAS work, but the
    // caller asked for fuel tracking).
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("ns lookup")
        .expect("record");
    let result = build_index_for_record_with_tracker(
        fluree.content_store(ledger_id),
        enabled_tracker(),
        &record,
        IndexerConfig::default(),
    )
    .await
    .expect("build");

    assert_eq!(
        result.fuel,
        Some(0.0),
        "already-current build with tracking enabled should report Some(0.0)"
    );
}

#[tokio::test]
async fn trigger_index_reports_positive_fuel() {
    use fluree_db_api::tx::IndexingMode;
    use fluree_db_api::TriggerIndexOptions;
    use support::start_background_indexer_local;

    let mut fluree = FlureeBuilder::memory().build_memory();
    let (local, indexer_handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().as_arc_indexing_nameservice().expect("test fluree has writable nameservice"),
        fluree_db_indexer::IndexerConfig::default(),
    );
    fluree.set_indexing_mode(IndexingMode::Background(indexer_handle));

    local
        .run_until(async move {
            let ledger = fluree
                .create_ledger("it-indexing-fuel-trigger")
                .await
                .expect("create_ledger");
            let tx = json!({
                "@context": {"ex": "http://example.org/"},
                "@graph": [
                    {"@id": "ex:a", "@type": "ex:Person", "ex:name": "Alice"},
                    {"@id": "ex:b", "@type": "ex:Person", "ex:name": "Bob"},
                ]
            });
            let result = fluree.insert(ledger, &tx).await.expect("insert");
            let committed_t = result.ledger.t();

            let trigger = fluree
                .trigger_index(
                    "it-indexing-fuel-trigger:main",
                    TriggerIndexOptions::default(),
                )
                .await
                .expect("trigger_index");

            assert!(trigger.index_t >= committed_t, "indexed to >= committed t");
            let fuel = trigger
                .fuel
                .expect("background indexer always populates fuel");
            assert!(
                fuel > 0.0,
                "background indexer should charge non-zero fuel for real work; got {fuel}"
            );
        })
        .await;
}

#[tokio::test]
async fn non_tracked_rebuild_reports_fuel_none() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/indexing-fuel-none:main";

    let db0 = LedgerSnapshot::genesis(ledger_id);
    let mut state = LedgerState::new(db0, Novelty::new(0));
    state = insert_one(
        &fluree,
        state,
        json!({
            "@context": {"ex": "http://example.org/"},
            "@graph": [{"@id": "ex:a", "@type": "ex:Person"}]
        }),
    )
    .await;
    let _ = state;

    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("ns lookup")
        .expect("record");
    let result = fluree_db_indexer::rebuild_index_from_commits(
        fluree.content_store(ledger_id),
        ledger_id,
        &record,
        IndexerConfig::default(),
    )
    .await
    .expect("rebuild");

    assert_eq!(
        result.fuel, None,
        "rebuild via the non-tracking API should report fuel=None"
    );
}
