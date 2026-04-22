//! End-to-end test for HostPlusN namespace split mode.
//!
//! Exercises the full pipeline (insert → index → reload → query) with
//! `NsSplitMode::HostPlusN(1)`, verifying that namespace encoding is
//! consistent across persist/reload cycles and that queries return
//! correct results under the non-default split strategy.
//!
//! All existing integration tests use `MostGranular` (default), so
//! this test covers the alternate path that the import pipeline enables
//! for high-cardinality namespace datasets.
//!
//! Run with:
//!   cargo test -p fluree-db-api --test it_host_plus_n_e2e --features native

#![cfg(feature = "native")]

use std::sync::Arc;
mod support;

use fluree_db_api::{FlureeBuilder, IndexConfig};
use fluree_db_core::NsSplitMode;
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::{start_background_indexer_local, trigger_index_and_wait_outcome};

/// Insert data under HostPlusN(1), index, rebuild from disk, and query.
///
/// With HostPlusN(1), IRIs like `http://example.org/ns/thing1` split as:
///   prefix = "http://example.org/"  (host + 1 path segment → "example.org" + "ns")
///   Wait, actually with HostPlusN(1): host + 1 path segment.
///   For "http://example.org/ns/thing1": scheme+authority = "http://example.org",
///   then 1 path segment = "/ns/", so prefix = "http://example.org/ns/", suffix = "thing1".
///
/// This differs from MostGranular which splits at the last `/` or `#`:
///   prefix = "http://example.org/ns/", suffix = "thing1" — same for this IRI.
///
/// The real difference shows with deeper paths:
///   "http://example.org/deep/nested/thing" under HostPlusN(1):
///     prefix = "http://example.org/deep/", suffix = "nested/thing"
///   vs MostGranular:
///     prefix = "http://example.org/deep/nested/", suffix = "thing"
#[tokio::test]
async fn host_plus_n_insert_index_reload_query() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let mut fluree = FlureeBuilder::file(path.clone()).build().expect("build");

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    let index_cfg = IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 1_000_000,
    };

    let ledger_id = "it/host-plus-n-e2e:main";

    local
        .run_until(async {
            // Create ledger and set HostPlusN(1) split mode on genesis snapshot.
            let mut ledger0 = fluree.create_ledger(ledger_id).await.unwrap();
            ledger0
                .snapshot
                .set_ns_split_mode(NsSplitMode::HostPlusN(1), 0)
                .expect("set HostPlusN(1) on genesis");

            // Insert data using multiple distinct namespaces (deep and shallow paths).
            let tx = json!({
                "@context": {
                    "ex": "http://example.org/ns/",
                    "deep": "http://example.org/deep/nested/"
                },
                "@graph": [
                    {
                        "@id": "ex:widget1",
                        "@type": "ex:Widget",
                        "ex:name": "Alpha",
                        "ex:score": 42
                    },
                    {
                        "@id": "deep:item1",
                        "@type": "deep:Item",
                        "deep:label": "Beta"
                    }
                ]
            });

            let r1 = fluree
                .insert_with_opts(
                    ledger0,
                    &tx,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .unwrap();

            // Verify the snapshot recorded HostPlusN(1).
            assert_eq!(
                r1.ledger.snapshot.ns_split_mode(),
                NsSplitMode::HostPlusN(1),
                "split mode should be HostPlusN(1) after first commit"
            );

            // Index at t=1.
            trigger_index_and_wait_outcome(&handle, ledger_id, r1.receipt.t).await;

            // Insert more data post-index (creates novelty on top of the indexed base).
            let ledger_indexed = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(
                ledger_indexed.snapshot.ns_split_mode(),
                NsSplitMode::HostPlusN(1),
                "split mode preserved after index + reload"
            );

            let tx2 = json!({
                "@context": {
                    "ex": "http://example.org/ns/",
                    "deep": "http://example.org/deep/nested/"
                },
                "@graph": [
                    {
                        "@id": "ex:widget2",
                        "@type": "ex:Widget",
                        "ex:name": "Gamma",
                        "ex:score": 99
                    },
                    {
                        "@id": "deep:item2",
                        "@type": "deep:Item",
                        "deep:label": "Delta"
                    }
                ]
            });
            let _r2 = fluree
                .insert_with_opts(
                    ledger_indexed,
                    &tx2,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .unwrap();

            // Rebuild from disk — forces full reload with sync_store_and_snapshot_ns.
            drop(fluree);
            let fluree2 = FlureeBuilder::file(path.clone()).build().expect("rebuild");
            let ledger_reloaded = fluree2.ledger(ledger_id).await.unwrap();

            // Verify split mode survived the round-trip.
            assert_eq!(
                ledger_reloaded.snapshot.ns_split_mode(),
                NsSplitMode::HostPlusN(1),
                "split mode must survive persist/reload"
            );

            // Query shallow namespace (ex:).
            let q_widgets = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?name ?score
                WHERE { ?w a ex:Widget ; ex:name ?name ; ex:score ?score }
                ORDER BY ?name
            ";
            let widgets = support::query_sparql(&fluree2, &ledger_reloaded, q_widgets)
                .await
                .unwrap()
                .to_jsonld(&ledger_reloaded.snapshot)
                .unwrap();
            assert_eq!(
                widgets,
                json!([["Alpha", 42], ["Gamma", 99]]),
                "widget query should return both indexed and novelty rows"
            );

            // Query deep namespace (deep:).
            let q_items = r"
                PREFIX deep: <http://example.org/deep/nested/>
                SELECT ?label
                WHERE { ?i a deep:Item ; deep:label ?label }
                ORDER BY ?label
            ";
            let items = support::query_sparql(&fluree2, &ledger_reloaded, q_items)
                .await
                .unwrap()
                .to_jsonld(&ledger_reloaded.snapshot)
                .unwrap();
            assert_eq!(
                items,
                json!(["Beta", "Delta"]),
                "deep namespace query should return both items"
            );

            // Cross-namespace counts (verify both types survive reload).
            let q_widget_count = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?c) WHERE { ?x a ex:Widget }
            ";
            let q_item_count = r"
                PREFIX deep: <http://example.org/deep/nested/>
                SELECT (COUNT(*) AS ?c) WHERE { ?x a deep:Item }
            ";
            let widget_count = support::query_sparql(&fluree2, &ledger_reloaded, q_widget_count)
                .await
                .unwrap()
                .to_jsonld(&ledger_reloaded.snapshot)
                .unwrap();
            let item_count = support::query_sparql(&fluree2, &ledger_reloaded, q_item_count)
                .await
                .unwrap()
                .to_jsonld(&ledger_reloaded.snapshot)
                .unwrap();
            assert_eq!(
                widget_count,
                json!([2]),
                "should find 2 widgets after reload"
            );
            assert_eq!(item_count, json!([2]), "should find 2 items after reload");
        })
        .await;
}
