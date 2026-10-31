//! The upsert-deletion subject pre-check must actually *fire*, not merely stay
//! correct.
//!
//! `it_transact_upsert_indexed.rs` pins that the pre-check never drops a real
//! retraction — but those assertions also pass against the old `decode_sid`
//! fail-open, because reporting "present" was conservatively correct: it only
//! cost extra queries and produced identical results. A results-only assertion
//! therefore cannot see the skip switching off, which is the regression that
//! matters. `generate_upsert_deletions` reports `skipped_subjects` and
//! `pattern_queries`; asserting on those is what makes it detectable.
//!
//! Its own `[[test]]` binary predates the shared capture helper: the capture
//! goes through `fluree_test_support::scoped_default`, so a sibling test can't
//! leave the `upsert deletion subject pre-check` callsite disabled, and the
//! file could join a group. See `docs/contributing/tests.md`.
#![cfg(feature = "native")]

mod support;

use crate::support::{
    span_capture, start_background_indexer_local, trigger_index_and_wait_outcome,
};
use fluree_db_api::{FlureeBuilder, IndexConfig};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;

fn ctx() -> serde_json::Value {
    json!({"ex": "http://example.org/ns/"})
}

#[tokio::test(flavor = "current_thread")]
async fn upsert_indexed_skip_fires_for_per_subject_namespace_subjects() {
    let (spans, _guard) = span_capture::init_test_tracing();

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let index_cfg = IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 1_000_000,
    };

    let mut fluree = FlureeBuilder::file(path).build().expect("build");
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/upsert-indexed-skip-fires:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();

            // Seed an unrelated subject so a base index exists to probe.
            let r1 = fluree
                .upsert_with_opts(
                    ledger,
                    &json!({"@context": ctx(), "@id": "urn:it:seed:aaaa", "ex:name": "S1"}),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .unwrap();
            trigger_index_and_wait_outcome(&handle, ledger_id, r1.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert!(ledger.snapshot.range_provider.is_some());

            // Three brand-new subjects, each minting its own namespace code.
            fluree
                .upsert_with_opts(
                    ledger,
                    &json!({
                        "@context": ctx(),
                        "@graph": [
                            {"@id": "urn:it:ev:aaaa:r:s1", "ex:name": "R1"},
                            {"@id": "urn:it:ev:bbbb:r:s2", "ex:name": "R2"},
                            {"@id": "urn:it:ev:cccc:r:s3", "ex:name": "R3"}
                        ]
                    }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .unwrap();

            let events = spans.find_events("upsert deletion subject pre-check");
            let last = events.last().expect(
                "generate_upsert_deletions must report its pre-check outcome; \
                 if this event was renamed or removed, update this assertion",
            );
            let field = |k: &str| -> u64 {
                last.fields
                    .get(k)
                    .unwrap_or_else(|| panic!("pre-check event missing field `{k}`"))
                    .parse()
                    .unwrap_or_else(|_| panic!("field `{k}` is not a number"))
            };

            assert_eq!(field("subject_count"), 3, "all three subjects are grouped");
            // The load-bearing assertions: absent subjects must be proven
            // absent and skipped, so no existing-value query is issued at all.
            assert_eq!(
                field("skipped_subjects"),
                3,
                "every absent subject must be skipped — a namespace code the \
                 snapshot cannot decode is not grounds to assume presence"
            );
            assert_eq!(
                field("pattern_queries"),
                0,
                "a skipped subject must issue no existing-value lookups"
            );
        })
        .await;
}

/// A ledger loaded from storage with an index and leftover novelty keeps its
/// dictionary novelty covering that novelty, through the load and through
/// later commits, so upserts of new subjects settle absence with a
/// dictionary probe instead of a seek per subject.
#[tokio::test(flavor = "current_thread")]
async fn indexed_ledger_load_keeps_dictionary_coverage() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let index_cfg = IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 1_000_000,
    };
    let ledger_id = "it/indexed-dict-coverage:main";

    let mut fluree = FlureeBuilder::file(path.clone()).build().expect("build");
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));
    local
        .run_until(async {
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();
            let r1 = fluree
                .upsert_with_opts(
                    ledger,
                    &json!({"@context": ctx(), "@id": "urn:it:seed:aaaa", "ex:name": "S1"}),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .unwrap();
            trigger_index_and_wait_outcome(&handle, ledger_id, r1.receipt.t).await;
        })
        .await;
    // A commit after the index, left in novelty.
    let ledger = fluree.ledger(ledger_id).await.unwrap();
    fluree
        .upsert(
            ledger,
            &json!({"@context": ctx(), "@id": "urn:it:seed:bbbb", "ex:name": "S2"}),
        )
        .await
        .unwrap();
    drop(fluree);

    let fluree = FlureeBuilder::file(path).build().expect("reopen");
    let ledger = fluree.ledger(ledger_id).await.unwrap();
    assert!(
        ledger.snapshot.range_provider.is_some(),
        "loaded with its index"
    );
    assert!(
        !ledger.novelty.is_empty(),
        "loaded with novelty after the index"
    );
    assert!(
        ledger.dict_novelty.covers(ledger.novelty.t),
        "the load's dictionary novelty covers novelty"
    );
    let r = fluree
        .upsert(
            ledger,
            &json!({"@context": ctx(), "@id": "urn:it:new:cccc", "ex:name": "N1"}),
        )
        .await
        .unwrap();
    assert!(
        r.ledger.dict_novelty.covers(r.ledger.novelty.t),
        "and still covers it after a commit"
    );
}
