//! A bound subject that is absent from the base index must not widen into a
//! predicate-partition walk.
//!
//! `BinaryScanOperator::open` sets `unresolved_bound_subject_iri` whenever a
//! bound subject fails to resolve to a persisted `s_id`, then switches the scan
//! to PSOT and re-checks every row of the predicate's partition by resolving its
//! subject IRI. That made a point query for a missing subject cost
//! O(partition size) with a dictionary lookup per row, while the same query for
//! a present subject is a single dictionary hit.
//!
//! The IRI is now probed against the store's subject dictionary first, so a
//! conclusive miss goes overlay-only instead. These tests pin the resulting cost
//! as **flat in the partition size** — an assertion that fails loudly if the
//! widening comes back, and that (unlike a wall-clock bound) is deterministic.
//!
//! Fuel is the measured quantity because it counts index touches directly, so a
//! widened scan shows up as a larger number without any wall-clock flakiness.
//! See `docs/design/query-execution.md` for the resolution rules these pin.
#![cfg(feature = "native")]

mod support;

use crate::support::{
    query_jsonld_tracked, start_background_indexer_local, trigger_index_and_wait_outcome,
};
use fluree_db_api::{FlureeBuilder, IndexConfig};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;

fn ctx() -> serde_json::Value {
    json!({"ex": "http://example.org/ns/"})
}

/// Build an indexed ledger holding `n` subjects that all share `ex:name`. When
/// `novel` is set, that subject is committed *after* the index build, so it
/// exists only in the overlay. Returns `(fuel, rows)` for a point query on
/// `probe_iri`.
async fn probe(n: usize, probe_iri: &str, novel: Option<&str>) -> (f64, serde_json::Value) {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let index_cfg = IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 1_000_000_000,
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

    let probe_iri = probe_iri.to_string();
    let novel = novel.map(str::to_string);

    local
        .run_until(async move {
            let ledger_id = "it/absent-subject-narrowing:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();

            let nodes: Vec<_> = (0..n)
                .map(|i| {
                    json!({
                        "@id": format!("http://example.org/ns/s{i}"),
                        "ex:name": format!("n{i}")
                    })
                })
                .collect();
            let r = fluree
                .upsert_with_opts(
                    ledger,
                    &json!({"@context": ctx(), "@graph": nodes}),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .unwrap();
            trigger_index_and_wait_outcome(&handle, ledger_id, r.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert!(
                ledger.snapshot.range_provider.is_some(),
                "test needs a base index to probe against"
            );

            // Optionally add a subject that exists ONLY in novelty.
            let ledger = match novel.as_deref() {
                Some(iri) => {
                    fluree
                        .upsert_with_opts(
                            ledger,
                            &json!({"@context": ctx(), "@id": iri, "ex:name": "NOVEL"}),
                            TxnOpts::default(),
                            CommitOpts::default(),
                            &index_cfg,
                        )
                        .await
                        .unwrap()
                        .ledger
                }
                None => ledger,
            };

            let q = json!({
                "@context": ctx(),
                "select": ["?o"],
                "where": {"@id": probe_iri, "ex:name": "?o"}
            });
            let tracked = query_jsonld_tracked(&fluree, &ledger, &q)
                .await
                .expect("query should succeed");
            (
                tracked.fuel.expect("tracked query reports fuel"),
                tracked.result,
            )
        })
        .await
}

/// Fuel only — for the cost-shape assertions.
async fn fuel_for_probe(n: usize, probe_iri: &str) -> f64 {
    probe(n, probe_iri, None).await.0
}

/// The cost of probing a missing subject must not grow with the size of the
/// predicate's partition. Before the narrowing fix this scaled linearly: the
/// scan walked every `ex:name` row and resolved its subject IRI.
#[tokio::test(flavor = "current_thread")]
async fn absent_subject_probe_cost_is_flat_in_partition_size() {
    let small = fuel_for_probe(500, "http://example.org/ns/ABSENT").await;
    let large = fuel_for_probe(4000, "http://example.org/ns/ABSENT").await;

    assert_eq!(
        small, large,
        "probing an absent subject burned more fuel against a larger partition \
         ({small} vs {large}) — the scan is walking the predicate partition \
         again instead of taking the conclusive dictionary miss"
    );
}

/// Same, for an IRI whose namespace prefix was never registered at all. This
/// takes a different arm of the filter build (no namespace code to translate),
/// so it needs its own guard.
#[tokio::test(flavor = "current_thread")]
async fn absent_subject_with_unknown_namespace_is_also_flat() {
    let small = fuel_for_probe(500, "http://never.seen.example/zzz").await;
    let large = fuel_for_probe(4000, "http://never.seen.example/zzz").await;

    assert_eq!(
        small, large,
        "unregistered-namespace subject probe scaled with partition size \
         ({small} vs {large})"
    );
}

/// The narrowing must not swallow real rows: a subject that exists only in
/// novelty is a conclusive *base* miss, and the overlay-only fallback is what
/// keeps it visible. If `Ok(None)` were treated as "no rows anywhere" this
/// returns empty.
#[tokio::test(flavor = "current_thread")]
async fn novelty_only_subject_still_resolves_through_the_fallback() {
    let novel = "http://example.org/ns/NOVEL_ONLY";
    let (_fuel, rows) = probe(500, novel, Some(novel)).await;

    let names = support::normalize_flat_results(&rows);
    assert_eq!(
        names,
        vec![json!(["NOVEL"])],
        "a subject committed after the last index build is absent from the base \
         dictionary, so the conclusive-miss arm must hand off to the overlay \
         rather than report no rows at all; got {rows}"
    );
}
