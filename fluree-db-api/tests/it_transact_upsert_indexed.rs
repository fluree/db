//! Upsert replace-semantics against an indexed ledger.
//!
//! Regression coverage for the upsert-deletion subject pre-check in
//! `generate_upsert_deletions`: subjects absent from both the persisted
//! subject dictionary and novelty skip their existing-value lookups entirely
//! (a bound-subject scan for an unresolvable subject degrades to a full PSOT
//! predicate-partition walk — ~8 ms/flake on real ledgers). These tests pin
//! that the skip never drops retractions for subjects that DO exist:
//! persisted (base index), novelty-only (committed after the last index), and
//! brand-new subjects all in one transaction.
//!
//! Correctness assertions alone cannot see the skip switching *off* in the
//! absent-subject direction — failing open is conservatively correct, costing
//! only extra queries — so that direction is asserted on the
//! `skipped_subjects` / `pattern_queries` counters in the standalone
//! `it_upsert_skip_fires` binary (span capture needs its own process).

#![cfg(feature = "native")]

use crate::support::{
    normalize_rows, query_jsonld_formatted, start_background_indexer_local,
    trigger_index_and_wait_outcome,
};
use fluree_db_api::{FlureeBuilder, IndexConfig};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;

fn ctx() -> serde_json::Value {
    json!({"ex": "http://example.org/ns/"})
}

async fn name_values(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    subject: &str,
) -> Vec<serde_json::Value> {
    let query = json!({
        "@context": ctx(),
        "where": {"@id": subject, "ex:name": "?n"},
        "select": ["?n"]
    });
    let rows = query_jsonld_formatted(fluree, ledger, &query)
        .await
        .expect("query");
    normalize_rows(&rows)
}

const GRAPH_ALPHA: &str = "http://example.org/graphs/alpha";
const GRAPH_BETA: &str = "http://example.org/graphs/beta";
const GRAPH_GAMMA: &str = "http://example.org/graphs/gamma";

async fn graph_name_values(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    ledger_id: &str,
    graph_iri: &str,
    subject: &str,
) -> Vec<String> {
    let query = json!({
        "@context": ctx(),
        "from": format!("{ledger_id}#{graph_iri}"),
        "where": {"@id": subject, "ex:name": "?n"},
        "select": "?n"
    });
    let results = fluree
        .query_connection(&query)
        .await
        .expect("named-graph query");
    let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    let mut vals: Vec<String> = results
        .as_array()
        .expect("array")
        .iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect();
    vals.sort();
    vals
}

/// One upsert touching a persisted subject, a novelty-only subject, and a
/// brand-new subject must replace values for the first two and plainly insert
/// the third — before and after the next index build.
#[tokio::test]
async fn upsert_mixed_persisted_novelty_new_subjects() {
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
            let ledger_id = "it/upsert-indexed-mixed:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();

            // Persisted subject with a multi-valued property (all values must
            // be retracted on upsert, not just one).
            let r1 = fluree
                .upsert_with_opts(
                    ledger,
                    &json!({
                        "@context": ctx(),
                        "@graph": [
                            {"@id": "ex:a", "ex:name": ["A1a", "A1b"]},
                            {"@id": "ex:b", "ex:name": "B1"}
                        ]
                    }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .unwrap();

            // Index + reload so ex:a / ex:b live in the binary index.
            trigger_index_and_wait_outcome(&handle, ledger_id, r1.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert!(ledger.snapshot.range_provider.is_some());

            // Novelty-only subject: committed after the index build.
            let r2 = fluree
                .upsert_with_opts(
                    ledger,
                    &json!({
                        "@context": ctx(),
                        "@id": "ex:c",
                        "ex:name": "C1"
                    }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .unwrap();

            // One upsert across all three subject classes.
            let r3 = fluree
                .upsert_with_opts(
                    r2.ledger,
                    &json!({
                        "@context": ctx(),
                        "@graph": [
                            {"@id": "ex:a", "ex:name": "A2"},
                            {"@id": "ex:c", "ex:name": "C2"},
                            {"@id": "ex:d", "ex:name": "D1"}
                        ]
                    }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .unwrap();

            let expect = |v: &str| vec![json!([v])];
            assert_eq!(
                name_values(&fluree, &r3.ledger, "ex:a").await,
                expect("A2"),
                "persisted subject: both old values replaced"
            );
            assert_eq!(
                name_values(&fluree, &r3.ledger, "ex:b").await,
                expect("B1"),
                "untouched subject keeps its value"
            );
            assert_eq!(
                name_values(&fluree, &r3.ledger, "ex:c").await,
                expect("C2"),
                "novelty-only subject: old value replaced"
            );
            assert_eq!(
                name_values(&fluree, &r3.ledger, "ex:d").await,
                expect("D1"),
                "new subject inserted"
            );

            // Same assertions after the next index build (retractions must
            // have been staged, not merely masked by novelty).
            trigger_index_and_wait_outcome(&handle, ledger_id, r3.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(name_values(&fluree, &ledger, "ex:a").await, expect("A2"));
            assert_eq!(name_values(&fluree, &ledger, "ex:b").await, expect("B1"));
            assert_eq!(name_values(&fluree, &ledger, "ex:c").await, expect("C2"));
            assert_eq!(name_values(&fluree, &ledger, "ex:d").await, expect("D1"));
        })
        .await;
}

/// Subjects whose IRI shape mints a **fresh namespace code per subject**.
///
/// Under `MostGranular` splitting, `urn:…:<id>:r:<sig>` splits at the last
/// `:`, so every such subject gets its own namespace prefix. The pre-check
/// resolves a subject's IRI in order to run the store's full-IRI lookup; when
/// the pre-transaction snapshot cannot decode the namespace code it falls back
/// to the store's own namespace table, and only reports "absent" when NEITHER
/// can decode it (a code neither knows cannot name a row in the base index).
///
/// This pins both directions of that decision: subjects of this shape that
/// already exist must still have their old values retracted, and brand-new
/// ones must still be skipped rather than dragging the whole transaction
/// through a per-(subject, predicate) scan.
///
/// The load-bearing case is `novel` — committed *after* the last index build,
/// so its namespace code IS in the pre-transaction snapshot while the store's
/// `find_subject_id` conclusively misses. `subject_in_base` therefore reports
/// absent and only the novelty backstop keeps it from being skipped. That is
/// the one direction where the fail-open removal could lose a retraction, and
/// it is visible in the results: a wrong skip leaves the old value behind.
#[tokio::test]
async fn upsert_indexed_replaces_values_for_per_subject_namespaces() {
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
            let ledger_id = "it/upsert-indexed-ns-per-subject:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();

            // `base` shares one namespace; the rest each mint their own.
            let base = "urn:it:ev:aaaa";
            let rev = "urn:it:ev:aaaa:r:sig1";
            let novel = "urn:it:ev:cccc:r:sig3";
            let fresh = "urn:it:ev:bbbb:r:sig2";

            let r1 = fluree
                .upsert_with_opts(
                    ledger,
                    &json!({
                        "@context": ctx(),
                        "@graph": [
                            {"@id": base, "ex:name": ["V1a", "V1b"]},
                            {"@id": rev, "ex:name": "R1"}
                        ]
                    }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .unwrap();

            trigger_index_and_wait_outcome(&handle, ledger_id, r1.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert!(ledger.snapshot.range_provider.is_some());

            // Novelty-only subject on a per-subject namespace: its code is
            // minted here, so the next transaction's snapshot can decode it
            // while the base index still has no row for it.
            let r2 = fluree
                .upsert_with_opts(
                    ledger,
                    &json!({"@context": ctx(), "@id": novel, "ex:name": "C1"}),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .unwrap();

            // Re-upsert the persisted and novelty-only subjects, plus a
            // brand-new one that also mints its own namespace (the skip path).
            let r3 = fluree
                .upsert_with_opts(
                    r2.ledger,
                    &json!({
                        "@context": ctx(),
                        "@graph": [
                            {"@id": base, "ex:name": "V2"},
                            {"@id": rev, "ex:name": "R2"},
                            {"@id": novel, "ex:name": "C2"},
                            {"@id": fresh, "ex:name": "N1"}
                        ]
                    }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .unwrap();

            let expect = |v: &str| vec![json!([v])];
            assert_eq!(
                name_values(&fluree, &r3.ledger, base).await,
                expect("V2"),
                "persisted subject on a shared namespace: both old values replaced"
            );
            assert_eq!(
                name_values(&fluree, &r3.ledger, rev).await,
                expect("R2"),
                "persisted subject on a per-subject namespace: old value replaced"
            );
            assert_eq!(
                name_values(&fluree, &r3.ledger, novel).await,
                expect("C2"),
                "novelty-only subject on a per-subject namespace: absent from the \
                 base index, so only the novelty backstop keeps it from being skipped"
            );
            assert_eq!(
                name_values(&fluree, &r3.ledger, fresh).await,
                expect("N1"),
                "brand-new per-subject-namespace subject inserted"
            );

            // Retractions must be staged, not merely masked by novelty.
            trigger_index_and_wait_outcome(&handle, ledger_id, r3.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(name_values(&fluree, &ledger, base).await, expect("V2"));
            assert_eq!(name_values(&fluree, &ledger, rev).await, expect("R2"));
            assert_eq!(name_values(&fluree, &ledger, novel).await, expect("C2"));
            assert_eq!(name_values(&fluree, &ledger, fresh).await, expect("N1"));
        })
        .await;
}

/// Post-index named-graph upsert. The subject pre-check keys its novelty set
/// per ledger graph while `subject_in_base` is graph-agnostic, and the
/// txn-graph → ledger-graph translation is hoisted out of the subject loop —
/// this pins that restructuring against a real binary index: the same subject
/// IRI upserted into two named graphs is replaced independently with no
/// cross-graph leakage, alongside a brand-new subject (skip path) and a graph
/// not yet in the ledger registry (nothing-to-retract path) in the same
/// transaction.
#[tokio::test]
async fn upsert_indexed_named_graphs_replace_independently() {
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
            let ledger_id = "it/upsert-indexed-named-graphs:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();

            // Same subject IRI in two named graphs, different values.
            let trig1 = r#"
                @prefix ex: <http://example.org/ns/> .
                GRAPH <http://example.org/graphs/alpha> { ex:s ex:name "A1" . }
                GRAPH <http://example.org/graphs/beta>  { ex:s ex:name "B1" . }
            "#;
            let r1 = fluree
                .stage_owned(ledger)
                .upsert_turtle(trig1)
                .index_config(index_cfg.clone())
                .execute()
                .await
                .unwrap();

            // Index + reload so both graphs live in the binary index.
            trigger_index_and_wait_outcome(&handle, ledger_id, r1.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert!(ledger.snapshot.range_provider.is_some());

            // One post-index upsert: ex:s replaced in alpha AND beta with
            // different values, a brand-new subject in beta, and ex:s in
            // gamma — a graph the ledger registry has never seen.
            let trig2 = r#"
                @prefix ex: <http://example.org/ns/> .
                GRAPH <http://example.org/graphs/alpha> { ex:s ex:name "A2" . }
                GRAPH <http://example.org/graphs/beta>  { ex:s ex:name "B2" . ex:t ex:name "T1" . }
                GRAPH <http://example.org/graphs/gamma> { ex:s ex:name "G1" . }
            "#;
            let r2 = fluree
                .stage_owned(ledger)
                .upsert_turtle(trig2)
                .index_config(index_cfg.clone())
                .execute()
                .await
                .unwrap();

            let expect = |v: &str| vec![v.to_string()];
            for stage in ["pre-index", "post-index"] {
                let ledger = if stage == "pre-index" {
                    r2.ledger.clone()
                } else {
                    trigger_index_and_wait_outcome(&handle, ledger_id, r2.receipt.t).await;
                    fluree.ledger(ledger_id).await.unwrap()
                };
                assert_eq!(
                    graph_name_values(&fluree, &ledger, ledger_id, GRAPH_ALPHA, "ex:s").await,
                    expect("A2"),
                    "{stage}: alpha value replaced, no leakage from beta/gamma"
                );
                assert_eq!(
                    graph_name_values(&fluree, &ledger, ledger_id, GRAPH_BETA, "ex:s").await,
                    expect("B2"),
                    "{stage}: beta value replaced, no leakage from alpha/gamma"
                );
                assert_eq!(
                    graph_name_values(&fluree, &ledger, ledger_id, GRAPH_BETA, "ex:t").await,
                    expect("T1"),
                    "{stage}: brand-new subject inserted in beta"
                );
                assert_eq!(
                    graph_name_values(&fluree, &ledger, ledger_id, GRAPH_GAMMA, "ex:s").await,
                    expect("G1"),
                    "{stage}: new graph gets its value, nothing retracted"
                );
            }
        })
        .await;
}
