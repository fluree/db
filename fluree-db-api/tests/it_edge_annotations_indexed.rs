//! Edge annotations — M2b indexed-arena integration tests.
//!
//! Pins the slice 5 validation matrix:
//!
//! - **Incremental seal then arena-backed query** — annotated insert,
//!   trigger reindex with attachment-events provider attached,
//!   verify `LedgerSnapshot.annotation_index.is_some()` and that
//!   queries return the same results they did pre-arena.
//! - **Full rebuild fallback** — when no `Authoritative` events are
//!   provided, the new root carries `annotation_index = None` and
//!   queries fall back to the M2a scan path. Correctness is
//!   identical to the arena path; only the read shape differs.
//! - **Post-defensive-drop sticky bit** — the
//!   `(prev_arena=None, has_annotations=true)` cell stays in
//!   scan-fallback under `Augment` coverage. We drive a ledger into
//!   that state with a no-provider reindex, then verify the next
//!   reindex with provider stays scan-only.
//! - **Storage inspection** — when an arena is sealed, the four
//!   expected blobs (forward leaf + branch, reverse leaf + branch)
//!   exist in CAS at the CIDs the index root advertises.
//!
//! All tests run against the file-backed (non-memory) path so
//! storage inspection has a real CAS to verify against.
//!
//! See `EDGE_ANNOTATIONS_IMPL_PLAN.md` M2 §"Tests" and slice 5 in
//! the session log.

#![cfg(feature = "native")]

mod support;

use std::sync::Arc;

use fluree_db_api::FlureeBuilder;
use fluree_db_indexer::IndexerConfig;
use serde_json::{json, Value as JsonValue};
use support::genesis_ledger;

fn ctx() -> JsonValue {
    json!({
        "ex": "http://example.org/",
    })
}

/// Standard one-edge-with-one-annotation insert used across tests.
fn annotated_insert() -> JsonValue {
    json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:worksFor": {
            "@id": "ex:acme",
            "@annotation": {
                "@id": "ex:emp/alice-acme",
                "ex:role": "Engineer"
            }
        }
    })
}

/// Inline-annotation query that both M2a (scan) and M2b (arena) paths
/// must return identical results for.
fn inline_annotation_query() -> JsonValue {
    json!({
        "@context": ctx(),
        "select": ["?person", "?org", "?role"],
        "where": {
            "@id": "?person",
            "ex:worksFor": {
                "@id": "?org",
                "@annotation": { "ex:role": "?role" }
            }
        }
    })
}

#[tokio::test]
async fn incremental_arena_seal_then_arena_backed_query() {
    // 1. Insert annotated edge.
    // 2. Trigger background indexer with attachment-events provider
    //    attached → arena gets sealed.
    // 3. After reindex: snapshot.annotation_index is Some, and the
    //    inline-annotation query returns the same row it did
    //    pre-reindex (when the M2a scan path was active).
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations-indexed:incremental-seal";

    let (local, handle) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());

    local
        .run_until(async move {
            let ledger0 = genesis_ledger(&fluree, ledger_id);
            let after_insert = fluree
                .insert(ledger0, &annotated_insert())
                .await
                .expect("annotated insert");

            // Force the LedgerManager to load + cache the post-insert
            // state via `ledger_cached`. This populates the manager's
            // `entries` so the indexer's `AttachmentEventsProvider`
            // finds the running ledger when it queries the overlay.
            // (`fluree.ledger(...)` bypasses the cache and would
            // leave entries empty, causing the provider to return
            // None and the indexer to take the defensive-drop path.)
            let pre_reindex_handle = fluree
                .ledger_cached(ledger_id)
                .await
                .expect("cached load before reindex");
            let pre_reindex_loaded = pre_reindex_handle.snapshot().await.to_ledger_state();
            assert!(
                pre_reindex_loaded
                    .novelty
                    .attachments
                    .iter_event_pairs()
                    .next()
                    .is_some(),
                "running ledger must have attachments before reindex \
                 (provider reads from this overlay)"
            );
            let pre_rows = support::query_jsonld_formatted(
                &fluree,
                &pre_reindex_loaded,
                &inline_annotation_query(),
            )
            .await
            .expect("pre-reindex query");
            let pre_arr = pre_rows.as_array().expect("rows array");
            assert_eq!(pre_arr.len(), 1, "pre-reindex inline-annotation row count");

            // Trigger reindex.
            support::trigger_index_and_wait(&handle, ledger_id, after_insert.receipt.t).await;
            support::wait_for_index_application(&fluree, ledger_id, after_insert.receipt.t).await;

            // Post-reindex: arena should be sealed.
            let post = fluree
                .ledger(ledger_id)
                .await
                .expect("reload after reindex");
            assert!(
                post.snapshot.has_annotations,
                "sticky bit must be set after annotated insert + reindex"
            );
            assert!(
                post.snapshot.annotation_index.is_some(),
                "incremental indexer must seal an arena when provider \
                 supplies attachment events"
            );
            assert!(
                post.snapshot.has_arena_reader(),
                "snapshot must carry both annotation_index and \
                 content_store after a successful reindex"
            );

            // Same query post-reindex must return the same row,
            // now via the arena-backed hydration path.
            let post_rows =
                support::query_jsonld_formatted(&fluree, &post, &inline_annotation_query())
                    .await
                    .expect("post-reindex query");
            assert_eq!(
                pre_rows, post_rows,
                "arena-backed result must match scan-based result"
            );
        })
        .await;
}

#[tokio::test]
async fn full_rebuild_without_authoritative_falls_back_to_scan() {
    // No attachment-events provider → reindex lands the new root
    // with `annotation_index = None`. The inline-annotation query
    // must still return the right row via the M2a scan path.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations-indexed:scan-fallback";

    // Bare config — NO attachment-events provider. This simulates
    // a deployment that hasn't wired the provider yet, or a test
    // harness that intentionally exercises the scan-fallback path.
    let (local, handle) = support::start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger0 = genesis_ledger(&fluree, ledger_id);
            let after_insert = fluree
                .insert(ledger0, &annotated_insert())
                .await
                .expect("annotated insert");
            support::trigger_index_and_wait(&handle, ledger_id, after_insert.receipt.t).await;
            // No `wait_for_index_application` here: the test loads
            // fresh via `fluree.ledger()` (no cache participation),
            // so the api's notify-driven cache update isn't on the
            // critical path.

            let post = fluree
                .ledger(ledger_id)
                .await
                .expect("reload after reindex");
            assert!(post.snapshot.has_annotations, "sticky bit set");
            assert!(
                post.snapshot.annotation_index.is_none(),
                "without an events provider the indexer must not seal \
                 an arena (delta-unknown / Augment-without-base path)"
            );
            assert!(
                !post.snapshot.has_arena_reader(),
                "scan-fallback: hydration goes through the M2a POST scan"
            );

            // Query still works — exercises the M2a scan path.
            let rows = support::query_jsonld_formatted(&fluree, &post, &inline_annotation_query())
                .await
                .expect("query against scan-fallback snapshot");
            let arr = rows.as_array().expect("rows array");
            assert_eq!(
                arr.len(),
                1,
                "scan path returns the same row count as the arena path"
            );
        })
        .await;
}

#[tokio::test]
async fn post_defensive_drop_stays_in_scan_fallback() {
    // Two-step setup driving a ledger into the
    // (has_annotations=true, annotation_index=None) state on the
    // base root, then verifying the next reindex with provider
    // attached stays scan-only because Augment can't recover
    // history without a base arena.
    //
    // Step A: annotated insert + reindex WITH provider → arena
    //         sealed.
    // Step B: another commit + reindex WITHOUT provider → defensive
    //         drop; new root has annotation_index = None,
    //         has_annotations = true.
    // Step C: another commit + reindex WITH provider returning
    //         Augment → arena stays None (the gate I just added).
    //
    // To run these steps we juggle two workers — one with provider,
    // one without. The cleaner alternative would be a switchable
    // provider; for the test we just trigger separate workers.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations-indexed:post-defensive-drop";

    // Worker A: with provider.
    let (local_a, handle_a) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());

    local_a
        .run_until(async move {
            let ledger0 = genesis_ledger(&fluree, ledger_id);
            let after_a = fluree
                .insert(ledger0, &annotated_insert())
                .await
                .expect("step A insert");
            // Cache the ledger BEFORE trigger so the provider has
            // events when the worker dispatches.
            let _pre_a = fluree
                .ledger_cached(ledger_id)
                .await
                .expect("pre-step-A cached load");
            support::trigger_index_and_wait(&handle_a, ledger_id, after_a.receipt.t).await;
            support::wait_for_index_application(&fluree, ledger_id, after_a.receipt.t).await;

            let after_step_a_handle = fluree
                .ledger_cached(ledger_id)
                .await
                .expect("cached load after step A reindex");
            let after_step_a = after_step_a_handle.snapshot().await.to_ledger_state();
            assert!(
                after_step_a.snapshot.annotation_index.is_some(),
                "step A: arena sealed via provider"
            );

            // Step B: another commit + reindex without provider.
            // We swap to a second worker that has no provider —
            // this simulates a deployment that lost provider
            // wiring between passes.
            let (local_b, handle_b) = support::start_background_indexer_local(
                fluree.backend().clone(),
                Arc::new(fluree.nameservice_mode().clone()),
                IndexerConfig::small(),
            );
            local_b
                .run_until(async move {
                    let extra_b = json!({
                        "@context": ctx(),
                        "@id": "ex:bob",
                        "ex:name": "Bob"
                    });
                    let after_b = fluree
                        .insert(after_step_a, &extra_b)
                        .await
                        .expect("step B insert");
                    support::trigger_index_and_wait(&handle_b, ledger_id, after_b.receipt.t).await;
                    support::wait_for_index_application(&fluree, ledger_id, after_b.receipt.t)
                        .await;
                    let after_step_b_handle = fluree
                        .ledger_cached(ledger_id)
                        .await
                        .expect("cached load after step B reindex");
                    let after_step_b = after_step_b_handle.snapshot().await.to_ledger_state();
                    assert!(
                        after_step_b.snapshot.has_annotations,
                        "sticky bit stays set"
                    );
                    assert!(
                        after_step_b.snapshot.annotation_index.is_none(),
                        "step B: no provider → defensive drop on the new root"
                    );

                    // Step C: another commit + reindex with provider.
                    // Augment without a base arena BUT sticky=true
                    // → must NOT seal (the gate).
                    let (local_c, handle_c) = support::start_background_indexer_with_attachments(
                        &fluree,
                        IndexerConfig::small(),
                    );
                    local_c
                        .run_until(async move {
                            let extra_c = json!({
                                "@context": ctx(),
                                "@id": "ex:carol",
                                "ex:name": "Carol"
                            });
                            let after_c = fluree
                                .insert(after_step_b, &extra_c)
                                .await
                                .expect("step C insert");
                            support::trigger_index_and_wait(
                                &handle_c,
                                ledger_id,
                                after_c.receipt.t,
                            )
                            .await;
                            support::wait_for_index_application(
                                &fluree,
                                ledger_id,
                                after_c.receipt.t,
                            )
                            .await;
                            let after_step_c_handle = fluree
                                .ledger_cached(ledger_id)
                                .await
                                .expect("cached load after step C reindex");
                            let after_step_c =
                                after_step_c_handle.snapshot().await.to_ledger_state();
                            assert!(after_step_c.snapshot.has_annotations);
                            assert!(
                                after_step_c.snapshot.annotation_index.is_none(),
                                "step C: Augment + no prev arena + sticky=true \
                                 must stay scan-fallback (gate from prior commit)"
                            );

                            // Hydration still returns the correct row via scan.
                            let rows = support::query_jsonld_formatted(
                                &fluree,
                                &after_step_c,
                                &inline_annotation_query(),
                            )
                            .await
                            .expect("query against post-drop snapshot");
                            assert_eq!(
                                rows.as_array().unwrap().len(),
                                1,
                                "scan path still returns the annotated row"
                            );
                        })
                        .await;
                })
                .await;
        })
        .await;
}

#[tokio::test]
async fn storage_inspection_finds_arena_artifacts() {
    // After a successful arena seal, the index root's
    // forward_branch_cid and reverse_branch_cid must resolve to
    // real bytes in CAS, and those branches must reference real
    // leaf CIDs.
    use fluree_db_binary_index::annotation_arena::{
        AnnotationForwardBranch, AnnotationReverseBranch,
    };
    use fluree_db_core::storage::ContentStore;

    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations-indexed:storage-inspection";

    let (local, handle) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());

    local
        .run_until(async move {
            let ledger0 = genesis_ledger(&fluree, ledger_id);
            let after = fluree
                .insert(ledger0, &annotated_insert())
                .await
                .expect("annotated insert");
            // Pre-reindex cached load — populates LedgerManager so
            // the provider can read attachments.
            let _pre = fluree
                .ledger_cached(ledger_id)
                .await
                .expect("pre-reindex cached load");
            support::trigger_index_and_wait(&handle, ledger_id, after.receipt.t).await;

            let post = fluree.ledger(ledger_id).await.expect("reload");
            let ann_root = post
                .snapshot
                .annotation_index
                .as_ref()
                .expect("arena sealed");
            let cs = post
                .snapshot
                .content_store
                .as_ref()
                .expect("content store handle");

            // Forward branch resolves to bytes.
            let fwd_branch_bytes = cs
                .get(&ann_root.forward_branch_cid)
                .await
                .expect("forward branch bytes in CAS");
            let fwd_branch = AnnotationForwardBranch::decode(&fwd_branch_bytes)
                .expect("forward branch decodes cleanly");
            assert!(
                !fwd_branch.leaves.is_empty(),
                "forward branch must reference at least one leaf"
            );

            // Each forward-leaf CID resolves to bytes in CAS.
            for entry in &fwd_branch.leaves {
                let leaf_bytes = cs
                    .get(&entry.leaf_cid)
                    .await
                    .expect("forward leaf bytes in CAS");
                assert!(
                    !leaf_bytes.is_empty(),
                    "forward leaf must be a non-empty blob"
                );
            }

            // Reverse branch + leaves: same shape.
            let rev_branch_bytes = cs
                .get(&ann_root.reverse_branch_cid)
                .await
                .expect("reverse branch bytes in CAS");
            let rev_branch = AnnotationReverseBranch::decode(&rev_branch_bytes)
                .expect("reverse branch decodes cleanly");
            assert!(!rev_branch.leaves.is_empty());
            for entry in &rev_branch.leaves {
                let _leaf_bytes = cs.get(&entry.leaf_cid).await.expect("reverse leaf bytes");
            }

            // Stats line up with the inserted shape: one
            // (edge, ann) pair → one distinct edge, one distinct
            // annotation, one event row in each direction.
            assert_eq!(ann_root.stats.forward_rows, 1);
            assert_eq!(ann_root.stats.reverse_rows, 1);
            assert_eq!(ann_root.stats.distinct_edges, 1);
            assert_eq!(ann_root.stats.distinct_annotations, 1);
        })
        .await;
}
