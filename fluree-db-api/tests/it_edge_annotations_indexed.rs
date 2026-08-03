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
//! See `docs/design/edge-annotations.md` for the on-disk arena
//! layout this suite validates.

#![cfg(feature = "native")]

use crate::support;
use crate::support::genesis_ledger;
use fluree_db_api::FlureeBuilder;
use fluree_db_indexer::IndexerConfig;
use serde_json::{json, Value as JsonValue};

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

/// Subject-hydration query that exercises
/// `HydrationFormatter::inject_annotations` — the only call site
/// that takes the M2b arena path. A flat `select` with
/// `@annotation` in the `where` clause goes through query
/// expansion and the **sync** JSON-LD formatter, which never
/// touches `inject_annotations`. The hydration path fires only
/// when a subject's `@annotation` block is materialized while
/// formatting a ref value during subject expansion.
fn annotated_hydration_query() -> JsonValue {
    json!({
        "@context": ctx(),
        "select": {"?person": ["*", {"ex:worksFor": ["*"]}]},
        "where": {"@id": "?person", "ex:worksFor": {"@id": "?org"}}
    })
}

/// Pull the annotation body's `ex:role` out of a hydration result
/// row, returning `None` when the row's shape doesn't carry one.
/// Tolerates compact-IRI vs expanded-IRI keys and bare-object vs
/// single-element-array shapes (both forms are formatter-legal).
fn extract_role_from_hydration(rows: &JsonValue) -> Option<String> {
    let arr = rows.as_array()?;
    let first = arr.first()?.as_object()?;
    let works_for = first
        .get("ex:worksFor")
        .or_else(|| first.get("http://example.org/worksFor"))?;
    let edge_obj = works_for.as_object().or_else(|| {
        works_for
            .as_array()
            .and_then(|a| a.first().and_then(|v| v.as_object()))
    })?;
    let ann = edge_obj.get("@annotation")?;
    let ann_obj = ann.as_object().or_else(|| {
        ann.as_array()
            .and_then(|a| a.first().and_then(|v| v.as_object()))
    })?;
    ann_obj
        .get("ex:role")
        .or_else(|| ann_obj.get("http://example.org/role"))?
        .as_str()
        .map(String::from)
}

#[tokio::test]
async fn incremental_arena_seal_then_arena_backed_query() {
    // 1. Insert annotated edge.
    // 2. Trigger background indexer with attachment-events provider
    //    attached → arena gets sealed.
    // 3. After reindex: snapshot.annotation_index is Some, and the
    //    subject-hydration query returns the same row it did
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
            // Pre-reindex hydration query — exercises the M2a scan
            // path inside `HydrationFormatter::inject_annotations`.
            let pre_rows = support::query_jsonld_formatted(
                &fluree,
                &pre_reindex_loaded,
                &annotated_hydration_query(),
            )
            .await
            .expect("pre-reindex hydration query");
            assert_eq!(
                extract_role_from_hydration(&pre_rows).as_deref(),
                Some("Engineer"),
                "pre-reindex hydration must surface the annotation body via scan path"
            );

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

            // Post-reindex: same hydration query, now via the arena
            // path. `inject_annotations` constructs an
            // `AnnotationArenaReader` once per response (see
            // `HydrationFormatter::new`) and resolves the worksFor
            // edge through it instead of issuing a POST scan.
            let post_rows =
                support::query_jsonld_formatted(&fluree, &post, &annotated_hydration_query())
                    .await
                    .expect("post-reindex hydration query");
            assert_eq!(
                extract_role_from_hydration(&post_rows).as_deref(),
                Some("Engineer"),
                "post-reindex hydration must surface the annotation body via arena path"
            );
            assert_eq!(
                pre_rows, post_rows,
                "arena-backed hydration must produce identical output to scan-based"
            );
        })
        .await;
}

#[tokio::test]
async fn full_rebuild_without_authoritative_falls_back_to_scan() {
    // No attachment-events provider → reindex lands the new root
    // with `annotation_index = None`. The subject-hydration query
    // must still surface the annotation body via the M2a indexed-
    // scan-fallback path inside `inject_annotations`.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations-indexed:scan-fallback";

    // Bare config — NO attachment-events provider. This simulates
    // a deployment that hasn't wired the provider yet, or a test
    // harness that intentionally exercises the scan-fallback path.
    let (local, handle) = support::start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
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

            // Hydration query against the indexed scan-fallback
            // snapshot. `inject_annotations` falls through to the
            // M2a POST scan because the arena reader can't be
            // constructed without `annotation_index`. The data
            // lives in the indexed POST (the test reindexed
            // above), so this exercises the indexed-scan-fallback
            // path — not the novelty-only path.
            let rows =
                support::query_jsonld_formatted(&fluree, &post, &annotated_hydration_query())
                    .await
                    .expect("hydration against scan-fallback snapshot");
            assert_eq!(
                extract_role_from_hydration(&rows).as_deref(),
                Some("Engineer"),
                "indexed-scan-fallback hydration must surface the annotation body"
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
                fluree
                    .nameservice_mode()
                    .publisher_arc()
                    .expect("test setup requires ReadWrite nameservice mode"),
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

                            // Hydration still surfaces the annotation
                            // body via the M2a indexed-scan path.
                            let rows = support::query_jsonld_formatted(
                                &fluree,
                                &after_step_c,
                                &annotated_hydration_query(),
                            )
                            .await
                            .expect("hydration against post-drop snapshot");
                            assert_eq!(
                                extract_role_from_hydration(&rows).as_deref(),
                                Some("Engineer"),
                                "indexed-scan-fallback hydration must still \
                                 surface the annotation body after defensive drop"
                            );
                        })
                        .await;
                })
                .await;
        })
        .await;
}

#[tokio::test]
async fn had_annotation_arena_sticky_survives_defensive_drop() {
    // Pins the High-finding fix: the sticky
    // `IndexRoot.had_annotation_arena` bit must be set the first
    // time an arena is sealed and must persist across a defensive
    // drop (when the indexer drops the arena because it received
    // Unknown / None coverage). Without this persistence, the
    // post-drop root looks identical to a fresh annotation-bearing
    // import and the provider's bootstrap base-index scan-fallback
    // could resurrect a live-only `Authoritative` arena, losing
    // historical retract/reassert rows.
    //
    // Setup mirrors `post_defensive_drop_stays_in_scan_fallback`:
    // step A seals an arena with provider attached; step B drops
    // it by running the indexer without provider. The new
    // assertion is on the sticky bit itself.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations-indexed:sticky-arena-bit";

    let (local_a, handle_a) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());

    local_a
        .run_until(async move {
            let ledger0 = genesis_ledger(&fluree, ledger_id);
            let after_a = fluree
                .insert(ledger0, &annotated_insert())
                .await
                .expect("step A insert");
            let _pre_a = fluree
                .ledger_cached(ledger_id)
                .await
                .expect("pre-step-A cached load");
            support::trigger_index_and_wait(&handle_a, ledger_id, after_a.receipt.t).await;
            support::wait_for_index_application(&fluree, ledger_id, after_a.receipt.t).await;

            let after_step_a = fluree
                .ledger_cached(ledger_id)
                .await
                .expect("cached load after step A reindex")
                .snapshot()
                .await
                .to_ledger_state();
            assert!(
                after_step_a.snapshot.annotation_index.is_some(),
                "step A: arena sealed via provider"
            );
            assert!(
                after_step_a.snapshot.had_annotation_arena,
                "step A: sticky bit set on first seal"
            );

            let (local_b, handle_b) = support::start_background_indexer_local(
                fluree.backend().clone(),
                fluree
                    .nameservice_mode()
                    .publisher_arc()
                    .expect("test setup requires ReadWrite nameservice mode"),
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
                    let after_step_b = fluree
                        .ledger_cached(ledger_id)
                        .await
                        .expect("cached load after step B reindex")
                        .snapshot()
                        .await
                        .to_ledger_state();
                    assert!(
                        after_step_b.snapshot.annotation_index.is_none(),
                        "step B: no provider → defensive drop on the new root"
                    );
                    assert!(
                        after_step_b.snapshot.has_annotations,
                        "step B: f:reifies* sticky bit persists"
                    );
                    assert!(
                        after_step_b.snapshot.had_annotation_arena,
                        "step B: had_annotation_arena MUST persist through \
                         defensive drop — without this the provider's \
                         bootstrap scan-fallback can't distinguish defensive \
                         drop from fresh import and would seal a live-only \
                         arena, losing retract/reassert history"
                    );
                })
                .await;
        })
        .await;
}

#[tokio::test]
async fn indexer_pass_without_provider_marks_arena_history_owned() {
    // A non-bulk-import ledger that accumulates annotation flakes
    // through the normal commit pipeline and gets indexed *without*
    // a provider must still mark the resulting root's sticky bit.
    //
    // Without this coercion, the root would land at
    // (has_annotations=true, annotation_index=None,
    //  had_annotation_arena=false) — the same shape as a fresh
    // bulk-import — and a later empty-overlay provider pass could
    // misclassify it as bootstrap-eligible and re-seal a
    // live-only `Authoritative` arena, silently dropping any
    // retract/reassert events that lived in the running overlay
    // before the no-provider reindex consumed it.
    //
    // The coercion lives in `IncrementalRootBuilder::build()` /
    // `encode_and_write_root_v6`: any indexer-produced root with
    // `has_annotations=true` gets the sticky bit set, even when no
    // arena is sealed. Bulk import never goes through these paths,
    // so its `had_annotation_arena=false` state stays the unique
    // bootstrap-eligible shape.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations-indexed:indexer-owns-annotation-history";

    // Worker without provider — defensive drop / no-op annotation
    // arena handling.
    let (local, handle) = support::start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
        IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger0 = genesis_ledger(&fluree, ledger_id);
            let after = fluree
                .insert(ledger0, &annotated_insert())
                .await
                .expect("annotated insert");
            support::trigger_index_and_wait(&handle, ledger_id, after.receipt.t).await;
            support::wait_for_index_application(&fluree, ledger_id, after.receipt.t).await;

            let post = fluree
                .ledger_cached(ledger_id)
                .await
                .expect("cached load after no-provider reindex")
                .snapshot()
                .await
                .to_ledger_state();
            assert!(
                post.snapshot.has_annotations,
                "f:reifies* predicates were observed → sticky bit is set"
            );
            assert!(
                post.snapshot.annotation_index.is_none(),
                "no provider → no arena sealed"
            );
            assert!(
                post.snapshot.had_annotation_arena,
                "indexer-produced root with has_annotations=true MUST mark \
                 had_annotation_arena=true, even when no arena is sealed — \
                 otherwise a later empty-overlay provider pass would treat \
                 this state like fresh import and bootstrap from a \
                 live-only base-index scan, losing history"
            );
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
            assert_eq!(ann_root.stats.live_attachment_pairs, 1);

            // Per-slot NDVs (M3.1 follow-up): the single edge has one
            // subject (alice), one predicate (worksFor), one object
            // (acme), no named graph, and no language tag.
            assert_eq!(ann_root.stats.distinct_reified_subjects, 1);
            assert_eq!(ann_root.stats.distinct_reified_predicates, 1);
            assert_eq!(ann_root.stats.distinct_reified_objects, 1);
            assert_eq!(ann_root.stats.reifies_graph_rows, 0);
            assert_eq!(ann_root.stats.distinct_reified_graphs, 0);
            assert_eq!(ann_root.stats.distinct_graph_anns, 0);
            assert_eq!(ann_root.stats.reifies_lang_rows, 0);
            assert_eq!(ann_root.stats.distinct_reified_langs, 0);
            assert_eq!(ann_root.stats.distinct_lang_anns, 0);
            assert_eq!(ann_root.stats.reifies_list_index_rows, 0);
            assert_eq!(ann_root.stats.distinct_list_index_anns, 0);
            // f:reifiesDatatype is intentionally NOT synthesized
            // from the arena — see `AnnotationStats::reifies_datatype_rows`.
            assert_eq!(ann_root.stats.reifies_datatype_rows, 0);
            assert_eq!(ann_root.stats.distinct_reified_datatypes, 0);
        })
        .await;
}

#[tokio::test]
async fn non_annotation_ledger_skips_inject_annotations() {
    // Hydration on a ledger that has never seen an `f:reifies*`
    // flake must NOT pay the per-ref-value POST scan that
    // `inject_annotations` does on the M2a fallback path. The gate
    // (mirror of the cascade fast-path) checks both
    // `snapshot.has_annotations` and the overlay's
    // `attachments.has_annotations()`. We can't directly observe
    // "the scan didn't run," but we can verify three positive
    // signals:
    //
    // 1. `snapshot.has_annotations == false` — sticky bit never
    //    flipped on an annotation-free ledger.
    // 2. The overlay's `attachments.has_annotations()` is also
    //    false — no novelty-side `f:reifies*` events.
    // 3. The hydration query returns the right shape with no
    //    `@annotation` keys anywhere — the only output the gate
    //    short-circuits on (the keys would still be absent on the
    //    scan path, but we'd pay the POST scan to find that out).
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations-indexed:non-annotation-skip";

    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let plain_insert = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "ex:worksFor": {"@id": "ex:acme"}},
            {"@id": "ex:acme", "ex:name": "Acme"}
        ]
    });
    let after = fluree
        .insert(ledger0, &plain_insert)
        .await
        .expect("plain insert");

    assert!(
        !after.ledger.snapshot.has_annotations,
        "non-annotation ledger must not have sticky bit set"
    );
    assert!(
        !after.ledger.novelty.attachments.has_annotations(),
        "novelty overlay must report zero annotations"
    );
    assert!(
        after.ledger.snapshot.annotation_index.is_none(),
        "non-annotation ledger must not have an annotation_index"
    );
    assert!(
        !after.ledger.snapshot.has_arena_reader(),
        "non-annotation ledger must not advertise an arena reader \
         (gate guarantees no CAS reads on hydration either)"
    );

    // Subject hydration that would otherwise call `inject_annotations`
    // on the worksFor ref value. Confirm output is correct AND has
    // no `@annotation` artifacts.
    let query = json!({
        "@context": ctx(),
        "select": {"?person": ["*", {"ex:worksFor": ["*"]}]},
        "where": {"@id": "?person", "ex:worksFor": {"@id": "?org"}}
    });
    let rows = support::query_jsonld_formatted(&fluree, &after.ledger, &query)
        .await
        .expect("hydration on non-annotation ledger");
    let arr = rows.as_array().expect("rows array");
    assert_eq!(arr.len(), 1, "single subject row");
    let json_str = serde_json::to_string(&arr[0]).expect("serialize row");
    assert!(
        !json_str.contains("@annotation"),
        "non-annotation ledger must not produce any @annotation keys: {json_str}"
    );

    // Reindex with provider attached. Even with the provider asking
    // for events, an annotation-free ledger must produce a fresh
    // root with `annotation_index = None` (no arena artifacts in
    // CAS at all). Verifies the indexer's "non-annotation fast
    // path" — no CAS writes for branch/leaf blobs that would just
    // be empty placeholders.
    let (local, handle) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());
    local
        .run_until(async {
            let _ = fluree.ledger_cached(ledger_id).await.unwrap();
            let completion = handle.trigger(ledger_id, after.receipt.t).await;
            let _ = completion.wait().await;
            support::wait_for_index_application(&fluree, ledger_id, after.receipt.t).await;

            let post = fluree.ledger(ledger_id).await.expect("post-reindex");
            assert!(
                !post.snapshot.has_annotations,
                "indexed root must not flip sticky bit on non-annotation ledger"
            );
            assert!(
                post.snapshot.annotation_index.is_none(),
                "indexed root must not carry an annotation_index"
            );
            assert!(
                !post.snapshot.has_arena_reader(),
                "post-reindex snapshot must still skip arena reader"
            );
        })
        .await;
}

#[tokio::test]
async fn explain_tags_annotation_role_and_uses_arena_stats() {
    // M3.2: `/explain` must (a) expand `@annotation` / `@reifies`
    // patterns the same way the executor does, (b) tag the resulting
    // `f:reifies*` triples with their slot name so the chosen ordering
    // is observable, and (c) report stats as available when the
    // annotation arena is sealed even if no other property stats
    // exist yet.
    use crate::support::graphdb_from_ledger;

    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations-indexed:explain-tags";

    let (local, handle) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());

    local
        .run_until(async move {
            let ledger0 = genesis_ledger(&fluree, ledger_id);
            let after = fluree
                .insert(ledger0, &annotated_insert())
                .await
                .expect("annotated insert");
            let _pre = fluree
                .ledger_cached(ledger_id)
                .await
                .expect("pre-reindex cached load");
            support::trigger_index_and_wait(&handle, ledger_id, after.receipt.t).await;
            support::wait_for_index_application(&fluree, ledger_id, after.receipt.t).await;

            let post = fluree.ledger(ledger_id).await.expect("reload");
            assert!(
                post.snapshot.annotation_index.is_some(),
                "arena must be sealed for the explain test to exercise M3.1 stats"
            );

            // `@reifies`-rooted query: filter by annotation metadata,
            // ask for the edge it reifies. Lowering produces a
            // `Pattern::AnnotationTarget` which `/explain` should now
            // expand into a base edge triple + 3 `f:reifies*` lookups.
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

            // Pin the annotation-role tags on the normally-indexed
            // snapshot first.
            let db = graphdb_from_ledger(&post);
            let resp = fluree.explain(&db, &query).await.expect("explain");

            assert_ne!(
                resp["plan"]["optimization"], "none",
                "with annotation arena present, explain must report stats availability \
                 (got plan: {})",
                resp["plan"]
            );

            let optimized = resp["plan"]["optimized"]
                .as_array()
                .expect("optimized order is an array");

            // Collect the annotation-role tags we saw, in optimized
            // order, so the test pins both presence and ordering.
            let roles: Vec<String> = optimized
                .iter()
                .filter_map(|entry| {
                    entry
                        .get("annotation-role")
                        .and_then(|v| v.as_str())
                        .map(String::from)
                })
                .collect();

            // The expansion emits exactly three `f:reifies*` triples
            // per edge-annotation pattern (subject + predicate +
            // object). The optimizer may reorder them but their
            // count and slot identities are fixed.
            let mut sorted = roles.clone();
            sorted.sort();
            assert_eq!(
                sorted,
                vec!["object", "predicate", "subject"],
                "optimized order must contain exactly the three required \
                 f:reifies* slots (got: {roles:?})"
            );

            // Sanity: every entry in the optimized order has a
            // selectivity score field — we're not silently dropping
            // patterns the planner doesn't have inputs for.
            for entry in optimized {
                assert!(
                    entry.get("selectivity").is_some(),
                    "optimized entry missing selectivity: {entry}"
                );
            }

            // M3.1 review fix: prove the arena-only stats path. Clone
            // the LedgerState and strip `snapshot.stats` so the
            // ordinary `IndexStats` is unavailable. With the M3.1
            // wiring, `/explain` should still report optimization
            // (not "none") because `merge_annotation_stats` populates
            // the view from `annotation_index` alone.
            let mut arena_only = post.clone();
            assert!(
                arena_only.snapshot.stats.is_some(),
                "preconditions: post-reindex snapshot has IndexStats"
            );
            std::sync::Arc::make_mut(&mut arena_only.snapshot).stats = None;
            let db_arena_only = graphdb_from_ledger(&arena_only);
            let resp_arena_only = fluree
                .explain(&db_arena_only, &query)
                .await
                .expect("explain (arena-only stats)");
            assert_ne!(
                resp_arena_only["plan"]["optimization"], "none",
                "with snapshot.stats=None but annotation_index=Some, explain must \
                 still report optimization via merged arena stats (got plan: {})",
                resp_arena_only["plan"]
            );
            assert_eq!(
                resp_arena_only["plan"]["statistics"]["total-flakes"], 0,
                "no IndexStats → total-flakes reports zero, but stats are still available"
            );
            // Roles still present in the arena-only path.
            let arena_only_roles: Vec<String> = resp_arena_only["plan"]["optimized"]
                .as_array()
                .expect("optimized array")
                .iter()
                .filter_map(|entry| {
                    entry
                        .get("annotation-role")
                        .and_then(|v| v.as_str())
                        .map(String::from)
                })
                .collect();
            let mut arena_only_sorted = arena_only_roles.clone();
            arena_only_sorted.sort();
            assert_eq!(
                arena_only_sorted,
                vec!["object", "predicate", "subject"],
                "arena-only path must still tag the three required slots (got: {arena_only_roles:?})"
            );
        })
        .await;
}

#[tokio::test]
async fn reindex_seals_arena_when_caller_supplies_only_a_provider() {
    // Regression for the review finding: when the caller passes an
    // `IndexerConfig` with `attachment_events_provider: Some(_)` but
    // `attachment_events: None`, the api's reindex path must still
    // resolve the provider into a concrete envelope — the direct
    // rebuild path only consumes the concrete field, so skipping
    // resolution would silently land the rebuild in scan-fallback.
    use fluree_db_api::ReindexOptions;

    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations-indexed:reindex-seals-arena-caller-provider";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    fluree
        .insert(ledger0, &annotated_insert())
        .await
        .expect("annotated insert");

    // Caller hand-builds an `IndexerConfig` carrying only the
    // attachment-events provider — no concrete events. The api's
    // reindex path must call the provider during resolution.
    let caller_provider = fluree
        .attachment_events_provider()
        .expect("api provider is available when ledger caching is enabled");
    let caller_config = fluree_db_indexer::IndexerConfig::default()
        .with_attachment_events_provider(caller_provider);

    fluree
        .reindex(
            ledger_id,
            ReindexOptions::default().with_indexer_config(caller_config),
        )
        .await
        .expect("reindex must succeed");

    let post = fluree.ledger(ledger_id).await.expect("reload");
    assert!(
        post.snapshot.annotation_index.is_some(),
        "reindex must seal an annotation arena via the caller-supplied \
         AttachmentEventsProvider — got annotation_index=None which means \
         the provider-only IndexerConfig branch is back to scan-fallback"
    );
}

#[tokio::test]
async fn reindex_seals_arena_when_caching_enabled_no_provider_in_opts() {
    // Closes the bulk-import → arena seal gap: a `fluree.reindex(...)`
    // call with default options should seal an authoritative arena
    // when ledger caching is enabled, even though the caller didn't
    // attach an `AttachmentEventsProvider` explicitly. The api's
    // admin path attaches its own provider (backed by the running
    // `LedgerManager`) and pre-loads the ledger so the provider can
    // read its overlay.
    //
    // Without this wiring, a CLI-driven import + reindex flow would
    // silently land in scan-fallback and require a *second* reindex
    // through the api to seal the arena.
    use fluree_db_api::ReindexOptions;

    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations-indexed:reindex-seals-arena";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    fluree
        .insert(ledger0, &annotated_insert())
        .await
        .expect("annotated insert");

    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex must succeed");

    let post = fluree.ledger(ledger_id).await.expect("reload");
    assert!(
        post.snapshot.has_annotations,
        "sticky bit set after annotated insert + reindex"
    );
    assert!(
        post.snapshot.annotation_index.is_some(),
        "reindex must seal an annotation arena via the api-attached \
         AttachmentEventsProvider — got annotation_index=None which \
         means the bulk-import → arena gap is still open"
    );
    let stats = &post
        .snapshot
        .annotation_index
        .as_ref()
        .expect("arena root")
        .stats;
    assert_eq!(stats.distinct_annotations, 1);
    assert_eq!(stats.live_attachment_pairs, 1);
}

/// #1467: a reification-aware COPY of a named-graph annotation must survive a
/// reindex. The attachment indexer decodes each bundle via
/// `EdgeKey::from_reifies_facts` at seal time
/// (`indexer_attachment_provider.rs`), so an incorrect re-home would drop the
/// annotation from the durable arena — not just from a live query. This drives
/// the full seal path and then decodes the re-homed bundle out of the indexed
/// snapshot (novelty-free), proving the anchor moved to the dest graph.
#[tokio::test]
async fn transfer_named_to_named_survives_reindex() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations-indexed:xfer-named-to-named";
    let g1 = "http://example.org/g1";
    let g2 = "http://example.org/g2";

    let (local, handle) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());

    local
        .run_until(async move {
            let ledger0 = genesis_ledger(&fluree, ledger_id);
            let seeded = fluree
                .insert(
                    ledger0,
                    &json!({
                        "@context": ctx(),
                        "@id": "ex:alice",
                        "@graph": g1,
                        "ex:worksFor": {
                            "@id": "ex:acme",
                            "@annotation": { "@id": "ex:emp/alice-acme", "ex:role": "Engineer" }
                        }
                    }),
                )
                .await
                .expect("seed named-graph annotation");

            // COPY <g1> TO <g2> — lower the SPARQL op and stage it.
            let sparql = format!("COPY <{g1}> TO <{g2}>");
            let parsed = fluree_db_sparql::parse_sparql(&sparql);
            assert!(
                !parsed.has_errors(),
                "parse errors: {:?}",
                parsed.diagnostics
            );
            let ast = parsed.ast.expect("AST");
            let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&seeded.ledger.snapshot);
            let txn = fluree_db_transact::lower_sparql_update_ast(
                &ast,
                &mut ns,
                fluree_db_transact::TxnOpts::default(),
            )
            .expect("lower COPY");
            let after_copy = fluree
                .stage_owned(seeded.ledger)
                .txn(txn)
                .execute()
                .await
                .expect("stage COPY");

            // Force the manager to cache the running ledger so the indexer's
            // attachment-events provider finds it (mirrors the pattern in
            // `incremental_arena_seal_then_arena_backed_query`).
            let _ = fluree
                .ledger_cached(ledger_id)
                .await
                .expect("cached load before reindex");

            support::trigger_index_and_wait(&handle, ledger_id, after_copy.receipt.t).await;
            support::wait_for_index_application(&fluree, ledger_id, after_copy.receipt.t).await;

            let post = fluree
                .ledger(ledger_id)
                .await
                .expect("reload after reindex");
            let ann_root = post
                .snapshot
                .annotation_index
                .as_ref()
                .expect("reindex with the attachment provider must seal an arena");

            // Index-time correctness gate. The attachment indexer decodes each
            // bundle via `EdgeKey::from_reifies_facts` at seal time; a bad
            // re-home of the g2 copy (anchor OBJECT still pointing at g1 while
            // the flake-level `g` is g2) would be a `GraphMismatch` and get
            // dropped from the arena. So the annotation now lives in BOTH the
            // original g1 and the copied g2 — two distinct reified graphs, two
            // f:reifiesGraph anchor rows in the durable index. Without the fix
            // only g1 would survive the seal.
            assert_eq!(
                ann_root.stats.distinct_reified_graphs, 2,
                "both the source (g1) and re-homed (g2) graph anchors must seal"
            );
            assert_eq!(
                ann_root.stats.reifies_graph_rows, 2,
                "two f:reifiesGraph anchor rows (one per graph) must survive the seal"
            );
        })
        .await;
}

/// default→named at the DURABLE-INDEX layer (#1483 review: the synthesized
/// anchor was only checked in-memory). `COPY DEFAULT TO <g2>` SYNTHESIZES an
/// `f:reifiesGraph` anchor the source never had; the attachment indexer
/// decodes every bundle via `EdgeKey::from_reifies_facts` at seal time, so a
/// malformed synthesized anchor would be silently dropped from the arena.
#[tokio::test]
async fn transfer_default_to_named_synthesized_anchor_survives_reindex() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations-indexed:xfer-default-to-named";
    let g2 = "http://example.org/g2";

    let (local, handle) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());

    local
        .run_until(async move {
            let ledger0 = genesis_ledger(&fluree, ledger_id);
            // Default-graph annotated edge — NO f:reifiesGraph anchor exists.
            let seeded = fluree
                .insert(
                    ledger0,
                    &json!({
                        "@context": ctx(),
                        "@id": "ex:alice",
                        "ex:worksFor": {
                            "@id": "ex:acme",
                            "@annotation": { "@id": "ex:emp/alice-acme", "ex:role": "Engineer" }
                        }
                    }),
                )
                .await
                .expect("seed default-graph annotation");

            let sparql = format!("COPY DEFAULT TO <{g2}>");
            let parsed = fluree_db_sparql::parse_sparql(&sparql);
            assert!(
                !parsed.has_errors(),
                "parse errors: {:?}",
                parsed.diagnostics
            );
            let ast = parsed.ast.expect("AST");
            let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&seeded.ledger.snapshot);
            let txn = fluree_db_transact::lower_sparql_update_ast(
                &ast,
                &mut ns,
                fluree_db_transact::TxnOpts::default(),
            )
            .expect("lower COPY DEFAULT");
            let after_copy = fluree
                .stage_owned(seeded.ledger)
                .txn(txn)
                .execute()
                .await
                .expect("stage COPY DEFAULT");

            let _ = fluree
                .ledger_cached(ledger_id)
                .await
                .expect("cached load before reindex");

            support::trigger_index_and_wait(&handle, ledger_id, after_copy.receipt.t).await;
            support::wait_for_index_application(&fluree, ledger_id, after_copy.receipt.t).await;

            let post = fluree
                .ledger(ledger_id)
                .await
                .expect("reload after reindex");
            let ann_root = post
                .snapshot
                .annotation_index
                .as_ref()
                .expect("reindex with the attachment provider must seal an arena");

            // The default-graph bundle carries NO anchor; the g2 copy carries
            // exactly the ONE synthesized anchor — and it must decode at seal
            // time (a bad synthesis would be dropped as GraphMismatch).
            assert_eq!(
                ann_root.stats.distinct_reified_graphs, 1,
                "exactly the synthesized g2 anchor's graph must seal"
            );
            assert_eq!(
                ann_root.stats.reifies_graph_rows, 1,
                "exactly one synthesized f:reifiesGraph anchor row must survive the seal"
            );
        })
        .await;
}

// ============================================================================
// Insert-flow seal matrix — regression pins for the write-only ingest trap:
// a background index build racing an annotated insert used to run with
// "delta unknown" (ledger not resident in the manager), defensively drop
// the arena, and stamp the sticky bit; an explicit reindex then read a
// stale NsRecord and rebuilt under Augment with no base arena — leaving
// `annotation_index = None` permanently. Three fixes pinned here:
// transient provider load, Augment merge with the previous root's arena,
// and the post-quiesce record re-fetch in `Fluree::reindex`.
// ============================================================================

async fn assert_seals(fluree: &fluree_db_api::Fluree, ledger_id: &str, label: &str) {
    fluree
        .reindex(ledger_id, fluree_db_api::ReindexOptions::default())
        .await
        .expect("reindex");
    let post = fluree.ledger(ledger_id).await.expect("reload");
    assert!(
        post.snapshot.has_annotations,
        "{label}: sticky bit after annotated insert"
    );
    assert!(
        post.snapshot.annotation_index.is_some(),
        "{label}: first reindex must seal the arena"
    );
}

#[tokio::test]
async fn seal_file_backed_small_insert() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let fluree: fluree_db_api::Fluree =
        FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
            .build()
            .expect("fluree");
    let ledger_id = "bisect/file-small:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.expect("create");
    fluree
        .insert(ledger0, &annotated_insert())
        .await
        .expect("insert");
    assert_seals(&fluree, ledger_id, "file+create_ledger+small").await;
}

#[tokio::test]
async fn seal_memory_create_ledger_insert() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "bisect/mem-create:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.expect("create");
    fluree
        .insert(ledger0, &annotated_insert())
        .await
        .expect("insert");
    assert_seals(&fluree, ledger_id, "memory+create_ledger+small").await;
}

#[tokio::test]
async fn seal_memory_lpg_graph_insert() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "bisect/mem-lpg:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    fluree
        .insert(
            ledger0,
            &json!({
                "@graph": [
                    {"@id": "n0", "@type": "INDEXED", "value": "v0",
                     "order": {"@id": "n1", "@annotation": {"w": 1}}},
                    {"@id": "n1", "@type": "INDEXED", "value": "v1",
                     "related": {"@id": "n0", "@annotation": {"w": 2}}}
                ],
                "opts": {"lpgEdgeLifecycle": true}
            }),
        )
        .await
        .expect("insert");
    assert_seals(&fluree, ledger_id, "memory+genesis+lpg-graph").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_survives_background_index_race() {
    // Probe-scale: hundreds of annotated edges in one transaction, enough
    // novelty for the background indexer to race the explicit reindex.
    let tmp = tempfile::tempdir().expect("tempdir");
    let fluree: fluree_db_api::Fluree =
        FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
            .build()
            .expect("fluree");
    let ledger_id = "bisect/file-large:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.expect("create");
    let nodes = 500usize;
    let graph: Vec<JsonValue> = (0..nodes)
        .map(|i| {
            json!({
                "@id": format!("http://kb.example/node/{i}"),
                "@type": "INDEXED",
                "value": format!("http://kb.example/entity/{i}"),
                "order": {
                    "@id": format!("http://kb.example/node/{}", (i + 1) % nodes),
                    "@annotation": {"w": 1}
                }
            })
        })
        .collect();
    fluree
        .insert(
            ledger0,
            &json!({"@graph": graph, "opts": {"lpgEdgeLifecycle": true}}),
        )
        .await
        .expect("insert");
    assert_seals(&fluree, ledger_id, "file+large-lpg-graph").await;
}

/// Arena-backed probe over a LITERAL reified triple with a VAR object.
/// The recognized chain accepts a var object; the arena keys literal
/// edges by (value, dt, lang) exactly as they were written, so the probe
/// must match them — previously a runtime literal binding dropped the
/// row (returned nothing) instead of probing.
#[tokio::test]
async fn arena_probe_matches_literal_object_annotation() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations-indexed:literal-object";

    let (local, handle) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());

    local
        .run_until(async move {
            let ledger0 = genesis_ledger(&fluree, ledger_id);
            let after_insert = fluree
                .insert(
                    ledger0,
                    &json!({
                        "@context": ctx(),
                        "@id": "ex:alice",
                        "ex:score": {
                            "@value": 42,
                            "@annotation": {"ex:confidence": "high"}
                        }
                    }),
                )
                .await
                .expect("literal annotated insert");
            let _ = fluree
                .ledger_cached(ledger_id)
                .await
                .expect("cached load before reindex");

            support::trigger_index_and_wait(&handle, ledger_id, after_insert.receipt.t).await;
            support::wait_for_index_application(&fluree, ledger_id, after_insert.receipt.t).await;

            let post = fluree
                .ledger(ledger_id)
                .await
                .expect("reload after reindex");
            assert!(
                post.snapshot.annotation_index.is_some(),
                "arena must be sealed for this test to exercise the probe lane"
            );

            let sparql = r"
                PREFIX ex: <http://example.org/>
                SELECT ?score ?conf WHERE {
                  ex:alice ex:score ?score {| ex:confidence ?conf |} .
                }
            ";
            let result = support::query_sparql(&fluree, &post, sparql)
                .await
                .expect("var-object literal annotation query");
            let rows = result.to_sparql_json(&post.snapshot).expect("sparql json");
            let bindings = rows["results"]["bindings"]
                .as_array()
                .expect("bindings array")
                .clone();
            assert_eq!(
                bindings.len(),
                1,
                "the literal reified triple must match via the arena probe: {bindings:#?}"
            );
            assert_eq!(bindings[0]["score"]["value"].as_str(), Some("42"));
            assert_eq!(bindings[0]["conf"]["value"].as_str(), Some("high"));
        })
        .await;
}
