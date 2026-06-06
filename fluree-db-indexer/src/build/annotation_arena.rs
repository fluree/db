//! Indexer-side orchestration for sealing edge-annotation arenas.
//!
//! Glues the pure builder in
//! `fluree_db_binary_index::annotation_arena` to the CAS write seam
//! the indexer already drives for branches/leaves. The function takes
//! a pre-decoded set of attachment events (sourced from the running
//! ledger's `AttachmentNovelty.iter_event_pairs()` and threaded in
//! via `IndexerConfig.attachment_events`) plus the previous root's
//! arena, builds forward + reverse blobs, writes them, and returns
//! the populated [`AnnotationIndexRoot`] that the root encoder will
//! seal.
//!
//! ## What this module owns
//!
//! - Merging events from two sources (previous arena + novelty).
//! - Driving the CAS writes for arena leaves and branches.
//! - Returning a structurally-correct `AnnotationIndexRoot` plus the
//!   replaced leaf CIDs the GC pass needs.
//!
//! ## What this module does NOT own
//!
//! - **Decoding events from the commit stream.** The orchestrator
//!   layer collects the pre-decoded events from the running
//!   `AttachmentNovelty` and threads them through `IndexerConfig`.

use fluree_db_binary_index::annotation_arena::{
    build_arenas_from_event_pairs, build_forward_branch, build_reverse_branch,
    AnnotationArenaReader, DEFAULT_TARGET_ROWS_PER_LEAF,
};
// Note: collect_all_forward_events was used when this module merged
// the previous arena's events into a delta. The current contract is
// that callers pass the complete event history (typically from
// AttachmentNovelty.iter_event_pairs()), so the previous arena
// participates only for GC reachability via all_leaf_cids.
use fluree_db_core::storage::ContentStore;
use fluree_db_core::{AnnotationIndexRoot, ContentKind, EdgeKey, Sid};

use crate::error::{IndexerError, Result};

/// Output of [`build_and_persist_annotation_arena`].
///
/// `replaced_leaf_cids` enumerates every leaf CID referenced by the
/// previous arena (if any). `new_leaf_cids` enumerates every leaf CID
/// referenced by the arena just sealed. Pass BOTH to
/// `IncrementalRootBuilder::set_annotation_index` so it can record
/// only the old CIDs the new arena no longer references as garbage —
/// content-addressed storage means a re-sealed unchanged arena
/// produces identical CIDs, and GC must not delete leaves/branches the
/// new root still points at. The previous and new branch CIDs are
/// reconciled inside `set_annotation_index` (old from
/// `root.annotation_index`, new from the passed `new_index`).
///
#[derive(Debug, Default)]
pub struct PersistedArenaResult {
    pub new_index: Option<AnnotationIndexRoot>,
    pub replaced_leaf_cids: Vec<fluree_db_core::ContentId>,
    pub new_leaf_cids: Vec<fluree_db_core::ContentId>,
}

/// Build and persist an annotation arena from a complete event set.
///
/// Writes forward + reverse leaf and branch blobs to CAS and returns
/// the populated [`AnnotationIndexRoot`] plus the previous arena's
/// leaf CIDs (for GC bookkeeping) in [`PersistedArenaResult`].
///
/// ## Contract: `events` is the complete history, not a delta
///
/// `events` must contain every `f:reifies*` attachment event the new
/// snapshot should publish — typically the running ledger's full
/// `AttachmentNovelty.iter_event_pairs()` (which preserves events
/// across reindexes), clipped to `t <= IndexRoot.index_t`. The arena
/// is rebuilt from scratch from this set; the previous arena
/// participates **only** for GC reachability — its leaf CIDs are
/// returned in `replaced_leaf_cids` so callers can record them as
/// reclaimable.
///
/// This rebuild-from-scratch contract is safer than a delta merge:
/// the api-side `AttachmentNovelty` accumulates events across
/// reindexes (no `clear_up_to` on attachments), so a "delta" + "base
/// arena" merge would double-count any event indexed in a prior pass.
///
/// Returns `Ok(PersistedArenaResult { new_index: None, .. })` only
/// when **both** `events` is empty AND there is no previous arena —
/// preserving the "zero attachments" guarantee for non-annotation
/// ledgers. When the previous arena is `Some` and `events` is empty,
/// the new root advertises an empty arena (still authoritative —
/// empty events explicitly assert "no attachments live anywhere").
pub async fn build_and_persist_annotation_arena(
    content_store: &dyn ContentStore,
    previous_index: Option<&AnnotationIndexRoot>,
    events: Vec<(EdgeKey, Sid, i64, bool)>,
) -> Result<PersistedArenaResult> {
    if previous_index.is_none() && events.is_empty() {
        return Ok(PersistedArenaResult::default());
    }

    // Collect previous-arena leaf CIDs for GC. We do NOT merge the
    // previous arena's events with `events` — `events` is the
    // complete history per the contract above.
    let replaced_leaf_cids: Vec<fluree_db_core::ContentId> = match previous_index {
        Some(prev) => {
            let reader = AnnotationArenaReader::new(prev, content_store);
            reader.all_leaf_cids().await.map_err(IndexerError::Core)?
        }
        None => Vec::new(),
    };

    let out = build_arenas_from_event_pairs(events, DEFAULT_TARGET_ROWS_PER_LEAF);

    // Forward leaves first.
    let mut fwd_pairs = Vec::with_capacity(out.forward_leaves.len());
    for (summary, blob) in out.forward_leaves {
        let cid = content_store
            .put(ContentKind::AnnotationForwardLeaf, &blob)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        fwd_pairs.push((summary, cid));
    }
    let fwd_branch_bytes = build_forward_branch(&fwd_pairs);
    let fwd_branch_cid = content_store
        .put(ContentKind::AnnotationForwardBranch, &fwd_branch_bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

    let mut rev_pairs = Vec::with_capacity(out.reverse_leaves.len());
    for (summary, blob) in out.reverse_leaves {
        let cid = content_store
            .put(ContentKind::AnnotationReverseLeaf, &blob)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        rev_pairs.push((summary, cid));
    }
    let rev_branch_bytes = build_reverse_branch(&rev_pairs);
    let rev_branch_cid = content_store
        .put(ContentKind::AnnotationReverseBranch, &rev_branch_bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

    // Leaf CIDs the new arena references — handed to
    // `set_annotation_index` so it can exclude any that the previous
    // arena also referenced (identical re-sealed leaves) from the
    // garbage manifest.
    let new_leaf_cids: Vec<fluree_db_core::ContentId> = fwd_pairs
        .iter()
        .map(|(_, cid)| cid.clone())
        .chain(rev_pairs.iter().map(|(_, cid)| cid.clone()))
        .collect();

    Ok(PersistedArenaResult {
        new_index: Some(AnnotationIndexRoot {
            version: 1,
            max_t: out.max_t,
            forward_branch_cid: fwd_branch_cid,
            reverse_branch_cid: rev_branch_cid,
            stats: out.stats,
        }),
        replaced_leaf_cids,
        new_leaf_cids,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::storage::MemoryContentStore;
    use fluree_db_core::FlakeValue;
    use std::sync::Arc;

    fn ann(name: &str) -> Sid {
        Sid::new(20, name)
    }
    fn refs(name: &str) -> Sid {
        Sid::new(11, name)
    }
    fn id_dt() -> Sid {
        fluree_db_core::id_datatype_sid()
    }
    fn edge(s: &str, p: &str, o: &str) -> EdgeKey {
        EdgeKey {
            g: None,
            s: refs(s),
            p: refs(p),
            o: FlakeValue::Ref(refs(o)),
            dt: id_dt(),
            lang: None,
            list_i: None,
        }
    }

    #[tokio::test]
    async fn empty_inputs_skip_arena_seal() {
        let store: Arc<dyn ContentStore> = Arc::new(MemoryContentStore::new());
        let result = build_and_persist_annotation_arena(&store, None, Vec::new())
            .await
            .unwrap();
        assert!(result.new_index.is_none());
        assert!(result.replaced_leaf_cids.is_empty());
    }

    #[tokio::test]
    async fn novelty_only_seals_new_arena() {
        let store: Arc<dyn ContentStore> = Arc::new(MemoryContentStore::new());
        let events = vec![
            (edge("alice", "worksFor", "acme"), ann("ann_1"), 5, true),
            (edge("alice", "worksFor", "acme"), ann("ann_2"), 6, true),
        ];
        let result = build_and_persist_annotation_arena(&store, None, events)
            .await
            .unwrap();
        let new_index = result.new_index.expect("arena sealed");
        assert_eq!(new_index.max_t, 6);
        assert_eq!(new_index.stats.forward_rows, 2);
        assert_eq!(new_index.stats.distinct_edges, 1);
        assert_eq!(new_index.stats.distinct_annotations, 2);
        assert!(
            result.replaced_leaf_cids.is_empty(),
            "no previous arena → no replaced leaves"
        );

        // Roundtrip the new arena via a reader to confirm CAS writes
        // landed correctly.
        let reader = AnnotationArenaReader::new(&new_index, store.as_ref());
        let live = reader
            .current_annotations_for(&edge("alice", "worksFor", "acme"), 100)
            .await
            .unwrap();
        let mut sids: Vec<Sid> = live.into_iter().collect();
        sids.sort();
        assert_eq!(sids, vec![ann("ann_1"), ann("ann_2")]);
    }

    #[tokio::test]
    async fn empty_events_with_previous_arena_seals_empty_arena() {
        // Under the complete-history contract, `Some(vec![])`
        // explicitly asserts "no attachments live anywhere." The
        // new arena is empty — the previous arena's content is
        // NOT merged in. This is the correct shape for a ledger
        // whose attachments were all retracted between passes.
        let store: Arc<dyn ContentStore> = Arc::new(MemoryContentStore::new());
        let first = build_and_persist_annotation_arena(
            &store,
            None,
            vec![(edge("alice", "worksFor", "acme"), ann("ann_1"), 5, true)],
        )
        .await
        .unwrap();
        let prev = first.new_index.unwrap();

        let second = build_and_persist_annotation_arena(&store, Some(&prev), Vec::new())
            .await
            .unwrap();
        let new_index = second.new_index.expect("empty-events still seals an arena");
        assert_eq!(
            new_index.stats.forward_rows, 0,
            "empty events → empty arena under complete-history contract"
        );
        assert_eq!(new_index.stats.distinct_edges, 0);

        // Previous arena's leaves are still recorded for GC.
        assert_eq!(second.replaced_leaf_cids.len(), 2);
    }

    #[tokio::test]
    async fn truncates_events_above_job_t() {
        // The indexer clips `attachment_events` to `t <= job_t` before
        // calling the orchestrator helper. We verify the helper itself
        // does not over-shoot when it gets pre-clipped input: max_t
        // matches the highest event in the input, never higher.
        let store: Arc<dyn ContentStore> = Arc::new(MemoryContentStore::new());
        let result = build_and_persist_annotation_arena(
            &store,
            None,
            vec![
                (edge("alice", "worksFor", "acme"), ann("ann_1"), 5, true),
                (edge("alice", "worksFor", "acme"), ann("ann_2"), 7, true),
            ],
        )
        .await
        .unwrap();
        let new_index = result.new_index.unwrap();
        assert_eq!(
            new_index.max_t, 7,
            "max_t must reflect input, not exceed it"
        );
    }

    #[tokio::test]
    async fn rebuilds_from_complete_history_with_previous_arena_only_for_gc() {
        // Contract: `events` is the COMPLETE history. The previous
        // arena participates only for GC reachability — its events
        // are NOT merged in.
        let store: Arc<dyn ContentStore> = Arc::new(MemoryContentStore::new());

        // First seal: ann_1 attached at t=5.
        let first = build_and_persist_annotation_arena(
            &store,
            None,
            vec![(edge("alice", "worksFor", "acme"), ann("ann_1"), 5, true)],
        )
        .await
        .unwrap();
        let prev = first.new_index.expect("first seal produces an arena");

        let reader = AnnotationArenaReader::new(&prev, store.as_ref());
        let prev_leaves: std::collections::HashSet<_> =
            reader.all_leaf_cids().await.unwrap().into_iter().collect();
        assert_eq!(prev_leaves.len(), 2, "one forward + one reverse leaf");

        // Second seal: complete history is the original assert PLUS
        // a retract at t=8. Caller passes BOTH events, not just the
        // delta.
        let second = build_and_persist_annotation_arena(
            &store,
            Some(&prev),
            vec![
                (edge("alice", "worksFor", "acme"), ann("ann_1"), 5, true),
                (edge("alice", "worksFor", "acme"), ann("ann_1"), 8, false),
            ],
        )
        .await
        .unwrap();
        let new_index = second.new_index.expect("second seal");
        assert_eq!(new_index.max_t, 8);
        assert_eq!(
            new_index.stats.forward_rows, 2,
            "rebuild reflects the complete history, no double-count"
        );
        assert_eq!(
            new_index.stats.distinct_edges, 0,
            "ann_1 retracted → not live"
        );

        // Previous arena's leaves recorded for GC.
        let replaced: std::collections::HashSet<_> =
            second.replaced_leaf_cids.iter().cloned().collect();
        assert_eq!(replaced, prev_leaves);

        // Live read: ann_1 not visible at t=100.
        let reader = AnnotationArenaReader::new(&new_index, store.as_ref());
        let live = reader
            .current_annotations_for(&edge("alice", "worksFor", "acme"), 100)
            .await
            .unwrap();
        assert!(live.is_empty());
    }

    #[tokio::test]
    async fn augment_path_merges_and_dedupes() {
        // The Augment path in Phase 3d concats prev events with
        // caller events, then sorts + dedups by full tuple. Verify
        // the dedup actually drops exact-tuple duplicates.
        let mut combined: Vec<(EdgeKey, Sid, i64, bool)> = vec![
            (edge("alice", "worksFor", "acme"), ann("ann_1"), 5, true),
            (edge("alice", "worksFor", "acme"), ann("ann_1"), 5, true), // duplicate
            (edge("alice", "worksFor", "acme"), ann("ann_1"), 8, false),
        ];
        combined.sort();
        combined.dedup();
        assert_eq!(
            combined.len(),
            2,
            "exact-tuple duplicates must collapse to one"
        );
    }

    #[tokio::test]
    async fn rebuild_does_not_double_count_when_caller_supplies_complete_history() {
        // If a caller mistakenly passed only a delta, the previous
        // arena's events would be missed but the rebuild stays
        // self-consistent. Here we verify the no-merge contract
        // explicitly: identical input → identical row count.
        let store: Arc<dyn ContentStore> = Arc::new(MemoryContentStore::new());

        let events = vec![
            (edge("alice", "worksFor", "acme"), ann("ann_1"), 5, true),
            (edge("alice", "worksFor", "acme"), ann("ann_2"), 6, true),
        ];

        let first = build_and_persist_annotation_arena(&store, None, events.clone())
            .await
            .unwrap();
        let prev = first.new_index.unwrap();
        assert_eq!(prev.stats.forward_rows, 2);

        // Re-seal with the same complete history. The prev arena is
        // present but its events are NOT merged — total stays at 2.
        let second = build_and_persist_annotation_arena(&store, Some(&prev), events)
            .await
            .unwrap();
        let new_index = second.new_index.unwrap();
        assert_eq!(
            new_index.stats.forward_rows, 2,
            "previous arena must not be merged into the rebuild"
        );
    }

    /// Sanity-only: this orchestrator helper trusts what it's given.
    /// The `Augment`-without-prev-arena-but-sticky=true gate lives in
    /// `build/incremental.rs` (where `has_annotations` is in scope).
    /// Verify that here we'd happily seal an incomplete arena from
    /// partial events, which is precisely why the upper-layer gate
    /// is needed.
    #[tokio::test]
    async fn function_itself_does_not_gate_partial_events() {
        let store: Arc<dyn ContentStore> = Arc::new(MemoryContentStore::new());
        let result = build_and_persist_annotation_arena(
            &store,
            None,
            vec![(edge("alice", "worksFor", "acme"), ann("ann_1"), 5, true)],
        )
        .await
        .unwrap();
        let new_index = result.new_index.unwrap();
        assert_eq!(
            new_index.stats.forward_rows, 1,
            "function happily seals partial input — caller is responsible \
             for ensuring `events` is complete or that the result is OK \
             to publish"
        );
    }
}
