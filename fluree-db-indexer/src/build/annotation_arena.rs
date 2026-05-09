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
use fluree_db_core::storage::ContentStore;
use fluree_db_core::{AnnotationIndexRoot, ContentKind, EdgeKey, Sid};
use std::sync::Arc;

use crate::error::{IndexerError, Result};

/// Output of [`build_and_persist_annotation_arena`].
///
/// `replaced_leaf_cids` enumerates every leaf CID referenced by the
/// previous arena (if any). Pass it directly to
/// `IncrementalRootBuilder::set_annotation_index` so GC can reclaim
/// the old leaves once the new root supersedes the chain. The
/// previous branch CIDs are recorded automatically by
/// `set_annotation_index` from `root.annotation_index`, so this list
/// covers only the leaves.
///
#[derive(Debug, Default)]
pub struct PersistedArenaResult {
    pub new_index: Option<AnnotationIndexRoot>,
    pub replaced_leaf_cids: Vec<fluree_db_core::ContentId>,
}

/// Build a new annotation arena from the union of the previous arena
/// (when `previous_index` is `Some`) and the in-memory overlay events.
///
/// Writes the resulting forward + reverse leaf and branch blobs to
/// CAS and returns the populated [`AnnotationIndexRoot`] plus the
/// previous arena's leaf CIDs (for GC bookkeeping) in
/// [`PersistedArenaResult`].
///
/// Returns `Ok(PersistedArenaResult { new_index: None, .. })` only
/// when **both** sources are empty AND there is no previous arena —
/// preserving the "zero attachments" guarantee for non-annotation
/// ledgers. If `previous_index` is `Some`, the new index will always
/// be `Some` too (possibly with empty arenas), so readers don't see a
/// regression in the truth-table state.
///
/// `novelty_events` is the post-base-root attachment delta as
/// `(edge, ann, t, op)` tuples — typically collected from the running
/// ledger's `AttachmentNovelty.iter_event_pairs()` and threaded into
/// the indexer through `IndexerConfig.attachment_events`. Decoupling
/// the function from the concrete `AttachmentNovelty` type keeps the
/// indexer free of a `fluree-db-novelty` runtime dep on this code
/// path.
pub async fn build_and_persist_annotation_arena(
    content_store: &Arc<dyn ContentStore>,
    previous_index: Option<&AnnotationIndexRoot>,
    novelty_events: Vec<(EdgeKey, Sid, i64, bool)>,
) -> Result<PersistedArenaResult> {
    if previous_index.is_none() && novelty_events.is_empty() {
        return Ok(PersistedArenaResult::default());
    }

    let mut combined = novelty_events;
    let mut replaced_leaf_cids: Vec<fluree_db_core::ContentId> = Vec::new();
    if let Some(prev) = previous_index {
        let reader = AnnotationArenaReader::new(prev, content_store.as_ref());
        // Collect both the events (for the merge) and every leaf CID
        // referenced by the previous arena (for GC reachability).
        // Two branches × N leaves each — same set of CAS reads either
        // way; we just record the CIDs from the loaded branches.
        let prev_events = reader
            .collect_all_forward_events()
            .await
            .map_err(IndexerError::Core)?;
        combined.reserve(prev_events.len());
        combined.extend(prev_events);
        replaced_leaf_cids = reader.all_leaf_cids().await.map_err(IndexerError::Core)?;
    }

    let out = build_arenas_from_event_pairs(combined, DEFAULT_TARGET_ROWS_PER_LEAF);

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

    Ok(PersistedArenaResult {
        new_index: Some(AnnotationIndexRoot {
            version: 1,
            max_t: out.max_t,
            forward_branch_cid: fwd_branch_cid,
            reverse_branch_cid: rev_branch_cid,
            stats: out.stats,
        }),
        replaced_leaf_cids,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::storage::MemoryContentStore;
    use fluree_db_core::FlakeValue;

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
    async fn empty_event_vec_seals_authoritative_arena_with_no_changes() {
        // `Some(vec![])` ≠ `None`. The caller has explicitly
        // confirmed there are no new events since the base arena.
        // The merge produces the same row set as the base, but a
        // fresh arena gets written so readers can prefer it.
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
        let new_index = second.new_index.expect("empty-delta still seals an arena");
        assert_eq!(new_index.stats.forward_rows, 1);
        assert_eq!(new_index.stats.distinct_edges, 1);

        // Live read confirms the merged arena reflects the same state
        // as the base.
        let reader = AnnotationArenaReader::new(&new_index, store.as_ref());
        let live = reader
            .current_annotations_for(&edge("alice", "worksFor", "acme"), 100)
            .await
            .unwrap();
        assert_eq!(live, vec![ann("ann_1")]);
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
    async fn merging_with_previous_arena_yields_replaced_leaf_cids() {
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

        // Snapshot the previous leaf CIDs so we can compare.
        let reader = AnnotationArenaReader::new(&prev, store.as_ref());
        let prev_leaves: std::collections::HashSet<_> =
            reader.all_leaf_cids().await.unwrap().into_iter().collect();
        assert_eq!(prev_leaves.len(), 2, "one forward + one reverse leaf");

        // Second seal: novelty retracts ann_1 at t=8. The merge
        // should yield a 2-row arena (assert + retract) and report
        // every CID from the previous arena as replaced.
        let second = build_and_persist_annotation_arena(
            &store,
            Some(&prev),
            vec![(edge("alice", "worksFor", "acme"), ann("ann_1"), 8, false)],
        )
        .await
        .unwrap();
        let new_index = second.new_index.expect("second seal");
        assert_eq!(new_index.max_t, 8);
        assert_eq!(new_index.stats.forward_rows, 2);
        assert_eq!(
            new_index.stats.distinct_edges, 0,
            "ann_1 retracted → not live"
        );

        let replaced: std::collections::HashSet<_> =
            second.replaced_leaf_cids.iter().cloned().collect();
        assert_eq!(
            replaced, prev_leaves,
            "replaced_leaf_cids must enumerate every leaf from the previous arena"
        );

        // Live read of the merged arena: ann_1 not visible at t=100.
        let reader = AnnotationArenaReader::new(&new_index, store.as_ref());
        let live = reader
            .current_annotations_for(&edge("alice", "worksFor", "acme"), 100)
            .await
            .unwrap();
        assert!(live.is_empty(), "retract overrides assert in merged arena");
    }
}
