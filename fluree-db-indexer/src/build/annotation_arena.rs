//! Indexer-side orchestration for sealing edge-annotation arenas.
//!
//! Glues the pure builder in
//! `fluree_db_binary_index::annotation_arena` to the CAS write seam
//! the indexer already drives for branches/leaves. The function takes
//! a fully decoded set of attachment events (from
//! [`fluree_db_novelty::AttachmentNovelty`] and/or the previous root's
//! arena), builds forward + reverse blobs, writes them, and returns
//! the populated [`AnnotationIndexRoot`] that the root encoder will
//! seal.
//!
//! ## What this module owns
//!
//! - Merging events from two sources (previous arena + novelty).
//! - Driving the CAS writes for arena leaves and branches.
//! - Returning a structurally-correct `AnnotationIndexRoot`.
//!
//! ## What this module does NOT own
//!
//! - **Where the events come from on the indexer's collection side.**
//!   For incremental indexing the caller passes the running ledger's
//!   `AttachmentNovelty`; for full rebuild the caller will materialize
//!   one from the resolved commit stream (see slice 3d).

use fluree_db_binary_index::annotation_arena::{
    build_arenas_from_event_pairs, build_forward_branch, build_reverse_branch,
    AnnotationArenaReader, DEFAULT_TARGET_ROWS_PER_LEAF,
};
use fluree_db_core::storage::ContentStore;
use fluree_db_core::{AnnotationIndexRoot, ContentKind, EdgeKey, Sid};
use fluree_db_novelty::AttachmentNovelty;
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
/// Kept for: M2b slice 3d (the call site that consumes both fields).
/// Use when: incremental indexer wires `build_and_persist_annotation_arena`
/// → `IncrementalRootBuilder::set_annotation_index` and needs both
/// the new index pointer and the old leaf CIDs.
#[expect(dead_code)]
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
/// Kept for: M2b slice 3d (call site in `build/incremental.rs` after
/// resolving novelty). The orchestrator passes the running ledger's
/// `AttachmentNovelty` plus the base root's `annotation_index`, then
/// wires the result into `IncrementalRootBuilder::set_annotation_index`
/// (passing both `result.new_index` and `result.replaced_leaf_cids`).
/// Use when: incremental indexer has access to the running
/// AttachmentNovelty (today the indexer takes only commits + content
/// store; 3d adds the novelty as a public-API parameter).
#[expect(dead_code)]
pub async fn build_and_persist_annotation_arena(
    content_store: &Arc<dyn ContentStore>,
    previous_index: Option<&AnnotationIndexRoot>,
    attachments: &AttachmentNovelty,
) -> Result<PersistedArenaResult> {
    let novelty_events: Vec<(EdgeKey, Sid, i64, bool)> = attachments.iter_event_pairs().collect();

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
