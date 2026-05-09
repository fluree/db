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

/// Build a new annotation arena from the union of the previous arena
/// (when `previous_index` is `Some`) and the in-memory overlay events.
///
/// Writes the resulting forward + reverse leaf and branch blobs to
/// CAS and returns the populated [`AnnotationIndexRoot`] to seal into
/// the new index root.
///
/// Returns `Ok(None)` only when **both** sources are empty AND there
/// is no previous arena — preserving the "zero attachments" guarantee
/// for non-annotation ledgers. If `previous_index` is `Some`, the new
/// root will always be `Some` too (possibly with empty arenas), so
/// readers don't see a regression in the truth-table state.
pub async fn build_and_persist_annotation_arena(
    content_store: &Arc<dyn ContentStore>,
    previous_index: Option<&AnnotationIndexRoot>,
    attachments: &AttachmentNovelty,
) -> Result<Option<AnnotationIndexRoot>> {
    let novelty_events: Vec<(EdgeKey, Sid, i64, bool)> = attachments.iter_event_pairs().collect();

    if previous_index.is_none() && novelty_events.is_empty() {
        return Ok(None);
    }

    let mut combined = novelty_events;
    if let Some(prev) = previous_index {
        let reader = AnnotationArenaReader::new(prev, content_store.as_ref());
        let prev_events = reader
            .collect_all_forward_events()
            .await
            .map_err(IndexerError::Core)?;
        combined.reserve(prev_events.len());
        combined.extend(prev_events);
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

    Ok(Some(AnnotationIndexRoot {
        version: 1,
        max_t: out.max_t,
        forward_branch_cid: fwd_branch_cid,
        reverse_branch_cid: rev_branch_cid,
        stats: out.stats,
    }))
}
