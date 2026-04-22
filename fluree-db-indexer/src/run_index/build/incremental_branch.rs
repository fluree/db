//! Incremental branch update for V3 index format (FBR3).
//!
//! Given an existing V3 branch manifest + sorted novelty records, produces a
//! new FBR3 branch manifest with updated leaves.
//!
//! ## Strategy
//!
//! 1. Decode the existing FBR3 manifest.
//! 2. Slice novelty to leaves using half-open boundary intervals.
//! 3. For each touched leaf: fetch bytes, call `update_leaf`, collect results.
//! 4. Untouched leaves: carry forward existing `LeafEntry` unchanged.
//! 5. Assemble updated `LeafEntry` list and build new FBR3 manifest.

use std::io;

use fluree_db_binary_index::format::branch::{
    build_branch_bytes, read_branch_from_bytes, BranchManifest, LeafEntry,
};
use fluree_db_binary_index::format::leaf::decode_leaf_header_v3;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::{
    cmp_v2_for_order, read_ordered_key_v2, RunRecordV2,
};
use fluree_db_core::ContentId;

use super::incremental_leaf::{update_leaf, LeafUpdateInput, NewLeafBlob};

// ============================================================================
// Configuration and output types
// ============================================================================

/// Configuration for a branch update.
pub struct BranchUpdateConfig {
    /// Sort order.
    pub order: RunSortOrder,
    /// Graph id.
    pub g_id: u16,
    /// Zstd compression level for re-encoded leaflets.
    pub zstd_level: i32,
    /// Target rows per leaflet.
    pub leaflet_target_rows: usize,
    /// Target rows per leaf.
    pub leaf_target_rows: usize,
}

/// Result of a branch update.
pub struct BranchUpdateResult {
    /// Updated leaf entries for the new branch manifest.
    pub leaf_entries: Vec<LeafEntry>,
    /// New leaf blobs to upload to CAS.
    pub new_leaf_blobs: Vec<NewLeafBlob>,
    /// CIDs of replaced leaves (for GC).
    pub replaced_leaf_cids: Vec<ContentId>,
    /// CIDs of replaced sidecars (for GC).
    pub replaced_sidecar_cids: Vec<ContentId>,
    /// Encoded FBR3 branch manifest bytes.
    pub branch_bytes: Vec<u8>,
    /// CID of the new branch manifest.
    pub branch_cid: ContentId,
}

// ============================================================================
// Main entry point
// ============================================================================

/// Update a V3 branch with sorted novelty records.
///
/// `fetch_leaf` fetches leaf bytes by CID (synchronous).
/// `fetch_sidecar` fetches sidecar bytes by CID (synchronous; returns None if absent).
///
/// Leaves are processed sequentially. The caller is responsible for prefetching
/// touched leaves if parallelism is desired.
pub fn update_branch<F, G>(
    existing_branch_bytes: &[u8],
    novelty: &[RunRecordV2],
    novelty_ops: &[u8],
    config: &BranchUpdateConfig,
    fetch_leaf: &F,
    fetch_sidecar: &G,
) -> io::Result<BranchUpdateResult>
where
    F: Fn(&ContentId) -> io::Result<Vec<u8>>,
    G: Fn(&ContentId) -> io::Result<Option<Vec<u8>>>,
{
    let order = config.order;
    let g_id = config.g_id;

    // Catch mis-sorted novelty early — silent mis-slicing is brutal to debug.
    debug_assert!(
        novelty
            .windows(2)
            .all(|w| cmp_v2_for_order(order)(&w[0], &w[1]) != std::cmp::Ordering::Greater),
        "novelty must be sorted by the branch's sort order ({order:?})"
    );

    let manifest = read_branch_from_bytes(existing_branch_bytes)?;
    let cmp = cmp_v2_for_order(order);

    // Slice novelty to leaves.
    let novelty_slices = slice_novelty_to_leaves(novelty, novelty_ops, &manifest, cmp);

    let mut leaf_entries: Vec<LeafEntry> = Vec::with_capacity(manifest.leaves.len() + 4);
    let mut new_blobs: Vec<NewLeafBlob> = Vec::new();
    let mut replaced_leaf_cids: Vec<ContentId> = Vec::new();
    let mut replaced_sidecar_cids: Vec<ContentId> = Vec::new();

    for (i, (nov_slice, ops_slice)) in novelty_slices.iter().enumerate() {
        let existing = &manifest.leaves[i];

        if nov_slice.is_empty() {
            // Untouched leaf — carry forward.
            leaf_entries.push(existing.clone());
            continue;
        }

        // Fetch leaf bytes.
        let leaf_bytes = fetch_leaf(&existing.leaf_cid)?;
        let existing_header = decode_leaf_header_v3(&leaf_bytes)?;

        // Fetch sidecar bytes (if the leaf has one).
        let sidecar_bytes = match &existing.sidecar_cid {
            Some(cid) => fetch_sidecar(cid)?,
            None => None,
        };

        // Update the leaf.
        //
        // Incremental updates are intended to preserve branch structure for CID stability.
        // Avoid splitting a touched leaf into multiple new leaves by ensuring the
        // effective target is at least large enough for the existing rows plus this novelty slice.
        let effective_leaf_target_rows = (existing_header.total_rows as usize)
            .saturating_add(nov_slice.len())
            .saturating_add(1);
        let update_input = LeafUpdateInput {
            leaf_bytes: &leaf_bytes,
            novelty: nov_slice,
            novelty_ops: ops_slice,
            order,
            g_id,
            zstd_level: config.zstd_level,
            leaflet_target_rows: config.leaflet_target_rows,
            leaf_target_rows: config.leaf_target_rows.max(effective_leaf_target_rows),
            sidecar_bytes: sidecar_bytes.as_deref(),
        };

        let output = update_leaf(&update_input)?;

        // Record replaced CIDs for GC.
        replaced_leaf_cids.push(existing.leaf_cid.clone());
        if let Some(sc_cid) = &existing.sidecar_cid {
            replaced_sidecar_cids.push(sc_cid.clone());
        }

        // Add new leaf entries.
        for new_leaf in output.leaves {
            // Read the actual first/last keys from the new leaf header.
            let header = decode_leaf_header_v3(&new_leaf.info.leaf_bytes)?;
            let first_key = read_ordered_key_v2(order, &header.first_key);
            let last_key = read_ordered_key_v2(order, &header.last_key);

            leaf_entries.push(LeafEntry {
                first_key,
                last_key,
                row_count: new_leaf.info.total_rows,
                leaf_cid: new_leaf.info.leaf_cid.clone(),
                sidecar_cid: new_leaf.info.sidecar_cid.clone(),
            });

            new_blobs.push(new_leaf);
        }
    }

    // Build new branch manifest.
    let branch_bytes = build_branch_bytes(order, g_id, &leaf_entries);
    let branch_cid = compute_branch_cid(&branch_bytes);

    Ok(BranchUpdateResult {
        leaf_entries,
        new_leaf_blobs: new_blobs,
        replaced_leaf_cids,
        replaced_sidecar_cids,
        branch_bytes,
        branch_cid,
    })
}

// ============================================================================
// Novelty slicing
// ============================================================================

/// Slice novelty records to leaves using half-open boundary intervals.
///
/// Leaf 0: (-∞, leaf[1].first_key)
/// Leaf i: [leaf[i].first_key, leaf[i+1].first_key)
/// Last:   [leaf[last].first_key, +∞)
fn slice_novelty_to_leaves<'a>(
    novelty: &'a [RunRecordV2],
    ops: &'a [u8],
    manifest: &BranchManifest,
    cmp: fn(&RunRecordV2, &RunRecordV2) -> std::cmp::Ordering,
) -> Vec<(&'a [RunRecordV2], &'a [u8])> {
    let n_leaves = manifest.leaves.len();
    if n_leaves == 0 {
        return vec![];
    }
    if n_leaves == 1 {
        return vec![(novelty, ops)];
    }

    let mut result = Vec::with_capacity(n_leaves);
    let mut remaining_records = novelty;
    let mut remaining_ops = ops;

    for i in 0..n_leaves {
        if i + 1 < n_leaves {
            let next_first = &manifest.leaves[i + 1].first_key;
            let split_pos = remaining_records
                .partition_point(|rec| cmp(rec, next_first) == std::cmp::Ordering::Less);

            let (this_recs, rest_recs) = remaining_records.split_at(split_pos);
            let (this_ops, rest_ops) = remaining_ops.split_at(split_pos);
            result.push((this_recs, this_ops));
            remaining_records = rest_recs;
            remaining_ops = rest_ops;
        } else {
            result.push((remaining_records, remaining_ops));
        }
    }

    result
}

// ============================================================================
// Helpers
// ============================================================================

fn compute_branch_cid(bytes: &[u8]) -> ContentId {
    let hex_digest = fluree_db_core::sha256_hex(bytes);
    ContentId::from_hex_digest(
        fluree_db_core::content_kind::CODEC_FLUREE_INDEX_BRANCH,
        &hex_digest,
    )
    .expect("valid SHA-256 hex digest")
}
