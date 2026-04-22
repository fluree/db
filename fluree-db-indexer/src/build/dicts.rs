//! Incremental reverse tree update helpers.
//!
//! These functions handle updating existing reverse dictionary trees when
//! new subjects or strings are added during incremental indexing.

use fluree_db_binary_index::DictTreeRefs;
use fluree_db_core::{ContentId, ContentKind, ContentStore};

use crate::error::{IndexerError, Result};

use super::types::UpdatedReverseTree;

pub(crate) async fn upload_incremental_reverse_tree_async(
    content_store: &dyn ContentStore,
    dict: fluree_db_core::DictKind,
    existing_refs: &DictTreeRefs,
    new_subjects: Vec<(u16, u64, Vec<u8>)>,
) -> Result<UpdatedReverseTree> {
    use fluree_db_binary_index::dict::reverse_leaf::{subject_reverse_key, ReverseEntry};

    let mut entries: Vec<ReverseEntry> = new_subjects
        .iter()
        .map(|(ns_code, local_id, suffix)| ReverseEntry {
            key: subject_reverse_key(*ns_code, suffix),
            id: fluree_db_core::subject_id::SubjectId::new(*ns_code, *local_id).as_u64(),
        })
        .collect();
    entries.sort_by(|a, b| a.key.cmp(&b.key));

    upload_incremental_reverse_tree_core(content_store, dict, existing_refs, entries).await
}

/// Async version of reverse tree upload for **string** dictionaries.
///
/// Builds `ReverseEntry` from `(string_id, value)`, pre-fetches affected
/// leaves, runs the CPU-bound CoW update in `spawn_blocking`, then
/// async-uploads new artifacts.
pub(crate) async fn upload_incremental_reverse_tree_async_strings(
    content_store: &dyn ContentStore,
    dict: fluree_db_core::DictKind,
    existing_refs: &DictTreeRefs,
    new_strings: Vec<(u32, Vec<u8>)>,
) -> Result<UpdatedReverseTree> {
    use fluree_db_binary_index::dict::reverse_leaf::ReverseEntry;

    let mut entries: Vec<ReverseEntry> = new_strings
        .iter()
        .map(|(string_id, value)| ReverseEntry {
            key: value.clone(),
            id: *string_id as u64,
        })
        .collect();
    entries.sort_by(|a, b| a.key.cmp(&b.key));

    upload_incremental_reverse_tree_core(content_store, dict, existing_refs, entries).await
}

/// Core async reverse tree upload: pre-fetch affected leaves, spawn_blocking
/// for CoW update, async-upload new artifacts.
async fn upload_incremental_reverse_tree_core(
    content_store: &dyn ContentStore,
    dict: fluree_db_core::DictKind,
    existing_refs: &DictTreeRefs,
    entries: Vec<fluree_db_binary_index::dict::reverse_leaf::ReverseEntry>,
) -> Result<UpdatedReverseTree> {
    let kind = ContentKind::DictBlob { dict };

    // 1. Async fetch existing branch.
    let existing_branch_bytes = content_store
        .get(&existing_refs.branch)
        .await
        .map_err(|e| IndexerError::StorageRead(format!("load reverse branch: {e}")))?;
    let existing_branch =
        fluree_db_binary_index::dict::branch::DictBranch::decode(&existing_branch_bytes)
            .map_err(|e| IndexerError::StorageRead(format!("decode reverse branch: {e}")))?;

    // 2. Build address→CID map for existing leaves.
    let mut address_to_cid: std::collections::HashMap<String, ContentId> =
        std::collections::HashMap::new();
    for (i, entry) in existing_branch.leaves.iter().enumerate() {
        if let Some(cid) = existing_refs.leaves.get(i) {
            address_to_cid.insert(entry.address.clone(), cid.clone());
        }
    }

    // 3. Pre-fetch only affected leaves (those that will receive novelty).
    let affected_indices = compute_affected_leaf_indices(&entries, &existing_branch.leaves);
    let mut prefetched: std::collections::HashMap<usize, Vec<u8>> =
        std::collections::HashMap::with_capacity(affected_indices.len());
    for &idx in &affected_indices {
        let cid = existing_refs.leaves.get(idx).ok_or_else(|| {
            IndexerError::StorageRead(format!("reverse leaf index {idx} out of bounds"))
        })?;
        let bytes = content_store
            .get(cid)
            .await
            .map_err(|e| IndexerError::StorageRead(format!("fetch reverse leaf: {e}")))?;
        prefetched.insert(idx, bytes);
    }

    // 4. CPU-bound CoW update in spawn_blocking.
    let existing_branch_owned = existing_branch;
    let tree_result = tokio::task::spawn_blocking(move || {
        let mut fetch_leaf = |idx: usize| -> std::result::Result<Vec<u8>, std::io::Error> {
            prefetched
                .remove(&idx)
                .ok_or_else(|| std::io::Error::other(format!("reverse leaf {idx} not prefetched")))
        };
        fluree_db_binary_index::dict::incremental::update_reverse_tree(
            &existing_branch_owned,
            &entries,
            fluree_db_binary_index::dict::builder::DEFAULT_TARGET_LEAF_BYTES,
            &mut fetch_leaf,
        )
    })
    .await
    .map_err(|e| IndexerError::StorageWrite(format!("reverse tree task panicked: {e}")))?
    .map_err(|e| IndexerError::StorageWrite(format!("incremental reverse tree: {e}")))?;

    // 5. Async upload new leaf artifacts.
    //
    // Use CID strings as the "address" placeholder fed into `finalize_branch`
    // since the CAS layer is content-addressed only — there is no distinct
    // storage address.
    let mut hash_to_address: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for leaf_art in &tree_result.new_leaves {
        let cid = content_store
            .put(kind, &leaf_art.bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        let cid_str = cid.to_string();
        address_to_cid.insert(cid_str.clone(), cid);
        hash_to_address.insert(leaf_art.hash.clone(), cid_str);
    }

    // 6. Finalize branch (replace pending:hash → real addresses).
    let (finalized_branch, finalized_bytes, _) =
        fluree_db_binary_index::dict::builder::finalize_branch(
            tree_result.branch,
            &hash_to_address,
        )
        .map_err(|e| IndexerError::StorageWrite(format!("finalize reverse branch: {e}")))?;

    // 7. Async upload finalized branch.
    let new_branch_cid = content_store
        .put(kind, &finalized_bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

    // 8. Build leaf CID list from finalized branch.
    let mut leaf_cids: Vec<ContentId> = Vec::with_capacity(finalized_branch.leaves.len());
    for entry in &finalized_branch.leaves {
        let cid = address_to_cid.get(&entry.address).ok_or_else(|| {
            IndexerError::StorageWrite(format!(
                "no CID for reverse leaf address: {}",
                entry.address
            ))
        })?;
        leaf_cids.push(cid.clone());
    }

    // 9. Collect replaced CIDs for GC.
    let mut replaced_cids = vec![existing_refs.branch.clone()];
    for &idx in &tree_result.replaced_leaf_indices {
        if let Some(cid) = existing_refs.leaves.get(idx) {
            replaced_cids.push(cid.clone());
        }
    }

    Ok(UpdatedReverseTree {
        tree_refs: DictTreeRefs {
            branch: new_branch_cid,
            leaves: leaf_cids,
        },
        replaced_cids,
    })
}

/// Compute which leaf indices in a reverse tree branch are affected by new entries.
///
/// Uses the same half-open interval logic as the internal `slice_entries_to_leaves`:
/// leaf `i` owns keys in `[leaf[i].first_key, leaf[i+1].first_key)`, and the last
/// leaf owns `[leaf[last].first_key, +∞)`.
fn compute_affected_leaf_indices(
    entries: &[fluree_db_binary_index::dict::reverse_leaf::ReverseEntry],
    leaves: &[fluree_db_binary_index::dict::branch::BranchLeafEntry],
) -> Vec<usize> {
    let n = leaves.len();
    if n == 0 || entries.is_empty() {
        return Vec::new();
    }

    let mut affected = Vec::new();
    let mut start = 0;

    for i in 0..n {
        if i == n - 1 {
            // Last leaf: anything remaining goes here.
            if start < entries.len() {
                affected.push(i);
            }
        } else {
            let next_key = &leaves[i + 1].first_key;
            let end = start
                + entries[start..].partition_point(|e| e.key.as_slice() < next_key.as_slice());
            if end > start {
                affected.push(i);
            }
            start = end;
        }
    }

    affected
}
