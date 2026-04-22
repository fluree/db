//! Incremental dictionary update: append new forward packs + CoW reverse tree update.
//!
//! ## Forward Packs (append-only)
//!
//! New entries always have IDs above the existing watermark. We build new packs
//! from only the new entries and append their `PackBranchEntry` refs to the
//! existing list. All existing packs are reused unchanged.
//!
//! ## Reverse Trees (CoW update)
//!
//! Route new entries to affected DLR1 leaves by key range, fetch + decode +
//! merge-insert + re-encode. Split oversized leaves. Unchanged leaves keep
//! their existing CAS addresses. Produces a new DTB1 branch with mixed
//! old (real) and new (`pending:hash`) addresses — caller finalizes via
//! [`super::builder::finalize_branch`] after CAS upload.

use std::io;

use super::branch::{BranchLeafEntry, DictBranch};
use super::builder::LeafArtifact;
use super::pack_builder::{
    build_string_forward_packs, build_subject_forward_packs_for_ns, PackArtifact,
    DEFAULT_TARGET_PACK_BYTES, DEFAULT_TARGET_PAGE_BYTES,
};
use super::reverse_leaf::{encode_reverse_leaf, ReverseEntry, ReverseLeaf};
use crate::format::wire_helpers::PackBranchEntry;

// ============================================================================
// Forward Packs (append-only)
// ============================================================================

/// Result of incremental forward pack building.
pub struct IncrementalPackResult {
    /// New pack artifacts that need CAS upload.
    pub new_packs: Vec<PackArtifact>,
    /// All pack refs (existing + new) in ID order.
    pub all_pack_refs: Vec<PackBranchEntry>,
}

/// Build incremental string forward packs from new entries.
///
/// `existing_refs` are the current string forward pack routing refs.
/// `new_entries` are `(string_id, value)` pairs above the current watermark,
/// sorted ascending by string_id.
///
/// Returns new pack artifacts + the combined routing table.
pub fn build_incremental_string_packs(
    existing_refs: &[PackBranchEntry],
    new_entries: &[(u32, &[u8])],
) -> io::Result<IncrementalPackResult> {
    if new_entries.is_empty() {
        return Ok(IncrementalPackResult {
            new_packs: Vec::new(),
            all_pack_refs: existing_refs.to_vec(),
        });
    }

    let result = build_string_forward_packs(
        new_entries,
        DEFAULT_TARGET_PAGE_BYTES,
        DEFAULT_TARGET_PACK_BYTES,
    )?;

    let new_refs: Vec<PackBranchEntry> = result
        .packs
        .iter()
        .map(|p| PackBranchEntry {
            first_id: p.first_id,
            last_id: p.last_id,
            pack_cid: fluree_db_core::ContentId::from_hex_digest(
                fluree_db_core::content_kind::CODEC_FLUREE_DICT_BLOB,
                &fluree_db_core::sha256_hex(&p.bytes),
            )
            .expect("valid SHA-256"),
        })
        .collect();

    let mut all_refs = existing_refs.to_vec();
    all_refs.extend(new_refs.iter().cloned());

    tracing::debug!(
        existing_packs = existing_refs.len(),
        new_packs = new_refs.len(),
        new_entries = new_entries.len(),
        "incremental string forward packs built"
    );

    Ok(IncrementalPackResult {
        new_packs: result.packs,
        all_pack_refs: all_refs,
    })
}

/// Build incremental subject forward packs for a single namespace.
///
/// `existing_refs` are the current pack routing refs for this namespace.
/// `new_entries` are `(local_id, suffix_bytes)` pairs above the watermark.
pub fn build_incremental_subject_packs_for_ns(
    ns_code: u16,
    existing_refs: &[PackBranchEntry],
    new_entries: &[(u64, &[u8])],
) -> io::Result<IncrementalPackResult> {
    if new_entries.is_empty() {
        return Ok(IncrementalPackResult {
            new_packs: Vec::new(),
            all_pack_refs: existing_refs.to_vec(),
        });
    }

    let result = build_subject_forward_packs_for_ns(
        ns_code,
        new_entries,
        DEFAULT_TARGET_PAGE_BYTES,
        DEFAULT_TARGET_PACK_BYTES,
    )?;

    let new_refs: Vec<PackBranchEntry> = result
        .packs
        .iter()
        .map(|p| PackBranchEntry {
            first_id: p.first_id,
            last_id: p.last_id,
            pack_cid: fluree_db_core::ContentId::from_hex_digest(
                fluree_db_core::content_kind::CODEC_FLUREE_DICT_BLOB,
                &fluree_db_core::sha256_hex(&p.bytes),
            )
            .expect("valid SHA-256"),
        })
        .collect();

    let mut all_refs = existing_refs.to_vec();
    all_refs.extend(new_refs);

    tracing::debug!(
        ns_code = ns_code,
        existing_packs = existing_refs.len(),
        new_entries = new_entries.len(),
        "incremental subject forward packs built"
    );

    Ok(IncrementalPackResult {
        new_packs: result.packs,
        all_pack_refs: all_refs,
    })
}

// ============================================================================
// Reverse Tree (CoW update)
// ============================================================================

/// Split threshold: leaves larger than target × SPLIT_FACTOR get split.
const SPLIT_FACTOR: f64 = 1.5;

/// Result of incremental reverse tree update.
pub struct IncrementalTreeResult {
    /// Updated branch (new leaves have `pending:hash` addresses).
    pub branch: DictBranch,
    /// Encoded DTB1 branch bytes.
    pub branch_bytes: Vec<u8>,
    /// SHA-256 hex hash of branch bytes.
    pub branch_hash: String,
    /// New/modified leaf artifacts (need CAS upload).
    pub new_leaves: Vec<LeafArtifact>,
    /// Indices of leaves in the original branch that were replaced.
    pub replaced_leaf_indices: Vec<usize>,
}

/// Incrementally update a reverse dictionary tree with new entries.
///
/// `new_entries` must be sorted by key in ascending byte order.
/// `fetch_leaf` is called with the leaf index for leaves that need updating.
///
/// Unchanged leaves keep their existing CAS addresses in the branch.
/// New/modified leaves get `pending:hash` addresses — use
/// [`super::builder::finalize_branch`] after CAS upload to set real addresses.
pub fn update_reverse_tree(
    existing_branch: &DictBranch,
    new_entries: &[ReverseEntry],
    target_leaf_bytes: usize,
    fetch_leaf: &mut dyn FnMut(usize) -> Result<Vec<u8>, io::Error>,
) -> io::Result<IncrementalTreeResult> {
    // Empty branch: build from scratch using new entries only
    if existing_branch.leaves.is_empty() {
        return build_fresh_tree(new_entries, target_leaf_bytes);
    }

    if new_entries.is_empty() {
        return Ok(unchanged_tree(existing_branch));
    }

    // Slice new entries to leaves using half-open intervals on first_key
    let slices = slice_entries_to_leaves(new_entries, &existing_branch.leaves);

    let mut branch_entries: Vec<BranchLeafEntry> = Vec::new();
    let mut new_leaves: Vec<LeafArtifact> = Vec::new();
    let mut replaced_indices: Vec<usize> = Vec::new();

    for (i, existing_leaf) in existing_branch.leaves.iter().enumerate() {
        let slice = slices[i];
        if slice.is_empty() {
            // Unchanged: keep existing entry
            branch_entries.push(existing_leaf.clone());
        } else {
            // Fetch, decode, merge, re-encode
            let leaf_bytes = fetch_leaf(i)?;
            let decoded = ReverseLeaf::from_bytes(&leaf_bytes)?;
            let merged = merge_leaf_entries(&decoded, slice);

            let split_threshold = (target_leaf_bytes as f64 * SPLIT_FACTOR) as usize;
            let estimated_bytes = estimate_leaf_bytes(&merged);

            if estimated_bytes > split_threshold {
                // Split into two leaves
                let mid = merged.len() / 2;
                let (first_half, second_half) = merged.split_at(mid);

                let a = encode_and_track(first_half, &mut new_leaves);
                let b = encode_and_track(second_half, &mut new_leaves);
                branch_entries.push(a);
                branch_entries.push(b);
            } else {
                let entry = encode_and_track(&merged, &mut new_leaves);
                branch_entries.push(entry);
            }

            replaced_indices.push(i);
        }
    }

    let branch = DictBranch {
        leaves: branch_entries,
    };
    let branch_bytes = branch.encode();
    let branch_hash = fluree_db_core::sha256_hex(&branch_bytes);

    tracing::debug!(
        total_leaves = existing_branch.leaves.len(),
        touched = replaced_indices.len(),
        new_leaf_artifacts = new_leaves.len(),
        output_leaves = branch.leaves.len(),
        new_entries = new_entries.len(),
        "reverse tree update complete"
    );

    Ok(IncrementalTreeResult {
        branch,
        branch_bytes,
        branch_hash,
        new_leaves,
        replaced_leaf_indices: replaced_indices,
    })
}

// ============================================================================
// Internal helpers
// ============================================================================

/// Slice new entries to leaves using half-open intervals on `first_key`.
fn slice_entries_to_leaves<'a>(
    entries: &'a [ReverseEntry],
    leaves: &[BranchLeafEntry],
) -> Vec<&'a [ReverseEntry]> {
    let n = leaves.len();
    if n == 0 || entries.is_empty() {
        return vec![&[] as &[ReverseEntry]; n];
    }

    let mut slices = Vec::with_capacity(n);
    let mut start = 0;

    for i in 0..n {
        if i == n - 1 {
            slices.push(&entries[start..]);
        } else {
            let next_key = &leaves[i + 1].first_key;
            let end = start
                + entries[start..].partition_point(|e| e.key.as_slice() < next_key.as_slice());
            slices.push(&entries[start..end]);
            start = end;
        }
    }

    slices
}

/// Merge new entries into a decoded leaf, producing a sorted combined list.
fn merge_leaf_entries(leaf: &ReverseLeaf<'_>, new: &[ReverseEntry]) -> Vec<ReverseEntry> {
    // Collect existing entries via iterator
    let existing: Vec<ReverseEntry> = leaf
        .iter()
        .map(|(key, id)| ReverseEntry {
            key: key.to_vec(),
            id,
        })
        .collect();

    let mut result = Vec::with_capacity(existing.len() + new.len());
    let mut ei = 0usize;
    let mut ni = 0usize;

    while ei < existing.len() && ni < new.len() {
        match existing[ei].key.as_slice().cmp(new[ni].key.as_slice()) {
            std::cmp::Ordering::Less => {
                result.push(existing[ei].clone());
                ei += 1;
            }
            std::cmp::Ordering::Greater => {
                result.push(new[ni].clone());
                ni += 1;
            }
            std::cmp::Ordering::Equal => {
                // New entry replaces existing (shouldn't happen for new IDs, but safe)
                result.push(new[ni].clone());
                ei += 1;
                ni += 1;
            }
        }
    }

    // Drain remaining
    result.extend_from_slice(&existing[ei..]);
    result.extend_from_slice(&new[ni..]);
    result
}

/// Estimate encoded leaf byte size for a set of entries.
fn estimate_leaf_bytes(entries: &[ReverseEntry]) -> usize {
    // header(8) + offset_table(4 × n) + data(12 + key_len per entry)
    8 + entries.len() * 4 + entries.iter().map(|e| 12 + e.key.len()).sum::<usize>()
}

/// Encode entries to a DLR1 leaf, compute hash, add to artifacts, return branch entry.
fn encode_and_track(
    entries: &[ReverseEntry],
    artifacts: &mut Vec<LeafArtifact>,
) -> BranchLeafEntry {
    let leaf_bytes = encode_reverse_leaf(entries);
    let hash = fluree_db_core::sha256_hex(&leaf_bytes);

    let first_key = entries.first().unwrap().key.clone();
    let last_key = entries.last().unwrap().key.clone();
    let entry_count = entries.len() as u32;

    artifacts.push(LeafArtifact {
        hash: hash.clone(),
        bytes: leaf_bytes,
    });

    BranchLeafEntry {
        first_key,
        last_key,
        entry_count,
        address: format!("pending:{hash}"),
    }
}

/// Build a fresh tree from entries (when existing branch is empty).
fn build_fresh_tree(
    entries: &[ReverseEntry],
    target_leaf_bytes: usize,
) -> io::Result<IncrementalTreeResult> {
    let tree = super::builder::build_reverse_tree(entries.to_vec(), target_leaf_bytes)?;
    Ok(IncrementalTreeResult {
        branch: tree.branch,
        branch_bytes: tree.branch_bytes,
        branch_hash: tree.branch_hash,
        new_leaves: tree.leaves,
        replaced_leaf_indices: vec![],
    })
}

/// Return an unchanged tree result (no new entries).
fn unchanged_tree(existing_branch: &DictBranch) -> IncrementalTreeResult {
    let branch = existing_branch.clone();
    let branch_bytes = branch.encode();
    let branch_hash = fluree_db_core::sha256_hex(&branch_bytes);
    IncrementalTreeResult {
        branch,
        branch_bytes,
        branch_hash,
        new_leaves: vec![],
        replaced_leaf_indices: vec![],
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::builder::{build_reverse_tree, DEFAULT_TARGET_LEAF_BYTES};
    use super::*;
    use crate::format::wire_helpers::PackBranchEntry;
    use fluree_db_core::ContentId;

    fn make_string_entry(id: u32, val: &str) -> (u32, Vec<u8>) {
        (id, val.as_bytes().to_vec())
    }

    fn make_reverse_entry(key: &str, id: u64) -> ReverseEntry {
        ReverseEntry {
            key: key.as_bytes().to_vec(),
            id,
        }
    }

    fn dummy_cid(index: u32) -> ContentId {
        ContentId::from_hex_digest(
            fluree_db_core::content_kind::CODEC_FLUREE_DICT_BLOB,
            &fluree_db_core::sha256_hex(format!("pack-{index}").as_bytes()),
        )
        .unwrap()
    }

    // ---- Forward pack tests ----

    #[test]
    fn test_incremental_string_packs_empty_new() {
        let existing = vec![PackBranchEntry {
            first_id: 0,
            last_id: 99,
            pack_cid: dummy_cid(0),
        }];
        let result = build_incremental_string_packs(&existing, &[]).unwrap();
        assert!(result.new_packs.is_empty());
        assert_eq!(result.all_pack_refs.len(), 1);
        assert_eq!(result.all_pack_refs[0].pack_cid, existing[0].pack_cid);
    }

    #[test]
    fn test_incremental_string_packs_appends() {
        let existing = vec![PackBranchEntry {
            first_id: 0,
            last_id: 99,
            pack_cid: dummy_cid(0),
        }];

        let new_entries: Vec<(u32, Vec<u8>)> = (100..110)
            .map(|i| make_string_entry(i, &format!("value_{i}")))
            .collect();
        let new_refs: Vec<(u32, &[u8])> = new_entries
            .iter()
            .map(|(id, v)| (*id, v.as_slice()))
            .collect();

        let result = build_incremental_string_packs(&existing, &new_refs).unwrap();
        assert_eq!(result.new_packs.len(), 1);
        assert_eq!(result.all_pack_refs.len(), 2);
        // First ref unchanged
        assert_eq!(result.all_pack_refs[0].pack_cid, existing[0].pack_cid);
        // Second ref is new
        assert_eq!(result.all_pack_refs[1].first_id, 100);
        assert_eq!(result.all_pack_refs[1].last_id, 109);
    }

    #[test]
    fn test_incremental_subject_packs_appends() {
        let existing = vec![PackBranchEntry {
            first_id: 1,
            last_id: 50,
            pack_cid: dummy_cid(0),
        }];

        let new_entries: Vec<(u64, Vec<u8>)> = (51..56)
            .map(|i| (i as u64, format!("suffix_{i}").into_bytes()))
            .collect();
        let new_refs: Vec<(u64, &[u8])> = new_entries
            .iter()
            .map(|(id, v)| (*id, v.as_slice()))
            .collect();

        let result = build_incremental_subject_packs_for_ns(0, &existing, &new_refs).unwrap();
        assert_eq!(result.new_packs.len(), 1);
        assert_eq!(result.all_pack_refs.len(), 2);
        assert_eq!(result.all_pack_refs[1].first_id, 51);
        assert_eq!(result.all_pack_refs[1].last_id, 55);
    }

    #[test]
    fn test_incremental_string_packs_from_empty() {
        let new_entries: Vec<(u32, Vec<u8>)> = (0..5)
            .map(|i| make_string_entry(i, &format!("val_{i}")))
            .collect();
        let new_refs: Vec<(u32, &[u8])> = new_entries
            .iter()
            .map(|(id, v)| (*id, v.as_slice()))
            .collect();

        let result = build_incremental_string_packs(&[], &new_refs).unwrap();
        assert_eq!(result.new_packs.len(), 1);
        assert_eq!(result.all_pack_refs.len(), 1);
        assert_eq!(result.all_pack_refs[0].first_id, 0);
        assert_eq!(result.all_pack_refs[0].last_id, 4);
    }

    // ---- Reverse tree tests ----

    fn build_test_tree(entries: &[ReverseEntry]) -> (DictBranch, Vec<Vec<u8>>) {
        let result = build_reverse_tree(entries.to_vec(), DEFAULT_TARGET_LEAF_BYTES).unwrap();

        // Extract leaf bytes in order
        let leaf_bytes: Vec<Vec<u8>> = result.leaves.iter().map(|l| l.bytes.clone()).collect();
        // The branch has pending:hash addresses — pretend they're real
        let mut branch = result.branch;
        for (i, leaf) in branch.leaves.iter_mut().enumerate() {
            leaf.address = format!("leaf_{i}");
        }
        (branch, leaf_bytes)
    }

    #[test]
    fn test_reverse_tree_update_no_new_entries() {
        let entries = vec![
            make_reverse_entry("alpha", 1),
            make_reverse_entry("beta", 2),
            make_reverse_entry("gamma", 3),
        ];
        let (branch, _leaf_bytes) = build_test_tree(&entries);

        let result = update_reverse_tree(&branch, &[], DEFAULT_TARGET_LEAF_BYTES, &mut |_| {
            panic!("should not fetch")
        })
        .unwrap();

        assert!(result.new_leaves.is_empty());
        assert!(result.replaced_leaf_indices.is_empty());
        assert_eq!(result.branch.leaves.len(), branch.leaves.len());
    }

    #[test]
    fn test_reverse_tree_update_inserts_new_entry() {
        let entries = vec![
            make_reverse_entry("alpha", 1),
            make_reverse_entry("gamma", 3),
        ];
        let (branch, leaf_bytes) = build_test_tree(&entries);

        let new_entries = vec![make_reverse_entry("beta", 2)];
        let result = update_reverse_tree(
            &branch,
            &new_entries,
            DEFAULT_TARGET_LEAF_BYTES,
            &mut |idx| Ok(leaf_bytes[idx].clone()),
        )
        .unwrap();

        // Should have 1 replaced leaf and 1 new leaf
        assert_eq!(result.replaced_leaf_indices.len(), 1);
        assert_eq!(result.new_leaves.len(), 1);

        // Verify the new leaf contains all 3 entries
        let new_leaf = ReverseLeaf::from_bytes(&result.new_leaves[0].bytes).unwrap();
        assert_eq!(new_leaf.entry_count(), 3);
        assert_eq!(new_leaf.lookup(b"alpha"), Some(1));
        assert_eq!(new_leaf.lookup(b"beta"), Some(2));
        assert_eq!(new_leaf.lookup(b"gamma"), Some(3));
    }

    #[test]
    fn test_reverse_tree_update_empty_branch() {
        let branch = DictBranch { leaves: vec![] };
        let new_entries = vec![
            make_reverse_entry("hello", 1),
            make_reverse_entry("world", 2),
        ];

        let result = update_reverse_tree(
            &branch,
            &new_entries,
            DEFAULT_TARGET_LEAF_BYTES,
            &mut |_| panic!("should not fetch"),
        )
        .unwrap();

        assert_eq!(result.new_leaves.len(), 1);
        assert!(result.replaced_leaf_indices.is_empty());
        assert_eq!(result.branch.leaves.len(), 1);

        let leaf = ReverseLeaf::from_bytes(&result.new_leaves[0].bytes).unwrap();
        assert_eq!(leaf.lookup(b"hello"), Some(1));
        assert_eq!(leaf.lookup(b"world"), Some(2));
    }

    #[test]
    fn test_reverse_tree_split_oversized_leaf() {
        // Build a tree with a single small leaf
        let mut entries: Vec<ReverseEntry> = (0..50)
            .map(|i| make_reverse_entry(&format!("key_{i:04}"), i as u64))
            .collect();
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        let (branch, leaf_bytes) = build_test_tree(&entries);

        // Add enough entries to exceed split threshold with a tiny target
        let mut new_entries: Vec<ReverseEntry> = (50..150)
            .map(|i| make_reverse_entry(&format!("key_{i:04}"), i as u64))
            .collect();
        new_entries.sort_by(|a, b| a.key.cmp(&b.key));

        // Use a very small target to force split
        let tiny_target = 100;
        let result = update_reverse_tree(&branch, &new_entries, tiny_target, &mut |idx| {
            Ok(leaf_bytes[idx].clone())
        })
        .unwrap();

        // Should have split into 2 new leaves
        assert!(
            result.new_leaves.len() >= 2,
            "expected split into >=2 leaves, got {}",
            result.new_leaves.len()
        );
        assert_eq!(result.branch.leaves.len(), result.new_leaves.len());

        // Verify all entries are findable across leaves
        for i in 0..150u64 {
            let key = format!("key_{i:04}");
            let found = result.new_leaves.iter().any(|l| {
                let leaf = ReverseLeaf::from_bytes(&l.bytes).unwrap();
                leaf.lookup(key.as_bytes()) == Some(i)
            });
            assert!(found, "entry key_{i:04} not found in any leaf");
        }
    }

    #[test]
    fn test_reverse_tree_multiple_leaves_partial_touch() {
        // Build a tree with enough entries to span multiple leaves
        let mut entries: Vec<ReverseEntry> = (0..200)
            .map(|i| ReverseEntry {
                key: format!("key_{i:06}").into_bytes(),
                id: i as u64,
            })
            .collect();
        entries.sort_by(|a, b| a.key.cmp(&b.key));

        // Use a small target to get multiple leaves
        let tree = build_reverse_tree(entries, 500).unwrap();
        let leaf_bytes: Vec<Vec<u8>> = tree.leaves.iter().map(|l| l.bytes.clone()).collect();
        let mut branch = tree.branch;
        for (i, leaf) in branch.leaves.iter_mut().enumerate() {
            leaf.address = format!("real_addr_{i}");
        }
        let original_count = branch.leaves.len();
        assert!(original_count >= 3, "need >=3 leaves, got {original_count}");

        // New entries that only touch the LAST leaf's range
        let last_key = &branch.leaves.last().unwrap().last_key;
        // Insert something after the last key
        let new_key = format!("{}z", std::str::from_utf8(last_key).unwrap_or("zzz"));
        let new_entries = vec![ReverseEntry {
            key: new_key.into_bytes(),
            id: 999,
        }];

        let result = update_reverse_tree(&branch, &new_entries, 500, &mut |idx| {
            Ok(leaf_bytes[idx].clone())
        })
        .unwrap();

        // Only the last leaf should be replaced
        assert_eq!(result.replaced_leaf_indices.len(), 1);
        assert_eq!(result.replaced_leaf_indices[0], original_count - 1);

        // Earlier leaves should keep their real addresses
        for i in 0..original_count - 1 {
            assert_eq!(result.branch.leaves[i].address, format!("real_addr_{i}"));
        }

        // Last leaf(s) should have pending addresses
        let last_entry = result.branch.leaves.last().unwrap();
        assert!(
            last_entry.address.starts_with("pending:"),
            "new leaf should have pending address"
        );
    }

    #[test]
    fn test_entry_slicing_half_open() {
        let leaves = vec![
            BranchLeafEntry {
                first_key: b"aaa".to_vec(),
                last_key: b"azz".to_vec(),
                entry_count: 10,
                address: "leaf_0".into(),
            },
            BranchLeafEntry {
                first_key: b"baa".to_vec(),
                last_key: b"bzz".to_vec(),
                entry_count: 10,
                address: "leaf_1".into(),
            },
            BranchLeafEntry {
                first_key: b"caa".to_vec(),
                last_key: b"czz".to_vec(),
                entry_count: 10,
                address: "leaf_2".into(),
            },
        ];

        let entries = vec![
            make_reverse_entry("abc", 1), // leaf 0
            make_reverse_entry("baa", 2), // exactly at leaf 1 boundary → leaf 1
            make_reverse_entry("bcd", 3), // leaf 1
            make_reverse_entry("def", 4), // after leaf 2 → last leaf
        ];

        let slices = slice_entries_to_leaves(&entries, &leaves);
        assert_eq!(slices.len(), 3);
        assert_eq!(slices[0].len(), 1); // abc
        assert_eq!(slices[1].len(), 2); // baa, bcd
        assert_eq!(slices[2].len(), 1); // def
    }
}
