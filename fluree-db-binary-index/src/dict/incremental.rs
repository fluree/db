//! Incremental dictionary update: append new forward packs + CoW reverse tree update.
//!
//! ## Forward Packs (append + compaction)
//!
//! New entries always have IDs above the existing watermark, so new packs are
//! built from only the new entries and appended to the existing routing list.
//!
//! Appending alone grows the routing table once per index build forever, so
//! qualifying runs are then merged back down: see [`plan_compaction`].
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
// Forward Pack Compaction
// ============================================================================

/// Packs at or below this size are merged on sight, whatever their ratio.
///
/// Below one page a mapping cannot pay for itself, and these are the packs that
/// drove the routing table to five figures. Collapsing them unconditionally is
/// what clears that backlog; the growth rule below governs everything larger.
pub const COMPACTION_SMALL_PACK_BYTES: u64 = 64 * 1024;

/// Maximum encoded size of a compacted pack. Also the point at which a pack
/// stops absorbing: nothing can merge into a pack already at the target.
pub const COMPACTION_MAX_OUTPUT_BYTES: u64 = DEFAULT_TARGET_PACK_BYTES as u64;

/// Longest run one merge may consume. Bounds the objects a single merge
/// fetches, and so the bytes it holds in memory at once.
pub const COMPACTION_MAX_WINDOW: usize = 32;

/// Packs to gather before merging peers of a similar size.
///
/// This is the write-amplification guard, and wide beats narrow: merging `F`
/// peers at a time rewrites each byte once per tier it climbs, so the cost of
/// reaching the target is `log_F(target / smallest)` — about 3x at `F = 8`,
/// versus roughly 8x for a scheme that absorbs into one growing pack (fanout 2,
/// whose absorbed tail must itself be built by merges).
const MIN_MERGE_PACKS: usize = 8;

/// How far peer sizes may spread within one merge. Keeps a large pack from
/// being rewritten to absorb a much smaller one, which is what makes the
/// tiering implicit — no level has to be recorded anywhere.
const MAX_SIZE_RATIO: u64 = 4;

/// A group at or above this share of the output target may merge with fewer
/// than [`MIN_MERGE_PACKS`] members.
///
/// Without this a stream stalls short of the target: once packs reach a size
/// where eight of them exceed the 16 MiB cap, no full-width group can ever
/// form again, and the largest achievable pack is pinned at a fraction of the
/// target (from 113-byte input, the fanout ladder tops out near 3.7 MiB).
const NEAR_TARGET_SHARE: u64 = 2;

/// A planned merge of `refs[start..end]` into one pack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackCompaction {
    /// First pack in the run (inclusive).
    pub start: usize,
    /// One past the last pack in the run.
    pub end: usize,
    /// Combined encoded size of the inputs, and so of the output.
    pub input_bytes: u64,
}

/// Plan a merge for one forward-dictionary stream, or `None` to leave it.
///
/// `sizes[i]` is the encoded byte length of `refs[i]`. Scans left to right and
/// returns the **longest qualifying contiguous run** from the earliest position
/// that has one.
///
/// A run qualifies when its members are of comparable size (or all small), and
/// there are either enough of them or enough bytes to be worth a rewrite.
/// Nothing about the decision needs to know which packs were previously
/// compacted — the rule is scale-free, which is why no compaction state is
/// carried in the root.
///
/// Runs rather than suffixes, and left to right rather than from the tail,
/// because a merge output is larger than the packs around it. Anchoring every
/// candidate at the newest pack means that once a merge lands, every candidate
/// contains it, it fails the size ratio against its smaller neighbours, and any
/// backlog sitting *behind* it becomes permanently unreachable. Scanning for a
/// run lets that older fragmentation be repaired in place.
///
/// `scan_from` skips positions already known not to start a qualifying run.
/// Replacing a run with its (larger) merge output cannot make an earlier start
/// qualify — the run would be shorter and its size spread wider — so a caller
/// merging repeatedly can carry the previous `start` forward and keep the whole
/// drain linear instead of rescanning the table on every merge.
pub fn plan_compaction(
    refs: &[PackBranchEntry],
    sizes: &[u64],
    max_output_bytes: u64,
    scan_from: usize,
) -> Option<PackCompaction> {
    debug_assert_eq!(refs.len(), sizes.len());
    if refs.len() < 2 {
        return None;
    }

    for start in scan_from.min(refs.len() - 1)..refs.len() - 1 {
        // Grow the run while it stays adjacent, bounded, and under the target.
        let mut end = start + 1;
        let mut total = sizes[start];
        while end < refs.len()
            && end - start < COMPACTION_MAX_WINDOW
            && refs[end].first_id == refs[end - 1].last_id.saturating_add(1)
            && total + sizes[end] <= max_output_bytes
        {
            total += sizes[end];
            end += 1;
        }

        // Longest first, shrinking from the far end.
        let (mut e, mut t) = (end, total);
        while e > start + 1 {
            if qualifies_for_merge(&sizes[start..e], t, max_output_bytes) {
                return Some(PackCompaction {
                    start,
                    end: e,
                    input_bytes: t,
                });
            }
            e -= 1;
            t -= sizes[e];
        }
    }

    None
}

fn qualifies_for_merge(sizes: &[u64], total: u64, max_output_bytes: u64) -> bool {
    let largest = sizes.iter().copied().max().unwrap_or(0);
    let smallest = sizes.iter().copied().min().unwrap_or(0);

    let comparable = largest <= COMPACTION_SMALL_PACK_BYTES
        || largest <= smallest.saturating_mul(MAX_SIZE_RATIO);

    comparable && (sizes.len() >= MIN_MERGE_PACKS || total >= max_output_bytes / NEAR_TARGET_SHARE)
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

    // ---- Compaction policy ----

    /// Contiguous refs, one per size, starting at id 0.
    fn contiguous_refs(sizes: &[u64]) -> Vec<PackBranchEntry> {
        let mut refs = Vec::with_capacity(sizes.len());
        let mut next_id = 0u64;
        for (i, _) in sizes.iter().enumerate() {
            refs.push(PackBranchEntry {
                first_id: next_id,
                last_id: next_id + 9,
                pack_cid: dummy_cid(i as u32),
            });
            next_id += 10;
        }
        refs
    }

    fn plan(sizes: &[u64]) -> Option<PackCompaction> {
        plan_compaction(
            &contiguous_refs(sizes),
            sizes,
            COMPACTION_MAX_OUTPUT_BYTES,
            0,
        )
    }

    #[test]
    fn a_single_pack_is_never_compacted() {
        assert_eq!(plan(&[]), None);
        assert_eq!(plan(&[113]), None);
    }

    #[test]
    fn tiny_packs_collapse_in_one_merge() {
        // The observed pathology: a long run of ~113-byte packs. The whole run
        // merges at once rather than one tier per cycle.
        let sizes = vec![113u64; 20];
        let p = plan(&sizes).expect("tiny run must compact");
        assert_eq!(p.start, 0);
        // Bounded by the window, not by the whole table.
        assert_eq!(p.end, COMPACTION_MAX_WINDOW.min(20));
        assert_eq!(p.input_bytes, 113 * p.end as u64);

        // But a couple of tiny packs are not yet worth a rewrite.
        assert_eq!(plan(&[113, 113]), None);
    }

    #[test]
    fn peers_merge_only_when_comparably_sized() {
        let mb = 1024 * 1024u64;

        // Eight peers within the size ratio.
        assert!(plan(&[mb; 8]).is_some(), "eight equal packs must merge");

        // A pack far larger than its neighbours is left alone rather than
        // rewritten to absorb them.
        let mut lopsided = vec![8 * mb];
        lopsided.extend(std::iter::repeat_n(64 * 1024 + 1, 7));
        assert_eq!(
            plan(&lopsided),
            None,
            "a dominant pack must not be rewritten for a much smaller tail"
        );
    }

    #[test]
    fn a_pack_near_the_target_stops_being_rewritten() {
        let near_target = 15 * 1024 * 1024u64;
        assert_eq!(plan(&[near_target, 100 * 1024]), None);

        let at_target = COMPACTION_MAX_OUTPUT_BYTES;
        assert_eq!(plan(&[at_target, at_target]), None);
    }

    #[test]
    fn a_near_target_group_merges_below_full_width() {
        // The rule that keeps a stream from stalling short of the target:
        // four 3.7 MiB peers cannot form a group of eight without blowing the
        // 16 MiB cap, so they must be allowed to merge at four.
        let big = 3_700_000u64;
        let p = plan(&[big; 4]).expect("a near-target group must merge below full width");
        assert_eq!((p.start, p.end), (0, 4));
        assert_eq!(p.input_bytes, big * 4);
    }

    #[test]
    fn an_oversized_group_shrinks_to_the_longest_fitting_suffix() {
        let mb = 1024 * 1024u64;
        // 12+6+6 exceeds 16 MiB; the 6+6 suffix fits and is near-target.
        let p = plan(&[12 * mb, 6 * mb, 6 * mb]).expect("run must compact");
        assert_eq!((p.start, p.end), (1, 3));
        assert_eq!(p.input_bytes, 12 * mb);
    }

    #[test]
    fn non_contiguous_packs_are_not_merged() {
        // Gaps between packs are legal on disk; merging across one would
        // produce a pack spanning ids it has no values for.
        let sizes = vec![113u64; 20];
        let mut refs = contiguous_refs(&sizes);
        refs[9].first_id += 5; // punch a hole ahead of pack 9

        // The run before the gap is itself long enough to merge, and must stop
        // at the gap rather than span it.
        let p = plan_compaction(&refs, &sizes, COMPACTION_MAX_OUTPUT_BYTES, 0)
            .expect("the run before the gap must compact");
        assert_eq!((p.start, p.end), (0, 9));

        // With too few packs ahead of the gap, the run behind it is chosen.
        let sizes = vec![113u64; 20];
        let mut refs = contiguous_refs(&sizes);
        refs[2].first_id += 5;
        let p = plan_compaction(&refs, &sizes, COMPACTION_MAX_OUTPUT_BYTES, 0)
            .expect("the run after the gap must compact");
        assert_eq!((p.start, p.end), (2, 20));
    }

    #[test]
    fn a_preexisting_backlog_drains_instead_of_stalling() {
        // A table inherited from before compaction existed: thousands of tiny
        // packs, nothing new appended. Suffix-anchored planning stalls here —
        // the first merge puts a >64 KiB pack at the tail, every later
        // candidate contains it, none clear the size ratio, and the packs
        // behind it are stranded forever. Runs must reach them.
        let mut sizes = vec![113u64; 19_629];
        let mut refs = contiguous_refs(&sizes);
        let mut merges = 0;
        let mut scan = 0;

        while let Some(p) = plan_compaction(&refs, &sizes, COMPACTION_MAX_OUTPUT_BYTES, scan) {
            let merged: u64 = sizes[p.start..p.end].iter().sum();
            let entry = PackBranchEntry {
                first_id: refs[p.start].first_id,
                last_id: refs[p.end - 1].last_id,
                pack_cid: refs[p.start].pack_cid.clone(),
            };
            sizes.splice(p.start..p.end, [merged]);
            refs.splice(p.start..p.end, [entry]);
            scan = p.start;
            merges += 1;
            assert!(merges < 5_000, "backlog drain failed to converge");
        }

        assert!(
            sizes.len() < 100,
            "backlog stalled at {} packs after {merges} merges",
            sizes.len()
        );
    }

    #[test]
    fn repeated_merges_converge_on_the_output_target() {
        // The property a fixed-width fanout could not deliver: a stream fed
        // small packs forever must converge on the 16 MiB target, not stall at
        // a fraction of it. Simulates many index cycles, each appending one
        // small pack and compacting whatever qualifies.
        let small = 32 * 1024u64;
        let mut sizes: Vec<u64> = Vec::new();
        let mut rewritten = 0u64;

        for _ in 0..4000 {
            sizes.push(small);
            while let Some(p) = plan(&sizes) {
                let merged: u64 = sizes[p.start..p.end].iter().sum();
                rewritten += merged;
                sizes.splice(p.start..p.end, [merged]);
            }
        }

        let total: u64 = sizes.iter().sum();
        let largest = *sizes.iter().max().unwrap();

        assert!(
            largest > COMPACTION_MAX_OUTPUT_BYTES / 2,
            "largest pack {largest} never approached the {COMPACTION_MAX_OUTPUT_BYTES} target"
        );
        assert!(
            sizes.len()
                <= (total / COMPACTION_MAX_OUTPUT_BYTES + COMPACTION_MAX_WINDOW as u64) as usize,
            "pack count {} is not bounded by data size ({total} bytes)",
            sizes.len()
        );
        // Write amplification stays near the ~3x a fanout-8 ladder implies.
        assert!(
            rewritten < total * 4,
            "rewrote {rewritten} bytes for {total} bytes of data"
        );
    }
}
