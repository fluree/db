//! Bulk tree construction from sorted entries.
//!
//! Takes a sorted iterator of entries, partitions them into leaves of
//! approximately `target_leaf_bytes`, encodes each leaf, and produces
//! a branch manifest referencing the leaf artifacts.
//!
//! The caller is responsible for uploading the artifacts to CAS —
//! this module produces the raw bytes and computes SHA-256 hashes.

use std::io;

use super::branch::{BranchLeafEntry, DictBranch};
use super::reverse_leaf::{self, ReverseEntry};

/// Result of building a dictionary tree.
#[derive(Debug)]
pub struct TreeBuildResult {
    /// The branch manifest (ready to encode and upload).
    pub branch: DictBranch,
    /// Branch bytes (encoded, ready for CAS upload).
    pub branch_bytes: Vec<u8>,
    /// SHA-256 hex hash of the branch bytes.
    pub branch_hash: String,
    /// Leaf artifacts: (hash, bytes) pairs ready for CAS upload.
    pub leaves: Vec<LeafArtifact>,
}

/// A single leaf artifact produced during tree building.
#[derive(Debug)]
pub struct LeafArtifact {
    /// SHA-256 hex hash of the leaf bytes.
    pub hash: String,
    /// Encoded leaf bytes.
    pub bytes: Vec<u8>,
}

/// Target leaf size in bytes. Larger leaves reduce tree depth and file count
/// at the cost of reading more data per lookup. 2MB keeps the leaf count
/// manageable for large dictionaries while staying cache-friendly.
pub const DEFAULT_TARGET_LEAF_BYTES: usize = 2 * 1024 * 1024;

/// Build a reverse tree (key → id) from sorted entries.
///
/// `entries` must be sorted by `key` in ascending byte order.
pub fn build_reverse_tree(
    entries: Vec<ReverseEntry>,
    target_leaf_bytes: usize,
) -> io::Result<TreeBuildResult> {
    if entries.is_empty() {
        return Ok(empty_tree_result());
    }

    let mut leaves = Vec::new();
    let mut branch_entries = Vec::new();

    let mut chunk_start = 0;
    let mut chunk_bytes = 0usize;

    for (i, entry) in entries.iter().enumerate() {
        let entry_size = 12 + entry.key.len(); // key_len(4) + key + id(8)
        chunk_bytes += entry_size;

        let is_last = i == entries.len() - 1;
        if chunk_bytes >= target_leaf_bytes || is_last {
            let chunk = &entries[chunk_start..=i];
            let leaf_bytes = reverse_leaf::encode_reverse_leaf(chunk);
            let hash = fluree_db_core::sha256_hex(&leaf_bytes);

            branch_entries.push(BranchLeafEntry {
                first_key: chunk.first().unwrap().key.clone(),
                last_key: chunk.last().unwrap().key.clone(),
                entry_count: chunk.len() as u32,
                address: format!("pending:{hash}"),
            });

            leaves.push(LeafArtifact {
                hash,
                bytes: leaf_bytes,
            });

            chunk_start = i + 1;
            chunk_bytes = 0;
        }
    }

    let branch = DictBranch {
        leaves: branch_entries,
    };
    let branch_bytes = branch.encode();
    let branch_hash = fluree_db_core::sha256_hex(&branch_bytes);

    Ok(TreeBuildResult {
        branch,
        branch_bytes,
        branch_hash,
        leaves,
    })
}

/// Produce an empty tree result (for empty dictionaries).
fn empty_tree_result() -> TreeBuildResult {
    let branch = DictBranch { leaves: vec![] };
    let branch_bytes = branch.encode();
    let branch_hash = fluree_db_core::sha256_hex(&branch_bytes);
    TreeBuildResult {
        branch,
        branch_bytes,
        branch_hash,
        leaves: vec![],
    }
}

/// After CAS upload, update branch leaf addresses from pending hashes
/// to actual CAS addresses. Returns updated branch bytes + hash.
///
/// `hash_to_address` maps leaf SHA-256 hex → CAS address string.
pub fn finalize_branch(
    mut branch: DictBranch,
    hash_to_address: &std::collections::HashMap<String, String>,
) -> io::Result<(DictBranch, Vec<u8>, String)> {
    for leaf in &mut branch.leaves {
        if let Some(hash) = leaf.address.strip_prefix("pending:") {
            match hash_to_address.get(hash) {
                Some(addr) => leaf.address = addr.clone(),
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("no CAS address for leaf hash {hash}"),
                    ));
                }
            }
        }
    }
    let branch_bytes = branch.encode();
    let branch_hash = fluree_db_core::sha256_hex(&branch_bytes);
    Ok((branch, branch_bytes, branch_hash))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_reverse_tree() {
        let mut entries: Vec<ReverseEntry> = (0..100)
            .map(|i| ReverseEntry {
                key: format!("key_{i:04}").into_bytes(),
                id: i as u64,
            })
            .collect();
        entries.sort_by(|a, b| a.key.cmp(&b.key));

        let result = build_reverse_tree(entries, DEFAULT_TARGET_LEAF_BYTES).unwrap();
        assert_eq!(result.leaves.len(), 1);
        assert_eq!(result.branch.total_entries(), 100);
    }

    #[test]
    fn test_build_empty_reverse_tree() {
        let result = build_reverse_tree(vec![], DEFAULT_TARGET_LEAF_BYTES).unwrap();
        assert_eq!(result.leaves.len(), 0);
        assert_eq!(result.branch.leaves.len(), 0);
    }

    #[test]
    fn test_finalize_branch() {
        let entries: Vec<ReverseEntry> = (0..10)
            .map(|i| ReverseEntry {
                key: format!("key_{i:04}").into_bytes(),
                id: i as u64,
            })
            .collect();

        let result = build_reverse_tree(entries, DEFAULT_TARGET_LEAF_BYTES).unwrap();
        let leaf_hash = &result.leaves[0].hash;

        let mut map = std::collections::HashMap::new();
        map.insert(leaf_hash.clone(), "cas://real_address".to_string());

        let (finalized, _, _) = finalize_branch(result.branch, &map).unwrap();
        assert_eq!(finalized.leaves[0].address, "cas://real_address");
    }
}
