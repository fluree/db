//! Dictionary tree branch format.
//!
//! A branch is a single-level index mapping key ranges to leaf CAS addresses.
//! Supports both fixed-width (u64 ID) and variable-length (byte string) keys.
//!
//! ## Binary layout
//!
//! ```text
//! [magic: 4B "DTB1"]
//! [leaf_count: u32]
//! [offset_table: u32 × leaf_count]  // byte offset of each leaf entry
//! [leaf entries...]
//!   entry :=
//!     [first_key_len: u32] [first_key_bytes]
//!     [last_key_len: u32]  [last_key_bytes]
//!     [entry_count: u32]
//!     [address_len: u16]   [address_bytes]
//! ```
//!
//! Leaf entries are sorted by their key range. Binary search on `first_key`
//! finds the correct leaf for any lookup key.

use std::io;

/// Magic bytes for a dictionary tree branch.
pub const BRANCH_MAGIC: [u8; 4] = *b"DTB1";

/// Header size: magic (4) + leaf_count (4).
const HEADER_SIZE: usize = 8;

/// A single leaf descriptor within a branch.
#[derive(Debug, Clone)]
pub struct BranchLeafEntry {
    /// First key in this leaf (inclusive lower bound).
    pub first_key: Vec<u8>,
    /// Last key in this leaf (inclusive upper bound).
    pub last_key: Vec<u8>,
    /// Number of entries in this leaf.
    pub entry_count: u32,
    /// CAS address of the leaf file.
    pub address: String,
}

/// Decoded dictionary tree branch.
#[derive(Debug, Clone)]
pub struct DictBranch {
    pub leaves: Vec<BranchLeafEntry>,
}

impl DictBranch {
    /// Encode branch to binary format for CAS storage.
    pub fn encode(&self) -> Vec<u8> {
        let leaf_count = self.leaves.len() as u32;
        let offset_table_size = self.leaves.len() * 4;

        // Pre-compute total size
        let entries_size: usize = self
            .leaves
            .iter()
            .map(|l| {
                4 + l.first_key.len()    // first_key_len + bytes
            + 4 + l.last_key.len()   // last_key_len + bytes
            + 4                      // entry_count
            + 2 + l.address.len() // address_len + bytes
            })
            .sum();

        let total = HEADER_SIZE + offset_table_size + entries_size;
        let mut buf = Vec::with_capacity(total);

        // Header
        buf.extend_from_slice(&BRANCH_MAGIC);
        buf.extend_from_slice(&leaf_count.to_le_bytes());

        // Offset table
        let mut offset: u32 = 0;
        for l in &self.leaves {
            buf.extend_from_slice(&offset.to_le_bytes());
            let entry_size = 4 + l.first_key.len() + 4 + l.last_key.len() + 4 + 2 + l.address.len();
            offset += entry_size as u32;
        }

        // Leaf entries
        for l in &self.leaves {
            buf.extend_from_slice(&(l.first_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&l.first_key);
            buf.extend_from_slice(&(l.last_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&l.last_key);
            buf.extend_from_slice(&l.entry_count.to_le_bytes());
            buf.extend_from_slice(&(l.address.len() as u16).to_le_bytes());
            buf.extend_from_slice(l.address.as_bytes());
        }

        debug_assert_eq!(buf.len(), total);
        buf
    }

    /// Decode branch from binary format.
    pub fn decode(data: &[u8]) -> io::Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "dict branch: too small for header",
            ));
        }
        if data[0..4] != BRANCH_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "dict branch: invalid magic",
            ));
        }
        let leaf_count = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
        let offset_table_start = HEADER_SIZE;
        let data_section_start = offset_table_start + leaf_count * 4;

        if data.len() < data_section_start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "dict branch: truncated offset table",
            ));
        }

        let mut leaves = Vec::with_capacity(leaf_count);
        for i in 0..leaf_count {
            let table_pos = offset_table_start + i * 4;
            let relative =
                u32::from_le_bytes(data[table_pos..table_pos + 4].try_into().unwrap()) as usize;
            let mut pos = data_section_start + relative;

            // first_key
            let fk_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let first_key = data[pos..pos + fk_len].to_vec();
            pos += fk_len;

            // last_key
            let lk_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let last_key = data[pos..pos + lk_len].to_vec();
            pos += lk_len;

            // entry_count
            let entry_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;

            // address
            let addr_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            let address = String::from_utf8(data[pos..pos + addr_len].to_vec())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            leaves.push(BranchLeafEntry {
                first_key,
                last_key,
                entry_count,
                address,
            });
        }

        Ok(Self { leaves })
    }

    /// Find the leaf index whose key range contains `target_key`.
    ///
    /// Uses binary search on `first_key`. Returns the index of the leaf
    /// whose range includes `target_key`, or `None` if the key is outside
    /// all ranges.
    pub fn find_leaf(&self, target_key: &[u8]) -> Option<usize> {
        if self.leaves.is_empty() {
            return None;
        }

        // Binary search: find the last leaf whose first_key <= target_key
        let mut lo = 0usize;
        let mut hi = self.leaves.len();

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.leaves[mid].first_key.as_slice() <= target_key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        // lo is now the first leaf whose first_key > target_key.
        // The candidate is lo - 1.
        if lo == 0 {
            return None;
        }
        let idx = lo - 1;

        // Verify the key falls within this leaf's range
        if target_key <= self.leaves[idx].last_key.as_slice() {
            Some(idx)
        } else {
            None
        }
    }

    /// Total entry count across all leaves.
    pub fn total_entries(&self) -> u64 {
        self.leaves.iter().map(|l| l.entry_count as u64).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id_to_branch_key(id: u64) -> Vec<u8> {
        id.to_be_bytes().to_vec()
    }

    fn make_branch() -> DictBranch {
        DictBranch {
            leaves: vec![
                BranchLeafEntry {
                    first_key: id_to_branch_key(0),
                    last_key: id_to_branch_key(99),
                    entry_count: 100,
                    address: "cas://leaf_0".into(),
                },
                BranchLeafEntry {
                    first_key: id_to_branch_key(100),
                    last_key: id_to_branch_key(199),
                    entry_count: 100,
                    address: "cas://leaf_1".into(),
                },
                BranchLeafEntry {
                    first_key: id_to_branch_key(200),
                    last_key: id_to_branch_key(299),
                    entry_count: 100,
                    address: "cas://leaf_2".into(),
                },
            ],
        }
    }

    #[test]
    fn test_round_trip() {
        let branch = make_branch();
        let blob = branch.encode();
        let decoded = DictBranch::decode(&blob).unwrap();

        assert_eq!(decoded.leaves.len(), 3);
        assert_eq!(decoded.leaves[0].entry_count, 100);
        assert_eq!(decoded.leaves[0].address, "cas://leaf_0");
        assert_eq!(decoded.leaves[2].address, "cas://leaf_2");
    }

    #[test]
    fn test_find_leaf_variable_keys() {
        let branch = DictBranch {
            leaves: vec![
                BranchLeafEntry {
                    first_key: b"aaa".to_vec(),
                    last_key: b"azz".to_vec(),
                    entry_count: 50,
                    address: "cas://a_leaf".into(),
                },
                BranchLeafEntry {
                    first_key: b"baa".to_vec(),
                    last_key: b"bzz".to_vec(),
                    entry_count: 50,
                    address: "cas://b_leaf".into(),
                },
            ],
        };

        assert_eq!(branch.find_leaf(b"abc"), Some(0));
        assert_eq!(branch.find_leaf(b"bcd"), Some(1));
        assert_eq!(branch.find_leaf(b"zzz"), None);
    }

    #[test]
    fn test_empty_branch() {
        let branch = DictBranch { leaves: vec![] };
        let blob = branch.encode();
        let decoded = DictBranch::decode(&blob).unwrap();
        assert_eq!(decoded.leaves.len(), 0);
        assert_eq!(decoded.find_leaf(b"anything"), None);
    }

    #[test]
    fn test_total_entries() {
        let branch = make_branch();
        assert_eq!(branch.total_entries(), 300);
    }
}
