//! Reverse leaf format: variable-length key → numeric ID.
//!
//! Used by both subject reverse (`(ns_code, suffix)` → sid64) and string
//! reverse (`value` → string_id) trees. Entries are sorted by key for
//! binary search.
//!
//! ## Binary layout
//!
//! ```text
//! [magic: 4B "DLR1"]
//! [entry_count: u32]
//! [offset_table: u32 × entry_count]  // byte offset of each entry in data section
//! [data section: entries...]
//!   entry := [key_len: u32] [key_bytes: u8 × key_len] [id: u64]
//! ```
//!
//! ## Key Format
//!
//! For **string reverse** trees, the key is the raw UTF-8 value bytes.
//! Sorted in UTF-8 byte order.
//!
//! For **subject reverse** trees, the key is `[ns_code: u16 BE] [suffix: UTF-8]`.
//! The big-endian ns_code prefix ensures lexicographic byte comparison
//! matches the logical `(ns_code, suffix)` ordering.

use std::io;

/// Magic bytes for a reverse leaf file.
pub const REVERSE_LEAF_MAGIC: [u8; 4] = *b"DLR1";

/// Header size: magic (4) + entry_count (4).
const HEADER_SIZE: usize = 8;

/// Fixed overhead per entry: key_len (4) + id (8).
const ENTRY_OVERHEAD: usize = 12;

/// A single reverse leaf entry (key → id).
#[derive(Debug, Clone)]
pub struct ReverseEntry {
    /// For string reverse: raw UTF-8 value.
    /// For subject reverse: `[ns_code: 2B BE] [suffix bytes]`.
    pub key: Vec<u8>,
    pub id: u64,
}

/// Build a subject reverse key from (ns_code, suffix).
///
/// Encodes ns_code as big-endian u16 prefix so lexicographic byte
/// comparison matches `(ns_code, suffix)` ordering.
pub fn subject_reverse_key(ns_code: u16, suffix: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + suffix.len());
    key.extend_from_slice(&ns_code.to_be_bytes());
    key.extend_from_slice(suffix);
    key
}

/// Extract (ns_code, suffix) from a subject reverse key.
pub fn split_subject_reverse_key(key: &[u8]) -> (u16, &[u8]) {
    debug_assert!(key.len() >= 2);
    let ns_code = u16::from_be_bytes(key[0..2].try_into().unwrap());
    (ns_code, &key[2..])
}

/// Encode a sorted slice of reverse entries into a leaf blob.
///
/// Entries **must** be sorted by `key` in ascending byte order.
pub fn encode_reverse_leaf(entries: &[ReverseEntry]) -> Vec<u8> {
    let entry_count = entries.len() as u32;

    let data_size: usize = entries.iter().map(|e| ENTRY_OVERHEAD + e.key.len()).sum();
    let offset_table_size = entries.len() * 4;
    let total = HEADER_SIZE + offset_table_size + data_size;

    let mut buf = Vec::with_capacity(total);

    // Header
    buf.extend_from_slice(&REVERSE_LEAF_MAGIC);
    buf.extend_from_slice(&entry_count.to_le_bytes());

    // Offset table
    let mut offset: u32 = 0;
    for e in entries {
        buf.extend_from_slice(&offset.to_le_bytes());
        offset += (ENTRY_OVERHEAD + e.key.len()) as u32;
    }

    // Data section
    for e in entries {
        buf.extend_from_slice(&(e.key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&e.key);
        buf.extend_from_slice(&e.id.to_le_bytes());
    }

    debug_assert_eq!(buf.len(), total);
    buf
}

/// Decoded reverse leaf providing O(log n) lookup by key.
pub struct ReverseLeaf<'a> {
    data: &'a [u8],
    entry_count: u32,
    offset_table_start: usize,
    data_section_start: usize,
}

impl<'a> ReverseLeaf<'a> {
    /// Parse a reverse leaf from raw bytes.
    pub fn from_bytes(data: &'a [u8]) -> io::Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "reverse leaf: too small for header",
            ));
        }
        if data[0..4] != REVERSE_LEAF_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "reverse leaf: invalid magic",
            ));
        }
        let entry_count = u32::from_le_bytes(data[4..8].try_into().unwrap());
        let offset_table_start = HEADER_SIZE;
        let data_section_start = offset_table_start + (entry_count as usize) * 4;

        if data.len() < data_section_start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "reverse leaf: truncated offset table",
            ));
        }

        Ok(Self {
            data,
            entry_count,
            offset_table_start,
            data_section_start,
        })
    }

    /// Number of entries in this leaf.
    pub fn entry_count(&self) -> u32 {
        self.entry_count
    }

    /// Byte offset of entry `index` within `self.data`.
    #[inline]
    fn entry_offset(&self, index: usize) -> usize {
        let table_pos = self.offset_table_start + index * 4;
        let relative =
            u32::from_le_bytes(self.data[table_pos..table_pos + 4].try_into().unwrap()) as usize;
        self.data_section_start + relative
    }

    /// Read the key bytes at the given index.
    #[inline]
    fn key_at(&self, index: usize) -> &'a [u8] {
        let offset = self.entry_offset(index);
        let key_len =
            u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap()) as usize;
        &self.data[offset + 4..offset + 4 + key_len]
    }

    /// Read the ID at the given index.
    #[inline]
    fn id_at(&self, index: usize) -> u64 {
        let offset = self.entry_offset(index);
        let key_len =
            u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap()) as usize;
        let id_offset = offset + 4 + key_len;
        u64::from_le_bytes(self.data[id_offset..id_offset + 8].try_into().unwrap())
    }

    /// Look up an ID by exact key match using binary search.
    pub fn lookup(&self, target_key: &[u8]) -> Option<u64> {
        if self.entry_count == 0 {
            return None;
        }

        let mut lo = 0usize;
        let mut hi = self.entry_count as usize;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mid_key = self.key_at(mid);
            match mid_key.cmp(target_key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(self.id_at(mid)),
            }
        }

        None
    }

    /// Return the first key in this leaf (for branch boundary keys).
    pub fn first_key(&self) -> Option<&'a [u8]> {
        if self.entry_count > 0 {
            Some(self.key_at(0))
        } else {
            None
        }
    }

    /// Return the last key in this leaf.
    pub fn last_key(&self) -> Option<&'a [u8]> {
        if self.entry_count > 0 {
            Some(self.key_at(self.entry_count as usize - 1))
        } else {
            None
        }
    }

    /// Iterate all entries in order.
    pub fn iter(&'a self) -> ReverseLeafIter<'a> {
        ReverseLeafIter {
            leaf: self,
            index: 0,
        }
    }

    /// Collect all entries whose key is in `[start_key, end_key)`.
    ///
    /// Uses binary search to find the starting position, then scans forward.
    pub fn scan_range(&self, start_key: &[u8], end_key: &[u8]) -> Vec<(&'a [u8], u64)> {
        if self.entry_count == 0 {
            return Vec::new();
        }

        // Binary search for the first entry >= start_key.
        let n = self.entry_count as usize;
        let mut lo = 0usize;
        let mut hi = n;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.key_at(mid) < start_key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        // Scan forward collecting entries while key < end_key.
        let mut results = Vec::new();
        for i in lo..n {
            let key = self.key_at(i);
            if key >= end_key {
                break;
            }
            results.push((key, self.id_at(i)));
        }
        results
    }
}

/// Iterator over reverse leaf entries.
pub struct ReverseLeafIter<'a> {
    leaf: &'a ReverseLeaf<'a>,
    index: usize,
}

impl<'a> Iterator for ReverseLeafIter<'a> {
    /// (key, id)
    type Item = (&'a [u8], u64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.leaf.entry_count as usize {
            return None;
        }
        let key = self.leaf.key_at(self.index);
        let id = self.leaf.id_at(self.index);
        self.index += 1;
        Some((key, id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_reverse_round_trip() {
        let mut entries: Vec<ReverseEntry> = vec![
            ReverseEntry {
                key: b"alpha".to_vec(),
                id: 100,
            },
            ReverseEntry {
                key: b"beta".to_vec(),
                id: 200,
            },
            ReverseEntry {
                key: b"gamma".to_vec(),
                id: 300,
            },
        ];
        entries.sort_by(|a, b| a.key.cmp(&b.key));

        let blob = encode_reverse_leaf(&entries);
        let leaf = ReverseLeaf::from_bytes(&blob).unwrap();

        assert_eq!(leaf.entry_count(), 3);
        assert_eq!(leaf.lookup(b"alpha"), Some(100));
        assert_eq!(leaf.lookup(b"beta"), Some(200));
        assert_eq!(leaf.lookup(b"gamma"), Some(300));
        assert_eq!(leaf.lookup(b"delta"), None);
    }

    #[test]
    fn test_subject_reverse_key_ordering() {
        // ns_code 2 < ns_code 3, and within same ns_code, suffix sorts
        let k1 = subject_reverse_key(2, b"aaa");
        let k2 = subject_reverse_key(2, b"bbb");
        let k3 = subject_reverse_key(3, b"aaa");

        assert!(k1 < k2);
        assert!(k2 < k3);
    }

    #[test]
    fn test_subject_reverse_round_trip() {
        let mut entries = vec![
            ReverseEntry {
                key: subject_reverse_key(2, b"Alice"),
                id: 2u64 << 48,
            },
            ReverseEntry {
                key: subject_reverse_key(2, b"Bob"),
                id: (2u64 << 48) | 1,
            },
            ReverseEntry {
                key: subject_reverse_key(3, b"Carol"),
                id: 3u64 << 48,
            },
        ];
        entries.sort_by(|a, b| a.key.cmp(&b.key));

        let blob = encode_reverse_leaf(&entries);
        let leaf = ReverseLeaf::from_bytes(&blob).unwrap();

        let key = subject_reverse_key(2, b"Bob");
        assert_eq!(leaf.lookup(&key), Some((2u64 << 48) | 1));

        let miss = subject_reverse_key(2, b"Dave");
        assert_eq!(leaf.lookup(&miss), None);
    }

    #[test]
    fn test_split_subject_key() {
        let key = subject_reverse_key(42, b"hello");
        let (ns, suffix) = split_subject_reverse_key(&key);
        assert_eq!(ns, 42);
        assert_eq!(suffix, b"hello");
    }

    #[test]
    fn test_empty_leaf() {
        let blob = encode_reverse_leaf(&[]);
        let leaf = ReverseLeaf::from_bytes(&blob).unwrap();
        assert_eq!(leaf.entry_count(), 0);
        assert!(leaf.first_key().is_none());
        assert!(leaf.lookup(b"anything").is_none());
    }

    #[test]
    fn test_first_last_key() {
        let entries = vec![
            ReverseEntry {
                key: b"aaa".to_vec(),
                id: 1,
            },
            ReverseEntry {
                key: b"zzz".to_vec(),
                id: 2,
            },
        ];
        let blob = encode_reverse_leaf(&entries);
        let leaf = ReverseLeaf::from_bytes(&blob).unwrap();

        assert_eq!(leaf.first_key(), Some(b"aaa".as_slice()));
        assert_eq!(leaf.last_key(), Some(b"zzz".as_slice()));
    }

    #[test]
    fn test_iterator() {
        let entries = vec![
            ReverseEntry {
                key: b"a".to_vec(),
                id: 10,
            },
            ReverseEntry {
                key: b"b".to_vec(),
                id: 20,
            },
            ReverseEntry {
                key: b"c".to_vec(),
                id: 30,
            },
        ];
        let blob = encode_reverse_leaf(&entries);
        let leaf = ReverseLeaf::from_bytes(&blob).unwrap();

        let collected: Vec<_> = leaf.iter().collect();
        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0], (b"a".as_slice(), 10));
        assert_eq!(collected[1], (b"b".as_slice(), 20));
        assert_eq!(collected[2], (b"c".as_slice(), 30));
    }

    #[test]
    fn test_large_reverse_leaf() {
        let mut entries: Vec<ReverseEntry> = (0..5000)
            .map(|i| ReverseEntry {
                key: format!("http://example.org/entity/{i:06}").into_bytes(),
                id: i as u64,
            })
            .collect();
        entries.sort_by(|a, b| a.key.cmp(&b.key));

        let blob = encode_reverse_leaf(&entries);
        let leaf = ReverseLeaf::from_bytes(&blob).unwrap();

        assert_eq!(leaf.entry_count(), 5000);

        // Spot-check
        let key = format!("http://example.org/entity/{:06}", 2500).into_bytes();
        assert_eq!(leaf.lookup(&key), Some(2500));

        let miss = b"http://example.org/nonexistent".to_vec();
        assert_eq!(leaf.lookup(&miss), None);
    }

    /// Regression test: reverse tree entries for subjects must store the full
    /// SubjectId (ns_code + local_id encoded via `SubjectId::new().as_u64()`),
    /// not just the raw local_id. Using raw local_id loses the namespace and
    /// breaks multi-pattern query JOINs that translate Sids back to s_ids via
    /// the reverse tree.
    #[test]
    fn test_subject_reverse_stores_full_subject_id() {
        use fluree_db_core::subject_id::SubjectId;

        let ns_code: u16 = 12;
        let local_id: u64 = 2;
        let suffix = b"person2";

        // Build entry the CORRECT way — full SubjectId encoding
        let sid = SubjectId::new(ns_code, local_id);
        let entry = ReverseEntry {
            key: subject_reverse_key(ns_code, suffix),
            id: sid.as_u64(),
        };

        let blob = encode_reverse_leaf(&[entry]);
        let leaf = ReverseLeaf::from_bytes(&blob).unwrap();

        let lookup_key = subject_reverse_key(ns_code, suffix);
        let found_id = leaf.lookup(&lookup_key).expect("entry should be found");

        // The stored ID must decode back to the original (ns_code, local_id)
        let decoded = SubjectId::from_u64(found_id);
        assert_eq!(decoded.ns_code(), ns_code);
        assert_eq!(decoded.local_id(), local_id);

        // Critically: the raw local_id alone is NOT the same as the full sid64
        assert_ne!(
            found_id, local_id,
            "must store full sid64, not raw local_id"
        );
        assert_eq!(found_id, sid.as_u64());
    }
}
