#![allow(clippy::type_complexity)]
//! Sorted cell index storage.
//!
//! The cell index maps S2 cell IDs to subject entries, enabling efficient
//! range scans for spatial queries. Entries are sorted by:
//! `(cell_id, subject_id, t DESC)`
//!
//! This sort order allows:
//! - Range scans for S2 covering intervals
//! - Efficient time-travel replay (newest first within each key)
//! - Global dedup across cells (by subject_id)
//!
//! # Format
//!
//! The index is chunked into leaflets (for CAS) with a branch manifest
//! for routing by cell_id range.
//!
//! ```text
//! FSC1 (Fluree Spatial Cell Index v2)
//!
//! Leaflet Header (44 bytes):
//!   magic: "FSC1" (4B)
//!   version: u8
//!   flags: u8
//!   _reserved: u16
//!   entry_count: u32 (LE)
//!   compressed_len: u32 (LE)
//!   uncompressed_len: u32 (LE)
//!   first_cell_id: u64 (LE)
//!   last_cell_id: u64 (LE)
//!   crc32_compressed: u32 (LE)    -- CRC32 of compressed body (quick integrity check)
//!   crc32_uncompressed: u32 (LE)  -- CRC32 of uncompressed data (post-decompression validation)
//!
//! Body (zstd compressed):
//!   entries: [CellEntry; entry_count]
//! ```

use crate::error::{Result, SpatialError};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// Magic bytes for cell index leaflets.
pub const LEAFLET_MAGIC: &[u8; 4] = b"FSC1";

/// Current leaflet format version.
pub const LEAFLET_VERSION: u8 = 2;

/// Header length for v2 (with checksums).
const LEAFLET_HEADER_LEN: usize = 44;

/// A single entry in the cell index.
///
/// Maps an S2 cell to a subject at a specific transaction time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(C)]
pub struct CellEntry {
    /// S2 cell ID (Hilbert-curve ordered).
    pub cell_id: u64,

    /// Subject ID (reuses Fluree's subject ID scheme).
    pub subject_id: u64,

    /// Handle into the geometry arena.
    pub geo_handle: u32,

    /// Transaction time.
    pub t: i64,

    /// Operation: 1 = assert, 0 = retract.
    pub op: u8,

    /// Reserved for future use.
    pub _reserved: [u8; 3],
}

impl CellEntry {
    /// Size of a serialized entry in bytes.
    pub const SIZE: usize = 32;

    /// Create a new cell entry.
    pub fn new(cell_id: u64, subject_id: u64, geo_handle: u32, t: i64, op: u8) -> Self {
        Self {
            cell_id,
            subject_id,
            geo_handle,
            t,
            op,
            _reserved: [0; 3],
        }
    }

    /// Check if this is an assert operation.
    pub fn is_assert(&self) -> bool {
        self.op == 1
    }

    /// Check if this is a retract operation.
    pub fn is_retract(&self) -> bool {
        self.op == 0
    }

    /// Compare for index ordering: (cell_id, subject_id, t DESC, op ASC).
    ///
    /// The `op` tie-break ensures that for same `(cell_id, subject_id, t)`:
    /// - retract (op=0) sorts before assert (op=1)
    /// - this gives "retract wins" semantics during replay
    ///
    /// Note: Same-t assert+retract pairs shouldn't occur in normal commits
    /// (each commit produces one operation per subject/predicate), but this
    /// ordering provides defined behavior if they do.
    pub fn cmp_index(&self, other: &Self) -> Ordering {
        match self.cell_id.cmp(&other.cell_id) {
            Ordering::Equal => match self.subject_id.cmp(&other.subject_id) {
                Ordering::Equal => match other.t.cmp(&self.t) {
                    // t DESC
                    Ordering::Equal => self.op.cmp(&other.op), // op ASC (0=retract before 1=assert)
                    ord => ord,
                },
                ord => ord,
            },
            ord => ord,
        }
    }

    /// Serialize to bytes (little-endian).
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..8].copy_from_slice(&self.cell_id.to_le_bytes());
        buf[8..16].copy_from_slice(&self.subject_id.to_le_bytes());
        buf[16..20].copy_from_slice(&self.geo_handle.to_le_bytes());
        buf[20..28].copy_from_slice(&self.t.to_le_bytes());
        buf[28] = self.op;
        buf[29..32].copy_from_slice(&self._reserved);
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8; Self::SIZE]) -> Self {
        Self {
            cell_id: u64::from_le_bytes(data[0..8].try_into().unwrap()),
            subject_id: u64::from_le_bytes(data[8..16].try_into().unwrap()),
            geo_handle: u32::from_le_bytes(data[16..20].try_into().unwrap()),
            t: i64::from_le_bytes(data[20..28].try_into().unwrap()),
            op: data[28],
            _reserved: data[29..32].try_into().unwrap(),
        }
    }
}

/// Wrapper for CellEntry that implements Ord using cmp_index().
///
/// Used for k-way merge in the heap.
#[derive(Clone, Copy)]
struct CellEntryOrd(CellEntry);

impl PartialEq for CellEntryOrd {
    fn eq(&self, other: &Self) -> bool {
        self.0.cmp_index(&other.0) == Ordering::Equal
    }
}

impl Eq for CellEntryOrd {}

impl PartialOrd for CellEntryOrd {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CellEntryOrd {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp_index(&other.0)
    }
}

/// Metadata for a cell index leaflet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeafletMeta {
    /// Content hash of the compressed leaflet (for CAS addressing).
    pub content_hash: String,

    /// Number of entries in this leaflet.
    pub entry_count: u32,

    /// First cell_id in this leaflet.
    pub first_cell_id: u64,

    /// Last cell_id in this leaflet.
    pub last_cell_id: u64,

    /// Compressed size in bytes.
    pub compressed_bytes: u32,
}

/// Branch manifest for routing to leaflets by cell_id range.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellIndexManifest {
    /// Total entry count across all leaflets.
    pub total_entries: u64,

    /// Leaflet metadata in cell_id order.
    pub leaflets: Vec<LeafletMeta>,
}

impl CellIndexManifest {
    /// Find leaflets that may contain entries for the given cell_id range.
    pub fn leaflets_for_range(&self, min_cell: u64, max_cell: u64) -> Vec<&LeafletMeta> {
        self.leaflets
            .iter()
            .filter(|l| l.last_cell_id >= min_cell && l.first_cell_id <= max_cell)
            .collect()
    }
}

/// Builder for cell index leaflets.
pub struct CellIndexBuilder {
    /// Accumulated entries (sorted).
    entries: Vec<CellEntry>,

    /// Target chunk size in bytes.
    chunk_target_bytes: usize,
}

impl CellIndexBuilder {
    /// Create a new builder with the given target chunk size.
    pub fn new(chunk_target_bytes: usize) -> Self {
        Self {
            entries: Vec::new(),
            chunk_target_bytes,
        }
    }

    /// Add an entry. Entries can be added in any order; they will be sorted
    /// before building leaflets.
    pub fn push(&mut self, entry: CellEntry) {
        self.entries.push(entry);
    }

    /// Add multiple entries.
    pub fn extend(&mut self, entries: impl IntoIterator<Item = CellEntry>) {
        self.entries.extend(entries);
    }

    /// Number of entries added so far.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Build the cell index, returning leaflet bytes and manifest.
    ///
    /// The `write_chunk` callback is called for each leaflet with its
    /// compressed bytes; it should persist to CAS and return the content hash.
    pub fn build<F>(mut self, mut write_chunk: F) -> Result<CellIndexManifest>
    where
        F: FnMut(&[u8]) -> Result<String>,
    {
        // Sort entries by index order
        self.entries.sort_by(CellEntry::cmp_index);

        let mut leaflets = Vec::new();
        let entries_per_chunk = self.chunk_target_bytes / CellEntry::SIZE;
        let entries_per_chunk = entries_per_chunk.max(1);

        for chunk in self.entries.chunks(entries_per_chunk) {
            let leaflet_bytes = Self::build_leaflet(chunk)?;
            let content_hash = write_chunk(&leaflet_bytes)?;

            leaflets.push(LeafletMeta {
                content_hash,
                entry_count: chunk.len() as u32,
                first_cell_id: chunk.first().unwrap().cell_id,
                last_cell_id: chunk.last().unwrap().cell_id,
                compressed_bytes: leaflet_bytes.len() as u32,
            });
        }

        Ok(CellIndexManifest {
            total_entries: self.entries.len() as u64,
            leaflets,
        })
    }

    /// Build a single leaflet from sorted entries.
    fn build_leaflet(entries: &[CellEntry]) -> Result<Vec<u8>> {
        // Serialize entries
        let mut uncompressed = Vec::with_capacity(entries.len() * CellEntry::SIZE);
        for entry in entries {
            uncompressed.extend_from_slice(&entry.to_bytes());
        }

        // Compute CRC32 of uncompressed data (for post-decompression validation)
        let crc32_uncompressed = crc32fast::hash(&uncompressed);

        // Compress with zstd
        let compressed = zstd::encode_all(&uncompressed[..], 3)
            .map_err(|e| SpatialError::Io(std::io::Error::other(e)))?;

        // Compute CRC32 of compressed body (for quick integrity check)
        let crc32_compressed = crc32fast::hash(&compressed);

        // Build header + body
        let mut buf = Vec::with_capacity(LEAFLET_HEADER_LEN + compressed.len());

        // Header (44 bytes)
        buf.extend_from_slice(LEAFLET_MAGIC); // 0-4
        buf.push(LEAFLET_VERSION); // 4
        buf.push(0); // flags                                                    // 5
        buf.extend_from_slice(&[0u8; 2]); // reserved                            // 6-8
        buf.extend_from_slice(&(entries.len() as u32).to_le_bytes()); // 8-12
        buf.extend_from_slice(&(compressed.len() as u32).to_le_bytes()); // 12-16
        buf.extend_from_slice(&(uncompressed.len() as u32).to_le_bytes()); // 16-20
        buf.extend_from_slice(&entries.first().unwrap().cell_id.to_le_bytes()); // 20-28
        buf.extend_from_slice(&entries.last().unwrap().cell_id.to_le_bytes()); // 28-36
        buf.extend_from_slice(&crc32_compressed.to_le_bytes()); // 36-40
        buf.extend_from_slice(&crc32_uncompressed.to_le_bytes()); // 40-44

        // Body
        buf.extend_from_slice(&compressed);

        Ok(buf)
    }
}

/// Reader for cell index leaflets.
pub struct CellIndexReader {
    /// The manifest for this index.
    manifest: CellIndexManifest,

    /// Callback to fetch chunk bytes by content hash.
    fetch_chunk: Box<dyn Fn(&str) -> Result<Vec<u8>> + Send + Sync>,
}

impl CellIndexReader {
    /// Create a new reader with the given manifest and chunk fetcher.
    pub fn new<F>(manifest: CellIndexManifest, fetch_chunk: F) -> Self
    where
        F: Fn(&str) -> Result<Vec<u8>> + Send + Sync + 'static,
    {
        Self {
            manifest,
            fetch_chunk: Box::new(fetch_chunk),
        }
    }

    /// Get the manifest.
    pub fn manifest(&self) -> &CellIndexManifest {
        &self.manifest
    }

    /// Scan entries in the given cell_id range.
    ///
    /// Returns entries in index order. Each leaflet is sorted internally;
    /// we use a k-way merge to produce globally sorted output without
    /// materializing all entries at once.
    ///
    /// Note: For very large scans across many leaflets, a streaming iterator
    /// would be more memory-efficient. This version still loads all matching
    /// leaflets but avoids a final sort by using merge.
    pub fn scan_range(&self, min_cell: u64, max_cell: u64) -> Result<Vec<CellEntry>> {
        let leaflets = self.manifest.leaflets_for_range(min_cell, max_cell);

        if leaflets.is_empty() {
            return Ok(Vec::new());
        }

        // Load all matching leaflets and filter to range
        let mut leaflet_entries: Vec<Vec<CellEntry>> = Vec::with_capacity(leaflets.len());

        for leaflet_meta in leaflets {
            let entries = self.read_leaflet(leaflet_meta)?;

            // Filter to requested range (each leaflet is already sorted)
            let filtered: Vec<_> = entries
                .into_iter()
                .filter(|e| e.cell_id >= min_cell && e.cell_id <= max_cell)
                .collect();

            if !filtered.is_empty() {
                leaflet_entries.push(filtered);
            }
        }

        // K-way merge using a min-heap
        // For a small number of leaflets, this is efficient
        self.kway_merge(leaflet_entries)
    }

    /// K-way merge of sorted entry lists.
    fn kway_merge(&self, mut lists: Vec<Vec<CellEntry>>) -> Result<Vec<CellEntry>> {
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;

        if lists.is_empty() {
            return Ok(Vec::new());
        }

        if lists.len() == 1 {
            return Ok(lists.pop().unwrap());
        }

        // Total capacity hint
        let total: usize = lists.iter().map(std::vec::Vec::len).sum();
        let mut result = Vec::with_capacity(total);

        // Heap of (entry, list_index, position_in_list)
        // We use Reverse because BinaryHeap is a max-heap
        let mut heap: BinaryHeap<Reverse<(CellEntryOrd, usize, usize)>> = BinaryHeap::new();

        // Initialize heap with first element from each list
        for (i, list) in lists.iter().enumerate() {
            if !list.is_empty() {
                heap.push(Reverse((CellEntryOrd(list[0]), i, 0)));
            }
        }

        while let Some(Reverse((CellEntryOrd(entry), list_idx, pos))) = heap.pop() {
            result.push(entry);

            // Push next element from same list if available
            let next_pos = pos + 1;
            if next_pos < lists[list_idx].len() {
                heap.push(Reverse((
                    CellEntryOrd(lists[list_idx][next_pos]),
                    list_idx,
                    next_pos,
                )));
            }
        }

        Ok(result)
    }

    /// Read and decode a single leaflet (v2 format with checksums).
    fn read_leaflet(&self, meta: &LeafletMeta) -> Result<Vec<CellEntry>> {
        let data = (self.fetch_chunk)(&meta.content_hash)?;

        if data.len() < LEAFLET_HEADER_LEN {
            return Err(SpatialError::FormatError("leaflet too short".into()));
        }

        // Verify magic
        if &data[0..4] != LEAFLET_MAGIC {
            return Err(SpatialError::FormatError("invalid leaflet magic".into()));
        }

        let version = data[4];
        if version != LEAFLET_VERSION {
            return Err(SpatialError::FormatError(format!(
                "unsupported leaflet version: {version} (only v{LEAFLET_VERSION} supported)"
            )));
        }

        let entry_count = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
        let compressed_len = u32::from_le_bytes(data[12..16].try_into().unwrap()) as usize;

        if data.len() < LEAFLET_HEADER_LEN + compressed_len {
            return Err(SpatialError::FormatError("truncated leaflet body".into()));
        }

        // Extract checksums
        let expected_crc_compressed = u32::from_le_bytes(data[36..40].try_into().unwrap());
        let expected_crc_uncompressed = u32::from_le_bytes(data[40..44].try_into().unwrap());

        // Get compressed data
        let compressed = &data[LEAFLET_HEADER_LEN..LEAFLET_HEADER_LEN + compressed_len];

        // Verify compressed checksum (quick integrity check before decompression)
        let actual_crc_c = crc32fast::hash(compressed);
        if actual_crc_c != expected_crc_compressed {
            return Err(SpatialError::FormatError(format!(
                "compressed CRC32 mismatch: expected {expected_crc_compressed:08x}, got {actual_crc_c:08x}"
            )));
        }

        // Decompress
        let decompressed =
            zstd::decode_all(compressed).map_err(|e| SpatialError::Io(std::io::Error::other(e)))?;

        // Verify uncompressed checksum (post-decompression validation)
        let actual_crc_u = crc32fast::hash(&decompressed);
        if actual_crc_u != expected_crc_uncompressed {
            return Err(SpatialError::FormatError(format!(
                "uncompressed CRC32 mismatch: expected {expected_crc_uncompressed:08x}, got {actual_crc_u:08x}"
            )));
        }

        // Parse entries
        if decompressed.len() != entry_count * CellEntry::SIZE {
            return Err(SpatialError::FormatError("entry count mismatch".into()));
        }

        let mut entries = Vec::with_capacity(entry_count);
        for chunk in decompressed.chunks_exact(CellEntry::SIZE) {
            entries.push(CellEntry::from_bytes(chunk.try_into().unwrap()));
        }

        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cell_entry_roundtrip() {
        let entry = CellEntry::new(12345, 67890, 42, 100, 1);
        let bytes = entry.to_bytes();
        let recovered = CellEntry::from_bytes(&bytes);
        assert_eq!(entry, recovered);
    }

    #[test]
    fn test_cell_entry_ordering() {
        let e1 = CellEntry::new(100, 1, 0, 10, 1);
        let e2 = CellEntry::new(100, 1, 0, 20, 1); // same key, higher t
        let e3 = CellEntry::new(100, 2, 0, 10, 1); // different subject

        // e2 should come before e1 (t DESC)
        assert_eq!(e1.cmp_index(&e2), Ordering::Greater);
        assert_eq!(e2.cmp_index(&e1), Ordering::Less);

        // e3 should come after e1 (subject_id ASC)
        assert_eq!(e1.cmp_index(&e3), Ordering::Less);
    }

    #[test]
    fn test_builder_and_reader() {
        let mut builder = CellIndexBuilder::new(1024);

        // Add entries out of order
        builder.push(CellEntry::new(200, 1, 0, 10, 1));
        builder.push(CellEntry::new(100, 1, 0, 10, 1));
        builder.push(CellEntry::new(100, 2, 0, 10, 1));
        builder.push(CellEntry::new(150, 1, 0, 10, 1));

        // Build with in-memory storage
        let mut chunks: std::collections::HashMap<String, Vec<u8>> =
            std::collections::HashMap::new();
        let mut chunk_id = 0;

        let manifest = builder
            .build(|data| {
                let hash = format!("chunk-{chunk_id}");
                chunk_id += 1;
                chunks.insert(hash.clone(), data.to_vec());
                Ok(hash)
            })
            .unwrap();

        assert_eq!(manifest.total_entries, 4);

        // Read back
        let reader = CellIndexReader::new(manifest, move |hash| {
            chunks
                .get(hash)
                .cloned()
                .ok_or_else(|| SpatialError::ChunkNotFound(hash.into()))
        });

        let entries = reader.scan_range(100, 200).unwrap();
        assert_eq!(entries.len(), 4);

        // Verify sorted order
        assert_eq!(entries[0].cell_id, 100);
        assert_eq!(entries[0].subject_id, 1);
        assert_eq!(entries[1].cell_id, 100);
        assert_eq!(entries[1].subject_id, 2);
        assert_eq!(entries[2].cell_id, 150);
        assert_eq!(entries[3].cell_id, 200);
    }
}
