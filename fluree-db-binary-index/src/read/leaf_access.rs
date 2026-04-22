//! Unified leaf access abstraction for V3 (FLI3) index leaves.
//!
//! Provides a [`LeafHandle`] trait that abstracts over two access strategies:
//!
//! - **[`FullBlobLeafHandle`]**: holds the entire leaf blob in memory. Used for
//!   local filesystem access (OS page cache is optimal) and locally cached leaves.
//!
//! - **[`RangeReadLeafHandle`]**: holds only the decoded header + directory.
//!   Fetches individual column blocks on demand via byte-range reads. Used for
//!   remote object stores (S3/CAS) where downloading the entire leaf would be
//!   wasteful when the query only needs 1-2 columns.
//!
//! Both handles produce identical [`ColumnBatch`] output — the choice of handle
//! is invisible to the cursor and cache layers.

use std::io;
use std::ops::Range;
use std::sync::Arc;

use fluree_db_core::ContentId;

use crate::format::column_block::{ColumnBlockRef, ColumnId};
use crate::format::history_sidecar::{decode_history_segment, HistEntryV2, HistorySegmentRef};
use crate::format::leaf::{
    decode_leaf_dir_v3_with_base, decode_leaf_header_v3, DecodedLeafDirV3, LeafletDirEntryV3,
    LEAF_V3_HEADER_SIZE,
};
use crate::format::run_record::RunSortOrder;

use super::column_loader::load_leaflet_columns;
use super::column_types::{ColumnBatch, ColumnProjection};

// ============================================================================
// LeafHandle trait
// ============================================================================

/// Handle to an opened V3 leaf (header + directory decoded).
///
/// Implementations either hold the full leaf blob in memory (local fast path)
/// or hold only the header+directory and fetch column blocks on demand
/// (range-read path for remote storage).
pub trait LeafHandle: Send + Sync {
    /// Access the decoded leaflet directory.
    fn dir(&self) -> &DecodedLeafDirV3;

    /// Load columns for a specific leaflet.
    ///
    /// For full-blob handles, this slices from the in-memory buffer.
    /// For range-read handles, this fetches only the needed column blocks.
    fn load_columns(
        &self,
        leaflet_idx: usize,
        projection: &ColumnProjection,
        order: RunSortOrder,
    ) -> io::Result<ColumnBatch>;

    /// Load a history segment for a specific leaflet (for time-travel replay).
    ///
    /// Uses the directory entry's `history_offset` and `history_len` to read
    /// only the relevant segment. Returns an empty vec if no history exists.
    fn load_sidecar_segment(&self, leaflet_idx: usize) -> io::Result<Vec<HistEntryV2>>;

    /// Access raw sidecar bytes (full blob, for handles that pre-fetched it).
    /// Returns `None` if no sidecar is available.
    fn sidecar_bytes(&self) -> Option<&[u8]>;

    /// The leaf identity hash (xxh3_128 of leaf CID bytes) for cache keying.
    fn leaf_id(&self) -> u128;
}

// ============================================================================
// FullBlobLeafHandle
// ============================================================================

/// Leaf handle backed by the full leaf blob in memory.
///
/// This is the fast path for local filesystem access and for leaves
/// already cached locally. Column loading delegates to the existing
/// `load_leaflet_columns()` function.
pub struct FullBlobLeafHandle {
    bytes: Vec<u8>,
    dir: DecodedLeafDirV3,
    sidecar: Option<Vec<u8>>,
    leaf_id: u128,
}

impl FullBlobLeafHandle {
    /// Create from raw leaf bytes and optional sidecar bytes.
    ///
    /// Parses the header and directory from the leaf bytes.
    pub fn new(bytes: Vec<u8>, sidecar: Option<Vec<u8>>, leaf_id: u128) -> io::Result<Self> {
        let header = decode_leaf_header_v3(&bytes)?;
        let dir = decode_leaf_dir_v3_with_base(&bytes, &header)?;
        Ok(Self {
            bytes,
            dir,
            sidecar,
            leaf_id,
        })
    }
}

impl LeafHandle for FullBlobLeafHandle {
    fn dir(&self) -> &DecodedLeafDirV3 {
        &self.dir
    }

    fn load_columns(
        &self,
        leaflet_idx: usize,
        projection: &ColumnProjection,
        order: RunSortOrder,
    ) -> io::Result<ColumnBatch> {
        let entry = &self.dir.entries[leaflet_idx];
        load_leaflet_columns(&self.bytes, entry, self.dir.payload_base, projection, order)
    }

    fn load_sidecar_segment(&self, leaflet_idx: usize) -> io::Result<Vec<HistEntryV2>> {
        let entry = &self.dir.entries[leaflet_idx];
        if entry.history_len == 0 {
            return Ok(Vec::new());
        }
        let sc_bytes = self.sidecar.as_deref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "sidecar bytes required for history replay but not available",
            )
        })?;
        let seg = HistorySegmentRef {
            offset: entry.history_offset,
            len: entry.history_len,
            min_t: entry.history_min_t,
            max_t: entry.history_max_t,
        };
        decode_history_segment(sc_bytes, &seg)
    }

    fn sidecar_bytes(&self) -> Option<&[u8]> {
        self.sidecar.as_deref()
    }

    fn leaf_id(&self) -> u128 {
        self.leaf_id
    }
}

// ============================================================================
// RangeReadFetcher trait
// ============================================================================

/// Sync-safe interface for fetching byte ranges from a CAS object.
///
/// Bridges the async `ContentStore::get_range()` to the synchronous
/// cursor/column-loader world. Implementations use the same thread-spawn
/// + `Handle::block_on()` pattern proven in `get_leaf_bytes_sync()`.
pub trait RangeReadFetcher: Send + Sync {
    fn fetch_range(&self, id: &ContentId, range: Range<u64>) -> io::Result<Vec<u8>>;
}

// ============================================================================
// RangeReadLeafHandle
// ============================================================================

/// Leaf handle that fetches column blocks via byte-range reads.
///
/// Holds only the decoded header+directory (fetched as a single small range
/// read). Individual column blocks are fetched on demand from the remote CAS.
///
/// Used for remote storage (S3, etc.) where downloading the entire leaf blob
/// would be wasteful when only 1-2 columns are needed.
pub struct RangeReadLeafHandle {
    leaf_cid: ContentId,
    dir: DecodedLeafDirV3,
    /// Absolute byte offset of the payload section in the leaf blob.
    payload_base: u64,
    leaf_id: u128,
    fetcher: Arc<dyn RangeReadFetcher>,
    /// CID for the history sidecar, if one exists.
    sidecar_cid: Option<ContentId>,
}

/// Gap threshold for coalescing adjacent range reads (bytes).
/// If two column blocks are within this many bytes of each other,
/// they are fetched in a single request.
const COALESCE_GAP: u64 = 4096;

impl RangeReadLeafHandle {
    /// Create a range-read handle from a pre-fetched header+directory.
    ///
    /// The caller is responsible for having fetched enough of the leaf
    /// to decode the full header and directory (use
    /// [`fetch_header_and_directory`] for this).
    pub fn new(
        leaf_cid: ContentId,
        dir: DecodedLeafDirV3,
        payload_base: u64,
        leaf_id: u128,
        fetcher: Arc<dyn RangeReadFetcher>,
        sidecar_cid: Option<ContentId>,
    ) -> Self {
        Self {
            leaf_cid,
            dir,
            payload_base,
            leaf_id,
            fetcher,
            sidecar_cid,
        }
    }

    /// Compute the absolute byte range for a column block within the leaf blob.
    fn block_range(&self, entry: &LeafletDirEntryV3, block_ref: &ColumnBlockRef) -> Range<u64> {
        let start = self.payload_base + entry.payload_offset as u64 + block_ref.offset as u64;
        let end = start + block_ref.compressed_len as u64;
        start..end
    }

    /// Compute the byte ranges needed for the requested columns and coalesce
    /// adjacent ones. Returns a list of `(fetch_range, [(col_id, block_ref, local_offset)])`.
    fn plan_fetches(
        &self,
        entry: &LeafletDirEntryV3,
        requested_cols: &[(ColumnId, u8)], // (col_id, elem_width_tag)
    ) -> Vec<CoalescedFetch> {
        let mut blocks: Vec<(ColumnId, &ColumnBlockRef, Range<u64>)> = Vec::new();

        for &(col_id, _) in requested_cols {
            if let Some(block_ref) = entry.column_refs.iter().find(|r| r.col_id == col_id as u16) {
                let range = self.block_range(entry, block_ref);
                blocks.push((col_id, block_ref, range));
            }
        }

        if blocks.is_empty() {
            return Vec::new();
        }

        // Sort by range start for coalescing.
        blocks.sort_by_key(|(_, _, r)| r.start);

        let mut result: Vec<CoalescedFetch> = Vec::new();
        for (col_id, block_ref, range) in blocks {
            let merged = if let Some(last) = result.last_mut() {
                // Coalesce if the gap is within threshold.
                if range.start <= last.fetch_range.end + COALESCE_GAP {
                    last.fetch_range.end = last.fetch_range.end.max(range.end);
                    last.columns.push(CoalescedColumn {
                        col_id,
                        block_ref: *block_ref,
                        abs_start: range.start,
                    });
                    true
                } else {
                    false
                }
            } else {
                false
            };

            if !merged {
                result.push(CoalescedFetch {
                    fetch_range: range.clone(),
                    columns: vec![CoalescedColumn {
                        col_id,
                        block_ref: *block_ref,
                        abs_start: range.start,
                    }],
                });
            }
        }

        result
    }
}

struct CoalescedFetch {
    /// Byte range to fetch from the leaf blob.
    fetch_range: Range<u64>,
    /// Columns contained within this fetch.
    columns: Vec<CoalescedColumn>,
}

struct CoalescedColumn {
    col_id: ColumnId,
    block_ref: ColumnBlockRef,
    /// Absolute byte offset of this column block in the leaf blob.
    abs_start: u64,
}

use crate::format::leaflet::flags::{
    HAS_O_I as FLAG_HAS_O_I, HAS_O_TYPE_COL as FLAG_HAS_O_TYPE_COL,
};

impl LeafHandle for RangeReadLeafHandle {
    fn dir(&self) -> &DecodedLeafDirV3 {
        &self.dir
    }

    fn load_columns(
        &self,
        leaflet_idx: usize,
        projection: &ColumnProjection,
        _order: RunSortOrder,
    ) -> io::Result<ColumnBatch> {
        use super::column_types::ColumnData;
        use crate::format::column_block::{
            decode_column_u16, decode_column_u32, decode_column_u64,
        };

        let entry = &self.dir.entries[leaflet_idx];
        let row_count = entry.row_count as usize;
        let eff = projection.effective();

        // Determine which columns need fetching (not constant/absent).
        let mut needed_fetches: Vec<(ColumnId, u8)> = Vec::new();

        if eff.contains(ColumnId::SId) {
            needed_fetches.push((ColumnId::SId, 8));
        }
        if eff.contains(ColumnId::OKey) {
            needed_fetches.push((ColumnId::OKey, 8));
        }
        if eff.contains(ColumnId::PId) && entry.p_const.is_none() {
            needed_fetches.push((ColumnId::PId, 4));
        }
        if eff.contains(ColumnId::OType)
            && entry.o_type_const.is_none()
            && (entry.flags & FLAG_HAS_O_TYPE_COL != 0)
        {
            needed_fetches.push((ColumnId::OType, 2));
        }
        if eff.contains(ColumnId::OI) && (entry.flags & FLAG_HAS_O_I != 0) {
            needed_fetches.push((ColumnId::OI, 4));
        }
        if eff.contains(ColumnId::T) {
            needed_fetches.push((ColumnId::T, 4));
        }

        // Plan and execute coalesced fetches.
        let fetch_plan = self.plan_fetches(entry, &needed_fetches);

        // Collect raw fetched data.
        struct FetchedData {
            fetch_start: u64,
            bytes: Vec<u8>,
        }
        let mut fetched: Vec<FetchedData> = Vec::with_capacity(fetch_plan.len());

        for cf in &fetch_plan {
            let bytes = self
                .fetcher
                .fetch_range(&self.leaf_cid, cf.fetch_range.clone())?;
            fetched.push(FetchedData {
                fetch_start: cf.fetch_range.start,
                bytes,
            });
        }

        // Helper: find the fetched bytes for a column and decode.
        let find_and_decode_u64 = |col_id: ColumnId| -> io::Result<Vec<u64>> {
            for (fi, cf) in fetch_plan.iter().enumerate() {
                for cc in &cf.columns {
                    if cc.col_id == col_id {
                        let local_offset = (cc.abs_start - fetched[fi].fetch_start) as u32;
                        let local_ref = ColumnBlockRef {
                            offset: local_offset,
                            ..cc.block_ref
                        };
                        let decoded = decode_column_u64(&fetched[fi].bytes, &local_ref)?;
                        if decoded.len() != row_count {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!(
                                    "{:?}: decoded {} values but leaflet has {} rows",
                                    col_id,
                                    decoded.len(),
                                    row_count
                                ),
                            ));
                        }
                        return Ok(decoded);
                    }
                }
            }
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("missing column block for {col_id:?}"),
            ))
        };

        let find_and_decode_u32 = |col_id: ColumnId| -> io::Result<Vec<u32>> {
            for (fi, cf) in fetch_plan.iter().enumerate() {
                for cc in &cf.columns {
                    if cc.col_id == col_id {
                        let local_offset = (cc.abs_start - fetched[fi].fetch_start) as u32;
                        let local_ref = ColumnBlockRef {
                            offset: local_offset,
                            ..cc.block_ref
                        };
                        let decoded = decode_column_u32(&fetched[fi].bytes, &local_ref)?;
                        if decoded.len() != row_count {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!(
                                    "{:?}: decoded {} values but leaflet has {} rows",
                                    col_id,
                                    decoded.len(),
                                    row_count
                                ),
                            ));
                        }
                        return Ok(decoded);
                    }
                }
            }
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("missing column block for {col_id:?}"),
            ))
        };

        let find_and_decode_u16_col = |col_id: ColumnId| -> io::Result<Vec<u16>> {
            for (fi, cf) in fetch_plan.iter().enumerate() {
                for cc in &cf.columns {
                    if cc.col_id == col_id {
                        let local_offset = (cc.abs_start - fetched[fi].fetch_start) as u32;
                        let local_ref = ColumnBlockRef {
                            offset: local_offset,
                            ..cc.block_ref
                        };
                        let decoded = decode_column_u16(&fetched[fi].bytes, &local_ref)?;
                        if decoded.len() != row_count {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!(
                                    "{:?}: decoded {} values but leaflet has {} rows",
                                    col_id,
                                    decoded.len(),
                                    row_count
                                ),
                            ));
                        }
                        return Ok(decoded);
                    }
                }
            }
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("missing column block for {col_id:?}"),
            ))
        };

        // Build ColumnBatch using the same logic as column_loader.rs.
        let s_id = if eff.contains(ColumnId::SId) {
            ColumnData::Block(Arc::from(find_and_decode_u64(ColumnId::SId)?))
        } else {
            ColumnData::AbsentDefault
        };

        let o_key = if eff.contains(ColumnId::OKey) {
            ColumnData::Block(Arc::from(find_and_decode_u64(ColumnId::OKey)?))
        } else {
            ColumnData::AbsentDefault
        };

        let p_id = if eff.contains(ColumnId::PId) {
            if let Some(p) = entry.p_const {
                ColumnData::Const(p)
            } else {
                ColumnData::Block(Arc::from(find_and_decode_u32(ColumnId::PId)?))
            }
        } else {
            ColumnData::AbsentDefault
        };

        let o_type = if eff.contains(ColumnId::OType) {
            if let Some(ot) = entry.o_type_const {
                ColumnData::Const(ot)
            } else if entry.flags & FLAG_HAS_O_TYPE_COL != 0 {
                ColumnData::Block(Arc::from(find_and_decode_u16_col(ColumnId::OType)?))
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "leaflet has neither o_type_const nor o_type column",
                ));
            }
        } else {
            ColumnData::AbsentDefault
        };

        let o_i = if eff.contains(ColumnId::OI) {
            if entry.flags & FLAG_HAS_O_I != 0 {
                ColumnData::Block(Arc::from(find_and_decode_u32(ColumnId::OI)?))
            } else {
                ColumnData::Const(u32::MAX)
            }
        } else {
            ColumnData::AbsentDefault
        };

        let t = if eff.contains(ColumnId::T) {
            ColumnData::Block(Arc::from(find_and_decode_u32(ColumnId::T)?))
        } else {
            ColumnData::AbsentDefault
        };

        Ok(ColumnBatch {
            row_count,
            s_id,
            o_key,
            p_id,
            o_type,
            o_i,
            t,
        })
    }

    fn load_sidecar_segment(&self, leaflet_idx: usize) -> io::Result<Vec<HistEntryV2>> {
        use crate::format::history_sidecar::HIST_ENTRY_V2_SIZE;

        let entry = &self.dir.entries[leaflet_idx];
        if entry.history_len == 0 {
            return Ok(Vec::new());
        }
        let sidecar_cid = self.sidecar_cid.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "sidecar CID required for history replay but not available",
            )
        })?;
        // Range-read just the segment from the sidecar blob.
        // `history_offset` is already absolute within the sidecar blob
        // (includes the 8-byte FHS1 header).
        let offset = entry.history_offset;
        let end = offset + entry.history_len as u64;
        let bytes = self.fetcher.fetch_range(sidecar_cid, offset..end)?;

        // Segment format: 4-byte entry_count (u32 LE) followed by
        // entry_count × 31-byte HistEntryV2 records, sorted by t descending.
        if bytes.len() < 4 {
            return Ok(Vec::new());
        }
        let entry_count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let mut entries = Vec::with_capacity(entry_count);
        let mut pos = 4;
        for _ in 0..entry_count {
            if pos + HIST_ENTRY_V2_SIZE > bytes.len() {
                break;
            }
            entries.push(HistEntryV2::read_le(&bytes[pos..])?);

            pos += HIST_ENTRY_V2_SIZE;
        }
        Ok(entries)
    }

    fn sidecar_bytes(&self) -> Option<&[u8]> {
        // Range-read handle doesn't hold full sidecar bytes.
        None
    }

    fn leaf_id(&self) -> u128 {
        self.leaf_id
    }
}

// ============================================================================
// Helper: fetch header + directory from remote leaf
// ============================================================================

/// Fetch enough of a V3 leaf blob to decode the header and full directory.
///
/// Strategy:
/// 1. Fetch the first 72 bytes (header) to get `leaflet_count`.
/// 2. Estimate directory size as `leaflet_count * 120` (generous).
/// 3. Fetch `0..(72 + estimated_dir_size)` in one request.
/// 4. If the directory extends beyond the fetched bytes, fetch the remainder.
///
/// Returns the parsed directory, payload base offset, and leaf_id.
pub fn fetch_header_and_directory(
    fetcher: &dyn RangeReadFetcher,
    leaf_cid: &ContentId,
) -> io::Result<(DecodedLeafDirV3, u64)> {
    // Step 1: fetch header to learn leaflet_count.
    let header_bytes = fetcher.fetch_range(leaf_cid, 0..LEAF_V3_HEADER_SIZE as u64)?;
    let header = decode_leaf_header_v3(&header_bytes)?;

    // Step 2: estimate total header+directory size and fetch.
    let estimated_dir_size = header.leaflet_count as u64 * 120;
    let estimated_total = LEAF_V3_HEADER_SIZE as u64 + estimated_dir_size;
    let full_header_dir = fetcher.fetch_range(leaf_cid, 0..estimated_total)?;

    // Step 3: try to parse directory. If we fetched enough, this succeeds.
    // On failure, double the estimate up to 3 times (covers up to ~960 bytes
    // per leaflet, far beyond any realistic directory entry size).
    let mut buf = full_header_dir;
    let mut parsed: Option<DecodedLeafDirV3> = None;
    for _ in 0..3 {
        match decode_leaf_dir_v3_with_base(&buf, &header) {
            Ok(dir) => {
                parsed = Some(dir);
                break;
            }
            Err(_) => {
                // Directory was larger than estimated — double and retry.
                let next_size = (buf.len() as u64) * 2;
                buf = fetcher.fetch_range(leaf_cid, 0..next_size)?;
            }
        }
    }
    // Final attempt after tripling the budget.
    let dir = match parsed {
        Some(dir) => dir,
        None => decode_leaf_dir_v3_with_base(&buf, &header)?,
    };
    let payload_base = dir.payload_base as u64;
    Ok((dir, payload_base))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::leaf::LeafWriter;
    use crate::format::run_record::RunSortOrder;
    use crate::format::run_record_v2::RunRecordV2;
    use crate::read::column_types::{ColumnData, ColumnSet};
    use fluree_db_core::o_type::OType;
    use fluree_db_core::subject_id::SubjectId;
    use std::sync::Mutex;

    fn make_rec(s_id: u64, p_id: u32, o_type: u16, o_key: u64, t: u32) -> RunRecordV2 {
        RunRecordV2 {
            s_id: SubjectId(s_id),
            o_key,
            p_id,
            t,
            o_i: u32::MAX,
            o_type,
            g_id: 0,
        }
    }

    /// Build a test leaf blob with known data.
    fn build_test_leaf() -> (Vec<u8>, ContentId) {
        let mut writer = LeafWriter::new(RunSortOrder::Post, 100, 1000, 1);
        writer.set_skip_history(true);

        let ot = OType::XSD_INTEGER.as_u16();
        for i in 0..5u64 {
            writer
                .push_record(make_rec(i + 1, 1, ot, i * 10, 1))
                .unwrap();
        }
        let infos = writer.finish().unwrap();
        let leaf = &infos[0];
        (leaf.leaf_bytes.clone(), leaf.leaf_cid.clone())
    }

    /// Mock fetcher that serves from an in-memory blob and records requests.
    struct MockFetcher {
        data: std::collections::HashMap<String, Vec<u8>>,
        requests: Mutex<Vec<(String, Range<u64>)>>,
    }

    impl MockFetcher {
        fn new() -> Self {
            Self {
                data: std::collections::HashMap::new(),
                requests: Mutex::new(Vec::new()),
            }
        }

        fn insert(&mut self, cid: &ContentId, bytes: Vec<u8>) {
            self.data.insert(cid.to_string(), bytes);
        }

        fn request_count(&self) -> usize {
            self.requests.lock().unwrap().len()
        }
    }

    impl RangeReadFetcher for MockFetcher {
        fn fetch_range(&self, id: &ContentId, range: Range<u64>) -> io::Result<Vec<u8>> {
            self.requests
                .lock()
                .unwrap()
                .push((id.to_string(), range.clone()));

            let full = self
                .data
                .get(&id.to_string())
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "not found"))?;
            let start = range.start as usize;
            let end = (range.end as usize).min(full.len());
            if start >= full.len() {
                return Ok(Vec::new());
            }
            Ok(full[start..end].to_vec())
        }
    }

    #[test]
    fn full_blob_handle_matches_column_loader() {
        let (leaf_bytes, leaf_cid) = build_test_leaf();
        let leaf_id = xxhash_rust::xxh3::xxh3_128(leaf_cid.to_bytes().as_ref());

        let handle = FullBlobLeafHandle::new(leaf_bytes, None, leaf_id).unwrap();
        assert_eq!(handle.dir().entries.len(), 1);

        let proj = ColumnProjection::all();
        let batch = handle.load_columns(0, &proj, RunSortOrder::Post).unwrap();

        assert_eq!(batch.row_count, 5);
        assert!(batch.p_id.is_const());
        assert_eq!(batch.p_id.get(0), 1);
        assert!(matches!(batch.s_id, ColumnData::Block(_)));
        for i in 0..5 {
            assert_eq!(batch.s_id.get(i), (i + 1) as u64);
            assert_eq!(batch.o_key.get(i), i as u64 * 10);
        }
    }

    #[test]
    fn range_read_handle_produces_same_output() {
        let (leaf_bytes, leaf_cid) = build_test_leaf();
        let leaf_id = xxhash_rust::xxh3::xxh3_128(leaf_cid.to_bytes().as_ref());

        // Build mock fetcher with the leaf blob.
        let mut fetcher = MockFetcher::new();
        fetcher.insert(&leaf_cid, leaf_bytes.clone());
        let fetcher = Arc::new(fetcher);

        // Fetch header + directory.
        let (dir, payload_base) = fetch_header_and_directory(fetcher.as_ref(), &leaf_cid).unwrap();

        let handle = RangeReadLeafHandle::new(
            leaf_cid,
            dir,
            payload_base,
            leaf_id,
            Arc::clone(&fetcher) as Arc<dyn RangeReadFetcher>,
            None,
        );

        let proj = ColumnProjection::all();
        let batch = handle.load_columns(0, &proj, RunSortOrder::Post).unwrap();

        assert_eq!(batch.row_count, 5);
        assert!(batch.p_id.is_const());
        assert_eq!(batch.p_id.get(0), 1);
        for i in 0..5 {
            assert_eq!(batch.s_id.get(i), (i + 1) as u64);
            assert_eq!(batch.o_key.get(i), i as u64 * 10);
        }
    }

    #[test]
    fn range_read_selective_projection() {
        let (leaf_bytes, leaf_cid) = build_test_leaf();
        let leaf_id = xxhash_rust::xxh3::xxh3_128(leaf_cid.to_bytes().as_ref());

        let mut fetcher = MockFetcher::new();
        fetcher.insert(&leaf_cid, leaf_bytes);
        let fetcher = Arc::new(fetcher);

        let (dir, payload_base) = fetch_header_and_directory(fetcher.as_ref(), &leaf_cid).unwrap();

        // Clear request log after directory fetch.
        fetcher.requests.lock().unwrap().clear();

        let handle = RangeReadLeafHandle::new(
            leaf_cid,
            dir,
            payload_base,
            leaf_id,
            Arc::clone(&fetcher) as Arc<dyn RangeReadFetcher>,
            None,
        );

        // Request only s_id and o_key.
        let proj = ColumnProjection {
            output: ColumnSet::single(ColumnId::SId).union(ColumnSet::single(ColumnId::OKey)),
            internal: ColumnSet::EMPTY,
        };

        let batch = handle.load_columns(0, &proj, RunSortOrder::Post).unwrap();

        assert_eq!(batch.row_count, 5);
        assert!(matches!(batch.s_id, ColumnData::Block(_)));
        assert!(matches!(batch.o_key, ColumnData::Block(_)));
        assert!(batch.p_id.is_absent() || batch.p_id.is_const()); // p_const → Absent since not projected
        assert!(batch.t.is_absent());

        // Should have made range-read requests (coalesced into potentially 1).
        let reqs = fetcher.request_count();
        assert!(
            reqs >= 1,
            "expected at least 1 range-read request, got {reqs}"
        );
    }

    #[test]
    fn range_read_sidecar_segment() {
        use crate::format::history_sidecar::HistSidecarBuilder;

        // Build a sidecar with known history entries for leaflet 0.
        let hist_entries = vec![
            HistEntryV2 {
                s_id: SubjectId(1),
                p_id: 1,
                o_type: OType::XSD_INTEGER.as_u16(),
                o_key: 100,
                o_i: u32::MAX,
                t: 5,
                op: 1, // assert
            },
            HistEntryV2 {
                s_id: SubjectId(1),
                p_id: 1,
                o_type: OType::XSD_INTEGER.as_u16(),
                o_key: 50,
                o_i: u32::MAX,
                t: 3,
                op: 0, // retract
            },
        ];
        let mut builder = HistSidecarBuilder::new();
        builder.start_leaflet();
        for entry in &hist_entries {
            builder.push_entry(*entry);
        }
        let (sidecar_bytes, seg_refs) = builder.build();
        let sidecar_cid = fluree_db_core::ContentId::new(
            fluree_db_core::content_kind::ContentKind::Commit,
            b"test-sidecar-cid",
        );

        // Build a leaf and mock the history_offset/len from sidecar segment refs.
        let (leaf_bytes, leaf_cid) = build_test_leaf();
        let leaf_id = xxhash_rust::xxh3::xxh3_128(leaf_cid.to_bytes().as_ref());

        let mut fetcher = MockFetcher::new();
        fetcher.insert(&leaf_cid, leaf_bytes);
        fetcher.insert(&sidecar_cid, sidecar_bytes);
        let fetcher = Arc::new(fetcher);

        let (mut dir, payload_base) =
            fetch_header_and_directory(fetcher.as_ref(), &leaf_cid).unwrap();

        // Patch directory entry with sidecar segment refs from the builder.
        let seg = &seg_refs[0];
        dir.entries[0].history_offset = seg.offset;
        dir.entries[0].history_len = seg.len;
        dir.entries[0].history_min_t = seg.min_t;
        dir.entries[0].history_max_t = seg.max_t;

        let handle = RangeReadLeafHandle::new(
            leaf_cid,
            dir,
            payload_base,
            leaf_id,
            Arc::clone(&fetcher) as Arc<dyn RangeReadFetcher>,
            Some(sidecar_cid.clone()),
        );

        let entries = handle.load_sidecar_segment(0).unwrap();
        assert_eq!(entries.len(), 2);
        // Entries should be sorted by t descending (newest first).
        assert!(entries[0].t >= entries[1].t);
        assert_eq!(entries[0].s_id, SubjectId(1));
        assert_eq!(entries[0].o_key, 100);
        assert_eq!(entries[1].o_key, 50);

        // Verify the fetcher received a range request for the sidecar.
        let reqs = fetcher.requests.lock().unwrap();
        let sidecar_key = sidecar_cid.to_string();
        let sidecar_reqs: Vec<_> = reqs.iter().filter(|(id, _)| *id == sidecar_key).collect();
        assert_eq!(
            sidecar_reqs.len(),
            1,
            "expected exactly 1 sidecar range request"
        );
        // The range should be just the segment, not the full sidecar.
        let (_, range) = &sidecar_reqs[0];
        assert_eq!(range.start, seg.offset);
        assert_eq!(range.end, seg.offset + seg.len as u64);
    }

    #[test]
    fn full_blob_handle_sidecar_segment_matches_range_read() {
        use crate::format::history_sidecar::HistSidecarBuilder;

        // Build sidecar with entries.
        let hist_entries = vec![HistEntryV2 {
            s_id: SubjectId(2),
            p_id: 3,
            o_type: OType::XSD_STRING.as_u16(),
            o_key: 200,
            o_i: u32::MAX,
            t: 10,
            op: 1, // assert
        }];
        let mut builder = HistSidecarBuilder::new();
        builder.start_leaflet();
        for entry in &hist_entries {
            builder.push_entry(*entry);
        }
        let (sidecar_bytes, seg_refs) = builder.build();
        let sidecar_cid = fluree_db_core::ContentId::new(
            fluree_db_core::content_kind::ContentKind::Commit,
            b"test-sidecar-2",
        );

        let (leaf_bytes, leaf_cid) = build_test_leaf();
        let leaf_id = xxhash_rust::xxh3::xxh3_128(leaf_cid.to_bytes().as_ref());

        // FullBlobLeafHandle path.
        let mut full_handle =
            FullBlobLeafHandle::new(leaf_bytes.clone(), Some(sidecar_bytes.clone()), leaf_id)
                .unwrap();
        // Patch history fields.
        let seg = &seg_refs[0];
        full_handle.dir.entries[0].history_offset = seg.offset;
        full_handle.dir.entries[0].history_len = seg.len;
        full_handle.dir.entries[0].history_min_t = seg.min_t;
        full_handle.dir.entries[0].history_max_t = seg.max_t;
        let full_entries = full_handle.load_sidecar_segment(0).unwrap();

        // RangeReadLeafHandle path.
        let mut fetcher = MockFetcher::new();
        fetcher.insert(&leaf_cid, leaf_bytes);
        fetcher.insert(&sidecar_cid, sidecar_bytes);
        let fetcher = Arc::new(fetcher);

        let (mut dir, payload_base) =
            fetch_header_and_directory(fetcher.as_ref(), &leaf_cid).unwrap();
        dir.entries[0].history_offset = seg.offset;
        dir.entries[0].history_len = seg.len;
        dir.entries[0].history_min_t = seg.min_t;
        dir.entries[0].history_max_t = seg.max_t;

        let range_handle = RangeReadLeafHandle::new(
            leaf_cid,
            dir,
            payload_base,
            leaf_id,
            Arc::clone(&fetcher) as Arc<dyn RangeReadFetcher>,
            Some(sidecar_cid),
        );
        let range_entries = range_handle.load_sidecar_segment(0).unwrap();

        // Both paths should produce identical entries.
        assert_eq!(full_entries.len(), range_entries.len());
        for (f, r) in full_entries.iter().zip(range_entries.iter()) {
            assert_eq!(f.s_id, r.s_id);
            assert_eq!(f.p_id, r.p_id);
            assert_eq!(f.o_type, r.o_type);
            assert_eq!(f.o_key, r.o_key);
            assert_eq!(f.o_i, r.o_i);
            assert_eq!(f.t, r.t);
            assert_eq!(f.op, r.op);
        }
    }
}
