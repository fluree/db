//! Unified leaf access abstraction for V3 (FLI3) index leaves.
//!
//! Provides a [`LeafHandle`] trait that abstracts over two access strategies:
//!
//! - **[`FullBlobLeafHandle`]**: holds the whole leaf blob — mapped, shared
//!   from the store, or read. Used for every leaf the store has locally, and
//!   for remote leaves promoted to a whole-blob fetch.
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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use fluree_db_core::{ContentBytes, ContentId};

use crate::format::column_block::{ColumnBlockRef, ColumnId, COLUMN_BLOCK_REF_SIZE};
use crate::format::history_sidecar::{decode_history_segment, HistEntryV2, HistorySegmentRef};
use crate::format::leaf::{
    decode_leaf_dir_v3_with_base, decode_leaf_header_v3, leaf_header_size, leaflet_dir_fixed_size,
    DecodedLeafDirV3, LeafletDirEntryV3, LEAFLET_DIR_HISTORY_SIZE,
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

/// Leaf handle over the whole leaf blob.
///
/// The bytes are whatever the store handed out: a mapping of a local file
/// (only the pages a scan touches fault in), bytes a store holds in memory
/// (shared, not copied), or a buffer read or fetched for this leaf. Column
/// loading delegates to `load_leaflet_columns()`.
pub struct FullBlobLeafHandle {
    bytes: ContentBytes,
    /// Shared from the [`LeafletCache`](super::leaflet_cache::LeafletCache)
    /// when the opener has one, so a leaf's directory is decoded once per
    /// leaf CID rather than per open.
    dir: Arc<DecodedLeafDirV3>,
    sidecar: Option<ContentBytes>,
    leaf_id: u128,
}

impl FullBlobLeafHandle {
    /// Create from leaf bytes and optional sidecar bytes, decoding the
    /// header and directory from the leaf bytes.
    pub fn new(
        bytes: impl Into<ContentBytes>,
        sidecar: Option<ContentBytes>,
        leaf_id: u128,
    ) -> io::Result<Self> {
        let bytes = bytes.into();
        let header = decode_leaf_header_v3(&bytes)?;
        let dir = Arc::new(decode_leaf_dir_v3_with_base(&bytes, &header)?);
        Ok(Self::with_dir(bytes, dir, sidecar, leaf_id))
    }

    /// Create with a directory already decoded.
    pub fn with_dir(
        bytes: ContentBytes,
        dir: Arc<DecodedLeafDirV3>,
        sidecar: Option<ContentBytes>,
        leaf_id: u128,
    ) -> Self {
        Self {
            bytes,
            dir,
            sidecar,
            leaf_id,
        }
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
    dir: Arc<DecodedLeafDirV3>,
    /// Absolute byte offset of the payload section in the leaf blob.
    payload_base: u64,
    leaf_id: u128,
    fetcher: Arc<dyn RangeReadFetcher>,
    /// CID for the history sidecar, if one exists.
    sidecar_cid: Option<ContentId>,
    /// Leaflet loads served by range reads so far.
    range_loads: AtomicUsize,
    /// Whether a scan promotes to one whole-leaf read: worth it against a
    /// remote store's per-request latency, not against a local source.
    promote_scans: bool,
    /// Requests closer than this are merged into one read.
    coalesce_gap: u64,
    /// The whole leaf, once this handle has seen a scan.
    whole: OnceLock<Vec<u8>>,
}

/// Range-read leaflet loads one handle serves before it fetches the rest of
/// the leaf in one request. A point read touches one or two leaflets; past
/// that a remote store's per-request latency outweighs the extra bytes.
const SCAN_WHOLE_LEAF_AFTER: usize = 3;

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
        dir: impl Into<Arc<DecodedLeafDirV3>>,
        payload_base: u64,
        leaf_id: u128,
        fetcher: Arc<dyn RangeReadFetcher>,
        sidecar_cid: Option<ContentId>,
    ) -> Self {
        Self {
            leaf_cid,
            dir: dir.into(),
            payload_base,
            leaf_id,
            fetcher,
            sidecar_cid,
            range_loads: AtomicUsize::new(0),
            promote_scans: true,
            coalesce_gap: COALESCE_GAP,
            whole: OnceLock::new(),
        }
    }

    /// A handle over a local source whose reads cost in units of
    /// `read_unit` bytes (decrypted segments). Scans never promote to a
    /// whole-leaf read, so a leaf is never held whole, and requests within
    /// one unit of each other merge: the unit is paid for either way.
    pub fn over_local_source(
        leaf_cid: ContentId,
        dir: Arc<DecodedLeafDirV3>,
        leaf_id: u128,
        fetcher: Arc<dyn RangeReadFetcher>,
        sidecar_cid: Option<ContentId>,
        read_unit: u64,
    ) -> Self {
        let payload_base = dir.payload_base as u64;
        Self {
            promote_scans: false,
            coalesce_gap: read_unit.max(COALESCE_GAP),
            ..Self::new(leaf_cid, dir, payload_base, leaf_id, fetcher, sidecar_cid)
        }
    }

    /// The whole leaf once the access pattern turns into a scan: fetched in
    /// one request after `SCAN_WHOLE_LEAF_AFTER` range-read leaflet loads,
    /// while leaflets remain to be read.
    fn whole_for_scan(&self, leaflet_idx: usize) -> io::Result<Option<&[u8]>> {
        if let Some(bytes) = self.whole.get() {
            return Ok(Some(bytes));
        }
        if !self.promote_scans {
            return Ok(None);
        }
        let loads = self.range_loads.fetch_add(1, Ordering::Relaxed) + 1;
        if loads <= SCAN_WHOLE_LEAF_AFTER || leaflet_idx + 1 >= self.dir.entries.len() {
            return Ok(None);
        }
        let payload_end = self
            .dir
            .entries
            .iter()
            .map(|e| e.payload_offset as u64 + e.payload_len as u64)
            .max()
            .unwrap_or(0);
        let bytes = self
            .fetcher
            .fetch_range(&self.leaf_cid, 0..self.payload_base + payload_end)?;
        Ok(Some(self.whole.get_or_init(|| bytes)))
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
                if range.start <= last.fetch_range.end + self.coalesce_gap {
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
        order: RunSortOrder,
    ) -> io::Result<ColumnBatch> {
        use super::column_types::ColumnData;
        use crate::format::column_block::{
            decode_column_u16, decode_column_u32, decode_column_u64,
        };

        let entry = &self.dir.entries[leaflet_idx];
        if let Some(bytes) = self.whole_for_scan(leaflet_idx)? {
            return load_leaflet_columns(bytes, entry, self.dir.payload_base, projection, order);
        }
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

        // Pooled-tree leaflets only; dedicated trees have no graph block. Its
        // width is the leaflet's own (2 or 4 bytes).
        let g_ref = entry
            .column_refs
            .iter()
            .find(|r| r.col_id == ColumnId::GId.to_u16())
            .filter(|_| eff.contains(ColumnId::GId));
        let g_id = match g_ref {
            Some(r) if r.elem_width == 4 => {
                ColumnData::Block(Arc::from(find_and_decode_u32(ColumnId::GId)?))
            }
            Some(_) => ColumnData::Block(
                find_and_decode_u16_col(ColumnId::GId)?
                    .into_iter()
                    .map(u32::from)
                    .collect(),
            ),
            None => ColumnData::AbsentDefault,
        };

        Ok(ColumnBatch {
            row_count,
            s_id,
            o_key,
            p_id,
            o_type,
            o_i,
            t,
            g_id,
        })
    }

    fn load_sidecar_segment(&self, leaflet_idx: usize) -> io::Result<Vec<HistEntryV2>> {
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

        crate::format::history_sidecar::decode_segment_bytes(&bytes)
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

/// Speculative first read: the header plus a directory of up to ~330
/// six-column leaflets in one request. Remote stores are latency-bound, so
/// over-reading a small leaf's payload costs nothing next to a second trip.
pub(crate) const SPECULATIVE_HEADER_DIR_BYTES: u64 = 64 * 1024;

/// Largest directory entry for `order`: its fixed fields, a ref per column,
/// and the history ref.
fn max_dir_entry_bytes(order: RunSortOrder) -> u64 {
    (leaflet_dir_fixed_size(order)
        + ColumnId::COUNT * COLUMN_BLOCK_REF_SIZE
        + LEAFLET_DIR_HISTORY_SIZE) as u64
}

/// Fetch enough of a V3 leaf blob to decode the header and full directory.
///
/// One speculative request covers the header and most directories; a
/// directory that outgrows it is fetched once more at its exact upper bound
/// (`leaflet_count` × the largest entry).
///
/// Returns the parsed directory, the payload base offset, and the whole leaf
/// when it fit inside the speculative read.
pub fn fetch_header_and_directory(
    fetcher: &dyn RangeReadFetcher,
    leaf_cid: &ContentId,
) -> io::Result<(DecodedLeafDirV3, u64, Option<Vec<u8>>)> {
    let mut buf = fetcher.fetch_range(leaf_cid, 0..SPECULATIVE_HEADER_DIR_BYTES)?;
    let header = decode_leaf_header_v3(&buf)?;
    if let Ok(dir) = decode_leaf_dir_v3_with_base(&buf, &header) {
        let payload_base = dir.payload_base as u64;
        // A short read stopped at the end of the leaf.
        let whole = ((buf.len() as u64) < SPECULATIVE_HEADER_DIR_BYTES).then_some(buf);
        return Ok((dir, payload_base, whole));
    }
    let upper = leaf_header_size(header.order) as u64
        + header.leaflet_count as u64 * max_dir_entry_bytes(header.order);
    if upper > buf.len() as u64 {
        buf = fetcher.fetch_range(leaf_cid, 0..upper)?;
    }
    let dir = decode_leaf_dir_v3_with_base(&buf, &header)?;
    let payload_base = dir.payload_base as u64;
    Ok((dir, payload_base, None))
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
        let (dir, payload_base, _) =
            fetch_header_and_directory(fetcher.as_ref(), &leaf_cid).unwrap();

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

        let (dir, payload_base, _) =
            fetch_header_and_directory(fetcher.as_ref(), &leaf_cid).unwrap();

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
                op: 1, // assert,
                g_id: 0,
            },
            HistEntryV2 {
                s_id: SubjectId(1),
                p_id: 1,
                o_type: OType::XSD_INTEGER.as_u16(),
                o_key: 50,
                o_i: u32::MAX,
                t: 3,
                op: 0, // retract,
                g_id: 0,
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

        let (mut dir, payload_base, _) =
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
            op: 1, // assert,
            g_id: 0,
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
        let mut full_handle = FullBlobLeafHandle::new(
            leaf_bytes.clone(),
            Some(sidecar_bytes.clone().into()),
            leaf_id,
        )
        .unwrap();
        // Patch history fields.
        let seg = &seg_refs[0];
        let dir = Arc::make_mut(&mut full_handle.dir);
        dir.entries[0].history_offset = seg.offset;
        dir.entries[0].history_len = seg.len;
        dir.entries[0].history_min_t = seg.min_t;
        dir.entries[0].history_max_t = seg.max_t;
        let full_entries = full_handle.load_sidecar_segment(0).unwrap();

        // RangeReadLeafHandle path.
        let mut fetcher = MockFetcher::new();
        fetcher.insert(&leaf_cid, leaf_bytes);
        fetcher.insert(&sidecar_cid, sidecar_bytes);
        let fetcher = Arc::new(fetcher);

        let (mut dir, payload_base, _) =
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

    /// One request covers header + directory for a typical leaf; a directory
    /// past the speculative window costs exactly one more.
    #[test]
    fn header_and_directory_request_count() {
        fn leaf_with_leaflets(n: u64) -> (Vec<u8>, ContentId) {
            let mut writer = LeafWriter::new(RunSortOrder::Post, 1, 100_000, 1);
            writer.set_skip_history(true);
            let ot = OType::XSD_INTEGER.as_u16();
            for i in 0..n {
                writer.push_record(make_rec(i + 1, 1, ot, i, 1)).unwrap();
            }
            let leaf = writer.finish().unwrap().remove(0);
            (leaf.leaf_bytes, leaf.leaf_cid)
        }
        for (leaflets, want_requests) in [(5, 1), (800, 2)] {
            let (bytes, cid) = leaf_with_leaflets(leaflets);
            let mut fetcher = MockFetcher::new();
            fetcher.insert(&cid, bytes);
            let (dir, _, _) = fetch_header_and_directory(&fetcher, &cid).unwrap();
            assert_eq!(dir.entries.len() as u64, leaflets);
            assert_eq!(
                fetcher.request_count(),
                want_requests,
                "{leaflets} leaflets"
            );
        }
    }

    /// A scan over a range-read leaf switches to one whole-leaf fetch after
    /// `SCAN_WHOLE_LEAF_AFTER` leaflets, and decodes the same rows.
    #[test]
    fn range_read_scan_fetches_whole_leaf_once() {
        let mut writer = LeafWriter::new(RunSortOrder::Post, 4, 100_000, 1);
        writer.set_skip_history(true);
        let ot = OType::XSD_INTEGER.as_u16();
        for i in 0..80u64 {
            writer.push_record(make_rec(i + 1, 1, ot, i, 1)).unwrap();
        }
        let leaf = writer.finish().unwrap().remove(0);
        let leaf_id = xxhash_rust::xxh3::xxh3_128(leaf.leaf_cid.to_bytes().as_ref());
        let full = FullBlobLeafHandle::new(leaf.leaf_bytes.clone(), None, leaf_id).unwrap();

        let mut fetcher = MockFetcher::new();
        fetcher.insert(&leaf.leaf_cid, leaf.leaf_bytes);
        let fetcher = Arc::new(fetcher);
        let (dir, payload_base, _) =
            fetch_header_and_directory(fetcher.as_ref(), &leaf.leaf_cid).unwrap();
        let leaflets = dir.entries.len();
        assert_eq!(leaflets, 20);
        let handle = RangeReadLeafHandle::new(
            leaf.leaf_cid,
            dir,
            payload_base,
            leaf_id,
            Arc::clone(&fetcher) as Arc<dyn RangeReadFetcher>,
            None,
        );
        let proj = ColumnProjection::all();
        for i in 0..leaflets {
            let got = handle.load_columns(i, &proj, RunSortOrder::Post).unwrap();
            let want = full.load_columns(i, &proj, RunSortOrder::Post).unwrap();
            assert_eq!(got.row_count, want.row_count);
            for r in 0..got.row_count {
                assert_eq!(got.s_id.get(r), want.s_id.get(r));
                assert_eq!(got.o_key.get(r), want.o_key.get(r));
            }
        }
        // directory + SCAN_WHOLE_LEAF_AFTER range reads + one whole-leaf read
        assert_eq!(fetcher.request_count(), 1 + SCAN_WHOLE_LEAF_AFTER + 1);
    }
}
