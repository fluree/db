//! V3 leaf writer (FLI3) — columnar, segmentation-aware, sidecar-producing.
//!
//! ## Key differences from V1 `LeafWriter` (leaf.rs)
//!
//! 1. **Segmentation-aware flushing**: `p_id` transitions flush for POST/PSOT,
//!    `o_type` transitions flush for OPST. SPOT is row-count only.
//! 2. **Variable leaflets per leaf**: threshold is `leaf_target_rows`, not
//!    `leaflets_per_leaf`.
//! 3. **Two-artifact output**: `flush_leaf()` produces both a leaf blob and
//!    a history sidecar. The sidecar CID is NOT stored in the leaf — it lives
//!    in the branch manifest exclusively.
//! 4. **Columnar leaflets**: each leaflet is independently-compressed column
//!    blocks, not R1/R2/R3 regions.
//!
//! ## Blob layout
//!
//! ```text
//! Header (72 bytes):
//!   magic:          [u8; 4]   "FLI3"
//!   version:        u8        1
//!   order:          u8        (0=SPOT, 1=PSOT, 2=POST, 3=OPST)
//!   padding:        [u8; 2]
//!   leaflet_count:  u32
//!   total_rows:     u64
//!   first_key:      [u8; 26]  (order-specific routing key)
//!   last_key:       [u8; 26]  (order-specific routing key)
//!
//! Directory (variable):
//!   [LeafletDirEntryV3 × leaflet_count]
//!
//! Payload:
//!   [concatenated leaflet column block bytes]
//! ```

use std::io;

use fluree_db_core::ContentId;

use super::column_block::{ColumnBlockRef, COLUMN_BLOCK_REF_SIZE};
use super::history_sidecar::{HistEntryV2, HistSidecarBuilder, HistorySegmentRef};
use super::leaflet::{encode_leaflet, EncodedLeaflet};
use super::run_record::RunSortOrder;
use super::run_record_v2::{write_ordered_key_v2, RunRecordV2, ORDERED_KEY_V2_SIZE};

// ── Constants ──────────────────────────────────────────────────────────

pub const LEAF_V3_MAGIC: &[u8; 4] = b"FLI3";
pub const LEAF_V3_VERSION: u8 = 1;

/// Fixed header size: 4 (magic) + 1 (version) + 1 (order) + 2 (pad)
///                   + 4 (leaflet_count) + 8 (total_rows)
///                   + 26 (first_key) + 26 (last_key)
///                   = 72 bytes.
pub const LEAF_V3_HEADER_SIZE: usize = 72;

// ── Output types ───────────────────────────────────────────────────────

/// Info about a produced leaf file (returned to the build pipeline).
#[derive(Debug)]
pub struct LeafInfo {
    /// Content ID of the leaf blob.
    pub leaf_cid: ContentId,
    /// Leaf bytes (for CAS upload).
    pub leaf_bytes: Vec<u8>,
    /// Content ID of the history sidecar blob (None if no history).
    pub sidecar_cid: Option<ContentId>,
    /// Sidecar bytes (for CAS upload; None if no history).
    pub sidecar_bytes: Option<Vec<u8>>,
    /// Total rows across all leaflets in this leaf.
    pub total_rows: u64,
    /// First routing key of the leaf.
    pub first_key: RunRecordV2,
    /// Last routing key of the leaf.
    pub last_key: RunRecordV2,
}

// ── Writer ─────────────────────────────────────────────────────────────

/// V3 leaf writer — segmentation-aware, columnar, sidecar-producing.
pub struct LeafWriter {
    order: RunSortOrder,
    leaflet_target_rows: usize,
    leaf_target_rows: usize,
    zstd_level: i32,
    skip_history: bool,

    // Current leaflet accumulation.
    record_buf: Vec<RunRecordV2>,

    // History accumulation.
    sidecar_builder: HistSidecarBuilder,
    /// Whether a sidecar segment has been started for the current leaflet buffer.
    /// Reset to false on flush_leaflet(). Set to true on first push_history_entry()
    /// or on flush_leaflet() itself (which starts a segment for the encoded leaflet).
    current_segment_started: bool,

    // Segmentation tracking.
    current_seg_p_id: Option<u32>,
    current_seg_o_type: Option<u16>,

    // Encoded leaflets for the current leaf.
    encoded_leaflets: Vec<EncodedLeaflet>,
    history_seg_refs: Vec<HistorySegmentRef>,
    leaf_accumulated_rows: u64,

    // First record of the current leaf (for routing key).
    leaf_first_record: Option<RunRecordV2>,
    // Last record pushed.
    last_record: Option<RunRecordV2>,

    // Completed leaves.
    completed_leaves: Vec<LeafInfo>,
}

impl LeafWriter {
    /// Create a new V3 leaf writer.
    pub fn new(
        order: RunSortOrder,
        leaflet_target_rows: usize,
        leaf_target_rows: usize,
        zstd_level: i32,
    ) -> Self {
        Self {
            order,
            leaflet_target_rows,
            leaf_target_rows,
            zstd_level,
            skip_history: false,
            record_buf: Vec::with_capacity(leaflet_target_rows),
            sidecar_builder: HistSidecarBuilder::new(),
            current_segment_started: false,
            current_seg_p_id: None,
            current_seg_o_type: None,
            encoded_leaflets: Vec::new(),
            history_seg_refs: Vec::new(),
            leaf_accumulated_rows: 0,
            leaf_first_record: None,
            last_record: None,
            completed_leaves: Vec::new(),
        }
    }

    /// Skip history sidecar production (import fast path).
    pub fn set_skip_history(&mut self, skip: bool) {
        self.skip_history = skip;
    }

    /// Push a record into the writer.
    ///
    /// The writer handles segmentation (flushing on key transitions)
    /// and row-count thresholds automatically.
    pub fn push_record(&mut self, record: RunRecordV2) -> io::Result<()> {
        // Check segmentation constraint before buffering.
        if !self.record_buf.is_empty() && self.should_flush_for_segmentation(&record) {
            self.flush_leaflet()?;
        }

        // Track first record of the leaf.
        if self.leaf_first_record.is_none() {
            self.leaf_first_record = Some(record);
        }

        // Update segmentation key tracking.
        match self.order {
            RunSortOrder::Post | RunSortOrder::Psot => {
                self.current_seg_p_id = Some(record.p_id);
            }
            RunSortOrder::Opst => {
                self.current_seg_o_type = Some(record.o_type);
            }
            RunSortOrder::Spot => {}
        }

        self.record_buf.push(record);
        self.last_record = Some(record);

        // Row-count threshold.
        if self.record_buf.len() >= self.leaflet_target_rows {
            self.flush_leaflet()?;
        }

        // Leaf-level threshold.
        if self.leaf_accumulated_rows >= self.leaf_target_rows as u64 {
            self.flush_leaf()?;
        }

        Ok(())
    }

    /// Push a history-only entry (retract-winner: no latest-state row,
    /// but history must be logged).
    ///
    /// History entries are associated with the current leaflet's sidecar segment.
    /// If no segment has been started yet (before the first `flush_leaflet`),
    /// one is started automatically. When `flush_leaflet` runs, if a segment
    /// was already started by history entries, it keeps it; otherwise it starts
    /// a new empty one. This ensures segment count always equals leaflet count.
    pub fn push_history_entry(&mut self, entry: HistEntryV2) {
        if self.skip_history {
            return;
        }
        if !self.current_segment_started {
            self.sidecar_builder.start_leaflet();
            self.current_segment_started = true;
        }
        self.sidecar_builder.push_entry(entry);
    }

    /// Consume the writer, flushing any remaining data, and return all
    /// produced leaves.
    pub fn finish(mut self) -> io::Result<Vec<LeafInfo>> {
        if !self.record_buf.is_empty() {
            self.flush_leaflet()?;
        }
        if !self.encoded_leaflets.is_empty() {
            self.flush_leaf()?;
        }
        Ok(self.completed_leaves)
    }

    // ── Segmentation ───────────────────────────────────────────────────

    fn should_flush_for_segmentation(&self, next: &RunRecordV2) -> bool {
        match self.order {
            RunSortOrder::Post | RunSortOrder::Psot => {
                self.current_seg_p_id.is_some_and(|p| p != next.p_id)
            }
            RunSortOrder::Opst => self.current_seg_o_type.is_some_and(|ot| ot != next.o_type),
            RunSortOrder::Spot => false,
        }
    }

    // ── Flush leaflet ──────────────────────────────────────────────────

    fn flush_leaflet(&mut self) -> io::Result<()> {
        if self.record_buf.is_empty() {
            return Ok(());
        }

        let records = std::mem::take(&mut self.record_buf);
        let encoded = encode_leaflet(&records, self.order, self.zstd_level)?;
        let row_count = encoded.row_count as u64;

        // Ensure a history segment exists for this leaflet.
        // If push_history_entry() already started one, skip. Otherwise start an empty one.
        // This guarantees segment count == leaflet count.
        if !self.skip_history && !self.current_segment_started {
            self.sidecar_builder.start_leaflet();
        }
        // Reset for the next leaflet.
        self.current_segment_started = false;

        self.leaf_accumulated_rows += row_count;
        self.encoded_leaflets.push(encoded);

        // Reset segmentation tracking.
        self.current_seg_p_id = None;
        self.current_seg_o_type = None;

        Ok(())
    }

    // ── Flush leaf ─────────────────────────────────────────────────────

    fn flush_leaf(&mut self) -> io::Result<()> {
        if self.encoded_leaflets.is_empty() {
            return Ok(());
        }

        let first_record = self.leaf_first_record.take().unwrap();
        let last_record = self.last_record.unwrap();

        // 1. Build history sidecar (must come first — CAS ordering).
        let sidecar_builder = std::mem::take(&mut self.sidecar_builder);
        let (sidecar_cid, sidecar_bytes, seg_refs) =
            if !self.skip_history && sidecar_builder.has_history() {
                let (bytes, refs) = sidecar_builder.build();
                let cid = compute_cid_sidecar(&bytes);
                (Some(cid), Some(bytes), refs)
            } else {
                // No sidecar — pass empty refs so leaf directory gets all-zero
                // history fields. Do NOT pass the builder's "empty segment" refs
                // (which have len=4) because there is no sidecar to fetch.
                (None, None, Vec::new())
            };

        // 2. Build the leaf blob.
        let leaflets = std::mem::take(&mut self.encoded_leaflets);
        let leaf_bytes = build_leaf_blob(
            self.order,
            &leaflets,
            &seg_refs,
            &first_record,
            &last_record,
        );
        let leaf_cid = compute_cid_leaf(&leaf_bytes);

        let total_rows = self.leaf_accumulated_rows;

        self.completed_leaves.push(LeafInfo {
            leaf_cid,
            leaf_bytes,
            sidecar_cid,
            sidecar_bytes,
            total_rows,
            first_key: first_record,
            last_key: last_record,
        });

        // Reset for next leaf.
        self.leaf_accumulated_rows = 0;
        self.leaf_first_record = None;
        self.history_seg_refs.clear();

        Ok(())
    }
}

// ── CID helpers ────────────────────────────────────────────────────────

pub fn compute_cid_leaf(bytes: &[u8]) -> ContentId {
    let hex_digest = fluree_db_core::sha256_hex(bytes);
    ContentId::from_hex_digest(
        fluree_db_core::content_kind::CODEC_FLUREE_INDEX_LEAF,
        &hex_digest,
    )
    .expect("valid SHA-256 hex digest")
}

pub fn compute_cid_sidecar(bytes: &[u8]) -> ContentId {
    let hex_digest = fluree_db_core::sha256_hex(bytes);
    ContentId::from_hex_digest(
        fluree_db_core::content_kind::CODEC_FLUREE_HISTORY_SIDECAR,
        &hex_digest,
    )
    .expect("valid SHA-256 hex digest")
}

// ── Leaf blob assembly ─────────────────────────────────────────────────

pub fn build_leaf_blob(
    order: RunSortOrder,
    leaflets: &[EncodedLeaflet],
    history_seg_refs: &[HistorySegmentRef],
    first_record: &RunRecordV2,
    last_record: &RunRecordV2,
) -> Vec<u8> {
    let mut first_key = [0u8; ORDERED_KEY_V2_SIZE];
    let mut last_key = [0u8; ORDERED_KEY_V2_SIZE];
    write_ordered_key_v2(order, first_record, &mut first_key);
    write_ordered_key_v2(order, last_record, &mut last_key);
    build_leaf_blob_raw_keys(order, leaflets, history_seg_refs, &first_key, &last_key)
}

/// Build a leaf blob using pre-computed ordered key bytes for first/last.
///
/// This is useful for incremental leaf assembly where the first/last keys
/// are already available as raw `[u8; 26]` from leaflet directory entries.
pub fn build_leaf_blob_raw_keys(
    order: RunSortOrder,
    leaflets: &[EncodedLeaflet],
    history_seg_refs: &[HistorySegmentRef],
    first_key: &[u8; ORDERED_KEY_V2_SIZE],
    last_key: &[u8; ORDERED_KEY_V2_SIZE],
) -> Vec<u8> {
    let leaflet_count = leaflets.len() as u32;
    let total_rows: u64 = leaflets.iter().map(|l| l.row_count as u64).sum();
    let first_key = *first_key;
    let last_key = *last_key;

    // Pre-compute directory size.
    let dir_size = compute_directory_size(leaflets);
    let payload_offset = LEAF_V3_HEADER_SIZE + dir_size;

    let mut buf = Vec::new();

    // ── Header (72 bytes) ──────────────────────────────────────────
    buf.extend_from_slice(LEAF_V3_MAGIC);
    buf.push(LEAF_V3_VERSION);
    buf.push(order.to_wire_id());
    buf.extend_from_slice(&[0u8; 2]); // padding
    buf.extend_from_slice(&leaflet_count.to_le_bytes());
    buf.extend_from_slice(&total_rows.to_le_bytes());
    buf.extend_from_slice(&first_key);
    buf.extend_from_slice(&last_key);
    debug_assert_eq!(buf.len(), LEAF_V3_HEADER_SIZE);

    // ── Directory ──────────────────────────────────────────────────
    // We need to know each leaflet's payload offset relative to the
    // payload section start. Compute cumulative payload offsets.
    let mut cumulative_payload_offset = 0u32;
    for (i, leaflet) in leaflets.iter().enumerate() {
        // Leaflet dir entry format:
        // row_count: u32 (4)
        // lead_group_count: u32 (4)
        // first_key: [u8; 26] (26)
        // last_key: [u8; 26] (26)
        // p_const: u32 (4), uses u32::MAX for "not present"
        // o_type_const: u16 (2), uses u16::MAX for "not present"
        // flags: u32 (4)
        // payload_offset: u32 (4)  — offset of this leaflet's column data relative to payload start
        // payload_len: u32 (4)     — length of this leaflet's column data
        // column_count: u16 (2)
        // [ColumnBlockRef × column_count] (16 bytes each)
        // history_offset: u64 (8)
        // history_len: u32 (4)
        // history_min_t: u32 (4)
        // history_max_t: u32 (4)

        buf.extend_from_slice(&leaflet.row_count.to_le_bytes());
        buf.extend_from_slice(&leaflet.lead_group_count.to_le_bytes());
        buf.extend_from_slice(&leaflet.first_key);
        buf.extend_from_slice(&leaflet.last_key);
        buf.extend_from_slice(&leaflet.p_const.unwrap_or(u32::MAX).to_le_bytes());
        buf.extend_from_slice(&leaflet.o_type_const.unwrap_or(u16::MAX).to_le_bytes());
        buf.extend_from_slice(&leaflet.flags.to_le_bytes());
        buf.extend_from_slice(&cumulative_payload_offset.to_le_bytes());
        buf.extend_from_slice(&(leaflet.payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(leaflet.column_refs.len() as u16).to_le_bytes());
        for col_ref in &leaflet.column_refs {
            // Adjust column block offsets: they are relative to the leaflet
            // payload start. We need them relative to the leaflet's payload
            // start within the leaf, which is cumulative_payload_offset.
            // Actually, they're already relative to the leaflet payload start
            // — keep them as-is. Readers add cumulative_payload_offset + col_ref.offset.
            let mut ref_buf = [0u8; COLUMN_BLOCK_REF_SIZE];
            col_ref.write_le(&mut ref_buf);
            buf.extend_from_slice(&ref_buf);
        }

        // History segment reference.
        let seg = history_seg_refs.get(i);
        let (h_offset, h_len, h_min_t, h_max_t) = match seg {
            Some(s) => (s.offset, s.len, s.min_t, s.max_t),
            None => (0, 0, 0, 0),
        };
        buf.extend_from_slice(&h_offset.to_le_bytes());
        buf.extend_from_slice(&h_len.to_le_bytes());
        buf.extend_from_slice(&h_min_t.to_le_bytes());
        buf.extend_from_slice(&h_max_t.to_le_bytes());

        cumulative_payload_offset += leaflet.payload.len() as u32;
    }

    debug_assert_eq!(buf.len(), payload_offset);

    // ── Payload ────────────────────────────────────────────────────
    for leaflet in leaflets {
        buf.extend_from_slice(&leaflet.payload);
    }

    buf
}

pub fn compute_directory_size(leaflets: &[EncodedLeaflet]) -> usize {
    leaflets
        .iter()
        .map(|l| {
            // Fixed fields: 4+4+26+26+4+2+4+4+4+2 = 80 bytes
            // Column refs: column_count * 16
            // History: 8+4+4+4 = 20 bytes
            80 + l.column_refs.len() * COLUMN_BLOCK_REF_SIZE + 20
        })
        .sum()
}

// ── Minimal decoder (for test validation) ──────────────────────────────

/// Parsed V3 leaf header.
#[derive(Debug)]
pub struct LeafHeaderV3 {
    pub version: u8,
    pub order: RunSortOrder,
    pub leaflet_count: u32,
    pub total_rows: u64,
    pub first_key: [u8; ORDERED_KEY_V2_SIZE],
    pub last_key: [u8; ORDERED_KEY_V2_SIZE],
}

/// Parsed V3 leaflet directory entry.
#[derive(Debug, Clone)]
pub struct LeafletDirEntryV3 {
    pub row_count: u32,
    pub lead_group_count: u32,
    pub first_key: [u8; ORDERED_KEY_V2_SIZE],
    pub last_key: [u8; ORDERED_KEY_V2_SIZE],
    pub p_const: Option<u32>,
    pub o_type_const: Option<u16>,
    pub flags: u32,
    pub payload_offset: u32,
    pub payload_len: u32,
    pub column_refs: Vec<ColumnBlockRef>,
    pub history_offset: u64,
    pub history_len: u32,
    pub history_min_t: u32,
    pub history_max_t: u32,
}

/// Decode a V3 leaf header. Validates magic bytes.
pub fn decode_leaf_header_v3(data: &[u8]) -> io::Result<LeafHeaderV3> {
    if data.len() < LEAF_V3_HEADER_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "leaf too short",
        ));
    }
    if &data[0..4] != LEAF_V3_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("bad magic: expected FLI3, got {:?}", &data[0..4]),
        ));
    }
    let version = data[4];
    let order = RunSortOrder::from_u8(data[5])
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad order byte"))?;
    let leaflet_count = u32::from_le_bytes(data[8..12].try_into().unwrap());
    let total_rows = u64::from_le_bytes(data[12..20].try_into().unwrap());
    let mut first_key = [0u8; ORDERED_KEY_V2_SIZE];
    first_key.copy_from_slice(&data[20..46]);
    let mut last_key = [0u8; ORDERED_KEY_V2_SIZE];
    last_key.copy_from_slice(&data[46..72]);

    Ok(LeafHeaderV3 {
        version,
        order,
        leaflet_count,
        total_rows,
        first_key,
        last_key,
    })
}

/// Decode the leaflet directory from a V3 leaf blob.
pub fn decode_leaf_dir_v3(
    data: &[u8],
    header: &LeafHeaderV3,
) -> io::Result<Vec<LeafletDirEntryV3>> {
    decode_leaf_dir_v3_with_base(data, header).map(|d| d.entries)
}

/// Decoded leaf directory plus the payload base offset.
///
/// `payload_base` is the byte offset in the leaf blob where the concatenated
/// leaflet column block payloads begin. Pass it to `load_leaflet_columns()`.
#[derive(Debug, Clone)]
pub struct DecodedLeafDirV3 {
    pub entries: Vec<LeafletDirEntryV3>,
    /// Byte offset in the leaf blob where payload data starts
    /// (= header size + directory size). Authoritative — do not recompute.
    pub payload_base: usize,
}

/// Decode the leaflet directory from a V3 leaf blob, returning the directory
/// entries and the authoritative payload base offset.
///
/// Prefer this over `decode_leaf_dir_v3` when you need `payload_base` for
/// column loading — it avoids recomputing directory size.
pub fn decode_leaf_dir_v3_with_base(
    data: &[u8],
    header: &LeafHeaderV3,
) -> io::Result<DecodedLeafDirV3> {
    let mut pos = LEAF_V3_HEADER_SIZE;
    let mut entries = Vec::with_capacity(header.leaflet_count as usize);

    for _ in 0..header.leaflet_count {
        if pos + 80 > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "directory truncated",
            ));
        }

        let row_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        let lead_group_count = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap());
        let mut first_key = [0u8; ORDERED_KEY_V2_SIZE];
        first_key.copy_from_slice(&data[pos + 8..pos + 34]);
        let mut last_key = [0u8; ORDERED_KEY_V2_SIZE];
        last_key.copy_from_slice(&data[pos + 34..pos + 60]);
        let p_const_raw = u32::from_le_bytes(data[pos + 60..pos + 64].try_into().unwrap());
        let p_const = if p_const_raw == u32::MAX {
            None
        } else {
            Some(p_const_raw)
        };
        let o_type_const_raw = u16::from_le_bytes(data[pos + 64..pos + 66].try_into().unwrap());
        let o_type_const = if o_type_const_raw == u16::MAX {
            None
        } else {
            Some(o_type_const_raw)
        };
        let flags = u32::from_le_bytes(data[pos + 66..pos + 70].try_into().unwrap());
        let payload_offset = u32::from_le_bytes(data[pos + 70..pos + 74].try_into().unwrap());
        let payload_len = u32::from_le_bytes(data[pos + 74..pos + 78].try_into().unwrap());
        let column_count =
            u16::from_le_bytes(data[pos + 78..pos + 80].try_into().unwrap()) as usize;
        pos += 80;

        let mut column_refs = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            if pos + COLUMN_BLOCK_REF_SIZE > data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "column ref truncated",
                ));
            }
            let ref_buf: [u8; COLUMN_BLOCK_REF_SIZE] =
                data[pos..pos + COLUMN_BLOCK_REF_SIZE].try_into().unwrap();
            column_refs.push(ColumnBlockRef::read_le(&ref_buf));
            pos += COLUMN_BLOCK_REF_SIZE;
        }

        if pos + 20 > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "history ref truncated",
            ));
        }
        let history_offset = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        let history_len = u32::from_le_bytes(data[pos + 8..pos + 12].try_into().unwrap());
        let history_min_t = u32::from_le_bytes(data[pos + 12..pos + 16].try_into().unwrap());
        let history_max_t = u32::from_le_bytes(data[pos + 16..pos + 20].try_into().unwrap());
        pos += 20;

        entries.push(LeafletDirEntryV3 {
            row_count,
            lead_group_count,
            first_key,
            last_key,
            p_const,
            o_type_const,
            flags,
            payload_offset,
            payload_len,
            column_refs,
            history_offset,
            history_len,
            history_min_t,
            history_max_t,
        });
    }

    Ok(DecodedLeafDirV3 {
        entries,
        payload_base: pos,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::column_block::{decode_column_u64, ColumnId};
    use crate::format::run_record::LIST_INDEX_NONE;
    use fluree_db_core::o_type::OType;
    use fluree_db_core::subject_id::SubjectId;

    fn make_rec(s_id: u64, p_id: u32, o_type: u16, o_key: u64, t: u32) -> RunRecordV2 {
        RunRecordV2 {
            s_id: SubjectId(s_id),
            o_key,
            p_id,
            t,
            o_i: LIST_INDEX_NONE,
            o_type,
            g_id: 0,
        }
    }

    #[test]
    fn basic_post_leaf() {
        let mut writer = LeafWriter::new(RunSortOrder::Post, 100, 1000, 1);
        writer.set_skip_history(true);

        // Push records for a single predicate.
        for i in 0..10 {
            writer
                .push_record(make_rec(i, 1, OType::XSD_INTEGER.as_u16(), i * 10, 1))
                .unwrap();
        }

        let leaves = writer.finish().unwrap();
        assert_eq!(leaves.len(), 1);

        let leaf = &leaves[0];
        assert_eq!(leaf.total_rows, 10);
        assert!(leaf.sidecar_cid.is_none());

        // Decode header.
        let header = decode_leaf_header_v3(&leaf.leaf_bytes).unwrap();
        assert_eq!(header.version, 1);
        assert_eq!(header.order, RunSortOrder::Post);
        assert_eq!(header.leaflet_count, 1);
        assert_eq!(header.total_rows, 10);

        // Decode directory.
        let dir = decode_leaf_dir_v3(&leaf.leaf_bytes, &header).unwrap();
        assert_eq!(dir.len(), 1);
        assert_eq!(dir[0].row_count, 10);
        assert_eq!(dir[0].p_const, Some(1));
        assert_eq!(dir[0].o_type_const, Some(OType::XSD_INTEGER.as_u16()));
    }

    #[test]
    fn segmentation_splits_on_p_id_change() {
        let mut writer = LeafWriter::new(RunSortOrder::Post, 100, 1000, 1);
        writer.set_skip_history(true);

        // 5 records for p_id=1, then 5 for p_id=2.
        for i in 0..5 {
            writer
                .push_record(make_rec(i, 1, OType::XSD_INTEGER.as_u16(), i * 10, 1))
                .unwrap();
        }
        for i in 0..5 {
            writer
                .push_record(make_rec(i, 2, OType::XSD_STRING.as_u16(), i * 10, 2))
                .unwrap();
        }

        let leaves = writer.finish().unwrap();
        assert_eq!(leaves.len(), 1);

        let header = decode_leaf_header_v3(&leaves[0].leaf_bytes).unwrap();
        assert_eq!(header.leaflet_count, 2); // split on p_id transition

        let dir = decode_leaf_dir_v3(&leaves[0].leaf_bytes, &header).unwrap();
        assert_eq!(dir[0].row_count, 5);
        assert_eq!(dir[0].p_const, Some(1));
        assert_eq!(dir[1].row_count, 5);
        assert_eq!(dir[1].p_const, Some(2));
    }

    #[test]
    fn opst_segmentation_splits_on_o_type_change() {
        let mut writer = LeafWriter::new(RunSortOrder::Opst, 100, 1000, 1);
        writer.set_skip_history(true);

        for i in 0..3 {
            writer
                .push_record(make_rec(i, 1, OType::XSD_INTEGER.as_u16(), i * 10, 1))
                .unwrap();
        }
        for i in 0..3 {
            writer
                .push_record(make_rec(i, 1, OType::XSD_STRING.as_u16(), i * 10, 2))
                .unwrap();
        }

        let leaves = writer.finish().unwrap();
        let header = decode_leaf_header_v3(&leaves[0].leaf_bytes).unwrap();
        assert_eq!(header.leaflet_count, 2);

        let dir = decode_leaf_dir_v3(&leaves[0].leaf_bytes, &header).unwrap();
        assert_eq!(dir[0].o_type_const, Some(OType::XSD_INTEGER.as_u16()));
        assert_eq!(dir[1].o_type_const, Some(OType::XSD_STRING.as_u16()));
    }

    #[test]
    fn spot_no_segmentation() {
        let mut writer = LeafWriter::new(RunSortOrder::Spot, 100, 1000, 1);
        writer.set_skip_history(true);

        // Push records with different p_id and o_type — no forced flush.
        writer
            .push_record(make_rec(1, 1, OType::XSD_INTEGER.as_u16(), 10, 1))
            .unwrap();
        writer
            .push_record(make_rec(1, 2, OType::XSD_STRING.as_u16(), 20, 2))
            .unwrap();
        writer
            .push_record(make_rec(2, 1, OType::XSD_DOUBLE.as_u16(), 30, 3))
            .unwrap();

        let leaves = writer.finish().unwrap();
        let header = decode_leaf_header_v3(&leaves[0].leaf_bytes).unwrap();
        assert_eq!(header.leaflet_count, 1); // all in one leaflet
    }

    #[test]
    fn leaf_splits_on_target_rows() {
        // leaflet=5, leaf=10 → 2 records per leaf (2 leaflets of 5 = 10 rows per leaf).
        let mut writer = LeafWriter::new(RunSortOrder::Post, 5, 10, 1);
        writer.set_skip_history(true);

        // 20 records, same predicate → should produce 2 leaves of 10 rows each.
        for i in 0..20u64 {
            writer
                .push_record(make_rec(i, 1, OType::XSD_INTEGER.as_u16(), i * 10, 1))
                .unwrap();
        }

        let leaves = writer.finish().unwrap();
        assert_eq!(leaves.len(), 2);
        assert_eq!(leaves[0].total_rows, 10);
        assert_eq!(leaves[1].total_rows, 10);
    }

    #[test]
    fn column_data_spot_check() {
        let mut writer = LeafWriter::new(RunSortOrder::Post, 100, 1000, 1);
        writer.set_skip_history(true);

        writer
            .push_record(make_rec(100, 5, OType::XSD_INTEGER.as_u16(), 42, 7))
            .unwrap();
        writer
            .push_record(make_rec(200, 5, OType::XSD_INTEGER.as_u16(), 43, 8))
            .unwrap();

        let leaves = writer.finish().unwrap();
        let header = decode_leaf_header_v3(&leaves[0].leaf_bytes).unwrap();
        let dir = decode_leaf_dir_v3(&leaves[0].leaf_bytes, &header).unwrap();

        // Find the o_key column and decode it.
        let entry = &dir[0];
        let o_key_ref = entry
            .column_refs
            .iter()
            .find(|r| r.col_id == ColumnId::OKey.to_u16())
            .unwrap();

        // The column offsets are relative to the leaflet payload within the leaf.
        // The absolute payload start in the leaf blob:
        let payload_section_start = LEAF_V3_HEADER_SIZE + compute_directory_size_from_dir(&dir);
        let leaflet_payload_start = payload_section_start + entry.payload_offset as usize;

        // We need to adjust the block_ref offset to be absolute within leaf_bytes.
        let mut adjusted_ref = *o_key_ref;
        adjusted_ref.offset += leaflet_payload_start as u32;
        let o_keys = decode_column_u64(&leaves[0].leaf_bytes, &adjusted_ref).unwrap();
        assert_eq!(o_keys, vec![42, 43]);
    }

    fn compute_directory_size_from_dir(dir: &[LeafletDirEntryV3]) -> usize {
        dir.iter()
            .map(|e| 80 + e.column_refs.len() * COLUMN_BLOCK_REF_SIZE + 20)
            .sum()
    }
}
