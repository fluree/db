//! History sidecar encoder/decoder (FHS1) for the V3 index format.
//!
//! The history sidecar is a separate CAS object referenced by the branch
//! manifest (`LeafEntry.sidecar_cid`). It stores per-leaflet history
//! segments containing fixed-size `HistEntryV2` records.
//!
//! ## Sidecar blob layout
//!
//! ```text
//! magic:  [u8; 4]   "FHS1"
//! version: u8       1
//! padding: [u8; 3]  reserved
//! [HistorySegment for leaflet 0]
//! [HistorySegment for leaflet 1]
//! ...
//! [HistorySegment for leaflet N]
//! ```
//!
//! Each segment:
//! ```text
//! entry_count: u32
//! [HistEntryV2 × entry_count]   — sorted by t descending (newest first)
//! ```
//!
//! A pooled tree's sidecar (version 2) appends each entry's graph, so its
//! entries are [`HIST_ENTRY_V2_G_SIZE`] bytes; a segment's entry width follows
//! from its length and count.

use fluree_db_core::subject_id::SubjectId;

/// Magic bytes for history sidecar.
pub const SIDECAR_MAGIC: &[u8; 4] = b"FHS1";

/// Sidecar format version.
pub const SIDECAR_VERSION: u8 = 1;

/// Sidecar format version whose entries carry their graph (pooled trees).
pub const SIDECAR_VERSION_GRAPH: u8 = 2;

/// Header size (magic + version + padding).
pub const SIDECAR_HEADER_SIZE: usize = 8;

/// Wire size of a single `HistEntryV2` record.
///
/// Layout (31 bytes):
/// ```text
/// s_id:    u64  [0..8]
/// p_id:    u32  [8..12]
/// o_type:  u16  [12..14]
/// o_key:   u64  [14..22]
/// o_i:     u32  [22..26]
/// t:       u32  [26..30]
/// op:      u8   [30]
/// ```
pub const HIST_ENTRY_V2_SIZE: usize = 31;

/// Wire size of an entry that carries its graph: the 31 bytes above, then
/// `g_id: u32 [31..35]`.
pub const HIST_ENTRY_V2_G_SIZE: usize = 35;

/// A single history entry for the V2/V3 index format.
///
/// Records a past assert or retract event for time-travel replay.
/// `o_i` is always present (using the `u32::MAX` sentinel for non-list facts)
/// so entries are fixed-size for simple offset arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistEntryV2 {
    pub s_id: SubjectId,
    pub p_id: u32,
    pub o_type: u16,
    pub o_key: u64,
    pub o_i: u32,
    pub t: u32,
    /// 0 = retract, 1 = assert.
    pub op: u8,
    /// Graph of the fact; on the wire only in a pooled tree's sidecar, 0
    /// elsewhere.
    pub g_id: fluree_db_core::GraphId,
}

impl HistEntryV2 {
    /// Write to wire format (31 bytes, little-endian).
    pub fn write_le(&self, buf: &mut [u8]) {
        debug_assert!(buf.len() >= HIST_ENTRY_V2_SIZE);
        buf[0..8].copy_from_slice(&self.s_id.as_u64().to_le_bytes());
        buf[8..12].copy_from_slice(&self.p_id.to_le_bytes());
        buf[12..14].copy_from_slice(&self.o_type.to_le_bytes());
        buf[14..22].copy_from_slice(&self.o_key.to_le_bytes());
        buf[22..26].copy_from_slice(&self.o_i.to_le_bytes());
        buf[26..30].copy_from_slice(&self.t.to_le_bytes());
        buf[30] = self.op;
    }

    /// Read from wire format (31 bytes, little-endian).
    ///
    /// # Errors
    /// Returns `io::Error` if `buf` is shorter than [`HIST_ENTRY_V2_SIZE`] bytes.
    pub fn read_le(buf: &[u8]) -> std::io::Result<Self> {
        if buf.len() < HIST_ENTRY_V2_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "HistEntryV2 requires {} bytes, got {}",
                    HIST_ENTRY_V2_SIZE,
                    buf.len()
                ),
            ));
        }
        Ok(Self {
            s_id: SubjectId(u64::from_le_bytes(buf[0..8].try_into().unwrap())),
            p_id: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            o_type: u16::from_le_bytes(buf[12..14].try_into().unwrap()),
            o_key: u64::from_le_bytes(buf[14..22].try_into().unwrap()),
            o_i: u32::from_le_bytes(buf[22..26].try_into().unwrap()),
            t: u32::from_le_bytes(buf[26..30].try_into().unwrap()),
            op: buf[30],
            g_id: 0,
        })
    }

    /// Write `width` bytes: [`HIST_ENTRY_V2_G_SIZE`] appends the graph.
    pub fn write_le_width(&self, buf: &mut [u8], width: usize) {
        self.write_le(buf);
        if width == HIST_ENTRY_V2_G_SIZE {
            buf[31..35].copy_from_slice(&self.g_id.to_le_bytes());
        }
    }

    /// Read a `width`-byte entry (see [`Self::write_le_width`]).
    pub fn read_le_width(buf: &[u8], width: usize) -> std::io::Result<Self> {
        let mut entry = Self::read_le(buf)?;
        if width == HIST_ENTRY_V2_G_SIZE {
            let g = buf.get(31..35).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "HistEntryV2 with graph requires 35 bytes",
                )
            })?;
            entry.g_id = u32::from_le_bytes(g.try_into().unwrap());
        }
        Ok(entry)
    }
}

/// Every entry of a whole sidecar blob, segment by segment (the segments sit
/// back to back after the header).
pub fn decode_sidecar_entries(bytes: &[u8]) -> std::io::Result<Vec<HistEntryV2>> {
    if bytes.len() < SIDECAR_HEADER_SIZE || bytes[0..4] != *SIDECAR_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "history sidecar: bad header",
        ));
    }
    let width = if bytes[4] == SIDECAR_VERSION_GRAPH {
        HIST_ENTRY_V2_G_SIZE
    } else {
        HIST_ENTRY_V2_SIZE
    };
    let mut out = Vec::new();
    let mut pos = SIDECAR_HEADER_SIZE;
    while pos + 4 <= bytes.len() {
        let count = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        let end = pos + 4 + count * width;
        if end > bytes.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "history sidecar: truncated segment",
            ));
        }
        out.extend(decode_segment_bytes(&bytes[pos..end])?);
        pos = end;
    }
    Ok(out)
}

/// Decode one segment's bytes (count prefix, then entries), taking the entry
/// width from the segment's own length.
pub fn decode_segment_bytes(bytes: &[u8]) -> std::io::Result<Vec<HistEntryV2>> {
    let Some(count) = bytes.get(0..4) else {
        return Ok(Vec::new());
    };
    let count = u32::from_le_bytes(count.try_into().unwrap()) as usize;
    if count == 0 {
        return Ok(Vec::new());
    }
    let body = bytes.len() - 4;
    let width = [HIST_ENTRY_V2_SIZE, HIST_ENTRY_V2_G_SIZE]
        .into_iter()
        .find(|w| body == count * w)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("history segment of {body} bytes does not hold {count} entries"),
            )
        })?;
    (0..count)
        .map(|i| HistEntryV2::read_le_width(&bytes[4 + i * width..], width))
        .collect()
}

/// Per-leaflet history segment info (offset and length into the sidecar blob).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistorySegmentRef {
    /// Absolute byte offset into the sidecar blob (including the 8-byte header).
    pub offset: u64,
    /// Byte length of the segment (including the 4-byte entry_count prefix).
    pub len: u32,
    /// Minimum `t` value across all entries in this segment.
    pub min_t: u32,
    /// Maximum `t` value across all entries in this segment.
    pub max_t: u32,
}

/// Builder for a per-leaf history sidecar blob.
///
/// Accumulates per-leaflet history entry lists, then produces the
/// concatenated sidecar bytes and per-leaflet segment references.
pub struct HistSidecarBuilder {
    /// One entry vec per leaflet.
    leaflet_entries: Vec<Vec<HistEntryV2>>,
    /// Entries carry their graph (a pooled tree's sidecar).
    with_graph: bool,
}

impl HistSidecarBuilder {
    pub fn new() -> Self {
        Self {
            leaflet_entries: Vec::new(),
            with_graph: false,
        }
    }

    /// A builder for a leaf of `order`: pooled orders keep each entry's graph.
    pub fn for_order(order: crate::format::run_record::RunSortOrder) -> Self {
        Self {
            leaflet_entries: Vec::new(),
            with_graph: order.is_pooled(),
        }
    }

    /// Start a new leaflet's history segment.
    pub fn start_leaflet(&mut self) {
        self.leaflet_entries.push(Vec::new());
    }

    /// Add a history entry to the current (last) leaflet segment.
    pub fn push_entry(&mut self, entry: HistEntryV2) {
        if let Some(last) = self.leaflet_entries.last_mut() {
            last.push(entry);
        }
    }

    /// Returns true if the builder has any history entries across all leaflets.
    pub fn has_history(&self) -> bool {
        self.leaflet_entries.iter().any(|v| !v.is_empty())
    }

    /// Build the sidecar blob.
    ///
    /// Returns:
    /// - `sidecar_bytes`: the complete sidecar blob (header + segments)
    /// - `segment_refs`: per-leaflet `HistorySegmentRef` (one per leaflet)
    ///
    /// Entries within each segment are sorted by `t` descending (newest first).
    pub fn build(mut self) -> (Vec<u8>, Vec<HistorySegmentRef>) {
        let mut out = Vec::new();

        // Header.
        out.extend_from_slice(SIDECAR_MAGIC);
        let (version, width) = if self.with_graph {
            (SIDECAR_VERSION_GRAPH, HIST_ENTRY_V2_G_SIZE)
        } else {
            (SIDECAR_VERSION, HIST_ENTRY_V2_SIZE)
        };
        out.push(version);
        out.extend_from_slice(&[0u8; 3]); // padding

        let mut segment_refs = Vec::with_capacity(self.leaflet_entries.len());

        for entries in &mut self.leaflet_entries {
            let offset = out.len() as u64;

            if entries.is_empty() {
                // Empty segment: 0 entries.
                out.extend_from_slice(&0u32.to_le_bytes());
                segment_refs.push(HistorySegmentRef {
                    offset,
                    len: 4,
                    min_t: 0,
                    max_t: 0,
                });
                continue;
            }

            // Sort by t descending (newest first).
            entries.sort_unstable_by_key(|b| std::cmp::Reverse(b.t));

            let entry_count = entries.len() as u32;
            let min_t = entries.last().unwrap().t;
            let max_t = entries[0].t;

            out.extend_from_slice(&entry_count.to_le_bytes());
            for entry in entries.iter() {
                let start = out.len();
                out.resize(start + width, 0);
                entry.write_le_width(&mut out[start..], width);
            }

            let len = (out.len() as u64 - offset) as u32;
            segment_refs.push(HistorySegmentRef {
                offset,
                len,
                min_t,
                max_t,
            });
        }

        (out, segment_refs)
    }
}

impl Default for HistSidecarBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Minimal decoder (for test validation)
// ============================================================================

/// Decode a history segment at the given offset.
///
/// Returns the entries in the order they appear (t descending).
pub fn decode_history_segment(
    data: &[u8],
    seg: &HistorySegmentRef,
) -> std::io::Result<Vec<HistEntryV2>> {
    let offset = seg.offset as usize;
    if offset + 4 > data.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!(
                "history segment offset {} + 4 exceeds data length {}",
                offset,
                data.len()
            ),
        ));
    }
    let required = offset + seg.len as usize;
    if required > data.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!(
                "history segment needs {} bytes but data has {}",
                required,
                data.len()
            ),
        ));
    }
    decode_segment_bytes(&data[offset..required])
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::o_type::OType;
    use fluree_db_core::subject_id::SubjectId;

    fn make_entry(s_id: u64, p_id: u32, o_type: u16, o_key: u64, t: u32, op: u8) -> HistEntryV2 {
        HistEntryV2 {
            s_id: SubjectId(s_id),
            p_id,
            o_type,
            o_key,
            o_i: u32::MAX,
            t,
            op,
            g_id: 0,
        }
    }

    #[test]
    fn hist_entry_roundtrip() {
        let entry = HistEntryV2 {
            s_id: SubjectId(12345),
            p_id: 42,
            o_type: OType::XSD_INTEGER.as_u16(),
            o_key: 999,
            o_i: 3,
            t: 7,
            op: 1,
            g_id: 0,
        };
        let mut buf = [0u8; HIST_ENTRY_V2_SIZE];
        entry.write_le(&mut buf);
        let decoded = HistEntryV2::read_le(&buf).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn a_pooled_sidecar_keeps_each_entry_graph() {
        use crate::format::run_record::RunSortOrder;
        let entry = |g_id, t| HistEntryV2 {
            g_id,
            ..make_entry(1, 10, OType::XSD_INTEGER.as_u16(), 100, t, 1)
        };
        let mut builder = HistSidecarBuilder::for_order(RunSortOrder::Gspo);
        builder.start_leaflet();
        builder.push_entry(entry(7, 3));
        builder.push_entry(entry(300, 5));
        let (bytes, refs) = builder.build();
        assert_eq!(bytes[4], SIDECAR_VERSION_GRAPH);
        assert_eq!(refs[0].len as usize, 4 + 2 * HIST_ENTRY_V2_G_SIZE);
        let decoded = decode_history_segment(&bytes, &refs[0]).unwrap();
        assert_eq!(decoded, vec![entry(300, 5), entry(7, 3)]);
        let at = refs[0].offset as usize;
        assert_eq!(
            decode_segment_bytes(&bytes[at..at + refs[0].len as usize]).unwrap(),
            decoded
        );
    }

    #[test]
    fn a_whole_sidecar_decodes_segment_by_segment() {
        use crate::format::run_record::RunSortOrder;
        let entry = |g_id, t| HistEntryV2 {
            g_id,
            ..make_entry(1, 10, OType::XSD_INTEGER.as_u16(), 100, t, 1)
        };
        let mut builder = HistSidecarBuilder::for_order(RunSortOrder::Gspo);
        builder.start_leaflet();
        builder.push_entry(entry(7, 3));
        builder.start_leaflet();
        builder.start_leaflet();
        builder.push_entry(entry(9, 4));
        builder.push_entry(entry(9, 6));
        let (bytes, _) = builder.build();
        assert_eq!(
            decode_sidecar_entries(&bytes).unwrap(),
            vec![entry(7, 3), entry(9, 6), entry(9, 4)]
        );
    }

    #[test]
    fn a_dedicated_sidecar_is_unchanged() {
        use crate::format::run_record::RunSortOrder;
        let build = |mut builder: HistSidecarBuilder| {
            builder.start_leaflet();
            builder.push_entry(make_entry(1, 10, OType::XSD_INTEGER.as_u16(), 100, 3, 1));
            builder.build()
        };
        let (bytes, refs) = build(HistSidecarBuilder::for_order(RunSortOrder::Spot));
        assert_eq!(bytes, build(HistSidecarBuilder::new()).0);
        assert_eq!(bytes[4], SIDECAR_VERSION);
        assert_eq!(refs[0].len as usize, 4 + HIST_ENTRY_V2_SIZE);
    }

    #[test]
    fn a_segment_whose_length_fits_no_entry_width_is_rejected() {
        let mut bytes = 2u32.to_le_bytes().to_vec();
        bytes.resize(4 + 2 * HIST_ENTRY_V2_SIZE + 1, 0);
        assert!(decode_segment_bytes(&bytes).is_err());
    }

    #[test]
    fn empty_sidecar() {
        let mut builder = HistSidecarBuilder::new();
        builder.start_leaflet();
        builder.start_leaflet();
        assert!(!builder.has_history());
        let (bytes, refs) = builder.build();

        assert_eq!(&bytes[0..4], SIDECAR_MAGIC);
        assert_eq!(bytes[4], SIDECAR_VERSION);
        assert_eq!(refs.len(), 2);
        // Each empty segment is just a 4-byte zero count.
        assert_eq!(refs[0].len, 4);
        assert_eq!(refs[1].len, 4);
    }

    #[test]
    fn sidecar_with_entries() {
        let mut builder = HistSidecarBuilder::new();

        // Leaflet 0: 2 history entries.
        builder.start_leaflet();
        builder.push_entry(make_entry(1, 10, OType::XSD_INTEGER.as_u16(), 100, 3, 1));
        builder.push_entry(make_entry(1, 10, OType::XSD_INTEGER.as_u16(), 100, 5, 0));

        // Leaflet 1: empty.
        builder.start_leaflet();

        // Leaflet 2: 1 entry.
        builder.start_leaflet();
        builder.push_entry(make_entry(2, 20, OType::XSD_STRING.as_u16(), 200, 7, 1));

        assert!(builder.has_history());
        let (bytes, refs) = builder.build();

        assert_eq!(refs.len(), 3);

        // Leaflet 0: sorted by t descending → t=5 first, t=3 second.
        let entries0 = decode_history_segment(&bytes, &refs[0]).unwrap();
        assert_eq!(entries0.len(), 2);
        assert_eq!(entries0[0].t, 5);
        assert_eq!(entries0[1].t, 3);
        assert_eq!(refs[0].min_t, 3);
        assert_eq!(refs[0].max_t, 5);

        // Leaflet 1: empty.
        let entries1 = decode_history_segment(&bytes, &refs[1]).unwrap();
        assert!(entries1.is_empty());

        // Leaflet 2: single entry.
        let entries2 = decode_history_segment(&bytes, &refs[2]).unwrap();
        assert_eq!(entries2.len(), 1);
        assert_eq!(entries2[0].t, 7);
        assert_eq!(refs[2].min_t, 7);
        assert_eq!(refs[2].max_t, 7);
    }
}
