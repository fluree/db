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

use fluree_db_core::subject_id::SubjectId;

/// Magic bytes for history sidecar.
pub const SIDECAR_MAGIC: &[u8; 4] = b"FHS1";

/// Sidecar format version.
pub const SIDECAR_VERSION: u8 = 1;

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
        })
    }
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
}

impl HistSidecarBuilder {
    pub fn new() -> Self {
        Self {
            leaflet_entries: Vec::new(),
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
        out.push(SIDECAR_VERSION);
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
                out.resize(start + HIST_ENTRY_V2_SIZE, 0);
                entry.write_le(&mut out[start..]);
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
    let entry_count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
    let required = offset + 4 + entry_count * HIST_ENTRY_V2_SIZE;
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
    let mut entries = Vec::with_capacity(entry_count);
    let mut pos = offset + 4;
    for _ in 0..entry_count {
        entries.push(HistEntryV2::read_le(&data[pos..])?);
        pos += HIST_ENTRY_V2_SIZE;
    }
    Ok(entries)
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
        };
        let mut buf = [0u8; HIST_ENTRY_V2_SIZE];
        entry.write_le(&mut buf);
        let decoded = HistEntryV2::read_le(&buf).unwrap();
        assert_eq!(entry, decoded);
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
