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
//! version: u8       3
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
//! A pooled tree's sidecar (version 4) appends each entry's graph, so its
//! entries are [`HIST_ENTRY_V2_G_SIZE`] bytes. A v4 index's sidecar (version
//! one) stores a 32-bit `o_i` ([`HIST_ENTRY_V2_NARROW_SIZE`]). The three
//! entry widths differ, so a segment's width follows from its length and
//! count.

use fluree_db_core::subject_id::SubjectId;

/// Magic bytes for history sidecar.
pub const SIDECAR_MAGIC: &[u8; 4] = b"FHS1";

/// Sidecar format version.
pub const SIDECAR_VERSION: u8 = 3;

/// Sidecar format version whose entries carry their graph (pooled trees).
pub const SIDECAR_VERSION_GRAPH: u8 = 4;

/// A v4 index's sidecar, whose entries store a 32-bit `o_i`. Read only.
const SIDECAR_VERSION_NARROW: u8 = 1;

/// Header size (magic + version + padding).
pub const SIDECAR_HEADER_SIZE: usize = 8;

/// Wire size of a single `HistEntryV2` record.
///
/// Layout (36 bytes):
/// ```text
/// s_id:    u64  [0..8]
/// p_id:    u32  [8..12]
/// o_type:  u16  [12..14]
/// o_key:   u64  [14..22]
/// o_i:     u64  [22..30]
/// t:       u32  [30..34]
/// op:      u8   [34]
/// reserved: u8  [35]    zero; keeps the width apart from every narrow one
/// ```
pub const HIST_ENTRY_V2_SIZE: usize = 36;

/// [`HIST_ENTRY_V2_SIZE`] in a sidecar storing a 32-bit `o_i`: `o_i: u32
/// [22..26]`, `t: u32 [26..30]`, `op: u8 [30]`.
pub const HIST_ENTRY_V2_NARROW_SIZE: usize = 31;

/// Wire size of an entry that carries its graph: the 36 bytes above, then
/// `g_id: u32 [36..40]`.
pub const HIST_ENTRY_V2_G_SIZE: usize = 40;

/// A single history entry for the V2/V3 index format.
///
/// Records a past assert or retract event for time-travel replay.
/// `o_i` is always present (using the `LIST_INDEX_NONE` sentinel for non-list
/// facts) so entries are fixed-size for simple offset arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistEntryV2 {
    pub s_id: SubjectId,
    pub p_id: u32,
    pub o_type: u16,
    pub o_key: u64,
    pub o_i: u64,
    pub t: u32,
    /// 0 = retract, 1 = assert.
    pub op: u8,
    /// Graph of the fact; on the wire only in a pooled tree's sidecar, 0
    /// elsewhere.
    pub g_id: fluree_db_core::GraphId,
}

impl HistEntryV2 {
    /// Write to wire format (36 bytes, little-endian).
    pub fn write_le(&self, buf: &mut [u8]) {
        debug_assert!(buf.len() >= HIST_ENTRY_V2_SIZE);
        buf[0..8].copy_from_slice(&self.s_id.as_u64().to_le_bytes());
        buf[8..12].copy_from_slice(&self.p_id.to_le_bytes());
        buf[12..14].copy_from_slice(&self.o_type.to_le_bytes());
        buf[14..22].copy_from_slice(&self.o_key.to_le_bytes());
        buf[22..30].copy_from_slice(&self.o_i.to_le_bytes());
        buf[30..34].copy_from_slice(&self.t.to_le_bytes());
        buf[34] = self.op;
        buf[35] = 0;
    }

    /// Read from wire format (36 bytes, little-endian).
    ///
    /// # Errors
    /// Returns `io::Error` if `buf` is shorter than [`HIST_ENTRY_V2_SIZE`] bytes.
    pub fn read_le(buf: &[u8]) -> std::io::Result<Self> {
        Self::read_le_width(buf, HIST_ENTRY_V2_SIZE)
    }

    /// Write `width` bytes: [`HIST_ENTRY_V2_G_SIZE`] appends the graph.
    pub fn write_le_width(&self, buf: &mut [u8], width: usize) {
        self.write_le(buf);
        if width == HIST_ENTRY_V2_G_SIZE {
            buf[36..40].copy_from_slice(&self.g_id.to_le_bytes());
        }
    }

    /// Read a `width`-byte entry: one of the three widths a sidecar stores
    /// (see [`Self::write_le_width`] and the narrow widths).
    pub fn read_le_width(buf: &[u8], width: usize) -> std::io::Result<Self> {
        if buf.len() < width {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("HistEntryV2 requires {width} bytes, got {}", buf.len()),
            ));
        }
        let u32_at = |i: usize| u32::from_le_bytes(buf[i..i + 4].try_into().unwrap());
        let (o_i, t, op) = if width == HIST_ENTRY_V2_NARROW_SIZE {
            (
                crate::format::run_record::widen_list_index(u32_at(22)),
                u32_at(26),
                buf[30],
            )
        } else {
            (
                u64::from_le_bytes(buf[22..30].try_into().unwrap()),
                u32_at(30),
                buf[34],
            )
        };
        Ok(Self {
            s_id: SubjectId(u64::from_le_bytes(buf[0..8].try_into().unwrap())),
            p_id: u32_at(8),
            o_type: u16::from_le_bytes(buf[12..14].try_into().unwrap()),
            o_key: u64::from_le_bytes(buf[14..22].try_into().unwrap()),
            o_i,
            t,
            op,
            g_id: if width == HIST_ENTRY_V2_G_SIZE {
                u32_at(36)
            } else {
                0
            },
        })
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
    let width = match bytes[4] {
        SIDECAR_VERSION_GRAPH => HIST_ENTRY_V2_G_SIZE,
        SIDECAR_VERSION_NARROW => HIST_ENTRY_V2_NARROW_SIZE,
        _ => HIST_ENTRY_V2_SIZE,
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
    let width = [
        HIST_ENTRY_V2_SIZE,
        HIST_ENTRY_V2_G_SIZE,
        HIST_ENTRY_V2_NARROW_SIZE,
    ]
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
    use crate::format::run_record::LIST_INDEX_NONE;
    use fluree_db_core::o_type::OType;
    use fluree_db_core::subject_id::SubjectId;

    fn make_entry(s_id: u64, p_id: u32, o_type: u16, o_key: u64, t: u32, op: u8) -> HistEntryV2 {
        HistEntryV2 {
            s_id: SubjectId(s_id),
            p_id,
            o_type,
            o_key,
            o_i: LIST_INDEX_NONE,
            t,
            op,
            g_id: 0,
        }
    }

    /// `(o_key, t, op)` of what `drop` keeps of `entries`.
    fn kept(drop: &HistoryDrop, mut entries: Vec<HistEntryV2>) -> Vec<(u64, u32, u8)> {
        drop.retain(&mut entries);
        let mut out: Vec<_> = entries.iter().map(|e| (e.o_key, e.t, e.op)).collect();
        out.sort_unstable();
        out
    }

    #[test]
    fn a_range_leaves_out_a_span_only_when_both_ends_fall_in_it() {
        let span = |o_key, from, until| {
            vec![
                make_entry(1, 7, 3, o_key, from, 1),
                make_entry(1, 7, 3, o_key, until, 0),
            ]
        };
        let mut until_4 = HistoryDrop::default();
        until_4.insert(7, 0, Some(4));
        // Inside: gone. Straddling the end: kept whole. After it: kept.
        let entries = [span(10, 1, 3), span(20, 3, 5), span(30, 5, 6)].concat();
        assert_eq!(
            kept(&until_4, entries),
            vec![(20, 3, 1), (20, 5, 0), (30, 5, 1), (30, 6, 0)]
        );
        // One fact's spans, one after another.
        let chain = [span(10, 1, 2), span(10, 3, 5)].concat();
        assert_eq!(kept(&until_4, chain), vec![(10, 3, 1), (10, 5, 0)]);
        // Straddling the start: kept whole.
        let mut from_2 = HistoryDrop::default();
        from_2.insert(7, 2, None);
        assert_eq!(kept(&from_2, span(10, 1, 3)), vec![(10, 1, 1), (10, 3, 0)]);
        // Another predicate is untouched.
        let other = vec![make_entry(1, 8, 3, 10, 1, 1), make_entry(1, 8, 3, 10, 3, 0)];
        assert_eq!(kept(&until_4, other), vec![(10, 1, 1), (10, 3, 0)]);
        // A named graph's tree: the assertion decoded without its graph, the
        // retraction derived from novelty with it.
        let mut named = span(20, 3, 5);
        named[1].g_id = 5;
        assert_eq!(kept(&until_4, named), vec![(20, 3, 1), (20, 5, 0)]);
    }

    #[test]
    fn a_pooled_drop_covers_only_its_graphs() {
        let span = |g_id| {
            [(1, 1), (3, 0)].map(|(t, op)| HistEntryV2 {
                g_id,
                ..make_entry(1, 7, 3, 10, t, op)
            })
        };
        let entries = [span(3), span(4), span(5)].concat();
        let graphs = |drop: &HistoryDrop| {
            let mut entries = entries.clone();
            drop.retain(&mut entries);
            let mut graphs: Vec<_> = entries.iter().map(|e| e.g_id).collect();
            graphs.dedup();
            graphs
        };
        let mut all_but_4 = HistoryDrop::pooled();
        all_but_4.insert_in(Graphs::AllBut(vec![4]), 7, 0, None);
        assert_eq!(graphs(&all_but_4), vec![4]);
        let mut only_4 = HistoryDrop::pooled();
        only_4.insert_in(Graphs::One(4), 7, 0, None);
        assert_eq!(graphs(&only_4), vec![3, 5]);
        // One graph's span ends where another's straddles the range's end.
        let mut until_4 = HistoryDrop::pooled();
        until_4.insert_in(Graphs::AllBut(Vec::new()), 7, 0, Some(4));
        let mut mixed = span(3).to_vec();
        mixed.push(HistEntryV2 {
            g_id: 4,
            ..make_entry(1, 7, 3, 10, 2, 1)
        });
        mixed.push(HistEntryV2 {
            g_id: 4,
            ..make_entry(1, 7, 3, 10, 4, 0)
        });
        assert_eq!(kept(&until_4, mixed), vec![(10, 2, 1), (10, 4, 0)]);
    }

    #[test]
    fn retain_spans_keeps_what_its_window_does_not_close() {
        let mut drop = HistoryDrop::default();
        drop.insert(7, 0, None);
        // The fact was present: the window retracts it, asserts and retracts
        // it again, and asserts it for good.
        let mut events = vec![
            make_entry(1, 7, 3, 10, 2, 0),
            make_entry(1, 7, 3, 10, 3, 1),
            make_entry(1, 7, 3, 10, 4, 0),
            make_entry(1, 7, 3, 10, 5, 1),
        ];
        drop.retain_spans(&mut events, |e| *e);
        let kept: Vec<_> = events.iter().map(|e| (e.t, e.op)).collect();
        assert_eq!(kept, vec![(2, 0), (5, 1)]);
    }

    #[test]
    fn a_span_across_two_ranges_is_kept() {
        let mut drop = HistoryDrop::default();
        drop.insert(7, 0, Some(3));
        drop.insert(7, 5, None);
        let span = vec![make_entry(1, 7, 3, 10, 2, 1), make_entry(1, 7, 3, 10, 6, 0)];
        assert_eq!(kept(&drop, span), vec![(10, 2, 1), (10, 6, 0)]);
    }

    #[test]
    fn a_straddling_span_does_not_keep_an_erased_event() {
        let entries = vec![make_entry(1, 7, 3, 10, 3, 1), make_entry(1, 7, 3, 10, 5, 0)];
        let mut drop = HistoryDrop::default();
        drop.insert(7, 0, Some(4));
        for e in &entries {
            drop.insert_event(e);
        }
        assert!(kept(&drop, entries).is_empty());
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

/// History entries an index build leaves out: by predicate over ranges of
/// `t`, for retention that keeps no history (`f:noHistory`) for a predicate,
/// and as single events, for permanent erasure.
///
/// Both leave out whole spans of one fact, an assertion with the retraction
/// that ended it, so what is left replays consistently at every point. A
/// range leaves a span out only when both its ends fall in it (see
/// [`HistoryDrop::retain`]).
///
/// A pooled tree's drop ([`HistoryDrop::pooled`]) covers some of the graphs
/// the tree holds, and tells facts apart by graph, which its entries name.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HistoryDrop {
    by_p_id: std::collections::HashMap<u32, Vec<Range>>,
    events: std::collections::HashSet<EventKey>,
    pooled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Range {
    from: i64,
    until: i64,
    /// Only a pooled tree's drop reads it.
    graphs: Graphs,
}

impl Range {
    #[inline]
    fn holds(&self, t: u32) -> bool {
        self.from <= i64::from(t) && i64::from(t) < self.until
    }
}

/// The graphs a range of a pooled tree's drop covers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Graphs {
    One(fluree_db_core::GraphId),
    AllBut(Vec<fluree_db_core::GraphId>),
}

impl Graphs {
    fn covers(&self, g_id: fluree_db_core::GraphId) -> bool {
        match self {
            Graphs::One(g) => *g == g_id,
            Graphs::AllBut(except) => !except.contains(&g_id),
        }
    }
}

/// An event's identity: `(s_id, p_id, o_type, o_key, o_i, t, op)`.
type EventKey = (u64, u32, u16, u64, u64, u32, u8);

fn event_key(entry: &HistEntryV2) -> EventKey {
    (
        entry.s_id.as_u64(),
        entry.p_id,
        entry.o_type,
        entry.o_key,
        entry.o_i,
        entry.t,
        entry.op,
    )
}

impl HistoryDrop {
    /// An empty drop for a pooled tree.
    pub fn pooled() -> Self {
        Self {
            pooled: true,
            ..Self::default()
        }
    }

    /// Leave out `p_id`'s history entries with `from_t <= t < until_t`.
    pub fn insert(&mut self, p_id: u32, from_t: i64, until_t: Option<i64>) {
        self.insert_in(Graphs::AllBut(Vec::new()), p_id, from_t, until_t);
    }

    /// [`Self::insert`], in `graphs` of a pooled tree.
    pub fn insert_in(&mut self, graphs: Graphs, p_id: u32, from_t: i64, until_t: Option<i64>) {
        self.by_p_id.entry(p_id).or_default().push(Range {
            from: from_t,
            until: until_t.unwrap_or(i64::MAX),
            graphs,
        });
    }

    /// Leave out the single event `entry`.
    pub fn insert_event(&mut self, entry: &HistEntryV2) {
        self.events.insert(event_key(entry));
    }

    pub fn is_empty(&self) -> bool {
        self.by_p_id.is_empty() && self.events.is_empty()
    }

    /// The ranges that cover `entry`'s predicate, in its graph.
    fn ranges<'a>(&'a self, entry: &'a HistEntryV2) -> impl Iterator<Item = &'a Range> {
        self.by_p_id
            .get(&entry.p_id)
            .into_iter()
            .flatten()
            .filter(move |r| !self.pooled || r.graphs.covers(entry.g_id))
    }

    #[inline]
    fn in_range(&self, entry: &HistEntryV2) -> bool {
        self.ranges(entry).any(|r| r.holds(entry.t))
    }

    #[inline]
    fn holds_span(&self, assert: &HistEntryV2, retract: &HistEntryV2) -> bool {
        self.ranges(assert)
            .any(|r| r.holds(assert.t) && r.holds(retract.t))
    }

    #[inline]
    fn erases(&self, entry: &HistEntryV2) -> bool {
        !self.events.is_empty() && self.events.contains(&event_key(entry))
    }

    /// The spans in `entries` of the predicates this drop covers: each
    /// assertion and the retraction that follows it, by index.
    fn spans(&self, entries: &[HistEntryV2]) -> Vec<(usize, usize)> {
        type Fact = (u64, u32, u16, u64, u64, fluree_db_core::GraphId);
        let mut facts: std::collections::HashMap<Fact, Vec<usize>> =
            std::collections::HashMap::new();
        for (i, e) in entries.iter().enumerate() {
            if self.by_p_id.contains_key(&e.p_id) {
                // A dedicated tree holds one graph, and an entry decoded
                // from its sidecar says 0 where one derived from novelty
                // names it.
                let g_id = if self.pooled { e.g_id } else { 0 };
                let fact = (e.s_id.as_u64(), e.p_id, e.o_type, e.o_key, e.o_i, g_id);
                facts.entry(fact).or_default().push(i);
            }
        }
        let mut spans = Vec::new();
        for mut transitions in facts.into_values() {
            transitions.sort_unstable_by_key(|&i| {
                fluree_db_core::same_t::op_order(entries[i].t, entries[i].op == 1)
            });
            for pair in transitions.windows(2) {
                if entries[pair[0]].op == 1 && entries[pair[1]].op == 0 {
                    spans.push((pair[0], pair[1]));
                }
            }
        }
        spans
    }

    /// Leave out of `entries`, which hold every transition of the facts they
    /// name, what is dropped. A span whose assertion or retraction falls
    /// outside the ranges is kept whole: one asserted while a rule was in
    /// force and retracted after it ended still reads as present between the
    /// two, and never before its assertion.
    pub fn retain(&self, entries: &mut Vec<HistEntryV2>) {
        let mut dropped: Vec<bool> = entries.iter().map(|e| self.in_range(e)).collect();
        let any_ranged = dropped.contains(&true);
        if !any_ranged && self.events.is_empty() {
            return;
        }
        if any_ranged {
            for (assert, retract) in self.spans(entries) {
                let whole = self.holds_span(&entries[assert], &entries[retract]);
                dropped[assert] = whole;
                dropped[retract] = whole;
            }
        }
        let mut i = 0;
        entries.retain(|e| {
            let keep = !dropped[i] && !self.erases(e);
            i += 1;
            keep
        });
    }

    /// Leave out of `events`, a window's transitions of the facts they name,
    /// the spans a range holds whole. Unlike [`Self::retain`], it keeps an
    /// event whose span the window does not close: that event changes what
    /// is current.
    pub fn retain_spans<T>(&self, events: &mut Vec<T>, entry: impl Fn(&T) -> HistEntryV2) {
        if self.by_p_id.is_empty() {
            return;
        }
        let entries: Vec<HistEntryV2> = events.iter().map(entry).collect();
        let mut dropped = vec![false; entries.len()];
        for (assert, retract) in self.spans(&entries) {
            if self.holds_span(&entries[assert], &entries[retract]) {
                dropped[assert] = true;
                dropped[retract] = true;
            }
        }
        let mut i = 0;
        events.retain(|_| {
            let keep = !dropped[i];
            i += 1;
            keep
        });
    }
}
