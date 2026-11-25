//! The statistics store's objects: base segments and the hot object.
//!
//! Shared by the indexer, which maintains them, and query code, which reads
//! their summaries; HLL registers are plain arrays here.
//!
//! Each holds complete per-(graph, property) entries, sorted by key. A base
//! segment owns a key range and is rewritten only by compaction; the hot
//! object holds every entry changed since its segment was written, which
//! shadows the segment's, plus the drop markers that hide a dropped graph's
//! older entries.
//!
//! # Format
//!
//! Little-endian. An uncompressed 32-byte header:
//!
//! | field                  | type     |
//! |------------------------|----------|
//! | magic                  | `FSS1`   |
//! | kind                   | u8: 0 segment, 1 hot |
//! | reserved               | 3 bytes, zero |
//! | entry count            | u32      |
//! | marker count           | u32      |
//! | summary length         | u32, decompressed |
//! | summary frame length   | u32      |
//! | payload length         | u32, decompressed |
//! | payload frame length   | u32      |
//!
//! then two zstd frames, so a reader that needs no HLL decompresses only the
//! first.
//!
//! **Summary**, one record per entry in key order:
//! `g_id u32, p_id u32, count u64, ndv_values u64, ndv_subjects u64,
//! last_modified_t i64, n u16, n × (datatype tag u8, count u64), m u16,
//! m × historical tag u8, payload length u16`, then the markers in
//! `(g_id, floor)` order: `g_id u32, floor i64`.
//!
//! **Payload**, per entry in the same order: the values HLL then the subjects
//! HLL, each a representation tag and either 256 register bytes (dense) or a
//! u16 pair count and `(register u8, rank u8)` pairs by register (sparse),
//! whichever is smaller; ties go to dense.

use std::io::Read;

use fluree_db_core::GraphId;

/// An entry's key: graph, then property, the order of every store object.
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct StatsKey {
    pub g_id: GraphId,
    pub p_id: u32,
}

/// One HLL256's registers.
pub type Registers = [u8; REGISTERS];

const MAGIC: [u8; 4] = *b"FSS1";
const HEADER_LEN: usize = 32;
/// Bounds what a corrupt or hostile header can make a decoder allocate, per
/// section. Far above any segment the compactor writes.
const MAX_SECTION_BYTES: usize = 256 << 20;

/// Registers per HLL: precision 8.
pub const REGISTERS: usize = 256;
const PRECISION: u8 = 8;
const MAX_RANK: u8 = 64 - PRECISION + 1;
const HLL_DENSE: u8 = 0;
const HLL_SPARSE: u8 = 1;

/// Which object a byte string encodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreObjectKind {
    Segment = 0,
    Hot = 1,
}

/// One (graph, property) entry's statistics, without its HLL registers: all
/// a planner reads.
#[derive(Debug, Clone, PartialEq)]
pub struct EntrySummary {
    pub key: StatsKey,
    pub count: u64,
    pub ndv_values: u64,
    pub ndv_subjects: u64,
    pub last_modified_t: i64,
    /// Live datatype counts by tag, ascending, no zero counts.
    pub datatypes: Vec<(u8, u64)>,
    /// Every datatype tag the entry has seen, ascending.
    pub historical_datatypes: Vec<u8>,
}

/// One (graph, property) entry's complete statistics state.
#[derive(Debug, Clone, PartialEq)]
pub struct StatsEntry {
    pub summary: EntrySummary,
    /// Registers of the distinct-values HLL.
    pub values: Registers,
    /// Registers of the distinct-subjects HLL.
    pub subjects: Registers,
}

/// Hides a dropped graph's entries last modified before its drop's `floor`
/// (`GraphDrop::floor`). Graph ids survive a drop, and a commit can drop a
/// graph and write it afresh at the same `t`, so entries modified at the
/// floor or later belong to the recreated graph and stay visible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DropMarker {
    pub g_id: GraphId,
    pub floor: i64,
}

impl DropMarker {
    /// Whether this marker hides `entry`.
    pub fn hides(&self, entry: &EntrySummary) -> bool {
        entry.key.g_id == self.g_id && entry.last_modified_t < self.floor
    }
}

/// A decoded segment or hot object.
#[derive(Debug, Clone, PartialEq)]
pub struct StoreObject {
    pub kind: StoreObjectKind,
    /// Strictly ascending by key.
    pub entries: Vec<StatsEntry>,
    /// Ascending; always empty in a segment.
    pub markers: Vec<DropMarker>,
}

/// A segment or hot object decoded without HLL registers.
#[derive(Debug, Clone, PartialEq)]
pub struct StoreSummaries {
    pub kind: StoreObjectKind,
    /// Strictly ascending by key.
    pub summaries: Vec<EntrySummary>,
    pub markers: Vec<DropMarker>,
}

/// Why a stats store object could not be read.
#[derive(Debug, thiserror::Error)]
#[error("malformed stats store object: {0}")]
pub struct StoreDecodeError(String);

fn malformed(msg: impl Into<String>) -> StoreDecodeError {
    StoreDecodeError(msg.into())
}

impl StoreObject {
    /// Encode, compressing both sections at `zstd_level`. Entries and markers
    /// must already be in order; the encoder checks rather than sorts, so a
    /// caller's ordering bug surfaces at write time.
    pub fn encode(&self, zstd_level: i32) -> std::io::Result<Vec<u8>> {
        let invalid = |msg: &str| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("stats store: {msg}"),
            )
        };
        if !self
            .entries
            .windows(2)
            .all(|w| w[0].summary.key < w[1].summary.key)
        {
            return Err(invalid("entries out of key order"));
        }
        if !self.markers.windows(2).all(|w| w[0] < w[1]) {
            return Err(invalid("drop markers out of order"));
        }
        if self.kind == StoreObjectKind::Segment && !self.markers.is_empty() {
            return Err(invalid("a segment cannot carry drop markers"));
        }
        for e in self.entries.iter().map(|e| &e.summary) {
            if !e.datatypes.windows(2).all(|w| w[0].0 < w[1].0)
                || e.datatypes.iter().any(|&(_, n)| n == 0)
                || !e.historical_datatypes.windows(2).all(|w| w[0] < w[1])
            {
                return Err(invalid(&format!(
                    "{:?}: datatypes out of order or zero, or historical tags out of order",
                    e.key
                )));
            }
        }

        let mut summary = Vec::with_capacity(self.entries.len() * 64);
        let mut payload = Vec::with_capacity(self.entries.len() * 32);
        for entry in &self.entries {
            let start = payload.len();
            write_hll(&mut payload, &entry.values);
            write_hll(&mut payload, &entry.subjects);
            let payload_len = u16::try_from(payload.len() - start).expect("two HLLs fit a u16");
            let e = &entry.summary;

            summary.extend_from_slice(&e.key.g_id.to_le_bytes());
            summary.extend_from_slice(&e.key.p_id.to_le_bytes());
            summary.extend_from_slice(&e.count.to_le_bytes());
            summary.extend_from_slice(&e.ndv_values.to_le_bytes());
            summary.extend_from_slice(&e.ndv_subjects.to_le_bytes());
            summary.extend_from_slice(&e.last_modified_t.to_le_bytes());
            summary.extend_from_slice(&(e.datatypes.len() as u16).to_le_bytes());
            for &(tag, n) in &e.datatypes {
                summary.push(tag);
                summary.extend_from_slice(&n.to_le_bytes());
            }
            summary.extend_from_slice(&(e.historical_datatypes.len() as u16).to_le_bytes());
            summary.extend_from_slice(&e.historical_datatypes);
            summary.extend_from_slice(&payload_len.to_le_bytes());
        }
        for m in &self.markers {
            summary.extend_from_slice(&m.g_id.to_le_bytes());
            summary.extend_from_slice(&m.floor.to_le_bytes());
        }

        let summary_frame = zstd::bulk::compress(&summary, zstd_level)?;
        let payload_frame = zstd::bulk::compress(&payload, zstd_level)?;
        let len = |n: usize, what: &str| {
            u32::try_from(n).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("stats store {what} of {n} bytes exceeds u32"),
                )
            })
        };
        let mut out = Vec::with_capacity(HEADER_LEN + summary_frame.len() + payload_frame.len());
        out.extend_from_slice(&MAGIC);
        out.push(self.kind as u8);
        out.extend_from_slice(&[0; 3]);
        out.extend_from_slice(&len(self.entries.len(), "entry count")?.to_le_bytes());
        out.extend_from_slice(&len(self.markers.len(), "marker count")?.to_le_bytes());
        out.extend_from_slice(&len(summary.len(), "summary")?.to_le_bytes());
        out.extend_from_slice(&len(summary_frame.len(), "summary frame")?.to_le_bytes());
        out.extend_from_slice(&len(payload.len(), "payload")?.to_le_bytes());
        out.extend_from_slice(&len(payload_frame.len(), "payload frame")?.to_le_bytes());
        out.extend_from_slice(&summary_frame);
        out.extend_from_slice(&payload_frame);
        Ok(out)
    }

    /// Decode every entry, HLL registers included.
    pub fn decode(bytes: &[u8]) -> Result<Self, StoreDecodeError> {
        Self::decode_selected(bytes, |_| true)
    }

    /// Decode every summary and every marker, but HLL registers only for the
    /// keys `wanted` accepts: index maintenance needs registers only for
    /// entries it touches. The payload is still checked whole.
    pub fn decode_selected(
        bytes: &[u8],
        mut wanted: impl FnMut(&StatsKey) -> bool,
    ) -> Result<Self, StoreDecodeError> {
        let header = Header::read(bytes)?;
        let summary = decompress(header.summary_frame(bytes), header.summary_len)?;
        let payload = decompress(header.payload_frame(bytes), header.payload_len)?;
        let (records, markers) = read_summary(&header, &summary)?;

        let mut r = Cursor::new(&payload);
        let mut entries = Vec::new();
        for (summary, payload_len) in records {
            let mut p = Cursor::new(r.take(payload_len)?);
            if !wanted(&summary.key) {
                continue;
            }
            let values = read_hll(&mut p, &summary.key)?;
            let subjects = read_hll(&mut p, &summary.key)?;
            if !p.is_empty() {
                return Err(malformed(format!(
                    "{:?}: payload longer than its HLLs",
                    summary.key
                )));
            }
            entries.push(StatsEntry {
                summary,
                values,
                subjects,
            });
        }
        if !r.is_empty() {
            return Err(malformed(format!(
                "{} trailing payload bytes",
                r.remaining()
            )));
        }
        Ok(Self {
            kind: header.kind,
            entries,
            markers,
        })
    }

    /// Decode summaries and markers only: the payload frame is not touched.
    pub fn decode_summaries(bytes: &[u8]) -> Result<StoreSummaries, StoreDecodeError> {
        let header = Header::read(bytes)?;
        let summary = decompress(header.summary_frame(bytes), header.summary_len)?;
        let (records, markers) = read_summary(&header, &summary)?;
        Ok(StoreSummaries {
            kind: header.kind,
            summaries: records.into_iter().map(|(s, _)| s).collect(),
            markers,
        })
    }
}

/// The fixed header.
struct Header {
    kind: StoreObjectKind,
    entries: usize,
    markers: usize,
    summary_len: usize,
    summary_frame: std::ops::Range<usize>,
    payload_len: usize,
    payload_frame: std::ops::Range<usize>,
}

impl Header {
    fn read(bytes: &[u8]) -> Result<Self, StoreDecodeError> {
        let mut r = Cursor::new(bytes);
        if r.array::<4>()? != MAGIC {
            return Err(malformed("not a stats store object"));
        }
        let kind = match r.u8()? {
            0 => StoreObjectKind::Segment,
            1 => StoreObjectKind::Hot,
            k => return Err(malformed(format!("unknown object kind {k}"))),
        };
        if r.array::<3>()? != [0; 3] {
            return Err(malformed("reserved header bytes set"));
        }
        let entries = r.u32()? as usize;
        let markers = r.u32()? as usize;
        let summary_len = r.u32()? as usize;
        let summary_frame_len = r.u32()? as usize;
        let payload_len = r.u32()? as usize;
        let payload_frame_len = r.u32()? as usize;
        if kind == StoreObjectKind::Segment && markers != 0 {
            return Err(malformed("a segment carries drop markers"));
        }
        let summary_frame = HEADER_LEN..HEADER_LEN + summary_frame_len;
        let payload_frame = summary_frame.end..summary_frame.end + payload_frame_len;
        if payload_frame.end != bytes.len() {
            return Err(malformed(format!(
                "frames end at {}, object is {} bytes",
                payload_frame.end,
                bytes.len()
            )));
        }
        Ok(Self {
            kind,
            entries,
            markers,
            summary_len,
            summary_frame,
            payload_len,
            payload_frame,
        })
    }

    fn summary_frame<'a>(&self, bytes: &'a [u8]) -> &'a [u8] {
        &bytes[self.summary_frame.clone()]
    }

    fn payload_frame<'a>(&self, bytes: &'a [u8]) -> &'a [u8] {
        &bytes[self.payload_frame.clone()]
    }
}

/// Each summary with its payload length, then the markers.
type SummarySection = (Vec<(EntrySummary, usize)>, Vec<DropMarker>);

fn read_summary(header: &Header, summary: &[u8]) -> Result<SummarySection, StoreDecodeError> {
    // The smallest record has no datatypes and no historical tags.
    const MIN_RECORD: usize = 4 + 4 + 8 * 4 + 2 + 2 + 2;
    if header.entries > summary.len() / MIN_RECORD {
        return Err(malformed(format!(
            "{} entries cannot fit a {}-byte summary",
            header.entries,
            summary.len()
        )));
    }
    let mut r = Cursor::new(summary);
    // Each summary with its payload length.
    let mut records: Vec<(EntrySummary, usize)> = Vec::with_capacity(header.entries);
    for _ in 0..header.entries {
        let key = StatsKey {
            g_id: r.u32()?,
            p_id: r.u32()?,
        };
        if records.last().is_some_and(|(prev, _)| prev.key >= key) {
            return Err(malformed(format!("{key:?} out of order")));
        }
        let count = r.u64()?;
        let ndv_values = r.u64()?;
        let ndv_subjects = r.u64()?;
        let last_modified_t = r.i64()?;
        let n = r.u16()? as usize;
        let mut datatypes = Vec::with_capacity(n.min(r.remaining() / 9));
        for _ in 0..n {
            let tag = r.u8()?;
            let c = r.u64()?;
            if datatypes.last().is_some_and(|&(prev, _)| prev >= tag) || c == 0 {
                return Err(malformed(format!(
                    "{key:?}: datatype {tag} out of order or zero"
                )));
            }
            datatypes.push((tag, c));
        }
        let m = r.u16()? as usize;
        let historical_datatypes = r.take(m)?.to_vec();
        if historical_datatypes.windows(2).any(|w| w[0] >= w[1]) {
            return Err(malformed(format!("{key:?}: historical tags out of order")));
        }
        let payload_len = r.u16()? as usize;
        records.push((
            EntrySummary {
                key,
                count,
                ndv_values,
                ndv_subjects,
                last_modified_t,
                datatypes,
                historical_datatypes,
            },
            payload_len,
        ));
    }
    if header.markers != r.remaining() / 12 || !r.remaining().is_multiple_of(12) {
        return Err(malformed(format!(
            "{} markers declared, {} summary bytes left",
            header.markers,
            r.remaining()
        )));
    }
    let mut markers: Vec<DropMarker> = Vec::with_capacity(header.markers);
    for _ in 0..header.markers {
        let marker = DropMarker {
            g_id: r.u32()?,
            floor: r.i64()?,
        };
        if markers.last().is_some_and(|prev| *prev >= marker) {
            return Err(malformed(format!("{marker:?} out of order")));
        }
        markers.push(marker);
    }
    Ok((records, markers))
}

fn write_hll(out: &mut Vec<u8>, registers: &Registers) {
    let nonzero = registers.iter().filter(|&&r| r != 0).count();
    if 3 + 2 * nonzero < 1 + REGISTERS {
        out.push(HLL_SPARSE);
        out.extend_from_slice(&(nonzero as u16).to_le_bytes());
        for (index, &rank) in registers.iter().enumerate() {
            if rank != 0 {
                out.push(index as u8);
                out.push(rank);
            }
        }
    } else {
        out.push(HLL_DENSE);
        out.extend_from_slice(registers);
    }
}

fn read_hll(r: &mut Cursor<'_>, key: &StatsKey) -> Result<Registers, StoreDecodeError> {
    let mut registers = [0u8; REGISTERS];
    match r.u8()? {
        HLL_DENSE => {
            registers.copy_from_slice(r.take(REGISTERS)?);
            if let Some(&rank) = registers.iter().find(|&&rank| rank > MAX_RANK) {
                return Err(malformed(format!("{key:?}: register rank {rank}")));
            }
        }
        HLL_SPARSE => {
            let n = r.u16()? as usize;
            if n > REGISTERS {
                return Err(malformed(format!(
                    "{key:?}: sparse HLL claims {n} registers"
                )));
            }
            let mut prev: Option<u8> = None;
            for pair in r.take(2 * n)?.chunks_exact(2) {
                let (index, rank) = (pair[0], pair[1]);
                if prev.is_some_and(|p| p >= index) || rank == 0 || rank > MAX_RANK {
                    return Err(malformed(format!(
                        "{key:?}: sparse register {index} rank {rank}"
                    )));
                }
                registers[index as usize] = rank;
                prev = Some(index);
            }
        }
        tag => {
            return Err(malformed(format!(
                "{key:?}: unknown HLL representation {tag}"
            )))
        }
    }
    Ok(registers)
}

/// Decompress exactly one zstd frame to exactly `declared` bytes, never
/// buffering more than the ceiling whatever the header claims.
fn decompress(frame: &[u8], declared: usize) -> Result<Vec<u8>, StoreDecodeError> {
    if declared > MAX_SECTION_BYTES {
        return Err(malformed(format!(
            "declared section of {declared} bytes exceeds the {MAX_SECTION_BYTES}-byte ceiling"
        )));
    }
    let frame_len = zstd::zstd_safe::find_frame_compressed_size(frame).map_err(|code| {
        malformed(format!(
            "zstd frame: {}",
            zstd::zstd_safe::get_error_name(code)
        ))
    })?;
    if frame_len != frame.len() {
        return Err(malformed(format!(
            "{} trailing bytes after a zstd frame",
            frame.len() - frame_len
        )));
    }
    let decoder = zstd::stream::read::Decoder::with_buffer(frame)
        .map_err(|e| malformed(format!("zstd: {e}")))?
        .single_frame();
    let mut out = Vec::new();
    decoder
        .take(declared as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|e| malformed(format!("zstd: {e}")))?;
    if out.len() != declared {
        return Err(malformed(format!(
            "section decompressed to {}{} bytes, header declares {declared}",
            out.len(),
            if out.len() > declared { "+" } else { "" }
        )));
    }
    Ok(out)
}

struct Cursor<'a> {
    bytes: &'a [u8],
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], StoreDecodeError> {
        if n > self.bytes.len() {
            return Err(malformed("truncated"));
        }
        let (head, rest) = self.bytes.split_at(n);
        self.bytes = rest;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], StoreDecodeError> {
        Ok(self.take(N)?.try_into().expect("take returns N bytes"))
    }

    fn u8(&mut self) -> Result<u8, StoreDecodeError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, StoreDecodeError> {
        self.array().map(u16::from_le_bytes)
    }

    fn u32(&mut self) -> Result<u32, StoreDecodeError> {
        self.array().map(u32::from_le_bytes)
    }

    fn u64(&mut self) -> Result<u64, StoreDecodeError> {
        self.array().map(u64::from_le_bytes)
    }

    fn i64(&mut self) -> Result<i64, StoreDecodeError> {
        self.array().map(i64::from_le_bytes)
    }

    fn remaining(&self) -> usize {
        self.bytes.len()
    }

    fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `n` nonzero registers (all 256 past that): sparse when few, dense
    /// when many.
    fn hll(n: u64, salt: u64) -> Registers {
        let mut r = [0u8; REGISTERS];
        for (i, reg) in r
            .iter_mut()
            .enumerate()
            .take(n.min(REGISTERS as u64) as usize)
        {
            *reg = ((i as u64 * 7 + salt) % 20 + 1) as u8;
        }
        r
    }

    fn entry(g_id: GraphId, p_id: u32, n: u64) -> StatsEntry {
        StatsEntry {
            summary: EntrySummary {
                key: StatsKey { g_id, p_id },
                count: n * 3,
                ndv_values: n,
                ndv_subjects: n / 2 + 1,
                last_modified_t: 40 + i64::from(p_id),
                datatypes: [(3, n), (7, n * 2)]
                    .into_iter()
                    .filter(|&(_, c)| c > 0)
                    .collect(),
                historical_datatypes: vec![3, 7, 9],
            },
            values: hll(n, 1),
            subjects: hll(n / 2 + 1, 2),
        }
    }

    fn hot() -> StoreObject {
        StoreObject {
            kind: StoreObjectKind::Hot,
            entries: vec![
                entry(0, 1, 0),
                entry(0, 2, 5),
                entry(3, 1, 200),
                entry(9, 4, 4000),
            ],
            markers: vec![
                DropMarker { g_id: 2, floor: 17 },
                DropMarker { g_id: 2, floor: 30 },
                DropMarker { g_id: 5, floor: 8 },
            ],
        }
    }

    #[test]
    fn round_trips_sparse_dense_and_empty_hlls_with_markers() {
        let object = hot();
        let bytes = object.encode(1).unwrap();
        assert_eq!(StoreObject::decode(&bytes).unwrap(), object);

        let segment = StoreObject {
            kind: StoreObjectKind::Segment,
            entries: object.entries.clone(),
            markers: Vec::new(),
        };
        assert_eq!(
            StoreObject::decode(&segment.encode(3).unwrap()).unwrap(),
            segment
        );
    }

    #[test]
    fn an_empty_object_round_trips() {
        let empty = StoreObject {
            kind: StoreObjectKind::Hot,
            entries: Vec::new(),
            markers: Vec::new(),
        };
        assert_eq!(
            StoreObject::decode(&empty.encode(1).unwrap()).unwrap(),
            empty
        );
    }

    #[test]
    fn encoding_is_deterministic() {
        assert_eq!(hot().encode(1).unwrap(), hot().encode(1).unwrap());
    }

    /// A commit can drop a graph and write it afresh at the same `t`
    /// (`GraphRegistry` test `a_commit_can_drop_a_graph_and_write_it_afresh`):
    /// what it wrote at the floor is the new graph's.
    #[test]
    fn a_marker_hides_only_its_graph_before_the_floor() {
        let marker = DropMarker { g_id: 3, floor: 41 };
        let mut e = entry(3, 1, 10).summary;
        assert_eq!(e.last_modified_t, 41);
        assert!(
            !marker.hides(&e),
            "written at the floor: the recreated graph"
        );
        e.last_modified_t = 40;
        assert!(marker.hides(&e), "before the floor: the dropped graph");
        e.last_modified_t = 42;
        assert!(!marker.hides(&e), "after the floor");
        let mut other = entry(4, 1, 10).summary;
        other.last_modified_t = 40;
        assert!(!marker.hides(&other), "another graph");
    }

    #[test]
    fn rejects_corrupt_objects() {
        let bytes = hot().encode(1).unwrap();
        let reject = |b: &[u8], why: &str| {
            assert!(StoreObject::decode(b).is_err(), "{why}");
        };
        reject(&bytes[..bytes.len() - 1], "truncated");
        reject(&[bytes.as_slice(), &[0]].concat(), "trailing byte");
        let mut b = bytes.clone();
        b[0] = b'X';
        reject(&b, "magic");
        let mut b = bytes.clone();
        b[4] = 9;
        reject(&b, "kind");
        let mut b = bytes.clone();
        b[8] = b[8].wrapping_add(1);
        reject(&b, "entry count");
        let mut b = bytes.clone();
        b[24..28].copy_from_slice(&u32::MAX.to_le_bytes());
        reject(&b, "payload length past the ceiling");
        let mut b = bytes.clone();
        b[4] = StoreObjectKind::Segment as u8;
        reject(&b, "segment with markers");
    }

    #[test]
    fn refuses_to_encode_out_of_order_or_misplaced_state() {
        let mut object = hot();
        object.entries.swap(0, 1);
        assert!(object.encode(1).is_err(), "entries out of order");
        let mut object = hot();
        object.markers.swap(0, 2);
        assert!(object.encode(1).is_err(), "markers out of order");
        let object = StoreObject {
            kind: StoreObjectKind::Segment,
            ..hot()
        };
        assert!(object.encode(1).is_err(), "a segment with markers");
        let mut object = hot();
        object.entries[1].summary.datatypes.push((5, 1));
        assert!(object.encode(1).is_err(), "datatypes out of order");
        let mut object = hot();
        object.entries[1].summary.datatypes[0].1 = 0;
        assert!(object.encode(1).is_err(), "a zero datatype count");
    }

    #[test]
    fn summaries_and_selected_registers_decode_without_the_rest() {
        let object = hot();
        let bytes = object.encode(1).unwrap();

        let summaries = StoreObject::decode_summaries(&bytes).unwrap();
        assert_eq!(summaries.kind, StoreObjectKind::Hot);
        assert_eq!(summaries.markers, object.markers);
        assert_eq!(
            summaries.summaries,
            object
                .entries
                .iter()
                .map(|e| e.summary.clone())
                .collect::<Vec<_>>()
        );

        let wanted = StatsKey { g_id: 3, p_id: 1 };
        let selected = StoreObject::decode_selected(&bytes, |k| *k == wanted).unwrap();
        assert_eq!(selected.markers, object.markers);
        assert_eq!(selected.entries, vec![object.entries[2].clone()]);
    }
}
