//! CID-based HLL sketch blob persistence.
//!
//! Per-property HLL sketches are serialized into a single [`HllSketchBlob`] and
//! stored in content-addressed storage (CAS) via `ContentKind::StatsSketch`.
//! The blob's `ContentId` is stored in `IndexRoot.sketch_ref`; the next
//! incremental build is its only reader.
//!
//! # Format v2 (written)
//!
//! Little-endian throughout. An uncompressed header:
//!
//! | field          | type     |
//! |----------------|----------|
//! | magic          | `FHLL`   |
//! | version        | u16 = 2  |
//! | precision      | u8 = 8   |
//! | index_t        | i64      |
//! | entry count    | u32      |
//! | payload length | u32      |
//!
//! then one zstd frame whose decompressed payload is the entries, sorted by
//! `(g_id, p_id)`:
//!
//! `g_id u16, p_id u32, count u64, last_modified_t i64, n_datatypes u8,
//! n × (tag u8, count u64), values HLL, subjects HLL`
//!
//! Each HLL is a representation tag followed by either 256 register bytes
//! (dense) or a u16 pair count and `(register index u8, rank u8)` pairs
//! sorted by index (sparse), whichever is smaller; ties go to dense. Most
//! (graph, property) entries in a many-graph ledger touch a handful of
//! registers, which is what makes sparse worth a second representation.
//!
//! # Format v1 (read only)
//!
//! JSON with hex-encoded dense registers. Recognized by its leading `{`.

use std::collections::HashMap;
use std::io::Read;

use serde::Deserialize;

use crate::error::{IndexerError, Result};
use crate::hll::HllSketch256;
use fluree_db_core::GraphId;

use super::id_hook::{GraphPropertyKey, IdPropertyHll};

const MAGIC: [u8; 4] = *b"FHLL";
const FORMAT_VERSION: u16 = 2;
const HEADER_LEN: usize = 4 + 2 + 1 + 8 + 4 + 4;
const ZSTD_LEVEL: i32 = 1;
/// Far above any plausible sketch; bounds what a corrupt or hostile header
/// can make the decoder allocate.
const MAX_PAYLOAD_BYTES: usize = 256 << 20;

const REGISTERS: usize = 256;
const PRECISION: u8 = HllSketch256::PRECISION as u8;
const MAX_RANK: u8 = 64 - PRECISION + 1;

const HLL_DENSE: u8 = 0;
const HLL_SPARSE: u8 = 1;

/// Smallest encoded entry: fixed fields, no datatypes, two empty sparse HLLs.
const MIN_ENTRY_LEN: usize = 2 + 4 + 8 + 8 + 1 + 2 * 3;

/// Why a persisted sketch could not be read.
#[derive(Debug, thiserror::Error)]
pub enum SketchDecodeError {
    /// Written by a newer format than this build reads: an older indexer is
    /// running against a ledger a newer one has indexed.
    #[error("unsupported stats sketch {0}; this build reads format versions 1 and 2")]
    Unsupported(String),
    /// Corrupt, truncated or otherwise invalid bytes.
    #[error("malformed stats sketch: {0}")]
    Malformed(String),
}

impl From<SketchDecodeError> for IndexerError {
    fn from(e: SketchDecodeError) -> Self {
        IndexerError::Serialization(e.to_string())
    }
}

fn malformed(msg: impl Into<String>) -> SketchDecodeError {
    SketchDecodeError::Malformed(msg.into())
}

/// CAS-persisted HLL sketch blob.
///
/// Contains all per-(graph, property) HLL sketches produced by `IdStatsHook`.
/// Counts are clamped to ≥ 0 (snapshot state, not raw signed deltas).
///
/// Entries are sorted by `(g_id, p_id)` for deterministic serialization and
/// thus deterministic CID computation.
#[derive(Debug)]
pub struct HllSketchBlob {
    /// The maximum transaction time covered (equals `index_t`).
    pub index_t: i64,
    /// Per-(graph, property) HLL entries, sorted by `(g_id, p_id)`.
    pub entries: Vec<HllPropertyEntry>,
}

/// A single property's HLL state within an [`HllSketchBlob`].
#[derive(Debug)]
pub struct HllPropertyEntry {
    /// Graph dictionary ID (0 = default graph).
    pub g_id: GraphId,
    /// Predicate dictionary ID.
    pub p_id: u32,
    /// Flake count (clamped to ≥ 0; snapshot state, not raw delta).
    pub count: u64,
    /// Distinct object values.
    pub values_hll: HllSketch256,
    /// Distinct subjects.
    pub subjects_hll: HllSketch256,
    /// Most recent transaction time for this property.
    pub last_modified_t: i64,
    /// Per-datatype flake counts, sorted by tag, all > 0.
    pub datatypes: Vec<(u8, u64)>,
}

impl HllSketchBlob {
    /// Serialize from the `IdStatsHook`'s properties map.
    ///
    /// Must be called BEFORE `finalize_with_aggregate_properties()` consumes the
    /// hook, by borrowing `hook.properties()`. Counts and per-datatype deltas are
    /// clamped to ≥ 0 (the blob represents snapshot state, not raw signed deltas).
    pub fn from_properties(
        index_t: i64,
        properties: &HashMap<GraphPropertyKey, IdPropertyHll>,
    ) -> Self {
        let mut entries: Vec<HllPropertyEntry> = properties
            .iter()
            .map(|(key, hll)| {
                let mut dt_vec: Vec<(u8, u64)> = hll
                    .datatypes
                    .iter()
                    .filter(|(_, &v)| v > 0)
                    .map(|(&k, &v)| (k, v as u64))
                    .collect();
                dt_vec.sort_by_key(|&(tag, _)| tag);

                HllPropertyEntry {
                    g_id: key.g_id,
                    p_id: key.p_id,
                    count: hll.count.max(0) as u64,
                    values_hll: hll.values_hll.clone(),
                    subjects_hll: hll.subjects_hll.clone(),
                    last_modified_t: hll.last_modified_t,
                    datatypes: dt_vec,
                }
            })
            .collect();
        entries.sort_by_key(|a| (a.g_id, a.p_id));

        Self { index_t, entries }
    }

    /// Encode as format v2. Deterministic for a given zstd version.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let entry_count = u32::try_from(self.entries.len()).map_err(|_| {
            IndexerError::Serialization(format!(
                "stats sketch has {} entries, more than the format holds",
                self.entries.len()
            ))
        })?;

        let mut payload = Vec::with_capacity(self.entries.len() * 64);
        for e in &self.entries {
            let n_datatypes = u8::try_from(e.datatypes.len()).map_err(|_| {
                IndexerError::Serialization(format!(
                    "stats sketch entry g{}:p{} has {} datatypes, more than the format holds",
                    e.g_id,
                    e.p_id,
                    e.datatypes.len()
                ))
            })?;
            payload.extend_from_slice(&e.g_id.to_le_bytes());
            payload.extend_from_slice(&e.p_id.to_le_bytes());
            payload.extend_from_slice(&e.count.to_le_bytes());
            payload.extend_from_slice(&e.last_modified_t.to_le_bytes());
            payload.push(n_datatypes);
            for &(tag, count) in &e.datatypes {
                payload.push(tag);
                payload.extend_from_slice(&count.to_le_bytes());
            }
            write_hll(&mut payload, &e.values_hll);
            write_hll(&mut payload, &e.subjects_hll);
        }
        if payload.len() > MAX_PAYLOAD_BYTES {
            return Err(IndexerError::Serialization(format!(
                "stats sketch payload of {} bytes exceeds the {MAX_PAYLOAD_BYTES}-byte ceiling",
                payload.len()
            )));
        }

        let frame = zstd::bulk::compress(&payload, ZSTD_LEVEL)
            .map_err(|e| IndexerError::Serialization(format!("stats sketch compress: {e}")))?;

        let mut out = Vec::with_capacity(HEADER_LEN + frame.len());
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        out.push(PRECISION);
        out.extend_from_slice(&self.index_t.to_le_bytes());
        out.extend_from_slice(&entry_count.to_le_bytes());
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&frame);
        Ok(out)
    }

    /// Decode a persisted sketch, format v1 (JSON) or v2 (binary).
    pub fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, SketchDecodeError> {
        match bytes.first() {
            Some(b'{') => decode_v1(bytes),
            Some(_) if bytes.starts_with(&MAGIC) => decode_v2(bytes),
            Some(_) => Err(malformed("unrecognized leading bytes")),
            None => Err(malformed("empty")),
        }
    }

    /// Reconstruct the `HashMap<GraphPropertyKey, IdPropertyHll>` from the blob.
    ///
    /// Used to load prior sketches for incremental refresh.
    pub fn into_properties(self) -> HashMap<GraphPropertyKey, IdPropertyHll> {
        self.entries
            .into_iter()
            .map(|e| {
                let datatypes = e
                    .datatypes
                    .into_iter()
                    .map(|(k, v)| (k, v as i64))
                    .collect();
                (
                    GraphPropertyKey {
                        g_id: e.g_id,
                        p_id: e.p_id,
                    },
                    IdPropertyHll::from_sketches(
                        e.count as i64,
                        e.values_hll,
                        e.subjects_hll,
                        e.last_modified_t,
                        datatypes,
                    ),
                )
            })
            .collect()
    }
}

fn write_hll(out: &mut Vec<u8>, hll: &HllSketch256) {
    let registers = hll.registers();
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

fn decode_v2(bytes: &[u8]) -> std::result::Result<HllSketchBlob, SketchDecodeError> {
    let mut header =
        Cursor::new(bytes.get(..HEADER_LEN).ok_or_else(|| {
            malformed(format!("{} bytes is shorter than the header", bytes.len()))
        })?);
    header.take(MAGIC.len())?;
    let version = header.u16()?;
    if version != FORMAT_VERSION {
        return Err(SketchDecodeError::Unsupported(format!(
            "format version {version}"
        )));
    }
    let precision = header.u8()?;
    if precision != PRECISION {
        return Err(SketchDecodeError::Unsupported(format!(
            "HLL precision {precision}"
        )));
    }
    let index_t = header.i64()?;
    let entry_count = header.u32()? as usize;
    let payload_len = header.u32()? as usize;

    let payload = decompress(&bytes[HEADER_LEN..], payload_len)?;

    if entry_count > payload.len() / MIN_ENTRY_LEN {
        return Err(malformed(format!(
            "{entry_count} entries cannot fit in {} payload bytes",
            payload.len()
        )));
    }
    let mut r = Cursor::new(&payload);
    let mut entries: Vec<HllPropertyEntry> = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let g_id = r.u16()?;
        let p_id = r.u32()?;
        if let Some(prev) = entries.last() {
            if (prev.g_id, prev.p_id) >= (g_id, p_id) {
                return Err(malformed(format!(
                    "entry g{g_id}:p{p_id} out of order after g{}:p{}",
                    prev.g_id, prev.p_id
                )));
            }
        }
        let count = r.u64()?;
        let last_modified_t = r.i64()?;
        let n_datatypes = r.u8()?;
        let mut datatypes = Vec::with_capacity(n_datatypes as usize);
        for _ in 0..n_datatypes {
            let tag = r.u8()?;
            let dt_count = r.u64()?;
            if dt_count == 0 {
                return Err(malformed(format!(
                    "g{g_id}:p{p_id} datatype {tag} has a zero count"
                )));
            }
            if datatypes.last().is_some_and(|&(prev, _)| prev >= tag) {
                return Err(malformed(format!(
                    "g{g_id}:p{p_id} datatype {tag} out of order"
                )));
            }
            datatypes.push((tag, dt_count));
        }
        let values_hll = read_hll(&mut r, g_id, p_id)?;
        let subjects_hll = read_hll(&mut r, g_id, p_id)?;
        entries.push(HllPropertyEntry {
            g_id,
            p_id,
            count,
            values_hll,
            subjects_hll,
            last_modified_t,
            datatypes,
        });
    }
    if !r.is_empty() {
        return Err(malformed(format!(
            "{} trailing payload bytes",
            r.remaining()
        )));
    }
    Ok(HllSketchBlob { index_t, entries })
}

/// Decompress exactly one zstd frame to exactly `declared` bytes, never
/// buffering more than the ceiling whatever the header claims.
fn decompress(frame: &[u8], declared: usize) -> std::result::Result<Vec<u8>, SketchDecodeError> {
    if declared > MAX_PAYLOAD_BYTES {
        return Err(malformed(format!(
            "declared payload of {declared} bytes exceeds the {MAX_PAYLOAD_BYTES}-byte ceiling"
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
            "{} trailing bytes after the zstd frame",
            frame.len() - frame_len
        )));
    }
    let decoder = zstd::stream::read::Decoder::with_buffer(frame)
        .map_err(|e| malformed(format!("zstd: {e}")))?
        .single_frame();
    let mut payload = Vec::new();
    decoder
        .take(declared as u64 + 1)
        .read_to_end(&mut payload)
        .map_err(|e| malformed(format!("zstd: {e}")))?;
    if payload.len() != declared {
        return Err(malformed(format!(
            "payload decompressed to {}{} bytes, header declares {declared}",
            payload.len(),
            if payload.len() > declared { "+" } else { "" }
        )));
    }
    Ok(payload)
}

fn read_hll(
    r: &mut Cursor<'_>,
    g_id: GraphId,
    p_id: u32,
) -> std::result::Result<HllSketch256, SketchDecodeError> {
    let mut registers = [0u8; REGISTERS];
    match r.u8()? {
        HLL_DENSE => {
            registers.copy_from_slice(r.take(REGISTERS)?);
            if let Some(&rank) = registers.iter().find(|&&rank| rank > MAX_RANK) {
                return Err(malformed(format!("g{g_id}:p{p_id} register rank {rank}")));
            }
        }
        HLL_SPARSE => {
            let n = r.u16()? as usize;
            if n > REGISTERS {
                return Err(malformed(format!(
                    "g{g_id}:p{p_id} sparse HLL claims {n} registers"
                )));
            }
            let mut prev: Option<u8> = None;
            for pair in r.take(2 * n)?.chunks_exact(2) {
                let (index, rank) = (pair[0], pair[1]);
                if prev.is_some_and(|p| p >= index) {
                    return Err(malformed(format!(
                        "g{g_id}:p{p_id} sparse register {index} out of order"
                    )));
                }
                if rank == 0 || rank > MAX_RANK {
                    return Err(malformed(format!(
                        "g{g_id}:p{p_id} sparse register {index} rank {rank}"
                    )));
                }
                registers[index as usize] = rank;
                prev = Some(index);
            }
        }
        tag => {
            return Err(malformed(format!(
                "g{g_id}:p{p_id} unknown HLL representation {tag}"
            )))
        }
    }
    Ok(HllSketch256::from_bytes(&registers))
}

struct Cursor<'a> {
    bytes: &'a [u8],
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    fn take(&mut self, n: usize) -> std::result::Result<&'a [u8], SketchDecodeError> {
        if n > self.bytes.len() {
            return Err(malformed("truncated"));
        }
        let (head, rest) = self.bytes.split_at(n);
        self.bytes = rest;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> std::result::Result<[u8; N], SketchDecodeError> {
        Ok(self.take(N)?.try_into().expect("take returns N bytes"))
    }

    fn u8(&mut self) -> std::result::Result<u8, SketchDecodeError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> std::result::Result<u16, SketchDecodeError> {
        self.array().map(u16::from_le_bytes)
    }

    fn u32(&mut self) -> std::result::Result<u32, SketchDecodeError> {
        self.array().map(u32::from_le_bytes)
    }

    fn u64(&mut self) -> std::result::Result<u64, SketchDecodeError> {
        self.array().map(u64::from_le_bytes)
    }

    fn i64(&mut self) -> std::result::Result<i64, SketchDecodeError> {
        self.array().map(i64::from_le_bytes)
    }

    fn remaining(&self) -> usize {
        self.bytes.len()
    }

    fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

#[derive(Deserialize)]
struct V1Blob {
    version: u32,
    index_t: i64,
    entries: Vec<V1Entry>,
}

#[derive(Deserialize)]
struct V1Entry {
    g_id: GraphId,
    p_id: u32,
    count: u64,
    values_hll: String,
    subjects_hll: String,
    last_modified_t: i64,
    #[serde(default)]
    datatypes: Vec<(u8, u64)>,
}

fn decode_v1(bytes: &[u8]) -> std::result::Result<HllSketchBlob, SketchDecodeError> {
    let blob: V1Blob = serde_json::from_slice(bytes).map_err(|e| malformed(e.to_string()))?;
    if blob.version != 1 {
        return Err(SketchDecodeError::Unsupported(format!(
            "JSON version {}",
            blob.version
        )));
    }
    let hll = |hex_str: &str, g_id: GraphId, p_id: u32| {
        hex::decode(hex_str)
            .ok()
            .and_then(|b| HllSketch256::from_slice(&b))
            .ok_or_else(|| malformed(format!("bad v1 HLL registers for g{g_id}:p{p_id}")))
    };
    let entries = blob
        .entries
        .into_iter()
        .map(|e| {
            Ok(HllPropertyEntry {
                values_hll: hll(&e.values_hll, e.g_id, e.p_id)?,
                subjects_hll: hll(&e.subjects_hll, e.g_id, e.p_id)?,
                g_id: e.g_id,
                p_id: e.p_id,
                count: e.count,
                last_modified_t: e.last_modified_t,
                datatypes: e.datatypes,
            })
        })
        .collect::<std::result::Result<_, SketchDecodeError>>()?;
    Ok(HllSketchBlob {
        index_t: blob.index_t,
        entries,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn regs(pairs: impl IntoIterator<Item = (usize, u8)>) -> HllSketch256 {
        let mut r = [0u8; REGISTERS];
        for (i, v) in pairs {
            r[i] = v;
        }
        HllSketch256::from_bytes(&r)
    }

    /// Properties encoded in both `testdata` fixtures: a full dense HLL, an
    /// empty one, single-register ones, and both sides of the crossover
    /// (127 nonzero → dense, 126 → sparse) within one entry.
    fn fixture_properties() -> HashMap<GraphPropertyKey, IdPropertyHll> {
        HashMap::from([
            (
                GraphPropertyKey { g_id: 0, p_id: 1 },
                IdPropertyHll::from_sketches(
                    5,
                    regs((0..256).map(|i| (i, (i % 57) as u8 + 1))),
                    regs([(17, 4), (200, 57)]),
                    10,
                    HashMap::from([(3u8, 3i64), (5, 2)]),
                ),
            ),
            (
                GraphPropertyKey { g_id: 3, p_id: 7 },
                IdPropertyHll::from_sketches(
                    1,
                    regs([]),
                    regs([(0, 1)]),
                    12,
                    HashMap::from([(3u8, 1i64)]),
                ),
            ),
            (
                GraphPropertyKey { g_id: 3, p_id: 2 },
                IdPropertyHll::from_sketches(
                    2,
                    regs((0..127).map(|i| (i * 2, 3))),
                    regs((0..126).map(|i| (i * 2 + 1, 9))),
                    11,
                    HashMap::new(),
                ),
            ),
        ])
    }

    fn assert_matches_properties(
        blob: HllSketchBlob,
        expected: &HashMap<GraphPropertyKey, IdPropertyHll>,
    ) {
        let restored = blob.into_properties();
        assert_eq!(restored.len(), expected.len());
        for (key, want) in expected {
            let got = &restored[key];
            assert_eq!(got.count, want.count, "{key:?} count");
            assert_eq!(got.last_modified_t, want.last_modified_t, "{key:?} t");
            assert_eq!(got.datatypes, want.datatypes, "{key:?} datatypes");
            assert_eq!(
                got.values_hll.registers(),
                want.values_hll.registers(),
                "{key:?} values registers"
            );
            assert_eq!(
                got.subjects_hll.registers(),
                want.subjects_hll.registers(),
                "{key:?} subjects registers"
            );
        }
    }

    fn round_trip(props: &HashMap<GraphPropertyKey, IdPropertyHll>) -> HllSketchBlob {
        let bytes = HllSketchBlob::from_properties(10, props)
            .to_bytes()
            .unwrap();
        HllSketchBlob::from_bytes(&bytes).unwrap()
    }

    fn single(
        values: HllSketch256,
        subjects: HllSketch256,
    ) -> HashMap<GraphPropertyKey, IdPropertyHll> {
        HashMap::from([(
            GraphPropertyKey { g_id: 0, p_id: 1 },
            IdPropertyHll::from_sketches(1, values, subjects, 1, HashMap::new()),
        )])
    }

    // ---- fixtures ----

    /// Written by the v1 JSON encoder, before v2 existed.
    #[test]
    fn decodes_pinned_v1_fixture() {
        let blob = HllSketchBlob::from_bytes(include_bytes!("testdata/sketch_v1.json")).unwrap();
        assert_eq!(blob.index_t, 12);
        assert_matches_properties(blob, &fixture_properties());
    }

    /// Written by the first v2 encoder. Only decoded, never compared against
    /// fresh encoder output: a zstd upgrade may change compressed bytes.
    #[test]
    fn decodes_pinned_v2_fixture() {
        let bytes = include_bytes!("testdata/sketch_v2.bin");
        assert_eq!(&bytes[..4], b"FHLL");
        let blob = HllSketchBlob::from_bytes(bytes).unwrap();
        assert_eq!(blob.index_t, 12);
        assert_matches_properties(blob, &fixture_properties());
    }

    // ---- round trips ----

    #[test]
    fn round_trips_fixture_properties() {
        let props = fixture_properties();
        let blob = round_trip(&props);
        assert_eq!(blob.index_t, 10);
        let keys: Vec<_> = blob.entries.iter().map(|e| (e.g_id, e.p_id)).collect();
        assert_eq!(keys, vec![(0, 1), (3, 2), (3, 7)]);
        assert_eq!(blob.entries[0].datatypes, vec![(3, 3), (5, 2)]);
        assert_matches_properties(blob, &props);
    }

    #[test]
    fn round_trips_every_fill_around_the_crossover() {
        for nonzero in [0, 1, 2, 125, 126, 127, 128, 200, 255, 256] {
            let values = regs((0..nonzero).map(|i| (i, (i % 57) as u8 + 1)));
            let subjects = regs((0..nonzero).map(|i| (255 - i, 57)));
            let props = single(values, subjects);
            assert_matches_properties(round_trip(&props), &props);
        }
    }

    #[test]
    fn picks_the_smaller_representation_and_dense_on_ties() {
        let encoded = |nonzero: usize| {
            let mut out = Vec::new();
            write_hll(&mut out, &regs((0..nonzero).map(|i| (i, 1))));
            out
        };
        assert_eq!(encoded(0), vec![HLL_SPARSE, 0, 0]);
        assert_eq!(encoded(1), vec![HLL_SPARSE, 1, 0, 0, 1]);
        assert_eq!(encoded(126)[0], HLL_SPARSE);
        assert_eq!(encoded(126).len(), 3 + 2 * 126);
        assert_eq!(encoded(127)[0], HLL_DENSE, "257 bytes either way: dense");
        assert_eq!(encoded(127).len(), 1 + REGISTERS);
        assert_eq!(encoded(128)[0], HLL_DENSE);
    }

    #[test]
    fn estimates_and_merges_match_the_original_after_round_trip() {
        let mut values = HllSketch256::new();
        let mut subjects = HllSketch256::new();
        for h in 0..40u64 {
            values.insert_hash(h.wrapping_mul(0x9E37_79B9_7F4A_7C15));
            subjects.insert_hash(h.wrapping_mul(0xC2B2_AE3D_27D4_EB4F));
        }
        let props = single(values.clone(), subjects.clone());
        let restored = round_trip(&props).into_properties();
        let got = &restored[&GraphPropertyKey { g_id: 0, p_id: 1 }];
        assert_eq!(got.values_hll.estimate(), values.estimate());
        assert_eq!(got.subjects_hll.estimate(), subjects.estimate());

        let mut merged_original = values.clone();
        merged_original.merge(&subjects);
        let mut merged_restored = got.values_hll.clone();
        merged_restored.merge(&got.subjects_hll);
        assert_eq!(merged_restored, merged_original);
    }

    #[test]
    fn empty_sketch_round_trips() {
        let blob = round_trip(&HashMap::new());
        assert!(blob.entries.is_empty());
    }

    #[test]
    fn bytes_do_not_depend_on_insertion_order() {
        let encode = |order: &[(u16, u32)]| {
            let mut source = fixture_properties();
            let mut map = HashMap::new();
            for &(g_id, p_id) in order {
                let key = GraphPropertyKey { g_id, p_id };
                let hll = source.remove(&key).unwrap();
                map.insert(key, hll);
            }
            HllSketchBlob::from_properties(10, &map).to_bytes().unwrap()
        };
        let forward = encode(&[(0, 1), (3, 2), (3, 7)]);
        assert_eq!(encode(&[(3, 7), (3, 2), (0, 1)]), forward);
        assert_eq!(encode(&[(3, 2), (0, 1), (3, 7)]), forward);
    }

    #[test]
    fn clamps_negatives_and_drops_empty_datatypes() {
        let mut hll = IdPropertyHll::new();
        hll.count = -3;
        hll.datatypes.insert(3, -2);
        hll.datatypes.insert(4, 0);
        hll.datatypes.insert(5, 1);
        let map = HashMap::from([(GraphPropertyKey { g_id: 0, p_id: 1 }, hll)]);

        let blob = HllSketchBlob::from_properties(5, &map);
        assert_eq!(blob.entries[0].count, 0);
        assert_eq!(blob.entries[0].datatypes, vec![(5, 1)]);
        let bytes = blob.to_bytes().unwrap();
        assert_eq!(
            HllSketchBlob::from_bytes(&bytes).unwrap().entries[0].count,
            0
        );
    }

    // ---- rejections ----

    fn sparse(pairs: &[(u8, u8)]) -> Vec<u8> {
        let mut out = vec![HLL_SPARSE];
        out.extend_from_slice(&(pairs.len() as u16).to_le_bytes());
        for &(i, r) in pairs {
            out.extend_from_slice(&[i, r]);
        }
        out
    }

    fn entry(
        g_id: u16,
        p_id: u32,
        datatypes: &[(u8, u64)],
        values: &[u8],
        subjects: &[u8],
    ) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&g_id.to_le_bytes());
        out.extend_from_slice(&p_id.to_le_bytes());
        out.extend_from_slice(&7u64.to_le_bytes());
        out.extend_from_slice(&3i64.to_le_bytes());
        out.push(datatypes.len() as u8);
        for &(tag, count) in datatypes {
            out.push(tag);
            out.extend_from_slice(&count.to_le_bytes());
        }
        out.extend_from_slice(values);
        out.extend_from_slice(subjects);
        out
    }

    struct Header {
        version: u16,
        precision: u8,
        entries: u32,
        declared: Option<u32>,
    }

    impl Header {
        fn entries(entries: u32) -> Self {
            Self {
                version: FORMAT_VERSION,
                precision: PRECISION,
                entries,
                declared: None,
            }
        }

        fn encode(&self, payload: &[u8]) -> Vec<u8> {
            let mut out = MAGIC.to_vec();
            out.extend_from_slice(&self.version.to_le_bytes());
            out.push(self.precision);
            out.extend_from_slice(&1i64.to_le_bytes());
            out.extend_from_slice(&self.entries.to_le_bytes());
            let declared = self.declared.unwrap_or(payload.len() as u32);
            out.extend_from_slice(&declared.to_le_bytes());
            out.extend_from_slice(&zstd::bulk::compress(payload, ZSTD_LEVEL).unwrap());
            out
        }
    }

    fn malformed_err(bytes: &[u8]) -> String {
        match HllSketchBlob::from_bytes(bytes) {
            Err(SketchDecodeError::Malformed(msg)) => msg,
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    fn unsupported_err(bytes: &[u8]) -> String {
        match HllSketchBlob::from_bytes(bytes) {
            Err(SketchDecodeError::Unsupported(msg)) => msg,
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn hand_built_payload_decodes() {
        // Guards the rejection tests below: the helpers produce valid input.
        let payload = [
            entry(0, 1, &[(3, 2)], &sparse(&[(4, 57)]), &sparse(&[])),
            entry(
                0,
                2,
                &[],
                &sparse(&[]),
                &[[HLL_DENSE].as_slice(), &[0; REGISTERS]].concat(),
            ),
        ]
        .concat();
        let blob = HllSketchBlob::from_bytes(&Header::entries(2).encode(&payload)).unwrap();
        assert_eq!(blob.entries.len(), 2);
        assert_eq!(blob.entries[0].values_hll.registers()[4], 57);
    }

    #[test]
    fn rejects_every_truncation() {
        let bytes = HllSketchBlob::from_properties(10, &fixture_properties())
            .to_bytes()
            .unwrap();
        for len in 0..bytes.len() {
            assert!(
                HllSketchBlob::from_bytes(&bytes[..len]).is_err(),
                "prefix of {len} bytes decoded"
            );
        }
    }

    #[test]
    fn rejects_truncated_payload() {
        // Long enough to pass the entry-count bound, so truncation is what fails.
        let payload = entry(0, 1, &[(3, 1)], &sparse(&[]), &sparse(&[(4, 1)]));
        let msg = malformed_err(&Header::entries(1).encode(&payload[..payload.len() - 1]));
        assert!(msg.contains("truncated"), "{msg}");
    }

    #[test]
    fn rejects_unknown_leading_bytes() {
        malformed_err(b"");
        malformed_err(b"FHLX\x02\x00");
        malformed_err(b" {\"version\":1}");
        malformed_err(&[0x28, 0xB5, 0x2F, 0xFD]);
    }

    #[test]
    fn unsupported_version_and_precision_are_distinct_from_corruption() {
        let payload = entry(0, 1, &[], &sparse(&[]), &sparse(&[]));
        let mut header = Header::entries(1);
        header.version = 3;
        assert!(unsupported_err(&header.encode(&payload)).contains("version 3"));

        let mut header = Header::entries(1);
        header.precision = 12;
        assert!(unsupported_err(&header.encode(&payload)).contains("precision 12"));

        let json = br#"{"version":99,"index_t":1,"entries":[]}"#;
        assert!(unsupported_err(json).contains("version 99"));
    }

    #[test]
    fn rejects_bad_hll_representations() {
        let bad_tag = [2u8];
        let mut dense_rank = vec![HLL_DENSE];
        dense_rank.extend_from_slice(&[0; REGISTERS]);
        dense_rank[100] = MAX_RANK + 1;
        let cases: [(&[u8], &str); 6] = [
            (&bad_tag, "unknown HLL representation 2"),
            (&dense_rank, "rank 58"),
            (&sparse(&[(4, MAX_RANK + 1)]), "rank 58"),
            (&sparse(&[(4, 0)]), "rank 0"),
            (&sparse(&[(4, 1), (4, 2)]), "out of order"),
            (&sparse(&[(9, 1), (4, 2)]), "out of order"),
        ];
        for (hll, want) in cases {
            // The datatype pads a one-byte HLL past the entry-count bound.
            let payload = entry(0, 1, &[(3, 1)], hll, &sparse(&[]));
            let msg = malformed_err(&Header::entries(1).encode(&payload));
            assert!(msg.contains(want), "{want}: {msg}");
        }
        // A dense HLL with zero registers is valid.
        let mut dense_zero = vec![HLL_DENSE];
        dense_zero.extend_from_slice(&[0; REGISTERS]);
        let payload = entry(0, 1, &[], &dense_zero, &sparse(&[]));
        HllSketchBlob::from_bytes(&Header::entries(1).encode(&payload)).unwrap();
    }

    #[test]
    fn rejects_sparse_count_that_disagrees_with_its_pairs() {
        // Claims one more pair than present: the subjects HLL's tag is
        // consumed as the pair, then the subjects HLL runs out.
        let mut short = sparse(&[(4, 1)]);
        short[1] = 2;
        let payload = entry(0, 1, &[], &short, &sparse(&[]));
        assert!(HllSketchBlob::from_bytes(&Header::entries(1).encode(&payload)).is_err());

        // Claims one fewer: the extra pair is read as the subjects HLL tag.
        let mut long = sparse(&[(4, 1), (5, 1)]);
        long[1] = 1;
        let payload = entry(0, 1, &[], &long, &sparse(&[]));
        assert!(HllSketchBlob::from_bytes(&Header::entries(1).encode(&payload)).is_err());

        let mut huge = sparse(&[]);
        huge[1..3].copy_from_slice(&257u16.to_le_bytes());
        let payload = entry(0, 1, &[], &huge, &sparse(&[]));
        let msg = malformed_err(&Header::entries(1).encode(&payload));
        assert!(msg.contains("claims 257"), "{msg}");
    }

    #[test]
    fn rejects_unsorted_or_duplicate_keys() {
        for (a, b) in [((0, 2), (0, 1)), ((1, 1), (0, 9)), ((0, 1), (0, 1))] {
            let payload = [
                entry(a.0, a.1, &[], &sparse(&[]), &sparse(&[])),
                entry(b.0, b.1, &[], &sparse(&[]), &sparse(&[])),
            ]
            .concat();
            let msg = malformed_err(&Header::entries(2).encode(&payload));
            assert!(msg.contains("out of order"), "{msg}");
        }
    }

    #[test]
    fn rejects_bad_datatypes() {
        for (dts, want) in [
            (vec![(5, 1), (3, 1)], "out of order"),
            (vec![(3, 1), (3, 1)], "out of order"),
            (vec![(3, 0)], "zero count"),
        ] {
            let payload = entry(0, 1, &dts, &sparse(&[]), &sparse(&[]));
            let msg = malformed_err(&Header::entries(1).encode(&payload));
            assert!(msg.contains(want), "{want}: {msg}");
        }
    }

    #[test]
    fn rejects_trailing_bytes() {
        let payload = entry(0, 1, &[], &sparse(&[]), &sparse(&[]));
        let mut padded = payload.clone();
        padded.push(0);
        let msg = malformed_err(&Header::entries(1).encode(&padded));
        assert!(msg.contains("trailing payload"), "{msg}");

        let mut after_frame = Header::entries(1).encode(&payload);
        after_frame.push(0);
        let msg = malformed_err(&after_frame);
        assert!(msg.contains("after the zstd frame"), "{msg}");

        let mut two_frames = Header::entries(1).encode(&payload);
        two_frames.extend(zstd::bulk::compress(&[0u8], ZSTD_LEVEL).unwrap());
        malformed_err(&two_frames);
    }

    #[test]
    fn declared_length_is_checked_not_trusted() {
        let payload = entry(0, 1, &[], &sparse(&[]), &sparse(&[]));
        for declared in [payload.len() as u32 - 1, payload.len() as u32 + 1] {
            let mut header = Header::entries(1);
            header.declared = Some(declared);
            let msg = malformed_err(&header.encode(&payload));
            assert!(msg.contains("header declares"), "{msg}");
        }

        let mut header = Header::entries(1);
        header.declared = Some(MAX_PAYLOAD_BYTES as u32 + 1);
        let msg = malformed_err(&header.encode(&payload));
        assert!(msg.contains("ceiling"), "{msg}");
    }

    #[test]
    fn entry_count_is_checked_before_allocating() {
        let payload = entry(0, 1, &[], &sparse(&[]), &sparse(&[]));
        let msg = malformed_err(&Header::entries(u32::MAX).encode(&payload));
        assert!(msg.contains("cannot fit"), "{msg}");
        // Fewer entries than the payload holds leaves trailing bytes.
        malformed_err(&Header::entries(0).encode(&payload));
    }

    #[test]
    fn rejects_bad_v1_registers() {
        let json = br#"{"version":1,"index_t":1,"entries":[{"g_id":0,"p_id":1,"count":1,"values_hll":"00","subjects_hll":"00","last_modified_t":1}]}"#;
        let msg = malformed_err(json);
        assert!(msg.contains("bad v1 HLL registers"), "{msg}");
    }
}
