//! 40-byte fixed-width run record for external sort.
//!
//! Each record represents a single resolved op with global IDs.
//! The format is `#[repr(C)]` for direct binary I/O.
//!
//! `g_id` is NOT stored in the run wire format (34 bytes) because each graph
//! gets its own set of run files and indexes. The spool wire format (36 bytes)
//! still includes `g_id` as `u16` because spool files are pre-partition.

use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::value_id::{ObjKey, ObjKind};
use fluree_db_core::{DatatypeDictId, LangId, ListIndex, ObjPair, PredicateId};
use std::cmp::Ordering;

/// Wire format size of a single RunRecord in **run files**, in bytes.
///
/// Does NOT include `g_id` (graph is implicit from the file path).
/// The in-memory struct is 40 bytes due to `#[repr(C)]` alignment.
pub const RECORD_WIRE_SIZE: usize = 34;

/// Wire format size of a single record in **spool files**, in bytes.
///
/// Includes `g_id` as `u16` (spool records are pre-graph-partition).
pub const SPOOL_RECORD_WIRE_SIZE: usize = 36;

/// Sentinel value for "not a list member" in the `u32` `i` field.
pub const LIST_INDEX_NONE: u32 = u32::MAX;

/// 40-byte fixed-width record for external sort.
///
/// Sort key: `(s_id, p_id, o_kind, o_key, dt, t, op)` where the prefix depends
/// on the index order (SPOT, PSOT, POST, OPST). `g_id` is NOT part of the sort
/// key — each graph is indexed independently.
///
/// `dt` is included in the sort key so that values with the same `(ObjKind,
/// ObjKey)` but different XSD types (e.g., `xsd:integer 3` vs `xsd:long 3`)
/// remain distinguishable.
///
/// ## Run wire layout (34 bytes, little-endian, no g_id)
///
/// ```text
/// s_id:    u64   [0..8]     subject ID (sid64: ns_code << 48 | local_id)
/// o_key:   u64   [8..16]    object key payload
/// p_id:    u32   [16..20]
/// t:       u32   [20..24]
/// i:       u32   [24..28]   list index (u32::MAX = none)
/// dt:      u16   [28..30]   datatype dict index (tie-breaker)
/// lang_id: u16   [30..32]   language tag id (0 = none)
/// o_kind:  u8    [32]       object kind discriminant
/// op:      u8    [33]       assert (1) / retract (0)
/// ```
///
/// ## Spool wire layout (36 bytes, little-endian, includes g_id)
///
/// ```text
/// g_id:    u16   [0..2]     graph ID
/// s_id:    u64   [2..10]    (NB: unaligned)
/// o_key:   u64   [10..18]
/// p_id:    u32   [18..22]
/// t:       u32   [22..26]
/// i:       u32   [26..30]
/// dt:      u16   [30..32]
/// lang_id: u16   [32..34]
/// o_kind:  u8    [34]
/// op:      u8    [35]
/// ```
#[derive(Copy, Clone, Eq, PartialEq)]
#[repr(C)]
pub struct RunRecord {
    /// Subject ID (sid64: ns_code << 48 | local_id).
    pub s_id: SubjectId,
    /// Object key payload (interpretation depends on `o_kind`).
    pub o_key: u64,
    /// Predicate ID (global dictionary).
    pub p_id: u32,
    /// Transaction number (non-negative, fits in u32).
    pub t: u32,
    /// List index (u32::MAX = none).
    pub i: u32,
    /// Graph ID (0 = default graph). In-memory only; not in run wire format.
    pub g_id: u16,
    /// Datatype dict index (for sort-key tie-breaking).
    pub dt: u16,
    /// Language tag id (per-run assignment, 0 = none).
    pub lang_id: u16,
    /// Object kind discriminant (see `ObjKind`).
    pub o_kind: u8,
    /// Assert (1) or retract (0).
    pub op: u8,
}

const _: () = assert!(std::mem::size_of::<RunRecord>() == 40);

impl RunRecord {
    /// Create a new RunRecord with all fields.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        g_id: u16,
        s_id: SubjectId,
        p_id: u32,
        o_kind: ObjKind,
        o_key: ObjKey,
        t: u32,
        op: bool,
        dt: u16,
        lang_id: u16,
        i: Option<u32>,
    ) -> Self {
        Self {
            s_id,
            o_key: o_key.as_u64(),
            p_id,
            t,
            i: i.unwrap_or(LIST_INDEX_NONE),
            g_id,
            dt,
            lang_id,
            o_kind: o_kind.as_u8(),
            op: op as u8,
        }
    }

    /// Serialize to [`RECORD_WIRE_SIZE`] (34) bytes, little-endian.
    ///
    /// Does NOT include `g_id` — run files are graph-scoped.
    pub fn write_le(&self, buf: &mut [u8; RECORD_WIRE_SIZE]) {
        buf[0..8].copy_from_slice(&self.s_id.as_u64().to_le_bytes());
        buf[8..16].copy_from_slice(&self.o_key.to_le_bytes());
        buf[16..20].copy_from_slice(&self.p_id.to_le_bytes());
        buf[20..24].copy_from_slice(&self.t.to_le_bytes());
        buf[24..28].copy_from_slice(&self.i.to_le_bytes());
        buf[28..30].copy_from_slice(&self.dt.to_le_bytes());
        buf[30..32].copy_from_slice(&self.lang_id.to_le_bytes());
        buf[32] = self.o_kind;
        buf[33] = self.op;
    }

    /// Deserialize from [`RECORD_WIRE_SIZE`] (34) bytes, little-endian.
    ///
    /// `g_id` is set to 0; caller should set it from run file header context.
    pub fn read_le(buf: &[u8; RECORD_WIRE_SIZE]) -> Self {
        Self {
            s_id: SubjectId::from_u64(u64::from_le_bytes(buf[0..8].try_into().unwrap())),
            o_key: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            p_id: u32::from_le_bytes(buf[16..20].try_into().unwrap()),
            t: u32::from_le_bytes(buf[20..24].try_into().unwrap()),
            i: u32::from_le_bytes(buf[24..28].try_into().unwrap()),
            g_id: 0,
            dt: u16::from_le_bytes(buf[28..30].try_into().unwrap()),
            lang_id: u16::from_le_bytes(buf[30..32].try_into().unwrap()),
            o_kind: buf[32],
            op: buf[33],
        }
    }

    /// Serialize to [`SPOOL_RECORD_WIRE_SIZE`] (36) bytes, little-endian.
    ///
    /// Includes `g_id` as `u16` — spool records are pre-graph-partition.
    pub fn write_spool_le(&self, buf: &mut [u8; SPOOL_RECORD_WIRE_SIZE]) {
        buf[0..2].copy_from_slice(&self.g_id.to_le_bytes());
        buf[2..10].copy_from_slice(&self.s_id.as_u64().to_le_bytes());
        buf[10..18].copy_from_slice(&self.o_key.to_le_bytes());
        buf[18..22].copy_from_slice(&self.p_id.to_le_bytes());
        buf[22..26].copy_from_slice(&self.t.to_le_bytes());
        buf[26..30].copy_from_slice(&self.i.to_le_bytes());
        buf[30..32].copy_from_slice(&self.dt.to_le_bytes());
        buf[32..34].copy_from_slice(&self.lang_id.to_le_bytes());
        buf[34] = self.o_kind;
        buf[35] = self.op;
    }

    /// Deserialize from [`SPOOL_RECORD_WIRE_SIZE`] (36) bytes, little-endian.
    ///
    /// Includes `g_id` as `u16`.
    pub fn read_spool_le(buf: &[u8; SPOOL_RECORD_WIRE_SIZE]) -> Self {
        Self {
            g_id: u16::from_le_bytes(buf[0..2].try_into().unwrap()),
            s_id: SubjectId::from_u64(u64::from_le_bytes(buf[2..10].try_into().unwrap())),
            o_key: u64::from_le_bytes(buf[10..18].try_into().unwrap()),
            p_id: u32::from_le_bytes(buf[18..22].try_into().unwrap()),
            t: u32::from_le_bytes(buf[22..26].try_into().unwrap()),
            i: u32::from_le_bytes(buf[26..30].try_into().unwrap()),
            dt: u16::from_le_bytes(buf[30..32].try_into().unwrap()),
            lang_id: u16::from_le_bytes(buf[32..34].try_into().unwrap()),
            o_kind: buf[34],
            op: buf[35],
        }
    }

    /// Get the object kind as an `ObjKind`.
    #[inline]
    pub fn obj_kind(&self) -> ObjKind {
        ObjKind::from_u8(self.o_kind)
    }

    /// Get the object key as an `ObjKey`.
    #[inline]
    pub fn obj_key(&self) -> ObjKey {
        ObjKey::from_u64(self.o_key)
    }
}

impl std::fmt::Debug for RunRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let i_display = if self.i == LIST_INDEX_NONE {
            "None".to_string()
        } else {
            self.i.to_string()
        };
        f.debug_struct("RunRecord")
            .field("g_id", &self.g_id)
            .field("s_id", &self.s_id)
            .field("p_id", &self.p_id)
            .field("o_kind", &ObjKind::from_u8(self.o_kind))
            .field("o_key", &ObjKey::from_u64(self.o_key))
            .field("t", &self.t)
            .field("op", &(self.op != 0))
            .field("dt", &self.dt)
            .field("lang_id", &self.lang_id)
            .field("i", &i_display)
            .finish()
    }
}

// ============================================================================
// Comparators
// ============================================================================

/// SPOT comparator: `(s_id, p_id, o_kind, o_key, dt, t, op)`.
///
/// `g_id` is NOT part of the sort key — each graph is indexed independently.
#[inline]
pub fn cmp_spot(a: &RunRecord, b: &RunRecord) -> Ordering {
    a.s_id
        .cmp(&b.s_id)
        .then(a.p_id.cmp(&b.p_id))
        .then(a.o_kind.cmp(&b.o_kind))
        .then(a.o_key.cmp(&b.o_key))
        .then(a.dt.cmp(&b.dt))
        .then(a.t.cmp(&b.t))
        .then(a.op.cmp(&b.op))
}

/// PSOT comparator: `(p_id, s_id, o_kind, o_key, dt, t, op)`.
#[inline]
pub fn cmp_psot(a: &RunRecord, b: &RunRecord) -> Ordering {
    a.p_id
        .cmp(&b.p_id)
        .then(a.s_id.cmp(&b.s_id))
        .then(a.o_kind.cmp(&b.o_kind))
        .then(a.o_key.cmp(&b.o_key))
        .then(a.dt.cmp(&b.dt))
        .then(a.t.cmp(&b.t))
        .then(a.op.cmp(&b.op))
}

/// POST comparator: `(p_id, o_kind, o_key, dt, s_id, t, op)`.
#[inline]
pub fn cmp_post(a: &RunRecord, b: &RunRecord) -> Ordering {
    a.p_id
        .cmp(&b.p_id)
        .then(a.o_kind.cmp(&b.o_kind))
        .then(a.o_key.cmp(&b.o_key))
        .then(a.dt.cmp(&b.dt))
        .then(a.s_id.cmp(&b.s_id))
        .then(a.t.cmp(&b.t))
        .then(a.op.cmp(&b.op))
}

/// OPST comparator: `(o_kind, o_key, dt, p_id, s_id, t, op)`.
#[inline]
pub fn cmp_opst(a: &RunRecord, b: &RunRecord) -> Ordering {
    a.o_kind
        .cmp(&b.o_kind)
        .then(a.o_key.cmp(&b.o_key))
        .then(a.dt.cmp(&b.dt))
        .then(a.p_id.cmp(&b.p_id))
        .then(a.s_id.cmp(&b.s_id))
        .then(a.t.cmp(&b.t))
        .then(a.op.cmp(&b.op))
}

/// Comparator: `(g_id, SPOT)`. Used for sorted commit files where records
/// from multiple graphs need to be partitioned then SPOT-sorted within each.
#[inline]
pub fn cmp_g_spot(a: &RunRecord, b: &RunRecord) -> Ordering {
    a.g_id.cmp(&b.g_id).then_with(|| cmp_spot(a, b))
}

/// Return the comparator function for a given sort order.
pub fn cmp_for_order(order: RunSortOrder) -> fn(&RunRecord, &RunRecord) -> Ordering {
    match order {
        RunSortOrder::Spot => cmp_spot,
        RunSortOrder::Psot => cmp_psot,
        RunSortOrder::Post => cmp_post,
        RunSortOrder::Opst => cmp_opst,
    }
}

/// Sort order identifier for run files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RunSortOrder {
    Spot = 0,
    Psot = 1,
    Post = 2,
    Opst = 3,
}

impl PartialOrd for RunSortOrder {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RunSortOrder {
    /// Ordering is based on the canonical wire ID, not variant declaration order.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.to_wire_id().cmp(&other.to_wire_id())
    }
}

impl RunSortOrder {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Spot),
            1 => Some(Self::Psot),
            2 => Some(Self::Post),
            3 => Some(Self::Opst),
            _ => None,
        }
    }

    /// Canonical wire ID for binary index formats (branch headers, root routing).
    ///
    /// Single source of truth: 0=SPOT, 1=PSOT, 2=POST, 3=OPST.
    /// All encoders/decoders in FBR3 and FIR6 must use this mapping.
    #[inline]
    pub fn to_wire_id(self) -> u8 {
        self as u8
    }

    /// Parse from canonical wire ID. Returns `None` for unknown IDs.
    #[inline]
    pub fn from_wire_id(v: u8) -> Option<Self> {
        Self::from_u8(v)
    }

    /// Directory name for this sort order (e.g., `"spot"`, `"psot"`).
    pub fn dir_name(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Psot => "psot",
            Self::Post => "post",
            Self::Opst => "opst",
        }
    }

    /// Parse a sort order from its directory name.
    pub fn from_dir_name(name: &str) -> Option<Self> {
        match name {
            "spot" => Some(Self::Spot),
            "psot" => Some(Self::Psot),
            "post" => Some(Self::Post),
            "opst" => Some(Self::Opst),
            _ => None,
        }
    }

    /// All orders that should be built during index generation.
    pub fn all_build_orders() -> &'static [RunSortOrder] {
        &[Self::Spot, Self::Psot, Self::Post, Self::Opst]
    }

    /// Secondary orders (all except SPOT). Used when SPOT is built separately
    /// from sorted commit files via streaming k-way merge.
    pub fn secondary_orders() -> &'static [RunSortOrder] {
        &[Self::Psot, Self::Post, Self::Opst]
    }
}

// ============================================================================
// FactKey — identity key for dedup during replay and merge
// ============================================================================

/// Fact identity key used for deduplication during replay and novelty merge.
///
/// Mirrors the identity semantics of [`same_identity()`](super::merge::same_identity):
/// `(s_id, p_id, o_kind, o_key, dt, effective_lang_id, i)`.
///
/// `lang_id` is forced to 0 unless `dt == LANG_STRING`. `i` participates as-is
/// (with `ListIndex::none()` sentinel for non-list facts).
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct FactKey {
    pub s_id: SubjectId,
    pub p_id: PredicateId,
    pub o: ObjPair,
    pub dt: DatatypeDictId,
    /// Effective lang_id: `LangId::none()` unless `dt == LANG_STRING`.
    pub lang_id: LangId,
    /// List index (`ListIndex::none()` for non-list facts).
    pub i: ListIndex,
}

impl FactKey {
    /// Build a FactKey from decoded Region 1+2 row data.
    ///
    /// `dt_raw` is `u32` (Region 2 decode output); truncated to `u16` here.
    pub fn from_decoded_row(
        s_id: u64,
        p_id: u32,
        o_kind: u8,
        o_key: u64,
        dt_raw: u32,
        lang_id: u16,
        i: i32,
    ) -> Self {
        let dt = DatatypeDictId::from_u16(dt_raw as u16);
        let effective_lang_id = if dt == DatatypeDictId::LANG_STRING {
            LangId::from_u16(lang_id)
        } else {
            LangId::none()
        };
        Self {
            s_id: SubjectId::from_u64(s_id),
            p_id: PredicateId::from_u32(p_id),
            o: ObjPair::new(ObjKind::from_u8(o_kind), ObjKey::from_u64(o_key)),
            dt,
            lang_id: effective_lang_id,
            i: ListIndex::from_i32(i),
        }
    }

    /// Build a FactKey from a RunRecord.
    pub fn from_run_record(r: &RunRecord) -> Self {
        let dt = DatatypeDictId::from_u16(r.dt);
        let effective_lang_id = if dt == DatatypeDictId::LANG_STRING {
            LangId::from_u16(r.lang_id)
        } else {
            LangId::none()
        };
        let i = if r.i == LIST_INDEX_NONE {
            ListIndex::none()
        } else {
            ListIndex::from_i32(r.i as i32)
        };
        Self {
            s_id: r.s_id,
            p_id: PredicateId::from_u32(r.p_id),
            o: ObjPair::new(ObjKind::from_u8(r.o_kind), ObjKey::from_u64(r.o_key)),
            dt,
            lang_id: effective_lang_id,
            i,
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_record(s_id: u64, p_id: u32, o_int: i64, dt: u16, t: u32) -> RunRecord {
        RunRecord::new(
            0,
            SubjectId::from_u64(s_id),
            p_id,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(o_int),
            t,
            true,
            dt,
            0,
            None,
        )
    }

    #[test]
    fn test_record_size() {
        assert_eq!(std::mem::size_of::<RunRecord>(), 40);
    }

    #[test]
    fn test_serialization_round_trip() {
        let rec = RunRecord::new(
            1,
            SubjectId::from_u64(42),
            7,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(-100),
            5,
            true,
            DatatypeDictId::LONG.as_u16(),
            3,
            Some(2),
        );

        // Run wire format (34 bytes) — does NOT include g_id
        let mut buf = [0u8; RECORD_WIRE_SIZE];
        rec.write_le(&mut buf);
        let restored = RunRecord::read_le(&buf);

        assert_eq!(restored.g_id, 0); // g_id not in run wire; defaults to 0
        assert_eq!(rec.s_id, restored.s_id);
        assert_eq!(rec.p_id, restored.p_id);
        assert_eq!(rec.o_kind, restored.o_kind);
        assert_eq!(rec.o_key, restored.o_key);
        assert_eq!(rec.t, restored.t);
        assert_eq!(rec.op, restored.op);
        assert_eq!(rec.dt, restored.dt);
        assert_eq!(rec.lang_id, restored.lang_id);
        assert_eq!(rec.i, restored.i);

        // Spool wire format (36 bytes) — includes g_id
        let mut sbuf = [0u8; SPOOL_RECORD_WIRE_SIZE];
        rec.write_spool_le(&mut sbuf);
        let spool_restored = RunRecord::read_spool_le(&sbuf);

        assert_eq!(rec.g_id, spool_restored.g_id);
        assert_eq!(rec.s_id, spool_restored.s_id);
        assert_eq!(rec.p_id, spool_restored.p_id);
        assert_eq!(rec.o_kind, spool_restored.o_kind);
        assert_eq!(rec.o_key, spool_restored.o_key);
        assert_eq!(rec.t, spool_restored.t);
        assert_eq!(rec.op, spool_restored.op);
        assert_eq!(rec.dt, spool_restored.dt);
        assert_eq!(rec.lang_id, spool_restored.lang_id);
        assert_eq!(rec.i, spool_restored.i);
    }

    #[test]
    fn test_spot_ordering_by_subject() {
        let a = make_record(1, 1, 0, DatatypeDictId::INTEGER.as_u16(), 1);
        let b = make_record(2, 1, 0, DatatypeDictId::INTEGER.as_u16(), 1);
        assert_eq!(cmp_spot(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_spot_ordering_by_predicate() {
        let a = make_record(1, 1, 0, DatatypeDictId::INTEGER.as_u16(), 1);
        let b = make_record(1, 2, 0, DatatypeDictId::INTEGER.as_u16(), 1);
        assert_eq!(cmp_spot(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_spot_ordering_by_object() {
        let a = make_record(1, 1, 10, DatatypeDictId::INTEGER.as_u16(), 1);
        let b = make_record(1, 1, 20, DatatypeDictId::INTEGER.as_u16(), 1);
        assert_eq!(cmp_spot(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_spot_ordering_by_dt_tiebreak() {
        let a = RunRecord::new(
            0,
            SubjectId::from_u64(1),
            1,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(3),
            1,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        );
        let b = RunRecord::new(
            0,
            SubjectId::from_u64(1),
            1,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(3),
            1,
            true,
            DatatypeDictId::LONG.as_u16(),
            0,
            None,
        );
        assert_eq!(cmp_spot(&a, &b), Ordering::Less);
        assert_ne!(a, b);
    }

    #[test]
    fn test_spot_ordering_by_t() {
        let a = make_record(1, 1, 0, DatatypeDictId::INTEGER.as_u16(), 1);
        let b = make_record(1, 1, 0, DatatypeDictId::INTEGER.as_u16(), 2);
        assert_eq!(cmp_spot(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_spot_ordering_by_op() {
        let a = RunRecord::new(
            0,
            SubjectId::from_u64(1),
            1,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(0),
            1,
            false,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        );
        let b = RunRecord::new(
            0,
            SubjectId::from_u64(1),
            1,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(0),
            1,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        );
        assert_eq!(cmp_spot(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_spot_ordering_ignores_graph() {
        // g_id is NOT part of the sort key (each graph is indexed independently).
        let a = RunRecord::new(
            0,
            SubjectId::from_u64(1),
            1,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(0),
            1,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        );
        let b = RunRecord::new(
            1,
            SubjectId::from_u64(1),
            1,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(0),
            1,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        );
        assert_eq!(cmp_spot(&a, &b), Ordering::Equal);
    }

    #[test]
    fn test_sort_unstable_by_spot() {
        let mut records = [
            make_record(3, 1, 0, DatatypeDictId::INTEGER.as_u16(), 1),
            make_record(1, 2, 0, DatatypeDictId::INTEGER.as_u16(), 1),
            make_record(1, 1, 0, DatatypeDictId::INTEGER.as_u16(), 1),
            make_record(2, 1, 0, DatatypeDictId::INTEGER.as_u16(), 1),
            make_record(1, 1, 10, DatatypeDictId::INTEGER.as_u16(), 1),
        ];
        records.sort_unstable_by(cmp_spot);

        assert_eq!(records[0].s_id, SubjectId::from_u64(1));
        assert_eq!(records[0].p_id, 1);
        assert_eq!(records[0].o_key, ObjKey::encode_i64(0).as_u64());
        assert_eq!(records[1].s_id, SubjectId::from_u64(1));
        assert_eq!(records[1].p_id, 1);
        assert_eq!(records[1].o_key, ObjKey::encode_i64(10).as_u64());
        assert_eq!(records[2].s_id, SubjectId::from_u64(1));
        assert_eq!(records[2].p_id, 2);
        assert_eq!(records[3].s_id, SubjectId::from_u64(2));
        assert_eq!(records[4].s_id, SubjectId::from_u64(3));
    }

    #[test]
    fn test_no_list_index_sentinel() {
        let rec = RunRecord::new(
            0,
            SubjectId::from_u64(1),
            1,
            ObjKind::NULL,
            ObjKey::ZERO,
            1,
            true,
            DatatypeDictId::STRING.as_u16(),
            0,
            None,
        );
        assert_eq!(rec.i, LIST_INDEX_NONE);

        let rec2 = RunRecord::new(
            0,
            SubjectId::from_u64(1),
            1,
            ObjKind::NULL,
            ObjKey::ZERO,
            1,
            true,
            DatatypeDictId::STRING.as_u16(),
            0,
            Some(5),
        );
        assert_eq!(rec2.i, 5);
    }

    #[test]
    fn test_run_sort_order_round_trip() {
        assert_eq!(RunSortOrder::from_u8(0), Some(RunSortOrder::Spot));
        assert_eq!(RunSortOrder::from_u8(1), Some(RunSortOrder::Psot));
        assert_eq!(RunSortOrder::from_u8(2), Some(RunSortOrder::Post));
        assert_eq!(RunSortOrder::from_u8(3), Some(RunSortOrder::Opst));
        assert_eq!(RunSortOrder::from_u8(4), None);
        assert_eq!(RunSortOrder::from_u8(255), None);
    }

    #[test]
    fn test_sort_order_dir_names() {
        assert_eq!(RunSortOrder::Spot.dir_name(), "spot");
        assert_eq!(RunSortOrder::Psot.dir_name(), "psot");
        assert_eq!(RunSortOrder::Post.dir_name(), "post");
        assert_eq!(RunSortOrder::Opst.dir_name(), "opst");
    }

    #[test]
    fn test_all_build_orders() {
        let orders = RunSortOrder::all_build_orders();
        assert_eq!(orders.len(), 4);
        assert_eq!(orders[0], RunSortOrder::Spot);
        assert_eq!(orders[1], RunSortOrder::Psot);
        assert_eq!(orders[2], RunSortOrder::Post);
        assert_eq!(orders[3], RunSortOrder::Opst);
    }

    // ---- PSOT comparator tests ----

    #[test]
    fn test_psot_ordering_predicate_first() {
        let a = make_record(2, 1, 0, DatatypeDictId::INTEGER.as_u16(), 1);
        let b = make_record(1, 2, 0, DatatypeDictId::INTEGER.as_u16(), 1);
        assert_eq!(cmp_psot(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_psot_ordering_subject_within_predicate() {
        let a = make_record(1, 5, 0, DatatypeDictId::INTEGER.as_u16(), 1);
        let b = make_record(2, 5, 0, DatatypeDictId::INTEGER.as_u16(), 1);
        assert_eq!(cmp_psot(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_psot_ordering_object() {
        let a = make_record(1, 5, 10, DatatypeDictId::INTEGER.as_u16(), 1);
        let b = make_record(1, 5, 20, DatatypeDictId::INTEGER.as_u16(), 1);
        assert_eq!(cmp_psot(&a, &b), Ordering::Less);
    }

    // ---- POST comparator tests ----

    #[test]
    fn test_post_ordering_predicate_first() {
        let a = make_record(2, 1, 100, DatatypeDictId::INTEGER.as_u16(), 1);
        let b = make_record(1, 2, 0, DatatypeDictId::INTEGER.as_u16(), 1);
        assert_eq!(cmp_post(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_post_ordering_object_before_subject() {
        let a = make_record(2, 5, 10, DatatypeDictId::INTEGER.as_u16(), 1);
        let b = make_record(1, 5, 20, DatatypeDictId::INTEGER.as_u16(), 1);
        assert_eq!(cmp_post(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_post_ordering_dt_before_subject() {
        let a = RunRecord::new(
            0,
            SubjectId::from_u64(2),
            5,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(10),
            1,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        );
        let b = RunRecord::new(
            0,
            SubjectId::from_u64(1),
            5,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(10),
            1,
            true,
            DatatypeDictId::LONG.as_u16(),
            0,
            None,
        );
        assert_eq!(cmp_post(&a, &b), Ordering::Less);
    }

    // ---- OPST comparator tests ----

    #[test]
    fn test_opst_ordering_object_first() {
        let a = make_record(10, 10, 1, DatatypeDictId::INTEGER.as_u16(), 1);
        let b = make_record(1, 1, 2, DatatypeDictId::INTEGER.as_u16(), 1);
        assert_eq!(cmp_opst(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_opst_ordering_dt_before_predicate() {
        let a = RunRecord::new(
            0,
            SubjectId::from_u64(10),
            10,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(5),
            1,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        );
        let b = RunRecord::new(
            0,
            SubjectId::from_u64(1),
            1,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(5),
            1,
            true,
            DatatypeDictId::LONG.as_u16(),
            0,
            None,
        );
        assert_eq!(cmp_opst(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_opst_ordering_predicate_before_subject() {
        let a = make_record(10, 1, 5, DatatypeDictId::INTEGER.as_u16(), 1);
        let b = make_record(1, 2, 5, DatatypeDictId::INTEGER.as_u16(), 1);
        assert_eq!(cmp_opst(&a, &b), Ordering::Less);
    }

    // ---- cmp_for_order dispatch ----

    #[test]
    fn test_cmp_for_order_dispatches_correctly() {
        let a = make_record(2, 1, 0, DatatypeDictId::INTEGER.as_u16(), 1);
        let b = make_record(1, 2, 0, DatatypeDictId::INTEGER.as_u16(), 1);
        assert_eq!(cmp_for_order(RunSortOrder::Spot)(&a, &b), Ordering::Greater);
        assert_eq!(cmp_for_order(RunSortOrder::Psot)(&a, &b), Ordering::Less);
    }

    // ---- Cross-kind ordering in comparators ----

    #[test]
    fn test_spot_ordering_cross_kind() {
        // NumInt should sort before NumF64 (0x03 < 0x04)
        let int_rec = RunRecord::new(
            0,
            SubjectId::from_u64(1),
            1,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(100),
            1,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        );
        let f64_rec = RunRecord::new(
            0,
            SubjectId::from_u64(1),
            1,
            ObjKind::NUM_F64,
            ObjKey::encode_f64(0.001).unwrap(),
            1,
            true,
            DatatypeDictId::DOUBLE.as_u16(),
            0,
            None,
        );
        assert_eq!(cmp_spot(&int_rec, &f64_rec), Ordering::Less);
    }

    // ---- FactKey tests ----

    #[test]
    fn test_fact_key_same_identity() {
        let a = FactKey::from_run_record(&RunRecord::new(
            0,
            SubjectId::from_u64(10),
            5,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(42),
            1,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        ));
        let b = FactKey::from_run_record(&RunRecord::new(
            0,
            SubjectId::from_u64(10),
            5,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(42),
            99,
            false,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        ));
        assert_eq!(a, b);
    }

    #[test]
    fn test_fact_key_different_subject() {
        let a = FactKey::from_run_record(&RunRecord::new(
            0,
            SubjectId::from_u64(10),
            5,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(42),
            1,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        ));
        let b = FactKey::from_run_record(&RunRecord::new(
            0,
            SubjectId::from_u64(11),
            5,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(42),
            1,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        ));
        assert_ne!(a, b);
    }

    #[test]
    fn test_fact_key_different_dt() {
        let a = FactKey::from_run_record(&RunRecord::new(
            0,
            SubjectId::from_u64(10),
            5,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(42),
            1,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        ));
        let b = FactKey::from_run_record(&RunRecord::new(
            0,
            SubjectId::from_u64(10),
            5,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(42),
            1,
            true,
            DatatypeDictId::LONG.as_u16(),
            0,
            None,
        ));
        assert_ne!(a, b);
    }

    #[test]
    fn test_fact_key_lang_effective() {
        let a = FactKey::from_run_record(&RunRecord::new(
            0,
            SubjectId::from_u64(1),
            1,
            ObjKind::LEX_ID,
            ObjKey::encode_u32_id(5),
            1,
            true,
            DatatypeDictId::LANG_STRING.as_u16(),
            3,
            None,
        ));
        let b = FactKey::from_run_record(&RunRecord::new(
            0,
            SubjectId::from_u64(1),
            1,
            ObjKind::LEX_ID,
            ObjKey::encode_u32_id(5),
            1,
            true,
            DatatypeDictId::LANG_STRING.as_u16(),
            4,
            None,
        ));
        assert_ne!(a, b);

        let c = FactKey::from_run_record(&RunRecord::new(
            0,
            SubjectId::from_u64(1),
            1,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(42),
            1,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            3,
            None,
        ));
        let d = FactKey::from_run_record(&RunRecord::new(
            0,
            SubjectId::from_u64(1),
            1,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(42),
            1,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            99,
            None,
        ));
        assert_eq!(c, d);
        assert_eq!(c.lang_id, LangId::none());
    }

    #[test]
    fn test_fact_key_list_index() {
        let a = FactKey::from_run_record(&RunRecord::new(
            0,
            SubjectId::from_u64(1),
            1,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(42),
            1,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            Some(0),
        ));
        let b = FactKey::from_run_record(&RunRecord::new(
            0,
            SubjectId::from_u64(1),
            1,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(42),
            1,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            Some(1),
        ));
        assert_ne!(a, b);

        let c = FactKey::from_run_record(&RunRecord::new(
            0,
            SubjectId::from_u64(1),
            1,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(42),
            1,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        ));
        let d = FactKey::from_run_record(&RunRecord::new(
            0,
            SubjectId::from_u64(1),
            1,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(42),
            1,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            Some(0),
        ));
        assert_ne!(c, d);
    }

    #[test]
    fn test_fact_key_from_decoded_row() {
        let key = FactKey::from_decoded_row(
            10,
            5,
            ObjKind::NUM_INT.as_u8(),
            ObjKey::encode_i64(42).as_u64(),
            DatatypeDictId::INTEGER.as_u16() as u32,
            0,
            ListIndex::none().as_i32(),
        );
        let from_record = FactKey::from_run_record(&RunRecord::new(
            0,
            SubjectId::from_u64(10),
            5,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(42),
            1,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        ));
        assert_eq!(key, from_record);
    }
}
