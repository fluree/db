//! V2 run record for the FLI3 index format.
//!
//! Replaces the V1 `(o_kind, dt, lang_id)` identity triple with a single
//! `o_type: u16` (`OType`). Dropping `op` from the wire format (import-only:
//! all ops are asserts; `op` only appears in `HistEntryV2`).
//!
//! ## Run wire layout (30 bytes, little-endian, no g_id)
//!
//! ```text
//! s_id:    u64   [0..8]     subject ID (sid64)
//! o_key:   u64   [8..16]    object key payload
//! p_id:    u32   [16..20]   predicate ID
//! t:       u32   [20..24]   transaction number
//! o_i:     u32   [24..28]   list index (u32::MAX = none)
//! o_type:  u16   [28..30]   unified type tag (OType)
//! ```
//!
//! ## Spool wire layout (32 bytes, little-endian, includes g_id)
//!
//! ```text
//! g_id:    u16   [0..2]     graph ID
//! s_id:    u64   [2..10]    (NB: unaligned)
//! o_key:   u64   [10..18]
//! p_id:    u32   [18..22]
//! t:       u32   [22..26]
//! o_i:     u32   [26..30]
//! o_type:  u16   [30..32]
//! ```

use fluree_db_core::o_type::OType;
use fluree_db_core::o_type_registry::OTypeRegistry;
use fluree_db_core::subject_id::SubjectId;
use std::cmp::Ordering;

use super::run_record::{RunRecord, RunSortOrder, LIST_INDEX_NONE};

/// Wire format size of a V2 run record (no g_id, no op).
pub const RECORD_V2_WIRE_SIZE: usize = 30;

/// Wire format size of a V2 run record with op byte (no g_id).
///
/// Used by the rebuild pipeline where retract-winners must be identified.
/// Import uses `RECORD_V2_WIRE_SIZE` (no op, all records are asserts).
pub const RECORD_V2_WITH_OP_WIRE_SIZE: usize = 31;

/// Wire format size of a V2 spool record (includes g_id).
pub const SPOOL_V2_WIRE_SIZE: usize = 32;

/// V2 run record — `o_type` replaces `(o_kind, dt, lang_id)`.
///
/// `op` is omitted from the struct and wire format for the import-only
/// milestone. All records in latest-state indexes are implicitly asserts.
/// `op` appears only in history sidecar entries (`HistEntryV2`).
///
/// ## Sort orders (no `t` or `op` in sort key)
///
/// - **SPOT**: `(s_id, p_id, o_type, o_key, o_i)`
/// - **PSOT**: `(p_id, s_id, o_type, o_key, o_i)`
/// - **POST**: `(p_id, o_type, o_key, o_i, s_id)`
/// - **OPST**: `(o_type, o_key, o_i, p_id, s_id)`
///
/// `t` is a **data column** carried in-memory for merge tie-breaking (higher
/// `t` wins) but is NOT part of the index sort comparator.
#[derive(Copy, Clone, Eq, PartialEq)]
#[repr(C)]
pub struct RunRecordV2 {
    /// Subject ID (sid64: ns_code << 48 | local_id).
    pub s_id: SubjectId,
    /// Object key payload (interpretation depends on `o_type`).
    pub o_key: u64,
    /// Predicate ID (global dictionary).
    pub p_id: u32,
    /// Transaction number (data column, not part of sort key).
    pub t: u32,
    /// List index (`u32::MAX` = not a list member).
    pub o_i: u32,
    /// Unified type tag (OType).
    pub o_type: u16,
    /// Graph ID (0 = default graph). In-memory only; not in run wire format.
    pub g_id: u16,
}

// Verify size: 8 + 8 + 4 + 4 + 4 + 2 + 2 = 32 bytes.
const _: () = assert!(std::mem::size_of::<RunRecordV2>() == 32);

impl RunRecordV2 {
    /// Create a new V2 record with all fields.
    #[inline]
    pub fn new(
        g_id: u16,
        s_id: SubjectId,
        p_id: u32,
        o_type: OType,
        o_key: u64,
        o_i: u32,
        t: u32,
    ) -> Self {
        Self {
            s_id,
            o_key,
            p_id,
            t,
            o_i,
            o_type: o_type.as_u16(),
            g_id,
        }
    }

    /// Convert a V1 `RunRecord` to V2 using the given registry.
    #[inline]
    pub fn from_v1(v1: &RunRecord, registry: &OTypeRegistry) -> Self {
        let o_type = registry.resolve(
            v1.obj_kind(),
            fluree_db_core::DatatypeDictId::from_u16(v1.dt),
            v1.lang_id,
        );
        Self {
            s_id: v1.s_id,
            o_key: v1.o_key,
            p_id: v1.p_id,
            t: v1.t,
            o_i: v1.i,
            o_type: o_type.as_u16(),
            g_id: v1.g_id,
        }
    }

    /// Get the `OType` for this record.
    #[inline]
    pub fn o_type(&self) -> OType {
        OType::from_u16(self.o_type)
    }

    /// True if this record has a list index (i.e., `o_i != LIST_INDEX_NONE`).
    #[inline]
    pub fn has_list_index(&self) -> bool {
        self.o_i != LIST_INDEX_NONE
    }

    // ── Wire format I/O ────────────────────────────────────────────────

    /// Serialize to run wire format (30 bytes, no g_id).
    #[inline]
    pub fn write_run_le(&self, buf: &mut [u8; RECORD_V2_WIRE_SIZE]) {
        buf[0..8].copy_from_slice(&self.s_id.as_u64().to_le_bytes());
        buf[8..16].copy_from_slice(&self.o_key.to_le_bytes());
        buf[16..20].copy_from_slice(&self.p_id.to_le_bytes());
        buf[20..24].copy_from_slice(&self.t.to_le_bytes());
        buf[24..28].copy_from_slice(&self.o_i.to_le_bytes());
        buf[28..30].copy_from_slice(&self.o_type.to_le_bytes());
    }

    /// Deserialize from run wire format (30 bytes, no g_id).
    #[inline]
    pub fn read_run_le(buf: &[u8; RECORD_V2_WIRE_SIZE]) -> Self {
        Self {
            s_id: SubjectId(u64::from_le_bytes(buf[0..8].try_into().unwrap())),
            o_key: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            p_id: u32::from_le_bytes(buf[16..20].try_into().unwrap()),
            t: u32::from_le_bytes(buf[20..24].try_into().unwrap()),
            o_i: u32::from_le_bytes(buf[24..28].try_into().unwrap()),
            o_type: u16::from_le_bytes(buf[28..30].try_into().unwrap()),
            g_id: 0,
        }
    }

    /// Serialize to run wire format with op byte (31 bytes, no g_id).
    ///
    /// Used by the rebuild pipeline's run writer to preserve assert/retract identity.
    #[inline]
    pub fn write_run_le_with_op(&self, op: u8, buf: &mut [u8; RECORD_V2_WITH_OP_WIRE_SIZE]) {
        buf[0..8].copy_from_slice(&self.s_id.as_u64().to_le_bytes());
        buf[8..16].copy_from_slice(&self.o_key.to_le_bytes());
        buf[16..20].copy_from_slice(&self.p_id.to_le_bytes());
        buf[20..24].copy_from_slice(&self.t.to_le_bytes());
        buf[24..28].copy_from_slice(&self.o_i.to_le_bytes());
        buf[28..30].copy_from_slice(&self.o_type.to_le_bytes());
        buf[30] = op;
    }

    /// Deserialize from run wire format with op byte (31 bytes, no g_id).
    #[inline]
    pub fn read_run_le_with_op(buf: &[u8; RECORD_V2_WITH_OP_WIRE_SIZE]) -> (Self, u8) {
        let record = Self {
            s_id: SubjectId(u64::from_le_bytes(buf[0..8].try_into().unwrap())),
            o_key: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            p_id: u32::from_le_bytes(buf[16..20].try_into().unwrap()),
            t: u32::from_le_bytes(buf[20..24].try_into().unwrap()),
            o_i: u32::from_le_bytes(buf[24..28].try_into().unwrap()),
            o_type: u16::from_le_bytes(buf[28..30].try_into().unwrap()),
            g_id: 0,
        };
        (record, buf[30])
    }

    /// Serialize to spool wire format (32 bytes, includes g_id).
    #[inline]
    pub fn write_spool_le(&self, buf: &mut [u8; SPOOL_V2_WIRE_SIZE]) {
        buf[0..2].copy_from_slice(&self.g_id.to_le_bytes());
        buf[2..10].copy_from_slice(&self.s_id.as_u64().to_le_bytes());
        buf[10..18].copy_from_slice(&self.o_key.to_le_bytes());
        buf[18..22].copy_from_slice(&self.p_id.to_le_bytes());
        buf[22..26].copy_from_slice(&self.t.to_le_bytes());
        buf[26..30].copy_from_slice(&self.o_i.to_le_bytes());
        buf[30..32].copy_from_slice(&self.o_type.to_le_bytes());
    }

    /// Deserialize from spool wire format (32 bytes, includes g_id).
    #[inline]
    pub fn read_spool_le(buf: &[u8; SPOOL_V2_WIRE_SIZE]) -> Self {
        Self {
            g_id: u16::from_le_bytes(buf[0..2].try_into().unwrap()),
            s_id: SubjectId(u64::from_le_bytes(buf[2..10].try_into().unwrap())),
            o_key: u64::from_le_bytes(buf[10..18].try_into().unwrap()),
            p_id: u32::from_le_bytes(buf[18..22].try_into().unwrap()),
            t: u32::from_le_bytes(buf[22..26].try_into().unwrap()),
            o_i: u32::from_le_bytes(buf[26..30].try_into().unwrap()),
            o_type: u16::from_le_bytes(buf[30..32].try_into().unwrap()),
        }
    }
}

impl std::fmt::Debug for RunRecordV2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunRecordV2")
            .field("g_id", &self.g_id)
            .field("s_id", &self.s_id)
            .field("p_id", &self.p_id)
            .field("o_type", &self.o_type())
            .field("o_key", &self.o_key)
            .field("o_i", &self.o_i)
            .field("t", &self.t)
            .finish()
    }
}

// ============================================================================
// V2 Sort Comparators
// ============================================================================
//
// Sort key columns ONLY — no `t` or `op` in the comparator.
// `t` is a merge tie-breaker applied separately by the merge engine.

/// V2 SPOT comparator: `(s_id, p_id, o_type, o_key, o_i)`.
#[inline]
pub fn cmp_v2_spot(a: &RunRecordV2, b: &RunRecordV2) -> Ordering {
    a.s_id
        .cmp(&b.s_id)
        .then(a.p_id.cmp(&b.p_id))
        .then(a.o_type.cmp(&b.o_type))
        .then(a.o_key.cmp(&b.o_key))
        .then(a.o_i.cmp(&b.o_i))
}

/// V2 PSOT comparator: `(p_id, s_id, o_type, o_key, o_i)`.
#[inline]
pub fn cmp_v2_psot(a: &RunRecordV2, b: &RunRecordV2) -> Ordering {
    a.p_id
        .cmp(&b.p_id)
        .then(a.s_id.cmp(&b.s_id))
        .then(a.o_type.cmp(&b.o_type))
        .then(a.o_key.cmp(&b.o_key))
        .then(a.o_i.cmp(&b.o_i))
}

/// V2 POST comparator: `(p_id, o_type, o_key, o_i, s_id)`.
#[inline]
pub fn cmp_v2_post(a: &RunRecordV2, b: &RunRecordV2) -> Ordering {
    a.p_id
        .cmp(&b.p_id)
        .then(a.o_type.cmp(&b.o_type))
        .then(a.o_key.cmp(&b.o_key))
        .then(a.o_i.cmp(&b.o_i))
        .then(a.s_id.cmp(&b.s_id))
}

/// V2 OPST comparator: `(o_type, o_key, o_i, p_id, s_id)`.
#[inline]
pub fn cmp_v2_opst(a: &RunRecordV2, b: &RunRecordV2) -> Ordering {
    a.o_type
        .cmp(&b.o_type)
        .then(a.o_key.cmp(&b.o_key))
        .then(a.o_i.cmp(&b.o_i))
        .then(a.p_id.cmp(&b.p_id))
        .then(a.s_id.cmp(&b.s_id))
}

/// V2 graph-prefixed SPOT: `(g_id, s_id, p_id, o_type, o_key, o_i)`.
#[inline]
pub fn cmp_v2_g_spot(a: &RunRecordV2, b: &RunRecordV2) -> Ordering {
    a.g_id.cmp(&b.g_id).then_with(|| cmp_v2_spot(a, b))
}

/// Return the V2 comparator function for a given sort order.
pub fn cmp_v2_for_order(order: RunSortOrder) -> fn(&RunRecordV2, &RunRecordV2) -> Ordering {
    match order {
        RunSortOrder::Spot => cmp_v2_spot,
        RunSortOrder::Psot => cmp_v2_psot,
        RunSortOrder::Post => cmp_v2_post,
        RunSortOrder::Opst => cmp_v2_opst,
    }
}

/// V2 fact identity comparison: `(s_id, p_id, o_type, o_key, o_i)`.
///
/// Unlike V1, identity is always the same five columns with no conditional
/// `lang_id`/`i` logic. `o_type` absorbs `lang_id` and `o_i` always participates.
#[inline]
pub fn same_identity_v2(a: &RunRecordV2, b: &RunRecordV2) -> bool {
    a.s_id == b.s_id
        && a.p_id == b.p_id
        && a.o_type == b.o_type
        && a.o_key == b.o_key
        && a.o_i == b.o_i
}

// ============================================================================
// V2 Ordered Key (for leaf/branch routing)
// ============================================================================

/// Fixed-size 26-byte key for leaf/branch routing.
///
/// The field order matches the physical sort order so comparison is
/// lexicographic on the raw bytes (memcmp). All fields are big-endian
/// for correct byte-wise comparison.
///
/// | Order | Layout |
/// |-------|--------|
/// | SPOT  | `s_id(8) + p_id(4) + o_type(2) + o_key(8) + o_i(4)` |
/// | PSOT  | `p_id(4) + s_id(8) + o_type(2) + o_key(8) + o_i(4)` |
/// | POST  | `p_id(4) + o_type(2) + o_key(8) + o_i(4) + s_id(8)` |
/// | OPST  | `o_type(2) + o_key(8) + o_i(4) + p_id(4) + s_id(8)` |
pub const ORDERED_KEY_V2_SIZE: usize = 26;

/// Write a V2 ordered routing key for the given sort order.
///
/// All fields are written **big-endian** so that `memcmp` on the resulting
/// bytes produces the same ordering as the field-by-field comparator.
#[inline]
pub fn write_ordered_key_v2(
    order: RunSortOrder,
    rec: &RunRecordV2,
    buf: &mut [u8; ORDERED_KEY_V2_SIZE],
) {
    match order {
        RunSortOrder::Spot => {
            buf[0..8].copy_from_slice(&rec.s_id.as_u64().to_be_bytes());
            buf[8..12].copy_from_slice(&rec.p_id.to_be_bytes());
            buf[12..14].copy_from_slice(&rec.o_type.to_be_bytes());
            buf[14..22].copy_from_slice(&rec.o_key.to_be_bytes());
            buf[22..26].copy_from_slice(&rec.o_i.to_be_bytes());
        }
        RunSortOrder::Psot => {
            buf[0..4].copy_from_slice(&rec.p_id.to_be_bytes());
            buf[4..12].copy_from_slice(&rec.s_id.as_u64().to_be_bytes());
            buf[12..14].copy_from_slice(&rec.o_type.to_be_bytes());
            buf[14..22].copy_from_slice(&rec.o_key.to_be_bytes());
            buf[22..26].copy_from_slice(&rec.o_i.to_be_bytes());
        }
        RunSortOrder::Post => {
            buf[0..4].copy_from_slice(&rec.p_id.to_be_bytes());
            buf[4..6].copy_from_slice(&rec.o_type.to_be_bytes());
            buf[6..14].copy_from_slice(&rec.o_key.to_be_bytes());
            buf[14..18].copy_from_slice(&rec.o_i.to_be_bytes());
            buf[18..26].copy_from_slice(&rec.s_id.as_u64().to_be_bytes());
        }
        RunSortOrder::Opst => {
            buf[0..2].copy_from_slice(&rec.o_type.to_be_bytes());
            buf[2..10].copy_from_slice(&rec.o_key.to_be_bytes());
            buf[10..14].copy_from_slice(&rec.o_i.to_be_bytes());
            buf[14..18].copy_from_slice(&rec.p_id.to_be_bytes());
            buf[18..26].copy_from_slice(&rec.s_id.as_u64().to_be_bytes());
        }
    }
}

/// Parse a V2 ordered routing key back into a `RunRecordV2`.
///
/// Inverse of `write_ordered_key_v2`. Fields not stored in the key
/// (`t`, `g_id`) are set to 0.
#[inline]
pub fn read_ordered_key_v2(order: RunSortOrder, buf: &[u8; ORDERED_KEY_V2_SIZE]) -> RunRecordV2 {
    match order {
        RunSortOrder::Spot => RunRecordV2 {
            s_id: SubjectId(u64::from_be_bytes(buf[0..8].try_into().unwrap())),
            p_id: u32::from_be_bytes(buf[8..12].try_into().unwrap()),
            o_type: u16::from_be_bytes(buf[12..14].try_into().unwrap()),
            o_key: u64::from_be_bytes(buf[14..22].try_into().unwrap()),
            o_i: u32::from_be_bytes(buf[22..26].try_into().unwrap()),
            t: 0,
            g_id: 0,
        },
        RunSortOrder::Psot => RunRecordV2 {
            p_id: u32::from_be_bytes(buf[0..4].try_into().unwrap()),
            s_id: SubjectId(u64::from_be_bytes(buf[4..12].try_into().unwrap())),
            o_type: u16::from_be_bytes(buf[12..14].try_into().unwrap()),
            o_key: u64::from_be_bytes(buf[14..22].try_into().unwrap()),
            o_i: u32::from_be_bytes(buf[22..26].try_into().unwrap()),
            t: 0,
            g_id: 0,
        },
        RunSortOrder::Post => RunRecordV2 {
            p_id: u32::from_be_bytes(buf[0..4].try_into().unwrap()),
            o_type: u16::from_be_bytes(buf[4..6].try_into().unwrap()),
            o_key: u64::from_be_bytes(buf[6..14].try_into().unwrap()),
            o_i: u32::from_be_bytes(buf[14..18].try_into().unwrap()),
            s_id: SubjectId(u64::from_be_bytes(buf[18..26].try_into().unwrap())),
            t: 0,
            g_id: 0,
        },
        RunSortOrder::Opst => RunRecordV2 {
            o_type: u16::from_be_bytes(buf[0..2].try_into().unwrap()),
            o_key: u64::from_be_bytes(buf[2..10].try_into().unwrap()),
            o_i: u32::from_be_bytes(buf[10..14].try_into().unwrap()),
            p_id: u32::from_be_bytes(buf[14..18].try_into().unwrap()),
            s_id: SubjectId(u64::from_be_bytes(buf[18..26].try_into().unwrap())),
            t: 0,
            g_id: 0,
        },
    }
}

/// Compare two V2 ordered keys by raw bytes (memcmp).
#[inline]
pub fn cmp_ordered_key_v2(
    a: &[u8; ORDERED_KEY_V2_SIZE],
    b: &[u8; ORDERED_KEY_V2_SIZE],
) -> Ordering {
    a.cmp(b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::o_type::OType;
    use fluree_db_core::subject_id::SubjectId;

    fn make_rec(s_id: u64, p_id: u32, o_type: u16, o_key: u64, o_i: u32, t: u32) -> RunRecordV2 {
        RunRecordV2 {
            s_id: SubjectId(s_id),
            o_key,
            p_id,
            t,
            o_i,
            o_type,
            g_id: 0,
        }
    }

    #[test]
    fn wire_roundtrip_run() {
        let rec = make_rec(
            123_456,
            42,
            OType::XSD_INTEGER.as_u16(),
            999,
            LIST_INDEX_NONE,
            7,
        );
        let mut buf = [0u8; RECORD_V2_WIRE_SIZE];
        rec.write_run_le(&mut buf);
        let rec2 = RunRecordV2::read_run_le(&buf);
        assert_eq!(rec.s_id, rec2.s_id);
        assert_eq!(rec.o_key, rec2.o_key);
        assert_eq!(rec.p_id, rec2.p_id);
        assert_eq!(rec.t, rec2.t);
        assert_eq!(rec.o_i, rec2.o_i);
        assert_eq!(rec.o_type, rec2.o_type);
        assert_eq!(rec2.g_id, 0); // g_id not in run wire
    }

    #[test]
    fn wire_roundtrip_spool() {
        let mut rec = make_rec(123_456, 42, OType::XSD_STRING.as_u16(), 888, 5, 10);
        rec.g_id = 3;
        let mut buf = [0u8; SPOOL_V2_WIRE_SIZE];
        rec.write_spool_le(&mut buf);
        let rec2 = RunRecordV2::read_spool_le(&buf);
        assert_eq!(rec.g_id, rec2.g_id);
        assert_eq!(rec.s_id, rec2.s_id);
        assert_eq!(rec.o_key, rec2.o_key);
        assert_eq!(rec.p_id, rec2.p_id);
        assert_eq!(rec.t, rec2.t);
        assert_eq!(rec.o_i, rec2.o_i);
        assert_eq!(rec.o_type, rec2.o_type);
    }

    #[test]
    fn comparator_spot_order() {
        let a = make_rec(1, 2, 3, 4, 5, 100);
        let b = make_rec(1, 2, 3, 4, 6, 50);
        // Same (s,p,o_type,o_key) but different o_i → o_i breaks tie.
        assert_eq!(cmp_v2_spot(&a, &b), Ordering::Less);
    }

    #[test]
    fn comparator_excludes_t() {
        let a = make_rec(1, 2, 3, 4, 5, 100);
        let b = make_rec(1, 2, 3, 4, 5, 200);
        // All sort fields equal — `t` NOT in comparator → Equal.
        assert_eq!(cmp_v2_spot(&a, &b), Ordering::Equal);
        assert_eq!(cmp_v2_post(&a, &b), Ordering::Equal);
    }

    #[test]
    fn identity_check() {
        let a = make_rec(1, 2, 3, 4, 5, 100);
        let b = make_rec(1, 2, 3, 4, 5, 200);
        assert!(same_identity_v2(&a, &b)); // different t, same identity

        let c = make_rec(1, 2, 3, 4, 6, 100);
        assert!(!same_identity_v2(&a, &c)); // different o_i
    }

    #[test]
    fn ordered_key_spot_memcmp() {
        let a = make_rec(1, 10, 3, 100, LIST_INDEX_NONE, 0);
        let b = make_rec(2, 5, 3, 100, LIST_INDEX_NONE, 0);
        // s_id=1 < s_id=2 → a < b in SPOT
        let mut ka = [0u8; ORDERED_KEY_V2_SIZE];
        let mut kb = [0u8; ORDERED_KEY_V2_SIZE];
        write_ordered_key_v2(RunSortOrder::Spot, &a, &mut ka);
        write_ordered_key_v2(RunSortOrder::Spot, &b, &mut kb);
        assert_eq!(cmp_ordered_key_v2(&ka, &kb), Ordering::Less);
        // Also verify memcmp matches the field comparator.
        assert_eq!(cmp_ordered_key_v2(&ka, &kb), cmp_v2_spot(&a, &b));
    }

    #[test]
    fn ordered_key_post_memcmp() {
        let a = make_rec(100, 1, 3, 50, LIST_INDEX_NONE, 0);
        let b = make_rec(50, 2, 3, 50, LIST_INDEX_NONE, 0);
        // POST: p_id first → p=1 < p=2 → a < b
        let mut ka = [0u8; ORDERED_KEY_V2_SIZE];
        let mut kb = [0u8; ORDERED_KEY_V2_SIZE];
        write_ordered_key_v2(RunSortOrder::Post, &a, &mut ka);
        write_ordered_key_v2(RunSortOrder::Post, &b, &mut kb);
        assert_eq!(cmp_ordered_key_v2(&ka, &kb), cmp_v2_post(&a, &b));
    }

    #[test]
    fn ordered_key_opst_memcmp() {
        let a = make_rec(1, 1, OType::XSD_INTEGER.as_u16(), 42, LIST_INDEX_NONE, 0);
        let b = make_rec(1, 1, OType::XSD_STRING.as_u16(), 42, LIST_INDEX_NONE, 0);
        // OPST: o_type first → INTEGER (0x0003) < STRING (0x8000) → a < b
        let mut ka = [0u8; ORDERED_KEY_V2_SIZE];
        let mut kb = [0u8; ORDERED_KEY_V2_SIZE];
        write_ordered_key_v2(RunSortOrder::Opst, &a, &mut ka);
        write_ordered_key_v2(RunSortOrder::Opst, &b, &mut kb);
        assert_eq!(cmp_ordered_key_v2(&ka, &kb), cmp_v2_opst(&a, &b));
    }

    #[test]
    fn from_v1_conversion() {
        use fluree_db_core::o_type_registry::OTypeRegistry;
        use fluree_db_core::value_id::{ObjKey, ObjKind};

        let registry = OTypeRegistry::builtin_only();
        let v1 = RunRecord::new(
            0,                      // g_id
            SubjectId(100),         // s_id
            5,                      // p_id
            ObjKind::NUM_INT,       // o_kind
            ObjKey::encode_i64(42), // o_key
            7,                      // t
            true,                   // op
            3,                      // dt (INTEGER)
            0,                      // lang_id
            None,                   // i (no list index)
        );

        let v2 = RunRecordV2::from_v1(&v1, &registry);
        assert_eq!(v2.s_id, v1.s_id);
        assert_eq!(v2.o_key, v1.o_key);
        assert_eq!(v2.p_id, v1.p_id);
        assert_eq!(v2.t, v1.t);
        assert_eq!(v2.o_i, v1.i);
        assert_eq!(v2.o_type(), OType::XSD_INTEGER);
        assert_eq!(v2.g_id, v1.g_id);
    }
}
