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

/// Wire format size of a V2 spool record (includes a `u32` g_id).
pub const SPOOL_V2_WIRE_SIZE: usize = 34;

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
    /// Graph ID (0 = default graph). Only pooled run files carry it.
    pub g_id: fluree_db_core::GraphId,
}

// Verify size: 8 + 8 + 4 + 4 + 4 + 2 + 4, padded to 40 bytes.
const _: () = assert!(std::mem::size_of::<RunRecordV2>() == 40);

impl RunRecordV2 {
    /// Create a new V2 record with all fields.
    #[inline]
    pub fn new(
        g_id: fluree_db_core::GraphId,
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

    /// Serialize to spool wire format (34 bytes, includes g_id).
    #[inline]
    pub fn write_spool_le(&self, buf: &mut [u8; SPOOL_V2_WIRE_SIZE]) {
        buf[0..4].copy_from_slice(&self.g_id.to_le_bytes());
        buf[4..12].copy_from_slice(&self.s_id.as_u64().to_le_bytes());
        buf[12..20].copy_from_slice(&self.o_key.to_le_bytes());
        buf[20..24].copy_from_slice(&self.p_id.to_le_bytes());
        buf[24..28].copy_from_slice(&self.t.to_le_bytes());
        buf[28..32].copy_from_slice(&self.o_i.to_le_bytes());
        buf[32..34].copy_from_slice(&self.o_type.to_le_bytes());
    }

    /// Deserialize from spool wire format (34 bytes, includes g_id).
    #[inline]
    pub fn read_spool_le(buf: &[u8; SPOOL_V2_WIRE_SIZE]) -> Self {
        Self {
            g_id: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            s_id: SubjectId(u64::from_le_bytes(buf[4..12].try_into().unwrap())),
            o_key: u64::from_le_bytes(buf[12..20].try_into().unwrap()),
            p_id: u32::from_le_bytes(buf[20..24].try_into().unwrap()),
            t: u32::from_le_bytes(buf[24..28].try_into().unwrap()),
            o_i: u32::from_le_bytes(buf[28..32].try_into().unwrap()),
            o_type: u16::from_le_bytes(buf[32..34].try_into().unwrap()),
        }
    }
}

/// Run-file record layout for one file: the 30-byte record, then `g_id`
/// (u32 LE) when the file's order is pooled, then the op byte when the file
/// carries ops. Dedicated orders keep the 30/31-byte layouts above, so their
/// run files are unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunRecordLayout {
    pooled: bool,
    with_op: bool,
}

impl RunRecordLayout {
    #[inline]
    pub fn new(order: RunSortOrder, with_op: bool) -> Self {
        Self {
            pooled: order.is_pooled(),
            with_op,
        }
    }

    /// Bytes per record.
    #[inline]
    pub fn size(self) -> usize {
        RECORD_V2_WIRE_SIZE + if self.pooled { 4 } else { 0 } + usize::from(self.with_op)
    }

    /// Encode `rec` (and `op`, when the layout carries ops) into `buf`.
    #[inline]
    pub fn write(self, rec: &RunRecordV2, op: u8, buf: &mut [u8]) {
        rec.write_run_le((&mut buf[..RECORD_V2_WIRE_SIZE]).try_into().unwrap());
        let mut at = RECORD_V2_WIRE_SIZE;
        if self.pooled {
            buf[at..at + 4].copy_from_slice(&rec.g_id.to_le_bytes());
            at += 4;
        }
        if self.with_op {
            buf[at] = op;
        }
    }

    /// Decode one record; the op reads as 1 (assert) when the layout has none.
    #[inline]
    pub fn read(self, buf: &[u8]) -> (RunRecordV2, u8) {
        let mut rec = RunRecordV2::read_run_le(buf[..RECORD_V2_WIRE_SIZE].try_into().unwrap());
        let mut at = RECORD_V2_WIRE_SIZE;
        if self.pooled {
            rec.g_id = u32::from_le_bytes(buf[at..at + 4].try_into().unwrap());
            at += 4;
        }
        let op = if self.with_op { buf[at] } else { 1 };
        (rec, op)
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

/// Pooled SPOT: `(s_id, p_id, o_type, o_key, o_i, g_id)`.
#[inline]
pub fn cmp_v2_spot_g(a: &RunRecordV2, b: &RunRecordV2) -> Ordering {
    cmp_v2_spot(a, b).then(a.g_id.cmp(&b.g_id))
}

/// Pooled PSOT: `(p_id, s_id, o_type, o_key, o_i, g_id)`.
#[inline]
pub fn cmp_v2_psot_g(a: &RunRecordV2, b: &RunRecordV2) -> Ordering {
    cmp_v2_psot(a, b).then(a.g_id.cmp(&b.g_id))
}

/// Pooled POST: `(p_id, o_type, o_key, o_i, s_id, g_id)`.
#[inline]
pub fn cmp_v2_post_g(a: &RunRecordV2, b: &RunRecordV2) -> Ordering {
    cmp_v2_post(a, b).then(a.g_id.cmp(&b.g_id))
}

/// Pooled OPST: `(o_type, o_key, o_i, p_id, s_id, g_id)`.
#[inline]
pub fn cmp_v2_opst_g(a: &RunRecordV2, b: &RunRecordV2) -> Ordering {
    cmp_v2_opst(a, b).then(a.g_id.cmp(&b.g_id))
}

/// Return the V2 comparator function for a given sort order.
pub fn cmp_v2_for_order(order: RunSortOrder) -> fn(&RunRecordV2, &RunRecordV2) -> Ordering {
    match order {
        RunSortOrder::Spot => cmp_v2_spot,
        RunSortOrder::Psot => cmp_v2_psot,
        RunSortOrder::Post => cmp_v2_post,
        RunSortOrder::Opst => cmp_v2_opst,
        RunSortOrder::Gspo => cmp_v2_g_spot,
        RunSortOrder::SpotG => cmp_v2_spot_g,
        RunSortOrder::PsotG => cmp_v2_psot_g,
        RunSortOrder::PostG => cmp_v2_post_g,
        RunSortOrder::OpstG => cmp_v2_opst_g,
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

/// Fact identity in the pooled tree: the five identity columns plus `g_id`.
/// The same triple in two graphs is two facts.
///
/// Dedicated trees must keep [`same_identity_v2`]: their records do not agree
/// on `g_id` (rows decoded from leaves carry 0, novelty carries the graph).
#[inline]
pub fn same_identity_v2_g(a: &RunRecordV2, b: &RunRecordV2) -> bool {
    a.g_id == b.g_id && same_identity_v2(a, b)
}

/// Identity function for merges in a tree of the given order.
pub fn identity_v2_for_order(order: RunSortOrder) -> fn(&RunRecordV2, &RunRecordV2) -> bool {
    if order.is_pooled() {
        same_identity_v2_g
    } else {
        same_identity_v2
    }
}

// ============================================================================
// V2 Ordered Key (for leaf/branch routing)
// ============================================================================

/// Routing key for leaf/branch routing, held in a fixed 30-byte array.
///
/// The field order matches the physical sort order so comparison is
/// lexicographic on the raw bytes (memcmp). All fields are big-endian
/// for correct byte-wise comparison.
///
/// Dedicated orders use the first 26 bytes and leave the rest zero; only those
/// 26 bytes are stored ([`ordered_key_len`]). Pooled orders add `g_id` as a
/// u32, trailing for `SpotG`..`OpstG` and leading for `Gspo`, and store all 30.
///
/// | Order | Layout |
/// |-------|--------|
/// | SPOT  | `s_id(8) + p_id(4) + o_type(2) + o_key(8) + o_i(4)` |
/// | PSOT  | `p_id(4) + s_id(8) + o_type(2) + o_key(8) + o_i(4)` |
/// | POST  | `p_id(4) + o_type(2) + o_key(8) + o_i(4) + s_id(8)` |
/// | OPST  | `o_type(2) + o_key(8) + o_i(4) + p_id(4) + s_id(8)` |
/// | GSPO  | `g_id(4) + ` SPOT |
/// | xxxxG | the base order's 26 bytes `+ g_id(4)` |
pub const ORDERED_KEY_V2_SIZE: usize = 30;

/// Stored key length of a dedicated order.
pub const ORDERED_KEY_V2_BASE_SIZE: usize = 26;

/// Stored length of an ordered key for `order`: 26 bytes for dedicated
/// orders, 30 for pooled ones.
#[inline]
pub fn ordered_key_len(order: RunSortOrder) -> usize {
    if order.is_pooled() {
        ORDERED_KEY_V2_SIZE
    } else {
        ORDERED_KEY_V2_BASE_SIZE
    }
}

#[inline]
fn write_base_key(order: RunSortOrder, rec: &RunRecordV2, buf: &mut [u8]) {
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
        _ => unreachable!("write_base_key takes a dedicated order"),
    }
}

#[inline]
fn read_base_key(order: RunSortOrder, buf: &[u8]) -> RunRecordV2 {
    let u16_at = |i: usize| u16::from_be_bytes(buf[i..i + 2].try_into().unwrap());
    let u32_at = |i: usize| u32::from_be_bytes(buf[i..i + 4].try_into().unwrap());
    let u64_at = |i: usize| u64::from_be_bytes(buf[i..i + 8].try_into().unwrap());
    let (s_id, p_id, o_type, o_key, o_i) = match order {
        RunSortOrder::Spot => (u64_at(0), u32_at(8), u16_at(12), u64_at(14), u32_at(22)),
        RunSortOrder::Psot => (u64_at(4), u32_at(0), u16_at(12), u64_at(14), u32_at(22)),
        RunSortOrder::Post => (u64_at(18), u32_at(0), u16_at(4), u64_at(6), u32_at(14)),
        RunSortOrder::Opst => (u64_at(18), u32_at(14), u16_at(0), u64_at(2), u32_at(10)),
        _ => unreachable!("read_base_key takes a dedicated order"),
    };
    RunRecordV2 {
        s_id: SubjectId(s_id),
        o_key,
        p_id,
        t: 0,
        o_i,
        o_type,
        g_id: 0,
    }
}

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
    let g = rec.g_id.to_be_bytes();
    match order {
        RunSortOrder::Gspo => {
            buf[0..4].copy_from_slice(&g);
            write_base_key(RunSortOrder::Spot, rec, &mut buf[4..30]);
        }
        o if o.is_pooled() => {
            write_base_key(o.base(), rec, &mut buf[0..26]);
            buf[26..30].copy_from_slice(&g);
        }
        o => {
            write_base_key(o, rec, &mut buf[0..26]);
            buf[26..30].fill(0);
        }
    }
}

/// Parse a V2 ordered routing key back into a `RunRecordV2`.
///
/// Inverse of `write_ordered_key_v2`. `t` is not stored and reads as 0, as
/// does `g_id` for a dedicated order.
#[inline]
pub fn read_ordered_key_v2(order: RunSortOrder, buf: &[u8; ORDERED_KEY_V2_SIZE]) -> RunRecordV2 {
    let g_at = |i: usize| u32::from_be_bytes(buf[i..i + 4].try_into().unwrap());
    match order {
        RunSortOrder::Gspo => {
            let mut rec = read_base_key(RunSortOrder::Spot, &buf[4..30]);
            rec.g_id = g_at(0);
            rec
        }
        o if o.is_pooled() => {
            let mut rec = read_base_key(o.base(), &buf[0..26]);
            rec.g_id = g_at(26);
            rec
        }
        o => read_base_key(o, &buf[0..26]),
    }
}

/// Read a stored key of `ordered_key_len(order)` bytes into the in-memory
/// 30-byte form (zero-padded for dedicated orders).
#[inline]
pub fn ordered_key_from_stored(order: RunSortOrder, bytes: &[u8]) -> [u8; ORDERED_KEY_V2_SIZE] {
    let mut key = [0u8; ORDERED_KEY_V2_SIZE];
    let n = ordered_key_len(order);
    key[..n].copy_from_slice(&bytes[..n]);
    key
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

    fn in_graph(mut rec: RunRecordV2, g_id: fluree_db_core::GraphId) -> RunRecordV2 {
        rec.g_id = g_id;
        rec
    }

    #[test]
    fn pooled_key_round_trips_with_its_graph() {
        let rec = in_graph(
            make_rec(7, 3, OType::XSD_INTEGER.as_u16(), 42, LIST_INDEX_NONE, 1),
            513,
        );
        for &order in RunSortOrder::pool_build_orders() {
            assert_eq!(ordered_key_len(order), 30);
            let mut key = [0u8; ORDERED_KEY_V2_SIZE];
            write_ordered_key_v2(order, &rec, &mut key);
            let back = read_ordered_key_v2(order, &key);
            assert_eq!(cmp_v2_for_order(order)(&rec, &back), Ordering::Equal);
            assert_eq!(back.g_id, 513, "{order:?}");
        }
        for &order in RunSortOrder::all_build_orders() {
            assert_eq!(ordered_key_len(order), 26);
            let mut key = [0xFF; ORDERED_KEY_V2_SIZE];
            write_ordered_key_v2(order, &rec, &mut key);
            assert_eq!(key[26..], [0, 0, 0, 0], "{order:?}: graph bytes stay zero");
            assert_eq!(read_ordered_key_v2(order, &key).g_id, 0);
        }
    }

    #[test]
    fn pooled_key_memcmp_matches_comparator() {
        let int = OType::XSD_INTEGER.as_u16();
        let string = OType::XSD_STRING.as_u16();
        let recs = [
            in_graph(make_rec(1, 1, int, 5, LIST_INDEX_NONE, 1), 3),
            in_graph(make_rec(1, 1, int, 5, LIST_INDEX_NONE, 1), 4),
            in_graph(make_rec(1, 1, int, 5, 0, 1), 3),
            in_graph(make_rec(2, 1, int, 5, LIST_INDEX_NONE, 1), 3),
            in_graph(make_rec(1, 2, int, 4, LIST_INDEX_NONE, 1), 300),
            in_graph(make_rec(0, 9, string, 1, LIST_INDEX_NONE, 1), u32::MAX),
            in_graph(make_rec(u64::MAX, 0, 0, u64::MAX, LIST_INDEX_NONE, 1), 0),
        ];
        for &order in RunSortOrder::pool_build_orders() {
            let cmp = cmp_v2_for_order(order);
            for a in &recs {
                for b in &recs {
                    let (mut ka, mut kb) = ([0u8; ORDERED_KEY_V2_SIZE], [0u8; ORDERED_KEY_V2_SIZE]);
                    write_ordered_key_v2(order, a, &mut ka);
                    write_ordered_key_v2(order, b, &mut kb);
                    assert_eq!(
                        cmp_ordered_key_v2(&ka, &kb),
                        cmp(a, b),
                        "{order:?} {a:?} {b:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn trailing_graph_keeps_cross_graph_ranges_contiguous() {
        let int = OType::XSD_INTEGER.as_u16();
        let a = in_graph(make_rec(9, 1, int, 5, LIST_INDEX_NONE, 1), 3);
        let b = in_graph(make_rec(1, 1, int, 5, LIST_INDEX_NONE, 1), 4);
        let c = in_graph(make_rec(1, 1, int, 6, LIST_INDEX_NONE, 1), 3);
        // POST with a trailing graph: one value's rows from every graph are
        // adjacent, ordered by subject before graph.
        assert_eq!(cmp_v2_post_g(&b, &a), Ordering::Less);
        assert_eq!(cmp_v2_post_g(&a, &c), Ordering::Less);
        // GSPO: one graph's rows are adjacent.
        assert_eq!(cmp_v2_g_spot(&a, &b), Ordering::Less);
        assert_eq!(cmp_v2_g_spot(&c, &b), Ordering::Less);
    }

    #[test]
    fn pooled_identity_includes_the_graph() {
        let int = OType::XSD_INTEGER.as_u16();
        let a = in_graph(make_rec(1, 1, int, 5, LIST_INDEX_NONE, 1), 3);
        let b = in_graph(make_rec(1, 1, int, 5, LIST_INDEX_NONE, 2), 4);
        assert!(same_identity_v2(&a, &b));
        assert!(!same_identity_v2_g(&a, &b));
        assert!(!identity_v2_for_order(RunSortOrder::PostG)(&a, &b));
        assert!(identity_v2_for_order(RunSortOrder::Post)(&a, &b));
    }
}
