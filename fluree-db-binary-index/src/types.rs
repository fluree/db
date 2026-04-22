//! Query-facing run_index types shared across crate boundaries.

use crate::format::run_record::RunSortOrder;

/// Immutable input column slices for reading row data.
///
/// Bundles the 8 column slices that provide decoded row data during
/// replay operations (reading existing leaflet data).
pub struct RowColumnSlice<'a> {
    /// Subject IDs (u64).
    pub s: &'a [u64],
    /// Predicate IDs (u32).
    pub p: &'a [u32],
    /// Object kind discriminants.
    pub o_kinds: &'a [u8],
    /// Object key payloads.
    pub o_keys: &'a [u64],
    /// Datatype codes.
    pub dt: &'a [u32],
    /// Transaction timestamps (u32 on disk/cache; widened to i64 at API boundary).
    pub t: &'a [u32],
    /// Language tag IDs.
    pub lang: &'a [u16],
    /// List indices.
    pub i: &'a [i32],
}

impl RowColumnSlice<'_> {
    /// Get the number of rows in these column slices.
    #[inline]
    pub fn len(&self) -> usize {
        self.s.len()
    }

    /// Check if the column slices are empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.s.is_empty()
    }

    /// Get a single decoded row at the given index.
    #[inline]
    pub fn get(&self, idx: usize) -> DecodedRow {
        DecodedRow {
            s_id: self.s[idx],
            p_id: self.p[idx],
            o_kind: self.o_kinds[idx],
            o_key: self.o_keys[idx],
            dt: self.dt[idx],
            t: self.t[idx] as i64,
            lang_id: self.lang[idx],
            i: self.i[idx],
        }
    }
}

/// A single decoded row from columnar storage.
///
/// Used for identity comparisons, conversions to Flake, and passing
/// row data through function boundaries without 8 separate parameters.
#[derive(Debug, Clone, Copy)]
pub struct DecodedRow {
    /// Subject dictionary ID.
    pub s_id: u64,
    /// Predicate dictionary ID.
    pub p_id: u32,
    /// Object kind discriminant (see `ObjKind`).
    pub o_kind: u8,
    /// Object key payload (interpretation depends on `o_kind`).
    pub o_key: u64,
    /// Datatype code.
    pub dt: u32,
    /// Transaction timestamp.
    pub t: i64,
    /// Language tag ID.
    pub lang_id: u16,
    /// List index.
    pub i: i32,
}

/// An overlay operation translated to integer-ID space.
///
/// Produced by translating `Flake` overlay ops via `BinaryIndexStore` reverse
/// lookups (`sid_to_s_id`, `sid_to_p_id`, `value_to_value_id`). Sorted by the
/// cursor's sort order for streaming merge with decoded leaflet rows.
///
/// Unlike `RunRecord`, this type is for ephemeral query-time merge only —
/// overlay ops are never persisted to disk.
#[derive(Debug, Clone, Copy)]
pub struct OverlayOp {
    pub s_id: u64,
    pub p_id: u32,
    /// Object kind discriminant (see `ObjKind`).
    pub o_kind: u8,
    /// Object key payload (interpretation depends on `o_kind`).
    pub o_key: u64,
    pub t: i64,
    /// true = assert, false = retract.
    pub op: bool,
    pub dt: u16,
    pub lang_id: u16,
    pub i_val: i32,
}

/// Sort overlay ops by the given sort order's column priority.
///
/// Column priorities must match the on-disk comparator order used by
/// `cmp_for_order`, `cmp_row_vs_overlay`, and `cmp_row_vs_record` so
/// that the merge cursors see a consistent sequence.
pub fn sort_overlay_ops(ops: &mut [OverlayOp], order: RunSortOrder) {
    ops.sort_unstable_by(|a, b| match order {
        RunSortOrder::Spot => a
            .s_id
            .cmp(&b.s_id)
            .then(a.p_id.cmp(&b.p_id))
            .then(a.o_kind.cmp(&b.o_kind))
            .then(a.o_key.cmp(&b.o_key))
            .then(a.dt.cmp(&b.dt)),
        RunSortOrder::Psot => a
            .p_id
            .cmp(&b.p_id)
            .then(a.s_id.cmp(&b.s_id))
            .then(a.o_kind.cmp(&b.o_kind))
            .then(a.o_key.cmp(&b.o_key))
            .then(a.dt.cmp(&b.dt)),
        RunSortOrder::Post => a
            .p_id
            .cmp(&b.p_id)
            .then(a.o_kind.cmp(&b.o_kind))
            .then(a.o_key.cmp(&b.o_key))
            .then(a.dt.cmp(&b.dt))
            .then(a.s_id.cmp(&b.s_id)),
        RunSortOrder::Opst => a
            .o_kind
            .cmp(&b.o_kind)
            .then(a.o_key.cmp(&b.o_key))
            .then(a.dt.cmp(&b.dt))
            .then(a.p_id.cmp(&b.p_id))
            .then(a.s_id.cmp(&b.s_id)),
    });
}

/// Per-predicate classification of numeric value kinds.
///
/// Used by the binary scan path to decide whether a single POST scan
/// (`IntOnly` or `FloatOnly`) suffices, or whether a fallback is needed (`Mixed`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericShape {
    /// All numeric values for this predicate are `NumInt` (kind 0x03).
    IntOnly,
    /// All numeric values for this predicate are `NumF64` (kind 0x04).
    FloatOnly,
    /// Predicate has both `NumInt` and `NumF64` values.
    Mixed,
}
