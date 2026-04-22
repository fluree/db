//! V3 column-oriented read types.
//!
//! These types replace the V2 `CachedRegion1`/`CachedRegion2` pair with a
//! projection-driven, column-selective model. Each column is independently
//! materialized, constant (p_const/o_type_const), or absent (optional columns
//! like o_i that carry a sentinel default).

use std::sync::Arc;

use crate::format::column_block::ColumnId;
use crate::format::run_record::RunSortOrder;

// ============================================================================
// ColumnData — per-column representation
// ============================================================================

/// A single column's data for one leaflet.
///
/// Avoids allocation for constant or absent columns:
/// - `Block`: materialized (decompressed) column values.
/// - `Const`: every row has the same value (e.g. `p_const`, `o_type_const`).
/// - `AbsentDefault`: column not present; readers use the type's sentinel
///   (e.g. `u32::MAX` for o_i meaning "no list index").
#[derive(Debug, Clone)]
pub enum ColumnData<T: Copy> {
    /// Materialized column block (decompressed/decoded).
    Block(Arc<[T]>),
    /// Virtual constant — value is the same for every row in the leaflet.
    Const(T),
    /// Column not present and semantically defaulted.
    AbsentDefault,
}

impl<T: Copy> ColumnData<T> {
    /// Get the value at `idx`.
    ///
    /// # Panics
    /// - Out-of-bounds index on `Block`.
    /// - Called on `AbsentDefault` — use `is_absent()` to check first, or `get_or(default)`.
    #[inline]
    pub fn get(&self, idx: usize) -> T {
        match self {
            ColumnData::Block(arr) => arr[idx],
            ColumnData::Const(v) => *v,
            ColumnData::AbsentDefault => {
                panic!("ColumnData::get called on AbsentDefault; use get_or() or check is_absent() first")
            }
        }
    }

    /// Get the value at `idx`, or `default` if absent.
    #[inline]
    pub fn get_or(&self, idx: usize, default: T) -> T {
        match self {
            ColumnData::Block(arr) => arr[idx],
            ColumnData::Const(v) => *v,
            ColumnData::AbsentDefault => default,
        }
    }

    /// True if this column is absent (not stored, not constant).
    #[inline]
    pub fn is_absent(&self) -> bool {
        matches!(self, ColumnData::AbsentDefault)
    }

    /// True if this column is a constant.
    #[inline]
    pub fn is_const(&self) -> bool {
        matches!(self, ColumnData::Const(_))
    }

    /// Byte size estimate for cache weighing.
    pub fn byte_size(&self) -> usize {
        match self {
            ColumnData::Block(arr) => arr.len() * std::mem::size_of::<T>(),
            ColumnData::Const(_) => std::mem::size_of::<T>(),
            ColumnData::AbsentDefault => 0,
        }
    }
}

// ============================================================================
// ColumnBatch — one leaflet's worth of decoded columns
// ============================================================================

/// Columnar batch from one leaflet, replacing `CachedRegion1 + CachedRegion2`.
///
/// Fields are `ColumnData<T>` so constant columns (p_const, o_type_const)
/// avoid allocation, and optional columns (o_i, t) can be absent.
#[derive(Debug, Clone)]
pub struct ColumnBatch {
    /// Number of rows in this batch.
    pub row_count: usize,

    /// Subject IDs — always present.
    pub s_id: ColumnData<u64>,
    /// Object keys — always present.
    pub o_key: ColumnData<u64>,

    /// Predicate IDs — `Const(p_const)` for POST/PSOT leaflets.
    pub p_id: ColumnData<u32>,
    /// Object type tags — `Const(o_type_const)` when type-homogeneous.
    pub o_type: ColumnData<u16>,

    /// List index — `AbsentDefault` when all values are sentinel (`u32::MAX`).
    pub o_i: ColumnData<u32>,
    /// Transaction time — always stored on disk but only decoded when requested.
    pub t: ColumnData<u32>,
}

impl ColumnBatch {
    /// Create an empty batch with zero rows.
    pub fn empty() -> Self {
        Self {
            row_count: 0,
            s_id: ColumnData::AbsentDefault,
            o_key: ColumnData::AbsentDefault,
            p_id: ColumnData::AbsentDefault,
            o_type: ColumnData::AbsentDefault,
            o_i: ColumnData::AbsentDefault,
            t: ColumnData::AbsentDefault,
        }
    }

    /// True if this batch has no rows.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Byte size estimate for cache weighing.
    pub fn byte_size(&self) -> usize {
        self.s_id.byte_size()
            + self.o_key.byte_size()
            + self.p_id.byte_size()
            + self.o_type.byte_size()
            + self.o_i.byte_size()
            + self.t.byte_size()
    }
}

// ============================================================================
// ColumnSet — bitflag set of column IDs
// ============================================================================

/// Bitflag set of column IDs (u16 for future-proofing).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnSet(pub u16);

impl ColumnSet {
    pub const EMPTY: Self = Self(0);

    /// All core columns (s_id, p_id, o_type, o_key).
    pub const CORE: Self = Self(
        (1 << ColumnId::SId as u16)
            | (1 << ColumnId::PId as u16)
            | (1 << ColumnId::OType as u16)
            | (1 << ColumnId::OKey as u16),
    );

    /// All columns including optional ones.
    pub const ALL: Self = Self(
        (1 << ColumnId::SId as u16)
            | (1 << ColumnId::PId as u16)
            | (1 << ColumnId::OType as u16)
            | (1 << ColumnId::OKey as u16)
            | (1 << ColumnId::OI as u16)
            | (1 << ColumnId::T as u16),
    );

    #[inline]
    pub fn contains(self, col: ColumnId) -> bool {
        self.0 & (1 << col as u16) != 0
    }

    #[inline]
    pub fn insert(&mut self, col: ColumnId) {
        self.0 |= 1 << col as u16;
    }

    #[inline]
    pub fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Create a set from a single column.
    #[inline]
    pub fn single(col: ColumnId) -> Self {
        Self(1 << col as u16)
    }
}

// ============================================================================
// ColumnProjection — what to load/decompress
// ============================================================================

/// Physical projection: which columns must be loaded/decompressed.
///
/// Separates "output" columns (materialized in the returned batch) from
/// "internal" columns needed by the cursor for correctness (filter evaluation,
/// overlay merge identity, sort-order comparison) but not returned to the operator.
#[derive(Debug, Clone, Copy)]
pub struct ColumnProjection {
    /// Columns that must be materialized in the returned batch.
    pub output: ColumnSet,
    /// Columns needed for cursor internals (filter/order/identity).
    /// May be dropped before returning the batch.
    pub internal: ColumnSet,
}

impl ColumnProjection {
    /// The effective set of columns to load: `output ∪ internal`.
    #[inline]
    pub fn effective(&self) -> ColumnSet {
        self.output.union(self.internal)
    }

    /// All columns, both output and internal.
    pub fn all() -> Self {
        Self {
            output: ColumnSet::ALL,
            internal: ColumnSet::EMPTY,
        }
    }

    /// Check if a column is needed (in either output or internal).
    #[inline]
    pub fn needs(&self, col: ColumnId) -> bool {
        self.effective().contains(col)
    }
}

// ============================================================================
// BinaryFilter — integer-ID filter for V3
// ============================================================================

/// Row-level integer-ID filter for V3 columnar scans.
///
/// Replaces V2's `BinaryFilter` which used `(s_id, p_id, o_kind, o_key)`.
/// V3 uses `(s_id, p_id, o_type, o_key, o_i)` matching the new identity model.
#[derive(Debug, Clone, Default)]
pub struct BinaryFilter {
    pub s_id: Option<u64>,
    pub p_id: Option<u32>,
    pub o_type: Option<u16>,
    pub o_key: Option<u64>,
    pub o_i: Option<u32>,
}

impl BinaryFilter {
    /// True if the filter has no constraints (matches everything).
    pub fn is_empty(&self) -> bool {
        self.s_id.is_none()
            && self.p_id.is_none()
            && self.o_type.is_none()
            && self.o_key.is_none()
            && self.o_i.is_none()
    }

    /// Check whether a single row passes this filter.
    #[inline]
    pub fn matches(&self, s_id: u64, p_id: u32, o_type: u16, o_key: u64, o_i: u32) -> bool {
        if let Some(f) = self.s_id {
            if s_id != f {
                return false;
            }
        }
        if let Some(f) = self.p_id {
            if p_id != f {
                return false;
            }
        }
        if let Some(f) = self.o_type {
            if o_type != f {
                return false;
            }
        }
        if let Some(f) = self.o_key {
            if o_key != f {
                return false;
            }
        }
        if let Some(f) = self.o_i {
            if o_i != f {
                return false;
            }
        }
        true
    }

    /// Check whether a leaflet can be entirely skipped based on directory
    /// metadata (p_const, o_type_const). Returns true if the leaflet
    /// definitely has no matching rows.
    pub fn skip_leaflet(&self, p_const: Option<u32>, o_type_const: Option<u16>) -> bool {
        if let (Some(filter_p), Some(leaflet_p)) = (self.p_id, p_const) {
            if filter_p != leaflet_p {
                return true;
            }
        }
        if let (Some(filter_ot), Some(leaflet_ot)) = (self.o_type, o_type_const) {
            if filter_ot != leaflet_ot {
                return true;
            }
        }
        false
    }
}

// ============================================================================
// EmitMask → ColumnProjection conversion helpers
// ============================================================================

impl ColumnProjection {
    /// Build a projection for a basic scan that needs identity columns for
    /// cursor correctness plus specific output columns.
    ///
    /// `needs_t`: whether the `t` column should be loaded (for time-travel
    /// or when `?t` is bound in the query).
    pub fn for_scan(output: ColumnSet, needs_t: bool, _order: RunSortOrder) -> Self {
        // Internal columns: the cursor always needs enough to maintain sort
        // order for overlay merge and to evaluate the filter. The identity
        // columns (s_id, p_id, o_type, o_key, o_i) are always internal.
        let mut internal = ColumnSet::CORE;
        internal.insert(ColumnId::OI);
        if needs_t {
            internal.insert(ColumnId::T);
        }
        // Remove from internal anything already in output (avoid duplication).
        let internal = ColumnSet(internal.0 & !output.0);
        Self { output, internal }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_data_const() {
        let cd: ColumnData<u32> = ColumnData::Const(42);
        assert_eq!(cd.get(0), 42);
        assert_eq!(cd.get(999), 42);
        assert!(cd.is_const());
        assert!(!cd.is_absent());
    }

    #[test]
    fn column_data_block() {
        let arr: Arc<[u64]> = vec![10, 20, 30].into();
        let cd = ColumnData::Block(arr);
        assert_eq!(cd.get(0), 10);
        assert_eq!(cd.get(2), 30);
        assert!(!cd.is_const());
        assert!(!cd.is_absent());
    }

    #[test]
    fn column_data_absent_default() {
        let cd: ColumnData<u32> = ColumnData::AbsentDefault;
        assert!(cd.is_absent());
        assert_eq!(cd.get_or(0, u32::MAX), u32::MAX);
    }

    #[test]
    fn column_set_operations() {
        let mut set = ColumnSet::EMPTY;
        assert!(!set.contains(ColumnId::SId));
        set.insert(ColumnId::SId);
        assert!(set.contains(ColumnId::SId));
        assert!(!set.contains(ColumnId::PId));

        let set2 = ColumnSet::single(ColumnId::PId);
        let union = set.union(set2);
        assert!(union.contains(ColumnId::SId));
        assert!(union.contains(ColumnId::PId));
    }

    #[test]
    fn projection_effective() {
        let proj = ColumnProjection {
            output: ColumnSet::single(ColumnId::SId),
            internal: ColumnSet::single(ColumnId::OKey),
        };
        let eff = proj.effective();
        assert!(eff.contains(ColumnId::SId));
        assert!(eff.contains(ColumnId::OKey));
        assert!(!eff.contains(ColumnId::T));
    }

    #[test]
    fn filter_matches() {
        let filter = BinaryFilter {
            p_id: Some(5),
            ..Default::default()
        };
        assert!(filter.matches(1, 5, 0, 0, u32::MAX));
        assert!(!filter.matches(1, 6, 0, 0, u32::MAX));
    }

    #[test]
    fn filter_skip_leaflet() {
        let filter = BinaryFilter {
            p_id: Some(5),
            ..Default::default()
        };
        // p_const=5 matches filter → don't skip
        assert!(!filter.skip_leaflet(Some(5), None));
        // p_const=6 doesn't match filter → skip
        assert!(filter.skip_leaflet(Some(6), None));
        // no p_const → can't skip
        assert!(!filter.skip_leaflet(None, None));
    }
}
