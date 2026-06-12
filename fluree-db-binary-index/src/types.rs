//! Query-facing run_index types shared across crate boundaries.
//!
//! NOTE (2026-06 audit, Phase 0.2): this module once carried a parallel set
//! of row/overlay types (`RowColumnSlice`, `DecodedRow`, `OverlayOp`,
//! `sort_overlay_ops`) predating the V3 read path. They had drifted from the
//! live definitions in `read::types` — including a u32-vs-u16 datatype-code
//! width mismatch flagged by the audit — and had no remaining users, so they
//! were deleted. The live types are `read::types::{DecodedRowV3, OverlayOp}`.

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
