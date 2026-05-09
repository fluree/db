//! Inline metadata for the edge-annotation arenas.
//!
//! Lives in `fluree-db-core` (rather than `fluree-db-binary-index`) so
//! [`crate::db::LedgerSnapshot`] can hold an
//! `Option<AnnotationIndexRoot>` directly. The heavy on-disk arena
//! formats (forward/reverse branch + leaf blobs) stay in
//! `fluree-db-binary-index::annotation_arena::format`.
//!
//! ## Empty-vs-absent semantics
//!
//! The indexed-arena guarantee depends on **both** signals on
//! [`crate::db::LedgerSnapshot`]:
//!
//! | `has_annotations` | `annotation_index` | Meaning                                |
//! |-------------------|--------------------|----------------------------------------|
//! | `false`           | `None`             | Hard guarantee: zero attachments. Cascade and reads short-circuit. |
//! | `true`            | `Some(_)`          | Builder ran. Forward/reverse arenas are authoritative for `t ≤ max_t`; novelty supplies the tail. |
//! | `true`            | `None`             | Pre-builder transitional state: snapshot may carry `f:reifies*` flakes but no arena yet — readers fall back to scan, cascade still runs. |
//! | `false`           | `Some(_)`          | Invariant violation. The FIR6 encoder coerces `FLAG_HAS_ANNOTATIONS` whenever an arena is present, so this state never reaches the wire. |
//!
//! Builders that produce arenas must set both signals; the encoder
//! defends against forgetting the sticky bit but cannot fix the
//! inverse (arena present, bool false in memory).
//!
//! See `EDGE_ANNOTATIONS.md` (Sidecar Artifacts) for the design contract.

use crate::ContentId;
use serde::{Deserialize, Serialize};

/// Aggregate counters populated at arena build time. Surfaced for
/// cost-based planning (M3) and storage inspection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnotationStats {
    /// Total forward-arena rows (one per asserted/retracted attachment event).
    pub forward_rows: u64,
    /// Total reverse-arena rows (mirror of `forward_rows` after compaction).
    pub reverse_rows: u64,
    /// Distinct edges with at least one current (live) attachment.
    pub distinct_edges: u64,
    /// Distinct annotation subjects.
    pub distinct_annotations: u64,
}

/// Inline section in the binary index root.
///
/// Carries CIDs for the forward/reverse arena root branches plus
/// build-time stats. Always emitted (with empty CIDs and zero stats)
/// when the indexed snapshot might contain attachments — never as
/// `None` if uncertain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnotationIndexRoot {
    /// Format version of the on-disk arena artifacts. Independent of
    /// the parent index root's own version so the arena format can
    /// roll forward without a full FIR6 bump.
    pub version: u8,
    /// Highest commit `t` reflected in either arena. Reads with
    /// `as_of_t` above this fall back to novelty for any newer
    /// attachments.
    pub max_t: i64,
    /// Forward-arena branch CID (`EAFB1`).
    pub forward_branch_cid: ContentId,
    /// Reverse-arena branch CID (`EARB1`).
    pub reverse_branch_cid: ContentId,
    /// Build-time stats. Always present (zero-valued for empty arenas).
    pub stats: AnnotationStats,
}
