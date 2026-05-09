//! Edge-annotation arenas — secondary indexes over `f:reifies*` system facts.
//!
//! Two arenas, both content-addressed and treated like dictionary trees:
//!
//! - **Forward arena** (`EAFB1` / `EAFL1`): `EdgeKey -> ann_sid` lookup. Used by
//!   the read-side hydrator to attach annotation metadata to base-edge rows,
//!   and by the retract cascade to find dependent attachments.
//! - **Reverse arena** (`EARB1` / `EARL1`): `ann_sid -> EdgeKey` lookup. Used by
//!   `@reifies`-rooted queries and SPARQL-star `?ann rdf:reifies <<...>>` shape.
//!
//! Rows carry `(t, op)` so history queries surface the same attach/detach
//! events as flake history. Visibility filtering is applied at read time;
//! readers return iterators because the underlying store is a multimap.
//!
//! ## Layering
//!
//! - **Format** (`format`) — wire bytes, magic numbers, codec routing,
//!   roundtrip-only. No I/O.
//! - **Builder** (`builder`) — chunk pre-sorted rows into leaves and
//!   manifest entries; pure (no I/O), so callers interleave the CAS
//!   write between leaf encode and branch encode.
//! - Reader / incremental-merge live in sibling modules added in
//!   later slices of M2b.
//!
//! ## Empty-vs-absent semantics
//!
//! - `IndexRoot.annotation_index = None` paired with
//!   `IndexRoot.has_annotations = false` is a hard guarantee that the
//!   indexed snapshot has zero annotation attachments. See the truth
//!   table in [`fluree_db_core::annotation_index`].
//! - Empty branches/leaves are valid and decode to empty row vectors.
//!
//! See `EDGE_ANNOTATIONS.md` (storage shape) and `EDGE_ANNOTATIONS_IMPL_PLAN.md`
//! M2 for the design contract.

pub mod builder;
pub mod bundle;
pub mod format;

pub use builder::{
    build_forward_branch, build_forward_leaves, build_reverse_branch, build_reverse_leaves,
    forward_arena_stats, ForwardLeafSummary, ReverseLeafSummary, DEFAULT_TARGET_ROWS_PER_LEAF,
};
pub use bundle::{build_arenas_from_flakes, ArenaBuildOutput};
pub use format::{
    AnnotationForwardBranch, AnnotationForwardBranchEntry, AnnotationForwardLeaf,
    AnnotationForwardRow, AnnotationIndexRoot, AnnotationReverseBranch,
    AnnotationReverseBranchEntry, AnnotationReverseLeaf, AnnotationReverseRow, AnnotationStats,
    DecodeError, FORWARD_BRANCH_MAGIC, FORWARD_LEAF_MAGIC, REVERSE_BRANCH_MAGIC,
    REVERSE_LEAF_MAGIC,
};
