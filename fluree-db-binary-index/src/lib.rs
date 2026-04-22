//! Binary columnar index formats, codecs, and read-side runtime for Fluree DB.
//!
//! This crate owns the on-disk binary index formats (FIR6, FBR3, FLI3, FPK1,
//! DTB1, DLR1, VAS1, NB1) and the read-side runtime for loading and querying
//! binary indexes. It is the dependency for `fluree-db-query` (instead of
//! depending on the full `fluree-db-indexer` build pipeline).

pub mod analyzer;
pub mod error;
pub mod types;

pub mod arena;
pub mod dict;
pub mod dict_novelty_safe;
pub mod format;
pub mod read;

// ── Read-side types ─────────────────────────────────────────────────────────
pub use read::batched_lookup::batched_lookup_predicate_refs;
pub use read::binary_cursor::BinaryCursor;
pub use read::binary_index_store::{BinaryGraphView, BinaryIndexStore};
pub use read::column_types::{BinaryFilter, ColumnBatch, ColumnData, ColumnProjection, ColumnSet};
pub use read::leaflet_cache::{LeafletCache, LeafletCacheKey, V3BatchCacheKey};
pub use read::replay::replay_leaflet;

// ── Format types ────────────────────────────────────────────────────────────
pub use format::branch::{BranchManifest, LeafEntry};
pub use format::index_root::IndexRoot;
pub use format::run_record::{cmp_for_order, cmp_psot, cmp_spot, RunRecord, RunSortOrder};
pub use format::wire_helpers::{
    BinaryGarbageRef, BinaryPrevIndexRef, DictPackRefs, DictRefs, DictTreeRefs, FulltextArenaRef,
    GraphArenaRefs, PackBranchEntry, SpatialArenaRef, VectorDictRef,
};

// ── Arena ───────────────────────────────────────────────────────────────────
pub use arena::fulltext::FulltextArena;

// ── Types ───────────────────────────────────────────────────────────────────
pub use read::types::{resolve_overlay_ops, sort_overlay_ops, DecodedRowV3, OverlayOp};
pub use types::NumericShape;

// ── Dict ────────────────────────────────────────────────────────────────────
pub use dict::{
    DictBranch, DictTreeReader, ForwardPack, ForwardPackReader, LanguageTagDict, PredicateDict,
    TreeBuildResult,
};
