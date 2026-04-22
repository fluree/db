//! S2-based spatial indexing for Fluree DB.
//!
//! This crate provides spatial indexing for complex geometries (polygons, linestrings, etc.)
//! using Google's S2 cell-based spatial indexing. It supports:
//!
//! - **Time-travel queries** via `to_t` semantics
//! - **Novelty overlay** for uncommitted changes
//! - **Content-addressed, chunked snapshots** for CAS storage
//! - **Both embedded and remote** deployment modes
//!
//! # Architecture
//!
//! The spatial index maps geometries to S2 cell coverings, storing entries sorted by
//! `(cell_id, subject_id, t DESC)`. This enables efficient range scans for spatial queries
//! while preserving Fluree's versioned snapshot model.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                        SpatialIndexSnapshot                         │
//! ├─────────────────────────────────────────────────────────────────────┤
//! │  cell_index (chunked, CAS)  │  novelty (in-memory)  │  arena (CAS)  │
//! └─────────────────────────────────────────────────────────────────────┘
//!                    │                      │                    │
//!                    └──────────┬───────────┘                    │
//!                               ▼                                │
//!                    MergeSorted iterator                        │
//!                               │                                │
//!                               ▼                                │
//!                    ReplayResolver (at to_t)                    │
//!                               │                                │
//!                               ▼                                │
//!                    Deduper (by subject_id)                     │
//!                               │                                │
//!                               ▼                                │
//!                    BBox prefilter ◄────────────────────────────┘
//!                               │
//!                               ▼
//!                    Exact predicate refine (geo crate)
//!                               │
//!                               ▼
//!                    Query results
//! ```
//!
//! # Modules
//!
//! - [`config`]: Spatial index configuration types
//! - [`builder`]: Spatial index builder for creating indexes from geometry data
//! - [`geometry`]: WKT parsing, geometry arena, and metadata computation
//! - [`cell_index`]: Sorted cell index storage (chunked, CAS-backed)
//! - [`covering`]: S2 covering generation
//! - [`replay`]: Time-travel replay and novelty merge
//! - [`dedup`]: Global deduplication across cells
//! - [`snapshot`]: Spatial index snapshot (combines all components)
//! - [`provider`]: Provider trait for embedded and remote modes
//! - [`error`]: Error types

pub mod config;
pub mod error;

mod builder;
mod cell_index;
pub mod covering;
pub(crate) mod dedup;
pub mod geometry;
pub mod novelty;
mod provider;
pub(crate) mod replay;
mod snapshot;

// Re-export key types
pub use builder::{BuildResult, BuildStats, SpatialIndexBuilder, WriteResult};
pub use cell_index::{CellEntry, CellIndexBuilder, CellIndexManifest, CellIndexReader};
pub use config::{SpatialConfig, SpatialCreateConfig};
pub use error::{Result, SpatialError};
pub use geometry::{GeometryArena, GeometryMetadata, GeometryType};
pub use novelty::{derive_spatial_novelty, DerivedNovelty, SpatialNoveltyOp};
pub use provider::{
    EmbeddedSpatialProvider, NoveltyState, ProximityResult, SpatialIndexProvider, SpatialResult,
    NOVELTY_HANDLE_FLAG,
};
pub use snapshot::{QueryStats, SpatialIndexRoot, SpatialIndexSnapshot};
