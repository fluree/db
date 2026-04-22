//! Spatial index configuration types.
//!
//! Defines configuration for creating and managing spatial indexes.

use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Configuration for S2 covering generation.
///
/// Controls the granularity and cell count of S2 coverings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S2CoveringConfig {
    /// Minimum S2 cell level (0-30). Lower = coarser cells.
    /// Default: 4 (covers ~1000km² at equator)
    pub min_level: u8,

    /// Maximum S2 cell level (0-30). Higher = finer cells.
    /// Default: 16 (covers ~150m² at equator)
    pub max_level: u8,

    /// Maximum number of cells in a covering.
    /// More cells = tighter fit but larger index entries.
    /// Default: 8
    pub max_cells: usize,
}

impl Default for S2CoveringConfig {
    fn default() -> Self {
        Self {
            min_level: 4,
            max_level: 16,
            max_cells: 8,
        }
    }
}

/// Configuration for geometry metadata computation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataConfig {
    /// Compute and store bounding box (strongly recommended).
    pub compute_bbox: bool,

    /// Compute and store centroid.
    pub compute_centroid: bool,

    /// Compute and store area (for polygons).
    pub compute_area: bool,

    /// Compute and store length (for linestrings).
    pub compute_length: bool,
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            compute_bbox: true,
            compute_centroid: true,
            compute_area: false,
            compute_length: false,
        }
    }
}

/// Configuration for creating a spatial index.
///
/// Used when building a new spatial index from a ledger.
#[derive(Debug, Clone)]
pub struct SpatialCreateConfig {
    /// Alias for the spatial virtual graph (e.g., "geo-index:main").
    pub vg_alias: Arc<str>,

    /// Source ledger alias to index.
    pub ledger_alias: Arc<str>,

    /// Predicate IRI to index (e.g., "http://example.org/location").
    /// Only geometries with this predicate are included.
    pub predicate: Arc<str>,

    /// S2 covering configuration.
    pub s2_config: S2CoveringConfig,

    /// Metadata computation configuration.
    pub metadata_config: MetadataConfig,

    /// Whether to also index POINT geometries in the S2 index.
    /// Default: false (points use inline GeoPoint in POST index).
    pub index_points: bool,

    /// Target chunk size for cell index leaflets (bytes).
    /// Default: 256KB
    pub chunk_target_bytes: usize,
}

impl SpatialCreateConfig {
    /// Create a new config with the given aliases and predicate.
    pub fn new(
        vg_alias: impl Into<Arc<str>>,
        ledger_alias: impl Into<Arc<str>>,
        predicate: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            vg_alias: vg_alias.into(),
            ledger_alias: ledger_alias.into(),
            predicate: predicate.into(),
            s2_config: S2CoveringConfig::default(),
            metadata_config: MetadataConfig::default(),
            index_points: false,
            chunk_target_bytes: 256 * 1024,
        }
    }

    /// Set S2 covering configuration.
    pub fn with_s2_config(mut self, config: S2CoveringConfig) -> Self {
        self.s2_config = config;
        self
    }

    /// Set metadata computation configuration.
    pub fn with_metadata_config(mut self, config: MetadataConfig) -> Self {
        self.metadata_config = config;
        self
    }

    /// Enable indexing of POINT geometries in S2 index.
    pub fn with_index_points(mut self, index_points: bool) -> Self {
        self.index_points = index_points;
        self
    }
}

/// Runtime configuration for a spatial index.
///
/// Stored in the index root manifest and used for query execution.
/// Uses `String` instead of `Arc<str>` for serde compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpatialConfig {
    /// Predicate IRI that was indexed.
    pub predicate: String,

    /// S2 covering configuration used at build time.
    pub s2_config: S2CoveringConfig,

    /// Metadata configuration used at build time.
    pub metadata_config: MetadataConfig,

    /// Whether points are included in the S2 index.
    pub index_points: bool,

    /// Version of the spatial index format.
    pub format_version: u32,
}

impl SpatialConfig {
    /// Current format version.
    pub const CURRENT_VERSION: u32 = 1;

    /// Create from a create config.
    pub fn from_create_config(create: &SpatialCreateConfig) -> Self {
        Self {
            predicate: create.predicate.to_string(),
            s2_config: create.s2_config.clone(),
            metadata_config: create.metadata_config.clone(),
            index_points: create.index_points,
            format_version: Self::CURRENT_VERSION,
        }
    }
}
