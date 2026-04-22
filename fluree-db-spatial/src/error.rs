//! Error types for the spatial index.

use thiserror::Error;

/// Spatial index errors.
#[derive(Error, Debug)]
pub enum SpatialError {
    /// WKT parsing error.
    #[error("WKT parse error: {0}")]
    WktParse(String),

    /// Invalid geometry (e.g., self-intersecting polygon).
    #[error("Invalid geometry: {0}")]
    InvalidGeometry(String),

    /// S2 covering generation error.
    #[error("S2 covering error: {0}")]
    CoveringError(String),

    /// IO error during index read/write.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Chunk not found in CAS.
    #[error("Chunk not found: {0}")]
    ChunkNotFound(String),

    /// Index format error (corrupt or incompatible version).
    #[error("Index format error: {0}")]
    FormatError(String),

    /// Time range not covered by the index.
    #[error("Time range not covered: requested t={requested_t} but index base_t={base_t}")]
    TimeRangeNotCovered { requested_t: i64, base_t: i64 },

    /// Configuration error.
    #[error("Configuration error: {0}")]
    Config(String),

    /// Remote provider error.
    #[error("Remote error: {0}")]
    Remote(String),

    /// Internal error (should not happen).
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Result type for spatial operations.
pub type Result<T> = std::result::Result<T, SpatialError>;
