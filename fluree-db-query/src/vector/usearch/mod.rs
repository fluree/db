//! Embedded Vector Search via usearch
//!
//! This module provides embedded HNSW-based approximate nearest neighbor search
//! using the usearch library. It follows the same patterns as the BM25 module.
//!
//! # Module Structure
//!
//! - `index` - VectorIndex struct wrapping usearch::Index with IRI mapping
//! - `builder` - VectorIndexBuilder and IncrementalVectorUpdater
//! - `serialize` - FVEC snapshot format for persistence
//! - `error` - VectorError type
//!
//! Build an index with `VectorIndexBuilder`, search with `index.search`, and persist with `serialize`.

pub mod builder;
pub mod error;
pub mod index;
pub mod serialize;

// Re-export commonly used types
pub use builder::{IncrementalVectorUpdateResult, IncrementalVectorUpdater, VectorIndexBuilder};
pub use error::{Result, VectorError};
pub use index::{
    PointIdAssigner, VectorIndex, VectorIndexMetadata, VectorIndexOptions, VectorPropertyDeps,
    VectorSearchResult,
};
pub use serialize::{deserialize, serialize};
