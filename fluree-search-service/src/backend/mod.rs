//! Search backend implementations.
//!
//! This module defines the `SearchBackend` trait and provides
//! implementations for different search types:
//!
//! - [`Bm25Backend`]: BM25 text search backend
//! - [`VectorBackend`]: Vector similarity search backend
//! - [`CompositeBackend`]: Multi-type dispatch backend
//!
//! Backends handle index loading, caching, and search execution.

#[cfg(feature = "bm25")]
mod bm25;

#[cfg(feature = "vector")]
mod vector;

mod composite;

#[cfg(feature = "bm25")]
pub use bm25::{Bm25Backend, Bm25BackendConfig, IndexLoader};

#[cfg(feature = "vector")]
pub use vector::{VectorBackend, VectorBackendConfig, VectorIndexLoader};

pub use composite::CompositeBackend;

use crate::error::Result;
use async_trait::async_trait;
use fluree_search_protocol::{QueryVariant, SearchHit};

/// Backend for handling search requests.
///
/// Each backend implements a specific search type (BM25, vector, etc.)
/// and manages its own index loading and caching.
#[async_trait]
pub trait SearchBackend: std::fmt::Debug + Send + Sync {
    /// Execute a search query.
    ///
    /// # Arguments
    ///
    /// * `graph_source_id` - Graph source name
    /// * `query` - The search query variant
    /// * `limit` - Maximum number of results
    /// * `as_of_t` - Target transaction for time-travel (None = latest)
    /// * `sync` - Whether to wait for index to reach head
    /// * `timeout_ms` - Request timeout in milliseconds
    ///
    /// # Returns
    ///
    /// Tuple of (index_t, hits) where index_t is the actual transaction
    /// of the snapshot that was searched.
    async fn search(
        &self,
        graph_source_id: &str,
        query: &QueryVariant,
        limit: usize,
        as_of_t: Option<i64>,
        sync: bool,
        timeout_ms: Option<u64>,
    ) -> Result<(i64, Vec<SearchHit>)>;

    /// Check if this backend can handle the given query type.
    fn supports(&self, query: &QueryVariant) -> bool;
}
