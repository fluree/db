//! Vector Similarity Search Module
//!
//! This module implements vector similarity search for Fluree's graph source system.
//! It provides a provider-based abstraction that supports different backends:
//! - Embedded in-process indexes (requires `vector` feature)
//! - External services (future)
//!
//! # Architecture
//!
//! The vector search system mirrors BM25's graph source pattern:
//! - `VectorIndexProvider` trait for backend abstraction
//! - `VectorSearchOperator` for query execution
//! - `Pattern::VectorSearch` in query IR
//!
//! # Query Syntax
//!
//! Vector search uses the `f:*` pattern syntax:
//!
//! ```json
//! {
//!   "where": [{
//!     "f:graphSource": "embeddings:main",
//!     "f:queryVector": [0.1, 0.2, ...],  // or "?embedding" variable
//!     "f:distanceMetric": "cosine",          // "cosine" | "dot" | "l2"
//!     "f:searchLimit": 10,
//!     "f:searchResult": {
//!       "f:resultId": "?doc",
//!       "f:resultScore": "?score"
//!     }
//!   }]
//! }
//! ```
//!
//! # Features
//!
//! - `vector` - Enables embedded HNSW-based vector search

pub mod operator;

// Embedded vector search (HNSW-based approximate nearest neighbor)
#[cfg(feature = "vector")]
pub mod usearch;

// Re-export commonly used types
pub use operator::{VectorIndexProvider, VectorSearchHit, VectorSearchOperator};

use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Distance metric for vector similarity search
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DistanceMetric {
    /// Cosine similarity (1 - cosine_distance)
    /// Best for normalized embeddings, measures angle between vectors
    #[default]
    Cosine,
    /// Dot product (inner product)
    /// Best for embeddings where magnitude matters
    Dot,
    /// Euclidean distance (L2)
    /// Best for absolute distance in vector space
    Euclidean,
}

impl DistanceMetric {
    /// Parse from string (case-insensitive)
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "cosine" | "cos" => Some(DistanceMetric::Cosine),
            "dot" | "dotproduct" | "inner" | "ip" => Some(DistanceMetric::Dot),
            "euclidean" | "l2" | "euclid" => Some(DistanceMetric::Euclidean),
            _ => None,
        }
    }
}

impl std::str::FromStr for DistanceMetric {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s).ok_or_else(|| format!("unknown distance metric: {s}"))
    }
}

impl std::fmt::Display for DistanceMetric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DistanceMetric::Cosine => write!(f, "cosine"),
            DistanceMetric::Dot => write!(f, "dot"),
            DistanceMetric::Euclidean => write!(f, "euclidean"),
        }
    }
}

/// Parameters for vector similarity search.
///
/// This struct bundles the search parameters to reduce argument count
/// in the `VectorIndexProvider::search` trait method.
#[derive(Debug, Clone)]
pub struct VectorSearchParams<'a> {
    /// The query vector to find similar vectors for
    pub query_vector: &'a [f32],
    /// Distance metric to use
    pub metric: DistanceMetric,
    /// Maximum number of results
    pub limit: usize,
    /// Target transaction time (for time-travel queries).
    /// In dataset (multi-ledger) mode, there is no meaningful "dataset t".
    /// Callers should pass `None` unless the query provides an unambiguous
    /// as-of anchor (e.g., graph-specific time selection).
    pub as_of_t: Option<i64>,
    /// Whether to sync before querying
    pub sync: bool,
    /// Query timeout in milliseconds
    pub timeout_ms: Option<u64>,
}

impl<'a> VectorSearchParams<'a> {
    /// Create new search params with required fields and defaults for optional ones.
    pub fn new(query_vector: &'a [f32], metric: DistanceMetric, limit: usize) -> Self {
        Self {
            query_vector,
            metric,
            limit,
            as_of_t: None,
            sync: false,
            timeout_ms: None,
        }
    }

    /// Set the as-of transaction time.
    pub fn with_as_of_t(mut self, t: Option<i64>) -> Self {
        self.as_of_t = t;
        self
    }

    /// Set whether to sync before querying.
    pub fn with_sync(mut self, sync: bool) -> Self {
        self.sync = sync;
        self
    }

    /// Set the query timeout.
    pub fn with_timeout_ms(mut self, timeout_ms: Option<u64>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
}

/// Configuration for a vector index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexConfig {
    /// Source ledger alias (e.g., "docs:main")
    pub source_ledger: String,

    /// Query that selects documents and their embedding vectors
    pub query: serde_json::Value,

    /// Property path to the embedding vector (e.g., "schema:embedding")
    pub embedding_property: String,

    /// Distance metric for similarity search
    #[serde(default)]
    pub metric: DistanceMetric,

    /// Expected vector dimensions (for validation)
    pub dimensions: Option<usize>,

    /// Provider-specific configuration (e.g., index-specific settings)
    #[serde(default)]
    pub provider_config: serde_json::Value,
}

impl VectorIndexConfig {
    /// Create a new vector index configuration
    pub fn new(
        source_ledger: impl Into<String>,
        query: serde_json::Value,
        embedding_property: impl Into<String>,
    ) -> Self {
        Self {
            source_ledger: source_ledger.into(),
            query,
            embedding_property: embedding_property.into(),
            metric: DistanceMetric::default(),
            dimensions: None,
            provider_config: serde_json::Value::Null,
        }
    }

    /// Set the distance metric
    pub fn with_metric(mut self, metric: DistanceMetric) -> Self {
        self.metric = metric;
        self
    }

    /// Set expected dimensions
    pub fn with_dimensions(mut self, dimensions: usize) -> Self {
        self.dimensions = Some(dimensions);
        self
    }

    /// Set provider-specific configuration
    pub fn with_provider_config(mut self, config: serde_json::Value) -> Self {
        self.provider_config = config;
        self
    }
}

/// Document with vector embedding for indexing
#[derive(Debug, Clone)]
pub struct VectorDocument {
    /// Document IRI (subject identifier)
    pub iri: Arc<str>,
    /// Source ledger alias
    pub ledger_alias: Arc<str>,
    /// Embedding vector (f32 for efficiency)
    pub vector: Vec<f32>,
    /// Optional payload metadata
    pub payload: Option<serde_json::Value>,
}

impl VectorDocument {
    /// Create a new vector document
    pub fn new(
        iri: impl Into<Arc<str>>,
        ledger_alias: impl Into<Arc<str>>,
        vector: Vec<f32>,
    ) -> Self {
        Self {
            iri: iri.into(),
            ledger_alias: ledger_alias.into(),
            vector,
            payload: None,
        }
    }

    /// Add payload metadata
    pub fn with_payload(mut self, payload: serde_json::Value) -> Self {
        self.payload = Some(payload);
        self
    }

    /// Generate a unique point ID for this document.
    ///
    /// Uses a hash of ledger_alias + iri to ensure uniqueness across ledgers.
    pub fn point_id(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.ledger_alias.hash(&mut hasher);
        self.iri.hash(&mut hasher);
        hasher.finish()
    }
}

/// Result of creating/syncing a vector index
#[derive(Debug, Clone)]
pub struct VectorSyncResult {
    /// Number of vectors upserted
    pub upserted: usize,
    /// Number of vectors deleted
    pub deleted: usize,
    /// New watermark (transaction time)
    pub watermark: i64,
    /// Total vectors in index after sync
    pub total_vectors: Option<usize>,
}

impl VectorSyncResult {
    /// Create a new sync result
    pub fn new(upserted: usize, deleted: usize, watermark: i64) -> Self {
        Self {
            upserted,
            deleted,
            watermark,
            total_vectors: None,
        }
    }

    /// Set total vector count
    pub fn with_total(mut self, total: usize) -> Self {
        self.total_vectors = Some(total);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distance_metric_from_str() {
        assert_eq!(
            DistanceMetric::parse("cosine"),
            Some(DistanceMetric::Cosine)
        );
        assert_eq!(
            DistanceMetric::parse("COSINE"),
            Some(DistanceMetric::Cosine)
        );
        assert_eq!(DistanceMetric::parse("dot"), Some(DistanceMetric::Dot));
        assert_eq!(
            DistanceMetric::parse("dotproduct"),
            Some(DistanceMetric::Dot)
        );
        assert_eq!(DistanceMetric::parse("l2"), Some(DistanceMetric::Euclidean));
        assert_eq!(
            DistanceMetric::parse("euclidean"),
            Some(DistanceMetric::Euclidean)
        );
        assert_eq!(DistanceMetric::parse("invalid"), None);
    }

    #[test]
    fn test_vector_document_point_id() {
        let doc1 = VectorDocument::new("http://example.org/doc1", "ledger:main", vec![0.1, 0.2]);
        let doc2 = VectorDocument::new("http://example.org/doc1", "ledger:main", vec![0.3, 0.4]);
        let doc3 = VectorDocument::new("http://example.org/doc1", "other:main", vec![0.1, 0.2]);

        // Same IRI + ledger = same point ID (regardless of vector)
        assert_eq!(doc1.point_id(), doc2.point_id());
        // Different ledger = different point ID
        assert_ne!(doc1.point_id(), doc3.point_id());
    }
}
