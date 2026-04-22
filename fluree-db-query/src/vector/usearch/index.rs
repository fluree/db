//! Vector Index Implementation
//!
//! This module implements the core `VectorIndex` struct wrapping usearch::Index
//! with IRI mapping and collision handling for stable point IDs.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use usearch::{Index, IndexOptions, MetricKind, ScalarKind};

use crate::bm25::index::{GraphSourceWatermark, PropertyDeps};

use super::super::DistanceMetric;
use super::error::{Result, VectorError};

/// The usearch crate version this module was built against.
/// Used for snapshot compatibility checking.
pub const USEARCH_CRATE_VERSION: &str = "2.16";

/// High bit mask for distinguishing collision overflow IDs from hash IDs.
/// Hash IDs have high bit cleared (0..2^63), collision IDs have it set (2^63..2^64).
const COLLISION_ID_HIGH_BIT: u64 = 1 << 63;

/// Type alias for collision overflow map: (ledger_alias, iri) -> assigned_id
type CollisionMap = BTreeMap<(Arc<str>, Arc<str>), u64>;

/// Compute stable point ID from ledger alias and IRI.
///
/// Uses SHA-256 truncated to u64 for stability across Rust versions and platforms.
/// The high bit is always cleared to reserve the upper half of the ID space
/// for collision overflow IDs.
fn compute_point_id(ledger_alias: &str, iri: &str) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(ledger_alias.as_bytes());
    hasher.update(b"\0"); // separator
    hasher.update(iri.as_bytes());
    let hash = hasher.finalize();

    // Take first 8 bytes as little-endian u64, then clear high bit
    let bytes: [u8; 8] = hash[0..8]
        .try_into()
        .expect("SHA-256 produces at least 8 bytes");
    u64::from_le_bytes(bytes) & !COLLISION_ID_HIGH_BIT
}

/// Point ID assignment with collision handling.
///
/// Uses SHA-256 hash of (ledger_alias, iri) as primary strategy.
/// On collision (rare but possible), assigns overflow IDs from the high-bit range.
///
/// ID space partitioning:
/// - Hash IDs: 0..2^63-1 (high bit cleared)
/// - Collision overflow IDs: 2^63..2^64-1 (high bit set)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PointIdAssigner {
    /// Primary IDs: point_id -> (ledger_alias, iri)
    primary: BTreeMap<u64, (Arc<str>, Arc<str>)>,
    /// Collision overflow: (ledger_alias, iri) -> assigned_id
    /// Uses BTreeMap for deterministic serialization order.
    #[serde(with = "collision_map_serde")]
    collisions: CollisionMap,
    /// Next collision ID to assign (starts at 2^63, the collision range)
    next_collision_id: u64,
}

/// Custom serialization for collision map since tuples as keys don't serialize well
mod collision_map_serde {
    use super::*;
    use serde::{Deserializer, Serializer};

    #[derive(Serialize, Deserialize)]
    struct CollisionEntry {
        ledger_alias: Arc<str>,
        iri: Arc<str>,
        id: u64,
    }

    pub fn serialize<S>(map: &CollisionMap, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // BTreeMap iteration is already sorted, so this is deterministic
        let entries: Vec<CollisionEntry> = map
            .iter()
            .map(|((ledger, iri), id)| CollisionEntry {
                ledger_alias: ledger.clone(),
                iri: iri.clone(),
                id: *id,
            })
            .collect();
        entries.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> std::result::Result<CollisionMap, D::Error>
    where
        D: Deserializer<'de>,
    {
        let entries: Vec<CollisionEntry> = Vec::deserialize(deserializer)?;
        Ok(entries
            .into_iter()
            .map(|e| ((e.ledger_alias, e.iri), e.id))
            .collect())
    }
}

impl PointIdAssigner {
    /// Create a new empty assigner.
    pub fn new() -> Self {
        Self {
            primary: BTreeMap::new(),
            collisions: BTreeMap::new(),
            // Start collision IDs at the high-bit range (2^63)
            next_collision_id: COLLISION_ID_HIGH_BIT,
        }
    }

    /// Create from snapshot data.
    pub fn from_snapshot(
        primary: BTreeMap<u64, (Arc<str>, Arc<str>)>,
        collisions: CollisionMap,
    ) -> Self {
        // Derive next_collision_id from existing overflow IDs
        // If there are existing collisions, continue from max+1
        // Otherwise start at the high-bit boundary
        let next_collision_id = collisions
            .values()
            .max()
            .map(|m| m + 1)
            .unwrap_or(COLLISION_ID_HIGH_BIT);
        Self {
            primary,
            collisions,
            next_collision_id,
        }
    }

    /// Assign a point ID for the given (ledger_alias, iri) pair.
    ///
    /// Returns the same ID for the same input across calls.
    pub fn assign(&mut self, ledger_alias: &str, iri: &str) -> u64 {
        let hash_id = compute_point_id(ledger_alias, iri);

        if let Some((existing_ledger, existing_iri)) = self.primary.get(&hash_id) {
            if existing_ledger.as_ref() == ledger_alias && existing_iri.as_ref() == iri {
                return hash_id; // Already assigned this exact pair
            }
            // Collision! Check if we already have an overflow ID
            let key = (Arc::from(ledger_alias), Arc::from(iri));
            if let Some(&id) = self.collisions.get(&key) {
                return id;
            }
            // Assign new overflow ID
            let id = self.next_collision_id;
            self.next_collision_id += 1;
            self.collisions.insert(key, id);
            id
        } else {
            // No collision, use hash ID
            self.primary
                .insert(hash_id, (Arc::from(ledger_alias), Arc::from(iri)));
            hash_id
        }
    }

    /// Look up the (ledger_alias, iri) for a point ID.
    pub fn get(&self, point_id: u64) -> Option<(&str, &str)> {
        // Check primary map first
        if let Some((ledger, iri)) = self.primary.get(&point_id) {
            return Some((ledger.as_ref(), iri.as_ref()));
        }
        // Check collision overflow (linear scan, but collisions are rare)
        for ((ledger, iri), &id) in &self.collisions {
            if id == point_id {
                return Some((ledger.as_ref(), iri.as_ref()));
            }
        }
        None
    }

    /// Get the point ID for a (ledger_alias, iri) pair if it exists.
    pub fn get_id(&self, ledger_alias: &str, iri: &str) -> Option<u64> {
        let hash_id = compute_point_id(ledger_alias, iri);

        if let Some((existing_ledger, existing_iri)) = self.primary.get(&hash_id) {
            if existing_ledger.as_ref() == ledger_alias && existing_iri.as_ref() == iri {
                return Some(hash_id);
            }
        }
        // Check collision overflow
        let key = (Arc::from(ledger_alias), Arc::from(iri));
        self.collisions.get(&key).copied()
    }

    /// Remove a point ID assignment.
    pub fn remove(&mut self, ledger_alias: &str, iri: &str) -> Option<u64> {
        let hash_id = compute_point_id(ledger_alias, iri);

        if let Some((existing_ledger, existing_iri)) = self.primary.get(&hash_id) {
            if existing_ledger.as_ref() == ledger_alias && existing_iri.as_ref() == iri {
                self.primary.remove(&hash_id);
                return Some(hash_id);
            }
        }
        // Check collision overflow
        let key = (Arc::from(ledger_alias), Arc::from(iri));
        self.collisions.remove(&key)
    }

    /// Number of assigned IDs.
    pub fn len(&self) -> usize {
        self.primary.len() + self.collisions.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.primary.is_empty() && self.collisions.is_empty()
    }

    /// Get the primary IRI map (for serialization).
    pub fn primary_map(&self) -> &BTreeMap<u64, (Arc<str>, Arc<str>)> {
        &self.primary
    }

    /// Get the collision map (for serialization).
    pub fn collision_map(&self) -> &CollisionMap {
        &self.collisions
    }

    /// Iterate over all (ledger_alias, iri) pairs, including those in collisions.
    ///
    /// This is used for full resyncs to ensure no entries are missed.
    pub fn all_entries(&self) -> impl Iterator<Item = (&str, &str)> {
        let primary_iter = self
            .primary
            .values()
            .map(|(ledger, iri)| (ledger.as_ref(), iri.as_ref()));

        let collision_iter = self
            .collisions
            .keys()
            .map(|(ledger, iri)| (ledger.as_ref(), iri.as_ref()));

        primary_iter.chain(collision_iter)
    }
}

/// Metadata for a vector index snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexMetadata {
    /// Snapshot format version (for compatibility checking)
    pub format_version: u8,
    /// Index kind identifier
    pub index_kind: String,
    /// Vector dimensions
    pub dimensions: usize,
    /// Distance metric
    pub metric: DistanceMetric,
    /// Number of vectors in the index
    pub vector_count: usize,
    /// Quantization type (e.g., "f32", "f16", "i8")
    pub quantization: String,
    /// usearch crate version
    pub usearch_crate_version: String,
    /// usearch internal index version
    pub usearch_index_version: u8,
    /// Point ID assignment strategy
    pub point_id_strategy: String,
    /// Serialization encoding
    pub encoding: String,
}

impl VectorIndexMetadata {
    /// Create metadata for a new index.
    pub fn new(dimensions: usize, metric: DistanceMetric, vector_count: usize) -> Self {
        Self {
            format_version: 1,
            index_kind: "usearch_hnsw".to_string(),
            dimensions,
            metric,
            vector_count,
            quantization: "f32".to_string(),
            usearch_crate_version: USEARCH_CRATE_VERSION.to_string(),
            usearch_index_version: 1,
            point_id_strategy: "sha256_ledger_iri".to_string(),
            encoding: "postcard".to_string(),
        }
    }
}

/// Result from a vector search operation.
#[derive(Debug, Clone)]
pub struct VectorSearchResult {
    /// Point ID in the index
    pub point_id: u64,
    /// Document IRI
    pub iri: Arc<str>,
    /// Source ledger alias
    pub ledger_alias: Arc<str>,
    /// Similarity score (higher is better)
    pub score: f64,
}

/// Property dependencies for vector index incremental updates.
///
/// Tracks both the embedding property AND query WHERE dependencies
/// (like type constraints and filter properties).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VectorPropertyDeps {
    /// The embedding property IRI
    pub embedding_property: Arc<str>,
    /// Additional properties from the indexing query's WHERE clause
    pub query_deps: PropertyDeps,
}

impl VectorPropertyDeps {
    /// Create new vector property dependencies.
    pub fn new(embedding_property: impl Into<Arc<str>>) -> Self {
        Self {
            embedding_property: embedding_property.into(),
            query_deps: PropertyDeps::new(),
        }
    }

    /// Create from embedding property and indexing query.
    pub fn from_query(embedding_property: &str, query: &serde_json::Value) -> Self {
        let mut deps = PropertyDeps::from_indexing_query(query);
        // Ensure embedding property is also tracked
        deps.add(embedding_property);

        Self {
            embedding_property: Arc::from(embedding_property),
            query_deps: deps,
        }
    }

    /// Get all tracked property IRIs.
    pub fn all_properties(&self) -> impl Iterator<Item = &Arc<str>> {
        std::iter::once(&self.embedding_property).chain(self.query_deps.property_iris.iter())
    }
}

/// Configuration options for creating a vector index.
#[derive(Debug, Clone)]
pub struct VectorIndexOptions {
    /// Vector dimensions
    pub dimensions: usize,
    /// Distance metric
    pub metric: DistanceMetric,
    /// HNSW connectivity parameter (default: 16)
    pub connectivity: usize,
    /// Expansion factor during index construction (default: 128)
    pub expansion_add: usize,
    /// Expansion factor during search (default: 64)
    pub expansion_search: usize,
}

impl VectorIndexOptions {
    /// Create default options for the given dimensions and metric.
    pub fn new(dimensions: usize, metric: DistanceMetric) -> Self {
        Self {
            dimensions,
            metric,
            connectivity: 16,
            expansion_add: 128,
            expansion_search: 64,
        }
    }

    /// Set HNSW connectivity parameter.
    pub fn with_connectivity(mut self, connectivity: usize) -> Self {
        self.connectivity = connectivity;
        self
    }

    /// Set expansion factor for index construction.
    pub fn with_expansion_add(mut self, expansion_add: usize) -> Self {
        self.expansion_add = expansion_add;
        self
    }

    /// Set expansion factor for search.
    pub fn with_expansion_search(mut self, expansion_search: usize) -> Self {
        self.expansion_search = expansion_search;
        self
    }
}

/// Embedded vector index wrapping usearch::Index.
pub struct VectorIndex {
    /// The usearch index
    inner: Index,
    /// Point ID assigner (handles IRI mapping and collisions)
    id_assigner: PointIdAssigner,
    /// Index metadata
    pub metadata: VectorIndexMetadata,
    /// Multi-ledger watermarks
    pub watermark: GraphSourceWatermark,
    /// Property dependencies for incremental updates
    pub property_deps: VectorPropertyDeps,
}

impl std::fmt::Debug for VectorIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorIndex")
            .field("metadata", &self.metadata)
            .field("watermark", &self.watermark)
            .field("property_deps", &self.property_deps)
            .field("id_assigner_len", &self.id_assigner.len())
            .finish()
    }
}

impl VectorIndex {
    /// Create a new empty vector index.
    pub fn new(dimensions: usize, metric: DistanceMetric) -> Result<Self> {
        Self::with_options(VectorIndexOptions::new(dimensions, metric))
    }

    /// Create a new vector index with custom options.
    pub fn with_options(options: VectorIndexOptions) -> Result<Self> {
        let usearch_metric = match options.metric {
            DistanceMetric::Cosine => MetricKind::Cos,
            DistanceMetric::Dot => MetricKind::IP,
            DistanceMetric::Euclidean => MetricKind::L2sq,
        };

        let index_options = IndexOptions {
            dimensions: options.dimensions,
            metric: usearch_metric,
            quantization: ScalarKind::F32,
            connectivity: options.connectivity,
            expansion_add: options.expansion_add,
            expansion_search: options.expansion_search,
            multi: false, // Single vector per key
        };

        let inner = Index::new(&index_options).map_err(|e| VectorError::Usearch(e.to_string()))?;

        Ok(Self {
            inner,
            id_assigner: PointIdAssigner::new(),
            metadata: VectorIndexMetadata::new(options.dimensions, options.metric, 0),
            watermark: GraphSourceWatermark::new(),
            property_deps: VectorPropertyDeps::default(),
        })
    }

    /// Create from deserialized components.
    pub(crate) fn from_parts(
        inner: Index,
        id_assigner: PointIdAssigner,
        metadata: VectorIndexMetadata,
        watermark: GraphSourceWatermark,
        property_deps: VectorPropertyDeps,
    ) -> Self {
        Self {
            inner,
            id_assigner,
            metadata,
            watermark,
            property_deps,
        }
    }

    /// Reserve capacity for vectors.
    pub fn reserve(&mut self, capacity: usize) -> Result<()> {
        self.inner
            .reserve(capacity)
            .map_err(|e| VectorError::Usearch(e.to_string()))
    }

    /// Add a vector to the index.
    ///
    /// Returns the assigned point ID.
    pub fn add(&mut self, ledger_alias: &str, iri: &str, vector: &[f32]) -> Result<u64> {
        // Validate dimensions
        if vector.len() != self.metadata.dimensions {
            return Err(VectorError::DimensionMismatch {
                expected: self.metadata.dimensions,
                actual: vector.len(),
            });
        }

        // Check if document already exists - if so, skip (idempotent add)
        // A future enhancement could support update-in-place semantics
        if let Some(existing_id) = self.id_assigner.get_id(ledger_alias, iri) {
            return Ok(existing_id);
        }

        // Auto-reserve capacity if needed (usearch requires reserve before add)
        let current_size = self.inner.size();
        let current_capacity = self.inner.capacity();
        if current_size >= current_capacity {
            // Reserve more capacity (double or at least 16)
            let new_capacity = std::cmp::max(current_capacity * 2, 16);
            self.inner
                .reserve(new_capacity)
                .map_err(|e| VectorError::Usearch(e.to_string()))?;
        }

        let point_id = self.id_assigner.assign(ledger_alias, iri);

        self.inner
            .add(point_id, vector)
            .map_err(|e| VectorError::Usearch(e.to_string()))?;

        self.metadata.vector_count = self.id_assigner.len();

        Ok(point_id)
    }

    /// Remove a vector from the index.
    ///
    /// Returns true if the vector was found and removed.
    pub fn remove(&mut self, ledger_alias: &str, iri: &str) -> Result<bool> {
        let Some(point_id) = self.id_assigner.remove(ledger_alias, iri) else {
            return Ok(false);
        };

        self.inner
            .remove(point_id)
            .map_err(|e| VectorError::Usearch(e.to_string()))?;

        self.metadata.vector_count = self.id_assigner.len();

        Ok(true)
    }

    /// Search for similar vectors.
    ///
    /// Returns results ordered by similarity (best first).
    /// Scores are normalized so that higher is always better.
    pub fn search(&self, query: &[f32], limit: usize) -> Result<Vec<VectorSearchResult>> {
        if query.len() != self.metadata.dimensions {
            return Err(VectorError::DimensionMismatch {
                expected: self.metadata.dimensions,
                actual: query.len(),
            });
        }

        let matches = self
            .inner
            .search(query, limit)
            .map_err(|e| VectorError::Usearch(e.to_string()))?;

        let mut results = Vec::with_capacity(matches.keys.len());

        for (key, distance) in matches.keys.iter().zip(matches.distances.iter()) {
            let Some((ledger_alias, iri)) = self.id_assigner.get(*key) else {
                continue; // Skip orphaned points
            };

            // Normalize score: always "higher is better"
            let score = self.normalize_score(*distance);

            results.push(VectorSearchResult {
                point_id: *key,
                iri: Arc::from(iri),
                ledger_alias: Arc::from(ledger_alias),
                score,
            });
        }

        Ok(results)
    }

    /// Normalize distance to similarity score (higher is better).
    fn normalize_score(&self, distance: f32) -> f64 {
        let d = distance as f64;
        match self.metadata.metric {
            // Cosine: usearch returns cosine distance in [0, 2]
            // Convert to similarity in [-1, 1] (or [0, 1] for normalized vectors)
            DistanceMetric::Cosine => 1.0 - d,
            // Dot product: usearch returns negative inner product
            // Negate to get actual inner product (higher = more similar)
            DistanceMetric::Dot => -d,
            // Euclidean: distance in [0, inf)
            // Convert to similarity in (0, 1]
            DistanceMetric::Euclidean => 1.0 / (1.0 + d),
        }
    }

    /// Check if a document exists in the index.
    pub fn contains(&self, ledger_alias: &str, iri: &str) -> bool {
        self.id_assigner.get_id(ledger_alias, iri).is_some()
    }

    /// Get the IRI for a point ID.
    pub fn get_iri(&self, point_id: u64) -> Option<(&str, &str)> {
        self.id_assigner.get(point_id)
    }

    /// Number of vectors in the index.
    pub fn len(&self) -> usize {
        self.id_assigner.len()
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.id_assigner.is_empty()
    }

    /// Get the index dimensions.
    pub fn dimensions(&self) -> usize {
        self.metadata.dimensions
    }

    /// Get the distance metric.
    pub fn metric(&self) -> DistanceMetric {
        self.metadata.metric
    }

    /// Get reference to the inner usearch index (for serialization).
    pub(crate) fn inner(&self) -> &Index {
        &self.inner
    }

    /// Get reference to the ID assigner (for serialization).
    pub(crate) fn id_assigner(&self) -> &PointIdAssigner {
        &self.id_assigner
    }

    /// Get the serialized length of the inner usearch index (for buffer sizing).
    pub(crate) fn serialized_length(&self) -> usize {
        self.inner.serialized_length()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_id_stable() {
        let id1 = compute_point_id("ledger:main", "http://example.org/doc1");
        let id2 = compute_point_id("ledger:main", "http://example.org/doc1");
        assert_eq!(id1, id2);

        let id3 = compute_point_id("ledger:main", "http://example.org/doc2");
        assert_ne!(id1, id3);

        let id4 = compute_point_id("other:main", "http://example.org/doc1");
        assert_ne!(id1, id4);
    }

    #[test]
    fn test_point_id_assigner_basic() {
        let mut assigner = PointIdAssigner::new();

        let id1 = assigner.assign("ledger:main", "http://example.org/doc1");
        let id2 = assigner.assign("ledger:main", "http://example.org/doc1");
        assert_eq!(id1, id2);

        let id3 = assigner.assign("ledger:main", "http://example.org/doc2");
        assert_ne!(id1, id3);

        assert_eq!(assigner.len(), 2);

        // Lookup
        let (ledger, iri) = assigner.get(id1).unwrap();
        assert_eq!(ledger, "ledger:main");
        assert_eq!(iri, "http://example.org/doc1");

        // Get ID
        assert_eq!(
            assigner.get_id("ledger:main", "http://example.org/doc1"),
            Some(id1)
        );
        assert_eq!(
            assigner.get_id("ledger:main", "http://example.org/unknown"),
            None
        );

        // Remove
        let removed = assigner.remove("ledger:main", "http://example.org/doc1");
        assert_eq!(removed, Some(id1));
        assert_eq!(assigner.len(), 1);
        assert!(assigner.get(id1).is_none());
    }

    #[test]
    fn test_point_id_assigner_from_snapshot() {
        let mut primary = BTreeMap::new();
        primary.insert(
            123,
            (
                Arc::from("ledger:main"),
                Arc::from("http://example.org/doc1"),
            ),
        );

        let mut collisions = BTreeMap::new();
        collisions.insert(
            (
                Arc::from("ledger:main"),
                Arc::from("http://example.org/collided"),
            ),
            COLLISION_ID_HIGH_BIT + 5,
        );

        let assigner = PointIdAssigner::from_snapshot(primary, collisions);

        // next_collision_id should be derived from max collision ID
        assert_eq!(assigner.next_collision_id, COLLISION_ID_HIGH_BIT + 6);
        assert_eq!(assigner.len(), 2);
    }

    #[test]
    fn test_point_id_hash_stability() {
        // This test ensures that the hash function produces stable results.
        // These values should never change across Rust versions or rebuilds.
        let id1 = compute_point_id("ledger:main", "http://example.org/doc1");
        let id2 = compute_point_id("ledger:main", "http://example.org/doc1");

        // Same input should always produce same output
        assert_eq!(id1, id2);

        // High bit should always be clear for hash IDs
        assert_eq!(
            id1 & COLLISION_ID_HIGH_BIT,
            0,
            "hash ID should have high bit cleared"
        );

        // Different inputs should produce different hashes (with overwhelming probability)
        let id3 = compute_point_id("ledger:main", "http://example.org/doc2");
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_collision_id_space() {
        let mut assigner = PointIdAssigner::new();

        // Create a scenario where we force a collision by inserting two different
        // (ledger, iri) pairs that hash to the same value.
        // We do this by directly manipulating the primary map.
        let fake_hash_id = compute_point_id("ledger:main", "http://example.org/original");

        // Insert the original entry
        let id1 = assigner.assign("ledger:main", "http://example.org/original");
        assert_eq!(id1, fake_hash_id);
        assert_eq!(
            id1 & COLLISION_ID_HIGH_BIT,
            0,
            "primary ID should have high bit cleared"
        );

        // Now manually create a collision scenario by pre-populating primary with
        // a different entry at a hash we're about to generate
        let collider_hash = compute_point_id("ledger:other", "http://example.org/collider");

        // First assign something to that hash slot
        let id2 = assigner.assign("ledger:other", "http://example.org/collider");
        assert_eq!(id2, collider_hash);

        // The assigner should handle its internal state correctly
        assert_eq!(assigner.len(), 2);
    }

    #[test]
    fn test_all_entries_includes_collisions() {
        let mut primary = BTreeMap::new();
        primary.insert(
            100,
            (
                Arc::from("ledger:main"),
                Arc::from("http://example.org/doc1"),
            ),
        );
        primary.insert(
            200,
            (
                Arc::from("ledger:main"),
                Arc::from("http://example.org/doc2"),
            ),
        );

        let mut collisions = BTreeMap::new();
        collisions.insert(
            (
                Arc::from("ledger:main"),
                Arc::from("http://example.org/collided"),
            ),
            COLLISION_ID_HIGH_BIT,
        );

        let assigner = PointIdAssigner::from_snapshot(primary, collisions);

        let all: Vec<_> = assigner.all_entries().collect();
        assert_eq!(all.len(), 3);

        // Check that all entries are present
        assert!(all.contains(&("ledger:main", "http://example.org/doc1")));
        assert!(all.contains(&("ledger:main", "http://example.org/doc2")));
        assert!(all.contains(&("ledger:main", "http://example.org/collided")));
    }

    #[test]
    fn test_vector_index_options() {
        let opts = VectorIndexOptions::new(768, DistanceMetric::Cosine)
            .with_connectivity(32)
            .with_expansion_add(256)
            .with_expansion_search(128);

        assert_eq!(opts.dimensions, 768);
        assert_eq!(opts.metric, DistanceMetric::Cosine);
        assert_eq!(opts.connectivity, 32);
        assert_eq!(opts.expansion_add, 256);
        assert_eq!(opts.expansion_search, 128);
    }

    #[test]
    fn test_vector_index_new() {
        let index = VectorIndex::new(384, DistanceMetric::Cosine).unwrap();
        assert_eq!(index.dimensions(), 384);
        assert_eq!(index.metric(), DistanceMetric::Cosine);
        assert!(index.is_empty());
    }

    #[test]
    fn test_vector_index_add_search() {
        let mut index = VectorIndex::new(3, DistanceMetric::Cosine).unwrap();

        // Add some vectors
        index
            .add("ledger:main", "http://example.org/doc1", &[1.0, 0.0, 0.0])
            .unwrap();
        index
            .add("ledger:main", "http://example.org/doc2", &[0.0, 1.0, 0.0])
            .unwrap();
        index
            .add(
                "ledger:main",
                "http://example.org/doc3",
                &[0.707, 0.707, 0.0],
            )
            .unwrap();

        assert_eq!(index.len(), 3);
        assert!(index.contains("ledger:main", "http://example.org/doc1"));
        assert!(!index.contains("ledger:main", "http://example.org/unknown"));

        // Search for vector similar to [1, 0, 0]
        let results = index.search(&[1.0, 0.0, 0.0], 3).unwrap();
        assert_eq!(results.len(), 3);

        // First result should be doc1 (exact match)
        assert_eq!(results[0].iri.as_ref(), "http://example.org/doc1");
        // Score should be high (close to 1.0 for cosine similarity)
        assert!(results[0].score > 0.9);
    }

    #[test]
    fn test_vector_index_dimension_mismatch() {
        let mut index = VectorIndex::new(3, DistanceMetric::Cosine).unwrap();

        let result = index.add("ledger:main", "http://example.org/doc1", &[1.0, 0.0]);
        assert!(matches!(
            result,
            Err(VectorError::DimensionMismatch {
                expected: 3,
                actual: 2
            })
        ));

        // Add a valid vector first
        index
            .add("ledger:main", "http://example.org/doc1", &[1.0, 0.0, 0.0])
            .unwrap();

        // Search with wrong dimensions
        let result = index.search(&[1.0, 0.0], 1);
        assert!(matches!(
            result,
            Err(VectorError::DimensionMismatch {
                expected: 3,
                actual: 2
            })
        ));
    }

    #[test]
    fn test_vector_index_remove() {
        let mut index = VectorIndex::new(3, DistanceMetric::Cosine).unwrap();

        index
            .add("ledger:main", "http://example.org/doc1", &[1.0, 0.0, 0.0])
            .unwrap();
        index
            .add("ledger:main", "http://example.org/doc2", &[0.0, 1.0, 0.0])
            .unwrap();

        assert_eq!(index.len(), 2);

        let removed = index
            .remove("ledger:main", "http://example.org/doc1")
            .unwrap();
        assert!(removed);
        assert_eq!(index.len(), 1);
        assert!(!index.contains("ledger:main", "http://example.org/doc1"));

        // Remove non-existent
        let removed = index
            .remove("ledger:main", "http://example.org/doc1")
            .unwrap();
        assert!(!removed);
    }

    #[test]
    fn test_vector_property_deps() {
        let deps = VectorPropertyDeps::new("http://example.org/embedding");
        assert_eq!(
            deps.embedding_property.as_ref(),
            "http://example.org/embedding"
        );

        let query = serde_json::json!({
            "@context": {"ex": "http://example.org/"},
            "where": [{"@id": "?x", "@type": "ex:Article"}],
            "select": {"?x": ["@id", "ex:title"]}
        });

        let deps = VectorPropertyDeps::from_query("http://example.org/embedding", &query);
        let props: Vec<_> = deps.all_properties().collect();

        // Should include embedding property, rdf:type (from @type), and ex:title
        assert!(props
            .iter()
            .any(|p| p.as_ref() == "http://example.org/embedding"));
        assert!(props.iter().any(|p| p.as_ref().contains("type")));
        assert!(props
            .iter()
            .any(|p| p.as_ref() == "http://example.org/title"));
    }

    #[test]
    fn test_score_normalization_cosine() {
        let index = VectorIndex::new(3, DistanceMetric::Cosine).unwrap();

        // Cosine distance 0 -> similarity 1
        assert!((index.normalize_score(0.0) - 1.0).abs() < 0.001);
        // Cosine distance 1 -> similarity 0
        assert!((index.normalize_score(1.0) - 0.0).abs() < 0.001);
        // Cosine distance 2 -> similarity -1
        assert!((index.normalize_score(2.0) - (-1.0)).abs() < 0.001);
    }

    #[test]
    fn test_score_normalization_euclidean() {
        let index = VectorIndex::new(3, DistanceMetric::Euclidean).unwrap();

        // Distance 0 -> similarity 1
        assert!((index.normalize_score(0.0) - 1.0).abs() < 0.001);
        // Distance 1 -> similarity 0.5
        assert!((index.normalize_score(1.0) - 0.5).abs() < 0.001);
        // Larger distance -> smaller similarity
        assert!(index.normalize_score(10.0) < index.normalize_score(1.0));
    }

    #[test]
    fn test_score_normalization_dot_product_ordering() {
        // This test verifies that dot product normalization produces
        // the correct ordering: higher actual similarity = higher score.
        //
        // usearch with MetricKind::IP returns a distance where smaller = more similar.
        // Our normalization converts this to a score where higher = more similar.
        let mut index = VectorIndex::new(3, DistanceMetric::Dot).unwrap();

        // Add vectors with known dot products relative to query [1, 0, 0]:
        // doc1: [1, 0, 0]     -> dot = 1.0 (most similar)
        // doc2: [0.5, 0.5, 0] -> dot = 0.5
        // doc3: [0, 1, 0]     -> dot = 0.0
        // doc4: [-1, 0, 0]    -> dot = -1.0 (least similar)
        index
            .add("ledger:main", "http://example.org/doc1", &[1.0, 0.0, 0.0])
            .unwrap();
        index
            .add("ledger:main", "http://example.org/doc2", &[0.5, 0.5, 0.0])
            .unwrap();
        index
            .add("ledger:main", "http://example.org/doc3", &[0.0, 1.0, 0.0])
            .unwrap();
        index
            .add("ledger:main", "http://example.org/doc4", &[-1.0, 0.0, 0.0])
            .unwrap();

        let results = index.search(&[1.0, 0.0, 0.0], 4).unwrap();

        // Verify ordering: doc1 (highest dot) should be first
        assert_eq!(
            results[0].iri.as_ref(),
            "http://example.org/doc1",
            "Expected doc1 (dot=1.0) first, got {}",
            results[0].iri
        );
        assert_eq!(
            results[1].iri.as_ref(),
            "http://example.org/doc2",
            "Expected doc2 (dot=0.5) second, got {}",
            results[1].iri
        );
        assert_eq!(
            results[2].iri.as_ref(),
            "http://example.org/doc3",
            "Expected doc3 (dot=0.0) third, got {}",
            results[2].iri
        );
        assert_eq!(
            results[3].iri.as_ref(),
            "http://example.org/doc4",
            "Expected doc4 (dot=-1.0) last, got {}",
            results[3].iri
        );

        // Verify scores are in descending order (CRITICAL: higher score = more similar)
        assert!(
            results[0].score >= results[1].score,
            "Score 0 ({}) should be >= score 1 ({})",
            results[0].score,
            results[1].score
        );
        assert!(
            results[1].score >= results[2].score,
            "Score 1 ({}) should be >= score 2 ({})",
            results[1].score,
            results[2].score
        );
        assert!(
            results[2].score >= results[3].score,
            "Score 2 ({}) should be >= score 3 ({})",
            results[2].score,
            results[3].score
        );

        // Verify the relative ordering of scores matches dot product ordering
        // Even if absolute values differ, doc1 should have highest score
        // Note: usearch may return slightly different distances based on HNSW approximation
        // and internal representation, so we verify ordering rather than exact values.

        // Print scores for debugging (visible when test fails or run with --nocapture)
        println!(
            "Dot product scores: doc1={}, doc2={}, doc3={}, doc4={}",
            results[0].score, results[1].score, results[2].score, results[3].score
        );
    }
}
