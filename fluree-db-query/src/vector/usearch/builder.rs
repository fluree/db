//! Vector Index Builder
//!
//! Provides functionality for building vector indexes from query results.
//! This is used during initial index creation and full rebuilds.
//!
//! # Building an Index
//!
//! The builder takes query results (JSON-LD items) and builds an index:
//!
//! 1. Extract subject IRI from each result (`@id` field)
//! 2. Extract embedding vector from the configured property
//! 3. Validate dimensions match expected value
//! 4. Add document to the vector index
//!
//! Create a `VectorIndexBuilder`, call `add_results` with JSON-LD items containing embedding arrays, then `build` to produce the index.

use std::collections::HashSet;
use std::sync::Arc;

use serde_json::Value;
use tracing::warn;

use super::super::DistanceMetric;
use super::error::Result;
use super::index::{VectorIndex, VectorIndexOptions, VectorPropertyDeps};

/// Result of an extraction attempt.
#[derive(Debug)]
pub enum ExtractionResult {
    /// Successfully extracted a vector.
    Success(Vec<f32>),
    /// Missing embedding property (skip with warning).
    MissingProperty,
    /// Property value is not an array (skip with warning).
    NotArray,
    /// Array contains non-numeric values (skip with warning).
    InvalidElement,
    /// Dimension mismatch (skip with warning).
    DimensionMismatch { expected: usize, actual: usize },
}

/// Extracts embedding vectors from query results.
pub struct VectorExtractor {
    /// The embedding property path (e.g., "schema:embedding")
    embedding_property: String,
    /// Expected vector dimensions
    expected_dimensions: usize,
}

impl VectorExtractor {
    /// Create a new vector extractor.
    pub fn new(embedding_property: impl Into<String>, expected_dimensions: usize) -> Self {
        Self {
            embedding_property: embedding_property.into(),
            expected_dimensions,
        }
    }

    /// Extract a vector from a query result.
    ///
    /// Handles JSON-LD framing where the value may be `{"@value": [...]}`.
    pub fn extract(&self, result: &Value) -> ExtractionResult {
        let embedding = match result.get(&self.embedding_property) {
            Some(v) => v,
            None => return ExtractionResult::MissingProperty,
        };

        // Handle JSON-LD framing: {"@value": [...]}
        let array = if let Some(obj) = embedding.as_object() {
            obj.get("@value").and_then(|v| v.as_array())
        } else {
            embedding.as_array()
        };

        let Some(array) = array else {
            return ExtractionResult::NotArray;
        };

        // Convert to f32 vector
        let vector: Option<Vec<f32>> = array.iter().map(|v| v.as_f64().map(|f| f as f32)).collect();

        let Some(vector) = vector else {
            return ExtractionResult::InvalidElement;
        };

        // Check dimensions
        if vector.len() != self.expected_dimensions {
            return ExtractionResult::DimensionMismatch {
                expected: self.expected_dimensions,
                actual: vector.len(),
            };
        }

        ExtractionResult::Success(vector)
    }
}

/// Builder for creating vector indexes from query results.
pub struct VectorIndexBuilder {
    /// The ledger alias for documents being indexed
    ledger_alias: Arc<str>,
    /// The vector index being built
    index: VectorIndex,
    /// Vector extractor
    extractor: VectorExtractor,
    /// Count of documents successfully indexed
    indexed_count: usize,
    /// Count of documents skipped (extraction errors)
    skipped_count: usize,
}

impl VectorIndexBuilder {
    /// Create a new builder for the given ledger alias.
    pub fn new(
        ledger_alias: impl Into<Arc<str>>,
        dimensions: usize,
        metric: DistanceMetric,
    ) -> Result<Self> {
        let index = VectorIndex::new(dimensions, metric)?;

        Ok(Self {
            ledger_alias: ledger_alias.into(),
            index,
            extractor: VectorExtractor::new("embedding", dimensions),
            indexed_count: 0,
            skipped_count: 0,
        })
    }

    /// Create a new builder with custom index options.
    pub fn with_options(
        ledger_alias: impl Into<Arc<str>>,
        options: VectorIndexOptions,
    ) -> Result<Self> {
        let dimensions = options.dimensions;
        let index = VectorIndex::with_options(options)?;

        Ok(Self {
            ledger_alias: ledger_alias.into(),
            index,
            extractor: VectorExtractor::new("embedding", dimensions),
            indexed_count: 0,
            skipped_count: 0,
        })
    }

    /// Set the embedding property name.
    pub fn with_embedding_property(mut self, property: impl Into<String>) -> Self {
        self.extractor = VectorExtractor::new(property, self.index.dimensions());
        self
    }

    /// Set property dependencies for incremental updates.
    pub fn with_property_deps(mut self, deps: VectorPropertyDeps) -> Self {
        self.index.property_deps = deps;
        self
    }

    /// Set the initial watermark for the ledger.
    pub fn with_watermark(mut self, t: i64) -> Self {
        self.index.watermark.update(self.ledger_alias.as_ref(), t);
        self
    }

    /// Reserve capacity for the expected number of vectors.
    pub fn with_capacity(mut self, capacity: usize) -> Result<Self> {
        self.index.reserve(capacity)?;
        Ok(self)
    }

    /// Add a single query result to the index.
    ///
    /// The result should be a JSON-LD object with an `@id` field and the
    /// configured embedding property.
    ///
    /// Returns `Ok(true)` if the document was indexed, `Ok(false)` if skipped.
    pub fn add_result(&mut self, result: &Value) -> Result<bool> {
        // Extract @id from the result
        let subject_iri = match self.extract_subject_iri(result) {
            Some(iri) => iri,
            None => {
                self.skipped_count += 1;
                return Ok(false);
            }
        };

        // Extract embedding vector
        match self.extractor.extract(result) {
            ExtractionResult::Success(vector) => {
                self.index
                    .add(self.ledger_alias.as_ref(), &subject_iri, &vector)?;
                self.indexed_count += 1;
                Ok(true)
            }
            ExtractionResult::MissingProperty => {
                warn!(
                    iri = %subject_iri,
                    property = %self.extractor.embedding_property,
                    "Skipping document: missing embedding property"
                );
                self.skipped_count += 1;
                Ok(false)
            }
            ExtractionResult::NotArray => {
                warn!(
                    iri = %subject_iri,
                    property = %self.extractor.embedding_property,
                    "Skipping document: embedding is not an array"
                );
                self.skipped_count += 1;
                Ok(false)
            }
            ExtractionResult::InvalidElement => {
                warn!(
                    iri = %subject_iri,
                    property = %self.extractor.embedding_property,
                    "Skipping document: embedding contains non-numeric values"
                );
                self.skipped_count += 1;
                Ok(false)
            }
            ExtractionResult::DimensionMismatch { expected, actual } => {
                warn!(
                    iri = %subject_iri,
                    expected = expected,
                    actual = actual,
                    "Skipping document: dimension mismatch"
                );
                self.skipped_count += 1;
                Ok(false)
            }
        }
    }

    /// Add multiple query results to the index.
    pub fn add_results(&mut self, results: &[Value]) -> Result<()> {
        for result in results {
            self.add_result(result)?;
        }
        Ok(())
    }

    /// Add results from a JSON array value.
    pub fn add_results_value(&mut self, results: &Value) -> Result<()> {
        match results {
            Value::Array(arr) => {
                for result in arr {
                    self.add_result(result)?;
                }
            }
            Value::Object(_) => {
                self.add_result(results)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Extract the subject IRI from a query result.
    fn extract_subject_iri(&self, result: &Value) -> Option<String> {
        // Try direct @id string
        if let Some(Value::String(s)) = result.get("@id") {
            return Some(s.clone());
        }

        // Try @id as object with nested @id
        if let Some(Value::Object(obj)) = result.get("@id") {
            if let Some(s) = obj.get("@id").and_then(|v| v.as_str()) {
                return Some(s.to_string());
            }
        }

        // Try variable binding like "?x": {"@id": "..."}
        if let Value::Object(map) = result {
            for (k, v) in map {
                if !k.starts_with('?') {
                    continue;
                }
                if let Value::Object(obj) = v {
                    if let Some(id) = obj.get("@id").and_then(|vv| vv.as_str()) {
                        return Some(id.to_string());
                    }
                }
            }
        }

        None
    }

    /// Get the number of documents successfully indexed.
    pub fn indexed_count(&self) -> usize {
        self.indexed_count
    }

    /// Get the number of documents skipped.
    pub fn skipped_count(&self) -> usize {
        self.skipped_count
    }

    /// Finalize and return the built index.
    pub fn build(self) -> VectorIndex {
        self.index
    }
}

/// Result of an incremental update operation.
#[derive(Debug, Clone, Default)]
pub struct IncrementalVectorUpdateResult {
    /// Number of documents upserted (added or updated)
    pub upserted: usize,
    /// Number of documents removed
    pub removed: usize,
    /// Number of documents skipped (extraction errors)
    pub skipped: usize,
    /// New watermark after update
    pub new_watermark: i64,
}

/// Applies incremental updates to an existing vector index.
///
/// This is used after initial index creation to keep the index
/// current with ledger updates.
pub struct IncrementalVectorUpdater<'a> {
    /// The ledger alias for documents being updated
    ledger_alias: Arc<str>,
    /// The vector index being updated
    index: &'a mut VectorIndex,
    /// Vector extractor
    extractor: VectorExtractor,
}

impl<'a> IncrementalVectorUpdater<'a> {
    /// Create a new updater for the given ledger.
    pub fn new(ledger_alias: impl Into<Arc<str>>, index: &'a mut VectorIndex) -> Self {
        let dimensions = index.dimensions();
        let embedding_property = index.property_deps.embedding_property.to_string();

        Self {
            ledger_alias: ledger_alias.into(),
            index,
            extractor: VectorExtractor::new(embedding_property, dimensions),
        }
    }

    /// Apply an incremental update from query results.
    ///
    /// This method:
    /// 1. Upserts documents that are in the query results
    /// 2. Removes documents that were affected but are NOT in results
    /// 3. Updates the watermark for the ledger
    ///
    /// # Arguments
    ///
    /// * `results` - Query results for affected subjects (re-run of indexing query)
    /// * `affected_iris` - Set of subject IRIs that were affected by the commit
    /// * `new_t` - The new watermark (commit t) after this update
    pub fn apply_update(
        &mut self,
        results: &[Value],
        affected_iris: &HashSet<Arc<str>>,
        new_t: i64,
    ) -> IncrementalVectorUpdateResult {
        let mut result = IncrementalVectorUpdateResult {
            new_watermark: new_t,
            ..Default::default()
        };

        // Track which affected IRIs we've seen in the results
        let mut seen_iris: HashSet<Arc<str>> = HashSet::new();

        // Process each query result
        for item in results {
            if let Some(subject_iri) = self.extract_subject_iri(item) {
                let iri_arc: Arc<str> = Arc::from(subject_iri.as_str());
                seen_iris.insert(iri_arc.clone());

                // Only process if this subject was actually affected
                if affected_iris.contains(&iri_arc) {
                    match self.upsert_document(&subject_iri, item) {
                        Ok(true) => result.upserted += 1,
                        Ok(false) => result.skipped += 1,
                        Err(e) => {
                            warn!(iri = %subject_iri, error = %e, "Failed to upsert document");
                            result.skipped += 1;
                        }
                    }
                }
            }
        }

        // Remove documents for affected subjects NOT in results
        for iri in affected_iris {
            if !seen_iris.contains(iri) {
                match self.index.remove(self.ledger_alias.as_ref(), iri.as_ref()) {
                    Ok(true) => result.removed += 1,
                    Ok(false) => {} // Already not in index
                    Err(e) => {
                        warn!(iri = %iri, error = %e, "Failed to remove document");
                    }
                }
            }
        }

        // Update watermark
        self.index
            .watermark
            .update(self.ledger_alias.as_ref(), new_t);

        result
    }

    /// Apply a bulk update for a full resync.
    ///
    /// Unlike `apply_update`, this replaces ALL documents for this ledger
    /// with the given results.
    pub fn apply_full_sync(
        &mut self,
        results: &[Value],
        new_t: i64,
    ) -> IncrementalVectorUpdateResult {
        let mut result = IncrementalVectorUpdateResult {
            new_watermark: new_t,
            ..Default::default()
        };

        // Collect all existing IRIs for this ledger (including collisions!)
        let existing_iris: Vec<Arc<str>> = self
            .index
            .id_assigner()
            .all_entries()
            .filter(|(ledger, _)| *ledger == self.ledger_alias.as_ref())
            .map(|(_, iri)| Arc::from(iri))
            .collect();

        // Track which IRIs we see in the new results
        let mut seen_iris: HashSet<Arc<str>> = HashSet::new();

        // Process all results
        for item in results {
            if let Some(subject_iri) = self.extract_subject_iri(item) {
                let iri_arc: Arc<str> = Arc::from(subject_iri.as_str());
                seen_iris.insert(iri_arc);

                match self.upsert_document(&subject_iri, item) {
                    Ok(true) => result.upserted += 1,
                    Ok(false) => result.skipped += 1,
                    Err(e) => {
                        warn!(iri = %subject_iri, error = %e, "Failed to upsert document");
                        result.skipped += 1;
                    }
                }
            }
        }

        // Remove documents not in results
        for iri in existing_iris {
            if !seen_iris.contains(&iri) {
                match self.index.remove(self.ledger_alias.as_ref(), iri.as_ref()) {
                    Ok(true) => result.removed += 1,
                    Ok(false) => {}
                    Err(e) => {
                        warn!(iri = %iri, error = %e, "Failed to remove document");
                    }
                }
            }
        }

        // Update watermark
        self.index
            .watermark
            .update(self.ledger_alias.as_ref(), new_t);

        result
    }

    /// Extract the subject IRI from a query result.
    fn extract_subject_iri(&self, result: &Value) -> Option<String> {
        // Try direct @id string
        if let Some(Value::String(s)) = result.get("@id") {
            return Some(s.clone());
        }

        // Try @id as object with nested @id
        if let Some(Value::Object(obj)) = result.get("@id") {
            if let Some(s) = obj.get("@id").and_then(|v| v.as_str()) {
                return Some(s.to_string());
            }
        }

        // Try variable binding like "?x": {"@id": "..."}
        if let Value::Object(map) = result {
            for (k, v) in map {
                if !k.starts_with('?') {
                    continue;
                }
                if let Value::Object(obj) = v {
                    if let Some(id) = obj.get("@id").and_then(|vv| vv.as_str()) {
                        return Some(id.to_string());
                    }
                }
            }
        }

        None
    }

    /// Upsert a single document into the index.
    ///
    /// Returns `Ok(true)` if successfully upserted, `Ok(false)` if skipped.
    fn upsert_document(&mut self, subject_iri: &str, result: &Value) -> Result<bool> {
        match self.extractor.extract(result) {
            ExtractionResult::Success(vector) => {
                // Remove existing first (if any) to handle updates
                let _ = self.index.remove(self.ledger_alias.as_ref(), subject_iri);
                self.index
                    .add(self.ledger_alias.as_ref(), subject_iri, &vector)?;
                Ok(true)
            }
            _ => {
                // Extraction failed - remove from index if it exists
                let _ = self.index.remove(self.ledger_alias.as_ref(), subject_iri);
                Ok(false)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_vector_extractor_success() {
        let extractor = VectorExtractor::new("embedding", 3);

        let result = json!({
            "@id": "http://example.org/doc1",
            "embedding": [0.1, 0.2, 0.3]
        });

        match extractor.extract(&result) {
            ExtractionResult::Success(vec) => {
                assert_eq!(vec.len(), 3);
                assert!((vec[0] - 0.1).abs() < 0.001);
            }
            other => panic!("Expected Success, got {other:?}"),
        }
    }

    #[test]
    fn test_vector_extractor_jsonld_value() {
        let extractor = VectorExtractor::new("embedding", 3);

        // JSON-LD value object form
        let result = json!({
            "@id": "http://example.org/doc1",
            "embedding": {"@value": [0.1, 0.2, 0.3]}
        });

        match extractor.extract(&result) {
            ExtractionResult::Success(vec) => {
                assert_eq!(vec.len(), 3);
            }
            other => panic!("Expected Success, got {other:?}"),
        }
    }

    #[test]
    fn test_vector_extractor_missing_property() {
        let extractor = VectorExtractor::new("embedding", 3);

        let result = json!({
            "@id": "http://example.org/doc1",
            "title": "No embedding"
        });

        assert!(matches!(
            extractor.extract(&result),
            ExtractionResult::MissingProperty
        ));
    }

    #[test]
    fn test_vector_extractor_not_array() {
        let extractor = VectorExtractor::new("embedding", 3);

        let result = json!({
            "@id": "http://example.org/doc1",
            "embedding": "not an array"
        });

        assert!(matches!(
            extractor.extract(&result),
            ExtractionResult::NotArray
        ));
    }

    #[test]
    fn test_vector_extractor_invalid_element() {
        let extractor = VectorExtractor::new("embedding", 3);

        let result = json!({
            "@id": "http://example.org/doc1",
            "embedding": [0.1, "not a number", 0.3]
        });

        assert!(matches!(
            extractor.extract(&result),
            ExtractionResult::InvalidElement
        ));
    }

    #[test]
    fn test_vector_extractor_dimension_mismatch() {
        let extractor = VectorExtractor::new("embedding", 3);

        let result = json!({
            "@id": "http://example.org/doc1",
            "embedding": [0.1, 0.2]
        });

        assert!(matches!(
            extractor.extract(&result),
            ExtractionResult::DimensionMismatch {
                expected: 3,
                actual: 2
            }
        ));
    }

    #[test]
    fn test_builder_single_document() {
        let mut builder = VectorIndexBuilder::new("ledger:main", 3, DistanceMetric::Cosine)
            .unwrap()
            .with_embedding_property("embedding");

        let result = json!({
            "@id": "http://example.org/doc1",
            "embedding": [0.1, 0.2, 0.3]
        });

        let indexed = builder.add_result(&result).unwrap();
        assert!(indexed);
        assert_eq!(builder.indexed_count(), 1);
        assert_eq!(builder.skipped_count(), 0);

        let index = builder.build();
        assert_eq!(index.len(), 1);
        assert!(index.contains("ledger:main", "http://example.org/doc1"));
    }

    #[test]
    fn test_builder_multiple_documents() {
        let mut builder = VectorIndexBuilder::new("ledger:main", 3, DistanceMetric::Cosine)
            .unwrap()
            .with_embedding_property("embedding");

        let results = vec![
            json!({"@id": "http://example.org/doc1", "embedding": [1.0, 0.0, 0.0]}),
            json!({"@id": "http://example.org/doc2", "embedding": [0.0, 1.0, 0.0]}),
            json!({"@id": "http://example.org/doc3", "embedding": [0.0, 0.0, 1.0]}),
        ];

        builder.add_results(&results).unwrap();
        assert_eq!(builder.indexed_count(), 3);

        let index = builder.build();
        assert_eq!(index.len(), 3);
    }

    #[test]
    fn test_builder_skips_missing_embedding() {
        let mut builder = VectorIndexBuilder::new("ledger:main", 3, DistanceMetric::Cosine)
            .unwrap()
            .with_embedding_property("embedding");

        let result = json!({
            "@id": "http://example.org/doc1",
            "title": "No embedding"
        });

        let indexed = builder.add_result(&result).unwrap();
        assert!(!indexed);
        assert_eq!(builder.indexed_count(), 0);
        assert_eq!(builder.skipped_count(), 1);
    }

    #[test]
    fn test_builder_with_watermark() {
        let builder = VectorIndexBuilder::new("ledger:main", 3, DistanceMetric::Cosine)
            .unwrap()
            .with_watermark(42);

        let index = builder.build();
        assert_eq!(index.watermark.get("ledger:main"), Some(42));
    }

    #[test]
    fn test_builder_with_property_deps() {
        let deps = VectorPropertyDeps::new("http://example.org/embedding");

        let builder = VectorIndexBuilder::new("ledger:main", 3, DistanceMetric::Cosine)
            .unwrap()
            .with_property_deps(deps);

        let index = builder.build();
        assert_eq!(
            index.property_deps.embedding_property.as_ref(),
            "http://example.org/embedding"
        );
    }

    #[test]
    fn test_incremental_update_upsert() {
        // Build initial index
        let mut builder = VectorIndexBuilder::new("ledger:main", 3, DistanceMetric::Cosine)
            .unwrap()
            .with_embedding_property("embedding")
            .with_watermark(1);

        builder
            .add_results(&[
                json!({"@id": "http://example.org/doc1", "embedding": [1.0, 0.0, 0.0]}),
                json!({"@id": "http://example.org/doc2", "embedding": [0.0, 1.0, 0.0]}),
            ])
            .unwrap();

        let mut index = builder.build();
        index.property_deps = VectorPropertyDeps::new("embedding");
        assert_eq!(index.len(), 2);

        // Apply incremental update: doc1 changed, doc3 added
        let affected_iris: HashSet<Arc<str>> = [
            Arc::from("http://example.org/doc1"),
            Arc::from("http://example.org/doc3"),
        ]
        .into_iter()
        .collect();

        let results = vec![
            json!({"@id": "http://example.org/doc1", "embedding": [0.5, 0.5, 0.0]}),
            json!({"@id": "http://example.org/doc3", "embedding": [0.0, 0.0, 1.0]}),
        ];

        let mut updater = IncrementalVectorUpdater::new("ledger:main", &mut index);
        let result = updater.apply_update(&results, &affected_iris, 2);

        assert_eq!(result.upserted, 2);
        assert_eq!(result.removed, 0);
        assert_eq!(result.new_watermark, 2);

        // Index should now have 3 docs
        assert_eq!(index.len(), 3);
        assert!(index.contains("ledger:main", "http://example.org/doc1"));
        assert!(index.contains("ledger:main", "http://example.org/doc2"));
        assert!(index.contains("ledger:main", "http://example.org/doc3"));
    }

    #[test]
    fn test_incremental_update_remove() {
        // Build initial index
        let mut builder = VectorIndexBuilder::new("ledger:main", 3, DistanceMetric::Cosine)
            .unwrap()
            .with_embedding_property("embedding")
            .with_watermark(1);

        builder
            .add_results(&[
                json!({"@id": "http://example.org/doc1", "embedding": [1.0, 0.0, 0.0]}),
                json!({"@id": "http://example.org/doc2", "embedding": [0.0, 1.0, 0.0]}),
                json!({"@id": "http://example.org/doc3", "embedding": [0.0, 0.0, 1.0]}),
            ])
            .unwrap();

        let mut index = builder.build();
        index.property_deps = VectorPropertyDeps::new("embedding");
        assert_eq!(index.len(), 3);

        // Apply incremental update: doc2 was deleted
        let affected_iris: HashSet<Arc<str>> =
            [Arc::from("http://example.org/doc2")].into_iter().collect();

        let results: Vec<Value> = vec![];

        let mut updater = IncrementalVectorUpdater::new("ledger:main", &mut index);
        let result = updater.apply_update(&results, &affected_iris, 2);

        assert_eq!(result.upserted, 0);
        assert_eq!(result.removed, 1);

        assert_eq!(index.len(), 2);
        assert!(index.contains("ledger:main", "http://example.org/doc1"));
        assert!(!index.contains("ledger:main", "http://example.org/doc2"));
        assert!(index.contains("ledger:main", "http://example.org/doc3"));
    }

    #[test]
    fn test_full_sync() {
        // Build initial index
        let mut builder = VectorIndexBuilder::new("ledger:main", 3, DistanceMetric::Cosine)
            .unwrap()
            .with_embedding_property("embedding")
            .with_watermark(1);

        builder
            .add_results(&[
                json!({"@id": "http://example.org/doc1", "embedding": [1.0, 0.0, 0.0]}),
                json!({"@id": "http://example.org/doc2", "embedding": [0.0, 1.0, 0.0]}),
                json!({"@id": "http://example.org/doc3", "embedding": [0.0, 0.0, 1.0]}),
            ])
            .unwrap();

        let mut index = builder.build();
        index.property_deps = VectorPropertyDeps::new("embedding");
        assert_eq!(index.len(), 3);

        // Full sync with new set of documents
        let results = vec![
            json!({"@id": "http://example.org/doc1", "embedding": [0.5, 0.5, 0.0]}),
            json!({"@id": "http://example.org/doc4", "embedding": [0.3, 0.3, 0.4]}),
        ];

        let mut updater = IncrementalVectorUpdater::new("ledger:main", &mut index);
        let result = updater.apply_full_sync(&results, 10);

        assert_eq!(result.upserted, 2);
        assert_eq!(result.removed, 2);
        assert_eq!(result.new_watermark, 10);

        assert_eq!(index.len(), 2);
        assert!(index.contains("ledger:main", "http://example.org/doc1"));
        assert!(!index.contains("ledger:main", "http://example.org/doc2"));
        assert!(!index.contains("ledger:main", "http://example.org/doc3"));
        assert!(index.contains("ledger:main", "http://example.org/doc4"));
    }
}
