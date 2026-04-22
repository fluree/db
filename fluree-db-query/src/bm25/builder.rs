//! BM25 Index Builder
//!
//! Provides functionality for building BM25 indexes from query results.
//! This is used during initial index creation and full rebuilds.
//!
//! # Building an Index
//!
//! The builder takes query results (JSON-LD items) and builds an index:
//!
//! 1. Extract subject IRI from each result (`@id` field)
//! 2. Extract text from the result using `extract_text()`
//! 3. Analyze text to get term frequencies
//! 4. Add document to the BM25 index
//!
//! Create a `Bm25IndexBuilder`, call `add_results` with JSON-LD items, then `build` to produce the index.

use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::analyzer::Analyzer;
use super::index::{Bm25Config, Bm25Index, DocKey, GraphSourceWatermark, PropertyDeps};
use super::text::extract_text;

/// Error type for builder operations.
#[derive(Debug, thiserror::Error)]
pub enum BuilderError {
    #[error("Missing @id in result: {0}")]
    MissingId(String),

    #[error("Invalid @id type, expected string: {0}")]
    InvalidIdType(String),
}

/// Result type for builder operations.
pub type Result<T> = std::result::Result<T, BuilderError>;

/// Builder for creating BM25 indexes from query results.
pub struct Bm25IndexBuilder {
    /// The ledger alias for documents being indexed
    ledger_alias: Arc<str>,
    /// The BM25 index being built
    index: Bm25Index,
    /// Analyzer for text processing
    analyzer: Analyzer,
    /// Count of documents successfully indexed
    indexed_count: usize,
    /// Count of documents skipped (no @id or empty text)
    skipped_count: usize,
}

impl Bm25IndexBuilder {
    /// Create a new builder for the given ledger alias.
    pub fn new(ledger_alias: impl Into<Arc<str>>, config: Bm25Config) -> Self {
        Self {
            ledger_alias: ledger_alias.into(),
            index: Bm25Index::with_config(config),
            analyzer: Analyzer::english_default(),
            indexed_count: 0,
            skipped_count: 0,
        }
    }

    /// Create a new builder with a custom analyzer.
    pub fn with_analyzer(
        ledger_alias: impl Into<Arc<str>>,
        config: Bm25Config,
        analyzer: Analyzer,
    ) -> Self {
        Self {
            ledger_alias: ledger_alias.into(),
            index: Bm25Index::with_config(config),
            analyzer,
            indexed_count: 0,
            skipped_count: 0,
        }
    }

    /// Set property dependencies for incremental updates.
    pub fn with_property_deps(mut self, deps: PropertyDeps) -> Self {
        self.index.property_deps = deps;
        self
    }

    /// Set the initial watermark for the ledger.
    pub fn with_watermark(mut self, t: i64) -> Self {
        self.index.watermark.update(self.ledger_alias.as_ref(), t);
        self
    }

    /// Add a single query result to the index.
    ///
    /// The result should be a JSON-LD object with an `@id` field.
    /// Returns `Ok(true)` if the document was indexed, `Ok(false)` if skipped.
    pub fn add_result(&mut self, result: &Value) -> Result<bool> {
        // Extract @id from the result.
        //
        // Preferred shape (JSON-LD): { "@id": "http://..." }
        // Also tolerate value-object form: { "@id": { "@id": "http://..." } }
        // and execution-helper form: { "?x": { "@id": "http://..." }, ... }.
        let subject_iri: Arc<str> = match result.get("@id") {
            Some(Value::String(s)) => Arc::from(s.as_str()),
            Some(Value::Object(obj)) => obj
                .get("@id")
                .and_then(|v| v.as_str())
                .map(Arc::from)
                .ok_or_else(|| BuilderError::InvalidIdType(format!("{obj:?}")))?,
            Some(other) => return Err(BuilderError::InvalidIdType(format!("{other:?}"))),
            None => {
                // Fall back to a top-level variable binding like "?x": {"@id": "..."}.
                let mut found: Option<Arc<str>> = None;
                if let Value::Object(map) = result {
                    for (k, v) in map {
                        if !k.starts_with('?') {
                            continue;
                        }
                        if let Value::Object(obj) = v {
                            if let Some(id) = obj.get("@id").and_then(|vv| vv.as_str()) {
                                found = Some(Arc::from(id));
                                break;
                            }
                        }
                    }
                }

                match found {
                    Some(id) => id,
                    None => {
                        self.skipped_count += 1;
                        return Ok(false);
                    }
                }
            }
        };

        // Extract text from the result
        let text = extract_text(result);

        // Skip if no text to index
        if text.trim().is_empty() {
            self.skipped_count += 1;
            return Ok(false);
        }

        // Analyze text to get term frequencies
        let term_freqs = self.analyzer.analyze_to_term_freqs(&text);

        // Skip if no terms after analysis (all stopwords, etc.)
        if term_freqs.is_empty() {
            self.skipped_count += 1;
            return Ok(false);
        }

        // Create document key
        let doc_key = DocKey::new(self.ledger_alias.clone(), subject_iri);

        // Convert HashMap<String, u32> to HashMap<&str, u32>
        let term_freqs_ref: HashMap<&str, u32> =
            term_freqs.iter().map(|(k, v)| (k.as_str(), *v)).collect();

        // Add to index
        self.index.upsert_document(doc_key, term_freqs_ref);
        self.indexed_count += 1;

        Ok(true)
    }

    /// Add multiple query results to the index.
    ///
    /// Results should be an array of JSON-LD objects, each with an `@id` field.
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
                // Single object, treat as one result
                self.add_result(results)?;
            }
            _ => {
                // Skip non-object, non-array values
            }
        }
        Ok(())
    }

    /// Get the number of documents successfully indexed.
    pub fn indexed_count(&self) -> usize {
        self.indexed_count
    }

    /// Get the number of documents skipped.
    pub fn skipped_count(&self) -> usize {
        self.skipped_count
    }

    /// Get the current index statistics.
    pub fn stats(&self) -> &super::index::Bm25Stats {
        &self.index.stats
    }

    /// Finalize and return the built index.
    pub fn build(self) -> Bm25Index {
        self.index
    }
}

/// Builder for multi-ledger BM25 indexes.
///
/// Supports building an index from multiple source ledgers with
/// per-ledger watermark tracking.
pub struct MultiBm25IndexBuilder {
    /// The BM25 index being built
    index: Bm25Index,
    /// Analyzer for text processing
    analyzer: Analyzer,
    /// Per-ledger document counts
    ledger_counts: HashMap<String, usize>,
}

impl MultiBm25IndexBuilder {
    /// Create a new multi-ledger builder.
    pub fn new(config: Bm25Config) -> Self {
        Self {
            index: Bm25Index::with_config(config),
            analyzer: Analyzer::english_default(),
            ledger_counts: HashMap::new(),
        }
    }

    /// Create a new multi-ledger builder with a custom analyzer.
    pub fn with_analyzer(config: Bm25Config, analyzer: Analyzer) -> Self {
        Self {
            index: Bm25Index::with_config(config),
            analyzer,
            ledger_counts: HashMap::new(),
        }
    }

    /// Set property dependencies for incremental updates.
    pub fn with_property_deps(mut self, deps: PropertyDeps) -> Self {
        self.index.property_deps = deps;
        self
    }

    /// Set the watermark for a specific ledger.
    pub fn with_watermark(mut self, ledger_alias: &str, t: i64) -> Self {
        self.index.watermark.update(ledger_alias, t);
        self
    }

    /// Add results from a specific ledger.
    pub fn add_ledger_results(&mut self, ledger_alias: &str, results: &[Value]) -> Result<usize> {
        let ledger_arc: Arc<str> = Arc::from(ledger_alias);
        let mut count = 0;

        for result in results {
            // Extract @id from the result
            let subject_iri = match result.get("@id") {
                Some(Value::String(s)) => Arc::from(s.as_str()),
                Some(_) | None => continue,
            };

            // Extract and analyze text
            let text = extract_text(result);
            if text.trim().is_empty() {
                continue;
            }

            let term_freqs = self.analyzer.analyze_to_term_freqs(&text);
            if term_freqs.is_empty() {
                continue;
            }

            // Create document key with ledger info
            let doc_key = DocKey::new(ledger_arc.clone(), subject_iri);

            // Convert to ref map
            let term_freqs_ref: HashMap<&str, u32> =
                term_freqs.iter().map(|(k, v)| (k.as_str(), *v)).collect();

            self.index.upsert_document(doc_key, term_freqs_ref);
            count += 1;
        }

        *self
            .ledger_counts
            .entry(ledger_alias.to_string())
            .or_default() += count;
        Ok(count)
    }

    /// Get the document count for a specific ledger.
    pub fn ledger_count(&self, ledger_alias: &str) -> usize {
        self.ledger_counts.get(ledger_alias).copied().unwrap_or(0)
    }

    /// Get the total document count across all ledgers.
    pub fn total_count(&self) -> usize {
        self.ledger_counts.values().sum()
    }

    /// Get the watermark tracker.
    pub fn watermark(&self) -> &GraphSourceWatermark {
        &self.index.watermark
    }

    /// Finalize and return the built index.
    pub fn build(self) -> Bm25Index {
        self.index
    }
}

// =============================================================================
// Incremental Updates
// =============================================================================

/// Result of an incremental update operation.
#[derive(Debug, Clone, Default)]
pub struct IncrementalUpdateResult {
    /// Number of documents upserted (added or updated)
    pub upserted: usize,
    /// Number of documents removed (no longer match query)
    pub removed: usize,
    /// Number of documents unchanged (same content)
    pub unchanged: usize,
    /// New watermark after update
    pub new_watermark: i64,
}

/// Applies incremental updates to an existing BM25 index.
///
/// This is used after initial index creation to keep the index
/// current with ledger updates. The flow is:
///
/// 1. Get affected subjects from commit flakes using `CompiledPropertyDeps::affected_subjects()`
/// 2. Convert subject SIDs to IRIs
/// 3. Re-run indexing query for those specific subjects
/// 4. Apply results using `IncrementalUpdater::apply_update()`
///
/// Create with `IncrementalUpdater::new`, then call `apply_update` with re-queried results and affected IRIs.
pub struct IncrementalUpdater<'a> {
    /// The ledger alias for documents being updated
    ledger_alias: Arc<str>,
    /// The BM25 index being updated
    index: &'a mut Bm25Index,
    /// Analyzer for text processing (must match initial build)
    analyzer: Analyzer,
}

impl<'a> IncrementalUpdater<'a> {
    /// Create a new updater for the given ledger.
    pub fn new(ledger_alias: impl Into<Arc<str>>, index: &'a mut Bm25Index) -> Self {
        Self {
            ledger_alias: ledger_alias.into(),
            index,
            analyzer: Analyzer::english_default(),
        }
    }

    /// Create a new updater with a custom analyzer.
    ///
    /// IMPORTANT: The analyzer must match the one used during initial build,
    /// otherwise term frequencies will be inconsistent.
    pub fn with_analyzer(
        ledger_alias: impl Into<Arc<str>>,
        index: &'a mut Bm25Index,
        analyzer: Analyzer,
    ) -> Self {
        Self {
            ledger_alias: ledger_alias.into(),
            index,
            analyzer,
        }
    }

    /// Apply an incremental update from query results.
    ///
    /// This method:
    /// 1. Upserts documents that are in the query results
    /// 2. Removes documents that were affected but are NOT in results
    ///    (deleted or no longer match query criteria)
    /// 3. Updates the watermark for the ledger
    ///
    /// # Arguments
    ///
    /// * `results` - Query results for affected subjects (re-run of indexing query)
    /// * `affected_iris` - Set of subject IRIs that were affected by the commit.
    ///   Any IRI in this set that is NOT in results will be removed.
    /// * `new_t` - The new watermark (commit t) after this update
    ///
    /// # Returns
    ///
    /// Statistics about the update operation.
    pub fn apply_update(
        &mut self,
        results: &[Value],
        affected_iris: &HashSet<Arc<str>>,
        new_t: i64,
    ) -> IncrementalUpdateResult {
        let mut result = IncrementalUpdateResult {
            new_watermark: new_t,
            ..Default::default()
        };

        // Track which affected IRIs we've seen in the results
        let mut seen_iris: HashSet<Arc<str>> = HashSet::new();

        // Process each query result
        for item in results {
            if let Some(subject_iri) = self.extract_subject_iri(item) {
                seen_iris.insert(subject_iri.clone());

                // Only process if this subject was actually affected
                if affected_iris.contains(&subject_iri) {
                    if self.upsert_document(subject_iri, item) {
                        result.upserted += 1;
                    } else {
                        result.unchanged += 1;
                    }
                }
            }
        }

        // Remove documents for affected subjects NOT in results
        // (they were deleted or no longer match the query criteria)
        for iri in affected_iris {
            if !seen_iris.contains(iri) {
                let doc_key = DocKey::new(self.ledger_alias.clone(), iri.clone());
                if self.index.remove_document(&doc_key) {
                    result.removed += 1;
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
    /// with the given results. Use this when catching up from a significantly
    /// stale watermark or when incremental tracking is unreliable.
    ///
    /// # Arguments
    ///
    /// * `results` - Complete query results for this ledger
    /// * `new_t` - The new watermark after this sync
    ///
    /// # Returns
    ///
    /// Statistics about the sync operation.
    pub fn apply_full_sync(&mut self, results: &[Value], new_t: i64) -> IncrementalUpdateResult {
        let mut result = IncrementalUpdateResult {
            new_watermark: new_t,
            ..Default::default()
        };

        // Collect all existing doc keys for this ledger
        let existing_keys: Vec<DocKey> = self
            .index
            .doc_meta
            .iter()
            .filter_map(|opt| opt.as_ref())
            .filter(|meta| meta.doc_key.ledger_alias == self.ledger_alias)
            .map(|meta| meta.doc_key.clone())
            .collect();

        // Track which IRIs we see in the new results
        let mut seen_iris: HashSet<Arc<str>> = HashSet::new();

        // Process all results
        for item in results {
            if let Some(subject_iri) = self.extract_subject_iri(item) {
                seen_iris.insert(subject_iri.clone());
                if self.upsert_document(subject_iri, item) {
                    result.upserted += 1;
                } else {
                    result.unchanged += 1;
                }
            }
        }

        // Remove documents not in results
        for key in existing_keys {
            if !seen_iris.contains(&key.subject_iri) && self.index.remove_document(&key) {
                result.removed += 1;
            }
        }

        // Update watermark
        self.index
            .watermark
            .update(self.ledger_alias.as_ref(), new_t);

        result
    }

    /// Extract the subject IRI from a query result.
    fn extract_subject_iri(&self, result: &Value) -> Option<Arc<str>> {
        // Try direct @id string
        if let Some(Value::String(s)) = result.get("@id") {
            return Some(Arc::from(s.as_str()));
        }

        // Try @id as object with nested @id
        if let Some(Value::Object(obj)) = result.get("@id") {
            if let Some(s) = obj.get("@id").and_then(|v| v.as_str()) {
                return Some(Arc::from(s));
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
                        return Some(Arc::from(id));
                    }
                }
            }
        }

        None
    }

    /// Upsert a single document into the index.
    ///
    /// Returns `true` if the document was actually updated (content changed),
    /// `false` if unchanged or skipped.
    fn upsert_document(&mut self, subject_iri: Arc<str>, result: &Value) -> bool {
        // Extract text
        let text = extract_text(result);
        if text.trim().is_empty() {
            // No text - remove if exists
            let doc_key = DocKey::new(self.ledger_alias.clone(), subject_iri);
            return self.index.remove_document(&doc_key);
        }

        // Analyze text
        let term_freqs = self.analyzer.analyze_to_term_freqs(&text);
        if term_freqs.is_empty() {
            // Only stopwords - remove if exists
            let doc_key = DocKey::new(self.ledger_alias.clone(), subject_iri);
            return self.index.remove_document(&doc_key);
        }

        // Create document key
        let doc_key = DocKey::new(self.ledger_alias.clone(), subject_iri);

        // Convert to ref map
        let term_freqs_ref: HashMap<&str, u32> =
            term_freqs.iter().map(|(k, v)| (k.as_str(), *v)).collect();

        // Upsert into index
        self.index.upsert_document(doc_key, term_freqs_ref);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_builder_single_document() {
        let mut builder = Bm25IndexBuilder::new("ledger:main", Bm25Config::default());

        let result = json!({
            "@id": "http://example.org/doc1",
            "title": "Hello World",
            "body": "This is a test document about rust programming."
        });

        let indexed = builder.add_result(&result).unwrap();
        assert!(indexed);
        assert_eq!(builder.indexed_count(), 1);
        assert_eq!(builder.skipped_count(), 0);

        let index = builder.build();
        assert_eq!(index.num_docs(), 1);

        let doc_key = DocKey::new("ledger:main", "http://example.org/doc1");
        assert!(index.contains_doc(&doc_key));
    }

    #[test]
    fn test_builder_multiple_documents() {
        let mut builder = Bm25IndexBuilder::new("ledger:main", Bm25Config::default());

        let results = vec![
            json!({"@id": "http://example.org/doc1", "title": "Rust Programming"}),
            json!({"@id": "http://example.org/doc2", "title": "Python Development"}),
            json!({"@id": "http://example.org/doc3", "title": "JavaScript Guide"}),
        ];

        builder.add_results(&results).unwrap();
        assert_eq!(builder.indexed_count(), 3);

        let index = builder.build();
        assert_eq!(index.num_docs(), 3);
    }

    #[test]
    fn test_builder_skips_missing_id() {
        let mut builder = Bm25IndexBuilder::new("ledger:main", Bm25Config::default());

        let result = json!({
            "title": "No ID Document"
        });

        let indexed = builder.add_result(&result).unwrap();
        assert!(!indexed);
        assert_eq!(builder.indexed_count(), 0);
        assert_eq!(builder.skipped_count(), 1);
    }

    #[test]
    fn test_builder_skips_empty_text() {
        let mut builder = Bm25IndexBuilder::new("ledger:main", Bm25Config::default());

        // Only @id, no content
        let result = json!({
            "@id": "http://example.org/empty"
        });

        let indexed = builder.add_result(&result).unwrap();
        assert!(!indexed);
        assert_eq!(builder.skipped_count(), 1);
    }

    #[test]
    fn test_builder_skips_only_stopwords() {
        let mut builder = Bm25IndexBuilder::new("ledger:main", Bm25Config::default());

        // Only stopwords after analysis
        let result = json!({
            "@id": "http://example.org/stopwords",
            "content": "the a an is are"
        });

        let indexed = builder.add_result(&result).unwrap();
        assert!(!indexed);
        assert_eq!(builder.skipped_count(), 1);
    }

    #[test]
    fn test_builder_with_watermark() {
        let builder =
            Bm25IndexBuilder::new("ledger:main", Bm25Config::default()).with_watermark(42);

        let index = builder.build();
        assert_eq!(index.watermark.get("ledger:main"), Some(42));
    }

    #[test]
    fn test_builder_with_property_deps() {
        let mut deps = PropertyDeps::new();
        deps.add("http://schema.org/name");
        deps.add("http://schema.org/description");

        let builder =
            Bm25IndexBuilder::new("ledger:main", Bm25Config::default()).with_property_deps(deps);

        let index = builder.build();
        assert!(index.property_deps.contains("http://schema.org/name"));
        assert!(index
            .property_deps
            .contains("http://schema.org/description"));
    }

    #[test]
    fn test_builder_upserts_duplicate_ids() {
        let mut builder = Bm25IndexBuilder::new("ledger:main", Bm25Config::default());

        // First version
        builder
            .add_result(&json!({
                "@id": "http://example.org/doc1",
                "title": "Original Title"
            }))
            .unwrap();

        // Updated version (same ID)
        builder
            .add_result(&json!({
                "@id": "http://example.org/doc1",
                "title": "Updated Title"
            }))
            .unwrap();

        // Should still be 1 document (upserted)
        let index = builder.build();
        assert_eq!(index.num_docs(), 1);
    }

    #[test]
    fn test_multi_builder_basic() {
        let mut builder = MultiBm25IndexBuilder::new(Bm25Config::default());

        let ledger1_results = vec![
            json!({"@id": "http://example.org/doc1", "title": "Doc 1"}),
            json!({"@id": "http://example.org/doc2", "title": "Doc 2"}),
        ];

        let ledger2_results = vec![json!({"@id": "http://example.org/doc3", "title": "Doc 3"})];

        builder
            .add_ledger_results("ledger1:main", &ledger1_results)
            .unwrap();
        builder
            .add_ledger_results("ledger2:main", &ledger2_results)
            .unwrap();

        assert_eq!(builder.ledger_count("ledger1:main"), 2);
        assert_eq!(builder.ledger_count("ledger2:main"), 1);
        assert_eq!(builder.total_count(), 3);

        let index = builder.build();
        assert_eq!(index.num_docs(), 3);

        // Documents from different ledgers with same IRI are distinct
        assert!(index.contains_doc(&DocKey::new("ledger1:main", "http://example.org/doc1")));
        assert!(index.contains_doc(&DocKey::new("ledger2:main", "http://example.org/doc3")));
    }

    #[test]
    fn test_multi_builder_same_iri_different_ledgers() {
        let mut builder = MultiBm25IndexBuilder::new(Bm25Config::default());

        // Same IRI in different ledgers
        builder
            .add_ledger_results(
                "ledger1:main",
                &[json!({"@id": "http://example.org/shared", "title": "Ledger 1 version"})],
            )
            .unwrap();

        builder
            .add_ledger_results(
                "ledger2:main",
                &[json!({"@id": "http://example.org/shared", "title": "Ledger 2 version"})],
            )
            .unwrap();

        let index = builder.build();

        // Should have 2 distinct documents
        assert_eq!(index.num_docs(), 2);
        assert!(index.contains_doc(&DocKey::new("ledger1:main", "http://example.org/shared")));
        assert!(index.contains_doc(&DocKey::new("ledger2:main", "http://example.org/shared")));
    }

    #[test]
    fn test_multi_builder_watermarks() {
        let builder = MultiBm25IndexBuilder::new(Bm25Config::default())
            .with_watermark("ledger1:main", 10)
            .with_watermark("ledger2:main", 20);

        let index = builder.build();

        assert_eq!(index.watermark.get("ledger1:main"), Some(10));
        assert_eq!(index.watermark.get("ledger2:main"), Some(20));
        assert_eq!(index.watermark.effective_t(), 10); // min of all
    }

    // =========================================================================
    // IncrementalUpdater tests
    // =========================================================================

    #[test]
    fn test_incremental_update_upsert() {
        // Build initial index
        // Note: Using non-stopword terms to ensure documents get indexed
        let mut builder =
            Bm25IndexBuilder::new("ledger:main", Bm25Config::default()).with_watermark(1);

        builder
            .add_results(&[
                json!({"@id": "http://example.org/doc1", "title": "Original Widget"}),
                json!({"@id": "http://example.org/doc2", "title": "Database Server"}),
            ])
            .unwrap();

        let mut index = builder.build();
        assert_eq!(index.num_docs(), 2);

        // Apply incremental update: doc1 changed, doc3 added
        let affected_iris: HashSet<Arc<str>> = [
            Arc::from("http://example.org/doc1"),
            Arc::from("http://example.org/doc3"),
        ]
        .into_iter()
        .collect();

        let results = vec![
            json!({"@id": "http://example.org/doc1", "title": "Updated Widget"}),
            json!({"@id": "http://example.org/doc3", "title": "Database Document"}),
        ];

        let mut updater = IncrementalUpdater::new("ledger:main", &mut index);
        let result = updater.apply_update(&results, &affected_iris, 2);

        assert_eq!(result.upserted, 2);
        assert_eq!(result.removed, 0);
        assert_eq!(result.new_watermark, 2);

        // Index should now have 3 docs
        assert_eq!(index.num_docs(), 3);
        assert!(index.contains_doc(&DocKey::new("ledger:main", "http://example.org/doc1")));
        assert!(index.contains_doc(&DocKey::new("ledger:main", "http://example.org/doc2")));
        assert!(index.contains_doc(&DocKey::new("ledger:main", "http://example.org/doc3")));

        // Watermark should be updated
        assert_eq!(index.watermark.get("ledger:main"), Some(2));
    }

    #[test]
    fn test_incremental_update_remove() {
        // Build initial index
        // Note: Using non-stopword terms like "widget", "server", "database"
        let mut builder =
            Bm25IndexBuilder::new("ledger:main", Bm25Config::default()).with_watermark(1);

        builder
            .add_results(&[
                json!({"@id": "http://example.org/doc1", "title": "Widget Alpha"}),
                json!({"@id": "http://example.org/doc2", "title": "Server Beta"}),
                json!({"@id": "http://example.org/doc3", "title": "Database Gamma"}),
            ])
            .unwrap();

        let mut index = builder.build();
        assert_eq!(index.num_docs(), 3);

        // Apply incremental update: doc2 was deleted (affected but not in results)
        let affected_iris: HashSet<Arc<str>> =
            [Arc::from("http://example.org/doc2")].into_iter().collect();

        // Empty results for the affected subject = it was deleted
        let results: Vec<Value> = vec![];

        let mut updater = IncrementalUpdater::new("ledger:main", &mut index);
        let result = updater.apply_update(&results, &affected_iris, 2);

        assert_eq!(result.upserted, 0);
        assert_eq!(result.removed, 1);
        assert_eq!(result.new_watermark, 2);

        // Index should now have 2 docs
        assert_eq!(index.num_docs(), 2);
        assert!(index.contains_doc(&DocKey::new("ledger:main", "http://example.org/doc1")));
        assert!(!index.contains_doc(&DocKey::new("ledger:main", "http://example.org/doc2")));
        assert!(index.contains_doc(&DocKey::new("ledger:main", "http://example.org/doc3")));
    }

    #[test]
    fn test_incremental_update_type_change() {
        // Build initial index (only indexes type=Article)
        // Note: Using non-stopword terms
        let mut builder =
            Bm25IndexBuilder::new("ledger:main", Bm25Config::default()).with_watermark(1);

        builder.add_results(&[
            json!({"@id": "http://example.org/doc1", "@type": "Article", "title": "Widget Alpha"}),
            json!({"@id": "http://example.org/doc2", "@type": "Article", "title": "Database Beta"}),
        ]).unwrap();

        let mut index = builder.build();
        assert_eq!(index.num_docs(), 2);

        // doc1's type changed from Article to something else (no longer matches query)
        // The query results won't include doc1 anymore
        let affected_iris: HashSet<Arc<str>> =
            [Arc::from("http://example.org/doc1")].into_iter().collect();

        // doc1 not in results = removed from index
        let results: Vec<Value> = vec![];

        let mut updater = IncrementalUpdater::new("ledger:main", &mut index);
        let result = updater.apply_update(&results, &affected_iris, 2);

        assert_eq!(result.removed, 1);
        assert_eq!(index.num_docs(), 1);
        assert!(!index.contains_doc(&DocKey::new("ledger:main", "http://example.org/doc1")));
        assert!(index.contains_doc(&DocKey::new("ledger:main", "http://example.org/doc2")));
    }

    #[test]
    fn test_incremental_update_mixed_operations() {
        // Build initial index
        // Note: Using non-stopword terms
        let mut builder =
            Bm25IndexBuilder::new("ledger:main", Bm25Config::default()).with_watermark(1);

        builder
            .add_results(&[
                json!({"@id": "http://example.org/doc1", "title": "Widget Alpha"}),
                json!({"@id": "http://example.org/doc2", "title": "Server Beta"}),
            ])
            .unwrap();

        let mut index = builder.build();

        // Mixed: doc1 updated, doc2 deleted, doc3 added
        let affected_iris: HashSet<Arc<str>> = [
            Arc::from("http://example.org/doc1"),
            Arc::from("http://example.org/doc2"),
            Arc::from("http://example.org/doc3"),
        ]
        .into_iter()
        .collect();

        let results = vec![
            json!({"@id": "http://example.org/doc1", "title": "Updated Widget"}),
            // doc2 missing = deleted
            json!({"@id": "http://example.org/doc3", "title": "Database Gamma"}),
        ];

        let mut updater = IncrementalUpdater::new("ledger:main", &mut index);
        let result = updater.apply_update(&results, &affected_iris, 2);

        assert_eq!(result.upserted, 2); // doc1 updated, doc3 added
        assert_eq!(result.removed, 1); // doc2 deleted

        assert_eq!(index.num_docs(), 2);
        assert!(index.contains_doc(&DocKey::new("ledger:main", "http://example.org/doc1")));
        assert!(!index.contains_doc(&DocKey::new("ledger:main", "http://example.org/doc2")));
        assert!(index.contains_doc(&DocKey::new("ledger:main", "http://example.org/doc3")));
    }

    #[test]
    fn test_incremental_update_ignores_unaffected() {
        // Build initial index
        // Note: Using non-stopword terms
        let mut builder =
            Bm25IndexBuilder::new("ledger:main", Bm25Config::default()).with_watermark(1);

        builder
            .add_results(&[json!({"@id": "http://example.org/doc1", "title": "Widget Alpha"})])
            .unwrap();

        let mut index = builder.build();

        // Only doc2 is affected, but results include doc1 and doc2
        let affected_iris: HashSet<Arc<str>> =
            [Arc::from("http://example.org/doc2")].into_iter().collect();

        let results = vec![
            json!({"@id": "http://example.org/doc1", "title": "Widget Alpha Unchanged"}),
            json!({"@id": "http://example.org/doc2", "title": "Server Beta"}),
        ];

        let mut updater = IncrementalUpdater::new("ledger:main", &mut index);
        let result = updater.apply_update(&results, &affected_iris, 2);

        // Only doc2 should be counted as upserted
        assert_eq!(result.upserted, 1);
        assert_eq!(index.num_docs(), 2);
    }

    #[test]
    fn test_full_sync() {
        // Build initial index
        // Note: Using non-stopword terms
        let mut builder =
            Bm25IndexBuilder::new("ledger:main", Bm25Config::default()).with_watermark(1);

        builder
            .add_results(&[
                json!({"@id": "http://example.org/doc1", "title": "Widget Alpha"}),
                json!({"@id": "http://example.org/doc2", "title": "Server Beta"}),
                json!({"@id": "http://example.org/doc3", "title": "Database Gamma"}),
            ])
            .unwrap();

        let mut index = builder.build();
        assert_eq!(index.num_docs(), 3);

        // Full sync with new set of documents
        let results = vec![
            json!({"@id": "http://example.org/doc1", "title": "Updated Widget"}),
            // doc2 and doc3 removed
            json!({"@id": "http://example.org/doc4", "title": "Storage Delta"}),
        ];

        let mut updater = IncrementalUpdater::new("ledger:main", &mut index);
        let result = updater.apply_full_sync(&results, 10);

        assert_eq!(result.upserted, 2); // doc1 updated, doc4 added
        assert_eq!(result.removed, 2); // doc2 and doc3 removed
        assert_eq!(result.new_watermark, 10);

        assert_eq!(index.num_docs(), 2);
        assert!(index.contains_doc(&DocKey::new("ledger:main", "http://example.org/doc1")));
        assert!(!index.contains_doc(&DocKey::new("ledger:main", "http://example.org/doc2")));
        assert!(!index.contains_doc(&DocKey::new("ledger:main", "http://example.org/doc3")));
        assert!(index.contains_doc(&DocKey::new("ledger:main", "http://example.org/doc4")));

        assert_eq!(index.watermark.get("ledger:main"), Some(10));
    }

    #[test]
    fn test_full_sync_preserves_other_ledgers() {
        // Build multi-ledger index
        // Note: Using non-stopword terms
        let mut builder = MultiBm25IndexBuilder::new(Bm25Config::default())
            .with_watermark("ledger1:main", 1)
            .with_watermark("ledger2:main", 1);

        builder
            .add_ledger_results(
                "ledger1:main",
                &[json!({"@id": "http://example.org/doc1", "title": "Widget Alpha"})],
            )
            .unwrap();

        builder
            .add_ledger_results(
                "ledger2:main",
                &[json!({"@id": "http://example.org/doc2", "title": "Server Beta"})],
            )
            .unwrap();

        let mut index = builder.build();
        assert_eq!(index.num_docs(), 2);

        // Full sync only ledger1
        let results = vec![json!({"@id": "http://example.org/doc3", "title": "Database Gamma"})];

        let mut updater = IncrementalUpdater::new("ledger1:main", &mut index);
        updater.apply_full_sync(&results, 5);

        // ledger2 should be untouched
        assert_eq!(index.num_docs(), 2);
        assert!(!index.contains_doc(&DocKey::new("ledger1:main", "http://example.org/doc1")));
        assert!(index.contains_doc(&DocKey::new("ledger1:main", "http://example.org/doc3")));
        assert!(index.contains_doc(&DocKey::new("ledger2:main", "http://example.org/doc2")));

        // Only ledger1 watermark should be updated
        assert_eq!(index.watermark.get("ledger1:main"), Some(5));
        assert_eq!(index.watermark.get("ledger2:main"), Some(1));
    }
}
