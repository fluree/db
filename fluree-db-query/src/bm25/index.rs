//! BM25 Index Data Structures
//!
//! This module defines the core data structures for BM25 full-text search
//! Key design decisions:
//!
//! - `DocKey` uses canonical IRI strings (not `Sid`) for multi-ledger safety
//! - Inverted posting list representation for efficient query-time scoring
//! - Per-ledger watermarks for multi-source graph sources
//! - Lazy deletion with compact-on-serialize for CAS determinism

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use fluree_db_core::Flake;
use serde::{Deserialize, Serialize};

// ============================================================================
// Document Identity
// ============================================================================

/// Document key for BM25 index entries.
///
/// Uses canonical IRI string (not `Sid`) because `Sid` values are ledger-local:
/// two ledgers can encode the same IRI to different Sids. Cross-ledger identity
/// requires the canonical IRI. Convert to/from `Sid` only at ledger query boundaries.
///
/// Implements `Ord` to support deterministic serialization via `BTreeMap`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DocKey {
    /// Ledger alias including branch (e.g., "source-ledger:main")
    pub ledger_alias: Arc<str>,
    /// Canonical IRI string for the subject
    pub subject_iri: Arc<str>,
}

impl DocKey {
    /// Create a new document key
    pub fn new(ledger_alias: impl Into<Arc<str>>, subject_iri: impl Into<Arc<str>>) -> Self {
        Self {
            ledger_alias: ledger_alias.into(),
            subject_iri: subject_iri.into(),
        }
    }
}

// ============================================================================
// Posting List Structures
// ============================================================================

/// A single posting entry: a document that contains a given term, with its frequency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Posting {
    /// Internal document ID (index into doc_meta)
    pub doc_id: u32,
    /// Term frequency in this document
    pub term_freq: u32,
}

/// Number of postings per block. Matches Lucene's default.
/// 128 postings ≈ 1 KB per block at current `Posting` size — L1 cache friendly.
pub(crate) const POSTING_BLOCK_SIZE: usize = 128;

/// Per-block metadata for navigation and future WAND score upper bounds.
///
/// Each block covers a contiguous slice of the parent `PostingList.postings` vec.
/// Block boundaries are implicit from `end_offset` values (block 0 starts at 0).
#[derive(Debug, Clone, Default)]
pub(crate) struct BlockMeta {
    /// Exclusive end offset into the parent `PostingList.postings` vec.
    /// Block i spans `postings[block_meta[i-1].end_offset .. block_meta[i].end_offset]`
    /// (block 0 starts at 0).
    pub end_offset: u32,
    /// Maximum doc_id in this block. Since postings are sorted by doc_id,
    /// this is always `postings[end_offset - 1].doc_id` (no scan needed).
    pub max_doc_id: u32,
    /// Maximum term_freq in this block. Used by WAND for per-block score upper bounds
    /// (combined with global `min_doc_len` to compute a conservative BM25 upper bound).
    pub max_tf: u32,
}

/// Posting list for a single term. Postings are sorted by doc_id.
///
/// `block_meta` is derived from `postings` and rebuilt by
/// [`Bm25Index::rebuild_lookups()`]. It is empty during index building
/// (when documents are being added) and populated after deserialization or
/// compaction. Code that reads `block_meta` must handle the empty case
/// (fall back to flat iteration/search).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PostingList {
    /// Postings sorted by doc_id
    pub postings: Vec<Posting>,
    /// Block metadata for navigation. Derived — not serialized.
    /// Empty during index building; populated by `rebuild_lookups()`.
    #[serde(skip)]
    pub(crate) block_meta: Vec<BlockMeta>,
}

impl PostingList {
    /// Rebuild block metadata from the flat postings array.
    ///
    /// Single O(n) pass: `max_doc_id` is free (last posting in sorted block),
    /// `max_tf` requires scanning each block. Negligible vs decompression cost.
    pub fn rebuild_block_meta(&mut self) {
        self.block_meta.clear();
        if self.postings.is_empty() {
            return;
        }
        debug_assert!(
            self.postings.len() <= u32::MAX as usize,
            "posting list exceeds u32 addressability"
        );

        let mut start = 0usize;
        while start < self.postings.len() {
            let end = (start + POSTING_BLOCK_SIZE).min(self.postings.len());
            let block = &self.postings[start..end];
            // max_doc_id: free from sort order (last element in sorted block)
            let max_doc_id = block.last().unwrap().doc_id;
            // max_tf: requires scanning the block
            let max_tf = block.iter().map(|p| p.term_freq).max().unwrap_or(0);
            self.block_meta.push(BlockMeta {
                end_offset: end as u32,
                max_doc_id,
                max_tf,
            });
            start = end;
        }
    }

    /// Get the postings slice for block at `block_idx`.
    ///
    /// # Panics
    /// Panics if `block_idx >= self.block_meta.len()` or if `block_meta` is empty.
    pub fn block_postings(&self, block_idx: usize) -> &[Posting] {
        debug_assert!(
            block_idx < self.block_meta.len(),
            "block_idx {block_idx} out of range (num_blocks={})",
            self.block_meta.len()
        );
        let start = if block_idx == 0 {
            0
        } else {
            self.block_meta[block_idx - 1].end_offset as usize
        };
        let end = self.block_meta[block_idx].end_offset as usize;
        &self.postings[start..end]
    }

    /// Find the first block whose `max_doc_id >= target_doc_id`.
    /// Returns `None` if all blocks have `max_doc_id < target_doc_id`.
    pub fn block_containing(&self, target_doc_id: u32) -> Option<usize> {
        let idx = self
            .block_meta
            .partition_point(|bm| bm.max_doc_id < target_doc_id);
        (idx < self.block_meta.len()).then_some(idx)
    }

    /// Number of blocks (0 if block_meta not yet populated).
    pub fn num_blocks(&self) -> usize {
        self.block_meta.len()
    }
}

/// Document metadata entry: maps internal doc_id to identity and length.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocMeta {
    /// The full document key (ledger_alias + subject_iri)
    pub doc_key: DocKey,
    /// Total term count in this document (for BM25 length normalization)
    pub doc_len: u32,
}

// ============================================================================
// Term Index Entry
// ============================================================================

/// Entry in the term index.
///
/// Maps a term to its global index and tracks document frequency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermEntry {
    /// Global index for this term (used as index into posting_lists)
    pub idx: u32,
    /// Number of documents containing this term
    pub doc_freq: u32,
}

impl TermEntry {
    /// Create a new term entry with the given index
    pub fn new(idx: u32) -> Self {
        Self { idx, doc_freq: 0 }
    }

    /// Increment the document frequency
    pub fn inc_doc_freq(&mut self) {
        self.doc_freq += 1;
    }

    /// Decrement the document frequency (for document removal)
    pub fn dec_doc_freq(&mut self) {
        self.doc_freq = self.doc_freq.saturating_sub(1);
    }
}

// ============================================================================
// BM25 Configuration
// ============================================================================

/// BM25 scoring parameters.
///
/// Default values match the legacy implementation:
/// - k1 = 1.2 (term frequency saturation)
/// - b = 0.75 (document length normalization)
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Bm25Config {
    /// Term frequency saturation parameter (default: 1.2)
    pub k1: f64,
    /// Document length normalization parameter (default: 0.75)
    pub b: f64,
}

impl Default for Bm25Config {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

impl Bm25Config {
    /// Create a new BM25 config with custom parameters
    pub fn new(k1: f64, b: f64) -> Self {
        Self { k1, b }
    }
}

// ============================================================================
// Corpus Statistics
// ============================================================================

/// Statistics about the indexed corpus.
///
/// Used for BM25 IDF calculation and document length normalization.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Bm25Stats {
    /// Total number of documents in the index
    pub num_docs: u64,
    /// Total number of terms across all documents
    pub total_terms: u64,
}

impl Bm25Stats {
    /// Create new empty stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Average document length
    pub fn avg_doc_len(&self) -> f64 {
        if self.num_docs == 0 {
            0.0
        } else {
            self.total_terms as f64 / self.num_docs as f64
        }
    }

    /// Add a document with the given length to the statistics
    pub fn add_doc(&mut self, doc_len: u32) {
        self.num_docs += 1;
        self.total_terms += doc_len as u64;
    }

    /// Remove a document with the given length from the statistics
    pub fn remove_doc(&mut self, doc_len: u32) {
        self.num_docs = self.num_docs.saturating_sub(1);
        self.total_terms = self.total_terms.saturating_sub(doc_len as u64);
    }
}

// ============================================================================
// Watermarks (Multi-Ledger Support)
// ============================================================================

/// Watermark tracking for multi-ledger graph sources.
///
/// BM25 is valid at `T` iff ALL dependency ledgers have watermark >= T.
///
/// Uses `BTreeMap` for deterministic serialization (content-addressable snapshots).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GraphSourceWatermark {
    /// Per-ledger watermarks (ledger_alias -> commit_t)
    pub ledger_watermarks: BTreeMap<String, i64>,
}

impl GraphSourceWatermark {
    /// Create a new empty watermark tracker
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with initial ledger watermarks
    pub fn with_watermarks(watermarks: BTreeMap<String, i64>) -> Self {
        Self {
            ledger_watermarks: watermarks,
        }
    }

    /// Effective t for "as-of" queries (minimum of all ledgers)
    pub fn effective_t(&self) -> i64 {
        self.ledger_watermarks.values().copied().min().unwrap_or(0)
    }

    /// Check if graph source can answer query at target_t
    pub fn is_valid_at(&self, target_t: i64) -> bool {
        self.ledger_watermarks.values().all(|&t| t >= target_t)
    }

    /// Update watermark for a specific ledger
    pub fn update(&mut self, ledger_alias: &str, t: i64) {
        self.ledger_watermarks.insert(ledger_alias.to_string(), t);
    }

    /// Get watermark for a specific ledger
    pub fn get(&self, ledger_alias: &str) -> Option<i64> {
        self.ledger_watermarks.get(ledger_alias).copied()
    }
}

// ============================================================================
// Property Dependencies
// ============================================================================

/// Property dependencies for incremental updates.
///
/// Stores IRIs (config portable), compiled to SIDs per-ledger at runtime
/// for fast flake filtering.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PropertyDeps {
    /// IRIs of properties that trigger reindexing (config-level)
    pub property_iris: HashSet<Arc<str>>,
}

impl PropertyDeps {
    /// Create new empty property dependencies
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with initial property IRIs
    pub fn with_iris(iris: impl IntoIterator<Item = impl Into<Arc<str>>>) -> Self {
        Self {
            property_iris: iris.into_iter().map(Into::into).collect(),
        }
    }

    /// Add a property IRI
    pub fn add(&mut self, iri: impl Into<Arc<str>>) {
        self.property_iris.insert(iri.into());
    }

    /// Check if a property IRI is tracked
    pub fn contains(&self, iri: &str) -> bool {
        self.property_iris.iter().any(|i| i.as_ref() == iri)
    }

    /// Number of tracked properties
    pub fn len(&self) -> usize {
        self.property_iris.len()
    }

    /// Check if no properties are tracked
    pub fn is_empty(&self) -> bool {
        self.property_iris.is_empty()
    }

    /// Extract property dependencies from a BM25 indexing query configuration.
    ///
    /// The indexing query format is:
    /// ```json
    /// {
    ///   "ledger": "docs",
    ///   "query": {
    ///     "@context": {"ex": "http://example.org/"},
    ///     "where": [{"@id": "?x", "@type": "ex:Article"}],
    ///     "select": {"?x": ["@id", "ex:title", "ex:content"]}
    ///   }
    /// }
    /// ```
    ///
    /// Extracts property IRIs from:
    /// - WHERE clause patterns (predicate keys like "@type", "ex:title")
    /// - SELECT clause property arrays (excluding "@id")
    ///
    /// The `@context` is used to expand prefixed IRIs to full IRIs.
    pub fn from_indexing_query(config: &serde_json::Value) -> Self {
        let mut deps = PropertyDeps::new();

        // Extract @context for prefix expansion
        let query = config.get("query").unwrap_or(config);
        let context = query.get("@context");

        // Helper to expand a prefixed IRI using context
        let expand_iri = |key: &str| -> Option<Arc<str>> {
            // Variables are not predicates.
            if key.starts_with('?') {
                return None;
            }

            // JSON-LD @type is rdf:type, and must be tracked for incremental updates.
            if key == "@type" {
                return Some(Arc::from(fluree_vocab::rdf::TYPE));
            }

            // Other JSON-LD keywords are not tracked as predicates.
            if key.starts_with('@') {
                return None;
            }

            // Handle prefixed IRIs (e.g., "ex:title")
            if let Some(colon_pos) = key.find(':') {
                let prefix = &key[..colon_pos];
                let local = &key[colon_pos + 1..];

                // Look up prefix in context
                if let Some(ctx) = context {
                    if let Some(base) = ctx.get(prefix).and_then(|v| v.as_str()) {
                        return Some(Arc::from(format!("{base}{local}")));
                    }
                }
            }

            // Already a full IRI or no context match - use as-is
            Some(Arc::from(key))
        };

        // Extract properties from WHERE clause patterns
        if let Some(where_clause) = query.get("where") {
            Self::extract_where_properties(where_clause, &expand_iri, &mut deps);
        }

        // Extract properties from SELECT clause
        if let Some(select) = query.get("select") {
            Self::extract_select_properties(select, &expand_iri, &mut deps);
        }

        deps
    }

    /// Extract properties from WHERE clause patterns (recursive)
    fn extract_where_properties<F>(
        value: &serde_json::Value,
        expand_iri: &F,
        deps: &mut PropertyDeps,
    ) where
        F: Fn(&str) -> Option<Arc<str>>,
    {
        match value {
            serde_json::Value::Array(arr) => {
                for item in arr {
                    Self::extract_where_properties(item, expand_iri, deps);
                }
            }
            serde_json::Value::Object(map) => {
                for (key, val) in map {
                    // Extract property IRI from predicate key
                    if let Some(iri) = expand_iri(key) {
                        deps.add(iri);
                    }
                    // Recurse into nested patterns
                    Self::extract_where_properties(val, expand_iri, deps);
                }
            }
            _ => {}
        }
    }

    /// Extract properties from SELECT clause
    fn extract_select_properties<F>(
        value: &serde_json::Value,
        expand_iri: &F,
        deps: &mut PropertyDeps,
    ) where
        F: Fn(&str) -> Option<Arc<str>>,
    {
        match value {
            // SELECT as object: {"?x": ["@id", "ex:title", "ex:content"]}
            serde_json::Value::Object(map) => {
                for (_var, props) in map {
                    if let serde_json::Value::Array(arr) = props {
                        for prop in arr {
                            if let Some(s) = prop.as_str() {
                                if let Some(iri) = expand_iri(s) {
                                    deps.add(iri);
                                }
                            }
                        }
                    }
                }
            }
            // SELECT as array of property strings
            serde_json::Value::Array(arr) => {
                for prop in arr {
                    if let Some(s) = prop.as_str() {
                        if let Some(iri) = expand_iri(s) {
                            deps.add(iri);
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

// ============================================================================
// Compiled Property Dependencies (Per-Ledger)
// ============================================================================

/// Compiled property dependencies for a specific ledger.
///
/// Converts IRI-based `PropertyDeps` to SID-based for efficient flake filtering.
/// This allows O(1) lookup when checking if a flake's predicate triggers reindexing.
#[derive(Debug, Clone, Default)]
pub struct CompiledPropertyDeps {
    /// Predicate SIDs that trigger reindexing for this ledger
    pub predicate_sids: HashSet<fluree_db_core::Sid>,
}

impl CompiledPropertyDeps {
    /// Create new empty compiled deps
    pub fn new() -> Self {
        Self::default()
    }

    /// Compile PropertyDeps to SIDs using a ledger's namespace encoding.
    ///
    /// IRIs that cannot be encoded are silently skipped (they don't exist in
    /// this ledger's namespace table yet).
    pub fn compile<F>(deps: &PropertyDeps, encode_iri: F) -> Self
    where
        F: Fn(&str) -> Option<fluree_db_core::Sid>,
    {
        let predicate_sids = deps
            .property_iris
            .iter()
            .filter_map(|iri| encode_iri(iri.as_ref()))
            .collect();
        Self { predicate_sids }
    }

    /// Check if a predicate SID triggers reindexing
    pub fn contains(&self, sid: &fluree_db_core::Sid) -> bool {
        self.predicate_sids.contains(sid)
    }

    /// Number of compiled predicates
    pub fn len(&self) -> usize {
        self.predicate_sids.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.predicate_sids.is_empty()
    }

    /// Find subjects affected by a set of flakes.
    ///
    /// Filters flakes by predicate SID (O(1) lookup) and collects unique subject SIDs.
    /// This is the core of incremental update detection: given the flakes from a commit,
    /// determine which subjects need to be reindexed.
    pub fn affected_subjects(&self, flakes: &[Flake]) -> HashSet<fluree_db_core::Sid> {
        flakes
            .iter()
            .filter(|f| self.predicate_sids.contains(&f.p))
            .map(|f| f.s.clone())
            .collect()
    }

    /// Find subjects affected by a set of flakes, filtering by transaction time range.
    ///
    /// Like `affected_subjects`, but only considers flakes with `from_t < t <= to_t`.
    pub fn affected_subjects_in_range(
        &self,
        flakes: &[Flake],
        from_t: i64,
        to_t: i64,
    ) -> HashSet<fluree_db_core::Sid> {
        flakes
            .iter()
            .filter(|f| f.t > from_t && f.t <= to_t)
            .filter(|f| self.predicate_sids.contains(&f.p))
            .map(|f| f.s.clone())
            .collect()
    }
}

// ============================================================================
// Main BM25 Index
// ============================================================================

/// BM25 Full-Text Search Index
///
/// Main index structure containing:
/// - Term dictionary mapping terms to global indices
/// - Inverted posting lists (term → doc_ids + term frequencies)
/// - Document metadata (doc_id → DocKey + doc_length)
/// - Corpus statistics for scoring
/// - Multi-ledger watermarks
/// - Property dependencies for incremental updates
///
/// Uses `BTreeMap` for terms to ensure deterministic serialization.
/// `compact()` is called before serialization to ensure CAS-deterministic output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bm25Index {
    /// Term dictionary: term -> TermEntry (BTreeMap for deterministic order)
    pub terms: BTreeMap<Arc<str>, TermEntry>,
    /// Posting lists indexed by term_idx. Each PostingList is sorted by doc_id.
    pub posting_lists: Vec<PostingList>,
    /// Document metadata indexed by doc_id. None = tombstoned (lazily deleted).
    pub doc_meta: Vec<Option<DocMeta>>,
    /// Reverse lookup: DocKey → doc_id (not serialized — rebuilt on deserialize)
    #[serde(skip)]
    doc_id_lookup: HashMap<DocKey, u32>,
    /// Forward lookup: doc_id → term indices in this doc (for lazy removal)
    #[serde(skip)]
    doc_terms: Vec<Option<Vec<u32>>>,
    /// Reverse lookup: term_idx → term string (for efficient doc_freq updates)
    #[serde(skip)]
    idx_to_term: Vec<Arc<str>>,
    /// Corpus statistics
    pub stats: Bm25Stats,
    /// BM25 configuration parameters
    pub config: Bm25Config,
    /// Multi-ledger watermarks
    pub watermark: GraphSourceWatermark,
    /// Property dependencies for incremental updates
    pub property_deps: PropertyDeps,
    /// Next term index to allocate
    next_term_idx: u32,
    /// Next document ID to allocate
    next_doc_id: u32,
}

impl Default for Bm25Index {
    fn default() -> Self {
        Self::new()
    }
}

impl Bm25Index {
    /// Create a new empty BM25 index with default configuration
    pub fn new() -> Self {
        Self::with_config(Bm25Config::default())
    }

    /// Create a new empty BM25 index with custom configuration
    pub fn with_config(config: Bm25Config) -> Self {
        Self {
            terms: BTreeMap::new(),
            posting_lists: Vec::new(),
            doc_meta: Vec::new(),
            doc_id_lookup: HashMap::new(),
            doc_terms: Vec::new(),
            idx_to_term: Vec::new(),
            stats: Bm25Stats::new(),
            config,
            watermark: GraphSourceWatermark::new(),
            property_deps: PropertyDeps::new(),
            next_term_idx: 0,
            next_doc_id: 0,
        }
    }

    /// Get or create a term entry, returning its global index.
    /// Also extends posting_lists and idx_to_term for new terms.
    pub fn get_or_create_term(&mut self, term: &str) -> u32 {
        if let Some(entry) = self.terms.get(term) {
            return entry.idx;
        }

        let idx = self.next_term_idx;
        self.next_term_idx += 1;

        let term_arc: Arc<str> = Arc::from(term);
        self.terms.insert(term_arc.clone(), TermEntry::new(idx));

        // Extend posting_lists to accommodate the new term
        while self.posting_lists.len() <= idx as usize {
            self.posting_lists.push(PostingList::default());
        }

        // Extend idx_to_term
        while self.idx_to_term.len() <= idx as usize {
            self.idx_to_term.push(Arc::from(""));
        }
        self.idx_to_term[idx as usize] = term_arc;

        idx
    }

    /// Get the term entry for a given term (if it exists)
    pub fn get_term(&self, term: &str) -> Option<&TermEntry> {
        self.terms.get(term)
    }

    /// Get the global index for a term (if it exists)
    pub fn term_idx(&self, term: &str) -> Option<u32> {
        self.terms.get(term).map(|e| e.idx)
    }

    /// Add a document to the index with pre-computed term frequencies.
    ///
    /// `term_freqs` maps term strings to their frequency in the document.
    pub fn add_document(&mut self, doc_key: DocKey, term_freqs: HashMap<&str, u32>) {
        let doc_id = self.next_doc_id;
        self.next_doc_id += 1;

        // Compute doc_len
        let doc_len: u32 = term_freqs.values().sum();

        // Extend doc_meta and doc_terms to accommodate the new doc_id
        while self.doc_meta.len() <= doc_id as usize {
            self.doc_meta.push(None);
        }
        while self.doc_terms.len() <= doc_id as usize {
            self.doc_terms.push(None);
        }

        // Store document metadata
        self.doc_meta[doc_id as usize] = Some(DocMeta {
            doc_key: doc_key.clone(),
            doc_len,
        });

        // Insert into reverse lookup
        self.doc_id_lookup.insert(doc_key, doc_id);

        // Build posting entries and track term indices
        let mut term_indices = Vec::with_capacity(term_freqs.len());

        for (term, tf) in term_freqs {
            if tf == 0 {
                continue;
            }

            let idx = self.get_or_create_term(term);
            term_indices.push(idx);

            // Append posting to the term's posting list
            self.posting_lists[idx as usize].postings.push(Posting {
                doc_id,
                term_freq: tf,
            });

            // Update document frequency for this term
            if let Some(entry) = self.terms.get_mut(term) {
                entry.inc_doc_freq();
            }
        }

        // Store term indices for this doc (for lazy removal)
        self.doc_terms[doc_id as usize] = Some(term_indices);

        // Update corpus statistics
        self.stats.add_doc(doc_len);
    }

    /// Upsert a document: if it exists, remove it first to maintain correct stats.
    ///
    /// This is the preferred method for incremental updates where a document's
    /// content may have changed. It ensures:
    /// - Document frequency counts remain accurate
    /// - Corpus statistics (num_docs, total_terms) remain accurate
    ///
    /// Returns `true` if this was an update (document existed), `false` if insert.
    pub fn upsert_document(&mut self, doc_key: DocKey, term_freqs: HashMap<&str, u32>) -> bool {
        let was_update = self.remove_document(&doc_key);
        self.add_document(doc_key, term_freqs);
        was_update
    }

    /// Remove a document from the index (lazy deletion).
    ///
    /// Tombstones the doc_meta entry and decrements doc_freq for each term,
    /// but does NOT remove postings from posting lists. The scorer skips
    /// tombstoned doc_ids. Stale postings are cleaned up by `compact()`.
    pub fn remove_document(&mut self, doc_key: &DocKey) -> bool {
        let Some(&doc_id) = self.doc_id_lookup.get(doc_key) else {
            return false;
        };

        // Get doc_len before tombstoning
        let doc_len = match &self.doc_meta[doc_id as usize] {
            Some(meta) => meta.doc_len,
            None => return false, // Already tombstoned
        };

        // Decrement doc_freq for each term in this document
        if let Some(Some(term_indices)) = self.doc_terms.get(doc_id as usize) {
            for &term_idx in term_indices {
                if let Some(term_str) = self.idx_to_term.get(term_idx as usize) {
                    if let Some(entry) = self.terms.get_mut(term_str.as_ref()) {
                        entry.dec_doc_freq();
                    }
                }
            }
        }

        // Tombstone doc_meta
        self.doc_meta[doc_id as usize] = None;
        // Clear doc_terms
        if (doc_id as usize) < self.doc_terms.len() {
            self.doc_terms[doc_id as usize] = None;
        }
        // Remove from reverse lookup
        self.doc_id_lookup.remove(doc_key);
        // Update stats
        self.stats.remove_doc(doc_len);

        true
    }

    /// Number of documents in the index (excluding tombstoned)
    pub fn num_docs(&self) -> u64 {
        self.stats.num_docs
    }

    /// Number of unique terms in the index
    pub fn num_terms(&self) -> usize {
        self.terms.len()
    }

    /// Check if a document exists in the index
    pub fn contains_doc(&self, doc_key: &DocKey) -> bool {
        self.doc_id_lookup.contains_key(doc_key)
    }

    /// Get the document metadata for a document (if it exists and is not tombstoned)
    pub fn get_doc_meta(&self, doc_key: &DocKey) -> Option<&DocMeta> {
        let &doc_id = self.doc_id_lookup.get(doc_key)?;
        self.doc_meta.get(doc_id as usize)?.as_ref()
    }

    /// Iterate over all live document keys in the index
    pub fn iter_doc_keys(&self) -> impl Iterator<Item = &DocKey> {
        self.doc_meta
            .iter()
            .filter_map(|opt| opt.as_ref())
            .map(|meta| &meta.doc_key)
    }

    /// Create a Bm25Index from raw components (used by deserialization/conversion).
    ///
    /// Caller must call `rebuild_lookups()` after construction.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts(
        terms: BTreeMap<Arc<str>, TermEntry>,
        posting_lists: Vec<PostingList>,
        doc_meta: Vec<Option<DocMeta>>,
        stats: Bm25Stats,
        config: Bm25Config,
        watermark: GraphSourceWatermark,
        property_deps: PropertyDeps,
        next_term_idx: u32,
        next_doc_id: u32,
    ) -> Self {
        Self {
            terms,
            posting_lists,
            doc_meta,
            doc_id_lookup: HashMap::new(),
            doc_terms: Vec::new(),
            idx_to_term: Vec::new(),
            stats,
            config,
            watermark,
            property_deps,
            next_term_idx,
            next_doc_id,
        }
    }

    /// Get a posting list by term index
    pub fn get_posting_list(&self, term_idx: u32) -> Option<&PostingList> {
        self.posting_lists.get(term_idx as usize)
    }

    /// Get the next_doc_id (used by scorer for Vec sizing)
    pub fn next_doc_id(&self) -> u32 {
        self.next_doc_id
    }

    /// Look up doc_id for a DocKey (used by scorer)
    pub fn doc_id_for(&self, doc_key: &DocKey) -> Option<u32> {
        self.doc_id_lookup.get(doc_key).copied()
    }

    /// Rebuild transient lookup tables from serialized data.
    ///
    /// Must be called after deserialization. Reconstructs:
    /// - `doc_id_lookup`: DocKey → doc_id from doc_meta
    /// - `doc_terms`: doc_id → term indices from posting_lists (O(total_postings))
    /// - `idx_to_term`: term_idx → term string from terms BTreeMap
    pub fn rebuild_lookups(&mut self) {
        // Rebuild doc_id_lookup
        self.doc_id_lookup.clear();
        for (doc_id, opt_meta) in self.doc_meta.iter().enumerate() {
            if let Some(meta) = opt_meta {
                self.doc_id_lookup
                    .insert(meta.doc_key.clone(), doc_id as u32);
            }
        }

        // Rebuild idx_to_term
        self.idx_to_term = vec![Arc::from(""); self.next_term_idx as usize];
        for (term_str, entry) in &self.terms {
            if (entry.idx as usize) < self.idx_to_term.len() {
                self.idx_to_term[entry.idx as usize] = term_str.clone();
            }
        }

        // Rebuild doc_terms by scanning all posting lists.
        // Invariant: each (term_idx, doc_id) pair should appear at most once.
        self.doc_terms = vec![None; self.next_doc_id as usize];
        for (term_idx, posting_list) in self.posting_lists.iter().enumerate() {
            for posting in &posting_list.postings {
                let doc_id = posting.doc_id as usize;
                if doc_id < self.doc_terms.len() {
                    // Only track for live docs
                    if self
                        .doc_meta
                        .get(doc_id)
                        .and_then(|opt| opt.as_ref())
                        .is_some()
                    {
                        self.doc_terms[doc_id]
                            .get_or_insert_with(Vec::new)
                            .push(term_idx as u32);
                    }
                }
            }
        }

        // Debug-only: verify no duplicate term indices per doc (would cause
        // over-decrement of doc_freq on removal if violated)
        #[cfg(debug_assertions)]
        for (doc_id, opt_terms) in self.doc_terms.iter().enumerate() {
            if let Some(terms) = opt_terms {
                let mut sorted = terms.clone();
                sorted.sort();
                let before = sorted.len();
                sorted.dedup();
                debug_assert_eq!(
                    before,
                    sorted.len(),
                    "doc_terms[{doc_id}] contains duplicate term indices — \
                     posting lists have duplicate (term, doc) entries"
                );
            }
        }

        // Rebuild block metadata for all posting lists
        for pl in &mut self.posting_lists {
            pl.rebuild_block_meta();
        }
    }

    /// Compact the index: drop tombstones, renumber doc_ids and term_idx deterministically.
    ///
    /// This is the only place that physically rewrites posting lists. After compaction:
    /// - doc_ids are 0..N assigned in BTreeMap<DocKey> order (deterministic)
    /// - term_idx are 0..M assigned in BTreeMap<Arc<str>> key order (deterministic)
    /// - All tombstoned entries and stale postings are removed
    /// - Two indexes with the same logical content produce identical serialized bytes
    pub fn compact(&mut self) {
        // Collect live docs in deterministic order (BTreeMap<DocKey, _>)
        let mut live_docs: BTreeMap<DocKey, u32> = BTreeMap::new(); // DocKey → old_doc_len
        let mut old_doc_id_to_terms: HashMap<u32, Vec<(u32, u32)>> = HashMap::new(); // old_doc_id → [(old_term_idx, tf)]

        // Build old_doc_id → term_freqs from posting lists
        for (term_idx, posting_list) in self.posting_lists.iter().enumerate() {
            for posting in &posting_list.postings {
                let doc_id = posting.doc_id as usize;
                if doc_id < self.doc_meta.len() {
                    if let Some(meta) = &self.doc_meta[doc_id] {
                        live_docs
                            .entry(meta.doc_key.clone())
                            .or_insert(meta.doc_len);
                        old_doc_id_to_terms
                            .entry(posting.doc_id)
                            .or_default()
                            .push((term_idx as u32, posting.term_freq));
                    }
                }
            }
        }

        // Also collect live docs that may have no postings (shouldn't happen, but be safe)
        for (doc_id, opt_meta) in self.doc_meta.iter().enumerate() {
            if let Some(meta) = opt_meta {
                live_docs
                    .entry(meta.doc_key.clone())
                    .or_insert(meta.doc_len);
                // Ensure entry exists in old_doc_id_to_terms
                old_doc_id_to_terms.entry(doc_id as u32).or_default();
            }
        }

        // Assign new doc_ids in BTreeMap<DocKey> order
        let mut old_doc_key_to_new_id: HashMap<DocKey, u32> = HashMap::new();
        let mut new_doc_meta: Vec<Option<DocMeta>> = Vec::with_capacity(live_docs.len());
        for (new_id, (doc_key, doc_len)) in live_docs.iter().enumerate() {
            old_doc_key_to_new_id.insert(doc_key.clone(), new_id as u32);
            new_doc_meta.push(Some(DocMeta {
                doc_key: doc_key.clone(),
                doc_len: *doc_len,
            }));
        }

        // Reassign term_idx in BTreeMap<Arc<str>> key order
        let mut old_term_idx_to_new: HashMap<u32, u32> = HashMap::new();
        let mut new_terms: BTreeMap<Arc<str>, TermEntry> = BTreeMap::new();
        for (new_idx, (term_str, old_entry)) in self.terms.iter().enumerate() {
            old_term_idx_to_new.insert(old_entry.idx, new_idx as u32);
            new_terms.insert(
                term_str.clone(),
                TermEntry {
                    idx: new_idx as u32,
                    doc_freq: old_entry.doc_freq,
                },
            );
        }

        // Build new posting lists using new term_idx and new doc_ids
        let num_terms = new_terms.len();
        let mut new_posting_lists: Vec<PostingList> =
            (0..num_terms).map(|_| PostingList::default()).collect();

        // Build from the old_doc_id_to_terms mapping
        // We need to map: old_doc_id → DocKey → new_doc_id, and old_term_idx → new_term_idx
        let old_doc_id_to_key: HashMap<u32, DocKey> = self
            .doc_meta
            .iter()
            .enumerate()
            .filter_map(|(id, opt)| opt.as_ref().map(|m| (id as u32, m.doc_key.clone())))
            .collect();

        for (old_doc_id, term_freqs) in &old_doc_id_to_terms {
            if let Some(doc_key) = old_doc_id_to_key.get(old_doc_id) {
                if let Some(&new_doc_id) = old_doc_key_to_new_id.get(doc_key) {
                    for &(old_term_idx, tf) in term_freqs {
                        if let Some(&new_term_idx) = old_term_idx_to_new.get(&old_term_idx) {
                            new_posting_lists[new_term_idx as usize]
                                .postings
                                .push(Posting {
                                    doc_id: new_doc_id,
                                    term_freq: tf,
                                });
                        }
                    }
                }
            }
        }

        // Sort each posting list by doc_id
        for pl in &mut new_posting_lists {
            pl.postings.sort_by_key(|p| p.doc_id);
        }

        // Apply
        self.terms = new_terms;
        self.posting_lists = new_posting_lists;
        self.doc_meta = new_doc_meta;
        self.next_doc_id = live_docs.len() as u32;
        self.next_term_idx = num_terms as u32;

        // Rebuild transient lookups
        self.rebuild_lookups();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_doc_key_equality() {
        let k1 = DocKey::new("ledger:main", "http://example.org/1");
        let k2 = DocKey::new("ledger:main", "http://example.org/1");
        let k3 = DocKey::new("ledger:main", "http://example.org/2");
        let k4 = DocKey::new("other:main", "http://example.org/1");

        assert_eq!(k1, k2);
        assert_ne!(k1, k3);
        assert_ne!(k1, k4);
    }

    #[test]
    fn test_bm25_stats_avg_doc_len() {
        let mut stats = Bm25Stats::new();
        assert_eq!(stats.avg_doc_len(), 0.0);

        stats.add_doc(10);
        stats.add_doc(20);
        stats.add_doc(30);

        assert_eq!(stats.num_docs, 3);
        assert_eq!(stats.total_terms, 60);
        assert!((stats.avg_doc_len() - 20.0).abs() < 0.001);
    }

    #[test]
    fn test_graph_source_watermark() {
        let mut wm = GraphSourceWatermark::new();

        wm.update("ledger1:main", 10);
        wm.update("ledger2:main", 20);

        assert_eq!(wm.effective_t(), 10);
        assert!(wm.is_valid_at(5));
        assert!(wm.is_valid_at(10));
        assert!(!wm.is_valid_at(15));
    }

    #[test]
    fn test_bm25_index_add_document() {
        let mut index = Bm25Index::new();

        let doc_key = DocKey::new("ledger:main", "http://example.org/doc1");
        let mut term_freqs = HashMap::new();
        term_freqs.insert("hello", 2);
        term_freqs.insert("world", 1);

        index.add_document(doc_key.clone(), term_freqs);

        assert_eq!(index.num_docs(), 1);
        assert_eq!(index.num_terms(), 2);
        assert!(index.contains_doc(&doc_key));

        let meta = index.get_doc_meta(&doc_key).unwrap();
        assert_eq!(meta.doc_len, 3); // 2 + 1
    }

    #[test]
    fn test_bm25_index_remove_document() {
        let mut index = Bm25Index::new();

        let doc_key = DocKey::new("ledger:main", "http://example.org/doc1");
        let mut term_freqs = HashMap::new();
        term_freqs.insert("hello", 2);
        term_freqs.insert("world", 1);

        index.add_document(doc_key.clone(), term_freqs);
        assert_eq!(index.num_docs(), 1);

        let removed = index.remove_document(&doc_key);
        assert!(removed);
        assert_eq!(index.num_docs(), 0);
        assert!(!index.contains_doc(&doc_key));
    }

    #[test]
    fn test_bm25_index_upsert_document() {
        let mut index = Bm25Index::new();

        let doc_key = DocKey::new("ledger:main", "http://example.org/doc1");

        // Initial insert
        let mut term_freqs1 = HashMap::new();
        term_freqs1.insert("hello", 2);
        term_freqs1.insert("world", 1);

        let was_update = index.upsert_document(doc_key.clone(), term_freqs1);
        assert!(!was_update, "First insert should not be an update");
        assert_eq!(index.num_docs(), 1);
        assert_eq!(index.stats.total_terms, 3); // 2 + 1

        // Verify initial doc_freq for "hello"
        let hello_doc_freq = index.get_term("hello").map(|e| e.doc_freq);
        assert_eq!(hello_doc_freq, Some(1));

        // Upsert with different content
        let mut term_freqs2 = HashMap::new();
        term_freqs2.insert("goodbye", 1);
        term_freqs2.insert("world", 2);
        term_freqs2.insert("moon", 1);

        let was_update = index.upsert_document(doc_key.clone(), term_freqs2);
        assert!(was_update, "Second upsert should be an update");
        assert_eq!(index.num_docs(), 1); // Still 1 document
        assert_eq!(index.stats.total_terms, 4); // 1 + 2 + 1

        // Verify doc_freq updated correctly
        let hello_doc_freq = index.get_term("hello").map(|e| e.doc_freq);
        assert_eq!(
            hello_doc_freq,
            Some(0),
            "hello should have doc_freq 0 after upsert"
        );

        let goodbye_doc_freq = index.get_term("goodbye").map(|e| e.doc_freq);
        assert_eq!(goodbye_doc_freq, Some(1), "goodbye should have doc_freq 1");

        let world_doc_freq = index.get_term("world").map(|e| e.doc_freq);
        assert_eq!(
            world_doc_freq,
            Some(1),
            "world should still have doc_freq 1"
        );
    }

    #[test]
    fn test_posting_lists_populated() {
        let mut index = Bm25Index::new();

        let doc1 = DocKey::new("ledger:main", "http://example.org/doc1");
        let mut tf1 = HashMap::new();
        tf1.insert("hello", 2);
        tf1.insert("world", 1);
        index.add_document(doc1, tf1);

        let doc2 = DocKey::new("ledger:main", "http://example.org/doc2");
        let mut tf2 = HashMap::new();
        tf2.insert("hello", 1);
        tf2.insert("rust", 3);
        index.add_document(doc2, tf2);

        // "hello" should appear in both docs
        let hello_idx = index.term_idx("hello").unwrap();
        let hello_pl = index.get_posting_list(hello_idx).unwrap();
        assert_eq!(hello_pl.postings.len(), 2);

        // "world" only in doc1
        let world_idx = index.term_idx("world").unwrap();
        let world_pl = index.get_posting_list(world_idx).unwrap();
        assert_eq!(world_pl.postings.len(), 1);

        // "rust" only in doc2
        let rust_idx = index.term_idx("rust").unwrap();
        let rust_pl = index.get_posting_list(rust_idx).unwrap();
        assert_eq!(rust_pl.postings.len(), 1);
    }

    #[test]
    fn test_lazy_deletion_leaves_postings() {
        let mut index = Bm25Index::new();

        let doc1 = DocKey::new("ledger:main", "http://example.org/doc1");
        let mut tf1 = HashMap::new();
        tf1.insert("hello", 1);
        index.add_document(doc1.clone(), tf1);

        let hello_idx = index.term_idx("hello").unwrap();

        // Posting list has 1 entry before removal
        assert_eq!(index.get_posting_list(hello_idx).unwrap().postings.len(), 1);

        // Remove doc — lazy, postings stay
        index.remove_document(&doc1);

        // Posting list still has 1 entry (stale)
        assert_eq!(index.get_posting_list(hello_idx).unwrap().postings.len(), 1);

        // But doc is not findable
        assert!(!index.contains_doc(&doc1));
        assert_eq!(index.num_docs(), 0);

        // doc_freq should be decremented
        assert_eq!(index.get_term("hello").unwrap().doc_freq, 0);
    }

    #[test]
    fn test_compact_removes_stale_postings() {
        let mut index = Bm25Index::new();

        let doc1 = DocKey::new("ledger:main", "http://example.org/doc1");
        let mut tf1 = HashMap::new();
        tf1.insert("hello", 1);
        index.add_document(doc1.clone(), tf1);

        let doc2 = DocKey::new("ledger:main", "http://example.org/doc2");
        let mut tf2 = HashMap::new();
        tf2.insert("hello", 2);
        index.add_document(doc2.clone(), tf2);

        // Remove doc1 (lazy)
        index.remove_document(&doc1);

        // Before compact: 2 postings for "hello" (one stale)
        let hello_idx = index.term_idx("hello").unwrap();
        assert_eq!(index.get_posting_list(hello_idx).unwrap().postings.len(), 2);

        // Compact
        index.compact();

        // After compact: 1 posting for "hello"
        let hello_idx = index.term_idx("hello").unwrap();
        assert_eq!(index.get_posting_list(hello_idx).unwrap().postings.len(), 1);

        // doc2 still exists
        assert!(index.contains_doc(&doc2));
        assert_eq!(index.num_docs(), 1);
    }

    #[test]
    fn test_compact_deterministic_ids() {
        // Two indexes built in different order should produce identical compact() results
        let mut index_a = Bm25Index::new();
        let mut index_b = Bm25Index::new();

        let doc1 = DocKey::new("ledger:main", "http://example.org/aaa");
        let doc2 = DocKey::new("ledger:main", "http://example.org/bbb");

        let mut tf1 = HashMap::new();
        tf1.insert("alpha", 1);
        tf1.insert("beta", 2);

        let mut tf2 = HashMap::new();
        tf2.insert("beta", 1);
        tf2.insert("gamma", 3);

        // Build in order: doc1, doc2
        index_a.add_document(doc1.clone(), tf1.clone());
        index_a.add_document(doc2.clone(), tf2.clone());

        // Build in reverse order: doc2, doc1
        index_b.add_document(doc2.clone(), tf2);
        index_b.add_document(doc1.clone(), tf1);

        index_a.compact();
        index_b.compact();

        // After compact, both should have doc_id 0 = aaa, doc_id 1 = bbb (BTreeMap order)
        assert_eq!(index_a.doc_meta.len(), index_b.doc_meta.len());
        for i in 0..index_a.doc_meta.len() {
            let a = index_a.doc_meta[i].as_ref().unwrap();
            let b = index_b.doc_meta[i].as_ref().unwrap();
            assert_eq!(a.doc_key, b.doc_key);
            assert_eq!(a.doc_len, b.doc_len);
        }

        // term_idx should also be deterministic (BTreeMap key order: alpha, beta, gamma)
        assert_eq!(index_a.terms.len(), index_b.terms.len());
        for (term, entry_a) in &index_a.terms {
            let entry_b = index_b.terms.get(term).unwrap();
            assert_eq!(entry_a.idx, entry_b.idx, "term_idx mismatch for {term}");
        }

        // Posting lists should be identical
        assert_eq!(index_a.posting_lists.len(), index_b.posting_lists.len());
        for (i, (pl_a, pl_b)) in index_a
            .posting_lists
            .iter()
            .zip(index_b.posting_lists.iter())
            .enumerate()
        {
            assert_eq!(
                pl_a.postings.len(),
                pl_b.postings.len(),
                "posting list len mismatch at idx {i}"
            );
            for (pa, pb) in pl_a.postings.iter().zip(pl_b.postings.iter()) {
                assert_eq!(pa.doc_id, pb.doc_id, "doc_id mismatch at term_idx {i}");
                assert_eq!(
                    pa.term_freq, pb.term_freq,
                    "term_freq mismatch at term_idx {i}"
                );
            }
        }
    }

    #[test]
    fn test_rebuild_lookups() {
        let mut index = Bm25Index::new();

        let doc1 = DocKey::new("ledger:main", "http://example.org/doc1");
        let mut tf1 = HashMap::new();
        tf1.insert("hello", 2);
        tf1.insert("world", 1);
        index.add_document(doc1.clone(), tf1);

        // Clear transient fields to simulate deserialization
        index.doc_id_lookup.clear();
        index.doc_terms = vec![None; index.next_doc_id as usize];
        index.idx_to_term.clear();

        // Rebuild
        index.rebuild_lookups();

        // Verify doc_id_lookup works
        assert!(index.contains_doc(&doc1));
        assert_eq!(index.doc_id_for(&doc1), Some(0));

        // Verify idx_to_term works
        let hello_idx = index.term_idx("hello").unwrap();
        assert_eq!(index.idx_to_term[hello_idx as usize].as_ref(), "hello");

        // Verify doc_terms works
        assert!(index.doc_terms[0].is_some());
        let terms = index.doc_terms[0].as_ref().unwrap();
        assert_eq!(terms.len(), 2); // hello + world
    }

    #[test]
    fn test_property_deps() {
        let mut deps = PropertyDeps::new();

        deps.add("http://schema.org/name");
        deps.add("http://schema.org/description");

        assert!(deps.contains("http://schema.org/name"));
        assert!(!deps.contains("http://schema.org/title"));
        assert_eq!(deps.len(), 2);
    }

    #[test]
    fn test_property_deps_from_indexing_query_basic() {
        use serde_json::json;

        let config = json!({
            "ledger": "docs",
            "query": {
                "@context": {"ex": "http://example.org/"},
                "where": [{"@id": "?x", "@type": "ex:Article"}],
                "select": {"?x": ["@id", "ex:title", "ex:content"]}
            }
        });

        let deps = PropertyDeps::from_indexing_query(&config);

        // Should extract ex:title and ex:content from SELECT (not @id)
        assert!(deps.contains("http://example.org/title"));
        assert!(deps.contains("http://example.org/content"));

        // @id should be excluded; @type should be tracked as rdf:type
        assert!(!deps.contains("@id"));
        assert!(
            deps.contains(fluree_vocab::rdf::TYPE),
            "should include rdf:type when query uses @type"
        );

        // Should have 3 properties (type + title + content)
        assert_eq!(deps.len(), 3);
    }

    #[test]
    fn test_property_deps_from_indexing_query_with_where_properties() {
        use serde_json::json;

        let config = json!({
            "query": {
                "@context": {
                    "ex": "http://example.org/",
                    "schema": "http://schema.org/"
                },
                "where": [
                    {"@id": "?x", "schema:name": "?name"},
                    {"@id": "?x", "ex:category": "news"}
                ],
                "select": {"?x": ["@id", "ex:body"]}
            }
        });

        let deps = PropertyDeps::from_indexing_query(&config);

        // Should extract properties from WHERE clause
        assert!(deps.contains("http://schema.org/name"));
        assert!(deps.contains("http://example.org/category"));

        // And from SELECT clause
        assert!(deps.contains("http://example.org/body"));

        assert_eq!(deps.len(), 3);
    }

    #[test]
    fn test_property_deps_from_indexing_query_no_context() {
        use serde_json::json;

        // Query with full IRIs (no context)
        let config = json!({
            "query": {
                "where": [{"@id": "?x", "http://schema.org/name": "?name"}],
                "select": {"?x": ["@id", "http://schema.org/description"]}
            }
        });

        let deps = PropertyDeps::from_indexing_query(&config);

        // Should preserve full IRIs
        assert!(deps.contains("http://schema.org/name"));
        assert!(deps.contains("http://schema.org/description"));
        assert_eq!(deps.len(), 2);
    }

    #[test]
    fn test_property_deps_from_indexing_query_nested_where() {
        use serde_json::json;

        let config = json!({
            "query": {
                "@context": {"ex": "http://example.org/"},
                "where": [
                    {"@id": "?x", "ex:author": {"@id": "?author", "ex:name": "?authorName"}}
                ],
                "select": ["?x", "?authorName"]
            }
        });

        let deps = PropertyDeps::from_indexing_query(&config);

        // Should extract properties from nested patterns
        assert!(deps.contains("http://example.org/author"));
        assert!(deps.contains("http://example.org/name"));
        assert_eq!(deps.len(), 2);
    }

    #[test]
    fn test_compiled_property_deps() {
        use fluree_db_core::Sid;

        let mut deps = PropertyDeps::new();
        deps.add("http://schema.org/name");
        deps.add("http://schema.org/description");
        deps.add("http://example.org/unknown");

        // Mock encode_iri function - simulates ledger namespace encoding
        let encode_iri = |iri: &str| -> Option<Sid> {
            match iri {
                "http://schema.org/name" => Some(Sid::new(100, "name")),
                "http://schema.org/description" => Some(Sid::new(100, "description")),
                _ => None, // Unknown IRI
            }
        };

        let compiled = CompiledPropertyDeps::compile(&deps, encode_iri);

        // Should have 2 SIDs (unknown IRI skipped)
        assert_eq!(compiled.len(), 2);

        // Should contain the encoded SIDs
        assert!(compiled.contains(&Sid::new(100, "name")));
        assert!(compiled.contains(&Sid::new(100, "description")));

        // Should not contain unknown IRI
        assert!(!compiled.contains(&Sid::new(100, "unknown")));
    }

    #[test]
    fn test_affected_subjects_basic() {
        use fluree_db_core::{Flake, FlakeValue, Sid};

        // Set up compiled deps tracking "name" and "title" predicates
        let mut deps = PropertyDeps::new();
        deps.add("http://schema.org/name");
        deps.add("http://schema.org/title");

        let name_sid = Sid::new(100, "name");
        let title_sid = Sid::new(100, "title");
        let other_sid = Sid::new(100, "other");
        let dt_string = Sid::new(3, "string");

        let encode_iri = |iri: &str| -> Option<Sid> {
            match iri {
                "http://schema.org/name" => Some(name_sid.clone()),
                "http://schema.org/title" => Some(title_sid.clone()),
                _ => None,
            }
        };

        let compiled = CompiledPropertyDeps::compile(&deps, encode_iri);

        // Create test flakes
        let subject1 = Sid::new(1, "alice");
        let subject2 = Sid::new(1, "bob");
        let subject3 = Sid::new(1, "charlie");

        let flakes = vec![
            // alice has name changed - should be affected
            Flake::new(
                subject1.clone(),
                name_sid.clone(),
                FlakeValue::String("Alice".into()),
                dt_string.clone(),
                1,
                true,
                None,
            ),
            // bob has title changed - should be affected
            Flake::new(
                subject2.clone(),
                title_sid.clone(),
                FlakeValue::String("CEO".into()),
                dt_string.clone(),
                1,
                true,
                None,
            ),
            // charlie has "other" property changed - NOT affected
            Flake::new(
                subject3.clone(),
                other_sid.clone(),
                FlakeValue::String("data".into()),
                dt_string.clone(),
                1,
                true,
                None,
            ),
        ];

        let affected = compiled.affected_subjects(&flakes);

        assert_eq!(affected.len(), 2);
        assert!(affected.contains(&subject1));
        assert!(affected.contains(&subject2));
        assert!(!affected.contains(&subject3));
    }

    #[test]
    fn test_affected_subjects_deduplication() {
        use fluree_db_core::{Flake, FlakeValue, Sid};

        let name_sid = Sid::new(100, "name");
        let dt_string = Sid::new(3, "string");
        let subject1 = Sid::new(1, "alice");

        let mut compiled = CompiledPropertyDeps::new();
        compiled.predicate_sids.insert(name_sid.clone());

        // Multiple flakes for the same subject
        let flakes = vec![
            Flake::new(
                subject1.clone(),
                name_sid.clone(),
                FlakeValue::String("Alice".into()),
                dt_string.clone(),
                1,
                true,
                None,
            ),
            Flake::new(
                subject1.clone(),
                name_sid.clone(),
                FlakeValue::String("Alicia".into()),
                dt_string.clone(),
                2,
                true,
                None,
            ),
        ];

        let affected = compiled.affected_subjects(&flakes);

        // Should be deduplicated to 1 subject
        assert_eq!(affected.len(), 1);
        assert!(affected.contains(&subject1));
    }

    #[test]
    fn test_affected_subjects_in_range() {
        use fluree_db_core::{Flake, FlakeValue, Sid};

        let name_sid = Sid::new(100, "name");
        let dt_string = Sid::new(3, "string");
        let subject1 = Sid::new(1, "alice");
        let subject2 = Sid::new(1, "bob");
        let subject3 = Sid::new(1, "charlie");

        let mut compiled = CompiledPropertyDeps::new();
        compiled.predicate_sids.insert(name_sid.clone());

        let flakes = vec![
            // t=5, outside range
            Flake::new(
                subject1.clone(),
                name_sid.clone(),
                FlakeValue::String("Alice".into()),
                dt_string.clone(),
                5,
                true,
                None,
            ),
            // t=10, at lower bound (exclusive, NOT included)
            Flake::new(
                subject2.clone(),
                name_sid.clone(),
                FlakeValue::String("Bob".into()),
                dt_string.clone(),
                10,
                true,
                None,
            ),
            // t=15, in range
            Flake::new(
                subject3.clone(),
                name_sid.clone(),
                FlakeValue::String("Charlie".into()),
                dt_string.clone(),
                15,
                true,
                None,
            ),
        ];

        // Range: 10 < t <= 20
        let affected = compiled.affected_subjects_in_range(&flakes, 10, 20);

        assert_eq!(affected.len(), 1);
        assert!(!affected.contains(&subject1)); // t=5 < 10
        assert!(!affected.contains(&subject2)); // t=10 == from_t (exclusive)
        assert!(affected.contains(&subject3)); // t=15, in range
    }

    #[test]
    fn test_affected_subjects_empty_deps() {
        use fluree_db_core::{Flake, FlakeValue, Sid};

        let compiled = CompiledPropertyDeps::new();

        let name_sid = Sid::new(100, "name");
        let dt_string = Sid::new(3, "string");
        let subject1 = Sid::new(1, "alice");

        let flakes = vec![Flake::new(
            subject1.clone(),
            name_sid.clone(),
            FlakeValue::String("Alice".into()),
            dt_string.clone(),
            1,
            true,
            None,
        )];

        // With no tracked predicates, no subjects should be affected
        let affected = compiled.affected_subjects(&flakes);
        assert!(affected.is_empty());
    }

    // ========================================================================
    // Block metadata tests
    // ========================================================================

    /// Helper: build a PostingList with `n` postings (doc_ids 0..n, tf = doc_id % 7 + 1).
    fn make_posting_list(n: usize) -> PostingList {
        let postings: Vec<Posting> = (0..n)
            .map(|i| Posting {
                doc_id: i as u32,
                term_freq: (i % 7) as u32 + 1,
            })
            .collect();
        PostingList {
            postings,
            block_meta: Vec::new(),
        }
    }

    #[test]
    fn test_block_meta_rebuild_various_sizes() {
        for &n in &[0usize, 1, 127, 128, 129, 256, 300] {
            let mut pl = make_posting_list(n);
            pl.rebuild_block_meta();

            let expected_blocks = if n == 0 {
                0
            } else {
                n.div_ceil(POSTING_BLOCK_SIZE)
            };
            assert_eq!(
                pl.num_blocks(),
                expected_blocks,
                "wrong block count for n={n}"
            );

            if n == 0 {
                continue;
            }

            // Verify end_offset monotonically increasing and within bounds
            let mut prev_end = 0u32;
            for (i, bm) in pl.block_meta.iter().enumerate() {
                assert!(
                    bm.end_offset > prev_end || (i == 0 && bm.end_offset > 0),
                    "end_offset not increasing at block {i} for n={n}"
                );
                assert!(
                    bm.end_offset as usize <= n,
                    "end_offset out of bounds at block {i} for n={n}"
                );
                prev_end = bm.end_offset;
            }
            // Last block covers all postings
            assert_eq!(
                pl.block_meta.last().unwrap().end_offset as usize,
                n,
                "last block end_offset != n for n={n}"
            );

            // Verify max_doc_id == last posting in block (sorted invariant)
            for i in 0..pl.num_blocks() {
                let block = pl.block_postings(i);
                assert_eq!(
                    block.last().unwrap().doc_id,
                    pl.block_meta[i].max_doc_id,
                    "max_doc_id mismatch at block {i} for n={n}"
                );
            }

            // Verify max_tf matches actual max within each block
            for i in 0..pl.num_blocks() {
                let block = pl.block_postings(i);
                let actual_max_tf = block.iter().map(|p| p.term_freq).max().unwrap();
                assert_eq!(
                    actual_max_tf, pl.block_meta[i].max_tf,
                    "max_tf mismatch at block {i} for n={n}"
                );
            }
        }
    }

    #[test]
    fn test_block_containing() {
        let mut pl = make_posting_list(300); // ~3 blocks (0..127, 128..255, 256..299)
        pl.rebuild_block_meta();
        assert_eq!(pl.num_blocks(), 3);

        // doc_id in first block
        assert_eq!(pl.block_containing(0), Some(0));
        assert_eq!(pl.block_containing(50), Some(0));
        assert_eq!(pl.block_containing(127), Some(0));

        // doc_id in second block
        assert_eq!(pl.block_containing(128), Some(1));
        assert_eq!(pl.block_containing(200), Some(1));
        assert_eq!(pl.block_containing(255), Some(1));

        // doc_id in third block
        assert_eq!(pl.block_containing(256), Some(2));
        assert_eq!(pl.block_containing(299), Some(2));

        // doc_id larger than all blocks
        assert_eq!(pl.block_containing(300), None);
        assert_eq!(pl.block_containing(1000), None);

        // Empty posting list
        let empty = PostingList::default();
        assert_eq!(empty.block_containing(0), None);
    }

    #[test]
    fn test_block_postings_slices() {
        let mut pl = make_posting_list(300);
        pl.rebuild_block_meta();

        // First block: 128 postings
        let b0 = pl.block_postings(0);
        assert_eq!(b0.len(), 128);
        assert_eq!(b0[0].doc_id, 0);
        assert_eq!(b0[127].doc_id, 127);

        // Second block: 128 postings
        let b1 = pl.block_postings(1);
        assert_eq!(b1.len(), 128);
        assert_eq!(b1[0].doc_id, 128);
        assert_eq!(b1[127].doc_id, 255);

        // Third block: 44 postings (300 - 256)
        let b2 = pl.block_postings(2);
        assert_eq!(b2.len(), 44);
        assert_eq!(b2[0].doc_id, 256);
        assert_eq!(b2[43].doc_id, 299);
    }

    #[test]
    fn test_score_with_blocks_matches_flat() {
        use crate::bm25::scoring::Bm25Scorer;

        // Build an index large enough to span multiple blocks (>128 docs per term)
        let mut index = Bm25Index::new();
        for i in 0..300 {
            let doc_key = DocKey::new("l:main", format!("http://ex.org/doc{i}"));
            let mut tf = HashMap::new();
            tf.insert("common", (i % 5) as u32 + 1);
            if i % 3 == 0 {
                tf.insert("rare", 1);
            }
            index.add_document(doc_key, tf);
        }
        // rebuild_lookups populates block_meta
        index.rebuild_lookups();

        let terms = &["common", "rare"];
        let scorer_with_blocks = Bm25Scorer::new(&index, terms);

        // Collect scores with blocks
        let scores_with: Vec<(DocKey, f64)> = index
            .iter_doc_keys()
            .map(|dk| {
                let s = scorer_with_blocks.score(dk);
                (dk.clone(), s)
            })
            .collect();

        // Clear block_meta to force flat fallback
        for pl in &mut index.posting_lists {
            pl.block_meta.clear();
        }
        let scorer_flat = Bm25Scorer::new(&index, terms);

        // Collect scores without blocks (flat binary search)
        for (dk, score_with) in &scores_with {
            let score_flat = scorer_flat.score(dk);
            assert_eq!(
                f64::to_bits(*score_with),
                f64::to_bits(score_flat),
                "score mismatch for {:?}: with_blocks={score_with} flat={score_flat}",
                dk.subject_iri
            );
        }
    }

    #[test]
    fn test_block_meta_after_compact() {
        // After compact, block_meta should be rebuilt correctly
        let mut index = Bm25Index::new();
        for i in 0..200 {
            let doc_key = DocKey::new("l:main", format!("http://ex.org/doc{i}"));
            let mut tf = HashMap::new();
            tf.insert("word", i as u32 + 1);
            index.add_document(doc_key, tf);
        }
        // Block meta empty during building
        let word_idx = index.term_idx("word").unwrap();
        assert!(
            index
                .get_posting_list(word_idx)
                .unwrap()
                .block_meta
                .is_empty(),
            "block_meta should be empty during building"
        );

        // compact() calls rebuild_lookups() which populates block_meta
        index.compact();

        let word_idx = index.term_idx("word").unwrap();
        let pl = index.get_posting_list(word_idx).unwrap();
        assert_eq!(pl.num_blocks(), 2); // ceil(200/128) = 2
        assert_eq!(pl.block_postings(0).len(), 128);
        assert_eq!(pl.block_postings(1).len(), 72);
    }
}
