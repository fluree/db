//! BM25 Scoring Algorithm
//!
//! Implements BM25 scoring:
//! - IDF: log(1 + (N - n + 0.5) / (n + 0.5))
//! - Score: Σ IDF * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (doc_len / avg_doc_len)))
//!
//! Default parameters: k1=1.2, b=0.75
//!
//! Scoring iterates posting lists (term → docs) rather than scanning all documents,
//! making query time proportional to the number of matching postings rather than O(N).

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};

use super::index::{Bm25Config, Bm25Index, DocKey, Posting, PostingList};

// ============================================================================
// Top-K Heap (min-heap for WAND)
// ============================================================================

/// Entry in the top-k min-heap.
///
/// Ordering: score ascending, then DocKey descending (reversed).
/// With `Reverse<HeapEntry>`, the heap root is the "worst" entry
/// (lowest score, highest DocKey) — exactly what to evict first.
struct HeapEntry {
    score: f64,
    doc_key: DocKey,
}

impl Eq for HeapEntry {}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.doc_key.cmp(&self.doc_key))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Min-heap of top-k results. The root is the worst entry (lowest score,
/// highest DocKey), enabling O(log k) eviction of the least-qualified result.
struct TopKHeap {
    k: usize,
    heap: BinaryHeap<Reverse<HeapEntry>>,
}

impl TopKHeap {
    fn new(k: usize) -> Self {
        Self {
            k,
            heap: BinaryHeap::with_capacity(k + 1),
        }
    }

    /// Current score threshold for pivot selection.
    /// Returns 0.0 if the heap is not yet full — any positive-scoring doc qualifies.
    fn threshold(&self) -> f64 {
        if self.heap.len() < self.k {
            0.0
        } else {
            self.heap.peek().map(|r| r.0.score).unwrap_or(0.0)
        }
    }

    /// Push a candidate into the heap. Admits when the heap isn't full,
    /// or when the candidate is better than the current worst (using full
    /// HeapEntry Ord — not score alone — to handle tie-replacement correctly).
    fn push(&mut self, score: f64, doc_key: DocKey) {
        let entry = HeapEntry { score, doc_key };
        if self.heap.len() < self.k {
            self.heap.push(Reverse(entry));
        } else if let Some(root) = self.heap.peek() {
            // entry > root means entry is better (higher score, or same score with smaller DocKey)
            if entry > root.0 {
                self.heap.pop();
                self.heap.push(Reverse(entry));
            }
        }
    }

    /// Drain the heap into a Vec sorted by score descending, then DocKey ascending.
    fn into_sorted_vec(self) -> Vec<(DocKey, f64)> {
        let mut results: Vec<(DocKey, f64)> = self
            .heap
            .into_vec()
            .into_iter()
            .map(|r| (r.0.doc_key, r.0.score))
            .collect();
        results.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        results
    }
}

// ============================================================================
// Term Cursor (for WAND traversal)
// ============================================================================

/// Cursor over a single term's posting list, with precomputed per-block
/// BM25 score upper bounds for WAND pivot selection.
struct TermCursor<'a> {
    posting_list: &'a PostingList,
    idf: f64,
    /// Suffix-max of per-block BM25 upper bounds.
    /// `suffix_upper_bounds[i] = max(block_ub[i..])`.
    /// Used for safe pivot selection — valid for any doc_id >= current position.
    suffix_upper_bounds: Vec<f64>,
    /// Current block index (advances forward only).
    current_block: usize,
    /// Current position in the flat postings vec (advances forward only).
    current_pos: usize,
}

impl<'a> TermCursor<'a> {
    /// Create a new cursor with precomputed upper bounds.
    ///
    /// `min_doc_len` is the global minimum document length across all live documents.
    /// BM25 score is monotonically decreasing in doc_len (for k1 > 0, 0 <= b <= 1),
    /// so `min_doc_len` maximizes the score, giving a safe upper bound.
    fn new(
        posting_list: &'a PostingList,
        idf: f64,
        min_doc_len: f64,
        avg_doc_len: f64,
        config: &Bm25Config,
    ) -> Self {
        let len = posting_list.block_meta.len();

        // Per-block upper bounds: use block's max_tf with global min_doc_len
        let block_upper_bounds: Vec<f64> = posting_list
            .block_meta
            .iter()
            .map(|bm| compute_term_score(bm.max_tf as f64, idf, min_doc_len, avg_doc_len, config))
            .collect();

        // Suffix max (right to left)
        let mut suffix_upper_bounds = block_upper_bounds;
        for i in (0..len.saturating_sub(1)).rev() {
            let next = suffix_upper_bounds[i + 1];
            if next > suffix_upper_bounds[i] {
                suffix_upper_bounds[i] = next;
            }
        }

        Self {
            posting_list,
            idf,
            suffix_upper_bounds,
            current_block: 0,
            current_pos: 0,
        }
    }

    /// Current doc_id, or None if exhausted.
    fn current_doc_id(&self) -> Option<u32> {
        self.posting_list
            .postings
            .get(self.current_pos)
            .map(|p| p.doc_id)
    }

    /// Current posting, or None if exhausted.
    fn current_posting(&self) -> Option<&Posting> {
        self.posting_list.postings.get(self.current_pos)
    }

    /// Score upper bound for any posting at or after current position.
    /// Returns 0.0 if exhausted.
    fn upper_bound(&self) -> f64 {
        self.suffix_upper_bounds
            .get(self.current_block)
            .copied()
            .unwrap_or(0.0)
    }

    /// Advance cursor to the first posting with doc_id >= target.
    /// Skips entire blocks where max_doc_id < target, then linear scans
    /// within the target block (≤128 entries, L1-friendly).
    fn advance_to(&mut self, target: u32) {
        let block_meta = &self.posting_list.block_meta;

        // Skip blocks where max_doc_id < target
        while self.current_block < block_meta.len()
            && block_meta[self.current_block].max_doc_id < target
        {
            self.current_pos = block_meta[self.current_block].end_offset as usize;
            self.current_block += 1;
        }

        // Linear scan within the current block to find doc_id >= target
        let postings = &self.posting_list.postings;
        while self.current_pos < postings.len() && postings[self.current_pos].doc_id < target {
            self.current_pos += 1;
        }

        // Update current_block if we've passed its end
        while self.current_block < block_meta.len()
            && self.current_pos >= block_meta[self.current_block].end_offset as usize
        {
            self.current_block += 1;
        }
    }

    /// Advance cursor past target: to the first posting with doc_id > target.
    fn advance_past(&mut self, target: u32) {
        let block_meta = &self.posting_list.block_meta;

        // Skip blocks where max_doc_id <= target (all postings in block are <= target)
        while self.current_block < block_meta.len()
            && block_meta[self.current_block].max_doc_id <= target
        {
            self.current_pos = block_meta[self.current_block].end_offset as usize;
            self.current_block += 1;
        }

        // Linear scan within the current block to find doc_id > target
        let postings = &self.posting_list.postings;
        while self.current_pos < postings.len() && postings[self.current_pos].doc_id <= target {
            self.current_pos += 1;
        }

        // Update current_block if we've passed its end
        while self.current_block < block_meta.len()
            && self.current_pos >= block_meta[self.current_block].end_offset as usize
        {
            self.current_block += 1;
        }
    }
}

// ============================================================================
// BM25 Scorer
// ============================================================================

/// BM25 scorer for computing document relevance scores.
///
/// Iterates posting lists for query terms, accumulating scores in a dense Vec.
/// Tombstoned documents (lazy-deleted) are skipped during scoring.
pub struct Bm25Scorer<'a> {
    index: &'a Bm25Index,
    /// Precomputed IDF values for query terms (term_idx -> idf)
    /// Deduplicated: each term appears at most once.
    query_idfs: Vec<(u32, f64)>,
}

impl<'a> Bm25Scorer<'a> {
    /// Create a new scorer for the given query terms.
    ///
    /// `query_terms` should be the analyzed (tokenized, filtered, stemmed) query terms.
    /// Duplicate terms are automatically deduplicated (matching `distinct` behavior).
    pub fn new(index: &'a Bm25Index, query_terms: &[&str]) -> Self {
        // Deduplicate query terms using BTreeMap for deterministic order
        // (term_idx -> idf), where each term contributes only once
        let mut term_map: BTreeMap<u32, f64> = BTreeMap::new();

        for term in query_terms {
            if let Some(entry) = index.get_term(term) {
                // Only insert if not already present (first occurrence wins, though IDF is same)
                term_map
                    .entry(entry.idx)
                    .or_insert_with(|| compute_idf(index.stats.num_docs, entry.doc_freq));
            }
        }

        let query_idfs: Vec<(u32, f64)> = term_map.into_iter().collect();

        Self { index, query_idfs }
    }

    /// Score a single document against the query.
    ///
    /// Returns the BM25 score, or 0.0 if the document has no matching terms.
    ///
    /// When block metadata is populated (after `rebuild_lookups()`), uses
    /// block-aware search: `partition_point` on block `max_doc_id` to narrow
    /// to one block, then binary search within. Falls back to flat binary
    /// search when block metadata is empty (during index building).
    pub fn score(&self, doc_key: &DocKey) -> f64 {
        let Some(doc_id) = self.index.doc_id_for(doc_key) else {
            return 0.0;
        };

        let Some(Some(meta)) = self.index.doc_meta.get(doc_id as usize) else {
            return 0.0; // Tombstoned
        };

        let config = &self.index.config;
        let avg_dl = self.index.stats.avg_doc_len();
        let doc_len = meta.doc_len as f64;

        let mut score = 0.0;

        for &(term_idx, idf) in &self.query_idfs {
            if let Some(posting_list) = self.index.get_posting_list(term_idx) {
                let tf = if posting_list.block_meta.is_empty() {
                    // Fallback: no block metadata (during building)
                    posting_list
                        .postings
                        .binary_search_by_key(&doc_id, |p| p.doc_id)
                        .ok()
                        .map(|pos| posting_list.postings[pos].term_freq as f64)
                } else {
                    // Block-aware: partition_point to find block, binary search within
                    posting_list.block_containing(doc_id).and_then(|bi| {
                        let block = posting_list.block_postings(bi);
                        block
                            .binary_search_by_key(&doc_id, |p| p.doc_id)
                            .ok()
                            .map(|pos| block[pos].term_freq as f64)
                    })
                };

                if let Some(tf) = tf {
                    score += compute_term_score(tf, idf, doc_len, avg_dl, config);
                }
            }
        }

        score
    }

    /// Score all documents in the index, returning results sorted by score (descending).
    ///
    /// Only returns documents with score > 0. Uses a dense Vec accumulator
    /// indexed by doc_id for efficient scoring. Tombstoned docs are skipped.
    /// Ties are broken by DocKey ascending for deterministic output.
    pub fn score_all(&self) -> Vec<(DocKey, f64)> {
        let config = &self.index.config;
        let avg_dl = self.index.stats.avg_doc_len();
        let next_doc_id = self.index.next_doc_id() as usize;

        // Dense score accumulator — O(1) per posting
        let mut scores = vec![0.0f64; next_doc_id];

        for &(term_idx, idf) in &self.query_idfs {
            if let Some(posting_list) = self.index.get_posting_list(term_idx) {
                for posting in &posting_list.postings {
                    let doc_id = posting.doc_id as usize;
                    if doc_id >= next_doc_id {
                        continue;
                    }
                    // Check if doc is live (not tombstoned)
                    if let Some(Some(meta)) = self.index.doc_meta.get(doc_id) {
                        let tf = posting.term_freq as f64;
                        let doc_len = meta.doc_len as f64;
                        scores[doc_id] += compute_term_score(tf, idf, doc_len, avg_dl, config);
                    }
                }
            }
        }

        // Collect non-zero scores with DocKeys
        let mut results: Vec<(DocKey, f64)> = scores
            .iter()
            .enumerate()
            .filter(|(_, &score)| score > 0.0)
            .filter_map(|(doc_id, &score)| {
                self.index
                    .doc_meta
                    .get(doc_id)
                    .and_then(|opt| opt.as_ref())
                    .map(|meta| (meta.doc_key.clone(), score))
            })
            .collect();

        // Sort by score descending, then DocKey ascending for deterministic tie-breaking
        results.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

        results
    }

    /// Score all documents and return top-k results.
    ///
    /// Uses Block-Max WAND when block metadata is available and k < corpus size,
    /// providing early termination that skips posting list segments whose upper-bound
    /// score cannot enter the current top-k. Falls back to `score_all() + truncate`
    /// when block metadata is absent (during index building) or when k >= corpus size.
    ///
    /// **Invariant**: returns identical results to `score_all().truncate(k)` — same
    /// documents, same scores (bit-exact), same ordering (score DESC, DocKey ASC).
    pub fn top_k(&self, k: usize) -> Vec<(DocKey, f64)> {
        if k == 0 || self.query_idfs.is_empty() {
            return Vec::new();
        }
        if self.index.stats.num_docs == 0 {
            return Vec::new();
        }
        // If k >= corpus size, score_all is optimal (no early termination benefit)
        if k as u64 >= self.index.stats.num_docs {
            let mut results = self.score_all();
            results.truncate(k);
            return results;
        }
        // WAND requires block metadata on all non-empty posting lists
        let has_blocks = self.query_idfs.iter().all(|&(term_idx, _)| {
            self.index
                .get_posting_list(term_idx)
                .map(|pl| pl.postings.is_empty() || !pl.block_meta.is_empty())
                .unwrap_or(true)
        });
        if has_blocks {
            self.top_k_wand(k)
        } else {
            let mut results = self.score_all();
            results.truncate(k);
            results
        }
    }

    /// Compute the minimum document length across all live documents.
    /// BM25 score is monotonically decreasing in doc_len (for k1 > 0, 0 <= b <= 1),
    /// so min_doc_len maximizes the per-term score upper bound.
    fn compute_min_doc_len(&self) -> u32 {
        self.index
            .doc_meta
            .iter()
            .filter_map(|opt| opt.as_ref())
            .map(|meta| meta.doc_len)
            .min()
            .unwrap_or(0)
    }

    /// Block-Max WAND top-k scoring.
    ///
    /// Uses per-block score upper bounds (from `BlockMeta::max_tf` + global `min_doc_len`)
    /// to skip posting list segments that cannot contribute to the current top-k.
    /// Suffix-max upper bounds ensure safe pivot selection even when cursors advance
    /// across multiple blocks.
    ///
    /// Accumulation order matches `score_all()` (iterates `query_idfs` order) for
    /// bit-exact floating-point results.
    fn top_k_wand(&self, k: usize) -> Vec<(DocKey, f64)> {
        debug_assert!(self.index.config.k1 > 0.0);
        debug_assert!((0.0..=1.0).contains(&self.index.config.b));

        let config = &self.index.config;
        let avg_dl = self.index.stats.avg_doc_len();
        let min_doc_len = self.compute_min_doc_len();

        // Build cursors in query_idfs order (never reordered — accumulation order matters)
        let mut cursors: Vec<TermCursor> = self
            .query_idfs
            .iter()
            .filter_map(|&(term_idx, idf)| {
                let pl = self.index.get_posting_list(term_idx)?;
                if pl.postings.is_empty() || pl.block_meta.is_empty() {
                    return None;
                }
                Some(TermCursor::new(pl, idf, min_doc_len as f64, avg_dl, config))
            })
            .collect();

        if cursors.is_empty() {
            return Vec::new();
        }

        // Index array for doc_id-sorted traversal (cursors stay in term order)
        let mut sorted_indices: Vec<usize> = (0..cursors.len()).collect();
        let mut heap = TopKHeap::new(k);

        loop {
            // Sort index array by current_doc_id (exhausted cursors sort to end)
            sorted_indices.sort_by(|&a, &b| {
                match (cursors[a].current_doc_id(), cursors[b].current_doc_id()) {
                    (None, None) => std::cmp::Ordering::Equal,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (Some(da), Some(db)) => da.cmp(&db),
                }
            });

            // Count active cursors
            let active = sorted_indices
                .iter()
                .take_while(|&&i| cursors[i].current_doc_id().is_some())
                .count();
            if active == 0 {
                break;
            }

            // Find pivot: cumulative suffix upper bounds >= threshold
            let threshold = heap.threshold();
            let mut cumulative = 0.0;
            let mut pivot_pos = None;
            for pos in 0..active {
                cumulative += cursors[sorted_indices[pos]].upper_bound();
                if cumulative >= threshold {
                    pivot_pos = Some(pos);
                    break;
                }
            }
            let Some(p) = pivot_pos else { break };

            let pivot_doc_id = cursors[sorted_indices[p]].current_doc_id().unwrap();

            // Check if the lowest cursor is already at pivot_doc_id
            if cursors[sorted_indices[0]].current_doc_id().unwrap() == pivot_doc_id {
                // Lowest cursor at pivot — evaluate candidate (full score)
                if let Some(Some(meta)) = self.index.doc_meta.get(pivot_doc_id as usize) {
                    let doc_len = meta.doc_len as f64;
                    let mut score = 0.0;
                    // Iterate in query_idfs order for bit-exact accumulation
                    for cursor in &cursors {
                        if cursor.current_doc_id() == Some(pivot_doc_id) {
                            let tf = cursor.current_posting().unwrap().term_freq as f64;
                            score += compute_term_score(tf, cursor.idf, doc_len, avg_dl, config);
                        }
                    }
                    if score > 0.0 {
                        heap.push(score, meta.doc_key.clone());
                    }
                }
                // Advance all cursors at pivot_doc_id past it
                for cursor in &mut cursors {
                    if cursor.current_doc_id() == Some(pivot_doc_id) {
                        cursor.advance_past(pivot_doc_id);
                    }
                }
            } else {
                // Advance the furthest-behind cursor toward pivot
                let lagging = sorted_indices[0];
                cursors[lagging].advance_to(pivot_doc_id);
            }
        }

        heap.into_sorted_vec()
    }
}

/// Compute IDF (Inverse Document Frequency) for a term.
///
/// Uses the formula: log(1 + (N - n + 0.5) / (n + 0.5))
/// where N is total number of documents and n is document frequency of the term.
///
/// This matches the legacy implementation.
#[inline]
pub fn compute_idf(total_docs: u64, doc_freq: u32) -> f64 {
    let n = total_docs as f64;
    let df = doc_freq as f64;

    // IDF formula: log(1 + (N - n + 0.5) / (n + 0.5))
    ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
}

/// Compute the BM25 score contribution for a single term.
///
/// Uses the formula: IDF * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (doc_len / avg_doc_len)))
#[inline]
pub fn compute_term_score(
    tf: f64,
    idf: f64,
    doc_len: f64,
    avg_doc_len: f64,
    config: &Bm25Config,
) -> f64 {
    let k1 = config.k1;
    let b = config.b;

    // Normalize document length
    let len_norm = if avg_doc_len > 0.0 {
        doc_len / avg_doc_len
    } else {
        1.0
    };

    // BM25 score contribution for this term
    let numerator = tf * (k1 + 1.0);
    let denominator = tf + k1 * (1.0 - b + b * len_norm);

    idf * numerator / denominator
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn build_test_index() -> Bm25Index {
        let mut index = Bm25Index::new();

        // Document 1: "the quick brown fox"
        let doc1 = DocKey::new("test:main", "http://example.org/doc1");
        let mut tf1 = HashMap::new();
        tf1.insert("the", 1);
        tf1.insert("quick", 1);
        tf1.insert("brown", 1);
        tf1.insert("fox", 1);
        index.add_document(doc1, tf1);

        // Document 2: "the lazy brown dog"
        let doc2 = DocKey::new("test:main", "http://example.org/doc2");
        let mut tf2 = HashMap::new();
        tf2.insert("the", 1);
        tf2.insert("lazy", 1);
        tf2.insert("brown", 1);
        tf2.insert("dog", 1);
        index.add_document(doc2, tf2);

        // Document 3: "the quick fox jumps" (more foxes!)
        let doc3 = DocKey::new("test:main", "http://example.org/doc3");
        let mut tf3 = HashMap::new();
        tf3.insert("the", 1);
        tf3.insert("quick", 1);
        tf3.insert("fox", 2); // Higher frequency
        tf3.insert("jumps", 1);
        index.add_document(doc3, tf3);

        index
    }

    #[test]
    fn test_idf_calculation() {
        // Term appearing in 1 of 10 documents
        let idf = compute_idf(10, 1);
        assert!(idf > 0.0);

        // Term appearing in all documents has lower IDF
        let idf_common = compute_idf(10, 10);
        assert!(idf_common < idf);

        // Term appearing in half the documents
        let idf_half = compute_idf(10, 5);
        assert!(idf_half > idf_common);
        assert!(idf_half < idf);
    }

    #[test]
    fn test_scorer_basic() {
        let index = build_test_index();
        let scorer = Bm25Scorer::new(&index, &["fox"]);

        // Doc1 has "fox" once
        let doc1 = DocKey::new("test:main", "http://example.org/doc1");
        let score1 = scorer.score(&doc1);

        // Doc3 has "fox" twice
        let doc3 = DocKey::new("test:main", "http://example.org/doc3");
        let score3 = scorer.score(&doc3);

        // Doc2 has no "fox"
        let doc2 = DocKey::new("test:main", "http://example.org/doc2");
        let score2 = scorer.score(&doc2);

        assert!(score1 > 0.0);
        assert!(score3 > score1); // Higher TF should give higher score
        assert_eq!(score2, 0.0);
    }

    #[test]
    fn test_scorer_multi_term() {
        let index = build_test_index();
        let scorer = Bm25Scorer::new(&index, &["quick", "fox"]);

        let doc1 = DocKey::new("test:main", "http://example.org/doc1");
        let doc2 = DocKey::new("test:main", "http://example.org/doc2");
        let doc3 = DocKey::new("test:main", "http://example.org/doc3");

        let score1 = scorer.score(&doc1);
        let score2 = scorer.score(&doc2);
        let score3 = scorer.score(&doc3);

        // Doc1 and Doc3 have both terms, Doc2 has neither
        assert!(score1 > 0.0);
        assert!(score3 > 0.0);
        assert_eq!(score2, 0.0);

        // Doc3 should score higher (has "fox" twice)
        assert!(score3 > score1);
    }

    #[test]
    fn test_top_k() {
        let index = build_test_index();
        let scorer = Bm25Scorer::new(&index, &["fox"]);

        let results = scorer.top_k(2);

        assert_eq!(results.len(), 2);
        // Results should be sorted by score descending
        assert!(results[0].1 >= results[1].1);
        // Doc3 (with "fox" twice) should be first
        assert_eq!(results[0].0.subject_iri.as_ref(), "http://example.org/doc3");
    }

    #[test]
    fn test_score_all() {
        let index = build_test_index();
        let scorer = Bm25Scorer::new(&index, &["the"]);

        let results = scorer.score_all();

        // All 3 documents have "the"
        assert_eq!(results.len(), 3);
        // All scores should be positive
        assert!(results.iter().all(|(_, s)| *s > 0.0));
    }

    #[test]
    fn test_unknown_query_terms() {
        let index = build_test_index();
        let scorer = Bm25Scorer::new(&index, &["nonexistent", "terms"]);

        let results = scorer.score_all();
        assert!(results.is_empty());
    }

    #[test]
    fn test_duplicate_query_terms_deduplicated() {
        let index = build_test_index();

        // Create scorer with duplicate terms
        let scorer_with_dupes = Bm25Scorer::new(&index, &["fox", "fox", "fox"]);
        // Create scorer with single term
        let scorer_without_dupes = Bm25Scorer::new(&index, &["fox"]);

        let doc1 = DocKey::new("test:main", "http://example.org/doc1");

        // Scores should be identical - duplicates are deduplicated
        let score_with_dupes = scorer_with_dupes.score(&doc1);
        let score_without_dupes = scorer_without_dupes.score(&doc1);

        assert!(
            (score_with_dupes - score_without_dupes).abs() < 1e-10,
            "Duplicate query terms should be deduplicated: {score_with_dupes} vs {score_without_dupes}"
        );
    }

    #[test]
    fn test_bm25_config() {
        let mut index = Bm25Index::with_config(Bm25Config::new(2.0, 0.5));

        let doc1 = DocKey::new("test:main", "http://example.org/doc1");
        let mut tf1 = HashMap::new();
        tf1.insert("test", 1);
        index.add_document(doc1.clone(), tf1);

        let scorer = Bm25Scorer::new(&index, &["test"]);
        let score = scorer.score(&doc1);

        // With different k1/b, score should still be positive
        assert!(score > 0.0);
    }

    #[test]
    fn test_scorer_skips_tombstoned_docs() {
        let mut index = Bm25Index::new();

        let doc1 = DocKey::new("test:main", "http://example.org/doc1");
        let mut tf1 = HashMap::new();
        tf1.insert("hello", 1);
        index.add_document(doc1.clone(), tf1);

        let doc2 = DocKey::new("test:main", "http://example.org/doc2");
        let mut tf2 = HashMap::new();
        tf2.insert("hello", 2);
        index.add_document(doc2.clone(), tf2);

        // Remove doc1 (lazy — posting stays)
        index.remove_document(&doc1);

        let scorer = Bm25Scorer::new(&index, &["hello"]);
        let results = scorer.score_all();

        // Only doc2 should appear
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, doc2);
    }

    // ========================================================================
    // WAND tests
    // ========================================================================

    /// Helper: build an index with N documents and call rebuild_lookups() so
    /// block metadata is populated and WAND engages.
    ///
    /// Documents contain a mix of terms with varying frequencies to exercise
    /// multi-term scoring. Terms: "alpha", "beta", "gamma", "delta", "common".
    fn build_wand_test_index(n: usize) -> Bm25Index {
        let mut index = Bm25Index::new();

        // Pre-generate unique terms so we can borrow &str without leaking
        let unique_terms: Vec<String> = (0..n).map(|i| format!("unique{i}")).collect();

        for (i, unique_term) in unique_terms.iter().enumerate() {
            let doc_key = DocKey::new("test:main", format!("http://example.org/doc{i}"));
            let mut tf = HashMap::new();

            // "common" appears in every doc
            tf.insert("common", 1);

            // Distribute terms based on doc index for variety
            if i % 2 == 0 {
                tf.insert("alpha", ((i % 5) + 1) as u32);
            }
            if i % 3 == 0 {
                tf.insert("beta", ((i % 7) + 1) as u32);
            }
            if i % 5 == 0 {
                tf.insert("gamma", ((i % 3) + 1) as u32);
            }
            if i % 7 == 0 {
                tf.insert("delta", ((i % 4) + 1) as u32);
            }

            // Add a unique term per doc to vary doc_len
            tf.insert(unique_term.as_str(), 1);

            index.add_document(doc_key, tf);
        }

        // Populate block metadata so WAND engages
        index.rebuild_lookups();
        index
    }

    /// Compare WAND top_k results against score_all + truncate with bit-exact f64 equality.
    fn assert_wand_matches_score_all(index: &Bm25Index, query_terms: &[&str], k: usize) {
        let scorer = Bm25Scorer::new(index, query_terms);

        // Reference: score_all + truncate
        let mut expected = scorer.score_all();
        expected.truncate(k);

        // WAND path (top_k dispatches to WAND when blocks are available)
        let actual = scorer.top_k(k);

        assert_eq!(
            expected.len(),
            actual.len(),
            "Length mismatch for k={k}, query={query_terms:?}: expected {}, got {}",
            expected.len(),
            actual.len()
        );

        for (i, ((exp_key, exp_score), (act_key, act_score))) in
            expected.iter().zip(actual.iter()).enumerate()
        {
            assert_eq!(
                exp_key, act_key,
                "DocKey mismatch at position {i} for k={k}, query={query_terms:?}: \
                 expected {exp_key:?}, got {act_key:?}"
            );
            assert_eq!(
                exp_score.to_bits(),
                act_score.to_bits(),
                "Score mismatch at position {i} for k={k}, query={query_terms:?}: \
                 expected {exp_score} (bits={}), got {act_score} (bits={})",
                exp_score.to_bits(),
                act_score.to_bits()
            );
        }
    }

    #[test]
    fn test_wand_matches_score_all() {
        let index = build_wand_test_index(500);

        // Single-term queries
        for k in [1, 2, 5, 10, 50, 100, 200, 499] {
            assert_wand_matches_score_all(&index, &["alpha"], k);
            assert_wand_matches_score_all(&index, &["common"], k);
        }

        // Multi-term queries
        for k in [1, 2, 5, 10, 50, 100, 200, 499] {
            assert_wand_matches_score_all(&index, &["alpha", "beta"], k);
            assert_wand_matches_score_all(&index, &["alpha", "beta", "gamma"], k);
            assert_wand_matches_score_all(&index, &["alpha", "beta", "gamma", "delta"], k);
        }
    }

    #[test]
    fn test_wand_accumulation_order() {
        // Multi-term query with overlapping postings — bit-exact equality catches
        // accumulation order bugs (different f64 addition order → different rounding).
        let index = build_wand_test_index(300);

        for k in [1, 5, 10, 50] {
            assert_wand_matches_score_all(
                &index,
                &["alpha", "beta", "gamma", "delta", "common"],
                k,
            );
        }
    }

    #[test]
    fn test_wand_tie_breaking() {
        // Many docs with identical scores (single common term, same tf=1, same doc_len).
        // DocKey ordering decides which docs make the cut.
        let mut index = Bm25Index::new();
        for i in 0..200 {
            let doc_key = DocKey::new("test:main", format!("http://example.org/tie{i:04}"));
            let mut tf = HashMap::new();
            tf.insert("shared", 1);
            index.add_document(doc_key, tf);
        }
        index.rebuild_lookups();

        // k cuts in the middle of the tie group
        for k in [1, 10, 50, 100, 199] {
            assert_wand_matches_score_all(&index, &["shared"], k);
        }
    }

    #[test]
    fn test_wand_tombstoned_docs() {
        let mut index = Bm25Index::new();
        for i in 0..50 {
            let doc_key = DocKey::new("test:main", format!("http://example.org/doc{i}"));
            let mut tf = HashMap::new();
            tf.insert("hello", (i % 5 + 1) as u32);
            tf.insert("world", 1);
            index.add_document(doc_key, tf);
        }

        // Remove half the docs
        for i in (0..50).step_by(2) {
            let doc_key = DocKey::new("test:main", format!("http://example.org/doc{i}"));
            index.remove_document(&doc_key);
        }

        index.rebuild_lookups();

        for k in [1, 5, 10, 24] {
            assert_wand_matches_score_all(&index, &["hello"], k);
            assert_wand_matches_score_all(&index, &["hello", "world"], k);
        }
    }

    #[test]
    fn test_wand_edge_cases() {
        let index = build_wand_test_index(10);
        let scorer = Bm25Scorer::new(&index, &["alpha"]);

        // k=0 → empty
        assert!(scorer.top_k(0).is_empty());

        // Unknown terms → empty
        let scorer_unknown = Bm25Scorer::new(&index, &["nonexistent"]);
        assert!(scorer_unknown.top_k(5).is_empty());

        // k >= corpus size → falls back to score_all (same result)
        assert_wand_matches_score_all(&index, &["alpha"], 10);
        assert_wand_matches_score_all(&index, &["alpha"], 100);

        // Empty query → empty
        let scorer_empty = Bm25Scorer::new(&index, &[]);
        assert!(scorer_empty.top_k(5).is_empty());
    }

    #[test]
    fn test_wand_single_term() {
        let index = build_wand_test_index(100);

        // Degenerate single-term case — every cursor operation is on one posting list
        for k in [1, 5, 10, 50, 99] {
            assert_wand_matches_score_all(&index, &["alpha"], k);
            assert_wand_matches_score_all(&index, &["beta"], k);
            assert_wand_matches_score_all(&index, &["gamma"], k);
            assert_wand_matches_score_all(&index, &["delta"], k);
        }
    }

    #[test]
    fn test_wand_fallback_no_blocks() {
        // Without rebuild_lookups(), block_meta is empty → top_k falls back to score_all+truncate
        let mut index = Bm25Index::new();
        for i in 0..20 {
            let doc_key = DocKey::new("test:main", format!("http://example.org/doc{i}"));
            let mut tf = HashMap::new();
            tf.insert("term", (i + 1) as u32);
            index.add_document(doc_key, tf);
        }
        // No rebuild_lookups() → no block_meta

        let scorer = Bm25Scorer::new(&index, &["term"]);
        let top5 = scorer.top_k(5);
        let mut all = scorer.score_all();
        all.truncate(5);

        assert_eq!(top5.len(), all.len());
        for ((tk, ts), (ak, ascore)) in top5.iter().zip(all.iter()) {
            assert_eq!(tk, ak);
            assert_eq!(ts.to_bits(), ascore.to_bits());
        }
    }

    #[test]
    fn test_wand_heap_replaces_on_dockey_tie() {
        // Heap full with worst entry (score=S, docKey="zzz..."), then encounter a doc
        // with score == S and smaller DocKey — confirm it replaces the worst.
        let mut index = Bm25Index::new();

        // All docs get the same term with the same tf → identical scores
        // DocKeys are lexicographically ordered: aaa, bbb, ..., zzz
        let labels = ["aaa", "bbb", "ccc", "ddd", "eee", "fff", "zzz"];
        for label in &labels {
            let doc_key = DocKey::new("test:main", format!("http://example.org/{label}"));
            let mut tf = HashMap::new();
            tf.insert("equal", 1);
            index.add_document(doc_key, tf);
        }
        index.rebuild_lookups();

        // k=3: tie-breaking by DocKey ASC means aaa, bbb, ccc should win
        let scorer = Bm25Scorer::new(&index, &["equal"]);
        let results = scorer.top_k(3);

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0.subject_iri.as_ref(), "http://example.org/aaa");
        assert_eq!(results[1].0.subject_iri.as_ref(), "http://example.org/bbb");
        assert_eq!(results[2].0.subject_iri.as_ref(), "http://example.org/ccc");

        // Verify scores are identical (bit-exact)
        assert_eq!(results[0].1.to_bits(), results[1].1.to_bits());
        assert_eq!(results[1].1.to_bits(), results[2].1.to_bits());
    }
}
