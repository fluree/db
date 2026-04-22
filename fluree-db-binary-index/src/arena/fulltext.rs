//! Per-predicate fulltext Bag-of-Words arena for BM25 scoring.
//!
//! ## Document model
//!
//! The "document" unit for BM25 is a single asserted `(s, p, o)` triple at
//! head. A string shared by N subjects contributes N documents, tracked via
//! `triple_count` on each `DocBoW` entry.
//!
//! ## On-disk format (FTA1)
//!
//! ```text
//! Header (28 bytes):
//!   magic:      [u8; 4]   = "FTA1"
//!   version:    u8        = 1
//!   pad:        [u8; 3]   = 0
//!   term_count: u32 LE
//!   doc_count:  u32 LE    (number of DocBoW entries, not N)
//!   n:          u32 LE    (total triples = Σ triple_count)
//!   sum_dl:     u64 LE    (Σ triple_count × doc_len for avgdl)
//!
//! Term dictionary:
//!   for each term: term_len(u16 LE), term_bytes([u8; term_len])
//!   Terms are sorted lexicographically.
//!
//! Document BoW section:
//!   for each doc: string_id(u32 LE), triple_count(u32 LE), doc_len(u32 LE),
//!                 bow_count(u16 LE),
//!                 [(term_id(u32 LE), tf(u16 LE)); bow_count]
//!   Sorted by string_id.
//!
//! DF section:
//!   [df(u32 LE); term_count]
//! ```
//!
//! One content-addressed FTA1 blob per `(g_id, p_id)`.

use std::collections::BTreeMap;
use std::io;

/// Magic bytes for the fulltext arena.
const FTA_MAGIC: [u8; 4] = *b"FTA1";

/// Wire format version.
const FTA_VERSION: u8 = 1;

/// Header size: magic(4) + version(1) + pad(3) + term_count(4) + doc_count(4) + n(4) + sum_dl(8) = 28.
const FTA_HEADER_LEN: usize = 28;

/// BM25 parameters (Okapi BM25, standard defaults).
const K1: f64 = 1.2;
const B: f64 = 0.75;

/// Bag-of-Words for a single string dictionary entry.
#[derive(Debug, Clone)]
pub struct DocBoW {
    /// Number of asserted triples using this string_id at head.
    pub triple_count: u32,
    /// Document length: sum of all TF counts (total tokens after analysis).
    pub doc_len: u32,
    /// Term frequencies, sorted by term_id.
    pub terms: Vec<(u32, u16)>,
}

/// Corpus-level statistics for BM25 scoring.
#[derive(Debug, Clone)]
pub struct CorpusStats {
    /// Total triple count: Σ triple_count across all docs.
    pub n: u32,
    /// Sum of (triple_count × doc_len) for avgdl computation.
    pub sum_dl: u64,
    /// Document frequency per term_id: Σ triple_count for strings containing the term.
    pub df: Vec<u32>,
}

/// In-memory fulltext arena for one `(g_id, p_id)` pair.
#[derive(Debug, Clone)]
pub struct FulltextArena {
    /// Term dictionary: term_id → term string (sorted lexicographically).
    terms: Vec<String>,
    /// Per-string BoW: string_id → DocBoW.
    docs: BTreeMap<u32, DocBoW>,
    /// Corpus-level BM25 statistics.
    stats: CorpusStats,
}

impl FulltextArena {
    /// Create a new empty arena.
    pub fn new() -> Self {
        Self {
            terms: Vec::new(),
            docs: BTreeMap::new(),
            stats: CorpusStats {
                n: 0,
                sum_dl: 0,
                df: Vec::new(),
            },
        }
    }

    /// Look up or insert a term, returning its term_id.
    ///
    /// Terms must be inserted in sorted order during the build phase.
    /// After building, use `term_id()` for read-only lookups.
    pub fn get_or_insert_term(&mut self, term: &str) -> u32 {
        match self.terms.binary_search_by(|t| t.as_str().cmp(term)) {
            Ok(idx) => idx as u32,
            Err(idx) => {
                self.terms.insert(idx, term.to_string());
                idx as u32
            }
        }
    }

    /// Insert or increment a string's BoW on assertion.
    ///
    /// `bow` is the analyzed term frequencies: `(term_id, tf)` pairs,
    /// sorted by term_id. Call `get_or_insert_term()` first for each term.
    pub fn inc_string(&mut self, string_id: u32, bow: &[(u32, u16)]) {
        let doc_len: u32 = bow.iter().map(|(_, tf)| *tf as u32).sum();
        self.docs
            .entry(string_id)
            .and_modify(|d| {
                d.triple_count += 1;
            })
            .or_insert_with(|| DocBoW {
                triple_count: 1,
                doc_len,
                terms: bow.to_vec(),
            });
    }

    /// Decrement a string's triple count on retraction.
    ///
    /// Removes the entry entirely if triple_count reaches 0.
    pub fn dec_string(&mut self, string_id: u32) {
        if let Some(doc) = self.docs.get_mut(&string_id) {
            doc.triple_count = doc.triple_count.saturating_sub(1);
            if doc.triple_count == 0 {
                self.docs.remove(&string_id);
            }
        }
    }

    /// Recompute corpus-level stats from current docs.
    ///
    /// Must be called after all inc/dec operations are complete
    /// (i.e., after processing all entries from the commit chain).
    pub fn finalize_stats(&mut self) {
        let mut n: u32 = 0;
        let mut sum_dl: u64 = 0;
        let mut df = vec![0u32; self.terms.len()];

        for doc in self.docs.values() {
            n += doc.triple_count;
            sum_dl += doc.triple_count as u64 * doc.doc_len as u64;
            for &(term_id, _) in &doc.terms {
                if (term_id as usize) < df.len() {
                    df[term_id as usize] += doc.triple_count;
                }
            }
        }

        self.stats = CorpusStats { n, sum_dl, df };
    }

    /// Look up a term_id by string (binary search).
    pub fn term_id(&self, term: &str) -> Option<u32> {
        self.terms
            .binary_search_by(|t| t.as_str().cmp(term))
            .ok()
            .map(|i| i as u32)
    }

    /// Get the BoW for a given string_id.
    pub fn doc_bow(&self, string_id: u32) -> Option<&DocBoW> {
        self.docs.get(&string_id)
    }

    /// Corpus stats.
    pub fn stats(&self) -> &CorpusStats {
        &self.stats
    }

    /// Term dictionary.
    pub fn terms(&self) -> &[String] {
        &self.terms
    }

    /// Number of document entries (unique string_ids with triple_count > 0).
    pub fn doc_count(&self) -> usize {
        self.docs.len()
    }

    /// Whether the arena has any documents.
    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }

    /// Iterator over all (string_id, DocBoW) entries.
    pub fn docs(&self) -> impl Iterator<Item = (&u32, &DocBoW)> {
        self.docs.iter()
    }

    /// Insert a raw DocBoW entry directly (for incremental arena rebuilding).
    ///
    /// Overwrites any existing entry for the given string_id.
    /// Caller must ensure term_ids in `doc.terms` are valid for this arena's
    /// term dictionary.
    pub fn insert_doc_raw(&mut self, string_id: u32, doc: DocBoW) {
        self.docs.insert(string_id, doc);
    }

    /// Compute BM25 score for a string given query term IDs.
    ///
    /// Returns 0.0 if the string_id is not in the arena or has no matching terms.
    pub fn score_bm25(&self, string_id: u32, query_term_ids: &[u32]) -> f64 {
        let doc = match self.docs.get(&string_id) {
            Some(d) => d,
            None => return 0.0,
        };

        let n = self.stats.n as f64;
        if n == 0.0 {
            return 0.0;
        }
        let avgdl = self.stats.sum_dl as f64 / n;
        let dl = doc.doc_len as f64;

        let mut score = 0.0;
        for &qt_id in query_term_ids {
            // Look up term frequency in this document
            let tf = match doc.terms.binary_search_by_key(&qt_id, |(tid, _)| *tid) {
                Ok(idx) => doc.terms[idx].1 as f64,
                Err(_) => continue,
            };
            // Document frequency for this term
            let df = self.stats.df.get(qt_id as usize).copied().unwrap_or(0) as f64;
            // IDF: ln((N - df + 0.5) / (df + 0.5) + 1)
            let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
            // TF saturation
            let tf_norm = (tf * (K1 + 1.0)) / (tf + K1 * (1.0 - B + B * dl / avgdl));
            score += idf * tf_norm;
        }
        score
    }

    // ========================================================================
    // Serialization (FTA1 format)
    // ========================================================================

    /// Encode to the binary FTA1 wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(FTA_HEADER_LEN + self.terms.len() * 16);

        // Header
        buf.extend_from_slice(&FTA_MAGIC);
        buf.push(FTA_VERSION);
        buf.extend_from_slice(&[0u8; 3]); // pad
        buf.extend_from_slice(&(self.terms.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(self.docs.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.stats.n.to_le_bytes());
        buf.extend_from_slice(&self.stats.sum_dl.to_le_bytes());

        // Term dictionary (sorted)
        for term in &self.terms {
            let bytes = term.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(bytes);
        }

        // Document BoW (sorted by string_id via BTreeMap iteration)
        for (&string_id, doc) in &self.docs {
            buf.extend_from_slice(&string_id.to_le_bytes());
            buf.extend_from_slice(&doc.triple_count.to_le_bytes());
            buf.extend_from_slice(&doc.doc_len.to_le_bytes());
            buf.extend_from_slice(&(doc.terms.len() as u16).to_le_bytes());
            for &(term_id, tf) in &doc.terms {
                buf.extend_from_slice(&term_id.to_le_bytes());
                buf.extend_from_slice(&tf.to_le_bytes());
            }
        }

        // DF section
        for &df in &self.stats.df {
            buf.extend_from_slice(&df.to_le_bytes());
        }

        buf
    }

    /// Decode from the binary FTA1 wire format.
    pub fn decode(data: &[u8]) -> Result<Self, io::Error> {
        if data.len() < FTA_HEADER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "FTA1: data too short for header",
            ));
        }
        if data[0..4] != FTA_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("FTA1: bad magic {:?}", &data[0..4]),
            ));
        }
        let version = data[4];
        if version != FTA_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("FTA1: unsupported version {version}"),
            ));
        }

        let mut pos = 8; // skip magic(4) + version(1) + pad(3)
        let term_count = read_u32(data, &mut pos)? as usize;
        let doc_count = read_u32(data, &mut pos)? as usize;
        let n = read_u32(data, &mut pos)?;
        let sum_dl = read_u64(data, &mut pos)?;

        // Term dictionary
        let mut terms = Vec::with_capacity(term_count);
        for _ in 0..term_count {
            let len = read_u16(data, &mut pos)? as usize;
            if pos + len > data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "FTA1: term data truncated",
                ));
            }
            let s = std::str::from_utf8(&data[pos..pos + len])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            terms.push(s.to_string());
            pos += len;
        }

        // Document BoW
        let mut docs = BTreeMap::new();
        for _ in 0..doc_count {
            let string_id = read_u32(data, &mut pos)?;
            let triple_count = read_u32(data, &mut pos)?;
            let doc_len = read_u32(data, &mut pos)?;
            let bow_count = read_u16(data, &mut pos)? as usize;
            let mut bow_terms = Vec::with_capacity(bow_count);
            for _ in 0..bow_count {
                let term_id = read_u32(data, &mut pos)?;
                let tf = read_u16(data, &mut pos)?;
                bow_terms.push((term_id, tf));
            }
            docs.insert(
                string_id,
                DocBoW {
                    triple_count,
                    doc_len,
                    terms: bow_terms,
                },
            );
        }

        // DF section
        let mut df = Vec::with_capacity(term_count);
        for _ in 0..term_count {
            df.push(read_u32(data, &mut pos)?);
        }

        Ok(Self {
            terms,
            docs,
            stats: CorpusStats { n, sum_dl, df },
        })
    }
}

impl Default for FulltextArena {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Wire helpers
// ============================================================================

fn read_u16(data: &[u8], pos: &mut usize) -> Result<u16, io::Error> {
    if *pos + 2 > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "FTA1: unexpected EOF reading u16",
        ));
    }
    let v = u16::from_le_bytes([data[*pos], data[*pos + 1]]);
    *pos += 2;
    Ok(v)
}

fn read_u32(data: &[u8], pos: &mut usize) -> Result<u32, io::Error> {
    if *pos + 4 > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "FTA1: unexpected EOF reading u32",
        ));
    }
    let v = u32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
    *pos += 4;
    Ok(v)
}

fn read_u64(data: &[u8], pos: &mut usize) -> Result<u64, io::Error> {
    if *pos + 8 > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "FTA1: unexpected EOF reading u64",
        ));
    }
    let v = u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_arena() {
        let arena = FulltextArena::new();
        assert!(arena.is_empty());
        assert_eq!(arena.doc_count(), 0);
        assert_eq!(arena.score_bm25(42, &[0]), 0.0);
    }

    #[test]
    fn test_build_and_score() {
        let mut arena = FulltextArena::new();

        // Insert terms in sorted order to avoid term_id shifting.
        let hello = arena.get_or_insert_term("hello");
        let rust = arena.get_or_insert_term("rust");
        let world = arena.get_or_insert_term("world");

        // Doc 1: "hello world" (string_id=10, 2 triples)
        arena.inc_string(10, &[(hello, 1), (world, 1)]);
        arena.inc_string(10, &[(hello, 1), (world, 1)]); // second triple

        // Doc 2: "hello rust" (string_id=20, 1 triple)
        arena.inc_string(20, &[(hello, 1), (rust, 1)]);

        arena.finalize_stats();

        // N = 3 (2 + 1)
        assert_eq!(arena.stats().n, 3);
        assert_eq!(arena.doc_count(), 2);

        // Score for "hello" query on doc 10
        let score10 = arena.score_bm25(10, &[hello]);
        // Score for "hello" query on doc 20
        let score20 = arena.score_bm25(20, &[hello]);
        // Both docs contain "hello" with same TF, so scores should be similar
        assert!(score10 > 0.0);
        assert!(score20 > 0.0);

        // Score for "rust" query — doc 20 has it, doc 10 doesn't
        let score10_rust = arena.score_bm25(10, &[rust]);
        let score20_rust = arena.score_bm25(20, &[rust]);
        assert_eq!(score10_rust, 0.0);
        assert!(score20_rust > 0.0);
    }

    #[test]
    fn test_dec_string() {
        let mut arena = FulltextArena::new();
        let hello = arena.get_or_insert_term("hello");

        arena.inc_string(10, &[(hello, 1)]);
        arena.inc_string(10, &[(hello, 1)]);
        assert_eq!(arena.doc_bow(10).unwrap().triple_count, 2);

        arena.dec_string(10);
        assert_eq!(arena.doc_bow(10).unwrap().triple_count, 1);

        arena.dec_string(10);
        assert!(arena.doc_bow(10).is_none());
        assert!(arena.is_empty());
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let mut arena = FulltextArena::new();
        let apple = arena.get_or_insert_term("apple");
        let banana = arena.get_or_insert_term("banana");
        let cherry = arena.get_or_insert_term("cherry");

        arena.inc_string(5, &[(apple, 2), (banana, 1)]);
        arena.inc_string(10, &[(banana, 3), (cherry, 1)]);
        arena.inc_string(10, &[(banana, 3), (cherry, 1)]); // 2nd triple
        arena.finalize_stats();

        let bytes = arena.encode();
        assert_eq!(&bytes[0..4], b"FTA1");

        let decoded = FulltextArena::decode(&bytes).unwrap();
        assert_eq!(decoded.terms().len(), 3);
        assert_eq!(decoded.doc_count(), 2);
        assert_eq!(decoded.stats().n, arena.stats().n);
        assert_eq!(decoded.stats().sum_dl, arena.stats().sum_dl);
        assert_eq!(decoded.stats().df, arena.stats().df);

        // Verify docs match
        let d5 = decoded.doc_bow(5).unwrap();
        assert_eq!(d5.triple_count, 1);
        assert_eq!(d5.doc_len, 3); // apple(2) + banana(1)
        assert_eq!(d5.terms, vec![(apple, 2), (banana, 1)]);

        let d10 = decoded.doc_bow(10).unwrap();
        assert_eq!(d10.triple_count, 2);

        // BM25 scores should be identical
        let orig_score = arena.score_bm25(5, &[apple, banana]);
        let decoded_score = decoded.score_bm25(5, &[apple, banana]);
        assert!((orig_score - decoded_score).abs() < 1e-10);
    }

    #[test]
    fn test_term_ordering() {
        let mut arena = FulltextArena::new();
        // Insert out of order — terms should be stored in sorted order.
        // Note: get_or_insert_term returns position-at-insertion-time, which
        // may shift as more terms are inserted. Use term_id() for final lookup.
        arena.get_or_insert_term("zebra");
        arena.get_or_insert_term("alpha");
        arena.get_or_insert_term("mango");

        assert_eq!(arena.terms(), &["alpha", "mango", "zebra"]);
        assert_eq!(arena.term_id("alpha"), Some(0));
        assert_eq!(arena.term_id("mango"), Some(1));
        assert_eq!(arena.term_id("zebra"), Some(2));
        assert_eq!(arena.term_id("banana"), None);
    }

    #[test]
    fn test_decode_bad_magic() {
        let bad = b"BAD1rest_of_data_here_padding__";
        let result = FulltextArena::decode(bad);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("bad magic"));
    }

    #[test]
    fn test_decode_too_short() {
        let result = FulltextArena::decode(&[0u8; 10]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too short"));
    }
}
