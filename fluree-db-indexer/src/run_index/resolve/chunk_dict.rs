//! Per-chunk local dictionaries for the parallel import pipeline (Tier 2).
//!
//! During parallel import, each parse worker assigns **chunk-local** sequential
//! IDs to subjects and string values. These local IDs are written to spool
//! files. After all chunks are parsed, a merge phase builds global dictionaries
//! and remap tables that translate chunk-local IDs to globally unique IDs.
//!
//! ## ChunkSubjectDict
//!
//! Maps `(ns_code, name_bytes)` → chunk-local `u64` subject ID.
//! Hash key: `xxh3_128(ns_code.to_le_bytes() || name_bytes)`.
//! Forward entries store `(ns_code, name_bytes)` for the merge phase.
//!
//! ## ChunkStringDict
//!
//! Maps string bytes → chunk-local `u32` string ID.
//! Hash key: `xxh3_128(string_bytes)`.
//! Forward entries store raw string bytes for the merge phase.

use std::io;
use std::path::Path;

use rustc_hash::{FxBuildHasher, FxHashMap};
use xxhash_rust::xxh3::Xxh3;

use crate::run_index::vocab::vocab_file::{StringVocabWriter, SubjectVocabWriter};

// ============================================================================
// ChunkSubjectDict
// ============================================================================

/// Per-chunk subject dictionary with chunk-local sequential IDs.
///
/// IDs are simple sequential `u64` values (0, 1, 2, ...), NOT the
/// `(ns_code << 48) | local_id` format used by `SubjectDict`. The
/// sid64 encoding happens during the merge phase.
///
/// Forward entries store `(ns_code, name_bytes)` — just the local name
/// within the namespace, NOT the full IRI. Full IRI reconstruction:
/// `namespace_codes[ns_code] + str::from_utf8(name_bytes)`.
pub struct ChunkSubjectDict {
    /// Reverse: xxh3_128(ns_code || name_bytes) → chunk-local ID.
    reverse: FxHashMap<u128, u64>,
    /// Forward: indexed by chunk-local ID. Stores (ns_code, name_bytes).
    forward: Vec<(u16, Vec<u8>)>,
    /// Next chunk-local ID to assign.
    next_id: u64,
}

impl ChunkSubjectDict {
    /// Create an empty chunk subject dictionary.
    pub fn new() -> Self {
        Self {
            reverse: FxHashMap::default(),
            forward: Vec::new(),
            next_id: 0,
        }
    }

    /// Create with a pre-allocated capacity hint (number of expected unique subjects).
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            reverse: FxHashMap::with_capacity_and_hasher(capacity, FxBuildHasher),
            forward: Vec::with_capacity(capacity),
            next_id: 0,
        }
    }

    /// Look up or insert a subject by namespace code and local name.
    ///
    /// Returns a chunk-local sequential ID (0, 1, 2, ...).
    /// The hash is computed internally from `(ns_code, name)`.
    pub fn get_or_insert(&mut self, ns_code: u16, name: &[u8]) -> u64 {
        let hash = hash_subject(ns_code, name);
        self.get_or_insert_with_hash(hash, ns_code, name)
    }

    /// Look up or insert using a pre-computed xxh3_128 hash.
    ///
    /// `hash` must be `xxh3_128(ns_code.to_le_bytes() || name_bytes)`.
    pub fn get_or_insert_with_hash(&mut self, hash: u128, ns_code: u16, name: &[u8]) -> u64 {
        if let Some(&id) = self.reverse.get(&hash) {
            return id;
        }

        let id = self.next_id;
        self.next_id += 1;
        self.reverse.insert(hash, id);
        self.forward.push((ns_code, name.to_vec()));
        id
    }

    /// Number of unique subjects in this chunk.
    pub fn len(&self) -> u64 {
        self.next_id
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.next_id == 0
    }

    /// Forward entries: indexed by chunk-local ID → `(ns_code, name_bytes)`.
    ///
    /// Used by the merge phase to reconstruct full IRIs and assign global IDs.
    pub fn forward_entries(&self) -> &[(u16, Vec<u8>)] {
        &self.forward
    }

    /// Consume the dict and return the forward entries.
    pub fn into_forward_entries(self) -> Vec<(u16, Vec<u8>)> {
        self.forward
    }

    /// Sort the forward entries by `(ns_code ASC, suffix_bytes ASC)` and write
    /// them to a sorted vocab file at `path`.
    ///
    /// Each entry is written with its original chunk-local ID (its position in
    /// the unsorted `forward` vec) so the merge phase can build remap tables.
    ///
    /// Consumes `self` — the dict's reverse map and forward vec are freed after
    /// writing. Returns the number of entries written.
    pub fn write_sorted_vocab(self, path: &Path) -> io::Result<u64> {
        let forward = self.forward;
        let n = forward.len();

        // Build sorted indices.
        let mut indices: Vec<usize> = (0..n).collect();
        indices.sort_unstable_by(|&a, &b| {
            let (ns_a, ref suf_a) = forward[a];
            let (ns_b, ref suf_b) = forward[b];
            ns_a.cmp(&ns_b).then_with(|| suf_a.cmp(suf_b))
        });

        let mut writer = SubjectVocabWriter::new(path)?;
        for &idx in &indices {
            let (ns_code, ref suffix) = forward[idx];
            writer.write_entry(ns_code, idx as u64, suffix)?;
        }
        writer.finish()
    }

    /// Sort entries by canonical subject order `(ns_code ASC, suffix ASC)`,
    /// write a sorted vocab file with **sorted-position IDs** (0, 1, 2, ...),
    /// and return the insertion→sorted remap table.
    ///
    /// The remap table has length `self.len()`. `remap[insertion_id] = sorted_position`.
    /// This remap is used to convert chunk-local insertion-order IDs in buffered
    /// records to sorted-order IDs before writing sorted commit files.
    ///
    /// The vocab file entries use sorted-position IDs as `local_id` so the
    /// downstream k-way merge produces remap tables mapping
    /// `sorted_local_id → global_id`.
    ///
    /// Consumes `self`. Returns `(remap, entry_count)`.
    pub fn sort_and_write_sorted_vocab(self, path: &Path) -> io::Result<(Vec<u64>, u64)> {
        let forward = self.forward;
        let n = forward.len();

        // Build sorted indices.
        let mut sorted_indices: Vec<usize> = (0..n).collect();
        sorted_indices.sort_unstable_by(|&a, &b| {
            let (ns_a, ref suf_a) = forward[a];
            let (ns_b, ref suf_b) = forward[b];
            ns_a.cmp(&ns_b).then_with(|| suf_a.cmp(suf_b))
        });

        // Build insertion→sorted remap: remap[insertion_id] = sorted_position.
        let mut remap = vec![0u64; n];
        for (sorted_pos, &orig_idx) in sorted_indices.iter().enumerate() {
            remap[orig_idx] = sorted_pos as u64;
        }

        // Write vocab with sorted-position IDs.
        let mut writer = SubjectVocabWriter::new(path)?;
        for (sorted_pos, &orig_idx) in sorted_indices.iter().enumerate() {
            let (ns_code, ref suffix) = forward[orig_idx];
            writer.write_entry(ns_code, sorted_pos as u64, suffix)?;
        }
        let count = writer.finish()?;

        Ok((remap, count))
    }
}

impl Default for ChunkSubjectDict {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// ChunkStringDict
// ============================================================================

/// Per-chunk string value dictionary with chunk-local sequential IDs.
///
/// IDs are simple sequential `u32` values (0, 1, 2, ...).
/// Forward entries store raw string bytes for the merge phase.
pub struct ChunkStringDict {
    /// Reverse: xxh3_128(string_bytes) → chunk-local ID.
    reverse: FxHashMap<u128, u32>,
    /// Forward: indexed by chunk-local ID. Stores raw string bytes.
    forward: Vec<Vec<u8>>,
    /// Next chunk-local ID to assign.
    next_id: u32,
}

impl ChunkStringDict {
    /// Create an empty chunk string dictionary.
    pub fn new() -> Self {
        Self {
            reverse: FxHashMap::default(),
            forward: Vec::new(),
            next_id: 0,
        }
    }

    /// Create with a pre-allocated capacity hint.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            reverse: FxHashMap::with_capacity_and_hasher(capacity, FxBuildHasher),
            forward: Vec::with_capacity(capacity),
            next_id: 0,
        }
    }

    /// Look up or insert a string, returning its chunk-local ID.
    ///
    /// Hash is computed internally from the string bytes.
    pub fn get_or_insert(&mut self, s: &[u8]) -> u32 {
        let hash = xxhash_rust::xxh3::xxh3_128(s);
        self.get_or_insert_with_hash(hash, s)
    }

    /// Look up or insert using a pre-computed xxh3_128 hash.
    ///
    /// `hash` must be `xxh3_128(string_bytes)`.
    pub fn get_or_insert_with_hash(&mut self, hash: u128, s: &[u8]) -> u32 {
        if let Some(&id) = self.reverse.get(&hash) {
            return id;
        }

        let id = self.next_id;
        self.next_id += 1;
        self.reverse.insert(hash, id);
        self.forward.push(s.to_vec());
        id
    }

    /// Number of unique strings in this chunk.
    pub fn len(&self) -> u32 {
        self.next_id
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.next_id == 0
    }

    /// Forward entries: indexed by chunk-local ID → raw string bytes.
    ///
    /// Used by the merge phase to build the global string dictionary.
    pub fn forward_entries(&self) -> &[Vec<u8>] {
        &self.forward
    }

    /// Consume the dict and return the forward entries.
    pub fn into_forward_entries(self) -> Vec<Vec<u8>> {
        self.forward
    }

    /// Sort the forward entries by string bytes (lexicographic ASC) and write
    /// them to a sorted vocab file at `path`.
    ///
    /// Each entry is written with its original chunk-local ID (its position in
    /// the unsorted `forward` vec) so the merge phase can build remap tables.
    ///
    /// Consumes `self`. Returns the number of entries written.
    pub fn write_sorted_vocab(self, path: &Path) -> io::Result<u64> {
        let forward = self.forward;
        let n = forward.len();

        let mut indices: Vec<usize> = (0..n).collect();
        indices.sort_unstable_by(|&a, &b| forward[a].cmp(&forward[b]));

        let mut writer = StringVocabWriter::new(path)?;
        for &idx in &indices {
            writer.write_entry(idx as u32, &forward[idx])?;
        }
        writer.finish()
    }

    /// Sort entries by UTF-8 byte-lex order, write a sorted vocab file with
    /// **sorted-position IDs** (0, 1, 2, ...), and return the insertion→sorted
    /// remap table.
    ///
    /// The remap table has length `self.len()`. `remap[insertion_id] = sorted_position`.
    ///
    /// Consumes `self`. Returns `(remap, entry_count)`.
    pub fn sort_and_write_sorted_vocab(self, path: &Path) -> io::Result<(Vec<u32>, u64)> {
        let forward = self.forward;
        let n = forward.len();

        let mut sorted_indices: Vec<usize> = (0..n).collect();
        sorted_indices.sort_unstable_by(|&a, &b| forward[a].cmp(&forward[b]));

        // Build insertion→sorted remap.
        let mut remap = vec![0u32; n];
        for (sorted_pos, &orig_idx) in sorted_indices.iter().enumerate() {
            remap[orig_idx] = sorted_pos as u32;
        }

        // Write vocab with sorted-position IDs.
        let mut writer = StringVocabWriter::new(path)?;
        for (sorted_pos, &orig_idx) in sorted_indices.iter().enumerate() {
            writer.write_entry(sorted_pos as u32, &forward[orig_idx])?;
        }
        let count = writer.finish()?;

        Ok((remap, count))
    }
}

impl Default for ChunkStringDict {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Hash helpers
// ============================================================================

/// Compute the subject hash key from namespace code and local name.
///
/// Uses streaming xxh3_128 to avoid concatenation allocation:
/// `xxh3_128(ns_code.to_le_bytes() || name_bytes)`.
///
/// The ns_code is included in the hash so that two different namespaces
/// with the same local name don't collide (e.g., `ex:Alice` vs `foaf:Alice`).
#[inline]
pub fn hash_subject(ns_code: u16, name: &[u8]) -> u128 {
    let mut hasher = Xxh3::new();
    hasher.update(&ns_code.to_le_bytes());
    hasher.update(name);
    hasher.digest128()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- ChunkSubjectDict tests ----

    #[test]
    fn test_chunk_subject_dict_basic() {
        let mut dict = ChunkSubjectDict::new();

        let id0 = dict.get_or_insert(10, b"Alice");
        let id1 = dict.get_or_insert(10, b"Bob");
        let id0_again = dict.get_or_insert(10, b"Alice");

        assert_eq!(id0, 0);
        assert_eq!(id1, 1);
        assert_eq!(id0, id0_again);
        assert_eq!(dict.len(), 2);
    }

    #[test]
    fn test_chunk_subject_dict_cross_namespace() {
        let mut dict = ChunkSubjectDict::new();

        // Same local name, different namespace → different IDs
        let id_ns10 = dict.get_or_insert(10, b"Alice");
        let id_ns20 = dict.get_or_insert(20, b"Alice");

        assert_eq!(id_ns10, 0);
        assert_eq!(id_ns20, 1);
        assert_ne!(id_ns10, id_ns20);
        assert_eq!(dict.len(), 2);
    }

    #[test]
    fn test_chunk_subject_dict_forward_entries() {
        let mut dict = ChunkSubjectDict::new();
        dict.get_or_insert(10, b"Alice");
        dict.get_or_insert(20, b"Bob");

        let entries = dict.forward_entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (10, b"Alice".to_vec()));
        assert_eq!(entries[1], (20, b"Bob".to_vec()));
    }

    #[test]
    fn test_chunk_subject_dict_with_hash() {
        let mut dict = ChunkSubjectDict::new();

        let hash = hash_subject(10, b"Alice");
        let id0 = dict.get_or_insert_with_hash(hash, 10, b"Alice");
        let id0_again = dict.get_or_insert_with_hash(hash, 10, b"Alice");

        assert_eq!(id0, 0);
        assert_eq!(id0, id0_again);

        // Verify consistency with non-hash insert
        assert_eq!(dict.get_or_insert(10, b"Alice"), 0);
    }

    #[test]
    fn test_chunk_subject_dict_with_capacity() {
        let mut dict = ChunkSubjectDict::with_capacity(1000);
        assert!(dict.is_empty());

        dict.get_or_insert(10, b"Alice");
        assert_eq!(dict.len(), 1);
    }

    #[test]
    fn test_chunk_subject_dict_into_forward() {
        let mut dict = ChunkSubjectDict::new();
        dict.get_or_insert(10, b"Alice");
        dict.get_or_insert(20, b"Bob");

        let entries = dict.into_forward_entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (10, b"Alice".to_vec()));
        assert_eq!(entries[1], (20, b"Bob".to_vec()));
    }

    // ---- ChunkStringDict tests ----

    #[test]
    fn test_chunk_string_dict_basic() {
        let mut dict = ChunkStringDict::new();

        let id0 = dict.get_or_insert(b"hello");
        let id1 = dict.get_or_insert(b"world");
        let id0_again = dict.get_or_insert(b"hello");

        assert_eq!(id0, 0);
        assert_eq!(id1, 1);
        assert_eq!(id0, id0_again);
        assert_eq!(dict.len(), 2);
    }

    #[test]
    fn test_chunk_string_dict_forward_entries() {
        let mut dict = ChunkStringDict::new();
        dict.get_or_insert(b"alpha");
        dict.get_or_insert(b"beta");
        dict.get_or_insert(b"gamma");

        let entries = dict.forward_entries();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], b"alpha");
        assert_eq!(entries[1], b"beta");
        assert_eq!(entries[2], b"gamma");
    }

    #[test]
    fn test_chunk_string_dict_with_hash() {
        let mut dict = ChunkStringDict::new();

        let hash = xxhash_rust::xxh3::xxh3_128(b"hello");
        let id0 = dict.get_or_insert_with_hash(hash, b"hello");
        let id0_again = dict.get_or_insert_with_hash(hash, b"hello");

        assert_eq!(id0, 0);
        assert_eq!(id0, id0_again);
    }

    #[test]
    fn test_chunk_string_dict_with_capacity() {
        let mut dict = ChunkStringDict::with_capacity(500);
        assert!(dict.is_empty());

        dict.get_or_insert(b"test");
        assert_eq!(dict.len(), 1);
    }

    #[test]
    fn test_chunk_string_dict_into_forward() {
        let mut dict = ChunkStringDict::new();
        dict.get_or_insert(b"alpha");
        dict.get_or_insert(b"beta");

        let entries = dict.into_forward_entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], b"alpha");
        assert_eq!(entries[1], b"beta");
    }

    // ---- hash_subject tests ----

    #[test]
    fn test_hash_subject_deterministic() {
        let h1 = hash_subject(10, b"Alice");
        let h2 = hash_subject(10, b"Alice");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_hash_subject_ns_discriminates() {
        // Same name, different namespace → different hashes
        let h1 = hash_subject(10, b"Alice");
        let h2 = hash_subject(20, b"Alice");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_hash_subject_name_discriminates() {
        // Same namespace, different name → different hashes
        let h1 = hash_subject(10, b"Alice");
        let h2 = hash_subject(10, b"Bob");
        assert_ne!(h1, h2);
    }

    // ---- write_sorted_vocab tests ----

    fn temp_path(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join("fluree_chunk_dict_tests");
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    #[test]
    fn test_subject_write_sorted_vocab() {
        use crate::run_index::vocab::vocab_file::SubjectVocabReader;

        let mut dict = ChunkSubjectDict::new();
        // Insert out of sorted order: ns=20 before ns=5, and within ns=5 "Zara" before "Alice"
        dict.get_or_insert(20, b"Carol"); // local_id=0
        dict.get_or_insert(5, b"Zara"); // local_id=1
        dict.get_or_insert(5, b"Alice"); // local_id=2

        let path = temp_path("subj_sorted.voc");
        let count = dict.write_sorted_vocab(&path).unwrap();
        assert_eq!(count, 3);

        // Read back: should be sorted (5,"Alice"), (5,"Zara"), (20,"Carol")
        let mut r = SubjectVocabReader::open(&path).unwrap();

        let e0 = r.next_entry().unwrap().unwrap();
        assert_eq!((e0.ns_code, &e0.suffix[..]), (5, &b"Alice"[..]));
        assert_eq!(e0.local_id, 2); // original position

        let e1 = r.next_entry().unwrap().unwrap();
        assert_eq!((e1.ns_code, &e1.suffix[..]), (5, &b"Zara"[..]));
        assert_eq!(e1.local_id, 1);

        let e2 = r.next_entry().unwrap().unwrap();
        assert_eq!((e2.ns_code, &e2.suffix[..]), (20, &b"Carol"[..]));
        assert_eq!(e2.local_id, 0);

        assert!(r.next_entry().unwrap().is_none());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_string_write_sorted_vocab() {
        use crate::run_index::vocab::vocab_file::StringVocabReader;

        let mut dict = ChunkStringDict::new();
        // Insert out of sorted order
        dict.get_or_insert(b"gamma"); // local_id=0
        dict.get_or_insert(b"alpha"); // local_id=1
        dict.get_or_insert(b"beta"); // local_id=2

        let path = temp_path("str_sorted.voc");
        let count = dict.write_sorted_vocab(&path).unwrap();
        assert_eq!(count, 3);

        // Read back: should be sorted "alpha", "beta", "gamma"
        let mut r = StringVocabReader::open(&path).unwrap();

        let e0 = r.next_entry().unwrap().unwrap();
        assert_eq!(&e0.string_bytes[..], b"alpha");
        assert_eq!(e0.local_id, 1);

        let e1 = r.next_entry().unwrap().unwrap();
        assert_eq!(&e1.string_bytes[..], b"beta");
        assert_eq!(e1.local_id, 2);

        let e2 = r.next_entry().unwrap().unwrap();
        assert_eq!(&e2.string_bytes[..], b"gamma");
        assert_eq!(e2.local_id, 0);

        assert!(r.next_entry().unwrap().is_none());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_subject_write_sorted_vocab_empty() {
        let dict = ChunkSubjectDict::new();
        let path = temp_path("subj_sorted_empty.voc");
        let count = dict.write_sorted_vocab(&path).unwrap();
        assert_eq!(count, 0);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_string_write_sorted_vocab_empty() {
        let dict = ChunkStringDict::new();
        let path = temp_path("str_sorted_empty.voc");
        let count = dict.write_sorted_vocab(&path).unwrap();
        assert_eq!(count, 0);
        std::fs::remove_file(&path).ok();
    }
}
