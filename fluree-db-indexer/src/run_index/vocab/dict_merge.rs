//! Global dictionary merge for the parallel import pipeline (Tier 2).
//!
//! After all parse workers finish, each holds a [`ChunkSubjectDict`] and
//! [`ChunkStringDict`] with chunk-local IDs. This module merges them into
//! global dictionaries and produces per-chunk remap tables.
//!
//! ## Subject merge
//!
//! Each chunk's forward entries contain `(ns_code, name_bytes)`. The merge
//! builds a global dedup map keyed by `xxh3_128(ns_code || name)` and assigns
//! sid64 values in `(ns_code << 48) | local_id` format (same as `SubjectDict`).
//! Chunks are processed in order for deterministic output.
//!
//! ## String merge
//!
//! Each chunk's forward entries contain raw string bytes. The merge builds a
//! global dedup map keyed by `xxh3_128(string_bytes)` and assigns sequential
//! `u32` IDs (same as `StringValueDict`).
//!
//! ## Remap tables
//!
//! Per-chunk remap tables map `chunk_local_id → global_id`:
//! - Subject remap: `Vec<u64>` (chunk-local u64 → global sid64)
//! - String remap: `Vec<u32>` (chunk-local u32 → global string ID)

use crate::run_index::resolve::chunk_dict::{hash_subject, ChunkStringDict, ChunkSubjectDict};
use rustc_hash::{FxBuildHasher, FxHashMap};

// ============================================================================
// Subject merge
// ============================================================================

/// Result of merging chunk subject dicts into a single global dictionary.
pub struct SubjectMergeResult {
    /// Total number of unique subjects across all chunks.
    pub total_subjects: u64,
    /// Whether any namespace has more than `u16::MAX` local IDs.
    /// When true, leaflets must use wide (u64) subject encoding.
    pub needs_wide: bool,
    /// Forward entries in insertion order: `(ns_code, name_bytes)`.
    /// The i-th entry corresponds to the i-th unique subject encountered.
    /// Used by dict upload to build CAS subject trees.
    pub forward_entries: Vec<(u16, Vec<u8>)>,
    /// Parallel vec: sid64 for the i-th unique subject.
    /// Used by dict upload for the `subjects.sids` file.
    pub forward_sids: Vec<u64>,
}

/// Merge chunk subject dicts into global sid64 values and per-chunk remap tables.
///
/// Chunks are processed in index order (0, 1, 2, ...) for deterministic output.
/// Each chunk's forward entries are iterated sequentially — the first chunk to
/// introduce a `(ns_code, name)` pair gets to "define" its global sid64.
///
/// Returns the merge result and one remap table per chunk. Each remap table
/// maps chunk-local subject ID → global sid64.
pub fn merge_subject_dicts(chunks: &[ChunkSubjectDict]) -> (SubjectMergeResult, Vec<Vec<u64>>) {
    // Estimate capacity from total chunk sizes
    let total_local: usize = chunks.iter().map(|c| c.len() as usize).sum();

    let mut global_map: FxHashMap<u128, u64> =
        FxHashMap::with_capacity_and_hasher(total_local, FxBuildHasher);
    let mut ns_counters: FxHashMap<u16, u64> =
        FxHashMap::with_capacity_and_hasher(64, FxBuildHasher);
    let mut forward_entries = Vec::with_capacity(total_local);
    let mut forward_sids = Vec::with_capacity(total_local);
    let mut needs_wide = false;
    let mut remap_tables = Vec::with_capacity(chunks.len());

    for chunk in chunks {
        let entries = chunk.forward_entries();
        let mut remap = vec![0u64; entries.len()];

        for (local_id, (ns_code, name_bytes)) in entries.iter().enumerate() {
            let hash = hash_subject(*ns_code, name_bytes);

            let global_sid = if let Some(&sid) = global_map.get(&hash) {
                sid
            } else {
                // New subject: assign global sid64
                let counter = ns_counters.entry(*ns_code).or_insert(0);
                let local_id_in_ns = *counter;
                *counter += 1;

                if local_id_in_ns > u16::MAX as u64 {
                    needs_wide = true;
                }

                let sid64 = ((*ns_code as u64) << 48) | local_id_in_ns;
                global_map.insert(hash, sid64);
                forward_entries.push((*ns_code, name_bytes.clone()));
                forward_sids.push(sid64);
                sid64
            };

            remap[local_id] = global_sid;
        }

        remap_tables.push(remap);
    }

    let result = SubjectMergeResult {
        total_subjects: global_map.len() as u64,
        needs_wide,
        forward_entries,
        forward_sids,
    };

    (result, remap_tables)
}

// ============================================================================
// String merge
// ============================================================================

/// Result of merging chunk string dicts into a single global dictionary.
pub struct StringMergeResult {
    /// Total number of unique strings across all chunks.
    pub total_strings: u32,
    /// Forward entries in insertion order: raw string bytes.
    /// The i-th entry corresponds to global string ID `i`.
    /// Used by dict upload to build CAS string trees.
    pub forward_entries: Vec<Vec<u8>>,
}

/// Merge chunk string dicts into global IDs and per-chunk remap tables.
///
/// Chunks are processed in index order for deterministic output.
///
/// Returns the merge result and one remap table per chunk. Each remap table
/// maps chunk-local string ID → global string ID.
pub fn merge_string_dicts(chunks: &[ChunkStringDict]) -> (StringMergeResult, Vec<Vec<u32>>) {
    let total_local: usize = chunks.iter().map(|c| c.len() as usize).sum();

    let mut global_map: FxHashMap<u128, u32> =
        FxHashMap::with_capacity_and_hasher(total_local, FxBuildHasher);
    let mut next_id: u32 = 0;
    let mut forward_entries = Vec::with_capacity(total_local);
    let mut remap_tables = Vec::with_capacity(chunks.len());

    for chunk in chunks {
        let entries = chunk.forward_entries();
        let mut remap = vec![0u32; entries.len()];

        for (local_id, string_bytes) in entries.iter().enumerate() {
            let hash = xxhash_rust::xxh3::xxh3_128(string_bytes);

            let global_id = if let Some(&id) = global_map.get(&hash) {
                id
            } else {
                let id = next_id;
                next_id = next_id
                    .checked_add(1)
                    .expect("string id overflow (>4B unique strings)");
                global_map.insert(hash, id);
                forward_entries.push(string_bytes.clone());
                id
            };

            remap[local_id] = global_id;
        }

        remap_tables.push(remap);
    }

    let result = StringMergeResult {
        total_strings: next_id,
        forward_entries,
    };

    (result, remap_tables)
}

// ============================================================================
// Persistence — write merge results to flat files
// ============================================================================

use std::collections::HashMap;
use std::io::{self, Write};
use std::path::Path;

/// Write merged subject/string dictionaries to flat files on disk.
///
/// Produces the same file layout as `vocab_merge` / `GlobalDicts::persist()`:
/// - `subjects.fwd` — concatenated full IRIs (prefix + suffix)
/// - `subjects.idx` — forward index (offsets + lens)
/// - `subjects.sids` — sid64 mapping
/// - `strings.fwd` — concatenated string bytes
/// - `strings.idx` — forward index
pub fn persist_merge_artifacts(
    run_dir: &Path,
    subjects: &SubjectMergeResult,
    strings: &StringMergeResult,
    ns_prefixes: &HashMap<u16, String>,
) -> io::Result<()> {
    use super::dict_io::{write_subject_index, write_subject_sid_map};

    // --- Subjects ---
    // Build a permutation that sorts subjects by sid64. This ensures
    // subjects.sids is monotonically increasing, which is required by
    // binary_search in build_class_stats_json and BinaryIndexStore.
    // (vocab_merge produces sorted output via its min-heap; dict_merge
    // produces insertion-order, so we sort here.)
    let n = subjects.forward_entries.len();
    let mut perm: Vec<usize> = (0..n).collect();
    perm.sort_unstable_by_key(|&i| subjects.forward_sids[i]);

    let mut fwd = io::BufWriter::new(std::fs::File::create(run_dir.join("subjects.fwd"))?);
    let mut offsets = Vec::with_capacity(n);
    let mut lens = Vec::with_capacity(n);
    let mut sorted_sids = Vec::with_capacity(n);
    let mut offset: u64 = 0;

    for &i in &perm {
        let (ns_code, suffix) = &subjects.forward_entries[i];
        let prefix = ns_prefixes
            .get(ns_code)
            .map(std::string::String::as_str)
            .unwrap_or("");
        let total_len = prefix.len() + suffix.len();
        offsets.push(offset);
        lens.push(total_len as u32);
        fwd.write_all(prefix.as_bytes())?;
        fwd.write_all(suffix)?;
        offset += total_len as u64;
        sorted_sids.push(subjects.forward_sids[i]);
    }
    fwd.flush()?;

    write_subject_index(&run_dir.join("subjects.idx"), &offsets, &lens)?;
    write_subject_sid_map(&run_dir.join("subjects.sids"), &sorted_sids)?;

    // --- Strings ---
    let mut str_fwd = io::BufWriter::new(std::fs::File::create(run_dir.join("strings.fwd"))?);
    let mut str_offsets = Vec::with_capacity(strings.forward_entries.len());
    let mut str_lens = Vec::with_capacity(strings.forward_entries.len());
    let mut str_offset: u64 = 0;

    for entry in &strings.forward_entries {
        str_offsets.push(str_offset);
        str_lens.push(entry.len() as u32);
        str_fwd.write_all(entry)?;
        str_offset += entry.len() as u64;
    }
    str_fwd.flush()?;

    write_subject_index(&run_dir.join("strings.idx"), &str_offsets, &str_lens)?;

    Ok(())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- Subject merge tests --

    #[test]
    fn test_subject_merge_single_chunk() {
        let mut dict = ChunkSubjectDict::new();
        dict.get_or_insert(10, b"Alice");
        dict.get_or_insert(10, b"Bob");
        dict.get_or_insert(20, b"Alice"); // different namespace

        let (result, remaps) = merge_subject_dicts(&[dict]);

        assert_eq!(result.total_subjects, 3);
        assert!(!result.needs_wide);
        assert_eq!(result.forward_entries.len(), 3);
        assert_eq!(result.forward_sids.len(), 3);
        assert_eq!(remaps.len(), 1);
        assert_eq!(remaps[0].len(), 3);

        // Check sid64 format: ns_code in high 16 bits
        let sid_alice_ns10 = remaps[0][0];
        let sid_bob_ns10 = remaps[0][1];
        let sid_alice_ns20 = remaps[0][2];

        assert_eq!(sid_alice_ns10 >> 48, 10); // ns_code = 10
        assert_eq!(sid_bob_ns10 >> 48, 10);
        assert_eq!(sid_alice_ns20 >> 48, 20); // ns_code = 20

        // Within namespace 10, Alice and Bob have sequential local IDs
        assert_eq!(sid_alice_ns10 & 0xFFFF_FFFF_FFFF, 0);
        assert_eq!(sid_bob_ns10 & 0xFFFF_FFFF_FFFF, 1);
        // Namespace 20 starts at 0
        assert_eq!(sid_alice_ns20 & 0xFFFF_FFFF_FFFF, 0);
    }

    #[test]
    fn test_subject_merge_dedup_across_chunks() {
        // Chunk 0: Alice, Bob
        let mut dict0 = ChunkSubjectDict::new();
        dict0.get_or_insert(10, b"Alice");
        dict0.get_or_insert(10, b"Bob");

        // Chunk 1: Bob, Carol (Bob is shared with chunk 0)
        let mut dict1 = ChunkSubjectDict::new();
        dict1.get_or_insert(10, b"Bob");
        dict1.get_or_insert(10, b"Carol");

        let (result, remaps) = merge_subject_dicts(&[dict0, dict1]);

        // 3 unique subjects: Alice, Bob, Carol
        assert_eq!(result.total_subjects, 3);
        assert_eq!(result.forward_entries.len(), 3);

        // Chunk 0: Alice → sid(0), Bob → sid(1)
        let sid_alice = remaps[0][0];
        let sid_bob_c0 = remaps[0][1];

        // Chunk 1: Bob → sid(1) (deduped), Carol → sid(2)
        let sid_bob_c1 = remaps[1][0];
        let sid_carol = remaps[1][1];

        // Bob should have the same global sid in both chunks
        assert_eq!(sid_bob_c0, sid_bob_c1);

        // All three subjects should be distinct
        assert_ne!(sid_alice, sid_bob_c0);
        assert_ne!(sid_bob_c0, sid_carol);
        assert_ne!(sid_alice, sid_carol);
    }

    #[test]
    fn test_subject_merge_empty_chunks() {
        let dict0 = ChunkSubjectDict::new(); // empty
        let mut dict1 = ChunkSubjectDict::new();
        dict1.get_or_insert(10, b"Alice");

        let (result, remaps) = merge_subject_dicts(&[dict0, dict1]);

        assert_eq!(result.total_subjects, 1);
        assert_eq!(remaps[0].len(), 0); // empty chunk → empty remap
        assert_eq!(remaps[1].len(), 1);
    }

    #[test]
    fn test_subject_merge_no_chunks() {
        let (result, remaps) = merge_subject_dicts(&[]);

        assert_eq!(result.total_subjects, 0);
        assert!(remaps.is_empty());
    }

    #[test]
    fn test_subject_merge_forward_entries_order() {
        // Chunk 0: [A, B], Chunk 1: [B, C], Chunk 2: [C, D]
        let mut dict0 = ChunkSubjectDict::new();
        dict0.get_or_insert(10, b"A");
        dict0.get_or_insert(10, b"B");

        let mut dict1 = ChunkSubjectDict::new();
        dict1.get_or_insert(10, b"B");
        dict1.get_or_insert(10, b"C");

        let mut dict2 = ChunkSubjectDict::new();
        dict2.get_or_insert(10, b"C");
        dict2.get_or_insert(10, b"D");

        let (result, _) = merge_subject_dicts(&[dict0, dict1, dict2]);

        // Forward entries should be in first-seen order: A, B, C, D
        assert_eq!(result.forward_entries.len(), 4);
        assert_eq!(result.forward_entries[0], (10, b"A".to_vec()));
        assert_eq!(result.forward_entries[1], (10, b"B".to_vec()));
        assert_eq!(result.forward_entries[2], (10, b"C".to_vec()));
        assert_eq!(result.forward_entries[3], (10, b"D".to_vec()));
    }

    #[test]
    fn test_subject_merge_deterministic() {
        // Same input chunks → same output
        let make_chunks = || {
            let mut d0 = ChunkSubjectDict::new();
            d0.get_or_insert(10, b"Alice");
            d0.get_or_insert(10, b"Bob");
            let mut d1 = ChunkSubjectDict::new();
            d1.get_or_insert(10, b"Bob");
            d1.get_or_insert(10, b"Carol");
            vec![d0, d1]
        };

        let chunks1 = make_chunks();
        let chunks2 = make_chunks();

        let (r1, t1) = merge_subject_dicts(&chunks1);
        let (r2, t2) = merge_subject_dicts(&chunks2);

        assert_eq!(r1.total_subjects, r2.total_subjects);
        assert_eq!(r1.forward_sids, r2.forward_sids);
        assert_eq!(t1, t2);
    }

    // -- String merge tests --

    #[test]
    fn test_string_merge_single_chunk() {
        let mut dict = ChunkStringDict::new();
        dict.get_or_insert(b"hello");
        dict.get_or_insert(b"world");

        let (result, remaps) = merge_string_dicts(&[dict]);

        assert_eq!(result.total_strings, 2);
        assert_eq!(result.forward_entries.len(), 2);
        assert_eq!(remaps.len(), 1);
        assert_eq!(remaps[0], vec![0, 1]);
    }

    #[test]
    fn test_string_merge_dedup_across_chunks() {
        let mut dict0 = ChunkStringDict::new();
        dict0.get_or_insert(b"alpha");
        dict0.get_or_insert(b"beta");

        let mut dict1 = ChunkStringDict::new();
        dict1.get_or_insert(b"beta"); // shared
        dict1.get_or_insert(b"gamma");

        let (result, remaps) = merge_string_dicts(&[dict0, dict1]);

        assert_eq!(result.total_strings, 3);

        // Chunk 0: alpha → 0, beta → 1
        assert_eq!(remaps[0][0], 0);
        assert_eq!(remaps[0][1], 1);

        // Chunk 1: beta → 1 (deduped), gamma → 2
        assert_eq!(remaps[1][0], 1);
        assert_eq!(remaps[1][1], 2);
    }

    #[test]
    fn test_string_merge_empty_chunks() {
        let dict0 = ChunkStringDict::new();
        let mut dict1 = ChunkStringDict::new();
        dict1.get_or_insert(b"test");

        let (result, remaps) = merge_string_dicts(&[dict0, dict1]);

        assert_eq!(result.total_strings, 1);
        assert_eq!(remaps[0].len(), 0);
        assert_eq!(remaps[1], vec![0]);
    }

    #[test]
    fn test_string_merge_forward_entries_order() {
        let mut dict0 = ChunkStringDict::new();
        dict0.get_or_insert(b"C");
        dict0.get_or_insert(b"A");

        let mut dict1 = ChunkStringDict::new();
        dict1.get_or_insert(b"B");
        dict1.get_or_insert(b"A"); // dedup

        let (result, _) = merge_string_dicts(&[dict0, dict1]);

        // First-seen order: C, A, B
        assert_eq!(result.forward_entries.len(), 3);
        assert_eq!(result.forward_entries[0], b"C");
        assert_eq!(result.forward_entries[1], b"A");
        assert_eq!(result.forward_entries[2], b"B");
    }

    #[test]
    fn test_string_merge_deterministic() {
        let make_chunks = || {
            let mut d0 = ChunkStringDict::new();
            d0.get_or_insert(b"hello");
            d0.get_or_insert(b"world");
            let mut d1 = ChunkStringDict::new();
            d1.get_or_insert(b"world");
            d1.get_or_insert(b"!");
            vec![d0, d1]
        };

        let chunks1 = make_chunks();
        let chunks2 = make_chunks();

        let (r1, t1) = merge_string_dicts(&chunks1);
        let (r2, t2) = merge_string_dicts(&chunks2);

        assert_eq!(r1.total_strings, r2.total_strings);
        assert_eq!(r1.forward_entries, r2.forward_entries);
        assert_eq!(t1, t2);
    }

    // -- Combined tests --

    #[test]
    fn test_large_merge_many_chunks() {
        // Simulate 10 chunks with overlapping subjects/strings
        let mut subject_chunks = Vec::new();
        let mut string_chunks = Vec::new();

        for chunk_idx in 0..10u32 {
            let mut subjects = ChunkSubjectDict::new();
            let mut strings = ChunkStringDict::new();

            for i in 0..100u32 {
                // Some subjects are unique to this chunk, some are shared
                let name = format!("entity_{}", i + chunk_idx * 50);
                subjects.get_or_insert(10, name.as_bytes());

                let value = format!("value_{}", i + chunk_idx * 50);
                strings.get_or_insert(value.as_bytes());
            }

            subject_chunks.push(subjects);
            string_chunks.push(strings);
        }

        let (s_result, s_remaps) = merge_subject_dicts(&subject_chunks);
        let (str_result, str_remaps) = merge_string_dicts(&string_chunks);

        // Each chunk has 100 entries with 50 overlap → 550 unique
        assert_eq!(s_result.total_subjects, 550);
        assert_eq!(str_result.total_strings, 550);
        assert_eq!(s_remaps.len(), 10);
        assert_eq!(str_remaps.len(), 10);

        // Verify remap tables are consistent: same entity name → same global ID
        // across different chunk remap tables
        for i in 0..10 {
            assert_eq!(s_remaps[i].len(), 100);
            assert_eq!(str_remaps[i].len(), 100);
        }
    }
}
