//! K-way merge of sorted vocabulary files into global dictionaries + remap tables.
//!
//! Replaces the hash-map-based dictionary merge with a streaming external-sort
//! merge that bounds memory to O(K) where K = number of chunks.
//!
//! ## Algorithm
//!
//! 1. Open all sorted `.voc` files as readers (one per chunk)
//! 2. Pre-allocate per-chunk remap files via mmap (sized from vocab headers)
//! 3. Seed a min-heap with the first entry from each reader
//! 4. Pop minimum: if same key as previous → duplicate (reuse global ID),
//!    else → assign new global ID, write to forward dict
//! 5. Write the global ID into the remap mmap at `remap[chunk_id][local_id]`
//! 6. Advance the popped reader; push its next entry to the heap
//!
//! ## Output files
//!
//! Produces the same files as the old hash-map merge:
//! - `subjects.fwd` + `subjects.idx` + `subjects.sids` — forward subject dict
//! - `strings.fwd` + `strings.idx` — forward string dict
//! - `remap/subjects_NNNNN.rmp` — per-chunk subject remap (flat u64 LE array)
//! - `remap/strings_NNNNN.rmp` — per-chunk string remap (flat u32 LE array)

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::vocab_file::{StringVocabReader, SubjectVocabReader};

// ============================================================================
// Subject merge
// ============================================================================

/// Result of merging subject vocabularies.
#[derive(Debug)]
pub struct SubjectMergeStats {
    /// Number of globally unique subjects.
    pub total_unique: u64,
    /// Whether any namespace's local counter exceeded u16::MAX,
    /// requiring wide subject ID encoding in leaflets.
    pub needs_wide: bool,
}

/// Heap entry for subject k-way merge. Owns the sort key for `Ord`.
struct SubjectHeapEntry {
    ns_code: u16,
    suffix: Vec<u8>,
    local_id: u64,
    chunk_id: usize,
}

impl PartialEq for SubjectHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.ns_code == other.ns_code && self.suffix == other.suffix
    }
}

impl Eq for SubjectHeapEntry {}

impl PartialOrd for SubjectHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SubjectHeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ns_code
            .cmp(&other.ns_code)
            .then_with(|| self.suffix.cmp(&other.suffix))
    }
}

/// Merge all sorted subject vocab files into global forward dict + remap tables.
///
/// - `vocab_paths`: sorted `.voc` file paths, one per chunk
/// - `chunk_ids`: original chunk indices (used for remap file naming: `subjects_NNNNN.rmp`)
/// - `remap_dir`: directory for remap output files (`subjects_NNNNN.rmp`)
/// - `run_dir`: directory for forward dict output files
/// - `namespace_codes`: ns_code → IRI prefix for full IRI reconstruction
pub fn merge_subject_vocabs(
    vocab_paths: &[PathBuf],
    chunk_ids: &[usize],
    remap_dir: &Path,
    run_dir: &Path,
    namespace_codes: &HashMap<u16, String>,
) -> io::Result<SubjectMergeStats> {
    let k = vocab_paths.len();
    assert_eq!(
        chunk_ids.len(),
        k,
        "chunk_ids must match vocab_paths length"
    );

    // Open all readers + create mmap'd remap files.
    let mut readers: Vec<SubjectVocabReader> = Vec::with_capacity(k);
    let mut remaps: Vec<MmapRemapU64> = Vec::with_capacity(k);

    for (i, path) in vocab_paths.iter().enumerate() {
        let reader = SubjectVocabReader::open(path)?;
        let entry_count = reader.header().entry_count;

        let remap_path = remap_dir.join(format!("subjects_{:05}.rmp", chunk_ids[i]));
        let remap = MmapRemapU64::create(&remap_path, entry_count)?;

        readers.push(reader);
        remaps.push(remap);
    }

    // Open forward dict output streams.
    let mut fwd = BufWriter::new(std::fs::File::create(run_dir.join("subjects.fwd"))?);
    let mut idx = ForwardIndexStream::new(run_dir, "subjects")?;
    let sids_path = run_dir.join("subjects.sids");
    let mut sids = SubjectSidMapStream::new(&sids_path)?;

    // Seed the min-heap.
    let mut heap: BinaryHeap<Reverse<SubjectHeapEntry>> = BinaryHeap::with_capacity(k);
    for (chunk_id, reader) in readers.iter_mut().enumerate() {
        if let Some(entry) = reader.next_entry()? {
            heap.push(Reverse(SubjectHeapEntry {
                ns_code: entry.ns_code,
                suffix: entry.suffix,
                local_id: entry.local_id,
                chunk_id,
            }));
        }
    }

    // Track state for dedup + sid64 assignment.
    let mut ns_counters: HashMap<u16, u64> = HashMap::new();
    let mut needs_wide = false;
    let mut total_unique: u64 = 0;
    let mut prev_key: Option<(u16, Vec<u8>)> = None;
    let mut current_sid64: u64 = 0;

    while let Some(Reverse(entry)) = heap.pop() {
        // Check if this is a duplicate of the previous entry.
        let is_dup = prev_key
            .as_ref()
            .is_some_and(|(ns, suf)| *ns == entry.ns_code && *suf == entry.suffix);

        if is_dup {
            // Duplicate: reuse current_sid64.
        } else {
            // New unique entry: assign a new sid64.
            let counter = ns_counters.entry(entry.ns_code).or_insert(0);
            let local_id_in_ns = *counter;
            *counter += 1;
            if local_id_in_ns > u16::MAX as u64 {
                needs_wide = true;
            }
            current_sid64 = ((entry.ns_code as u64) << 48) | local_id_in_ns;
            total_unique += 1;

            // Write to forward dict.
            let prefix = namespace_codes
                .get(&entry.ns_code)
                .map(std::string::String::as_str)
                .unwrap_or("");
            let total_len = (prefix.len() + entry.suffix.len()) as u32;
            idx.push_len(total_len)?;
            fwd.write_all(prefix.as_bytes())?;
            fwd.write_all(&entry.suffix)?;
            sids.push(current_sid64)?;

            prev_key = Some((entry.ns_code, entry.suffix));
        }

        // Write to remap table.
        remaps[entry.chunk_id].set(entry.local_id, current_sid64)?;

        // Advance this chunk's reader.
        if let Some(next) = readers[entry.chunk_id].next_entry()? {
            heap.push(Reverse(SubjectHeapEntry {
                ns_code: next.ns_code,
                suffix: next.suffix,
                local_id: next.local_id,
                chunk_id: entry.chunk_id,
            }));
        }
    }

    // Finalize outputs.
    fwd.flush()?;
    idx.finish(&run_dir.join("subjects.idx"))?;
    sids.finish(&sids_path)?;

    // Drop mmaps (flushes to disk).
    drop(remaps);

    Ok(SubjectMergeStats {
        total_unique,
        needs_wide,
    })
}

// ============================================================================
// String merge
// ============================================================================

/// Result of merging string vocabularies.
#[derive(Debug)]
pub struct StringMergeStats {
    /// Number of globally unique strings.
    pub total_unique: u32,
}

/// Heap entry for string k-way merge.
struct StringHeapEntry {
    key: Vec<u8>,
    local_id: u32,
    chunk_id: usize,
}

impl PartialEq for StringHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for StringHeapEntry {}

impl PartialOrd for StringHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StringHeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

/// Merge all sorted string vocab files into global forward dict + remap tables.
///
/// - `chunk_ids`: original chunk indices (used for remap file naming: `strings_NNNNN.rmp`)
pub fn merge_string_vocabs(
    vocab_paths: &[PathBuf],
    chunk_ids: &[usize],
    remap_dir: &Path,
    run_dir: &Path,
) -> io::Result<StringMergeStats> {
    let k = vocab_paths.len();
    assert_eq!(
        chunk_ids.len(),
        k,
        "chunk_ids must match vocab_paths length"
    );

    let mut readers: Vec<StringVocabReader> = Vec::with_capacity(k);
    let mut remaps: Vec<MmapRemapU32> = Vec::with_capacity(k);

    for (i, path) in vocab_paths.iter().enumerate() {
        let reader = StringVocabReader::open(path)?;
        let entry_count = reader.header().entry_count;

        let remap_path = remap_dir.join(format!("strings_{:05}.rmp", chunk_ids[i]));
        let remap = MmapRemapU32::create(&remap_path, entry_count)?;

        readers.push(reader);
        remaps.push(remap);
    }

    let mut fwd = BufWriter::new(std::fs::File::create(run_dir.join("strings.fwd"))?);
    let mut idx = ForwardIndexStream::new(run_dir, "strings")?;

    let mut heap: BinaryHeap<Reverse<StringHeapEntry>> = BinaryHeap::with_capacity(k);
    for (chunk_id, reader) in readers.iter_mut().enumerate() {
        if let Some(entry) = reader.next_entry()? {
            heap.push(Reverse(StringHeapEntry {
                key: entry.string_bytes,
                local_id: entry.local_id,
                chunk_id,
            }));
        }
    }

    let mut next_global_id: u32 = 0;
    let mut prev_key: Option<Vec<u8>> = None;
    let mut current_global_id: u32 = 0;

    while let Some(Reverse(entry)) = heap.pop() {
        let is_dup = prev_key.as_ref().is_some_and(|k| *k == entry.key);

        if is_dup {
            // Reuse current_global_id.
        } else {
            current_global_id = next_global_id;
            next_global_id = next_global_id.checked_add(1).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "string id overflow (>4B unique strings)",
                )
            })?;

            idx.push_len(entry.key.len() as u32)?;
            fwd.write_all(&entry.key)?;

            prev_key = Some(entry.key);
        }

        remaps[entry.chunk_id].set(entry.local_id as u64, current_global_id as u64)?;

        if let Some(next) = readers[entry.chunk_id].next_entry()? {
            heap.push(Reverse(StringHeapEntry {
                key: next.string_bytes,
                local_id: next.local_id,
                chunk_id: entry.chunk_id,
            }));
        }
    }

    fwd.flush()?;
    idx.finish(&run_dir.join("strings.idx"))?;

    drop(remaps);

    Ok(StringMergeStats {
        total_unique: next_global_id,
    })
}

// ============================================================================
// Mmap remap helpers
// ============================================================================

/// Memory-mapped remap table for u64 entries (subjects).
///
/// Pre-allocated to `entry_count × 8` bytes. Entries are written at
/// `local_id × 8` offset via [`set`](MmapRemapU64::set).
struct MmapRemapU64 {
    mmap: memmap2::MmapMut,
    len: u64,
}

impl MmapRemapU64 {
    fn create(path: &Path, entry_count: u64) -> io::Result<Self> {
        let byte_len = entry_count.checked_mul(8).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "subject remap size overflow")
        })?;

        if byte_len == 0 {
            // Empty file: create zero-length file, use empty mmap.
            std::fs::File::create(path)?;
            return Ok(Self {
                mmap: memmap2::MmapMut::map_anon(0)?,
                len: 0,
            });
        }

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.set_len(byte_len)?;
        let mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };

        Ok(Self {
            mmap,
            len: entry_count,
        })
    }

    #[inline]
    fn set(&mut self, local_id: u64, value: u64) -> io::Result<()> {
        if local_id >= self.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "subject remap set out of bounds: local_id={}, len={}",
                    local_id, self.len
                ),
            ));
        }
        let offset = (local_id as usize) * 8;
        self.mmap[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        Ok(())
    }
}

/// Memory-mapped remap table for u32 entries (strings).
struct MmapRemapU32 {
    mmap: memmap2::MmapMut,
    len: u64,
}

impl MmapRemapU32 {
    fn create(path: &Path, entry_count: u64) -> io::Result<Self> {
        let byte_len = entry_count.checked_mul(4).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "string remap size overflow")
        })?;

        if byte_len == 0 {
            std::fs::File::create(path)?;
            return Ok(Self {
                mmap: memmap2::MmapMut::map_anon(0)?,
                len: 0,
            });
        }

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.set_len(byte_len)?;
        let mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };

        Ok(Self {
            mmap,
            len: entry_count,
        })
    }

    #[inline]
    fn set(&mut self, local_id: u64, value: u64) -> io::Result<()> {
        let value_u32: u32 = value.try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("string remap value overflow: {value}"),
            )
        })?;
        if local_id >= self.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "string remap set out of bounds: local_id={}, len={}",
                    local_id, self.len
                ),
            ));
        }
        let offset = (local_id as usize) * 4;
        self.mmap[offset..offset + 4].copy_from_slice(&value_u32.to_le_bytes());
        Ok(())
    }
}

// ============================================================================
// Forward index stream (same format as import.rs ForwardIndexStream)
// ============================================================================

/// Stream-build a forward index without holding all offsets/lens in RAM.
///
/// Produces the same `FSI1` format as `fluree-db-indexer/dict_io`:
/// `"FSI1" + count(u32) + offsets(u64[count]) + lens(u32[count])`.
struct ForwardIndexStream {
    offsets_path: PathBuf,
    lens_path: PathBuf,
    offsets: BufWriter<std::fs::File>,
    lens: BufWriter<std::fs::File>,
    count: u64,
    offset: u64,
}

impl ForwardIndexStream {
    fn new(dir: &Path, name: &str) -> io::Result<Self> {
        let offsets_path = dir.join(format!("{name}.offsets.tmp"));
        let lens_path = dir.join(format!("{name}.lens.tmp"));
        Ok(Self {
            offsets: BufWriter::new(std::fs::File::create(&offsets_path)?),
            lens: BufWriter::new(std::fs::File::create(&lens_path)?),
            offsets_path,
            lens_path,
            count: 0,
            offset: 0,
        })
    }

    #[inline]
    fn push_len(&mut self, len: u32) -> io::Result<()> {
        let off = self.offset;
        self.offsets.write_all(&off.to_le_bytes())?;
        self.lens.write_all(&len.to_le_bytes())?;
        self.offset += len as u64;
        self.count += 1;
        Ok(())
    }

    fn finish(mut self, idx_path: &Path) -> io::Result<()> {
        const INDEX_MAGIC: [u8; 4] = *b"FSI1";
        self.offsets.flush()?;
        self.lens.flush()?;

        let count_u32: u32 = self.count.try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "forward index count overflow")
        })?;

        let mut out = BufWriter::new(std::fs::File::create(idx_path)?);
        out.write_all(&INDEX_MAGIC)?;
        out.write_all(&count_u32.to_le_bytes())?;

        let mut off_in = std::io::BufReader::new(std::fs::File::open(&self.offsets_path)?);
        let mut len_in = std::io::BufReader::new(std::fs::File::open(&self.lens_path)?);
        std::io::copy(&mut off_in, &mut out)?;
        std::io::copy(&mut len_in, &mut out)?;
        out.flush()?;

        let _ = std::fs::remove_file(&self.offsets_path);
        let _ = std::fs::remove_file(&self.lens_path);
        Ok(())
    }
}

// ============================================================================
// Subject SID map stream (same format as import.rs SubjectSidMapStream)
// ============================================================================

/// Stream-writer for subjects.sids (`SSM1 + count(u64) + [sid64]*count`).
struct SubjectSidMapStream {
    file: BufWriter<std::fs::File>,
    count: u64,
}

impl SubjectSidMapStream {
    fn new(path: &Path) -> io::Result<Self> {
        const SID_MAP_MAGIC: [u8; 4] = *b"SSM1";
        let mut raw = std::fs::File::create(path)?;
        raw.write_all(&SID_MAP_MAGIC)?;
        raw.write_all(&0u64.to_le_bytes())?; // placeholder count
        Ok(Self {
            file: BufWriter::new(raw),
            count: 0,
        })
    }

    #[inline]
    fn push(&mut self, sid64: u64) -> io::Result<()> {
        self.file.write_all(&sid64.to_le_bytes())?;
        self.count += 1;
        Ok(())
    }

    fn finish(mut self, path: &Path) -> io::Result<()> {
        self.file.flush()?;
        let mut raw = self.file.into_inner()?;
        raw.seek(SeekFrom::Start(4))?;
        raw.write_all(&self.count.to_le_bytes())?;
        raw.flush()?;
        let _ = std::fs::metadata(path)?;
        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_index::resolve::chunk_dict::{ChunkStringDict, ChunkSubjectDict};
    use crate::run_index::runs::spool::{
        MmapStringRemap, MmapSubjectRemap, StringRemap, SubjectRemap,
    };

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir()
            .join("fluree_vocab_merge_tests")
            .join(name);
        if dir.exists() {
            std::fs::remove_dir_all(&dir).unwrap();
        }
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Build a ChunkSubjectDict, write sorted vocab, return the path.
    fn make_subject_vocab(dir: &Path, chunk_id: usize, entries: &[(u16, &[u8])]) -> PathBuf {
        let mut dict = ChunkSubjectDict::new();
        for &(ns, name) in entries {
            dict.get_or_insert(ns, name);
        }
        let path = dir.join(format!("chunk_{chunk_id:05}.subjects.voc"));
        dict.write_sorted_vocab(&path).unwrap();
        path
    }

    /// Build a ChunkStringDict, write sorted vocab, return the path.
    fn make_string_vocab(dir: &Path, chunk_id: usize, entries: &[&[u8]]) -> PathBuf {
        let mut dict = ChunkStringDict::new();
        for &s in entries {
            dict.get_or_insert(s);
        }
        let path = dir.join(format!("chunk_{chunk_id:05}.strings.voc"));
        dict.write_sorted_vocab(&path).unwrap();
        path
    }

    // ---- Subject merge tests ----

    #[test]
    fn test_subject_merge_single_chunk() {
        let dir = temp_dir("subj_single");
        let remap_dir = dir.join("remap");
        std::fs::create_dir_all(&remap_dir).unwrap();

        let voc = make_subject_vocab(&dir, 0, &[(5, b"Alice"), (5, b"Bob"), (10, b"Carol")]);

        let ns = HashMap::from([
            (5u16, "http://ex.org/".to_string()),
            (10, "http://foo/".to_string()),
        ]);

        let stats = merge_subject_vocabs(&[voc], &[0], &remap_dir, &dir, &ns).unwrap();
        assert_eq!(stats.total_unique, 3);
        assert!(!stats.needs_wide);

        // Verify remap: 3 entries, each maps to a unique sid64.
        let remap = MmapSubjectRemap::open(remap_dir.join("subjects_00000.rmp")).unwrap();
        assert_eq!(SubjectRemap::len(&remap), 3);

        // local_id 0 → inserted first ("Alice" at ns=5), sorted as (5,"Alice")=first
        // local_id 1 → "Bob" at ns=5, sorted second
        // local_id 2 → "Carol" at ns=10, sorted third
        let sid_alice = remap.get(0).unwrap(); // "Alice" was local_id=0
        let sid_bob = remap.get(1).unwrap(); // "Bob" was local_id=1
        let sid_carol = remap.get(2).unwrap(); // "Carol" was local_id=2

        // All should be unique.
        assert_ne!(sid_alice, sid_bob);
        assert_ne!(sid_alice, sid_carol);
        assert_ne!(sid_bob, sid_carol);

        // sid64 format: (ns_code << 48) | local_counter_in_ns
        // ns=5 has Alice(0) and Bob(1), ns=10 has Carol(0)
        assert_eq!(sid_alice, 5u64 << 48);
        assert_eq!(sid_bob, (5u64 << 48) | 1);
        assert_eq!(sid_carol, 10u64 << 48);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_subject_merge_dedup_across_chunks() {
        let dir = temp_dir("subj_dedup");
        let remap_dir = dir.join("remap");
        std::fs::create_dir_all(&remap_dir).unwrap();

        // Chunk 0: Alice, Bob (ns=5)
        let voc0 = make_subject_vocab(&dir, 0, &[(5, b"Alice"), (5, b"Bob")]);
        // Chunk 1: Alice, Carol (ns=5) — "Alice" is a duplicate
        let voc1 = make_subject_vocab(&dir, 1, &[(5, b"Alice"), (5, b"Carol")]);

        let ns = HashMap::from([(5u16, "http://ex.org/".to_string())]);

        let stats = merge_subject_vocabs(&[voc0, voc1], &[0, 1], &remap_dir, &dir, &ns).unwrap();
        assert_eq!(stats.total_unique, 3); // Alice, Bob, Carol

        let remap0 = MmapSubjectRemap::open(remap_dir.join("subjects_00000.rmp")).unwrap();
        let remap1 = MmapSubjectRemap::open(remap_dir.join("subjects_00001.rmp")).unwrap();

        // In chunk 0: local_id 0 = Alice, local_id 1 = Bob
        let sid_alice_c0 = remap0.get(0).unwrap();
        let sid_bob_c0 = remap0.get(1).unwrap();

        // In chunk 1: local_id 0 = Alice, local_id 1 = Carol
        let sid_alice_c1 = remap1.get(0).unwrap();
        let sid_carol_c1 = remap1.get(1).unwrap();

        // Alice should get the same global ID in both chunks.
        assert_eq!(sid_alice_c0, sid_alice_c1);

        // Bob and Carol should be unique.
        assert_ne!(sid_bob_c0, sid_carol_c1);
        assert_ne!(sid_alice_c0, sid_bob_c0);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_subject_merge_deterministic() {
        // Same input → same output, regardless of run.
        let dir1 = temp_dir("subj_det1");
        let dir2 = temp_dir("subj_det2");

        let ns = HashMap::from([(1u16, "ns1:".to_string()), (2, "ns2:".to_string())]);

        for dir in [&dir1, &dir2] {
            let remap_dir = dir.join("remap");
            std::fs::create_dir_all(&remap_dir).unwrap();

            let v0 = make_subject_vocab(dir, 0, &[(1, b"X"), (2, b"Y")]);
            let v1 = make_subject_vocab(dir, 1, &[(1, b"X"), (1, b"Z")]);

            merge_subject_vocabs(&[v0, v1], &[0, 1], &remap_dir, dir, &ns).unwrap();
        }

        // Compare remap files byte-for-byte.
        for name in ["subjects_00000.rmp", "subjects_00001.rmp"] {
            let b1 = std::fs::read(dir1.join("remap").join(name)).unwrap();
            let b2 = std::fs::read(dir2.join("remap").join(name)).unwrap();
            assert_eq!(b1, b2, "remap file {name} differs between runs");
        }

        std::fs::remove_dir_all(&dir1).ok();
        std::fs::remove_dir_all(&dir2).ok();
    }

    #[test]
    fn test_subject_merge_empty_chunk() {
        let dir = temp_dir("subj_empty");
        let remap_dir = dir.join("remap");
        std::fs::create_dir_all(&remap_dir).unwrap();

        let v0 = make_subject_vocab(&dir, 0, &[(5, b"Alice")]);
        let v1 = make_subject_vocab(&dir, 1, &[]); // empty chunk
        let v2 = make_subject_vocab(&dir, 2, &[(5, b"Bob")]);

        let ns = HashMap::from([(5u16, "http://ex.org/".to_string())]);

        let stats = merge_subject_vocabs(&[v0, v1, v2], &[0, 1, 2], &remap_dir, &dir, &ns).unwrap();
        assert_eq!(stats.total_unique, 2);

        // Empty chunk's remap file should exist but be zero-length.
        let remap1_path = remap_dir.join("subjects_00001.rmp");
        assert!(remap1_path.exists());
        assert_eq!(std::fs::metadata(&remap1_path).unwrap().len(), 0);

        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- String merge tests ----

    #[test]
    fn test_string_merge_single_chunk() {
        let dir = temp_dir("str_single");
        let remap_dir = dir.join("remap");
        std::fs::create_dir_all(&remap_dir).unwrap();

        let voc = make_string_vocab(&dir, 0, &[b"alpha", b"beta", b"gamma"]);

        let stats = merge_string_vocabs(&[voc], &[0], &remap_dir, &dir).unwrap();
        assert_eq!(stats.total_unique, 3);

        let remap = MmapStringRemap::open(remap_dir.join("strings_00000.rmp")).unwrap();
        assert_eq!(StringRemap::len(&remap), 3);

        // Sorted order: alpha(1), beta(2), gamma(0) — local_ids are original insert order
        // Actually: insert order is alpha=0, beta=1, gamma=2
        // Sorted: alpha, beta, gamma — same order, so ids 0,1,2 → global 0,1,2
        assert_eq!(remap.get(0).unwrap(), 0); // alpha → global 0
        assert_eq!(remap.get(1).unwrap(), 1); // beta → global 1
        assert_eq!(remap.get(2).unwrap(), 2); // gamma → global 2

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_string_merge_dedup_across_chunks() {
        let dir = temp_dir("str_dedup");
        let remap_dir = dir.join("remap");
        std::fs::create_dir_all(&remap_dir).unwrap();

        let v0 = make_string_vocab(&dir, 0, &[b"hello", b"world"]);
        let v1 = make_string_vocab(&dir, 1, &[b"hello", b"rust"]); // "hello" is dup

        let stats = merge_string_vocabs(&[v0, v1], &[0, 1], &remap_dir, &dir).unwrap();
        assert_eq!(stats.total_unique, 3); // hello, rust, world

        let remap0 = MmapStringRemap::open(remap_dir.join("strings_00000.rmp")).unwrap();
        let remap1 = MmapStringRemap::open(remap_dir.join("strings_00001.rmp")).unwrap();

        // chunk 0: local_id 0 = "hello", local_id 1 = "world"
        let gid_hello_c0 = remap0.get(0).unwrap();
        let gid_world_c0 = remap0.get(1).unwrap();

        // chunk 1: local_id 0 = "hello", local_id 1 = "rust"
        let gid_hello_c1 = remap1.get(0).unwrap();
        let gid_rust_c1 = remap1.get(1).unwrap();

        // "hello" should have the same global ID in both chunks.
        assert_eq!(gid_hello_c0, gid_hello_c1);

        // All unique strings have different global IDs.
        let mut ids = vec![gid_hello_c0, gid_world_c0, gid_rust_c1];
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 3);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_string_merge_empty_chunk() {
        let dir = temp_dir("str_empty");
        let remap_dir = dir.join("remap");
        std::fs::create_dir_all(&remap_dir).unwrap();

        let v0 = make_string_vocab(&dir, 0, &[b"x"]);
        let v1 = make_string_vocab(&dir, 1, &[]);

        let stats = merge_string_vocabs(&[v0, v1], &[0, 1], &remap_dir, &dir).unwrap();
        assert_eq!(stats.total_unique, 1);

        let remap1_path = remap_dir.join("strings_00001.rmp");
        assert!(remap1_path.exists());
        assert_eq!(std::fs::metadata(&remap1_path).unwrap().len(), 0);

        std::fs::remove_dir_all(&dir).ok();
    }
}
