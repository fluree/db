//! Global dictionaries for dictionary resolution (Phase B).
//!
//! Three dictionary types serve different scaling needs:
//!
//! - **SubjectDict**: xxh3_128 hash-based reverse map, file-backed forward map.
//!   Handles 100M+ subjects at ~52 bytes/entry (~5GB RAM at 100M — "big iron" mode).
//!   The forward file is NOT on the hot path — only read for projection/upgrade.
//!
//! - **PredicateDict** / **StringValueDict**: `HashMap<String, u32>` with `&str` lookup
//!   via the `Borrow` trait. Appropriate for small-to-medium cardinality.
//!   Predicate dictionaries are typically < 10K entries. String value dictionaries
//!   can grow large for high-NDV string properties; per-predicate dictionaries
//!   are a later optimization.
//!
//! - **LanguageTagDict**: Per-run `u16` assignment. Rebuilt at each run flush.
//!
//! ## Note on Doubles as LEX_ID
//!
//! In Phase 4, non-integer doubles are stored as LEX_ID in the global
//! StringValueDict. For datasets with high-NDV float properties (e.g., sensor
//! readings), this can cause the string dictionary to grow large. Per-predicate
//! NUM_FLOAT dictionaries with midpoint-splitting ranks are a later optimization.

use fluree_db_core::vec_bi_dict::VecBiDict;
use fluree_db_core::GraphId;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use serde_json;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

// ============================================================================
// SubjectDict (xxh3_128 reverse map, file-backed forward map)
// ============================================================================

/// Subject dictionary using xxh3_128 hashing for O(1) repeat-subject lookup.
///
/// Hot path (repeat subjects): compute xxh3_128 + FxHashMap lookup. No string
/// construction, no disk I/O.
///
/// Novel entries: construct full IRI, append to forward file, insert into map.
///
/// Subject IDs are namespace-structured 64-bit values (sid64):
///   `sid64 = (ns_code_u16 << 48) | local_id_u48`
/// Per-namespace counters ensure local_ids are dense within each namespace.
///
/// Memory: ~60 bytes/entry (HashMap entry ~48B + forward vecs 20B).
/// At 100M subjects: ~6GB RAM. This is intentionally "big iron" mode;
/// production dictionary uses the partitioned-on-disk strategy from VALUE_ID_PROPOSAL.
pub struct SubjectDict {
    /// Reverse: xxh3_128(iri) → sid64 (raw u64).
    /// 128-bit hash makes collisions negligible (~10^-22 at 100M entries).
    reverse: FxHashMap<u128, u64>,
    /// Forward: sequential insertion index → offset into forward_file.
    /// Separate vecs for proper alignment (avoids 16B padded tuple).
    forward_offsets: Vec<u64>,
    /// Forward: sequential insertion index → byte length of IRI in forward_file.
    forward_lens: Vec<u32>,
    /// Forward: sequential insertion index → sid64 (for writing sid mapping file).
    forward_sids: Vec<u64>,
    /// Append-only file of IRI bytes (no length prefix — lengths in forward_lens).
    forward_file: Option<BufWriter<std::fs::File>>,
    /// Path to the forward file (for diagnostics/reopening).
    forward_path: PathBuf,
    /// Current write offset in the forward file.
    forward_write_offset: u64,
    /// Per-namespace next local_id counter. Indexed by ns_code (u16).
    /// Grows on demand when a new namespace is first seen.
    next_local_ids: Vec<u64>,
    /// Total number of entries across all namespaces.
    count: u64,
    /// Set when any namespace's local_id exceeds u16::MAX.
    /// Once set, never reverts. Determines narrow vs wide leaflet encoding.
    needs_wide: bool,
}

impl SubjectDict {
    /// Maximum local_id value within a 48-bit field.
    const MAX_LOCAL_ID: u64 = (1u64 << 48) - 1;

    /// Create a new SubjectDict with a forward file at the given path.
    pub fn new(forward_path: impl AsRef<Path>) -> io::Result<Self> {
        let path = forward_path.as_ref().to_path_buf();
        let file = std::fs::File::create(&path)?;
        Ok(Self {
            reverse: FxHashMap::default(),
            forward_offsets: Vec::new(),
            forward_lens: Vec::new(),
            forward_sids: Vec::new(),
            forward_file: Some(BufWriter::new(file)),
            forward_path: path,
            forward_write_offset: 0,
            next_local_ids: Vec::new(),
            count: 0,
            needs_wide: false,
        })
    }

    /// Create a SubjectDict without a forward file (in-memory only, for tests).
    pub fn new_memory() -> Self {
        Self {
            reverse: FxHashMap::default(),
            forward_offsets: Vec::new(),
            forward_lens: Vec::new(),
            forward_sids: Vec::new(),
            forward_file: None,
            forward_path: PathBuf::new(),
            forward_write_offset: 0,
            next_local_ids: Vec::new(),
            count: 0,
            needs_wide: false,
        }
    }

    /// Look up or insert an IRI by its pre-computed xxh3_128 hash.
    ///
    /// `ns_code` is the namespace code for this subject (determines which
    /// per-namespace counter allocates the local_id portion of the sid64).
    ///
    /// `iri_builder` is only called for novel entries (to get the full IRI
    /// for the forward file). Repeat subjects never call it.
    ///
    /// Returns the raw sid64 value (`(ns_code << 48) | local_id`).
    pub fn get_or_insert_with_hash<F>(
        &mut self,
        hash: u128,
        ns_code: u16,
        iri_builder: F,
    ) -> io::Result<u64>
    where
        F: FnOnce() -> String,
    {
        if let Some(&sid64) = self.reverse.get(&hash) {
            return Ok(sid64);
        }

        // Allocate next local_id for this namespace
        let ns_idx = ns_code as usize;
        if ns_idx >= self.next_local_ids.len() {
            self.next_local_ids.resize(ns_idx + 1, 0);
        }
        let local_id = self.next_local_ids[ns_idx];
        if local_id > Self::MAX_LOCAL_ID {
            return Err(io::Error::other(format!(
                "SubjectDict: local_id overflow for ns_code {ns_code} (exceeded 2^48)"
            )));
        }
        self.next_local_ids[ns_idx] = local_id + 1;

        // Track wide requirement
        if local_id > u16::MAX as u64 {
            self.needs_wide = true;
        }

        // Construct sid64
        let sid64 = ((ns_code as u64) << 48) | local_id;

        // Write to forward file if available
        let iri = iri_builder();
        let iri_bytes = iri.as_bytes();
        let offset = self.forward_write_offset;
        let len = iri_bytes.len() as u32;

        if let Some(ref mut writer) = self.forward_file {
            writer.write_all(iri_bytes)?;
        }

        self.forward_offsets.push(offset);
        self.forward_lens.push(len);
        self.forward_sids.push(sid64);
        self.forward_write_offset += len as u64;

        self.reverse.insert(hash, sid64);
        self.count += 1;
        Ok(sid64)
    }

    /// Convenience: compute xxh3_128 from the IRI string and insert.
    ///
    /// `ns_code` is the namespace code for this subject.
    pub fn get_or_insert(&mut self, iri: &str, ns_code: u16) -> io::Result<u64> {
        let hash = xxhash_rust::xxh3::xxh3_128(iri.as_bytes());
        let iri_owned = iri.to_string();
        self.get_or_insert_with_hash(hash, ns_code, move || iri_owned)
    }

    /// Number of entries in the dictionary.
    pub fn len(&self) -> u64 {
        self.count
    }

    /// Check if the dictionary is empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Whether any namespace's local_id has exceeded u16::MAX.
    ///
    /// When true, leaflet columns must use wide (u64) encoding.
    /// When false, narrow (u32) encoding suffices.
    pub fn needs_wide(&self) -> bool {
        self.needs_wide
    }

    /// Per-namespace max assigned local_id watermarks for `DictNovelty`.
    ///
    /// Returns `watermarks[i]` = max local_id for namespace code `i`.
    /// 0 for namespaces with no assigned subjects.
    pub fn subject_watermarks(&self) -> Vec<u64> {
        self.next_local_ids
            .iter()
            .map(|&next| next.saturating_sub(1))
            .collect()
    }

    /// Flush the forward file buffer to disk.
    pub fn flush(&mut self) -> io::Result<()> {
        if let Some(ref mut writer) = self.forward_file {
            writer.flush()?;
        }
        Ok(())
    }

    /// Read all entries as (sid64, iri_bytes) pairs.
    ///
    /// Reads the forward file from disk. Call `flush()` first to ensure
    /// all buffered writes are visible.
    pub fn read_all_entries(&self) -> io::Result<Vec<(u64, Vec<u8>)>> {
        let data = std::fs::read(&self.forward_path)?;
        let mut entries = Vec::with_capacity(self.count as usize);
        for seq in 0..self.count as usize {
            let offset = self.forward_offsets[seq] as usize;
            let len = self.forward_lens[seq] as usize;
            let sid64 = self.forward_sids[seq];
            entries.push((sid64, data[offset..offset + len].to_vec()));
        }
        Ok(entries)
    }

    /// Forward offset table: `offsets[seq]` = byte offset into subjects.fwd.
    /// Indexed by sequential insertion order (not sid64).
    pub fn forward_offsets(&self) -> &[u64] {
        &self.forward_offsets
    }

    /// Forward length table: `lens[seq]` = byte length of IRI in subjects.fwd.
    /// Indexed by sequential insertion order (not sid64).
    pub fn forward_lens(&self) -> &[u32] {
        &self.forward_lens
    }

    /// SubjectId table: `sids[seq]` = subject_id for the seq-th inserted subject.
    /// Used to write the sid mapping file alongside the forward index.
    pub fn forward_sids(&self) -> &[u64] {
        &self.forward_sids
    }

    /// Write a reverse hash index to `subjects.rev` for O(log N) IRI → s_id lookup.
    ///
    /// Format: `SRV2` magic (4B) + count (u64) + sorted records of
    /// `(hash_hi: u64, hash_lo: u64, sid64: u64)` — 24 bytes per record.
    ///
    /// Sorted by (hash_hi, hash_lo) for binary search at query time.
    pub fn write_reverse_index(&self, path: &Path) -> io::Result<()> {
        use std::io::Write;

        let mut entries: Vec<(u64, u64, u64)> = self
            .reverse
            .iter()
            .map(|(&hash, &sid64)| {
                let hi = (hash >> 64) as u64;
                let lo = hash as u64;
                (hi, lo, sid64)
            })
            .collect();

        // Sort by (hash_hi, hash_lo) for binary search
        entries.sort_unstable();

        let mut file = io::BufWriter::new(std::fs::File::create(path)?);
        file.write_all(b"SRV2")?;
        file.write_all(&(entries.len() as u64).to_le_bytes())?;

        for &(hi, lo, sid64) in &entries {
            file.write_all(&hi.to_le_bytes())?;
            file.write_all(&lo.to_le_bytes())?;
            file.write_all(&sid64.to_le_bytes())?;
        }

        file.flush()?;
        tracing::info!(
            path = %path.display(),
            entries = entries.len(),
            size_mb = (entries.len() * 24) / (1024 * 1024),
            "subject reverse index written (SRV2)"
        );
        Ok(())
    }
}

// ============================================================================
// PredicateDict (simple, always small)
// ============================================================================

/// Simple string → u32 dictionary for predicates and graphs.
///
/// Backed by `VecBiDict<u32>`: Vec for O(1) forward lookups, HashMap for
/// reverse lookups, with Arc<str> shared between both (no string duplication).
/// Appropriate for small cardinality (< 10K).
pub struct PredicateDict {
    inner: VecBiDict<u32>,
}

impl PredicateDict {
    pub fn new() -> Self {
        Self {
            inner: VecBiDict::new(0),
        }
    }

    /// Look up or insert a string, returning its sequential u32 ID.
    pub fn get_or_insert(&mut self, s: &str) -> u32 {
        self.inner.assign_or_lookup(s)
    }

    /// Look up or insert by prefix + name parts, avoiding heap allocation on hits.
    ///
    /// Uses a stack buffer to concatenate prefix + name for the HashMap lookup.
    /// Only allocates a heap String on miss (novel entry). Most predicate/graph
    /// IRIs are well under 256 bytes.
    pub fn get_or_insert_parts(&mut self, prefix: &str, name: &str) -> u32 {
        let total_len = prefix.len() + name.len();

        // Stack-based lookup for short IRIs (avoids heap allocation on hits)
        if total_len <= 256 {
            let mut buf = [0u8; 256];
            buf[..prefix.len()].copy_from_slice(prefix.as_bytes());
            buf[prefix.len()..total_len].copy_from_slice(name.as_bytes());
            // SAFETY: buf[..total_len] is copied from two valid UTF-8 &str slices.
            let iri = unsafe { std::str::from_utf8_unchecked(&buf[..total_len]) };

            if let Some(id) = self.inner.find(iri) {
                return id;
            }
        }

        // Miss (or rare long IRI): heap allocate for insertion
        let mut full_iri = String::with_capacity(total_len);
        full_iri.push_str(prefix);
        full_iri.push_str(name);
        self.inner.assign_or_lookup(&full_iri)
    }

    /// Look up a string without inserting.
    pub fn get(&self, s: &str) -> Option<u32> {
        self.inner.find(s)
    }

    /// Get the string for a given ID.
    pub fn resolve(&self, id: u32) -> Option<&str> {
        self.inner.resolve(id)
    }

    pub fn len(&self) -> u32 {
        self.inner.len() as u32
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Return all entries as (id, value_bytes) pairs. Already in-memory.
    pub fn all_entries(&self) -> Vec<(u64, Vec<u8>)> {
        self.inner
            .iter()
            .map(|(id, s)| (id as u64, s.as_bytes().to_vec()))
            .collect()
    }

    /// Write a reverse hash index for O(log N) string → str_id lookup.
    ///
    /// Format: `LRV1` magic (4B) + count (u32) + sorted records of
    /// `(hash_hi: u64, hash_lo: u64, str_id: u32)` — 20 bytes per record.
    ///
    /// Uses xxh3_128 of the string bytes, sorted by (hash_hi, hash_lo)
    /// for binary search at query time. Mirrors the `subjects.rev` format.
    pub fn write_reverse_index(&self, path: &Path) -> io::Result<()> {
        let mut entries: Vec<(u64, u64, u32)> = Vec::with_capacity(self.inner.len());

        for (str_id, s) in self.inner.iter() {
            let hash = xxhash_rust::xxh3::xxh3_128(s.as_bytes());
            let hi = (hash >> 64) as u64;
            let lo = hash as u64;
            entries.push((hi, lo, str_id));
        }

        entries.sort_unstable();

        let mut file = BufWriter::new(std::fs::File::create(path)?);
        file.write_all(b"LRV1")?;
        file.write_all(&(entries.len() as u32).to_le_bytes())?;

        for &(hi, lo, str_id) in &entries {
            file.write_all(&hi.to_le_bytes())?;
            file.write_all(&lo.to_le_bytes())?;
            file.write_all(&str_id.to_le_bytes())?;
        }

        file.flush()?;
        tracing::info!(
            path = %path.display(),
            entries = entries.len(),
            size_mb = (entries.len() * 20) / (1024 * 1024),
            "string reverse index written"
        );
        Ok(())
    }

    /// Reconstruct from an ordered list of IRIs (e.g., from `IndexRoot`).
    ///
    /// Entry at index `i` gets ID `i`. This is the safe way to seed a dict
    /// from persisted data — it guarantees ID stability.
    pub fn from_ordered_iris(iris: Vec<std::sync::Arc<str>>) -> Self {
        Self {
            inner: VecBiDict::from_ordered_vec(0, iris),
        }
    }

    /// Iterator over `(id, &str)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (u32, &str)> {
        self.inner.iter()
    }
}

impl Default for PredicateDict {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// SharedDictAllocator (thread-safe, for parallel import)
// ============================================================================

/// Thread-safe dictionary allocator for small-cardinality domains
/// (predicates, datatypes, graphs).
///
/// Backed by a `RwLock`-protected `VecBiDict<u32>`. Read-lock fast path
/// for hits; write-lock only for novel entries. Pre-seeded with domain
/// defaults (e.g., reserved datatype IDs, txn-meta graph).
///
/// Used alongside [`DictWorkerCache`] for per-worker lock-free lookups
/// in the parallel import pipeline.
pub struct SharedDictAllocator {
    inner: RwLock<VecBiDict<u32>>,
}

impl SharedDictAllocator {
    /// Create from an existing `PredicateDict` by cloning its inner state.
    ///
    /// The allocator inherits all pre-seeded entries (e.g., reserved
    /// datatypes, txn-meta graph) and continues ID allocation from
    /// where the dict left off.
    pub fn from_predicate_dict(dict: &PredicateDict) -> Self {
        Self {
            inner: RwLock::new(dict.inner.clone()),
        }
    }

    /// Thread-safe get-or-insert with read-lock fast path.
    ///
    /// Hot path (>99% of lookups): read lock + HashMap lookup.
    /// Cold path (novel entry): upgrades to write lock, double-checks,
    /// then inserts.
    pub fn get_or_insert(&self, s: &str) -> u32 {
        {
            let inner = self.inner.read();
            if let Some(id) = inner.find(s) {
                return id;
            }
        }
        // Write lock with double-check (another thread may have inserted)
        let mut inner = self.inner.write();
        inner.assign_or_lookup(s)
    }

    /// Thread-safe get-or-insert by prefix + name parts.
    ///
    /// Uses a stack buffer for short IRIs (< 256 bytes) to avoid heap
    /// allocation on hits. Only allocates on miss (novel entry).
    pub fn get_or_insert_parts(&self, prefix: &str, name: &str) -> u32 {
        let total_len = prefix.len() + name.len();

        // Stack-based lookup for short IRIs (avoids heap allocation on hits)
        if total_len <= 256 {
            let mut buf = [0u8; 256];
            buf[..prefix.len()].copy_from_slice(prefix.as_bytes());
            buf[prefix.len()..total_len].copy_from_slice(name.as_bytes());
            // SAFETY: buf[..total_len] is copied from two valid UTF-8 &str slices.
            let iri = unsafe { std::str::from_utf8_unchecked(&buf[..total_len]) };

            {
                let inner = self.inner.read();
                if let Some(id) = inner.find(iri) {
                    return id;
                }
            }
        }

        // Miss or rare long IRI: heap allocate + write lock
        let mut full_iri = String::with_capacity(total_len);
        full_iri.push_str(prefix);
        full_iri.push_str(name);
        let mut inner = self.inner.write();
        inner.assign_or_lookup(&full_iri)
    }

    /// Take a snapshot for [`DictWorkerCache`] initialization.
    ///
    /// Returns `(reverse_map, snapshot_next_id)` where `snapshot_next_id`
    /// is the current next-to-be-assigned ID. Workers use `snapshot_next_id`
    /// to identify which IDs were allocated after the snapshot.
    pub fn snapshot(&self) -> (FxHashMap<String, u32>, u32) {
        let inner = self.inner.read();
        let map: FxHashMap<String, u32> = inner.iter().map(|(id, s)| (s.to_string(), id)).collect();
        let next_id = inner.base_id() + inner.len() as u32;
        (map, next_id)
    }

    /// Number of entries.
    pub fn len(&self) -> u32 {
        self.inner.read().len() as u32
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    /// Forward resolve: id → IRI string.
    pub fn resolve(&self, id: u32) -> Option<String> {
        self.inner
            .read()
            .resolve(id)
            .map(std::string::ToString::to_string)
    }

    /// Return all entries as `(id, value_bytes)` pairs.
    ///
    /// Used for dict persistence / CAS upload after import completes.
    pub fn all_entries(&self) -> Vec<(u64, Vec<u8>)> {
        self.inner
            .read()
            .iter()
            .map(|(id, s)| (id as u64, s.as_bytes().to_vec()))
            .collect()
    }

    /// Write a reverse hash index (same format as `PredicateDict::write_reverse_index`).
    pub fn write_reverse_index(&self, path: &Path) -> io::Result<()> {
        let inner = self.inner.read();
        let mut entries: Vec<(u64, u64, u32)> = Vec::with_capacity(inner.len());

        for (str_id, s) in inner.iter() {
            let hash = xxhash_rust::xxh3::xxh3_128(s.as_bytes());
            let hi = (hash >> 64) as u64;
            let lo = hash as u64;
            entries.push((hi, lo, str_id));
        }

        entries.sort_unstable();

        let mut file = BufWriter::new(std::fs::File::create(path)?);
        file.write_all(b"LRV1")?;
        file.write_all(&(entries.len() as u32).to_le_bytes())?;

        for &(hi, lo, str_id) in &entries {
            file.write_all(&hi.to_le_bytes())?;
            file.write_all(&lo.to_le_bytes())?;
            file.write_all(&str_id.to_le_bytes())?;
        }

        file.flush()?;
        Ok(())
    }

    /// Create a shared allocator pre-seeded with the reserved datatype IDs.
    ///
    /// Matches the seeding in `new_datatype_dict()`: @id(0), xsd:string(1),
    /// xsd:boolean(2), ... @vector(13). 14 entries total.
    pub fn new_datatype() -> Self {
        Self::from_predicate_dict(&new_datatype_dict())
    }

    /// Create a shared allocator pre-seeded for system graph IDs:
    /// - dict_id=0 → g_id=1 txn-meta
    /// - dict_id=1 → g_id=2 config
    ///
    /// This aligns the index root `graph_iris` layout with `fluree-db-core`'s
    /// `GraphRegistry::apply_delta`, which reserves g_id=2 and assigns user graphs
    /// starting at g_id=3.
    pub fn new_graph(ledger_id: &str) -> Self {
        let mut d = PredicateDict::new();
        let txn_meta_iri = fluree_db_core::graph_registry::txn_meta_graph_iri(ledger_id);
        let config_iri = fluree_db_core::graph_registry::config_graph_iri(ledger_id);
        d.get_or_insert(&txn_meta_iri);
        d.get_or_insert(&config_iri);
        Self::from_predicate_dict(&d)
    }

    /// Create an empty shared allocator for predicates (no pre-seeded entries).
    pub fn new_predicate() -> Self {
        Self::from_predicate_dict(&PredicateDict::new())
    }

    /// Extract a snapshot as a `PredicateDict` (for persistence via `write_predicate_dict`).
    ///
    /// This clones the inner `VecBiDict` — safe to call after all workers are done.
    pub fn to_predicate_dict(&self) -> PredicateDict {
        PredicateDict {
            inner: self.inner.read().clone(),
        }
    }
}

// ============================================================================
// DictWorkerCache (per-worker, lock-free hot path)
// ============================================================================

/// Per-worker dictionary cache for lock-free lookups.
///
/// Created at worker spawn time from a snapshot of [`SharedDictAllocator`].
/// All lookups hit the local `FxHashMap` first (no lock). Only genuinely
/// new entries touch the shared allocator.
///
/// Follows the same pattern as [`WorkerCache`] in
/// `fluree-db-transact/src/namespace.rs`.
pub struct DictWorkerCache {
    alloc: Arc<SharedDictAllocator>,
    /// Local reverse map: IRI string → id. Populated from snapshot,
    /// extended incrementally on shared allocator misses.
    local_map: FxHashMap<String, u32>,
    /// The allocator's next-to-be-assigned ID at snapshot time.
    /// Any id >= this was allocated after the snapshot.
    snapshot_next_id: u32,
}

impl DictWorkerCache {
    /// Create a new worker cache from a snapshot of the shared allocator.
    pub fn new(alloc: Arc<SharedDictAllocator>) -> Self {
        let (local_map, snapshot_next_id) = alloc.snapshot();
        Self {
            alloc,
            local_map,
            snapshot_next_id,
        }
    }

    /// Look up or insert a string. Local cache hit path is lock-free.
    pub fn get_or_insert(&mut self, s: &str) -> u32 {
        // Local fast path — no lock
        if let Some(&id) = self.local_map.get(s) {
            return id;
        }
        // Shared allocator (may lock)
        let id = self.alloc.get_or_insert(s);
        self.local_map.insert(s.to_string(), id);
        id
    }

    /// Look up or insert by prefix + name parts.
    ///
    /// Uses a stack buffer to avoid heap allocation on local cache hits.
    pub fn get_or_insert_parts(&mut self, prefix: &str, name: &str) -> u32 {
        let total_len = prefix.len() + name.len();

        // Stack-based lookup for short IRIs
        if total_len <= 256 {
            let mut buf = [0u8; 256];
            buf[..prefix.len()].copy_from_slice(prefix.as_bytes());
            buf[prefix.len()..total_len].copy_from_slice(name.as_bytes());
            // SAFETY: buf[..total_len] is copied from two valid UTF-8 &str slices.
            let iri = unsafe { std::str::from_utf8_unchecked(&buf[..total_len]) };

            if let Some(&id) = self.local_map.get(iri) {
                return id;
            }
        }

        // Miss: heap allocate full IRI, go through shared allocator
        let mut full_iri = String::with_capacity(total_len);
        full_iri.push_str(prefix);
        full_iri.push_str(name);
        let id = self.alloc.get_or_insert(&full_iri);
        self.local_map.insert(full_iri, id);
        id
    }

    /// The snapshot watermark. IDs >= this value were allocated after
    /// this worker's snapshot (by this or other workers).
    pub fn snapshot_next_id(&self) -> u32 {
        self.snapshot_next_id
    }
}

// ============================================================================
// DictAllocator (enum wrapper abstracting over serial / parallel modes)
// ============================================================================

/// Abstraction over dictionary allocation for both serial and parallel paths.
///
/// - `Exclusive`: wraps `&mut PredicateDict` for serial paths (transact,
///   chunk 0, TriG).
/// - `Cached`: wraps `&mut DictWorkerCache` for parallel import workers.
pub enum DictAllocator<'a> {
    Exclusive(&'a mut PredicateDict),
    Cached(&'a mut DictWorkerCache),
}

impl DictAllocator<'_> {
    /// Look up or insert a string, returning its u32 ID.
    pub fn get_or_insert(&mut self, s: &str) -> u32 {
        match self {
            DictAllocator::Exclusive(dict) => dict.get_or_insert(s),
            DictAllocator::Cached(cache) => cache.get_or_insert(s),
        }
    }

    /// Look up or insert by prefix + name parts.
    pub fn get_or_insert_parts(&mut self, prefix: &str, name: &str) -> u32 {
        match self {
            DictAllocator::Exclusive(dict) => dict.get_or_insert_parts(prefix, name),
            DictAllocator::Cached(cache) => cache.get_or_insert_parts(prefix, name),
        }
    }
}

// ============================================================================
// StringValueDict (xxh3_128 reverse map, file-backed forward map)
// ============================================================================

/// Global string value dictionary with file-backed forward storage.
///
/// Uses xxh3_128 hashing for the reverse map (like SubjectDict), avoiding
/// storage of string values in memory. String bytes are written to an
/// append-only file; only the hash→id map, offsets, and lengths stay in RAM.
///
/// Memory per entry: ~52 bytes (FxHashMap<u128,u32> ~40B + offset 8B + len 4B).
/// At 80M entries: ~4.2GB vs ~16GB with the old in-memory approach.
///
/// Phase 4 limitation: all string/decimal/double/JSON values share one
/// global dictionary. Per-predicate string dictionaries are a later optimization.
pub struct StringValueDict {
    /// Reverse: xxh3_128(string) → str_id.
    /// 128-bit hash makes collisions negligible (~10^-22 at 80M entries).
    reverse: FxHashMap<u128, u32>,
    /// Forward: sequential insertion index → byte offset into forward file.
    forward_offsets: Vec<u64>,
    /// Forward: sequential insertion index → byte length in forward file.
    forward_lens: Vec<u32>,
    /// Append-only file of string bytes (no length prefix — lengths in forward_lens).
    forward_file: Option<BufWriter<std::fs::File>>,
    /// Path to the forward file (for reading back entries).
    forward_path: PathBuf,
    /// Current write offset in the forward file.
    forward_write_offset: u64,
    /// Total number of entries.
    count: u32,
}

impl StringValueDict {
    /// Create a new StringValueDict with a forward file at the given path.
    pub fn new(forward_path: impl AsRef<Path>) -> io::Result<Self> {
        let path = forward_path.as_ref().to_path_buf();
        let file = std::fs::File::create(&path)?;
        Ok(Self {
            reverse: FxHashMap::default(),
            forward_offsets: Vec::new(),
            forward_lens: Vec::new(),
            forward_file: Some(BufWriter::new(file)),
            forward_path: path,
            forward_write_offset: 0,
            count: 0,
        })
    }

    /// Create a StringValueDict without a forward file (in-memory only, for tests).
    pub fn new_memory() -> Self {
        Self {
            reverse: FxHashMap::default(),
            forward_offsets: Vec::new(),
            forward_lens: Vec::new(),
            forward_file: None,
            forward_path: PathBuf::new(),
            forward_write_offset: 0,
            count: 0,
        }
    }

    /// Look up or insert a string, returning its sequential u32 ID.
    ///
    /// On cache hit: hash + HashMap lookup (no allocation, no I/O).
    /// On miss: hash + HashMap insert + file write (string bytes go to disk).
    pub fn get_or_insert(&mut self, s: &str) -> io::Result<u32> {
        let hash = xxhash_rust::xxh3::xxh3_128(s.as_bytes());
        if let Some(&id) = self.reverse.get(&hash) {
            return Ok(id);
        }

        let id = self.count;
        let bytes = s.as_bytes();
        let offset = self.forward_write_offset;
        let len = bytes.len() as u32;

        if let Some(ref mut writer) = self.forward_file {
            writer.write_all(bytes)?;
        }

        self.forward_offsets.push(offset);
        self.forward_lens.push(len);
        self.forward_write_offset += len as u64;
        self.reverse.insert(hash, id);
        self.count += 1;
        Ok(id)
    }

    /// Look up a string without inserting.
    pub fn get(&self, s: &str) -> Option<u32> {
        let hash = xxhash_rust::xxh3::xxh3_128(s.as_bytes());
        self.reverse.get(&hash).copied()
    }

    /// Number of entries in the dictionary.
    pub fn len(&self) -> u32 {
        self.count
    }

    /// Check if the dictionary is empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Flush the forward file buffer to disk.
    pub fn flush(&mut self) -> io::Result<()> {
        if let Some(ref mut writer) = self.forward_file {
            writer.flush()?;
        }
        Ok(())
    }

    /// Forward offset table (for persisting the index).
    pub fn forward_offsets(&self) -> &[u64] {
        &self.forward_offsets
    }

    /// Forward length table (for persisting the index).
    pub fn forward_lens(&self) -> &[u32] {
        &self.forward_lens
    }

    /// Read all entries as (id, value_bytes) pairs from the forward file.
    ///
    /// Call `flush()` first to ensure all buffered writes are visible.
    pub fn all_entries(&self) -> io::Result<Vec<(u64, Vec<u8>)>> {
        let data = std::fs::read(&self.forward_path)?;
        let mut entries = Vec::with_capacity(self.count as usize);
        for i in 0..self.count as usize {
            let offset = self.forward_offsets[i] as usize;
            let len = self.forward_lens[i] as usize;
            entries.push((i as u64, data[offset..offset + len].to_vec()));
        }
        Ok(entries)
    }

    /// Write a reverse hash index for O(log N) string → str_id lookup.
    ///
    /// Format: `LRV1` magic (4B) + count (u32) + sorted records of
    /// `(hash_hi: u64, hash_lo: u64, str_id: u32)` — 20 bytes per record.
    ///
    /// The hashes are already stored in the reverse map, so no string
    /// re-reading or re-hashing is needed.
    pub fn write_reverse_index(&self, path: &Path) -> io::Result<()> {
        let mut entries: Vec<(u64, u64, u32)> = Vec::with_capacity(self.reverse.len());

        for (&hash, &str_id) in &self.reverse {
            let hi = (hash >> 64) as u64;
            let lo = hash as u64;
            entries.push((hi, lo, str_id));
        }

        entries.sort_unstable();

        let mut file = BufWriter::new(std::fs::File::create(path)?);
        file.write_all(b"LRV1")?;
        file.write_all(&(entries.len() as u32).to_le_bytes())?;

        for &(hi, lo, str_id) in &entries {
            file.write_all(&hi.to_le_bytes())?;
            file.write_all(&lo.to_le_bytes())?;
            file.write_all(&str_id.to_le_bytes())?;
        }

        file.flush()?;
        tracing::info!(
            path = %path.display(),
            entries = entries.len(),
            size_mb = (entries.len() * 20) / (1024 * 1024),
            "string reverse index written"
        );
        Ok(())
    }
}

// ============================================================================
// LanguageTagDict (per-run)
// ============================================================================

/// Per-run language tag dictionary.
///
/// Maps language tags (e.g., "en", "fr") to u16 IDs. ID 0 means "no language
/// tag". Rebuilt at each run flush — downstream merge renumbers.
///
/// Backed by `VecBiDict<u16>` with base_id=1 (1-based IDs; 0 = "no tag").
#[derive(Clone)]
pub struct LanguageTagDict {
    inner: VecBiDict<u16>,
}

impl LanguageTagDict {
    pub fn new() -> Self {
        Self {
            inner: VecBiDict::new(1),
        }
    }

    /// Look up or insert a language tag, returning its u16 ID (>= 1).
    /// Returns 0 if `tag` is None.
    pub fn get_or_insert(&mut self, tag: Option<&str>) -> u16 {
        match tag {
            Some(t) => self.inner.assign_or_lookup(t),
            None => 0,
        }
    }

    /// Get the tag string for a given ID.
    pub fn resolve(&self, id: u16) -> Option<&str> {
        if id == 0 {
            return None;
        }
        self.inner.resolve(id)
    }

    /// Number of distinct language tags (excluding the "none" sentinel).
    pub fn len(&self) -> u16 {
        self.inner.len() as u16
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Find the ID for a language tag (reverse lookup).
    ///
    /// Returns `None` if the tag is not in the dictionary.
    pub fn find_id(&self, tag: &str) -> Option<u16> {
        self.inner.find(tag)
    }

    /// Iterator over (id, tag) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (u16, &str)> {
        self.inner.iter()
    }

    /// Clear and reset the dictionary (for per-run reuse).
    pub fn clear(&mut self) {
        self.inner = VecBiDict::new(1);
    }

    /// Reconstruct from an ordered list of tags (e.g., from `IndexRoot`).
    ///
    /// Tag at index `i` gets ID `i + 1` (base_id=1; 0 = "no tag").
    pub fn from_ordered_tags(tags: Vec<std::sync::Arc<str>>) -> Self {
        Self {
            inner: VecBiDict::from_ordered_vec(1, tags),
        }
    }
}

impl Default for LanguageTagDict {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Datatype dict constants (dt_ids)
// ============================================================================

/// Reserved datatype dictionary IDs.
///
/// These constants are defined in `fluree_db_core::DatatypeDictId`.
/// Only types with special encoding/coercion rules get reserved IDs.
/// Everything else is dynamically assigned (ID 14+).
///
/// Type is `u16` to match `RunRecord.dt` — most datasets use ≤255 types
/// (encoded as u8 in leaf Region 2), but u16 supports up to 65535 distinct
/// datatype IRIs in a single import.
///
/// Create a new datatype dict with reserved entries pre-inserted.
///
/// Order matters: `get_or_insert` returns sequential IDs starting at 0.
/// Only types with special encoding/coercion rules are reserved.
pub(crate) fn new_datatype_dict() -> PredicateDict {
    let mut d = PredicateDict::new();
    d.get_or_insert("@id"); // 0
    d.get_or_insert(fluree_vocab::xsd::STRING); // 1
    d.get_or_insert(fluree_vocab::xsd::BOOLEAN); // 2
    d.get_or_insert(fluree_vocab::xsd::INTEGER); // 3
    d.get_or_insert(fluree_vocab::xsd::LONG); // 4
    d.get_or_insert(fluree_vocab::xsd::DECIMAL); // 5
    d.get_or_insert(fluree_vocab::xsd::DOUBLE); // 6
    d.get_or_insert(fluree_vocab::xsd::FLOAT); // 7
    d.get_or_insert(fluree_vocab::xsd::DATE_TIME); // 8
    d.get_or_insert(fluree_vocab::xsd::DATE); // 9
    d.get_or_insert(fluree_vocab::xsd::TIME); // 10
    d.get_or_insert(fluree_vocab::rdf::LANG_STRING); // 11
    d.get_or_insert(fluree_vocab::rdf::JSON); // 12
    d.get_or_insert(fluree_vocab::fluree::EMBEDDING_VECTOR); // 13
    d.get_or_insert(fluree_vocab::fluree::FULL_TEXT); // 14
    debug_assert_eq!(d.len(), 15);
    d
}

// ============================================================================
// GlobalDicts (bundle)
// ============================================================================

/// All global dictionaries needed for dictionary resolution.
pub struct GlobalDicts {
    pub subjects: SubjectDict,
    pub predicates: PredicateDict,
    pub graphs: PredicateDict,
    pub strings: StringValueDict,
    pub languages: LanguageTagDict,
    pub datatypes: PredicateDict,
    /// Per-graph, per-predicate overflow numeric arenas (BigInt/BigDecimal).
    /// Outer key = g_id, inner key = p_id.
    pub numbigs:
        FxHashMap<GraphId, FxHashMap<u32, fluree_db_binary_index::arena::numbig::NumBigArena>>,
    /// Per-graph, per-predicate vector arenas (packed f32).
    /// Outer key = g_id, inner key = p_id.
    pub vectors:
        FxHashMap<GraphId, FxHashMap<u32, fluree_db_binary_index::arena::vector::VectorArena>>,
    /// Fact-identity → vector arena handle mapping for retraction lookup.
    ///
    /// Keyed by `(g_id, s_id, p_id, o_i, f32_bits)` — the full RDF fact
    /// identity. `f32_bits` is the per-element bit pattern of the
    /// quantized f32 vector; this is required (not just `(s, p, o_i)`)
    /// because:
    /// - Two distinct subjects can hold the same value under the same
    ///   predicate, each with its own arena handle.
    /// - One subject can hold MULTIPLE different vector values under
    ///   the same predicate without a list index — both share
    ///   `(s, p, o_i=LIST_INDEX_NONE)` so a value-free key would have
    ///   the second insertion overwrite the first.
    ///
    /// `o_i = u32::MAX` is the sentinel for "no list index" (matches
    /// `LIST_INDEX_NONE` in run records).
    #[allow(clippy::type_complexity)]
    pub vector_fact_handles: FxHashMap<GraphId, FxHashMap<(u64, u32, u32, Vec<u32>), u32>>,
}

impl GlobalDicts {
    /// Create GlobalDicts with file-backed subject and string dictionaries.
    ///
    /// Creates `subjects.fwd` and `strings.fwd` in the given `run_dir`.
    /// The same `run_dir` must be passed to `persist()` later.
    ///
    /// Pre-inserts the ledger-scoped system graph IRIs as the first graph entries,
    /// guaranteeing stable IDs:
    /// - dict_id=0 → g_id=1 txn-meta
    /// - dict_id=1 → g_id=2 config
    pub fn new(run_dir: impl AsRef<Path>, ledger_id: &str) -> io::Result<Self> {
        let dir = run_dir.as_ref();
        let txn_meta_iri = fluree_db_core::graph_registry::txn_meta_graph_iri(ledger_id);
        let config_iri = fluree_db_core::graph_registry::config_graph_iri(ledger_id);
        let mut dicts = Self {
            subjects: SubjectDict::new(dir.join("subjects.fwd"))?,
            predicates: PredicateDict::new(),
            graphs: PredicateDict::new(),
            strings: StringValueDict::new(dir.join("strings.fwd"))?,
            languages: LanguageTagDict::new(),
            datatypes: new_datatype_dict(),
            numbigs: FxHashMap::default(),
            vectors: FxHashMap::default(),
            vector_fact_handles: FxHashMap::default(),
        };
        // Reserve g_id=1 for txn-meta, g_id=2 for config.
        dicts.graphs.get_or_insert(&txn_meta_iri);
        dicts.graphs.get_or_insert(&config_iri);
        Ok(dicts)
    }

    /// Create GlobalDicts with in-memory dictionaries (no disk files, for tests).
    ///
    /// Pre-inserts the ledger-scoped system graph IRIs as the first graph entries,
    /// guaranteeing stable IDs:
    /// - dict_id=0 → g_id=1 txn-meta
    /// - dict_id=1 → g_id=2 config
    pub fn new_memory(ledger_id: &str) -> Self {
        let txn_meta_iri = fluree_db_core::graph_registry::txn_meta_graph_iri(ledger_id);
        let config_iri = fluree_db_core::graph_registry::config_graph_iri(ledger_id);
        let mut dicts = Self {
            subjects: SubjectDict::new_memory(),
            predicates: PredicateDict::new(),
            graphs: PredicateDict::new(),
            strings: StringValueDict::new_memory(),
            languages: LanguageTagDict::new(),
            datatypes: new_datatype_dict(),
            numbigs: FxHashMap::default(),
            vectors: FxHashMap::default(),
            vector_fact_handles: FxHashMap::default(),
        };
        // Reserve g_id=1 for txn-meta, g_id=2 for config.
        dicts.graphs.get_or_insert(&txn_meta_iri);
        dicts.graphs.get_or_insert(&config_iri);
        dicts
    }

    /// Persist all dictionaries to disk alongside the run files.
    ///
    /// Writes:
    /// - `subjects.idx` — subject forward-file offset/len index
    /// - `subjects.sids` — sid64 mapping (sequential index → sid64)
    /// - `strings.idx` — string value forward-file index (fwd file already written incrementally)
    /// - `graphs.dict` — graph dictionary
    /// - `predicates.json` — predicate id→IRI table (for index-build p_width + tooling)
    pub fn persist(&mut self, run_dir: &Path) -> io::Result<()> {
        use crate::run_index::vocab::dict_io::{
            write_predicate_dict, write_subject_index, write_subject_sid_map,
        };

        // Flush subject forward file
        self.subjects.flush()?;

        // Write subject index (offsets + lens for subjects.fwd)
        write_subject_index(
            &run_dir.join("subjects.idx"),
            self.subjects.forward_offsets(),
            self.subjects.forward_lens(),
        )?;

        // Write sid64 mapping (sequential index → sid64)
        write_subject_sid_map(&run_dir.join("subjects.sids"), self.subjects.forward_sids())?;

        // Flush string forward file and write index.
        // The forward file (strings.fwd) is written incrementally during import;
        // we only need to flush remaining buffered bytes and write the index.
        self.strings.flush()?;
        write_subject_index(
            &run_dir.join("strings.idx"),
            self.strings.forward_offsets(),
            self.strings.forward_lens(),
        )?;

        // Write predicate id → IRI table (JSON array by id).
        // This is not a CAS artifact; the canonical query-time mapping is in the v4 root.
        let preds: Vec<&str> = (0..self.predicates.len())
            .map(|p_id| self.predicates.resolve(p_id).unwrap_or(""))
            .collect();
        std::fs::write(
            run_dir.join("predicates.json"),
            serde_json::to_vec(&preds)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
        )?;

        // Write graph dict
        write_predicate_dict(&run_dir.join("graphs.dict"), &self.graphs)?;

        // Write datatype dict
        write_predicate_dict(&run_dir.join("datatypes.dict"), &self.datatypes)?;

        // Write numbig arenas (per-graph subdirectories, one file per predicate)
        {
            let mut total_predicates = 0usize;
            let mut total_entries = 0usize;
            for (&g_id, per_pred) in &self.numbigs {
                if per_pred.is_empty() {
                    continue;
                }
                let nb_dir = run_dir.join(format!("g_{g_id}")).join("numbig");
                std::fs::create_dir_all(&nb_dir)?;
                for (&p_id, arena) in per_pred {
                    fluree_db_binary_index::arena::numbig::write_numbig_arena(
                        &nb_dir.join(format!("p_{p_id}.nba")),
                        arena,
                    )?;
                    total_predicates += 1;
                    total_entries += arena.len();
                }
            }
            if total_predicates > 0 {
                tracing::info!(
                    graphs = self.numbigs.len(),
                    predicates = total_predicates,
                    total_entries,
                    "numbig arenas persisted"
                );
            }
        }

        // Write vector arenas (per-graph subdirectories, shards + manifests per predicate)
        {
            let mut total_predicates = 0usize;
            let mut total_vectors = 0usize;
            for (&g_id, per_pred) in &self.vectors {
                if per_pred.is_empty() {
                    continue;
                }
                let vec_dir = run_dir.join(format!("g_{g_id}")).join("vectors");
                std::fs::create_dir_all(&vec_dir)?;
                for (&p_id, arena) in per_pred {
                    if arena.is_empty() {
                        continue;
                    }
                    let shard_paths = fluree_db_binary_index::arena::vector::write_vector_shards(
                        &vec_dir, p_id, arena,
                    )?;
                    // Write manifest with placeholder CAS addresses (local paths).
                    // The real CAS addresses are filled in during upload_dicts_to_cas.
                    let shard_infos: Vec<fluree_db_binary_index::arena::vector::ShardInfo> =
                        shard_paths
                            .iter()
                            .enumerate()
                            .map(|(i, path)| {
                                let cap = fluree_db_binary_index::arena::vector::SHARD_CAPACITY;
                                let start = i as u32 * cap;
                                let count = (arena.len() - start).min(cap);
                                fluree_db_binary_index::arena::vector::ShardInfo {
                                    cas: path.display().to_string(),
                                    count,
                                }
                            })
                            .collect();
                    fluree_db_binary_index::arena::vector::write_vector_manifest(
                        &vec_dir.join(format!("p_{p_id}.vam")),
                        arena,
                        &shard_infos,
                    )?;
                    total_predicates += 1;
                    total_vectors += arena.len() as usize;
                }
            }
            if total_predicates > 0 {
                tracing::info!(
                    graphs = self.vectors.len(),
                    predicates = total_predicates,
                    total_vectors,
                    "vector arenas persisted"
                );
            }
        }

        tracing::info!(
            subjects = self.subjects.len(),
            predicates = self.predicates.len(),
            strings = self.strings.len(),
            graphs = self.graphs.len(),
            datatypes = self.datatypes.len(),
            "dictionaries persisted"
        );

        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::DatatypeDictId;

    // ---- SubjectDict tests ----

    #[test]
    fn test_subject_dict_insert_and_dedup() {
        let mut dict = SubjectDict::new_memory();
        let ns: u16 = 100; // test namespace

        let id1 = dict.get_or_insert("http://example.org/Alice", ns).unwrap();
        let id2 = dict.get_or_insert("http://example.org/Bob", ns).unwrap();
        let id1_again = dict.get_or_insert("http://example.org/Alice", ns).unwrap();

        // sid64 = (ns << 48) | local_id
        let expected_0 = (ns as u64) << 48;
        let expected_1 = ((ns as u64) << 48) | 1;
        assert_eq!(id1, expected_0);
        assert_eq!(id2, expected_1);
        assert_eq!(id1, id1_again);
        assert_eq!(dict.len(), 2);
    }

    #[test]
    fn test_subject_dict_streaming_hash() {
        let mut dict = SubjectDict::new_memory();
        let ns: u16 = 100;

        // Simulate streaming hash: prefix + name
        let prefix = "http://example.org/";
        let name = "Alice";

        use xxhash_rust::xxh3::Xxh3;
        let mut hasher = Xxh3::new();
        hasher.update(prefix.as_bytes());
        hasher.update(name.as_bytes());
        let hash = hasher.digest128();

        let full_iri = format!("{prefix}{name}");
        let id1 = dict
            .get_or_insert_with_hash(hash, ns, || full_iri.clone())
            .unwrap();

        // Same hash → same ID (no iri_builder called)
        let mut called = false;
        let id2 = dict
            .get_or_insert_with_hash(hash, ns, || {
                called = true;
                full_iri.clone()
            })
            .unwrap();

        assert_eq!(id1, id2);
        assert!(
            !called,
            "iri_builder should not be called for existing entry"
        );
    }

    #[test]
    fn test_subject_dict_with_file() {
        let dir = std::env::temp_dir().join("fluree_test_subject_dict");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("subjects.fwd");

        let mut dict = SubjectDict::new(&path).unwrap();
        dict.get_or_insert("http://example.org/Alice", 100).unwrap();
        dict.get_or_insert("http://example.org/Bob", 100).unwrap();
        dict.flush().unwrap();

        // Verify forward file exists and has content
        let meta = std::fs::metadata(&path).unwrap();
        assert!(meta.len() > 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_subject_dict_per_namespace_ids() {
        let mut dict = SubjectDict::new_memory();

        // Insert subjects in two different namespaces
        let ns_a: u16 = 10;
        let ns_b: u16 = 20;

        let id_a0 = dict.get_or_insert("http://a.org/x", ns_a).unwrap();
        let id_b0 = dict.get_or_insert("http://b.org/y", ns_b).unwrap();
        let id_a1 = dict.get_or_insert("http://a.org/z", ns_a).unwrap();

        // Each namespace has its own local_id counter
        assert_eq!(id_a0, 10u64 << 48); // ns_a, local_id=0
        assert_eq!(id_b0, 20u64 << 48); // ns_b, local_id=0
        assert_eq!(id_a1, (10u64 << 48) | 1); // ns_a, local_id=1

        assert_eq!(dict.len(), 3);
        assert!(!dict.needs_wide());
    }

    #[test]
    fn test_subject_dict_needs_wide() {
        let mut dict = SubjectDict::new_memory();
        let ns: u16 = 5;

        // Insert u16::MAX + 1 subjects to trigger needs_wide
        for i in 0..=(u16::MAX as u64) {
            let iri = format!("http://example.org/entity/{i}");
            dict.get_or_insert(&iri, ns).unwrap();
        }
        // At this point local_id went from 0 to 65535 (u16::MAX) → still narrow
        // The u16::MAX-th subject has local_id = 65535, which fits u16
        // But the (u16::MAX+1)-th subject gets local_id = 65536, which exceeds u16
        assert_eq!(dict.len(), (u16::MAX as u64) + 1);
        // local_ids 0..=65535 all fit u16, 65536 exceeds → needs_wide should be true
        // Actually: 65536 subjects means local_ids 0..65535. The 65536th (index 65535)
        // has local_id=65535 which equals u16::MAX. We inserted u16::MAX+1 subjects,
        // so the last one (65536th) has local_id=65535. Not yet wide.
        // Let's insert one more to actually trigger it.
        dict.get_or_insert("http://example.org/entity/overflow", ns)
            .unwrap();
        assert!(
            dict.needs_wide(),
            "should need wide after exceeding u16::MAX local_id"
        );
    }

    // ---- PredicateDict tests ----

    #[test]
    fn test_predicate_dict_insert_and_lookup() {
        let mut dict = PredicateDict::new();

        let id1 = dict.get_or_insert("http://example.org/name");
        let id2 = dict.get_or_insert("http://example.org/age");
        let id1_again = dict.get_or_insert("http://example.org/name");

        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(id1, id1_again);
        assert_eq!(dict.len(), 2);
    }

    #[test]
    fn test_predicate_dict_str_lookup() {
        let mut dict = PredicateDict::new();
        dict.get_or_insert("http://example.org/name");
        dict.get_or_insert("http://example.org/age");

        // Lookup with &str (no allocation)
        assert_eq!(dict.get("http://example.org/name"), Some(0));
        assert_eq!(dict.get("http://example.org/age"), Some(1));
        assert_eq!(dict.get("http://example.org/missing"), None);
    }

    #[test]
    fn test_predicate_dict_resolve() {
        let mut dict = PredicateDict::new();
        dict.get_or_insert("alpha");
        dict.get_or_insert("beta");

        assert_eq!(dict.resolve(0), Some("alpha"));
        assert_eq!(dict.resolve(1), Some("beta"));
        assert_eq!(dict.resolve(2), None);
    }

    // ---- LanguageTagDict tests ----

    #[test]
    fn test_lang_dict_insert_and_resolve() {
        let mut dict = LanguageTagDict::new();

        assert_eq!(dict.get_or_insert(None), 0);
        assert_eq!(dict.get_or_insert(Some("en")), 1);
        assert_eq!(dict.get_or_insert(Some("fr")), 2);
        assert_eq!(dict.get_or_insert(Some("en")), 1); // dedup
        assert_eq!(dict.len(), 2);

        assert_eq!(dict.resolve(0), None);
        assert_eq!(dict.resolve(1), Some("en"));
        assert_eq!(dict.resolve(2), Some("fr"));
    }

    #[test]
    fn test_lang_dict_clear() {
        let mut dict = LanguageTagDict::new();
        dict.get_or_insert(Some("en"));
        dict.get_or_insert(Some("de"));
        assert_eq!(dict.len(), 2);

        dict.clear();
        assert_eq!(dict.len(), 0);
        assert!(dict.is_empty());

        // After clear, new insertions start fresh
        assert_eq!(dict.get_or_insert(Some("ja")), 1);
    }

    #[test]
    fn test_lang_dict_iter() {
        let mut dict = LanguageTagDict::new();
        dict.get_or_insert(Some("en"));
        dict.get_or_insert(Some("fr"));
        dict.get_or_insert(Some("de"));

        let pairs: Vec<_> = dict.iter().collect();
        assert_eq!(pairs, vec![(1, "en"), (2, "fr"), (3, "de")]);
    }

    // ---- GlobalDicts tests ----

    #[test]
    fn test_global_dicts_memory() {
        let mut dicts = GlobalDicts::new_memory("test:main");
        dicts
            .subjects
            .get_or_insert("http://example.org/Alice", 100)
            .unwrap();
        dicts.predicates.get_or_insert("http://example.org/name");
        dicts.strings.get_or_insert("Alice").unwrap();
        dicts.languages.get_or_insert(Some("en"));

        assert_eq!(dicts.subjects.len(), 1);
        assert_eq!(dicts.predicates.len(), 1);
        assert_eq!(dicts.strings.len(), 1);
        assert_eq!(dicts.languages.len(), 1);
        // datatypes dict has 15 reserved entries
        assert_eq!(dicts.datatypes.len(), 15);
    }

    // ---- StringValueDict tests ----

    #[test]
    fn test_string_value_dict_insert_and_dedup() {
        let mut dict = StringValueDict::new_memory();
        let id1 = dict.get_or_insert("hello").unwrap();
        let id2 = dict.get_or_insert("world").unwrap();
        let id1_again = dict.get_or_insert("hello").unwrap();

        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(id1, id1_again);
        assert_eq!(dict.len(), 2);
    }

    #[test]
    fn test_string_value_dict_get() {
        let mut dict = StringValueDict::new_memory();
        dict.get_or_insert("alpha").unwrap();
        dict.get_or_insert("beta").unwrap();

        assert_eq!(dict.get("alpha"), Some(0));
        assert_eq!(dict.get("beta"), Some(1));
        assert_eq!(dict.get("gamma"), None);
    }

    #[test]
    fn test_string_value_dict_with_file() {
        let dir = std::env::temp_dir().join("fluree_test_string_value_dict");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("strings.fwd");

        let mut dict = StringValueDict::new(&path).unwrap();
        dict.get_or_insert("Alice").unwrap();
        dict.get_or_insert("Bob").unwrap();
        dict.get_or_insert("Charlie").unwrap();
        dict.flush().unwrap();

        // Verify forward file exists and has content
        let meta = std::fs::metadata(&path).unwrap();
        assert!(meta.len() > 0);

        // Read back all entries from file
        let entries = dict.all_entries().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(&entries[0].1, b"Alice");
        assert_eq!(&entries[1].1, b"Bob");
        assert_eq!(&entries[2].1, b"Charlie");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Datatype dict tests ----

    #[test]
    fn test_new_datatype_dict_reserved_entries() {
        let d = new_datatype_dict();
        assert_eq!(d.len(), 15);

        // Verify reserved positions match DatatypeDictId constants
        assert_eq!(d.get("@id"), Some(DatatypeDictId::ID.as_u16() as u32));
        assert_eq!(
            d.get(fluree_vocab::xsd::STRING),
            Some(DatatypeDictId::STRING.as_u16() as u32)
        );
        assert_eq!(
            d.get(fluree_vocab::xsd::BOOLEAN),
            Some(DatatypeDictId::BOOLEAN.as_u16() as u32)
        );
        assert_eq!(
            d.get(fluree_vocab::xsd::INTEGER),
            Some(DatatypeDictId::INTEGER.as_u16() as u32)
        );
        assert_eq!(
            d.get(fluree_vocab::xsd::LONG),
            Some(DatatypeDictId::LONG.as_u16() as u32)
        );
        assert_eq!(
            d.get(fluree_vocab::xsd::DECIMAL),
            Some(DatatypeDictId::DECIMAL.as_u16() as u32)
        );
        assert_eq!(
            d.get(fluree_vocab::xsd::DOUBLE),
            Some(DatatypeDictId::DOUBLE.as_u16() as u32)
        );
        assert_eq!(
            d.get(fluree_vocab::xsd::FLOAT),
            Some(DatatypeDictId::FLOAT.as_u16() as u32)
        );
        assert_eq!(
            d.get(fluree_vocab::xsd::DATE_TIME),
            Some(DatatypeDictId::DATE_TIME.as_u16() as u32)
        );
        assert_eq!(
            d.get(fluree_vocab::xsd::DATE),
            Some(DatatypeDictId::DATE.as_u16() as u32)
        );
        assert_eq!(
            d.get(fluree_vocab::xsd::TIME),
            Some(DatatypeDictId::TIME.as_u16() as u32)
        );
        assert_eq!(
            d.get(fluree_vocab::rdf::LANG_STRING),
            Some(DatatypeDictId::LANG_STRING.as_u16() as u32)
        );
        assert_eq!(
            d.get(fluree_vocab::rdf::JSON),
            Some(DatatypeDictId::JSON.as_u16() as u32)
        );
        assert_eq!(
            d.get(fluree_vocab::fluree::EMBEDDING_VECTOR),
            Some(DatatypeDictId::VECTOR.as_u16() as u32)
        );
        assert_eq!(
            d.get(fluree_vocab::fluree::FULL_TEXT),
            Some(DatatypeDictId::FULL_TEXT.as_u16() as u32)
        );
    }

    #[test]
    fn test_datatype_dict_idempotent_insert() {
        let mut d = new_datatype_dict();
        // Re-inserting a reserved type returns the same ID
        assert_eq!(d.get_or_insert("@id"), DatatypeDictId::ID.as_u16() as u32);
        assert_eq!(
            d.get_or_insert(fluree_vocab::xsd::STRING),
            DatatypeDictId::STRING.as_u16() as u32
        );
        assert_eq!(d.len(), 15); // no new entries
    }

    #[test]
    fn test_datatype_dict_dynamic_assignment() {
        let mut d = new_datatype_dict();
        // Custom/unknown types get dynamic IDs starting at 15
        let g_year_id = d.get_or_insert(fluree_vocab::xsd::G_YEAR);
        assert_eq!(g_year_id, DatatypeDictId::RESERVED_COUNT as u32); // 15
        let custom_id = d.get_or_insert("http://example.org/custom#myType");
        assert_eq!(custom_id, 16);
        assert_eq!(d.len(), 17);

        // Re-insert returns same ID
        assert_eq!(d.get_or_insert(fluree_vocab::xsd::G_YEAR), g_year_id);
    }

    // ---- String reverse index tests ----

    #[test]
    fn test_string_reverse_index_write_and_read() {
        let dir = std::env::temp_dir().join("fluree_test_string_rev");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("strings.rev");

        let mut dict = PredicateDict::new();
        dict.get_or_insert("SIGIR");
        dict.get_or_insert("VLDB");
        dict.get_or_insert("SIGMOD");

        dict.write_reverse_index(&path).unwrap();

        // Verify file structure
        let data = std::fs::read(&path).unwrap();
        assert_eq!(&data[0..4], b"LRV1");
        let count = u32::from_le_bytes(data[4..8].try_into().unwrap());
        assert_eq!(count, 3);
        assert_eq!(data.len(), 8 + 3 * 20); // header + 3 records × 20 bytes

        // Verify round-trip: hash each string and find it via binary search
        for (expected_str, expected_id) in &[("SIGIR", 0u32), ("VLDB", 1), ("SIGMOD", 2)] {
            let hash = xxhash_rust::xxh3::xxh3_128(expected_str.as_bytes());
            let target_hi = (hash >> 64) as u64;
            let target_lo = hash as u64;

            // Binary search over the written records
            let record_data = &data[8..];
            let record_size = 20;
            let mut lo = 0usize;
            let mut hi = count as usize;
            let mut found = None;
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                let off = mid * record_size;
                let mid_hi = u64::from_le_bytes(record_data[off..off + 8].try_into().unwrap());
                let mid_lo = u64::from_le_bytes(record_data[off + 8..off + 16].try_into().unwrap());
                match (mid_hi, mid_lo).cmp(&(target_hi, target_lo)) {
                    std::cmp::Ordering::Less => lo = mid + 1,
                    std::cmp::Ordering::Greater => hi = mid,
                    std::cmp::Ordering::Equal => {
                        let str_id =
                            u32::from_le_bytes(record_data[off + 16..off + 20].try_into().unwrap());
                        found = Some(str_id);
                        break;
                    }
                }
            }
            assert_eq!(
                found,
                Some(*expected_id),
                "failed to find {expected_str} in reverse index"
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- SharedDictAllocator tests ----

    #[test]
    fn test_shared_dict_from_predicate_dict() {
        let mut dict = PredicateDict::new();
        dict.get_or_insert("http://example.org/name");
        dict.get_or_insert("http://example.org/age");

        let alloc = SharedDictAllocator::from_predicate_dict(&dict);
        assert_eq!(alloc.len(), 2);

        // Pre-seeded entries are found via read-lock fast path
        assert_eq!(alloc.get_or_insert("http://example.org/name"), 0);
        assert_eq!(alloc.get_or_insert("http://example.org/age"), 1);
        assert_eq!(alloc.len(), 2); // no new entries

        // Novel entry gets next sequential ID
        assert_eq!(alloc.get_or_insert("http://example.org/email"), 2);
        assert_eq!(alloc.len(), 3);
    }

    #[test]
    fn test_shared_dict_from_datatype_dict() {
        let dt_dict = new_datatype_dict();
        let alloc = SharedDictAllocator::from_predicate_dict(&dt_dict);
        assert_eq!(alloc.len(), 15);

        // Reserved datatypes are found
        assert_eq!(
            alloc.get_or_insert(fluree_vocab::xsd::STRING),
            DatatypeDictId::STRING.as_u16() as u32
        );

        // Novel datatype gets ID 15
        assert_eq!(alloc.get_or_insert(fluree_vocab::xsd::G_YEAR), 15);
    }

    #[test]
    fn test_shared_dict_get_or_insert_parts() {
        let alloc = SharedDictAllocator::from_predicate_dict(&PredicateDict::new());

        let id1 = alloc.get_or_insert_parts("http://example.org/", "name");
        let id2 = alloc.get_or_insert_parts("http://example.org/", "age");
        let id1_again = alloc.get_or_insert_parts("http://example.org/", "name");

        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(id1, id1_again);
        assert_eq!(alloc.len(), 2);

        // Verify consistency with full-string insert
        assert_eq!(alloc.get_or_insert("http://example.org/name"), 0);
    }

    #[test]
    fn test_shared_dict_resolve_and_all_entries() {
        let alloc = SharedDictAllocator::from_predicate_dict(&PredicateDict::new());
        alloc.get_or_insert("alpha");
        alloc.get_or_insert("beta");

        assert_eq!(alloc.resolve(0), Some("alpha".to_string()));
        assert_eq!(alloc.resolve(1), Some("beta".to_string()));
        assert_eq!(alloc.resolve(2), None);

        let entries = alloc.all_entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(&entries[0], &(0u64, b"alpha".to_vec()));
        assert_eq!(&entries[1], &(1u64, b"beta".to_vec()));
    }

    #[test]
    fn test_shared_dict_concurrent_access() {
        let alloc = Arc::new(SharedDictAllocator::from_predicate_dict(
            &PredicateDict::new(),
        ));

        // Pre-seed some entries
        alloc.get_or_insert("http://example.org/name");
        alloc.get_or_insert("http://example.org/age");

        // Spawn threads that concurrently look up and insert
        let handles: Vec<_> = (0..4)
            .map(|thread_id| {
                let alloc = Arc::clone(&alloc);
                std::thread::spawn(move || {
                    let mut ids = Vec::new();
                    // All threads look up the same pre-seeded entries
                    ids.push(alloc.get_or_insert("http://example.org/name"));
                    ids.push(alloc.get_or_insert("http://example.org/age"));
                    // Each thread inserts a unique entry
                    ids.push(alloc.get_or_insert(&format!("http://example.org/prop_{thread_id}")));
                    ids
                })
            })
            .collect();

        let results: Vec<Vec<u32>> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // All threads agree on pre-seeded IDs
        for r in &results {
            assert_eq!(r[0], 0, "name should be ID 0");
            assert_eq!(r[1], 1, "age should be ID 1");
        }

        // Each thread's unique entry got a distinct ID >= 2
        let unique_ids: std::collections::HashSet<u32> = results.iter().map(|r| r[2]).collect();
        assert_eq!(
            unique_ids.len(),
            4,
            "4 unique props should get 4 distinct IDs"
        );
        for &id in &unique_ids {
            assert!(id >= 2, "unique IDs should start at 2, got {id}");
        }

        assert_eq!(alloc.len(), 6); // 2 pre-seeded + 4 unique
    }

    // ---- DictWorkerCache tests ----

    #[test]
    fn test_worker_cache_local_hit() {
        let mut dict = PredicateDict::new();
        dict.get_or_insert("http://example.org/name");
        dict.get_or_insert("http://example.org/age");

        let alloc = Arc::new(SharedDictAllocator::from_predicate_dict(&dict));
        let mut cache = DictWorkerCache::new(Arc::clone(&alloc));

        // Local hit — no lock needed
        assert_eq!(cache.get_or_insert("http://example.org/name"), 0);
        assert_eq!(cache.get_or_insert("http://example.org/age"), 1);

        // Miss → goes to shared allocator
        assert_eq!(cache.get_or_insert("http://example.org/email"), 2);

        // Subsequent lookup is now a local hit
        assert_eq!(cache.get_or_insert("http://example.org/email"), 2);
    }

    #[test]
    fn test_worker_cache_parts_lookup() {
        let mut dict = PredicateDict::new();
        dict.get_or_insert("http://example.org/name");

        let alloc = Arc::new(SharedDictAllocator::from_predicate_dict(&dict));
        let mut cache = DictWorkerCache::new(Arc::clone(&alloc));

        // Parts lookup hits local cache
        assert_eq!(cache.get_or_insert_parts("http://example.org/", "name"), 0);

        // Parts lookup for novel entry
        assert_eq!(cache.get_or_insert_parts("http://example.org/", "email"), 1);

        // Consistent with full-string lookup
        assert_eq!(cache.get_or_insert("http://example.org/email"), 1);
    }

    #[test]
    fn test_worker_cache_snapshot_watermark() {
        let mut dict = PredicateDict::new();
        dict.get_or_insert("alpha");
        dict.get_or_insert("beta");

        let alloc = Arc::new(SharedDictAllocator::from_predicate_dict(&dict));
        let cache = DictWorkerCache::new(Arc::clone(&alloc));

        // Snapshot watermark reflects pre-seeded entries
        assert_eq!(cache.snapshot_next_id(), 2);
    }

    #[test]
    fn test_worker_cache_sees_other_workers_inserts() {
        let alloc = Arc::new(SharedDictAllocator::from_predicate_dict(
            &PredicateDict::new(),
        ));

        let mut cache_a = DictWorkerCache::new(Arc::clone(&alloc));
        let mut cache_b = DictWorkerCache::new(Arc::clone(&alloc));

        // Worker A inserts
        let id_a = cache_a.get_or_insert("http://example.org/foo");
        assert_eq!(id_a, 0);

        // Worker B inserts the same string — gets same ID (from shared allocator)
        let id_b = cache_b.get_or_insert("http://example.org/foo");
        assert_eq!(id_b, 0);

        // Worker B inserts a new string
        let id_b2 = cache_b.get_or_insert("http://example.org/bar");
        assert_eq!(id_b2, 1);

        assert_eq!(alloc.len(), 2);
    }

    // ---- DictAllocator tests ----

    #[test]
    fn test_dict_allocator_exclusive_mode() {
        let mut dict = PredicateDict::new();
        let mut alloc = DictAllocator::Exclusive(&mut dict);

        assert_eq!(alloc.get_or_insert("alpha"), 0);
        assert_eq!(alloc.get_or_insert("beta"), 1);
        assert_eq!(alloc.get_or_insert_parts("http://", "gamma"), 2);
        assert_eq!(alloc.get_or_insert("alpha"), 0); // dedup
    }

    #[test]
    fn test_dict_allocator_cached_mode() {
        let dict = PredicateDict::new();
        let shared = Arc::new(SharedDictAllocator::from_predicate_dict(&dict));
        let mut cache = DictWorkerCache::new(shared);
        let mut alloc = DictAllocator::Cached(&mut cache);

        assert_eq!(alloc.get_or_insert("alpha"), 0);
        assert_eq!(alloc.get_or_insert("beta"), 1);
        assert_eq!(alloc.get_or_insert_parts("http://", "gamma"), 2);
        assert_eq!(alloc.get_or_insert("alpha"), 0); // dedup
    }

    #[test]
    fn test_graph_allocator_convention() {
        // Verify the graph allocator convention: dict_id + 1 = g_id.
        // Default graph (g_id=0) is NOT in the dict.
        // txn-meta is dict_id=0, so g_id=0+1=1.
        let txn_meta_iri = fluree_db_core::graph_registry::txn_meta_graph_iri("test:main");
        let mut graph_dict = PredicateDict::new();
        let txn_meta_dict_id = graph_dict.get_or_insert(&txn_meta_iri);
        assert_eq!(txn_meta_dict_id, 0);
        assert_eq!(txn_meta_dict_id + 1, 1); // g_id = 1 for txn-meta

        // Promote to shared allocator
        let alloc = Arc::new(SharedDictAllocator::from_predicate_dict(&graph_dict));
        let mut cache = DictWorkerCache::new(alloc);

        // txn-meta lookup returns same dict_id
        assert_eq!(cache.get_or_insert(&txn_meta_iri), 0);

        // Custom named graph gets dict_id=1 → g_id=2
        let custom_dict_id = cache.get_or_insert("http://example.org/graph/custom");
        assert_eq!(custom_dict_id, 1);
        assert_eq!(custom_dict_id + 1, 2); // g_id = 2
    }
}
