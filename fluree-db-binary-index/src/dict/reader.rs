//! Demand-loading read interface for dictionary trees.
//!
//! A `DictTreeReader` holds a decoded branch manifest and resolves lookups
//! by loading the appropriate leaf on demand. Leaf data comes from a content
//! store — read in place when the store has it locally, fetched otherwise —
//! or is provided in memory.
//!
//! When a shared `LeafletCache` is provided, lookups go through the global
//! LRU cache (respecting the customer's memory budget).

use fluree_db_core::clock::Instant;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::branch::DictBranch;
use super::reverse_leaf::{Fenced, Fences, ReverseLeaf};

use crate::read::cached_units::{CachedUnits, UnitCursor};
use crate::read::leaflet_cache::LeafletCache;
use fluree_db_core::{ContentBytes, ContentId, ContentStore, RangeSource};

/// Bytes between a fenced reverse leaf's fences: a lookup scans about this
/// much of the leaf.
const FENCE_SPACING: u64 = 4096;

/// A reverse leaf read by range: the first entry starting in each
/// `spacing`-byte stretch of it, found by reading it whole once. Held in the
/// leaflet cache's fence pool, keyed by the leaf's CID and weighed with its
/// fences, so every reader of that leaf shares it.
pub(crate) struct FencedLeaf {
    source: Arc<dyn RangeSource>,
    fences: Fences,
    entry_count: u32,
    len: u64,
}

impl FencedLeaf {
    /// Read `source` whole once to fence it; `None` unless its entries lie
    /// one after another in key order, which a scan relies on.
    fn open(source: Arc<dyn RangeSource>, spacing: u64) -> io::Result<Option<Self>> {
        let len = source.len();
        let bytes = source
            .read_range(0..len)
            .map_err(|e| io::Error::other(e.to_string()))?;
        let leaf = ReverseLeaf::from_bytes(&bytes)?;
        if !leaf.is_sequential() {
            return Ok(None);
        }
        let fences = leaf.fences(spacing.max(1) as usize);
        fluree_db_core::io_stats::record_work("reverse leaf fenced", bytes.len());
        Ok(Some(Self {
            entry_count: leaf.entry_count(),
            source,
            fences,
            len,
        }))
    }

    /// What holding it costs: the fences and the source handle.
    pub(crate) fn cache_weight(&self) -> usize {
        std::mem::size_of::<Self>() + self.fence_bytes() + self.source_charge()
    }

    pub(crate) fn fence_bytes(&self) -> usize {
        self.fences.heap_bytes()
    }

    /// The source handle's charge: for a mapped envelope, a fraction of the
    /// mapping's length, as for any mapped leaf.
    pub(crate) fn source_charge(&self) -> usize {
        self.source.cache_weight()
    }

    fn invalid(what: &str) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, format!("reverse leaf: {what}"))
    }

    fn lookup(&self, key: &[u8], cache: &Arc<LeafletCache>) -> io::Result<Option<u64>> {
        let Some(fenced) = self.fences.locate(key, self.entry_count, self.len) else {
            return Ok(None);
        };
        if fenced.end_offset < fenced.offset || fenced.end_offset > self.len {
            return Err(Self::invalid("fence out of bounds"));
        }
        let units = CachedUnits::new(Arc::clone(&self.source), Arc::clone(cache));
        Self::scan(&units, key, &fenced)
    }

    /// Scan the fenced entries in place: those wholly within a unit straight
    /// from its bytes, one crossing into the next through the cursor.
    fn scan(units: &CachedUnits, key: &[u8], fenced: &Fenced) -> io::Result<Option<u64>> {
        let size = units.unit_size();
        let mut cursor = UnitCursor::new(units);
        let mut pos = fenced.offset;
        let mut remaining = fenced.end_index - fenced.index;
        let mut found = None;
        'units: while remaining > 0 {
            let index = pos / size;
            let base = index * size;
            let unit = cursor.unit(index)?;
            let limit = fenced.end_offset.min(base + unit.len() as u64) - base;
            let bytes = &unit[..limit as usize];
            let mut at = (pos - base) as usize;
            while remaining > 0 {
                let Some(len) = bytes.get(at..at + 4) else {
                    break;
                };
                let key_len = u32::from_le_bytes(len.try_into().expect("four bytes")) as usize;
                let id_end = at + 4 + key_len + 8;
                let Some(entry) = bytes.get(at + 4..id_end) else {
                    break;
                };
                remaining -= 1;
                let (entry_key, id) = entry.split_at(key_len);
                match entry_key.cmp(key) {
                    std::cmp::Ordering::Less => at = id_end,
                    std::cmp::Ordering::Greater => break 'units,
                    std::cmp::Ordering::Equal => {
                        found = Some(u64::from_le_bytes(id.try_into().expect("eight bytes")));
                        break 'units;
                    }
                }
            }
            pos = base + at as u64;
            if remaining == 0 {
                break;
            }
            // The next entry runs out of this unit's bytes: across into the
            // next unit, or past the stretch.
            let len_end = pos + 4;
            if len_end > fenced.end_offset {
                return Err(Self::invalid("entry past its stretch"));
            }
            let key_len = u64::from(u32::from_le_bytes(
                cursor.read(pos..len_end)?.try_into().expect("four bytes"),
            ));
            let id_end = len_end + key_len + 8;
            if id_end > fenced.end_offset {
                return Err(Self::invalid("entry past its stretch"));
            }
            remaining -= 1;
            let entry = cursor.read(len_end..id_end)?;
            let (entry_key, id) = entry.split_at(key_len as usize);
            match entry_key.cmp(key) {
                std::cmp::Ordering::Less => pos = id_end,
                std::cmp::Ordering::Greater => break,
                std::cmp::Ordering::Equal => {
                    found = Some(u64::from_le_bytes(id.try_into().expect("eight bytes")));
                    break;
                }
            }
        }
        fluree_db_core::io_stats::record_work(
            "reverse leaf scanned",
            (pos - fenced.offset) as usize,
        );
        Ok(found)
    }
}

/// A clock for the slow-lookup debug logs below, read only when this module
/// logs at debug level. Nothing else reads these timings, and a clock read is
/// a large share of a lookup that hits a cached leaf. Its cost depends on the
/// platform and clock source: on arm64, macOS and Linux alike, a read and its
/// `elapsed()` take about 35 ns.
struct DebugClock(Option<Instant>);

impl DebugClock {
    fn start() -> Self {
        Self(tracing::enabled!(tracing::Level::DEBUG).then(Instant::now))
    }

    /// Time since the start or the previous lap, and restart the lap; zero
    /// when not timing.
    fn lap(&mut self) -> std::time::Duration {
        match &mut self.0 {
            Some(started) => {
                let now = Instant::now();
                let lap = now.duration_since(*started);
                *started = now;
                lap
            }
            None => std::time::Duration::ZERO,
        }
    }
}

/// Whole milliseconds, for log fields.
fn millis(d: std::time::Duration) -> u64 {
    d.as_millis() as u64
}

/// Leaf data source for demand-loading.
#[derive(Debug)]
pub enum LeafSource {
    /// Leaves in a content store, keyed by CAS address. Each is read in place
    /// when the store has it locally and fetched otherwise, decided at lookup:
    /// nothing is probed or downloaded while the reader is built (critical
    /// for Lambda + S3 cold starts). Leaf bytes are cached in `LeafletCache`
    /// when configured.
    Cas {
        cs: Arc<dyn ContentStore>,
        cids: HashMap<String, ContentId>,
    },
    /// Leaves are provided inline (for testing or small dictionaries).
    InMemory(HashMap<String, Arc<[u8]>>),
}

fn fetch_remote_leaf_bytes(cs: Arc<dyn ContentStore>, cid: ContentId) -> io::Result<ContentBytes> {
    // DictTreeReader is sync, but ContentStore::get is async. Bridge via the
    // shared `run_sync_on_runtime` helper, which uses
    // `block_in_place(handle.block_on)` on a multi-thread runtime (so a
    // replacement worker keeps driving the reactor while this thread blocks)
    // and a process-wide helper runtime when needed.
    //
    // The previous hand-rolled `thread::spawn` + outer-`Handle::block_on` +
    // `rx.recv()` re-injected the fetch onto the OUTER runtime with no
    // `block_in_place`: on a small (e.g. 2-worker) runtime every worker could
    // park in `recv()` with no thread left to drive the reactor, so the fetch
    // never completed — a hard wedge under query fan-out.
    let timeout = crate::read::binary_index_store::cas_sync_timeout();
    crate::read::binary_index_store::run_sync_on_runtime(async move {
        let fetch = async {
            cs.get(&cid)
                .await
                .map_err(|e| io::Error::other(e.to_string()))
        };
        // Optional per-fetch ceiling (FLUREE_CAS_SYNC_TIMEOUT_MS): a stalled
        // dict-leaf fetch self-aborts instead of blocking.
        match timeout {
            Some(dur) => tokio::time::timeout(dur, fetch).await.map_err(|_| {
                io::Error::other(format!(
                    "dict leaf CAS fetch timed out after {}ms",
                    dur.as_millis()
                ))
            })?,
            None => fetch.await,
        }
    })
}

/// A demand-loading reader for dictionary trees (forward or reverse).
///
/// For `LocalFiles` sources, uses the global `LeafletCache` (if provided)
/// to avoid repeated disk reads. Dict tree leaves are content-addressed
/// and immutable, so the CAS address hash has astronomically unlikely
/// collisions with no epoch/staging dimension.
/// Whether a reader reads leaves by range through fences: when they come
/// from a store that decrypts on local reads and it has a cache that keeps
/// their fences and units.
fn reads_fenced(leaf_source: &LeafSource, cache: Option<&Arc<LeafletCache>>) -> bool {
    matches!(leaf_source, LeafSource::Cas { cs, .. }
        if cache.is_some_and(|c| c.holds_fences()) && cs.costly_local_reads())
}

pub struct DictTreeReader {
    branch: DictBranch,
    /// Content id of `branch` when it was loaded from CAS; the identity a
    /// reload matches on to reuse this reader whole.
    branch_cid: Option<ContentId>,
    leaf_source: LeafSource,
    /// Shared global cache for dict leaf blobs. Content-addressed leaves
    /// use `xxh3_128(cas_address)` as the cache key — immutable, no
    /// epoch/time dimension needed.
    global_cache: Option<Arc<LeafletCache>>,
    /// Whether single-key lookups read leaves by range through fences.
    fenced: bool,
    fence_spacing: u64,
    /// Performance counters (atomic for shared access).
    disk_reads: AtomicU64,
    local_file_reads: AtomicU64,
    remote_fetches: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
}

impl DictTreeReader {
    /// Create a reader from a decoded branch and leaf source.
    pub fn new(branch: DictBranch, leaf_source: LeafSource) -> Self {
        Self {
            fenced: reads_fenced(&leaf_source, None),
            fence_spacing: FENCE_SPACING,
            branch,
            branch_cid: None,
            leaf_source,
            global_cache: None,
            disk_reads: AtomicU64::new(0),
            local_file_reads: AtomicU64::new(0),
            remote_fetches: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
        }
    }

    /// Create a reader backed by the global leaflet cache.
    ///
    /// The cache is shared across all stores and readers, giving one
    /// global memory budget. Dict leaves are cached by their CAS address
    /// hash (content-addressed → immutable, astronomically unlikely collisions).
    pub fn with_cache(
        branch: DictBranch,
        leaf_source: LeafSource,
        cache: Arc<LeafletCache>,
    ) -> Self {
        Self {
            fenced: reads_fenced(&leaf_source, Some(&cache)),
            fence_spacing: FENCE_SPACING,
            branch,
            branch_cid: None,
            leaf_source,
            global_cache: Some(cache),
            disk_reads: AtomicU64::new(0),
            local_file_reads: AtomicU64::new(0),
            remote_fetches: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
        }
    }

    /// Create a reader with all leaves pre-loaded in memory.
    pub fn from_memory(branch: DictBranch, leaves: HashMap<String, Vec<u8>>) -> Self {
        let arc_leaves: HashMap<String, Arc<[u8]>> = leaves
            .into_iter()
            .map(|(k, v)| (k, Arc::from(v.into_boxed_slice())))
            .collect();
        Self {
            fenced: false,
            fence_spacing: FENCE_SPACING,
            branch,
            branch_cid: None,
            leaf_source: LeafSource::InMemory(arc_leaves),
            global_cache: None,
            disk_reads: AtomicU64::new(0),
            local_file_reads: AtomicU64::new(0),
            remote_fetches: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
        }
    }

    /// Load a `DictTreeReader` from CAS-stored [`DictTreeRefs`].
    ///
    /// Fetches and decodes the branch, then constructs the reader with an
    /// optional leaflet cache. Leaves are resolved when a lookup needs them.
    pub async fn from_refs(
        cs: &Arc<dyn ContentStore>,
        refs: &crate::format::wire_helpers::DictTreeRefs,
        leaflet_cache: Option<&Arc<LeafletCache>>,
    ) -> io::Result<Self> {
        Self::load_refs(cs, refs, leaflet_cache).await
    }

    /// [`Self::from_refs`] for a reload: returns `prev` itself when it was
    /// built from the same branch cid, which is sound because branches are
    /// immutable content-addressed blobs — the same cid is the same bytes.
    pub async fn from_refs_reusing(
        cs: &Arc<dyn ContentStore>,
        refs: &crate::format::wire_helpers::DictTreeRefs,
        leaflet_cache: Option<&Arc<LeafletCache>>,
        prev: Option<&Arc<DictTreeReader>>,
    ) -> io::Result<Arc<Self>> {
        if let Some(prev) = prev {
            let same_branch = prev.branch_cid.as_ref() == Some(&refs.branch);
            let same_cache = match (&prev.global_cache, leaflet_cache) {
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                (None, None) => true,
                _ => false,
            };
            if same_branch && same_cache {
                return Ok(Arc::clone(prev));
            }
        }
        let reader = Self::load_refs(cs, refs, leaflet_cache).await?;
        Ok(Arc::new(reader))
    }

    async fn load_refs(
        cs: &Arc<dyn ContentStore>,
        refs: &crate::format::wire_helpers::DictTreeRefs,
        leaflet_cache: Option<&Arc<LeafletCache>>,
    ) -> io::Result<Self> {
        let branch_bytes = cs
            .get(&refs.branch)
            .await
            .map_err(|e| io::Error::other(format!("failed to load branch: {e}")))?;
        let branch = DictBranch::decode(&branch_bytes)?;

        let cids = refs
            .leaves
            .iter()
            .zip(branch.leaves.iter())
            .map(|(cid, bl)| (bl.address.clone(), cid.clone()))
            .collect();
        let leaf_source = LeafSource::Cas {
            cs: Arc::clone(cs),
            cids,
        };

        let mut reader = match leaflet_cache {
            Some(cache) => Self::with_cache(branch, leaf_source, Arc::clone(cache)),
            None => Self::new(branch, leaf_source),
        };
        reader.branch_cid = Some(refs.branch.clone());
        Ok(reader)
    }

    /// Attach a global cache to this reader.
    pub fn set_cache(&mut self, cache: Option<Arc<LeafletCache>>) {
        self.fenced = reads_fenced(&self.leaf_source, cache.as_ref());
        self.global_cache = cache;
    }

    #[cfg(test)]
    fn with_fence_spacing(mut self, spacing: u64) -> Self {
        self.fence_spacing = spacing;
        self
    }

    /// Leaf `leaf_idx` fenced for range reads, from the cache or opened
    /// there; `None` when it is read whole.
    fn fenced_leaf(&self, leaf_idx: usize) -> io::Result<Option<Arc<FencedLeaf>>> {
        if !self.fenced {
            return Ok(None);
        }
        let (LeafSource::Cas { cs, cids }, Some(cache)) = (&self.leaf_source, &self.global_cache)
        else {
            return Ok(None);
        };
        let address = &self.branch.leaves[leaf_idx].address;
        let cid = cids.get(address).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("dict tree: no CID mapping for leaf {address}"),
            )
        })?;
        let key = LeafletCache::cid_cache_key(&cid.to_bytes());
        cache.try_get_or_load_fenced_leaf(key, || {
            let Some(source) = cs
                .get_local_ranged(cid)
                .map_err(|e| io::Error::other(format!("dict leaf {cid}: {e}")))?
            else {
                cache.note_unfenceable_leaf();
                return Ok(None);
            };
            Ok(FencedLeaf::open(source, self.fence_spacing)?.map(Arc::new))
        })
    }

    /// The underlying branch manifest.
    pub fn branch(&self) -> &DictBranch {
        &self.branch
    }

    /// A short label for the configured leaf source.
    pub fn source_kind(&self) -> &'static str {
        match &self.leaf_source {
            LeafSource::Cas { .. } => "cas",
            LeafSource::InMemory(_) => "in_memory",
        }
    }

    /// Number of leaves in the decoded branch manifest.
    pub fn leaf_count(&self) -> usize {
        self.branch.leaves.len()
    }

    /// Whether a shared global cache is configured.
    pub fn has_global_cache(&self) -> bool {
        self.global_cache.is_some()
    }

    /// Reverse lookup: find ID by key bytes.
    pub fn reverse_lookup(&self, key: &[u8]) -> io::Result<Option<u64>> {
        const SLOW_LOOKUP_WARN_MS: u64 = 250;

        let mut clock = DebugClock::start();
        let leaf_idx = match self.branch.find_leaf(key) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        let find_leaf = clock.lap();

        if let Some(fenced) = self.fenced_leaf(leaf_idx)? {
            let cache = self
                .global_cache
                .as_ref()
                .expect("fenced leaves are cached");
            return fenced.lookup(key, cache);
        }

        let address = &self.branch.leaves[leaf_idx].address;
        let leaf_data = self.load_leaf(address)?;
        let load_leaf = clock.lap();
        let leaf = ReverseLeaf::from_bytes(&leaf_data)?;
        let decode_leaf = clock.lap();
        let result = leaf.lookup(key);
        let lookup_leaf = clock.lap();
        let total_ms = millis(find_leaf + load_leaf + decode_leaf + lookup_leaf);
        let (find_leaf_ms, load_leaf_ms) = (millis(find_leaf), millis(load_leaf));
        let (decode_leaf_ms, lookup_leaf_ms) = (millis(decode_leaf), millis(lookup_leaf));

        if total_ms >= SLOW_LOOKUP_WARN_MS || load_leaf_ms >= SLOW_LOOKUP_WARN_MS {
            tracing::debug!(
                key_len = key.len(),
                leaf_idx,
                address,
                source = self.source_kind(),
                total_ms,
                find_leaf_ms,
                load_leaf_ms,
                decode_leaf_ms,
                lookup_leaf_ms,
                disk_reads = self.disk_reads(),
                local_file_reads = self.local_file_reads(),
                remote_fetches = self.remote_fetches(),
                cache_hits = self.cache_hits(),
                cache_misses = self.cache_misses(),
                "dict tree: slow reverse lookup"
            );
        }

        Ok(result)
    }

    /// Distinct leaf `address`es the given keys map to, via the in-memory
    /// branch routing (no I/O). Lets a caller learn which leaves a batched
    /// reverse lookup will touch so it can prewarm them concurrently before
    /// the sync lookup runs. Preserves first-seen order; deduplicated.
    pub fn touched_leaf_addresses<'a, I>(&self, keys: I) -> Vec<&str>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let mut seen = vec![false; self.branch.leaves.len()];
        let mut addresses = Vec::new();
        for key in keys {
            if let Some(leaf_idx) = self.branch.find_leaf(key) {
                if !seen[leaf_idx] {
                    seen[leaf_idx] = true;
                    addresses.push(self.branch.leaves[leaf_idx].address.as_str());
                }
            }
        }
        addresses
    }

    /// The CID of the leaf at `address`, when this reader reads its leaves
    /// from a content store. `None` for in-memory leaves and for unknown
    /// addresses. Used to prewarm leaves before a sync reverse lookup.
    pub fn leaf_cid(&self, address: &str) -> Option<&ContentId> {
        match &self.leaf_source {
            LeafSource::Cas { cids, .. } => cids.get(address),
            LeafSource::InMemory(_) => None,
        }
    }

    /// Batched reverse lookup: find IDs by key bytes while loading each touched
    /// leaf at most once for the batch.
    pub fn reverse_lookup_many<'a, I>(&self, keys: I) -> io::Result<Vec<Option<u64>>>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        const SLOW_BATCH_WARN_MS: u64 = 250;

        let mut clock = DebugClock::start();
        let key_refs: Vec<&[u8]> = keys.into_iter().collect();
        if key_refs.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = vec![None; key_refs.len()];
        let mut key_indices_by_leaf = vec![Vec::<usize>::new(); self.branch.leaves.len()];

        for (idx, key) in key_refs.iter().enumerate() {
            if let Some(leaf_idx) = self.branch.find_leaf(key) {
                key_indices_by_leaf[leaf_idx].push(idx);
            }
        }
        let find_leaf = clock.lap();

        let mut touched_leaves = 0usize;
        for (leaf_idx, key_indices) in key_indices_by_leaf.iter().enumerate() {
            if key_indices.is_empty() {
                continue;
            }

            touched_leaves += 1;
            let address = &self.branch.leaves[leaf_idx].address;
            let leaf_data = self.load_leaf(address)?;
            let leaf = ReverseLeaf::from_bytes(&leaf_data)?;
            for &key_idx in key_indices {
                results[key_idx] = leaf.lookup(key_refs[key_idx]);
            }
        }
        let load_and_decode = clock.lap();
        let total_ms = millis(find_leaf + load_and_decode);
        let (find_leaf_ms, load_and_decode_ms) = (millis(find_leaf), millis(load_and_decode));

        if total_ms >= SLOW_BATCH_WARN_MS {
            tracing::debug!(
                key_count = key_refs.len(),
                touched_leaves,
                total_ms,
                find_leaf_ms,
                load_and_decode_ms,
                disk_reads = self.disk_reads(),
                local_file_reads = self.local_file_reads(),
                remote_fetches = self.remote_fetches(),
                cache_hits = self.cache_hits(),
                cache_misses = self.cache_misses(),
                "dict tree: slow batched reverse lookup"
            );
        }

        Ok(results)
    }

    /// Range scan: find all entries whose key is in `[start_key, end_key)`.
    ///
    /// Scans across multiple B-tree leaves as needed. Returns `(key_bytes, id)` pairs
    /// in sorted key order. Used for subject prefix scans (e.g., commit SHA lookup).
    pub fn reverse_range_scan(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> io::Result<Vec<(Vec<u8>, u64)>> {
        if self.branch.leaves.is_empty() {
            return Ok(Vec::new());
        }

        // The first leaf that can hold a key >= start_key: leaves are sorted
        // and disjoint, so it is the first whose last key is not below it. A
        // start key in the gap between two leaves begins at the later one.
        let start_leaf = self
            .branch
            .leaves
            .partition_point(|leaf| leaf.last_key.as_slice() < start_key);

        let mut results = Vec::new();

        for leaf_idx in start_leaf..self.branch.leaves.len() {
            let leaf_entry = &self.branch.leaves[leaf_idx];

            // If this leaf's first_key >= end_key, no more matches possible.
            if leaf_entry.first_key.as_slice() >= end_key {
                break;
            }

            let leaf_data = self.load_leaf(&leaf_entry.address)?;
            let leaf = ReverseLeaf::from_bytes(&leaf_data)?;

            for (key, id) in leaf.scan_range(start_key, end_key) {
                results.push((key.to_vec(), id));
            }
        }

        Ok(results)
    }

    /// Residency-mode leaf load: no filesystem and no sync→async bridge —
    /// serve the global cache, then the content store's residency tier
    /// (`get_local`), surfacing a typed
    /// [`crate::read::need_fetch::NeedFetch`] miss (recorded in the store's
    /// miss register) for an async caller to fetch and retry. Kept free of
    /// `Instant::now()` timing stamps, which trap on wasm32.
    #[cfg(any(target_arch = "wasm32", feature = "residency"))]
    fn load_leaf_resident(
        &self,
        cs: &Arc<dyn ContentStore>,
        cids: &HashMap<String, ContentId>,
        address: &str,
    ) -> io::Result<Arc<[u8]>> {
        use crate::read::need_fetch::{resident_or_need_fetch, FetchKind};
        let cid = cids.get(address).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("dict tree: no CID mapping for leaf {address}"),
            )
        })?;
        if let Some(cache) = &self.global_cache {
            let cache_key = xxhash_rust::xxh3::xxh3_128(address.as_bytes());
            if let Some(bytes) = cache.get_dict_leaf(cache_key) {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(bytes);
            }
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
            // Residency invariant: resolve residency BEFORE entering the
            // cache's loader closure — loader errors are shared as
            // `Arc<io::Error>` and re-wrapped, which keeps the typed miss
            // only via the rewrap helper's best effort.
            let bytes = resident_or_need_fetch(cs.as_ref(), cid, FetchKind::DictLeaf)?;
            cache.try_get_or_load_dict_leaf(cache_key, move || Ok(bytes.into_shared()))
        } else {
            resident_or_need_fetch(cs.as_ref(), cid, FetchKind::DictLeaf)
                .map(fluree_db_core::ContentBytes::into_shared)
        }
    }

    /// Load leaf bytes from the configured source, through the global cache
    /// (keyed by `xxh3_128(cas_address)`) when one is configured.
    ///
    /// Residency-mode stores (miss-register-bearing `Cas` sources) divert to
    /// [`Self::load_leaf_resident`] before any filesystem probe or timing
    /// stamp.
    fn load_leaf(&self, address: &str) -> io::Result<Arc<[u8]>> {
        #[cfg(any(target_arch = "wasm32", feature = "residency"))]
        if let LeafSource::Cas { cs, cids } = &self.leaf_source {
            if cs.miss_register().is_some() {
                return self.load_leaf_resident(cs, cids, address);
            }
        }

        let mut clock = DebugClock::start();
        let result = match &self.leaf_source {
            LeafSource::Cas { cs, cids } => {
                let cid = cids.get(address).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("dict tree: no CID mapping for leaf {address}"),
                    )
                })?;
                if let Some(cache) = &self.global_cache {
                    let cache_key = xxhash_rust::xxh3::xxh3_128(address.as_bytes());
                    if let Some(bytes) = cache.get_dict_leaf(cache_key) {
                        self.cache_hits.fetch_add(1, Ordering::Relaxed);
                        return Ok(bytes);
                    }
                    self.cache_misses.fetch_add(1, Ordering::Relaxed);
                    cache.try_get_or_load_dict_leaf(cache_key, || {
                        self.load_cas_leaf(cs, cid, address)
                    })
                } else {
                    self.load_cas_leaf(cs, cid, address)
                }
            }
            LeafSource::InMemory(map) => {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                map.get(address).cloned().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("dict tree: no in-memory leaf for {address}"),
                    )
                })
            }
        };

        if let Ok(bytes) = &result {
            let elapsed_ms = millis(clock.lap());
            if elapsed_ms >= 250 {
                tracing::debug!(
                    address,
                    source = self.source_kind(),
                    bytes = bytes.len(),
                    elapsed_ms,
                    disk_reads = self.disk_reads(),
                    local_file_reads = self.local_file_reads(),
                    remote_fetches = self.remote_fetches(),
                    cache_hits = self.cache_hits(),
                    cache_misses = self.cache_misses(),
                    "dict tree: slow leaf load"
                );
            }
        }

        result
    }

    /// One leaf from the content store: its local bytes when the store has
    /// them (for a remote store, its disk cache), else a fetch over the
    /// sync→async bridge.
    fn load_cas_leaf(
        &self,
        cs: &Arc<dyn ContentStore>,
        cid: &ContentId,
        address: &str,
    ) -> io::Result<Arc<[u8]>> {
        if let Some(bytes) = cs
            .get_local(cid)
            .map_err(|e| io::Error::other(format!("dict leaf {cid}: {e}")))?
        {
            if !matches!(bytes, fluree_db_core::ContentBytes::Shared(_)) {
                self.disk_reads.fetch_add(1, Ordering::Relaxed);
                self.local_file_reads.fetch_add(1, Ordering::Relaxed);
            }
            return Ok(bytes.into_shared());
        }
        tracing::debug!(address, %cid, "dict tree: remote leaf fetch starting");
        self.disk_reads.fetch_add(1, Ordering::Relaxed);
        self.remote_fetches.fetch_add(1, Ordering::Relaxed);
        let fetch_started = Instant::now();
        let bytes = fetch_remote_leaf_bytes(Arc::clone(cs), cid.clone())?;
        tracing::debug!(
            address,
            bytes = bytes.len(),
            elapsed_ms = fetch_started.elapsed().as_millis() as u64,
            "dict tree: remote leaf fetch complete"
        );
        Ok(bytes.into_shared())
    }

    /// Total entries across all leaves.
    pub fn total_entries(&self) -> u64 {
        self.branch.total_entries()
    }

    /// Number of disk reads performed since creation.
    pub fn disk_reads(&self) -> u64 {
        self.disk_reads.load(Ordering::Relaxed)
    }

    /// Number of local file reads performed since creation.
    pub fn local_file_reads(&self) -> u64 {
        self.local_file_reads.load(Ordering::Relaxed)
    }

    /// Number of remote fetches performed since creation.
    pub fn remote_fetches(&self) -> u64 {
        self.remote_fetches.load(Ordering::Relaxed)
    }

    /// Number of cache hits since creation (InMemory always counts as hit).
    pub fn cache_hits(&self) -> u64 {
        self.cache_hits.load(Ordering::Relaxed)
    }

    /// Number of cache misses since creation.
    pub fn cache_misses(&self) -> u64 {
        self.cache_misses.load(Ordering::Relaxed)
    }

    /// Preload all leaves into the global cache (or just read them into OS page cache).
    ///
    /// Returns the number of leaves loaded. This is useful for warming caches
    /// at server startup so the first query doesn't pay cold-start I/O penalties.
    pub fn preload_all_leaves(&self) -> io::Result<usize> {
        let mut loaded = 0;
        for entry in &self.branch.leaves {
            let _ = self.load_leaf(&entry.address)?;
            loaded += 1;
        }
        Ok(loaded)
    }
}

impl std::fmt::Debug for DictTreeReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DictTreeReader")
            .field("leaf_count", &self.branch.leaves.len())
            .field("total_entries", &self.total_entries())
            .field("has_global_cache", &self.global_cache.is_some())
            .field("source_kind", &self.source_kind())
            .field("disk_reads", &self.disk_reads())
            .field("local_file_reads", &self.local_file_reads())
            .field("remote_fetches", &self.remote_fetches())
            .field("cache_hits", &self.cache_hits())
            .field("cache_misses", &self.cache_misses())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dict::builder;
    use crate::dict::reverse_leaf::ReverseEntry;
    use std::path::PathBuf;

    fn build_reverse_reader(entries: Vec<ReverseEntry>) -> DictTreeReader {
        let result =
            builder::build_reverse_tree(entries, builder::DEFAULT_TARGET_LEAF_BYTES).unwrap();

        let mut leaf_map = HashMap::new();
        for (leaf_artifact, branch_leaf) in result.leaves.iter().zip(result.branch.leaves.iter()) {
            leaf_map.insert(branch_leaf.address.clone(), leaf_artifact.bytes.clone());
        }

        DictTreeReader::from_memory(result.branch, leaf_map)
    }

    /// A range whose start key sorts between two leaves (after one leaf's
    /// last key, before the next one's first) begins at the later leaf.
    #[test]
    fn range_scan_starting_between_leaves_reads_the_next_leaf() {
        let entries: Vec<ReverseEntry> = (0..64u64)
            .map(|i| ReverseEntry {
                key: format!("k{:04}", i * 2).into_bytes(),
                id: i,
            })
            .collect();
        let result = builder::build_reverse_tree(entries, 64).unwrap();
        assert!(result.branch.leaves.len() > 2);
        let gap_after = result.branch.leaves[0].last_key.clone();
        let next_first = result.branch.leaves[1].first_key.clone();
        let mut leaf_map = HashMap::new();
        for (leaf_artifact, branch_leaf) in result.leaves.iter().zip(result.branch.leaves.iter()) {
            leaf_map.insert(branch_leaf.address.clone(), leaf_artifact.bytes.clone());
        }
        let reader = DictTreeReader::from_memory(result.branch, leaf_map);

        // Odd numbers are absent, so "last key + 1" lies in the gap.
        let mut start = gap_after.clone();
        *start.last_mut().unwrap() += 1;
        assert!(
            start.as_slice() > gap_after.as_slice() && start.as_slice() < next_first.as_slice()
        );
        let got = reader.reverse_range_scan(&start, b"k9999").unwrap();
        assert_eq!(got.first().map(|(k, _)| k.clone()), Some(next_first));
        assert_eq!(got.len() as u64, 64 - got[0].1);
    }

    /// Content store whose blobs live in files it hands out through
    /// `get_local`, counting how often it is asked — the probe a reader must
    /// not make while it is built.
    #[derive(Debug)]
    struct CountingFileStore {
        dir: std::path::PathBuf,
        paths: parking_lot::Mutex<HashMap<ContentId, PathBuf>>,
        resolves: AtomicU64,
    }

    impl CountingFileStore {
        fn new() -> Self {
            static N: AtomicU64 = AtomicU64::new(0);
            let dir = std::env::temp_dir().join(format!(
                "fluree_dict_reader_reuse_{}_{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&dir).unwrap();
            Self {
                dir,
                paths: parking_lot::Mutex::new(HashMap::new()),
                resolves: AtomicU64::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl ContentStore for CountingFileStore {
        async fn has(&self, id: &ContentId) -> fluree_db_core::Result<bool> {
            Ok(self.paths.lock().contains_key(id))
        }
        async fn get(
            &self,
            id: &ContentId,
        ) -> fluree_db_core::Result<fluree_db_core::ContentBytes> {
            let path = self.paths.lock().get(id).cloned().expect("known cid");
            Ok(std::fs::read(path).unwrap().into())
        }
        async fn put(
            &self,
            kind: fluree_db_core::ContentKind,
            bytes: &[u8],
        ) -> fluree_db_core::Result<ContentId> {
            let id = ContentId::new(kind, bytes);
            self.put_with_id(&id, bytes).await?;
            Ok(id)
        }
        async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> fluree_db_core::Result<()> {
            let path = self.dir.join(format!("{}.blob", id.digest_hex()));
            std::fs::write(&path, bytes).unwrap();
            self.paths.lock().insert(id.clone(), path);
            Ok(())
        }
        async fn release(&self, _id: &ContentId) -> fluree_db_core::Result<()> {
            Ok(())
        }
        fn get_local(
            &self,
            id: &ContentId,
        ) -> fluree_db_core::Result<Option<fluree_db_core::ContentBytes>> {
            self.resolves.fetch_add(1, Ordering::Relaxed);
            let Some(path) = self.paths.lock().get(id).cloned() else {
                return Ok(None);
            };
            // SAFETY: the test never rewrites a file it has stored.
            unsafe { fluree_db_core::ContentBytes::open(&path) }
                .map_err(|e| fluree_db_core::Error::io(e.to_string()))
        }
    }

    /// Build a reverse tree from `keys` (ids in key order, offset by
    /// `first_id`), store its leaves, and return its branch entries paired
    /// with the leaf cids.
    async fn stored_leaves(
        cs: &CountingFileStore,
        keys: &[&str],
        first_id: u64,
    ) -> Vec<(super::super::branch::BranchLeafEntry, ContentId)> {
        let entries: Vec<ReverseEntry> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| ReverseEntry {
                key: k.as_bytes().to_vec(),
                id: first_id + i as u64,
            })
            .collect();
        // One leaf per few entries so a tree spans several leaves.
        let result = builder::build_reverse_tree(entries, 64).unwrap();
        let mut out = Vec::new();
        for (leaf, bl) in result.leaves.iter().zip(result.branch.leaves.iter()) {
            let cid = cs
                .put(
                    fluree_db_core::ContentKind::DictBlob {
                        dict: fluree_db_core::DictKind::SubjectReverse,
                    },
                    &leaf.bytes,
                )
                .await
                .unwrap();
            out.push((bl.clone(), cid));
        }
        out
    }

    async fn stored_tree(
        cs: &CountingFileStore,
        leaves: &[(super::super::branch::BranchLeafEntry, ContentId)],
    ) -> crate::format::wire_helpers::DictTreeRefs {
        let branch = DictBranch {
            leaves: leaves.iter().map(|(bl, _)| bl.clone()).collect(),
        };
        let branch_cid = cs
            .put(
                fluree_db_core::ContentKind::DictBlob {
                    dict: fluree_db_core::DictKind::SubjectReverse,
                },
                &branch.encode(),
            )
            .await
            .unwrap();
        crate::format::wire_helpers::DictTreeRefs {
            branch: branch_cid,
            leaves: leaves.iter().map(|(_, cid)| cid.clone()).collect(),
        }
    }

    /// Building a reader probes no leaf: each is resolved when a lookup needs
    /// it. A reload with the same branch cid is the same reader, and one with
    /// a grown branch resolves through old and new leaves alike.
    #[tokio::test]
    async fn readers_resolve_leaves_only_on_lookup() {
        let store = Arc::new(CountingFileStore::new());
        let cs: Arc<dyn ContentStore> = store.clone();

        let low = stored_leaves(&store, &["a", "b", "c", "d", "e", "f"], 0).await;
        let high = stored_leaves(&store, &["m", "n", "o", "p", "q", "r"], 100).await;
        assert!(low.len() >= 2, "fixture must span several leaves");

        let refs_v1 = stored_tree(&store, &low).await;
        let mut grown = low.clone();
        grown.extend(high.iter().cloned());
        let refs_v2 = stored_tree(&store, &grown).await;

        let v1 = DictTreeReader::from_refs_reusing(&cs, &refs_v1, None, None)
            .await
            .unwrap();
        assert_eq!(
            store.resolves.load(Ordering::Relaxed),
            0,
            "building a reader must not probe its leaves"
        );
        assert_eq!(v1.reverse_lookup(b"c").unwrap(), Some(2));
        assert_eq!(store.resolves.load(Ordering::Relaxed), 1);

        let same = DictTreeReader::from_refs_reusing(&cs, &refs_v1, None, Some(&v1))
            .await
            .unwrap();
        assert!(
            Arc::ptr_eq(&same, &v1),
            "an unchanged branch cid must hand back the previous reader"
        );

        let v2 = DictTreeReader::from_refs_reusing(&cs, &refs_v2, None, Some(&v1))
            .await
            .unwrap();
        assert!(!Arc::ptr_eq(&v2, &v1));
        assert_eq!(store.resolves.load(Ordering::Relaxed), 1);
        assert_eq!(v2.leaf_count(), grown.len());
        assert_eq!(v2.reverse_lookup(b"c").unwrap(), Some(2));
        assert_eq!(v2.reverse_lookup(b"p").unwrap(), Some(103));

        let _ = std::fs::remove_dir_all(&store.dir);
    }

    /// In-memory store that hands its leaves out shared, as memory storage
    /// does through the bridge. Fetches count.
    #[derive(Debug)]
    struct ResidentStore {
        inner: fluree_db_core::MemoryContentStore,
        resident: std::sync::RwLock<HashMap<ContentId, Arc<[u8]>>>,
        gets: AtomicU64,
    }

    #[async_trait::async_trait]
    impl ContentStore for ResidentStore {
        async fn has(&self, id: &ContentId) -> fluree_db_core::Result<bool> {
            self.inner.has(id).await
        }
        async fn get(
            &self,
            id: &ContentId,
        ) -> fluree_db_core::Result<fluree_db_core::ContentBytes> {
            self.gets.fetch_add(1, Ordering::Relaxed);
            self.inner.get(id).await
        }
        async fn put(
            &self,
            kind: fluree_db_core::ContentKind,
            bytes: &[u8],
        ) -> fluree_db_core::Result<ContentId> {
            let id = self.inner.put(kind, bytes).await?;
            self.resident
                .write()
                .unwrap()
                .insert(id.clone(), Arc::from(bytes));
            Ok(id)
        }
        async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> fluree_db_core::Result<()> {
            self.inner.put_with_id(id, bytes).await?;
            self.resident
                .write()
                .unwrap()
                .insert(id.clone(), Arc::from(bytes));
            Ok(())
        }
        async fn release(&self, id: &ContentId) -> fluree_db_core::Result<()> {
            self.inner.release(id).await
        }
        fn get_local(
            &self,
            id: &ContentId,
        ) -> fluree_db_core::Result<Option<fluree_db_core::ContentBytes>> {
            Ok(self
                .resident
                .read()
                .unwrap()
                .get(id)
                .cloned()
                .map(fluree_db_core::ContentBytes::Shared))
        }
    }

    /// A store holding its dict leaves in memory hands them out shared: a
    /// lookup borrows the leaf, through the global cache or not, and never
    /// fetches it.
    #[test]
    fn resident_dict_leaves_are_borrowed_not_fetched() {
        let entries = ["a", "b", "c"]
            .iter()
            .enumerate()
            .map(|(i, k)| ReverseEntry {
                key: k.as_bytes().to_vec(),
                id: i as u64,
            })
            .collect();
        let tree =
            builder::build_reverse_tree(entries, builder::DEFAULT_TARGET_LEAF_BYTES).unwrap();
        assert_eq!(tree.leaves.len(), 1);
        let address = tree.branch.leaves[0].address.clone();

        for with_cache in [false, true] {
            let store = Arc::new(ResidentStore {
                inner: fluree_db_core::MemoryContentStore::new(),
                resident: Default::default(),
                gets: AtomicU64::new(0),
            });
            let cs: Arc<dyn ContentStore> = store.clone();
            let cid = crate::read::binary_index_store::run_sync_on_runtime({
                let cs = Arc::clone(&cs);
                let bytes = tree.leaves[0].bytes.clone();
                async move {
                    cs.put(
                        fluree_db_core::ContentKind::DictBlob {
                            dict: fluree_db_core::DictKind::SubjectReverse,
                        },
                        &bytes,
                    )
                    .await
                    .map_err(|e| io::Error::other(e.to_string()))
                }
            })
            .unwrap();

            let mut cids = HashMap::new();
            cids.insert(address.clone(), cid.clone());
            let mut reader = DictTreeReader::new(tree.branch.clone(), LeafSource::Cas { cs, cids });
            if with_cache {
                reader.global_cache = Some(Arc::new(LeafletCache::with_max_mb(4)));
            }

            for _ in 0..2 {
                assert_eq!(
                    reader.reverse_lookup(b"c").unwrap(),
                    Some(2),
                    "with_cache={with_cache}"
                );
            }
            let leaf = reader.load_leaf(&address).unwrap();
            let held = store.resident.read().unwrap().get(&cid).cloned().unwrap();
            assert!(
                Arc::ptr_eq(&leaf, &held),
                "with_cache={with_cache}: the leaf must be the store's own allocation"
            );
            assert_eq!(
                store.gets.load(Ordering::Relaxed),
                0,
                "with_cache={with_cache}: a resident leaf must not be fetched"
            );
        }
    }

    #[test]
    fn test_reverse_lookup() {
        let mut entries: Vec<ReverseEntry> = (0..100)
            .map(|i| ReverseEntry {
                key: format!("key_{i:04}").into_bytes(),
                id: i as u64,
            })
            .collect();
        entries.sort_by(|a, b| a.key.cmp(&b.key));

        let reader = build_reverse_reader(entries);

        assert_eq!(reader.reverse_lookup(b"key_0050").unwrap(), Some(50));
        assert_eq!(reader.reverse_lookup(b"nonexistent").unwrap(), None);
    }

    /// The target the slow-lookup logs, and the clock's `enabled!` check, use.
    const DICT_READER_TARGET: &str = "fluree_db_binary_index::dict::reader";

    /// Runs `f` under `subscriber` as this thread's default, so a cached
    /// interest from another test can't decide whether the clock runs.
    fn with_subscriber<S, R>(subscriber: S, f: impl FnOnce() -> R) -> R
    where
        S: tracing::Subscriber + Send + Sync + 'static,
    {
        fluree_test_support::with_scoped_default(subscriber, f)
    }

    fn reader_at(level: tracing::Level) -> impl tracing::Subscriber + Send + Sync {
        use tracing_subscriber::layer::SubscriberExt;
        tracing_subscriber::registry()
            .with(tracing_subscriber::filter::Targets::new().with_target(DICT_READER_TARGET, level))
    }

    fn lap_after_sleep() -> (bool, std::time::Duration) {
        let mut clock = DebugClock::start();
        std::thread::sleep(std::time::Duration::from_millis(5));
        (clock.0.is_some(), clock.lap())
    }

    #[test]
    fn debug_clock_does_not_read_without_debug() {
        let none = with_subscriber(
            tracing::subscriber::NoSubscriber::default(),
            lap_after_sleep,
        );
        assert_eq!(none, (false, std::time::Duration::ZERO), "no subscriber");

        let info = with_subscriber(reader_at(tracing::Level::INFO), lap_after_sleep);
        assert_eq!(info, (false, std::time::Duration::ZERO), "reader at INFO");
    }

    #[test]
    fn debug_clock_times_laps_with_debug() {
        let (timing, lap) = with_subscriber(reader_at(tracing::Level::DEBUG), lap_after_sleep);
        assert!(timing, "reader at DEBUG should read the clock");
        assert!(
            lap >= std::time::Duration::from_millis(5),
            "lap {lap:?} should cover the 5ms sleep"
        );
    }

    /// Reverse leaves of a store whose local reads decrypt, read by range
    /// through per-leaf fences.
    mod fenced {
        use super::*;
        use crate::read::binary_index_store::tests::ranged_local::SegmentedContentStore;
        use fluree_db_core::{ContentKind, DictKind};
        use std::sync::atomic::Ordering as AtomicOrdering;

        fn entries() -> Vec<ReverseEntry> {
            let mut entries: Vec<ReverseEntry> = (0..6_000u64)
                .map(|i| ReverseEntry {
                    key: format!("subject-{:06}-{}", i * 2, "x".repeat((i % 40) as usize))
                        .into_bytes(),
                    id: i,
                })
                .collect();
            entries.sort_by(|a, b| a.key.cmp(&b.key));
            entries
        }

        /// A cache as storage that decrypts on read gets, with no room for
        /// dictionary packs.
        fn fence_cache(bytes: u64) -> Arc<LeafletCache> {
            Arc::new(LeafletCache::with_reservable_budget(bytes, Some(0)))
        }

        fn fenced_reader(
            cs: &SegmentedContentStore,
            leaves: Vec<(String, Vec<u8>)>,
            branch: DictBranch,
        ) -> DictTreeReader {
            reader_over(cs, leaves, branch, fence_cache(16 << 20))
        }

        fn reader_over(
            cs: &SegmentedContentStore,
            leaves: Vec<(String, Vec<u8>)>,
            branch: DictBranch,
            cache: Arc<LeafletCache>,
        ) -> DictTreeReader {
            let cids = leaves
                .into_iter()
                .map(|(address, bytes)| {
                    let kind = ContentKind::DictBlob {
                        dict: DictKind::SubjectReverse,
                    };
                    (address, cs.put_sync(kind, &bytes))
                })
                .collect();
            let source = LeafSource::Cas {
                cs: Arc::new(cs.clone()),
                cids,
            };
            DictTreeReader::with_cache(branch, source, cache)
        }

        fn built(entries: Vec<ReverseEntry>) -> (Vec<(String, Vec<u8>)>, DictBranch) {
            let result = builder::build_reverse_tree(entries, 64 * 1024).unwrap();
            let leaves = result
                .leaves
                .iter()
                .zip(result.branch.leaves.iter())
                .map(|(leaf, b)| (b.address.clone(), leaf.bytes.clone()))
                .collect();
            (leaves, result.branch)
        }

        fn probes(entries: &[ReverseEntry]) -> Vec<Vec<u8>> {
            let mut probes: Vec<Vec<u8>> = entries.iter().map(|e| e.key.clone()).collect();
            for e in entries.iter().step_by(7) {
                let mut absent = e.key.clone();
                absent.push(b'!');
                probes.push(absent);
            }
            probes.extend([b"a".to_vec(), b"zzz".to_vec(), b"subject-".to_vec()]);
            probes
        }

        /// A pack that cannot be read by range, opened while a leaf is being
        /// fenced, gives the fence pool back before the leaf is noted as
        /// fenced: the lookup still uses the leaf, and the pool given back
        /// does not keep it.
        #[test]
        fn a_leaf_fenced_as_the_pool_is_given_back_is_not_kept() {
            let cs = SegmentedContentStore::new(true);
            let (leaves, _) = built(entries());
            let kind = ContentKind::DictBlob {
                dict: DictKind::SubjectReverse,
            };
            let cid = cs.put_sync(kind, &leaves[0].1);
            let source = cs.get_local_ranged(&cid).unwrap().expect("ranged");
            let cache = LeafletCache::with_reservable_budget(16 << 20, Some(4 << 20));
            let leaf = cache
                .try_get_or_load_fenced_leaf(7, || {
                    cache.note_unrangeable_pack();
                    Ok(FencedLeaf::open(source, FENCE_SPACING)?.map(Arc::new))
                })
                .unwrap();
            assert!(leaf.is_some(), "fenced for this lookup");
            assert!(!cache.holds_fences(), "the pool was given back");
            cache.run_pending_tasks();
            assert_eq!(cache.fenced_leaf_memory().leaves, 0, "and keeps nothing");
        }

        /// Every fenced lookup finds exactly what a whole-leaf lookup finds,
        /// with fences closer than, at, and wider than the 4 KiB read unit,
        /// so entries spanning units are read across them.
        #[test]
        fn fenced_lookups_match_whole_leaf_lookups() {
            let entries = entries();
            let whole = build_reverse_reader(entries.clone());
            for spacing in [FENCE_SPACING, 512, 8192] {
                let cs = SegmentedContentStore::new(true);
                let (leaves, branch) = built(entries.clone());
                assert!(branch.leaves.len() > 3, "several leaves");
                let reader = fenced_reader(&cs, leaves, branch).with_fence_spacing(spacing);
                for key in probes(&entries) {
                    assert_eq!(
                        reader.reverse_lookup(&key).unwrap(),
                        whole.reverse_lookup(&key).unwrap(),
                        "{spacing} {}",
                        String::from_utf8_lossy(&key)
                    );
                }
                assert_eq!(cs.whole_reads.load(AtomicOrdering::SeqCst), 0, "{spacing}");
            }
        }

        /// A leaf's fences live in the cache under its CID: a reader of
        /// another snapshot sharing the leaf, over the same cache, uses them
        /// without reading the leaf whole again; one over another cache
        /// opens its own.
        #[test]
        fn fences_are_shared_through_the_cache_across_readers() {
            let entries = entries();
            let cs = SegmentedContentStore::new(true);
            let (leaves, branch) = built(entries.clone());
            let cache = fence_cache(16 << 20);
            let opening = |cs: &SegmentedContentStore| {
                cs.reads()
                    .iter()
                    .filter(|r| r.start == 0 && r.end - r.start > 4096)
                    .count()
            };
            let first = reader_over(&cs, leaves.clone(), branch.clone(), Arc::clone(&cache));
            for e in entries.iter().step_by(50) {
                assert_eq!(first.reverse_lookup(&e.key).unwrap(), Some(e.id));
            }
            let opened = opening(&cs);
            assert_eq!(opened, branch.leaves.len(), "each leaf opened once");
            drop(first);
            let next = reader_over(&cs, leaves.clone(), branch.clone(), Arc::clone(&cache));
            for e in entries.iter().step_by(50) {
                assert_eq!(next.reverse_lookup(&e.key).unwrap(), Some(e.id));
            }
            assert_eq!(opening(&cs), opened, "fences found in the cache");
            assert_eq!(
                cache.dictionary_allowance().stats().rent_counted,
                0,
                "reverse reads run no rent clock"
            );
            let elsewhere = reader_over(&cs, leaves, branch, fence_cache(16 << 20));
            elsewhere.reverse_lookup(&entries[0].key).unwrap();
            assert_eq!(opening(&cs), opened + 1);
        }

        /// Fences live in a pool of their own: filling the leaflet pool past
        /// its budget leaves them in place, so no leaf is read whole again.
        #[test]
        fn fences_outlast_pressure_on_the_leaflet_pool() {
            let entries = entries();
            let cs = SegmentedContentStore::new(true);
            let (leaves, branch) = built(entries.clone());
            let cache = fence_cache(2 << 20);
            let reader = reader_over(&cs, leaves, branch, Arc::clone(&cache));
            let opening = |cs: &SegmentedContentStore| {
                cs.reads()
                    .iter()
                    .filter(|r| r.start == 0 && r.end - r.start > 4096)
                    .count()
            };
            for e in entries.iter().step_by(50) {
                assert_eq!(reader.reverse_lookup(&e.key).unwrap(), Some(e.id));
            }
            let opened = opening(&cs);
            for i in 0..200u128 {
                cache.insert_dict_leaf(i, Arc::from(vec![0u8; 64 * 1024]));
                cache.get_dict_leaf(i);
            }
            cache.run_pending_tasks();
            assert!(cache.weighted_size_bytes() <= 2 * 1024 * 1024);
            for e in entries.iter().step_by(50) {
                assert_eq!(reader.reverse_lookup(&e.key).unwrap(), Some(e.id));
            }
            assert_eq!(opening(&cs), opened, "no leaf fenced again");
        }

        /// A fence index the pool lets go is rebuilt on next use, by reading
        /// its leaf whole once more, with the same answers.
        #[test]
        fn an_evicted_fence_index_is_rebuilt() {
            let entries = entries();
            let cs = SegmentedContentStore::new(true);
            let (leaves, branch) = built(entries.clone());
            let leaf_count = branch.leaves.len();
            let cache = fence_cache(16 << 20);
            let reader = reader_over(&cs, leaves, branch, Arc::clone(&cache));
            let opening = |cs: &SegmentedContentStore| {
                cs.reads()
                    .iter()
                    .filter(|r| r.start == 0 && r.end - r.start > 4096)
                    .count()
            };
            for e in entries.iter().step_by(50) {
                assert_eq!(reader.reverse_lookup(&e.key).unwrap(), Some(e.id));
            }
            assert_eq!(opening(&cs), leaf_count);
            cache.invalidate_all();
            for e in entries.iter().step_by(50) {
                assert_eq!(reader.reverse_lookup(&e.key).unwrap(), Some(e.id));
            }
            assert_eq!(opening(&cs), 2 * leaf_count, "each leaf fenced again");
            assert_eq!(cs.whole_reads.load(AtomicOrdering::SeqCst), 0);
        }

        /// A fenced leaf heavier than the fence pool takes is never cached
        /// there charged at less than it holds: its leaf is read whole, once,
        /// through the leaflet pool.
        #[test]
        fn a_leaf_too_heavy_for_the_fence_pool_is_read_whole() {
            let entries = entries();
            let cs = SegmentedContentStore::new(true);
            let (leaves, branch) = built(entries.clone());
            let leaf_count = branch.leaves.len();
            // A 128 KiB budget: a 4 KiB fence pool, taking leaves up to 2 KiB,
            // short of a test leaf's fences and 1 KiB source charge.
            let cache = fence_cache(128 * 1024);
            let reader =
                reader_over(&cs, leaves, branch, Arc::clone(&cache)).with_fence_spacing(256);
            for _ in 0..3 {
                for e in entries.iter().step_by(25) {
                    assert_eq!(reader.reverse_lookup(&e.key).unwrap(), Some(e.id));
                }
            }
            let memory = cache.fenced_leaf_memory();
            assert_eq!((memory.fence_bytes, memory.source_charge), (0, 0));
            let opened = cs
                .reads()
                .iter()
                .filter(|r| r.start == 0 && r.end - r.start > 4096)
                .count();
            assert_eq!(opened, leaf_count, "each leaf fenced once, then read whole");
        }

        /// The cache weighs a fenced leaf with its fences: closer fences
        /// weigh more, by exactly what their index holds.
        #[cfg(not(target_arch = "wasm32"))]
        #[test]
        fn fences_are_weighed_in_the_cache() {
            let entries: Vec<ReverseEntry> = entries().into_iter().take(2_000).collect();
            let blob = crate::dict::reverse_leaf::encode_reverse_leaf(&entries);
            let leaf = ReverseLeaf::from_bytes(&blob).unwrap();
            let weigh = |spacing: u64| {
                let branch = builder::build_reverse_tree(entries.clone(), usize::MAX)
                    .unwrap()
                    .branch;
                let address = branch.leaves[0].address.clone();
                let cs = SegmentedContentStore::new(true);
                let cache = fence_cache(16 << 20);
                let reader = reader_over(
                    &cs,
                    vec![(address, blob.clone())],
                    branch,
                    Arc::clone(&cache),
                )
                .with_fence_spacing(spacing);
                reader.reverse_lookup(&entries[7].key).unwrap();
                let memory = cache.fenced_leaf_memory();
                assert_eq!(memory.leaves, 1);
                assert_eq!(
                    memory.fence_bytes,
                    leaf.fences(spacing as usize).heap_bytes() as u64
                );
                assert_eq!(memory.source_charge, 1024, "the test source's charge");
                cache
                    .occupancy()
                    .into_iter()
                    .find(|(kind, ..)| *kind == "fenced-leaf")
                    .map(|(_, _, bytes)| bytes)
                    .expect("a fenced leaf cached")
            };
            let (wide, close) = (weigh(4096), weigh(256));
            let (wide_index, close_index) = (
                leaf.fences(4096).heap_bytes() as u64,
                leaf.fences(256).heap_bytes() as u64,
            );
            assert!(close_index > 8 * wide_index);
            assert!(wide > wide_index + 1024, "fences and source handle: {wide}");
            assert_eq!(close - wide, close_index - wide_index);
        }

        /// A leaf is read whole once, to fence it; after that a scan reads
        /// only its key's stretch, a unit or two.
        #[test]
        fn a_fenced_scan_reads_one_stretch_after_opening() {
            let entries = entries();
            let cs = SegmentedContentStore::new(true);
            let (leaves, branch) = built(entries.clone());
            let reader = fenced_reader(&cs, leaves, branch);
            let first = &entries[100].key;
            assert_eq!(reader.reverse_lookup(first).unwrap(), Some(entries[100].id));
            let opened = cs.reads();
            let len = opened[0].end - opened[0].start;
            assert_eq!(
                opened[0].start, 0,
                "opening reads the leaf whole: {opened:?}"
            );
            assert!(len > 8 * 4096, "a leaf of many units");
            assert!(
                opened.len() <= 3,
                "then a unit or two of its stretch: {opened:?}"
            );
            for e in entries.iter().take(400).skip(101) {
                assert_eq!(reader.reverse_lookup(&e.key).unwrap(), Some(e.id));
            }
            let later = &cs.reads()[1..];
            assert!(later
                .iter()
                .all(|r| r.start % 4096 == 0 && r.end - r.start == 4096));
        }

        /// A leaf whose entries do not lie one after another is read whole,
        /// with the same answers.
        #[test]
        fn a_leaf_out_of_layout_is_read_whole() {
            let entries: Vec<ReverseEntry> = entries().into_iter().take(500).collect();
            let blob = crate::dict::reverse_leaf::encode_reverse_leaf(&entries);
            let mut moved = blob.clone();
            // Point entry 0 at entry 1's bytes and entry 1 at entry 0's:
            // the table is still sorted by key only if the entries swap
            // too, so swap the keys' data and their offsets together.
            let leaf = ReverseLeaf::from_bytes(&blob).unwrap();
            assert!(leaf.is_sequential());
            let (a, b) = (8usize, 12usize);
            let (oa, ob) = (
                u32::from_le_bytes(blob[a..a + 4].try_into().unwrap()),
                u32::from_le_bytes(blob[b..b + 4].try_into().unwrap()),
            );
            let data = 8 + entries.len() * 4;
            let e0 = 12 + entries[0].key.len();
            let e1 = 12 + entries[1].key.len();
            let mut swapped = blob[data + ob as usize..data + ob as usize + e1].to_vec();
            swapped.extend_from_slice(&blob[data + oa as usize..data + oa as usize + e0]);
            moved[data..data + e0 + e1].copy_from_slice(&swapped);
            moved[a..a + 4].copy_from_slice(&(e1 as u32).to_le_bytes());
            moved[b..b + 4].copy_from_slice(&0u32.to_le_bytes());
            let moved_leaf = ReverseLeaf::from_bytes(&moved).unwrap();
            assert!(!moved_leaf.is_sequential());
            for e in &entries {
                assert_eq!(moved_leaf.lookup(&e.key), Some(e.id));
            }
            let branch = builder::build_reverse_tree(entries.clone(), usize::MAX)
                .unwrap()
                .branch;
            assert_eq!(branch.leaves.len(), 1);
            let cs = SegmentedContentStore::new(true);
            let address = branch.leaves[0].address.clone();
            let reader = fenced_reader(&cs, vec![(address, moved)], branch);
            for e in &entries {
                assert_eq!(reader.reverse_lookup(&e.key).unwrap(), Some(e.id));
            }
            assert_eq!(cs.whole_reads.load(AtomicOrdering::SeqCst), 1, "read whole");
        }

        /// An entry whose length runs past its stretch, or past the leaf, is
        /// an error, never a read past the bytes.
        #[test]
        fn a_corrupt_entry_length_is_an_error() {
            let entries: Vec<ReverseEntry> = entries().into_iter().take(2_000).collect();
            let blob = crate::dict::reverse_leaf::encode_reverse_leaf(&entries);
            let leaf = ReverseLeaf::from_bytes(&blob).unwrap();
            // Fences at the 4 KiB read unit, and closer, so a stretch ends
            // inside a unit.
            for spacing in [4096usize, 512] {
                let fences = leaf.fences(spacing);
                let at = fences.offset(1) as usize;
                let mut bad = blob.clone();
                bad[at..at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
                // The last entry before fence 3 runs past its stretch but not
                // past the leaf.
                let last = fences.index(3) as usize - 1;
                let last_at = fences.offset(3) as usize - (12 + entries[last].key.len());
                let overrun = entries[last].key.len() as u32 + 16;
                bad[last_at..last_at + 4].copy_from_slice(&overrun.to_le_bytes());
                let cs = SegmentedContentStore::new(true);
                let cid = cs.put_sync(
                    ContentKind::DictBlob {
                        dict: DictKind::SubjectReverse,
                    },
                    &bad,
                );
                let source = cs.get_local_ranged(&cid).unwrap().unwrap();
                let cache = fence_cache(4 << 20);
                let fenced = FencedLeaf {
                    source,
                    fences,
                    entry_count: leaf.entry_count(),
                    len: blob.len() as u64,
                };
                // The corrupted entry's own key: every lookup must read it.
                let key = &entries[fenced.fences.index(1) as usize].key;
                let err = fenced.lookup(key, &cache).unwrap_err();
                assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{spacing}");
                let err = fenced.lookup(&entries[last].key, &cache).unwrap_err();
                assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{spacing}");
                let fine = &entries[fenced.fences.index(3) as usize].key;
                assert!(fenced.lookup(fine, &cache).unwrap().is_some(), "{spacing}");
            }
        }
    }
}
