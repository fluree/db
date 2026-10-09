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
use super::reverse_leaf::ReverseLeaf;

use crate::read::leaflet_cache::LeafletCache;
use fluree_db_core::{ContentBytes, ContentId, ContentStore};

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
        self.global_cache = cache;
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

        let lookup_started = Instant::now();
        let find_leaf_started = Instant::now();
        let leaf_idx = match self.branch.find_leaf(key) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        let find_leaf_ms = find_leaf_started.elapsed().as_millis() as u64;

        let address = &self.branch.leaves[leaf_idx].address;
        let load_leaf_started = Instant::now();
        let leaf_data = self.load_leaf(address)?;
        let load_leaf_ms = load_leaf_started.elapsed().as_millis() as u64;
        let decode_started = Instant::now();
        let leaf = ReverseLeaf::from_bytes(&leaf_data)?;
        let decode_leaf_ms = decode_started.elapsed().as_millis() as u64;
        let leaf_lookup_started = Instant::now();
        let result = leaf.lookup(key);
        let lookup_leaf_ms = leaf_lookup_started.elapsed().as_millis() as u64;
        let total_ms = lookup_started.elapsed().as_millis() as u64;

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

        let lookup_started = Instant::now();
        let key_refs: Vec<&[u8]> = keys.into_iter().collect();
        if key_refs.is_empty() {
            return Ok(Vec::new());
        }

        let find_leaf_started = Instant::now();
        let mut results = vec![None; key_refs.len()];
        let mut key_indices_by_leaf = vec![Vec::<usize>::new(); self.branch.leaves.len()];

        for (idx, key) in key_refs.iter().enumerate() {
            if let Some(leaf_idx) = self.branch.find_leaf(key) {
                key_indices_by_leaf[leaf_idx].push(idx);
            }
        }
        let find_leaf_ms = find_leaf_started.elapsed().as_millis() as u64;

        let load_and_decode_started = Instant::now();
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
        let load_and_decode_ms = load_and_decode_started.elapsed().as_millis() as u64;
        let total_ms = lookup_started.elapsed().as_millis() as u64;

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

        // Find the first leaf that might contain start_key.
        let start_leaf = match self.branch.find_leaf(start_key) {
            Some(idx) => idx,
            None => {
                // start_key is before the first leaf or after the last.
                // If the first leaf's first_key >= start_key, it might have matches.
                if self.branch.leaves[0].first_key.as_slice() >= start_key {
                    0
                } else {
                    return Ok(Vec::new());
                }
            }
        };

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

        let load_started = Instant::now();
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
            let elapsed_ms = load_started.elapsed().as_millis() as u64;
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
}
