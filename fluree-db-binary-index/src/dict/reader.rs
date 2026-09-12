//! Demand-loading read interface for dictionary trees.
//!
//! A `DictTreeReader` holds a decoded branch manifest and resolves lookups
//! by loading the appropriate leaf on demand. Leaf data is read from local
//! files or provided in memory.
//!
//! When a shared `LeafletCache` is provided, `LocalFiles` lookups go through
//! the global LRU cache (respecting the customer's memory budget). Without
//! a cache, leaves are read directly from disk on each lookup.

use fluree_db_core::clock::Instant;
use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::branch::DictBranch;
use super::reverse_leaf::ReverseLeaf;

use crate::read::leaflet_cache::LeafletCache;
use fluree_db_core::{ContentId, ContentStore};

/// Leaf data source for demand-loading.
#[derive(Debug)]
pub enum LeafSource {
    /// Read leaves from local files. Maps CAS address → file path.
    LocalFiles(HashMap<String, PathBuf>),
    /// Fetch leaves from a CAS content store on demand (remote storages), with optional
    /// local file fallbacks for any leaves that are already locally resolvable.
    ///
    /// This avoids pre-downloading entire dictionaries during store construction
    /// (critical for Lambda + S3 cold starts). Leaf bytes are cached in `LeafletCache`
    /// when configured.
    CasOnDemand {
        cs: Arc<dyn ContentStore>,
        local_files: HashMap<String, PathBuf>,
        remote_cids: HashMap<String, ContentId>,
    },
    /// Leaves are provided inline (for testing or small dictionaries).
    InMemory(HashMap<String, Arc<[u8]>>),
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
    /// Optional disk-backed artifact cache directory for whole remote dict leaves.
    /// Used in remote/object-store environments to avoid repeated full-blob fetches.
    disk_cache_dir: Option<PathBuf>,
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
            disk_cache_dir: None,
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
            disk_cache_dir: None,
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
            disk_cache_dir: None,
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
    /// Fetches and decodes the branch, resolves each leaf CID to a local
    /// file path or keeps it as a remote CID for on-demand fetching, then
    /// constructs the reader with an optional leaflet cache.
    pub async fn from_refs(
        cs: &Arc<dyn ContentStore>,
        refs: &crate::format::wire_helpers::DictTreeRefs,
        leaflet_cache: Option<&Arc<LeafletCache>>,
        disk_cache_dir: Option<&std::path::Path>,
    ) -> io::Result<Self> {
        Self::load_refs(cs, refs, leaflet_cache, disk_cache_dir, None).await
    }

    /// [`Self::from_refs`] for a reload: returns `prev` itself when it was
    /// built from the same branch cid, and otherwise builds a new reader that
    /// carries over `prev`'s local leaf paths instead of probing the
    /// filesystem for every leaf the two branches share.
    ///
    /// Both are sound because branches and leaves are immutable
    /// content-addressed blobs: the same cid is the same bytes, and a leaf
    /// referenced by the new root is live, so a path that resolved for the
    /// previous root still does.
    pub async fn from_refs_reusing(
        cs: &Arc<dyn ContentStore>,
        refs: &crate::format::wire_helpers::DictTreeRefs,
        leaflet_cache: Option<&Arc<LeafletCache>>,
        disk_cache_dir: Option<&std::path::Path>,
        prev: Option<&Arc<DictTreeReader>>,
    ) -> io::Result<Arc<Self>> {
        if let Some(prev) = prev {
            let same_branch = prev.branch_cid.as_ref() == Some(&refs.branch);
            let same_cache = match (&prev.global_cache, leaflet_cache) {
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                (None, None) => true,
                _ => false,
            };
            let same_dir = prev.disk_cache_dir.as_deref() == disk_cache_dir;
            if same_branch && same_cache && same_dir {
                return Ok(Arc::clone(prev));
            }
        }
        let reader = Self::load_refs(cs, refs, leaflet_cache, disk_cache_dir, prev).await?;
        Ok(Arc::new(reader))
    }

    async fn load_refs(
        cs: &Arc<dyn ContentStore>,
        refs: &crate::format::wire_helpers::DictTreeRefs,
        leaflet_cache: Option<&Arc<LeafletCache>>,
        disk_cache_dir: Option<&std::path::Path>,
        prev: Option<&Arc<DictTreeReader>>,
    ) -> io::Result<Self> {
        let branch_bytes = cs
            .get(&refs.branch)
            .await
            .map_err(|e| io::Error::other(format!("failed to load branch: {e}")))?;
        let branch = DictBranch::decode(&branch_bytes)?;

        let known_local: Option<&HashMap<String, PathBuf>> =
            prev.and_then(|p| match &p.leaf_source {
                LeafSource::LocalFiles(map) => Some(map),
                LeafSource::CasOnDemand { local_files, .. } => Some(local_files),
                LeafSource::InMemory(_) => None,
            });

        let mut local_files = HashMap::with_capacity(branch.leaves.len());
        let mut remote_cids = HashMap::new();

        for (cid, bl) in refs.leaves.iter().zip(branch.leaves.iter()) {
            if let Some(path) = known_local.and_then(|m| m.get(&bl.address)) {
                local_files.insert(bl.address.clone(), path.clone());
            } else if let Some(local_path) = cs.resolve_local_path(cid) {
                local_files.insert(bl.address.clone(), local_path);
            } else {
                remote_cids.insert(bl.address.clone(), cid.clone());
            }
        }

        let leaf_source = if remote_cids.is_empty() {
            LeafSource::LocalFiles(local_files)
        } else {
            LeafSource::CasOnDemand {
                cs: Arc::clone(cs),
                local_files,
                remote_cids,
            }
        };

        let mut reader = match leaflet_cache {
            Some(cache) => Self::with_cache(branch, leaf_source, Arc::clone(cache)),
            None => Self::new(branch, leaf_source),
        };
        reader.branch_cid = Some(refs.branch.clone());
        reader.disk_cache_dir = disk_cache_dir.map(std::path::Path::to_path_buf);
        Ok(reader)
    }

    /// Attach a global cache to this reader.
    pub fn set_cache(&mut self, cache: Option<Arc<LeafletCache>>) {
        self.global_cache = cache;
    }

    /// Attach or clear the disk-backed artifact cache directory.
    pub fn set_disk_cache_dir(&mut self, cache_dir: Option<PathBuf>) {
        self.disk_cache_dir = cache_dir;
    }

    /// The underlying branch manifest.
    pub fn branch(&self) -> &DictBranch {
        &self.branch
    }

    /// A short label for the configured leaf source.
    pub fn source_kind(&self) -> &'static str {
        match &self.leaf_source {
            LeafSource::LocalFiles(_) => "local_files",
            LeafSource::CasOnDemand { .. } => "cas_on_demand",
            LeafSource::InMemory(_) => "in_memory",
        }
    }

    /// Number of leaves in the decoded branch manifest.
    pub fn leaf_count(&self) -> usize {
        self.branch.leaves.len()
    }

    /// Number of locally resolvable leaves for this reader.
    pub fn local_file_count(&self) -> usize {
        match &self.leaf_source {
            LeafSource::LocalFiles(map) => map.len(),
            LeafSource::CasOnDemand { local_files, .. } => local_files.len(),
            LeafSource::InMemory(map) => map.len(),
        }
    }

    /// Number of remotely fetched leaves available to this reader.
    pub fn remote_cid_count(&self) -> usize {
        match &self.leaf_source {
            LeafSource::CasOnDemand { remote_cids, .. } => remote_cids.len(),
            _ => 0,
        }
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

    /// The remote CID for a leaf `address`, if this reader fetches that leaf
    /// from CAS on demand (i.e. it is not a local file). Returns `None` for
    /// local/in-memory leaves and for unknown addresses. Used to prewarm the
    /// disk artifact cache before a sync reverse lookup.
    pub fn remote_leaf_cid(&self, address: &str) -> Option<&ContentId> {
        match &self.leaf_source {
            LeafSource::CasOnDemand { remote_cids, .. } => remote_cids.get(address),
            _ => None,
        }
    }

    /// The disk-backed artifact cache directory this reader reads prewarmed
    /// leaves from, if configured. A prefetch must write to this same dir for
    /// `load_leaf` to find the bytes.
    pub fn disk_cache_dir(&self) -> Option<&std::path::Path> {
        self.disk_cache_dir.as_deref()
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
    /// (`resolve_cached_bytes`), surfacing a typed
    /// [`crate::read::need_fetch::NeedFetch`] miss (recorded in the store's
    /// miss register) for an async caller to fetch and retry. Kept free of
    /// `Instant::now()` timing stamps, which trap on wasm32.
    #[cfg(any(target_arch = "wasm32", feature = "residency"))]
    fn load_leaf_resident(
        &self,
        cs: &Arc<dyn ContentStore>,
        remote_cids: &HashMap<String, ContentId>,
        address: &str,
    ) -> io::Result<Arc<[u8]>> {
        use crate::read::need_fetch::{resident_or_need_fetch, FetchKind};
        let cid = remote_cids.get(address).ok_or_else(|| {
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
            cache.try_get_or_load_dict_leaf(cache_key, move || Ok(bytes))
        } else {
            resident_or_need_fetch(cs.as_ref(), cid, FetchKind::DictLeaf)
        }
    }

    /// Load leaf bytes from the configured source.
    ///
    /// For `LocalFiles` with a global cache: uses the cache (keyed by
    /// `xxh3_128(cas_address)`) to avoid repeated disk reads.
    /// Without a cache: reads directly from disk.
    ///
    /// Residency-mode stores (miss-register-bearing `CasOnDemand` sources)
    /// divert to [`Self::load_leaf_resident`] before any filesystem probe or
    /// timing stamp.
    fn load_leaf(&self, address: &str) -> io::Result<Arc<[u8]>> {
        #[cfg(any(target_arch = "wasm32", feature = "residency"))]
        if let LeafSource::CasOnDemand {
            cs, remote_cids, ..
        } = &self.leaf_source
        {
            if cs.miss_register().is_some() {
                return self.load_leaf_resident(cs, remote_cids, address);
            }
        }
        fn read_disk_cached_leaf(
            disk_cache_dir: Option<&PathBuf>,
            cid: &ContentId,
        ) -> io::Result<Option<Vec<u8>>> {
            let Some(cache_dir) = disk_cache_dir else {
                return Ok(None);
            };
            let cache_path = cache_dir.join(cid.to_string());
            match std::fs::read(&cache_path) {
                Ok(bytes) => Ok(Some(bytes)),
                Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(err) => Err(err),
            }
        }

        fn fetch_remote_leaf_bytes(
            cs: Arc<dyn ContentStore>,
            cid: ContentId,
            disk_cache_dir: Option<PathBuf>,
        ) -> io::Result<Vec<u8>> {
            // DictTreeReader is sync, but ContentStore::get is async. Bridge via
            // the shared `run_sync_on_runtime` helper, which uses
            // `block_in_place(handle.block_on)` on a multi-thread runtime (so a
            // replacement worker keeps driving the reactor while this thread
            // blocks) and a process-wide helper runtime when needed.
            //
            // The previous hand-rolled `thread::spawn` + outer-`Handle::block_on`
            // + `rx.recv()` re-injected the fetch onto the OUTER runtime with no
            // `block_in_place`: on a small (e.g. 2-worker) runtime every worker
            // could park in `recv()` with no thread left to drive the reactor,
            // so the fetch never completed — a hard wedge under query fan-out.
            let timeout = crate::read::binary_index_store::cas_sync_timeout();
            crate::read::binary_index_store::run_sync_on_runtime(async move {
                let fetch = async {
                    if let Some(cache_dir) = disk_cache_dir {
                        crate::read::artifact_cache::fetch_cached_bytes_cid(
                            cs.as_ref(),
                            &cid,
                            &cache_dir,
                        )
                        .await
                        .map_err(|e| io::Error::other(e.to_string()))
                    } else {
                        cs.get(&cid)
                            .await
                            .map_err(|e| io::Error::other(e.to_string()))
                    }
                };
                // Optional per-fetch ceiling (FLUREE_CAS_SYNC_TIMEOUT_MS): a
                // stalled dict-leaf fetch self-aborts instead of blocking.
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

        let load_started = Instant::now();
        let result = match &self.leaf_source {
            LeafSource::LocalFiles(map) => {
                let path = map.get(address).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("dict tree: no local file for leaf {address}"),
                    )
                })?;

                if let Some(cache) = &self.global_cache {
                    let cache_key = xxhash_rust::xxh3::xxh3_128(address.as_bytes());
                    if let Some(bytes) = cache.get_dict_leaf(cache_key) {
                        self.cache_hits.fetch_add(1, Ordering::Relaxed);
                        return Ok(bytes);
                    }
                    self.cache_misses.fetch_add(1, Ordering::Relaxed);
                    let path = path.clone();
                    let disk_reads = &self.disk_reads;
                    let local_file_reads = &self.local_file_reads;
                    cache.try_get_or_load_dict_leaf(cache_key, || {
                        disk_reads.fetch_add(1, Ordering::Relaxed);
                        local_file_reads.fetch_add(1, Ordering::Relaxed);
                        let bytes = std::fs::read(&path)?;
                        Ok(Arc::from(bytes.into_boxed_slice()))
                    })
                } else {
                    self.disk_reads.fetch_add(1, Ordering::Relaxed);
                    self.local_file_reads.fetch_add(1, Ordering::Relaxed);
                    let bytes = std::fs::read(path)?;
                    Ok(Arc::from(bytes.into_boxed_slice()))
                }
            }
            LeafSource::CasOnDemand {
                cs,
                local_files,
                remote_cids,
            } => {
                // Local file fast-path when available (e.g., file storage or mixed backends).
                if let Some(path) = local_files.get(address) {
                    if let Some(cache) = &self.global_cache {
                        let cache_key = xxhash_rust::xxh3::xxh3_128(address.as_bytes());
                        if let Some(bytes) = cache.get_dict_leaf(cache_key) {
                            self.cache_hits.fetch_add(1, Ordering::Relaxed);
                            return Ok(bytes);
                        }
                        self.cache_misses.fetch_add(1, Ordering::Relaxed);
                        let path = path.clone();
                        let disk_reads = &self.disk_reads;
                        let local_file_reads = &self.local_file_reads;
                        return cache.try_get_or_load_dict_leaf(cache_key, || {
                            disk_reads.fetch_add(1, Ordering::Relaxed);
                            local_file_reads.fetch_add(1, Ordering::Relaxed);
                            let bytes = std::fs::read(&path)?;
                            Ok(Arc::from(bytes.into_boxed_slice()))
                        });
                    }
                    self.disk_reads.fetch_add(1, Ordering::Relaxed);
                    self.local_file_reads.fetch_add(1, Ordering::Relaxed);
                    let bytes = std::fs::read(path)?;
                    return Ok(Arc::from(bytes.into_boxed_slice()));
                }

                let cid = remote_cids.get(address).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("dict tree: no CID mapping for leaf {address}"),
                    )
                })?;

                // Remote fetch path: cache bytes in LeafletCache (keyed by CAS address).
                if let Some(cache) = &self.global_cache {
                    let cache_key = xxhash_rust::xxh3::xxh3_128(address.as_bytes());
                    if let Some(bytes) = cache.get_dict_leaf(cache_key) {
                        self.cache_hits.fetch_add(1, Ordering::Relaxed);
                        return Ok(bytes);
                    }
                    self.cache_misses.fetch_add(1, Ordering::Relaxed);
                    let cs = Arc::clone(cs);
                    let cid = cid.clone();
                    let disk_reads = &self.disk_reads;
                    let remote_fetches = &self.remote_fetches;
                    let local_file_reads = &self.local_file_reads;
                    let address = address.to_owned();
                    let disk_cache_dir = self.disk_cache_dir.clone();
                    cache.try_get_or_load_dict_leaf(cache_key, || {
                        if let Some(bytes) = read_disk_cached_leaf(disk_cache_dir.as_ref(), &cid)? {
                            disk_reads.fetch_add(1, Ordering::Relaxed);
                            local_file_reads.fetch_add(1, Ordering::Relaxed);
                            return Ok(Arc::from(bytes.into_boxed_slice()));
                        }
                        tracing::debug!(
                            address,
                            %cid,
                            "dict tree: remote leaf fetch starting"
                        );
                        disk_reads.fetch_add(1, Ordering::Relaxed);
                        remote_fetches.fetch_add(1, Ordering::Relaxed);
                        let fetch_started = Instant::now();
                        let bytes = fetch_remote_leaf_bytes(cs, cid, disk_cache_dir)?;
                        tracing::debug!(
                            address,
                            bytes = bytes.len(),
                            elapsed_ms = fetch_started.elapsed().as_millis() as u64,
                            "dict tree: remote leaf fetch complete"
                        );
                        Ok(Arc::from(bytes.into_boxed_slice()))
                    })
                } else {
                    if let Some(bytes) = read_disk_cached_leaf(self.disk_cache_dir.as_ref(), cid)? {
                        self.disk_reads.fetch_add(1, Ordering::Relaxed);
                        self.local_file_reads.fetch_add(1, Ordering::Relaxed);
                        return Ok(Arc::from(bytes.into_boxed_slice()));
                    }
                    tracing::debug!(
                        address,
                        %cid,
                        "dict tree: remote leaf fetch starting"
                    );
                    self.disk_reads.fetch_add(1, Ordering::Relaxed);
                    self.remote_fetches.fetch_add(1, Ordering::Relaxed);
                    let fetch_started = Instant::now();
                    let bytes = fetch_remote_leaf_bytes(
                        Arc::clone(cs),
                        cid.clone(),
                        self.disk_cache_dir.clone(),
                    )?;
                    tracing::debug!(
                        address,
                        bytes = bytes.len(),
                        elapsed_ms = fetch_started.elapsed().as_millis() as u64,
                        "dict tree: remote leaf fetch complete"
                    );
                    Ok(Arc::from(bytes.into_boxed_slice()))
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
    /// `resolve_local_path`, counting how often a reload asks — the probe a
    /// reload is meant to skip for every leaf it already had a path for.
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
        async fn get(&self, id: &ContentId) -> fluree_db_core::Result<Vec<u8>> {
            let path = self.paths.lock().get(id).cloned().expect("known cid");
            Ok(std::fs::read(path).unwrap())
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
        fn resolve_local_path(&self, id: &ContentId) -> Option<PathBuf> {
            self.resolves.fetch_add(1, Ordering::Relaxed);
            self.paths.lock().get(id).cloned()
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

    /// A reload with the same branch cid is the same reader; a reload with a
    /// grown branch probes the filesystem only for the leaves it did not
    /// already have a path for, and still resolves through all of them.
    #[tokio::test]
    async fn a_reload_reuses_the_reader_or_its_leaf_paths() {
        let store = Arc::new(CountingFileStore::new());
        let cs: Arc<dyn ContentStore> = store.clone();

        let low = stored_leaves(&store, &["a", "b", "c", "d", "e", "f"], 0).await;
        let high = stored_leaves(&store, &["m", "n", "o", "p", "q", "r"], 100).await;
        assert!(low.len() >= 2, "fixture must span several leaves");

        let refs_v1 = stored_tree(&store, &low).await;
        let mut grown = low.clone();
        grown.extend(high.iter().cloned());
        let refs_v2 = stored_tree(&store, &grown).await;

        let v1 = DictTreeReader::from_refs_reusing(&cs, &refs_v1, None, None, None)
            .await
            .unwrap();
        assert_eq!(store.resolves.load(Ordering::Relaxed), low.len() as u64);
        assert_eq!(v1.reverse_lookup(b"c").unwrap(), Some(2));

        let same = DictTreeReader::from_refs_reusing(&cs, &refs_v1, None, None, Some(&v1))
            .await
            .unwrap();
        assert!(
            Arc::ptr_eq(&same, &v1),
            "an unchanged branch cid must hand back the previous reader"
        );
        assert_eq!(store.resolves.load(Ordering::Relaxed), low.len() as u64);

        let v2 = DictTreeReader::from_refs_reusing(&cs, &refs_v2, None, None, Some(&v1))
            .await
            .unwrap();
        assert!(!Arc::ptr_eq(&v2, &v1));
        assert_eq!(
            store.resolves.load(Ordering::Relaxed),
            (low.len() + high.len()) as u64,
            "only the new leaves may be probed"
        );
        assert_eq!(v2.local_file_count(), grown.len());
        assert_eq!(v2.reverse_lookup(b"c").unwrap(), Some(2));
        assert_eq!(v2.reverse_lookup(b"p").unwrap(), Some(103));

        let _ = std::fs::remove_dir_all(&store.dir);
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
