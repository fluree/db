//! Unified LRU cache for decoded leaflet regions and dictionary leaves.
//!
//! Uses one `moka::sync::SegmentedCache` (synchronous — `BinaryCursor` is
//! sync) with TinyLFU eviction. Every entry type (see `CacheKey`) shares one
//! pool so TinyLFU decides what stays based on actual access patterns rather
//! than fixed budget splits. The pool is split by key hash into shards; see
//! [`pool_segments`].
//!
//! History is **never cached** — it lives outside the leaflet entirely, in the
//! per-leaf history sidecar (`FHS1`, located via `LeafEntry.sidecar_cid`). It's
//! cold-path data decoded on demand for time-travel replay and discarded
//! afterwards, so a HEAD-only query never pays for it.
//!
//! Scans cache a leaflet's decoded base columns (`V3Batch`), keyed by the
//! content-addressed leaf and the leaflet index. Overlay merging and
//! time-travel replay run after the cache, so one entry serves every `t`.

use crate::format::leaf::DecodedLeafDirV3;
use crate::read::types::OverlayOp;
#[cfg(target_arch = "wasm32")]
use crate::wasm_compat::memmap2;
use fluree_db_core::{Flake, Sid, StatsView};
// moka's eviction clock reads std::time::Instant at cache construction, which
// aborts on wasm32-unknown-unknown; shadow-import core's clock-free LRU
// stand-in there (same API subset, exact-LRU instead of TinyLFU).
#[cfg(target_arch = "wasm32")]
use fluree_db_core::wasm_cache::Cache;
#[cfg(not(target_arch = "wasm32"))]
use moka::sync::Cache;
#[cfg(not(target_arch = "wasm32"))]
type Pool<K, V> = moka::sync::SegmentedCache<K, V>;
#[cfg(target_arch = "wasm32")]
type Pool<K, V> = Cache<K, V>;
use std::io;
use std::sync::Arc;

/// Convert a shared loader error back into `io::Error`, preserving a typed
/// `NeedFetch` payload when one is present.
///
/// Loader closures run inside the cache's `try_get_with`, which shares the
/// error as `Arc<io::Error>`; a plain `to_string()` re-wrap severs the typed
/// residency miss. Residency invariant: read paths should resolve residency
/// BEFORE entering a loader closure where possible (the dict-leaf path
/// does); this rewrap is the backstop for paths that cannot — e.g.
/// `open_leaf_dir`'s header fetch runs inside the dir-load closure. The
/// store-level miss register is unaffected either way: the miss was
/// recorded inside the closure before the error propagated.
fn arc_io_error(arc_err: std::sync::Arc<io::Error>) -> io::Error {
    if let Some(nf) = fluree_db_core::storage::residency::NeedFetch::from_io_error(&arc_err) {
        return nf.clone().into_io_error();
    }
    io::Error::new(arc_err.kind(), arc_err.to_string())
}

// ============================================================================
// Cache key
// ============================================================================

/// Unified cache key. The enum discriminant keeps entry types apart even when
/// their underlying identifiers match.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum CacheKey {
    /// Key = xxh3_128(CAS address). Content-addressed → immutable.
    DictLeaf(u128),
    /// Decoded leaf directory (header+dir only, no payload). Key =
    /// xxh3_128(leaf CID bytes) — content-addressed → immutable,
    /// no epoch/time dimension needed.
    LeafDir(u128),
    /// Shared leaf-file memory mapping. Key = xxh3_128(leaf CID bytes) —
    /// content-addressed → immutable. Caching the mapping saves the
    /// open+mmap+munmap syscall cycle every cursor otherwise pays per leaf
    /// access (the dominant fixed cost of an indexed point lookup).
    LeafMmap(u128),
    /// BM25 posting leaflet. Key = xxh3_128(CAS CID bytes).
    /// Content-addressed → immutable, no epoch/time dimension needed.
    Bm25Leaflet(u128),
    /// Parsed vector shard. Key = xxh3_128(shard identity bytes).
    /// For CAS-loaded stores: CID bytes. For local builds: CAS address
    /// string from the manifest. Immutable once written.
    VectorShard(u128),
    /// Cached `ledger-info` response blob (JSON bytes).
    ///
    /// Key = xxh3_128 of a canonical ledger-info cache key string.
    LedgerInfo(u128),
    /// V3 (FLI3) decoded column batch, one per leaflet whatever its columns.
    /// Content-addressed via `leaf_id` (derived from leaf CID) — immutable,
    /// self-invalidating on rewrite. `leaflet_idx` selects which leaflet
    /// within the leaf.
    V3Batch(V3LeafletKey),
    /// Translated overlay ops for ONE immutable novelty segment.
    /// Key = `xxh3_128(store_id, seg_id, index)` — see
    /// [`LeafletCache::segment_ops_key`]. The store-instance id makes the key
    /// sound (a dict rebuild builds a new store → new id); see the segment-aware
    /// overlay design doc.
    SegmentOps(u128),
}

/// Cache key for a V3 decoded `ColumnBatch`.
///
/// Base columns are immutable (content-addressed leaf CID), so no `to_t`/`epoch`
/// dimension is needed. Different scans with different overlay state or time bounds
/// skip the cache rather than storing separate entries — overlay merge and time-travel
/// replay are applied downstream from the cached base columns.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct V3BatchCacheKey {
    /// `xxh3_128(leaf_cid.to_bytes())` — content-addressed, self-invalidating.
    pub leaf_id: u128,
    /// Leaflet slot within the leaf (0..leaflet_count).
    pub leaflet_idx: u32,
    /// `ColumnSet` bitmask of the columns the caller needs. A leaflet has one
    /// cache entry holding every column decoded for it so far; a request it
    /// covers is served projected, and a wider one decodes only the missing
    /// columns and widens the entry.
    pub columns: u16,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct V3LeafletKey {
    leaf_id: u128,
    leaflet_idx: u32,
}

impl From<&V3BatchCacheKey> for V3LeafletKey {
    fn from(key: &V3BatchCacheKey) -> Self {
        Self {
            leaf_id: key.leaf_id,
            leaflet_idx: key.leaflet_idx,
        }
    }
}

/// A leaflet's decoded columns and which columns they are (`AbsentDefault`
/// also encodes an all-sentinel column, so the batch alone cannot say).
#[derive(Clone)]
struct CachedV3Batch {
    columns: super::column_types::ColumnSet,
    batch: super::column_types::ColumnBatch,
}

impl CachedV3Batch {
    /// `want`'s columns, which this entry must cover.
    fn into_served(self, want: super::column_types::ColumnSet) -> super::column_types::ColumnBatch {
        debug_assert!(self.columns.covers(want));
        if self.columns == want {
            self.batch
        } else {
            self.batch.project_to(want)
        }
    }

    /// `self` widened by a decode of `columns` from the same leaflet.
    fn widen(
        &self,
        decoded: &super::column_types::ColumnBatch,
        columns: super::column_types::ColumnSet,
    ) -> Self {
        Self {
            columns: self.columns.union(columns),
            batch: self.batch.with_columns_from(decoded, columns),
        }
    }
}

// ============================================================================
// Cached value types
// ============================================================================

/// Translated overlay ops for one immutable novelty segment, cached in the
/// shared pool. `ops` are sorted by the index order but **not** lifecycle-
/// resolved or `to_t`-filtered; `untranslated` are the `@vector`/unsupported
/// flakes; `ephemeral_preds` maps novelty-only predicate Sids to their stable
/// (dictionary-backed) ids. Produced by the segment-aware overlay path.
#[derive(Clone)]
pub struct CachedOverlaySegment {
    pub ops: Arc<[OverlayOp]>,
    pub untranslated: Arc<[Flake]>,
    pub ephemeral_preds: Arc<std::collections::HashMap<Sid, u32>>,
}

impl CachedOverlaySegment {
    /// Approximate heap size, for the moka byte weigher. Counts the `Arc<[T]>`
    /// payloads, the flake heaps, and — the part a naive `len * size_of` misses
    /// — the `HashMap` table capacity plus each predicate `Sid`'s `Arc<str>`
    /// name heap, which dominates for novelty-only-predicate-heavy overlays.
    pub fn byte_size(&self) -> usize {
        // Arc<[T]> control block (strong+weak counts + slice len), x3 fields.
        const ARC_SLICE_OVERHEAD: usize = 3 * 2 * std::mem::size_of::<usize>();
        let ops = self.ops.len() * std::mem::size_of::<OverlayOp>();
        let untranslated: usize = self.untranslated.iter().map(Flake::size_bytes).sum();
        let eph = self.ephemeral_preds.capacity() * std::mem::size_of::<(Sid, u32)>()
            + self
                .ephemeral_preds
                .keys()
                .map(|s| s.name_str().len())
                .sum::<usize>();
        std::mem::size_of::<Self>() + ARC_SLICE_OVERHEAD + ops + untranslated + eph
    }
}

// ============================================================================
// Unified cached value
// ============================================================================

/// Unified value stored in the single moka cache.
#[derive(Clone)]
enum CachedEntry {
    DictLeaf(Arc<[u8]>),
    LeafDir(Arc<DecodedLeafDirV3>),
    LeafMmap(Arc<memmap2::Mmap>),
    Bm25Leaflet(Arc<[u8]>),
    VectorShard(Arc<crate::arena::vector::VectorShard>),
    LedgerInfo(Arc<[u8]>),
    /// V3 decoded column batch (base columns, no overlay/replay applied).
    V3Batch(CachedV3Batch),
    /// Translated overlay ops for one novelty segment.
    SegmentOps(Arc<CachedOverlaySegment>),
}

impl CachedEntry {
    /// Approximate byte size for the moka weigher.
    fn byte_size(&self) -> usize {
        match self {
            CachedEntry::DictLeaf(bytes) => bytes.len(),
            CachedEntry::LeafDir(dir) => dir.byte_size(),
            // A mapping is file-backed (reclaimable page cache), not heap:
            // weigh it far below its length so it doesn't crowd out decoded
            // batches, but keep it length-proportional so total mapped
            // address space stays bounded by the cache budget (× 64).
            CachedEntry::LeafMmap(mmap) => (mmap.len() / 64).max(16 * 1024),
            CachedEntry::Bm25Leaflet(bytes) => bytes.len(),
            CachedEntry::VectorShard(shard) => {
                // Use capacity() for conservative accounting — correct even if
                // the parser's allocation strategy changes over time.
                std::mem::size_of::<crate::arena::vector::VectorShard>()
                    + shard.values.capacity() * std::mem::size_of::<f32>()
            }
            CachedEntry::LedgerInfo(bytes) => bytes.len(),
            CachedEntry::V3Batch(entry) => entry.batch.byte_size(),
            CachedEntry::SegmentOps(seg) => seg.byte_size(),
        }
    }
}

// ============================================================================
// LeafletCache
// ============================================================================

/// Unified LRU cache for decoded leaflet regions and dictionary tree leaves,
/// backed by one moka TinyLFU pool split into key-hashed shards.
///
/// All entry types share one memory budget — TinyLFU decides what stays
/// based on actual access frequency/recency rather than fixed splits.
pub struct LeafletCache {
    inner: Pool<CacheKey, CachedEntry>,
    /// Query planner `StatsView`s, in a pool of their own. See
    /// [`stats_view_pool`].
    stats_views: Cache<u128, Arc<StatsView>>,
}

impl std::fmt::Debug for LeafletCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeafletCache")
            .field("entries", &self.entry_count())
            .field("bytes", &self.weighted_size_bytes())
            .field("stats_views", &self.stats_views.entry_count())
            .field("stats_view_bytes", &self.stats_views.weighted_size())
            .finish()
    }
}

/// Share of the leaflet budget given to the stats view pool.
const STATS_VIEW_BUDGET_DIVISOR: u64 = 16;

/// The pool for query planner `StatsView`s.
///
/// Separate from leaflets so scan pressure cannot evict a view, and LRU rather
/// than TinyLFU because views are keyed by overlay epoch: the newest is the one
/// queries want, while frequency admission favors the epoch it replaced.
///
/// Weight is capped at half the budget because moka never caches an entry
/// heavier than the whole pool, which would make an oversized view rebuild on
/// every query. The cap means the real bound is two full-size views, not the
/// budget: on a ledger with millions of classes that is significant memory.
fn stats_view_pool(leaflet_budget_bytes: u64) -> Cache<u128, Arc<StatsView>> {
    let budget = (leaflet_budget_bytes / STATS_VIEW_BUDGET_DIVISOR).max(1);
    let max_weight = (budget / 2).clamp(1, u64::from(u32::MAX)) as usize;
    let builder = Cache::builder()
        .weigher(move |_key: &u128, view: &Arc<StatsView>| view.byte_size().min(max_weight) as u32)
        .max_capacity(budget);
    // The wasm32 stand-in is exact-LRU already and has no policy knob.
    #[cfg(not(target_arch = "wasm32"))]
    let builder = builder.eviction_policy(moka::policy::EvictionPolicy::lru());
    builder.build()
}

/// Smallest budget a leaflet pool shard is given.
///
/// moka never caches an entry heavier than its shard, and a vector shard
/// alone can weigh tens of megabytes, so a shard must stay far larger than
/// any single entry.
#[cfg(not(target_arch = "wasm32"))]
const MIN_SEGMENT_BYTES: u64 = 256 * 1024 * 1024;

/// Number of shards for a leaflet pool of `max_bytes` on `cores` cores.
///
/// Each moka cache records every hit in one bounded channel that all threads
/// share, so at high core counts hits spend their time contending on it. A
/// shard has its own channel, so one shard per core spreads that contention
/// out. Shards hold `max_bytes / n` each, so their count is capped to keep
/// every shard at least [`MIN_SEGMENT_BYTES`]; budgets under twice that stay
/// one pool. Always a power of two, as moka rounds up to one.
///
/// Each shard enforces its share of the budget when its housekeeping runs,
/// which moka does once a shard has queued 64 writes or a short interval has
/// passed. Until then fresh inserts sit above the budget, so the transient
/// overshoot grows with the shard count: up to about `64 × n` entries rather
/// than 64.
#[cfg(not(target_arch = "wasm32"))]
fn pool_segments(max_bytes: u64, cores: usize) -> usize {
    let by_budget = (max_bytes / MIN_SEGMENT_BYTES).max(1);
    let by_budget = 1usize << by_budget.ilog2().min(usize::BITS - 2);
    cores.max(1).next_power_of_two().min(by_budget)
}

/// Run a moka single-flight call (`get_with`/`try_get_with`) under a Tokio
/// blocking region when on a multi-thread runtime.
///
/// moka single-flight is **synchronous**: when N callers contend on the same
/// key, one runs the initializer and the other N-1 **block the calling thread**
/// waiting for it — with no `block_in_place` of their own. On a small (e.g.
/// AWS Lambda 2-worker) runtime those waiters park every worker, so nothing is
/// left to drive the IO reactor; the leader's `block_on` CAS fetch then never
/// completes and the moka key stays locked forever — a hard, permanent wedge
/// (one envelope's sub-queries, contending the same cold dict/column key, burn
/// the container). Running the wait inside `block_in_place` converts the
/// waiting worker so tokio promotes a replacement that keeps the reactor live.
///
/// Callers MUST peek the cache first (non-blocking `get`) and only enter this
/// region on a miss, so plain cache hits never pay the conversion cost (every
/// scan flows through these methods).
#[cfg(not(target_arch = "wasm32"))]
fn in_blocking_region<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor()) {
        Ok(tokio::runtime::RuntimeFlavor::MultiThread) => tokio::task::block_in_place(f),
        _ => f(),
    }
}

/// wasm32 is single-threaded: there is no worker to convert, run inline.
#[cfg(target_arch = "wasm32")]
fn in_blocking_region<T>(f: impl FnOnce() -> T) -> T {
    f()
}

impl LeafletCache {
    /// Create a new cache with the given maximum byte budget.
    ///
    /// One TinyLFU pool and budget for every leaflet-derived entry type,
    /// sharded per [`pool_segments`]. Query planner stats views get a separate
    /// LRU pool sized from the same budget; see [`stats_view_pool`].
    pub fn with_max_bytes(max_bytes: u64) -> Self {
        let cores = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        Self::with_max_bytes_on(max_bytes, cores)
    }

    fn with_max_bytes_on(max_bytes: u64, cores: usize) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        let builder = Pool::builder(pool_segments(max_bytes, cores));
        // The wasm32 stand-in is single-threaded: nothing to shard.
        #[cfg(target_arch = "wasm32")]
        let builder = {
            let _ = cores;
            Pool::builder()
        };
        let inner = builder
            .weigher(|_key: &CacheKey, val: &CachedEntry| {
                val.byte_size().min(u32::MAX as usize) as u32
            })
            .max_capacity(max_bytes)
            .build();

        Self {
            inner,
            stats_views: stats_view_pool(max_bytes),
        }
    }

    /// Create a new cache with the given maximum megabyte budget.
    pub fn with_max_mb(mb: u64) -> Self {
        Self::with_max_bytes(mb.saturating_mul(1024 * 1024))
    }

    /// Approximate total size of entries in bytes (moka weighted size).
    pub fn weighted_size_bytes(&self) -> u64 {
        self.inner.weighted_size()
    }

    /// Number of cached entries across all entry types.
    pub fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }

    /// Number of shards the pool is split into; see [`pool_segments`].
    pub fn shard_count(&self) -> usize {
        #[cfg(not(target_arch = "wasm32"))]
        return self.inner.policy().num_segments();
        #[cfg(target_arch = "wasm32")]
        1
    }

    // ========================================================================
    // Dict tree leaf cache
    // ========================================================================

    /// Check if a dict tree leaf is cached (read-only, no insertion).
    pub fn get_dict_leaf(&self, key: u128) -> Option<Arc<[u8]>> {
        match self.inner.get(&CacheKey::DictLeaf(key)) {
            Some(CachedEntry::DictLeaf(bytes)) => Some(bytes),
            _ => None,
        }
    }

    /// Get or load a dict tree leaf with single-flight and error propagation.
    ///
    /// Uses `try_get_with` so that only one thread loads a given leaf;
    /// concurrent callers block on the same initializer. If the load
    /// fails, nothing is cached and the error propagates.
    ///
    /// Key should be `xxh3_128(cas_address.as_bytes())`.
    pub fn try_get_or_load_dict_leaf<F>(&self, key: u128, load_fn: F) -> io::Result<Arc<[u8]>>
    where
        F: FnOnce() -> io::Result<Arc<[u8]>>,
    {
        // Fast path: a plain hit must not pay the block_in_place cost.
        if let Some(bytes) = self.get_dict_leaf(key) {
            return Ok(bytes);
        }
        // Miss: run the single-flight wait/init in a blocking region so a
        // waiter promotes a replacement worker (see in_blocking_region).
        let result = in_blocking_region(|| {
            self.inner.try_get_with(CacheKey::DictLeaf(key), || {
                load_fn().map(CachedEntry::DictLeaf)
            })
        });
        match result {
            Ok(CachedEntry::DictLeaf(bytes)) => Ok(bytes),
            Ok(_) => unreachable!("DictLeaf key always maps to DictLeaf entry"),
            Err(arc_err) => Err(arc_io_error(arc_err)),
        }
    }

    /// Warm-on-write: seed a reverse-dict leaf the *writer* already holds.
    ///
    /// Key must be `cid_cache_key(cas_address_string)` — the reader keys dict
    /// leaves on the CAS address string (`cid.to_string()`), not the CID bytes.
    /// Value is the raw on-disk leaf bytes; the reader decodes on access.
    pub fn insert_dict_leaf(&self, key: u128, bytes: Arc<[u8]>) {
        self.inner
            .insert(CacheKey::DictLeaf(key), CachedEntry::DictLeaf(bytes));
    }

    // ========================================================================
    // Decoded leaf directory cache
    // ========================================================================

    /// Check if a decoded leaf directory is cached (read-only, no insertion).
    pub fn get_leaf_dir(&self, key: u128) -> Option<Arc<DecodedLeafDirV3>> {
        match self.inner.get(&CacheKey::LeafDir(key)) {
            Some(CachedEntry::LeafDir(dir)) => Some(dir),
            _ => None,
        }
    }

    /// Check if a leaf's shared memory mapping is cached (read-only, no
    /// insertion). Key is `xxh3_128(leaf_cid.to_bytes())`, as for
    /// [`try_get_or_load_leaf_mmap`](Self::try_get_or_load_leaf_mmap).
    pub fn get_leaf_mmap(&self, key: u128) -> Option<Arc<memmap2::Mmap>> {
        match self.inner.get(&CacheKey::LeafMmap(key)) {
            Some(CachedEntry::LeafMmap(mmap)) => Some(mmap),
            _ => None,
        }
    }

    /// Get or load the shared memory mapping of a leaf file with
    /// single-flight and error propagation. Key should be
    /// `xxh3_128(leaf_cid.to_bytes())` — content-addressed, so entries are
    /// immutable and self-invalidating on leaf rewrite. NotFound from the
    /// loader is NOT cached (moka only caches `Ok` values), so a
    /// remote-promotion cache-miss probe stays a cheap error path.
    pub fn try_get_or_load_leaf_mmap<F>(
        &self,
        key: u128,
        load_fn: F,
    ) -> io::Result<Arc<memmap2::Mmap>>
    where
        F: FnOnce() -> io::Result<Arc<memmap2::Mmap>>,
    {
        // Fast path: a plain hit must not pay the block_in_place cost.
        if let Some(mmap) = self.get_leaf_mmap(key) {
            return Ok(mmap);
        }
        let result = in_blocking_region(|| {
            self.inner.try_get_with(CacheKey::LeafMmap(key), || {
                load_fn().map(CachedEntry::LeafMmap)
            })
        });
        match result {
            Ok(CachedEntry::LeafMmap(mmap)) => Ok(mmap),
            Ok(_) => unreachable!("LeafMmap key always maps to LeafMmap entry"),
            Err(arc_err) => Err(arc_io_error(arc_err)),
        }
    }

    /// Get or load a decoded leaf directory with single-flight and error
    /// propagation.
    ///
    /// Key should be `xxh3_128(leaf_cid.to_bytes())` — content-addressed, so
    /// entries are immutable and self-invalidating on leaf rewrite.
    pub fn try_get_or_load_leaf_dir<F>(
        &self,
        key: u128,
        load_fn: F,
    ) -> io::Result<Arc<DecodedLeafDirV3>>
    where
        F: FnOnce() -> io::Result<Arc<DecodedLeafDirV3>>,
    {
        // Fast path: a plain hit must not pay the block_in_place cost.
        if let Some(dir) = self.get_leaf_dir(key) {
            return Ok(dir);
        }
        let result = in_blocking_region(|| {
            self.inner.try_get_with(CacheKey::LeafDir(key), || {
                load_fn().map(CachedEntry::LeafDir)
            })
        });
        match result {
            Ok(CachedEntry::LeafDir(dir)) => Ok(dir),
            Ok(_) => unreachable!("LeafDir key always maps to LeafDir entry"),
            Err(arc_err) => Err(arc_io_error(arc_err)),
        }
    }

    /// Warm-on-write: seed a decoded leaf directory the *writer* already holds.
    ///
    /// The incremental indexer, running co-located with the query server, has
    /// the just-written leaf's bytes in hand; decoding + inserting the directory
    /// here saves the reader that immediately revisits the leaf a re-decode.
    /// Key is `cid_cache_key(leaf_cid)` — content-addressed, so this can never
    /// serve stale data. Admission is TinyLFU-bounded like any other insert.
    pub fn insert_leaf_dir(&self, key: u128, dir: Arc<DecodedLeafDirV3>) {
        self.inner
            .insert(CacheKey::LeafDir(key), CachedEntry::LeafDir(dir));
    }

    // ========================================================================
    // BM25 posting leaflet cache
    // ========================================================================

    /// Compute a cache key from raw CID bytes (xxh3_128 hash).
    ///
    /// Use for both BM25 leaflet and DictLeaf entries — produces a 128-bit key
    /// from content-addressed CID bytes. The `CacheKey` enum discriminant
    /// prevents collisions between entry types that share the same hash.
    pub fn cid_cache_key(cid_bytes: &[u8]) -> u128 {
        xxhash_rust::xxh3::xxh3_128(cid_bytes)
    }

    /// Check if a BM25 posting leaflet is cached (read-only, no insertion).
    pub fn get_bm25_leaflet(&self, key: u128) -> Option<Arc<[u8]>> {
        match self.inner.get(&CacheKey::Bm25Leaflet(key)) {
            Some(CachedEntry::Bm25Leaflet(bytes)) => Some(bytes),
            _ => None,
        }
    }

    /// Insert a BM25 posting leaflet into the cache.
    ///
    /// Call after fetching raw bytes from CAS. The bytes are the compressed
    /// leaflet blob as stored in content-addressed storage — decompression
    /// and deserialization happen at the call site on each access.
    pub fn insert_bm25_leaflet(&self, key: u128, bytes: Arc<[u8]>) {
        self.inner
            .insert(CacheKey::Bm25Leaflet(key), CachedEntry::Bm25Leaflet(bytes));
    }

    // ========================================================================
    // Per-segment translated overlay ops (segment-aware overlay path)
    // ========================================================================

    /// Content-addressed key for one segment's translated overlay ops, from the
    /// store-instance id, the segment id, and the index order. `store_id` makes
    /// this sound: a dictionary rebuild constructs a fresh store (new id), so a
    /// stale translation is never reused (see the segment-aware overlay design).
    pub fn segment_ops_key(store_id: u64, seg_id: u64, index: fluree_db_core::IndexType) -> u128 {
        let mut bytes = [0u8; 17];
        bytes[0..8].copy_from_slice(&store_id.to_le_bytes());
        bytes[8..16].copy_from_slice(&seg_id.to_le_bytes());
        bytes[16] = index as u8;
        xxhash_rust::xxh3::xxh3_128(&bytes)
    }

    /// Get a segment's cached translated ops (read-only, no insertion).
    pub fn get_segment_ops(&self, key: u128) -> Option<Arc<CachedOverlaySegment>> {
        match self.inner.get(&CacheKey::SegmentOps(key)) {
            Some(CachedEntry::SegmentOps(v)) => Some(v),
            _ => None,
        }
    }

    /// Insert a segment's translated ops into the shared pool.
    pub fn insert_segment_ops(&self, key: u128, value: Arc<CachedOverlaySegment>) {
        self.inner
            .insert(CacheKey::SegmentOps(key), CachedEntry::SegmentOps(value));
    }

    // ========================================================================
    // Vector shard cache
    // ========================================================================

    /// Check if a parsed vector shard is cached (read-only, no insertion).
    pub fn get_vector_shard(&self, key: u128) -> Option<Arc<crate::arena::vector::VectorShard>> {
        match self.inner.get(&CacheKey::VectorShard(key)) {
            Some(CachedEntry::VectorShard(shard)) => Some(shard),
            _ => None,
        }
    }

    /// Get or load a parsed vector shard with single-flight and error propagation.
    ///
    /// Uses `try_get_with` so that only one thread loads a given shard;
    /// concurrent callers block on the same initializer. If the load
    /// fails, nothing is cached and the error propagates.
    ///
    /// Key should be `xxh3_128(cas_cid.as_bytes())`.
    pub fn try_get_or_load_vector_shard<F>(
        &self,
        key: u128,
        load_fn: F,
    ) -> io::Result<Arc<crate::arena::vector::VectorShard>>
    where
        F: FnOnce() -> io::Result<Arc<crate::arena::vector::VectorShard>>,
    {
        // Fast path: a plain hit must not pay the block_in_place cost.
        if let Some(shard) = self.get_vector_shard(key) {
            return Ok(shard);
        }
        // Miss: run the single-flight wait/init in a blocking region so a
        // waiter promotes a replacement worker (see in_blocking_region).
        let result = in_blocking_region(|| {
            self.inner.try_get_with(CacheKey::VectorShard(key), || {
                load_fn().map(CachedEntry::VectorShard)
            })
        });
        match result {
            Ok(CachedEntry::VectorShard(shard)) => Ok(shard),
            Ok(_) => unreachable!("VectorShard key always maps to VectorShard entry"),
            Err(arc_err) => Err(arc_io_error(arc_err)),
        }
    }

    // ========================================================================
    // Ledger-info response cache (JSON bytes)
    // ========================================================================

    /// Check if a ledger-info response blob is cached (read-only, no insertion).
    pub fn get_ledger_info(&self, key: u128) -> Option<Arc<[u8]>> {
        match self.inner.get(&CacheKey::LedgerInfo(key)) {
            Some(CachedEntry::LedgerInfo(bytes)) => Some(bytes),
            _ => None,
        }
    }

    /// Insert a ledger-info response blob into the unified cache.
    pub fn insert_ledger_info(&self, key: u128, bytes: Arc<[u8]>) {
        self.inner
            .insert(CacheKey::LedgerInfo(key), CachedEntry::LedgerInfo(bytes));
    }

    // ========================================================================
    // Query StatsView cache
    // ========================================================================

    /// Get a cached query `StatsView` (read-only, no insertion).
    pub fn get_stats_view(&self, key: u128) -> Option<Arc<StatsView>> {
        self.stats_views.get(&key)
    }

    /// Get or build a query `StatsView`, single-flight per key.
    pub fn get_or_build_stats_view<F>(&self, key: u128, build_fn: F) -> Arc<StatsView>
    where
        F: FnOnce() -> Arc<StatsView>,
    {
        // Fast path: a plain hit must not pay the block_in_place cost.
        if let Some(view) = self.get_stats_view(key) {
            return view;
        }
        // Miss: run the single-flight wait/init in a blocking region so a
        // waiter promotes a replacement worker (see in_blocking_region).
        in_blocking_region(|| self.stats_views.get_with(key, build_fn))
    }

    // ========================================================================
    // V3 column batch cache (FLI3 decoded leaflets)
    // ========================================================================

    fn get_v3_entry(&self, key: &V3LeafletKey) -> Option<CachedV3Batch> {
        match self.inner.get(&CacheKey::V3Batch(key.clone())) {
            Some(CachedEntry::V3Batch(entry)) => Some(entry),
            _ => None,
        }
    }

    /// A leaflet's cached batch and the columns it holds, whatever they are.
    pub fn v3_leaflet_entry(
        &self,
        leaf_id: u128,
        leaflet_idx: u32,
    ) -> Option<(
        super::column_types::ColumnSet,
        super::column_types::ColumnBatch,
    )> {
        self.get_v3_entry(&V3LeafletKey {
            leaf_id,
            leaflet_idx,
        })
        .map(|entry| (entry.columns, entry.batch))
    }

    /// Get a cached V3 column batch covering `key.columns` (read-only, no
    /// insertion).
    pub fn get_v3_batch(&self, key: &V3BatchCacheKey) -> Option<super::column_types::ColumnBatch> {
        let want = super::column_types::ColumnSet(key.columns);
        let entry = self.get_v3_entry(&key.into())?;
        entry.columns.covers(want).then(|| entry.into_served(want))
    }

    /// Get or decode a V3 column batch with single-flight and error propagation.
    ///
    /// `decode_fn` decodes the given columns of the leaflet. A cold leaflet
    /// decodes `key.columns` once, shared by concurrent callers via
    /// `try_get_with`. A leaflet cached with fewer columns than requested
    /// decodes only the missing ones and widens its entry, so each leaflet
    /// stays one decoded copy whichever projections read it.
    pub fn try_get_or_decode_v3_batch<F>(
        &self,
        key: V3BatchCacheKey,
        decode_fn: F,
    ) -> io::Result<super::column_types::ColumnBatch>
    where
        F: FnOnce(super::column_types::ColumnSet) -> io::Result<super::column_types::ColumnBatch>,
    {
        let want = super::column_types::ColumnSet(key.columns);
        let slot = V3LeafletKey::from(&key);
        let mut decode_fn = Some(decode_fn);
        // Fast path: a plain hit must not pay the block_in_place cost.
        let entry = match self.get_v3_entry(&slot) {
            Some(entry) => entry,
            None => {
                // Run the single-flight wait/init in a blocking region so a
                // waiter promotes a replacement worker (see in_blocking_region).
                let result = in_blocking_region(|| {
                    self.inner
                        .try_get_with(CacheKey::V3Batch(slot.clone()), || {
                            let decode = decode_fn.take().expect("init runs at most once");
                            decode(want).map(|batch| {
                                CachedEntry::V3Batch(CachedV3Batch {
                                    columns: want,
                                    batch,
                                })
                            })
                        })
                });
                match result {
                    Ok(CachedEntry::V3Batch(entry)) => entry,
                    Ok(_) => unreachable!("V3Batch key always maps to V3Batch entry"),
                    Err(arc_err) => return Err(arc_io_error(arc_err)),
                }
            }
        };
        if entry.columns.covers(want) {
            return Ok(entry.into_served(want));
        }
        // Only an entry another caller decoded can fall short of `want`.
        let decode = decode_fn
            .take()
            .expect("decode_fn unused when the entry was not ours");
        let missing = want.difference(entry.columns);
        let decoded = decode(missing)?;
        let widened = entry.widen(&decoded, missing);
        let batch = widened.batch.project_to(want);
        self.store_widened(slot, widened);
        Ok(batch)
    }

    /// Insert `widened` unless a concurrent widening already covers it,
    /// keeping any columns that one added.
    fn store_widened(&self, slot: V3LeafletKey, widened: CachedV3Batch) {
        let widened = match self.get_v3_entry(&slot) {
            Some(current) if current.columns.covers(widened.columns) => return,
            Some(current) => {
                let extra = current.columns.difference(widened.columns);
                widened.widen(&current.batch, extra)
            }
            None => widened,
        };
        self.inner
            .insert(CacheKey::V3Batch(slot), CachedEntry::V3Batch(widened));
    }

    /// Warm-on-write: seed a decoded V3 column batch the *writer* already holds.
    ///
    /// Counterpart to [`Self::insert_leaf_dir`] for the column data. The batch
    /// joins whatever the leaflet's entry already holds; readers needing any
    /// subset of `key.columns` are served from it. Content-addressed leaf id
    /// makes this always safe.
    pub fn insert_v3_batch(&self, key: V3BatchCacheKey, batch: super::column_types::ColumnBatch) {
        let columns = super::column_types::ColumnSet(key.columns);
        self.store_widened((&key).into(), CachedV3Batch { columns, batch });
    }

    // ========================================================================
    // Housekeeping
    // ========================================================================

    /// Invalidate all entries (e.g., after index rebuild).
    pub fn invalidate_all(&self) {
        self.inner.invalidate_all();
        self.stats_views.invalidate_all();
    }

    // Note: `entry_count()` is provided near cache construction for reuse in
    // operational logging alongside `weighted_size_bytes()`.
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_v3_batch_superset_serves_narrow_without_decode() {
        use crate::read::column_types::{ColumnBatch, ColumnData, ColumnSet};
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);

        // Warm-on-write seeds a full (ALL) batch for a leaflet.
        let all_batch = ColumnBatch {
            row_count: 2,
            s_id: ColumnData::Block(vec![10u64, 11].into()),
            o_key: ColumnData::Block(vec![100u64, 101].into()),
            p_id: ColumnData::Const(5),
            o_type: ColumnData::Const(7),
            o_i: ColumnData::Block(vec![0u32, 1].into()),
            t: ColumnData::Block(vec![3u32, 4].into()),
        };
        let all_key = V3BatchCacheKey {
            leaf_id: 0xABCD,
            leaflet_idx: 2,
            columns: ColumnSet::ALL.0,
        };
        cache.insert_v3_batch(all_key.clone(), all_batch);

        // A narrow (CORE) read must be served from the ALL batch, projected —
        // decode_fn panics to prove no re-decode happens.
        let core_key = V3BatchCacheKey {
            leaf_id: 0xABCD,
            leaflet_idx: 2,
            columns: ColumnSet::CORE.0,
        };
        let served = cache
            .try_get_or_decode_v3_batch(core_key, |_| panic!("must not decode: ALL covers CORE"))
            .expect("superset hit");

        // Requested (CORE) columns carry real data; omitted ones are absent.
        assert_eq!(served.row_count, 2);
        assert_eq!(served.s_id.get(1), 11);
        assert_eq!(served.o_key.get(0), 100);
        assert_eq!(served.p_id.get(0), 5);
        assert_eq!(served.o_type.get(0), 7);
        assert!(served.o_i.is_absent(), "o_i not in CORE → projected away");
        assert!(served.t.is_absent(), "t not in CORE → projected away");

        // A request for columns NOT covered (here, an exact ALL request) still
        // hits the stored ALL batch directly.
        let full = cache
            .try_get_or_decode_v3_batch(all_key, |_| panic!("must not decode: exact ALL hit"))
            .expect("exact hit");
        assert!(!full.t.is_absent(), "exact ALL request keeps t");
    }

    #[test]
    fn test_v3_batch_widens_one_entry_decoding_only_missing_columns() {
        use crate::format::column_block::ColumnId;
        use crate::read::column_types::{ColumnBatch, ColumnData, ColumnSet};
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);
        let full = ColumnBatch {
            row_count: 2,
            s_id: ColumnData::Block(vec![10u64, 11].into()),
            o_key: ColumnData::Block(vec![100u64, 101].into()),
            p_id: ColumnData::Const(5),
            o_type: ColumnData::Const(7),
            o_i: ColumnData::Block(vec![0u32, 1].into()),
            t: ColumnData::Block(vec![3u32, 4].into()),
        };
        let key = |columns: ColumnSet| V3BatchCacheKey {
            leaf_id: 0xBEEF,
            leaflet_idx: 0,
            columns: columns.0,
        };
        let decoded = std::cell::RefCell::new(Vec::new());
        let decode = |columns: ColumnSet| {
            decoded.borrow_mut().push(columns);
            Ok(full.project_to(columns))
        };
        let narrow = ColumnSet::single(ColumnId::SId).union(ColumnSet::single(ColumnId::PId));

        let first = cache
            .try_get_or_decode_v3_batch(key(narrow), decode)
            .unwrap();
        assert!(first.t.is_absent());
        let wide = cache
            .try_get_or_decode_v3_batch(key(ColumnSet::ALL), decode)
            .unwrap();
        assert_eq!(
            *decoded.borrow(),
            vec![narrow, ColumnSet::ALL.difference(narrow)],
            "the wide read decodes only the columns the narrow entry lacks"
        );
        assert_eq!(wide.s_id.get(1), 11);
        assert_eq!(wide.p_id.get(0), 5);
        assert_eq!(wide.o_key.get(0), 100);
        assert_eq!(wide.t.get(1), 4);

        let again = cache
            .try_get_or_decode_v3_batch(key(narrow), |_| panic!("covered by the widened entry"))
            .unwrap();
        assert_eq!(again.s_id.get(0), 10);
        assert!(again.o_key.is_absent() && again.t.is_absent());
        cache.inner.run_pending_tasks();
        assert_eq!(cache.entry_count(), 1, "one entry per leaflet");
    }

    #[test]
    fn test_v3_batch_insert_joins_existing_columns() {
        use crate::format::column_block::ColumnId;
        use crate::read::column_types::{ColumnBatch, ColumnData, ColumnSet};
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);
        let full = ColumnBatch {
            row_count: 1,
            s_id: ColumnData::Block(vec![10u64].into()),
            o_key: ColumnData::Block(vec![100u64].into()),
            p_id: ColumnData::Const(5),
            o_type: ColumnData::Const(7),
            o_i: ColumnData::AbsentDefault,
            t: ColumnData::Block(vec![3u32].into()),
        };
        let key = |columns: ColumnSet| V3BatchCacheKey {
            leaf_id: 0xF00D,
            leaflet_idx: 1,
            columns: columns.0,
        };
        let t = ColumnSet::single(ColumnId::T);
        cache.insert_v3_batch(key(ColumnSet::CORE), full.project_to(ColumnSet::CORE));
        cache.insert_v3_batch(key(t), full.project_to(t));
        let core_and_t = ColumnSet::CORE.union(t);
        let served = cache
            .get_v3_batch(&key(core_and_t))
            .expect("both inserts joined");
        assert_eq!(served.s_id.get(0), 10);
        assert_eq!(served.t.get(0), 3);
        assert!(
            cache.get_v3_batch(&key(ColumnSet::ALL)).is_none(),
            "o_i was never decoded, so ALL is not covered"
        );
    }

    #[test]
    fn test_bm25_leaflet_insert_get_miss() {
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);

        // Miss on empty cache.
        assert!(cache.get_bm25_leaflet(42).is_none());

        // Insert and retrieve.
        let data: Arc<[u8]> = vec![0xDE, 0xAD, 0xBE, 0xEF].into_boxed_slice().into();
        cache.insert_bm25_leaflet(42, data);
        let got = cache.get_bm25_leaflet(42).unwrap();
        assert_eq!(got.len(), 4);
        assert_eq!(got[0], 0xDE);

        // Different key → miss.
        assert!(cache.get_bm25_leaflet(99).is_none());
    }

    #[test]
    fn test_bm25_leaflet_coexists_with_other_types() {
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);

        // Insert DictLeaf and BM25Leaflet — both share the same pool.
        let dict_data: Arc<[u8]> = Arc::from(vec![1u8; 256].into_boxed_slice());
        cache
            .try_get_or_load_dict_leaf(999, || Ok(dict_data))
            .unwrap();

        let bm25_data: Arc<[u8]> = vec![2u8; 512].into_boxed_slice().into();
        cache.insert_bm25_leaflet(888, bm25_data);

        // Both retrievable from the same pool.
        assert!(cache.get_dict_leaf(999).is_some());
        assert!(cache.get_bm25_leaflet(888).is_some());

        // No cross-contamination: BM25 key 999 is not the same as DictLeaf key 999.
        assert!(cache.get_bm25_leaflet(999).is_none());
    }

    #[test]
    fn test_cid_cache_key_determinism() {
        let cid_bytes = b"sha256:abc123def456";
        let key1 = LeafletCache::cid_cache_key(cid_bytes);
        let key2 = LeafletCache::cid_cache_key(cid_bytes);
        assert_eq!(key1, key2);

        // Different input → different key.
        let other = b"sha256:xyz789";
        let key3 = LeafletCache::cid_cache_key(other);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_vector_shard_insert_get() {
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);

        // Miss on empty cache.
        assert!(cache.get_vector_shard(42).is_none());

        // Load and retrieve via try_get_or_load.
        let shard = Arc::new(crate::arena::vector::VectorShard {
            dims: 3,
            count: 2,
            values: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        });
        let shard_clone = shard.clone();
        let got = cache
            .try_get_or_load_vector_shard(42, move || Ok(shard_clone))
            .unwrap();
        assert_eq!(got.dims, 3);
        assert_eq!(got.count, 2);
        assert_eq!(got.get_f32(0).unwrap(), &[1.0f32, 2.0, 3.0]);
        assert_eq!(got.get_f32(1).unwrap(), &[4.0f32, 5.0, 6.0]);

        // Hit — should not call load fn.
        let got2 = cache
            .try_get_or_load_vector_shard(42, || {
                panic!("should not be called on cache hit");
            })
            .unwrap();
        assert_eq!(got2.count, 2);

        // Read-only get also works.
        assert!(cache.get_vector_shard(42).is_some());

        // Different key → miss.
        assert!(cache.get_vector_shard(99).is_none());
    }

    #[test]
    fn test_vector_shard_coexists_with_other_types() {
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);

        // Insert DictLeaf, BM25Leaflet, and VectorShard — all share one pool.
        let dict_data: Arc<[u8]> = Arc::from(vec![1u8; 256].into_boxed_slice());
        cache
            .try_get_or_load_dict_leaf(999, || Ok(dict_data))
            .unwrap();

        let bm25_data: Arc<[u8]> = vec![2u8; 512].into_boxed_slice().into();
        cache.insert_bm25_leaflet(888, bm25_data);

        let shard = Arc::new(crate::arena::vector::VectorShard {
            dims: 2,
            count: 1,
            values: vec![0.5, 0.5],
        });
        cache
            .try_get_or_load_vector_shard(777, move || Ok(shard))
            .unwrap();

        // All three retrievable from the same pool.
        assert!(cache.get_dict_leaf(999).is_some());
        assert!(cache.get_bm25_leaflet(888).is_some());
        assert!(cache.get_vector_shard(777).is_some());

        // No cross-contamination: VectorShard key 999 is not the same as DictLeaf key 999.
        assert!(cache.get_vector_shard(999).is_none());
    }

    #[test]
    fn test_vector_shard_load_error_not_cached() {
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);

        // Fallible load that fails → nothing cached.
        let result = cache.try_get_or_load_vector_shard(42, || {
            Err(io::Error::new(io::ErrorKind::NotFound, "shard read failed"))
        });
        assert!(result.is_err());
        assert!(cache.get_vector_shard(42).is_none());

        // Subsequent successful load → now cached.
        let shard = Arc::new(crate::arena::vector::VectorShard {
            dims: 2,
            count: 1,
            values: vec![1.0, 2.0],
        });
        let got = cache
            .try_get_or_load_vector_shard(42, move || Ok(shard))
            .unwrap();
        assert_eq!(got.dims, 2);
        assert!(cache.get_vector_shard(42).is_some());
    }

    #[test]
    fn test_dict_leaf_load_error_not_cached() {
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);

        // Fallible load that fails → nothing cached.
        let result = cache.try_get_or_load_dict_leaf(42, || {
            Err(io::Error::new(io::ErrorKind::NotFound, "disk read failed"))
        });
        assert!(result.is_err());
        assert!(cache.get_dict_leaf(42).is_none());

        // Subsequent successful load → now cached.
        let data: Arc<[u8]> = Arc::from(vec![7u8; 64].into_boxed_slice());
        let got = cache.try_get_or_load_dict_leaf(42, || Ok(data)).unwrap();
        assert_eq!(got.len(), 64);
        assert!(cache.get_dict_leaf(42).is_some());
    }

    #[test]
    fn test_stats_view_cache_reuses_arc() {
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);
        let key = 123_u128;

        let first = cache.get_or_build_stats_view(key, || Arc::new(StatsView::default()));
        let second = cache.get_or_build_stats_view(key, || unreachable!("should hit cache"));

        assert!(Arc::ptr_eq(&first, &second));
        assert!(cache.get_stats_view(key).is_some());
    }

    /// A view whose `byte_size` is at least `bytes`.
    fn stats_view_of_bytes(bytes: usize) -> Arc<StatsView> {
        let mut view = StatsView::default();
        let name = "p".repeat(200);
        let mut i = 0u32;
        while view.byte_size() < bytes {
            view.properties.insert(
                Sid::new(100, format!("{name}{i}")),
                fluree_db_core::PropertyStatData {
                    count: 1,
                    ndv_values: 1,
                    ndv_subjects: 1,
                },
            );
            i += 1;
        }
        Arc::new(view)
    }

    /// 16 MiB of leaflet budget gives the stats view pool 1 MiB, and caps any
    /// one view's weight at 512 KiB.
    const POOL_TEST_BUDGET: u64 = 16 * 1024 * 1024;

    #[test]
    fn stats_view_survives_leaflet_pressure() {
        let cache = LeafletCache::with_max_bytes(POOL_TEST_BUDGET);
        let view = cache.get_or_build_stats_view(1, || stats_view_of_bytes(64 * 1024));

        // Four times the leaflet budget in dict leaves.
        for key in 0..64u128 {
            cache.insert_dict_leaf(key, Arc::from(vec![0u8; 1024 * 1024]));
        }
        cache.inner.run_pending_tasks();
        cache.stats_views.run_pending_tasks();

        let hit = cache
            .get_stats_view(1)
            .expect("leaflet churn must not evict a stats view");
        assert!(Arc::ptr_eq(&hit, &view));
    }

    #[test]
    fn stats_view_larger_than_its_pool_is_still_cached() {
        let cache = LeafletCache::with_max_bytes(POOL_TEST_BUDGET);
        let view = cache.get_or_build_stats_view(1, || stats_view_of_bytes(2 * 1024 * 1024));
        assert!(view.byte_size() > (POOL_TEST_BUDGET / STATS_VIEW_BUDGET_DIVISOR) as usize);
        cache.stats_views.run_pending_tasks();

        let hit = cache.get_or_build_stats_view(1, || {
            unreachable!("an oversized view must stay cached, not rebuild per query")
        });
        assert!(Arc::ptr_eq(&hit, &view));
    }

    /// A new overlay epoch's view is the one queries want next, however often
    /// the view it replaces was read.
    #[test]
    fn newest_stats_view_is_kept_over_a_frequently_read_one() {
        let cache = LeafletCache::with_max_bytes(POOL_TEST_BUDGET);
        for key in [1u128, 2] {
            cache.get_or_build_stats_view(key, || stats_view_of_bytes(600 * 1024));
            cache.stats_views.run_pending_tasks();
        }
        for _ in 0..32 {
            assert!(cache.get_stats_view(1).is_some());
            assert!(cache.get_stats_view(2).is_some());
        }

        let newest = cache.get_or_build_stats_view(3, || stats_view_of_bytes(600 * 1024));
        cache.stats_views.run_pending_tasks();

        let hit = cache
            .get_stats_view(3)
            .expect("the newest view must be admitted");
        assert!(Arc::ptr_eq(&hit, &newest));
        assert!(
            cache.get_stats_view(1).is_none(),
            "the least recently used view makes room"
        );
    }

    #[test]
    fn pool_has_a_shard_per_core_within_the_shard_floor() {
        const GIB: u64 = 1024 * 1024 * 1024;
        // Budgets under two shards' floor stay one pool, whatever the cores.
        for max_bytes in [0, 4 << 20, 2 * MIN_SEGMENT_BYTES - 1] {
            assert_eq!(pool_segments(max_bytes, 64), 1, "{max_bytes} bytes");
        }
        assert_eq!(pool_segments(2 * MIN_SEGMENT_BYTES, 64), 2);
        // Cores bound a large budget, rounded up to a power of two.
        assert_eq!(pool_segments(64 * GIB, 0), 1);
        assert_eq!(pool_segments(64 * GIB, 1), 1);
        assert_eq!(pool_segments(64 * GIB, 6), 8);
        assert_eq!(pool_segments(64 * GIB, 64), 64);
        // The floor bounds many cores, rounded down to a power of two.
        assert_eq!(pool_segments(16 * GIB, 192), 64);
        assert_eq!(pool_segments(24 * GIB, 192), 64);
        for max_bytes in [GIB, 3 * GIB, 51 * GIB, 150 * GIB] {
            for cores in [1, 3, 32, 64, 96, 192, 1024] {
                let shards = pool_segments(max_bytes, cores);
                assert!(shards.is_power_of_two());
                assert!(max_bytes / shards as u64 >= MIN_SEGMENT_BYTES);
            }
        }
    }

    /// The floor is what keeps heavy entries cacheable: a 48 MiB entry fits a
    /// 256 MiB shard, but not one of the 64 shards of 16 MiB that one shard per
    /// core would give this budget.
    #[test]
    fn sharded_pool_still_caches_an_entry_heavier_than_a_core_share() {
        let max_bytes = 1024 * 1024 * 1024;
        let cache = LeafletCache::with_max_bytes_on(max_bytes, 64);
        assert_eq!(cache.inner.policy().num_segments(), 4);

        let heavy = 48 * 1024 * 1024;
        assert!(heavy as u64 > max_bytes / 64);
        cache.insert_dict_leaf(u128::MAX, Arc::from(vec![0u8; heavy]));
        for key in 0..256u128 {
            cache.insert_dict_leaf(key, Arc::from(vec![key as u8; 64]));
        }
        cache.inner.run_pending_tasks();

        assert_eq!(cache.get_dict_leaf(u128::MAX).map(|b| b.len()), Some(heavy));
        for key in 0..256u128 {
            assert_eq!(
                cache.get_dict_leaf(key).as_deref(),
                Some(&[key as u8; 64][..])
            );
        }
        assert_eq!(cache.entry_count(), 257);

        cache.invalidate_all();
        assert!(cache.get_dict_leaf(7).is_none());
        assert!(cache.get_dict_leaf(u128::MAX).is_none());
    }
}
