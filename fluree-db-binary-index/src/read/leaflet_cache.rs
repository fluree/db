//! Unified LRU cache for decoded leaflet regions and dictionary leaves.
//!
//! Uses a **single** `moka::sync::Cache` (synchronous — `BinaryCursor` is
//! sync) with TinyLFU eviction. All entry types — Region 1, Region 2, and
//! dictionary tree leaves — share one pool so TinyLFU decides what stays
//! based on actual access patterns rather than fixed budget splits.
//!
//! History is **never cached** — it lives outside the leaflet entirely, in the
//! per-leaf history sidecar (`FHS1`, located via `LeafEntry.sidecar_cid`). It's
//! cold-path data decoded on demand for time-travel replay and discarded
//! afterwards, so a HEAD-only query never pays for it.
//!
//! Note: "Region 1" / "Region 2" here are *decode groupings* used by this cache
//! (R1 = the key columns used for filtering and joins; R2 = the remaining
//! per-row columns needed to reconstruct a full flake). The V3 on-disk leaflet
//! is per-column blocks, not numbered regions — see
//! `crate::format::leaflet` and `docs/design/index-format.md`.
//!
//! ## Cache Key
//!
//! A `CacheKey` enum discriminates entry types:
//! - `R1` / `R2`: keyed by `(leaf_id, leaflet_index, to_t, epoch)`.
//!   `to_t` distinguishes time-travel snapshots; `epoch` distinguishes
//!   staged vs committed views at the same `t`.
//! - `DictLeaf`: keyed by `xxh3_128(cas_address)`. Content-addressed and
//!   immutable — no epoch/time dimension needed.

use crate::dict::reader::FencedLeaf;
use crate::format::leaf::DecodedLeafDirV3;
use crate::read::types::OverlayOp;
use fluree_db_core::subject_id::SubjectIdColumn;
use fluree_db_core::{ContentBytes, Flake, ListIndex, RangeSource, Sid, StatsView};
// moka's eviction clock reads std::time::Instant at cache construction, which
// aborts on wasm32-unknown-unknown; shadow-import core's clock-free LRU
// stand-in there (same API subset, exact-LRU instead of TinyLFU).
#[cfg(target_arch = "wasm32")]
use fluree_db_core::wasm_cache::Cache;
#[cfg(not(target_arch = "wasm32"))]
use moka::sync::Cache;
use std::collections::HashSet;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

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
// Sparse column types (for lang_id and list-index)
// ============================================================================

/// Sparse u16 column for lang_id values.
///
/// Stores only the non-zero rows as parallel `(position, value)` arrays.
/// Positions are sorted ascending. Binary search provides O(log n) lookup
/// where n is the number of non-zero entries (typically tiny).
#[derive(Clone, Debug)]
pub struct SparseU16Column {
    /// Sorted ascending row indices within the leaflet.
    pub positions: Arc<[u16]>,
    /// Parallel values (same length as positions).
    pub values: Arc<[u16]>,
}

impl SparseU16Column {
    /// Look up the value at `row`. Returns 0 if the row is absent.
    #[inline]
    pub fn get(&self, row: u16) -> u16 {
        match self.positions.binary_search(&row) {
            Ok(idx) => self.values[idx],
            Err(_) => 0,
        }
    }

    /// Number of non-zero entries.
    #[inline]
    pub fn count(&self) -> usize {
        self.positions.len()
    }

    /// Approximate byte size for cache weighing.
    #[inline]
    pub fn byte_size(&self) -> usize {
        // positions: 2 bytes each, values: 2 bytes each
        self.positions.len() * 4
    }
}

/// Sparse list-index column with variable storage width.
///
/// Values are non-negative list positions stored at the narrowest width
/// that fits the leaflet's `max_i`. Absent rows return `ListIndex::none()`.
#[derive(Clone, Debug)]
pub enum SparseIColumn {
    U8 {
        positions: Arc<[u16]>,
        values: Arc<[u8]>,
    },
    U16 {
        positions: Arc<[u16]>,
        values: Arc<[u16]>,
    },
    U32 {
        positions: Arc<[u16]>,
        values: Arc<[u32]>,
    },
}

impl SparseIColumn {
    /// Look up the list index at `row`. Returns `ListIndex::none().as_i32()` if absent.
    #[inline]
    pub fn get(&self, row: u16) -> i32 {
        match self {
            SparseIColumn::U8 { positions, values } => match positions.binary_search(&row) {
                Ok(idx) => values[idx] as i32,
                Err(_) => ListIndex::none().as_i32(),
            },
            SparseIColumn::U16 { positions, values } => match positions.binary_search(&row) {
                Ok(idx) => values[idx] as i32,
                Err(_) => ListIndex::none().as_i32(),
            },
            SparseIColumn::U32 { positions, values } => match positions.binary_search(&row) {
                Ok(idx) => values[idx] as i32,
                Err(_) => ListIndex::none().as_i32(),
            },
        }
    }

    /// Number of non-sentinel entries.
    #[inline]
    pub fn count(&self) -> usize {
        match self {
            SparseIColumn::U8 { positions, .. } => positions.len(),
            SparseIColumn::U16 { positions, .. } => positions.len(),
            SparseIColumn::U32 { positions, .. } => positions.len(),
        }
    }

    /// Approximate byte size for cache weighing.
    #[inline]
    pub fn byte_size(&self) -> usize {
        match self {
            // positions: 2 bytes each, values: 1 byte each
            SparseIColumn::U8 { positions, .. } => positions.len() * 3,
            // positions: 2 bytes each, values: 2 bytes each
            SparseIColumn::U16 { positions, .. } => positions.len() * 4,
            // positions: 2 bytes each, values: 4 bytes each
            SparseIColumn::U32 { positions, .. } => positions.len() * 6,
        }
    }

    /// The positions array (sorted ascending row indices).
    #[inline]
    pub fn positions(&self) -> &[u16] {
        match self {
            SparseIColumn::U8 { positions, .. } => positions,
            SparseIColumn::U16 { positions, .. } => positions,
            SparseIColumn::U32 { positions, .. } => positions,
        }
    }
}

// ============================================================================
// Cache key
// ============================================================================

/// Leaflet identity fields shared by R1 and R2 entries.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct LeafletCacheKey {
    /// Leaf identity: xxh3_128 of the content hash string from the branch manifest.
    /// Uses 128-bit hash to make collisions astronomically unlikely (~1 in 3.4×10^38).
    /// Leaves are content-addressed (`{sha256}.fli`), so this is stable.
    pub leaf_id: u128,
    /// Leaflet slot within the leaf (0..leaflet_count).
    pub leaflet_index: u32,
    /// Effective "state-at" t — always the resolved numeric t, never a sentinel.
    /// For current-time queries: `to_t = store.max_t()`.
    /// For time-travel: `to_t` = the target t requested by the query.
    pub to_t: i64,
    /// Overlay/stage invalidation epoch.
    /// From `OverlayProvider::epoch()`; staged != committed.
    /// 0 when no overlay is active.
    pub epoch: u64,
}

/// Unified cache key. The enum discriminant ensures R1, R2, and dict leaf
/// entries never collide even when the underlying identifiers match.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum CacheKey {
    R1(LeafletCacheKey),
    R2(LeafletCacheKey),
    /// Key = xxh3_128(CAS address). Content-addressed → immutable.
    DictLeaf(u128),
    /// Decoded leaf directory (header+dir only, no payload). Key =
    /// xxh3_128(leaf CID bytes) — content-addressed → immutable,
    /// no epoch/time dimension needed.
    LeafDir(u128),
    /// A local leaf's bytes: its file mapping, or heap bytes for a small or
    /// decrypted leaf. Key = xxh3_128(leaf CID bytes) — content-addressed →
    /// immutable. Caching the mapping saves the open+mmap+munmap syscall
    /// cycle every cursor otherwise pays per leaf access (the dominant fixed
    /// cost of an indexed point lookup).
    LeafBytes(u128),
    /// A local leaf read by range, for a store whose whole-leaf local read
    /// decrypts it all. Key = xxh3_128(leaf CID bytes), as for `LeafBytes`.
    RangedLeaf(u128),
    /// One read unit (a decrypted segment) of a ranged local source, keyed
    /// by the source's identity, which is scoped to the storage instance and
    /// to the object's envelope.
    SourceUnit(SourceUnitKey),
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

/// A read unit of a ranged source: `source` is its
/// [`cache_identity`](fluree_db_core::RangeSource::cache_identity).
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct SourceUnitKey {
    pub source: u128,
    pub unit: u64,
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

/// Cached decoded Region 1 (core columns: s_id, p_id, o_kind, o_key).
///
/// Uses `Arc<[T]>` for zero-copy sharing between cache and in-flight cursors.
/// Subject IDs use [`SubjectIdColumn`] for compact storage: narrow mode (u32) saves
/// ~20% cache capacity compared to wide mode (u64).
#[derive(Clone, Debug)]
pub struct CachedRegion1 {
    pub s_ids: SubjectIdColumn,
    pub p_ids: Arc<[u32]>,
    pub o_kinds: Arc<[u8]>,
    pub o_keys: Arc<[u64]>,
    pub row_count: usize,
}

/// Cached decoded Region 2 (metadata columns: dt, t, lang, i).
///
/// `t` is stored as `u32` (narrowed from on-disk u32). Widen to `i64` at API
/// boundaries via `t_i64()`. `lang` and `i` are stored sparsely — `None` when
/// all rows have the default value (0 / sentinel).
#[derive(Clone, Debug)]
pub struct CachedRegion2 {
    pub dt_values: Arc<[u32]>,
    pub t_values: Arc<[u32]>,
    pub lang: Option<SparseU16Column>,
    pub i_col: Option<SparseIColumn>,
}

impl CachedRegion1 {
    /// Approximate byte size of this cached value (for cache weighing).
    pub fn byte_size(&self) -> usize {
        // s_ids: 4 (narrow) or 8 (wide) + p_ids(4) + o_kinds(1) + o_keys(8)
        self.s_ids.byte_size() + self.row_count * (4 + 1 + 8)
    }
}

impl CachedRegion2 {
    /// Approximate byte size of this cached value (for cache weighing).
    pub fn byte_size(&self) -> usize {
        let rows = self.dt_values.len();
        // dt(4) + t(4) = 8 bytes per row, plus sparse overhead
        let dense = rows * 8;
        let lang_bytes = self.lang.as_ref().map_or(0, SparseU16Column::byte_size);
        let i_bytes = self.i_col.as_ref().map_or(0, SparseIColumn::byte_size);
        dense + lang_bytes + i_bytes
    }

    /// Get `t` at `row`, widened to i64 for API boundaries.
    #[inline]
    pub fn t_i64(&self, row: usize) -> i64 {
        self.t_values[row] as i64
    }

    /// Get `lang_id` at `row` (0 if absent).
    #[inline]
    pub fn lang_id(&self, row: usize) -> u16 {
        self.lang.as_ref().map_or(0, |c| c.get(row as u16))
    }

    /// Get list index at `row` (`ListIndex::none().as_i32()` if absent).
    #[inline]
    pub fn list_index(&self, row: usize) -> i32 {
        self.i_col
            .as_ref()
            .map_or(ListIndex::none().as_i32(), |c| c.get(row as u16))
    }
}

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
    R1(CachedRegion1),
    R2(CachedRegion2),
    DictLeaf(Arc<[u8]>),
    LeafDir(Arc<DecodedLeafDirV3>),
    LeafBytes(ContentBytes),
    RangedLeaf(Arc<dyn RangeSource>),
    SourceUnit(Arc<[u8]>),
    Bm25Leaflet(Arc<[u8]>),
    VectorShard(Arc<crate::arena::vector::VectorShard>),
    LedgerInfo(Arc<[u8]>),
    /// V3 decoded column batch (base columns, no overlay/replay applied).
    V3Batch(CachedV3Batch),
    /// Translated overlay ops for one novelty segment.
    SegmentOps(Arc<CachedOverlaySegment>),
}

impl CachedEntry {
    /// The entry's type, for [`LeafletCache::occupancy`].
    #[cfg(not(target_arch = "wasm32"))]
    fn kind(&self) -> &'static str {
        match self {
            CachedEntry::R1(_) => "region1",
            CachedEntry::R2(_) => "region2",
            CachedEntry::DictLeaf(_) => "dict-leaf",
            CachedEntry::LeafDir(_) => "leaf-dir",
            CachedEntry::LeafBytes(ContentBytes::Owned(_)) => "leaf-bytes-heap",
            CachedEntry::LeafBytes(ContentBytes::Shared(_)) => "leaf-bytes-shared",
            #[cfg(not(target_arch = "wasm32"))]
            CachedEntry::LeafBytes(ContentBytes::Mapped(_)) => "leaf-bytes-mapped",
            CachedEntry::RangedLeaf(_) => "ranged-leaf",
            CachedEntry::SourceUnit(_) => "source-unit",
            CachedEntry::Bm25Leaflet(_) => "bm25-leaflet",
            CachedEntry::VectorShard(_) => "vector-shard",
            CachedEntry::LedgerInfo(_) => "ledger-info",
            CachedEntry::V3Batch(_) => "v3-batch",
            CachedEntry::SegmentOps(_) => "segment-ops",
        }
    }

    /// Approximate byte size for the moka weigher.
    fn byte_size(&self) -> usize {
        match self {
            CachedEntry::R1(r1) => r1.byte_size(),
            CachedEntry::R2(r2) => r2.byte_size(),
            CachedEntry::DictLeaf(bytes) => bytes.len(),
            CachedEntry::LeafDir(dir) => dir.byte_size(),
            // A mapping is file-backed (reclaimable page cache), not heap, so
            // it weighs far less than its length; heap bytes weigh in full.
            CachedEntry::LeafBytes(bytes) => bytes.cache_weight(),
            CachedEntry::RangedLeaf(source) => source.cache_weight(),
            CachedEntry::SourceUnit(unit) => unit.len(),
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
/// backed by a single moka TinyLFU pool.
///
/// All entry types share one memory budget — TinyLFU decides what stays
/// based on actual access frequency/recency rather than fixed splits.
/// The lazy-Region-2 strategy is preserved: R1 and R2 use separate
/// `CacheKey` variants, so inserting R1 never evicts or implies R2.
pub struct LeafletCache {
    /// The leaflet pool, sized to what the budget has not reserved.
    stages: Stages,
    /// Query planner `StatsView`s, in a pool of their own. See
    /// [`stats_view_pool`].
    stats_views: Cache<u128, Arc<StatsView>>,
    /// Reverse dictionary leaves fenced for range reads, or `None` to read
    /// one whole, keyed by xxh3_128(leaf CID bytes). See [`fenced_leaf_pool`].
    fenced_leaves: Cache<u128, Option<Arc<FencedLeaf>>>,
    /// The heaviest fenced leaf the pool takes.
    fenced_leaf_max: usize,
    /// What forward dictionary packs may hold decrypted whole, shared by
    /// every reader of this cache.
    dictionary_allowance: Arc<crate::dict::allowance::DictionaryAllowance>,
}

impl std::fmt::Debug for LeafletCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeafletCache")
            .field("entries", &self.entry_count())
            .field("bytes", &self.weighted_size_bytes())
            .field("stats_views", &self.stats_views.entry_count())
            .field("stats_view_bytes", &self.stats_views.weighted_size())
            .field("dictionary_allowance", &self.dictionary_allowance)
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

/// Share of the cache budget, in percent, that storage decrypting on read
/// gives the dictionary allowance unless configured otherwise.
pub const DEFAULT_DICTIONARY_PERCENT: u64 = 60;

/// The most of the cache budget, in percent, a configured dictionary
/// allowance takes: the leaflet pool, which every other read goes through,
/// keeps the rest.
pub const MAX_DICTIONARY_PERCENT: u64 = 90;

/// Share of the leaflet budget for fenced reverse leaves.
const FENCED_LEAF_BUDGET_DIVISOR: u64 = 32;

/// Halvings of the dictionary cap the allowance's smallest level is.
const DICTIONARY_LEVELS: u32 = 7;

/// What a budget reserves for storage that decrypts on read — the fenced-leaf
/// pool and the dictionary allowance — and the leaflet pool that holds the
/// rest. A cache for such storage starts with both reserved, and gives back
/// what its data cannot use once that data is opened: the fenced-leaf pool
/// when a reverse leaf cannot be fenced and none has been, the allowance —
/// down to the smallest level that still covers what it holds and the packs
/// it could promote opened so far — when a pack cannot be read by range
/// (version 1 envelopes, remote reads). What is given back is not taken
/// again.
///
/// The leaflet pool only ever grows: a pool that shrank would leave the heap
/// it had grown into with the allocator, counted beside the allowance. It
/// cannot be resized, so each stage has a pool of its own, built when the
/// cache reaches it; entries carry over and the old pool is emptied.
struct Stages {
    budget: u64,
    fence_budget: u64,
    dictionary_cap: u64,
    /// Pools by stage: [`Stages::index`] of whether the fenced-leaf pool is
    /// still reserved and the allowance's level.
    pools: Box<[OnceLock<Cache<CacheKey, CachedEntry>>]>,
    current: AtomicUsize,
    /// Distinct packs the allowance could hold, and their bytes.
    promotable: Mutex<(HashSet<u128>, u64)>,
    /// Whether a reverse leaf has been fenced.
    fenced: AtomicBool,
    moving: Mutex<()>,
}

/// Apply `pool`'s pending removals until it holds nothing: one pass applies
/// only a batch.
fn drain(pool: &Cache<CacheKey, CachedEntry>) {
    for _ in 0..4096 {
        pool.run_pending_tasks();
        if pool.entry_count() == 0 && pool.weighted_size() == 0 {
            return;
        }
    }
}

fn leaflet_pool(capacity: u64) -> Cache<CacheKey, CachedEntry> {
    Cache::builder()
        .weigher(|_key: &CacheKey, val: &CachedEntry| val.byte_size().min(u32::MAX as usize) as u32)
        .max_capacity(capacity)
        .build()
}

impl Stages {
    /// Allowance levels: the cap, its halvings, and none.
    const LEVELS: usize = DICTIONARY_LEVELS as usize + 2;

    fn new(budget: u64, fence_budget: u64, dictionary_cap: u64) -> Self {
        let pools: Box<[OnceLock<_>]> = (0..2 * Self::LEVELS).map(|_| OnceLock::new()).collect();
        let stages = Self {
            budget,
            fence_budget,
            dictionary_cap,
            pools,
            current: AtomicUsize::new(0),
            promotable: Mutex::new((HashSet::new(), 0)),
            fenced: AtomicBool::new(false),
            moving: Mutex::new(()),
        };
        let _ = stages.pools[0].set(leaflet_pool(budget.saturating_sub(stages.reserved_at(0))));
        stages
    }

    fn index(fences: bool, level: usize) -> usize {
        usize::from(!fences) * Self::LEVELS + level
    }

    fn split(stage: usize) -> (bool, usize) {
        (stage < Self::LEVELS, stage % Self::LEVELS)
    }

    /// The allowance at `level`: the cap halved `level` times, the last
    /// level none.
    fn allowance_at(&self, level: usize) -> u64 {
        if level + 1 >= Self::LEVELS {
            0
        } else {
            self.dictionary_cap >> level
        }
    }

    /// The highest level, smallest allowance, still at least `bytes`.
    fn level_covering(&self, bytes: u64) -> usize {
        (0..Self::LEVELS)
            .rev()
            .find(|&level| self.allowance_at(level) >= bytes)
            .unwrap_or(0)
    }

    fn reserved_at(&self, stage: usize) -> u64 {
        let (fences, level) = Self::split(stage);
        (if fences { self.fence_budget } else { 0 }) + self.allowance_at(level)
    }

    fn pool(&self) -> &Cache<CacheKey, CachedEntry> {
        self.pools[self.current.load(Ordering::Acquire)]
            .get()
            .expect("the current stage's pool is built before the stage is")
    }

    fn reserved(&self) -> u64 {
        self.reserved_at(self.current.load(Ordering::Acquire))
    }

    fn fences_reserved(&self) -> bool {
        self.fence_budget > 0 && Self::split(self.current.load(Ordering::Acquire)).0
    }

    /// Move to the stage with `fences` and `level`, if that reserves less;
    /// `lower` first gives the allowance its new level and returns it.
    fn release(&self, fences: impl FnOnce(bool) -> bool, lower: impl FnOnce(usize) -> usize) {
        let _moving = self
            .moving
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let from = self.current.load(Ordering::Acquire);
        let (kept, level) = Self::split(from);
        let stage = Self::index(kept && fences(kept), lower(level).max(level));
        if self.reserved_at(stage) >= self.reserved_at(from) {
            return;
        }
        let old = self.pools[from].get().expect("current stage built");
        let new = self.pools[stage]
            .get_or_init(|| leaflet_pool(self.budget.saturating_sub(self.reserved_at(stage))));
        // The new pool is larger: everything carries over, where pools can
        // be walked.
        #[cfg(not(target_arch = "wasm32"))]
        for (key, entry) in old {
            new.insert((*key).clone(), entry);
        }
        #[cfg(target_arch = "wasm32")]
        let _ = new;
        self.current.store(stage, Ordering::Release);
        // Nothing reads a retired pool again, so nothing would run the
        // housekeeping that frees what `invalidate_all` only marks: entries
        // are removed one by one and the removals applied now.
        #[cfg(not(target_arch = "wasm32"))]
        for (key, _) in old {
            old.invalidate(key.as_ref());
        }
        old.invalidate_all();
        drain(old);
    }
}

/// See [`LeafletCache::writing`].
struct Writing<'a> {
    cache: &'a LeafletCache,
    pool: &'a Cache<CacheKey, CachedEntry>,
    key: CacheKey,
}

impl std::ops::Deref for Writing<'_> {
    type Target = Cache<CacheKey, CachedEntry>;
    fn deref(&self) -> &Self::Target {
        self.pool
    }
}

impl Drop for Writing<'_> {
    fn drop(&mut self) {
        if !std::ptr::eq(self.pool, self.cache.pool()) {
            self.pool.invalidate(&self.key);
            self.pool.run_pending_tasks();
        }
    }
}

/// See [`LeafletCache::fenced_leaf_memory`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FencedLeafMemory {
    pub leaves: u64,
    pub charged: u64,
    pub fence_bytes: u64,
    pub source_charge: u64,
}

/// The fenced-leaf pool's share of a leaflet budget, taken out of it.
fn fenced_leaf_budget(leaflet_budget_bytes: u64) -> u64 {
    (leaflet_budget_bytes / FENCED_LEAF_BUDGET_DIVISOR).max(1)
}

/// The pool for fenced reverse leaves, `budget` bytes.
///
/// Separate from leaflets so scan pressure cannot evict a fence index: one
/// weighs a few percent of its leaf, but rebuilding it decrypts the whole
/// leaf. LRU, since a new snapshot's leaves are the ones lookups want.
/// Entries are weighed in full; one heavier than half the pool is never
/// cached here (see [`LeafletCache::try_get_or_load_fenced_leaf`]).
fn fenced_leaf_pool(budget: u64) -> Cache<u128, Option<Arc<FencedLeaf>>> {
    let builder = Cache::builder()
        .weigher(|_key: &u128, leaf: &Option<Arc<FencedLeaf>>| {
            u32::try_from(fenced_leaf_weight(leaf)).unwrap_or(u32::MAX)
        })
        .max_capacity(budget);
    #[cfg(not(target_arch = "wasm32"))]
    let builder = builder.eviction_policy(moka::policy::EvictionPolicy::lru());
    builder.build()
}

fn fenced_leaf_weight(leaf: &Option<Arc<FencedLeaf>>) -> usize {
    leaf.as_ref()
        .map_or(std::mem::size_of::<Option<Arc<FencedLeaf>>>(), |leaf| {
            leaf.cache_weight()
        })
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

macro_rules! region_cache_methods {
    ($get_or:ident, $get:ident, $contains:ident, $cache_key_variant:ident, $entry_variant:ident, $ty:ty) => {
        /// Get or decode a leaflet region for the given key.
        ///
        /// On cache miss, calls `decode_fn` to produce the value, inserts it,
        /// and returns the cached copy.
        pub fn $get_or<F>(&self, key: LeafletCacheKey, decode_fn: F) -> $ty
        where
            F: FnOnce() -> $ty,
        {
            // Fast path: a plain hit must not pay the block_in_place cost.
            if let Some(v) = self.$get(&key) {
                return v;
            }
            // Miss: run the single-flight wait/init in a blocking region so a
            // waiter promotes a replacement worker (see in_blocking_region).
            let entry = in_blocking_region(|| {
                self.writing(CacheKey::$cache_key_variant(key.clone()))
                    .get_with(CacheKey::$cache_key_variant(key), || {
                        CachedEntry::$entry_variant(decode_fn())
                    })
            });
            match entry {
                CachedEntry::$entry_variant(v) => v,
                _ => unreachable!(concat!(
                    stringify!($cache_key_variant),
                    " key always maps to ",
                    stringify!($entry_variant),
                    " entry"
                )),
            }
        }

        /// Check if the region is cached for the given key (read-only, no insertion).
        pub fn $get(&self, key: &LeafletCacheKey) -> Option<$ty> {
            match self.pool().get(&CacheKey::$cache_key_variant(key.clone())) {
                Some(CachedEntry::$entry_variant(v)) => Some(v),
                _ => None,
            }
        }

        /// Check if the region is cached for the given key.
        pub fn $contains(&self, key: &LeafletCacheKey) -> bool {
            self.pool()
                .contains_key(&CacheKey::$cache_key_variant(key.clone()))
        }
    };
}

impl LeafletCache {
    /// Create a new cache with the given maximum byte budget, reserving
    /// none of it: it holds no dictionary pack whole and no fenced reverse
    /// leaf.
    ///
    /// One TinyLFU pool and budget for every leaflet-derived entry type. Query
    /// planner stats views get a separate LRU pool sized from the same budget;
    /// see [`stats_view_pool`].
    pub fn with_max_bytes(max_bytes: u64) -> Self {
        Self::with_stages(max_bytes, 0, 0)
    }

    /// A cache of `max_bytes` for storage that decrypts on read: a
    /// fenced-leaf pool and a dictionary allowance of `dictionary_cap` (by
    /// default [`DEFAULT_DICTIONARY_PERCENT`] of the budget, and at most
    /// [`MAX_DICTIONARY_PERCENT`]) are reserved out of it, and given back to
    /// the leaflet pool as far as its data turns out unable to use them (see
    /// [`Stages`]).
    pub fn with_reservable_budget(max_bytes: u64, dictionary_cap: Option<u64>) -> Self {
        let share = |percent: u64| (u128::from(max_bytes) * u128::from(percent) / 100) as u64;
        let ceiling = share(MAX_DICTIONARY_PERCENT);
        let dictionary = match dictionary_cap {
            None => share(DEFAULT_DICTIONARY_PERCENT),
            Some(cap) if cap > ceiling => {
                tracing::warn!(
                    cache_dictionary_max_mb = cap >> 20,
                    cache_max_mb = max_bytes >> 20,
                    using_mb = ceiling >> 20,
                    "cacheDictionaryMaxMb would leave the leaflet cache too little of \
                     cacheMaxMb; capped at {MAX_DICTIONARY_PERCENT}% of it"
                );
                ceiling
            }
            Some(cap) => cap,
        };
        Self::with_stages(
            max_bytes,
            fenced_leaf_budget(max_bytes - dictionary),
            dictionary,
        )
    }

    fn with_stages(budget: u64, fence_budget: u64, dictionary_cap: u64) -> Self {
        Self {
            stages: Stages::new(budget, fence_budget, dictionary_cap),
            stats_views: stats_view_pool(budget.saturating_sub(dictionary_cap)),
            fenced_leaves: fenced_leaf_pool(fence_budget.max(1)),
            fenced_leaf_max: (fence_budget / 2) as usize,
            dictionary_allowance: crate::dict::allowance::DictionaryAllowance::new(dictionary_cap),
        }
    }

    fn pool(&self) -> &Cache<CacheKey, CachedEntry> {
        self.stages.pool()
    }

    /// The leaflet pool, for writing `key`: if the cache moves to another
    /// stage before the write lands, the key is dropped from the old pool
    /// again once it has, so a retired pool keeps nothing.
    fn writing(&self, key: CacheKey) -> Writing<'_> {
        Writing {
            cache: self,
            pool: self.pool(),
            key,
        }
    }

    /// Note a pack the dictionary allowance could hold, `len` bytes, read by
    /// range: what a release keeps the allowance at least large enough for.
    pub(crate) fn note_promotable_pack(&self, identity: u128, len: u64) {
        if len == 0 || len > self.stages.dictionary_cap {
            return;
        }
        let mut seen = self
            .stages
            .promotable
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if seen.0.insert(identity) {
            seen.1 += len;
        }
    }

    /// Note a pack of storage that decrypts on read that cannot be read by
    /// range — a version 1 envelope, or a remote read — so is decrypted
    /// whole: the allowance gives back all but what covers what it holds and
    /// the packs it could promote opened so far, and, unless a reverse leaf
    /// has been fenced, the fenced-leaf pool goes too: such storage has no
    /// leaf to fence either.
    pub(crate) fn note_unrangeable_pack(&self) {
        if self.stages.dictionary_cap == 0 {
            return;
        }
        let promotable = self
            .stages
            .promotable
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .1;
        self.stages.release(
            |_| self.stages.fenced.load(Ordering::Acquire),
            |_| {
                let level = self.dictionary_allowance.lower_to(|held| {
                    self.stages
                        .allowance_at(self.stages.level_covering(held.max(promotable)))
                });
                self.stages.level_covering(level)
            },
        );
    }

    /// Note a reverse leaf fenced: the fenced-leaf pool stays reserved.
    fn note_fenced_leaf(&self) {
        self.stages.fenced.store(true, Ordering::Release);
    }

    /// Note a reverse leaf of storage that decrypts on read that cannot be
    /// fenced, having no ranged source: unless one has been fenced, the
    /// fenced-leaf pool is given back.
    pub(crate) fn note_unfenceable_leaf(&self) {
        if !self.stages.fences_reserved() || self.stages.fenced.load(Ordering::Acquire) {
            return;
        }
        self.stages.release(
            |_| self.stages.fenced.load(Ordering::Acquire),
            |level| level,
        );
    }

    /// Bytes of the budget reserved out of the leaflet pool: the fenced-leaf
    /// pool's and the dictionary allowance's.
    pub fn reserved_bytes(&self) -> u64 {
        self.stages.reserved()
    }

    /// Apply pending evictions in every pool (tests).
    #[cfg(test)]
    pub(crate) fn run_pending_tasks(&self) {
        self.pool().run_pending_tasks();
        self.fenced_leaves.run_pending_tasks();
    }

    /// Replace the dictionary allowance (tests).
    #[cfg(test)]
    pub(crate) fn with_dictionary_allowance(mut self, bytes: u64) -> Self {
        self.dictionary_allowance = crate::dict::allowance::DictionaryAllowance::new(bytes);
        self
    }

    /// Replace the dictionary allowance with one whose recent rent never
    /// ages, for tests of eviction and sharing rather than of aging.
    #[cfg(test)]
    pub(crate) fn with_unaging_dictionary_allowance(mut self, bytes: u64) -> Self {
        self.dictionary_allowance =
            crate::dict::allowance::DictionaryAllowance::with_rent_period(bytes, u64::MAX);
        self
    }

    /// Whether it keeps fenced reverse leaves: while it reserves their pool.
    pub(crate) fn holds_fences(&self) -> bool {
        self.stages.fences_reserved()
    }

    /// What forward dictionary packs may hold decrypted whole.
    pub fn dictionary_allowance(&self) -> &Arc<crate::dict::allowance::DictionaryAllowance> {
        &self.dictionary_allowance
    }

    /// Create a new cache with the given maximum megabyte budget.
    pub fn with_max_mb(mb: u64) -> Self {
        Self::with_max_bytes(mb.saturating_mul(1024 * 1024))
    }

    /// Approximate total size of entries in bytes (moka weighted size),
    /// fenced leaves included.
    pub fn weighted_size_bytes(&self) -> u64 {
        self.pool().weighted_size() + self.fenced_leaves.weighted_size()
    }

    /// What the fenced-leaf pool holds: leaves, the bytes of their fence
    /// indexes, and the charge for their sources' mappings, which are
    /// reclaimable page cache rather than heap. Walks every entry: a
    /// diagnostic.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn fenced_leaf_memory(&self) -> FencedLeafMemory {
        let mut memory = FencedLeafMemory::default();
        for (_, leaf) in &self.fenced_leaves {
            memory.leaves += 1;
            memory.charged += fenced_leaf_weight(&leaf) as u64;
            if let Some(leaf) = &leaf {
                memory.fence_bytes += leaf.fence_bytes() as u64;
                memory.source_charge += leaf.source_charge() as u64;
            }
        }
        memory
    }

    /// Number of cached entries across all entry types.
    pub fn entry_count(&self) -> u64 {
        self.pool().entry_count()
    }

    // ========================================================================
    // Region 1
    // ========================================================================

    region_cache_methods!(get_or_decode_r1, get_r1, contains_r1, R1, R1, CachedRegion1);

    // ========================================================================
    // Region 2
    // ========================================================================

    region_cache_methods!(get_or_decode_r2, get_r2, contains_r2, R2, R2, CachedRegion2);

    // ========================================================================
    // Dict tree leaf cache
    // ========================================================================

    /// Check if a dict tree leaf is cached (read-only, no insertion).
    pub fn get_dict_leaf(&self, key: u128) -> Option<Arc<[u8]>> {
        match self.pool().get(&CacheKey::DictLeaf(key)) {
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
            self.writing(CacheKey::DictLeaf(key))
                .try_get_with(CacheKey::DictLeaf(key), || {
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
        self.writing(CacheKey::DictLeaf(key))
            .insert(CacheKey::DictLeaf(key), CachedEntry::DictLeaf(bytes));
    }

    // ========================================================================
    // Decoded leaf directory cache
    // ========================================================================

    /// Check if a decoded leaf directory is cached (read-only, no insertion).
    pub fn get_leaf_dir(&self, key: u128) -> Option<Arc<DecodedLeafDirV3>> {
        match self.pool().get(&CacheKey::LeafDir(key)) {
            Some(CachedEntry::LeafDir(dir)) => Some(dir),
            _ => None,
        }
    }

    /// A local leaf's cached bytes (read-only, no insertion). Key is
    /// `xxh3_128(leaf_cid.to_bytes())`.
    pub fn get_leaf_bytes(&self, key: u128) -> Option<ContentBytes> {
        match self.pool().get(&CacheKey::LeafBytes(key)) {
            Some(CachedEntry::LeafBytes(bytes)) => Some(bytes),
            _ => None,
        }
    }

    /// Cache a local leaf's bytes, returning the entry that ends up cached:
    /// `bytes`, or what a concurrent opener of the same leaf inserted first.
    /// The caller has already done the I/O, so there is nothing to wait on
    /// and no blocking region.
    pub fn get_or_insert_leaf_bytes(&self, key: u128, bytes: ContentBytes) -> ContentBytes {
        match self
            .writing(CacheKey::LeafBytes(key))
            .get_with(CacheKey::LeafBytes(key), || CachedEntry::LeafBytes(bytes))
        {
            CachedEntry::LeafBytes(bytes) => bytes,
            _ => unreachable!("LeafBytes key always maps to LeafBytes entry"),
        }
    }

    /// A local leaf's bytes, read by `load` at most once however many
    /// openers race on the leaf, inside a blocking region — for a store whose
    /// local reads are costly (decryption). `Ok(None)` when the store does
    /// not hold the leaf; neither that nor an error is cached.
    pub fn try_get_or_load_leaf_bytes<F>(
        &self,
        key: u128,
        load: F,
    ) -> io::Result<Option<ContentBytes>>
    where
        F: FnOnce() -> io::Result<Option<ContentBytes>>,
    {
        enum Miss {
            Absent,
            Failed(io::Error),
        }
        // Fast path: a plain hit must not pay the block_in_place cost.
        if let Some(bytes) = self.get_leaf_bytes(key) {
            return Ok(Some(bytes));
        }
        let result = in_blocking_region(|| {
            self.writing(CacheKey::LeafBytes(key))
                .try_get_with(CacheKey::LeafBytes(key), || match load() {
                    Ok(Some(bytes)) => Ok(CachedEntry::LeafBytes(bytes)),
                    Ok(None) => Err(Miss::Absent),
                    Err(err) => Err(Miss::Failed(err)),
                })
        });
        match result {
            Ok(CachedEntry::LeafBytes(bytes)) => Ok(Some(bytes)),
            Ok(_) => unreachable!("LeafBytes key always maps to LeafBytes entry"),
            Err(miss) => match miss.as_ref() {
                Miss::Absent => Ok(None),
                Miss::Failed(err) => Err(io::Error::new(err.kind(), err.to_string())),
            },
        }
    }

    /// A leaf's cached range source (read-only, no insertion). Key is
    /// `xxh3_128(leaf_cid.to_bytes())`.
    pub fn get_ranged_leaf(&self, key: u128) -> Option<Arc<dyn RangeSource>> {
        match self.pool().get(&CacheKey::RangedLeaf(key)) {
            Some(CachedEntry::RangedLeaf(source)) => Some(source),
            _ => None,
        }
    }

    /// Cache a leaf's range source, returning the one that ends up cached:
    /// `source`, or what a concurrent opener inserted first. Opening a
    /// source decrypts nothing, so there is nothing to coalesce.
    pub fn get_or_insert_ranged_leaf(
        &self,
        key: u128,
        source: Arc<dyn RangeSource>,
    ) -> Arc<dyn RangeSource> {
        match self
            .writing(CacheKey::RangedLeaf(key))
            .get_with(CacheKey::RangedLeaf(key), || {
                CachedEntry::RangedLeaf(source)
            }) {
            CachedEntry::RangedLeaf(source) => source,
            _ => unreachable!("RangedLeaf key always maps to RangedLeaf entry"),
        }
    }

    /// A ranged source's read unit, read by `load` at most once however
    /// many readers race on it. A failed load is not cached.
    pub fn try_get_or_load_source_unit<F>(
        &self,
        key: SourceUnitKey,
        load: F,
    ) -> io::Result<Arc<[u8]>>
    where
        F: FnOnce() -> io::Result<Arc<[u8]>>,
    {
        // Fast path: a plain hit must not pay the block_in_place cost.
        if let Some(CachedEntry::SourceUnit(unit)) = self.pool().get(&CacheKey::SourceUnit(key)) {
            return Ok(unit);
        }
        let result = in_blocking_region(|| {
            self.writing(CacheKey::SourceUnit(key))
                .try_get_with(CacheKey::SourceUnit(key), || {
                    load().map(CachedEntry::SourceUnit)
                })
        });
        match result {
            Ok(CachedEntry::SourceUnit(unit)) => Ok(unit),
            Ok(_) => unreachable!("SourceUnit key always maps to SourceUnit entry"),
            Err(arc_err) => Err(arc_io_error(arc_err)),
        }
    }

    /// A reverse leaf fenced for range reads, or `None` to read it whole,
    /// opened by `open` at most once however many lookups race on it. A
    /// failed open is not cached.
    pub(crate) fn try_get_or_load_fenced_leaf<F>(
        &self,
        key: u128,
        open: F,
    ) -> io::Result<Option<Arc<FencedLeaf>>>
    where
        F: FnOnce() -> io::Result<Option<Arc<FencedLeaf>>>,
    {
        if !self.holds_fences() {
            return Ok(None);
        }
        // Fast path: a plain hit must not pay the block_in_place cost.
        if let Some(leaf) = self.fenced_leaves.get(&key) {
            return Ok(leaf);
        }
        // A leaf too heavy for the pool is read whole, through the leaflet
        // pool, rather than charged less than it holds.
        let open = || {
            let leaf = open()?.filter(|leaf| {
                let fits = leaf.cache_weight() <= self.fenced_leaf_max;
                if !fits {
                    fluree_db_core::io_stats::record_work(
                        "fenced leaf too heavy",
                        leaf.cache_weight(),
                    );
                }
                fits
            });
            if leaf.is_some() {
                self.note_fenced_leaf();
            }
            Ok(leaf)
        };
        let leaf = in_blocking_region(|| self.fenced_leaves.try_get_with(key, open))
            .map_err(arc_io_error)?;
        // A stage change that gave the pool back while this leaf was opening
        // read `fenced` before it was set, so the leaf is taken out again;
        // under the stage lock, so a change still to come reads it set.
        let _moving = self
            .stages
            .moving
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !self.holds_fences() {
            self.fenced_leaves.invalidate(&key);
        }
        Ok(leaf)
    }

    /// A ranged source's read unit, if cached; never loads.
    pub fn peek_source_unit(&self, key: SourceUnitKey) -> Option<Arc<[u8]>> {
        match self.pool().get(&CacheKey::SourceUnit(key)) {
            Some(CachedEntry::SourceUnit(unit)) => Some(unit),
            _ => None,
        }
    }

    pub fn invalidate_source_unit(&self, key: SourceUnitKey) {
        self.pool().invalidate(&CacheKey::SourceUnit(key));
    }

    /// Entry count and weighed bytes per entry type, heaviest first. Walks
    /// every entry: a diagnostic, not for hot paths.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn occupancy(&self) -> Vec<(&'static str, u64, u64)> {
        let mut by_kind: std::collections::BTreeMap<&'static str, (u64, u64)> =
            std::collections::BTreeMap::new();
        for (_, entry) in self.pool() {
            let slot = by_kind.entry(entry.kind()).or_insert((0, 0));
            slot.0 += 1;
            slot.1 += entry.byte_size() as u64;
        }
        for (_, leaf) in &self.fenced_leaves {
            let slot = by_kind.entry("fenced-leaf").or_insert((0, 0));
            slot.0 += 1;
            slot.1 += fenced_leaf_weight(&leaf) as u64;
        }
        let mut out: Vec<_> = by_kind.into_iter().map(|(k, (n, b))| (k, n, b)).collect();
        out.sort_by_key(|entry| std::cmp::Reverse(entry.2));
        out
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
            self.writing(CacheKey::LeafDir(key))
                .try_get_with(CacheKey::LeafDir(key), || {
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
        self.writing(CacheKey::LeafDir(key))
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
        match self.pool().get(&CacheKey::Bm25Leaflet(key)) {
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
        self.writing(CacheKey::Bm25Leaflet(key))
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
        match self.pool().get(&CacheKey::SegmentOps(key)) {
            Some(CachedEntry::SegmentOps(v)) => Some(v),
            _ => None,
        }
    }

    /// Insert a segment's translated ops into the shared pool.
    pub fn insert_segment_ops(&self, key: u128, value: Arc<CachedOverlaySegment>) {
        self.writing(CacheKey::SegmentOps(key))
            .insert(CacheKey::SegmentOps(key), CachedEntry::SegmentOps(value));
    }

    // ========================================================================
    // Vector shard cache
    // ========================================================================

    /// Check if a parsed vector shard is cached (read-only, no insertion).
    pub fn get_vector_shard(&self, key: u128) -> Option<Arc<crate::arena::vector::VectorShard>> {
        match self.pool().get(&CacheKey::VectorShard(key)) {
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
            self.writing(CacheKey::VectorShard(key))
                .try_get_with(CacheKey::VectorShard(key), || {
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
        match self.pool().get(&CacheKey::LedgerInfo(key)) {
            Some(CachedEntry::LedgerInfo(bytes)) => Some(bytes),
            _ => None,
        }
    }

    /// Insert a ledger-info response blob into the unified cache.
    pub fn insert_ledger_info(&self, key: u128, bytes: Arc<[u8]>) {
        self.writing(CacheKey::LedgerInfo(key))
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
        match self.pool().get(&CacheKey::V3Batch(key.clone())) {
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
                    self.writing(CacheKey::V3Batch(slot.clone())).try_get_with(
                        CacheKey::V3Batch(slot.clone()),
                        || {
                            let decode = decode_fn.take().expect("init runs at most once");
                            decode(want).map(|batch| {
                                CachedEntry::V3Batch(CachedV3Batch {
                                    columns: want,
                                    batch,
                                })
                            })
                        },
                    )
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
        self.writing(CacheKey::V3Batch(slot.clone()))
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
        self.pool().invalidate_all();
        self.stats_views.invalidate_all();
        self.fenced_leaves.invalidate_all();
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

    fn leaflet_capacity(cache: &LeafletCache) -> u64 {
        cache.pool().policy().max_capacity().unwrap()
    }

    /// A cache for storage that decrypts on read starts with the fenced-leaf
    /// pool reserved out of its budget, and gives it back when a reverse leaf
    /// cannot be fenced — unless one has been; a cache that reserves nothing
    /// has none.
    #[test]
    fn the_fence_pool_is_given_back_when_no_leaf_can_be_fenced() {
        for budget in [1u64 << 20, 256 << 20, 3_000_017] {
            let cache = LeafletCache::with_reservable_budget(budget, Some(0));
            let fences = cache.fenced_leaves.policy().max_capacity().unwrap();
            assert_eq!(fences, budget / 32);
            assert_eq!(cache.reserved_bytes(), fences);
            assert_eq!(leaflet_capacity(&cache) + fences, budget);
            assert!(cache.holds_fences());
            cache.note_unfenceable_leaf();
            assert_eq!(
                (cache.reserved_bytes(), leaflet_capacity(&cache)),
                (0, budget)
            );
            assert!(!cache.holds_fences());

            let fenced = LeafletCache::with_reservable_budget(budget, Some(0));
            fenced.note_fenced_leaf();
            fenced.note_unfenceable_leaf();
            assert_eq!(fenced.reserved_bytes(), fences, "a leaf was fenced");

            let plain = LeafletCache::with_max_bytes(budget);
            assert_eq!(
                (leaflet_capacity(&plain), plain.reserved_bytes()),
                (budget, 0)
            );
            assert!(!plain.holds_fences());
        }
    }

    /// The dictionary allowance starts reserved at its cap. A pack that
    /// cannot be read by range gives back all but the smallest level that
    /// covers what the allowance holds and the packs it could promote opened
    /// so far — and the fenced-leaf pool, no leaf having been fenced. The
    /// leaflet pool grows by what is given back, keeping its entries, and
    /// nothing given back is reserved again.
    #[test]
    fn the_allowance_gives_back_what_its_data_cannot_use() {
        let mib = 1u64 << 20;
        let cache = LeafletCache::with_reservable_budget(640 * mib, None);
        let allowance = Arc::clone(cache.dictionary_allowance());
        assert_eq!(allowance.capacity(), 384 * mib);
        assert_eq!(cache.reserved_bytes(), 8 * mib + 384 * mib);
        assert_eq!(leaflet_capacity(&cache), 248 * mib);
        cache.insert_dict_leaf(1, Arc::from(vec![7u8; 1024]));
        cache.note_promotable_pack(1, 16 * mib);
        cache.note_promotable_pack(1, 16 * mib);
        cache.note_unrangeable_pack();
        // Levels 384, 192, 96, 48, 24, 12 … MiB: the smallest ≥ 16 MiB.
        assert_eq!(allowance.capacity(), 24 * mib);
        assert_eq!(cache.reserved_bytes(), 24 * mib, "and no fences");
        assert_eq!(leaflet_capacity(&cache), 640 * mib - 24 * mib);
        assert!(cache.get_dict_leaf(1).is_some(), "carried over");
        for identity in 2..10 {
            cache.note_promotable_pack(identity, 16 * mib);
        }
        cache.note_unrangeable_pack();
        assert_eq!(allowance.capacity(), 24 * mib, "not taken back");

        // What the allowance already holds stays covered.
        let holding = LeafletCache::with_reservable_budget(640 * mib, None);
        let held = holding.dictionary_allowance();
        assert!(matches!(
            held.promote(1, 40 * mib, 40 * mib, |buf| Ok(buf.len() as u64))
                .unwrap(),
            crate::dict::allowance::Promotion::Promoted(_)
        ));
        holding.note_unrangeable_pack();
        assert_eq!(held.capacity(), 48 * mib);
        assert_eq!(holding.reserved_bytes(), 48 * mib);

        let none = LeafletCache::with_reservable_budget(640 * mib, None);
        none.note_unrangeable_pack();
        assert_eq!(
            (
                none.reserved_bytes(),
                none.dictionary_allowance().capacity()
            ),
            (0, 0)
        );
        assert_eq!(leaflet_capacity(&none), 640 * mib);

        let plain = LeafletCache::with_max_bytes(640 * mib);
        plain.note_unrangeable_pack();
        assert_eq!(
            (
                plain.reserved_bytes(),
                plain.dictionary_allowance().capacity()
            ),
            (0, 0)
        );
    }

    /// A configured allowance past [`MAX_DICTIONARY_PERCENT`] of the budget,
    /// up to and beyond all of it, is capped there, so the leaflet pool keeps
    /// the rest and still caches; one within it is taken as configured.
    #[test]
    fn an_allowance_past_its_share_leaves_the_leaflet_pool_the_rest() {
        let mib = 1u64 << 20;
        let budget = 64 * mib;
        let ceiling = budget * MAX_DICTIONARY_PERCENT / 100;
        for cap in [ceiling + 1, budget, 2 * budget, u64::MAX] {
            let cache = LeafletCache::with_reservable_budget(budget, Some(cap));
            let fences = cache.fenced_leaves.policy().max_capacity().unwrap();
            assert_eq!(cache.dictionary_allowance().capacity(), ceiling, "{cap}");
            assert_eq!(fences, (budget - ceiling) / 32);
            assert_eq!(leaflet_capacity(&cache), budget - ceiling - fences);
            cache.insert_dict_leaf(1, Arc::from(vec![7u8; 1024]));
            cache.run_pending_tasks();
            assert!(
                cache.get_dict_leaf(1).is_some(),
                "{cap}: the leaflet pool caches"
            );
        }
        let within = LeafletCache::with_reservable_budget(budget, Some(ceiling));
        assert_eq!(within.dictionary_allowance().capacity(), ceiling);
    }

    /// Readers and writers of the cache keep going while it gives back its
    /// reservation under them: nothing panics, and every insert stays
    /// readable or is evicted.
    #[test]
    fn the_pool_changes_stage_under_concurrent_use() {
        let mib = 1u64 << 20;
        let cache = LeafletCache::with_reservable_budget(64 * mib, Some(32 * mib));
        std::thread::scope(|scope| {
            for t in 0..4u128 {
                let cache = &cache;
                scope.spawn(move || {
                    for i in 0..2_000u128 {
                        let key = t * 1_000_000 + i;
                        let bytes: Arc<[u8]> = Arc::from(vec![(i % 251) as u8; 512]);
                        cache.insert_dict_leaf(key, Arc::clone(&bytes));
                        if let Some(read) = cache.get_dict_leaf(key) {
                            assert_eq!(read, bytes);
                        }
                    }
                });
            }
            scope.spawn(|| {
                for identity in 0..64u128 {
                    cache.note_promotable_pack(identity, mib >> (identity % 8));
                    cache.note_unrangeable_pack();
                }
                cache.note_unfenceable_leaf();
            });
        });
        assert!(cache.reserved_bytes() < 32 * mib);
        cache.run_pending_tasks();
        assert!(cache.weighted_size_bytes() <= 64 * mib);
    }

    /// A pool the cache leaves behind holds nothing: once the current pool
    /// lets an entry go, no retired pool keeps its memory alive.
    #[test]
    fn retired_pools_release_their_entries() {
        let mib = 1u64 << 20;
        let cache = LeafletCache::with_reservable_budget(64 * mib, Some(32 * mib));
        let bytes: Arc<[u8]> = Arc::from(vec![3u8; 4096]);
        cache.insert_dict_leaf(5, Arc::clone(&bytes));
        cache.note_unrangeable_pack();
        assert_eq!(cache.reserved_bytes(), 0, "moved on");
        assert_eq!(
            cache.get_dict_leaf(5).as_deref(),
            Some(&bytes[..]),
            "carried over"
        );
        cache.pool().invalidate(&CacheKey::DictLeaf(5));
        cache.pool().run_pending_tasks();
        // moka's map frees what it removes once every thread in the process
        // has left the epoch it was removed in, which other tests can delay.
        for _ in 0..10_000 {
            if Arc::strong_count(&bytes) == 1 {
                break;
            }
            crossbeam_epoch::pin().flush();
            std::thread::yield_now();
        }
        assert_eq!(Arc::strong_count(&bytes), 1, "nothing else holds it");
    }

    /// A write that lands after the cache has moved to another stage leaves
    /// nothing in the pool it retired.
    #[test]
    fn a_write_racing_a_stage_change_leaves_nothing_behind() {
        let mib = 1u64 << 20;
        let cache = LeafletCache::with_reservable_budget(64 * mib, Some(32 * mib));
        let key = CacheKey::DictLeaf(9);
        {
            let writing = cache.writing(key.clone());
            cache.note_unrangeable_pack();
            assert_eq!(cache.reserved_bytes(), 0, "moved on");
            writing.insert(key.clone(), CachedEntry::DictLeaf(Arc::from(vec![1u8; 64])));
        }
        let retired = cache.stages.pools[0].get().expect("stage 0");
        retired.run_pending_tasks();
        assert!(!retired.contains_key(&key));
        assert_eq!(retired.entry_count(), 0);
    }

    fn make_key(leaf_id: u128, leaflet_index: u32, to_t: i64, epoch: u64) -> LeafletCacheKey {
        LeafletCacheKey {
            leaf_id,
            leaflet_index,
            to_t,
            epoch,
        }
    }

    fn make_r1(row_count: usize) -> CachedRegion1 {
        CachedRegion1 {
            s_ids: SubjectIdColumn::from_narrow(vec![1u32; row_count]),
            p_ids: vec![2u32; row_count].into(),
            o_kinds: vec![0u8; row_count].into(),
            o_keys: vec![3u64; row_count].into(),
            row_count,
        }
    }

    fn make_r2(row_count: usize) -> CachedRegion2 {
        CachedRegion2 {
            dt_values: vec![0u32; row_count].into(),
            t_values: vec![1u32; row_count].into(),
            lang: None,
            i_col: None,
        }
    }

    #[test]
    fn test_leaflet_cache_miss_then_hit() {
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024); // 10MB
        let key = make_key(42, 0, 100, 0);

        // Miss — should return None
        assert!(cache.get_r1(&key).is_none());

        // get_or_decode should call the decode fn
        let mut called = false;
        let r1 = cache.get_or_decode_r1(key.clone(), || {
            called = true;
            make_r1(100)
        });
        assert!(called);
        assert_eq!(r1.row_count, 100);

        // Hit — should not call decode fn
        let mut called_again = false;
        let r1_cached = cache.get_or_decode_r1(key.clone(), || {
            called_again = true;
            make_r1(999) // should NOT be used
        });
        assert!(!called_again);
        assert_eq!(r1_cached.row_count, 100); // original value
    }

    #[test]
    fn test_leaflet_cache_r2_independent() {
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);
        let key = make_key(42, 0, 100, 0);

        // Insert R1 but not R2
        cache.get_or_decode_r1(key.clone(), || make_r1(50));
        assert!(cache.get_r1(&key).is_some());
        assert!(cache.get_r2(&key).is_none());

        // Insert R2
        cache.get_or_decode_r2(key.clone(), || make_r2(50));
        assert!(cache.get_r2(&key).is_some());
    }

    #[test]
    fn test_leaflet_cache_epoch_invalidation() {
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);

        let key_committed = make_key(42, 0, 100, 0);
        let key_staged = make_key(42, 0, 100, 1);

        cache.get_or_decode_r1(key_committed.clone(), || make_r1(100));

        // Different epoch → cache miss
        assert!(cache.get_r1(&key_staged).is_none());

        // Same epoch → cache hit
        assert!(cache.get_r1(&key_committed).is_some());
    }

    #[test]
    fn test_leaflet_cache_different_to_t() {
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);

        let key_t100 = make_key(42, 0, 100, 0);
        let key_t50 = make_key(42, 0, 50, 0);

        cache.get_or_decode_r1(key_t100.clone(), || make_r1(100));

        // Different to_t → cache miss (time-travel produces different state)
        assert!(cache.get_r1(&key_t50).is_none());

        // Same to_t → cache hit
        assert!(cache.get_r1(&key_t100).is_some());
    }

    #[test]
    fn test_leaflet_cache_different_leaflet_index() {
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);

        let key_0 = make_key(42, 0, 100, 0);
        let key_1 = make_key(42, 1, 100, 0);

        cache.get_or_decode_r1(key_0.clone(), || make_r1(100));

        // Different leaflet index → cache miss
        assert!(cache.get_r1(&key_1).is_none());
    }

    #[test]
    fn test_leaflet_cache_invalidate_all() {
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);
        let key = make_key(42, 0, 100, 0);

        cache.get_or_decode_r1(key.clone(), || make_r1(100));
        cache.get_or_decode_r2(key.clone(), || make_r2(100));
        assert!(cache.get_r1(&key).is_some());
        assert!(cache.get_r2(&key).is_some());

        cache.invalidate_all();
        // moka may not synchronously evict; run_pending() forces it
        // Use get() which should return None for invalidated entries
        // Note: moka invalidate_all is lazy, but get() after invalidate should miss
    }

    #[test]
    fn test_cached_region_byte_sizes() {
        let r1 = make_r1(25000);
        // narrow: s_ids(4) + p_ids(4) + o_kinds(1) + o_keys(8) = 17 bytes per row
        assert_eq!(r1.byte_size(), 25000 * 17); // 425KB

        let r2 = make_r2(25000);
        // dt(4) + t(4) = 8 bytes per row, no sparse overhead when lang/i absent
        assert_eq!(r2.byte_size(), 25000 * 8); // 200KB
    }

    #[test]
    fn test_unified_pool_r1_and_dict_leaf() {
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);
        let key = make_key(42, 0, 100, 0);

        // Insert R1 and a dict leaf into the same pool.
        cache.get_or_decode_r1(key.clone(), || make_r1(50));
        let dict_data: Arc<[u8]> = Arc::from(vec![1u8; 256].into_boxed_slice());
        cache
            .try_get_or_load_dict_leaf(999, || Ok(dict_data))
            .unwrap();

        // Both retrievable from the same pool.
        assert!(cache.get_r1(&key).is_some());
        assert!(cache.get_dict_leaf(999).is_some());

        // Different dict key → miss.
        assert!(cache.get_dict_leaf(1000).is_none());
    }

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
            g_id: ColumnData::AbsentDefault,
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
            g_id: ColumnData::AbsentDefault,
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
        cache.pool().run_pending_tasks();
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
            g_id: ColumnData::AbsentDefault,
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
        let r1_key = make_key(42, 0, 100, 0);

        // Insert R1, DictLeaf, and BM25Leaflet — all share the same pool.
        cache.get_or_decode_r1(r1_key.clone(), || make_r1(50));

        let dict_data: Arc<[u8]> = Arc::from(vec![1u8; 256].into_boxed_slice());
        cache
            .try_get_or_load_dict_leaf(999, || Ok(dict_data))
            .unwrap();

        let bm25_data: Arc<[u8]> = vec![2u8; 512].into_boxed_slice().into();
        cache.insert_bm25_leaflet(888, bm25_data);

        // All three retrievable from the same pool.
        assert!(cache.get_r1(&r1_key).is_some());
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
        let r1_key = make_key(42, 0, 100, 0);

        // Insert R1, DictLeaf, BM25Leaflet, and VectorShard — all share one pool.
        cache.get_or_decode_r1(r1_key.clone(), || make_r1(50));

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

        // All four retrievable from the same pool.
        assert!(cache.get_r1(&r1_key).is_some());
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
        cache.pool().run_pending_tasks();
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
}
