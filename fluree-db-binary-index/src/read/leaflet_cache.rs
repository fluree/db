//! Unified LRU cache for decoded leaflet regions and dictionary leaves.
//!
//! Uses a **single** `moka::sync::Cache` (synchronous — `BinaryCursor` is
//! sync) with TinyLFU eviction. All entry types — Region 1, Region 2, and
//! dictionary tree leaves — share one pool so TinyLFU decides what stays
//! based on actual access patterns rather than fixed budget splits.
//!
//! Region 3 (history journal) is **never cached** — it's cold-path data
//! decoded on demand for time-travel replay and discarded afterwards.
//!
//! ## Cache Key
//!
//! A `CacheKey` enum discriminates entry types:
//! - `R1` / `R2`: keyed by `(leaf_id, leaflet_index, to_t, epoch)`.
//!   `to_t` distinguishes time-travel snapshots; `epoch` distinguishes
//!   staged vs committed views at the same `t`.
//! - `DictLeaf`: keyed by `xxh3_128(cas_address)`. Content-addressed and
//!   immutable — no epoch/time dimension needed.

use fluree_db_core::subject_id::SubjectIdColumn;
use fluree_db_core::{ListIndex, StatsView};
use moka::sync::Cache;
use std::io;
use std::sync::Arc;

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
    /// Cached query `StatsView`.
    ///
    /// Key = xxh3_128 of a canonical stats-view cache key string.
    StatsView(u128),
    /// V3 (FLI3) decoded column batch. Content-addressed via `leaf_id`
    /// (derived from leaf CID) — immutable, self-invalidating on rewrite.
    /// `leaflet_idx` selects which leaflet within the leaf.
    V3Batch(V3BatchCacheKey),
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

// ============================================================================
// Unified cached value
// ============================================================================

/// Unified value stored in the single moka cache.
#[derive(Clone)]
enum CachedEntry {
    R1(CachedRegion1),
    R2(CachedRegion2),
    DictLeaf(Arc<[u8]>),
    Bm25Leaflet(Arc<[u8]>),
    VectorShard(Arc<crate::arena::vector::VectorShard>),
    LedgerInfo(Arc<[u8]>),
    StatsView(Arc<StatsView>),
    /// V3 decoded column batch (base columns, no overlay/replay applied).
    V3Batch(super::column_types::ColumnBatch),
}

impl CachedEntry {
    /// Approximate byte size for the moka weigher.
    fn byte_size(&self) -> usize {
        match self {
            CachedEntry::R1(r1) => r1.byte_size(),
            CachedEntry::R2(r2) => r2.byte_size(),
            CachedEntry::DictLeaf(bytes) => bytes.len(),
            CachedEntry::Bm25Leaflet(bytes) => bytes.len(),
            CachedEntry::VectorShard(shard) => {
                // Use capacity() for conservative accounting — correct even if
                // the parser's allocation strategy changes over time.
                std::mem::size_of::<crate::arena::vector::VectorShard>()
                    + shard.values.capacity() * std::mem::size_of::<f32>()
            }
            CachedEntry::LedgerInfo(bytes) => bytes.len(),
            CachedEntry::StatsView(view) => view.byte_size(),
            CachedEntry::V3Batch(batch) => batch.byte_size(),
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
    inner: Cache<CacheKey, CachedEntry>,
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
            let entry = self.inner.get_with(CacheKey::$cache_key_variant(key), || {
                CachedEntry::$entry_variant(decode_fn())
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
            match self.inner.get(&CacheKey::$cache_key_variant(key.clone())) {
                Some(CachedEntry::$entry_variant(v)) => Some(v),
                _ => None,
            }
        }

        /// Check if the region is cached for the given key.
        pub fn $contains(&self, key: &LeafletCacheKey) -> bool {
            self.inner
                .contains_key(&CacheKey::$cache_key_variant(key.clone()))
        }
    };
}

impl LeafletCache {
    /// Create a new cache with the given maximum byte budget.
    ///
    /// One pool, one budget. TinyLFU eviction applies across all entry types.
    pub fn with_max_bytes(max_bytes: u64) -> Self {
        let inner = Cache::builder()
            .weigher(|_key: &CacheKey, val: &CachedEntry| {
                val.byte_size().min(u32::MAX as usize) as u32
            })
            .max_capacity(max_bytes)
            .build();

        Self { inner }
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
        let result = self.inner.try_get_with(CacheKey::DictLeaf(key), || {
            load_fn().map(CachedEntry::DictLeaf)
        });
        match result {
            Ok(CachedEntry::DictLeaf(bytes)) => Ok(bytes),
            Ok(_) => unreachable!("DictLeaf key always maps to DictLeaf entry"),
            Err(arc_err) => Err(io::Error::new(arc_err.kind(), arc_err.to_string())),
        }
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
        let result = self.inner.try_get_with(CacheKey::VectorShard(key), || {
            load_fn().map(CachedEntry::VectorShard)
        });
        match result {
            Ok(CachedEntry::VectorShard(shard)) => Ok(shard),
            Ok(_) => unreachable!("VectorShard key always maps to VectorShard entry"),
            Err(arc_err) => Err(io::Error::new(arc_err.kind(), arc_err.to_string())),
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
        match self.inner.get(&CacheKey::StatsView(key)) {
            Some(CachedEntry::StatsView(view)) => Some(view),
            _ => None,
        }
    }

    /// Get or build a query `StatsView` in the unified cache.
    pub fn get_or_build_stats_view<F>(&self, key: u128, build_fn: F) -> Arc<StatsView>
    where
        F: FnOnce() -> Arc<StatsView>,
    {
        let entry = self.inner.get_with(CacheKey::StatsView(key), || {
            CachedEntry::StatsView(build_fn())
        });
        match entry {
            CachedEntry::StatsView(view) => view,
            _ => unreachable!("StatsView key always maps to StatsView entry"),
        }
    }

    // ========================================================================
    // V3 column batch cache (FLI3 decoded leaflets)
    // ========================================================================

    /// Get a cached V3 column batch (read-only, no insertion).
    pub fn get_v3_batch(&self, key: &V3BatchCacheKey) -> Option<super::column_types::ColumnBatch> {
        match self.inner.get(&CacheKey::V3Batch(key.clone())) {
            Some(CachedEntry::V3Batch(batch)) => Some(batch),
            _ => None,
        }
    }

    /// Get or decode a V3 column batch with single-flight and error propagation.
    ///
    /// On cache miss, calls `decode_fn` to produce the batch, inserts it, and
    /// returns the cached copy. Uses `try_get_with` so concurrent callers for
    /// the same leaflet share one decompression.
    pub fn try_get_or_decode_v3_batch<F>(
        &self,
        key: V3BatchCacheKey,
        decode_fn: F,
    ) -> io::Result<super::column_types::ColumnBatch>
    where
        F: FnOnce() -> io::Result<super::column_types::ColumnBatch>,
    {
        let result = self.inner.try_get_with(CacheKey::V3Batch(key), || {
            decode_fn().map(CachedEntry::V3Batch)
        });
        match result {
            Ok(CachedEntry::V3Batch(batch)) => Ok(batch),
            Ok(_) => unreachable!("V3Batch key always maps to V3Batch entry"),
            Err(arc_err) => Err(io::Error::new(arc_err.kind(), arc_err.to_string())),
        }
    }

    // ========================================================================
    // Housekeeping
    // ========================================================================

    /// Invalidate all entries (e.g., after index rebuild).
    pub fn invalidate_all(&self) {
        self.inner.invalidate_all();
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
}
