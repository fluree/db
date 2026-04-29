//! Connection-level ledger state caching
//!
//! Provides `LedgerManager` for caching loaded ledger state across queries and transactions.
//! This enables:
//! - Reusing cached ledger state (no reload per request)
//! - Freshness checking with pluggable watermark sources
//! - Idle eviction to manage memory
//! - Single-flight loading (concurrent requests share one I/O operation)
//!
//! # Architecture
//!
//! - `LedgerHandle`: Cheap cloneable reference to cached ledger state
//! - `LedgerManager`: Connection-level cache with single-flight loading
//! - `FreshnessSource`: Trait for sources that provide remote watermark info
//!
//! # Thread Safety
//!
//! - Queries get cheap clones via `snapshot()` (brief lock, then released)
//! - Transactions serialize via `lock_for_write()` (hold lock for stage+commit)
//! - Manager lock is released during I/O (no blocking other ledgers)

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use std::path::PathBuf;

use fluree_db_binary_index::{BinaryIndexStore, LeafletCache};
use fluree_db_core::db::{LedgerSnapshot, LedgerSnapshotMetadata};
use fluree_db_core::dict_novelty::DictNovelty;
use fluree_db_core::trace_commits_by_id;
use fluree_db_core::{ledger_id::normalize_ledger_id, ContentId, ContentStore, StorageBackend};
use fluree_db_ledger::{LedgerState, TypeErasedStore};
use fluree_db_nameservice::NsRecord;
use fluree_db_novelty::Novelty;
use tokio::sync::{oneshot, Mutex, RwLock};

use crate::error::{ApiError, Result};

// ============================================================================
// Monotonic Clock for Eviction
// ============================================================================

/// Global process start time for monotonic timing
///
/// Using `Instant` avoids issues with NTP clock adjustments that can affect
/// `SystemTime`. All eviction TTL calculations are based on elapsed time
/// from this anchor point.
static PROCESS_START: OnceLock<Instant> = OnceLock::new();

/// Get monotonic seconds since process start
///
/// Returns elapsed seconds from a fixed anchor point, avoiding NTP drift.
fn monotonic_secs() -> u64 {
    let start = PROCESS_START.get_or_init(Instant::now);
    start.elapsed().as_secs()
}

// ============================================================================
// CachedLedgerState - Read-only view (no lock held)
// ============================================================================

/// Read-only snapshot of ledger state - does NOT hold any lock
///
/// Safe to pass around and use for queries without blocking other operations.
/// This is a cheap clone of the underlying state (LedgerSnapshot clone is cheap via Arc fields).
pub struct CachedLedgerState {
    /// The indexed database snapshot (cheap clone - Arc fields)
    pub snapshot: LedgerSnapshot,
    /// In-memory overlay of uncommitted transactions
    pub novelty: Arc<Novelty>,
    /// Dictionary novelty layer (subjects and strings since last index build)
    pub dict_novelty: Arc<fluree_db_core::DictNovelty>,
    /// Ledger-scoped runtime IDs for predicates and datatypes.
    pub runtime_small_dicts: Arc<fluree_db_core::RuntimeSmallDicts>,
    /// Current transaction t value
    pub t: i64,
    /// Content identifier of the head commit (identity)
    pub head_commit_id: Option<fluree_db_core::ContentId>,
    /// Content identifier of the current index root (identity)
    pub head_index_id: Option<fluree_db_core::ContentId>,
    /// Nameservice record (if loaded via nameservice)
    pub ns_record: Option<NsRecord>,
    /// Binary columnar index store (v2 only).
    ///
    /// Present when `snapshot.range_provider` is also set — the two are always
    /// set/cleared together (see coherence `debug_assert` in `snapshot()`).
    pub binary_store: Option<Arc<BinaryIndexStore>>,
}

impl CachedLedgerState {
    /// Create a snapshot from ledger state
    ///
    /// Note: `binary_store` is set to `None` here — callers that have a
    /// binary store must set it after construction (see `LedgerHandle::snapshot()`).
    fn from_state(state: &LedgerState) -> Self {
        Self {
            snapshot: state.snapshot.clone(), // Cheap: Arc fields
            novelty: Arc::clone(&state.novelty),
            dict_novelty: Arc::clone(&state.dict_novelty),
            runtime_small_dicts: Arc::clone(&state.runtime_small_dicts),
            t: state.t(),
            head_commit_id: state.head_commit_id.clone(),
            head_index_id: state.head_index_id.clone(),
            ns_record: state.ns_record.clone(),
            binary_store: None,
        }
    }

    /// Get the ledger name (without branch suffix)
    ///
    /// Returns the base ledger name (e.g., "mydb"), NOT the canonical form (e.g., "mydb:main").
    /// For the canonical ledger_id, use `ledger_id()` instead.
    ///
    /// Note: This matches `NsRecord.name` semantics where "name" is the base name.
    pub fn name(&self) -> Option<&str> {
        self.ns_record.as_ref().map(|r| r.name.as_str())
    }

    /// Get the canonical ledger ID (with branch suffix)
    ///
    /// Returns the canonical form (e.g., "mydb:main") suitable for cache keys.
    /// This is the primary identifier for ledger lookups.
    pub fn ledger_id(&self) -> Option<&str> {
        self.ns_record.as_ref().map(|r| r.ledger_id.as_str())
    }

    /// Get index_t from the underlying LedgerSnapshot
    pub fn index_t(&self) -> i64 {
        self.snapshot.t
    }

    /// Convert snapshot to LedgerState for backward compatibility
    ///
    /// This creates a LedgerState with the same data as the snapshot.
    /// Use this when you need to pass the state to APIs that expect LedgerState.
    pub fn to_ledger_state(self) -> LedgerState {
        let dict_novelty = self.dict_novelty;
        LedgerState {
            snapshot: self.snapshot,
            novelty: self.novelty,
            dict_novelty,
            runtime_small_dicts: self.runtime_small_dicts,
            head_commit_id: self.head_commit_id,
            head_index_id: self.head_index_id,
            ns_record: self.ns_record,
            binary_store: self.binary_store.map(|store| TypeErasedStore(store)),
            spatial_indexes: None,
        }
    }
}

// ============================================================================
// LedgerWriteGuard - Holds mutex for transaction duration
// ============================================================================

/// Write guard that holds the ledger mutex for transaction duration
///
/// Transactions hold this guard across stage+commit to serialize writes
/// to the same ledger.
pub struct LedgerWriteGuard<'a> {
    guard: tokio::sync::MutexGuard<'a, LedgerState>,
}

impl LedgerWriteGuard<'_> {
    /// Get reference to current state
    pub fn state(&self) -> &LedgerState {
        &self.guard
    }

    /// Clone current state for passing to stage (which consumes by value)
    pub fn clone_state(&self) -> LedgerState {
        self.guard.clone()
    }

    /// Get mutable reference to current state for in-place updates
    pub fn state_mut(&mut self) -> &mut LedgerState {
        &mut self.guard
    }

    /// Replace state with new state after successful commit
    pub fn replace(&mut self, new_state: LedgerState) {
        *self.guard = new_state;
    }
}

// ============================================================================
// LedgerHandle - Cheap cloneable reference to cached state
// ============================================================================

/// Handle to a cached ledger state - cheap to clone
///
/// Provides access to cached ledger state for queries and transactions.
/// Multiple handles can reference the same cached state (via Arc).
pub struct LedgerHandle {
    inner: Arc<LedgerHandleInner>,
}

// Manual Clone impl to avoid requiring S: Clone, C: Clone bounds
// (Arc<T> is Clone regardless of T)
impl Clone for LedgerHandle {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Lock ordering invariant: always acquire `state` before `binary_store`.
/// All paths that touch both locks (snapshot, apply_index_v2, reload)
/// follow this order to prevent deadlock and ensure coherence.
struct LedgerHandleInner {
    /// Single mutex for all access (queries clone snapshot, txns hold for duration)
    state: Mutex<LedgerState>,
    /// Ledger ID (e.g., "mydb:main")
    ledger_id: String,
    /// Last access time (monotonic secs since process start)
    last_access: AtomicU64,
    /// Binary columnar index store (v2 only).
    ///
    /// Always coherent with `state.snapshot.range_provider` — writers hold
    /// the `state` lock while updating this.
    binary_store: Mutex<Option<Arc<BinaryIndexStore>>>,
}

impl LedgerHandle {
    /// Create a new handle wrapping ledger state
    pub fn new(
        ledger_id: String,
        state: LedgerState,
        binary_store: Option<Arc<BinaryIndexStore>>,
    ) -> Self {
        Self {
            inner: Arc::new(LedgerHandleInner {
                state: Mutex::new(state),
                ledger_id,
                last_access: AtomicU64::new(monotonic_secs()),
                binary_store: Mutex::new(binary_store),
            }),
        }
    }

    /// Create an ephemeral handle (for when caching is disabled)
    ///
    /// This is functionally identical to `new()`, but the naming clarifies
    /// that this handle is NOT cached and each call creates a fresh load.
    pub fn ephemeral(ledger_id: String, state: LedgerState) -> Self {
        Self::new(ledger_id, state, None)
    }

    /// Get read-only snapshot for queries (brief lock, clone, release)
    ///
    /// IMPORTANT: Queries must NOT execute while holding the internal lock.
    /// The snapshot is a cheap clone; the lock is released immediately after.
    pub async fn snapshot(&self) -> CachedLedgerState {
        self.touch();
        let state = self.inner.state.lock().await;
        let binary_store = self.inner.binary_store.lock().await.clone();
        let mut snap = CachedLedgerState::from_state(&state);
        snap.binary_store = binary_store;
        debug_assert!(
            snap.snapshot.range_provider.is_some() == snap.binary_store.is_some(),
            "range_provider and binary_store must be coherent"
        );
        snap
        // Locks released here
    }

    /// Acquire exclusive access for transaction (hold lock for stage+commit)
    pub async fn lock_for_write(&self) -> LedgerWriteGuard<'_> {
        self.touch();
        LedgerWriteGuard {
            guard: self.inner.state.lock().await,
        }
    }

    /// Keep the out-of-band cached binary store coherent with the current state.
    ///
    /// Call this while holding the state lock, **before** `guard.replace()`,
    /// passing `&new_state`. This ensures the binary_store is updated before
    /// the new state becomes visible to concurrent readers via `snapshot()`,
    /// preventing a TOCTOU window where a reader could observe the new state
    /// paired with the old binary_store.
    pub async fn sync_binary_store_from_state(&self, state: &LedgerState) {
        let binary_store = state.binary_store.as_ref().and_then(|te| {
            std::sync::Arc::clone(&te.0)
                .downcast::<BinaryIndexStore>()
                .ok()
        });
        *self.inner.binary_store.lock().await = binary_store;
    }

    /// Update last access time
    fn touch(&self) {
        self.inner
            .last_access
            .store(monotonic_secs(), Ordering::Relaxed);
    }

    /// Get last access time (monotonic secs since process start)
    pub fn last_access_secs(&self) -> u64 {
        self.inner.last_access.load(Ordering::Relaxed)
    }

    /// Get ledger ID
    pub fn ledger_id(&self) -> &str {
        &self.inner.ledger_id
    }

    /// Check if currently locked (for eviction - skip if in use)
    pub fn is_locked(&self) -> bool {
        self.inner.state.try_lock().is_err()
    }

    /// Get current index_t (brief lock to read)
    ///
    /// This returns the indexed DB's t value, NOT including novelty.
    /// For freshness checking against remote watermarks, use this method.
    pub async fn index_t(&self) -> i64 {
        let state = self.inner.state.lock().await;
        state.index_t()
    }

    /// Get current t value (max of db.t and novelty.t)
    ///
    /// This returns the ledger's current t including any unindexed novelty.
    /// Use this for comparing against nameservice commit_t to detect staleness.
    pub async fn t(&self) -> i64 {
        let state = self.inner.state.lock().await;
        state.t()
    }

    /// Get state metrics for update planning
    ///
    /// Returns (t, index_t, index_head_id) needed for UpdatePlan::plan()
    pub async fn state_metrics(&self) -> (i64, i64, Option<ContentId>) {
        let state = self.inner.state.lock().await;
        (
            state.t(),
            state.index_t(),
            state
                .ns_record
                .as_ref()
                .and_then(|r| r.index_head_id.clone()),
        )
    }

    /// Check if cached state is fresh vs remote watermark
    pub async fn check_freshness(&self, remote: &RemoteWatermark) -> FreshnessCheck {
        let local_index_t = self.index_t().await;

        if remote.index_t > local_index_t {
            FreshnessCheck::Stale
        } else {
            FreshnessCheck::Current
        }
    }

    /// Apply a v2 binary index root to this handle.
    ///
    /// All I/O (root read, BinaryIndexStore load) happens outside any lock.
    /// The state lock is held for the brief atomic swap of both `state` and
    /// `binary_store`, ensuring coherence between `db.range_provider` and
    /// `binary_store` (lock ordering: state → binary_store).
    ///
    /// `cs` MUST be branch-aware for branched ledgers (built via
    /// [`fluree_db_nameservice::branched_content_store_for_record`]) so the
    /// index root and any inherited leaf/branch blobs that live under a
    /// parent branch's namespace can be resolved.
    pub async fn apply_index_v2(
        &self,
        index_id: &ContentId,
        cs: Arc<dyn ContentStore>,
        cache_dir: &std::path::Path,
        leaflet_cache: Option<Arc<LeafletCache>>,
    ) -> Result<()> {
        let bytes = cs
            .get(index_id)
            .await
            .map_err(|e| ApiError::internal(format!("failed to read index root: {e}")))?;

        let mut store = BinaryIndexStore::load_from_root_bytes(
            Arc::clone(&cs),
            &bytes,
            cache_dir,
            leaflet_cache,
        )
        .await
        .map_err(|e| ApiError::internal(format!("failed to load binary index: {e}")))?;

        // Build metadata-only LedgerSnapshot from FIR6 root.
        let root = fluree_db_binary_index::IndexRoot::decode(&bytes)
            .map_err(|e| ApiError::internal(format!("failed to decode FIR6 root: {e}")))?;
        let meta = LedgerSnapshotMetadata {
            ledger_id: root.ledger_id,
            t: root.index_t,
            base_t: root.base_t,
            namespace_codes: root.namespace_codes.into_iter().collect(),
            ns_split_mode: root.ns_split_mode,
            stats: root.stats,
            schema: root.schema,
            subject_watermarks: root.subject_watermarks,
            string_watermark: root.string_watermark,
            graph_iris: root.graph_iris,
        };
        let db = LedgerSnapshot::new_meta(meta)
            .map_err(|e| ApiError::internal(format!("graph registry from root: {e}")))?;

        // Brief lock: apply snapshot (trims novelty, rebuilds dict_novelty),
        // then wire up range_provider with the correct dict_novelty.
        // Lock ordering: state → binary_store (same as snapshot()).
        {
            let mut state = self.inner.state.lock().await;

            // apply_loaded_db: validates, trims novelty, rebuilds dict_novelty
            state
                .apply_loaded_db(db, Some(index_id))
                .map_err(|e| ApiError::internal(format!("apply_loaded_db failed: {e}")))?;

            // Sync namespace codes between store and snapshot (bimap validation).
            crate::ns_helpers::sync_store_and_snapshot_ns(&mut store, &mut state.snapshot)?;

            let arc_store = Arc::new(store);
            crate::runtime_dicts::reseed_runtime_small_dicts(&mut state, &arc_store);

            // Build range_provider with the real dict_novelty (rebuilt by apply_loaded_db)
            let ns_fallback = Some(Arc::new(state.snapshot.namespaces().clone()));
            let provider = BinaryRangeProvider::new(
                Arc::clone(&arc_store),
                Arc::clone(&state.dict_novelty),
                Arc::clone(&state.runtime_small_dicts),
                ns_fallback,
            );
            state.snapshot.range_provider = Some(Arc::new(provider));

            let te_store: Arc<dyn std::any::Any + Send + Sync> = arc_store.clone();
            state.binary_store = Some(TypeErasedStore(te_store));
            *self.inner.binary_store.lock().await = Some(arc_store);
        }

        Ok(())
    }
}

// ============================================================================
// Freshness Types
// ============================================================================

/// Remote watermark for freshness comparison
///
/// Matches server's existing RemoteLedgerWatermark structure for compatibility.
#[derive(Clone, Debug)]
pub struct RemoteWatermark {
    /// Remote commit_t value
    pub commit_t: i64,
    /// Remote index_t value (used for freshness comparison)
    pub index_t: i64,
    /// Remote index head CID (for potential future optimization)
    pub index_head_id: Option<ContentId>,
    /// When this watermark was last updated
    pub updated_at: Instant,
}

/// Trait for sources that provide remote freshness info
///
/// Server's PeerState implements this; library doesn't depend on server types.
pub trait FreshnessSource: Send + Sync {
    /// Get remote watermark for a ledger ID
    fn watermark(&self, ledger_id: &str) -> Option<RemoteWatermark>;
}

/// Result of checking if cached state is fresh
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FreshnessCheck {
    /// Local index_t >= remote index_t
    Current,
    /// Remote index_t > local index_t, needs reload
    Stale,
}

// ============================================================================
// LoadState - Single-flight coordination
// ============================================================================

/// Loading state for single-flight coordination
///
/// Note: Loading sends `Result<LedgerHandle>` to waiters (they need the handle).
///       Reloading sends `Result<()>` to waiters (handle already obtained).
enum LoadState {
    /// Initial load in progress - waiters receive handle on success
    Loading(Vec<oneshot::Sender<std::result::Result<LedgerHandle, Arc<ApiError>>>>),
    /// Loaded and cached
    Ready(LedgerHandle),
    /// Reload in progress - handle stays valid, waiters receive () on success
    Reloading {
        handle: LedgerHandle,
        waiters: Vec<oneshot::Sender<std::result::Result<(), Arc<ApiError>>>>,
    },
}

// ============================================================================
// LedgerManagerConfig
// ============================================================================

/// Configuration for the ledger manager
#[derive(Clone)]
pub struct LedgerManagerConfig {
    /// TTL before idle ledgers are evicted (default: 30 min)
    pub idle_ttl: Duration,
    /// Sweep interval for background cleanup (default: 1 min)
    pub sweep_interval: Duration,
    /// Directory for binary index cache files (leaflets, forward indexes, etc.)
    ///
    /// Layout: `{cache_dir}/{alias_hash}/{root_hash}/...`
    /// Default: `$TMPDIR/fluree_binary_cache`
    pub cache_dir: PathBuf,
    /// Shared leaflet cache across all ledgers.
    ///
    /// By default this is injected by `Fluree` from its global cache budget.
    pub leaflet_cache: Option<Arc<LeafletCache>>,
}

impl std::fmt::Debug for LedgerManagerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LedgerManagerConfig")
            .field("idle_ttl", &self.idle_ttl)
            .field("sweep_interval", &self.sweep_interval)
            .field("cache_dir", &self.cache_dir)
            .field("has_leaflet_cache", &self.leaflet_cache.is_some())
            .finish()
    }
}

impl Default for LedgerManagerConfig {
    fn default() -> Self {
        Self {
            idle_ttl: Duration::from_secs(30 * 60),
            sweep_interval: Duration::from_secs(60),
            cache_dir: std::env::temp_dir().join("fluree_binary_cache"),
            leaflet_cache: None,
        }
    }
}

// ============================================================================
// Binary Index Loading Helper
// ============================================================================

use fluree_db_query::BinaryRangeProvider;

/// Load BinaryIndexStore from a v2 index root, attach range_provider
/// to the LedgerState's LedgerSnapshot, and return the Arc'd store.
///
/// Returns `Ok(None)` if no index_head_id is present or the root is not v2.
///
/// `nameservice` is used to assemble a branch-aware content store when the
/// ledger is a branch — without it, the index root and any inherited
/// leaf/branch blobs that live under the source branch's namespace would
/// 404 on a fresh branch that hasn't yet had its own index built.
pub(crate) async fn load_and_attach_binary_store(
    backend: &StorageBackend,
    nameservice: &dyn fluree_db_nameservice::NameService,
    state: &mut LedgerState,
    cache_dir: &std::path::Path,
    leaflet_cache: Option<Arc<LeafletCache>>,
) -> std::result::Result<Option<Arc<BinaryIndexStore>>, ApiError> {
    let record = match state.ns_record.as_ref() {
        Some(r) => r,
        None => return Ok(None),
    };
    let index_cid = match record.index_head_id.as_ref() {
        Some(cid) => cid.clone(),
        None => return Ok(None),
    };

    // Branch-aware store: walks branch ancestry on read miss so a fresh
    // branch can read leaf/branch/history blobs written under the source
    // branch's namespace. Also uses `record.ledger_id` (canonical) rather
    // than `snapshot.ledger_id`, which may still carry a source id for
    // imported/cloned roots.
    let cs: Arc<dyn ContentStore> =
        fluree_db_nameservice::branched_content_store_for_record(backend, nameservice, record)
            .await?;
    let bytes = cs
        .get(&index_cid)
        .await
        .map_err(|e| ApiError::internal(format!("failed to read index root: {e}")))?;

    // Decode FIR6 root metadata to populate snapshot watermarks.
    // `LedgerSnapshot::from_root_bytes` only parses the header; watermarks are needed for
    // DictNovelty/DictOverlay correctness (especially bound-object filters and overlay merges).
    let root = fluree_db_binary_index::IndexRoot::decode(&bytes)
        .map_err(|e| ApiError::internal(format!("failed to decode FIR6 root: {e}")))?;
    state.snapshot.subject_watermarks = root.subject_watermarks;
    state.snapshot.string_watermark = root.string_watermark;
    if root.stats.is_some() && state.snapshot.stats.is_none() {
        state.snapshot.stats = root.stats;
        tracing::debug!("loaded stats from FIR6 root");
    }
    if root.schema.is_some() && state.snapshot.schema.is_none() {
        state.snapshot.schema = root.schema;
        tracing::debug!("loaded schema from FIR6 root");
    }
    state.dict_novelty = Arc::new(DictNovelty::with_watermarks(
        state.snapshot.subject_watermarks.clone(),
        state.snapshot.string_watermark,
    ));

    let mut store =
        BinaryIndexStore::load_from_root_bytes(Arc::clone(&cs), &bytes, cache_dir, leaflet_cache)
            .await
            .map_err(|e| ApiError::internal(format!("failed to load binary index: {e}")))?;

    // Sync namespace codes between store and snapshot (bimap validation).
    crate::ns_helpers::sync_store_and_snapshot_ns(&mut store, &mut state.snapshot)?;

    // Re-populate DictNovelty from already-loaded novelty flakes, but *only* for
    // entries not present in the persisted dictionaries (canonical IDs must win).
    //
    // This prevents minting a second internal ID for an already-indexed IRI/string.
    if !state.novelty.is_empty() {
        let novelty = state.novelty.as_ref();
        let dn = Arc::make_mut(&mut state.dict_novelty);
        fluree_db_binary_index::dict_novelty_safe::populate_dict_novelty_safe(
            dn,
            Some(&store),
            novelty
                .iter_index(fluree_db_core::IndexType::Post)
                .map(|id| novelty.get_flake(id)),
        )
        .map_err(|e| ApiError::internal(format!("populate_dict_novelty_safe: {e}")))?;
    }

    let arc_store = Arc::new(store);
    crate::runtime_dicts::reseed_runtime_small_dicts(state, &arc_store);
    let ns_fallback = Some(Arc::new(state.snapshot.namespaces().clone()));
    let provider = BinaryRangeProvider::new(
        Arc::clone(&arc_store),
        Arc::clone(&state.dict_novelty),
        Arc::clone(&state.runtime_small_dicts),
        ns_fallback,
    );
    // Always rebuild the provider here so it is coherent with the freshly
    // loaded BinaryIndexStore, DictNovelty, and runtime dictionary state.
    state.snapshot.range_provider = Some(Arc::new(provider));
    // Also attach the type-erased store to the state so transaction staging
    // (which clones LedgerState under the write lock) can construct
    // graph-scoped BinaryRangeProviders (needed for named-graph upsert deletions).
    let te_store: Arc<dyn std::any::Any + Send + Sync> = arc_store.clone();
    state.binary_store = Some(TypeErasedStore(te_store));

    Ok(Some(arc_store))
}

// ============================================================================
// LedgerManager - Connection-level cache
// ============================================================================

/// Connection-level ledger cache manager
///
/// Provides single-flight loading (concurrent requests share one I/O operation)
/// and idle eviction.
pub struct LedgerManager {
    /// Cached ledger handles + loading state
    entries: RwLock<HashMap<String, LoadState>>,
    /// Storage backend for ledger loading
    backend: StorageBackend,
    /// Shared cache for index nodes
    /// Nameservice for ledger lookup/loading
    nameservice_mode: crate::NameServiceMode,
    /// Configuration
    config: LedgerManagerConfig,
    /// Shutdown flag — prevents load/reload leaders from re-inserting after disconnect_all
    shutdown: AtomicBool,
}

impl LedgerManager {
    /// Get the shared leaflet cache (if configured).
    pub fn leaflet_cache(&self) -> Option<&Arc<LeafletCache>> {
        self.config.leaflet_cache.as_ref()
    }

    /// Create a new ledger manager
    pub fn new(
        backend: StorageBackend,
        nameservice: crate::NameServiceMode,
        config: LedgerManagerConfig,
    ) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            backend,
            nameservice_mode: nameservice,
            config,
            shutdown: AtomicBool::new(false),
        }
    }

    /// Get the manager configuration
    pub fn config(&self) -> &LedgerManagerConfig {
        &self.config
    }

    /// Get cached handle or load from nameservice
    ///
    /// Uses single-flight pattern: concurrent requests for same ledger ID
    /// will share one load operation, not stampede.
    ///
    /// The ledger_id is normalized to canonical form (e.g., "mydb" -> "mydb:main")
    /// before caching to ensure consistent cache keys regardless of input form.
    pub async fn get_or_load(&self, ledger_id: &str) -> Result<LedgerHandle> {
        // Normalize ledger_id to canonical form for consistent cache keys
        // This ensures "mydb" and "mydb:main" use the same cache entry
        let canonical_alias =
            normalize_ledger_id(ledger_id).unwrap_or_else(|_| ledger_id.to_string());

        // Fast path: already loaded
        {
            let entries = self.entries.read().await;
            if let Some(LoadState::Ready(handle)) = entries.get(&canonical_alias) {
                return Ok(handle.clone());
            }
            // Also check Reloading - handle is still valid
            if let Some(LoadState::Reloading { handle, .. }) = entries.get(&canonical_alias) {
                return Ok(handle.clone());
            }
        }

        // Slow path: need to coordinate loading
        let (_should_load, rx) = {
            let mut entries = self.entries.write().await;

            match entries.get_mut(&canonical_alias) {
                Some(LoadState::Ready(handle)) => {
                    // Another task loaded while we waited for write lock
                    return Ok(handle.clone());
                }
                Some(LoadState::Reloading { handle, .. }) => {
                    // Handle is valid even during reload
                    return Ok(handle.clone());
                }
                Some(LoadState::Loading(waiters)) => {
                    // Someone else is loading - add ourselves as waiter
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    (false, Some(rx))
                }
                None => {
                    // We're first - mark as loading, release lock, do I/O
                    entries.insert(canonical_alias.clone(), LoadState::Loading(Vec::new()));
                    (true, None)
                }
            }
        };
        // Manager lock released here

        if let Some(rx) = rx {
            // Wait for the loader to finish
            // Note: Waiters receive an Http error (preserving status code) since
            // ApiError isn't Clone. The leader (first caller) gets the full error type.
            return rx
                .await
                .map_err(|_| ApiError::internal("load cancelled"))?
                .map_err(|arc_err| {
                    // The Arc contains an ApiError::Http - extract status and message
                    match arc_err.as_ref() {
                        ApiError::Http { status, message } => ApiError::http(*status, message),
                        // Fallback for any other error type (shouldn't happen)
                        other => ApiError::http(other.status_code(), other.to_string()),
                    }
                });
        }

        // We're the loader - do the I/O without holding manager lock
        // Note: We pass the original address to nameservice (it handles resolution),
        // but cache under the canonical address for consistent lookup
        let result = LedgerState::load(&self.nameservice_mode, ledger_id, &self.backend)
            .await
            .map_err(ApiError::from); // Convert LedgerError to ApiError

        // Publish result to waiters
        let mut entries = self.entries.write().await;
        let shutting_down = self.is_shutdown();

        match result {
            Ok(mut state) => {
                // Attempt to load binary index store (v2 only).
                // Non-fatal: if loading fails, log and continue without binary index.
                let binary_store = match load_and_attach_binary_store(
                    &self.backend,
                    self.nameservice_mode.reader(),
                    &mut state,
                    &self.config.cache_dir,
                    self.config.leaflet_cache.clone(),
                )
                .await
                {
                    Ok(store) => store,
                    Err(e) => {
                        tracing::warn!(
                            ledger_id = %ledger_id,
                            error = %e,
                            "Failed to load binary store, continuing without"
                        );
                        None
                    }
                };

                let handle = LedgerHandle::new(canonical_alias.clone(), state, binary_store);

                // Notify waiters
                if let Some(LoadState::Loading(waiters)) = entries.remove(&canonical_alias) {
                    for tx in waiters {
                        let _ = tx.send(Ok(handle.clone()));
                    }
                }

                // Don't re-insert into cache if shutdown has been initiated
                if !shutting_down {
                    entries.insert(canonical_alias, LoadState::Ready(handle.clone()));
                }
                Ok(handle)
            }
            Err(e) => {
                // Capture error with status code for waiters before consuming the error
                // Note: Waiters receive an Http error (preserving status code);
                // the leader (first caller) gets the original error type preserved.
                let error_for_waiters = Arc::new(ApiError::http(e.status_code(), e.to_string()));

                // Notify waiters of failure
                if let Some(LoadState::Loading(waiters)) = entries.remove(&canonical_alias) {
                    for tx in waiters {
                        let _ = tx.send(Err(Arc::clone(&error_for_waiters)));
                    }
                }

                // Leader returns the original error (preserves full type/variant)
                Err(e)
            }
        }
    }

    /// Remove ledger from cache
    ///
    /// Note: If loading/reloading is in progress, waiters will receive
    /// cancellation errors. This is acceptable - disconnect is a "force evict."
    pub async fn disconnect(&self, ledger_id: &str) {
        // Normalize ledger_id to match cache key format
        let canonical_alias =
            normalize_ledger_id(ledger_id).unwrap_or_else(|_| ledger_id.to_string());

        let mut entries = self.entries.write().await;
        // Removal will drop any pending oneshot senders, causing waiters to get RecvError
        entries.remove(&canonical_alias);
    }

    /// Remove all ledgers from cache (for shutdown)
    ///
    /// Sets a shutdown flag that prevents in-flight load/reload leaders from
    /// re-inserting entries after this call completes.
    ///
    /// Any in-flight Loading/Reloading waiters will receive cancellation
    /// errors when their oneshot senders are dropped. This is the expected
    /// behavior for a force-evict during shutdown.
    pub async fn disconnect_all(&self) {
        self.shutdown.store(true, Ordering::Release);
        let mut entries = self.entries.write().await;
        entries.clear();
    }

    /// Check if shutdown has been initiated
    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Reload a ledger in place (for peer freshness)
    ///
    /// Truly coalesces concurrent reloads: only one actual reload I/O happens,
    /// other callers wait for it. Does NOT invalidate the handle object.
    ///
    /// State machine:
    /// - Ready(h) → Reloading{h, waiters=[]} (caller becomes leader)
    /// - Reloading{h, waiters} → add waiter, await completion
    /// - Loading(waiters) → wait for initial load, then return Ok(())
    /// - None → Ok(()) (not loaded, nothing to reload)
    pub async fn reload(&self, ledger_id: &str) -> Result<()> {
        // Normalize ledger_id to match cache key format
        let canonical_alias =
            normalize_ledger_id(ledger_id).unwrap_or_else(|_| ledger_id.to_string());

        enum ReloadAction {
            BecomeLeader(LedgerHandle),
            WaitForReload(oneshot::Receiver<std::result::Result<(), Arc<ApiError>>>),
            WaitForInitialLoad(oneshot::Receiver<std::result::Result<LedgerHandle, Arc<ApiError>>>),
            NotLoaded,
        }

        // Determine action under lock
        let action = {
            let mut entries = self.entries.write().await;

            match entries.get_mut(&canonical_alias) {
                Some(LoadState::Ready(h)) => {
                    // Transition to Reloading, become leader
                    let handle = h.clone();
                    let reloading = LoadState::Reloading {
                        handle: handle.clone(),
                        waiters: Vec::new(),
                    };
                    entries.insert(canonical_alias.clone(), reloading);
                    ReloadAction::BecomeLeader(handle)
                }
                Some(LoadState::Reloading { waiters, .. }) => {
                    // Join existing reload
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    ReloadAction::WaitForReload(rx)
                }
                Some(LoadState::Loading(waiters)) => {
                    // Initial load in progress - wait for it, then done
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    ReloadAction::WaitForInitialLoad(rx)
                }
                None => ReloadAction::NotLoaded,
            }
        };
        // Manager lock released

        match action {
            ReloadAction::NotLoaded => Ok(()),

            ReloadAction::WaitForInitialLoad(rx) => {
                // Wait for initial load to complete, then we're fresh
                // Note: Waiters receive Http error (preserving status code) since ApiError isn't Clone
                rx.await
                    .map_err(|_| ApiError::internal("load cancelled"))?
                    .map_err(|arc_err| {
                        // Extract Http error preserving status code
                        match arc_err.as_ref() {
                            ApiError::Http { status, message } => ApiError::http(*status, message),
                            other => ApiError::http(other.status_code(), other.to_string()),
                        }
                    })?;
                Ok(())
            }

            ReloadAction::WaitForReload(rx) => {
                // Wait for reload leader to complete
                // Note: Waiters receive Http error (preserving status code) since ApiError isn't Clone
                rx.await
                    .map_err(|_| ApiError::internal("reload cancelled"))?
                    .map_err(|arc_err| {
                        // Extract Http error preserving status code
                        match arc_err.as_ref() {
                            ApiError::Http { status, message } => ApiError::http(*status, message),
                            other => ApiError::http(other.status_code(), other.to_string()),
                        }
                    })
            }

            ReloadAction::BecomeLeader(handle) => {
                // We're the reload leader - do I/O without manager lock
                let mut write_guard = handle.lock_for_write().await;

                let result = LedgerState::load(&self.nameservice_mode, ledger_id, &self.backend)
                    .await
                    .map_err(ApiError::from); // Convert LedgerError to ApiError

                // Publish result under lock
                let mut entries = self.entries.write().await;
                let shutting_down = self.is_shutdown();

                match result {
                    Ok(mut new_state) => {
                        // Attempt to load binary index store (v2 only)
                        let new_binary_store = match load_and_attach_binary_store(
                            &self.backend,
                            self.nameservice_mode.reader(),
                            &mut new_state,
                            &self.config.cache_dir,
                            self.config.leaflet_cache.clone(),
                        )
                        .await
                        {
                            Ok(store) => store,
                            Err(e) => {
                                tracing::warn!(
                                    ledger_id = %ledger_id,
                                    error = %e,
                                    "Failed to load binary store during reload, continuing without"
                                );
                                None
                            }
                        };

                        write_guard.replace(new_state);
                        // Update binary_store coherently with the new state
                        *handle.inner.binary_store.lock().await = new_binary_store;

                        // Notify waiters and restore Ready state (unless shutting down)
                        if let Some(LoadState::Reloading { handle, waiters }) =
                            entries.remove(&canonical_alias)
                        {
                            for tx in waiters {
                                let _ = tx.send(Ok(()));
                            }
                            if !shutting_down {
                                entries.insert(canonical_alias, LoadState::Ready(handle));
                            }
                        }
                        Ok(())
                    }
                    Err(e) => {
                        // Capture error with status code for waiters before consuming the error
                        // Note: Waiters receive Http error (preserving status code); leader gets original type
                        let error_for_waiters =
                            Arc::new(ApiError::http(e.status_code(), e.to_string()));

                        // Notify waiters of failure, restore Ready (keep old data) unless shutting down
                        if let Some(LoadState::Reloading { handle, waiters }) =
                            entries.remove(&canonical_alias)
                        {
                            for tx in waiters {
                                let _ = tx.send(Err(Arc::clone(&error_for_waiters)));
                            }
                            if !shutting_down {
                                entries.insert(canonical_alias, LoadState::Ready(handle));
                            }
                        }
                        // Leader returns the original error (preserves full type/variant)
                        Err(e)
                    }
                }
            }
        }
    }

    /// Sweep idle entries (called by maintenance task)
    ///
    /// Only evicts Ready entries. Never evicts Loading/Reloading entries
    /// (they're transient; eviction would cancel waiters unexpectedly).
    pub async fn sweep_idle(&self) -> Vec<String> {
        let now = monotonic_secs();
        let ttl_secs = self.config.idle_ttl.as_secs();

        let mut entries = self.entries.write().await;
        let mut evicted = Vec::new();

        entries.retain(|alias, load_state| {
            if let LoadState::Ready(handle) = load_state {
                let age = now.saturating_sub(handle.last_access_secs());
                if age > ttl_secs && !handle.is_locked() {
                    evicted.push(alias.clone());
                    return false;
                }
            }
            // Keep Loading/Reloading entries - they're transient
            true
        });

        evicted
    }

    /// Get count of cached ledgers (for metrics)
    pub async fn cached_count(&self) -> usize {
        let entries = self.entries.read().await;
        entries
            .values()
            .filter(|s| matches!(s, LoadState::Ready(_) | LoadState::Reloading { .. }))
            .count()
    }

    /// Get list of cached ledger IDs (for introspection)
    pub async fn cached_aliases(&self) -> Vec<String> {
        let entries = self.entries.read().await;
        entries
            .iter()
            .filter_map(|(alias, state)| {
                if matches!(state, LoadState::Ready(_) | LoadState::Reloading { .. }) {
                    Some(alias.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Spawn maintenance task for idle sweeping
    ///
    /// Returns JoinHandle for graceful shutdown. Call `.abort()` on shutdown.
    pub fn spawn_maintenance(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let mgr = Arc::clone(self);
        let sweep_interval = self.config.sweep_interval;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(sweep_interval);

            loop {
                interval.tick().await;
                let evicted = mgr.sweep_idle().await;
                if !evicted.is_empty() {
                    tracing::debug!(
                        count = evicted.len(),
                        aliases = ?evicted,
                        "Swept idle ledgers"
                    );
                }
            }
        })
    }
}

// ============================================================================
// Notify Types - Update Plan
// ============================================================================

/// Maximum number of commits to catch up incrementally before falling back to
/// full reload. For gaps larger than this, the cost of N individual commit loads
/// exceeds the cost of a single full reload.
const MAX_INCREMENTAL_COMMITS: i64 = 5;

/// Decision from comparing cached state to nameservice record
///
/// Based on the legacy `plan-ns-update` behavior:
/// - Compare local `t()` (max of index + novelty) against nameservice `commit_t`
/// - Determine minimal action needed to bring cache up to date
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdatePlan {
    /// Nothing to do - state is current
    /// (ns.commit_t == local.t() AND index unchanged)
    Noop,

    /// Index advanced but commit_t unchanged
    /// (ns.commit_t == local.t() BUT ns.index_t > local.index_t)
    /// Action: reload index root, trim novelty to only commits > new index_t
    IndexOnly {
        /// New index head CID to load
        index_head_id: ContentId,
        /// New index_t value
        index_t: i64,
    },

    /// Small commit gap — catch up incrementally
    /// (ns.commit_t > local.t() AND gap <= MAX_INCREMENTAL_COMMITS)
    /// Action: walk backward from commit_head to local_t, apply in oldest→newest order
    CommitCatchUp {
        /// CID of the remote commit head
        commit_head_id: ContentId,
        /// Remote commit_t
        commit_t: i64,
        /// Number of commits to catch up (1 = single commit fast path)
        gap: i64,
        /// If the index also advanced, apply after commits
        index_update: Option<(ContentId, i64)>,
    },

    /// Stale - remote is too many commits ahead for incremental catch-up
    /// (ns.commit_t - local.t() > MAX_INCREMENTAL_COMMITS)
    /// Action: full reload from nameservice
    Reload,
}

impl UpdatePlan {
    /// Plan the update action based on local state vs nameservice record
    ///
    /// This mirrors the legacy `plan-ns-update` logic:
    /// - If commit_t matches local t(), check if index advanced
    /// - If commit_t is exactly local t() + 1, we can apply just that commit
    /// - If commit_t is further ahead, we're stale and need full reload
    ///
    /// # Arguments
    /// * `local_t` - Local ledger's current t (max of index + novelty)
    /// * `local_index_t` - Local ledger's indexed t (db.t)
    /// * `local_index_id` - Local ledger's current index CID (if any)
    /// * `ns` - Fresh nameservice record
    pub fn plan(
        local_t: i64,
        local_index_t: i64,
        local_index_id: Option<&ContentId>,
        ns: &NsRecord,
    ) -> Self {
        if ns.commit_t == local_t {
            // Commits are in sync - check if index advanced
            match (&ns.index_head_id, local_index_id) {
                (Some(ns_idx), Some(local_idx))
                    if ns_idx != local_idx && ns.index_t > local_index_t =>
                {
                    // Index advanced, same commit_t
                    UpdatePlan::IndexOnly {
                        index_head_id: ns_idx.clone(),
                        index_t: ns.index_t,
                    }
                }
                (Some(ns_idx), None) if ns.index_t > local_index_t => {
                    // Index appeared where there was none
                    UpdatePlan::IndexOnly {
                        index_head_id: ns_idx.clone(),
                        index_t: ns.index_t,
                    }
                }
                _ => UpdatePlan::Noop,
            }
        } else if ns.commit_t > local_t && (ns.commit_t - local_t) <= MAX_INCREMENTAL_COMMITS {
            // Small gap — catch up incrementally
            let gap = ns.commit_t - local_t;
            match &ns.commit_head_id {
                Some(cid) => {
                    // Check if index also advanced
                    let index_update = match (&ns.index_head_id, local_index_id) {
                        (Some(ns_idx), Some(local_idx))
                            if ns_idx != local_idx && ns.index_t > local_index_t =>
                        {
                            Some((ns_idx.clone(), ns.index_t))
                        }
                        (Some(ns_idx), None) if ns.index_t > local_index_t => {
                            Some((ns_idx.clone(), ns.index_t))
                        }
                        _ => None,
                    };
                    UpdatePlan::CommitCatchUp {
                        commit_head_id: cid.clone(),
                        commit_t: ns.commit_t,
                        gap,
                        index_update,
                    }
                }
                None => UpdatePlan::Reload,
            }
        } else if ns.commit_t > local_t {
            // Large gap — full reload
            UpdatePlan::Reload
        } else {
            // ns.commit_t < local_t - shouldn't happen (time travel?)
            // Treat as noop - local is somehow ahead
            tracing::warn!(
                local_t = local_t,
                ns_commit_t = ns.commit_t,
                "Local t is ahead of nameservice commit_t - unexpected"
            );
            UpdatePlan::Noop
        }
    }

    /// Check if this plan requires any action
    pub fn is_noop(&self) -> bool {
        matches!(self, UpdatePlan::Noop)
    }

    /// Check if this plan requires a full reload
    pub fn requires_reload(&self) -> bool {
        matches!(self, UpdatePlan::Reload)
    }
}

/// Input for notify: ledger ID + optional fresh NsRecord
pub struct NsNotify {
    /// Ledger ID
    pub ledger_id: String,
    /// Fresh nameservice record (if already fetched)
    pub record: Option<NsRecord>,
}

/// Result of notify operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotifyResult {
    /// Ledger not in cache, no action taken
    NotLoaded,
    /// Already up to date, no action taken (Noop plan)
    Current,
    /// Index was updated incrementally (trimmed novelty, loaded new index root)
    IndexUpdated,
    /// Applied commits incrementally to novelty
    CommitsApplied {
        /// Number of commits applied
        count: i64,
    },
    /// Was stale, reloaded in-place via reload()
    Reloaded,
}

/// Options for `Fluree::refresh()`.
///
/// Controls minimum-`t` enforcement: if `min_t` is set and the ledger's `t`
/// is still below that value after pulling the latest state from the
/// nameservice, the call returns [`ApiError::AwaitTNotReached`] so the
/// caller can decide whether to retry, back off, or time out.
#[derive(Debug, Clone, Default)]
pub struct RefreshOpts {
    /// If set, refresh will return an error when the ledger's `t` is still
    /// below this value after the nameservice pull + apply cycle.
    pub min_t: Option<i64>,
}

/// Result of a successful `Fluree::refresh()` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshResult {
    /// The ledger's `t` value after the refresh completed.
    pub t: i64,
    /// What action was taken (no-op, incremental, reload, etc.).
    pub action: NotifyResult,
}

impl LedgerManager {
    /// Handle nameservice update notification
    ///
    /// Uses update planning to determine minimal action:
    /// - Noop: nothing to do
    /// - IndexOnly: load new index root, trim novelty (incremental)
    /// - CommitCatchUp: load 1-5 commits, merge into novelty (incremental)
    /// - Reload: full reload from storage (large gaps)
    pub async fn notify(&self, input: NsNotify) -> Result<NotifyResult> {
        // Check if ledger is cached
        let handle = {
            let entries = self.entries.read().await;
            match entries.get(&input.ledger_id) {
                Some(LoadState::Ready(h)) => h.clone(),
                Some(LoadState::Reloading { handle, .. }) => handle.clone(),
                _ => return Ok(NotifyResult::NotLoaded),
            }
        };

        // Get fresh record from nameservice if not provided
        let ns_record = match input.record {
            Some(r) => r,
            None => match self
                .nameservice_mode
                .reader()
                .lookup(&input.ledger_id)
                .await?
            {
                Some(r) => r,
                None => return Ok(NotifyResult::Current), // Ledger doesn't exist
            },
        };

        // Get local state metrics for planning
        let (local_t, local_index_t, local_index_id) = handle.state_metrics().await;

        // Plan the update action
        let plan = UpdatePlan::plan(local_t, local_index_t, local_index_id.as_ref(), &ns_record);

        tracing::debug!(
            alias = %input.ledger_id,
            local_t = local_t,
            local_index_t = local_index_t,
            ns_commit_t = ns_record.commit_t,
            ns_index_t = ns_record.index_t,
            ?plan,
            "notify: computed update plan"
        );

        // Build a branch-aware content store once for any plan variant that
        // walks the commit chain or loads index blobs. For non-branched
        // ledgers this returns a flat namespace store with no extra cost
        // beyond the `ns_record` lookup we already did above.
        let cs_for_record = || async {
            fluree_db_nameservice::branched_content_store_for_record(
                &self.backend,
                self.nameservice_mode.reader(),
                &ns_record,
            )
            .await
            .map_err(ApiError::from)
        };

        match plan {
            UpdatePlan::Noop => Ok(NotifyResult::Current),

            UpdatePlan::IndexOnly {
                index_head_id,
                index_t,
            } => {
                tracing::debug!(
                    alias = %input.ledger_id,
                    %index_head_id, index_t,
                    "notify: applying index update (incremental)"
                );
                let cs = cs_for_record().await?;
                handle
                    .apply_index_v2(
                        &index_head_id,
                        cs,
                        &self.config.cache_dir,
                        self.config.leaflet_cache.clone(),
                    )
                    .await?;
                Ok(NotifyResult::IndexUpdated)
            }

            UpdatePlan::CommitCatchUp {
                commit_head_id,
                commit_t,
                gap,
                index_update,
            } => {
                tracing::debug!(
                    alias = %input.ledger_id,
                    %commit_head_id, commit_t, gap,
                    has_index_update = index_update.is_some(),
                    "notify: catching up commits (incremental)"
                );

                let ledger_id_canonical = handle.ledger_id().to_string();
                let cs: Arc<dyn ContentStore> = cs_for_record().await?;

                // Load commits outside any lock.
                // trace_commits_by_id walks HEAD → oldest, stopping at local_t.
                // Collect then reverse to apply oldest → newest.
                let mut commits = Vec::with_capacity(gap as usize);
                {
                    let stream =
                        trace_commits_by_id(Arc::clone(&cs), commit_head_id.clone(), local_t);
                    futures::pin_mut!(stream);
                    while let Some(result) = futures::StreamExt::next(&mut stream).await {
                        let commit = result.map_err(|e| {
                            ApiError::internal(format!("load commit during catch-up: {e}"))
                        })?;
                        commits.push(commit);
                    }
                }

                // Verify we got the expected number of commits.
                // If the chain is broken or shorter than expected, fall back to reload.
                let loaded = commits.len() as i64;
                if loaded != gap {
                    tracing::warn!(
                        alias = %input.ledger_id,
                        expected = gap,
                        loaded,
                        "incremental catch-up: commit count mismatch, falling back to reload"
                    );
                    self.reload(&input.ledger_id).await?;
                    return Ok(NotifyResult::Reloaded);
                }

                commits.reverse(); // oldest → newest

                // Apply commits under write lock (brief — all sync).
                // Re-check state.t() under lock to guard against concurrent updates.
                {
                    let mut write_guard = handle.lock_for_write().await;
                    let current_t = write_guard.state().t();
                    if current_t != local_t {
                        // State advanced while we were loading commits — re-plan
                        // by falling through to a reload (safe, not optimal).
                        tracing::debug!(
                            alias = %input.ledger_id,
                            local_t, current_t,
                            "incremental catch-up: state advanced concurrently, falling back to reload"
                        );
                        drop(write_guard);
                        self.reload(&input.ledger_id).await?;
                        return Ok(NotifyResult::Reloaded);
                    }
                    for commit in commits {
                        write_guard
                            .state_mut()
                            .apply_single_commit(commit, &ledger_id_canonical)
                            .map_err(|e| ApiError::internal(format!("apply commit: {e}")))?;
                    }

                    // If a binary range provider is attached, refresh it to point at the
                    // updated DictNovelty. `apply_single_commit` replaces `state.dict_novelty`
                    // with a new Arc; without re-attaching here, the provider holds a stale
                    // Arc and overlay translation will fail for novelty-only strings/subjects.
                    if let Some(rp) = write_guard.state().snapshot.range_provider.as_ref() {
                        if let Some(brp) = rp.as_any().downcast_ref::<BinaryRangeProvider>() {
                            let store = Arc::clone(brp.store());
                            let dn = Arc::clone(&write_guard.state().dict_novelty);
                            let runtime_small_dicts =
                                Arc::clone(&write_guard.state().runtime_small_dicts);
                            let ns_fallback =
                                Some(Arc::new(write_guard.state().snapshot.namespaces().clone()));
                            write_guard.state_mut().snapshot.range_provider =
                                Some(Arc::new(BinaryRangeProvider::new(
                                    store,
                                    dn,
                                    runtime_small_dicts,
                                    ns_fallback,
                                )));
                        }
                    }
                }

                // Apply index update if present (after commits so novelty has latest flakes).
                // Reuse the same branch-aware store built above for the commit walk.
                if let Some((index_head_id, _index_t)) = index_update {
                    handle
                        .apply_index_v2(
                            &index_head_id,
                            Arc::clone(&cs),
                            &self.config.cache_dir,
                            self.config.leaflet_cache.clone(),
                        )
                        .await?;
                }

                Ok(NotifyResult::CommitsApplied { count: gap })
            }

            UpdatePlan::Reload => {
                self.reload(&input.ledger_id).await?;
                Ok(NotifyResult::Reloaded)
            }
        }
    }

    /// Returns the cached ledger's current `t`, or `None` if not cached.
    pub async fn current_t(&self, ledger_id: &str) -> Option<i64> {
        let entries = self.entries.read().await;
        match entries.get(ledger_id) {
            Some(LoadState::Ready(handle)) => {
                let (t, _, _) = handle.state_metrics().await;
                Some(t)
            }
            Some(LoadState::Reloading { handle, .. }) => {
                let (t, _, _) = handle.state_metrics().await;
                Some(t)
            }
            _ => None,
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monotonic_secs() {
        let t1 = monotonic_secs();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let t2 = monotonic_secs();
        // Should be monotonically non-decreasing
        assert!(t2 >= t1);
    }

    #[test]
    fn test_config_defaults() {
        let config = LedgerManagerConfig::default();
        assert_eq!(config.idle_ttl, Duration::from_secs(30 * 60));
        assert_eq!(config.sweep_interval, Duration::from_secs(60));
    }

    #[test]
    fn test_freshness_check() {
        let remote = RemoteWatermark {
            commit_t: 10,
            index_t: 8,
            index_head_id: None,
            updated_at: Instant::now(),
        };

        // These are compile-time checks that the types work correctly
        assert_eq!(FreshnessCheck::Current, FreshnessCheck::Current);
        assert_eq!(FreshnessCheck::Stale, FreshnessCheck::Stale);
        assert_ne!(FreshnessCheck::Current, FreshnessCheck::Stale);

        // RemoteWatermark is Clone
        let _cloned = remote.clone();
    }

    #[test]
    fn test_update_plan_variants() {
        assert_eq!(UpdatePlan::Noop, UpdatePlan::Noop);
        assert_eq!(UpdatePlan::Reload, UpdatePlan::Reload);
        assert_ne!(UpdatePlan::Noop, UpdatePlan::Reload);
    }

    #[test]
    fn test_notify_result_variants() {
        assert_eq!(NotifyResult::NotLoaded, NotifyResult::NotLoaded);
        assert_eq!(NotifyResult::Current, NotifyResult::Current);
        assert_eq!(NotifyResult::Reloaded, NotifyResult::Reloaded);
    }

    // ========================================================================
    // UpdatePlan::plan() tests - compatibility scenarios
    // ========================================================================

    fn make_cid(label: &str) -> ContentId {
        use fluree_db_core::ContentKind;
        ContentId::new(ContentKind::Commit, label.as_bytes())
    }

    fn make_index_cid(label: &str) -> ContentId {
        use fluree_db_core::ContentKind;
        ContentId::new(ContentKind::IndexRoot, label.as_bytes())
    }

    fn make_ns_record(
        commit_t: i64,
        index_t: i64,
        commit_id: Option<ContentId>,
        index_id: Option<ContentId>,
    ) -> NsRecord {
        NsRecord {
            ledger_id: "test:main".to_string(),
            name: "test:main".to_string(),
            branch: "main".to_string(),
            commit_head_id: commit_id,
            config_id: None,
            commit_t,
            index_head_id: index_id,
            index_t,
            default_context: None,
            retracted: false,
            source_branch: None,
            branches: 0,
        }
    }

    #[test]
    fn test_update_plan_noop_when_commit_t_matches() {
        // Local t == ns.commit_t, index unchanged -> Noop
        let idx_cid = make_index_cid("index:8");
        let ns = make_ns_record(10, 8, Some(make_cid("commit:10")), Some(idx_cid.clone()));
        let plan = UpdatePlan::plan(10, 8, Some(&idx_cid), &ns);
        assert_eq!(plan, UpdatePlan::Noop);
    }

    #[test]
    fn test_update_plan_noop_when_commit_t_matches_no_index() {
        // Local t == ns.commit_t, no index on either side -> Noop
        let ns = make_ns_record(5, 0, Some(make_cid("commit:5")), None);
        let plan = UpdatePlan::plan(5, 0, None, &ns);
        assert_eq!(plan, UpdatePlan::Noop);
    }

    #[test]
    fn test_update_plan_noop_with_novelty_present() {
        // Key regression test: local has novelty (commit_t > index_t is normal)
        // ns.commit_t == local.t() should be Noop, not trigger reload
        let idx_cid = make_index_cid("index:5");
        let ns = make_ns_record(10, 5, Some(make_cid("commit:10")), Some(idx_cid.clone()));
        // Local: index_t=5, but t()=10 due to novelty
        let plan = UpdatePlan::plan(10, 5, Some(&idx_cid), &ns);
        assert_eq!(plan, UpdatePlan::Noop);
    }

    #[test]
    fn test_update_plan_index_only_when_index_advanced() {
        // Local t == ns.commit_t, but ns.index_t > local.index_t -> IndexOnly
        let ns = make_ns_record(
            10,
            10,
            Some(make_cid("commit:10")),
            Some(make_index_cid("index:10")),
        );
        let local_idx = make_index_cid("index:5");
        // Local: t()=10, index_t=5
        let plan = UpdatePlan::plan(10, 5, Some(&local_idx), &ns);
        assert!(matches!(plan, UpdatePlan::IndexOnly { index_t: 10, .. }));
    }

    #[test]
    fn test_update_plan_index_only_when_index_appears() {
        // Local t == ns.commit_t, index appears where there was none -> IndexOnly
        let ns = make_ns_record(
            10,
            10,
            Some(make_cid("commit:10")),
            Some(make_index_cid("index:10")),
        );
        // Local: t()=10, no index
        let plan = UpdatePlan::plan(10, 0, None, &ns);
        assert!(matches!(plan, UpdatePlan::IndexOnly { index_t: 10, .. }));
    }

    #[test]
    fn test_update_plan_commit_catch_up_when_one_ahead() {
        // ns.commit_t == local.t() + 1 -> CommitCatchUp with gap=1
        let local_idx = make_index_cid("index:5");
        let ns = make_ns_record(11, 5, Some(make_cid("commit:11")), Some(local_idx.clone()));
        let plan = UpdatePlan::plan(10, 5, Some(&local_idx), &ns);
        assert!(matches!(
            plan,
            UpdatePlan::CommitCatchUp {
                commit_t: 11,
                gap: 1,
                ..
            }
        ));
    }

    #[test]
    fn test_update_plan_reload_when_stale() {
        // ns.commit_t far ahead of local -> Reload (gap > MAX_INCREMENTAL_COMMITS)
        let local_idx = make_index_cid("index:5");
        let ns = make_ns_record(
            20,
            15,
            Some(make_cid("commit:20")),
            Some(make_index_cid("index:15")),
        );
        let plan = UpdatePlan::plan(10, 5, Some(&local_idx), &ns);
        assert_eq!(plan, UpdatePlan::Reload);
    }

    #[test]
    fn test_update_plan_catch_up_when_small_gap() {
        // ns.commit_t a few ahead -> CommitCatchUp (gap <= MAX_INCREMENTAL_COMMITS)
        let local_idx = make_index_cid("index:5");
        let ns = make_ns_record(13, 5, Some(make_cid("commit:13")), Some(local_idx.clone()));
        let plan = UpdatePlan::plan(10, 5, Some(&local_idx), &ns);
        assert!(matches!(
            plan,
            UpdatePlan::CommitCatchUp {
                commit_t: 13,
                gap: 3,
                index_update: None,
                ..
            }
        ));
    }

    #[test]
    fn test_update_plan_catch_up_with_index_update() {
        // Commit + index both advanced
        let local_idx = make_index_cid("index:5");
        let ns = make_ns_record(
            11,
            10,
            Some(make_cid("commit:11")),
            Some(make_index_cid("index:10")),
        );
        let plan = UpdatePlan::plan(10, 5, Some(&local_idx), &ns);
        match plan {
            UpdatePlan::CommitCatchUp {
                gap: 1,
                index_update: Some((_, idx_t)),
                ..
            } => {
                assert_eq!(idx_t, 10);
            }
            other => panic!("expected CommitCatchUp with index_update, got {other:?}"),
        }
    }

    #[test]
    fn test_update_plan_noop_when_local_ahead() {
        // Edge case: local is somehow ahead of ns (shouldn't happen, but be safe)
        let local_idx = make_index_cid("index:5");
        let ns = make_ns_record(5, 5, Some(make_cid("commit:5")), Some(local_idx.clone()));
        let plan = UpdatePlan::plan(10, 5, Some(&local_idx), &ns);
        assert_eq!(plan, UpdatePlan::Noop);
    }

    #[test]
    fn test_update_plan_reload_when_commit_next_missing_cid() {
        // ns.commit_t == local.t() + 1 but no commit_head_id -> Reload (safety)
        let local_idx = make_index_cid("index:5");
        let ns = make_ns_record(11, 5, None, Some(local_idx.clone()));
        let plan = UpdatePlan::plan(10, 5, Some(&local_idx), &ns);
        assert_eq!(plan, UpdatePlan::Reload);
    }

    #[test]
    fn test_update_plan_helpers() {
        assert!(UpdatePlan::Noop.is_noop());
        assert!(!UpdatePlan::Reload.is_noop());

        assert!(UpdatePlan::Reload.requires_reload());
        assert!(!UpdatePlan::Noop.requires_reload());
    }

    // ========================================================================
    // Error propagation tests - verify status codes are preserved for waiters
    // ========================================================================

    #[test]
    fn test_error_status_code_preservation() {
        // Verify that ApiError::http preserves status codes correctly
        // This is the mechanism used for waiter error propagation

        // NotFound should map to 404
        let not_found = ApiError::NotFound("ledger foo".to_string());
        assert_eq!(not_found.status_code(), 404);

        // When converted for waiters via http(), status should be preserved
        let http_not_found = ApiError::http(not_found.status_code(), not_found.to_string());
        assert_eq!(http_not_found.status_code(), 404);

        // LedgerExists should map to 409
        let exists = ApiError::LedgerExists("ledger foo".to_string());
        assert_eq!(exists.status_code(), 409);

        let http_exists = ApiError::http(exists.status_code(), exists.to_string());
        assert_eq!(http_exists.status_code(), 409);

        // Internal should map to 500
        let internal = ApiError::internal("something failed");
        assert_eq!(internal.status_code(), 500);

        let http_internal = ApiError::http(internal.status_code(), internal.to_string());
        assert_eq!(http_internal.status_code(), 500);
    }

    #[tokio::test]
    async fn test_disconnect_all_clears_entries() {
        use fluree_db_core::MemoryStorage;
        use fluree_db_nameservice::memory::MemoryNameService;

        let storage = MemoryStorage::new();
        let backend = StorageBackend::Managed(Arc::new(storage));
        let ns = MemoryNameService::new();
        let ns_mode = crate::NameServiceMode::ReadWrite(Arc::new(ns));
        let config = LedgerManagerConfig::default();
        let mgr = LedgerManager::new(backend, ns_mode, config);

        // Directly insert Loading entries (simulates in-flight loads)
        {
            let mut entries = mgr.entries.write().await;
            entries.insert("ledger_a:main".to_string(), LoadState::Loading(Vec::new()));
            entries.insert("ledger_b:main".to_string(), LoadState::Loading(Vec::new()));
        }

        // Verify entries exist
        {
            let entries = mgr.entries.read().await;
            assert_eq!(entries.len(), 2);
        }

        // disconnect_all should clear everything
        mgr.disconnect_all().await;

        {
            let entries = mgr.entries.read().await;
            assert_eq!(entries.len(), 0);
        }
    }

    #[tokio::test]
    async fn test_shutdown_flag_prevents_reinsertion() {
        use fluree_db_core::MemoryStorage;
        use fluree_db_nameservice::memory::MemoryNameService;

        let storage = MemoryStorage::new();
        let backend = StorageBackend::Managed(Arc::new(storage));
        let ns = MemoryNameService::new();
        let ns_mode = crate::NameServiceMode::ReadWrite(Arc::new(ns));
        let config = LedgerManagerConfig::default();
        let mgr = LedgerManager::new(backend, ns_mode, config);

        // Simulate: disconnect_all sets shutdown flag and clears entries
        mgr.disconnect_all().await;
        assert!(mgr.is_shutdown());

        // Simulate a load leader completing after shutdown by directly inserting
        // (this mimics what get_or_load's publish path would do without the guard)
        {
            let mut entries = mgr.entries.write().await;
            // The shutdown guard in get_or_load checks is_shutdown() before inserting.
            // Verify the flag is set so the guard would skip insertion.
            if !mgr.shutdown.load(Ordering::Acquire) {
                entries.insert(
                    "should_not_appear:main".to_string(),
                    LoadState::Loading(Vec::new()),
                );
            }
        }

        // Entries should still be empty because shutdown flag was set
        {
            let entries = mgr.entries.read().await;
            assert_eq!(entries.len(), 0);
        }
    }

    #[test]
    fn test_waiter_error_arc_extraction() {
        // Simulate the waiter error extraction pattern
        let original = ApiError::NotFound("ledger bar".to_string());
        let arc_error = Arc::new(ApiError::http(original.status_code(), original.to_string()));

        // Extract like a waiter would
        let extracted = match arc_error.as_ref() {
            ApiError::Http { status, message } => ApiError::http(*status, message),
            other => ApiError::http(other.status_code(), other.to_string()),
        };

        // Status code should be preserved (404, not 500)
        assert_eq!(extracted.status_code(), 404);
        assert!(extracted.to_string().contains("ledger bar"));
    }
}
