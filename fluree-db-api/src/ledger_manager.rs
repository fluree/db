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
use tokio::sync::{oneshot, RwLock};

use crate::error::{ApiError, Result};
use crate::ledger_view::LedgerView;

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
// LedgerWriteGuard - Holds mutex for transaction duration
// ============================================================================

/// Write guard that holds the ledger mutex for transaction duration
///
/// Transactions hold this guard across stage+commit to serialize writes
/// to the same ledger.
pub struct LedgerWriteGuard<'a> {
    guard: tokio::sync::RwLockWriteGuard<'a, LedgerState>,
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
    /// Guards all access to the ledger state. A `RwLock` so concurrent reads
    /// (every query takes a brief shared `read()` to clone a cheap, Arc-backed
    /// snapshot) run in parallel instead of serializing; transactions take an
    /// exclusive `write()` for the stage+commit duration.
    state: RwLock<LedgerState>,
    /// Ledger ID (e.g., "mydb:main")
    ledger_id: String,
    /// Last access time (monotonic secs since process start)
    last_access: AtomicU64,
    /// Binary columnar index store (v2 only).
    ///
    /// Always coherent with `state.snapshot.range_provider` — writers hold
    /// the `state` lock while updating this.
    binary_store: RwLock<Option<Arc<BinaryIndexStore>>>,
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
                state: RwLock::new(state),
                ledger_id,
                last_access: AtomicU64::new(monotonic_secs()),
                binary_store: RwLock::new(binary_store),
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
    pub async fn snapshot(&self) -> LedgerView {
        self.touch();
        let state = self.inner.state.read().await;
        let binary_store = self.inner.binary_store.read().await.clone();
        let mut snap = LedgerView::from_state(&state);
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
            guard: self.inner.state.write().await,
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
        *self.inner.binary_store.write().await = binary_store;
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
        self.inner.state.try_write().is_err()
    }

    /// Get current index_t (brief lock to read)
    ///
    /// This returns the indexed DB's t value, NOT including novelty.
    /// For freshness checking against remote watermarks, use this method.
    pub async fn index_t(&self) -> i64 {
        let state = self.inner.state.read().await;
        state.index_t()
    }

    /// Get current t value (max of db.t and novelty.t)
    ///
    /// This returns the ledger's current t including any unindexed novelty.
    /// Use this for comparing against nameservice commit_t to detect staleness.
    pub async fn t(&self) -> i64 {
        let state = self.inner.state.read().await;
        state.t()
    }

    /// Get state metrics for update planning
    ///
    /// Returns (t, index_t, index_head_id) needed for UpdatePlan::plan()
    pub async fn state_metrics(&self) -> (i64, i64, Option<ContentId>) {
        let state = self.inner.state.read().await;
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
            has_annotations: root.has_annotations,
            annotation_index: root.annotation_index.clone(),
            had_annotation_arena: root.had_annotation_arena,
        };
        let db = LedgerSnapshot::new_meta(meta)
            .map_err(|e| ApiError::internal(format!("graph registry from root: {e}")))?;

        // Brief lock: apply snapshot (trims novelty, rebuilds dict_novelty),
        // then wire up range_provider with the correct dict_novelty.
        // Lock ordering: state → binary_store (same as snapshot()).
        {
            let mut state = self.inner.state.write().await;

            // apply_loaded_db: validates, trims novelty, rebuilds dict_novelty
            state
                .apply_loaded_db(db, Some(index_id))
                .map_err(|e| ApiError::internal(format!("apply_loaded_db failed: {e}")))?;

            // Sync namespace codes between store and snapshot (bimap validation).
            crate::ns_helpers::sync_store_and_snapshot_ns(
                &mut store,
                Arc::make_mut(&mut state.snapshot),
            )?;

            let arc_store = Arc::new(store);
            crate::runtime_dicts::reseed_runtime_small_dicts(&mut state, &arc_store);

            // Build range_provider with the real dict_novelty (rebuilt by apply_loaded_db)
            let ns_fallback = Some(state.snapshot.shared_namespaces());
            let provider = BinaryRangeProvider::new(
                Arc::clone(&arc_store),
                Arc::clone(&state.dict_novelty),
                Arc::clone(&state.runtime_small_dicts),
                ns_fallback,
            );
            let snap = Arc::make_mut(&mut state.snapshot);
            snap.range_provider = Some(Arc::new(provider));
            // Plumb the CAS handle so arena-backed annotation reads can
            // resolve `AnnotationIndexRoot.{forward,reverse}_branch_cid`.
            // Without this, `LedgerSnapshot::has_arena_reader()` always
            // returns false and the formatter / cascade falls back to
            // the M2a scan path even on snapshots with on-disk arenas.
            snap.content_store = Some(Arc::clone(&cs));

            let te_store: Arc<dyn std::any::Any + Send + Sync> = arc_store.clone();
            state.binary_store = Some(TypeErasedStore(te_store));
            *self.inner.binary_store.write().await = Some(arc_store);
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
    /// Initial load in progress - waiters receive handle on success.
    ///
    /// `generation` is a per-slot token allocated from
    /// [`LedgerManager::load_generation`] when the leader inserts this entry.
    /// A [`LoadingLeaderGuard`]'s detached cleanup removes the slot only if the
    /// current `Loading` carries the same generation, so a stale guard can
    /// never clobber a *new* leader's slot that was inserted after the original
    /// orphan was cleared (e.g. by `disconnect`).
    Loading {
        generation: u64,
        waiters: Vec<oneshot::Sender<std::result::Result<LedgerHandle, Arc<ApiError>>>>,
    },
    /// Loaded and cached
    Ready(LedgerHandle),
    /// Reload in progress - handle stays valid, waiters receive () on success.
    ///
    /// `generation` is a per-slot token (same source as `Loading`) so a
    /// [`ReloadLeaderGuard`]'s cleanup only reclaims its own orphaned slot.
    Reloading {
        generation: u64,
        handle: LedgerHandle,
        waiters: Vec<oneshot::Sender<std::result::Result<(), Arc<ApiError>>>>,
    },
}

// ============================================================================
// LoadingLeaderGuard - cancellation safety for single-flight loads
// ============================================================================

/// Cancellation-safety guard for a single-flight load leader in
/// [`LedgerManager::get_or_load`].
///
/// The leader inserts [`LoadState::Loading`] before awaiting load I/O and only
/// clears it on the publish path. If the leader future is dropped (cancelled)
/// in between — an HTTP handler future dropped on client disconnect, an
/// aborted task, an outer `tokio::time::timeout` — the `Loading` slot would be
/// orphaned forever: [`LedgerManager::sweep_idle`] deliberately never evicts
/// `Loading`, so every later caller for that ledger would park on `rx.await`
/// indefinitely (a permanent per-ledger wedge).
///
/// On drop-before-publish this guard reclaims the orphaned slot. Removing the
/// entry drops the queued waiter `oneshot` senders, so parked waiters receive
/// `RecvError` (surfaced as `ApiError::internal("load cancelled")`) and the
/// next caller re-elects a fresh leader. The leader calls [`Self::disarm`]
/// once it has published (success or error), making normal completion a no-op.
struct LoadingLeaderGuard {
    entries: Arc<RwLock<HashMap<String, LoadState>>>,
    alias: String,
    /// Generation of the `Loading` slot this leader inserted. Cleanup removes
    /// the slot only when it still carries this generation (ABA protection).
    generation: u64,
    armed: bool,
}

impl LoadingLeaderGuard {
    /// Mark the load as published; the subsequent drop becomes a no-op.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for LoadingLeaderGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Drop is synchronous but the `entries` lock is async, so hand the
        // cleanup to a detached task. A runtime context is present whenever a
        // leader future is dropped on a worker thread (poll-time cancellation,
        // task abort, or outer-future drop); if there is none, there is no
        // runtime left to wedge.
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let entries = Arc::clone(&self.entries);
        let alias = std::mem::take(&mut self.alias);
        let generation = self.generation;
        rt.spawn(async move {
            let mut entries = entries.write().await;
            // Only reclaim OUR still-`Loading` slot. A generation mismatch means
            // the original orphan was already cleared (e.g. by `disconnect`) and
            // a new leader inserted a fresh slot — never clobber that, nor a
            // `Ready`/`Reloading` entry a later leader published.
            if matches!(
                entries.get(&alias),
                Some(LoadState::Loading { generation: g, .. }) if *g == generation
            ) {
                entries.remove(&alias);
            }
        });
    }
}

/// Cancellation-safety guard for a reload leader in [`LedgerManager::reload`].
///
/// `reload` transitions `Ready(h) → Reloading{h, waiters}` and only clears it
/// on the publish path. A dropped reload-leader future would otherwise orphan
/// the `Reloading` slot forever; unlike `Loading` this does NOT wedge query
/// reads (`get_or_load` returns the still-valid handle for `Reloading`), but it
/// permanently stalls future `reload`s and `current_t` for that ledger.
///
/// On drop-before-publish this guard evicts its own orphaned slot (generation
/// match), dropping the waiter senders so parked reloaders get `RecvError`
/// (surfaced as `ApiError::internal("reload cancelled")`). Eviction is safe —
/// the next query cold-loads the ledger fresh — and avoids re-inserting after a
/// concurrent `disconnect`/shutdown. The leader calls [`Self::disarm`] once it
/// has published.
struct ReloadLeaderGuard {
    entries: Arc<RwLock<HashMap<String, LoadState>>>,
    alias: String,
    generation: u64,
    armed: bool,
}

impl ReloadLeaderGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ReloadLeaderGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let entries = Arc::clone(&self.entries);
        let alias = std::mem::take(&mut self.alias);
        let generation = self.generation;
        rt.spawn(async move {
            let mut entries = entries.write().await;
            // Only reclaim OUR still-`Reloading` slot (generation match), so a
            // stale guard never clobbers a fresh slot inserted after the orphan
            // was cleared.
            if matches!(
                entries.get(&alias),
                Some(LoadState::Reloading { generation: g, .. }) if *g == generation
            ) {
                entries.remove(&alias);
            }
        });
    }
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
    {
        let snap = Arc::make_mut(&mut state.snapshot);
        snap.subject_watermarks = root.subject_watermarks;
        snap.string_watermark = root.string_watermark;
        if root.stats.is_some() && snap.stats.is_none() {
            snap.stats = root.stats;
            tracing::debug!("loaded stats from FIR6 root");
        }
        if root.schema.is_some() && snap.schema.is_none() {
            snap.schema = root.schema;
            tracing::debug!("loaded schema from FIR6 root");
        }
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
    crate::ns_helpers::sync_store_and_snapshot_ns(&mut store, Arc::make_mut(&mut state.snapshot))?;

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
    let ns_fallback = Some(state.snapshot.shared_namespaces());
    let provider = BinaryRangeProvider::new(
        Arc::clone(&arc_store),
        Arc::clone(&state.dict_novelty),
        Arc::clone(&state.runtime_small_dicts),
        ns_fallback,
    );
    // Always rebuild the provider here so it is coherent with the freshly
    // loaded BinaryIndexStore, DictNovelty, and runtime dictionary state.
    let snap = Arc::make_mut(&mut state.snapshot);
    snap.range_provider = Some(Arc::new(provider));
    // Plumb the CAS handle so arena-backed annotation reads can resolve
    // `AnnotationIndexRoot.{forward,reverse}_branch_cid`. Mirror of the
    // identical line in `apply_index_v2` — fresh-load path needs the
    // same wiring as the cache-update path or `has_arena_reader()`
    // would always be false on snapshots loaded outside the
    // LedgerManager handle path.
    snap.content_store = Some(Arc::clone(&cs));
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
/// Coverage envelope returned alongside the running ledger's
/// attachment events.
///
/// Distinguishes "we walked every commit since genesis" (safe to
/// publish as `Authoritative`) from "we only have the post-index
/// tail" (must be merged with a base arena via `Augment`).
#[derive(Debug, Clone, Copy)]
pub enum RunningCoverage {
    /// Snapshot.t == 0: no index has ever run, so the running
    /// `AttachmentNovelty` was built by walking every commit since
    /// genesis. Provider can return `Authoritative`.
    Authoritative,
    /// Snapshot.t > 0: an index has run. The running
    /// `AttachmentNovelty` may be the full history (continuously-
    /// running ledger) or only the post-index tail (after a
    /// reload). We can't distinguish, so the provider must return
    /// `Augment`.
    Augment,
}

/// Result of `LedgerManager::try_running_attachment_events`.
#[derive(Debug, Clone)]
pub struct RunningAttachmentEvents {
    pub coverage: RunningCoverage,
    pub events: Vec<(fluree_db_core::EdgeKey, fluree_db_core::Sid, i64, bool)>,
}

pub struct LedgerManager {
    /// Cached ledger handles + loading state
    ///
    /// `Arc` so a [`LoadingLeaderGuard`] can reclaim an orphaned `Loading`
    /// slot from a detached cleanup task if a load leader future is cancelled.
    entries: Arc<RwLock<HashMap<String, LoadState>>>,
    /// Storage backend for ledger loading
    backend: StorageBackend,
    /// Shared cache for index nodes
    /// Nameservice for ledger lookup/loading
    nameservice_mode: crate::NameServiceMode,
    /// Configuration
    config: LedgerManagerConfig,
    /// Shutdown flag — prevents load/reload leaders from re-inserting after disconnect_all
    shutdown: AtomicBool,
    /// Monotonic generation source for `LoadState::Loading` slots. Lets a
    /// [`LoadingLeaderGuard`]'s detached cleanup distinguish its own orphaned
    /// slot from a fresh slot inserted by a later leader (ABA protection).
    load_generation: AtomicU64,
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
            entries: Arc::new(RwLock::new(HashMap::new())),
            backend,
            nameservice_mode: nameservice,
            config,
            shutdown: AtomicBool::new(false),
            load_generation: AtomicU64::new(0),
        }
    }

    /// Get the manager configuration
    pub fn config(&self) -> &LedgerManagerConfig {
        &self.config
    }

    /// Snapshot the running ledger's attachment-event delta in the
    /// shape the indexer's arena builder expects, plus the coverage
    /// envelope describing what the events span.
    ///
    /// Returns `None` when:
    /// - the ledger isn't currently loaded into this manager (no
    ///   running overlay to snapshot — the indexer treats this as
    ///   "delta unknown" and defensively drops any base arena),
    /// - the ledger is loading (we don't block the indexer's job
    ///   dispatch on a load).
    ///
    /// Returns `Some(vec)` (possibly empty) when the snapshot was
    /// observed cleanly — the empty case explicitly asserts "no
    /// events since the base arena," which the indexer treats as
    /// "delta is empty" and seals an authoritative (unchanged)
    /// arena.
    pub async fn try_running_attachment_events(
        &self,
        ledger_id: &str,
    ) -> Option<RunningAttachmentEvents> {
        let canonical_alias =
            normalize_ledger_id(ledger_id).unwrap_or_else(|_| ledger_id.to_string());
        let entries = self.entries.read().await;
        let entry = entries.get(&canonical_alias)?;
        let LoadState::Ready(handle) = entry else {
            return None;
        };
        let view = handle.snapshot().await;
        // Coverage heuristic: when the snapshot's `t` is zero, no
        // index has ever run on this ledger, so the running
        // `AttachmentNovelty` was built by walking every commit
        // since genesis — it carries the complete event history.
        // Once `snapshot.t > 0`, we can't distinguish a continuously-
        // running ledger (full history preserved across reindexes)
        // from a reloaded one (only post-index tail in the overlay),
        // so the safe call is `Augment`.
        let coverage = if view.snapshot.t == 0 {
            RunningCoverage::Authoritative
        } else {
            RunningCoverage::Augment
        };
        let events: Vec<_> = view.novelty.attachments.iter_event_pairs().collect();
        Some(RunningAttachmentEvents { coverage, events })
    }

    /// Return a read-only `LedgerView` for a currently-loaded ledger
    /// without forcing a load. Returns `None` when the ledger isn't
    /// in the cache.
    ///
    /// Used by `ApiAttachmentEventsProvider`'s bulk-import seal path:
    /// when the running overlay reports no events but the snapshot's
    /// sticky bit says annotations exist (the post-import state),
    /// the provider needs the snapshot + range_provider to scan the
    /// base index for `f:reifies*` flakes itself.
    pub async fn get_loaded_view(&self, ledger_id: &str) -> Option<LedgerView> {
        let canonical_alias =
            normalize_ledger_id(ledger_id).unwrap_or_else(|_| ledger_id.to_string());
        let entries = self.entries.read().await;
        let entry = entries.get(&canonical_alias)?;
        let LoadState::Ready(handle) = entry else {
            return None;
        };
        Some(handle.snapshot().await)
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
        let (rx, generation) = {
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
                Some(LoadState::Loading { waiters, .. }) => {
                    // Someone else is loading - add ourselves as waiter
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    (Some(rx), 0)
                }
                None => {
                    // We're first - mark as loading (with a fresh generation),
                    // release lock, do I/O.
                    let generation = self.load_generation.fetch_add(1, Ordering::Relaxed);
                    entries.insert(
                        canonical_alias.clone(),
                        LoadState::Loading {
                            generation,
                            waiters: Vec::new(),
                        },
                    );
                    (None, generation)
                }
            }
        };
        // Manager lock released here

        if let Some(rx) = rx {
            // Phase instrumentation (debug!): a waiter parked here with
            // no matching leader publish is the orphaned-Loading signature.
            tracing::debug!(alias = %canonical_alias, "ledger.load.waiter.park");
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

        // We're the loader. Arm a guard over the `Loading` slot: if this
        // leader future is cancelled before it publishes (HTTP handler dropped
        // on client disconnect, task abort, outer timeout), the guard reclaims
        // the slot so waiters unblock and a fresh caller re-elects a leader —
        // otherwise the slot orphans forever and wedges the ledger.
        let mut leader_guard = LoadingLeaderGuard {
            entries: Arc::clone(&self.entries),
            alias: canonical_alias.clone(),
            generation,
            armed: true,
        };

        // Phase instrumentation (debug!): leader load phases localize a hang to
        // nameservice (DDB, no default timeout) vs storage (S3, 35s/attempt) vs
        // the binary-store attach.
        tracing::debug!(alias = %canonical_alias, "ledger.load.leader.begin");

        // Do ALL load I/O — including the binary index store attach, which
        // performs S3 reads — WITHOUT holding the manager lock. Holding the
        // `entries` write lock across that I/O previously turned the entire
        // ledger cache into a global mutex for the duration of any cold load.
        // Note: we pass the original address to nameservice (it handles
        // resolution), but cache under the canonical address.
        let load_result = LedgerState::load(&self.nameservice_mode, ledger_id, &self.backend)
            .await
            .map_err(ApiError::from); // Convert LedgerError to ApiError
        tracing::debug!(
            alias = %canonical_alias,
            ok = load_result.is_ok(),
            "ledger.load.state.done"
        );

        let publish = match load_result {
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
                tracing::debug!(alias = %canonical_alias, "ledger.load.binary_store.done");
                Ok(LedgerHandle::new(
                    canonical_alias.clone(),
                    state,
                    binary_store,
                ))
            }
            Err(e) => Err(e),
        };

        // Phase instrumentation (debug!): leader reached publish.
        tracing::debug!(
            alias = %canonical_alias,
            ok = publish.is_ok(),
            "ledger.load.leader.publish"
        );

        // Publish under a brief lock. There is no `.await` between acquiring
        // the lock and disarming the guard, so the leader cannot be cancelled
        // mid-publish: either the guard already fired (we never reached here)
        // or the publish runs to completion in this poll.
        let mut entries = self.entries.write().await;
        let shutting_down = self.is_shutdown();
        leader_guard.disarm();

        // We only own the slot — and may remove/notify/cache it — if it is
        // still OUR `Loading` (generation match). If `disconnect` cleared it and
        // a newer leader inserted a fresh `Loading`, that slot belongs to the
        // newer leader: publishing over it would clobber it and notify its
        // waiters with our stale result. In that case we just return our own
        // result to the direct caller without touching the cache. (The
        // generation also guards the detached drop cleanup.)
        let owns_slot = matches!(
            entries.get(&canonical_alias),
            Some(LoadState::Loading { generation: g, .. }) if *g == generation
        );

        match publish {
            Ok(handle) => {
                if owns_slot {
                    if let Some(LoadState::Loading { waiters, .. }) =
                        entries.remove(&canonical_alias)
                    {
                        for tx in waiters {
                            let _ = tx.send(Ok(handle.clone()));
                        }
                    }
                    // Don't re-insert into cache if shutdown has been initiated
                    if !shutting_down {
                        entries.insert(canonical_alias, LoadState::Ready(handle.clone()));
                    }
                }
                Ok(handle)
            }
            Err(e) => {
                // Capture error with status code for waiters before consuming the error
                // Note: Waiters receive an Http error (preserving status code);
                // the leader (first caller) gets the original error type preserved.
                if owns_slot {
                    let error_for_waiters =
                        Arc::new(ApiError::http(e.status_code(), e.to_string()));
                    if let Some(LoadState::Loading { waiters, .. }) =
                        entries.remove(&canonical_alias)
                    {
                        for tx in waiters {
                            let _ = tx.send(Err(Arc::clone(&error_for_waiters)));
                        }
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
            BecomeLeader {
                handle: LedgerHandle,
                generation: u64,
            },
            WaitForReload(oneshot::Receiver<std::result::Result<(), Arc<ApiError>>>),
            WaitForInitialLoad(oneshot::Receiver<std::result::Result<LedgerHandle, Arc<ApiError>>>),
            NotLoaded,
        }

        // Determine action under lock
        let action = {
            let mut entries = self.entries.write().await;

            match entries.get_mut(&canonical_alias) {
                Some(LoadState::Ready(h)) => {
                    // Transition to Reloading, become leader. Tag the slot with
                    // a fresh generation so a cancelled leader's guard reclaims
                    // only its own slot.
                    let handle = h.clone();
                    let generation = self.load_generation.fetch_add(1, Ordering::Relaxed);
                    let reloading = LoadState::Reloading {
                        generation,
                        handle: handle.clone(),
                        waiters: Vec::new(),
                    };
                    entries.insert(canonical_alias.clone(), reloading);
                    ReloadAction::BecomeLeader { handle, generation }
                }
                Some(LoadState::Reloading { waiters, .. }) => {
                    // Join existing reload
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    ReloadAction::WaitForReload(rx)
                }
                Some(LoadState::Loading { waiters, .. }) => {
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

            ReloadAction::BecomeLeader { handle, generation } => {
                // Guard the Reloading slot: a cancelled reload-leader future
                // would otherwise orphan it (stalling future reloads/current_t).
                let mut reload_guard = ReloadLeaderGuard {
                    entries: Arc::clone(&self.entries),
                    alias: canonical_alias.clone(),
                    generation,
                    armed: true,
                };

                // We're the reload leader. Build the replacement state + binary
                // store WITHOUT holding the handle `state` lock, so concurrent
                // queries (snapshot() -> state.lock()) are not blocked behind the
                // reload's nameservice/S3/index I/O. The lock is taken only for
                // the brief coherent swap below. The `entries` lock is likewise
                // not held over any of this and is acquired after the swap, so
                // the two locks never overlap (avoids the entries↔state ordering
                // hazard with `current_t`).
                let loaded = LedgerState::load(&self.nameservice_mode, ledger_id, &self.backend)
                    .await
                    .map_err(ApiError::from);
                let result = match loaded {
                    Ok(mut new_state) => {
                        // Attempt to load binary index store (v2 only) — still off-lock.
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

                        // Brief coherent swap. Lock order: state -> binary_store;
                        // acquire both before mutating so there is no `.await`
                        // between the two assignments (no incoherent
                        // state/binary_store window on cancellation, and readers
                        // blocked on `state` never observe a half-applied swap).
                        let mut write_guard = handle.lock_for_write().await;
                        // Compare total `t()` (novelty included), not `index_t()`:
                        // this is deliberately novelty-protective. If the reloaded
                        // state carries a newer persisted index but an older total
                        // `t` than the in-memory state, we skip the swap and keep
                        // the fresher novelty — transiently forgoing the newer
                        // index, which the next reload picks up. Never clobber a
                        // newer in-memory commit to adopt a newer index.
                        if new_state.t() >= write_guard.state().t() {
                            let mut bs_guard = handle.inner.binary_store.write().await;
                            write_guard.replace(new_state);
                            *bs_guard = new_binary_store;
                        } else {
                            // A concurrent commit advanced the in-memory state
                            // past the reloaded storage HEAD (a txn took the
                            // still-valid handle via get_or_load's Reloading
                            // fast path while we loaded). Keep the fresher
                            // in-memory state rather than clobber the commit.
                            tracing::debug!(
                                ledger_id = %ledger_id,
                                "reload: in-memory state newer than reloaded storage; skipping swap"
                            );
                        }
                        Ok(())
                        // state (and binary_store) guards dropped here, before `entries`.
                    }
                    Err(e) => Err(e),
                };

                // Publish under a brief `entries` lock — no `.await` between the
                // lock and disarm, so the leader can't be cancelled mid-publish.
                let mut entries = self.entries.write().await;
                let shutting_down = self.is_shutdown();
                reload_guard.disarm();

                // Only publish if WE still own the slot (our `Reloading`
                // generation). If `disconnect` cleared it and a newer reload
                // inserted a fresh `Reloading`, that slot belongs to the newer
                // leader; removing/notifying/restoring over it would deliver our
                // stale success/error to its waiters and clobber it. In that
                // case do nothing and return our own result to the caller.
                let owns_slot = matches!(
                    entries.get(&canonical_alias),
                    Some(LoadState::Reloading { generation: g, .. }) if *g == generation
                );

                match result {
                    Ok(()) => {
                        // Notify waiters and restore Ready state (unless shutting down)
                        if owns_slot {
                            if let Some(LoadState::Reloading {
                                handle, waiters, ..
                            }) = entries.remove(&canonical_alias)
                            {
                                for tx in waiters {
                                    let _ = tx.send(Ok(()));
                                }
                                if !shutting_down {
                                    entries.insert(canonical_alias, LoadState::Ready(handle));
                                }
                            }
                        }
                        Ok(())
                    }
                    Err(e) => {
                        // Capture error with status code for waiters before consuming the error
                        // Note: Waiters receive Http error (preserving status code); leader gets original type
                        if owns_slot {
                            let error_for_waiters =
                                Arc::new(ApiError::http(e.status_code(), e.to_string()));
                            // Notify waiters of failure, restore Ready (keep old data) unless shutting down
                            if let Some(LoadState::Reloading {
                                handle, waiters, ..
                            }) = entries.remove(&canonical_alias)
                            {
                                for tx in waiters {
                                    let _ = tx.send(Err(Arc::clone(&error_for_waiters)));
                                }
                                if !shutting_down {
                                    entries.insert(canonical_alias, LoadState::Ready(handle));
                                }
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
                                Some(write_guard.state().snapshot.shared_namespaces());
                            Arc::make_mut(&mut write_guard.state_mut().snapshot).range_provider =
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
        // Clone the handle out and drop the `entries` read guard BEFORE locking
        // the handle `state` (via state_metrics): never hold `entries` across a
        // `state` lock, so there is no entries↔state acquisition-order pair to
        // deadlock with writers. Mirrors `notify`.
        let handle = {
            let entries = self.entries.read().await;
            match entries.get(ledger_id) {
                Some(LoadState::Ready(handle)) => handle.clone(),
                Some(LoadState::Reloading { handle, .. }) => handle.clone(),
                _ => return None,
            }
        };
        let (t, _, _) = handle.state_metrics().await;
        Some(t)
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
            entries.insert(
                "ledger_a:main".to_string(),
                LoadState::Loading {
                    generation: 0,
                    waiters: Vec::new(),
                },
            );
            entries.insert(
                "ledger_b:main".to_string(),
                LoadState::Loading {
                    generation: 0,
                    waiters: Vec::new(),
                },
            );
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
                    LoadState::Loading {
                        generation: 0,
                        waiters: Vec::new(),
                    },
                );
            }
        }

        // Entries should still be empty because shutdown flag was set
        {
            let entries = mgr.entries.read().await;
            assert_eq!(entries.len(), 0);
        }
    }

    // ========================================================================
    // Cancellation-safety tests - LoadingLeaderGuard
    // ========================================================================

    fn make_test_manager() -> LedgerManager {
        use fluree_db_core::MemoryStorage;
        use fluree_db_nameservice::memory::MemoryNameService;
        let backend = StorageBackend::Managed(Arc::new(MemoryStorage::new()));
        let ns_mode = crate::NameServiceMode::ReadWrite(Arc::new(MemoryNameService::new()));
        LedgerManager::new(backend, ns_mode, LedgerManagerConfig::default())
    }

    /// Regression: a leader future cancelled mid-load must NOT orphan its
    /// `Loading` slot. Before the guard, the slot persisted forever (sweep_idle
    /// never evicts Loading) and every later caller wedged on `rx.await`.
    #[tokio::test]
    async fn test_cancelled_leader_does_not_orphan_loading_slot() {
        let mgr = make_test_manager();
        {
            let mut entries = mgr.entries.write().await;
            entries.insert(
                "x:main".to_string(),
                LoadState::Loading {
                    generation: 0,
                    waiters: Vec::new(),
                },
            );
        }
        // Simulate a leader that inserted Loading then was dropped before publish.
        {
            let _guard = LoadingLeaderGuard {
                entries: Arc::clone(&mgr.entries),
                alias: "x:main".to_string(),
                generation: 0,
                armed: true,
            };
            // dropped here WITHOUT disarm() == cancelled leader
        }
        // Cleanup is detached; give it a few scheduler turns to run.
        for _ in 0..20 {
            tokio::task::yield_now().await;
            if mgr.entries.read().await.get("x:main").is_none() {
                break;
            }
        }
        assert!(
            mgr.entries.read().await.get("x:main").is_none(),
            "orphaned Loading slot must be reclaimed when the leader future is dropped"
        );
    }

    /// A waiter parked on the orphaned slot must unblock (RecvError) when the
    /// cancelled leader's guard reclaims the slot — not hang to the 900s limit.
    #[tokio::test]
    async fn test_waiter_unblocks_when_leader_cancelled() {
        let mgr = make_test_manager();
        let (tx, rx) = oneshot::channel::<std::result::Result<LedgerHandle, Arc<ApiError>>>();
        {
            let mut entries = mgr.entries.write().await;
            entries.insert(
                "x:main".to_string(),
                LoadState::Loading {
                    generation: 0,
                    waiters: vec![tx],
                },
            );
        }
        {
            let _guard = LoadingLeaderGuard {
                entries: Arc::clone(&mgr.entries),
                alias: "x:main".to_string(),
                generation: 0,
                armed: true,
            };
        }
        // Removing the slot drops the sender -> the waiter observes RecvError
        // instead of hanging forever.
        let res = tokio::time::timeout(Duration::from_secs(2), rx)
            .await
            .expect("waiter must not hang after the leader is cancelled");
        assert!(
            res.is_err(),
            "waiter should receive RecvError once the orphaned slot is reclaimed"
        );
    }

    /// A leader that publishes (disarms) must leave its slot intact — the
    /// guard's drop is a no-op on the success path.
    #[tokio::test]
    async fn test_disarmed_guard_leaves_slot_intact() {
        let mgr = make_test_manager();
        {
            let mut entries = mgr.entries.write().await;
            entries.insert(
                "x:main".to_string(),
                LoadState::Loading {
                    generation: 0,
                    waiters: Vec::new(),
                },
            );
        }
        {
            let mut guard = LoadingLeaderGuard {
                entries: Arc::clone(&mgr.entries),
                alias: "x:main".to_string(),
                generation: 0,
                armed: true,
            };
            guard.disarm();
        }
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        assert!(
            mgr.entries.read().await.get("x:main").is_some(),
            "a disarmed guard must not touch the slot"
        );
    }

    /// ABA protection: a stale guard whose original slot was already cleared
    /// (e.g. by `disconnect`) and replaced by a NEW leader's `Loading` slot
    /// (different generation) must NOT remove the new leader's slot.
    #[tokio::test]
    async fn test_stale_guard_does_not_clobber_new_leader_slot() {
        let mgr = make_test_manager();
        // A new leader has inserted a fresh slot with generation 7.
        {
            let mut entries = mgr.entries.write().await;
            entries.insert(
                "x:main".to_string(),
                LoadState::Loading {
                    generation: 7,
                    waiters: Vec::new(),
                },
            );
        }
        // A stale guard from a previous, already-cleared load (generation 3)
        // is dropped now.
        {
            let _guard = LoadingLeaderGuard {
                entries: Arc::clone(&mgr.entries),
                alias: "x:main".to_string(),
                generation: 3,
                armed: true,
            };
        }
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        // The new leader's slot (gen 7) must survive the stale guard's cleanup.
        let entries = mgr.entries.read().await;
        assert!(
            matches!(
                entries.get("x:main"),
                Some(LoadState::Loading { generation: 7, .. })
            ),
            "stale guard (gen 3) must not remove the new leader's slot (gen 7)"
        );
    }

    // ========================================================================
    // Cancellation-safety tests - ReloadLeaderGuard
    // ========================================================================

    /// Build a minimal cached handle for guard tests (genesis snapshot, empty
    /// novelty, no binary store).
    fn make_test_handle(alias: &str) -> LedgerHandle {
        use fluree_db_core::db::LedgerSnapshot;
        use fluree_db_novelty::Novelty;
        let state = LedgerState::new(LedgerSnapshot::genesis(alias), Novelty::new(0));
        LedgerHandle::new(alias.to_string(), state, None)
    }

    /// Regression: a reload-leader future cancelled mid-load must not orphan its
    /// `Reloading` slot (which would stall future reloads/current_t).
    #[tokio::test]
    async fn test_cancelled_reload_leader_reclaims_reloading_slot() {
        let mgr = make_test_manager();
        let handle = make_test_handle("x:main");
        {
            let mut entries = mgr.entries.write().await;
            entries.insert(
                "x:main".to_string(),
                LoadState::Reloading {
                    generation: 5,
                    handle: handle.clone(),
                    waiters: Vec::new(),
                },
            );
        }
        {
            let _guard = ReloadLeaderGuard {
                entries: Arc::clone(&mgr.entries),
                alias: "x:main".to_string(),
                generation: 5,
                armed: true,
            };
            // dropped WITHOUT disarm() == cancelled reload leader
        }
        for _ in 0..20 {
            tokio::task::yield_now().await;
            if mgr.entries.read().await.get("x:main").is_none() {
                break;
            }
        }
        assert!(
            mgr.entries.read().await.get("x:main").is_none(),
            "orphaned Reloading slot must be reclaimed when the reload leader is dropped"
        );
    }

    /// ABA: a stale reload guard must not clobber a fresh slot (different
    /// generation, or a `Loading`/`Ready` a later caller installed).
    #[tokio::test]
    async fn test_stale_reload_guard_does_not_clobber_new_slot() {
        let mgr = make_test_manager();
        let handle = make_test_handle("x:main");
        {
            let mut entries = mgr.entries.write().await;
            entries.insert(
                "x:main".to_string(),
                LoadState::Reloading {
                    generation: 9,
                    handle,
                    waiters: Vec::new(),
                },
            );
        }
        {
            let _guard = ReloadLeaderGuard {
                entries: Arc::clone(&mgr.entries),
                alias: "x:main".to_string(),
                generation: 4,
                armed: true,
            };
        }
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        let entries = mgr.entries.read().await;
        assert!(
            matches!(
                entries.get("x:main"),
                Some(LoadState::Reloading { generation: 9, .. })
            ),
            "stale reload guard (gen 4) must not remove the newer slot (gen 9)"
        );
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
