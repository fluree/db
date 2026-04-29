//! Indexer orchestration helpers
//!
//! This module provides utilities for coordinating index operations:
//!
//! - `IndexerHandle`: A coalescing request handler for index operations
//!
//! # Trait Bounds
//!
//! The orchestrator requires storage to implement `Storage + Send + Sync`
//! because it uses the unified indexing pipeline (`build_index_for_ledger`) which
//! can fall back to batched rebuild with checkpoint cleanup.
//!
//! # Thread Safety Note
//!
//! While storage must be `Send + Sync`, the background indexer worker is designed
//! for single-threaded async contexts. For multi-threaded usage:
//!
//! 1. Use `tokio::task::spawn_local` with a `LocalSet`
//! 2. Run indexing in a dedicated single-threaded runtime
//! 3. Use `build_index_for_ledger` directly in your own async context
//!
//! # Example: Simple Coalescing Pattern
//!
//! ```ignore
//! use tokio::sync::watch;
//!
//! // Create a watch channel for coalescing
//! let (tx, mut rx) = watch::channel::<Option<String>>(None);
//!
//! // Trigger indexing (latest ledger_id wins)
//! tx.send(Some("mydb:main".to_string())).ok();
//!
//! // In your indexing loop (running in a LocalSet or dedicated runtime):
//! loop {
//!     if rx.changed().await.is_err() { break; }
//!     let ledger_id = rx.borrow_and_update().clone();
//!     if let Some(ledger_id) = ledger_id {
//!         let result = build_index_for_ledger(&storage, &ns, &ledger_id, config.clone()).await;
//!         // handle result...
//!     }
//! }
//! ```

use crate::config::IndexerConfig;
use crate::error::Result;
use crate::{publish_index_result, IndexResult};
#[cfg(feature = "embedded-orchestrator")]
use fluree_db_core::Storage;
use fluree_db_core::StorageBackend;
use fluree_db_nameservice::ReadWriteNameService;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, watch, Mutex, Notify};
use tracing::{debug, info, warn};

tokio::task_local! {
    static INDEX_REQUEST_CORRELATION: IndexRequestCorrelation;
}

/// Lightweight request correlation copied from the triggering request.
///
/// The background index worker keeps only these copied fields so it can emit
/// correlated logs without holding the original request span open.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexRequestCorrelation {
    /// Request identifier extracted from ingress headers.
    pub request_id: Option<String>,
    /// Trace identifier extracted from distributed tracing context.
    pub trace_id: Option<String>,
    /// High-level operation that queued the indexing work.
    pub operation: Option<String>,
}

impl IndexRequestCorrelation {
    /// Create a correlation payload, skipping absent values.
    pub fn new(
        request_id: Option<impl Into<String>>,
        trace_id: Option<impl Into<String>>,
        operation: Option<impl Into<String>>,
    ) -> Self {
        Self {
            request_id: request_id.map(Into::into),
            trace_id: trace_id.map(Into::into),
            operation: operation.map(Into::into),
        }
    }
}

/// Run a future with request correlation available to background triggers.
pub async fn with_index_request_correlation<Fut>(
    correlation: IndexRequestCorrelation,
    future: Fut,
) -> Fut::Output
where
    Fut: Future,
{
    INDEX_REQUEST_CORRELATION.scope(correlation, future).await
}

/// Return the request correlation copied onto the current task, if any.
pub fn current_index_request_correlation() -> Option<IndexRequestCorrelation> {
    INDEX_REQUEST_CORRELATION.try_with(Clone::clone).ok()
}

// =============================================================================
// Indexing Status & Completion Types
// =============================================================================

/// Outcome of waiting for indexing (terminal result)
#[derive(Debug, Clone)]
pub enum IndexOutcome {
    /// Indexing reached the required t (or beyond)
    Completed {
        /// The t that was indexed to
        index_t: i64,
        /// Content identifier of the index root
        root_id: Option<fluree_db_core::ContentId>,
    },
    /// Indexing failed after retries exhausted or fatal error
    Failed(String),
    /// Request was cancelled before satisfaction
    Cancelled,
}

/// Point-in-time snapshot of indexing status for a ledger
#[derive(Debug, Clone)]
pub struct IndexStatusSnapshot {
    /// Current phase
    pub phase: IndexPhase,
    /// Minimum t needed (coalesced from all pending requests)
    pub pending_min_t: Option<i64>,
    /// Last successfully indexed t (from nameservice or last success)
    pub last_index_t: i64,
    /// Last error message (if any)
    pub last_error: Option<String>,
    /// Number of waiters currently attached
    pub waiter_count: usize,
}

/// Phase of indexing for a ledger
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexPhase {
    /// No pending work
    Idle,
    /// Work is queued but not yet started
    Pending,
    /// Currently being indexed
    InProgress,
}

/// Handle to wait for indexing completion
///
/// Resolves when `index_t >= min_t` (the predicate is satisfied),
/// or when the request fails or is cancelled.
pub struct IndexCompletion {
    receiver: oneshot::Receiver<IndexOutcome>,
}

impl IndexCompletion {
    /// Wait for indexing to satisfy the min_t predicate (or fail/cancel)
    pub async fn wait(self) -> IndexOutcome {
        self.receiver.await.unwrap_or(IndexOutcome::Cancelled)
    }

    /// Non-blocking check - returns None if not yet complete
    pub fn try_get(&mut self) -> Option<IndexOutcome> {
        match self.receiver.try_recv() {
            Ok(outcome) => Some(outcome),
            Err(oneshot::error::TryRecvError::Empty) => None,
            Err(oneshot::error::TryRecvError::Closed) => Some(IndexOutcome::Cancelled),
        }
    }
}

impl std::fmt::Debug for IndexCompletion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexCompletion").finish_non_exhaustive()
    }
}

// =============================================================================
// Internal Per-Ledger State
// =============================================================================

/// Internal per-ledger tracking state
#[derive(Debug)]
struct LedgerIndexState {
    /// Coalesced minimum t needed from all triggers
    pending_min_t: Option<i64>,
    /// Waiters: (min_t they need, sender to notify)
    waiters: Vec<(i64, oneshot::Sender<IndexOutcome>)>,
    /// Current phase
    phase: IndexPhase,
    /// Last successful index_t
    last_index_t: i64,
    /// Last error message
    last_error: Option<String>,
    /// Cancelled flag - prevents retries, resolves unsatisfied waiters
    cancelled: bool,
    /// Retry tracking
    retry_count: u32,
    /// When to retry next (if in backoff)
    next_retry_at: Option<tokio::time::Instant>,
}

impl Default for LedgerIndexState {
    fn default() -> Self {
        Self {
            pending_min_t: None,
            waiters: Vec::new(),
            phase: IndexPhase::Idle,
            last_index_t: 0,
            last_error: None,
            cancelled: false,
            retry_count: 0,
            next_retry_at: None,
        }
    }
}

impl LedgerIndexState {
    /// Create a snapshot of the current state
    fn snapshot(&self) -> IndexStatusSnapshot {
        IndexStatusSnapshot {
            phase: self.phase,
            pending_min_t: self.pending_min_t,
            last_index_t: self.last_index_t,
            last_error: self.last_error.clone(),
            waiter_count: self.waiters.len(),
        }
    }

    /// Resolve all waiters whose min_t is <= threshold_t
    fn resolve_waiters_below(&mut self, threshold_t: i64, outcome: IndexOutcome) {
        let (satisfied, remaining): (Vec<_>, Vec<_>) = self
            .waiters
            .drain(..)
            .partition(|(min_t, _)| *min_t <= threshold_t);

        for (_min_t, sender) in satisfied {
            let _ = sender.send(outcome.clone());
        }
        self.waiters = remaining;
    }

    /// Recalculate pending_min_t from remaining waiters
    fn recalculate_pending_min_t(&mut self) {
        self.pending_min_t = self.waiters.iter().map(|(min_t, _)| *min_t).min();
        if self.pending_min_t.is_none() {
            self.phase = IndexPhase::Idle;
        }
    }

    /// Check if there's any pending work
    fn has_pending_work(&self) -> bool {
        self.pending_min_t.is_some() || !self.waiters.is_empty()
    }
}

#[cfg(feature = "embedded-orchestrator")]
use crate::error::IndexerError;
#[cfg(feature = "embedded-orchestrator")]
use fluree_db_ledger::{IndexConfig, LedgerState};

/// Indexer orchestration for embedded mode
///
/// This struct provides methods for indexing ledgers with support for:
/// - Checking if indexing is needed
/// - Performing full rebuilds
/// - Publishing results to nameservice
///
/// Note: Does not spawn background tasks. Use in a single-threaded async context
/// (e.g., `LocalSet`) or manage threading at a higher level.
pub struct IndexerOrchestrator {
    backend: StorageBackend,
    nameservice: Arc<dyn ReadWriteNameService>,
    config: IndexerConfig,
}

impl IndexerOrchestrator {
    /// Create a new indexer orchestrator
    pub fn new(
        backend: StorageBackend,
        nameservice: Arc<dyn ReadWriteNameService>,
        config: IndexerConfig,
    ) -> Self {
        Self {
            backend,
            nameservice,
            config,
        }
    }

    /// Check if a ledger needs indexing
    ///
    /// Returns `true` if the ledger's index is behind its commits.
    pub async fn needs_indexing(&self, ledger_id: &str) -> Result<bool> {
        let record = self
            .nameservice
            .lookup(ledger_id)
            .await
            .map_err(|e| crate::error::IndexerError::NameService(e.to_string()))?
            .ok_or_else(|| crate::error::IndexerError::LedgerNotFound(ledger_id.to_string()))?;

        // Check if there's a commit without an index, or index_t < commit_t
        if record.commit_head_id.is_none() {
            return Ok(false); // No commits, nothing to index
        }

        if record.index_head_id.is_none() {
            return Ok(true); // Has commits but no index
        }

        Ok(record.index_t < record.commit_t)
    }

    /// Index a ledger using refresh-first strategy
    ///
    /// Returns the existing index if already current, otherwise:
    /// 1. Attempts incremental refresh if an index exists
    /// 2. Falls back to full batched rebuild if refresh fails or no index exists
    pub async fn index_ledger(&self, ledger_id: &str) -> Result<IndexResult> {
        let cs = fluree_db_nameservice::branched_content_store_for_id(
            &self.backend,
            self.nameservice.as_ref(),
            ledger_id,
        )
        .await
        .map_err(|e| crate::error::IndexerError::NameService(e.to_string()))?;
        crate::build_index_for_ledger(
            cs,
            self.nameservice.as_ref(),
            ledger_id,
            self.config.clone(),
        )
        .await
    }

    /// Index a ledger and publish the result
    ///
    /// Combines `index_ledger` with publishing to the nameservice.
    pub async fn index_and_publish(&self, ledger_id: &str) -> Result<IndexResult> {
        let result = self.index_ledger(ledger_id).await?;
        publish_index_result(self.nameservice.as_ref(), &result).await?;
        Ok(result)
    }

    /// Get a reference to the storage backend
    pub fn backend(&self) -> &StorageBackend {
        &self.backend
    }

    /// Get a reference to the nameservice
    pub fn nameservice(&self) -> &Arc<dyn ReadWriteNameService> {
        &self.nameservice
    }

    /// Get a reference to the config
    pub fn config(&self) -> &IndexerConfig {
        &self.config
    }
}

// =============================================================================
// Background Indexer Worker with Predicate-Based Completion
// =============================================================================

/// Per-ledger state map
type LedgerStates = BTreeMap<String, LedgerIndexState>;

/// Handle for triggering background indexing
///
/// Provides APIs for:
/// - Triggering indexing with completion tracking
/// - Cancelling pending work
/// - Checking status
/// - Waiting for idle state
#[derive(Clone)]
pub struct IndexerHandle {
    /// Per-ledger state (shared with worker)
    states: Arc<Mutex<LedgerStates>>,
    /// Signal to wake the worker
    tick: watch::Sender<u64>,
    /// Notifier for idle state changes
    idle_notify: Arc<Notify>,
}

impl std::fmt::Debug for IndexerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexerHandle")
            .field("states", &"<Mutex<LedgerStates>>")
            .field("tick", &"<watch::Sender>")
            .finish()
    }
}

impl IndexerHandle {
    /// Trigger indexing for a ledger with completion tracking
    ///
    /// Returns a completion handle that resolves when `index_t >= min_t`.
    /// Multiple triggers coalesce: worker indexes to latest commit_t,
    /// then resolves ALL waiters whose min_t is satisfied.
    ///
    /// Fire-and-forget: just drop the returned `IndexCompletion`.
    pub async fn trigger(&self, ledger_id: impl Into<String>, min_t: i64) -> IndexCompletion {
        let ledger_id = ledger_id.into();
        let (tx, rx) = oneshot::channel();
        let (phase, pending_min_t, waiter_count);

        {
            let mut states = self.states.lock().await;
            let state = states.entry(ledger_id.clone()).or_default();

            // Clear cancelled flag on new trigger
            state.cancelled = false;

            // New trigger should not be held back by a prior retry backoff.
            // This also makes tests and post-commit hooks behave predictably.
            state.retry_count = 0;
            state.next_retry_at = None;

            // Add waiter
            state.waiters.push((min_t, tx));

            // Update coalesced min_t
            state.pending_min_t = Some(
                state
                    .pending_min_t
                    .map_or(min_t, |existing| existing.min(min_t)),
            );

            // Mark as pending if idle
            if state.phase == IndexPhase::Idle {
                state.phase = IndexPhase::Pending;
            }

            phase = state.phase;
            pending_min_t = state.pending_min_t;
            waiter_count = state.waiters.len();
        }

        // Signal the worker
        self.tick.send_modify(|t| *t = t.wrapping_add(1));

        info!(
            ledger_id = %ledger_id,
            requested_min_t = min_t,
            phase = ?phase,
            pending_min_t = ?pending_min_t,
            waiter_count,
            "Queued indexing request"
        );

        IndexCompletion { receiver: rx }
    }

    /// Cancel pending/queued work for a ledger
    ///
    /// - Removes from pending queue (if not yet started)
    /// - Prevents retries on failure
    /// - Does NOT abort in-progress indexing (it will complete and publish)
    /// - Resolves all waiters whose min_t is NOT yet satisfied as Cancelled
    ///
    /// Returns true if there was pending work to cancel.
    pub async fn cancel(&self, ledger_id: &str) -> bool {
        let had_work = {
            let mut states = self.states.lock().await;
            if let Some(state) = states.get_mut(ledger_id) {
                let had_work = state.has_pending_work();
                state.cancelled = true;
                // Resolve all waiters as cancelled (they haven't been satisfied)
                state.resolve_waiters_below(i64::MAX, IndexOutcome::Cancelled);
                state.pending_min_t = None;
                if state.phase == IndexPhase::Pending {
                    state.phase = IndexPhase::Idle;
                }
                // Note: if InProgress, we let it finish but won't retry on failure
                had_work
            } else {
                false
            }
        };

        if had_work {
            self.idle_notify.notify_waiters();
        }

        had_work
    }

    /// Cancel all pending work (for shutdown)
    pub async fn cancel_all(&self) {
        let mut states = self.states.lock().await;
        for state in states.values_mut() {
            state.cancelled = true;
            state.resolve_waiters_below(i64::MAX, IndexOutcome::Cancelled);
            state.pending_min_t = None;
            if state.phase == IndexPhase::Pending {
                state.phase = IndexPhase::Idle;
            }
        }
        drop(states);
        self.idle_notify.notify_waiters();
    }

    /// Check current status for a ledger
    pub async fn status(&self, ledger_id: &str) -> Option<IndexStatusSnapshot> {
        let states = self.states.lock().await;
        states.get(ledger_id).map(LedgerIndexState::snapshot)
    }

    /// Check if a ledger has pending or in-progress work
    pub async fn is_pending(&self, ledger_id: &str) -> bool {
        let states = self.states.lock().await;
        states
            .get(ledger_id)
            .is_some_and(|s| s.phase != IndexPhase::Idle)
    }

    /// List all ledgers with pending/in-progress work
    pub async fn pending_ledgers(&self) -> Vec<String> {
        let states = self.states.lock().await;
        states
            .iter()
            .filter(|(_, s)| s.phase != IndexPhase::Idle)
            .map(|(ledger_id, _)| ledger_id.clone())
            .collect()
    }

    /// Wait until a specific ledger has no pending work
    ///
    /// Returns immediately if no work pending.
    /// Different from `IndexCompletion`: this waits for the queue to drain,
    /// not for a specific min_t to be reached.
    pub async fn wait_for_idle(&self, ledger_id: &str) {
        loop {
            // Avoid missed-wakeup races: create the notification future *before*
            // checking the condition, then await it if still not idle.
            let notified = self.idle_notify.notified();
            {
                let states = self.states.lock().await;
                if let Some(state) = states.get(ledger_id) {
                    if state.phase == IndexPhase::Idle {
                        return;
                    }
                } else {
                    return; // Ledger not tracked = idle
                }
            }
            // Wait for a state change notification
            notified.await;
        }
    }

    /// Wait until ALL pending work completes (queue fully drained)
    pub async fn wait_all_idle(&self) {
        loop {
            // Avoid missed-wakeup races (see `wait_for_idle`).
            let notified = self.idle_notify.notified();
            {
                let states = self.states.lock().await;
                let any_busy = states.values().any(|s| s.phase != IndexPhase::Idle);
                if !any_busy {
                    return;
                }
            }
            notified.await;
        }
    }
}

/// Background indexer worker with predicate-based completion
///
/// This worker processes index requests one ledger at a time with:
/// - Per-ledger state tracking with waiters
/// - Predicate-based completion (resolves when index_t >= min_t)
/// - Exponential backoff on failures (capped at 30s)
/// - Cooperative cancellation
/// - Clean shutdown when all handles are dropped
pub struct BackgroundIndexerWorker {
    backend: StorageBackend,
    nameservice: Arc<dyn ReadWriteNameService>,
    config: IndexerConfig,
    states: Arc<Mutex<LedgerStates>>,
    tick_rx: watch::Receiver<u64>,
    idle_notify: Arc<Notify>,
}

impl BackgroundIndexerWorker {
    /// Create a new worker and its associated handle
    pub fn new(
        backend: StorageBackend,
        nameservice: Arc<dyn ReadWriteNameService>,
        config: IndexerConfig,
    ) -> (Self, IndexerHandle) {
        let states = Arc::new(Mutex::new(BTreeMap::new()));
        let (tick_tx, tick_rx) = watch::channel(0u64);
        let idle_notify = Arc::new(Notify::new());

        let worker = Self {
            backend,
            nameservice,
            config,
            states: states.clone(),
            tick_rx,
            idle_notify: idle_notify.clone(),
        };

        let handle = IndexerHandle {
            states,
            tick: tick_tx,
            idle_notify,
        };

        (worker, handle)
    }

    /// Run the worker loop (consumes self)
    ///
    /// This loop:
    /// 1. Waits for tick signal OR retry deadline
    /// 2. Processes ledgers with pending work
    /// 3. Resolves waiters based on min_t predicate
    /// 4. Handles cancellation and backoff
    pub async fn run(mut self) {
        loop {
            // Compute next retry deadline
            let retry_deadline = {
                let states = self.states.lock().await;
                states.values().filter_map(|s| s.next_retry_at).min()
            };

            // Wait for tick OR retry deadline (whichever comes first)
            let wait_result = if let Some(deadline) = retry_deadline {
                tokio::select! {
                    result = self.tick_rx.changed() => result,
                    () = tokio::time::sleep_until(deadline) => Ok(()),
                }
            } else {
                self.tick_rx.changed().await
            };

            if wait_result.is_err() {
                // All senders dropped - resolve remaining waiters and shutdown
                let mut states = self.states.lock().await;
                for state in states.values_mut() {
                    state.resolve_waiters_below(i64::MAX, IndexOutcome::Cancelled);
                }
                break;
            }

            // Collect ledgers that need processing
            let now = tokio::time::Instant::now();
            let ledgers_to_process: Vec<String> = {
                let states = self.states.lock().await;
                states
                    .iter()
                    .filter(|(_, state)| {
                        state.pending_min_t.is_some()
                            && !state.cancelled
                            && state.next_retry_at.is_none_or(|t| t <= now)
                    })
                    .map(|(ledger_id, _)| ledger_id.clone())
                    .collect()
            };

            debug!(
                ledger_count = ledgers_to_process.len(),
                has_retry_deadline = retry_deadline.is_some(),
                "Background indexer worker tick"
            );

            // Process each ledger
            for ledger_id in ledgers_to_process {
                debug!(ledger_id = %ledger_id, "Dispatching queued ledger to process_ledger");
                self.process_ledger(&ledger_id).await;
                debug!(ledger_id = %ledger_id, "process_ledger returned");
            }

            // Handle cancelled ledgers
            {
                let mut states = self.states.lock().await;
                for state in states.values_mut() {
                    if state.cancelled && state.has_pending_work() {
                        state.resolve_waiters_below(i64::MAX, IndexOutcome::Cancelled);
                        state.pending_min_t = None;
                        state.phase = IndexPhase::Idle;
                        state.cancelled = false;
                    }
                }
            }

            // Notify idle waiters
            self.idle_notify.notify_waiters();
        }
    }

    /// Process a single ledger
    async fn process_ledger(&self, ledger_id: &str) {
        let process_started = Instant::now();

        let (pending_min_t, waiter_count, retry_count) = {
            let lock_started = Instant::now();
            let states = self.states.lock().await;
            debug!(
                ledger_id = %ledger_id,
                lock_wait_ms = lock_started.elapsed().as_millis(),
                "Acquired state lock for queued indexing snapshot"
            );
            if let Some(state) = states.get(ledger_id) {
                (state.pending_min_t, state.waiters.len(), state.retry_count)
            } else {
                debug!(
                    ledger_id = %ledger_id,
                    "Ledger disappeared before queued indexing snapshot"
                );
                return;
            }
        };

        // Mark as in-progress
        {
            let lock_started = Instant::now();
            let mut states = self.states.lock().await;
            debug!(
                ledger_id = %ledger_id,
                lock_wait_ms = lock_started.elapsed().as_millis(),
                "Acquired state lock to mark queued indexing in progress"
            );
            if let Some(state) = states.get_mut(ledger_id) {
                if state.cancelled {
                    debug!(
                        ledger_id = %ledger_id,
                        "Queued indexing cancelled before marking in progress"
                    );
                    return;
                }
                state.phase = IndexPhase::InProgress;
                debug!(
                    ledger_id = %ledger_id,
                    pending_min_t = ?state.pending_min_t,
                    waiter_count = state.waiters.len(),
                    "Marked queued indexing work in progress"
                );
            } else {
                debug!(
                    ledger_id = %ledger_id,
                    "Ledger disappeared before marking queued indexing in progress"
                );
                return;
            }
        }

        info!(
            ledger_id = %ledger_id,
            pending_min_t = ?pending_min_t,
            waiter_count,
            retry_count,
            "Starting queued indexing work"
        );

        // Re-check nameservice for current state
        debug!(ledger_id = %ledger_id, "Looking up nameservice record for queued indexing work");
        let record = match self.nameservice.lookup(ledger_id).await {
            Ok(Some(r)) => r,
            Ok(None) => {
                // Ledger doesn't exist - resolve waiters as failed
                let mut states = self.states.lock().await;
                if let Some(state) = states.get_mut(ledger_id) {
                    state.resolve_waiters_below(
                        i64::MAX,
                        IndexOutcome::Failed("Ledger not found".to_string()),
                    );
                    state.pending_min_t = None;
                    state.phase = IndexPhase::Idle;
                }
                return;
            }
            Err(e) => {
                warn!(
                ledger_id = %ledger_id,
                        error = %e,
                        "Nameservice lookup failed, will retry"
                    );
                self.schedule_retry(ledger_id, &e.to_string()).await;
                return;
            }
        };

        let current_index_t = record.index_t;
        let commit_gap = record.commit_t - current_index_t;
        info!(
            ledger_id = %ledger_id,
            current_index_t,
            commit_t = record.commit_t,
            commit_gap,
            pending_min_t = ?pending_min_t,
            has_index = record.index_head_id.is_some(),
            "Loaded ledger state for queued indexing work"
        );

        // Check if index already satisfies all waiters
        {
            let lock_started = Instant::now();
            debug!(
                ledger_id = %ledger_id,
                current_index_t,
                commit_t = record.commit_t,
                "Waiting for state lock before queued indexing gate"
            );
            let mut states = self.states.lock().await;
            debug!(
                ledger_id = %ledger_id,
                lock_wait_ms = lock_started.elapsed().as_millis(),
                "Acquired state lock for queued indexing gate"
            );
            if let Some(state) = states.get_mut(ledger_id) {
                state.last_index_t = current_index_t;
                debug!(
                    ledger_id = %ledger_id,
                    state_pending_min_t = ?state.pending_min_t,
                    state_waiter_count = state.waiters.len(),
                    state_cancelled = state.cancelled,
                    state_retry_count = state.retry_count,
                    state_last_error = ?state.last_error,
                    "Entered queued indexing gate"
                );

                if let Some(pending_min) = state.pending_min_t {
                    debug!(
                        ledger_id = %ledger_id,
                        pending_min,
                        current_index_t,
                        "Evaluating queued indexing satisfaction gate"
                    );
                    if current_index_t >= pending_min {
                        debug!(
                            ledger_id = %ledger_id,
                            pending_min,
                            current_index_t,
                            has_index_head = record.index_head_id.is_some(),
                            "Current index already satisfies queued waiters"
                        );
                        // Already satisfied - resolve waiters
                        if let Some(root_id) = record.index_head_id.clone() {
                            let outcome = IndexOutcome::Completed {
                                index_t: current_index_t,
                                root_id: Some(root_id),
                            };
                            state.resolve_waiters_below(current_index_t, outcome);
                            state.recalculate_pending_min_t();
                            state.retry_count = 0;
                            state.next_retry_at = None;
                            state.last_error = None;
                            debug!(
                                ledger_id = %ledger_id,
                                remaining_pending_min_t = ?state.pending_min_t,
                                remaining_waiter_count = state.waiters.len(),
                                "Resolved satisfied queued waiters with existing index root"
                            );

                            // If no more work, we're done
                            if state.pending_min_t.is_none() {
                                debug!(
                                    ledger_id = %ledger_id,
                                    process_elapsed_ms = process_started.elapsed().as_millis(),
                                    "Queued indexing fully satisfied before build"
                                );
                                return;
                            }
                        } else if current_index_t == 0 && pending_min <= 0 {
                            // Genesis-ish case: predicate satisfied without a materialized index.
                            // We allow an empty root address here to avoid failing benign waits.
                            let outcome = IndexOutcome::Completed {
                                index_t: current_index_t,
                                root_id: None,
                            };
                            state.resolve_waiters_below(current_index_t, outcome);
                            state.recalculate_pending_min_t();
                            state.retry_count = 0;
                            state.next_retry_at = None;
                            state.last_error = None;
                            debug!(
                                ledger_id = %ledger_id,
                                remaining_pending_min_t = ?state.pending_min_t,
                                remaining_waiter_count = state.waiters.len(),
                                "Resolved queued waiters in genesis-ish no-root case"
                            );

                            if state.pending_min_t.is_none() {
                                debug!(
                                    ledger_id = %ledger_id,
                                    process_elapsed_ms = process_started.elapsed().as_millis(),
                                    "Queued indexing fully satisfied without build"
                                );
                                return;
                            }
                        } else {
                            // Nameservice is reporting an index_t but no index address.
                            // Don't spin: force a retry with backoff.
                            state.last_error =
                                Some("Nameservice missing index_head_id".to_string());
                            state.phase = IndexPhase::Pending;
                            state.next_retry_at =
                                Some(tokio::time::Instant::now() + Duration::from_millis(250));
                            state.retry_count = state.retry_count.saturating_add(1);
                            warn!(
                                ledger_id = %ledger_id,
                                current_index_t,
                                pending_min,
                                "Queued indexing gate found index_t without index_head_id; retrying"
                            );
                            return;
                        }
                    }
                }

                // Check if cancelled during lookup
                if state.cancelled {
                    state.phase = IndexPhase::Idle;
                    debug!(
                        ledger_id = %ledger_id,
                        "Queued indexing cancelled after nameservice lookup"
                    );
                    return;
                }
                debug!(
                    ledger_id = %ledger_id,
                    process_elapsed_ms = process_started.elapsed().as_millis(),
                    "Queued indexing gate passed; proceeding to build checks"
                );
            } else {
                debug!(
                    ledger_id = %ledger_id,
                    "Ledger disappeared during queued indexing gate"
                );
            }
        }

        // Skip if already indexed to current commit
        if record.commit_t <= current_index_t {
            info!(
                ledger_id = %ledger_id,
                current_index_t,
                commit_t = record.commit_t,
                "Queued indexing request already satisfied"
            );
            let mut states = self.states.lock().await;
            if let Some(state) = states.get_mut(ledger_id) {
                // Resolve waiters that can be satisfied
                if let Some(root_id) = &record.index_head_id {
                    let outcome = IndexOutcome::Completed {
                        index_t: current_index_t,
                        root_id: Some(root_id.clone()),
                    };
                    state.resolve_waiters_below(current_index_t, outcome);
                } else if current_index_t == 0 {
                    // Nothing to index yet; see genesis-ish behavior above.
                    let outcome = IndexOutcome::Completed {
                        index_t: current_index_t,
                        root_id: None,
                    };
                    state.resolve_waiters_below(current_index_t, outcome);
                } else {
                    // Don't loop forever: retry with backoff.
                    state.last_error = Some("Nameservice missing index_head_id".to_string());
                    state.phase = IndexPhase::Pending;
                    state.next_retry_at =
                        Some(tokio::time::Instant::now() + Duration::from_millis(250));
                    state.retry_count = state.retry_count.saturating_add(1);
                    return;
                }
                state.recalculate_pending_min_t();
                if state.pending_min_t.is_some() {
                    state.phase = IndexPhase::Pending;
                }
            }
            return;
        }

        debug!(
            ledger_id = %ledger_id,
            process_elapsed_ms = process_started.elapsed().as_millis(),
            "Queued indexing is about to call build_index_for_record"
        );
        // Execute refresh-first indexing to CURRENT commit_t
        info!(
            ledger_id = %ledger_id,
            current_index_t,
            commit_t = record.commit_t,
            commit_gap,
            pending_min_t = ?pending_min_t,
            "Starting index build for queued work"
        );
        let content_store = match fluree_db_nameservice::branched_content_store_for_record(
            &self.backend,
            self.nameservice.as_ref(),
            &record,
        )
        .await
        {
            Ok(cs) => cs,
            Err(e) => {
                warn!(
                    ledger_id = %ledger_id,
                    error = %e,
                    "Failed to build branched content store for indexing; aborting build"
                );
                let mut states = self.states.lock().await;
                if let Some(state) = states.get_mut(ledger_id) {
                    state.last_error = Some(e.to_string());
                    state.phase = IndexPhase::Pending;
                    state.next_retry_at =
                        Some(tokio::time::Instant::now() + Duration::from_millis(250));
                    state.retry_count = state.retry_count.saturating_add(1);
                }
                return;
            }
        };
        let result =
            crate::build_index_for_record(content_store, &record, self.config.clone()).await;

        match result {
            Ok(index_result) => {
                // Try to publish
                if let Err(e) =
                    crate::publish_index_result(self.nameservice.as_ref(), &index_result).await
                {
                    warn!(
                    ledger_id = %ledger_id,
                            error = %e,
                            "Failed to publish index, will retry"
                        );
                    self.schedule_retry(ledger_id, &e.to_string()).await;
                } else {
                    info!(
                    ledger_id = %ledger_id,
                            index_t = index_result.index_t,
                            "Successfully indexed ledger"
                        );

                    // Spawn garbage collection (fire-and-forget, non-fatal).
                    let gc_store = self.backend.content_store(&index_result.ledger_id);
                    let gc_root_id = index_result.root_id.clone();
                    let gc_config = crate::gc::CleanGarbageConfig {
                        max_old_indexes: Some(self.config.gc_max_old_indexes),
                        min_time_garbage_mins: Some(self.config.gc_min_time_mins),
                    };
                    tokio::spawn(async move {
                        if let Err(e) =
                            crate::gc::clean_garbage(gc_store.as_ref(), &gc_root_id, gc_config)
                                .await
                        {
                            warn!(
                                error = %e,
                                root_id = %gc_root_id,
                                "Background GC failed (non-fatal)"
                            );
                        } else {
                            debug!(root_id = %gc_root_id, "Background GC completed");
                        }
                    });

                    // Resolve waiters
                    let mut states = self.states.lock().await;
                    if let Some(state) = states.get_mut(ledger_id) {
                        let outcome = IndexOutcome::Completed {
                            index_t: index_result.index_t,
                            root_id: Some(index_result.root_id.clone()),
                        };
                        state.resolve_waiters_below(index_result.index_t, outcome);
                        state.last_index_t = index_result.index_t;
                        state.recalculate_pending_min_t();
                        if state.pending_min_t.is_some() {
                            state.phase = IndexPhase::Pending;
                        }
                        state.retry_count = 0;
                        state.next_retry_at = None;
                        state.last_error = None;
                    }
                }
            }
            Err(e) => {
                warn!(
                ledger_id = %ledger_id,
                        error = %e,
                        "Indexing failed, will retry"
                    );
                self.schedule_retry(ledger_id, &e.to_string()).await;
            }
        }
    }

    /// Schedule a retry with exponential backoff
    async fn schedule_retry(&self, ledger_id: &str, error: &str) {
        let mut states = self.states.lock().await;
        if let Some(state) = states.get_mut(ledger_id) {
            // Check if cancelled - don't retry
            if state.cancelled {
                state.resolve_waiters_below(i64::MAX, IndexOutcome::Cancelled);
                state.pending_min_t = None;
                state.phase = IndexPhase::Idle;
                return;
            }

            state.last_error = Some(error.to_string());

            // Compute backoff: 100ms * 2^retry_count, capped at 30s
            let exp = state.retry_count.min(20);
            let factor = 1u64.checked_shl(exp).unwrap_or(u64::MAX);
            let backoff_ms = 100u64.saturating_mul(factor).min(30_000);
            state.next_retry_at =
                Some(tokio::time::Instant::now() + Duration::from_millis(backoff_ms));

            state.retry_count = state.retry_count.saturating_add(1);
            state.phase = IndexPhase::Pending;
            info!(
                ledger_id = %ledger_id,
                retry_count = state.retry_count,
                backoff_ms,
                error = %error,
                pending_min_t = ?state.pending_min_t,
                "Scheduled indexing retry"
            );
        }
    }
}

// =============================================================================
// Post-commit indexing helpers (embedded-orchestrator feature)
// =============================================================================

/// Result of a post-commit refresh attempt (embedded mode)
///
/// `attempted` is true only if we actually invoked the index build.
/// Early-returns (threshold not met, nothing to do) set `attempted: false`.
#[cfg(feature = "embedded-orchestrator")]
#[derive(Debug)]
pub struct PostCommitIndexResult {
    /// True if index build was actually invoked
    pub attempted: bool,
    /// True if index build succeeded
    pub refreshed: bool,
    /// True if publish_index succeeded
    pub published: bool,
    /// True if apply_index succeeded
    pub applied: bool,
    /// The index result if successful
    pub refresh: Option<IndexResult>,
    /// Error message if any step failed (for logging)
    pub error: Option<String>,
}

#[cfg(feature = "embedded-orchestrator")]
fn current_ns_record(ledger: &LedgerState) -> Option<&fluree_db_nameservice::NsRecord> {
    let record = ledger.ns_record.as_ref()?;
    let commit_matches =
        record.commit_t == ledger.t() && record.commit_head_id == ledger.head_commit_id;
    let index_matches =
        record.index_t == ledger.index_t() && record.index_head_id == ledger.head_index_id;

    if record.ledger_id == ledger.ledger_id() && commit_matches && index_matches {
        Some(record)
    } else {
        None
    }
}

/// Opportunistically refresh + publish + apply after a commit.
///
/// - Uses the provided `storage` for index building and apply.
/// - Uses `target_t` explicitly (use `CommitReceipt.t`).
/// - Never fails the commit path; returns status + error string for logging.
/// - **Applies index even if publish fails** for local correctness.
#[cfg(feature = "embedded-orchestrator")]
pub async fn maybe_refresh_after_commit<S>(
    storage: &S,
    nameservice: &dyn ReadWriteNameService,
    mut ledger: LedgerState,
    index_config: &IndexConfig,
    indexer_config: IndexerConfig,
    _target_t: i64,
) -> (LedgerState, PostCommitIndexResult)
where
    S: Storage + fluree_db_core::StorageMethod + Clone + Send + Sync + 'static,
{
    // Check threshold
    if ledger.maybe_trigger_index(index_config).is_none() {
        return (
            ledger,
            PostCommitIndexResult {
                attempted: false,
                refreshed: false,
                published: false,
                applied: false,
                refresh: None,
                error: None,
            },
        );
    }

    // Early-return if nothing to do
    let effective_target_t = ledger.t();
    if ledger.novelty_size() == 0 || effective_target_t <= ledger.index_t() {
        return (
            ledger,
            PostCommitIndexResult {
                attempted: false,
                refreshed: false,
                published: false,
                applied: false,
                refresh: None,
                error: None,
            },
        );
    }

    let ledger_addr = ledger.ledger_id().to_string();
    let backend = StorageBackend::Managed(std::sync::Arc::new(storage.clone()));
    let cs: std::sync::Arc<dyn fluree_db_core::ContentStore> =
        match fluree_db_nameservice::branched_content_store_for_record_or_id(
            &backend,
            nameservice,
            current_ns_record(&ledger),
            &ledger_addr,
        )
        .await
        {
            Ok(cs) => cs,
            Err(e) => {
                return (
                    ledger,
                    PostCommitIndexResult {
                        attempted: true,
                        refreshed: false,
                        published: false,
                        applied: false,
                        refresh: None,
                        error: Some(format!("build branched content store: {e}")),
                    },
                );
            }
        };

    // Use the ledger's reindex_max_bytes as the commit-walk byte budget
    // so incremental indexing falls back to a full rebuild when the
    // commit chain would exceed the novelty threshold.
    let mut indexer_config = indexer_config;
    if indexer_config.incremental_max_commit_bytes.is_none() {
        indexer_config.incremental_max_commit_bytes = Some(index_config.reindex_max_bytes);
    }

    let build_result = if let Some(record) = current_ns_record(&ledger) {
        crate::build_index_for_record(cs.clone(), record, indexer_config).await
    } else {
        crate::build_index_for_ledger(cs.clone(), nameservice, &ledger_addr, indexer_config).await
    };

    match build_result {
        Ok(result) => {
            // Track publish result but continue regardless
            let publish_result = nameservice
                .publish_index(&ledger_addr, result.index_t, &result.root_id)
                .await;
            let published = publish_result.is_ok();
            let publish_error = publish_result.err().map(|e| e.to_string());

            // Apply even if publish failed (local correctness)
            let apply_result = ledger.apply_index(&result.root_id, &cs).await;
            let applied = apply_result.is_ok();

            // Build final error message
            let error = match (&publish_error, &apply_result) {
                (Some(pe), Ok(())) => Some(format!("index applied but publish failed: {pe}")),
                (None, Err(ae)) => Some(format!("index published but apply_index failed: {ae}")),
                (Some(pe), Err(ae)) => Some(format!(
                    "publish failed: {pe}; apply_index also failed: {ae}"
                )),
                (None, Ok(())) => None,
            };

            (
                ledger,
                PostCommitIndexResult {
                    attempted: true,
                    refreshed: true,
                    published,
                    applied,
                    refresh: Some(result),
                    error,
                },
            )
        }
        Err(e) => (
            ledger,
            PostCommitIndexResult {
                attempted: true,
                refreshed: false,
                published: false,
                applied: false,
                refresh: None,
                error: Some(e.to_string()),
            },
        ),
    }
}

/// Blocking refresh path for hard-threshold scenarios (call before commit).
///
/// - Intended usage: if `ledger.require_index(&index_config).is_some()`, call this.
/// - Should typically run *before* staging (since `stage()` also checks max novelty).
/// - Errors are fatal here because the caller is explicitly trying to unblock commits.
#[cfg(feature = "embedded-orchestrator")]
pub async fn require_refresh_before_commit<S>(
    storage: &S,
    nameservice: &dyn ReadWriteNameService,
    mut ledger: LedgerState,
    indexer_config: IndexerConfig,
    _target_t: i64,
) -> Result<LedgerState>
where
    S: Storage + fluree_db_core::StorageMethod + Clone + Send + Sync + 'static,
{
    let ledger_addr = ledger.ledger_id().to_string();
    let backend = StorageBackend::Managed(std::sync::Arc::new(storage.clone()));
    let cs: std::sync::Arc<dyn fluree_db_core::ContentStore> =
        fluree_db_nameservice::branched_content_store_for_record_or_id(
            &backend,
            nameservice,
            current_ns_record(&ledger),
            &ledger_addr,
        )
        .await
        .map_err(|e| IndexerError::NameService(e.to_string()))?;

    let result = if let Some(record) = current_ns_record(&ledger) {
        crate::build_index_for_record(cs.clone(), record, indexer_config).await?
    } else {
        crate::build_index_for_ledger(cs.clone(), nameservice, &ledger_addr, indexer_config).await?
    };

    nameservice
        .publish_index(&ledger_addr, result.index_t, &result.root_id)
        .await
        .map_err(|e| IndexerError::NameService(e.to_string()))?;

    ledger
        .apply_index(&result.root_id, &cs)
        .await
        .map_err(|e| IndexerError::LedgerApply(e.to_string()))?;

    Ok(ledger)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{
        ContentAddressedWrite, ContentId, ContentKind, Flake, FlakeValue, MemoryStorage, Sid,
    };
    use fluree_db_nameservice::memory::MemoryNameService;
    use fluree_db_nameservice::{NameService, Publisher};
    use fluree_db_novelty::Commit;
    use std::collections::HashMap;

    fn make_flake(s_code: u16, s_name: &str, p_code: u16, p_name: &str, val: i64, t: i64) -> Flake {
        Flake::new(
            Sid::new(s_code, s_name),
            Sid::new(p_code, p_name),
            FlakeValue::Long(val),
            Sid::new(2, "xsd:long"),
            t,
            true,
            None,
        )
    }

    async fn store_commit(storage: &MemoryStorage, commit: &Commit) -> ContentId {
        use fluree_db_core::commit::codec::envelope::{encode_envelope_fields, CodecEnvelope};
        use fluree_db_core::commit::codec::format::{
            self, CommitFooter, CommitHeader, FOOTER_LEN, HEADER_LEN,
        };
        use fluree_db_core::commit::codec::op_codec::{encode_op, CommitDicts};
        use std::collections::HashMap;

        let mut dicts = CommitDicts::new();
        let mut ops_buf = Vec::new();
        for f in &commit.flakes {
            encode_op(f, &mut dicts, &mut ops_buf).unwrap();
        }

        let envelope = CodecEnvelope {
            t: commit.t,
            parents: commit.parents.clone(),
            namespace_delta: commit
                .namespace_delta
                .iter()
                .map(|(k, v)| (*k, v.clone()))
                .collect::<HashMap<_, _>>(),
            txn: commit.txn.clone(),
            time: commit.time.clone(),
            txn_signature: None,
            txn_meta: commit.txn_meta.clone(),
            graph_delta: commit.graph_delta.clone(),
            ns_split_mode: None,
        };
        let mut envelope_bytes = Vec::new();
        encode_envelope_fields(&envelope, &mut envelope_bytes).unwrap();

        let dict_bytes: Vec<Vec<u8>> = vec![
            dicts.graph.serialize(),
            dicts.subject.serialize(),
            dicts.predicate.serialize(),
            dicts.datatype.serialize(),
            dicts.object_ref.serialize(),
        ];

        let ops_section_len = ops_buf.len() as u32;
        let envelope_len = envelope_bytes.len() as u32;
        let dict_start = HEADER_LEN + envelope_bytes.len() + ops_buf.len();
        let mut dict_locations = [format::DictLocation::default(); 5];
        let mut offset = dict_start as u64;
        for (i, d) in dict_bytes.iter().enumerate() {
            dict_locations[i] = format::DictLocation {
                offset,
                len: d.len() as u32,
            };
            offset += d.len() as u64;
        }

        let footer = CommitFooter {
            dicts: dict_locations,
            ops_section_len,
        };
        let header = CommitHeader {
            version: format::VERSION,
            flags: 0,
            t: commit.t,
            op_count: commit.flakes.len() as u32,
            envelope_len,
            sig_block_len: 0,
        };

        // V4: no embedded hash
        let total_len = HEADER_LEN
            + envelope_bytes.len()
            + ops_buf.len()
            + dict_bytes.iter().map(std::vec::Vec::len).sum::<usize>()
            + FOOTER_LEN;
        let mut blob = vec![0u8; total_len];

        let mut pos = 0;
        header.write_to(&mut blob[pos..]);
        pos += HEADER_LEN;
        blob[pos..pos + envelope_bytes.len()].copy_from_slice(&envelope_bytes);
        pos += envelope_bytes.len();
        blob[pos..pos + ops_buf.len()].copy_from_slice(&ops_buf);
        pos += ops_buf.len();
        for d in &dict_bytes {
            blob[pos..pos + d.len()].copy_from_slice(d);
            pos += d.len();
        }
        footer.write_to(&mut blob[pos..]);

        // Write via content_write_bytes (CID = SHA-256 of full blob)
        storage
            .content_write_bytes(ContentKind::Commit, "test:main", &blob)
            .await
            .unwrap();

        ContentId::new(ContentKind::Commit, &blob)
    }

    #[tokio::test]
    async fn test_orchestrator_needs_indexing_no_commits() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        ns.create_ledger("test:main").unwrap();

        let orchestrator = IndexerOrchestrator::new(
            StorageBackend::Managed(Arc::new(storage)),
            ns.clone(),
            IndexerConfig::small(),
        );

        // No commits - doesn't need indexing
        let needs = orchestrator.needs_indexing("test:main").await.unwrap();
        assert!(!needs);
    }

    #[tokio::test]
    async fn test_orchestrator_needs_indexing_with_commits_no_index() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());

        // Setup a commit
        let commit = Commit {
            id: None,
            t: 1,
            time: None,
            flakes: vec![make_flake(1, "ex:alice", 1, "ex:age", 30, 1)],
            parents: Vec::new(),
            txn: None,
            namespace_delta: HashMap::from([(1, "ex:".to_string())]),
            txn_signature: None,
            commit_signatures: Vec::new(),
            txn_meta: Vec::new(),
            graph_delta: HashMap::new(),
            ns_split_mode: None,
        };
        let cid = store_commit(&storage, &commit).await;
        ns.publish_commit("test:main", 1, &cid).await.unwrap();

        let orchestrator = IndexerOrchestrator::new(
            StorageBackend::Managed(Arc::new(storage)),
            ns.clone(),
            IndexerConfig::small(),
        );

        // Has commits but no index - needs indexing
        let needs = orchestrator.needs_indexing("test:main").await.unwrap();
        assert!(needs);
    }

    #[tokio::test]
    async fn test_orchestrator_needs_indexing_index_current() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());

        // Setup a commit
        let commit = Commit {
            id: None,
            t: 1,
            time: None,
            flakes: vec![make_flake(1, "ex:alice", 1, "ex:age", 30, 1)],
            parents: Vec::new(),
            txn: None,
            namespace_delta: HashMap::from([(1, "ex:".to_string())]),
            txn_signature: None,
            commit_signatures: Vec::new(),
            txn_meta: Vec::new(),
            graph_delta: HashMap::new(),
            ns_split_mode: None,
        };
        let cid = store_commit(&storage, &commit).await;
        ns.publish_commit("test:main", 1, &cid).await.unwrap();

        let config = IndexerConfig::small()
            .with_data_dir(std::env::temp_dir().join("fluree-test-orch-idx-current"));
        let orchestrator = IndexerOrchestrator::new(
            StorageBackend::Managed(Arc::new(storage.clone())),
            ns.clone(),
            config,
        );

        // Index the ledger
        let result = orchestrator.index_and_publish("test:main").await.unwrap();
        assert_eq!(result.index_t, 1);

        // Now index is current - doesn't need indexing
        let needs = orchestrator.needs_indexing("test:main").await.unwrap();
        assert!(!needs);
    }

    #[tokio::test]
    async fn test_orchestrator_needs_indexing_index_behind() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());

        // Setup first commit and index
        let commit1 = Commit {
            id: None,
            t: 1,
            time: None,
            flakes: vec![make_flake(1, "ex:alice", 1, "ex:age", 30, 1)],
            parents: Vec::new(),
            txn: None,
            namespace_delta: HashMap::from([(1, "ex:".to_string())]),
            txn_signature: None,
            commit_signatures: Vec::new(),
            txn_meta: Vec::new(),
            graph_delta: HashMap::new(),
            ns_split_mode: None,
        };
        let cid1 = store_commit(&storage, &commit1).await;
        ns.publish_commit("test:main", 1, &cid1).await.unwrap();

        let config = IndexerConfig::small()
            .with_data_dir(std::env::temp_dir().join("fluree-test-orch-idx-behind"));
        let orchestrator = IndexerOrchestrator::new(
            StorageBackend::Managed(Arc::new(storage.clone())),
            ns.clone(),
            config,
        );
        orchestrator.index_and_publish("test:main").await.unwrap();

        // Add another commit
        let commit2 = Commit {
            id: None,
            t: 2,
            time: None,
            flakes: vec![make_flake(1, "ex:bob", 1, "ex:age", 25, 2)],
            parents: vec![cid1.clone()],
            txn: None,
            namespace_delta: HashMap::new(),
            txn_signature: None,
            commit_signatures: Vec::new(),
            txn_meta: Vec::new(),
            graph_delta: HashMap::new(),
            ns_split_mode: None,
        };
        let cid2 = store_commit(&storage, &commit2).await;
        ns.publish_commit("test:main", 2, &cid2).await.unwrap();

        // Index is now behind - needs indexing
        let needs = orchestrator.needs_indexing("test:main").await.unwrap();
        assert!(needs);
    }

    #[tokio::test]
    async fn test_orchestrator_index_ledger() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());

        // Setup a commit
        let commit = Commit {
            id: None,
            t: 1,
            time: None,
            flakes: vec![make_flake(1, "ex:alice", 1, "ex:age", 30, 1)],
            parents: Vec::new(),
            txn: None,
            namespace_delta: HashMap::from([(1, "ex:".to_string())]),
            txn_signature: None,
            commit_signatures: Vec::new(),
            txn_meta: Vec::new(),
            graph_delta: HashMap::new(),
            ns_split_mode: None,
        };
        let cid = store_commit(&storage, &commit).await;
        ns.publish_commit("test:main", 1, &cid).await.unwrap();

        let config = IndexerConfig::small()
            .with_data_dir(std::env::temp_dir().join("fluree-test-orch-idx-ledger"));
        let orchestrator = IndexerOrchestrator::new(
            StorageBackend::Managed(Arc::new(storage.clone())),
            ns.clone(),
            config,
        );

        let result = orchestrator.index_ledger("test:main").await.unwrap();
        assert_eq!(result.index_t, 1);
        assert_eq!(result.ledger_id, "test:main");
    }

    #[tokio::test]
    async fn test_orchestrator_index_and_publish() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());

        // Setup a commit
        let commit = Commit {
            id: None,
            t: 1,
            time: None,
            flakes: vec![make_flake(1, "ex:alice", 1, "ex:age", 30, 1)],
            parents: Vec::new(),
            txn: None,
            namespace_delta: HashMap::from([(1, "ex:".to_string())]),
            txn_signature: None,
            commit_signatures: Vec::new(),
            txn_meta: Vec::new(),
            graph_delta: HashMap::new(),
            ns_split_mode: None,
        };
        let cid = store_commit(&storage, &commit).await;
        ns.publish_commit("test:main", 1, &cid).await.unwrap();

        let config = IndexerConfig::small()
            .with_data_dir(std::env::temp_dir().join("fluree-test-orch-idx-publish"));
        let orchestrator = IndexerOrchestrator::new(
            StorageBackend::Managed(Arc::new(storage.clone())),
            ns.clone(),
            config,
        );

        let result = orchestrator.index_and_publish("test:main").await.unwrap();
        assert_eq!(result.index_t, 1);

        // Verify the index was published
        let record = ns.lookup("test:main").await.unwrap().unwrap();
        assert_eq!(record.index_t, 1);
        assert!(record.index_head_id.is_some());
    }

    #[tokio::test]
    async fn test_orchestrator_returns_existing_when_current() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());

        // Setup a commit
        let commit = Commit {
            id: None,
            t: 1,
            time: None,
            flakes: vec![make_flake(1, "ex:alice", 1, "ex:age", 30, 1)],
            parents: Vec::new(),
            txn: None,
            namespace_delta: HashMap::from([(1, "ex:".to_string())]),
            txn_signature: None,
            commit_signatures: Vec::new(),
            txn_meta: Vec::new(),
            graph_delta: HashMap::new(),
            ns_split_mode: None,
        };
        let cid = store_commit(&storage, &commit).await;
        ns.publish_commit("test:main", 1, &cid).await.unwrap();

        let config = IndexerConfig::small()
            .with_data_dir(std::env::temp_dir().join("fluree-test-orch-existing"));
        let orchestrator = IndexerOrchestrator::new(
            StorageBackend::Managed(Arc::new(storage.clone())),
            ns.clone(),
            config,
        );

        // First index
        let result1 = orchestrator.index_and_publish("test:main").await.unwrap();

        // Second index - should return existing
        let result2 = orchestrator.index_ledger("test:main").await.unwrap();

        assert_eq!(result1.root_id, result2.root_id);
        assert_eq!(result2.stats.flake_count, 0); // No work done
    }

    #[tokio::test]
    async fn test_orchestrator_ledger_not_found() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());

        let orchestrator = IndexerOrchestrator::new(
            StorageBackend::Managed(Arc::new(storage)),
            ns.clone(),
            IndexerConfig::small(),
        );

        let result = orchestrator.index_ledger("nonexistent:main").await;
        assert!(result.is_err());
    }

    // =========================================================================
    // Tests for IndexerHandle and completion tracking
    // =========================================================================

    #[tokio::test]
    async fn test_handle_trigger_returns_completion() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        let (worker, handle) = BackgroundIndexerWorker::new(
            StorageBackend::Managed(Arc::new(storage)),
            ns,
            IndexerConfig::small(),
        );

        // Trigger without starting worker - completion should be pending
        let mut completion = handle.trigger("test:main", 1).await;

        // Should not be ready yet (worker not running)
        assert!(completion.try_get().is_none());

        // Drop worker to cancel
        drop(worker);
        drop(handle);

        // Now completion should resolve as cancelled
        let outcome = completion.wait().await;
        assert!(matches!(outcome, IndexOutcome::Cancelled));
    }

    #[tokio::test]
    async fn test_handle_cancel_resolves_waiters_as_cancelled() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        let (_worker, handle) = BackgroundIndexerWorker::new(
            StorageBackend::Managed(Arc::new(storage)),
            ns,
            IndexerConfig::small(),
        );

        // Trigger and get completion
        let completion = handle.trigger("test:main", 1).await;

        // Cancel the ledger
        let had_work = handle.cancel("test:main").await;
        assert!(had_work);

        // Completion should be cancelled
        let outcome = completion.wait().await;
        assert!(matches!(outcome, IndexOutcome::Cancelled));
    }

    #[tokio::test]
    async fn test_handle_cancel_all() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        let (_worker, handle) = BackgroundIndexerWorker::new(
            StorageBackend::Managed(Arc::new(storage)),
            ns,
            IndexerConfig::small(),
        );

        // Trigger multiple ledgers
        let c1 = handle.trigger("test:one", 1).await;
        let c2 = handle.trigger("test:two", 2).await;

        // Cancel all
        handle.cancel_all().await;

        // Both should be cancelled
        assert!(matches!(c1.wait().await, IndexOutcome::Cancelled));
        assert!(matches!(c2.wait().await, IndexOutcome::Cancelled));
    }

    #[tokio::test]
    async fn test_handle_status() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        let (_worker, handle) = BackgroundIndexerWorker::new(
            StorageBackend::Managed(Arc::new(storage)),
            ns,
            IndexerConfig::small(),
        );

        // No status for unknown ledger
        assert!(handle.status("unknown").await.is_none());

        // Trigger a ledger
        let _completion = handle.trigger("test:main", 5).await;

        // Should have status now
        let status = handle.status("test:main").await.unwrap();
        assert_eq!(status.phase, IndexPhase::Pending);
        assert_eq!(status.pending_min_t, Some(5));
        assert_eq!(status.waiter_count, 1);
    }

    #[tokio::test]
    async fn test_handle_is_pending() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        let (_worker, handle) = BackgroundIndexerWorker::new(
            StorageBackend::Managed(Arc::new(storage)),
            ns,
            IndexerConfig::small(),
        );

        assert!(!handle.is_pending("test:main").await);

        let _completion = handle.trigger("test:main", 1).await;

        assert!(handle.is_pending("test:main").await);
    }

    #[tokio::test]
    async fn test_handle_pending_ledgers() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        let (_worker, handle) = BackgroundIndexerWorker::new(
            StorageBackend::Managed(Arc::new(storage)),
            ns,
            IndexerConfig::small(),
        );

        assert!(handle.pending_ledgers().await.is_empty());

        let _c1 = handle.trigger("test:one", 1).await;
        let _c2 = handle.trigger("test:two", 2).await;

        let pending = handle.pending_ledgers().await;
        assert_eq!(pending.len(), 2);
        assert!(pending.contains(&"test:one".to_string()));
        assert!(pending.contains(&"test:two".to_string()));
    }

    #[tokio::test]
    async fn test_handle_multiple_triggers_coalesce_min_t() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        let (_worker, handle) = BackgroundIndexerWorker::new(
            StorageBackend::Managed(Arc::new(storage)),
            ns,
            IndexerConfig::small(),
        );

        // First trigger with min_t=5
        let _c1 = handle.trigger("test:main", 5).await;
        // Second trigger with min_t=3 (lower)
        let _c2 = handle.trigger("test:main", 3).await;

        // Status should show coalesced min_t = 3 (the minimum)
        let status = handle.status("test:main").await.unwrap();
        assert_eq!(status.pending_min_t, Some(3));
        // Should have 2 waiters
        assert_eq!(status.waiter_count, 2);
    }

    #[tokio::test]
    async fn test_handle_trigger_clears_backoff() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        let (worker, handle) = BackgroundIndexerWorker::new(
            StorageBackend::Managed(Arc::new(storage)),
            ns,
            IndexerConfig::small(),
        );

        // Ensure ledger state exists.
        let _c1 = handle.trigger("test:main", 1).await;

        // Put it into backoff.
        worker.schedule_retry("test:main", "boom").await;
        {
            let states = handle.states.lock().await;
            let state = states.get("test:main").expect("state exists");
            assert!(state.next_retry_at.is_some());
            assert!(state.retry_count > 0);
        }

        // New trigger should clear backoff.
        let _c2 = handle.trigger("test:main", 2).await;
        {
            let states = handle.states.lock().await;
            let state = states.get("test:main").expect("state exists");
            assert!(state.next_retry_at.is_none());
            assert_eq!(state.retry_count, 0);
        }
    }

    #[tokio::test]
    async fn test_handle_wait_for_idle_immediate_return() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        let (_worker, handle) = BackgroundIndexerWorker::new(
            StorageBackend::Managed(Arc::new(storage)),
            ns,
            IndexerConfig::small(),
        );

        // Should return immediately for unknown ledger
        handle.wait_for_idle("unknown").await;
    }

    #[tokio::test]
    async fn test_handle_wait_all_idle_immediate_return() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        let (_worker, handle) = BackgroundIndexerWorker::new(
            StorageBackend::Managed(Arc::new(storage)),
            ns,
            IndexerConfig::small(),
        );

        // Should return immediately when nothing pending
        handle.wait_all_idle().await;
    }

    #[tokio::test]
    async fn test_index_completion_debug() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        let (_worker, handle) = BackgroundIndexerWorker::new(
            StorageBackend::Managed(Arc::new(storage)),
            ns,
            IndexerConfig::small(),
        );

        let completion = handle.trigger("test:main", 1).await;
        // Should not panic
        let debug_str = format!("{completion:?}");
        assert!(debug_str.contains("IndexCompletion"));
    }

    #[tokio::test]
    async fn test_index_outcome_clone() {
        let outcome = IndexOutcome::Completed {
            index_t: 5,
            root_id: None,
        };
        let cloned = outcome.clone();
        assert!(matches!(cloned, IndexOutcome::Completed { index_t: 5, .. }));

        let failed = IndexOutcome::Failed("error".to_string());
        let cloned_failed = failed.clone();
        assert!(matches!(cloned_failed, IndexOutcome::Failed(_)));

        let cancelled = IndexOutcome::Cancelled;
        let cloned_cancelled = cancelled.clone();
        assert!(matches!(cloned_cancelled, IndexOutcome::Cancelled));
    }

    #[tokio::test]
    async fn test_index_status_snapshot_clone() {
        let snapshot = IndexStatusSnapshot {
            phase: IndexPhase::Pending,
            pending_min_t: Some(5),
            last_index_t: 3,
            last_error: Some("test error".to_string()),
            waiter_count: 2,
        };
        let cloned = snapshot.clone();
        assert_eq!(cloned.phase, IndexPhase::Pending);
        assert_eq!(cloned.pending_min_t, Some(5));
        assert_eq!(cloned.last_index_t, 3);
        assert_eq!(cloned.last_error, Some("test error".to_string()));
        assert_eq!(cloned.waiter_count, 2);
    }
}

// =============================================================================
// Tests for embedded-orchestrator feature
// =============================================================================

#[cfg(all(test, feature = "embedded-orchestrator"))]
mod embedded_tests {
    use super::*;
    use fluree_db_core::{
        ContentAddressedWrite, ContentId, ContentKind, Flake, FlakeValue, LedgerSnapshot,
        MemoryStorage, Sid,
    };
    use fluree_db_ledger::LedgerState;
    use fluree_db_nameservice::memory::MemoryNameService;
    use fluree_db_nameservice::Publisher;
    use fluree_db_novelty::{Commit, Novelty};
    use std::collections::HashMap;

    fn make_flake(s_code: u16, s_name: &str, p_code: u16, p_name: &str, val: i64, t: i64) -> Flake {
        Flake::new(
            Sid::new(s_code, s_name),
            Sid::new(p_code, p_name),
            FlakeValue::Long(val),
            Sid::new(2, "xsd:long"),
            t,
            true,
            None,
        )
    }

    fn make_large_flake(t: i64, size_hint: usize) -> Flake {
        let big_value = "x".repeat(size_hint);
        Flake::new(
            Sid::new(1, "ex:entity"),
            Sid::new(1, "ex:data"),
            FlakeValue::String(big_value),
            Sid::new(2, "xsd:string"),
            t,
            true,
            None,
        )
    }

    async fn store_commit(storage: &MemoryStorage, commit: &Commit) -> ContentId {
        use fluree_db_core::commit::codec::envelope::{encode_envelope_fields, CodecEnvelope};
        use fluree_db_core::commit::codec::format::{
            self, CommitFooter, CommitHeader, FOOTER_LEN, HEADER_LEN,
        };
        use fluree_db_core::commit::codec::op_codec::{encode_op, CommitDicts};

        let mut dicts = CommitDicts::new();
        let mut ops_buf = Vec::new();
        for f in &commit.flakes {
            encode_op(f, &mut dicts, &mut ops_buf).unwrap();
        }

        let envelope = CodecEnvelope {
            t: commit.t,
            parents: commit.parents.clone(),
            namespace_delta: commit
                .namespace_delta
                .iter()
                .map(|(k, v)| (*k, v.clone()))
                .collect::<HashMap<_, _>>(),
            txn: commit.txn.clone(),
            time: commit.time.clone(),
            txn_signature: None,
            txn_meta: Vec::new(),
            graph_delta: HashMap::new(),
            ns_split_mode: None,
        };
        let mut envelope_bytes = Vec::new();
        encode_envelope_fields(&envelope, &mut envelope_bytes).unwrap();

        let dict_bytes: Vec<Vec<u8>> = vec![
            dicts.graph.serialize(),
            dicts.subject.serialize(),
            dicts.predicate.serialize(),
            dicts.datatype.serialize(),
            dicts.object_ref.serialize(),
        ];

        let ops_section_len = ops_buf.len() as u32;
        let envelope_len = envelope_bytes.len() as u32;
        let dict_start = HEADER_LEN + envelope_bytes.len() + ops_buf.len();
        let mut dict_locations = [format::DictLocation::default(); 5];
        let mut offset = dict_start as u64;
        for (i, d) in dict_bytes.iter().enumerate() {
            dict_locations[i] = format::DictLocation {
                offset,
                len: d.len() as u32,
            };
            offset += d.len() as u64;
        }

        let footer = CommitFooter {
            dicts: dict_locations,
            ops_section_len,
        };
        let header = CommitHeader {
            version: format::VERSION,
            flags: 0,
            t: commit.t,
            op_count: commit.flakes.len() as u32,
            envelope_len,
            sig_block_len: 0,
        };

        // V4: no embedded hash
        let total_len = HEADER_LEN
            + envelope_bytes.len()
            + ops_buf.len()
            + dict_bytes.iter().map(std::vec::Vec::len).sum::<usize>()
            + FOOTER_LEN;
        let mut blob = vec![0u8; total_len];

        let mut pos = 0;
        header.write_to(&mut blob[pos..]);
        pos += HEADER_LEN;
        blob[pos..pos + envelope_bytes.len()].copy_from_slice(&envelope_bytes);
        pos += envelope_bytes.len();
        blob[pos..pos + ops_buf.len()].copy_from_slice(&ops_buf);
        pos += ops_buf.len();
        for d in &dict_bytes {
            blob[pos..pos + d.len()].copy_from_slice(d);
            pos += d.len();
        }
        footer.write_to(&mut blob[pos..]);

        // Write via content_write_bytes (CID = SHA-256 of full blob)
        storage
            .content_write_bytes(ContentKind::Commit, "test:main", &blob)
            .await
            .unwrap();

        ContentId::new(ContentKind::Commit, &blob)
    }

    #[tokio::test]
    async fn test_maybe_refresh_below_threshold_returns_not_attempted() {
        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let ns = MemoryNameService::new();
        let index_config = IndexConfig {
            reindex_min_bytes: 100_000,
            reindex_max_bytes: 1_000_000_000,
        }; // High threshold
        let indexer_config = IndexerConfig::small();

        let (returned_ledger, result) =
            maybe_refresh_after_commit(&storage, &ns, ledger, &index_config, indexer_config, 1)
                .await;

        // Should not have attempted since threshold not met (default is high)
        assert!(!result.attempted);
        assert!(!result.refreshed);
        assert!(!result.published);
        assert!(!result.applied);
        assert!(result.refresh.is_none());
        assert!(result.error.is_none());
        assert_eq!(returned_ledger.t(), 0);
    }

    #[tokio::test]
    async fn test_maybe_refresh_empty_novelty_returns_not_attempted() {
        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0); // empty
        let ledger = LedgerState::new(db, novelty);

        let ns = MemoryNameService::new();
        // Low threshold, but novelty is empty so the early-return should trigger
        let index_config = IndexConfig {
            reindex_min_bytes: 1,
            reindex_max_bytes: 10000,
        };
        let indexer_config = IndexerConfig::small();

        let (_returned_ledger, result) =
            maybe_refresh_after_commit(&storage, &ns, ledger, &index_config, indexer_config, 0)
                .await;

        assert!(!result.attempted);
    }

    #[tokio::test]
    async fn test_maybe_refresh_above_threshold_attempts_refresh() {
        let storage = MemoryStorage::new();
        let ns = MemoryNameService::new();

        // Store a commit-v2 blob
        let commit = Commit {
            id: None,
            t: 1,
            time: None,
            flakes: vec![make_flake(1, "ex:alice", 1, "ex:age", 30, 1)],
            parents: Vec::new(),
            txn: None,
            namespace_delta: HashMap::from([(1, "ex:".to_string())]),
            txn_signature: None,
            commit_signatures: Vec::new(),
            txn_meta: Vec::new(),
            graph_delta: HashMap::new(),
            ns_split_mode: None,
        };
        let cid = store_commit(&storage, &commit).await;
        ns.publish_commit("test:main", 1, &cid).await.unwrap();

        // Create a LedgerState with enough novelty to trigger threshold
        let db = LedgerSnapshot::genesis("test:main");
        let mut novelty = Novelty::new(0);
        let large_flake = make_large_flake(2, 2000);
        novelty
            .apply_commit(vec![large_flake], 2, &HashMap::new())
            .unwrap();
        let ledger = LedgerState::new(db, novelty);

        let index_config = IndexConfig {
            reindex_min_bytes: 100,
            reindex_max_bytes: 10000,
        };
        let indexer_config = IndexerConfig::small()
            .with_data_dir(std::env::temp_dir().join("fluree-test-emb-refresh"));

        let (_returned_ledger, result) =
            maybe_refresh_after_commit(&storage, &ns, ledger, &index_config, indexer_config, 2)
                .await;

        assert!(result.attempted, "expected attempted");
        assert!(
            result.refreshed,
            "expected refreshed, error: {:?}",
            result.error
        );
        assert!(result.refresh.is_some(), "expected refresh result");
        assert!(result.published, "expected published");
    }

    #[tokio::test]
    async fn test_require_refresh_success_path() {
        let storage = MemoryStorage::new();
        let ns = MemoryNameService::new();

        // Store a commit-v2 blob
        let commit = Commit {
            id: None,
            t: 1,
            time: None,
            flakes: vec![make_flake(1, "ex:alice", 1, "ex:age", 30, 1)],
            parents: Vec::new(),
            txn: None,
            namespace_delta: HashMap::from([(1, "ex:".to_string())]),
            txn_signature: None,
            commit_signatures: Vec::new(),
            txn_meta: Vec::new(),
            graph_delta: HashMap::new(),
            ns_split_mode: None,
        };
        let cid = store_commit(&storage, &commit).await;
        ns.publish_commit("test:main", 1, &cid).await.unwrap();

        let db = LedgerSnapshot::genesis("test:main");
        let mut novelty = Novelty::new(0);
        let flake = make_flake(1, "ex:bob", 1, "ex:age", 25, 2);
        novelty
            .apply_commit(vec![flake], 2, &HashMap::new())
            .unwrap();
        let ledger = LedgerState::new(db, novelty);

        let indexer_config = IndexerConfig::small()
            .with_data_dir(std::env::temp_dir().join("fluree-test-emb-require"));

        let result = require_refresh_before_commit(&storage, &ns, ledger, indexer_config, 2).await;

        // Phase 4.3-4.5 is complete: apply_index now succeeds with v2 roots.
        let updated_ledger = result.expect("require_refresh should succeed");
        assert!(
            updated_ledger.t() > 0,
            "ledger should have advanced past genesis"
        );
    }

    #[tokio::test]
    async fn test_require_refresh_no_commits_returns_error() {
        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let ns = MemoryNameService::new();
        let indexer_config = IndexerConfig::small();

        // Ledger not in nameservice → should return LedgerNotFound
        let result = require_refresh_before_commit(&storage, &ns, ledger, indexer_config, 0).await;

        assert!(result.is_err());
    }
}
