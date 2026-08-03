//! Background BM25 maintenance worker
//!
//! This module provides a background worker that automatically syncs BM25 indexes
//! when their source ledgers are updated. It subscribes to nameservice events and
//! triggers sync operations for dependent graph sources.
//!
//! # Architecture
//!
//! The worker maintains a reverse dependency map (ledger -> graph sources) and subscribes
//! to nameservice events. When a `LedgerCommitPublished` event is received, it
//! enqueues sync tasks for all dependent graph sources.
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_api::{FlureeBuilder, Bm25MaintenanceWorker};
//!
//! let fluree = Arc::new(FlureeBuilder::memory().build_memory());
//!
//! // Start the maintenance worker
//! let worker = Bm25MaintenanceWorker::new(Arc::clone(&fluree));
//! let handle = worker.handle();
//! tokio::spawn(async move { worker.run().await.ok(); });
//!
//! // Register a graph source for automatic sync
//! handle.register_graph_source(fluree.nameservice(), "my-search:main").await?;
//!
//! // Stop the worker when done
//! handle.stop();
//! ```

use crate::{ApiError, Result};
use fluree_db_nameservice::{GraphSourcePublisher, NameServiceEvent, NameServiceLookup};
use futures::StreamExt;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::time::{self, Duration, Instant};
use tracing::{debug, error, info, warn};

/// Type alias for a pinned boxed future used in the BM25 sync worker.
///
/// `Send` so [`Bm25MaintenanceWorker::run`] can be driven by a multi-threaded
/// executor via `tokio::spawn`.
type SyncFuture<'a> = Pin<Box<dyn Future<Output = (String, Result<()>)> + Send + 'a>>;

/// Remove up to `capacity` graph sources from `pending` that have no sync
/// running, and return them.
///
/// Sources in `in_flight` are left in `pending` rather than dropped: the commit
/// that queued one still needs to reach the index, it just has to wait for the
/// running sync to finish. Starting a second sync of the same index instead
/// would have both publish a snapshot against the same manifest, orphaning one
/// of them and moving the watermark backwards whenever the higher-`t` sync
/// publishes first.
fn take_ready_for_sync(
    pending: &mut HashSet<String>,
    in_flight: &HashSet<String>,
    capacity: usize,
) -> Vec<String> {
    let ready: Vec<String> = pending
        .iter()
        .filter(|graph_source_id| !in_flight.contains(*graph_source_id))
        .take(capacity)
        .cloned()
        .collect();

    for graph_source_id in &ready {
        pending.remove(graph_source_id);
    }

    ready
}

/// Configuration for the BM25 maintenance worker.
#[derive(Debug, Clone)]
pub struct Bm25WorkerConfig {
    /// Maximum number of concurrent sync operations.
    pub max_concurrent_syncs: usize,
    /// Whether to auto-register graph sources on creation.
    pub auto_register: bool,
    /// Debounce interval in milliseconds (delay sync to batch rapid commits).
    pub debounce_ms: u64,
}

impl Default for Bm25WorkerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_syncs: 4,
            auto_register: true,
            debounce_ms: 100,
        }
    }
}

/// Statistics for the maintenance worker.
#[derive(Debug, Clone, Default)]
pub struct Bm25WorkerStats {
    /// Total number of sync operations performed.
    pub syncs_performed: u64,
    /// Number of sync operations that failed.
    pub syncs_failed: u64,
    /// Number of events received.
    pub events_received: u64,
    /// Number of registered graph sources.
    pub registered_graph_sources: usize,
}

/// State for the BM25 maintenance worker.
///
/// Shared between the worker and its [`Bm25WorkerHandle`] behind a `Mutex`;
/// every operation on it is a short in-memory map update, never held across an
/// await.
pub struct Bm25WorkerState {
    /// Reverse dependency map: ledger_id -> set of graph source IDs.
    ledger_to_graph_sources: HashMap<String, HashSet<String>>,
    /// Forward map: graph_source_id -> set of ledger_ides (for unregistration).
    gs_to_ledgers: HashMap<String, HashSet<String>>,
    /// Statistics.
    stats: Bm25WorkerStats,
}

impl Bm25WorkerState {
    /// Create a new empty worker state.
    pub fn new() -> Self {
        Self {
            ledger_to_graph_sources: HashMap::new(),
            gs_to_ledgers: HashMap::new(),
            stats: Bm25WorkerStats::default(),
        }
    }

    /// Register a graph source with its dependencies.
    pub fn register_graph_source(&mut self, graph_source_id: &str, dependencies: &[String]) {
        let deps_set: HashSet<String> = dependencies.iter().cloned().collect();

        // Update forward map
        self.gs_to_ledgers
            .insert(graph_source_id.to_string(), deps_set.clone());

        // Update reverse map
        for ledger in &deps_set {
            self.ledger_to_graph_sources
                .entry(ledger.clone())
                .or_default()
                .insert(graph_source_id.to_string());
        }

        self.stats.registered_graph_sources = self.gs_to_ledgers.len();
        debug!(
            graph_source_id,
            ?dependencies,
            "Registered graph source for maintenance"
        );
    }

    /// Unregister a graph source.
    pub fn unregister_graph_source(&mut self, graph_source_id: &str) {
        if let Some(ledgers) = self.gs_to_ledgers.remove(graph_source_id) {
            // Remove from reverse map
            for ledger in ledgers {
                if let Some(graph_sources) = self.ledger_to_graph_sources.get_mut(&ledger) {
                    graph_sources.remove(graph_source_id);
                    if graph_sources.is_empty() {
                        self.ledger_to_graph_sources.remove(&ledger);
                    }
                }
            }
        }
        self.stats.registered_graph_sources = self.gs_to_ledgers.len();
        debug!(
            graph_source_id,
            "Unregistered graph source from maintenance"
        );
    }

    /// Get graph sources that depend on a ledger.
    pub fn graph_sources_for_ledger(&self, ledger_id: &str) -> Vec<String> {
        self.ledger_to_graph_sources
            .get(ledger_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get all registered graph sources.
    pub fn registered_graph_sources(&self) -> Vec<String> {
        self.gs_to_ledgers.keys().cloned().collect()
    }

    /// Get all watched ledgers.
    pub fn watched_ledgers(&self) -> Vec<String> {
        self.ledger_to_graph_sources.keys().cloned().collect()
    }

    /// Record a sync operation.
    pub fn record_sync(&mut self, success: bool) {
        self.stats.syncs_performed += 1;
        if !success {
            self.stats.syncs_failed += 1;
        }
    }

    /// Record an event.
    pub fn record_event(&mut self) {
        self.stats.events_received += 1;
    }

    /// Get current stats.
    pub fn stats(&self) -> &Bm25WorkerStats {
        &self.stats
    }
}

impl Default for Bm25WorkerState {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle to interact with a running BM25 maintenance worker.
///
/// This handle allows registering/unregistering graph sources and stopping the worker.
pub struct Bm25WorkerHandle {
    state: Arc<Mutex<Bm25WorkerState>>,
    /// Signal to stop the worker (set to true to request stop).
    stop_requested: Arc<AtomicBool>,
}

impl Bm25WorkerHandle {
    /// Register a graph source for automatic maintenance.
    ///
    /// The worker will sync this graph source whenever any of its source ledgers are updated.
    pub async fn register_graph_source<N: NameServiceLookup + GraphSourcePublisher>(
        &self,
        ns: &N,
        graph_source_id: &str,
    ) -> Result<()> {
        // Look up graph source to get its dependencies
        let record = ns
            .lookup_graph_source(graph_source_id)
            .await?
            .ok_or_else(|| {
                ApiError::NotFound(format!("Graph source not found: {graph_source_id}"))
            })?;

        self.state
            .lock()
            .register_graph_source(graph_source_id, &record.dependencies);
        Ok(())
    }

    /// Register a graph source with explicit dependencies (no nameservice lookup).
    pub fn register_graph_source_with_deps(&self, graph_source_id: &str, dependencies: &[String]) {
        self.state
            .lock()
            .register_graph_source(graph_source_id, dependencies);
    }

    /// Unregister a graph source from automatic maintenance.
    pub fn unregister_graph_source(&self, graph_source_id: &str) {
        self.state.lock().unregister_graph_source(graph_source_id);
    }

    /// Get current worker statistics.
    pub fn stats(&self) -> Bm25WorkerStats {
        self.state.lock().stats().clone()
    }

    /// Get all registered graph sources.
    pub fn registered_graph_sources(&self) -> Vec<String> {
        self.state.lock().registered_graph_sources()
    }

    /// Request the worker to stop.
    pub fn stop(&self) {
        self.stop_requested.store(true, Ordering::Relaxed);
        info!("BM25 maintenance worker stop requested");
    }

    /// Check if stop has been requested.
    pub fn is_stop_requested(&self) -> bool {
        self.stop_requested.load(Ordering::Relaxed)
    }
}

/// BM25 maintenance worker.
///
/// Monitors nameservice events and automatically syncs BM25 indexes when their
/// source ledgers are updated.
pub struct Bm25MaintenanceWorker {
    fluree: Arc<crate::Fluree>,
    config: Bm25WorkerConfig,
    state: Arc<Mutex<Bm25WorkerState>>,
    stop_requested: Arc<AtomicBool>,
}

impl Bm25MaintenanceWorker {
    /// Create a new maintenance worker.
    ///
    /// Takes an owned handle rather than a borrow so the worker is `'static`
    /// and can be moved into a spawned task.
    pub fn new(fluree: Arc<crate::Fluree>) -> Self {
        Self {
            fluree,
            config: Bm25WorkerConfig::default(),
            state: Arc::new(Mutex::new(Bm25WorkerState::new())),
            stop_requested: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Create a new maintenance worker with custom config.
    pub fn with_config(fluree: Arc<crate::Fluree>, config: Bm25WorkerConfig) -> Self {
        Self {
            fluree,
            config,
            state: Arc::new(Mutex::new(Bm25WorkerState::new())),
            stop_requested: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Get a handle to interact with the worker.
    pub fn handle(&self) -> Bm25WorkerHandle {
        Bm25WorkerHandle {
            state: self.state.clone(),
            stop_requested: self.stop_requested.clone(),
        }
    }

    /// Process a single nameservice event.
    ///
    /// Returns the list of graph source IDs that need syncing.
    pub fn process_event(&self, event: &NameServiceEvent) -> Vec<String> {
        self.state.lock().record_event();

        match event {
            NameServiceEvent::LedgerCommitPublished {
                ledger_id,
                commit_t,
                ..
            } => {
                let graph_sources = self.state.lock().graph_sources_for_ledger(ledger_id);
                if !graph_sources.is_empty() {
                    info!(
                        ledger = %ledger_id,
                        commit_t,
                        gs_count = graph_sources.len(),
                        "Ledger commit triggers graph source sync"
                    );
                }
                graph_sources
            }
            NameServiceEvent::LedgerIndexPublished {
                ledger_id, index_t, ..
            } => {
                // Index updates don't require graph source sync (commit already triggered it)
                debug!(ledger = %ledger_id, index_t, "Ledger index published (no graph source sync needed)");
                vec![]
            }
            NameServiceEvent::GraphSourceConfigPublished {
                graph_source_id,
                dependencies,
                ..
            } => {
                // Auto-register graph source if configured
                if self.config.auto_register {
                    self.state
                        .lock()
                        .register_graph_source(graph_source_id, dependencies);
                    info!(graph_source = %graph_source_id, "Auto-registered graph source for maintenance");
                }
                vec![]
            }
            NameServiceEvent::GraphSourceRetracted { graph_source_id } => {
                // Unregister retracted graph source
                self.state.lock().unregister_graph_source(graph_source_id);
                info!(graph_source = %graph_source_id, "Unregistered retracted graph source");
                vec![]
            }
            _ => vec![], // Other events don't trigger sync
        }
    }

    /// Sync a single graph source (called by the event loop).
    pub async fn sync_graph_source(&self, graph_source_id: &str) -> Result<()> {
        debug!(graph_source = %graph_source_id, "Syncing graph source");

        match self.fluree.sync_bm25_index(graph_source_id).await {
            Ok(result) => {
                self.state.lock().record_sync(true);
                info!(
                    graph_source = %graph_source_id,
                    upserted = result.upserted,
                    removed = result.removed,
                    new_watermark = result.new_watermark,
                    "Graph source sync completed"
                );
                Ok(())
            }
            Err(e) => {
                self.state.lock().record_sync(false);
                error!(graph_source = %graph_source_id, error = %e, "Graph source sync failed");
                Err(e)
            }
        }
    }

    /// Run the maintenance loop.
    ///
    /// This subscribes to nameservice events and processes them until stopped.
    /// The returned future is `Send`, so it can be driven by a multi-threaded
    /// runtime with `tokio::spawn`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let worker = Bm25MaintenanceWorker::new(Arc::clone(&fluree));
    /// let handle = worker.handle();
    ///
    /// tokio::spawn(async move { worker.run().await.ok(); });
    ///
    /// // Later, stop the worker
    /// handle.stop();
    /// ```
    pub async fn run(&self) -> Result<()> {
        info!("Starting BM25 maintenance worker");

        // Subscribe to all nameservice events (ledger and graph source changes).
        let mut subscription = self
            .fluree
            .event_bus()
            .subscribe(fluree_db_nameservice::SubscriptionScope::All);

        // Debounced batching: we accumulate graph sources to sync and flush them after `debounce_ms`.
        let mut pending: HashSet<String> = HashSet::new();
        let mut next_flush: Option<Instant> = None;

        // In-flight syncs (bounded by config.max_concurrent_syncs).
        let mut in_flight: futures::stream::FuturesUnordered<SyncFuture<'_>> =
            futures::stream::FuturesUnordered::new();

        // The graph sources those syncs are running against, so a commit
        // arriving mid-sync re-queues the source instead of starting a second
        // concurrent sync of the same index.
        let mut in_flight_ids: HashSet<String> = HashSet::new();

        loop {
            // Check for stop request
            if self.stop_requested.load(Ordering::Relaxed) {
                info!("BM25 maintenance worker stopping");
                break;
            }

            // Flush pending syncs if debounce timer elapsed and we have capacity.
            let now = Instant::now();
            let can_flush = next_flush.map(|t| now >= t).unwrap_or(false);
            if can_flush {
                let capacity = self
                    .config
                    .max_concurrent_syncs
                    .saturating_sub(in_flight.len());

                for graph_source_id in take_ready_for_sync(&mut pending, &in_flight_ids, capacity) {
                    in_flight_ids.insert(graph_source_id.clone());

                    // Spawn a non-Send future into our in-flight set (polled on this task).
                    let fut = async move {
                        let res = self.sync_graph_source(&graph_source_id).await;
                        (graph_source_id, res)
                    };
                    in_flight.push(Box::pin(fut));
                }

                // Anything left in `pending` is over the concurrency cap or is
                // waiting behind a running sync of the same source, so keep the
                // deadline armed to retry it.
                if pending.is_empty() {
                    next_flush = None;
                } else {
                    next_flush =
                        Some(Instant::now() + Duration::from_millis(self.config.debounce_ms));
                }
            }

            // Compute a sleep duration: either until next flush or a small tick for stop checks.
            let sleep_until =
                next_flush.unwrap_or_else(|| Instant::now() + Duration::from_millis(100));
            let sleep_fut = time::sleep_until(sleep_until);
            tokio::pin!(sleep_fut);

            tokio::select! {
                biased;

                // Prefer stop checks + flushing, but still service events promptly.
                res = subscription.receiver.recv() => {
                    match res {
                        Ok(event) => {
                            let sources_to_sync = self.process_event(&event);
                            if !sources_to_sync.is_empty() {
                                for gs in sources_to_sync {
                                    pending.insert(gs);
                                }
                                next_flush = Some(Instant::now() + Duration::from_millis(self.config.debounce_ms));
                            }
                        }
                        Err(e) => {
                            // Broadcast channel lagged or closed
                            warn!(error = %e, "Event channel error, resubscribing");
                            subscription = self
                                .fluree
                                .event_bus()
                                .subscribe(fluree_db_nameservice::SubscriptionScope::All);
                        }
                    }
                }

                // Complete one in-flight sync.
                Some((graph_source_id, res)) = in_flight.next() => {
                    in_flight_ids.remove(&graph_source_id);
                    if let Err(e) = res {
                        warn!(graph_source = %graph_source_id, error = %e, "Failed to sync graph source");
                    }
                }

                // Debounce tick / stop-check tick
                () = &mut sleep_fut => {}
            }
        }

        info!("BM25 maintenance worker stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id_set<const N: usize>(ids: [&str; N]) -> HashSet<String> {
        ids.iter().map(|id| (*id).to_string()).collect()
    }

    #[test]
    fn take_ready_for_sync_defers_a_source_that_is_already_syncing() {
        let mut pending = id_set(["search:main"]);

        let ready = take_ready_for_sync(&mut pending, &id_set(["search:main"]), 4);

        assert!(ready.is_empty(), "a running sync blocks a second one");
        assert!(
            pending.contains("search:main"),
            "the deferred source stays queued for a later flush"
        );
    }

    #[test]
    fn take_ready_for_sync_takes_sources_that_are_not_syncing() {
        let mut pending = id_set(["search:main", "titles:main"]);

        let ready = take_ready_for_sync(&mut pending, &id_set(["search:main"]), 4);

        assert_eq!(ready, vec!["titles:main".to_string()]);
        assert_eq!(pending, id_set(["search:main"]));
    }

    #[test]
    fn take_ready_for_sync_stops_at_capacity() {
        let mut pending = id_set(["a:main", "b:main", "c:main"]);

        let ready = take_ready_for_sync(&mut pending, &HashSet::new(), 2);

        assert_eq!(ready.len(), 2);
        assert_eq!(pending.len(), 1, "the remainder waits for free capacity");
    }

    #[test]
    fn take_ready_for_sync_takes_nothing_without_capacity() {
        let mut pending = id_set(["search:main"]);

        let ready = take_ready_for_sync(&mut pending, &HashSet::new(), 0);

        assert!(ready.is_empty());
        assert_eq!(pending, id_set(["search:main"]));
    }

    #[test]
    fn test_worker_state_register_graph_source() {
        let mut state = Bm25WorkerState::new();

        state.register_graph_source(
            "search:main",
            &["ledger1:main".to_string(), "ledger2:main".to_string()],
        );

        assert_eq!(state.registered_graph_sources(), vec!["search:main"]);
        assert!(state
            .watched_ledgers()
            .contains(&"ledger1:main".to_string()));
        assert!(state
            .watched_ledgers()
            .contains(&"ledger2:main".to_string()));

        let graph_sources = state.graph_sources_for_ledger("ledger1:main");
        assert_eq!(graph_sources, vec!["search:main"]);
    }

    #[test]
    fn test_worker_state_unregister_graph_source() {
        let mut state = Bm25WorkerState::new();

        state.register_graph_source("search:main", &["ledger1:main".to_string()]);
        state.register_graph_source("other:main", &["ledger1:main".to_string()]);

        // Both graph sources depend on ledger1
        let graph_sources = state.graph_sources_for_ledger("ledger1:main");
        assert_eq!(graph_sources.len(), 2);

        // Unregister one
        state.unregister_graph_source("search:main");

        let graph_sources = state.graph_sources_for_ledger("ledger1:main");
        assert_eq!(graph_sources, vec!["other:main"]);

        // Unregister the other
        state.unregister_graph_source("other:main");

        let graph_sources = state.graph_sources_for_ledger("ledger1:main");
        assert!(graph_sources.is_empty());
        assert!(state.watched_ledgers().is_empty());
    }

    #[test]
    fn test_worker_state_multiple_dependencies() {
        let mut state = Bm25WorkerState::new();

        // gs1 depends on ledger1 and ledger2
        state.register_graph_source(
            "gs1:main",
            &["ledger1:main".to_string(), "ledger2:main".to_string()],
        );
        // gs2 depends on ledger2 and ledger3
        state.register_graph_source(
            "gs2:main",
            &["ledger2:main".to_string(), "ledger3:main".to_string()],
        );

        // ledger1 triggers only gs1
        let graph_sources = state.graph_sources_for_ledger("ledger1:main");
        assert_eq!(graph_sources, vec!["gs1:main"]);

        // ledger2 triggers both
        let mut graph_sources = state.graph_sources_for_ledger("ledger2:main");
        graph_sources.sort();
        assert_eq!(graph_sources, vec!["gs1:main", "gs2:main"]);

        // ledger3 triggers only gs2
        let graph_sources = state.graph_sources_for_ledger("ledger3:main");
        assert_eq!(graph_sources, vec!["gs2:main"]);
    }

    #[test]
    fn test_worker_stats() {
        let mut state = Bm25WorkerState::new();

        state.register_graph_source("gs:main", &["ledger:main".to_string()]);
        assert_eq!(state.stats().registered_graph_sources, 1);

        state.record_event();
        state.record_event();
        assert_eq!(state.stats().events_received, 2);

        state.record_sync(true);
        state.record_sync(false);
        assert_eq!(state.stats().syncs_performed, 2);
        assert_eq!(state.stats().syncs_failed, 1);
    }

    /// The worker and its handle have to be `Send` for `tokio::spawn` to accept
    /// `run()` on a multi-threaded runtime, which is how the server drives it.
    /// This is a compile-time assertion — it fails to build, not at runtime, if
    /// the shared state regresses to a non-`Send` container.
    #[test]
    fn worker_types_are_send() {
        fn assert_send<T: Send>() {}

        assert_send::<Bm25WorkerState>();
        assert_send::<Bm25WorkerHandle>();
        assert_send::<Bm25MaintenanceWorker>();
    }

    /// `tokio::spawn` needs `Send + 'static`, so the worker must be movable
    /// into a task, not merely thread-safe. This is what the owned
    /// `Arc<Fluree>` buys over a borrow.
    #[test]
    fn worker_is_spawnable() {
        fn assert_spawnable<T: Send + 'static>() {}

        assert_spawnable::<Bm25MaintenanceWorker>();
        assert_spawnable::<Bm25WorkerHandle>();
    }

    /// `run()`'s future must also be `Send`, which the `SyncFuture` bound and
    /// the absence of a lock guard held across an await are what buy us.
    #[test]
    fn worker_run_future_is_send() {
        fn assert_send_future<F: Future + Send>(_: F) {}

        // Never polled — this exists so the compiler checks the bound.
        let _check = |worker: &Bm25MaintenanceWorker| assert_send_future(worker.run());
    }
}
