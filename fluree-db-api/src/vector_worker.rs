//! Background Vector Maintenance Worker
//!
//! This module provides a background worker that automatically syncs vector indexes
//! when their source ledgers are updated. It subscribes to nameservice events and
//! triggers sync operations for dependent graph sources.
//!
//! # Architecture
//!
//! The worker maintains a reverse dependency map (ledger -> graph sources) and subscribes
//! to nameservice events. When a `LedgerCommitPublished` event is received, it
//! enqueues sync tasks for all dependent vector graph sources.
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_api::{FlureeBuilder, VectorMaintenanceWorker};
//!
//! let fluree = FlureeBuilder::memory().build_memory();
//!
//! // Start the maintenance worker
//! let worker = VectorMaintenanceWorker::new(&fluree);
//! let handle = worker.handle();
//!
//! // Register a graph source for automatic sync
//! handle.register_graph_source(&fluree.nameservice(), "embeddings:main").await?;
//!
//! // Stop the worker when done
//! handle.stop();
//! ```

#[cfg(feature = "vector")]
use crate::{ApiError, Result};
#[cfg(feature = "vector")]
use fluree_db_nameservice::{GraphSourcePublisher, NameService, NameServiceEvent};
#[cfg(feature = "vector")]
use futures::StreamExt;
#[cfg(feature = "vector")]
use std::cell::RefCell;
#[cfg(feature = "vector")]
use std::collections::{HashMap, HashSet};
#[cfg(feature = "vector")]
use std::rc::Rc;
#[cfg(feature = "vector")]
use tokio::time::{self, Duration, Instant};
#[cfg(feature = "vector")]
use tracing::{debug, error, info, warn};

/// Configuration for the Vector maintenance worker.
#[cfg(feature = "vector")]
#[derive(Debug, Clone)]
pub struct VectorWorkerConfig {
    /// Maximum number of concurrent sync operations.
    pub max_concurrent_syncs: usize,
    /// Whether to auto-register graph sources on creation.
    pub auto_register: bool,
    /// Debounce interval in milliseconds (delay sync to batch rapid commits).
    pub debounce_ms: u64,
}

#[cfg(feature = "vector")]
impl Default for VectorWorkerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_syncs: 4,
            auto_register: true,
            debounce_ms: 100,
        }
    }
}

/// Statistics for the maintenance worker.
#[cfg(feature = "vector")]
#[derive(Debug, Clone, Default)]
pub struct VectorWorkerStats {
    /// Total number of sync operations performed.
    pub syncs_performed: u64,
    /// Number of sync operations that failed.
    pub syncs_failed: u64,
    /// Number of events received.
    pub events_received: u64,
    /// Number of registered graph sources.
    pub registered_graph_sources: usize,
}

/// State for the Vector maintenance worker (single-threaded).
///
/// Uses `RefCell` for interior mutability to work in single-threaded contexts.
#[cfg(feature = "vector")]
pub struct VectorWorkerState {
    /// Reverse dependency map: ledger_id -> set of graph source IDs.
    ledger_to_graph_sources: HashMap<String, HashSet<String>>,
    /// Forward map: graph_source_id -> set of ledger_ides (for unregistration).
    gs_to_ledgers: HashMap<String, HashSet<String>>,
    /// Statistics.
    stats: VectorWorkerStats,
}

#[cfg(feature = "vector")]
impl VectorWorkerState {
    /// Create a new empty worker state.
    pub fn new() -> Self {
        Self {
            ledger_to_graph_sources: HashMap::new(),
            gs_to_ledgers: HashMap::new(),
            stats: VectorWorkerStats::default(),
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
            "Registered vector graph source for maintenance"
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
            "Unregistered vector graph source from maintenance"
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
    pub fn stats(&self) -> &VectorWorkerStats {
        &self.stats
    }
}

#[cfg(feature = "vector")]
impl Default for VectorWorkerState {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle to interact with a running Vector maintenance worker.
///
/// This handle allows registering/unregistering graph sources and stopping the worker.
#[cfg(feature = "vector")]
pub struct VectorWorkerHandle {
    state: Rc<RefCell<VectorWorkerState>>,
    /// Signal to stop the worker (set to true to request stop).
    stop_requested: Rc<RefCell<bool>>,
}

#[cfg(feature = "vector")]
impl VectorWorkerHandle {
    /// Register a graph source for automatic maintenance.
    ///
    /// The worker will sync this graph source whenever any of its source ledgers are updated.
    pub async fn register_graph_source<N: NameService + GraphSourcePublisher>(
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
            .borrow_mut()
            .register_graph_source(graph_source_id, &record.dependencies);
        Ok(())
    }

    /// Register a graph source with explicit dependencies (no nameservice lookup).
    pub fn register_graph_source_with_deps(&self, graph_source_id: &str, dependencies: &[String]) {
        self.state
            .borrow_mut()
            .register_graph_source(graph_source_id, dependencies);
    }

    /// Unregister a graph source from automatic maintenance.
    pub fn unregister_graph_source(&self, graph_source_id: &str) {
        self.state
            .borrow_mut()
            .unregister_graph_source(graph_source_id);
    }

    /// Get current worker statistics.
    pub fn stats(&self) -> VectorWorkerStats {
        self.state.borrow().stats().clone()
    }

    /// Get all registered graph sources.
    pub fn registered_graph_sources(&self) -> Vec<String> {
        self.state.borrow().registered_graph_sources()
    }

    /// Request the worker to stop.
    pub fn stop(&self) {
        *self.stop_requested.borrow_mut() = true;
        info!("Vector maintenance worker stop requested");
    }

    /// Check if stop has been requested.
    pub fn is_stop_requested(&self) -> bool {
        *self.stop_requested.borrow()
    }
}

/// Vector maintenance worker.
///
/// Monitors nameservice events and automatically syncs vector indexes when their
/// source ledgers are updated.
#[cfg(feature = "vector")]
pub struct VectorMaintenanceWorker<'a> {
    fluree: &'a crate::Fluree,
    config: VectorWorkerConfig,
    state: Rc<RefCell<VectorWorkerState>>,
    stop_requested: Rc<RefCell<bool>>,
}

#[cfg(feature = "vector")]
impl<'a> VectorMaintenanceWorker<'a> {
    /// Create a new maintenance worker.
    pub fn new(fluree: &'a crate::Fluree) -> Self {
        Self {
            fluree,
            config: VectorWorkerConfig::default(),
            state: Rc::new(RefCell::new(VectorWorkerState::new())),
            stop_requested: Rc::new(RefCell::new(false)),
        }
    }

    /// Create a new maintenance worker with custom config.
    pub fn with_config(fluree: &'a crate::Fluree, config: VectorWorkerConfig) -> Self {
        Self {
            fluree,
            config,
            state: Rc::new(RefCell::new(VectorWorkerState::new())),
            stop_requested: Rc::new(RefCell::new(false)),
        }
    }

    /// Get a handle to interact with the worker.
    pub fn handle(&self) -> VectorWorkerHandle {
        VectorWorkerHandle {
            state: self.state.clone(),
            stop_requested: self.stop_requested.clone(),
        }
    }

    /// Process a single nameservice event.
    ///
    /// Returns the list of graph source aliases that need syncing.
    pub fn process_event(&self, event: &NameServiceEvent) -> Vec<String> {
        self.state.borrow_mut().record_event();

        match event {
            NameServiceEvent::LedgerCommitPublished {
                ledger_id,
                commit_t,
                ..
            } => {
                let graph_sources = self.state.borrow().graph_sources_for_ledger(ledger_id);
                if !graph_sources.is_empty() {
                    info!(
                        ledger = %ledger_id,
                        commit_t,
                        gs_count = graph_sources.len(),
                        "Ledger commit triggers vector graph source sync"
                    );
                }
                graph_sources
            }
            NameServiceEvent::LedgerIndexPublished {
                ledger_id, index_t, ..
            } => {
                // Index updates don't require graph source sync (commit already triggered it)
                debug!(ledger = %ledger_id, index_t, "Ledger index published (no vector graph source sync needed)");
                vec![]
            }
            NameServiceEvent::GraphSourceConfigPublished {
                graph_source_id,
                source_type,
                dependencies,
                ..
            } => {
                // Only auto-register Vector graph sources
                if self.config.auto_register
                    && *source_type == fluree_db_nameservice::GraphSourceType::Vector
                {
                    self.state
                        .borrow_mut()
                        .register_graph_source(graph_source_id, dependencies);
                    info!(graph_source = %graph_source_id, "Auto-registered vector graph source for maintenance");
                }
                vec![]
            }
            NameServiceEvent::GraphSourceRetracted { graph_source_id } => {
                // Unregister retracted graph source
                self.state
                    .borrow_mut()
                    .unregister_graph_source(graph_source_id);
                info!(graph_source = %graph_source_id, "Unregistered retracted vector graph source");
                vec![]
            }
            _ => vec![], // Other events don't trigger sync
        }
    }

    /// Sync a single graph source (called by the event loop).
    pub async fn sync_graph_source(&self, graph_source_id: &str) -> Result<()> {
        debug!(graph_source = %graph_source_id, "Syncing vector graph source");

        match self.fluree.sync_vector_index(graph_source_id).await {
            Ok(result) => {
                self.state.borrow_mut().record_sync(true);
                info!(
                    graph_source = %graph_source_id,
                    upserted = result.upserted,
                    removed = result.removed,
                    skipped = result.skipped,
                    new_watermark = result.new_watermark,
                    "Vector graph source sync completed"
                );
                Ok(())
            }
            Err(e) => {
                self.state.borrow_mut().record_sync(false);
                error!(graph_source = %graph_source_id, error = %e, "Vector graph source sync failed");
                Err(e)
            }
        }
    }

    /// Run the maintenance loop.
    ///
    /// This subscribes to nameservice events and processes them until stopped.
    /// The worker uses `Rc<RefCell<...>>` internally, so it must be run on a
    /// single-threaded runtime or via `spawn_local`.
    pub async fn run(&self) -> Result<()> {
        info!("Starting vector maintenance worker");

        // Subscribe to all nameservice events (ledger and graph source changes).
        let mut subscription = self
            .fluree
            .event_bus()
            .subscribe(fluree_db_nameservice::SubscriptionScope::All);

        // Debounced batching: we accumulate graph sources to sync and flush them after `debounce_ms`.
        let mut pending: HashSet<String> = HashSet::new();
        let mut next_flush: Option<Instant> = None;

        // In-flight syncs (bounded by config.max_concurrent_syncs).
        #[allow(clippy::type_complexity)]
        let mut in_flight: futures::stream::FuturesUnordered<
            std::pin::Pin<Box<dyn std::future::Future<Output = (String, Result<()>)>>>,
        > = futures::stream::FuturesUnordered::new();

        loop {
            // Check for stop request
            if *self.stop_requested.borrow() {
                info!("Vector maintenance worker stopping");
                break;
            }

            // Flush pending syncs if debounce timer elapsed and we have capacity.
            let now = Instant::now();
            let can_flush = next_flush.map(|t| now >= t).unwrap_or(false);
            if can_flush {
                while in_flight.len() < self.config.max_concurrent_syncs {
                    let Some(graph_source_id) = pending.iter().next().cloned() else {
                        break;
                    };
                    pending.remove(&graph_source_id);

                    // Spawn a non-Send future into our in-flight set (polled on this task).
                    let fut = async move {
                        let res = self.sync_graph_source(&graph_source_id).await;
                        (graph_source_id, res)
                    };
                    in_flight.push(Box::pin(fut));
                }

                // If we've drained pending, clear flush deadline; otherwise keep flushing.
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
                    if let Err(e) = res {
                        warn!(graph_source = %graph_source_id, error = %e, "Failed to sync vector graph source");
                    }
                }

                // Debounce tick / stop-check tick
                () = &mut sleep_fut => {}
            }
        }

        info!("Vector maintenance worker stopped");
        Ok(())
    }
}

#[cfg(all(test, feature = "vector"))]
mod tests {
    use super::*;

    #[test]
    fn test_worker_state_register_graph_source() {
        let mut state = VectorWorkerState::new();

        state.register_graph_source(
            "embeddings:main",
            &["ledger1:main".to_string(), "ledger2:main".to_string()],
        );

        assert_eq!(state.registered_graph_sources(), vec!["embeddings:main"]);
        assert!(state
            .watched_ledgers()
            .contains(&"ledger1:main".to_string()));
        assert!(state
            .watched_ledgers()
            .contains(&"ledger2:main".to_string()));

        // Graph sources for ledger
        assert_eq!(
            state.graph_sources_for_ledger("ledger1:main"),
            vec!["embeddings:main"]
        );
        assert_eq!(
            state.graph_sources_for_ledger("unknown:main"),
            Vec::<String>::new()
        );
    }

    #[test]
    fn test_worker_state_unregister_graph_source() {
        let mut state = VectorWorkerState::new();

        state.register_graph_source("embeddings:main", &["ledger:main".to_string()]);
        assert_eq!(state.registered_graph_sources().len(), 1);

        state.unregister_graph_source("embeddings:main");
        assert_eq!(state.registered_graph_sources().len(), 0);
        assert_eq!(state.watched_ledgers().len(), 0);
    }

    #[test]
    fn test_worker_state_multiple_graph_sources() {
        let mut state = VectorWorkerState::new();

        state.register_graph_source("gs1:main", &["ledger:main".to_string()]);
        state.register_graph_source("gs2:main", &["ledger:main".to_string()]);
        state.register_graph_source("gs3:main", &["other:main".to_string()]);

        // Both gs1 and gs2 depend on ledger:main
        let graph_sources = state.graph_sources_for_ledger("ledger:main");
        assert_eq!(graph_sources.len(), 2);
        assert!(graph_sources.contains(&"gs1:main".to_string()));
        assert!(graph_sources.contains(&"gs2:main".to_string()));

        // Only gs3 depends on other:main
        assert_eq!(
            state.graph_sources_for_ledger("other:main"),
            vec!["gs3:main"]
        );
    }

    #[test]
    fn test_worker_state_stats() {
        let mut state = VectorWorkerState::new();

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
}
