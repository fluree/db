//! Fluree DB HTTP Server
//!
//! A thin HTTP REST API wrapper around `fluree-db-api`, providing endpoints
//! equivalent to the legacy server behavior.
//!
//! # Features
//!
//! - JSON-LD and SPARQL query support
//! - Transaction endpoints (transact, insert, upsert)
//! - History queries
//! - Ledger management (create, drop, info)
//! - Header-based policy injection
//! - CORS support
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_server::{FlureeServer, ServerConfig};
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = ServerConfig::default();
//!     let server = FlureeServer::new(config).await.unwrap();
//!     server.run().await.unwrap();
//! }
//! ```

pub mod config;
pub mod config_file;
pub mod error;
pub mod extract;
#[cfg(feature = "oidc")]
pub mod jwks;
pub mod mcp;
pub mod peer;
#[cfg(feature = "raft")]
pub mod raft;
pub mod registry;
pub mod routes;
pub mod serde;
pub mod state;
pub mod telemetry;
#[cfg(feature = "oidc")]
pub mod token_verify;

pub use config::{ServerConfig, ServerRole};
pub use error::{Result, ServerError};
pub use peer::{ForwardingClient, PeerState, PeerSubscriptionTask};
pub use state::AppState;
pub use telemetry::{init_logging, shutdown_tracer, TelemetryConfig};

use axum::Router;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

/// Private listener config for the Raft inter-node RPC + admin
/// routers. Only populated when the server is constructed with a
/// Raft handle via [`FlureeServer::new_with_raft`].
#[cfg(feature = "raft")]
struct RaftListener {
    /// Routed under `/raft` (inter-node RPC) and `/cluster` (admin).
    private_router: Router,
    /// Address for the VPC-internal listener. Distinct from the
    /// public client-facing listener at `config.listen_addr`.
    listen_addr: std::net::SocketAddr,
}

/// Fluree HTTP Server
pub struct FlureeServer {
    /// Application state
    state: Arc<AppState>,
    /// Configured router
    router: Router,
    /// Optional private Raft listener (consensus + admin).
    #[cfg(feature = "raft")]
    raft_listener: Option<RaftListener>,
    /// Leader-aware watcher driving every leader-only background
    /// task (indexer, commit-queue worker). `Some` when raft mode is
    /// on. Aborted on shutdown so the spawned tasks tear down with
    /// the rest of the server.
    #[cfg(feature = "raft")]
    raft_leader_watcher: Option<tokio::task::JoinHandle<()>>,
    /// Per-node release task that drains the state-machine adapter's
    /// CAS release channel. Runs on every node (not just the leader)
    /// so admin-cleared queue entries and idempotency-evicted
    /// envelopes don't orphan their bodies in the content store.
    /// Aborted on shutdown.
    #[cfg(feature = "raft")]
    raft_release_task: Option<tokio::task::JoinHandle<()>>,
}

impl FlureeServer {
    /// Create a new server with the given configuration.
    ///
    /// Sugar for `FlureeServerBuilder` with no extras. For Raft
    /// mode, use [`FlureeServerBuilder::with_raft`].
    pub async fn new(config: ServerConfig) -> std::result::Result<Self, fluree_db_api::ApiError> {
        FlureeServerBuilder::for_config(config).build().await
    }

    /// Pre-load all non-retracted ledgers into the LRU cache.
    ///
    /// This warms the binary index store cache for every ledger so that the
    /// first query doesn't pay a cold-start penalty. Errors are logged but
    /// do not prevent the server from starting.
    async fn preload_all_ledgers(state: &Arc<AppState>) {
        let start = std::time::Instant::now();

        let records = match state.fluree.nameservice().all_records().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to enumerate ledgers for preloading");
                return;
            }
        };

        let active: Vec<_> = records.into_iter().filter(|r| !r.retracted).collect();
        if active.is_empty() {
            return;
        }

        let total = active.len();
        let mut loaded = 0usize;

        for record in &active {
            let handle = state.fluree.ledger_cached(&record.ledger_id).await;

            match handle {
                Ok(handle) => {
                    loaded += 1;

                    // Warm dict tree leaves into the LeafletCache so the first
                    // query doesn't pay cold-start disk I/O for IRI/string resolution.
                    let snap = handle.snapshot().await;
                    if let Some(store) = &snap.binary_store {
                        match store.preload_dict_leaves() {
                            Ok(leaf_count) => {
                                tracing::debug!(
                                    ledger = %record.ledger_id,
                                    leaf_count,
                                    "Preloaded ledger + dict leaves"
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    ledger = %record.ledger_id,
                                    error = %e,
                                    "Preloaded ledger but dict leaf warming failed"
                                );
                            }
                        }
                    } else {
                        tracing::debug!(ledger = %record.ledger_id, "Preloaded ledger (no binary index)");
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        ledger = %record.ledger_id,
                        error = %e,
                        "Failed to preload ledger"
                    );
                }
            }
        }

        let elapsed = start.elapsed();
        info!(
            loaded,
            total,
            elapsed_ms = elapsed.as_millis() as u64,
            "Ledger preload complete"
        );
    }

    /// Get a reference to the application state
    pub fn state(&self) -> &Arc<AppState> {
        &self.state
    }

    /// Get the router for testing
    pub fn router(&self) -> Router {
        self.router.clone()
    }

    /// Run the server
    pub async fn run(self) -> std::result::Result<(), std::io::Error> {
        let addr = self.state.config.listen_addr;
        let listener = TcpListener::bind(addr).await?;

        // Bind the private Raft listener up front so a port-in-use
        // failure surfaces before we've spawned any background tasks.
        #[cfg(feature = "raft")]
        let raft_listener_bound = match self.raft_listener {
            Some(rl) => {
                let l = TcpListener::bind(rl.listen_addr).await?;
                Some((l, rl.private_router, rl.listen_addr))
            }
            None => None,
        };

        // Start peer subscription/sync task if in peer mode
        let subscription_task = if self.state.config.is_peer_mode() {
            let peer_state = self
                .state
                .peer_state
                .clone()
                .expect("peer_state should exist in peer mode");

            if !self.state.fluree.nameservice_mode().is_read_only() {
                // Shared storage: PeerSyncTask persists refs into local FileNameService
                let events_url = peer::build_peer_events_url(&self.state.config);
                let auth_token = self.state.config.load_peer_events_token().ok().flatten();
                let watch = fluree_db_nameservice_sync::SseRemoteWatch::new(events_url, auth_token);
                let task = peer::PeerSyncTask::new(
                    Arc::clone(&self.state.fluree),
                    peer_state,
                    watch,
                    self.state.config.clone(),
                );
                Some(task.spawn())
            } else {
                // Proxy storage: existing PeerSubscriptionTask (in-memory watermarks only)
                let task = peer::PeerSubscriptionTask::new(
                    self.state.config.clone(),
                    peer_state,
                    Arc::clone(&self.state.fluree),
                );
                Some(task.spawn())
            }
        } else {
            None
        };

        // Start ledger manager maintenance task for idle eviction
        let ledger_maintenance_task = self.state.fluree.spawn_maintenance();

        // Spawn the private Raft listener. Carries the inter-node
        // RPC + cluster admin routers — mount on a VPC-internal
        // interface (no auth on these endpoints by design).
        #[cfg(feature = "raft")]
        let raft_listener_task = raft_listener_bound.map(|(private_listener, router, addr)| {
            info!(addr = %addr, "Raft private listener starting");
            tokio::spawn(async move {
                if let Err(e) = axum::serve(private_listener, router).await {
                    tracing::error!(error = %e, "Raft private listener exited");
                }
            })
        });

        info!(
            addr = %addr,
            storage = %self.state.config.storage_type_str(),
            server_role = ?self.state.config.server_role,
            ledger_caching = ledger_maintenance_task.is_some(),
            mcp_enabled = self.state.config.mcp_enabled,
            "Fluree server starting"
        );

        // Run server
        let result = axum::serve(listener, self.router).await;

        // Cancel maintenance tasks on shutdown
        if let Some(task) = subscription_task {
            task.abort();
        }
        if let Some(task) = ledger_maintenance_task {
            task.abort();
        }
        #[cfg(feature = "raft")]
        if let Some(task) = raft_listener_task {
            task.abort();
        }
        #[cfg(feature = "raft")]
        if let Some(task) = self.raft_leader_watcher {
            // Aborting the watcher's outer task drops its metrics
            // receiver; on its way down it also aborts every
            // currently-running leader task — see
            // `raft::spawn_leader_watcher` for the teardown path.
            task.abort();
        }
        #[cfg(feature = "raft")]
        if let Some(task) = self.raft_release_task {
            task.abort();
        }

        result
    }

    /// Start the registry maintenance task for tracking ledger watermarks.
    ///
    /// This spawns a background task that:
    /// - Listens to nameservice events and updates registry watermarks
    /// - Periodically sweeps idle entries based on the registry's TTL
    ///
    /// Returns a JoinHandle that can be used to await or abort the task.
    /// The task will automatically stop when the nameservice broadcast channel closes.
    pub async fn start_registry_maintenance(
        &self,
        sweep_interval: std::time::Duration,
    ) -> std::result::Result<tokio::task::JoinHandle<()>, fluree_db_api::ApiError> {
        use fluree_db_nameservice::SubscriptionScope;

        let subscription = self
            .state
            .fluree
            .event_bus()
            .subscribe(SubscriptionScope::All);

        let handle = registry::LedgerRegistry::spawn_maintenance_task(
            self.state.registry.clone(),
            subscription.receiver,
            sweep_interval,
        );

        info!("Registry maintenance task started");
        Ok(handle)
    }
}

/// Builder for FlureeServer with fluent API
pub struct FlureeServerBuilder {
    config: ServerConfig,
    /// Optional Raft integration and the private listener address
    /// for inter-node RPC + cluster admin. Set via
    /// [`Self::with_raft`].
    #[cfg(feature = "raft")]
    raft: Option<(Arc<crate::raft::RaftIntegration>, std::net::SocketAddr)>,
}

impl FlureeServerBuilder {
    /// Create a new builder with default config (memory storage)
    pub fn new() -> Self {
        Self::for_config(ServerConfig::default())
    }

    /// Create a builder wrapping an already-built [`ServerConfig`].
    /// Used by [`FlureeServer::new`] as the no-extras shortcut path.
    pub fn for_config(config: ServerConfig) -> Self {
        Self {
            config,
            #[cfg(feature = "raft")]
            raft: None,
        }
    }

    /// Create a builder configured for memory storage
    pub fn memory() -> Self {
        Self::new()
    }

    /// Create a builder configured for file storage
    #[cfg(feature = "native")]
    pub fn file(path: impl Into<std::path::PathBuf>) -> Self {
        let mut builder = Self::new();
        builder.config.storage_path = Some(path.into());
        builder
    }

    /// Set the listen address
    pub fn listen_addr(mut self, addr: impl Into<std::net::SocketAddr>) -> Self {
        self.config.listen_addr = addr.into();
        self
    }

    /// Enable or disable CORS
    pub fn cors_enabled(mut self, enabled: bool) -> Self {
        self.config.cors_enabled = enabled;
        self
    }

    /// Enable or disable background indexing
    pub fn indexing_enabled(mut self, enabled: bool) -> Self {
        self.config.indexing_enabled = enabled;
        self
    }

    /// Set global cache budget in MB
    pub fn cache_max_mb(mut self, max_mb: usize) -> Self {
        self.config.cache_max_mb = Some(max_mb);
        self
    }

    /// Attach a [`RaftIntegration`](crate::raft::RaftIntegration) and
    /// the private listener address. The resulting server mounts the
    /// leader-forward middleware over write routes and serves the
    /// inter-node RPC + cluster admin routers on `listen_addr`.
    /// `listen_addr` should be a VPC-internal interface — those
    /// routers carry no auth of their own.
    #[cfg(feature = "raft")]
    pub fn with_raft(
        mut self,
        integration: Arc<crate::raft::RaftIntegration>,
        listen_addr: std::net::SocketAddr,
    ) -> Self {
        self.raft = Some((integration, listen_addr));
        self
    }

    /// Build the server.
    ///
    /// Single construction path: pick the `Fluree` constructor
    /// (default vs raft-replicated nameservice) based on whether
    /// raft is attached, then build `AppState` around it, then warm
    /// JWKS, preload ledgers, and build the router.
    pub async fn build(self) -> std::result::Result<FlureeServer, fluree_db_api::ApiError> {
        let telemetry_config = TelemetryConfig::with_server_config(&self.config);

        // Construct `RaftNameService` once and reuse it for both the
        // Fluree read path (downcast to `NameServiceLookup`) and the
        // leader-aware indexer launcher (upcast to
        // `IndexingNameService`). Keeping a single Arc keeps reads
        // and the index publisher coherent — both observe the same
        // shared state and propose through the same Raft handle.
        #[cfg(feature = "raft")]
        let raft_nameservice = self.raft.as_ref().map(|(integration, _)| {
            std::sync::Arc::new(
                fluree_db_consensus::raft::nameservice::RaftNameService::new(
                    integration.shared_state.clone(),
                    std::sync::Arc::clone(&integration.raft),
                )
                .with_staged_receipts(std::sync::Arc::clone(&integration.staged_receipts)),
            )
        });

        // Build `Fluree` with the right nameservice for the
        // deployment mode. Raft mode wires `RaftNameService` so
        // every node's reads observe replicated state; default mode
        // uses whatever the storage backend implies.
        #[cfg(feature = "raft")]
        let (fluree, cache_stats_handle) =
            if let Some(raft_ns) = raft_nameservice.as_ref() {
                // RaftNameService satisfies both LifecycleNameService
                // (NameServiceLookup + LedgerLifecycle + BranchLifecycle)
                // and IndexingNameService (NameServiceLookup +
                // IndexPublisher), so both Lifecycle fields hold a
                // clone of the same Arc. Method-form `.clone()`
                // returns `Arc<RaftNameService>` which each let
                // binding coerces to its target trait object.
                let lifecycle: std::sync::Arc<dyn fluree_db_api::LifecycleNameService> =
                    raft_ns.clone();
                let indexing: std::sync::Arc<dyn fluree_db_nameservice::IndexingNameService> =
                    raft_ns.clone();
                let ns_mode = fluree_db_api::NameServiceMode::Lifecycle {
                    lifecycle,
                    indexing,
                };
                state::build_fluree_with_nameservice(&self.config, ns_mode).await?
            } else {
                state::build_default_fluree(&self.config).await?
            };
        #[cfg(not(feature = "raft"))]
        let (fluree, cache_stats_handle) = state::build_default_fluree(&self.config).await?;

        #[allow(unused_mut)]
        let mut state_inner =
            AppState::with_fluree(self.config, telemetry_config, fluree, cache_stats_handle)
                .await?;

        #[cfg(feature = "raft")]
        let raft_listener = self.raft.as_ref().map(|(integration, listen_addr)| {
            // Consensus-side committer stack: `QueuedTransactor`
            // routes all five `Committer` methods through
            // `EnqueueCommand` plus the per-process `WaiterMap` and
            // `StagedReceiptMap`; `CachingCommitter` sits on top so
            // keyed retries dedupe before the queue propose.
            let queued = fluree_db_consensus::raft::queued_transactor::QueuedTransactor::new(
                Arc::clone(&integration.raft),
                Arc::clone(&state_inner.fluree),
                Arc::clone(&integration.waiter_map),
                integration.shared_state.clone(),
            );
            state_inner.committer =
                Arc::new(fluree_db_consensus::CachingCommitter::wrapping(queued));
            state_inner.raft = Some(Arc::clone(integration));
            RaftListener {
                private_router: integration.private_router(),
                listen_addr: *listen_addr,
            }
        });

        // Per-node CAS release task. The state-machine adapter pushes
        // `(ledger_id, request_cid)` pairs through the integration's
        // release channel whenever an apply surfaces evictable
        // envelopes (idempotency eviction, admin clears). Followers
        // see the same applies as the leader, so running this task on
        // every node keeps the content store consistent across the
        // cluster.
        #[cfg(feature = "raft")]
        let raft_release_task = match self.raft.as_ref() {
            Some((integration, _)) => {
                let rx = integration.take_release_receiver().await;
                rx.map(|mut rx| {
                    let fluree = Arc::clone(&state_inner.fluree);
                    tokio::spawn(async move {
                        while let Some((ledger_id, cid)) = rx.recv().await {
                            if let Err(err) =
                                fluree.content_store(&ledger_id).release(&cid).await
                            {
                                tracing::warn!(
                                    %ledger_id,
                                    %cid,
                                    error = %err,
                                    "failed to release envelope from content store"
                                );
                            }
                        }
                    })
                })
            }
            None => None,
        };

        // Wire the leader-aware launcher. Bundles both the background
        // indexer and the commit-queue worker so they share one
        // metrics watcher and one spawn/abort lifecycle. See
        // `raft::spawn_leader_watcher` for the contract.
        #[cfg(feature = "raft")]
        let raft_leader_watcher =
            self.raft
                .as_ref()
                .map(|(integration, _)| {
                    let raft_ns = std::sync::Arc::clone(raft_nameservice.as_ref().expect(
                        "raft_nameservice present whenever self.raft is Some",
                    ));
                    let backend = state_inner.fluree.backend().clone();
                    let indexer_config = fluree_db_indexer::IndexerConfig::default();
                    let event_bus = Arc::clone(&integration.event_bus);
                    // Same `RaftNameService` doubles as the
                    // `CommitPublisher` so the worker's head advance
                    // goes through `publish_commit` → `ApplyHead`
                    // under the queue front it sampled.
                    let publisher: std::sync::Arc<
                        dyn fluree_db_nameservice::CommitPublisher,
                    > = std::sync::Arc::clone(&raft_ns) as _;
                    let commit_worker = fluree_db_consensus::raft::commit_worker::CommitWorker::new(
                        Arc::clone(&integration.raft),
                        publisher,
                        Arc::clone(&state_inner.fluree),
                        state_inner
                            .index_config
                            .clone()
                            .expect("index_config set by AppState::new"),
                        integration.shared_state.clone(),
                        Arc::clone(&integration.staged_receipts),
                    );
                    let eviction_scheduler =
                        fluree_db_consensus::raft::eviction_scheduler::EvictionScheduler::new(
                            Arc::clone(&integration.raft),
                        );
                    let spawn_leader_tasks = move || {
                        let nameservice: std::sync::Arc<
                            dyn fluree_db_nameservice::IndexingNameService,
                        > = raft_ns.clone();
                        let (worker, _handle) = fluree_db_indexer::BackgroundIndexerWorker::new(
                            backend.clone(),
                            nameservice,
                            indexer_config.clone(),
                        );
                        let worker = worker.with_event_bus(Arc::clone(&event_bus));
                        vec![
                            tokio::spawn(worker.run()),
                            tokio::spawn(commit_worker.clone().run()),
                            tokio::spawn(eviction_scheduler.clone().run()),
                        ]
                    };
                    crate::raft::spawn_leader_watcher(
                        Arc::clone(&integration.raft),
                        integration.self_id,
                        spawn_leader_tasks,
                    )
                });

        // The raft tuple is no longer needed beyond this point —
        // both raft_listener and raft_leader_watcher captured what
        // they need. Drop the rest.
        #[cfg(feature = "raft")]
        drop(self.raft);
        #[cfg(feature = "raft")]
        drop(raft_nameservice);

        let state = Arc::new(state_inner);

        // Warm JWKS cache (async — fetch keys from configured endpoints).
        #[cfg(feature = "oidc")]
        if let Some(jwks_cache) = &state.jwks_cache {
            let warmed = jwks_cache.warm().await;
            let total = jwks_cache.configured_issuer_count();
            if warmed == 0 && total > 0 {
                if state.config.data_auth_mode == crate::config::DataAuthMode::Required {
                    tracing::error!(
                        total_issuers = total,
                        "No JWKS endpoints reachable at startup — \
                         OIDC token verification will FAIL until endpoints become available"
                    );
                } else {
                    tracing::warn!(
                        total_issuers = total,
                        "No JWKS endpoints reachable at startup — \
                         OIDC tokens will be rejected until endpoints become available"
                    );
                }
            }
        }

        // Pre-load all ledgers into the LRU cache so the first query
        // against each ledger doesn't pay the cold-start penalty
        // (loading the binary index root from CAS, deserializing
        // dicts, etc.).
        FlureeServer::preload_all_ledgers(&state).await;

        let router = routes::build_router(state.clone());

        Ok(FlureeServer {
            state,
            router,
            #[cfg(feature = "raft")]
            raft_listener,
            #[cfg(feature = "raft")]
            raft_leader_watcher,
            #[cfg(feature = "raft")]
            raft_release_task,
        })
    }
}

impl Default for FlureeServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}
