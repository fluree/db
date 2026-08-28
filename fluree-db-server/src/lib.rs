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

#[cfg(feature = "bolt")]
pub mod bolt;
pub mod config;
pub mod config_file;
pub mod error;
pub mod extract;
pub mod import_jobs;
#[cfg(feature = "oidc")]
pub mod jwks;
pub mod mcp;
pub mod peer;
pub(crate) mod query_control;
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
use fluree_db_api::{Bm25MaintenanceWorker, Bm25WorkerHandle, Fluree};
use fluree_db_nameservice::GraphSourceRecord;
use std::sync::Arc;

/// Whether this process runs a Raft node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Consensus {
    /// Raft is configured, so the leader watcher owns leader-only tasks.
    Raft,
    /// No consensus layer; this process owns its tasks outright.
    Standalone,
}

/// Which part of the deployment runs the BM25 maintenance worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Bm25WorkerOwner {
    /// Nothing: `--bm25-auto-sync` is off.
    Disabled,
    /// Nothing on this server. A peer forwards its writes, so the commit
    /// events that drive a sync are published on the transaction server's
    /// event bus rather than this one.
    PeerForwardsWrites,
    /// The Raft leader watcher, which spawns the worker on election. A sync
    /// proposes `PublishGraphSourceIndex`, so it must originate on the leader.
    RaftLeader,
    /// This server, for the life of `serve()`.
    ThisServer,
}

/// Decide who runs the BM25 maintenance worker for this deployment.
fn bm25_worker_owner(config: &ServerConfig, consensus: Consensus) -> Bm25WorkerOwner {
    if !config.bm25_auto_sync {
        Bm25WorkerOwner::Disabled
    } else if config.server_role == ServerRole::Peer {
        Bm25WorkerOwner::PeerForwardsWrites
    } else if consensus == Consensus::Raft {
        Bm25WorkerOwner::RaftLeader
    } else {
        Bm25WorkerOwner::ThisServer
    }
}

/// Select the graph sources the startup registration pass hands to the worker.
///
/// Retracted indexes are left out: syncing one is refused, so registering it
/// would only log a failed sync on every commit to its source ledger.
pub fn indexes_to_auto_sync(records: &[GraphSourceRecord]) -> Vec<&GraphSourceRecord> {
    records
        .iter()
        .filter(|gs| gs.is_bm25() && !gs.retracted)
        .collect()
}

/// Build a BM25 maintenance worker seeded with the indexes that already exist.
///
/// `auto_register` only picks up indexes created while the worker is running,
/// so without this pass an index created before startup would never sync.
/// Failing to enumerate is not fatal: those indexes stay unregistered until
/// their next config publish, and ones created from here on still register.
async fn build_bm25_worker(fluree: Arc<Fluree>) -> (Bm25MaintenanceWorker, Bm25WorkerHandle) {
    let worker = Bm25MaintenanceWorker::new(Arc::clone(&fluree));
    let handle = worker.handle();

    match fluree.nameservice().all_graph_source_records().await {
        Ok(records) => {
            let indexes = indexes_to_auto_sync(&records);
            for gs in &indexes {
                handle.register_graph_source_with_deps(&gs.graph_source_id, &gs.dependencies);
            }
            info!(registered = indexes.len(), "BM25 auto-sync starting");
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to enumerate BM25 indexes for auto-sync");
        }
    }

    (worker, handle)
}

/// Drive a BM25 maintenance worker to completion, logging an unexpected exit.
///
/// Used by the Raft leader watcher, whose task-spawning closure is synchronous
/// and so cannot do the registration pass itself.
#[cfg(feature = "raft")]
async fn run_bm25_worker(fluree: Arc<Fluree>) {
    let (worker, _handle) = build_bm25_worker(fluree).await;
    if let Err(e) = worker.run().await {
        tracing::error!(error = %e, "BM25 maintenance worker exited");
    }
}
use tokio::net::TcpListener;
use tracing::info;

/// Private listener config for the Raft inter-node RPC + admin
/// routers. Only populated when the server is constructed with a
/// Raft handle via [`FlureeServer::new_with_raft`].
#[cfg(feature = "raft")]
struct RaftListener {
    /// Routed under `/raft` (inter-node RPC, peer-trusted) and
    /// `/cluster` (admin, gated by `routes::admin_auth::require_admin_token`
    /// against the active `admin_auth` config; pass-through when the
    /// mode is `None`).
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
    /// The committer, worker supervisor, leader watcher, and release
    /// task, owned together so they shut down in the order that keeps
    /// the content store consistent. `Some` when raft mode is on.
    #[cfg(feature = "raft")]
    raft_node: Option<fluree_db_consensus::raft::embedded::EmbeddedRaftNode>,
}

/// How long in-flight requests get to complete after a shutdown
/// signal before remaining connections are closed. Long-lived
/// response streams (the SSE events endpoint) never complete on
/// their own — without this bound they would hold graceful shutdown
/// open past the process supervisor's kill deadline, and the
/// post-serve teardown below (worker drain, CAS release drain)
/// would never run at all.
const SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(10);

/// Resolves when the process receives SIGTERM (unix service
/// managers, Kubernetes) or SIGINT (ctrl-c).
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("SIGINT handler installs on any supported platform");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler installs on unix")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}

impl FlureeServer {
    /// Create a new server with the given configuration.
    ///
    /// Sugar for `FlureeServerBuilder` with no extras. For Raft
    /// mode, use [`FlureeServerBuilder::with_raft`].
    pub async fn new(config: ServerConfig) -> std::result::Result<Self, fluree_db_api::ApiError> {
        FlureeServerBuilder::for_config(config).build().await
    }

    /// Pre-load non-retracted ledgers into the LRU cache and warm their
    /// forward-dictionary pages into the OS page cache.
    ///
    /// Runs in the background (spawned from [`run`](Self::run) after the
    /// listener binds) so it never delays the server accepting requests. Each
    /// ledger is structurally loaded (index root + dict readers + arenas), then
    /// its forward-dict pack pages are touched into the page cache so the first
    /// queries don't pay cold page-fault I/O resolving IRIs/strings.
    ///
    /// Forward-dict warming is capped at [`warm_budget_bytes`] (~2/3 of system
    /// RAM): beyond that, touching more pages would evict pages we just warmed,
    /// so the warming (not the structural load) stops and remaining ledgers warm
    /// lazily on first query. Errors are logged, never fatal.
    ///
    /// [`warm_budget_bytes`]: fluree_db_api::server_defaults::warm_budget_bytes
    async fn preload_all_ledgers(state: Arc<AppState>) {
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
        let warm_budget = fluree_db_api::server_defaults::warm_budget_bytes();
        let mut loaded = 0usize;
        let mut warmed_ledgers = 0usize;
        let mut warmed_bytes: u64 = 0;

        for record in &active {
            match state.fluree.ledger_cached(&record.ledger_id).await {
                Ok(handle) => {
                    loaded += 1;

                    // Warm forward-dict pages until the budget is reached. The
                    // structural load above still runs for every ledger; only
                    // the (dominant, file-touching) page warming is capped, so
                    // we never evict pages we just warmed.
                    if warmed_bytes >= warm_budget {
                        continue;
                    }
                    // Take just the binary store; the rest of the snapshot is
                    // dropped here so no view is held across the blocking warm.
                    let Some(store) = handle.snapshot().await.binary_store else {
                        tracing::debug!(ledger = %record.ledger_id, "Preloaded ledger (no binary index)");
                        continue;
                    };
                    let remaining = warm_budget - warmed_bytes;
                    // Page-touching blocks (faults) — keep it off the async workers.
                    let n =
                        tokio::task::spawn_blocking(move || store.prewarm_forward_dicts(remaining))
                            .await
                            .unwrap_or(0);
                    if n > 0 {
                        warmed_bytes += n;
                        warmed_ledgers += 1;
                    }
                    tracing::debug!(
                        ledger = %record.ledger_id,
                        warmed_bytes = n,
                        "Preloaded ledger + warmed forward dicts"
                    );
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
        let budget_reached = warmed_bytes >= warm_budget && loaded > warmed_ledgers;
        info!(
            loaded,
            total,
            warmed_ledgers,
            warmed_mb = warmed_bytes / (1024 * 1024),
            warm_budget_mb = warm_budget / (1024 * 1024),
            budget_reached,
            elapsed_ms = elapsed.as_millis() as u64,
            "Background ledger preload + forward-dict warming complete"
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

        // BM25 auto-sync.
        #[cfg(feature = "raft")]
        let consensus = if self.state.raft.is_some() {
            Consensus::Raft
        } else {
            Consensus::Standalone
        };
        #[cfg(not(feature = "raft"))]
        let consensus = Consensus::Standalone;

        let bm25_auto_sync = match bm25_worker_owner(&self.state.config, consensus) {
            Bm25WorkerOwner::Disabled | Bm25WorkerOwner::RaftLeader => None,
            Bm25WorkerOwner::PeerForwardsWrites => {
                info!("BM25 auto-sync requested but does not run in peer mode");
                None
            }
            Bm25WorkerOwner::ThisServer => {
                let (worker, handle) = build_bm25_worker(Arc::clone(&self.state.fluree)).await;
                let task = tokio::spawn(async move {
                    if let Err(e) = worker.run().await {
                        tracing::error!(error = %e, "BM25 maintenance worker exited");
                    }
                });
                Some((handle, task))
            }
        };

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

        // Bolt protocol listener (Neo4j drivers). Auth is enforced
        // per-session against `data_auth_mode`, same as the HTTP data plane.
        #[cfg(feature = "bolt")]
        let bolt_task = match self.state.config.bolt_listen_addr {
            Some(bolt_addr) => Some(
                bolt::spawn_listener(Arc::clone(&self.state), bolt_addr)
                    .await?
                    .1,
            ),
            None => None,
        };
        #[cfg(not(feature = "bolt"))]
        if self.state.config.bolt_listen_addr.is_some() {
            tracing::warn!(
                "bolt_listen_addr is set but this binary was built without the `bolt` \
                 feature; the Bolt listener will not start"
            );
        }

        // Warm ledger caches + forward-dict pages in the BACKGROUND, after the
        // listener is bound, so the server accepts requests immediately rather
        // than blocking startup until every (potentially large) ledger loads.
        // Safe to race with on-demand request loads: `get_or_load` is
        // single-flight (concurrent loads of the same ledger coalesce), and the
        // leaflet cache is concurrency-safe. Aborted on shutdown.
        let warm_task = tokio::spawn(Self::preload_all_ledgers(Arc::clone(&self.state)));

        info!(
            addr = %addr,
            storage = %self.state.config.storage_type_str(),
            server_role = ?self.state.config.server_role,
            ledger_caching = ledger_maintenance_task.is_some(),
            mcp_enabled = self.state.config.mcp_enabled,
            "Fluree server starting"
        );

        // Run the server until it errors or a shutdown signal
        // arrives. On signal, stop accepting and give in-flight
        // requests `SHUTDOWN_GRACE` to finish; then fall through to
        // the teardown below regardless, closing whatever remains
        // (long-lived SSE streams never finish on their own). The
        // teardown is what drains the raft workers and the CAS
        // release channel — reaching it on SIGTERM is the entire
        // point of handling the signal.
        let shutdown = tokio_util::sync::CancellationToken::new();
        {
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                shutdown_signal().await;
                info!("shutdown signal received; draining");
                shutdown.cancel();
            });
        }
        let graceful = shutdown.clone();
        let serve = axum::serve(listener, self.router).with_graceful_shutdown(async move {
            graceful.cancelled().await;
        });
        let result = tokio::select! {
            result = serve => result,
            () = async {
                shutdown.cancelled().await;
                tokio::time::sleep(SHUTDOWN_GRACE).await;
            } => {
                tracing::warn!(
                    grace_secs = SHUTDOWN_GRACE.as_secs(),
                    "drain window elapsed; closing remaining connections"
                );
                Ok(())
            }
        };

        // Cancel background tasks on shutdown
        warm_task.abort();
        #[cfg(feature = "bolt")]
        if let Some(task) = bolt_task {
            task.abort();
        }
        if let Some(task) = subscription_task {
            task.abort();
        }
        if let Some(task) = ledger_maintenance_task {
            task.abort();
        }
        // Ask the BM25 worker to stop rather than aborting it, then await the
        // task: `run()` drains the syncs already in flight before returning, so
        // a publish in progress completes instead of being cut mid-write. The
        // `stop()` alone only sets a flag — without the await, the runtime goes
        // away underneath the sync and cancels it at its next await point.
        //
        // Bounded, because a sync re-runs the whole indexing query over the
        // source ledger; a large corpus must not hold teardown open.
        if let Some((handle, task)) = bm25_auto_sync {
            handle.stop();
            if tokio::time::timeout(SHUTDOWN_GRACE, task).await.is_err() {
                tracing::warn!(
                    grace_secs = SHUTDOWN_GRACE.as_secs(),
                    "BM25 worker did not finish its in-flight syncs; abandoning them"
                );
            }
        }
        #[cfg(feature = "raft")]
        if let Some(task) = raft_listener_task {
            task.abort();
        }
        #[cfg(feature = "raft")]
        if let Some(node) = self.raft_node {
            // Workers, then leader tasks, then the raft core, then the
            // release drain — the node owns that ordering and the
            // reasons for it.
            node.shutdown().await;
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
    /// Threshold tuning for the leader-only liveness monitor. Defaults
    /// to [`LivenessConfig::default`]; tests override with sub-second
    /// thresholds to keep runtimes short.
    #[cfg(feature = "raft")]
    liveness_config: fluree_db_consensus::raft::liveness_monitor::LivenessConfig,
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
            #[cfg(feature = "raft")]
            liveness_config: fluree_db_consensus::raft::liveness_monitor::LivenessConfig::default(),
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

    /// Set the global on-disk cache budget in MB (Fluree object storage + Iceberg)
    pub fn disk_cache_max_mb(mut self, max_mb: usize) -> Self {
        self.config.disk_cache_max_mb = Some(max_mb);
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

    /// Override the leader-only liveness monitor's threshold tuning.
    /// Defaults are sane for production; tests use this hook to
    /// shrink the unreachable / live windows so the demotion path
    /// fires within a couple of seconds.
    #[cfg(feature = "raft")]
    pub fn with_liveness_config(
        mut self,
        config: fluree_db_consensus::raft::liveness_monitor::LivenessConfig,
    ) -> Self {
        self.liveness_config = config;
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
        // `RaftIntegration` owns the per-node `RaftNameService` (built
        // in its constructor with the same shared state, staged
        // receipts, and HTTP client that drive the rest of the
        // integration). We borrow the same handle here so reads,
        // publishes, and the inbound `apply_staged_commit` route all
        // see one consistent picture.
        #[cfg(feature = "raft")]
        let raft_nameservice = self
            .raft
            .as_ref()
            .map(|(integration, _)| integration.nameservice());

        // Build `Fluree` with the right nameservice for the
        // deployment mode. Raft mode wires `RaftNameService` so
        // every node's reads observe replicated state; default mode
        // uses whatever the storage backend implies.
        //
        // Raft-mode also threads the integration's
        // `LedgerEventBus` into Fluree. Without this, the
        // state-machine adapter emits `NameServiceEvent`s on the
        // integration's private bus while the events endpoint and
        // Fluree's own cache reconciler subscribe on
        // `Fluree::event_bus()` — a different bus instance.
        // Runtime raft commits then never surface to SSE
        // subscribers (peers, external tools), because nothing
        // bridges the two. Passing the same `Arc` here makes both
        // sides observe the same broadcast channel.
        #[cfg(feature = "raft")]
        let raft_event_bus = self
            .raft
            .as_ref()
            .map(|(integration, _)| std::sync::Arc::clone(&integration.event_bus));
        #[cfg(feature = "raft")]
        let (fluree, cache_stats_handle) = if let Some(raft_ns) = raft_nameservice.as_ref() {
            // RaftNameService satisfies the full
            // `NameServicePublisher` surface (refs, admin reindex,
            // status / config push, graph-source publish / index /
            // retract), so it slots directly into ReadWrite.
            let publisher: std::sync::Arc<dyn fluree_db_nameservice::NameServicePublisher> =
                raft_ns.clone();
            let ns_mode = fluree_db_api::NameServiceMode::ReadWrite(publisher);
            state::build_fluree_with_nameservice(&self.config, ns_mode, raft_event_bus).await?
        } else {
            state::build_default_fluree(&self.config, raft_event_bus).await?
        };
        #[cfg(not(feature = "raft"))]
        let (fluree, cache_stats_handle) = state::build_default_fluree(&self.config, None).await?;

        // Only the raft path below mutates `state_inner` (swapping in
        // the queued committer + adapter wiring); a non-raft build
        // leaves it untouched, so bind it `mut` only under `raft`.
        #[cfg(feature = "raft")]
        let mut state_inner =
            AppState::with_fluree(self.config, telemetry_config, fluree, cache_stats_handle)
                .await?;
        #[cfg(not(feature = "raft"))]
        let state_inner =
            AppState::with_fluree(self.config, telemetry_config, fluree, cache_stats_handle)
                .await?;

        // Everything node-scoped that isn't the HTTP listener —
        // committer, worker supervisor, leader watcher, release task —
        // is assembled by `EmbeddedRaftNode`, the same entry point an
        // embedding process uses. The server contributes only what
        // `fluree-db-consensus` cannot: the background indexer (a
        // dependency-direction constraint) and BM25 auto-sync.
        #[cfg(feature = "raft")]
        let raft_node = match self.raft.as_ref() {
            Some((integration, _)) => {
                let raft_ns = std::sync::Arc::clone(
                    raft_nameservice
                        .as_ref()
                        .expect("raft_nameservice present whenever self.raft is Some"),
                );
                let backend = state_inner.fluree.backend().clone();
                let bm25_auto_sync = state_inner.config.bm25_auto_sync;
                let bm25_fluree = Arc::clone(&state_inner.fluree);
                let indexer_config = fluree_db_indexer::IndexerConfig::default();
                let event_bus = Arc::clone(&integration.event_bus);
                let leader_tasks = move || {
                    let nameservice: std::sync::Arc<
                        dyn fluree_db_nameservice::IndexingNameService,
                    > = raft_ns.clone();
                    let (worker, handle) = fluree_db_indexer::BackgroundIndexerWorker::new(
                        backend.clone(),
                        nameservice,
                        indexer_config.clone(),
                    );
                    let worker = worker.with_event_bus(Arc::clone(&event_bus));
                    // The handle owns the worker's ShutdownTrigger:
                    // dropping it fires the shutdown oneshot and `run()`
                    // exits on its FIRST select — silently, before its
                    // first log line — leaving a raft cluster with no
                    // indexer at all and every read walking the commit
                    // chain unindexed. Move it into the worker's task so
                    // they live and die together; the leader watcher's
                    // abort on leadership loss releases both.
                    let mut tasks = vec![tokio::spawn(async move {
                        let _keepalive = handle;
                        worker.run().await;
                    })];
                    // BM25 auto-sync is leader-only for the same reason
                    // the indexer is: it publishes through the
                    // nameservice, which under Raft proposes to the
                    // state machine. Registration happens inside the
                    // task because this closure re-runs on every
                    // leadership acquisition, by which point the set of
                    // indexes may have changed. Losing leadership
                    // abort-and-awaits it — an ex-leader's publish no
                    // longer carries, so finishing would only delay the
                    // handover.
                    if bm25_auto_sync {
                        tasks.push(tokio::spawn(run_bm25_worker(Arc::clone(&bm25_fluree))));
                    }
                    tasks
                };
                let config = fluree_db_consensus::raft::embedded::EmbeddedRaftConfig {
                    index_config: state_inner
                        .index_config
                        .clone()
                        .expect("index_config set by AppState::new"),
                    liveness: self.liveness_config.clone(),
                    extra_leader_tasks: Some(Box::new(leader_tasks)),
                };
                let node = fluree_db_consensus::raft::embedded::EmbeddedRaftNode::attach(
                    Arc::clone(integration),
                    Arc::clone(&state_inner.fluree),
                    config,
                )
                .await;
                state_inner.committer = Arc::clone(&node.committer);
                state_inner.raft = Some(Arc::clone(integration));
                Some(node)
            }
            None => None,
        };
        #[cfg(feature = "raft")]
        let raft_listener_parts = self
            .raft
            .as_ref()
            .map(|(integration, listen_addr)| (Arc::clone(integration), *listen_addr));

        // The raft tuple is no longer needed beyond this point.
        #[cfg(feature = "raft")]
        drop(self.raft);
        #[cfg(feature = "raft")]
        drop(raft_nameservice);

        let state = Arc::new(state_inner);

        // Assemble the private-listener router now that `state` is an
        // `Arc<AppState>` — `require_admin_token` needs that shape. The
        // `/cluster` admin subtree is gated against the configured
        // `admin_auth` mode (pass-through when `None`); `/raft` peer
        // RPC stays unauthenticated and relies on network trust.
        #[cfg(feature = "raft")]
        let raft_listener = raft_listener_parts.map(|(integration, listen_addr)| {
            let cluster_admin =
                integration
                    .cluster_admin_router()
                    .layer(axum::middleware::from_fn_with_state(
                        Arc::clone(&state),
                        crate::routes::admin_auth::require_admin_token,
                    ));
            // `raft_rpc_router` includes the openraft RPCs plus the
            // cross-node `apply_staged_commit` endpoint — intra-cluster
            // trusted, no auth layer.
            let private_router = Router::new()
                .nest("/raft", integration.raft_rpc_router())
                .nest("/cluster", cluster_admin);
            RaftListener {
                private_router,
                listen_addr,
            }
        });

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

        // NOTE: ledger preloading + forward-dict warming is deliberately NOT
        // done here. It runs as a background task spawned in `run()` AFTER the
        // listener binds, so the server accepts requests immediately instead of
        // blocking startup until every (potentially large) ledger is loaded.
        // Preload is a pure latency optimization — a ledger not yet warmed is
        // still served correctly via an on-demand cold load on first access.

        let router = routes::build_router(state.clone());

        Ok(FlureeServer {
            state,
            router,
            #[cfg(feature = "raft")]
            raft_listener,
            #[cfg(feature = "raft")]
            raft_node,
        })
    }
}

impl Default for FlureeServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_nameservice::GraphSourceType;

    fn auto_sync_config(server_role: ServerRole) -> ServerConfig {
        ServerConfig {
            bm25_auto_sync: true,
            server_role,
            ..ServerConfig::default()
        }
    }

    fn record(name: &str, source_type: GraphSourceType) -> GraphSourceRecord {
        GraphSourceRecord::new(
            name,
            "main",
            source_type,
            "{}",
            vec!["docs:main".to_string()],
        )
    }

    fn retracted(name: &str, source_type: GraphSourceType) -> GraphSourceRecord {
        GraphSourceRecord {
            retracted: true,
            ..record(name, source_type)
        }
    }

    #[test]
    fn auto_sync_is_off_unless_the_flag_is_set() {
        let config = ServerConfig::default();

        assert!(!config.bm25_auto_sync, "the flag must default off");
        assert_eq!(
            bm25_worker_owner(&config, Consensus::Standalone),
            Bm25WorkerOwner::Disabled
        );
    }

    #[test]
    fn the_flag_alone_runs_the_worker_on_this_server() {
        assert_eq!(
            bm25_worker_owner(
                &auto_sync_config(ServerRole::Transaction),
                Consensus::Standalone
            ),
            Bm25WorkerOwner::ThisServer
        );
    }

    #[test]
    fn a_peer_does_not_run_the_worker() {
        assert_eq!(
            bm25_worker_owner(&auto_sync_config(ServerRole::Peer), Consensus::Standalone),
            Bm25WorkerOwner::PeerForwardsWrites
        );
    }

    #[test]
    fn raft_hands_the_worker_to_the_leader_watcher() {
        assert_eq!(
            bm25_worker_owner(&auto_sync_config(ServerRole::Transaction), Consensus::Raft),
            Bm25WorkerOwner::RaftLeader
        );
    }

    /// Peer mode wins over Raft: a peer has no commit events to act on at all,
    /// so there is nothing for a leader watcher to own either.
    #[test]
    fn a_raft_peer_does_not_run_the_worker() {
        assert_eq!(
            bm25_worker_owner(&auto_sync_config(ServerRole::Peer), Consensus::Raft),
            Bm25WorkerOwner::PeerForwardsWrites
        );
    }

    #[test]
    fn registration_selects_live_bm25_indexes() {
        let records = vec![record("search", GraphSourceType::Bm25)];

        let selected = indexes_to_auto_sync(&records);

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].graph_source_id, "search:main");
    }

    #[test]
    fn registration_skips_retracted_indexes() {
        let records = vec![retracted("search", GraphSourceType::Bm25)];

        assert!(indexes_to_auto_sync(&records).is_empty());
    }

    #[test]
    fn registration_skips_other_graph_source_types() {
        let records = vec![
            record("vectors", GraphSourceType::Vector),
            record("tables", GraphSourceType::Iceberg),
            record("geo", GraphSourceType::Geo),
        ];

        assert!(indexes_to_auto_sync(&records).is_empty());
    }
}
