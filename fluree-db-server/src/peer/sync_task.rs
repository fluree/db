//! Nameservice sync task for peer mode (shared storage)
//!
//! Unlike [`PeerSubscriptionTask`](super::subscription::PeerSubscriptionTask) which only
//! updates in-memory [`PeerState`] watermarks, `PeerSyncTask` **persists remote refs into
//! the local [`FileNameService`]** via `RefPublisher` CAS operations. This means a restarted
//! peer can serve queries immediately without waiting for SSE replay.
//!
//! Used for shared-storage peers where the nameservice is read-write.
//! Proxy-storage peers continue to use `PeerSubscriptionTask`.

use std::sync::Arc;

use fluree_db_api::{NotifyResult, NsNotify};
use fluree_db_nameservice::{
    CasResult, NameServiceError, NameServiceEvent, NsRecord, RefKind, RefValue,
};
use fluree_db_nameservice_sync::watch::{RemoteEvent, RemoteWatch};
use fluree_db_nameservice_sync::SseRemoteWatch;
use futures::StreamExt;

use crate::config::ServerConfig;
use crate::peer::state::PeerState;

/// Background task that syncs nameservice state from a remote transaction server
/// into the local nameservice via SSE events and `RefPublisher` CAS operations.
pub struct PeerSyncTask {
    fluree: Arc<fluree_db_api::Fluree>,
    peer_state: Arc<PeerState>,
    watch: SseRemoteWatch,
    config: ServerConfig,
}

impl PeerSyncTask {
    pub fn new(
        fluree: Arc<fluree_db_api::Fluree>,
        peer_state: Arc<PeerState>,
        watch: SseRemoteWatch,
        config: ServerConfig,
    ) -> Self {
        Self {
            fluree,
            peer_state,
            watch,
            config,
        }
    }

    /// Spawn the sync task as a background tokio task.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }

    async fn run(&self) {
        let mut stream = self.watch.watch();

        while let Some(event) = stream.next().await {
            match event {
                RemoteEvent::Connected => {
                    // Mirror PeerSubscriptionTask: clear stale state, mark connected, preload
                    self.peer_state.clear().await;
                    self.peer_state.set_connected(true).await;

                    tracing::info!("Connected to transaction server, receiving snapshot");
                    self.preload_configured_ledgers().await;
                }
                RemoteEvent::Disconnected { reason } => {
                    self.peer_state.set_connected(false).await;
                    tracing::warn!(reason = %reason, "Disconnected from transaction server");
                }
                RemoteEvent::Fatal { reason } => {
                    self.peer_state.set_connected(false).await;
                    tracing::error!(
                        reason = %reason,
                        "Fatal peer sync error, will not retry"
                    );
                    break;
                }
                RemoteEvent::LedgerUpdated(record) => {
                    self.handle_ledger_updated(&record).await;
                }
                RemoteEvent::LedgerRetracted { ledger_id } => {
                    self.handle_ledger_retracted(&ledger_id).await;
                }
                RemoteEvent::GraphSourceUpdated(record) => {
                    let graph_source_id = record.graph_source_id.clone();
                    let config_hash = graph_source_config_hash(&record.config);
                    let changed = self
                        .peer_state
                        .update_graph_source(
                            &graph_source_id,
                            record.index_t,
                            config_hash,
                            record
                                .index_id
                                .as_ref()
                                .map(std::string::ToString::to_string),
                        )
                        .await;

                    if changed {
                        // Emit event for graph source index update
                        if let Some(ref index_id) = record.index_id {
                            self.fluree.event_bus().notify(
                                NameServiceEvent::GraphSourceIndexPublished {
                                    graph_source_id: graph_source_id.clone(),
                                    index_id: index_id.clone(),
                                    index_t: record.index_t,
                                },
                            );
                        }
                        tracing::info!(
                            graph_source_id = %graph_source_id,
                            index_t = record.index_t,
                            "Remote graph source watermark updated"
                        );
                    }
                }
                RemoteEvent::GraphSourceRetracted { graph_source_id } => {
                    self.peer_state.remove_graph_source(&graph_source_id).await;
                    self.fluree
                        .event_bus()
                        .notify(NameServiceEvent::GraphSourceRetracted {
                            graph_source_id: graph_source_id.clone(),
                        });
                    tracing::info!(graph_source_id = %graph_source_id, "Graph source retracted from remote");
                }
            }
        }
    }

    /// Persist remote ledger state into local FileNameService, then update
    /// in-memory watermarks and notify LedgerManager.
    async fn handle_ledger_updated(&self, record: &NsRecord) {
        let Some(ns) = self.fluree.nameservice_mode().publisher() else {
            tracing::error!("PeerSyncTask requires a read-write nameservice");
            return;
        };

        // 1. Ensure ledger exists locally (idempotent)
        match ns.publish_ledger_init(&record.ledger_id).await {
            Ok(()) => {}
            Err(NameServiceError::LedgerAlreadyExists(_)) => {}
            Err(e) => {
                tracing::warn!(
                    alias = %record.ledger_id,
                    error = %e,
                    "Failed to init ledger locally"
                );
                return;
            }
        }

        // 2. Fast-forward commit head (if record has a commit)
        if record.commit_head_id.is_some() {
            let commit_ref = RefValue {
                id: record.commit_head_id.clone(),
                t: record.commit_t,
            };
            match ns
                .fast_forward_commit(&record.ledger_id, &commit_ref, 3)
                .await
            {
                Ok(CasResult::Updated) => {}
                Ok(CasResult::Conflict { actual }) => {
                    // Local diverged — server is authoritative in Mode B, force-follow.
                    tracing::warn!(
                        alias = %record.ledger_id,
                        ?actual,
                        remote_t = record.commit_t,
                        "Local commit diverged from server, force-following"
                    );
                    self.fluree.disconnect_ledger(&record.ledger_id).await;
                    if let Some(ref cur) = actual {
                        if cur.t > record.commit_t {
                            // This should never happen in peer Mode B (writes are forwarded),
                            // but if it does, we cannot "force-follow" due to the strict
                            // monotonic guard for commit refs (new.t must be > current.t).
                            tracing::error!(
                                alias = %record.ledger_id,
                                local_t = cur.t,
                                remote_t = record.commit_t,
                                "Local commit_t is ahead of remote; refusing to force-follow"
                            );
                            return;
                        }
                    }
                    let force_result = ns
                        .compare_and_set_ref(
                            &record.ledger_id,
                            RefKind::CommitHead,
                            actual.as_ref(),
                            &commit_ref,
                        )
                        .await;

                    match force_result {
                        Ok(CasResult::Updated) => {
                            tracing::info!(
                                alias = %record.ledger_id,
                                commit_t = record.commit_t,
                                "Force-follow CAS updated local commit head"
                            );
                        }
                        Ok(CasResult::Conflict { actual: new_actual }) => {
                            tracing::warn!(
                                alias = %record.ledger_id,
                                ?new_actual,
                                remote_t = record.commit_t,
                                "Force-follow CAS still conflicted (local may remain divergent)"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                alias = %record.ledger_id,
                                error = %e,
                                "Force-follow CAS failed"
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        alias = %record.ledger_id,
                        error = %e,
                        "Failed to fast-forward commit"
                    );
                    return;
                }
            }
        }

        // 3. Update index head — read current first, NOT expected=None
        if record.index_head_id.is_some() {
            let index_ref = RefValue {
                id: record.index_head_id.clone(),
                t: record.index_t,
            };
            let current = ns
                .get_ref(&record.ledger_id, RefKind::IndexHead)
                .await
                .ok()
                .flatten();
            let index_result = ns
                .compare_and_set_ref(
                    &record.ledger_id,
                    RefKind::IndexHead,
                    current.as_ref(),
                    &index_ref,
                )
                .await;
            match index_result {
                Ok(CasResult::Updated) => {}
                Ok(CasResult::Conflict { actual }) => {
                    if let Some(ref cur) = actual {
                        if cur.t > record.index_t {
                            tracing::warn!(
                                alias = %record.ledger_id,
                                local_index_t = cur.t,
                                remote_index_t = record.index_t,
                                "Local index_t is ahead of remote; index ref not updated"
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        alias = %record.ledger_id,
                        error = %e,
                        "Failed to update index ref"
                    );
                }
            }
        }

        // 4. Update in-memory watermarks (AFTER persisting to NS)
        let changed = self
            .peer_state
            .update_ledger(
                &record.ledger_id,
                record.commit_t,
                record.index_t,
                record
                    .commit_head_id
                    .as_ref()
                    .map(std::string::ToString::to_string),
                record
                    .index_head_id
                    .as_ref()
                    .map(std::string::ToString::to_string),
            )
            .await;

        if changed {
            tracing::info!(
                alias = %record.ledger_id,
                commit_t = record.commit_t,
                index_t = record.index_t,
                "Remote ledger watermark updated (persisted to local NS)"
            );
        }

        // 5. Notify LedgerManager (AFTER NS is updated, so reload sees new refs)
        // Note: events are emitted automatically by NotifyingNameService when
        // the CAS operations above succeed — no manual emission needed here.
        self.refresh_cached_ledger(record).await;
    }

    /// Retract ledger locally and evict from cache.
    async fn handle_ledger_retracted(&self, ledger_id: &str) {
        // 1. Retract via Publisher::retract()
        let Some(ns) = self.fluree.nameservice_mode().publisher() else {
            tracing::error!("PeerSyncTask requires a read-write nameservice");
            return;
        };
        if let Err(e) = ns.retract(ledger_id).await {
            tracing::warn!(
                ledger_id = %ledger_id,
                error = %e,
                "Failed to retract ledger locally"
            );
        }

        // Note: retraction event emitted automatically by NotifyingNameService.

        // 2. Clear in-memory watermarks
        self.peer_state.remove_ledger(ledger_id).await;

        // 3. Evict from cache
        self.fluree.disconnect_ledger(ledger_id).await;

        tracing::info!(ledger_id = %ledger_id, "Ledger retracted from remote");
    }

    /// Notify LedgerManager to refresh a cached ledger from the NS update.
    async fn refresh_cached_ledger(&self, record: &NsRecord) {
        let Some(mgr) = self.fluree.ledger_manager() else {
            return;
        };

        match mgr
            .notify(NsNotify {
                ledger_id: record.ledger_id.clone(),
                record: Some(record.clone()),
            })
            .await
        {
            Ok(NotifyResult::NotLoaded) => {
                // Not cached — do not cold-load on events
            }
            Ok(NotifyResult::Current) => {
                // Already up to date
            }
            Ok(
                result @ (NotifyResult::Reloaded
                | NotifyResult::IndexUpdated
                | NotifyResult::CommitsApplied { .. }),
            ) => {
                let after_t = mgr.current_t(&record.ledger_id).await;
                tracing::debug!(
                    alias = %record.ledger_id,
                    after_cached_t = ?after_t,
                    ?result,
                    "refreshed cached ledger from SSE update"
                );
            }
            Err(e) => {
                tracing::warn!(
                    alias = %record.ledger_id,
                    error = %e,
                    "Failed to refresh cached ledger from SSE update"
                );
            }
        }
    }

    /// Preload explicitly configured ledgers into the cache.
    async fn preload_configured_ledgers(&self) {
        let sub = self.config.peer_subscription();
        if sub.all || sub.ledgers.is_empty() {
            return;
        }

        for ledger_id in &sub.ledgers {
            match self.fluree.ledger_cached(ledger_id).await {
                Ok(_) => {
                    tracing::info!(ledger_id = %ledger_id, "Preloaded ledger into peer cache");
                }
                Err(e) => {
                    tracing::warn!(
                        ledger_id = %ledger_id,
                        error = %e,
                        "Failed to preload ledger"
                    );
                }
            }
        }
    }
}

/// Compute a config hash for graph source change detection.
///
/// Uses SHA-256 truncated to 8 hex chars (4 bytes) to match the server's graph source SSE
/// event ID format. Same algorithm as `GraphSourceRecord::config_hash()` in `fluree-db-peer`.
fn graph_source_config_hash(config: &str) -> String {
    use sha2::{Digest, Sha256};

    let hash = Sha256::digest(config.as_bytes());
    hex::encode(&hash[..4])
}
