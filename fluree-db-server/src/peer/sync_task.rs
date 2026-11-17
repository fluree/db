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

use fluree_db_nameservice::{
    CasResult, NameServiceError, NameServiceEvent, NsRecord, NsRecordSnapshot, RefKind, RefValue,
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
    /// in-memory watermarks. Cache refresh follows from the events the
    /// notifying nameservice emits when the CAS operations succeed.
    async fn handle_ledger_updated(&self, record: &NsRecord) {
        let Some(ns) = self.fluree.nameservice_mode().publisher() else {
            tracing::error!("PeerSyncTask requires a read-write nameservice");
            return;
        };

        // 1. Ensure ledger exists locally (idempotent)
        match ns.init(&record.ledger_id).await {
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
            let commit_head = RefValue {
                id: record.commit_head_id.clone(),
                t: record.commit_t,
            };
            match ns
                .fast_forward_commit(&record.ledger_id, &commit_head, 3)
                .await
            {
                Ok(CasResult::Updated) => {}
                Ok(CasResult::Conflict { actual }) => {
                    let same_head = actual
                        .as_ref()
                        .is_some_and(|a| a.id == record.commit_head_id && a.t == record.commit_t);
                    if !same_head {
                        // Local diverged — server is authoritative in Mode B,
                        // so the peer takes its record whole. A rebase can
                        // move a branch's head to a lower or equal `t`, which
                        // no compare-and-set of the head can follow.
                        tracing::warn!(
                            alias = %record.ledger_id,
                            ?actual,
                            remote_t = record.commit_t,
                            "Local commit diverged from server, force-following"
                        );
                        self.follow_record(record).await;
                        return;
                    }
                    // The head is already the record's (an index-only
                    // update): the cached ledger is dropped so it loads the
                    // index the steps below record.
                    self.fluree.disconnect_ledger(&record.ledger_id).await;
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
        let current = ns
            .get_ref(&record.ledger_id, RefKind::IndexHead)
            .await
            .ok()
            .flatten();
        let local_head = ns
            .get_ref(&record.ledger_id, RefKind::CommitHead)
            .await
            .ok()
            .flatten();
        if remote_lowered_index(record, current.as_ref(), local_head.as_ref()) {
            self.follow_record(record).await;
            return;
        } else if record.index_head_id.is_some() {
            let index_head = RefValue {
                id: record.index_head_id.clone(),
                t: record.index_t,
            };
            let index_result = ns
                .compare_and_set_ref(
                    &record.ledger_id,
                    RefKind::IndexHead,
                    current.as_ref(),
                    &index_head,
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
    }

    /// Retract ledger locally and clear its in-memory watermarks. Cache
    /// eviction follows from the retraction event the notifying
    /// nameservice emits.
    /// Take the server's record whole: head, index and watermark. A rebase
    /// replaces a branch's line, can move its head to a lower `t`, and
    /// resets its index to the source's or to none, none of which the
    /// monotonic refs can follow. The cached ledger is dropped so the next
    /// read loads the record.
    async fn follow_record(&self, record: &NsRecord) {
        let Some(admin) = self.fluree.nameservice_mode().branch_admin() else {
            return;
        };
        let snapshot = NsRecordSnapshot {
            commit_head_id: record.commit_head_id.clone(),
            commit_t: record.commit_t,
            index_head_id: record.index_head_id.clone(),
            index_t: record.index_t,
        };
        match admin.reset_head(record.ledger_id.as_str(), snapshot).await {
            Ok(()) => {
                tracing::info!(
                    alias = %record.ledger_id,
                    commit_t = record.commit_t,
                    index_t = record.index_t,
                    "Local nameservice reset to the remote's record"
                );
                self.fluree.disconnect_ledger(&record.ledger_id).await;
                self.peer_state
                    .reset_ledger(
                        &record.ledger_id,
                        record.commit_t,
                        record.index_t,
                        record.commit_head_id.as_ref().map(ToString::to_string),
                        record.index_head_id.as_ref().map(ToString::to_string),
                    )
                    .await;
            }
            Err(e) => {
                tracing::warn!(
                    alias = %record.ledger_id,
                    error = %e,
                    "Failed to reset the local nameservice to the remote's record"
                );
            }
        }
    }

    async fn handle_ledger_retracted(&self, ledger_id: &fluree_db_api::LedgerId) {
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

        // 2. Clear in-memory watermarks
        self.peer_state.remove_ledger(ledger_id).await;

        tracing::info!(ledger_id = %ledger_id, "Ledger retracted from remote");
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
/// Whether `record` lowers the index this peer holds: the local index is
/// newer than the record's, or the record has none. Only a record for the
/// head the peer now holds counts, so a stale record replayed out of order
/// never lowers it.
fn remote_lowered_index(
    record: &NsRecord,
    local_index: Option<&RefValue>,
    local_head: Option<&RefValue>,
) -> bool {
    let Some(local_index) = local_index.filter(|r| r.id.is_some()) else {
        return false;
    };
    let record_is_current = local_head.is_some_and(|head| {
        head.id.is_some() && head.id == record.commit_head_id && head.t == record.commit_t
    });
    record_is_current && (record.index_head_id.is_none() || local_index.t > record.index_t)
}

fn graph_source_config_hash(config: &str) -> String {
    use sha2::{Digest, Sha256};

    let hash = Sha256::digest(config.as_bytes());
    hex::encode(&hash[..4])
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_api::{FlureeBuilder, ReindexOptions};
    use serde_json::json;

    async fn indexed(fluree: &fluree_db_api::Fluree, ledger_id: &str, commits: usize) -> NsRecord {
        let mut ledger = fluree.create_ledger(ledger_id).await.expect("create");
        for i in 0..commits {
            ledger = fluree
                .insert(
                    ledger,
                    &json!({"@context": {"ex": "http://example.org/"}, "@id": format!("ex:s{i}"), "ex:p": i}),
                )
                .await
                .expect("insert")
                .ledger;
        }
        fluree
            .reindex(ledger_id, ReindexOptions::default())
            .await
            .expect("reindex");
        fluree
            .nameservice()
            .lookup(ledger_id)
            .await
            .expect("lookup")
            .expect("record")
    }

    fn task(fluree: &Arc<fluree_db_api::Fluree>) -> PeerSyncTask {
        PeerSyncTask::new(
            Arc::clone(fluree),
            Arc::new(PeerState::new()),
            SseRemoteWatch::new("http://127.0.0.1:9/fluree/events", None),
            ServerConfig::default(),
        )
    }

    async fn local(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> NsRecord {
        fluree
            .nameservice()
            .lookup(ledger_id)
            .await
            .expect("lookup")
            .expect("record")
    }

    fn reference(id: &Option<fluree_db_core::ContentId>, t: i64) -> RefValue {
        RefValue { id: id.clone(), t }
    }

    #[test]
    fn only_a_current_record_lowers_the_index() {
        let head = Some(fluree_db_core::ContentId::new(
            fluree_db_core::ContentKind::Commit,
            b"head",
        ));
        let older = Some(fluree_db_core::ContentId::new(
            fluree_db_core::ContentKind::Commit,
            b"older",
        ));
        let index = Some(fluree_db_core::ContentId::new(
            fluree_db_core::ContentKind::IndexRoot,
            b"index",
        ));
        let record = |commit: &Option<fluree_db_core::ContentId>, commit_t, index_t| NsRecord {
            commit_head_id: commit.clone(),
            commit_t,
            index_head_id: None,
            index_t,
            ..NsRecord::new("peer:main")
        };
        let local_index = reference(&index, 3);
        let local_head = reference(&head, 5);
        // The record for the held head with no index lowers it.
        assert!(remote_lowered_index(
            &record(&head, 5, 0),
            Some(&local_index),
            Some(&local_head)
        ));
        // A record of another head is not the current one.
        assert!(!remote_lowered_index(
            &record(&older, 4, 0),
            Some(&local_index),
            Some(&local_head)
        ));
        // Nothing to lower without a local index.
        assert!(!remote_lowered_index(
            &record(&head, 5, 0),
            None,
            Some(&local_head)
        ));
        // A record whose index is at least the local one does not lower it.
        let mut same = record(&head, 5, 3);
        same.index_head_id = index.clone();
        assert!(!remote_lowered_index(
            &same,
            Some(&local_index),
            Some(&local_head)
        ));
    }

    /// A rebase resets the branch's index to the source's or to none, under a
    /// head the peer already holds: the peer lowers its index to match.
    #[tokio::test]
    async fn a_record_that_lowers_the_index_is_followed() {
        let fluree = Arc::new(FlureeBuilder::memory().build_memory());
        let before = indexed(&fluree, "peer:main", 2).await;
        assert!(before.index_head_id.is_some(), "indexed");

        let rebased = NsRecord {
            index_head_id: None,
            index_t: 0,
            ..before.clone()
        };
        task(&fluree).handle_ledger_updated(&rebased).await;

        let after = local(&fluree, "peer:main").await;
        assert_eq!(after.index_head_id, None, "the index follows the record");
        assert_eq!(after.commit_head_id, before.commit_head_id);
        assert_eq!(after.commit_t, before.commit_t);
    }

    /// A rebase can move the head to a lower `t` on another line, which the
    /// monotonic head ref cannot follow: the peer takes the record whole.
    #[tokio::test]
    async fn a_record_that_moves_the_head_back_is_followed() {
        let fluree = Arc::new(FlureeBuilder::memory().build_memory());
        let before = indexed(&fluree, "peer:main", 3).await;
        let other = indexed(&fluree, "other:main", 1).await;

        let rebased = NsRecord {
            commit_head_id: other.commit_head_id.clone(),
            commit_t: other.commit_t,
            index_head_id: other.index_head_id.clone(),
            index_t: other.index_t,
            ..before.clone()
        };
        assert!(rebased.commit_t < before.commit_t, "the head moves back");
        task(&fluree).handle_ledger_updated(&rebased).await;

        let after = local(&fluree, "peer:main").await;
        assert_eq!(after.commit_head_id, other.commit_head_id);
        assert_eq!(after.commit_t, other.commit_t);
        assert_eq!(after.index_head_id, other.index_head_id);
        assert_eq!(after.index_t, other.index_t);
    }

    /// The record the peer already holds changes nothing.
    #[tokio::test]
    async fn the_held_record_is_left_alone() {
        let fluree = Arc::new(FlureeBuilder::memory().build_memory());
        let before = indexed(&fluree, "peer:main", 2).await;
        task(&fluree).handle_ledger_updated(&before).await;
        let after = local(&fluree, "peer:main").await;
        assert_eq!(after.commit_head_id, before.commit_head_id);
        assert_eq!(after.index_head_id, before.index_head_id);
        assert_eq!(after.index_t, before.index_t);
    }
}
