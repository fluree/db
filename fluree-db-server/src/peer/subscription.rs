//! SSE subscription task for peer mode, in either storage access mode
//!
//! Drives the shared head-stream pump
//! ([`fluree_db_nameservice_sync::run_head_stream`]) over the transaction
//! server's `/fluree/events` endpoint and applies its events to the peer
//! state and the library-level ledger cache. A peer writes nothing: on
//! shared storage the nameservice is the transaction server's own, and the
//! peer only reads it. The pump owns the loop shape
//! (connect, parse, dispatch, reconnect with the configured backoff,
//! 401/403 fatal); this module owns what the events *mean* for a peer.

use std::sync::Arc;
use std::time::Duration;

use crate::config::ServerConfig;
use crate::peer::state::PeerState;

use async_trait::async_trait;
use fluree_db_api::{Fluree, NotifyResult};
use fluree_db_nameservice::NsRecord;
use fluree_db_nameservice_sync::{
    run_head_stream, HeadSink, HeadStreamConfig, RemoteEvent, ReqwestSseSource, TokioSleeper,
};

/// Background task that maintains SSE subscription to transaction server
pub struct PeerSubscriptionTask {
    config: ServerConfig,
    peer_state: Arc<PeerState>,
    fluree: Arc<Fluree>,
}

impl PeerSubscriptionTask {
    pub fn new(config: ServerConfig, peer_state: Arc<PeerState>, fluree: Arc<Fluree>) -> Self {
        Self {
            config,
            peer_state,
            fluree,
        }
    }

    /// Spawn the subscription task
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }

    async fn run(&self) {
        let url = self.build_events_url();
        tracing::info!(url = %url, "Connecting to transaction server events");

        // Token resolution happens per connect attempt so a rotated
        // on-disk token is picked up at the next reconnect; a load failure
        // is fatal (matches the historical taxonomy).
        let token_config = self.config.clone();
        let source =
            ReqwestSseSource::new(url, Arc::new(move || token_config.load_peer_events_token()));
        let sink = PeerEventSink {
            config: self.config.clone(),
            peer_state: Arc::clone(&self.peer_state),
            fluree: Arc::clone(&self.fluree),
        };
        let stream_config = HeadStreamConfig {
            reconnect_initial: Duration::from_millis(self.config.peer_reconnect_initial_ms),
            reconnect_max: Duration::from_millis(self.config.peer_reconnect_max_ms),
            reconnect_multiplier: self.config.peer_reconnect_multiplier,
        };
        // The task runs for the server's lifetime; the stop sender is held
        // here so the pump ends when the task future is dropped.
        let (_stop, stop_rx) = tokio::sync::watch::channel(false);
        run_head_stream(&source, &sink, &TokioSleeper, stream_config, stop_rx).await;
    }

    fn build_events_url(&self) -> String {
        let mut url = self
            .config
            .peer_events_url()
            .expect("peer_events_url should be set in peer mode");

        let sub = self.config.peer_subscription();
        let mut params = vec![];

        if sub.all {
            params.push("all=true".to_string());
        } else {
            for l in &sub.ledgers {
                params.push(format!("ledger={}", urlencoding::encode(l)));
            }
            for gs in &sub.graph_sources {
                params.push(format!("graph-source={}", urlencoding::encode(gs)));
            }
        }

        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }

        url
    }
}

/// Applies head-stream events to peer state, the ledger cache, and logs —
/// the peer-mode meaning of each event.
struct PeerEventSink {
    config: ServerConfig,
    peer_state: Arc<PeerState>,
    fluree: Arc<Fluree>,
}

impl PeerEventSink {
    async fn preload_configured_ledgers(&self) {
        let sub = self.config.peer_subscription();
        if sub.all || sub.ledgers.is_empty() {
            return;
        }

        for ledger_id in &sub.ledgers {
            // Preload by loading into the connection-level ledger cache.
            let result = self.fluree.ledger_cached(ledger_id).await.map(|_| ());

            match result {
                Ok(()) => {
                    tracing::info!(ledger_id = %ledger_id, "Preloaded ledger into peer cache");
                }
                Err(e) => {
                    tracing::warn!(ledger_id = %ledger_id, error = %e, "Failed to preload ledger");
                }
            }
        }
    }

    /// Bring a ledger this peer has cached up to the record, in place. A
    /// ledger it has not cached is left to load on its first query.
    async fn refresh_cached_ledger(&self, record: &NsRecord) {
        let Some(mgr) = self.fluree.ledger_manager() else {
            return;
        };
        let ledger_id = record.ledger_id.clone();
        match mgr.follow(record.clone()).await {
            Ok(NotifyResult::NotLoaded) => {
                // Not cached - do not cold-load on events (avoids subscribe-all stampede).
            }
            Ok(NotifyResult::Current) => {
                // Already up to date.
            }
            Ok(
                result @ (NotifyResult::Reloaded
                | NotifyResult::IndexUpdated
                | NotifyResult::CommitsApplied { .. }),
            ) => {
                tracing::info!(ledger_id = %ledger_id, ?result, "Refreshed cached ledger from SSE update");
            }
            Err(e) => {
                tracing::warn!(ledger_id = %ledger_id, error = %e, "Failed to refresh cached ledger from SSE update");
            }
        }
    }
}

/// 8-hex-char config fingerprint (matches the server's `sha256_short` used
/// to build graph-source SSE event ids).
fn config_hash(config: &str) -> String {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(config.as_bytes());
    hex::encode(&hash[..4])
}

#[async_trait]
impl HeadSink for PeerEventSink {
    async fn on_event(&self, event: RemoteEvent) {
        match event {
            RemoteEvent::Connected => {
                // Clear state on reconnect (new snapshot coming).
                self.peer_state.clear().await;
                self.peer_state.set_connected(true).await;
                tracing::info!("Connected to transaction server, receiving snapshot");

                // Optional: preload explicitly configured ledgers so the
                // peer starts "warm". We intentionally do NOT preload on
                // subscribe-all to avoid accidentally loading a large
                // number of ledgers.
                self.preload_configured_ledgers().await;
            }
            RemoteEvent::LedgerUpdated(record) => {
                // The cached ledger first: a query that sees the new
                // watermark over a cached ledger behind it reloads it whole.
                self.refresh_cached_ledger(&record).await;
                let changed = self
                    .peer_state
                    .update_ledger(
                        &record.ledger_id,
                        record.commit_t,
                        record.index_t,
                        record.commit_head_id.as_ref().map(ToString::to_string),
                        record.index_head_id.as_ref().map(ToString::to_string),
                    )
                    .await;
                if changed {
                    tracing::info!(
                        ledger_id = %record.ledger_id,
                        commit_t = record.commit_t,
                        index_t = record.index_t,
                        "Remote ledger watermark updated"
                    );
                }
            }
            RemoteEvent::GraphSourceUpdated(record) => {
                let changed = self
                    .peer_state
                    .update_graph_source(
                        &record.graph_source_id,
                        record.index_t,
                        config_hash(&record.config),
                        record.index_id.as_ref().map(ToString::to_string),
                    )
                    .await;
                if changed {
                    tracing::info!(
                        graph_source_id = %record.graph_source_id,
                        index_t = record.index_t,
                        "Remote graph source watermark updated"
                    );
                }
            }
            RemoteEvent::LedgerRetracted { ledger_id } => {
                self.peer_state.remove_ledger(&ledger_id).await;
                tracing::info!(ledger_id = %ledger_id, "Ledger retracted from remote");

                // Evict any cached state for the ledger (no-op if not cached).
                self.fluree.disconnect_ledger(&ledger_id).await;
            }
            RemoteEvent::GraphSourceRetracted { graph_source_id } => {
                self.peer_state.remove_graph_source(&graph_source_id).await;
                tracing::info!(graph_source_id = %graph_source_id, "Graph source retracted from remote");
            }
            RemoteEvent::Disconnected { reason } => {
                self.peer_state.set_connected(false).await;
                tracing::warn!(reason = %reason, "Peer SSE disconnected, will reconnect");
            }
            RemoteEvent::Fatal { reason } => {
                self.peer_state.set_connected(false).await;
                tracing::error!(reason = %reason, "Fatal peer subscription error, will not retry");
            }
        }
    }
}

/// Historical error taxonomy for peer subscriptions. The connection loop
/// now lives in the shared head-stream pump (which classifies 401/403 and
/// token-load failures as fatal exactly as [`is_fatal`](Self::is_fatal)
/// did); the type is kept for API compatibility.
#[derive(Debug, thiserror::Error)]
pub enum PeerSubscriptionError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("HTTP status {0}")]
    HttpStatus(reqwest::StatusCode),

    #[error("Failed to load token: {0}")]
    TokenLoad(std::io::Error),

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
}

impl PeerSubscriptionError {
    pub fn is_fatal(&self) -> bool {
        match self {
            PeerSubscriptionError::HttpStatus(status) => {
                status.as_u16() == 401 || status.as_u16() == 403
            }
            PeerSubscriptionError::TokenLoad(_) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_api::{FlureeBuilder, LedgerId};
    use serde_json::json;

    /// A transaction server's root and a shared-storage peer reading it.
    struct SharedRoot {
        _dir: tempfile::TempDir,
        writer: Fluree,
        peer: Arc<Fluree>,
        sink: PeerEventSink,
    }

    impl SharedRoot {
        async fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().to_string_lossy().to_string();
            let writer = FlureeBuilder::file(path.clone())
                .without_indexing()
                .build()
                .unwrap();
            let peer = Arc::new(
                FlureeBuilder::file(path)
                    .read_only()
                    .build_client()
                    .await
                    .unwrap(),
            );
            let sink = PeerEventSink {
                config: ServerConfig::default(),
                peer_state: Arc::new(PeerState::new()),
                fluree: Arc::clone(&peer),
            };
            Self {
                _dir: dir,
                writer,
                peer,
                sink,
            }
        }

        async fn commit(&self, n: usize) -> NsRecord {
            let ledger = match self.writer.ledger("peer:main").await {
                Ok(ledger) => ledger,
                Err(_) => self.writer.create_ledger("peer:main").await.unwrap(),
            };
            self.writer
                .insert(
                    ledger,
                    &json!({"@context": {"ex": "http://example.org/"},
                            "@id": format!("ex:s{n}"), "ex:p": n}),
                )
                .await
                .unwrap();
            self.record().await
        }

        async fn record(&self) -> NsRecord {
            self.writer
                .nameservice()
                .lookup("peer:main")
                .await
                .unwrap()
                .unwrap()
        }

        async fn cached_t(&self) -> Option<i64> {
            self.peer
                .ledger_manager()
                .unwrap()
                .current_t(&LedgerId::parse("peer:main").unwrap())
                .await
        }
    }

    /// The peer has already read past the record: it keeps its cached
    /// ledger, and the transaction server's record is not moved back.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_record_the_peer_has_passed_changes_nothing() {
        let root = SharedRoot::new().await;
        let behind = root.commit(1).await;
        let head = root.commit(2).await;
        root.peer.ledger_cached("peer:main").await.unwrap();

        root.sink.on_event(RemoteEvent::LedgerUpdated(behind)).await;

        assert_eq!(root.cached_t().await, Some(head.commit_t), "still cached");
        let after = root.record().await;
        assert_eq!(
            (after.commit_head_id, after.commit_t),
            (head.commit_head_id, head.commit_t)
        );
    }

    /// A new commit is applied to the cached ledger in place: it is not
    /// evicted, so no query waits on a cold load.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_new_commit_is_applied_to_the_cached_ledger() {
        let root = SharedRoot::new().await;
        root.commit(1).await;
        root.peer.ledger_cached("peer:main").await.unwrap();
        let next = root.commit(2).await;

        root.sink
            .on_event(RemoteEvent::LedgerUpdated(next.clone()))
            .await;

        assert_eq!(root.cached_t().await, Some(next.commit_t));
        let watermark = root
            .sink
            .peer_state
            .get_remote_ledger(&next.ledger_id)
            .await
            .unwrap();
        assert_eq!(watermark.commit_t, next.commit_t);
    }

    /// A shared-storage peer opens the root as a reader.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_peer_cannot_write_the_nameservice() {
        let root = SharedRoot::new().await;
        assert!(root.peer.nameservice_mode().is_read_only());
        assert!(root.peer.nameservice_mode().publisher().is_none());
    }
}
