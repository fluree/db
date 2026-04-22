//! Peer runtime orchestration
//!
//! The PeerRuntime ties together the SSE client, state machine, and callbacks.
//! It handles events from the SSE stream, updates state, and invokes callbacks.

use std::sync::Arc;

use async_trait::async_trait;

use crate::config::PeerConfig;
use crate::error::PeerError;
use crate::sse::{SseClient, SseClientEvent};
use crate::state::{GraphSourceState, LedgerState, PeerState};

/// Callback trait for peer events
///
/// Implement this trait to receive notifications when state changes.
/// All methods have default no-op implementations.
#[async_trait]
pub trait PeerCallbacks: Send + Sync {
    /// Called when connected to the events endpoint
    async fn on_connected(&self) {}

    /// Called when disconnected from the events endpoint
    async fn on_disconnected(&self, _reason: &str) {}

    /// Called when the initial snapshot is complete
    async fn on_snapshot_complete(&self, _hash: &str) {}

    /// Called when a ledger is created or updated
    async fn on_ledger_updated(&self, _ledger_id: &str, _state: &LedgerState) {}

    /// Called when a graph source is created or updated
    async fn on_graph_source_updated(&self, _graph_source_id: &str, _state: &GraphSourceState) {}

    /// Called when a resource is retracted
    async fn on_retracted(&self, _kind: &str, _resource_id: &str) {}
}

/// Default logging-only callbacks
pub struct LoggingCallbacks;

#[async_trait]
impl PeerCallbacks for LoggingCallbacks {
    async fn on_connected(&self) {
        tracing::info!("Connected to events endpoint");
    }

    async fn on_disconnected(&self, reason: &str) {
        tracing::warn!(reason, "Disconnected from events endpoint");
    }

    async fn on_snapshot_complete(&self, hash: &str) {
        tracing::info!(hash, "Snapshot complete");
    }

    async fn on_ledger_updated(&self, ledger_id: &str, state: &LedgerState) {
        tracing::info!(
            ledger_id,
            commit_t = state.commit_t,
            index_t = state.index_t,
            "Ledger updated"
        );
    }

    async fn on_graph_source_updated(&self, graph_source_id: &str, state: &GraphSourceState) {
        tracing::info!(
            graph_source_id,
            index_t = state.index_t,
            config_hash = %state.config_hash,
            "Graph source updated"
        );
    }

    async fn on_retracted(&self, kind: &str, resource_id: &str) {
        tracing::info!(kind, resource_id, "Resource retracted");
    }
}

/// The peer runtime orchestrates SSE client + state + callbacks
pub struct PeerRuntime<C: PeerCallbacks> {
    config: PeerConfig,
    state: Arc<PeerState>,
    callbacks: Arc<C>,
}

impl<C: PeerCallbacks + 'static> PeerRuntime<C> {
    /// Create a new runtime with the given config and callbacks
    pub fn new(config: PeerConfig, callbacks: C) -> Self {
        Self {
            config,
            state: Arc::new(PeerState::new()),
            callbacks: Arc::new(callbacks),
        }
    }

    /// Get a reference to the state (for query serving)
    pub fn state(&self) -> Arc<PeerState> {
        self.state.clone()
    }

    /// Get a reference to the callbacks
    pub fn callbacks(&self) -> Arc<C> {
        self.callbacks.clone()
    }

    /// Start the runtime (blocking)
    ///
    /// This runs the event loop, handling SSE events until the connection
    /// is permanently closed or the process is terminated.
    pub async fn run(&self) -> Result<(), PeerError> {
        self.config
            .validate()
            .map_err(|e| PeerError::Config(e.to_string()))?;

        let client = SseClient::new(self.config.clone());
        let mut events = client.start();

        while let Some(event) = events.recv().await {
            self.handle_event(event).await;
        }

        Ok(())
    }

    async fn handle_event(&self, event: SseClientEvent) {
        match event {
            SseClientEvent::Connected => {
                // Clear state on reconnect to receive fresh snapshot
                self.state.clear().await;
                self.callbacks.on_connected().await;
            }

            SseClientEvent::SnapshotComplete { hash } => {
                self.state.set_snapshot_hash(hash.clone()).await;
                self.callbacks.on_snapshot_complete(&hash).await;
            }

            SseClientEvent::LedgerRecord(record) => {
                let changed = self.state.handle_ledger_record(&record).await;
                if changed {
                    if let Some(state) = self.state.get_ledger(&record.ledger_id).await {
                        self.callbacks
                            .on_ledger_updated(&record.ledger_id, &state)
                            .await;
                    }
                }
            }

            SseClientEvent::GraphSourceRecord(record) => {
                let changed = self.state.handle_graph_source_record(&record).await;
                if changed {
                    if let Some(state) = self.state.get_graph_source(&record.graph_source_id).await
                    {
                        self.callbacks
                            .on_graph_source_updated(&record.graph_source_id, &state)
                            .await;
                    }
                }
            }

            SseClientEvent::Retracted { kind, resource_id } => {
                self.state.handle_retracted(&kind, &resource_id).await;
                self.callbacks.on_retracted(&kind, &resource_id).await;
            }

            SseClientEvent::Disconnected { reason } => {
                self.callbacks.on_disconnected(&reason).await;
            }

            SseClientEvent::Fatal { error } => {
                tracing::error!(error, "Fatal SSE error");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingCallbacks {
        connected: AtomicUsize,
        ledger_updates: AtomicUsize,
        graph_source_updates: AtomicUsize,
        retractions: AtomicUsize,
    }

    impl CountingCallbacks {
        fn new() -> Self {
            Self {
                connected: AtomicUsize::new(0),
                ledger_updates: AtomicUsize::new(0),
                graph_source_updates: AtomicUsize::new(0),
                retractions: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl PeerCallbacks for CountingCallbacks {
        async fn on_connected(&self) {
            self.connected.fetch_add(1, Ordering::SeqCst);
        }

        async fn on_ledger_updated(&self, _ledger_id: &str, _state: &LedgerState) {
            self.ledger_updates.fetch_add(1, Ordering::SeqCst);
        }

        async fn on_graph_source_updated(&self, _graph_source_id: &str, _state: &GraphSourceState) {
            self.graph_source_updates.fetch_add(1, Ordering::SeqCst);
        }

        async fn on_retracted(&self, _kind: &str, _resource_id: &str) {
            self.retractions.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn test_handle_event_connected() {
        let config = PeerConfig::default();
        let callbacks = CountingCallbacks::new();
        let runtime = PeerRuntime::new(config, callbacks);

        runtime.handle_event(SseClientEvent::Connected).await;

        assert_eq!(runtime.callbacks.connected.load(Ordering::SeqCst), 1);
        assert!(!runtime.state.is_snapshot_received().await);
    }

    #[tokio::test]
    async fn test_handle_event_ledger_record() {
        let config = PeerConfig::default();
        let callbacks = CountingCallbacks::new();
        let runtime = PeerRuntime::new(config, callbacks);

        use crate::sse::LedgerRecord;
        let record = LedgerRecord {
            ledger_id: "books:main".to_string(),
            branch: Some("main".to_string()),
            commit_head_id: Some("commit-cid:1".to_string()),
            commit_t: 5,
            index_head_id: Some("index-cid:1".to_string()),
            index_t: 3,
            retracted: false,
        };

        runtime
            .handle_event(SseClientEvent::LedgerRecord(record))
            .await;

        assert_eq!(runtime.callbacks.ledger_updates.load(Ordering::SeqCst), 1);
        let ledger = runtime.state.get_ledger("books:main").await.unwrap();
        assert_eq!(ledger.commit_t, 5);
    }

    #[tokio::test]
    async fn test_handle_event_retracted() {
        let config = PeerConfig::default();
        let callbacks = CountingCallbacks::new();
        let runtime = PeerRuntime::new(config, callbacks);

        // First add a ledger
        use crate::sse::LedgerRecord;
        let record = LedgerRecord {
            ledger_id: "books:main".to_string(),
            branch: Some("main".to_string()),
            commit_head_id: Some("commit-cid:1".to_string()),
            commit_t: 5,
            index_head_id: Some("index-cid:1".to_string()),
            index_t: 3,
            retracted: false,
        };
        runtime
            .handle_event(SseClientEvent::LedgerRecord(record))
            .await;
        assert!(runtime.state.get_ledger("books:main").await.is_some());

        // Then retract it
        runtime
            .handle_event(SseClientEvent::Retracted {
                kind: "ledger".to_string(),
                resource_id: "books:main".to_string(),
            })
            .await;

        assert_eq!(runtime.callbacks.retractions.load(Ordering::SeqCst), 1);
        assert!(runtime.state.get_ledger("books:main").await.is_none());
    }

    #[tokio::test]
    async fn test_reconnect_clears_state() {
        let config = PeerConfig::default();
        let callbacks = CountingCallbacks::new();
        let runtime = PeerRuntime::new(config, callbacks);

        // Add a ledger
        use crate::sse::LedgerRecord;
        let record = LedgerRecord {
            ledger_id: "books:main".to_string(),
            branch: Some("main".to_string()),
            commit_head_id: Some("commit-cid:1".to_string()),
            commit_t: 5,
            index_head_id: Some("index-cid:1".to_string()),
            index_t: 3,
            retracted: false,
        };
        runtime
            .handle_event(SseClientEvent::LedgerRecord(record))
            .await;
        assert_eq!(runtime.state.ledger_count().await, 1);

        // Simulate reconnect
        runtime.handle_event(SseClientEvent::Connected).await;

        // State should be cleared
        assert_eq!(runtime.state.ledger_count().await, 0);
        assert_eq!(runtime.callbacks.connected.load(Ordering::SeqCst), 1);
    }
}
