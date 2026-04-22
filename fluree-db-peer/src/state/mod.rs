//! State tracking for the query peer
//!
//! Maintains an in-memory view of ledger and graph source metadata
//! based on events received from the SSE stream.

use std::collections::HashMap;
use std::time::Instant;

use tokio::sync::RwLock;

use crate::sse::{GraphSourceRecord, LedgerRecord};
use fluree_sse::{SSE_KIND_GRAPH_SOURCE, SSE_KIND_LEDGER};

/// Tracked state for a ledger
#[derive(Debug, Clone)]
pub struct LedgerState {
    pub ledger_id: String,
    pub commit_t: i64,
    pub index_t: i64,
    pub commit_head_id: Option<String>,
    pub index_head_id: Option<String>,
    /// Needs refresh (state changed since last query operation)
    pub dirty: bool,
    pub last_updated: Instant,
}

/// Tracked state for a graph source
#[derive(Debug, Clone)]
pub struct GraphSourceState {
    pub graph_source_id: String,
    pub index_t: i64,
    pub config_hash: String,
    pub index_id: Option<String>,
    pub dependencies: Vec<String>,
    /// Needs refresh
    pub dirty: bool,
    pub last_updated: Instant,
}

/// Central state tracker for the peer
pub struct PeerState {
    ledgers: RwLock<HashMap<String, LedgerState>>,
    graph_sources: RwLock<HashMap<String, GraphSourceState>>,
    snapshot_hash: RwLock<Option<String>>,
    /// Whether we've received the initial snapshot
    snapshot_received: RwLock<bool>,
}

impl PeerState {
    /// Create a new empty state tracker
    pub fn new() -> Self {
        Self {
            ledgers: RwLock::new(HashMap::new()),
            graph_sources: RwLock::new(HashMap::new()),
            snapshot_hash: RwLock::new(None),
            snapshot_received: RwLock::new(false),
        }
    }

    /// Handle a ledger record from SSE
    /// Returns true if the state changed (needs action)
    pub async fn handle_ledger_record(&self, record: &LedgerRecord) -> bool {
        let mut ledgers = self.ledgers.write().await;

        let changed = match ledgers.get(&record.ledger_id) {
            Some(existing) => {
                // Check if watermarks advanced
                record.commit_t > existing.commit_t || record.index_t > existing.index_t
            }
            None => true, // New ledger
        };

        if changed {
            ledgers.insert(
                record.ledger_id.clone(),
                LedgerState {
                    ledger_id: record.ledger_id.clone(),
                    commit_t: record.commit_t,
                    index_t: record.index_t,
                    commit_head_id: record.commit_head_id.clone(),
                    index_head_id: record.index_head_id.clone(),
                    dirty: true,
                    last_updated: Instant::now(),
                },
            );
        }

        changed
    }

    /// Handle a graph source record from SSE
    /// Returns true if the state changed
    pub async fn handle_graph_source_record(&self, record: &GraphSourceRecord) -> bool {
        let mut graph_sources = self.graph_sources.write().await;

        let config_hash = record.config_hash();

        let changed = match graph_sources.get(&record.graph_source_id) {
            Some(existing) => {
                record.index_t > existing.index_t || config_hash != existing.config_hash
            }
            None => true,
        };

        if changed {
            graph_sources.insert(
                record.graph_source_id.clone(),
                GraphSourceState {
                    graph_source_id: record.graph_source_id.clone(),
                    index_t: record.index_t,
                    config_hash,
                    index_id: record.index_id.clone(),
                    dependencies: record.dependencies.clone(),
                    dirty: true,
                    last_updated: Instant::now(),
                },
            );
        }

        changed
    }

    /// Handle retraction (remove from state)
    pub async fn handle_retracted(&self, kind: &str, resource_id: &str) {
        match kind {
            SSE_KIND_LEDGER => {
                self.ledgers.write().await.remove(resource_id);
            }
            SSE_KIND_GRAPH_SOURCE => {
                self.graph_sources.write().await.remove(resource_id);
            }
            _ => {
                tracing::warn!(kind, resource_id, "Unknown retraction kind");
            }
        }
    }

    /// Mark snapshot complete
    pub async fn set_snapshot_hash(&self, hash: String) {
        *self.snapshot_hash.write().await = Some(hash);
        *self.snapshot_received.write().await = true;
    }

    /// Mark snapshot as received (even without hash)
    pub async fn mark_snapshot_received(&self) {
        *self.snapshot_received.write().await = true;
    }

    /// Check if initial snapshot has been received
    pub async fn is_snapshot_received(&self) -> bool {
        *self.snapshot_received.read().await
    }

    /// Clear all state (on reconnect, before new snapshot)
    pub async fn clear(&self) {
        self.ledgers.write().await.clear();
        self.graph_sources.write().await.clear();
        *self.snapshot_hash.write().await = None;
        *self.snapshot_received.write().await = false;
    }

    /// Get current ledger state
    pub async fn get_ledger(&self, ledger_id: &str) -> Option<LedgerState> {
        self.ledgers.read().await.get(ledger_id).cloned()
    }

    /// Get current graph source state
    pub async fn get_graph_source(&self, graph_source_id: &str) -> Option<GraphSourceState> {
        self.graph_sources
            .read()
            .await
            .get(graph_source_id)
            .cloned()
    }

    /// Get all ledger states
    pub async fn all_ledgers(&self) -> Vec<LedgerState> {
        self.ledgers.read().await.values().cloned().collect()
    }

    /// Get all graph source states
    pub async fn all_graph_sources(&self) -> Vec<GraphSourceState> {
        self.graph_sources.read().await.values().cloned().collect()
    }

    /// Get all dirty ledgers (need refresh)
    pub async fn dirty_ledgers(&self) -> Vec<LedgerState> {
        self.ledgers
            .read()
            .await
            .values()
            .filter(|l| l.dirty)
            .cloned()
            .collect()
    }

    /// Get all dirty graph sources
    pub async fn dirty_graph_sources(&self) -> Vec<GraphSourceState> {
        self.graph_sources
            .read()
            .await
            .values()
            .filter(|gs| gs.dirty)
            .cloned()
            .collect()
    }

    /// Mark ledger as clean (after refresh)
    pub async fn mark_ledger_clean(&self, ledger_id: &str) {
        if let Some(ledger) = self.ledgers.write().await.get_mut(ledger_id) {
            ledger.dirty = false;
        }
    }

    /// Mark graph source as clean
    pub async fn mark_graph_source_clean(&self, graph_source_id: &str) {
        if let Some(gs) = self.graph_sources.write().await.get_mut(graph_source_id) {
            gs.dirty = false;
        }
    }

    /// Get ledger count
    pub async fn ledger_count(&self) -> usize {
        self.ledgers.read().await.len()
    }

    /// Get graph source count
    pub async fn graph_source_count(&self) -> usize {
        self.graph_sources.read().await.len()
    }

    /// Get the current snapshot hash
    pub async fn snapshot_hash(&self) -> Option<String> {
        self.snapshot_hash.read().await.clone()
    }
}

impl Default for PeerState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ledger_record(ledger_id: &str, commit_t: i64, index_t: i64) -> LedgerRecord {
        LedgerRecord {
            ledger_id: ledger_id.to_string(),
            branch: Some("main".to_string()),
            commit_head_id: Some(format!("commit-cid:{commit_t}")),
            commit_t,
            index_head_id: Some(format!("index-cid:{index_t}")),
            index_t,
            retracted: false,
        }
    }

    fn make_graph_source_record(graph_source_id: &str, index_t: i64) -> GraphSourceRecord {
        GraphSourceRecord {
            graph_source_id: graph_source_id.to_string(),
            name: Some("test".to_string()),
            branch: Some("main".to_string()),
            source_type: Some("fulltext".to_string()),
            config: Some(r#"{"analyzer": "standard"}"#.to_string()),
            dependencies: vec![],
            index_id: Some(format!("gs-cid:{index_t}")),
            index_t,
            retracted: false,
        }
    }

    #[tokio::test]
    async fn test_handle_ledger_record_new() {
        let state = PeerState::new();
        let record = make_ledger_record("books:main", 5, 3);

        let changed = state.handle_ledger_record(&record).await;
        assert!(changed);

        let ledger = state.get_ledger("books:main").await.unwrap();
        assert_eq!(ledger.commit_t, 5);
        assert_eq!(ledger.index_t, 3);
        assert!(ledger.dirty);
    }

    #[tokio::test]
    async fn test_handle_ledger_record_update() {
        let state = PeerState::new();

        // Initial record
        let record1 = make_ledger_record("books:main", 5, 3);
        state.handle_ledger_record(&record1).await;

        // Same watermarks - no change
        let record2 = make_ledger_record("books:main", 5, 3);
        let changed = state.handle_ledger_record(&record2).await;
        assert!(!changed);

        // Higher commit_t - change
        let record3 = make_ledger_record("books:main", 6, 3);
        let changed = state.handle_ledger_record(&record3).await;
        assert!(changed);

        // Higher index_t - change
        let record4 = make_ledger_record("books:main", 6, 5);
        let changed = state.handle_ledger_record(&record4).await;
        assert!(changed);
    }

    #[tokio::test]
    async fn test_handle_graph_source_record() {
        let state = PeerState::new();
        let record = make_graph_source_record("search:main", 2);

        let changed = state.handle_graph_source_record(&record).await;
        assert!(changed);

        let gs = state.get_graph_source("search:main").await.unwrap();
        assert_eq!(gs.index_t, 2);
        assert!(gs.dirty);
    }

    #[tokio::test]
    async fn test_handle_retracted() {
        let state = PeerState::new();

        // Add a ledger
        let record = make_ledger_record("books:main", 5, 3);
        state.handle_ledger_record(&record).await;
        assert!(state.get_ledger("books:main").await.is_some());

        // Retract it
        state.handle_retracted("ledger", "books:main").await;
        assert!(state.get_ledger("books:main").await.is_none());
    }

    #[tokio::test]
    async fn test_clear() {
        let state = PeerState::new();

        // Add some state
        let ledger = make_ledger_record("books:main", 5, 3);
        let gs = make_graph_source_record("search:main", 2);
        state.handle_ledger_record(&ledger).await;
        state.handle_graph_source_record(&gs).await;
        state.set_snapshot_hash("abc123".to_string()).await;

        assert_eq!(state.ledger_count().await, 1);
        assert_eq!(state.graph_source_count().await, 1);
        assert!(state.is_snapshot_received().await);

        // Clear
        state.clear().await;

        assert_eq!(state.ledger_count().await, 0);
        assert_eq!(state.graph_source_count().await, 0);
        assert!(!state.is_snapshot_received().await);
    }

    #[tokio::test]
    async fn test_dirty_ledgers() {
        let state = PeerState::new();

        // Add two ledgers
        let record1 = make_ledger_record("books:main", 5, 3);
        let record2 = make_ledger_record("users:main", 2, 1);
        state.handle_ledger_record(&record1).await;
        state.handle_ledger_record(&record2).await;

        // Both dirty
        assert_eq!(state.dirty_ledgers().await.len(), 2);

        // Mark one clean
        state.mark_ledger_clean("books:main").await;
        assert_eq!(state.dirty_ledgers().await.len(), 1);
        assert_eq!(state.dirty_ledgers().await[0].ledger_id, "users:main");
    }
}
