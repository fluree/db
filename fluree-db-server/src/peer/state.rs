//! Peer state tracking for remote watermarks
//!
//! Tracks what the transaction server has committed/indexed via SSE events.
//! This is separate from local ledger state which may lag behind.

use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::RwLock;

/// Remote watermark state for a ledger (from SSE)
/// This is what the transaction server has, not what the peer has locally.
#[derive(Debug, Clone)]
pub struct RemoteLedgerWatermark {
    pub ledger_id: String,
    pub commit_t: i64,
    pub index_t: i64,
    pub commit_head_id: Option<String>,
    pub index_head_id: Option<String>,
    pub last_updated: Instant,
}

/// Remote watermark state for a graph source (from SSE)
#[derive(Debug, Clone)]
pub struct RemoteGraphSourceWatermark {
    pub graph_source_id: String,
    pub index_t: i64,
    pub config_hash: String,
    pub index_id: Option<String>,
    pub last_updated: Instant,
}

/// Peer state tracking remote watermarks from SSE subscription
///
/// This tracks what the transaction server has committed/indexed.
/// The peer's local ledger state may lag behind these watermarks.
pub struct PeerState {
    /// Remote ledger watermarks from SSE
    ledgers: RwLock<HashMap<String, RemoteLedgerWatermark>>,
    /// Remote graph source watermarks from SSE
    graph_sources: RwLock<HashMap<String, RemoteGraphSourceWatermark>>,
    /// Whether SSE connection is active
    connected: RwLock<bool>,
}

impl PeerState {
    pub fn new() -> Self {
        Self {
            ledgers: RwLock::new(HashMap::new()),
            graph_sources: RwLock::new(HashMap::new()),
            connected: RwLock::new(false),
        }
    }

    /// Check if a ledger needs refresh based on local vs remote watermarks.
    ///
    /// Returns `NeedsRefresh::Yes` if the remote index_t is ahead of local.
    /// Returns `NeedsRefresh::Unknown` if we haven't seen this ledger in SSE yet.
    /// Returns `NeedsRefresh::No` if local is up-to-date.
    ///
    /// IMPORTANT: `NeedsRefresh::Unknown` means the peer hasn't received SSE state
    /// for this ledger yet. The caller should decide policy: either proceed with
    /// local state (if available) or reject the query.
    pub async fn check_ledger_freshness(
        &self,
        ledger_id: &str,
        local_index_t: i64,
    ) -> NeedsRefresh {
        match self.ledgers.read().await.get(ledger_id) {
            Some(remote) => {
                if remote.index_t > local_index_t {
                    NeedsRefresh::Yes {
                        local_index_t,
                        remote_index_t: remote.index_t,
                        remote_index_id: remote.index_head_id.clone(),
                    }
                } else {
                    NeedsRefresh::No
                }
            }
            None => NeedsRefresh::Unknown,
        }
    }

    /// Check if a graph source needs refresh based on local vs remote state.
    pub async fn check_graph_source_freshness(
        &self,
        graph_source_id: &str,
        local_index_t: i64,
        local_config_hash: &str,
    ) -> GraphSourceNeedsRefresh {
        match self.graph_sources.read().await.get(graph_source_id) {
            Some(remote) => {
                if remote.index_t > local_index_t {
                    GraphSourceNeedsRefresh::IndexAdvanced {
                        remote_index_t: remote.index_t,
                    }
                } else if remote.config_hash != local_config_hash {
                    GraphSourceNeedsRefresh::ConfigChanged {
                        remote_config_hash: remote.config_hash.clone(),
                    }
                } else {
                    GraphSourceNeedsRefresh::No
                }
            }
            None => GraphSourceNeedsRefresh::Unknown,
        }
    }

    /// Get remote watermark for a ledger (if known from SSE)
    pub async fn get_remote_ledger(&self, ledger_id: &str) -> Option<RemoteLedgerWatermark> {
        self.ledgers.read().await.get(ledger_id).cloned()
    }

    /// Update ledger watermark from SSE event (returns true if changed)
    pub async fn update_ledger(
        &self,
        ledger_id: &str,
        commit_t: i64,
        index_t: i64,
        commit_head_id: Option<String>,
        index_head_id: Option<String>,
    ) -> bool {
        let mut ledgers = self.ledgers.write().await;

        let changed = match ledgers.get(ledger_id) {
            Some(existing) => commit_t > existing.commit_t || index_t > existing.index_t,
            None => true,
        };

        if changed {
            ledgers.insert(
                ledger_id.to_string(),
                RemoteLedgerWatermark {
                    ledger_id: ledger_id.to_string(),
                    commit_t,
                    index_t,
                    commit_head_id,
                    index_head_id,
                    last_updated: Instant::now(),
                },
            );
        }

        changed
    }

    /// Remove ledger (on retraction)
    pub async fn remove_ledger(&self, ledger_id: &str) {
        self.ledgers.write().await.remove(ledger_id);
    }

    /// Update graph source watermark from SSE event (returns true if changed)
    pub async fn update_graph_source(
        &self,
        graph_source_id: &str,
        index_t: i64,
        config_hash: String,
        index_id: Option<String>,
    ) -> bool {
        let mut graph_sources = self.graph_sources.write().await;

        let changed = match graph_sources.get(graph_source_id) {
            Some(existing) => index_t > existing.index_t || config_hash != existing.config_hash,
            None => true,
        };

        if changed {
            graph_sources.insert(
                graph_source_id.to_string(),
                RemoteGraphSourceWatermark {
                    graph_source_id: graph_source_id.to_string(),
                    index_t,
                    config_hash,
                    index_id,
                    last_updated: Instant::now(),
                },
            );
        }

        changed
    }

    /// Remove graph source (on retraction)
    pub async fn remove_graph_source(&self, graph_source_id: &str) {
        self.graph_sources.write().await.remove(graph_source_id);
    }

    /// Clear all state (on reconnect, before new snapshot)
    pub async fn clear(&self) {
        self.ledgers.write().await.clear();
        self.graph_sources.write().await.clear();
    }

    /// Mark connected/disconnected
    pub async fn set_connected(&self, connected: bool) {
        *self.connected.write().await = connected;
    }

    pub async fn is_connected(&self) -> bool {
        *self.connected.read().await
    }

    /// Get all known ledger aliases (for introspection/health)
    pub async fn known_ledgers(&self) -> Vec<String> {
        self.ledgers.read().await.keys().cloned().collect()
    }

    /// Get all known graph source aliases (for introspection/health)
    pub async fn known_graph_sources(&self) -> Vec<String> {
        self.graph_sources.read().await.keys().cloned().collect()
    }

    /// Get ledger count
    pub async fn ledger_count(&self) -> usize {
        self.ledgers.read().await.len()
    }

    /// Get graph source count
    pub async fn graph_source_count(&self) -> usize {
        self.graph_sources.read().await.len()
    }
}

impl Default for PeerState {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of checking if a ledger needs refresh
#[derive(Debug, Clone)]
pub enum NeedsRefresh {
    /// Local state is up-to-date with remote
    No,
    /// Remote has newer index, refresh needed
    Yes {
        local_index_t: i64,
        remote_index_t: i64,
        remote_index_id: Option<String>,
    },
    /// SSE hasn't reported this ledger yet
    /// Caller must decide: proceed with local state or reject query
    Unknown,
}

/// Result of checking if a graph source needs refresh
#[derive(Debug, Clone)]
pub enum GraphSourceNeedsRefresh {
    /// Local state is up-to-date
    No,
    /// Remote has newer index
    IndexAdvanced { remote_index_t: i64 },
    /// Config changed (requires rebuild)
    ConfigChanged { remote_config_hash: String },
    /// SSE hasn't reported this graph source yet
    Unknown,
}

// Implement FreshnessSource trait from fluree-db-api
impl fluree_db_api::FreshnessSource for PeerState {
    /// Get remote watermark for freshness comparison
    ///
    /// Uses try_read() to avoid blocking. Returns None if:
    /// - The lock is contended (rare, SSE updates are infrequent)
    /// - The ledger hasn't been seen in SSE yet
    ///
    /// When None is returned, the caller uses lenient policy (treat as current).
    fn watermark(&self, ledger_id: &str) -> Option<fluree_db_api::RemoteWatermark> {
        // Try to read without blocking
        let ledgers = self.ledgers.try_read().ok()?;
        let w = ledgers.get(ledger_id)?;
        Some(fluree_db_api::RemoteWatermark {
            commit_t: w.commit_t,
            index_t: w.index_t,
            index_head_id: w
                .index_head_id
                .as_deref()
                .and_then(|s| s.parse::<fluree_db_core::ContentId>().ok()),
            updated_at: w.last_updated,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_new_ledger_update() {
        let state = PeerState::new();

        let changed = state
            .update_ledger(
                "books:main",
                5,
                3,
                Some("commit-cid:5".to_string()),
                Some("index-cid:3".to_string()),
            )
            .await;

        assert!(changed);

        let ledger = state.get_remote_ledger("books:main").await.unwrap();
        assert_eq!(ledger.commit_t, 5);
        assert_eq!(ledger.index_t, 3);
    }

    #[tokio::test]
    async fn test_ledger_update_no_change() {
        let state = PeerState::new();

        // Initial update
        state.update_ledger("books:main", 5, 3, None, None).await;

        // Same watermarks - no change
        let changed = state.update_ledger("books:main", 5, 3, None, None).await;
        assert!(!changed);
    }

    #[tokio::test]
    async fn test_ledger_update_commit_advanced() {
        let state = PeerState::new();

        state.update_ledger("books:main", 5, 3, None, None).await;

        // Higher commit_t
        let changed = state.update_ledger("books:main", 6, 3, None, None).await;
        assert!(changed);

        let ledger = state.get_remote_ledger("books:main").await.unwrap();
        assert_eq!(ledger.commit_t, 6);
    }

    #[tokio::test]
    async fn test_ledger_update_index_advanced() {
        let state = PeerState::new();

        state.update_ledger("books:main", 5, 3, None, None).await;

        // Higher index_t
        let changed = state.update_ledger("books:main", 5, 5, None, None).await;
        assert!(changed);

        let ledger = state.get_remote_ledger("books:main").await.unwrap();
        assert_eq!(ledger.index_t, 5);
    }

    #[tokio::test]
    async fn test_check_ledger_freshness_no() {
        let state = PeerState::new();
        state.update_ledger("books:main", 5, 3, None, None).await;

        let result = state.check_ledger_freshness("books:main", 3).await;
        assert!(matches!(result, NeedsRefresh::No));
    }

    #[tokio::test]
    async fn test_check_ledger_freshness_yes() {
        let state = PeerState::new();
        state.update_ledger("books:main", 5, 5, None, None).await;

        let result = state.check_ledger_freshness("books:main", 3).await;
        match result {
            NeedsRefresh::Yes {
                local_index_t,
                remote_index_t,
                ..
            } => {
                assert_eq!(local_index_t, 3);
                assert_eq!(remote_index_t, 5);
            }
            _ => panic!("Expected NeedsRefresh::Yes"),
        }
    }

    #[tokio::test]
    async fn test_check_ledger_freshness_unknown() {
        let state = PeerState::new();

        let result = state.check_ledger_freshness("unknown:main", 0).await;
        assert!(matches!(result, NeedsRefresh::Unknown));
    }

    #[tokio::test]
    async fn test_remove_ledger() {
        let state = PeerState::new();
        state.update_ledger("books:main", 5, 3, None, None).await;
        assert!(state.get_remote_ledger("books:main").await.is_some());

        state.remove_ledger("books:main").await;
        assert!(state.get_remote_ledger("books:main").await.is_none());
    }

    #[tokio::test]
    async fn test_clear() {
        let state = PeerState::new();
        state.update_ledger("books:main", 5, 3, None, None).await;
        state
            .update_graph_source("search:main", 2, "abc123".to_string(), None)
            .await;

        assert_eq!(state.ledger_count().await, 1);
        assert_eq!(state.graph_source_count().await, 1);

        state.clear().await;

        assert_eq!(state.ledger_count().await, 0);
        assert_eq!(state.graph_source_count().await, 0);
    }

    #[tokio::test]
    async fn test_graph_source_freshness_no() {
        let state = PeerState::new();
        state
            .update_graph_source("search:main", 2, "abc123".to_string(), None)
            .await;

        let result = state
            .check_graph_source_freshness("search:main", 2, "abc123")
            .await;
        assert!(matches!(result, GraphSourceNeedsRefresh::No));
    }

    #[tokio::test]
    async fn test_graph_source_freshness_index_advanced() {
        let state = PeerState::new();
        state
            .update_graph_source("search:main", 5, "abc123".to_string(), None)
            .await;

        let result = state
            .check_graph_source_freshness("search:main", 2, "abc123")
            .await;
        match result {
            GraphSourceNeedsRefresh::IndexAdvanced { remote_index_t } => {
                assert_eq!(remote_index_t, 5);
            }
            _ => panic!("Expected GraphSourceNeedsRefresh::IndexAdvanced"),
        }
    }

    #[tokio::test]
    async fn test_graph_source_freshness_config_changed() {
        let state = PeerState::new();
        state
            .update_graph_source("search:main", 2, "def456".to_string(), None)
            .await;

        let result = state
            .check_graph_source_freshness("search:main", 2, "abc123")
            .await;
        match result {
            GraphSourceNeedsRefresh::ConfigChanged { remote_config_hash } => {
                assert_eq!(remote_config_hash, "def456");
            }
            _ => panic!("Expected GraphSourceNeedsRefresh::ConfigChanged"),
        }
    }

    #[tokio::test]
    async fn test_connected_state() {
        let state = PeerState::new();
        assert!(!state.is_connected().await);

        state.set_connected(true).await;
        assert!(state.is_connected().await);

        state.set_connected(false).await;
        assert!(!state.is_connected().await);
    }
}
