//! Ledger Registry for Server-Layer Tracking
//!
//! This module provides a registry for tracking loaded ledgers and their watermarks.
//! The SSE endpoint does NOT load ledgers; the registry only updates watermarks
//! for already-loaded entries.
//!
//! ## Purpose
//!
//! - Track which ledgers are currently "active" in the server
//! - Maintain last access times for idle eviction
//! - Update commit_t/index_t watermarks based on nameservice events
//! - Support idle sweep to clean up unused entries

use fluree_db_nameservice::NameServiceEvent;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Handle representing a loaded ledger's tracking state
#[derive(Debug)]
pub struct LoadedLedgerHandle {
    /// The ledger ID (e.g., "mydb:main")
    pub ledger_id: String,

    /// Last time this ledger was accessed
    pub last_access: RwLock<Instant>,

    /// Last known commit_t from nameservice events
    pub last_commit_t: RwLock<i64>,

    /// Last known index_t from nameservice events
    pub last_index_t: RwLock<i64>,
}

impl LoadedLedgerHandle {
    /// Create a new handle for a ledger
    pub fn new(ledger_id: impl Into<String>) -> Self {
        Self {
            ledger_id: ledger_id.into(),
            last_access: RwLock::new(Instant::now()),
            last_commit_t: RwLock::new(0),
            last_index_t: RwLock::new(0),
        }
    }

    /// Update the last access time to now
    pub fn touch(&self) {
        if let Ok(mut guard) = self.last_access.write() {
            *guard = Instant::now();
        }
    }

    /// Check if this handle has been idle longer than the given duration
    pub fn is_idle(&self, ttl: Duration) -> bool {
        self.last_access
            .read()
            .map(|guard| guard.elapsed() > ttl)
            .unwrap_or(false)
    }

    /// Update commit_t if the new value is higher
    pub fn update_commit_t(&self, commit_t: i64) {
        if let Ok(mut current) = self.last_commit_t.write() {
            if commit_t > *current {
                *current = commit_t;
            }
        }
    }

    /// Update index_t if the new value is higher
    pub fn update_index_t(&self, index_t: i64) {
        if let Ok(mut current) = self.last_index_t.write() {
            if index_t > *current {
                *current = index_t;
            }
        }
    }

    /// Get the current commit_t
    pub fn commit_t(&self) -> i64 {
        self.last_commit_t.read().map(|g| *g).unwrap_or(0)
    }

    /// Get the current index_t
    pub fn index_t(&self) -> i64 {
        self.last_index_t.read().map(|g| *g).unwrap_or(0)
    }
}

/// Registry for tracking loaded ledgers
///
/// Provides a server-layer view of which ledgers are actively being used,
/// with support for idle eviction.
#[derive(Debug)]
pub struct LedgerRegistry {
    /// Map of ledger_id -> handle for tracked ledgers
    entries: RwLock<HashMap<String, Arc<LoadedLedgerHandle>>>,

    /// Time-to-live for idle entries before they can be swept
    idle_ttl: Duration,
}

impl LedgerRegistry {
    /// Create a new registry with the given idle TTL
    pub fn new(idle_ttl: Duration) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            idle_ttl,
        }
    }

    /// Get or create a handle for a ledger
    ///
    /// If the ledger is already tracked, returns the existing handle and touches it.
    /// Otherwise, creates a new handle.
    pub fn get_or_create(&self, ledger_id: &str) -> Arc<LoadedLedgerHandle> {
        // Fast path: read lock to check if exists
        if let Ok(entries) = self.entries.read() {
            if let Some(handle) = entries.get(ledger_id) {
                handle.touch();
                return handle.clone();
            }
        }

        // Slow path: write lock to create
        if let Ok(mut entries) = self.entries.write() {
            // Double-check after acquiring write lock
            if let Some(handle) = entries.get(ledger_id) {
                handle.touch();
                return handle.clone();
            }

            let handle = Arc::new(LoadedLedgerHandle::new(ledger_id));
            entries.insert(ledger_id.to_string(), handle.clone());
            return handle;
        }

        // Fallback: create new handle without storing (lock poisoned)
        Arc::new(LoadedLedgerHandle::new(ledger_id))
    }

    /// Touch a ledger to update its last access time
    ///
    /// No-op if the ledger is not tracked.
    pub fn touch(&self, ledger_id: &str) {
        if let Ok(entries) = self.entries.read() {
            if let Some(handle) = entries.get(ledger_id) {
                handle.touch();
            }
        }
    }

    /// Check if a ledger is currently being tracked
    pub fn is_tracked(&self, ledger_id: &str) -> bool {
        self.entries
            .read()
            .map(|e| e.contains_key(ledger_id))
            .unwrap_or(false)
    }

    /// Disconnect (remove) a ledger from tracking
    ///
    /// Returns the handle if it was tracked, None otherwise.
    pub fn disconnect(&self, ledger_id: &str) -> Option<Arc<LoadedLedgerHandle>> {
        self.entries.write().ok()?.remove(ledger_id)
    }

    /// Sweep idle entries and return the ledger IDs that were removed
    pub fn sweep_idle(&self) -> Vec<String> {
        let Ok(mut entries) = self.entries.write() else {
            return Vec::new();
        };
        let ttl = self.idle_ttl;

        let idle_ledger_ids: Vec<String> = entries
            .iter()
            .filter(|(_, handle)| handle.is_idle(ttl))
            .map(|(ledger_id, _)| ledger_id.clone())
            .collect();

        for ledger_id in &idle_ledger_ids {
            entries.remove(ledger_id);
        }

        idle_ledger_ids
    }

    /// Process a nameservice event to update watermarks
    ///
    /// Only updates tracked ledgers; does not create new entries.
    pub fn on_ns_event(&self, event: &NameServiceEvent) {
        match event {
            NameServiceEvent::LedgerCommitPublished {
                ledger_id,
                commit_t,
                ..
            } => {
                if let Ok(entries) = self.entries.read() {
                    if let Some(handle) = entries.get(ledger_id) {
                        handle.update_commit_t(*commit_t);
                    }
                }
            }
            NameServiceEvent::LedgerIndexPublished {
                ledger_id, index_t, ..
            } => {
                if let Ok(entries) = self.entries.read() {
                    if let Some(handle) = entries.get(ledger_id) {
                        handle.update_index_t(*index_t);
                    }
                }
            }
            NameServiceEvent::LedgerRetracted { ledger_id } => {
                // Remove retracted ledgers from tracking
                if let Ok(mut entries) = self.entries.write() {
                    entries.remove(ledger_id);
                }
            }
            // Graph source events are not tracked in the ledger registry
            _ => {}
        }
    }

    /// Get the number of tracked ledgers
    pub fn len(&self) -> usize {
        self.entries.read().map(|e| e.len()).unwrap_or(0)
    }

    /// Check if the registry is empty
    pub fn is_empty(&self) -> bool {
        self.entries.read().map(|e| e.is_empty()).unwrap_or(true)
    }

    /// Get all tracked ledger IDs
    pub fn ledger_ids(&self) -> Vec<String> {
        self.entries
            .read()
            .map(|e| e.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Spawn a background task that listens to nameservice events and feeds them to the registry.
    ///
    /// Also periodically sweeps idle entries based on the registry's TTL.
    /// Returns a handle that can be used to stop the task.
    ///
    /// # Arguments
    /// * `registry` - Arc to the registry to update
    /// * `receiver` - Broadcast receiver for nameservice events
    /// * `sweep_interval` - How often to sweep idle entries
    pub fn spawn_maintenance_task(
        registry: Arc<Self>,
        mut receiver: tokio::sync::broadcast::Receiver<NameServiceEvent>,
        sweep_interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut sweep_ticker = tokio::time::interval(sweep_interval);
            sweep_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    biased;

                    // Process nameservice events
                    result = receiver.recv() => {
                        match result {
                            Ok(event) => {
                                registry.on_ns_event(&event);
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                tracing::warn!(
                                    lagged = n,
                                    "Registry event receiver lagged, some watermarks may be stale"
                                );
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                tracing::info!("Registry maintenance task stopping: channel closed");
                                break;
                            }
                        }
                    }

                    // Periodic idle sweep
                    _ = sweep_ticker.tick() => {
                        let swept = registry.sweep_idle();
                        if !swept.is_empty() {
                            tracing::debug!(
                                count = swept.len(),
                                ledger_ids = ?swept,
                                "Swept idle ledger registry entries"
                            );
                        }
                    }
                }
            }
        })
    }
}

impl Default for LedgerRegistry {
    fn default() -> Self {
        // Default idle TTL of 5 minutes
        Self::new(Duration::from_secs(300))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{ContentId, ContentKind};

    #[test]
    fn test_loaded_ledger_handle_new() {
        let handle = LoadedLedgerHandle::new("test:main");
        assert_eq!(handle.ledger_id, "test:main");
        assert_eq!(handle.commit_t(), 0);
        assert_eq!(handle.index_t(), 0);
    }

    #[test]
    fn test_loaded_ledger_handle_update_watermarks() {
        let handle = LoadedLedgerHandle::new("test:main");

        handle.update_commit_t(10);
        assert_eq!(handle.commit_t(), 10);

        // Lower value should be ignored
        handle.update_commit_t(5);
        assert_eq!(handle.commit_t(), 10);

        // Higher value should update
        handle.update_commit_t(20);
        assert_eq!(handle.commit_t(), 20);

        // Same for index_t
        handle.update_index_t(8);
        assert_eq!(handle.index_t(), 8);
    }

    #[test]
    fn test_registry_get_or_create() {
        let registry = LedgerRegistry::new(Duration::from_secs(60));

        let handle1 = registry.get_or_create("test:main");
        let handle2 = registry.get_or_create("test:main");

        // Should be the same handle
        assert!(Arc::ptr_eq(&handle1, &handle2));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_registry_disconnect() {
        let registry = LedgerRegistry::new(Duration::from_secs(60));

        registry.get_or_create("test:main");
        assert!(registry.is_tracked("test:main"));

        let handle = registry.disconnect("test:main");
        assert!(handle.is_some());
        assert!(!registry.is_tracked("test:main"));

        // Disconnect again should return None
        let handle2 = registry.disconnect("test:main");
        assert!(handle2.is_none());
    }

    #[test]
    fn test_registry_on_ns_event() {
        let registry = LedgerRegistry::new(Duration::from_secs(60));

        // Create a tracked ledger
        let handle = registry.get_or_create("test:main");
        assert_eq!(handle.commit_t(), 0);

        // Process commit event
        registry.on_ns_event(&NameServiceEvent::LedgerCommitPublished {
            ledger_id: "test:main".to_string(),
            commit_id: ContentId::new(ContentKind::Commit, b"test-commit"),
            commit_t: 42,
        });
        assert_eq!(handle.commit_t(), 42);

        // Process index event
        registry.on_ns_event(&NameServiceEvent::LedgerIndexPublished {
            ledger_id: "test:main".to_string(),
            index_id: ContentId::new(ContentKind::IndexRoot, b"test-index"),
            index_t: 40,
        });
        assert_eq!(handle.index_t(), 40);

        // Untracked ledger should be ignored
        registry.on_ns_event(&NameServiceEvent::LedgerCommitPublished {
            ledger_id: "other:main".to_string(),
            commit_id: ContentId::new(ContentKind::Commit, b"other-commit"),
            commit_t: 100,
        });
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_registry_retraction_removes_entry() {
        let registry = LedgerRegistry::new(Duration::from_secs(60));

        registry.get_or_create("test:main");
        assert!(registry.is_tracked("test:main"));

        registry.on_ns_event(&NameServiceEvent::LedgerRetracted {
            ledger_id: "test:main".to_string(),
        });
        assert!(!registry.is_tracked("test:main"));
    }

    #[test]
    fn test_registry_sweep_idle() {
        let registry = LedgerRegistry::new(Duration::from_millis(1));

        registry.get_or_create("test:main");
        registry.get_or_create("test:dev");

        // Wait for entries to become idle
        std::thread::sleep(Duration::from_millis(10));

        let swept = registry.sweep_idle();
        assert_eq!(swept.len(), 2);
        assert!(registry.is_empty());
    }
}
