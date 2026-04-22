//! Remote tracking state for nameservice sync
//!
//! This module provides the types and traits for storing "what the remote has" locally,
//! analogous to git's `refs/remotes/origin/*`. Tracking records live outside the `ns@v2/`
//! tree (at `{base}/ns-sync/remotes/...`) so that `all_records()` and `list_prefix("ns@v2/")`
//! never enumerate sync metadata.

use crate::{RefValue, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// A named remote (analogous to git remote names like "origin").
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RemoteName(pub String);

impl RemoteName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RemoteName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A single tracking record for one ledger ID on one remote.
///
/// Contains both commit and index refs (one file per ledger ID on disk).
/// Versioned so the on-disk format can evolve without breaking existing state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrackingRecord {
    /// Schema version for forward compatibility (currently 1).
    pub schema_version: u32,

    /// The remote this record belongs to.
    pub remote: RemoteName,

    /// The ledger ID (e.g., "mydb:main").
    pub ledger_id: String,

    /// The remote's commit head (if known).
    pub commit_ref: Option<RefValue>,

    /// The remote's index head (if known).
    pub index_ref: Option<RefValue>,

    /// Whether the ledger has been retracted on the remote.
    pub retracted: bool,

    /// When this record was last fetched (RFC 3339 timestamp).
    pub last_fetched: Option<String>,
}

impl TrackingRecord {
    /// Create a new tracking record with sensible defaults.
    pub fn new(remote: RemoteName, ledger_id: impl Into<String>) -> Self {
        Self {
            schema_version: 1,
            remote,
            ledger_id: ledger_id.into(),
            commit_ref: None,
            index_ref: None,
            retracted: false,
            last_fetched: None,
        }
    }
}

/// Store for remote tracking state.
///
/// Each implementation stores tracking records keyed by `(remote, ledger_id)`.
/// File-based stores use `{base}/ns-sync/remotes/{remote}/{address_encoded}.json`.
#[async_trait]
pub trait RemoteTrackingStore: Debug + Send + Sync {
    /// Get the tracking record for a specific remote + ledger ID.
    ///
    /// Returns `None` if no tracking record exists.
    async fn get_tracking(
        &self,
        remote: &RemoteName,
        ledger_id: &str,
    ) -> Result<Option<TrackingRecord>>;

    /// Store (create or update) a tracking record.
    async fn set_tracking(&self, record: &TrackingRecord) -> Result<()>;

    /// List all tracking records for a given remote.
    async fn list_tracking(&self, remote: &RemoteName) -> Result<Vec<TrackingRecord>>;

    /// Remove a tracking record.
    async fn remove_tracking(&self, remote: &RemoteName, ledger_id: &str) -> Result<()>;
}

/// In-memory implementation of [`RemoteTrackingStore`] for testing.
#[derive(Debug)]
pub struct MemoryTrackingStore {
    records: parking_lot::RwLock<std::collections::HashMap<(String, String), TrackingRecord>>,
}

impl Default for MemoryTrackingStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryTrackingStore {
    pub fn new() -> Self {
        Self {
            records: parking_lot::RwLock::new(std::collections::HashMap::new()),
        }
    }

    fn make_key(remote: &RemoteName, ledger_id: &str) -> (String, String) {
        (remote.0.clone(), ledger_id.to_string())
    }
}

#[async_trait]
impl RemoteTrackingStore for MemoryTrackingStore {
    async fn get_tracking(
        &self,
        remote: &RemoteName,
        ledger_id: &str,
    ) -> Result<Option<TrackingRecord>> {
        let key = Self::make_key(remote, ledger_id);
        Ok(self.records.read().get(&key).cloned())
    }

    async fn set_tracking(&self, record: &TrackingRecord) -> Result<()> {
        let key = Self::make_key(&record.remote, &record.ledger_id);
        self.records.write().insert(key, record.clone());
        Ok(())
    }

    async fn list_tracking(&self, remote: &RemoteName) -> Result<Vec<TrackingRecord>> {
        let records = self.records.read();
        Ok(records
            .iter()
            .filter(|((r, _), _)| r == &remote.0)
            .map(|(_, v)| v.clone())
            .collect())
    }

    async fn remove_tracking(&self, remote: &RemoteName, ledger_id: &str) -> Result<()> {
        let key = Self::make_key(remote, ledger_id);
        self.records.write().remove(&key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RefValue;

    fn origin() -> RemoteName {
        RemoteName::new("origin")
    }

    fn upstream() -> RemoteName {
        RemoteName::new("upstream")
    }

    #[tokio::test]
    async fn test_get_tracking_empty() {
        let store = MemoryTrackingStore::new();
        let result = store.get_tracking(&origin(), "mydb:main").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_set_and_get_tracking() {
        let store = MemoryTrackingStore::new();
        let mut record = TrackingRecord::new(origin(), "mydb:main");
        record.commit_ref = Some(RefValue { id: None, t: 5 });
        record.last_fetched = Some("2025-01-01T00:00:00Z".to_string());

        store.set_tracking(&record).await.unwrap();

        let fetched = store
            .get_tracking(&origin(), "mydb:main")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.ledger_id, "mydb:main");
        assert_eq!(fetched.schema_version, 1);
        assert_eq!(fetched.commit_ref.as_ref().unwrap().t, 5);
        assert_eq!(
            fetched.last_fetched.as_deref(),
            Some("2025-01-01T00:00:00Z")
        );
    }

    #[tokio::test]
    async fn test_set_tracking_overwrites() {
        let store = MemoryTrackingStore::new();

        let mut record = TrackingRecord::new(origin(), "mydb:main");
        record.commit_ref = Some(RefValue { id: None, t: 1 });
        store.set_tracking(&record).await.unwrap();

        // Overwrite with newer data
        record.commit_ref = Some(RefValue { id: None, t: 5 });
        store.set_tracking(&record).await.unwrap();

        let fetched = store
            .get_tracking(&origin(), "mydb:main")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.commit_ref.as_ref().unwrap().t, 5);
    }

    #[tokio::test]
    async fn test_list_tracking_by_remote() {
        let store = MemoryTrackingStore::new();

        store
            .set_tracking(&TrackingRecord::new(origin(), "db1:main"))
            .await
            .unwrap();
        store
            .set_tracking(&TrackingRecord::new(origin(), "db2:main"))
            .await
            .unwrap();
        store
            .set_tracking(&TrackingRecord::new(upstream(), "db3:main"))
            .await
            .unwrap();

        let origin_records = store.list_tracking(&origin()).await.unwrap();
        assert_eq!(origin_records.len(), 2);

        let upstream_records = store.list_tracking(&upstream()).await.unwrap();
        assert_eq!(upstream_records.len(), 1);
        assert_eq!(upstream_records[0].ledger_id, "db3:main");
    }

    #[tokio::test]
    async fn test_remove_tracking() {
        let store = MemoryTrackingStore::new();

        store
            .set_tracking(&TrackingRecord::new(origin(), "mydb:main"))
            .await
            .unwrap();
        assert!(store
            .get_tracking(&origin(), "mydb:main")
            .await
            .unwrap()
            .is_some());

        store.remove_tracking(&origin(), "mydb:main").await.unwrap();
        assert!(store
            .get_tracking(&origin(), "mydb:main")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_remove_nonexistent_is_noop() {
        let store = MemoryTrackingStore::new();
        // Should not error
        store
            .remove_tracking(&origin(), "nonexistent:main")
            .await
            .unwrap();
    }

    #[test]
    fn test_tracking_record_serde_roundtrip() {
        let mut record = TrackingRecord::new(origin(), "mydb:main");
        record.commit_ref = Some(RefValue { id: None, t: 5 });
        record.index_ref = Some(RefValue { id: None, t: 3 });
        record.retracted = false;
        record.last_fetched = Some("2025-06-15T12:00:00Z".to_string());

        let json = serde_json::to_string_pretty(&record).unwrap();
        let deserialized: TrackingRecord = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.schema_version, 1);
        assert_eq!(deserialized.remote, origin());
        assert_eq!(deserialized.ledger_id, "mydb:main");
        assert_eq!(deserialized.commit_ref.as_ref().unwrap().t, 5);
        assert_eq!(deserialized.index_ref.as_ref().unwrap().t, 3);
        assert!(!deserialized.retracted);
    }

    #[test]
    fn test_remote_name_display() {
        let name = RemoteName::new("origin");
        assert_eq!(format!("{name}"), "origin");
        assert_eq!(name.as_str(), "origin");
    }
}
