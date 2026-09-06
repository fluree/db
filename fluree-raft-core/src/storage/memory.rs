//! In-memory [`RaftStorage`] backend.
//!
//! Ephemeral state held under `RwLock`s — nothing survives a process
//! restart. Useful for tests and single-process demos where durability
//! across restarts doesn't matter.

use super::{
    LogEntry, LogId, LogState, RaftLogStore, RaftSnapshotStore, RaftStorage, SnapshotId,
    SnapshotMeta, StorageError, Vote,
};
use async_trait::async_trait;
use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::RwLock;

#[derive(Default)]
struct LogStoreState {
    entries: BTreeMap<u64, LogEntry>,
    vote: Option<Vote>,
    committed: Option<LogId>,
    last_purged: Option<LogId>,
}

/// In-memory implementation of [`RaftLogStore`].
#[derive(Default)]
pub struct MemoryRaftLogStore {
    state: RwLock<LogStoreState>,
}

impl MemoryRaftLogStore {
    pub fn new() -> Self {
        Self::default()
    }
}

fn lock_poisoned(label: &'static str) -> StorageError {
    StorageError::other(format!("{label} lock poisoned"))
}

#[async_trait]
impl RaftLogStore for MemoryRaftLogStore {
    async fn append(&self, entries: &[LogEntry]) -> Result<(), StorageError> {
        let mut state = self.state.write().map_err(|_| lock_poisoned("log store"))?;
        for entry in entries {
            state.entries.insert(entry.log_id.index, entry.clone());
        }
        Ok(())
    }

    async fn read_range(&self, range: Range<u64>) -> Result<Vec<LogEntry>, StorageError> {
        let state = self.state.read().map_err(|_| lock_poisoned("log store"))?;
        Ok(state.entries.range(range).map(|(_, e)| e.clone()).collect())
    }

    async fn truncate_from(&self, from_index: u64) -> Result<(), StorageError> {
        let mut state = self.state.write().map_err(|_| lock_poisoned("log store"))?;
        state.entries.retain(|&k, _| k < from_index);
        Ok(())
    }

    async fn purge_through(&self, log_id: LogId) -> Result<(), StorageError> {
        let mut state = self.state.write().map_err(|_| lock_poisoned("log store"))?;
        state.entries.retain(|&k, _| k > log_id.index);
        match state.last_purged {
            Some(existing) if existing.index >= log_id.index => {}
            _ => state.last_purged = Some(log_id),
        }
        Ok(())
    }

    async fn log_state(&self) -> Result<LogState, StorageError> {
        let state = self.state.read().map_err(|_| lock_poisoned("log store"))?;
        let last_log = state.entries.values().next_back().map(|e| e.log_id);
        Ok(LogState {
            last_purged: state.last_purged,
            last_log,
        })
    }

    async fn save_vote(&self, vote: &Vote) -> Result<(), StorageError> {
        let mut state = self.state.write().map_err(|_| lock_poisoned("log store"))?;
        state.vote = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(&self) -> Result<Option<Vote>, StorageError> {
        let state = self.state.read().map_err(|_| lock_poisoned("log store"))?;
        Ok(state.vote.clone())
    }

    async fn save_committed(&self, log_id: Option<LogId>) -> Result<(), StorageError> {
        let mut state = self.state.write().map_err(|_| lock_poisoned("log store"))?;
        state.committed = log_id;
        Ok(())
    }

    async fn read_committed(&self) -> Result<Option<LogId>, StorageError> {
        let state = self.state.read().map_err(|_| lock_poisoned("log store"))?;
        Ok(state.committed)
    }
}

#[derive(Default)]
struct SnapshotStoreState {
    snapshots: HashMap<SnapshotId, (SnapshotMeta, Vec<u8>)>,
    current: Option<SnapshotId>,
}

/// In-memory implementation of [`RaftSnapshotStore`].
#[derive(Default)]
pub struct MemoryRaftSnapshotStore {
    state: RwLock<SnapshotStoreState>,
}

impl MemoryRaftSnapshotStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl RaftSnapshotStore for MemoryRaftSnapshotStore {
    async fn write(&self, meta: &SnapshotMeta, data: Vec<u8>) -> Result<(), StorageError> {
        let mut state = self
            .state
            .write()
            .map_err(|_| lock_poisoned("snapshot store"))?;
        state
            .snapshots
            .insert(meta.id.clone(), (meta.clone(), data));
        state.current = Some(meta.id.clone());
        Ok(())
    }

    async fn read(&self, id: &SnapshotId) -> Result<Option<Vec<u8>>, StorageError> {
        let state = self
            .state
            .read()
            .map_err(|_| lock_poisoned("snapshot store"))?;
        Ok(state.snapshots.get(id).map(|(_, data)| data.clone()))
    }

    async fn current(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, StorageError> {
        let state = self
            .state
            .read()
            .map_err(|_| lock_poisoned("snapshot store"))?;
        Ok(state
            .current
            .as_ref()
            .and_then(|id| state.snapshots.get(id).cloned()))
    }
}

/// Combined in-memory [`RaftStorage`].
#[derive(Default)]
pub struct MemoryRaftStorage {
    log: MemoryRaftLogStore,
    snapshots: MemoryRaftSnapshotStore,
}

impl MemoryRaftStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

impl RaftStorage for MemoryRaftStorage {
    type LogStore = MemoryRaftLogStore;
    type SnapshotStore = MemoryRaftSnapshotStore;

    fn log(&self) -> &Self::LogStore {
        &self.log
    }

    fn snapshots(&self) -> &Self::SnapshotStore {
        &self.snapshots
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(term: u64, index: u64) -> LogEntry {
        LogEntry {
            log_id: LogId::new(term, index),
            payload: format!("entry-{term}-{index}").into_bytes(),
        }
    }

    #[tokio::test]
    async fn append_and_read_full_range() {
        let store = MemoryRaftLogStore::new();
        store
            .append(&[entry(1, 1), entry(1, 2), entry(1, 3)])
            .await
            .unwrap();

        let got = store.read_range(1..4).await.unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].log_id, LogId::new(1, 1));
        assert_eq!(got[2].log_id, LogId::new(1, 3));
    }

    #[tokio::test]
    async fn read_range_returns_subset() {
        let store = MemoryRaftLogStore::new();
        store
            .append(&[entry(1, 1), entry(1, 2), entry(1, 3), entry(1, 4)])
            .await
            .unwrap();

        let got = store.read_range(2..4).await.unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].log_id.index, 2);
        assert_eq!(got[1].log_id.index, 3);
    }

    #[tokio::test]
    async fn read_range_past_end_is_partial() {
        let store = MemoryRaftLogStore::new();
        store.append(&[entry(1, 1), entry(1, 2)]).await.unwrap();

        let got = store.read_range(1..100).await.unwrap();
        assert_eq!(got.len(), 2);
    }

    #[tokio::test]
    async fn truncate_removes_suffix() {
        let store = MemoryRaftLogStore::new();
        store
            .append(&[entry(1, 1), entry(1, 2), entry(1, 3), entry(1, 4)])
            .await
            .unwrap();
        store.truncate_from(3).await.unwrap();

        let got = store.read_range(0..100).await.unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[1].log_id.index, 2);
    }

    #[tokio::test]
    async fn truncate_past_end_is_noop() {
        let store = MemoryRaftLogStore::new();
        store.append(&[entry(1, 1), entry(1, 2)]).await.unwrap();
        store.truncate_from(100).await.unwrap();

        let got = store.read_range(0..100).await.unwrap();
        assert_eq!(got.len(), 2);
    }

    #[tokio::test]
    async fn purge_removes_prefix_and_records_last_purged() {
        let store = MemoryRaftLogStore::new();
        store
            .append(&[entry(1, 1), entry(1, 2), entry(1, 3), entry(1, 4)])
            .await
            .unwrap();
        store.purge_through(LogId::new(1, 2)).await.unwrap();

        let got = store.read_range(0..100).await.unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].log_id.index, 3);

        let state = store.log_state().await.unwrap();
        assert_eq!(state.last_purged, Some(LogId::new(1, 2)));
        assert_eq!(state.last_log, Some(LogId::new(1, 4)));
    }

    #[tokio::test]
    async fn purge_idempotent_when_already_past() {
        let store = MemoryRaftLogStore::new();
        store
            .append(&[entry(1, 1), entry(1, 2), entry(1, 3)])
            .await
            .unwrap();
        store.purge_through(LogId::new(1, 2)).await.unwrap();
        // Older purge should be a no-op; last_purged stays at the higher mark.
        store.purge_through(LogId::new(1, 1)).await.unwrap();

        let state = store.log_state().await.unwrap();
        assert_eq!(state.last_purged, Some(LogId::new(1, 2)));
    }

    #[tokio::test]
    async fn log_state_empty_log() {
        let store = MemoryRaftLogStore::new();
        let state = store.log_state().await.unwrap();
        assert_eq!(state, LogState::default());
    }

    #[tokio::test]
    async fn vote_round_trip() {
        let store = MemoryRaftLogStore::new();
        assert!(store.read_vote().await.unwrap().is_none());

        let vote = Vote {
            term: 5,
            candidate: 42,
            committed: false,
        };
        store.save_vote(&vote).await.unwrap();

        assert_eq!(store.read_vote().await.unwrap(), Some(vote));
    }

    #[tokio::test]
    async fn committed_round_trip() {
        let store = MemoryRaftLogStore::new();
        assert!(store.read_committed().await.unwrap().is_none());

        store.save_committed(Some(LogId::new(3, 10))).await.unwrap();
        assert_eq!(
            store.read_committed().await.unwrap(),
            Some(LogId::new(3, 10))
        );

        store.save_committed(None).await.unwrap();
        assert!(store.read_committed().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn snapshot_write_current_and_read() {
        let store = MemoryRaftSnapshotStore::new();
        assert!(store.current().await.unwrap().is_none());

        let meta = SnapshotMeta {
            id: SnapshotId::new("snap-1"),
            last_applied: Some(LogId::new(1, 5)),
            membership: vec![1, 2, 3],
        };
        let data = vec![10, 20, 30];
        store.write(&meta, data.clone()).await.unwrap();

        let (current_meta, current_data) = store.current().await.unwrap().unwrap();
        assert_eq!(current_meta, meta);
        assert_eq!(current_data, data);

        assert_eq!(
            store.read(&SnapshotId::new("snap-1")).await.unwrap(),
            Some(data)
        );
        assert!(store
            .read(&SnapshotId::new("missing"))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn snapshot_current_tracks_latest_write() {
        let store = MemoryRaftSnapshotStore::new();
        let meta1 = SnapshotMeta {
            id: SnapshotId::new("snap-1"),
            last_applied: Some(LogId::new(1, 5)),
            membership: vec![],
        };
        let meta2 = SnapshotMeta {
            id: SnapshotId::new("snap-2"),
            last_applied: Some(LogId::new(1, 10)),
            membership: vec![],
        };
        store.write(&meta1, vec![1]).await.unwrap();
        store.write(&meta2, vec![2]).await.unwrap();

        let (current_meta, current_data) = store.current().await.unwrap().unwrap();
        assert_eq!(current_meta.id, SnapshotId::new("snap-2"));
        assert_eq!(current_data, vec![2]);

        // The older snapshot is still readable by id.
        assert_eq!(
            store.read(&SnapshotId::new("snap-1")).await.unwrap(),
            Some(vec![1])
        );
    }

    #[tokio::test]
    async fn combined_storage_routes_to_both_stores() {
        let storage = MemoryRaftStorage::new();
        storage.log().append(&[entry(1, 1)]).await.unwrap();

        let meta = SnapshotMeta {
            id: SnapshotId::new("snap-1"),
            last_applied: Some(LogId::new(1, 1)),
            membership: vec![],
        };
        storage.snapshots().write(&meta, vec![1, 2]).await.unwrap();

        assert_eq!(
            storage.log().log_state().await.unwrap().last_log,
            Some(LogId::new(1, 1))
        );
        assert_eq!(
            storage.snapshots().current().await.unwrap().map(|(_, d)| d),
            Some(vec![1, 2])
        );
    }
}
