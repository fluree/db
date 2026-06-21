//! Filesystem-backed [`RaftStorage`] backend.
//!
//! Layout under the storage root:
//!
//! ```text
//! <root>/
//!   vote           # postcard-serialized Vote
//!   committed      # postcard-serialized LogId (absent when None)
//!   last_purged    # postcard-serialized LogId (absent when never purged)
//!   log/
//!     <index>.entry  # postcard-serialized LogEntry, name is zero-padded
//!                    #   16-char hex of the index so directory listings
//!                    #   sort naturally
//!   snapshots/
//!     current        # plain-text snapshot id
//!     <id>.meta      # postcard-serialized SnapshotMeta
//!     <id>.data      # raw snapshot bytes
//! ```
//!
//! Every mutation is atomic-write-then-rename with `fsync` of both the
//! temp file and the parent directory (so the rename's directory
//! entry is durable across power loss, not just the file contents).
//! Directory fsync is a no-op on non-Unix targets, which don't expose
//! an equivalent operation.

use super::{
    LogEntry, LogId, LogState, RaftLogStore, RaftSnapshotStore, RaftStorage, SnapshotId,
    SnapshotMeta, StorageError, Vote,
};
use async_trait::async_trait;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;

fn io_err(action: &str, err: io::Error) -> StorageError {
    StorageError::io(format!("{action}: {err}"))
}

fn ser_err(action: &str, err: postcard::Error) -> StorageError {
    StorageError::serialization(format!("{action}: {err}"))
}

/// fsync the directory at `path` so any rename/create/unlink whose
/// effect on the directory entry should outlive a power loss is
/// actually persisted.
///
/// On Unix this opens the directory read-only and calls `fsync` on
/// the resulting fd. On non-Unix targets (Windows) the platform has
/// no equivalent operation; the call is a no-op and we accept the
/// weaker durability rather than failing.
async fn fsync_dir(path: &Path) -> Result<(), StorageError> {
    #[cfg(unix)]
    {
        let dir = fs::File::open(path)
            .await
            .map_err(|e| io_err("open parent dir", e))?;
        dir.sync_all()
            .await
            .map_err(|e| io_err("sync parent dir", e))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

/// Atomic write: stage to `<path>.tmp`, fsync, rename onto `path`,
/// fsync the parent directory.
///
/// On any POSIX-y filesystem the rename is atomic, so readers either
/// see the previous good file or the new good file — never a torn
/// write. Fsyncing the parent directory after the rename is what
/// keeps that guarantee across a power loss: without it the rename
/// can revert on remount, silently rolling back a write whose `Ok`
/// callers (Raft vote / log entry / snapshot pointer persistence)
/// rely on for safety.
async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    let tmp = path.with_extension("tmp");
    {
        let mut file = fs::File::create(&tmp)
            .await
            .map_err(|e| io_err("create tmp", e))?;
        file.write_all(bytes)
            .await
            .map_err(|e| io_err("write tmp", e))?;
        file.sync_all().await.map_err(|e| io_err("sync tmp", e))?;
    }
    fs::rename(&tmp, path)
        .await
        .map_err(|e| io_err("rename tmp", e))?;
    if let Some(parent) = path.parent() {
        fsync_dir(parent).await?;
    }
    Ok(())
}

/// Read `path` and distinguish "missing" from "I/O failure".
async fn read_if_exists(path: &Path) -> Result<Option<Vec<u8>>, StorageError> {
    match fs::read(path).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(io_err("read", e)),
    }
}

fn entry_filename(index: u64) -> String {
    format!("{index:016x}.entry")
}

fn parse_entry_filename(name: &str) -> Option<u64> {
    let stem = name.strip_suffix(".entry")?;
    u64::from_str_radix(stem, 16).ok()
}

/// Filesystem-backed implementation of [`RaftLogStore`].
pub struct FsRaftLogStore {
    root: PathBuf,
}

impl FsRaftLogStore {
    /// Open or create the log store rooted at `root`. Creates the
    /// directory tree if missing.
    pub async fn open(root: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let root = root.into();
        fs::create_dir_all(root.join("log"))
            .await
            .map_err(|e| io_err("create log dir", e))?;
        Ok(Self { root })
    }

    fn log_dir(&self) -> PathBuf {
        self.root.join("log")
    }

    fn vote_path(&self) -> PathBuf {
        self.root.join("vote")
    }

    fn committed_path(&self) -> PathBuf {
        self.root.join("committed")
    }

    fn last_purged_path(&self) -> PathBuf {
        self.root.join("last_purged")
    }

    fn entry_path(&self, index: u64) -> PathBuf {
        self.log_dir().join(entry_filename(index))
    }

    async fn list_entry_indices(&self) -> Result<Vec<u64>, StorageError> {
        let mut dir = fs::read_dir(self.log_dir())
            .await
            .map_err(|e| io_err("read log dir", e))?;
        let mut indices = Vec::new();
        while let Some(entry) = dir
            .next_entry()
            .await
            .map_err(|e| io_err("iter log dir", e))?
        {
            let name = entry.file_name();
            if let Some(name_str) = name.to_str() {
                if let Some(idx) = parse_entry_filename(name_str) {
                    indices.push(idx);
                }
            }
        }
        indices.sort_unstable();
        Ok(indices)
    }

    async fn read_entry(&self, index: u64) -> Result<Option<LogEntry>, StorageError> {
        match read_if_exists(&self.entry_path(index)).await? {
            Some(bytes) => {
                let entry = postcard::from_bytes(&bytes).map_err(|e| ser_err("decode entry", e))?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    async fn read_last_purged(&self) -> Result<Option<LogId>, StorageError> {
        match read_if_exists(&self.last_purged_path()).await? {
            Some(bytes) => {
                let id =
                    postcard::from_bytes(&bytes).map_err(|e| ser_err("decode last_purged", e))?;
                Ok(Some(id))
            }
            None => Ok(None),
        }
    }
}

#[async_trait]
impl RaftLogStore for FsRaftLogStore {
    async fn append(&self, entries: &[LogEntry]) -> Result<(), StorageError> {
        for entry in entries {
            let bytes = postcard::to_allocvec(entry).map_err(|e| ser_err("encode entry", e))?;
            atomic_write(&self.entry_path(entry.log_id.index), &bytes).await?;
        }
        Ok(())
    }

    async fn read_range(&self, range: Range<u64>) -> Result<Vec<LogEntry>, StorageError> {
        let indices = self.list_entry_indices().await?;
        let mut entries = Vec::new();
        for idx in indices.into_iter().filter(|i| range.contains(i)) {
            if let Some(entry) = self.read_entry(idx).await? {
                entries.push(entry);
            }
        }
        Ok(entries)
    }

    async fn truncate_from(&self, from_index: u64) -> Result<(), StorageError> {
        // Delete in descending order so the surviving prefix stays
        // contiguous after a crash mid-loop. The reverse order
        // (ascending) can leave a hole — e.g. removing 5 and 6 but
        // crashing before 7..N — and `log_state` would then report
        // `last_log` from a stale-term entry above the missing
        // window, which openraft cannot reconcile. With descending
        // deletion the worst-case post-crash state is some
        // stale-term entries still in [from_index, k]; openraft's
        // append-entries conflict detection re-triggers
        // `truncate_from` against the actual conflict point on
        // recovery, so no missing-middle hole ever surfaces.
        let indices = self.list_entry_indices().await?;
        for idx in indices.into_iter().filter(|&i| i >= from_index).rev() {
            remove_if_exists(&self.entry_path(idx)).await?;
        }
        Ok(())
    }

    async fn purge_through(&self, log_id: LogId) -> Result<(), StorageError> {
        let existing = self.read_last_purged().await?;
        if matches!(existing, Some(p) if p.index >= log_id.index) {
            return Ok(());
        }

        // Persist the marker BEFORE deleting any entry files. A crash
        // after the marker but before all deletions leaves orphans at
        // indices <= log_id.index, which `log_state` / `read_range`
        // filter out via the marker; openraft sees a consistent
        // (last_purged, last_log] window. The reverse order would
        // leave entries 1..k missing with `last_purged` still
        // pointing at an older id — a hole openraft cannot reconcile.
        let bytes = postcard::to_allocvec(&log_id).map_err(|e| ser_err("encode last_purged", e))?;
        atomic_write(&self.last_purged_path(), &bytes).await?;

        let indices = self.list_entry_indices().await?;
        for idx in indices.into_iter().filter(|&i| i <= log_id.index) {
            remove_if_exists(&self.entry_path(idx)).await?;
        }
        Ok(())
    }

    async fn log_state(&self) -> Result<LogState, StorageError> {
        let last_purged = self.read_last_purged().await?;
        let purged_cutoff = last_purged.map(|p| p.index);
        let indices = self.list_entry_indices().await?;
        // `last_log` reflects only entries strictly above
        // `last_purged`; orphans at indices <= cutoff (left behind by
        // a crashed `purge_through` between marker write and
        // deletion) must not bump `last_log` or openraft sees a
        // last_log inside the purged range.
        let last_log = match indices
            .iter()
            .rev()
            .find(|&&idx| purged_cutoff.is_none_or(|c| idx > c))
            .copied()
        {
            Some(idx) => self.read_entry(idx).await?.map(|e| e.log_id),
            None => None,
        };
        Ok(LogState {
            last_purged,
            last_log,
        })
    }

    async fn save_vote(&self, vote: &Vote) -> Result<(), StorageError> {
        let bytes = postcard::to_allocvec(vote).map_err(|e| ser_err("encode vote", e))?;
        atomic_write(&self.vote_path(), &bytes).await
    }

    async fn read_vote(&self) -> Result<Option<Vote>, StorageError> {
        match read_if_exists(&self.vote_path()).await? {
            Some(bytes) => {
                let vote = postcard::from_bytes(&bytes).map_err(|e| ser_err("decode vote", e))?;
                Ok(Some(vote))
            }
            None => Ok(None),
        }
    }

    async fn save_committed(&self, log_id: Option<LogId>) -> Result<(), StorageError> {
        match log_id {
            Some(id) => {
                let bytes =
                    postcard::to_allocvec(&id).map_err(|e| ser_err("encode committed", e))?;
                atomic_write(&self.committed_path(), &bytes).await
            }
            None => remove_if_exists(&self.committed_path()).await,
        }
    }

    async fn read_committed(&self) -> Result<Option<LogId>, StorageError> {
        match read_if_exists(&self.committed_path()).await? {
            Some(bytes) => {
                let id =
                    postcard::from_bytes(&bytes).map_err(|e| ser_err("decode committed", e))?;
                Ok(Some(id))
            }
            None => Ok(None),
        }
    }
}

async fn remove_if_exists(path: &Path) -> Result<(), StorageError> {
    match fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(io_err("remove file", e)),
    }
}

/// Filesystem-backed implementation of [`RaftSnapshotStore`].
pub struct FsRaftSnapshotStore {
    root: PathBuf,
}

impl FsRaftSnapshotStore {
    /// Open or create the snapshot store rooted at `root`. Creates the
    /// directory tree if missing.
    pub async fn open(root: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let root = root.into();
        fs::create_dir_all(root.join("snapshots"))
            .await
            .map_err(|e| io_err("create snapshot dir", e))?;
        Ok(Self { root })
    }

    fn snapshot_dir(&self) -> PathBuf {
        self.root.join("snapshots")
    }

    fn meta_path(&self, id: &SnapshotId) -> PathBuf {
        self.snapshot_dir().join(format!("{}.meta", id.as_str()))
    }

    fn data_path(&self, id: &SnapshotId) -> PathBuf {
        self.snapshot_dir().join(format!("{}.data", id.as_str()))
    }

    fn current_path(&self) -> PathBuf {
        self.snapshot_dir().join("current")
    }
}

#[async_trait]
impl RaftSnapshotStore for FsRaftSnapshotStore {
    async fn write(&self, meta: &SnapshotMeta, data: Vec<u8>) -> Result<(), StorageError> {
        let meta_bytes =
            postcard::to_allocvec(meta).map_err(|e| ser_err("encode snapshot meta", e))?;
        atomic_write(&self.meta_path(&meta.id), &meta_bytes).await?;
        atomic_write(&self.data_path(&meta.id), &data).await?;
        atomic_write(&self.current_path(), meta.id.as_str().as_bytes()).await?;
        Ok(())
    }

    async fn read(&self, id: &SnapshotId) -> Result<Option<Vec<u8>>, StorageError> {
        read_if_exists(&self.data_path(id)).await
    }

    async fn current(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, StorageError> {
        let Some(id_bytes) = read_if_exists(&self.current_path()).await? else {
            return Ok(None);
        };
        let id_str = std::str::from_utf8(&id_bytes).map_err(|e| {
            StorageError::corruption(format!("current snapshot id is not utf8: {e}"))
        })?;
        let id = SnapshotId::new(id_str);

        let Some(meta_bytes) = read_if_exists(&self.meta_path(&id)).await? else {
            return Ok(None);
        };
        let meta =
            postcard::from_bytes(&meta_bytes).map_err(|e| ser_err("decode snapshot meta", e))?;

        let Some(data) = read_if_exists(&self.data_path(&id)).await? else {
            return Ok(None);
        };

        Ok(Some((meta, data)))
    }
}

/// Combined filesystem-backed [`RaftStorage`]: log and snapshot stores
/// share the same root directory.
pub struct FsRaftStorage {
    log: FsRaftLogStore,
    snapshots: FsRaftSnapshotStore,
}

impl FsRaftStorage {
    /// Open or create the storage tree under `root`. Each constituent
    /// store creates its own subdirectory.
    pub async fn open(root: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let root = root.into();
        Ok(Self {
            log: FsRaftLogStore::open(&root).await?,
            snapshots: FsRaftSnapshotStore::open(&root).await?,
        })
    }
}

impl RaftStorage for FsRaftStorage {
    type LogStore = FsRaftLogStore;
    type SnapshotStore = FsRaftSnapshotStore;

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
    use tempfile::TempDir;

    fn entry(term: u64, index: u64) -> LogEntry {
        LogEntry {
            log_id: LogId::new(term, index),
            payload: format!("entry-{term}-{index}").into_bytes(),
        }
    }

    async fn fresh_log_store() -> (TempDir, FsRaftLogStore) {
        let dir = TempDir::new().unwrap();
        let store = FsRaftLogStore::open(dir.path().to_path_buf())
            .await
            .unwrap();
        (dir, store)
    }

    #[tokio::test]
    async fn append_and_read_range() {
        let (_dir, store) = fresh_log_store().await;
        store
            .append(&[entry(1, 1), entry(1, 2), entry(1, 3)])
            .await
            .unwrap();
        let got = store.read_range(0..100).await.unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].log_id, LogId::new(1, 1));
        assert_eq!(got[2].log_id, LogId::new(1, 3));
    }

    #[tokio::test]
    async fn read_range_returns_subset() {
        let (_dir, store) = fresh_log_store().await;
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
    async fn truncate_removes_suffix() {
        let (_dir, store) = fresh_log_store().await;
        store
            .append(&[entry(1, 1), entry(1, 2), entry(1, 3)])
            .await
            .unwrap();
        store.truncate_from(2).await.unwrap();
        let got = store.read_range(0..100).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].log_id.index, 1);
    }

    /// Simulates a crash mid-`truncate_from`: the descending-order
    /// loop completed deletions of the top few entries but stopped
    /// before reaching the truncation point. The surviving log must
    /// be a contiguous prefix — no missing-middle hole — so
    /// `log_state` reports a coherent `last_log` and `read_range`
    /// returns every index up to it.
    #[tokio::test]
    async fn partial_truncate_leaves_contiguous_prefix() {
        let (_dir, store) = fresh_log_store().await;
        store
            .append(&[
                entry(1, 1),
                entry(1, 2),
                entry(1, 3),
                entry(1, 4),
                entry(1, 5),
            ])
            .await
            .unwrap();

        // Hand-delete the top two entries to mimic the on-disk
        // state after a `truncate_from(2)` that crashed after
        // removing 5 and 4 but before 3 and 2. Ascending-order
        // deletion would have left 5 in place with 2 and 3 gone — a
        // hole at indices 2,3 with last_log=5.
        remove_if_exists(&store.entry_path(5)).await.unwrap();
        remove_if_exists(&store.entry_path(4)).await.unwrap();

        let state = store.log_state().await.unwrap();
        assert_eq!(state.last_log, Some(LogId::new(1, 3)));
        let got = store.read_range(0..100).await.unwrap();
        let indices: Vec<u64> = got.iter().map(|e| e.log_id.index).collect();
        assert_eq!(indices, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn purge_removes_prefix_and_records_last_purged() {
        let (_dir, store) = fresh_log_store().await;
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
        let (_dir, store) = fresh_log_store().await;
        store
            .append(&[entry(1, 1), entry(1, 2), entry(1, 3)])
            .await
            .unwrap();
        store.purge_through(LogId::new(1, 2)).await.unwrap();
        store.purge_through(LogId::new(1, 1)).await.unwrap();
        let state = store.log_state().await.unwrap();
        assert_eq!(state.last_purged, Some(LogId::new(1, 2)));
    }

    /// Simulates a crash after `purge_through` wrote the marker but
    /// before all entry files at or below it were deleted. The
    /// orphans must be invisible to openraft: `log_state` reports
    /// `last_log` from entries strictly above the marker, and
    /// `read_range` returns only the live tail.
    #[tokio::test]
    async fn log_state_hides_orphans_below_last_purged() {
        let (_dir, store) = fresh_log_store().await;
        store
            .append(&[entry(1, 1), entry(1, 2), entry(1, 3), entry(2, 4)])
            .await
            .unwrap();

        // Hand-write the marker as if a purge through index 3 had
        // gotten that far. Entries 1..3 are still on disk — those
        // are the orphans the next-step deletion would have removed.
        let marker =
            postcard::to_allocvec(&LogId::new(1, 3)).expect("encode last_purged for fixture");
        atomic_write(&store.last_purged_path(), &marker)
            .await
            .expect("write last_purged fixture");

        let state = store.log_state().await.unwrap();
        assert_eq!(state.last_purged, Some(LogId::new(1, 3)));
        assert_eq!(
            state.last_log,
            Some(LogId::new(2, 4)),
            "last_log must come from entries strictly above last_purged"
        );

        // A range covering everything above the marker must not be
        // affected by the orphans.
        let tail = store.read_range(4..100).await.unwrap();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].log_id, LogId::new(2, 4));
    }

    /// Edge case: a marker is in place and every entry at or below
    /// it survives (none above). `log_state` must report `last_log =
    /// None` so the openraft adapter falls back to the marker
    /// instead of advertising an orphan as the live tail.
    #[tokio::test]
    async fn log_state_returns_none_when_only_orphans_remain() {
        let (_dir, store) = fresh_log_store().await;
        store.append(&[entry(1, 1), entry(1, 2)]).await.unwrap();

        let marker =
            postcard::to_allocvec(&LogId::new(1, 5)).expect("encode last_purged for fixture");
        atomic_write(&store.last_purged_path(), &marker)
            .await
            .expect("write last_purged fixture");

        let state = store.log_state().await.unwrap();
        assert_eq!(state.last_purged, Some(LogId::new(1, 5)));
        assert!(
            state.last_log.is_none(),
            "orphans below the marker must not surface as last_log"
        );
    }

    #[tokio::test]
    async fn vote_round_trip_and_clear() {
        let (_dir, store) = fresh_log_store().await;
        assert!(store.read_vote().await.unwrap().is_none());
        let vote = Vote {
            term: 5,
            candidate: 42,
            committed: true,
        };
        store.save_vote(&vote).await.unwrap();
        assert_eq!(store.read_vote().await.unwrap(), Some(vote));
    }

    #[tokio::test]
    async fn committed_round_trip_and_clear() {
        let (_dir, store) = fresh_log_store().await;
        assert!(store.read_committed().await.unwrap().is_none());
        store.save_committed(Some(LogId::new(2, 7))).await.unwrap();
        assert_eq!(
            store.read_committed().await.unwrap(),
            Some(LogId::new(2, 7))
        );
        store.save_committed(None).await.unwrap();
        assert!(store.read_committed().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn entries_vote_committed_survive_reopen() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();
        {
            let store = FsRaftLogStore::open(&path).await.unwrap();
            store.append(&[entry(1, 1), entry(2, 2)]).await.unwrap();
            store
                .save_vote(&Vote {
                    term: 5,
                    candidate: 7,
                    committed: true,
                })
                .await
                .unwrap();
            store.save_committed(Some(LogId::new(2, 2))).await.unwrap();
            store.purge_through(LogId::new(1, 1)).await.unwrap();
        }
        let store = FsRaftLogStore::open(&path).await.unwrap();
        let got = store.read_range(0..100).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].log_id, LogId::new(2, 2));
        let state = store.log_state().await.unwrap();
        assert_eq!(state.last_purged, Some(LogId::new(1, 1)));
        assert_eq!(state.last_log, Some(LogId::new(2, 2)));
        assert_eq!(
            store.read_vote().await.unwrap(),
            Some(Vote {
                term: 5,
                candidate: 7,
                committed: true
            })
        );
        assert_eq!(
            store.read_committed().await.unwrap(),
            Some(LogId::new(2, 2))
        );
    }

    #[tokio::test]
    async fn snapshot_round_trip_with_reopen() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();
        {
            let store = FsRaftSnapshotStore::open(&path).await.unwrap();
            let meta = SnapshotMeta {
                id: SnapshotId::new("snap-1"),
                last_applied: Some(LogId::new(1, 5)),
                membership: vec![1, 2, 3],
            };
            store.write(&meta, vec![10, 20, 30]).await.unwrap();
        }
        let store = FsRaftSnapshotStore::open(&path).await.unwrap();
        let (meta, data) = store.current().await.unwrap().unwrap();
        assert_eq!(meta.id, SnapshotId::new("snap-1"));
        assert_eq!(meta.last_applied, Some(LogId::new(1, 5)));
        assert_eq!(data, vec![10, 20, 30]);
    }

    #[tokio::test]
    async fn snapshot_current_tracks_latest_write() {
        let dir = TempDir::new().unwrap();
        let store = FsRaftSnapshotStore::open(dir.path().to_path_buf())
            .await
            .unwrap();
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
        assert_eq!(
            store.read(&SnapshotId::new("snap-1")).await.unwrap(),
            Some(vec![1])
        );
    }

    #[tokio::test]
    async fn combined_storage_open_writes_both_sides() {
        let dir = TempDir::new().unwrap();
        let storage = FsRaftStorage::open(dir.path().to_path_buf()).await.unwrap();
        storage.log().append(&[entry(1, 1)]).await.unwrap();
        let meta = SnapshotMeta {
            id: SnapshotId::new("snap-1"),
            last_applied: Some(LogId::new(1, 1)),
            membership: vec![],
        };
        storage.snapshots().write(&meta, vec![99]).await.unwrap();

        assert_eq!(
            storage.log().log_state().await.unwrap().last_log,
            Some(LogId::new(1, 1))
        );
        let (_, data) = storage.snapshots().current().await.unwrap().unwrap();
        assert_eq!(data, vec![99]);
    }
}
