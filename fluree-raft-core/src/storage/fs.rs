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
use tracing::warn;

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

/// Durably write `bytes` to a temp file and rename it over `path`,
/// but leave the parent-directory fsync (which makes the rename
/// itself durable) to the caller. The file *contents* are synced
/// before the rename, so after the caller fsyncs the directory the
/// entry is fully durable. A batch writer (`append`) uses this to
/// pay one directory fsync for the whole batch instead of one per
/// entry; single writers go through [`atomic_write`].
async fn write_and_rename(path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
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
    Ok(())
}

/// [`write_and_rename`] plus the parent-directory fsync, so the
/// rename is durable on return. For one-off writes (vote, committed,
/// snapshot files); batch writers fsync the directory once at the end.
async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    write_and_rename(path, bytes).await?;
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
        // Each entry's contents are fsync'd before its rename; the
        // renames are made durable by a single directory fsync at the
        // end, so a batch of N pays N+1 fsyncs rather than 2N. A crash
        // before that final fsync can leave the tail renames durable
        // out of order (a gap), but the append hasn't been acked to
        // openraft, and `log_state` reports only the contiguous prefix
        // above the purge cutoff — so the gap and any orphans past it
        // are dropped and re-appended on recovery.
        for entry in entries {
            let bytes = postcard::to_allocvec(entry).map_err(|e| ser_err("encode entry", e))?;
            write_and_rename(&self.entry_path(entry.log_id.index), &bytes).await?;
        }
        if !entries.is_empty() {
            fsync_dir(&self.log_dir()).await?;
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
        let mut removed = false;
        for idx in indices.into_iter().filter(|&i| i >= from_index).rev() {
            remove_if_exists(&self.entry_path(idx)).await?;
            removed = true;
        }
        // One directory fsync makes the whole unlink batch durable.
        // Without it a crash can resurrect deleted entries, and
        // POSIX doesn't order unlink persistence, so even the
        // descending delete order above can be defeated — a
        // resurrected entry above a persisted deletion is a mid-log
        // hole that `read_range` cannot represent.
        if removed {
            fsync_dir(&self.log_dir()).await?;
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
        let mut removed = false;
        for idx in indices.into_iter().filter(|&i| i <= log_id.index) {
            remove_if_exists(&self.entry_path(idx)).await?;
            removed = true;
        }
        // See `truncate_from`: the batch's unlinks aren't durable
        // until the directory is synced.
        if removed {
            fsync_dir(&self.log_dir()).await?;
        }
        Ok(())
    }

    async fn log_state(&self) -> Result<LogState, StorageError> {
        let last_purged = self.read_last_purged().await?;
        let purged_cutoff = last_purged.map(|p| p.index);
        let indices = self.list_entry_indices().await?;
        // `last_log` is the top of the contiguous run of entries above
        // `last_purged`. Two orphan sources make this a contiguous
        // scan rather than a plain max:
        //   - indices <= cutoff (a `purge_through` that crashed between
        //     marker write and deletion) — filtered out by the cutoff;
        //   - a gap anywhere from `cutoff + 1` up (an `append` batch
        //     that crashed before its final directory fsync, leaving
        //     renames durable out of order) — the scan is anchored at
        //     `cutoff + 1` (or index 1 with nothing purged) and stops
        //     at the first missing index, so openraft never sees a
        //     `last_log` past a hole. A gap at the very front (the
        //     first expected index absent) yields `None`. Entries
        //     beyond the gap are orphans that recovery re-appends over.
        let first_expected = purged_cutoff.map_or(1, |c| c + 1);
        let last_index = indices
            .iter()
            .copied()
            .filter(|&idx| purged_cutoff.is_none_or(|c| idx > c))
            .scan(first_expected, |expected, idx| {
                if idx == *expected {
                    *expected += 1;
                    Some(idx)
                } else {
                    None // gap (at the anchor or in the tail) — stop the run
                }
            })
            .last();
        let last_log = match last_index {
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
            None => {
                remove_if_exists(&self.committed_path()).await?;
                // The unlink is a durability point like the write
                // arm's `atomic_write`: sync its directory so the
                // stale committed marker can't resurrect on crash.
                fsync_dir(&self.root).await
            }
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

    fn meta_path(&self, id: &str) -> PathBuf {
        self.snapshot_dir().join(format!("{id}.meta"))
    }

    fn data_path(&self, id: &str) -> PathBuf {
        self.snapshot_dir().join(format!("{id}.data"))
    }

    fn current_path(&self) -> PathBuf {
        self.snapshot_dir().join("current")
    }
}

/// Reject snapshot ids that would be unsafe to interpolate into a
/// path component under the snapshots directory.
///
/// `install_snapshot` carries the snapshot id as a peer-supplied
/// string. Without this gate, a peer could push a snapshot whose id
/// is `../../../etc/whatever` and the meta/data writes would land
/// outside the storage root with peer-controlled bytes — a
/// single-peer compromise turns into cluster-wide arbitrary FS
/// write bounded only by the raft-storage UID's permissions. The
/// `current` pointer file also stores the id as raw bytes, so a
/// one-time poison would re-fire on every restart until disinfected.
///
/// Allowlist: non-empty, `[A-Za-z0-9._-]+`, length ≤ 128, no `..`
/// substring. The locally-generated `snap-{last_index}-{counter}`
/// format always passes; on the read paths a previously poisoned id
/// (from before this gate landed) is rejected before any path
/// construction. Returns the validated string for the caller to
/// thread into `meta_path` / `data_path`.
fn validate_path_safe_id(id: &SnapshotId) -> Result<&str, StorageError> {
    const MAX_LEN: usize = 128;
    let s = id.as_str();
    if s.is_empty() {
        return Err(StorageError::corruption("snapshot id is empty"));
    }
    if s.len() > MAX_LEN {
        return Err(StorageError::corruption(format!(
            "snapshot id length {} exceeds cap {MAX_LEN}",
            s.len()
        )));
    }
    if s.contains("..") {
        return Err(StorageError::corruption(format!(
            "snapshot id contains path-traversal '..': {s:?}"
        )));
    }
    if let Some(c) = s
        .chars()
        .find(|c| !matches!(c, 'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.'))
    {
        return Err(StorageError::corruption(format!(
            "snapshot id contains disallowed character {c:?}: {s:?}"
        )));
    }
    Ok(s)
}

#[async_trait]
impl RaftSnapshotStore for FsRaftSnapshotStore {
    async fn write(&self, meta: &SnapshotMeta, data: Vec<u8>) -> Result<(), StorageError> {
        let safe_id = validate_path_safe_id(&meta.id)?;
        let meta_bytes =
            postcard::to_allocvec(meta).map_err(|e| ser_err("encode snapshot meta", e))?;
        atomic_write(&self.meta_path(safe_id), &meta_bytes).await?;
        atomic_write(&self.data_path(safe_id), &data).await?;
        atomic_write(&self.current_path(), safe_id.as_bytes()).await?;
        self.reclaim_superseded(safe_id).await;
        Ok(())
    }

    async fn read(&self, id: &SnapshotId) -> Result<Option<Vec<u8>>, StorageError> {
        let safe_id = validate_path_safe_id(id)?;
        read_if_exists(&self.data_path(safe_id)).await
    }

    async fn current(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, StorageError> {
        let Some(id_bytes) = read_if_exists(&self.current_path()).await? else {
            return Ok(None);
        };
        let id_str = std::str::from_utf8(&id_bytes).map_err(|e| {
            StorageError::corruption(format!("current snapshot id is not utf8: {e}"))
        })?;
        let id = SnapshotId::new(id_str);
        let safe_id = validate_path_safe_id(&id)?;

        // A pointer naming missing files is corruption, not a fresh
        // boot: the log below the snapshot has typically been
        // purged, so silently reporting "no snapshot" would restart
        // the node with committed state unrecoverable. Only an
        // absent pointer means "never snapshotted".
        let Some(meta_bytes) = read_if_exists(&self.meta_path(safe_id)).await? else {
            return Err(StorageError::corruption(format!(
                "current snapshot pointer names {safe_id:?} but its meta file is missing"
            )));
        };
        let meta =
            postcard::from_bytes(&meta_bytes).map_err(|e| ser_err("decode snapshot meta", e))?;

        let Some(data) = read_if_exists(&self.data_path(safe_id)).await? else {
            return Err(StorageError::corruption(format!(
                "current snapshot pointer names {safe_id:?} but its data file is missing"
            )));
        };

        Ok(Some((meta, data)))
    }
}

impl FsRaftSnapshotStore {
    /// Remove every snapshot file except `keep_id`'s pair and the
    /// `current` pointer, plus any `*.tmp` staged by a crashed
    /// `atomic_write`. Called after the pointer durably names
    /// `keep_id`, so nothing removed here is reachable; without the
    /// sweep every superseded snapshot (a full serialized state
    /// machine) stays on disk forever. Best-effort — a failed
    /// removal leaves an orphan for the next write's sweep, never a
    /// broken snapshot — but completed removals get one directory
    /// fsync so a crash can't resurrect a half-removed sibling.
    async fn reclaim_superseded(&self, keep_id: &str) {
        let dir = self.snapshot_dir();
        let mut entries = match fs::read_dir(&dir).await {
            Ok(entries) => entries,
            Err(e) => {
                warn!(error = %e, "snapshot reclamation could not list directory");
                return;
            }
        };
        let mut removed = false;
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let superseded = name
                .strip_suffix(".meta")
                .or_else(|| name.strip_suffix(".data"))
                .is_some_and(|stem| stem != keep_id)
                || name.ends_with(".tmp");
            if !superseded {
                continue;
            }
            match fs::remove_file(entry.path()).await {
                Ok(()) => removed = true,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => {
                    warn!(file = %name, error = %e, "snapshot reclamation failed to remove file");
                }
            }
        }
        if removed {
            if let Err(e) = fsync_dir(&dir).await {
                warn!(error = %e, "snapshot reclamation directory sync failed");
            }
        }
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
    async fn log_state_stops_at_a_tail_gap() {
        // Simulate an `append` batch that crashed before its final
        // directory fsync: entries 1..3 durable, 4's rename lost, 5
        // durable past the gap. `log_state` must report `last_log = 3`
        // (the contiguous prefix), never 5 — otherwise openraft would
        // see a `last_log` past the missing entry 4.
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
        std::fs::remove_file(store.entry_path(4)).expect("open the gap at index 4");

        let state = store.log_state().await.unwrap();
        assert_eq!(
            state.last_log,
            Some(LogId::new(1, 3)),
            "last_log must stop at the gap, ignoring the orphan at 5"
        );
    }

    #[tokio::test]
    async fn log_state_reports_none_on_a_front_gap() {
        // The gap is at the very first expected index. This is the
        // crash-mid-append case on a log that was empty above the
        // cutoff (fresh node, or right after a snapshot install +
        // full purge): the batch's first entry's rename is lost while
        // later ones survive. With nothing older to anchor the run,
        // `last_log` must be `None` — reporting the surviving orphans'
        // top would put `last_log` above the missing anchor.

        // No purge marker → the anchor is index 1; drop entry 1.
        let (_dir, store) = fresh_log_store().await;
        store
            .append(&[entry(1, 1), entry(1, 2), entry(1, 3)])
            .await
            .unwrap();
        std::fs::remove_file(store.entry_path(1)).expect("open the front gap at index 1");
        let state = store.log_state().await.unwrap();
        assert!(
            state.last_log.is_none(),
            "a gap at the anchor must yield last_log=None, got {:?}",
            state.last_log
        );

        // With a purge cutoff at 3, the anchor is index 4; drop entry 4.
        let (_dir2, store2) = fresh_log_store().await;
        store2
            .append(&[entry(1, 4), entry(1, 5), entry(1, 6)])
            .await
            .unwrap();
        let marker = postcard::to_allocvec(&LogId::new(1, 3)).expect("encode marker");
        atomic_write(&store2.last_purged_path(), &marker)
            .await
            .expect("write marker");
        std::fs::remove_file(store2.entry_path(4)).expect("open the front gap at index 4");
        let state2 = store2.log_state().await.unwrap();
        assert!(
            state2.last_log.is_none(),
            "a gap at cutoff+1 must yield last_log=None, got {:?}",
            state2.last_log
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
        // The superseded snapshot is reclaimed once `current` names
        // its successor — `read` reports it gone, and its files no
        // longer occupy disk.
        assert_eq!(store.read(&SnapshotId::new("snap-1")).await.unwrap(), None);
        assert!(!dir.path().join("snapshots").join("snap-1.meta").exists());
        assert!(!dir.path().join("snapshots").join("snap-1.data").exists());
    }

    /// A `current` pointer naming missing files must hard-fail as
    /// corruption: the log below the snapshot is typically purged,
    /// so booting as if no snapshot exists silently abandons
    /// committed state.
    #[tokio::test]
    async fn dangling_current_pointer_is_corruption() {
        let dir = TempDir::new().unwrap();
        let store = FsRaftSnapshotStore::open(dir.path().to_path_buf())
            .await
            .unwrap();
        let meta = SnapshotMeta {
            id: SnapshotId::new("snap-1"),
            last_applied: Some(LogId::new(1, 5)),
            membership: vec![],
        };
        store.write(&meta, vec![1]).await.unwrap();

        let data_path = dir.path().join("snapshots").join("snap-1.data");
        std::fs::remove_file(&data_path).unwrap();
        let err = store.current().await.expect_err("missing data file");
        assert!(matches!(err, StorageError::Corruption(_)), "got {err:?}");

        let meta_path = dir.path().join("snapshots").join("snap-1.meta");
        std::fs::remove_file(&meta_path).unwrap();
        let err = store.current().await.expect_err("missing meta file");
        assert!(matches!(err, StorageError::Corruption(_)), "got {err:?}");

        // An absent pointer is still a legitimate fresh boot.
        std::fs::remove_file(dir.path().join("snapshots").join("current")).unwrap();
        assert!(store.current().await.unwrap().is_none());
    }

    /// Reclamation also sweeps `*.tmp` files staged by a crashed
    /// `atomic_write` — by the time it runs, every completed write's
    /// staging file has been renamed away, so any survivor is a
    /// leftover.
    #[tokio::test]
    async fn reclamation_sweeps_stale_tmp_files() {
        let dir = TempDir::new().unwrap();
        let store = FsRaftSnapshotStore::open(dir.path().to_path_buf())
            .await
            .unwrap();
        let stale_tmp = dir.path().join("snapshots").join("snap-0.tmp");
        std::fs::write(&stale_tmp, b"crashed mid-write").unwrap();

        let meta = SnapshotMeta {
            id: SnapshotId::new("snap-1"),
            last_applied: Some(LogId::new(1, 5)),
            membership: vec![],
        };
        store.write(&meta, vec![1]).await.unwrap();

        assert!(!stale_tmp.exists());
        // The new snapshot itself is intact.
        let (current_meta, _) = store.current().await.unwrap().unwrap();
        assert_eq!(current_meta.id, SnapshotId::new("snap-1"));
    }

    #[test]
    fn validate_path_safe_id_accepts_safe_inputs() {
        // The shape the local builder emits — always passes.
        assert!(validate_path_safe_id(&SnapshotId::new("snap-1-0")).is_ok());
        assert!(validate_path_safe_id(&SnapshotId::new("snap-18446744073709551615-0")).is_ok());
        // Allowlisted alphabet.
        assert!(validate_path_safe_id(&SnapshotId::new("abc_DEF-123.tag")).is_ok());
        assert!(validate_path_safe_id(&SnapshotId::new("a")).is_ok());
    }

    #[test]
    fn validate_path_safe_id_rejects_path_traversal() {
        // The headline exploit shape — peer-supplied id whose
        // unmodified interpolation escapes <root>/snapshots.
        let id = SnapshotId::new("../../../etc/passwd");
        let err = validate_path_safe_id(&id).unwrap_err();
        assert!(matches!(err, StorageError::Corruption(_)));
        assert!(err.to_string().contains(".."));

        // `..` anywhere triggers, even surrounded by allowlisted chars.
        assert!(validate_path_safe_id(&SnapshotId::new("foo..bar")).is_err());
        assert!(validate_path_safe_id(&SnapshotId::new("a..b")).is_err());
        assert!(validate_path_safe_id(&SnapshotId::new("..")).is_err());
        assert!(validate_path_safe_id(&SnapshotId::new("...")).is_err());
    }

    #[test]
    fn validate_path_safe_id_rejects_path_separators_and_exotic_chars() {
        // Path separators on both Unix and Windows.
        assert!(validate_path_safe_id(&SnapshotId::new("dir/file")).is_err());
        assert!(validate_path_safe_id(&SnapshotId::new("dir\\file")).is_err());
        // Null byte — would terminate C strings early in some FS layers.
        assert!(validate_path_safe_id(&SnapshotId::new("a\0b")).is_err());
        // Whitespace + non-ASCII.
        assert!(validate_path_safe_id(&SnapshotId::new("a b")).is_err());
        assert!(validate_path_safe_id(&SnapshotId::new("café")).is_err());
        // Empty.
        assert!(validate_path_safe_id(&SnapshotId::new("")).is_err());
    }

    #[test]
    fn validate_path_safe_id_caps_length() {
        // Locally-generated ids are tens of chars; anything orders
        // of magnitude larger is a peer trying to exhaust resources.
        let oversized = "a".repeat(129);
        assert!(validate_path_safe_id(&SnapshotId::new(oversized)).is_err());
        let at_cap = "a".repeat(128);
        assert!(validate_path_safe_id(&SnapshotId::new(at_cap)).is_ok());
    }

    #[tokio::test]
    async fn snapshot_write_rejects_traversal_id_and_writes_nothing() {
        // End-to-end check that the validator gates `write` before
        // any FS touch — the snapshots dir should still be empty
        // (only the directory itself, no files) after the rejection.
        let dir = TempDir::new().unwrap();
        let store = FsRaftSnapshotStore::open(dir.path().to_path_buf())
            .await
            .unwrap();
        let meta = SnapshotMeta {
            id: SnapshotId::new("../escape"),
            last_applied: Some(LogId::new(1, 1)),
            membership: vec![],
        };
        let err = store.write(&meta, vec![1, 2, 3]).await.unwrap_err();
        assert!(matches!(err, StorageError::Corruption(_)));

        let mut entries = fs::read_dir(store.snapshot_dir()).await.unwrap();
        assert!(
            entries.next_entry().await.unwrap().is_none(),
            "no snapshot files should have been written after a rejected traversal id"
        );
    }

    #[tokio::test]
    async fn snapshot_current_rejects_poisoned_id_on_disk() {
        // A pre-existing poisoned `current` file (written by a
        // vulnerable older build) is rejected at read time before
        // any meta/data path is constructed.
        let dir = TempDir::new().unwrap();
        let store = FsRaftSnapshotStore::open(dir.path().to_path_buf())
            .await
            .unwrap();
        atomic_write(&store.current_path(), b"../etc/escape")
            .await
            .unwrap();
        let err = store.current().await.unwrap_err();
        assert!(matches!(err, StorageError::Corruption(_)));
        assert!(err.to_string().contains(".."));
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
