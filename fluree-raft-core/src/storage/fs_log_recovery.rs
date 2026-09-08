//! Recovery of the file log's contiguous, unpurged extent.
use super::*;

impl FsRaftLogStore {
    async fn read_start(&self) -> Result<Option<u64>, StorageError> {
        if let Some(&start) = self.start.get() {
            return Ok(Some(start));
        }
        let Some(bytes) = read_if_exists(&self.root.join("log_start")).await? else {
            return Ok(None);
        };
        let start: u64 =
            postcard::from_bytes(&bytes).map_err(|e| ser_err("decode log_start", e))?;
        if start > 1 {
            return Err(StorageError::corruption("invalid log_start"));
        }
        let _ = self.start.set(start);
        Ok(Some(start))
    }

    pub(super) async fn ensure_start(&self, first_index: u64) -> Result<(), StorageError> {
        if self.read_start().await?.is_some() {
            return Ok(());
        }
        // openraft starts at zero. Older embedders/tests also use one-based
        // logs. Persist the origin BEFORE the first append, so a lost first
        // rename cannot turn a zero-based log into an apparently valid one-based log.
        let indices = self.list_entry_indices().await?;
        let start = u64::from(!indices.contains(&0) && first_index != 0);
        atomic_write(
            &self.root.join("log_start"),
            &postcard::to_allocvec(&start).map_err(|e| ser_err("encode log_start", e))?,
        )
        .await?;
        let _ = self.start.set(start);
        Ok(())
    }

    pub(super) async fn live_extent(&self) -> Result<(LogState, Vec<u64>), StorageError> {
        let last_purged = self.read_last_purged().await?;
        let indices = self.list_entry_indices().await?;
        let start = match self.read_start().await? {
            Some(start) => start,
            // Legacy roots have no origin marker. Infer only the supported
            // zero/one-based convention, never the first arbitrary surviving file.
            None => u64::from(!indices.contains(&0)),
        };
        let mut expected = match last_purged {
            Some(id) => id.index.checked_add(1),
            None => Some(start),
        };
        let mut live = Vec::new();
        for index in indices {
            if last_purged.is_some_and(|p| index <= p.index) {
                continue;
            }
            if Some(index) != expected {
                break;
            }
            live.push(index);
            expected = index.checked_add(1);
        }
        let last_log = match live.last() {
            Some(&index) => Some(self.required_entry(index).await?.log_id),
            None => None,
        };
        if let Some(committed) = self.read_committed().await? {
            if last_purged.is_some_and(|p| committed.index < p.index) {
                // Covered by the already durable snapshot/purge boundary.
            } else if last_purged.is_some_and(|p| committed.index == p.index) {
                if Some(committed) != last_purged {
                    return Err(StorageError::corruption("committed/purged log id mismatch"));
                }
            } else if !live.contains(&committed.index)
                || self.required_entry(committed.index).await?.log_id != committed
            {
                return Err(StorageError::corruption(
                    "committed log is missing or inconsistent",
                ));
            }
        }
        Ok((
            LogState {
                last_purged,
                last_log,
            },
            live,
        ))
    }

    pub(super) async fn required_entry(&self, index: u64) -> Result<LogEntry, StorageError> {
        self.read_entry(index)
            .await?
            .ok_or_else(|| StorageError::corruption(format!("missing log entry {index}")))
    }

    pub(super) async fn repair_on_open(&self) -> Result<(), StorageError> {
        // Only called before exposing the store to a running Raft node. Verify
        // the known committed boundary before removing anything; never disguise
        // a missing committed prefix as an interrupted, uncommitted append.
        let (_, live) = self.live_extent().await?;
        for &index in &live {
            self.required_entry(index).await?;
        }
        let indices = self.list_entry_indices().await?;
        if !indices.is_empty() {
            // Upgrade legacy roots while their origin can still be observed.
            self.ensure_start(indices[0]).await?;
        }
        let mut removed = false;
        for index in indices {
            if live.binary_search(&index).is_err() {
                remove_if_exists(&self.entry_path(index)).await?;
                removed = true;
            }
        }
        if removed {
            // Otherwise filling the old gap can resurrect stale entries after
            // another restart. A failed sync prevents opening; repeat repair on retry.
            fsync_dir(&self.log_dir()).await?;
        }
        Ok(())
    }
}
