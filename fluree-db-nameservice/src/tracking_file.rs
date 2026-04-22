//! File-based implementation of [`RemoteTrackingStore`]
//!
//! Stores tracking records at `{base_path}/ns-sync/remotes/{remote_name}/{address_encoded}.json`.
//! Ledger IDs are percent-encoded to handle `:` and `/` safely in filenames.
//!
//! This module is only available with the `native` feature (requires filesystem access).

use crate::tracking::{RemoteName, RemoteTrackingStore, TrackingRecord};
use crate::{NameServiceError, Result};
use async_trait::async_trait;
use std::fmt::Debug;
use std::path::{Path, PathBuf};

/// File-based tracking store.
///
/// Stores tracking state outside the `ns@v2/` tree at:
/// `{base_path}/ns-sync/remotes/{remote}/{address_encoded}.json`
#[derive(Debug)]
pub struct FileTrackingStore {
    base_path: PathBuf,
}

impl FileTrackingStore {
    /// Create a new file-based tracking store.
    ///
    /// `base_path` is the same root used by `FileNameService` — tracking
    /// state goes into `{base_path}/ns-sync/` (outside `ns@v2/`).
    pub fn new(base_path: impl AsRef<Path>) -> Self {
        Self {
            base_path: base_path.as_ref().to_path_buf(),
        }
    }

    /// Directory for a specific remote's tracking files.
    fn remote_dir(&self, remote: &RemoteName) -> PathBuf {
        self.base_path
            .join("ns-sync")
            .join("remotes")
            .join(&remote.0)
    }

    /// Full path to a tracking record file.
    fn record_path(&self, remote: &RemoteName, ledger_id: &str) -> PathBuf {
        self.remote_dir(remote)
            .join(format!("{}.json", encode_address(ledger_id)))
    }
}

/// Percent-encode a ledger ID for use as a filename.
///
/// Encodes `:`, `/`, `\`, and `%` to avoid path traversal and filesystem issues.
fn encode_address(address: &str) -> String {
    let mut encoded = String::with_capacity(address.len());
    for ch in address.chars() {
        match ch {
            '%' => encoded.push_str("%25"),
            ':' => encoded.push_str("%3A"),
            '/' => encoded.push_str("%2F"),
            '\\' => encoded.push_str("%5C"),
            _ => encoded.push(ch),
        }
    }
    encoded
}

/// Decode a percent-encoded address filename back to the original address.
#[cfg(test)]
fn decode_address(encoded: &str) -> String {
    let mut decoded = String::with_capacity(encoded.len());
    let mut chars = encoded.chars();
    while let Some(ch) = chars.next() {
        if ch == '%' {
            let hex: String = chars.by_ref().take(2).collect();
            match hex.as_str() {
                "25" => decoded.push('%'),
                "3A" | "3a" => decoded.push(':'),
                "2F" | "2f" => decoded.push('/'),
                "5C" | "5c" => decoded.push('\\'),
                _ => {
                    decoded.push('%');
                    decoded.push_str(&hex);
                }
            }
        } else {
            decoded.push(ch);
        }
    }
    decoded
}

#[async_trait]
impl RemoteTrackingStore for FileTrackingStore {
    async fn get_tracking(
        &self,
        remote: &RemoteName,
        ledger_id: &str,
    ) -> Result<Option<TrackingRecord>> {
        let path = self.record_path(remote, ledger_id);
        let path_clone = path.clone();
        let parent_span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
            let _guard = parent_span.enter();
            match std::fs::read_to_string(&path_clone) {
                Ok(contents) => {
                    let record: TrackingRecord = serde_json::from_str(&contents)?;
                    Ok(Some(record))
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(NameServiceError::storage(format!(
                    "Failed to read tracking record {}: {}",
                    path_clone.display(),
                    e
                ))),
            }
        })
        .await
        .map_err(|e| NameServiceError::storage(format!("Task join error: {e}")))?
    }

    async fn set_tracking(&self, record: &TrackingRecord) -> Result<()> {
        let path = self.record_path(&record.remote, &record.ledger_id);
        let json = serde_json::to_string_pretty(record)?;
        let parent_span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
            let _guard = parent_span.enter();
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    NameServiceError::storage(format!(
                        "Failed to create tracking directory {}: {}",
                        parent.display(),
                        e
                    ))
                })?;
            }
            // Atomic write via temp file + rename
            let tmp_path = path.with_extension("json.tmp");
            std::fs::write(&tmp_path, json.as_bytes()).map_err(|e| {
                NameServiceError::storage(format!(
                    "Failed to write tracking record {}: {}",
                    tmp_path.display(),
                    e
                ))
            })?;
            std::fs::rename(&tmp_path, &path).map_err(|e| {
                NameServiceError::storage(format!(
                    "Failed to rename tracking record {}: {}",
                    path.display(),
                    e
                ))
            })?;
            Ok(())
        })
        .await
        .map_err(|e| NameServiceError::storage(format!("Task join error: {e}")))?
    }

    async fn list_tracking(&self, remote: &RemoteName) -> Result<Vec<TrackingRecord>> {
        let dir = self.remote_dir(remote);
        let parent_span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
            let _guard = parent_span.enter();
            let entries = match std::fs::read_dir(&dir) {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
                Err(e) => {
                    return Err(NameServiceError::storage(format!(
                        "Failed to list tracking directory {}: {}",
                        dir.display(),
                        e
                    )))
                }
            };

            let mut records = Vec::new();
            for entry in entries {
                let entry = entry.map_err(|e| {
                    NameServiceError::storage(format!("Failed to read directory entry: {e}"))
                })?;
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "json") {
                    // Skip tmp files
                    if path
                        .file_name()
                        .and_then(|f| f.to_str())
                        .is_some_and(|f| f.ends_with(".json.tmp"))
                    {
                        continue;
                    }

                    match std::fs::read_to_string(&path) {
                        Ok(contents) => match serde_json::from_str::<TrackingRecord>(&contents) {
                            Ok(record) => records.push(record),
                            Err(e) => {
                                tracing::warn!(
                                    "Skipping malformed tracking record {}: {}",
                                    path.display(),
                                    e
                                );
                            }
                        },
                        Err(e) => {
                            tracing::warn!(
                                "Failed to read tracking record {}: {}",
                                path.display(),
                                e
                            );
                        }
                    }
                }
            }
            Ok(records)
        })
        .await
        .map_err(|e| NameServiceError::storage(format!("Task join error: {e}")))?
    }

    async fn remove_tracking(&self, remote: &RemoteName, ledger_id: &str) -> Result<()> {
        let path = self.record_path(remote, ledger_id);
        let parent_span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
            let _guard = parent_span.enter();
            match std::fs::remove_file(&path) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()), // idempotent
                Err(e) => Err(NameServiceError::storage(format!(
                    "Failed to remove tracking record {}: {}",
                    path.display(),
                    e
                ))),
            }
        })
        .await
        .map_err(|e| NameServiceError::storage(format!("Task join error: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RefValue;
    use tempfile::TempDir;

    fn origin() -> RemoteName {
        RemoteName::new("origin")
    }

    #[test]
    fn test_encode_decode_address() {
        let address = "mydb:main";
        let encoded = encode_address(address);
        assert_eq!(encoded, "mydb%3Amain");
        assert_eq!(decode_address(&encoded), address);

        let address_with_slash = "org/mydb:main";
        let encoded2 = encode_address(address_with_slash);
        assert_eq!(encoded2, "org%2Fmydb%3Amain");
        assert_eq!(decode_address(&encoded2), address_with_slash);

        // Roundtrip for percent
        let with_percent = "a%b:c";
        let enc = encode_address(with_percent);
        assert_eq!(decode_address(&enc), with_percent);
    }

    #[tokio::test]
    async fn test_file_tracking_get_empty() {
        let tmp = TempDir::new().unwrap();
        let store = FileTrackingStore::new(tmp.path());
        let result = store.get_tracking(&origin(), "mydb:main").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_file_tracking_set_and_get() {
        let tmp = TempDir::new().unwrap();
        let store = FileTrackingStore::new(tmp.path());

        let mut record = TrackingRecord::new(origin(), "mydb:main");
        record.commit_ref = Some(RefValue { id: None, t: 5 });

        store.set_tracking(&record).await.unwrap();

        let fetched = store
            .get_tracking(&origin(), "mydb:main")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.ledger_id, "mydb:main");
        assert_eq!(fetched.commit_ref.as_ref().unwrap().t, 5);
    }

    #[tokio::test]
    async fn test_file_tracking_stored_outside_ns_v2() {
        let tmp = TempDir::new().unwrap();
        let store = FileTrackingStore::new(tmp.path());

        store
            .set_tracking(&TrackingRecord::new(origin(), "mydb:main"))
            .await
            .unwrap();

        // Verify file is under ns-sync/, not ns@v2/
        let expected_path = tmp.path().join("ns-sync/remotes/origin/mydb%3Amain.json");
        assert!(
            expected_path.exists(),
            "File should exist at {expected_path:?}"
        );

        // Verify ns@v2 directory does NOT exist
        assert!(!tmp.path().join("ns@v2").exists());
    }

    #[tokio::test]
    async fn test_file_tracking_list() {
        let tmp = TempDir::new().unwrap();
        let store = FileTrackingStore::new(tmp.path());

        store
            .set_tracking(&TrackingRecord::new(origin(), "db1:main"))
            .await
            .unwrap();
        store
            .set_tracking(&TrackingRecord::new(origin(), "db2:main"))
            .await
            .unwrap();

        let upstream = RemoteName::new("upstream");
        store
            .set_tracking(&TrackingRecord::new(upstream.clone(), "db3:main"))
            .await
            .unwrap();

        let origin_records = store.list_tracking(&origin()).await.unwrap();
        assert_eq!(origin_records.len(), 2);

        let upstream_records = store.list_tracking(&upstream).await.unwrap();
        assert_eq!(upstream_records.len(), 1);
    }

    #[tokio::test]
    async fn test_file_tracking_remove() {
        let tmp = TempDir::new().unwrap();
        let store = FileTrackingStore::new(tmp.path());

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
    async fn test_file_tracking_remove_nonexistent() {
        let tmp = TempDir::new().unwrap();
        let store = FileTrackingStore::new(tmp.path());
        // Should not error
        store
            .remove_tracking(&origin(), "nonexistent:main")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_file_tracking_list_empty_remote() {
        let tmp = TempDir::new().unwrap();
        let store = FileTrackingStore::new(tmp.path());
        let records = store.list_tracking(&origin()).await.unwrap();
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn test_file_tracking_overwrite() {
        let tmp = TempDir::new().unwrap();
        let store = FileTrackingStore::new(tmp.path());

        let mut record = TrackingRecord::new(origin(), "mydb:main");
        record.commit_ref = Some(RefValue { id: None, t: 1 });
        store.set_tracking(&record).await.unwrap();

        record.commit_ref = Some(RefValue { id: None, t: 5 });
        store.set_tracking(&record).await.unwrap();

        let fetched = store
            .get_tracking(&origin(), "mydb:main")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.commit_ref.as_ref().unwrap().t, 5);
    }
}
