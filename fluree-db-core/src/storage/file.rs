//! Filesystem storage backend (requires the `native` feature).
//!
//! Provides [`FileStorage`], which stores ledger data on the local filesystem
//! using `tokio::fs` for async I/O. This module is only compiled on non-WASM
//! targets with the `native` feature enabled.

use crate::error::Result;
use crate::{
    content_address, CasAction, CasOutcome, ContentAddressedWrite, ContentKind, ContentWriteResult,
    StorageCas, StorageExtError, StorageExtResult, StorageMethod, StorageRead, StorageWrite,
};
use async_trait::async_trait;
use std::path::PathBuf;

/// Storage method for local filesystem storage.
pub const STORAGE_METHOD_FILE: &str = "file";

/// File-based storage backed by `tokio::fs`.
#[derive(Debug, Clone)]
pub struct FileStorage {
    /// Base directory for index files
    base_path: std::path::PathBuf,
}

impl FileStorage {
    /// Create a new file storage with the given base path
    ///
    /// The base path should be the ledger's data directory containing the ledger
    /// subdirectories (e.g. `mydb/main/index/...`).
    pub fn new(base_path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            base_path: base_path.into(),
        }
    }

    /// Get the base path for this storage
    pub fn base_path(&self) -> &std::path::Path {
        &self.base_path
    }

    /// Extract the path portion from a Fluree address.
    ///
    /// Handles formats like:
    /// - `fluree:file://path/to/file.json` -> `Some("path/to/file.json")`
    /// - `fluree:memory://path/to/file.json` -> `Some("path/to/file.json")`
    /// - `raw/path` -> `None` (not a fluree address)
    fn extract_path_from_address(address: &str) -> Option<&str> {
        if let Some(path) = address.strip_prefix("fluree:file://") {
            return Some(path);
        }
        if address.starts_with("fluree:") {
            if let Some(path_start) = address.find("://") {
                return Some(&address[path_start + 3..]);
            }
        }
        None
    }

    /// Resolve an address to a file path
    ///
    /// Handles both raw file paths and Fluree address format.
    /// Address format: `fluree:file://path/to/file.json`
    fn resolve_path(&self, address: &str) -> Result<std::path::PathBuf> {
        if let Some(path) = Self::extract_path_from_address(address) {
            return self.resolve_relative_path(path);
        }
        // Simple case: just a node ID, look for it as a .json file
        self.resolve_relative_path(&format!("{address}.json"))
    }

    fn resolve_relative_path(&self, path: &str) -> Result<std::path::PathBuf> {
        use std::path::Component;
        let p = std::path::Path::new(path);

        // Disallow absolute paths and path traversal.
        if p.is_absolute()
            || p.components().any(|c| {
                matches!(
                    c,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(crate::error::Error::storage(format!(
                "Invalid storage path '{path}': must be a relative path without '..'"
            )));
        }

        Ok(self.base_path.join(p))
    }
}

#[async_trait]
impl StorageRead for FileStorage {
    async fn read_bytes(&self, address: &str) -> Result<Vec<u8>> {
        let path = self.resolve_path(address)?;
        tokio::fs::read(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                crate::error::Error::not_found(format!("{}: {}", address, path.display()))
            } else {
                crate::error::Error::io(format!("Failed to read {}: {}", path.display(), e))
            }
        })
    }

    fn resolve_local_path(&self, address: &str) -> Option<std::path::PathBuf> {
        let path = self.resolve_path(address).ok()?;
        if path.exists() {
            Some(path)
        } else {
            None
        }
    }

    async fn read_byte_range(&self, address: &str, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
        let path = self.resolve_path(address)?;
        if range.end <= range.start {
            return Ok(Vec::new());
        }
        let len = (range.end - range.start) as usize;
        let offset = range.start;
        let address = address.to_owned();
        tokio::task::spawn_blocking(move || {
            let mut buf = vec![0u8; len];
            let file = std::fs::File::open(&path).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    crate::error::Error::not_found(format!("{}: {}", address, path.display()))
                } else {
                    crate::error::Error::io(format!("Failed to open {}: {}", path.display(), e))
                }
            })?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                let mut total = 0;
                while total < len {
                    let n = file
                        .read_at(&mut buf[total..], offset + total as u64)
                        .map_err(|e| {
                            crate::error::Error::io(format!(
                                "Failed to read range from {}: {}",
                                path.display(),
                                e
                            ))
                        })?;
                    if n == 0 {
                        break; // EOF
                    }
                    total += n;
                }
                buf.truncate(total);
            }
            #[cfg(not(unix))]
            {
                use std::io::{Read, Seek, SeekFrom};
                let mut file = file;
                file.seek(SeekFrom::Start(offset)).map_err(|e| {
                    crate::error::Error::io(format!("Failed to seek {}: {}", path.display(), e))
                })?;
                let mut total = 0;
                while total < len {
                    let n = file.read(&mut buf[total..]).map_err(|e| {
                        crate::error::Error::io(format!(
                            "Failed to read range from {}: {}",
                            path.display(),
                            e
                        ))
                    })?;
                    if n == 0 {
                        break; // EOF
                    }
                    total += n;
                }
                buf.truncate(total);
            }
            Ok(buf)
        })
        .await
        .map_err(|e| crate::error::Error::io(format!("spawn_blocking failed: {e}")))?
    }

    async fn exists(&self, address: &str) -> Result<bool> {
        let path = self.resolve_path(address)?;
        match tokio::fs::metadata(&path).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(crate::error::Error::io(format!(
                "Failed to stat {}: {}",
                path.display(),
                e
            ))),
        }
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        // Extract the path from the prefix (handle fluree:file:// format)
        let path_prefix = Self::extract_path_from_address(prefix).unwrap_or(prefix);

        // Get the directory to list from and the file prefix to match
        let full_path = self.base_path.join(path_prefix);
        let (list_dir, file_prefix) = if full_path.is_dir() {
            (full_path, String::new())
        } else {
            // The prefix might be a partial filename, so list the parent
            let parent = full_path.parent().unwrap_or(&self.base_path);
            let file_part = full_path
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default();
            (parent.to_path_buf(), file_part)
        };

        // Check if directory exists
        if !list_dir.exists() {
            return Ok(Vec::new());
        }

        // Walk directory recursively
        let mut results = Vec::new();
        let mut dirs_to_visit = vec![list_dir.clone()];

        while let Some(dir) = dirs_to_visit.pop() {
            let mut entries = match tokio::fs::read_dir(&dir).await {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => {
                    return Err(crate::error::Error::io(format!(
                        "Failed to list {}: {}",
                        dir.display(),
                        e
                    )));
                }
            };

            while let Some(entry) = entries.next_entry().await.map_err(|e| {
                crate::error::Error::io(format!("Failed to read entry in {}: {}", dir.display(), e))
            })? {
                let path = entry.path();
                let file_type = entry.file_type().await.map_err(|e| {
                    crate::error::Error::io(format!(
                        "Failed to get file type for {}: {}",
                        path.display(),
                        e
                    ))
                })?;

                if file_type.is_dir() {
                    dirs_to_visit.push(path);
                } else if file_type.is_file() {
                    // Convert back to relative path from base
                    if let Ok(relative) = path.strip_prefix(&self.base_path) {
                        let relative_str = relative.to_string_lossy().to_string();
                        // Check if it matches the file prefix (if any)
                        if file_prefix.is_empty() || relative_str.starts_with(path_prefix) {
                            // Return as fluree:file:// address
                            results.push(format!("fluree:file://{relative_str}"));
                        }
                    }
                }
            }
        }

        Ok(results)
    }
}

#[async_trait]
impl StorageWrite for FileStorage {
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> Result<()> {
        let path = self.resolve_path(address)?;

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                crate::error::Error::io(format!(
                    "Failed to create directory {}: {}",
                    parent.display(),
                    e
                ))
            })?;
        }

        // Write file (overwrites if exists - idempotent for content-addressed)
        tokio::fs::write(&path, bytes).await.map_err(|e| {
            crate::error::Error::io(format!("Failed to write {}: {}", path.display(), e))
        })
    }

    async fn delete(&self, address: &str) -> Result<()> {
        let path = self.resolve_path(address)?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            // Idempotent: not found is OK
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(crate::error::Error::io(format!(
                "Failed to delete {}: {}",
                path.display(),
                e
            ))),
        }
    }
}

impl StorageMethod for FileStorage {
    fn storage_method(&self) -> &str {
        STORAGE_METHOD_FILE
    }
}

#[async_trait]
impl ContentAddressedWrite for FileStorage {
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> Result<ContentWriteResult> {
        let address = content_address(STORAGE_METHOD_FILE, kind, ledger_id, content_hash_hex);
        self.write_bytes(&address, bytes).await?;
        Ok(ContentWriteResult {
            address,
            content_hash: content_hash_hex.to_string(),
            size_bytes: bytes.len(),
        })
    }
}

impl FileStorage {
    /// Atomic file insert inside `spawn_blocking`.
    ///
    /// Uses `O_CREAT | O_EXCL` for atomic create-if-not-exists.
    async fn blocking_insert(&self, path: PathBuf, bytes: Vec<u8>) -> StorageExtResult<bool> {
        tokio::task::spawn_blocking(move || {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    StorageExtError::io(format!("mkdir {}: {}", parent.display(), e))
                })?;
            }

            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(mut file) => {
                    use std::io::Write;
                    file.write_all(&bytes).map_err(|e| {
                        StorageExtError::io(format!("write {}: {}", path.display(), e))
                    })?;
                    Ok(true)
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
                Err(e) => Err(StorageExtError::io(format!(
                    "open {}: {}",
                    path.display(),
                    e
                ))),
            }
        })
        .await
        .map_err(|e| StorageExtError::io(format!("spawn_blocking join: {e}")))?
    }

    /// Atomic locked read inside `spawn_blocking`.
    ///
    /// Acquires an exclusive flock on a sidecar `.lock` file, reads the data
    /// file, and returns the current bytes. The lock is held across the
    /// returned guard so the caller can write back atomically.
    ///
    /// Returns `(current_bytes, lock_guard_and_path)` — drop the second
    /// element to release the lock.
    async fn blocking_locked_read(
        &self,
        path: PathBuf,
    ) -> StorageExtResult<(Option<Vec<u8>>, LockedFile)> {
        tokio::task::spawn_blocking(move || {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    StorageExtError::io(format!("mkdir {}: {}", parent.display(), e))
                })?;
            }

            // Use a separate lock file so that the atomic rename of the data
            // file doesn't invalidate the lock (rename replaces the directory
            // entry, creating a new inode on Linux — the lock on the old inode
            // would no longer protect the new file).
            let lock_path = path.with_extension("lock");
            let lock_file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&lock_path)
                .map_err(|e| {
                    StorageExtError::io(format!("open lock {}: {}", lock_path.display(), e))
                })?;

            fs2::FileExt::lock_exclusive(&lock_file)
                .map_err(|e| StorageExtError::io(format!("lock {}: {}", lock_path.display(), e)))?;

            let current = match std::fs::read(&path) {
                Ok(buf) if buf.is_empty() => None,
                Ok(buf) => Some(buf),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(e) => {
                    return Err(StorageExtError::io(format!(
                        "read {}: {}",
                        path.display(),
                        e
                    )))
                }
            };

            Ok((
                current,
                LockedFile {
                    path,
                    _lock_file: lock_file,
                },
            ))
        })
        .await
        .map_err(|e| StorageExtError::io(format!("spawn_blocking join: {e}")))?
    }

    /// Atomic locked write inside `spawn_blocking`.
    ///
    /// Writes `new_bytes` to a temp file and renames into place while the
    /// flock from `blocking_locked_read` is still held. The lock is released
    /// when the `LockedFile` guard is dropped at the end.
    async fn blocking_locked_write(
        &self,
        locked: LockedFile,
        new_bytes: Vec<u8>,
    ) -> StorageExtResult<()> {
        tokio::task::spawn_blocking(move || {
            let tmp_path = locked.path.with_extension("tmp");
            {
                use std::io::Write;
                let mut tmp = std::fs::File::create(&tmp_path).map_err(|e| {
                    StorageExtError::io(format!("create {}: {}", tmp_path.display(), e))
                })?;
                tmp.write_all(&new_bytes).map_err(|e| {
                    StorageExtError::io(format!("write {}: {}", tmp_path.display(), e))
                })?;
            }
            std::fs::rename(&tmp_path, &locked.path).map_err(|e| {
                StorageExtError::io(format!(
                    "rename {} -> {}: {}",
                    tmp_path.display(),
                    locked.path.display(),
                    e
                ))
            })?;
            Ok(())
            // lock released when `locked._lock_file` is dropped
        })
        .await
        .map_err(|e| StorageExtError::io(format!("spawn_blocking join: {e}")))?
    }
}

/// Holds an exclusive flock and the data file path for the duration of a CAS.
///
/// The lock is released when this struct is dropped (the `_lock_file` field's
/// `Drop` impl calls `flock(LOCK_UN)`).
struct LockedFile {
    path: PathBuf,
    _lock_file: std::fs::File,
}

#[async_trait]
impl StorageCas for FileStorage {
    async fn insert(&self, address: &str, bytes: &[u8]) -> StorageExtResult<bool> {
        let path = self
            .resolve_path(address)
            .map_err(|e| StorageExtError::io(e.to_string()))?;
        self.blocking_insert(path, bytes.to_vec()).await
    }

    async fn compare_and_swap<T, F>(&self, address: &str, f: F) -> StorageExtResult<CasOutcome<T>>
    where
        F: Fn(Option<&[u8]>) -> std::result::Result<CasAction<T>, StorageExtError> + Send + Sync,
        T: Send,
    {
        let path = self
            .resolve_path(address)
            .map_err(|e| StorageExtError::io(e.to_string()))?;

        // Phase 1: acquire lock + read (blocking)
        let (current, locked) = self.blocking_locked_read(path).await?;

        // Phase 2: call closure on async task
        match f(current.as_deref())? {
            CasAction::Write(new_bytes) => {
                // Phase 3: write under same lock (blocking)
                self.blocking_locked_write(locked, new_bytes).await?;
                Ok(CasOutcome::Written)
            }
            CasAction::Abort(t) => Ok(CasOutcome::Aborted(t)),
        }
        // Lock released when `locked` is dropped (on Abort path, dropped here)
    }
}
