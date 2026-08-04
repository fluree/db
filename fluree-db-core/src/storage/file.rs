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

/// Unique suffix source for atomic-write temp files. Two writers storing the same
/// content-addressed blob concurrently is normal and must not have them clobber each
/// other's temp file, so the name carries pid + counter.
static TMP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

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
        let bytes = tokio::fs::read(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                crate::error::Error::not_found(format!("{}: {}", address, path.display()))
            } else {
                crate::error::Error::io(format!("Failed to read {}: {}", path.display(), e))
            }
        })?;
        // A ZERO-LENGTH blob is not content — it is debris, and reporting it as
        // absent is strictly better than returning it.
        //
        // Blobs here are content-addressed, so the address commits to a digest and
        // no real artifact hashes to empty. An empty file at such an address can
        // therefore only be a failed write (create succeeded, write did not — the
        // classic ENOSPC shape, which left ~4,000 of these on one deployment).
        //
        // The distinction matters because the two outcomes are not equally
        // recoverable: "absent" makes callers re-fetch or rebuild, while empty
        // content propagates as a parse failure at some distant call site
        // ("pack header: need 40 bytes, got 0") that no caller knows how to repair.
        if bytes.is_empty() {
            tracing::warn!(
                address,
                path = %path.display(),
                "zero-length blob treated as absent (failed write debris); it will be \
                 re-fetched or rebuilt. Delete it to reclaim the inode."
            );
            return Err(crate::error::Error::not_found(format!(
                "{}: {} (zero-length blob, treated as absent)",
                address,
                path.display()
            )));
        }
        Ok(bytes)
    }

    fn resolve_local_path(&self, address: &str) -> Option<std::path::PathBuf> {
        let path = self.resolve_path(address).ok()?;
        // PRESENCE IS NOT VALIDITY. This returned any path that merely `exists()`,
        // and callers then mmap or parse it directly — so a zero-length blob became
        // an unrecoverable error at the reader instead of a miss that the CAS could
        // heal. Excluding empty files here is what converts that poison back into a
        // fetch. See `read_bytes` for why empty can never be legitimate content.
        match std::fs::metadata(&path) {
            Ok(m) if m.len() > 0 => Some(path),
            Ok(_) => {
                tracing::warn!(
                    address,
                    path = %path.display(),
                    "zero-length blob ignored for local resolution; falling back to fetch"
                );
                None
            }
            Err(_) => None,
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

    fn supports_ranged_reads(&self) -> bool {
        true
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

        // ATOMIC: temp file -> fsync -> rename. Never write in place.
        //
        // This used to be `tokio::fs::write(&path, bytes)`, justified as "overwrites
        // if exists - idempotent for content-addressed". Rewriting identical content
        // IS idempotent — but only if the write COMPLETES. `write` is
        // create+truncate+write, so an abnormal termination mid-write leaves a
        // TRUNCATED blob at the final path, having destroyed the valid one that was
        // there.
        //
        // That is not theoretical. On a production deployment a SIGBUS during a dict
        // upload left a 7,778,304-byte fragment of a ~56 MB pack — a pack that 15
        // already-published index roots referenced. Every subsequent index load of
        // that ledger failed with "page directory at offset 55962372 exceeds pack
        // length 7778304", and because a content-addressed store treats presence as
        // validity, the fragment was permanent: rebuilds found the path occupied and
        // reused it. The same flaw under ENOSPC (create succeeds, write fails) left
        // ~4,000 zero-length blobs across the same deployment earlier the same day.
        //
        // Rename is atomic on POSIX, so a reader sees either the old complete blob or
        // the new complete blob, never a partial one. The `fsync` before rename is
        // what makes that true across a machine crash rather than just a process
        // crash: without it the rename can be durable while the data is not.
        let tmp = path.with_extension(format!(
            "tmp.{}.{}",
            std::process::id(),
            TMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let write_result = async {
            let mut f = tokio::fs::File::create(&tmp).await?;
            tokio::io::AsyncWriteExt::write_all(&mut f, bytes).await?;
            // Durability before visibility.
            f.sync_all().await?;
            drop(f);
            tokio::fs::rename(&tmp, &path).await
        }
        .await;

        match write_result {
            Ok(()) => Ok(()),
            Err(e) => {
                // Leave no partial temp behind; a failed write must not turn into
                // disk that nothing will ever reclaim.
                let _ = tokio::fs::remove_file(&tmp).await;
                Err(crate::error::Error::io(format!(
                    "Failed to write {}: {}",
                    path.display(),
                    e
                )))
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "fluree_filestorage_{tag}_{}_{}",
            std::process::id(),
            TMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// A write must leave NO temp files behind. A leaked temp is disk that nothing
    /// reclaims, on the same volume whose exhaustion started this whole class of bug.
    #[tokio::test]
    async fn atomic_write_leaves_no_temp_files() {
        let dir = tmpdir("notmp");
        let s = FileStorage::new(&dir);
        s.write_bytes("a/b/blob.dict", b"hello").await.unwrap();

        let strays: Vec<_> = walkdir_files(&dir)
            .into_iter()
            .filter(|p| p.to_string_lossy().contains(".tmp."))
            .collect();
        assert!(strays.is_empty(), "temp files left behind: {strays:?}");
        assert_eq!(s.read_bytes("a/b/blob.dict").await.unwrap(), b"hello");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// THE CRASH-SAFETY PROPERTY. Rewriting an existing blob must never be able to
    /// leave it shorter than it was: the new content lands via rename, so a reader
    /// sees the old complete blob or the new complete blob. Verified by checking the
    /// final path is never truncated mid-write — approximated here by asserting the
    /// destination is only ever the full old or full new content, and that a
    /// same-content rewrite (the idempotent case that justified the old in-place
    /// write) still works.
    #[tokio::test]
    async fn rewriting_an_existing_blob_never_shortens_it() {
        let dir = tmpdir("rewrite");
        let s = FileStorage::new(&dir);
        let big = vec![b'x'; 200_000];

        s.write_bytes("p/big.dict", &big).await.unwrap();
        assert_eq!(s.read_bytes("p/big.dict").await.unwrap().len(), 200_000);

        // Idempotent rewrite of identical content — the case the old code was
        // justified by. Must still be a no-op from the reader's point of view.
        s.write_bytes("p/big.dict", &big).await.unwrap();
        assert_eq!(s.read_bytes("p/big.dict").await.unwrap().len(), 200_000);

        // A shorter write is a *different* blob at a different address in real use;
        // if it ever targets the same path, the result must be complete, not spliced.
        s.write_bytes("p/big.dict", b"short").await.unwrap();
        assert_eq!(s.read_bytes("p/big.dict").await.unwrap(), b"short");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A zero-length blob must read as ABSENT, not as empty content. This is the
    /// ENOSPC debris case: ~4,000 such files survived one outage and turned every
    /// later read into "pack header: need 40 bytes, got 0" — a parse failure no
    /// caller can repair, where a miss would have been re-fetched.
    #[tokio::test]
    async fn zero_length_blob_reads_as_absent() {
        let dir = tmpdir("empty");
        let s = FileStorage::new(&dir);
        // Write the fixture at the RESOLVED path: a bare address gets `.json`
        // appended (see `resolve_path`), so constructing the path by hand would test
        // a file the reader never looks at — which is exactly what it did first time.
        let path = s.resolve_path("z/empty.dict").unwrap();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"").unwrap();

        let err = s
            .read_bytes("z/empty.dict")
            .await
            .expect_err("must not succeed");
        assert!(
            format!("{err}").contains("zero-length"),
            "error should say why it is absent, got: {err}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `resolve_local_path` must not hand out a zero-length blob. Callers mmap or
    /// parse that path directly, so returning it converts recoverable debris into an
    /// unrecoverable reader error — this is the `exists()`-is-validity flaw.
    #[test]
    fn resolve_local_path_rejects_a_zero_length_blob() {
        let dir = tmpdir("resolve");
        let s = FileStorage::new(&dir);

        let good = s.resolve_path("g/ok.dict").unwrap();
        std::fs::create_dir_all(good.parent().unwrap()).unwrap();
        std::fs::write(&good, b"content").unwrap();
        assert!(
            s.resolve_local_path("g/ok.dict").is_some(),
            "valid blob must resolve"
        );

        let empty = s.resolve_path("g/empty.dict").unwrap();
        std::fs::write(&empty, b"").unwrap();
        assert!(
            s.resolve_local_path("g/empty.dict").is_none(),
            "zero-length blob must NOT resolve as a local path"
        );

        assert!(s.resolve_local_path("g/missing.dict").is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn walkdir_files(root: &std::path::Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(d) = stack.pop() {
            let Ok(rd) = std::fs::read_dir(&d) else {
                continue;
            };
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else {
                    out.push(p);
                }
            }
        }
        out
    }
}
