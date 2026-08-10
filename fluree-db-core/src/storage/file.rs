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
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub use super::Durability;

/// Storage method for local filesystem storage.
pub const STORAGE_METHOD_FILE: &str = "file";

/// Suffix marking a staging file left by an interrupted atomic write.
const TMP_SUFFIX: &str = ".tmp";

/// How one write is flushed, and where it reports the flushes it issued.
///
/// The counter is what makes the durability setting *observable*. A flushed
/// write and an unflushed one leave byte-identical files behind, so no
/// assertion about a write's outcome can tell them apart; without a count,
/// removing the fsync is undetectable from outside the process.
#[derive(Debug, Clone)]
struct WritePolicy {
    durability: Durability,
    fsyncs: Arc<AtomicU64>,
}

impl WritePolicy {
    fn syncs(&self) -> bool {
        self.durability.syncs()
    }

    /// Record one device flush. Relaxed: the count is a diagnostic, and it is
    /// ordered by the syscall it follows anyway.
    fn record_fsync(&self) {
        self.fsyncs.fetch_add(1, Ordering::Relaxed);
    }
}

/// fsync the directory holding `path` so the rename or link that put the file
/// there survives power loss.
///
/// Unix-only: Windows exposes no equivalent, so the call is skipped and the
/// weaker guarantee accepted rather than failing the write. Mirrors
/// `fluree-db-consensus/src/raft/storage/fs.rs`.
fn fsync_parent_dir(path: &Path, policy: &WritePolicy) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        if let Some(parent) = path.parent() {
            std::fs::File::open(parent)?.sync_all()?;
            policy.record_fsync();
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (path, policy);
    }
    Ok(())
}

/// Distinguishes staging files from content within one process. Writers to the
/// same address are not always serialized (`write_bytes` takes no lock), so a
/// fixed staging name would let two writers clobber each other's partial file
/// and rename the result into place.
static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Staging path alongside `path`, unique per process and per call.
///
/// Appends rather than replacing the extension so `foo.json` stages as
/// `foo.json.<pid>.<seq>.tmp`, keeping the final name recoverable by eye and
/// leaving multi-part extensions intact.
fn tmp_sibling(path: &Path) -> PathBuf {
    let seq = TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".{}.{seq}{TMP_SUFFIX}", std::process::id()));
    path.with_file_name(name)
}

/// True for a staging file left behind by an interrupted write.
fn is_tmp_artifact(name: &str) -> bool {
    name.ends_with(TMP_SUFFIX)
}

/// Write `bytes` to a staging sibling of `path`, returning the staging path.
///
/// Under [`Durability::Sync`] the contents are flushed before returning, so a
/// caller that then makes the file visible has its bytes on the device first.
/// The staging file is removed if any step fails, leaving nothing behind for
/// `list_prefix` or a later reader to find.
fn stage_bytes(path: &Path, bytes: &[u8], policy: &WritePolicy) -> std::io::Result<PathBuf> {
    use std::io::Write;

    let tmp = tmp_sibling(path);
    let staged = (|| {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(bytes)?;
        if policy.syncs() {
            file.sync_all()?;
            policy.record_fsync();
        }
        Ok(())
    })();
    if let Err(e) = staged {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    Ok(tmp)
}

/// Stage `bytes` and rename them onto `path`.
///
/// A concurrent reader of `path` observes either the previous contents or the
/// complete new contents; the final name is never a partially written file.
///
/// The rename gives `path` a new inode, so ownership, mode, ACLs and hard
/// links applied to the destination path do not survive a write. Documented
/// alongside the durability setting in `docs/operations/storage.md`.
fn write_atomic(path: &Path, bytes: &[u8], policy: &WritePolicy) -> std::io::Result<()> {
    let tmp = stage_bytes(path, bytes, policy)?;
    if let Err(e) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    if policy.syncs() {
        fsync_parent_dir(path, policy)?;
    }
    Ok(())
}

/// True for the errors a filesystem returns when it has no hard links at all.
///
/// exFAT, several FUSE filesystems and some NFS configurations refuse
/// `link(2)` outright, with `EPERM` or `EOPNOTSUPP`. `O_EXCL` works
/// everywhere, so those mounts get the create-if-absent guarantee back through
/// the fallback below.
fn rejects_hard_links(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::Unsupported
    )
}

/// Stage `bytes` and link them onto `path` only if `path` is absent.
///
/// Returns `false` when `path` already exists, leaving it untouched. Uses
/// `hard_link` rather than `rename` because `rename` would replace an existing
/// file, and the create-if-absent answer is what callers use to detect a
/// duplicate ledger.
fn create_new_atomic(path: &Path, bytes: &[u8], policy: &WritePolicy) -> std::io::Result<bool> {
    let tmp = stage_bytes(path, bytes, policy)?;
    let created = match std::fs::hard_link(&tmp, path) {
        Ok(()) => true,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => false,
        // No hard links on this mount. Fall back to `O_CREAT|O_EXCL`, which
        // keeps create-if-absent correct at the cost of the staged file's
        // atomicity — a reader can catch this one mid-write. That is the
        // pre-staging behaviour, so it is a floor, not a regression.
        Err(e) if rejects_hard_links(&e) => {
            let _ = std::fs::remove_file(&tmp);
            return create_new_in_place(path, bytes, policy);
        }
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
    };
    let _ = std::fs::remove_file(&tmp);
    // The unlink of the staging entry rides along on the same directory fsync.
    if created && policy.syncs() {
        fsync_parent_dir(path, policy)?;
    }
    Ok(created)
}

/// Create-if-absent without a staging file, for mounts that refuse `link(2)`.
fn create_new_in_place(path: &Path, bytes: &[u8], policy: &WritePolicy) -> std::io::Result<bool> {
    use std::io::Write;

    let mut file = match std::fs::File::create_new(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(false),
        Err(e) => return Err(e),
    };
    file.write_all(bytes)?;
    if policy.syncs() {
        file.sync_all()?;
        policy.record_fsync();
        fsync_parent_dir(path, policy)?;
    }
    Ok(true)
}

/// File-based storage backed by `tokio::fs`.
#[derive(Debug, Clone)]
pub struct FileStorage {
    /// Base directory for index files
    base_path: std::path::PathBuf,
    /// When a write is reported complete. Applies to source-of-truth content;
    /// derived content is written [`Durability::PageCache`] regardless, since
    /// it can be rebuilt from the commit chain.
    durability: Durability,
    /// Device flushes issued so far. Shared across clones, which address the
    /// same directory and so are the same storage. See [`Self::fsyncs_issued`].
    fsyncs: Arc<AtomicU64>,
}

impl FileStorage {
    /// Create a new file storage with the given base path
    ///
    /// The base path should be the ledger's data directory containing the ledger
    /// subdirectories (e.g. `mydb/main/index/...`).
    ///
    /// Durability defaults to [`Durability::Sync`], overridable for this
    /// process by [`Durability::ENV_VAR`] or per instance by
    /// [`Self::with_durability`].
    pub fn new(base_path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            base_path: base_path.into(),
            durability: Durability::from_env(),
            fsyncs: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Set when writes are reported complete.
    pub fn with_durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }

    /// When writes to this storage are reported complete.
    pub fn durability(&self) -> Durability {
        self.durability
    }

    /// Device flushes issued by this storage since it was constructed, counting
    /// both the staged file and its parent directory.
    ///
    /// Stays at zero under [`Durability::PageCache`] and for derived content in
    /// either mode. Exposed because a flush leaves no trace in the bytes on
    /// disk, so this is the only way to tell a durable write from a cheap one.
    pub fn fsyncs_issued(&self) -> u64 {
        self.fsyncs.load(Ordering::Relaxed)
    }

    /// Durability for a write of `kind`.
    ///
    /// Derived content is recomputable from the commit chain, so it is never
    /// worth an fsync — that keeps index builds off the sync path even when the
    /// ledger's own writes are durable.
    fn durability_for(&self, kind: ContentKind) -> Durability {
        if kind.is_derived() {
            Durability::PageCache
        } else {
            self.durability
        }
    }

    /// Write policy for a given durability, reporting flushes to this storage.
    fn policy(&self, durability: Durability) -> WritePolicy {
        WritePolicy {
            durability,
            fsyncs: Arc::clone(&self.fsyncs),
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
                    // A staging file left by an interrupted write is not
                    // content and must not be handed out as an address.
                    if is_tmp_artifact(&entry.file_name().to_string_lossy()) {
                        continue;
                    }
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
        self.write_bytes_durable(address, bytes, self.durability)
            .await
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
        self.write_bytes_durable(&address, bytes, self.durability_for(kind))
            .await?;
        Ok(ContentWriteResult {
            address,
            content_hash: content_hash_hex.to_string(),
            size_bytes: bytes.len(),
        })
    }
}

impl FileStorage {
    /// `write_bytes` with an explicit durability, so a content write can pick
    /// one from its [`ContentKind`].
    async fn write_bytes_durable(
        &self,
        address: &str,
        bytes: &[u8],
        durability: Durability,
    ) -> Result<()> {
        let path = self.resolve_path(address)?;
        let bytes = bytes.to_vec();
        let for_err = path.clone();
        let policy = self.policy(durability);

        // One blocking hop for mkdir + stage + rename, rather than one per
        // `tokio::fs` call.
        tokio::task::spawn_blocking(move || {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    crate::error::Error::io(format!(
                        "Failed to create directory {}: {}",
                        parent.display(),
                        e
                    ))
                })?;
            }
            // Overwrites if present, which is idempotent for content-addressed
            // writes: the address is derived from these bytes.
            write_atomic(&path, &bytes, &policy).map_err(|e| {
                crate::error::Error::io(format!("Failed to write {}: {}", path.display(), e))
            })
        })
        .await
        .map_err(|e| crate::error::Error::io(format!("write {} join: {e}", for_err.display())))?
    }

    /// Create-if-absent file insert inside `spawn_blocking`.
    ///
    /// Stages the bytes and links them into place, so a caller that observes
    /// the file sees it complete.
    async fn blocking_insert(&self, path: PathBuf, bytes: Vec<u8>) -> StorageExtResult<bool> {
        let policy = self.policy(self.durability);
        tokio::task::spawn_blocking(move || {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    StorageExtError::io(format!("mkdir {}: {}", parent.display(), e))
                })?;
            }

            create_new_atomic(&path, &bytes, &policy)
                .map_err(|e| StorageExtError::io(format!("write {}: {}", path.display(), e)))
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
        let policy = self.policy(self.durability);
        tokio::task::spawn_blocking(move || {
            write_atomic(&locked.path, &new_bytes, &policy)
                .map_err(|e| StorageExtError::io(format!("write {}: {}", locked.path.display(), e)))
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

    fn storage() -> (tempfile::TempDir, FileStorage) {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = FileStorage::new(dir.path());
        (dir, storage)
    }

    /// A ledger that reports a commit written must not lose it to power loss,
    /// so the safe setting is the one you get without asking. Asserted on the
    /// parse, not on a constructed storage, so the test does not depend on the
    /// environment it runs in.
    #[test]
    fn durability_defaults_to_sync() {
        assert_eq!(Durability::default(), Durability::Sync);
        assert_eq!(Durability::parse(None), Durability::Sync);
    }

    #[test]
    fn durability_env_opts_out_on_falsey_spellings() {
        for v in ["0", "false", "off", "no", "OFF", " false "] {
            assert_eq!(Durability::parse(Some(v)), Durability::PageCache, "{v:?}");
        }
        // Anything else keeps the safe setting rather than guessing.
        for v in ["1", "true", "on", "", "nonsense"] {
            assert_eq!(Durability::parse(Some(v)), Durability::Sync, "{v:?}");
        }
    }

    /// Environment beats configuration beats the default, so an operator can
    /// override a checked-in config file for one run without editing it.
    #[test]
    fn durability_precedence_is_env_then_config_then_default() {
        use Durability::{PageCache, Sync};
        assert_eq!(Durability::resolve_from(None, None), Sync);
        assert_eq!(Durability::resolve_from(None, Some(PageCache)), PageCache);
        assert_eq!(Durability::resolve_from(Some(Sync), Some(PageCache)), Sync);
        assert_eq!(
            Durability::resolve_from(Some(PageCache), Some(Sync)),
            PageCache
        );
    }

    #[test]
    fn durability_mode_names_parse_and_reject() {
        use Durability::{PageCache, Sync};
        assert_eq!(Durability::from_mode_name("sync"), Some(Sync));
        assert_eq!(Durability::from_mode_name(" SYNC "), Some(Sync));
        assert_eq!(Durability::from_mode_name("page-cache"), Some(PageCache));
        assert_eq!(Durability::from_mode_name("page_cache"), Some(PageCache));
        // Unrecognized must be rejected, not defaulted — a typo in a config
        // file should fail loudly rather than pick a durability silently.
        assert_eq!(Durability::from_mode_name("eventually"), None);
        assert_eq!(Durability::from_mode_name(""), None);
    }

    #[test]
    fn with_durability_overrides_the_default() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            FileStorage::new(dir.path())
                .with_durability(Durability::PageCache)
                .durability(),
            Durability::PageCache
        );
    }

    /// Index builds write far more objects than commits do; paying an fsync per
    /// index node would put the sync cost on the path that can least afford it,
    /// for content a rebuild reproduces.
    #[test]
    fn derived_content_never_syncs() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::Sync);

        for kind in [
            ContentKind::IndexRoot,
            ContentKind::IndexBranch,
            ContentKind::IndexLeaf,
            ContentKind::StatsSketch,
            ContentKind::HistorySidecar,
        ] {
            assert_eq!(
                storage.durability_for(kind),
                Durability::PageCache,
                "{kind:?} is derived"
            );
        }
    }

    #[test]
    fn source_of_truth_content_follows_the_configured_durability() {
        let dir = tempfile::tempdir().unwrap();
        for mode in [Durability::Sync, Durability::PageCache] {
            let storage = FileStorage::new(dir.path()).with_durability(mode);
            for kind in [ContentKind::Commit, ContentKind::Txn] {
                assert_eq!(storage.durability_for(kind), mode, "{kind:?}");
            }
        }
    }

    /// The destination must never be opened for truncation: a rename replaces
    /// the inode, an in-place write reuses it. Asserted on the shape of the
    /// write because its *outcome* is identical either way — the bytes on disk
    /// cannot distinguish a staged-and-renamed write from `fs::write`.
    #[cfg(unix)]
    #[tokio::test]
    async fn write_bytes_lands_via_rename_not_in_place() {
        use std::os::unix::fs::MetadataExt;
        let (_dir, storage) = storage();

        storage
            .write_bytes("k.json", &vec![b'a'; 4096])
            .await
            .unwrap();
        let path = storage.resolve_path("k.json").unwrap();
        let before = std::fs::metadata(&path).unwrap().ino();

        storage
            .write_bytes("k.json", &vec![b'z'; 4096])
            .await
            .unwrap();
        assert_ne!(
            before,
            std::fs::metadata(&path).unwrap().ino(),
            "blob was written in place, not staged and renamed"
        );
    }

    /// The CAS write-back goes through the same staging path, so a reader
    /// racing a nameservice head update never sees a half-written ref.
    #[cfg(unix)]
    #[tokio::test]
    async fn compare_and_swap_lands_via_rename_not_in_place() {
        use std::os::unix::fs::MetadataExt;
        let (_dir, storage) = storage();
        storage.insert("h.json", b"v0").await.unwrap();
        let path = storage.resolve_path("h.json").unwrap();
        let before = std::fs::metadata(&path).unwrap().ino();

        let outcome: CasOutcome<()> = storage
            .compare_and_swap("h.json", |_| Ok(CasAction::Write(b"v1".to_vec())))
            .await
            .unwrap();

        assert!(matches!(outcome, CasOutcome::Written));
        assert_ne!(
            before,
            std::fs::metadata(&path).unwrap().ino(),
            "CAS wrote in place, not staged and renamed"
        );
    }

    /// A flush leaves no trace in the bytes on disk, so the count is the only
    /// evidence the setting was consulted at all.
    #[tokio::test]
    async fn sync_mode_flushes_source_of_truth_writes_to_the_device() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::Sync);

        storage.write_bytes("k.json", b"v").await.unwrap();
        let after_write = storage.fsyncs_issued();
        assert!(after_write > 0, "durable write issued no fsync");

        storage.insert("n.json", b"a").await.unwrap();
        assert!(
            storage.fsyncs_issued() > after_write,
            "durable insert issued no fsync"
        );
    }

    #[tokio::test]
    async fn page_cache_mode_issues_no_flush() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::PageCache);

        storage.write_bytes("k.json", b"v").await.unwrap();
        storage.insert("n.json", b"a").await.unwrap();
        let _: CasOutcome<()> = storage
            .compare_and_swap("h.json", |_| Ok(CasAction::Write(b"v1".to_vec())))
            .await
            .unwrap();

        assert_eq!(
            storage.fsyncs_issued(),
            0,
            "page-cache mode reached the device"
        );
    }

    /// The classification has to reach the write, not just `durability_for`:
    /// an index build that fsynced every node would pay the sync cost on the
    /// path that can least afford it, for content a rebuild reproduces.
    #[tokio::test]
    async fn derived_content_skips_the_flush_on_the_write_path() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::Sync);

        storage
            .content_write_bytes(ContentKind::IndexLeaf, "mydb:main", b"leaf")
            .await
            .unwrap();
        assert_eq!(storage.fsyncs_issued(), 0, "derived content was flushed");

        storage
            .content_write_bytes(ContentKind::Commit, "mydb:main", b"commit")
            .await
            .unwrap();
        assert!(
            storage.fsyncs_issued() > 0,
            "source-of-truth content was not flushed"
        );
    }

    /// Mounts that refuse `link(2)` fall back to `O_CREAT|O_EXCL`, which has to
    /// give the same create-if-absent answer — that answer is how a duplicate
    /// ledger is detected.
    #[tokio::test]
    async fn create_new_in_place_matches_the_hard_link_answer() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::Sync);
        let policy = storage.policy(Durability::Sync);
        let path = dir.path().join("led.json");

        assert!(create_new_in_place(&path, b"first", &policy).unwrap());
        assert!(!create_new_in_place(&path, b"second", &policy).unwrap());
        assert_eq!(std::fs::read(&path).unwrap(), b"first");
        assert!(storage.fsyncs_issued() > 0, "fallback create did not flush");
    }

    /// Both settings stage and rename; they differ only in what is flushed.
    #[tokio::test]
    async fn page_cache_mode_still_writes_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::PageCache);

        storage.write_bytes("k.json", b"v").await.unwrap();
        assert_eq!(storage.read_bytes("k.json").await.unwrap(), b"v");
        assert!(storage.insert("n.json", b"a").await.unwrap());
        assert!(!storage.insert("n.json", b"b").await.unwrap());

        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .filter(|n| is_tmp_artifact(n))
            .collect();
        assert!(leftovers.is_empty(), "staging files left: {leftovers:?}");
    }

    /// Staging names append to the full file name so a multi-part extension
    /// survives; `with_extension` would have turned `a.json.gz` into `a.json`.
    #[test]
    fn tmp_sibling_appends_to_the_full_file_name() {
        let tmp = tmp_sibling(Path::new("/data/a.json.gz"));
        let name = tmp.file_name().unwrap().to_string_lossy().to_string();
        assert!(name.starts_with("a.json.gz."), "got {name}");
        assert!(is_tmp_artifact(&name), "got {name}");
        assert_eq!(tmp.parent(), Some(Path::new("/data")));
    }

    /// Two staging paths for one address never collide, which is what lets
    /// unsynchronized writers to the same address stage concurrently.
    #[test]
    fn tmp_sibling_is_unique_per_call() {
        let a = tmp_sibling(Path::new("/data/x"));
        let b = tmp_sibling(Path::new("/data/x"));
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn write_bytes_leaves_no_staging_file() {
        let (dir, storage) = storage();
        storage.write_bytes("a/b/c.json", b"hello").await.unwrap();

        assert_eq!(storage.read_bytes("a/b/c.json").await.unwrap(), b"hello");
        let leftovers: Vec<_> = std::fs::read_dir(dir.path().join("a/b"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .filter(|n| is_tmp_artifact(n))
            .collect();
        assert!(leftovers.is_empty(), "staging files left: {leftovers:?}");
    }

    /// An overwrite replaces the whole file rather than truncating in place, so
    /// a shorter payload cannot leave a tail of the previous contents behind.
    #[tokio::test]
    async fn write_bytes_overwrite_replaces_entire_contents() {
        let (_dir, storage) = storage();
        storage
            .write_bytes("k.json", &vec![b'x'; 4096])
            .await
            .unwrap();
        storage.write_bytes("k.json", b"short").await.unwrap();

        assert_eq!(storage.read_bytes("k.json").await.unwrap(), b"short");
    }

    #[tokio::test]
    async fn insert_reports_creation_once_and_preserves_the_original() {
        let (_dir, storage) = storage();

        assert!(storage.insert("ns/led.json", b"first").await.unwrap());
        assert!(!storage.insert("ns/led.json", b"second").await.unwrap());
        assert_eq!(storage.read_bytes("ns/led.json").await.unwrap(), b"first");
    }

    #[tokio::test]
    async fn insert_leaves_no_staging_file_on_either_outcome() {
        let (dir, storage) = storage();
        storage.insert("ns/led.json", b"first").await.unwrap();
        storage.insert("ns/led.json", b"second").await.unwrap();

        let leftovers: Vec<_> = std::fs::read_dir(dir.path().join("ns"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .filter(|n| is_tmp_artifact(n))
            .collect();
        assert!(leftovers.is_empty(), "staging files left: {leftovers:?}");
    }

    /// A staging file left by an interrupted write is not content; handing it
    /// out as an address would let callers read a partial object.
    #[tokio::test]
    async fn list_prefix_skips_staging_files() {
        let (dir, storage) = storage();
        storage
            .write_bytes("fluree:file://d/real.json", b"v")
            .await
            .unwrap();
        std::fs::write(dir.path().join("d/real.json.999.0.tmp"), b"partial").unwrap();

        let listed = storage.list_prefix("d").await.unwrap();
        assert_eq!(listed, vec!["fluree:file://d/real.json".to_string()]);
    }

    #[tokio::test]
    async fn compare_and_swap_writes_through_staging() {
        let (dir, storage) = storage();
        storage.insert("h.json", b"v0").await.unwrap();

        let outcome: CasOutcome<()> = storage
            .compare_and_swap("h.json", |cur| {
                assert_eq!(cur, Some(b"v0".as_slice()));
                Ok(CasAction::Write(b"v1".to_vec()))
            })
            .await
            .unwrap();

        assert!(matches!(outcome, CasOutcome::Written));
        assert_eq!(storage.read_bytes("h.json").await.unwrap(), b"v1");
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .filter(|n| is_tmp_artifact(n))
            .collect();
        assert!(leftovers.is_empty(), "staging files left: {leftovers:?}");
    }
}
