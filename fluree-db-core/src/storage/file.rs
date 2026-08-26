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

/// Distinguishes this process from every other one that may be writing the
/// same directory — including ones on other hosts.
///
/// The pid is unique per host only. Two nodes of a Raft cluster sharing a
/// content store over NFS can have the same pid, and each starts `TMP_SEQ`
/// at zero, so `(pid, seq)` alone can collide across hosts: both stage to
/// the same sibling name, and the loser's rename fails even though the bytes
/// (content-addressed, hence identical) are in place. A 64-bit random token
/// drawn once per process makes that collision negligible without needing a
/// node id plumbed down from whoever knows one.
fn process_token() -> u64 {
    static TOKEN: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *TOKEN.get_or_init(rand::random::<u64>)
}

/// Staging path alongside `path`, unique per process — across hosts — and
/// per call.
///
/// Appends rather than replacing the extension so `foo.json` stages as
/// `foo.json.<pid>.<token>.<seq>.tmp`, keeping the final name recoverable by
/// eye and leaving multi-part extensions intact. The pid stays in the name
/// because it is what an operator greps for; the token is what makes it
/// unique.
fn tmp_sibling(path: &Path) -> PathBuf {
    let seq = TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(format!(
        ".{}.{:016x}.{seq}{TMP_SUFFIX}",
        std::process::id(),
        process_token()
    ));
    path.with_file_name(name)
}

/// True for a staging file left behind by an interrupted write.
fn is_tmp_artifact(name: &str) -> bool {
    name.ends_with(TMP_SUFFIX)
}

/// The process token embedded in a staging name, for names shaped the way
/// [`tmp_sibling`] writes them (`<name>.<pid>.<token>.<seq>.tmp`).
///
/// Returns `None` for anything else ending in `.tmp` — a file an operator
/// dropped there, or one written by an older version of this code. The sweep
/// treats an unparseable name as *not* ours, which is the safe direction: it
/// still has to clear the age threshold before anything happens to it.
fn staging_token(name: &str) -> Option<&str> {
    let rest = name.strip_suffix(TMP_SUFFIX)?;
    let (head, _seq) = rest.rsplit_once('.')?;
    let (_prefix, token) = head.rsplit_once('.')?;
    Some(token)
}

/// How long a staging file must have gone untouched before a sweep may
/// reclaim it.
///
/// A staging file's entire life is one [`stage_bytes`] call: create, write one
/// in-memory buffer, optionally fsync, then rename or link immediately. There
/// is no legitimate case where that takes a long time, which is what makes an
/// age threshold a sound discriminator here — unlike an index build (#1635),
/// whose duration grows with the ledger and so can never be bounded by a
/// constant. A day is orders of magnitude past any real staging write and
/// swamps NTP-scale clock skew between hosts sharing a mount.
const STALE_STAGING_AGE: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);

/// Directory entries one sweep will look at before giving up.
///
/// The walk runs on the construction path, so it must not turn opening a large
/// volume into a startup stall. Exhausting the budget leaves the rest of the
/// tree for the next start rather than delaying this one — orphans are inert,
/// so deferring them costs nothing but the disk they sit on.
const SWEEP_ENTRY_BUDGET: usize = 100_000;

/// What one sweep did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct StagingSweep {
    /// Staging files unlinked.
    reclaimed: usize,
    /// Staging files deliberately left alone — this process's own, too young,
    /// or refusing to be removed.
    kept: usize,
    /// The entry budget ran out before the walk finished, so part of the tree
    /// was never looked at.
    truncated: bool,
}

/// Unlink staging files left behind by writes that never finished.
///
/// A crash between the `File::create` in [`stage_bytes`] and the rename that
/// follows leaves a full copy of the object on disk under a `.tmp` name.
/// `list_prefix` filters those out, so one can never be served as content —
/// which is also precisely why nothing ever removed them. They accumulate, and
/// the crash loop that produces them is often a disk-exhaustion crash loop, so
/// the growth lands on the volume that can least afford it.
///
/// # What this will not delete
///
/// Storage is shared by more than one process in a multi-instance deployment,
/// so a sweep that guessed wrong would pull a live writer's file out from under
/// it. Two rules stop that:
///
/// 1. **Never this process's own.** The staging name carries a 64-bit token
///    drawn once per process, so a name bearing our token is ours — in flight
///    or already leaked, and a directory entry cannot tell those apart. Both
///    are left alone. This rule is exact, not a heuristic.
/// 2. **Never a file touched recently.** Anything modified within `older_than`
///    is left for whoever is writing it. This rule *is* a heuristic: it reads
///    the writer's clock through ours, and it assumes no legitimate staging
///    write stays open that long. See [`STALE_STAGING_AGE`] for why that
///    assumption holds for staging files specifically.
///
/// Anything the sweep cannot classify — a name it cannot parse, an entry it
/// cannot stat, an mtime in the future — is kept. Every unknown resolves
/// toward leaving the file alone.
///
/// If both rules were somehow beaten, the damage is bounded: on POSIX the
/// writer keeps its open descriptor, so its `write_all` and `sync_all` still
/// succeed against the now-unlinked inode and only the final rename fails. The
/// write reports an error; nothing partial is ever published under a content
/// address.
fn sweep_orphaned_staging_files(
    base: &Path,
    older_than: std::time::Duration,
    budget: usize,
) -> StagingSweep {
    let mut sweep = StagingSweep::default();
    let own_token = format!("{:016x}", process_token());
    let now = std::time::SystemTime::now();
    let mut budget = budget;
    let mut dirs = vec![base.to_path_buf()];

    while let Some(dir) = dirs.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            if budget == 0 {
                sweep.truncated = true;
                return sweep;
            }
            budget -= 1;

            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                dirs.push(entry.path());
                continue;
            }
            if !file_type.is_file() {
                continue;
            }

            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !is_tmp_artifact(&name) {
                continue;
            }
            // Rule 1: ours, whatever its age.
            if staging_token(&name) == Some(own_token.as_str()) {
                sweep.kept += 1;
                continue;
            }
            // Rule 2: young enough that someone may still be writing it. An
            // mtime we cannot read, or one in the future, counts as young —
            // `duration_since` fails on a future timestamp, and a clock the
            // sweep does not understand is not grounds for deleting data.
            let stale = entry
                .metadata()
                .and_then(|m| m.modified())
                .ok()
                .and_then(|m| now.duration_since(m).ok())
                .is_some_and(|age| age >= older_than);
            if !stale {
                sweep.kept += 1;
                continue;
            }

            match std::fs::remove_file(entry.path()) {
                Ok(()) => sweep.reclaimed += 1,
                // Someone else got there first, or the platform refuses to
                // unlink a file another process still holds open (Windows).
                // Both are fine outcomes for a best-effort reclaim.
                Err(_) => sweep.kept += 1,
            }
        }
    }
    sweep
}

/// Base paths this process has already swept.
///
/// `FileStorage::new` runs once per connection, once per nameservice and again
/// for the API's own handle, frequently on the same directory. Only the first
/// walk can find anything; the rest would re-walk the tree to look at exactly
/// the files the first one declined to touch.
fn claim_sweep(base: &Path) -> bool {
    static SWEPT: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<PathBuf>>> =
        std::sync::OnceLock::new();
    SWEPT
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(base.to_path_buf())
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
    ///
    /// Constructing the storage also reclaims staging files orphaned by an
    /// earlier crash — see [`sweep_orphaned_staging_files`] for what it will
    /// and will not delete, and [`Self::SWEEP_ENV_VAR`] to turn it off. The
    /// walk is bounded and runs at most once per base path per process.
    pub fn new(base_path: impl Into<std::path::PathBuf>) -> Self {
        let base_path = base_path.into();
        Self::sweep_on_construction(&base_path);
        Self {
            base_path,
            durability: Durability::from_env(),
            fsyncs: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Set to a falsey value (`0`, `false`, `off`, `no`) to skip the
    /// startup sweep of orphaned staging files.
    ///
    /// The escape hatch exists because the sweep walks the storage tree, and
    /// an operator who knows their volume is enormous — or who wants a crash's
    /// leftovers preserved for a post-mortem — should be able to say so
    /// without patching the binary.
    pub const SWEEP_ENV_VAR: &'static str = "FLUREE_STORAGE_TMP_SWEEP";

    /// Whether [`Self::SWEEP_ENV_VAR`] asks for the sweep to be skipped.
    fn sweep_disabled_by_env() -> bool {
        std::env::var(Self::SWEEP_ENV_VAR)
            .ok()
            .is_some_and(|v| Self::env_says_off(&v))
    }

    /// Pure half of [`Self::sweep_disabled_by_env`], so the accepted spellings
    /// are testable without touching process environment. Same spellings
    /// [`Durability::ENV_VAR`] accepts — one convention for the whole storage
    /// backend, not one per switch.
    fn env_says_off(value: &str) -> bool {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no"
        )
    }

    /// Reclaim orphaned staging files under `base_path`, at most once per path
    /// per process.
    fn sweep_on_construction(base_path: &Path) {
        if Self::sweep_disabled_by_env() || !claim_sweep(base_path) {
            return;
        }
        let sweep = sweep_orphaned_staging_files(base_path, STALE_STAGING_AGE, SWEEP_ENTRY_BUDGET);
        // Silence is the normal case, and a startup log line per storage would
        // be noise. Say something only when there was debris to report or a
        // walk that did not finish.
        if sweep.reclaimed > 0 || sweep.truncated {
            tracing::info!(
                base_path = %base_path.display(),
                reclaimed = sweep.reclaimed,
                kept = sweep.kept,
                truncated = sweep.truncated,
                "reclaimed staging files orphaned by an interrupted write"
            );
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
        // an unrecoverable reader error rather than a miss the caller could heal.
        // Excluding empty files here is what converts that poison back into a fetch.
        // See `read_bytes` for why empty can never be legitimate content.
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
        let requested = range.end - range.start;
        let offset = range.start;
        let address = address.to_owned();
        tokio::task::spawn_blocking(move || {
            let file = std::fs::File::open(&path).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    crate::error::Error::not_found(format!("{}: {}", address, path.display()))
                } else {
                    crate::error::Error::io(format!("Failed to open {}: {}", path.display(), e))
                }
            })?;
            // One stat off the open handle, serving both the zero-length guard
            // and the clamp below: no extra syscall, and no window between the
            // check and the read. `fstat` on a descriptor this thread owns does
            // not fail in practice; if it ever did there would be nothing to
            // size the read against, and saying so beats guessing a length.
            let file_len = file
                .metadata()
                .map_err(|e| {
                    crate::error::Error::io(format!("Failed to stat {}: {}", path.display(), e))
                })?
                .len();
            // The fourth read path, held to the same rule as the other three:
            // an empty file at a content address is debris, not content. A
            // ranged read would otherwise stop at EOF and hand back an empty
            // buffer — the "empty content" answer this whole change exists to
            // replace with "absent". This arm is not hypothetical: once
            // `resolve_local_path` refuses the debris, the leaflet reader
            // falls through to `ContentStore::get_range`, which lands here for
            // the very same file.
            if file_len == 0 {
                tracing::warn!(
                    address,
                    path = %path.display(),
                    "zero-length blob treated as absent on a ranged read (failed write \
                     debris); it will be re-fetched or rebuilt. Delete it to reclaim the inode."
                );
                return Err(crate::error::Error::not_found(format!(
                    "{}: {} (zero-length blob, treated as absent)",
                    address,
                    path.display()
                )));
            }
            // SIZE THE BUFFER FROM THE OBJECT, NOT FROM THE RANGE. The trait
            // documents a ranged read as returning bytes that "may be shorter
            // than requested if the object is smaller than `range.end`", and
            // `mid..u64::MAX` is the established spelling of "read to the end"
            // against it. Trusting the range's width made that spelling a
            // `usize::MAX` allocation — a capacity-overflow panic that
            // `spawn_blocking` caught and relabelled `Io("spawn_blocking
            // failed: ...")`, so the one backend that could not serve the call
            // was also the one that could not say why. The loops below already
            // stop at EOF, so a range that fits reads exactly as before; this
            // only stops the allocation from believing the caller.
            let len = requested.min(file_len.saturating_sub(offset)) as usize;
            let mut buf = vec![0u8; len];
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
            // Zero length is absent here too, and the consistency is the point:
            // reporting `true` for a blob `read_bytes` then refuses to return is a
            // worse contract than either answer alone — a caller that checks before
            // reading would see the blob appear and then vanish. Answering `false`
            // also lets a writer replace the debris instead of skipping it as
            // already-present, which is how the bad file finally leaves the disk.
            Ok(m) => Ok(m.len() > 0),
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
    use std::io::Write;
    use std::time::{Duration, SystemTime};

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
    /// Two *processes* staging the same address must pick different
    /// siblings, or — on a shared mount, where two hosts can share a
    /// pid — one of them loses its rename. Proven by re-executing this
    /// test binary as a child and comparing what it picks.
    #[test]
    fn tmp_sibling_is_unique_across_processes() {
        const PROBE: &str = "FLUREE_TMP_SIBLING_PROBE";
        let mine = tmp_sibling(Path::new("/data/x"))
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        if std::env::var_os(PROBE).is_some() {
            println!("{mine}");
            return;
        }

        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "storage::file::tests::tmp_sibling_is_unique_across_processes",
                "--nocapture",
            ])
            .env(PROBE, "1")
            .output()
            .expect("re-exec the test binary");
        assert!(
            out.status.success(),
            "child failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let theirs = String::from_utf8_lossy(&out.stdout)
            .lines()
            .find(|l| l.starts_with("x."))
            .expect("child printed its sibling name")
            .to_string();

        // Neither the pid nor the sequence may be what keeps them apart:
        // two hosts can share a pid, and both start the counter at zero.
        // Only the per-process token is cross-host unique, so it is the
        // component that must differ.
        let token = |n: &str| -> String {
            // `x.<pid>.<token>.<seq>.tmp`
            let parts: Vec<&str> = n.split('.').collect();
            assert_eq!(parts.len(), 5, "unexpected staging name shape: {n}");
            parts[2].to_string()
        };
        assert_ne!(
            token(&mine),
            token(&theirs),
            "two processes must draw different tokens: {mine} vs {theirs}",
        );
    }

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

    // ---------------------------------------------------------------------
    // Orphaned staging files
    // ---------------------------------------------------------------------

    /// Plant a staging file the way a crashed write would leave one, aged
    /// `age` by setting its mtime rather than by waiting. `token` stands in
    /// for the writing process, so a test can plant one that looks like
    /// another instance's or like our own.
    fn plant_staging_file(dir: &Path, name: &str, token: &str, age: Duration) -> PathBuf {
        let path = dir.join(format!("{name}.4242.{token}.0{TMP_SUFFIX}"));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(b"half a leaf").unwrap();
        // Age the file by stamping its mtime, not by sleeping — the threshold
        // is the thing under test, and a test that waits for a real clock is
        // both slow and a flake waiting to happen.
        file.set_modified(SystemTime::now() - age).unwrap();
        assert!(path.exists());
        path
    }

    fn own_token() -> String {
        format!("{:016x}", process_token())
    }

    /// A crash between `File::create` and the rename leaves a full copy of the
    /// object behind, and `list_prefix` hides it from every reader — which is
    /// exactly why nothing ever removed it. Opening the storage has to.
    #[test]
    fn construction_reclaims_an_orphaned_staging_file() {
        let dir = tempfile::tempdir().unwrap();
        let orphan = plant_staging_file(
            &dir.path().join("a/b"),
            "leaf.json",
            "0123456789abcdef",
            STALE_STAGING_AGE * 2,
        );

        let _storage = FileStorage::new(dir.path());

        assert!(
            !orphan.exists(),
            "an orphan older than the threshold survived construction: {}",
            orphan.display()
        );
    }

    /// THE ONE THAT MATTERS. Storage is shared in a multi-instance deployment,
    /// so a sweep that deletes a staging file another process is still writing
    /// takes that process's rename out from under it. A file young enough to
    /// belong to a live write is not the sweep's business at any age policy.
    #[test]
    fn construction_leaves_a_live_looking_staging_file_alone() {
        let dir = tempfile::tempdir().unwrap();
        // Another instance's token, written a moment ago — the shape of a
        // write that is in flight right now.
        let live = plant_staging_file(
            dir.path(),
            "commit.json",
            "fedcba9876543210",
            Duration::from_secs(0),
        );

        let _storage = FileStorage::new(dir.path());

        assert!(
            live.exists(),
            "the sweep deleted a staging file a concurrent writer may still hold: {}",
            live.display()
        );
    }

    /// A file bearing our own token is either in flight on another task or
    /// already leaked, and a directory entry cannot tell those apart. Age is
    /// not allowed to break the tie: even with the threshold at zero, the
    /// exact rule wins.
    #[test]
    fn our_own_staging_file_is_never_reclaimed_however_old() {
        let dir = tempfile::tempdir().unwrap();
        let ours = plant_staging_file(
            dir.path(),
            "mine.json",
            &own_token(),
            STALE_STAGING_AGE * 100,
        );

        let sweep = sweep_orphaned_staging_files(dir.path(), Duration::ZERO, SWEEP_ENTRY_BUDGET);

        assert!(
            ours.exists(),
            "the sweep deleted this process's own staging file: {}",
            ours.display()
        );
        assert_eq!(sweep.reclaimed, 0);
        assert_eq!(sweep.kept, 1);
    }

    /// The token in the name is what separates our files from every other
    /// writer's, so the sweep must read it out of a real `tmp_sibling` name
    /// rather than a shape a test invented.
    #[test]
    fn staging_token_reads_what_tmp_sibling_writes() {
        let name = tmp_sibling(Path::new("/data/a.json.gz"))
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        assert_eq!(staging_token(&name), Some(own_token().as_str()), "{name}");

        // Anything not shaped like a staging name is not ours, which is the
        // safe answer: it still has to clear the age threshold.
        assert_eq!(staging_token("plain.tmp"), None);
        assert_eq!(staging_token("leaf.json"), None);
        assert_ne!(
            staging_token("leaf.json.999.0.tmp"),
            Some(own_token().as_str())
        );
    }

    /// The threshold is the only thing standing between a foreign in-flight
    /// write and deletion, so it has to be the age that decides — not merely
    /// "is this file ours".
    #[test]
    fn only_files_past_the_threshold_are_reclaimed() {
        let dir = tempfile::tempdir().unwrap();
        let threshold = Duration::from_secs(3600);
        let young = plant_staging_file(dir.path(), "young.json", "aaaa", threshold / 2);
        let old = plant_staging_file(dir.path(), "old.json", "bbbb", threshold * 2);

        let sweep = sweep_orphaned_staging_files(dir.path(), threshold, SWEEP_ENTRY_BUDGET);

        assert!(young.exists(), "a file inside the threshold was reclaimed");
        assert!(!old.exists(), "a file past the threshold survived");
        assert_eq!((sweep.reclaimed, sweep.kept), (1, 1));
    }

    /// An mtime in the future means the writer's clock and ours disagree, and
    /// a clock the sweep does not understand is not grounds for deleting data.
    #[test]
    fn a_future_mtime_is_treated_as_live() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(format!("skewed.json.7.cccc.0{TMP_SUFFIX}"));
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(b"partial").unwrap();
        file.set_modified(SystemTime::now() + Duration::from_secs(3600))
            .unwrap();

        let sweep = sweep_orphaned_staging_files(dir.path(), Duration::ZERO, SWEEP_ENTRY_BUDGET);

        assert!(path.exists(), "a future-dated staging file was reclaimed");
        assert_eq!((sweep.reclaimed, sweep.kept), (0, 1));
    }

    /// Content is not staging debris. The sweep must not touch a real object
    /// however old it is — content-addressed blobs are written once and then
    /// sit there for the life of the ledger.
    #[tokio::test]
    async fn the_sweep_never_touches_content() {
        let (dir, storage) = storage();
        storage
            .write_bytes("a/b/real.json", b"content")
            .await
            .unwrap();
        let real = storage.resolve_path("a/b/real.json").unwrap();
        // `futimens` needs a writable descriptor, so this cannot be a plain
        // `File::open`.
        std::fs::OpenOptions::new()
            .write(true)
            .open(&real)
            .unwrap()
            .set_modified(SystemTime::now() - STALE_STAGING_AGE * 10)
            .unwrap();

        let sweep = sweep_orphaned_staging_files(dir.path(), Duration::ZERO, SWEEP_ENTRY_BUDGET);

        assert_eq!(sweep.reclaimed, 0, "the sweep reclaimed real content");
        assert_eq!(
            storage.read_bytes("a/b/real.json").await.unwrap(),
            b"content"
        );
    }

    /// The walk is on the construction path, so a huge volume must not turn
    /// opening a ledger into a startup stall. Running out of budget stops the
    /// walk and says so, rather than running to completion.
    #[test]
    fn the_walk_is_bounded_by_its_entry_budget() {
        let dir = tempfile::tempdir().unwrap();
        for i in 0..8 {
            plant_staging_file(
                dir.path(),
                &format!("o{i}.json"),
                "dddd",
                STALE_STAGING_AGE * 2,
            );
        }

        let sweep = sweep_orphaned_staging_files(dir.path(), Duration::ZERO, 3);

        assert!(sweep.truncated, "the budget did not stop the walk");
        assert_eq!(sweep.reclaimed, 3, "the walk went past its budget");
    }

    /// One walk per base path per process. `FileStorage::new` runs once per
    /// connection, once per nameservice and again for the API's own handle,
    /// often on the same directory; re-walking the tree each time would only
    /// re-examine the files the first walk declined to touch.
    #[test]
    fn a_base_path_is_swept_at_most_once_per_process() {
        let dir = tempfile::tempdir().unwrap();
        assert!(claim_sweep(dir.path()), "first claim must win");
        assert!(!claim_sweep(dir.path()), "second claim must be refused");
    }

    /// Same spellings the durability switch accepts — one convention for the
    /// storage backend, not one per environment variable.
    #[test]
    fn sweep_env_var_accepts_the_durability_spellings() {
        for v in ["0", "false", "off", "no", "OFF", " false "] {
            assert!(FileStorage::env_says_off(v), "{v:?}");
        }
        for v in ["1", "true", "on", "", "nonsense"] {
            assert!(!FileStorage::env_says_off(v), "{v:?}");
        }
    }

    /// Write a zero-length file directly, bypassing the storage API — which is
    /// the only way this state arises now that writes are atomic. It models
    /// debris already on disk from before that, or from a truncating crash
    /// outside this process.
    fn plant_zero_length(storage: &FileStorage, address: &str) -> std::path::PathBuf {
        let path = storage.resolve_path(address).expect("resolve");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"").unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        path
    }

    /// A zero-length blob must read as ABSENT, not as empty content. This is the
    /// ENOSPC debris case: ~4,000 such files survived one outage and turned every
    /// later read into `pack header: need 40 bytes, got 0` — a parse failure no
    /// caller can repair, where a miss would have been re-fetched.
    #[tokio::test]
    async fn zero_length_blob_reads_as_absent() {
        let (_dir, storage) = storage();
        plant_zero_length(&storage, "z/empty.dict");

        let err = storage
            .read_bytes("z/empty.dict")
            .await
            .expect_err("a zero-length blob must not read as empty content");
        assert!(
            matches!(err, crate::error::Error::NotFound(_)),
            "must be NotFound so callers re-fetch rather than parse nothing: {err:?}"
        );
    }

    /// `resolve_local_path` hands a path to callers that mmap or parse it
    /// directly, so returning a zero-length blob converts recoverable debris into
    /// an unrecoverable reader error. Presence is not validity.
    #[test]
    fn resolve_local_path_rejects_a_zero_length_blob() {
        let (_dir, storage) = storage();
        plant_zero_length(&storage, "z/empty.dict");

        assert!(
            storage.resolve_local_path("z/empty.dict").is_none(),
            "a zero-length blob must not be offered as a local path"
        );
    }

    /// `exists` has to agree with `read_bytes`. Reporting a blob present that the
    /// reader then refuses is a worse contract than either answer alone, and
    /// answering `false` is also what lets a writer replace the debris rather
    /// than skip it as already-present.
    #[tokio::test]
    async fn exists_agrees_with_read_bytes_on_a_zero_length_blob() {
        let (_dir, storage) = storage();
        plant_zero_length(&storage, "z/empty.dict");

        assert!(
            !storage.exists("z/empty.dict").await.unwrap(),
            "exists must not report a blob that read_bytes treats as absent"
        );

        // And the debris is replaceable: a normal write over it restores service.
        storage.write_bytes("z/empty.dict", b"real").await.unwrap();
        assert!(storage.exists("z/empty.dict").await.unwrap());
        assert_eq!(storage.read_bytes("z/empty.dict").await.unwrap(), b"real");
    }

    /// A ranged read must agree with `read_bytes` on the same blob. Without the
    /// guard the read stops at EOF and returns `Ok([])` — empty content, the
    /// answer no caller can heal — where `read_bytes` says absent.
    ///
    /// Reached in practice *because of* `resolve_local_path`: the leaflet
    /// reader tries the local path first, that guard refuses the debris, and it
    /// falls through to `ContentStore::get_range`, which reads the same file
    /// through here.
    #[tokio::test]
    async fn zero_length_blob_reads_as_absent_through_a_ranged_read() {
        let (_dir, storage) = storage();
        let address = "z/ranged.dict";
        plant_zero_length(&storage, address);

        let err = storage
            .read_byte_range(address, 0..40)
            .await
            .expect_err("a zero-length blob must be absent on a ranged read, not empty content");
        assert!(
            matches!(err, crate::error::Error::NotFound(_)),
            "expected NotFound, got {err:?}"
        );

        // The two read surfaces must not disagree about the same blob.
        assert!(storage.read_bytes(address).await.is_err());

        // And a real write over the debris restores both.
        storage.write_bytes(address, b"real").await.unwrap();
        assert_eq!(
            storage.read_byte_range(address, 0..4).await.unwrap(),
            b"real"
        );
    }

    /// `StorageRead::read_byte_range` documents a ranged read as returning
    /// "the bytes within the range, which may be shorter than requested if the
    /// object is smaller than `range.end`", and `mid..u64::MAX` is the
    /// established spelling of "read to the end" against that trait. The
    /// default implementation, `MemoryStorage` and the proxy (which inherits
    /// the default) all clamp; this backend sized its buffer from the range's
    /// width instead, so the same call allocated `usize::MAX` and came back as
    /// `Io("spawn_blocking failed: task panicked ... capacity overflow")`.
    #[tokio::test]
    async fn an_open_ended_range_is_clamped_to_the_object() {
        let (_dir, storage) = storage();
        storage
            .write_bytes("z/clamp.dict", b"hello world")
            .await
            .unwrap();

        let tail = storage
            .read_byte_range("z/clamp.dict", 6..u64::MAX)
            .await
            .expect("open-ended range must clamp like every other backend");
        assert_eq!(tail, b"world");

        // From the top, too — the whole object, not a `usize::MAX` buffer.
        let all = storage
            .read_byte_range("z/clamp.dict", 0..u64::MAX)
            .await
            .expect("open-ended range from zero must clamp");
        assert_eq!(all, b"hello world");

        // A start past the end is empty, matching the default implementation
        // rather than erroring.
        assert!(storage
            .read_byte_range("z/clamp.dict", 99..u64::MAX)
            .await
            .unwrap()
            .is_empty());
    }

    /// The clamp must agree with the backend every caller compares against.
    /// Same bytes, same ranges, same answers — that is the whole point of the
    /// shared trait.
    #[tokio::test]
    async fn ranged_reads_match_the_memory_backend() {
        let (_dir, file) = storage();
        let memory = crate::storage::memory::MemoryStorage::new();
        let bytes = b"the quick brown fox".as_slice();
        file.write_bytes("z/same.dict", bytes).await.unwrap();
        memory.write_bytes("z/same.dict", bytes).await.unwrap();

        for range in [
            0..u64::MAX,
            4..u64::MAX,
            0..4,
            4..9,
            0..1000,
            18..1000,
            19..u64::MAX,
            50..60,
            5..5,
        ] {
            assert_eq!(
                file.read_byte_range("z/same.dict", range.clone())
                    .await
                    .unwrap_or_else(|e| panic!("file backend refused {range:?}: {e:?}")),
                memory
                    .read_byte_range("z/same.dict", range.clone())
                    .await
                    .unwrap(),
                "backends disagree on {range:?}"
            );
        }
    }

    /// The guard must not fire on legitimate content. A one-byte blob is the
    /// smallest thing that is genuinely there.
    #[tokio::test]
    async fn a_one_byte_blob_is_still_present() {
        let (_dir, storage) = storage();
        storage.write_bytes("z/tiny.dict", b"x").await.unwrap();

        assert!(storage.exists("z/tiny.dict").await.unwrap());
        assert!(storage.resolve_local_path("z/tiny.dict").is_some());
        assert_eq!(storage.read_bytes("z/tiny.dict").await.unwrap(), b"x");
        // The ranged path agrees that one byte is present.
        assert_eq!(
            storage.read_byte_range("z/tiny.dict", 0..1).await.unwrap(),
            b"x"
        );
    }
}
