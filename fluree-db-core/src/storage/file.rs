//! Filesystem storage backend (requires the `native` feature).
//!
//! Provides [`FileStorage`], which stores ledger data on the local filesystem
//! using `tokio::fs` for async I/O. This module is only compiled on non-WASM
//! targets with the `native` feature enabled.

use super::wal::{self, Acquire, Op, Wal, WAL_DIR};
use crate::error::Result;
use crate::{
    content_address, CasAction, CasOutcome, ContentAddressedWrite, ContentKind, ContentWriteResult,
    StorageCas, StorageExtError, StorageExtResult, StorageMethod, StorageRead, StorageWrite,
};
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

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

/// A decimal number written the way this backend writes one: digits, and no
/// leading zeros.
///
/// The zero-padding rule is free — `std::process::id()` and a `fetch_add`
/// counter never produce a padded number, so nothing we emit is excluded — and
/// it narrows what the legacy branch below will admit. `backup.2026.08.tmp` and
/// `wal.000001.000002.tmp` are the shape of a foreign file that would otherwise
/// read as `<name>.<pid>.<seq>.tmp`.
fn is_plain_decimal(s: &str) -> bool {
    match s.as_bytes() {
        [] => false,
        // A sequence number legitimately starts at zero; `00` does not.
        [b'0'] => true,
        [first, rest @ ..] => {
            first.is_ascii_digit() && *first != b'0' && rest.iter().all(u8::is_ascii_digit)
        }
    }
}

/// The 16 lowercase hex digits [`tmp_sibling`] formats a process token as.
fn is_process_token(s: &str) -> bool {
    s.len() == 16
        && s.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

/// The process token embedded in a staging name, for names shaped exactly the
/// way [`tmp_sibling`] writes them (`<name>.<pid>.<token>.<seq>.tmp`).
///
/// **This is the sweep's admission test, not a convenience.** `.tmp` is a
/// suffix, not a namespace: the indexer's vocab merge, the disk cache, the
/// nameservice's tracking file and the Raft log all stage under it, and the
/// nameservice shares the swept tree by construction (`FileNameService::new`
/// builds a `FileStorage` on the path `FlureeBuilder::build` also hands to
/// storage). Recognizing our own naming — rather than trusting the extension —
/// is the only thing keeping this sweep off their files.
///
/// Both formats this backend has ever written parse: the current one, and the
/// pre-`b0a9c416a` `<name>.<pid>.<seq>.tmp`, whose pid lands in the token
/// position and is accepted as a decimal. **That branch is load-bearing, not
/// legacy politeness** — staging writes arrived in `85183c9ca`, which is
/// contained in v4.1.5 and v4.1.6, so every store written by a currently
/// released build produces orphans in that format. Dropping it would strand
/// them on disk forever.
///
/// Every segment is shape-checked, so a foreign name that merely happens to
/// have enough dots in it (`a.b.c.tmp`) does not slip through on arity alone.
///
/// Residual: the legacy branch still admits a foreign name shaped
/// `<non-empty>.<decimal>.<decimal>.tmp` — `dump.1.2.tmp` parses. No in-tree
/// `.tmp` producer emits that shape, so this is an operator's own file rather
/// than a collision with anything we ship, and narrowing it further means
/// gating legacy reclaim behind an opt-in, which is an upgrade-behavior
/// decision rather than a parser one. The residual is tolerable *because*
/// the sweep is a deliberate startup action an operator can see and disable
/// ([`FileStorage::sweep_orphaned_staging`], [`FileStorage::SWEEP_ENV_VAR`])
/// — not a side effect of constructing a storage, which is what would let a
/// stray unit test or inspection tool reach it.
///
/// Returns `None` for anything else. That is the safe direction in the only
/// sense that matters: failing to reclaim an orphan wastes disk, and reclaiming
/// someone else's file loses data.
fn staging_token(name: &str) -> Option<&str> {
    let rest = name.strip_suffix(TMP_SUFFIX)?;
    let (head, seq) = rest.rsplit_once('.')?;
    let (prefix, token) = head.rsplit_once('.')?;
    // There has to be a destination name in front of the pid, or this is some
    // other file that merely ends in `.tmp`.
    if prefix.is_empty() || !is_plain_decimal(seq) {
        return None;
    }
    if is_process_token(token) || is_plain_decimal(token) {
        Some(token)
    } else {
        None
    }
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
/// A walk bounded in *entries* is not bounded in wall-clock — on the shared
/// mount this design is written for, each `readdir` is a network round trip —
/// so the cap stays even though the walk no longer runs on the caller's thread.
///
/// **Exhausting it is not a deferral.** The walk restarts from the base path
/// every time with no cursor, and it never removes content files, so a tree
/// whose first `SWEEP_ENTRY_BUDGET` entries in traversal order are content will
/// re-walk those same entries on every start and never reach an orphan beyond
/// them. That is silent under-delivery, which is why truncation logs at `warn`
/// and why [`FileStorage::SWEEP_ENV_VAR`] accepts a larger budget: an operator
/// who sees the warning needs something to do about it.
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
/// and the tree is shared by other *subsystems* even in one process, so a sweep
/// that guessed wrong would pull a live writer's file out from under it. Three
/// rules stop that:
///
/// 0. **Never a file this backend's staging writer did not name.** `.tmp` is a
///    suffix, not a namespace — see [`staging_token`], which is the admission
///    test. Everything below applies only to names that pass it.
/// 1. **Never this process's own.** The staging name carries a 64-bit token
///    drawn once per process, so a name bearing our token is ours — in flight
///    or already leaked, and a directory entry cannot tell those apart. Both
///    are left alone. This rule is exact, not a heuristic — but exact for the
///    *current* name format only. A legacy `<name>.<pid>.<seq>.tmp` name (see
///    [`staging_token`]) carries a pid in the token slot, and a pid can never
///    equal a 16-hex process token, so for legacy names this rule is inert
///    and rule 2 stands alone. That case is not a corner but the upgrade
///    path itself: in a rolling upgrade, a still-running v4.1.5/v4.1.6
///    process stages into the shared tree in the legacy format, and the age
///    heuristic is the only thing protecting its in-flight writes. Tolerable
///    — a staging write is open for milliseconds against a one-day threshold
///    — but the rules do not layer there, and this doc should not pretend
///    they do.
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
                // The WAL stages nothing this sweep should touch.
                if entry.file_name() != WAL_DIR {
                    dirs.push(entry.path());
                }
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
            // RULE 0, and the one that decides whether the other two are even
            // asked. `.tmp` is a suffix several subsystems use, some of them
            // writing into this very tree, so a file only becomes the sweep's
            // business once its name parses as one *this* writer produced.
            // Without this, rule 1 protects nothing outside our own naming and
            // the age heuristic alone stands between every other subsystem's
            // staging file and an unlink — including the indexer's vocab-merge
            // temporaries, which are exactly the long-running shape
            // `STALE_STAGING_AGE` argues an age threshold must not judge.
            let Some(token) = staging_token(&name) else {
                continue;
            };
            // Rule 1: ours, whatever its age.
            if token == own_token.as_str() {
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
/// Startup opens several handles on one directory — the connection's storage,
/// the API's own handle — and more than one startup layer calls
/// [`FileStorage::sweep_orphaned_staging`] on the way up. Only the first walk
/// can find anything; the rest would re-walk the tree to look at exactly the
/// files the first one declined to touch.
///
/// Canonicalized first, so the guarantee is about the *directory* and not about
/// how a caller spelled it — `/x`, `/x/.` and `/x/` are one base path, not
/// three. Falls back to the literal path when canonicalization fails (the
/// directory may not exist yet), which is the pre-existing behaviour.
fn claim_sweep(base: &Path) -> bool {
    static SWEPT: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<PathBuf>>> =
        std::sync::OnceLock::new();
    let key = std::fs::canonicalize(base).unwrap_or_else(|_| base.to_path_buf());
    SWEPT
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(key)
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
    /// The root's WAL under [`Durability::Wal`], attached on the
    /// first write or by [`Self::recover_wal`]. Shared across clones.
    wal: Arc<OnceLock<WalAttach>>,
    /// Which log this handle owns when the root is shared by several
    /// processes. See [`Self::with_wal_owner`].
    wal_owner: Option<Arc<str>>,
    /// Derived content written since the last [`StorageWrite::sync`], as
    /// root-relative keys. Shared across clones. Empty under
    /// [`Durability::PageCache`], where nothing is ever flushed.
    unflushed: Arc<std::sync::Mutex<Vec<String>>>,
    /// One [`StorageWrite::sync`] at a time per root, so a caller whose
    /// writes an earlier, still-running flush took waits for that flush
    /// instead of returning before its files are on the device.
    flushing: Arc<tokio::sync::Mutex<()>>,
    #[cfg(test)]
    after_checkpoint: AfterCheckpoint,
}

#[cfg(test)]
type Hook = Box<dyn Fn() + Send + Sync>;

/// A test's hook into the oversized write path, run between the checkpoint
/// and the write.
#[cfg(test)]
#[derive(Clone, Default)]
struct AfterCheckpoint(Arc<std::sync::Mutex<Option<Hook>>>);

#[cfg(test)]
impl std::fmt::Debug for AfterCheckpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AfterCheckpoint")
    }
}

/// A logged operation's exclusive hold on its key; see [`FileStorage::hold_key`].
type KeyHold = tokio::sync::OwnedMutexGuard<()>;

/// What the WAL resolved to for one root.
#[derive(Debug)]
enum WalAttach {
    Log(Arc<Wal>),
    /// The log could not be owned; writes flush per file instead.
    Fallback,
}

impl FileStorage {
    /// Create a new file storage with the given base path
    ///
    /// The base path should be the ledger's data directory containing the ledger
    /// subdirectories (e.g. `mydb/main/index/...`).
    ///
    /// Durability defaults to [`Durability::Wal`], overridable for this
    /// process by [`Durability::ENV_VAR`] or per instance by
    /// [`Self::with_durability`].
    ///
    /// Constructing a storage touches nothing on disk. In particular it does
    /// **not** reclaim orphaned staging files or replay a WAL: both are
    /// startup decisions, and only the startup layer knows it is starting up.
    /// A test or a tool that constructs a `FileStorage` on a directory it does
    /// not own must not mutate that directory — the connection and builder
    /// startup paths call [`Self::sweep_orphaned_staging`] and
    /// [`Self::recover_wal`] explicitly.
    pub fn new(base_path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            base_path: base_path.into(),
            durability: Durability::from_env(),
            fsyncs: Arc::new(AtomicU64::new(0)),
            wal: Arc::new(OnceLock::new()),
            wal_owner: None,
            unflushed: Arc::new(std::sync::Mutex::new(Vec::new())),
            flushing: Arc::new(tokio::sync::Mutex::new(())),
            #[cfg(test)]
            after_checkpoint: AfterCheckpoint::default(),
        }
    }

    /// Set to a falsey value (`0`, `false`, `off`, `no`) to skip the startup
    /// sweep of orphaned staging files.
    ///
    /// The escape hatch exists because the sweep walks the storage tree, and an
    /// operator who wants a crash's leftovers preserved for a post-mortem
    /// should be able to say so without patching the binary.
    pub const SWEEP_ENV_VAR: &'static str = "FLUREE_STORAGE_TMP_SWEEP";

    /// Overrides [`SWEEP_ENTRY_BUDGET`], the number of directory entries one
    /// sweep will look at.
    ///
    /// Deliberately a *separate* variable rather than an overload of
    /// [`Self::SWEEP_ENV_VAR`]: `FLUREE_STORAGE_FSYNC=1` means "on", so an
    /// operator would reasonably write `FLUREE_STORAGE_TMP_SWEEP=1` meaning the
    /// same — and if that spelling were read as a budget it would silently mean
    /// "look at one entry", which is off wearing a disguise. One variable, one
    /// job.
    ///
    /// This exists so the truncation warning has a remedy attached. A warning
    /// an operator cannot act on is just noise.
    pub const SWEEP_BUDGET_ENV_VAR: &'static str = "FLUREE_STORAGE_TMP_SWEEP_BUDGET";

    /// The entry budget for a sweep, or `None` to skip it entirely.
    fn sweep_budget_from_env() -> Option<usize> {
        if std::env::var(Self::SWEEP_ENV_VAR)
            .ok()
            .is_some_and(|v| Self::env_says_off(&v))
        {
            return None;
        }
        Some(Self::parse_budget(
            std::env::var(Self::SWEEP_BUDGET_ENV_VAR).ok().as_deref(),
        ))
    }

    /// Pure half of the budget lookup, so the accepted spellings are testable
    /// without touching process environment.
    ///
    /// An unrecognized value keeps the default rather than guessing, matching
    /// how `Durability::parse` treats a typo — a mistyped budget should not
    /// quietly turn the sweep into a no-op.
    ///
    /// Note that includes `0`, which therefore means [`SWEEP_ENTRY_BUDGET`] and
    /// *not* "don't walk", even though it reads like the latter. Turning the
    /// sweep off is [`Self::SWEEP_ENV_VAR`]'s job: a budget of zero would be a
    /// second, sideways spelling of off, and one switch per decision is the
    /// whole reason these are two variables.
    fn parse_budget(value: Option<&str>) -> usize {
        value
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(SWEEP_ENTRY_BUDGET)
    }

    /// Whether a value spells one of the falsey settings. Same spellings
    /// [`Durability::ENV_VAR`] accepts — one convention for the whole storage
    /// backend, not one per switch.
    fn env_says_off(value: &str) -> bool {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no"
        )
    }

    /// Reclaim staging files orphaned by an earlier crash under this
    /// storage's base path — see [`sweep_orphaned_staging_files`] for what
    /// the walk will and will not delete, and [`Self::SWEEP_ENV_VAR`] /
    /// [`Self::SWEEP_BUDGET_ENV_VAR`] to disable or widen it.
    ///
    /// **A deliberate startup action, not a constructor side effect.** The
    /// sweep unlinks files, and every argument for its rules leans on it
    /// being a startup decision — so it is mounted where startup is actually
    /// known to be happening: the file arms of `create_sync_connection` /
    /// `create_async_connection` in `fluree-db-connection`, and the
    /// file-backed build paths in `fluree-db-api`. Constructing a
    /// `FileStorage` deletes nothing, so a unit test or a tool holding a
    /// handle on a directory it does not own cannot unlink an operator's
    /// files by existing.
    ///
    /// The walk is bounded ([`SWEEP_ENTRY_BUDGET`]), runs at most once per
    /// base path per process however many handles startup opens on it, and
    /// is handed to the blocking pool when there is a runtime to hand it to,
    /// so the caller never waits on it.
    ///
    /// Returns the spawned task when the walk was handed to the blocking pool,
    /// so a test can await it. Production ignores it — the sweep is best-effort
    /// and nothing waits on the result.
    pub fn sweep_orphaned_staging(&self) -> Option<tokio::task::JoinHandle<()>> {
        let budget = Self::sweep_budget_from_env()?;
        if !claim_sweep(&self.base_path) {
            return None;
        }
        let base = self.base_path.clone();
        // A RECURSIVE `read_dir` IS BLOCKING I/O, AND THIS IS CALLED FROM
        // ASYNC STARTUP (`create_async_connection`, and the API's async
        // client builds), so running it on the caller would park a runtime
        // worker for the whole walk — measured at ~1s warm on local APFS for
        // the default budget, and a readdir on the shared mount this design
        // targets is a network round trip rather than a page-cache hit.
        // #1620 asked for the walk to be bounded *or* backgrounded; it is
        // worth being both.
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => Some(handle.spawn_blocking(move || Self::run_sweep(&base, budget))),
            // No runtime: a plain synchronous caller (`create_sync_connection`),
            // which can afford to block on its own thread.
            Err(_) => {
                Self::run_sweep(&base, budget);
                None
            }
        }
    }

    /// The walk itself, plus what it reports.
    fn run_sweep(base_path: &Path, budget: usize) {
        let sweep = sweep_orphaned_staging_files(base_path, STALE_STAGING_AGE, budget);
        // Silence is the normal case, and a startup line per storage would be
        // noise. Reclaiming is routine good news at `info`.
        if sweep.reclaimed > 0 {
            tracing::info!(
                base_path = %base_path.display(),
                reclaimed = sweep.reclaimed,
                kept = sweep.kept,
                "reclaimed staging files orphaned by an interrupted write"
            );
        }
        // Truncation is a different event and a worse one: the walk has no
        // cursor, so this base path stays under-swept on every future start
        // until the budget is raised. That is not a deferral, so it does not
        // get to look like one.
        if sweep.truncated {
            tracing::warn!(
                base_path = %base_path.display(),
                budget,
                env_var = Self::SWEEP_BUDGET_ENV_VAR,
                "staging-file sweep hit its entry budget and stopped; entries past it are not \
                 reached on this or any later start. Raise the budget to cover the tree."
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

    /// Journal a root that other processes journal too, under a log of this
    /// handle's own. For a Raft cluster's shared payload store: each node
    /// passes its own id. An owned log flushes on every write, because the
    /// head that would otherwise flush on a payload's behalf lives in Raft,
    /// not in a file under this root; a payload is therefore durable before
    /// its reference is proposed, at one flush instead of two. Any node
    /// applies a stopped node's unflushed tail when it opens the root or
    /// misses a file.
    ///
    /// Owner names are 1-64 characters of `[A-Za-z0-9._-]` not starting with
    /// a dot; an invalid name makes the first write fail.
    pub fn with_wal_owner(mut self, owner: impl Into<String>) -> Self {
        self.wal_owner = Some(Arc::from(owner.into()));
        self
    }

    /// Flush calls issued by this storage since it was constructed, counting
    /// both the staged file and its parent directory, and under
    /// [`Durability::Wal`] every flush of the root's WAL, including the
    /// background ones that retire its segments. On Apple platforms a
    /// retirement batch hands each file to the drive with `fsync(2)` and
    /// commits them with one `F_FULLFSYNC`; both count, though only the
    /// latter is a drive-cache flush.
    ///
    /// Stays at zero under [`Durability::PageCache`] and for derived content in
    /// any mode. Exposed because a flush leaves no trace in the bytes on
    /// disk, so this is the only way to tell a durable write from a cheap one.
    pub fn fsyncs_issued(&self) -> u64 {
        let own = self.fsyncs.load(Ordering::Relaxed);
        match self.wal.get() {
            Some(WalAttach::Log(log)) => own + log.fsyncs_issued(),
            _ => own,
        }
    }

    /// The durability writes actually get. [`Durability::Wal`] reads as
    /// [`Durability::Sync`] once this root's log turned out to be unavailable.
    pub fn effective_durability(&self) -> Durability {
        match (self.durability, self.wal.get()) {
            (Durability::Wal, Some(WalAttach::Fallback)) => Durability::Sync,
            (durability, _) => durability,
        }
    }

    /// Replay the WAL an earlier run left under this root, if any, so
    /// state acknowledged before a crash is on disk before the first read.
    ///
    /// A startup action like [`Self::sweep_orphaned_staging`]: the connection
    /// and builder paths call it, constructing a handle never does. Blocking.
    /// Replays under any durability setting, so an operator who switched back
    /// to per-write flushing after a crash still sees the acknowledged tail.
    /// Leaves no trace on a root that never journaled.
    pub fn recover_wal(&self) -> Result<()> {
        if self.durability == Durability::Wal {
            self.attach_wal(false)?;
        } else {
            // Replay and let go: this handle is not going to journal.
            Wal::acquire(&self.base_path, self.wal_owner.as_deref(), false)
                .map(drop)
                .map_err(|e| Self::recovery_error(&self.base_path, e))?;
        }
        // A root several processes journal: apply what a stopped one left.
        wal::replay_unowned(&self.base_path)
            .map(drop)
            .map_err(|e| Self::recovery_error(&self.base_path, e))
    }

    /// Apply the unflushed tail of any owner that is no longer running, for
    /// a file that turned out to be missing. Only a shared root has owners,
    /// so a standalone root pays one directory probe per miss and no more.
    /// Returns whether anything was applied, so the caller can retry.
    async fn replay_foreign_logs(&self) -> Result<bool> {
        let base = self.base_path.clone();
        tokio::task::spawn_blocking(move || {
            wal::replay_unowned(&base)
                .map(|records| records > 0)
                .map_err(|e| Self::recovery_error(&base, e))
        })
        .await
        .map_err(|e| crate::error::Error::io(format!("WAL replay join: {e}")))?
    }

    fn recovery_error(base: &Path, e: std::io::Error) -> crate::error::Error {
        crate::error::Error::storage(format!("WAL recovery failed for {}: {e}", base.display()))
    }

    /// Own this root's WAL, replaying what an earlier run left behind.
    ///
    /// `None` means writes flush per file instead: the WAL is off for this
    /// handle, another process holds the log, the filesystem cannot support
    /// it, or (with `create` false) there is no log to speak of. Blocking, so
    /// the write paths call it from their blocking hop.
    fn attach_wal(&self, create: bool) -> Result<Option<Arc<Wal>>> {
        if self.durability != Durability::Wal {
            return Ok(None);
        }
        let attached = match self.wal.get() {
            Some(attach) => attach,
            None => {
                let attach = match Wal::acquire(&self.base_path, self.wal_owner.as_deref(), create)
                    .map_err(|e| Self::recovery_error(&self.base_path, e))?
                {
                    Acquire::Log(log) => WalAttach::Log(log),
                    Acquire::Absent => return Ok(None),
                    Acquire::Busy => {
                        tracing::warn!(
                            root = %self.base_path.display(),
                            "another process holds this root's WAL; this handle flushes per write instead"
                        );
                        WalAttach::Fallback
                    }
                    Acquire::Unsupported(e) => {
                        tracing::warn!(
                            root = %self.base_path.display(),
                            error = %e,
                            "WAL unavailable here; this handle flushes per write instead"
                        );
                        WalAttach::Fallback
                    }
                };
                // A racing clone may have attached first; either way the root
                // has one answer from here on.
                self.wal.get_or_init(|| attach)
            }
        };
        Ok(match attached {
            WalAttach::Log(log) => Some(Arc::clone(log)),
            WalAttach::Fallback => None,
        })
    }

    /// How a write of `len` bytes lands under `durability`: the log it is
    /// appended to first, if any, and the policy the file is then written with.
    /// A record too large for the log is flushed directly instead, once the
    /// log is checkpointed: its earlier appends must be durable before a head
    /// published this way is, and nothing left to replay may touch the key
    /// the write lands on, or a crash would undo a write the caller was told
    /// is durable.
    fn write_plan(
        &self,
        durability: Durability,
        len: usize,
    ) -> Result<(WritePolicy, Option<Arc<Wal>>)> {
        if durability != Durability::Wal {
            return Ok((self.policy(durability), None));
        }
        if let Some(log) = self.attach_wal(true)? {
            if len <= wal::MAX_RECORD_BYTES {
                return Ok((self.policy(Durability::PageCache), Some(log)));
            }
            let waiting = log.checkpoint().map_err(|e| {
                crate::error::Error::io(format!("WAL checkpoint before an oversized write: {e}"))
            })?;
            if waiting > 0 {
                return Err(crate::error::Error::io(format!(
                    "WAL checkpoint before an oversized write left {waiting} segment(s) with \
                     records still materializing; refusing a write that replay could undo"
                )));
            }
            #[cfg(test)]
            if let Some(hook) = self.after_checkpoint.0.lock().unwrap().as_ref() {
                hook();
            }
        }
        Ok((self.policy(Durability::Sync), None))
    }

    /// Flush everything the WAL under `root` still covers and retire its
    /// segments, so the root reads the same to a binary that knows nothing
    /// about the log. The shutdown hook for a WAL-owning process; dropping the
    /// last handle does the same, but a handle a background task still holds
    /// would keep the log open until that task ends. Blocking. A root this
    /// process is not the WAL is left alone.
    pub fn checkpoint_wal(root: impl AsRef<Path>) -> Result<bool> {
        wal::checkpoint_root(root.as_ref()).map_err(|e| Self::recovery_error(root.as_ref(), e))
    }

    /// Abandon the WAL without flushing, as a crash would, so a test can
    /// reopen the root and exercise replay within one process.
    #[doc(hidden)]
    pub fn simulate_crash_for_test(&self) {
        if let Some(WalAttach::Log(log)) = self.wal.get() {
            log.simulate_crash();
        }
    }

    /// Keep WAL segments until close, so a test that crashes the log finds
    /// its records still there however slowly it runs. Attaches the log.
    #[doc(hidden)]
    pub fn hold_wal_segments_for_test(&self) -> Result<()> {
        if let Some(log) = self.attach_wal(true)? {
            log.hold_segments();
        }
        Ok(())
    }

    /// Run `f` on the writing thread between an oversized write's checkpoint
    /// and its write. Shared across clones.
    #[cfg(test)]
    pub(crate) fn set_after_checkpoint_hook_for_test(&self, f: impl Fn() + Send + Sync + 'static) {
        *self.after_checkpoint.0.lock().unwrap() = Some(Box::new(f));
    }

    /// The root log's hold on `key` for an operation logged under
    /// `durability`, or nothing when the operation is not logged. Take it
    /// before the operation's record and keep it past its file operation;
    /// see [`Wal::key_stripe`]. Blocking; attaches the log.
    fn hold_key(&self, durability: Durability, key: &str) -> Result<Option<KeyHold>> {
        if durability != Durability::Wal {
            return Ok(None);
        }
        Ok(self.attach_wal(true)?.map(|log| log.key_stripe(key)))
    }

    /// Delay every WAL device flush by `delay`, so a test can widen the
    /// window in which concurrent commits pile up behind one flush.
    /// Attaches the log.
    #[doc(hidden)]
    pub fn slow_wal_sync_for_test(&self, delay: std::time::Duration) -> Result<()> {
        if let Some(log) = self.attach_wal(true)? {
            log.slow_sync(delay);
        }
        Ok(())
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
        self.resolve_key(address).map(|(_, path)| path)
    }

    /// Resolve an address to the root-relative key the WAL records and
    /// the file path it names.
    fn resolve_key(&self, address: &str) -> Result<(String, std::path::PathBuf)> {
        let key = match Self::extract_path_from_address(address) {
            Some(path) => path.to_owned(),
            // Simple case: just a node ID, look for it as a .json file
            None => format!("{address}.json"),
        };
        let path = self.resolve_relative_path(&key)?;
        Ok((key, path))
    }

    fn resolve_relative_path(&self, path: &str) -> Result<std::path::PathBuf> {
        use std::path::Component;
        let p = std::path::Path::new(path);

        // Disallow absolute paths, path traversal, and the log's own directory.
        if p.is_absolute()
            || p.components().any(|c| {
                matches!(
                    c,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                ) || c.as_os_str() == WAL_DIR
            })
        {
            return Err(crate::error::Error::storage(format!(
                "Invalid storage path '{path}': must be a relative path without '..' or '{WAL_DIR}'"
            )));
        }

        Ok(self.base_path.join(p))
    }
}

#[async_trait]
impl StorageRead for FileStorage {
    async fn read_bytes(&self, address: &str) -> Result<Vec<u8>> {
        let path = self.resolve_path(address)?;
        let mut read = tokio::fs::read(&path).await;
        if read
            .as_ref()
            .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound)
            && self.replay_foreign_logs().await?
        {
            read = tokio::fs::read(&path).await;
        }
        let bytes = read.map_err(|e| {
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
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // On a shared root a stopped owner's log may still hold this
                // file. Applying it blocks, but only on a miss, and only when
                // the root has owned logs at all.
                match wal::replay_unowned(&self.base_path) {
                    Ok(applied) if applied > 0 => std::fs::metadata(&path)
                        .ok()
                        .filter(|m| m.len() > 0)
                        .map(|_| path),
                    _ => None,
                }
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

        /// One attempt; the caller retries once after applying a stopped
        /// owner's log on a miss (see `replay_foreign_logs`).
        async fn read_once(
            path: PathBuf,
            address: String,
            offset: u64,
            requested: u64,
        ) -> Result<Vec<u8>> {
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

        match read_once(path.clone(), address.to_owned(), offset, requested).await {
            Err(crate::error::Error::NotFound(_)) if self.replay_foreign_logs().await? => {
                read_once(path, address.to_owned(), offset, requested).await
            }
            first => first,
        }
    }

    fn supports_ranged_reads(&self) -> bool {
        true
    }

    async fn exists(&self, address: &str) -> Result<bool> {
        let path = self.resolve_path(address)?;
        let mut metadata = tokio::fs::metadata(&path).await;
        if metadata
            .as_ref()
            .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound)
            && self.replay_foreign_logs().await?
        {
            metadata = tokio::fs::metadata(&path).await;
        }
        match metadata {
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
                    // Log segments are not content and never carry an address.
                    if entry.file_name() != WAL_DIR {
                        dirs_to_visit.push(path);
                    }
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

    /// Flush the derived content written since the last call, file by file
    /// plus the directories between them and the root, and the log's
    /// unflushed tail. Source-of-truth content is otherwise durable no later
    /// than the next head publication through this root; a caller publishing
    /// its pointer elsewhere gets the same guarantee from this barrier.
    async fn sync(&self) -> Result<()> {
        // The guard travels into the blocking task below: cancelling this
        // future must not release the barrier while the flush is still
        // running, or a later caller would see nothing pending and return
        // before its files were on the device.
        let one_at_a_time = Arc::clone(&self.flushing).lock_owned().await;
        let log = match self.wal.get() {
            Some(WalAttach::Log(log)) => Some(Arc::clone(log)),
            _ => None,
        };
        let keys: Vec<String> = {
            let mut pending = self
                .unflushed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut keys: Vec<String> = std::mem::take(&mut *pending);
            keys.sort_unstable();
            keys.dedup();
            keys
        };
        if keys.is_empty() && log.is_none() {
            return Ok(());
        }
        let base = self.base_path.clone();
        let fsyncs = Arc::clone(&self.fsyncs);
        let unflushed = Arc::clone(&self.unflushed);
        tokio::task::spawn_blocking(move || {
            let _one_at_a_time = one_at_a_time;
            let flushed = match &log {
                Some(log) => log.flush().map_err(|e| format!("flush WAL: {e}")),
                None => Ok(()),
            }
            .and_then(|()| {
                wal::flush_keys(&base, &keys, &fsyncs)
                    .map_err(|e| format!("flush derived content: {e}"))
            });
            match flushed {
                Ok(()) => Ok(()),
                Err(e) => {
                    // The batch is still owed: a retry must flush it, and a
                    // publish must not go ahead on a success that never was.
                    let mut pending = unflushed
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let mut owed = keys;
                    owed.append(&mut pending);
                    *pending = owed;
                    Err(crate::error::Error::io(e))
                }
            }
        })
        .await
        .map_err(|e| crate::error::Error::io(format!("sync join: {e}")))?
    }

    async fn delete(&self, address: &str) -> Result<()> {
        let (key, path) = self.resolve_key(address)?;
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            let _key = storage.hold_key(storage.durability, &key)?;
            let (_, log) = storage.write_plan(storage.durability, 0)?;
            let _appended = match &log {
                // Ordered after the writes it undoes, so replay cannot bring
                // the file back. Covered by the next flush, which is no weaker
                // than an unlink that was never followed by a directory flush.
                Some(log) => {
                    Some(log.append(Op::Delete { key: &key }, false).map_err(|e| {
                        crate::error::Error::io(format!("WAL append for {key}: {e}"))
                    })?)
                }
                None => None,
            };
            match std::fs::remove_file(&path) {
                Ok(()) => Ok(()),
                // Idempotent: not found is OK
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(crate::error::Error::io(format!(
                    "Failed to delete {}: {}",
                    path.display(),
                    e
                ))),
            }
        })
        .await
        .map_err(|e| crate::error::Error::io(format!("delete join: {e}")))?
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
        let durability = self.durability_for(kind);
        self.write_bytes_durable(&address, bytes, durability)
            .await?;
        // Derived content on a durable instance is flushed later, by `sync`,
        // in one batch before the pointer that names it is published.
        if durability == Durability::PageCache && self.durability != Durability::PageCache {
            let (key, _) = self.resolve_key(&address)?;
            self.unflushed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(key);
        }
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
        let (key, path) = self.resolve_key(address)?;
        let bytes = bytes.to_vec();
        let for_err = path.clone();
        let storage = self.clone();

        // One blocking hop for mkdir + log + stage + rename, rather than one
        // per `tokio::fs` call. Attaching the log may replay, so that is in
        // here too.
        tokio::task::spawn_blocking(move || {
            let _key = storage.hold_key(durability, &key)?;
            let (policy, log) = storage.write_plan(durability, bytes.len())?;
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    crate::error::Error::io(format!(
                        "Failed to create directory {}: {}",
                        parent.display(),
                        e
                    ))
                })?;
            }
            // Logged before it lands, and not flushed: the head publication
            // that follows flushes the log, and a crash before then leaves an
            // unreferenced file at worst. The guard keeps the record's segment
            // from being retired until the file is written.
            let _appended = match &log {
                Some(log) => Some(
                    log.append(
                        Op::Write {
                            key: &key,
                            bytes: &bytes,
                        },
                        false,
                    )
                    .map_err(|e| crate::error::Error::io(format!("WAL append for {key}: {e}")))?,
                ),
                None => None,
            };
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
    async fn blocking_insert(
        &self,
        key: String,
        path: PathBuf,
        bytes: Vec<u8>,
    ) -> StorageExtResult<bool> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            let _key = storage
                .hold_key(storage.durability, &key)
                .map_err(|e| StorageExtError::io(e.to_string()))?;
            let (policy, log) = storage
                .write_plan(storage.durability, bytes.len())
                .map_err(|e| StorageExtError::io(e.to_string()))?;
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    StorageExtError::io(format!("mkdir {}: {}", parent.display(), e))
                })?;
            }
            // The same sidecar lock a compare-and-swap on this key holds and
            // replay takes: between the link and the record no other writer
            // may advance the file, or the log would carry their transition
            // ahead of the creation it builds on.
            let _key_lock = wal::key_lock(&path)
                .map_err(|e| StorageExtError::io(format!("lock {}: {}", path.display(), e)))?;
            let created = create_new_atomic(&path, &bytes, &policy)
                .map_err(|e| StorageExtError::io(format!("write {}: {}", path.display(), e)))?;
            if created {
                if let Some(log) = &log {
                    // The file decides the race, so only the caller whose
                    // link won logs; replay then installs what won. Logged
                    // after the file exists, and flushed here: an insert is
                    // a lifecycle event (a ledger coming into existence)
                    // with nothing after it to flush on its behalf.
                    let _appended = log
                        .append(
                            Op::Insert {
                                key: &key,
                                bytes: &bytes,
                            },
                            true,
                        )
                        .map_err(|e| StorageExtError::io(format!("WAL append for {key}: {e}")))?;
                }
            }
            Ok(created)
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
        key: String,
        path: PathBuf,
    ) -> StorageExtResult<(Option<Vec<u8>>, LockedFile)> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            // Attaching replays the leftover log, and replay takes the same
            // sidecar lock for a record on this key; it has to come first.
            let key_hold = storage
                .hold_key(storage.durability, &key)
                .map_err(|e| StorageExtError::io(e.to_string()))?;
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
                    key,
                    path,
                    _lock_file: lock_file,
                    _key_hold: key_hold,
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
    /// when the `LockedFile` guard is dropped at the end. `expected` is what
    /// the read under that lock returned, so the log can replay the transition
    /// only against the state it was made from.
    async fn blocking_locked_write(
        &self,
        locked: LockedFile,
        expected: Option<Vec<u8>>,
        new_bytes: Vec<u8>,
    ) -> StorageExtResult<()> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            let (policy, log) = storage
                .write_plan(storage.durability, new_bytes.len())
                .map_err(|e| StorageExtError::io(e.to_string()))?;
            // The one flush a commit pays. It lands before the file does, so
            // a head on disk always has its log record — and every content
            // append before it — on disk too.
            let appended = match &log {
                Some(log) => Some(
                    log.append(
                        Op::Cas {
                            key: &locked.key,
                            expected: expected.as_deref(),
                            new: &new_bytes,
                        },
                        true,
                    )
                    .map_err(|e| {
                        StorageExtError::io(format!("WAL append for {}: {e}", locked.key))
                    })?,
                ),
                None => None,
            };
            if let Err(e) = write_atomic(&locked.path, &new_bytes, &policy) {
                // Staging or the rename failed, so the file still holds
                // `expected` while the log says it moved. Cancel the record
                // before a retry logs its own transition from the same value.
                if let (Some(log), Some(appended)) = (&log, &appended) {
                    log.cancel(appended.seq, &locked.key).map_err(|cancel| {
                        StorageExtError::io(format!(
                            "write {}: {e}; WAL cancel also failed: {cancel}",
                            locked.path.display()
                        ))
                    })?;
                }
                return Err(StorageExtError::io(format!(
                    "write {}: {e}",
                    locked.path.display()
                )));
            }
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
    key: String,
    path: PathBuf,
    _lock_file: std::fs::File,
    /// The log's hold on the key, released with the flock.
    _key_hold: Option<KeyHold>,
}

#[async_trait]
impl StorageCas for FileStorage {
    async fn insert(&self, address: &str, bytes: &[u8]) -> StorageExtResult<bool> {
        let (key, path) = self
            .resolve_key(address)
            .map_err(|e| StorageExtError::io(e.to_string()))?;
        self.blocking_insert(key, path, bytes.to_vec()).await
    }

    async fn compare_and_swap<T, F>(&self, address: &str, f: F) -> StorageExtResult<CasOutcome<T>>
    where
        F: Fn(Option<&[u8]>) -> std::result::Result<CasAction<T>, StorageExtError> + Send + Sync,
        T: Send,
    {
        let (key, path) = self
            .resolve_key(address)
            .map_err(|e| StorageExtError::io(e.to_string()))?;

        // Phase 1: acquire lock + read (blocking)
        let (current, locked) = self.blocking_locked_read(key, path).await?;

        // Phase 2: call closure on async task
        match f(current.as_deref())? {
            CasAction::Write(new_bytes) => {
                // Phase 3: write under same lock (blocking)
                self.blocking_locked_write(locked, current, new_bytes)
                    .await?;
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
    fn durability_defaults_to_wal() {
        assert_eq!(Durability::default(), Durability::Wal);
        assert_eq!(Durability::parse(None), Durability::Wal);
    }

    #[test]
    fn durability_env_opts_out_on_falsey_spellings() {
        for v in ["0", "false", "off", "no", "OFF", " false "] {
            assert_eq!(Durability::parse(Some(v)), Durability::PageCache, "{v:?}");
        }
        // Per-write flushing has to be asked for by name.
        for v in ["sync", "fsync", "direct", " SYNC "] {
            assert_eq!(Durability::parse(Some(v)), Durability::Sync, "{v:?}");
        }
        // Anything else keeps the safe setting rather than guessing.
        for v in ["1", "true", "on", "", "nonsense", "journal", "wal"] {
            assert_eq!(Durability::parse(Some(v)), Durability::Wal, "{v:?}");
        }
    }

    /// Environment beats configuration beats the default, so an operator can
    /// override a checked-in config file for one run without editing it.
    #[test]
    fn durability_precedence_is_env_then_config_then_default() {
        use Durability::{PageCache, Sync, Wal};
        assert_eq!(Durability::resolve_from(None, None), Wal);
        assert_eq!(Durability::resolve_from(None, Some(PageCache)), PageCache);
        assert_eq!(Durability::resolve_from(Some(Sync), Some(PageCache)), Sync);
        assert_eq!(
            Durability::resolve_from(Some(PageCache), Some(Sync)),
            PageCache
        );
    }

    #[test]
    fn durability_mode_names_parse_and_reject() {
        use Durability::{PageCache, Sync, Wal};
        assert_eq!(Durability::from_mode_name("journal"), Some(Wal));
        assert_eq!(Durability::from_mode_name("WAL"), Some(Wal));
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
        for mode in [Durability::Wal, Durability::Sync, Durability::PageCache] {
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
    /// exactly why nothing ever removed it. The explicit startup sweep has to.
    ///
    /// The intermediate assertion is a pin of its own: **constructing a
    /// storage deletes nothing.** A bare `FileStorage::new` is reachable from
    /// unit tests and tooling pointed at directories the process does not own
    /// (a Debug-formatting test on a hardcoded `/tmp/test` was the
    /// demonstrated case), so the unlink must wait for the deliberate call.
    #[test]
    fn construction_is_pure_and_the_explicit_sweep_reclaims_an_orphan() {
        let dir = tempfile::tempdir().unwrap();
        let orphan = plant_staging_file(
            &dir.path().join("a/b"),
            "leaf.json",
            "0123456789abcdef",
            STALE_STAGING_AGE * 2,
        );

        let storage = FileStorage::new(dir.path());
        assert!(
            orphan.exists(),
            "constructing a FileStorage must not delete anything: {}",
            orphan.display()
        );

        // No runtime in a plain #[test], so the walk runs inline and is
        // finished when the call returns.
        storage.sweep_orphaned_staging();
        assert!(
            !orphan.exists(),
            "an orphan older than the threshold survived the startup sweep: {}",
            orphan.display()
        );
    }

    /// THE ONE THAT MATTERS. Storage is shared in a multi-instance deployment,
    /// so a sweep that deletes a staging file another process is still writing
    /// takes that process's rename out from under it. A file young enough to
    /// belong to a live write is not the sweep's business at any age policy.
    #[test]
    fn the_sweep_leaves_a_live_looking_staging_file_alone() {
        let dir = tempfile::tempdir().unwrap();
        // Another instance's token, written a moment ago — the shape of a
        // write that is in flight right now.
        let live = plant_staging_file(
            dir.path(),
            "commit.json",
            "fedcba9876543210",
            Duration::from_secs(0),
        );

        FileStorage::new(dir.path()).sweep_orphaned_staging();

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

        // The pre-`b0a9c416a` format put the pid where the token now sits. It
        // still parses, so an orphan written by that build is still reclaimable
        // — and it is plainly not ours.
        assert_eq!(staging_token("leaf.json.999.0.tmp"), Some("999"));
        assert_ne!(
            staging_token("leaf.json.999.0.tmp"),
            Some(own_token().as_str())
        );

        // Enough dots is not the same as the right shape: a foreign name with
        // the same arity must not slip through on arity alone.
        assert_eq!(staging_token("subjects.offsets.0.tmp"), None);
        assert_eq!(staging_token("a.b.c.tmp"), None);
        assert_eq!(staging_token(".4242.7.tmp"), None, "no destination name");
        assert_eq!(staging_token("plain.tmp"), None);
        assert_eq!(staging_token("leaf.json"), None);

        // Zero-padded numbers are not something this backend emits, and
        // rejecting them is what keeps date- and offset-stamped foreign files
        // out of the legacy branch.
        assert_eq!(staging_token("backup.2026.08.tmp"), None);
        assert_eq!(staging_token("wal.000001.000002.tmp"), None);
        // ...while a sequence number legitimately starting at zero still parses.
        assert_eq!(staging_token("leaf.json.999.0.tmp"), Some("999"));
        assert_eq!(staging_token("leaf.json.999.00.tmp"), None);
    }

    /// The staging name is built from a caller-supplied destination, so the
    /// shape check must not reject the odd but legal names a real ledger
    /// produces. Driven through `tmp_sibling` itself so it cannot drift.
    #[test]
    fn the_shape_check_accepts_every_destination_name_we_can_stage() {
        for destination in [
            "leaf",
            "a.json.gz",
            "many.dots.in.here.json",
            ".hidden",
            "UPPER.JSON",
            "with space.json",
            "ünïcødé.json",
            "trailing.",
            "4242",
            "0123456789abcdef",
            "-",
            "_",
        ] {
            let name = tmp_sibling(Path::new("/data").join(destination).as_path())
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string();
            assert_eq!(
                staging_token(&name),
                Some(own_token().as_str()),
                "{destination:?} staged as {name:?} and was not recognised as ours"
            );
        }
    }

    /// THE OTHER ONE THAT MATTERS. `.tmp` is a suffix, not a namespace: the
    /// indexer's vocab merge, the disk cache, the nameservice tracking file and
    /// the Raft log all stage under it, and the nameservice shares this very
    /// tree — `FileNameService::new` builds a `FileStorage` on the path
    /// `FlureeBuilder::build` also hands to storage. If the sweep predicated on
    /// the extension, the age threshold would be the only thing between all of
    /// them and an unlink. The vocab-merge entries are the sharp case: index
    /// build temporaries are exactly the long-running shape `STALE_STAGING_AGE`
    /// argues an age threshold must not judge.
    #[test]
    fn the_sweep_ignores_tmp_files_other_subsystems_wrote() {
        let dir = tempfile::tempdir().unwrap();
        let foreign = [
            "notes.tmp",
            "report.tmp",
            "subjects.offsets.tmp", // vocab_merge.rs
            "strings.lens.tmp",     // vocab_merge.rs
            ".cas_4242_7.tmp",      // disk_cache.rs
            "myledger.json.tmp",    // tracking_file.rs
            "snap-0.tmp",           // fluree-raft-core storage/fs.rs
            // Same arity as one of ours, but the segments are the wrong shape.
            "subjects.offsets.0.tmp",
        ];
        for name in &foreign {
            let path = dir.path().join(name);
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(b"someone else's in-flight work").unwrap();
            f.set_modified(SystemTime::now() - STALE_STAGING_AGE * 10)
                .unwrap();
        }

        // Threshold at zero, so age offers no protection at all — the shape
        // check is the only thing on trial.
        let sweep = sweep_orphaned_staging_files(dir.path(), Duration::ZERO, SWEEP_ENTRY_BUDGET);

        assert_eq!(
            sweep.reclaimed, 0,
            "the sweep unlinked another subsystem's staging file"
        );
        for name in &foreign {
            assert!(dir.path().join(name).exists(), "{name} was reclaimed");
        }
    }

    /// The shape check must not cost us the orphans the sweep exists for: a
    /// foreign-token file in our own naming is still reclaimed, sitting in the
    /// same directory as the files that must survive.
    #[test]
    fn the_shape_check_still_reclaims_our_own_orphans() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("subjects.offsets.tmp"), b"theirs").unwrap();
        let ours = plant_staging_file(
            dir.path(),
            "leaf.json",
            "fedcba9876543210",
            STALE_STAGING_AGE * 2,
        );
        // And the legacy `<name>.<pid>.<seq>.tmp` shape, which predates the
        // token and must still be reclaimable.
        let legacy = dir.path().join(format!("old.json.999.0{TMP_SUFFIX}"));
        let mut f = std::fs::File::create(&legacy).unwrap();
        f.write_all(b"legacy orphan").unwrap();
        f.set_modified(SystemTime::now() - STALE_STAGING_AGE * 2)
            .unwrap();

        let sweep = sweep_orphaned_staging_files(dir.path(), STALE_STAGING_AGE, SWEEP_ENTRY_BUDGET);

        assert!(!ours.exists(), "a real orphan survived the shape check");
        assert!(!legacy.exists(), "a legacy-format orphan survived");
        assert_eq!(sweep.reclaimed, 2);
        assert!(
            dir.path().join("subjects.offsets.tmp").exists(),
            "the foreign file next to them was taken"
        );
    }

    /// The sweep is called from inside `create_async_connection` and the
    /// API's async client builds, so a synchronous recursive `read_dir` there
    /// parks a runtime worker for the whole walk. Awaiting the handle rather
    /// than polling for the file keeps this deterministic.
    #[tokio::test]
    async fn the_walk_is_handed_to_the_blocking_pool_when_a_runtime_exists() {
        let dir = tempfile::tempdir().unwrap();
        let orphan = plant_staging_file(
            dir.path(),
            "leaf.json",
            "fedcba9876543210",
            STALE_STAGING_AGE * 2,
        );

        let handle = FileStorage::new(dir.path())
            .sweep_orphaned_staging()
            .expect("a sweep inside a runtime must be spawned, not run inline");
        handle.await.expect("sweep task panicked");

        assert!(!orphan.exists(), "the spawned sweep did not run");
    }

    /// Without a runtime there is nothing to hand the walk to, and a plain
    /// synchronous caller can afford to block — so it runs inline and is
    /// *finished* by the time the call returns. Asserting the orphan is
    /// already gone is the part that carries the coverage; `is_none()` alone
    /// would also be satisfied by a sweep that never ran.
    #[test]
    fn the_walk_runs_inline_without_a_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let orphan = plant_staging_file(
            dir.path(),
            "leaf.json",
            "fedcba9876543210",
            STALE_STAGING_AGE * 2,
        );

        assert!(
            FileStorage::new(dir.path())
                .sweep_orphaned_staging()
                .is_none(),
            "no runtime, so there is no task to return"
        );
        assert!(
            !orphan.exists(),
            "the inline sweep had not finished when the call returned"
        );
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

    /// An operator who sees the truncation warning needs something to do about
    /// it. Note `"1"` means a budget of one only because it arrives through the
    /// budget variable — through the on/off switch it would mean "on", which is
    /// exactly why these are two variables and not one.
    #[test]
    fn sweep_budget_env_var_parses_a_size() {
        assert_eq!(FileStorage::parse_budget(Some("5000000")), 5_000_000);
        assert_eq!(FileStorage::parse_budget(Some(" 250 ")), 250);
        // Unset, mistyped, or a zero that would quietly make the sweep a no-op
        // all keep the default rather than guessing.
        for v in [None, Some(""), Some("nonsense"), Some("0"), Some("-5")] {
            assert_eq!(FileStorage::parse_budget(v), SWEEP_ENTRY_BUDGET, "{v:?}");
        }
    }

    /// The threshold is the only thing standing between a foreign in-flight
    /// write and deletion, so it has to be the age that decides — not merely
    /// "is this file ours".
    #[test]
    fn only_files_past_the_threshold_are_reclaimed() {
        let dir = tempfile::tempdir().unwrap();
        let threshold = Duration::from_secs(3600);
        let young = plant_staging_file(dir.path(), "young.json", "aaaaaaaaaaaaaaaa", threshold / 2);
        let old = plant_staging_file(dir.path(), "old.json", "bbbbbbbbbbbbbbbb", threshold * 2);

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
        let path = dir
            .path()
            .join(format!("skewed.json.7.cccccccccccccccc.0{TMP_SUFFIX}"));
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

    /// The walk is on the startup path, so a huge volume must not turn
    /// opening a ledger into a startup stall. Running out of budget stops the
    /// walk and says so, rather than running to completion.
    #[test]
    fn the_walk_is_bounded_by_its_entry_budget() {
        let dir = tempfile::tempdir().unwrap();
        for i in 0..8 {
            plant_staging_file(
                dir.path(),
                &format!("o{i}.json"),
                "dddddddddddddddd",
                STALE_STAGING_AGE * 2,
            );
        }

        let sweep = sweep_orphaned_staging_files(dir.path(), Duration::ZERO, 3);

        assert!(sweep.truncated, "the budget did not stop the walk");
        assert_eq!(sweep.reclaimed, 3, "the walk went past its budget");
    }

    /// One walk per base path per process. Startup opens several handles on
    /// one directory and more than one startup layer sweeps on the way up;
    /// re-walking the tree each time would only re-examine the files the
    /// first walk declined to touch.
    #[test]
    fn a_base_path_is_swept_at_most_once_per_process() {
        let dir = tempfile::tempdir().unwrap();
        assert!(claim_sweep(dir.path()), "first claim must win");
        assert!(!claim_sweep(dir.path()), "second claim must be refused");
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

#[cfg(all(test, unix))]
mod wal_tests {
    use super::*;
    use crate::{CasAction, CasOutcome, StorageCas, StorageRead, StorageWrite};

    const TXN: &str = "fluree:file://ledger/txn/aaaa.bin";
    const COMMIT: &str = "fluree:file://ledger/commit/bbbb.bin";
    const HEAD: &str = "fluree:file://ns@v2/ledger/main.json";

    fn with_wal(dir: &Path) -> FileStorage {
        let storage = FileStorage::new(dir).with_durability(Durability::Wal);
        storage.recover_wal().unwrap();
        storage
    }

    async fn publish(storage: &FileStorage, head: &[u8]) {
        let head = head.to_vec();
        let outcome = storage
            .compare_and_swap(HEAD, |_| Ok(CasAction::Write::<()>(head.clone())))
            .await
            .unwrap();
        assert!(matches!(outcome, CasOutcome::Written));
    }

    /// One commit: raw transaction, commit blob, head publication.
    async fn commit(storage: &FileStorage, n: u8) {
        storage.write_bytes(TXN, &[b'r', n]).await.unwrap();
        storage.write_bytes(COMMIT, &[b'c', n]).await.unwrap();
        publish(storage, &[b'h', n]).await;
    }

    fn wal_entries(dir: &Path) -> Vec<String> {
        let mut names: Vec<_> = std::fs::read_dir(dir.join(WAL_DIR))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    /// The point of the mode. A commit is three writes; per-write flushing
    /// pays two flushes for each of them, the log pays one for the lot.
    #[tokio::test]
    async fn a_commit_costs_one_flush() {
        let dir = tempfile::tempdir().unwrap();
        let storage = with_wal(dir.path());
        // The first write also creates the segment, which is its own cost.
        commit(&storage, 1).await;
        let before = storage.fsyncs_issued();
        commit(&storage, 2).await;
        assert_eq!(storage.fsyncs_issued() - before, 1);

        // Same three writes with per-write flushing, as the control.
        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::Sync);
        commit(&storage, 1).await;
        assert_eq!(storage.fsyncs_issued(), 6);
    }

    /// What the flush buys: after a crash that loses every file the page
    /// cache held, replay rebuilds them from the log, then retires the log.
    #[tokio::test]
    async fn replay_restores_acknowledged_writes_after_a_crash() {
        let dir = tempfile::tempdir().unwrap();
        let storage = with_wal(dir.path());
        storage.hold_wal_segments_for_test().unwrap();
        commit(&storage, 1).await;
        commit(&storage, 2).await;
        storage.simulate_crash_for_test();
        drop(storage);
        for address in [TXN, COMMIT, HEAD] {
            let path = FileStorage::new(dir.path()).resolve_path(address).unwrap();
            std::fs::remove_file(path).unwrap();
        }

        let storage = with_wal(dir.path());
        assert_eq!(storage.read_bytes(TXN).await.unwrap(), b"r\x02");
        assert_eq!(storage.read_bytes(COMMIT).await.unwrap(), b"c\x02");
        assert_eq!(storage.read_bytes(HEAD).await.unwrap(), b"h\x02");
        assert_eq!(wal_entries(dir.path()), ["LOCK"], "replay retires the log");
    }

    /// Replay applies records in order, so a delete cannot resurrect what it
    /// removed, and a later write after the delete wins.
    #[tokio::test]
    async fn replay_keeps_write_and_delete_order() {
        let dir = tempfile::tempdir().unwrap();
        let storage = with_wal(dir.path());
        storage.hold_wal_segments_for_test().unwrap();
        storage
            .write_bytes("fluree:file://a.bin", b"a")
            .await
            .unwrap();
        storage.delete("fluree:file://a.bin").await.unwrap();
        storage
            .write_bytes("fluree:file://b.bin", b"b1")
            .await
            .unwrap();
        storage.delete("fluree:file://b.bin").await.unwrap();
        storage
            .write_bytes("fluree:file://b.bin", b"b2")
            .await
            .unwrap();
        publish(&storage, b"h").await;
        storage.simulate_crash_for_test();
        drop(storage);
        let _ = std::fs::remove_file(dir.path().join("b.bin"));

        let storage = with_wal(dir.path());
        assert!(!storage.exists("fluree:file://a.bin").await.unwrap());
        assert_eq!(
            storage.read_bytes("fluree:file://b.bin").await.unwrap(),
            b"b2"
        );
    }

    /// A head that moved on — another process in per-write mode advanced it
    /// after the crash — is left alone rather than rolled back by replay.
    #[tokio::test]
    async fn replay_does_not_roll_back_a_head_that_moved_on() {
        let dir = tempfile::tempdir().unwrap();
        let storage = with_wal(dir.path());
        storage.hold_wal_segments_for_test().unwrap();
        publish(&storage, b"h1").await;
        storage.simulate_crash_for_test();
        drop(storage);

        let other = FileStorage::new(dir.path()).with_durability(Durability::Sync);
        publish(&other, b"h9").await;

        let storage = with_wal(dir.path());
        assert_eq!(storage.read_bytes(HEAD).await.unwrap(), b"h9");
    }

    /// A clean close flushes everything and leaves no segments, so a binary
    /// that has never heard of the log reads the root exactly as before.
    #[tokio::test]
    async fn clean_close_leaves_nothing_to_replay() {
        let dir = tempfile::tempdir().unwrap();
        let storage = with_wal(dir.path());
        commit(&storage, 1).await;
        drop(storage);
        assert_eq!(wal_entries(dir.path()), ["LOCK"]);

        let plain = FileStorage::new(dir.path()).with_durability(Durability::Sync);
        assert_eq!(plain.read_bytes(HEAD).await.unwrap(), b"h\x01");
        assert_eq!(plain.read_bytes(COMMIT).await.unwrap(), b"c\x01");
    }

    /// A root another process holds the WAL for cannot be taken over. The
    /// second handle keeps every guarantee by flushing each write itself.
    #[tokio::test]
    async fn second_owner_falls_back_to_per_write_flushing() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(WAL_DIR)).unwrap();
        let held = std::fs::File::create(dir.path().join(WAL_DIR).join("LOCK")).unwrap();
        fs2::FileExt::lock_exclusive(&held).unwrap();

        let storage = with_wal(dir.path());
        assert_eq!(storage.effective_durability(), Durability::Sync);
        storage.write_bytes(COMMIT, b"c").await.unwrap();
        assert_eq!(
            storage.fsyncs_issued(),
            2,
            "file and directory, as in sync mode"
        );
    }

    /// Index output is rebuilt from commits and never goes through the log;
    /// a root that only ever holds derived content gets no log at all.
    #[tokio::test]
    async fn derived_content_stays_off_the_log() {
        let dir = tempfile::tempdir().unwrap();
        let storage = with_wal(dir.path());
        storage
            .content_write_bytes_with_hash(
                ContentKind::IndexLeaf,
                "l:main",
                "ab".repeat(16).as_str(),
                b"leaf",
            )
            .await
            .unwrap();
        assert_eq!(storage.fsyncs_issued(), 0);
        assert!(!dir.path().join(WAL_DIR).exists());
    }

    fn owned(dir: &Path, owner: &str) -> FileStorage {
        let storage = FileStorage::new(dir)
            .with_durability(Durability::Wal)
            .with_wal_owner(owner);
        storage.recover_wal().unwrap();
        storage
    }

    /// Several processes on one root each own a log, and an owned log
    /// flushes each write on its own: nothing later would.
    #[tokio::test]
    async fn owners_keep_separate_logs_and_flush_every_write() {
        let dir = tempfile::tempdir().unwrap();
        let a = owned(dir.path(), "node-1");
        let b = owned(dir.path(), "node-2");
        a.write_bytes(TXN, b"from a").await.unwrap();
        let before = a.fsyncs_issued();
        a.write_bytes(COMMIT, b"from a").await.unwrap();
        assert_eq!(a.fsyncs_issued() - before, 1, "one flush, for the log");
        assert_eq!(b.read_bytes(TXN).await.unwrap(), b"from a");
        b.write_bytes(HEAD, b"from b").await.unwrap();
        assert_eq!(a.read_bytes(HEAD).await.unwrap(), b"from b");
        let owners = dir.path().join(WAL_DIR).join("owners");
        assert!(owners.join("node-1").join("LOCK").exists());
        assert!(owners.join("node-2").join("LOCK").exists());
        assert!(a.write_bytes("fluree:file://x.bin", b"x").await.is_ok());
        assert!(FileStorage::new(dir.path())
            .with_durability(Durability::Wal)
            .with_wal_owner("../escape")
            .write_bytes("fluree:file://y.bin", b"y")
            .await
            .is_err());
    }

    /// A node that stopped with records still in its log: another node
    /// missing one of those files applies that log and finds the file.
    #[tokio::test]
    async fn another_owner_recovers_a_stopped_owners_tail_on_a_miss() {
        let dir = tempfile::tempdir().unwrap();
        let a = owned(dir.path(), "node-1");
        a.hold_wal_segments_for_test().unwrap();
        a.write_bytes(COMMIT, b"c").await.unwrap();
        a.simulate_crash_for_test();
        drop(a);
        std::fs::remove_file(dir.path().join("ledger/commit/bbbb.bin")).unwrap();

        let b = owned(dir.path(), "node-2");
        assert!(b.exists(COMMIT).await.unwrap());
        assert_eq!(b.read_bytes(COMMIT).await.unwrap(), b"c");
        assert_eq!(
            std::fs::read_dir(dir.path().join(WAL_DIR).join("owners").join("node-1"))
                .unwrap()
                .count(),
            1,
            "the stopped owner's log was retired, leaving its lock file"
        );
    }

    /// Opening the root applies stopped owners' logs too, under any
    /// durability, so a per-write handle sees the acknowledged tail.
    #[tokio::test]
    async fn opening_a_shared_root_applies_stopped_owners_logs() {
        let dir = tempfile::tempdir().unwrap();
        let a = owned(dir.path(), "node-1");
        a.hold_wal_segments_for_test().unwrap();
        a.write_bytes(TXN, b"t").await.unwrap();
        a.simulate_crash_for_test();
        drop(a);
        std::fs::remove_file(dir.path().join("ledger/txn/aaaa.bin")).unwrap();

        let plain = FileStorage::new(dir.path()).with_durability(Durability::Sync);
        plain.recover_wal().unwrap();
        assert!(dir.path().join("ledger/txn/aaaa.bin").exists());
    }

    /// Two inserts race on one key: the file decides, and only the caller
    /// whose link won logs a record, so replay installs what won.
    #[tokio::test]
    async fn only_the_winning_insert_is_logged() {
        let dir = tempfile::tempdir().unwrap();
        let storage = with_wal(dir.path());
        storage.hold_wal_segments_for_test().unwrap();
        assert!(storage.insert(HEAD, b"winner").await.unwrap());
        let after_winner = storage.fsyncs_issued();
        assert!(!storage.insert(HEAD, b"loser").await.unwrap());
        assert_eq!(
            storage.fsyncs_issued(),
            after_winner,
            "a losing insert appends and flushes nothing"
        );
        storage.simulate_crash_for_test();
        drop(storage);
        std::fs::remove_file(dir.path().join("ns@v2/ledger/main.json")).unwrap();
        let storage = with_wal(dir.path());
        assert_eq!(storage.read_bytes(HEAD).await.unwrap(), b"winner");
    }

    /// An insert holds the key's sidecar lock from the link to the record,
    /// so a compare-and-swap on the same key cannot slip its transition into
    /// the log ahead of the creation it builds on.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_insert_waits_for_a_held_cas_lock() {
        let dir = tempfile::tempdir().unwrap();
        let storage = with_wal(dir.path());
        let path = storage.resolve_path(HEAD).unwrap();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let held = std::fs::File::create(path.with_extension("lock")).unwrap();
        fs2::FileExt::lock_exclusive(&held).unwrap();

        let inserter = storage.clone();
        let insert = tokio::spawn(async move { inserter.insert(HEAD, b"initial").await });
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(
            !insert.is_finished(),
            "the insert must wait for the key's lock"
        );
        assert!(!path.exists(), "and must not have linked its file");
        drop(held);
        assert!(insert.await.unwrap().unwrap());
        assert_eq!(storage.read_bytes(HEAD).await.unwrap(), b"initial");
    }

    /// Index output is written page-cache and flushed once, in a batch, by
    /// `sync`: nothing on the write path, every file and directory after.
    #[tokio::test]
    async fn derived_content_is_flushed_by_sync_not_by_its_write() {
        for durability in [Durability::Wal, Durability::Sync] {
            let dir = tempfile::tempdir().unwrap();
            let storage = FileStorage::new(dir.path()).with_durability(durability);
            for i in 0..3u8 {
                storage
                    .content_write_bytes_with_hash(
                        ContentKind::IndexLeaf,
                        "l:main",
                        &format!("{i:0>64}"),
                        &[i],
                    )
                    .await
                    .unwrap();
            }
            assert_eq!(
                storage.fsyncs_issued(),
                0,
                "{durability:?}: no flush on write"
            );
            storage.sync().await.unwrap();
            let after = storage.fsyncs_issued();
            assert!(
                after > 3,
                "{durability:?}: three files and at least their directory, got {after}"
            );
            storage.sync().await.unwrap();
            assert_eq!(
                storage.fsyncs_issued(),
                after,
                "{durability:?}: nothing left to flush"
            );
        }

        // Under page-cache durability nothing is ever flushed, sync included.
        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::PageCache);
        storage
            .content_write_bytes_with_hash(ContentKind::IndexLeaf, "l:main", &"a".repeat(64), b"x")
            .await
            .unwrap();
        storage.sync().await.unwrap();
        assert_eq!(storage.fsyncs_issued(), 0);
    }

    /// A second `sync` cannot return before a flush already in progress has
    /// put the caller's files on the device: the first drained them, so the
    /// second must wait for it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_concurrent_sync_waits_for_the_flush_in_progress() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::Sync);
        let result = storage
            .content_write_bytes_with_hash(
                ContentKind::IndexLeaf,
                "l:main",
                &"a".repeat(64),
                b"leaf",
            )
            .await
            .unwrap();
        let path = storage.resolve_path(&result.address).unwrap();
        std::fs::remove_file(&path).unwrap();
        // A FIFO in the artifact's place holds the first flush at its open.
        let cpath = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(cpath.as_ptr(), 0o600) }, 0);
        let first_storage = storage.clone();
        let first = tokio::spawn(async move { first_storage.sync().await });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !storage.unflushed.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first sync drained the batch");
        let second =
            tokio::time::timeout(std::time::Duration::from_millis(100), storage.sync()).await;
        // Release the held open; its flush of a FIFO then fails, harmlessly.
        let unblock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let _ = first.await.unwrap();
        drop(unblock);
        assert!(
            second.is_err(),
            "a second sync returned before the first flushed anything: {second:?}"
        );
    }

    /// Cancelling a `sync` must not release the barrier while its flush is
    /// still running on the blocking pool: the guard rides with the flush.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cancelled_sync_keeps_its_barrier_until_the_flush_finishes() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::Sync);
        let result = storage
            .content_write_bytes_with_hash(
                ContentKind::IndexLeaf,
                "l:main",
                &"a".repeat(64),
                b"leaf",
            )
            .await
            .unwrap();
        let path = storage.resolve_path(&result.address).unwrap();
        let sentinel = storage
            .content_write_bytes_with_hash(
                ContentKind::IndexLeaf,
                "l:main",
                &"b".repeat(64),
                b"sentinel",
            )
            .await
            .unwrap();
        let sentinel_path = storage.resolve_path(&sentinel.address).unwrap();
        std::fs::remove_file(&sentinel_path).unwrap();
        std::os::unix::fs::symlink("/dev/null", &sentinel_path).unwrap();
        std::fs::remove_file(&path).unwrap();
        let cpath = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(cpath.as_ptr(), 0o600) }, 0);
        let first_storage = storage.clone();
        let first = tokio::spawn(async move { first_storage.sync().await });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !storage.unflushed.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first sync drained the batch");
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let second =
            tokio::time::timeout(std::time::Duration::from_millis(100), storage.sync()).await;
        let unblock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        // The detached flush reaches the device node, fails, and returns
        // its batch to the queue.
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while storage.unflushed.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the failed batch came back");
        drop(unblock);
        assert!(
            second.is_err(),
            "a sync returned while the cancelled caller's flush was still running: {second:?}"
        );
    }

    /// A batch whose flush failed stays owed: the retry flushes it rather
    /// than reporting a success the device never saw.
    #[tokio::test]
    async fn a_failed_sync_keeps_its_batch_for_the_retry() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::Sync);
        let result = storage
            .content_write_bytes_with_hash(
                ContentKind::IndexLeaf,
                "l:main",
                &"a".repeat(64),
                b"leaf",
            )
            .await
            .unwrap();
        let path = storage.resolve_path(&result.address).unwrap();
        std::fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink("/dev/null", &path).unwrap();
        assert!(
            storage.sync().await.is_err(),
            "flushing a device node fails"
        );
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, b"leaf").unwrap();
        let before = storage.fsyncs_issued();
        storage.sync().await.unwrap();
        assert!(
            storage.fsyncs_issued() > before,
            "the retry must flush the file from the failed batch"
        );
    }

    /// The log's directory is neither content nor staging debris.
    #[tokio::test]
    async fn listing_and_addresses_keep_out_of_the_log_directory() {
        let dir = tempfile::tempdir().unwrap();
        let storage = with_wal(dir.path());
        commit(&storage, 1).await;
        let listed = storage.list_prefix("").await.unwrap();
        assert!(!listed.is_empty());
        assert!(listed.iter().all(|a| !a.contains(WAL_DIR)), "{listed:?}");
        assert!(storage
            .write_bytes(&format!("fluree:file://{WAL_DIR}/x.bin"), b"x")
            .await
            .is_err());
    }
}
