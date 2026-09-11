//! Per-root WAL behind [`Durability::Wal`](super::Durability::Wal).
//!
//! The files under a storage root stay the source of truth. This log is only
//! the tail that has not yet been flushed to the device: a write appends one
//! framed record here, the file itself is written page-cache, and a background
//! thread flushes those files and retires the segment that covered them. A
//! restart replays whatever segments are left before the root is used.
//!
//! The one flush per commit comes from the ordering at the write sites in
//! `file.rs`: content writes append without flushing, and the head
//! compare-and-swap appends *with* a flush, which covers every earlier append
//! in the same file. A record is therefore durable no later than the next head
//! publication or the next flusher tick.
//!
//! Recovery reads segments in order and applies each record in sequence. A torn
//! final frame is discarded: acknowledgment follows the flush, and a flush
//! covers every earlier frame, so a frame that did not make it whole was never
//! acknowledged. Damage *before* a later, complete segment cannot be explained
//! that way and fails the open instead of being skipped.

use fs2::FileExt;
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};
use xxhash_rust::xxh64::Xxh64;

/// Directory under the storage root holding the lock and segments.
pub(super) const WAL_DIR: &str = ".fluree-wal";
/// Under `WAL_DIR`, one log directory per owner when several processes
/// share a root (a Raft cluster's payload store).
const OWNERS_DIR: &str = "owners";
const LOCK_FILE: &str = "LOCK";
const SEGMENT_EXT: &str = "wal";
const LEGACY_SEGMENT_MAGIC: &[u8; 8] = b"FRDOSEG1";
const SEGMENT_MAGIC: &[u8; 8] = b"FRDOSEG2";
const SEGMENT_HEADER: usize = 16;
const FRAME_MAGIC: &[u8; 4] = b"FRDO";
const FRAME_HEADER: usize = 4 + 4 + 8;
const HASH: usize = 8;
const LEGACY_HASH: usize = 32;

/// Largest single record the log accepts. A write above this bypasses the log
/// and is flushed directly: past a few megabytes a flush costs bandwidth, not
/// latency, and the log would only write the bytes twice and bloat replay.
pub(super) const MAX_RECORD_BYTES: usize = 8 * 1024 * 1024;
/// A segment is closed for flushing once it passes this size ...
const ROTATE_BYTES: u64 = 8 * 1024 * 1024;
/// ... or this age with any record in it, so replay after a crash stays short.
const ROTATE_AGE: Duration = Duration::from_secs(1);
/// How often the background thread looks for work.
const TICK: Duration = Duration::from_millis(100);
/// An append that no head publication has flushed is flushed by the thread
/// once it has sat this long.
const LONE_APPEND_SYNC_AFTER: Duration = Duration::from_millis(50);

const TAG_WRITE: u8 = 1;
const TAG_INSERT: u8 = 2;
const TAG_CAS: u8 = 3;
const TAG_DELETE: u8 = 4;
const TAG_CANCEL: u8 = 5;

/// One logged storage operation. Keys are paths relative to the storage root.
pub(super) enum Op<'a> {
    /// Overwrite (or create) a file.
    Write { key: &'a str, bytes: &'a [u8] },
    /// Create a file only if absent.
    Insert { key: &'a str, bytes: &'a [u8] },
    /// Replace a file whose current contents were `expected`.
    Cas {
        key: &'a str,
        expected: Option<&'a [u8]>,
        new: &'a [u8],
    },
    /// Remove a file.
    Delete { key: &'a str },
    /// Record `seq`, on `key`, was appended but its file write failed:
    /// replay must not apply it.
    Cancel { seq: u64, key: &'a str },
}

enum OwnedOp {
    Write {
        key: String,
        bytes: Vec<u8>,
    },
    Insert {
        key: String,
        bytes: Vec<u8>,
    },
    Cas {
        key: String,
        expected: Option<Vec<u8>>,
        new: Vec<u8>,
    },
    Delete {
        key: String,
    },
    Cancel {
        seq: u64,
        key: String,
    },
}

impl Op<'_> {
    fn key(&self) -> &str {
        match self {
            Op::Write { key, .. }
            | Op::Insert { key, .. }
            | Op::Cas { key, .. }
            | Op::Delete { key }
            | Op::Cancel { key, .. } => key,
        }
    }

    fn encode(&self) -> Vec<u8> {
        fn put(out: &mut Vec<u8>, bytes: &[u8]) {
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
        }
        let mut out = Vec::new();
        match self {
            Op::Write { key, bytes } => {
                out.push(TAG_WRITE);
                put(&mut out, key.as_bytes());
                put(&mut out, bytes);
            }
            Op::Insert { key, bytes } => {
                out.push(TAG_INSERT);
                put(&mut out, key.as_bytes());
                put(&mut out, bytes);
            }
            Op::Cas { key, expected, new } => {
                out.push(TAG_CAS);
                put(&mut out, key.as_bytes());
                match expected {
                    Some(expected) => {
                        out.push(1);
                        put(&mut out, expected);
                    }
                    None => out.push(0),
                }
                put(&mut out, new);
            }
            Op::Delete { key } => {
                out.push(TAG_DELETE);
                put(&mut out, key.as_bytes());
            }
            Op::Cancel { seq, key } => {
                out.push(TAG_CANCEL);
                put(&mut out, key.as_bytes());
                out.extend_from_slice(&seq.to_le_bytes());
            }
        }
        out
    }
}

fn decode_op(payload: &[u8]) -> Option<OwnedOp> {
    struct Cursor<'a>(&'a [u8]);
    impl Cursor<'_> {
        fn u8(&mut self) -> Option<u8> {
            let (first, rest) = self.0.split_first()?;
            self.0 = rest;
            Some(*first)
        }
        fn bytes(&mut self) -> Option<Vec<u8>> {
            let len = u32::from_le_bytes(self.0.get(..4)?.try_into().ok()?) as usize;
            let out = self.0.get(4..4 + len)?.to_vec();
            self.0 = &self.0[4 + len..];
            Some(out)
        }
        fn key(&mut self) -> Option<String> {
            String::from_utf8(self.bytes()?).ok()
        }
        fn done(&self) -> bool {
            self.0.is_empty()
        }
    }
    let mut c = Cursor(payload);
    let op = match c.u8()? {
        TAG_WRITE => OwnedOp::Write {
            key: c.key()?,
            bytes: c.bytes()?,
        },
        TAG_INSERT => OwnedOp::Insert {
            key: c.key()?,
            bytes: c.bytes()?,
        },
        TAG_CAS => {
            let key = c.key()?;
            let expected = match c.u8()? {
                0 => None,
                1 => Some(c.bytes()?),
                _ => return None,
            };
            OwnedOp::Cas {
                key,
                expected,
                new: c.bytes()?,
            }
        }
        TAG_DELETE => OwnedOp::Delete { key: c.key()? },
        TAG_CANCEL => {
            let key = c.key()?;
            let seq = u64::from_le_bytes(c.0.get(..8)?.try_into().ok()?);
            c.0 = &c.0[8..];
            OwnedOp::Cancel { seq, key }
        }
        _ => return None,
    };
    c.done().then_some(op)
}

/// Hash the payload and allocate its frame before taking the log mutex.
/// Version 2 checksums payload followed by header, so the sequence number
/// can be folded in after it is assigned without rehashing the payload.
fn prepare_frame(payload: &[u8]) -> (Vec<u8>, Xxh64) {
    let mut frame = Vec::with_capacity(FRAME_HEADER + payload.len() + HASH);
    frame.extend_from_slice(FRAME_MAGIC);
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&0u64.to_le_bytes());
    frame.extend_from_slice(payload);
    let mut hash = Xxh64::new(0);
    hash.update(payload);
    (frame, hash)
}

fn finish_frame(seq: u64, mut frame: Vec<u8>, mut hash: Xxh64) -> Vec<u8> {
    frame[8..16].copy_from_slice(&seq.to_le_bytes());
    hash.update(&frame[..FRAME_HEADER]);
    frame.extend_from_slice(&hash.digest().to_le_bytes());
    frame
}

#[cfg(test)]
fn encode_frame(seq: u64, payload: &[u8]) -> Vec<u8> {
    let (frame, hash) = prepare_frame(payload);
    finish_frame(seq, frame, hash)
}

fn segment_hash_size(bytes: &[u8]) -> Option<usize> {
    match bytes.get(..8)? {
        magic if magic == SEGMENT_MAGIC => Some(HASH),
        magic if magic == LEGACY_SEGMENT_MAGIC => Some(LEGACY_HASH),
        _ => None,
    }
}

/// Frames decoded from one segment, and whether the segment ended cleanly.
struct Decoded {
    ops: Vec<OwnedOp>,
    next_seq: u64,
    /// `false` when trailing bytes did not form a complete, valid frame.
    clean: bool,
}

fn decode_segment(bytes: &[u8], expected_first_seq: Option<u64>) -> io::Result<Decoded> {
    if bytes.len() < SEGMENT_HEADER || segment_hash_size(bytes).is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "WAL segment header is missing or unrecognized",
        ));
    }
    let hash_size = segment_hash_size(bytes).expect("validated above");
    let first_seq = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    if expected_first_seq.is_some_and(|expected| expected != first_seq) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "WAL segment sequence does not continue the previous segment",
        ));
    }
    let mut ops = Vec::new();
    let mut seq = first_seq;
    let mut at = SEGMENT_HEADER;
    let clean = loop {
        if at == bytes.len() {
            break true;
        }
        let Some(header) = bytes.get(at..at + FRAME_HEADER) else {
            break false;
        };
        if &header[..4] != FRAME_MAGIC {
            break false;
        }
        let len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        if u64::from_le_bytes(header[8..16].try_into().unwrap()) != seq {
            break false;
        }
        let end = at + FRAME_HEADER + len;
        let Some(stored) = bytes.get(end..end + hash_size) else {
            break false;
        };
        let valid = if hash_size == LEGACY_HASH {
            Sha256::digest(&bytes[at..end]).as_slice() == stored
        } else {
            let mut hash = Xxh64::new(0);
            hash.update(&bytes[at + FRAME_HEADER..end]);
            hash.update(header);
            hash.digest().to_le_bytes() == stored
        };
        if !valid {
            break false;
        }
        let Some(op) = decode_op(&bytes[at + FRAME_HEADER..end]) else {
            break false;
        };
        ops.push(op);
        seq += 1;
        at = end + hash_size;
    };
    Ok(Decoded {
        ops,
        next_seq: seq,
        clean,
    })
}

struct Segment {
    id: u64,
    path: PathBuf,
    file: File,
    len: u64,
    /// Keys whose files must be flushed before this segment can be retired.
    touched: Vec<String>,
    /// Appends whose caller has not yet materialized the file. A segment
    /// is never retired while this is non-zero: the flush would miss the
    /// file and the record would be gone when the file finally lands.
    in_flight: Arc<AtomicUsize>,
    dirty: bool,
    last_append: Instant,
    opened: Instant,
}

struct Closed {
    path: PathBuf,
    touched: Vec<String>,
    in_flight: Arc<AtomicUsize>,
}

/// Returned by [`Wal::append`]; hold it until the file the record
/// describes has been written, so the segment cannot be retired in between.
#[must_use = "drop this only after the file the record describes is written"]
pub(super) struct Appended {
    in_flight: Arc<AtomicUsize>,
    /// The record's sequence, for a [`Wal::cancel`] should its file write fail.
    pub(super) seq: u64,
}

impl Drop for Appended {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
enum DeviceStep {
    FilesFlushed(PathBuf),
    Unlink(PathBuf),
    DirFlushed,
}

struct Inner {
    active: Option<Segment>,
    closed: Vec<Closed>,
    next_seq: u64,
    next_segment: u64,
}

/// The WAL for one storage root. One per canonical root per process; the
/// `LOCK` file keeps a second process out, and that process falls back to
/// per-write flushing.
pub(super) struct Wal {
    base: PathBuf,
    dir: PathBuf,
    fsyncs: AtomicU64,
    inner: Mutex<Inner>,
    lock: Mutex<Option<File>>,
    crashed: AtomicBool,
    /// Test hook: keep every segment until close, so a test can crash the
    /// log with records still in it however slowly it runs.
    hold_segments: AtomicBool,
    /// Owned logs flush every append: on a shared root nothing later flushes
    /// on a payload's behalf, because the head lives in Raft, not in a file.
    flush_every_append: bool,
    /// Set after a flush failed. The durable boundary is then unknown, so no
    /// further append is accepted; a restart replays what is there.
    poisoned: AtomicBool,
    /// One retirement at a time, from draining the closed segments to the
    /// last unlink. Two retirements with separate batches could otherwise
    /// remove a later segment while an earlier one was still held back.
    retiring: Mutex<()>,
    /// Per-key exclusion for logged operations, by hash of the key. On the
    /// log rather than the handle: every handle on a root shares its log,
    /// so they share what orders it. See [`Self::key_stripe`].
    key_stripes: Vec<Arc<tokio::sync::Mutex<()>>>,
    /// Test hook: make the next flush report failure.
    #[cfg(test)]
    fail_next_sync: AtomicBool,
    #[cfg(test)]
    device_trace: Mutex<Vec<DeviceStep>>,
}

/// How many keys share one lock in [`Wal::key_stripes`].
const KEY_STRIPES: usize = 256;

impl std::fmt::Debug for Wal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wal").field("dir", &self.dir).finish()
    }
}

/// Outcome of trying to own a root's log.
pub(super) enum Acquire {
    Log(Arc<Wal>),
    /// No log directory and the caller did not ask to create one.
    Absent,
    /// Another process holds the lock.
    Busy,
    /// The filesystem refused the lock or the directory could not be set up.
    Unsupported(io::Error),
}

fn registry() -> &'static Mutex<HashMap<PathBuf, Weak<Wal>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<PathBuf, Weak<Wal>>>> = OnceLock::new();
    REGISTRY.get_or_init(Default::default)
}

/// Flush and retire every segment of every log this process holds under
/// `base`, the root's own and any owned ones. The shutdown hook: a handle
/// that is still referenced by some background task would otherwise keep
/// its log until that task ends. Returns whether there was a live log.
pub(super) fn checkpoint_root(base: &Path) -> io::Result<bool> {
    let base = match std::fs::canonicalize(base) {
        Ok(base) => base,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e),
    };
    let under = base.join(WAL_DIR);
    let logs: Vec<Arc<Wal>> = registry()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .filter(|(dir, _)| dir.starts_with(&under))
        .filter_map(|(_, log)| log.upgrade())
        .collect();
    let mut any = false;
    for log in logs {
        if log.crashed.load(Ordering::Acquire) || log.hold_segments.load(Ordering::Acquire) {
            continue;
        }
        log.checkpoint()?;
        any = true;
    }
    Ok(any)
}

/// Replay the logs of owners that are no longer running, if this root has
/// owned logs at all. A dead node's acknowledged payloads may exist only in
/// its log; any node opening the root, or missing a file, can apply them.
/// Logs this process holds, or another live process holds, are skipped.
/// Returns how many records were applied.
pub(super) fn replay_unowned(base: &Path) -> io::Result<usize> {
    let base = match std::fs::canonicalize(base) {
        Ok(base) => base,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    let owners = base.join(WAL_DIR).join(OWNERS_DIR);
    let entries = match std::fs::read_dir(&owners) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    let mut applied = 0;
    for entry in entries {
        let dir = entry?.path();
        if !dir.is_dir() {
            continue;
        }
        let live = registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&dir)
            .and_then(Weak::upgrade)
            .is_some_and(|log| !log.crashed.load(Ordering::Acquire));
        if live {
            continue;
        }
        let lock = match OpenOptions::new()
            .read(true)
            .write(true)
            .open(dir.join(LOCK_FILE))
        {
            Ok(lock) => lock,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => {
                skip_unavailable_owner(&dir, e)?;
                continue;
            }
        };
        match lock.try_lock_exclusive() {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
            Err(e) => {
                skip_unavailable_owner(&dir, e)?;
                continue;
            }
        }
        let (_, _, records) = replay(&base, &dir, &AtomicU64::new(0))?;
        if records > 0 {
            tracing::info!(
                owner = %dir.display(),
                records,
                "applied the WAL of an owner that is no longer running"
            );
        }
        applied += records;
        // The lock drops here, so the owner can come back and reopen an
        // already retired log.
    }
    Ok(applied)
}

/// A failed first acquisition can leave an empty owner directory. It is
/// safe to ignore that directory, but retained segments may hold the only
/// durable copy of an acknowledged payload and must not be skipped silently.
fn skip_unavailable_owner(dir: &Path, error: io::Error) -> io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        if entry?
            .path()
            .extension()
            .is_some_and(|ext| ext == SEGMENT_EXT)
        {
            return Err(io::Error::new(
                error.kind(),
                format!("cannot lock retained WAL at {}: {error}", dir.display()),
            ));
        }
    }
    tracing::warn!(owner = %dir.display(), error = %error,
        "cannot lock an empty owner's WAL; skipping it (per-write fallback)");
    Ok(())
}

fn validate_owner(owner: &str) -> io::Result<()> {
    if owner.is_empty()
        || owner.len() > 64
        || !owner
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
        || owner.starts_with('.')
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "WAL owner must be 1-64 characters of [A-Za-z0-9._-] not starting with '.'",
        ));
    }
    Ok(())
}

/// Flush a directory so the entries in it survive power loss. Unix only, as
/// with the rest of the backend; the log is never enabled elsewhere.
fn fsync_dir(path: &Path, fsyncs: &AtomicU64) -> io::Result<()> {
    if cfg!(unix) {
        File::open(path)?.sync_all()?;
        fsyncs.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

/// Flush the files named by `keys` and every directory between them and the
/// root. Missing files were deleted after being logged, which is fine: the
/// directory flush makes the unlink durable.
pub(super) fn flush_keys(base: &Path, keys: &[String], fsyncs: &AtomicU64) -> io::Result<()> {
    let mut unique: Vec<&String> = keys.iter().collect();
    unique.sort_unstable();
    unique.dedup();
    let mut dirs = BTreeSet::new();
    for key in unique {
        let path = base.join(key);
        match File::open(&path) {
            Ok(file) => {
                file.sync_all()?;
                fsyncs.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let mut dir = path.parent();
        while let Some(d) = dir {
            if !dirs.insert(d.to_path_buf()) || d == base {
                break;
            }
            dir = d.parent();
        }
    }
    for dir in dirs {
        match fsync_dir(&dir, fsyncs) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn write_page_cache(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_file_name(format!(
        "{}.wal-replay.{}.tmp",
        path.file_name().unwrap_or_default().to_string_lossy(),
        std::process::id()
    ));
    let result = std::fs::write(&tmp, bytes).and_then(|()| std::fs::rename(&tmp, path));
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

fn read_opt(path: &Path) -> io::Result<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(bytes) if bytes.is_empty() => Ok(None),
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// The sidecar lock a live compare-and-swap holds across its read and
/// write. Replay takes it too: a writer in per-write mode on the same root
/// may be publishing the same head, and a replayed transition must not land
/// on top of a newer one.
pub(super) fn key_lock(path: &Path) -> io::Result<File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path.with_extension("lock"))?;
    lock.lock_exclusive()?;
    Ok(lock)
}

/// Apply one recovered record to the root. Every arm is idempotent, so a
/// replay interrupted and restarted converges on the same files.
fn apply(base: &Path, op: &OwnedOp, touched: &mut Vec<String>) -> io::Result<()> {
    match op {
        OwnedOp::Write { key, bytes } => {
            let path = base.join(key);
            if read_opt(&path)?.as_deref() != Some(bytes.as_slice()) {
                write_page_cache(&path, bytes)?;
            }
            touched.push(key.clone());
        }
        OwnedOp::Insert { key, bytes } => {
            let path = base.join(key);
            let _lock = key_lock(&path)?;
            if read_opt(&path)?.is_none() {
                write_page_cache(&path, bytes)?;
            }
            touched.push(key.clone());
        }
        OwnedOp::Cas { key, expected, new } => {
            let path = base.join(key);
            let _lock = key_lock(&path)?;
            let current = read_opt(&path)?;
            if current.as_deref() == Some(new.as_slice()) {
                // Already materialized before the crash.
            } else if current.is_none() || current == *expected {
                // The rename never reached the device, or it did ahead of
                // the replacement inode's contents. Nothing that replaces a
                // head leaves it without contents, so an empty or missing
                // file is a torn write to repair, not a later state.
                write_page_cache(&path, new)?;
            } else {
                // A later record, or a writer in per-write mode, moved the
                // file past this transition. Replaying it would roll back.
                tracing::warn!(
                    key,
                    "WAL replay skipped a compare-and-swap whose file has moved on"
                );
            }
            touched.push(key.clone());
        }
        OwnedOp::Delete { key } => {
            match std::fs::remove_file(base.join(key)) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
            touched.push(key.clone());
        }
        // Acted on before any record is applied; see `replay`.
        OwnedOp::Cancel { .. } => {}
    }
    Ok(())
}

fn segment_paths(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut segments = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(stem) = name.strip_suffix(&format!(".{SEGMENT_EXT}")) {
            if let Ok(id) = stem.parse::<u64>() {
                segments.push((id, entry.path()));
            }
        } else if name.ends_with(&format!(".{SEGMENT_EXT}.tmp")) {
            // A segment whose header never finished; nothing was ever
            // appended to it, so nothing is lost.
            std::fs::remove_file(entry.path())?;
        }
    }
    segments.sort();
    Ok(segments)
}

/// Replay every segment in `dir` onto `base`, flush what that touched, and
/// remove the segments. Returns the next sequence number, the next segment
/// id, and how many records were applied.
/// Read the segments under `dir` in order, each checked against the last,
/// and hand every segment's records to `f` with the sequence of its first.
/// A final segment that never got its header is removed as never used, and
/// a torn final frame is discarded, noted when `report` is set. Returns the
/// sequence after the last record, if there was any segment.
fn walk_segments(
    dir: &Path,
    fsyncs: &AtomicU64,
    report: bool,
    mut f: impl FnMut(u64, &[OwnedOp]) -> io::Result<()>,
) -> io::Result<Option<u64>> {
    let segments = segment_paths(dir)?;
    let last = segments.len().saturating_sub(1);
    let mut next_seq = None;
    for (i, (_, path)) in segments.iter().enumerate() {
        let mut bytes = Vec::new();
        File::open(path)?.read_to_end(&mut bytes)?;
        if i == last && bytes.len() < SEGMENT_HEADER {
            // Creation interrupted before the header was written. A record
            // is only ever appended after the header is durable, so this
            // segment holds nothing acknowledged.
            tracing::warn!(
                segment = %path.display(),
                "WAL segment has no valid header; removing it as never used"
            );
            std::fs::remove_file(path)?;
            fsync_dir(dir, fsyncs)?;
            break;
        }
        let decoded = decode_segment(&bytes, next_seq).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("WAL segment {} unreadable: {e}", path.display()),
            )
        })?;
        if !decoded.clean && i != last {
            // Rotation flushes a segment before opening its successor, so a
            // damaged frame here is not a torn tail and may cover an
            // acknowledged write. Refuse rather than silently drop it.
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "WAL segment {} is damaged before a later segment; move it aside to \
                     open without it (records after sequence {} in it are lost)",
                    path.display(),
                    decoded.next_seq
                ),
            ));
        }
        if !decoded.clean && report {
            tracing::info!(
                segment = %path.display(),
                replayed = decoded.ops.len(),
                "WAL ends in a torn frame; discarding it as unacknowledged"
            );
        }
        f(decoded.next_seq - decoded.ops.len() as u64, &decoded.ops)?;
        next_seq = Some(decoded.next_seq);
    }
    Ok(next_seq)
}

fn replay(base: &Path, dir: &Path, fsyncs: &AtomicU64) -> io::Result<(u64, u64, usize)> {
    let segments = segment_paths(dir)?;
    // A record cancelled by a later one is never applied, and only the whole
    // log says which those are: one pass to collect them, one to apply.
    let mut cancelled = BTreeSet::new();
    walk_segments(dir, fsyncs, false, |first, ops| {
        for (i, op) in ops.iter().enumerate() {
            if let OwnedOp::Cancel { seq, key } = op {
                if *seq >= first + i as u64 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "WAL record {} on {key} cancels a record not before it",
                            first + i as u64
                        ),
                    ));
                }
                cancelled.insert(*seq);
            }
        }
        Ok(())
    })?;
    let mut touched = Vec::new();
    let mut records = 0usize;
    let next_seq = walk_segments(dir, fsyncs, true, |first, ops| {
        for (i, op) in ops.iter().enumerate() {
            if !cancelled.contains(&(first + i as u64)) {
                apply(base, op, &mut touched)?;
            }
        }
        records += ops.len();
        Ok(())
    })?;
    if records > 0 {
        tracing::info!(
            root = %base.display(),
            records,
            segments = segments.len(),
            "replayed WAL after an unclean shutdown"
        );
    }
    flush_keys(base, &touched, fsyncs)?;
    for (_, path) in &segments {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    if !segments.is_empty() {
        fsync_dir(dir, fsyncs)?;
    }
    let next_segment = segments.last().map_or(1, |(id, _)| id + 1);
    Ok((next_seq.unwrap_or(1), next_segment, records))
}

impl Wal {
    /// Own the log for `base`, replaying any leftover segments first.
    ///
    /// With an `owner`, the log lives in its own directory under the root's
    /// log directory, so several processes can each journal one shared root,
    /// and every append is flushed (see `flush_every_append`).
    ///
    /// Blocking; call from a blocking context. With `create` false an absent
    /// log directory is reported rather than made, so a read-only open leaves
    /// no trace on a root that never journaled.
    pub(super) fn acquire(base: &Path, owner: Option<&str>, create: bool) -> io::Result<Acquire> {
        if !cfg!(unix) {
            return Ok(Acquire::Unsupported(io::Error::other(
                "the WAL needs directory fsync, which only Unix provides",
            )));
        }
        if let Some(owner) = owner {
            validate_owner(owner)?;
        }
        if create {
            std::fs::create_dir_all(base)?;
        }
        let base = match std::fs::canonicalize(base) {
            Ok(base) => base,
            Err(e) if e.kind() == io::ErrorKind::NotFound && !create => return Ok(Acquire::Absent),
            Err(e) => return Err(e),
        };
        let dir = match owner {
            Some(owner) => base.join(WAL_DIR).join(OWNERS_DIR).join(owner),
            None => base.join(WAL_DIR),
        };
        let mut registry = registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(log) = registry.get(&dir).and_then(Weak::upgrade) {
            if !log.crashed.load(Ordering::Acquire) {
                return Ok(Acquire::Log(log));
            }
        }
        if !dir.exists() {
            if !create {
                return Ok(Acquire::Absent);
            }
            std::fs::create_dir_all(&dir)?;
            // Every new directory between the root and the log is flushed,
            // deepest first, so the log is reachable after power loss.
            let mut made = dir.as_path();
            while made != base {
                if let Err(e) = fsync_dir(made, &AtomicU64::new(0)) {
                    return Ok(Acquire::Unsupported(e));
                }
                made = made.parent().expect("under base");
            }
            if let Err(e) = fsync_dir(&base, &AtomicU64::new(0)) {
                return Ok(Acquire::Unsupported(e));
            }
        }
        let lock = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join(LOCK_FILE))
        {
            Ok(lock) => lock,
            Err(e) => return Ok(Acquire::Unsupported(e)),
        };
        match lock.try_lock_exclusive() {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(Acquire::Busy),
            Err(e) => return Ok(Acquire::Unsupported(e)),
        }
        let fsyncs = AtomicU64::new(0);
        let (next_seq, next_segment, _) = replay(&base, &dir, &fsyncs)?;
        let log = Arc::new(Wal {
            base,
            dir: dir.clone(),
            fsyncs,
            flush_every_append: owner.is_some(),
            inner: Mutex::new(Inner {
                active: None,
                closed: Vec::new(),
                next_seq,
                next_segment,
            }),
            lock: Mutex::new(Some(lock)),
            crashed: AtomicBool::new(false),
            hold_segments: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            retiring: Mutex::new(()),
            key_stripes: (0..KEY_STRIPES)
                .map(|_| Arc::new(tokio::sync::Mutex::new(())))
                .collect(),
            #[cfg(test)]
            fail_next_sync: AtomicBool::new(false),
            #[cfg(test)]
            device_trace: Mutex::new(Vec::new()),
        });
        let weak = Arc::downgrade(&log);
        let ticker = weak.clone();
        std::thread::Builder::new()
            .name("fluree-wal-flush".into())
            .spawn(move || loop {
                std::thread::sleep(TICK);
                let Some(log) = ticker.upgrade() else { break };
                if let Err(e) = log.tick() {
                    tracing::warn!(error = %e, dir = %log.dir.display(), "WAL flush failed");
                }
            })?;
        registry.insert(dir, weak);
        Ok(Acquire::Log(log))
    }

    pub(super) fn fsyncs_issued(&self) -> u64 {
        self.fsyncs.load(Ordering::Relaxed)
    }

    /// Hold `key` for a logged operation, from its record to its file
    /// operation, so a key's records sit in the log in the order its file
    /// operations landed and replay reproduces what the process saw. An
    /// oversized write holds it from the checkpoint to the rename, so no
    /// record for its key can come between the two. A compare-and-swap
    /// holds it across its read, closure and write, hence the owned guard.
    /// Blocking; call from a blocking context.
    pub(super) fn key_stripe(&self, key: &str) -> tokio::sync::OwnedMutexGuard<()> {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let stripe = (hasher.finish() % self.key_stripes.len() as u64) as usize;
        Arc::clone(&self.key_stripes[stripe]).blocking_lock_owned()
    }

    fn refuse_if_unavailable(&self) -> io::Result<()> {
        if self.crashed.load(Ordering::Acquire) {
            return Err(io::Error::other("WAL was abandoned (simulated crash)"));
        }
        if self.poisoned.load(Ordering::Acquire) {
            return Err(io::Error::other(
                "WAL refused after a failed flush: the durable boundary is unknown; \
                 restart to recover",
            ));
        }
        Ok(())
    }

    /// A flush failed, so what is durable is unknown. Refuse everything from
    /// here on; the next open replays whatever did reach the device.
    fn poison(&self, error: io::Error) -> io::Error {
        self.poisoned.store(true, Ordering::Release);
        tracing::error!(
            error = %error,
            dir = %self.dir.display(),
            "WAL flush failed; refusing further writes until restart"
        );
        error
    }

    fn sync_segment(&self, segment: &mut Segment) -> io::Result<()> {
        #[cfg(test)]
        if self.fail_next_sync.swap(false, Ordering::AcqRel) {
            return Err(io::Error::other("injected flush failure"));
        }
        segment.file.sync_all()?;
        self.fsyncs.fetch_add(1, Ordering::Relaxed);
        segment.dirty = false;
        Ok(())
    }

    /// Make every append so far durable. Closed segments were flushed when
    /// they closed, so only the active one can still be dirty. A write too
    /// large for the log pays its own fsync and calls this first, so nothing
    /// it may name is left behind on a crash that keeps it.
    pub(super) fn flush(&self) -> io::Result<()> {
        self.refuse_if_unavailable()?;
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.refuse_if_unavailable()?;
        match inner.active.as_mut() {
            Some(segment) if segment.dirty => {
                self.sync_segment(segment).map_err(|e| self.poison(e))
            }
            _ => Ok(()),
        }
    }

    /// Append one record. With `sync`, the segment is flushed before returning,
    /// which also covers every earlier unflushed append. Hold the returned
    /// guard until the file the record describes is written.
    pub(super) fn append(&self, op: Op<'_>, sync: bool) -> io::Result<Appended> {
        self.refuse_if_unavailable()?;
        let sync = sync || self.flush_every_append;
        let payload = op.encode();
        let (frame, hash) = prepare_frame(&payload);
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Again under the mutex: the append this one waited behind may have
        // poisoned the log, and its sequence number must not be reused.
        self.refuse_if_unavailable()?;
        let seq = inner.next_seq;
        if inner.active.is_none() {
            let segment = self.open_segment(&mut inner, seq)?;
            inner.active = Some(segment);
        }
        let frame = finish_frame(seq, frame, hash);
        let (full, in_flight) = {
            let segment = inner.active.as_mut().expect("opened above");
            let start = segment.len;
            // Always write at the frame boundary the log knows about, never
            // at wherever an earlier, failed write left the cursor.
            let written = segment
                .file
                .seek(SeekFrom::Start(start))
                .and_then(|_| segment.file.write_all(&frame));
            if let Err(e) = written {
                // Nothing acknowledged depends on the bytes that may have
                // landed; cut back to the last frame boundary. If even
                // that fails the file's state is unknown.
                return Err(match segment.file.set_len(start) {
                    Ok(()) => e,
                    Err(_) => self.poison(e),
                });
            }
            segment.len += frame.len() as u64;
            segment.dirty = true;
            segment.last_append = Instant::now();
            segment.touched.push(op.key().to_owned());
            if sync {
                if let Err(e) = self.sync_segment(segment) {
                    return Err(self.poison(e));
                }
            }
            segment.in_flight.fetch_add(1, Ordering::AcqRel);
            (segment.len >= ROTATE_BYTES, Arc::clone(&segment.in_flight))
        };
        inner.next_seq = seq + 1;
        if full {
            self.rotate(&mut inner)?;
        }
        Ok(Appended { in_flight, seq })
    }

    /// Log that record `seq`, appended and flushed, was never materialized:
    /// its file write failed. Replay then skips that record, and only that
    /// record; a value another writer lands afterwards, even the same bytes,
    /// is theirs. Without this replay would install the failed transition
    /// and skip a later one that succeeded from the same starting value.
    /// Flushed like the record it cancels. If even this cannot be appended
    /// the log is poisoned: what replay would do is then unknown.
    pub(super) fn cancel(&self, seq: u64, key: &str) -> io::Result<()> {
        self.append(Op::Cancel { seq, key }, true)
            .map(drop)
            .map_err(|e| {
                if self.poisoned.load(Ordering::Acquire) {
                    e
                } else {
                    self.poison(e)
                }
            })
    }

    /// Create the next segment: header written and flushed under a temporary
    /// name, then renamed into place, so a crash can never leave a segment
    /// whose header is missing or partial.
    fn open_segment(&self, inner: &mut Inner, first_seq: u64) -> io::Result<Segment> {
        let id = inner.next_segment;
        inner.next_segment += 1;
        let path = self.dir.join(format!("{id:08}.{SEGMENT_EXT}"));
        let tmp = self.dir.join(format!("{id:08}.{SEGMENT_EXT}.tmp"));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        let mut header = SEGMENT_MAGIC.to_vec();
        header.extend_from_slice(&first_seq.to_le_bytes());
        file.write_all(&header)?;
        file.sync_all()?;
        self.fsyncs.fetch_add(1, Ordering::Relaxed);
        std::fs::rename(&tmp, &path)?;
        fsync_dir(&self.dir, &self.fsyncs)?;
        Ok(Segment {
            id,
            path,
            file,
            len: SEGMENT_HEADER as u64,
            touched: Vec::new(),
            in_flight: Arc::new(AtomicUsize::new(0)),
            dirty: false,
            last_append: Instant::now(),
            opened: Instant::now(),
        })
    }

    /// Close the active segment so the flusher can retire it. Flushed first:
    /// the next segment's flushes do not cover this file.
    fn rotate(&self, inner: &mut Inner) -> io::Result<()> {
        let Some(mut segment) = inner.active.take() else {
            return Ok(());
        };
        if segment.dirty {
            if let Err(e) = self.sync_segment(&mut segment) {
                inner.active = Some(segment);
                return Err(self.poison(e));
            }
        }
        tracing::trace!(
            segment = segment.id,
            bytes = segment.len,
            "WAL segment closed"
        );
        inner.closed.push(Closed {
            path: segment.path,
            touched: segment.touched,
            in_flight: segment.in_flight,
        });
        Ok(())
    }

    fn tick(&self) -> io::Result<()> {
        if self.crashed.load(Ordering::Acquire) || self.poisoned.load(Ordering::Acquire) {
            return Ok(());
        }
        let hold = self.hold_segments.load(Ordering::Acquire);
        {
            let mut inner = self
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(segment) = inner.active.as_mut() {
                let now = Instant::now();
                if segment.dirty
                    && now.duration_since(segment.last_append) >= LONE_APPEND_SYNC_AFTER
                {
                    if let Err(e) = self.sync_segment(segment) {
                        return Err(self.poison(e));
                    }
                }
                if !hold
                    && !segment.touched.is_empty()
                    && now.duration_since(segment.opened) >= ROTATE_AGE
                {
                    self.rotate(&mut inner)?;
                }
            }
        }
        if hold {
            return Ok(());
        }
        self.retire_closed().map(|_| ())
    }

    /// Stop the background thread from retiring segments until close.
    #[doc(hidden)]
    pub fn hold_segments(&self) {
        self.hold_segments.store(true, Ordering::Release);
    }

    /// Flush the files each closed segment covered, then remove the segment,
    /// oldest first and only as a contiguous prefix. Retirement stops at the
    /// first segment it cannot retire yet — an append whose file is not
    /// written, or a flush that failed — and everything from there on goes
    /// back to the queue in order, so no later segment is ever removed ahead
    /// of an earlier one and replay never meets a hole. One retirement runs
    /// at a time, from draining the queue to the last unlink. Returns how
    /// many segments are still queued.
    fn retire_closed(&self) -> io::Result<usize> {
        let _one_at_a_time = self
            .retiring
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let closed = std::mem::take(
            &mut self
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .closed,
        );
        if closed.is_empty() {
            return Ok(0);
        }
        let mut pending = closed.into_iter();
        let mut kept = Vec::new();
        let mut removed = false;
        let mut failure = None;
        for segment in pending.by_ref() {
            if segment.in_flight.load(Ordering::Acquire) > 0 {
                kept.push(segment);
                break;
            }
            let retired = flush_keys(&self.base, &segment.touched, &self.fsyncs).and_then(|()| {
                #[cfg(test)]
                self.device_trace
                    .lock()
                    .unwrap()
                    .push(DeviceStep::FilesFlushed(segment.path.clone()));
                std::fs::remove_file(&segment.path)?;
                #[cfg(test)]
                self.device_trace
                    .lock()
                    .unwrap()
                    .push(DeviceStep::Unlink(segment.path.clone()));
                Ok(())
            });
            match retired {
                Ok(()) => removed = true,
                Err(e) => {
                    kept.push(segment);
                    failure = Some(e);
                    break;
                }
            }
        }
        kept.extend(pending);
        if removed {
            if let Err(e) = fsync_dir(&self.dir, &self.fsyncs) {
                failure.get_or_insert(e);
            } else {
                #[cfg(test)]
                self.device_trace
                    .lock()
                    .unwrap()
                    .push(DeviceStep::DirFlushed);
            }
        }
        let waiting = kept.len();
        if waiting > 0 {
            let mut inner = self
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            kept.append(&mut inner.closed);
            inner.closed = kept;
        }
        match failure {
            Some(e) => Err(e),
            None => Ok(waiting),
        }
    }

    /// Flush everything and leave no segments behind, so the root reads the
    /// same to a binary that knows nothing about the log. Waits briefly for
    /// appends still materializing; anything still in flight after that is
    /// left for the next open to replay. Returns how many segments that is.
    pub(super) fn checkpoint(&self) -> io::Result<usize> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            {
                let mut inner = self
                    .inner
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                self.rotate(&mut inner)?;
            }
            let waiting = self.retire_closed()?;
            if waiting == 0 || Instant::now() >= deadline {
                return Ok(waiting);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Abandon the log without flushing, as a crash would. The segments stay on
    /// disk for the next open to replay; this instance refuses further appends.
    #[doc(hidden)]
    pub fn simulate_crash(&self) {
        self.crashed.store(true, Ordering::Release);
        registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.dir);
        drop(
            self.lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take(),
        );
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        if self.crashed.load(Ordering::Acquire) {
            return;
        }
        if let Err(e) = self.checkpoint() {
            tracing::warn!(error = %e, dir = %self.dir.display(), "WAL checkpoint on close failed; the next open will replay it");
        }
        registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.dir);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cas<'a>(key: &'a str, expected: Option<&'a [u8]>, new: &'a [u8]) -> Op<'a> {
        Op::Cas { key, expected, new }
    }

    #[test]
    fn ops_round_trip_through_frames() {
        let ops = [
            Op::Write {
                key: "a/b.json",
                bytes: b"hello",
            },
            Op::Insert {
                key: "ns/x.json",
                bytes: b"",
            },
            cas("ns/h.json", None, b"v1"),
            cas("ns/h.json", Some(b"v1"), b"v2"),
            Op::Delete { key: "gone.json" },
            Op::Cancel {
                seq: 10,
                key: "ns/h.json",
            },
        ];
        let mut segment = SEGMENT_MAGIC.to_vec();
        segment.extend_from_slice(&7u64.to_le_bytes());
        for (i, op) in ops.iter().enumerate() {
            segment.extend_from_slice(&encode_frame(7 + i as u64, &op.encode()));
        }
        let decoded = decode_segment(&segment, Some(7)).unwrap();
        assert!(decoded.clean);
        assert_eq!(decoded.next_seq, 13);
        assert_eq!(decoded.ops.len(), 6);
        match &decoded.ops[3] {
            OwnedOp::Cas { key, expected, new } => {
                assert_eq!(key, "ns/h.json");
                assert_eq!(expected.as_deref(), Some(b"v1".as_slice()));
                assert_eq!(new, b"v2");
            }
            _ => panic!("wrong op"),
        }
        match &decoded.ops[5] {
            OwnedOp::Cancel { seq, key } => {
                assert_eq!(*seq, 10);
                assert_eq!(key, "ns/h.json");
            }
            _ => panic!("wrong op"),
        }
    }

    #[test]
    fn torn_tail_is_reported_not_replayed() {
        let mut segment = SEGMENT_MAGIC.to_vec();
        segment.extend_from_slice(&1u64.to_le_bytes());
        let whole = encode_frame(
            1,
            &Op::Write {
                key: "k",
                bytes: b"x",
            }
            .encode(),
        );
        segment.extend_from_slice(&whole);
        let torn = encode_frame(
            2,
            &Op::Write {
                key: "k2",
                bytes: b"yy",
            }
            .encode(),
        );
        segment.extend_from_slice(&torn[..torn.len() - 5]);
        let decoded = decode_segment(&segment, None).unwrap();
        assert!(!decoded.clean);
        assert_eq!(decoded.ops.len(), 1);
        assert_eq!(decoded.next_seq, 2);
    }

    #[test]
    fn corrupted_frame_stops_decoding() {
        let mut segment = SEGMENT_MAGIC.to_vec();
        segment.extend_from_slice(&1u64.to_le_bytes());
        let start = segment.len();
        segment.extend_from_slice(&encode_frame(1, &Op::Delete { key: "k" }.encode()));
        segment.extend_from_slice(&encode_frame(2, &Op::Delete { key: "k2" }.encode()));
        segment[start + FRAME_HEADER + 3] ^= 0xff;
        let decoded = decode_segment(&segment, None).unwrap();
        assert!(!decoded.clean);
        assert!(decoded.ops.is_empty());
    }

    #[test]
    fn sequence_gap_between_segments_is_rejected() {
        let mut segment = SEGMENT_MAGIC.to_vec();
        segment.extend_from_slice(&5u64.to_le_bytes());
        assert!(decode_segment(&segment, Some(4)).is_err());
    }

    fn owned(base: &Path) -> Arc<Wal> {
        match Wal::acquire(base, Some("review"), true).unwrap() {
            Acquire::Log(log) => log,
            _ => panic!("log unavailable"),
        }
    }

    /// A record's segment cannot be retired while the file it describes is
    /// still being written: the flush would miss the file and the record
    /// would be gone when the file finally landed, unflushed.
    #[test]
    fn retirement_waits_for_the_file_the_record_describes() {
        let dir = tempfile::tempdir().unwrap();
        let log = owned(dir.path());
        let payload = dir.path().join("payload");
        let appended = log
            .append(
                Op::Write {
                    key: "payload",
                    bytes: b"acknowledged",
                },
                true,
            )
            .unwrap();
        // The flusher runs between the append and the file write.
        log.checkpoint().unwrap();
        assert!(
            segment_paths(&log.dir).unwrap().len() == 1,
            "the segment stays while the write is in flight"
        );
        write_page_cache(&payload, b"acknowledged").unwrap();
        drop(appended);
        log.checkpoint().unwrap();
        assert!(
            segment_paths(&log.dir).unwrap().is_empty(),
            "retired once the file exists"
        );

        // The same interleaving followed by a crash before the flush:
        // replay still has the record because retirement waited.
        let appended = log
            .append(
                Op::Write {
                    key: "second",
                    bytes: b"two",
                },
                true,
            )
            .unwrap();
        log.checkpoint().unwrap();
        write_page_cache(&dir.path().join("second"), b"two").unwrap();
        drop(appended);
        log.simulate_crash();
        std::fs::remove_file(dir.path().join("second")).unwrap();
        let _reopened = owned(dir.path());
        assert_eq!(std::fs::read(dir.path().join("second")).unwrap(), b"two");
    }

    /// A failed flush leaves the durable boundary unknown. The log refuses
    /// further appends rather than reuse a sequence a later open would read
    /// as the end of the stream; a reopen replays what did land.
    #[test]
    fn a_failed_flush_refuses_further_appends_until_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let log = owned(dir.path());
        log.hold_segments();
        drop(
            log.append(
                Op::Write {
                    key: "first",
                    bytes: b"first",
                },
                true,
            )
            .unwrap(),
        );
        log.fail_next_sync.store(true, Ordering::Release);
        assert!(log
            .append(
                Op::Write {
                    key: "failed",
                    bytes: b"failed"
                },
                true
            )
            .is_err());
        let refused = log
            .append(
                Op::Write {
                    key: "after",
                    bytes: b"after",
                },
                true,
            )
            .err()
            .expect("poisoned log refuses");
        assert!(refused.to_string().contains("failed flush"), "{refused}");
        log.simulate_crash();
        let reopened = owned(dir.path());
        assert_eq!(std::fs::read(dir.path().join("first")).unwrap(), b"first");
        // The reopened log accepts appends again.
        drop(
            reopened
                .append(
                    Op::Write {
                        key: "after",
                        bytes: b"after",
                    },
                    true,
                )
                .unwrap(),
        );
    }

    /// A crash while the next segment was being created leaves either no
    /// file or a temporary one, never a published segment without a header;
    /// and an empty published file from an older layout is removed, not fatal.
    #[test]
    fn a_torn_new_segment_does_not_block_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let log = owned(dir.path());
        log.hold_segments();
        drop(
            log.append(
                Op::Write {
                    key: "first",
                    bytes: b"first",
                },
                true,
            )
            .unwrap(),
        );
        log.simulate_crash();
        let owner_dir = dir.path().join(WAL_DIR).join("owners/review");
        std::fs::write(owner_dir.join("00000002.wal"), []).unwrap();
        std::fs::write(owner_dir.join("00000003.wal.tmp"), b"partial").unwrap();
        let reopened = owned(dir.path());
        assert_eq!(std::fs::read(dir.path().join("first")).unwrap(), b"first");
        drop(
            reopened
                .append(
                    Op::Write {
                        key: "next",
                        bytes: b"next",
                    },
                    true,
                )
                .unwrap(),
        );
        assert!(!owner_dir.join("00000003.wal.tmp").exists());
    }

    /// Replay takes the same sidecar lock a live compare-and-swap holds, so
    /// it cannot land a replayed head on top of one a per-write writer is
    /// publishing at that moment.
    #[test]
    fn replay_waits_for_a_live_cas_lock() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().to_path_buf();
        let Acquire::Log(log) = Wal::acquire(&base, None, true).unwrap() else {
            panic!()
        };
        log.hold_segments();
        write_page_cache(&base.join("head.json"), b"old").unwrap();
        drop(
            log.append(
                Op::Cas {
                    key: "head.json",
                    expected: Some(b"old"),
                    new: b"recovered",
                },
                true,
            )
            .unwrap(),
        );
        log.simulate_crash();
        let lock = File::create(base.join("head.lock")).unwrap();
        lock.lock_exclusive().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let replay_base = base.clone();
        let thread = std::thread::spawn(move || {
            let recovered = Wal::acquire(&replay_base, None, false).is_ok();
            tx.send(recovered).unwrap();
        });
        assert!(
            rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "replay must wait for the head's lock"
        );
        assert_eq!(std::fs::read(base.join("head.json")).unwrap(), b"old");
        drop(lock);
        thread.join().unwrap();
        assert_eq!(std::fs::read(base.join("head.json")).unwrap(), b"recovered");
    }

    fn rotate_now(log: &Wal) {
        let mut inner = log.inner.lock().unwrap();
        log.rotate(&mut inner).unwrap();
    }

    /// A segment kept for an unfinished write keeps every later segment
    /// too: retiring one in the middle would leave a sequence hole that
    /// replay must refuse.
    #[test]
    fn retirement_keeps_a_contiguous_tail() {
        let dir = tempfile::tempdir().unwrap();
        let log = owned(dir.path());
        log.hold_segments();
        let first = log
            .append(
                Op::Write {
                    key: "first",
                    bytes: b"one",
                },
                true,
            )
            .unwrap();
        rotate_now(&log);
        let second = log
            .append(
                Op::Write {
                    key: "second",
                    bytes: b"two",
                },
                true,
            )
            .unwrap();
        write_page_cache(&dir.path().join("second"), b"two").unwrap();
        drop(second);
        rotate_now(&log);
        let third = log
            .append(
                Op::Write {
                    key: "third",
                    bytes: b"three",
                },
                true,
            )
            .unwrap();
        log.retire_closed().unwrap();
        assert_eq!(
            segment_paths(&log.dir).unwrap().len(),
            3,
            "nothing behind the held segment goes"
        );
        log.simulate_crash();
        drop(first);
        drop(third);
        let _reopened = owned(dir.path());
        assert_eq!(std::fs::read(dir.path().join("third")).unwrap(), b"three");
    }

    fn append_file(log: &Wal, base: &Path, key: &str) {
        let guard = log
            .append(
                Op::Write {
                    key,
                    bytes: key.as_bytes(),
                },
                true,
            )
            .unwrap();
        write_page_cache(&base.join(key), key.as_bytes()).unwrap();
        drop(guard);
    }

    /// Retirements from the background thread and from a checkpoint cannot
    /// interleave: each drains the queue and finishes under one lock, so a
    /// held early segment holds every later one back for both.
    #[test]
    fn concurrent_retirements_keep_global_order() {
        let dir = tempfile::tempdir().unwrap();
        let log = owned(dir.path());
        log.hold_segments();
        let held = log
            .append(
                Op::Write {
                    key: "first",
                    bytes: b"one",
                },
                true,
            )
            .unwrap();
        rotate_now(&log);
        append_file(&log, dir.path(), "second");
        rotate_now(&log);
        append_file(&log, dir.path(), "third");
        let racers: Vec<_> = (0..4)
            .map(|_| {
                let log = Arc::clone(&log);
                std::thread::spawn(move || {
                    for _ in 0..20 {
                        log.retire_closed().unwrap();
                    }
                })
            })
            .collect();
        for racer in racers {
            racer.join().unwrap();
        }
        assert_eq!(
            segment_paths(&log.dir).unwrap().len(),
            3,
            "nothing goes while the first segment is held"
        );
        drop(held);
        log.retire_closed().unwrap();
        assert_eq!(
            segment_paths(&log.dir).unwrap().len(),
            1,
            "closed ones retired in order"
        );
        log.simulate_crash();
        let _reopened = owned(dir.path());
        assert_eq!(std::fs::read(dir.path().join("third")).unwrap(), b"third");
    }

    /// A retirement that fails keeps its segment and every later one queued;
    /// once the cause is fixed the same retirement proceeds in order.
    #[test]
    fn a_failed_retirement_keeps_its_batch_queued() {
        let dir = tempfile::tempdir().unwrap();
        let log = owned(dir.path());
        log.hold_segments();
        append_file(&log, dir.path(), "first");
        rotate_now(&log);
        let path = dir.path().join("first");
        std::fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink("/dev/null", &path).unwrap();
        assert!(log.retire_closed().is_err(), "flushing a device node fails");
        assert_eq!(
            segment_paths(&log.dir).unwrap().len(),
            1,
            "the failed segment stays"
        );
        std::fs::remove_file(&path).unwrap();
        write_page_cache(&path, b"first").unwrap();
        append_file(&log, dir.path(), "second");
        rotate_now(&log);
        append_file(&log, dir.path(), "third");
        log.retire_closed().unwrap();
        assert_eq!(
            segment_paths(&log.dir).unwrap().len(),
            1,
            "first and second retired in order"
        );
        log.simulate_crash();
        let _reopened = owned(dir.path());
        assert_eq!(std::fs::read(dir.path().join("third")).unwrap(), b"third");
    }

    /// A short write followed by an error is rolled back to the frame
    /// boundary, cursor included, so the next append continues the stream
    /// rather than leaving a hole that ends replay.
    #[test]
    fn a_short_write_is_rolled_back_to_the_frame_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let log = owned(dir.path());
        log.hold_segments();
        drop(
            log.append(
                Op::Write {
                    key: "first",
                    bytes: b"one",
                },
                true,
            )
            .unwrap(),
        );
        {
            let mut inner = log.inner.lock().unwrap();
            let segment = inner.active.as_mut().unwrap();
            segment.file.write_all(b"FR").unwrap();
            segment.file.set_len(segment.len).unwrap();
        }
        drop(
            log.append(
                Op::Write {
                    key: "after",
                    bytes: b"acknowledged",
                },
                true,
            )
            .unwrap(),
        );
        log.simulate_crash();
        let _reopened = owned(dir.path());
        assert_eq!(
            std::fs::read(dir.path().join("after")).unwrap(),
            b"acknowledged"
        );
    }

    /// A head too large for the log is flushed directly, but the content it
    /// names may still sit in unflushed records. Publishing it has to put
    /// those on the device and retire them first: a crash must not keep the
    /// head and lose what it points to, and no record may be left to replay
    /// over the file the head lands on.
    #[tokio::test]
    async fn an_oversized_head_checkpoints_the_log_before_it_lands() {
        use crate::{CasAction, Durability, FileStorage, StorageCas, StorageRead, StorageWrite};

        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        storage.hold_wal_segments_for_test().unwrap();
        let log = match Wal::acquire(dir.path(), None, true).unwrap() {
            Acquire::Log(log) => log,
            _ => panic!("log unavailable"),
        };
        let head = "fluree:file://head.json";
        let payload = "fluree:file://payload.bin";
        storage.insert(head, b"initial").await.unwrap();
        storage.write_bytes(payload, b"payload").await.unwrap();
        {
            // Keep the idle-flush timer out of it: the publication itself
            // must make the payload record durable.
            let mut inner = log.inner.lock().unwrap();
            let segment = inner.active.as_mut().unwrap();
            assert!(segment.dirty);
            segment.last_append = Instant::now() + Duration::from_secs(3600);
        }
        let before = log.fsyncs_issued();
        let oversized = vec![b'x'; MAX_RECORD_BYTES + 1];
        storage
            .compare_and_swap(head, |_| Ok(CasAction::Write::<()>(oversized.clone())))
            .await
            .unwrap();
        assert!(
            log.fsyncs_issued() > before,
            "an oversized head was published without flushing the log"
        );
        assert!(
            segment_paths(&log.dir).unwrap().is_empty(),
            "records were left to replay over a directly written head"
        );
        assert!(log.inner.lock().unwrap().active.is_none());

        // Power loss keeps the directly flushed head and the payload the
        // checkpoint put on the device ahead of it; nothing is replayed.
        log.simulate_crash();
        let reopened = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        reopened.recover_wal().unwrap();
        assert_eq!(
            reopened.read_bytes(head).await.unwrap().len(),
            oversized.len()
        );
        assert_eq!(reopened.read_bytes(payload).await.unwrap(), b"payload");
    }

    /// A head write that fails after its record is logged must not replay
    /// over the retry that then succeeded from the same starting value.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_failed_head_write_does_not_replay_over_its_successful_retry() {
        use crate::{CasAction, Durability, FileStorage, StorageCas, StorageRead};
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        let key = "fluree:file://ns/head.json";
        storage.insert(key, b"checkpointed").await.unwrap();
        FileStorage::checkpoint_wal(dir.path()).unwrap();
        storage.hold_wal_segments_for_test().unwrap();
        let ns = dir.path().join("ns");
        std::fs::set_permissions(&ns, std::fs::Permissions::from_mode(0o555)).unwrap();
        let failed = storage
            .compare_and_swap(key, |_| Ok(CasAction::<()>::Write(b"failed".to_vec())))
            .await;
        std::fs::set_permissions(&ns, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(
            failed.is_err(),
            "staging the head must fail after its record is logged"
        );
        storage
            .compare_and_swap(key, |_| {
                Ok(CasAction::<()>::Write(b"acknowledged".to_vec()))
            })
            .await
            .unwrap();
        storage.simulate_crash_for_test();
        // The retry's rename was lost; the checkpointed head is intact.
        std::fs::write(ns.join("head.json"), b"checkpointed").unwrap();
        let reopened = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        reopened.recover_wal().unwrap();
        assert_eq!(reopened.read_bytes(key).await.unwrap(), b"acknowledged");
    }

    /// A write too large for the log lands directly, and nothing the log
    /// still holds may replay over it: a crash would undo a write the caller
    /// was told is durable.
    #[tokio::test]
    async fn an_oversized_write_is_not_undone_by_the_records_before_it() {
        use crate::{Durability, FileStorage, StorageRead, StorageWrite};

        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        storage.hold_wal_segments_for_test().unwrap();
        let key = "fluree:file://mutable.json";
        storage.write_bytes(key, b"old").await.unwrap();
        let large = vec![b'n'; MAX_RECORD_BYTES + 1];
        storage.write_bytes(key, &large).await.unwrap();
        storage.simulate_crash_for_test();
        let reopened = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        reopened.recover_wal().unwrap();
        assert_eq!(reopened.read_bytes(key).await.unwrap().len(), large.len());
    }

    /// The content-addressed shape of the same hazard: a blob deleted through
    /// the log and re-created directly is named by an acknowledged head, and
    /// replaying the deletion must not remove it.
    #[tokio::test]
    async fn an_oversized_blob_recreated_after_a_delete_survives_replay() {
        use crate::{CasAction, Durability, FileStorage, StorageCas, StorageRead, StorageWrite};

        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        storage.hold_wal_segments_for_test().unwrap();
        let key = "fluree:file://large.bin";
        let large = vec![b'n'; MAX_RECORD_BYTES + 1];
        storage.write_bytes(key, &large).await.unwrap();
        storage.delete(key).await.unwrap();
        storage.write_bytes(key, &large).await.unwrap();
        storage
            .compare_and_swap("fluree:file://head.json", |_| {
                Ok(CasAction::<()>::Write(b"large.bin".to_vec()))
            })
            .await
            .unwrap();
        storage.simulate_crash_for_test();
        let reopened = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        reopened.recover_wal().unwrap();
        assert!(
            reopened.exists(key).await.unwrap(),
            "replay removed the blob the acknowledged head names"
        );
    }

    /// Once the head before a transition is checkpointed only that
    /// transition's record remains. A rename that reached the device ahead of
    /// the replacement inode's contents leaves an empty file; replay must read
    /// that as a torn write to repair, not as a later state to leave alone.
    #[tokio::test]
    async fn a_head_whose_contents_were_lost_is_restored_from_its_record() {
        use crate::{CasAction, Durability, FileStorage, StorageCas, StorageRead};

        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        let key = "fluree:file://head.json";
        storage.insert(key, b"checkpointed").await.unwrap();
        FileStorage::checkpoint_wal(dir.path()).unwrap();
        storage.hold_wal_segments_for_test().unwrap();
        storage
            .compare_and_swap(key, |_| {
                Ok(CasAction::<()>::Write(b"acknowledged".to_vec()))
            })
            .await
            .unwrap();
        storage.simulate_crash_for_test();
        std::fs::write(dir.path().join("head.json"), []).unwrap();
        let reopened = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        reopened.recover_wal().unwrap();
        assert_eq!(reopened.read_bytes(key).await.unwrap(), b"acknowledged");
    }

    /// The undo of a failed creation names the value that failed, so replay
    /// leaves a head another writer created there in the meantime alone.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_failed_creation_undo_leaves_another_writers_head_alone() {
        use crate::{CasAction, Durability, FileStorage, StorageCas, StorageRead};
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        storage.hold_wal_segments_for_test().unwrap();
        let key = "fluree:file://ns/head.json";
        let ns = dir.path().join("ns");
        std::fs::create_dir_all(&ns).unwrap();
        // The sidecar lock exists already; only staging the head can fail.
        std::fs::write(ns.join("head.lock"), []).unwrap();
        std::fs::set_permissions(&ns, std::fs::Permissions::from_mode(0o555)).unwrap();
        let failed = storage
            .compare_and_swap(key, |_| Ok(CasAction::<()>::Write(b"failed".to_vec())))
            .await;
        std::fs::set_permissions(&ns, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(failed.is_err(), "staging the head must fail");
        // A writer in per-write mode on the same root creates the head.
        let other = FileStorage::new(dir.path()).with_durability(Durability::Sync);
        other
            .compare_and_swap(key, |_| Ok(CasAction::<()>::Write(b"theirs".to_vec())))
            .await
            .unwrap();
        storage.simulate_crash_for_test();
        let reopened = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        reopened.recover_wal().unwrap();
        assert_eq!(reopened.read_bytes(key).await.unwrap(), b"theirs");
    }

    /// A record appended while an oversized write sits between its checkpoint
    /// and its rename would replay over the write. The key is held from one
    /// to the other, so a concurrent operation on it lands after, in the log
    /// and on the device alike, and recovery agrees with what the process saw.
    /// The racer is a separate handle on the root, not a clone: handles share
    /// the root's log, so they have to share what orders it.
    #[tokio::test]
    async fn an_operation_racing_an_oversized_write_recovers_in_the_order_it_landed() {
        use crate::{Durability, FileStorage, StorageRead, StorageWrite};

        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        storage.hold_wal_segments_for_test().unwrap();
        let key = "fluree:file://large.bin";
        let racer = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        let deleting: Arc<Mutex<Option<std::thread::JoinHandle<()>>>> = Arc::default();
        let started = Arc::clone(&deleting);
        storage.set_after_checkpoint_hook_for_test(move || {
            // Delete the same key from another thread while the write is
            // between its checkpoint and its rename, and give it time to
            // either finish or block, whichever the write path allows.
            let racer = racer.clone();
            let handle = std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(racer.delete(key))
                    .unwrap();
            });
            std::thread::sleep(Duration::from_millis(200));
            *started.lock().unwrap() = Some(handle);
        });
        let large = vec![b'n'; MAX_RECORD_BYTES + 1];
        storage.write_bytes(key, &large).await.unwrap();
        let handle = deleting.lock().unwrap().take().expect("the delete ran");
        handle.join().unwrap();
        let live = storage.exists(key).await.unwrap();
        storage.simulate_crash_for_test();
        let reopened = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        reopened.recover_wal().unwrap();
        assert_eq!(
            reopened.exists(key).await.unwrap(),
            live,
            "recovery disagreed with the state the process saw (present: {live})"
        );
    }

    /// A failed head write's undo must name the record that failed, not the
    /// value: a writer in per-write mode may land the very same bytes
    /// afterwards, durably, and recovery has to leave those alone whether the
    /// failed write was a creation or a replacement.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_failed_head_write_undo_spares_the_same_value_written_by_another_writer() {
        use crate::{CasAction, Durability, FileStorage, StorageCas, StorageRead};
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        let fresh = "fluree:file://ns/fresh.json";
        let existing = "fluree:file://ns/existing.json";
        storage.insert(existing, b"before").await.unwrap();
        FileStorage::checkpoint_wal(dir.path()).unwrap();
        storage.hold_wal_segments_for_test().unwrap();
        let ns = dir.path().join("ns");
        std::fs::write(ns.join("fresh.lock"), []).unwrap();
        std::fs::set_permissions(&ns, std::fs::Permissions::from_mode(0o555)).unwrap();
        for key in [fresh, existing] {
            let failed = storage
                .compare_and_swap(key, |_| Ok(CasAction::<()>::Write(b"same".to_vec())))
                .await;
            assert!(failed.is_err(), "staging {key} must fail");
        }
        std::fs::set_permissions(&ns, std::fs::Permissions::from_mode(0o755)).unwrap();
        let other = FileStorage::new(dir.path()).with_durability(Durability::Sync);
        for key in [fresh, existing] {
            other
                .compare_and_swap(key, |_| Ok(CasAction::<()>::Write(b"same".to_vec())))
                .await
                .unwrap();
        }
        storage.simulate_crash_for_test();
        let reopened = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        reopened.recover_wal().unwrap();
        for key in [fresh, existing] {
            assert_eq!(
                reopened.read_bytes(key).await.unwrap(),
                b"same",
                "{key}: recovery undid a head another writer wrote"
            );
        }
    }

    /// A handle's first operation may be a compare-and-swap on a key the
    /// leftover log holds a record for. Attaching the log replays that record
    /// under the key's sidecar lock, so it must happen before the swap takes
    /// that lock, or the handle waits on itself.
    #[tokio::test]
    async fn a_first_compare_and_swap_replays_before_taking_the_key_lock() {
        use crate::{CasAction, Durability, FileStorage, StorageCas, StorageRead};

        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        storage.hold_wal_segments_for_test().unwrap();
        let key = "fluree:file://head.json";
        storage.insert(key, b"first").await.unwrap();
        storage
            .compare_and_swap(key, |_| Ok(CasAction::<()>::Write(b"second".to_vec())))
            .await
            .unwrap();
        storage.simulate_crash_for_test();
        let fresh = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        let swapped = tokio::time::timeout(
            Duration::from_secs(5),
            fresh.compare_and_swap(key, |current| {
                assert_eq!(current, Some(&b"second"[..]));
                Ok(CasAction::<()>::Write(b"third".to_vec()))
            }),
        )
        .await
        .expect("the swap deadlocked on its own replay");
        swapped.unwrap();
        assert_eq!(fresh.read_bytes(key).await.unwrap(), b"third");
    }

    /// `sync` is the barrier a caller uses before publishing a pointer
    /// somewhere other than this root's own head file, so it flushes the
    /// log's tail too, not only derived content.
    #[tokio::test]
    async fn sync_flushes_the_logged_tail() {
        use crate::{Durability, FileStorage, StorageWrite};

        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        storage.hold_wal_segments_for_test().unwrap();
        storage
            .write_bytes("fluree:file://commit.bin", b"commit")
            .await
            .unwrap();
        let before = storage.fsyncs_issued();
        storage.sync().await.unwrap();
        assert!(
            storage.fsyncs_issued() > before,
            "sync returned with the log still dirty"
        );
    }

    #[tokio::test]
    async fn review_empty_unavailable_owner_does_not_break_startup_or_read_misses() {
        use crate::{Durability, FileStorage, StorageRead};
        let dir = tempfile::tempdir().unwrap();
        let owner = dir.path().join(WAL_DIR).join(OWNERS_DIR).join("node-1");
        // Opening a directory for read/write fails on all supported Unix
        // platforms; exercise the same fallback for an unusable owner lock.
        std::fs::create_dir_all(owner.join(LOCK_FILE)).unwrap();
        let storage = FileStorage::new(dir.path())
            .with_durability(Durability::Wal)
            .with_wal_owner("node-1");
        storage.recover_wal().unwrap();
        assert_eq!(storage.effective_durability(), Durability::Sync);
        assert!(matches!(
            storage.read_bytes("fluree:file://missing").await,
            Err(crate::Error::NotFound(_))
        ));
        // A retained log is different: it may hold acknowledged data.
        std::fs::write(owner.join("00000001.wal"), b"retained").unwrap();
        assert!(storage
            .recover_wal()
            .unwrap_err()
            .to_string()
            .contains("retained WAL"));
        assert!(owner.join("00000001.wal").exists());
    }

    #[cfg(target_vendor = "apple")]
    #[test]
    fn review_unsupported_flock_on_empty_owner_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let owner = dir.path().join(WAL_DIR).join(OWNERS_DIR).join("node-1");
        std::fs::create_dir_all(&owner).unwrap();
        let lock_path = owner.join(LOCK_FILE);
        assert!(std::process::Command::new("mkfifo")
            .arg(&lock_path)
            .status()
            .unwrap()
            .success());
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)
            .unwrap();
        let error = lock.try_lock_exclusive().unwrap_err();
        assert_ne!(error.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(replay_unowned(dir.path()).unwrap(), 0);
    }

    #[tokio::test]
    async fn review_failed_plain_write_is_cancelled_before_recovery() {
        use crate::{Durability, FileStorage, StorageRead, StorageWrite};
        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        storage.hold_wal_segments_for_test().unwrap();
        let path = dir.path().join("head");
        std::fs::create_dir(&path).unwrap(); // The final rename must fail.
        assert!(storage
            .write_bytes("fluree:file://head", b"failed")
            .await
            .is_err());
        storage.simulate_crash_for_test();
        std::fs::remove_dir(&path).unwrap();
        let reopened = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        reopened.recover_wal().unwrap();
        assert!(matches!(
            reopened.read_bytes("fluree:file://head").await,
            Err(crate::Error::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn review_failed_plain_delete_preserves_a_later_writer() {
        use crate::{Durability, FileStorage, StorageRead, StorageWrite};
        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        storage.hold_wal_segments_for_test().unwrap();
        let path = dir.path().join("head");
        std::fs::create_dir(&path).unwrap(); // remove_file cannot delete it.
        assert!(storage.delete("fluree:file://head").await.is_err());
        std::fs::remove_dir(&path).unwrap();
        let other = FileStorage::new(dir.path()).with_durability(Durability::Sync);
        other
            .write_bytes("fluree:file://head", b"acknowledged")
            .await
            .unwrap();
        storage.simulate_crash_for_test();
        let reopened = FileStorage::new(dir.path()).with_durability(Durability::Wal);
        reopened.recover_wal().unwrap();
        assert_eq!(
            reopened.read_bytes("fluree:file://head").await.unwrap(),
            b"acknowledged"
        );
    }

    #[test]
    fn review_retirement_flushes_repeated_keys_once_before_unlink() {
        let dir = tempfile::tempdir().unwrap();
        let log = owned(dir.path());
        log.hold_segments();
        std::fs::write(dir.path().join("head"), b"value").unwrap();
        for _ in 0..20 {
            drop(
                log.append(
                    Op::Write {
                        key: "head",
                        bytes: b"value",
                    },
                    false,
                )
                .unwrap(),
            );
        }
        rotate_now(&log);
        let segment = log.inner.lock().unwrap().closed[0].path.clone();
        let before = log.fsyncs_issued();
        assert_eq!(log.retire_closed().unwrap(), 0);
        assert_eq!(
            log.fsyncs_issued() - before,
            3,
            "file, root directory, WAL directory"
        );
        assert_eq!(
            *log.device_trace.lock().unwrap(),
            vec![
                DeviceStep::FilesFlushed(segment.clone()),
                DeviceStep::Unlink(segment),
                DeviceStep::DirFlushed,
            ]
        );
    }

    fn legacy_frame(seq: u64, payload: &[u8]) -> Vec<u8> {
        let mut frame = FRAME_MAGIC.to_vec();
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&seq.to_le_bytes());
        frame.extend_from_slice(payload);
        let hash = Sha256::digest(&frame);
        frame.extend_from_slice(&hash);
        frame
    }

    #[test]
    fn review_replays_legacy_and_new_checksum_segments_together() {
        let dir = tempfile::tempdir().unwrap();
        let wal = dir.path().join(WAL_DIR);
        std::fs::create_dir(&wal).unwrap();
        for (id, magic, frame) in [
            (
                1,
                LEGACY_SEGMENT_MAGIC,
                legacy_frame(
                    1,
                    &Op::Write {
                        key: "head",
                        bytes: b"old",
                    }
                    .encode(),
                ),
            ),
            (
                2,
                SEGMENT_MAGIC,
                encode_frame(
                    2,
                    &Op::Write {
                        key: "head",
                        bytes: b"new",
                    }
                    .encode(),
                ),
            ),
        ] {
            let mut bytes = magic.to_vec();
            bytes.extend_from_slice(&(id as u64).to_le_bytes());
            bytes.extend_from_slice(&frame);
            std::fs::write(wal.join(format!("{id:08}.wal")), bytes).unwrap();
        }
        let (_, _, records) = replay(dir.path(), &wal, &AtomicU64::new(0)).unwrap();
        assert_eq!(records, 2);
        assert_eq!(std::fs::read(dir.path().join("head")).unwrap(), b"new");
        assert!(segment_paths(&wal).unwrap().is_empty());
    }

    #[test]
    fn review_new_checksum_covers_payload_and_sequence() {
        let payload = Op::Write {
            key: "head",
            bytes: b"value",
        }
        .encode();
        let mut bytes = SEGMENT_MAGIC.to_vec();
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&encode_frame(1, &payload));
        assert!(decode_segment(&bytes, None).unwrap().clean);
        let mut changed_payload = bytes.clone();
        changed_payload[SEGMENT_HEADER + FRAME_HEADER + payload.len() - 1] ^= 1;
        assert!(!decode_segment(&changed_payload, None).unwrap().clean);
        // Keep the structural sequence checks satisfied; the checksum must
        // independently detect that the header's sequence was changed.
        bytes[8..16].copy_from_slice(&2u64.to_le_bytes());
        bytes[SEGMENT_HEADER + 8..SEGMENT_HEADER + 16].copy_from_slice(&2u64.to_le_bytes());
        assert!(!decode_segment(&bytes, None).unwrap().clean);
    }

    #[test]
    fn review_unknown_segment_version_is_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let wal = dir.path().join(WAL_DIR);
        std::fs::create_dir(&wal).unwrap();
        let path = wal.join("00000001.wal");
        let mut bytes = b"FRDOSEG9".to_vec();
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&encode_frame(1, &Op::Delete { key: "head" }.encode()));
        std::fs::write(&path, &bytes).unwrap();
        assert!(replay(dir.path(), &wal, &AtomicU64::new(0)).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }
}
