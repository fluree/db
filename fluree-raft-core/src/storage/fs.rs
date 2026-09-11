//! Filesystem-backed [`RaftStorage`] backend.
//!
//! Layout under the storage root:
//!
//! ```text
//! <root>/
//!   vote             # postcard-serialized Vote
//!   committed.slots  # two 64-byte slots, the newer valid one wins
//!   last_purged      # postcard-serialized LogId (absent when never purged)
//!   log/
//!     <first>.seg    # framed entries from index <first> up, zero-padded
//!                    #   16-char hex so directory listings sort naturally;
//!                    #   <index>.entry files from earlier releases are
//!                    #   folded into a segment on open
//!   snapshots/
//!     current        # plain-text snapshot id
//!     <id>.meta      # postcard-serialized SnapshotMeta
//!     <id>.data      # raw snapshot bytes
//! ```
//!
//! Entries append to segments with one flush per batch and the
//! committed watermark is rewritten in place with one flush; see the
//! segmented-log section below. Every other mutation is
//! atomic-write-then-rename with `fsync` of both the temp file and
//! the parent directory (so the rename's directory entry is durable
//! across power loss, not just the file contents). Directory fsync
//! is a no-op on non-Unix targets, which don't expose an equivalent
//! operation.

use super::{
    LogEntry, LogId, LogState, RaftLogStore, RaftSnapshotStore, RaftStorage, SnapshotId,
    SnapshotMeta, StorageError, Vote,
};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
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

// ---- Segmented log ----------------------------------------------------------
//
// Entries live in append-only segments under `log/`, one flush per
// `append` batch instead of a file flush per entry plus a directory
// flush per batch. The committed watermark lives in a two-slot file
// rewritten in place, one flush instead of a staged write and a
// directory flush. Vote, purge marker and snapshots keep their
// atomic-write discipline; they are rare.
//
// Recovery reads segments in order. A frame that does not decode at
// the end of the last segment is a torn append that was never
// acknowledged and is cut off; a bad frame before a later segment
// cannot be explained that way and fails the open. Per-entry files
// from earlier releases are folded into a segment once, on open.

const SEGMENT_MAGIC: &[u8; 8] = b"FRSG0001";
const SEGMENT_HEADER: usize = 16;
const FRAME_MAGIC: &[u8; 4] = b"FRLE";
const FRAME_HEADER: usize = 4 + 4 + 8 + 8;
const FRAME_HASH: usize = 8;
const DEFAULT_SEGMENT_BYTES: u64 = 4 * 1024 * 1024;
const COMMITTED_MAGIC: &[u8; 4] = b"FRCM";
const COMMITTED_SLOT: usize = 64;
const COMMITTED_FILE: &str = "committed.slots";

fn segment_filename(first_index: u64) -> String {
    format!("{first_index:016x}.seg")
}

fn parse_segment_filename(name: &str) -> Option<u64> {
    u64::from_str_radix(name.strip_suffix(".seg")?, 16).ok()
}

fn frame_hash(bytes: &[u8]) -> u64 {
    xxhash_rust::xxh64::xxh64(bytes, 0x5261_6674_4c6f_6721)
}

fn encode_frame(entry: &LogEntry) -> Vec<u8> {
    let mut frame = Vec::with_capacity(FRAME_HEADER + entry.payload.len() + FRAME_HASH);
    frame.extend_from_slice(FRAME_MAGIC);
    frame.extend_from_slice(&(entry.payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&entry.log_id.index.to_le_bytes());
    frame.extend_from_slice(&entry.log_id.term.to_le_bytes());
    frame.extend_from_slice(&entry.payload);
    let hash = frame_hash(&frame);
    frame.extend_from_slice(&hash.to_le_bytes());
    frame
}

/// The frame at `at`: its id, payload and total length. `None` for
/// anything short, unrecognized or failing its hash.
fn decode_frame(bytes: &[u8], at: usize) -> Option<(LogId, &[u8], usize)> {
    let header = bytes.get(at..at + FRAME_HEADER)?;
    if &header[..4] != FRAME_MAGIC {
        return None;
    }
    let len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
    let index = u64::from_le_bytes(header[8..16].try_into().unwrap());
    let term = u64::from_le_bytes(header[16..24].try_into().unwrap());
    let end = at + FRAME_HEADER + len;
    let stored = bytes.get(end..end + FRAME_HASH)?;
    if frame_hash(&bytes[at..end]).to_le_bytes() != stored {
        return None;
    }
    Some((
        LogId::new(term, index),
        &bytes[at + FRAME_HEADER..end],
        FRAME_HEADER + len + FRAME_HASH,
    ))
}

fn encode_committed(generation: u64, id: Option<LogId>) -> [u8; COMMITTED_SLOT] {
    let mut slot = [0u8; COMMITTED_SLOT];
    slot[..4].copy_from_slice(COMMITTED_MAGIC);
    slot[4..12].copy_from_slice(&generation.to_le_bytes());
    if let Some(id) = id {
        slot[12] = 1;
        slot[13..21].copy_from_slice(&id.term.to_le_bytes());
        slot[21..29].copy_from_slice(&id.index.to_le_bytes());
    }
    let hash = frame_hash(&slot[..COMMITTED_SLOT - FRAME_HASH]);
    slot[COMMITTED_SLOT - FRAME_HASH..].copy_from_slice(&hash.to_le_bytes());
    slot
}

fn decode_committed(slot: &[u8]) -> Option<(u64, Option<LogId>)> {
    if slot.len() != COMMITTED_SLOT || &slot[..4] != COMMITTED_MAGIC {
        return None;
    }
    if frame_hash(&slot[..COMMITTED_SLOT - FRAME_HASH]).to_le_bytes()
        != slot[COMMITTED_SLOT - FRAME_HASH..]
    {
        return None;
    }
    let generation = u64::from_le_bytes(slot[4..12].try_into().unwrap());
    let id = (slot[12] == 1).then(|| {
        LogId::new(
            u64::from_le_bytes(slot[13..21].try_into().unwrap()),
            u64::from_le_bytes(slot[21..29].try_into().unwrap()),
        )
    });
    Some((generation, id))
}

fn fsync_dir_blocking(path: &Path) -> Result<(), StorageError> {
    #[cfg(unix)]
    {
        std::fs::File::open(path)
            .and_then(|dir| dir.sync_all())
            .map_err(|e| io_err("sync dir", e))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

fn atomic_write_blocking(path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    use std::io::Write;
    let tmp = path.with_extension("tmp");
    {
        let mut file = std::fs::File::create(&tmp).map_err(|e| io_err("create tmp", e))?;
        file.write_all(bytes).map_err(|e| io_err("write tmp", e))?;
        file.sync_all().map_err(|e| io_err("sync tmp", e))?;
    }
    std::fs::rename(&tmp, path).map_err(|e| io_err("rename tmp", e))?;
    if let Some(parent) = path.parent() {
        fsync_dir_blocking(parent)?;
    }
    Ok(())
}

fn read_postcard<T: serde::de::DeserializeOwned>(
    path: &Path,
    what: &str,
) -> Result<Option<T>, StorageError> {
    match std::fs::read(path) {
        Ok(bytes) => postcard::from_bytes(&bytes)
            .map(Some)
            .map_err(|e| ser_err(what, e)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(io_err(what, e)),
    }
}

struct Segment {
    first_index: u64,
    /// Highest index physically present, purged or not. `None` for a
    /// segment holding only its header.
    last_index: Option<u64>,
    path: PathBuf,
    len: u64,
}

#[derive(Clone, Copy)]
struct Loc {
    segment: u64,
    offset: u64,
    len: u32,
    term: u64,
}

/// One thing `truncate_from` or `purge_through` did to the device, in
/// order, for the tests that pin their crash ordering.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
enum DeviceStep {
    Unlink(PathBuf),
    DirFsync,
    Shorten,
}

struct Inner {
    root: PathBuf,
    log_dir: PathBuf,
    segment_bytes: u64,
    segments: Vec<Segment>,
    #[cfg(test)]
    device_trace: Vec<DeviceStep>,
    /// Live entries (above the purge cutoff) by index.
    index: BTreeMap<u64, Loc>,
    /// Write handle on the last segment, opened on demand.
    active: Option<std::fs::File>,
    last_purged: Option<LogId>,
    committed: Option<LogId>,
    committed_generation: u64,
    committed_file: Option<std::fs::File>,
}

impl Inner {
    fn open(root: PathBuf, segment_bytes: u64) -> Result<Self, StorageError> {
        let log_dir = root.join("log");
        std::fs::create_dir_all(&log_dir).map_err(|e| io_err("create log dir", e))?;
        let last_purged = read_postcard(&root.join("last_purged"), "decode last_purged")?;
        let mut inner = Self {
            root,
            log_dir,
            segment_bytes,
            segments: Vec::new(),
            #[cfg(test)]
            device_trace: Vec::new(),
            index: BTreeMap::new(),
            active: None,
            last_purged,
            committed: None,
            committed_generation: 0,
            committed_file: None,
        };
        inner.migrate_legacy_entries()?;
        inner.scan_segments()?;
        inner.load_committed()?;
        Ok(inner)
    }

    fn cutoff(&self) -> Option<u64> {
        self.last_purged.map(|p| p.index)
    }

    fn live(&self, index: u64) -> bool {
        self.cutoff().is_none_or(|c| index > c)
    }

    /// Fold per-entry files from earlier releases into one segment. The
    /// contiguous run above the purge cutoff is kept, exactly what the
    /// old `log_state` would have reported; orphans past a gap are
    /// dropped as they were on every restart. The segment is published
    /// by rename before any entry file is removed, so a crash at any
    /// point leaves either the entries or the finished segment.
    fn migrate_legacy_entries(&mut self) -> Result<(), StorageError> {
        let mut indices = Vec::new();
        let mut has_segment = false;
        for entry in std::fs::read_dir(&self.log_dir).map_err(|e| io_err("read log dir", e))? {
            let entry = entry.map_err(|e| io_err("iter log dir", e))?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(index) = parse_entry_filename(name) {
                indices.push(index);
            } else if parse_segment_filename(name).is_some() {
                has_segment = true;
            } else if name.ends_with(".seg.tmp") {
                std::fs::remove_file(entry.path())
                    .map_err(|e| io_err("remove stale segment", e))?;
            }
        }
        if indices.is_empty() {
            return Ok(());
        }
        indices.sort_unstable();
        if !has_segment {
            let anchor = self.cutoff().map_or(1, |c| c + 1);
            let run: Vec<u64> = indices
                .iter()
                .copied()
                .filter(|&i| self.live(i))
                .scan(anchor, |expected, i| {
                    (i == *expected).then(|| {
                        *expected += 1;
                        i
                    })
                })
                .collect();
            if let Some(&first) = run.first() {
                let mut buf = SEGMENT_MAGIC.to_vec();
                buf.extend_from_slice(&first.to_le_bytes());
                for index in &run {
                    let bytes = std::fs::read(self.log_dir.join(entry_filename(*index)))
                        .map_err(|e| io_err("read legacy entry", e))?;
                    let entry: LogEntry = postcard::from_bytes(&bytes)
                        .map_err(|e| ser_err("decode legacy entry", e))?;
                    buf.extend_from_slice(&encode_frame(&entry));
                }
                let path = self.log_dir.join(segment_filename(first));
                let tmp = self.log_dir.join(format!("{first:016x}.seg.tmp"));
                {
                    use std::io::Write;
                    let mut file =
                        std::fs::File::create(&tmp).map_err(|e| io_err("create segment", e))?;
                    file.write_all(&buf)
                        .map_err(|e| io_err("write segment", e))?;
                    file.sync_all().map_err(|e| io_err("sync segment", e))?;
                }
                std::fs::rename(&tmp, &path).map_err(|e| io_err("publish segment", e))?;
                fsync_dir_blocking(&self.log_dir)?;
            }
            tracing::info!(
                entries = run.len(),
                dropped = indices.len() - run.len(),
                "folded per-entry raft log files into a segment"
            );
        }
        for index in indices {
            match std::fs::remove_file(self.log_dir.join(entry_filename(index))) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(io_err("remove legacy entry", e)),
            }
        }
        fsync_dir_blocking(&self.log_dir)
    }

    fn scan_segments(&mut self) -> Result<(), StorageError> {
        let mut found = Vec::new();
        for entry in std::fs::read_dir(&self.log_dir).map_err(|e| io_err("read log dir", e))? {
            let entry = entry.map_err(|e| io_err("iter log dir", e))?;
            if let Some(first) = entry.file_name().to_str().and_then(parse_segment_filename) {
                found.push((first, entry.path()));
            }
        }
        found.sort();
        let last = found.len().saturating_sub(1);
        let mut expected_next: Option<u64> = None;
        for (i, (first, path)) in found.into_iter().enumerate() {
            let bytes = std::fs::read(&path).map_err(|e| io_err("read segment", e))?;
            if bytes.len() < SEGMENT_HEADER
                || &bytes[..8] != SEGMENT_MAGIC
                || u64::from_le_bytes(bytes[8..16].try_into().unwrap()) != first
            {
                if i == last {
                    // Creation interrupted before the header landed. An entry
                    // is only appended after the header is durable, so this
                    // segment holds nothing acknowledged.
                    warn!(
                        segment = %path.display(),
                        "raft log segment has no valid header; removing it as never used"
                    );
                    std::fs::remove_file(&path).map_err(|e| io_err("remove segment", e))?;
                    fsync_dir_blocking(&self.log_dir)?;
                    break;
                }
                return Err(StorageError::corruption(format!(
                    "raft log segment {} has a bad header",
                    path.display()
                )));
            }
            if expected_next.is_some_and(|next| next != first) {
                return Err(StorageError::corruption(format!(
                    "raft log segment {} does not continue the previous segment",
                    path.display()
                )));
            }
            let mut at = SEGMENT_HEADER;
            let mut physical_next = first;
            let mut last_index = None;
            loop {
                if at == bytes.len() {
                    break;
                }
                match decode_frame(&bytes, at) {
                    Some((id, _, frame_len)) if id.index == physical_next => {
                        if self.live(id.index) {
                            self.index.insert(
                                id.index,
                                Loc {
                                    segment: first,
                                    offset: at as u64,
                                    len: frame_len as u32,
                                    term: id.term,
                                },
                            );
                        }
                        last_index = Some(id.index);
                        physical_next += 1;
                        at += frame_len;
                    }
                    _ if i == last => {
                        // A torn append: never acknowledged, so never
                        // relied upon. Cut it off so the next append
                        // continues from a clean frame boundary.
                        warn!(
                            segment = %path.display(),
                            kept = last_index,
                            "raft log segment ends in a torn frame; discarding it"
                        );
                        let file = std::fs::OpenOptions::new()
                            .write(true)
                            .open(&path)
                            .map_err(|e| io_err("open segment", e))?;
                        file.set_len(at as u64)
                            .map_err(|e| io_err("cut segment", e))?;
                        file.sync_all().map_err(|e| io_err("sync segment", e))?;
                        break;
                    }
                    _ => {
                        return Err(StorageError::corruption(format!(
                            "raft log segment {} is damaged before a later segment \
                             (frame at byte {at}); move it aside only if the entries \
                             it held are covered by a snapshot",
                            path.display()
                        )));
                    }
                }
            }
            self.segments.push(Segment {
                first_index: first,
                last_index,
                path,
                len: at as u64,
            });
            expected_next = Some(physical_next);
        }
        Ok(())
    }

    fn load_committed(&mut self) -> Result<(), StorageError> {
        let slots = self.root.join(COMMITTED_FILE);
        match std::fs::read(&slots) {
            Ok(bytes) => {
                let best = bytes
                    .chunks(COMMITTED_SLOT)
                    .filter_map(decode_committed)
                    .max_by_key(|(generation, _)| *generation);
                if let Some((generation, id)) = best {
                    self.committed_generation = generation;
                    self.committed = id;
                }
                return Ok(());
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_err("read committed", e)),
        }
        // An earlier release's marker, folded into the slot file once.
        let legacy = self.root.join("committed");
        if let Some(id) = read_postcard::<LogId>(&legacy, "decode committed")? {
            self.committed = Some(id);
            self.write_committed(Some(id))?;
            std::fs::remove_file(&legacy).map_err(|e| io_err("remove legacy committed", e))?;
            fsync_dir_blocking(&self.root)?;
        }
        Ok(())
    }

    fn write_committed(&mut self, id: Option<LogId>) -> Result<(), StorageError> {
        use std::io::{Seek, SeekFrom, Write};
        if self.committed_file.is_none() {
            let created = !self.root.join(COMMITTED_FILE).exists();
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(self.root.join(COMMITTED_FILE))
                .map_err(|e| io_err("open committed", e))?;
            if created {
                file.set_len((2 * COMMITTED_SLOT) as u64)
                    .map_err(|e| io_err("size committed", e))?;
                file.sync_all().map_err(|e| io_err("sync committed", e))?;
                fsync_dir_blocking(&self.root)?;
            }
            self.committed_file = Some(file);
        }
        let generation = self.committed_generation + 1;
        let slot = encode_committed(generation, id);
        let file = self.committed_file.as_mut().expect("opened above");
        // Alternate slots, so a torn write can only damage the older of
        // the two and the read side falls back to the other.
        file.seek(SeekFrom::Start(
            ((generation % 2) as usize * COMMITTED_SLOT) as u64,
        ))
        .map_err(|e| io_err("seek committed", e))?;
        file.write_all(&slot)
            .map_err(|e| io_err("write committed", e))?;
        file.sync_all().map_err(|e| io_err("sync committed", e))?;
        self.committed_generation = generation;
        self.committed = id;
        Ok(())
    }

    /// Create the next segment: header written and flushed under a temporary
    /// name, then renamed into place, so a crash can never leave a segment
    /// whose header is missing or partial.
    fn start_segment(&mut self, first_index: u64) -> Result<(), StorageError> {
        use std::io::Write;
        let path = self.log_dir.join(segment_filename(first_index));
        let tmp = self.log_dir.join(format!("{first_index:016x}.seg.tmp"));
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|e| io_err("create segment", e))?;
        let mut header = SEGMENT_MAGIC.to_vec();
        header.extend_from_slice(&first_index.to_le_bytes());
        file.write_all(&header)
            .map_err(|e| io_err("write segment header", e))?;
        file.sync_all().map_err(|e| io_err("sync segment", e))?;
        std::fs::rename(&tmp, &path).map_err(|e| io_err("publish segment", e))?;
        fsync_dir_blocking(&self.log_dir)?;
        self.segments.push(Segment {
            first_index,
            last_index: None,
            path,
            len: SEGMENT_HEADER as u64,
        });
        self.active = Some(file);
        Ok(())
    }

    fn active_file(&mut self) -> Result<&mut std::fs::File, StorageError> {
        if self.active.is_none() {
            let path = &self.segments.last().expect("a segment exists").path;
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .map_err(|e| io_err("open segment", e))?;
            self.active = Some(file);
        }
        Ok(self.active.as_mut().expect("set above"))
    }

    fn append(&mut self, entries: &[LogEntry]) -> Result<(), StorageError> {
        use std::io::{Seek, SeekFrom, Write};
        let Some(first) = entries.first().map(|e| e.log_id.index) else {
            return Ok(());
        };
        if let Some((&last, _)) = self.index.iter().next_back() {
            if first != last + 1 {
                return Err(StorageError::corruption(format!(
                    "append at index {first} does not continue the log at {last}"
                )));
            }
        }
        // A segment holding only its header names a first index; if that is
        // not the index being appended, it is a leftover (a truncation that
        // emptied it, or a purge that outran it) and must go, or its entries
        // would read as torn on the next open.
        if let Some(seg) = self
            .segments
            .pop_if(|seg| seg.last_index.is_none() && seg.first_index != first)
        {
            self.active = None;
            std::fs::remove_file(&seg.path).map_err(|e| io_err("remove segment", e))?;
            fsync_dir_blocking(&self.log_dir)?;
        }
        let needs_segment = self
            .segments
            .last()
            .is_none_or(|seg| seg.len >= self.segment_bytes);
        if needs_segment {
            self.start_segment(first)?;
        }
        let mut buf = Vec::new();
        let mut locs = Vec::with_capacity(entries.len());
        {
            let seg = self.segments.last().expect("ensured above");
            for entry in entries {
                let frame = encode_frame(entry);
                locs.push((
                    entry.log_id.index,
                    Loc {
                        segment: seg.first_index,
                        offset: seg.len + buf.len() as u64,
                        len: frame.len() as u32,
                        term: entry.log_id.term,
                    },
                ));
                buf.extend_from_slice(&frame);
            }
        }
        let start = self.segments.last().expect("ensured above").len;
        let file = self.active_file()?;
        file.seek(SeekFrom::Start(start))
            .map_err(|e| io_err("seek segment", e))?;
        file.write_all(&buf)
            .map_err(|e| io_err("write segment", e))?;
        // The one flush an append pays.
        file.sync_all().map_err(|e| io_err("sync segment", e))?;
        let seg = self.segments.last_mut().expect("ensured above");
        seg.len += buf.len() as u64;
        seg.last_index = Some(entries.last().expect("non-empty").log_id.index);
        for (index, loc) in locs {
            self.index.insert(index, loc);
        }
        Ok(())
    }

    fn segment_path(&self, first_index: u64) -> Result<&Path, StorageError> {
        self.segments
            .iter()
            .find(|s| s.first_index == first_index)
            .map(|s| s.path.as_path())
            .ok_or_else(|| StorageError::corruption(format!("no segment starts at {first_index}")))
    }

    fn read_range(&self, range: Range<u64>) -> Result<Vec<LogEntry>, StorageError> {
        use std::io::{Read, Seek, SeekFrom};
        let mut out = Vec::new();
        let mut open: Option<(u64, std::fs::File)> = None;
        for (&index, loc) in self.index.range(range) {
            if open.as_ref().is_none_or(|(first, _)| *first != loc.segment) {
                let file = std::fs::File::open(self.segment_path(loc.segment)?)
                    .map_err(|e| io_err("open segment", e))?;
                open = Some((loc.segment, file));
            }
            let file = &mut open.as_mut().expect("opened above").1;
            let mut frame = vec![0u8; loc.len as usize];
            file.seek(SeekFrom::Start(loc.offset))
                .map_err(|e| io_err("seek entry", e))?;
            file.read_exact(&mut frame)
                .map_err(|e| io_err("read entry", e))?;
            let (id, payload, _) = decode_frame(&frame, 0)
                .filter(|(id, _, _)| id.index == index)
                .ok_or_else(|| {
                    StorageError::corruption(format!("raft log entry {index} failed its check"))
                })?;
            out.push(LogEntry {
                log_id: id,
                payload: payload.to_vec(),
            });
        }
        Ok(out)
    }

    fn truncate_from(&mut self, from_index: u64) -> Result<(), StorageError> {
        if self
            .index
            .iter()
            .next_back()
            .is_none_or(|(&last, _)| last < from_index)
        {
            return Ok(());
        }
        let cut = self
            .index
            .get(&from_index)
            .map(|loc| (loc.segment, loc.offset))
            .or_else(|| {
                // `from_index` sits below the live range: cut everything.
                self.index
                    .iter()
                    .next()
                    .map(|(_, loc)| (loc.segment, SEGMENT_HEADER as u64))
            })
            .expect("live entries exist");
        // Every crash point must leave a prefix of the log: a later segment
        // gone while the retained one still reaches past the cut reads as a
        // truncation not yet made, a hole does not. So later segments go
        // newest first, each unlink durable before the next, and the segment
        // holding the cut is shortened only once they are all gone.
        self.active = None;
        while self
            .segments
            .last()
            .is_some_and(|seg| seg.first_index > cut.0)
        {
            self.remove_last_segment()?;
        }
        if cut.1 == SEGMENT_HEADER as u64 {
            // Nothing of the segment survives the cut: remove it rather than
            // keep a header naming an index the log may never return to.
            self.remove_last_segment()?;
        } else {
            let file = self.active_file()?;
            file.set_len(cut.1).map_err(|e| io_err("cut segment", e))?;
            file.sync_all().map_err(|e| io_err("sync segment", e))?;
            #[cfg(test)]
            self.device_trace.push(DeviceStep::Shorten);
            let seg = self.segments.last_mut().expect("holds the cut");
            seg.len = cut.1;
            seg.last_index = Some(from_index - 1);
        }
        self.index.split_off(&from_index);
        Ok(())
    }

    /// Unlink the newest segment and make that durable before forgetting it.
    fn remove_last_segment(&mut self) -> Result<(), StorageError> {
        let seg = self.segments.last().expect("a segment to remove");
        std::fs::remove_file(&seg.path).map_err(|e| io_err("remove segment", e))?;
        #[cfg(test)]
        self.device_trace.push(DeviceStep::Unlink(seg.path.clone()));
        fsync_dir_blocking(&self.log_dir)?;
        #[cfg(test)]
        self.device_trace.push(DeviceStep::DirFsync);
        self.segments.pop();
        Ok(())
    }

    fn purge_through(&mut self, log_id: LogId) -> Result<(), StorageError> {
        if self.last_purged.is_some_and(|p| p.index >= log_id.index) {
            return Ok(());
        }
        // The marker first: a crash after it leaves entries the scan
        // ignores, never a hole below a stale marker.
        let bytes = postcard::to_allocvec(&log_id).map_err(|e| ser_err("encode last_purged", e))?;
        atomic_write_blocking(&self.root.join("last_purged"), &bytes)?;
        self.last_purged = Some(log_id);
        self.index = self.index.split_off(&(log_id.index + 1));
        self.active = None;
        // Oldest first, each unlink durable before the next, so the segments
        // on the device are a contiguous suffix at every crash point. A newer
        // unlink outliving an older one would leave a hole the scan refuses,
        // even below the marker.
        let mut kept = Vec::with_capacity(self.segments.len());
        for seg in self.segments.drain(..) {
            // Whole segments at or below the marker go, and so does an empty
            // one whose first index the marker has passed.
            let obsolete = match seg.last_index {
                Some(last) => last <= log_id.index,
                None => seg.first_index <= log_id.index,
            };
            if obsolete {
                std::fs::remove_file(&seg.path).map_err(|e| io_err("remove segment", e))?;
                #[cfg(test)]
                self.device_trace.push(DeviceStep::Unlink(seg.path.clone()));
                fsync_dir_blocking(&self.log_dir)?;
                #[cfg(test)]
                self.device_trace.push(DeviceStep::DirFsync);
            } else {
                kept.push(seg);
            }
        }
        self.segments = kept;
        Ok(())
    }

    fn log_state(&self) -> LogState {
        LogState {
            last_purged: self.last_purged,
            last_log: self
                .index
                .iter()
                .next_back()
                .map(|(&index, loc)| LogId::new(loc.term, index)),
        }
    }
}

/// Filesystem-backed implementation of [`RaftLogStore`].
pub struct FsRaftLogStore {
    root: PathBuf,
    inner: Arc<Mutex<Inner>>,
}

impl FsRaftLogStore {
    /// Open or create the log store rooted at `root`. Creates the
    /// directory tree if missing, folds per-entry files from earlier
    /// releases into a segment, and replays the segments.
    pub async fn open(root: impl Into<PathBuf>) -> Result<Self, StorageError> {
        Self::open_with_segment_bytes(root, DEFAULT_SEGMENT_BYTES).await
    }

    /// [`Self::open`] with the size at which a segment is closed and the
    /// next one started. Tests use small values to exercise rotation.
    #[doc(hidden)]
    pub async fn open_with_segment_bytes(
        root: impl Into<PathBuf>,
        segment_bytes: u64,
    ) -> Result<Self, StorageError> {
        let root = root.into();
        let for_open = root.clone();
        let inner = tokio::task::spawn_blocking(move || Inner::open(for_open, segment_bytes))
            .await
            .map_err(|e| StorageError::other(format!("open join: {e}")))??;
        Ok(Self {
            root,
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    fn vote_path(&self) -> PathBuf {
        self.root.join("vote")
    }

    #[cfg(test)]
    fn last_purged_path(&self) -> PathBuf {
        self.root.join("last_purged")
    }

    #[cfg(test)]
    fn log_dir(&self) -> PathBuf {
        self.root.join("log")
    }

    /// Drain what the last truncation or purge did to the device, in order.
    #[cfg(test)]
    async fn device_trace(&self) -> Vec<DeviceStep> {
        self.with_inner(|inner| Ok(std::mem::take(&mut inner.device_trace)))
            .await
            .unwrap()
    }

    /// Run `f` against the store's state on the blocking pool.
    async fn with_inner<T, F>(&self, f: F) -> Result<T, StorageError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Inner) -> Result<T, StorageError> + Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut guard = inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            f(&mut guard)
        })
        .await
        .map_err(|e| StorageError::other(format!("log store join: {e}")))?
    }
}

#[async_trait]
impl RaftLogStore for FsRaftLogStore {
    async fn append(&self, entries: &[LogEntry]) -> Result<(), StorageError> {
        let entries = entries.to_vec();
        self.with_inner(move |inner| inner.append(&entries)).await
    }

    async fn read_range(&self, range: Range<u64>) -> Result<Vec<LogEntry>, StorageError> {
        self.with_inner(move |inner| inner.read_range(range)).await
    }

    async fn truncate_from(&self, from_index: u64) -> Result<(), StorageError> {
        self.with_inner(move |inner| inner.truncate_from(from_index))
            .await
    }

    async fn purge_through(&self, log_id: LogId) -> Result<(), StorageError> {
        self.with_inner(move |inner| inner.purge_through(log_id))
            .await
    }

    async fn log_state(&self) -> Result<LogState, StorageError> {
        self.with_inner(|inner| Ok(inner.log_state())).await
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
        self.with_inner(move |inner| inner.write_committed(log_id))
            .await
    }

    async fn read_committed(&self) -> Result<Option<LogId>, StorageError> {
        self.with_inner(|inner| Ok(inner.committed)).await
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

    /// A segment's files in index order, for tests that damage them.
    fn segment_files(store: &FsRaftLogStore) -> Vec<PathBuf> {
        let mut files: Vec<_> = std::fs::read_dir(store.log_dir())
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "seg"))
            .collect();
        files.sort();
        files
    }

    async fn reopen(dir: &TempDir) -> FsRaftLogStore {
        FsRaftLogStore::open(dir.path().to_path_buf())
            .await
            .unwrap()
    }

    /// Simulates a crash mid-`truncate_from` across segments: the later
    /// segment was removed but the one holding the cut was not yet
    /// shortened. The surviving log must be a contiguous prefix — no
    /// missing-middle hole — so `log_state` reports a coherent
    /// `last_log` and `read_range` returns every index up to it.
    #[tokio::test]
    async fn partial_truncate_leaves_contiguous_prefix() {
        let dir = TempDir::new().unwrap();
        // Tiny segments: every append starts a new one.
        let store = FsRaftLogStore::open_with_segment_bytes(dir.path().to_path_buf(), 1)
            .await
            .unwrap();
        store.append(&[entry(1, 1), entry(1, 2)]).await.unwrap();
        store.append(&[entry(1, 3)]).await.unwrap();
        store.append(&[entry(1, 4), entry(1, 5)]).await.unwrap();
        let files = segment_files(&store);
        assert_eq!(files.len(), 3);
        drop(store);

        // Hand-remove the last segment, as a `truncate_from(2)` that
        // crashed after removing later segments would leave things.
        std::fs::remove_file(&files[2]).unwrap();
        let store = reopen(&dir).await;
        let state = store.log_state().await.unwrap();
        assert_eq!(state.last_log, Some(LogId::new(1, 3)));
        let got = store.read_range(0..100).await.unwrap();
        let indices: Vec<u64> = got.iter().map(|e| e.log_id.index).collect();
        assert_eq!(indices, vec![1, 2, 3]);
    }

    /// The unlinks a directory fsync has not yet covered at `crash_at`, in
    /// the order they were issued. A crash there may lose any of them.
    fn unsynced_unlinks(trace: &[DeviceStep], crash_at: usize) -> Vec<PathBuf> {
        let mut unsynced = Vec::new();
        for step in &trace[..crash_at] {
            match step {
                DeviceStep::Unlink(path) => unsynced.push(path.clone()),
                DeviceStep::DirFsync => unsynced.clear(),
                DeviceStep::Shorten => {}
            }
        }
        unsynced
    }

    async fn three_segments() -> (TempDir, FsRaftLogStore, Vec<(PathBuf, Vec<u8>)>) {
        let dir = TempDir::new().unwrap();
        // Tiny segments: every append starts a new one.
        let store = FsRaftLogStore::open_with_segment_bytes(dir.path().to_path_buf(), 1)
            .await
            .unwrap();
        store.append(&[entry(1, 1), entry(1, 2)]).await.unwrap();
        store.append(&[entry(1, 3)]).await.unwrap();
        store.append(&[entry(1, 4)]).await.unwrap();
        let files = segment_files(&store);
        assert_eq!(files.len(), 3);
        let saved = files
            .iter()
            .map(|p| (p.clone(), std::fs::read(p).unwrap()))
            .collect();
        (dir, store, saved)
    }

    /// Every crash point inside `truncate_from` must leave a prefix of the
    /// log. Modelled on what a directory fsync promises: an unlink is on the
    /// device once one follows it and may be lost until then. A crash right
    /// after the retained segment is shortened has to find every later
    /// segment's unlink already covered, or reopen meets a hole.
    #[tokio::test]
    async fn later_segments_are_durably_gone_before_the_cut_segment_shrinks() {
        let (dir, store, saved) = three_segments().await;
        store.truncate_from(2).await.unwrap();
        let trace = store.device_trace().await;
        let shorten = trace
            .iter()
            .position(|s| *s == DeviceStep::Shorten)
            .expect("the cut segment was shortened");
        let lost = unsynced_unlinks(&trace, shorten + 1);
        drop(store);
        for (path, bytes) in &saved {
            if lost.contains(path) {
                std::fs::write(path, bytes).unwrap();
            }
        }
        let reopened = FsRaftLogStore::open(dir.path().to_path_buf())
            .await
            .unwrap_or_else(|e| {
                panic!("an interrupted truncation must not prevent restart: {e} (unlinks not durable when the segment shrank: {lost:?})")
            });
        let got = reopened.read_range(0..100).await.unwrap();
        let indices: Vec<u64> = got.iter().map(|e| e.log_id.index).collect();
        assert_eq!(indices, vec![1]);
    }

    /// The same for `purge_through`, which removes obsolete segments oldest
    /// first: at any crash point the segments still on the device must be a
    /// contiguous suffix. Losing an older unlink while a newer one held
    /// would leave a hole below the marker that the scan refuses.
    #[tokio::test]
    async fn purged_segments_go_oldest_first_each_durable_before_the_next() {
        let (dir, store, saved) = three_segments().await;
        store.purge_through(LogId::new(1, 3)).await.unwrap();
        let trace = store.device_trace().await;
        let last_fsync = trace
            .iter()
            .rposition(|s| *s == DeviceStep::DirFsync)
            .expect("removals were flushed");
        // Crash just before the final directory fsync, and lose the oldest
        // unlink it had not yet covered while keeping every newer one.
        let lost = unsynced_unlinks(&trace, last_fsync).into_iter().next();
        drop(store);
        if let Some(path) = &lost {
            let bytes = &saved.iter().find(|(p, _)| p == path).unwrap().1;
            std::fs::write(path, bytes).unwrap();
        }
        let reopened = FsRaftLogStore::open(dir.path().to_path_buf())
            .await
            .unwrap_or_else(|e| {
                panic!("an interrupted purge must not prevent restart: {e} (lost unlink: {lost:?})")
            });
        let got = reopened.read_range(0..100).await.unwrap();
        let indices: Vec<u64> = got.iter().map(|e| e.log_id.index).collect();
        assert_eq!(indices, vec![4]);
    }

    #[tokio::test]
    async fn entries_span_segments_and_survive_reopen() {
        let dir = TempDir::new().unwrap();
        let store = FsRaftLogStore::open_with_segment_bytes(dir.path().to_path_buf(), 64)
            .await
            .unwrap();
        for i in 1..=20 {
            store.append(&[entry(1, i)]).await.unwrap();
        }
        assert!(segment_files(&store).len() > 1, "rotation happened");
        let got = store.read_range(5..15).await.unwrap();
        let indices: Vec<u64> = got.iter().map(|e| e.log_id.index).collect();
        assert_eq!(indices, (5..15).collect::<Vec<_>>());
        drop(store);

        let store = reopen(&dir).await;
        let got = store.read_range(0..100).await.unwrap();
        assert_eq!(got.len(), 20);
        assert_eq!(got[19].payload, b"entry-1-20");
        assert_eq!(
            store.log_state().await.unwrap().last_log,
            Some(LogId::new(1, 20))
        );
        // The log continues where it left off after a reopen.
        store.append(&[entry(2, 21)]).await.unwrap();
        assert_eq!(
            store.log_state().await.unwrap().last_log,
            Some(LogId::new(2, 21))
        );
    }

    #[tokio::test]
    async fn truncate_inside_a_segment_then_append_continues() {
        let dir = TempDir::new().unwrap();
        let store = reopen(&dir).await;
        store
            .append(&[entry(1, 1), entry(1, 2), entry(1, 3), entry(1, 4)])
            .await
            .unwrap();
        store.truncate_from(3).await.unwrap();
        store.append(&[entry(2, 3), entry(2, 4)]).await.unwrap();
        let got = store.read_range(0..100).await.unwrap();
        let ids: Vec<LogId> = got.iter().map(|e| e.log_id).collect();
        assert_eq!(
            ids,
            vec![
                LogId::new(1, 1),
                LogId::new(1, 2),
                LogId::new(2, 3),
                LogId::new(2, 4)
            ]
        );
        drop(store);
        let store = reopen(&dir).await;
        let got = store.read_range(0..100).await.unwrap();
        assert_eq!(got.len(), 4);
        assert_eq!(got[2].log_id, LogId::new(2, 3));
    }

    /// Per-entry files from an earlier release fold into one segment on
    /// open, keeping the contiguous run above the purge cutoff and
    /// dropping orphans past a gap, as the old store's `log_state` did.
    #[tokio::test]
    async fn legacy_entry_files_fold_into_a_segment_on_open() {
        let dir = TempDir::new().unwrap();
        let log_dir = dir.path().join("log");
        std::fs::create_dir_all(&log_dir).unwrap();
        for i in [1, 2, 3, 4, 6] {
            let bytes = postcard::to_allocvec(&entry(1, i)).unwrap();
            std::fs::write(log_dir.join(entry_filename(i)), bytes).unwrap();
        }
        let marker = postcard::to_allocvec(&LogId::new(1, 1)).unwrap();
        std::fs::write(dir.path().join("last_purged"), marker).unwrap();
        let committed = postcard::to_allocvec(&LogId::new(1, 3)).unwrap();
        std::fs::write(dir.path().join("committed"), committed).unwrap();

        let store = reopen(&dir).await;
        let got = store.read_range(0..100).await.unwrap();
        let indices: Vec<u64> = got.iter().map(|e| e.log_id.index).collect();
        assert_eq!(indices, vec![2, 3, 4], "above the cutoff, up to the gap");
        assert_eq!(
            store.read_committed().await.unwrap(),
            Some(LogId::new(1, 3))
        );
        assert!(
            std::fs::read_dir(&log_dir)
                .unwrap()
                .flatten()
                .all(|e| e.path().extension().is_some_and(|x| x == "seg")),
            "only segments remain"
        );
        assert!(!dir.path().join("committed").exists());
        store.append(&[entry(1, 5)]).await.unwrap();
        assert_eq!(
            store.log_state().await.unwrap().last_log,
            Some(LogId::new(1, 5))
        );
    }

    /// Simulates a crash after `purge_through` wrote the marker but
    /// before the segments at or below it were removed. The orphans
    /// must be invisible to openraft: `log_state` reports `last_log`
    /// from entries strictly above the marker, and `read_range`
    /// returns only the live tail.
    #[tokio::test]
    async fn log_state_hides_orphans_below_last_purged() {
        let dir = TempDir::new().unwrap();
        let store = reopen(&dir).await;
        store
            .append(&[entry(1, 1), entry(1, 2), entry(1, 3), entry(2, 4)])
            .await
            .unwrap();
        let marker =
            postcard::to_allocvec(&LogId::new(1, 3)).expect("encode last_purged for fixture");
        atomic_write(&store.last_purged_path(), &marker)
            .await
            .expect("write last_purged fixture");
        drop(store);

        let store = reopen(&dir).await;
        let state = store.log_state().await.unwrap();
        assert_eq!(state.last_purged, Some(LogId::new(1, 3)));
        assert_eq!(
            state.last_log,
            Some(LogId::new(2, 4)),
            "last_log must come from entries strictly above last_purged"
        );
        let tail = store.read_range(0..100).await.unwrap();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].log_id, LogId::new(2, 4));
    }

    /// Edge case: a marker is in place and every entry at or below
    /// it survives (none above). `log_state` must report `last_log =
    /// None` so the openraft adapter falls back to the marker
    /// instead of advertising an orphan as the live tail.
    #[tokio::test]
    async fn log_state_returns_none_when_only_orphans_remain() {
        let dir = TempDir::new().unwrap();
        let store = reopen(&dir).await;
        store.append(&[entry(1, 1), entry(1, 2)]).await.unwrap();
        let marker =
            postcard::to_allocvec(&LogId::new(1, 5)).expect("encode last_purged for fixture");
        atomic_write(&store.last_purged_path(), &marker)
            .await
            .expect("write last_purged fixture");
        drop(store);

        let store = reopen(&dir).await;
        let state = store.log_state().await.unwrap();
        assert_eq!(state.last_purged, Some(LogId::new(1, 5)));
        assert!(
            state.last_log.is_none(),
            "orphans below the marker must not surface as last_log"
        );
    }

    /// An append that crashed mid-write leaves a torn frame at the end
    /// of the last segment. It was never acknowledged; the open cuts it
    /// off and `last_log` is the last whole frame.
    #[tokio::test]
    async fn a_torn_tail_is_cut_on_open() {
        let dir = TempDir::new().unwrap();
        let store = reopen(&dir).await;
        store
            .append(&[entry(1, 1), entry(1, 2), entry(1, 3)])
            .await
            .unwrap();
        let file = segment_files(&store).pop().unwrap();
        drop(store);
        let len = std::fs::metadata(&file).unwrap().len();
        let torn = std::fs::OpenOptions::new().write(true).open(&file).unwrap();
        torn.set_len(len - 3).unwrap();
        drop(torn);

        let store = reopen(&dir).await;
        assert_eq!(
            store.log_state().await.unwrap().last_log,
            Some(LogId::new(1, 2))
        );
        store.append(&[entry(1, 3)]).await.unwrap();
        let got = store.read_range(0..100).await.unwrap();
        assert_eq!(got.len(), 3);
    }

    /// A torn frame at the very front leaves nothing: `last_log` is
    /// `None`, with or without a purge cutoff, rather than an orphan.
    #[tokio::test]
    async fn a_torn_first_frame_leaves_an_empty_log() {
        let dir = TempDir::new().unwrap();
        let store = reopen(&dir).await;
        store.append(&[entry(1, 4), entry(1, 5)]).await.unwrap();
        let marker = postcard::to_allocvec(&LogId::new(1, 3)).expect("encode marker");
        atomic_write(&store.last_purged_path(), &marker)
            .await
            .expect("write marker");
        let file = segment_files(&store).pop().unwrap();
        drop(store);
        let mut bytes = std::fs::read(&file).unwrap();
        bytes[SEGMENT_HEADER + 5] ^= 0xff;
        std::fs::write(&file, bytes).unwrap();

        let store = reopen(&dir).await;
        let state = store.log_state().await.unwrap();
        assert_eq!(state.last_purged, Some(LogId::new(1, 3)));
        assert!(
            state.last_log.is_none(),
            "a torn anchor must yield last_log=None, got {:?}",
            state.last_log
        );
    }

    /// Damage before a later segment is not a torn append — rotation
    /// flushed that segment before the next one opened — so it may
    /// cover acknowledged entries. The open refuses instead of skipping.
    #[tokio::test]
    async fn damage_before_a_later_segment_fails_open() {
        let dir = TempDir::new().unwrap();
        let store = FsRaftLogStore::open_with_segment_bytes(dir.path().to_path_buf(), 1)
            .await
            .unwrap();
        store.append(&[entry(1, 1), entry(1, 2)]).await.unwrap();
        store.append(&[entry(1, 3)]).await.unwrap();
        let files = segment_files(&store);
        drop(store);
        let mut bytes = std::fs::read(&files[0]).unwrap();
        bytes[SEGMENT_HEADER + 5] ^= 0xff;
        std::fs::write(&files[0], bytes).unwrap();

        let err = match FsRaftLogStore::open(dir.path().to_path_buf()).await {
            Ok(_) => panic!("damaged early segment must refuse"),
            Err(err) => err,
        };
        assert!(matches!(err, StorageError::Corruption(_)), "{err:?}");
    }

    /// Truncating a segment to nothing and purging past it must not leave a
    /// header naming an old index that a later append would silently fill
    /// behind, only for the next open to read those entries as torn.
    #[tokio::test]
    async fn snapshot_after_truncation_preserves_new_entries() {
        let dir = TempDir::new().unwrap();
        let store = reopen(&dir).await;
        store
            .append(&(1..=10).map(|i| entry(1, i)).collect::<Vec<_>>())
            .await
            .unwrap();
        store.truncate_from(1).await.unwrap();
        store.purge_through(LogId::new(2, 10)).await.unwrap();
        store.append(&[entry(2, 11)]).await.unwrap();
        assert_eq!(store.read_range(11..12).await.unwrap().len(), 1);
        drop(store);
        let store = reopen(&dir).await;
        assert_eq!(store.read_range(11..12).await.unwrap().len(), 1);
        assert_eq!(
            store.log_state().await.unwrap().last_log,
            Some(LogId::new(2, 11))
        );
    }

    /// A crash while the next segment was being created leaves no published
    /// segment without a header; an empty published file from an older
    /// layout is removed rather than refusing the open.
    #[tokio::test]
    async fn a_torn_new_segment_does_not_block_recovery() {
        let dir = TempDir::new().unwrap();
        let store = reopen(&dir).await;
        store.append(&[entry(1, 1)]).await.unwrap();
        drop(store);
        let log_dir = dir.path().join("log");
        std::fs::write(log_dir.join(segment_filename(2)), []).unwrap();
        std::fs::write(log_dir.join("0000000000000003.seg.tmp"), b"partial").unwrap();
        let store = reopen(&dir).await;
        assert_eq!(
            store.log_state().await.unwrap().last_log,
            Some(LogId::new(1, 1))
        );
        store.append(&[entry(1, 2)]).await.unwrap();
        assert_eq!(store.read_range(0..100).await.unwrap().len(), 2);
        assert!(!log_dir.join("0000000000000003.seg.tmp").exists());
    }

    /// The committed watermark alternates between two slots; a torn
    /// write of the newer one falls back to the older valid value.
    #[tokio::test]
    async fn committed_falls_back_to_the_older_slot_when_the_newer_is_torn() {
        let dir = TempDir::new().unwrap();
        let store = reopen(&dir).await;
        store.save_committed(Some(LogId::new(1, 1))).await.unwrap();
        store.save_committed(Some(LogId::new(1, 2))).await.unwrap();
        drop(store);
        let path = dir.path().join(COMMITTED_FILE);
        let mut bytes = std::fs::read(&path).unwrap();
        // Generation 2 landed in slot 0; damage it.
        bytes[3] ^= 0xff;
        std::fs::write(&path, bytes).unwrap();
        let store = reopen(&dir).await;
        assert_eq!(
            store.read_committed().await.unwrap(),
            Some(LogId::new(1, 1))
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
