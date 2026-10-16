//! Process-wide artifact read accounting, on when `FLUREE_IO_STATS` is set.
//!
//! Every artifact read records its kind, where the bytes came from and the
//! byte count. Remote fetches are counted once, at the storage bridge
//! (`store`: a whole-object get, `store-range`: a ranged get), with dictionary
//! blobs told apart by format. The bridge also counts what the storage
//! served from this machine (`local`), and the disk-cache wrapper what its
//! entries served (`cache`, `cache-range`).
//! `FLUREE_FORCE_REMOTE_READS` makes a local store report what S3 would be
//! asked for. Off, every hook is one relaxed atomic load and builds no label.
//!
//! A second table counts work done on bytes after they are read: decryption
//! (whole objects and segments) and what readers keep loaded. It is reported
//! apart from reads so neither total mixes the two. Within it, a version 2
//! envelope decrypted whole counts once as an object and again per segment:
//! do not add `decrypt v2 object` to `decrypt v2 segment`.
//!
//! Labels are artifact kinds and fixed names, never addresses, content ids,
//! plaintext or key material. On, every record takes a process-wide lock, so
//! timings taken with accounting on are not comparable to timings without
//! it. A diagnostic for investigations, not a metrics interface.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};

static ENABLED: AtomicBool = AtomicBool::new(false);
static INIT: OnceLock<()> = OnceLock::new();
/// `(kind, source)` → `(count, bytes)`.
type Table = BTreeMap<(String, &'static str), (u64, u64)>;
static TABLE: Mutex<Table> = Mutex::new(BTreeMap::new());
/// `kind` → `(count, bytes)`.
static WORK: Mutex<BTreeMap<&'static str, (u64, u64)>> = Mutex::new(BTreeMap::new());

/// Whether accounting is on (`FLUREE_IO_STATS` set to anything but `0`).
#[inline]
pub fn enabled() -> bool {
    INIT.get_or_init(|| {
        let on = std::env::var("FLUREE_IO_STATS").is_ok_and(|v| v != "0");
        ENABLED.store(on, Ordering::Relaxed);
    });
    ENABLED.load(Ordering::Relaxed)
}

/// Record one read of `bytes` bytes of a `kind()` artifact from `source`.
/// `kind` runs only when accounting is on.
pub fn record<K: Into<String>>(kind: impl FnOnce() -> K, source: &'static str, bytes: usize) {
    if !enabled() {
        return;
    }
    let mut table = TABLE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let entry = table.entry((kind().into(), source)).or_insert((0, 0));
    entry.0 += 1;
    entry.1 += bytes as u64;
}

/// Record one unit of `kind` work over `bytes` bytes, such as one segment
/// decrypted.
pub fn record_work(kind: &'static str, bytes: usize) {
    if !enabled() {
        return;
    }
    let mut work = WORK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let entry = work.entry(kind).or_insert((0, 0));
    entry.0 += 1;
    entry.1 += bytes as u64;
}

/// The reads recorded so far: `(kind, source, count, bytes)`.
pub fn reads_snapshot() -> Vec<(String, &'static str, u64, u64)> {
    TABLE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .map(|((kind, source), (n, b))| (kind.clone(), *source, *n, *b))
        .collect()
}

/// The work recorded so far: `(kind, count, bytes)`.
pub fn work_snapshot() -> Vec<(&'static str, u64, u64)> {
    WORK.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .map(|(kind, (n, b))| (*kind, *n, *b))
        .collect()
}

/// Forget everything recorded so far.
pub fn reset() {
    if enabled() {
        TABLE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        WORK.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }
}

/// The recorded reads as one line per `(kind, source)` plus a total, or
/// `None` when accounting is off or nothing was read.
pub fn report() -> Option<String> {
    if !enabled() {
        return None;
    }
    let table = TABLE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let work = WORK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if table.is_empty() && work.is_empty() {
        return None;
    }
    let mut out = String::from("io reads (kind, source, count, bytes):\n");
    let (mut count, mut bytes) = (0u64, 0u64);
    for ((kind, source), (n, b)) in table.iter() {
        out.push_str(&format!("  {kind:<10} {source:<12} {n:>8} {b:>14}\n"));
        count += n;
        bytes += b;
    }
    out.push_str(&format!(
        "  {:<10} {:<12} {count:>8} {bytes:>14}",
        "total", ""
    ));
    if !work.is_empty() {
        out.push_str("\nwork (kind, count, bytes):");
        for (kind, (n, b)) in work.iter() {
            out.push_str(&format!("\n  {kind:<23} {n:>8} {b:>14}"));
        }
    }
    Some(out)
}
