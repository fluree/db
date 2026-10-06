//! Process-wide artifact read accounting, on when `FLUREE_IO_STATS` is set.
//!
//! Every artifact read records its kind, where the bytes came from and the
//! byte count. Remote fetches are counted once, at the storage bridge
//! (`store`: a whole-object get, `store-range`: a ranged get), with dictionary
//! blobs told apart by format; readers count what a local path (`local`,
//! `local-range`) or the disk artifact cache (`cache`, `cache-range`) served.
//! `FLUREE_FORCE_REMOTE_READS` makes a local store report what S3 would be
//! asked for. Off, every hook is one relaxed atomic load and builds no label.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};

static ENABLED: AtomicBool = AtomicBool::new(false);
static INIT: OnceLock<()> = OnceLock::new();
/// `(kind, source)` → `(count, bytes)`.
type Table = BTreeMap<(String, &'static str), (u64, u64)>;
static TABLE: Mutex<Table> = Mutex::new(BTreeMap::new());

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

/// Forget everything recorded so far.
pub fn reset() {
    if enabled() {
        TABLE
            .lock()
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
    if table.is_empty() {
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
    Some(out)
}
