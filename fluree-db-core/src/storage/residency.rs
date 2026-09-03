//! Sync residency tier: typed misses and the store-level miss register.
//!
//! On targets without a sync→async bridge (`wasm32`), the binary-index read
//! path serves CAS-backed bytes exclusively from a store's resident tier
//! ([`ContentStore::resolve_cached_bytes`](crate::ContentStore::resolve_cached_bytes) /
//! [`StorageRead::resolve_cached_bytes`](crate::storage::StorageRead::resolve_cached_bytes)).
//! A miss cannot block on a fetch, so it must be *reported* instead. Two
//! channels exist, with different guarantees:
//!
//! - **The miss register (load-bearing).** A store that participates in
//!   residency exposes a [`MissRegister`]; every sync miss records its
//!   `(ContentId, FetchKind)` want into it before erroring. A retry frame —
//!   an operator's async frame, or an outer query loop — reacts to *any*
//!   `Err` from execution by draining the register: non-empty means the error
//!   was (or was accompanied by) a residency miss, so it fetches the wants,
//!   verifies they became resident, and re-runs the failing unit. This
//!   channel survives every error conversion in the engine, including the
//!   many sites that flatten errors to strings.
//! - **The typed [`NeedFetch`] error (best-effort).** The miss also surfaces
//!   as a `NeedFetch` payload inside the returned `io::Error`. It survives
//!   only through wrappers that preserve `io::Error` or `source()` chains —
//!   most query-engine wrappers do **not** (they format errors into
//!   strings), which is exactly why the register, not the error, is the
//!   contract. The typed error is still valuable where it does survive
//!   (`QueryError::from_io` downcasts it, mirroring the fuel-limit
//!   precedent) and in log messages, which always name the wanted CID.
//!
//! Progress and termination: content is immutable and pinned once fetched
//! (see the fetch-pins contract on `resolve_cached_bytes`), so the resident
//! set grows monotonically and the wanted set for a fixed query is finite.
//! A retry loop therefore terminates by requiring *progress* — at least one
//! drained want newly resident per round — rather than by guessing a round
//! count; a large sanity cap guards against contract violations.

use std::io;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::ContentId;

/// Which read-path family wanted the bytes. Diagnostic only — retry logic
/// needs just the [`ContentId`] (fetch the object, re-run); the kind labels
/// telemetry and error messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FetchKind {
    /// Whole index-leaf blob (scan opens, point-lookup probes, dir walks).
    IndexLeaf,
    /// Per-leaf history sidecar (time-travel replay).
    HistorySidecar,
    /// Dictionary-tree leaf (reverse lookups: IRI → id, string → id).
    DictLeaf,
    /// Forward pack (id → IRI, id → string materialization).
    ForwardPack,
    /// Vector arena shard (vector search).
    VectorShard,
}

impl std::fmt::Display for FetchKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            FetchKind::IndexLeaf => "index-leaf",
            FetchKind::HistorySidecar => "history-sidecar",
            FetchKind::DictLeaf => "dict-leaf",
            FetchKind::ForwardPack => "forward-pack",
            FetchKind::VectorShard => "vector-shard",
        };
        f.write_str(s)
    }
}

/// One wanted object: fetch `cid` into the resident tier, then re-run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Want {
    pub cid: ContentId,
    pub kind: FetchKind,
}

/// A sync read needed CAS-backed bytes that are not resident.
///
/// Carried inside `io::Error` as a custom payload. Best-effort channel only:
/// see the module docs — the [`MissRegister`] is the load-bearing carrier,
/// because most engine error wrappers flatten errors to strings.
#[derive(Debug, Clone)]
pub struct NeedFetch {
    /// The content object to fetch.
    pub cid: ContentId,
    /// Which read-path family wanted it.
    pub kind: FetchKind,
}

impl NeedFetch {
    pub fn new(cid: ContentId, kind: FetchKind) -> Self {
        Self { cid, kind }
    }

    /// Wrap into the `io::Error` channel the sync accessors return.
    pub fn into_io_error(self) -> io::Error {
        io::Error::other(self)
    }

    /// Recover a `NeedFetch` from an `io::Error`, if it carries one.
    pub fn from_io_error(err: &io::Error) -> Option<&NeedFetch> {
        err.get_ref()?.downcast_ref::<NeedFetch>()
    }

    /// Recover a `NeedFetch` from an error's `source()` chain, descending
    /// through `io::Error::get_ref` (std's `io::Error::source()` skips its
    /// custom payload — it returns the payload's source).
    ///
    /// Only works through wrappers that preserve `source()`; string-formatting
    /// wrappers — the norm in the query engine — sever it. Use the
    /// [`MissRegister`] for the reliable signal.
    pub fn find_in_chain<'a>(err: &'a (dyn std::error::Error + 'static)) -> Option<&'a NeedFetch> {
        let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(err);
        while let Some(e) = cur {
            if let Some(nf) = e.downcast_ref::<NeedFetch>() {
                return Some(nf);
            }
            cur = if let Some(io_err) = e.downcast_ref::<io::Error>() {
                io_err
                    .get_ref()
                    .map(|inner| inner as &(dyn std::error::Error + 'static))
                    .or_else(|| e.source())
            } else {
                e.source()
            };
        }
        None
    }
}

impl std::fmt::Display for NeedFetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "content not resident: {} {} (fetch and retry)",
            self.kind, self.cid
        )
    }
}

impl std::error::Error for NeedFetch {}

/// Store-level record of residency misses: the wants a retry frame drains.
///
/// Sync read paths record into it on every miss — and, where a caller knows
/// its unit's whole routed want set (a cursor's remaining leaves, a batched
/// probe's routed leaf set), they record the *entire* set so one retry round
/// fetches N objects concurrently instead of learning one per round.
///
/// Recording deduplicates by CID; [`drain`](Self::drain) hands the
/// accumulated wants to the retry frame and resets. A want whose fetch
/// failed will simply be re-recorded by the next attempt, so the register
/// never needs failure bookkeeping of its own.
///
/// Interior-mutability + `Send + Sync`: safe to share behind the store `Arc`.
///
/// ## Scope: one draining query per store at a time
///
/// The register is store-global, and `drain` hands EVERYTHING recorded to
/// the caller. Two queries concurrently missing on the same store would
/// steal each other's wants (one drains the other's recordings), turning a
/// recoverable miss into a spurious hard error for the loser. The current
/// consumers respect this: native tests run one query per store, and the
/// browser's advance-cycle serializes re-runs under a single cycle guard.
/// A future multi-query driver must either serialize its drains or scope
/// wants per query before relying on concurrent recovery.
#[derive(Debug, Default)]
pub struct MissRegister {
    inner: Mutex<RegisterInner>,
}

#[derive(Debug, Default)]
struct RegisterInner {
    /// Insertion-ordered wants; `seen` mirrors the CIDs for O(1) dedupe.
    wants: Vec<Want>,
    seen: std::collections::HashSet<ContentId>,
}

impl MissRegister {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a want. Returns `true` if it was newly recorded (not a
    /// duplicate of an undrained want).
    pub fn record(&self, cid: &ContentId, kind: FetchKind) -> bool {
        let mut inner = self.inner.lock();
        if !inner.seen.insert(cid.clone()) {
            return false;
        }
        inner.wants.push(Want {
            cid: cid.clone(),
            kind,
        });
        true
    }

    /// Take every recorded want, resetting the register.
    pub fn drain(&self) -> Vec<Want> {
        let mut inner = self.inner.lock();
        inner.seen.clear();
        std::mem::take(&mut inner.wants)
    }

    pub fn len(&self) -> usize {
        self.inner.lock().wants.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().wants.is_empty()
    }
}

/// Serve `cid` from a store's resident tier, or report the miss.
///
/// The whole sync read tier for residency-mode stores: an O(1) lookup
/// returning shared zero-copy bytes on hit; on miss, the want is recorded
/// into the store's [`MissRegister`] (when it exposes one) and a typed
/// [`NeedFetch`] error is returned. Performs no I/O and never blocks, so it
/// is safe from any sync frame on any target.
pub fn resident_or_need_fetch(
    cs: &dyn crate::ContentStore,
    cid: &ContentId,
    kind: FetchKind,
) -> io::Result<Arc<[u8]>> {
    match cs.resolve_cached_bytes(cid) {
        Some(bytes) => Ok(bytes),
        None => {
            if let Some(register) = cs.miss_register() {
                register.record(cid, kind);
            }
            Err(NeedFetch::new(cid.clone(), kind).into_io_error())
        }
    }
}
