//! Residency-mode miss reporting and the async fetch/retry primitives.
//!
//! On native targets, the sync read path bridges a cache miss to an async CAS
//! fetch via [`run_sync_on_runtime`](super::binary_index_store::run_sync_on_runtime).
//! In **residency mode** — a content store that exposes a
//! [`MissRegister`] (always the case on `wasm32`, where no sync→async bridge
//! can exist) — the sync accessors instead serve bytes exclusively from the
//! store's resident tier and, on a miss, *report* the want and error out.
//!
//! Two reporting channels, with different guarantees (see
//! [`fluree_db_core::storage::residency`] for the full contract):
//!
//! - **The store-level [`MissRegister`] is the load-bearing channel.** Every
//!   miss records `(ContentId, FetchKind)` into it before erroring, and
//!   routed callers (the scan cursor, the batched probes) record their
//!   unit's *whole* remaining want set, so one retry round learns N objects.
//!   A retry frame reacts to any `Err` from execution by calling
//!   [`RetryBudget::after_error`]: drains the register, fetches the wants
//!   concurrently, verifies they became resident, and reports whether the
//!   failing unit should re-run. This channel survives every error
//!   conversion in the engine — including the many query-crate sites that
//!   flatten errors into strings.
//! - **The typed [`NeedFetch`] payload inside `io::Error` is best-effort.**
//!   It survives only io-preserving wrappers (`QueryError::from_io`
//!   downcasts it, mirroring the fuel-limit precedent) and makes every miss
//!   error name its CID for logs. It is NOT reliably catchable above the
//!   operator boundary — most wrappers stringify — which is exactly why the
//!   register, not the error chain, is the contract.
//!
//! Native builds without the `residency` feature compile none of the
//! residency read arms; the types and primitives here are target-agnostic
//! and always available (the browser crate and native tests drive them).

use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};

use fluree_db_core::ContentStore;

pub use fluree_db_core::storage::residency::{
    resident_or_need_fetch, FetchKind, MissRegister, NeedFetch, Want,
};

/// Default concurrent fetch width for retry rounds. Matches the browser's
/// practical per-host connection budget; callers with better knowledge
/// (HTTP/2, local stores) pass their own.
pub const DEFAULT_FETCH_WIDTH: usize = 8;

/// Sanity cap on retry rounds per failing unit. Termination comes from the
/// progress requirement (each round must pin at least one new object), not
/// from this number — the cap only bounds a store that violates the
/// fetch-pins contract in a way that still reports progress.
pub const DEFAULT_ROUND_CAP: usize = 256;

/// Outcome of fetching one drained want set.
#[derive(Debug)]
pub struct FetchOutcome {
    /// Wants attempted.
    pub wanted: usize,
    /// Wants that are resident after the round (fetch succeeded and the
    /// store upheld the fetch-pins contract).
    pub newly_resident: usize,
    /// Per-want failures: fetch errors, or a fetched object the store did
    /// not pin.
    pub failures: Vec<(Want, String)>,
}

/// Fetch `wants` into the store's resident tier, `width` at a time.
///
/// Uses [`ContentStore::get`] as the fetch-and-pin primitive (the residency
/// contract: bytes returned by `get` become resident) and verifies each want
/// via [`ContentStore::resolve_cached_bytes`] afterwards. Failures are
/// collected, not short-circuited — a partially fetched round can still be
/// progress.
pub async fn fetch_wants(cs: &dyn ContentStore, wants: Vec<Want>, width: usize) -> FetchOutcome {
    use futures::stream::StreamExt;

    let wanted = wants.len();
    let newly_resident = AtomicUsize::new(0);
    let failures: parking_lot::Mutex<Vec<(Want, String)>> = parking_lot::Mutex::new(Vec::new());

    futures::stream::iter(wants)
        .for_each_concurrent(width.max(1), |want| {
            let newly_resident = &newly_resident;
            let failures = &failures;
            async move {
                match cs.get(&want.cid).await {
                    Ok(_bytes) => {
                        if cs.resolve_cached_bytes(&want.cid).is_some() {
                            newly_resident.fetch_add(1, Ordering::Relaxed);
                        } else {
                            failures.lock().push((
                                want,
                                "store did not pin fetched bytes (fetch-pins contract violated)"
                                    .to_string(),
                            ));
                        }
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        failures.lock().push((want, msg));
                    }
                }
            }
        })
        .await;

    FetchOutcome {
        wanted,
        newly_resident: newly_resident.load(Ordering::Relaxed),
        failures: failures.into_inner(),
    }
}

/// Progress-terminated retry policy for one failing unit of work.
///
/// The drain/fetch/re-run loop every retry frame runs — an operator's async
/// frame (scan cursor), an outer query loop (fast paths, whole-query
/// backstop), or the browser driver:
///
/// ```ignore
/// let mut budget = RetryBudget::default();
/// loop {
///     match failing_unit() {
///         Ok(v) => break v,
///         Err(e) => {
///             if budget.after_error(cs, DEFAULT_FETCH_WIDTH).await? {
///                 continue; // wants fetched and pinned — re-run the unit
///             }
///             return Err(e); // not a residency miss — a real error
///         }
///     }
/// }
/// ```
///
/// Termination: `after_error` returns `Ok(true)` only when the round pinned
/// at least one new object. The resident set grows monotonically and the
/// unit's want set is finite, so the loop terminates; [`DEFAULT_ROUND_CAP`]
/// is a sanity net, not the mechanism.
#[derive(Debug)]
pub struct RetryBudget {
    rounds: usize,
    cap: usize,
    fetched_total: usize,
}

impl Default for RetryBudget {
    fn default() -> Self {
        Self::with_cap(DEFAULT_ROUND_CAP)
    }
}

impl RetryBudget {
    pub fn with_cap(cap: usize) -> Self {
        Self {
            rounds: 0,
            cap,
            fetched_total: 0,
        }
    }

    /// Rounds run so far.
    pub fn rounds(&self) -> usize {
        self.rounds
    }

    /// Total objects pinned across rounds.
    pub fn fetched_total(&self) -> usize {
        self.fetched_total
    }

    /// React to an execution error: drain the store's miss register and, if
    /// it held wants, fetch them and report whether to re-run.
    ///
    /// - `Ok(true)` — wants were fetched with progress; re-run the unit.
    /// - `Ok(false)` — the register was empty (or the store has none): the
    ///   error was not a residency miss; surface it.
    /// - `Err(_)` — wants existed but no progress is possible (every fetch
    ///   failed, the store broke the fetch-pins contract, or the sanity cap
    ///   tripped); surface this error instead.
    pub async fn after_error(&mut self, cs: &dyn ContentStore, width: usize) -> io::Result<bool> {
        let Some(register) = cs.miss_register() else {
            return Ok(false);
        };
        let wants = register.drain();
        if wants.is_empty() {
            return Ok(false);
        }

        self.rounds += 1;
        if self.rounds > self.cap {
            return Err(io::Error::other(format!(
                "residency retry round cap exceeded: rounds={}, fetched_total={}, pending_wants={} (first: {})",
                self.rounds,
                self.fetched_total,
                wants.len(),
                wants[0].cid,
            )));
        }

        // The first-recorded want is the object the caller is actually
        // blocked on — the cursor records the leaf it failed to open first,
        // and `MissRegister` preserves insertion order. Termination is gated
        // on THIS object becoming resident, not on the weaker "some want was
        // fetched": under a resident tier too small to hold a retry round,
        // re-fetching an evicted tail satisfies `newly_resident > 0` every
        // round while the caller's blocking object is evicted again before it
        // can be re-read, and the cursor never advances. Gating on the
        // blocking object makes each `Ok(true)` guarantee forward progress
        // over a finite, monotonically-advancing routed set.
        let blocking = wants[0].cid.clone();
        let outcome = fetch_wants(cs, wants, width).await;
        self.fetched_total += outcome.newly_resident;
        if cs.resolve_cached_bytes(&blocking).is_none() {
            let detail = outcome
                .failures
                .first()
                .map(|(w, e)| format!("{} {}: {}", w.kind, w.cid, e))
                .unwrap_or_else(|| {
                    "the blocking object was evicted before re-read — resident \
                     tier smaller than one retry round?"
                        .to_string()
                });
            return Err(io::Error::other(format!(
                "residency retry cannot make progress: blocking object {} not resident after \
                 fetching {} want(s) (round {}; {})",
                blocking, outcome.wanted, self.rounds, detail,
            )));
        }
        tracing::debug!(
            round = self.rounds,
            wanted = outcome.wanted,
            newly_resident = outcome.newly_resident,
            failures = outcome.failures.len(),
            "residency retry round fetched wants"
        );
        Ok(true)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::read::binary_index_store::run_sync_on_runtime;
    use async_trait::async_trait;
    use fluree_db_core::content_kind::ContentKind;
    use fluree_db_core::{ContentId, MemoryContentStore};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    /// Content store whose async side has the bytes but whose resident tier
    /// starts empty: the wasm-shaped scenario where objects exist in CAS but
    /// have not been fetched into sync-readable memory. `get` pins
    /// (fetch-pins contract); misses are recorded in the register.
    #[derive(Debug)]
    pub(crate) struct MissInjectingStore {
        pub(crate) inner: MemoryContentStore,
        resident: RwLock<HashMap<ContentId, Arc<[u8]>>>,
        register: MissRegister,
    }

    impl MissInjectingStore {
        pub(crate) fn new() -> Self {
            Self {
                inner: MemoryContentStore::new(),
                resident: RwLock::new(HashMap::new()),
                register: MissRegister::new(),
            }
        }
    }

    #[async_trait]
    impl ContentStore for MissInjectingStore {
        async fn has(&self, id: &ContentId) -> fluree_db_core::Result<bool> {
            self.inner.has(id).await
        }

        async fn get(&self, id: &ContentId) -> fluree_db_core::Result<Vec<u8>> {
            let bytes = self.inner.get(id).await?;
            // Fetch-pins contract: fetched bytes become resident.
            self.resident
                .write()
                .insert(id.clone(), Arc::from(bytes.clone().into_boxed_slice()));
            Ok(bytes)
        }

        async fn put(&self, kind: ContentKind, bytes: &[u8]) -> fluree_db_core::Result<ContentId> {
            self.inner.put(kind, bytes).await
        }

        async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> fluree_db_core::Result<()> {
            self.inner.put_with_id(id, bytes).await
        }

        async fn release(&self, id: &ContentId) -> fluree_db_core::Result<()> {
            self.inner.release(id).await
        }

        fn resolve_cached_bytes(&self, id: &ContentId) -> Option<Arc<[u8]>> {
            self.resident.read().get(id).cloned()
        }

        fn miss_register(&self) -> Option<&MissRegister> {
            Some(&self.register)
        }
    }

    /// The load-bearing round trip: miss → register carries the want → any
    /// error triggers `after_error` → fetch+pin → re-run hits. Positive
    /// assertions that the miss fired and was recorded — absence of error is
    /// not a pass.
    #[test]
    fn register_drain_fetch_retry_round_trip() {
        let store = Arc::new(MissInjectingStore::new());
        let payload = b"leaf bytes".to_vec();
        let cid = run_sync_on_runtime({
            let inner = store.inner.clone();
            let payload = payload.clone();
            async move {
                inner
                    .put(ContentKind::IndexLeaf, &payload)
                    .await
                    .map_err(|e| io::Error::other(e.to_string()))
            }
        })
        .expect("seed CAS");

        // Miss MUST fire, MUST be recorded, and MUST name its CID.
        let err = resident_or_need_fetch(store.as_ref(), &cid, FetchKind::IndexLeaf)
            .expect_err("resident tier is empty; the miss must surface");
        assert_eq!(store.register.len(), 1, "the miss must be registered");
        let nf = NeedFetch::from_io_error(&err).expect("io::Error carries a typed NeedFetch");
        assert_eq!(nf.cid, cid);
        assert_eq!(nf.kind, FetchKind::IndexLeaf);
        assert!(err.to_string().contains(&cid.to_string()));

        // The retry frame: ANY error + non-empty register → fetch and re-run.
        let should_retry = run_sync_on_runtime({
            let store = Arc::clone(&store);
            async move {
                let mut budget = RetryBudget::default();
                let r = budget.after_error(store.as_ref(), 4).await?;
                Ok((r, budget.rounds()))
            }
        })
        .expect("retry round succeeds");
        assert_eq!(should_retry, (true, 1), "wants fetched → re-run signal");

        // Re-run: hit, zero copy.
        let bytes = resident_or_need_fetch(store.as_ref(), &cid, FetchKind::IndexLeaf)
            .expect("resident after fetch");
        assert_eq!(&bytes[..], &payload[..]);
        let again = resident_or_need_fetch(store.as_ref(), &cid, FetchKind::IndexLeaf).unwrap();
        assert!(Arc::ptr_eq(&bytes, &again), "hits clone the Arc");

        // Register drained: a real error now reports no-retry.
        let no_retry = run_sync_on_runtime({
            let store = Arc::clone(&store);
            async move { RetryBudget::default().after_error(store.as_ref(), 4).await }
        })
        .unwrap();
        assert!(!no_retry, "empty register → the error was real");
    }

    /// Multi-want: several recorded misses drain and fetch as one round.
    #[test]
    fn one_round_fetches_the_whole_want_set() {
        let store = Arc::new(MissInjectingStore::new());
        let cids: Vec<ContentId> = (0..5u8)
            .map(|i| {
                run_sync_on_runtime({
                    let inner = store.inner.clone();
                    async move {
                        inner
                            .put(ContentKind::IndexLeaf, &[i; 16])
                            .await
                            .map_err(|e| io::Error::other(e.to_string()))
                    }
                })
                .unwrap()
            })
            .collect();

        for cid in &cids {
            let _ = resident_or_need_fetch(store.as_ref(), cid, FetchKind::IndexLeaf);
            // Duplicate recordings dedupe.
            let _ = resident_or_need_fetch(store.as_ref(), cid, FetchKind::IndexLeaf);
        }
        assert_eq!(store.register.len(), cids.len(), "want set deduplicated");

        let retried = run_sync_on_runtime({
            let store = Arc::clone(&store);
            async move {
                let mut budget = RetryBudget::default();
                let r = budget.after_error(store.as_ref(), 3).await?;
                Ok((r, budget.fetched_total()))
            }
        })
        .unwrap();
        assert_eq!(retried, (true, cids.len()), "one round pinned every want");
        for cid in &cids {
            assert!(store.resolve_cached_bytes(cid).is_some());
        }
    }

    /// A store that fetches but never pins is the worst case of a
    /// byte-budgeted resident tier: the blocking object is evicted the instant
    /// after it is fetched. The retry loop must fail fast on the first round
    /// rather than spin re-fetching it to the sanity cap.
    #[derive(Debug)]
    struct NonPinningStore {
        inner: MemoryContentStore,
        register: MissRegister,
    }

    #[async_trait]
    impl ContentStore for NonPinningStore {
        async fn has(&self, id: &ContentId) -> fluree_db_core::Result<bool> {
            self.inner.has(id).await
        }
        async fn get(&self, id: &ContentId) -> fluree_db_core::Result<Vec<u8>> {
            self.inner.get(id).await
        }
        async fn put(&self, kind: ContentKind, bytes: &[u8]) -> fluree_db_core::Result<ContentId> {
            self.inner.put(kind, bytes).await
        }
        async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> fluree_db_core::Result<()> {
            self.inner.put_with_id(id, bytes).await
        }
        async fn release(&self, id: &ContentId) -> fluree_db_core::Result<()> {
            self.inner.release(id).await
        }
        fn miss_register(&self) -> Option<&MissRegister> {
            Some(&self.register)
        }
    }

    #[test]
    fn no_progress_is_an_error_not_a_loop() {
        let store = Arc::new(NonPinningStore {
            inner: MemoryContentStore::new(),
            register: MissRegister::new(),
        });
        let mut budget = RetryBudget::default();
        let cid = run_sync_on_runtime({
            let inner = store.inner.clone();
            async move {
                inner
                    .put(ContentKind::IndexLeaf, b"x")
                    .await
                    .map_err(|e| io::Error::other(e.to_string()))
            }
        })
        .unwrap();
        store.register.record(&cid, FetchKind::IndexLeaf);

        let err = run_sync_on_runtime({
            let store = Arc::clone(&store);
            async move {
                budget
                    .after_error(store.as_ref(), 2)
                    .await
                    .map(|outcome| (outcome, budget.rounds()))
            }
        })
        .expect_err("no progress must surface as an error");
        // Fast-fail on round 1: the message is the progress-gate error, not
        // "round cap exceeded" — proof the loop did not spin re-fetching the
        // evicted object hundreds of times before giving up.
        assert!(
            err.to_string().contains("cannot make progress")
                && err.to_string().contains("not resident"),
            "error names the blocking object that never became resident: {err}"
        );
        assert!(
            !err.to_string().contains("round cap"),
            "must fail on the blocking-object gate, not by spinning to the cap: {err}"
        );
    }
}
