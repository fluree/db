//! Live-query driver: the advance-cycle over a set of subscriptions.
//!
//! The React SDK's engine half (design H §2 / G §4). A [`LiveQuerySet`]
//! holds per-query subscriptions; on every head change the host runs ONE
//! advance-cycle:
//!
//! 1. freeze ONE snapshot of the new head (`Fluree::db` — an immutable
//!    `GraphDb`, the engine's existing consistency primitive);
//! 2. take ONE cycle-level query guard and hold it across every re-run —
//!    the guard is a counter, so it nests with the per-query guard the
//!    production retry loop takes, and no residency eviction can undercut
//!    queries 2..N of the cycle (query 1's fetches stay resident);
//! 3. re-run every subscription of that ledger against the one view
//!    through the production query entry (which owns retry on wasm32);
//! 4. hash the formatted result bytes (xxh3, post-materialization —
//!    internal ids are unstable across index rebuilds, formatted bytes
//!    are not) and split changed/unchanged on the previous hash;
//! 5. emit ONE batch [`CycleOutcome`] — atomicity to the SDK is this
//!    single message; unchanged subscriptions cost zero payload.
//!
//! Per-subscription errors land in `errored` and never hold the barrier
//! (keep-last-good-data semantics live upstream in the SDK — this layer
//! only reports). Mid-cycle head changes coalesce: the running cycle
//! finishes (its results are a consistent view at its `t`), then exactly
//! one follow-up cycle runs at the latest head ([`Coalescer`]).
//!
//! **One concurrency regime per ledger.** Every cycle that EMITS goes
//! through the [`Coalescer`] — [`advance`](LiveQuerySet::advance) for head
//! changes and [`prime`](LiveQuerySet::prime) for a mounting subscription
//! alike — so two cycles for one ledger are never in flight at once and
//! emission order is snapshot order. That is what makes "subscribers
//! cannot disagree about which commit they are showing" hold ACROSS
//! cycles and not merely within one: a cycle that opened its snapshot at
//! an older head can never emit after a cycle that opened one at a newer
//! head, so no subscriber is pinned below its siblings and no `last_hash`
//! is left describing a superseded result. (The pure
//! [`run_cycle`](LiveQuerySet::run_cycle) family does not emit and is for
//! hosts that deliver outcomes themselves — such a host owns the
//! ordering.)
//!
//! **Cost of the cycle guard.** It is held across the WHOLE cycle by
//! design (step 2), so for the cycle's duration the resident set is pinned
//! and grows with (subscriptions x their working set) rather than with the
//! largest single query. That is the trade that keeps queries 2..N from
//! re-fetching what query 1 just made resident; it also means a page with
//! many large subscriptions on one ledger sets the residency high-water
//! mark, not the biggest one of them.
//!
//! Invalidation is v1 of the ladder: re-run everything, diff before
//! notify. The v2 footprint filter plugs into the [`FootprintFilter`]
//! seam — [`run_cycle_with_flakes`](LiveQuerySet::run_cycle_with_flakes)
//! consults it per subscription when the host hands it the cycle's
//! novelty flakes; the default filter reports everything affected, which
//! is exactly v1.

use crate::cas::BrowserCasStorage;
use fluree_db_api::{Fluree, GraphSnapshotQueryBuilder, QueryExecutionOptions};
use fluree_db_core::flake::Flake;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

/// Identifies one subscription for its lifetime.
pub type SubId = u64;

/// The subscribed query, in its author's language. Results come back in
/// the language-matched format (design H §4): SPARQL → SPARQL JSON
/// results, JSON-LD → the engine's formatted JSON-LD.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiveQuery {
    Sparql(String),
    JsonLd(JsonValue),
}

/// v2 invalidation seam: given the cycle's novelty flakes, may this
/// subscription's results have changed? `true` means re-run. The default
/// implementation reports everything affected — v1's re-run-all ladder.
///
/// Contract: a `false` may only skip work, never correctness — a
/// subscription that has never produced a result is re-run regardless of
/// the filter, and hosts that cannot supply flakes run every subscription.
pub trait FootprintFilter: Send + Sync {
    fn affected(&self, commit_flakes: &[Flake]) -> bool {
        let _ = commit_flakes;
        true
    }
}

/// The v1 filter: everything is always affected.
#[derive(Debug, Clone, Copy, Default)]
pub struct AlwaysAffected;

impl FootprintFilter for AlwaysAffected {}

/// One changed subscription in a cycle outcome.
#[derive(Debug, Clone)]
pub struct ChangedSub {
    pub sub_id: SubId,
    /// UTF-8 JSON bytes in the subscription's language-matched format —
    /// exactly what the SDK forwards to the main thread.
    pub payload: Vec<u8>,
}

/// The single batch message of one advance-cycle (H §2's worker-boundary
/// shape). The SDK applies it in one dispatch so components can never
/// observe mixed-`t` results.
#[derive(Debug, Clone)]
pub struct CycleOutcome {
    pub ledger_id: String,
    /// The frozen view's transaction watermark, or `-1` when no view
    /// could be opened (every subscription then reports in `errored`).
    pub t: i64,
    pub changed: Vec<ChangedSub>,
    pub unchanged: Vec<SubId>,
    pub errored: Vec<(SubId, String)>,
}

struct Subscription {
    ledger: String,
    query: LiveQuery,
    filter: Arc<dyn FootprintFilter>,
    last_hash: Option<u64>,
}

/// Per-cycle snapshot of one subscription's descriptors, taken so the
/// registry lock is never held across an await.
struct SelectedSub {
    sub_id: SubId,
    query: LiveQuery,
    filter: Arc<dyn FootprintFilter>,
    last_hash: Option<u64>,
}

/// Coalescing state machine for one ledger's cycles: a head change during
/// a running cycle folds into exactly ONE follow-up at the latest head
/// (novelty has already absorbed every intermediate commit).
#[derive(Default)]
struct Coalescer {
    states: Mutex<HashMap<String, CycleState>>,
}

#[derive(Default)]
struct CycleState {
    running: bool,
    pending: bool,
}

impl Coalescer {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, CycleState>> {
        self.states.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// `Some(lease)`: the caller runs the cycle now, and holds the ledger's
    /// cycle slot until the lease is finished or dropped. `None`: a cycle
    /// is already running — the signal folded into its follow-up.
    fn begin<'a>(&'a self, ledger: &'a str) -> Option<CycleLease<'a>> {
        let mut states = self.lock();
        let state = states.entry(ledger.to_string()).or_default();
        if state.running {
            state.pending = true;
            None
        } else {
            state.running = true;
            drop(states);
            Some(CycleLease {
                coalescer: self,
                ledger,
                held: true,
            })
        }
    }

    /// Report a finished cycle. `true`: signals arrived meanwhile — the
    /// caller runs exactly one more cycle (still marked running).
    fn finish(&self, ledger: &str) -> bool {
        let mut states = self.lock();
        let Some(state) = states.get_mut(ledger) else {
            return false;
        };
        if state.pending {
            state.pending = false;
            true
        } else {
            state.running = false;
            false
        }
    }

    /// Release a ledger's cycle slot without running the follow-up. Also
    /// drops any signal folded into the abandoned cycle: nothing is left to
    /// run it, and the next signal opens a fresh cycle at the latest head
    /// anyway.
    fn abandon(&self, ledger: &str) {
        if let Some(state) = self.lock().get_mut(ledger) {
            state.running = false;
            state.pending = false;
        }
    }
}

/// RAII hold on one ledger's cycle slot.
///
/// The slot is a plain flag, and every emitting cycle runs behind it — so a
/// cycle future that is DROPPED mid-await (a `select!`, an abort handle, a
/// cancelled task) would leave the ledger marked running forever: no head
/// change would ever advance it and no mounting subscription would ever
/// prime, with no error anywhere. Neither host does that today, but the
/// safety of the whole live-query path should not rest on a property of the
/// callers, so the flag is released by `Drop` rather than by discipline.
struct CycleLease<'a> {
    coalescer: &'a Coalescer,
    ledger: &'a str,
    held: bool,
}

impl CycleLease<'_> {
    /// Report a finished cycle. `true`: signals arrived meanwhile — run
    /// exactly one more, still holding the slot. `false`: the slot is
    /// released and this lease is spent.
    fn finish(&mut self) -> bool {
        if !self.held {
            return false;
        }
        if self.coalescer.finish(self.ledger) {
            true
        } else {
            self.held = false;
            false
        }
    }
}

impl Drop for CycleLease<'_> {
    fn drop(&mut self) {
        if self.held {
            self.coalescer.abandon(self.ledger);
        }
    }
}

type OutcomeCallback = Box<dyn Fn(&CycleOutcome) + Send + Sync>;

struct Inner {
    fluree: Fluree,
    /// Cycle-level guard source. `None` (tests over plain memory ledgers)
    /// skips the guard; production peers pass their storage.
    guard_source: Option<BrowserCasStorage>,
    execution: QueryExecutionOptions,
    subs: Mutex<HashMap<SubId, Subscription>>,
    next_id: AtomicU64,
    callbacks: Mutex<Vec<OutcomeCallback>>,
    coalescer: Coalescer,
}

/// The subscription registry + advance-cycle runner. Cheap to clone; all
/// clones share the registry.
#[derive(Clone)]
pub struct LiveQuerySet {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for LiveQuerySet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveQuerySet")
            .field("subscriptions", &self.len())
            .finish_non_exhaustive()
    }
}

impl LiveQuerySet {
    /// A live-query set over `fluree`. `guard_source` supplies the
    /// cycle-level query guard (the peer's storage); `None` runs cycles
    /// unguarded (fine for fully-local engines).
    pub fn new(fluree: Fluree, guard_source: Option<BrowserCasStorage>) -> Self {
        Self {
            inner: Arc::new(Inner {
                fluree,
                guard_source,
                execution: QueryExecutionOptions::default(),
                subs: Mutex::new(HashMap::new()),
                next_id: AtomicU64::new(1),
                callbacks: Mutex::new(Vec::new()),
                coalescer: Coalescer::default(),
            }),
        }
    }

    /// Like [`new`](Self::new), with execution options (memory budget,
    /// tracking) applied to every subscription run.
    pub fn with_execution_options(
        fluree: Fluree,
        guard_source: Option<BrowserCasStorage>,
        execution: QueryExecutionOptions,
    ) -> Self {
        let mut set = Self::new(fluree, guard_source);
        Arc::get_mut(&mut set.inner)
            .expect("freshly constructed")
            .execution = execution;
        set
    }

    /// Register an outcome callback. [`advance`](Self::advance) and
    /// [`prime`](Self::prime) emit through these; [`run_cycle`]
    /// (Self::run_cycle) is pure and does not. Callbacks run on the cycle
    /// task — keep them cheap (the wasm host forwards to `postMessage`).
    pub fn on_outcome(&self, callback: impl Fn(&CycleOutcome) + Send + Sync + 'static) {
        self.inner
            .callbacks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Box::new(callback));
    }

    /// Subscribe `query` against `ledger` with v1 invalidation (re-run
    /// every cycle). Returns the subscription's id.
    pub fn subscribe(&self, ledger: impl Into<String>, query: LiveQuery) -> SubId {
        self.subscribe_with_filter(ledger, query, Arc::new(AlwaysAffected))
    }

    /// Subscribe with a v2 footprint filter (see [`FootprintFilter`]).
    pub fn subscribe_with_filter(
        &self,
        ledger: impl Into<String>,
        query: LiveQuery,
        filter: Arc<dyn FootprintFilter>,
    ) -> SubId {
        let sub_id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        self.subs_lock().insert(
            sub_id,
            Subscription {
                ledger: ledger.into(),
                query,
                filter,
                last_hash: None,
            },
        );
        sub_id
    }

    /// Remove a subscription. Returns whether it existed. A subscription
    /// removed mid-cycle is dropped from that cycle's outcome.
    pub fn unsubscribe(&self, sub_id: SubId) -> bool {
        self.subs_lock().remove(&sub_id).is_some()
    }

    /// Number of live subscriptions.
    pub fn len(&self) -> usize {
        self.subs_lock().len()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.subs_lock().is_empty()
    }

    /// Whether any subscription is registered against `ledger` (exact
    /// match — normalize before asking).
    ///
    /// Hosts should gate head-change advances on this. An [`advance`]
    /// (Self::advance) for a ledger nobody subscribes to is not free: the
    /// empty-cycle branch still opens the ledger to report the head it
    /// observed, which on a browser peer is a nameservice resolution, a
    /// root index-block fetch, CID verification and an IndexedDB write. A
    /// peer tracking every ledger its token can see (the SSE default)
    /// would pay that for every commit anywhere on the server.
    pub fn has_ledger(&self, ledger: &str) -> bool {
        self.subs_lock().values().any(|sub| sub.ledger == ledger)
    }

    /// Run one advance-cycle for `ledger` at its current head and return
    /// the batch outcome WITHOUT emitting it (pure form — hosts that
    /// deliver outcomes themselves). Equivalent to
    /// [`run_cycle_with_flakes`](Self::run_cycle_with_flakes) with no
    /// flakes (every subscription re-runs).
    pub async fn run_cycle(&self, ledger: &str) -> CycleOutcome {
        self.run_cycle_with_flakes(ledger, None).await
    }

    /// [`run_cycle`](Self::run_cycle) with the cycle's novelty flakes for
    /// the v2 footprint seam: a subscription whose filter reports
    /// unaffected (and which has produced a result before) is placed in
    /// `unchanged` without re-running.
    pub async fn run_cycle_with_flakes(
        &self,
        ledger: &str,
        commit_flakes: Option<&[Flake]>,
    ) -> CycleOutcome {
        self.cycle_over(ledger, None, commit_flakes).await
    }

    /// Coalescing driver for head changes: runs a cycle now, or — when
    /// one is already running for `ledger` — folds this signal into
    /// exactly one follow-up cycle at the latest head. Emits every
    /// outcome through [`on_outcome`](Self::on_outcome) callbacks.
    pub async fn advance(&self, ledger: &str) {
        self.run_serialized(ledger, None).await;
    }

    /// Run a NEW subscription against the current head — a mounting
    /// component must not wait for the next commit.
    ///
    /// A solo prime is just a cycle with a one-element selection, so it
    /// takes the SAME coalescer every head change takes; there is no
    /// second concurrency regime. Two outcomes:
    ///
    /// - the ledger is idle → the solo cycle runs now and its outcome is
    ///   emitted and returned;
    /// - a cycle is already in flight → this signal FOLDS into that
    ///   cycle's follow-up (`None`). The subscription is already
    ///   registered, so the follow-up — a full cycle at a head no older
    ///   than this one — serves it. Nothing is lost and nothing is
    ///   delivered out of order.
    ///
    /// `None` therefore means "no solo outcome of its own": either the
    /// fold above, or `sub_id` is not registered (in which case nothing
    /// will ever deliver for it).
    pub async fn prime(&self, sub_id: SubId) -> Option<CycleOutcome> {
        let ledger = self.subs_lock().get(&sub_id).map(|s| s.ledger.clone())?;
        self.run_serialized(&ledger, Some(vec![sub_id])).await
    }

    /// The single serialized entry to emitting cycles for one ledger.
    /// `solo` restricts only the FIRST cycle (a prime's one-element
    /// selection); every follow-up is a full cycle at the latest head.
    /// Returns the first cycle's outcome when `solo` asked for one.
    ///
    /// The ledger's cycle slot is held by a [`CycleLease`] for the whole
    /// loop, so every exit — normal return, panic-unwind, or this future
    /// being dropped mid-await — releases it. Cancellation-safe by
    /// construction rather than by a property of the callers.
    async fn run_serialized(&self, ledger: &str, solo: Option<Vec<SubId>>) -> Option<CycleOutcome> {
        let mut lease = self.inner.coalescer.begin(ledger)?;
        let want_outcome = solo.is_some();
        let mut only = solo;
        let mut first: Option<CycleOutcome> = None;
        loop {
            let outcome = self.cycle_over(ledger, only.as_deref(), None).await;
            self.emit(&outcome);
            if want_outcome && first.is_none() {
                first = Some(outcome);
            }
            only = None;
            if !lease.finish() {
                return first;
            }
        }
    }

    fn subs_lock(&self) -> std::sync::MutexGuard<'_, HashMap<SubId, Subscription>> {
        self.inner
            .subs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn emit(&self, outcome: &CycleOutcome) {
        for callback in self
            .inner
            .callbacks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
        {
            callback(outcome);
        }
    }

    /// The advance-cycle core: one snapshot, one cycle guard, every
    /// selected subscription re-run against the one view, one outcome.
    async fn cycle_over(
        &self,
        ledger: &str,
        only: Option<&[SubId]>,
        commit_flakes: Option<&[Flake]>,
    ) -> CycleOutcome {
        // Snapshot descriptors without holding the lock across awaits.
        let selected: Vec<SelectedSub> = {
            let subs = self.subs_lock();
            subs.iter()
                .filter(|(id, sub)| sub.ledger == ledger && only.is_none_or(|ids| ids.contains(id)))
                .map(|(id, sub)| SelectedSub {
                    sub_id: *id,
                    query: sub.query.clone(),
                    filter: Arc::clone(&sub.filter),
                    last_hash: sub.last_hash,
                })
                .collect()
        };

        let mut outcome = CycleOutcome {
            ledger_id: ledger.to_string(),
            t: -1,
            changed: Vec::new(),
            unchanged: Vec::new(),
            errored: Vec::new(),
        };
        if selected.is_empty() {
            // An empty cycle still reports the head it observed, when it
            // can be observed at all.
            if let Ok(view) = self.inner.fluree.db(ledger).await {
                outcome.t = view.t;
            }
            return outcome;
        }

        // (1) ONE frozen snapshot for the whole cycle.
        let view = match self.inner.fluree.db(ledger).await {
            Ok(view) => view,
            Err(e) => {
                // No consistent view: every selected subscription errors;
                // the barrier itself never blocks.
                let message = e.to_string();
                outcome.errored = selected
                    .into_iter()
                    .map(|sub| (sub.sub_id, message.clone()))
                    .collect();
                return outcome;
            }
        };
        outcome.t = view.t;

        // (2) ONE cycle-level guard, held across every re-run. Nests with
        // the per-query guard the production retry loop takes (counters),
        // and keeps query 1's fetches resident for queries 2..N.
        let _cycle_guard = self
            .inner
            .guard_source
            .as_ref()
            .map(BrowserCasStorage::query_guard);

        let mut hash_updates: Vec<(SubId, u64)> = Vec::new();
        for SelectedSub {
            sub_id,
            query,
            filter,
            last_hash,
        } in selected
        {
            // v2 seam: skip provably-unaffected subscriptions — but never
            // one that has yet to produce its first result.
            if last_hash.is_some() && commit_flakes.is_some_and(|flakes| !filter.affected(flakes)) {
                outcome.unchanged.push(sub_id);
                continue;
            }

            // (3) re-run through the production entry (retry loop included
            // on wasm32) against the shared frozen view.
            let builder = GraphSnapshotQueryBuilder::new_from_parts(&self.inner.fluree, &view)
                .execution_options(self.inner.execution.clone());
            let builder = match &query {
                LiveQuery::Sparql(text) => builder.sparql(text),
                LiveQuery::JsonLd(json) => builder.jsonld(json),
            };
            match builder.execute_formatted().await {
                Ok(formatted) => {
                    let payload = match serde_json::to_vec(&formatted) {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            outcome.errored.push((sub_id, e.to_string()));
                            continue;
                        }
                    };
                    // (4) the change gate: xxh3 over the formatted bytes.
                    let hash = xxhash_rust::xxh3::xxh3_64(&payload);
                    if last_hash == Some(hash) {
                        outcome.unchanged.push(sub_id);
                    } else {
                        hash_updates.push((sub_id, hash));
                        outcome.changed.push(ChangedSub { sub_id, payload });
                    }
                }
                Err(e) => {
                    // One broken subscription never holds the barrier.
                    outcome.errored.push((sub_id, e.to_string()));
                }
            }
        }

        // Write back hashes for subscriptions still registered; anything
        // unsubscribed mid-cycle is dropped from the outcome too.
        {
            let mut subs = self.subs_lock();
            for (sub_id, hash) in &hash_updates {
                if let Some(sub) = subs.get_mut(sub_id) {
                    sub.last_hash = Some(*hash);
                }
            }
            outcome.changed.retain(|c| subs.contains_key(&c.sub_id));
            outcome.unchanged.retain(|id| subs.contains_key(id));
            outcome.errored.retain(|(id, _)| subs.contains_key(id));
        }

        // (5) snapshot and cycle guard drop here.
        outcome
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use fluree_db_api::FlureeBuilder;

    const LEDGER: &str = "live/test:main";

    async fn seeded_fluree() -> Fluree {
        let fluree = FlureeBuilder::memory().build_memory();
        fluree.create_ledger(LEDGER).await.expect("create");
        let tx = serde_json::json!({
            "@context": {"ex": "http://example.org/ns/"},
            "@graph": [
                {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice"},
                {"@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob"}
            ]
        });
        fluree
            .graph(LEDGER)
            .transact()
            .insert(&tx)
            .commit()
            .await
            .expect("seed");
        fluree
    }

    async fn add_person(fluree: &Fluree, id: &str, name: &str) {
        let tx = serde_json::json!({
            "@context": {"ex": "http://example.org/ns/"},
            "@graph": [{"@id": format!("ex:{id}"), "@type": "ex:Person", "ex:name": name}]
        });
        fluree
            .graph(LEDGER)
            .transact()
            .insert(&tx)
            .commit()
            .await
            .expect("commit");
    }

    fn names_query() -> LiveQuery {
        LiveQuery::Sparql(
            "SELECT ?name WHERE { ?s <http://example.org/ns/name> ?name } ORDER BY ?name"
                .to_string(),
        )
    }

    fn count_query() -> LiveQuery {
        LiveQuery::Sparql(
            "SELECT (COUNT(?s) AS ?n) WHERE { ?s a <http://example.org/ns/Person> }".to_string(),
        )
    }

    fn bob_query() -> LiveQuery {
        LiveQuery::Sparql(
            "SELECT ?name WHERE { <http://example.org/ns/bob> <http://example.org/ns/name> ?name }"
                .to_string(),
        )
    }

    #[tokio::test]
    async fn three_subscriptions_advance_in_one_batch_with_correct_split() {
        let fluree = seeded_fluree().await;
        let live = LiveQuerySet::new(fluree.clone(), None);

        let names = live.subscribe(LEDGER, names_query());
        let count = live.subscribe(LEDGER, count_query());
        // A subscription whose results only involve bob — unchanged by
        // later commits about other subjects.
        let bob = live.subscribe(LEDGER, bob_query());

        // Initial cycle: nothing has a hash yet — everything changes.
        let first = live.run_cycle(LEDGER).await;
        assert_eq!(first.changed.len(), 3, "{first:?}");
        assert!(first.unchanged.is_empty() && first.errored.is_empty());
        let t1 = first.t;
        assert!(t1 > 0);
        // Payloads are the language-matched formatted JSON.
        let names_payload = first
            .changed
            .iter()
            .find(|c| c.sub_id == names)
            .expect("names payload");
        let parsed: JsonValue = serde_json::from_slice(&names_payload.payload).unwrap();
        let rendered = parsed.to_string();
        assert!(
            rendered.contains("Alice") && rendered.contains("Bob"),
            "{rendered}"
        );

        // Advance the head with a commit that changes names+count but not bob.
        add_person(&fluree, "carol", "Carol").await;
        let second = live.run_cycle(LEDGER).await;
        assert!(second.t > t1, "monotone t: {} -> {}", t1, second.t);
        let changed_ids: Vec<SubId> = second.changed.iter().map(|c| c.sub_id).collect();
        assert!(changed_ids.contains(&names) && changed_ids.contains(&count));
        assert_eq!(second.unchanged, vec![bob], "bob's results are identical");
        assert!(second.errored.is_empty());

        // A cycle with no commit in between: everything unchanged, zero
        // payload bytes.
        let third = live.run_cycle(LEDGER).await;
        assert_eq!(third.t, second.t);
        assert!(third.changed.is_empty());
        assert_eq!(third.unchanged.len(), 3);
    }

    #[tokio::test]
    async fn per_subscription_errors_never_hold_the_barrier() {
        let fluree = seeded_fluree().await;
        let live = LiveQuerySet::new(fluree.clone(), None);
        let good = live.subscribe(LEDGER, names_query());
        let broken = live.subscribe(
            LEDGER,
            LiveQuery::Sparql("SELECT ?x WHERE { this is not sparql".to_string()),
        );

        let outcome = live.run_cycle(LEDGER).await;
        assert!(outcome.t > 0);
        assert_eq!(outcome.changed.len(), 1);
        assert_eq!(outcome.changed[0].sub_id, good);
        assert_eq!(outcome.errored.len(), 1);
        assert_eq!(outcome.errored[0].0, broken);
        assert!(!outcome.errored[0].1.is_empty());

        // The error repeats every cycle (keep-last-good lives upstream)
        // and still doesn't disturb the healthy subscription.
        let again = live.run_cycle(LEDGER).await;
        assert_eq!(again.unchanged, vec![good]);
        assert_eq!(again.errored.len(), 1);
    }

    #[tokio::test]
    async fn unopenable_ledger_errors_every_subscription_without_blocking() {
        let fluree = FlureeBuilder::memory().build_memory();
        let live = LiveQuerySet::new(fluree, None);
        let a = live.subscribe("missing/ledger:main", names_query());
        let b = live.subscribe("missing/ledger:main", count_query());
        let outcome = live.run_cycle("missing/ledger:main").await;
        assert_eq!(outcome.t, -1, "no consistent view");
        assert_eq!(outcome.errored.len(), 2);
        let ids: Vec<SubId> = outcome.errored.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&a) && ids.contains(&b));
    }

    #[tokio::test]
    async fn coalescer_folds_signals_into_exactly_one_followup() {
        let c = Coalescer::default();
        // Leases must be BOUND: a temporary one would drop straight away
        // and release the slot, which is the point of the guard.
        let mut lease = c.begin("l").expect("idle: run now");
        assert!(c.begin("l").is_none(), "mid-cycle: coalesced");
        assert!(
            c.begin("l").is_none(),
            "still coalesced (folds, not queues)"
        );
        assert!(lease.finish(), "one follow-up owed");
        // A signal during the follow-up cycle folds again — and earns
        // exactly one more cycle, never a queue.
        assert!(c.begin("l").is_none(), "the follow-up is the running cycle");
        assert!(lease.finish(), "that signal earns one more");
        assert!(!lease.finish(), "done");
        drop(lease);
        let mut lease = c.begin("l").expect("idle again");
        assert!(!lease.finish());
        drop(lease);
        // Ledgers coalesce independently.
        let _a = c.begin("a").expect("independent ledger");
        let _b = c.begin("b").expect("independent ledger");
    }

    /// A cycle future dropped mid-await must not strand its ledger. The
    /// slot is a plain flag behind which EVERY emitting cycle runs, so
    /// leaking it means that ledger never advances and never primes again,
    /// silently. Neither host cancels a cycle today; the guard is what
    /// keeps that from being load-bearing.
    #[tokio::test]
    async fn a_dropped_cycle_lease_releases_the_ledger() {
        let c = Coalescer::default();
        {
            let _lease = c.begin("l").expect("idle");
            assert!(c.begin("l").is_none(), "a cycle is running");
            // Scope end = the cycle future was dropped mid-await.
        }
        let mut lease = c
            .begin("l")
            .expect("a dropped lease must release the ledger, not strand it");
        assert!(!lease.finish(), "and the slot is a clean one, not a fold");
    }

    #[tokio::test]
    async fn advance_emits_batches_and_coalesces_rapid_signals() {
        let fluree = seeded_fluree().await;
        let live = LiveQuerySet::new(fluree.clone(), None);
        live.subscribe(LEDGER, names_query());

        let outcomes: Arc<Mutex<Vec<(i64, usize, usize)>>> = Arc::new(Mutex::new(Vec::new()));
        {
            let outcomes = Arc::clone(&outcomes);
            live.on_outcome(move |o| {
                outcomes
                    .lock()
                    .unwrap()
                    .push((o.t, o.changed.len(), o.unchanged.len()));
            });
        }

        live.advance(LEDGER).await;
        add_person(&fluree, "carol", "Carol").await;
        live.advance(LEDGER).await;

        let seen = outcomes.lock().unwrap().clone();
        assert_eq!(seen.len(), 2, "one batch per completed cycle: {seen:?}");
        assert!(seen[1].0 > seen[0].0, "monotone t across batches");
        assert_eq!(seen[0].1, 1, "initial cycle: changed");
        assert_eq!(seen[1].1, 1, "post-commit cycle: changed");
    }

    /// Head signals arriving while a cycle runs fold into exactly ONE
    /// follow-up cycle, observed through the public API: outcome callbacks
    /// run mid-`advance` (cycle finished, loop not yet), so signalling
    /// from one is a mid-cycle head event. `advance` never awaits before
    /// its coalesced early-return, so `block_on` is safe in the callback.
    #[tokio::test]
    async fn mid_cycle_head_signals_coalesce_into_one_followup_cycle() {
        let fluree = seeded_fluree().await;
        let live = LiveQuerySet::new(fluree.clone(), None);
        live.subscribe(LEDGER, names_query());

        let outcomes: Arc<Mutex<Vec<(i64, usize, usize)>>> = Arc::new(Mutex::new(Vec::new()));
        {
            let outcomes = Arc::clone(&outcomes);
            let live = live.clone();
            let signalled = std::sync::atomic::AtomicBool::new(false);
            live.clone().on_outcome(move |o| {
                outcomes
                    .lock()
                    .unwrap()
                    .push((o.t, o.changed.len(), o.unchanged.len()));
                if !signalled.swap(true, Ordering::SeqCst) {
                    // TWO head signals during the running cycle: both must
                    // fold into a single follow-up.
                    futures::executor::block_on(live.advance(LEDGER));
                    futures::executor::block_on(live.advance(LEDGER));
                }
            });
        }

        live.advance(LEDGER).await;
        let seen = outcomes.lock().unwrap().clone();
        assert_eq!(seen.len(), 2, "initial cycle + ONE follow-up: {seen:?}");
        assert_eq!(seen[0].0, seen[1].0, "no commit between: same t");
        assert_eq!((seen[0].1, seen[0].2), (1, 0), "first cycle: changed");
        assert_eq!((seen[1].1, seen[1].2), (0, 1), "follow-up: unchanged");
    }

    #[tokio::test]
    async fn prime_runs_a_new_subscription_solo_then_it_joins_the_barrier() {
        let fluree = seeded_fluree().await;
        let live = LiveQuerySet::new(fluree.clone(), None);
        let early = live.subscribe(LEDGER, names_query());
        let _ = live.run_cycle(LEDGER).await;

        // A component mounts later: prime runs it alone at the current
        // head — the earlier subscription is untouched.
        let late = live.subscribe(LEDGER, count_query());
        let solo = live.prime(late).await.expect("known sub");
        assert_eq!(solo.changed.len(), 1);
        assert_eq!(solo.changed[0].sub_id, late);
        assert!(solo.unchanged.is_empty() && solo.errored.is_empty());

        // Next full cycle: both are unchanged (no commit since).
        let cycle = live.run_cycle(LEDGER).await;
        assert!(cycle.changed.is_empty());
        assert_eq!(cycle.unchanged.len(), 2);
        assert!(cycle.unchanged.contains(&early) && cycle.unchanged.contains(&late));

        assert!(live.prime(9_999).await.is_none(), "unknown sub");
    }

    /// The ordering invariant everything above the driver rests on: a
    /// cycle that opened its snapshot at an OLDER head can never emit
    /// after one that opened at a newer head.
    ///
    /// `prime` used to call `cycle_over` directly while `advance` went
    /// through the coalescer — two concurrency regimes, only one of them
    /// serialized. A prime that opened its snapshot at t and was still
    /// fetching when a commit's `advance` ran and finished would emit the
    /// OLDER t last: that subscriber renders pre-commit data while its
    /// siblings sit at t+1, and its `last_hash` is left describing the
    /// superseded result, so a later commit restoring that result reports
    /// `unchanged` with no payload and the subscriber never recovers.
    ///
    /// The fix is structural — a solo prime is a cycle, so it takes the
    /// same `begin`/`finish` — and that is what this pins. The in-flight
    /// cycle is stood in for by taking the coalescer directly, which is
    /// exactly the state `advance` holds while it awaits its snapshot's
    /// queries (a real one cannot be suspended mid-await from here:
    /// nothing in the cycle body yields on a memory ledger).
    #[tokio::test]
    async fn a_prime_racing_a_running_cycle_cannot_emit_an_older_watermark() {
        let fluree = seeded_fluree().await;
        let live = LiveQuerySet::new(fluree.clone(), None);
        let early = live.subscribe(LEDGER, names_query());

        let seen: Arc<Mutex<Vec<(i64, Vec<SubId>)>>> = Arc::new(Mutex::new(Vec::new()));
        {
            let seen = Arc::clone(&seen);
            live.on_outcome(move |o| {
                seen.lock()
                    .unwrap()
                    .push((o.t, o.changed.iter().map(|c| c.sub_id).collect()));
            });
        }

        live.advance(LEDGER).await;
        let t1 = {
            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), 1, "one batch for the first cycle: {seen:?}");
            assert_eq!(seen[0].1, vec![early]);
            seen[0].0
        };

        // A cycle is now in flight for this ledger: holding its lease is
        // exactly the state `advance` is in while it awaits its snapshot's
        // queries.
        let mut in_flight = live
            .inner
            .coalescer
            .begin(LEDGER)
            .expect("the ledger is idle, so this stands in for a cycle in flight");

        // A component mounts against the pre-commit head and primes.
        let late = live.subscribe(LEDGER, count_query());
        assert!(
            live.prime(late).await.is_none(),
            "a prime racing a running cycle must FOLD into it, not open a second snapshot"
        );
        assert_eq!(
            seen.lock().unwrap().len(),
            1,
            "a folded prime emits nothing of its own: {:?}",
            seen.lock().unwrap()
        );

        // The in-flight cycle's commit lands before it finishes.
        add_person(&fluree, "carol", "Carol").await;

        // Hand-off: the fold earned exactly ONE follow-up, which is what
        // `advance`'s loop runs (cycle, emit, finish).
        assert!(
            in_flight.finish(),
            "the folded prime owes a follow-up cycle"
        );
        let follow_up = live.run_cycle(LEDGER).await;
        assert!(!in_flight.finish(), "and owes exactly one");

        // The mounting subscription's FIRST result carries the NEWER
        // watermark. The un-serialized shape delivered it at `t1`, after
        // this cycle's `t` had already reached the SDK.
        assert!(
            follow_up.t > t1,
            "the follow-up observes the newer head: {t1} -> {}",
            follow_up.t
        );
        let changed: Vec<SubId> = follow_up.changed.iter().map(|c| c.sub_id).collect();
        assert!(
            changed.contains(&late),
            "the folded prime is served by the follow-up: {follow_up:?}"
        );
        assert!(
            changed.contains(&early),
            "alongside the commit that landed: {follow_up:?}"
        );
    }

    #[tokio::test]
    async fn footprint_seam_skips_unaffected_subscriptions_but_never_unprimed_ones() {
        struct Never;
        impl FootprintFilter for Never {
            fn affected(&self, _: &[Flake]) -> bool {
                false
            }
        }

        let fluree = seeded_fluree().await;
        let live = LiveQuerySet::new(fluree.clone(), None);
        let filtered = live.subscribe_with_filter(LEDGER, names_query(), Arc::new(Never));
        let plain = live.subscribe(LEDGER, count_query());

        // First cycle WITH flakes: the filtered subscription has no result
        // yet, so the filter must not skip it.
        let first = live.run_cycle_with_flakes(LEDGER, Some(&[])).await;
        assert_eq!(
            first.changed.len(),
            2,
            "unprimed subs always run: {first:?}"
        );

        // Later cycles with flakes: the filter short-circuits the
        // filtered subscription straight to unchanged (no re-run), while
        // the unfiltered one re-runs and diffs.
        add_person(&fluree, "carol", "Carol").await;
        let second = live.run_cycle_with_flakes(LEDGER, Some(&[])).await;
        assert!(second.unchanged.contains(&filtered), "{second:?}");
        assert_eq!(second.changed.len(), 1);
        assert_eq!(second.changed[0].sub_id, plain);

        // Without flakes (v1 hosts), everything re-runs regardless.
        add_person(&fluree, "dave", "Dave").await;
        let third = live.run_cycle(LEDGER).await;
        let ids: Vec<SubId> = third.changed.iter().map(|c| c.sub_id).collect();
        assert!(ids.contains(&filtered), "v1 re-runs all: {third:?}");
    }

    /// Hosts gate head-change advances on this, so it has to track
    /// subscribe/unsubscribe exactly — a stale `true` costs a ledger open
    /// per commit on a peer, a stale `false` silently stops updating a
    /// live subscription.
    #[tokio::test]
    async fn has_ledger_tracks_subscriptions_exactly() {
        let fluree = seeded_fluree().await;
        let live = LiveQuerySet::new(fluree, None);
        assert!(!live.has_ledger(LEDGER), "nothing subscribed yet");

        let sub = live.subscribe(LEDGER, names_query());
        assert!(live.has_ledger(LEDGER));
        // Exact match: the registry never fuzzy-matches ledger strings, so
        // an un-normalized id is a MISS, not a near-hit.
        assert!(!live.has_ledger("live/test"), "unnormalized id must miss");
        assert!(!live.has_ledger("other/ledger:main"));

        assert!(live.unsubscribe(sub));
        assert!(
            !live.has_ledger(LEDGER),
            "the last unsubscribe closes the ledger to advances"
        );
    }

    #[tokio::test]
    async fn unsubscribe_drops_from_registry_and_in_flight_outcomes() {
        let fluree = seeded_fluree().await;
        let live = LiveQuerySet::new(fluree.clone(), None);
        let keep = live.subscribe(LEDGER, names_query());
        let drop_me = live.subscribe(LEDGER, count_query());
        assert_eq!(live.len(), 2);
        assert!(live.unsubscribe(drop_me));
        assert!(!live.unsubscribe(drop_me), "idempotent");
        assert_eq!(live.len(), 1);

        let outcome = live.run_cycle(LEDGER).await;
        assert_eq!(outcome.changed.len(), 1);
        assert_eq!(outcome.changed[0].sub_id, keep);
    }

    /// The cycle guard genuinely wraps the cycle: a footprint filter (which
    /// runs inside the cycle) observes the guard held on the peer storage.
    #[tokio::test]
    async fn cycle_holds_one_query_guard_across_the_whole_cycle() {
        use crate::cas::tests::{storage_with, MockState};
        use std::sync::Mutex as StdMutex;

        let state = Arc::new(StdMutex::new(MockState::default()));
        let (storage, io, driver) =
            storage_with(&state, &crate::config::BrowserIoConfig::default());

        struct GuardProbe {
            cas: BrowserCasStorage,
            observed: Arc<StdMutex<Vec<usize>>>,
        }
        impl FootprintFilter for GuardProbe {
            fn affected(&self, _: &[Flake]) -> bool {
                self.observed
                    .lock()
                    .unwrap()
                    .push(self.cas.residency().queries_in_flight());
                true
            }
        }

        // Queries run against a local memory ledger; the peer storage only
        // supplies the cycle guard — which is exactly what's under test.
        let live = LiveQuerySet::new(seeded_fluree().await, Some(storage.clone()));
        let observed = Arc::new(StdMutex::new(Vec::new()));
        let probe = Arc::new(GuardProbe {
            cas: storage.clone(),
            observed: Arc::clone(&observed),
        });
        live.subscribe_with_filter(LEDGER, names_query(), probe);

        // Prime: flakes absent, so the filter is not consulted yet.
        let first = live.run_cycle(LEDGER).await;
        assert_eq!(first.changed.len(), 1);
        assert_eq!(storage.residency().queries_in_flight(), 0);

        // Now the filter runs INSIDE the cycle and sees the cycle guard.
        let second = live.run_cycle_with_flakes(LEDGER, Some(&[])).await;
        assert_eq!(second.unchanged.len(), 1);
        assert_eq!(storage.residency().queries_in_flight(), 0, "released");
        let seen = observed.lock().unwrap().clone();
        assert_eq!(seen, vec![1], "guard held while the cycle evaluates subs");

        drop(live);
        io.shutdown();
        driver.await.unwrap();
    }
}
