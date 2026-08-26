//! In-memory residency tier: the bytes the synchronous read path can see.
//!
//! The binary-index read path on wasm32 cannot block on a fetch; it asks
//! the content store for already-resident bytes via
//! `resolve_cached_bytes` and surfaces a typed `NeedFetch` miss otherwise.
//! This tier is that map: CID → `Arc<[u8]>`, O(1) lookup, `Arc` clone on
//! hit, no I/O. Bytes enter it only after CID verification, so a hit is
//! trusted.
//!
//! Eviction is LRU over a byte budget, restricted by an **epoch-tick
//! rule** while queries are in flight: a hit cannot be attributed to a
//! query through the sync hook, so monotone progress — the engine's
//! fetch-pins contract: bytes made resident for a retry round must still
//! be resident on the re-run — is guaranteed structurally instead. Each
//! [`QueryGuard`] records the tier-clock tick at which it began; every
//! observation (a `resolve` hit, an `insert` including its
//! already-resident return) bumps the entry's `last_use` under the same
//! lock, so any byte a live query has touched carries a tick at or after
//! that query's begin tick. Entries whose `last_use` predates the OLDEST
//! live guard's begin tick therefore belong to no in-flight query's
//! working set and stay evictable — a budget-full tier never bricks the
//! next query's cold fetches; it sheds previous queries' leftovers. Only
//! when even those cannot make room does an insert get the typed
//! [`ResidencyError::EvictionDeferred`] (the async caller waits for a
//! release and retries). Entries pinned through a [`PinSet`] are never
//! evicted regardless, and when the pinned working set alone would exceed
//! the budget, inserts fail with
//! [`ResidencyError::WorkingSetExceedsBudget`] instead of thrashing.
//! `remove`/`clear_unpinned` obey the same epoch rule.

use fluree_db_core::ContentId;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::Notify;

/// Why bytes could not be made resident.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResidencyError {
    /// The pinned working set plus this object cannot fit; evicting
    /// unpinned entries would not help.
    #[error(
        "working set exceeds residency budget: {needed} bytes needed with {pinned} bytes pinned, budget {budget} bytes"
    )]
    WorkingSetExceedsBudget {
        needed: usize,
        pinned: usize,
        budget: usize,
    },
    /// A single object is larger than the whole budget.
    #[error("object of {size} bytes exceeds the residency budget of {budget} bytes")]
    ObjectExceedsBudget { size: usize, budget: usize },
    /// Making room would require evicting while queries are in flight,
    /// which the monotone-progress contract forbids. The caller should
    /// wait for a release (a query finishing, an entry removed) and retry.
    #[error(
        "residency eviction deferred: {in_flight} queries in flight, {needed} more bytes needed"
    )]
    EvictionDeferred { needed: usize, in_flight: usize },
}

/// Point-in-time counters for observability and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ResidencyStats {
    pub entries: usize,
    pub bytes: usize,
    pub pinned_entries: usize,
    pub pinned_bytes: usize,
    pub budget: usize,
    pub evictions: u64,
}

struct Entry {
    bytes: Arc<[u8]>,
    last_use: u64,
    pins: u32,
}

struct Inner {
    entries: HashMap<ContentId, Entry>,
    total: usize,
    pinned_total: usize,
    pinned_entries: usize,
    clock: u64,
    evictions: u64,
    /// Live query guards, keyed by their begin tick (count per tick).
    /// The smallest key is the epoch boundary: entries whose `last_use`
    /// predates it are provably unobservable by any in-flight query.
    guards: std::collections::BTreeMap<u64, usize>,
}

impl Inner {
    /// The oldest live guard's begin tick, if any query is in flight.
    fn oldest_live_begin(&self) -> Option<u64> {
        self.guards.keys().next().copied()
    }

    /// Whether the unpinned entry `e` is provably unobservable by every
    /// in-flight query — the epoch-tick invariant: every observation a
    /// query makes (a `resolve` hit, an `insert` — including the
    /// already-resident return) bumps `last_use` to a tick taken under
    /// this lock AFTER the query's guard registered its begin tick, so an
    /// entry with `last_use` older than the oldest live begin tick belongs
    /// to no live query's working set and is safe to drop without
    /// breaking monotone progress. With no query in flight everything
    /// unpinned is fair game.
    fn unobservable(&self, e: &Entry) -> bool {
        match self.oldest_live_begin() {
            Some(oldest) => e.last_use < oldest,
            None => true,
        }
    }
}

/// The residency map. Cheap to share behind an `Arc`.
pub struct ResidencyTier {
    budget: usize,
    inner: Mutex<Inner>,
    in_flight: AtomicUsize,
    /// Notified when room may have appeared: any query guard dropped
    /// (which can advance the eviction epoch), or resident bytes removed.
    released: Notify,
}

impl std::fmt::Debug for ResidencyTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.stats();
        f.debug_struct("ResidencyTier")
            .field("entries", &s.entries)
            .field("bytes", &s.bytes)
            .field("pinned_bytes", &s.pinned_bytes)
            .field("budget", &s.budget)
            .finish()
    }
}

impl ResidencyTier {
    /// A tier that keeps at most `budget_bytes` resident (pinned bytes
    /// included).
    pub fn new(budget_bytes: usize) -> Self {
        Self {
            budget: budget_bytes,
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                total: 0,
                pinned_total: 0,
                pinned_entries: 0,
                clock: 0,
                evictions: 0,
                guards: std::collections::BTreeMap::new(),
            }),
            in_flight: AtomicUsize::new(0),
            released: Notify::new(),
        }
    }

    /// Mark a query as in flight for the guard's lifetime. While any guard
    /// is alive, eviction is restricted to entries provably unobservable by
    /// every in-flight query — unpinned entries whose `last_use` predates
    /// the oldest live guard's begin tick (see [`Inner::unobservable`]).
    /// Bytes a running query has observed therefore stay resident (monotone
    /// progress without per-query pin attribution), while a cold fetch can
    /// still make room out of previous queries' leftovers.
    pub fn begin_query(self: &Arc<Self>) -> QueryGuard {
        let begin_tick = {
            let mut inner = self.lock();
            inner.clock += 1;
            let tick = inner.clock;
            *inner.guards.entry(tick).or_insert(0) += 1;
            tick
        };
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        QueryGuard {
            tier: Arc::clone(self),
            begin_tick,
        }
    }

    /// Number of queries currently in flight.
    pub fn queries_in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }

    /// Resolves on the next release event: a query guard dropping (which
    /// can advance the eviction epoch), or resident bytes being removed.
    /// Used by async callers to retry a deferred insert; pair with a
    /// deadline (a release may never come if the waiter itself holds the
    /// only guard).
    pub async fn released(&self) {
        self.released.notified().await;
    }

    /// The raw release signal, for callers that must register interest
    /// BEFORE re-checking state (`Notified::enable`) so a release landing
    /// in between cannot be missed.
    pub(crate) fn release_notify(&self) -> &Notify {
        &self.released
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Byte budget this tier was built with.
    pub fn budget(&self) -> usize {
        self.budget
    }

    /// Synchronous lookup. A hit refreshes the entry's LRU position and
    /// returns a shared clone of the bytes — no copy, no I/O.
    pub fn resolve(&self, id: &ContentId) -> Option<Arc<[u8]>> {
        let mut inner = self.lock();
        inner.clock += 1;
        let tick = inner.clock;
        let entry = inner.entries.get_mut(id)?;
        entry.last_use = tick;
        Some(Arc::clone(&entry.bytes))
    }

    /// Whether `id` is resident (does not touch LRU order).
    pub fn contains(&self, id: &ContentId) -> bool {
        self.lock().entries.contains_key(id)
    }

    /// Make verified bytes resident, evicting least-recently-used unpinned
    /// entries as needed. Returns the resident `Arc` (the existing one if
    /// the CID was already present — content is immutable, so the first
    /// copy wins and the caller's allocation is dropped).
    pub fn insert(&self, id: ContentId, bytes: Arc<[u8]>) -> Result<Arc<[u8]>, ResidencyError> {
        let size = bytes.len();
        let mut inner = self.lock();
        inner.clock += 1;
        let tick = inner.clock;

        if let Some(existing) = inner.entries.get_mut(&id) {
            existing.last_use = tick;
            return Ok(Arc::clone(&existing.bytes));
        }
        if size > self.budget {
            return Err(ResidencyError::ObjectExceedsBudget {
                size,
                budget: self.budget,
            });
        }
        if inner.pinned_total + size > self.budget {
            return Err(ResidencyError::WorkingSetExceedsBudget {
                needed: size,
                pinned: inner.pinned_total,
                budget: self.budget,
            });
        }
        if inner.total + size > self.budget {
            let need = inner.total + size - self.budget;
            if !Self::try_evict_unobservable(&mut inner, need) {
                // Not enough provably-unobservable bytes: with no query in
                // flight this is impossible (the pinned precheck above
                // guarantees the unpinned set suffices), so some live
                // query's working set is in the way — defer.
                return Err(ResidencyError::EvictionDeferred {
                    needed: size,
                    in_flight: self.in_flight.load(Ordering::Acquire),
                });
            }
        }
        inner.total += size;
        inner.entries.insert(
            id,
            Entry {
                bytes: Arc::clone(&bytes),
                last_use: tick,
                pins: 0,
            },
        );
        Ok(bytes)
    }

    /// Evict provably-unobservable entries (unpinned, `last_use` older than
    /// the oldest live guard's begin tick — everything unpinned when no
    /// query is in flight) in LRU order until at least `need` bytes are
    /// freed. Plans first: when the eligible set cannot cover `need`,
    /// nothing is evicted and `false` is returned, so a deferred insert
    /// never wastes cached bytes.
    fn try_evict_unobservable(inner: &mut Inner, need: usize) -> bool {
        let oldest = inner.oldest_live_begin();
        let mut candidates: Vec<(u64, ContentId, usize)> = inner
            .entries
            .iter()
            .filter(|(_, e)| e.pins == 0 && oldest.is_none_or(|o| e.last_use < o))
            .map(|(id, e)| (e.last_use, id.clone(), e.bytes.len()))
            .collect();
        if candidates.iter().map(|(_, _, size)| size).sum::<usize>() < need {
            return false;
        }
        candidates.sort_unstable_by_key(|(tick, _, _)| *tick);
        let mut freed = 0usize;
        for (_, id, size) in candidates {
            if freed >= need {
                break;
            }
            inner.entries.remove(&id);
            inner.total -= size;
            inner.evictions += 1;
            freed += size;
        }
        true
    }

    /// Pin a resident entry so eviction skips it. Returns `false` when the
    /// CID is not resident (nothing to pin). Pins are counted; every
    /// `pin` needs a matching [`unpin`](Self::unpin).
    pub fn pin(&self, id: &ContentId) -> bool {
        let mut inner = self.lock();
        let Some(entry) = inner.entries.get_mut(id) else {
            return false;
        };
        entry.pins += 1;
        if entry.pins == 1 {
            let size = entry.bytes.len();
            inner.pinned_total += size;
            inner.pinned_entries += 1;
        }
        true
    }

    /// Release one pin. A CID that is not pinned (or not resident) is
    /// ignored.
    pub fn unpin(&self, id: &ContentId) {
        let mut inner = self.lock();
        let Some(entry) = inner.entries.get_mut(id) else {
            return;
        };
        if entry.pins == 0 {
            return;
        }
        entry.pins -= 1;
        if entry.pins == 0 {
            let size = entry.bytes.len();
            inner.pinned_total -= size;
            inner.pinned_entries -= 1;
        }
    }

    /// Drop an unpinned entry. Returns `false` if the entry is absent,
    /// pinned, or possibly observed by an in-flight query — removal obeys
    /// the same epoch-tick rule as eviction, so a shell-side "free memory"
    /// call cannot break a running query's monotone progress.
    pub fn remove(&self, id: &ContentId) -> bool {
        let removed = {
            let mut inner = self.lock();
            match inner.entries.get(id) {
                Some(e) if e.pins == 0 && inner.unobservable(e) => {
                    let size = e.bytes.len();
                    inner.entries.remove(id);
                    inner.total -= size;
                    true
                }
                _ => false,
            }
        };
        if removed {
            self.released.notify_waiters();
        }
        removed
    }

    /// Drop every entry that is neither pinned nor possibly observed by an
    /// in-flight query (the epoch-tick rule — with no query in flight this
    /// clears everything unpinned).
    pub fn clear_unpinned(&self) {
        {
            let mut inner = self.lock();
            let oldest = inner.oldest_live_begin();
            let keep: Vec<ContentId> = inner
                .entries
                .iter()
                .filter(|(_, e)| e.pins > 0 || oldest.is_some_and(|o| e.last_use >= o))
                .map(|(id, _)| id.clone())
                .collect();
            let mut kept = HashMap::with_capacity(keep.len());
            let mut kept_total = 0usize;
            for id in keep {
                if let Some(e) = inner.entries.remove(&id) {
                    kept_total += e.bytes.len();
                    kept.insert(id, e);
                }
            }
            inner.entries = kept;
            inner.total = kept_total;
        }
        self.released.notify_waiters();
    }

    /// Current counters.
    pub fn stats(&self) -> ResidencyStats {
        let inner = self.lock();
        ResidencyStats {
            entries: inner.entries.len(),
            bytes: inner.total,
            pinned_entries: inner.pinned_entries,
            pinned_bytes: inner.pinned_total,
            budget: self.budget,
            evictions: inner.evictions,
        }
    }

    /// A query-duration pin set over this tier.
    pub fn pin_set(self: &Arc<Self>) -> PinSet {
        PinSet {
            tier: Arc::clone(self),
            pinned: Mutex::new(HashSet::new()),
        }
    }
}

/// Marks one query as in flight; dropping it releases the mark and, when it
/// was the last one, wakes waiters whose inserts were deferred.
///
/// Discipline: hold the guard across a query's sync compute AND its retry
/// rounds (the fetch-pins contract needs the fetched bytes to survive to
/// the re-run), and never wait on
/// [`ResidencyTier::released`] while holding the only guard — the deadline
/// in the caller is the safety net for that mistake.
pub struct QueryGuard {
    tier: Arc<ResidencyTier>,
    /// Tier-clock tick at [`ResidencyTier::begin_query`]: the epoch
    /// boundary below which this query can have observed nothing.
    begin_tick: u64,
}

impl std::fmt::Debug for QueryGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryGuard")
            .field("in_flight", &self.tier.queries_in_flight())
            .finish()
    }
}

impl Drop for QueryGuard {
    fn drop(&mut self) {
        {
            let mut inner = self.tier.lock();
            if let Some(count) = inner.guards.get_mut(&self.begin_tick) {
                *count -= 1;
                if *count == 0 {
                    inner.guards.remove(&self.begin_tick);
                }
            }
        }
        self.tier.in_flight.fetch_sub(1, Ordering::AcqRel);
        // Any guard dropping can advance the eviction epoch (not just the
        // last one) — wake deferred inserts to retry.
        self.tier.released.notify_waiters();
    }
}

/// Query-duration pins: every CID pinned through the set is released when
/// the set drops, so a fetch-and-re-run loop can pin as it goes and never
/// leak a pin across queries.
pub struct PinSet {
    tier: Arc<ResidencyTier>,
    pinned: Mutex<HashSet<ContentId>>,
}

impl std::fmt::Debug for PinSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinSet").field("len", &self.len()).finish()
    }
}

impl PinSet {
    /// Pin `id` for the lifetime of this set. Returns `false` when the CID
    /// is not resident. Pinning the same CID twice through one set is a
    /// no-op.
    pub fn pin(&self, id: &ContentId) -> bool {
        let mut pinned = self.pinned.lock().unwrap_or_else(PoisonError::into_inner);
        if pinned.contains(id) {
            return true;
        }
        if self.tier.pin(id) {
            pinned.insert(id.clone());
            true
        } else {
            false
        }
    }

    /// Whether `id` is pinned through this set.
    pub fn contains(&self, id: &ContentId) -> bool {
        self.pinned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(id)
    }

    /// Number of CIDs pinned through this set.
    pub fn len(&self) -> usize {
        self.pinned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Whether the set holds no pins.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Release every pin now (equivalent to dropping the set).
    pub fn release(&self) {
        let ids: Vec<ContentId> = self
            .pinned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain()
            .collect();
        for id in ids {
            self.tier.unpin(&id);
        }
    }
}

impl Drop for PinSet {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use fluree_db_core::ContentKind;

    fn blob(tag: u8, len: usize) -> (ContentId, Arc<[u8]>) {
        let bytes = vec![tag; len];
        let id = ContentId::new(ContentKind::IndexLeaf, &bytes);
        (id, Arc::from(bytes))
    }

    #[test]
    fn hit_is_a_shared_clone_and_miss_is_none() {
        let tier = ResidencyTier::new(1024);
        let (id, bytes) = blob(1, 10);
        assert!(tier.resolve(&id).is_none());
        let stored = tier.insert(id.clone(), bytes.clone()).unwrap();
        let hit = tier.resolve(&id).unwrap();
        assert!(
            Arc::ptr_eq(&stored, &hit),
            "hit must be the same allocation"
        );
        assert!(Arc::ptr_eq(&bytes, &hit));
        assert_eq!(tier.stats().bytes, 10);
    }

    #[test]
    fn reinserting_a_cid_keeps_the_first_allocation() {
        let tier = ResidencyTier::new(1024);
        let (id, first) = blob(2, 10);
        let second: Arc<[u8]> = Arc::from(vec![2u8; 10]);
        tier.insert(id.clone(), first.clone()).unwrap();
        let kept = tier.insert(id.clone(), second).unwrap();
        assert!(Arc::ptr_eq(&kept, &first));
        assert_eq!(tier.stats().entries, 1);
        assert_eq!(tier.stats().bytes, 10);
    }

    #[test]
    fn evicts_least_recently_used_unpinned_entries_to_fit() {
        let tier = ResidencyTier::new(30);
        let (a, ab) = blob(1, 10);
        let (b, bb) = blob(2, 10);
        let (c, cb) = blob(3, 10);
        tier.insert(a.clone(), ab).unwrap();
        tier.insert(b.clone(), bb).unwrap();
        tier.insert(c.clone(), cb).unwrap();
        // Touch `a` so `b` becomes the LRU victim.
        assert!(tier.resolve(&a).is_some());
        let (d, db) = blob(4, 10);
        tier.insert(d.clone(), db).unwrap();
        assert!(tier.contains(&a));
        assert!(!tier.contains(&b), "LRU entry must be evicted");
        assert!(tier.contains(&c));
        assert!(tier.contains(&d));
        assert_eq!(tier.stats().bytes, 30);
        assert_eq!(tier.stats().evictions, 1);
    }

    #[test]
    fn pinned_entries_survive_eviction_and_block_overcommit() {
        let tier = Arc::new(ResidencyTier::new(30));
        let (a, ab) = blob(1, 10);
        let (b, bb) = blob(2, 10);
        tier.insert(a.clone(), ab).unwrap();
        tier.insert(b.clone(), bb).unwrap();
        let pins = tier.pin_set();
        assert!(pins.pin(&a));
        assert!(pins.pin(&b));
        assert_eq!(tier.stats().pinned_bytes, 20);

        // Room for one more 10-byte entry (30 budget, 20 pinned).
        let (c, cb) = blob(3, 10);
        tier.insert(c.clone(), cb).unwrap();
        // A fourth cannot fit: only `c` is evictable (10 bytes) — evicting
        // it makes room, so this succeeds and `c` goes.
        let (d, db) = blob(4, 10);
        tier.insert(d.clone(), db).unwrap();
        assert!(tier.contains(&a) && tier.contains(&b));
        assert!(!tier.contains(&c));

        // Pin `d` too: now 30 bytes pinned — any further insert is a typed
        // working-set error, not an eviction of pinned bytes.
        assert!(pins.pin(&d));
        let (e, eb) = blob(5, 10);
        let err = tier.insert(e, eb).unwrap_err();
        assert_eq!(
            err,
            ResidencyError::WorkingSetExceedsBudget {
                needed: 10,
                pinned: 30,
                budget: 30
            }
        );
        assert!(tier.contains(&a) && tier.contains(&b) && tier.contains(&d));

        // Dropping the pin set releases everything; the next insert evicts
        // normally again.
        drop(pins);
        assert_eq!(tier.stats().pinned_bytes, 0);
        let (f, fb) = blob(6, 10);
        tier.insert(f.clone(), fb).unwrap();
        assert!(tier.contains(&f));
        assert_eq!(tier.stats().bytes, 30);
    }

    #[test]
    fn oversized_object_is_rejected_without_disturbing_the_tier() {
        let tier = ResidencyTier::new(16);
        let (a, ab) = blob(1, 8);
        tier.insert(a.clone(), ab).unwrap();
        let (big, bigb) = blob(9, 32);
        let err = tier.insert(big, bigb).unwrap_err();
        assert_eq!(
            err,
            ResidencyError::ObjectExceedsBudget {
                size: 32,
                budget: 16
            }
        );
        assert!(tier.contains(&a));
    }

    #[test]
    fn pin_of_non_resident_cid_is_false_and_unpin_is_idempotent() {
        let tier = Arc::new(ResidencyTier::new(64));
        let (a, ab) = blob(1, 8);
        assert!(!tier.pin(&a));
        tier.unpin(&a); // no-op
        tier.insert(a.clone(), ab).unwrap();
        assert!(tier.pin(&a));
        assert!(tier.pin(&a));
        assert_eq!(tier.stats().pinned_entries, 1);
        tier.unpin(&a);
        assert_eq!(tier.stats().pinned_entries, 1, "still one pin outstanding");
        tier.unpin(&a);
        assert_eq!(tier.stats().pinned_entries, 0);
        tier.unpin(&a); // extra unpin ignored
        assert_eq!(tier.stats().pinned_entries, 0);
        assert!(tier.remove(&a));
        assert!(!tier.contains(&a));
    }

    #[test]
    fn clear_unpinned_keeps_pinned_entries() {
        let tier = Arc::new(ResidencyTier::new(64));
        let (a, ab) = blob(1, 8);
        let (b, bb) = blob(2, 8);
        tier.insert(a.clone(), ab).unwrap();
        tier.insert(b.clone(), bb).unwrap();
        let pins = tier.pin_set();
        pins.pin(&a);
        tier.clear_unpinned();
        assert!(tier.contains(&a));
        assert!(!tier.contains(&b));
        assert_eq!(tier.stats().bytes, 8);
        assert!(!tier.remove(&a), "pinned entries cannot be removed");
    }

    /// H-1 regression (the bricking scenario): a tier filled to budget by
    /// PREVIOUS work must not stall a new query's cold fetch — entries
    /// older than the oldest live guard's begin tick are evictable.
    #[tokio::test]
    async fn cold_fetch_evicts_pre_guard_leftovers_while_in_flight() {
        let tier = Arc::new(ResidencyTier::new(20));
        let (a, ab) = blob(1, 10);
        let (b, bb) = blob(2, 10);
        let (c, cb) = blob(3, 10);
        tier.insert(a.clone(), ab).unwrap();
        tier.insert(b.clone(), bb).unwrap();
        // Tier is full to budget; a new query begins.
        let _guard = tier.begin_query();
        // Its cold fetch must succeed immediately by shedding the LRU
        // pre-guard leftover (a), not defer against the query's own guard.
        tier.insert(c.clone(), cb).unwrap();
        assert!(!tier.contains(&a), "LRU pre-guard leftover evicted");
        assert!(tier.contains(&b));
        assert!(tier.contains(&c));
        assert_eq!(tier.stats().bytes, 20);
        assert_eq!(tier.queries_in_flight(), 1);
    }

    /// Monotonicity under the epoch rule: bytes a live query has observed
    /// (resolve under its guard) are never evicted by another context's
    /// fetch; only unobserved pre-guard entries are.
    #[tokio::test]
    async fn observed_bytes_survive_another_contexts_fetch() {
        let tier = Arc::new(ResidencyTier::new(20));
        let (a, ab) = blob(1, 10);
        let (b, bb) = blob(2, 10);
        let (c, cb) = blob(3, 10);
        let (d, db) = blob(4, 10);
        tier.insert(a.clone(), ab).unwrap();
        tier.insert(b.clone(), bb).unwrap();

        let guard = tier.begin_query();
        // The live query observes `a`; `b` stays a pre-guard leftover.
        assert!(tier.resolve(&a).is_some());

        // Another fetch needs room: `b` (unobserved, pre-guard) goes;
        // `a` (observed under a live guard) survives.
        tier.insert(c.clone(), cb).unwrap();
        assert!(tier.contains(&a), "observed byte must survive");
        assert!(!tier.contains(&b), "unobserved leftover evicted");
        assert!(tier.contains(&c));

        // Now everything resident was observed or inserted at/after the
        // guard's begin tick — a further insert must defer, not evict.
        let err = tier.insert(d.clone(), Arc::clone(&db)).unwrap_err();
        assert!(
            matches!(
                err,
                ResidencyError::EvictionDeferred {
                    needed: 10,
                    in_flight: 1
                }
            ),
            "got {err:?}"
        );
        assert!(
            tier.contains(&a) && tier.contains(&c),
            "no partial eviction"
        );

        // Guard drop advances the epoch; the insert now succeeds.
        drop(guard);
        tier.insert(d.clone(), db).unwrap();
        assert!(tier.contains(&d));
        assert_eq!(tier.stats().bytes, 20);
    }

    /// remove/clear obey the epoch rule: entries a live query may have
    /// observed are refused; provably-unobservable ones (and everything,
    /// once idle) still clear.
    #[tokio::test]
    async fn remove_and_clear_respect_the_eviction_epoch() {
        let tier = Arc::new(ResidencyTier::new(64));
        let (a, ab) = blob(1, 8);
        let (b, bb) = blob(2, 8);
        tier.insert(a.clone(), ab).unwrap();
        tier.insert(b.clone(), bb).unwrap();

        let guard = tier.begin_query();
        assert!(tier.resolve(&a).is_some(), "query observes a");
        assert!(!tier.remove(&a), "observed entry refuses removal");
        assert!(tier.remove(&b), "pre-guard leftover removable");

        let (c, cb) = blob(3, 8);
        tier.insert(c.clone(), cb).unwrap();
        tier.clear_unpinned();
        assert!(
            tier.contains(&a) && tier.contains(&c),
            "clear keeps possibly-observed entries while in flight"
        );

        drop(guard);
        assert!(tier.remove(&a), "removal allowed once idle");
        tier.clear_unpinned();
        assert!(!tier.contains(&c), "clear drops everything once idle");
        assert_eq!(tier.stats().bytes, 0);
    }

    #[tokio::test]
    async fn guard_drop_wakes_released_waiters() {
        let tier = Arc::new(ResidencyTier::new(16));
        let guard = tier.begin_query();
        assert_eq!(tier.queries_in_flight(), 1);
        let waiter = {
            let tier = Arc::clone(&tier);
            tokio::spawn(async move { tier.released().await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "no release yet");
        drop(guard);
        waiter.await.unwrap();
        assert_eq!(tier.queries_in_flight(), 0);
    }
}
