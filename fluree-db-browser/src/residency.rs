//! In-memory residency tier: the bytes the synchronous read path can see.
//!
//! The binary-index read path on wasm32 cannot block on a fetch; it asks
//! the content store for already-resident bytes via
//! `resolve_cached_bytes` and surfaces a typed `NeedFetch` miss otherwise.
//! This tier is that map: CID → `Arc<[u8]>`, O(1) lookup, `Arc` clone on
//! hit, no I/O. Bytes enter it only after CID verification, so a hit is
//! trusted.
//!
//! Eviction is LRU over a byte budget. Entries pinned through a [`PinSet`]
//! are never evicted, which is what makes the fetch-and-re-run loop
//! monotone: everything a query has already pulled in stays resident until
//! the query's pin set drops. When the pinned working set alone would
//! exceed the budget, inserts fail with a typed error instead of thrashing.

use fluree_db_core::ContentId;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

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
}

/// The residency map. Cheap to share behind an `Arc`.
pub struct ResidencyTier {
    budget: usize,
    inner: Mutex<Inner>,
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
            }),
        }
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
            Self::evict_unpinned(&mut inner, need);
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

    /// Evict unpinned entries in LRU order until at least `need` bytes are
    /// freed. Callers have already checked that the unpinned set is large
    /// enough.
    fn evict_unpinned(inner: &mut Inner, need: usize) {
        let mut candidates: Vec<(u64, ContentId, usize)> = inner
            .entries
            .iter()
            .filter(|(_, e)| e.pins == 0)
            .map(|(id, e)| (e.last_use, id.clone(), e.bytes.len()))
            .collect();
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

    /// Drop an unpinned entry. Returns `false` if the entry is absent or
    /// pinned.
    pub fn remove(&self, id: &ContentId) -> bool {
        let mut inner = self.lock();
        match inner.entries.get(id) {
            Some(e) if e.pins == 0 => {
                let size = e.bytes.len();
                inner.entries.remove(id);
                inner.total -= size;
                true
            }
            _ => false,
        }
    }

    /// Drop every unpinned entry.
    pub fn clear_unpinned(&self) {
        let mut inner = self.lock();
        let keep: Vec<ContentId> = inner
            .entries
            .iter()
            .filter(|(_, e)| e.pins > 0)
            .map(|(id, _)| id.clone())
            .collect();
        let mut kept = HashMap::with_capacity(keep.len());
        for id in keep {
            if let Some(e) = inner.entries.remove(&id) {
                kept.insert(id, e);
            }
        }
        inner.entries = kept;
        inner.total = inner.pinned_total;
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
        assert!(Arc::ptr_eq(&stored, &hit), "hit must be the same allocation");
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
}
