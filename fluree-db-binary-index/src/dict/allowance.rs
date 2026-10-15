//! The memory forward dictionary packs may hold decrypted whole.
//!
//! Two experiment policies draw on it. First-come ([`DictionaryAllowance::hold`]):
//! a pack is held whole for its readers' lifetime if its size can be reserved
//! when it is opened. Adaptive ([`DictionaryAllowance::promote`]): the
//! allowance owns whole packs and evicts them in clock order to admit
//! others; readers hold one only for a lookup.
//!
//! Either way a pack's bytes stay charged until the last reference drops,
//! and readers of the same object — snapshots, reloads, ledgers on the same
//! storage — share one copy, charged once.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

/// Experiment control for the segmented-envelope A/B, not a supported
/// configuration: `FLUREE_DICT_ALLOWANCE_MB`, unbounded when unset.
pub(crate) fn allowance_from_env() -> u64 {
    std::env::var("FLUREE_DICT_ALLOWANCE_MB")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(u64::MAX, |mb| mb.saturating_mul(1024 * 1024))
}

/// Experiment control: `FLUREE_DICT_PACK_ALLOC=mmap` backs promoted packs
/// with anonymous mappings of their own instead of the heap.
pub(crate) fn mapped_from_env() -> bool {
    std::env::var("FLUREE_DICT_PACK_ALLOC").is_ok_and(|v| v == "mmap")
}

/// A promoted pack's bytes: on the heap, or in an anonymous mapping of its
/// own whose memory goes back to the system when it is dropped, whatever the
/// allocator would keep.
enum PackBuf {
    Heap(Box<[u8]>),
    #[cfg(not(target_arch = "wasm32"))]
    Mapped(memmap2::MmapMut),
}

impl PackBuf {
    fn zeroed(len: usize, mapped: bool) -> io::Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        if mapped && len > 0 {
            return memmap2::MmapMut::map_anon(len).map(PackBuf::Mapped);
        }
        let _ = mapped;
        Ok(PackBuf::Heap(vec![0u8; len].into_boxed_slice()))
    }

    fn as_mut(&mut self) -> &mut [u8] {
        match self {
            PackBuf::Heap(bytes) => bytes,
            #[cfg(not(target_arch = "wasm32"))]
            PackBuf::Mapped(map) => map,
        }
    }
}

impl Deref for PackBuf {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            PackBuf::Heap(bytes) => bytes,
            #[cfg(not(target_arch = "wasm32"))]
            PackBuf::Mapped(map) => map,
        }
    }
}

/// Coverage deciles: `covered` of `len` bytes as a bucket 0..=9.
fn decile(covered: u64, len: u64) -> usize {
    ((covered.saturating_mul(10)) / len.max(1)).min(9) as usize
}

/// Buckets of a count by powers of two: 0, 1, 2-3, 4-7, …
const SERVED_BUCKETS: usize = 24;

fn log2_bucket(n: u64) -> usize {
    ((u64::BITS - n.leading_zeros()) as usize).min(SERVED_BUCKETS - 1)
}

fn histogram<const N: usize>() -> [AtomicU64; N] {
    std::array::from_fn(|_| AtomicU64::new(0))
}

fn snapshot<const N: usize>(h: &[AtomicU64; N]) -> [u64; N] {
    std::array::from_fn(|i| h[i].load(Ordering::Relaxed))
}

pub struct DictionaryAllowance {
    capacity: u64,
    /// Promoted packs live in anonymous mappings rather than on the heap.
    mapped: bool,
    state: Mutex<State>,
    /// Bumped whenever the pool admits or evicts a pack, so a reader that
    /// found its pack absent knows when to look again.
    generation: AtomicU64,
    admitted: AtomicU64,
    declined: AtomicU64,
    shared: AtomicU64,
    released: AtomicU64,
    promoted: AtomicU64,
    promoted_bytes: AtomicU64,
    assembly_decrypted: AtomicU64,
    assembly_reused: AtomicU64,
    rent_covered_bytes: AtomicU64,
    /// Coverage deciles of a pack's rental when its rent first reached its
    /// size, and when it was promoted.
    crossing_coverage: [AtomicU64; 10],
    promotion_coverage: [AtomicU64; 10],
    /// Over evicted packs: lookups and value bytes served whole between
    /// promotion and eviction, and the lookups' distribution.
    evicted_served_lookups: AtomicU64,
    evicted_served_bytes: AtomicU64,
    evicted_served: [AtomicU64; SERVED_BUCKETS],
    evicted: AtomicU64,
    evicted_bytes: AtomicU64,
    busy: AtomicU64,
    no_room: AtomicU64,
    repromoted: AtomicU64,
    repromoted_bytes: AtomicU64,
}

#[derive(Default)]
struct State {
    /// Every byte charged: first-come holds, the pool's packs, packs it
    /// evicted that a reader still holds, and promotions in flight.
    used: u64,
    peak: u64,
    /// First-come holds.
    held: HashMap<u128, Weak<HeldPack>>,
    /// The pool's packs, and their clock order.
    resident: HashMap<u128, Arc<HeldPack>>,
    clock: VecDeque<u128>,
    in_flight: HashSet<u128>,
    /// Rent meters by pack, shared by every reader of it; an entry lives as
    /// long as some reader holds its meter, and dead ones are pruned
    /// whenever the table doubles.
    meters: HashMap<u128, MeterEntry>,
    meters_pruned_at: usize,
}

/// A pack's rent meter, held weakly, with what a report names it by.
struct MeterEntry {
    meter: Weak<crate::read::cached_units::MissMeter>,
    len: u64,
    kind: u8,
}

/// One live pack's rental, for diagnostics.
#[derive(Clone, Copy, Debug)]
pub struct PackReport {
    /// The pack's dictionary kind (`KIND_*_FWD`).
    pub kind: u8,
    pub len: u64,
    /// Bytes missed, and bytes of distinct units missed, this rental.
    pub rent: u64,
    pub covered: u64,
    pub promotions: u64,
    pub resident: bool,
}

impl State {
    fn charge(&mut self, len: u64) {
        self.used += len;
        self.peak = self.peak.max(self.used);
    }
}

/// What the allowance holds and has done, for diagnostics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AllowanceStats {
    pub capacity: u64,
    pub used: u64,
    pub peak: u64,
    pub packs: u64,
    pub admitted: u64,
    pub declined: u64,
    pub shared: u64,
    pub released: u64,
    pub resident_packs: u64,
    pub resident_bytes: u64,
    pub promoted: u64,
    pub promoted_bytes: u64,
    /// Bytes promotions decrypted, and bytes they copied from cached units.
    pub assembly_decrypted: u64,
    pub assembly_reused: u64,
    /// Over all promotions: bytes of distinct units the pack's ranged misses
    /// had decrypted in its rental, out of `promoted_bytes`.
    pub rent_covered_bytes: u64,
    pub crossing_coverage: [u64; 10],
    pub promotion_coverage: [u64; 10],
    pub evicted_served_lookups: u64,
    pub evicted_served_bytes: u64,
    pub evicted_served: [u64; SERVED_BUCKETS],
    pub evicted: u64,
    pub evicted_bytes: u64,
    /// Promotions turned away: another in flight, or no room yet.
    pub busy: u64,
    pub no_room: u64,
    /// Promotions of a pack promoted before, and so evicted since.
    pub repromoted: u64,
    pub repromoted_bytes: u64,
    /// Bytes of evicted packs a reader still holds.
    pub outstanding: u64,
}

/// A decrypted pack held within the allowance; dropping the last reference
/// returns its bytes to it.
pub struct HeldPack {
    bytes: PackBuf,
    identity: u128,
    allowance: Weak<DictionaryAllowance>,
    /// Clock reference bit: set on use, cleared as the hand passes.
    referenced: AtomicBool,
    /// Set when the pool lets the pack go, so a reader drops it after its
    /// lookup instead of keeping it alive.
    evicted: AtomicBool,
    served_lookups: AtomicU64,
    served_bytes: AtomicU64,
}

impl HeldPack {
    #[cfg(test)]
    pub(crate) fn is_mapped(&self) -> bool {
        #[cfg(not(target_arch = "wasm32"))]
        return matches!(self.bytes, PackBuf::Mapped(_));
        #[cfg(target_arch = "wasm32")]
        false
    }

    /// Lookups served whole so far, and their value bytes.
    #[cfg(test)]
    pub(crate) fn served(&self) -> (u64, u64) {
        (
            self.served_lookups.load(Ordering::Relaxed),
            self.served_bytes.load(Ordering::Relaxed),
        )
    }

    /// Count a lookup served from the whole pack, of `bytes` value bytes.
    pub(crate) fn note_served(&self, bytes: usize) {
        self.served_lookups.fetch_add(1, Ordering::Relaxed);
        self.served_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn mark_used(&self) {
        if !self.referenced.load(Ordering::Relaxed) {
            self.referenced.store(true, Ordering::Relaxed);
        }
    }

    pub(crate) fn is_evicted(&self) -> bool {
        self.evicted.load(Ordering::Acquire)
    }
}

/// The outcome of [`DictionaryAllowance::promote`].
pub enum Promotion {
    /// Assembled and admitted by this call.
    Promoted(Arc<HeldPack>),
    /// Already in the pool.
    Resident(Arc<HeldPack>),
    /// Another promotion of it is in flight.
    Busy,
    /// Not enough room yet: what eviction freed is still held by readers.
    NoRoom,
    /// Larger than the whole allowance.
    TooLarge,
}

impl Deref for HeldPack {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for HeldPack {
    fn drop(&mut self) {
        let Some(allowance) = self.allowance.upgrade() else {
            return;
        };
        let mut state = allowance.lock();
        state.used -= self.bytes.len() as u64;
        // A later hold of the same object may already have replaced this
        // entry; only remove our own.
        if state
            .held
            .get(&self.identity)
            .is_some_and(|w| std::ptr::eq(w.as_ptr(), self))
        {
            state.held.remove(&self.identity);
        }
        allowance.released.fetch_add(1, Ordering::Relaxed);
    }
}

impl DictionaryAllowance {
    pub fn new(capacity: u64) -> Arc<Self> {
        Self::with_backing(capacity, false)
    }

    /// An allowance whose promoted packs live in anonymous mappings when
    /// `mapped`, on the heap otherwise.
    pub fn with_backing(capacity: u64, mapped: bool) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            mapped,
            state: Mutex::default(),
            generation: AtomicU64::new(0),
            admitted: AtomicU64::new(0),
            declined: AtomicU64::new(0),
            shared: AtomicU64::new(0),
            released: AtomicU64::new(0),
            promoted: AtomicU64::new(0),
            promoted_bytes: AtomicU64::new(0),
            assembly_decrypted: AtomicU64::new(0),
            assembly_reused: AtomicU64::new(0),
            rent_covered_bytes: AtomicU64::new(0),
            crossing_coverage: histogram(),
            promotion_coverage: histogram(),
            evicted_served_lookups: AtomicU64::new(0),
            evicted_served_bytes: AtomicU64::new(0),
            evicted_served: histogram(),
            evicted: AtomicU64::new(0),
            evicted_bytes: AtomicU64::new(0),
            busy: AtomicU64::new(0),
            no_room: AtomicU64::new(0),
            repromoted: AtomicU64::new(0),
            repromoted_bytes: AtomicU64::new(0),
        })
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// The rent meter of the pack `identity` — `len` bytes in `units`
    /// units, of dictionary `kind` — shared by all its readers.
    pub fn meter(
        &self,
        identity: u128,
        units: u64,
        len: u64,
        kind: u8,
    ) -> Arc<crate::read::cached_units::MissMeter> {
        let mut state = self.lock();
        if let Some(meter) = state.meters.get(&identity).and_then(|e| e.meter.upgrade()) {
            return meter;
        }
        if state.meters.len() >= 2 * state.meters_pruned_at.max(32) {
            state.meters.retain(|_, e| e.meter.strong_count() > 0);
            state.meters_pruned_at = state.meters.len();
        }
        let meter = Arc::new(crate::read::cached_units::MissMeter::new(units));
        state.meters.insert(
            identity,
            MeterEntry {
                meter: Arc::downgrade(&meter),
                len,
                kind,
            },
        );
        meter
    }

    /// Every live pack's rental and whether the pool holds it.
    pub fn pack_report(&self) -> Vec<PackReport> {
        let state = self.lock();
        state
            .meters
            .iter()
            .filter_map(|(identity, e)| {
                let meter = e.meter.upgrade()?;
                Some(PackReport {
                    kind: e.kind,
                    len: e.len,
                    rent: meter.bytes(),
                    covered: meter.covered(),
                    promotions: meter.promotions(),
                    resident: state.resident.contains_key(identity),
                })
            })
            .collect()
    }

    /// Record a pack's rent first reaching its size `len` in a rental, with
    /// `covered` bytes of distinct units decrypted by then.
    pub fn note_rent_reached(&self, covered: u64, len: u64) {
        self.crossing_coverage[decile(covered, len)].fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a pack promoted before, `len` bytes, was promoted again.
    pub fn note_repromotion(&self, len: u64) {
        self.repromoted.fetch_add(1, Ordering::Relaxed);
        self.repromoted_bytes.fetch_add(len, Ordering::Relaxed);
    }

    /// The pool's copy of `identity`, if it holds one.
    pub fn resident(&self, identity: u128) -> Option<Arc<HeldPack>> {
        self.lock().resident.get(&identity).cloned()
    }

    /// Admit `identity`, `len` bytes, to the pool: evict packs in clock order
    /// until it fits, reserve its bytes, then `assemble` it into its backing,
    /// which returns how many bytes it decrypted rather than copied. `covered`
    /// is the bytes of distinct units its rental decrypted, for the record.
    pub fn promote(
        self: &Arc<Self>,
        identity: u128,
        len: u64,
        covered: u64,
        assemble: impl FnOnce(&mut [u8]) -> io::Result<u64>,
    ) -> io::Result<Promotion> {
        let victims = {
            let mut state = self.lock();
            if let Some(pack) = state.resident.get(&identity) {
                return Ok(Promotion::Resident(Arc::clone(pack)));
            }
            if len > self.capacity {
                return Ok(Promotion::TooLarge);
            }
            if state.in_flight.contains(&identity) {
                self.busy.fetch_add(1, Ordering::Relaxed);
                return Ok(Promotion::Busy);
            }
            self.evict_for(&mut state, len)
        };
        // Dropped outside the lock: a victim no reader holds returns its
        // bytes now, which takes the lock.
        drop(victims);
        {
            let mut state = self.lock();
            if let Some(pack) = state.resident.get(&identity) {
                return Ok(Promotion::Resident(Arc::clone(pack)));
            }
            if state.in_flight.contains(&identity) {
                self.busy.fetch_add(1, Ordering::Relaxed);
                return Ok(Promotion::Busy);
            }
            if state.used.saturating_add(len) > self.capacity {
                self.no_room.fetch_add(1, Ordering::Relaxed);
                return Ok(Promotion::NoRoom);
            }
            state.charge(len);
            state.in_flight.insert(identity);
        }
        let assembled = PackBuf::zeroed(len as usize, self.mapped)
            .and_then(|mut buf| assemble(buf.as_mut()).map(|decrypted| (buf, decrypted)));
        let mut state = self.lock();
        state.in_flight.remove(&identity);
        let (bytes, decrypted) = match assembled {
            Ok(a) => a,
            Err(e) => {
                state.used -= len;
                return Err(e);
            }
        };
        let pack = Arc::new(HeldPack {
            bytes,
            identity,
            allowance: Arc::downgrade(self),
            referenced: AtomicBool::new(false),
            evicted: AtomicBool::new(false),
            served_lookups: AtomicU64::new(0),
            served_bytes: AtomicU64::new(0),
        });
        state.resident.insert(identity, Arc::clone(&pack));
        state.clock.push_back(identity);
        drop(state);
        self.promoted.fetch_add(1, Ordering::Relaxed);
        self.promoted_bytes.fetch_add(len, Ordering::Relaxed);
        self.assembly_decrypted
            .fetch_add(decrypted, Ordering::Relaxed);
        self.assembly_reused
            .fetch_add(len - decrypted, Ordering::Relaxed);
        self.rent_covered_bytes
            .fetch_add(covered, Ordering::Relaxed);
        self.promotion_coverage[decile(covered, len)].fetch_add(1, Ordering::Relaxed);
        self.generation.fetch_add(1, Ordering::AcqRel);
        Ok(Promotion::Promoted(pack))
    }

    /// Take packs out of the pool in clock order until `len` more bytes
    /// would fit, giving each referenced pack a second chance. The caller
    /// drops the returned packs once the lock is released.
    fn evict_for(&self, state: &mut State, len: u64) -> Vec<Arc<HeldPack>> {
        let mut need = (state.used + len).saturating_sub(self.capacity);
        let mut victims = Vec::new();
        let mut turns = 2 * state.clock.len();
        while need > 0 && turns > 0 {
            turns -= 1;
            let Some(identity) = state.clock.pop_front() else {
                break;
            };
            let Some(pack) = state.resident.get(&identity) else {
                continue;
            };
            if pack.referenced.swap(false, Ordering::Relaxed) {
                state.clock.push_back(identity);
                continue;
            }
            let pack = state.resident.remove(&identity).expect("present");
            pack.evicted.store(true, Ordering::Release);
            let served = pack.served_lookups.load(Ordering::Relaxed);
            self.evicted_served_lookups
                .fetch_add(served, Ordering::Relaxed);
            self.evicted_served_bytes
                .fetch_add(pack.served_bytes.load(Ordering::Relaxed), Ordering::Relaxed);
            self.evicted_served[log2_bucket(served)].fetch_add(1, Ordering::Relaxed);
            need = need.saturating_sub(pack.len() as u64);
            self.evicted.fetch_add(1, Ordering::Relaxed);
            self.evicted_bytes
                .fetch_add(pack.len() as u64, Ordering::Relaxed);
            victims.push(pack);
        }
        if !victims.is_empty() {
            self.generation.fetch_add(1, Ordering::AcqRel);
        }
        victims
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The object `identity` of `len` bytes held whole: the copy another
    /// reader already holds, or `load`ed within the allowance. `None` when
    /// it does not fit beside what is held.
    pub fn hold(
        self: &Arc<Self>,
        identity: u128,
        len: u64,
        load: impl FnOnce() -> io::Result<Vec<u8>>,
    ) -> io::Result<Option<Arc<HeldPack>>> {
        {
            let mut state = self.lock();
            if let Some(pack) = state.held.get(&identity).and_then(Weak::upgrade) {
                self.shared.fetch_add(1, Ordering::Relaxed);
                return Ok(Some(pack));
            }
            if state.used.saturating_add(len) > self.capacity {
                self.declined.fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
            state.charge(len);
        }
        let unreserve = || self.lock().used -= len;
        let bytes = match load() {
            Ok(bytes) if bytes.len() as u64 == len => bytes,
            Ok(_) => {
                unreserve();
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "held pack: length differs from its source",
                ));
            }
            Err(e) => {
                unreserve();
                return Err(e);
            }
        };
        let mut state = self.lock();
        // Another reader loaded the same object meanwhile: share theirs.
        if let Some(pack) = state.held.get(&identity).and_then(Weak::upgrade) {
            state.used -= len;
            self.shared.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(pack));
        }
        let pack = Arc::new(HeldPack {
            bytes: PackBuf::Heap(bytes.into_boxed_slice()),
            identity,
            allowance: Arc::downgrade(self),
            referenced: AtomicBool::new(false),
            evicted: AtomicBool::new(false),
            served_lookups: AtomicU64::new(0),
            served_bytes: AtomicU64::new(0),
        });
        state.held.insert(identity, Arc::downgrade(&pack));
        self.admitted.fetch_add(1, Ordering::Relaxed);
        Ok(Some(pack))
    }

    pub fn stats(&self) -> AllowanceStats {
        let state = self.lock();
        let load = |c: &AtomicU64| c.load(Ordering::Relaxed);
        AllowanceStats {
            capacity: self.capacity,
            used: state.used,
            peak: state.peak,
            packs: state.held.values().filter(|w| w.strong_count() > 0).count() as u64,
            admitted: load(&self.admitted),
            declined: load(&self.declined),
            shared: load(&self.shared),
            released: load(&self.released),
            resident_packs: state.resident.len() as u64,
            resident_bytes: state.resident.values().map(|p| p.len() as u64).sum(),
            promoted: load(&self.promoted),
            promoted_bytes: load(&self.promoted_bytes),
            assembly_decrypted: load(&self.assembly_decrypted),
            assembly_reused: load(&self.assembly_reused),
            rent_covered_bytes: load(&self.rent_covered_bytes),
            crossing_coverage: snapshot(&self.crossing_coverage),
            promotion_coverage: snapshot(&self.promotion_coverage),
            evicted_served_lookups: load(&self.evicted_served_lookups),
            evicted_served_bytes: load(&self.evicted_served_bytes),
            evicted_served: snapshot(&self.evicted_served),
            evicted: load(&self.evicted),
            evicted_bytes: load(&self.evicted_bytes),
            busy: load(&self.busy),
            no_room: load(&self.no_room),
            repromoted: load(&self.repromoted),
            repromoted_bytes: load(&self.repromoted_bytes),
            outstanding: state.used.saturating_sub(
                state.resident.values().map(|p| p.len() as u64).sum::<u64>()
                    + state
                        .held
                        .values()
                        .filter_map(Weak::upgrade)
                        .map(|p| p.len() as u64)
                        .sum::<u64>(),
            ),
        }
    }
}

impl std::fmt::Debug for DictionaryAllowance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DictionaryAllowance")
            .field("stats", &self.stats())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(len: usize) -> io::Result<Vec<u8>> {
        Ok(vec![7; len])
    }

    #[test]
    fn holds_within_capacity_and_declines_beyond() {
        let allowance = DictionaryAllowance::new(100);
        let a = allowance.hold(1, 60, || bytes(60)).unwrap().unwrap();
        assert_eq!(&a[..3], [7, 7, 7]);
        assert!(allowance
            .hold(2, 50, || panic!("must not load"))
            .unwrap()
            .is_none());
        let b = allowance.hold(3, 40, || bytes(40)).unwrap();
        assert!(b.is_some(), "exactly full is within");
        let stats = allowance.stats();
        assert_eq!((stats.used, stats.packs), (100, 2));
        assert_eq!((stats.admitted, stats.declined), (2, 1));
    }

    /// The reservation stays charged while any holder lives, and is returned
    /// by the last.
    #[test]
    fn the_last_holder_returns_the_reservation() {
        let allowance = DictionaryAllowance::new(100);
        let a = allowance.hold(1, 80, || bytes(80)).unwrap().unwrap();
        let again = allowance
            .hold(1, 80, || panic!("shared, not loaded"))
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&a, &again));
        assert_eq!(allowance.stats().used, 80, "charged once");
        drop(a);
        assert_eq!(allowance.stats().used, 80, "still held");
        assert!(allowance.hold(2, 40, || bytes(40)).unwrap().is_none());
        drop(again);
        let stats = allowance.stats();
        assert_eq!((stats.used, stats.packs, stats.released), (0, 0, 1));
        assert!(allowance.hold(2, 40, || bytes(40)).unwrap().is_some());
    }

    #[test]
    fn a_failed_or_short_load_returns_its_reservation() {
        let allowance = DictionaryAllowance::new(100);
        assert!(allowance
            .hold(1, 60, || Err(io::Error::other("auth")))
            .is_err());
        assert!(allowance.hold(1, 60, || bytes(59)).is_err());
        assert_eq!(allowance.stats().used, 0);
        assert!(allowance.hold(1, 60, || bytes(60)).unwrap().is_some());
    }

    /// Fill a promoted pack's backing; half of it counts as decrypted.
    fn fill(buf: &mut [u8]) -> io::Result<u64> {
        buf.fill(9);
        Ok(buf.len() as u64 / 2)
    }

    fn promoted(p: Promotion) -> Arc<HeldPack> {
        match p {
            Promotion::Promoted(pack) => pack,
            _ => panic!("expected a promotion"),
        }
    }

    /// A promotion that needs room evicts in clock order, giving a pack used
    /// since the hand last passed a second chance.
    #[test]
    fn promotion_evicts_the_least_recently_used_in_clock_order() {
        let allowance = DictionaryAllowance::new(100);
        drop(promoted(allowance.promote(1, 40, 30, fill).unwrap()));
        drop(promoted(allowance.promote(2, 40, 30, fill).unwrap()));
        allowance.resident(1).unwrap().mark_used();
        // 1 is first in clock order but was used: the hand clears it and
        // evicts 2.
        drop(promoted(allowance.promote(3, 40, 30, fill).unwrap()));
        assert!(allowance.resident(1).is_some() && allowance.resident(2).is_none());
        // 1's second chance is spent: next it goes.
        drop(promoted(allowance.promote(4, 40, 30, fill).unwrap()));
        assert!(allowance.resident(1).is_none());
        assert!(allowance.resident(3).is_some() && allowance.resident(4).is_some());
        let stats = allowance.stats();
        assert_eq!((stats.promoted, stats.evicted, stats.used), (4, 2, 80));
        assert_eq!((stats.resident_packs, stats.resident_bytes), (2, 80));
        assert_eq!((stats.assembly_decrypted, stats.assembly_reused), (80, 80));
        assert_eq!(stats.rent_covered_bytes, 120);
        assert_eq!(stats.promotion_coverage[7], 4, "30 of 40 bytes covered");
    }

    /// An evicted pack a reader still holds stays charged: a promotion that
    /// needs its bytes waits until the reader drops it.
    #[test]
    fn an_evicted_pack_stays_charged_while_a_reader_holds_it() {
        let allowance = DictionaryAllowance::new(100);
        drop(promoted(allowance.promote(1, 60, 45, fill).unwrap()));
        let reader = allowance.resident(1).unwrap();
        assert!(matches!(
            allowance
                .promote(2, 60, 0, |_| panic!("no room to assemble"))
                .unwrap(),
            Promotion::NoRoom
        ));
        assert!(reader.is_evicted() && allowance.resident(1).is_none());
        let stats = allowance.stats();
        assert_eq!((stats.used, stats.outstanding), (60, 60), "still charged");
        drop(reader);
        let stats = allowance.stats();
        assert_eq!((stats.used, stats.outstanding), (0, 0));
        promoted(allowance.promote(2, 60, 45, fill).unwrap());
        assert_eq!(allowance.stats().peak, 60);
    }

    /// A promotion already in flight turns another away; one already done
    /// is shared; one too large for the allowance is refused.
    /// Readers of one pack share a meter; the table forgets packs no reader
    /// holds a meter for, so it does not grow with retired packs.
    #[test]
    fn meters_are_shared_per_pack_and_pruned() {
        let allowance = DictionaryAllowance::new(100);
        let a = allowance.meter(1, 4, 0, 0);
        assert!(Arc::ptr_eq(&a, &allowance.meter(1, 4, 0, 0)));
        for identity in 2..1_000u128 {
            drop(allowance.meter(identity, 4, 0, 0));
        }
        let live = allowance.lock().meters.len();
        assert!(live <= 64, "{live} entries");
        assert!(
            Arc::ptr_eq(&a, &allowance.meter(1, 4, 0, 0)),
            "a held meter survives"
        );
    }

    #[test]
    fn promotions_are_deduplicated_and_bounded() {
        let allowance = DictionaryAllowance::new(100);
        let inner = Arc::clone(&allowance);
        let pack = promoted(
            allowance
                .promote(1, 50, 0, |buf| {
                    assert!(matches!(
                        inner.promote(1, 50, 0, |_| panic!("dup")).unwrap(),
                        Promotion::Busy
                    ));
                    fill(buf)
                })
                .unwrap(),
        );
        assert!(matches!(
            allowance.promote(1, 50, 0, |_| panic!("resident")).unwrap(),
            Promotion::Resident(p) if Arc::ptr_eq(&p, &pack)
        ));
        assert!(matches!(
            allowance
                .promote(2, 101, 0, |_| panic!("too large"))
                .unwrap(),
            Promotion::TooLarge
        ));
        assert!(allowance
            .promote(3, 50, 0, |_| Err(io::Error::other("auth")))
            .is_err());
        let stats = allowance.stats();
        assert_eq!((stats.used, stats.promoted, stats.busy), (50, 1, 1));
        promoted(allowance.promote(3, 50, 37, fill).unwrap());
    }

    /// A released object is held again, and the new copy is shared.
    #[test]
    fn a_released_object_is_held_again() {
        let allowance = DictionaryAllowance::new(100);
        drop(allowance.hold(1, 10, || bytes(10)).unwrap());
        let second = allowance.hold(1, 10, || bytes(10)).unwrap().unwrap();
        let third = allowance.hold(1, 10, || panic!("shared")).unwrap().unwrap();
        assert!(Arc::ptr_eq(&second, &third));
        assert_eq!(allowance.stats().admitted, 2);
    }
}
