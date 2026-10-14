//! The memory forward dictionary packs may hold decrypted whole.
//!
//! A pack whose store decrypts on read is held whole only if its size can be
//! reserved here; otherwise it is read by range. The reservation is charged
//! until the last reader holding the pack drops it, and readers of the same
//! object — snapshots, reloads, ledgers on the same storage — share one copy,
//! charged once.

use std::collections::HashMap;
use std::io;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

/// Experiment control for the segmented-envelope A/B, not a supported
/// configuration: `FLUREE_DICT_ALLOWANCE_MB`, unbounded when unset.
pub(crate) fn allowance_from_env() -> u64 {
    std::env::var("FLUREE_DICT_ALLOWANCE_MB")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(u64::MAX, |mb| mb.saturating_mul(1024 * 1024))
}

pub struct DictionaryAllowance {
    capacity: u64,
    state: Mutex<State>,
    admitted: AtomicU64,
    declined: AtomicU64,
    shared: AtomicU64,
    released: AtomicU64,
}

#[derive(Default)]
struct State {
    used: u64,
    held: HashMap<u128, Weak<HeldPack>>,
}

/// What the allowance holds and has done, for diagnostics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AllowanceStats {
    pub capacity: u64,
    pub used: u64,
    pub packs: u64,
    pub admitted: u64,
    pub declined: u64,
    pub shared: u64,
    pub released: u64,
}

/// A decrypted pack held within the allowance; dropping the last reference
/// returns its bytes to it.
pub struct HeldPack {
    bytes: Box<[u8]>,
    identity: u128,
    allowance: Weak<DictionaryAllowance>,
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
        Arc::new(Self {
            capacity,
            state: Mutex::default(),
            admitted: AtomicU64::new(0),
            declined: AtomicU64::new(0),
            shared: AtomicU64::new(0),
            released: AtomicU64::new(0),
        })
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
            state.used += len;
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
            bytes: bytes.into_boxed_slice(),
            identity,
            allowance: Arc::downgrade(self),
        });
        state.held.insert(identity, Arc::downgrade(&pack));
        self.admitted.fetch_add(1, Ordering::Relaxed);
        Ok(Some(pack))
    }

    pub fn stats(&self) -> AllowanceStats {
        let state = self.lock();
        AllowanceStats {
            capacity: self.capacity,
            used: state.used,
            packs: state.held.values().filter(|w| w.strong_count() > 0).count() as u64,
            admitted: self.admitted.load(Ordering::Relaxed),
            declined: self.declined.load(Ordering::Relaxed),
            shared: self.shared.load(Ordering::Relaxed),
            released: self.released.load(Ordering::Relaxed),
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
