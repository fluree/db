//! Request coalescing: concurrent misses on one key share a single fetch.
//!
//! The first caller for a key becomes the leader and performs the work;
//! later callers become waiters and receive a clone of the leader's result.
//! If the leader's future is dropped before completing (cancellation), the
//! guard fails the waiters instead of leaving them hanging.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::oneshot;

type Waiters<V, E> = Vec<oneshot::Sender<Result<V, E>>>;

/// Coalescing map. Cheap to clone; all clones share state.
pub struct InFlight<K, V, E> {
    inner: Arc<Mutex<HashMap<K, Waiters<V, E>>>>,
}

impl<K, V, E> Clone for InFlight<K, V, E> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K, V, E> Default for InFlight<K, V, E> {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<K, V, E> std::fmt::Debug for InFlight<K, V, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n = self
            .inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        f.debug_struct("InFlight").field("keys", &n).finish()
    }
}

/// What a caller got when it asked to start work on a key.
pub enum Ticket<K, V, E>
where
    K: Eq + Hash + Clone,
{
    /// Nobody else is working on the key: do the work, then
    /// [`LeaderGuard::complete`].
    Leader(LeaderGuard<K, V, E>),
    /// Someone else is on it: await the shared result.
    Waiter(oneshot::Receiver<Result<V, E>>),
}

/// Held by the leader while it works; completing it fans the result out.
pub struct LeaderGuard<K, V, E>
where
    K: Eq + Hash + Clone,
{
    map: InFlight<K, V, E>,
    key: K,
    done: bool,
}

impl<K, V, E> InFlight<K, V, E>
where
    K: Eq + Hash + Clone,
    V: Clone,
    E: Clone,
{
    /// Join the work on `key`: leader if nobody is on it, waiter otherwise.
    pub fn begin(&self, key: K) -> Ticket<K, V, E> {
        let mut map = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        match map.get_mut(&key) {
            Some(waiters) => {
                let (tx, rx) = oneshot::channel();
                waiters.push(tx);
                Ticket::Waiter(rx)
            }
            None => {
                map.insert(key.clone(), Vec::new());
                Ticket::Leader(LeaderGuard {
                    map: self.clone(),
                    key,
                    done: false,
                })
            }
        }
    }

    /// Number of keys with work in flight.
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Whether nothing is in flight.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn finish(&self, key: &K, result: Result<V, E>) {
        let waiters = self
            .inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(key)
            .unwrap_or_default();
        for tx in waiters {
            // A waiter that went away is not an error.
            let _ = tx.send(result.clone());
        }
    }
}

impl<K, V, E> LeaderGuard<K, V, E>
where
    K: Eq + Hash + Clone,
    V: Clone,
    E: Clone,
{
    /// Publish the result to every waiter and release the key.
    pub fn complete(mut self, result: Result<V, E>) {
        self.done = true;
        self.map.finish(&self.key, result);
    }

    /// The key this leader owns.
    pub fn key(&self) -> &K {
        &self.key
    }
}

impl<K, V, E> Drop for LeaderGuard<K, V, E>
where
    K: Eq + Hash + Clone,
{
    fn drop(&mut self) {
        if self.done {
            return;
        }
        // Cancelled before completing: release the key and drop the
        // waiters' senders, which resolves their receivers with a
        // `RecvError` they can report as "leader cancelled".
        let _ = self
            .map
            .inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.key);
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn waiters_receive_the_leaders_result() {
        let map: InFlight<&'static str, u32, String> = InFlight::default();
        let Ticket::Leader(leader) = map.begin("k") else {
            panic!("first caller must lead");
        };
        let Ticket::Waiter(w1) = map.begin("k") else {
            panic!("second caller must wait");
        };
        let Ticket::Waiter(w2) = map.begin("k") else {
            panic!("third caller must wait");
        };
        assert_eq!(map.len(), 1);
        leader.complete(Ok(42));
        assert_eq!(w1.await.unwrap().unwrap(), 42);
        assert_eq!(w2.await.unwrap().unwrap(), 42);
        assert!(map.is_empty());
        // Key is free again.
        assert!(matches!(map.begin("k"), Ticket::Leader(_)));
    }

    #[tokio::test]
    async fn errors_fan_out_too() {
        let map: InFlight<u8, u32, String> = InFlight::default();
        let Ticket::Leader(leader) = map.begin(1) else {
            panic!()
        };
        let Ticket::Waiter(w) = map.begin(1) else {
            panic!()
        };
        leader.complete(Err("boom".to_string()));
        assert_eq!(w.await.unwrap().unwrap_err(), "boom");
    }

    #[tokio::test]
    async fn cancelled_leader_fails_waiters_instead_of_hanging_them() {
        let map: InFlight<u8, u32, String> = InFlight::default();
        let Ticket::Leader(leader) = map.begin(1) else {
            panic!()
        };
        let Ticket::Waiter(w) = map.begin(1) else {
            panic!()
        };
        drop(leader);
        assert!(w.await.is_err(), "waiter must observe the cancellation");
        assert!(map.is_empty());
    }
}
