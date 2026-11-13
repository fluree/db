//! Standalone event bus for ledger and graph source change notifications.
//!
//! [`LedgerEventBus`] is an application-layer event notification mechanism,
//! decoupled from any specific nameservice backend. It uses a
//! [`tokio::sync::broadcast`] channel internally.
//!
//! Typically wrapped around a nameservice via [`NotifyingNameService`](crate::NotifyingNameService),
//! which automatically emits events after successful writes.

use std::sync::Arc;
use tokio::sync::broadcast;

use crate::{NameServiceEvent, Subscription, SubscriptionScope};

/// Standalone event bus for ledger and graph source change notifications.
///
/// Owns a [`broadcast::Sender`] and provides `emit` / `subscribe` methods.
/// Multiple producers can share an `Arc<LedgerEventBus>` and emit concurrently;
/// each subscriber gets an independent receiver.
///
/// This struct is backend-agnostic — it works with any nameservice implementation
/// (file, memory, S3, DynamoDB, etc.).
pub struct LedgerEventBus {
    broadcast: broadcast::Sender<NameServiceEvent>,
    observers: std::sync::RwLock<Vec<Observer>>,
}

/// A synchronous event observer; see [`LedgerEventBus::observe`].
pub type Observer = Arc<dyn Fn(&NameServiceEvent) + Send + Sync>;

impl std::fmt::Debug for LedgerEventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LedgerEventBus")
            .field("receivers", &self.broadcast.receiver_count())
            .finish_non_exhaustive()
    }
}

impl LedgerEventBus {
    /// Create a new event bus with the given channel capacity.
    ///
    /// `capacity` controls how many events can be buffered before slow
    /// receivers start lagging (receiving `RecvError::Lagged`).
    pub fn new(capacity: usize) -> Self {
        let (broadcast, _) = broadcast::channel(capacity);
        Self {
            broadcast,
            observers: std::sync::RwLock::new(Vec::new()),
        }
    }

    /// Notify every observer, then all current subscribers, of an event.
    ///
    /// Returns silently if there are no subscribers (the event is dropped).
    pub fn notify(&self, event: NameServiceEvent) {
        for observer in self
            .observers
            .read()
            .expect("observer lock never poisoned")
            .iter()
        {
            observer(&event);
        }
        let _ = self.broadcast.send(event);
    }

    /// Run `observer` on every event, inside [`Self::notify`]: unlike a
    /// subscriber, it has seen an event before the write that announced it
    /// returns, and it never lags. It must be quick and must not notify.
    pub fn observe(&self, observer: Observer) {
        self.observers
            .write()
            .expect("observer lock never poisoned")
            .push(observer);
    }

    /// Subscribe to events with the given scope.
    ///
    /// The returned [`Subscription`] contains a broadcast receiver that
    /// delivers all events; scope-based filtering is the caller's
    /// responsibility (matching the prior `Publication` contract).
    pub fn subscribe(&self, scope: SubscriptionScope) -> Subscription {
        Subscription {
            scope,
            receiver: self.broadcast.subscribe(),
        }
    }

    /// Create a shared reference suitable for passing to multiple components.
    pub fn into_arc(self) -> Arc<Self> {
        Arc::new(self)
    }
}
