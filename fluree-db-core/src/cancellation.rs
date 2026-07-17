//! Cooperative query cancellation primitives.
//!
//! This module is runtime-agnostic: it uses only atomics so embedders can wire
//! cancellation from any HTTP framework, task runtime, or resource monitor.

use std::fmt;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

const NOT_CANCELLED: u8 = 0;
const CANCELLED: u8 = 1;
const TIMEOUT: u8 = 2;
const CLIENT_DISCONNECTED: u8 = 3;

/// Sentinel in [`QueryCancellationInner::memory_limit`] meaning "no ceiling set" —
/// the embedder has not pinned a per-query budget onto this handle.
const NO_MEMORY_LIMIT: usize = usize::MAX;

/// Reason a cooperative query cancellation was requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryCancellationReason {
    /// Generic caller-initiated cancellation.
    Cancelled,
    /// An external timeout monitor cancelled the query.
    Timeout,
    /// The client connection/request was dropped.
    ClientDisconnected,
}

impl QueryCancellationReason {
    fn from_code(code: u8) -> Option<Self> {
        match code {
            CANCELLED => Some(Self::Cancelled),
            TIMEOUT => Some(Self::Timeout),
            CLIENT_DISCONNECTED => Some(Self::ClientDisconnected),
            _ => None,
        }
    }

    fn as_code(self) -> u8 {
        match self {
            Self::Cancelled => CANCELLED,
            Self::Timeout => TIMEOUT,
            Self::ClientDisconnected => CLIENT_DISCONNECTED,
        }
    }

    /// Stable lowercase reason string for logs and error payloads.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cancelled => "cancelled",
            Self::Timeout => "timeout",
            Self::ClientDisconnected => "client_disconnected",
        }
    }
}

impl fmt::Display for QueryCancellationReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug)]
struct QueryCancellationInner {
    reason: AtomicU8,
    /// Bytes of retained query memory recorded via [`QueryCancellation::record_alloc`].
    /// A monotonic, deliberately-conservative high-water accumulator: callers record
    /// where a retained buffer grows, so the total tracks a query's live post-scan
    /// memory across all its operators.
    allocated: AtomicUsize,
    /// Optional per-query memory ceiling in bytes (`NO_MEMORY_LIMIT` = unset). Stored
    /// as an opaque number — this crate never compares or enforces it; the query
    /// engine reads it at a checkpoint and decides. Lets an embedder pin a budget onto
    /// the same handle that carries cancellation.
    memory_limit: AtomicUsize,
}

impl Default for QueryCancellationInner {
    fn default() -> Self {
        Self {
            reason: AtomicU8::new(NOT_CANCELLED),
            allocated: AtomicUsize::new(0),
            memory_limit: AtomicUsize::new(NO_MEMORY_LIMIT),
        }
    }
}

/// Shared cooperative resource-governance handle for query execution: cancellation
/// (timeout / disconnect / caller-initiated) plus query-scoped memory accounting.
///
/// A disabled value is a single `None` pointer, so callers that do not opt in pay
/// only a cheap branch at checkpoints. Timeout and disconnect detection are external
/// concerns: callers decide when to signal this handle. The memory counter is pure
/// mechanism — this crate accumulates and reports bytes but never enforces a budget;
/// the query engine's checkpoint compares the count against the ceiling.
#[derive(Debug, Clone, Default)]
pub struct QueryCancellation {
    inner: Option<Arc<QueryCancellationInner>>,
}

impl QueryCancellation {
    /// No cancellation handle.
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Create a handle that can be cancelled by cloning it and calling
    /// [`cancel`](Self::cancel) or [`cancel_with`](Self::cancel_with).
    pub fn new() -> Self {
        Self {
            inner: Some(Arc::new(QueryCancellationInner::default())),
        }
    }

    /// Whether this handle can ever report cancellation.
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Request generic cancellation.
    pub fn cancel(&self) {
        self.cancel_with(QueryCancellationReason::Cancelled);
    }

    /// Request cancellation with a specific reason.
    pub fn cancel_with(&self, reason: QueryCancellationReason) {
        if let Some(inner) = &self.inner {
            let _ = inner.reason.compare_exchange(
                NOT_CANCELLED,
                reason.as_code(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
    }

    /// Return the cancellation reason if cancellation was externally signalled.
    #[inline]
    pub fn reason(&self) -> Option<QueryCancellationReason> {
        if let Some(inner) = &self.inner {
            if let Some(reason) =
                QueryCancellationReason::from_code(inner.reason.load(Ordering::Relaxed))
            {
                return Some(reason);
            }
        }
        None
    }

    /// Record `bytes` of retained query memory into the shared counter. Callers record
    /// at the points where a retained buffer (a hash-join build table, a GROUP BY map,
    /// a fused dim-map) grows, so the counter tracks the query's live post-scan memory.
    /// Monotonic and intentionally conservative — over-counting can only trip the guard
    /// on a query already near its ceiling. No-op on a disabled handle.
    #[inline]
    pub fn record_alloc(&self, bytes: usize) {
        if let Some(inner) = &self.inner {
            inner.allocated.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Bytes recorded via [`record_alloc`](Self::record_alloc) so far (0 on a disabled
    /// handle).
    #[inline]
    pub fn allocated_bytes(&self) -> usize {
        self.inner
            .as_ref()
            .map_or(0, |i| i.allocated.load(Ordering::Relaxed))
    }

    /// Pin an optional per-query memory ceiling (bytes) onto this handle. The query
    /// engine reads it at a checkpoint; this crate never enforces it. No-op on a
    /// disabled handle. Mainly for tests / embedders that want an explicit budget
    /// rather than the engine's container-derived default.
    pub fn set_memory_limit(&self, bytes: usize) {
        if let Some(inner) = &self.inner {
            inner.memory_limit.store(bytes, Ordering::Relaxed);
        }
    }

    /// The pinned memory ceiling, or `None` when unset (or on a disabled handle), in
    /// which case the engine applies its own default budget.
    #[inline]
    pub fn memory_limit(&self) -> Option<usize> {
        match self
            .inner
            .as_ref()
            .map(|i| i.memory_limit.load(Ordering::Relaxed))
        {
            Some(v) if v != NO_MEMORY_LIMIT => Some(v),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{QueryCancellation, QueryCancellationReason};

    #[test]
    fn disabled_never_reports_cancellation() {
        let cancellation = QueryCancellation::disabled();

        cancellation.cancel();

        assert!(!cancellation.is_enabled());
        assert_eq!(cancellation.reason(), None);
    }

    #[test]
    fn cloned_handle_observes_cancellation_reason() {
        let cancellation = QueryCancellation::new();
        let observer = cancellation.clone();

        cancellation.cancel_with(QueryCancellationReason::ClientDisconnected);

        assert!(observer.is_enabled());
        assert_eq!(
            observer.reason(),
            Some(QueryCancellationReason::ClientDisconnected)
        );
    }

    #[test]
    fn first_cancellation_reason_wins() {
        let cancellation = QueryCancellation::new();

        cancellation.cancel_with(QueryCancellationReason::ClientDisconnected);
        cancellation.cancel_with(QueryCancellationReason::Timeout);

        assert_eq!(
            cancellation.reason(),
            Some(QueryCancellationReason::ClientDisconnected)
        );
    }

    #[test]
    fn timeout_is_an_externally_signalled_reason() {
        let cancellation = QueryCancellation::new();

        cancellation.cancel_with(QueryCancellationReason::Timeout);

        assert_eq!(
            cancellation.reason(),
            Some(QueryCancellationReason::Timeout)
        );
    }

    #[test]
    fn memory_accounting_accumulates_and_is_shared_across_clones() {
        let cancellation = QueryCancellation::new();
        let derived = cancellation.clone(); // mirrors a derived per-graph context

        cancellation.record_alloc(100);
        derived.record_alloc(50);

        // The counter lives on the shared Arc, so both handles observe the total.
        assert_eq!(cancellation.allocated_bytes(), 150);
        assert_eq!(derived.allocated_bytes(), 150);
    }

    #[test]
    fn memory_limit_defaults_unset_and_round_trips() {
        let cancellation = QueryCancellation::new();
        assert_eq!(cancellation.memory_limit(), None);

        cancellation.set_memory_limit(4096);
        assert_eq!(cancellation.memory_limit(), Some(4096));
    }

    #[test]
    fn disabled_handle_has_no_memory_guard() {
        let cancellation = QueryCancellation::disabled();

        cancellation.record_alloc(1 << 30);
        cancellation.set_memory_limit(1);

        // Both are no-ops on a disabled handle: no counter, no ceiling.
        assert_eq!(cancellation.allocated_bytes(), 0);
        assert_eq!(cancellation.memory_limit(), None);
    }
}
