//! Cooperative query cancellation primitives.
//!
//! This module is runtime-agnostic: it uses only atomics so embedders can wire
//! cancellation from any HTTP framework, task runtime, or resource monitor.

use crate::clock::Instant;
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
    /// A deliberately-conservative accumulator: callers record where a retained buffer
    /// grows, so the total tracks a query's live post-scan memory across all its
    /// operators. Mostly grows (persistent join/aggregate builds, retained lookups);
    /// an allocation with a PROVABLE drop point (a scan window handed off and dropped)
    /// may be paired with [`QueryCancellation::release`], so this is a high-water of
    /// *live* accounted memory, not a monotonic all-time sum.
    allocated: AtomicUsize,
    /// Optional per-query memory ceiling in bytes (`NO_MEMORY_LIMIT` = unset). Stored
    /// as an opaque number — this crate never compares or enforces it; the query
    /// engine reads it at a checkpoint and decides. Lets an embedder pin a budget onto
    /// the same handle that carries cancellation.
    memory_limit: AtomicUsize,
    /// When set, the handle reports [`QueryCancellationReason::Timeout`] once
    /// this instant passes, without anyone having to signal it.
    deadline: Option<Instant>,
    /// A handle whose cancellation this one also reports.
    parent: Option<QueryCancellation>,
}

impl Default for QueryCancellationInner {
    fn default() -> Self {
        Self {
            reason: AtomicU8::new(NOT_CANCELLED),
            allocated: AtomicUsize::new(0),
            memory_limit: AtomicUsize::new(NO_MEMORY_LIMIT),
            deadline: None,
            parent: None,
        }
    }
}

/// Shared cooperative resource-governance handle for query execution: cancellation
/// (timeout / disconnect / caller-initiated) plus query-scoped memory accounting.
///
/// A disabled value is a single `None` pointer, so callers that do not opt in pay
/// only a cheap branch at checkpoints. Timeout and disconnect detection are external
/// concerns: callers decide when to signal this handle, or give it a deadline at
/// construction ([`with_deadline`](Self::with_deadline)) that checkpoints compare. The memory counter is pure
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

    /// A handle that times out by itself once `deadline` passes, and can also
    /// be cancelled like one from [`new`](Self::new).
    pub fn with_deadline(deadline: Instant) -> Self {
        Self {
            inner: Some(Arc::new(QueryCancellationInner {
                deadline: Some(deadline),
                ..QueryCancellationInner::default()
            })),
        }
    }

    /// A handle for work done on this handle's behalf under a deadline of its
    /// own: it reports this handle's cancellation, and times out by itself
    /// once `deadline` passes without cancelling this one. Its memory is
    /// accounted separately. Disabled when this handle is and there is no
    /// deadline.
    pub fn with_deadline_inside(&self, deadline: Option<Instant>) -> Self {
        if !self.is_enabled() && deadline.is_none() {
            return Self::disabled();
        }
        Self {
            inner: Some(Arc::new(QueryCancellationInner {
                deadline,
                parent: self.is_enabled().then(|| self.clone()),
                ..QueryCancellationInner::default()
            })),
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

    /// Return the cancellation reason if cancellation was signalled or the
    /// handle's deadline has passed.
    #[inline]
    pub fn reason(&self) -> Option<QueryCancellationReason> {
        if let Some(inner) = &self.inner {
            if let Some(reason) =
                QueryCancellationReason::from_code(inner.reason.load(Ordering::Relaxed))
            {
                return Some(reason);
            }
            if inner
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                self.cancel_with(QueryCancellationReason::Timeout);
                return self.reason();
            }
            if let Some(parent) = &inner.parent {
                return parent.reason();
            }
        }
        None
    }

    /// Record `bytes` of retained query memory into the shared counter. Callers record
    /// at the points where a retained buffer (a hash-join build table, a GROUP BY map,
    /// a fused dim-map) grows, so the counter tracks the query's live post-scan memory.
    /// Intentionally conservative — over-counting can only trip the guard on a query
    /// already near its ceiling. Persistent allocations are never released; an
    /// allocation with a provable drop point is paired with [`release`](Self::release).
    /// No-op on a disabled handle.
    #[inline]
    pub fn record_alloc(&self, bytes: usize) {
        if let Some(inner) = &self.inner {
            inner.allocated.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Release `bytes` previously recorded via [`record_alloc`](Self::record_alloc),
    /// for an allocation whose drop point the caller can prove — e.g. a scan window
    /// that has been handed off downstream and is about to drop, so a streaming scan
    /// of N sequentially-freed windows accounts only the resident one rather than
    /// their all-time sum. Saturating, so a double- or over-release can never
    /// underflow the counter. It is a CALLER INVARIANT that `release` is paired only
    /// with a matching non-persistent `record_alloc`: persistent buffers (join /
    /// aggregate builds, retained lookups) must never be released, or the guard would
    /// under-count live memory. No-op on a disabled handle.
    #[inline]
    pub fn release(&self, bytes: usize) {
        if let Some(inner) = &self.inner {
            let _ = inner
                .allocated
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                    Some(cur.saturating_sub(bytes))
                });
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
    use crate::clock::Instant;
    use std::time::Duration;

    #[test]
    fn a_handle_inside_another_reports_both_but_cancels_only_itself() {
        let query = QueryCancellation::new();
        let expired = query.with_deadline_inside(Some(Instant::now()));
        assert_eq!(expired.reason(), Some(QueryCancellationReason::Timeout));
        assert_eq!(query.reason(), None);

        let pending = query.with_deadline_inside(Some(Instant::now() + Duration::from_secs(3600)));
        assert_eq!(pending.reason(), None);
        query.cancel_with(QueryCancellationReason::ClientDisconnected);
        assert_eq!(
            pending.reason(),
            Some(QueryCancellationReason::ClientDisconnected)
        );

        assert!(!QueryCancellation::disabled()
            .with_deadline_inside(None)
            .is_enabled());
    }

    #[test]
    fn a_deadline_reports_timeout_once_it_passes() {
        let expired = QueryCancellation::with_deadline(Instant::now());
        assert_eq!(expired.reason(), Some(QueryCancellationReason::Timeout));

        let pending = QueryCancellation::with_deadline(Instant::now() + Duration::from_secs(3600));
        assert_eq!(pending.reason(), None);
        pending.cancel_with(QueryCancellationReason::ClientDisconnected);
        assert_eq!(
            pending.reason(),
            Some(QueryCancellationReason::ClientDisconnected)
        );
    }

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
    fn release_subtracts_and_saturates_at_zero() {
        let cancellation = QueryCancellation::new();
        let derived = cancellation.clone();

        cancellation.record_alloc(1000);
        // A provable-drop allocation is released — the counter tracks LIVE memory, so
        // charging then releasing nets zero (a streaming scan's per-window pattern).
        cancellation.release(400);
        assert_eq!(cancellation.allocated_bytes(), 600);
        assert_eq!(
            derived.allocated_bytes(),
            600,
            "release is shared across clones"
        );

        // Over-release saturates at zero rather than underflowing.
        derived.release(10_000);
        assert_eq!(cancellation.allocated_bytes(), 0);
    }

    #[test]
    fn disabled_handle_release_is_a_noop() {
        let cancellation = QueryCancellation::disabled();
        cancellation.release(1 << 20);
        assert_eq!(cancellation.allocated_bytes(), 0);
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
