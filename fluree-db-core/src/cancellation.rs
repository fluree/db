//! Cooperative query cancellation primitives.
//!
//! This module is runtime-agnostic: it uses only atomics and `std::time::Instant`
//! so embedders can wire cancellation from any HTTP framework or task runtime.

use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const NOT_CANCELLED: u8 = 0;
const CANCELLED: u8 = 1;
const TIMEOUT: u8 = 2;
const CLIENT_DISCONNECTED: u8 = 3;

/// Reason a cooperative query cancellation was requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryCancellationReason {
    /// Generic caller-initiated cancellation.
    Cancelled,
    /// The query exceeded its configured deadline.
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

#[derive(Debug, Default)]
struct QueryCancellationInner {
    reason: AtomicU8,
}

/// Shared cooperative cancellation/deadline handle for query execution.
///
/// A disabled value is a single `None` pointer plus no deadline, so callers that
/// do not opt in pay only a cheap branch at checkpoints.
#[derive(Debug, Clone, Default)]
pub struct QueryCancellation {
    inner: Option<Arc<QueryCancellationInner>>,
    deadline: Option<Instant>,
}

impl QueryCancellation {
    /// No cancellation and no deadline.
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Create a handle that can be cancelled by cloning it and calling
    /// [`cancel`](Self::cancel) or [`cancel_with`](Self::cancel_with).
    pub fn new() -> Self {
        Self {
            inner: Some(Arc::new(QueryCancellationInner::default())),
            deadline: None,
        }
    }

    /// Create a handle with an absolute deadline.
    pub fn with_deadline(deadline: Instant) -> Self {
        Self {
            inner: Some(Arc::new(QueryCancellationInner::default())),
            deadline: Some(deadline),
        }
    }

    /// Create a handle with a relative timeout.
    pub fn with_timeout(timeout: Duration) -> Self {
        Self::with_deadline(Instant::now() + timeout)
    }

    /// Return a clone with the given absolute deadline.
    pub fn deadline(mut self, deadline: Instant) -> Self {
        if self.inner.is_none() {
            self.inner = Some(Arc::new(QueryCancellationInner::default()));
        }
        self.deadline = Some(deadline);
        self
    }

    /// Return a clone with the given relative timeout.
    pub fn timeout(self, timeout: Duration) -> Self {
        self.deadline(Instant::now() + timeout)
    }

    /// Whether this handle can ever report cancellation.
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.inner.is_some() || self.deadline.is_some()
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

    /// Return the cancellation reason if cancelled or timed out.
    #[inline]
    pub fn reason(&self) -> Option<QueryCancellationReason> {
        if let Some(inner) = &self.inner {
            if let Some(reason) =
                QueryCancellationReason::from_code(inner.reason.load(Ordering::Relaxed))
            {
                return Some(reason);
            }
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Some(QueryCancellationReason::Timeout);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{QueryCancellation, QueryCancellationReason};
    use std::time::{Duration, Instant};

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
    fn elapsed_deadline_reports_timeout() {
        let cancellation =
            QueryCancellation::with_deadline(Instant::now() - Duration::from_millis(1));

        assert_eq!(
            cancellation.reason(),
            Some(QueryCancellationReason::Timeout)
        );
    }
}
