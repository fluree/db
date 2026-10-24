//! Thread-local tracing subscribers that see every event they should.
//!
//! A test that asserts on tracing output installs a subscriber as its own
//! thread's default and reads back what it recorded. Each callsite's interest
//! is cached process-wide, though, and `tracing-core` (0.1.36) computes a newly
//! registered callsite's interest from the registering thread's default alone
//! whenever at most one dispatcher is registered. A test capture is often that
//! one dispatcher, and the callsite is often first hit on another test's
//! thread, whose default is the no-op subscriber: the callsite is cached as
//! never interesting and the capture never sees it. It comes back empty,
//! intermittently and more often under load.
//!
//! [`scoped_default`] and [`with_scoped_default`] close that. Before installing
//! the subscriber they register, once per process, two dispatchers that are
//! never anyone's default. Each answers every callsite with
//! `Interest::sometimes()`, hints `TRACE`, and enables nothing. With two alive,
//! the one-dispatcher path is never taken again, even while no capture is
//! installed: every callsite's interest is combined across all live
//! dispatchers under tracing's own lock, and a combination that includes
//! `sometimes` is never `never`. So each event reaches the installed
//! subscriber's `enabled`, which decides. (One would do while a capture is
//! alive; a second keeps the path closed between captures, when a callsite
//! registered on a thread without a subscriber would otherwise be cached
//! `never` again, racing the next capture's install.) The `TRACE` hint means
//! they never narrow the level filter. They receive no events themselves; only
//! a thread's default does.
//!
//! Install a test's thread-local subscriber only through these, never with
//! `tracing::subscriber::set_default` or `with_default` directly;
//! [`assert_scoped_subscribers_use_the_floor`](crate::assert_scoped_subscribers_use_the_floor)
//! checks that.

use std::sync::OnceLock;
use tracing::level_filters::LevelFilter;
use tracing::subscriber::{DefaultGuard, Interest};
use tracing::{span, Dispatch, Event, Metadata, Subscriber};

/// Installs `subscriber` as this thread's default until the guard drops.
pub fn scoped_default<S>(subscriber: S) -> DefaultGuard
where
    S: Subscriber + Send + Sync + 'static,
{
    install_floor();
    let guard = tracing::subscriber::set_default(subscriber);
    // Installing rebuilt every registered callsite with all live dispatchers.
    // A callsite whose registration began before the floor existed can still
    // store a stale `never` after that; one more rebuild repairs it.
    tracing::callsite::rebuild_interest_cache();
    guard
}

/// Runs `f` with `subscriber` as this thread's default.
pub fn with_scoped_default<S, T>(subscriber: S, f: impl FnOnce() -> T) -> T
where
    S: Subscriber + Send + Sync + 'static,
{
    let _guard = scoped_default(subscriber);
    f()
}

fn install_floor() {
    static FLOOR: OnceLock<[Dispatch; 2]> = OnceLock::new();
    FLOOR.get_or_init(|| [Dispatch::new(InterestFloor), Dispatch::new(InterestFloor)]);
}

/// Registered, never a default: it shapes cached interest and records
/// nothing.
struct InterestFloor;

impl Subscriber for InterestFloor {
    fn register_callsite(&self, _: &'static Metadata<'static>) -> Interest {
        Interest::sometimes()
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(LevelFilter::TRACE)
    }

    fn enabled(&self, _: &Metadata<'_>) -> bool {
        false
    }

    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }

    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}

    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}

    fn event(&self, _: &Event<'_>) {}

    fn enter(&self, _: &span::Id) {}

    fn exit(&self, _: &span::Id) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Counts the events it is handed; interested in every callsite.
    struct Count(Arc<AtomicUsize>);

    impl Subscriber for Count {
        fn register_callsite(&self, _: &'static Metadata<'static>) -> Interest {
            Interest::always()
        }

        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
            span::Id::from_u64(1)
        }

        fn record(&self, _: &span::Id, _: &span::Record<'_>) {}

        fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}

        fn event(&self, _: &Event<'_>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn enter(&self, _: &span::Id) {}

        fn exit(&self, _: &span::Id) {}
    }

    /// A callsite first hit on another thread, one with no subscriber, still
    /// reaches this thread's capture. Through a bare `set_default` the other
    /// thread caches it as never interesting and the count stays 0.
    #[test]
    fn a_callsite_registered_first_on_another_thread_reaches_the_capture() {
        fn hit() {
            tracing::info!(target: "scoped_subscriber", "first hit on another thread");
        }
        let count = Arc::new(AtomicUsize::new(0));
        let _guard = scoped_default(Count(Arc::clone(&count)));
        std::thread::spawn(hit).join().unwrap();
        hit();
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}
