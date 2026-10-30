//! A held credential renewed single-flight, with a pause after a failure.
//!
//! The Google token cache and the catalog-vended storage credentials hold one
//! credential each and renew it inside a margin before it expires. Both are
//! shared by many concurrent requests (a host identity's token by the whole
//! process), so how they renew decides what an outage at the issuer costs:
//!
//! - One renewal is in flight at a time. While it runs, a caller whose held
//!   credential is still valid reads on with it, without waiting; a caller
//!   with nothing usable waits for that renewal rather than starting its own.
//! - A failed renewal is remembered for [`RENEWAL_BACKOFF`]. Within it no
//!   renewal is attempted: a still-valid credential is served, else the
//!   failure is returned at once. An outage costs one attempt per window, not
//!   one per caller, each up to the issuer's timeout.
//! - A server's refusal forces a renewal at most once per
//!   [`FORCED_RENEWAL_INTERVAL`], so one source's 401s cannot re-mint, over
//!   and over, a credential many sources share. A credential the holder
//!   marks refused ([`Renewing::refuse`]) is never served again;
//!   [`Renewing::force`] renews without taking one refusal as everyone's.

use std::future::Future;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use crate::error::{IcebergError, Result};

/// How long a failed renewal is remembered: no renewal is attempted within it.
pub(crate) const RENEWAL_BACKOFF: Duration = Duration::from_secs(5);

/// How often a server's refusal may force a renewal.
pub(crate) const FORCED_RENEWAL_INTERVAL: Duration = Duration::from_secs(30);

/// How a held credential stands, as its holder judges it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Standing {
    /// Outside the renewal margin: served as held.
    Fresh,
    /// Inside the margin but not expired: renew, and serve it meanwhile.
    Usable,
    /// Expired: renew, and serve nothing else.
    Expired,
}

/// One held credential and its renewal.
pub(crate) struct Renewing<T> {
    state: Mutex<State<T>>,
    /// Held across a renewal: the single flight.
    renewal: tokio::sync::Mutex<()>,
}

struct State<T> {
    held: Option<T>,
    /// A server refused the held credential: never serve it, renew on next use.
    refused: bool,
    /// The last renewal failed, at this instant, with this error.
    failed: Option<(Instant, IcebergError)>,
    /// When a refusal last forced a renewal that succeeded.
    last_forced: Option<Instant>,
}

impl<T: Clone> Default for Renewing<T> {
    fn default() -> Self {
        Self::new(None)
    }
}

/// What a caller may be served without renewing, decided under the state lock.
enum Decision<T> {
    Serve(T),
    Fail(IcebergError),
    Renew { usable: Option<T> },
}

impl<T: Clone> Renewing<T> {
    /// Hold `initial` (or nothing yet).
    pub(crate) fn new(initial: Option<T>) -> Self {
        Self {
            state: Mutex::new(State {
                held: initial,
                refused: false,
                failed: None,
                last_forced: None,
            }),
            renewal: tokio::sync::Mutex::new(()),
        }
    }

    fn state(&self) -> MutexGuard<'_, State<T>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The held credential, unless a server refused it.
    pub(crate) fn held(&self) -> Option<T> {
        let state = self.state();
        state.held.clone().filter(|_| !state.refused)
    }

    fn decide(&self, standing: &impl Fn(&T) -> Standing) -> Decision<T> {
        let state = self.state();
        let usable = match state.held.as_ref().filter(|_| !state.refused) {
            Some(held) => match standing(held) {
                Standing::Fresh => return Decision::Serve(held.clone()),
                Standing::Usable => Some(held.clone()),
                Standing::Expired => None,
            },
            None => None,
        };
        match &state.failed {
            Some((at, err)) if at.elapsed() < RENEWAL_BACKOFF => match usable {
                Some(held) => Decision::Serve(held),
                None => Decision::Fail(err.clone()),
            },
            _ => Decision::Renew { usable },
        }
    }

    /// The held credential while `standing` calls it fresh, else a renewed
    /// one from `renew` (see the module docs for who renews and who waits).
    /// A failed renewal reads on with a credential that has not expired yet.
    pub(crate) async fn get_or_renew<F, Fut>(
        &self,
        standing: impl Fn(&T) -> Standing,
        renew: F,
    ) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let usable = match self.decide(&standing) {
            Decision::Serve(held) => return Ok(held),
            Decision::Fail(err) => return Err(err),
            Decision::Renew { usable } => usable,
        };
        let _flight = match &usable {
            // A renewal is in flight: read on with the held credential.
            Some(held) => match self.renewal.try_lock() {
                Ok(flight) => flight,
                Err(_) => return Ok(held.clone()),
            },
            None => self.renewal.lock().await,
        };
        // The renewal waited for may have answered.
        let usable = match self.decide(&standing) {
            Decision::Serve(held) => return Ok(held),
            Decision::Fail(err) => return Err(err),
            Decision::Renew { usable } => usable,
        };
        let renewed = renew().await;
        let mut state = self.state();
        match renewed {
            Ok(fresh) => {
                if state.refused {
                    state.last_forced = Some(Instant::now());
                }
                state.held = Some(fresh.clone());
                state.refused = false;
                state.failed = None;
                Ok(fresh)
            }
            Err(err) => {
                state.failed = Some((Instant::now(), err.clone()));
                match usable {
                    Some(held) => {
                        tracing::warn!(error = %err, "credential renewal failed; reading on with the held credential");
                        Ok(held)
                    }
                    None => Err(err),
                }
            }
        }
    }

    /// A server refused the held credential: never serve it again, and renew
    /// on next use — unless a refusal forced a renewal within
    /// [`FORCED_RENEWAL_INTERVAL`], when the held credential (itself just
    /// renewed for a refusal) is kept and the caller's retry reports the
    /// server's refusal. Returns whether the credential was refused.
    pub(crate) fn refuse(&self) -> bool {
        let mut state = self.state();
        let recently_forced = state
            .last_forced
            .is_some_and(|at| at.elapsed() < FORCED_RENEWAL_INTERVAL);
        if state.held.is_some() && recently_forced {
            tracing::debug!(
                "a credential was renewed for a refusal moments ago; not renewing another"
            );
            return false;
        }
        state.refused = true;
        // A refusal is a reason to try again now, not after a backoff.
        state.failed = None;
        true
    }

    /// Renew now, after a server refused the held credential, at most once
    /// per [`FORCED_RENEWAL_INTERVAL`]: within that window of the last forced
    /// renewal the held credential (itself just renewed for a refusal) is
    /// kept, and the caller's retry reports the server's refusal. Unlike
    /// [`refuse`](Self::refuse), the held credential stays served to others
    /// if the renewal fails: one source's refusal is not taken as everyone's.
    /// Within [`RENEWAL_BACKOFF`] of a failed renewal, that failure is
    /// returned without another attempt. Returns whether it renewed.
    pub(crate) async fn force<F, Fut>(&self, renew: F) -> Result<bool>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let skip = |state: &State<T>| -> Option<Result<bool>> {
            let recently_forced = state
                .last_forced
                .is_some_and(|at| at.elapsed() < FORCED_RENEWAL_INTERVAL);
            if state.held.is_some() && recently_forced {
                tracing::debug!(
                    "a credential was renewed for a refusal moments ago; not renewing another"
                );
                return Some(Ok(false));
            }
            match &state.failed {
                Some((at, err)) if at.elapsed() < RENEWAL_BACKOFF => Some(Err(err.clone())),
                _ => None,
            }
        };
        if let Some(skipped) = skip(&self.state()) {
            return skipped;
        }
        let _flight = self.renewal.lock().await;
        if let Some(skipped) = skip(&self.state()) {
            return skipped;
        }
        let renewed = renew().await;
        let mut state = self.state();
        match renewed {
            Ok(fresh) => {
                state.held = Some(fresh);
                state.refused = false;
                state.failed = None;
                state.last_forced = Some(Instant::now());
                Ok(true)
            }
            Err(err) => {
                state.failed = Some((Instant::now(), err.clone()));
                Err(err)
            }
        }
    }

    /// Move every remembered instant `by` into the past. Tests only: a fake
    /// clock for the backoff and the forced-renewal interval.
    #[cfg(test)]
    pub(crate) fn rewind(&self, by: Duration) {
        let mut state = self.state();
        let back = |at: Instant| at.checked_sub(by).unwrap_or(at);
        if let Some((at, _)) = state.failed.as_mut() {
            *at = back(*at);
        }
        state.last_forced = state.last_forced.map(back);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// A credential that is just a name and how it stands.
    type Cred = (&'static str, Standing);

    fn standing(cred: &Cred) -> Standing {
        cred.1
    }

    /// Counts renewals; fails while `fail` is set.
    #[derive(Default)]
    struct Issuer {
        calls: AtomicUsize,
        fail: std::sync::atomic::AtomicBool,
    }

    impl Issuer {
        async fn issue(&self) -> Result<Cred> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail.load(Ordering::SeqCst) {
                Err(IcebergError::Credential("issuer down".to_string()))
            } else {
                Ok(("renewed", Standing::Fresh))
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    /// After a failed renewal, nothing is renewed until the backoff passes:
    /// callers with nothing usable get the failure at once, callers with a
    /// still-valid credential are served it; then one renewal is tried again.
    #[tokio::test]
    async fn a_failed_renewal_is_not_retried_until_the_backoff_passes() {
        let issuer = Issuer::default();
        issuer.fail.store(true, Ordering::SeqCst);
        let empty = Renewing::<Cred>::default();
        for _ in 0..5 {
            let err = empty
                .get_or_renew(standing, || issuer.issue())
                .await
                .unwrap_err();
            assert!(err.to_string().contains("issuer down"), "{err}");
        }
        assert_eq!(issuer.calls(), 1, "one attempt per backoff window");

        issuer.fail.store(false, Ordering::SeqCst);
        assert!(
            empty
                .get_or_renew(standing, || issuer.issue())
                .await
                .is_err(),
            "still inside the window"
        );
        empty.rewind(RENEWAL_BACKOFF);
        let renewed = empty.get_or_renew(standing, || issuer.issue()).await;
        assert_eq!(renewed.unwrap().0, "renewed");
        assert_eq!(issuer.calls(), 2);

        // A still-valid credential reads on through a failed renewal and
        // its window, without asking again.
        issuer.fail.store(true, Ordering::SeqCst);
        let held = Renewing::new(Some(("held", Standing::Usable)));
        for _ in 0..5 {
            let served = held.get_or_renew(standing, || issuer.issue()).await;
            assert_eq!(served.unwrap().0, "held");
        }
        assert_eq!(issuer.calls(), 3);
    }

    /// While one renewal is in flight, a caller holding a still-valid
    /// credential is served it without waiting; a caller with nothing usable
    /// waits for that renewal and is served its answer, not a second one.
    #[tokio::test]
    async fn a_renewal_in_flight_does_not_hold_up_a_still_valid_credential() {
        let gate = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let slow = |gate: Arc<tokio::sync::Notify>, calls: Arc<AtomicUsize>| async move {
            calls.fetch_add(1, Ordering::SeqCst);
            gate.notified().await;
            Ok(("renewed", Standing::Fresh))
        };

        let held = Arc::new(Renewing::new(Some(("held", Standing::Usable))));
        let in_flight = tokio::spawn({
            let (held, gate, calls) = (Arc::clone(&held), Arc::clone(&gate), Arc::clone(&calls));
            async move { held.get_or_renew(standing, || slow(gate, calls)).await }
        });
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let served = held
            .get_or_renew(standing, || slow(Arc::clone(&gate), Arc::clone(&calls)))
            .await;
        assert_eq!(served.unwrap().0, "held", "served without waiting");
        gate.notify_one();
        assert_eq!(in_flight.await.unwrap().unwrap().0, "renewed");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let calls = Arc::new(AtomicUsize::new(0));
        let empty = Arc::new(Renewing::<Cred>::default());
        let first = tokio::spawn({
            let (empty, gate, calls) = (Arc::clone(&empty), Arc::clone(&gate), Arc::clone(&calls));
            async move { empty.get_or_renew(standing, || slow(gate, calls)).await }
        });
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let second = tokio::spawn({
            let (empty, gate, calls) = (Arc::clone(&empty), Arc::clone(&gate), Arc::clone(&calls));
            async move { empty.get_or_renew(standing, || slow(gate, calls)).await }
        });
        tokio::task::yield_now().await;
        gate.notify_one();
        assert_eq!(first.await.unwrap().unwrap().0, "renewed");
        assert_eq!(second.await.unwrap().unwrap().0, "renewed");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the waiter renewed nothing"
        );
    }

    /// A forced renewal happens at most once per interval, keeps the held
    /// credential for others when it fails, and is not retried within the
    /// backoff of a failure.
    #[tokio::test]
    async fn a_forced_renewal_that_fails_keeps_the_held_credential_and_backs_off() {
        let issuer = Issuer::default();
        let held = Renewing::new(Some(("held", Standing::Fresh)));
        issuer.fail.store(true, Ordering::SeqCst);
        assert!(held.force(|| issuer.issue()).await.is_err());
        assert_eq!(held.held().unwrap().0, "held", "still served to others");
        assert!(held.force(|| issuer.issue()).await.is_err());
        assert_eq!(issuer.calls(), 1, "not retried within the backoff");

        held.rewind(RENEWAL_BACKOFF);
        issuer.fail.store(false, Ordering::SeqCst);
        assert!(held.force(|| issuer.issue()).await.unwrap());
        assert!(
            !held.force(|| issuer.issue()).await.unwrap(),
            "within the interval"
        );
        assert_eq!(issuer.calls(), 2);
        held.rewind(FORCED_RENEWAL_INTERVAL);
        assert!(held.force(|| issuer.issue()).await.unwrap());
        assert_eq!(issuer.calls(), 3);
    }

    /// A refused credential is never served, even still valid and even when
    /// its renewal fails; a refusal within the forced-renewal interval of the
    /// last forced renewal keeps the held one; a refusal ends a backoff.
    #[tokio::test]
    async fn a_refused_credential_is_renewed_at_most_once_per_interval() {
        let issuer = Issuer::default();
        let held = Renewing::new(Some(("held", Standing::Fresh)));
        assert!(held.refuse());
        issuer.fail.store(true, Ordering::SeqCst);
        assert!(held
            .get_or_renew(standing, || issuer.issue())
            .await
            .is_err());
        assert!(held.held().is_none(), "refused, so never served");

        // The refusal set aside the backoff the failure started.
        assert!(held.refuse());
        issuer.fail.store(false, Ordering::SeqCst);
        let renewed = held.get_or_renew(standing, || issuer.issue()).await;
        assert_eq!(renewed.unwrap().0, "renewed");
        assert_eq!(issuer.calls(), 2);

        assert!(!held.refuse(), "renewed for a refusal moments ago");
        assert_eq!(held.held().unwrap().0, "renewed", "kept");
        held.rewind(FORCED_RENEWAL_INTERVAL);
        assert!(held.refuse(), "past the interval");
        assert!(held.held().is_none());
    }
}
