//! The process-wide Google token cache, refreshed single-flight.
//!
//! Catalog clients are cached per source and rebuilt on a TTL, and several
//! sources can share one identity, so tokens are cached here by identity
//! rather than per provider: one exchange per identity, subject and scope set,
//! however many clients ask.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use chrono::Utc;

use crate::auth::token::CachedToken;
use crate::error::Result;

/// What a cached token is for. Every part is needed for two holders to share it:
///
/// - `identity`: the credential's non-secret identity and the endpoints it is
///   exchanged at (SA email + key id; WIF audience + impersonated SA; …).
/// - `binding`: a digest of the secret that proves the identity, where one
///   exists (a service-account private key). The identity fields of a key file
///   are not secret, so without this a caller holding only another tenant's
///   email and key id would be handed that tenant's token. In memory only.
/// - `subject`: whose AWS credentials signed a WIF exchange (`host`, or an
///   embedder-supplied principal). Two tenants pasting one WIF config with
///   different AWS subjects are different identities on Google.
/// - `scopes`: sorted, so the order a caller lists them in does not split it.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) struct TokenKey {
    pub(super) identity: String,
    pub(super) binding: Option<[u8; 32]>,
    pub(super) subject: String,
    pub(super) scopes: Vec<String>,
}

/// One key's token. The async lock is held across a refresh, so concurrent
/// callers wait for the one exchange in flight instead of starting their own.
#[derive(Default)]
pub(super) struct TokenSlot {
    held: tokio::sync::Mutex<Held>,
}

/// A slot's token, and when a server's refusal last forced a new one.
#[derive(Default)]
struct Held {
    token: Option<CachedToken>,
    last_forced: Option<Instant>,
}

/// How often a server's refusal may force a slot to mint a new token. A slot
/// can be shared by many sources (a host identity is one slot for the whole
/// process), so without this any one of them could make every 401 re-mint it.
pub(super) const FORCED_REFETCH_INTERVAL: Duration = Duration::from_secs(30);

/// Prune slots nobody holds and whose token has expired once the map grows
/// past this, so a long-lived process that has seen many sources stays small.
const PRUNE_ABOVE: usize = 1024;

fn slots() -> MutexGuard<'static, HashMap<TokenKey, Arc<TokenSlot>>> {
    static SLOTS: OnceLock<Mutex<HashMap<TokenKey, Arc<TokenSlot>>>> = OnceLock::new();
    SLOTS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// The slot for `key`, created on first use.
pub(super) fn slot(key: &TokenKey) -> Arc<TokenSlot> {
    let mut slots = slots();
    if let Some(slot) = slots.get(key) {
        return Arc::clone(slot);
    }
    if slots.len() >= PRUNE_ABOVE {
        slots.retain(|_, slot| {
            Arc::strong_count(slot) > 1
                || slot.held.try_lock().map_or(true, |held| {
                    held.token
                        .as_ref()
                        .is_some_and(|t| Utc::now() < t.expires_at)
                })
        });
    }
    Arc::clone(slots.entry(key.clone()).or_default())
}

impl TokenSlot {
    /// The held token while it is outside the refresh margin, else a fresh one
    /// from `fetch`. A failed refresh reads on with a token that has not
    /// actually expired yet (renewal starts inside the margin), so a blip at
    /// Google does not fail a request the held token can still serve.
    pub(super) async fn get_or_fetch<F, Fut>(&self, fetch: F) -> Result<CachedToken>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<CachedToken>>,
    {
        let mut held = self.held.lock().await;
        if let Some(token) = held.token.as_ref().filter(|t| !t.is_expired()) {
            return Ok(token.clone());
        }
        match fetch().await {
            Ok(token) => {
                held.token = Some(token.clone());
                Ok(token)
            }
            Err(e) => match held.token.as_ref().filter(|t| Utc::now() < t.expires_at) {
                Some(still_valid) => {
                    tracing::warn!(error = %e, "Google token refresh failed; reading on with the held token");
                    Ok(still_valid.clone())
                }
                None => Err(e),
            },
        }
    }

    /// Replace the held token with a fresh one after a server refused it, at
    /// most once per [`FORCED_REFETCH_INTERVAL`]: within that window of the
    /// last forced one the held token is kept (it was itself just re-minted
    /// for a refusal), and the caller's retry reports the server's refusal.
    /// Returns whether a new token was minted.
    pub(super) async fn refetch<F, Fut>(&self, fetch: F) -> Result<bool>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<CachedToken>>,
    {
        let mut held = self.held.lock().await;
        let recently_forced = held
            .last_forced
            .is_some_and(|at| at.elapsed() < FORCED_REFETCH_INTERVAL);
        if held.token.is_some() && recently_forced {
            tracing::debug!(
                "a Google token was re-minted for a refusal moments ago; not minting another"
            );
            return Ok(false);
        }
        held.token = Some(fetch().await?);
        held.last_forced = Some(Instant::now());
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn token() -> CachedToken {
        CachedToken {
            access_token: "t".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
        }
    }

    /// A server's refusal forces at most one new token per window: the first
    /// re-mints (a token revoked right after it was minted is renewed), the
    /// rest keep it until the window passes; an empty slot always mints.
    #[tokio::test]
    async fn a_forced_refetch_happens_at_most_once_per_window() {
        let mints = AtomicUsize::new(0);
        let mint = || async {
            mints.fetch_add(1, Ordering::SeqCst);
            Ok(token())
        };
        let slot = TokenSlot::default();
        slot.get_or_fetch(mint).await.unwrap();
        assert!(
            slot.refetch(mint).await.unwrap(),
            "the first refusal re-mints"
        );
        for _ in 0..3 {
            assert!(!slot.refetch(mint).await.unwrap(), "within the window");
        }
        assert_eq!(mints.load(Ordering::SeqCst), 2);

        slot.held.lock().await.last_forced =
            Instant::now().checked_sub(FORCED_REFETCH_INTERVAL + Duration::from_secs(1));
        assert!(slot.refetch(mint).await.unwrap(), "past the window");
        assert!(!slot.refetch(mint).await.unwrap(), "and a window again");
        assert_eq!(mints.load(Ordering::SeqCst), 3);

        assert!(TokenSlot::default().refetch(mint).await.unwrap());
        assert_eq!(mints.load(Ordering::SeqCst), 4);
    }
}
