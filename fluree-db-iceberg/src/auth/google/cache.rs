//! The process-wide Google token cache, refreshed single-flight.
//!
//! Catalog clients are cached per source and rebuilt on a TTL, and several
//! sources can share one identity, so tokens are cached here by identity
//! rather than per provider: one exchange per identity, subject and scope set,
//! however many clients ask.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

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
    held: tokio::sync::Mutex<Option<CachedToken>>,
}

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
                    held.as_ref().is_some_and(|t| Utc::now() < t.expires_at)
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
        if let Some(token) = held.as_ref().filter(|t| !t.is_expired()) {
            return Ok(token.clone());
        }
        match fetch().await {
            Ok(token) => {
                *held = Some(token.clone());
                Ok(token)
            }
            Err(e) => match held.as_ref().filter(|t| Utc::now() < t.expires_at) {
                Some(still_valid) => {
                    tracing::warn!(error = %e, "Google token refresh failed; reading on with the held token");
                    Ok(still_valid.clone())
                }
                None => Err(e),
            },
        }
    }

    /// Replace the held token with a fresh one (after the server refused it).
    pub(super) async fn refetch<F, Fut>(&self, fetch: F) -> Result<()>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<CachedToken>>,
    {
        let mut held = self.held.lock().await;
        *held = Some(fetch().await?);
        Ok(())
    }
}
