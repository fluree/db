//! Shared cached access token with jittered expiry.
//!
//! Used by the refreshing catalog-auth providers ([`OAuth2ClientCredentials`]
//! and [`GoogleIdentity`]) so the expiry/refresh policy lives in one place.
//!
//! [`OAuth2ClientCredentials`]: crate::auth::OAuth2ClientCredentials
//! [`GoogleIdentity`]: crate::auth::GoogleIdentity

use chrono::{DateTime, Duration, Utc};
use rand::Rng;

/// The longest lifetime a token response is believed for. Token endpoints say
/// an hour; a value past this (or one too large to add to a time) is clamped,
/// so a hostile or broken `expires_in` neither panics nor pins a token for
/// years.
pub(crate) const MAX_TOKEN_LIFETIME_SECS: i64 = 24 * 60 * 60;

/// When a token whose response said `expires_in` seconds expires: clamped to
/// `0..=MAX_TOKEN_LIFETIME_SECS` (a negative lifetime is already expired).
pub(crate) fn expires_at_in(expires_in: i64) -> DateTime<Utc> {
    Utc::now() + Duration::seconds(expires_in.clamp(0, MAX_TOKEN_LIFETIME_SECS))
}

/// A cached access token with its expiry.
#[derive(Clone)]
pub(crate) struct CachedToken {
    pub(crate) access_token: String,
    pub(crate) token_type: String,
    pub(crate) expires_at: DateTime<Utc>,
}

/// Redacting `Debug`: the `access_token` is a live bearer credential, so never
/// leak it via a `{:?}` in a log or error.
impl std::fmt::Debug for CachedToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedToken")
            .field("access_token", &"***")
            .field("token_type", &self.token_type)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

impl CachedToken {
    /// Check if the token is expired or will expire within the buffer period.
    ///
    /// Uses a 30-second base buffer plus 0-5s jitter to avoid thundering herds.
    pub(crate) fn is_expired(&self) -> bool {
        let jitter = rand::thread_rng().gen_range(0..5);
        let buffer = Duration::seconds(30 + jitter);
        Utc::now() + buffer >= self.expires_at
    }

    /// Get the authorization header value (uses the response `token_type`,
    /// rather than hardcoding `Bearer`).
    pub(crate) fn authorization_header(&self) -> String {
        format!("{} {}", self.token_type, self.access_token)
    }
}
