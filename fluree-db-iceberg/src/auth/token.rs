//! Shared cached access token with jittered expiry.
//!
//! Used by the refreshing catalog-auth providers ([`OAuth2ClientCredentials`]
//! and [`GoogleMetadataAuth`]) so the expiry/refresh policy lives in one place.
//!
//! [`OAuth2ClientCredentials`]: crate::auth::OAuth2ClientCredentials
//! [`GoogleMetadataAuth`]: crate::auth::GoogleMetadataAuth

use chrono::{DateTime, Duration, Utc};
use rand::Rng;

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
