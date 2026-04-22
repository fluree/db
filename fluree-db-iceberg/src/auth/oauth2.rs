//! OAuth2 client credentials flow authentication.

use crate::auth::{CatalogAuth, SendCatalogAuth};
use crate::error::{IcebergError, Result};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use rand::Rng;
use std::sync::Arc;
use tokio::sync::RwLock;

/// OAuth2 client credentials configuration.
#[derive(Debug, Clone)]
pub struct OAuth2Config {
    pub token_url: String,
    pub client_id: String,
    pub client_secret: String,
    pub scope: Option<String>,
    pub audience: Option<String>,
}

/// Cached token with expiration.
#[derive(Debug, Clone)]
struct CachedToken {
    access_token: String,
    token_type: String,
    expires_at: DateTime<Utc>,
}

impl CachedToken {
    /// Check if token is expired or will expire within buffer period.
    ///
    /// Uses a 30-second base buffer plus 0-5s jitter to avoid thundering herds.
    fn is_expired(&self) -> bool {
        let jitter = rand::thread_rng().gen_range(0..5);
        let buffer = Duration::seconds(30 + jitter);
        Utc::now() + buffer >= self.expires_at
    }

    /// Get the authorization header value.
    fn authorization_header(&self) -> String {
        // Use token_type from response (don't hardcode "Bearer")
        format!("{} {}", self.token_type, self.access_token)
    }
}

/// OAuth2 client credentials authentication.
///
/// Handles token acquisition and automatic refresh before expiration.
pub struct OAuth2ClientCredentials {
    config: OAuth2Config,
    http_client: reqwest::Client,
    cached_token: Arc<RwLock<Option<CachedToken>>>,
}

impl std::fmt::Debug for OAuth2ClientCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuth2ClientCredentials")
            .field("token_url", &self.config.token_url)
            .field("client_id", &self.config.client_id)
            .finish_non_exhaustive()
    }
}

impl OAuth2ClientCredentials {
    /// Create a new OAuth2 auth provider.
    pub fn new(config: OAuth2Config) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(30))
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .map_err(|e| IcebergError::Http(format!("Failed to build HTTP client: {e}")))?;

        Ok(Self {
            config,
            http_client,
            cached_token: Arc::new(RwLock::new(None)),
        })
    }

    /// Fetch a new access token from the token endpoint.
    async fn fetch_token(&self) -> Result<CachedToken> {
        let mut form = vec![
            ("grant_type", "client_credentials".to_string()),
            ("client_id", self.config.client_id.clone()),
            ("client_secret", self.config.client_secret.clone()),
        ];

        if let Some(scope) = &self.config.scope {
            form.push(("scope", scope.clone()));
        }

        if let Some(audience) = &self.config.audience {
            form.push(("audience", audience.clone()));
        }

        let response = self
            .http_client
            .post(&self.config.token_url)
            .form(&form)
            .send()
            .await
            .map_err(|e| IcebergError::Http(format!("Token request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(IcebergError::Auth(format!(
                "OAuth2 token request failed ({status}): {body}"
            )));
        }

        #[derive(serde::Deserialize)]
        struct TokenResponse {
            access_token: String,
            #[serde(default = "default_token_type")]
            token_type: String,
            expires_in: Option<i64>,
            #[serde(default)]
            scope: Option<String>,
        }

        fn default_token_type() -> String {
            "Bearer".to_string()
        }

        let token_resp: TokenResponse = response
            .json()
            .await
            .map_err(|e| IcebergError::Auth(format!("Failed to parse token response: {e}")))?;

        // Default to 1 hour if not specified
        let expires_in = token_resp.expires_in.unwrap_or(3600);
        let expires_at = Utc::now() + Duration::seconds(expires_in);

        tracing::debug!(
            expires_in = expires_in,
            token_type = %token_resp.token_type,
            scope = ?token_resp.scope,
            "OAuth2 token acquired"
        );

        Ok(CachedToken {
            access_token: token_resp.access_token,
            token_type: token_resp.token_type,
            expires_at,
        })
    }

    /// Get a valid token, refreshing if needed.
    async fn get_token(&self) -> Result<CachedToken> {
        // Fast path: check if we have a valid cached token
        {
            let cached = self.cached_token.read().await;
            if let Some(token) = cached.as_ref() {
                if !token.is_expired() {
                    return Ok(token.clone());
                }
            }
        }

        // Slow path: need to refresh
        let new_token = self.fetch_token().await?;

        {
            let mut cached = self.cached_token.write().await;
            *cached = Some(new_token.clone());
        }

        Ok(new_token)
    }
}

#[async_trait(?Send)]
impl CatalogAuth for OAuth2ClientCredentials {
    async fn authorization_header(&self) -> Result<Option<String>> {
        let token = self.get_token().await?;
        Ok(Some(token.authorization_header()))
    }

    async fn refresh(&self) -> Result<()> {
        let new_token = self.fetch_token().await?;
        let mut cached = self.cached_token.write().await;
        *cached = Some(new_token);
        Ok(())
    }
}

#[async_trait]
impl SendCatalogAuth for OAuth2ClientCredentials {
    async fn authorization_header(&self) -> Result<Option<String>> {
        let token = self.get_token().await?;
        Ok(Some(token.authorization_header()))
    }

    async fn refresh(&self) -> Result<()> {
        let new_token = self.fetch_token().await?;
        let mut cached = self.cached_token.write().await;
        *cached = Some(new_token);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_expiration_check() {
        // Token that expires in 1 hour should not be expired
        let future_token = CachedToken {
            access_token: "token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Utc::now() + Duration::hours(1),
        };
        assert!(!future_token.is_expired());

        // Token that expires in 10 seconds should be expired (within 30s buffer)
        let soon_token = CachedToken {
            access_token: "token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Utc::now() + Duration::seconds(10),
        };
        assert!(soon_token.is_expired());

        // Token that already expired should definitely be expired
        let past_token = CachedToken {
            access_token: "token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Utc::now() - Duration::seconds(10),
        };
        assert!(past_token.is_expired());
    }

    #[test]
    fn test_authorization_header_format() {
        let token = CachedToken {
            access_token: "my-access-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Utc::now() + Duration::hours(1),
        };
        assert_eq!(token.authorization_header(), "Bearer my-access-token");

        // Test with non-Bearer token type
        let custom_token = CachedToken {
            access_token: "my-access-token".to_string(),
            token_type: "MAC".to_string(),
            expires_at: Utc::now() + Duration::hours(1),
        };
        assert_eq!(custom_token.authorization_header(), "MAC my-access-token");
    }
}
