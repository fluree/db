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
            ("client_secret", self.config.client_secret.clone()),
        ];

        // Only send `client_id` when non-empty. Some catalogs (notably Snowflake
        // Horizon / Polaris for the `session:role:` token exchange) reject the
        // request with `invalid_scope` if a non-empty `client_id` is present, and
        // require it to be omitted entirely.
        if !self.config.client_id.is_empty() {
            form.push(("client_id", self.config.client_id.clone()));
        }

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

    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Parse a `application/x-www-form-urlencoded` body into key/value pairs,
    /// percent-decoding each component so value assertions are encoding-agnostic.
    fn parse_form(body: &[u8]) -> Vec<(String, String)> {
        let s = std::str::from_utf8(body).unwrap();
        s.split('&')
            .filter(|kv| !kv.is_empty())
            .map(|kv| {
                let mut it = kv.splitn(2, '=');
                let k = decode(it.next().unwrap_or(""));
                let v = decode(it.next().unwrap_or(""));
                (k, v)
            })
            .collect()
    }

    /// Minimal `application/x-www-form-urlencoded` component decoder (`+` -> space,
    /// `%XX` -> byte). Sufficient for the ASCII scope/audience values under test.
    fn decode(s: &str) -> String {
        let bytes = s.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            match bytes[i] {
                b'+' => {
                    out.push(b' ');
                    i += 1;
                }
                b'%' if i + 2 < bytes.len() => {
                    let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap();
                    out.push(u8::from_str_radix(hex, 16).unwrap());
                    i += 3;
                }
                b => {
                    out.push(b);
                    i += 1;
                }
            }
        }
        String::from_utf8(out).unwrap()
    }

    async fn mock_token_server() -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/oauth/tokens"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "application/json")
                    .set_body_string(
                        r#"{"access_token":"abc123","token_type":"Bearer","expires_in":3600}"#,
                    ),
            )
            .mount(&server)
            .await;
        server
    }

    #[tokio::test]
    async fn fetch_token_encodes_scope_and_omits_empty_client_id() {
        let server = mock_token_server().await;
        let config = OAuth2Config {
            token_url: format!("{}/v1/oauth/tokens", server.uri()),
            client_id: String::new(), // empty -> must be omitted
            client_secret: "pat-secret".to_string(),
            scope: Some("session:role:ICEBERG_READER".to_string()),
            audience: None,
        };
        let auth = OAuth2ClientCredentials::new(config).unwrap();
        let token = auth.fetch_token().await.unwrap();
        assert_eq!(token.access_token, "abc123");

        let reqs = server.received_requests().await.unwrap();
        assert_eq!(reqs.len(), 1);
        let form = parse_form(&reqs[0].body);

        // (a) scope IS form-encoded in the token request
        assert_eq!(
            form.iter()
                .find(|(k, _)| k == "scope")
                .map(|(_, v)| v.as_str()),
            Some("session:role:ICEBERG_READER")
        );
        // (b) client_id is OMITTED when empty
        assert!(
            !form.iter().any(|(k, _)| k == "client_id"),
            "client_id must be omitted when empty, got: {form:?}"
        );
        // client_secret + grant_type always present
        assert_eq!(
            form.iter()
                .find(|(k, _)| k == "client_secret")
                .map(|(_, v)| v.as_str()),
            Some("pat-secret")
        );
        // (c) audience absent when None
        assert!(!form.iter().any(|(k, _)| k == "audience"));
    }

    #[tokio::test]
    async fn fetch_token_includes_client_id_when_non_empty_and_audience() {
        let server = mock_token_server().await;
        let config = OAuth2Config {
            token_url: format!("{}/v1/oauth/tokens", server.uri()),
            client_id: "my-client".to_string(),
            client_secret: "the-secret".to_string(),
            scope: Some("PRINCIPAL_ROLE:ALL".to_string()),
            audience: Some("polaris".to_string()),
        };
        let auth = OAuth2ClientCredentials::new(config).unwrap();
        auth.fetch_token().await.unwrap();

        let reqs = server.received_requests().await.unwrap();
        assert_eq!(reqs.len(), 1);
        let form = parse_form(&reqs[0].body);

        // (b) client_id is PRESENT when non-empty
        assert_eq!(
            form.iter()
                .find(|(k, _)| k == "client_id")
                .map(|(_, v)| v.as_str()),
            Some("my-client")
        );
        // (c) audience sent when Some
        assert_eq!(
            form.iter()
                .find(|(k, _)| k == "audience")
                .map(|(_, v)| v.as_str()),
            Some("polaris")
        );
        assert_eq!(
            form.iter()
                .find(|(k, _)| k == "scope")
                .map(|(_, v)| v.as_str()),
            Some("PRINCIPAL_ROLE:ALL")
        );
    }
}
