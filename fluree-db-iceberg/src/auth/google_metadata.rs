//! Google metadata-server token authentication.
//!
//! Mints and refreshes short-lived Google OAuth access tokens from the GCE/GKE
//! instance metadata server — the credential model for a workload running as a
//! GCP service account (GKE Workload Identity). Unlike a static bearer token
//! (which Google issues with a ~1h lifetime and cannot be renewed once set), this
//! provider refreshes internally, so a long-running catalog client — notably the
//! materialization tracking worker polling a Google Iceberg REST catalog
//! (BigLake) — keeps authenticating past the first hour.
//!
//! The metadata server is only reachable from within GCE/GKE; for local runs use
//! a static `bearer` token instead.

use crate::auth::token::CachedToken;
use crate::auth::{CatalogAuth, SendCatalogAuth};
use crate::error::{IcebergError, Result};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use std::sync::Arc;
use tokio::sync::RwLock;

/// GCE/GKE metadata-server token endpoint for the attached (default) service
/// account.
const DEFAULT_METADATA_TOKEN_URL: &str =
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";

/// Default OAuth scope for Google Cloud APIs (covers the BigLake REST catalog).
const DEFAULT_SCOPES: &str = "https://www.googleapis.com/auth/cloud-platform";

/// Configuration for [`GoogleMetadataAuth`].
#[derive(Debug, Clone)]
pub struct GoogleMetadataConfig {
    /// Token endpoint. Defaults to the metadata server; overridable for testing.
    pub metadata_url: String,
    /// OAuth scopes (comma-separated). Defaults to cloud-platform.
    pub scopes: String,
}

impl Default for GoogleMetadataConfig {
    fn default() -> Self {
        Self {
            metadata_url: DEFAULT_METADATA_TOKEN_URL.to_string(),
            scopes: DEFAULT_SCOPES.to_string(),
        }
    }
}

/// Google metadata-server OAuth authentication.
///
/// Acquires tokens from the instance metadata server and refreshes them
/// automatically before expiration (see [`CachedToken`]).
pub struct GoogleMetadataAuth {
    config: GoogleMetadataConfig,
    http_client: reqwest::Client,
    cached_token: Arc<RwLock<Option<CachedToken>>>,
}

impl std::fmt::Debug for GoogleMetadataAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GoogleMetadataAuth")
            .field("metadata_url", &self.config.metadata_url)
            .field("scopes", &self.config.scopes)
            .finish_non_exhaustive()
    }
}

impl GoogleMetadataAuth {
    /// Create a new metadata-server auth provider.
    pub fn new(config: GoogleMetadataConfig) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| IcebergError::Http(format!("Failed to build HTTP client: {e}")))?;

        Ok(Self {
            config,
            http_client,
            cached_token: Arc::new(RwLock::new(None)),
        })
    }

    /// Fetch a fresh access token from the metadata server.
    async fn fetch_token(&self) -> Result<CachedToken> {
        let response = self
            .http_client
            .get(&self.config.metadata_url)
            .query(&[("scopes", self.config.scopes.as_str())])
            // The metadata server requires this header on every request.
            .header("Metadata-Flavor", "Google")
            .send()
            .await
            .map_err(|e| IcebergError::Http(format!("Metadata token request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(IcebergError::Auth(format!(
                "Google metadata token request failed ({status}): {body} \
                 (the metadata server is only reachable on GCE/GKE; use a static bearer locally)"
            )));
        }

        #[derive(serde::Deserialize)]
        struct TokenResponse {
            access_token: String,
            #[serde(default = "default_token_type")]
            token_type: String,
            expires_in: Option<i64>,
        }

        fn default_token_type() -> String {
            "Bearer".to_string()
        }

        let token_resp: TokenResponse = response.json().await.map_err(|e| {
            IcebergError::Auth(format!("Failed to parse metadata token response: {e}"))
        })?;

        // Default to 1 hour if not specified.
        let expires_in = token_resp.expires_in.unwrap_or(3600);
        let expires_at = Utc::now() + Duration::seconds(expires_in);

        tracing::debug!(expires_in = expires_in, "Google metadata token acquired");

        Ok(CachedToken {
            access_token: token_resp.access_token,
            token_type: token_resp.token_type,
            expires_at,
        })
    }

    /// Get a valid token, refreshing if needed.
    async fn get_token(&self) -> Result<CachedToken> {
        // Fast path: a cached, non-expired token.
        {
            let cached = self.cached_token.read().await;
            if let Some(token) = cached.as_ref() {
                if !token.is_expired() {
                    return Ok(token.clone());
                }
            }
        }

        // Slow path: refresh.
        let new_token = self.fetch_token().await?;
        {
            let mut cached = self.cached_token.write().await;
            *cached = Some(new_token.clone());
        }
        Ok(new_token)
    }
}

#[async_trait(?Send)]
impl CatalogAuth for GoogleMetadataAuth {
    async fn authorization_header(&self) -> Result<Option<String>> {
        Ok(Some(self.get_token().await?.authorization_header()))
    }

    async fn refresh(&self) -> Result<()> {
        let new_token = self.fetch_token().await?;
        *self.cached_token.write().await = Some(new_token);
        Ok(())
    }
}

#[async_trait]
impl SendCatalogAuth for GoogleMetadataAuth {
    async fn authorization_header(&self) -> Result<Option<String>> {
        Ok(Some(self.get_token().await?.authorization_header()))
    }

    async fn refresh(&self) -> Result<()> {
        let new_token = self.fetch_token().await?;
        *self.cached_token.write().await = Some(new_token);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn provider(url: String) -> GoogleMetadataAuth {
        GoogleMetadataAuth::new(GoogleMetadataConfig {
            metadata_url: url,
            scopes: DEFAULT_SCOPES.to_string(),
        })
        .unwrap()
    }

    #[tokio::test]
    async fn sends_flavor_header_and_scopes_and_caches() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/token"))
            .and(header("metadata-flavor", "Google"))
            .and(query_param(
                "scopes",
                "https://www.googleapis.com/auth/cloud-platform",
            ))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "application/json")
                    .set_body_string(
                        r#"{"access_token":"ya29.abc","token_type":"Bearer","expires_in":3599}"#,
                    ),
            )
            .mount(&server)
            .await;

        let auth = provider(format!("{}/token", server.uri()));
        let header = SendCatalogAuth::authorization_header(&auth).await.unwrap();
        assert_eq!(header, Some("Bearer ya29.abc".to_string()));

        // A second call reuses the cached (non-expired) token — no re-fetch.
        SendCatalogAuth::authorization_header(&auth).await.unwrap();
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            1,
            "token should be cached, not re-fetched"
        );
    }

    #[tokio::test]
    async fn surfaces_metadata_failure() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(403).set_body_string("Forbidden"))
            .mount(&server)
            .await;

        let auth = provider(format!("{}/token", server.uri()));
        let err = SendCatalogAuth::authorization_header(&auth)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("403"), "got: {err}");
    }
}
