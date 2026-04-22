//! Authentication module for Iceberg REST catalogs.
//!
//! This module provides authentication providers for REST catalog requests.
//! The core trait [`CatalogAuth`] is runtime-agnostic (no `Send + Sync` requirement)
//! to maintain compatibility with WASM and other async runtimes.
//!
//! # Implementations
//!
//! - [`BearerTokenAuth`] - Static bearer token authentication
//! - [`OAuth2ClientCredentials`] - OAuth2 client credentials flow with token caching

mod bearer;
mod oauth2;

pub use bearer::BearerTokenAuth;
pub use oauth2::{OAuth2ClientCredentials, OAuth2Config};

use crate::config_value::ConfigValue;
use crate::error::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Authentication provider for REST catalog requests.
///
/// Implementations handle obtaining and refreshing authentication tokens.
///
/// Note: `Send + Sync` bounds are intentionally NOT required at the trait level
/// to keep the core runtime/WASM-friendly. Apply bounds at integration points
/// (e.g., `Arc<dyn CatalogAuth + Send + Sync>` in server code) as needed.
#[async_trait(?Send)]
pub trait CatalogAuth: Debug {
    /// Get the current authorization header value.
    ///
    /// Returns `Some("Bearer <token>")` for bearer auth, or `None` if no auth needed.
    /// Implementations should handle token refresh internally.
    async fn authorization_header(&self) -> Result<Option<String>>;

    /// Force a token refresh (e.g., after a 401 response).
    async fn refresh(&self) -> Result<()>;
}

/// Send-safe authentication provider trait.
///
/// This trait mirrors [`CatalogAuth`] but requires `Send + Sync` and produces
/// `Send` futures. Use this for server-side code that needs to spawn tasks.
#[async_trait]
pub trait SendCatalogAuth: Debug + Send + Sync {
    /// Get the current authorization header value.
    async fn authorization_header(&self) -> Result<Option<String>>;

    /// Force a token refresh.
    async fn refresh(&self) -> Result<()>;
}

/// Configuration for catalog authentication.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[derive(Default)]
pub enum AuthConfig {
    /// No authentication required
    #[default]
    None,
    /// Static bearer token
    Bearer {
        /// The bearer token value (can be literal or ConfigValue for env vars)
        token: ConfigValue,
    },
    /// OAuth2 client credentials flow
    #[serde(rename = "oauth2_client_credentials")]
    OAuth2ClientCredentials {
        /// Token endpoint URL
        token_url: String,
        /// Client ID (can be ConfigValue for env var injection)
        client_id: ConfigValue,
        /// Client secret (can be ConfigValue for env vars)
        client_secret: ConfigValue,
        /// Optional scope
        #[serde(default)]
        scope: Option<String>,
        /// Optional audience
        #[serde(default)]
        audience: Option<String>,
    },
}

impl AuthConfig {
    /// Create the appropriate auth provider from this config.
    pub fn create_provider(&self) -> Result<Box<dyn CatalogAuth>> {
        match self {
            AuthConfig::None => Ok(Box::new(NoAuth)),
            AuthConfig::Bearer { token } => {
                let resolved = token.resolve()?;
                Ok(Box::new(BearerTokenAuth::new(resolved)))
            }
            AuthConfig::OAuth2ClientCredentials {
                token_url,
                client_id,
                client_secret,
                scope,
                audience,
            } => {
                let config = OAuth2Config {
                    token_url: token_url.clone(),
                    client_id: client_id.resolve()?,
                    client_secret: client_secret.resolve()?,
                    scope: scope.clone(),
                    audience: audience.clone(),
                };
                Ok(Box::new(OAuth2ClientCredentials::new(config)?))
            }
        }
    }

    /// Create the appropriate auth provider as an Arc with Send-safe trait.
    ///
    /// This is used when the provider needs to be shared across threads
    /// and produce Send futures, such as with RestCatalogClient.
    pub fn create_provider_arc(&self) -> Result<std::sync::Arc<dyn SendCatalogAuth>> {
        match self {
            AuthConfig::None => Ok(std::sync::Arc::new(NoAuth)),
            AuthConfig::Bearer { token } => {
                let resolved = token.resolve()?;
                Ok(std::sync::Arc::new(BearerTokenAuth::new(resolved)))
            }
            AuthConfig::OAuth2ClientCredentials {
                token_url,
                client_id,
                client_secret,
                scope,
                audience,
            } => {
                let config = OAuth2Config {
                    token_url: token_url.clone(),
                    client_id: client_id.resolve()?,
                    client_secret: client_secret.resolve()?,
                    scope: scope.clone(),
                    audience: audience.clone(),
                };
                Ok(std::sync::Arc::new(OAuth2ClientCredentials::new(config)?))
            }
        }
    }
}

/// No-op authentication (for testing or public catalogs).
#[derive(Debug)]
pub struct NoAuth;

#[async_trait(?Send)]
impl CatalogAuth for NoAuth {
    async fn authorization_header(&self) -> Result<Option<String>> {
        Ok(None)
    }

    async fn refresh(&self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl SendCatalogAuth for NoAuth {
    async fn authorization_header(&self) -> Result<Option<String>> {
        Ok(None)
    }

    async fn refresh(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_none_auth() {
        let json = r#"{"type": "none"}"#;
        let config: AuthConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(config, AuthConfig::None));
    }

    #[test]
    fn test_parse_bearer_auth() {
        let json = r#"{"type": "bearer", "token": "my-token"}"#;
        let config: AuthConfig = serde_json::from_str(json).unwrap();
        match config {
            AuthConfig::Bearer { token } => {
                assert_eq!(token.resolve().unwrap(), "my-token");
            }
            _ => panic!("Expected bearer auth"),
        }
    }

    #[test]
    fn test_parse_oauth2_auth() {
        let json = r#"{
            "type": "oauth2_client_credentials",
            "token_url": "https://auth.example.com/token",
            "client_id": "my-client",
            "client_secret": "my-secret",
            "scope": "PRINCIPAL_ROLE:ALL"
        }"#;
        let config: AuthConfig = serde_json::from_str(json).unwrap();
        match config {
            AuthConfig::OAuth2ClientCredentials {
                token_url,
                client_id,
                client_secret,
                scope,
                ..
            } => {
                assert_eq!(token_url, "https://auth.example.com/token");
                assert_eq!(client_id.resolve().unwrap(), "my-client");
                assert_eq!(client_secret.resolve().unwrap(), "my-secret");
                assert_eq!(scope, Some("PRINCIPAL_ROLE:ALL".to_string()));
            }
            _ => panic!("Expected OAuth2 auth"),
        }
    }
}
