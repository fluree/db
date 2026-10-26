//! Static bearer token authentication.

use crate::auth::{CatalogAuth, SendCatalogAuth};
use crate::error::Result;
use async_trait::async_trait;
use reqwest::header::HeaderMap;

/// Static bearer token authentication.
///
/// Simple auth that returns a fixed token. Token is resolved at construction
/// time (env vars expanded), then used unchanged.
pub struct BearerTokenAuth {
    token: String,
}

/// Redacting `Debug`: the resolved bearer token is a live credential.
impl std::fmt::Debug for BearerTokenAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BearerTokenAuth")
            .field("token", &"***")
            .finish()
    }
}

impl BearerTokenAuth {
    /// Create with a resolved token value.
    pub fn new(token: String) -> Self {
        Self { token }
    }
}

#[async_trait(?Send)]
impl CatalogAuth for BearerTokenAuth {
    async fn request_headers(&self) -> Result<HeaderMap> {
        crate::auth::authorization(&format!("Bearer {}", self.token))
    }

    async fn refresh(&self) -> Result<()> {
        // Static token cannot be refreshed
        Ok(())
    }
}

#[async_trait]
impl SendCatalogAuth for BearerTokenAuth {
    async fn request_headers(&self) -> Result<HeaderMap> {
        crate::auth::authorization(&format!("Bearer {}", self.token))
    }

    async fn refresh(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn authorization(headers: &HeaderMap) -> Option<&str> {
        headers
            .get(reqwest::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
    }

    #[tokio::test]
    async fn test_bearer_auth() {
        let auth = BearerTokenAuth::new("test-token".to_string());
        // Use SendCatalogAuth trait explicitly to avoid ambiguity
        let headers = SendCatalogAuth::request_headers(&auth).await.unwrap();
        assert_eq!(authorization(&headers), Some("Bearer test-token"));
        assert_eq!(headers.len(), 1, "a bearer sends nothing else");
        assert!(headers[reqwest::header::AUTHORIZATION].is_sensitive());
    }

    #[tokio::test]
    async fn test_refresh_is_noop() {
        let auth = BearerTokenAuth::new("test-token".to_string());
        // Should not error - use SendCatalogAuth trait explicitly
        SendCatalogAuth::refresh(&auth).await.unwrap();
        // Token should be unchanged
        let headers = SendCatalogAuth::request_headers(&auth).await.unwrap();
        assert_eq!(authorization(&headers), Some("Bearer test-token"));
    }

    #[tokio::test]
    async fn a_token_that_is_not_a_header_value_is_refused_without_echoing_it() {
        let auth = BearerTokenAuth::new("bad\ntoken-secret".to_string());
        let err = SendCatalogAuth::request_headers(&auth).await.unwrap_err();
        assert!(!err.to_string().contains("token-secret"), "{err}");
    }

    #[test]
    fn debug_redacts_token() {
        let auth = BearerTokenAuth::new("super-secret-token".to_string());
        let dbg = format!("{auth:?}");
        assert!(!dbg.contains("super-secret-token"), "token leaked: {dbg}");
        assert!(dbg.contains("***"));
    }
}
