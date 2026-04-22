//! Static bearer token authentication.

use crate::auth::{CatalogAuth, SendCatalogAuth};
use crate::error::Result;
use async_trait::async_trait;

/// Static bearer token authentication.
///
/// Simple auth that returns a fixed token. Token is resolved at construction
/// time (env vars expanded), then used unchanged.
#[derive(Debug)]
pub struct BearerTokenAuth {
    token: String,
}

impl BearerTokenAuth {
    /// Create with a resolved token value.
    pub fn new(token: String) -> Self {
        Self { token }
    }
}

#[async_trait(?Send)]
impl CatalogAuth for BearerTokenAuth {
    async fn authorization_header(&self) -> Result<Option<String>> {
        Ok(Some(format!("Bearer {}", self.token)))
    }

    async fn refresh(&self) -> Result<()> {
        // Static token cannot be refreshed
        Ok(())
    }
}

#[async_trait]
impl SendCatalogAuth for BearerTokenAuth {
    async fn authorization_header(&self) -> Result<Option<String>> {
        Ok(Some(format!("Bearer {}", self.token)))
    }

    async fn refresh(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_bearer_auth() {
        let auth = BearerTokenAuth::new("test-token".to_string());
        // Use SendCatalogAuth trait explicitly to avoid ambiguity
        let header = SendCatalogAuth::authorization_header(&auth).await.unwrap();
        assert_eq!(header, Some("Bearer test-token".to_string()));
    }

    #[tokio::test]
    async fn test_refresh_is_noop() {
        let auth = BearerTokenAuth::new("test-token".to_string());
        // Should not error - use SendCatalogAuth trait explicitly
        SendCatalogAuth::refresh(&auth).await.unwrap();
        // Token should be unchanged
        let header = SendCatalogAuth::authorization_header(&auth).await.unwrap();
        assert_eq!(header, Some("Bearer test-token".to_string()));
    }
}
