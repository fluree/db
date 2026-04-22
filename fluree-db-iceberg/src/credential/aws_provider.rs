//! AWS SDK credential provider backed by vended credentials.

use crate::credential::{CredentialCacheKey, SendCredentialResolver, VendedCredentials};
use aws_credential_types::provider::future::ProvideCredentials as ProvideCredentialsFuture;
use aws_credential_types::provider::ProvideCredentials;
use aws_credential_types::Credentials;
use std::sync::Arc;

/// AWS credential provider that uses vended credentials from an Iceberg catalog.
///
/// This adapter allows using vended credentials with the AWS SDK by implementing
/// the `ProvideCredentials` trait.
///
/// # Example
///
/// ```ignore
/// use fluree_db_iceberg::credential::{
///     VendedAwsCredentialProvider, CredentialCacheKey, SendCredentialResolver,
/// };
/// use aws_sdk_s3::config::Builder as S3ConfigBuilder;
///
/// let resolver: Arc<dyn SendCredentialResolver> = /* ... */;
/// let key = CredentialCacheKey::for_read("https://polaris.example.com", "ns.table");
/// let provider = VendedAwsCredentialProvider::new(key, resolver);
///
/// let s3_config = S3ConfigBuilder::new()
///     .credentials_provider(provider)
///     .build();
/// ```
pub struct VendedAwsCredentialProvider {
    /// Cache key for credential lookup
    key: CredentialCacheKey,
    /// Underlying credential resolver (must be Send + Sync for AWS SDK)
    resolver: Arc<dyn SendCredentialResolver>,
}

impl std::fmt::Debug for VendedAwsCredentialProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VendedAwsCredentialProvider")
            .field("key", &self.key)
            .finish()
    }
}

impl VendedAwsCredentialProvider {
    /// Create a new provider for a specific table.
    pub fn new(key: CredentialCacheKey, resolver: Arc<dyn SendCredentialResolver>) -> Self {
        Self { key, resolver }
    }

    /// Create with catalog URI and table identifier (defaults to Read scope).
    pub fn for_table(
        catalog_uri: impl Into<String>,
        table_identifier: impl Into<String>,
        resolver: Arc<dyn SendCredentialResolver>,
    ) -> Self {
        Self {
            key: CredentialCacheKey::for_read(catalog_uri, table_identifier),
            resolver,
        }
    }
}

impl ProvideCredentials for VendedAwsCredentialProvider {
    fn provide_credentials<'a>(&'a self) -> ProvideCredentialsFuture<'a>
    where
        Self: 'a,
    {
        ProvideCredentialsFuture::new(async move {
            let vended = self
                .resolver
                .resolve(&self.key)
                .await
                .map_err(|e| {
                    aws_credential_types::provider::error::CredentialsError::provider_error(e)
                })?
                .ok_or_else(|| {
                    aws_credential_types::provider::error::CredentialsError::not_loaded(
                        "No vended credentials available for table",
                    )
                })?;

            convert_to_aws_credentials(&vended)
        })
    }
}

/// Convert vended credentials to AWS SDK credentials.
fn convert_to_aws_credentials(
    vended: &VendedCredentials,
) -> std::result::Result<Credentials, aws_credential_types::provider::error::CredentialsError> {
    let expiration = vended.expires_at.map(|dt| {
        std::time::SystemTime::UNIX_EPOCH
            + std::time::Duration::from_millis(dt.timestamp_millis() as u64)
    });

    Ok(Credentials::new(
        &vended.access_key_id,
        &vended.secret_access_key,
        vended.session_token.clone(),
        expiration,
        "iceberg-vended",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential::VendedCredentialCache;
    use crate::error::Result;
    use chrono::{Duration, Utc};

    /// Test resolver that returns static credentials.
    /// Implements SendCredentialResolver (with Send + Sync) for AWS SDK compatibility.
    #[derive(Debug)]
    struct TestResolver {
        cache: VendedCredentialCache,
    }

    impl TestResolver {
        fn new() -> Self {
            Self {
                cache: VendedCredentialCache::new(),
            }
        }

        async fn set_credentials(&self, key: CredentialCacheKey, creds: VendedCredentials) {
            self.cache.put(key, creds).await;
        }
    }

    #[async_trait::async_trait]
    impl SendCredentialResolver for TestResolver {
        async fn resolve(&self, key: &CredentialCacheKey) -> Result<Option<VendedCredentials>> {
            Ok(self.cache.get(key).await)
        }

        fn invalidate(&self, _key: &CredentialCacheKey) {
            // No-op for test
        }
    }

    #[tokio::test]
    async fn test_convert_credentials() {
        let vended = VendedCredentials {
            access_key_id: "AKIATEST".to_string(),
            secret_access_key: "secret123".to_string(),
            session_token: Some("session456".to_string()),
            expires_at: Some(Utc::now() + Duration::hours(1)),
            endpoint: None,
            region: None,
            path_style: false,
        };

        let aws_creds = convert_to_aws_credentials(&vended).unwrap();
        assert_eq!(aws_creds.access_key_id(), "AKIATEST");
        assert_eq!(aws_creds.secret_access_key(), "secret123");
        assert_eq!(aws_creds.session_token(), Some("session456"));
        assert!(aws_creds.expiry().is_some());
    }

    #[tokio::test]
    async fn test_provider_resolves_credentials() {
        let resolver = Arc::new(TestResolver::new());
        let key = CredentialCacheKey::for_read("https://polaris.example.com", "ns.table");

        let creds = VendedCredentials {
            access_key_id: "AKIATEST".to_string(),
            secret_access_key: "secret123".to_string(),
            session_token: None,
            expires_at: Some(Utc::now() + Duration::hours(1)),
            endpoint: None,
            region: None,
            path_style: false,
        };

        resolver.set_credentials(key.clone(), creds).await;

        let provider = VendedAwsCredentialProvider::new(key, resolver);
        let aws_creds = provider.provide_credentials().await.unwrap();

        assert_eq!(aws_creds.access_key_id(), "AKIATEST");
    }
}
