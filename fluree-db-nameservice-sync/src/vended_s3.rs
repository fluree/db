//! Consume vended S3 credentials: fetch a ledger-scoped grant from a remote
//! server's `GET /storage/credentials` endpoint and build an S3 reader from
//! it, refreshing credentials automatically as grants expire.
//!
//! This is the direct-from-S3 fast path for peer mode: the origin hands out
//! short-lived STS credentials scoped to the ledger's prefix, and the
//! consumer reads canonical CAS bytes (native ranged reads included) without
//! proxying every object through the origin's HTTP server. A 404 from the
//! endpoint means vending is unavailable — callers fall back to
//! [`ProxyStorage`](crate::proxy_storage::ProxyStorage).

use aws_credential_types::provider::error::CredentialsError;
use aws_credential_types::provider::{future, ProvideCredentials};
use aws_credential_types::Credentials;
use fluree_db_storage_aws::{S3Config, S3Storage};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

use crate::error::{Result, SyncError};

/// Refresh grants this long before their stated expiry.
const REFRESH_MARGIN: Duration = Duration::from_secs(60);

/// A ledger-scoped S3 grant as served by `GET /storage/credentials`.
///
/// Mirrors the server's `VendedS3Grant` wire shape (fluree-db-api); optional
/// fields default so older servers stay parseable.
#[derive(Debug, Clone, Deserialize)]
pub struct VendedS3Grant {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    /// Grant expiry as Unix epoch seconds.
    pub expires_at_epoch_secs: i64,
    pub bucket: String,
    pub region: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Path-style addressing for `endpoint`. Defaults to unset so a grant
    /// from a server predating the field still parses.
    #[serde(default)]
    pub force_path_style: Option<bool>,
    /// Root key prefix to configure on the S3 reader so address→key mapping
    /// matches the origin.
    #[serde(default)]
    pub key_prefix: Option<String>,
    /// The prefix the grant is scoped to (informational).
    #[serde(default)]
    pub scoped_prefix: Option<String>,
}

impl VendedS3Grant {
    fn expires_at(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(self.expires_at_epoch_secs.max(0) as u64)
    }

    fn needs_refresh(&self) -> bool {
        SystemTime::now() + REFRESH_MARGIN >= self.expires_at()
    }

    fn to_credentials(&self) -> Credentials {
        Credentials::new(
            self.access_key_id.clone(),
            self.secret_access_key.clone(),
            Some(self.session_token.clone()),
            Some(self.expires_at()),
            "fluree-vended-credentials",
        )
    }
}

/// HTTP client for the vended-credentials endpoint.
#[derive(Clone)]
pub struct VendedCredentialsClient {
    client: Client,
    api_base: String,
    token: String,
    ledger: String,
}

impl Debug for VendedCredentialsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VendedCredentialsClient")
            .field("api_base", &self.api_base)
            .field("ledger", &self.ledger)
            .finish_non_exhaustive()
    }
}

impl VendedCredentialsClient {
    /// Create a client for one ledger's grants. `api_base` is the full API
    /// base URL (e.g. `https://data.example.com/v1/fluree`).
    pub fn new(
        api_base: impl Into<String>,
        token: impl Into<String>,
        ledger: impl Into<String>,
    ) -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("Failed to create vended credentials client"),
            api_base: api_base.into().trim_end_matches('/').to_string(),
            token: token.into(),
            ledger: ledger.into(),
        }
    }

    /// Fetch a grant. `Ok(None)` means the server doesn't vend credentials
    /// (endpoint absent, disabled, out of scope, or non-S3 storage) —
    /// callers fall back to proxied block reads.
    pub async fn fetch(&self) -> Result<Option<VendedS3Grant>> {
        let response = self
            .client
            .get(format!("{}/storage/credentials", self.api_base))
            .query(&[("ledger", self.ledger.as_str())])
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| SyncError::Remote(format!("vended credentials request failed: {e}")))?;

        match response.status() {
            StatusCode::OK => {
                let grant: VendedS3Grant = response.json().await.map_err(|e| {
                    SyncError::Remote(format!("vended credentials response parse failed: {e}"))
                })?;
                Ok(Some(grant))
            }
            StatusCode::NOT_FOUND => Ok(None),
            status => Err(SyncError::Remote(format!(
                "vended credentials request rejected: {status}"
            ))),
        }
    }
}

/// AWS credentials provider that refreshes grants from the vending endpoint
/// as they approach expiry.
#[derive(Debug)]
pub struct RefreshingVendedProvider {
    client: VendedCredentialsClient,
    cached: Arc<Mutex<VendedS3Grant>>,
}

impl RefreshingVendedProvider {
    pub fn new(client: VendedCredentialsClient, initial: VendedS3Grant) -> Self {
        Self {
            client,
            cached: Arc::new(Mutex::new(initial)),
        }
    }

    async fn current_credentials(&self) -> std::result::Result<Credentials, CredentialsError> {
        // Holding the lock across the refresh single-flights concurrent
        // refreshes; waiters then read the fresh grant.
        let mut cached = self.cached.lock().await;
        if cached.needs_refresh() {
            match self.client.fetch().await {
                Ok(Some(grant)) => *cached = grant,
                Ok(None) => {
                    return Err(CredentialsError::provider_error(
                        "vending endpoint no longer offers credentials for this ledger",
                    ));
                }
                Err(e) => return Err(CredentialsError::provider_error(e)),
            }
        }
        Ok(cached.to_credentials())
    }
}

impl ProvideCredentials for RefreshingVendedProvider {
    fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        future::ProvideCredentials::new(self.current_credentials())
    }
}

/// Build an S3 reader for one remote ledger from vended credentials.
///
/// Fetches an initial grant (returning `Ok(None)` when the server doesn't
/// vend — fall back to proxied reads), then constructs an
/// [`S3Storage`] whose credentials refresh automatically via
/// [`RefreshingVendedProvider`].
pub async fn build_vended_s3_storage(
    api_base: impl Into<String>,
    token: impl Into<String>,
    ledger: impl Into<String>,
) -> Result<Option<S3Storage>> {
    let client = VendedCredentialsClient::new(api_base, token, ledger);
    let Some(grant) = client.fetch().await? else {
        return Ok(None);
    };

    let s3_config = S3Config {
        bucket: grant.bucket.clone(),
        prefix: grant.key_prefix.clone(),
        endpoint: grant.endpoint.clone(),
        // Travels with `endpoint`: an override without it addresses
        // virtual-hosted, which a plain MinIO rejects.
        force_path_style: grant.force_path_style,
        ..Default::default()
    };
    let region = aws_config::Region::new(grant.region.clone());
    let provider = RefreshingVendedProvider::new(client, grant);

    let sdk_config = aws_config::SdkConfig::builder()
        .credentials_provider(
            aws_credential_types::provider::SharedCredentialsProvider::new(provider),
        )
        .region(region)
        .behavior_version(aws_config::BehaviorVersion::latest())
        .build();

    let storage = S3Storage::new(&sdk_config, s3_config)
        .await
        .map_err(|e| SyncError::Remote(format!("vended S3 storage init failed: {e}")))?;
    Ok(Some(storage))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grant(expires_in_secs: i64) -> VendedS3Grant {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        VendedS3Grant {
            access_key_id: "AKIA".into(),
            secret_access_key: "secret".into(),
            session_token: "token".into(),
            expires_at_epoch_secs: now + expires_in_secs,
            bucket: "b".into(),
            region: "us-east-1".into(),
            endpoint: None,
            force_path_style: None,
            key_prefix: Some("ledgers".into()),
            scoped_prefix: Some("ledgers/inv".into()),
        }
    }

    #[test]
    fn refresh_margin_applies() {
        assert!(grant(30).needs_refresh(), "inside the 60s margin");
        assert!(!grant(600).needs_refresh(), "well before expiry");
        assert!(grant(-10).needs_refresh(), "already expired");
    }

    #[test]
    fn grant_parses_minimal_wire_shape() {
        // Older/newer servers may omit optional fields.
        let json = r#"{
            "access_key_id": "AKIA",
            "secret_access_key": "s",
            "session_token": "t",
            "expires_at_epoch_secs": 1700000000,
            "bucket": "b",
            "region": "us-east-1",
            "scoped_prefix": "inv"
        }"#;
        let grant: VendedS3Grant = serde_json::from_str(json).unwrap();
        assert!(grant.key_prefix.is_none());
        assert!(grant.endpoint.is_none());
    }

    fn grant_json(access_key: &str, expires_in_secs: i64) -> serde_json::Value {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        serde_json::json!({
            "access_key_id": access_key,
            "secret_access_key": "s",
            "session_token": "t",
            "expires_at_epoch_secs": now + expires_in_secs,
            "bucket": "b",
            "region": "us-east-1",
            "scoped_prefix": "ledgers/inv"
        })
    }

    #[tokio::test]
    async fn fetch_returns_none_on_404() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/storage/credentials"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let client = VendedCredentialsClient::new(server.uri(), "tok", "inv:main");
        let result = client.fetch().await.expect("fetch should not error");
        assert!(result.is_none(), "404 means vending unavailable");
    }

    #[tokio::test]
    async fn provider_refreshes_expiring_grant() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/storage/credentials"))
            .and(query_param("ledger", "inv:main"))
            .respond_with(ResponseTemplate::new(200).set_body_json(grant_json("AKIA-FRESH", 900)))
            .expect(1)
            .mount(&server)
            .await;

        let client = VendedCredentialsClient::new(server.uri(), "tok", "inv:main");
        // Seed with a grant inside the refresh margin: the first
        // provide_credentials must refetch.
        let expiring: VendedS3Grant = serde_json::from_value(grant_json("AKIA-STALE", 10)).unwrap();
        let provider = RefreshingVendedProvider::new(client, expiring);

        let creds = provider
            .provide_credentials()
            .await
            .expect("refresh should succeed");
        assert_eq!(creds.access_key_id(), "AKIA-FRESH");

        // A fresh grant is served from cache — the mock's expect(1) verifies
        // no second request happens.
        let creds = provider.provide_credentials().await.expect("cached grant");
        assert_eq!(creds.access_key_id(), "AKIA-FRESH");
    }
}
