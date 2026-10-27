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
//! - [`GoogleCatalogAuth`] - a [`GoogleIdentity`] (metadata server, service
//!   account, Workload Identity Federation, or host ADC), refreshed
//!   automatically and carrying the quota project — for Google Iceberg REST
//!   catalogs (BigLake), where a static bearer would expire

mod bearer;
pub mod google;
mod oauth2;
mod token;

pub use bearer::BearerTokenAuth;
pub use google::{
    AccessToken, ExternalAccountConfig, GoogleCatalogAuth, GoogleCredentials, GoogleIdentity,
    GoogleSource,
};
#[cfg(feature = "aws")]
pub use google::{AwsCredentials, AwsSubjectCredentials};
pub use oauth2::{OAuth2ClientCredentials, OAuth2Config};

use crate::config_value::{ConfigValue, SecretResolver};
use crate::error::Result;
use crate::host_identity::HostIdentityPolicy;
use async_trait::async_trait;
use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;

/// Authentication provider for REST catalog requests.
///
/// Implementations handle obtaining and refreshing authentication tokens.
///
/// Note: `Send + Sync` bounds are intentionally NOT required at the trait level
/// to keep the core runtime/WASM-friendly. Apply bounds at integration points
/// (e.g., `Arc<dyn CatalogAuth + Send + Sync>` in server code) as needed.
#[async_trait(?Send)]
pub trait CatalogAuth: Debug {
    /// The headers every request carries: `Authorization` (when the provider
    /// authenticates at all) plus any provider headers, e.g. Google's
    /// `x-goog-user-project`. One method rather than a separate extra-headers
    /// hook, so no caller can send the token and drop the rest.
    ///
    /// Implementations should handle token refresh internally.
    async fn request_headers(&self) -> Result<HeaderMap>;

    /// Force a token refresh (e.g., after a 401 response).
    async fn refresh(&self) -> Result<()>;
}

/// Send-safe authentication provider trait.
///
/// This trait mirrors [`CatalogAuth`] but requires `Send + Sync` and produces
/// `Send` futures. Use this for server-side code that needs to spawn tasks.
#[async_trait]
pub trait SendCatalogAuth: Debug + Send + Sync {
    /// The headers every request carries; see [`CatalogAuth::request_headers`].
    async fn request_headers(&self) -> Result<HeaderMap>;

    /// Force a token refresh.
    async fn refresh(&self) -> Result<()>;
}

/// What a provider is built with, beyond its stored config: the host's secret
/// resolver, its host-identity policy, and the AWS subject that signs Workload
/// Identity Federation exchanges. Built by the API layer (one per `Fluree`
/// instance, cloned per tenant) and passed wherever a provider is built.
#[derive(Clone, Debug, Default)]
pub struct IdentityContext {
    /// Resolves `ConfigValue::SecretRef` references; `None` fails them closed.
    pub secret_resolver: Option<Arc<dyn SecretResolver>>,
    /// Which host-derived identities sources may use.
    pub policy: HostIdentityPolicy,
    /// The AWS identity that signs WIF subject tokens. `None` is the host's
    /// own aws-config chain, a host identity the policy governs.
    #[cfg(feature = "aws")]
    pub aws_subject: Option<Arc<dyn AwsSubjectCredentials>>,
}

impl IdentityContext {
    /// A stable, non-secret rendering of what this context changes about a
    /// provider built under it (the AWS subject and the policy), for keys of
    /// caches holding built clients: a client built for one tenant's subject,
    /// or under a looser policy, must not serve another context.
    pub fn cache_scope(&self) -> String {
        #[cfg(feature = "aws")]
        let subject = match &self.aws_subject {
            Some(subject) => format!("embedder:{}", subject.principal_key()),
            None => "host".to_string(),
        };
        #[cfg(not(feature = "aws"))]
        let subject = "host".to_string();
        format!("subject={subject};policy={}", self.policy.cache_scope())
    }
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
    /// Google metadata-server OAuth (GKE Workload Identity / GCE): the stored
    /// form that predates [`AuthConfig::Google`], kept so existing records
    /// read and round-trip unchanged. Served by the same Google identity as
    /// `{"type": "google", "credentials": {"source": "metadata"}}`.
    #[serde(rename = "google_metadata")]
    GoogleMetadata {
        /// OAuth scopes (comma-separated). Defaults to cloud-platform.
        #[serde(default)]
        scopes: Option<String>,
        /// Must be absent or this instance's own metadata URL: the URL is
        /// pinned, and only the operator can override it
        /// (`FLUREE_GOOGLE_METADATA_URL`).
        #[serde(default)]
        metadata_url: Option<String>,
    },
    /// A Google identity (see [`GoogleCredentials`]), sent as a bearer token
    /// plus `x-goog-user-project` when there is a quota project.
    Google {
        /// Where the credential comes from.
        credentials: GoogleCredentials,
        /// OAuth scopes (comma-separated). Defaults to cloud-platform, which
        /// BigLake accepts (as does `bigquery`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scopes: Option<String>,
        /// The project requests bill to. Wins over the credential JSON's
        /// `quota_project_id`. Needed for user credentials (ADC), which
        /// Google refuses without one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        quota_project: Option<String>,
    },
}

impl AuthConfig {
    /// Create the appropriate auth provider from this config.
    pub fn create_provider(&self, ctx: &IdentityContext) -> Result<Box<dyn CatalogAuth>> {
        match self {
            AuthConfig::None => Ok(Box::new(NoAuth)),
            AuthConfig::Bearer { token } => {
                let resolved = token.resolve()?;
                Ok(Box::new(BearerTokenAuth::new(resolved)))
            }
            AuthConfig::OAuth2ClientCredentials { .. } => Ok(Box::new(
                OAuth2ClientCredentials::new(self.oauth2_config()?)?,
            )),
            AuthConfig::GoogleMetadata { .. } | AuthConfig::Google { .. } => Ok(Box::new(
                self.google_auth(ctx, google::GoogleEndpoints::process())?,
            )),
        }
    }

    /// Create the appropriate auth provider as an Arc with Send-safe trait.
    ///
    /// This is used when the provider needs to be shared across threads
    /// and produce Send futures, such as with RestCatalogClient. `ctx` carries
    /// the instance's host-identity policy and AWS subject; a host identity
    /// the policy refuses is refused here, before any credential is read.
    pub fn create_provider_arc(&self, ctx: &IdentityContext) -> Result<Arc<dyn SendCatalogAuth>> {
        self.create_provider_arc_with(ctx, google::GoogleEndpoints::process())
    }

    pub(crate) fn create_provider_arc_with(
        &self,
        ctx: &IdentityContext,
        endpoints: &google::GoogleEndpoints,
    ) -> Result<Arc<dyn SendCatalogAuth>> {
        match self {
            AuthConfig::None => Ok(Arc::new(NoAuth)),
            AuthConfig::Bearer { token } => {
                let resolved = token.resolve()?;
                Ok(Arc::new(BearerTokenAuth::new(resolved)))
            }
            AuthConfig::OAuth2ClientCredentials { .. } => Ok(Arc::new(
                OAuth2ClientCredentials::new(self.oauth2_config()?)?,
            )),
            AuthConfig::GoogleMetadata { .. } | AuthConfig::Google { .. } => {
                Ok(Arc::new(self.google_auth(ctx, endpoints)?))
            }
        }
    }

    /// The Google identity a `google` or `google_metadata` config authenticates
    /// as, built under `ctx` (its host-identity policy applies); `None` for the
    /// other variants. The config must be hydrated. No network I/O.
    ///
    /// This is the identity a source with `io.storage_auth = catalog` reads its
    /// `gs://` files as: the catalog's own, as Google's model for BigLake
    /// federation has it.
    pub fn google_identity(&self, ctx: &IdentityContext) -> Result<Option<GoogleIdentity>> {
        match self {
            AuthConfig::GoogleMetadata { .. } | AuthConfig::Google { .. } => self
                .google_identity_with(ctx, google::GoogleEndpoints::process())
                .map(|(identity, _)| Some(identity)),
            AuthConfig::None
            | AuthConfig::Bearer { .. }
            | AuthConfig::OAuth2ClientCredentials { .. } => Ok(None),
        }
    }

    /// The Google provider for the two Google variants.
    fn google_auth(
        &self,
        ctx: &IdentityContext,
        endpoints: &google::GoogleEndpoints,
    ) -> Result<GoogleCatalogAuth> {
        let (identity, scopes) = self.google_identity_with(ctx, endpoints)?;
        Ok(GoogleCatalogAuth::new(Arc::new(identity), scopes))
    }

    /// The identity and scopes of the two Google variants.
    fn google_identity_with(
        &self,
        ctx: &IdentityContext,
        endpoints: &google::GoogleEndpoints,
    ) -> Result<(GoogleIdentity, Vec<String>)> {
        let (credentials, scopes, quota_project) = match self {
            AuthConfig::GoogleMetadata {
                scopes,
                metadata_url,
            } => {
                google::check_stored_metadata_url(metadata_url.as_deref(), endpoints)?;
                (&GoogleCredentials::Metadata, scopes, None)
            }
            AuthConfig::Google {
                credentials,
                scopes,
                quota_project,
            } => (credentials, scopes, quota_project.as_deref()),
            AuthConfig::None
            | AuthConfig::Bearer { .. }
            | AuthConfig::OAuth2ClientCredentials { .. } => {
                return Err(crate::error::IcebergError::Config(
                    "not a Google auth config".to_string(),
                ))
            }
        };
        let scopes = google::parse_scopes(scopes.as_deref())?;
        let identity = GoogleIdentity::with_endpoints(credentials, quota_project, ctx, endpoints)?;
        Ok((identity, scopes))
    }

    fn oauth2_config(&self) -> Result<OAuth2Config> {
        match self {
            AuthConfig::OAuth2ClientCredentials {
                token_url,
                client_id,
                client_secret,
                scope,
                audience,
            } => Ok(OAuth2Config {
                token_url: token_url.clone(),
                client_id: client_id.resolve()?,
                client_secret: client_secret.resolve()?,
                scope: scope.clone(),
                audience: audience.clone(),
            }),
            _ => Err(crate::error::IcebergError::Config(
                "not an OAuth2 client-credentials config".to_string(),
            )),
        }
    }

    /// Resolve every [`ConfigValue::SecretRef`] auth field via `resolver`,
    /// returning a config whose secret-bearing values are all literal (or
    /// env-var / `None`) and therefore safe to hand to the SYNCHRONOUS
    /// [`create_provider`](Self::create_provider) /
    /// [`create_provider_arc`](Self::create_provider_arc).
    ///
    /// This is the single async step in the auth pipeline; call it once (with the
    /// host's injected resolver, [`IdentityContext::secret_resolver`]) before
    /// building a provider. Fields carrying no secret reference clone through
    /// untouched, and a `None` resolver only errors when a `SecretRef` is
    /// actually present (fail closed).
    pub async fn hydrate(&self, resolver: Option<&Arc<dyn SecretResolver>>) -> Result<AuthConfig> {
        match self {
            AuthConfig::None => Ok(AuthConfig::None),
            AuthConfig::Bearer { token } => Ok(AuthConfig::Bearer {
                token: token.hydrate(resolver).await?,
            }),
            AuthConfig::OAuth2ClientCredentials {
                token_url,
                client_id,
                client_secret,
                scope,
                audience,
            } => Ok(AuthConfig::OAuth2ClientCredentials {
                token_url: token_url.clone(),
                client_id: client_id.hydrate(resolver).await?,
                client_secret: client_secret.hydrate(resolver).await?,
                scope: scope.clone(),
                audience: audience.clone(),
            }),
            // No secret material to hydrate: the credential is minted per call from
            // the instance metadata server and lives only in an in-memory
            // `CachedToken`. Written out rather than matched by a wildcard so a NEW
            // variant still fails compilation here and earns an explicit decision.
            AuthConfig::GoogleMetadata {
                scopes,
                metadata_url,
            } => Ok(AuthConfig::GoogleMetadata {
                scopes: scopes.clone(),
                metadata_url: metadata_url.clone(),
            }),
            // A service-account key (or a WIF config held behind a reference)
            // may be a secret reference.
            AuthConfig::Google {
                credentials,
                scopes,
                quota_project,
            } => Ok(AuthConfig::Google {
                credentials: credentials.hydrate(resolver).await?,
                scopes: scopes.clone(),
                quota_project: quota_project.clone(),
            }),
        }
    }
}

/// No-op authentication (for testing or public catalogs).
#[derive(Debug)]
pub struct NoAuth;

#[async_trait(?Send)]
impl CatalogAuth for NoAuth {
    async fn request_headers(&self) -> Result<HeaderMap> {
        Ok(HeaderMap::new())
    }

    async fn refresh(&self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl SendCatalogAuth for NoAuth {
    async fn request_headers(&self) -> Result<HeaderMap> {
        Ok(HeaderMap::new())
    }

    async fn refresh(&self) -> Result<()> {
        Ok(())
    }
}

/// `Authorization: <value>`, marked sensitive so no HTTP-layer debug output
/// shows it.
pub(crate) fn authorization(value: &str) -> Result<HeaderMap> {
    let mut header = reqwest::header::HeaderValue::from_str(value).map_err(|_| {
        crate::error::IcebergError::Auth(
            "the catalog credential is not a valid HTTP header value".to_string(),
        )
    })?;
    header.set_sensitive(true);
    let mut headers = HeaderMap::new();
    headers.insert(reqwest::header::AUTHORIZATION, header);
    Ok(headers)
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
    fn test_parse_google_metadata_auth() {
        // Minimal form (no overrides) — defaults to the metadata server + cloud-platform.
        let config: AuthConfig = serde_json::from_str(r#"{"type": "google_metadata"}"#).unwrap();
        assert!(matches!(
            config,
            AuthConfig::GoogleMetadata {
                scopes: None,
                metadata_url: None
            }
        ));
        // Provider construction succeeds (no network I/O at build time).
        assert!(config
            .create_provider_arc(&IdentityContext::default())
            .is_ok());
    }

    #[test]
    fn stored_google_metadata_records_round_trip_byte_identically() {
        // As this variant has always serialized (no skip_serializing_if), and
        // the minimal form records written by hand carry.
        for stored in [
            r#"{"type":"google_metadata","scopes":null,"metadata_url":null}"#,
            r#"{"type":"google_metadata","scopes":"https://www.googleapis.com/auth/bigquery","metadata_url":null}"#,
            r#"{"type":"google_metadata","scopes":null,"metadata_url":"http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"}"#,
        ] {
            let parsed: AuthConfig = serde_json::from_str(stored).unwrap();
            assert!(matches!(parsed, AuthConfig::GoogleMetadata { .. }));
            assert_eq!(serde_json::to_string(&parsed).unwrap(), stored);
        }
    }

    #[test]
    fn google_auth_parses_and_round_trips() {
        for stored in [
            r#"{"type":"google","credentials":{"source":"metadata"}}"#,
            r#"{"type":"google","credentials":{"source":"application_default"},"quota_project":"lake-project"}"#,
            r#"{"type":"google","credentials":{"source":"service_account","key":{"secret_ref":"vault://sa"}},"scopes":"https://www.googleapis.com/auth/bigquery"}"#,
        ] {
            let parsed: AuthConfig = serde_json::from_str(stored).unwrap();
            assert!(matches!(parsed, AuthConfig::Google { .. }));
            assert_eq!(serde_json::to_string(&parsed).unwrap(), stored);
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

    #[derive(Debug)]
    struct StubResolver;

    #[async_trait]
    impl SecretResolver for StubResolver {
        async fn resolve_secret(
            &self,
            secret_ref: &str,
        ) -> std::result::Result<String, crate::config_value::SecretResolveError> {
            Ok(format!("resolved:{secret_ref}"))
        }
    }

    #[tokio::test]
    async fn hydrate_oauth2_resolves_secret_ref_client_secret() {
        let resolver: Arc<dyn SecretResolver> = Arc::new(StubResolver);
        let auth = AuthConfig::OAuth2ClientCredentials {
            token_url: "https://c.example.com/token".to_string(),
            client_id: ConfigValue::literal("svc-client"),
            client_secret: ConfigValue::SecretRef {
                secret_ref: "vault://cs".to_string(),
            },
            scope: Some("session:role:ANALYST".to_string()),
            audience: None,
        };
        let hydrated = auth.hydrate(Some(&resolver)).await.unwrap();
        match hydrated {
            AuthConfig::OAuth2ClientCredentials {
                client_id,
                client_secret,
                scope,
                ..
            } => {
                // client_id (no ref) untouched; client_secret ref resolved.
                assert_eq!(client_id.resolve().unwrap(), "svc-client");
                assert_eq!(client_secret.resolve().unwrap(), "resolved:vault://cs");
                assert_eq!(scope, Some("session:role:ANALYST".to_string()));
            }
            _ => panic!("expected OAuth2 auth"),
        }
    }

    #[tokio::test]
    async fn hydrate_google_resolves_a_secret_ref_key() {
        let resolver: Arc<dyn SecretResolver> = Arc::new(StubResolver);
        let auth = AuthConfig::Google {
            credentials: GoogleCredentials::ServiceAccount {
                key: ConfigValue::SecretRef {
                    secret_ref: "vault://sa".to_string(),
                },
            },
            scopes: None,
            quota_project: Some("p".to_string()),
        };
        match auth.hydrate(Some(&resolver)).await.unwrap() {
            AuthConfig::Google {
                credentials: GoogleCredentials::ServiceAccount { key },
                quota_project,
                ..
            } => {
                assert_eq!(key.resolve().unwrap(), "resolved:vault://sa");
                assert_eq!(quota_project.as_deref(), Some("p"));
            }
            other => panic!("expected Google auth, got {other:?}"),
        }
        assert!(
            auth.hydrate(None).await.is_err(),
            "no resolver fails closed"
        );
    }

    #[tokio::test]
    async fn hydrate_bearer_secret_ref_without_resolver_fails_closed() {
        let auth = AuthConfig::Bearer {
            token: ConfigValue::SecretRef {
                secret_ref: "vault://tok".to_string(),
            },
        };
        assert!(auth.hydrate(None).await.is_err());
    }

    #[test]
    fn identity_context_cache_scope_tracks_the_policy() {
        let open = IdentityContext::default();
        let closed = IdentityContext {
            policy: HostIdentityPolicy::deny_all(),
            ..IdentityContext::default()
        };
        assert_ne!(open.cache_scope(), closed.cache_scope());
    }
}
