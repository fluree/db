//! One Google identity behind every Google-authenticated request a graph
//! source makes: the Iceberg REST catalog (BigLake), and the SQL and Delta
//! sources that share [`AuthConfig`](crate::auth::AuthConfig).
//!
//! Hand-rolled over the codebase's own token cache and hardened HTTP client
//! rather than `google-cloud-auth`: credential JSON comes from tenants on a
//! hosted stack, and Google's formats can name arbitrary token endpoints, local
//! files, URLs and executables. A library follows what it is given; this
//! accepts a validated subset and refuses the rest:
//!
//! - **metadata**: the GCE/GKE metadata server, at a pinned URL;
//! - **service_account**: an RS256 JWT assertion at `oauth2.googleapis.com`;
//! - **external_account**: Workload Identity Federation from AWS: a SigV4-signed
//!   GetCallerIdentity at `sts.googleapis.com`, then optionally
//!   `iamcredentials.googleapis.com` impersonation;
//! - **application_default**: the host's gcloud ADC file.
//!
//! Metadata, ADC and host-signed WIF are host identities, gated by the
//! instance's [`HostIdentityPolicy`](crate::host_identity::HostIdentityPolicy).
//! Signing (service-account JWTs, WIF subject tokens) needs the `aws` feature,
//! which every shipped build enables; without it those sources refuse.

mod adc;
#[cfg(feature = "aws")]
mod aws_subject;
mod cache;
mod credentials;
mod external_account;
mod metadata;
mod service_account;
mod wire;

#[cfg(feature = "aws")]
pub use aws_subject::AwsSubjectCredentials;
pub use credentials::{ExternalAccountConfig, GoogleCredentials};

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION};

use crate::auth::token::CachedToken;
use crate::auth::{CatalogAuth, IdentityContext, SendCatalogAuth};
use crate::error::{IcebergError, Result};
use crate::host_identity::HostIdentityKind;

/// The scope Google catalog auth asks for by default (BigLake accepts it, or
/// `bigquery`).
pub const CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

/// Operator override for the metadata server's token URL, for tests and
/// unusual deployments. Never settable per source.
pub const METADATA_URL_ENV: &str = "FLUREE_GOOGLE_METADATA_URL";

/// Google's OAuth token endpoint (service-account assertions, refresh tokens).
pub(crate) const OAUTH2_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
/// Google's Security Token Service (Workload Identity Federation).
pub(crate) const STS_TOKEN_URL: &str = "https://sts.googleapis.com/v1/token";
/// Google's IAM credentials API (service-account impersonation).
const IAM_CREDENTIALS_URL: &str = "https://iamcredentials.googleapis.com";
/// The metadata server's token URL for the attached service account.
pub(crate) const METADATA_TOKEN_URL: &str =
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";

const QUOTA_PROJECT_HEADER: &str = "x-goog-user-project";

/// Where token requests go. Production values are pinned; a test points them
/// at a mock (they are not settable from any config).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GoogleEndpoints {
    pub(crate) oauth2_token: String,
    pub(crate) sts_token: String,
    pub(crate) iam_credentials: String,
    pub(crate) metadata_token: String,
}

impl GoogleEndpoints {
    /// This process's endpoints: Google's, with the metadata URL from
    /// [`METADATA_URL_ENV`] when the operator set it (read once).
    pub(crate) fn process() -> &'static GoogleEndpoints {
        static ENDPOINTS: OnceLock<GoogleEndpoints> = OnceLock::new();
        ENDPOINTS.get_or_init(|| GoogleEndpoints {
            oauth2_token: OAUTH2_TOKEN_URL.to_string(),
            sts_token: STS_TOKEN_URL.to_string(),
            iam_credentials: IAM_CREDENTIALS_URL.to_string(),
            metadata_token: std::env::var(METADATA_URL_ENV)
                .ok()
                .filter(|url| !url.is_empty())
                .unwrap_or_else(|| METADATA_TOKEN_URL.to_string()),
        })
    }

    /// Every endpoint at `base` (a mock server), for tests.
    #[cfg(test)]
    pub(crate) fn at(base: &str) -> GoogleEndpoints {
        GoogleEndpoints {
            oauth2_token: format!("{base}/token"),
            sts_token: format!("{base}/v1/token"),
            iam_credentials: base.to_string(),
            metadata_token: format!(
                "{base}/computeMetadata/v1/instance/service-accounts/default/token"
            ),
        }
    }
}

/// What kind of credential a [`GoogleIdentity`] holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoogleSource {
    /// The metadata server (a host identity).
    Metadata,
    /// A service-account key.
    ServiceAccount,
    /// Workload Identity Federation; `host_signed` when the AWS subject token
    /// is signed with the host's own credential chain (a host identity).
    ExternalAccount {
        /// Signed by the host's AWS chain rather than an embedder's subject.
        host_signed: bool,
    },
    /// The host's application-default credentials (a host identity).
    ApplicationDefault,
}

/// A minted Google access token.
#[derive(Clone)]
pub struct AccessToken {
    /// The bearer token.
    pub token: String,
    /// When Google said it expires.
    pub expires_at: DateTime<Utc>,
}

/// Redacting `Debug`: the token is a live bearer credential.
impl std::fmt::Debug for AccessToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccessToken")
            .field("token", &"***")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

impl From<CachedToken> for AccessToken {
    fn from(token: CachedToken) -> Self {
        AccessToken {
            token: token.access_token,
            expires_at: token.expires_at,
        }
    }
}

/// The credential material an identity mints with.
enum Material {
    Metadata(metadata::MetadataSource),
    #[cfg_attr(not(feature = "aws"), allow(dead_code))]
    ServiceAccount(service_account::ServiceAccountKey),
    #[cfg(feature = "aws")]
    ExternalAccount {
        spec: external_account::ExternalAccountSpec,
        subject: Arc<dyn AwsSubjectCredentials>,
    },
    ApplicationDefault {
        path: std::path::PathBuf,
    },
}

/// A Google identity: mints and caches access tokens for any scope set.
///
/// Tokens live in a process-wide cache keyed by this identity's non-secret
/// fingerprint, the AWS subject that signed it (WIF), a digest binding it to
/// its secret (service accounts), and the sorted scopes; each key refreshes
/// single-flight with a margin before expiry.
pub struct GoogleIdentity {
    material: Material,
    source: GoogleSource,
    principal: String,
    quota_project: Option<String>,
    /// Non-secret identity + the endpoints it is exchanged at.
    fingerprint: String,
    binding: Option<[u8; 32]>,
    subject: String,
    endpoints: GoogleEndpoints,
    http: reqwest::Client,
}

/// Redacting `Debug`: the kind, principal and quota project, never material.
impl std::fmt::Debug for GoogleIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GoogleIdentity")
            .field("source", &self.source)
            .field("principal", &self.principal)
            .field("quota_project", &self.quota_project)
            .finish_non_exhaustive()
    }
}

impl GoogleIdentity {
    /// Build the identity `credentials` describe. `credentials` must be
    /// hydrated (a `secret_ref` fails closed). `quota_project`, when given,
    /// wins over the credential JSON's `quota_project_id`. Host identities are
    /// checked against `ctx`'s policy here, before anything is read.
    ///
    /// No network I/O: the first token is minted on first use.
    pub fn new(
        credentials: &GoogleCredentials,
        quota_project: Option<&str>,
        ctx: &IdentityContext,
    ) -> Result<Self> {
        Self::with_endpoints(credentials, quota_project, ctx, GoogleEndpoints::process())
    }

    pub(crate) fn with_endpoints(
        credentials: &GoogleCredentials,
        quota_project: Option<&str>,
        ctx: &IdentityContext,
        endpoints: &GoogleEndpoints,
    ) -> Result<Self> {
        if let Some(project) = quota_project {
            if !is_project_id(project) {
                return Err(IcebergError::GoogleCredentialsInvalid(
                    "quota_project is not a Google Cloud project id".to_string(),
                ));
            }
        }
        let built = match credentials {
            GoogleCredentials::Metadata => {
                ctx.policy.ensure(HostIdentityKind::GoogleMetadata)?;
                let source = metadata::MetadataSource::new(&endpoints.metadata_token)?;
                Built {
                    fingerprint: format!("metadata|{}", source.url()),
                    material: Material::Metadata(source),
                    source: GoogleSource::Metadata,
                    principal: "metadata".to_string(),
                    file_quota_project: None,
                    binding: None,
                    subject: "-".to_string(),
                }
            }
            GoogleCredentials::ServiceAccount { key } => {
                let key = service_account::ServiceAccountKey::parse(
                    &key.resolve()?,
                    "service_account.key",
                )?;
                Built::service_account(key, endpoints)?
            }
            GoogleCredentials::ExternalAccount { config } => {
                let spec = external_account::parse(&config.resolve()?)?;
                Built::external_account(spec, ctx, endpoints)?
            }
            GoogleCredentials::ApplicationDefault => {
                ctx.policy
                    .ensure(HostIdentityKind::GoogleApplicationDefault)?;
                let path = adc::locate()?;
                let file = adc::read(&path)?;
                let principal = file.principal();
                if let adc::AdcFile::ServiceAccount(_) = &file {
                    require_signing("application_default (service_account)")?;
                }
                Built {
                    fingerprint: format!("adc|{principal}|{}", endpoints.oauth2_token),
                    file_quota_project: file.quota_project_id().map(str::to_string),
                    material: Material::ApplicationDefault { path },
                    source: GoogleSource::ApplicationDefault,
                    principal,
                    binding: None,
                    subject: "-".to_string(),
                }
            }
        };
        let http = crate::net::hardened_client_builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| IcebergError::Http(format!("Failed to build HTTP client: {e}")))?;
        Ok(Self {
            material: built.material,
            source: built.source,
            principal: built.principal,
            quota_project: quota_project
                .map(str::to_string)
                .or(built.file_quota_project),
            fingerprint: built.fingerprint,
            binding: built.binding,
            subject: built.subject,
            endpoints: endpoints.clone(),
            http,
        })
    }

    /// A token for `scopes` (cloud-platform when empty), from the cache when
    /// one is outside its refresh margin, else minted single-flight.
    pub async fn access_token(&self, scopes: &[&str]) -> Result<AccessToken> {
        let scopes = normalize_scopes(scopes);
        let slot = cache::slot(&self.key(&scopes));
        let token = slot.get_or_fetch(|| self.fetch(&scopes)).await?;
        Ok(token.into())
    }

    /// Mint a fresh token for `scopes`, replacing the cached one (after a
    /// server refused it).
    pub async fn refresh(&self, scopes: &[&str]) -> Result<()> {
        let scopes = normalize_scopes(scopes);
        let slot = cache::slot(&self.key(&scopes));
        slot.refetch(|| self.fetch(&scopes)).await
    }

    /// The project Google requests bill to (`x-goog-user-project`): the
    /// explicit `quota_project`, else the credential JSON's `quota_project_id`.
    pub fn quota_project(&self) -> Option<&str> {
        self.quota_project.as_deref()
    }

    /// A stable, non-secret name for this identity: the service-account email,
    /// `wif:<audience>[→<impersonated SA>]`, `metadata`, or
    /// `adc:<client id or SA email>`.
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// What kind of credential this is.
    pub fn source(&self) -> GoogleSource {
        self.source
    }

    fn key(&self, scopes: &[String]) -> cache::TokenKey {
        cache::TokenKey {
            identity: self.fingerprint.clone(),
            binding: self.binding,
            subject: self.subject.clone(),
            scopes: scopes.to_vec(),
        }
    }

    async fn fetch(&self, scopes: &[String]) -> Result<CachedToken> {
        let token = match &self.material {
            Material::Metadata(source) => source.fetch(scopes).await,
            Material::ServiceAccount(key) => self.fetch_service_account(key, scopes).await,
            #[cfg(feature = "aws")]
            Material::ExternalAccount { spec, subject } => {
                spec.fetch(subject.as_ref(), &self.http, &self.endpoints, scopes)
                    .await
            }
            Material::ApplicationDefault { path } => {
                // Re-read: a refreshed login is picked up, but the identity the
                // source was opened with must still be the one in the file.
                let file = adc::read(path)?;
                if file.principal() != self.principal {
                    return Err(IcebergError::GoogleCredentialsInvalid(
                        "application_default: the credentials file now holds a different \
                         identity than when this source was opened"
                            .to_string(),
                    ));
                }
                match &file {
                    adc::AdcFile::AuthorizedUser(user) => {
                        user.fetch(&self.http, &self.endpoints, scopes).await
                    }
                    adc::AdcFile::ServiceAccount(key) => {
                        self.fetch_service_account(key, scopes).await
                    }
                }
            }
        }?;
        tracing::debug!(
            source = ?self.source,
            principal = %self.principal,
            valid_for_s = (token.expires_at - Utc::now()).num_seconds(),
            "Google access token minted"
        );
        Ok(token)
    }

    #[cfg(feature = "aws")]
    async fn fetch_service_account(
        &self,
        key: &service_account::ServiceAccountKey,
        scopes: &[String],
    ) -> Result<CachedToken> {
        key.fetch(&self.http, &self.endpoints.oauth2_token, scopes)
            .await
    }

    #[cfg(not(feature = "aws"))]
    async fn fetch_service_account(
        &self,
        _key: &service_account::ServiceAccountKey,
        _scopes: &[String],
    ) -> Result<CachedToken> {
        Err(signing_unavailable("service_account"))
    }
}

/// The parts of an identity each source works out.
struct Built {
    material: Material,
    source: GoogleSource,
    principal: String,
    file_quota_project: Option<String>,
    fingerprint: String,
    binding: Option<[u8; 32]>,
    subject: String,
}

impl Built {
    fn service_account(
        key: service_account::ServiceAccountKey,
        endpoints: &GoogleEndpoints,
    ) -> Result<Self> {
        require_signing("service_account")?;
        #[cfg(feature = "aws")]
        let binding = Some(key.binding);
        #[cfg(not(feature = "aws"))]
        let binding = None;
        Ok(Built {
            fingerprint: format!(
                "sa|{}|{}|{}",
                key.client_email, key.private_key_id, endpoints.oauth2_token
            ),
            principal: key.client_email.clone(),
            file_quota_project: key.quota_project_id.clone(),
            material: Material::ServiceAccount(key),
            source: GoogleSource::ServiceAccount,
            binding,
            subject: "-".to_string(),
        })
    }

    #[cfg(feature = "aws")]
    fn external_account(
        spec: external_account::ExternalAccountSpec,
        ctx: &IdentityContext,
        endpoints: &GoogleEndpoints,
    ) -> Result<Self> {
        // An embedder's subject is its own per-tenant identity; only the
        // host's chain is a host identity the policy governs.
        let (subject, subject_key, host_signed): (Arc<dyn AwsSubjectCredentials>, String, bool) =
            match &ctx.aws_subject {
                Some(subject) => (
                    Arc::clone(subject),
                    format!("embedder:{}", subject.principal_key()),
                    false,
                ),
                None => {
                    ctx.policy.ensure(HostIdentityKind::GoogleHostSignedWif)?;
                    (
                        Arc::new(aws_subject::HostAwsSubject::default()),
                        aws_subject::HOST_PRINCIPAL_KEY.to_string(),
                        true,
                    )
                }
            };
        let principal = match &spec.impersonate {
            Some(email) => format!("wif:{}→{email}", spec.audience),
            None => format!("wif:{}", spec.audience),
        };
        Ok(Built {
            fingerprint: format!(
                "wif|{}|{}|{}|{}|{}",
                spec.audience,
                spec.impersonate.as_deref().unwrap_or("-"),
                spec.lifetime_secs,
                endpoints.sts_token,
                endpoints.iam_credentials
            ),
            principal,
            file_quota_project: spec.quota_project_id.clone(),
            source: GoogleSource::ExternalAccount { host_signed },
            material: Material::ExternalAccount { spec, subject },
            binding: None,
            subject: subject_key,
        })
    }

    #[cfg(not(feature = "aws"))]
    fn external_account(
        _spec: external_account::ExternalAccountSpec,
        _ctx: &IdentityContext,
        _endpoints: &GoogleEndpoints,
    ) -> Result<Self> {
        Err(signing_unavailable("external_account"))
    }
}

/// Signing needs the `aws` feature; refuse where it is off.
fn require_signing(source: &str) -> Result<()> {
    if cfg!(feature = "aws") {
        Ok(())
    } else {
        Err(signing_unavailable(source))
    }
}

fn signing_unavailable(source: &str) -> IcebergError {
    IcebergError::Config(format!(
        "Google {source} credentials need request signing, which this build of \
         fluree-db-iceberg was compiled without (the `aws` feature)"
    ))
}

/// Sorted, de-duplicated scopes; cloud-platform when none are given.
fn normalize_scopes(scopes: &[&str]) -> Vec<String> {
    let mut out: Vec<String> = scopes
        .iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    if out.is_empty() {
        out.push(CLOUD_PLATFORM_SCOPE.to_string());
    }
    out.sort();
    out.dedup();
    out
}

/// Parse a stored comma-separated scope list (`None` → cloud-platform).
pub(crate) fn parse_scopes(scopes: Option<&str>) -> Result<Vec<String>> {
    let Some(raw) = scopes else {
        return Ok(vec![CLOUD_PLATFORM_SCOPE.to_string()]);
    };
    let parsed: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    if parsed.is_empty() || parsed.iter().any(|s| s.contains(char::is_whitespace)) {
        return Err(IcebergError::GoogleCredentialsInvalid(
            "scopes must be a comma-separated list of OAuth scopes".to_string(),
        ));
    }
    Ok(normalize_scopes(
        &parsed.iter().map(String::as_str).collect::<Vec<_>>(),
    ))
}

/// A Google Cloud project id as `x-goog-user-project` takes it (including the
/// legacy domain-scoped `example.com:project` form).
pub(crate) fn is_project_id(project: &str) -> bool {
    !project.is_empty()
        && project.len() <= 100
        && project
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b':' | b'_'))
}

/// A service-account email (`name@project.iam.gserviceaccount.com`, or a
/// Google-managed one): one `@`, and nothing that could leave a URL path
/// segment.
pub(crate) fn is_service_account_email(email: &str) -> bool {
    let Some((local, domain)) = email.split_once('@') else {
        return false;
    };
    let part = |s: &str| {
        !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_'))
    };
    email.len() <= 254 && part(local) && part(domain) && domain.contains('.')
}

/// Google's public universe.
fn is_google_universe(universe: &str) -> bool {
    universe == "googleapis.com"
}

/// Catalog auth with a [`GoogleIdentity`]: `Authorization: Bearer <token>`,
/// plus `x-goog-user-project` when the identity carries a quota project.
pub struct GoogleCatalogAuth {
    identity: Arc<GoogleIdentity>,
    scopes: Vec<String>,
}

impl std::fmt::Debug for GoogleCatalogAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GoogleCatalogAuth")
            .field("identity", &self.identity)
            .field("scopes", &self.scopes)
            .finish()
    }
}

impl GoogleCatalogAuth {
    /// Authenticate requests as `identity` with `scopes`.
    pub fn new(identity: Arc<GoogleIdentity>, scopes: Vec<String>) -> Self {
        Self { identity, scopes }
    }

    /// The identity requests are made as.
    pub fn identity(&self) -> &GoogleIdentity {
        &self.identity
    }

    fn scope_refs(&self) -> Vec<&str> {
        self.scopes.iter().map(String::as_str).collect()
    }

    async fn headers(&self) -> Result<HeaderMap> {
        let token = self.identity.access_token(&self.scope_refs()).await?;
        let mut headers = HeaderMap::new();
        let mut bearer =
            HeaderValue::from_str(&format!("Bearer {}", token.token)).map_err(|_| {
                IcebergError::Auth("Google issued an unusable access token".to_string())
            })?;
        bearer.set_sensitive(true);
        headers.insert(AUTHORIZATION, bearer);
        if let Some(project) = self.identity.quota_project() {
            // Validated as a project id at construction, so always a valid value.
            let value = HeaderValue::from_str(project).map_err(|_| {
                IcebergError::GoogleCredentialsInvalid(
                    "quota_project is not a Google Cloud project id".to_string(),
                )
            })?;
            headers.insert(HeaderName::from_static(QUOTA_PROJECT_HEADER), value);
        }
        Ok(headers)
    }
}

#[async_trait(?Send)]
impl CatalogAuth for GoogleCatalogAuth {
    async fn request_headers(&self) -> Result<HeaderMap> {
        self.headers().await
    }

    async fn refresh(&self) -> Result<()> {
        self.identity.refresh(&self.scope_refs()).await
    }
}

#[async_trait]
impl SendCatalogAuth for GoogleCatalogAuth {
    async fn request_headers(&self) -> Result<HeaderMap> {
        self.headers().await
    }

    async fn refresh(&self) -> Result<()> {
        self.identity.refresh(&self.scope_refs()).await
    }
}

/// The metadata URL a stored `google_metadata` record names must be this
/// process's own (see [`metadata::check_stored_url`]).
pub(crate) fn check_stored_metadata_url(
    stored: Option<&str>,
    endpoints: &GoogleEndpoints,
) -> Result<()> {
    metadata::check_stored_url(stored, &endpoints.metadata_token)
}

#[cfg(test)]
mod tests;
