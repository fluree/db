//! The host's application-default credentials (ADC) file: what `gcloud auth
//! application-default login` writes, or the file `GOOGLE_APPLICATION_CREDENTIALS`
//! names. A host identity, for local development and single-tenant hosts.
//!
//! Read when the identity is built (to learn its principal and quota project)
//! and again at every token exchange, so a refreshed login is picked up. A file
//! that changed identity in between is refused rather than followed.
//!
//! One host-global identity: there is one ADC file per host, every source that
//! names `application_default` reads it, and the host-identity policy gates
//! it (`google-adc`). So sources sharing its principal (and the storage scope
//! keyed on it) is by design, not a confusion between tenants: no tenant
//! supplies or distinguishes it. Its principal is weak on its own: gcloud's
//! user credentials carry gcloud's public OAuth client id, the same for
//! every user, so the `account` gcloud records is used instead when the file
//! names one (current gcloud writes the field but leaves it empty).

use std::path::PathBuf;

use crate::auth::token::CachedToken;
use crate::error::{GoogleTokenStage, IcebergError, Result};

use super::service_account::ServiceAccountKey;
use super::{is_google_universe, is_project_id, wire, GoogleEndpoints};

/// Names the ADC file explicitly.
pub const APPLICATION_CREDENTIALS_ENV: &str = "GOOGLE_APPLICATION_CREDENTIALS";
/// Overrides gcloud's configuration directory.
const CLOUDSDK_CONFIG_ENV: &str = "CLOUDSDK_CONFIG";
const WELL_KNOWN_FILE: &str = "application_default_credentials.json";

/// One read of the ADC file.
#[derive(Debug)]
pub(super) enum AdcFile {
    /// gcloud user credentials.
    AuthorizedUser(AuthorizedUser),
    /// A service-account key.
    ServiceAccount(ServiceAccountKey),
}

/// gcloud user credentials: a refresh token for gcloud's OAuth client.
pub(super) struct AuthorizedUser {
    client_id: String,
    client_secret: String,
    refresh_token: String,
    quota_project_id: Option<String>,
    /// The user gcloud logged in, when the file says (an email; not secret).
    account: Option<String>,
}

/// Redacting `Debug`: the client secret and refresh token never print.
impl std::fmt::Debug for AuthorizedUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthorizedUser")
            .field("client_id", &self.client_id)
            .field("account", &self.account)
            .field("quota_project_id", &self.quota_project_id)
            .finish_non_exhaustive()
    }
}

impl AdcFile {
    /// `adc:<account>` for user credentials whose file names the account,
    /// else `adc:<client id>` (gcloud's public client id: the same for every
    /// user, see the module docs); `adc:<SA email>` for a key.
    pub(super) fn principal(&self) -> String {
        match self {
            AdcFile::AuthorizedUser(AuthorizedUser {
                account: Some(account),
                ..
            }) => format!("adc:{account}"),
            AdcFile::AuthorizedUser(user) => format!("adc:{}", user.client_id),
            AdcFile::ServiceAccount(key) => format!("adc:{}", key.client_email),
        }
    }

    /// Digest of the secret this read of the file proves its identity with
    /// (the refresh token and client secret; a service account's key), so a
    /// cached token never outlives a change to it. In memory only.
    pub(super) fn binding(&self) -> [u8; 32] {
        match self {
            AdcFile::AuthorizedUser(user) => {
                super::secret_binding(&[&user.client_id, &user.client_secret, &user.refresh_token])
            }
            AdcFile::ServiceAccount(key) => key.binding,
        }
    }

    pub(super) fn quota_project_id(&self) -> Option<&str> {
        match self {
            AdcFile::AuthorizedUser(user) => user.quota_project_id.as_deref(),
            AdcFile::ServiceAccount(key) => key.quota_project_id.as_deref(),
        }
    }
}

/// Where the ADC file is: `GOOGLE_APPLICATION_CREDENTIALS`, else gcloud's
/// well-known path (under `CLOUDSDK_CONFIG` when set).
pub(super) fn locate() -> Result<PathBuf> {
    let env = |name: &str| std::env::var_os(name).filter(|v| !v.is_empty());
    if let Some(path) = env(APPLICATION_CREDENTIALS_ENV) {
        return Ok(PathBuf::from(path));
    }
    let config_dir = env(CLOUDSDK_CONFIG_ENV).map(PathBuf::from).or_else(|| {
        if cfg!(windows) {
            env("APPDATA").map(|d| PathBuf::from(d).join("gcloud"))
        } else {
            env("HOME").map(|h| PathBuf::from(h).join(".config").join("gcloud"))
        }
    });
    config_dir.map(|d| d.join(WELL_KNOWN_FILE)).ok_or_else(|| {
        IcebergError::GoogleCredentialsInvalid(format!(
            "application_default: no credentials file found (set {APPLICATION_CREDENTIALS_ENV} \
             or run `gcloud auth application-default login`)"
        ))
    })
}

/// Read and validate the ADC file at `path`. The path is not put in errors
/// (they can reach API clients); it is logged at debug.
pub(super) fn read(path: &std::path::Path) -> Result<AdcFile> {
    let text = std::fs::read_to_string(path).map_err(|e| {
        tracing::debug!(path = %path.display(), error = %e, "ADC file unreadable");
        IcebergError::GoogleCredentialsInvalid(format!(
            "application_default: the credentials file could not be read (set \
             {APPLICATION_CREDENTIALS_ENV} or run `gcloud auth application-default login`)"
        ))
    })?;
    parse(&text)
}

pub(super) fn parse(text: &str) -> Result<AdcFile> {
    let invalid =
        |what: &str| IcebergError::GoogleCredentialsInvalid(format!("application_default: {what}"));
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|_| invalid("the credentials file is not JSON"))?;
    let kind = value.get("type").and_then(serde_json::Value::as_str);
    match kind {
        Some("authorized_user") => {
            let field = |name: &str| {
                value
                    .get(name)
                    .and_then(serde_json::Value::as_str)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| {
                        IcebergError::GoogleCredentialsInvalid(format!(
                            "application_default: authorized_user credentials need {name}"
                        ))
                    })
            };
            if let Some(universe) = value.get("universe_domain").and_then(|v| v.as_str()) {
                if !is_google_universe(universe) {
                    return Err(invalid("universe_domain must be googleapis.com"));
                }
            }
            let quota_project_id = match value.get("quota_project_id").and_then(|v| v.as_str()) {
                Some(project) if !is_project_id(project) => {
                    return Err(invalid("quota_project_id is not a Google Cloud project id"))
                }
                project => project.map(str::to_string),
            };
            // Only an email-shaped account names a principal; an empty one
            // (what current gcloud writes) or anything else is ignored.
            let account = value
                .get("account")
                .and_then(serde_json::Value::as_str)
                .filter(|a| {
                    a.len() <= 254 && a.contains('@') && a.bytes().all(|b| b.is_ascii_graphic())
                })
                .map(str::to_string);
            Ok(AdcFile::AuthorizedUser(AuthorizedUser {
                client_id: field("client_id")?,
                client_secret: field("client_secret")?,
                refresh_token: field("refresh_token")?,
                quota_project_id,
                account,
            }))
        }
        Some("service_account") => Ok(AdcFile::ServiceAccount(ServiceAccountKey::parse(
            text,
            "application_default",
        )?)),
        // Named only when it is one of Google's own types; anything else in
        // the file is not echoed.
        Some(
            known @ ("external_account"
            | "external_account_authorized_user"
            | "impersonated_service_account"
            | "gdch_service_account"),
        ) => Err(invalid(&format!(
            "credentials of type {known} are not supported (authorized_user and \
             service_account are)"
        ))),
        _ => Err(invalid(
            "the credentials file is not authorized_user or service_account credentials",
        )),
    }
}

impl AuthorizedUser {
    /// The refresh-token grant at Google's OAuth endpoint.
    pub(super) async fn fetch(
        &self,
        http: &reqwest::Client,
        endpoints: &GoogleEndpoints,
        scopes: &[String],
    ) -> Result<CachedToken> {
        let stage = GoogleTokenStage::Refresh;
        let scope = scopes.join(" ");
        let response = http
            .post(&endpoints.oauth2_token)
            .form(&[
                ("grant_type", "refresh_token"),
                ("client_id", self.client_id.as_str()),
                ("client_secret", self.client_secret.as_str()),
                ("refresh_token", self.refresh_token.as_str()),
                ("scope", scope.as_str()),
            ])
            .send()
            .await
            .map_err(|e| wire::transport_error(stage, &e))?;
        wire::read_token(stage, response).await
    }
}
