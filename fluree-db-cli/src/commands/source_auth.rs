//! The shared graph-source flag groups ([`CatalogAuthArgs`], [`GoogleAuthArgs`],
//! [`StorageIdentityArgs`]), each with one conversion onto the codec every
//! surface uses ([`fluree_db_api::CatalogAuthFields`],
//! [`fluree_db_api::StorageAuthFields`]), so a flag means what the matching
//! HTTP field means and is refused for the same reasons, in the CLI's own
//! spelling.

use crate::cli::{CatalogAuthArgs, GoogleAuthArgs, StorageIdentity, StorageIdentityArgs};
use crate::error::{CliError, CliResult};
use fluree_db_api::{
    CatalogAuthFields, ExternalAccountConfig, GoogleCredentials, GraphSourceInputError,
    IcebergConfigValue as ConfigValue, StorageAuthFields,
};
use std::path::Path;

/// A field name as its flag (`auth_bearer_env` → `--auth-bearer-env`).
pub(crate) fn flag(field: &str) -> String {
    format!("--{}", field.replace('_', "-"))
}

/// A refusal, in the CLI's spelling.
pub(crate) fn usage(e: GraphSourceInputError) -> CliError {
    CliError::Usage(e.message(flag))
}

impl CatalogAuthArgs {
    /// The auth these flags (and, where the target takes one, the Google
    /// group's) give. A literal and its `_env` variable together are refused.
    pub fn fields(&self, google: Option<&GoogleAuthArgs>) -> CliResult<CatalogAuthFields> {
        let secret = |literal_field, literal: &Option<String>, env_field, env: &Option<String>| {
            fluree_db_api::secret_field(literal_field, literal.clone(), env_field, env.clone())
                .map_err(usage)
        };
        Ok(CatalogAuthFields {
            bearer: secret(
                "auth_bearer",
                &self.auth_bearer,
                "auth_bearer_env",
                &self.auth_bearer_env,
            )?,
            oauth2_token_url: self.oauth2_token_url.clone(),
            oauth2_client_id: self.oauth2_client_id.clone(),
            oauth2_client_secret: secret(
                "oauth2_client_secret",
                &self.oauth2_client_secret,
                "oauth2_client_secret_env",
                &self.oauth2_client_secret_env,
            )?,
            oauth2_scope: self.oauth2_scope.clone(),
            oauth2_audience: self.oauth2_audience.clone(),
            google: match google {
                Some(google) => google.sources()?,
                None => Vec::new(),
            },
            google_scopes: google.and_then(|g| g.auth_google_scopes.clone()),
            google_quota_project: google.and_then(|g| g.auth_google_quota_project.clone()),
        })
    }
}

impl GoogleAuthArgs {
    /// Each Google credential source given, by its field.
    fn sources(&self) -> CliResult<Vec<(&'static str, GoogleCredentials)>> {
        google_sources(
            [
                "auth_google_metadata",
                "auth_google_adc",
                "auth_google_service_account_env",
                "auth_google_wif_config",
            ],
            self.auth_google_metadata,
            self.auth_google_adc,
            self.auth_google_service_account_env.as_deref(),
            self.auth_google_wif_config.as_deref(),
        )
    }
}

impl StorageIdentityArgs {
    /// Whether any storage flag is given.
    pub fn is_given(&self) -> bool {
        self.storage_identity.is_some()
            || self.storage_google_metadata
            || self.storage_google_adc
            || self.storage_google_service_account_env.is_some()
            || self.storage_google_wif_config.is_some()
            || self.storage_google_quota_project.is_some()
    }

    /// The storage identity these flags give.
    pub fn fields(&self) -> CliResult<StorageAuthFields> {
        Ok(StorageAuthFields {
            identity: self.storage_identity.map(|identity| {
                match identity {
                    StorageIdentity::Vended => "vended",
                    StorageIdentity::Ambient => "ambient",
                    StorageIdentity::Catalog => "catalog",
                    StorageIdentity::Google => "google",
                }
                .to_string()
            }),
            google: google_sources(
                [
                    "storage_google_metadata",
                    "storage_google_adc",
                    "storage_google_service_account_env",
                    "storage_google_wif_config",
                ],
                self.storage_google_metadata,
                self.storage_google_adc,
                self.storage_google_service_account_env.as_deref(),
                self.storage_google_wif_config.as_deref(),
            )?,
            google_quota_project: self.storage_google_quota_project.clone(),
        })
    }
}

/// The Google credential sources a group's four flags give, in order: the
/// metadata server, application-default credentials, a service-account key
/// in an environment variable, a WIF configuration file (read here and
/// stored inline: it is not a secret).
fn google_sources(
    names: [&'static str; 4],
    metadata: bool,
    adc: bool,
    service_account_env: Option<&str>,
    wif_config: Option<&Path>,
) -> CliResult<Vec<(&'static str, GoogleCredentials)>> {
    let mut sources = Vec::new();
    if metadata {
        sources.push((names[0], GoogleCredentials::Metadata));
    }
    if adc {
        sources.push((names[1], GoogleCredentials::ApplicationDefault));
    }
    if let Some(var) = service_account_env {
        sources.push((
            names[2],
            GoogleCredentials::ServiceAccount {
                key: ConfigValue::from_env(var),
            },
        ));
    }
    if let Some(path) = wif_config {
        sources.push((
            names[3],
            GoogleCredentials::ExternalAccount {
                config: ExternalAccountConfig::Inline(read_wif_config(names[3], path)?),
            },
        ));
    }
    Ok(sources)
}

/// A WIF credential configuration file's JSON object, checked here as a
/// registration would check it: the file is sent to a remote server as is,
/// and a service-account key given by mistake must not leave this machine.
fn read_wif_config(
    field: &str,
    path: &Path,
) -> CliResult<serde_json::Map<String, serde_json::Value>> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| CliError::Input(format!("failed to read {}: {e}", path.display())))?;
    let config = match serde_json::from_str(&text) {
        Ok(serde_json::Value::Object(config)) => config,
        _ => {
            return Err(CliError::Usage(format!(
                "{} {} does not hold a JSON object (the credential configuration that `gcloud \
                 iam workload-identity-pools create-cred-config` prints)",
                flag(field),
                path.display()
            )))
        }
    };
    match config.get("type").and_then(serde_json::Value::as_str) {
        Some("external_account") => {}
        Some("service_account") => {
            return Err(CliError::Usage(format!(
                "{} {} holds a service-account key, not a WIF credential configuration. A key \
                 is a secret: name the environment variable that holds it with {} instead",
                flag(field),
                path.display(),
                flag(&field.replace("wif_config", "service_account_env"))
            )))
        }
        _ => {
            return Err(CliError::Usage(format!(
                "{} {} is not a WIF credential configuration (its `type` must be \
                 `external_account`)",
                flag(field),
                path.display()
            )))
        }
    }
    GoogleCredentials::ExternalAccount {
        config: ExternalAccountConfig::Inline(config.clone()),
    }
    .validate_static()
    .map_err(|e| CliError::Usage(format!("{} {}: {e}", flag(field), path.display())))?;
    Ok(config)
}
