//! Every [`ConfigValue`] a stored auth config carries, and what it holds.
//!
//! One walk per type, written as exhaustive matches that name every field, so
//! a surface's wire checks and the stored-config redactor classify the same
//! values: a new variant, or a new field on one, does not compile until it
//! says whether it holds a secret.

use crate::auth::{AuthConfig, ExternalAccountConfig, GoogleCredentials};
use crate::config::StorageAuth;
use crate::config_value::ConfigValue;

/// What a [`ConfigValue`] in a stored config holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueClass {
    /// A credential: a bearer token, an OAuth2 or Azure client secret.
    /// Read-outs mask it.
    Secret,
    /// A Google service-account key. A credential no surface may give
    /// literally, since the literal would be stored with the graph source.
    /// Read-outs mask it.
    ServiceAccountKey,
    /// Identifies but never authenticates: an OAuth2 client id, a Workload
    /// Identity Federation credential configuration. Read-outs show it.
    Public,
}

impl ValueClass {
    /// Whether a read-out masks the value.
    pub fn is_secret(self) -> bool {
        match self {
            ValueClass::Secret | ValueClass::ServiceAccountKey => true,
            ValueClass::Public => false,
        }
    }
}

/// One [`ConfigValue`] of a config, at its path in the stored JSON.
#[derive(Debug, Clone)]
pub struct ConfigValueField<'a> {
    /// Dot-separated object keys from the object the walk started at
    /// (`token`, `credentials.key`).
    pub path: String,
    pub value: &'a ConfigValue,
    pub class: ValueClass,
}

impl<'a> ConfigValueField<'a> {
    /// The value at `path`.
    pub fn new(path: &str, value: &'a ConfigValue, class: ValueClass) -> Self {
        Self {
            path: path.to_string(),
            value,
            class,
        }
    }

    /// This field, `prefix` levels further down.
    pub fn under(mut self, prefix: &str) -> Self {
        self.path = format!("{prefix}.{}", self.path);
        self
    }
}

fn under<'a>(prefix: &str, fields: Vec<ConfigValueField<'a>>) -> Vec<ConfigValueField<'a>> {
    fields.into_iter().map(|f| f.under(prefix)).collect()
}

impl AuthConfig {
    /// Every [`ConfigValue`] this auth holds, with paths relative to the auth
    /// object itself.
    pub fn config_values(&self) -> Vec<ConfigValueField<'_>> {
        use ValueClass::{Public, Secret};
        match self {
            AuthConfig::None => Vec::new(),
            AuthConfig::Bearer { token } => vec![ConfigValueField::new("token", token, Secret)],
            AuthConfig::OAuth2ClientCredentials {
                token_url: _,
                client_id,
                client_secret,
                scope: _,
                audience: _,
            } => vec![
                ConfigValueField::new("client_id", client_id, Public),
                ConfigValueField::new("client_secret", client_secret, Secret),
            ],
            AuthConfig::GoogleMetadata {
                scopes: _,
                metadata_url: _,
            } => Vec::new(),
            AuthConfig::Google {
                credentials,
                scopes: _,
                quota_project: _,
            } => under("credentials", credentials.config_values()),
        }
    }
}

impl GoogleCredentials {
    /// Every [`ConfigValue`] these credentials hold, with paths relative to
    /// the credentials object.
    pub fn config_values(&self) -> Vec<ConfigValueField<'_>> {
        match self {
            GoogleCredentials::Metadata | GoogleCredentials::ApplicationDefault => Vec::new(),
            GoogleCredentials::ServiceAccount { key } => vec![ConfigValueField::new(
                "key",
                key,
                ValueClass::ServiceAccountKey,
            )],
            GoogleCredentials::ExternalAccount {
                config: ExternalAccountConfig::Ref(value),
            } => vec![ConfigValueField::new("config", value, ValueClass::Public)],
            // Not a `ConfigValue`, and not secret: gcloud's printed config.
            GoogleCredentials::ExternalAccount {
                config: ExternalAccountConfig::Inline(_),
            } => Vec::new(),
        }
    }
}

impl StorageAuth {
    /// Every [`ConfigValue`] this storage identity holds, with paths relative
    /// to the `storage_auth` object.
    pub fn config_values(&self) -> Vec<ConfigValueField<'_>> {
        match self {
            StorageAuth::Vended | StorageAuth::Ambient | StorageAuth::Catalog => Vec::new(),
            StorageAuth::Google {
                credentials,
                quota_project: _,
            } => under("credentials", credentials.config_values()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths(fields: &[ConfigValueField<'_>]) -> Vec<(String, ValueClass)> {
        fields.iter().map(|f| (f.path.clone(), f.class)).collect()
    }

    #[test]
    fn each_auth_names_its_values() {
        let secret = ConfigValue::literal("s");
        let oauth2 = AuthConfig::OAuth2ClientCredentials {
            token_url: "https://t".into(),
            client_id: ConfigValue::literal("id"),
            client_secret: secret.clone(),
            scope: None,
            audience: None,
        };
        assert_eq!(
            paths(&oauth2.config_values()),
            [
                ("client_id".to_string(), ValueClass::Public),
                ("client_secret".to_string(), ValueClass::Secret)
            ]
        );
        let google = AuthConfig::Google {
            credentials: GoogleCredentials::ServiceAccount { key: secret },
            scopes: None,
            quota_project: None,
        };
        assert_eq!(
            paths(&google.config_values()),
            [("credentials.key".to_string(), ValueClass::ServiceAccountKey)]
        );
        let storage = StorageAuth::Google {
            credentials: GoogleCredentials::ExternalAccount {
                config: ExternalAccountConfig::Ref(ConfigValue::from_env("WIF")),
            },
            quota_project: None,
        };
        assert_eq!(
            paths(&storage.config_values()),
            [("credentials.config".to_string(), ValueClass::Public)]
        );
    }
}
