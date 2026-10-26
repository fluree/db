//! The stored shape of a Google identity: where its credential comes from.

use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize};

use crate::config_value::{ConfigValue, SecretResolver};
use crate::error::{IcebergError, Result};

/// Where a Google identity's credential comes from.
///
/// ```json
/// {"source": "metadata"}
/// {"source": "service_account", "key": {"secret_ref": "…"}}
/// {"source": "external_account", "config": { …gcloud's WIF credential configuration… }}
/// {"source": "application_default"}
/// ```
#[derive(Clone, PartialEq, Deserialize, Serialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum GoogleCredentials {
    /// The GCE/GKE metadata server's attached service account (a host identity).
    Metadata,
    /// A service-account key JSON. Secret: an env var or secret reference on
    /// every stored surface (a literal is accepted only through the Rust API).
    ServiceAccount {
        /// The key file's JSON text.
        key: ConfigValue,
    },
    /// A Workload Identity Federation credential configuration with an AWS
    /// credential source: what `gcloud iam workload-identity-pools
    /// create-cred-config … --aws` prints. Not secret.
    ExternalAccount {
        /// The configuration, inline or as a value holding its JSON text.
        config: ExternalAccountConfig,
    },
    /// The host's application-default credentials file
    /// (`GOOGLE_APPLICATION_CREDENTIALS`, else gcloud's well-known path), read
    /// when a token is minted (a host identity).
    ApplicationDefault,
}

/// Redacting `Debug`: the service-account key is a live credential (its
/// `ConfigValue` redacts a literal), and an inline WIF configuration is shown
/// only as present.
impl std::fmt::Debug for GoogleCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GoogleCredentials::Metadata => f.write_str("Metadata"),
            GoogleCredentials::ServiceAccount { key } => {
                f.debug_struct("ServiceAccount").field("key", key).finish()
            }
            GoogleCredentials::ExternalAccount { config } => f
                .debug_struct("ExternalAccount")
                .field("config", config)
                .finish(),
            GoogleCredentials::ApplicationDefault => f.write_str("ApplicationDefault"),
        }
    }
}

impl GoogleCredentials {
    /// Check what can be checked before any secret is resolved: an inline (or
    /// literal) WIF configuration, a literal service-account key. References
    /// are checked when the identity is built.
    pub fn validate_static(&self) -> Result<()> {
        match self {
            GoogleCredentials::ExternalAccount {
                config:
                    config @ (ExternalAccountConfig::Inline(_)
                    | ExternalAccountConfig::Ref(ConfigValue::Literal(_))),
            } => super::external_account::parse(&config.resolve()?).map(|_| ()),
            GoogleCredentials::ServiceAccount {
                key: ConfigValue::Literal(text),
            } => super::service_account::ServiceAccountKey::parse(text, "service_account.key")
                .map(|_| ()),
            GoogleCredentials::Metadata
            | GoogleCredentials::ApplicationDefault
            | GoogleCredentials::ServiceAccount { .. }
            | GoogleCredentials::ExternalAccount { .. } => Ok(()),
        }
    }

    /// Resolve any secret reference via `resolver` (see [`ConfigValue::hydrate`]).
    pub async fn hydrate(&self, resolver: Option<&Arc<dyn SecretResolver>>) -> Result<Self> {
        Ok(match self {
            GoogleCredentials::Metadata => GoogleCredentials::Metadata,
            GoogleCredentials::ServiceAccount { key } => GoogleCredentials::ServiceAccount {
                key: key.hydrate(resolver).await?,
            },
            GoogleCredentials::ExternalAccount { config } => GoogleCredentials::ExternalAccount {
                config: match config {
                    ExternalAccountConfig::Ref(value) => {
                        ExternalAccountConfig::Ref(value.hydrate(resolver).await?)
                    }
                    ExternalAccountConfig::Inline(map) => {
                        ExternalAccountConfig::Inline(map.clone())
                    }
                },
            },
            GoogleCredentials::ApplicationDefault => GoogleCredentials::ApplicationDefault,
        })
    }
}

/// A WIF credential configuration: inline, or a [`ConfigValue`] (env var or
/// secret reference) holding its JSON text.
///
/// Deserialized by hand rather than `#[serde(untagged)]`: `ConfigValue`'s
/// `Dynamic` arm has only defaulted fields, so it matches *any* JSON object and
/// would swallow an inline configuration as an empty env-var reference. The
/// rules:
///
/// - a string is a [`ConfigValue::Literal`] holding the configuration's JSON;
/// - an object whose every key is one of `ConfigValue`'s own (`secret_ref`,
///   `env_var`, `default_val`, `java_property`) is a reference;
/// - an object with a `type` and none of those keys is the configuration;
/// - anything else, including an object mixing the two, is refused.
#[derive(Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ExternalAccountConfig {
    /// A value resolving to the configuration's JSON text.
    Ref(ConfigValue),
    /// The configuration itself.
    Inline(serde_json::Map<String, serde_json::Value>),
}

/// The keys a [`ConfigValue`] object can carry.
const CONFIG_VALUE_KEYS: [&str; 4] = ["secret_ref", "env_var", "java_property", "default_val"];

impl<'de> Deserialize<'de> for ExternalAccountConfig {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        use serde::de::Error;
        match serde_json::Value::deserialize(deserializer)? {
            serde_json::Value::String(text) => {
                Ok(ExternalAccountConfig::Ref(ConfigValue::Literal(text)))
            }
            serde_json::Value::Object(map)
                if map.contains_key("type")
                    && !map.keys().any(|k| CONFIG_VALUE_KEYS.contains(&k.as_str())) =>
            {
                Ok(ExternalAccountConfig::Inline(map))
            }
            serde_json::Value::Object(map)
                if !map.is_empty()
                    && map.keys().all(|k| CONFIG_VALUE_KEYS.contains(&k.as_str())) =>
            {
                ConfigValue::deserialize(serde_json::Value::Object(map))
                    .map(ExternalAccountConfig::Ref)
                    .map_err(D::Error::custom)
            }
            _ => Err(D::Error::custom(
                "external_account.config must be the credential configuration object, its JSON \
                 text, or an env_var / secret_ref holding it",
            )),
        }
    }
}

impl std::fmt::Debug for ExternalAccountConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExternalAccountConfig::Ref(value) => f.debug_tuple("Ref").field(value).finish(),
            ExternalAccountConfig::Inline(_) => f.write_str("Inline(..)"),
        }
    }
}

impl ExternalAccountConfig {
    /// The configuration object. A reference must already be hydrated (a
    /// `secret_ref` fails closed here, like every synchronous resolve).
    pub(crate) fn resolve(&self) -> Result<serde_json::Map<String, serde_json::Value>> {
        match self {
            ExternalAccountConfig::Inline(map) => Ok(map.clone()),
            ExternalAccountConfig::Ref(value) => {
                let text = value.resolve()?;
                match serde_json::from_str::<serde_json::Value>(&text) {
                    Ok(serde_json::Value::Object(map)) => Ok(map),
                    _ => Err(IcebergError::GoogleCredentialsInvalid(
                        "external_account.config does not hold a JSON object".to_string(),
                    )),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_source_round_trips() {
        for json in [
            r#"{"source":"metadata"}"#,
            r#"{"source":"application_default"}"#,
            r#"{"source":"service_account","key":{"secret_ref":"vault://sa"}}"#,
            r#"{"source":"service_account","key":{"env_var":"SA_KEY","java_property":null,"default_val":null}}"#,
            r#"{"source":"external_account","config":{"type":"external_account","audience":"a"}}"#,
            r#"{"source":"external_account","config":{"env_var":"WIF","java_property":null,"default_val":null}}"#,
            r#"{"source":"external_account","config":"{\"type\":\"external_account\"}"}"#,
            r#"{"source":"external_account","config":{"secret_ref":"vault://wif"}}"#,
        ] {
            let parsed: GoogleCredentials = serde_json::from_str(json).unwrap();
            assert_eq!(serde_json::to_string(&parsed).unwrap(), json);
        }
    }

    #[test]
    fn an_inline_config_is_not_swallowed_as_an_env_reference() {
        let parsed: GoogleCredentials = serde_json::from_str(
            r#"{"source":"external_account","config":{"type":"external_account"}}"#,
        )
        .unwrap();
        assert!(
            matches!(
                parsed,
                GoogleCredentials::ExternalAccount {
                    config: ExternalAccountConfig::Inline(_)
                }
            ),
            "{parsed:?}"
        );
        let parsed: GoogleCredentials =
            serde_json::from_str(r#"{"source":"external_account","config":{"secret_ref":"r"}}"#)
                .unwrap();
        assert!(matches!(
            parsed,
            GoogleCredentials::ExternalAccount {
                config: ExternalAccountConfig::Ref(ConfigValue::SecretRef { .. })
            }
        ));
    }

    #[test]
    fn each_config_shape_deserializes_to_its_variant() {
        let config = |json: &str| -> ExternalAccountConfig {
            match serde_json::from_str::<GoogleCredentials>(&format!(
                r#"{{"source":"external_account","config":{json}}}"#
            ))
            .unwrap()
            {
                GoogleCredentials::ExternalAccount { config } => config,
                other => panic!("{other:?}"),
            }
        };
        assert!(matches!(
            config(r#""{\"type\":\"external_account\"}""#),
            ExternalAccountConfig::Ref(ConfigValue::Literal(_))
        ));
        assert!(matches!(
            config(r#"{"secret_ref":"vault://wif"}"#),
            ExternalAccountConfig::Ref(ConfigValue::SecretRef { .. })
        ));
        assert!(matches!(
            config(r#"{"env_var":"WIF","default_val":"{}"}"#),
            ExternalAccountConfig::Ref(ConfigValue::Dynamic { .. })
        ));
        assert!(matches!(
            config(r#"{"type":"external_account","audience":"a"}"#),
            ExternalAccountConfig::Inline(_)
        ));
    }

    #[test]
    fn an_ambiguous_object_is_refused() {
        // A reference key beside an inline configuration's: which was meant
        // cannot be decided, so neither is chosen.
        for config in [
            r#"{"type":"external_account","secret_ref":"vault://wif"}"#,
            r#"{"type":"external_account","env_var":"WIF"}"#,
            r#"{"env_var":"WIF","audience":"a"}"#,
        ] {
            let json = format!(r#"{{"source":"external_account","config":{config}}}"#);
            assert!(
                serde_json::from_str::<GoogleCredentials>(&json).is_err(),
                "{config} must be refused"
            );
        }
    }

    #[test]
    fn an_object_that_is_neither_is_refused() {
        for config in [
            r"{}",
            r#"{"audience":"a"}"#,
            r#"{"env_var":"E","extra":1}"#,
            "5",
        ] {
            let json = format!(r#"{{"source":"external_account","config":{config}}}"#);
            assert!(
                serde_json::from_str::<GoogleCredentials>(&json).is_err(),
                "{config} must be refused"
            );
        }
    }

    #[test]
    fn debug_redacts_a_literal_key() {
        let creds = GoogleCredentials::ServiceAccount {
            key: ConfigValue::literal(r#"{"private_key":"-----BEGIN PRIVATE KEY-----SECRET"}"#),
        };
        let dbg = format!("{creds:?}");
        assert!(!dbg.contains("SECRET"), "{dbg}");
        assert!(!dbg.contains("private_key"), "{dbg}");
        let inline = GoogleCredentials::ExternalAccount {
            config: ExternalAccountConfig::Inline(
                serde_json::json!({"type":"external_account","audience":"//iam.example/aud"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        };
        assert!(!format!("{inline:?}").contains("aud"), "{inline:?}");
    }

    #[test]
    fn a_reference_holding_something_other_than_an_object_is_refused() {
        let config = ExternalAccountConfig::Ref(ConfigValue::literal("[1, 2]"));
        assert!(matches!(
            config.resolve(),
            Err(IcebergError::GoogleCredentialsInvalid(_))
        ));
    }
}
