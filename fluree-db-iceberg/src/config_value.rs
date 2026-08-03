//! Configuration value with environment variable expansion.
//!
//! This module provides [`ConfigValue`], a flexible configuration type that
//! can hold either a literal string value or resolve dynamically from
//! environment variables with optional defaults.

use crate::error::{IcebergError, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A configuration value that can be either a literal string or resolved
/// dynamically from environment variables.
///
/// # JSON Formats
///
/// **Literal string:**
/// ```json
/// "my-literal-value"
/// ```
///
/// **Environment variable:**
/// ```json
/// {"env_var": "MY_TOKEN"}
/// ```
///
/// **Environment variable with default:**
/// ```json
/// {"env_var": "MY_TOKEN", "default_val": "fallback-value"}
/// ```
///
/// # Example
///
/// ```
/// use fluree_db_iceberg::ConfigValue;
///
/// // Literal value
/// let literal: ConfigValue = serde_json::from_str(r#""my-token""#).unwrap();
/// assert_eq!(literal.resolve().unwrap(), "my-token");
///
/// // With default (when env var not set)
/// let with_default: ConfigValue = serde_json::from_str(
///     r#"{"env_var": "UNSET_VAR", "default_val": "default"}"#
/// ).unwrap();
/// assert_eq!(with_default.resolve().unwrap(), "default");
/// ```
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum ConfigValue {
    /// Literal string value
    Literal(String),
    /// Opaque reference to a secret held outside the config, resolved at use
    /// time by an injected [`SecretResolver`]. Wire shape: `{"secret_ref":
    /// "<opaque>"}`. The reference is passed to the resolver verbatim — this
    /// crate never parses or interprets it.
    ///
    /// **Variant ORDER is load-bearing.** `serde(untagged)` tries variants in
    /// declaration order, and `Dynamic`'s fields are all `#[serde(default)]`, so
    /// `Dynamic` matches *any* JSON object (it would silently swallow
    /// `{"secret_ref": ...}`). `SecretRef` MUST therefore be declared BEFORE
    /// `Dynamic`. The `secret_ref` field is REQUIRED (no serde default) so that
    /// `{"env_var": ...}` fails to match `SecretRef` and correctly falls through
    /// to `Dynamic`.
    SecretRef {
        /// Opaque secret reference, passed to the resolver as-is.
        secret_ref: String,
    },
    /// Dynamic value from environment or properties
    Dynamic {
        /// Environment variable name to resolve
        #[serde(default)]
        env_var: Option<String>,
        /// Java system property name (for JVM interop, currently ignored in Rust)
        #[serde(default)]
        java_property: Option<String>,
        /// Default value if env var is not set
        #[serde(default)]
        default_val: Option<String>,
    },
}

/// Redacting `Debug`: `ConfigValue` carries auth secrets (bearer tokens, OAuth2
/// client secrets), so a `{:?}` in a log or error must never leak them. The
/// environment-variable *name* is shown (it aids debugging and is not secret);
/// the literal value and any inline default are redacted.
impl std::fmt::Debug for ConfigValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigValue::Literal(_) => f.write_str("Literal(\"***\")"),
            // A secret *reference* is not itself a secret (same policy as an
            // env-var name), so the ref is shown to aid debugging.
            ConfigValue::SecretRef { secret_ref } => f
                .debug_struct("SecretRef")
                .field("secret_ref", secret_ref)
                .finish(),
            ConfigValue::Dynamic {
                env_var,
                java_property,
                default_val,
            } => f
                .debug_struct("Dynamic")
                .field("env_var", env_var)
                .field("java_property", java_property)
                .field("default_val", &default_val.as_ref().map(|_| "***"))
                .finish(),
        }
    }
}

impl ConfigValue {
    /// Create a literal config value.
    pub fn literal(value: impl Into<String>) -> Self {
        Self::Literal(value.into())
    }

    /// Create a config value from an environment variable.
    pub fn from_env(var_name: impl Into<String>) -> Self {
        Self::Dynamic {
            env_var: Some(var_name.into()),
            java_property: None,
            default_val: None,
        }
    }

    /// Create a config value from an environment variable with a default.
    pub fn from_env_with_default(var_name: impl Into<String>, default: impl Into<String>) -> Self {
        Self::Dynamic {
            env_var: Some(var_name.into()),
            java_property: None,
            default_val: Some(default.into()),
        }
    }

    /// Resolve the configuration value to a string.
    ///
    /// For literal values, returns the string directly.
    /// For dynamic values, attempts to resolve from environment variable,
    /// falling back to the default if provided.
    ///
    /// # Errors
    ///
    /// Returns an error if the value is dynamic, the environment variable
    /// is not set, and no default is provided.
    pub fn resolve(&self) -> Result<String> {
        match self {
            ConfigValue::Literal(value) => Ok(value.clone()),
            // A `SecretRef` cannot be resolved synchronously from config alone —
            // it needs the injected async resolver (see [`ConfigValue::hydrate`]).
            // Fail closed so every path that never threaded a resolver (OSS/CLI)
            // errors actionably instead of silently using an empty/wrong value.
            ConfigValue::SecretRef { .. } => Err(IcebergError::credential(
                "secret reference requires an injected secret resolver; \
                 OSS/CLI contexts must use literals or env vars",
            )),
            ConfigValue::Dynamic {
                env_var,
                java_property: _,
                default_val,
            } => {
                // Try environment variable first
                if let Some(var_name) = env_var {
                    if let Ok(value) = std::env::var(var_name) {
                        return Ok(value);
                    }
                }

                // Fall back to default
                if let Some(default) = default_val {
                    return Ok(default.clone());
                }

                // No value found
                let var_name = env_var.as_deref().unwrap_or("(unspecified)");
                Err(IcebergError::Config(format!(
                    "Environment variable '{var_name}' not set and no default provided"
                )))
            }
        }
    }

    /// Check if this is a literal value (no dynamic resolution needed).
    pub fn is_literal(&self) -> bool {
        matches!(self, ConfigValue::Literal(_))
    }

    /// Resolve a [`ConfigValue::SecretRef`] to a [`ConfigValue::Literal`] using
    /// the injected `resolver`, cloning every other variant through untouched.
    ///
    /// Async because resolution may hit a remote secret backend. This is the
    /// bridge that lets the rest of the auth stack (`create_provider*`) stay
    /// synchronous: hydrate once (async), then resolve/build synchronously.
    ///
    /// # Errors
    ///
    /// - `SecretRef` with `Some` resolver: any [`SecretResolveError`] is mapped
    ///   to [`IcebergError::Credential`], preserving the Denied/NotFound/
    ///   Unavailable kind in the message.
    /// - `SecretRef` with `None` resolver: fails closed with the same actionable
    ///   message as [`ConfigValue::resolve`].
    pub async fn hydrate(&self, resolver: Option<&Arc<dyn SecretResolver>>) -> Result<ConfigValue> {
        match self {
            ConfigValue::SecretRef { secret_ref } => match resolver {
                Some(resolver) => {
                    let value = resolver.resolve_secret(secret_ref).await.map_err(|e| {
                        IcebergError::credential(format!("failed to resolve secret reference: {e}"))
                    })?;
                    Ok(ConfigValue::Literal(value))
                }
                None => Err(IcebergError::credential(
                    "secret reference requires an injected secret resolver; \
                     OSS/CLI contexts must use literals or env vars",
                )),
            },
            other => Ok(other.clone()),
        }
    }
}

/// Error returned by a [`SecretResolver`] when a secret reference cannot be
/// turned into a value. The kind is preserved so callers (and their surfaced
/// errors) can distinguish an authorization failure from a genuinely missing
/// secret or a transient backend outage.
#[derive(Debug, thiserror::Error)]
pub enum SecretResolveError {
    /// The caller was authenticated but is not authorized for this secret.
    #[error("secret reference denied: {0}")]
    Denied(String),
    /// No secret exists for the given reference.
    #[error("secret reference not found: {0}")]
    NotFound(String),
    /// The secret backend was unreachable or otherwise failed transiently.
    #[error("secret resolver unavailable: {0}")]
    Unavailable(String),
}

/// Resolves opaque secret references ([`ConfigValue::SecretRef`]) to their
/// secret values.
///
/// The implementation performs its **own authorization** — the host constructs
/// it with the tenant/principal captured — so this crate (and db) never sees
/// tenant identity and stays tenant-agnostic. The `secret_ref` string is passed
/// through verbatim; db never parses or interprets it.
#[async_trait]
pub trait SecretResolver: Send + Sync + std::fmt::Debug {
    /// Resolve `secret_ref` to its secret value, or a typed error describing why
    /// not. Implementations authorize the (captured) caller before returning.
    async fn resolve_secret(
        &self,
        secret_ref: &str,
    ) -> std::result::Result<String, SecretResolveError>;
}

impl From<String> for ConfigValue {
    fn from(s: String) -> Self {
        ConfigValue::Literal(s)
    }
}

impl From<&str> for ConfigValue {
    fn from(s: &str) -> Self {
        ConfigValue::Literal(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_literal_value() {
        let value = ConfigValue::literal("my-token");
        assert_eq!(value.resolve().unwrap(), "my-token");
        assert!(value.is_literal());
    }

    #[test]
    fn test_parse_literal_from_json() {
        let json = r#""my-literal-value""#;
        let value: ConfigValue = serde_json::from_str(json).unwrap();
        assert_eq!(value.resolve().unwrap(), "my-literal-value");
    }

    #[test]
    fn test_parse_dynamic_from_json() {
        let json = r#"{"env_var": "TEST_VAR", "default_val": "fallback"}"#;
        let value: ConfigValue = serde_json::from_str(json).unwrap();
        // Should use default since TEST_VAR is likely not set
        assert_eq!(value.resolve().unwrap(), "fallback");
        assert!(!value.is_literal());
    }

    #[test]
    fn test_env_var_resolution() {
        // Set a test env var
        std::env::set_var("ICEBERG_TEST_TOKEN", "resolved-value");

        let value = ConfigValue::from_env("ICEBERG_TEST_TOKEN");
        assert_eq!(value.resolve().unwrap(), "resolved-value");

        // Clean up
        std::env::remove_var("ICEBERG_TEST_TOKEN");
    }

    #[test]
    fn test_env_var_with_default() {
        let value = ConfigValue::from_env_with_default("UNSET_VARIABLE_12345", "my-default");
        assert_eq!(value.resolve().unwrap(), "my-default");
    }

    #[test]
    fn test_missing_env_var_no_default() {
        let value = ConfigValue::from_env("DEFINITELY_NOT_SET_12345");
        let result = value.resolve();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("DEFINITELY_NOT_SET_12345"));
    }

    #[test]
    fn test_serialize_literal() {
        let value = ConfigValue::literal("test");
        let json = serde_json::to_string(&value).unwrap();
        assert_eq!(json, r#""test""#);
    }

    #[test]
    fn test_serialize_dynamic() {
        let value = ConfigValue::from_env_with_default("MY_VAR", "default");
        let json = serde_json::to_string(&value).unwrap();
        // Should serialize with the dynamic structure
        assert!(json.contains("env_var"));
        assert!(json.contains("MY_VAR"));
        assert!(json.contains("default_val"));
    }

    #[test]
    fn test_from_string() {
        let value: ConfigValue = "test".into();
        assert!(value.is_literal());
        assert_eq!(value.resolve().unwrap(), "test");
    }

    #[test]
    fn debug_redacts_literal_secret() {
        let value = ConfigValue::literal("super-secret-token");
        let dbg = format!("{value:?}");
        assert!(
            !dbg.contains("super-secret-token"),
            "Debug must not leak the literal secret, got: {dbg}"
        );
        assert!(dbg.contains("***"), "got: {dbg}");
    }

    #[test]
    fn debug_shows_env_var_name_but_redacts_default() {
        let value = ConfigValue::from_env_with_default("POLARIS_TOKEN", "fallback-secret");
        let dbg = format!("{value:?}");
        // The env var NAME is safe to show; the inline default is a secret.
        assert!(dbg.contains("POLARIS_TOKEN"), "got: {dbg}");
        assert!(
            !dbg.contains("fallback-secret"),
            "Debug must not leak the default secret, got: {dbg}"
        );
    }

    // ── SecretRef: ordering canary + resolve + hydrate ──

    /// A resolver stub whose behavior is selected by the ref value, so a single
    /// impl exercises success and every error kind.
    #[derive(Debug)]
    struct StubResolver;

    #[async_trait]
    impl SecretResolver for StubResolver {
        async fn resolve_secret(
            &self,
            secret_ref: &str,
        ) -> std::result::Result<String, SecretResolveError> {
            match secret_ref {
                "deny" => Err(SecretResolveError::Denied("no access to secret".into())),
                "missing" => Err(SecretResolveError::NotFound("unknown ref".into())),
                "down" => Err(SecretResolveError::Unavailable("backend down".into())),
                other => Ok(format!("resolved:{other}")),
            }
        }
    }

    #[test]
    fn secret_ref_parses_and_is_not_dynamic() {
        // ORDERING CANARY: {"secret_ref": ...} MUST deserialize to SecretRef, not
        // be silently swallowed by Dynamic (whose all-default fields match any
        // object). If SecretRef is ever moved after Dynamic, this fails.
        let value: ConfigValue = serde_json::from_str(r#"{"secret_ref": "vault://abc"}"#).unwrap();
        match value {
            ConfigValue::SecretRef { ref secret_ref } => assert_eq!(secret_ref, "vault://abc"),
            other => panic!("expected SecretRef, got {other:?}"),
        }
    }

    #[test]
    fn env_var_object_still_parses_as_dynamic() {
        // The complement of the ordering canary: a required `secret_ref` field
        // means {"env_var": ...} does NOT match SecretRef and falls to Dynamic.
        let value: ConfigValue = serde_json::from_str(r#"{"env_var": "E"}"#).unwrap();
        assert!(
            matches!(value, ConfigValue::Dynamic { .. }),
            "got {value:?}"
        );
    }

    #[test]
    fn bare_string_still_parses_as_literal() {
        let value: ConfigValue = serde_json::from_str(r#""just-a-string""#).unwrap();
        assert!(matches!(value, ConfigValue::Literal(_)), "got {value:?}");
    }

    #[test]
    fn secret_ref_round_trips_losslessly() {
        let value = ConfigValue::SecretRef {
            secret_ref: "vault://team/secret".to_string(),
        };
        let json = serde_json::to_string(&value).unwrap();
        assert_eq!(json, r#"{"secret_ref":"vault://team/secret"}"#);
        let back: ConfigValue = serde_json::from_str(&json).unwrap();
        assert_eq!(back, value);
    }

    #[test]
    fn secret_ref_debug_shows_ref() {
        // A reference is not a secret; it stays visible (aids debugging).
        let value = ConfigValue::SecretRef {
            secret_ref: "vault://visible-ref".to_string(),
        };
        let dbg = format!("{value:?}");
        assert!(dbg.contains("vault://visible-ref"), "got: {dbg}");
    }

    #[test]
    fn resolve_on_secret_ref_errors_actionably() {
        let value = ConfigValue::SecretRef {
            secret_ref: "vault://abc".to_string(),
        };
        let err = value.resolve().unwrap_err().to_string();
        assert!(err.contains("secret resolver"), "got: {err}");
    }

    #[tokio::test]
    async fn hydrate_secret_ref_with_resolver_becomes_literal() {
        let resolver: Arc<dyn SecretResolver> = Arc::new(StubResolver);
        let value = ConfigValue::SecretRef {
            secret_ref: "abc".to_string(),
        };
        let hydrated = value.hydrate(Some(&resolver)).await.unwrap();
        assert_eq!(hydrated, ConfigValue::Literal("resolved:abc".to_string()));
    }

    #[tokio::test]
    async fn hydrate_secret_ref_without_resolver_fails_closed() {
        let value = ConfigValue::SecretRef {
            secret_ref: "abc".to_string(),
        };
        let err = value.hydrate(None).await.unwrap_err().to_string();
        assert!(err.contains("secret resolver"), "got: {err}");
    }

    #[tokio::test]
    async fn hydrate_passes_literal_and_dynamic_through() {
        let resolver: Arc<dyn SecretResolver> = Arc::new(StubResolver);
        let literal = ConfigValue::literal("plain");
        assert_eq!(
            literal.hydrate(Some(&resolver)).await.unwrap(),
            ConfigValue::literal("plain")
        );
        let dynamic = ConfigValue::from_env("SOME_VAR");
        assert_eq!(
            dynamic.hydrate(Some(&resolver)).await.unwrap(),
            ConfigValue::from_env("SOME_VAR")
        );
    }

    #[tokio::test]
    async fn hydrate_preserves_denied_kind_in_message() {
        let resolver: Arc<dyn SecretResolver> = Arc::new(StubResolver);
        let value = ConfigValue::SecretRef {
            secret_ref: "deny".to_string(),
        };
        let err = value
            .hydrate(Some(&resolver))
            .await
            .unwrap_err()
            .to_string();
        // The Denied kind (via SecretResolveError's Display) survives the mapping.
        assert!(err.contains("denied"), "kind lost in mapping: {err}");
    }
}
