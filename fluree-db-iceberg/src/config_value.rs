//! Configuration value with environment variable expansion.
//!
//! This module provides [`ConfigValue`], a flexible configuration type that
//! can hold either a literal string value or resolve dynamically from
//! environment variables with optional defaults.

use crate::error::{IcebergError, Result};
use serde::{Deserialize, Serialize};

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
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum ConfigValue {
    /// Literal string value
    Literal(String),
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
}
