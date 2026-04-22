//! Search deployment configuration.
//!
//! This module defines configuration types for search deployment modes,
//! allowing graph sources to use either embedded or remote search providers.

use serde::{Deserialize, Serialize};

/// Deployment mode for a search provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum DeploymentMode {
    /// Embedded mode: index loaded locally, scoring happens in-process.
    #[default]
    Embedded,
    /// Remote mode: search delegated to a remote search service.
    Remote,
}

/// Search deployment configuration.
///
/// This is typically embedded in the graph source configuration
/// to specify how search should be performed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchDeploymentConfig {
    /// Deployment mode (embedded or remote).
    #[serde(default)]
    pub mode: DeploymentMode,

    /// Remote service endpoint (required when mode is Remote).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,

    /// Authentication token for remote service (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,

    /// Connection timeout in milliseconds (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connect_timeout_ms: Option<u64>,

    /// Request timeout in milliseconds (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_timeout_ms: Option<u64>,
}

impl Default for SearchDeploymentConfig {
    fn default() -> Self {
        Self {
            mode: DeploymentMode::Embedded,
            endpoint: None,
            auth_token: None,
            connect_timeout_ms: None,
            request_timeout_ms: None,
        }
    }
}

impl SearchDeploymentConfig {
    /// Create an embedded deployment configuration.
    pub fn embedded() -> Self {
        Self::default()
    }

    /// Create a remote deployment configuration.
    pub fn remote(endpoint: impl Into<String>) -> Self {
        Self {
            mode: DeploymentMode::Remote,
            endpoint: Some(endpoint.into()),
            ..Default::default()
        }
    }

    /// Set the authentication token.
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Set the connection timeout.
    pub fn with_connect_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.connect_timeout_ms = Some(timeout_ms);
        self
    }

    /// Set the request timeout.
    pub fn with_request_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.request_timeout_ms = Some(timeout_ms);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_is_embedded() {
        let config = SearchDeploymentConfig::default();
        assert_eq!(config.mode, DeploymentMode::Embedded);
        assert!(config.endpoint.is_none());
    }

    #[test]
    fn test_remote_config() {
        let config = SearchDeploymentConfig::remote("http://search.example.com:9090")
            .with_auth_token("secret-token")
            .with_request_timeout_ms(10_000);

        assert_eq!(config.mode, DeploymentMode::Remote);
        assert_eq!(
            config.endpoint,
            Some("http://search.example.com:9090".to_string())
        );
        assert_eq!(config.auth_token, Some("secret-token".to_string()));
        assert_eq!(config.request_timeout_ms, Some(10_000));
    }

    #[test]
    fn test_serialization() {
        let config = SearchDeploymentConfig::remote("http://localhost:9090");
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("\"mode\":\"remote\""));
        assert!(json.contains("http://localhost:9090"));

        let parsed: SearchDeploymentConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.mode, DeploymentMode::Remote);
    }

    #[test]
    fn test_embedded_serialization() {
        let config = SearchDeploymentConfig::embedded();
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("\"mode\":\"embedded\""));
        // endpoint should not be in JSON when None
        assert!(!json.contains("endpoint"));
    }
}
