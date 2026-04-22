//! Polaris-compatible REST catalog client.

use crate::auth::SendCatalogAuth;
use crate::catalog::{
    encode_namespace_for_rest, CatalogClient, LoadTableResponse, TableIdentifier,
};
use crate::credential::VendedCredentials;
use crate::error::{IcebergError, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Configuration for REST catalog client.
#[derive(Debug, Clone)]
pub struct RestCatalogConfig {
    /// Base URI for the REST catalog (e.g., "https://polaris.example.com")
    pub uri: String,
    /// Optional warehouse identifier
    pub warehouse: Option<String>,
    /// Connect timeout in seconds (default: 30)
    pub connect_timeout_secs: u64,
    /// Request timeout in seconds (default: 60)
    pub request_timeout_secs: u64,
}

impl Default for RestCatalogConfig {
    fn default() -> Self {
        Self {
            uri: String::new(),
            warehouse: None,
            connect_timeout_secs: 30,
            request_timeout_secs: 60,
        }
    }
}

/// Polaris-compatible REST catalog client.
///
/// Uses `SendCatalogAuth` for Send-safe futures, enabling use with
/// tokio::spawn and async_trait without ?Send.
pub struct RestCatalogClient {
    pub(crate) config: RestCatalogConfig,
    auth: Arc<dyn SendCatalogAuth>,
    http_client: reqwest::Client,
}

impl std::fmt::Debug for RestCatalogClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestCatalogClient")
            .field("uri", &self.config.uri)
            .field("warehouse", &self.config.warehouse)
            .finish()
    }
}

impl RestCatalogClient {
    /// Create a new REST catalog client.
    pub fn new(config: RestCatalogConfig, auth: Arc<dyn SendCatalogAuth>) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(config.connect_timeout_secs))
            .timeout(Duration::from_secs(config.request_timeout_secs))
            .build()
            .map_err(|e| IcebergError::Http(format!("Failed to build HTTP client: {e}")))?;

        Ok(Self {
            config,
            auth,
            http_client,
        })
    }

    /// Make a GET request to the catalog API.
    ///
    /// Handles authentication headers and 401 retry with token refresh.
    async fn get(&self, path: &str, headers: &[(&str, &str)]) -> Result<serde_json::Value> {
        self.request_with_retry(path, headers, false).await
    }

    /// Internal request method with retry on 401.
    ///
    /// Uses Box::pin for the recursive call to avoid infinite type size.
    /// Returns a Send future for compatibility with tokio::spawn and async_trait.
    fn request_with_retry<'a>(
        &'a self,
        path: &'a str,
        headers: &'a [(&'a str, &'a str)],
        is_retry: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value>> + Send + 'a>>
    {
        Box::pin(async move {
            let url = format!("{}{}", self.config.uri, path);

            let mut request = self
                .http_client
                .get(&url)
                .header("Accept", "application/json");

            // Add auth header if available
            if let Some(auth_header) = self.auth.authorization_header().await? {
                request = request.header("Authorization", auth_header);
            }

            // Add custom headers
            for (name, value) in headers {
                request = request.header(*name, *value);
            }

            let response = request.send().await?;
            let status = response.status();

            if status == reqwest::StatusCode::UNAUTHORIZED && !is_retry {
                // Try refresh and retry once
                tracing::debug!("Got 401, refreshing auth token and retrying");
                self.auth.refresh().await?;
                return self.request_with_retry(path, headers, true).await;
            }

            if status == reqwest::StatusCode::NOT_FOUND {
                let body = response.text().await.unwrap_or_default();
                return Err(IcebergError::TableNotFound(format!(
                    "Resource not found at {path}: {body}"
                )));
            }

            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                return Err(IcebergError::Catalog(format!(
                    "Catalog request failed ({status}): {body}"
                )));
            }

            response
                .json()
                .await
                .map_err(|e| IcebergError::Catalog(format!("Failed to parse response: {e}")))
        })
    }

    /// Build REST API path for a table.
    fn table_path(&self, table_id: &TableIdentifier) -> String {
        let encoded_ns = encode_namespace_for_rest(&table_id.namespace);
        let base = self.api_prefix();
        format!(
            "{}/namespaces/{}/tables/{}",
            base, encoded_ns, table_id.table
        )
    }

    /// Get the API prefix, optionally including the warehouse.
    ///
    /// Standard Iceberg REST: `/v1/namespaces/...`
    /// Polaris with warehouse: `/v1/{warehouse}/namespaces/...`
    fn api_prefix(&self) -> String {
        match &self.config.warehouse {
            Some(warehouse) => format!("/v1/{warehouse}"),
            None => "/v1".to_string(),
        }
    }
}

#[async_trait(?Send)]
impl CatalogClient for RestCatalogClient {
    async fn list_namespaces(&self) -> Result<Vec<String>> {
        let path = format!("{}/namespaces", self.api_prefix());
        let response = self.get(&path, &[]).await?;

        let namespaces = response
            .get("namespaces")
            .and_then(|v| v.as_array())
            .ok_or_else(|| IcebergError::Catalog("Invalid namespaces response".to_string()))?;

        Ok(namespaces
            .iter()
            .filter_map(|v| {
                // Namespaces are arrays of strings representing levels
                v.as_array().map(|parts| {
                    parts
                        .iter()
                        .filter_map(|p| p.as_str())
                        .collect::<Vec<_>>()
                        .join(".")
                })
            })
            .collect())
    }

    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
        let encoded_ns = encode_namespace_for_rest(namespace);
        let path = format!("{}/namespaces/{}/tables", self.api_prefix(), encoded_ns);
        let response = self.get(&path, &[]).await?;

        let identifiers = response
            .get("identifiers")
            .and_then(|v| v.as_array())
            .ok_or_else(|| IcebergError::Catalog("Invalid tables response".to_string()))?;

        Ok(identifiers
            .iter()
            .filter_map(|id| {
                let ns = id
                    .get("namespace")
                    .and_then(|v| v.as_array())
                    .map(|parts| {
                        parts
                            .iter()
                            .filter_map(|p| p.as_str())
                            .collect::<Vec<_>>()
                            .join(".")
                    })?;
                let table = id.get("name").and_then(|v| v.as_str())?;
                Some(format!("{ns}.{table}"))
            })
            .collect())
    }

    async fn load_table(
        &self,
        table_id: &TableIdentifier,
        request_credentials: bool,
    ) -> Result<LoadTableResponse> {
        let path = self.table_path(table_id);

        let headers = if request_credentials {
            vec![("X-Iceberg-Access-Delegation", "vended-credentials")]
        } else {
            vec![]
        };

        let response = self.get(&path, &headers).await?;

        let metadata_location = response
            .get("metadata-location")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                IcebergError::Catalog("Missing metadata-location in response".to_string())
            })?
            .to_string();

        // Extract config map
        let config: HashMap<String, serde_json::Value> =
            if let Some(config_obj) = response.get("config").and_then(|v| v.as_object()) {
                config_obj
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            } else {
                HashMap::new()
            };

        // Parse vended credentials if present
        let credentials = VendedCredentials::from_config_map(&config)?;

        Ok(LoadTableResponse {
            metadata_location,
            config,
            credentials,
        })
    }
}

#[async_trait]
impl super::SendCatalogClient for RestCatalogClient {
    async fn list_namespaces(&self) -> Result<Vec<String>> {
        let path = format!("{}/namespaces", self.api_prefix());
        let response = self.get(&path, &[]).await?;

        let namespaces = response
            .get("namespaces")
            .and_then(|v| v.as_array())
            .ok_or_else(|| IcebergError::Catalog("Invalid namespaces response".to_string()))?;

        Ok(namespaces
            .iter()
            .filter_map(|v| {
                v.as_array().map(|parts| {
                    parts
                        .iter()
                        .filter_map(|p| p.as_str())
                        .collect::<Vec<_>>()
                        .join(".")
                })
            })
            .collect())
    }

    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
        let encoded_ns = encode_namespace_for_rest(namespace);
        let path = format!("{}/namespaces/{}/tables", self.api_prefix(), encoded_ns);
        let response = self.get(&path, &[]).await?;

        let identifiers = response
            .get("identifiers")
            .and_then(|v| v.as_array())
            .ok_or_else(|| IcebergError::Catalog("Invalid tables response".to_string()))?;

        Ok(identifiers
            .iter()
            .filter_map(|id| {
                let ns = id
                    .get("namespace")
                    .and_then(|v| v.as_array())
                    .map(|parts| {
                        parts
                            .iter()
                            .filter_map(|p| p.as_str())
                            .collect::<Vec<_>>()
                            .join(".")
                    })?;
                let table = id.get("name").and_then(|v| v.as_str())?;
                Some(format!("{ns}.{table}"))
            })
            .collect())
    }

    async fn load_table(
        &self,
        table_id: &TableIdentifier,
        request_credentials: bool,
    ) -> Result<LoadTableResponse> {
        let path = self.table_path(table_id);

        let headers = if request_credentials {
            vec![("X-Iceberg-Access-Delegation", "vended-credentials")]
        } else {
            vec![]
        };

        let response = self.get(&path, &headers).await?;

        let metadata_location = response
            .get("metadata-location")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                IcebergError::Catalog("Missing metadata-location in response".to_string())
            })?
            .to_string();

        let config: HashMap<String, serde_json::Value> =
            if let Some(config_obj) = response.get("config").and_then(|v| v.as_object()) {
                config_obj
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            } else {
                HashMap::new()
            };

        let credentials = VendedCredentials::from_config_map(&config)?;

        Ok(LoadTableResponse {
            metadata_location,
            config,
            credentials,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_path_single_namespace() {
        let client = RestCatalogClient {
            config: RestCatalogConfig {
                uri: "https://polaris.example.com".to_string(),
                ..Default::default()
            },
            auth: Arc::new(crate::auth::BearerTokenAuth::new("test".to_string())),
            http_client: reqwest::Client::new(),
        };

        let table_id = TableIdentifier::new("openflights", "airlines");
        let path = client.table_path(&table_id);
        assert_eq!(path, "/v1/namespaces/openflights/tables/airlines");
    }

    #[test]
    fn test_table_path_multi_level_namespace() {
        let client = RestCatalogClient {
            config: RestCatalogConfig {
                uri: "https://polaris.example.com".to_string(),
                ..Default::default()
            },
            auth: Arc::new(crate::auth::BearerTokenAuth::new("test".to_string())),
            http_client: reqwest::Client::new(),
        };

        let table_id = TableIdentifier::new("db.schema", "events");
        let path = client.table_path(&table_id);
        // Multi-level namespace should use unit separator encoding
        assert_eq!(path, "/v1/namespaces/db%1Fschema/tables/events");
    }
}
