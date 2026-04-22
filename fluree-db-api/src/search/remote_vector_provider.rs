//! Remote vector search provider.
//!
//! This module provides [`RemoteVectorSearchProvider`], which implements
//! [`VectorIndexProvider`] by making HTTP requests to a remote search service.
//!
//! # Feature Flags
//!
//! This module requires the `search-remote-client` feature. It does NOT
//! require `vector` since it only uses the base vector types
//! (`VectorIndexProvider`, `VectorSearchHit`, `DistanceMetric`) which are
//! always available.

use async_trait::async_trait;
use fluree_db_query::error::{QueryError, Result};
use fluree_db_query::vector::{VectorIndexProvider, VectorSearchHit, VectorSearchParams};
use fluree_search_protocol::{ErrorCode, SearchError, SearchRequest, SearchResponse};
use reqwest::Client;
use std::fmt;
use std::time::Duration;

use super::config::SearchDeploymentConfig;

/// Remote vector search provider that delegates to a search service via HTTP.
///
/// This provider implements [`VectorIndexProvider`] by constructing
/// [`SearchRequest`] messages with [`QueryVariant::Vector`] and sending
/// them to the configured endpoint.
///
/// # Configuration
///
/// The provider is configured via [`SearchDeploymentConfig`], which specifies:
/// - `endpoint`: The search service URL (e.g., "http://search.example.com:9090/v1/search")
/// - `auth_token`: Optional bearer token for authentication
/// - `connect_timeout_ms`: Connection timeout
/// - `request_timeout_ms`: Per-request timeout
///
/// # Example
///
/// ```ignore
/// use fluree_db_api::search::{RemoteVectorSearchProvider, SearchDeploymentConfig};
/// use fluree_db_query::vector::DistanceMetric;
///
/// let config = SearchDeploymentConfig::remote("http://search.example.com:9090/v1/search")
///     .with_auth_token("my-token");
/// let provider = RemoteVectorSearchProvider::from_config(&config)?;
///
/// let results = provider.search(
///     "embeddings:main", &[0.1, 0.2, 0.3],
///     DistanceMetric::Cosine, 10, 100, false, None,
/// ).await?;
/// ```
pub struct RemoteVectorSearchProvider {
    /// HTTP client.
    client: Client,
    /// Search service endpoint.
    endpoint: String,
    /// Optional authentication token.
    auth_token: Option<String>,
    /// Request timeout.
    request_timeout: Duration,
}

impl RemoteVectorSearchProvider {
    /// Create a new remote provider from configuration.
    pub fn from_config(config: &SearchDeploymentConfig) -> Result<Self> {
        let endpoint = config.endpoint.as_ref().ok_or_else(|| {
            QueryError::InvalidQuery("Remote search config missing 'endpoint'".to_string())
        })?;

        let connect_timeout = Duration::from_millis(config.connect_timeout_ms.unwrap_or(5_000));
        let request_timeout = Duration::from_millis(config.request_timeout_ms.unwrap_or(30_000));

        let client = Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()
            .map_err(|e| QueryError::Internal(format!("Failed to create HTTP client: {e}")))?;

        Ok(Self {
            client,
            endpoint: endpoint.clone(),
            auth_token: config.auth_token.clone(),
            request_timeout,
        })
    }

    /// Create a new remote provider with explicit parameters.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            endpoint: endpoint.into(),
            auth_token: None,
            request_timeout: Duration::from_secs(30),
        }
    }

    /// Set the authentication token.
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Set the request timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }
}

impl fmt::Debug for RemoteVectorSearchProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteVectorSearchProvider")
            .field("endpoint", &self.endpoint)
            .field("has_auth_token", &self.auth_token.is_some())
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

#[async_trait]
impl VectorIndexProvider for RemoteVectorSearchProvider {
    async fn search(
        &self,
        graph_source_id: &str,
        params: VectorSearchParams<'_>,
    ) -> Result<Vec<VectorSearchHit>> {
        // Build the search request
        let mut request =
            SearchRequest::vector(graph_source_id, params.query_vector.to_vec(), params.limit);
        request.as_of_t = params.as_of_t;
        request.sync = params.sync;
        request.timeout_ms = params.timeout_ms;

        // Set the metric on the query variant
        if let fluree_search_protocol::QueryVariant::Vector {
            metric: ref mut m, ..
        } = request.query
        {
            *m = Some(params.metric.to_string());
        }

        // Use the per-request timeout if provided, otherwise use default
        let timeout = params
            .timeout_ms
            .map(Duration::from_millis)
            .unwrap_or(self.request_timeout);

        // Build HTTP request
        let mut http_request = self.client.post(&self.endpoint).json(&request);

        // Add auth header if configured
        if let Some(ref token) = self.auth_token {
            http_request = http_request.bearer_auth(token);
        }

        // Send request
        let response = http_request.timeout(timeout).send().await.map_err(|e| {
            if e.is_timeout() {
                QueryError::Internal(format!("Vector search request timeout: {e}"))
            } else if e.is_connect() {
                QueryError::Internal(format!("Failed to connect to search service: {e}"))
            } else {
                QueryError::Internal(format!("Vector search request failed: {e}"))
            }
        })?;

        // Check for HTTP errors
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            // Try to parse as protocol SearchError for structured error info
            if let Ok(search_error) = serde_json::from_str::<SearchError>(&body) {
                let code = search_error.error.code;
                let msg = search_error.error.message;
                return Err(match code {
                    ErrorCode::GraphSourceNotFound
                    | ErrorCode::IndexNotBuilt
                    | ErrorCode::NoSnapshotForAsOfT => {
                        QueryError::InvalidQuery(format!("{code}: {msg}"))
                    }
                    ErrorCode::InvalidRequest => QueryError::InvalidQuery(msg),
                    _ => QueryError::Internal(format!("{code}: {msg}")),
                });
            }
            return Err(QueryError::Internal(format!(
                "Search service returned {status}: {body}"
            )));
        }

        // Parse response
        let search_response: SearchResponse = response
            .json()
            .await
            .map_err(|e| QueryError::Internal(format!("Failed to parse search response: {e}")))?;

        // Convert SearchHit -> VectorSearchHit
        let hits = search_response
            .hits
            .into_iter()
            .map(|hit| VectorSearchHit::new(hit.iri, hit.ledger_alias, hit.score))
            .collect();

        Ok(hits)
    }

    async fn collection_exists(&self, _graph_source_id: &str) -> Result<bool> {
        // For remote mode, optimistically return true.
        // The remote service will return an error if the collection doesn't exist.
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_config_missing_endpoint() {
        let config = SearchDeploymentConfig::default(); // embedded mode, no endpoint
        let result = RemoteVectorSearchProvider::from_config(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_config_with_endpoint() {
        let config = SearchDeploymentConfig::remote("http://localhost:9090/v1/search");
        let result = RemoteVectorSearchProvider::from_config(&config);
        assert!(result.is_ok());
        let provider = result.unwrap();
        assert_eq!(provider.endpoint, "http://localhost:9090/v1/search");
    }

    #[test]
    fn test_from_config_with_auth() {
        let config = SearchDeploymentConfig::remote("http://localhost:9090/v1/search")
            .with_auth_token("my-secret-token");
        let provider = RemoteVectorSearchProvider::from_config(&config).unwrap();
        assert_eq!(provider.auth_token, Some("my-secret-token".to_string()));
    }

    #[test]
    fn test_builder_pattern() {
        let provider = RemoteVectorSearchProvider::new("http://localhost:9090")
            .with_auth_token("token")
            .with_timeout(Duration::from_secs(60));

        assert_eq!(provider.endpoint, "http://localhost:9090");
        assert_eq!(provider.auth_token, Some("token".to_string()));
        assert_eq!(provider.request_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_debug_hides_token() {
        let provider = RemoteVectorSearchProvider::new("http://localhost:9090")
            .with_auth_token("secret-token");

        let debug_output = format!("{provider:?}");
        assert!(debug_output.contains("has_auth_token: true"));
        assert!(!debug_output.contains("secret-token"));
    }
}
