//! Remote BM25 search provider.
//!
//! This module provides [`RemoteBm25SearchProvider`], which implements
//! [`Bm25SearchProvider`] by making HTTP requests to a remote search service.
//!
//! # Feature Flag
//!
//! This module is only available when the `search-remote-client` feature is enabled.

use async_trait::async_trait;
use fluree_db_query::bm25::{Bm25SearchProvider, Bm25SearchResult};
use fluree_db_query::error::{QueryError, Result};
use fluree_search_protocol::{ErrorCode, SearchError, SearchRequest, SearchResponse};
use reqwest::Client;
use std::fmt;
use std::time::Duration;

use super::config::SearchDeploymentConfig;

/// Remote BM25 search provider that delegates to a search service via HTTP.
///
/// This provider implements [`Bm25SearchProvider`] by constructing
/// [`SearchRequest`] messages and sending them to the configured endpoint.
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
/// use fluree_db_api::search::{RemoteBm25SearchProvider, SearchDeploymentConfig};
///
/// let config = SearchDeploymentConfig::remote("http://search.example.com:9090/v1/search")
///     .with_auth_token("my-token");
/// let provider = RemoteBm25SearchProvider::from_config(&config)?;
///
/// let result = provider.search_bm25("products:main", "wireless", 10, None, false, None).await?;
/// ```
pub struct RemoteBm25SearchProvider {
    /// HTTP client.
    client: Client,
    /// Search service endpoint.
    endpoint: String,
    /// Optional authentication token.
    auth_token: Option<String>,
    /// Request timeout.
    request_timeout: Duration,
}

impl RemoteBm25SearchProvider {
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

impl fmt::Debug for RemoteBm25SearchProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteBm25SearchProvider")
            .field("endpoint", &self.endpoint)
            .field("has_auth_token", &self.auth_token.is_some())
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

#[async_trait]
impl Bm25SearchProvider for RemoteBm25SearchProvider {
    async fn search_bm25(
        &self,
        graph_source_id: &str,
        query_text: &str,
        limit: usize,
        as_of_t: Option<i64>,
        sync: bool,
        timeout_ms: Option<u64>,
    ) -> Result<Bm25SearchResult> {
        // Build the search request
        let mut request = SearchRequest::bm25(graph_source_id, query_text, limit);
        request.as_of_t = as_of_t;
        request.sync = sync;
        request.timeout_ms = timeout_ms;

        // Use the per-request timeout if provided, otherwise use default
        let timeout = timeout_ms
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
                QueryError::Internal(format!("Search request timeout: {e}"))
            } else if e.is_connect() {
                QueryError::Internal(format!("Failed to connect to search service: {e}"))
            } else {
                QueryError::Internal(format!("Search request failed: {e}"))
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

        Ok(Bm25SearchResult::new(
            search_response.index_t,
            search_response.hits,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_config_missing_endpoint() {
        let config = SearchDeploymentConfig::default(); // embedded mode, no endpoint
        let result = RemoteBm25SearchProvider::from_config(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_config_with_endpoint() {
        let config = SearchDeploymentConfig::remote("http://localhost:9090/v1/search");
        let result = RemoteBm25SearchProvider::from_config(&config);
        assert!(result.is_ok());
        let provider = result.unwrap();
        assert_eq!(provider.endpoint, "http://localhost:9090/v1/search");
    }

    #[test]
    fn test_from_config_with_auth() {
        let config = SearchDeploymentConfig::remote("http://localhost:9090/v1/search")
            .with_auth_token("my-secret-token");
        let provider = RemoteBm25SearchProvider::from_config(&config).unwrap();
        assert_eq!(provider.auth_token, Some("my-secret-token".to_string()));
    }

    #[test]
    fn test_builder_pattern() {
        let provider = RemoteBm25SearchProvider::new("http://localhost:9090")
            .with_auth_token("token")
            .with_timeout(Duration::from_secs(60));

        assert_eq!(provider.endpoint, "http://localhost:9090");
        assert_eq!(provider.auth_token, Some("token".to_string()));
        assert_eq!(provider.request_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_debug_hides_token() {
        let provider =
            RemoteBm25SearchProvider::new("http://localhost:9090").with_auth_token("secret-token");

        let debug_output = format!("{provider:?}");
        assert!(debug_output.contains("has_auth_token: true"));
        assert!(!debug_output.contains("secret-token"));
    }
}
