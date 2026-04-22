//! Concrete `RemoteServiceExecutor` implementation for remote Fluree instances.
//!
//! Provides `HttpRemoteService` (behind the `search-remote-client` feature) which
//! sends SPARQL queries to remote Fluree servers via HTTP, and `RemoteConnectionRegistry`
//! which maps connection aliases to `(base_url, bearer_token)` pairs.

use fluree_db_query::remote_service::{RemoteQueryResult, RemoteServiceExecutor};
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

/// A registered remote Fluree connection.
#[derive(Clone)]
pub struct RemoteConnection {
    /// Base URL of the remote server (e.g., "https://acme-fluree.example.com")
    pub base_url: String,
    /// Bearer token for authentication (None = unauthenticated)
    pub token: Option<String>,
    /// Per-endpoint timeout (default: 30 seconds)
    pub timeout: Duration,
}

impl RemoteConnection {
    pub fn new(base_url: impl Into<String>, token: Option<String>) -> Self {
        Self {
            base_url: base_url.into(),
            token,
            timeout: Duration::from_secs(30),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

/// Registry mapping connection names to remote Fluree endpoints.
#[derive(Clone, Default)]
pub struct RemoteConnectionRegistry {
    connections: HashMap<String, RemoteConnection>,
}

impl fmt::Debug for RemoteConnectionRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteConnectionRegistry")
            .field("connections", &self.connections.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl RemoteConnectionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, name: impl Into<String>, connection: RemoteConnection) {
        self.connections.insert(name.into(), connection);
    }

    pub fn get(&self, name: &str) -> Option<&RemoteConnection> {
        self.connections.get(name)
    }

    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }
}

/// HTTP-based remote SERVICE executor using reqwest.
///
/// Sends SPARQL queries to remote Fluree servers and parses SPARQL Results JSON responses.
#[cfg(feature = "search-remote-client")]
pub struct HttpRemoteService {
    registry: std::sync::Arc<RemoteConnectionRegistry>,
    client: reqwest::Client,
}

#[cfg(feature = "search-remote-client")]
impl fmt::Debug for HttpRemoteService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpRemoteService")
            .field("registry", &self.registry)
            .finish()
    }
}

#[cfg(feature = "search-remote-client")]
impl HttpRemoteService {
    pub fn new(registry: std::sync::Arc<RemoteConnectionRegistry>) -> Self {
        let client = reqwest::Client::new();
        Self { registry, client }
    }

    fn build_query_url(base_url: &str, ledger: &str) -> String {
        let base = base_url.trim_end_matches('/');
        format!("{base}/v1/fluree/query/{ledger}")
    }
}

#[cfg(feature = "search-remote-client")]
#[async_trait::async_trait]
impl RemoteServiceExecutor for HttpRemoteService {
    async fn execute_remote_sparql(
        &self,
        connection_name: &str,
        ledger: &str,
        sparql: &str,
    ) -> fluree_db_query::error::Result<RemoteQueryResult> {
        use fluree_db_query::error::QueryError;

        let conn = self.registry.get(connection_name).ok_or_else(|| {
            QueryError::InvalidQuery(format!(
                "Unknown remote connection '{connection_name}'. Register it with .remote_connection() on the builder."
            ))
        })?;

        let url = Self::build_query_url(&conn.base_url, ledger);

        let mut req = self
            .client
            .post(&url)
            .header("Content-Type", "application/sparql-query")
            .header("Accept", "application/sparql-results+json")
            .timeout(conn.timeout)
            .body(sparql.to_string());

        if let Some(ref token) = conn.token {
            req = req.bearer_auth(token);
        }

        let response = req.send().await.map_err(|e| {
            QueryError::InvalidQuery(format!("Remote SERVICE request to '{url}' failed: {e}"))
        })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unreadable>".to_string());
            return Err(QueryError::InvalidQuery(format!(
                "Remote SERVICE endpoint '{url}' returned {status}: {body}"
            )));
        }

        let json: serde_json::Value = response.json().await.map_err(|e| {
            QueryError::InvalidQuery(format!(
                "Failed to parse SPARQL results JSON from '{url}': {e}"
            ))
        })?;

        fluree_db_query::sparql_results::parse_sparql_results_json(&json)
    }
}

/// Mock remote executor for testing — returns pre-programmed SPARQL Results JSON responses.
pub struct MockRemoteService {
    responses: std::sync::Mutex<HashMap<String, serde_json::Value>>,
}

impl fmt::Debug for MockRemoteService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockRemoteService").finish()
    }
}

impl Default for MockRemoteService {
    fn default() -> Self {
        Self::new()
    }
}

impl MockRemoteService {
    pub fn new() -> Self {
        Self {
            responses: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Register a canned SPARQL Results JSON response for a given connection+ledger.
    pub fn register_response(&self, connection: &str, ledger: &str, response: serde_json::Value) {
        let key = format!("{connection}/{ledger}");
        self.responses.lock().unwrap().insert(key, response);
    }
}

#[async_trait::async_trait]
impl RemoteServiceExecutor for MockRemoteService {
    async fn execute_remote_sparql(
        &self,
        connection_name: &str,
        ledger: &str,
        _sparql: &str,
    ) -> fluree_db_query::error::Result<RemoteQueryResult> {
        use fluree_db_query::error::QueryError;

        let key = format!("{connection_name}/{ledger}");
        let guard = self.responses.lock().unwrap();
        let json = guard.get(&key).ok_or_else(|| {
            QueryError::InvalidQuery(format!(
                "MockRemoteService: no response registered for '{key}'"
            ))
        })?;

        fluree_db_query::sparql_results::parse_sparql_results_json(json)
    }
}
