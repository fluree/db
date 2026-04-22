//! Proxy nameservice implementation for peer mode
//!
//! Fetches nameservice records via the transaction server's `/v1/fluree/storage/ns/{alias}`
//! endpoint instead of direct file access. This allows peers to operate without storage
//! credentials.

use async_trait::async_trait;
use fluree_db_nameservice::{NameService, NameServiceError, NsRecord, Result};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use std::fmt::Debug;
use std::time::Duration;

/// NameService implementation that proxies lookups through the transaction server
#[derive(Clone)]
pub struct ProxyNameService {
    client: Client,
    base_url: String,
    token: String,
}

impl Debug for ProxyNameService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyNameService")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

/// Response from nameservice lookup endpoint.
/// Must match `NsRecordResponse` from routes/storage_proxy.rs.
///
/// Uses `#[serde(default)]` on optional fields so that missing JSON keys
/// deserialize as `None` rather than failing — this keeps the peer
/// forward-compatible when the server adds new fields.
#[derive(Debug, Deserialize)]
struct NsRecordResponse {
    #[serde(default)]
    name: Option<String>,
    branch: String,
    commit_head_id: Option<String>,
    commit_t: i64,
    index_head_id: Option<String>,
    index_t: i64,
    #[serde(default)]
    default_context: Option<String>,
    retracted: bool,
    #[serde(default)]
    config_id: Option<String>,
}

impl NsRecordResponse {
    /// Convert to NsRecord, using the original lookup key as the ledger_id.
    ///
    /// When the server omits `name`, derive it from `lookup_key` by splitting
    /// on `:` (e.g., `"books:main"` → `"books"`). This avoids copying the full
    /// `ledger_id` (which includes the branch) into the `name` field.
    fn into_ns_record(self, lookup_key: &str) -> NsRecord {
        use fluree_db_core::ContentId;

        let derived_name = self.name.unwrap_or_else(|| {
            lookup_key
                .split_once(':')
                .map(|(name, _branch)| name.to_string())
                .unwrap_or_else(|| lookup_key.to_string())
        });

        NsRecord {
            // ledger_id is the key used for lookup (may differ from name)
            ledger_id: lookup_key.to_string(),
            name: derived_name,
            branch: self.branch,
            commit_head_id: self
                .commit_head_id
                .and_then(|s| s.parse::<ContentId>().ok()),
            config_id: self.config_id.and_then(|s| s.parse::<ContentId>().ok()),
            commit_t: self.commit_t,
            index_head_id: self.index_head_id.and_then(|s| s.parse::<ContentId>().ok()),
            index_t: self.index_t,
            default_context: self
                .default_context
                .and_then(|s| s.parse::<ContentId>().ok()),
            retracted: self.retracted,
            source_branch: None,
            branches: 0,
        }
    }
}

impl ProxyNameService {
    /// Create a new proxy nameservice client
    ///
    /// # Arguments
    ///
    /// * `base_url` - Base URL of the transaction server (e.g., `https://tx.fluree.internal:8090`)
    /// * `token` - Bearer token for authentication (with `fluree.storage.*` claims)
    pub fn new(base_url: String, token: String) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30)) // 30 seconds for NS lookups
            .build()
            .expect("Failed to create proxy nameservice client");

        // Normalize base_url by trimming trailing slashes
        let base_url = base_url.trim_end_matches('/').to_string();

        Self {
            client,
            base_url,
            token,
        }
    }

    /// Build the nameservice lookup endpoint URL
    fn ns_url(&self, alias: &str) -> String {
        format!(
            "{}/v1/fluree/storage/ns/{}",
            self.base_url,
            urlencoding::encode(alias)
        )
    }
}

#[async_trait]
impl fluree_db_nameservice::RefLookup for ProxyNameService {
    async fn get_ref(
        &self,
        _ledger_id: &str,
        _kind: fluree_db_nameservice::RefKind,
    ) -> Result<Option<fluree_db_nameservice::RefValue>> {
        Err(NameServiceError::storage(
            "get_ref not supported in proxy mode".to_string(),
        ))
    }
}

#[async_trait]
impl fluree_db_nameservice::StatusLookup for ProxyNameService {
    async fn get_status(
        &self,
        _ledger_id: &str,
    ) -> Result<Option<fluree_db_nameservice::StatusValue>> {
        Err(NameServiceError::storage(
            "get_status not supported in proxy mode".to_string(),
        ))
    }
}

#[async_trait]
impl fluree_db_nameservice::ConfigLookup for ProxyNameService {
    async fn get_config(
        &self,
        _ledger_id: &str,
    ) -> Result<Option<fluree_db_nameservice::ConfigValue>> {
        Err(NameServiceError::storage(
            "get_config not supported in proxy mode".to_string(),
        ))
    }
}

#[async_trait]
impl NameService for ProxyNameService {
    async fn lookup(&self, ledger_id: &str) -> Result<Option<NsRecord>> {
        let url = self.ns_url(ledger_id);

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .await
            .map_err(|e| {
                NameServiceError::storage(format!("Nameservice proxy request failed: {e}"))
            })?;

        let status = response.status();

        match status {
            StatusCode::OK => {
                let ns_response: NsRecordResponse = response.json().await.map_err(|e| {
                    NameServiceError::storage(format!("Failed to parse NS response: {e}"))
                })?;
                Ok(Some(ns_response.into_ns_record(ledger_id)))
            }
            StatusCode::NOT_FOUND => Ok(None),
            StatusCode::UNAUTHORIZED => Err(NameServiceError::storage(format!(
                "Nameservice proxy authentication failed for {ledger_id}: check token validity"
            ))),
            StatusCode::FORBIDDEN => {
                // Not in token scope - treat as not found (no existence leak)
                Ok(None)
            }
            _ => Err(NameServiceError::storage(format!(
                "Nameservice proxy unexpected status {status} for {ledger_id}"
            ))),
        }
    }

    async fn all_records(&self) -> Result<Vec<NsRecord>> {
        // Peers use SSE for discovery, not all_records()
        // Return empty - this is intentional for proxy mode
        // The peer maintains its own view of known ledgers via SSE events
        Ok(Vec::new())
    }

    async fn create_branch(
        &self,
        _ledger_name: &str,
        _new_branch: &str,
        _source_branch: &str,
    ) -> Result<()> {
        // Proxy peers forward branch creation to the tx server via HTTP
        Err(NameServiceError::storage(
            "create_branch not supported in proxy mode".to_string(),
        ))
    }

    async fn drop_branch(&self, _ledger_id: &str) -> Result<Option<u32>> {
        // Proxy peers forward branch deletion to the tx server via HTTP
        Err(NameServiceError::storage(
            "drop_branch not supported in proxy mode".to_string(),
        ))
    }

    async fn reset_head(
        &self,
        _ledger_id: &str,
        _snapshot: fluree_db_nameservice::NsRecordSnapshot,
    ) -> Result<()> {
        Err(NameServiceError::storage(
            "reset_head not supported in proxy mode".to_string(),
        ))
    }
}

#[async_trait]
impl fluree_db_nameservice::GraphSourceLookup for ProxyNameService {
    async fn lookup_graph_source(
        &self,
        _graph_source_id: &str,
    ) -> Result<Option<fluree_db_nameservice::GraphSourceRecord>> {
        Ok(None) // Proxy doesn't have local graph source records
    }

    async fn lookup_any(
        &self,
        _resource_id: &str,
    ) -> Result<fluree_db_nameservice::NsLookupResult> {
        Ok(fluree_db_nameservice::NsLookupResult::NotFound)
    }

    async fn all_graph_source_records(
        &self,
    ) -> Result<Vec<fluree_db_nameservice::GraphSourceRecord>> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proxy_nameservice_debug() {
        let ns = ProxyNameService::new(
            "http://localhost:8090".to_string(),
            "test-token".to_string(),
        );
        let debug = format!("{ns:?}");
        assert!(debug.contains("ProxyNameService"));
        assert!(debug.contains("localhost:8090"));
        // Token should NOT be in debug output
        assert!(!debug.contains("test-token"));
    }

    #[test]
    fn test_ns_url() {
        let ns = ProxyNameService::new(
            "http://localhost:8090".to_string(),
            "test-token".to_string(),
        );
        assert_eq!(
            ns.ns_url("books:main"),
            "http://localhost:8090/v1/fluree/storage/ns/books%3Amain"
        );
    }

    #[test]
    fn test_ns_url_no_special_chars() {
        let ns = ProxyNameService::new(
            "http://localhost:8090".to_string(),
            "test-token".to_string(),
        );
        // Alias without colon doesn't need encoding
        assert_eq!(
            ns.ns_url("books"),
            "http://localhost:8090/v1/fluree/storage/ns/books"
        );
    }

    #[test]
    fn test_ns_record_conversion() {
        let response = NsRecordResponse {
            name: Some("books".to_string()),
            branch: "main".to_string(),
            commit_head_id: None,
            commit_t: 42,
            index_head_id: None,
            index_t: 40,
            default_context: None,
            retracted: false,
            config_id: None,
        };

        // Use the lookup key as ledger_id (simulating lookup("books"))
        let record = response.into_ns_record("books");
        // ledger_id should be the lookup key, not the alias
        assert_eq!(record.ledger_id, "books");
        assert_eq!(record.name, "books");
        assert_eq!(record.branch, "main");
        assert_eq!(record.commit_t, 42);
        assert_eq!(record.index_t, 40);
        assert!(!record.retracted);
        // default_context is not exposed via proxy API
        assert!(record.default_context.is_none());
    }

    #[test]
    fn test_ns_record_name_derived_from_lookup_key() {
        // When server omits `name`, derive it by splitting lookup_key on ':'
        let response = NsRecordResponse {
            name: None,
            branch: "main".to_string(),
            commit_head_id: None,
            commit_t: 10,
            index_head_id: None,
            index_t: 0,
            default_context: None,
            retracted: false,
            config_id: None,
        };

        let record = response.into_ns_record("books:main");
        assert_eq!(record.ledger_id, "books:main");
        // name should be "books", NOT "books:main"
        assert_eq!(record.name, "books");
    }

    #[test]
    fn test_ns_record_name_no_branch_in_lookup_key() {
        // When lookup_key has no colon, use it as-is
        let response = NsRecordResponse {
            name: None,
            branch: "main".to_string(),
            commit_head_id: None,
            commit_t: 10,
            index_head_id: None,
            index_t: 0,
            default_context: None,
            retracted: false,
            config_id: None,
        };

        let record = response.into_ns_record("books");
        assert_eq!(record.ledger_id, "books");
        assert_eq!(record.name, "books");
    }
}
