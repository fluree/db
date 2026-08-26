//! Proxy nameservice implementation for peer mode
//!
//! Fetches nameservice records via the transaction server's `/v1/fluree/storage/ns/{alias}`
//! endpoint instead of direct file access. This allows peers to operate without storage
//! credentials.

use crate::transport::{HttpTransport, TransportRequest};
use async_trait::async_trait;
use fluree_db_nameservice::{NameServiceError, NsRecord, Result};
use http::StatusCode;
use serde::Deserialize;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

/// NameService implementation that proxies lookups through the transaction server
///
/// All network I/O goes through the [`HttpTransport`] seam; see
/// [`crate::transport`] for the wasm-implementability contract.
#[derive(Clone)]
pub struct ProxyNameService {
    transport: Arc<dyn HttpTransport>,
    api_base: String,
    token: String,
}

impl Debug for ProxyNameService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyNameService")
            .field("api_base", &self.api_base)
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
    /// Parent branch this branch was forked from. Required for
    /// peers to build the `BranchedContentStore` that resolves
    /// commits inherited from the source branch — without it, all
    /// reads of inherited commits 404 even when the ledger is
    /// reachable.
    #[serde(default)]
    source_branch: Option<String>,
    /// Number of child branches forked from this one. Defaults to
    /// 0 when an older server omits the field.
    #[serde(default)]
    branches: u32,
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
            source_branch: self.source_branch,
            branches: self.branches,
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
        // Server root → default versioned API base.
        let api_base = format!("{}/v1/fluree", base_url.trim_end_matches('/'));
        Self::from_api_base(api_base, token)
    }

    /// Create a proxy nameservice client from a full API base URL (e.g.
    /// `https://data.example.com/v1/fluree`), as stored by `fluree remote
    /// add` or advertised via discovery's `api_base_url`. Use this instead
    /// of [`new`](Self::new) when the API may be mounted under a
    /// non-default prefix.
    pub fn from_api_base(api_base: String, token: String) -> Self {
        let transport = Arc::new(crate::transport::ReqwestTransport::with_timeout(
            Duration::from_secs(30), // 30 seconds for NS lookups
        ));
        Self::from_api_base_with_transport(api_base, token, transport)
    }

    /// Create a proxy nameservice client over a caller-supplied
    /// [`HttpTransport`] (e.g. a browser fetch transport);
    /// [`new`](Self::new) and [`from_api_base`](Self::from_api_base) are
    /// conveniences that plug in the default reqwest transport.
    pub fn from_api_base_with_transport(
        api_base: String,
        token: String,
        transport: Arc<dyn HttpTransport>,
    ) -> Self {
        Self {
            transport,
            api_base: api_base.trim_end_matches('/').to_string(),
            token,
        }
    }

    /// Build the nameservice lookup endpoint URL
    fn ns_url(&self, alias: &str) -> String {
        format!(
            "{}/storage/ns/{}",
            self.api_base,
            urlencoding::encode(alias)
        )
    }
}

/// Project a fetched record onto one head ref.
///
/// A retracted branch yields `None`, matching the raft nameservice: `lookup`
/// keeps serving the record so admin tooling can see the `retracted` flag, but
/// this is the active-read surface and must not resolve a head for a branch the
/// operator soft-deleted. The remote endpoint projects `lookup`, so without
/// this the proxy would resurrect branches its own raft-backed origin reports
/// as gone.
fn project_ref(
    record: Option<NsRecord>,
    kind: fluree_db_nameservice::RefKind,
) -> Option<fluree_db_nameservice::RefValue> {
    use fluree_db_nameservice::{RefKind, RefValue};
    let r = record.filter(|r| !r.retracted)?;
    Some(match kind {
        RefKind::CommitHead => RefValue {
            id: r.commit_head_id,
            t: r.commit_t,
        },
        RefKind::IndexHead => RefValue {
            id: r.index_head_id,
            t: r.index_t,
        },
    })
}

/// Project a fetched record onto both head refs. Tombstones retracted
/// branches for the same reason as [`project_ref`].
fn project_heads(record: Option<NsRecord>) -> Option<fluree_db_nameservice::LedgerHeads> {
    record
        .filter(|r| !r.retracted)
        .map(|r| fluree_db_nameservice::LedgerHeads::from_record(&r))
}

#[async_trait]
impl fluree_db_nameservice::RefLookup for ProxyNameService {
    /// The proxy endpoint serves whole records, so a single-ref read is a
    /// projection of `lookup` — still one HTTP round trip. Retracted branches
    /// report `None`; see [`project_ref`].
    async fn get_ref(
        &self,
        ledger_id: &str,
        kind: fluree_db_nameservice::RefKind,
    ) -> Result<Option<fluree_db_nameservice::RefValue>> {
        use fluree_db_nameservice::NameServiceLookup;
        Ok(project_ref(self.lookup(ledger_id).await?, kind))
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
impl fluree_db_nameservice::NameServiceLookup for ProxyNameService {
    async fn lookup(&self, ledger_id: &str) -> Result<Option<NsRecord>> {
        let url = self.ns_url(ledger_id);

        let request =
            TransportRequest::get(url).header("authorization", format!("Bearer {}", self.token));
        let response = self.transport.execute(request).await.map_err(|e| match e {
            // Body-read failures historically surfaced through response
            // decoding, so they keep the parse-error message.
            crate::transport::TransportError::Body(e) => {
                NameServiceError::storage(format!("Failed to parse NS response: {e}"))
            }
            other => {
                NameServiceError::storage(format!("Nameservice proxy request failed: {other}"))
            }
        })?;

        let status = response.status;

        match status {
            StatusCode::OK => {
                let ns_response: NsRecordResponse = serde_json::from_slice(&response.body)
                    .map_err(|e| {
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

    /// Retracted branches report `None` — see [`project_heads`].
    async fn heads(&self, ledger_id: &str) -> Result<Option<fluree_db_nameservice::LedgerHeads>> {
        Ok(project_heads(self.lookup(ledger_id).await?))
    }

    async fn all_records(&self) -> Result<Vec<NsRecord>> {
        // Peers use SSE for discovery, not all_records()
        // Return empty - this is intentional for proxy mode
        // The peer maintains its own view of known ledgers via SSE events
        Ok(Vec::new())
    }
}

// No `BranchLifecycle` impl for `ProxyNameService`: peer mode has no
// authority to mutate the nameservice. Branch lifecycle requests are
// served by the upstream transaction server (via its own HTTP routes,
// not through this trait).

#[async_trait]
impl fluree_db_nameservice::GraphSourceLookup for ProxyNameService {
    async fn lookup_graph_source(
        &self,
        _graph_source_id: &str,
    ) -> Result<Option<fluree_db_nameservice::GraphSourceRecord>> {
        Ok(None) // Proxy doesn't have local graph source records
    }

    async fn lookup_any(&self, resource_id: &str) -> Result<fluree_db_nameservice::NsLookupResult> {
        // Delegate to the ledger lookup endpoint. Graph-source
        // discovery isn't exposed through the storage proxy today,
        // so a non-ledger resource still reports NotFound — but a
        // real ledger record now resolves correctly instead of
        // always returning NotFound and breaking every caller that
        // routes through `GraphSourceLookup::lookup_any`.
        use fluree_db_nameservice::{NameServiceLookup, NsLookupResult};
        match self.lookup(resource_id).await? {
            Some(record) => Ok(NsLookupResult::Ledger(record)),
            None => Ok(NsLookupResult::NotFound),
        }
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
    use fluree_db_core::{ContentId, ContentKind};
    use fluree_db_nameservice::{RefKind, RefValue};

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
            source_branch: None,
            branches: 0,
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

    fn record(retracted: bool) -> NsRecord {
        NsRecordResponse {
            name: Some("books".to_string()),
            branch: "main".to_string(),
            commit_head_id: Some(ContentId::new(ContentKind::Commit, b"c").to_string()),
            commit_t: 42,
            index_head_id: Some(ContentId::new(ContentKind::IndexRoot, b"i").to_string()),
            index_t: 40,
            default_context: None,
            retracted,
            config_id: None,
            source_branch: None,
            branches: 0,
        }
        .into_ns_record("books:main")
    }

    #[test]
    fn projections_carry_both_heads() {
        let r = record(false);
        let commit = project_ref(Some(r.clone()), RefKind::CommitHead).expect("commit ref");
        assert_eq!(commit.id, Some(ContentId::new(ContentKind::Commit, b"c")));
        assert_eq!(commit.t, 42);

        let index = project_ref(Some(r.clone()), RefKind::IndexHead).expect("index ref");
        assert_eq!(index.id, Some(ContentId::new(ContentKind::IndexRoot, b"i")));
        assert_eq!(index.t, 40);

        // `heads` must agree with the two single-ref reads, not drift from them.
        let heads = project_heads(Some(r)).expect("heads");
        assert_eq!(heads.commit, commit);
        assert_eq!(heads.index, index);
    }

    /// A retracted branch is tombstoned on the active-read surface even though
    /// the remote endpoint keeps serving its record (with the flag set) through
    /// `lookup`. Without this the proxy resurrects branches a raft-backed
    /// origin reports as gone — see `project_ref`.
    #[test]
    fn projections_tombstone_a_retracted_branch() {
        let r = record(true);
        assert!(r.retracted, "the record itself still carries the flag");
        assert_eq!(project_ref(Some(r.clone()), RefKind::CommitHead), None);
        assert_eq!(project_ref(Some(r.clone()), RefKind::IndexHead), None);
        assert_eq!(project_heads(Some(r)), None);
    }

    /// An unknown ledger (the endpoint's 404) is `None`, distinct from a
    /// known-but-unborn branch, which projects zeroed refs.
    #[test]
    fn projections_distinguish_unknown_from_unborn() {
        assert_eq!(project_ref(None, RefKind::CommitHead), None);
        assert_eq!(project_heads(None), None);

        let unborn = NsRecordResponse {
            name: Some("books".to_string()),
            branch: "main".to_string(),
            commit_head_id: None,
            commit_t: 0,
            index_head_id: None,
            index_t: 0,
            default_context: None,
            retracted: false,
            config_id: None,
            source_branch: None,
            branches: 0,
        }
        .into_ns_record("books:main");
        let heads = project_heads(Some(unborn)).expect("unborn branch is known");
        assert_eq!(heads.commit, RefValue { id: None, t: 0 });
        assert_eq!(heads.index, RefValue { id: None, t: 0 });
    }

    /// Regression for finding #11: `source_branch` and `branches`
    /// must round-trip through the proxy so peers can build the
    /// `BranchedContentStore` for forked branches. Earlier code
    /// hardcoded `source_branch: None` regardless of what the
    /// server sent, breaking every read of an inherited commit.
    #[test]
    fn ns_record_conversion_preserves_branch_lineage() {
        let response = NsRecordResponse {
            name: Some("books".to_string()),
            branch: "feature".to_string(),
            commit_head_id: None,
            commit_t: 7,
            index_head_id: None,
            index_t: 5,
            default_context: None,
            retracted: false,
            config_id: None,
            source_branch: Some("main".to_string()),
            branches: 3,
        };

        let record = response.into_ns_record("books:feature");
        assert_eq!(record.source_branch.as_deref(), Some("main"));
        assert_eq!(record.branches, 3);
    }

    /// Old-server compatibility: when the wire response omits
    /// `source_branch`/`branches`, deserialization defaults them
    /// to `None`/`0` rather than failing.
    #[test]
    fn ns_record_response_deserializes_without_lineage_fields() {
        let json = r#"{
            "name": "books",
            "branch": "main",
            "commit_head_id": null,
            "commit_t": 0,
            "index_head_id": null,
            "index_t": 0,
            "retracted": false
        }"#;
        let response: NsRecordResponse =
            serde_json::from_str(json).expect("response without lineage fields parses");
        assert!(response.source_branch.is_none());
        assert_eq!(response.branches, 0);
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
            source_branch: None,
            branches: 0,
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
            source_branch: None,
            branches: 0,
        };

        let record = response.into_ns_record("books");
        assert_eq!(record.ledger_id, "books");
        assert_eq!(record.name, "books");
    }
}
