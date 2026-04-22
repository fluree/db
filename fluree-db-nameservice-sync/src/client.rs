//! Remote nameservice client
//!
//! Provides an abstraction for communicating with a remote Fluree nameservice
//! via HTTP REST endpoints.

use crate::error::{Result, SyncError};
use async_trait::async_trait;
use fluree_db_nameservice::{CasResult, GraphSourceRecord, NsRecord, RefKind, RefValue};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Snapshot of all remote records
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteSnapshot {
    pub ledgers: Vec<NsRecord>,
    #[serde(default)]
    pub graph_sources: Vec<GraphSourceRecord>,
}

/// Request body for CAS push
#[derive(Debug, Serialize)]
struct PushRefRequest<'a> {
    expected: Option<&'a RefValue>,
    new: &'a RefValue,
}

/// Response from a push operation
#[derive(Debug, Deserialize)]
#[allow(dead_code)] // Fields read during deserialization
struct PushRefResponse {
    status: String,
    #[serde(rename = "ref")]
    ref_value: Option<RefValue>,
    actual: Option<RefValue>,
}

/// Response from init operation
#[derive(Debug, Deserialize)]
struct InitResponse {
    created: bool,
}

/// Client for communicating with a remote nameservice
#[async_trait]
pub trait RemoteNameserviceClient: Debug + Send + Sync {
    /// Look up a single ledger record on the remote
    async fn lookup(&self, ledger_id: &str) -> Result<Option<NsRecord>>;

    /// Get a full snapshot of all remote records (ledgers + graph sources)
    async fn snapshot(&self) -> Result<RemoteSnapshot>;

    /// CAS push for a ref on the remote
    async fn push_ref(
        &self,
        ledger_id: &str,
        kind: RefKind,
        expected: Option<&RefValue>,
        new: &RefValue,
    ) -> Result<CasResult>;

    /// Initialize a ledger on the remote (create-if-absent)
    ///
    /// Returns `true` if created, `false` if already existed.
    async fn init_ledger(&self, ledger_id: &str) -> Result<bool>;
}

/// HTTP-based remote client
#[derive(Debug)]
pub struct HttpRemoteClient {
    base_url: String,
    http: reqwest::Client,
    auth_token: Option<String>,
}

impl HttpRemoteClient {
    pub fn new(base_url: impl Into<String>, auth_token: Option<String>) -> Self {
        let raw = base_url.into();
        let trimmed = raw.trim_end_matches('/').to_string();
        let normalized = if trimmed.ends_with("/fluree") {
            trimmed
        } else {
            format!("{trimmed}/fluree")
        };
        Self {
            base_url: normalized,
            http: reqwest::Client::new(),
            auth_token,
        }
    }

    fn add_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(ref token) = self.auth_token {
            req.bearer_auth(token)
        } else {
            req
        }
    }

    async fn remote_error_with_body(
        context: &str,
        url: &str,
        resp: reqwest::Response,
    ) -> SyncError {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if body.trim().is_empty() {
            SyncError::Remote(format!("{context} failed with status {status} for {url}"))
        } else {
            SyncError::Remote(format!(
                "{context} failed with status {status} for {url}: {body}"
            ))
        }
    }

    fn kind_path(kind: RefKind) -> &'static str {
        match kind {
            RefKind::CommitHead => "commit",
            RefKind::IndexHead => "index",
        }
    }
}

#[async_trait]
impl RemoteNameserviceClient for HttpRemoteClient {
    async fn lookup(&self, ledger_id: &str) -> Result<Option<NsRecord>> {
        // base_url is the Fluree API base (ends with `/fluree`), so route paths
        // here should be relative to that prefix.
        let url = format!("{}/storage/ns/{}", self.base_url, ledger_id);
        let resp = self.add_auth(self.http.get(&url)).send().await?;

        match resp.status().as_u16() {
            200 => {
                let record: NsRecord = resp.json().await?;
                Ok(Some(record))
            }
            404 => Ok(None),
            _ => Err(Self::remote_error_with_body("Lookup", &url, resp).await),
        }
    }

    async fn snapshot(&self) -> Result<RemoteSnapshot> {
        let url = format!("{}/nameservice/snapshot", self.base_url);
        let resp = self.add_auth(self.http.get(&url)).send().await?;

        if !resp.status().is_success() {
            return Err(Self::remote_error_with_body("Snapshot", &url, resp).await);
        }

        let snapshot: RemoteSnapshot = resp.json().await?;
        Ok(snapshot)
    }

    async fn push_ref(
        &self,
        ledger_id: &str,
        kind: RefKind,
        expected: Option<&RefValue>,
        new: &RefValue,
    ) -> Result<CasResult> {
        let kind_path = Self::kind_path(kind);
        let url = format!(
            "{}/nameservice/refs/{}/{}",
            self.base_url, ledger_id, kind_path
        );

        let body = PushRefRequest { expected, new };
        let resp = self
            .add_auth(self.http.post(&url))
            .json(&body)
            .send()
            .await?;

        match resp.status().as_u16() {
            200 => Ok(CasResult::Updated),
            409 => {
                let push_resp: PushRefResponse = resp.json().await?;
                Ok(CasResult::Conflict {
                    actual: push_resp.actual,
                })
            }
            _ => Err(Self::remote_error_with_body("Push", &url, resp).await),
        }
    }

    async fn init_ledger(&self, ledger_id: &str) -> Result<bool> {
        let url = format!("{}/nameservice/refs/{}/init", self.base_url, ledger_id);

        let resp = self.add_auth(self.http.post(&url)).send().await?;

        if !resp.status().is_success() {
            return Err(Self::remote_error_with_body("Init", &url, resp).await);
        }

        let init_resp: InitResponse = resp.json().await?;
        Ok(init_resp.created)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_serde() {
        let json = r#"{
            "ledgers": [
                {
                    "ledger_id": "mydb:main",
                    "name": "mydb",
                    "branch": "main",
                    "commit_head_id": null,
                    "commit_t": 5,
                    "index_head_id": null,
                    "index_t": 3,
                    "retracted": false
                }
            ],
            "graph_sources": []
        }"#;

        let snapshot: RemoteSnapshot = serde_json::from_str(json).unwrap();
        assert_eq!(snapshot.ledgers.len(), 1);
        assert_eq!(snapshot.ledgers[0].commit_t, 5);
        assert!(snapshot.graph_sources.is_empty());
    }

    #[test]
    fn test_snapshot_missing_graph_sources_field() {
        // graph_sources field is optional (default empty)
        let json = r#"{
            "ledgers": []
        }"#;

        let snapshot: RemoteSnapshot = serde_json::from_str(json).unwrap();
        assert!(snapshot.ledgers.is_empty());
        assert!(snapshot.graph_sources.is_empty());
    }
}
