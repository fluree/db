//! SSE event types and parsing
//!
//! Types representing the events received from the `/fluree/events` SSE endpoint.

use serde::Deserialize;

/// Ledger record from SSE ns-record event
#[derive(Debug, Clone, Deserialize)]
pub struct LedgerRecord {
    pub ledger_id: String,
    #[serde(default)]
    pub branch: Option<String>,
    /// Storage-agnostic identity of the head commit (CID string).
    #[serde(default)]
    pub commit_head_id: Option<String>,
    pub commit_t: i64,
    /// Storage-agnostic identity of the head index root (CID string).
    #[serde(default)]
    pub index_head_id: Option<String>,
    pub index_t: i64,
    #[serde(default)]
    pub retracted: bool,
}

/// Graph source record from SSE ns-record event
#[derive(Debug, Clone, Deserialize)]
pub struct GraphSourceRecord {
    pub graph_source_id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub source_type: Option<String>,
    #[serde(default)]
    pub config: Option<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    pub index_id: Option<String>,
    pub index_t: i64,
    #[serde(default)]
    pub retracted: bool,
}

/// Wrapper for the SSE event payload
#[derive(Debug, Clone, Deserialize)]
pub struct NsRecordEvent {
    pub action: String,
    pub kind: String,
    pub resource_id: String,
    pub record: serde_json::Value,
    pub emitted_at: String,
}

/// Wrapper for the retraction event payload
#[derive(Debug, Clone, Deserialize)]
pub struct NsRetractedEvent {
    pub action: String,
    pub kind: String,
    pub resource_id: String,
    pub emitted_at: String,
}

/// Snapshot complete event payload
///
/// NOTE: The server does not currently emit `snapshot-complete` events.
/// This type exists for future compatibility when the server adds this feature.
/// Until then, `SseClientEvent::SnapshotComplete` will never fire.
#[derive(Debug, Clone, Deserialize)]
pub struct SnapshotCompleteEvent {
    pub hash: Option<String>,
}

/// Events emitted by the SSE client to the runtime
#[derive(Debug, Clone)]
pub enum SseClientEvent {
    /// Connected and receiving snapshot
    Connected,
    /// Snapshot complete marker received
    ///
    /// NOTE: The server does not currently emit this event.
    /// This variant exists for future compatibility.
    SnapshotComplete { hash: String },
    /// Ledger record received
    LedgerRecord(LedgerRecord),
    /// Graph source record received
    GraphSourceRecord(GraphSourceRecord),
    /// Resource retracted
    Retracted { kind: String, resource_id: String },
    /// Connection lost (will reconnect)
    Disconnected { reason: String },
    /// Fatal error (will not reconnect)
    Fatal { error: String },
}

impl LedgerRecord {
    /// Compute a config hash for change detection
    /// Since ledgers don't have config, use commit_t + index_t
    pub fn state_hash(&self) -> String {
        format!("{}:{}", self.commit_t, self.index_t)
    }
}

impl GraphSourceRecord {
    /// Compute a config hash for change detection.
    ///
    /// Uses SHA-256 truncated to 8 hex chars (4 bytes) to match
    /// the server's graph source SSE event ID format.
    pub fn config_hash(&self) -> String {
        use sha2::{Digest, Sha256};

        let config_str = self.config.as_deref().unwrap_or("");
        let hash = Sha256::digest(config_str.as_bytes());
        // Take first 4 bytes = 8 hex chars (matches server's sha256_short)
        hex::encode(&hash[..4])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ledger_record() {
        let json = r#"{
            "ledger_id": "books:main",
            "branch": "main",
            "commit_head_id": "fluree:commit:abc123",
            "commit_t": 5,
            "index_head_id": "fluree:index:def456",
            "index_t": 3,
            "retracted": false
        }"#;

        let record: LedgerRecord = serde_json::from_str(json).unwrap();
        assert_eq!(record.ledger_id, "books:main");
        assert_eq!(record.commit_t, 5);
        assert_eq!(record.index_t, 3);
    }

    #[test]
    fn test_parse_graph_source_record() {
        let json = r#"{
            "graph_source_id": "search:main",
            "name": "search",
            "branch": "main",
            "source_type": "fulltext",
            "config": "{\"analyzer\": \"standard\"}",
            "dependencies": ["books:main"],
            "index_id": "fluree:index:gs/search",
            "index_t": 2,
            "retracted": false
        }"#;

        let record: GraphSourceRecord = serde_json::from_str(json).unwrap();
        assert_eq!(record.graph_source_id, "search:main");
        assert_eq!(record.index_t, 2);
        assert_eq!(record.dependencies, vec!["books:main"]);
    }

    #[test]
    fn test_parse_ns_record_event() {
        let json = r#"{
            "action": "ns-record",
            "kind": "ledger",
            "resource_id": "books:main",
            "record": {"ledger_id": "books:main", "commit_t": 1, "index_t": 1},
            "emitted_at": "2024-01-01T00:00:00Z"
        }"#;

        let event: NsRecordEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.action, "ns-record");
        assert_eq!(event.kind, "ledger");
    }

    #[test]
    fn test_parse_ns_retracted_event() {
        let json = r#"{
            "action": "ns-retracted",
            "kind": "ledger",
            "resource_id": "books:main",
            "emitted_at": "2024-01-01T00:00:00Z"
        }"#;

        let event: NsRetractedEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.action, "ns-retracted");
        assert_eq!(event.resource_id, "books:main");
    }

    #[test]
    fn test_graph_source_config_hash_sha256() {
        // Test that config_hash produces SHA-256 truncated to 8 hex chars
        // This should match the server's sha256_short function
        let record = GraphSourceRecord {
            graph_source_id: "test:main".to_string(),
            name: None,
            branch: None,
            source_type: None,
            config: Some("test config".to_string()),
            dependencies: vec![],
            index_id: None,
            index_t: 1,
            retracted: false,
        };

        let hash = record.config_hash();
        // SHA-256 of "test config" truncated to first 4 bytes (8 hex chars)
        // Verify it's 8 hex chars and consistent
        assert_eq!(hash.len(), 8);
        assert_eq!(hash, "4369f6f9");
    }

    #[test]
    fn test_graph_source_config_hash_empty() {
        // Empty config should still produce valid hash
        let record = GraphSourceRecord {
            graph_source_id: "test:main".to_string(),
            name: None,
            branch: None,
            source_type: None,
            config: None, // None config
            dependencies: vec![],
            index_id: None,
            index_t: 1,
            retracted: false,
        };

        let hash = record.config_hash();
        assert_eq!(hash.len(), 8);
        // SHA-256 of empty string starts with e3b0c442...
        assert_eq!(hash, "e3b0c442");
    }
}
