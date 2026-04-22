//! Garbage record types
//!
//! Defines the data structures for garbage collection records.

use serde::{Deserialize, Serialize};

/// Garbage record containing obsolete CAS artifacts from an index refresh.
///
/// JSON format: `{ "ledger_id": "...", "t": N, "garbage": [...], "created_at_ms": N }`
///
/// The garbage list is sorted and deduplicated for determinism.
/// Note: `created_at_ms` is safe to include because the garbage record is NOT
/// content-addressed (only its path is deterministic based on t).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GarbageRecord {
    /// Ledger ID (e.g., "mydb:main")
    pub ledger_id: String,
    /// Transaction time this record was created for
    pub t: i64,
    /// Sorted, deduped list of obsolete CID strings (base32-lower multibase).
    ///
    /// Each entry is a `ContentId.to_string()` value identifying an obsolete
    /// CAS artifact (index leaf, branch, dict, etc.) that was replaced during
    /// this index refresh.
    pub garbage: Vec<String>,
    /// Wall-clock timestamp when this record was created (milliseconds since epoch)
    /// Used for time-based GC retention checks
    #[serde(default)]
    pub created_at_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{ContentId, ContentKind};

    /// Helper: create a realistic CID string for test garbage entries.
    fn test_cid_string(kind: ContentKind, label: &[u8]) -> String {
        ContentId::new(kind, label).to_string()
    }

    #[test]
    fn test_garbage_record_serialization() {
        let record = GarbageRecord {
            ledger_id: "test:ledger".to_string(),
            t: 42,
            garbage: vec![
                test_cid_string(ContentKind::IndexLeaf, b"leaf-abc"),
                test_cid_string(ContentKind::IndexLeaf, b"leaf-def"),
            ],
            created_at_ms: 1_700_000_000_000,
        };

        let json = serde_json::to_string(&record).unwrap();
        let parsed: GarbageRecord = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed, record);
    }

    #[test]
    fn test_garbage_record_json_format() {
        let cid1 = test_cid_string(ContentKind::IndexBranch, b"branch-1");
        let cid2 = test_cid_string(ContentKind::IndexBranch, b"branch-2");
        let record = GarbageRecord {
            ledger_id: "test:main".to_string(),
            t: 100,
            garbage: vec![cid1, cid2],
            created_at_ms: 1_700_000_000_000,
        };

        let json = serde_json::to_string(&record).unwrap();

        // Verify expected JSON structure
        assert!(json.contains("\"ledger_id\":\"test:main\""));
        assert!(json.contains("\"t\":100"));
        assert!(json.contains("\"garbage\":["));
        assert!(json.contains("\"created_at_ms\":1700000000000"));
    }

    #[test]
    fn test_garbage_record_empty_garbage() {
        let record = GarbageRecord {
            ledger_id: "test".to_string(),
            t: 1,
            garbage: vec![],
            created_at_ms: 0,
        };

        let json = serde_json::to_string(&record).unwrap();
        let parsed: GarbageRecord = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.garbage.len(), 0);
    }

    #[test]
    fn test_garbage_record_backwards_compatible() {
        // Old format without created_at_ms should deserialize with default 0
        let old_json = r#"{"ledger_id":"test","t":1,"garbage":[]}"#;
        let parsed: GarbageRecord = serde_json::from_str(old_json).unwrap();

        assert_eq!(parsed.ledger_id, "test");
        assert_eq!(parsed.t, 1);
        assert_eq!(parsed.created_at_ms, 0); // Default value
    }
}
