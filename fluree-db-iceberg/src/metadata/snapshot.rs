//! Iceberg snapshot structures and selection.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Iceberg table snapshot.
///
/// A snapshot represents the state of a table at a point in time,
/// including pointers to manifest files that describe the data files.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct Snapshot {
    /// Unique snapshot ID
    pub snapshot_id: i64,
    /// Parent snapshot ID (None for the first snapshot)
    #[serde(default)]
    pub parent_snapshot_id: Option<i64>,
    /// Sequence number (v2 only)
    #[serde(default)]
    pub sequence_number: i64,
    /// Timestamp when snapshot was created (ms since epoch)
    pub timestamp_ms: i64,
    /// Path to manifest list file (v2)
    #[serde(default)]
    pub manifest_list: Option<String>,
    /// Direct manifest paths (v1 format)
    #[serde(default)]
    pub manifests: Option<Vec<String>>,
    /// Summary statistics
    #[serde(default)]
    pub summary: HashMap<String, String>,
    /// Schema ID at snapshot time
    #[serde(default)]
    pub schema_id: Option<i32>,
}

impl Snapshot {
    /// Get the manifest list location.
    pub fn manifest_list_location(&self) -> Option<&str> {
        self.manifest_list.as_deref()
    }

    /// Get manifest paths (for v1 tables without manifest list).
    pub fn manifest_paths(&self) -> Option<&[String]> {
        self.manifests.as_deref()
    }

    /// Get the total records count from summary.
    pub fn total_records(&self) -> Option<i64> {
        self.summary
            .get("total-records")
            .and_then(|s| s.parse().ok())
    }

    /// Get the total data files count from summary.
    pub fn total_data_files(&self) -> Option<i64> {
        self.summary
            .get("total-data-files")
            .and_then(|s| s.parse().ok())
    }

    /// Get the total file size in bytes from summary.
    pub fn total_files_size(&self) -> Option<i64> {
        self.summary
            .get("total-files-size")
            .and_then(|s| s.parse().ok())
    }

    /// Get the operation that created this snapshot.
    pub fn operation(&self) -> Option<&str> {
        self.summary
            .get("operation")
            .map(std::string::String::as_str)
    }

    /// Get added records count from summary.
    pub fn added_records(&self) -> Option<i64> {
        self.summary
            .get("added-records")
            .and_then(|s| s.parse().ok())
    }

    /// Get deleted records count from summary.
    pub fn deleted_records(&self) -> Option<i64> {
        self.summary
            .get("deleted-records")
            .and_then(|s| s.parse().ok())
    }
}

/// Snapshot selection criteria for time travel queries.
#[derive(Debug, Clone, Default)]
pub enum SnapshotSelection {
    /// Use the current snapshot (default)
    #[default]
    Current,
    /// Use a specific snapshot by ID
    SnapshotId(i64),
    /// Use the snapshot valid at a specific timestamp (epoch ms)
    AsOfTime(i64),
}

/// Select a snapshot from table metadata based on selection criteria.
///
/// # Arguments
///
/// * `metadata` - The table metadata containing snapshots
/// * `selection` - The selection criteria
///
/// # Returns
///
/// The selected snapshot, or `None` if no matching snapshot is found.
///
/// # Examples
///
/// ```ignore
/// use fluree_db_iceberg::metadata::{TableMetadata, SnapshotSelection, select_snapshot};
///
/// let metadata: TableMetadata = /* load from file */;
///
/// // Get current snapshot
/// let current = select_snapshot(&metadata, &SnapshotSelection::Current);
///
/// // Get snapshot at specific time
/// let historical = select_snapshot(&metadata, &SnapshotSelection::AsOfTime(1699500000000));
/// ```
pub fn select_snapshot<'a>(
    metadata: &'a super::TableMetadata,
    selection: &SnapshotSelection,
) -> Option<&'a Snapshot> {
    match selection {
        SnapshotSelection::Current => metadata.current_snapshot(),

        SnapshotSelection::SnapshotId(id) => metadata.snapshot(*id),

        SnapshotSelection::AsOfTime(timestamp_ms) => {
            // Find the most recent snapshot with timestamp <= target
            // This implements "as of time" semantics
            metadata
                .snapshots
                .iter()
                .filter(|s| s.timestamp_ms <= *timestamp_ms)
                .max_by_key(|s| s.timestamp_ms)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_metadata() -> super::super::TableMetadata {
        super::super::TableMetadata {
            format_version: 2,
            table_uuid: None,
            location: "s3://bucket/table".to_string(),
            last_sequence_number: 3,
            last_updated_ms: 3000,
            last_column_id: 1,
            current_schema_id: 0,
            schemas: vec![],
            current_snapshot_id: Some(3),
            snapshots: vec![
                Snapshot {
                    snapshot_id: 1,
                    parent_snapshot_id: None,
                    sequence_number: 1,
                    timestamp_ms: 1000,
                    manifest_list: Some("s3://bucket/table/metadata/snap-1.avro".to_string()),
                    manifests: None,
                    summary: {
                        let mut m = HashMap::new();
                        m.insert("total-records".to_string(), "10".to_string());
                        m.insert("operation".to_string(), "append".to_string());
                        m
                    },
                    schema_id: Some(0),
                },
                Snapshot {
                    snapshot_id: 2,
                    parent_snapshot_id: Some(1),
                    sequence_number: 2,
                    timestamp_ms: 2000,
                    manifest_list: Some("s3://bucket/table/metadata/snap-2.avro".to_string()),
                    manifests: None,
                    summary: {
                        let mut m = HashMap::new();
                        m.insert("total-records".to_string(), "50".to_string());
                        m
                    },
                    schema_id: Some(0),
                },
                Snapshot {
                    snapshot_id: 3,
                    parent_snapshot_id: Some(2),
                    sequence_number: 3,
                    timestamp_ms: 3000,
                    manifest_list: Some("s3://bucket/table/metadata/snap-3.avro".to_string()),
                    manifests: None,
                    summary: {
                        let mut m = HashMap::new();
                        m.insert("total-records".to_string(), "100".to_string());
                        m
                    },
                    schema_id: Some(0),
                },
            ],
            snapshot_log: vec![],
            default_spec_id: 0,
            partition_specs: vec![],
            last_partition_id: 0,
            sort_orders: vec![],
            default_sort_order_id: 0,
            properties: HashMap::new(),
        }
    }

    #[test]
    fn test_snapshot_summary_accessors() {
        let snap = Snapshot {
            snapshot_id: 1,
            parent_snapshot_id: None,
            sequence_number: 1,
            timestamp_ms: 1000,
            manifest_list: Some("path".to_string()),
            manifests: None,
            summary: {
                let mut m = HashMap::new();
                m.insert("total-records".to_string(), "100".to_string());
                m.insert("total-data-files".to_string(), "5".to_string());
                m.insert("total-files-size".to_string(), "1048576".to_string());
                m.insert("operation".to_string(), "append".to_string());
                m.insert("added-records".to_string(), "50".to_string());
                m.insert("deleted-records".to_string(), "10".to_string());
                m
            },
            schema_id: None,
        };

        assert_eq!(snap.total_records(), Some(100));
        assert_eq!(snap.total_data_files(), Some(5));
        assert_eq!(snap.total_files_size(), Some(1_048_576));
        assert_eq!(snap.operation(), Some("append"));
        assert_eq!(snap.added_records(), Some(50));
        assert_eq!(snap.deleted_records(), Some(10));
    }

    #[test]
    fn test_select_current_snapshot() {
        let metadata = make_test_metadata();
        let snap = select_snapshot(&metadata, &SnapshotSelection::Current).unwrap();
        assert_eq!(snap.snapshot_id, 3);
    }

    #[test]
    fn test_select_snapshot_by_id() {
        let metadata = make_test_metadata();

        let snap1 = select_snapshot(&metadata, &SnapshotSelection::SnapshotId(1)).unwrap();
        assert_eq!(snap1.snapshot_id, 1);

        let snap2 = select_snapshot(&metadata, &SnapshotSelection::SnapshotId(2)).unwrap();
        assert_eq!(snap2.snapshot_id, 2);

        // Non-existent snapshot
        let none = select_snapshot(&metadata, &SnapshotSelection::SnapshotId(999));
        assert!(none.is_none());
    }

    #[test]
    fn test_select_snapshot_as_of_time() {
        let metadata = make_test_metadata();

        // Exact match on timestamp
        let snap = select_snapshot(&metadata, &SnapshotSelection::AsOfTime(2000)).unwrap();
        assert_eq!(snap.snapshot_id, 2);

        // Between timestamps - should get most recent before target
        let snap = select_snapshot(&metadata, &SnapshotSelection::AsOfTime(2500)).unwrap();
        assert_eq!(snap.snapshot_id, 2);

        // After all snapshots
        let snap = select_snapshot(&metadata, &SnapshotSelection::AsOfTime(5000)).unwrap();
        assert_eq!(snap.snapshot_id, 3);

        // Before all snapshots - no match
        let none = select_snapshot(&metadata, &SnapshotSelection::AsOfTime(500));
        assert!(none.is_none());
    }

    #[test]
    fn test_select_default_is_current() {
        let metadata = make_test_metadata();
        let snap = select_snapshot(&metadata, &SnapshotSelection::default()).unwrap();
        assert_eq!(snap.snapshot_id, 3);
    }
}
