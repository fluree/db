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

    /// Get the total number of merge-on-read **delete files** from the snapshot
    /// summary (`total-delete-files`), if present. A value `> 0` means the
    /// snapshot carries position/equality delete files that Fluree does not yet
    /// apply — see [`crate::mor_guard`].
    pub fn total_delete_files(&self) -> Option<i64> {
        self.summary
            .get("total-delete-files")
            .and_then(|s| s.parse().ok())
    }

    /// Get the total number of merge-on-read **position deletes** from the
    /// snapshot summary (`total-position-deletes`), if present.
    pub fn total_position_deletes(&self) -> Option<i64> {
        self.summary
            .get("total-position-deletes")
            .and_then(|s| s.parse().ok())
    }

    /// Get the total number of merge-on-read **equality deletes** from the
    /// snapshot summary (`total-equality-deletes`), if present.
    pub fn total_equality_deletes(&self) -> Option<i64> {
        self.summary
            .get("total-equality-deletes")
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
            // The latest state the table PRESENTED at or before the instant, not
            // the latest-committed retained snapshot: a rolled-back or branch
            // snapshot is retained but was never (or is no longer) the table's
            // state, and `snapshots` alone cannot tell the two apart.
            main_lineage(metadata)
                .filter(|(became_current_ms, _)| *became_current_ms <= *timestamp_ms)
                .max_by_key(|(became_current_ms, _)| *became_current_ms)
                .and_then(|(_, id)| metadata.snapshot(id))
        }
    }
}

/// The instants at which the table's current snapshot changed, as
/// `(became_current_ms, snapshot_id)`, oldest first — the timeline an "as of"
/// instant is resolved against.
///
/// The snapshot log is that timeline by definition (a rollback appends a new
/// entry for the old snapshot at the rollback time). A table without one falls
/// back to the current snapshot's ancestor chain, using each snapshot's commit
/// time; a table with no current snapshot has no timeline.
pub fn main_lineage(metadata: &super::TableMetadata) -> impl Iterator<Item = (i64, i64)> + '_ {
    let from_log = metadata
        .snapshot_log
        .iter()
        .map(|e| (e.timestamp_ms, e.snapshot_id));
    let mut next = if metadata.snapshot_log.is_empty() {
        metadata.current_snapshot()
    } else {
        None
    };
    let from_ancestry = std::iter::from_fn(move || {
        let s = next?;
        next = s.parent_snapshot_id.and_then(|p| metadata.snapshot(p));
        Some((s.timestamp_ms, s.snapshot_id))
    })
    .collect::<Vec<_>>()
    .into_iter()
    .rev();
    from_log.chain(from_ancestry)
}

/// The earliest instant an "as of" selection can resolve, if the table has ever
/// had a current snapshot.
pub fn earliest_as_of_time_ms(metadata: &super::TableMetadata) -> Option<i64> {
    main_lineage(metadata).map(|(ms, _)| ms).min()
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
        // This append snapshot carries no delete-file counters.
        assert_eq!(snap.total_delete_files(), None);
        assert_eq!(snap.total_position_deletes(), None);
        assert_eq!(snap.total_equality_deletes(), None);
    }

    #[test]
    fn test_mor_delete_summary_accessors() {
        let mut summary = HashMap::new();
        summary.insert("total-delete-files".to_string(), "2".to_string());
        summary.insert("total-position-deletes".to_string(), "17".to_string());
        summary.insert("total-equality-deletes".to_string(), "0".to_string());
        let snap = Snapshot {
            snapshot_id: 1,
            parent_snapshot_id: None,
            sequence_number: 1,
            timestamp_ms: 1000,
            manifest_list: Some("path".to_string()),
            manifests: None,
            summary,
            schema_id: None,
        };
        assert_eq!(snap.total_delete_files(), Some(2));
        assert_eq!(snap.total_position_deletes(), Some(17));
        assert_eq!(snap.total_equality_deletes(), Some(0));
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

    /// After a rollback, the retained newer snapshot is still selectable by id
    /// but is no longer the state "as of" any instant after the rollback; and
    /// an instant while it WAS current still resolves to it, as the snapshot
    /// log records.
    #[test]
    fn as_of_time_follows_the_snapshot_log_not_all_retained_snapshots() {
        let mut metadata = make_test_metadata();
        // Rolled back to snapshot 1 at t=4000: 2 and 3 stay retained.
        metadata.current_snapshot_id = Some(1);
        metadata.snapshot_log = [(1000, 1), (2000, 2), (3000, 3), (4000, 1)]
            .into_iter()
            .map(
                |(timestamp_ms, snapshot_id)| super::super::SnapshotLogEntry {
                    snapshot_id,
                    timestamp_ms,
                },
            )
            .collect();

        let at = |ms| select_snapshot(&metadata, &SnapshotSelection::AsOfTime(ms));
        assert_eq!(at(2500).unwrap().snapshot_id, 2, "2 was current at 2500");
        assert_eq!(at(3500).unwrap().snapshot_id, 3, "3 was current at 3500");
        assert_eq!(at(4000).unwrap().snapshot_id, 1, "rolled back at 4000");
        assert_eq!(
            at(9000).unwrap().snapshot_id,
            1,
            "now = current, not max-ts"
        );
        assert!(at(500).is_none());
        assert_eq!(earliest_as_of_time_ms(&metadata), Some(1000));
        // By id, a retained rolled-back snapshot is still addressable.
        assert_eq!(
            select_snapshot(&metadata, &SnapshotSelection::SnapshotId(3))
                .unwrap()
                .snapshot_id,
            3
        );
    }

    /// No snapshot log (older writers): the current snapshot's ancestry is the
    /// timeline, so an orphaned branch snapshot is never selected by time.
    #[test]
    fn as_of_time_without_a_log_walks_current_ancestry() {
        let mut metadata = make_test_metadata();
        assert!(metadata.snapshot_log.is_empty());
        // Snapshot 3 is a branch off 1 that never became current.
        metadata.current_snapshot_id = Some(2);
        metadata.snapshots[2].parent_snapshot_id = Some(1);
        let at = |m: &super::super::TableMetadata, ms| {
            select_snapshot(m, &SnapshotSelection::AsOfTime(ms)).map(|s| s.snapshot_id)
        };
        assert_eq!(at(&metadata, 9000), Some(2));
        assert_eq!(at(&metadata, 1500), Some(1));
        assert_eq!(at(&metadata, 500), None);
        assert_eq!(earliest_as_of_time_ms(&metadata), Some(1000));
        metadata.current_snapshot_id = None;
        assert_eq!(at(&metadata, 9000), None);
        assert_eq!(earliest_as_of_time_ms(&metadata), None);
    }

    #[test]
    fn test_select_default_is_current() {
        let metadata = make_test_metadata();
        let snap = select_snapshot(&metadata, &SnapshotSelection::default()).unwrap();
        assert_eq!(snap.snapshot_id, 3);
    }
}
