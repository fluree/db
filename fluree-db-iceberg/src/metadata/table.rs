//! Iceberg table metadata structures.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Iceberg table metadata (v1/v2 format).
///
/// This structure represents the JSON metadata file for an Iceberg table,
/// containing schemas, snapshots, partition specs, and other table properties.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableMetadata {
    /// Format version (1 or 2)
    pub format_version: i32,
    /// Table UUID
    #[serde(default)]
    pub table_uuid: Option<String>,
    /// Location of the table (base path for data files)
    pub location: String,
    /// Last sequence number (v2)
    #[serde(default)]
    pub last_sequence_number: i64,
    /// Last updated timestamp (ms since epoch)
    pub last_updated_ms: i64,
    /// Last assigned column ID
    pub last_column_id: i32,
    /// Current schema ID
    #[serde(default)]
    pub current_schema_id: i32,
    /// All schemas
    #[serde(default)]
    pub schemas: Vec<Schema>,
    /// Current snapshot ID
    #[serde(default)]
    pub current_snapshot_id: Option<i64>,
    /// All snapshots
    #[serde(default)]
    pub snapshots: Vec<super::Snapshot>,
    /// Snapshot log (ordered history)
    #[serde(default)]
    pub snapshot_log: Vec<SnapshotLogEntry>,
    /// Default partition spec ID
    #[serde(default)]
    pub default_spec_id: i32,
    /// Partition specs
    #[serde(default)]
    pub partition_specs: Vec<PartitionSpec>,
    /// Last assigned partition ID
    #[serde(default)]
    pub last_partition_id: i32,
    /// Sort orders
    #[serde(default)]
    pub sort_orders: Vec<SortOrder>,
    /// Default sort order ID
    #[serde(default)]
    pub default_sort_order_id: i32,
    /// Table properties
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

impl TableMetadata {
    /// Parse metadata from JSON bytes.
    pub fn from_json(json: &[u8]) -> crate::error::Result<Self> {
        serde_json::from_slice(json).map_err(|e| {
            crate::error::IcebergError::Metadata(format!("Failed to parse metadata: {e}"))
        })
    }

    /// Parse metadata from JSON string.
    pub fn from_json_str(json: &str) -> crate::error::Result<Self> {
        serde_json::from_str(json).map_err(|e| {
            crate::error::IcebergError::Metadata(format!("Failed to parse metadata: {e}"))
        })
    }

    /// Get the current schema.
    pub fn current_schema(&self) -> Option<&Schema> {
        self.schemas
            .iter()
            .find(|s| s.schema_id == self.current_schema_id)
            .or_else(|| self.schemas.first())
    }

    /// Get the current snapshot.
    pub fn current_snapshot(&self) -> Option<&super::Snapshot> {
        self.current_snapshot_id
            .and_then(|id| self.snapshots.iter().find(|s| s.snapshot_id == id))
    }

    /// Get a snapshot by ID.
    pub fn snapshot(&self, id: i64) -> Option<&super::Snapshot> {
        self.snapshots.iter().find(|s| s.snapshot_id == id)
    }

    /// Get a schema by ID.
    pub fn schema(&self, id: i32) -> Option<&Schema> {
        self.schemas.iter().find(|s| s.schema_id == id)
    }

    /// Get the partition spec by ID.
    pub fn partition_spec(&self, id: i32) -> Option<&PartitionSpec> {
        self.partition_specs.iter().find(|s| s.spec_id == id)
    }

    /// Get the default partition spec.
    pub fn default_partition_spec(&self) -> Option<&PartitionSpec> {
        self.partition_spec(self.default_spec_id)
    }

    /// The snapshots in the window `(from_id, to_id]`, newest first, walking the
    /// `parent_snapshot_id` chain from `to_id` back toward `from_id`.
    ///
    /// `from_id = None` walks to the root (the full history up to `to_id`).
    /// Returns an error if `to_id` is unknown, an ancestor is missing (e.g. an
    /// expired snapshot), or `from_id` is not an ancestor of `to_id` (a branch or
    /// rollback) — in all of which the caller should fall back to a full re-read.
    pub fn snapshot_window(
        &self,
        from_id: Option<i64>,
        to_id: i64,
    ) -> crate::error::Result<Vec<&super::Snapshot>> {
        if from_id == Some(to_id) {
            return Ok(Vec::new());
        }
        let mut window = Vec::new();
        let mut cur = self.snapshot(to_id).ok_or_else(|| {
            crate::error::IcebergError::SnapshotNotFound(format!("snapshot {to_id} not found"))
        })?;
        loop {
            window.push(cur);
            match cur.parent_snapshot_id {
                Some(pid) if Some(pid) == from_id => return Ok(window),
                Some(pid) => {
                    cur = self.snapshot(pid).ok_or_else(|| {
                        crate::error::IcebergError::SnapshotNotFound(format!(
                            "ancestor snapshot {pid} not found (history may be expired)"
                        ))
                    })?;
                }
                None => {
                    // Reached the root snapshot.
                    if from_id.is_none() {
                        return Ok(window);
                    }
                    return Err(crate::error::IcebergError::Metadata(format!(
                        "snapshot {} is not an ancestor of {to_id}",
                        from_id.unwrap()
                    )));
                }
            }
        }
    }

    /// Whether every snapshot in `(from_id, to_id]` was created by an `append`
    /// operation. Only then does an added-files incremental scan capture all
    /// changes (no `overwrite`/`delete`/`replace` => no updates or deletions to
    /// miss). A snapshot with no recorded operation is treated as not-append-only
    /// (fail safe: caller should full-refresh). Propagates `snapshot_window`
    /// errors (unknown/expired/non-ancestor).
    pub fn window_is_append_only(
        &self,
        from_id: Option<i64>,
        to_id: i64,
    ) -> crate::error::Result<bool> {
        let window = self.snapshot_window(from_id, to_id)?;
        Ok(window.iter().all(|s| s.operation() == Some("append")))
    }

    /// Whether every snapshot in `(from_id, to_id]` is incremental-safe for an
    /// **added-files-only** scan — i.e. each is an `append` (new data files
    /// only) or a `replace` (compaction: files rewritten without any logical
    /// change). A `replace` is safe because Iceberg preserves each row's
    /// `data_sequence_number` through compaction, so the sequence-number window
    /// `(from.seq, to.seq]` still excludes the rewritten old rows — compaction
    /// never surfaces as a spurious "added" row. `overwrite`/`delete`
    /// operations carry row-level updates and deletions an added-files scan
    /// cannot see, so they are NOT incremental-safe (the caller must
    /// full-refresh). A snapshot with no recorded operation is treated as
    /// unsafe (fail safe). Propagates `snapshot_window` errors
    /// (unknown/expired/non-ancestor).
    ///
    /// This is the check the materialization path uses: it keeps routine
    /// appends *and* periodic compaction on the cheap incremental path, while
    /// still falling back to a full re-read whenever genuine updates/deletes
    /// (overwrite/delete) appear in the window.
    pub fn window_is_incremental_safe(
        &self,
        from_id: Option<i64>,
        to_id: i64,
    ) -> crate::error::Result<bool> {
        let window = self.snapshot_window(from_id, to_id)?;
        Ok(window
            .iter()
            .all(|s| matches!(s.operation(), Some("append" | "replace"))))
    }
}

/// Schema definition.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct Schema {
    /// Schema ID
    #[serde(default)]
    pub schema_id: i32,
    /// Identifier field IDs (for equality deletes)
    #[serde(default)]
    pub identifier_field_ids: Vec<i32>,
    /// Schema fields
    pub fields: Vec<SchemaField>,
}

impl Schema {
    /// Get a field by ID.
    pub fn field(&self, id: i32) -> Option<&SchemaField> {
        self.fields.iter().find(|f| f.id == id)
    }

    /// Get a field by name.
    pub fn field_by_name(&self, name: &str) -> Option<&SchemaField> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Get all field names.
    pub fn field_names(&self) -> Vec<&str> {
        self.fields.iter().map(|f| f.name.as_str()).collect()
    }
}

/// Schema field definition.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct SchemaField {
    /// Field ID
    pub id: i32,
    /// Field name
    pub name: String,
    /// Whether field is required (non-nullable)
    pub required: bool,
    /// Field type (can be string or nested struct)
    #[serde(rename = "type")]
    pub field_type: serde_json::Value,
    /// Documentation
    #[serde(default)]
    pub doc: Option<String>,
}

impl SchemaField {
    /// Get the type as a string (for primitive types).
    pub fn type_string(&self) -> Option<&str> {
        self.field_type.as_str()
    }

    /// Check if this is a nested type (struct, list, map).
    pub fn is_nested(&self) -> bool {
        self.field_type.is_object()
    }
}

/// Partition specification.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionSpec {
    /// Partition spec ID
    pub spec_id: i32,
    /// Partition fields
    #[serde(default)]
    pub fields: Vec<PartitionField>,
}

/// Partition field definition.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionField {
    /// Source column ID
    pub source_id: i32,
    /// Partition field ID
    pub field_id: i32,
    /// Partition field name
    pub name: String,
    /// Transform function (identity, bucket, truncate, year, month, day, hour)
    pub transform: String,
}

/// Sort order definition.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct SortOrder {
    /// Sort order ID
    pub order_id: i32,
    /// Sort fields
    #[serde(default)]
    pub fields: Vec<SortField>,
}

/// Sort field definition.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct SortField {
    /// Source column ID
    pub source_id: i32,
    /// Transform function
    pub transform: String,
    /// Sort direction (asc, desc)
    pub direction: String,
    /// Null ordering (nulls-first, nulls-last)
    pub null_order: String,
}

/// Snapshot log entry.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotLogEntry {
    /// Snapshot ID
    pub snapshot_id: i64,
    /// Timestamp when this snapshot became current (ms since epoch)
    pub timestamp_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_METADATA: &str = r#"{
        "format-version": 2,
        "table-uuid": "test-uuid",
        "location": "s3://bucket/table",
        "last-sequence-number": 3,
        "last-updated-ms": 1700000000000,
        "last-column-id": 5,
        "current-schema-id": 0,
        "schemas": [{
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {"id": 2, "name": "name", "required": false, "type": "string"},
                {"id": 3, "name": "data", "required": false, "type": {
                    "type": "struct",
                    "fields": [{"id": 4, "name": "value", "required": true, "type": "int"}]
                }}
            ]
        }],
        "current-snapshot-id": 2,
        "snapshots": [
            {"snapshot-id": 1, "timestamp-ms": 1699000000000, "summary": {}},
            {"snapshot-id": 2, "timestamp-ms": 1700000000000, "summary": {"total-records": "100"}}
        ],
        "partition-specs": [{
            "spec-id": 0,
            "fields": []
        }],
        "sort-orders": [{
            "order-id": 0,
            "fields": []
        }],
        "properties": {
            "owner": "test"
        }
    }"#;

    #[test]
    fn test_parse_metadata() {
        let metadata = TableMetadata::from_json_str(SAMPLE_METADATA).unwrap();

        assert_eq!(metadata.format_version, 2);
        assert_eq!(metadata.table_uuid, Some("test-uuid".to_string()));
        assert_eq!(metadata.location, "s3://bucket/table");
        assert_eq!(metadata.current_snapshot_id, Some(2));
    }

    #[test]
    fn test_current_schema() {
        let metadata = TableMetadata::from_json_str(SAMPLE_METADATA).unwrap();
        let schema = metadata.current_schema().unwrap();

        assert_eq!(schema.schema_id, 0);
        assert_eq!(schema.fields.len(), 3);
    }

    #[test]
    fn test_schema_field_access() {
        let metadata = TableMetadata::from_json_str(SAMPLE_METADATA).unwrap();
        let schema = metadata.current_schema().unwrap();

        let id_field = schema.field_by_name("id").unwrap();
        assert_eq!(id_field.id, 1);
        assert!(id_field.required);
        assert_eq!(id_field.type_string(), Some("long"));

        let data_field = schema.field_by_name("data").unwrap();
        assert!(data_field.is_nested());
    }

    #[test]
    fn test_current_snapshot() {
        let metadata = TableMetadata::from_json_str(SAMPLE_METADATA).unwrap();
        let snapshot = metadata.current_snapshot().unwrap();

        assert_eq!(snapshot.snapshot_id, 2);
        assert_eq!(snapshot.total_records(), Some(100));
    }

    #[test]
    fn test_snapshot_by_id() {
        let metadata = TableMetadata::from_json_str(SAMPLE_METADATA).unwrap();

        let snap1 = metadata.snapshot(1).unwrap();
        assert_eq!(snap1.timestamp_ms, 1_699_000_000_000);

        let snap2 = metadata.snapshot(2).unwrap();
        assert_eq!(snap2.timestamp_ms, 1_700_000_000_000);

        assert!(metadata.snapshot(999).is_none());
    }

    #[test]
    fn test_properties() {
        let metadata = TableMetadata::from_json_str(SAMPLE_METADATA).unwrap();
        assert_eq!(metadata.properties.get("owner"), Some(&"test".to_string()));
    }

    // ---- incremental window helpers ----

    fn snap(id: i64, parent: Option<i64>, seq: i64, op: Option<&str>) -> crate::metadata::Snapshot {
        let mut summary = HashMap::new();
        if let Some(o) = op {
            summary.insert("operation".to_string(), o.to_string());
        }
        crate::metadata::Snapshot {
            snapshot_id: id,
            parent_snapshot_id: parent,
            sequence_number: seq,
            timestamp_ms: seq * 1000,
            manifest_list: Some(format!("snap-{id}.avro")),
            manifests: None,
            summary,
            schema_id: Some(0),
        }
    }

    fn meta_with(snapshots: Vec<crate::metadata::Snapshot>) -> TableMetadata {
        let current = snapshots.last().map(|s| s.snapshot_id);
        let last_seq = snapshots
            .iter()
            .map(|s| s.sequence_number)
            .max()
            .unwrap_or(0);
        TableMetadata {
            format_version: 2,
            table_uuid: None,
            location: "s3://b/t".to_string(),
            last_sequence_number: last_seq,
            last_updated_ms: 0,
            last_column_id: 1,
            current_schema_id: 0,
            schemas: vec![],
            current_snapshot_id: current,
            snapshots,
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
    fn snapshot_window_walks_parent_chain() {
        let m = meta_with(vec![
            snap(1, None, 1, Some("append")),
            snap(2, Some(1), 2, Some("append")),
            snap(3, Some(2), 3, Some("append")),
        ]);
        // (1, 3] -> snapshots 3 then 2 (newest first), excluding the `from` (1)
        let ids: Vec<i64> = m
            .snapshot_window(Some(1), 3)
            .unwrap()
            .iter()
            .map(|s| s.snapshot_id)
            .collect();
        assert_eq!(ids, vec![3, 2]);
        // from == to -> empty
        assert!(m.snapshot_window(Some(3), 3).unwrap().is_empty());
        // from = None -> full history
        let full: Vec<i64> = m
            .snapshot_window(None, 3)
            .unwrap()
            .iter()
            .map(|s| s.snapshot_id)
            .collect();
        assert_eq!(full, vec![3, 2, 1]);
        // `from` not an ancestor of `to` (branch/rollback) -> error
        assert!(m.snapshot_window(Some(99), 3).is_err());
        // unknown `to` -> error
        assert!(m.snapshot_window(Some(1), 42).is_err());
    }

    #[test]
    fn window_is_append_only_detects_non_append() {
        let all_append = meta_with(vec![
            snap(1, None, 1, Some("append")),
            snap(2, Some(1), 2, Some("append")),
        ]);
        assert!(all_append.window_is_append_only(Some(1), 2).unwrap());
        // from == to -> empty window -> vacuously append-only (nothing to apply)
        assert!(all_append.window_is_append_only(Some(2), 2).unwrap());

        let with_overwrite = meta_with(vec![
            snap(1, None, 1, Some("append")),
            snap(2, Some(1), 2, Some("overwrite")),
        ]);
        assert!(!with_overwrite.window_is_append_only(Some(1), 2).unwrap());

        // A snapshot with no recorded operation is not provably append-only.
        let no_op = meta_with(vec![
            snap(1, None, 1, Some("append")),
            snap(2, Some(1), 2, None),
        ]);
        assert!(!no_op.window_is_append_only(Some(1), 2).unwrap());
    }

    #[test]
    fn window_is_incremental_safe_allows_compaction_but_not_overwrite() {
        // append + compaction (replace) is incremental-safe: compaction
        // preserves data_sequence_number, so the seq-number window still
        // excludes the rewritten old rows.
        let append_then_compact = meta_with(vec![
            snap(1, None, 1, Some("append")),
            snap(2, Some(1), 2, Some("append")),
            snap(3, Some(2), 3, Some("replace")),
        ]);
        assert!(append_then_compact
            .window_is_incremental_safe(Some(1), 3)
            .unwrap());
        // ...but a pure replace window must NOT be treated as append-only.
        assert!(!append_then_compact
            .window_is_append_only(Some(1), 3)
            .unwrap());

        // overwrite carries row-level updates/deletes -> not incremental-safe.
        let with_overwrite = meta_with(vec![
            snap(1, None, 1, Some("append")),
            snap(2, Some(1), 2, Some("overwrite")),
        ]);
        assert!(!with_overwrite
            .window_is_incremental_safe(Some(1), 2)
            .unwrap());

        // delete carries row removals -> not incremental-safe.
        let with_delete = meta_with(vec![
            snap(1, None, 1, Some("append")),
            snap(2, Some(1), 2, Some("delete")),
        ]);
        assert!(!with_delete.window_is_incremental_safe(Some(1), 2).unwrap());

        // unrecorded operation -> fail safe.
        let no_op = meta_with(vec![
            snap(1, None, 1, Some("append")),
            snap(2, Some(1), 2, None),
        ]);
        assert!(!no_op.window_is_incremental_safe(Some(1), 2).unwrap());
    }
}
