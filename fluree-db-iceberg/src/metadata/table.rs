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

    /// The schema in effect AT `snapshot` — the one its rows were written and
    /// committed under — falling back to [`Self::current_schema`] when the
    /// snapshot carries no `schema-id` or the id is unknown (both legal:
    /// `schema-id` is optional on v1-era snapshots).
    ///
    /// Reads pinned to a historical snapshot must project against this, not
    /// `current_schema()`: Iceberg reads Parquet by field id, so what schema
    /// evolution breaks is the name→id mapping (a column renamed since the pin
    /// resolves to nothing, or to a different field) and type interpretation
    /// (a re-typed column decodes wrong). When `snapshot` IS the current
    /// snapshot the two agree and this is a no-op.
    pub fn schema_for_snapshot(&self, snapshot: &super::Snapshot) -> Option<&Schema> {
        snapshot
            .schema_id
            .and_then(|id| self.schema(id))
            .or_else(|| self.current_schema())
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

    /// The end of a window `(from_id, to_id]` capped to at most `max_snapshots`
    /// snapshots, so a consumer can advance through a long backlog in bounded
    /// steps instead of one unbounded pass.
    ///
    /// Returns `to_id` unchanged when the window already fits, when `from_id` is
    /// `None` (an initial full read has no prefix to take), or when
    /// `max_snapshots` is `0` (disabled).
    ///
    /// **Why a consumer wants this.** A materialization whose watermark cannot
    /// advance re-reads a window that grows without bound, and once that window
    /// outgrows the source's snapshot retention the watermark can never be
    /// resolved again — every later poll degrades to a full table read, forever.
    /// Advancing by a bounded prefix keeps the watermark moving, which keeps it
    /// inside retention, which is what makes the incremental path recoverable
    /// rather than a one-way door.
    ///
    /// Errors propagate from [`Self::snapshot_window`] (unknown/expired/
    /// non-ancestor), where the caller must already fall back to a full re-read.
    pub fn window_end_capped(
        &self,
        from_id: Option<i64>,
        to_id: i64,
        max_snapshots: usize,
    ) -> crate::error::Result<i64> {
        if max_snapshots == 0 || from_id.is_none() {
            return Ok(to_id);
        }
        let window = self.snapshot_window(from_id, to_id)?;
        if window.len() <= max_snapshots {
            return Ok(to_id);
        }
        // `snapshot_window` is NEWEST-first, and we want the OLDEST
        // `max_snapshots` of them — the prefix adjacent to `from_id`. Counting
        // that many back from the old end lands on the last snapshot of the
        // prefix, which becomes this pass's `to`.
        Ok(window[window.len() - max_snapshots].snapshot_id)
    }

    /// Where an unpinned incremental consumer should end this pass: the head,
    /// or an earlier snapshot when the backlog from `from_id` exceeds
    /// `max_snapshots`.
    ///
    /// This is the whole decision in one place, so it is testable without a
    /// storage backend — the caller does nothing but use the answer. Every way
    /// of declining to cap returns the head unchanged:
    ///
    /// - no snapshots at all (`None`; there is nothing to read);
    /// - `max_snapshots == 0`, the disable switch;
    /// - `from_id` is `None` — an initial full read has no prefix to take, and a
    ///   partial "full" read would be worse than an unbounded one;
    /// - the backlog already fits;
    /// - the window cannot be walked (expired ancestor, non-ancestor, rollback).
    ///   That is the existing full-read fallback's case and it must keep it.
    ///
    /// Capping only ever returns an EARLIER snapshot than the head, never a
    /// later one, so a consumer that records where it stopped cannot skip data.
    pub fn capped_scan_end(
        &self,
        from_id: Option<i64>,
        max_snapshots: usize,
    ) -> Option<&super::Snapshot> {
        let head = self.current_snapshot()?;
        self.window_end_capped(from_id, head.snapshot_id, max_snapshots)
            .ok()
            .filter(|id| *id != head.snapshot_id)
            .and_then(|id| self.snapshot(id))
            .or(Some(head))
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
    fn schema_for_snapshot_follows_the_snapshot_schema_id() {
        let bare_schema = |id: i32| Schema {
            schema_id: id,
            identifier_field_ids: vec![],
            fields: vec![],
        };
        let mut m = meta_with(vec![
            snap(1, None, 1, Some("append")),
            snap(2, Some(1), 2, Some("append")),
        ]);
        // Schema evolved after snapshot 1: current is 1, snapshot 1 pinned 0.
        m.schemas = vec![bare_schema(0), bare_schema(1)];
        m.current_schema_id = 1;
        m.snapshots[0].schema_id = Some(0);

        // Pinned snapshot → its own (historical) schema, not current.
        let s1 = m.snapshot(1).unwrap();
        assert_eq!(m.schema_for_snapshot(s1).unwrap().schema_id, 0);

        // Snapshot pinning the current schema → current.
        m.snapshots[1].schema_id = Some(1);
        let s2 = m.snapshot(2).unwrap();
        assert_eq!(m.schema_for_snapshot(s2).unwrap().schema_id, 1);

        // No schema-id on the snapshot (v1-era metadata) → fall back to current.
        m.snapshots[0].schema_id = None;
        let s1 = m.snapshot(1).unwrap();
        assert_eq!(m.schema_for_snapshot(s1).unwrap().schema_id, 1);

        // Unknown schema-id → fall back to current rather than failing the read.
        m.snapshots[0].schema_id = Some(99);
        let s1 = m.snapshot(1).unwrap();
        assert_eq!(m.schema_for_snapshot(s1).unwrap().schema_id, 1);
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

    /// A backlog longer than the cap is advanced in bounded steps, taking the
    /// OLDEST snapshots first. Taking the newest instead would skip everything
    /// between `from` and the chosen end — silent data loss, and the reason the
    /// direction is asserted rather than assumed.
    #[test]
    fn window_end_capped_takes_the_oldest_prefix() {
        // 1 <- 2 <- 3 <- 4 <- 5, watermark at 1, so the window is (1, 5] = 4 wide.
        let meta = meta_with(vec![
            snap(1, None, 1, Some("append")),
            snap(2, Some(1), 2, Some("append")),
            snap(3, Some(2), 3, Some("append")),
            snap(4, Some(3), 4, Some("append")),
            snap(5, Some(4), 5, Some("append")),
        ]);

        // Cap 2 -> advance to snapshot 3, NOT 5: the two oldest after the
        // watermark. Snapshot 2 must not be skipped.
        assert_eq!(meta.window_end_capped(Some(1), 5, 2).unwrap(), 3);
        // Cap 1 -> one step at a time.
        assert_eq!(meta.window_end_capped(Some(1), 5, 1).unwrap(), 2);
        // Successive passes converge on the head rather than stalling short of it.
        assert_eq!(meta.window_end_capped(Some(3), 5, 2).unwrap(), 5);
    }

    #[test]
    fn window_end_capped_is_a_no_op_when_it_cannot_help() {
        let meta = meta_with(vec![
            snap(1, None, 1, Some("append")),
            snap(2, Some(1), 2, Some("append")),
            snap(3, Some(2), 3, Some("append")),
        ]);

        // Window already fits.
        assert_eq!(meta.window_end_capped(Some(1), 3, 5).unwrap(), 3);
        // Disabled.
        assert_eq!(meta.window_end_capped(Some(1), 3, 0).unwrap(), 3);
        // An initial full read has no prefix to take — capping it would produce a
        // partial "full" read, which is worse than the unbounded one.
        assert_eq!(meta.window_end_capped(None, 3, 1).unwrap(), 3);
        // from == to: empty window, nothing to cap.
        assert_eq!(meta.window_end_capped(Some(3), 3, 1).unwrap(), 3);
    }

    /// `capped_scan_end` is the decision a scan actually makes, so every way of
    /// declining to cap is pinned here — a consumer calls this and nothing else.
    #[test]
    fn capped_scan_end_covers_every_decline_path() {
        let meta = meta_with(vec![
            snap(1, None, 1, Some("append")),
            snap(2, Some(1), 2, Some("append")),
            snap(3, Some(2), 3, Some("append")),
            snap(4, Some(3), 4, Some("append")),
        ]);
        let head = 4;

        // Caps when the backlog exceeds the limit.
        assert_eq!(meta.capped_scan_end(Some(1), 2).unwrap().snapshot_id, 3);

        // Declines: disabled, initial full read, backlog already fits.
        assert_eq!(meta.capped_scan_end(Some(1), 0).unwrap().snapshot_id, head);
        assert_eq!(meta.capped_scan_end(None, 1).unwrap().snapshot_id, head);
        assert_eq!(meta.capped_scan_end(Some(1), 99).unwrap().snapshot_id, head);

        // Declines: the window cannot be walked. `from` is not an ancestor here,
        // which is the rollback/branch case — the full-read fallback owns it, so
        // this must return the head rather than inventing a bound.
        let orphan = meta_with(vec![
            snap(7, None, 7, Some("append")),
            snap(8, Some(7), 8, Some("append")),
        ]);
        assert_eq!(orphan.capped_scan_end(Some(1), 1).unwrap().snapshot_id, 8);

        // No snapshots: nothing to read, and no panic.
        assert!(meta_with(vec![]).capped_scan_end(Some(1), 1).is_none());
    }

    /// The cap must never hand back a snapshot NEWER than the head — that would
    /// read past what the caller asked for.
    #[test]
    fn capped_scan_end_never_exceeds_the_head() {
        let meta = meta_with(vec![
            snap(1, None, 1, Some("append")),
            snap(2, Some(1), 2, Some("append")),
            snap(3, Some(2), 3, Some("append")),
        ]);
        for cap in 0..6 {
            let chosen = meta.capped_scan_end(Some(1), cap).unwrap().sequence_number;
            assert!(
                chosen <= 3,
                "cap {cap} chose sequence {chosen}, past the head"
            );
        }
    }

    /// An expired ancestor must still ERROR rather than silently capping to
    /// something arbitrary — the caller falls back to a full re-read, and that
    /// decision has to stay with the caller.
    #[test]
    fn window_end_capped_propagates_an_expired_ancestor() {
        let meta = meta_with(vec![
            snap(3, Some(2), 3, Some("append")),
            snap(4, Some(3), 4, Some("append")),
        ]);
        assert!(meta.window_end_capped(Some(1), 4, 2).is_err());
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
