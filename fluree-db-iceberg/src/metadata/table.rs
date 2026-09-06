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

/// The outcome of [`TableMetadata::cap_window_by_rows`].
///
/// Typed rather than `Option` so a consumer can say WHICH reason left a window
/// unbounded. An over-budget window that is read whole cannot commit under the
/// ceiling the budget was derived from, and an uncommitted window writes no
/// watermark — so a silent decline is an invisible livelock, and the log has to
/// be able to name it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowCap {
    /// Read to `to_id`: the window fits the budget, or the cap does not apply
    /// (`max_rows == 0`, or no `from_id` to take a prefix after).
    Whole,
    /// Stop at this snapshot; everything after it waits for the next pass.
    Capped(i64),
    /// This snapshot carries no `added-records` summary, so the window cannot be
    /// sized and no cut is known to be safe. Read whole.
    Unsized(i64),
    /// The window is a single commit and over budget: there is no boundary short
    /// of `to_id` to stop at. Read whole.
    SingleCommit,
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

    /// Where an incremental consumer should stop reading `(from_id, to_id]` so
    /// that one pass stays near `max_rows`: at `to_id` when the window fits,
    /// else at the last snapshot the budget covers.
    ///
    /// Sized from each snapshot's `added-records` summary — the rows an
    /// added-files scan of that snapshot reads — so the decision needs no
    /// manifest I/O and is testable without a backend. A `replace` (compaction)
    /// counts its rewritten rows, which the scan then excludes by sequence, so
    /// the estimate only ever errs toward a smaller pass.
    ///
    /// **Why bound an incremental window at all.** A pass only advances the
    /// watermark when its whole window commits, and a window whose flakes exceed
    /// the target's novelty ceiling never can: it is deferred, nothing is
    /// recorded, and the next poll re-reads a window one poll wider. Once that
    /// window outgrows the source's snapshot retention the watermark stops
    /// resolving and every poll becomes a full read of the whole table. Capping
    /// each pass keeps the watermark moving, which keeps it inside retention.
    ///
    /// The snapshot the budget runs out in is kept WHOLE: a cut inside a commit
    /// would leave the target in a state no snapshot names, and an unnameable
    /// state cannot be resumed from. So a pass may exceed `max_rows` by up to
    /// one snapshot, and a consumer sizes the budget with that margin. Capping
    /// only ever moves the end EARLIER than `to_id`, never later, so nothing is
    /// skipped: the next pass resumes from where this one stopped.
    ///
    /// Errors propagate from [`Self::snapshot_window`] (unknown / expired /
    /// non-ancestor); the caller's full-read fallback owns those.
    pub fn cap_window_by_rows(
        &self,
        from_id: Option<i64>,
        to_id: i64,
        max_rows: i64,
    ) -> crate::error::Result<WindowCap> {
        if max_rows <= 0 || from_id.is_none() {
            return Ok(WindowCap::Whole);
        }
        let window = self.snapshot_window(from_id, to_id)?;
        // Size the whole window first: a window that fits is never split, and a
        // snapshot that cannot be sized anywhere in it means no cut is known to
        // be safe — the same rule the full-read cut applies to a task without a
        // commit sequence.
        let mut total = 0i64;
        for s in &window {
            let Some(added) = s.added_records() else {
                return Ok(WindowCap::Unsized(s.snapshot_id));
            };
            total = total.saturating_add(added);
        }
        if total <= max_rows {
            return Ok(WindowCap::Whole);
        }
        // `snapshot_window` is NEWEST-first; walk it oldest-first so the prefix
        // kept is the one adjacent to `from_id`. Taking the newest instead would
        // skip everything between the watermark and the chosen end — data loss,
        // not a performance bug.
        let mut rows = 0i64;
        for s in window.iter().rev() {
            rows = rows.saturating_add(s.added_records().unwrap_or(0));
            if rows >= max_rows {
                if s.snapshot_id != to_id {
                    return Ok(WindowCap::Capped(s.snapshot_id));
                }
                // The budget ran out inside the head. Stopping there is the whole
                // window with extra bookkeeping, so stop at the boundary just
                // below it instead — a smaller pass than asked for, and still
                // forward progress — unless the window IS the head, in which
                // case there is no boundary to stop at.
                return Ok(match window.get(1) {
                    Some(below_head) => WindowCap::Capped(below_head.snapshot_id),
                    None => WindowCap::SingleCommit,
                });
            }
        }
        // Unreachable: `total > max_rows` guarantees the accumulator crosses the
        // budget above. Typed as the harmless outcome rather than a panic.
        Ok(WindowCap::Whole)
    }

    /// The newest ancestor of `to_id` whose sequence number is at or below
    /// `seq` — the snapshot a consumer has reached once it has applied every
    /// file up to and including that sequence.
    ///
    /// This is what lets a *partial* full read record its progress. A full read
    /// selects the files live at `to_id`; applying only those with an effective
    /// sequence at or below `seq` leaves the target holding exactly the rows
    /// that had arrived by this snapshot, so the consumer can checkpoint here
    /// and let the next pass scan `(this, to_id]`.
    ///
    /// Returns `None` when no ancestor qualifies — every retained snapshot is
    /// newer than `seq` — in which case there is no safe checkpoint short of
    /// the whole read, and the consumer must not invent one.
    pub fn snapshot_at_or_before_sequence(
        &self,
        to_id: i64,
        seq: i64,
    ) -> crate::error::Result<Option<&super::Snapshot>> {
        // Walk parents from `to`, newest first, and stop at the first match.
        //
        // Deliberately NOT `snapshot_window(None, to_id)`: that walks the whole
        // history to the ROOT and errors on any missing ancestor. The tables
        // that most need a checkpoint are precisely the ones whose old
        // snapshots have expired, so requiring an intact history would refuse
        // exactly the case this exists for — and silently, by falling back to
        // an unbounded read.
        //
        // The walk is short in practice: the cut lands near the start of the
        // backlog, so the answer is usually a few hops from `to`. Hitting a
        // missing ancestor before finding one means there is no nameable
        // checkpoint, which is `None` — the caller then reads the whole thing,
        // which is correct rather than merely safe.
        let mut cur = self.snapshot(to_id).ok_or_else(|| {
            crate::error::IcebergError::SnapshotNotFound(format!("snapshot {to_id} not found"))
        })?;
        loop {
            if cur.sequence_number <= seq {
                return Ok(Some(cur));
            }
            match cur.parent_snapshot_id.and_then(|pid| self.snapshot(pid)) {
                Some(parent) => cur = parent,
                // Root reached, or the parent has been expired. Either way there
                // is nothing older we can name.
                None => return Ok(None),
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

    /// A snapshot that appended `rows` rows, as a writer records it.
    fn appended(id: i64, parent: Option<i64>, seq: i64, rows: i64) -> crate::metadata::Snapshot {
        let mut s = snap(id, parent, seq, Some("append"));
        s.summary
            .insert("added-records".to_string(), rows.to_string());
        s
    }

    /// A backlog over budget is advanced in bounded steps, taking the OLDEST
    /// snapshots first. Taking the newest instead would skip everything between
    /// `from` and the chosen end — silent data loss, and the reason the direction
    /// is asserted rather than assumed.
    #[test]
    fn cap_window_by_rows_takes_the_oldest_prefix() {
        // 1 <- 2 <- 3 <- 4 <- 5, 100 rows each; watermark at 1, so the window is
        // (1, 5] = 400 rows.
        let meta = meta_with(vec![
            appended(1, None, 1, 100),
            appended(2, Some(1), 2, 100),
            appended(3, Some(2), 3, 100),
            appended(4, Some(3), 4, 100),
            appended(5, Some(4), 5, 100),
        ]);

        // Budget 150: snapshot 2 fits, the budget runs out inside 3, and 3 is
        // kept whole -> stop at 3, NOT at 5, and never skipping 2.
        assert_eq!(
            meta.cap_window_by_rows(Some(1), 5, 150).unwrap(),
            WindowCap::Capped(3)
        );
        // Budget 100 -> one snapshot at a time.
        assert_eq!(
            meta.cap_window_by_rows(Some(1), 5, 100).unwrap(),
            WindowCap::Capped(2)
        );
        // Successive passes converge on the head rather than stalling short of it.
        assert_eq!(
            meta.cap_window_by_rows(Some(3), 5, 150).unwrap(),
            WindowCap::Capped(4)
        );
        assert_eq!(
            meta.cap_window_by_rows(Some(4), 5, 150).unwrap(),
            WindowCap::Whole
        );
    }

    /// The budget running out INSIDE the head is not "read it whole": that is the
    /// unbounded pass with extra bookkeeping, and on a source whose commits are
    /// each near the budget it would mean the bound never engages. Stop at the
    /// boundary just below the head instead — unless there is none.
    #[test]
    fn cap_window_by_rows_stops_below_a_head_it_cannot_fit() {
        let meta = meta_with(vec![
            appended(1, None, 1, 100),
            appended(2, Some(1), 2, 100),
            appended(3, Some(2), 3, 100),
        ]);
        // (1, 3] = 200 rows against a budget of 150: 2 fits, the budget runs out
        // in the head, so the pass stops at 2 and the next one reads 3 alone.
        assert_eq!(
            meta.cap_window_by_rows(Some(1), 3, 150).unwrap(),
            WindowCap::Capped(2)
        );
        // One commit wide and over budget: nothing short of the head to stop at.
        assert_eq!(
            meta.cap_window_by_rows(Some(2), 3, 50).unwrap(),
            WindowCap::SingleCommit
        );
    }

    #[test]
    fn cap_window_by_rows_is_whole_when_it_cannot_help() {
        let meta = meta_with(vec![
            appended(1, None, 1, 100),
            appended(2, Some(1), 2, 100),
            appended(3, Some(2), 3, 100),
        ]);
        let whole = |from, to, budget| meta.cap_window_by_rows(from, to, budget).unwrap();

        // Window already fits — exactly on the budget counts as fitting, so a
        // healthy source whose window matches the budget is never split.
        assert_eq!(whole(Some(1), 3, 500), WindowCap::Whole);
        assert_eq!(whole(Some(1), 3, 200), WindowCap::Whole);
        // Disabled.
        assert_eq!(whole(Some(1), 3, 0), WindowCap::Whole);
        // An initial full read has no prefix to take — a partial "full" read
        // would look complete while missing rows.
        assert_eq!(whole(None, 3, 1), WindowCap::Whole);
        // from == to: empty window, nothing to cap.
        assert_eq!(whole(Some(3), 3, 1), WindowCap::Whole);
    }

    /// A snapshot without `added-records` cannot be sized, and a cut placed
    /// without knowing the sizes is a guess. Decline, and name the snapshot so
    /// the log can say why the window went unbounded.
    #[test]
    fn cap_window_by_rows_declines_an_unsized_snapshot() {
        let meta = meta_with(vec![
            appended(1, None, 1, 100),
            appended(2, Some(1), 2, 100),
            snap(3, Some(2), 3, Some("append")),
            appended(4, Some(3), 4, 100),
        ]);
        assert_eq!(
            meta.cap_window_by_rows(Some(1), 4, 150).unwrap(),
            WindowCap::Unsized(3)
        );
    }

    /// An expired ancestor must still ERROR rather than silently capping to
    /// something arbitrary — the caller falls back to a full re-read, and that
    /// decision has to stay with the caller.
    #[test]
    fn cap_window_by_rows_propagates_an_unwalkable_window() {
        let expired = meta_with(vec![
            appended(3, Some(2), 3, 100),
            appended(4, Some(3), 4, 100),
        ]);
        assert!(expired.cap_window_by_rows(Some(1), 4, 50).is_err());

        // `from` is not an ancestor: the rollback/branch case.
        let orphan = meta_with(vec![
            appended(7, None, 7, 100),
            appended(8, Some(7), 8, 100),
        ]);
        assert!(orphan.cap_window_by_rows(Some(1), 8, 50).is_err());
    }

    /// The checkpoint walk must survive an EXPIRED ancestor, because the tables
    /// that need a checkpoint are the ones whose history has been expiring.
    ///
    /// Using a full-history walk here was a real bug: it errored on these very
    /// tables and the caller fell through to an unbounded read, so the bound
    /// never engaged in production while every unit test passed.
    #[test]
    fn snapshot_at_or_before_sequence_survives_an_expired_ancestor() {
        // 5 <- 6 <- 7 retained; 5's parent (4) has been expired away.
        let meta = meta_with(vec![
            snap(5, Some(4), 50, Some("append")),
            snap(6, Some(5), 60, Some("append")),
            snap(7, Some(6), 70, Some("append")),
        ]);

        // A checkpoint inside the retained range is found without ever touching
        // the missing ancestor.
        assert_eq!(
            meta.snapshot_at_or_before_sequence(7, 60)
                .unwrap()
                .map(|s| s.snapshot_id),
            Some(6)
        );
        // Below everything retained: no nameable checkpoint, but NOT an error —
        // the caller reads the whole thing. `is_none`, not `assert_eq!(.., None)`:
        // `Snapshot` has no `PartialEq`.
        assert!(meta
            .snapshot_at_or_before_sequence(7, 10)
            .unwrap()
            .is_none());
    }

    /// A partial full read checkpoints at the newest ancestor whose sequence it
    /// has fully applied — never at one it has only partly reached.
    #[test]
    fn snapshot_at_or_before_sequence_picks_the_newest_fully_applied() {
        let meta = meta_with(vec![
            snap(1, None, 10, Some("append")),
            snap(2, Some(1), 20, Some("append")),
            snap(3, Some(2), 30, Some("append")),
        ]);

        // Exactly on a boundary: that snapshot is fully applied.
        let at = |seq| {
            meta.snapshot_at_or_before_sequence(3, seq)
                .unwrap()
                .map(|s| s.snapshot_id)
        };
        assert_eq!(at(20), Some(2));
        // Between boundaries: fall BACK to the last fully-applied one. Rounding
        // up would checkpoint past rows that were never applied.
        assert_eq!(at(25), Some(2));
        // At or past the head.
        assert_eq!(at(30), Some(3));
        assert_eq!(at(99), Some(3));
        // Below every retained snapshot: no nameable checkpoint, so the caller
        // must read the whole thing rather than invent one.
        assert_eq!(at(5), None);
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
