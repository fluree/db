//! Scan planning for Iceberg tables.
//!
//! This module provides the `ScanPlanner` which:
//! 1. Loads the manifest list for a snapshot
//! 2. Prunes manifests using partition summaries
//! 3. Loads manifests and collects data files
//! 4. Prunes files using column statistics
//! 5. Builds `FileScanTask`s with projection information

use std::sync::Arc;

use crate::error::{IcebergError, Result};
use crate::io::IcebergStorage;
use crate::manifest::{parse_manifest, parse_manifest_list_with_deletes, DataFile};
use crate::metadata::{Schema, Snapshot, TableMetadata};
use crate::scan::predicate::Expression;
use crate::scan::pruning::can_contain_file;

/// Configuration for a table scan.
#[derive(Debug, Clone)]
pub struct ScanConfig {
    /// Field IDs to project (canonical). If None, project all columns.
    pub projection: Option<Vec<i32>>,
    /// Filter predicate for pushdown.
    pub filter: Option<Expression>,
    /// Maximum rows per batch (default: 1024).
    pub batch_row_limit: usize,
    /// Optional byte budget for batches (stop when string/bytes exceed this).
    pub batch_byte_budget: Option<usize>,
}

impl Default for ScanConfig {
    fn default() -> Self {
        Self {
            projection: None,
            filter: None,
            batch_row_limit: 1024,
            batch_byte_budget: None,
        }
    }
}

impl ScanConfig {
    /// Create a new scan config with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the projection (field IDs to read).
    pub fn with_projection(mut self, field_ids: Vec<i32>) -> Self {
        self.projection = Some(field_ids);
        self
    }

    /// Set the filter predicate.
    pub fn with_filter(mut self, filter: Expression) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Set the batch row limit.
    pub fn with_batch_row_limit(mut self, limit: usize) -> Self {
        self.batch_row_limit = limit;
        self
    }

    /// Set the batch byte budget.
    pub fn with_batch_byte_budget(mut self, budget: usize) -> Self {
        self.batch_byte_budget = Some(budget);
        self
    }
}

/// A file scan task representing a single data file to read.
#[derive(Debug, Clone)]
pub struct FileScanTask {
    /// The data file to read.
    pub data_file: DataFile,
    /// Field IDs to project (read only these columns).
    pub projected_field_ids: Vec<i32>,
    /// Residual filter to apply after reading (predicates not fully evaluated by stats).
    pub residual_filter: Option<Expression>,
    /// Start position for split reading (0 for whole file).
    pub start: i64,
    /// Length for split reading (file size for whole file).
    pub length: i64,
    /// Iceberg schema for field ID mapping (ensures correct column mapping after schema evolution).
    pub iceberg_schema: Option<Arc<Schema>>,
}

impl FileScanTask {
    /// Create a task for reading an entire file.
    pub fn for_whole_file(
        data_file: DataFile,
        projected_field_ids: Vec<i32>,
        residual_filter: Option<Expression>,
    ) -> Self {
        let length = data_file.file_size_in_bytes;
        Self {
            data_file,
            projected_field_ids,
            residual_filter,
            start: 0,
            length,
            iceberg_schema: None,
        }
    }

    /// Create a task for reading an entire file with schema for field ID mapping.
    pub fn for_whole_file_with_schema(
        data_file: DataFile,
        projected_field_ids: Vec<i32>,
        residual_filter: Option<Expression>,
        schema: Arc<Schema>,
    ) -> Self {
        let length = data_file.file_size_in_bytes;
        Self {
            data_file,
            projected_field_ids,
            residual_filter,
            start: 0,
            length,
            iceberg_schema: Some(schema),
        }
    }
}

/// A scan plan containing all file tasks and metadata.
#[derive(Debug)]
pub struct ScanPlan {
    /// File scan tasks to execute.
    pub tasks: Vec<FileScanTask>,
    /// Projected column names (for reference).
    pub projected_columns: Vec<String>,
    /// Projected field IDs.
    pub projected_field_ids: Vec<i32>,
    /// Residual filter to apply to results.
    pub residual_filter: Option<Expression>,
    /// Estimated total row count.
    pub estimated_row_count: i64,
    /// Number of files selected.
    pub files_selected: usize,
    /// Number of files pruned by statistics.
    pub files_pruned: usize,
    /// Whether the source snapshot carried merge-on-read delete files (summary
    /// counters or a `content=1` delete manifest). Under the fail-closed guard
    /// this is only ever `true` when [`crate::mor_guard`] was overridden — the
    /// planner refuses otherwise — but it must be propagated so any cached
    /// scan-file selection derived from this plan can re-refuse if the override
    /// is later turned off (audit F-AUD-1, cache-arm follow-up).
    pub has_delete_manifests: bool,
}

impl ScanPlan {
    /// Check if the scan plan is empty (no files to read).
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Get the total estimated bytes to read.
    pub fn estimated_bytes(&self) -> i64 {
        self.tasks
            .iter()
            .map(|t| t.data_file.file_size_in_bytes)
            .sum()
    }
}

/// An incremental (append-only) scan plan: the data-file tasks ADDED in a
/// `(from_sequence_number, to]` window, used to materialize only the rows that
/// appeared since the last sync.
///
/// This captures the **added** half of a changelog only; callers must ensure the
/// window contains no `overwrite`/`delete`/`replace` snapshots (see
/// [`TableMetadata::window_is_append_only`](crate::metadata::TableMetadata::window_is_append_only))
/// and otherwise fall back to a full re-materialization.
#[derive(Debug)]
pub struct IncrementalScanPlan {
    /// Tasks for data files added in the window.
    pub added_tasks: Vec<FileScanTask>,
    /// Projected column names (for reference).
    pub projected_columns: Vec<String>,
    /// Projected field IDs.
    pub projected_field_ids: Vec<i32>,
    /// Exclusive lower bound: only files with effective data sequence number
    /// strictly greater than this are included (`0` => from genesis).
    pub from_sequence_number: i64,
    /// The snapshot the window ends at (inclusive).
    pub to_snapshot_id: i64,
    /// Sequence number of `to_snapshot_id`.
    pub to_sequence_number: i64,
    /// Number of added files selected.
    pub files_selected: usize,
    /// Estimated added row count.
    pub estimated_row_count: i64,
}

/// Effective data sequence number of a manifest entry, applying Iceberg's
/// inheritance rule: an entry whose own sequence number is null/`0` inherits the
/// sequence number of the manifest (i.e. the manifest-list entry) that holds it.
///
/// This is what makes `ADDED` entries (often written with a null sequence number)
/// resolve to the sequence number of the snapshot that added them.
pub fn effective_sequence_number(
    entry_sequence_number: Option<i64>,
    manifest_sequence_number: i64,
) -> i64 {
    match entry_sequence_number {
        Some(s) if s != 0 => s,
        _ => manifest_sequence_number,
    }
}

/// Scan planner for Iceberg tables.
pub struct ScanPlanner<'a, S: IcebergStorage> {
    storage: &'a S,
    metadata: &'a TableMetadata,
    config: ScanConfig,
}

impl<'a, S: IcebergStorage> ScanPlanner<'a, S> {
    /// Create a new scan planner.
    pub fn new(storage: &'a S, metadata: &'a TableMetadata, config: ScanConfig) -> Self {
        Self {
            storage,
            metadata,
            config,
        }
    }

    /// Plan a scan for the current snapshot.
    pub async fn plan_scan(&self) -> Result<ScanPlan> {
        let snapshot = self
            .metadata
            .current_snapshot()
            .ok_or_else(|| IcebergError::SnapshotNotFound("No current snapshot".to_string()))?;

        self.plan_scan_for_snapshot(snapshot).await
    }

    /// Plan a scan for a specific snapshot.
    pub async fn plan_scan_for_snapshot(&self, snapshot: &Snapshot) -> Result<ScanPlan> {
        let schema = self
            .metadata
            .current_schema()
            .ok_or_else(|| IcebergError::Metadata("No current schema".to_string()))?;

        // Clone schema into Arc for sharing with tasks
        let schema_arc = Arc::new(schema.clone());

        // Determine projection
        let (projected_field_ids, projected_columns) = self.resolve_projection(schema)?;

        // Fail closed on merge-on-read delete files (F-AUD-1): the scan reads
        // only live data files and never applies deletes, so a MoR snapshot
        // would silently return deleted rows. Cheap zero-I/O check first.
        let allow_mor = crate::mor_guard::mor_deletes_allowed();
        crate::mor_guard::ensure_no_summary_deletes(snapshot, &self.metadata.location, allow_mor)?;

        // Load manifest list
        let manifest_list_path = snapshot.manifest_list.as_ref().ok_or_else(|| {
            IcebergError::Manifest(
                "Snapshot has no manifest list (v1 format not supported)".to_string(),
            )
        })?;

        let manifest_list_data = self.storage.read(manifest_list_path).await?;
        // Parse WITH delete manifests so the belt-and-suspenders guard can DETECT
        // them even when the snapshot summary omits/under-counts the counters.
        let manifest_entries = parse_manifest_list_with_deletes(&manifest_list_data, true)?;
        let delete_manifests = manifest_entries.iter().filter(|e| e.is_deletes()).count();
        crate::mor_guard::ensure_no_delete_manifests(
            delete_manifests,
            &self.metadata.location,
            allow_mor,
        )?;
        // Only ever `true` under the override (else refused above); propagated so
        // a cached scan-file selection can re-refuse if the override is later off.
        let has_delete_manifests =
            crate::mor_guard::summary_indicates_deletes(snapshot) || delete_manifests > 0;

        tracing::debug!(
            manifest_count = manifest_entries.len(),
            "Loaded manifest list"
        );

        // Collect data files from manifests, applying pruning
        let mut tasks = Vec::new();
        let mut files_selected = 0;
        let mut files_pruned = 0;
        let mut estimated_row_count = 0i64;

        for manifest_entry in &manifest_entries {
            // Skip delete manifests. Under the guard's default they never reach
            // here (refused above); under the override they are ignored (the
            // documented, pre-guard behavior).
            if manifest_entry.is_deletes() {
                continue;
            }

            // Load and parse manifest file
            let manifest_data = self.storage.read(&manifest_entry.manifest_path).await?;
            let data_file_entries = parse_manifest(&manifest_data)?;

            for entry in data_file_entries {
                let data_file = entry.data_file;

                // Apply file-level pruning
                if let Some(filter) = &self.config.filter {
                    if !can_contain_file(filter, &data_file, schema) {
                        files_pruned += 1;
                        continue;
                    }
                }

                files_selected += 1;
                estimated_row_count += data_file.record_count;

                // Create file scan task with schema for correct field ID mapping
                let task = FileScanTask::for_whole_file_with_schema(
                    data_file,
                    projected_field_ids.clone(),
                    self.config.filter.clone(),
                    Arc::clone(&schema_arc),
                );
                tasks.push(task);
            }
        }

        tracing::info!(
            files_selected,
            files_pruned,
            estimated_row_count,
            "Scan planning complete"
        );

        Ok(ScanPlan {
            tasks,
            projected_columns,
            projected_field_ids,
            residual_filter: self.config.filter.clone(),
            estimated_row_count,
            files_selected,
            files_pruned,
            has_delete_manifests,
        })
    }

    /// Resolve projection to field IDs and column names.
    fn resolve_projection(&self, schema: &Schema) -> Result<(Vec<i32>, Vec<String>)> {
        match &self.config.projection {
            Some(field_ids) => {
                let mut names = Vec::with_capacity(field_ids.len());
                for &id in field_ids {
                    let field = schema.field(id).ok_or_else(|| {
                        IcebergError::Scan(format!("Field ID {id} not found in schema"))
                    })?;
                    names.push(field.name.clone());
                }
                Ok((field_ids.clone(), names))
            }
            None => {
                // Project all non-nested fields
                let field_ids: Vec<i32> = schema
                    .fields
                    .iter()
                    .filter(|f| !f.is_nested())
                    .map(|f| f.id)
                    .collect();
                let names: Vec<String> = schema
                    .fields
                    .iter()
                    .filter(|f| !f.is_nested())
                    .map(|f| f.name.clone())
                    .collect();
                Ok((field_ids, names))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_config_builder() {
        let config = ScanConfig::new()
            .with_projection(vec![1, 2, 3])
            .with_filter(Expression::gt(
                1,
                "id",
                crate::scan::predicate::LiteralValue::Int64(100),
            ))
            .with_batch_row_limit(2048)
            .with_batch_byte_budget(1024 * 1024);

        assert_eq!(config.projection, Some(vec![1, 2, 3]));
        assert!(config.filter.is_some());
        assert_eq!(config.batch_row_limit, 2048);
        assert_eq!(config.batch_byte_budget, Some(1024 * 1024));
    }

    #[test]
    fn test_scan_config_default() {
        let config = ScanConfig::default();
        assert!(config.projection.is_none());
        assert!(config.filter.is_none());
        assert_eq!(config.batch_row_limit, 1024);
        assert!(config.batch_byte_budget.is_none());
    }

    #[test]
    fn test_file_scan_task_creation() {
        let data_file = DataFile {
            file_path: "s3://bucket/data/file.parquet".to_string(),
            file_format: crate::manifest::FileFormat::Parquet,
            record_count: 1000,
            file_size_in_bytes: 10240,
            partition: crate::manifest::PartitionData::default(),
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            split_offsets: None,
            sort_order_id: None,
        };

        let task = FileScanTask::for_whole_file(data_file.clone(), vec![1, 2], None);

        assert_eq!(task.data_file.file_path, "s3://bucket/data/file.parquet");
        assert_eq!(task.projected_field_ids, vec![1, 2]);
        assert!(task.residual_filter.is_none());
        assert_eq!(task.start, 0);
        assert_eq!(task.length, 10240);
    }

    #[test]
    fn test_effective_sequence_number_inheritance() {
        // An explicit, non-zero entry sequence number is used as-is.
        assert_eq!(effective_sequence_number(Some(7), 3), 7);
        // A null sequence number inherits the manifest's (ADDED entries are
        // commonly written with a null sequence number).
        assert_eq!(effective_sequence_number(None, 5), 5);
        // Zero is treated as null and also inherits.
        assert_eq!(effective_sequence_number(Some(0), 5), 5);
    }
}

/// Fail-closed merge-on-read guard, exercised end-to-end through `plan_scan`
/// (F-AUD-1). These build a REAL Avro manifest list with a `content=1` delete
/// manifest — the fixture that did not previously exist — so the CI actually
/// covers the guard rather than asserting on a synthetic bool.
#[cfg(test)]
mod mor_guard_tests {
    use super::*;
    use crate::error::IcebergError;
    use crate::io::MemoryStorage;
    use crate::metadata::{Schema, SchemaField, Snapshot, TableMetadata};
    use apache_avro::{types::Record, Schema as AvroSchema, Writer};
    use bytes::Bytes;
    use std::collections::HashMap;

    const MANIFEST_LIST_SCHEMA: &str = r#"{
      "type": "record",
      "name": "manifest_file",
      "fields": [
        {"name": "manifest_path", "type": "string"},
        {"name": "manifest_length", "type": "long"},
        {"name": "partition_spec_id", "type": "int"},
        {"name": "content", "type": "int", "default": 0},
        {"name": "sequence_number", "type": "long", "default": 0},
        {"name": "min_sequence_number", "type": "long", "default": 0},
        {"name": "added_snapshot_id", "type": "long"},
        {"name": "added_data_files_count", "type": "int", "default": 0},
        {"name": "existing_data_files_count", "type": "int", "default": 0},
        {"name": "deleted_data_files_count", "type": "int", "default": 0},
        {"name": "added_rows_count", "type": "long", "default": 0},
        {"name": "existing_rows_count", "type": "long", "default": 0},
        {"name": "deleted_rows_count", "type": "long", "default": 0},
        {"name": "partitions", "type": ["null", {"type": "array", "items": {
          "type": "record", "name": "field_summary",
          "fields": [{"name": "contains_null", "type": "boolean"}]
        }}], "default": null}
      ]
    }"#;

    /// Build a manifest-list Avro carrying the given `(path, content)` entries
    /// (content 0 = data, 1 = delete).
    fn build_manifest_list(entries: &[(&str, i32)]) -> Bytes {
        let schema = AvroSchema::parse_str(MANIFEST_LIST_SCHEMA).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());
        for (path, content) in entries {
            let mut record = Record::new(writer.schema()).unwrap();
            record.put("manifest_path", *path);
            record.put("manifest_length", 100i64);
            record.put("partition_spec_id", 0i32);
            record.put("content", *content);
            record.put("sequence_number", 1i64);
            record.put("min_sequence_number", 1i64);
            record.put("added_snapshot_id", 100i64);
            record.put("added_data_files_count", 1i32);
            record.put("existing_data_files_count", 0i32);
            record.put("deleted_data_files_count", 0i32);
            record.put("added_rows_count", 1000i64);
            record.put("existing_rows_count", 0i64);
            record.put("deleted_rows_count", 0i64);
            record.put(
                "partitions",
                apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
            );
            writer.append(record).unwrap();
        }
        Bytes::from(writer.into_inner().unwrap())
    }

    fn one_field_schema() -> Schema {
        Schema {
            schema_id: 0,
            identifier_field_ids: vec![1],
            fields: vec![SchemaField {
                id: 1,
                name: "ID".to_string(),
                required: true,
                field_type: serde_json::json!("long"),
                doc: None,
            }],
        }
    }

    /// A single-snapshot table whose current snapshot points at `list_path` and
    /// carries `summary`.
    fn metadata(list_path: &str, summary: &[(&str, &str)]) -> TableMetadata {
        TableMetadata {
            format_version: 2,
            table_uuid: None,
            location: "s3://bucket/dw/fact_orders".to_string(),
            last_sequence_number: 1,
            last_updated_ms: 1000,
            last_column_id: 1,
            current_schema_id: 0,
            schemas: vec![one_field_schema()],
            current_snapshot_id: Some(100),
            snapshots: vec![Snapshot {
                snapshot_id: 100,
                parent_snapshot_id: None,
                sequence_number: 1,
                timestamp_ms: 1000,
                manifest_list: Some(list_path.to_string()),
                manifests: None,
                summary: summary
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect::<HashMap<_, _>>(),
                schema_id: Some(0),
            }],
            snapshot_log: vec![],
            default_spec_id: 0,
            partition_specs: vec![],
            last_partition_id: 0,
            sort_orders: vec![],
            default_sort_order_id: 0,
            properties: HashMap::new(),
        }
    }

    // NOTE on the override arm: the "skipped under override" behavior is proven
    // at the guard-function level (mor_guard::delete_manifests_refused_then_
    // allowed_under_override) rather than here, because driving it through the
    // planner would require mutating the shared process env, which races other
    // tests. These planner tests assert the fail-CLOSED default only, so they
    // never touch the env (mor_deletes_allowed() reads false throughout).

    #[tokio::test]
    async fn plan_scan_refuses_when_manifest_list_has_delete_manifest() {
        // Summary omits the delete counters (a "summary lies/omits" snapshot);
        // the belt-and-suspenders manifest-list check must still refuse.
        let list_path = "s3://bucket/dw/fact_orders/metadata/snap.avro";
        let mut mem = MemoryStorage::new();
        mem.add_file(
            list_path,
            build_manifest_list(&[
                ("s3://bucket/dw/fact_orders/metadata/m-data.avro", 0),
                ("s3://bucket/dw/fact_orders/metadata/m-del.avro", 1),
            ]),
        );
        let md = metadata(list_path, &[("total-records", "1000")]);
        let planner = ScanPlanner::new(&mem, &md, ScanConfig::new());

        let err = planner.plan_scan().await.unwrap_err();
        assert!(
            matches!(err, IcebergError::MergeOnReadDeletes(_)),
            "expected fail-closed refusal, got {err:?}"
        );
    }

    #[tokio::test]
    async fn plan_scan_refuses_on_summary_delete_counters() {
        // The cheap zero-I/O arm: the summary carries delete counters, so the
        // planner refuses before the manifest list is even read (the path here
        // is intentionally absent from storage).
        let md = metadata(
            "s3://bucket/dw/fact_orders/metadata/never-read.avro",
            &[("total-position-deletes", "42")],
        );
        let mem = MemoryStorage::new();
        let planner = ScanPlanner::new(&mem, &md, ScanConfig::new());

        let err = planner.plan_scan().await.unwrap_err();
        assert!(
            matches!(err, IcebergError::MergeOnReadDeletes(_)),
            "expected fail-closed refusal from the summary counters, got {err:?}"
        );
    }

    #[tokio::test]
    async fn plan_scan_proceeds_when_no_deletes() {
        // Control: a delete-free snapshot plans normally (guard is a no-op). An
        // empty manifest list (zero manifests) keeps the fixture minimal — the
        // point is that the guard does not fire, not the file count.
        let list_path = "s3://bucket/dw/fact_orders/metadata/snap.avro";
        let mut mem = MemoryStorage::new();
        mem.add_file(list_path, build_manifest_list(&[]));
        let md = metadata(list_path, &[("total-records", "0")]);
        let planner = ScanPlanner::new(&mem, &md, ScanConfig::new());

        let plan = planner
            .plan_scan()
            .await
            .expect("delete-free snapshot must plan");
        assert_eq!(plan.files_selected, 0, "empty manifest list → no files");
    }
}
