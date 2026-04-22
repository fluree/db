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
use crate::manifest::{parse_manifest, parse_manifest_list, DataFile};
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

        // Load manifest list
        let manifest_list_path = snapshot.manifest_list.as_ref().ok_or_else(|| {
            IcebergError::Manifest(
                "Snapshot has no manifest list (v1 format not supported)".to_string(),
            )
        })?;

        let manifest_list_data = self.storage.read(manifest_list_path).await?;
        let manifest_entries = parse_manifest_list(&manifest_list_data)?;

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
            // Skip delete manifests (already filtered by parse_manifest_list)
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
}
