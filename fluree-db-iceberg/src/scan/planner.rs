//! Scan planning for Iceberg tables.
//!
//! Every planner builds a plan in the same two steps:
//! 1. Read the snapshot's manifest list, then its data manifests (concurrently,
//!    in manifest-list order; see [`crate::manifest::reader`]), into the
//!    snapshot's live data files, sorted into plan order
//!    ([`sort_into_plan_order`]).
//! 2. Keep the files the filter may match, judged by each file's column bounds
//!    and null counts ([`select_files`]), as `FileScanTask`s carrying the
//!    projection, the schema and the filter as their residual.
//!
//! Step 2 needs only the file list, so a caller that kept a snapshot's file
//! list can plan any filter against it without reading a manifest
//! (`SendScanPlanner::plan_from_files`).
//!
//! Every manifest of the snapshot is read: manifests are not skipped by their
//! partition summaries, and partition values are not used to prune files.

use std::borrow::Cow;
use std::sync::Arc;

use futures::{Stream, StreamExt};

use crate::error::{IcebergError, Result};
use crate::io::IcebergStorage;
use crate::manifest::{
    parse_manifest_list_with_deletes, read_manifests, DataFile, ManifestEntry, ManifestListEntry,
    DEFAULT_MANIFEST_READ_CONCURRENCY,
};
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
    /// How many manifests a plan reads at once (default
    /// [`DEFAULT_MANIFEST_READ_CONCURRENCY`]; 0 is treated as 1).
    pub manifest_read_concurrency: usize,
}

impl Default for ScanConfig {
    fn default() -> Self {
        Self {
            projection: None,
            filter: None,
            batch_row_limit: 1024,
            batch_byte_budget: None,
            manifest_read_concurrency: DEFAULT_MANIFEST_READ_CONCURRENCY,
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

    /// Set how many manifests a plan reads at once.
    pub fn with_manifest_read_concurrency(mut self, concurrency: usize) -> Self {
        self.manifest_read_concurrency = concurrency;
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
    /// Effective data sequence number of the file, when the planner knew it.
    ///
    /// Iceberg assigns this per commit and never reuses or reorders it, so it is
    /// the only value on a task that says *when* the file's rows entered the
    /// table. A consumer that must process files in commit order — or stop
    /// part-way through a backlog and resume without going backwards — has
    /// nothing else to sort or checkpoint on: paths are arbitrary, and manifest
    /// iteration order is an implementation detail rather than a contract.
    ///
    /// `None` where the planner cannot attribute one (a whole-table read built
    /// outside manifest traversal, and the test helpers). Consumers that need
    /// ordering must treat `None` as "unknown", not as zero.
    pub data_sequence_number: Option<i64>,
}

impl FileScanTask {
    /// Attach the file's effective data sequence number.
    #[must_use]
    pub fn with_data_sequence_number(mut self, seq: i64) -> Self {
        self.data_sequence_number = Some(seq);
        self
    }

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
            data_sequence_number: None,
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
            data_sequence_number: None,
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

/// The schema a plan of `snapshot` resolves its projection through, tests
/// files against, and attaches to its tasks: the snapshot's schema under
/// current column names ([`TableMetadata::read_schema_for_snapshot`]).
///
/// Every plan uses this one schema, however it was built (from the manifests,
/// from a kept file list, incrementally), because a task's schema names the
/// columns its batches carry: a plan under the snapshot's old names would
/// hide a column renamed since from a caller that names it as it is now.
/// Field ids and types are the snapshot's, so file pruning is unaffected.
pub(crate) fn plan_schema<'m>(
    metadata: &'m TableMetadata,
    snapshot: &Snapshot,
) -> Result<Cow<'m, Schema>> {
    metadata
        .read_schema_for_snapshot(snapshot)
        .ok_or_else(|| IcebergError::Metadata("No current schema".to_string()))
}

/// Resolve a projection (field ids, or `None` for every non-nested field) to
/// field ids and column names under `schema`.
pub(crate) fn resolve_projection(
    projection: Option<&[i32]>,
    schema: &Schema,
) -> Result<(Vec<i32>, Vec<String>)> {
    match projection {
        Some(field_ids) => {
            let mut names = Vec::with_capacity(field_ids.len());
            for &id in field_ids {
                let field = schema.field(id).ok_or_else(|| {
                    IcebergError::Scan(format!("Field ID {id} not found in schema"))
                })?;
                names.push(field.name.clone());
            }
            Ok((field_ids.to_vec(), names))
        }
        None => Ok(schema
            .fields
            .iter()
            .filter(|f| !f.is_nested())
            .map(|f| (f.id, f.name.clone()))
            .unzip()),
    }
}

/// Collect the data files of the manifests `manifests` yields (from
/// [`read_manifests`]), each with its effective data sequence number, in
/// manifest order.
///
/// The caller picks the manifests (data manifests only; an incremental plan
/// also drops those older than its window). The reader yields them in
/// manifest-list order whatever order its reads finish in, so the result and
/// the first error reported are those of a sequential walk.
pub(crate) async fn read_data_files<'m>(
    manifests: impl Stream<Item = Result<(&'m ManifestListEntry, Vec<ManifestEntry>)>>,
) -> Result<Vec<(DataFile, i64)>> {
    let mut files = Vec::new();
    let mut manifests = std::pin::pin!(manifests);
    while let Some(next) = manifests.next().await {
        let (manifest, entries) = next?;
        files.extend(entries.into_iter().map(|entry| {
            let seq = effective_sequence_number(entry.sequence_number, manifest.sequence_number);
            (entry.data_file, seq)
        }));
    }
    Ok(files)
}

/// Sort data files into plan order: commit order (effective data sequence
/// number), with the path as a deterministic tiebreak inside a commit.
///
/// Every full plan is in this order, and so is a file list a caller keeps, so
/// selecting from a kept list yields what a fresh plan would. See
/// `SendScanPlanner::plan_incremental` for why the order is a contract.
pub(crate) fn sort_into_plan_order(files: &mut [(DataFile, i64)]) {
    files.sort_by(|a, b| {
        a.1.cmp(&b.1)
            .then_with(|| a.0.file_path.cmp(&b.0.file_path))
    });
}

/// A file a plan selects from: borrowed from a kept file list (cloned only if
/// selected, and carrying no sequence number) or owned from a manifest read
/// (moved into its task, with its sequence number).
///
/// A trait rather than `Cow`: an item is then a reference or the file itself,
/// never a `DataFile`-sized enum moved through the loop for every file, which
/// on a warm scan of a large kept list is most of the selection's cost.
pub(crate) trait PlanFile {
    /// The file's statistics, for the filter test.
    fn data_file(&self) -> &DataFile;
    /// The file's task: the whole file under `schema` and `projection`, with
    /// `residual` as its row filter and the file's data sequence number when
    /// known.
    fn into_task(
        self,
        projection: &[i32],
        residual: Option<&Expression>,
        schema: &Arc<Schema>,
    ) -> FileScanTask;
}

impl PlanFile for &DataFile {
    fn data_file(&self) -> &DataFile {
        self
    }
    #[inline]
    fn into_task(
        self,
        projection: &[i32],
        residual: Option<&Expression>,
        schema: &Arc<Schema>,
    ) -> FileScanTask {
        FileScanTask::for_whole_file_with_schema(
            self.clone(),
            projection.to_vec(),
            residual.cloned(),
            Arc::clone(schema),
        )
    }
}

impl PlanFile for (DataFile, i64) {
    fn data_file(&self) -> &DataFile {
        &self.0
    }
    #[inline]
    fn into_task(
        self,
        projection: &[i32],
        residual: Option<&Expression>,
        schema: &Arc<Schema>,
    ) -> FileScanTask {
        let mut task = FileScanTask::for_whole_file_with_schema(
            self.0,
            projection.to_vec(),
            residual.cloned(),
            Arc::clone(schema),
        );
        task.data_sequence_number = Some(self.1);
        task
    }
}

impl PlanFile for &(DataFile, i64) {
    fn data_file(&self) -> &DataFile {
        &self.0
    }
    #[inline]
    fn into_task(
        self,
        projection: &[i32],
        residual: Option<&Expression>,
        schema: &Arc<Schema>,
    ) -> FileScanTask {
        let mut task = FileScanTask::for_whole_file_with_schema(
            self.0.clone(),
            projection.to_vec(),
            residual.cloned(),
            Arc::clone(schema),
        );
        task.data_sequence_number = Some(self.1);
        task
    }
}

/// The selection step every plan shares: keep the files `filter` may match
/// (every file without a filter), in the order given, as tasks carrying the
/// projection, the schema, the filter as their residual, and the file's data
/// sequence number where it is known.
///
/// `files` must already be in plan order ([`sort_into_plan_order`]); selection
/// only drops files, so the plan keeps that order. A borrowed file is cloned
/// only if it is selected. Without a filter no file is tested, the tasks are
/// built in one exact-size pass, and `known_record_count` (the files' total,
/// when the caller kept it) is the estimate instead of a per-file sum: an
/// unfiltered plan from a kept list then costs what copying the list does.
pub(crate) fn select_files<F: PlanFile>(
    files: impl IntoIterator<Item = F>,
    filter: Option<&Expression>,
    schema: &Schema,
    projected_field_ids: Vec<i32>,
    projected_columns: Vec<String>,
    has_delete_manifests: bool,
    known_record_count: Option<i64>,
) -> ScanPlan {
    let schema_arc = Arc::new(schema.clone());
    let mut files_pruned = 0;
    let mut estimated_row_count = 0i64;
    let ids = &projected_field_ids;
    let tasks: Vec<FileScanTask> = match (filter, known_record_count) {
        (None, Some(total)) => {
            estimated_row_count = total;
            files
                .into_iter()
                .map(|file| file.into_task(ids, None, &schema_arc))
                .collect()
        }
        (None, None) => files
            .into_iter()
            .map(|file| {
                estimated_row_count += file.data_file().record_count;
                file.into_task(ids, None, &schema_arc)
            })
            .collect(),
        (Some(filter), _) => files
            .into_iter()
            .filter(|file| {
                let keep = can_contain_file(filter, file.data_file(), schema);
                files_pruned += usize::from(!keep);
                keep
            })
            .map(|file| {
                estimated_row_count += file.data_file().record_count;
                file.into_task(ids, Some(filter), &schema_arc)
            })
            .collect(),
    };
    ScanPlan {
        files_selected: tasks.len(),
        tasks,
        projected_columns,
        projected_field_ids,
        residual_filter: filter.cloned(),
        estimated_row_count,
        files_pruned,
        has_delete_manifests,
    }
}

/// A snapshot's complete live data-file list, in plan order, as a plan read
/// it from the manifests (`SendScanPlanner::plan_scan_for_snapshot_retaining_files`),
/// with what a plan from it needs besides the files. What
/// `SendScanPlanner::plan_from_files` plans from.
#[derive(Debug, Clone, Copy)]
pub struct KeptFiles<'f> {
    /// Every live data file of the snapshot, in plan order.
    pub files: &'f [DataFile],
    /// The sum of the files' record counts (the unfiltered plan's estimate).
    pub record_count: i64,
    /// Whether the snapshot carries merge-on-read delete files, as the plan
    /// that read the list reported (only ever `true` under the override).
    pub has_delete_manifests: bool,
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
    ///
    /// The same plan as `SendScanPlanner::plan_scan_for_snapshot`, through the
    /// same shared steps; keep the two in sync.
    pub async fn plan_scan_for_snapshot(&self, snapshot: &Snapshot) -> Result<ScanPlan> {
        // Schema at `snapshot` under current names.
        let schema = plan_schema(self.metadata, snapshot)?;
        let (projected_field_ids, projected_columns) =
            resolve_projection(self.config.projection.as_deref(), &schema)?;

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

        // Delete manifests are skipped. Under the guard's default they never
        // reach here (refused above); under the override they are ignored (the
        // documented, pre-guard behavior).
        let mut files = read_data_files(read_manifests(
            manifest_entries.iter().filter(|m| !m.is_deletes()),
            self.config.manifest_read_concurrency,
            |path| self.storage.read(path),
        ))
        .await?;
        sort_into_plan_order(&mut files);
        // Every task carries its commit sequence, and the plan is in commit
        // order, so a consumer can checkpoint or resume by sequence from either
        // planner.
        let plan = select_files(
            files,
            self.config.filter.as_ref(),
            &schema,
            projected_field_ids,
            projected_columns,
            has_delete_manifests,
            None,
        );

        tracing::info!(
            files_selected = plan.files_selected,
            files_pruned = plan.files_pruned,
            estimated_row_count = plan.estimated_row_count,
            "Scan planning complete"
        );
        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The sort a resumable consumer depends on: commit order first, path as a
    /// deterministic tiebreak inside one commit. This is the sort every plan
    /// (and every kept file list) goes through.
    #[test]
    fn tasks_order_by_commit_then_path() {
        fn file(path: &str, seq: i64) -> (DataFile, i64) {
            let df = crate::manifest::DataFile {
                file_path: path.to_string(),
                file_format: crate::manifest::FileFormat::Parquet,
                record_count: 1,
                file_size_in_bytes: 1,
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
            (df, seq)
        }

        let mut files = [
            file("z-early.parquet", 1),
            file("a-late.parquet", 9),
            file("m-mid-b.parquet", 5),
            file("m-mid-a.parquet", 5),
        ];
        sort_into_plan_order(&mut files);

        let order: Vec<&str> = files.iter().map(|f| f.0.file_path.as_str()).collect();
        assert_eq!(
            order,
            vec![
                "z-early.parquet", // seq 1 — earliest commit, despite sorting last by path
                "m-mid-a.parquet", // seq 5, path tiebreak
                "m-mid-b.parquet",
                "a-late.parquet", // seq 9 — latest commit, despite sorting first by path
            ],
            "commit order must outrank path order: sorting by path alone would \
             process a later commit before an earlier one, which lets an older \
             row overwrite a newer one across a resume boundary"
        );
    }

    /// `None` must not be treated as sequence zero. An unattributed file sorting
    /// first would place it before every real commit.
    #[test]
    fn an_unknown_sequence_number_is_distinct_from_zero() {
        assert_ne!(
            Some(0i64),
            Option::<i64>::None,
            "a task with no attributable sequence number is 'unknown', not 'oldest'"
        );
        // And the helper never yields None: a missing entry seq inherits the
        // manifest's, which is what makes the sort total in practice.
        assert_eq!(effective_sequence_number(None, 7), 7);
        assert_eq!(effective_sequence_number(Some(0), 7), 7);
        assert_eq!(effective_sequence_number(Some(3), 7), 3);
    }

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
