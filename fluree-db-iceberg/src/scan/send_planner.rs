//! Send-safe scan planning for Iceberg tables.
//!
//! This module provides `SendScanPlanner` which mirrors `ScanPlanner` but uses
//! `SendIcebergStorage` for AWS SDK integration where futures must be `Send`.

use std::sync::Arc;

use crate::error::{IcebergError, Result};
use crate::io::SendIcebergStorage;
use crate::manifest::{parse_manifest, parse_manifest_list_with_deletes};
use crate::metadata::{Schema, Snapshot, TableMetadata};
use crate::scan::planner::{
    effective_sequence_number, FileScanTask, IncrementalScanPlan, ScanConfig, ScanPlan,
};
use crate::scan::pruning::can_contain_file;
use tracing::Instrument;

/// Send-safe scan planner for Iceberg tables.
///
/// This is identical to `ScanPlanner` but uses `SendIcebergStorage` instead of
/// `IcebergStorage`, producing `Send` futures for use with tokio::spawn and
/// async_trait without ?Send.
pub struct SendScanPlanner<'a, S: SendIcebergStorage> {
    storage: &'a S,
    metadata: &'a TableMetadata,
    config: ScanConfig,
}

impl<'a, S: SendIcebergStorage> SendScanPlanner<'a, S> {
    /// Create a new send-safe scan planner.
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
    /// Wraps the planning work (manifest-list + manifest reads and file pruning)
    /// in an `iceberg.scan_plan` timing span and records the selected/pruned file
    /// counts and estimated row count on the span once they are known.
    pub async fn plan_scan_for_snapshot(&self, snapshot: &Snapshot) -> Result<ScanPlan> {
        let span = tracing::debug_span!(
            "iceberg.scan_plan",
            files_selected = tracing::field::Empty,
            files_pruned = tracing::field::Empty,
            estimated_row_count = tracing::field::Empty,
        );
        async move {
            let plan = self.plan_scan_for_snapshot_inner(snapshot).await?;
            let span = tracing::Span::current();
            span.record("files_selected", plan.files_selected);
            span.record("files_pruned", plan.files_pruned);
            span.record("estimated_row_count", plan.estimated_row_count);
            Ok(plan)
        }
        .instrument(span)
        .await
    }

    /// Inner planning implementation (see [`Self::plan_scan_for_snapshot`]).
    async fn plan_scan_for_snapshot_inner(&self, snapshot: &Snapshot) -> Result<ScanPlan> {
        // Project against the schema in effect AT `snapshot`, not the current
        // one — for a historical snapshot under schema evolution the two
        // differ, and the name→field-id mapping / type interpretation must
        // follow the pinned snapshot. Identical for `snapshot` = current.
        let schema = self
            .metadata
            .schema_for_snapshot(snapshot)
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
                let eff_seq = effective_sequence_number(
                    entry.sequence_number,
                    manifest_entry.sequence_number,
                );
                let task = FileScanTask::for_whole_file_with_schema(
                    data_file,
                    projected_field_ids.clone(),
                    self.config.filter.clone(),
                    Arc::clone(&schema_arc),
                )
                // A full read carries it too. This is the read a consumer falls
                // back to when its watermark expired, so it is exactly the read
                // that most needs to be resumable.
                .with_data_sequence_number(eff_seq);
                tasks.push(task);
            }
        }

        // Commit order, path as the tiebreak — see the same sort in
        // `plan_incremental` for why this is a contract rather than a nicety.
        tasks.sort_by(|a, b| {
            a.data_sequence_number
                .cmp(&b.data_sequence_number)
                .then_with(|| a.data_file.file_path.cmp(&b.data_file.file_path))
        });

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

    /// Plan a full scan for a snapshot chosen by `selection` (current / by id /
    /// as-of-time) rather than always the current snapshot.
    pub async fn plan_scan_with_selection(
        &self,
        selection: &crate::metadata::SnapshotSelection,
    ) -> Result<ScanPlan> {
        let snapshot =
            crate::metadata::select_snapshot(self.metadata, selection).ok_or_else(|| {
                IcebergError::SnapshotNotFound("No snapshot matches selection".to_string())
            })?;
        self.plan_scan_for_snapshot(snapshot).await
    }

    /// Plan an **incremental (append-only)** scan: the data files ADDED in the
    /// window `(from_snapshot_id, to_snapshot_id]`.
    ///
    /// A file is "added in the window" when its *effective data sequence number*
    /// (see [`effective_sequence_number`]) is strictly greater than the `from`
    /// snapshot's sequence number. `from_snapshot_id = None` means "since genesis"
    /// (the full live state of `to`) — used for the initial materialization.
    ///
    /// This captures **additions only**. The caller MUST first verify the window
    /// is append-only via [`TableMetadata::window_is_append_only`](crate::metadata::TableMetadata::window_is_append_only)
    /// and fall back to a full re-read otherwise — overwrite/delete/replace
    /// snapshots carry updates/deletions this scan cannot see.
    pub async fn plan_incremental(
        &self,
        from_snapshot_id: Option<i64>,
        to_snapshot_id: i64,
    ) -> Result<IncrementalScanPlan> {
        let to_snapshot = self.metadata.snapshot(to_snapshot_id).ok_or_else(|| {
            IcebergError::SnapshotNotFound(format!("to snapshot {to_snapshot_id} not found"))
        })?;
        let from_seq = match from_snapshot_id {
            Some(id) => {
                self.metadata
                    .snapshot(id)
                    .ok_or_else(|| {
                        IcebergError::SnapshotNotFound(format!("from snapshot {id} not found"))
                    })?
                    .sequence_number
            }
            None => 0,
        };
        let to_seq = to_snapshot.sequence_number;

        // Schema at the `to` snapshot, not current — see the same choice in
        // `plan_scan_for_snapshot_inner`. The whole window's files are read
        // under `to`'s schema: files added earlier in the window were written
        // under `to`'s schema or an ancestor of it, and Iceberg field ids are
        // stable across evolution, so `to`'s name→id mapping is correct for
        // every file the window can select.
        let schema = self
            .metadata
            .schema_for_snapshot(to_snapshot)
            .ok_or_else(|| IcebergError::Metadata("No current schema".to_string()))?;
        let schema_arc = Arc::new(schema.clone());
        let (projected_field_ids, projected_columns) = self.resolve_projection(schema)?;

        let manifest_list_path = to_snapshot.manifest_list.as_ref().ok_or_else(|| {
            IcebergError::Manifest(
                "Snapshot has no manifest list (v1 format not supported)".to_string(),
            )
        })?;
        // Fail closed on merge-on-read delete files, exactly as `plan()` does.
        //
        // WHY THIS MATTERS MORE HERE THAN ON A QUERY. A query that reads a MoR table
        // without applying its delete files returns deleted rows once, to one caller,
        // who can re-run it. Materialization writes those rows into a ledger as
        // committed state and then advances the watermark PAST the window — so nothing
        // ever re-reads it. A transient wrong answer becomes permanent wrong data.
        //
        // The `from_snapshot_id = None` case is the concrete hole: it means "since
        // genesis", i.e. a full read of `to`'s live state — precisely the read `plan()`
        // refuses on a delete-bearing snapshot. It is also the initial-materialization
        // path, so the very first build of a twin was the least guarded.
        //
        // `window_is_append_only` is a CALLER contract (see this method's docs) and is
        // not enforced here, so it cannot be relied on as the guard.
        let allow_mor = crate::mor_guard::mor_deletes_allowed();
        crate::mor_guard::ensure_no_summary_deletes(
            to_snapshot,
            &self.metadata.location,
            allow_mor,
        )?;

        let manifest_list_data = self.storage.read(manifest_list_path).await?;
        // Parse WITH delete manifests so the backstop can SEE them. Previously this
        // used `parse_manifest_list`, which filters `content=1` entries out — which in
        // turn made the `is_deletes()` skip inside the loop below DEAD CODE: it read as
        // a delete-file safety check while being unreachable. Now the entries arrive,
        // the guard counts them, and the skip does the job its name claims.
        let manifest_entries = parse_manifest_list_with_deletes(&manifest_list_data, true)?;
        let delete_manifests = manifest_entries.iter().filter(|e| e.is_deletes()).count();
        crate::mor_guard::ensure_no_delete_manifests(
            delete_manifests,
            &self.metadata.location,
            allow_mor,
        )?;

        let mut added_tasks = Vec::new();
        let mut files_selected = 0;
        let mut estimated_row_count = 0i64;

        for manifest_entry in &manifest_entries {
            if manifest_entry.is_deletes() {
                continue; // append-only: data manifests only
            }
            // A manifest written at or before `from` cannot hold files newer than
            // `from` (every file it lists has data-seq <= the manifest's seq).
            if manifest_entry.sequence_number <= from_seq {
                continue;
            }

            let manifest_data = self.storage.read(&manifest_entry.manifest_path).await?;
            let data_file_entries = parse_manifest(&manifest_data)?;

            for entry in data_file_entries {
                let eff_seq = effective_sequence_number(
                    entry.sequence_number,
                    manifest_entry.sequence_number,
                );
                if eff_seq <= from_seq {
                    continue; // present at/before `from`
                }

                let data_file = entry.data_file;
                if let Some(filter) = &self.config.filter {
                    if !can_contain_file(filter, &data_file, schema) {
                        continue;
                    }
                }

                files_selected += 1;
                estimated_row_count += data_file.record_count;
                added_tasks.push(
                    FileScanTask::for_whole_file_with_schema(
                        data_file,
                        projected_field_ids.clone(),
                        self.config.filter.clone(),
                        Arc::clone(&schema_arc),
                    )
                    // `eff_seq` is already the value this loop filters on; carrying
                    // it lets a consumer order or checkpoint by commit without
                    // re-deriving it from manifests it no longer holds.
                    .with_data_sequence_number(eff_seq),
                );
            }
        }

        // Commit order, with the path as a deterministic tiebreak inside a commit.
        //
        // Manifest traversal is already sequential, so this order is stable today
        // — but only incidentally. A consumer that stops part-way through a
        // backlog and resumes needs the order to be a CONTRACT, because resuming
        // against a different order either repeats files or skips them. Sorting
        // makes it one, and costs a sort over file metadata rather than data.
        //
        // Sequence number first, not path first: an older row overwriting a newer
        // one is the failure mode that matters here, and processing commits in
        // order is what prevents it.
        added_tasks.sort_by(|a, b| {
            a.data_sequence_number
                .cmp(&b.data_sequence_number)
                .then_with(|| a.data_file.file_path.cmp(&b.data_file.file_path))
        });

        tracing::info!(
            ?from_snapshot_id,
            to_snapshot_id,
            from_seq,
            to_seq,
            files_selected,
            estimated_row_count,
            "Incremental scan planning complete (append-only)"
        );

        Ok(IncrementalScanPlan {
            added_tasks,
            projected_columns,
            projected_field_ids,
            from_sequence_number: from_seq,
            to_snapshot_id,
            to_sequence_number: to_seq,
            files_selected,
            estimated_row_count,
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

/// The production COUNT/scan path routes through `SendScanPlanner` (r2rml
/// `scan_table` → `SendScanPlanner::plan_scan`), so the fail-closed merge-on-read
/// guard must fire here too — a COUNT over a delete-bearing snapshot refuses
/// rather than returning an over-count (audit F-AUD-1, the table_row_count path).
/// The runtime-agnostic `ScanPlanner` is covered separately; this proves the
/// Send variant (which shares the guard helpers) inherits it.
#[cfg(test)]
mod mor_guard_tests {
    use super::*;
    use crate::metadata::SchemaField;
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::ops::Range;

    /// A `SendIcebergStorage` that errors on every read. The zero-I/O summary
    /// guard must refuse a delete-bearing snapshot BEFORE any manifest-list read,
    /// so nothing here should ever be called.
    #[derive(Debug)]
    struct NoReadStorage;

    #[async_trait]
    impl SendIcebergStorage for NoReadStorage {
        async fn read(&self, _path: &str) -> Result<Bytes> {
            Err(IcebergError::storage(
                "planner must not read: the summary guard should refuse first",
            ))
        }
        async fn read_range(&self, _path: &str, _range: Range<u64>) -> Result<Bytes> {
            Err(IcebergError::storage("planner must not read"))
        }
        async fn file_size(&self, _path: &str) -> Result<u64> {
            Err(IcebergError::storage("planner must not read"))
        }
    }

    fn metadata_with_summary(summary: &[(&str, &str)]) -> TableMetadata {
        TableMetadata {
            format_version: 2,
            table_uuid: None,
            location: "s3://bucket/dw/fact_orders".to_string(),
            last_sequence_number: 1,
            last_updated_ms: 1000,
            last_column_id: 1,
            current_schema_id: 0,
            schemas: vec![Schema {
                schema_id: 0,
                identifier_field_ids: vec![1],
                fields: vec![SchemaField {
                    id: 1,
                    name: "ID".to_string(),
                    required: true,
                    field_type: serde_json::json!("long"),
                    doc: None,
                }],
            }],
            current_snapshot_id: Some(100),
            snapshots: vec![Snapshot {
                snapshot_id: 100,
                parent_snapshot_id: None,
                sequence_number: 1,
                timestamp_ms: 1000,
                manifest_list: Some(
                    "s3://bucket/dw/fact_orders/metadata/never-read.avro".to_string(),
                ),
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

    #[tokio::test]
    async fn send_planner_refuses_delete_bearing_snapshot() {
        let md = metadata_with_summary(&[("total-position-deletes", "17")]);
        let storage = NoReadStorage;
        let planner = SendScanPlanner::new(&storage, &md, ScanConfig::new());
        let err = planner
            .plan_scan()
            .await
            .expect_err("the Send planner (production COUNT/scan path) must fail closed");
        assert!(
            matches!(err, IcebergError::MergeOnReadDeletes(_)),
            "expected a merge-on-read refusal, got {err:?}"
        );
    }

    /// B1. `plan_incremental` is the MATERIALIZE path, and it bypassed this guard
    /// entirely: no `mor_guard` call, and it parsed with `parse_manifest_list`, which
    /// filters `content=1` entries out — so it could not even see delete manifests.
    ///
    /// The stakes are higher here than on `plan_scan`. A query returns deleted rows
    /// once, to one caller, who can re-run it. Materialization writes them into a
    /// ledger as committed state and then advances the watermark PAST the window, so
    /// nothing ever re-reads it: a transient wrong answer becomes permanent wrong data.
    ///
    /// `NoReadStorage` errors on every read, so this also pins that the refusal happens
    /// BEFORE any manifest-list I/O — a guard that only fires after fetching the
    /// manifest list would still be correct, but not zero-cost on the hot poll path.
    #[tokio::test]
    async fn send_planner_refuses_delete_bearing_snapshot_incrementally() {
        let md = metadata_with_summary(&[("total-position-deletes", "17")]);
        let storage = NoReadStorage;
        let planner = SendScanPlanner::new(&storage, &md, ScanConfig::new());
        let err = planner
            .plan_incremental(Some(100), 100)
            .await
            .expect_err("the materialize path must fail closed on merge-on-read deletes");
        assert!(
            matches!(err, IcebergError::MergeOnReadDeletes(_)),
            "expected a merge-on-read refusal, got {err:?}"
        );
    }

    /// The `from = None` case specifically, because it is the worst one: it means
    /// "since genesis", i.e. a FULL read of the snapshot's live state — exactly the
    /// read `plan_scan` refuses — and it is the initial-materialization path, so the
    /// very first build of a twin was the least guarded thing in the pipeline.
    #[tokio::test]
    async fn initial_materialization_from_genesis_also_refuses() {
        let md = metadata_with_summary(&[("total-equality-deletes", "3")]);
        let storage = NoReadStorage;
        let planner = SendScanPlanner::new(&storage, &md, ScanConfig::new());
        let err = planner
            .plan_incremental(None, 100)
            .await
            .expect_err("a from-genesis incremental read is a full live-state read");
        assert!(
            matches!(err, IcebergError::MergeOnReadDeletes(_)),
            "expected a merge-on-read refusal, got {err:?}"
        );
    }

    /// The guard must not fire on a clean snapshot — otherwise it would break every
    /// copy-on-write table, which is the common case. Reaching storage (and failing
    /// there, on `NoReadStorage`) is the proof that the guard let it through.
    #[tokio::test]
    async fn a_clean_snapshot_is_not_refused_incrementally() {
        let md = metadata_with_summary(&[("total-records", "1000"), ("added-records", "10")]);
        let storage = NoReadStorage;
        let planner = SendScanPlanner::new(&storage, &md, ScanConfig::new());
        let err = planner
            .plan_incremental(Some(100), 100)
            .await
            .expect_err("NoReadStorage fails the manifest-list read");
        assert!(
            !matches!(err, IcebergError::MergeOnReadDeletes(_)),
            "a delete-free snapshot must NOT be refused as merge-on-read; got {err:?}"
        );
    }
}
