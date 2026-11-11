//! Send-safe scan planning for Iceberg tables.
//!
//! This module provides `SendScanPlanner` which mirrors `ScanPlanner` but uses
//! `SendIcebergStorage` for AWS SDK integration where futures must be `Send`.
//! Both planners share the read, sort and select steps in
//! [`crate::scan::planner`].

use std::borrow::Cow;

use crate::error::{IcebergError, Result};
use crate::io::SendIcebergStorage;
use crate::manifest::{parse_manifest_list_with_deletes, send_read_manifests, DataFile};
use crate::metadata::{Schema, Snapshot, TableMetadata};
use crate::scan::planner::{
    plan_schema, read_data_files, resolve_projection, select_files, sort_into_plan_order,
    IncrementalScanPlan, KeptFiles, ScanConfig, ScanPlan,
};
use tracing::Instrument;

/// The `iceberg.scan_plan` span every full plan runs in, whether it read the
/// manifests or selected from a kept file list. Its counters are recorded
/// once the plan is known ([`finish_plan`]).
fn scan_plan_span() -> tracing::Span {
    tracing::debug_span!(
        "iceberg.scan_plan",
        files_selected = tracing::field::Empty,
        files_pruned = tracing::field::Empty,
        estimated_row_count = tracing::field::Empty,
    )
}

/// Report a finished plan: its counters on the `iceberg.scan_plan` span and
/// the "Scan planning complete" log line. Every planning arm (manifests read,
/// list kept, list reused) ends here, so they all report alike.
fn finish_plan(span: &tracing::Span, plan: &ScanPlan) {
    span.record("files_selected", plan.files_selected);
    span.record("files_pruned", plan.files_pruned);
    span.record("estimated_row_count", plan.estimated_row_count);
    tracing::info!(
        files_selected = plan.files_selected,
        files_pruned = plan.files_pruned,
        estimated_row_count = plan.estimated_row_count,
        "Scan planning complete"
    );
}

/// A snapshot's live data files, read from its manifests, in plan order.
struct SnapshotFiles {
    /// Each file with its effective data sequence number.
    files: Vec<(DataFile, i64)>,
    /// Whether the snapshot carries merge-on-read deletes (only ever `true`
    /// when the fail-closed guard was overridden).
    has_delete_manifests: bool,
}

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
        let span = scan_plan_span();
        let plan = async {
            let (schema, projected_field_ids, projected_columns) = self.projection(snapshot)?;
            let read = self.read_snapshot_files(snapshot).await?;
            // A full read carries each file's commit sequence too. This is the
            // read a consumer falls back to when its watermark expired, so it
            // is exactly the read that most needs to be resumable.
            Ok::<_, IcebergError>(select_files(
                read.files,
                self.config.filter.as_ref(),
                &schema,
                projected_field_ids,
                projected_columns,
                read.has_delete_manifests,
                None,
            ))
        }
        .instrument(span.clone())
        .await?;
        finish_plan(&span, &plan);
        Ok(plan)
    }

    /// Plan a scan for `snapshot` as [`Self::plan_scan_for_snapshot`] does, and
    /// also return every live data file of the snapshot, in plan order, so the
    /// caller can keep the list and plan later scans of the snapshot from it
    /// with [`Self::plan_from_files`].
    ///
    /// The manifests are read once; the plan selects from the list (cloning
    /// only the files it selects) under one `iceberg.scan_plan` span, which
    /// records the plan's counters (the filter's, when there is one).
    pub async fn plan_scan_for_snapshot_retaining_files(
        &self,
        snapshot: &Snapshot,
    ) -> Result<(ScanPlan, Vec<DataFile>)> {
        let span = scan_plan_span();
        let (plan, files) = async {
            let (schema, projected_field_ids, projected_columns) = self.projection(snapshot)?;
            let read = self.read_snapshot_files(snapshot).await?;
            let plan = select_files(
                &read.files,
                self.config.filter.as_ref(),
                &schema,
                projected_field_ids,
                projected_columns,
                read.has_delete_manifests,
                None,
            );
            let files = read.files.into_iter().map(|(file, _)| file).collect();
            Ok::<_, IcebergError>((plan, files))
        }
        .instrument(span.clone())
        .await?;
        finish_plan(&span, &plan);
        Ok((plan, files))
    }

    /// Plan a scan for `snapshot` from its live data files as a caller kept
    /// them (from [`Self::plan_scan_for_snapshot_retaining_files`]), reading
    /// nothing.
    ///
    /// `kept` must describe the snapshot's complete live file list in plan
    /// order, its record total and the delete flag the plan that read it
    /// reported. The plan is the one [`Self::plan_scan_for_snapshot`] would build, with the
    /// same files in the same order, the same residual filter, the same merge-
    /// on-read refusal, and the same `iceberg.scan_plan` span and counters,
    /// except that its tasks carry no data sequence number: a kept list does
    /// not hold one (the full-scan reader never reads it; a reader that needs
    /// commit order must plan from the manifests).
    pub fn plan_from_files(&self, snapshot: &Snapshot, kept: KeptFiles<'_>) -> Result<ScanPlan> {
        let span = scan_plan_span();
        let plan = span.in_scope(|| {
            let (schema, projected_field_ids, projected_columns) = self.projection(snapshot)?;
            // The plan that read `files` refused a delete-bearing snapshot unless
            // the override was on; re-decide now, so a list kept under the
            // override is refused once it is off.
            let allow_mor = crate::mor_guard::mor_deletes_allowed();
            crate::mor_guard::ensure_no_summary_deletes(
                snapshot,
                &self.metadata.location,
                allow_mor,
            )?;
            crate::mor_guard::ensure_no_delete_manifests(
                usize::from(kept.has_delete_manifests),
                &self.metadata.location,
                allow_mor,
            )?;
            Ok::<_, IcebergError>(select_files(
                kept.files,
                self.config.filter.as_ref(),
                &schema,
                projected_field_ids,
                projected_columns,
                kept.has_delete_manifests,
                Some(kept.record_count),
            ))
        })?;
        finish_plan(&span, &plan);
        Ok(plan)
    }

    /// The schema a plan of `snapshot` reads through, and the projection
    /// resolved under it. Resolved before any read, so a bad projection is
    /// reported without touching storage.
    fn projection(&self, snapshot: &Snapshot) -> Result<(Cow<'a, Schema>, Vec<i32>, Vec<String>)> {
        // Project against the schema in effect AT `snapshot` (its field ids
        // and types), under current column names: for a historical snapshot
        // under schema evolution the type interpretation must follow the
        // pinned snapshot, while callers name columns as they are now (see
        // `plan_schema`). Identical for `snapshot` = current.
        let schema = plan_schema(self.metadata, snapshot)?;
        let (ids, names) = resolve_projection(self.config.projection.as_deref(), &schema)?;
        Ok((schema, ids, names))
    }

    /// Read `snapshot`'s manifest list and data manifests into its live data
    /// files, in plan order, after the merge-on-read guards.
    async fn read_snapshot_files(&self, snapshot: &Snapshot) -> Result<SnapshotFiles> {
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
        let mut files = read_data_files(send_read_manifests(
            self.storage,
            manifest_entries.iter().filter(|m| !m.is_deletes()),
            self.config.manifest_read_concurrency,
        ))
        .await?;
        // Commit order, path as the tiebreak — see the same sort in
        // `plan_incremental` for why this is a contract rather than a nicety.
        sort_into_plan_order(&mut files);
        Ok(SnapshotFiles {
            files,
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
    /// (see [`effective_sequence_number`](crate::scan::planner::effective_sequence_number))
    /// is strictly greater than the `from` snapshot's sequence number.
    /// `from_snapshot_id = None` means "since genesis" (the full live state of
    /// `to`) — used for the initial materialization.
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

        // Schema at the `to` snapshot under current names — see the same choice
        // in `projection`. The whole window's files are read under `to`'s schema:
        // files added earlier in the window were written under `to`'s schema or
        // an ancestor of it, and Iceberg field ids are stable across evolution,
        // so `to`'s name→id mapping is correct for every file the window can
        // select.
        let (schema, projected_field_ids, projected_columns) = self.projection(to_snapshot)?;

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
        // turn made the `is_deletes()` skip below DEAD CODE: it read as a delete-file
        // safety check while being unreachable. Now the entries arrive, the guard
        // counts them, and the skip does the job its name claims.
        let manifest_entries = parse_manifest_list_with_deletes(&manifest_list_data, true)?;
        let delete_manifests = manifest_entries.iter().filter(|e| e.is_deletes()).count();
        crate::mor_guard::ensure_no_delete_manifests(
            delete_manifests,
            &self.metadata.location,
            allow_mor,
        )?;

        // Data manifests only (append-only). A manifest written at or before
        // `from` cannot hold files newer than `from` (every file it lists has
        // data-seq <= the manifest's seq), so it is not read at all.
        let mut files = read_data_files(send_read_manifests(
            self.storage,
            manifest_entries
                .iter()
                .filter(move |m| !m.is_deletes() && m.sequence_number > from_seq),
            self.config.manifest_read_concurrency,
        ))
        .await?;
        // A file present at or before `from` is not an addition.
        files.retain(|(_, seq)| *seq > from_seq);

        // Commit order, with the path as a deterministic tiebreak inside a commit.
        //
        // Manifests are read concurrently but yielded in manifest-list order, so
        // the pre-sort order is stable today — but only incidentally. A consumer
        // that stops part-way through a backlog and resumes needs the order to be
        // a CONTRACT, because resuming against a different order either repeats
        // files or skips them. Sorting makes it one, and costs a sort over file
        // metadata rather than data.
        //
        // Sequence number first, not path first: an older row overwriting a newer
        // one is the failure mode that matters here, and processing commits in
        // order is what prevents it.
        sort_into_plan_order(&mut files);
        // Each task carries `eff_seq`, the value the window filters on, so a
        // consumer can order or checkpoint by commit without re-deriving it
        // from manifests it no longer holds.
        let plan = select_files(
            files,
            self.config.filter.as_ref(),
            &schema,
            projected_field_ids,
            projected_columns,
            false,
            None,
        );

        tracing::info!(
            ?from_snapshot_id,
            to_snapshot_id,
            from_seq,
            to_seq,
            files_selected = plan.files_selected,
            estimated_row_count = plan.estimated_row_count,
            "Incremental scan planning complete (append-only)"
        );

        Ok(IncrementalScanPlan {
            files_selected: plan.files_selected,
            estimated_row_count: plan.estimated_row_count,
            added_tasks: plan.tasks,
            projected_columns: plan.projected_columns,
            projected_field_ids: plan.projected_field_ids,
            from_sequence_number: from_seq,
            to_snapshot_id,
            to_sequence_number: to_seq,
        })
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

    // ------------------------------------------------------------------
    // BigQuery's shape: no delete counters in the summary at all, so only
    // the manifest list can show a delete manifest.
    // ------------------------------------------------------------------

    /// A BigQuery-written table (summary `{"operation": "overwrite"}`), its
    /// manifest list and its one data manifest: 2,000,000 rows in 4 files.
    const BQ_METADATA: &[u8] = include_bytes!("../../tests/fixtures/bigquery/big.metadata.json");
    const BQ_MANIFEST_LIST: &[u8] =
        include_bytes!("../../tests/fixtures/bigquery/big.manifest-list.avro");
    const BQ_MANIFEST: &[u8] = include_bytes!("../../tests/fixtures/bigquery/big.manifest.avro");

    /// Serves fixed files and records every path read.
    #[derive(Debug, Default)]
    struct FixedFiles {
        files: HashMap<String, Bytes>,
        reads: std::sync::Mutex<Vec<String>>,
    }

    impl FixedFiles {
        fn read_paths(&self) -> Vec<String> {
            self.reads.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl SendIcebergStorage for FixedFiles {
        async fn read(&self, path: &str) -> Result<Bytes> {
            self.reads.lock().unwrap().push(path.to_string());
            self.files
                .get(path)
                .cloned()
                .ok_or_else(|| IcebergError::storage(format!("no {path}")))
        }
        async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
            Ok(self
                .read(path)
                .await?
                .slice(range.start as usize..range.end as usize))
        }
        async fn file_size(&self, path: &str) -> Result<u64> {
            Ok(self.read(path).await?.len() as u64)
        }
    }

    /// The BigQuery manifest list with a delete manifest (`content = 1`)
    /// added beside its data manifest, in the spec field names BigQuery
    /// writes. The delete manifest itself does not exist: it must never be
    /// read.
    fn bq_manifest_list_with_a_delete_manifest() -> Bytes {
        bq_manifest_list_with_a_second_manifest(1)
    }

    /// The BigQuery manifest list with a second manifest beside its data
    /// manifest, whose `content` is `content`.
    fn bq_manifest_list_with_a_second_manifest(content_of_second: i32) -> Bytes {
        use apache_avro::types::{Record, Value as AvroValue};
        let data = crate::manifest::parse_manifest_list(&Bytes::from_static(BQ_MANIFEST_LIST))
            .unwrap()
            .remove(0);
        let schema = apache_avro::Schema::parse_str(
            r#"{"type": "record", "name": "manifest_file", "fields": [
                {"name": "manifest_path", "type": "string"},
                {"name": "manifest_length", "type": "long"},
                {"name": "partition_spec_id", "type": "int"},
                {"name": "content", "type": "int"},
                {"name": "sequence_number", "type": "long"},
                {"name": "min_sequence_number", "type": "long"},
                {"name": "added_snapshot_id", "type": "long"},
                {"name": "added_files_count", "type": "int"},
                {"name": "existing_files_count", "type": "int"},
                {"name": "deleted_files_count", "type": "int"},
                {"name": "added_rows_count", "type": "long"},
                {"name": "existing_rows_count", "type": "long"},
                {"name": "deleted_rows_count", "type": "long"}
            ]}"#,
        )
        .unwrap();
        let mut writer = apache_avro::Writer::new(&schema, Vec::new());
        let delete_path = data.manifest_path.replace(".avro", "-deletes.avro");
        for (path, content) in [
            (&data.manifest_path, 0i32),
            (&delete_path, content_of_second),
        ] {
            let mut record = Record::new(writer.schema()).unwrap();
            record.put("manifest_path", AvroValue::String(path.clone()));
            record.put("manifest_length", 100i64);
            record.put("partition_spec_id", 0i32);
            record.put("content", content);
            record.put("sequence_number", 0i64);
            record.put("min_sequence_number", 0i64);
            record.put("added_snapshot_id", data.added_snapshot_id);
            record.put("added_files_count", if content == 0 { 4i32 } else { 1i32 });
            record.put("existing_files_count", 0i32);
            record.put("deleted_files_count", 0i32);
            record.put(
                "added_rows_count",
                if content == 0 { 2_000_000i64 } else { 7i64 },
            );
            record.put("existing_rows_count", 0i64);
            record.put("deleted_rows_count", 0i64);
            writer.append(record).unwrap();
        }
        Bytes::from(writer.into_inner().unwrap())
    }

    fn bq_table(list: Bytes) -> (TableMetadata, FixedFiles) {
        let metadata = TableMetadata::from_json(BQ_METADATA).unwrap();
        let snapshot = metadata.current_snapshot().unwrap();
        assert!(
            !crate::mor_guard::summary_indicates_deletes(snapshot),
            "fixture precondition: the summary reports no deletes"
        );
        let list_path = snapshot.manifest_list_location().unwrap().to_string();
        let data_manifest =
            crate::manifest::parse_manifest_list(&Bytes::from_static(BQ_MANIFEST_LIST))
                .unwrap()
                .remove(0)
                .manifest_path;
        let mut files = HashMap::new();
        files.insert(list_path, list);
        files.insert(data_manifest, Bytes::from_static(BQ_MANIFEST));
        (
            metadata,
            FixedFiles {
                files,
                ..Default::default()
            },
        )
    }

    /// The control: the same table with no delete manifest plans its four
    /// files, so a refusal below comes from the manifest list alone.
    #[tokio::test]
    async fn a_bigquery_snapshot_without_delete_manifests_plans() {
        let (metadata, storage) = bq_table(Bytes::from_static(BQ_MANIFEST_LIST));
        let plan = SendScanPlanner::new(&storage, &metadata, ScanConfig::new())
            .plan_scan()
            .await
            .unwrap();
        assert_eq!(plan.files_selected, 4);
        assert_eq!(plan.estimated_row_count, 2_000_000);
        assert!(!plan.has_delete_manifests);
    }

    /// A delete manifest the summary does not report is refused by the scan
    /// and by the materialize path, with the summary guard's typed error, from
    /// the manifest list the planner already reads: no manifest is read.
    #[tokio::test]
    async fn a_delete_manifest_the_summary_does_not_report_is_refused() {
        let (metadata, storage) = bq_table(bq_manifest_list_with_a_delete_manifest());
        let snapshot_id = metadata.current_snapshot().unwrap().snapshot_id;
        let list_path = metadata
            .current_snapshot()
            .and_then(|s| s.manifest_list_location())
            .unwrap()
            .to_string();
        let planner = SendScanPlanner::new(&storage, &metadata, ScanConfig::new());

        let err = planner
            .plan_scan()
            .await
            .expect_err("the scan must fail closed");
        assert!(
            matches!(err, IcebergError::MergeOnReadDeletes(_)),
            "{err:?}"
        );
        assert_eq!(
            storage.read_paths(),
            vec![list_path.clone()],
            "only the list is read"
        );

        for from in [None, Some(snapshot_id)] {
            let err = planner
                .plan_incremental(from, snapshot_id)
                .await
                .expect_err("materialize must fail closed");
            assert!(
                matches!(err, IcebergError::MergeOnReadDeletes(_)),
                "{err:?}"
            );
        }
        assert!(
            storage.read_paths().iter().all(|p| p == &list_path),
            "no manifest is read: {:?}",
            storage.read_paths()
        );
    }

    /// A manifest whose `content` this reader cannot classify (the spec
    /// defines only 0 and 1) is refused like a delete manifest, with the same
    /// typed error, rather than planned as data.
    #[tokio::test]
    async fn a_manifest_of_unknown_content_is_refused_like_a_delete_manifest() {
        let (metadata, storage) = bq_table(bq_manifest_list_with_a_second_manifest(2));
        let err = SendScanPlanner::new(&storage, &metadata, ScanConfig::new())
            .plan_scan()
            .await
            .expect_err("an unclassifiable manifest must fail closed");
        assert!(
            matches!(err, IcebergError::MergeOnReadDeletes(_)),
            "{err:?}"
        );
    }
}

/// Planning from a kept file list (`plan_from_files`) against reading the
/// manifests (`plan_scan_for_snapshot`, and the miss path that keeps the list),
/// over a hand-built table whose files prune differently under each filter.
#[cfg(test)]
mod plan_from_files_tests {
    use super::*;
    use crate::manifest::TypedValue;
    use crate::metadata::SchemaField;
    use crate::scan::planner::ScanPlanner;
    use crate::scan::predicate::{Expression, LiteralValue};
    use crate::test_support::{manifest, manifest_list, FixedFiles, TestFile};
    use std::collections::HashMap;

    const LIST: &str = "mem://t/metadata/snap-3.avro";

    fn field(id: i32, name: &str, ty: &str, required: bool) -> SchemaField {
        SchemaField {
            id,
            name: name.to_string(),
            required,
            field_type: serde_json::json!(ty),
            doc: None,
        }
    }

    fn snapshot(id: i64, seq: i64, list: &str) -> Snapshot {
        Snapshot {
            snapshot_id: id,
            parent_snapshot_id: None,
            sequence_number: seq,
            timestamp_ms: 1000 * seq,
            manifest_list: Some(list.to_string()),
            manifests: None,
            summary: HashMap::new(),
            schema_id: Some(0),
        }
    }

    /// `id long`, `region string`, `status string` (nullable). Snapshot 10
    /// (seq 1) and the current snapshot 30 (seq 3).
    fn metadata() -> TableMetadata {
        TableMetadata {
            format_version: 2,
            table_uuid: None,
            location: "mem://t".to_string(),
            last_sequence_number: 3,
            last_updated_ms: 3000,
            last_column_id: 3,
            current_schema_id: 0,
            schemas: vec![Schema {
                schema_id: 0,
                identifier_field_ids: vec![1],
                fields: vec![
                    field(1, "id", "long", true),
                    field(2, "region", "string", true),
                    field(3, "status", "string", false),
                ],
            }],
            current_snapshot_id: Some(30),
            snapshots: vec![
                snapshot(10, 1, "mem://t/metadata/snap-1.avro"),
                snapshot(30, 3, LIST),
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

    /// 100 rows with ids `lo..=lo+99` in `region`; `status` has `nulls` nulls
    /// (and bounds unless every row is null).
    fn file(path: &str, lo: i64, region: &str, nulls: i64, seq: Option<i64>) -> TestFile {
        let mut bounds = vec![
            (1, TypedValue::Int64(lo), TypedValue::Int64(lo + 99)),
            (
                2,
                TypedValue::String(region.into()),
                TypedValue::String(region.into()),
            ),
        ];
        if nulls < 100 {
            bounds.push((
                3,
                TypedValue::String("closed".into()),
                TypedValue::String("open".into()),
            ));
        }
        TestFile {
            path: path.to_string(),
            record_count: 100,
            bounds,
            nulls: vec![(3, nulls)],
            sequence_number: seq,
        }
    }

    /// Three manifests, listed out of commit order, each listing its files out
    /// of path order; `c0` sits in the seq-3 manifest but carries its own seq
    /// 1, so it sorts with commit 1. Plan order is therefore
    /// a1 a2 a3 c0 | b1 b2 | c1.
    fn table() -> (TableMetadata, FixedFiles) {
        let mut files = HashMap::new();
        files.insert(
            LIST.to_string(),
            manifest_list(&[
                ("mem://t/metadata/m-b.avro", 2),
                ("mem://t/metadata/m-a.avro", 1),
                ("mem://t/metadata/m-c.avro", 3),
            ]),
        );
        files.insert(
            "mem://t/metadata/m-a.avro".to_string(),
            manifest(&[
                file("a2", 100, "EU", 0, None),
                file("a1", 0, "US", 3, None),
                file("a3", 200, "EU", 100, None),
            ]),
        );
        files.insert(
            "mem://t/metadata/m-b.avro".to_string(),
            manifest(&[
                file("b2", 400, "APAC", 0, None),
                file("b1", 300, "US", 0, None),
            ]),
        );
        files.insert(
            "mem://t/metadata/m-c.avro".to_string(),
            manifest(&[
                file("c1", 500, "EU", 0, None),
                file("c0", 600, "US", 0, Some(1)),
            ]),
        );
        (
            metadata(),
            FixedFiles {
                files,
                ..Default::default()
            },
        )
    }

    const ALL: [&str; 7] = ["a1", "a2", "a3", "c0", "b1", "b2", "c1"];

    fn kept_files(files: &[DataFile], has_delete_manifests: bool) -> KeptFiles<'_> {
        KeptFiles {
            files,
            record_count: files.iter().map(|f| f.record_count).sum(),
            has_delete_manifests,
        }
    }

    fn long(v: i64) -> LiteralValue {
        LiteralValue::Int64(v)
    }

    fn string(v: &str) -> LiteralValue {
        LiteralValue::String(v.into())
    }

    /// Each filter with the files it must select, in plan order, worked out by
    /// hand from the bounds and null counts above.
    fn cases() -> Vec<(&'static str, Option<Expression>, Vec<&'static str>)> {
        vec![
            ("no filter", None, ALL.to_vec()),
            (
                "id = 150",
                Some(Expression::eq(1, "id", long(150))),
                vec!["a2"],
            ),
            (
                "250 <= id < 450",
                Some(Expression::and(vec![
                    Expression::gt_eq(1, "id", long(250)),
                    Expression::lt(1, "id", long(450)),
                ])),
                vec!["a3", "b1", "b2"],
            ),
            (
                "id IN (50, 550, 650)",
                Some(Expression::in_list(
                    1,
                    "id",
                    vec![long(50), long(550), long(650)],
                )),
                vec!["a1", "c0", "c1"],
            ),
            (
                "region = EU",
                Some(Expression::eq(2, "region", string("EU"))),
                vec!["a2", "a3", "c1"],
            ),
            (
                "status IS NULL",
                Some(Expression::is_null(3, "status")),
                vec!["a1", "a3"],
            ),
            (
                "status IS NOT NULL",
                Some(Expression::is_not_null(3, "status")),
                vec!["a1", "a2", "c0", "b1", "b2", "c1"],
            ),
            (
                "region = APAC OR id < 50",
                Some(Expression::or(vec![
                    Expression::eq(2, "region", string("APAC")),
                    Expression::lt(1, "id", long(50)),
                ])),
                vec!["a1", "b2"],
            ),
            (
                "id > 10000",
                Some(Expression::gt(1, "id", long(10_000))),
                vec![],
            ),
        ]
    }

    fn paths(plan: &ScanPlan) -> Vec<&str> {
        plan.tasks
            .iter()
            .map(|t| t.data_file.file_path.as_str())
            .collect()
    }

    fn config(filter: &Option<Expression>) -> ScanConfig {
        let config = ScanConfig::new().with_projection(vec![1, 2]);
        match filter {
            Some(f) => config.with_filter(f.clone()),
            None => config,
        }
    }

    /// A plan from the kept list equals the plan read from the manifests: the
    /// same files in the same order, the filter as every task's residual, the
    /// same counters and projection. Both equal the hand-worked selection, so
    /// the comparison is not two copies of one mistake agreeing.
    #[tokio::test]
    async fn a_plan_from_the_kept_list_equals_a_plan_from_the_manifests() {
        let (md, storage) = table();
        let snap = md.current_snapshot().unwrap();
        for (name, filter, want) in cases() {
            let planner = SendScanPlanner::new(&storage, &md, config(&filter));
            let fresh = planner.plan_scan_for_snapshot(snap).await.unwrap();
            let (missed, kept) = planner
                .plan_scan_for_snapshot_retaining_files(snap)
                .await
                .unwrap();
            storage.clear_reads();
            let hit = planner
                .plan_from_files(snap, kept_files(&kept, false))
                .unwrap();
            assert_eq!(
                storage.reads(),
                Vec::<String>::new(),
                "{name}: a hit reads nothing"
            );

            let kept_paths: Vec<_> = kept.iter().map(|f| f.file_path.as_str()).collect();
            assert_eq!(
                kept_paths, ALL,
                "{name}: the kept list is every file, in plan order"
            );
            let residual = format!("{filter:?}");
            for (lane, plan) in [("fresh", &fresh), ("miss", &missed), ("hit", &hit)] {
                assert_eq!(paths(plan), want, "{name} ({lane}): files and order");
                assert_eq!(plan.files_selected, want.len(), "{name} ({lane})");
                assert_eq!(plan.files_pruned, ALL.len() - want.len(), "{name} ({lane})");
                assert_eq!(
                    plan.estimated_row_count,
                    100 * want.len() as i64,
                    "{name} ({lane})"
                );
                assert_eq!(
                    format!("{:?}", plan.residual_filter),
                    residual,
                    "{name} ({lane})"
                );
                for task in &plan.tasks {
                    assert_eq!(
                        format!("{:?}", task.residual_filter),
                        residual,
                        "{name} ({lane}): every task carries the filter as its residual"
                    );
                    assert_eq!(task.projected_field_ids, vec![1, 2]);
                    assert!(task.iceberg_schema.is_some());
                }
                assert_eq!(plan.projected_columns, vec!["id", "region"]);
                // One residual per plan, shared by its tasks rather than copied.
                assert!(
                    plan.tasks.windows(2).all(|w| {
                        match (&w[0].residual_filter, &w[1].residual_filter) {
                            (Some(a), Some(b)) => std::sync::Arc::ptr_eq(a, b),
                            (None, None) => true,
                            _ => false,
                        }
                    }),
                    "{name} ({lane}): tasks share one residual"
                );
            }
            // Sequence numbers: known when read from manifests, absent from a
            // kept list (it holds none).
            let seqs = |plan: &ScanPlan| {
                plan.tasks
                    .iter()
                    .map(|t| t.data_sequence_number)
                    .collect::<Vec<_>>()
            };
            assert!(seqs(&fresh).iter().all(Option::is_some), "{name}");
            assert_eq!(seqs(&missed), seqs(&fresh), "{name}");
            assert!(seqs(&hit).iter().all(Option::is_none), "{name}");
        }
    }

    /// The runtime-agnostic planner builds the same plans through the same
    /// steps.
    #[tokio::test]
    async fn the_runtime_agnostic_planner_plans_the_same_files() {
        let (md, storage) = table();
        let snap = md.current_snapshot().unwrap();
        for (name, filter, want) in cases() {
            let plan = ScanPlanner::new(&storage, &md, config(&filter))
                .plan_scan_for_snapshot(snap)
                .await
                .unwrap();
            assert_eq!(paths(&plan), want, "{name}");
            assert_eq!(plan.files_pruned, ALL.len() - want.len(), "{name}");
        }
    }

    /// Reading one manifest at a time or all at once gives the same plan.
    #[tokio::test]
    async fn manifest_read_concurrency_does_not_change_the_plan() {
        let (md, storage) = table();
        let snap = md.current_snapshot().unwrap();
        for concurrency in [0, 1, 2, 16] {
            let plan = SendScanPlanner::new(
                &storage,
                &md,
                ScanConfig::new().with_manifest_read_concurrency(concurrency),
            )
            .plan_scan_for_snapshot(snap)
            .await
            .unwrap();
            assert_eq!(paths(&plan), ALL, "concurrency {concurrency}");
        }
    }

    /// Every planning arm logs its plan alike: reading the manifests, keeping
    /// the list on a miss, and planning from the kept list on a hit each emit
    /// one "Scan planning complete" line with the same counters.
    #[tokio::test]
    async fn every_planning_arm_logs_its_plan() {
        #[derive(Clone, Default)]
        struct Captured(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Captured {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl Captured {
            fn take_plan_lines(&self) -> Vec<String> {
                let bytes = std::mem::take(&mut *self.0.lock().unwrap());
                String::from_utf8(bytes)
                    .unwrap()
                    .lines()
                    .filter(|l| l.contains("Scan planning complete"))
                    .map(|l| l[l.find("Scan planning complete").unwrap()..].to_string())
                    .collect()
            }
        }
        let captured = Captured::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer({
                let captured = captured.clone();
                move || captured.clone()
            })
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let (md, storage) = table();
        let snap = md.current_snapshot().unwrap();
        let planner = SendScanPlanner::new(&storage, &md, ScanConfig::new());
        planner.plan_scan_for_snapshot(snap).await.unwrap();
        let fresh = captured.take_plan_lines();
        let (_, kept) = planner
            .plan_scan_for_snapshot_retaining_files(snap)
            .await
            .unwrap();
        let miss = captured.take_plan_lines();
        planner
            .plan_from_files(snap, kept_files(&kept, false))
            .unwrap();
        let hit = captured.take_plan_lines();
        assert_eq!(fresh.len(), 1, "fresh: {fresh:?}");
        assert_eq!(miss, fresh, "miss logs as a fresh plan does");
        assert_eq!(hit, fresh, "hit logs as a fresh plan does");
    }

    /// A kept list flagged as delete-bearing (kept while the merge-on-read
    /// override was on) is refused once it is off, as a fresh plan would be.
    #[tokio::test]
    async fn a_kept_delete_bearing_list_is_refused() {
        let (md, storage) = table();
        let snap = md.current_snapshot().unwrap();
        let planner = SendScanPlanner::new(&storage, &md, ScanConfig::new());
        let (_, kept) = planner
            .plan_scan_for_snapshot_retaining_files(snap)
            .await
            .unwrap();
        assert!(planner
            .plan_from_files(snap, kept_files(&kept, false))
            .is_ok());
        let err = planner
            .plan_from_files(snap, kept_files(&kept, true))
            .expect_err("the guard is on by default");
        assert!(
            matches!(err, IcebergError::MergeOnReadDeletes(_)),
            "{err:?}"
        );
    }

    /// A plan of a snapshot written before a column was renamed names the
    /// column as it is now, from the manifests and from the kept list alike:
    /// a task's schema names its batches' columns, and callers use current
    /// names. Pruning goes by field id, so it is unaffected.
    #[tokio::test]
    async fn a_pinned_plan_names_columns_as_they_are_now() {
        let (mut md, storage) = table();
        let mut then = md.schemas[0].clone();
        then.schema_id = 7;
        then.fields[1].name = "zone".to_string();
        md.schemas.push(then);
        md.snapshots[0].schema_id = Some(7);
        md.snapshots[0].manifest_list = Some(LIST.to_string());
        let snap = md.snapshot(10).unwrap();
        let planner = SendScanPlanner::new(
            &storage,
            &md,
            ScanConfig::new().with_filter(Expression::eq(2, "region", string("EU"))),
        );
        let fresh = planner.plan_scan_for_snapshot(snap).await.unwrap();
        let (missed, kept) = planner
            .plan_scan_for_snapshot_retaining_files(snap)
            .await
            .unwrap();
        let hit = planner
            .plan_from_files(snap, kept_files(&kept, false))
            .unwrap();
        for (lane, plan) in [("fresh", &fresh), ("miss", &missed), ("hit", &hit)] {
            assert_eq!(plan.projected_columns, ["id", "region", "status"], "{lane}");
            assert_eq!(paths(plan), ["a2", "a3", "c1"], "{lane}");
            for task in &plan.tasks {
                let names: Vec<_> = task
                    .iceberg_schema
                    .as_ref()
                    .unwrap()
                    .fields
                    .iter()
                    .map(|f| f.name.as_str())
                    .collect();
                assert_eq!(names, ["id", "region", "status"], "{lane}");
            }
        }
    }

    /// The incremental plan reads only manifests newer than its window and
    /// keeps only files added after it, in commit order.
    #[tokio::test]
    async fn an_incremental_plan_reads_only_newer_manifests() {
        let (md, storage) = table();
        let plan = SendScanPlanner::new(&storage, &md, ScanConfig::new())
            .plan_incremental(Some(10), 30)
            .await
            .unwrap();
        let got: Vec<_> = plan
            .added_tasks
            .iter()
            .map(|t| (t.data_file.file_path.as_str(), t.data_sequence_number))
            .collect();
        // c0 is listed by the seq-3 manifest but was added at seq 1.
        assert_eq!(got, vec![("b1", Some(2)), ("b2", Some(2)), ("c1", Some(3))]);
        assert_eq!(plan.files_selected, 3);
        assert!(
            !storage.reads().iter().any(|p| p.ends_with("m-a.avro")),
            "the seq-1 manifest cannot hold an addition: {:?}",
            storage.reads()
        );
    }
}
