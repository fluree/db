//! Open a Delta table, select a version, and stream its logical rows.
//!
//! Kernel's API is synchronous over a background executor, so every call into
//! it runs on the blocking pool.

use std::collections::VecDeque;
use std::num::NonZero;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use delta_kernel::actions::deletion_vector::split_vector;
use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
use delta_kernel::engine::arrow_conversion::TryFromKernel;
use delta_kernel::engine::arrow_data::EngineDataArrowExt;
use delta_kernel::history_manager::error::{LogHistoryError, NearestTimestamp};
use delta_kernel::history_manager::{latest_version_as_of, HistoryCommitType};
use delta_kernel::scan::state::{transform_to_logical, ScanFile};
use delta_kernel::scan::Scan;
use delta_kernel::schema::{StructField, StructType};
use delta_kernel::snapshot::SnapshotRef;
use delta_kernel::{Engine as _, FileMeta, Predicate, PredicateRef, Snapshot};
use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder};
use fluree_db_tabular::{BatchSchema, ColumnBatch};
use futures::Stream;
use url::Url;

use crate::bridge::BatchBridge;
use crate::config::DeltaIoConfig;
use crate::error::{DeltaError, Result};
use crate::filter::{ColumnFilter, RowFilter};

type Engine = DefaultEngine<TokioMultiThreadExecutor>;

/// Rows per decoded batch; Kernel's default of 1000 spends most of a scan on
/// per-batch overhead.
const BATCH_ROWS: NonZero<usize> = NonZero::new(8192).unwrap();

/// One runtime for every table's I/O and Parquet decode. Owned rather than
/// borrowed from the caller, whose runtime may be single-threaded, and never
/// dropped: a runtime cannot be dropped from async context.
fn executor() -> Result<Arc<TokioMultiThreadExecutor>> {
    // A lock, not a `OnceLock`: the loser of an init race would drop its runtime.
    static EXECUTOR: Mutex<Option<Arc<TokioMultiThreadExecutor>>> = Mutex::new(None);
    let mut slot = EXECUTOR
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(executor) = slot.as_ref() {
        return Ok(executor.clone());
    }
    let built =
        TokioMultiThreadExecutor::new_owned_runtime(Some(scan_concurrency(usize::MAX)), None)
            .map_err(|e| DeltaError::Internal(format!("Delta reader runtime: {e}")))?;
    Ok(slot.insert(Arc::new(built)).clone())
}

/// Files of one scan read at a time: `FLUREE_DELTA_SCAN_CONCURRENCY`, else the
/// core count, capped at 32.
fn scan_concurrency(files: usize) -> usize {
    let wanted = std::env::var("FLUREE_DELTA_SCAN_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(NonZero::get)
                .unwrap_or(4)
                .min(32)
        });
    wanted.min(files.max(1))
}

/// Logical rows of one scan, a file's worth of batches at a time.
pub type DeltaBatchStream = Pin<Box<dyn Stream<Item = Result<ColumnBatch>> + Send + Sync>>;

/// Which state of the table to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionSelector {
    Latest,
    /// Exactly this Delta version.
    Version(u64),
    /// The latest version committed at or before this instant. An instant after
    /// the latest commit selects the latest version.
    AsOfTimestampMs(i64),
}

/// A Delta table and the engine that reads it. Cheap to clone; safe to cache
/// for the life of the process (it holds no table state).
#[derive(Clone)]
pub struct DeltaTable {
    name: Arc<str>,
    url: Url,
    engine: Arc<Engine>,
}

impl std::fmt::Debug for DeltaTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaTable")
            .field("name", &self.name)
            .field("url", &self.url.as_str())
            .finish_non_exhaustive()
    }
}

impl DeltaTable {
    /// `name` labels errors; `location` is the table directory.
    pub fn open(name: &str, location: &str, io: &DeltaIoConfig) -> Result<Self> {
        let (url, store) = crate::store::open(location, io)?;
        Ok(Self {
            name: name.into(),
            url,
            engine: Arc::new(
                DefaultEngineBuilder::new(store)
                    .with_task_executor(executor()?)
                    .with_batch_size(BATCH_ROWS)
                    .build(),
            ),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Replay the log to the selected version. A selector nothing satisfies is
    /// an error, never a different version.
    pub async fn snapshot(&self, selector: VersionSelector) -> Result<DeltaSnapshot> {
        let table = self.clone();
        blocking(move || table.snapshot_blocking(selector)).await
    }

    fn snapshot_blocking(&self, selector: VersionSelector) -> Result<DeltaSnapshot> {
        let engine = self.engine.as_ref();
        let build = |version: Option<u64>| {
            let builder = Snapshot::builder_for(self.url.clone());
            match version {
                Some(v) => builder.at_version(v),
                None => builder,
            }
            .build(engine)
        };
        let snapshot = match selector {
            VersionSelector::Latest => build(None).map_err(|e| self.kernel(e))?,
            VersionSelector::Version(version) => {
                let latest = build(None).map_err(|e| self.kernel(e))?;
                match version.cmp(&latest.version()) {
                    std::cmp::Ordering::Equal => latest,
                    std::cmp::Ordering::Greater => return Err(self.version_not_found(version)),
                    // Older than latest: the log no longer reaching it (cleanup)
                    // is the expected failure, so report it as the missing pin.
                    std::cmp::Ordering::Less => build(Some(version)).map_err(|e| {
                        tracing::debug!(table = %self.name, version, error = %e, "Delta version not reconstructable");
                        self.version_not_found(version)
                    })?,
                }
            }
            VersionSelector::AsOfTimestampMs(ms) => {
                let latest = build(None).map_err(|e| self.kernel(e))?;
                let commit =
                    latest_version_as_of(&latest, engine, ms, HistoryCommitType::Recreatable)
                        .map_err(|e| self.time_error(ms, e))?;
                if commit.version == latest.version() {
                    latest
                } else {
                    build(Some(commit.version)).map_err(|e| self.kernel(e))?
                }
            }
        };
        Ok(DeltaSnapshot {
            table: self.clone(),
            snapshot,
        })
    }

    fn kernel(&self, error: delta_kernel::Error) -> DeltaError {
        DeltaError::kernel(&self.name, error)
    }

    fn version_not_found(&self, version: u64) -> DeltaError {
        DeltaError::VersionNotFound {
            table: self.name.to_string(),
            version,
        }
    }

    fn time_error(&self, requested_ms: i64, error: delta_kernel::Error) -> DeltaError {
        let history = match &error {
            delta_kernel::Error::LogHistory(e) => Some(e.as_ref()),
            delta_kernel::Error::Backtraced { source, .. } => match source.as_ref() {
                delta_kernel::Error::LogHistory(e) => Some(e.as_ref()),
                _ => None,
            },
            _ => None,
        };
        match history {
            Some(LogHistoryError::TimestampOutOfRange {
                nearest_timestamp, ..
            }) => DeltaError::NoVersionAtTime {
                table: self.name.to_string(),
                requested_ms,
                oldest_ms: match nearest_timestamp {
                    NearestTimestamp::Earliest(ts) => Some(*ts),
                    _ => None,
                },
            },
            _ => self.kernel(error),
        }
    }
}

/// One version of a table.
#[derive(Clone)]
pub struct DeltaSnapshot {
    table: DeltaTable,
    snapshot: SnapshotRef,
}

impl DeltaSnapshot {
    pub fn version(&self) -> u64 {
        self.snapshot.version()
    }

    /// Commit time of this version in epoch milliseconds: the in-commit
    /// timestamp when the table records one, else the log file's modification
    /// time (which a copied table does not preserve).
    pub async fn timestamp_ms(&self) -> Result<i64> {
        let this = self.clone();
        blocking(move || {
            this.snapshot
                .get_timestamp(this.table.engine.as_ref())
                .map_err(|e| this.table.kernel(e))
        })
        .await
    }

    /// Column names of this version, in schema order.
    pub fn column_names(&self) -> Vec<String> {
        self.snapshot
            .schema()
            .fields()
            .map(|f| f.name().clone())
            .collect()
    }

    /// The batch schema a scan of `projection` yields (every column when
    /// empty), without reading data. Fails on a column this version lacks or a
    /// type the batch model cannot carry.
    pub fn batch_schema(&self, projection: &[String]) -> Result<Arc<BatchSchema>> {
        let (_, bridge) = self.plan(projection)?;
        Ok(bridge.schema().clone())
    }

    /// Stream this version's live rows: deletion vectors applied, partition
    /// values injected, physical columns mapped to their logical names.
    ///
    /// Rows arrive in no particular order. A filter the reader can state exactly
    /// (see [`crate::filter`]) removes the rows it rejects; any other is ignored,
    /// so the caller must still apply every filter itself.
    pub fn scan(
        &self,
        projection: &[String],
        filters: &[ColumnFilter],
    ) -> Result<DeltaBatchStream> {
        let (schema, bridge) = self.plan(projection)?;
        let predicate = crate::filter::to_predicate(&self.snapshot.schema(), filters);
        let rows = RowFilter::new(&self.snapshot.schema(), bridge.arrow_schema(), filters);
        let this = self.clone();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<ColumnBatch>>(4);
        tokio::task::spawn_blocking(move || {
            let planned = this
                .build_scan(Some(schema), predicate)
                .and_then(|scan| Ok((this.files(&scan)?, scan)));
            let (files, scan) = match planned {
                Ok(planned) => planned,
                Err(e) => {
                    let _ = tx.blocking_send(Err(e));
                    return;
                }
            };
            let workers = scan_concurrency(files.len());
            let queue = Arc::new(Mutex::new(VecDeque::from(files)));
            let scan = Arc::new(scan);
            let shape = Arc::new((bridge, rows));
            for _ in 0..workers {
                let (this, scan, shape, queue, tx) = (
                    this.clone(),
                    scan.clone(),
                    shape.clone(),
                    queue.clone(),
                    tx.clone(),
                );
                tokio::task::spawn_blocking(move || loop {
                    let next = queue
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .pop_front();
                    let Some(file) = next else { break };
                    match this.read_file(&scan, file, &shape.0, &shape.1, &tx) {
                        Ok(true) => {}
                        Ok(false) => break, // consumer stopped early
                        Err(e) => {
                            // Stop the other workers: one error ends the scan.
                            queue
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .clear();
                            let _ = tx.blocking_send(Err(e));
                            break;
                        }
                    }
                });
            }
        });
        Ok(Box::pin(futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        })))
    }

    /// How many data files a scan with `filters` reads.
    pub async fn file_count(&self, filters: &[ColumnFilter]) -> Result<usize> {
        let this = self.clone();
        let predicate = crate::filter::to_predicate(&self.snapshot.schema(), filters);
        blocking(move || {
            let scan = this.build_scan(None, predicate)?;
            Ok(this.files(&scan)?.len())
        })
        .await
    }

    /// The number of live rows, from the log alone — `None` unless it provably
    /// equals what a scan would count: every file records its row count, none
    /// carries a deletion vector, and statistics show no null in any of
    /// `non_null_columns`.
    pub async fn exact_row_count(&self, non_null_columns: &[String]) -> Result<Option<u64>> {
        let this = self.clone();
        let schema = self.snapshot.schema();
        let mut nullable = Vec::new();
        for column in non_null_columns {
            let Some(field) = resolve_field(&schema, column) else {
                return Ok(None);
            };
            if field.is_nullable() {
                nullable.push(Predicate::is_null(delta_kernel::Expression::column([
                    field.name().as_str(),
                ])));
            }
        }
        blocking(move || {
            if !nullable.is_empty() {
                // Skipping keeps a file unless its statistics rule a null out,
                // so an empty plan proves there is none.
                let may_hold_null =
                    this.build_scan(None, Some(Arc::new(Predicate::or_from(nullable))))?;
                if !this.files(&may_hold_null)?.is_empty() {
                    return Ok(None);
                }
            }
            let mut total = 0u64;
            for file in this.files(&this.build_scan(None, None)?)? {
                match file.stats {
                    Some(stats) if !file.dv_info.has_vector() => {
                        total = match total.checked_add(stats.num_records) {
                            Some(total) => total,
                            None => return Ok(None),
                        }
                    }
                    _ => return Ok(None),
                }
            }
            Ok(Some(total))
        })
        .await
    }

    fn build_scan(
        &self,
        schema: Option<Arc<StructType>>,
        predicate: Option<PredicateRef>,
    ) -> Result<Scan> {
        self.snapshot
            .clone()
            .scan_builder()
            .with_schema_opt(schema)
            .with_predicate(predicate)
            .build()
            .map_err(|e| self.table.kernel(e))
    }

    fn files(&self, scan: &Scan) -> Result<Vec<ScanFile>> {
        fn push(files: &mut Vec<ScanFile>, file: ScanFile) {
            files.push(file);
        }
        let mut files = Vec::new();
        for metadata in scan
            .scan_metadata(self.table.engine.as_ref())
            .map_err(|e| self.table.kernel(e))?
        {
            files = metadata
                .and_then(|m| m.visit_scan_files(files, push))
                .map_err(|e| self.table.kernel(e))?;
        }
        Ok(files)
    }

    /// Read one data file into `tx`; `false` once the consumer has gone.
    fn read_file(
        &self,
        scan: &Scan,
        file: ScanFile,
        bridge: &BatchBridge,
        rows: &RowFilter,
        tx: &tokio::sync::mpsc::Sender<Result<ColumnBatch>>,
    ) -> Result<bool> {
        let kernel = |e| self.table.kernel(e);
        let engine = self.table.engine.as_ref();
        let root = self.snapshot.table_root();
        let mut deleted = file
            .dv_info
            .get_selection_vector(engine, root)
            .map_err(kernel)?;
        let meta = FileMeta {
            location: root.join(&file.path).map_err(|e| {
                DeltaError::Internal(format!("data file path '{}': {e}", file.path))
            })?,
            last_modified: 0,
            size: u64::try_from(file.size)
                .map_err(|_| DeltaError::Internal(format!("data file size {}", file.size)))?,
        };
        let batches = engine
            .parquet_handler()
            .read_parquet_files(&[meta], scan.physical_schema().clone(), None)
            .map_err(kernel)?;
        for physical in batches {
            let logical = transform_to_logical(
                engine,
                physical.map_err(kernel)?,
                scan.physical_schema(),
                scan.logical_schema(),
                file.transform.clone(),
            )
            .map_err(kernel)?;
            // The vector covers the whole file; each batch takes its prefix.
            let mut mine = deleted.take();
            deleted = split_vector(mine.as_mut(), logical.len(), None);
            let live = match mine {
                Some(selection) => logical.apply_selection_vector(selection).map_err(kernel)?,
                None => logical,
            };
            let batch = rows.apply(live.try_into_record_batch().map_err(kernel)?)?;
            if batch.num_rows() == 0 {
                continue;
            }
            if tx.blocking_send(bridge.convert(&batch)).is_err() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Resolve `projection` against this version's schema and build the bridge
    /// that types its batches.
    fn plan(&self, projection: &[String]) -> Result<(Arc<StructType>, BatchBridge)> {
        let full = self.snapshot.schema();
        let names: Vec<&str> = if projection.is_empty() {
            full.fields().map(|f| f.name().as_str()).collect()
        } else {
            projection
                .iter()
                .map(|wanted| {
                    resolve_field(&full, wanted)
                        .map(|f| f.name().as_str())
                        .ok_or_else(|| DeltaError::ColumnNotFound {
                            table: self.table.name.to_string(),
                            column: wanted.clone(),
                            version: self.version(),
                        })
                })
                .collect::<Result<_>>()?
        };
        let schema = Arc::new(
            full.project_as_struct(&names)
                .map_err(|e| self.table.kernel(e))?,
        );
        // Column-mapping ids are stable across renames. An unmapped table has
        // none, so its columns are numbered by position in the full schema —
        // never by position in the projection.
        let ids = schema
            .fields()
            .map(|field| {
                let id = match field.column_mapping_id() {
                    Some(id) => id,
                    None => {
                        full.fields()
                            .position(|f| f.name() == field.name())
                            .ok_or_else(|| {
                                DeltaError::Internal(format!(
                                    "projected column '{}' absent from its snapshot",
                                    field.name()
                                ))
                            })? as i64
                            + 1
                    }
                };
                i32::try_from(id)
                    .map_err(|_| DeltaError::Internal(format!("column id {id} out of range")))
            })
            .collect::<Result<Vec<_>>>()?;
        let arrow = ArrowSchema::try_from_kernel(schema.as_ref())
            .map_err(|e| DeltaError::UnsupportedType(e.to_string()))?;
        let bridge = BatchBridge::new(Arc::new(arrow), &ids)?;
        Ok((schema, bridge))
    }
}

/// `wanted` by exact name, else ignoring ASCII case.
pub(crate) fn resolve_field<'a>(schema: &'a StructType, wanted: &str) -> Option<&'a StructField> {
    schema.fields().find(|f| f.name() == wanted).or_else(|| {
        schema
            .fields()
            .find(|f| f.name().eq_ignore_ascii_case(wanted))
    })
}

async fn blocking<T, F>(f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| DeltaError::Internal(format!("Delta reader task failed: {e}")))?
}
