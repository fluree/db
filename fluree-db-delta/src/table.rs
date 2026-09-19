//! Open a Delta table, select a version, and stream its logical rows.
//!
//! Kernel's API is synchronous over a background executor, so every call into
//! it runs on the blocking pool.

use std::pin::Pin;
use std::sync::Arc;

use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
use delta_kernel::engine::arrow_conversion::TryFromKernel;
use delta_kernel::engine::arrow_data::EngineDataArrowExt;
use delta_kernel::history_manager::error::{LogHistoryError, NearestTimestamp};
use delta_kernel::history_manager::{latest_version_as_of, HistoryCommitType};
use delta_kernel::schema::StructType;
use delta_kernel::snapshot::SnapshotRef;
use delta_kernel::Snapshot;
use delta_kernel_default_engine::executor::tokio::TokioBackgroundExecutor;
use delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder};
use fluree_db_tabular::{BatchSchema, ColumnBatch};
use futures::Stream;
use url::Url;

use crate::bridge::BatchBridge;
use crate::config::DeltaIoConfig;
use crate::error::{DeltaError, Result};

type Engine = DefaultEngine<TokioBackgroundExecutor>;

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
            engine: Arc::new(DefaultEngineBuilder::new(store).build()),
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
    pub fn scan(&self, projection: &[String]) -> Result<DeltaBatchStream> {
        let (schema, bridge) = self.plan(projection)?;
        let this = self.clone();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<ColumnBatch>>(2);
        tokio::task::spawn_blocking(move || {
            let run = || -> Result<()> {
                let scan = this
                    .snapshot
                    .clone()
                    .scan_builder()
                    .with_schema(schema)
                    .build()
                    .map_err(|e| this.table.kernel(e))?;
                for result in scan
                    .execute(this.table.engine.clone())
                    .map_err(|e| this.table.kernel(e))?
                {
                    let batch = result
                        .try_into_record_batch()
                        .map_err(|e| this.table.kernel(e))?;
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    if tx.blocking_send(bridge.convert(&batch)).is_err() {
                        break; // consumer stopped early
                    }
                }
                Ok(())
            };
            if let Err(e) = run() {
                let _ = tx.blocking_send(Err(e));
            }
        });
        Ok(Box::pin(futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        })))
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
                    full.fields()
                        .map(|f| f.name().as_str())
                        .find(|name| name == wanted)
                        .or_else(|| {
                            full.fields()
                                .map(|f| f.name().as_str())
                                .find(|name| name.eq_ignore_ascii_case(wanted))
                        })
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

async fn blocking<T, F>(f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| DeltaError::Internal(format!("Delta reader task failed: {e}")))?
}
