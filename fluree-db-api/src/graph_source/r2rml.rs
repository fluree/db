//! R2RML graph source operations and provider.
//!
//! This module provides APIs for creating R2RML graph sources and implements
//! the R2RML provider traits for query execution against Iceberg tables.
//!
//! This module is only available with the `iceberg` feature.

use super::lazy_storage::LazyS3Storage;
use crate::graph_source::cache::{CachedScanFiles, R2rmlCache};
use crate::graph_source::config::{CatalogMode, IcebergCreateConfig, R2rmlCreateConfig};
use crate::graph_source::iceberg_catalog::{
    decide_credential_source, storage_query_error, CredentialSource,
};
use crate::graph_source::result::{IcebergCreateResult, R2rmlCreateResult};
use crate::Result;
use async_trait::async_trait;
use fluree_db_core::ContentStore;
use fluree_db_iceberg::error::IcebergError;
use fluree_db_iceberg::{
    catalog::{
        LoadTableResponse, RestCatalogClient, RestCatalogConfig, SendCatalogClient, TableIdentifier,
    },
    config::IoConfig,
    io::{
        ColumnBatch, FileIcebergStorage, IcebergStorageBackend, S3IcebergStorage,
        SendIcebergStorage, SendParquetReader,
    },
    metadata::TableMetadata,
    scan::{
        topk::{batch_sort_values, plan_topk_read, TopKBound},
        ComparisonOp, Expression, FileScanTask, LiteralValue, ScanConfig, SendScanPlanner,
    },
    stats::{aggregate_column_stats, send_read_snapshot_data_files},
    DeleteConvention, IcebergGsConfig,
};
use fluree_db_nameservice::GraphSourceType;
use fluree_db_query::error::{QueryError, Result as QueryResult};
use fluree_db_query::r2rml::{
    ColumnBatchStream, R2rmlProvider, R2rmlTableProvider, ScanCmpOp, ScanFilter, ScanTopK,
    ScanValue,
};
use fluree_db_r2rml::mapping::CompiledR2rmlMapping;
use futures::StreamExt;
use std::sync::Arc;
use tracing::{debug, info, warn, Instrument};

/// Max files a scan-side top-k (PR-5) reads SEQUENTIALLY (bound-ordered, with
/// early-stop) before conceding the prune is ineffective and handing the rest to
/// the normal bounded-parallel reader. Caps the worst case (adversarial layout /
/// all files tie at the bound / a heap that never fills) so the topk path can
/// never be slower than the parallel path it replaces. The win case (q046) reads
/// ~10-15 files and stops well under this.
const TOPK_SEQUENTIAL_CAP: usize = 128;

/// How many data files to read concurrently. Defaults to
/// `min(available_parallelism, files, 32)`; override with
/// `FLUREE_ICEBERG_SCAN_CONCURRENCY` (a positive integer; not capped, so callers
/// can raise it further for high-latency remote object stores).
///
/// PR-2 Lever B raised the ceiling from 8 to 32. The per-file decode cost is
/// fixed S3 round-trip latency, not CPU (see
/// `docs/audit/2026-07-virtual-dataset-perf/06-per-file-cost.md`), so more
/// in-flight reads is close to pure win on the thousands-of-tiny-files fact-table
/// shape; the sweep showed wall still improving to c=32 with only mild per-file
/// contention creep past ~c=16, hence 32 as the ceiling. Raising the ceiling
/// never lowers the previous default on any core count (`clamp(1, 32) >=
/// clamp(1, 8)` pointwise; a 2-core host still runs 2), but the memory trade is
/// real: in-flight buffer bytes are `O(concurrency)` file decodes (each <=32MB
/// whole-file / <=64MB sparse-buffer), and the default now scales with cores up
/// to 32 where it was previously capped at 8 regardless of cores — up to 4x the
/// prior in-flight bytes on a >=32-core host. The sweep data and the
/// tiny-file fact-table shape justify the trade; the env override is the
/// pressure valve in both directions — raise it to reach the ceiling on a
/// low-core host, lower it on a memory-tight one.
fn iceberg_scan_concurrency(num_files: usize) -> usize {
    if let Some(n) = std::env::var("FLUREE_ICEBERG_SCAN_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
    {
        return n.min(num_files.max(1));
    }
    let cpus = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(4);
    cpus.min(num_files.max(1)).clamp(1, 32)
}

/// Stable hash of a graph source's raw config JSON. Keys the process-wide REST
/// catalog client cache. A config *edit* (including a secret written inline)
/// yields a new fingerprint and a freshly built client. Note this hashes the raw
/// JSON only: a secret referenced by env var / secret store is stored as that
/// reference, so rotating the underlying secret leaves the fingerprint unchanged
/// — the client cache's TTL (see `cache::DEFAULT_REST_CLIENT_TTL_SECS`), not this
/// fingerprint, is what bounds staleness in that case.
fn config_fingerprint(config: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    config.hash(&mut h);
    h.finish()
}

/// Build the process-wide REST-client cache key for a graph source: its id plus a
/// fingerprint of the raw config JSON. Shared by the query scan path and the
/// `/info` row-count fetch so both reuse the SAME cached client (one OAuth token
/// and one HTTPS connection pool), warmed by whichever path runs first. Keeping
/// this in one place guarantees the two keys never drift.
pub(crate) fn rest_client_cache_key(graph_source_id: &str, config: &str) -> String {
    format!("{graph_source_id}\u{1f}{:016x}", config_fingerprint(config))
}

/// Whether numeric (double / decimal) FILTER pushdown — including the integer →
/// scale-0-decimal coercion against a decimal column — is enabled (PR-7). Mirrors
/// the query-crate `FLUREE_ICEBERG_NUMERIC_STATS` switch (the two crates can't
/// share the `pub(crate)` symbol); read once, cached for the process. Off restores
/// the pre-PR-7 behavior: an integer literal against a decimal column pushes as
/// `Int64`, which the decimal bound compare declines → no prune (full revert).
pub(crate) fn iceberg_numeric_stats_enabled() -> bool {
    use std::sync::OnceLock;
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("FLUREE_ICEBERG_NUMERIC_STATS") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no"
        ),
        Err(_) => true,
    })
}

/// The Iceberg pushdown literal for an integer scan value against a column of
/// `type_str`. Against a `decimal(p,s)` column with `numeric_stats` on, the
/// integer is pushed as an EXACT scale-0 decimal (comparable to the column's
/// decimal bounds — `decimal_cmp` normalizes the scale gap); with it off it stays
/// `Int64` (which the decimal bound compare declines → no prune), preserving the
/// switch's revert guarantee. This is what lets an integer FILTER (`?deb >
/// 1000000`) prune an `xsd:decimal` column (q019 / H4). An `int`-typed column
/// narrows to `Int32`, skipping (`None`) an out-of-range literal rather than
/// wrapping. `None` = skip the push (the operator still enforces).
fn int_pushdown_literal(
    n: i64,
    type_str: Option<&str>,
    numeric_stats: bool,
) -> Option<LiteralValue> {
    match type_str {
        Some("int") => i32::try_from(n).ok().map(LiteralValue::Int32),
        Some(t) if t.starts_with("decimal") && numeric_stats => Some(LiteralValue::Decimal {
            unscaled: i128::from(n),
            // precision is cosmetic for pruning (`decimal_cmp` ignores it); an i64
            // is ≤19 digits, so the decimal128 max always covers it.
            precision: 38,
            scale: 0,
        }),
        _ => Some(LiteralValue::Int64(n)),
    }
}

/// One materialize scan: what to read, how it was chosen, and how stale the window is.
///
/// A struct rather than a tuple because `window_age_ms` is easy to drop silently
/// from a positional return, and dropping it reintroduces the watermark-staleness
/// failure it exists to prevent.
pub struct MaterializeScan {
    /// The source's current snapshot — the new watermark to persist. `None` when
    /// the table has no snapshots at all (nothing to materialize).
    pub to_snapshot_id: Option<i64>,
    /// Whether an added-files scan was used, rather than a full read.
    pub incremental: bool,
    /// Wall-clock span of the window, from Iceberg snapshot timestamps: how far the
    /// stored watermark now lags the current snapshot. `None` on a first run (no
    /// stored watermark) or when the stored snapshot is no longer resolvable —
    /// both of which mean "persist regardless".
    pub window_age_ms: Option<i64>,
    /// Column batches, streamed. See [`Self::stream`] usage notes on the method.
    pub stream: ColumnBatchStream,
}

/// Why a materialize poll reads added files only, or re-reads the whole table.
///
/// This type exists because the decision used to be one expression:
///
/// ```ignore
/// let incremental = from_snapshot_id.is_some()
///     && metadata.window_is_incremental_safe(from, to).unwrap_or(false);
/// ```
///
/// `unwrap_or(false)` collapses three different situations into one boolean and
/// discards the error that distinguishes them. That matters more than it looks,
/// because the two branches are not comparable in cost: an added-files scan
/// reads what changed, while the fallback reads the **entire table** into
/// `Vec<ColumnBatch>` in memory. So a swallowed error silently escalates a
/// few-MB read into an unbounded one.
///
/// It cost us a production outage. A source table's snapshot retention expired
/// the stored watermark, every poll took the fallback, and on a 728,876-row
/// table the process reached 21.4 GiB of anonymous memory and was OOMKilled
/// every 8–17 minutes. **Not one log line said a full read had been chosen, let
/// alone why** — the two branches were indistinguishable from outside, so the
/// symptom presented as "Fluree uses all the memory" and took days to trace.
///
/// The distinction the boolean threw away is the whole diagnosis:
///
/// - [`FullUnsafeWindow`](Self::FullUnsafeWindow) is **correct and routine**. An
///   `overwrite`/`delete` carries row-level changes an added-files scan cannot
///   see, so a full read is the right answer, not a degradation.
/// - [`FullUndeterminable`](Self::FullUndeterminable) is **a configuration
///   problem to fix**. Retention is shorter than the poll interval, or the job
///   was stopped for longer than retention. A full read still produces correct
///   data, so this deliberately does not fail — but it must be visible, because
///   left alone it repeats every poll forever and never self-heals.
///
/// Both used to log nothing. Now they log differently, which is the fix.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ScanChoice {
    /// Window is `append`/`replace`-only: an added-files scan sees every change.
    Incremental,
    /// No stored watermark — first materialization of this source. Expected once
    /// per source, and a full read is the only option.
    FullInitial,
    /// The window genuinely contains `overwrite`/`delete`. Full read is correct.
    FullUnsafeWindow,
    /// Safety could not be determined: the watermark snapshot is expired,
    /// unknown, or not an ancestor of the current one. Carries the underlying
    /// reason so the log says which.
    FullUndeterminable(String),
}

impl ScanChoice {
    fn decide(metadata: &TableMetadata, from: Option<i64>, to: i64) -> Self {
        // A missing watermark is not a failure to classify — it is a first run.
        // Kept separate from the error case so "expected once" and "someone
        // should look at this" never share a log line.
        if from.is_none() {
            return Self::FullInitial;
        }
        match metadata.window_is_incremental_safe(from, to) {
            Ok(true) => Self::Incremental,
            Ok(false) => Self::FullUnsafeWindow,
            Err(e) => Self::FullUndeterminable(e.to_string()),
        }
    }

    fn is_incremental(&self) -> bool {
        matches!(self, Self::Incremental)
    }
}

/// Bytes of flakes one materialized row costs, for sizing the full-read budget.
///
/// `FLUREE_MATERIALIZE_FLAKE_BYTES_PER_ROW`, default 108 — measured on the
/// production table this bound was tuned against. It exists only to convert the
/// novelty ceiling into a row count. A deployment whose rows are much wider or
/// narrower should set THIS rather than overriding the row budget directly, so
/// the derivation below keeps tracking the ceiling instead of drifting from it.
fn flake_bytes_per_row() -> i64 {
    static CACHED: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("FLUREE_MATERIALIZE_FLAKE_BYTES_PER_ROW")
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(108)
    })
}

/// The novelty ceiling this deployment commits against, in bytes.
///
/// Read from `FLUREE_REINDEX_MAX_BYTES` — the SAME variable that configures
/// `IndexConfig::reindex_max_bytes`, which `at_max_novelty` compares novelty
/// against (`novelty.size >= reindex_max_bytes`) — so the budget below and the
/// wall it has to fit under cannot drift apart.
fn novelty_ceiling_bytes() -> i64 {
    static CACHED: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("FLUREE_REINDEX_MAX_BYTES")
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or_else(|| {
                i64::try_from(crate::server_defaults::default_reindex_max_bytes())
                    .unwrap_or(i64::MAX)
            })
    })
}

/// Rows a single FULL materialize read may take before checkpointing.
///
/// `FLUREE_MATERIALIZE_MAX_ROWS_PER_FULL_PASS` overrides; `0` disables the bound
/// and restores read-it-all. Left unset it is DERIVED from the novelty ceiling,
/// and that derivation is the whole point rather than a convenience.
///
/// A bounded pass is only worth anything if it can COMMIT. A window over the
/// ceiling is DEFERRED, and a deferral discards the window's progress, so the
/// next poll re-reads the same rows and stops at the same wall — nothing
/// accumulates. A bound that does not fit under the ceiling is therefore not a
/// bound at all: it changes how much is read and nothing about whether any of it
/// lands. The previous flat 250_000 rows is ~27 MB of flakes, which against a
/// deployment that has pinned the ceiling to 8 MiB could never commit, so the
/// read repeated on every poll indefinitely.
///
/// Floored at 1_000 rows so a very small ceiling still makes forward progress
/// rather than producing a zero-row pass that reads nothing and checkpoints
/// nowhere.
fn materialize_max_rows_per_full_pass() -> i64 {
    static CACHED: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        if let Some(explicit) = std::env::var("FLUREE_MATERIALIZE_MAX_ROWS_PER_FULL_PASS")
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
            .filter(|v| *v >= 0)
        {
            return explicit;
        }
        rows_for_ceiling(novelty_ceiling_bytes(), flake_bytes_per_row())
    })
}

/// Rows that fit under `ceiling_bytes` at `bytes_per_row`, floored at 1_000.
///
/// Split out from the env plumbing above so the arithmetic — the part that has
/// to be right — is testable without touching process-global state.
fn rows_for_ceiling(ceiling_bytes: i64, bytes_per_row: i64) -> i64 {
    if bytes_per_row <= 0 {
        return 1_000;
    }
    (ceiling_bytes / bytes_per_row).max(1_000)
}

/// The outcome of sizing a full read down to a commit prefix.
///
/// Typed rather than `Option`, so the caller can say WHICH reason fired. Every
/// non-`Cut` outcome means "read it whole", and on a table whose window exceeds
/// the novelty ceiling that read cannot commit — an uncommitted window writes no
/// watermark, so the same read repeats on the next poll, forever. A silent
/// decline is therefore an invisible livelock, and one was: in production the
/// bound's success line never appeared across 16 consecutive passes and there was
/// nothing in the log to say why. Finding it needed a state-ledger query.
#[derive(Debug, PartialEq, Eq)]
enum FullReadCut {
    /// Cut after this commit sequence; keep every task at or below it.
    Cut(i64),
    /// The bound is switched off (`FLUREE_MATERIALIZE_MAX_ROWS_PER_FULL_PASS=0`).
    Disabled,
    /// The plan already fits the budget — not alarming, and not worth a warning.
    PlanFits,
    /// A data file carries no commit sequence, so no ordering — and therefore no
    /// cut — is safe.
    NoSequence,
    /// Every row sits in ONE commit, so there is no boundary short of the head to
    /// checkpoint at.
    SingleCommit,
}

impl FullReadCut {
    /// Operator-facing reason for declining to bound the read.
    fn reason(&self) -> &'static str {
        match self {
            Self::Cut(_) => "bounded",
            Self::Disabled => "the row bound is disabled",
            Self::PlanFits => "the plan already fits the row budget",
            Self::NoSequence => "a data file carries no commit sequence, so no cut is safe",
            Self::SingleCommit => {
                "every row is in a single commit, so there is no boundary short of the head"
            }
        }
    }

    /// Whether this outcome should be shouted about. `PlanFits` is the healthy
    /// small-read case and `Disabled` is a deliberate configuration choice;
    /// neither is news. The other two mean an unbounded read that may never
    /// commit.
    fn is_alarming(&self) -> bool {
        matches!(self, Self::NoSequence | Self::SingleCommit)
    }
}

/// The commit sequence to stop a full read at, or why it could not be cut.
///
/// Walks `tasks` (already sorted by `(data_sequence_number, path)`) accumulating
/// rows, and returns the sequence of the commit the budget ran out in. The whole
/// of that commit is kept: a cut INSIDE a commit leaves the target in a state no
/// snapshot names, and an unnameable state cannot be checkpointed.
fn full_read_prefix(tasks: &[fluree_db_iceberg::scan::FileScanTask], max_rows: i64) -> FullReadCut {
    if max_rows <= 0 {
        return FullReadCut::Disabled;
    }
    let total: i64 = tasks.iter().map(|t| t.data_file.record_count).sum();
    if total <= max_rows {
        return FullReadCut::PlanFits;
    }
    let Some(last_seq) = tasks
        .iter()
        .filter_map(|t| t.data_sequence_number)
        .next_back()
    else {
        return FullReadCut::NoSequence;
    };
    let mut rows = 0i64;
    for t in tasks {
        let Some(seq) = t.data_sequence_number else {
            return FullReadCut::NoSequence;
        };
        rows = rows.saturating_add(t.data_file.record_count);
        if rows >= max_rows {
            if seq < last_seq {
                return FullReadCut::Cut(seq);
            }
            // The budget ran out inside the HEAD commit. Checkpointing at the
            // head is the whole read with extra bookkeeping — but declining
            // outright, which is what this used to do, is how a COMPACTED table
            // livelocks. A compaction rewrites the table into files that all
            // share one sequence number, so from then on the budget always lands
            // in the head commit, the read is never bounded, and if the window
            // is over the novelty ceiling it can never commit either.
            //
            // Fall back to the newest boundary STRICTLY BELOW the head. That is
            // a smaller pass than the budget asked for, and it is still forward
            // progress, which is the only property that matters here.
            return tasks
                .iter()
                .filter_map(|t| t.data_sequence_number)
                .filter(|s| *s < last_seq)
                .max()
                .map_or(FullReadCut::SingleCommit, FullReadCut::Cut);
        }
    }
    // Unreachable: `total > max_rows` guarantees the accumulator crosses the
    // budget above. Typed as the harmless outcome rather than a panic.
    FullReadCut::PlanFits
}

/// Field ids to project for `projection` against `schema`: every non-nested
/// field when `projection` is empty, else the named columns that exist in the
/// schema (unknown names are skipped — the consumer treats them as absent).
fn projected_field_ids(
    schema: &fluree_db_iceberg::metadata::Schema,
    projection: &[String],
) -> Vec<i32> {
    if projection.is_empty() {
        schema
            .fields
            .iter()
            .filter(|f| !f.is_nested())
            .map(|f| f.id)
            .collect()
    } else {
        projection
            .iter()
            .filter_map(|col| schema.field_by_name(col).map(|f| f.id))
            .collect()
    }
}

/// Translate resolved scan filters into an Iceberg pushdown `Expression` for
/// file pruning. Filters on unknown columns are skipped; an empty result is
/// `None`. Conservative — pruning never drops matching rows because the
/// in-engine FILTER still runs.
fn build_iceberg_filter(
    filters: &[ScanFilter],
    schema: &fluree_db_iceberg::metadata::Schema,
) -> Option<Expression> {
    let mut comparisons = Vec::new();
    for f in filters {
        let Some(field) = schema.field_by_name(&f.column) else {
            continue;
        };
        // Item 7 (F-AUD-5): a set-membership filter builds `Expression::In` — the
        // backend keeps a file iff ANY member could lie in its column bounds
        // (pruning.rs). It is pushed whole or not at all: if a single member
        // cannot be represented against this physical column, the WHOLE `In` is
        // dropped (a partial `In` could prune a file a missing member's rows need).
        if f.op == ScanCmpOp::In {
            if let ScanValue::Set(members) = &f.value {
                if let Some(values) = set_members_to_literals(field.type_string(), members) {
                    comparisons.push(Expression::In {
                        field_id: field.id,
                        column: f.column.clone(),
                        values,
                    });
                }
            }
            continue;
        }
        let op = match f.op {
            ScanCmpOp::Eq => ComparisonOp::Eq,
            ScanCmpOp::NotEq => ComparisonOp::NotEq,
            ScanCmpOp::Lt => ComparisonOp::Lt,
            ScanCmpOp::LtEq => ComparisonOp::LtEq,
            ScanCmpOp::Gt => ComparisonOp::Gt,
            ScanCmpOp::GtEq => ComparisonOp::GtEq,
            // Handled above; `In` never reaches the scalar `ComparisonOp` mapping.
            ScanCmpOp::In => continue,
        };
        let Some(value) = scan_value_to_literal(field.type_string(), &f.value) else {
            continue;
        };
        comparisons.push(Expression::Comparison {
            field_id: field.id,
            column: f.column.clone(),
            op,
            value,
        });
    }
    match comparisons.len() {
        0 => None,
        1 => comparisons.into_iter().next(),
        _ => Some(Expression::And(comparisons)),
    }
}

/// Map each member of an `IN` set to an Iceberg [`LiteralValue`] against the
/// column's physical type, or `None` if ANY member cannot be represented — a
/// partial `IN` could prune a file that a dropped member's rows live in, which
/// the in-engine FILTER could then never recover. Reuses the exact scalar
/// conversion the single-value pushdown uses, so member soundness is identical.
fn set_members_to_literals(
    field_type: Option<&str>,
    members: &[ScanValue],
) -> Option<Vec<LiteralValue>> {
    let mut out = Vec::with_capacity(members.len());
    for m in members {
        out.push(scan_value_to_literal(field_type, m)?);
    }
    Some(out)
}

/// Translate one scalar [`ScanValue`] to an Iceberg pushdown [`LiteralValue`]
/// against a column of the given physical type, or `None` to skip the push
/// (leaving the in-engine FILTER as the authority). Shared by the single-value
/// `Comparison` path and the set-membership `In` path.
fn scan_value_to_literal(field_type: Option<&str>, value: &ScanValue) -> Option<LiteralValue> {
    Some(match value {
        ScanValue::Bool(b) => LiteralValue::Boolean(*b),
        // Push a Date literal only against a physically-`date` column. The
        // Arrow reader applies it as an exact row filter (casting the column
        // to text), but the operator enforces with a lenient `Date::parse`
        // that also accepts `"2024-01-15Z"` / offset forms. On a physically
        // string column the operator would keep such a row while the row
        // filter drops it — so gate the pushdown to keep it a strict subset.
        ScanValue::Date(d) => match field_type {
            Some("date") => LiteralValue::Date(*d),
            _ => return None,
        },
        // Iceberg `int` is 32-bit, `long` 64-bit; against a `decimal` column an
        // integer pushes as an EXACT scale-0 decimal when numeric pushdown is
        // on (else stays `Int64` → no prune). An out-of-i32-range literal on an
        // `int` column is skipped rather than wrapped. See `int_pushdown_literal`.
        ScanValue::Int(n) => int_pushdown_literal(*n, field_type, iceberg_numeric_stats_enabled())?,
        ScanValue::Str(s) => LiteralValue::String(s.clone()),
        // xsd:double / xsd:float FILTER value. Push only against a physically
        // `double` column (exact f64 bounds); a `float` column would need an
        // f64→f32 narrowing that can round the literal and over-prune a range,
        // so skip it — the in-engine FILTER still applies.
        ScanValue::Double(d) => match field_type {
            Some("double") => LiteralValue::Float64(*d),
            // A binary float → decimal coercion is not exact in general, so a
            // double literal is NOT pushed against a decimal column (keep is
            // correct; the in-engine FILTER enforces). Breadcrumb per the
            // decline-observably ruling.
            Some(t) if t.starts_with("decimal") => {
                debug!(
                    field_type = ?field_type,
                    "double literal vs decimal column: pushdown declined (inexact float→decimal); in-engine FILTER enforces"
                );
                return None;
            }
            _ => return None,
        },
        // xsd:decimal FILTER value. Push only against a `decimal(...)` column;
        // the literal keeps its own scale and the bound compare normalizes it
        // against the column's scale. Row-group stats prune only when the
        // column is FLBA-encoded (see `prunable_stats`); file-level manifest
        // bounds prune regardless.
        ScanValue::Decimal {
            unscaled,
            precision,
            scale,
        } => match field_type {
            Some(t) if t.starts_with("decimal") => LiteralValue::Decimal {
                unscaled: *unscaled,
                precision: *precision,
                scale: *scale,
            },
            // A decimal literal against an integer column has no exact
            // cross-type bound compare, so it is NOT pushed (keep is correct).
            // Breadcrumb per the decline-observably ruling.
            Some("int" | "long") => {
                debug!(
                    field_type = ?field_type,
                    "decimal literal vs integer column: pushdown declined (no exact cross-type bound compare); in-engine FILTER enforces"
                );
                return None;
            }
            _ => return None,
        },
        // A reversed subject-template key: coerce the raw string to the
        // column's physical type. A key that parses as an integer pushes as an
        // integer literal against an `int`/`long`/`decimal` column — including
        // a `decimal` of any scale (the Arrow reader casts the integer to the
        // column's decimal type; row-group stats conservatively skip
        // decimals). A `string` column pushes the raw string. A key that is
        // not integer-valued, or any other physical type
        // (float/date/timestamp/boolean), skips the pushdown — the operator
        // still enforces the subject equality either way.
        ScanValue::TemplateKey(s) => match field_type {
            Some("int") => match s.parse::<i32>() {
                Ok(v) => LiteralValue::Int32(v),
                Err(_) => return None,
            },
            Some(t) if t == "long" || t.starts_with("decimal") => match s.parse::<i64>() {
                Ok(v) => LiteralValue::Int64(v),
                Err(_) => return None,
            },
            Some("string") => LiteralValue::String(s.clone()),
            _ => return None,
        },
        // Item 10 (F-AUD-11): a dateTime pushes to MANIFEST-level pruning, and only
        // when the literal's frame matches the column's timestamp type — a tz-aware
        // (UTC) literal against `timestamptz`, a naive (wall-clock) literal against
        // `timestamp`. Any other pairing (frame mismatch or non-timestamp column)
        // declines: the micros would not be comparable to the file's decoded bounds,
        // and pushing anyway could over-prune. The in-engine FILTER stays authority.
        ScanValue::Timestamp { micros, tz } => match (field_type, tz) {
            (Some("timestamptz"), true) => LiteralValue::TimestampTz(*micros),
            (Some("timestamp"), false) => LiteralValue::Timestamp(*micros),
            _ => {
                debug!(
                    field_type = ?field_type,
                    tz_aware = tz,
                    "dateTime pushdown declined: literal frame does not match the column's timestamp type; in-engine FILTER enforces"
                );
                return None;
            }
        },
        // A set is never a scalar literal — the `In` path handles it member by
        // member (each member IS a scalar `ScanValue`, so this is unreachable for
        // a well-formed filter; declining defensively is never wrong).
        ScanValue::Set(_) => return None,
    })
}

// =============================================================================
// Iceberg/R2RML Graph Source Creation
// =============================================================================

impl crate::Fluree {
    /// Create an Iceberg graph source.
    ///
    /// This operation:
    /// 1. Validates the configuration
    /// 2. Optionally tests the catalog connection
    /// 3. Publishes the graph source record to the nameservice
    pub async fn create_iceberg_graph_source(
        &self,
        config: IcebergCreateConfig,
    ) -> Result<IcebergCreateResult> {
        let graph_source_id = config.graph_source_id();
        info!(
            graph_source_id = %graph_source_id,
            catalog = %config.catalog_uri_or_location(),
            table = %config.table_identifier_display(),
            "Creating Iceberg graph source"
        );

        // 1. Validate configuration
        config.validate()?;

        // 2. Test catalog connection (REST mode only — Direct mode verified at query time)
        let connection_tested = if config.is_rest() {
            let ok = self.test_iceberg_connection(&config).await.is_ok();
            if !ok {
                warn!(
                    graph_source_id = %graph_source_id,
                    "Could not verify catalog connection - graph source will be created but may fail at query time"
                );
            }
            ok
        } else {
            false
        };

        // 3. Convert config to storage format
        let iceberg_config = config.to_iceberg_gs_config();
        let config_json = iceberg_config
            .to_json()
            .map_err(|e| crate::ApiError::Config(format!("Failed to serialize config: {e}")))?;

        // 4. Publish graph source record to nameservice
        self.publisher()?
            .publish_graph_source(
                &config.name,
                config.effective_branch(),
                GraphSourceType::Iceberg,
                &config_json,
                &[], // No ledger dependencies for Iceberg graph sources
            )
            .await?;

        info!(
            graph_source_id = %graph_source_id,
            connection_tested = connection_tested,
            "Created Iceberg graph source"
        );

        Ok(IcebergCreateResult {
            graph_source_id,
            table_identifier: config.table_identifier_display(),
            catalog_uri: config.catalog_uri_or_location().to_string(),
            connection_tested,
        })
    }

    /// Create an R2RML graph source (Iceberg table with R2RML mapping).
    ///
    /// For `R2rmlMappingInput::Content`, validates the mapping content and
    /// stores it to CAS. For `R2rmlMappingInput::Address`, validates from
    /// the pre-existing storage address.
    pub async fn create_r2rml_graph_source(
        &self,
        config: R2rmlCreateConfig,
    ) -> Result<R2rmlCreateResult> {
        use crate::graph_source::config::R2rmlMappingInput;

        let graph_source_id = config.graph_source_id();
        info!(graph_source_id = %graph_source_id, "Creating R2RML graph source");

        config.validate()?;

        // Resolve mapping: validate and store to CAS if inline content
        let (mapping_address, triples_map_count, table_names, mapping_validated) = match &config
            .mapping
        {
            R2rmlMappingInput::Content(content) => {
                // Inline content has no filename to sniff; the shared resolver
                // defaults a missing media type to Turtle (matching the eventual
                // CID address, which is also extensionless).
                let compiled =
                    Self::compile_r2rml_content(content, config.mapping_media_type.as_deref(), "")?;
                let count = compiled.len();
                let tables = Self::sorted_table_names(&compiled);
                let gs_id = config.graph_source_id();
                let cs = self.content_store(&gs_id);
                let cid = cs
                    .put(
                        fluree_db_core::ContentKind::GraphSourceMapping,
                        content.as_bytes(),
                    )
                    .await
                    .map_err(|e| {
                        crate::ApiError::Config(format!("Failed to store R2RML mapping: {e}"))
                    })?;
                let addr = cid.to_string();
                info!(graph_source_id = %graph_source_id, mapping_cid = %addr, "R2RML mapping stored to CAS");
                (addr, count, tables, true)
            }
            R2rmlMappingInput::Address(address) => {
                let (count, tables, validated) = self
                        .validate_r2rml_mapping_from_address(address, &config)
                        .await
                        .map(|(c, t)| (c, t, true))
                        .unwrap_or_else(|e| {
                            warn!(graph_source_id = %graph_source_id, error = %e, "Could not validate R2RML mapping from address");
                            (0, Vec::new(), false)
                        });
                (address.clone(), count, tables, validated)
            }
        };
        let table_count = table_names.len();

        // Test catalog connection (REST mode only)
        let connection_tested = if config.iceberg.is_rest() {
            self.test_iceberg_connection(&config.iceberg).await.is_ok()
        } else {
            false
        };

        // Store config with CAS mapping address
        let iceberg_config = config.to_iceberg_gs_config(&mapping_address);
        let config_json = iceberg_config
            .to_json()
            .map_err(|e| crate::ApiError::Config(format!("Failed to serialize config: {e}")))?;

        self.publisher()?
            .publish_graph_source(
                &config.iceberg.name,
                config.iceberg.effective_branch(),
                GraphSourceType::Iceberg,
                &config_json,
                &[],
            )
            .await?;

        info!(graph_source_id = %graph_source_id, mapping_address = %mapping_address, "Created R2RML graph source");

        Ok(R2rmlCreateResult {
            graph_source_id,
            table_identifier: config.iceberg.table_identifier_display(),
            catalog_uri: config.iceberg.catalog_uri_or_location().to_string(),
            mapping_source: mapping_address,
            triples_map_count,
            table_count,
            table_names,
            connection_tested,
            mapping_validated,
        })
    }

    /// Test connection to an Iceberg REST catalog.
    ///
    /// Only applicable to REST mode. Direct mode has no catalog to test.
    async fn test_iceberg_connection(&self, config: &IcebergCreateConfig) -> Result<()> {
        use fluree_db_iceberg::catalog::parse_table_identifier;

        let rest = match &config.connection.catalog_mode {
            CatalogMode::Rest(rest) => rest,
            CatalogMode::Direct { .. } => {
                return Err(crate::ApiError::Config(
                    "Connection test is not supported for Direct catalog mode".to_string(),
                ));
            }
        };

        // Hydrate any SecretRef auth BEFORE building the provider — this is the
        // connection-test gate, so a secret reference with no resolver (or a
        // Denied resolution) must error HERE, actionably, before any network call.
        let hydrated_auth = rest
            .auth
            .hydrate(self.secret_resolver())
            .await
            .map_err(|e| {
                crate::ApiError::Config(format!("Failed to resolve catalog auth secret: {e}"))
            })?;

        // Create auth provider
        let auth = hydrated_auth
            .create_provider_arc()
            .map_err(|e| crate::ApiError::Config(format!("Failed to create auth provider: {e}")))?;

        // Create catalog client
        let catalog_config = RestCatalogConfig {
            uri: rest.catalog_uri.clone(),
            warehouse: rest.warehouse.clone(),
            ..Default::default()
        };

        let catalog = RestCatalogClient::new(catalog_config, auth).map_err(|e| {
            crate::ApiError::Config(format!("Failed to create catalog client: {e}"))
        })?;

        // Parse table identifier
        let table_id = parse_table_identifier(&config.table_identifier)
            .map_err(|e| crate::ApiError::Config(format!("Invalid table identifier: {e}")))?;

        // Attempt to load table metadata (this tests the connection)
        catalog
            .load_table(&table_id, config.connection.io.vended_credentials)
            .await
            .map_err(|e| {
                crate::ApiError::Config(format!("Failed to load table from catalog: {e}"))
            })?;

        Ok(())
    }

    /// Compile R2RML content and return the compiled mapping.
    ///
    /// `source` is the mapping's filename, storage address, or content-addressed
    /// CID; it is only consulted to infer the format when no explicit
    /// `media_type` is given. Format selection goes through the shared
    /// [`fluree_db_r2rml::loader::MappingFormat`] resolver (default Turtle) so
    /// registration and query time can never disagree (issue #1397).
    fn compile_r2rml_content(
        content: &str,
        media_type: Option<&str>,
        source: &str,
    ) -> Result<fluree_db_r2rml::mapping::CompiledR2rmlMapping> {
        use fluree_db_r2rml::loader::MappingFormat;
        match MappingFormat::resolve(media_type, source) {
            MappingFormat::Turtle => fluree_db_r2rml::loader::R2rmlLoader::from_turtle(content)
                .map_err(|e| crate::ApiError::Config(format!("Failed to parse R2RML Turtle: {e}")))?
                .compile()
                .map_err(|e| {
                    crate::ApiError::Config(format!("Failed to compile R2RML mapping: {e}"))
                }),
            MappingFormat::JsonLd => Err(crate::ApiError::Config(
                "R2RML mapping must be in Turtle format. JSON-LD is not yet supported.".into(),
            )),
        }
    }

    /// Validate an R2RML mapping from a pre-existing storage address.
    ///
    /// Returns the number of TriplesMap definitions and the sorted list of
    /// distinct logical table names referenced by the mapping.
    async fn validate_r2rml_mapping_from_address(
        &self,
        address: &str,
        config: &R2rmlCreateConfig,
    ) -> Result<(usize, Vec<String>)> {
        let storage = self.admin_storage().ok_or_else(|| {
            crate::ApiError::Config(format!(
                "Cannot load R2RML mapping from address '{address}': address-based reads are not supported on this backend"
            ))
        })?;
        let bytes = storage.read_bytes(address).await.map_err(|e| {
            crate::ApiError::Config(format!(
                "Failed to load R2RML mapping from '{address}': {e}"
            ))
        })?;
        let content = String::from_utf8(bytes).map_err(|e| {
            crate::ApiError::Config(format!("R2RML mapping is not valid UTF-8: {e}"))
        })?;
        // `address` may carry an extension (e.g. `.ttl`/`.jsonld`); pass it so the
        // resolver can infer the format when no explicit media type is set.
        let compiled =
            Self::compile_r2rml_content(&content, config.mapping_media_type.as_deref(), address)?;
        Ok((compiled.len(), Self::sorted_table_names(&compiled)))
    }

    /// Collect the distinct logical table names referenced by a compiled
    /// mapping, sorted for deterministic reporting.
    fn sorted_table_names(compiled: &CompiledR2rmlMapping) -> Vec<String> {
        let mut names: Vec<String> = compiled
            .table_names()
            .into_iter()
            .map(str::to_string)
            .collect();
        names.sort();
        names
    }
}

// =============================================================================
// R2RML Provider Implementation
// =============================================================================

/// Provider for R2RML graph source query integration.
///
/// This provider implements the `R2rmlProvider` and `R2rmlTableProvider` traits
/// required by the query engine to execute R2RML-backed queries against
/// Iceberg tables.
///
/// # Usage
///
/// ```ignore
/// use fluree_db_api::FlureeR2rmlProvider;
///
/// let provider = FlureeR2rmlProvider::new(&fluree);
/// let ctx = ExecutionContext::new(&db, &vars)
///     .with_r2rml_providers(&provider, &provider);
/// ```
pub struct FlureeR2rmlProvider<'a> {
    fluree: &'a crate::Fluree,
    /// Query-scoped catalog state. The provider is constructed once per query, so
    /// this caches the REST client (OAuth token) and `loadTable` responses for
    /// the lifetime of one query — collapsing the per-scan REST round-trip storm
    /// and pinning a single Iceberg snapshot across the query.
    session: std::sync::Arc<super::catalog_session::IcebergCatalogSession>,
}

impl<'a> FlureeR2rmlProvider<'a> {
    /// Create a new R2RML provider wrapping a Fluree instance.
    pub fn new(fluree: &'a crate::Fluree) -> Self {
        Self {
            fluree,
            session: std::sync::Arc::new(super::catalog_session::IcebergCatalogSession::default()),
        }
    }

    /// Resolve a graph source's storage backend, parsed table metadata, and
    /// metadata-location — the shared setup behind both full and incremental
    /// scans (REST/Direct × GCS/S3 × credentials × caching).
    ///
    /// TODO(dedup): `scan_table` still inlines this same setup + the
    /// `read_scan_tasks` read loop; once the incremental path is verified,
    /// refactor `scan_table` to call these helpers and drop the duplication.
    async fn prepare_iceberg_scan(
        &self,
        graph_source_id: &str,
        table_name: &str,
    ) -> QueryResult<(Arc<IcebergStorageBackend>, Arc<TableMetadata>, String)> {
        // Look up the graph source record to get Iceberg connection info
        let record = self
            .fluree
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await
            .map_err(|e| QueryError::Internal(format!("Nameservice error: {e}")))?
            .ok_or_else(|| {
                QueryError::InvalidQuery(format!("Graph source '{graph_source_id}' not found"))
            })?;

        let iceberg_config = IcebergGsConfig::from_json(&record.config).map_err(|e| {
            QueryError::Internal(format!(
                "Failed to parse Iceberg graph source config for '{graph_source_id}': {e}"
            ))
        })?;
        iceberg_config.validate().map_err(|e| {
            QueryError::InvalidQuery(format!(
                "Invalid Iceberg graph source config for '{graph_source_id}': {e}"
            ))
        })?;

        use fluree_db_iceberg::catalog::parse_table_identifier;
        use fluree_db_iceberg::config::CatalogConfig;
        use fluree_db_iceberg::SendDirectCatalogClient;

        let table_id = if !table_name.is_empty() {
            parse_table_identifier(table_name).map_err(|e| {
                QueryError::Internal(format!(
                    "Failed to parse table identifier '{table_name}': {e}"
                ))
            })?
        } else {
            iceberg_config.table_identifier().map_err(|e| {
                QueryError::Internal(format!("Failed to parse table identifier: {e}"))
            })?
        };

        // The effective Direct table location, captured for the relocated-table
        // remap inference below (None for REST).
        let mut direct_location: Option<String> = None;
        let (load_response, storage) = match &iceberg_config.catalog {
            CatalogConfig::Rest {
                uri,
                warehouse,
                auth,
                ..
            } => {
                let auth_provider = auth.create_provider_arc().map_err(|e| {
                    QueryError::Internal(format!("Failed to create auth provider: {e}"))
                })?;
                let catalog_config = RestCatalogConfig {
                    uri: uri.clone(),
                    warehouse: warehouse.clone(),
                    ..Default::default()
                };
                let catalog =
                    RestCatalogClient::new(catalog_config, auth_provider).map_err(|e| {
                        QueryError::Internal(format!("Failed to create catalog client: {e}"))
                    })?;
                let load_response = catalog
                    .load_table(&table_id, iceberg_config.io.vended_credentials)
                    .await
                    .map_err(|e| {
                        QueryError::Internal(format!("Failed to load table from catalog: {e}"))
                    })?;
                // GCS-backed tables read through this same S3 SDK path; the
                // client is pinned to HTTP/1.1 (see `S3IcebergStorage`). Vended
                // creds win, with the io config as fallback for region/endpoint/
                // path-style.
                let storage = if let Some(ref credentials) = load_response.credentials {
                    S3IcebergStorage::from_vended_credentials(
                        credentials,
                        iceberg_config.io.s3_region.as_deref(),
                        iceberg_config.io.s3_endpoint.as_deref(),
                        iceberg_config.io.s3_path_style,
                    )
                    .await
                    .map_err(|e| {
                        QueryError::Internal(format!("Failed to create S3 storage: {e}"))
                    })?
                } else {
                    S3IcebergStorage::from_default_chain(
                        iceberg_config.io.s3_region.as_deref(),
                        iceberg_config.io.s3_endpoint.as_deref(),
                        iceberg_config.io.s3_path_style,
                    )
                    .await
                    .map_err(|e| {
                        QueryError::Internal(format!("Failed to create S3 storage: {e}"))
                    })?
                };
                (load_response, Arc::new(IcebergStorageBackend::S3(storage)))
            }
            CatalogConfig::Direct { table_location } => {
                // Warehouse-root resolution — the same step the query path
                // (`load_table_context`) performs: when `table_location` is a
                // catalog-less multi-table ROOT, resolve THIS table's own
                // directory beneath it. Without this, pin resolution
                // (`current_snapshot_id`) and materialize scans read
                // `{root}/metadata/` and fail on any multi-table copy the
                // query path handles fine.
                //
                // A location whose leaf directory is NAMED AFTER the table
                // returns unchanged (that is the classifier — not "is this one
                // table"), so those sources are byte-identical. A single-table
                // location whose directory is named something else is treated
                // as a root here, exactly as the query path has always treated
                // it, so nothing that queries successfully today changes.
                let lt_key = super::catalog_session::IcebergCatalogSession::load_table_key(
                    graph_source_id,
                    &table_id.namespace,
                    &table_id.table,
                );
                let effective_location = self
                    .resolve_direct_table_location(
                        table_location,
                        &table_id,
                        &lt_key,
                        &iceberg_config,
                    )
                    .await?;
                let table_location = &effective_location;
                // The RESOLVED table dir is what the relocation remap compares
                // against the metadata's own `location` — a warehouse root would
                // never match it. Mirrors the query path's ordering.
                direct_location = Some(effective_location.clone());

                // Storage through the session, which dispatches on the location
                // scheme (a `file://` / absolute-path table reads the filesystem
                // directly, skipping the S3 client build and its credential-chain
                // resolution). Going through the session rather than building a
                // client here means the warehouse-root branch above — which
                // already built one to LIST the root — hands its client over
                // instead of having it constructed and discarded.
                let storage = direct_session_storage(
                    &self.session,
                    &lt_key,
                    table_location,
                    iceberg_config.io.s3_region.as_deref(),
                    iceberg_config.io.s3_endpoint.as_deref(),
                    iceberg_config.io.s3_path_style,
                )
                .await?;
                let cache = self.fluree.r2rml_cache();
                let load_response = if let Some(metadata_location) =
                    cache.get_direct_metadata_location(table_location).await
                {
                    fluree_db_iceberg::catalog::LoadTableResponse {
                        metadata_location,
                        config: std::collections::HashMap::default(),
                        credentials: None,
                        metadata: None,
                    }
                } else {
                    let direct_catalog =
                        SendDirectCatalogClient::new(table_location.clone(), Arc::clone(&storage));
                    let load_response =
                        direct_catalog
                            .load_table(&table_id, false)
                            .await
                            .map_err(|e| {
                                QueryError::Internal(format!(
                                    "Failed to resolve table metadata from {table_location}: {e}"
                                ))
                            })?;
                    cache
                        .put_direct_metadata_location(
                            table_location.clone(),
                            load_response.metadata_location.clone(),
                        )
                        .await;
                    load_response
                };
                (load_response, storage)
            }
        };

        let cache = self.fluree.r2rml_cache();
        let metadata_location = load_response.metadata_location.clone();
        let metadata = if let Some(cached) = cache.get_metadata(&metadata_location).await {
            cached
        } else {
            let metadata_bytes = storage
                .as_ref()
                .read(&metadata_location)
                .await
                .map_err(|e| QueryError::Internal(format!("Failed to read table metadata: {e}")))?;
            let parsed = TableMetadata::from_json(&metadata_bytes).map_err(|e| {
                QueryError::Internal(format!("Failed to parse table metadata: {e}"))
            })?;
            let metadata = Arc::new(parsed);
            cache
                .put_metadata(metadata_location.clone(), Arc::clone(&metadata))
                .await;
            metadata
        };

        // Relocated local table (copied/moved after writing): remap manifest
        // file references from the metadata's declared root to the configured
        // location. No-op for REST, S3, and locally-written tables.
        let storage = remap_local_storage(storage, &metadata, direct_location.as_deref());

        Ok((storage, metadata, metadata_location))
    }

    /// Stream a set of scan tasks as column batches, with bounded parallelism.
    ///
    /// Only `O(iceberg_scan_concurrency)` file decodes are resident at a time, so a
    /// consumer that processes and drops each batch never holds the whole table.
    /// This is the shared core of both [`Self::read_scan_tasks`] (which collects)
    /// and [`Self::scan_for_materialize_stream`] (which does not).
    fn stream_scan_tasks(
        &self,
        storage: &Arc<IcebergStorageBackend>,
        tasks: Vec<FileScanTask>,
    ) -> ColumnBatchStream {
        if tasks.is_empty() {
            return empty_batch_stream();
        }
        let footers = self.fluree.r2rml_cache().parquet_footers();
        let concurrency = iceberg_scan_concurrency(tasks.len());
        debug!(
            files = tasks.len(),
            concurrency, "streaming Parquet files (bounded parallel)"
        );
        let storage = Arc::clone(storage);
        let stream = futures::stream::iter(tasks)
            .map(move |task| {
                let storage = Arc::clone(&storage);
                let footers = Arc::clone(&footers);
                async move {
                    tokio::spawn(async move {
                        let reader =
                            SendParquetReader::with_cache(storage.as_ref(), footers.as_ref());
                        reader.read_task(&task).await.map_err(|e| {
                            QueryError::Internal(format!(
                                "Failed to read Parquet file '{}': {e}",
                                task.data_file.file_path
                            ))
                        })
                    })
                    .await
                    .map_err(|e| QueryError::Internal(format!("Parquet read worker failed: {e}")))?
                }
            })
            .buffer_unordered(concurrency)
            // One file's `Result<Vec<ColumnBatch>>` becomes individual
            // `Result<ColumnBatch>` items; a read error becomes one error item.
            .flat_map(|res: QueryResult<Vec<ColumnBatch>>| match res {
                Ok(batches) => {
                    futures::stream::iter(batches.into_iter().map(Ok).collect::<Vec<_>>())
                }
                Err(e) => futures::stream::iter(vec![Err(e)]),
            });
        Box::pin(stream)
    }

    /// Read a set of scan tasks into column batches with bounded parallelism.
    ///
    /// Collects the whole scan into memory. Prefer [`Self::stream_scan_tasks`] for
    /// anything that can process incrementally — on a wide source this `Vec` is
    /// gigabytes.
    async fn read_scan_tasks(
        &self,
        storage: &Arc<IcebergStorageBackend>,
        tasks: Vec<FileScanTask>,
    ) -> QueryResult<Vec<ColumnBatch>> {
        use futures::stream::TryStreamExt;
        self.stream_scan_tasks(storage, tasks).try_collect().await
    }

    /// The source table's current snapshot id (the materialization "to" point),
    /// or `None` if the table has no snapshots yet.
    pub async fn current_snapshot_id(
        &self,
        graph_source_id: &str,
        table_name: &str,
    ) -> QueryResult<Option<i64>> {
        let (_storage, metadata, _loc) = self
            .prepare_iceberg_scan(graph_source_id, table_name)
            .await?;
        Ok(metadata.current_snapshot().map(|s| s.snapshot_id))
    }

    /// The source graph source's materialization options from the persisted
    /// `IcebergGsConfig`: the optional tombstone/delete convention and the
    /// optional latest-by-key ordering column. Both `None` means additive,
    /// scan-order materialization (legacy behavior).
    pub async fn materialize_options(
        &self,
        graph_source_id: &str,
    ) -> QueryResult<(Option<DeleteConvention>, Option<String>)> {
        let record = self
            .fluree
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await
            .map_err(|e| QueryError::Internal(format!("Nameservice error: {e}")))?
            .ok_or_else(|| {
                QueryError::InvalidQuery(format!("Graph source '{graph_source_id}' not found"))
            })?;
        let config = IcebergGsConfig::from_json(&record.config).map_err(|e| {
            QueryError::Internal(format!(
                "Failed to parse Iceberg graph source config for '{graph_source_id}': {e}"
            ))
        })?;
        Ok((config.delete, config.order_by))
    }

    /// Scan only the data files ADDED in the snapshot window
    /// `(from_snapshot_id, to_snapshot_id]` (append-only incremental).
    ///
    /// Prefer [`Self::scan_for_materialize_stream`] with an explicit
    /// `to_snapshot_id`: it has the same explicit-window semantics but yields
    /// batches instead of collecting them, so peak memory is bounded by the
    /// scan concurrency rather than the window size. This collecting variant
    /// holds the whole window in memory (a `try_collect()` a streaming caller
    /// could have written for itself) and remains for callers that genuinely
    /// need the `Vec`.
    ///
    /// `from_snapshot_id = None` reads the full live state of `to_snapshot_id`
    /// (initial materialization). The caller must verify the window is
    /// incremental-safe (`TableMetadata::window_is_append_only`, allowing
    /// compaction) and fall back to a full scan otherwise.
    ///
    /// A snapshot id that no longer resolves in the table metadata — typically
    /// expired by the source's snapshot retention — is a typed
    /// [`QueryError::SnapshotNotFound`], never a silent fall-forward.
    pub async fn scan_table_incremental(
        &self,
        graph_source_id: &str,
        table_name: &str,
        projection: &[String],
        from_snapshot_id: Option<i64>,
        to_snapshot_id: i64,
    ) -> QueryResult<Vec<ColumnBatch>> {
        let (storage, metadata, _loc) = self
            .prepare_iceberg_scan(graph_source_id, table_name)
            .await?;

        let snapshot_err_table = if table_name.is_empty() {
            graph_source_id
        } else {
            table_name
        };
        let to_snapshot =
            metadata
                .snapshot(to_snapshot_id)
                .ok_or_else(|| QueryError::SnapshotNotFound {
                    table: snapshot_err_table.to_string(),
                    snapshot_id: to_snapshot_id,
                })?;
        let schema = metadata
            .schema_for_snapshot(to_snapshot)
            .ok_or_else(|| QueryError::Internal("Table has no current schema".to_string()))?;
        let projected_field_ids = projected_field_ids(schema, projection);

        let scan_config = ScanConfig::new().with_projection(projected_field_ids);
        let planner = SendScanPlanner::new(storage.as_ref(), &metadata, scan_config);
        let plan = planner
            .plan_incremental(from_snapshot_id, to_snapshot_id)
            .await
            .map_err(|e| match e {
                // `to` was resolved above, so the only snapshot the planner can
                // fail to find is `from` (`None` never reaches this arm: the
                // planner treats it as "since genesis" without a lookup).
                fluree_db_iceberg::IcebergError::SnapshotNotFound(_) => {
                    QueryError::SnapshotNotFound {
                        table: snapshot_err_table.to_string(),
                        snapshot_id: from_snapshot_id.unwrap_or(to_snapshot_id),
                    }
                }
                e => QueryError::Internal(format!("Failed to plan incremental scan: {e}")),
            })?;

        info!(
            from_snapshot_id = ?from_snapshot_id,
            to_snapshot_id,
            files = plan.files_selected,
            estimated_rows = plan.estimated_row_count,
            "Iceberg incremental scan plan created"
        );

        self.read_scan_tasks(&storage, plan.added_tasks).await
    }

    /// Read the rows to materialize for the window ending at `to_snapshot_id`
    /// (`None` = the source table's current snapshot), choosing incremental vs
    /// full automatically. The choice and its reason are [`ScanChoice`] — read
    /// that first, because "full" covers one routine case and one that should
    /// page someone.
    ///
    /// **Pinned reads.** An explicit `to_snapshot_id` pins the read: the
    /// window `(from, to]` and the projection schema both follow the pinned
    /// snapshot (its `schema-id`, so a historical read under schema evolution
    /// projects the columns its rows were written with). A pin that no longer
    /// resolves in the table metadata — typically expired by the source's
    /// snapshot retention — is a typed [`QueryError::SnapshotNotFound`],
    /// **never** a fall-forward: the from-side fall-forward below is a
    /// deliberate freshness/correctness trade a sync-to-head consumer wants,
    /// but silently moving a caller's `to` pin would change what the caller
    /// asked to read.
    ///
    /// Returns `(to_snapshot_id, incremental, batches)`:
    /// - `to_snapshot_id` — the resolved snapshot id of this read (the new
    ///   watermark to persist), or `None` if the table has no snapshots yet
    ///   (nothing to materialize; only possible when the caller didn't pin).
    /// - `incremental` — whether an added-files-only scan was used (vs a full
    ///   read of the live table state).
    ///
    /// An incremental scan is used when `from_snapshot_id` is set **and** the
    /// window `(from, to]` is incremental-safe — only `append`/`replace`
    /// (compaction) operations, see
    /// [`TableMetadata::window_is_incremental_safe`]. Otherwise a full scan of
    /// `to` is performed: initial materialization (`from = None`), expired or
    /// branched history, or a genuine `overwrite`/`delete` in the window (which
    /// an added-files scan would miss). This keeps routine appends and periodic
    /// compaction on the cheap incremental path while staying correct across
    /// updates/deletes.
    /// Yields batches rather than collecting them, so a consumer that processes and
    /// drops each batch never holds the whole table in memory.
    ///
    /// There is deliberately **no collecting variant of this**. One existed
    /// (`scan_for_materialize`, returning `Vec<ColumnBatch>`) and it was the only
    /// caller's default, which is how the memory problem below shipped. Removing it
    /// rather than leaving it beside this makes the memory shape structural instead
    /// of advisory — collecting is still one `try_collect()` away for a caller that
    /// genuinely needs it, but it has to be asked for.
    ///
    /// **This is the memory-shape difference that matters on a full read.** A full
    /// read is not a degradation to be avoided — it is required whenever the
    /// snapshot window contains `overwrite`/`delete`, which an added-files scan
    /// cannot see (see [`ScanChoice`]). So a deployment can legitimately full-read a
    /// large table on every poll, forever. Collecting that into a `Vec` made peak
    /// memory proportional to the *table*; streaming makes it proportional to
    /// `iceberg_scan_concurrency` file decodes.
    ///
    /// Measured on a 735,446-row source polled every ~50s: collecting drove the
    /// process to 21.4 GiB of anonymous memory against a 24 GiB limit and it was
    /// OOMKilled every 4–6 minutes, which also meant the transaction never committed
    /// and the watermark never advanced — so the next poll re-read the same table.
    pub async fn scan_for_materialize_stream(
        &self,
        graph_source_id: &str,
        table_name: &str,
        projection: &[String],
        from_snapshot_id: Option<i64>,
        to_snapshot_id: Option<i64>,
    ) -> QueryResult<MaterializeScan> {
        let (storage, metadata, _loc) = self
            .prepare_iceberg_scan(graph_source_id, table_name)
            .await?;

        let snapshot_err_table = if table_name.is_empty() {
            graph_source_id
        } else {
            table_name
        };
        let to_snapshot = match to_snapshot_id {
            // A caller's pin must resolve — typed error, never fall-forward
            // (see the doc comment).
            Some(id) => {
                Some(
                    metadata
                        .snapshot(id)
                        .ok_or_else(|| QueryError::SnapshotNotFound {
                            table: snapshot_err_table.to_string(),
                            snapshot_id: id,
                        })?,
                )
            }
            None => metadata.current_snapshot(),
        };
        let Some(to_snapshot) = to_snapshot else {
            // Table has no snapshots: nothing to materialize.
            return Ok(MaterializeScan {
                to_snapshot_id: None,
                incremental: false,
                window_age_ms: None,
                stream: empty_batch_stream(),
            });
        };
        let mut to_snapshot_id = to_snapshot.snapshot_id;

        // Schema AT the `to` snapshot (falls back to current when the snapshot
        // carries no schema-id) — identical to current for an unpinned read.
        let schema = metadata
            .schema_for_snapshot(to_snapshot)
            .ok_or_else(|| QueryError::Internal("Table has no current schema".to_string()))?;
        let projected_field_ids = projected_field_ids(schema, projection);

        let choice = ScanChoice::decide(&metadata, from_snapshot_id, to_snapshot_id);
        let incremental = choice.is_incremental();

        // Say WHY, at a level that matches how alarming it is. The routine
        // reasons are `info`; an undeterminable window is `warn` because it is
        // operator-actionable and unbounded in cost.
        match &choice {
            ScanChoice::Incremental | ScanChoice::FullInitial => {}
            ScanChoice::FullUnsafeWindow => info!(
                graph_source_id = %graph_source_id,
                table = %table_name,
                from_snapshot_id = ?from_snapshot_id,
                to_snapshot_id,
                "materialize: window contains overwrite/delete, full read required"
            ),
            ScanChoice::FullUndeterminable(reason) => warn!(
                graph_source_id = %graph_source_id,
                table = %table_name,
                from_snapshot_id = ?from_snapshot_id,
                to_snapshot_id,
                reason = %reason,
                "materialize: CANNOT DETERMINE incremental safety — falling back to a FULL \
                 table read, which loads the whole table into memory. Usually means the stored \
                 watermark snapshot has been expired by the source table's snapshot retention: \
                 either retention is shorter than this job's poll interval, or the job was \
                 stopped for longer than retention."
            ),
        }

        let scan_config = ScanConfig::new().with_projection(projected_field_ids);
        let planner = SendScanPlanner::new(storage.as_ref(), &metadata, scan_config);

        let tasks = if incremental {
            let plan = planner
                .plan_incremental(from_snapshot_id, to_snapshot_id)
                .await
                .map_err(|e| match e {
                    // Both window ends resolved before planning (`to` above,
                    // `from` inside the ScanChoice window walk), so this arm is
                    // a should-not-happen backstop — but keep it typed so a pin
                    // that expires between those checks and the manifest read
                    // still surfaces as what it is.
                    fluree_db_iceberg::IcebergError::SnapshotNotFound(_) => {
                        QueryError::SnapshotNotFound {
                            table: snapshot_err_table.to_string(),
                            snapshot_id: from_snapshot_id.unwrap_or(to_snapshot_id),
                        }
                    }
                    e => QueryError::Internal(format!("Failed to plan incremental scan: {e}")),
                })?;
            info!(
                from_snapshot_id = ?from_snapshot_id,
                to_snapshot_id,
                files = plan.files_selected,
                estimated_rows = plan.estimated_row_count,
                "materialize: incremental (added-files) scan plan"
            );
            plan.added_tasks
        } else {
            // Full read of the `to` snapshot's live state — the pinned snapshot
            // when the caller supplied one, the current snapshot otherwise
            // (`plan_scan()` is exactly `plan_scan_for_snapshot(current)`).
            let plan = planner
                .plan_scan_for_snapshot(to_snapshot)
                .await
                .map_err(|e| QueryError::Internal(format!("Failed to plan full scan: {e}")))?;
            info!(
                to_snapshot_id,
                files = plan.files_selected,
                estimated_rows = plan.estimated_row_count,
                "materialize: full scan plan"
            );

            // A full read cannot be bounded by snapshot — there is no `from` to
            // take a prefix after — so bound it by rows instead, and checkpoint
            // at the snapshot that prefix corresponds to.
            //
            // This is the path a source lands on once its watermark has expired,
            // and without a bound it is a trap: the read is the most expensive
            // one the table has, so it is the most likely to exhaust the target's
            // novelty and defer, which writes no watermark, which guarantees the
            // same read next poll. Every later poll is then the same full read,
            // forever. Bounding it means each pass finishes, writes a watermark,
            // and the one after starts incremental.
            //
            // Tasks arrive sorted by `(data_sequence_number, path)`, so a prefix
            // is a prefix in COMMIT order. The cut is then extended to the end of
            // its commit: splitting inside one would leave the target holding
            // part of a commit with no snapshot to name that state, and a
            // checkpoint that cannot be named cannot be resumed from.
            let budget_rows = materialize_max_rows_per_full_pass();
            let cut = full_read_prefix(&plan.tasks, budget_rows);
            match cut {
                FullReadCut::Cut(cut_seq) => {
                    match metadata.snapshot_at_or_before_sequence(to_snapshot_id, cut_seq) {
                        Ok(Some(checkpoint)) if checkpoint.snapshot_id != to_snapshot_id => {
                            let kept: Vec<_> = plan
                                .tasks
                                .into_iter()
                                .filter(|t| t.data_sequence_number.is_some_and(|s| s <= cut_seq))
                                .collect();
                            info!(
                                to_snapshot_id,
                                checkpoint_snapshot_id = checkpoint.snapshot_id,
                                checkpoint_sequence = cut_seq,
                                files_this_pass = kept.len(),
                                files_total = plan.files_selected,
                                "materialize: full read bounded to a commit prefix; \
                             the watermark will checkpoint short of the head"
                            );
                            // Reporting the checkpoint as this scan's `to` is what
                            // makes the caller's existing watermark write land there —
                            // no new vocabulary, and the crash-safety ordering
                            // (watermark after data) is unchanged.
                            to_snapshot_id = checkpoint.snapshot_id;
                            kept
                        }
                        // No nameable checkpoint short of the head: read it whole.
                        // Better an expensive honest read than a watermark pointing
                        // somewhere the data does not correspond to — but SAY SO,
                        // because an unbounded read over the novelty ceiling cannot
                        // commit, and what cannot commit writes no watermark and so
                        // repeats forever.
                        outcome => {
                            warn!(
                                to_snapshot_id,
                                cut_sequence = cut_seq,
                                budget_rows,
                                estimated_rows = plan.estimated_row_count,
                                files = plan.files_selected,
                                reason = match outcome {
                                    Ok(None) => "no retained snapshot names the cut",
                                    Ok(Some(_)) => "the only nameable checkpoint is the head",
                                    Err(_) => "the checkpoint walk failed",
                                },
                                "materialize: full read could NOT be bounded — reading the \
                                 whole table. If this window exceeds the novelty ceiling it \
                                 cannot commit, and an uncommitted window writes no watermark, \
                                 so this repeats on every poll"
                            );
                            plan.tasks
                        }
                    }
                }
                // Not every decline is news: `PlanFits` is the healthy small read
                // and `Disabled` is a deliberate choice. The other two mean an
                // unbounded read, which is the shape that livelocks.
                decline => {
                    if decline.is_alarming() {
                        warn!(
                            to_snapshot_id,
                            budget_rows,
                            estimated_rows = plan.estimated_row_count,
                            files = plan.files_selected,
                            reason = decline.reason(),
                            "materialize: full read could NOT be bounded — reading the whole \
                             table. If this window exceeds the novelty ceiling it cannot \
                             commit, and an uncommitted window writes no watermark, so this \
                             repeats on every poll"
                        );
                    }
                    plan.tasks
                }
            }
        };

        // How OLD is the window we are about to read? Measured from Iceberg's own
        // snapshot timestamps, so it survives restarts and needs no bookkeeping.
        //
        // The caller uses this to decide whether to persist a watermark when it
        // wrote NO data. Skipping that write is what stops the state ledger taking
        // ~1,200 empty commits/hour — but skipping it forever is worse than the
        // disease: the stored watermark eventually falls outside the source table's
        // snapshot retention, `snapshot_window` can no longer resolve it, and every
        // subsequent poll degrades to a FULL table read. That is exactly how one
        // source ended up full-reading 739k rows on every poll.
        let window_age_ms = from_snapshot_id
            .and_then(|from| metadata.snapshot(from).map(|s| s.timestamp_ms))
            .map(|from_ms| to_snapshot.timestamp_ms.saturating_sub(from_ms));

        let stream = self.stream_scan_tasks(&storage, tasks);
        Ok(MaterializeScan {
            to_snapshot_id: Some(to_snapshot_id),
            incremental,
            window_age_ms,
            stream,
        })
    }
}

impl std::fmt::Debug for FlureeR2rmlProvider<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlureeR2rmlProvider")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl R2rmlProvider for FlureeR2rmlProvider<'_> {
    /// Check if a graph source has an R2RML mapping.
    async fn has_r2rml_mapping(&self, graph_source_id: &str) -> bool {
        match self
            .fluree
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await
        {
            Ok(Some(record)) => {
                // First check if this is an R2RML or Iceberg graph source type
                if !matches!(
                    record.source_type,
                    GraphSourceType::R2rml | GraphSourceType::Iceberg
                ) {
                    return false;
                }

                // Parse into typed config to stay aligned with real config schema
                match IcebergGsConfig::from_json(&record.config) {
                    Ok(config) => config.mapping.is_some(),
                    Err(_) => false,
                }
            }
            Ok(None) => false,
            Err(_) => false,
        }
    }

    /// Get the compiled R2RML mapping for a graph source.
    ///
    /// This method uses the R2RML cache to avoid repeated parsing and compilation.
    async fn compiled_mapping(
        &self,
        graph_source_id: &str,
        _as_of_t: Option<i64>,
    ) -> QueryResult<Arc<CompiledR2rmlMapping>> {
        // Look up the graph source record
        let record = self
            .fluree
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await
            .map_err(|e| QueryError::Internal(format!("Nameservice error: {e}")))?
            .ok_or_else(|| {
                QueryError::InvalidQuery(format!("Graph source '{graph_source_id}' not found"))
            })?;

        // Verify it's an R2RML or Iceberg graph source
        if !matches!(
            record.source_type,
            GraphSourceType::R2rml | GraphSourceType::Iceberg
        ) {
            return Err(QueryError::InvalidQuery(format!(
                "Graph source '{}' is not an R2RML graph source (type: {:?})",
                graph_source_id, record.source_type
            )));
        }

        // Parse into typed config
        let iceberg_config = IcebergGsConfig::from_json(&record.config).map_err(|e| {
            QueryError::Internal(format!(
                "Failed to parse graph source config for '{graph_source_id}': {e}"
            ))
        })?;

        let mapping_config = iceberg_config.mapping.as_ref().ok_or_else(|| {
            QueryError::InvalidQuery(format!(
                "Graph source '{graph_source_id}' is missing 'mapping' in config"
            ))
        })?;

        let mapping_source = &mapping_config.source;
        let media_type = mapping_config.media_type.as_deref();

        // Check cache first
        let cache = self.fluree.r2rml_cache();
        let cache_key = R2rmlCache::mapping_cache_key(graph_source_id, mapping_source, media_type);

        if let Some(cached) = cache.get_mapping(&cache_key).await {
            debug!(
                graph_source_id = %graph_source_id,
                cache_key = %cache_key,
                "R2RML mapping cache hit"
            );
            return Ok(cached);
        }

        debug!(
            graph_source_id = %graph_source_id,
            cache_key = %cache_key,
            "R2RML mapping cache miss - loading from storage"
        );

        // Cache miss - load the mapping content.
        // Try CID-based content store first (CAS-stored mappings),
        // fall back to raw storage read (legacy address-based mappings).
        let mapping_bytes = if let Ok(cid) = mapping_source.parse::<fluree_db_core::ContentId>() {
            let cs = self.fluree.content_store(graph_source_id);
            cs.get(&cid).await.map_err(|e| {
                QueryError::InvalidQuery(format!(
                    "Failed to load R2RML mapping (CID {mapping_source}): {e}"
                ))
            })?
        } else {
            let storage = self.fluree.admin_storage().ok_or_else(|| {
                QueryError::InvalidQuery(format!(
                    "Cannot load R2RML mapping from address '{mapping_source}': address-based reads are not supported on this backend",
                ))
            })?;
            storage.read_bytes(mapping_source).await.map_err(|e| {
                QueryError::InvalidQuery(format!(
                    "Failed to load R2RML mapping from '{mapping_source}': {e}"
                ))
            })?
        };

        let mapping_content = String::from_utf8(mapping_bytes).map_err(|e| {
            QueryError::InvalidQuery(format!(
                "R2RML mapping at '{mapping_source}' is not valid UTF-8: {e}"
            ))
        })?;

        // Parse and compile the mapping. Format selection goes through the same
        // shared resolver the registration path uses, so a mapping stored
        // without an explicit media type (e.g. a CAS CID) defaults to Turtle
        // here too instead of erroring as JSON-LD (issue #1397).
        use fluree_db_r2rml::loader::MappingFormat;
        let compiled = match MappingFormat::resolve(media_type, mapping_source) {
            MappingFormat::Turtle => {
                fluree_db_r2rml::loader::R2rmlLoader::from_turtle(&mapping_content)
                    .map_err(|e| {
                        QueryError::InvalidQuery(format!(
                            "Failed to parse R2RML Turtle from '{mapping_source}': {e}"
                        ))
                    })?
                    .compile()
                    .map_err(|e| {
                        QueryError::InvalidQuery(format!(
                            "Failed to compile R2RML mapping from '{mapping_source}': {e}"
                        ))
                    })?
            }
            MappingFormat::JsonLd => {
                return Err(QueryError::InvalidQuery(format!(
                    "R2RML mapping for '{graph_source_id}' uses JSON-LD format, which is not yet supported. \
                     Please use Turtle format (.ttl)."
                )));
            }
        };

        let compiled = Arc::new(compiled);

        // Cache the compiled mapping
        cache
            .put_mapping(cache_key.clone(), Arc::clone(&compiled))
            .await;

        info!(
            graph_source_id = %graph_source_id,
            cache_key = %cache_key,
            triples_maps = compiled.triples_maps.len(),
            "Loaded, compiled, and cached R2RML mapping"
        );

        Ok(compiled)
    }

    /// Report the per-table build watermark from this provider's catalog session
    /// (DEC-003). Every scan of a table this build touches records its pinned
    /// snapshot into `self.session` (see `scan_table_inner`); this surfaces those
    /// captures as `{table → snapshot}` for `graph_source_id`. The bulk builder
    /// fails loud if this is empty for a non-empty table set, so a provider that
    /// never scanned (or a session that lost its pins) cannot publish an unstamped
    /// twin.
    fn build_watermark(
        &self,
        graph_source_id: &str,
    ) -> std::collections::HashMap<String, fluree_db_query::r2rml::TableWatermark> {
        self.session.pinned_tables(graph_source_id)
    }

    /// MAJOR-2 (#1529 review): refuse a materialize build whose stamped watermark
    /// could not be trusted. (1) With the loadTable metadata cache disabled the pin
    /// machinery is a documented no-op, so no snapshot is actually held for the
    /// build. (2) If any table yielded a second distinct `metadata_location` during
    /// the build, the source committed mid-build and the twin would be stamped with a
    /// snapshot it does not contain. Checked at the build's start (cache) and again
    /// before the completion stamp (conflicts).
    fn verify_build_snapshot_integrity(
        &self,
        graph_source_id: &str,
    ) -> std::result::Result<(), fluree_db_r2rml::R2rmlError> {
        use fluree_db_r2rml::R2rmlError;
        if !super::catalog_session::cache_enabled() {
            return Err(R2rmlError::BuildSnapshotIntegrity(
                "the loadTable metadata cache is disabled (FLUREE_ICEBERG_LOADTABLE_CACHE=0), so \
                 Iceberg snapshot pinning is a no-op and the twin's stamped watermark cannot be \
                 guaranteed to describe its contents; re-enable the cache to materialize"
                    .to_string(),
            ));
        }
        let conflicts = self.session.observed_snapshot_conflicts(graph_source_id);
        if let Some((table, first, second)) = conflicts.first() {
            return Err(R2rmlError::BuildSnapshotIntegrity(format!(
                "table '{table}' moved snapshots during the build (read metadata_location \
                 '{first}' then '{second}'): the source committed mid-build, so the twin's stamped \
                 watermark would not describe its contents. Re-run against a quiesced source.{}",
                if conflicts.len() > 1 {
                    format!(" ({} tables affected)", conflicts.len())
                } else {
                    String::new()
                }
            )));
        }
        Ok(())
    }
}

/// Bounded concurrency for warming per-table catalog contexts in
/// [`FlureeR2rmlProvider::prefetch_tables`] (PR-8 slice 1). Matches the
/// generate-path preview fan-out (`0ade90c59`); the catalog-request semaphore
/// (PR-8 slice 3) is the global Horizon-QPS bound, this is the per-query width.
const CATALOG_PREFETCH_CONCURRENCY: usize = 8;

#[async_trait]
impl R2rmlTableProvider for FlureeR2rmlProvider<'_> {
    /// Scan an Iceberg table, streaming column batches as data files are read.
    ///
    /// This method connects to the Iceberg catalog, plans the scan with the
    /// specified projection/filters, and returns a [`ColumnBatchStream`] that
    /// yields one file's batches at a time (bounded-parallel reads) so a
    /// streaming consumer never holds the whole table in memory.
    async fn scan_table(
        &self,
        graph_source_id: &str,
        table_name: &str,
        projection: &[String],
        filters: &[ScanFilter],
        topk: Option<&ScanTopK>,
        _as_of_t: Option<i64>,
    ) -> QueryResult<ColumnBatchStream> {
        // Time the whole scan SETUP (loadTable + planning) as one span; it closes
        // when the stream is constructed. Per-file Parquet decode happens later,
        // while the returned stream is consumed, and is timed separately by the
        // `iceberg.parquet_read` spans, so a bare wrapper here would not (and must
        // not) cover decode.
        let span = tracing::debug_span!(
            "r2rml.scan_table",
            graph_source_id,
            table_name,
            projection_len = projection.len()
        );
        self.scan_table_inner(
            graph_source_id,
            table_name,
            projection,
            filters,
            topk,
            _as_of_t,
        )
        .instrument(span)
        .await
    }

    /// The table's exact live row count from the pinned Iceberg manifest — **only
    /// when it provably equals a full-scan `COUNT(*)`** (see the trait contract).
    ///
    /// Resolves the SAME per-query pinned table context the scan uses (via
    /// [`Self::load_table_context`], sharing `self.session`), so a `COUNT` and a
    /// scan in one query read one Iceberg snapshot. It then reads that snapshot's
    /// manifest-list + manifest Avro (never a Parquet/data file), and returns the
    /// `record_count` sum only if [`sound_manifest_row_count`] proves it equals a
    /// full scan: no delete manifests, and every `non_null_col` provably zero-null.
    /// Otherwise `Ok(None)` and the caller falls back to the scan.
    async fn table_row_count(
        &self,
        graph_source_id: &str,
        table_name: &str,
        non_null_cols: &[String],
        _as_of_t: Option<i64>,
    ) -> QueryResult<Option<u64>> {
        // Time the manifest-only read as one span (the same `.instrument` split as
        // `scan_table` / `scan_table_inner`). `fired` records answered (true) vs
        // declined-to-scan (false). The name is allowlisted in
        // `fluree-bench-virtual::spans`, so the vbench pathway counters show the
        // shortcut directly instead of inferring it from `files_selected=0` plus
        // scan-span absence.
        let span = tracing::debug_span!(
            "r2rml.count_manifest",
            graph_source_id,
            table_name,
            fired = tracing::field::Empty
        );
        self.table_row_count_inner(graph_source_id, table_name, non_null_cols, _as_of_t)
            .instrument(span)
            .await
    }

    /// Warm the per-query catalog session pin + cross-query caches for a set of
    /// tables concurrently (PR-8 slice 1). Best-effort and side-effect-only: each
    /// `load_table_context` populates `self.session` + the moka caches, so the
    /// query's following *serial* scans resolve from the pin and skip the
    /// `loadTable` GET. Resolution errors are intentionally swallowed — the real
    /// scan re-resolves and surfaces them — so a warm failure degrades to today's
    /// serial GET, never a changed result.
    async fn prefetch_tables(&self, graph_source_id: &str, table_names: &[String]) {
        // Dedup, preserving first-seen order, AND skip tables already resolved
        // (with unexpired creds) in this query's session pin — re-warming a
        // pinned table would issue a wasted `loadTable` GET. Collect OWNED names:
        // a `Vec<&str>` here makes the `buffered` fan-out closure take a borrowed
        // argument, which trips rustc's "FnOnce is not general enough" HRTB check.
        let mut seen = std::collections::HashSet::new();
        let mut to_warm: Vec<String> = Vec::new();
        for t in table_names {
            if seen.insert(t.as_str()) && !self.is_table_pinned(graph_source_id, t) {
                to_warm.push(t.clone());
            }
        }

        // Engagement + measurement span (allowlisted as `r2rml.prefetch`): its
        // presence proves the prefetch path ran, and `warmed`/`requested` show the
        // fan-out width vs how many were skipped as already-pinned. Emitted even
        // for a no-op fan-out so "ran but skipped" is distinguishable from
        // "never ran".
        let span = tracing::debug_span!(
            "r2rml.prefetch",
            requested = table_names.len(),
            warmed = to_warm.len(),
        );
        if to_warm.len() < 2 {
            // Nothing to overlap. Enter/drop the span (no `.await` under it) so a
            // no-op prefetch is still visible in the counters.
            let _entered = span.entered();
            return;
        }
        // `buffered` polls these futures COOPERATIVELY on one task (no spawn), and
        // the REST-client build inside `load_table_context` is synchronous, so the
        // first future polled builds + caches the process-wide client before any
        // other future resumes past its (async) nameservice lookup — every later
        // table then reuses that one client and its cached OAuth token. Verified
        // live: a cold 3-table fan-out does exactly ONE `iceberg.oauth_token`
        // exchange, not one per table. (If the client build ever becomes async,
        // this dedup breaks and a serial first-table warm would be needed.)
        //
        // The `buffered` width here is the per-query fan-out ceiling; the true
        // bound on concurrent catalog QPS is the process-wide catalog-request
        // semaphore (PR-8 slice 3, `rest.rs`), which every `loadTable` GET this
        // fan-out issues must acquire — so the prefetch cannot defeat the 429
        // protection it runs ahead of, and a lower `FLUREE_ICEBERG_CATALOG_CONCURRENCY`
        // transparently throttles it.
        futures::stream::iter(to_warm)
            .map(|table| async move {
                let _ = self.load_table_context(graph_source_id, &table).await;
            })
            .buffered(CATALOG_PREFETCH_CONCURRENCY)
            .for_each(|()| async {})
            .instrument(span)
            .await;
    }
}

/// Build the catalog auth provider for a REST client, hydrating any
/// `ConfigValue::SecretRef` first (§3 of fluree/db#1500).
///
/// **Every query-path provider build goes through this**, never through a bare
/// `create_provider_arc`: that resolves `ConfigValue`s SYNCHRONOUSLY, so a `SecretRef`
/// reaching it un-hydrated hard-errors via `resolve()`'s fail-closed arm. Pairing the
/// two here means a NEW client-construction site cannot silently ship without
/// hydration — which is exactly how the pointer rung's copy arrived. (The one
/// deliberate exception is `test_iceberg_connection`, which is `ApiError`-typed and
/// hydrates inline so a missing/denied resolver surfaces as an actionable *config*
/// error at the connection-test gate rather than a query error.)
///
/// ⚠️ ORDERING: the caller MUST already have computed its `rest_client_cache_key`
/// fingerprint over the RAW, reference-bearing config BEFORE calling this. Hydrating
/// before the fingerprint re-keys the client cache on every secret rotation, turning
/// one resolver call + OAuth exchange per client lifetime into one PER QUERY. Pinned
/// by `secret_ref_fingerprint_is_rotation_stable`.
async fn hydrated_auth_provider(
    auth: &fluree_db_iceberg::auth::AuthConfig,
    resolver: Option<&Arc<dyn fluree_db_iceberg::SecretResolver>>,
) -> QueryResult<Arc<dyn fluree_db_iceberg::auth::SendCatalogAuth>> {
    let hydrated = auth
        .hydrate(resolver)
        .await
        .map_err(|e| QueryError::Internal(format!("Failed to resolve catalog auth secret: {e}")))?;
    hydrated
        .create_provider_arc()
        .map_err(|e| QueryError::Internal(format!("Failed to create auth provider: {e}")))
}

/// Resolve the S3 storage client for a REST table: the §2 credential decision, the
/// query-session reuse (#1498), and the vended/ambient construction — in ONE place.
///
/// **Both** the eager `load_table_context` arm and the deferred [`LazyS3Storage`]
/// builder (via [`resolve_rest_load_and_storage`]) construct storage for the same
/// table under the same config, so they MUST make the same credential decision. They
/// were two independent copies; the copy the deferred builder used silently omitted
/// §2's fail-closed check, which reopened the ambient downgrade on any lazy-forced
/// build. Keep them behind this one helper — a second copy is a fail-open waiting to
/// happen.
///
/// §2: a source configured for vended credentials (`io.vended_credentials = true`,
/// the default) whose catalog vended none ERRORS — it must never silently fall back
/// to the process's ambient AWS identity. Ambient is reachable only via an explicit
/// `vended_credentials = false`. REST only: Direct never requests vending and has its
/// own [`direct_session_storage`].
async fn rest_session_storage(
    session: &super::catalog_session::IcebergCatalogSession,
    lt_key: &str,
    io: &IoConfig,
    load_response: &LoadTableResponse,
    catalog_uri: &str,
) -> QueryResult<Arc<IcebergStorageBackend>> {
    // The loadTable caches preserve `credentials` verbatim and only ever
    // MISS-and-reload on expiry, so a resolved `credentials == None` always means
    // the catalog genuinely vended nothing — never a dropped-credential
    // reconstruction. Safe to fail closed on.
    if decide_credential_source(
        io.vended_credentials,
        load_response.credentials.is_some(),
        true,
    ) == CredentialSource::FailClosed
    {
        return Err(QueryError::CatalogCredentialsNotVended {
            catalog_uri: catalog_uri.to_string(),
        });
    }

    // Reuse the query session's cached S3 client for this table when one is present:
    // constructing it (`aws_config` load + S3 client + HTTP client) is not free, and a
    // correlated join — or the slice-1 prefetch→scan — resolves the same table
    // repeatedly. Any fresh loadTable dropped the entry via `store_load_table`, so a
    // hit here always corresponds to the current pinned credentials.
    if let Some(cached) = session.cached_storage(lt_key) {
        debug!("S3 storage client reused (query-scoped)");
        return Ok(cached);
    }

    // GCS-backed tables (S3-interop endpoint) are read through this same S3 SDK path;
    // the SDK client is pinned to HTTP/1.1 so the GCS HTTP/2 range-read bug cannot
    // occur.
    let built = if let Some(ref credentials) = load_response.credentials {
        info!(
            region = ?io.s3_region,
            endpoint = ?io.s3_endpoint,
            "Using vended credentials from catalog"
        );
        // Thread the io overrides so a catalog that omits the region (or where we want
        // an operator-configured endpoint/path-style) still resolves correctly.
        // Precedence inside the call: vended > these overrides > SDK.
        S3IcebergStorage::from_vended_credentials(
            credentials,
            io.s3_region.as_deref(),
            io.s3_endpoint.as_deref(),
            io.s3_path_style,
        )
        .await
        .map_err(|e| QueryError::Internal(format!("Failed to create S3 storage: {e}")))?
    } else {
        // Reachable only under an explicit `vended_credentials = false` (§2 returned
        // above otherwise).
        info!(
            region = ?io.s3_region,
            endpoint = ?io.s3_endpoint,
            "Using ambient AWS credentials"
        );
        S3IcebergStorage::from_default_chain(
            io.s3_region.as_deref(),
            io.s3_endpoint.as_deref(),
            io.s3_path_style,
        )
        .await
        .map_err(|e| QueryError::Internal(format!("Failed to create S3 storage: {e}")))?
    };
    let built = Arc::new(IcebergStorageBackend::S3(built));
    session.store_storage(lt_key.to_string(), Arc::clone(&built));
    Ok(built)
}

/// Resolve `loadTable` (the REST catalog GET, honoring the query pin + the
/// cross-query cache) and build the vended-credential S3 storage — the SAME
/// session-pin path the eager scan used before the lazy split (PR-8 loadTable-
/// metadata cache, `21-loadtable-metadata-cache.md`).
///
/// Takes OWNED handles (Arcs + owned config) so it can be called BOTH eagerly and
/// from the deferred [`LazyS3Storage`] builder — which must be `'static`, because
/// the Parquet reads `Arc::clone` the storage into `tokio::spawn` (in
/// `fluree-db-iceberg/src/io/send_parquet.rs`) and the provider holds `fluree: &'a`
/// (a borrow that cannot cross the spawn). A future refactor that narrows this back
/// to `&'a` will not fail here — it surfaces as a `'static`-bound lifetime error at
/// that spawn site, so the ownership is load-bearing, not incidental. This runs ONLY when
/// a real S3 read is needed: its `r2rml.load_table` span, and the REST client's
/// first-use OAuth token exchange, are exactly what the deterministic cache gate
/// proves absent (`load_table.n=0` AND `oauth_token.n=0`) on a fully disk-cached
/// query. No duplicate ladder — this IS the ladder both paths share.
async fn resolve_rest_load_and_storage(
    catalog: Arc<RestCatalogClient>,
    cache: Arc<R2rmlCache>,
    session: Arc<super::catalog_session::IcebergCatalogSession>,
    table_id: TableIdentifier,
    io: IoConfig,
    lt_key: String,
    catalog_uri: String,
) -> QueryResult<(Arc<IcebergStorageBackend>, LoadTableResponse)> {
    // (1) Resolve the loadTable response, cheapest first: the per-query pin, then
    // the cross-query cache (skips the ~1–3s GET), then a real REST load.
    let load_response = if let Some(cached) = session.cached_load_table(&lt_key) {
        cached
    } else {
        let pinned = session.pinned_metadata_location(&lt_key);
        let cross_query = if pinned.is_none() {
            cache.get_rest_load_table(&lt_key)
        } else {
            None
        };
        let mut resp = if let Some(cq) = cross_query {
            cq.to_response()
        } else {
            // The cold REST/OAuth catalog round-trip — the `r2rml.load_table` span
            // the cache gate proves absent when disk-warm.
            let actual = catalog
                .load_table(&table_id, io.vended_credentials)
                .instrument(tracing::debug_span!(
                    "r2rml.load_table",
                    namespace = %table_id.namespace,
                    table = %table_id.table,
                ))
                .await
                .map_err(|e| {
                    QueryError::Internal(format!("Failed to load table from catalog: {e}"))
                })?;
            cache.put_rest_load_table(
                lt_key.clone(),
                Arc::new(super::catalog_session::CachedLoadTable::from_response(
                    &actual,
                )),
            );
            let mut r = actual;
            if let Some(ref pinned_loc) = pinned {
                if *pinned_loc != r.metadata_location {
                    r.metadata_location = pinned_loc.clone();
                }
            }
            r
        };
        session.store_load_table(lt_key.clone(), &resp);
        if let Some(pinned_loc) = session.pinned_metadata_location(&lt_key) {
            resp.metadata_location = pinned_loc;
        }
        resp
    };

    // (2) Build (or reuse the query session's) S3 storage — §2 fail-closed decision
    // and session reuse included, shared verbatim with the eager arm.
    let storage =
        rest_session_storage(&session, &lt_key, &io, &load_response, &catalog_uri).await?;

    Ok((storage, load_response))
}

impl FlureeR2rmlProvider<'_> {
    /// Body of [`R2rmlTableProvider::table_row_count`], split out so the trait
    /// method can wrap it in the `r2rml.count_manifest` timing span via
    /// `.instrument()` (the same pattern as [`Self::scan_table_inner`]).
    async fn table_row_count_inner(
        &self,
        graph_source_id: &str,
        table_name: &str,
        non_null_cols: &[String],
        _as_of_t: Option<i64>,
    ) -> QueryResult<Option<u64>> {
        // Same pinned context as the scan: one Iceberg snapshot per query (the
        // shared `self.session` pin), so a count and a scan cannot disagree.
        // GREP: r2rml-as-of-t — `as_of_t` is ignored here exactly as the scan path
        // ignores it (matching breadcrumb in `scan_table_inner`); if time-travel
        // semantics ever land on the scan, this method MUST follow, or a COUNT and
        // a scan in one query could answer from different snapshots.
        let (storage, metadata, metadata_location) =
            self.load_table_context(graph_source_id, table_name).await?;

        // The count must equal a full scan of THIS snapshot — the one the scan
        // planner reads from the same pinned metadata. No current snapshot (an
        // empty table) or no current schema: decline and let the scan handle it (an
        // empty scan folds to 0; a missing schema surfaces the scan's own error).
        let (Some(snapshot), Some(schema)) =
            (metadata.current_snapshot(), metadata.current_schema())
        else {
            return Ok(None);
        };

        // Manifest-only read (never a Parquet/data file): the live data files, and
        // whether the snapshot carries merge-on-read delete manifests.
        //
        // PR-8 slice 2: this manifest read (the COUNT(*) path's, ~450ms cold) is
        // keyed by the content-addressed `metadata_location`, so persist it to the
        // disk catalog cache and serve it from there on a warm-catalog cold
        // process (no S3 read, no `r2rml.count_manifest_read` span).
        //
        // Measurement sub-span (PR-8 cold decomposition): the COUNT(*) path's
        // manifest-list + manifest read (the scan path's equivalent is
        // `iceberg.scan_plan`). For a bare `COUNT(*)` (q036) this plus
        // `r2rml.load_table` + `r2rml.read_metadata` accounts for the entire cold
        // wall — no data file is read. Allowlisted in `fluree-bench-virtual::spans`.
        let catalog_cache = self.catalog_disk_cache();
        let (data_files, has_delete_manifests) = if let Some(hit) =
            catalog_cache.get_count_stats(&metadata_location)
        {
            debug!(table_name = %table_name, "COUNT(*) manifest stats disk-cache hit");
            hit
        } else {
            let (data_files, _manifests_read, has_delete_manifests) =
                send_read_snapshot_data_files(storage.as_ref(), snapshot)
                    .instrument(tracing::debug_span!(
                        "r2rml.count_manifest_read",
                        table_name
                    ))
                    .await
                    .map_err(|e| {
                        storage_query_error(
                            &format!("Failed to read manifests for row count of '{table_name}'"),
                            e,
                        )
                    })?;
            catalog_cache.put_count_stats(&metadata_location, &data_files, has_delete_manifests);
            (data_files, has_delete_manifests)
        };

        let count =
            sound_manifest_row_count(schema, &data_files, has_delete_manifests, non_null_cols);
        // Recorded on the `r2rml.count_manifest` span wrapping this body.
        tracing::Span::current().record("fired", count.is_some());
        match count {
            Some(n) => debug!(
                table_name = %table_name,
                count = n,
                non_null_cols = non_null_cols.len(),
                "COUNT(*) manifest shortcut: answered from manifest record_count sum"
            ),
            None => debug!(
                table_name = %table_name,
                has_delete_manifests,
                "COUNT(*) manifest shortcut declined; falling back to scan"
            ),
        }
        Ok(count)
    }

    /// Whether `table_name` is already resolved (with unexpired credentials) in
    /// this query's session pin, so [`R2rmlTableProvider::prefetch_tables`] can
    /// skip re-warming it. A name that fails to parse is reported as NOT pinned so
    /// prefetch still attempts it and the real scan surfaces any error.
    fn is_table_pinned(&self, graph_source_id: &str, table_name: &str) -> bool {
        use fluree_db_iceberg::catalog::parse_table_identifier;
        let Ok(id) = parse_table_identifier(table_name) else {
            return false;
        };
        let key = super::catalog_session::IcebergCatalogSession::load_table_key(
            graph_source_id,
            &id.namespace,
            &id.table,
        );
        self.session.is_pinned(&key)
    }

    /// The persistent on-disk catalog cache (PR-8 slice 2), rooted in a dedicated
    /// dir sibling to this instance's Parquet/binary artifact cache so the cold
    /// benchmark protocol can clear data while keeping catalog persistence. Cheap
    /// to build per call (a `create_dir_all` that no-ops once the dir exists).
    fn catalog_disk_cache(&self) -> super::disk_catalog_cache::DiskCatalogCache {
        let artifact_dir = self.fluree.binary_store_cache_dir();
        super::disk_catalog_cache::DiskCatalogCache::for_dir(
            &super::disk_catalog_cache::catalog_cache_dir(&artifact_dir),
        )
    }

    /// Resolve a graph source down to its pinned Iceberg table context: the S3
    /// storage, the (metadata-location-pinned) [`TableMetadata`], and that metadata
    /// location. Shared by [`Self::scan_table_inner`] and
    /// [`R2rmlTableProvider::table_row_count`] so a `COUNT` and a scan in the same
    /// query read ONE Iceberg snapshot — the whole `loadTable` resolution (the
    /// per-query snapshot pin in [`super::catalog_session::IcebergCatalogSession`]
    /// plus the cross-query / metadata caches) runs here, through the shared
    /// `self.session`, exactly as the scan did before this was extracted. It
    /// excludes the scan-only concerns — the "Starting Iceberg table scan" log and
    /// the Parquet disk cache — which stay in `scan_table_inner`.
    /// Warm the in-memory + disk metadata caches for `metadata_location` WITHOUT
    /// any S3 read — the in-memory moka first, then the persistent slice-2 disk
    /// cache (warming the moka layer on a disk hit). `None` when neither has it (a
    /// real S3 read would be required, which the caller does via `storage`). Used
    /// by the pointer rung to serve metadata with zero storage.
    async fn metadata_from_caches(&self, metadata_location: &str) -> Option<Arc<TableMetadata>> {
        let cache = self.fluree.r2rml_cache();
        if let Some(m) = cache.get_metadata(metadata_location).await {
            return Some(m);
        }
        if let Some(m) = self.catalog_disk_cache().get_metadata(metadata_location) {
            cache
                .put_metadata(metadata_location.to_string(), Arc::clone(&m))
                .await;
            return Some(m);
        }
        None
    }

    /// Resolve the effective table location for a Direct-mode scan. When
    /// `table_location` is a warehouse ROOT (a catalog-less multi-table copy) —
    /// detected because its leaf directory does not name the requested table —
    /// LIST the root once (session-cached, one LIST per build) and match THIS
    /// table's own `<name>.<suffix>/` (or bare `<name>/`) directory beneath it,
    /// case-insensitively on the name part. Ambiguity or a miss fails loud, naming
    /// the candidates. Otherwise (a single-table direct location) returns
    /// `table_location` unchanged, so the caller's resolution is byte-identical.
    ///
    /// Detection is pure string logic; a table literally named after its parent
    /// directory reads as single-table (documented in `iceberg map --help`).
    async fn resolve_direct_table_location(
        &self,
        table_location: &str,
        table_id: &TableIdentifier,
        lt_key: &str,
        iceberg_config: &IcebergGsConfig,
    ) -> QueryResult<String> {
        let trimmed = table_location.trim_end_matches('/');
        let leaf = trimmed.rsplit('/').next().unwrap_or("");
        let leaf_name = leaf.split('.').next().unwrap_or("");
        let requested = table_id.table.trim();
        let is_warehouse = !requested.is_empty() && !leaf_name.eq_ignore_ascii_case(requested);
        if !is_warehouse {
            return Ok(table_location.to_string());
        }

        let root = trimmed.to_string();
        let listing = match self.session.cached_warehouse_listing(&root) {
            Some(l) => l,
            None => {
                let storage = direct_session_storage(
                    &self.session,
                    lt_key,
                    &root,
                    iceberg_config.io.s3_region.as_deref(),
                    iceberg_config.io.s3_endpoint.as_deref(),
                    iceberg_config.io.s3_path_style,
                )
                .await?;
                let dirs = storage.list_dir(&root).await.map_err(|e| {
                    storage_query_error(&format!("Failed to LIST warehouse root {root}"), e)
                })?;
                info!(
                    warehouse_root = %root,
                    tables_found = dirs.len(),
                    "Listed catalog-less warehouse root (direct mode)"
                );
                self.session.store_warehouse_listing(root.clone(), dirs)
            }
        };

        let matched =
            match fluree_db_iceberg::catalog::match_warehouse_table_dir(requested, &listing) {
                Ok(m) => m,
                Err(e) => {
                    // MAJOR-3 (#1529 review): the leaf-name heuristic (split on the
                    // FIRST '.') can misclassify a legitimate SINGLE-table direct
                    // location as a warehouse root — a table dir suffixed like
                    // `orders-1a2b3c4d`, or a dir literally `DW.FACT_ORDER`. A real
                    // Iceberg table directory has a `metadata/` child; if the listing
                    // has one, this IS a single table, so return the location
                    // unchanged rather than fail with a confusing "no directory
                    // matches" error.
                    if listing_is_single_table(&listing) {
                        return Ok(table_location.to_string());
                    }
                    return Err(QueryError::InvalidQuery(format!(
                        "warehouse-root resolution for table '{}': {e}",
                        table_id.table
                    )));
                }
            };
        Ok(format!("{root}/{matched}"))
    }

    async fn load_table_context(
        &self,
        graph_source_id: &str,
        table_name: &str,
    ) -> QueryResult<(Arc<LazyS3Storage<'static>>, Arc<TableMetadata>, String)> {
        // Look up the graph source record to get Iceberg connection info
        let record = self
            .fluree
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await
            .map_err(|e| QueryError::Internal(format!("Nameservice error: {e}")))?
            .ok_or_else(|| {
                QueryError::InvalidQuery(format!("Graph source '{graph_source_id}' not found"))
            })?;

        // Parse the Iceberg graph source config
        let iceberg_config = IcebergGsConfig::from_json(&record.config).map_err(|e| {
            QueryError::Internal(format!(
                "Failed to parse Iceberg graph source config for '{graph_source_id}': {e}"
            ))
        })?;

        // Validate the config
        iceberg_config.validate().map_err(|e| {
            QueryError::InvalidQuery(format!(
                "Invalid Iceberg graph source config for '{graph_source_id}': {e}"
            ))
        })?;

        // Branch on catalog mode: REST vs Direct
        use fluree_db_iceberg::config::CatalogConfig;
        use fluree_db_iceberg::SendDirectCatalogClient;

        // Parse the table identifier
        use fluree_db_iceberg::catalog::parse_table_identifier;
        let table_id = if !table_name.is_empty() {
            parse_table_identifier(table_name).map_err(|e| {
                QueryError::Internal(format!(
                    "Failed to parse table identifier '{table_name}': {e}"
                ))
            })?
        } else {
            iceberg_config.table_identifier().map_err(|e| {
                QueryError::Internal(format!("Failed to parse table identifier: {e}"))
            })?
        };

        // PR-8 loadTable-metadata cache: before building storage, try to resolve
        // this table WITHOUT the ~1–3s loadTable REST GET. The persisted pointer
        // (credential-free) gives the `metadata_location`; slice-2 gives the parsed
        // metadata — both with zero S3. When they hit, storage construction (and
        // thus the GET + its OAuth token exchange) is DEFERRED behind a
        // `LazyS3Storage` and never forced if every Parquet file is also disk-
        // cached → the deterministic gate's `load_table.n=0` AND `oauth_token.n=0`.
        // REST only; Direct mode has its own metadata-location cache.
        let lt_key = super::catalog_session::IcebergCatalogSession::load_table_key(
            graph_source_id,
            &table_id.namespace,
            &table_id.table,
        );
        let disk = self.catalog_disk_cache();
        if let CatalogConfig::Rest {
            uri,
            warehouse,
            auth,
            ..
        } = &iceberg_config.catalog
        {
            // A snapshot pin from an EARLIER touch of this table THIS query wins
            // unconditionally over the disk pointer (correlated re-loads must read
            // ONE snapshot). Only when unpinned do we consult the disk pointer.
            // `None` = a latest-snapshot read. GREP: r2rml-as-of-t — when Iceberg
            // snapshot time-travel lands, the requested snapshot's `timestamp_ms`
            // MUST be threaded here as `min_snapshot_ms` so bounded staleness can
            // never downgrade a time-travel request (the guard + hermetic already
            // exist in `disk_catalog_cache::pointer_is_usable`).
            let candidate = self
                .session
                .pinned_metadata_location(&lt_key)
                .or_else(|| disk.get_metadata_location(&lt_key, None));
            if let Some(loc) = candidate {
                if let Some(md) = self.metadata_from_caches(&loc).await {
                    // Pin the location BEFORE serving: a later touch whose disk
                    // pointer has since expired (TTL boundary mid-query) then still
                    // resolves THIS snapshot via `pinned_metadata_location` — the
                    // eager path reloads only credentials and re-pins here. Without
                    // this, the never-forced metadata path could read two snapshots
                    // in one query (the pin contract, `catalog_session.rs`).
                    self.session
                        .pin_metadata_location(lt_key.clone(), loc.clone());
                    // Build/get the process-wide REST client. Client construction
                    // does NO network; the OAuth token exchange rides the first
                    // catalog op (the loadTable GET, deferred) — a property ENFORCED
                    // by the cache gate's `oauth_token.n=0`, which fails forever if
                    // construction ever starts doing the token exchange.
                    let cache = self.fluree.r2rml_cache();
                    let client_fp = rest_client_cache_key(graph_source_id, &record.config);
                    let catalog = match cache.rest_client(&client_fp) {
                        Some(c) => c,
                        None => {
                            // ORDERING TRAP (§3): hydration happens ONLY here, inside
                            // the cache-miss arm, strictly AFTER `client_fp` was
                            // computed above over the RAW, reference-bearing config.
                            let auth_provider =
                                hydrated_auth_provider(auth, self.fluree.secret_resolver()).await?;
                            let client = Arc::new(
                                RestCatalogClient::new(
                                    RestCatalogConfig {
                                        uri: uri.clone(),
                                        warehouse: warehouse.clone(),
                                        ..Default::default()
                                    },
                                    auth_provider,
                                )
                                .map_err(|e| {
                                    QueryError::Internal(format!(
                                        "Failed to create catalog client: {e}"
                                    ))
                                })?,
                            );
                            cache.put_rest_client(client_fp, Arc::clone(&client));
                            client
                        }
                    };
                    // Owned captures for the `'static` deferred builder (the
                    // provider's `fluree: &'a` borrow can't cross the Parquet
                    // `tokio::spawn`). The builder runs the SAME session-pin ladder
                    // as the eager path (`resolve_rest_load_and_storage`), deferred.
                    let cache = Arc::clone(cache);
                    let session = Arc::clone(&self.session);
                    let io = iceberg_config.io.clone();
                    let table_id = table_id.clone();
                    let lt_key_b = lt_key.clone();
                    let uri_b = uri.clone();
                    let builder: super::lazy_storage::StorageBuilder<
                        'static,
                        IcebergStorageBackend,
                    > = Arc::new(move || {
                        let catalog = Arc::clone(&catalog);
                        let cache = Arc::clone(&cache);
                        let session = Arc::clone(&session);
                        let io = io.clone();
                        let table_id = table_id.clone();
                        let lt_key = lt_key_b.clone();
                        let catalog_uri = uri_b.clone();
                        Box::pin(async move {
                            resolve_rest_load_and_storage(
                                catalog,
                                cache,
                                session,
                                table_id,
                                io,
                                lt_key,
                                catalog_uri,
                            )
                            .await
                            .map(|(storage, _)| storage)
                            // Preserve the TYPED errors across the builder's
                            // `IcebergError` channel: `storage_query_error` lifts
                            // these two straight back to their `QueryError`
                            // counterparts, so a lazy-forced build reports the same
                            // 403 + wire code (`err:storage/AccessDenied` /
                            // `err:catalog/CredentialsNotVended`) as the eager path.
                            // A blanket `to_string()` here would flatten them into
                            // an opaque string and put hosts back to regex-guessing.
                            .map_err(|e| match e {
                                QueryError::StorageAccessDenied {
                                    bucket,
                                    key,
                                    region,
                                    message,
                                } => IcebergError::StorageAccessDenied {
                                    bucket,
                                    key,
                                    region,
                                    message,
                                },
                                QueryError::CatalogCredentialsNotVended { catalog_uri } => {
                                    IcebergError::CatalogCredentialsNotVended { catalog_uri }
                                }
                                other => IcebergError::Catalog(other.to_string()),
                            })
                        })
                    });
                    return Ok((Arc::new(LazyS3Storage::deferred(builder)), md, loc));
                }
            }
        }

        // Resolve metadata location and create storage based on catalog mode.
        // `mut` so we can move the REST catalog's inline metadata out of the
        // response below (see the metadata resolution) without cloning it.
        let mut direct_location: Option<String> = None;
        let (mut load_response, storage) = match &iceberg_config.catalog {
            CatalogConfig::Rest {
                uri,
                warehouse,
                auth,
                ..
            } => {
                let cache = self.fluree.r2rml_cache();

                // Process-wide REST client keyed by the source config fingerprint:
                // its OAuth `CachedToken` and HTTPS connection pool are reused
                // across queries, so a warm server does one token exchange per
                // ~hour instead of one per query. The fingerprint hashes the full
                // source config, so a rotated PAT (or any config change) builds a
                // fresh client.
                let client_fp = rest_client_cache_key(graph_source_id, &record.config);
                let catalog = match cache.rest_client(&client_fp) {
                    Some(c) => c,
                    None => {
                        // ORDERING TRAP (§3): hydration happens ONLY here, inside the
                        // cache-miss arm, strictly AFTER `client_fp` was computed above
                        // over the RAW, reference-bearing config. The fingerprint-
                        // stability test pins this.
                        let auth_provider =
                            hydrated_auth_provider(auth, self.fluree.secret_resolver()).await?;
                        let catalog_config = RestCatalogConfig {
                            uri: uri.clone(),
                            warehouse: warehouse.clone(),
                            ..Default::default()
                        };
                        let client = Arc::new(
                            RestCatalogClient::new(catalog_config, auth_provider).map_err(|e| {
                                QueryError::Internal(format!(
                                    "Failed to create catalog client: {e}"
                                ))
                            })?,
                        );
                        cache.put_rest_client(client_fp, Arc::clone(&client));
                        client
                    }
                };

                let lt_key = super::catalog_session::IcebergCatalogSession::load_table_key(
                    graph_source_id,
                    &table_id.namespace,
                    &table_id.table,
                );

                // Resolve `loadTable`, cheapest first: (1) the per-query pin (one
                // snapshot for the whole query); (2) the cross-query cache (skips
                // the ~1.3–3s catalog GET, TTL + creds gated); (3) a real REST
                // load, which populates both caches.
                let load_response = if let Some(cached) = self.session.cached_load_table(&lt_key) {
                    debug!(namespace = %table_id.namespace, table = %table_id.table,
                        "loadTable pin hit (query-scoped)");
                    cached
                } else {
                    let pinned = self.session.pinned_metadata_location(&lt_key);
                    // A cross-query hit applies only on the FIRST resolution of
                    // this table in the query. Once pinned, a reload is a creds
                    // refresh that must keep the pinned snapshot.
                    let cross_query = if pinned.is_none() {
                        cache.get_rest_load_table(&lt_key)
                    } else {
                        None
                    };
                    let mut resp = if let Some(cq) = cross_query {
                        debug!(namespace = %table_id.namespace, table = %table_id.table,
                            "loadTable cache hit (cross-query)");
                        cq.to_response()
                    } else {
                        info!(catalog_uri = %uri, namespace = %table_id.namespace,
                            table = %table_id.table, "Loading table from REST catalog");
                        // The cold REST/OAuth catalog round-trip (~1-3s) — the
                        // highest-value span for attributing a slow virtual-dataset
                        // query to cold-remote-retrieval vs. caching vs. decode.
                        let actual = catalog
                            .load_table(&table_id, iceberg_config.io.vended_credentials)
                            .instrument(tracing::debug_span!(
                                "r2rml.load_table",
                                namespace = %table_id.namespace,
                                table = %table_id.table,
                            ))
                            .await
                            .map_err(|e| {
                                QueryError::Internal(format!(
                                    "Failed to load table from catalog: {e}"
                                ))
                            })?;
                        // The cross-query cache reflects the CURRENT catalog state
                        // (never this query's pin), so other queries see the newest
                        // snapshot within the TTL.
                        cache.put_rest_load_table(
                            lt_key.clone(),
                            Arc::new(super::catalog_session::CachedLoadTable::from_response(
                                &actual,
                            )),
                        );
                        // This query keeps its pinned snapshot across a creds
                        // refresh: vended creds are bucket/prefix-scoped, so the
                        // fresh creds still read the pinned snapshot's immutable
                        // data files.
                        let mut r = actual;
                        if let Some(ref pinned_loc) = pinned {
                            if *pinned_loc != r.metadata_location {
                                debug!(pinned = %pinned_loc, reloaded = %r.metadata_location,
                                    "Refreshed vended credentials; keeping the query's pinned snapshot");
                                r.metadata_location = pinned_loc.clone();
                            }
                        }
                        info!(metadata_location = %r.metadata_location,
                            has_credentials = r.credentials.is_some(), "Loaded table metadata location");
                        r
                    };
                    self.session.store_load_table(lt_key.clone(), &resp);
                    // Converge on the pinned snapshot. `store_load_table` keeps the
                    // first writer's `metadata_location`, so if a concurrent first
                    // load of this table pinned a different location between our
                    // pin check above and this store, adopt the winning pin rather
                    // than scan our own freshly loaded location — otherwise two
                    // scans in one query could read different snapshots
                    // (fluree/db#1406 review). Sequential execution makes this a
                    // no-op; it holds the invariant unconditionally.
                    if let Some(pinned_loc) = self.session.pinned_metadata_location(&lt_key) {
                        resp.metadata_location = pinned_loc;
                    }
                    resp
                };

                // §2 fail-closed + query-session reuse + vended/ambient construction,
                // shared verbatim with the deferred `LazyS3Storage` builder's path so
                // the two can never make different credential decisions for the same
                // table (see `rest_session_storage`).
                let storage = rest_session_storage(
                    &self.session,
                    &lt_key,
                    &iceberg_config.io,
                    &load_response,
                    uri,
                )
                .await?;

                (load_response, storage)
            }
            CatalogConfig::Direct { table_location } => {
                // Session cache key for this table's storage client — the same
                // key the REST branch uses (source id + fully-qualified table),
                // so repeated scans of one Direct table in a query reuse a single
                // S3 client instead of rebuilding it (credential-chain resolution
                // + a fresh connection pool) per scan (fluree/db#1498).
                let lt_key = super::catalog_session::IcebergCatalogSession::load_table_key(
                    graph_source_id,
                    &table_id.namespace,
                    &table_id.table,
                );

                // Warehouse-root resolution: when `table_location` is a warehouse
                // ROOT (a catalog-less multi-table copy) rather than a single
                // table dir, resolve THIS table's own directory beneath it via one
                // session-cached LIST. Single-table direct returns `table_location`
                // unchanged, so its behavior below is byte-identical.
                let effective_location = self
                    .resolve_direct_table_location(
                        table_location,
                        &table_id,
                        &lt_key,
                        &iceberg_config,
                    )
                    .await?;
                let table_location = &effective_location;
                direct_location = Some(effective_location.clone());

                info!(
                    table_location = %table_location,
                    "Loading table via direct S3 access"
                );

                let cache = self.fluree.r2rml_cache();
                // Storage construction sits BELOW the metadata-location check so a
                // hit resolves through `direct_session_storage`, which serves the
                // session-cached client when this table was already resolved this
                // query and skips `from_default_chain` entirely. Both arms still
                // need storage (the scan reads data files; the miss arm also reads
                // version-hint/metadata via the direct catalog), so this is a
                // reordering — the session cache covers it, not an elision.
                let (load_response, storage) = if let Some(metadata_location) =
                    cache.get_direct_metadata_location(table_location).await
                {
                    debug!(
                        table_location = %table_location,
                        metadata_location = %metadata_location,
                        "Direct metadata-location cache hit"
                    );
                    let storage = direct_session_storage(
                        &self.session,
                        &lt_key,
                        table_location,
                        iceberg_config.io.s3_region.as_deref(),
                        iceberg_config.io.s3_endpoint.as_deref(),
                        iceberg_config.io.s3_path_style,
                    )
                    .await?;
                    let load_response = fluree_db_iceberg::catalog::LoadTableResponse {
                        metadata_location,
                        config: std::collections::HashMap::default(),
                        credentials: None,
                        metadata: None,
                    };
                    (load_response, storage)
                } else {
                    debug!(table_location = %table_location, "Direct metadata-location cache miss");

                    // The direct catalog reads version-hint.text + metadata from
                    // S3, so it needs the storage client up front.
                    let storage = direct_session_storage(
                        &self.session,
                        &lt_key,
                        table_location,
                        iceberg_config.io.s3_region.as_deref(),
                        iceberg_config.io.s3_endpoint.as_deref(),
                        iceberg_config.io.s3_path_style,
                    )
                    .await?;

                    let direct_catalog =
                        SendDirectCatalogClient::new(table_location.clone(), Arc::clone(&storage));

                    let load_response =
                        direct_catalog
                            .load_table(&table_id, false)
                            .await
                            .map_err(|e| {
                                storage_query_error(
                                    &format!(
                                        "Failed to resolve table metadata from {table_location}"
                                    ),
                                    e,
                                )
                            })?;
                    cache
                        .put_direct_metadata_location(
                            table_location.clone(),
                            load_response.metadata_location.clone(),
                        )
                        .await;
                    (load_response, storage)
                };

                info!(
                    metadata_location = %load_response.metadata_location,
                    "Resolved table metadata via version-hint.text"
                );

                (load_response, storage)
            }
        };

        // Resolve the table metadata from the cheapest available source: the
        // in-memory cache, the REST catalog's inline `metadata`, the disk catalog
        // cache, then a fresh S3 GET. Extracted into `resolve_table_metadata` so the
        // resolution order — in particular that an inline `loadTable` copy
        // short-circuits the disk/S3 fetch and its `r2rml.read_metadata` span — is
        // unit-testable against a storage stub. The inline metadata is moved out of
        // `load_response` (never cloned — it replaces an S3 GET) and is `None` for
        // Direct mode and cache-reconstructed responses (per-query pin / cross-query
        // cache), which is precisely why the disk/S3 fallback in the helper must stay
        // intact. It seeds BOTH cache layers — the disk write is what lets the pointer
        // rung above serve the NEXT process with zero REST (see the fn doc).
        let inline_metadata = load_response.metadata.take();
        let metadata = resolve_table_metadata(
            self.fluree.r2rml_cache(),
            storage.as_ref(),
            &load_response.metadata_location,
            inline_metadata,
            &disk,
        )
        .await?;
        // Persist the credential-free pointer so the NEXT process resolves this
        // table's `metadata_location` without a loadTable GET. `snapshot_ms` feeds
        // the as_of_t rider (grep r2rml-as-of-t). The eager `storage` is already
        // built (a pointer/metadata miss forced the GET), so it wraps in `ready`.
        disk.put_metadata_location(
            &lt_key,
            &load_response.metadata_location,
            metadata
                .current_snapshot()
                .map(|s| s.timestamp_ms)
                .unwrap_or(0),
        );
        // Relocated local table: remap manifest file references (no-op for
        // REST/S3 and locally-written tables) BEFORE the ready-wrap.
        let storage = remap_local_storage(storage, &metadata, direct_location.as_deref());
        Ok((
            Arc::new(LazyS3Storage::ready(storage)),
            metadata,
            load_response.metadata_location.clone(),
        ))
    }

    /// Inner implementation of [`R2rmlTableProvider::scan_table`], split out so the
    /// trait method can wrap the setup in an `r2rml.scan_table` timing span via
    /// `.instrument()` (the codebase's established pattern for timing an async body
    /// without holding a span guard across an `.await`). The shared `loadTable`
    /// resolution (and its per-query snapshot pin) lives in
    /// [`Self::load_table_context`]; this adds the scan-only concerns — the
    /// scan-start log and the Parquet disk cache — and the streaming scan plan.
    async fn scan_table_inner(
        &self,
        graph_source_id: &str,
        table_name: &str,
        projection: &[String],
        filters: &[ScanFilter],
        topk: Option<&ScanTopK>,
        _as_of_t: Option<i64>,
    ) -> QueryResult<ColumnBatchStream> {
        // GREP: r2rml-as-of-t — time-travel is not implemented for Iceberg scans;
        // `_as_of_t` is deliberately ignored. If as-of semantics ever land here,
        // `table_row_count_inner` MUST honor them identically (matching breadcrumb
        // there): a COUNT and a scan in one query must read the same snapshot.
        info!(
            graph_source_id = %graph_source_id,
            table_name = %table_name,
            projection = ?projection,
            "Starting Iceberg table scan"
        );

        // Resolve the pinned table context (S3 storage + the snapshot-pinned
        // metadata) shared with the COUNT(*) manifest shortcut, so a count and a
        // scan in one query read the same pinned Iceberg snapshot.
        let (storage, metadata, metadata_location) =
            self.load_table_context(graph_source_id, table_name).await?;

        // Capture this table's pinned snapshot into the build watermark (DEC-003).
        // The single caller of `load_table_context` records here (first-writer-wins),
        // avoiding the multiple resolution paths inside it; the ids come straight off
        // the already-parsed metadata (no extra I/O). Harmless for ordinary queries —
        // `build_watermark` only reads it during a materialize build.
        self.session.record_snapshot(
            super::catalog_session::IcebergCatalogSession::snapshot_key(
                graph_source_id,
                table_name,
            ),
            fluree_db_query::r2rml::TableWatermark {
                metadata_location: metadata_location.clone(),
                snapshot_id: metadata.current_snapshot_id,
                sequence_number: metadata.current_snapshot().map(|s| s.sequence_number),
            },
        );

        // Shared on-disk cache for data files (one global byte budget, deduped per
        // directory). Threaded into the Parquet readers, which apply a
        // whole-file-vs-range policy per file based on how much each query reads.
        // Scan-only: the COUNT shortcut reads no data files, so it never builds it.
        let cache_dir = self.fluree.binary_store_cache_dir();
        let disk_cache = fluree_db_iceberg::DiskArtifactCache::for_dir(&cache_dir);

        let cache = self.fluree.r2rml_cache();

        let schema = metadata
            .current_schema()
            .ok_or_else(|| QueryError::Internal("Table has no current schema".to_string()))?;

        info!(
            format_version = metadata.format_version,
            schema_id = schema.schema_id,
            field_count = schema.fields.len(),
            "Parsed table metadata"
        );

        // Resolve column names to field IDs for projection
        let projected_field_ids: Vec<i32> = if projection.is_empty() {
            schema
                .fields
                .iter()
                .filter(|f| !f.is_nested())
                .map(|f| f.id)
                .collect()
        } else {
            projection
                .iter()
                .filter_map(|col_name| schema.field_by_name(col_name).map(|f| f.id))
                .collect()
        };

        if projected_field_ids.is_empty() && !projection.is_empty() {
            return Err(QueryError::InvalidQuery(format!(
                "None of the projected columns {:?} exist in table schema. Available: {:?}",
                projection,
                schema.field_names()
            )));
        }

        let schema_arc = Arc::new(schema.clone());

        // Build an Iceberg pushdown predicate for file pruning. Filters resolve
        // to fields by name; unknown fields are skipped (conservative).
        let filter_expr = build_iceberg_filter(filters, schema);

        // Reuse manifest-derived file selections across repeated scans of the
        // same snapshot. Projection still varies per scan, so we rebuild tasks.
        // The scan-files cache is keyed only by metadata location, so it is
        // bypassed when a pushdown filter is present (different filter → a
        // different pruned file set).
        let (tasks, files_selected, files_pruned, estimated_row_count) = if let Some(filter) =
            &filter_expr
        {
            let scan_config = ScanConfig::new()
                .with_projection(projected_field_ids.clone())
                .with_filter(filter.clone());
            let planner = SendScanPlanner::new(storage.as_ref(), &metadata, scan_config);
            let plan = planner
                .plan_scan()
                .await
                .map_err(|e| storage_query_error("Failed to plan scan", e))?;
            (
                plan.tasks,
                plan.files_selected,
                plan.files_pruned,
                plan.estimated_row_count,
            )
        } else if let Some(cached) = cache.get_scan_files(&metadata_location).await {
            // F-AUD-1 cache-arm guard: an in-memory scan-files HIT rebuilds tasks
            // without calling the guarded plan_scan, so re-check the delete flag.
            guard_cached_scan_files(cached.has_delete_manifests, &metadata_location)?;
            debug!(
                metadata_location = %metadata_location,
                cached_files = cached.data_files.len(),
                "Iceberg scan-files cache hit"
            );

            let tasks = cached
                .data_files
                .iter()
                .cloned()
                .map(|data_file| {
                    FileScanTask::for_whole_file_with_schema(
                        data_file,
                        projected_field_ids.clone(),
                        None,
                        Arc::clone(&schema_arc),
                    )
                })
                .collect::<Vec<_>>();

            (
                tasks,
                cached.files_selected,
                cached.files_pruned,
                cached.estimated_row_count,
            )
        } else if let Some(disk) = self.catalog_disk_cache().get_scan_files(&metadata_location) {
            // F-AUD-1 cache-arm guard (the cross-process case): a persistent-disk
            // scan-files entry survives restarts, so a delete-bearing list cached
            // under the override by an earlier process must be re-checked before it
            // is served or promoted into the in-memory cache. Pre-guard (v2) entries
            // are already excluded by the CACHE_FORMAT_VERSION bump; this covers a v3
            // entry written under the override.
            guard_cached_scan_files(disk.has_delete_manifests, &metadata_location)?;
            // PR-8 slice 2: in-memory miss, but the persistent disk catalog
            // cache has this snapshot's (unfiltered) file list — a warm-catalog
            // cold process skips the manifest read (`iceberg.scan_plan`). Rebuild
            // tasks from the file list exactly as the in-memory-hit arm does, and
            // populate the in-memory cache for the rest of the process.
            debug!(
                metadata_location = %metadata_location,
                cached_files = disk.data_files.len(),
                "Iceberg scan-files disk-cache hit"
            );
            cache
                .put_scan_files(metadata_location.clone(), Arc::clone(&disk))
                .await;
            let tasks = disk
                .data_files
                .iter()
                .cloned()
                .map(|data_file| {
                    FileScanTask::for_whole_file_with_schema(
                        data_file,
                        projected_field_ids.clone(),
                        None,
                        Arc::clone(&schema_arc),
                    )
                })
                .collect::<Vec<_>>();
            (
                tasks,
                disk.files_selected,
                disk.files_pruned,
                disk.estimated_row_count,
            )
        } else {
            debug!(metadata_location = %metadata_location, "Iceberg scan-files cache miss");

            // Create scan configuration with projection for the first plan.
            let scan_config = ScanConfig::new().with_projection(projected_field_ids.clone());
            let planner = SendScanPlanner::new(storage.as_ref(), &metadata, scan_config);
            let plan = planner
                .plan_scan()
                .await
                .map_err(|e| storage_query_error("Failed to plan scan", e))?;

            let cached = Arc::new(CachedScanFiles {
                data_files: Arc::new(
                    plan.tasks
                        .iter()
                        .map(|task| task.data_file.clone())
                        .collect(),
                ),
                estimated_row_count: plan.estimated_row_count,
                files_selected: plan.files_selected,
                files_pruned: plan.files_pruned,
                has_delete_manifests: plan.has_delete_manifests,
            });
            cache
                .put_scan_files(metadata_location.clone(), Arc::clone(&cached))
                .await;
            // Persist to the disk catalog cache (content-addressed, immutable).
            self.catalog_disk_cache()
                .put_scan_files(&metadata_location, &cached);

            (
                plan.tasks,
                cached.files_selected,
                cached.files_pruned,
                cached.estimated_row_count,
            )
        };

        info!(
            files_selected,
            files_pruned,
            estimated_rows = estimated_row_count,
            "Scan plan created"
        );

        if tasks.is_empty() {
            info!("Scan plan has no files - returning empty result");
            return Ok(empty_batch_stream());
        }

        // Read data files with bounded parallelism, streaming each file's batches
        // to the consumer as the worker completes. Concurrency is capped (see
        // `iceberg_scan_concurrency`) so only O(concurrency) file decodes are
        // resident — the consumer (R2rmlScanOperator) materializes and aggregates
        // incrementally instead of the whole table being collected here.
        let footers = cache.parquet_footers();

        // PR-5 / item 8 scan-side top-k. When a resolvable single-column directive
        // is present, read files best-first (DESC by `upper_bound`, ASC by
        // `lower_bound`) with a running k-th bound and stop once no unread file can
        // beat it — streaming a strict SUPERSET of the top-k (the `SortOperator`
        // above is authoritative). ASC is admitted only for a required column. The
        // pruned subset MUST bypass the operator's scan cache (handled by its
        // `cacheable` guard gaining `&& topk.is_none()`); the disk *artifact* cache
        // is keyed by file path+size with whole-file entries, so a pruned subset
        // never poisons it. Falls through to the parallel path if the sort column
        // is unresolvable. Sequential reads are bounded by `TOPK_SEQUENTIAL_CAP`:
        // if the prune is ineffective (adversarial layout / all files tie at the
        // bound / a heap that can't fill), the remaining files are handed to the
        // normal parallel reader so the topk path can never be slower than it.
        if let Some(tk) = topk {
            // Item 8 (F-AUD-6): ASC top-k is SOUND ONLY for a REQUIRED (non-nullable)
            // column — SPARQL orders unbound values FIRST under ASC, so a nullable
            // column could hold an unread NULL row that belongs ahead of the top-k.
            // DESC is sound for any column (unbound sorts last). A declined directive
            // simply falls through to the normal parallel scan below (ignoring a
            // top-k is always correct — the `SortOperator` above is authoritative).
            let admitted = schema
                .field_by_name(&tk.sort_column)
                .filter(|field| !tk.ascending || field.required);
            if let Some(field) = admitted {
                let sort_field_id = field.id;
                let sort_type = field.type_string().map(str::to_string);
                let order = plan_topk_read(
                    tasks.iter().map(|t| &t.data_file),
                    sort_field_id,
                    sort_type.as_deref(),
                    tk.ascending,
                );

                let mut bound = TopKBound::new(tk.k, tk.ascending);
                let mut collected: Vec<ColumnBatch> = Vec::new();
                let mut tail: Vec<FileScanTask> = Vec::new();
                let mut reads = 0usize;
                for pos in 0..order.len() {
                    if pos >= TOPK_SEQUENTIAL_CAP {
                        // Prune ineffective after the cap — finish in parallel so
                        // the topk path is never slower than the full parallel scan.
                        tail = order[pos..]
                            .iter()
                            .map(|(orig, _)| tasks[*orig].clone())
                            .collect();
                        break;
                    }
                    let (orig, _) = order[pos];
                    let read_span = tracing::debug_span!(
                        "iceberg.parquet_read",
                        path = %tasks[orig].data_file.file_path,
                        file_size = tasks[orig].data_file.file_size_in_bytes,
                    );
                    let batches = SendParquetReader::with_caches(
                        storage.as_ref(),
                        footers.as_ref(),
                        &disk_cache,
                        &cache_dir,
                    )
                    .read_task(&tasks[orig])
                    .instrument(read_span)
                    .await
                    .map_err(|e| {
                        storage_query_error(
                            &format!(
                                "Failed to read Parquet file '{}'",
                                tasks[orig].data_file.file_path
                            ),
                            e,
                        )
                    })?;
                    // SOUNDNESS INVARIANT: the heap is fed the sort values of the
                    // rows this scan EMITS — which are the QUALIFYING result rows
                    // (post any pushed row filter). The directive is declined
                    // upstream (`resolve_topk_directive`) whenever a RESIDUAL filter
                    // the operator enforces after this scan is present, because
                    // feeding pre-filter values would ride the k-th bound too high
                    // and prune files whose qualifying rows belong in the true
                    // top-k. Never feed a superset of the qualifying rows here.
                    for b in &batches {
                        bound.observe_all(batch_sort_values(b, sort_field_id));
                    }
                    collected.extend(batches);
                    reads += 1;
                    // Stop iff the heap is full and the NEXT (best-remaining) file's
                    // bound is strictly WORSE than the k-th (below it for DESC, above
                    // it for ASC; over-keep on ties; a no-bound next → never stops).
                    // See `TopKBound::can_stop`.
                    if let Some((_, next_bound)) = order.get(pos + 1) {
                        if bound.can_stop(next_bound.as_ref()) {
                            break;
                        }
                    }
                }

                // Report the topk file selection through the SAME span the bench
                // harness sums (`iceberg.scan_plan`) — the planner's span does not
                // fire on this path, so without this the `files_selected` /
                // `files_pruned` counters would read 0. `files_selected` is the
                // files actually read (the sequential prefix plus any parallel
                // tail); the rest were provably unable to beat the k-th bound.
                let files_selected = reads + tail.len();
                let files_pruned = order.len().saturating_sub(files_selected);
                tracing::debug_span!(
                    "iceberg.scan_plan",
                    files_selected = files_selected as u64,
                    files_pruned = files_pruned as u64,
                )
                .in_scope(|| {});
                debug!(
                    files_selected,
                    files_pruned,
                    total_files = order.len(),
                    k = tk.k,
                    "scan-side top-k prune"
                );
                let prefix = futures::stream::iter(collected.into_iter().map(Ok));
                if tail.is_empty() {
                    return Ok(Box::pin(prefix));
                }
                // Parallel fallback tail (same bounded-parallel read as the normal
                // path). The bound still holds; we just stop paying sequentiality.
                let concurrency = iceberg_scan_concurrency(tail.len());
                let tail_stream =
                    futures::stream::iter(tail)
                        .map(move |task| {
                            let storage = Arc::clone(&storage);
                            let footers = Arc::clone(&footers);
                            let disk_cache = Arc::clone(&disk_cache);
                            let cache_dir = cache_dir.clone();
                            let read_span = tracing::debug_span!(
                                "iceberg.parquet_read",
                                path = %task.data_file.file_path,
                                file_size = task.data_file.file_size_in_bytes,
                            );
                            async move {
                                tokio::spawn(async move {
                                    let reader = SendParquetReader::with_caches(
                                        storage.as_ref(),
                                        footers.as_ref(),
                                        &disk_cache,
                                        &cache_dir,
                                    );
                                    reader.read_task(&task).instrument(read_span).await.map_err(
                                        |e| {
                                            storage_query_error(
                                                &format!(
                                                    "Failed to read Parquet file '{}'",
                                                    task.data_file.file_path
                                                ),
                                                e,
                                            )
                                        },
                                    )
                                })
                                .await
                                .map_err(|e| {
                                    QueryError::Internal(format!("Parquet read worker failed: {e}"))
                                })?
                            }
                        })
                        .buffer_unordered(concurrency)
                        .flat_map(|res: QueryResult<Vec<ColumnBatch>>| match res {
                            Ok(batches) => futures::stream::iter(
                                batches.into_iter().map(Ok).collect::<Vec<_>>(),
                            ),
                            Err(e) => futures::stream::iter(vec![Err(e)]),
                        });
                return Ok(Box::pin(prefix.chain(tail_stream)));
            }
        }

        let concurrency = iceberg_scan_concurrency(tasks.len());
        // T2.3: split the vCPU budget between the file-level fan-out and per-file
        // row-group parallelism. A single-file table (file concurrency 1) grants
        // all cores to its row groups; a many-file scan (file concurrency already
        // ≈ cores) grants 1 — no oversubscription. Deterministic integer division.
        let rowgroup_concurrency = (std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(4)
            / concurrency)
            .max(1);
        debug!(
            files = tasks.len(),
            concurrency, rowgroup_concurrency, "streaming Parquet files (bounded parallel)"
        );

        let stream = futures::stream::iter(tasks)
            .map(move |task| {
                let storage = Arc::clone(&storage);
                let footers = Arc::clone(&footers);
                let disk_cache = Arc::clone(&disk_cache);
                let cache_dir = cache_dir.clone();
                // Create the per-file span HERE, before `tokio::spawn`, so it is
                // parented under the consumer's current span: `tokio::spawn` does
                // NOT propagate the current span into the spawned task, but a span
                // records its parent at creation time. Instrumenting the read
                // future inside the task then times the actual read+decode while
                // keeping the correct parent (and gives each concurrent read a
                // distinct span, respecting the `buffer_unordered` fan-out).
                let read_span = tracing::debug_span!(
                    "iceberg.parquet_read",
                    path = %task.data_file.file_path,
                    file_size = task.data_file.file_size_in_bytes,
                );
                async move {
                    tokio::spawn(async move {
                        let reader = SendParquetReader::with_caches(
                            storage.as_ref(),
                            footers.as_ref(),
                            &disk_cache,
                            &cache_dir,
                        )
                        .with_rowgroup_concurrency(rowgroup_concurrency);
                        reader
                            .read_task(&task)
                            .instrument(read_span)
                            .await
                            .map_err(|e| {
                                storage_query_error(
                                    &format!(
                                        "Failed to read Parquet file '{}'",
                                        task.data_file.file_path
                                    ),
                                    e,
                                )
                            })
                    })
                    .await
                    .map_err(|e| QueryError::Internal(format!("Parquet read worker failed: {e}")))?
                }
            })
            .buffer_unordered(concurrency)
            // Flatten each file's `Result<Vec<ColumnBatch>>` into individual
            // `Result<ColumnBatch>` items; a read error becomes one error item.
            .flat_map(|res: QueryResult<Vec<ColumnBatch>>| match res {
                Ok(batches) => {
                    futures::stream::iter(batches.into_iter().map(Ok).collect::<Vec<_>>())
                }
                Err(e) => futures::stream::iter(vec![Err(e)]),
            });

        Ok(Box::pin(stream))
    }
}

/// An empty [`ColumnBatchStream`], used when a scan plan selects no files.
fn empty_batch_stream() -> ColumnBatchStream {
    Box::pin(futures::stream::empty())
}

/// Re-apply the fail-closed merge-on-read guard (audit F-AUD-1) to a cached
/// scan-files selection. A scan-files cache HIT (in-memory or persistent disk)
/// rebuilds tasks WITHOUT calling the guarded `plan_scan`, so a delete-bearing
/// file list — which is only ever cacheable when the guard was overridden at plan
/// time — must be re-checked here in case the override is now off. Cheap: the flag
/// rides on the cache entry, no manifest re-read. Shared by both hit arms so the
/// check cannot drift between them.
fn guard_cached_scan_files(has_delete_manifests: bool, metadata_location: &str) -> QueryResult<()> {
    let allow_mor = fluree_db_iceberg::mor_deletes_allowed();
    fluree_db_iceberg::ensure_no_delete_manifests(
        usize::from(has_delete_manifests),
        metadata_location,
        allow_mor,
    )
    .map_err(|e| storage_query_error("Refusing cached Iceberg scan", e))
}

/// Decide whether a pinned snapshot's manifest `record_count` sum is a sound
/// answer to a bare `COUNT(*)`, and if so return it. Pure over the manifest read
/// result (no I/O), so the soundness gates are unit-tested directly against
/// hand-built [`fluree_db_iceberg::DataFile`]s.
///
/// Returns `Some(n)` only when both hold:
/// 1. the snapshot has **no delete manifests** — a merge-on-read position/equality
///    delete would make the `record_count` sum an over-count; and
/// 2. **every** `non_null_col` is provably zero-null from the manifest stats.
///    `aggregate_column_stats`' coverage gate makes `null_count` `Some(0)` only
///    when EVERY data file reported a null count for the column and they sum to
///    zero; an absent or partially-covered stat is `None` (unknown) and a positive
///    count is `Some(n>0)` — both decline. An unknown null count is **never**
///    treated as zero. A column absent from the schema is likewise unproven.
/// 3. the per-file `record_count`s are well-formed — a negative per-file count,
///    or a sum that would overflow `u64` (both only possible in a corrupt
///    manifest), declines rather than serving a wrapped/bogus "exact" count.
///
/// An empty `non_null_cols` is a constant-subject mapping (a row is produced for
/// every table row), so the count is sound with no null proof required.
fn sound_manifest_row_count(
    schema: &fluree_db_iceberg::metadata::Schema,
    data_files: &[fluree_db_iceberg::DataFile],
    has_delete_manifests: bool,
    non_null_cols: &[String],
) -> Option<u64> {
    if has_delete_manifests {
        return None;
    }
    let agg = aggregate_column_stats(data_files, schema);
    for col in non_null_cols {
        let field = schema.field_by_name(col)?;
        match agg.columns.get(&field.id).and_then(|c| c.null_count) {
            Some(0) => {}
            _ => return None,
        }
    }
    // `record_count` is non-negative in real Iceberg metadata; a corrupt manifest
    // must decline rather than feed a bogus "exact" count. Re-summed here with
    // per-file checked u64 arithmetic instead of trusting `agg.row_count`, whose
    // plain i64 sum saturates on corrupt input and cannot distinguish a per-file
    // negative from a smaller valid total: a negative per-file count, or a sum
    // that would overflow `u64`, declines.
    let mut total: u64 = 0;
    for df in data_files {
        total = total.checked_add(u64::try_from(df.record_count).ok()?)?;
    }
    Some(total)
}

/// Resolve a table's [`TableMetadata`] from the cheapest available source.
///
/// Order, cheapest first:
/// 1. the in-memory moka cache (`cache`);
/// 2. `inline_metadata` — the parsed `metadata` a real REST `loadTable` already
///    handed us in `LoadTableResponse` (populated in `catalog/rest.rs`). It is
///    `None` for Direct mode and for cache-reconstructed responses (per-query pin
///    / cross-query cache), which carry only a metadata location;
/// 3. the persistent disk catalog cache (`disk`);
/// 4. a fresh S3 GET of the metadata JSON, timed under the `r2rml.read_metadata`
///    span.
///
/// The inline short-circuit is §1 of fluree/db#1500: a REST catalog that vends
/// metadata inline never re-fetches it from S3, and emits NO `r2rml.read_metadata`
/// span (that span must fire only on a real S3 read; the live gate asserts n=0 for
/// inline-vending REST catalogs).
///
/// The inline result seeds BOTH cache layers, and the disk write is LOAD-BEARING —
/// not bookkeeping. The pointer rung in `load_table_context` serves a cross-process
/// zero-REST resolve only when it finds BOTH a persisted `metadata_location` pointer
/// AND the metadata itself via `metadata_from_caches`, which reads the in-memory
/// cache then **this disk layer**. A fresh process has an empty in-memory cache, so
/// skipping the disk write here would starve the pointer rung for exactly the
/// catalogs that vend inline metadata (Snowflake Horizon / Polaris) — silently
/// converting their zero-REST first-ask back into a ~1–3 s `loadTable` GET, with the
/// regression visible only in the cache gate's `load_table.n` counter. Seeding from
/// the free inline copy is strictly better than the pre-§1 behaviour, where this
/// layer could only be populated by first paying an S3 GET.
async fn resolve_table_metadata<S: SendIcebergStorage>(
    cache: &R2rmlCache,
    storage: &S,
    metadata_location: &str,
    inline_metadata: Option<TableMetadata>,
    disk: &super::disk_catalog_cache::DiskCatalogCache,
) -> QueryResult<Arc<TableMetadata>> {
    if let Some(cached) = cache.get_metadata(metadata_location).await {
        debug!(metadata_location = %metadata_location, "Table metadata cache hit");
        return Ok(cached);
    }

    if let Some(inline) = inline_metadata {
        debug!(
            metadata_location = %metadata_location,
            "Table metadata used inline from loadTable response (no S3 fetch)"
        );
        let metadata = Arc::new(inline);
        // Seed the disk layer too: this is what the next process's pointer rung
        // reads (see the fn doc). Content-addressed by `metadata_location`, so the
        // entry is immutable and always current for that snapshot.
        disk.put_metadata(metadata_location, metadata.as_ref());
        cache
            .put_metadata(metadata_location.to_string(), Arc::clone(&metadata))
            .await;
        return Ok(metadata);
    }

    debug!(metadata_location = %metadata_location, "Table metadata cache miss");

    // PR-8 slice 2: on the in-memory miss, try the persistent disk catalog cache
    // before hitting S3. `metadata_location` is content-addressed, so a hit is
    // always current for that snapshot. A cold process with a warm catalog dir
    // serves the parsed metadata from local disk (no S3 GET, no
    // `r2rml.read_metadata` span).
    let metadata = if let Some(cached) = disk.get_metadata(metadata_location) {
        debug!(metadata_location = %metadata_location, "Table metadata disk-cache hit");
        cached
    } else {
        // Measurement sub-span (PR-8 cold decomposition): isolate the metadata-JSON
        // S3 GET + parse — the `load_table_context` component between the loadTable
        // REST GET (`r2rml.load_table`) and the manifest read (`iceberg.scan_plan` /
        // `r2rml.count_manifest_read`). Allowlisted in `fluree-bench-virtual::spans`.
        let metadata = async {
            let metadata_bytes = storage
                .read(metadata_location)
                .await
                .map_err(|e| storage_query_error("Failed to read table metadata", e))?;
            let parsed = TableMetadata::from_json(&metadata_bytes).map_err(|e| {
                QueryError::Internal(format!("Failed to parse table metadata: {e}"))
            })?;
            Ok::<_, QueryError>(Arc::new(parsed))
        }
        .instrument(tracing::debug_span!(
            "r2rml.read_metadata",
            metadata_location = %metadata_location,
        ))
        .await?;
        disk.put_metadata(metadata_location, metadata.as_ref());
        metadata
    };
    cache
        .put_metadata(metadata_location.to_string(), Arc::clone(&metadata))
        .await;

    info!(
        metadata_location = %metadata_location,
        format_version = metadata.format_version,
        "Loaded and cached table metadata"
    );

    Ok(metadata)
}

/// Acquire the S3 storage client for a Direct-mode table, reusing the query
/// session's cached client when one is present.
///
/// Direct mode builds from the ambient AWS credential chain
/// (`from_default_chain`: env → `~/.aws` → IMDS/ECS network round-trips) and
/// stands up a fresh connection pool — not free, and repeated for every scan of
/// a table that a correlated join (or the slice-1 prefetch→scan) re-resolves.
/// This mirrors the REST branch's `cached_storage`/`store_storage` reuse
/// (fluree/db#1498). Unlike REST, Direct never calls `store_load_table` (it has
/// no vended credentials to rotate), so the cached client is never invalidated
/// mid-query: the first build is stored and every later resolution of the same
/// table returns that Arc. `cached_storage`/`store_storage` are gated on
/// `cache_enabled()`, so with caching disabled this still builds per call
/// (matching REST and the pre-#1498 behavior).
/// Infer the location remap for a RELOCATED local Direct table — one copied or
/// moved after being written, so its metadata/manifests reference the ORIGINAL
/// root. Both ends of the mapping are already known: the metadata's own
/// `location` (the old root — possibly an `s3://` URI, the copied-from-S3
/// case) and the configured `table_location` (where the table sits now). So
/// "copy the table directory, point at it" needs zero configuration. Identical
/// roots — the common locally-written case — leave the storage untouched, as
/// does any non-local backend.
fn remap_local_storage(
    storage: Arc<IcebergStorageBackend>,
    metadata: &TableMetadata,
    direct_location: Option<&str>,
) -> Arc<IcebergStorageBackend> {
    let Some(configured) = direct_location else {
        return storage;
    };
    if !matches!(storage.as_ref(), IcebergStorageBackend::File(_)) {
        return storage;
    }
    // `file:///x`, `file:/x`, and `/x` are the same local root spelled three
    // ways — never remap between spellings of one root.
    fn local_path(s: &str) -> &str {
        s.strip_prefix("file://")
            .or_else(|| s.strip_prefix("file:"))
            .unwrap_or(s)
    }
    let declared = metadata.location.trim_end_matches('/');
    let configured = configured.trim_end_matches('/');
    if local_path(declared) == local_path(configured) {
        return storage;
    }
    debug!(
        declared = %declared,
        configured = %configured,
        "local Direct table is relocated; remapping manifest file references"
    );
    Arc::new(IcebergStorageBackend::File(FileIcebergStorage::with_remap(
        declared, configured,
    )))
}

async fn direct_session_storage(
    session: &super::catalog_session::IcebergCatalogSession,
    lt_key: &str,
    table_location: &str,
    region: Option<&str>,
    endpoint: Option<&str>,
    path_style: bool,
) -> QueryResult<Arc<IcebergStorageBackend>> {
    if let Some(cached) = session.cached_storage(lt_key) {
        debug!("storage client reused (query-scoped, direct)");
        return Ok(cached);
    }
    // Backend by location scheme: a local (`file://` / absolute-path) table
    // reads the filesystem directly — no SDK client, no credential-chain
    // resolution. Cached in the session like the S3 client for uniformity
    // (the build is nearly free, but callers treat the cache as authoritative).
    if FileIcebergStorage::is_local_location(table_location) {
        let built = Arc::new(IcebergStorageBackend::File(FileIcebergStorage::new()));
        session.store_storage(lt_key.to_string(), Arc::clone(&built));
        return Ok(built);
    }
    // gs://-backed tables (GCS S3-interop endpoint) are read through this same S3
    // SDK path; the client is pinned to HTTP/1.1 to avoid the AWS-SDK HTTP/2
    // range-read bug against that endpoint.
    let built = Arc::new(IcebergStorageBackend::S3(
        S3IcebergStorage::from_default_chain(region, endpoint, path_style)
            .await
            .map_err(|e| QueryError::Internal(format!("Failed to create S3 storage: {e}")))?,
    ));
    session.store_storage(lt_key.to_string(), Arc::clone(&built));
    Ok(built)
}

/// MAJOR-3 (#1529 review): whether a listing of immediate child directory names
/// denotes a SINGLE Iceberg table rather than a warehouse root. Every Iceberg
/// table directory carries a `metadata/` child (its table metadata); a warehouse
/// root instead holds per-table subdirectories. Used to recover the single-table
/// case when the leaf-name warehouse heuristic misfires.
fn listing_is_single_table(listing: &[String]) -> bool {
    listing.iter().any(|d| {
        d.trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or(d)
            .eq_ignore_ascii_case("metadata")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_iceberg::metadata::{Schema, SchemaField};
    use fluree_db_query::r2rml::{ScanCmpOp, ScanFilter, ScanValue};
    use serde_json::json;

    /// F-AUD-1 cache-arm follow-up: the shared guard both scan-files hit arms call
    /// must refuse a delete-bearing cached entry (guard active by default in the
    /// test env) and pass a delete-free one — so a cache HIT can never silently
    /// serve a merge-on-read file list around the plan-time guard.
    #[test]
    fn cached_scan_files_guard_refuses_delete_bearing_entry() {
        assert!(
            guard_cached_scan_files(false, "s3://b/t/metadata/00001.json").is_ok(),
            "a delete-free cached entry is served"
        );
        let err = guard_cached_scan_files(true, "s3://b/dw.fact_orders/metadata/00007.json")
            .expect_err("a delete-bearing cached entry must be refused with the guard active");
        let msg = err.to_string();
        assert!(
            msg.contains("FLUREE_ICEBERG_ALLOW_MOR_DELETES"),
            "the refusal names the override switch: {msg}"
        );
    }

    fn field(id: i32, name: &str, ty: serde_json::Value) -> SchemaField {
        SchemaField {
            id,
            name: name.to_string(),
            required: false,
            field_type: ty,
            doc: None,
        }
    }

    fn key_schema() -> Schema {
        Schema {
            schema_id: 0,
            identifier_field_ids: vec![],
            fields: vec![
                field(1, "int_key", json!("int")),
                field(2, "long_key", json!("long")),
                field(3, "dec_key", json!("decimal(38,0)")),
                field(4, "str_key", json!("string")),
                field(5, "date_key", json!("date")),
                field(6, "double_key", json!("double")),
                field(7, "float_key", json!("float")),
            ],
        }
    }

    /// Build `TableMetadata` from a snapshot chain, through the real JSON
    /// deserializer rather than a struct literal — `meta_with` in the iceberg
    /// crate is `#[cfg(test)]`-private, and going through `from_json_str` has
    /// the side benefit of exercising the path production actually takes.
    ///
    /// `chain` is `(snapshot_id, parent_id, operation)`.
    fn meta_with_chain(chain: &[(i64, Option<i64>, Option<&str>)]) -> TableMetadata {
        let snapshots: Vec<serde_json::Value> = chain
            .iter()
            .enumerate()
            .map(|(i, (id, parent, op))| {
                let mut s = json!({
                    "snapshot-id": id,
                    "sequence-number": i as i64 + 1,
                    "timestamp-ms": 0,
                    "manifest-list": format!("s3://b/t/snap-{id}.avro"),
                    "summary": match op {
                        Some(o) => json!({"operation": o}),
                        // A snapshot with no recorded operation is a real
                        // Iceberg shape and must stay representable: the
                        // safety check treats it as unsafe (fail closed).
                        None => json!({}),
                    },
                });
                if let Some(pid) = parent {
                    s["parent-snapshot-id"] = json!(pid);
                }
                s
            })
            .collect();

        let current = chain.last().map(|(id, _, _)| *id);
        let doc = json!({
            "format-version": 2,
            "location": "s3://b/t",
            "last-sequence-number": chain.len() as i64,
            "last-updated-ms": 0,
            "last-column-id": 1,
            "current-schema-id": 0,
            "schemas": [],
            "current-snapshot-id": current,
            "snapshots": snapshots,
            "snapshot-log": [],
            "default-spec-id": 0,
            "partition-specs": [],
            "last-partition-id": 0,
            "sort-orders": [],
            "default-sort-order-id": 0,
            "properties": {}
        });
        TableMetadata::from_json_str(&doc.to_string()).expect("metadata fixture must parse")
    }

    /// An append-only window takes the cheap path. The baseline: if this ever
    /// stops holding, every poll is reading whole tables.
    #[test]
    fn append_only_window_scans_incrementally() {
        let meta = meta_with_chain(&[
            (1, None, Some("append")),
            (2, Some(1), Some("append")),
            (3, Some(2), Some("replace")),
        ]);
        assert_eq!(
            ScanChoice::decide(&meta, Some(1), 3),
            ScanChoice::Incremental
        );
        assert!(ScanChoice::decide(&meta, Some(1), 3).is_incremental());
    }

    /// No watermark is a FIRST RUN, not a failure to classify. It must not be
    /// reported as undeterminable, or every new source looks like an incident.
    #[test]
    fn missing_watermark_is_initial_not_a_failure() {
        let meta = meta_with_chain(&[(1, None, Some("append"))]);
        assert_eq!(ScanChoice::decide(&meta, None, 1), ScanChoice::FullInitial);
        assert!(!ScanChoice::decide(&meta, None, 1).is_incremental());
    }

    /// `overwrite` in the window: a full read is CORRECT here, and must be
    /// distinguishable from the case below. Both were `false` before.
    #[test]
    fn overwrite_window_is_unsafe_not_undeterminable() {
        let meta = meta_with_chain(&[(1, None, Some("append")), (2, Some(1), Some("overwrite"))]);
        assert_eq!(
            ScanChoice::decide(&meta, Some(1), 2),
            ScanChoice::FullUnsafeWindow
        );
    }

    /// THE REGRESSION TEST. An expired watermark — the snapshot is simply gone
    /// from the table's history, which is what aggressive snapshot retention
    /// does — must be `FullUndeterminable` carrying a reason, NOT silently
    /// folded in with the routine case.
    ///
    /// This is the exact production failure: retention expired the stored
    /// watermark, `.unwrap_or(false)` turned the resulting error into "not
    /// safe", every poll full-read a 728,876-row table, and the process was
    /// OOMKilled every 8-17 minutes with no log line explaining why.
    #[test]
    fn expired_watermark_is_undeterminable_with_a_reason() {
        // Current history is 10 -> 11. Snapshot 1 has been expired away.
        let meta = meta_with_chain(&[(10, None, Some("append")), (11, Some(10), Some("append"))]);

        let choice = ScanChoice::decide(&meta, Some(1), 11);
        match &choice {
            ScanChoice::FullUndeterminable(reason) => {
                assert!(
                    !reason.is_empty(),
                    "the reason is the whole point — it is what tells an operator \
                     retention is shorter than the poll interval"
                );
            }
            other => panic!("expired watermark must be FullUndeterminable, got {other:?}"),
        }
        assert!(
            !choice.is_incremental(),
            "must not attempt an incremental scan"
        );
    }

    /// A watermark from a different lineage (branch/rewind) is also
    /// undeterminable rather than unsafe — same treatment, different cause.
    #[test]
    fn non_ancestor_watermark_is_undeterminable() {
        let meta = meta_with_chain(&[(1, None, Some("append")), (2, Some(1), Some("append"))]);
        // 2 is a DESCENDANT of 1, so asking for the window (2, 1] walks off the
        // root without finding 2 -> not an ancestor.
        assert!(matches!(
            ScanChoice::decide(&meta, Some(2), 1),
            ScanChoice::FullUndeterminable(_)
        ));
    }

    /// The four outcomes must stay four. Collapsing any two back together is
    /// how the original bug is reintroduced, so assert they are all distinct.
    #[test]
    fn the_four_outcomes_are_distinguishable() {
        let ok = meta_with_chain(&[(1, None, Some("append")), (2, Some(1), Some("append"))]);
        let bad = meta_with_chain(&[(1, None, Some("append")), (2, Some(1), Some("delete"))]);
        let gone = meta_with_chain(&[(9, None, Some("append")), (10, Some(9), Some("append"))]);

        let all = [
            ScanChoice::decide(&ok, Some(1), 2),
            ScanChoice::decide(&ok, None, 2),
            ScanChoice::decide(&bad, Some(1), 2),
            ScanChoice::decide(&gone, Some(1), 10),
        ];
        for (i, a) in all.iter().enumerate() {
            for (j, b) in all.iter().enumerate() {
                if i != j {
                    assert_ne!(a, b, "outcomes {i} and {j} collapsed into one another");
                }
            }
        }
    }

    fn key_filter(col: &str, raw: &str) -> ScanFilter {
        ScanFilter {
            column: col.to_string(),
            op: ScanCmpOp::Eq,
            value: ScanValue::TemplateKey(raw.to_string()),
        }
    }

    fn only_literal(filters: &[ScanFilter], schema: &Schema) -> Option<LiteralValue> {
        match build_iceberg_filter(filters, schema)? {
            Expression::Comparison { value, .. } => Some(value),
            other => panic!("expected a single comparison, got {other:?}"),
        }
    }

    #[test]
    fn template_key_coerces_by_physical_type() {
        let s = key_schema();
        assert!(matches!(
            only_literal(&[key_filter("int_key", "5")], &s),
            Some(LiteralValue::Int32(5))
        ));
        assert!(matches!(
            only_literal(&[key_filter("long_key", "5")], &s),
            Some(LiteralValue::Int64(5))
        ));
        // Integer key on a Decimal column pushes as Int64 — the Arrow reader casts
        // it to the Decimal column (the validated integer-vs-decimal path).
        assert!(matches!(
            only_literal(&[key_filter("dec_key", "5")], &s),
            Some(LiteralValue::Int64(5))
        ));
        // The raw string is already percent-decoded upstream.
        assert!(matches!(
            only_literal(&[key_filter("str_key", "west/5")], &s),
            Some(LiteralValue::String(v)) if v == "west/5"
        ));
    }

    #[test]
    fn date_scalar_pushed_only_against_date_column() {
        let s = key_schema();
        let date_filter = |col: &str| ScanFilter {
            column: col.to_string(),
            op: ScanCmpOp::Eq,
            value: ScanValue::Date(19_737), // 2024-01-15, days since epoch
        };
        // Physically-`date` column: the scan filter compares like the operator.
        assert!(matches!(
            only_literal(&[date_filter("date_key")], &s),
            Some(LiteralValue::Date(19_737))
        ));
        // Physically-`string` column: skip. The operator's lenient `Date::parse`
        // keeps `"2024-01-15Z"`/offset forms that the exact row filter (Date32 →
        // Utf8 `"2024-01-15"`) would drop — pushing here would remove an
        // operator-kept row.
        assert!(build_iceberg_filter(&[date_filter("str_key")], &s).is_none());
    }

    #[test]
    fn double_pushed_only_against_double_column() {
        let s = key_schema();
        let dbl = |col: &str| ScanFilter {
            column: col.to_string(),
            op: ScanCmpOp::Lt,
            value: ScanValue::Double(9.99),
        };
        // Physically-`double`: pushes as an exact f64 bound.
        assert!(matches!(
            only_literal(&[dbl("double_key")], &s),
            Some(LiteralValue::Float64(v)) if v == 9.99
        ));
        // Physically-`float`: skipped (an f64→f32 narrowing could round the
        // literal and over-prune a range).
        assert!(build_iceberg_filter(&[dbl("float_key")], &s).is_none());
        // Non-numeric column: skipped.
        assert!(build_iceberg_filter(&[dbl("str_key")], &s).is_none());
    }

    fn in_filter(col: &str, values: Vec<ScanValue>) -> ScanFilter {
        ScanFilter {
            column: col.to_string(),
            op: ScanCmpOp::In,
            value: ScanValue::Set(values),
        }
    }

    #[test]
    fn in_set_builds_expression_in_over_scalar_members() {
        // Item 7: a set filter lowers to `Expression::In` with one literal per
        // member, resolved against the column's physical type.
        let s = key_schema();
        let f = in_filter(
            "str_key",
            vec![
                ScanValue::Str("a".into()),
                ScanValue::Str("b".into()),
                ScanValue::Str("c".into()),
            ],
        );
        match build_iceberg_filter(&[f], &s) {
            Some(Expression::In { column, values, .. }) => {
                assert_eq!(column, "str_key");
                assert_eq!(
                    values,
                    vec![
                        LiteralValue::String("a".into()),
                        LiteralValue::String("b".into()),
                        LiteralValue::String("c".into()),
                    ]
                );
            }
            other => panic!("expected Expression::In, got {other:?}"),
        }
    }

    #[test]
    fn in_set_over_int_column_coerces_each_member() {
        let s = key_schema();
        let f = in_filter(
            "int_key",
            vec![ScanValue::Int(1), ScanValue::Int(2), ScanValue::Int(3)],
        );
        match build_iceberg_filter(&[f], &s) {
            Some(Expression::In { values, .. }) => assert_eq!(
                values,
                vec![
                    LiteralValue::Int32(1),
                    LiteralValue::Int32(2),
                    LiteralValue::Int32(3)
                ]
            ),
            other => panic!("expected Expression::In, got {other:?}"),
        }
    }

    #[test]
    fn in_set_declines_whole_when_any_member_cannot_push() {
        // A Date member against a physically-`string` column can't push; the WHOLE
        // In is dropped — a partial In could prune a file the Date member's rows
        // live in, and the in-engine FILTER could never recover them.
        let s = key_schema();
        let f = in_filter(
            "str_key",
            vec![ScanValue::Str("a".into()), ScanValue::Date(19_737)],
        );
        assert!(build_iceberg_filter(&[f], &s).is_none());
    }

    #[test]
    fn in_set_on_unknown_column_is_skipped() {
        let s = key_schema();
        let f = in_filter("nope", vec![ScanValue::Int(1)]);
        assert!(build_iceberg_filter(&[f], &s).is_none());
    }

    fn ts_schema() -> Schema {
        Schema {
            schema_id: 0,
            identifier_field_ids: vec![],
            fields: vec![
                field(1, "ts_ntz", json!("timestamp")),
                field(2, "ts_tz", json!("timestamptz")),
            ],
        }
    }

    fn ts_filter(col: &str, micros: i64, tz: bool) -> ScanFilter {
        ScanFilter {
            column: col.to_string(),
            op: ScanCmpOp::Gt,
            value: ScanValue::Timestamp { micros, tz },
        }
    }

    #[test]
    fn timestamp_pushdown_is_frame_matched() {
        // Item 10: a tz-AWARE (UTC) literal pushes only against a `timestamptz`
        // column; a NAIVE (wall-clock) literal only against a `timestamp` column.
        let s = ts_schema();
        assert!(matches!(
            only_literal(&[ts_filter("ts_tz", 1_717_200_000_000_000, true)], &s),
            Some(LiteralValue::TimestampTz(1_717_200_000_000_000))
        ));
        assert!(matches!(
            only_literal(&[ts_filter("ts_ntz", 42, false)], &s),
            Some(LiteralValue::Timestamp(42))
        ));
        // Frame mismatch declines (the micros would not be comparable to the file
        // bounds): a tz-aware literal on a `timestamp` column, or vice versa.
        assert!(build_iceberg_filter(&[ts_filter("ts_ntz", 42, true)], &s).is_none());
        assert!(build_iceberg_filter(&[ts_filter("ts_tz", 42, false)], &s).is_none());
    }

    #[test]
    fn timestamp_pushdown_declines_non_timestamp_column() {
        // A dateTime literal against a non-timestamp column never pushes.
        let s = key_schema();
        assert!(build_iceberg_filter(&[ts_filter("int_key", 42, false)], &s).is_none());
        assert!(build_iceberg_filter(&[ts_filter("str_key", 42, true)], &s).is_none());
    }

    #[test]
    fn int_literal_coerces_to_scale0_decimal_only_when_numeric_stats_on() {
        // On: an integer against a decimal column → EXACT scale-0 decimal (prunable).
        assert!(matches!(
            int_pushdown_literal(1_000_000, Some("decimal(38,2)"), true),
            Some(LiteralValue::Decimal {
                unscaled: 1_000_000,
                scale: 0,
                ..
            })
        ));
        // Off (revert guarantee): stays Int64 → the decimal bound compare declines
        // → no prune, exactly the pre-PR-7 behavior.
        assert!(matches!(
            int_pushdown_literal(1_000_000, Some("decimal(38,2)"), false),
            Some(LiteralValue::Int64(1_000_000))
        ));
        // An `int` column narrows to Int32; an out-of-range literal skips (no wrap).
        assert!(matches!(
            int_pushdown_literal(5, Some("int"), true),
            Some(LiteralValue::Int32(5))
        ));
        assert!(int_pushdown_literal(i64::from(i32::MAX) + 1, Some("int"), true).is_none());
        // `long` / other columns: Int64 unchanged, on or off.
        assert!(matches!(
            int_pushdown_literal(5, Some("long"), true),
            Some(LiteralValue::Int64(5))
        ));
        assert!(matches!(
            int_pushdown_literal(5, Some("long"), false),
            Some(LiteralValue::Int64(5))
        ));
    }

    #[test]
    fn int_scalar_against_decimal_column_pushes_scale0_decimal() {
        // End-to-end through build_iceberg_filter with the default (on) switch: an
        // integer FILTER literal on a decimal column becomes a scale-0 decimal.
        let s = key_schema();
        let f = ScanFilter {
            column: "dec_key".to_string(),
            op: ScanCmpOp::Gt,
            value: ScanValue::Int(1_000_000),
        };
        assert!(matches!(
            only_literal(&[f], &s),
            Some(LiteralValue::Decimal {
                unscaled: 1_000_000,
                scale: 0,
                ..
            })
        ));
    }

    #[test]
    fn decimal_pushed_only_against_decimal_column_preserving_literal_scale() {
        let s = key_schema();
        // The `ScanValue::Decimal` carries the LITERAL's scale (9.99 → scale 2);
        // the column is decimal(38,0). The bridge preserves the literal scale —
        // the bound compare normalizes across the column/literal scale gap.
        let dec = |col: &str| ScanFilter {
            column: col.to_string(),
            op: ScanCmpOp::Lt,
            value: ScanValue::Decimal {
                unscaled: 999,
                precision: 3,
                scale: 2,
            },
        };
        assert!(matches!(
            only_literal(&[dec("dec_key")], &s),
            Some(LiteralValue::Decimal {
                unscaled: 999,
                scale: 2,
                ..
            })
        ));
        // Non-decimal columns: skipped (no cross-type bound compare exists).
        assert!(build_iceberg_filter(&[dec("long_key")], &s).is_none());
        assert!(build_iceberg_filter(&[dec("str_key")], &s).is_none());
    }

    #[test]
    fn template_key_skips_unsupported_or_unparseable() {
        let s = key_schema();
        // Date physical type is not pushed yet (needs a live decimal/date check).
        assert!(build_iceberg_filter(&[key_filter("date_key", "2024-01-15")], &s).is_none());
        // Non-integer value against an integer column → skip (operator enforces).
        assert!(build_iceberg_filter(&[key_filter("int_key", "abc")], &s).is_none());
        assert!(build_iceberg_filter(&[key_filter("dec_key", "5.5")], &s).is_none());
        // Unknown column → skip.
        assert!(build_iceberg_filter(&[key_filter("nope", "5")], &s).is_none());
    }

    // ------------------------------------------------------------------
    // COUNT(*) manifest shortcut soundness (`sound_manifest_row_count`).
    // The decision core is pure over the manifest read result, so the gates
    // are exercised directly against hand-built DataFiles (the same fixture
    // style as `fluree_db_iceberg::stats` tests).
    // ------------------------------------------------------------------

    use fluree_db_iceberg::manifest::{DataFile, FileFormat, PartitionData};
    use std::collections::HashMap;

    fn count_schema() -> Schema {
        Schema {
            schema_id: 0,
            identifier_field_ids: vec![1],
            fields: vec![
                field(1, "SALE_KEY", json!("long")),
                field(2, "AMOUNT", json!("decimal(18,2)")),
            ],
        }
    }

    /// A data file with `record_count` rows. `null_value_counts` = `Some(pairs)`
    /// makes the file report those per-field null counts; `None` makes it report
    /// no null counts at all (to simulate absent/partial coverage).
    fn count_data_file(record_count: i64, null_value_counts: Option<&[(i32, i64)]>) -> DataFile {
        DataFile {
            file_path: "s3://b/t/data/f.parquet".to_string(),
            file_format: FileFormat::Parquet,
            record_count,
            file_size_in_bytes: 1000,
            partition: PartitionData::default(),
            column_sizes: None,
            value_counts: None,
            null_value_counts: null_value_counts
                .map(|pairs| pairs.iter().copied().collect::<HashMap<i32, i64>>()),
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            split_offsets: None,
            sort_order_id: None,
        }
    }

    #[test]
    fn count_shortcut_clean_table_returns_exact_count() {
        let schema = count_schema();
        // Two files; every required column reports zero nulls in EVERY file (full
        // coverage), so the record_count sum equals a full-scan COUNT.
        let files = vec![
            count_data_file(100, Some(&[(1, 0), (2, 0)])),
            count_data_file(200, Some(&[(1, 0), (2, 0)])),
        ];
        let cols = vec!["SALE_KEY".to_string(), "AMOUNT".to_string()];
        assert_eq!(
            sound_manifest_row_count(&schema, &files, false, &cols),
            Some(300)
        );
    }

    #[test]
    fn count_shortcut_declines_with_delete_manifests() {
        let schema = count_schema();
        let files = vec![count_data_file(300, Some(&[(1, 0), (2, 0)]))];
        // A merge-on-read delete manifest makes record_count an over-count.
        assert_eq!(
            sound_manifest_row_count(&schema, &files, true, &["SALE_KEY".to_string()]),
            None
        );
    }

    #[test]
    fn count_shortcut_declines_nullable_column() {
        let schema = count_schema();
        // AMOUNT carries 5 nulls; a COUNT requiring AMOUNT non-null must not adopt
        // the manifest total (which counts those rows).
        let files = vec![count_data_file(300, Some(&[(1, 0), (2, 5)]))];
        assert_eq!(
            sound_manifest_row_count(&schema, &files, false, &["AMOUNT".to_string()]),
            None
        );
        // Same table, but only the provably zero-null key is required → sound.
        assert_eq!(
            sound_manifest_row_count(&schema, &files, false, &["SALE_KEY".to_string()]),
            Some(300)
        );
    }

    #[test]
    fn count_shortcut_declines_when_null_stats_absent() {
        let schema = count_schema();
        // Two files, but only one reports a null count for the key: partial
        // coverage → aggregate_column_stats yields null_count None (unknown),
        // which must NOT be read as zero.
        let files = vec![
            count_data_file(100, Some(&[(1, 0)])),
            count_data_file(200, None),
        ];
        assert_eq!(
            sound_manifest_row_count(&schema, &files, false, &["SALE_KEY".to_string()]),
            None
        );
    }

    #[test]
    fn count_shortcut_constant_subject_needs_no_null_proof() {
        // Empty non_null_cols = constant-subject mapping: a row exists for every
        // table row, so the count is sound with no per-column null proof — even
        // when NO file reports any null stats.
        let schema = count_schema();
        let files = vec![count_data_file(100, None), count_data_file(200, None)];
        assert_eq!(
            sound_manifest_row_count(&schema, &files, false, &[]),
            Some(300)
        );
        // A delete manifest still declines, even for a constant subject.
        assert_eq!(sound_manifest_row_count(&schema, &files, true, &[]), None);
    }

    #[test]
    fn count_shortcut_declines_corrupt_record_counts() {
        let schema = count_schema();
        // A negative per-file record_count (corrupt manifest) declines — even
        // though the SUM (10 - 5 = 5) is positive and a sign check on the
        // aggregate alone would have served it as an "exact" count.
        let files = vec![count_data_file(10, None), count_data_file(-5, None)];
        assert_eq!(sound_manifest_row_count(&schema, &files, false, &[]), None);

        // Per-file counts whose total overflows u64 decline (three i64::MAX
        // files: the wrapped i64 sum would land positive at ~2^63-3, so a plain
        // sign check on the aggregate would happily pass it).
        let files = vec![
            count_data_file(i64::MAX, None),
            count_data_file(i64::MAX, None),
            count_data_file(i64::MAX, None),
        ];
        assert_eq!(sound_manifest_row_count(&schema, &files, false, &[]), None);
    }

    #[test]
    fn count_shortcut_declines_unknown_column() {
        let schema = count_schema();
        let files = vec![count_data_file(300, Some(&[(1, 0), (2, 0)]))];
        // A required column absent from the schema cannot be proven non-null.
        assert_eq!(
            sound_manifest_row_count(&schema, &files, false, &["NOPE".to_string()]),
            None
        );
    }

    // ---- §1 (fluree/db#1500): inline loadTable metadata short-circuits S3 ----

    use crate::graph_source::disk_catalog_cache::DiskCatalogCache;
    use fluree_db_iceberg::error::Result as IcebergResult;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Minimal valid Iceberg table metadata JSON (only the non-defaulted fields),
    /// so tests build `TableMetadata` through the real `from_json` path.
    fn sample_metadata_json(location: &str) -> String {
        serde_json::json!({
            "format-version": 2,
            "location": location,
            "last-updated-ms": 0,
            "last-column-id": 0,
        })
        .to_string()
    }

    fn sample_metadata(location: &str) -> TableMetadata {
        TableMetadata::from_json(sample_metadata_json(location).as_bytes())
            .expect("sample metadata JSON parses")
    }

    /// Storage stub that panics on any access: the inline-metadata path must never
    /// touch storage, so any read here fails the test loudly.
    #[derive(Debug)]
    struct NeverReadStorage;

    #[async_trait::async_trait]
    impl SendIcebergStorage for NeverReadStorage {
        async fn read(&self, path: &str) -> IcebergResult<bytes::Bytes> {
            panic!("storage.read must not be called on the inline-metadata path (path={path})");
        }
        async fn read_range(
            &self,
            _path: &str,
            _range: std::ops::Range<u64>,
        ) -> IcebergResult<bytes::Bytes> {
            panic!("storage.read_range must not be called on the inline-metadata path");
        }
        async fn file_size(&self, _path: &str) -> IcebergResult<u64> {
            panic!("storage.file_size must not be called on the inline-metadata path");
        }
    }

    /// Storage stub that serves a fixed metadata JSON and counts reads, for the
    /// fallback-intact test (no inline metadata => a real object read must happen).
    #[derive(Debug)]
    struct CountingStorage {
        body: bytes::Bytes,
        reads: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl SendIcebergStorage for CountingStorage {
        async fn read(&self, _path: &str) -> IcebergResult<bytes::Bytes> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.body.clone())
        }
        async fn read_range(
            &self,
            _path: &str,
            _range: std::ops::Range<u64>,
        ) -> IcebergResult<bytes::Bytes> {
            unreachable!("resolve_table_metadata reads whole objects, not ranges")
        }
        async fn file_size(&self, _path: &str) -> IcebergResult<u64> {
            unreachable!("resolve_table_metadata does not stat")
        }
    }

    /// A disk cache rooted at a unique temp dir (a guaranteed miss on first read).
    fn tmp_disk_cache(tag: &str) -> (DiskCatalogCache, std::path::PathBuf) {
        let dir =
            std::env::temp_dir().join(format!("fluree-r2rml-md-test-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        (DiskCatalogCache::for_dir(&dir), dir)
    }

    #[tokio::test]
    async fn inline_metadata_short_circuits_s3_and_seeds_both_caches() {
        let cache = R2rmlCache::new(4, 4);
        let loc = "s3://bucket/warehouse/t/metadata/v3.metadata.json";
        let inline = sample_metadata("s3://bucket/warehouse/t");
        let (disk, dir) = tmp_disk_cache("inline");

        // In-memory cache empty (miss); inline metadata present. `NeverReadStorage`
        // panics on any access, so a green result proves S3 was never touched —
        // without §1's inline branch this would fall through to the disk miss and
        // then a storage read, which panics here.
        let out = resolve_table_metadata(&cache, &NeverReadStorage, loc, Some(inline), &disk)
            .await
            .expect("inline metadata resolves without any storage read");
        assert_eq!(out.location, "s3://bucket/warehouse/t");

        // The inline result seeds the in-memory cache so a later cache-reconstructed
        // load (which carries `metadata: None`) hits it instead of re-fetching.
        assert!(
            cache.get_metadata(loc).await.is_some(),
            "inline metadata must be cached in-memory for later loads"
        );

        // ...and seeds the DISK layer, which is what the NEXT PROCESS's pointer rung
        // reads via `metadata_from_caches` (its in-memory cache is empty). Skipping
        // this write would silently starve the zero-REST first-ask for exactly the
        // catalogs that vend inline metadata — the regression would show up only as
        // the cache gate's `load_table.n` going non-zero. Asserted only when disk
        // caching is enabled; `put_metadata` is a no-op when the env switch is off.
        if crate::graph_source::disk_catalog_cache::disk_catalog_cache_enabled() {
            assert!(
                disk.get_metadata(loc).is_some(),
                "inline metadata must seed the disk layer for the pointer rung"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Composition guards (restack onto the loadTable-METADATA cache chain) ----
    //
    // These cover the seams where OUR credential handling meets the chain's
    // pointer-rung / `LazyS3Storage` architecture. Neither suite could see them
    // before: ours predates the rung, theirs predates SecretRef / fail-closed /
    // typed errors. Each one FAILS if the corresponding guard is dropped in a
    // future merge.

    fn vended_response(loc: &str, with_creds: bool) -> LoadTableResponse {
        LoadTableResponse {
            metadata_location: loc.to_string(),
            config: std::collections::HashMap::default(),
            credentials: with_creds.then(|| fluree_db_iceberg::credential::VendedCredentials {
                access_key_id: "AKIAEXAMPLE".to_string(),
                secret_access_key: "secret".to_string(),
                session_token: None,
                expires_at: None,
                endpoint: None,
                region: Some("us-east-2".to_string()),
                path_style: false,
            }),
            metadata: None,
        }
    }

    /// §2 on the LAZY path: a REST source configured to require vended credentials
    /// whose catalog vends none must FAIL CLOSED — never silently read under the
    /// process's ambient AWS identity. `rest_session_storage` is the one decision
    /// point the eager arm AND the deferred `LazyS3Storage` builder share, so this
    /// covers both; a second, unguarded copy is exactly the fail-open this pins.
    #[tokio::test]
    async fn rest_storage_fails_closed_when_catalog_vends_nothing() {
        let session = super::super::catalog_session::IcebergCatalogSession::default();
        let lt_key = super::super::catalog_session::IcebergCatalogSession::load_table_key(
            "gs:main",
            "DW",
            "DIM_STORE",
        );
        let io = IoConfig {
            vended_credentials: true, // the default
            ..Default::default()
        };
        let resp = vended_response("s3://b/t/metadata/v1.metadata.json", false);

        let err = rest_session_storage(&session, &lt_key, &io, &resp, "https://cat.example/v1")
            .await
            .expect_err("vended-configured source with no vended creds must fail closed");
        match err {
            QueryError::CatalogCredentialsNotVended { catalog_uri } => {
                assert_eq!(catalog_uri, "https://cat.example/v1");
            }
            other => panic!("expected CatalogCredentialsNotVended, got: {other:?}"),
        }
        // ...and it must NOT have cached an ambient client under this key.
        assert!(
            session.cached_storage(&lt_key).is_none(),
            "fail-closed must not leave a storage client in the session"
        );
    }

    /// The explicit ambient opt-in (`vended_credentials = false`) still resolves —
    /// fail-closed must not turn BYO-IAM into an error.
    #[tokio::test]
    async fn rest_storage_allows_explicit_ambient_opt_in() {
        let session = super::super::catalog_session::IcebergCatalogSession::default();
        let lt_key = super::super::catalog_session::IcebergCatalogSession::load_table_key(
            "gs:main",
            "DW",
            "DIM_AMBIENT",
        );
        let io = IoConfig {
            vended_credentials: false, // explicit BYO-IAM
            s3_region: Some("us-east-2".to_string()),
            ..Default::default()
        };
        let resp = vended_response("s3://b/t/metadata/v1.metadata.json", false);
        rest_session_storage(&session, &lt_key, &io, &resp, "https://cat.example/v1")
            .await
            .expect("explicit vended_credentials=false must use the ambient chain");
    }

    /// §5 across the deferred builder's `IcebergError` channel: the typed errors must
    /// round-trip back to their `QueryError` counterparts (and thus their 403 wire
    /// codes) instead of being flattened to an opaque string by a blanket
    /// `to_string()`. Mirrors the `map_err` the `LazyS3Storage` builder applies.
    #[test]
    fn typed_storage_errors_survive_the_lazy_builder_channel() {
        let denied = QueryError::StorageAccessDenied {
            bucket: "b".to_string(),
            key: "k".to_string(),
            region: Some("us-east-2".to_string()),
            message: "AccessDenied ... no identity-based policy allows".to_string(),
        };
        let ferried = match denied {
            QueryError::StorageAccessDenied {
                bucket,
                key,
                region,
                message,
            } => IcebergError::StorageAccessDenied {
                bucket,
                key,
                region,
                message,
            },
            other => IcebergError::Catalog(other.to_string()),
        };
        match super::super::iceberg_catalog::storage_query_error("ctx", ferried) {
            QueryError::StorageAccessDenied { bucket, key, .. } => {
                assert_eq!((bucket.as_str(), key.as_str()), ("b", "k"));
            }
            other => panic!("StorageAccessDenied must survive the builder: {other:?}"),
        }

        let not_vended = QueryError::CatalogCredentialsNotVended {
            catalog_uri: "https://cat.example/v1".to_string(),
        };
        let ferried = match not_vended {
            QueryError::CatalogCredentialsNotVended { catalog_uri } => {
                IcebergError::CatalogCredentialsNotVended { catalog_uri }
            }
            other => IcebergError::Catalog(other.to_string()),
        };
        match super::super::iceberg_catalog::storage_query_error("ctx", ferried) {
            QueryError::CatalogCredentialsNotVended { catalog_uri } => {
                assert_eq!(catalog_uri, "https://cat.example/v1");
            }
            other => panic!("CatalogCredentialsNotVended must survive the builder: {other:?}"),
        }
    }

    #[tokio::test]
    async fn no_inline_metadata_falls_back_to_storage_read() {
        let cache = R2rmlCache::new(4, 4);
        let loc = "s3://bucket/warehouse/u/metadata/v1.metadata.json";
        let reads = Arc::new(AtomicUsize::new(0));
        let storage = CountingStorage {
            body: bytes::Bytes::from(sample_metadata_json("s3://bucket/warehouse/u")),
            reads: Arc::clone(&reads),
        };

        // No inline metadata and an empty in-memory cache: resolution must reach
        // storage. A unique temp dir keeps the disk cache a guaranteed miss (or a
        // no-op when disk caching is disabled), so the resolution proceeds to the
        // object read regardless of the ambient disk-cache setting.
        let (disk, dir) = tmp_disk_cache("fallback");
        let out = resolve_table_metadata(&cache, &storage, loc, None, &disk)
            .await
            .expect("fallback resolves via the storage read");
        assert_eq!(out.location, "s3://bucket/warehouse/u");
        assert_eq!(
            reads.load(Ordering::SeqCst),
            1,
            "fallback path must perform exactly one storage read"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── ORDERING-TRAP GUARD: rest_client_cache_key must fingerprint the RAW,
    //    reference-bearing config, NOT a hydrated one. ──

    #[derive(Debug)]
    struct FixedResolver(&'static str);

    #[async_trait]
    impl fluree_db_iceberg::SecretResolver for FixedResolver {
        async fn resolve_secret(
            &self,
            _secret_ref: &str,
        ) -> std::result::Result<String, fluree_db_iceberg::SecretResolveError> {
            Ok(self.0.to_string())
        }
    }

    fn oauth2_auth(
        client_secret: fluree_db_iceberg::ConfigValue,
    ) -> fluree_db_iceberg::auth::AuthConfig {
        fluree_db_iceberg::auth::AuthConfig::OAuth2ClientCredentials {
            token_url: "https://c.example.com/token".to_string(),
            client_id: fluree_db_iceberg::ConfigValue::literal("svc"),
            client_secret,
            scope: None,
            audience: None,
        }
    }

    fn gs_config_json(auth: fluree_db_iceberg::auth::AuthConfig) -> String {
        use fluree_db_iceberg::config::{CatalogConfig, IoConfig, TableConfig};
        fluree_db_iceberg::IcebergGsConfig {
            catalog: CatalogConfig::Rest {
                catalog_type: "rest".to_string(),
                uri: "https://c.example.com".to_string(),
                auth,
                warehouse: None,
            },
            table: TableConfig::Identifier("ns.t".to_string()),
            io: IoConfig::default(),
            mapping: None,
            // This fixture exercises auth fingerprinting, not CDC semantics, so the
            // materialization options stay absent (their serde defaults).
            delete: None,
            order_by: None,
        }
        .to_json()
        .unwrap()
    }

    /// The fingerprint keys the process-wide REST client cache. It MUST be stable
    /// across secret rotations (so a warm server does one OAuth exchange per client
    /// lifetime, not one per query) — which holds ONLY because it is computed over
    /// the raw, reference-bearing config, BEFORE hydration. This test proves that
    /// discipline: fingerprinting the ref-bearing config is rotation-stable, while
    /// fingerprinting a hydrated config would re-key on every rotation.
    #[tokio::test]
    async fn secret_ref_fingerprint_is_rotation_stable() {
        use std::sync::Arc;
        let gsid = "gs-1";

        // Raw stored config carries the REFERENCE (this is `record.config` at the
        // scan site). Recomputing over it is identical even though the secret
        // behind the ref may have rotated between queries: the cache is NOT re-keyed.
        let raw = gs_config_json(oauth2_auth(fluree_db_iceberg::ConfigValue::SecretRef {
            secret_ref: "vault://cs".to_string(),
        }));
        let key_raw = rest_client_cache_key(gsid, &raw);
        assert_eq!(
            rest_client_cache_key(gsid, &raw),
            key_raw,
            "the ref-bearing fingerprint must be stable across rotations"
        );

        // Hydrate the SAME auth with two different resolver outputs (a rotation),
        // serialize the hydrated configs, and fingerprint those. They differ —
        // which is EXACTLY why hydration must run AFTER the fingerprint, never
        // before (hydrating first would re-key the client cache every rotation).
        let auth = oauth2_auth(fluree_db_iceberg::ConfigValue::SecretRef {
            secret_ref: "vault://cs".to_string(),
        });
        let r1: Arc<dyn fluree_db_iceberg::SecretResolver> = Arc::new(FixedResolver("secret-v1"));
        let r2: Arc<dyn fluree_db_iceberg::SecretResolver> = Arc::new(FixedResolver("secret-v2"));
        let hydrated_v1 = gs_config_json(auth.hydrate(Some(&r1)).await.unwrap());
        let hydrated_v2 = gs_config_json(auth.hydrate(Some(&r2)).await.unwrap());
        let key_h1 = rest_client_cache_key(gsid, &hydrated_v1);
        let key_h2 = rest_client_cache_key(gsid, &hydrated_v2);
        assert_ne!(
            key_h1, key_h2,
            "hydrated configs with rotated secrets differ → hydrating before the \
             fingerprint would re-key the client cache every rotation"
        );
        assert_ne!(
            key_raw, key_h1,
            "the raw ref-bearing key must differ from the hydrated key"
        );
    }

    /// fluree/db#1498: Direct mode resolves its S3 client through the query
    /// session cache, so two resolutions of the same table in one query share ONE
    /// client instead of rebuilding it (credential-chain resolution + a fresh
    /// connection pool) per scan. Direct mode never calls `store_load_table`, so
    /// nothing invalidates the cached client mid-query — the second acquisition
    /// must return the first Arc. `from_default_chain(Some(region), ..)` builds an
    /// SDK client offline (region set, ambient creds resolved lazily, no request),
    /// so this runs in CI without AWS credentials. This is the strongest seam that
    /// exercises the actual fix without a live catalog: it drives the exact helper
    /// the Direct branch now calls in both metadata-location arms.
    #[tokio::test]
    async fn direct_session_storage_reuses_arc_across_calls() {
        use crate::graph_source::catalog_session::IcebergCatalogSession;
        let session = IcebergCatalogSession::default();
        let key = IcebergCatalogSession::load_table_key("gs:main", "DW", "DIM_STORE");

        let first = direct_session_storage(
            &session,
            &key,
            "s3://bucket/t",
            Some("us-east-2"),
            None,
            false,
        )
        .await
        .expect("offline SDK client construction");
        let second = direct_session_storage(
            &session,
            &key,
            "s3://bucket/t",
            Some("us-east-2"),
            None,
            false,
        )
        .await
        .expect("offline SDK client construction");
        assert!(
            Arc::ptr_eq(&first, &second),
            "the second Direct-mode acquisition must reuse the session-cached S3 client"
        );

        // A different table (different key) is a distinct client — the cache keys
        // per (source, table), so unrelated tables never alias one client.
        let other_key = IcebergCatalogSession::load_table_key("gs:main", "DW", "DIM_GEOGRAPHY");
        let other = direct_session_storage(
            &session,
            &other_key,
            "s3://bucket/t",
            Some("us-east-2"),
            None,
            false,
        )
        .await
        .expect("offline SDK client construction");
        assert!(
            !Arc::ptr_eq(&first, &other),
            "a different table must not reuse another table's cached client"
        );
    }

    #[test]
    fn listing_is_single_table_detects_metadata_child() {
        // MAJOR-3: a single Iceberg table dir has a `metadata/` child (alongside
        // `data/`); a warehouse root instead holds per-table subdirs. The former
        // must be recovered as single-table even when the leaf-name heuristic
        // misfired (suffixed dir, or a dir literally `DW.FACT_ORDER`).
        assert!(super::listing_is_single_table(&[
            "data/".to_string(),
            "metadata/".to_string(),
        ]));
        assert!(super::listing_is_single_table(&["METADATA/".to_string()]));
        // A warehouse root (only table subdirs, no metadata/ child) is NOT single.
        assert!(!super::listing_is_single_table(&[
            "fact_order.UIHGsQex/".to_string(),
            "dim_customer.AbCdEf/".to_string(),
        ]));
        assert!(!super::listing_is_single_table(&[]));
    }

    fn t(seq: i64, rows: i64) -> fluree_db_iceberg::scan::FileScanTask {
        let df = fluree_db_iceberg::manifest::DataFile {
            file_path: format!("f{seq}-{rows}.parquet"),
            file_format: fluree_db_iceberg::manifest::FileFormat::Parquet,
            record_count: rows,
            file_size_in_bytes: rows,
            partition: fluree_db_iceberg::manifest::PartitionData::default(),
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            split_offsets: None,
            sort_order_id: None,
        };
        fluree_db_iceberg::scan::FileScanTask::for_whole_file(df, vec![], None)
            .with_data_sequence_number(seq)
    }

    /// The cut lands on a COMMIT boundary, never inside one. A partial commit
    /// leaves the target in a state no snapshot names, and an unnameable state
    /// cannot be checkpointed or resumed from.
    #[test]
    fn full_read_prefix_cuts_on_a_commit_boundary() {
        // commit 10: 60 rows, commit 20: 60, commit 30: 60 => 180 total
        let tasks = vec![
            t(10, 30),
            t(10, 30),
            t(20, 30),
            t(20, 30),
            t(30, 30),
            t(30, 30),
        ];

        // Budget runs out inside commit 20 -> keep all of 20, cut there.
        assert_eq!(
            super::full_read_prefix(&tasks, 70),
            super::FullReadCut::Cut(20)
        );
        // Budget runs out on the first file of commit 10 -> cut at 10.
        assert_eq!(
            super::full_read_prefix(&tasks, 10),
            super::FullReadCut::Cut(10)
        );
    }

    /// Every reason to decline to cut. Each returns `None`, meaning "read it
    /// whole" — the pre-existing behaviour.
    #[test]
    fn full_read_prefix_declines_when_a_cut_would_not_help() {
        let tasks = vec![t(10, 30), t(20, 30), t(30, 30)];

        use super::FullReadCut;
        assert_eq!(
            super::full_read_prefix(&tasks, 0),
            FullReadCut::Disabled,
            "disabled"
        );
        assert_eq!(
            super::full_read_prefix(&tasks, -1),
            FullReadCut::Disabled,
            "negative"
        );
        assert_eq!(
            super::full_read_prefix(&tasks, 90),
            FullReadCut::PlanFits,
            "already fits"
        );
        assert_eq!(
            super::full_read_prefix(&tasks, 1_000),
            FullReadCut::PlanFits,
            "budget exceeds total"
        );
        assert_eq!(
            super::full_read_prefix(&[], 10),
            FullReadCut::PlanFits,
            "no tasks"
        );

        // The budget runs out in the FINAL commit. Checkpointing at the head
        // would be the whole read with extra bookkeeping — but declining
        // outright is what livelocked a compacted table in production, so fall
        // back to the newest boundary strictly below the head instead.
        assert_eq!(
            super::full_read_prefix(&tasks, 85),
            FullReadCut::Cut(20),
            "falls back below the head commit rather than declining"
        );

        // A task with no attributable sequence cannot be ordered, so no cut is
        // safe — better an expensive honest read than a wrong checkpoint.
        let unattributed = vec![
            t(10, 30),
            fluree_db_iceberg::scan::FileScanTask::for_whole_file(
                tasks[0].data_file.clone(),
                vec![],
                None,
            ),
            t(30, 30),
        ];
        assert_eq!(
            super::full_read_prefix(&unattributed, 40),
            FullReadCut::NoSequence,
            "unknown sequence"
        );
    }

    /// A COMPACTED table is the case the old `None` return livelocked on: a
    /// compaction rewrites every file with one sequence number, so the budget
    /// always lands in the head commit and there is no boundary below it. That
    /// is genuinely uncuttable — but it must be REPORTED, not returned as a bare
    /// "declined", because an unbounded read over the novelty ceiling never
    /// commits and so repeats on every poll.
    #[test]
    fn full_read_prefix_reports_a_single_commit_table() {
        let one_commit = vec![t(7, 400_000), t(7, 400_000)];
        assert_eq!(
            super::full_read_prefix(&one_commit, 250_000),
            super::FullReadCut::SingleCommit
        );
        assert!(
            super::FullReadCut::SingleCommit.is_alarming(),
            "an uncuttable full read must be shouted about, not swallowed"
        );
        // And the two healthy outcomes must NOT be, or the log fills with noise
        // on every small read and the real signal is lost.
        assert!(!super::FullReadCut::PlanFits.is_alarming());
        assert!(!super::FullReadCut::Disabled.is_alarming());
    }

    /// The row budget has to fit under the novelty ceiling, because a pass that
    /// cannot commit defers, and a deferral discards the window's progress. The
    /// old flat 250_000 rows is ~27 MB of flakes: against the 8 MiB ceiling this
    /// deployment pins, it could never commit, so the read repeated forever.
    #[test]
    fn the_row_budget_is_derived_to_fit_the_novelty_ceiling() {
        const BYTES_PER_ROW: i64 = 108;
        let eight_mib = 8 * 1024 * 1024;

        let rows = super::rows_for_ceiling(eight_mib, BYTES_PER_ROW);
        assert!(
            rows * BYTES_PER_ROW <= eight_mib,
            "a full pass must fit the ceiling it has to commit under: \
             {rows} rows * {BYTES_PER_ROW}B > {eight_mib}B"
        );
        assert!(
            rows < 250_000,
            "the derived budget must be tighter than the flat 250_000 that could not commit"
        );

        // A generous ceiling derives a generous budget — the derivation tracks
        // the ceiling rather than clamping to some other constant.
        assert!(super::rows_for_ceiling(256 * 1024 * 1024, BYTES_PER_ROW) > rows);

        // Floors: a tiny or nonsensical ceiling still makes forward progress
        // rather than a zero-row pass that reads nothing and checkpoints nowhere.
        assert_eq!(super::rows_for_ceiling(1, BYTES_PER_ROW), 1_000);
        assert_eq!(super::rows_for_ceiling(eight_mib, 0), 1_000);
        assert_eq!(super::rows_for_ceiling(eight_mib, -5), 1_000);
    }
}
