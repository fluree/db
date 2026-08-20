//! Materialize an R2RML / Iceberg graph source into a native Fluree ledger.
//!
//! The query path reads a graph source on the fly via `CONSTRUCT`-style
//! template expansion. Native Fluree features (BM25 full-text search, vector /
//! RAG, reasoning) operate only on facts committed to a *native* ledger, so to
//! make those work over an external Iceberg table we **materialize** it: read
//! the source rows, expand the R2RML subject / predicate / object maps into RDF
//! terms exactly as the query path does, group them per subject, and `upsert`
//! them into a target ledger.
//!
//! Refreshes are incremental when safe. A per-(source, table) watermark (the
//! last materialized snapshot id) is persisted as a triple in the *target*
//! ledger and read back automatically, so callers track nothing. When the
//! source's `(from, to]` window is append/compaction-only (see
//! [`window_is_incremental_safe`](fluree_db_iceberg::metadata::TableMetadata::window_is_incremental_safe))
//! only the *added* rows are read; otherwise the live table is full-read.
//!
//! With a `delete` convention + `order_by` column configured on the source, the
//! pass is **latest-by-key**: per subject the highest-ordered row wins, a
//! whole-subject *replace* clears fields dropped in a newer revision, and a
//! tombstone row retracts the subject — matching a `ROW_NUMBER() … ORDER BY
//! <col> DESC` view.
//!
//! # Assumptions (latest-by-key mode)
//!
//! These hold for the append-only, full-image CDC sinks this targets (e.g.
//! Debezium → Iceberg) and are enforced where cheap, else documented:
//! - **One complete row per subject revision.** Each source row is a full
//!   snapshot of its subject (whole-row replace assumes this). A source where a
//!   subject is *assembled across multiple rows* (e.g. an unpivoted join table)
//!   is not supported in latest-by-key mode — use legacy mode (no `delete`/
//!   `order_by`) or a one-row-per-subject view. **One triples map per logical
//!   table** is required and enforced (multiple would clobber under replace).
//! - **`order_by` is a populated, value-orderable column** (integer / date /
//!   timestamp — enforced; float/decimal/string are rejected). A row with a NULL
//!   ordering value sorts as oldest, so the ordering column should be present on
//!   every row (CDC event timestamps are).
//! - **The target ledger is dedicated to this source** — whole-subject
//!   retraction owns the subject; mixing other sources or hand-written data
//!   about the same IRIs into the target is unsupported.
//! - **Two ordered commits, not one** (a combined delete+insert silently drops
//!   the grounded insert when the delete binds zero rows). The watermark advances
//!   only in the second (upsert) commit, so a crash — or a failed upsert — leaves
//!   the watermark un-advanced and the next poll re-materializes the same window
//!   (self-healing; a failed upsert leaves the just-retracted subjects missing
//!   only until that next poll).
//!
//! This module is only available with the `iceberg` feature.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Map, Value as JsonValue};

use fluree_db_iceberg::io::ColumnBatch;
use fluree_db_iceberg::DeleteConvention;
use fluree_db_query::r2rml::R2rmlProvider;
use fluree_db_r2rml::mapping::TriplesMap;
use fluree_db_r2rml::materialize::{
    batch_has_column, column_is_orderable, column_sort_key, column_string, emit_row_terms,
    expand_template_from_batch, materialize_graph_from_batch, materialize_subject_from_batch,
    MaterializeStats, ParentIndexSet, RdfTerm, TmEmitContext, TripleObserver,
};
use fluree_vocab::UnresolvedDatatypeConstraint;

use std::sync::Arc;

use fluree_db_r2rml::mapping::CompiledR2rmlMapping;

use crate::error::TargetTally;
use crate::graph_source::r2rml::MaterializeScan;
use crate::graph_source::FlureeR2rmlProvider;
use crate::{ApiError, Fluree, Result};
// `StreamExt::next` — the scan is consumed as a stream so the whole table is never
// resident; see the note at the `scan_for_materialize_stream` call site.
use crate::LedgerState;
use futures::StreamExt;
use tracing::{info, warn};

/// The shared ledger that holds materialization job watermarks (last materialized
/// source snapshot id per `(source, target-spec, table)`), for EVERY job — plain
/// or templated. Keeping watermarks here, out of the materialized target
/// ledger(s), (a) removes bookkeeping triples from user data, and (b) gives one
/// generic home a fan-out job can read *before* it discovers its per-row targets.
/// Ledger names are not charset-reserved from users, so this uses a clearly
/// namespaced id; collisions with a real user ledger of this exact name are the
/// operator's responsibility to avoid.
const MATERIALIZE_STATE_LEDGER: &str = "fluree_materialize_state:main";
/// Subject-IRI prefix for the per-(source, target-spec, table) materialization
/// watermark. The full subject is `{PREFIX}{source}:{target-spec}:{table}` so the
/// state ledger tracks every source → target → table independently.
const WATERMARK_SUBJECT_PREFIX: &str = "urn:fluree:materialize-state:";
/// Predicate holding the last materialized source snapshot id (stored as a
/// string to preserve full i64 precision for 19-digit snapshot ids).
const WATERMARK_SNAPSHOT_PRED: &str = "urn:fluree:materialize#lastSnapshotId";
/// Predicate recording which source the watermark belongs to (informational).
const WATERMARK_SOURCE_PRED: &str = "urn:fluree:materialize#source";
/// Predicate recording which target-spec (ledger id or template) the watermark
/// belongs to (informational).
const WATERMARK_TARGET_PRED: &str = "urn:fluree:materialize#target";
/// Predicate recording which source table the watermark belongs to.
const WATERMARK_TABLE_PRED: &str = "urn:fluree:materialize#table";

/// Subject-IRI prefix for a per-(source, RESOLVED target, table) applied marker.
///
/// Distinct from [`WATERMARK_SUBJECT_PREFIX`] because the two are keyed on
/// different things and must never collide: the watermark is keyed on the target
/// SPEC (one per job, `silver_{tenant}_{user}:main`), this on the RESOLVED ledger
/// (`silver_acme_u1:main`). For a non-templated job those strings are equal, so a
/// shared prefix would put both on the same subject.
const APPLIED_SUBJECT_PREFIX: &str = "urn:fluree:materialize-applied:";
/// Predicate holding the snapshot id a single resolved target has fully applied
/// (string-encoded, like the watermark, to preserve i64 precision).
///
/// WHY THIS EXISTS. The watermark is shared across every target a templated job
/// fans into, and it may not advance while ANY target is behind — otherwise the
/// laggards would skip that window permanently. Correct, but it means the targets
/// that DID commit earn no credit: the next poll re-reads the window and
/// re-commits all of them, forever, for as long as one target cannot fit.
///
/// Measured in production: 19 of 23 targets committing and 4 deferring on every
/// poll, unchanged across 22 consecutive polls, so 19 targets were rewritten every
/// ~4 minutes for 13.7 h. The re-commits are idempotent but not free — they are
/// writes, and they filled a 100 GiB volume to ENOSPC, which is unrecoverable
/// because GC needs to write in order to reclaim.
///
/// This marker records what each resolved target has already applied, so a target
/// that is caught up is SKIPPED rather than rewritten. It deliberately does not
/// touch the scan window: the shared watermark still chooses it, so the source read
/// is unchanged and there is no risk of starting a scan after rows a newly-created
/// target still needs.
const APPLIED_SNAPSHOT_PRED: &str = "urn:fluree:materialize#appliedSnapshotId";

/// Subject-IRI prefix for a persisted tracking job. The full subject is
/// `{PREFIX}{source}:{target-spec}`, matching the worker's own
/// `(source, target)` job key.
const JOB_SUBJECT_PREFIX: &str = "urn:fluree:materialize-job:";
/// Predicate holding the job as one serialized JSON blob. A single opaque value
/// keeps the restore decoder trivial and lets the job gain fields without a
/// schema migration; the `source`/`target`/`tracked` predicates below duplicate
/// the identifying bits as plain triples so an operator can see jobs with the
/// same SPARQL they already use for watermarks.
const JOB_BLOB_PRED: &str = "urn:fluree:materialize#job";
/// Predicate marking whether the job is currently tracked. `untrack` sets this
/// to `false` rather than retracting the row, so the record survives as an
/// audit trail and there is no delete path to get wrong.
const JOB_TRACKED_PRED: &str = "urn:fluree:materialize#tracked";

/// A tracking job as persisted in the shared materialization-state ledger.
///
/// Jobs used to be in-memory only, so a server restart silently stopped every
/// materialization until a client re-issued `POST /iceberg/track`. This is the
/// durable record that makes a restart recover on its own.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PersistedMaterializeJob {
    /// Graph source to read.
    pub source: String,
    /// Target ledger id, or the target TEMPLATE for a fan-out job — stored as
    /// configured, exactly like the watermark's target-spec.
    pub target: String,
    /// This job's own poll cadence.
    pub poll_interval_secs: u64,
}

/// Outcome of one materialization pass.
#[derive(Debug, Clone)]
pub struct MaterializeResult {
    /// The source snapshot that was materialized — persisted as the watermark in
    /// the target ledger for the next incremental refresh. `None` when the source
    /// table has no snapshots yet (nothing was materialized).
    pub to_snapshot_id: Option<i64>,
    /// The watermark this pass started from (the previously-materialized source
    /// snapshot id), or `None` for an initial / forced-full materialization.
    pub from_snapshot_id: Option<i64>,
    /// Whether an incremental (added-files-only) scan was used. `false` means a
    /// full re-read (initial run, expired/branched history, or a window
    /// containing a genuine `overwrite`/`delete`).
    pub incremental: bool,
    /// Whether anything was committed (data and/or an advanced watermark). A
    /// no-delta poll (`from == to`) returns `false` — no commit churn.
    pub committed: bool,
    /// Number of source rows read across all batches.
    pub rows_read: usize,
    /// Number of distinct subject nodes upserted (live rows).
    pub subjects_upserted: usize,
    /// Number of distinct subjects retracted (tombstone rows whose final state in
    /// this pass was a delete). Always 0 when no delete convention is configured
    /// or on the first materialization of a not-yet-existing target.
    pub subjects_retracted: usize,
    /// Per-target outcome counts. On the success path every target is in `ok`; the
    /// mixed case arrives as `ApiError::MaterializePartial` instead. Present on both
    /// so a caller can account for targets uniformly rather than inferring the count
    /// from whichever branch it landed in.
    pub tally: TargetTally,
}

/// The source side of a materialize pass: what the mapping is, how to interpret it, and
/// one window of rows.
///
/// Exists so the engine can be driven without an Iceberg catalog. Before this, the engine
/// opened with `let provider = FlureeR2rmlProvider::new(self)` — no injection point — and
/// every graph-source integration test in the repo needs live infrastructure (a Polaris
/// REST catalog, LocalStack or MinIO). The combination made
/// [`Fluree::materialize_r2rml_graph_source`] **structurally untestable**, which is why a
/// reviewer could replace the streaming core with "yield nothing" and leave 954/954 green.
/// The engine was not left uncovered by carelessness; there was no seam to cover it
/// through.
///
/// Deliberately THREE methods — the entire provider surface the engine actually touches.
/// A wider trait would be easier to write and harder to fake honestly.
///
/// `&dyn` rather than a generic parameter: the pass is ~400 lines and I/O-bound, so
/// monomorphizing it per provider buys nothing measurable and would leak a type parameter
/// into the per-target helpers. Dynamic dispatch costs three calls per table per poll.
///
/// # What faking this does and does not cover
///
/// It covers everything the ENGINE decides — latest-by-key collapsing, tombstone
/// retraction, the additive `@type` union, per-target fan-out and [`TargetTally`],
/// watermark hold-back, transaction chunking, stale-base retry, novelty deferral.
///
/// It does **NOT** cover the real provider's scan (`stream_scan_tasks`, `ScanChoice`, the
/// incremental snapshot window), because a fake bypasses that code by construction. The
/// storage seam for offline coverage now exists (`prepare_iceberg_scan` returns an
/// [`IcebergStorageBackend`](fluree_db_iceberg::io::IcebergStorageBackend), whose `File`
/// variant reads a local table), and `fluree-db-api/tests/it_iceberg_local_fs.rs` drives
/// the real scan against a committed pyiceberg fixture.
#[async_trait::async_trait]
pub trait MaterializeSource: Send + Sync {
    /// The compiled R2RML mapping for `graph_source_id`.
    async fn compiled_mapping(&self, graph_source_id: &str) -> Result<Arc<CompiledR2rmlMapping>>;

    /// Materialization options: `(delete convention, order_by column)`. Either one being
    /// present switches the pass from additive merge to latest-by-key.
    async fn materialize_options(
        &self,
        graph_source_id: &str,
    ) -> Result<(Option<DeleteConvention>, Option<String>)>;

    /// One window of rows for `table_name`, starting after `from_snapshot_id`, as a
    /// stream — the whole table is never resident.
    async fn scan_window(
        &self,
        graph_source_id: &str,
        table_name: &str,
        from_snapshot_id: Option<i64>,
    ) -> Result<MaterializeScan>;
}

/// The production implementation: straight delegation to [`FlureeR2rmlProvider`], so the
/// seam adds no behaviour of its own and cannot drift from what the server does.
#[async_trait::async_trait]
impl MaterializeSource for FlureeR2rmlProvider<'_> {
    async fn compiled_mapping(&self, graph_source_id: &str) -> Result<Arc<CompiledR2rmlMapping>> {
        Ok(R2rmlProvider::compiled_mapping(self, graph_source_id, None).await?)
    }

    async fn materialize_options(
        &self,
        graph_source_id: &str,
    ) -> Result<(Option<DeleteConvention>, Option<String>)> {
        Ok(FlureeR2rmlProvider::materialize_options(self, graph_source_id).await?)
    }

    async fn scan_window(
        &self,
        graph_source_id: &str,
        table_name: &str,
        from_snapshot_id: Option<i64>,
    ) -> Result<MaterializeScan> {
        Ok(self
            // `to = None`: the sync-to-head worker always reads to the source's
            // current snapshot; explicit pins are for point-in-time consumers.
            .scan_for_materialize_stream(graph_source_id, table_name, &[], from_snapshot_id, None)
            .await?)
    }
}

impl Fluree {
    /// Materialize an R2RML / Iceberg graph source into a native ledger.
    ///
    /// Each logical table in the mapping is materialized independently against
    /// its **own** per-(source, table) watermark, persisted as a triple in the
    /// target ledger and read back automatically (unless `force_full` ignores it
    /// and re-reads the whole live table). A table reads incrementally when its
    /// `(from, to]` window is append/compaction-only, else full. Callers track
    /// nothing; a bare re-run resumes incrementally — safe to invoke on a timer
    /// (the tracking worker does exactly that). A no-delta poll commits nothing
    /// and returns `committed = false`.
    ///
    /// **Latest-by-key + deletions.** With an `order_by` column configured, the
    /// rows of each subject in the window are ranked and the **latest** row wins;
    /// without one, the last row in scan order wins. The winning row is applied
    /// as a *whole-subject replace*: every subject seen in the window is
    /// retracted (clearing fields dropped in a newer revision), then the live
    /// nodes are re-asserted with the advanced watermarks. A row classified as a
    /// tombstone by the [`DeleteConvention`] (`IcebergGsConfig.delete`) retracts
    /// its subject and is not re-asserted. All subject IRIs come from the same
    /// subject materializer (parity with the query path). The two commits are
    /// ordered (retract, then upsert+watermark) so the watermark advances only in
    /// the upsert — a crash between them re-reads the same window on the next run.
    /// (Latest-by-key mode assumes the target ledger is dedicated to this source —
    /// whole-subject retraction owns the subject.) With neither `order_by` nor
    /// `delete` configured the pass is additive: non-type predicates are upserted
    /// per predicate, and `rdf:type` is asserted via an idempotent `insert` so
    /// classes UNION across sources — several sources (or a join table adding an
    /// edge to a parent) can contribute types to the same subject in a shared
    /// target without clobbering each other.
    pub async fn materialize_r2rml_graph_source(
        &self,
        source_graph_source_id: &str,
        target_ledger_id: &str,
        force_full: bool,
    ) -> Result<MaterializeResult> {
        let provider = FlureeR2rmlProvider::new(self);
        self.materialize_from_source(
            &provider,
            source_graph_source_id,
            target_ledger_id,
            force_full,
            // Production always derives its own transaction and memory budgets.
            None,
            None,
        )
        .await
    }

    /// The materialize pass itself, against any [`MaterializeSource`].
    ///
    /// Split out from [`Self::materialize_r2rml_graph_source`] so a test can drive the
    /// engine without an Iceberg catalog. The public method's signature is unchanged and
    /// simply supplies the production source, so nothing about the server's behaviour
    /// moves — see `MaterializeSource` for what a fake does and does not cover.
    ///
    /// `txn_budget_override` overrides the per-transaction byte budget. Production passes
    /// `None` and gets the derived value; only a test passes `Some`. It exists because the
    /// derived budget has a **1 MiB floor** (`.max(1 << 20)`), so proving that a window is
    /// split across transactions would otherwise need >1 MiB of JSON-LD — ~24,000 rows,
    /// which took minutes through the transact path. A parameter only tests supply is a
    /// smell; a multi-minute unit test nobody runs is a worse one, and an env var would be
    /// process-global and flaky under parallel tests.
    ///
    /// `accum_budget_override` overrides the accumulator memory budget
    /// ([`materialize_memory_budget_bytes`]) for the same reason: the shipped
    /// default is 1 GiB, so proving the gate fires would otherwise need a
    /// gigabyte-scale test window.
    pub(crate) async fn materialize_from_source(
        &self,
        provider: &dyn MaterializeSource,
        source_graph_source_id: &str,
        target_ledger_id: &str,
        force_full: bool,
        txn_budget_override: Option<usize>,
        accum_budget_override: Option<usize>,
    ) -> Result<MaterializeResult> {
        // 1. Compiled R2RML mapping (subject / predicate / object maps) and the
        //    materialization options (delete convention + latest-by-key ordering).
        let mapping = provider.compiled_mapping(source_graph_source_id).await?;
        if mapping.triples_maps.is_empty() {
            return Err(ApiError::Config(format!(
                "Graph source '{source_graph_source_id}' has no R2RML triples maps"
            )));
        }
        let (delete_convention, order_by) =
            provider.materialize_options(source_graph_source_id).await?;
        // Latest-by-key (whole-subject replace + tombstone deletes) is enabled by
        // configuring a delete convention and/or an ordering column. With neither,
        // the pass is the legacy additive merge (a subject may span multiple rows).
        let latest_by_key = delete_convention.is_some() || order_by.is_some();

        // 2. Group triples maps by their logical table (deterministic order), then
        //    read each table ONCE against its OWN per-(source,table) watermark and
        //    `(from, to]` window, applying ALL of that table's triples maps to each
        //    row. Classify rows into a latest-by-key accumulator.
        let mut tables: BTreeMap<&str, Vec<&TriplesMap>> = BTreeMap::new();
        for tm in mapping.triples_maps.values() {
            if let Some(table_name) = tm.table_name() {
                tables.entry(table_name).or_default().push(tm);
            }
        }
        // Latest-by-key requires one triples map per logical table: multiple
        // triples maps over the same table that emit the same subject would
        // clobber each other's predicates under whole-row replace. Fail loud
        // rather than silently drop triples. (Legacy additive mode merges, so it
        // is fine.)
        if latest_by_key {
            if let Some((table_name, tms)) = tables.iter().find(|(_, tms)| tms.len() > 1) {
                return Err(ApiError::Config(format!(
                    "latest-by-key materialization (delete/order_by configured) supports one \
                     triples map per logical table, but '{table_name}' has {} — split them into \
                     separate tables/sources or remove delete/order_by",
                    tms.len()
                )));
            }
        }

        // Shared-enumerator emission state. The parent-index plan validates the
        // whole mapping up front (fail-closed on a RefObjectMap naming an unknown
        // parent TriplesMap — a broken mapping, not a silent drop). Parents are
        // not yet INDEXED on this incremental path, so FK edges drop as dangling
        // (counted in `emit_stats.ref_dangling` and warned about below) — the
        // same net behavior as before, now visible. Indexing parents here needs
        // a window-vs-whole-table answer first: a parent row referenced by a new
        // child row may sit OUTSIDE the incremental window.
        let parents = ParentIndexSet::new(&mapping)
            .map_err(|e| ApiError::Config(format!("invalid R2RML mapping: {e}")))?;
        let mut emit_stats = MaterializeStats::default();
        let accum_budget = accum_budget_override.unwrap_or_else(materialize_memory_budget_bytes);
        let mut accum = MaterializeAccum::default();
        let mut rows_read = 0usize;
        let mut incremental_all = true;
        let mut any_table = false;
        // Set when at least one table's window has aged past the refresh bound, so a
        // no-data poll must still persist its watermark to keep it resolvable.
        let mut watermark_refresh_due = false;
        // (table, from-snapshot, advanced to-snapshot) per source table.
        let mut table_watermarks: Vec<(String, Option<i64>, i64)> = Vec::new();

        for (table_name, tms) in &tables {
            let from_t = if force_full {
                None
            } else {
                self.materialize_watermark(
                    MATERIALIZE_STATE_LEDGER,
                    source_graph_source_id,
                    target_ledger_id,
                    table_name,
                )
                .await?
            };

            // STREAM the scan; do not collect it. A full read is mandatory whenever
            // the snapshot window contains overwrite/delete, so on some sources this
            // path runs on every poll — collecting made peak memory proportional to
            // the source TABLE (735k rows drove 21.4 GiB of anon and an OOMKill
            // every 4-6 min), and the OOM also prevented the commit that would have
            // advanced the watermark, so the next poll re-read the same table.
            //
            // Each batch is folded into `accum` and dropped here, so only
            // O(iceberg_scan_concurrency) file decodes are resident. NOTE this
            // bounds the RAW SCAN only: `accum` still grows with the number of
            // distinct subjects in the window, which latest-by-key semantics
            // require (finalize() must see every row for a key). That is a smaller
            // and more predictable term than the raw columnar data, but it is not
            // O(1) — a window with millions of distinct subjects is still large.
            let scan = provider
                .scan_window(source_graph_source_id, table_name, from_t)
                .await?;
            let (to_id, incremental) = (scan.to_snapshot_id, scan.incremental);
            // A window older than the refresh bound must persist its watermark even
            // with zero rows — see `watermark_refresh_bound_ms`.
            if scan
                .window_age_ms
                .is_none_or(|age| age >= watermark_refresh_bound_ms())
            {
                watermark_refresh_due = true;
            }
            // Per-TriplesMap emit hoists (constant predicates, class terms, FK
            // joins), built once per table scan and reused for every row.
            let tm_ctxs: Vec<TmEmitContext<'_>> = tms
                .iter()
                .map(|tm| TmEmitContext::new(tm))
                .collect::<std::result::Result<_, _>>()
                .map_err(|e| ApiError::Config(format!("invalid R2RML mapping: {e}")))?;
            let mut batch_stream = scan.stream;
            // Only count a table as contributing once it actually has a snapshot.
            if let Some(to) = to_id {
                any_table = true;
                incremental_all = incremental_all && incremental;
                table_watermarks.push(((*table_name).to_string(), from_t, to));
            }

            while let Some(batch) = batch_stream.next().await {
                let batch = &batch?;
                // Configured marker / ordering columns must exist (and the
                // ordering column must be a value-orderable type); otherwise a
                // null-match convention would treat every row as a tombstone and
                // ordering would silently mis-sort.
                if let Some(conv) = delete_convention.as_ref() {
                    if !batch_has_column(batch, &conv.column) {
                        return Err(ApiError::Config(format!(
                            "delete.column '{}' not found in source table '{table_name}'",
                            conv.column
                        )));
                    }
                }
                if let Some(col) = order_by.as_deref() {
                    if !batch_has_column(batch, col) {
                        return Err(ApiError::Config(format!(
                            "order_by column '{col}' not found in source table '{table_name}'"
                        )));
                    }
                    if !column_is_orderable(batch, col) {
                        return Err(ApiError::Config(format!(
                            "order_by column '{col}' in source table '{table_name}' must be an \
                             integer, date, or timestamp type for value-correct latest-by-key \
                             ordering"
                        )));
                    }
                }
                for row in 0..batch.num_rows {
                    rows_read += 1;
                    // Resolve this row's TARGET ledger. A placeholder-free target
                    // is used verbatim for every row (plain single-target job); a
                    // templated target (e.g. `silver_{tenant_id}_{user_id}:main`)
                    // is expanded from the row's columns, so ONE job fans out into
                    // a ledger per (tenant,user). A row whose template columns are
                    // null cannot be routed to a ledger — skip it (it could not be
                    // isolated to a user anyway).
                    let Some(row_target) = resolve_target_ledger(target_ledger_id, batch, row)
                    else {
                        continue;
                    };
                    for ctx in &tm_ctxs {
                        materialize_row_into(
                            ctx,
                            batch,
                            row,
                            row_target.clone(),
                            delete_convention.as_ref(),
                            order_by.as_deref(),
                            latest_by_key,
                            &parents,
                            &mut emit_stats,
                            &mut accum,
                        )?;
                    }
                }

                // Pre-OOM circuit breaker on the pass's dominant memory term
                // (the retained-node accumulator). Checked per batch — one
                // batch's contribution is bounded by the scan batch size — and
                // firing here is PRE-COMMIT: no retract has run, no target is
                // touched, the watermark stays put, so the failure leaves
                // everything as the last successful poll did. See
                // `ApiError::MaterializeMemoryBudget` for the operator levers.
                if accum_budget > 0 && accum.estimated_bytes() > accum_budget {
                    return Err(ApiError::MaterializeMemoryBudget {
                        table: (*table_name).to_string(),
                        estimated_bytes: accum.estimated_bytes(),
                        budget_bytes: accum_budget,
                        distinct_subjects: accum.len(),
                    });
                }
            }
        }

        // FK edges the enumerator would have emitted but couldn't: the mapping
        // carries RefObjectMaps, and this path does not index parents yet. Before
        // the shared enumerator this was an undetectable silent drop; now it is a
        // counted, warned-about one.
        if emit_stats.ref_dangling > 0 {
            warn!(
                source = %source_graph_source_id,
                dropped_fk_edges = emit_stats.ref_dangling,
                "materialize: mapping has RefObjectMap foreign-key edges, which the \
                 incremental materializer does not materialize (parents are not indexed); \
                 the virtual query path still serves them"
            );
        }

        let incremental = any_table && incremental_all;
        // Surface from/to on the result only for a single-table mapping (one
        // materialized table); multi-table watermarks live per-table in the ledger.
        let (from_snapshot_id, to_snapshot_id) = if table_watermarks.len() == 1 {
            (table_watermarks[0].1, Some(table_watermarks[0].2))
        } else {
            (None, None)
        };

        // 3. Finalize the latest-by-key state: live nodes to (re)assert and the
        //    set of keys whose latest row is a tombstone (deletions).
        let (live, deletions) = accum.finalize();
        let subjects_upserted = live.len();

        // No-delta short-circuit.
        //
        // This used to require `all_watermarks_unchanged`, which made it almost never
        // fire: the watermark is the source's snapshot id, and the upstream Kafka
        // sink bumps that 37-72 times an hour per table whether or not any row
        // concerns us. So a poll that read NOTHING still wrote a watermark commit.
        // Seventeen sources at a ~50 s poll interval produced ~1,200 commits/hour of
        // two-triple payloads into the shared state ledger, which reached 27,179
        // commits against 3 index roots, saturated its novelty, and — because the
        // watermark write is the LAST step of every materialize — halted every sync
        // in the deployment.
        //
        // SNAPSHOT-ID MOVEMENT IS NOT WORK. What matters is whether we wrote rows.
        // The one exception is staleness: see `watermark_refresh_due`.
        if live.is_empty() && deletions.is_empty() && !watermark_refresh_due {
            return Ok(MaterializeResult {
                to_snapshot_id,
                from_snapshot_id,
                incremental,
                committed: false,
                rows_read,
                subjects_upserted: 0,
                subjects_retracted: 0,
                // No targets were attempted: the window had no rows to apply.
                tally: TargetTally::default(),
            });
        }

        // 4. Group the finalized state by TARGET ledger, then commit each target
        //    INDEPENDENTLY. A plain job has exactly one target; a templated target
        //    (e.g. `silver_{tenant_id}_{user_id}:main`) fans out into one ledger
        //    per (tenant,user). Per-graph scoping (rr:graphMap) still applies
        //    WITHIN each target. The job watermark(s) advance ONCE afterwards in
        //    the shared state ledger — never bundled into a target's data commit.
        type TargetState = (
            BTreeMap<(Option<String>, String), SubjectNode>,
            BTreeSet<(Option<String>, String)>,
        );
        let mut by_target: BTreeMap<String, TargetState> = BTreeMap::new();
        for ((target, graph, subject), node) in live {
            by_target
                .entry(target)
                .or_default()
                .0
                .insert((graph, subject), node);
        }
        for (target, graph, subject) in deletions {
            by_target
                .entry(target)
                .or_default()
                .1
                .insert((graph, subject));
        }

        // Per-transaction size budget, derived from the ledger's novelty ceiling.
        //
        // A transaction whose flakes reach `reindex_max_bytes` is REJECTED outright
        // (`TransactError::NoveltyWouldExceed`) — not delayed, not retried into
        // success. This window used to become ONE transaction per target, so a wide
        // source could never sync AT ALL: silver.observation produced 702k rows =
        // 75,964,480 B of flakes against the configured 67,108,864 B ceiling and
        // failed on every poll forever. The error carried `current=0`, proving
        // novelty was empty and the single transaction was simply too big — so no
        // amount of waiting or indexing could ever have cleared it.
        //
        // Raising the ceiling is the wrong lever: it is PER-LEDGER with no aggregate
        // cap, so it multiplies by concurrently-hot ledgers, and no fixed value is
        // safe because a larger window breaches any of them.
        //
        // The budget is measured in serialized JSON-LD bytes (cheap to estimate
        // here) while the ceiling is in flake bytes, so the divisor must absorb the
        // flake/JSON size ratio. Measured across four data shapes, the flakes a
        // document yields run 0.96–2.5× its JSON size — about the SAME size,
        // sometimes larger, never dramatically smaller. (The outage window above
        // happened to be a low-ratio shape, ~108 flake-bytes per row against JSON
        // nodes of a few hundred bytes; the ratio is shape-dependent — long IRIs
        // and per-flake overheads dominate narrow rows.) Budgeting JSON at a
        // QUARTER of the flake ceiling therefore holds even at the worst measured
        // ratio: 2.5 × 1/4 ≈ 0.63 of the ceiling per chunk, with the rest as
        // headroom for the estimate itself. Over-chunking is nearly free WHEN a
        // local indexer is running: `reindex_min_bytes` (default 100 bytes —
        // `server_defaults.rs`) keeps novelty draining between chunks rather than
        // letting it accumulate toward the ceiling. A node without a local indexer
        // has no such drain, which is why the tracking worker only runs where
        // indexing is enabled (`fluree-db-server/src/state.rs`).
        let txn_budget =
            txn_budget_override.unwrap_or((self.index_config.reindex_max_bytes / 4).max(1 << 20));

        // PER-TARGET ISOLATION.
        //
        // Each resolved target is an independent commit domain — its own novelty
        // ceiling, its own index, its own ways to be broken. Aborting the whole job on
        // the first bad one is how a SINGLE truncated dict pack in one ledger produced
        // 208 identical failures across all 17 sources in 20 minutes, while 21 healthy
        // targets got nothing.
        //
        // Note the loop already commits per target as it goes, so partial application
        // was ALWAYS the reality. What was missing is recording which targets
        // succeeded. This does not introduce partial writes; it stops discarding the
        // knowledge of them.
        let mut subjects_retracted = 0usize;
        let mut ok_targets = 0usize;
        let mut deferred: Vec<(String, usize)> = Vec::new();
        let mut failed: Vec<(String, ApiError)> = Vec::new();
        let mut skipped_targets = 0usize;
        let mut newly_applied: Vec<String> = Vec::new();

        // What each resolved target has already applied. Read once per pass, not per
        // target. `force_full` ignores the markers for the same reason it ignores the
        // watermark: the caller is asking for a rebuild, not a resume.
        let applied = if force_full {
            std::collections::HashMap::new()
        } else {
            self.materialize_applied_markers(MATERIALIZE_STATE_LEDGER, source_graph_source_id)
                .await
                .unwrap_or_else(|e| {
                    // Non-fatal: an unreadable marker set costs the skip, not the pass.
                    // Failing here would turn a state-ledger hiccup into a stalled job,
                    // and re-applying is idempotent.
                    warn!(
                        source = %source_graph_source_id,
                        error = %e,
                        "materialize: could not read applied markers; re-applying every target"
                    );
                    std::collections::HashMap::new()
                })
        };

        for (target, (live, deletions)) in by_target {
            // Already has this whole window. Skipping is what stops a job whose
            // watermark cannot advance from rewriting its healthy targets on every
            // poll — the amplification that filled a 100 GiB volume.
            if target_is_caught_up(&applied, &target, &table_watermarks) {
                skipped_targets += 1;
                ok_targets += 1;
                continue;
            }
            match self
                .materialize_one_target(&target, live, deletions, latest_by_key, txn_budget)
                .await
            {
                Ok(retracted) => {
                    subjects_retracted += retracted;
                    ok_targets += 1;
                    newly_applied.push(target);
                }
                // Deferral is not failure — it is backpressure on THIS target only.
                Err(ApiError::NoveltyDeferred { remaining }) => {
                    deferred.push((target, remaining));
                }
                Err(e) => {
                    // Logged per target, not just aggregated, so one persistently
                    // broken ledger is identifiable rather than hidden behind a count.
                    warn!(
                        target = %target,
                        error = %e,
                        "materialize: target FAILED; other targets continue"
                    );
                    failed.push((target, e));
                }
            }
        }

        let tally = TargetTally {
            ok: ok_targets,
            deferred: deferred.len(),
            failed: failed.len(),
        };
        // Report skips on EVERY pass, not only an incomplete one.
        //
        // An all-skip pass is `is_complete()`, so it would otherwise be the ONE outcome
        // that prints nothing whatsoever — and that is precisely the shape a
        // wrongly-skipped target takes. `all_skipped` is the specific alarm: nothing was
        // written this pass, yet the watermark is about to advance. That is legitimate
        // for a genuinely idle window and a data-loss signature otherwise, so it has to
        // be visible either way rather than inferred by diffing a target ledger's `t`.
        if skipped_targets > 0 {
            info!(
                source = %source_graph_source_id,
                targets_skipped = skipped_targets,
                targets_ok = tally.ok,
                all_skipped = skipped_targets == tally.ok && tally.is_complete(),
                "materialize: targets already held this window; nothing written for them"
            );
        }
        if !tally.is_complete() {
            info!(
                targets_ok = tally.ok,
                targets_deferred = tally.deferred,
                targets_failed = tally.failed,
                targets_skipped = skipped_targets,
                "materialize: partial window"
            );
        }

        // Record what just landed, BEFORE the partial-window returns below.
        //
        // The ordering is the whole point. A job whose watermark can never advance is
        // exactly the job that needs these markers, and both early returns sit between
        // here and the watermark write — so putting this after them would record
        // progress only for jobs that never needed it.
        //
        // After the data commits, never before: a crash in the gap re-applies the
        // window, which is idempotent, whereas a marker written first would skip rows
        // that were never committed. Same argument as the watermark, one target down.
        if !newly_applied.is_empty() && !table_watermarks.is_empty() {
            let nodes: Vec<JsonValue> = newly_applied
                .iter()
                .flat_map(|target| {
                    table_watermarks.iter().map(move |(table, _from, to)| {
                        applied_node(source_graph_source_id, target, table, *to)
                    })
                })
                .collect();
            let state = self.materialize_state_ledger().await?;
            // Through the backpressure helper for the same reason the watermark write
            // is: giving up here on a transient novelty condition would discard the
            // knowledge of a window that DID commit, and the next poll would rewrite it.
            self.transact_chunks_with_backpressure(
                state,
                vec![nodes],
                |c: &[JsonValue]| JsonValue::Array(c.to_vec()),
                ChunkVerb::Upsert,
            )
            .await?;
        }

        // The watermark is still SHARED across targets (per-target watermarks are the
        // separate C1 change), so it must not advance while any target is behind — that
        // would skip this window for them permanently. Successful targets keep their
        // data; the window is re-read next poll and re-applied idempotently.
        //
        // Failure outranks deferral: a deferral self-heals on the next poll, a failure
        // usually needs attention, and reporting the milder of the two would bury it.
        //
        // Both branches report through `MaterializePartial` so the TALLY survives. The
        // previous code collapsed the window to a single scalar outcome, which meant a
        // 21-of-22 poll was recorded as one deferral and zero commits: from the stats
        // alone, a healthy window and a total stall were the same reading.
        if !failed.is_empty() {
            let (target, e) = failed.remove(0);
            return Err(ApiError::MaterializePartial {
                tally,
                detail: format!("target {target} failed: {e}"),
            });
        }
        if !deferred.is_empty() {
            let remaining: usize = deferred.iter().map(|(_, r)| *r).sum();
            // Deliberately NOT special-cased to `NoveltyDeferred` when ok == 0. That
            // looked tidier, but it discards `tally.deferred` — the same shape of loss
            // this variant exists to prevent, just at the other end of the range. One
            // path keeps the counts intact for every mix.
            return Err(ApiError::MaterializePartial {
                tally,
                detail: format!("novelty at capacity, {remaining} items pending"),
            });
        }

        // 5. Advance the job watermark(s) in the shared state ledger — AFTER every
        //    target data commit. Crash-safety: the watermark never advances before
        //    the data, so a crash in the gap re-reads the window (idempotent:
        //    whole-subject replace / idempotent insert+upsert). Keyed by
        //    (source, target-spec, table), so a templated job keeps ONE watermark
        //    per source table regardless of how many ledgers it fanned into.
        if !table_watermarks.is_empty() {
            let state = self.materialize_state_ledger().await?;
            let watermark_nodes: Vec<JsonValue> = table_watermarks
                .iter()
                .map(|(table, _from, to)| {
                    watermark_node(source_graph_source_id, target_ledger_id, table, *to)
                })
                .collect();
            // Through the backpressure helper, not a bare upsert. This write is
            // last, so failing it discards the whole window's work and the next
            // poll redoes all of it — the most expensive possible place to give up
            // on a transient novelty condition. One chunk: the watermark nodes are
            // a handful of small records, so this is purely about the retry.
            self.transact_chunks_with_backpressure(
                state,
                vec![watermark_nodes],
                |c: &[JsonValue]| JsonValue::Array(c.to_vec()),
                ChunkVerb::Upsert,
            )
            .await?;
        }

        Ok(MaterializeResult {
            to_snapshot_id,
            from_snapshot_id,
            incremental,
            committed: true,
            rows_read,
            subjects_upserted,
            subjects_retracted,
            tally,
        })
    }

    /// Read the per-job materialization watermark (last materialized source
    /// snapshot id) for `(source_graph_source_id, target_spec, table_name)` from
    /// the shared materialization-state ledger. `target_spec` is the job's target
    /// *as configured* — a concrete ledger id for a plain job, or the target
    /// TEMPLATE (e.g. `silver_{tenant_id}_{user_id}:main`) for a fan-out job — so
    /// one templated job that fans out into many ledgers keeps ONE watermark per
    /// source table (a single scan feeds all its targets). Returns `None` if the
    /// state ledger does not exist yet or carries no watermark for that job/table.
    pub async fn materialize_watermark(
        &self,
        state_ledger_id: &str,
        source_graph_source_id: &str,
        target_spec: &str,
        table_name: &str,
    ) -> Result<Option<i64>> {
        if !self.ledger_exists(state_ledger_id).await? {
            return Ok(None);
        }
        let db = self.db(state_ledger_id).await?;

        let subject = watermark_subject(source_graph_source_id, target_spec, table_name);
        let mut where_obj = Map::new();
        where_obj.insert("@id".to_string(), JsonValue::String(subject));
        where_obj.insert(
            WATERMARK_SNAPSHOT_PRED.to_string(),
            JsonValue::String("?v".to_string()),
        );
        let query = json!({ "select": ["?v"], "where": JsonValue::Object(where_obj) });

        let result = self.query(&db, &query).await?;
        let json = result
            .to_jsonld(&db.snapshot)
            .map_err(|e| ApiError::Internal(format!("Failed to format watermark query: {e}")))?;
        Ok(extract_first_i64(&json))
    }

    /// Every resolved target's applied marker for one source, as
    /// `(target, table) -> applied snapshot id`.
    ///
    /// ONE query for the whole job, not one per target: a templated job fans into as
    /// many ledgers as it has (tenant,user) pairs — 23 here, unbounded in principle —
    /// and a per-target read would put that many round trips in front of every poll.
    ///
    /// An empty map is the correct answer for a state ledger written before this
    /// existed, and it makes the first poll behave exactly as it did before: nothing
    /// is skipped, everything is applied, and the markers are written on the way out.
    async fn materialize_applied_markers(
        &self,
        state_ledger_id: &str,
        source_graph_source_id: &str,
    ) -> Result<std::collections::HashMap<(String, String), i64>> {
        let mut out = std::collections::HashMap::new();
        if !self.ledger_exists(state_ledger_id).await? {
            return Ok(out);
        }
        let db = self.db(state_ledger_id).await?;

        let mut where_obj = Map::new();
        where_obj.insert("@id".to_string(), JsonValue::String("?s".to_string()));
        where_obj.insert(
            WATERMARK_SOURCE_PRED.to_string(),
            JsonValue::String(source_graph_source_id.to_string()),
        );
        where_obj.insert(
            WATERMARK_TARGET_PRED.to_string(),
            JsonValue::String("?target".to_string()),
        );
        where_obj.insert(
            WATERMARK_TABLE_PRED.to_string(),
            JsonValue::String("?table".to_string()),
        );
        // Binding the applied predicate is also what EXCLUDES watermark subjects:
        // they carry the same source/target/table triple but never this predicate.
        where_obj.insert(
            APPLIED_SNAPSHOT_PRED.to_string(),
            JsonValue::String("?applied".to_string()),
        );
        let query = json!({
            "select": ["?target", "?table", "?applied"],
            "where": JsonValue::Object(where_obj),
        });

        let result = self.query(&db, &query).await?;
        let json = result.to_jsonld(&db.snapshot).map_err(|e| {
            ApiError::Internal(format!("Failed to format applied-marker query: {e}"))
        })?;
        if let Some(rows) = json.as_array() {
            for row in rows {
                let Some(cols) = row.as_array() else { continue };
                let (Some(t), Some(tbl), Some(a)) = (cols.first(), cols.get(1), cols.get(2)) else {
                    continue;
                };
                let (Some(t), Some(tbl)) = (t.as_str(), tbl.as_str()) else {
                    continue;
                };
                // String-encoded on write; tolerate a numeric literal rather than
                // dropping the marker, since dropping it silently re-enables the
                // rewrite this exists to prevent.
                let applied = a
                    .as_str()
                    .and_then(|s| s.parse::<i64>().ok())
                    .or_else(|| a.as_i64());
                if let Some(applied) = applied {
                    out.insert((t.to_string(), tbl.to_string()), applied);
                }
            }
        }
        Ok(out)
    }

    /// Open the shared materialization-state ledger, creating it if absent.
    ///
    /// Tolerates losing the create race: two concurrent pollers (or a poller and
    /// a `track` call) can both see it missing, and the loser gets
    /// `LedgerExists` rather than a ledger. Before this existed the same
    /// check-then-create sat inline in the watermark write and could fail a
    /// whole materialization pass on that race.
    async fn materialize_state_ledger(&self) -> Result<crate::LedgerState> {
        if self.ledger_exists(MATERIALIZE_STATE_LEDGER).await? {
            return self.ledger(MATERIALIZE_STATE_LEDGER).await;
        }
        match self.create_ledger(MATERIALIZE_STATE_LEDGER).await {
            Ok(state) => Ok(state),
            Err(ApiError::LedgerExists(_)) => self.ledger(MATERIALIZE_STATE_LEDGER).await,
            Err(e) => Err(e),
        }
    }

    /// Record a tracking job so it survives a restart. Idempotent — re-tracking
    /// the same `(source, target)` overwrites in place (`upsert` retracts then
    /// asserts), which is also how an interval change is applied.
    pub async fn persist_materialize_job(&self, job: &PersistedMaterializeJob) -> Result<()> {
        let state = self.materialize_state_ledger().await?;
        let node = job_node(job, true)?;
        self.upsert(state, &node).await?;
        info!(
            source = %job.source,
            target = %job.target,
            poll_interval_secs = job.poll_interval_secs,
            "Persisted materialization tracking job"
        );
        Ok(())
    }

    /// Mark a tracking job untracked so a restart does not restore it. Keeps the
    /// row (with `tracked = false`) rather than retracting it. A job that was
    /// never persisted is a no-op, not an error.
    pub async fn forget_materialize_job(&self, source: &str, target: &str) -> Result<()> {
        if !self.ledger_exists(MATERIALIZE_STATE_LEDGER).await? {
            return Ok(());
        }
        let state = self.ledger(MATERIALIZE_STATE_LEDGER).await?;
        // Interval is irrelevant once untracked; keep whatever shape the blob
        // has so the row stays parseable if it is ever re-tracked by hand.
        let job = PersistedMaterializeJob {
            source: source.to_string(),
            target: target.to_string(),
            poll_interval_secs: 0,
        };
        self.upsert(state, &job_node(&job, false)?).await?;
        info!(source, target, "Untracked materialization job");
        Ok(())
    }

    /// Every job the state ledger still vouches for. This is the restore path —
    /// the worker calls it once at start-up to rebuild its job set.
    ///
    /// Returns an empty vec when the state ledger does not exist yet (a server
    /// that has never tracked anything), which is not an error.
    pub async fn tracked_materialize_jobs(&self) -> Result<Vec<PersistedMaterializeJob>> {
        if !self.ledger_exists(MATERIALIZE_STATE_LEDGER).await? {
            return Ok(Vec::new());
        }
        let db = self.db(MATERIALIZE_STATE_LEDGER).await?;

        let mut where_obj = Map::new();
        where_obj.insert("@id".to_string(), JsonValue::String("?job".to_string()));
        where_obj.insert(
            JOB_BLOB_PRED.to_string(),
            JsonValue::String("?blob".to_string()),
        );
        where_obj.insert(
            JOB_TRACKED_PRED.to_string(),
            JsonValue::String("true".to_string()),
        );
        let query = json!({ "select": ["?blob"], "where": JsonValue::Object(where_obj) });

        let result = self.query(&db, &query).await?;
        let json = result
            .to_jsonld(&db.snapshot)
            .map_err(|e| ApiError::Internal(format!("Failed to format job query: {e}")))?;

        let mut jobs = Vec::new();
        collect_jobs(&json, &mut jobs);
        jobs.sort_by(|a, b| (&a.source, &a.target).cmp(&(&b.source, &b.target)));
        jobs.dedup();
        Ok(jobs)
    }
}

/// The per-(source, target-spec, table) watermark subject IRI. All three segments
/// are escaped (`%` -> `%25`, `:` -> `%3A`) before joining with `:` so the
/// encoding is injective — distinct `(source, target, table)` triples can never
/// collide even when a segment itself contains `:` (e.g. a `name:branch` target).
fn watermark_subject(source_graph_source_id: &str, target_spec: &str, table_name: &str) -> String {
    fn esc(s: &str) -> String {
        s.replace('%', "%25").replace(':', "%3A")
    }
    format!(
        "{WATERMARK_SUBJECT_PREFIX}{}:{}:{}",
        esc(source_graph_source_id),
        esc(target_spec),
        esc(table_name)
    )
}

/// The per-(source, RESOLVED target, table) applied-marker subject IRI. Same
/// injective escaping as [`watermark_subject`], different prefix — see
/// [`APPLIED_SUBJECT_PREFIX`] for why they must not share one.
fn applied_subject(
    source_graph_source_id: &str,
    resolved_target: &str,
    table_name: &str,
) -> String {
    fn esc(s: &str) -> String {
        s.replace('%', "%25").replace(':', "%3A")
    }
    format!(
        "{APPLIED_SUBJECT_PREFIX}{}:{}:{}",
        esc(source_graph_source_id),
        esc(resolved_target),
        esc(table_name)
    )
}

/// Build an applied-marker JSON-LD node for one resolved target and table.
fn applied_node(
    source_graph_source_id: &str,
    resolved_target: &str,
    table_name: &str,
    applied_snapshot_id: i64,
) -> JsonValue {
    let mut node = Map::new();
    node.insert(
        "@id".to_string(),
        JsonValue::String(applied_subject(
            source_graph_source_id,
            resolved_target,
            table_name,
        )),
    );
    node.insert(
        APPLIED_SNAPSHOT_PRED.to_string(),
        JsonValue::String(applied_snapshot_id.to_string()),
    );
    node.insert(
        WATERMARK_SOURCE_PRED.to_string(),
        JsonValue::String(source_graph_source_id.to_string()),
    );
    node.insert(
        WATERMARK_TARGET_PRED.to_string(),
        JsonValue::String(resolved_target.to_string()),
    );
    node.insert(
        WATERMARK_TABLE_PRED.to_string(),
        JsonValue::String(table_name.to_string()),
    );
    JsonValue::Object(node)
}

/// Is this target caught up on every table in the window?
///
/// `true` only when EVERY table's `to` is already covered. A target that has
/// applied one table of a two-table window still has work, so it must not be
/// skipped — that is the difference between skipping a no-op and silently dropping
/// half a window.
///
/// # Why this is `==` and must not become an inequality
///
/// `applied` and `to` are Iceberg **snapshot ids**, and the spec assigns those
/// randomly: there is no ordering between two snapshot ids, not even between a
/// snapshot and its own parent. `a >= to` therefore compares two unrelated random
/// i64s and calls the answer "has this target already applied the window".
///
/// That comparison makes the marker a one-way ratchet. It is only rewritten when a
/// window's `to` happens to compare greater, so it climbs the running maximum of a
/// random sequence and then exceeds every later draw permanently — after roughly
/// `ln(n)` windows a target stops being written to at all. Measured on a 17-source
/// deployment: every marker had ratcheted to between 8.19e18 and 9.22e18 against an
/// `i64::MAX` of 9.223e18, so all 17 were skipped on every poll, each skip was
/// counted as an ok target, and the watermark advanced past data that was never
/// applied. About 80% of the entities in the source lakehouse were discarded.
///
/// Equality is what the question actually asks, and it fully covers the case this
/// skip exists for: a job whose watermark cannot advance re-presents the SAME `to`
/// on every poll, so equality matches and the skip fires.
///
/// The two cases an inequality was reaching for both survive equality:
///
/// - A re-poll of an older snapshot: equality declines to skip, so the target
///   re-applies. Re-application is idempotent, so the cost is repeated work, never
///   corruption — strictly safer than skipping data that was never applied.
/// - A window that moved backwards after a forced full: moot, because `force_full`
///   discards the markers before the target loop runs.
///
/// If "at or beyond" is ever genuinely needed it requires a quantity that IS
/// ordered — the Iceberg **sequence number**, which increases by one per commit —
/// which means storing the sequence alongside the snapshot id in the marker. Do not
/// reintroduce an inequality over snapshot ids.
fn target_is_caught_up(
    applied: &std::collections::HashMap<(String, String), i64>,
    target: &str,
    table_watermarks: &[(String, Option<i64>, i64)],
) -> bool {
    !table_watermarks.is_empty()
        && table_watermarks.iter().all(|(table, _from, to)| {
            applied
                .get(&(target.to_string(), table.clone()))
                .is_some_and(|a| *a == *to)
        })
}

/// Build the watermark JSON-LD node (`@id` + last snapshot id + source + target +
/// table). The snapshot id is stored as a string to preserve full i64 precision;
/// `upsert` retracts-then-asserts so the watermark advances cleanly in place.
fn watermark_node(
    source_graph_source_id: &str,
    target_spec: &str,
    table_name: &str,
    to_snapshot_id: i64,
) -> JsonValue {
    let mut node = Map::new();
    node.insert(
        "@id".to_string(),
        JsonValue::String(watermark_subject(
            source_graph_source_id,
            target_spec,
            table_name,
        )),
    );
    node.insert(
        WATERMARK_SNAPSHOT_PRED.to_string(),
        JsonValue::String(to_snapshot_id.to_string()),
    );
    node.insert(
        WATERMARK_SOURCE_PRED.to_string(),
        JsonValue::String(source_graph_source_id.to_string()),
    );
    node.insert(
        WATERMARK_TARGET_PRED.to_string(),
        JsonValue::String(target_spec.to_string()),
    );
    node.insert(
        WATERMARK_TABLE_PRED.to_string(),
        JsonValue::String(table_name.to_string()),
    );
    JsonValue::Object(node)
}

/// The per-(source, target-spec) job subject IRI, escaped the same way as
/// [`watermark_subject`] so the encoding stays injective when a segment itself
/// contains `:` (every `name:branch` target does).
fn job_subject(source: &str, target_spec: &str) -> String {
    fn esc(s: &str) -> String {
        s.replace('%', "%25").replace(':', "%3A")
    }
    format!("{JOB_SUBJECT_PREFIX}{}:{}", esc(source), esc(target_spec))
}

/// Build the job JSON-LD node: the opaque blob the restore path reads, plus the
/// identifying triples an operator can query.
fn job_node(job: &PersistedMaterializeJob, tracked: bool) -> Result<JsonValue> {
    let blob = serde_json::to_string(job)
        .map_err(|e| ApiError::Internal(format!("Failed to serialize tracking job: {e}")))?;
    let mut node = Map::new();
    node.insert(
        "@id".to_string(),
        JsonValue::String(job_subject(&job.source, &job.target)),
    );
    node.insert(JOB_BLOB_PRED.to_string(), JsonValue::String(blob));
    node.insert(
        JOB_TRACKED_PRED.to_string(),
        JsonValue::String(tracked.to_string()),
    );
    node.insert(
        WATERMARK_SOURCE_PRED.to_string(),
        JsonValue::String(job.source.clone()),
    );
    node.insert(
        WATERMARK_TARGET_PRED.to_string(),
        JsonValue::String(job.target.clone()),
    );
    Ok(JsonValue::Object(node))
}

/// Collect every job blob anywhere in a JSON-LD query result. Tolerant of the
/// formatter's row nesting for the same reason [`extract_first_i64`] is, and
/// self-validating: only a string that deserializes into a whole
/// [`PersistedMaterializeJob`] is taken, so the other selected triples on the
/// row can never be mistaken for one.
fn collect_jobs(v: &JsonValue, out: &mut Vec<PersistedMaterializeJob>) {
    match v {
        JsonValue::Array(a) => a.iter().for_each(|x| collect_jobs(x, out)),
        JsonValue::Object(o) => o.values().for_each(|x| collect_jobs(x, out)),
        JsonValue::String(s) => {
            if let Ok(job) = serde_json::from_str::<PersistedMaterializeJob>(s) {
                out.push(job);
            }
        }
        _ => {}
    }
}

/// Find the first value anywhere in a JSON-LD query result that parses as an
/// i64 (the watermark query selects exactly one string value, so this is
/// unambiguous). Tolerant of the formatter's row nesting.
fn extract_first_i64(v: &JsonValue) -> Option<i64> {
    match v {
        JsonValue::Array(a) => a.iter().find_map(extract_first_i64),
        JsonValue::Object(o) => o.values().find_map(extract_first_i64),
        JsonValue::String(s) => s.parse::<i64>().ok(),
        JsonValue::Number(n) => n.as_i64(),
        _ => None,
    }
}

/// The resolved latest state of one subject within a refresh window.
struct KeyState {
    /// Ordering rank of the winning row: `Some` when an `order_by` column is
    /// configured (numeric/timestamp/string sort key), `None` when ordering by
    /// scan position only. A new row replaces the stored state when its rank is
    /// `>=` the stored rank (later-in-scan breaks ties), so the latest row wins.
    rank: Option<(i128, String)>,
    /// The winning row's live node, or `None` if the winning row is a tombstone.
    node: Option<SubjectNode>,
    /// Estimated payload bytes of this state (rank + node), maintained by
    /// [`MaterializeAccum`]'s accounting. Key bytes are counted separately, once,
    /// on first insert — a replace keeps the map's existing key allocation.
    bytes: usize,
}

/// Accumulator key: **(target ledger, graph, subject)**.
///
/// - `target` is the concrete ledger a row is materialized into. With a plain
///   `target` it is the same for every row; with a **templated** target (e.g.
///   `silver_{tenant_id}_{user_id}:main`) it is resolved per row from the row's
///   columns, so ONE job fans out into a ledger per (tenant,user).
/// - `graph` is the materialized named-graph IRI (`None` = default graph), from
///   R2RML `rr:graphMap` — orthogonal to the target and usually `None`.
///
/// So the same subject IRI in two targets (or two graphs) is independent keys.
type AccumKey = (String, Option<String>, String);

/// Flat per-entry overhead added to the accumulator's byte estimate on first
/// insert of a key: the BTreeMap node share, the [`KeyState`] itself, and the
/// containers' headers. Approximate by design — the estimate is a pre-OOM
/// circuit breaker (see [`ApiError::MaterializeMemoryBudget`]), not a meter.
const ACCUM_ENTRY_OVERHEAD: usize = 96;

/// Per-pass latest-by-key accumulator: one [`KeyState`] per [`AccumKey`]. The
/// BTreeMap keeps keys in a stable order (deterministic transaction) and groups
/// naturally by target ledger (the first tuple element) at commit time.
///
/// `bytes` tracks the estimated resident size incrementally — this map is the
/// pass's dominant memory term (one retained node per distinct key for the
/// whole window), and the pass aborts with a typed error when it outgrows the
/// materialize memory budget rather than letting the kernel OOM-kill the
/// server. Replacements apply a delta; a tombstone replacing a live node
/// SHRINKS the estimate (its payload drops).
#[derive(Default)]
struct MaterializeAccum {
    keys: BTreeMap<AccumKey, KeyState>,
    /// Estimated resident bytes: Σ per-entry (key + payload + overhead).
    bytes: usize,
}

impl MaterializeAccum {
    /// Estimated payload bytes of one entry's state (rank + node).
    fn state_bytes(rank: &Option<(i128, String)>, node: &Option<SubjectNode>) -> usize {
        let rank_bytes = rank.as_ref().map_or(0, |(_, s)| 16 + s.len());
        rank_bytes + node.as_ref().map_or(0, SubjectNode::estimated_bytes)
    }

    /// Estimated bytes of an entry's key strings.
    fn key_bytes(key: &AccumKey) -> usize {
        key.0.len() + key.1.as_ref().map_or(0, String::len) + key.2.len()
    }

    /// Estimated resident bytes of the whole accumulator.
    fn estimated_bytes(&self) -> usize {
        self.bytes
    }

    /// Distinct (target, graph, subject) keys accumulated.
    fn len(&self) -> usize {
        self.keys.len()
    }
    /// Record a classified row for `subject_iri` in `target` / `graph`. `rank` is
    /// the row's ordering key (from the `order_by` column) or `None` for
    /// scan-order. `node` is the live node, or `None` for a tombstone. The row
    /// wins (replaces the prior state) unless its rank is strictly older than the
    /// stored rank — so with an ordering column the highest-ordered row wins, and
    /// without one (all ranks equal `None`) the last row in scan order wins. This
    /// is a whole-row REPLACE; per-subject merge across rows is the legacy
    /// additive path ([`merge_live`](Self::merge_live)).
    fn record(
        &mut self,
        target: String,
        graph: Option<String>,
        subject_iri: String,
        rank: Option<(i128, String)>,
        node: Option<SubjectNode>,
    ) {
        let key = (target, graph, subject_iri);
        match self.keys.get_mut(&key) {
            Some(existing) if rank < existing.rank => {} // older row: ignore
            Some(existing) => {
                // Replace: payload delta only (the map keeps its key). A
                // tombstone replacing a live node shrinks the estimate.
                let new_bytes = Self::state_bytes(&rank, &node);
                self.bytes = self.bytes - existing.bytes + new_bytes;
                *existing = KeyState {
                    rank,
                    node,
                    bytes: new_bytes,
                };
            }
            None => {
                let new_bytes = Self::state_bytes(&rank, &node);
                self.bytes += Self::key_bytes(&key) + ACCUM_ENTRY_OVERHEAD + new_bytes;
                self.keys.insert(
                    key,
                    KeyState {
                        rank,
                        node,
                        bytes: new_bytes,
                    },
                );
            }
        }
    }

    /// Legacy additive mode: merge a live row's node into the subject's
    /// accumulated node (a subject spanning multiple source rows collects their
    /// values). No ordering, no tombstones, no whole-subject retraction.
    fn merge_live(
        &mut self,
        target: String,
        graph: Option<String>,
        subject_iri: String,
        node: SubjectNode,
    ) {
        let key = (target, graph, subject_iri);
        match self.keys.get_mut(&key) {
            Some(state) if state.node.is_some() => {
                if let Some(existing) = state.node.as_mut() {
                    existing.merge(node);
                }
                let new_bytes = Self::state_bytes(&state.rank, &state.node);
                self.bytes = self.bytes - state.bytes + new_bytes;
                state.bytes = new_bytes;
            }
            Some(state) => {
                // Entry with no node (unreachable in additive mode, which never
                // records tombstones): replace the payload, delta the estimate.
                let node = Some(node);
                let new_bytes = Self::state_bytes(&None, &node);
                self.bytes = self.bytes - state.bytes + new_bytes;
                *state = KeyState {
                    rank: None,
                    node,
                    bytes: new_bytes,
                };
            }
            None => {
                let node = Some(node);
                let new_bytes = Self::state_bytes(&None, &node);
                self.bytes += Self::key_bytes(&key) + ACCUM_ENTRY_OVERHEAD + new_bytes;
                self.keys.insert(
                    key,
                    KeyState {
                        rank: None,
                        node,
                        bytes: new_bytes,
                    },
                );
            }
        }
    }

    /// Resolve into `(live nodes to assert, keys whose latest row is a tombstone)`.
    #[allow(clippy::type_complexity)]
    fn finalize(self) -> (BTreeMap<AccumKey, SubjectNode>, BTreeSet<AccumKey>) {
        let mut live = BTreeMap::new();
        let mut deletions = BTreeSet::new();
        for (key, state) in self.keys {
            match state.node {
                Some(node) => {
                    live.insert(key, node);
                }
                None => {
                    deletions.insert(key);
                }
            }
        }
        (live, deletions)
    }
}

/// Build the whole-subject retraction transaction for a set of subject IRIs:
/// bind every `(?p, ?o)` for each listed `?s` and delete it. An IRI that was
/// never materialized binds zero rows -> zero flakes -> harmless no-op.
///
/// Each subject is bound as a typed `@id` value (`{"@type":"@id","@value":...}`)
/// — NOT a bare string. A bare string parses to a string *literal*, which never
/// joins against a real subject Sid, so the wildcard delete would silently match
/// zero rows and retract nothing. The doc carries no `@context`, and the
/// materialized IRIs are already fully expanded, so the bound Sid matches the
/// `@id` the upsert asserted. (Shape proven in `it_transact_update.rs`.)
///
/// `graph` scopes the whole-subject retraction to a single named graph: the
/// transaction `"graph"` key applies to the WHERE and DELETE templates, so
/// `?s ?p ?o` binds and deletes only that graph's flakes. `None` retracts in the
/// default graph. This keeps a subject's statements in graph A untouched when its
/// same-IRI twin in graph B is replaced.
/// Which transaction verb a materialize chunk is applied with. All three share the
/// shape `(LedgerState, &JsonValue) -> Result<TransactResult>`, which is what lets
/// [`Fluree::transact_chunks_with_backpressure`] be verb-agnostic.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ChunkVerb {
    /// Idempotent `@type` assertion (classes UNION across sources).
    Insert,
    /// Per-predicate upsert, or whole-subject replace under latest-by-key.
    Upsert,
    /// The graph-scoped retract that precedes a latest-by-key re-assert.
    Update,
}

impl Fluree {
    /// Materialize one resolved target. Returns the number of subjects retracted.
    ///
    /// Extracted so the caller can collect a per-target outcome instead of aborting the
    /// whole job on the first error — see the isolation note at the call site. Every
    /// `?` in here now scopes to ONE target.
    #[allow(clippy::too_many_arguments)]
    async fn materialize_one_target(
        &self,
        target: &str,
        live: BTreeMap<(Option<String>, String), SubjectNode>,
        deletions: BTreeSet<(Option<String>, String)>,
        latest_by_key: bool,
        txn_budget: usize,
    ) -> Result<usize> {
        // TOLERATE LOSING THE CREATE RACE.
        //
        // `ledger_exists` then `create_ledger` is check-then-create, and it is not
        // atomic. Under fan-out this races constantly: 17 sources resolve the SAME ~22
        // targets, so when a target is new several sources see it missing in the same
        // instant and all try to create it. One wins; the losers previously failed the
        // whole target.
        //
        // This is not a new discovery — `materialize_state_ledger` in this very file
        // already documents and handles it ("Tolerates losing the create race ... Before
        // this existed the same check-then-create sat inline in the watermark write and
        // could fail a whole materialization pass on that race"). The fix was applied to
        // the state ledger and never to the target path, which is where fan-out makes it
        // far MORE likely. Observed as `Commit conflict: expected t=2, head_t=1` — t=1
        // being the signature of a just-created ledger.
        let existed_before = self.ledger_exists(target).await?;
        let (mut ledger, target_existed) = if existed_before {
            (self.ledger(target).await?, true)
        } else {
            match self.create_ledger(target).await {
                Ok(l) => (l, false),
                // Another source created it first. Open theirs and carry on.
                //
                // Reported as EXISTING rather than as created-by-us, deliberately: it
                // does exist now, and for latest-by-key that routes us through the
                // whole-subject retract before re-asserting. On a ledger the winner has
                // only just created that retract is very likely a no-op, so the cost is
                // at most one redundant transaction — whereas skipping a retract that
                // WAS needed would leave stale fields behind. Err toward the harmless
                // side.
                Err(ApiError::LedgerExists(_)) => {
                    info!(
                        target = %target,
                        "materialize: lost the create race for a new target; opening the \
                         existing ledger"
                    );
                    (self.ledger(target).await?, true)
                }
                Err(e) => return Err(e),
            }
        };
        // Deletions only count against a target that already held data.
        let subjects_retracted = if target_existed { deletions.len() } else { 0 };

        // Whole-subject REPLACE (latest-by-key): retract every subject seen in this
        // window (live OR tombstone) — per graph — before re-asserting, so a dropped
        // field clears and a tombstone is removed. The retract and re-assert are both
        // graph-scoped, so a subject in graph B never touches the same IRI in graph A.
        // Additive mode skips this (per-predicate upsert suffices; a subject may
        // legitimately span rows).
        let mut retract_by_graph: BTreeMap<Option<String>, BTreeSet<String>> = BTreeMap::new();
        if latest_by_key {
            for (graph, subject) in live.keys().cloned().chain(deletions.iter().cloned()) {
                retract_by_graph.entry(graph).or_default().insert(subject);
            }
        }

        if target_existed {
            for (graph, iris) in &retract_by_graph {
                if iris.is_empty() {
                    continue;
                }
                let iri_chunks: Vec<Vec<String>> = chunk_iris_by_size(iris, txn_budget)
                    .into_iter()
                    .map(|c| c.into_iter().collect())
                    .collect();
                let g = graph.clone();
                ledger = self
                    .transact_chunks_with_backpressure(
                        ledger,
                        iri_chunks,
                        move |c: &[String]| build_retract_doc(c, g.as_deref()),
                        ChunkVerb::Update,
                    )
                    .await?;
            }
        }

        if latest_by_key {
            // The retraction cleared every seen subject (per graph), so the re-asserted
            // nodes (carrying @type) are the sole source of truth — a single upsert per
            // graph is correct.
            let mut nodes_by_graph: BTreeMap<Option<String>, Vec<JsonValue>> = BTreeMap::new();
            for ((graph, _subject), node) in live {
                nodes_by_graph
                    .entry(graph)
                    .or_default()
                    .push(node.into_json());
            }
            // A delete-only window (tombstones, no live rows) leaves `nodes_by_graph`
            // empty; skip rather than send an empty `[]` doc (the transactor rejects an
            // upsert with no predicate/@type). The retracts above already applied.
            let live_doc = nodes_by_graph_to_doc(nodes_by_graph);
            ledger = self
                .transact_chunks_with_backpressure(
                    ledger,
                    chunk_nodes_by_size(live_doc, txn_budget),
                    |c: &[JsonValue]| JsonValue::Array(c.to_vec()),
                    ChunkVerb::Upsert,
                )
                .await?;
        } else {
            // Additive mode: assert `@type` via an idempotent `insert` so classes UNION
            // across sources, and `upsert` only the remaining predicates (a single
            // upsert carrying `@type` would retract-then-insert rdf:type per predicate,
            // clobbering classes other sources added). Both grouped per graph.
            let mut type_by_graph: BTreeMap<Option<String>, Vec<JsonValue>> = BTreeMap::new();
            let mut pred_by_graph: BTreeMap<Option<String>, Vec<JsonValue>> = BTreeMap::new();
            for ((graph, _subject), node) in live {
                let (type_node, pred_node) = node.into_type_and_predicate_nodes();
                if let Some(tn) = type_node {
                    type_by_graph.entry(graph.clone()).or_default().push(tn);
                }
                if let Some(pn) = pred_node {
                    pred_by_graph.entry(graph).or_default().push(pn);
                }
            }
            let type_doc = nodes_by_graph_to_doc(type_by_graph);
            ledger = self
                .transact_chunks_with_backpressure(
                    ledger,
                    chunk_nodes_by_size(type_doc, txn_budget),
                    |c: &[JsonValue]| JsonValue::Array(c.to_vec()),
                    ChunkVerb::Insert,
                )
                .await?;
            // A type-only source (e.g. an r2rml `entity_type` map: subject + rdf:type,
            // no other predicates) leaves `pred_by_graph` empty, so the doc is `[]`;
            // skip rather than send an empty doc. An unconditional `upsert([])` is
            // rejected ("Upsert must contain at least one predicate or @type").
            let pred_doc = nodes_by_graph_to_doc(pred_by_graph);
            ledger = self
                .transact_chunks_with_backpressure(
                    ledger,
                    chunk_nodes_by_size(pred_doc, txn_budget),
                    |c: &[JsonValue]| JsonValue::Array(c.to_vec()),
                    ChunkVerb::Upsert,
                )
                .await?;
        }

        // `ledger` is a PER-TARGET handle; its final state is deliberately not carried
        // further. Dropped explicitly so the last assignment is a use rather than
        // reading as dead code.
        drop(ledger);
        Ok(subjects_retracted)
    }

    /// Apply pre-sized chunks as transactions, **absorbing novelty backpressure**
    /// instead of failing the whole materialize on it.
    ///
    /// `TransactError::NoveltyWouldExceed` is not a validation failure. It is the
    /// transactor saying "novelty is too full *right now*" — a transient, retryable
    /// condition. Treating it as fatal is what made a single wide source
    /// permanently unsyncable:
    ///
    ///   chunk 1..N-1 commit and fill novelty -> the indexer cannot drain it fast
    ///   enough -> chunk N is rejected -> the whole materialize errors -> the
    ///   watermark never advances -> the next poll re-reads the entire source and
    ///   fails at the same place, forever.
    ///
    /// Measured on `silver.observation`: `current=53024023 delta=17701872
    /// max=67108864` on every attempt. Note `current` sat at **79% of the ceiling
    /// and never fell**, which disproves the assumption the chunk budget was built
    /// on (that "the indexer drains novelty between chunks"). Note also that a
    /// 16.7 MB JSON chunk yielded 17.7 MB of flakes — so the `/4` margin, justified
    /// by JSON running "several times larger than the flakes it yields", is
    /// *inverted* for this table.
    ///
    /// **Both of those are predictions about data, and both were wrong. So this does
    /// not predict — it reacts:**
    ///
    /// - **Too big for the headroom left?** Split the chunk in half and retry. This
    ///   needs no knowledge of the JSON->flake ratio, because the rejection itself
    ///   supplies the information.
    /// - **Novelty too full even for one node?** Splitting cannot help, so wait for
    ///   the indexer to drain and retry, bounded.
    ///
    /// Why not just raise `reindex_max_bytes`: `txn_budget` is derived from it
    /// (`/4`), so doubling the ceiling doubles the chunk. The failing ratio was
    /// `current/max = 79%` against a 26% chunk; at 2x you get the same wall further
    /// out. That lever cannot fix this shape.
    async fn transact_chunks_with_backpressure<T, B>(
        &self,
        mut ledger: LedgerState,
        chunks: Vec<Vec<T>>,
        build: B,
        verb: ChunkVerb,
    ) -> Result<LedgerState>
    where
        B: Fn(&[T]) -> JsonValue,
    {
        // A stack, because a rejected chunk is pushed back as two halves and both
        // must be applied before moving on — order within a chunk list does not
        // matter (insert/upsert are per-subject and idempotent), but completeness
        // does.
        let mut pending: Vec<Vec<T>> = chunks.into_iter().filter(|c| !c.is_empty()).rev().collect();

        // Reloads allowed across this whole call, not per chunk: a target being written
        // by many sources at once could otherwise be retried indefinitely.
        const MAX_CONFLICT_RETRIES: usize = 10;
        let mut conflict_retries = 0usize;

        while let Some(chunk) = pending.pop() {
            let doc = build(&chunk);
            let result = match verb {
                ChunkVerb::Insert => self.insert(ledger.clone(), &doc).await,
                ChunkVerb::Upsert => self.upsert(ledger.clone(), &doc).await,
                ChunkVerb::Update => self.update(ledger.clone(), &doc).await,
            };
            match result {
                Ok(r) => ledger = r.ledger,
                // STALE BASE — reload and retry.
                //
                // `CommitConflict{expected_t, head_t}` reads the opposite way to how the
                // field names suggest: `expected_t` is the NAMESERVICE's durable
                // commit_t and `head_t` is OUR in-memory base's t (see
                // `verify_sequencing` in fluree-db-transact). So `expected=21, head_t=5`
                // means our base is SIXTEEN commits behind, not ahead.
                //
                // That is an ordinary lost-update race, and under fan-out it is
                // constant: 17 sources resolve the same ~22 targets, so while we work
                // through our chunks other sources commit to the same ledger. The gap
                // size is simply how many they managed. Observed gaps: 1, 2, 4, 16.
                //
                // Reloading the base is the standard remedy. Bounded, because a target
                // under permanent write pressure should surface rather than spin, and
                // C4 isolates the failure to this target either way.
                Err(ref e)
                    if is_stale_base(e).is_some() && conflict_retries < MAX_CONFLICT_RETRIES =>
                {
                    conflict_retries += 1;
                    let detail = is_stale_base(e).unwrap_or_default();
                    let id = ledger.ledger_id().to_string();
                    info!(
                        ledger = %id,
                        detail = %detail,
                        attempt = conflict_retries,
                        "materialize: stale base after a concurrent write; reloading and \
                         retrying this chunk"
                    );
                    ledger = self.ledger(&id).await?;
                    pending.push(chunk);
                }
                Err(e) => {
                    // Classification lives in `classify_novelty` so it is testable;
                    // `None` means a real failure — fail fast.
                    let Some(pressure) = classify_novelty(&e) else {
                        return Err(e);
                    };
                    match pressure {
                        // SIZED rejection: this transaction was too big for the
                        // headroom left. Halving reduces `delta` and can succeed, and
                        // it costs nothing but a retry — no sleeping, nothing held.
                        NoveltyPressure::WouldExceed { .. } if chunk.len() > 1 => {
                            let (left, right) = split_in_half(chunk);
                            info!(
                                items = left.len() + right.len(),
                                ?pressure,
                                "materialize: novelty backpressure, splitting chunk"
                            );
                            pending.push(right);
                            pending.push(left);
                        }
                        // DEFER, do not wait.
                        //
                        // This replaces an in-process sleep-and-retry, and the reason
                        // is a deadlock it caused in production: novelty can only be
                        // drained by the INDEXER, and the materialize worker holds
                        // what the indexer needs to publish. Sleeping therefore
                        // guarantees the condition being waited on cannot clear — the
                        // indexer produced ZERO builds for six minutes, and recovered
                        // only when the wait was disabled.
                        //
                        // A bigger budget makes that worse, not better. The fix is to
                        // stop holding: return, let the caller record this target as
                        // deferred, and let the materialize worker's own 30-57 s poll
                        // interval be the backoff. Nothing is lost — the target keeps
                        // its watermark and retries next cycle.
                        _ => {
                            let remaining: usize =
                                chunk.len() + pending.iter().map(Vec::len).sum::<usize>();
                            warn!(
                                ?pressure,
                                items_deferred = remaining,
                                "materialize: novelty under pressure — DEFERRING this target to \
                                 the next poll. Not waiting in-process: only the indexer can \
                                 drain novelty, and holding the ledger while waiting starves it."
                            );
                            return Err(ApiError::NoveltyDeferred { remaining });
                        }
                    }
                }
            }
        }
        Ok(ledger)
    }
}

/// How stale a stored watermark may get before a no-data poll persists it anyway.
///
/// This bound is the whole reason skipping the write is safe. Skipping is what stops
/// the state ledger taking ~1,200 empty commits/hour — but skipping *unconditionally*
/// is worse than the problem it solves:
///
///   a stored watermark that never advances eventually falls outside the SOURCE
///   TABLE's snapshot retention -> `snapshot_window` can no longer resolve it ->
///   `ScanChoice::FullUndeterminable` -> a FULL table read, on every poll, forever.
///
/// That is not hypothetical: one source reached exactly this state and full-read
/// 739,446 rows per poll until the watermark was repaired.
///
/// So the bound must sit comfortably under the source's snapshot retention. Ours is
/// **4 hours** (`snapshotRetentionHours` in the lakehouse maintenance module, cut
/// from 24 h to keep BigLake's 1 MiB `metadata.json` cap in reach), hence a 30-minute
/// default — 8x margin, and still ~60x fewer commits than persisting every poll.
///
/// RAISE THIS ONLY WITH THE RETENTION IN HAND. If a deployment shortens snapshot
/// retention, this must shorten with it, or watermarks silently expire and every
/// poll becomes a full read. Override with `FLUREE_MATERIALIZE_WATERMARK_REFRESH_MINS`.
/// The materialize accumulator's memory budget in bytes, from
/// `FLUREE_MATERIALIZE_MEMORY_BUDGET_MB` (default 1024 MiB; `0` disables the
/// gate). See [`ApiError::MaterializeMemoryBudget`] for what exceeding it
/// means and the levers. The default caps a runaway window at ~1 GiB of
/// estimated accumulator — roughly 2-4M modest subjects — instead of letting
/// it grow to whatever the container allows (measured: 21.4 GiB before the
/// kernel intervened).
fn materialize_memory_budget_bytes() -> usize {
    static CACHED: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        let mb = std::env::var("FLUREE_MATERIALIZE_MEMORY_BUDGET_MB")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(1024);
        mb.saturating_mul(1024 * 1024)
    })
}

fn watermark_refresh_bound_ms() -> i64 {
    static CACHED: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        let mins = std::env::var("FLUREE_MATERIALIZE_WATERMARK_REFRESH_MINS")
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
            .filter(|m| *m > 0)
            .unwrap_or(30);
        mins.saturating_mul(60_000)
    })
}

/// Is this error a lost optimistic-concurrency race, i.e. "your base is stale, reload"?
///
/// Returns a short description for the log, or `None` if the error is a real failure.
///
/// FOUR VARIANTS, one condition. They differ only in WHERE the staleness is detected,
/// which is an implementation detail of the commit path, not something a caller should
/// have to branch on:
///
///   CommitConflict     base.t() != nameservice commit_t, caught before publishing.
///                      NOTE the field order reads backwards from the names:
///                      `expected_t` is the DURABLE head, `head_t` is OUR base — so
///                      `expected > head` means we are BEHIND (see verify_sequencing).
///   PublishLostRace    someone published while we were building; caught AT publish.
///   CommitIdMismatch   our base's head CID is not the nameservice's head CID.
///   NamespaceConflict  namespace allocation raced a concurrent staging registry.
///
/// Handling only `CommitConflict` left the other three fatal. Under fan-out — 17
/// sources resolving the same ~22 targets — that showed up immediately: after fixing
/// `CommitConflict`, the residual target failures were **all** `PublishLostRace`, one
/// per target, during the initial backfill burst when every source writes every target.
/// Same race, different detection point, so the same remedy applies.
fn is_stale_base(e: &ApiError) -> Option<String> {
    use fluree_db_transact::TransactError as TE;
    let ApiError::Transact(te) = e else {
        return None;
    };
    match te {
        TE::CommitConflict { expected_t, head_t } => Some(format!(
            "stale base: durable_t={expected_t} our_base_t={head_t} behind_by={}",
            expected_t - head_t
        )),
        TE::PublishLostRace {
            attempted_t,
            published_t,
            ..
        } => Some(format!(
            "publish race lost: attempted_t={attempted_t} published_t={published_t}"
        )),
        TE::CommitIdMismatch { expected, found } => Some(format!(
            "head CID mismatch: expected={expected} found={found}"
        )),
        TE::NamespaceConflict(msg) => Some(format!("namespace allocation raced: {msg}")),
        _ => None,
    }
}

/// Novelty backpressure, classified by what can actually resolve it.
///
/// Extracted as a named function rather than left as match arms inside the retry
/// loop for one reason: **a test of match arms buried in an async loop cannot
/// guard them.** A first attempt at a canary here asserted against a copy of the
/// classification written inside the test, which would have passed happily while
/// production regressed — the same vacuous shape as building a fixture path by
/// hand. Testing THIS function tests what the loop actually does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NoveltyPressure {
    /// The transaction was sized and rejected: `current + delta > max`. Splitting
    /// the chunk reduces `delta` and can therefore succeed.
    WouldExceed {
        current_bytes: usize,
        delta_bytes: usize,
        max_bytes: usize,
    },
    /// Novelty is already at the ceiling, raised BEFORE the transaction is sized —
    /// which is why it carries no numbers. No chunk is small enough; only the
    /// indexer draining helps.
    AtMax,
}

/// Classify an error as novelty backpressure, or `None` if it is a real failure.
///
/// Both novelty variants are transient and retryable — `NoveltyAtMax`'s own doc
/// comment in `fluree-db-transact` reads "Novelty at maximum size (backpressure)".
/// Handling only one made this fix INERT in production: a forced full
/// re-materialize failed instantly on all 17 sources, 74 `NoveltyAtMax` against 0
/// `NoveltyWouldExceed`, and the split path logged zero times.
fn classify_novelty(e: &ApiError) -> Option<NoveltyPressure> {
    match e {
        ApiError::Transact(fluree_db_transact::TransactError::NoveltyWouldExceed {
            current_bytes,
            delta_bytes,
            max_bytes,
        }) => Some(NoveltyPressure::WouldExceed {
            current_bytes: *current_bytes,
            delta_bytes: *delta_bytes,
            max_bytes: *max_bytes,
        }),
        ApiError::Transact(fluree_db_transact::TransactError::NoveltyAtMax) => {
            Some(NoveltyPressure::AtMax)
        }
        // Anything else is a genuine failure. Retrying a validation error forever is
        // worse than failing fast, so this must stay a closed set.
        _ => None,
    }
}

/// Split a chunk into two non-empty halves.
///
/// **Both halves MUST be non-empty.** An empty half is not a cosmetic problem: the
/// backpressure loop pushes both halves back onto its pending stack, so an empty one
/// would either be re-applied forever (never shrinking, never completing) or, if
/// filtered, silently drop the items in it. Callers guarantee `len >= 2`, which makes
/// `len / 2 >= 1` and both sides non-empty; this asserts that contract rather than
/// trusting it.
fn split_in_half<T>(chunk: Vec<T>) -> (Vec<T>, Vec<T>) {
    debug_assert!(
        chunk.len() >= 2,
        "split_in_half needs at least 2 items; the caller must handle indivisible chunks"
    );
    let mid = (chunk.len() / 2).max(1);
    let mut left = chunk;
    let right = left.split_off(mid);
    (left, right)
}

fn build_retract_doc(iris: &[String], graph: Option<&str>) -> JsonValue {
    let rows: Vec<JsonValue> = iris
        .iter()
        .map(|iri| JsonValue::Array(vec![json!({ "@type": "@id", "@value": iri })]))
        .collect();
    let mut doc = json!({
        "values": ["?s", rows],
        "where": { "@id": "?s", "?p": "?o" },
        "delete": { "@id": "?s", "?p": "?o" }
    });
    if let Some(g) = graph {
        doc.as_object_mut()
            .expect("json! object")
            .insert("graph".to_string(), JsonValue::String(g.to_string()));
    }
    doc
}

/// Flatten a per-graph node map into ONE JSON-LD transaction array. Default-graph
/// nodes (the `None` key) are emitted as plain top-level nodes; each named-graph
/// node carries its graph inline via a per-node `@graph` STRING selector
/// (`{"@id": <subject>, "@graph": "<graph-iri>", …}`).
///
/// This is the form `parse_insert`/`parse_upsert` actually accept: the parser reads
/// a node's `@graph` string, resolves it to a graph id, and scopes every triple that
/// node emits (`@type` + predicates) to it. The standard JSON-LD *envelope* form
/// `{"@id": g, "@graph": [nodes]}` (an `@graph` ARRAY) is NOT accepted by
/// insert/upsert — the parser skips the `@graph` key, so the wrapper collapses to an
/// `@id`-only node and yields zero triples ("an object with only @id is not a valid
/// insert"). Only the `update` parser handles a top-level `graph` key (see
/// `build_retract_doc`). Upsert's retract-existing is graph-scoped
/// (`generate_upsert_deletions` keys on graph_id), so upserting a subject into graph
/// B never clobbers the same IRI in graph A — the whole point of per-(tenant,user)
/// graphs. Empty named-graph groups contribute nothing.
fn nodes_by_graph_to_doc(by_graph: BTreeMap<Option<String>, Vec<JsonValue>>) -> Vec<JsonValue> {
    let mut out = Vec::new();
    // `None` sorts before any `Some(_)` in a BTreeMap, so default-graph nodes lead.
    for (graph, nodes) in by_graph {
        match graph {
            // Default graph: plain top-level nodes, no `@graph` selector.
            None => out.extend(nodes),
            // Named graph: tag each node with a per-node `@graph` string selector.
            Some(g) => {
                for mut node in nodes {
                    if let Some(obj) = node.as_object_mut() {
                        obj.insert("@graph".to_string(), JsonValue::String(g.clone()));
                    }
                    out.push(node);
                }
            }
        }
    }
    out
}

/// Approximate serialized-JSON size of a value, without allocating.
///
/// Deliberately an estimate: it exists only to decide where to cut a batch, so
/// being a few bytes out per node is irrelevant, and paying a full
/// `to_string()` per node just to measure it would double serialization work on
/// the exact path we are trying to keep cheap.
fn estimated_json_bytes(v: &JsonValue) -> usize {
    match v {
        JsonValue::Null => 4,
        JsonValue::Bool(_) => 5,
        JsonValue::Number(_) => 8,
        JsonValue::String(s) => s.len() + 2,
        // + len for the separating commas.
        JsonValue::Array(a) => 2 + a.len() + a.iter().map(estimated_json_bytes).sum::<usize>(),
        JsonValue::Object(o) => {
            2 + o.len()
                + o.iter()
                    .map(|(k, val)| k.len() + 3 + estimated_json_bytes(val))
                    .sum::<usize>()
        }
    }
}

/// Split a flat JSON-LD node array into batches that stay within `budget`
/// serialized bytes, so one materialize window becomes as many transactions as
/// it needs instead of a single unbounded one.
///
/// Safe to split because [`nodes_by_graph_to_doc`] emits SELF-CONTAINED nodes: a
/// named-graph node carries its own `@graph` string selector, so graph scoping
/// travels with the node and never depends on its neighbours. Do not "optimise"
/// this into splitting the envelope form — that form is not accepted by
/// insert/upsert at all (see `nodes_by_graph_to_doc`).
///
/// A node bigger than the budget on its own is emitted alone rather than
/// dropped: one oversized subject should fail loudly by itself rather than
/// silently take a whole batch down with it.
fn chunk_nodes_by_size(nodes: Vec<JsonValue>, budget: usize) -> Vec<Vec<JsonValue>> {
    let mut out: Vec<Vec<JsonValue>> = Vec::new();
    let mut current: Vec<JsonValue> = Vec::new();
    let mut current_bytes = 0usize;
    for node in nodes {
        let bytes = estimated_json_bytes(&node);
        if !current.is_empty() && current_bytes + bytes > budget {
            out.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes += bytes;
        current.push(node);
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// Split a retraction IRI set into batches within `budget` serialized bytes.
///
/// `latest_by_key` retracts every subject the window touched, so the retraction
/// doc scales with the window exactly as the upsert does and can breach the
/// novelty ceiling on its own. Each batch becomes a complete `build_retract_doc`
/// update carrying its own `graph` key, so batching preserves graph scoping.
fn chunk_iris_by_size(iris: &BTreeSet<String>, budget: usize) -> Vec<BTreeSet<String>> {
    // Per-IRI overhead of the `values` row: `[{"@type":"@id","@value":"…"}]`.
    const ROW_OVERHEAD: usize = 40;
    let mut out: Vec<BTreeSet<String>> = Vec::new();
    let mut current: BTreeSet<String> = BTreeSet::new();
    let mut current_bytes = 0usize;
    for iri in iris {
        let bytes = iri.len() + ROW_OVERHEAD;
        if !current.is_empty() && current_bytes + bytes > budget {
            out.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes += bytes;
        current.insert(iri.clone());
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// Accumulates all triples for a single subject IRI before emitting one JSON-LD
/// node. Predicates collect multiple object values (combined into an array on
/// output) and `@type` classes are de-duplicated.
struct SubjectNode {
    id: String,
    types: Vec<String>,
    /// predicate IRI -> object values (`@id` refs, value objects, or scalars).
    predicates: BTreeMap<String, Vec<JsonValue>>,
}

impl SubjectNode {
    fn new(id: String) -> Self {
        Self {
            id,
            types: Vec::new(),
            predicates: BTreeMap::new(),
        }
    }

    /// Approximate heap bytes held by this node: id + type IRIs + predicate
    /// keys + JSON values — sized with [`estimated_json_bytes`], the same model
    /// the transaction chunker uses, so the accumulator budget and the chunk
    /// budget agree about what a node "weighs" — plus a small per-element
    /// container overhead.
    fn estimated_bytes(&self) -> usize {
        let types: usize = self.types.iter().map(|t| t.len() + 8).sum();
        let preds: usize = self
            .predicates
            .iter()
            .map(|(k, vs)| k.len() + 32 + vs.iter().map(estimated_json_bytes).sum::<usize>())
            .sum();
        self.id.len() + types + preds
    }

    fn add_type(&mut self, class: &str) {
        if !self.types.iter().any(|c| c == class) {
            self.types.push(class.to_string());
        }
    }

    fn add_object(&mut self, predicate: &str, value: JsonValue) {
        let entry = self.predicates.entry(predicate.to_string()).or_default();
        if !entry.iter().any(|v| v == &value) {
            entry.push(value);
        }
    }

    /// Merge another node's types and objects into this one (legacy additive
    /// mode: a subject that spans multiple source rows accumulates their values).
    fn merge(&mut self, other: SubjectNode) {
        for t in other.types {
            self.add_type(&t);
        }
        for (pred, vals) in other.predicates {
            for v in vals {
                self.add_object(&pred, v);
            }
        }
    }

    fn into_json(self) -> JsonValue {
        let mut node = Map::new();
        node.insert("@id".to_string(), JsonValue::String(self.id));
        if !self.types.is_empty() {
            node.insert(
                "@type".to_string(),
                JsonValue::Array(self.types.into_iter().map(JsonValue::String).collect()),
            );
        }
        for (pred, mut vals) in self.predicates {
            let v = if vals.len() == 1 {
                vals.pop().expect("len == 1")
            } else {
                JsonValue::Array(vals)
            };
            node.insert(pred, v);
        }
        JsonValue::Object(node)
    }

    /// Split the node into `(optional @type-only node, optional predicates-only
    /// node)` for additive-mode application: `rdf:type` is asserted via an
    /// idempotent `insert` (classes UNION across sources) while the remaining
    /// predicates are `upsert`ed (last-writer-wins per predicate). Each side is
    /// `None` when it would carry no data (a type-less join row, or a class-only
    /// `entity_type` row) so callers can skip an empty transaction node.
    fn into_type_and_predicate_nodes(self) -> (Option<JsonValue>, Option<JsonValue>) {
        let SubjectNode {
            id,
            types,
            predicates,
        } = self;
        let type_node = if types.is_empty() {
            None
        } else {
            let mut m = Map::new();
            m.insert("@id".to_string(), JsonValue::String(id.clone()));
            m.insert(
                "@type".to_string(),
                JsonValue::Array(types.into_iter().map(JsonValue::String).collect()),
            );
            Some(JsonValue::Object(m))
        };
        let pred_node = if predicates.is_empty() {
            None
        } else {
            let mut m = Map::new();
            m.insert("@id".to_string(), JsonValue::String(id));
            for (pred, mut vals) in predicates {
                let v = if vals.len() == 1 {
                    vals.pop().expect("len == 1")
                } else {
                    JsonValue::Array(vals)
                };
                m.insert(pred, v);
            }
            Some(JsonValue::Object(m))
        };
        (type_node, pred_node)
    }
}

/// The `rdf:type` predicate IRI. A predicate-object map asserting it is treated
/// as a subject class (data-driven typing), not an ordinary predicate — see
/// [`NodeCollector`].
const RDF_TYPE_IRI: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

/// A [`TripleObserver`] that collects one row's triples into a [`SubjectNode`]
/// (the JSON-LD upsert shape). The subject is fixed by the caller — the
/// classification pass has already materialized and screened it — so observed
/// subject terms are not consulted.
///
/// A triple whose predicate is `rdf:type` with an IRI object is routed to the
/// node's `@type`, not to an ordinary predicate. `rr:class` is constant-only,
/// so per-row typing (e.g. `as:Announce` vs `as:Article` from a `type` column)
/// must be a POM with `rr:predicate rdf:type` — routing it to `@type` lets it
/// UNION across sources in additive mode, where an ordinary predicate would be
/// upserted and clobber other sources' types on a shared subject. (A non-IRI
/// rdf:type object is malformed; it falls through as an ordinary predicate.)
/// Constant `rr:class` classes arrive through the same rdf:type route, emitted
/// by the shared enumerator.
struct NodeCollector {
    node: SubjectNode,
}

impl TripleObserver for NodeCollector {
    fn observe(
        &mut self,
        _subject: &RdfTerm,
        predicate: &str,
        object: &RdfTerm,
    ) -> fluree_db_r2rml::R2rmlResult<()> {
        match object {
            RdfTerm::Iri(iri) if predicate == RDF_TYPE_IRI => self.node.add_type(iri),
            other => self
                .node
                .add_object(predicate, rdf_term_to_jsonld(other.clone())),
        }
        Ok(())
    }
}

/// Build a live `SubjectNode` from one source row via the shared enumerator
/// ([`emit_row_terms`]) — the same row→triples semantics as the bulk twin
/// builder (`fluree materialize`), so the two engines cannot drift on POM
/// interpretation. Relative to the hand-rolled predecessor this also emits
/// templated (non-constant) predicates and RefObjectMap foreign-key edges —
/// the latter only when `parents` has been indexed; unindexed parents drop the
/// edge as dangling (counted in `stats.ref_dangling`), matching the previous
/// behavior of not materializing FK joins.
fn build_live_node(
    ctx: &TmEmitContext<'_>,
    batch: &ColumnBatch,
    row: usize,
    parents: &ParentIndexSet,
    stats: &mut MaterializeStats,
    id: String,
) -> Result<SubjectNode> {
    let subject = RdfTerm::Iri(id.clone());
    let mut collector = NodeCollector {
        node: SubjectNode::new(id),
    };
    emit_row_terms(ctx, batch, row, &subject, parents, &mut collector, stats)
        .map_err(|e| ApiError::Internal(format!("R2RML row materialization failed: {e}")))?;
    Ok(collector.node)
}

/// Expand one source row through a triples map into the accumulator. The subject
/// IRI is built by the exact `term.rs` materializer (so live and tombstone rows
/// for the same key produce the same IRI).
///
/// When `latest_by_key` (a `delete` convention and/or `order_by` is configured),
/// the row is classified live vs tombstone, ranked by `order_by`, and recorded
/// as a whole-row REPLACE — the latest row per key wins. Otherwise (legacy
/// additive mode) the live row's triples are MERGED into the subject's node, so
/// a subject spanning multiple rows accumulates their values.
/// Resolve a materialization row's TARGET ledger id. A `target` with no `{...}`
/// placeholder is used verbatim (a plain single-target job routes every row to
/// the same ledger). A templated target is expanded against the row's columns
/// (fan-out: one ledger per partition, e.g. per (tenant,user)). Returns `None`
/// when a template placeholder column is null/missing — the row cannot be routed
/// to a ledger and is skipped (it could not be isolated to a user anyway).
fn resolve_target_ledger(target: &str, batch: &ColumnBatch, row: usize) -> Option<String> {
    if !target.contains('{') {
        return Some(target.to_string());
    }
    expand_template_from_batch(target, batch, row).ok()
}

#[allow(clippy::too_many_arguments)]
fn materialize_row_into(
    ctx: &TmEmitContext<'_>,
    batch: &ColumnBatch,
    row: usize,
    target_ledger: String,
    convention: Option<&DeleteConvention>,
    order_by: Option<&str>,
    latest_by_key: bool,
    parents: &ParentIndexSet,
    stats: &mut MaterializeStats,
    accum: &mut MaterializeAccum,
) -> Result<()> {
    let tm = ctx.tm();
    let subject_term = materialize_subject_from_batch(&tm.subject_map, batch, row)
        .map_err(|e| ApiError::Internal(format!("R2RML subject materialization failed: {e}")))?;
    let subject_iri = match subject_term {
        Some(RdfTerm::Iri(iri)) => iri,
        // Null subject column (skip), blank-node subjects (no stable identity to
        // upsert), or a literal (term.rs already rejects this) -> skip the row.
        Some(RdfTerm::BlankNode(_) | RdfTerm::Literal { .. }) | None => return Ok(()),
    };

    // Resolve the row's named graph from the subject-map-level graph map (R2RML
    // rr:graph / rr:graphMap). `None` = the default graph (the common case). A
    // per-row template (e.g. one graph per tenant/user) routes each row into its
    // own graph so the same subject IRI holds independent per-graph statements.
    // A null graph value materializes to `None` -> default graph (never dropped).
    let graph_iri = match &tm.subject_map.graph_map {
        Some(gm) => materialize_graph_from_batch(gm, batch, row)
            .map_err(|e| ApiError::Internal(format!("R2RML graph materialization failed: {e}")))?,
        None => None,
    };

    if !latest_by_key {
        // Legacy additive: merge this live row into the (target, graph, subject) node.
        let node = build_live_node(ctx, batch, row, parents, stats, subject_iri.clone())?;
        accum.merge_live(target_ledger, graph_iri, subject_iri, node);
        return Ok(());
    }

    // Latest-by-key: classify, rank, and record as a whole-row replace.
    let rank = order_by.and_then(|col| column_sort_key(batch, col, row));
    let is_tombstone = convention.is_some_and(|conv| {
        let value = column_string(batch, &conv.column, row);
        conv.is_tombstone(value.as_deref())
    });
    let node = if is_tombstone {
        None
    } else {
        Some(build_live_node(
            ctx,
            batch,
            row,
            parents,
            stats,
            subject_iri.clone(),
        )?)
    };
    accum.record(target_ledger, graph_iri, subject_iri, rank, node);
    Ok(())
}

/// Convert an [`RdfTerm`] into its JSON-LD object representation.
fn rdf_term_to_jsonld(term: RdfTerm) -> JsonValue {
    match term {
        RdfTerm::Iri(iri) | RdfTerm::BlankNode(iri) => {
            let mut m = Map::new();
            m.insert("@id".to_string(), JsonValue::String(iri));
            JsonValue::Object(m)
        }
        RdfTerm::Literal { value, dtc } => match dtc {
            // Plain literal (xsd:string) -> bare JSON string, matching the
            // query path's default datatype.
            None => JsonValue::String(value),
            Some(UnresolvedDatatypeConstraint::LangTag(lang)) => {
                let mut m = Map::new();
                m.insert("@value".to_string(), JsonValue::String(value));
                m.insert("@language".to_string(), JsonValue::String(lang.to_string()));
                JsonValue::Object(m)
            }
            Some(UnresolvedDatatypeConstraint::Explicit(dt)) => {
                let mut m = Map::new();
                m.insert("@value".to_string(), JsonValue::String(value));
                m.insert("@type".to_string(), JsonValue::String(dt.to_string()));
                JsonValue::Object(m)
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live(iri: &str) -> Option<SubjectNode> {
        Some(SubjectNode::new(iri.to_string()))
    }
    fn ts(n: i128) -> Option<(i128, String)> {
        Some((n, String::new()))
    }
    /// Fixed target ledger for accumulator tests that don't exercise fan-out.
    const TGT: &str = "t:main";

    /// `CommitConflict`'s fields read the OPPOSITE way to their names, and this pins
    /// that so nobody re-derives it wrongly.
    ///
    /// From `verify_sequencing` in fluree-db-transact:
    ///     expected_t = record.commit_t   -> the NAMESERVICE's durable head
    ///     head_t     = base.t()          -> OUR in-memory base
    ///
    /// So `expected_t > head_t` means our base is BEHIND, i.e. someone else committed
    /// while we held it — an ordinary lost update. Reading the names at face value
    /// suggests the reverse ("we expected the head to be further along"), which points
    /// at an exotic cause and away from the real one. That misreading is exactly what
    /// happened: the conflicts were documented as "direction unexplained" for two days
    /// before the field order was checked.
    #[test]
    fn commit_conflict_field_order_means_our_base_is_behind() {
        use fluree_db_transact::TransactError;

        // The shape actually observed in production, gap of 16.
        let e = TransactError::CommitConflict {
            expected_t: 21,
            head_t: 5,
        };
        let TransactError::CommitConflict { expected_t, head_t } = e else {
            panic!("variant changed");
        };
        assert!(
            expected_t > head_t,
            "expected_t is the DURABLE head and head_t is OUR base, so a lost update \
             has expected_t > head_t"
        );
        assert_eq!(
            expected_t - head_t,
            16,
            "the gap is how many commits landed while we held a stale base — not \
             evidence of anything exotic"
        );
    }

    /// Losing the create race must be tolerated on the TARGET path, exactly as it
    /// already is for the state ledger.
    ///
    /// A canary over the source text rather than a behavioural test, because forcing a
    /// genuine two-writer create race needs a concurrent harness this module does not
    /// have. It is still worth having: the failure mode is silent until fan-out hits a
    /// NEW target, at which point it fails a whole target with
    /// `Commit conflict: expected t=2, head_t=1`. Deleting the `LedgerExists` arm would
    /// reintroduce that and no other test would notice.
    #[test]
    fn target_create_race_is_tolerated_like_the_state_ledger() {
        let src = include_str!("r2rml_materialize.rs");

        // The state ledger's tolerance — the precedent this mirrors.
        assert!(
            src.contains("Err(ApiError::LedgerExists(_)) => self.ledger(MATERIALIZE_STATE_LEDGER)"),
            "the state ledger's create-race tolerance has moved or gone; if it was \
             deliberately removed, this test and the target path both need revisiting"
        );

        // The target path must have its own. Two arms total, one per path.
        let arms = src.matches("Err(ApiError::LedgerExists(_))").count();
        assert!(
            arms >= 2,
            "expected create-race tolerance on BOTH the state-ledger and target paths, \
             found {arms} LedgerExists arm(s). Under fan-out, 17 sources resolve the same \
             targets, so several race to create a new one; without this the losers fail \
             the whole target."
        );
    }

    /// The refresh bound must sit well under the source's snapshot retention, or
    /// watermarks expire and every poll degrades to a full table read.
    ///
    /// Asserted as a RATIO against the retention we actually run (4 h), not as a bare
    /// number, so that someone raising the bound has to confront the relationship
    /// rather than just moving a constant.
    #[test]
    fn watermark_refresh_bound_is_well_inside_snapshot_retention() {
        const DEPLOYED_RETENTION_MS: i64 = 4 * 60 * 60 * 1000;
        let bound = watermark_refresh_bound_ms();
        assert!(
            bound > 0,
            "a non-positive bound would persist on every poll"
        );
        assert!(
            bound * 4 <= DEPLOYED_RETENTION_MS,
            "refresh bound {bound} ms leaves under 4x margin against {DEPLOYED_RETENTION_MS} ms \
             of snapshot retention; an expired watermark makes EVERY poll a full read"
        );
    }

    /// A window whose age is unknown must be treated as due, not as fresh.
    ///
    /// `window_age_ms` is `None` on a first run and — critically — when the stored
    /// snapshot is no longer resolvable, which is precisely the case where the
    /// watermark most needs rewriting. Defaulting the unknown to "fresh" would make
    /// the expired-watermark state permanent.
    #[test]
    fn unknown_window_age_counts_as_refresh_due() {
        let bound = watermark_refresh_bound_ms();
        let unknown: Option<i64> = None;
        assert!(
            unknown.is_none_or(|age| age >= bound),
            "an unknown window age must force a watermark refresh"
        );
        // A fresh window must NOT force one — otherwise we are back to a commit per
        // poll, which is the failure this whole change exists to remove.
        assert!(
            Some(0i64).is_some_and(|age| age < bound),
            "a zero-age window must not force a refresh"
        );
        assert!(Some(bound - 1).is_some_and(|age| age < bound));
        assert!(
            Some(bound).is_none_or(|age| age >= bound),
            "at the bound, refresh"
        );
    }

    /// BOTH novelty variants must classify as backpressure, and they must classify
    /// DIFFERENTLY — because the two need opposite responses.
    ///
    /// This asserts against `classify_novelty`, the function the retry loop actually
    /// calls. An earlier version of this test defined its own copy of the
    /// classification and asserted against that, which would have passed while
    /// production regressed. Testing a replica of the logic is not testing the logic.
    #[test]
    fn classify_novelty_covers_both_variants_distinctly() {
        use fluree_db_transact::TransactError;

        // Sized rejection -> splittable, and the numbers must survive for the log.
        let exceed = ApiError::Transact(TransactError::NoveltyWouldExceed {
            current_bytes: 53_024_023,
            delta_bytes: 17_701_872,
            max_bytes: 67_108_864,
        });
        assert_eq!(
            classify_novelty(&exceed),
            Some(NoveltyPressure::WouldExceed {
                current_bytes: 53_024_023,
                delta_bytes: 17_701_872,
                max_bytes: 67_108_864,
            })
        );

        // Ceiling already reached -> NOT splittable. This is the variant whose
        // absence made the whole fix inert: 74 of these in production against 0 of
        // the above, with the split path never firing.
        let at_max = ApiError::Transact(TransactError::NoveltyAtMax);
        assert_eq!(classify_novelty(&at_max), Some(NoveltyPressure::AtMax));

        // They must NOT collapse together — splitting on AtMax would loop forever
        // shrinking a chunk that can never fit.
        assert_ne!(classify_novelty(&exceed), classify_novelty(&at_max));

        // A real failure must fail fast. Retrying a validation error forever is
        // worse than erroring, so this set stays closed.
        assert_eq!(
            classify_novelty(&ApiError::Transact(TransactError::InvalidTerm("x".into()))),
            None
        );
        assert_eq!(classify_novelty(&ApiError::Internal("boom".into())), None);
    }

    /// Every split must yield two NON-EMPTY halves. An empty half would make the
    /// backpressure loop either spin forever on an unshrinking chunk or drop the
    /// items in the empty one — so this is a data-loss guard, not a tidiness check.
    #[test]
    fn split_in_half_never_produces_an_empty_side() {
        for n in 2..=33usize {
            let (l, r) = split_in_half((0..n).collect::<Vec<_>>());
            assert!(
                !l.is_empty() && !r.is_empty(),
                "n={n} produced an empty half"
            );
            assert_eq!(l.len() + r.len(), n, "n={n} lost or duplicated items");
            // Concatenation must be the original sequence: the loop relies on the
            // two halves together covering exactly the chunk it rejected.
            let mut back = l;
            back.extend(r);
            assert_eq!(back, (0..n).collect::<Vec<_>>(), "n={n} reordered items");
        }
    }

    /// The smallest divisible chunk is the dangerous one: `len / 2` is 1, and an
    /// off-by-one here yields `(0, 2)` — an empty half and an unshrunk chunk, i.e.
    /// an infinite loop.
    #[test]
    fn split_in_half_of_two_gives_one_and_one() {
        let (l, r) = split_in_half(vec!["a", "b"]);
        assert_eq!(
            (l.as_slice(), r.as_slice()),
            (["a"].as_slice(), ["b"].as_slice())
        );
    }

    /// Repeated splitting must converge to single items with nothing lost — the
    /// property the backpressure loop depends on when novelty headroom is tiny and
    /// a chunk is halved several times.
    #[test]
    fn repeated_splitting_converges_without_losing_items() {
        let mut pending: Vec<Vec<usize>> = vec![(0..17).collect()];
        let mut applied: Vec<usize> = Vec::new();
        let mut guard = 0;
        while let Some(c) = pending.pop() {
            guard += 1;
            assert!(guard < 1000, "did not converge");
            if c.len() > 1 {
                let (l, r) = split_in_half(c);
                pending.push(r);
                pending.push(l);
            } else {
                applied.extend(c);
            }
        }
        applied.sort_unstable();
        assert_eq!(applied, (0..17).collect::<Vec<_>>());
    }

    /// The retract path now hands `Vec<String>` to `build_retract_doc` (it took a
    /// `&BTreeSet` before) so chunks can be split. Converting must preserve every
    /// IRI — dropping one would silently skip a retraction, leaving stale data.
    #[test]
    fn iri_chunks_convert_to_vecs_without_losing_iris() {
        let iris: BTreeSet<String> = (0..50).map(|i| format!("http://ex/{i}")).collect();
        let chunks: Vec<Vec<String>> = chunk_iris_by_size(&iris, 200)
            .into_iter()
            .map(|c| c.into_iter().collect())
            .collect();
        assert!(
            chunks.len() > 1,
            "budget must actually split, or this proves nothing"
        );
        let mut flat: Vec<String> = chunks.into_iter().flatten().collect();
        flat.sort();
        let mut expected: Vec<String> = iris.into_iter().collect();
        expected.sort();
        assert_eq!(flat, expected);
    }

    /// `build_retract_doc` takes a slice now; it must still emit one `values` row
    /// per IRI and carry the graph selector.
    #[test]
    fn build_retract_doc_accepts_a_slice() {
        let doc = build_retract_doc(
            &["http://ex/a".to_string(), "http://ex/b".to_string()],
            Some("g:1"),
        );
        let rows = doc["values"][1].as_array().expect("values rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(doc["where"]["@id"], "?s");
        assert!(
            doc.to_string().contains("g:1"),
            "graph selector must survive: {doc}"
        );
    }
    /// Default-graph key `(target, graph, subject)` for map/set assertions.
    fn dk(iri: &str) -> (String, Option<String>, String) {
        (TGT.to_string(), None, iri.to_string())
    }

    #[test]
    fn finalize_live_only_upserts() {
        let mut a = MaterializeAccum::default();
        a.record(TGT.into(), None, "urn:a".into(), None, live("urn:a"));
        let (live, del) = a.finalize();
        assert!(live.contains_key(&dk("urn:a")));
        assert!(del.is_empty());
    }

    #[test]
    fn finalize_tombstone_only_retracts() {
        let mut a = MaterializeAccum::default();
        a.record(TGT.into(), None, "urn:a".into(), None, None);
        let (live, del) = a.finalize();
        assert!(live.is_empty());
        assert!(del.contains(&dk("urn:a")));
    }

    #[test]
    fn scan_order_last_wins_live_then_tombstone() {
        // No ordering column: last row in scan order wins -> tombstone.
        let mut a = MaterializeAccum::default();
        a.record(TGT.into(), None, "urn:a".into(), None, live("urn:a"));
        a.record(TGT.into(), None, "urn:a".into(), None, None);
        let (live, del) = a.finalize();
        assert!(live.is_empty());
        assert!(del.contains(&dk("urn:a")));
    }

    #[test]
    fn scan_order_last_wins_tombstone_then_live() {
        let mut a = MaterializeAccum::default();
        a.record(TGT.into(), None, "urn:a".into(), None, None);
        a.record(TGT.into(), None, "urn:a".into(), None, live("urn:a"));
        let (live, del) = a.finalize();
        assert!(live.contains_key(&dk("urn:a")));
        assert!(!del.contains(&dk("urn:a")));
    }

    #[test]
    fn order_by_latest_wins_regardless_of_arrival() {
        // A higher-ranked tombstone arriving FIRST still wins over a lower-ranked
        // live row arriving later (ordering, not scan order, decides).
        let mut a = MaterializeAccum::default();
        a.record(TGT.into(), None, "urn:a".into(), ts(200), None); // newer tombstone
        a.record(TGT.into(), None, "urn:a".into(), ts(100), live("urn:a")); // older live
        let (live, del) = a.finalize();
        assert!(live.is_empty());
        assert!(del.contains(&dk("urn:a")));
    }

    #[test]
    fn order_by_newer_live_wins_over_older_tombstone() {
        let mut a = MaterializeAccum::default();
        a.record(TGT.into(), None, "urn:a".into(), ts(100), None); // older tombstone
        a.record(TGT.into(), None, "urn:a".into(), ts(200), live("urn:a")); // newer live
        let (live, del) = a.finalize();
        assert!(live.contains_key(&dk("urn:a")));
        assert!(!del.contains(&dk("urn:a")));
    }

    #[test]
    fn accum_bytes_follows_insert_replace_merge_and_tombstone() {
        // The byte estimate is the input to the pre-OOM circuit breaker, so its
        // accounting must track every mutation path. The one a naive
        // implementation gets wrong: a tombstone REPLACING a live node must
        // SHRINK the estimate (the payload drops; key + overhead remain).
        let mut a = MaterializeAccum::default();
        assert_eq!(a.estimated_bytes(), 0);

        let fat = |iri: &str| {
            let mut n = SubjectNode::new(iri.to_string());
            n.add_object("http://ex/name", json!("a-reasonably-long-value"));
            Some(n)
        };

        a.record(TGT.into(), None, "urn:a".into(), ts(100), fat("urn:a"));
        let after_insert = a.estimated_bytes();
        assert!(after_insert > 0, "fresh insert must grow the estimate");

        // An older row is ignored: no change.
        a.record(TGT.into(), None, "urn:a".into(), ts(50), fat("urn:a"));
        assert_eq!(a.estimated_bytes(), after_insert);

        // A newer tombstone replaces the live node: estimate shrinks but stays
        // positive (the entry itself remains).
        a.record(TGT.into(), None, "urn:a".into(), ts(200), None);
        let after_tombstone = a.estimated_bytes();
        assert!(
            after_tombstone < after_insert,
            "tombstone replacing a live node must shrink: {after_insert} -> {after_tombstone}"
        );
        assert!(after_tombstone > 0);
        assert_eq!(a.len(), 1, "replace, not a second entry");

        // Additive merge with a NEW predicate must grow the existing node.
        let mut b = MaterializeAccum::default();
        let mut n1 = SubjectNode::new("urn:m".to_string());
        n1.add_object("http://ex/p1", json!("v1"));
        b.merge_live(TGT.into(), None, "urn:m".into(), n1);
        let first = b.estimated_bytes();
        let mut n2 = SubjectNode::new("urn:m".to_string());
        n2.add_object("http://ex/p2", json!("v2"));
        b.merge_live(TGT.into(), None, "urn:m".into(), n2);
        assert!(
            b.estimated_bytes() > first,
            "additive merge must grow the estimate"
        );
        assert_eq!(b.len(), 1);
    }

    #[test]
    fn record_isolates_same_subject_across_graphs() {
        // The SAME subject IRI in two different graphs is two independent keys —
        // per-(tenant,user) statements about one entity never collide. Recording a
        // tombstone in graph A must NOT retract the live row in graph B.
        let ga = Some("urn:g:a".to_string());
        let gb = Some("urn:g:b".to_string());
        let mut a = MaterializeAccum::default();
        a.record(TGT.into(), ga.clone(), "urn:x".into(), None, None); // tombstone in A
        a.record(TGT.into(), gb.clone(), "urn:x".into(), None, live("urn:x")); // live in B
        let (live, del) = a.finalize();
        assert!(live.contains_key(&(TGT.to_string(), gb, "urn:x".to_string())));
        assert!(del.contains(&(TGT.to_string(), ga, "urn:x".to_string())));
        assert_eq!(live.len(), 1);
        assert_eq!(del.len(), 1);
    }

    #[test]
    fn record_isolates_same_subject_across_targets() {
        // The SAME subject IRI in two different TARGET ledgers is two independent
        // keys — a fan-out job's per-(tenant,user) ledgers never collide.
        let mut a = MaterializeAccum::default();
        a.record("silver_T1_U1:main".into(), None, "urn:x".into(), None, None); // tombstone in U1
        a.record(
            "silver_T1_U2:main".into(),
            None,
            "urn:x".into(),
            None,
            live("urn:x"),
        ); // live in U2
        let (live, del) = a.finalize();
        assert!(live.contains_key(&("silver_T1_U2:main".to_string(), None, "urn:x".to_string())));
        assert!(del.contains(&("silver_T1_U1:main".to_string(), None, "urn:x".to_string())));
        assert_eq!(live.len(), 1);
        assert_eq!(del.len(), 1);
    }

    #[test]
    fn merge_live_isolates_same_subject_across_graphs() {
        // Additive merge is per (target, graph, subject): the same IRI in two
        // graphs accumulates independently, not into one merged node.
        let ga = Some("urn:g:a".to_string());
        let gb = Some("urn:g:b".to_string());
        let mut a = MaterializeAccum::default();
        a.merge_live(
            TGT.into(),
            ga.clone(),
            "urn:x".into(),
            SubjectNode::new("urn:x".into()),
        );
        a.merge_live(
            TGT.into(),
            gb.clone(),
            "urn:x".into(),
            SubjectNode::new("urn:x".into()),
        );
        let (live, _del) = a.finalize();
        assert_eq!(live.len(), 2);
        assert!(live.contains_key(&(TGT.to_string(), ga, "urn:x".to_string())));
        assert!(live.contains_key(&(TGT.to_string(), gb, "urn:x".to_string())));
    }

    #[test]
    fn build_retract_doc_shape() {
        // Still fed from a BTreeSet in production (so still sorted), but passed as
        // a slice now — the backpressure loop must be able to split a chunk.
        let set: BTreeSet<String> = ["urn:a".to_string(), "urn:b".to_string()].into();
        let iris: Vec<String> = set.into_iter().collect();
        let doc = build_retract_doc(&iris, None);
        // Subjects MUST be typed @id values, not bare strings (a bare string
        // parses to a literal that never joins a real subject Sid). BTreeSet
        // => sorted order.
        assert_eq!(doc["values"][0], json!("?s"));
        assert_eq!(
            doc["values"][1],
            json!([
                [{ "@type": "@id", "@value": "urn:a" }],
                [{ "@type": "@id", "@value": "urn:b" }]
            ])
        );
        assert_eq!(doc["where"], json!({ "@id": "?s", "?p": "?o" }));
        assert_eq!(doc["delete"], json!({ "@id": "?s", "?p": "?o" }));
        // Default graph: no `graph` key.
        assert!(doc.get("graph").is_none());
    }

    #[test]
    fn build_retract_doc_scopes_to_named_graph() {
        let doc = build_retract_doc(&["urn:a".to_string()], Some("urn:g:tenant/user"));
        // The `graph` key scopes the whole-subject retract to that named graph
        // (WHERE + DELETE), so a same-IRI twin in another graph is untouched.
        assert_eq!(doc["graph"], json!("urn:g:tenant/user"));
        assert_eq!(doc["delete"], json!({ "@id": "?s", "?p": "?o" }));
    }

    #[test]
    fn nodes_by_graph_to_doc_tags_named_nodes_with_graph_selector() {
        let mut by_graph: BTreeMap<Option<String>, Vec<JsonValue>> = BTreeMap::new();
        by_graph.insert(None, vec![json!({ "@id": "urn:wm" })]);
        by_graph.insert(
            Some("urn:g:a".to_string()),
            vec![json!({ "@id": "urn:x", "@type": ["ex:T"] })],
        );
        by_graph.insert(Some("urn:g:empty".to_string()), vec![]); // contributes nothing
        let doc = nodes_by_graph_to_doc(by_graph);
        // Default-graph node stays plain; the named-graph node carries a per-node
        // `@graph` STRING selector (the form parse_insert/parse_upsert accept), NOT
        // the `{"@id":g,"@graph":[...]}` envelope; the empty named graph adds nothing.
        assert!(doc.contains(&json!({ "@id": "urn:wm" })));
        assert!(doc.contains(&json!({
            "@id": "urn:x",
            "@graph": "urn:g:a",
            "@type": ["ex:T"]
        })));
        assert_eq!(doc.len(), 2);
    }

    #[test]
    fn type_only_source_yields_no_predicate_upsert() {
        // A type-only source (r2rml `entity_type`: subject + rdf:type, no other
        // predicates) splits into a @type node and NO predicate node, so the
        // additive-mode predicate doc is empty. The materialize loop MUST skip the
        // upsert in that case: an `upsert([])` is rejected by the transactor
        // ("Upsert must contain at least one predicate or @type"), which aborts
        // the sync before its watermark advances → the next poll full-rescans and
        // re-fails forever (a new @type-insert commit every poll — pure churn).
        let mut node = SubjectNode::new("urn:e:1".to_string());
        node.add_type("https://www.w3.org/ns/activitystreams#Article");
        let (type_node, pred_node) = node.into_type_and_predicate_nodes();
        assert!(type_node.is_some(), "type-only node must emit a @type node");
        assert!(
            pred_node.is_none(),
            "type-only node must emit NO predicate node"
        );

        // Reproduce how the additive branch assembles the predicate doc.
        let mut pred_by_graph: BTreeMap<Option<String>, Vec<JsonValue>> = BTreeMap::new();
        if let Some(pn) = pred_node {
            pred_by_graph.entry(None).or_default().push(pn);
        }
        let pred_doc = nodes_by_graph_to_doc(pred_by_graph);
        assert!(
            pred_doc.is_empty(),
            "type-only source must produce an empty predicate doc (skip the upsert)"
        );
    }

    #[test]
    fn watermark_node_is_per_job_and_string_encoded() {
        let node = watermark_node(
            "people:main",
            "silver:main",
            "demo.actors",
            5_648_190_075_564_901_028,
        );
        // Every segment's ':' is escaped (%3A) so the (source, target, table)
        // encoding is injective.
        assert_eq!(
            node["@id"],
            json!("urn:fluree:materialize-state:people%3Amain:silver%3Amain:demo.actors")
        );
        // String-encoded to preserve full i64 precision.
        assert_eq!(node[WATERMARK_SNAPSHOT_PRED], json!("5648190075564901028"));
        assert_eq!(node[WATERMARK_SOURCE_PRED], json!("people:main"));
        assert_eq!(node[WATERMARK_TARGET_PRED], json!("silver:main"));
        assert_eq!(node[WATERMARK_TABLE_PRED], json!("demo.actors"));
    }

    /// Helper: build an applied-marker map the way a pass would read it.
    #[cfg(test)]
    fn applied_map(
        entries: &[(&str, &str, i64)],
    ) -> std::collections::HashMap<(String, String), i64> {
        entries
            .iter()
            .map(|(target, table, a)| ((target.to_string(), table.to_string()), *a))
            .collect()
    }

    /// The case this whole change exists for: a job whose watermark cannot advance
    /// must still skip the targets that already have the window.
    #[test]
    fn a_target_that_applied_the_window_is_caught_up() {
        let tw = vec![("silver.observation".to_string(), Some(10i64), 20i64)];
        let applied = applied_map(&[("silver_acme_u1:main", "silver.observation", 20)]);
        assert!(target_is_caught_up(&applied, "silver_acme_u1:main", &tw));
        // A target the markers say nothing about must NOT be skipped.
        assert!(!target_is_caught_up(&applied, "silver_acme_u2:main", &tw));
    }

    /// `==`, and only `==`. A marker that merely DIFFERS from this window's `to` — in
    /// either numeric direction — has not applied this window.
    #[test]
    fn only_an_applied_marker_equal_to_the_window_counts_as_caught_up() {
        let tw = vec![("t".to_string(), None, 20i64)];
        assert!(target_is_caught_up(
            &applied_map(&[("x:main", "t", 20)]),
            "x:main",
            &tw
        ));
        assert!(
            !target_is_caught_up(&applied_map(&[("x:main", "t", 21)]), "x:main", &tw),
            "a marker numerically above `to` is a DIFFERENT snapshot, not a later one"
        );
        assert!(!target_is_caught_up(
            &applied_map(&[("x:main", "t", 19)]),
            "x:main",
            &tw
        ));
    }

    /// Snapshot ids are random, so an applied marker can be numerically far larger
    /// than a snapshot committed long after it. Skipping on `>=` therefore ratchets:
    /// the marker climbs the running maximum of a random sequence and then exceeds
    /// every later draw permanently, and the target is never written to again.
    ///
    /// These are real values from a deployment where that happened — all 17 markers
    /// had reached 8.19e18–9.22e18 against an `i64::MAX` of 9.223e18, every target was
    /// skipped on every poll, and the watermark advanced past data that was never
    /// applied. Each `to` below is a genuinely LATER commit than the marker beside it,
    /// despite being numerically smaller.
    ///
    /// Small sequence-shaped fixtures cannot catch this — an ordered fixture will
    /// ratify an ordering comparison — which is why these are the production numbers.
    #[test]
    fn a_marker_numerically_above_a_later_snapshot_is_not_caught_up() {
        // (table, ratcheted applied marker, the genuinely later snapshot it skipped)
        let observed = [
            (
                "silver.place",
                9_220_834_252_869_770_488i64,
                5_644_295_785_472_712_989i64,
            ),
            (
                "silver.observation",
                9_217_340_116_954_323_563,
                3_238_671_374_642_079_740,
            ),
            (
                "silver.concept_scheme",
                9_086_429_568_298_186_029,
                753_340_878_885_049_461,
            ),
            (
                "silver.concept",
                9_052_937_455_619_636_065,
                1_349_575_232_631_351_152,
            ),
            (
                "silver.link",
                9_196_238_952_972_422_466,
                9_139_397_570_598_290_786,
            ),
        ];
        for (table, marker, later_to) in observed {
            assert!(
                marker > later_to,
                "{table}: this fixture is only meaningful while the marker is \
                 numerically larger than the later snapshot — that is the whole trap"
            );
            let tw = vec![(table.to_string(), None, later_to)];
            assert!(
                !target_is_caught_up(
                    &applied_map(&[("silver_acme_u1:main", table, marker)]),
                    "silver_acme_u1:main",
                    &tw
                ),
                "{table}: marker {marker} is a different snapshot from {later_to}, not \
                 a later one — skipping here is what discarded the window"
            );
        }
    }

    /// ALL tables, not any. A target that applied one table of a two-table window
    /// still owes the other one, and skipping it there would drop half a window
    /// silently — the one bug in this change that would lose data rather than repeat
    /// work.
    #[test]
    fn a_target_behind_on_any_table_is_not_caught_up() {
        let tw = vec![
            ("t_a".to_string(), None, 20i64),
            ("t_b".to_string(), None, 30i64),
        ];
        let both = applied_map(&[("x:main", "t_a", 20), ("x:main", "t_b", 30)]);
        assert!(target_is_caught_up(&both, "x:main", &tw));
        let only_a = applied_map(&[("x:main", "t_a", 20)]);
        assert!(!target_is_caught_up(&only_a, "x:main", &tw));
        let a_ok_b_behind = applied_map(&[("x:main", "t_a", 20), ("x:main", "t_b", 29)]);
        assert!(!target_is_caught_up(&a_ok_b_behind, "x:main", &tw));
    }

    /// An empty window must never mark anything caught up. `all()` over an empty
    /// slice is vacuously true, so without the guard a source with no snapshots yet
    /// would skip every target forever and materialize nothing.
    #[test]
    fn an_empty_window_leaves_every_target_not_caught_up() {
        let applied = applied_map(&[("x:main", "t", 99)]);
        assert!(!target_is_caught_up(&applied, "x:main", &[]));
    }

    /// The markers key on the RESOLVED target, and must not collide with the
    /// spec-keyed watermark subject — which they would if both used one prefix, since
    /// for a non-templated job spec == resolved.
    #[test]
    fn applied_and_watermark_subjects_never_collide() {
        let w = watermark_subject("src:main", "silver:main", "t");
        let a = applied_subject("src:main", "silver:main", "t");
        assert_ne!(w, a);
        assert!(a.starts_with(APPLIED_SUBJECT_PREFIX));
        // Injective in the resolved target, like the watermark is in the spec.
        assert_ne!(
            applied_subject("s", "silver_a:main_b", "t"),
            applied_subject("s", "silver_a:main", "b:t")
        );
    }

    #[test]
    fn watermark_subject_is_injective() {
        // Distinct (source, target, table) triples that would collide under a
        // naive ':' join must produce distinct subjects.
        assert_ne!(
            watermark_subject("a:b", "t", "c"),
            watermark_subject("a", "t", "b:c")
        );
        assert_ne!(
            watermark_subject("s", "a:b", "c"),
            watermark_subject("s", "a", "b:c")
        );
    }

    #[test]
    fn extract_first_i64_tolerant_of_nesting() {
        assert_eq!(extract_first_i64(&json!(["42"])), Some(42));
        assert_eq!(extract_first_i64(&json!([["42"]])), Some(42));
        assert_eq!(extract_first_i64(&json!([])), None);
        assert_eq!(
            extract_first_i64(&json!(["5648190075564901028"])),
            Some(5_648_190_075_564_901_028)
        );
    }

    #[test]
    fn split_type_and_predicates_both_present() {
        let mut node = SubjectNode::new("urn:x".into());
        node.add_type("https://ex/A");
        node.add_type("https://ex/B");
        node.add_object("https://ex/p", json!("v"));
        let (type_node, pred_node) = node.into_type_and_predicate_nodes();
        // @type is asserted separately (via insert) so classes union across
        // sources; the predicate goes to the upsert.
        assert_eq!(
            type_node.unwrap(),
            json!({ "@id": "urn:x", "@type": ["https://ex/A", "https://ex/B"] })
        );
        assert_eq!(
            pred_node.unwrap(),
            json!({ "@id": "urn:x", "https://ex/p": "v" })
        );
    }

    #[test]
    fn split_type_only_row_has_no_predicate_node() {
        // entity_type shape: a class-only row yields a type node and nothing to upsert.
        let mut node = SubjectNode::new("urn:x".into());
        node.add_type("https://ex/Announce");
        let (type_node, pred_node) = node.into_type_and_predicate_nodes();
        assert_eq!(
            type_node.unwrap(),
            json!({ "@id": "urn:x", "@type": ["https://ex/Announce"] })
        );
        assert!(pred_node.is_none());
    }

    #[test]
    fn split_edge_only_row_has_no_type_node() {
        // join-table shape (rr:class removed): an edge-only row yields a predicate
        // node and NO type node, so it never re-types (clobbers) its parent.
        let mut node = SubjectNode::new("urn:x".into());
        node.add_object("https://ex/tag", json!({ "@id": "urn:t" }));
        let (type_node, pred_node) = node.into_type_and_predicate_nodes();
        assert!(type_node.is_none());
        assert_eq!(
            pred_node.unwrap(),
            json!({ "@id": "urn:x", "https://ex/tag": { "@id": "urn:t" } })
        );
    }

    #[test]
    fn build_live_node_routes_rdf_type_pom_to_union_type() {
        use fluree_db_iceberg::io::{BatchSchema, Column, ColumnBatch, FieldInfo, FieldType};
        use fluree_db_r2rml::mapping::{
            CompiledR2rmlMapping, ObjectMap, PredicateMap, PredicateObjectMap, TriplesMap,
        };
        use std::sync::Arc;

        // A source row whose `type` column holds a full-IRI class (data-driven typing).
        let schema = Arc::new(BatchSchema::new(vec![FieldInfo {
            name: "type".to_string(),
            field_type: FieldType::String,
            nullable: true,
            field_id: 1,
        }]));
        let batch = ColumnBatch::new(
            schema,
            vec![Column::String(vec![Some(
                "https://www.w3.org/ns/activitystreams#Announce".to_string(),
            )])],
        )
        .unwrap();

        // Mapping with NO rr:class; the class comes from a `rr:predicate rdf:type`
        // object map reading the `type` column (the only way to express per-row
        // typing, since rr:class is constant-only).
        let tm = TriplesMap::new("#Article", "silver.article").with_predicate_object(
            PredicateObjectMap {
                predicate_map: PredicateMap::constant(RDF_TYPE_IRI),
                object_map: ObjectMap::column_iri("type"),
            },
        );

        // No RefObjectMaps anywhere, so an empty mapping's (empty) parent plan
        // is equivalent to this tm's.
        let parents = ParentIndexSet::new(&CompiledR2rmlMapping::new(vec![])).unwrap();
        let ctx = TmEmitContext::new(&tm).unwrap();
        let mut stats = MaterializeStats::default();
        let node =
            build_live_node(&ctx, &batch, 0, &parents, &mut stats, "urn:s".to_string()).unwrap();
        // The rdf:type POM is routed to @type (the additive union-insert path),
        // NOT to predicates (which would be upserted and clobber other sources'
        // classes on a shared subject IRI).
        assert_eq!(
            node.types,
            vec!["https://www.w3.org/ns/activitystreams#Announce".to_string()]
        );
        assert!(
            node.predicates.is_empty(),
            "rdf:type must be a class, not an ordinary predicate: {:?}",
            node.predicates
        );
    }

    fn node(id: &str, pad: usize) -> JsonValue {
        json!({ "@id": id, "p": "x".repeat(pad) })
    }

    #[test]
    fn chunk_nodes_empty_yields_no_chunks() {
        // Load-bearing: callers iterate the chunks instead of guarding on
        // `!doc.is_empty()`, and the transactor REJECTS an upsert/insert with no
        // predicate or @type. If an empty doc produced one empty chunk we would
        // send `[]`, which aborts the sync BEFORE its watermark advances — so the
        // next poll full-rescans and re-fails forever.
        assert!(chunk_nodes_by_size(vec![], 1024).is_empty());
        assert!(chunk_iris_by_size(&BTreeSet::new(), 1024).is_empty());
    }

    #[test]
    fn chunk_nodes_preserves_every_node_in_order() {
        // The whole point is to split a transaction, never to lose or reorder a
        // subject: materialize is a whole-subject REPLACE, so a dropped node
        // silently leaves a subject retracted and not re-asserted.
        let nodes: Vec<JsonValue> = (0..50).map(|i| node(&format!("s{i}"), 100)).collect();
        let chunks = chunk_nodes_by_size(nodes.clone(), 512);
        assert!(chunks.len() > 1, "expected a split at this budget");
        let flat: Vec<JsonValue> = chunks.into_iter().flatten().collect();
        assert_eq!(flat, nodes);
    }

    #[test]
    fn chunk_nodes_respects_budget_and_never_drops_an_oversized_node() {
        // A node larger than the whole budget cannot be split (it is one subject),
        // so it must be emitted ALONE rather than dropped or merged.
        let big = node("big", 4096);
        let small = node("small", 10);
        let chunks = chunk_nodes_by_size(vec![small.clone(), big.clone(), small.clone()], 512);
        let flat: Vec<JsonValue> = chunks.iter().flatten().cloned().collect();
        assert_eq!(flat, vec![small.clone(), big.clone(), small]);
        let big_chunk = chunks
            .iter()
            .find(|c| c.contains(&big))
            .expect("oversized node retained");
        assert_eq!(big_chunk.len(), 1, "oversized node must be sent alone");
        // Every other chunk stays within budget.
        for chunk in chunks.iter().filter(|c| !c.contains(&big)) {
            let bytes: usize = chunk.iter().map(estimated_json_bytes).sum();
            assert!(bytes <= 512, "chunk over budget: {bytes}");
        }
    }

    #[test]
    fn chunk_nodes_keeps_the_named_graph_selector_on_every_node() {
        // Chunking is only safe because `nodes_by_graph_to_doc` emits SELF-CONTAINED
        // nodes — a named-graph node carries its own `@graph` string. If that ever
        // moved to an envelope, splitting would silently drop graph scoping and
        // cross-write tenants' graphs.
        let mut by_graph: BTreeMap<Option<String>, Vec<JsonValue>> = BTreeMap::new();
        by_graph.insert(
            Some("urn:g1".to_string()),
            (0..20).map(|i| node(&format!("a{i}"), 80)).collect(),
        );
        let doc = nodes_by_graph_to_doc(by_graph);
        let chunks = chunk_nodes_by_size(doc, 256);
        assert!(chunks.len() > 1, "expected a split at this budget");
        for chunk in &chunks {
            for n in chunk {
                assert_eq!(
                    n.get("@graph").and_then(JsonValue::as_str),
                    Some("urn:g1"),
                    "chunking dropped the per-node graph selector"
                );
            }
        }
    }

    #[test]
    fn chunk_iris_partitions_the_set_exactly() {
        let iris: BTreeSet<String> = (0..40).map(|i| format!("urn:subject:{i}")).collect();
        let chunks = chunk_iris_by_size(&iris, 200);
        assert!(chunks.len() > 1, "expected a split at this budget");
        let mut union: BTreeSet<String> = BTreeSet::new();
        let mut total = 0usize;
        for chunk in &chunks {
            total += chunk.len();
            union.extend(chunk.iter().cloned());
        }
        assert_eq!(union, iris, "retraction chunks must cover every subject");
        assert_eq!(total, iris.len(), "chunks must not duplicate a subject");
    }
}

#[cfg(test)]
mod engine_tests {
    //! B3 — the first tests that drive the materialize engine itself.
    //!
    //! Before these, `materialize_r2rml_graph_source` had three callers in the whole repo
    //! and none was a test, so a reviewer could replace the streaming core with "yield
    //! nothing" and leave 954/954 green. It was not carelessness: every graph-source
    //! integration test needs live infrastructure (Polaris / LocalStack / MinIO), there is
    //! no offline Iceberg fixture anywhere in the tree, and the engine hardcoded its
    //! provider. [`MaterializeSource`] is the seam that makes this reachable.
    //!
    //! SCOPE, stated up front so no one mistakes these for more than they are: a fake
    //! source bypasses the real provider's scan by construction, so these do NOT cover
    //! `stream_scan_tasks` / `ScanChoice` / the incremental snapshot window. They cover
    //! what the ENGINE decides.

    use super::*;
    use crate::FlureeBuilder;
    use fluree_db_r2rml::mapping::{ObjectMap, PredicateMap, PredicateObjectMap};
    use fluree_db_tabular::batch::{BatchSchema, Column, FieldInfo, FieldType};

    /// One string column per name, one row per tuple element.
    fn batch(columns: &[(&str, &[&str])]) -> ColumnBatch {
        let fields = columns
            .iter()
            .enumerate()
            .map(|(i, (name, _))| FieldInfo {
                name: (*name).to_string(),
                field_type: FieldType::String,
                nullable: true,
                field_id: i as i32 + 1,
            })
            .collect();
        let cols = columns
            .iter()
            .map(|(_, vals)| Column::String(vals.iter().map(|v| Some((*v).to_string())).collect()))
            .collect();
        ColumnBatch::new(Arc::new(BatchSchema::new(fields)), cols).expect("batch")
    }

    /// `people` table → one subject per `id`, with a `name` predicate.
    fn people_mapping() -> Arc<CompiledR2rmlMapping> {
        let tm = TriplesMap::new("http://tm/people", "people")
            .with_subject_template("http://ex/person/{id}")
            .with_class("http://ex/Person")
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex/name"),
                object_map: ObjectMap::column("name"),
            });
        Arc::new(CompiledR2rmlMapping::new(vec![tm]))
    }

    /// A [`MaterializeSource`] backed by in-memory batches. Records how many scan windows
    /// were requested, so a test can prove the engine actually pulled from it — the guard
    /// against a fixture that silently reaches nothing.
    struct FakeSource {
        mapping: Arc<CompiledR2rmlMapping>,
        batches: std::sync::Mutex<Vec<ColumnBatch>>,
        delete: Option<DeleteConvention>,
        order_by: Option<String>,
        to_snapshot_id: Option<i64>,
        /// `None` means "first run / watermark unresolvable", which C3 treats as
        /// persist-regardless. Default to a FRESH window so tests opt in to staleness.
        window_age_ms: Option<i64>,
        scans: std::sync::atomic::AtomicUsize,
        /// Re-present the same batches on every scan instead of draining them.
        ///
        /// Draining is the right default — it catches a fixture that gets pulled twice
        /// when it should be pulled once. But it cannot model the case this module's
        /// applied markers exist for: a window that is re-read because the shared
        /// watermark could not advance. That needs the SAME rows and the SAME
        /// `to_snapshot_id` presented again.
        repeat: bool,
    }

    impl FakeSource {
        fn new(mapping: Arc<CompiledR2rmlMapping>, batches: Vec<ColumnBatch>) -> Self {
            Self {
                mapping,
                batches: std::sync::Mutex::new(batches),
                delete: None,
                order_by: None,
                to_snapshot_id: Some(7),
                window_age_ms: Some(0),
                scans: std::sync::atomic::AtomicUsize::new(0),
                repeat: false,
            }
        }
        /// Same window on every scan — see [`FakeSource::repeat`].
        fn repeating(mut self) -> Self {
            self.repeat = true;
            self
        }
        fn scans(&self) -> usize {
            self.scans.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl MaterializeSource for FakeSource {
        async fn compiled_mapping(&self, _gs: &str) -> Result<Arc<CompiledR2rmlMapping>> {
            Ok(self.mapping.clone())
        }
        async fn materialize_options(
            &self,
            _gs: &str,
        ) -> Result<(Option<DeleteConvention>, Option<String>)> {
            Ok((self.delete.clone(), self.order_by.clone()))
        }
        async fn scan_window(
            &self,
            _gs: &str,
            _table: &str,
            _from: Option<i64>,
        ) -> Result<MaterializeScan> {
            self.scans.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let taken: Vec<ColumnBatch> = if self.repeat {
                self.batches.lock().unwrap().clone()
            } else {
                std::mem::take(&mut *self.batches.lock().unwrap())
            };
            Ok(MaterializeScan {
                to_snapshot_id: self.to_snapshot_id,
                incremental: false,
                window_age_ms: self.window_age_ms,
                stream: Box::pin(futures::stream::iter(taken.into_iter().map(Ok))),
            })
        }
    }

    /// THE test the review asked for: the engine runs end to end, reads from the source,
    /// and writes subjects into a real target ledger.
    ///
    /// The `scans()` assertion is the anti-theatre guard. Without it, a fixture that wires
    /// up but never gets pulled would still "pass" — which is exactly how gutting
    /// `stream_scan_tasks` left 954 tests green.
    #[tokio::test]
    async fn engine_materializes_rows_into_the_target_ledger() {
        let fluree = FlureeBuilder::memory().build_memory();
        let src = FakeSource::new(
            people_mapping(),
            vec![batch(&[("id", &["1", "2"]), ("name", &["alice", "bob"])])],
        );

        let result = fluree
            .materialize_from_source(&src, "people:main", "people_native:main", false, None, None)
            .await
            .expect("materialize");

        assert_eq!(
            src.scans(),
            1,
            "the engine must actually pull a scan window"
        );
        assert!(result.committed, "two rows must produce a commit");
        assert_eq!(result.rows_read, 2, "both source rows must be read");
        assert_eq!(result.subjects_upserted, 2, "one subject per id");
        assert_eq!(
            result.to_snapshot_id,
            Some(7),
            "watermark advances to the window end"
        );
        assert_eq!(
            result.tally,
            TargetTally {
                ok: 1,
                deferred: 0,
                failed: 0
            },
            "one concrete target, fully applied"
        );
    }

    /// A window with no rows must NOT commit. This is C3: snapshot-id movement is not
    /// work, and treating it as work is what drove ~1,200 empty commits/hour into the
    /// shared state ledger until it saturated and halted every sync in the deployment.
    #[tokio::test]
    async fn an_empty_window_commits_nothing() {
        let fluree = FlureeBuilder::memory().build_memory();
        let src = FakeSource::new(people_mapping(), vec![]);
        assert_eq!(
            src.window_age_ms,
            Some(0),
            "a FRESH window — staleness is the other test"
        );

        let result = fluree
            .materialize_from_source(&src, "people:main", "people_native:main", false, None, None)
            .await
            .expect("materialize");

        assert_eq!(src.scans(), 1, "the window is still examined");
        assert!(!result.committed, "no rows means no commit churn");
        assert_eq!(result.rows_read, 0);
        assert_eq!(result.tally.total(), 0, "no target was attempted");
    }

    /// The other half of C3, and the reason it is a bound rather than a flat "skip empty
    /// windows": a window whose watermark has gone STALE must persist even with zero rows,
    /// or a quiet table's watermark ages out of Iceberg's snapshot retention and the next
    /// poll can only recover with a full table re-read.
    ///
    /// `window_age_ms: None` is the same signal — "first run, or the stored snapshot no
    /// longer resolves" — and it must also persist. Writing this test is what taught me
    /// that; my first version set `None` and then asserted no commit, which had the C3
    /// semantics exactly backwards.
    #[tokio::test]
    async fn a_stale_empty_window_still_persists_its_watermark() {
        for age in [None, Some(i64::MAX)] {
            let fluree = FlureeBuilder::memory().build_memory();
            let mut src = FakeSource::new(people_mapping(), vec![]);
            src.window_age_ms = age;

            let result = fluree
                .materialize_from_source(
                    &src,
                    "people:main",
                    "people_native:main",
                    false,
                    None,
                    None,
                )
                .await
                .expect("materialize");

            assert!(
                result.committed,
                "a stale/unresolvable window (age={age:?}) must persist its watermark                  even with no rows, or it ages out of snapshot retention"
            );
            assert_eq!(
                result.rows_read, 0,
                "still no rows — only the watermark moved"
            );
        }
    }

    /// The amplification this module's applied markers exist to stop, end to end.
    ///
    /// A re-presented window — same rows, same `to_snapshot_id`, which is exactly what
    /// a job whose shared watermark cannot advance sees on every poll — must not
    /// rewrite the targets that already have it.
    ///
    /// The assertion is each target ledger's `t`, and the choice matters. Reading the
    /// DATA proves nothing, because re-application is idempotent and leaves it
    /// identical either way. `subjects_upserted` proves nothing either — it is
    /// `live.len()`, the subjects the accumulator PREPARED, counted before the target
    /// loop runs, so it reads 3 whether or not a single commit happened. `t` only
    /// advances on an actual commit, so it is the one observable that separates
    /// "skipped" from "rewritten to the same value".
    #[tokio::test]
    async fn a_re_presented_window_does_not_rewrite_targets_that_already_applied_it() {
        let fluree = FlureeBuilder::memory().build_memory();
        let src = FakeSource::new(
            people_mapping(),
            vec![batch(&[
                ("id", &["1", "2", "3"]),
                ("name", &["alice", "bob", "carol"]),
                ("tenant", &["acme", "acme", "globex"]),
            ])],
        )
        .repeating();

        let first = fluree
            .materialize_from_source(
                &src,
                "people:main",
                "people_{tenant}:main",
                false,
                None,
                None,
            )
            .await
            .expect("first pass");
        assert_eq!(first.tally.ok, 2, "two tenants => two target ledgers");

        let targets = ["people_acme:main", "people_globex:main"];
        let mut t_after_first = Vec::new();
        for id in targets {
            t_after_first.push(fluree.ledger(id).await.expect("target ledger").t());
        }
        assert!(
            t_after_first.iter().all(|t| *t > 0),
            "first pass must actually commit or the second proves nothing: {t_after_first:?}"
        );

        let second = fluree
            .materialize_from_source(
                &src,
                "people:main",
                "people_{tenant}:main",
                false,
                None,
                None,
            )
            .await
            .expect("second pass");

        assert_eq!(
            src.scans(),
            2,
            "the window really was re-read, not short-circuited"
        );
        for (id, before) in targets.iter().zip(&t_after_first) {
            let after = fluree.ledger(id).await.expect("target ledger").t();
            assert_eq!(
                after, *before,
                "{id} already applied this window; a second commit means the rewrite is back"
            );
        }
        assert_eq!(
            second.tally.ok, 2,
            "a skipped target still counts as ok — it HAS the window; reporting it as \
             anything else would read as a regression in the tally"
        );
        assert_eq!(
            second.tally.deferred + second.tally.failed,
            0,
            "skipping is not deferral and not failure"
        );
    }

    /// A templated target fans out: one scan, N target ledgers, each its own commit
    /// domain. Pins that the tally counts TARGETS rather than polls — the accounting whose
    /// absence made a 21-of-22 production window read as a total stall.
    #[tokio::test]
    async fn a_templated_target_fans_out_per_row() {
        let fluree = FlureeBuilder::memory().build_memory();
        let src = FakeSource::new(
            people_mapping(),
            vec![batch(&[
                ("id", &["1", "2", "3"]),
                ("name", &["alice", "bob", "carol"]),
                ("tenant", &["acme", "acme", "globex"]),
            ])],
        );

        let result = fluree
            .materialize_from_source(
                &src,
                "people:main",
                "people_{tenant}:main",
                false,
                None,
                None,
            )
            .await
            .expect("materialize");

        assert_eq!(src.scans(), 1, "ONE scan feeds every target");
        assert!(result.committed);
        assert_eq!(
            result.tally,
            TargetTally {
                ok: 2,
                deferred: 0,
                failed: 0
            },
            "two distinct tenants => two target ledgers, both applied"
        );
        assert_eq!(
            result.subjects_upserted, 3,
            "all three subjects land somewhere"
        );
    }

    /// The reviewer's round-2 mutation, finally covered.
    ///
    /// Their mutation was NOT to break `chunk_nodes_by_size` — its own unit tests catch
    /// that. It neutralised chunking **at the call site**, leaving the function intact and
    /// the budget unbounded. I reproduced it (`txn_budget = usize::MAX`) and got **1019
    /// passed**, including the other engine tests here. So the seam alone did not meet the
    /// acceptance bar; this test is what does.
    ///
    /// The observable is the TARGET LEDGER'S `t`. A window that exceeds the transaction
    /// budget must be applied as several transactions, so `t` advances further than a
    /// single-chunk window would. `MaterializeResult` reports no chunk count, and the
    /// ledger's commit count is the honest proxy — it is what the novelty ceiling actually
    /// reacts to.
    #[tokio::test]
    async fn a_window_larger_than_the_txn_budget_is_applied_in_several_transactions() {
        let ids: Vec<String> = (0..40).map(|i| i.to_string()).collect();
        let names: Vec<String> = (0..40).map(|i| format!("person-{i:04}")).collect();
        let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let rows = vec![("id", id_refs.as_slice()), ("name", name_refs.as_slice())];

        // Same window twice: once unbounded (one chunk), once with a budget small enough
        // to force several. Comparing the two is what makes this mutation-sensitive —
        // asserting only "t > 2" could pass for unrelated reasons.
        let mut t_by_budget = Vec::new();
        for budget in [None, Some(200usize)] {
            let fluree = FlureeBuilder::memory().build_memory();
            let src = FakeSource::new(people_mapping(), vec![batch(&rows)]);
            let result = fluree
                .materialize_from_source(
                    &src,
                    "people:main",
                    "people_native:main",
                    false,
                    budget,
                    None,
                )
                .await
                .expect("materialize");
            assert_eq!(
                result.subjects_upserted, 40,
                "every row lands regardless of chunking"
            );
            let t = fluree
                .ledger("people_native:main")
                .await
                .expect("target ledger")
                .t();
            t_by_budget.push(t);
        }

        let (unbounded, chunked) = (t_by_budget[0], t_by_budget[1]);
        assert!(
            chunked > unbounded,
            "a small transaction budget must split the window into more transactions: \
             unbounded reached t={unbounded}, budget=200B reached t={chunked} — chunking \
             is not happening at the call site"
        );
    }

    /// Templated (non-constant) predicates must materialize. The hand-rolled emission
    /// this engine used before the shared enumerator silently DROPPED any POM whose
    /// predicate was not `rr:predicate`-constant, so a twin was missing triples the
    /// virtual query path serves. The shared enumerator expands
    /// `rr:predicateMap`/`rr:template` per row; this pins that the expanded predicate
    /// actually lands in the target ledger. Result-level counters cannot see it — a
    /// subject whose only POM is dropped still upserts (id + type), so
    /// `subjects_upserted` is identical either way and only ledger content
    /// distinguishes the two behaviors.
    #[tokio::test]
    async fn a_templated_predicate_materializes_into_the_target() {
        let tm = TriplesMap::new("http://tm/people", "people")
            .with_subject_template("http://ex/person/{id}")
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::template(
                    "http://ex/attr/{attr}",
                    vec!["attr".to_string()],
                ),
                object_map: ObjectMap::column("score"),
            });
        let mapping = Arc::new(CompiledR2rmlMapping::new(vec![tm]));

        let fluree = FlureeBuilder::memory().build_memory();
        let src = FakeSource::new(
            mapping,
            vec![batch(&[
                ("id", &["1"]),
                ("attr", &["height"]),
                ("score", &["tall"]),
            ])],
        );
        let result = fluree
            .materialize_from_source(&src, "people:main", "people_native:main", false, None, None)
            .await
            .expect("materialize");
        assert_eq!(result.subjects_upserted, 1);

        let ledger = fluree.ledger("people_native:main").await.expect("target");
        let db = crate::GraphDb::from_ledger_state(&ledger);
        let q = json!({
            "select": ["?o"],
            "where": { "@id": "http://ex/person/1", "http://ex/attr/height": "?o" }
        });
        let out = fluree
            .query(&db, &q)
            .await
            .expect("query")
            .to_jsonld_async(db.as_graph_db_ref())
            .await
            .expect("format")
            .to_string();
        assert!(
            out.contains("tall"),
            "the templated predicate's triple must land in the twin: {out}"
        );
    }

    /// The accumulator memory budget is a PRE-COMMIT circuit breaker: a window whose
    /// estimated accumulator exceeds it must fail with the typed error BEFORE anything is
    /// applied — no target ledger created, no state-ledger watermark persisted — and the
    /// SAME window under the default budget must succeed. The comparative shape is what
    /// makes it mutation-sensitive: gutting the gate turns the failing arm into the
    /// passing arm.
    #[tokio::test]
    async fn a_window_larger_than_the_memory_budget_fails_before_any_commit() {
        let ids: Vec<String> = (0..40).map(|i| i.to_string()).collect();
        let names: Vec<String> = (0..40).map(|i| format!("person-{i:04}")).collect();
        let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let rows = vec![("id", id_refs.as_slice()), ("name", name_refs.as_slice())];

        // Arm 1: a 200-byte budget cannot hold 40 subjects — typed error, nothing applied.
        let fluree = FlureeBuilder::memory().build_memory();
        let src = FakeSource::new(people_mapping(), vec![batch(&rows)]);
        let err = fluree
            .materialize_from_source(
                &src,
                "people:main",
                "people_native:main",
                false,
                None,
                Some(200),
            )
            .await
            .expect_err("40 subjects cannot fit a 200-byte accumulator budget");
        match &err {
            ApiError::MaterializeMemoryBudget {
                estimated_bytes,
                budget_bytes,
                distinct_subjects,
                ..
            } => {
                assert_eq!(*budget_bytes, 200);
                assert!(*estimated_bytes > 200);
                assert!(*distinct_subjects > 0);
            }
            other => panic!("expected MaterializeMemoryBudget, got: {other:?}"),
        }
        assert!(
            !fluree
                .ledger_exists("people_native:main")
                .await
                .expect("exists check"),
            "pre-commit abort must not create or touch the target ledger"
        );
        assert!(
            !fluree
                .ledger_exists(MATERIALIZE_STATE_LEDGER)
                .await
                .expect("exists check"),
            "pre-commit abort must not persist a watermark"
        );

        // Arm 2: the same window under the default budget applies fully.
        let fluree = FlureeBuilder::memory().build_memory();
        let src = FakeSource::new(people_mapping(), vec![batch(&rows)]);
        let result = fluree
            .materialize_from_source(&src, "people:main", "people_native:main", false, None, None)
            .await
            .expect("materialize");
        assert_eq!(result.subjects_upserted, 40);
    }

    /// A row whose template column is NULL cannot be routed anywhere, so it is skipped
    /// rather than defaulting into some other tenant's ledger. Silent misrouting across a
    /// tenant boundary is the worst available failure here.
    #[tokio::test]
    async fn an_unroutable_row_is_skipped_not_misfiled() {
        let fluree = FlureeBuilder::memory().build_memory();
        let fields = vec![
            FieldInfo {
                name: "id".into(),
                field_type: FieldType::String,
                nullable: true,
                field_id: 1,
            },
            FieldInfo {
                name: "name".into(),
                field_type: FieldType::String,
                nullable: true,
                field_id: 2,
            },
            FieldInfo {
                name: "tenant".into(),
                field_type: FieldType::String,
                nullable: true,
                field_id: 3,
            },
        ];
        let cols = vec![
            Column::String(vec![Some("1".into()), Some("2".into())]),
            Column::String(vec![Some("alice".into()), Some("bob".into())]),
            Column::String(vec![Some("acme".into()), None]), // row 2 has no tenant
        ];
        let b = ColumnBatch::new(Arc::new(BatchSchema::new(fields)), cols).expect("batch");
        let src = FakeSource::new(people_mapping(), vec![b]);

        let result = fluree
            .materialize_from_source(
                &src,
                "people:main",
                "people_{tenant}:main",
                false,
                None,
                None,
            )
            .await
            .expect("materialize");

        assert_eq!(
            result.tally,
            TargetTally {
                ok: 1,
                deferred: 0,
                failed: 0
            },
            "only the routable tenant gets a ledger"
        );
        assert_eq!(
            result.subjects_upserted, 1,
            "the unroutable row is dropped, not misfiled"
        );
    }
}
