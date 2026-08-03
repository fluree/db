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
    batch_has_column, column_is_orderable, column_sort_key, column_string,
    expand_template_from_batch, materialize_graph_from_batch, materialize_object_from_batch,
    materialize_subject_from_batch, RdfTerm,
};
use fluree_vocab::UnresolvedDatatypeConstraint;

use crate::graph_source::FlureeR2rmlProvider;
use crate::{ApiError, Fluree, Result};
use tracing::info;

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

        // 1. Compiled R2RML mapping (subject / predicate / object maps) and the
        //    materialization options (delete convention + latest-by-key ordering).
        let mapping = provider
            .compiled_mapping(source_graph_source_id, None)
            .await?;
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

        let mut accum = MaterializeAccum::default();
        let mut rows_read = 0usize;
        let mut incremental_all = true;
        let mut any_table = false;
        let mut all_watermarks_unchanged = true;
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

            let (to_id, incremental, batches) = provider
                .scan_for_materialize(source_graph_source_id, table_name, &[], from_t)
                .await?;
            // Only count a table as contributing once it actually has a snapshot.
            if let Some(to) = to_id {
                any_table = true;
                incremental_all = incremental_all && incremental;
                if Some(to) != from_t {
                    all_watermarks_unchanged = false;
                }
                table_watermarks.push(((*table_name).to_string(), from_t, to));
            }

            for batch in &batches {
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
                    for tm in tms {
                        materialize_row_into(
                            tm,
                            batch,
                            row,
                            row_target.clone(),
                            delete_convention.as_ref(),
                            order_by.as_deref(),
                            latest_by_key,
                            &mut accum,
                        )?;
                    }
                }
            }
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

        // No-delta short-circuit: nothing read and no watermark advanced.
        if live.is_empty() && deletions.is_empty() && all_watermarks_unchanged {
            return Ok(MaterializeResult {
                to_snapshot_id,
                from_snapshot_id,
                incremental,
                committed: false,
                rows_read,
                subjects_upserted: 0,
                subjects_retracted: 0,
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
        // here) while the ceiling is in flake bytes. JSON-LD text runs several times
        // larger than the flakes it yields — that window was ~108 flake-bytes per
        // row against JSON nodes of a few hundred bytes — so budgeting JSON bytes at
        // a QUARTER of the flake ceiling errs in the safe direction. Over-chunking is
        // nearly free: `reindex_min_bytes` is 1 MiB, so the indexer drains novelty
        // between chunks rather than letting it accumulate toward the ceiling.
        let txn_budget = (self.index_config.reindex_max_bytes / 4).max(1 << 20);

        let mut subjects_retracted = 0usize;
        for (target, (live, deletions)) in by_target {
            // Retraction is skipped on a brand-new target (nothing to retract);
            // deletions only count against a pre-existing target.
            let target_existed = self.ledger_exists(&target).await?;
            if target_existed {
                subjects_retracted += deletions.len();
            }

            // Whole-subject REPLACE (latest-by-key): retract every subject seen in
            // this window (live OR tombstone) — per graph — before re-asserting, so
            // a dropped field clears and a tombstone is removed. The retract and
            // re-assert are both graph-scoped, so a subject in graph B never touches
            // the same IRI in graph A. Additive mode skips this (per-predicate
            // upsert suffices; a subject may legitimately span rows).
            let mut retract_by_graph: BTreeMap<Option<String>, BTreeSet<String>> = BTreeMap::new();
            if latest_by_key {
                for (graph, subject) in live.keys().cloned().chain(deletions.iter().cloned()) {
                    retract_by_graph.entry(graph).or_default().insert(subject);
                }
            }

            let mut ledger = if target_existed {
                self.ledger(&target).await?
            } else {
                self.create_ledger(&target).await?
            };

            if target_existed {
                for (graph, iris) in &retract_by_graph {
                    if iris.is_empty() {
                        continue;
                    }
                    for chunk in chunk_iris_by_size(iris, txn_budget) {
                        ledger = self
                            .update(ledger, &build_retract_doc(&chunk, graph.as_deref()))
                            .await?
                            .ledger;
                    }
                }
            }

            if latest_by_key {
                // The retraction cleared every seen subject (per graph), so the
                // re-asserted nodes (carrying @type) are the sole source of truth —
                // a single upsert per graph is correct.
                let mut nodes_by_graph: BTreeMap<Option<String>, Vec<JsonValue>> = BTreeMap::new();
                for ((graph, _subject), node) in live {
                    nodes_by_graph
                        .entry(graph)
                        .or_default()
                        .push(node.into_json());
                }
                // A delete-only window (tombstones, no live rows) leaves
                // `nodes_by_graph` empty; skip the upsert rather than send an
                // empty `[]` doc (the transactor rejects an upsert with no
                // predicate/@type). The retracts above already applied.
                let live_doc = nodes_by_graph_to_doc(nodes_by_graph);
                // Chunked: one window used to be one transaction, which a wide
                // source cannot fit under the novelty ceiling at any window size
                // (see `transaction_json_budget`).
                for chunk in chunk_nodes_by_size(live_doc, txn_budget) {
                    ledger = self.upsert(ledger, &JsonValue::Array(chunk)).await?.ledger;
                }
            } else {
                // Additive mode: assert `@type` via an idempotent `insert` so
                // classes UNION across sources, and `upsert` only the remaining
                // predicates (a single upsert carrying `@type` would retract-then-
                // insert rdf:type per predicate, clobbering classes other sources
                // added). Both grouped per graph.
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
                for chunk in chunk_nodes_by_size(type_doc, txn_budget) {
                    ledger = self.insert(ledger, &JsonValue::Array(chunk)).await?.ledger;
                }
                // A type-only source (e.g. an r2rml `entity_type` map: subject +
                // rdf:type, no other predicates) leaves `pred_by_graph` empty, so
                // the doc is `[]`; skip the upsert rather than send an empty doc.
                // An unconditional `upsert([])` is rejected ("Upsert must contain
                // at least one predicate or @type"), which aborts the sync BEFORE
                // its watermark advances — so the next poll full-rescans and
                // re-fails forever (churn: a new @type-insert commit every poll).
                let pred_doc = nodes_by_graph_to_doc(pred_by_graph);
                for chunk in chunk_nodes_by_size(pred_doc, txn_budget) {
                    ledger = self.upsert(ledger, &JsonValue::Array(chunk)).await?.ledger;
                }
            }
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
            self.upsert(state, &JsonValue::Array(watermark_nodes))
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

/// Per-pass latest-by-key accumulator: one [`KeyState`] per [`AccumKey`]. The
/// BTreeMap keeps keys in a stable order (deterministic transaction) and groups
/// naturally by target ledger (the first tuple element) at commit time.
#[derive(Default)]
struct MaterializeAccum {
    keys: BTreeMap<AccumKey, KeyState>,
}

impl MaterializeAccum {
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
        match self.keys.get(&key) {
            Some(existing) if rank < existing.rank => {} // older row: ignore
            _ => {
                self.keys.insert(key, KeyState { rank, node });
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
            Some(KeyState {
                node: Some(existing),
                ..
            }) => existing.merge(node),
            _ => {
                self.keys.insert(
                    key,
                    KeyState {
                        rank: None,
                        node: Some(node),
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
fn build_retract_doc(iris: &BTreeSet<String>, graph: Option<&str>) -> JsonValue {
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
/// [`build_live_node`].
const RDF_TYPE_IRI: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

/// Build a live `SubjectNode` from one source row (subject classes + the
/// constant-predicate objects). RefObjectMap joins are resolved at query time,
/// not during materialization.
fn build_live_node(
    tm: &TriplesMap,
    batch: &ColumnBatch,
    row: usize,
    id: String,
) -> Result<SubjectNode> {
    let mut node = SubjectNode::new(id);
    for class in &tm.subject_map.classes {
        node.add_type(class);
    }
    for pom in &tm.predicate_object_maps {
        let Some(predicate) = pom.predicate_map.as_constant() else {
            continue;
        };
        let obj = materialize_object_from_batch(&pom.object_map, batch, row)
            .map_err(|e| ApiError::Internal(format!("R2RML object materialization failed: {e}")))?;
        if let Some(term) = obj {
            // A data-driven `rdf:type` object map is a CLASS, not an ordinary
            // predicate. `rr:class` is constant-only, so per-row typing (e.g.
            // `as:Announce` vs `as:Article` from a `type` column) must be a POM
            // with `rr:predicate rdf:type`. Route it to the subject's @type so it
            // UNIONS across sources in additive mode — as an ordinary predicate it
            // would be upserted and clobber other sources' types on a shared
            // subject. (A non-IRI rdf:type object is malformed; fall through.)
            match term {
                RdfTerm::Iri(iri) if predicate == RDF_TYPE_IRI => node.add_type(&iri),
                other => node.add_object(predicate, rdf_term_to_jsonld(other)),
            }
        }
    }
    Ok(node)
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
    tm: &TriplesMap,
    batch: &ColumnBatch,
    row: usize,
    target_ledger: String,
    convention: Option<&DeleteConvention>,
    order_by: Option<&str>,
    latest_by_key: bool,
    accum: &mut MaterializeAccum,
) -> Result<()> {
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
        let node = build_live_node(tm, batch, row, subject_iri.clone())?;
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
        Some(build_live_node(tm, batch, row, subject_iri.clone())?)
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
        let mut set = BTreeSet::new();
        set.insert("urn:a".to_string());
        set.insert("urn:b".to_string());
        let doc = build_retract_doc(&set, None);
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
        let mut set = BTreeSet::new();
        set.insert("urn:a".to_string());
        let doc = build_retract_doc(&set, Some("urn:g:tenant/user"));
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
        use fluree_db_r2rml::mapping::{ObjectMap, PredicateMap, PredicateObjectMap, TriplesMap};
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

        let node = build_live_node(&tm, &batch, 0, "urn:s".to_string()).unwrap();
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
