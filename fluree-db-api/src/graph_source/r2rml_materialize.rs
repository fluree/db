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
    materialize_graph_from_batch, materialize_object_from_batch, materialize_subject_from_batch,
    RdfTerm,
};
use fluree_vocab::UnresolvedDatatypeConstraint;

use crate::graph_source::FlureeR2rmlProvider;
use crate::{ApiError, Fluree, Result};

/// Subject-IRI prefix for the per-(source, table) materialization watermark
/// stored in the target ledger. The full subject is
/// `{PREFIX}{source_graph_source_id}:{table_name}` so one target ledger can
/// track several sources — and each source's tables — independently.
const WATERMARK_SUBJECT_PREFIX: &str = "urn:fluree:materialize-state:";
/// Predicate holding the last materialized source snapshot id (stored as a
/// string to preserve full i64 precision for 19-digit snapshot ids).
const WATERMARK_SNAPSHOT_PRED: &str = "urn:fluree:materialize#lastSnapshotId";
/// Predicate recording which source the watermark belongs to (informational).
const WATERMARK_SOURCE_PRED: &str = "urn:fluree:materialize#source";
/// Predicate recording which source table the watermark belongs to.
const WATERMARK_TABLE_PRED: &str = "urn:fluree:materialize#table";

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
        let target_existed = self.ledger_exists(target_ledger_id).await?;

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
            let from_t = if force_full || !target_existed {
                None
            } else {
                self.materialize_watermark(target_ledger_id, source_graph_source_id, table_name)
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
                    for tm in tms {
                        materialize_row_into(
                            tm,
                            batch,
                            row,
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
        //    set of subjects whose latest row is a tombstone (deletions).
        let (live, deletions) = accum.finalize();
        let subjects_upserted = live.len();
        // Deletions are only actually applied when the target already existed
        // (retraction is skipped on a brand-new target — nothing to retract).
        let subjects_retracted = if target_existed { deletions.len() } else { 0 };

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

        // 4. In latest-by-key mode, whole-subject REPLACE: retract every subject
        //    seen in this window (live OR tombstone) so a live revision that
        //    dropped a field clears the stale value and a tombstone is removed;
        //    then re-assert the live nodes + advance the per-table watermarks. Two
        //    ordered commits keep the watermark advancing ONLY in the upsert
        //    (crash-safe: a crash between them re-reads the same window next run).
        //    Retraction is skipped on a brand-new target (nothing to retract) and
        //    in legacy additive mode (per-predicate upsert suffices; a subject may
        //    legitimately span rows we must not clobber).
        //
        //    Named graphs: the retract and the re-assert are BOTH scoped to each
        //    subject's graph — a subject in graph B is retracted/upserted only in
        //    B, never touching the same IRI's statements in graph A. This is what
        //    lets one entity IRI carry independent per-(tenant,user) facts. The
        //    upsert's own retract-existing is already graph-scoped
        //    (generate_upsert_deletions keys on graph_id), and the whole-subject
        //    retract carries the graph via the UPDATE `graph` key.
        let mut retract_by_graph: BTreeMap<Option<String>, BTreeSet<String>> = BTreeMap::new();
        if latest_by_key {
            for (graph, subject) in live.keys().cloned().chain(deletions.iter().cloned()) {
                retract_by_graph.entry(graph).or_default().insert(subject);
            }
        }

        let mut ledger = if target_existed {
            self.ledger(target_ledger_id).await?
        } else {
            self.create_ledger(target_ledger_id).await?
        };

        if target_existed {
            for (graph, iris) in &retract_by_graph {
                if iris.is_empty() {
                    continue;
                }
                ledger = self
                    .update(ledger, &build_retract_doc(iris, graph.as_deref()))
                    .await?
                    .ledger;
            }
        }

        if latest_by_key {
            // Whole-subject replace: the retraction above cleared every seen
            // subject (per graph), so the re-asserted nodes (carrying @type) are
            // the sole source of truth for the subject in its graph — a single
            // upsert is correct. Group nodes by graph so each lands in its own
            // named graph; watermarks are bookkeeping and stay in the default graph.
            let mut by_graph: BTreeMap<Option<String>, Vec<JsonValue>> = BTreeMap::new();
            for ((graph, _subject), node) in live {
                by_graph.entry(graph).or_default().push(node.into_json());
            }
            for (table, _from, to) in &table_watermarks {
                by_graph.entry(None).or_default().push(watermark_node(
                    source_graph_source_id,
                    table,
                    *to,
                ));
            }
            self.upsert(ledger, &JsonValue::Array(nodes_by_graph_to_doc(by_graph)))
                .await?;
        } else {
            // Additive mode: a subject may be contributed to by several sources —
            // a shared target ledger fed by many graph sources, or a join table
            // that only adds an edge to a parent entity. Assert `@type` with an
            // idempotent `insert` so classes UNION across sources, and `upsert`
            // only the remaining predicates. A single upsert carrying `@type`
            // would retract-then-insert rdf:type per predicate, so the last source
            // to touch a shared subject would CLOBBER the classes the others added
            // (e.g. a `silver_article_tag` join row re-typing its article, or an
            // `entity_type` row adding `as:Announce`). Two commits: insert the
            // classes first, then upsert predicates + advance the watermark — the
            // watermark advances only in the upsert, so a crash between them
            // re-runs this window (self-healing; insert is idempotent). Both the
            // type-insert and the predicate-upsert are grouped by graph so classes
            // union and predicates upsert WITHIN each subject's graph.
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
            if !type_doc.is_empty() {
                ledger = self
                    .insert(ledger, &JsonValue::Array(type_doc))
                    .await?
                    .ledger;
            }
            for (table, _from, to) in &table_watermarks {
                pred_by_graph.entry(None).or_default().push(watermark_node(
                    source_graph_source_id,
                    table,
                    *to,
                ));
            }
            self.upsert(
                ledger,
                &JsonValue::Array(nodes_by_graph_to_doc(pred_by_graph)),
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
        })
    }

    /// Read the per-table materialization watermark (last materialized source
    /// snapshot id) for `(source_graph_source_id, table_name)` from the target
    /// ledger. Returns `None` if the target ledger does not exist yet or carries
    /// no watermark for that source table.
    pub async fn materialize_watermark(
        &self,
        target_ledger_id: &str,
        source_graph_source_id: &str,
        table_name: &str,
    ) -> Result<Option<i64>> {
        if !self.ledger_exists(target_ledger_id).await? {
            return Ok(None);
        }
        let db = self.db(target_ledger_id).await?;

        let subject = watermark_subject(source_graph_source_id, table_name);
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
}

/// The per-(source, table) watermark subject IRI. Both segments are escaped
/// (`%` -> `%25`, `:` -> `%3A`) before joining with `:` so the encoding is
/// injective — distinct `(source, table)` pairs can never collide (e.g.
/// `("a:b","c")` vs `("a","b:c")`).
fn watermark_subject(source_graph_source_id: &str, table_name: &str) -> String {
    fn esc(s: &str) -> String {
        s.replace('%', "%25").replace(':', "%3A")
    }
    format!(
        "{WATERMARK_SUBJECT_PREFIX}{}:{}",
        esc(source_graph_source_id),
        esc(table_name)
    )
}

/// Build the watermark JSON-LD node (`@id` + last snapshot id + source + table).
/// The snapshot id is stored as a string to preserve full i64 precision;
/// `upsert` retracts-then-asserts so the watermark advances cleanly in place.
fn watermark_node(
    source_graph_source_id: &str,
    table_name: &str,
    to_snapshot_id: i64,
) -> JsonValue {
    let mut node = Map::new();
    node.insert(
        "@id".to_string(),
        JsonValue::String(watermark_subject(source_graph_source_id, table_name)),
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
        WATERMARK_TABLE_PRED.to_string(),
        JsonValue::String(table_name.to_string()),
    );
    JsonValue::Object(node)
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

/// Per-pass latest-by-key accumulator: one [`KeyState`] per **(graph, subject)**.
/// The graph is the materialized named-graph IRI (`None` = default graph), so the
/// same subject IRI in two graphs is two independent keys — the basis for
/// per-(tenant,user) statements about a shared entity. The BTreeMap keeps keys in
/// a stable order (deterministic transaction).
#[derive(Default)]
struct MaterializeAccum {
    keys: BTreeMap<(Option<String>, String), KeyState>,
}

impl MaterializeAccum {
    /// Record a classified row for `subject_iri`. `rank` is the row's ordering
    /// key (from the `order_by` column) or `None` for scan-order. `node` is the
    /// live node, or `None` for a tombstone. The row wins (replaces the prior
    /// state) unless its rank is strictly older than the stored rank — so with an
    /// ordering column the highest-ordered row wins, and without one (all ranks
    /// equal `None`) the last row in scan order wins. This is a whole-row
    /// REPLACE; per-subject merge across rows is the legacy additive path
    /// ([`merge_live`](Self::merge_live)).
    fn record(
        &mut self,
        graph: Option<String>,
        subject_iri: String,
        rank: Option<(i128, String)>,
        node: Option<SubjectNode>,
    ) {
        let key = (graph, subject_iri);
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
    fn merge_live(&mut self, graph: Option<String>, subject_iri: String, node: SubjectNode) {
        let key = (graph, subject_iri);
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

    /// Resolve into `(live nodes to assert, subject IRIs whose latest row is a
    /// tombstone)`.
    #[allow(clippy::type_complexity)]
    fn finalize(
        self,
    ) -> (
        BTreeMap<(Option<String>, String), SubjectNode>,
        BTreeSet<(Option<String>, String)>,
    ) {
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
fn materialize_row_into(
    tm: &TriplesMap,
    batch: &ColumnBatch,
    row: usize,
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
        // Legacy additive: merge this live row into the (graph, subject) node.
        let node = build_live_node(tm, batch, row, subject_iri.clone())?;
        accum.merge_live(graph_iri, subject_iri, node);
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
    accum.record(graph_iri, subject_iri, rank, node);
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
    /// Default-graph key for `(graph, subject)` map/set assertions.
    fn dk(iri: &str) -> (Option<String>, String) {
        (None, iri.to_string())
    }

    #[test]
    fn finalize_live_only_upserts() {
        let mut a = MaterializeAccum::default();
        a.record(None, "urn:a".into(), None, live("urn:a"));
        let (live, del) = a.finalize();
        assert!(live.contains_key(&dk("urn:a")));
        assert!(del.is_empty());
    }

    #[test]
    fn finalize_tombstone_only_retracts() {
        let mut a = MaterializeAccum::default();
        a.record(None, "urn:a".into(), None, None);
        let (live, del) = a.finalize();
        assert!(live.is_empty());
        assert!(del.contains(&dk("urn:a")));
    }

    #[test]
    fn scan_order_last_wins_live_then_tombstone() {
        // No ordering column: last row in scan order wins -> tombstone.
        let mut a = MaterializeAccum::default();
        a.record(None, "urn:a".into(), None, live("urn:a"));
        a.record(None, "urn:a".into(), None, None);
        let (live, del) = a.finalize();
        assert!(live.is_empty());
        assert!(del.contains(&dk("urn:a")));
    }

    #[test]
    fn scan_order_last_wins_tombstone_then_live() {
        let mut a = MaterializeAccum::default();
        a.record(None, "urn:a".into(), None, None);
        a.record(None, "urn:a".into(), None, live("urn:a"));
        let (live, del) = a.finalize();
        assert!(live.contains_key(&dk("urn:a")));
        assert!(!del.contains(&dk("urn:a")));
    }

    #[test]
    fn order_by_latest_wins_regardless_of_arrival() {
        // A higher-ranked tombstone arriving FIRST still wins over a lower-ranked
        // live row arriving later (ordering, not scan order, decides).
        let mut a = MaterializeAccum::default();
        a.record(None, "urn:a".into(), ts(200), None); // newer tombstone
        a.record(None, "urn:a".into(), ts(100), live("urn:a")); // older live
        let (live, del) = a.finalize();
        assert!(live.is_empty());
        assert!(del.contains(&dk("urn:a")));
    }

    #[test]
    fn order_by_newer_live_wins_over_older_tombstone() {
        let mut a = MaterializeAccum::default();
        a.record(None, "urn:a".into(), ts(100), None); // older tombstone
        a.record(None, "urn:a".into(), ts(200), live("urn:a")); // newer live
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
        a.record(ga.clone(), "urn:x".into(), None, None); // tombstone in A
        a.record(gb.clone(), "urn:x".into(), None, live("urn:x")); // live in B
        let (live, del) = a.finalize();
        assert!(live.contains_key(&(gb, "urn:x".to_string())));
        assert!(del.contains(&(ga, "urn:x".to_string())));
        assert_eq!(live.len(), 1);
        assert_eq!(del.len(), 1);
    }

    #[test]
    fn merge_live_isolates_same_subject_across_graphs() {
        // Additive merge is per (graph, subject): the same IRI in two graphs
        // accumulates independently, not into one merged node.
        let ga = Some("urn:g:a".to_string());
        let gb = Some("urn:g:b".to_string());
        let mut a = MaterializeAccum::default();
        a.merge_live(ga.clone(), "urn:x".into(), SubjectNode::new("urn:x".into()));
        a.merge_live(gb.clone(), "urn:x".into(), SubjectNode::new("urn:x".into()));
        let (live, _del) = a.finalize();
        assert_eq!(live.len(), 2);
        assert!(live.contains_key(&(ga, "urn:x".to_string())));
        assert!(live.contains_key(&(gb, "urn:x".to_string())));
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
    fn watermark_node_is_per_table_and_string_encoded() {
        let node = watermark_node("people:main", "demo.actors", 5_648_190_075_564_901_028);
        // Source's ':' is escaped (%3A) so (source, table) encoding is injective.
        assert_eq!(
            node["@id"],
            json!("urn:fluree:materialize-state:people%3Amain:demo.actors")
        );
        // String-encoded to preserve full i64 precision.
        assert_eq!(node[WATERMARK_SNAPSHOT_PRED], json!("5648190075564901028"));
        assert_eq!(node[WATERMARK_SOURCE_PRED], json!("people:main"));
        assert_eq!(node[WATERMARK_TABLE_PRED], json!("demo.actors"));
    }

    #[test]
    fn watermark_subject_is_injective() {
        // Distinct (source, table) pairs that would collide under a naive ':'
        // join must produce distinct subjects.
        assert_ne!(watermark_subject("a:b", "c"), watermark_subject("a", "b:c"));
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
}
