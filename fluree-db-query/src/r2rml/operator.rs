//! R2RML Scan Operator
//!
//! This operator executes R2RML scans against Iceberg tables and emits
//! RDF term bindings according to the mapping specification.
//!
//! # Design
//!
//! The operator is correlated: it consumes a child stream (often an EmptyOperator seed)
//! and for each input row, scans the appropriate Iceberg table(s) and materializes
//! RDF terms according to the TriplesMap definition.
//!
//! # Execution Flow
//!
//! 1. `open()`: Load the compiled R2RML mapping from the provider
//! 2. `next_batch()`: For each input row:
//!    - Scan the logical table from the TriplesMap
//!    - For RefObjectMap joins, scan parent tables and build lookup indexes
//!    - Materialize subject/predicate/object terms
//!    - Emit bindings for query variables
//! 3. `close()`: Release resources
//!
//! # RefObjectMap Join Execution
//!
//! When a PredicateObjectMap contains a RefObjectMap (referencing a parent TriplesMap),
//! the operator:
//!
//! 1. Scans the parent TriplesMap's table
//! 2. Builds a hash lookup: parent join key → parent subject IRI
//! 3. For each child row, extracts child join key values
//! 4. Looks up the parent subject IRI from the hash map
//! 5. Emits the parent IRI as the object binding

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::eval::PreparedBoolExpression;
use crate::filter::filter_batch;
use crate::group_aggregate::{binding_to_group_key_normalized, GroupKeyOwned};
use crate::ir::R2rmlPattern;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::r2rml::ColumnBatchStream;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_r2rml::mapping::{CompiledR2rmlMapping, ObjectMap, PredicateObjectMap, TriplesMap};
use fluree_db_r2rml::materialize::{
    get_join_key_from_batch, materialize_object_from_batch, materialize_subject_from_batch, RdfTerm,
};
use fluree_db_tabular::ColumnBatch;
use fluree_vocab::xsd;
use futures::StreamExt;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

/// Lookup table for RefObjectMap joins.
///
/// Maps parent join key (as `Vec<String>`) to materialized parent subject IRI.
/// The key is a composite key of all parent columns specified in join conditions.
pub type ParentLookup = HashMap<Vec<String>, RdfTerm>;

/// Composite key for caching a parent lookup: `(parent_tm_iri, sorted_parent_join_cols)`.
type LookupCacheKey = (String, Vec<String>);

/// Target number of table rows to materialize into bindings per parallel window.
///
/// Materialization explodes the compact columnar form into fat `Binding` rows
/// (the memory wall: a full-table materialize of a 6M-row scan is ~14 GB). By
/// materializing one bounded window at a time — still in parallel on rayon —
/// the resident binding footprint is capped at roughly this many rows while the
/// scan streams to the downstream operator. Override with
/// `FLUREE_R2RML_MATERIALIZE_WINDOW_ROWS`.
const DEFAULT_MATERIALIZE_WINDOW_ROWS: usize = 512 * 1024;

fn materialize_window_rows() -> usize {
    std::env::var("FLUREE_R2RML_MATERIALIZE_WINDOW_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_MATERIALIZE_WINDOW_ROWS)
}

/// Whether LIMIT early-termination (row-budget) pushdown into the scan is
/// enabled. Read once from `FLUREE_R2RML_LIMIT_PUSHDOWN` (only `0`/`false`/`off`
/// disable it); disabling restores full-window materialization under a LIMIT.
fn limit_pushdown_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("FLUREE_R2RML_LIMIT_PUSHDOWN") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off"
        ),
        Err(_) => true,
    })
}

/// How a window of produced rows is combined with the buffered child rows.
///
/// The join is *flipped* relative to a naive per-child probe: the (small,
/// already-buffered) child side is indexed once, and the (large) produced side
/// is streamed window-by-window and probed against it. This is what lets the
/// scan avoid materializing the whole table's bindings at once.
enum JoinPlan {
    /// No shared variables: every produced row pairs with every child row
    /// (typically the single seed row).
    Cross,
    /// Shared join variables: child rows indexed by their join-key values.
    Hash {
        join_vars: Vec<VarId>,
        /// Fully-bound child rows: join key → child row indices.
        full_index: HashMap<Vec<GroupKeyOwned>, Vec<usize>>,
        /// Child rows with an unbound join var (a wildcard position, matched
        /// linearly): `(child_row_idx, key with None for the unbound vars)`.
        partial_rows: Vec<(usize, Vec<Option<GroupKeyOwned>>)>,
    },
}

/// One TriplesMap's streaming scan state: the live batch stream from the table
/// scan, the parent lookups for its RefObjectMap joins, and the precomputed join
/// plan against the buffered child. Batches are pulled in bounded windows and
/// dropped after materializing, so the whole table is never resident.
struct TmStream {
    tm_iri: String,
    stream: ColumnBatchStream,
    exhausted: bool,
    parent_lookups: HashMap<LookupCacheKey, ParentLookup>,
    join: JoinPlan,
}

/// In-flight streaming scan for one buffered child batch. The child is held
/// resident (it is the small side and is indexed by the join plans); each
/// TriplesMap's produced batches are materialized in bounded windows and emitted
/// incrementally, so the operator never holds the whole table's bindings.
struct ScanProgress {
    child_batch: Batch,
    child_schema: Vec<VarId>,
    tms: Vec<TmStream>,
    tm_idx: usize,
    window_rows: usize,
}

/// R2RML scan operator for `Pattern::R2rml`.
///
/// Scans an Iceberg table through an R2RML mapping and produces RDF term bindings.
pub struct R2rmlScanOperator {
    /// Child operator providing input solutions (may be EmptyOperator seed)
    child: BoxedOperator,
    /// R2RML pattern from the query IR
    pattern: R2rmlPattern,
    /// Output schema (child schema + new vars from R2RML scan)
    schema: Arc<[VarId]>,
    /// Mapping from variables to output column positions
    out_pos: HashMap<VarId, usize>,
    /// Cached compiled mapping (loaded once in open)
    mapping: Option<Arc<CompiledR2rmlMapping>>,
    /// Pending output rows that overflowed the current output batch.
    pending: VecDeque<Vec<Binding>>,
    /// In-flight streaming scan for the current buffered child batch, advanced
    /// one window per `next_batch` so the whole table is never materialized.
    progress: Option<ScanProgress>,
    /// Inner-table scans cached across child batches, keyed by
    /// `(table_name, projection)`. A correlated join re-invokes `build_progress`
    /// once per child batch; without this the (dimension-sized) inner table is
    /// re-scanned every batch. Only inners up to one materialize window are
    /// cached, so a cached inner never exceeds the resident footprint a single
    /// scan window already holds; larger inners fall back to per-batch streaming.
    /// Only UNFILTERED scans are cached — a filtered scan may return a pruned
    /// subset, which the filter-agnostic key must never replay for another scan.
    scan_cache: HashMap<(String, Vec<String>), Arc<Vec<ColumnBatch>>>,
    /// LIMIT early-termination budget: the max output rows a downstream `LIMIT`
    /// needs from this operator. `None` = unbounded. Set only when this is the
    /// topmost row-preserving scan (a scan feeding a join/FILTER never receives
    /// one), so once `emitted` reaches it the scan can stop without changing
    /// results. Also caps the materialize window so a `LIMIT n` does not
    /// materialize a full window before the first row.
    row_budget: Option<usize>,
    /// Output rows emitted so far, counted against `row_budget`.
    emitted: usize,
    /// A scan-local FILTER the planner folded into this scan (see
    /// [`R2rmlPattern::consumed_filter`]). Applied to each output batch with the
    /// same evaluator the dropped `FilterOperator` would use, so results are
    /// unchanged — but now the LIMIT budget and the filter live in one operator,
    /// so a `FILTER + LIMIT` scan can stop after enough *matching* rows.
    consumed_filter: Option<PreparedBoolExpression>,
    /// State
    state: OperatorState,
}

impl R2rmlScanOperator {
    /// Create a new R2RML scan operator.
    pub fn new(child: BoxedOperator, pattern: R2rmlPattern) -> Self {
        let child_schema = child.schema();

        // Build output schema: start with child vars, then add R2RML pattern vars
        let mut schema_vars: Vec<VarId> = child_schema.to_vec();
        let mut seen: HashSet<VarId> = schema_vars.iter().copied().collect();

        // Add subject variable if new (constant subjects bind no variable)
        if let Some(subject_var) = pattern.subject_var {
            if seen.insert(subject_var) {
                schema_vars.push(subject_var);
            }
        }

        // Add object variable if present and new
        if let Some(obj_var) = pattern.object_var {
            if seen.insert(obj_var) {
                schema_vars.push(obj_var);
            }
        }

        // Add same-subject star object variables if new
        for (_, var) in &pattern.star_bindings {
            if seen.insert(*var) {
                schema_vars.push(*var);
            }
        }

        // Build output position map
        let out_pos: HashMap<VarId, usize> = schema_vars
            .iter()
            .enumerate()
            .map(|(i, &v)| (v, i))
            .collect();

        let schema = Arc::from(schema_vars);

        let consumed_filter = pattern
            .consumed_filter
            .clone()
            .map(PreparedBoolExpression::new);

        Self {
            child,
            pattern,
            schema,
            out_pos,
            mapping: None,
            pending: VecDeque::new(),
            progress: None,
            scan_cache: HashMap::new(),
            row_budget: None,
            emitted: 0,
            consumed_filter,
            state: OperatorState::Created,
        }
    }

    /// Build the output batch from accumulated `columns`, applying the consumed
    /// scan-local filter when present. Returns `None` when nothing survives (an
    /// empty window, or every row filtered out), so the caller keeps pulling.
    fn finalize_batch(
        &self,
        columns: Vec<Vec<Binding>>,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<Batch>> {
        let batch = Batch::new(Arc::clone(&self.schema), columns)?;
        if batch.is_empty() {
            return Ok(None);
        }
        match &self.consumed_filter {
            Some(prepared) => filter_batch(&batch, prepared, &self.schema, ctx),
            None => Ok(Some(batch)),
        }
    }

    /// All predicate IRIs this pattern materializes: the base `predicate_filter`
    /// plus any same-subject star members. Used for projection and parent-lookup
    /// building so a star scan reads every needed column in one pass.
    fn pattern_predicates(&self) -> Vec<&str> {
        let mut preds: Vec<&str> = Vec::new();
        if let Some(p) = self.pattern.predicate_filter.as_deref() {
            preds.push(p);
        }
        for (pred, _) in &self.pattern.star_bindings {
            preds.push(pred.as_str());
        }
        preds
    }

    /// True for a pure `rdf:type`/subject-only pattern: no object var, no
    /// predicate filter, no star members. Such a pattern derives only the subject
    /// (its `rr:class` constraint is enforced by TriplesMap selection), so it
    /// needs only the subject columns and scans no RefObjectMap parents. A true
    /// wildcard `?s ?p ?o` is excluded — it has `object_var = Some` and must
    /// still materialize every POM/parent.
    fn is_subject_only_pattern(&self) -> bool {
        self.pattern.object_var.is_none()
            && self.pattern.predicate_filter.is_none()
            && self.pattern.star_bindings.is_empty()
    }

    /// Resolve this pattern's pushdown predicates (keyed by query variable) to
    /// table columns for the given TriplesMap, producing scan filters. A
    /// variable maps to a column via its predicate IRI; predicates backed by a
    /// RefObjectMap (an IRI join, not a scalar column) are skipped.
    fn build_scan_filters(&self, triples_map: &TriplesMap) -> Vec<crate::r2rml::ScanFilter> {
        let mut out = Vec::new();
        for pd in &self.pattern.scan_filters {
            let pred_iri = if Some(pd.var) == self.pattern.object_var {
                self.pattern.predicate_filter.as_deref()
            } else {
                self.pattern
                    .star_bindings
                    .iter()
                    .find(|(_, v)| *v == pd.var)
                    .map(|(p, _)| p.as_str())
            };
            let Some(pred_iri) = pred_iri else { continue };

            // The predicate's values come from EVERY matching object map, so a
            // file-level prune is only sound when the predicate maps to exactly
            // one scalar object map backed by exactly one column. Otherwise a row
            // could match via a column we didn't prune on — skip the pushdown and
            // let the in-engine FILTER handle it.
            let mut matching = triples_map
                .predicate_object_maps
                .iter()
                .filter(|p| p.predicate_map.as_constant() == Some(pred_iri));
            let (Some(pom), None) = (matching.next(), matching.next()) else {
                continue;
            };
            if matches!(pom.object_map, ObjectMap::RefObjectMap(_)) {
                continue;
            }
            let cols = pom.object_map.referenced_columns();
            let [col] = cols.as_slice() else {
                continue;
            };
            out.push(crate::r2rml::ScanFilter {
                column: (*col).to_string(),
                op: pd.op,
                value: pd.value.clone(),
            });
        }

        // A scalar constant-object equality pushes as a scan filter too
        // (optimization; the operator enforces correctness). IRI constants are
        // operator-enforced only — a FK-key pushdown needs template reversal.
        if let (Some(crate::r2rml::ObjectConstant::Scalar(value)), Some(pred_iri)) = (
            &self.pattern.object_constant,
            self.pattern.predicate_filter.as_deref(),
        ) {
            let mut matching = triples_map
                .predicate_object_maps
                .iter()
                .filter(|p| p.predicate_map.as_constant() == Some(pred_iri));
            if let (Some(pom), None) = (matching.next(), matching.next()) {
                if !matches!(pom.object_map, ObjectMap::RefObjectMap(_)) {
                    if let [col] = pom.object_map.referenced_columns().as_slice() {
                        out.push(crate::r2rml::ScanFilter {
                            column: (*col).to_string(),
                            op: crate::r2rml::ScanCmpOp::Eq,
                            value: value.clone(),
                        });
                    }
                }
            }
        }
        out
    }

    /// Materialize one window of a TriplesMap's produced column batches into
    /// variable assignments. Datatype Sids are resolved once into a
    /// `LiteralEncoder` (not per cell) and the window's batches are materialized
    /// in parallel on the rayon pool. The window (not the whole table) bounds the
    /// resident binding footprint.
    fn materialize_window(
        &self,
        triples_map: &TriplesMap,
        batches: &[ColumnBatch],
        parent_lookups: &HashMap<LookupCacheKey, ParentLookup>,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Vec<Vec<(VarId, Binding)>>> {
        use rayon::prelude::*;
        let encoder = LiteralEncoder::build(triples_map, ctx.active_snapshot);
        let pattern = &self.pattern;
        let per_batch: Vec<Vec<Vec<(VarId, Binding)>>> = batches
            .par_iter()
            .map(|batch| materialize_batch(pattern, triples_map, batch, parent_lookups, &encoder))
            .collect::<Result<Vec<_>>>()?;
        Ok(per_batch.into_iter().flatten().collect())
    }

    /// Index the buffered child rows by their join-key values, so the streamed
    /// produced side can be probed against them. Mirrors the per-child key logic
    /// of the previous probe (poisoned → drop, unbound → wildcard) with the roles
    /// flipped (child indexed, produced streamed).
    fn build_join_plan(
        &self,
        join_vars: &[VarId],
        child_schema: &[VarId],
        child_batch: &Batch,
        ctx: &ExecutionContext<'_>,
    ) -> JoinPlan {
        if join_vars.is_empty() {
            return JoinPlan::Cross;
        }
        let store = ctx.binary_store.as_deref();
        let gv = ctx.graph_view();
        let gv = gv.as_ref();

        let mut full_index: HashMap<Vec<GroupKeyOwned>, Vec<usize>> = HashMap::new();
        let mut partial_rows: Vec<(usize, Vec<Option<GroupKeyOwned>>)> = Vec::new();
        for row_idx in 0..child_batch.len() {
            let mut key: Vec<Option<GroupKeyOwned>> = Vec::with_capacity(join_vars.len());
            let mut all_bound = true;
            let mut poisoned = false;
            for &jv in join_vars {
                let pos = child_schema.iter().position(|&v| v == jv).unwrap();
                let b = &child_batch.column_by_idx(pos).unwrap()[row_idx];
                if b.is_poisoned() {
                    poisoned = true;
                    break;
                }
                if b.is_bound() {
                    key.push(Some(binding_to_group_key_normalized(b, store, gv)));
                } else {
                    all_bound = false;
                    key.push(None);
                }
            }
            if poisoned {
                // A poisoned binding can never match — drop the row.
                continue;
            }
            if all_bound {
                let full: Vec<GroupKeyOwned> = key.into_iter().map(Option::unwrap).collect();
                full_index.entry(full).or_default().push(row_idx);
            } else {
                partial_rows.push((row_idx, key));
            }
        }
        JoinPlan::Hash {
            join_vars: join_vars.to_vec(),
            full_index,
            partial_rows,
        }
    }

    /// Buffer one child batch and set up its streaming scan: resolve the matching
    /// TriplesMap(s), scan each table (and any RefObjectMap parent tables), and
    /// build the per-TriplesMap join plan against the child. Returns `None` when
    /// no TriplesMap matches this pattern (the caller pulls the next child).
    async fn build_progress(
        &mut self,
        ctx: &ExecutionContext<'_>,
        child_batch: Batch,
    ) -> Result<Option<ScanProgress>> {
        let mapping = self
            .mapping
            .as_ref()
            .ok_or_else(|| QueryError::Internal("R2RML mapping not loaded".to_string()))?
            .clone();
        let child_schema = self.child.schema().to_vec();

        // Resolve the TriplesMap(s) for this pattern (same for every child row).
        let triples_maps: Vec<&TriplesMap> = if let Some(ref tm_iri) = self.pattern.triples_map_iri
        {
            let tm = mapping.get(tm_iri).ok_or_else(|| {
                QueryError::InvalidQuery(format!(
                    "TriplesMap '{tm_iri}' not found in R2RML mapping"
                ))
            })?;
            vec![tm]
        } else {
            mapping
                .triples_maps
                .values()
                .filter(|tm| {
                    // class_filter: only maps that produce this class.
                    if let Some(ref class_filter) = self.pattern.class_filter {
                        if !tm.classes().contains(class_filter) {
                            return false;
                        }
                    }
                    // predicate_filter: only maps that have this predicate.
                    if let Some(ref pred_filter) = self.pattern.predicate_filter {
                        let has_pred = tm.predicate_object_maps.iter().any(|pom| {
                            pom.predicate_map.as_constant() == Some(pred_filter.as_str())
                        });
                        if !has_pred {
                            return false;
                        }
                    }
                    true
                })
                .collect()
        };

        if triples_maps.is_empty() {
            return Ok(None);
        }

        let table_provider = ctx.r2rml_table_provider.ok_or_else(|| {
            QueryError::InvalidQuery("R2RML table provider not configured".to_string())
        })?;

        // Join vars (pattern-produced vars the child already binds) are the same
        // for every TriplesMap of this pattern.
        let join_vars: Vec<VarId> = self
            .pattern
            .produced_vars()
            .into_iter()
            .filter(|v| child_schema.contains(v))
            .collect();

        let mut tms: Vec<TmStream> = Vec::with_capacity(triples_maps.len());
        let mut seen: HashSet<String> = HashSet::new();

        for triples_map in &triples_maps {
            if !seen.insert(triples_map.iri.clone()) {
                continue;
            }

            let table_name = triples_map.table_name().ok_or_else(|| {
                QueryError::InvalidQuery("TriplesMap has no logical table".to_string())
            })?;

            // Determine projection columns. For a same-subject star, project the
            // union of columns needed for every star predicate so the whole star
            // is satisfied by one scan.
            let projection: Vec<String> = if self.pattern.star_bindings.is_empty() {
                if self.is_subject_only_pattern() {
                    // rdf:type / subject-only pattern: only the subject columns are
                    // load-bearing. Projecting every POM column (the
                    // `columns_for_predicate(None)` fallback) reads FK/value columns
                    // that subject-only materialization never consults.
                    triples_map
                        .subject_columns()
                        .into_iter()
                        .map(std::string::ToString::to_string)
                        .collect()
                } else {
                    triples_map
                        .columns_for_predicate(self.pattern.predicate_filter.as_deref())
                        .into_iter()
                        .map(std::string::ToString::to_string)
                        .collect()
                }
            } else {
                let mut cols: Vec<String> = Vec::new();
                for pred in self.pattern_predicates() {
                    cols.extend(
                        triples_map
                            .columns_for_predicate(Some(pred))
                            .into_iter()
                            .map(std::string::ToString::to_string),
                    );
                }
                cols.sort();
                cols.dedup();
                cols
            };

            // Scan the table, pushing resolved FILTER predicates for file pruning
            // (column resolution needs the mapping, so it happens here).
            let scan_filters = self.build_scan_filters(triples_map);
            let as_of_t = if ctx.dataset.is_some() {
                None
            } else {
                Some(ctx.to_t)
            };
            // Reuse an already-materialized inner scan across child batches: a
            // correlated join calls `build_progress` once per child batch, so
            // without this the (dimension-sized) inner table is re-scanned every
            // batch. The first scan of a `(table, projection)` is collected (up to
            // one window) and replayed for later batches; a larger inner streams
            // fresh each batch as before.
            //
            // Only unfiltered scans are cached. A pushdown `scan_filter` can prune
            // files, so a filtered scan may yield a row SUBSET; the cache key is
            // `(table, projection)` and does not carry the filter, so replaying a
            // pruned subset for a differently-filtered (or unfiltered) scan of the
            // same table/projection would drop rows. Filtered scans therefore
            // bypass the cache entirely (both read and write).
            //
            // A budgeted scan (under a LIMIT) also bypasses the cache: caching
            // collects a full window before the operator can stop early, which
            // would defeat the LIMIT. A budgeted scan is the topmost
            // row-preserving scan, so it stops after ~a batch and gains little
            // from cross-batch reuse anyway.
            let cacheable =
                scan_cache_enabled() && scan_filters.is_empty() && self.row_budget.is_none();
            let cache_key = (table_name.to_string(), projection.clone());
            let stream: ColumnBatchStream = if !cacheable {
                table_provider
                    .scan_table(
                        &self.pattern.graph_source_id,
                        table_name,
                        &projection,
                        &scan_filters,
                        as_of_t,
                    )
                    .await?
            } else if let Some(cached) = self.scan_cache.get(&cache_key) {
                replay_stream(Arc::clone(cached))
            } else {
                let fresh = table_provider
                    .scan_table(
                        &self.pattern.graph_source_id,
                        table_name,
                        &projection,
                        &scan_filters,
                        as_of_t,
                    )
                    .await?;
                match collect_scan_capped(fresh, materialize_window_rows()).await? {
                    CollectedScan::Complete(batches) => {
                        let arc = Arc::new(batches);
                        self.scan_cache.insert(cache_key, Arc::clone(&arc));
                        replay_stream(arc)
                    }
                    CollectedScan::Overflow(prefix, remainder) => {
                        Box::pin(futures::stream::iter(prefix.into_iter().map(Ok)).chain(remainder))
                    }
                }
            };

            // Build parent lookup tables for RefObjectMap POMs that pass the
            // predicate filter. Parent (dimension) tables are small and consumed
            // whole into the lookup, so they are not streamed.
            let mut parent_lookups: HashMap<LookupCacheKey, ParentLookup> = HashMap::new();
            let star_preds = self.pattern_predicates();
            let filtered_poms: Vec<_> = triples_map
                .predicate_object_maps
                .iter()
                .filter(|pom| {
                    if !self.pattern.star_bindings.is_empty() {
                        pom.predicate_map
                            .as_constant()
                            .is_some_and(|p| star_preds.contains(&p))
                    } else if let Some(ref pred_filter) = self.pattern.predicate_filter {
                        pom.predicate_map.as_constant() == Some(pred_filter.as_str())
                    } else if self.pattern.object_var.is_none() {
                        // rdf:type / subject-only pattern: no POM is load-bearing
                        // (the parent scans it would trigger are pure dead work, as
                        // subject-only materialization never reads object/parent
                        // values). The all-POMs branch below is for a TRUE wildcard
                        // `?s ?p ?o`, where `?p`/`?o` range over every predicate.
                        false
                    } else {
                        true
                    }
                })
                .collect();

            for pom in &filtered_poms {
                if let ObjectMap::RefObjectMap(ref rom) = pom.object_map {
                    let mut parent_join_cols: Vec<String> = rom
                        .parent_columns()
                        .into_iter()
                        .map(std::string::ToString::to_string)
                        .collect();
                    parent_join_cols.sort();
                    let lookup_key: LookupCacheKey =
                        (rom.parent_triples_map.clone(), parent_join_cols.clone());

                    if parent_lookups.contains_key(&lookup_key) {
                        continue;
                    }

                    let parent_tm = match mapping.get(&rom.parent_triples_map) {
                        Some(tm) => tm,
                        None => {
                            tracing::warn!(
                                parent = %rom.parent_triples_map,
                                "Parent TriplesMap not found for RefObjectMap, skipping"
                            );
                            continue;
                        }
                    };

                    let parent_table = match parent_tm.table_name() {
                        Some(name) => name,
                        None => {
                            tracing::warn!(
                                parent = %rom.parent_triples_map,
                                "Parent TriplesMap has no logical table, skipping"
                            );
                            continue;
                        }
                    };

                    // Columns needed from the parent: join columns + subject
                    // template columns (+ rr:column if the subject uses one).
                    let mut parent_projection: Vec<String> = parent_join_cols.clone();
                    parent_projection
                        .extend(parent_tm.subject_map.template_columns.iter().cloned());
                    if let Some(ref col) = parent_tm.subject_map.column {
                        parent_projection.push(col.clone());
                    }
                    parent_projection.sort();
                    parent_projection.dedup();

                    let as_of_t = if ctx.dataset.is_some() {
                        None
                    } else {
                        Some(ctx.to_t)
                    };
                    // Parent (dimension) tables are small; collect the stream
                    // fully into the lookup rather than streaming it.
                    let parent_stream = table_provider
                        .scan_table(
                            &self.pattern.graph_source_id,
                            parent_table,
                            &parent_projection,
                            &[],
                            as_of_t,
                        )
                        .await?;
                    let parent_batches = collect_stream(parent_stream).await?;

                    let lookup = build_parent_lookup(parent_tm, &parent_join_cols, parent_batches)?;
                    parent_lookups.insert(lookup_key, lookup);
                }
            }

            let join = self.build_join_plan(&join_vars, &child_schema, &child_batch, ctx);

            tms.push(TmStream {
                tm_iri: triples_map.iri.clone(),
                stream,
                exhausted: false,
                parent_lookups,
                join,
            });
        }

        // Under a LIMIT, cap the materialize window at the remaining budget so a
        // `LIMIT n` does not explode a full 512K-row window into bindings before
        // the first output row.
        // Cap the window to the remaining LIMIT budget. This holds for a
        // consumed filter too: the budget counts *matching* rows while a window
        // materializes unfiltered rows, but the `next_batch` loop re-checks the
        // post-filter `emitted` and keeps pulling more windows until the budget
        // is met, so a bounded window can never under-return — it only avoids
        // materializing a full window before the filter runs.
        let window_rows = match self.row_budget {
            Some(b) => materialize_window_rows().min(b.saturating_sub(self.emitted).max(1)),
            None => materialize_window_rows(),
        };

        Ok(Some(ScanProgress {
            child_batch,
            child_schema,
            tms,
            tm_idx: 0,
            window_rows,
        }))
    }

    /// Pull and materialize the next window from the in-flight scan and emit its
    /// rows. Each call pulls one bounded window of batches from the current
    /// TriplesMap's stream (so only O(window + in-flight files) is resident),
    /// materializes them in parallel, emits, and drops the window. Returns `true`
    /// while batches remain, `false` once the scan for this child batch is fully
    /// consumed.
    async fn advance_one_window(
        &mut self,
        ctx: &ExecutionContext<'_>,
        progress: &mut ScanProgress,
        num_cols: usize,
        columns: &mut [Vec<Binding>],
    ) -> Result<bool> {
        let mapping = self
            .mapping
            .as_ref()
            .ok_or_else(|| QueryError::Internal("R2RML mapping not loaded".to_string()))?
            .clone();

        while progress.tm_idx < progress.tms.len() {
            let i = progress.tm_idx;
            if progress.tms[i].exhausted {
                progress.tm_idx += 1;
                continue;
            }

            // Pull a window of batches from the stream: at least one, then up to
            // the row budget. The stream itself bounds in-flight file decodes.
            let mut window: Vec<ColumnBatch> = Vec::new();
            let mut rows = 0usize;
            while rows < progress.window_rows {
                match progress.tms[i].stream.next().await {
                    Some(batch) => {
                        let batch = batch?;
                        rows += batch.num_rows;
                        window.push(batch);
                    }
                    None => {
                        progress.tms[i].exhausted = true;
                        break;
                    }
                }
            }

            if window.is_empty() {
                // Stream ended with nothing left for this TriplesMap.
                progress.tm_idx += 1;
                continue;
            }

            let triples_map = mapping.get(&progress.tms[i].tm_iri).ok_or_else(|| {
                QueryError::Internal(format!(
                    "TriplesMap '{}' missing from mapping mid-scan",
                    progress.tms[i].tm_iri
                ))
            })?;
            let produced = self.materialize_window(
                triples_map,
                &window,
                &progress.tms[i].parent_lookups,
                ctx,
            )?;

            if !produced.is_empty() {
                emit_produced_window(
                    &self.out_pos,
                    &progress.child_schema,
                    &progress.child_batch,
                    &progress.tms[i].join,
                    &produced,
                    num_cols,
                    ctx.batch_size,
                    columns,
                    &mut self.pending,
                    ctx,
                )?;
            }
            // Geometric window growth. A budgeted (LIMIT) scan starts with a small
            // window (~the remaining budget) so a selective query does not explode
            // a full window into bindings before the first output row. But when the
            // produced rows feed an internal join that filters most of them out,
            // the output budget is never met, and a fixed tiny window would re-scan
            // the whole table in many small passes (slower than the un-budgeted
            // full-window path — fluree/db#1406 review). Growing the window each
            // pass ramps it up to the full materialize size after a handful of
            // low-yield passes, so the pathological case self-corrects while a
            // genuinely selective LIMIT still stops after its cheap first window.
            // The un-budgeted window already starts at the full size, so `.min`
            // makes this a no-op there.
            progress.window_rows = progress
                .window_rows
                .saturating_mul(4)
                .min(materialize_window_rows());
            // `window` is dropped here, freeing the batches before the next pull.
            return Ok(true);
        }
        Ok(false)
    }
}

/// Drain a [`ColumnBatchStream`] fully into a vector. Used for small dimension
/// (parent) tables whose entire contents become a lookup.
async fn collect_stream(mut stream: ColumnBatchStream) -> Result<Vec<ColumnBatch>> {
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        out.push(batch?);
    }
    Ok(out)
}

/// Whether the correlated inner-scan cache is enabled. Read once from
/// `FLUREE_R2RML_SCAN_CACHE` (only `0`/`false`/`off` disable it); disabling
/// restores the per-child-batch re-scan behavior.
fn scan_cache_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("FLUREE_R2RML_SCAN_CACHE") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off"
        ),
        Err(_) => true,
    })
}

/// Outcome of trying to fully collect an inner scan for caching.
enum CollectedScan {
    /// The whole inner fit within the cap — safe to cache and replay.
    Complete(Vec<ColumnBatch>),
    /// The inner exceeded the cap — too large to cache. The prefix already
    /// pulled plus the still-open remainder serve this one batch.
    Overflow(Vec<ColumnBatch>, ColumnBatchStream),
}

/// Collect `stream` until it ends (→ `Complete`, cacheable) or its row count
/// reaches `cap` with more remaining (→ `Overflow`, too large to cache). The cap
/// equals one materialize window, so a cached inner never exceeds the resident
/// footprint a single scan window already materializes.
async fn collect_scan_capped(mut stream: ColumnBatchStream, cap: usize) -> Result<CollectedScan> {
    let mut collected = Vec::new();
    let mut rows = 0usize;
    while rows < cap {
        match stream.next().await {
            Some(batch) => {
                let batch = batch?;
                rows += batch.num_rows;
                collected.push(batch);
            }
            None => return Ok(CollectedScan::Complete(collected)),
        }
    }
    Ok(CollectedScan::Overflow(collected, stream))
}

/// A [`ColumnBatchStream`] that replays cached batches. `ColumnBatch` clones are
/// cheap (its columns are `Arc`-backed), so replay does not re-copy the data.
fn replay_stream(batches: Arc<Vec<ColumnBatch>>) -> ColumnBatchStream {
    Box::pin(futures::stream::iter(
        (0..batches.len()).map(move |i| Ok(batches[i].clone())),
    ))
}

/// Emit one combined output row: the child row's bindings overlaid with a
/// produced assignment, into `columns` (or `pending` once the batch is full).
///
/// The common (not-yet-full) path writes straight into the columnar buffers
/// instead of allocating a per-row `Vec<Binding>` — that per-row allocation was
/// the single largest heap-allocation site for analytical R2RML scans.
#[allow(clippy::too_many_arguments)]
fn emit_combined_row(
    out_pos: &HashMap<VarId, usize>,
    child_schema: &[VarId],
    child_batch: &Batch,
    child_row_idx: usize,
    prod: &[(VarId, Binding)],
    num_cols: usize,
    batch_size: usize,
    columns: &mut [Vec<Binding>],
    pending: &mut VecDeque<Vec<Binding>>,
) {
    if columns[0].len() < batch_size {
        // Push an `Unbound` placeholder to every column, then overwrite the
        // bound positions in place — no per-row temporary vector.
        for col in columns.iter_mut() {
            col.push(Binding::Unbound);
        }
        for (col_idx, &var) in child_schema.iter().enumerate() {
            let out_idx = *out_pos.get(&var).unwrap();
            *columns[out_idx].last_mut().unwrap() =
                child_batch.column_by_idx(col_idx).unwrap()[child_row_idx].clone();
        }
        for (var, binding) in prod {
            *columns[*out_pos.get(var).unwrap()].last_mut().unwrap() = binding.clone();
        }
    } else {
        // Overflow path: the batch is full, so stage a complete row for `pending`.
        let mut out_row: Vec<Binding> = vec![Binding::Unbound; num_cols];
        for (col_idx, &var) in child_schema.iter().enumerate() {
            out_row[*out_pos.get(&var).unwrap()] =
                child_batch.column_by_idx(col_idx).unwrap()[child_row_idx].clone();
        }
        for (var, binding) in prod {
            out_row[*out_pos.get(var).unwrap()] = binding.clone();
        }
        pending.push_back(out_row);
    }
}

/// Combine a window of produced rows with the buffered child rows per the join
/// plan, emitting into `columns`/`pending`. The produced side is the streamed
/// (large) side; the child index was built once in `build_progress`.
#[allow(clippy::too_many_arguments)]
fn emit_produced_window(
    out_pos: &HashMap<VarId, usize>,
    child_schema: &[VarId],
    child_batch: &Batch,
    join: &JoinPlan,
    produced: &[Vec<(VarId, Binding)>],
    num_cols: usize,
    batch_size: usize,
    columns: &mut [Vec<Binding>],
    pending: &mut VecDeque<Vec<Binding>>,
    ctx: &ExecutionContext<'_>,
) -> Result<()> {
    let mut emit = |child_row_idx: usize, prod: &[(VarId, Binding)]| -> Result<()> {
        ctx.tracker.consume_fuel(1)?;
        emit_combined_row(
            out_pos,
            child_schema,
            child_batch,
            child_row_idx,
            prod,
            num_cols,
            batch_size,
            columns,
            pending,
        );
        Ok(())
    };

    match join {
        JoinPlan::Cross => {
            // No shared vars: every produced row pairs with every child row
            // (child is usually the single seed row).
            for prod in produced {
                for child_row_idx in 0..child_batch.len() {
                    emit(child_row_idx, prod)?;
                }
            }
        }
        JoinPlan::Hash {
            join_vars,
            full_index,
            partial_rows,
        } => {
            let store = ctx.binary_store.as_deref();
            let gv = ctx.graph_view();
            let gv = gv.as_ref();
            for prod in produced {
                // A produced row always binds every pattern var, so its join key
                // is complete.
                let pkey: Vec<GroupKeyOwned> = join_vars
                    .iter()
                    .filter_map(|jv| {
                        prod.iter()
                            .find(|(v, _)| v == jv)
                            .map(|(_, b)| binding_to_group_key_normalized(b, store, gv))
                    })
                    .collect();
                if pkey.len() != join_vars.len() {
                    continue;
                }
                // Fully-bound child rows: exact hash probe.
                if let Some(rows) = full_index.get(&pkey) {
                    for &child_row_idx in rows {
                        emit(child_row_idx, prod)?;
                    }
                }
                // Child rows with an unbound (wildcard) join var: match those that
                // agree on every bound position.
                for (child_row_idx, partial) in partial_rows {
                    let agrees = partial
                        .iter()
                        .zip(pkey.iter())
                        .all(|(c, p)| c.as_ref().is_none_or(|c| c == p));
                    if agrees {
                        emit(*child_row_idx, prod)?;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Datatype Sids resolved once per scan instead of per literal cell — the
/// per-cell `encode_iri` was a large share of materialization cost. Shared with
/// the fused-aggregate operator so its filter/expression eval-var bindings are
/// encoded identically to the normal materialization path.
pub(crate) struct LiteralEncoder {
    dt_sids: HashMap<String, fluree_db_core::Sid>,
    xsd_string: fluree_db_core::Sid,
}

impl LiteralEncoder {
    pub(crate) fn build(
        triples_map: &TriplesMap,
        snapshot: &fluree_db_core::LedgerSnapshot,
    ) -> Self {
        let fallback = fluree_db_core::Sid::new(2, "string");
        let mut dt_sids: HashMap<String, fluree_db_core::Sid> = HashMap::new();
        for pom in &triples_map.predicate_object_maps {
            if let Some(dt) = object_map_datatype(&pom.object_map) {
                dt_sids
                    .entry(dt.to_string())
                    .or_insert_with(|| snapshot.encode_iri(dt).unwrap_or_else(|| fallback.clone()));
            }
        }
        let xsd_string = snapshot.encode_iri(xsd::STRING).unwrap_or(fallback);
        Self {
            dt_sids,
            xsd_string,
        }
    }

    /// Convert an RdfTerm to a Binding without touching the snapshot (datatype
    /// Sids are pre-resolved). IRIs are kept as raw strings — graph source IRIs
    /// are independent of any Fluree namespace table.
    pub(crate) fn encode(&self, term: &RdfTerm) -> Binding {
        use fluree_db_core::FlakeValue;
        use fluree_vocab::UnresolvedDatatypeConstraint as Udc;
        match term {
            RdfTerm::Iri(iri) => Binding::iri(iri.as_str()),
            RdfTerm::BlankNode(id) => Binding::iri(format!("_:{id}")),
            RdfTerm::Literal { value, dtc } => match dtc {
                Some(Udc::LangTag(lang)) => {
                    Binding::lit_lang(FlakeValue::String(value.clone()), lang.as_ref())
                }
                Some(Udc::Explicit(dt_iri)) => {
                    let dt_sid = self
                        .dt_sids
                        .get(dt_iri.as_ref())
                        .cloned()
                        .unwrap_or_else(|| self.xsd_string.clone());
                    // Coerce numeric XSD literals from string to typed FlakeValue
                    // (arithmetic reads the value, not the datatype Sid);
                    // non-numeric datatypes keep their string form.
                    let val = match fluree_db_core::coerce_value(
                        FlakeValue::String(value.clone()),
                        dt_iri.as_ref(),
                    ) {
                        Ok(
                            c @ (FlakeValue::Long(_)
                            | FlakeValue::Double(_)
                            | FlakeValue::BigInt(_)
                            | FlakeValue::Decimal(_)),
                        ) => c,
                        _ => FlakeValue::String(value.clone()),
                    };
                    Binding::lit(val, dt_sid)
                }
                _ => Binding::lit(FlakeValue::String(value.clone()), self.xsd_string.clone()),
            },
        }
    }
}

/// Datatype IRI declared by an ObjectMap, if any (column/template/constant).
fn object_map_datatype(om: &ObjectMap) -> Option<&str> {
    use fluree_db_r2rml::mapping::ConstantValue;
    match om {
        ObjectMap::Column { datatype, .. } | ObjectMap::Template { datatype, .. } => {
            datatype.as_deref()
        }
        ObjectMap::Constant {
            value: ConstantValue::Literal { datatype, .. },
        } => datatype.as_deref(),
        _ => None,
    }
}

/// Materialize the object term for one POM at a table row, resolving a
/// RefObjectMap through the pre-built parent lookup. Free fn so it runs off the
/// operator inside a rayon worker.
fn materialize_pom_object(
    pom: &PredicateObjectMap,
    iceberg_batch: &ColumnBatch,
    table_row_idx: usize,
    parent_lookups: &HashMap<(String, Vec<String>), ParentLookup>,
) -> Result<Option<RdfTerm>> {
    if let ObjectMap::RefObjectMap(ref rom) = pom.object_map {
        let child_columns: Vec<String> = rom
            .child_columns()
            .into_iter()
            .map(std::string::ToString::to_string)
            .collect();
        let child_key = match get_join_key_from_batch(&child_columns, iceberg_batch, table_row_idx)
        {
            Some(k) => k,
            None => return Ok(None),
        };
        let mut parent_join_cols: Vec<String> = rom
            .parent_columns()
            .into_iter()
            .map(std::string::ToString::to_string)
            .collect();
        parent_join_cols.sort();
        let lookup_key = (rom.parent_triples_map.clone(), parent_join_cols);
        Ok(parent_lookups
            .get(&lookup_key)
            .and_then(|l| l.get(&child_key))
            .cloned())
    } else {
        Ok(materialize_object_from_batch(
            &pom.object_map,
            iceberg_batch,
            table_row_idx,
        )?)
    }
}

/// Whether a materialized object term equals a constant-object constraint.
/// IRI constants match exactly; literal (scalar) constants are loose-matched
/// (gated in `convert_triple_to_r2rml`), comparing the value and ignoring the
/// materialized term's datatype/language.
///
/// Integer comparison is EXACT (parse to `i64`, no `f64`): a float compare would
/// both admit false positives across adjacent large integers and let the
/// operator keep a lexical form (`"2024.0"`) that the Arrow scan filter would
/// drop, breaking the invariant that pushdown never removes an operator-kept row.
fn rdf_term_eq_object_constant(term: &RdfTerm, constant: &crate::r2rml::ObjectConstant) -> bool {
    use crate::r2rml::{ObjectConstant, ScanValue};
    match constant {
        // Bound IRI / ref object: exact IRI match.
        ObjectConstant::Iri(iri) => matches!(term, RdfTerm::Iri(v) if v == iri),
        // Literal object: loose value match, ignoring datatype/language.
        ObjectConstant::Scalar(value) => {
            let RdfTerm::Literal { value: v, .. } = term else {
                return false;
            };
            match value {
                ScanValue::Str(s) => v == s,
                ScanValue::Int(n) => v.parse::<i64>().is_ok_and(|x| x == *n),
                ScanValue::Bool(b) => match v.as_str() {
                    "true" | "1" => *b,
                    "false" | "0" => !*b,
                    _ => false,
                },
                // Date constant objects are not produced by convert yet.
                ScanValue::Date(_) => false,
            }
        }
    }
}

/// Whether a materialized subject term equals a constant (bound) subject IRI.
/// Subject maps always produce IRIs, so a non-IRI term never matches.
fn subject_term_matches_iri(term: &RdfTerm, want: &str) -> bool {
    matches!(term, RdfTerm::Iri(v) if v == want)
}

/// Materialize one column batch into produced variable assignments (subject +
/// object vars) — the per-batch unit of the parallel scan. Mirrors the previous
/// per-row logic (star cross product, subject-only, single-object).
fn materialize_batch(
    pattern: &R2rmlPattern,
    triples_map: &TriplesMap,
    iceberg_batch: &ColumnBatch,
    parent_lookups: &HashMap<(String, Vec<String>), ParentLookup>,
    encoder: &LiteralEncoder,
) -> Result<Vec<Vec<(VarId, Binding)>>> {
    let mut produced: Vec<Vec<(VarId, Binding)>> = Vec::new();
    for table_row_idx in 0..iceberg_batch.num_rows {
        let subject_term = match materialize_subject_from_batch(
            &triples_map.subject_map,
            iceberg_batch,
            table_row_idx,
        )? {
            Some(t) => t,
            None => continue,
        };

        // Bound-subject filter (`<store/5> <pred> ?o`): keep only rows whose
        // subject IRI equals the constant. This is the pattern's semantics,
        // enforced regardless of any scan pushdown.
        if let Some(want) = pattern.subject_constant.as_deref() {
            if !subject_term_matches_iri(&subject_term, want) {
                continue;
            }
        }
        let subject_binding = encoder.encode(&subject_term);

        // Seed a fresh output row with the subject binding, or an empty row when
        // the subject is a constant (which binds no variable).
        let seed_row = || -> Vec<(VarId, Binding)> {
            match pattern.subject_var {
                Some(sv) => vec![(sv, subject_binding.clone())],
                None => Vec::new(),
            }
        };

        if !pattern.star_bindings.is_empty() {
            let mut members: Vec<(VarId, &str)> = Vec::new();
            if let (Some(ov), Some(pf)) = (pattern.object_var, pattern.predicate_filter.as_deref())
            {
                members.push((ov, pf));
            }
            for (pred, var) in &pattern.star_bindings {
                members.push((*var, pred.as_str()));
            }

            let mut binding_lists: Vec<(VarId, Vec<Binding>)> = Vec::with_capacity(members.len());
            let mut row_ok = true;
            for (var, pred) in &members {
                let mut vals: Vec<Binding> = Vec::new();
                for pom in triples_map
                    .predicate_object_maps
                    .iter()
                    .filter(|p| p.predicate_map.as_constant() == Some(*pred))
                {
                    if let Some(t) =
                        materialize_pom_object(pom, iceberg_batch, table_row_idx, parent_lookups)?
                    {
                        vals.push(encoder.encode(&t));
                    }
                }
                if vals.is_empty() {
                    row_ok = false;
                    break;
                }
                binding_lists.push((*var, vals));
            }
            if !row_ok {
                continue;
            }

            let mut rows: Vec<Vec<(VarId, Binding)>> = vec![seed_row()];
            for (var, vals) in &binding_lists {
                if vals.len() == 1 {
                    for r in &mut rows {
                        r.push((*var, vals[0].clone()));
                    }
                } else {
                    let mut next = Vec::with_capacity(rows.len() * vals.len());
                    for r in &rows {
                        for v in vals {
                            let mut nr = r.clone();
                            nr.push((*var, v.clone()));
                            next.push(nr);
                        }
                    }
                    rows = next;
                }
            }
            produced.extend(rows);
            continue;
        }

        let Some(obj_var) = pattern.object_var else {
            // Constant-object (`?s <pred> "value"`): keep the subject only when
            // this predicate has an object equal to the required constant. The
            // equality is the pattern's semantics, so it is enforced here
            // regardless of scan pushdown; the pushed ScanFilter is only an
            // optimization on top.
            if let Some(required) = &pattern.object_constant {
                let mut matched = false;
                for pom in triples_map.predicate_object_maps.iter().filter(|pom| {
                    pattern
                        .predicate_filter
                        .as_deref()
                        .is_some_and(|pf| pom.predicate_map.as_constant() == Some(pf))
                }) {
                    if let Some(t) =
                        materialize_pom_object(pom, iceberg_batch, table_row_idx, parent_lookups)?
                    {
                        if rdf_term_eq_object_constant(&t, required) {
                            matched = true;
                            break;
                        }
                    }
                }
                if matched {
                    produced.push(seed_row());
                }
                continue;
            }
            produced.push(seed_row());
            continue;
        };

        for pom in triples_map.predicate_object_maps.iter().filter(|pom| {
            pattern
                .predicate_filter
                .as_deref()
                .is_none_or(|pf| pom.predicate_map.as_constant() == Some(pf))
        }) {
            if let Some(t) =
                materialize_pom_object(pom, iceberg_batch, table_row_idx, parent_lookups)?
            {
                let object_binding = encoder.encode(&t);
                let mut row = seed_row();
                row.push((obj_var, object_binding));
                produced.push(row);
            }
        }
    }
    Ok(produced)
}

/// Build a parent lookup table for RefObjectMap joins.
///
/// Scans the parent TriplesMap's table and builds a HashMap mapping
/// parent join key → parent subject IRI.
///
/// # Arguments
///
/// * `parent_tm` - The parent TriplesMap
/// * `parent_columns` - Column names used in join conditions (from parent side)
/// * `batches` - Column batches from scanning the parent table
///
/// # Returns
///
/// HashMap mapping join key (as `Vec<String>`) to parent subject `RdfTerm`.
fn build_parent_lookup(
    parent_tm: &TriplesMap,
    parent_columns: &[String],
    batches: Vec<ColumnBatch>,
) -> Result<ParentLookup> {
    let mut lookup = ParentLookup::new();

    for batch in batches {
        for row_idx in 0..batch.num_rows {
            // Materialize parent subject
            let subject_term =
                match materialize_subject_from_batch(&parent_tm.subject_map, &batch, row_idx) {
                    Ok(Some(term)) => term,
                    Ok(None) => continue, // Null subject - skip
                    Err(e) => {
                        tracing::warn!(
                            parent_tm = %parent_tm.iri,
                            row_idx,
                            error = %e,
                            "Failed to materialize parent subject, skipping row"
                        );
                        continue;
                    }
                };

            // Extract join key from parent row
            let key = match get_join_key_from_batch(parent_columns, &batch, row_idx) {
                Some(k) => k,
                None => continue, // Null in join key - skip
            };

            // Insert into lookup (last wins for duplicate keys)
            lookup.insert(key, subject_term);
        }
    }

    tracing::debug!(
        parent_tm = %parent_tm.iri,
        lookup_size = lookup.len(),
        "Built parent lookup table for RefObjectMap join"
    );

    Ok(lookup)
}

#[async_trait]
impl Operator for R2rmlScanOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    fn set_row_budget(&mut self, budget: usize) {
        // Record the budget but do NOT forward it to the child: the child feeds
        // this operator's correlated scan/join, which is not row-preserving, so
        // an inner scan must still produce every row the join needs. Only the
        // topmost row-preserving scan is budgeted — `LimitOperator` forwards a
        // budget solely through row-preserving operators, so if this operator
        // received one, its output flows 1:1 to the LIMIT.
        if limit_pushdown_enabled() {
            self.row_budget = Some(budget);
        }
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        // Open child first
        self.child.open(ctx).await?;
        self.emitted = 0;

        // Load the compiled mapping from the provider
        let provider = ctx
            .r2rml_provider
            .ok_or_else(|| QueryError::InvalidQuery("R2RML provider not configured".to_string()))?;

        // IMPORTANT: In dataset mode, there is no meaningful dataset-level `to_t`.
        // Passing `None` avoids inventing a cross-ledger time and lets the provider
        // select the latest snapshot (or apply its own semantics).
        let as_of_t = if ctx.dataset.is_some() {
            None
        } else {
            Some(ctx.to_t)
        };
        let mapping = provider
            .compiled_mapping(&self.pattern.graph_source_id, as_of_t)
            .await?;

        self.mapping = Some(mapping);
        self.state = OperatorState::Open;

        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state == OperatorState::Exhausted {
            return Ok(None);
        }

        let num_cols = self.schema.len();
        let mut columns: Vec<Vec<Binding>> = (0..num_cols)
            .map(|_| Vec::with_capacity(ctx.batch_size))
            .collect();

        loop {
            // 1. Drain overflow from a prior window before doing more work.
            while !self.pending.is_empty() && columns[0].len() < ctx.batch_size {
                let row = self.pending.pop_front().unwrap();
                for (col_idx, binding) in row.into_iter().enumerate() {
                    columns[col_idx].push(binding);
                }
            }
            // Emit once a full batch is accumulated, or once the LIMIT budget is
            // (optimistically) met. With a consumed filter the pre-filter count
            // over-estimates matches, so this only triggers an emit *attempt* —
            // `finalize_batch` filters and `emitted` counts the survivors; if
            // that leaves the budget unmet the loop keeps pulling. Enabling it
            // for the consumed case is what stops the scan after ~one window
            // instead of accumulating a full `batch_size` before filtering.
            let budget_met = self
                .row_budget
                .is_some_and(|b| self.emitted + columns[0].len() >= b);
            if columns[0].len() >= ctx.batch_size || (budget_met && !columns[0].is_empty()) {
                // Fast path (no consumed filter): emit the accumulated columns
                // directly, exactly as before — no extra allocation.
                if self.consumed_filter.is_none() {
                    self.emitted += columns[0].len();
                    if self.row_budget.is_some_and(|b| self.emitted >= b) {
                        self.state = OperatorState::Exhausted;
                    }
                    return Ok(Some(Batch::new(Arc::clone(&self.schema), columns)?));
                }
                // Consumed-filter path: filter this window, count matching rows,
                // and keep pulling if the whole window is filtered out.
                let taken = std::mem::replace(
                    &mut columns,
                    (0..num_cols)
                        .map(|_| Vec::with_capacity(ctx.batch_size))
                        .collect(),
                );
                if let Some(out) = self.finalize_batch(taken, ctx)? {
                    self.emitted += out.len();
                    if self.row_budget.is_some_and(|b| self.emitted >= b) {
                        self.state = OperatorState::Exhausted;
                    }
                    return Ok(Some(out));
                }
                continue;
            }

            // 2. Advance an in-flight scan by one materialization window. The
            //    window's rows fill `columns` (overflow spills to `pending`), so
            //    the whole table is never materialized at once.
            if let Some(mut progress) = self.progress.take() {
                let more = self
                    .advance_one_window(ctx, &mut progress, num_cols, &mut columns)
                    .await?;
                if more {
                    self.progress = Some(progress);
                }
                continue;
            }

            // 3. No scan in flight: pull the next child batch and start one.
            match self.child.next_batch(ctx).await? {
                Some(child_batch) => {
                    if let Some(progress) = self.build_progress(ctx, child_batch).await? {
                        self.progress = Some(progress);
                    }
                    continue;
                }
                None => {
                    if columns[0].is_empty() {
                        self.state = OperatorState::Exhausted;
                        return Ok(None);
                    }
                    // Fast path (no consumed filter): emit directly, unchanged.
                    if self.consumed_filter.is_none() {
                        self.emitted += columns[0].len();
                        return Ok(Some(Batch::new(Arc::clone(&self.schema), columns)?));
                    }
                    // The child is exhausted, so this is the terminal batch
                    // whether or not any row survives the consumed filter.
                    self.state = OperatorState::Exhausted;
                    let taken = std::mem::take(&mut columns);
                    if let Some(out) = self.finalize_batch(taken, ctx)? {
                        self.emitted += out.len();
                        return Ok(Some(out));
                    }
                    return Ok(None);
                }
            }
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.mapping = None;
        self.pending.clear();
        self.progress = None;
        self.scan_cache.clear();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Could use Iceberg table statistics in the future
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::r2rml::{ObjectConstant, ScanValue};
    use fluree_db_r2rml::materialize::RdfTerm;

    #[test]
    fn object_constant_matching() {
        // IRI constant: exact IRI match only.
        let iri = ObjectConstant::Iri("http://ex/geo/1".to_string());
        assert!(rdf_term_eq_object_constant(
            &RdfTerm::iri("http://ex/geo/1"),
            &iri
        ));
        assert!(!rdf_term_eq_object_constant(
            &RdfTerm::iri("http://ex/geo/2"),
            &iri
        ));
        assert!(!rdf_term_eq_object_constant(
            &RdfTerm::string("http://ex/geo/1"),
            &iri
        ));

        // String constant: loose lexical match, datatype/language-agnostic — a
        // plain-string query object matches a lang-tagged materialized literal.
        let s = ObjectConstant::Scalar(ScanValue::Str("chat".to_string()));
        assert!(rdf_term_eq_object_constant(&RdfTerm::string("chat"), &s));
        assert!(rdf_term_eq_object_constant(
            &RdfTerm::lang_string("chat", "fr"),
            &s
        ));
        assert!(!rdf_term_eq_object_constant(&RdfTerm::string("dog"), &s));
        assert!(!rdf_term_eq_object_constant(&RdfTerm::iri("chat"), &s));

        // Integer constant: EXACT — "2024" matches; a decimal lexical does not
        // (it would break the pushdown invariant on a string-backed column).
        let n = ObjectConstant::Scalar(ScanValue::Int(2024));
        assert!(rdf_term_eq_object_constant(&RdfTerm::string("2024"), &n));
        assert!(!rdf_term_eq_object_constant(&RdfTerm::string("2024.0"), &n));
        assert!(!rdf_term_eq_object_constant(&RdfTerm::string("2025"), &n));
        // Large-integer boundary: f64 rounds these two together; exact i64 must
        // keep them distinct (no false positive).
        let big = ObjectConstant::Scalar(ScanValue::Int(9_007_199_254_740_993));
        assert!(rdf_term_eq_object_constant(
            &RdfTerm::string("9007199254740993"),
            &big
        ));
        assert!(!rdf_term_eq_object_constant(
            &RdfTerm::string("9007199254740992"),
            &big
        ));

        // Boolean constant: true/1 vs false/0.
        let b = ObjectConstant::Scalar(ScanValue::Bool(true));
        assert!(rdf_term_eq_object_constant(&RdfTerm::string("true"), &b));
        assert!(rdf_term_eq_object_constant(&RdfTerm::string("1"), &b));
        assert!(!rdf_term_eq_object_constant(&RdfTerm::string("false"), &b));
    }

    #[test]
    fn bound_subject_matching() {
        // Subject maps always yield IRIs: exact IRI match, never a literal.
        assert!(subject_term_matches_iri(
            &RdfTerm::iri("http://ex/store/5"),
            "http://ex/store/5"
        ));
        assert!(!subject_term_matches_iri(
            &RdfTerm::iri("http://ex/store/50"),
            "http://ex/store/5"
        ));
        assert!(!subject_term_matches_iri(
            &RdfTerm::string("http://ex/store/5"),
            "http://ex/store/5"
        ));
    }
}
