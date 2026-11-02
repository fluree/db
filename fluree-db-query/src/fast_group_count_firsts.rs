use crate::binding::Binding;
use crate::context::{ExecutionContext, WellKnownDatatypes};
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    allow_cursor_fast_path, build_psot_cursor_for_predicate, normalize_pred_sid,
    subject_ref_to_s_id,
};
use crate::fast_path_outcome::{stamp_fast_path, FastPathFallback, FastPathOutcome};
use crate::index_scope::{IndexScope, MemberSet};
use crate::ir::triple::{Ref, Term};
use crate::merged_cursor::MergedRangeCursor;
use crate::operator::BoxedOperator;
use crate::operator::{Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::leaf::{
    decode_leaf_dir_v3_with_base, decode_leaf_header_v3, LeafletDirEntryV3,
};
use fluree_db_binary_index::format::run_record_v2::{
    cmp_v2_for_order, read_ordered_key_v2, RunRecordV2,
};
use fluree_db_binary_index::read::column_loader::{
    load_leaflet_columns, load_leaflet_columns_cached,
};
use fluree_db_binary_index::{
    BinaryCursor, BinaryGraphView, BinaryIndexStore, ColumnBatch, ColumnProjection, ColumnSet,
    RunSortOrder,
};
use fluree_db_core::o_type::OType;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::{FlakeValue, GraphId, LedgerSnapshot, QueryCancellation, Sid};
use rustc_hash::FxHashMap;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Shared free functions
// ---------------------------------------------------------------------------

#[inline]
fn check_cancelled(cancellation: &QueryCancellation) -> Result<()> {
    match cancellation.reason() {
        Some(reason) => Err(QueryError::Cancelled { reason }),
        None => Ok(()),
    }
}

// ---------------------------------------------------------------------------
// Operator 1: PredicateGroupCountFirstsOperator
// ---------------------------------------------------------------------------

/// Fast-path: `?s <p> ?o GROUP BY ?o (COUNT(?s) AS ?count)` with `ORDER BY DESC(?count) LIMIT k`.
///
/// Uses only per-leaflet uncompressed "FIRST" headers to skip decoding entire leaflets when
/// `FIRST(i).(p,o) == FIRST(i+1).(p,o)` (boundary-equality implies the whole leaflet is that (p,o) in POST order).
///
/// If stats indicate this predicate has exactly one datatype for this graph, the operator never
/// decodes Region 2 (dt/lang are treated as constant). Otherwise it falls back to decoding Region 2
/// and grouping by full RDF literal identity (dt/lang).
///
/// Requires:
/// - POST order access
pub struct PredicateGroupCountFirstsOperator {
    /// Output schema: [object_var, count_var]
    schema: Arc<[VarId]>,
    subject_var: VarId,
    object_var: VarId,
    count_var: VarId,
    /// Bound predicate reference (Sid or Iri).
    predicate: crate::ir::triple::Ref,
    /// LIMIT k (top-k by count)
    limit: usize,
    /// Operator state
    state: OperatorState,
    /// Fallback operator for non-binary / overlay / policy / history contexts.
    fallback: Option<BoxedOperator>,
    /// V6 results: (o_type, o_key, count).
    results_v6: Option<Vec<(u16, u64, i64)>>,
    /// Next result to emit
    pos: usize,
    /// The graph whose view decodes the results.
    emit_g_id: GraphId,
    /// Emit IRI objects decoded: over several graphs no graph view exists
    /// downstream to decode an encoded subject against.
    decode_refs: bool,
    /// Temporal mode captured at planner-time for the fallback per-row scan.
    mode: crate::temporal_mode::TemporalMode,
}

impl PredicateGroupCountFirstsOperator {
    pub fn new(
        subject_var: VarId,
        object_var: VarId,
        count_var: VarId,
        predicate: crate::ir::triple::Ref,
        limit: usize,
        mode: crate::temporal_mode::TemporalMode,
    ) -> Self {
        Self {
            schema: Arc::from(vec![object_var, count_var].into_boxed_slice()),
            subject_var,
            object_var,
            count_var,
            predicate,
            limit: limit.max(1),
            state: OperatorState::Created,
            fallback: None,
            results_v6: None,
            pos: 0,
            emit_g_id: 0,
            decode_refs: false,
            mode,
        }
    }

    /// The grouped count over a default graph of several graphs, stamped at its
    /// own site; `None` (with the reason stamped) to fall back.
    fn group_union(&self, ctx: &ExecutionContext<'_>, set: &MemberSet<'_>) -> Option<TopKGroups> {
        let outcome = if set.all_current() {
            match group_count_union(
                set,
                &self.predicate,
                self.limit,
                &ctx.tracker,
                &ctx.cancellation,
            ) {
                Ok(outcome) => outcome,
                // As on one graph: what the V6 path can't give falls back.
                Err(_) => Err(FastPathFallback::GateDeclined),
            }
        } else {
            Err(FastPathFallback::Novelty)
        };
        stamp_fast_path(
            GROUP_BY_OBJECT_COUNT_TOPK_UNION_SITE,
            match outcome {
                Ok(_) => FastPathOutcome::Proceed,
                Err(reason) => FastPathOutcome::Fallback(reason),
            },
        );
        outcome.ok()
    }

    async fn open_fallback(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        use crate::dataset_operator::DatasetOperator;
        use crate::group_aggregate::{GroupAggregateOperator, StreamingAggSpec};
        use crate::ir::triple::{Ref, TriplePattern};
        use crate::ir::AggregateFn;
        use crate::limit::LimitOperator;
        use crate::sort::{SortDirection, SortOperator, SortSpec};

        let tp = TriplePattern::new(
            Ref::Var(self.subject_var),
            self.predicate.clone(),
            Term::Var(self.object_var),
        );

        // Note: EmitMask pruning is only effective on the binary scan path.
        // The range fallback path (used in memory / pre-index fallback) ignores EmitMask,
        // so we use the default (ALL) to avoid a schema mismatch.
        let scan: BoxedOperator = Box::new(DatasetOperator::scan(
            tp,
            None,
            Vec::new(),
            crate::binary_scan::EmitMask::ALL,
            None,
            self.mode,
        ));

        let agg_specs = vec![StreamingAggSpec {
            function: AggregateFn::CountAll,
            input_col: None,
            output_var: self.count_var,
        }];
        let grouped: BoxedOperator = Box::new(GroupAggregateOperator::new(
            scan,
            vec![self.object_var],
            agg_specs,
            None,
            false,
        ));

        let sorted: BoxedOperator = Box::new(SortOperator::new(
            grouped,
            vec![SortSpec {
                var: self.count_var,
                direction: SortDirection::Descending,
            }],
        ));

        let mut limited: BoxedOperator = Box::new(LimitOperator::new(sorted, self.limit));
        limited.open(ctx).await?;
        self.fallback = Some(limited);
        Ok(())
    }
}

#[async_trait]
impl Operator for PredicateGroupCountFirstsOperator {
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        self.fallback
            .as_deref()
            .map(|fb| vec![crate::plan_node::PlanChild::fallback(fb)])
            .unwrap_or_default()
    }
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            return Ok(());
        }
        self.state = OperatorState::Open;
        self.results_v6 = None;
        self.pos = 0;
        self.fallback = None;

        let mut one_graph_answered = false;
        let grouped = match ctx.index_scope() {
            // One graph whose index alone answers for it. A grouping the V6
            // path can't give falls back.
            Some(IndexScope::One(member)) if member.index_is_current() => {
                let grouped = group_count_v6(
                    member.store(),
                    member.g_id(),
                    &self.predicate,
                    self.limit,
                    &ctx.cancellation,
                )
                .ok();
                if grouped.is_some() {
                    one_graph_answered = true;
                    self.emit_g_id = member.g_id();
                    self.decode_refs = false;
                }
                grouped
            }
            Some(IndexScope::Union(set)) => {
                let grouped = self.group_union(ctx, &set);
                // Any member's view decodes the results: the merge declines
                // arena types, the one kind of value a graph scopes.
                self.emit_g_id = set.members()[0].g_id();
                self.decode_refs = true;
                grouped
            }
            // A default graph of none has no group.
            Some(IndexScope::Empty) => {
                stamp_fast_path(
                    GROUP_BY_OBJECT_COUNT_TOPK_UNION_SITE,
                    FastPathOutcome::Proceed,
                );
                Some(Vec::new())
            }
            _ => None,
        };
        // The plan stamped `Proceed` when the detector matched; the one-graph
        // lane's decline at open is stamped here, so planned and executed can
        // be told apart, whether the union lane or the general plan answers.
        if !one_graph_answered {
            stamp_fast_path(
                GROUP_BY_OBJECT_COUNT_TOPK_SITE,
                FastPathOutcome::Fallback(FastPathFallback::GateDeclined),
            );
        }
        match grouped {
            Some(rows) => {
                self.results_v6 = Some(rows);
                Ok(())
            }
            None => self.open_fallback(ctx).await,
        }
    }

    async fn next_batch(
        &mut self,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<crate::binding::Batch>> {
        if let Some(op) = self.fallback.as_mut() {
            let batch = op.next_batch(ctx).await?;
            if batch.is_none() {
                self.state = OperatorState::Exhausted;
            }
            return Ok(batch);
        }
        if !self.state.can_next() {
            return Ok(None);
        }

        let Some(v6_results) = &self.results_v6 else {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        };
        if self.pos >= v6_results.len() {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }
        let binary_index_store = ctx.binary_store.as_ref().ok_or_else(|| {
            QueryError::Internal("V6 group-count results but no V6 store".to_string())
        })?;
        let g_id = self.emit_g_id;
        let p_id = resolve_predicate_id_v6(&self.predicate, binary_index_store)?;
        let view = fluree_db_binary_index::BinaryGraphView::with_novelty(
            Arc::clone(binary_index_store),
            g_id,
            ctx.dict_novelty.clone(),
        )
        .with_namespace_codes_fallback(ctx.namespace_codes_fallback.clone());

        let batch_size = ctx.batch_size;
        let mut col_o: Vec<Binding> = Vec::with_capacity(batch_size);
        let mut col_c: Vec<Binding> = Vec::with_capacity(batch_size);

        while self.pos < v6_results.len() && col_o.len() < batch_size {
            let (o_type, o_key, count) = v6_results[self.pos];
            self.pos += 1;

            if o_type == OType::IRI_REF.as_u16() {
                col_o.push(if self.decode_refs {
                    Binding::sid(
                        view.resolve_subject_sid(o_key)
                            .map_err(|e| QueryError::Internal(format!("V6 subject: {e}")))?,
                    )
                } else {
                    Binding::encoded_sid(o_key)
                });
            } else {
                let val = view
                    .decode_value(o_type, o_key, p_id)
                    .map_err(|e| QueryError::Internal(format!("V6 decode_value: {e}")))?;
                let dt = binary_index_store
                    .resolve_datatype_sid_for_value(o_type, &val)
                    .unwrap_or_else(|| fluree_db_core::Sid::new(0, ""));
                let lang: Option<Arc<str>> =
                    binary_index_store.resolve_lang_tag(o_type).map(Arc::from);
                col_o.push(Binding::Lit {
                    val,
                    dtc: match lang {
                        Some(tag) => fluree_db_core::DatatypeConstraint::LangTag(tag),
                        None => fluree_db_core::DatatypeConstraint::Explicit(dt),
                    },
                    t: None,
                    op: None,
                    p_id: None,
                });
            }
            col_c.push(Binding::lit(
                fluree_db_core::FlakeValue::Long(count),
                fluree_db_core::Sid::xsd_integer(),
            ));
        }

        Ok(Some(crate::binding::Batch::new(
            self.schema.clone(),
            vec![col_o, col_c],
        )?))
    }

    fn close(&mut self) {
        self.state = OperatorState::Closed;
        if let Some(mut op) = self.fallback.take() {
            op.close();
        }
        self.results_v6 = None;
        self.pos = 0;
    }

    fn estimated_rows(&self) -> Option<usize> {
        Some(self.limit)
    }
}

// ---------------------------------------------------------------------------
// Operator 2: PredicateObjectCountFirstsOperator
// ---------------------------------------------------------------------------

/// Fast-path: `SELECT (COUNT(?s) AS ?count) WHERE { ?s <p> <o> }` in POST order.
///
/// Uses only per-leaflet uncompressed "FIRST" headers to skip decoding entire leaflets when
/// `FIRST(i).(p,o) == FIRST(i+1).(p,o)` (boundary-equality implies the whole leaflet is that (p,o) in POST order).
///
/// Semantics: matches the current "loose" mode when no datatype/lang constraint is specified:
/// compare on Region1 `(o_kind, o_key)` only (dt/lang are ignored).
pub struct PredicateObjectCountFirstsOperator {
    /// Output schema: [count_var]
    schema: Arc<[VarId]>,
    subject_var: VarId,
    count_var: VarId,
    /// Bound predicate reference (Sid or Iri).
    predicate: crate::ir::triple::Ref,
    /// Bound object term (Sid/Iri/Value).
    object: Term,
    /// Operator state
    state: OperatorState,
    /// Fallback operator for non-binary / overlay / policy / history contexts.
    fallback: Option<BoxedOperator>,
    /// Computed count (materialized at open)
    count: i64,
    /// Whether the single row has been emitted
    emitted: bool,
    /// Temporal mode captured at planner-time for the fallback scan.
    mode: crate::temporal_mode::TemporalMode,
}

impl PredicateObjectCountFirstsOperator {
    pub fn new(
        predicate: crate::ir::triple::Ref,
        subject_var: VarId,
        object: Term,
        count_var: VarId,
        mode: crate::temporal_mode::TemporalMode,
    ) -> Self {
        Self {
            schema: Arc::from(vec![count_var].into_boxed_slice()),
            subject_var,
            count_var,
            predicate,
            object,
            state: OperatorState::Created,
            fallback: None,
            count: 0,
            emitted: false,
            mode,
        }
    }

    async fn open_fallback(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        use crate::dataset_operator::DatasetOperator;
        use crate::group_aggregate::{GroupAggregateOperator, StreamingAggSpec};
        use crate::ir::triple::{Ref, TriplePattern};
        use crate::ir::AggregateFn;

        let tp = TriplePattern::new(
            Ref::Var(self.subject_var),
            self.predicate.clone(),
            self.object.clone(),
        );

        // Note: EmitMask pruning is only effective on the binary scan path.
        // The range fallback path (used in memory / pre-index fallback) ignores EmitMask,
        // so we use the default (ALL) to avoid a schema mismatch.
        let scan: BoxedOperator = Box::new(DatasetOperator::scan(
            tp,
            None,
            Vec::new(),
            crate::binary_scan::EmitMask::ALL,
            None,
            self.mode,
        ));

        let agg_specs = vec![StreamingAggSpec {
            function: AggregateFn::CountAll,
            input_col: None,
            output_var: self.count_var,
        }];
        let mut op: BoxedOperator = Box::new(GroupAggregateOperator::new(
            scan,
            vec![],
            agg_specs,
            None,
            false,
        ));
        op.open(ctx).await?;
        self.fallback = Some(op);
        Ok(())
    }

    /// The count over a default graph of several graphs, stamped at its own
    /// site; `None` (with the reason stamped) to fall back.
    fn count_union(&self, ctx: &ExecutionContext<'_>, set: &MemberSet<'_>) -> Option<i64> {
        let outcome = if set.all_current() {
            let first = set.members()[0].g_id();
            let stats_view = object_count_stats_view(ctx, set.store(), first);
            match count_bound_object_union(
                ctx.active_snapshot,
                set,
                &self.predicate,
                &self.object,
                stats_view.as_deref(),
                &ctx.tracker,
                &ctx.cancellation,
            ) {
                Ok(outcome) => outcome,
                // As on one graph: what the V6 path can't give falls back.
                Err(_) => Err(FastPathFallback::GateDeclined),
            }
        } else {
            Err(FastPathFallback::Novelty)
        };
        stamp_fast_path(
            PREDICATE_OBJECT_COUNT_UNION_SITE,
            match outcome {
                Ok(_) => FastPathOutcome::Proceed,
                Err(reason) => FastPathOutcome::Fallback(reason),
            },
        );
        outcome.ok()
    }
}

/// `fast-path outcome` site of the bound-object count over one graph, stamped
/// `Proceed` by the planner and a decline at open.
const PREDICATE_OBJECT_COUNT_SITE: &str = "predicate_object_count";

/// `fast-path outcome` site of the grouped object count over one graph,
/// stamped `Proceed` by the planner and a decline at open.
const GROUP_BY_OBJECT_COUNT_TOPK_SITE: &str = "group_by_object_count_topk";

/// `fast-path outcome` site of the grouped object count over a default graph
/// of several graphs, or of none.
pub(crate) const GROUP_BY_OBJECT_COUNT_TOPK_UNION_SITE: &str = "group_by_object_count_topk_union";

/// `fast-path outcome` site of the bound-object count over a default graph of
/// several graphs, or of none.
pub(crate) const PREDICATE_OBJECT_COUNT_UNION_SITE: &str = "predicate_object_count_union";

/// The stats view a bound-object count reads observed datatypes from. Built
/// from the ledger's indexed stats, which hold every graph's.
fn object_count_stats_view(
    ctx: &ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
) -> Option<Arc<fluree_db_core::StatsView>> {
    crate::stats_cache::cached_stats_view_for_db(
        fluree_db_core::GraphDbRef::new(ctx.active_snapshot, g_id, ctx.overlay(), ctx.to_t)
            .with_runtime_small_dicts_opt(ctx.runtime_small_dicts),
        Some(store),
        false,
    )
}

#[async_trait]
impl Operator for PredicateObjectCountFirstsOperator {
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        self.fallback
            .as_deref()
            .map(|fb| vec![crate::plan_node::PlanChild::fallback(fb)])
            .unwrap_or_default()
    }
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            return Ok(());
        }
        self.state = OperatorState::Open;
        self.count = 0;
        self.emitted = false;
        self.fallback = None;

        let mut one_graph_answered = false;
        let counted = match ctx.index_scope() {
            // One graph whose index alone answers for it (no novelty, read at
            // the index's `t`). A count the V6 path can't give falls back.
            Some(IndexScope::One(member)) if member.index_is_current() => {
                let stats_view = object_count_stats_view(ctx, member.store(), member.g_id());
                count_bound_object_v6(
                    ctx.active_snapshot,
                    member.store(),
                    member.g_id(),
                    &self.predicate,
                    &self.object,
                    stats_view.as_deref(),
                    &ctx.cancellation,
                )
                .ok()
                .inspect(|_| one_graph_answered = true)
            }
            Some(IndexScope::Union(set)) => self.count_union(ctx, &set),
            // A default graph of none holds no triple.
            Some(IndexScope::Empty) => {
                stamp_fast_path(PREDICATE_OBJECT_COUNT_UNION_SITE, FastPathOutcome::Proceed);
                Some(0)
            }
            _ => None,
        };
        // The plan stamped `Proceed` when the detector matched; the one-graph
        // lane's decline at open is stamped here, so planned and executed can
        // be told apart, whether the union lane or the general plan answers.
        if !one_graph_answered {
            stamp_fast_path(
                PREDICATE_OBJECT_COUNT_SITE,
                FastPathOutcome::Fallback(FastPathFallback::GateDeclined),
            );
        }
        match counted {
            Some(count) => {
                self.count = count;
                Ok(())
            }
            None => self.open_fallback(ctx).await,
        }
    }

    async fn next_batch(
        &mut self,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<crate::binding::Batch>> {
        if let Some(op) = self.fallback.as_mut() {
            let batch = op.next_batch(ctx).await?;
            if batch.is_none() {
                self.state = OperatorState::Exhausted;
            }
            return Ok(batch);
        }
        if !self.state.can_next() {
            return Ok(None);
        }
        if self.emitted {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }
        self.emitted = true;

        let col_c = vec![Binding::lit(
            fluree_db_core::FlakeValue::Long(self.count),
            fluree_db_core::Sid::xsd_integer(),
        )];

        Ok(Some(crate::binding::Batch::new(
            self.schema.clone(),
            vec![col_c],
        )?))
    }

    fn close(&mut self) {
        self.state = OperatorState::Closed;
        if let Some(mut op) = self.fallback.take() {
            op.close();
        }
        self.count = 0;
        self.emitted = false;
    }

    fn estimated_rows(&self) -> Option<usize> {
        Some(1)
    }
}

// ---------------------------------------------------------------------------
// V6 fast-path implementations
// ---------------------------------------------------------------------------

/// Extract the object prefix `(o_type, o_key)` from a V3 leaflet directory entry's
/// `first_key` field, interpreted in POST order.
///
/// Strips `p_id`. Only meaningful as a bound for rows known to be within the
/// same predicate range — see [`predicate_qualified_prefix`] for cross-predicate
/// comparisons.
#[inline]
fn prefix_v6_from_entry(entry: &LeafletDirEntryV3) -> (u16, u64) {
    let rec = read_ordered_key_v2(RunSortOrder::Post, &entry.first_key);
    (rec.o_type, rec.o_key)
}

/// Read the `(p_id, o_type, o_key)` tuple from a V3 leaflet directory entry's
/// `first_key`, interpreted in POST order. Includes `p_id` so callers can
/// detect predicate boundaries.
#[inline]
fn pid_prefix_v6_from_entry(entry: &LeafletDirEntryV3) -> (u32, u16, u64) {
    let rec = read_ordered_key_v2(RunSortOrder::Post, &entry.first_key);
    (rec.p_id, rec.o_type, rec.o_key)
}

/// Load a V3 leaflet's columns, using the `LeafletCache` when available.
fn load_v6_batch(
    leaf_bytes: &[u8],
    entry: &LeafletDirEntryV3,
    payload_base: usize,
    order: RunSortOrder,
    cache: &Option<&Arc<fluree_db_binary_index::LeafletCache>>,
    leaf_id: u128,
    leaflet_idx: u32,
) -> Result<fluree_db_binary_index::ColumnBatch> {
    let mut needed = ColumnSet::EMPTY;
    needed.insert(ColumnId::OKey);
    if entry.o_type_const.is_none() {
        needed.insert(ColumnId::OType);
    }
    // Mixed-predicate leaflets need the per-row `p_id` column to verify each
    // row belongs to the queried predicate.
    if entry.p_const.is_none() {
        needed.insert(ColumnId::PId);
    }
    let batch = match cache {
        Some(c) => load_leaflet_columns_cached(
            leaf_bytes,
            entry,
            payload_base,
            c,
            fluree_db_binary_index::read::column_loader::LeafletDecodeSpec {
                leaf_id,
                leaflet_idx,
                order,
                decode_set: needed,
            },
        ),
        None => {
            let projection = ColumnProjection {
                output: ColumnSet::EMPTY,
                internal: needed,
            };
            load_leaflet_columns(leaf_bytes, entry, payload_base, &projection, order)
        }
    };
    batch.map_err(|e| QueryError::Internal(format!("load columns: {e}")))
}

/// Resolve a predicate [`Ref`] to its V6 binary index `p_id`.
fn resolve_predicate_id_v6(
    predicate: &crate::ir::triple::Ref,
    store: &BinaryIndexStore,
) -> Result<u32> {
    let sid = normalize_pred_sid(store, predicate)?;
    store
        .sid_to_p_id(&sid)
        .ok_or_else(|| QueryError::Internal("predicate not found in V6 dictionary".to_string()))
}

/// V6 fast-path: count rows for a bound `(predicate, object)` triple.
///
/// Scans the POST leaf range for the predicate, uses boundary-equality on
/// `(o_type, o_key)` to skip whole leaflets, and decodes only `o_key` + `o_type`
/// columns when needed.
fn count_bound_object_v6(
    snapshot: &LedgerSnapshot,
    store: &BinaryIndexStore,
    g_id: GraphId,
    predicate: &Ref,
    object: &Term,
    stats_view: Option<&fluree_db_core::StatsView>,
    cancellation: &QueryCancellation,
) -> Result<i64> {
    let p_id = resolve_predicate_id_v6(predicate, store)?;

    // A bare number counts every numeric datatype holding an equal value: one
    // (o_type, o_key) per datatype the predicate carries, and the NumBig
    // arena's handles for it. A slice that needs the decoded-value filter
    // declines to the generic count.
    if let Term::Value(
        value @ (FlakeValue::Long(_)
        | FlakeValue::Double(_)
        | FlakeValue::Decimal(_)
        | FlakeValue::BigInt(_)),
    ) = object
    {
        // This path reads the index at its max_t, so arena handles are current.
        let slices = crate::binary_scan::untyped_numeric_slices(
            store,
            g_id,
            Some(p_id),
            crate::binary_scan::observed_datatypes(stats_view, g_id, p_id),
            value,
            true,
        )
        .ok_or_else(|| QueryError::Internal("non-finite numeric constant".to_string()))?;
        let mut total = 0;
        for (o_type, o_key) in slices {
            let o_key = o_key.ok_or_else(|| {
                QueryError::Internal("NumBig rows need the decoded-value filter".to_string())
            })?;
            total += count_object_key_v6(store, g_id, p_id, o_type.as_u16(), o_key, cancellation)?;
        }
        return Ok(total);
    }

    // Translate the bound object term into V6 (o_type, o_key). A `None` here is
    // a *conclusive* base-dict miss (refs are resolved snapshot-aware); since
    // this path runs only with no novelty overlay, the count is exactly 0 —
    // return without a fallback full-predicate scan.
    let Some((target_o_type, target_o_key)) =
        translate_term_to_v6(object, snapshot, store, p_id, g_id)?
    else {
        return Ok(0);
    };
    count_object_key_v6(store, g_id, p_id, target_o_type, target_o_key, cancellation)
}

/// Rows of predicate `p_id` whose object is exactly `(target_o_type, target_o_key)`.
fn count_object_key_v6(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
    target_o_type: u16,
    target_o_key: u64,
    cancellation: &QueryCancellation,
) -> Result<i64> {
    let branch = store
        .branch_for_order(g_id, RunSortOrder::Post)
        .ok_or_else(|| QueryError::Internal("no POST branch for graph".to_string()))?;
    let cmp = cmp_v2_for_order(RunSortOrder::Post);

    let min_key = RunRecordV2 {
        s_id: SubjectId(0),
        o_key: target_o_key,
        p_id,
        t: 0,
        o_i: 0,
        o_type: target_o_type,
        g_id,
    };
    let max_key = RunRecordV2 {
        s_id: SubjectId(u64::MAX),
        o_key: target_o_key,
        p_id,
        t: 0,
        o_i: u32::MAX,
        o_type: target_o_type,
        g_id,
    };
    let leaf_range = branch.find_leaves_in_range(&min_key, &max_key, cmp);

    let target_prefix = (target_o_type, target_o_key);
    let mut total: i64 = 0;
    let cache = store.leaflet_cache();

    for leaf_idx in leaf_range.clone() {
        check_cancelled(cancellation)?;
        let leaf_entry = &branch.leaves[leaf_idx];
        let bytes = store
            .get_leaf_bytes_shared(&leaf_entry.leaf_cid)
            .map_err(|e| QueryError::Internal(format!("leaf fetch: {e}")))?;
        let header =
            decode_leaf_header_v3(&bytes).map_err(|e| QueryError::Internal(e.to_string()))?;
        let dir = decode_leaf_dir_v3_with_base(&bytes, &header)
            .map_err(|e| QueryError::Internal(e.to_string()))?;
        let leaf_id = xxhash_rust::xxh3::xxh3_128(leaf_entry.leaf_cid.to_bytes().as_ref());

        for (i, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 {
                continue;
            }
            // For mixed-predicate leaflets (`p_const = None`) we cannot skip on
            // p_const alone — the leaflet may still contain rows for our p_id.
            if let Some(leaflet_p) = entry.p_const {
                if leaflet_p != p_id {
                    continue;
                }
            }

            let prefix = prefix_v6_from_entry(entry);
            // The leaflet's rows span `[prefix, next_prefix)` half-open: the
            // next leaflet's first key is the first row *not* in this leaflet.
            // (A single key value can spill across leaflet boundaries — both
            // leaflets then have rows for that value, and the gating below
            // handles it correctly.)
            //
            // POST sorts by `(p_id, o_type, o_key, ...)`. The `(o_type, o_key)`
            // pair is only a meaningful range bound *within* a single predicate;
            // when the next leaflet starts a different predicate, its
            // `(o_type, o_key)` tuple is unrelated to our range and cannot be
            // used to skip this leaflet. Detect that case and treat next_prefix
            // as `None` (unknown upper bound) — fall through to row-level scan.
            let next_full = if i + 1 < dir.entries.len() {
                Some(pid_prefix_v6_from_entry(&dir.entries[i + 1]))
            } else if leaf_idx + 1 < leaf_range.end {
                let next = &branch.leaves[leaf_idx + 1].first_key;
                Some((next.p_id, next.o_type, next.o_key))
            } else {
                None
            };
            let next_prefix = next_full.and_then(|(np_p, np_ot, np_ok)| {
                if np_p == p_id {
                    Some((np_ot, np_ok))
                } else {
                    None
                }
            });

            // `prefix` is computed from the leaflet's first row stripped of
            // `p_id`. For a homogeneous-predicate leaflet (`p_const = Some(p_id)`)
            // it's a sound sort key for early break, but in a mixed-predicate
            // leaflet the first row may belong to a *different* predicate — its
            // `(o_type, o_key)` could exceed `target_prefix` even when later
            // rows in the leaflet (or in subsequent leaflets) belong to our
            // predicate and are still on or before `target_prefix`. Restrict
            // the break to homogeneous leaflets.
            if prefix > target_prefix && entry.p_const == Some(p_id) {
                break;
            }
            // Skip a leaflet only when it ends strictly before the target —
            // i.e. the next leaflet's first row (within our predicate) also
            // sorts before the target. Equality is not enough: this leaflet's
            // last row can equal target if a key spills across the boundary.
            if let Some(np) = next_prefix {
                if np < target_prefix {
                    continue;
                }
            }

            // Boundary-equality fast count: only valid when the leaflet is
            // entirely target rows for our predicate (it starts at the target
            // value, the next leaflet also starts at the target value, AND the
            // leaflet is homogeneous on `p_id`). A mixed-predicate leaflet can
            // satisfy the first two conditions while still containing rows for
            // *other* predicates that must be excluded — fall through to the
            // per-row scan in that case.
            if prefix == target_prefix
                && next_prefix == Some(target_prefix)
                && entry.p_const == Some(p_id)
            {
                total += entry.row_count as i64;
                continue;
            }

            let batch = load_v6_batch(
                &bytes,
                entry,
                dir.payload_base,
                header.order,
                &cache,
                leaf_id,
                u32::try_from(i)
                    .map_err(|_| QueryError::Internal("leaflet idx exceeds u32".to_string()))?,
            )?;

            for row in 0..batch.row_count {
                // For mixed-predicate leaflets we must verify p_id per row —
                // a `p_const = Some(p_id)` leaflet lets us skip the column read.
                if entry.p_const.is_none() {
                    let row_p_id = batch.p_id.get_or(row, 0);
                    if row_p_id != p_id {
                        continue;
                    }
                }
                let ot = entry
                    .o_type_const
                    .unwrap_or_else(|| batch.o_type.get_or(row, 0));
                if ot == target_o_type && batch.o_key.get(row) == target_o_key {
                    total += 1;
                }
            }
        }
    }

    Ok(total)
}

/// V6 fast-path: GROUP BY ?o COUNT(?s) for a predicate.
///
/// Returns `Vec<(o_type, o_key, count)>` sorted by count descending, truncated to `limit`.
/// One `(o_type, o_key)` group's count, ordered so a `BinaryHeap` (max-heap)
/// keeps the *worst* element on top for O(log K) eviction.
///
/// "Worse" = the element the final sort would place LATER: lower count, then
/// higher `o_type`, then higher `o_key` (the reverse of the keep order, which is
/// count DESC, `o_type` ASC, `o_key` ASC). `into_sorted_vec()` then yields the
/// kept groups in keep order (best first) directly.
#[derive(PartialEq, Eq)]
struct GroupTopK {
    count: i64,
    o_type: u16,
    o_key: u64,
}

impl Ord for GroupTopK {
    fn cmp(&self, other: &Self) -> Ordering {
        // Larger == worse.
        other
            .count
            .cmp(&self.count)
            .then(self.o_type.cmp(&other.o_type))
            .then(self.o_key.cmp(&other.o_key))
    }
}

impl PartialOrd for GroupTopK {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Offer a completed group's run to the bounded top-K heap, keeping only the
/// `limit` best by (count DESC, o_type ASC, o_key ASC). No-op for empty runs.
fn offer_topk(heap: &mut BinaryHeap<GroupTopK>, limit: usize, cand: GroupTopK) {
    if limit == 0 || cand.count <= 0 {
        return;
    }
    if heap.len() < limit {
        heap.push(cand);
    } else if let Some(worst) = heap.peek() {
        // cand < worst means cand is better than the current worst kept element.
        if cand.cmp(worst) == Ordering::Less {
            heap.pop();
            heap.push(cand);
        }
    }
}

pub(crate) fn group_count_v6(
    store: &BinaryIndexStore,
    g_id: GraphId,
    predicate: &crate::ir::triple::Ref,
    limit: usize,
    cancellation: &QueryCancellation,
) -> Result<Vec<(u16, u64, i64)>> {
    let p_id = resolve_predicate_id_v6(predicate, store)?;

    let branch = store
        .branch_for_order(g_id, RunSortOrder::Post)
        .ok_or_else(|| QueryError::Internal("no POST branch for graph".to_string()))?;
    let cmp = cmp_v2_for_order(RunSortOrder::Post);

    let (min_key, max_key) = post_predicate_bounds(g_id, p_id);
    let leaf_range = branch.find_leaves_in_range(&min_key, &max_key, cmp);

    // Streaming run-length: POST order (p_id, o_type, o_key, …) makes every
    // object's rows physically contiguous, so COUNT(?subject) per object is the
    // length of its run — a single counter that persists across leaflet/leaf
    // boundaries, no per-row hashing. Completed runs feed a bounded top-K heap, so
    // there is no full sort of all distinct objects either.
    let mut heap: BinaryHeap<GroupTopK> =
        BinaryHeap::with_capacity(fluree_db_core::topk::initial_capacity(limit));
    let mut cur: Option<(u16, u64)> = None;
    let mut run: i64 = 0;
    let cache = store.leaflet_cache();

    for leaf_idx in leaf_range.clone() {
        check_cancelled(cancellation)?;
        let leaf_entry = &branch.leaves[leaf_idx];
        let bytes = store
            .get_leaf_bytes_shared(&leaf_entry.leaf_cid)
            .map_err(|e| QueryError::Internal(format!("leaf fetch: {e}")))?;
        let header =
            decode_leaf_header_v3(&bytes).map_err(|e| QueryError::Internal(e.to_string()))?;
        let dir = decode_leaf_dir_v3_with_base(&bytes, &header)
            .map_err(|e| QueryError::Internal(e.to_string()))?;
        let leaf_id = xxhash_rust::xxh3::xxh3_128(leaf_entry.leaf_cid.to_bytes().as_ref());

        for (i, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 {
                continue;
            }
            // Mixed-predicate leaflets (`p_const = None`) may still hold rows
            // for our predicate; only skip when the leaflet is constant on a
            // *different* predicate.
            if let Some(leaflet_p) = entry.p_const {
                if leaflet_p != p_id {
                    continue;
                }
            }

            let prefix = prefix_v6_from_entry(entry);

            // Boundary-equality: check the next leaflet's first object key,
            // but only when it's within the same predicate range — see
            // `count_bound_object_v6` for the rationale.
            let next_full = if i + 1 < dir.entries.len() {
                Some(pid_prefix_v6_from_entry(&dir.entries[i + 1]))
            } else if leaf_idx + 1 < leaf_range.end {
                let next = &branch.leaves[leaf_idx + 1].first_key;
                Some((next.p_id, next.o_type, next.o_key))
            } else {
                None
            };
            let next_prefix = next_full.and_then(|(np_p, np_ot, np_ok)| {
                if np_p == p_id {
                    Some((np_ot, np_ok))
                } else {
                    None
                }
            });

            // Boundary-equality fast count is only sound for homogeneous-
            // predicate leaflets — a mixed-predicate leaflet may have the
            // same `(o_type, o_key)` first row as the next leaflet but still
            // contain rows for *other* predicates that must be excluded.
            if next_prefix == Some(prefix) && entry.p_const == Some(p_id) {
                // Whole leaflet is one object that continues into the next leaflet:
                // extend the current run (or start one), no per-row decode.
                if cur == Some(prefix) {
                    run += entry.row_count as i64;
                } else {
                    offer_topk(
                        &mut heap,
                        limit,
                        GroupTopK {
                            count: run,
                            o_type: cur.map_or(0, |c| c.0),
                            o_key: cur.map_or(0, |c| c.1),
                        },
                    );
                    cur = Some(prefix);
                    run = entry.row_count as i64;
                }
                continue;
            }

            // Decode columns (cached when available).
            let batch = load_v6_batch(
                &bytes,
                entry,
                dir.payload_base,
                header.order,
                &cache,
                leaf_id,
                u32::try_from(i)
                    .map_err(|_| QueryError::Internal("leaflet idx exceeds u32".to_string()))?,
            )?;

            for row in 0..batch.row_count {
                // Per-row `p_id` check needed for mixed-predicate leaflets;
                // `load_v6_batch` decodes `p_id` exactly when `p_const` is
                // `None`.
                if entry.p_const.is_none() {
                    let row_p_id = batch.p_id.get_or(row, 0);
                    if row_p_id != p_id {
                        continue;
                    }
                }
                let ot = entry
                    .o_type_const
                    .unwrap_or_else(|| batch.o_type.get_or(row, 0));
                let ok = batch.o_key.get(row);
                // Contiguous run: extend if the object is unchanged, else flush the
                // completed run and start a new one.
                if cur == Some((ot, ok)) {
                    run += 1;
                } else {
                    offer_topk(
                        &mut heap,
                        limit,
                        GroupTopK {
                            count: run,
                            o_type: cur.map_or(0, |c| c.0),
                            o_key: cur.map_or(0, |c| c.1),
                        },
                    );
                    cur = Some((ot, ok));
                    run = 1;
                }
            }
        }
    }

    // Flush the final run, then emit the kept groups in keep order (best first):
    // count DESC, o_type ASC, o_key ASC — identical to the prior sort+truncate.
    offer_topk(
        &mut heap,
        limit,
        GroupTopK {
            count: run,
            o_type: cur.map_or(0, |c| c.0),
            o_key: cur.map_or(0, |c| c.1),
        },
    );
    let rows: Vec<(u16, u64, i64)> = heap
        .into_sorted_vec()
        .into_iter()
        .map(|g| (g.o_type, g.o_key, g.count))
        .collect();

    Ok(rows)
}

/// Translate a bound object `Term` to V6 `(o_type, o_key)`.
///
/// Return values:
/// - `Ok(Some(..))` — resolved to a persisted `(o_type, o_key)`.
/// - `Ok(None)` — the object is **conclusively absent from the base dict**.
///   Combined with the caller's no-live-novelty gate, "absent from
///   base dict" implies "absent from the logical DB", so the caller reports a
///   0 count.
/// - `Err(..)` — genuine error or unbound object; routes to the generic
///   fallback.
///
/// Refs (`Term::Iri` and `Term::Sid`) resolve through `subject_ref_to_s_id`,
/// which is snapshot-aware: a `Sid` is decoded via `snapshot.decode_sid` (then
/// `store.sid_to_iri`) and re-looked-up by full IRI. This makes a `None` a
/// genuine base miss rather than a "store can't decode this snapshot namespace
/// code" false negative — so returning a 0 count stays sound even for
/// pre-encoded `Term::Sid` objects of unknown provenance.
fn translate_term_to_v6(
    term: &Term,
    snapshot: &LedgerSnapshot,
    store: &BinaryIndexStore,
    _p_id: u32,
    _g_id: GraphId,
) -> Result<Option<(u16, u64)>> {
    match term {
        Term::Iri(iri) => Ok(
            subject_ref_to_s_id(snapshot, store, &Ref::Iri(iri.clone()))?
                .map(|s_id| (OType::IRI_REF.as_u16(), s_id)),
        ),
        Term::Sid(sid) => Ok(
            subject_ref_to_s_id(snapshot, store, &Ref::Sid(sid.clone()))?
                .map(|s_id| (OType::IRI_REF.as_u16(), s_id)),
        ),
        Term::Value(val) => {
            // Literal values: the FlakeValue → (o_type, o_key) translation.
            // A NotFound means the value isn't in the persisted dict — conclusive
            // for literals (no namespace ambiguity). Other errors are genuine and
            // propagate to the fallback. (Ref-valued objects arrive as Term::Iri.)
            match crate::binary_scan::value_to_otype_okey_simple(val, store) {
                Ok((ot, ok)) => Ok(Some((ot.as_u16(), ok))),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(QueryError::from_io("value_to_otype_okey_simple", e)),
            }
        }
        Term::Var(_) => Err(QueryError::InvalidQuery(
            "fast-path requires a bound object".to_string(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Operator 3: GroupByObjectStarTopKOperator
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct ObjGroupKey {
    o_type: u16,
    o_key: u64,
}

#[derive(Clone, Debug)]
struct AggStateStar {
    count: u64,
    min_s: Option<u64>,
    max_s: Option<u64>,
    sample_s: Option<u64>,
}

impl AggStateStar {
    fn new() -> Self {
        Self {
            count: 0,
            min_s: None,
            max_s: None,
            sample_s: None,
        }
    }

    /// Fold one group-predicate row for subject `s_id`, joined against `mult`
    /// filter-star rows (the product of the filter predicates' per-subject row
    /// counts). SPARQL bag semantics: the join multiplies each group row by
    /// `mult`, so COUNT adds `mult`; MIN/MAX/SAMPLE over `?s` are
    /// duplicate-insensitive and ignore it.
    fn observe(&mut self, s_id: u64, mult: u64, want_min: bool, want_max: bool, want_sample: bool) {
        self.count = self.count.saturating_add(mult);
        if want_sample && self.sample_s.is_none() {
            self.sample_s = Some(s_id);
        }
        if want_min {
            self.min_s = Some(self.min_s.map_or(s_id, |m| m.min(s_id)));
        }
        if want_max {
            self.max_s = Some(self.max_s.map_or(s_id, |m| m.max(s_id)));
        }
    }
}

/// Fast-path: same-subject star join with GROUP BY object and top-k ORDER BY DESC(count).
///
/// Shape:
/// `?s <p_group> ?o . ?s <p_filter1> ?x1 . ...`
/// `GROUP BY ?o ORDER BY DESC(?count) LIMIT k`
///
/// Optionally also computes MIN/MAX/SAMPLE on `?s`.
///
/// COUNT honors SPARQL bag semantics: each group-predicate row is multiplied
/// by the product of the filter predicates' per-subject row counts, not merely
/// gated on subject existence (#1652). That product is the star join's
/// multiplicity only because the filter object vars are pairwise distinct —
/// `detect_group_by_object_star_topk` declines when they are not.
pub struct GroupByObjectStarTopKOperator {
    group_pred: crate::ir::triple::Ref,
    filter_preds: Vec<crate::ir::triple::Ref>,
    group_var: VarId,
    count_var: VarId,
    min_var: Option<VarId>,
    max_var: Option<VarId>,
    sample_var: Option<VarId>,
    limit: usize,
    schema: Arc<[VarId]>,
    state: OperatorState,
    fallback: Option<BoxedOperator>,
    emitted: bool,
    result: Option<crate::binding::Batch>,
}

impl GroupByObjectStarTopKOperator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        group_pred: crate::ir::triple::Ref,
        filter_preds: Vec<crate::ir::triple::Ref>,
        group_var: VarId,
        count_var: VarId,
        min_var: Option<VarId>,
        max_var: Option<VarId>,
        sample_var: Option<VarId>,
        limit: usize,
        schema: Arc<[VarId]>,
        fallback: Option<BoxedOperator>,
    ) -> Self {
        Self {
            group_pred,
            filter_preds,
            group_var,
            count_var,
            min_var,
            max_var,
            sample_var,
            limit: limit.max(1),
            schema,
            state: OperatorState::Created,
            fallback,
            emitted: false,
            result: None,
        }
    }
}

#[async_trait]
impl Operator for GroupByObjectStarTopKOperator {
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        self.fallback
            .as_deref()
            .map(|fb| vec![crate::plan_node::PlanChild::fallback(fb)])
            .unwrap_or_default()
    }
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(QueryError::OperatorClosed);
            }
            return Err(QueryError::OperatorAlreadyOpened);
        }

        use crate::fast_path_outcome::{stamp_fast_path, FastPathFallback, FastPathOutcome};
        const SITE: &str = "group_by_object_star_topk";

        if allow_cursor_fast_path(ctx) {
            if let Some(store) = ctx.binary_store.as_ref() {
                let Some(batch) = compute_group_by_object_star_topk(
                    store,
                    ctx,
                    ctx.binary_g_id,
                    &self.group_pred,
                    &self.filter_preds,
                    Arc::clone(&self.schema),
                    self.group_var,
                    self.count_var,
                    self.min_var,
                    self.max_var,
                    self.sample_var,
                    self.limit,
                )?
                else {
                    // Fast-path unavailable under this execution context (e.g., overlay requires fallback).
                    // Fall through to the provided fallback operator.
                    stamp_fast_path(
                        SITE,
                        FastPathOutcome::Fallback(FastPathFallback::GateDeclined),
                    );
                    let Some(fallback) = &mut self.fallback else {
                        return Err(QueryError::Internal(
                            "group-by-object star topk fast-path unavailable and no fallback provided".into(),
                        ));
                    };
                    fallback.open(ctx).await?;
                    self.state = OperatorState::Open;
                    return Ok(());
                };
                stamp_fast_path(SITE, FastPathOutcome::Proceed);
                self.result = Some(batch);
                self.emitted = false;
                self.fallback = None;
                self.state = OperatorState::Open;
                return Ok(());
            }
        }

        stamp_fast_path(
            SITE,
            FastPathOutcome::Fallback(FastPathFallback::GateDeclined),
        );
        let Some(fallback) = &mut self.fallback else {
            return Err(QueryError::Internal(
                "group-by-object star topk fast-path unavailable and no fallback provided".into(),
            ));
        };
        fallback.open(ctx).await?;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(
        &mut self,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<crate::binding::Batch>> {
        if let Some(fb) = &mut self.fallback {
            return fb.next_batch(ctx).await;
        }
        if !self.state.can_next() {
            if self.state == OperatorState::Created {
                return Err(QueryError::OperatorNotOpened);
            }
            return Ok(None);
        }
        if self.emitted {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }
        self.emitted = true;
        Ok(self.result.take())
    }

    fn close(&mut self) {
        if let Some(fb) = &mut self.fallback {
            fb.close();
        }
        self.state = OperatorState::Closed;
        self.emitted = false;
        self.result = None;
    }
}

/// Stream one filter predicate's PSOT rows and fold them into a
/// `subject -> multiplicity` map. With `restrict_to` set (the running product
/// over earlier filter predicates), a subject survives only if present there,
/// and its new multiplicity is `restrict_to[s] * row_count(s)` — the star
/// join's bag multiplicity across filter predicates. Without it, the
/// multiplicity is this predicate's row count alone.
fn collect_subject_counts_for_predicate_group(
    store: &Arc<BinaryIndexStore>,
    ctx: &ExecutionContext<'_>,
    g_id: GraphId,
    pred: &crate::ir::triple::Ref,
    restrict_to: Option<&FxHashMap<u64, u64>>,
) -> Result<Option<FxHashMap<u64, u64>>> {
    let overlay_has_rows = crate::fast_path_common::overlay_has_novelty(ctx);
    let sid = normalize_pred_sid(store, pred)?;
    let Some(p_id) = store.sid_to_p_id(&sid) else {
        return if overlay_has_rows {
            Ok(None)
        } else {
            Ok(Some(FxHashMap::default()))
        };
    };
    let mut out = ColumnSet::EMPTY;
    out.insert(ColumnId::SId);
    let projection = ColumnProjection {
        output: out,
        internal: ColumnSet::EMPTY,
    };
    let Some(mut cursor) =
        build_psot_cursor_for_predicate(ctx, store, g_id, sid, p_id, projection)?
    else {
        return Ok(None);
    };

    let mut map: FxHashMap<u64, u64> = FxHashMap::default();
    let flush = |s: u64, n: u64, map: &mut FxHashMap<u64, u64>| match restrict_to {
        Some(r) => {
            if let Some(&m) = r.get(&s) {
                map.insert(s, m.saturating_mul(n));
            }
        }
        None => {
            map.insert(s, n);
        }
    };
    let mut cur_s: Option<u64> = None;
    let mut cur_n: u64 = 0;
    while let Some(batch) = cursor
        .next_batch()
        .map_err(|e| QueryError::Internal(format!("cursor batch: {e}")))?
    {
        ctx.check_cancelled()?;
        for i in 0..batch.row_count {
            let s = batch.s_id.get(i);
            if cur_s == Some(s) {
                cur_n += 1;
            } else {
                if let Some(prev) = cur_s {
                    flush(prev, cur_n, &mut map);
                }
                cur_s = Some(s);
                cur_n = 1;
            }
        }
    }
    if let Some(prev) = cur_s {
        flush(prev, cur_n, &mut map);
    }
    Ok(Some(map))
}

#[allow(clippy::too_many_arguments)]
fn compute_group_by_object_star_topk(
    store: &Arc<BinaryIndexStore>,
    ctx: &ExecutionContext<'_>,
    g_id: GraphId,
    group_pred: &crate::ir::triple::Ref,
    filter_preds: &[crate::ir::triple::Ref],
    schema: Arc<[VarId]>,
    group_var: VarId,
    count_var: VarId,
    min_var: Option<VarId>,
    max_var: Option<VarId>,
    sample_var: Option<VarId>,
    limit: usize,
) -> Result<Option<crate::binding::Batch>> {
    let overlay_has_rows = crate::fast_path_common::overlay_has_novelty(ctx);
    // Scan group predicate PSOT for (s_id, o_type, o_key).
    let sid = normalize_pred_sid(store, group_pred)?;
    let Some(p_id) = store.sid_to_p_id(&sid) else {
        return if overlay_has_rows {
            Ok(None)
        } else {
            Ok(Some(crate::binding::Batch::empty(schema)?))
        };
    };
    let mut out = ColumnSet::EMPTY;
    out.insert(ColumnId::SId);
    out.insert(ColumnId::OType);
    out.insert(ColumnId::OKey);
    let projection = ColumnProjection {
        output: out,
        internal: ColumnSet::EMPTY,
    };
    let Some(mut cursor) =
        build_psot_cursor_for_predicate(ctx, store, g_id, sid, p_id, projection)?
    else {
        return Ok(None);
    };

    let want_min = min_var.is_some();
    let want_max = max_var.is_some();
    let want_sample = sample_var.is_some();

    let mut aggs: FxHashMap<ObjGroupKey, AggStateStar> = FxHashMap::default();

    // Preferred path (fast + low-memory): merge-join on subject IDs when there is exactly one filter predicate.
    if filter_preds.len() == 1 {
        let fp = &filter_preds[0];
        let fp_sid = normalize_pred_sid(store, fp)?;
        let Some(fp_id) = store.sid_to_p_id(&fp_sid) else {
            return if overlay_has_rows {
                Ok(None)
            } else {
                Ok(Some(crate::binding::Batch::empty(schema)?))
            };
        };
        let mut fp_out = ColumnSet::EMPTY;
        fp_out.insert(ColumnId::SId);
        let fp_proj = ColumnProjection {
            output: fp_out,
            internal: ColumnSet::EMPTY,
        };
        let Some(mut fcur) =
            build_psot_cursor_for_predicate(ctx, store, g_id, fp_sid, fp_id, fp_proj)?
        else {
            return Ok(None);
        };

        let mut g_batch: Option<ColumnBatch> = None;
        let mut g_i: usize = 0;
        let mut f_batch: Option<ColumnBatch> = None;
        let mut f_i: usize = 0;

        // Next `(subject, row_count)` group of the filter predicate. The whole
        // run is consumed (across batch refills) before returning, so filter
        // subjects arrive strictly increasing with their bag multiplicity.
        let next_filter_group = |fcur: &mut BinaryCursor,
                                 f_batch: &mut Option<ColumnBatch>,
                                 f_i: &mut usize|
         -> Result<Option<(u64, u64)>> {
            loop {
                if f_batch.is_none() || *f_i >= f_batch.as_ref().unwrap().row_count {
                    ctx.check_cancelled()?;
                    *f_batch = fcur
                        .next_batch()
                        .map_err(|e| QueryError::Internal(format!("cursor batch: {e}")))?;
                    *f_i = 0;
                    if f_batch.is_none() {
                        return Ok(None);
                    }
                    continue;
                }
                break;
            }
            let s = f_batch.as_ref().unwrap().s_id.get(*f_i);
            let mut n: u64 = 0;
            loop {
                let b = f_batch.as_ref().unwrap();
                while *f_i < b.row_count && b.s_id.get(*f_i) == s {
                    n += 1;
                    *f_i += 1;
                }
                if *f_i < b.row_count {
                    break; // group ended within this batch
                }
                ctx.check_cancelled()?;
                *f_batch = fcur
                    .next_batch()
                    .map_err(|e| QueryError::Internal(format!("cursor batch: {e}")))?;
                *f_i = 0;
                if f_batch.is_none() {
                    break; // stream ended; group is complete
                }
            }
            Ok(Some((s, n)))
        };

        let peek_group_subject = |cursor: &mut BinaryCursor,
                                  g_batch: &mut Option<ColumnBatch>,
                                  g_i: &mut usize|
         -> Result<Option<u64>> {
            if g_batch.is_none() || *g_i >= g_batch.as_ref().unwrap().row_count {
                ctx.check_cancelled()?;
                *g_batch = cursor
                    .next_batch()
                    .map_err(|e| QueryError::Internal(format!("cursor batch: {e}")))?;
                *g_i = 0;
                if g_batch.is_none() {
                    return Ok(None);
                }
            }
            let b = g_batch.as_ref().unwrap();
            Ok(Some(b.s_id.get(*g_i)))
        };

        let mut fs = next_filter_group(&mut fcur, &mut f_batch, &mut f_i)?;
        while let (Some(gs), Some((cur_fs, f_mult))) =
            (peek_group_subject(&mut cursor, &mut g_batch, &mut g_i)?, fs)
        {
            match gs.cmp(&cur_fs) {
                Ordering::Less => {
                    // Skip all group rows for this subject.
                    let skip_s = gs;
                    while let Some(cur_gs) =
                        peek_group_subject(&mut cursor, &mut g_batch, &mut g_i)?
                    {
                        if cur_gs != skip_s {
                            break;
                        }
                        g_i += 1;
                    }
                }
                Ordering::Greater => {
                    fs = next_filter_group(&mut fcur, &mut f_batch, &mut f_i)?;
                }
                Ordering::Equal => {
                    let s = gs;
                    while let Some(cur_gs) =
                        peek_group_subject(&mut cursor, &mut g_batch, &mut g_i)?
                    {
                        if cur_gs != s {
                            break;
                        }
                        let b = g_batch.as_ref().unwrap();
                        let k = ObjGroupKey {
                            o_type: b.o_type.get(g_i),
                            o_key: b.o_key.get(g_i),
                        };
                        aggs.entry(k).or_insert_with(AggStateStar::new).observe(
                            s,
                            f_mult,
                            want_min,
                            want_max,
                            want_sample,
                        );
                        g_i += 1;
                    }
                    fs = next_filter_group(&mut fcur, &mut f_batch, &mut f_i)?;
                }
            }
        }
    } else {
        // General path: fold the filter predicates into a subject -> bag
        // multiplicity map (the product of per-subject row counts; a subject
        // absent from any filter predicate drops out).
        let mut s_counts: Option<FxHashMap<u64, u64>> = None;
        for p in filter_preds {
            let Some(next) = collect_subject_counts_for_predicate_group(
                store,
                ctx,
                g_id,
                p,
                s_counts.as_ref().map(|s| s as &FxHashMap<u64, u64>),
            )?
            else {
                return Ok(None);
            };
            s_counts = Some(next);
            if s_counts
                .as_ref()
                .is_some_and(std::collections::HashMap::is_empty)
            {
                break;
            }
        }
        let s_counts = s_counts.unwrap_or_default();
        if s_counts.is_empty() {
            return Ok(Some(crate::binding::Batch::empty(schema)?));
        }

        while let Some(batch) = cursor
            .next_batch()
            .map_err(|e| QueryError::Internal(format!("cursor batch: {e}")))?
        {
            ctx.check_cancelled()?;
            for i in 0..batch.row_count {
                let s = batch.s_id.get(i);
                let Some(&mult) = s_counts.get(&s) else {
                    continue;
                };
                let k = ObjGroupKey {
                    o_type: batch.o_type.get(i),
                    o_key: batch.o_key.get(i),
                };
                aggs.entry(k).or_insert_with(AggStateStar::new).observe(
                    s,
                    mult,
                    want_min,
                    want_max,
                    want_sample,
                );
            }
        }
    }

    if aggs.is_empty() {
        return Ok(Some(crate::binding::Batch::empty(schema)?));
    }

    // Select top-k by count desc.
    let mut rows: Vec<(ObjGroupKey, AggStateStar)> = aggs.into_iter().collect();
    rows.sort_unstable_by(|a, b| {
        b.1.count.cmp(&a.1.count).then_with(|| {
            a.0.o_type
                .cmp(&b.0.o_type)
                .then_with(|| a.0.o_key.cmp(&b.0.o_key))
        })
    });
    if rows.len() > limit {
        rows.truncate(limit);
    }

    // Build output columns.
    let view = BinaryGraphView::with_novelty(Arc::clone(store), g_id, ctx.dict_novelty.clone())
        .with_namespace_codes_fallback(ctx.namespace_codes_fallback.clone());
    // SPARQL COUNT yields xsd:integer (§18.5.1.6), matching the materialized /
    // streaming aggregate paths; xsd:long here re-typed the count (agg02).
    let dt_count = WellKnownDatatypes::new().xsd_integer;

    let mut col_o1: Vec<Binding> = Vec::with_capacity(rows.len());
    let mut col_count: Vec<Binding> = Vec::with_capacity(rows.len());
    let mut col_min: Vec<Binding> = Vec::new();
    let mut col_max: Vec<Binding> = Vec::new();
    let mut col_sample: Vec<Binding> = Vec::new();
    if want_min {
        col_min = Vec::with_capacity(rows.len());
    }
    if want_max {
        col_max = Vec::with_capacity(rows.len());
    }
    if want_sample {
        col_sample = Vec::with_capacity(rows.len());
    }

    for (k, st) in rows {
        if k.o_type == OType::IRI_REF.as_u16() {
            col_o1.push(Binding::encoded_sid(k.o_key));
        } else {
            let val = view
                .decode_value(k.o_type, k.o_key, p_id)
                .map_err(|e| QueryError::Internal(format!("decode_value: {e}")))?;
            let dt = store
                .resolve_datatype_sid_for_value(k.o_type, &val)
                .unwrap_or_else(|| Sid::new(0, ""));
            let lang = store.resolve_lang_tag(k.o_type).map(Arc::from);
            col_o1.push(Binding::Lit {
                val,
                dtc: match lang {
                    Some(tag) => fluree_db_core::DatatypeConstraint::LangTag(tag),
                    None => fluree_db_core::DatatypeConstraint::Explicit(dt),
                },
                t: None,
                op: None,
                p_id: None,
            });
        }
        col_count.push(Binding::lit(
            FlakeValue::Long(st.count as i64),
            dt_count.clone(),
        ));
        if want_min {
            col_min.push(st.min_s.map_or(Binding::Unbound, Binding::encoded_sid));
        }
        if want_max {
            col_max.push(st.max_s.map_or(Binding::Unbound, Binding::encoded_sid));
        }
        if want_sample {
            col_sample.push(st.sample_s.map_or(Binding::Unbound, Binding::encoded_sid));
        }
    }

    // Assemble columns in the SELECT schema order.
    let mut cols: Vec<Vec<Binding>> = Vec::with_capacity(schema.len());
    for v in schema.iter().copied() {
        if v == group_var {
            cols.push(col_o1.clone());
        } else if v == count_var {
            cols.push(col_count.clone());
        } else if Some(v) == min_var {
            cols.push(col_min.clone());
        } else if Some(v) == max_var {
            cols.push(col_max.clone());
        } else if Some(v) == sample_var {
            cols.push(col_sample.clone());
        } else {
            return Err(QueryError::Internal(format!(
                "group-by-object star: schema var {v:?} not produced by fast path"
            )));
        }
    }
    Ok(Some(crate::binding::Batch::new(schema, cols).map_err(
        |e| QueryError::execution(format!("batch build: {e}")),
    )?))
}

// ---------------------------------------------------------------------------
// Counts over a default graph of several graphs
// ---------------------------------------------------------------------------

/// One slice `(o_type, o_key)` of a bound object, and the members whose index
/// may hold rows of it.
#[derive(Debug, PartialEq)]
struct SliceHolders {
    o_type: u16,
    o_key: u64,
    members: smallvec::SmallVec<[GraphId; 4]>,
}

/// One member's slices of a bound object: `(o_type, o_key)`, with no key for a
/// whole `o_type` left to the decoded-value filter.
type Slices = Vec<(u16, Option<u64>)>;

/// Which way a union count went (`zero`; `rows`: every slice held by one
/// member, its row count; `merge`: some slice held by several, merged), so a
/// test can tell them apart; all three proceed at the site.
fn stamp_union_count_plan(plan: &'static str) {
    tracing::debug!(
        target: crate::fast_path_outcome::FAST_PATH_TARGET,
        site = PREDICATE_OBJECT_COUNT_UNION_SITE,
        plan,
        "union count plan",
    );
}

/// The slices of a bound object over a default graph of several graphs, each
/// with the members whose index may hold it, from each member's slices of the
/// object (`None` key: a whole `o_type`, left to the decoded-value filter) and
/// whether a member's index may hold a slice (`may_hold`: false only when no
/// leaf of its POST branch overlaps the slice).
///
/// A union is a set of facts (#2109's fact identity): two members' rows are
/// one fact only when their keys `(s, p, o_type, o_key, o_i)` are equal, and
/// different slices are different facts. That holds for every key except a NumBig
/// handle, which a member's own arena numbers: one value can have different
/// handles in two members, or several in one (scale aliases), and the
/// general plan counts them by value. So NumBig slices are counted only when
/// one member holds exactly one; otherwise the count declines `ArenaType`,
/// as it does for a slice with no key.
fn plan_union_count(
    member_slices: impl IntoIterator<Item = (GraphId, Slices)>,
    mut may_hold: impl FnMut(GraphId, u16, u64) -> bool,
) -> std::result::Result<Vec<SliceHolders>, FastPathFallback> {
    let mut slices: Vec<SliceHolders> = Vec::new();
    let mut numbig_slices = 0;
    for (g_id, member_slices) in member_slices {
        for (o_type, o_key) in member_slices {
            let Some(o_key) = o_key else {
                return Err(FastPathFallback::ArenaType);
            };
            if !may_hold(g_id, o_type, o_key) {
                continue;
            }
            if crate::merged_cursor::is_graph_scoped_arena(o_type) {
                numbig_slices += 1;
                if numbig_slices > 1 {
                    return Err(FastPathFallback::ArenaType);
                }
            }
            match slices
                .iter_mut()
                .find(|s| (s.o_type, s.o_key) == (o_type, o_key))
            {
                Some(slice) => slice.members.push(g_id),
                None => slices.push(SliceHolders {
                    o_type,
                    o_key,
                    members: smallvec::smallvec![g_id],
                }),
            }
        }
    }
    Ok(slices)
}

/// The POST key range of predicate `p_id`: every object, list index and
/// subject.
fn post_predicate_bounds(g_id: GraphId, p_id: u32) -> (RunRecordV2, RunRecordV2) {
    let key = |s_id, o_type, o_key, o_i| RunRecordV2 {
        s_id: SubjectId(s_id),
        o_key,
        p_id,
        t: 0,
        o_i,
        o_type,
        g_id,
    };
    (key(0, 0, 0, 0), key(u64::MAX, u16::MAX, u64::MAX, u32::MAX))
}

/// The POST key range of one slice `(p, o_type, o_key)`: every list index and
/// subject.
fn post_slice_bounds(
    g_id: GraphId,
    p_id: u32,
    o_type: u16,
    o_key: u64,
) -> (RunRecordV2, RunRecordV2) {
    let key = |s_id, o_i| RunRecordV2 {
        s_id: SubjectId(s_id),
        o_key,
        p_id,
        t: 0,
        o_i,
        o_type,
        g_id,
    };
    (key(0, 0), key(u64::MAX, u32::MAX))
}

/// Whether `g_id`'s index may hold rows of a slice: a leaf of its POST branch
/// overlaps the slice's key range. Read from the branch manifest alone; a
/// `false` proves the graph holds none (with no novelty to add).
fn index_may_hold(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
    o_type: u16,
    o_key: u64,
) -> bool {
    let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Post) else {
        return false;
    };
    let (min, max) = post_slice_bounds(g_id, p_id, o_type, o_key);
    !branch
        .find_leaves_in_range(&min, &max, cmp_v2_for_order(RunSortOrder::Post))
        .is_empty()
}

/// The facts of one slice that two or more members hold: the merged cursor
/// over its POST range across them, each fact once. With no projection a
/// batch is a row count.
fn merged_slice_count(
    set: &MemberSet<'_>,
    holders: &[GraphId],
    p_id: u32,
    (o_type, o_key): (u16, u64),
    tracker: &fluree_db_core::Tracker,
    cancellation: &QueryCancellation,
) -> Result<std::result::Result<i64, FastPathFallback>> {
    let members = set
        .members()
        .iter()
        .filter(|m| holders.contains(&m.g_id()))
        .copied()
        .collect();
    let holder_set = MemberSet::new(set.store(), members)
        .ok_or_else(|| QueryError::Internal("a merged slice of one member".to_string()))?;
    let (min, max) = post_slice_bounds(holders[0], p_id, o_type, o_key);
    let mut cursor = match MergedRangeCursor::new(
        &holder_set,
        RunSortOrder::Post,
        &min,
        &max,
        fluree_db_binary_index::BinaryFilter::default(),
        ColumnProjection {
            output: ColumnSet::EMPTY,
            internal: ColumnSet::EMPTY,
        },
    ) {
        Ok(cursor) => cursor.with_tracker(tracker),
        Err(decline) => return Ok(Err(decline.fallback())),
    };
    let mut rows = 0;
    loop {
        check_cancelled(cancellation)?;
        match cursor.next_batch()? {
            Ok(Some(batch)) => rows += batch.row_count as i64,
            Ok(None) => return Ok(Ok(rows)),
            Err(decline) => return Ok(Err(decline.fallback())),
        }
    }
}

/// `COUNT(?s)` over `?s <p> <o>` in a default graph of several graphs, every
/// member current: the facts holding the object in some member, each once
/// (the union is a set of facts, a fact being one value and list
/// position), which is what the general plan counts.
///
/// Each member contributes its own slices of the object (a bare number has one
/// per numeric datatype it observed, and its arena's handles). Different
/// slices are different facts, so the count is a sum over slices: a slice one
/// member holds counts its rows, which are distinct facts; a slice several
/// hold counts the merged cursor's rows.
fn count_bound_object_union(
    snapshot: &LedgerSnapshot,
    set: &MemberSet<'_>,
    predicate: &Ref,
    object: &Term,
    stats_view: Option<&fluree_db_core::StatsView>,
    tracker: &fluree_db_core::Tracker,
    cancellation: &QueryCancellation,
) -> Result<std::result::Result<i64, FastPathFallback>> {
    let store = set.store();
    let p_id = resolve_predicate_id_v6(predicate, store)?;
    let member_slices: Vec<(GraphId, Slices)> = if let Term::Value(
        value @ (FlakeValue::Long(_)
        | FlakeValue::Double(_)
        | FlakeValue::Decimal(_)
        | FlakeValue::BigInt(_)),
    ) = object
    {
        let mut out = Vec::with_capacity(set.members().len());
        for member in set.members() {
            let g_id = member.g_id();
            let slices = crate::binary_scan::untyped_numeric_slices(
                store,
                g_id,
                Some(p_id),
                crate::binary_scan::observed_datatypes(stats_view, g_id, p_id),
                value,
                true,
            )
            .ok_or_else(|| QueryError::Internal("non-finite numeric constant".to_string()))?;
            out.push((
                g_id,
                slices
                    .into_iter()
                    .map(|(ot, ok)| (ot.as_u16(), ok))
                    .collect(),
            ));
        }
        out
    } else {
        // Every other object is one term with one key in every member:
        // subject and string ids are ledger-wide, and a value that would
        // need an arena handle does not translate (an error: fallback).
        // With no novelty, a translation miss means no member holds it.
        let Some((o_type, o_key)) =
            translate_term_to_v6(object, snapshot, store, p_id, set.members()[0].g_id())?
        else {
            stamp_union_count_plan("zero");
            return Ok(Ok(0));
        };
        set.members()
            .iter()
            .map(|m| (m.g_id(), vec![(o_type, Some(o_key))]))
            .collect()
    };
    let slices = match plan_union_count(member_slices, |g_id, o_type, o_key| {
        index_may_hold(store, g_id, p_id, o_type, o_key)
    }) {
        Ok(slices) => slices,
        Err(reason) => return Ok(Err(reason)),
    };
    let mut total = 0;
    let mut merged = false;
    for slice in &slices {
        check_cancelled(cancellation)?;
        total += if let [g_id] = slice.members[..] {
            count_object_key_v6(store, g_id, p_id, slice.o_type, slice.o_key, cancellation)?
        } else {
            merged = true;
            match merged_slice_count(
                set,
                &slice.members,
                p_id,
                (slice.o_type, slice.o_key),
                tracker,
                cancellation,
            )? {
                Ok(rows) => rows,
                Err(reason) => return Ok(Err(reason)),
            }
        };
    }
    stamp_union_count_plan(match (slices.is_empty(), merged) {
        (true, _) => "zero",
        (false, false) => "rows",
        (false, true) => "merge",
    });
    Ok(Ok(total))
}

/// The groups a grouped object count keeps, best first: `(o_type, o_key, count)`.
type TopKGroups = Vec<(u16, u64, i64)>;

/// `GROUP BY ?o` with `COUNT(?s)` over `?s <p> ?o`, the best `limit` groups,
/// in a default graph of several graphs, every member current.
///
/// One merged read of the predicate's POST range across the members gives
/// each fact once, in object order (the union is a set of facts, one
/// per value and list position, as the merge keys). A group's merged rows are
/// its solutions and their run is its count. The merge declines on
/// arena-typed objects (a handle names no value across members, and handle
/// order is not value order).
fn group_count_union(
    set: &MemberSet<'_>,
    predicate: &Ref,
    limit: usize,
    tracker: &fluree_db_core::Tracker,
    cancellation: &QueryCancellation,
) -> Result<std::result::Result<TopKGroups, FastPathFallback>> {
    let p_id = resolve_predicate_id_v6(predicate, set.store())?;
    let (min, max) = post_predicate_bounds(set.members()[0].g_id(), p_id);
    let projection = ColumnProjection {
        output: ColumnSet::single(ColumnId::OType).union(ColumnSet::single(ColumnId::OKey)),
        internal: ColumnSet::EMPTY,
    };
    let mut cursor = match MergedRangeCursor::new(
        set,
        RunSortOrder::Post,
        &min,
        &max,
        fluree_db_binary_index::BinaryFilter::default(),
        projection,
    ) {
        Ok(cursor) => cursor.with_tracker(tracker),
        Err(decline) => return Ok(Err(decline.fallback())),
    };

    let mut heap: BinaryHeap<GroupTopK> =
        BinaryHeap::with_capacity(fluree_db_core::topk::initial_capacity(limit));
    let mut cur: Option<(u16, u64)> = None;
    let mut run: i64 = 0;
    loop {
        check_cancelled(cancellation)?;
        let batch = match cursor.next_batch()? {
            Ok(Some(batch)) => batch,
            Ok(None) => break,
            Err(decline) => return Ok(Err(decline.fallback())),
        };
        for row in 0..batch.row_count {
            let group = (batch.o_type.get(row), batch.o_key.get(row));
            if cur == Some(group) {
                run += 1;
            } else {
                offer_topk(
                    &mut heap,
                    limit,
                    GroupTopK {
                        count: run,
                        o_type: cur.map_or(0, |c| c.0),
                        o_key: cur.map_or(0, |c| c.1),
                    },
                );
                cur = Some(group);
                run = 1;
            }
        }
    }
    offer_topk(
        &mut heap,
        limit,
        GroupTopK {
            count: run,
            o_type: cur.map_or(0, |c| c.0),
            o_key: cur.map_or(0, |c| c.1),
        },
    );
    Ok(Ok(heap
        .into_sorted_vec()
        .into_iter()
        .map(|g| (g.o_type, g.o_key, g.count))
        .collect()))
}

#[cfg(test)]
mod union_count_tests {
    use super::{plan_union_count, SliceHolders};
    use crate::fast_path_outcome::FastPathFallback;
    use fluree_db_core::o_type::OType;

    const INT: u16 = OType::XSD_INTEGER.as_u16();
    const DBL: u16 = OType::XSD_DOUBLE.as_u16();
    const IRI: u16 = OType::IRI_REF.as_u16();
    const BIG: u16 = OType::NUM_BIG_OVERFLOW.as_u16();

    fn holders(o_type: u16, o_key: u64, members: &[u16]) -> SliceHolders {
        SliceHolders {
            o_type,
            o_key,
            members: members.iter().copied().collect(),
        }
    }

    #[test]
    fn slices_and_their_holders() {
        let all = |_: u16, _: u16, _: u64| true;
        assert_eq!(
            plan_union_count(vec![(1, vec![]), (2, vec![])], all),
            Ok(vec![])
        );
        // The slice in no member's index.
        assert_eq!(
            plan_union_count(
                vec![(1, vec![(IRI, Some(7))]), (2, vec![(IRI, Some(7))])],
                |_, _, _| false
            ),
            Ok(vec![])
        );
        // One member holds it; both hold it.
        assert_eq!(
            plan_union_count(
                vec![(1, vec![(IRI, Some(7))]), (2, vec![(IRI, Some(7))])],
                |g, _, _| g == 2
            ),
            Ok(vec![holders(IRI, 7, &[2])])
        );
        assert_eq!(
            plan_union_count(
                vec![(1, vec![(IRI, Some(7))]), (2, vec![(IRI, Some(7))])],
                all
            ),
            Ok(vec![holders(IRI, 7, &[1, 2])])
        );
        // A bare number: each member's own datatypes, each slice its own.
        assert_eq!(
            plan_union_count(
                vec![
                    (1, vec![(INT, Some(1)), (DBL, Some(9))]),
                    (2, vec![(INT, Some(1))])
                ],
                all
            ),
            Ok(vec![holders(INT, 1, &[1, 2]), holders(DBL, 9, &[1])])
        );
    }

    #[test]
    fn numbig_slices() {
        let all = |_: u16, _: u16, _: u64| true;
        // One member's one handle is a slice like any other.
        assert_eq!(
            plan_union_count(
                vec![(1, vec![(INT, Some(1))]), (2, vec![(BIG, Some(4))])],
                all
            ),
            Ok(vec![holders(INT, 1, &[1]), holders(BIG, 4, &[2])])
        );
        // Handles in two members, or two in one: their facts are equal by
        // value, which handles cannot show.
        for slices in [
            vec![(1, vec![(BIG, Some(0))]), (2, vec![(BIG, Some(0))])],
            vec![(1, vec![(BIG, Some(0))]), (2, vec![(BIG, Some(3))])],
            vec![(1, vec![(BIG, Some(0)), (BIG, Some(1))]), (2, vec![])],
        ] {
            assert_eq!(
                plan_union_count(slices.clone(), all),
                Err(FastPathFallback::ArenaType),
                "{slices:?}"
            );
        }
        // A handle no member's index holds is no slice.
        assert_eq!(
            plan_union_count(
                vec![(1, vec![(BIG, Some(0))]), (2, vec![(BIG, Some(0))])],
                |g, _, _| g == 1
            ),
            Ok(vec![holders(BIG, 0, &[1])])
        );
        // A whole o_type left to the value filter declines.
        assert_eq!(
            plan_union_count(
                vec![(1, vec![]), (2, vec![(INT, Some(1)), (BIG, None)])],
                all
            ),
            Err(FastPathFallback::ArenaType),
        );
    }
}

#[cfg(test)]
mod topk_tests {
    use super::{offer_topk, GroupTopK};
    use std::collections::BinaryHeap;

    /// Feed `(count, o_type, o_key)` groups through the bounded heap and return
    /// the kept groups in emit order `(o_type, o_key, count)`.
    fn topk(items: &[(i64, u16, u64)], limit: usize) -> Vec<(u16, u64, i64)> {
        let mut heap: BinaryHeap<GroupTopK> = BinaryHeap::new();
        for &(count, o_type, o_key) in items {
            offer_topk(
                &mut heap,
                limit,
                GroupTopK {
                    count,
                    o_type,
                    o_key,
                },
            );
        }
        heap.into_sorted_vec()
            .into_iter()
            .map(|g| (g.o_type, g.o_key, g.count))
            .collect()
    }

    #[test]
    fn orders_by_count_desc() {
        let r = topk(&[(2, 0, 0), (5, 0, 0), (3, 0, 0), (1, 0, 0)], 3);
        assert_eq!(r, vec![(0, 0, 5), (0, 0, 3), (0, 0, 2)]);
    }

    #[test]
    fn tie_break_is_otype_then_okey_ascending() {
        // All count 3: keep order is lower o_type first, then lower o_key.
        let r = topk(&[(3, 2, 9), (3, 1, 5), (3, 1, 2), (3, 0, 100)], 3);
        assert_eq!(r, vec![(0, 100, 3), (1, 2, 3), (1, 5, 3)]);
    }

    #[test]
    fn evicts_worst_and_respects_limit() {
        assert_eq!(
            topk(&[(1, 0, 0), (2, 0, 0), (3, 0, 0), (4, 0, 0), (5, 0, 0)], 2),
            vec![(0, 0, 5), (0, 0, 4)]
        );
        assert!(topk(&[], 3).is_empty());
        assert!(topk(&[(5, 0, 0)], 0).is_empty(), "limit 0 keeps nothing");
        assert!(
            topk(&[(0, 0, 0), (-1, 0, 0)], 3).is_empty(),
            "non-positive runs are not emitted"
        );
    }

    #[test]
    fn count_dominates_tie_break() {
        // A lower-count group must never outrank a higher-count one regardless of
        // o_type/o_key.
        let r = topk(&[(10, 9, 9), (3, 0, 0), (5, 1, 1)], 2);
        assert_eq!(r, vec![(9, 9, 10), (1, 1, 5)]);
    }
}
