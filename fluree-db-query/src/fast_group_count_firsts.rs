use crate::binding::Binding;
use crate::context::{ExecutionContext, WellKnownDatatypes};
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    allow_cursor_fast_path, build_psot_cursor_for_predicate, fast_path_store, normalize_pred_sid,
    subject_ref_to_s_id,
};
use crate::ir::triple::{Ref, Term};
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
use fluree_db_core::{FlakeValue, GraphId, LedgerSnapshot, Sid};
use rustc_hash::{FxHashMap, FxHashSet};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Shared free functions
// ---------------------------------------------------------------------------

#[inline]
fn should_fallback(ctx: &ExecutionContext<'_>) -> bool {
    fast_path_store(ctx).is_none()
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
            mode,
        }
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

        if should_fallback(ctx) {
            return self.open_fallback(ctx).await;
        }

        // Try V6 fast-path first (only when no novelty overlay — overlay delta merge not yet implemented).
        //
        // `ExecutionContext` always carries an overlay provider; `NoOverlay` has epoch=0.
        if ctx
            .overlay
            .map(fluree_db_core::OverlayProvider::epoch)
            .unwrap_or(0)
            == 0
        {
            if let Some(binary_index_store) = ctx.binary_store.as_ref() {
                match group_count_v6(
                    binary_index_store,
                    ctx.binary_g_id,
                    &self.predicate,
                    self.limit,
                ) {
                    Ok(v6_results) => {
                        self.results_v6 = Some(v6_results);
                        return Ok(());
                    }
                    Err(_) => {
                        // V6 path couldn't handle it — fall through to V5 or fallback.
                    }
                }
            }
        }

        // V5 leaf-scanning fast-path removed. Fall back to generic scan/aggregate.
        // TODO: V3 leaflet fast-path for group-count-firsts (port boundary-equality
        // optimization to V3 column-based format).
        self.open_fallback(ctx).await
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
        let g_id = ctx.binary_g_id;
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
                col_o.push(Binding::encoded_sid(o_key));
            } else {
                let val = view
                    .decode_value(o_type, o_key, p_id)
                    .map_err(|e| QueryError::Internal(format!("V6 decode_value: {e}")))?;
                let dt = binary_index_store
                    .resolve_datatype_sid(o_type)
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

        if should_fallback(ctx) {
            return self.open_fallback(ctx).await;
        }

        // Try V6 fast-path first (only when no novelty overlay — overlay delta merge not yet implemented).
        //
        // `ExecutionContext` always carries an overlay provider; `NoOverlay` has epoch=0.
        if ctx
            .overlay
            .map(fluree_db_core::OverlayProvider::epoch)
            .unwrap_or(0)
            == 0
        {
            if let Some(binary_index_store) = ctx.binary_store.as_ref() {
                match count_bound_object_v6(
                    ctx.active_snapshot,
                    binary_index_store,
                    ctx.binary_g_id,
                    &self.predicate,
                    &self.object,
                ) {
                    Ok(total) => {
                        self.count = total;
                        return Ok(());
                    }
                    Err(_) => {
                        // V6 path couldn't handle it — fall through to V5 or fallback.
                    }
                }
            }
        }

        // V5 leaf-scanning fast-path removed. Fall back to generic scan/aggregate.
        // TODO: V3 leaflet fast-path for predicate-object count (port boundary-equality
        // optimization to V3 column-based format).
        return self.open_fallback(ctx).await;
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
    if let Some(c) = cache {
        load_leaflet_columns_cached(
            leaf_bytes,
            entry,
            payload_base,
            c,
            fluree_db_binary_index::read::column_loader::LeafletDecodeSpec {
                leaf_id,
                leaflet_idx,
                order,
                decode_set: ColumnSet::ALL,
            },
        )
        .map_err(|e| QueryError::Internal(format!("load columns: {e}")))
    } else {
        let mut needed = ColumnSet::EMPTY;
        needed.insert(ColumnId::OKey);
        if entry.o_type_const.is_none() {
            needed.insert(ColumnId::OType);
        }
        // Mixed-predicate leaflets need the per-row `p_id` column to verify
        // each row belongs to the queried predicate; the cached path always
        // loads all columns, so this is only a concern for the no-cache path.
        if entry.p_const.is_none() {
            needed.insert(ColumnId::PId);
        }
        let projection = ColumnProjection {
            output: ColumnSet::EMPTY,
            internal: needed,
        };
        load_leaflet_columns(leaf_bytes, entry, payload_base, &projection, order)
            .map_err(|e| QueryError::Internal(format!("load columns: {e}")))
    }
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
) -> Result<i64> {
    let p_id = resolve_predicate_id_v6(predicate, store)?;

    // Translate the bound object term into V6 (o_type, o_key). A `None` here is
    // a *conclusive* base-dict miss (refs are resolved snapshot-aware); since
    // this path runs only with no novelty overlay, the count is exactly 0 —
    // return without a fallback full-predicate scan.
    let Some((target_o_type, target_o_key)) =
        translate_term_to_v6(object, snapshot, store, p_id, g_id)?
    else {
        return Ok(0);
    };

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
        let leaf_entry = &branch.leaves[leaf_idx];
        let bytes = store
            .get_leaf_bytes_sync(&leaf_entry.leaf_cid)
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

fn group_count_v6(
    store: &BinaryIndexStore,
    g_id: GraphId,
    predicate: &crate::ir::triple::Ref,
    limit: usize,
) -> Result<Vec<(u16, u64, i64)>> {
    let p_id = resolve_predicate_id_v6(predicate, store)?;

    let branch = store
        .branch_for_order(g_id, RunSortOrder::Post)
        .ok_or_else(|| QueryError::Internal("no POST branch for graph".to_string()))?;
    let cmp = cmp_v2_for_order(RunSortOrder::Post);

    let min_key = RunRecordV2 {
        s_id: SubjectId(0),
        o_key: 0,
        p_id,
        t: 0,
        o_i: 0,
        o_type: 0,
        g_id,
    };
    let max_key = RunRecordV2 {
        s_id: SubjectId(u64::MAX),
        o_key: u64::MAX,
        p_id,
        t: 0,
        o_i: u32::MAX,
        o_type: u16::MAX,
        g_id,
    };
    let leaf_range = branch.find_leaves_in_range(&min_key, &max_key, cmp);

    // Streaming run-length: POST order (p_id, o_type, o_key, …) makes every
    // object's rows physically contiguous, so COUNT(?subject) per object is the
    // length of its run — a single counter that persists across leaflet/leaf
    // boundaries, no per-row hashing. Completed runs feed a bounded top-K heap, so
    // there is no full sort of all distinct objects either.
    let mut heap: BinaryHeap<GroupTopK> = BinaryHeap::with_capacity(limit + 1);
    let mut cur: Option<(u16, u64)> = None;
    let mut run: i64 = 0;
    let cache = store.leaflet_cache();

    for leaf_idx in leaf_range.clone() {
        let leaf_entry = &branch.leaves[leaf_idx];
        let bytes = store
            .get_leaf_bytes_sync(&leaf_entry.leaf_cid)
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
                // the cached path always loads `p_id`, the no-cache path is
                // configured to load it via `load_v6_batch` when `p_const` is
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
///   Combined with the caller's no-novelty (`epoch == 0`) gate, "absent from
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

    fn observe(&mut self, s_id: u64, want_min: bool, want_max: bool, want_sample: bool) {
        self.count = self.count.saturating_add(1);
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
                    let Some(fallback) = &mut self.fallback else {
                        return Err(QueryError::Internal(
                            "group-by-object star topk fast-path unavailable and no fallback provided".into(),
                        ));
                    };
                    fallback.open(ctx).await?;
                    self.state = OperatorState::Open;
                    return Ok(());
                };
                self.result = Some(batch);
                self.emitted = false;
                self.fallback = None;
                self.state = OperatorState::Open;
                return Ok(());
            }
        }

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

fn collect_subject_set_for_predicate_group(
    store: &Arc<BinaryIndexStore>,
    ctx: &ExecutionContext<'_>,
    g_id: GraphId,
    pred: &crate::ir::triple::Ref,
    restrict_to: Option<&FxHashSet<u64>>,
) -> Result<Option<FxHashSet<u64>>> {
    let overlay_has_rows = ctx
        .overlay
        .map(fluree_db_core::OverlayProvider::epoch)
        .unwrap_or(0)
        != 0;
    let sid = normalize_pred_sid(store, pred)?;
    let Some(p_id) = store.sid_to_p_id(&sid) else {
        return if overlay_has_rows {
            Ok(None)
        } else {
            Ok(Some(FxHashSet::default()))
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

    let mut set: FxHashSet<u64> = FxHashSet::default();
    let mut last_s: Option<u64> = None;
    while let Some(batch) = cursor
        .next_batch()
        .map_err(|e| QueryError::Internal(format!("cursor batch: {e}")))?
    {
        for i in 0..batch.row_count {
            let s = batch.s_id.get(i);
            if last_s == Some(s) {
                continue;
            }
            last_s = Some(s);
            if let Some(r) = restrict_to {
                if !r.contains(&s) {
                    continue;
                }
            }
            set.insert(s);
        }
    }
    Ok(Some(set))
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
    let overlay_has_rows = ctx
        .overlay
        .map(fluree_db_core::OverlayProvider::epoch)
        .unwrap_or(0)
        != 0;
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
        let mut f_last: Option<u64> = None;

        let next_filter_subject = |fcur: &mut BinaryCursor,
                                   f_batch: &mut Option<ColumnBatch>,
                                   f_i: &mut usize,
                                   f_last: &mut Option<u64>|
         -> Result<Option<u64>> {
            loop {
                if f_batch.is_none() || *f_i >= f_batch.as_ref().unwrap().row_count {
                    *f_batch = fcur
                        .next_batch()
                        .map_err(|e| QueryError::Internal(format!("cursor batch: {e}")))?;
                    *f_i = 0;
                    if f_batch.is_none() {
                        return Ok(None);
                    }
                }
                let b = f_batch.as_ref().unwrap();
                let s = b.s_id.get(*f_i);
                *f_i += 1;
                if *f_last == Some(s) {
                    continue;
                }
                *f_last = Some(s);
                return Ok(Some(s));
            }
        };

        let peek_group_subject = |cursor: &mut BinaryCursor,
                                  g_batch: &mut Option<ColumnBatch>,
                                  g_i: &mut usize|
         -> Result<Option<u64>> {
            if g_batch.is_none() || *g_i >= g_batch.as_ref().unwrap().row_count {
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

        let mut fs = next_filter_subject(&mut fcur, &mut f_batch, &mut f_i, &mut f_last)?;
        while let (Some(gs), Some(cur_fs)) =
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
                    fs = next_filter_subject(&mut fcur, &mut f_batch, &mut f_i, &mut f_last)?;
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
                            want_min,
                            want_max,
                            want_sample,
                        );
                        g_i += 1;
                    }
                    fs = next_filter_subject(&mut fcur, &mut f_batch, &mut f_i, &mut f_last)?;
                }
            }
        }
    } else {
        // General path: build subject set S by intersecting filter predicates.
        let mut s_set: Option<FxHashSet<u64>> = None;
        for p in filter_preds {
            let Some(next) = collect_subject_set_for_predicate_group(
                store,
                ctx,
                g_id,
                p,
                s_set.as_ref().map(|s| s as &FxHashSet<u64>),
            )?
            else {
                return Ok(None);
            };
            s_set = Some(next);
            if s_set
                .as_ref()
                .is_some_and(std::collections::HashSet::is_empty)
            {
                break;
            }
        }
        let s_set = s_set.unwrap_or_default();
        if s_set.is_empty() {
            return Ok(Some(crate::binding::Batch::empty(schema)?));
        }

        while let Some(batch) = cursor
            .next_batch()
            .map_err(|e| QueryError::Internal(format!("cursor batch: {e}")))?
        {
            for i in 0..batch.row_count {
                let s = batch.s_id.get(i);
                if !s_set.contains(&s) {
                    continue;
                }
                let k = ObjGroupKey {
                    o_type: batch.o_type.get(i),
                    o_key: batch.o_key.get(i),
                };
                aggs.entry(k).or_insert_with(AggStateStar::new).observe(
                    s,
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
    let dt_count = WellKnownDatatypes::new().xsd_long;

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
                .resolve_datatype_sid(k.o_type)
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
