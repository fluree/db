use crate::binding::Binding;
use crate::context::{ExecutionContext, WellKnownDatatypes};
use crate::error::{QueryError, Result};
use crate::fast_path_common::{fast_path_store, normalize_pred_sid};
use crate::ir::triple::Term;
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
    BinaryCursor, BinaryFilter, BinaryGraphView, BinaryIndexStore, ColumnBatch, ColumnProjection,
    ColumnSet, RunSortOrder,
};
use fluree_db_core::o_type::OType;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::{FlakeValue, GraphId, Sid};
use rustc_hash::{FxHashMap, FxHashSet};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Shared free functions
// ---------------------------------------------------------------------------

#[inline]
fn should_fallback(ctx: &ExecutionContext<'_>) -> bool {
    fast_path_store(ctx).is_none()
}

#[inline]
fn allow_cursor_fast_path(ctx: &ExecutionContext<'_>) -> bool {
    // History mode is filtered at the planner — see
    // `execute::operator_tree::build_operator_tree_inner` — so this gate
    // doesn't duplicate that check.
    !ctx.is_multi_ledger()
        && ctx.from_t.is_none()
        && ctx.policy_enforcer.as_ref().is_none_or(|p| p.is_root())
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
        use crate::aggregate::AggregateFn;
        use crate::dataset_operator::DatasetOperator;
        use crate::group_aggregate::{GroupAggregateOperator, StreamingAggSpec};
        use crate::ir::triple::{Ref, TriplePattern};
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
            distinct: false,
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
        use crate::aggregate::AggregateFn;
        use crate::dataset_operator::DatasetOperator;
        use crate::group_aggregate::{GroupAggregateOperator, StreamingAggSpec};
        use crate::ir::triple::{Ref, TriplePattern};

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
            distinct: false,
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
            order,
            c,
            leaf_id,
            leaflet_idx,
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
    store: &BinaryIndexStore,
    g_id: GraphId,
    predicate: &crate::ir::triple::Ref,
    object: &Term,
) -> Result<i64> {
    let p_id = resolve_predicate_id_v6(predicate, store)?;

    // Translate the bound object term into V6 (o_type, o_key).
    let (target_o_type, target_o_key) = translate_term_to_v6(object, store, p_id, g_id)?;

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

    let mut counts: HashMap<(u16, u64), i64> = HashMap::new();
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
                *counts.entry(prefix).or_insert(0) += entry.row_count as i64;
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
                *counts.entry((ot, ok)).or_insert(0) += 1;
            }
        }
    }

    // Sort by count desc, truncate.
    let mut rows: Vec<(u16, u64, i64)> = counts
        .into_iter()
        .map(|((ot, ok), c)| (ot, ok, c))
        .collect();
    rows.sort_unstable_by(|a, b| b.2.cmp(&a.2).then(a.0.cmp(&b.0)).then(a.1.cmp(&b.1)));
    rows.truncate(limit);

    Ok(rows)
}

/// Translate a bound object `Term` to V6 `(o_type, o_key)`.
fn translate_term_to_v6(
    term: &Term,
    store: &BinaryIndexStore,
    _p_id: u32,
    _g_id: GraphId,
) -> Result<(u16, u64)> {
    match term {
        Term::Sid(sid) => {
            let s_id = store
                .find_subject_id_by_parts(sid.namespace_code, &sid.name)
                .map_err(|e| QueryError::execution(format!("find_subject_id_by_parts: {e}")))?
                .ok_or_else(|| {
                    QueryError::execution("bound object SID not found in V6 dict".to_string())
                })?;
            Ok((OType::IRI_REF.as_u16(), s_id))
        }
        Term::Iri(iri) => {
            let s_id = store
                .find_subject_id(iri)
                .map_err(|e| QueryError::execution(format!("find_subject_id: {e}")))?
                .ok_or_else(|| {
                    QueryError::execution("bound object IRI not found in V6 dict".to_string())
                })?;
            Ok((OType::IRI_REF.as_u16(), s_id))
        }
        Term::Value(val) => {
            // For literal values, we need the FlakeValue → (o_type, o_key) translation.
            // Use the Sid-based dt info from the FlakeValue if available.
            let (ot, ok) = crate::binary_scan::value_to_otype_okey_simple(val, store)?;
            Ok((ot.as_u16(), ok))
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

fn build_psot_cursor_for_predicate_group(
    ctx: &ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    pred_sid: Sid,
    p_id: u32,
    projection: ColumnProjection,
) -> Result<Option<BinaryCursor>> {
    let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Psot) else {
        return Ok(None);
    };
    let branch = Arc::clone(branch);

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

    let filter = BinaryFilter {
        p_id: Some(p_id),
        ..Default::default()
    };

    let mut cursor = BinaryCursor::new(
        Arc::clone(store),
        RunSortOrder::Psot,
        branch,
        &min_key,
        &max_key,
        filter,
        projection,
    );
    cursor.set_to_t(ctx.to_t);

    // Overlay merge — pre-filter by predicate.
    if ctx.overlay.is_some() {
        use std::collections::HashMap as StdHashMap;
        let dn = ctx.dict_novelty.clone().unwrap_or_else(|| {
            Arc::new(fluree_db_core::dict_novelty::DictNovelty::new_uninitialized())
        });
        let mut ephemeral_preds: StdHashMap<fluree_db_core::Sid, u32> = StdHashMap::new();
        let mut next_ep = store.predicate_count();
        let mut ops = Vec::new();
        let mut translate_failed = false;
        let mut translate_fail_count: u32 = 0;

        ctx.overlay().for_each_overlay_flake(
            g_id,
            fluree_db_core::IndexType::Psot,
            None,
            None,
            true,
            ctx.to_t,
            &mut |flake| {
                if flake.p != pred_sid {
                    return;
                }
                match crate::binary_scan::translate_one_flake_v3_pub(
                    flake,
                    store,
                    Some(&dn),
                    ctx.runtime_small_dicts,
                    &mut ephemeral_preds,
                    &mut next_ep,
                    g_id,
                ) {
                    Ok(op) => ops.push(op),
                    Err(e) => {
                        translate_failed = true;
                        translate_fail_count = translate_fail_count.saturating_add(1);
                        if translate_fail_count == 1 {
                            tracing::warn!(
                                error = %e,
                                s = %flake.s,
                                p = %flake.p,
                                t = flake.t,
                                op = flake.op,
                                "group-by-object star: overlay flake translation failed; disabling fast path for correctness"
                            );
                        }
                    }
                }
            },
        );
        if translate_failed {
            tracing::debug!(
                failures = translate_fail_count,
                "group-by-object star: falling back due to overlay translation failures"
            );
            return Ok(None);
        }

        if !ops.is_empty() {
            fluree_db_binary_index::read::types::sort_overlay_ops(&mut ops, RunSortOrder::Psot);
            fluree_db_binary_index::read::types::resolve_overlay_ops(&mut ops);
            cursor.set_overlay_ops(ops);
        }
        cursor.set_epoch(ctx.overlay().epoch());
    }

    Ok(Some(cursor))
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
        build_psot_cursor_for_predicate_group(ctx, store, g_id, sid, p_id, projection)?
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
        build_psot_cursor_for_predicate_group(ctx, store, g_id, sid, p_id, projection)?
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
            build_psot_cursor_for_predicate_group(ctx, store, g_id, fp_sid, fp_id, fp_proj)?
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
