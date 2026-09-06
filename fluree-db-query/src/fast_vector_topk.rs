//! Flat-rank vector scoring fast path.
//!
//! Serves the canonical vector-similarity shape —
//!
//! ```sparql
//! SELECT ?s ?score WHERE {
//!   VALUES ?target { <query vector> }        # or an inline constant
//!   ?s <vecPred> ?vec .
//!   BIND(dotProduct(?vec, ?target) AS ?score)
//!   FILTER(?score > K)                        # optional threshold
//! } ORDER BY DESC(?score) LIMIT k             # optional order/limit
//! ```
//!
//! — as a tight scan over the predicate's rows scoring the packed f32 vector
//! arena directly, instead of driving every row through the scan → bind →
//! filter → sort operator pipeline. Scores are computed exactly as the eval
//! path computes them (f32 shard values widened to f64, same SIMD dot
//! kernel), so results are identical; only the per-row machinery is removed.
//!
//! Overlay correctness: rows come from the shared overlay-merging cursor
//! (novelty asserts appear, retracted base rows are cancelled — the same
//! `merge_overlay_into_batch` lane every count fast path uses). Novelty
//! vector values (ephemeral handles) are pre-decoded once into a map before
//! the scan. Large base scans parallelize across subject-range partitions on
//! the global rayon pool with per-partition sliced overlay ops.
//!
//! Bails to the generic pipeline (`Ok(None)` from the compute closure) on
//! any uncertainty: non-vector rows on the predicate, missing arena with
//! novelty present, dimension mismatch, unresolvable overlay ops, policy /
//! multi-ledger / time-travel contexts.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::o_type::OType;
use fluree_db_core::value_id::ObjKind;
use fluree_db_core::{FlakeValue, GraphId, Sid};
use rustc_hash::FxHashMap;

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::eval::vector_math;
use crate::fast_path_common::{
    allow_cursor_fast_path, build_overlay_cursor_for_predicate,
    build_overlay_cursor_for_subject_range, cached_overlay_ops, count_rows_for_predicate_psot,
    cursor_projection_sid_otype_okey, empty_batch, leaf_entries_for_predicate, normalize_pred_sid,
    parallel_map_pooled, slice_overlay_ops_by_subject, FastPathOperator,
};
use crate::ir::{Expression, Function, Pattern, Query, Ref, Term};
use crate::operator::BoxedOperator;
use crate::sort::SortDirection;
use crate::var_registry::VarId;

// ============================================================================
// Detection
// ============================================================================

/// Detected flat-rank scoring query, ready for the shard-scan executor.
pub struct VectorTopKSpec {
    /// SELECT vars in projection order (subset of `{subject_var, score_var}`).
    pub projected: Vec<VarId>,
    pub subject_var: VarId,
    pub score_var: VarId,
    /// The vector predicate.
    pub pred: Ref,
    /// Query/target vector (f32-quantized at ingest, widened — same values
    /// the pipeline sees).
    pub target: Vec<f64>,
    /// Score threshold from `FILTER(?score > K)` / `>= K`; `bool` = strict.
    pub threshold: Option<(f64, bool)>,
    /// `ORDER BY DESC(?score)` present.
    pub order_desc: bool,
    pub limit: Option<usize>,
    pub offset: usize,
}

/// Extract an f64 threshold constant from a filter comparison operand.
fn const_to_f64(e: &Expression) -> Option<f64> {
    match e {
        Expression::Const(FlakeValue::Long(n)) => Some(*n as f64),
        Expression::Const(FlakeValue::Double(d)) => Some(*d),
        _ => None,
    }
}

/// Detect the flat-rank vector scoring shape. Shape-only; runtime gates
/// (store/arena presence, policy, overlay resolution) live in the operator.
pub fn detect_vector_topk(query: &Query) -> Option<VectorTopKSpec> {
    if query.grouping.is_some() || query.post_values.is_some() || !query.order_binds.is_empty() {
        return None;
    }
    if query.output.is_distinct() {
        return None;
    }

    // Ordering: none, or exactly `DESC(?score)` (score var checked below).
    let order_var = match query.ordering.as_slice() {
        [] => None,
        [ob] if ob.direction == SortDirection::Descending => Some(ob.var),
        _ => return None,
    };

    // A bare LIMIT with no ORDER BY means "any k rows"; the generic pipeline
    // returns them in scan order, so imposing top-k-by-score here would change
    // which rows come back. Only serve LIMIT alongside DESC(?score) — otherwise
    // decline so the shape routes to the pipeline unchanged.
    if query.limit.is_some() && order_var.is_none() {
        return None;
    }

    // Walk WHERE: exactly one triple, one dotProduct bind, ≤1 threshold
    // filter, ≤1 single-row VALUES supplying the target var.
    let mut triple: Option<(VarId, Ref, VarId)> = None;
    let mut bind: Option<(VarId, &Expression)> = None;
    let mut filter: Option<&Expression> = None;
    let mut values: Option<(VarId, &[Binding])> = None;

    for p in &query.patterns {
        match p {
            Pattern::Triple(tp) => {
                if triple.is_some() || tp.dtc.is_some() || !tp.p_bound() {
                    return None;
                }
                let Ref::Var(sv) = &tp.s else { return None };
                let Term::Var(ov) = &tp.o else { return None };
                if matches!(&tp.p, Ref::Var(_)) {
                    return None;
                }
                triple = Some((*sv, tp.p.clone(), *ov));
            }
            Pattern::Bind { var, expr } => {
                if bind.is_some() {
                    return None;
                }
                bind = Some((*var, expr));
            }
            Pattern::Filter(f) => {
                if filter.is_some() {
                    return None;
                }
                filter = Some(f);
            }
            Pattern::Values { vars, rows } => {
                if values.is_some() || vars.len() != 1 || rows.len() != 1 {
                    return None;
                }
                values = Some((vars[0], rows[0].as_slice()));
            }
            _ => return None,
        }
    }

    let (subject_var, pred, vec_var) = triple?;
    let (score_var, bind_expr) = bind?;
    if subject_var == vec_var || score_var == subject_var || score_var == vec_var {
        return None;
    }
    if let Some(ov) = order_var {
        if ov != score_var {
            return None;
        }
    }

    // Bind must be `dotProduct` over {?vec, target} in either order (dot is
    // commutative), target = inline const vector or the VALUES-bound var.
    let Expression::Call { func, args } = bind_expr else {
        return None;
    };
    if *func != Function::DotProduct || args.len() != 2 {
        return None;
    }
    let target_of = |e: &Expression| -> Option<Vec<f64>> {
        match e {
            Expression::Const(FlakeValue::Vector(v)) => Some(v.to_vec()),
            Expression::Var(v) => {
                let (tv, row) = values.as_ref()?;
                if *v != *tv || *v == vec_var || *v == subject_var || *v == score_var {
                    return None;
                }
                match row.first()? {
                    Binding::Lit {
                        val: FlakeValue::Vector(t),
                        ..
                    } => Some(t.to_vec()),
                    _ => None,
                }
            }
            _ => None,
        }
    };
    let is_vec_var = |e: &Expression| matches!(e, Expression::Var(v) if *v == vec_var);
    let target = if is_vec_var(&args[0]) {
        target_of(&args[1])?
    } else if is_vec_var(&args[1]) {
        target_of(&args[0])?
    } else {
        return None;
    };
    if target.is_empty() {
        return None;
    }
    // If a VALUES exists, it must be the one supplying the target (a stray
    // unrelated VALUES changes semantics).
    if let Some((tv, _)) = &values {
        let used = args
            .iter()
            .any(|a| matches!(a, Expression::Var(v) if v == tv));
        if !used {
            return None;
        }
    }

    // Optional threshold filter: `?score > K` / `>= K` (or mirrored `K < ?score`).
    let threshold = match filter {
        None => None,
        Some(Expression::Call { func, args }) if args.len() == 2 => {
            let is_score = |e: &Expression| matches!(e, Expression::Var(v) if *v == score_var);
            let t = match func {
                Function::Gt if is_score(&args[0]) => (const_to_f64(&args[1])?, true),
                Function::Ge if is_score(&args[0]) => (const_to_f64(&args[1])?, false),
                Function::Lt if is_score(&args[1]) => (const_to_f64(&args[0])?, true),
                Function::Le if is_score(&args[1]) => (const_to_f64(&args[0])?, false),
                _ => return None,
            };
            Some(t)
        }
        Some(_) => return None,
    };

    // Projection: non-empty subset of {subject_var, score_var}. The vector
    // var itself must not escape (materializing vectors is the pipeline's job).
    let projected = query.output.projected_vars()?;
    if projected.is_empty()
        || projected
            .iter()
            .any(|v| *v != subject_var && *v != score_var)
    {
        return None;
    }

    Some(VectorTopKSpec {
        projected,
        subject_var,
        score_var,
        pred,
        target,
        threshold,
        order_desc: order_var.is_some(),
        limit: query.limit,
        offset: query.offset.unwrap_or(0),
    })
}

// ============================================================================
// Executor
// ============================================================================

/// Heap entry ordered ascending by (score, then descending s_id), so the
/// min-heap root is the weakest kept row and equal-score evictions drop the
/// larger s_id first (final order: score desc, s_id asc — deterministic ties).
struct HeapEntry {
    score: f64,
    s_id: u64,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.score.total_cmp(&other.score) == std::cmp::Ordering::Equal && self.s_id == other.s_id
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.s_id.cmp(&self.s_id))
    }
}

/// Per-partition fold state: bounded top-k heap or unbounded passing list.
enum Fold {
    TopK {
        need: usize,
        heap: BinaryHeap<Reverse<HeapEntry>>,
    },
    All(Vec<(u64, f64)>),
}

impl Fold {
    fn new(need: Option<usize>) -> Self {
        match need {
            Some(n) => Fold::TopK {
                need: n,
                // Cap the eager reservation: `n` is a user literal (limit +
                // offset), so a large-but-legal LIMIT would otherwise reserve an
                // oversized heap per partition. The heap grows if genuinely
                // needed, and `push` caps it at `need`. `saturating_add` also
                // avoids the `n + 1` overflow debug-panic at pathological limits.
                heap: BinaryHeap::with_capacity(n.saturating_add(1).min(4096)),
            },
            None => Fold::All(Vec::new()),
        }
    }

    #[inline]
    fn push(&mut self, s_id: u64, score: f64) {
        match self {
            Fold::TopK { need, heap } => {
                let entry = HeapEntry { score, s_id };
                if heap.len() < *need {
                    heap.push(Reverse(entry));
                } else if let Some(weakest) = heap.peek() {
                    if entry > weakest.0 {
                        heap.pop();
                        heap.push(Reverse(entry));
                    }
                }
            }
            Fold::All(v) => v.push((s_id, score)),
        }
    }

    fn into_rows(self) -> Vec<(u64, f64)> {
        match self {
            Fold::TopK { heap, .. } => heap
                .into_iter()
                .map(|Reverse(e)| (e.s_id, e.score))
                .collect(),
            Fold::All(v) => v,
        }
    }
}

/// Shared per-scan scoring state.
struct Scorer<'a> {
    /// Pinned arena shards for direct zero-probe base-row reads. `None`
    /// when the predicate has no arena (novelty-only rows).
    snapshot: Option<&'a fluree_db_binary_index::arena::vector::VectorArenaSnapshot>,
    target: &'a [f64],
    /// Pre-decoded, pre-widened novelty vector values by ephemeral handle.
    ephemeral: &'a FxHashMap<u64, Vec<f64>>,
    threshold: Option<(f64, bool)>,
    vector_o_type: u16,
}

impl Scorer<'_> {
    /// Score one row. `Ok(None)` = row filtered out; `Err(Bail)` = shape
    /// assumption violated (non-vector row, dims mismatch, missing handle) —
    /// the whole fast path must defer to the pipeline.
    #[inline]
    fn score(&self, o_type: u16, o_key: u64, widen_scratch: &mut Vec<f64>) -> BailOr<Option<f64>> {
        if o_type != self.vector_o_type {
            return Err(Bail);
        }
        let score = if (o_key as u32) >= crate::dict_overlay::EPHEMERAL_VECTOR_BASE {
            let Some(v) = self.ephemeral.get(&o_key) else {
                return Err(Bail);
            };
            if v.len() != self.target.len() {
                return Err(Bail);
            }
            vector_math::dot_f64(v, self.target)
        } else {
            let Some(f32s) = self.snapshot.and_then(|s| s.get_f32(o_key as u32)) else {
                return Err(Bail);
            };
            if f32s.len() != self.target.len() {
                return Err(Bail);
            }
            // Widen to f64 and use the same kernel the eval path uses —
            // scores are bit-identical to the pipeline.
            widen_scratch.clear();
            widen_scratch.extend(f32s.iter().map(|&x| f64::from(x)));
            vector_math::dot_f64(widen_scratch, self.target)
        };
        if let Some((bound, strict)) = self.threshold {
            let pass = if strict {
                score > bound
            } else {
                score >= bound
            };
            if !pass {
                return Ok(None);
            }
        }
        Ok(Some(score))
    }
}

/// Sentinel error for "defer to the generic pipeline".
struct Bail;
type BailOr<T> = std::result::Result<T, Bail>;

/// Scored `(s_id, score)` rows from one scan lane / partition.
type ScoredRows = Vec<(u64, f64)>;

/// Minimum base rows before the partitioned parallel scan is worth its setup.
const PARALLEL_MIN_ROWS: u64 = 8_192;
/// Cap on scan partitions (matches the count fast paths' scale).
const MAX_PARTITIONS: usize = 16;

/// Build the flat-rank vector scoring fast-path operator.
pub fn vector_topk_operator(
    spec: VectorTopKSpec,
    fallback: Option<BoxedOperator>,
) -> BoxedOperator {
    let VectorTopKSpec {
        projected,
        subject_var,
        score_var,
        pred,
        target,
        threshold,
        order_desc,
        limit,
        offset,
    } = spec;
    let schema: Arc<[VarId]> = Arc::from(projected.into_boxed_slice());

    Box::new(FastPathOperator::with_schema(
        Arc::clone(&schema),
        move |ctx| {
            let Some(store) = ctx.binary_store.as_ref() else {
                return Ok(None);
            };
            if !allow_cursor_fast_path(ctx) {
                return Ok(None);
            }
            // Base + overlay reconstructs any `to_t >= index_t`; earlier
            // needs the history sidecar — defer.
            if ctx.to_t < store.max_t() {
                return Ok(None);
            }
            let overlay_present = ctx
                .overlay
                .map(fluree_db_core::OverlayProvider::epoch)
                .unwrap_or(0)
                != 0;
            let g_id: GraphId = ctx.binary_g_id;

            let _span = tracing::debug_span!(
                "fast_vector_topk",
                pred = ?pred,
                limit,
                offset,
                order_desc,
                overlay = overlay_present,
            )
            .entered();

            // Resolve the predicate. Absent p_id with overlay may mean
            // novelty-only rows → defer; without overlay the result is empty.
            let pred_sid = normalize_pred_sid(store.as_ref(), &pred)?;
            let p_id = match store.sid_to_p_id(&pred_sid) {
                Some(p) => p,
                None => {
                    if overlay_present {
                        return Ok(None);
                    }
                    return Ok(Some(empty_batch(Arc::clone(&schema))?));
                }
            };

            // Overlay ops for the predicate (cached per execution). `None`
            // means an op failed to resolve — defer.
            let ops = match cached_overlay_ops(ctx, store, g_id, RunSortOrder::Psot, &pred_sid)? {
                Some(o) => o,
                None => return Ok(None),
            };

            // Pre-decode novelty vector values (ephemeral handles) once, so
            // partition workers never need the ExecutionContext.
            let mut ephemeral: FxHashMap<u64, Vec<f64>> = FxHashMap::default();
            for op in ops.iter() {
                if op.o_type != OType::VECTOR.as_u16() {
                    // Non-vector novelty row on the predicate — the per-row
                    // o_type gate would bail mid-scan anyway; defer up front.
                    return Ok(None);
                }
                let key = op.o_key;
                if (key as u32) >= crate::dict_overlay::EPHEMERAL_VECTOR_BASE
                    && !ephemeral.contains_key(&key)
                {
                    let Some(gv) = ctx.graph_view() else {
                        return Ok(None);
                    };
                    match gv.decode_value_from_kind(ObjKind::VECTOR_ID.as_u8(), key, p_id, 0, 0) {
                        Ok(FlakeValue::Vector(v)) => {
                            ephemeral.insert(key, v.iter().copied().collect());
                        }
                        // Non-vector novelty value on the predicate, or a
                        // decode failure — defer.
                        _ => return Ok(None),
                    }
                }
            }

            let need = limit.map(|k| k.saturating_add(offset));
            if need == Some(0) {
                return Ok(Some(empty_batch(Arc::clone(&schema))?));
            }

            // Pin the arena shards once — the scan reads them directly with
            // no per-row cache probes. A shard load failure defers.
            let snapshot = match store.vector_arena_snapshot(g_id, p_id) {
                Ok(s) => s,
                Err(_) => return Ok(None),
            };
            let scorer = Scorer {
                snapshot: snapshot.as_ref(),
                target: &target,
                ephemeral: &ephemeral,
                threshold,
                vector_o_type: OType::VECTOR.as_u16(),
            };

            let total_rows = count_rows_for_predicate_psot(store, g_id, p_id)?;
            let scan = VectorScan {
                ctx,
                store,
                g_id,
                p_id,
                scorer: &scorer,
                need,
            };

            // ── Scan lanes ─────────────────────────────────────────────
            // Partitioned lane first (large predicates); its outcome decides
            // whether we fall back to the serial lane or defer straight to the
            // pipeline. A genuine bail must NOT re-scan serially.
            let partitioned: Option<Vec<(u64, f64)>> = if total_rows >= PARALLEL_MIN_ROWS {
                match scan_partitioned(&scan, &ops)? {
                    PartitionScan::Rows(r) => Some(r),
                    PartitionScan::Bailed => return Ok(None),
                    PartitionScan::TooFewPartitions => None,
                }
            } else {
                None
            };
            let rows = match partitioned {
                Some(r) => r,
                None => match scan_serial(&scan, &pred_sid)? {
                    Some(r) => r,
                    None => return Ok(None),
                },
            };

            // ── Order / offset / limit ─────────────────────────────────
            let mut rows = rows;
            if order_desc || limit.is_some() {
                rows.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            }
            let start = offset.min(rows.len());
            let end = match limit {
                Some(k) => (start + k).min(rows.len()),
                None => rows.len(),
            };
            let out_rows = &rows[start..end];

            tracing::debug!(returned = out_rows.len(), "fast-path: vector-topk emitted");

            // ── Build the output batch ─────────────────────────────────
            // Overlay-lane subjects may be novelty-only (absent from the
            // persisted dict) — materialize via the novelty-aware view.
            let view = if overlay_present {
                Some(ctx.graph_view().ok_or_else(|| {
                    QueryError::Internal("graph view unavailable for overlay subjects".into())
                })?)
            } else {
                None
            };
            let mut cols: Vec<Vec<Binding>> = Vec::with_capacity(schema.len());
            for var in schema.iter() {
                let mut col: Vec<Binding> = Vec::with_capacity(out_rows.len());
                if *var == subject_var {
                    for (s_id, _) in out_rows {
                        let b = match &view {
                            Some(gv) => Binding::sid(
                                gv.resolve_subject_sid(*s_id)
                                    .map_err(|e| QueryError::from_io("resolve_subject_sid", e))?,
                            ),
                            None => Binding::encoded_sid(*s_id),
                        };
                        col.push(b);
                    }
                } else if *var == score_var {
                    for (_, score) in out_rows {
                        col.push(Binding::lit(FlakeValue::Double(*score), Sid::xsd_double()));
                    }
                } else {
                    return Ok(None);
                }
                cols.push(col);
            }
            Ok(Some(Batch::new(Arc::clone(&schema), cols)?))
        },
        fallback,
        "vector-topk",
    ))
}

/// Serial lane: whole-predicate overlay-merging cursor.
fn scan_serial(scan: &VectorScan, pred_sid: &Sid) -> Result<Option<Vec<(u64, f64)>>> {
    let &VectorScan {
        ctx,
        store,
        g_id,
        p_id,
        scorer,
        need,
    } = scan;
    let Some(mut cursor) = build_overlay_cursor_for_predicate(
        ctx,
        store,
        g_id,
        RunSortOrder::Psot,
        pred_sid.clone(),
        p_id,
        cursor_projection_sid_otype_okey(),
    )?
    else {
        return Ok(None);
    };
    let mut fold = Fold::new(need);
    let mut scratch: Vec<f64> = Vec::new();
    while let Some(batch) = cursor
        .next_batch()
        .map_err(|e| QueryError::Internal(format!("cursor batch: {e}")))?
    {
        for r in 0..batch.row_count {
            match scorer.score(batch.o_type.get(r), batch.o_key.get(r), &mut scratch) {
                Ok(Some(score)) => fold.push(batch.s_id.get(r), score),
                Ok(None) => {}
                Err(Bail) => return Ok(None),
            }
        }
    }
    Ok(Some(fold.into_rows()))
}

/// Outcome of the partitioned scan lane. Distinguishes "not enough partitions,
/// fall back to the serial lane" from "a partition genuinely bailed, defer
/// straight to the pipeline" — the caller must not re-scan serially on a bail
/// (it would hit the same bail row and waste a full pass).
enum PartitionScan {
    /// Too few CPUs / partitions to parallelize — try the serial lane.
    TooFewPartitions,
    /// A partition hit a `Bail` condition — defer to the generic pipeline.
    Bailed,
    /// Scored rows.
    Rows(Vec<(u64, f64)>),
}

/// Shared context for one vector top-k scan of predicate `p_id` in graph
/// `g_id`: where to read (`store`/`ctx`), how to score (`scorer`), and the
/// top-k bound (`need`). Both the partitioned and serial lanes take this so the
/// lane functions only carry their lane-specific extras.
struct VectorScan<'a> {
    ctx: &'a ExecutionContext<'a>,
    store: &'a Arc<BinaryIndexStore>,
    g_id: GraphId,
    p_id: u32,
    scorer: &'a Scorer<'a>,
    need: Option<usize>,
}

/// Parallel lane: subject-range partitions at leaf boundaries, each with a
/// bounded overlay cursor and its subject-sliced overlay ops, folded on the
/// global rayon pool. Returns [`PartitionScan::TooFewPartitions`] to fall back
/// to the serial lane, or [`PartitionScan::Bailed`] to defer to the pipeline.
fn scan_partitioned(
    scan: &VectorScan,
    ops: &crate::fast_path_common::SharedOverlayOps,
) -> Result<PartitionScan> {
    let &VectorScan {
        ctx,
        store,
        g_id,
        p_id,
        scorer,
        need,
    } = scan;
    let to_t = ctx.to_t;
    let epoch = ctx
        .overlay
        .map(fluree_db_core::OverlayProvider::epoch)
        .unwrap_or(0);
    let ncpu = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);
    let k = ncpu.min(MAX_PARTITIONS);
    if k < 2 {
        return Ok(PartitionScan::TooFewPartitions);
    }
    // Candidate subject boundaries: leaf first-subjects when there are
    // enough leaves; else refine to leaflet first-subjects (opens the few
    // driver leaves, which the partitions scan anyway).
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
    let candidates: Vec<u64> = if leaves.len() >= k {
        leaves.iter().map(|e| e.first_key.s_id.as_u64()).collect()
    } else {
        use fluree_db_binary_index::format::run_record_v2::read_ordered_key_v2;
        let mut bounds: Vec<u64> = Vec::new();
        for leaf in leaves {
            let handle = store
                .open_leaf_handle(&leaf.leaf_cid, leaf.sidecar_cid.as_ref(), false)
                .map_err(|e| QueryError::from_io("leaf open", e))?;
            for entry in &handle.dir().entries {
                if entry.row_count == 0 || entry.p_const != Some(p_id) {
                    continue;
                }
                let first = read_ordered_key_v2(RunSortOrder::Psot, &entry.first_key);
                bounds.push(first.s_id.as_u64());
            }
        }
        bounds
    };
    if candidates.len() < 2 {
        return Ok(PartitionScan::TooFewPartitions);
    }
    let mut bounds: Vec<u64> = vec![0];
    for j in 1..k {
        let b = candidates[j * candidates.len() / k];
        if b > *bounds.last().expect("nonempty") {
            bounds.push(b);
        }
    }
    bounds.push(u64::MAX);
    if bounds.len() < 3 {
        return Ok(PartitionScan::TooFewPartitions);
    }
    let ranges: Vec<(u64, u64)> = bounds.windows(2).map(|w| (w[0], w[1])).collect();

    let cancellation = &ctx.cancellation;
    let partials: Vec<Result<Option<ScoredRows>>> = parallel_map_pooled(ranges, |(lo, hi)| {
        crate::fast_path_common::bail_if_cancelled(cancellation)?;
        let sliced = slice_overlay_ops_by_subject(ops, lo, hi);
        let Some(mut cursor) = build_overlay_cursor_for_subject_range(
            store,
            g_id,
            p_id,
            cursor_projection_sid_otype_okey(),
            lo,
            hi,
            sliced,
            to_t,
            epoch,
        ) else {
            return Ok(Some(Vec::new()));
        };
        let mut fold = Fold::new(need);
        let mut scratch: Vec<f64> = Vec::new();
        while let Some(batch) = cursor
            .next_batch()
            .map_err(|e| QueryError::Internal(format!("cursor batch: {e}")))?
        {
            for r in 0..batch.row_count {
                let s = batch.s_id.get(r);
                if s < lo || s >= hi {
                    continue; // boundary leaf shared with adjacent partition
                }
                match scorer.score(batch.o_type.get(r), batch.o_key.get(r), &mut scratch) {
                    Ok(Some(score)) => fold.push(s, score),
                    Ok(None) => {}
                    Err(Bail) => return Ok(None),
                }
            }
        }
        Ok(Some(fold.into_rows()))
    });

    let mut merged = Fold::new(need);
    for partial in partials {
        match partial? {
            Some(rows) => {
                for (s, score) in rows {
                    merged.push(s, score);
                }
            }
            None => return Ok(PartitionScan::Bailed), // a partition hit a bail condition
        }
    }
    Ok(PartitionScan::Rows(merged.into_rows()))
}
