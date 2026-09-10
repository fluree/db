//! RangeSemiJoinOperator — keep/drop of a driving row by whether its subject
//! carries a value of some predicate inside a FILTER range whose bounds are
//! computed from the row itself.
//!
//! The shape it replaces is a probe followed by a correlated range filter on a
//! variable nothing else needs:
//!
//! ```text
//! ?product bsbm:productPropertyNumeric1 ?sim .
//! FILTER (?sim < ?orig + 120 && ?sim > ?orig - 120)     # ?sim used nowhere else
//! ```
//!
//! The generic chain probes the predicate once per driving row, materializes a
//! row per value, evaluates the filter per row, and — because `?sim` is dead
//! afterwards — deduplicates again. When the driving stream is large and the
//! range is selective (BSBM Explore Q5: ~13% of all products are candidates, the
//! range keeps ~12% of them) that is thousands of scattered probes and row
//! copies to keep a handful of survivors.
//!
//! This operator instead reads the predicate's in-range values ONCE per range
//! envelope — the union of the batch's per-row intervals, inclusive on both
//! sides — into a map from subject to values, then answers each driving row
//! with a hash lookup and an exact check of the row's own interval. The
//! preferred build is an index-level walk of the predicate's POST leaflets
//! (directory-key pruning, a binary search of the sorted key column, only the
//! subject and key columns decoded, no bindings for rows outside the
//! envelope). Whenever the walk cannot answer a batch — leaflets are not
//! authoritative on their own (novelty overlay, time-travel, a dataset scope,
//! a restricting policy), a bound is not numeric (the walk keys inline
//! numerics only), or the envelope would cover too many rows for the driving
//! stream — the batch is answered by the same batched subject probes the
//! nested loop would have used. If that lane declines too, one seeded
//! probe/filter plan answers the batch through the generic pipeline.
//!
//! Semantics: this is a SEMI-join — a row is kept at most once however many of
//! its values pass. That equals the probe+filter chain only where downstream
//! cannot observe row multiplicity (`where_dedup_safe`), which is the same
//! license the WHERE-level early dedup relies on; the planner folds the probe
//! only under it, and only when the value variable is dead after its filter.
//! History mode is excluded at plan time (a ground probe matches once per
//! version there).
//!
//! The per-row check is exact by construction: a range-shaped filter is the
//! conjunction of its bounds, so a numeric value against the row's evaluated
//! numeric bounds (with the filter's own strictness) IS the filter; a numeric
//! value against a non-numeric bound is the type error the filter would raise
//! (row dropped); anything non-numeric goes back through the original
//! expression. A driving row whose subject is unbound is answered by an exact
//! seeded evaluation of the original probe and filter, so a partially-ground
//! row keeps join semantics.

use crate::binding::{Batch, Binding, RowAccess};
use crate::context::ExecutionContext;
use crate::distinct::DistinctOperator;
use crate::error::{QueryError, Result};
use crate::eval::PreparedBoolExpression;
use crate::execute::build_where_operators_seeded;
use crate::fast_path_common::{
    leaf_entries_for_predicate, root_or_no_policy, subject_probe_lane_plan, try_normalize_pred_sid,
    ProbeLanePlan, ProbeOps,
};
use crate::group_aggregate::{binding_to_group_key_normalized, GroupKeyOwned};
use crate::ir::triple::{Ref, Term, TriplePattern};
use crate::ir::{Expression, Pattern};
use crate::join::{
    batched_subject_probe_binary, make_dict_overlay, prepare_leaf_for_scan, LeafScan,
    SubjectProbeParams,
};
use crate::object_binding::{equality_norm, materialized_object_binding, EqualityNorm};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::seed::BatchSeedOperator;
use crate::temporal_mode::PlanningContext;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::run_record_v2::read_ordered_key_v2;
use fluree_db_binary_index::read::column_loader::{
    load_leaflet_columns, load_leaflet_columns_cached, LeafletDecodeSpec,
};
use fluree_db_binary_index::read::column_types::{ColumnData, ColumnProjection, ColumnSet};
use fluree_db_binary_index::{BinaryIndexStore, ColumnBatch, RunSortOrder};
use fluree_db_core::o_type::{DecodeKind, OType};
use fluree_db_core::{FlakeValue, GraphId, ObjKey, ObjKind, ObjectBounds};
use num_traits::ToPrimitive;
use rustc_hash::FxHashMap;
use std::cmp::Ordering;
use std::sync::Arc;

/// Routing-stamp site for this lane (see `fast_path_outcome`): `Proceed` when
/// a batch was answered from a built index, `Fallback(GateDeclined)` when it
/// was answered by probes instead (walk capped or declined, or a non-numeric
/// bound).
pub const RANGE_SEMIJOIN_SITE: &str = "range-semijoin";

/// Rows the walk may collect for one envelope: at least this many, or
/// [`WALK_ROWS_PER_DRIVING_ROW`] per driving row seen so far, whichever is
/// larger. Beyond it the walk stops paying against per-row probes. The floor
/// only matters for a small first batch and stays a fraction of one leaflet,
/// so an inflated driving estimate (a statistics-less root) cannot buy a big
/// walk for a query that a few probes would answer.
const WALK_ROW_FLOOR: usize = 4_000;
const WALK_ROWS_PER_DRIVING_ROW: usize = 16;
// A changing anchor must not re-walk an ever-growing envelope per batch.
// Fixed-anchor queries build once; allow a few expansions, then use probes.
const MAX_WALKS_PER_CONDITION: usize = 4;

fn walkable_numeric(o_type: OType) -> bool {
    o_type.is_numeric() || o_type == OType::NUM_BIG_OVERFLOW
}

fn walk_row_floor() -> usize {
    static ENV: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *ENV.get_or_init(|| {
        std::env::var("FLUREE_RANGE_SEMIJOIN_WALK_FLOOR")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(WALK_ROW_FLOOR)
    })
}

/// One folded `?s <p> ?v . FILTER(range on ?v)` pair.
#[derive(Clone, Debug)]
pub struct RangeSemiJoinCondition {
    /// The probe's fixed predicate.
    pub predicate: Ref,
    /// The probe's object variable — bound only while re-checking `filter`.
    pub value_var: VarId,
    /// `?v >[=] lower` — an expression over variables the driving row binds.
    pub lower: Option<(Expression, bool)>,
    /// `?v <[=] upper` — likewise.
    pub upper: Option<(Expression, bool)>,
    /// The original FILTER, re-evaluated for values the bounds cannot decide.
    pub filter: Expression,
}

impl RangeSemiJoinCondition {
    fn probe(&self, subject_var: VarId) -> TriplePattern {
        TriplePattern::new(
            Ref::Var(subject_var),
            self.predicate.clone(),
            Term::Var(self.value_var),
        )
    }

    fn bound_vars(&self) -> Vec<VarId> {
        let mut vars: Vec<VarId> = Vec::new();
        for (expr, _) in self.lower.iter().chain(self.upper.iter()) {
            for v in expr.referenced_vars() {
                if !vars.contains(&v) {
                    vars.push(v);
                }
            }
        }
        vars
    }
}

/// Inclusive envelope of a batch's row intervals. `None` on a side means
/// unbounded there (the condition has no bound on that side, or a row's bound
/// was non-numeric so no key range can represent it).
#[derive(Clone, Debug, Default)]
struct Envelope {
    lower: Option<FlakeValue>,
    upper: Option<FlakeValue>,
}

impl Envelope {
    fn covers(&self, other: &Envelope) -> bool {
        let lower_ok = match (&self.lower, &other.lower) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(a), Some(b)) => a.cmp(b) != Ordering::Greater,
        };
        let upper_ok = match (&self.upper, &other.upper) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(a), Some(b)) => a.cmp(b) != Ordering::Less,
        };
        lower_ok && upper_ok
    }

    fn union(&self, other: &Envelope) -> Envelope {
        Envelope {
            lower: match (&self.lower, &other.lower) {
                (Some(a), Some(b)) => Some(if a.cmp(b) == Ordering::Greater {
                    b.clone()
                } else {
                    a.clone()
                }),
                _ => None,
            },
            upper: match (&self.upper, &other.upper) {
                (Some(a), Some(b)) => Some(if a.cmp(b) == Ordering::Less {
                    b.clone()
                } else {
                    a.clone()
                }),
                _ => None,
            },
        }
    }
}

/// A value of the probed predicate as the semi-join keeps it: inline numerics
/// stay raw so the row check is a comparison, anything else keeps its binding
/// for the expression path.
#[derive(Clone, Debug)]
enum WalkValue {
    Int(i64),
    Float(f64),
    Other(Box<Binding>),
}

impl WalkValue {
    fn from_binding(binding: &Binding) -> Self {
        match binding {
            Binding::EncodedLit { o_kind, o_key, .. } if *o_kind == ObjKind::NUM_INT.as_u8() => {
                WalkValue::Int(ObjKey::from_u64(*o_key).decode_i64())
            }
            Binding::EncodedLit { o_kind, o_key, .. } if *o_kind == ObjKind::NUM_F64.as_u8() => {
                WalkValue::Float(ObjKey::from_u64(*o_key).decode_f64())
            }
            Binding::Lit {
                val: FlakeValue::Long(n),
                ..
            } => WalkValue::Int(*n),
            Binding::Lit {
                val: FlakeValue::Double(d),
                ..
            } => WalkValue::Float(*d),
            other => WalkValue::Other(Box::new(other.clone())),
        }
    }

    fn numeric(&self) -> Option<FlakeValue> {
        match self {
            WalkValue::Int(n) => Some(FlakeValue::Long(*n)),
            WalkValue::Float(f) => Some(FlakeValue::Double(*f)),
            WalkValue::Other(_) => None,
        }
    }
}

/// Values of one condition's predicate for a set of subjects, in one flat
/// arena chained per subject (`head` → `next`), so building and dropping cost
/// two allocations rather than one per subject.
struct ValueIndex {
    head: FxHashMap<GroupKeyOwned, u32>,
    values: Vec<(WalkValue, u32)>,
}

const NO_NEXT: u32 = u32::MAX;

impl ValueIndex {
    fn with_capacity(rows: usize) -> Self {
        Self {
            head: FxHashMap::with_capacity_and_hasher(rows, Default::default()),
            values: Vec::with_capacity(rows),
        }
    }

    fn push(&mut self, key: GroupKeyOwned, value: WalkValue) {
        let idx = self.values.len() as u32;
        let next = self.head.insert(key, idx).unwrap_or(NO_NEXT);
        self.values.push((value, next));
    }

    fn values_of(&self, key: &GroupKeyOwned) -> impl Iterator<Item = &WalkValue> {
        let mut cursor = self.head.get(key).copied();
        std::iter::from_fn(move || {
            let idx = cursor?;
            let (value, next) = &self.values[idx as usize];
            cursor = (*next != NO_NEXT).then_some(*next);
            Some(value)
        })
    }
}

/// The in-range values of one condition's predicate over a built envelope.
struct RangeIndex {
    envelope: Envelope,
    values: ValueIndex,
    rows: usize,
}

enum WalkOutcome {
    Built(RangeIndex),
    /// The envelope covers more rows than the driving stream justifies.
    Capped,
    /// Leaflets are not authoritative here (overlay, time-travel, dataset,
    /// policy) or the predicate cannot be resolved.
    Declined,
}

/// A driving row with one extra binding, for re-checking the folded FILTER.
struct RowWithValue<'a> {
    batch: &'a Batch,
    row: usize,
    value_var: VarId,
    value: &'a Binding,
}

impl RowAccess for RowWithValue<'_> {
    fn get(&self, var: VarId) -> Option<&Binding> {
        if var == self.value_var {
            Some(self.value)
        } else {
            self.batch.get(self.row, var)
        }
    }

    fn for_each_binding(&self, f: &mut dyn FnMut(VarId, &Binding)) {
        for (col, var) in self.batch.schema().iter().enumerate() {
            f(*var, self.batch.get_by_col(self.row, col));
        }
        f(self.value_var, self.value);
    }
}

/// One driving row's own interval: the bounds as the filter evaluates them
/// (with the filter's strictness) plus their envelope contribution.
struct RowInterval {
    exact: ObjectBounds,
    /// Every present bound is numeric, so a numeric value is decided by
    /// `exact` alone.
    numeric: bool,
    envelope: Envelope,
}

fn as_f64(v: &FlakeValue) -> Option<f64> {
    match v {
        FlakeValue::Long(n) => Some(*n as f64),
        FlakeValue::Double(d) => Some(*d),
        FlakeValue::BigInt(b) => b.to_f64(),
        FlakeValue::Decimal(d) => d.to_f64(),
        _ => None,
    }
}

fn clamp_i64(f: f64) -> i64 {
    if f <= i64::MIN as f64 {
        i64::MIN
    } else if f >= i64::MAX as f64 {
        i64::MAX
    } else {
        f as i64
    }
}

/// Inclusive raw-key bounds of `envelope` for one inline numeric kind, widened
/// (integer ceil/floor, float neighbours) so no borderline row is lost; the
/// exact check decides them. `None`: the kind is not range-keyed (arena
/// decimals), so rows are compared by decoded value instead.
fn key_bounds(envelope: &Envelope, kind: DecodeKind) -> Option<(u64, u64)> {
    let f = |v: &FlakeValue| as_f64(v).filter(|f| !f.is_nan());
    match kind {
        DecodeKind::I64 => {
            // Decimal/BigInt -> f64 can round inward above 2^53. Widen
            // before ceil/floor; the exact value check removes extra rows.
            let lo = match &envelope.lower {
                None => u64::MIN,
                Some(FlakeValue::Long(n)) => ObjKey::encode_i64(*n).as_u64(),
                Some(v) => ObjKey::encode_i64(clamp_i64(f(v)?.next_down().ceil())).as_u64(),
            };
            let hi = match &envelope.upper {
                None => u64::MAX,
                Some(FlakeValue::Long(n)) => ObjKey::encode_i64(*n).as_u64(),
                Some(v) => ObjKey::encode_i64(clamp_i64(f(v)?.next_up().floor())).as_u64(),
            };
            Some((lo, hi))
        }
        DecodeKind::F64 => {
            let lo = match &envelope.lower {
                None => u64::MIN,
                Some(v) => ObjKey::encode_f64(f(v)?.next_down()).map_or(u64::MIN, ObjKey::as_u64),
            };
            let hi = match &envelope.upper {
                None => u64::MAX,
                Some(v) => ObjKey::encode_f64(f(v)?.next_up()).map_or(u64::MAX, ObjKey::as_u64),
            };
            Some((lo, hi))
        }
        _ => None,
    }
}

/// A leaflet's in-range rows, located by the walk's first pass and collected
/// by its second once the total is known to be within the cap.
struct WalkedLeaflet {
    batch: ColumnBatch,
    /// `Some((o_type, start, end))` for a homogeneous numeric leaflet whose
    /// key column was binary-searched; `None` for a mixed leaflet, compared
    /// row by row against the envelope.
    keyed: Option<(u16, usize, usize)>,
    p_const: Option<u32>,
    o_type_const: Option<u16>,
}

pub struct RangeSemiJoinOperator {
    child: Option<BoxedOperator>,
    fallback_dedup: Option<DistinctOperator>,
    subject_var: VarId,
    conditions: Vec<RangeSemiJoinCondition>,
    filters: Vec<PreparedBoolExpression>,
    /// Per condition: the driving-row variables its bounds read.
    bound_vars: Vec<Vec<VarId>>,
    schema: Arc<[VarId]>,
    planning: PlanningContext,
    state: OperatorState,
    norm: Option<EqualityNorm>,
    indexes: Vec<Option<RangeIndex>>,
    /// Per condition: the walk capped out or declined once, so later batches
    /// go straight to probes instead of re-walking an envelope that only grows
    /// (the decline conditions hold for the whole query).
    walk_off: Vec<bool>,
    walk_attempts: Vec<usize>,
    fallback_batches: usize,
    probed_rows: usize,
    kept_rows: usize,
    fallback_rows: usize,
    probe_batches: usize,
}

impl RangeSemiJoinOperator {
    /// `out_vars` trims the output to the variables still needed downstream:
    /// the child must carry the bound operands for the re-check, but nothing
    /// after this operator reads them.
    pub fn new(
        child: BoxedOperator,
        subject_var: VarId,
        conditions: Vec<RangeSemiJoinCondition>,
        out_vars: Option<&[VarId]>,
        planning: PlanningContext,
    ) -> Self {
        let schema: Arc<[VarId]> = crate::operator::compute_trimmed_vars(child.schema(), out_vars)
            .unwrap_or_else(|| Arc::from(child.schema().to_vec().into_boxed_slice()));
        let filters = conditions
            .iter()
            .map(|c| PreparedBoolExpression::new(c.filter.clone()))
            .collect();
        let bound_vars = conditions
            .iter()
            .map(RangeSemiJoinCondition::bound_vars)
            .collect();
        let n = conditions.len();
        Self {
            child: Some(child),
            fallback_dedup: None,
            subject_var,
            conditions,
            filters,
            bound_vars,
            schema,
            planning,
            state: OperatorState::Created,
            norm: None,
            indexes: (0..n).map(|_| None).collect(),
            walk_off: vec![false; n],
            walk_attempts: vec![0; n],
            fallback_batches: 0,
            probed_rows: 0,
            kept_rows: 0,
            fallback_rows: 0,
            probe_batches: 0,
        }
    }

    fn input(&self) -> &dyn Operator {
        match &self.fallback_dedup {
            Some(dedup) => dedup,
            None => self.child.as_deref().expect("semi-join child"),
        }
    }

    fn input_mut(&mut self) -> &mut dyn Operator {
        match &mut self.fallback_dedup {
            Some(dedup) => dedup,
            None => self.child.as_deref_mut().expect("semi-join child"),
        }
    }

    fn restore_child(&mut self) {
        if let Some(dedup) = self.fallback_dedup.take() {
            self.child = Some(dedup.into_child());
        }
    }

    fn eval_bound<R: RowAccess>(
        expr: &Expression,
        row: &R,
        ctx: &ExecutionContext<'_>,
    ) -> Option<FlakeValue> {
        match expr.eval_to_comparable(row, Some(ctx)) {
            Ok(Some(value)) => Some(FlakeValue::from(&value)),
            _ => None,
        }
    }

    /// The row's own interval for condition `c`, or `None` when a bound cannot
    /// be evaluated (the filter could not pass either). Memoized against the
    /// previous row through `memo`: the bound operands rarely change between
    /// adjacent rows — in BSBM Q5 they are the anchor's values on every row.
    fn row_interval(
        &self,
        c: usize,
        batch: &Batch,
        row: usize,
        ctx: &ExecutionContext<'_>,
        memo: &mut Option<(usize, Option<u32>)>,
        distinct: &mut Vec<RowInterval>,
    ) -> Option<u32> {
        if let Some((prev_row, prev)) = memo {
            let same_inputs = self.bound_vars[c]
                .iter()
                .all(|v| batch.get(*prev_row, *v) == batch.get(row, *v));
            if same_inputs {
                return *prev;
            }
        }
        let cond = &self.conditions[c];
        let view = batch.row_view(row);
        let mut exact = ObjectBounds::default();
        let mut envelope = Envelope::default();
        let mut numeric = true;
        let mut dropped = false;
        if let Some((expr, inclusive)) = &cond.lower {
            match view.as_ref().and_then(|v| Self::eval_bound(expr, v, ctx)) {
                Some(value) => {
                    if value.is_numeric() {
                        envelope.lower = Some(value.clone());
                    } else {
                        numeric = false;
                    }
                    exact.lower = Some((value, *inclusive));
                }
                None => dropped = true,
            }
        }
        if let Some((expr, inclusive)) = &cond.upper {
            match view.as_ref().and_then(|v| Self::eval_bound(expr, v, ctx)) {
                Some(value) => {
                    if value.is_numeric() {
                        envelope.upper = Some(value.clone());
                    } else {
                        numeric = false;
                    }
                    exact.upper = Some((value, *inclusive));
                }
                None => dropped = true,
            }
        }
        let out = (!dropped).then(|| {
            distinct.push(RowInterval {
                exact,
                numeric,
                envelope,
            });
            (distinct.len() - 1) as u32
        });
        *memo = Some((row, out));
        out
    }

    /// Does `value` satisfy the row's interval? A numeric value against
    /// numeric bounds is the filter itself; against a non-numeric bound it is
    /// the filter's type error; anything else re-runs the original expression.
    fn value_passes(
        &self,
        c: usize,
        interval: &RowInterval,
        batch: &Batch,
        row: usize,
        value: &WalkValue,
        ctx: &ExecutionContext<'_>,
    ) -> Result<bool> {
        match value {
            WalkValue::Other(binding) => {
                let view = RowWithValue {
                    batch,
                    row,
                    value_var: self.conditions[c].value_var,
                    value: binding,
                };
                self.filters[c].eval_to_bool_non_strict(&view, Some(ctx))
            }
            numeric => Ok(interval.numeric
                && numeric
                    .numeric()
                    .is_some_and(|v| interval.exact.matches(&v))),
        }
    }

    /// Index-level build: walk the predicate's POST leaflets directly, pruning
    /// each leaflet on its directory keys, binary-searching the sorted key
    /// column of a homogeneous numeric leaflet for the envelope's raw-key
    /// range, and decoding only the subject and key columns. Rows outside the
    /// envelope never become values. The first pass only counts, so an
    /// envelope past the cap costs the cached leaflet decodes and nothing more.
    fn build_index_walk(
        &self,
        c: usize,
        envelope: &Envelope,
        ctx: &ExecutionContext<'_>,
    ) -> Result<WalkOutcome> {
        let Some(store) = ctx.binary_store.as_ref() else {
            return Ok(WalkOutcome::Declined);
        };
        if ctx.is_multi_ledger()
            || ctx.from_t.is_some()
            || !ctx.overlay_free_single_graph()
            || ctx.to_t < store.max_t()
            || !root_or_no_policy(ctx)
        {
            return Ok(WalkOutcome::Declined);
        }
        let cond = &self.conditions[c];
        let Some(pred_sid) = try_normalize_pred_sid(store, &cond.predicate) else {
            return Ok(WalkOutcome::Declined);
        };
        let g_id = ctx.binary_g_id;
        // Overlay-free and the predicate is not in the index: nothing can match.
        let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
            return Ok(WalkOutcome::Built(RangeIndex {
                envelope: envelope.clone(),
                values: ValueIndex::with_capacity(0),
                rows: 0,
            }));
        };
        let bounds = ObjectBounds {
            lower: envelope.lower.clone().map(|v| (v, true)),
            upper: envelope.upper.clone().map(|v| (v, true)),
        };
        let cache = store.leaflet_cache();
        let narrow = ColumnSet::single(ColumnId::SId).union(ColumnSet::single(ColumnId::OKey));
        let mixed = narrow
            .union(ColumnSet::single(ColumnId::OType))
            .union(ColumnSet::single(ColumnId::PId));
        let cap = walk_row_floor().max(WALK_ROWS_PER_DRIVING_ROW * self.probed_rows);

        // Pass 1: locate the in-range rows per leaflet and count them.
        let mut leaflets: Vec<WalkedLeaflet> = Vec::new();
        let mut total = 0usize;
        for leaf in leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id) {
            ctx.check_cancelled()?;
            let LeafScan {
                leaf_bytes,
                header,
                dir,
                leaf_id,
                ..
            } = prepare_leaf_for_scan(store, leaf, false)?;
            for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
                if entry.row_count == 0 || entry.p_const.is_some_and(|p| p != p_id) {
                    continue;
                }
                // A leaflet homogeneous in predicate and numeric o_type is
                // sorted by raw key, so the envelope is one contiguous run.
                let key_range = match (entry.p_const, entry.o_type_const) {
                    (Some(_), Some(ot)) => {
                        let o_type = OType::from_u16(ot);
                        if !walkable_numeric(o_type) {
                            continue;
                        }
                        match key_bounds(envelope, o_type.decode_kind()) {
                            Some((lo, hi)) => {
                                let first =
                                    read_ordered_key_v2(RunSortOrder::Post, &entry.first_key).o_key;
                                let last =
                                    read_ordered_key_v2(RunSortOrder::Post, &entry.last_key).o_key;
                                if last < lo || first > hi {
                                    continue;
                                }
                                Some((ot, lo, hi))
                            }
                            None => None,
                        }
                    }
                    _ => None,
                };
                let decode_set = if key_range.is_some() { narrow } else { mixed };
                let leaflet_idx_u32 = u32::try_from(leaflet_idx)
                    .map_err(|_| QueryError::Internal("leaflet idx exceeds u32".to_string()))?;
                let batch = match &cache {
                    Some(cache) => load_leaflet_columns_cached(
                        &leaf_bytes,
                        entry,
                        dir.payload_base,
                        cache,
                        LeafletDecodeSpec {
                            leaf_id,
                            leaflet_idx: leaflet_idx_u32,
                            order: header.order,
                            decode_set,
                        },
                    ),
                    None => load_leaflet_columns(
                        &leaf_bytes,
                        entry,
                        dir.payload_base,
                        &ColumnProjection {
                            output: decode_set,
                            internal: ColumnSet::EMPTY,
                        },
                        header.order,
                    ),
                }
                .map_err(|e| QueryError::Internal(format!("load columns (range walk): {e}")))?;

                let keyed = match key_range {
                    Some((ot, lo, hi)) => {
                        let (start, end) = match &batch.o_key {
                            ColumnData::Block(keys) => {
                                let keys: &[u64] = keys.as_ref();
                                (
                                    keys.partition_point(|k| *k < lo),
                                    keys.partition_point(|k| *k <= hi),
                                )
                            }
                            // A constant key column is one value: in or out.
                            _ => {
                                let k = batch.o_key.get_or(0, 0);
                                if k >= lo && k <= hi {
                                    (0, batch.row_count)
                                } else {
                                    (0, 0)
                                }
                            }
                        };
                        total += end - start;
                        Some((ot, start, end))
                    }
                    None => {
                        total += batch.row_count;
                        None
                    }
                };
                if total > cap {
                    charge_rows(ctx, total)?;
                    return Ok(WalkOutcome::Capped);
                }
                leaflets.push(WalkedLeaflet {
                    batch,
                    keyed,
                    p_const: entry.p_const,
                    o_type_const: entry.o_type_const,
                });
            }
        }

        charge_rows(ctx, total)?;

        // Pass 2: collect.
        let mut values = ValueIndex::with_capacity(total);
        let mut rows = 0usize;
        for leaflet in &leaflets {
            let batch = &leaflet.batch;
            match leaflet.keyed {
                Some((ot, start, end)) => {
                    let kind = OType::from_u16(ot).decode_kind();
                    for row in start..end {
                        let o_key = batch.o_key.get(row);
                        let value = match kind {
                            DecodeKind::I64 => WalkValue::Int(ObjKey::from_u64(o_key).decode_i64()),
                            DecodeKind::F64 => {
                                WalkValue::Float(ObjKey::from_u64(o_key).decode_f64())
                            }
                            _ => WalkValue::Other(Box::new(decoded_binding(
                                store, g_id, p_id, ot, o_key,
                            )?)),
                        };
                        values.push(GroupKeyOwned::Sid(batch.s_id.get(row)), value);
                        rows += 1;
                    }
                }
                None => {
                    for row in 0..batch.row_count {
                        if leaflet.p_const.is_none() && batch.p_id.get_or(row, 0) != p_id {
                            continue;
                        }
                        let ot = leaflet
                            .o_type_const
                            .unwrap_or_else(|| batch.o_type.get_or(row, 0));
                        if !walkable_numeric(OType::from_u16(ot)) {
                            continue;
                        }
                        let o_key = batch.o_key.get(row);
                        let val = store.decode_value_v3(ot, o_key, p_id, g_id).map_err(|e| {
                            QueryError::Internal(format!("decode_value_v3 (range walk): {e}"))
                        })?;
                        if !bounds.matches(&val) {
                            continue;
                        }
                        let value = match val {
                            FlakeValue::Long(n) => WalkValue::Int(n),
                            FlakeValue::Double(d) => WalkValue::Float(d),
                            other => WalkValue::Other(Box::new(materialized_object_binding(
                                store, ot, p_id, other, None, None,
                            ))),
                        };
                        values.push(GroupKeyOwned::Sid(batch.s_id.get(row)), value);
                        rows += 1;
                    }
                }
            }
        }
        tracing::debug!(
            predicate = ?cond.predicate,
            lower = ?envelope.lower,
            upper = ?envelope.upper,
            rows,
            subjects = values.head.len(),
            "range semijoin index built by POST walk"
        );
        Ok(WalkOutcome::Built(RangeIndex {
            envelope: envelope.clone(),
            values,
            rows,
        }))
    }

    /// Answer one batch's rows for condition `c` by batched subject probes —
    /// what the nested loop would have done — when no walked index can.
    async fn probe_batch(
        &mut self,
        c: usize,
        batch: &Batch,
        keep: &mut [bool],
        intervals: &[Option<u32>],
        distinct: &[RowInterval],
        ctx: &ExecutionContext<'_>,
    ) -> Result<()> {
        self.probe_batches += 1;
        let store = ctx.binary_store.clone();
        let pred_sid = store
            .as_ref()
            .and_then(|s| try_normalize_pred_sid(s, &self.conditions[c].predicate));
        let lane = match (&store, &pred_sid) {
            (Some(store), Some(pred_sid)) if !ctx.is_multi_ledger() => {
                subject_probe_lane_plan(ctx, store, pred_sid)?
            }
            _ => ProbeLanePlan::Decline,
        };
        let mut probed: Option<ValueIndex> = None;
        if let (Some(store), Some(pred_sid), false) =
            (&store, &pred_sid, matches!(lane, ProbeLanePlan::Decline))
        {
            let mut subject_ids: Vec<u64> = Vec::new();
            for (row, keep_row) in keep.iter().enumerate() {
                if !*keep_row || intervals[row].is_none() {
                    continue;
                }
                if let Some(Binding::EncodedSid { s_id, .. }) = batch.get(row, self.subject_var) {
                    subject_ids.push(*s_id);
                }
            }
            subject_ids.sort_unstable();
            subject_ids.dedup();
            let dict_overlay = make_dict_overlay(ctx, store);
            let mut probe_ops = match &lane {
                ProbeLanePlan::Merge(ops) => ProbeOps::new(ops.clone()),
                _ => None,
            };
            let matches = batched_subject_probe_binary(
                ctx,
                store,
                &SubjectProbeParams {
                    pred_sid,
                    subject_ids: &subject_ids,
                    object_bounds: None,
                    bound_object: None,
                    emit_object: true,
                    dict_overlay: dict_overlay.as_ref(),
                },
                probe_ops.as_mut(),
            )?;
            let mut values = ValueIndex::with_capacity(matches.len());
            for m in &matches {
                if let Some(object) = &m.object {
                    values.push(
                        GroupKeyOwned::Sid(m.subject_id),
                        WalkValue::from_binding(object),
                    );
                }
            }
            probed = Some(values);
        }

        let mut fallback_rows = Vec::new();
        for (row, keep_row) in keep.iter_mut().enumerate() {
            if !*keep_row {
                continue;
            }
            let Some(interval) = intervals[row].map(|i| &distinct[i as usize]) else {
                *keep_row = false;
                continue;
            };
            let passes = match (batch.get(row, self.subject_var), &probed) {
                (Some(Binding::EncodedSid { s_id, .. }), Some(values)) => {
                    let mut any = false;
                    for value in values.values_of(&GroupKeyOwned::Sid(*s_id)) {
                        if self.value_passes(c, interval, batch, row, value, ctx)? {
                            any = true;
                            break;
                        }
                    }
                    any
                }
                (Some(Binding::Poisoned), _) => false,
                _ => {
                    fallback_rows.push(row);
                    continue;
                }
            };
            if !passes {
                *keep_row = false;
            }
        }
        self.filter_batch_exactly(c, batch, &fallback_rows, keep, ctx)
            .await?;
        Ok(())
    }

    /// Plan the generic probe/filter once for all fallback rows. A private
    /// row id survives the join: subject identity alone is insufficient when
    /// two rows have different bounds, or an initially unbound subject binds.
    async fn filter_batch_exactly(
        &mut self,
        c: usize,
        batch: &Batch,
        rows: &[usize],
        keep: &mut [bool],
        ctx: &ExecutionContext<'_>,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        self.fallback_rows += rows.len();
        self.fallback_batches += 1;
        let cond = &self.conditions[c];
        let mut used: std::collections::HashSet<VarId> = batch.schema().iter().copied().collect();
        used.extend(cond.filter.referenced_vars());
        used.insert(self.subject_var);
        used.insert(cond.value_var);
        let row_var = (0..=u16::MAX)
            .rev()
            .map(VarId)
            .find(|v| !used.contains(v))
            .ok_or_else(|| {
                QueryError::Internal("no variable available for semi-join row id".into())
            })?;
        // Only the subject and filter operands are needed in the inner plan.
        let mut schema: Vec<VarId> = batch
            .schema()
            .iter()
            .copied()
            .filter(|v| *v == self.subject_var || self.bound_vars[c].contains(v))
            .collect();
        let mut columns: Vec<Vec<Binding>> = schema
            .iter()
            .map(|v| {
                rows.iter()
                    .map(|r| batch.get(*r, *v).cloned().unwrap_or(Binding::Unbound))
                    .collect()
            })
            .collect();
        schema.push(row_var);
        let row_datatype = fluree_db_core::Sid::new(fluree_vocab::namespaces::XSD, "long");
        columns.push(
            rows.iter()
                .map(|r| Binding::lit(FlakeValue::Long(*r as i64), row_datatype.clone()))
                .collect(),
        );
        let seed = BatchSeedOperator::from_batch(Batch::new(schema.into(), columns)?);
        let patterns = [
            Pattern::Triple(cond.probe(self.subject_var)),
            Pattern::Filter(cond.filter.clone()),
        ];
        let mut inner = build_where_operators_seeded(
            Some(Box::new(seed)),
            &patterns,
            None,
            Some(&[row_var]),
            &self.planning,
        )?;
        for row in rows {
            keep[*row] = false;
        }
        // Close the subtree on errors as well as on exhaustion.
        let result = async {
            inner.open(ctx).await?;
            let mut remaining = rows.len();
            while let Some(out) = inner.next_batch(ctx).await? {
                for r in 0..out.len() {
                    let Some(Binding::Lit {
                        val: FlakeValue::Long(row),
                        ..
                    }) = out.get(r, row_var)
                    else {
                        return Err(QueryError::Internal(
                            "semi-join fallback lost row id".into(),
                        ));
                    };
                    if !keep[*row as usize] {
                        keep[*row as usize] = true;
                        remaining -= 1;
                    }
                }
                // A semi-join needs only one witness per driving row.
                if remaining == 0 {
                    break;
                }
            }
            Ok(())
        }
        .await;
        inner.close();
        result
    }
}

/// One charge per batch of walked or driving rows, at the same rate as the
/// nested loop's probes (`charge_probe_rows`), never inside the row loops.
fn charge_rows(ctx: &ExecutionContext<'_>, rows: usize) -> Result<()> {
    ctx.tracker
        .consume_fuel(rows as u64 * fluree_db_core::tracking::schedule::PER_ROW_MICRO_FUEL)?;
    Ok(())
}

/// A walked row's value when it is not an inline numeric: decoded through the
/// store, as the index would deliver it to a scan.
fn decoded_binding(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
    o_type: u16,
    o_key: u64,
) -> Result<Binding> {
    let val = store
        .decode_value_v3(o_type, o_key, p_id, g_id)
        .map_err(|e| QueryError::Internal(format!("decode_value_v3 (range walk): {e}")))?;
    Ok(materialized_object_binding(
        store, o_type, p_id, val, None, None,
    ))
}

#[async_trait]
impl Operator for RangeSemiJoinOperator {
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        vec![crate::plan_node::PlanChild::child(self.input())]
    }

    fn plan_details(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut m = serde_json::Map::new();
        m.insert("subject".into(), format!("?v{}", self.subject_var.0).into());
        let conditions: Vec<serde_json::Value> = self
            .conditions
            .iter()
            .map(|c| {
                let side = |b: &Option<(Expression, bool)>, op: &str| {
                    b.as_ref()
                        .map(|(e, inclusive)| {
                            format!("{}{} {:?}", op, if *inclusive { "=" } else { "" }, e)
                        })
                        .unwrap_or_default()
                };
                let predicate = match &c.predicate {
                    Ref::Sid(sid) => format!("<{}:{}>", sid.namespace_code, sid.name),
                    Ref::Iri(iri) => format!("<{iri}>"),
                    Ref::Var(v) => format!("?v{}", v.0),
                };
                format!(
                    "?v{} {} ?v{} [{} {}]",
                    self.subject_var.0,
                    predicate,
                    c.value_var.0,
                    side(&c.lower, ">"),
                    side(&c.upper, "<"),
                )
                .into()
            })
            .collect();
        m.insert("conditions".into(), conditions.into());
        m
    }

    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if self.norm.is_none() {
            self.norm = equality_norm(ctx);
        }
        self.restore_child();
        if ctx.binary_store.is_none()
            || !root_or_no_policy(ctx)
            || ctx.is_multi_ledger()
            || ctx.eager_materialization
        {
            // The generic plan dedups dead feature variables before probing.
            // Retain that advantage when probe lanes decline. The fold's
            // where_dedup_safe license permits this; all child bindings,
            // including each row's range operands, remain in the dedup key.
            self.fallback_dedup = Some(DistinctOperator::new(
                self.child.take().expect("semi-join child"),
            ));
        }
        self.input_mut().open(ctx).await?;
        for index in &mut self.indexes {
            *index = None;
        }
        for off in &mut self.walk_off {
            *off = false;
        }
        self.walk_attempts.fill(0);
        self.probed_rows = 0;
        self.kept_rows = 0;
        self.fallback_rows = 0;
        self.fallback_batches = 0;
        self.probe_batches = 0;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }
        loop {
            let batch = match self.input_mut().next_batch(ctx).await? {
                Some(b) if !b.is_empty() => b,
                Some(_) => continue,
                None => {
                    tracing::debug!(
                        probed = self.probed_rows,
                        kept = self.kept_rows,
                        fallback = self.fallback_rows,
                        probe_batches = self.probe_batches,
                        fallback_batches = self.fallback_batches,
                        walk_attempts = ?self.walk_attempts,
                        walked = self
                            .indexes
                            .iter()
                            .map(|i| i.as_ref().map_or(0, |i| i.rows))
                            .sum::<usize>(),
                        "range semijoin exhausted"
                    );
                    self.state = OperatorState::Exhausted;
                    return Ok(None);
                }
            };
            ctx.check_cancelled()?;
            charge_rows(ctx, batch.len())?;
            self.probed_rows += batch.len();

            let mut keep = vec![true; batch.len()];
            for c in 0..self.conditions.len() {
                // Per-row intervals and the batch envelope they union to.
                let mut intervals: Vec<Option<u32>> = Vec::with_capacity(batch.len());
                let mut distinct: Vec<RowInterval> = Vec::new();
                let mut batch_envelope: Option<Envelope> = None;
                let mut all_numeric = true;
                let mut memo = None;
                for (row, keep_row) in keep.iter_mut().enumerate() {
                    if !*keep_row {
                        intervals.push(None);
                        continue;
                    }
                    let interval = self.row_interval(c, &batch, row, ctx, &mut memo, &mut distinct);
                    match interval {
                        None => *keep_row = false,
                        Some(idx) => {
                            let RowInterval {
                                numeric, envelope, ..
                            } = &distinct[idx as usize];
                            all_numeric &= *numeric;
                            batch_envelope = Some(match batch_envelope {
                                None => envelope.clone(),
                                Some(current) => current.union(envelope),
                            });
                        }
                    }
                    intervals.push(interval);
                }
                let Some(batch_envelope) = batch_envelope else {
                    continue;
                };

                // A non-numeric bound has no key range and its rows are not
                // in a walked index: this batch goes to probes, the index
                // stays for later numeric batches.
                let mut use_index = false;
                if all_numeric {
                    let covered = self.indexes[c]
                        .as_ref()
                        .is_some_and(|index| index.envelope.covers(&batch_envelope));
                    if !covered && !self.walk_off[c] {
                        let target = match &self.indexes[c] {
                            Some(index) => index.envelope.union(&batch_envelope),
                            None => batch_envelope.clone(),
                        };
                        let outcome = if self.walk_attempts[c] < MAX_WALKS_PER_CONDITION {
                            self.walk_attempts[c] += 1;
                            self.build_index_walk(c, &target, ctx)?
                        } else {
                            WalkOutcome::Capped
                        };
                        match outcome {
                            WalkOutcome::Built(index) => self.indexes[c] = Some(index),
                            WalkOutcome::Capped | WalkOutcome::Declined => {
                                self.walk_off[c] = true;
                                self.indexes[c] = None;
                            }
                        }
                    }
                    use_index = self.indexes[c].is_some();
                }

                let Some(index) = self.indexes[c].as_ref().filter(|_| use_index) else {
                    crate::fast_path_outcome::stamp_fast_path(
                        RANGE_SEMIJOIN_SITE,
                        crate::fast_path_outcome::FastPathOutcome::Fallback(
                            crate::fast_path_outcome::FastPathFallback::GateDeclined,
                        ),
                    );
                    self.probe_batch(c, &batch, &mut keep, &intervals, &distinct, ctx)
                        .await?;
                    continue;
                };
                crate::fast_path_outcome::stamp_fast_path(
                    RANGE_SEMIJOIN_SITE,
                    crate::fast_path_outcome::FastPathOutcome::Proceed,
                );

                let (store, gv) = EqualityNorm::parts(&self.norm);
                let mut fallback_rows: Vec<usize> = Vec::new();
                for (row, keep_row) in keep.iter_mut().enumerate() {
                    if !*keep_row {
                        continue;
                    }
                    let Some(interval) = intervals[row].map(|i| &distinct[i as usize]) else {
                        *keep_row = false;
                        continue;
                    };
                    let passes = match batch.get(row, self.subject_var) {
                        None | Some(Binding::Unbound) => {
                            fallback_rows.push(row);
                            continue;
                        }
                        Some(Binding::Poisoned) => false,
                        Some(subject) => {
                            let key = binding_to_group_key_normalized(subject, store, gv);
                            let mut any = false;
                            for value in index.values.values_of(&key) {
                                if self.value_passes(c, interval, &batch, row, value, ctx)? {
                                    any = true;
                                    break;
                                }
                            }
                            any
                        }
                    };
                    if !passes {
                        *keep_row = false;
                    }
                }
                self.filter_batch_exactly(c, &batch, &fallback_rows, &mut keep, ctx)
                    .await?;
            }

            let kept = keep.iter().filter(|k| **k).count();
            if kept == 0 {
                continue;
            }
            self.kept_rows += kept;
            // Column positions from the batch's own schema, not the child's
            // declared order (see `MembershipJoinOperator::all_keys_bound`).
            let source_cols: Vec<Option<usize>> = self
                .schema
                .iter()
                .map(|v| batch.schema().iter().position(|x| x == v))
                .collect();
            let mut columns: Vec<Vec<Binding>> = (0..self.schema.len())
                .map(|_| Vec::with_capacity(kept))
                .collect();
            for (row, keep_row) in keep.iter().enumerate() {
                if !keep_row {
                    continue;
                }
                for (out, source) in columns.iter_mut().zip(&source_cols) {
                    out.push(match source {
                        Some(col) => batch.get_by_col(row, *col).clone(),
                        None => Binding::Unbound,
                    });
                }
            }
            return Ok(Some(Batch::new(self.schema.clone(), columns)?));
        }
    }

    fn close(&mut self) {
        self.input_mut().close();
        self.restore_child();
        for index in &mut self.indexes {
            *index = None;
        }
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.input().estimated_rows()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn integer_walk_bounds_round_outward_above_f64_precision() {
        for (lower, upper, inside) in [
            (
                "18014398509481986.1",
                "18014398509481987.9",
                18_014_398_509_481_987,
            ),
            (
                "-18014398509481987.9",
                "-18014398509481986.1",
                -18_014_398_509_481_987,
            ),
        ] {
            let decimal =
                |s| FlakeValue::Decimal(Box::new(bigdecimal::BigDecimal::from_str(s).unwrap()));
            let envelope = Envelope {
                lower: Some(decimal(lower)),
                upper: Some(decimal(upper)),
            };
            let (lo, hi) = key_bounds(&envelope, DecodeKind::I64).unwrap();
            let key = ObjKey::encode_i64(inside).as_u64();
            assert!(
                lo <= key && key <= hi,
                "rounded walk bounds must include {inside}"
            );
        }
    }
}
