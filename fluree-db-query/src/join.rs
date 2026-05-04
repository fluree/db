//! Join operators for multi-pattern queries
//!
//! This module provides `NestedLoopJoinOperator` which implements nested-loop join
//! where left results drive right scans. It enforces var unification - shared
//! vars between left and right must match exactly.

use crate::binary_scan::EmitMask;
use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::dataset::ActiveGraphs;
use crate::error::{QueryError, Result};
use crate::ir::triple::{Ref, Term, TriplePattern};
use crate::object_binding::{late_materialized_object_binding, materialized_object_binding};
use crate::operator::inline::{apply_inline, extend_schema, InlineOperator};
use crate::operator::{
    compute_trimmed_vars, effective_schema, trim_batch, BoxedOperator, Operator, OperatorState,
};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::{BinaryGraphView, BinaryIndexStore};
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::{GraphId, IndexType, ObjectBounds, Sid, BATCHED_JOIN_SIZE};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;
use tracing::Instrument;

/// Emit a debug flush summary only when the batched existence probe did non-trivial
/// work; the per-join success path is otherwise far too noisy for perf captures.
const BATCHED_EXISTS_DEBUG_MIN_ACCUM: usize = 8;
const BATCHED_EXISTS_DEBUG_MIN_MS: u64 = 10;

/// Binary search for the first row in `batch.s_id[start..end]` where `s_id >= target`.
#[inline]
fn lower_bound_s_id(
    batch: &fluree_db_binary_index::ColumnBatch,
    start: usize,
    end: usize,
    target: u64,
) -> usize {
    let mut lo = start;
    let mut hi = end;
    while lo < hi {
        let mid = usize::midpoint(lo, hi);
        if batch.s_id.get(mid) < target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Binary search for the first row in `batch.s_id[start..end]` where `s_id > target`.
#[inline]
fn upper_bound_s_id(
    batch: &fluree_db_binary_index::ColumnBatch,
    start: usize,
    end: usize,
    target: u64,
) -> usize {
    let mut lo = start;
    let mut hi = end;
    while lo < hi {
        let mid = usize::midpoint(lo, hi);
        if batch.s_id.get(mid) <= target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

#[derive(Clone, Debug)]
pub(crate) struct BatchedSubjectProbeMatch {
    pub subject_id: u64,
    pub object: Option<Binding>,
}

#[derive(Clone, Debug)]
pub(crate) struct BatchedSpotStarMatch {
    pub subject_id: u64,
    pub predicate_idx: usize,
    pub object: Option<Binding>,
}

pub(crate) struct SpotStarPredicateParams<'a> {
    pub predicate_idx: usize,
    pub pred_sid: Sid,
    pub object_bounds: Option<&'a ObjectBounds>,
    pub bound_object: Option<&'a Term>,
    pub emit_object: bool,
}

pub(crate) fn make_dict_overlay(
    ctx: &ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
) -> Option<crate::dict_overlay::DictOverlay> {
    ctx.dict_novelty.as_ref().map(|dn| {
        crate::dict_overlay::DictOverlay::new(
            BinaryGraphView::with_novelty(Arc::clone(store), ctx.binary_g_id, Some(Arc::clone(dn)))
                .with_namespace_codes_fallback(ctx.namespace_codes_fallback.clone()),
            Arc::clone(dn),
        )
    })
}

/// Create a right-side scan operator for a join.
///
/// Wraps the scan in a [`DatasetOperator`](crate::dataset_operator::DatasetOperator)
/// so that multi-graph fanout is handled transparently.
#[allow(clippy::too_many_arguments)]
fn make_right_scan(
    pattern: TriplePattern,
    object_bounds: Option<ObjectBounds>,
    emit: EmitMask,
    inline_ops: Vec<InlineOperator>,
    index_hint: Option<IndexType>,
    _ctx: &ExecutionContext<'_>,
    mode: crate::temporal_mode::TemporalMode,
) -> Box<dyn Operator> {
    Box::new(crate::dataset_operator::DatasetOperator::scan(
        pattern,
        object_bounds,
        inline_ops,
        emit,
        index_hint,
        mode,
    ))
}

/// Position in a triple pattern (subject, predicate, object)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternPosition {
    Subject,
    Predicate,
    Object,
}

/// Binding instruction for right pattern
///
/// When executing the right pattern, substitute the value from left_col
/// into the specified position.
#[derive(Debug, Clone)]
pub struct BindInstruction {
    /// Position in right pattern to bind
    pub position: PatternPosition,
    /// Column index in left batch to get value from
    pub left_col: usize,
}

/// Unification check for shared variables
///
/// After right scan returns results, verify that the value at right_col
/// matches the value at left_col for shared variables.
#[derive(Debug, Clone)]
pub struct UnifyInstruction {
    /// Column index in left batch
    pub left_col: usize,
    /// Column index in right batch
    pub right_col: usize,
}

/// Identifies the source of a left row in `pending_output`.
#[derive(Debug, Clone)]
enum BatchRef {
    /// Left row is in `self.stored_left_batches[idx]`
    Stored(usize),
}

/// Check if the right pattern is eligible for batched subject join.
///
/// Eligible patterns have shape: `(?s, fixed_p, ?o)` where subject is bound
/// from the left, predicate is fixed (Ref::Sid), object is a new unbound
/// variable, with no datatype/language constraints.
fn is_batched_eligible(
    bind_instructions: &[BindInstruction],
    right_pattern: &TriplePattern,
) -> bool {
    // Subject must have a BindInstruction (bound from left)
    let has_subject_bind = bind_instructions
        .iter()
        .any(|b| b.position == PatternPosition::Subject);
    // Predicate must be fixed (Ref::Sid)
    let pred_fixed = right_pattern.p.is_sid();
    // Object must be a variable
    let obj_is_var = matches!(&right_pattern.o, Term::Var(_));
    // No BindInstruction for Object (object is a new variable, not shared)
    let no_obj_bind = !bind_instructions
        .iter()
        .any(|b| b.position == PatternPosition::Object);
    // No datatype or language constraints
    let no_constraint = right_pattern.dtc.is_none();

    has_subject_bind && pred_fixed && obj_is_var && no_obj_bind && no_constraint
}

/// Check if the right pattern is eligible for batched object join.
///
/// Eligible patterns have shape: `(?s_new, fixed_p, ?o_shared)` where:
/// - object is bound from the left (BindInstruction for Object)
/// - predicate is fixed (Ref::Sid)
/// - subject is a new unbound variable (no BindInstruction for Subject)
/// - no object bounds or datatype/language constraints
///
/// This enables scanning OPST in bulk for a set of bound ref objects rather than
/// opening one scan per left row.
fn is_batched_object_eligible(
    bind_instructions: &[BindInstruction],
    right_pattern: &TriplePattern,
    has_object_bounds: bool,
) -> bool {
    // Object must have a BindInstruction (bound from left)
    let has_object_bind = bind_instructions
        .iter()
        .any(|b| b.position == PatternPosition::Object);
    // Subject must NOT be bound from left (new variable)
    let no_subject_bind = !bind_instructions
        .iter()
        .any(|b| b.position == PatternPosition::Subject);
    // Predicate must be fixed (Ref::Sid)
    let pred_fixed = right_pattern.p.is_sid();
    // Subject must be a variable
    let subj_is_var = matches!(&right_pattern.s, Ref::Var(_));
    // Object must be a variable (shared var)
    let obj_is_var = matches!(&right_pattern.o, Term::Var(_));
    // No object bounds (no FILTER range pushdown)
    let no_bounds = !has_object_bounds;
    // No datatype or language constraints
    let no_constraint = right_pattern.dtc.is_none();

    has_object_bind
        && no_subject_bind
        && pred_fixed
        && subj_is_var
        && obj_is_var
        && no_bounds
        && no_constraint
}

/// Check if the right pattern is eligible for a batched subject existence join.
///
/// Eligible patterns have shape: `(?s, fixed_p, fixed_o)` where subject is bound
/// from the left, predicate is fixed, object is fixed, and the join emits no new
/// right-side variables. This lets us probe PSOT once for a batch of subjects
/// instead of opening one scan per left row for existence checks such as
/// `?person rdf:type :Person`.
fn is_batched_subject_exists_eligible(
    bind_instructions: &[BindInstruction],
    right_pattern: &TriplePattern,
) -> bool {
    let has_subject_bind = bind_instructions
        .iter()
        .any(|b| b.position == PatternPosition::Subject);
    let pred_fixed = right_pattern.p.is_sid();
    let obj_fixed = !matches!(&right_pattern.o, Term::Var(_));
    let no_obj_bind = !bind_instructions
        .iter()
        .any(|b| b.position == PatternPosition::Object);
    let no_constraint = right_pattern.dtc.is_none();

    has_subject_bind && pred_fixed && obj_fixed && no_obj_bind && no_constraint
}
/// Bind-join operator for nested-loop join
///
/// For each row from the left operator, substitutes bound variables into
/// the right pattern, executes a scan, and combines results.
///
/// # Var Unification
///
/// When a variable appears in both left schema and right pattern output,
/// the join enforces that both sides produce the same value. Rows with
/// conflicting bindings are dropped.
///
/// # Invariant
///
/// When a shared variable is `Unbound` on the left (e.g., from VALUES with UNDEF),
/// `combine_rows` falls back to the right-side value so the concrete binding
/// propagates through the join.
pub struct NestedLoopJoinOperator {
    /// Left (driving) operator
    left: Box<dyn Operator>,
    /// Right pattern template (will be instantiated per left row)
    right_pattern: TriplePattern,
    /// Schema from left operator
    left_schema: Arc<[VarId]>,
    /// Which right-pattern variables to emit into the right scan schema.
    ///
    /// This enables pruning of unused right-side variables so the scan can avoid
    /// decoding and materializing values that do not affect the query result.
    right_emit: EmitMask,
    /// New variables introduced by right pattern (not in left schema)
    right_new_vars: Vec<VarId>,
    /// Combined output schema: left vars + new right vars
    combined_schema: Arc<[VarId]>,
    /// Instructions for binding left values into right pattern
    bind_instructions: Vec<BindInstruction>,
    /// Instructions for unification checks on shared vars
    unify_instructions: Vec<UnifyInstruction>,
    /// Current state
    state: OperatorState,
    /// Current left batch being processed
    current_left_batch: Option<Batch>,
    /// Current row index in left batch
    current_left_row: usize,
    /// Pending output rows (batch_ref, left_row_idx, right_batch)
    pending_output: VecDeque<(BatchRef, usize, Batch)>,
    /// Current row index within the front right_batch (for partial processing across calls)
    pending_right_row: usize,
    /// Active right-side scan for the current left row.
    ///
    /// Keeping the scan open lets the join yield after each right batch instead of
    /// draining an entire left batch up front, which makes top-level LIMITs much
    /// more responsive on wide joins like `?s rdf:type <Class> ; ?p ?o`.
    active_right_scan: Option<BoxedOperator>,
    /// Left-row provenance for `active_right_scan`.
    active_right_batch_ref: Option<BatchRef>,
    /// Left-row index for `active_right_scan`.
    active_right_left_row: usize,
    /// Optional object bounds for range filter pushdown
    object_bounds: Option<ObjectBounds>,
    /// Whether this join is eligible for batched subject join
    batched_eligible: bool,
    /// Whether this join is eligible for batched object join
    batched_object_eligible: bool,
    /// Whether this join is eligible for batched subject existence join
    batched_exists_eligible: bool,
    /// Column index in left batch for the subject binding (batched mode)
    subject_left_col: Option<usize>,
    /// Column index in left batch for the object binding (batched object mode)
    object_left_col: Option<usize>,
    /// The fixed predicate SID (batched mode)
    batched_predicate: Option<Sid>,
    /// Accumulated entries for batched processing: (stored_batch_idx, row_idx, subject_s_id)
    /// Stores the raw s_id directly to avoid dictionary round-trips with EncodedSid.
    batched_accumulator: Vec<(usize, usize, u64)>,
    /// Left batches retained for the batched flush
    stored_left_batches: Vec<Batch>,
    /// Pre-built output batches from the batched path, ready to emit
    batched_output: VecDeque<Batch>,
    /// Cached index into `stored_left_batches` for the currently active left batch.
    ///
    /// This prevents storing/cloning the same `current_left_batch` repeatedly.
    current_left_batch_stored_idx: Option<usize>,
    /// Inline operators evaluated on combined rows during the join.
    ///
    /// These have all required variables bound by the combined (left + right)
    /// schema, so they can be evaluated immediately on each combined row rather than
    /// requiring separate Operator wrappers.
    inline_ops: Vec<InlineOperator>,
    /// Inline filters that depend only on right-side output vars and can run
    /// inside the right scan before row combination.
    right_scan_inline_ops: Vec<InlineOperator>,
    /// Preferred right-side scan order when planning can infer a better index.
    right_index_hint: Option<IndexType>,
    /// Variables required by downstream operators; if set, output is trimmed.
    out_schema: Option<Arc<[VarId]>>,
    /// Emit the runtime path decision once per open().
    logged_runtime_mode: bool,
    /// Temporal mode captured at planner-time for the late per-row right scan.
    mode: crate::temporal_mode::TemporalMode,
}

impl NestedLoopJoinOperator {
    fn partition_right_scan_inline_ops(
        left_schema: &[VarId],
        right_output_vars: &[VarId],
        inline_ops: Vec<InlineOperator>,
    ) -> (Vec<InlineOperator>, Vec<InlineOperator>) {
        let left_vars: HashSet<VarId> = left_schema.iter().copied().collect();
        let right_only_vars: HashSet<VarId> = right_output_vars
            .iter()
            .copied()
            .filter(|v| !left_vars.contains(v))
            .collect();
        let mut right_scan_inline_ops = Vec::new();
        let mut post_join_inline_ops = Vec::new();

        for op in inline_ops {
            match &op {
                InlineOperator::Filter(expr)
                    if expr
                        .referenced_vars()
                        .into_iter()
                        .all(|v| right_only_vars.contains(&v)) =>
                {
                    right_scan_inline_ops.push(op);
                }
                _ => post_join_inline_ops.push(op),
            }
        }

        (right_scan_inline_ops, post_join_inline_ops)
    }

    fn apply_right_scan_inline_ops(
        &self,
        ctx: &ExecutionContext<'_>,
        right_bindings: &mut Vec<Binding>,
    ) -> Result<bool> {
        if self.right_scan_inline_ops.is_empty() {
            return Ok(true);
        }
        apply_inline(
            &self.right_scan_inline_ops,
            &self.right_new_vars,
            right_bindings,
            Some(ctx),
        )
    }

    /// Create a new bind-join operator
    ///
    /// # Arguments
    ///
    /// * `left` - The left (driving) operator
    /// * `left_schema` - Schema of the left operator
    /// * `right_pattern` - Pattern to execute for each left row
    /// * `object_bounds` - Optional range bounds for object variable (filter pushdown)
    /// * `inline_ops` - Inline operators evaluated on combined rows
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        left: Box<dyn Operator>,
        left_schema: Arc<[VarId]>,
        right_pattern: TriplePattern,
        object_bounds: Option<ObjectBounds>,
        inline_ops: Vec<InlineOperator>,
        right_emit: EmitMask,
        mode: crate::temporal_mode::TemporalMode,
    ) -> Self {
        // Build bind instructions: which left columns bind which right pattern positions
        let mut bind_instructions = Vec::new();
        let left_var_positions: std::collections::HashMap<VarId, usize> = left_schema
            .iter()
            .enumerate()
            .map(|(i, v)| (*v, i))
            .collect();

        // Check subject
        if let Ref::Var(v) = &right_pattern.s {
            if let Some(&col) = left_var_positions.get(v) {
                bind_instructions.push(BindInstruction {
                    position: PatternPosition::Subject,
                    left_col: col,
                });
            }
        }

        // Check predicate
        if let Ref::Var(v) = &right_pattern.p {
            if let Some(&col) = left_var_positions.get(v) {
                bind_instructions.push(BindInstruction {
                    position: PatternPosition::Predicate,
                    left_col: col,
                });
            }
        }

        // Check object
        if let Term::Var(v) = &right_pattern.o {
            if let Some(&col) = left_var_positions.get(v) {
                bind_instructions.push(BindInstruction {
                    position: PatternPosition::Object,
                    left_col: col,
                });
            }
        }

        // Determine right pattern output vars (vars that are still unbound after substitution),
        // filtered by the emission mask. This allows plan-time pruning of unused vars.
        let mut right_output_vars: Vec<VarId> = Vec::new();
        if right_emit.s {
            if let Ref::Var(v) = &right_pattern.s {
                if !left_var_positions.contains_key(v) {
                    right_output_vars.push(*v);
                }
            }
        }
        if right_emit.p {
            if let Ref::Var(v) = &right_pattern.p {
                if !left_var_positions.contains_key(v) {
                    right_output_vars.push(*v);
                }
            }
        }
        if right_emit.o {
            if let Term::Var(v) = &right_pattern.o {
                if !left_var_positions.contains_key(v) {
                    right_output_vars.push(*v);
                }
            }
        }

        let (right_scan_inline_ops, inline_ops) =
            Self::partition_right_scan_inline_ops(&left_schema, &right_output_vars, inline_ops);

        // Build combined schema: left schema + new right vars + inline bind vars
        let mut combined = left_schema.to_vec();
        combined.extend(right_output_vars.iter().copied());
        let combined_schema: Arc<[VarId]> = extend_schema(&combined, &inline_ops).into();
        let right_index_hint = crate::binary_scan::preferred_index_hint_for_prefix_filters(
            &right_pattern,
            &right_scan_inline_ops,
        );

        // Build unify instructions for shared vars
        //
        // Variables that are in both left schema and right pattern need unification
        // UNLESS they are fully substituted into the pattern. However, substitution
        // depends on runtime binding types:
        // - Subject/Predicate: Only Sid bindings are substituted
        // - Object: Sid and Lit bindings are substituted
        //
        // We compute right_col based on right_output_vars (which excludes vars
        // expected to be substituted). At runtime, if substitution doesn't happen
        // (e.g., Lit at Subject position), the var remains in the right scan output
        // but at a different position than expected - this is handled by skipping
        // unification for such edge cases.
        //
        // Collect vars that WILL be substituted (based on position)
        // For Object position, we assume substitution will happen (Sid/Lit)
        // For Subject/Predicate, substitution requires Sid which we can't verify
        // at construction time, so we don't create unify instructions for these
        // positions when the var is in left schema (they have bind_instructions)
        let bound_vars: std::collections::HashSet<VarId> = bind_instructions
            .iter()
            .filter_map(|instr| match instr.position {
                PatternPosition::Subject => right_pattern.s.as_var(),
                PatternPosition::Predicate => right_pattern.p.as_var(),
                PatternPosition::Object => right_pattern.o.as_var(),
            })
            .collect();

        let mut unify_instructions = Vec::new();
        for var in right_pattern.produced_vars() {
            // Skip vars that have bind_instructions - they will be substituted
            // (or if not substituted due to binding type, the row is handled
            // by the scan returning no results or the substitution leaving the var)
            if bound_vars.contains(&var) {
                continue;
            }

            if let Some(&left_col) = left_var_positions.get(&var) {
                // This var is shared but NOT bound - find its position in right output
                // Right output schema only includes non-bound vars
                if let Some(right_idx) = right_output_vars.iter().position(|v| *v == var) {
                    unify_instructions.push(UnifyInstruction {
                        left_col,
                        right_col: right_idx,
                    });
                }
            }
        }

        let has_bounds = object_bounds.is_some();

        let batched_eligible = is_batched_eligible(&bind_instructions, &right_pattern);
        let batched_object_eligible = !batched_eligible
            && is_batched_object_eligible(&bind_instructions, &right_pattern, has_bounds);
        let batched_exists_eligible = !batched_eligible
            && !batched_object_eligible
            && is_batched_subject_exists_eligible(&bind_instructions, &right_pattern);
        let subject_left_col = if batched_eligible || batched_exists_eligible {
            bind_instructions
                .iter()
                .find(|b| b.position == PatternPosition::Subject)
                .map(|b| b.left_col)
        } else {
            None
        };
        let object_left_col = if batched_object_eligible {
            bind_instructions
                .iter()
                .find(|b| b.position == PatternPosition::Object)
                .map(|b| b.left_col)
        } else {
            None
        };
        let batched_predicate =
            if batched_eligible || batched_object_eligible || batched_exists_eligible {
                match &right_pattern.p {
                    Ref::Sid(sid) => Some(sid.clone()),
                    _ => None,
                }
            } else {
                None
            };

        Self {
            left,
            right_pattern,
            left_schema,
            right_emit,
            right_new_vars: right_output_vars,
            combined_schema,
            bind_instructions,
            unify_instructions,
            state: OperatorState::Created,
            current_left_batch: None,
            current_left_row: 0,
            pending_output: VecDeque::new(),
            pending_right_row: 0,
            active_right_scan: None,
            active_right_batch_ref: None,
            active_right_left_row: 0,
            object_bounds,
            batched_eligible,
            batched_object_eligible,
            batched_exists_eligible,
            subject_left_col,
            object_left_col,
            batched_predicate,
            batched_accumulator: Vec::new(),
            stored_left_batches: Vec::new(),
            batched_output: VecDeque::new(),
            current_left_batch_stored_idx: None,
            inline_ops,
            right_scan_inline_ops,
            right_index_hint,
            out_schema: None,
            logged_runtime_mode: false,
            mode,
        }
    }

    /// Trim output to only the specified downstream variables.
    pub fn with_out_schema(mut self, downstream_vars: Option<&[VarId]>) -> Self {
        self.out_schema = compute_trimmed_vars(&self.combined_schema, downstream_vars);
        self
    }

    /// Check if any binding used in bind instructions is Poisoned
    ///
    /// If a left binding is Poisoned, the right pattern cannot match,
    /// so we should skip this left row entirely (produces no results).
    fn has_poisoned_binding(&self, left_batch: &Batch, row: usize) -> bool {
        self.bind_instructions
            .iter()
            .any(|instr| left_batch.get_by_col(row, instr.left_col).is_poisoned())
    }

    /// Check for invalid binding types on subject/predicate positions.
    ///
    /// Subject/predicate positions accept Sid and IriMatch bindings. If a shared var is
    /// bound to a literal, the pattern cannot match.
    fn has_invalid_binding_type(&self, left_batch: &Batch, row: usize) -> bool {
        self.bind_instructions.iter().any(|instr| {
            let binding = left_batch.get_by_col(row, instr.left_col);
            match instr.position {
                PatternPosition::Subject | PatternPosition::Predicate => {
                    // Unbound does not constrain; Sid, IriMatch, Iri, and encoded variants
                    // are valid. Iri can come from BIND(ex:foo AS ?s) when the IRI isn't
                    // in the namespace table — the scan will attempt encode_iri at open time.
                    // Anything else (Lit) cannot match subject/predicate.
                    !matches!(
                        binding,
                        Binding::Unbound
                            | Binding::Sid { .. }
                            | Binding::IriMatch { .. }
                            | Binding::Iri(_)
                            | Binding::EncodedSid { .. }
                            | Binding::EncodedPid { .. }
                    )
                }
                PatternPosition::Object => false,
            }
        })
    }

    /// Substitute left row bindings into right pattern.
    ///
    /// Uses a novelty-aware `BinaryGraphView` for encoded binding resolution
    /// so that novelty-only subject/string IDs resolve correctly.
    fn substitute_pattern_with_store(
        &self,
        left_batch: &Batch,
        left_row: usize,
        gv: Option<&BinaryGraphView>,
    ) -> Result<TriplePattern> {
        let mut pattern = self.right_pattern.clone();

        for instr in &self.bind_instructions {
            let binding = left_batch.get_by_col(left_row, instr.left_col);

            match instr.position {
                PatternPosition::Subject => {
                    match binding {
                        Binding::Sid { sid, .. } => {
                            pattern.s = Ref::Sid(sid.clone());
                        }
                        Binding::IriMatch { iri, .. } | Binding::Iri(iri) => {
                            // Use Ref::Iri so scan can encode for each target ledger
                            pattern.s = Ref::Iri(iri.clone());
                        }
                        Binding::EncodedSid { s_id, .. } => {
                            // Resolve encoded s_id to IRI (novelty-aware via BinaryGraphView)
                            if let Some(gv) = gv {
                                let iri = gv.resolve_subject_iri(*s_id).map_err(|e| {
                                    tracing::debug!(
                                        s_id,
                                        error = %e,
                                        "join failed to resolve encoded subject binding"
                                    );
                                    QueryError::dictionary_lookup(format!(
                                        "join subject binding: resolve subject IRI for s_id={s_id}: {e}"
                                    ))
                                })?;
                                pattern.s = Ref::Iri(Arc::from(iri));
                            }
                            // Otherwise leave as variable
                        }
                        Binding::EncodedPid { p_id } => {
                            // Predicates are IRIs; allow using an encoded predicate as a subject.
                            if let Some(gv) = gv {
                                if let Some(iri) = gv.store().resolve_predicate_iri(*p_id) {
                                    pattern.s = Ref::Iri(Arc::from(iri));
                                }
                            }
                            // Otherwise leave as variable
                        }
                        _ => {
                            // Leave as variable
                        }
                    }
                }
                PatternPosition::Predicate => {
                    match binding {
                        Binding::Sid { sid, .. } => {
                            pattern.p = Ref::Sid(sid.clone());
                        }
                        Binding::IriMatch { iri, .. } | Binding::Iri(iri) => {
                            // Use Term::Iri so scan can encode for each target ledger
                            pattern.p = Ref::Iri(iri.clone());
                        }
                        Binding::EncodedSid { s_id, .. } => {
                            // Allow cross-position reuse: an IRI bound as a subject/object can
                            // be used to bind a predicate position. Resolve via subject dict.
                            if let Some(gv) = gv {
                                let iri = gv.resolve_subject_iri(*s_id).map_err(|e| {
                                    tracing::debug!(
                                        s_id,
                                        error = %e,
                                        "join failed to resolve encoded predicate binding via subject dictionary"
                                    );
                                    QueryError::dictionary_lookup(format!(
                                        "join predicate binding via subject lookup: s_id={s_id}: {e}"
                                    ))
                                })?;
                                pattern.p = Ref::Iri(Arc::from(iri));
                            }
                            // Otherwise leave as variable
                        }
                        Binding::EncodedPid { p_id } => {
                            // Resolve encoded p_id to IRI
                            if let Some(gv) = gv {
                                if let Some(iri) = gv.store().resolve_predicate_iri(*p_id) {
                                    pattern.p = Ref::Iri(Arc::from(iri));
                                }
                            }
                            // Otherwise leave as variable
                        }
                        _ => {
                            // Leave as variable
                        }
                    }
                }
                PatternPosition::Object => {
                    match binding {
                        Binding::Sid { sid, .. } => {
                            pattern.o = Term::Sid(sid.clone());
                        }
                        Binding::IriMatch { iri, .. } | Binding::Iri(iri) => {
                            // Use Term::Iri so scan can encode for each target ledger
                            pattern.o = Term::Iri(iri.clone());
                        }
                        Binding::Lit { val, .. } => {
                            pattern.o = Term::Value(val.clone());
                        }
                        Binding::EncodedLit {
                            o_kind,
                            o_key,
                            p_id,
                            dt_id,
                            lang_id,
                            ..
                        } => {
                            // Decode encoded literal (novelty-aware via BinaryGraphView).
                            // Must use decode_value_from_kind with the correct (o_kind, dt_id, lang_id)
                            // — dt_id is a DatatypeDictId, NOT an o_type. p_id is needed for
                            // NUM_BIG per-predicate arena lookup.
                            if let Some(gv) = gv {
                                let val = gv
                                    .decode_value_from_kind(*o_kind, *o_key, *p_id, *dt_id, *lang_id)
                                    .map_err(|e| {
                                        tracing::debug!(
                                            o_kind,
                                            o_key,
                                            p_id,
                                            dt_id,
                                            lang_id,
                                            error = %e,
                                            "join failed to decode encoded object binding"
                                        );
                                        QueryError::dictionary_lookup(format!(
                                            "join object binding decode: o_kind={o_kind}, o_key={o_key}, p_id={p_id}, dt_id={dt_id}, lang_id={lang_id}: {e}"
                                        ))
                                    })?;
                                pattern.o = Term::Value(val);
                            }
                            // Otherwise leave as variable
                        }
                        Binding::EncodedSid { s_id, .. } => {
                            // Resolve encoded s_id to IRI (novelty-aware)
                            if let Some(gv) = gv {
                                let iri = gv.resolve_subject_iri(*s_id).map_err(|e| {
                                    tracing::debug!(
                                        s_id,
                                        error = %e,
                                        "join failed to resolve encoded object subject binding"
                                    );
                                    QueryError::dictionary_lookup(format!(
                                        "join object subject lookup: s_id={s_id}: {e}"
                                    ))
                                })?;
                                pattern.o = Term::Iri(Arc::from(iri));
                            }
                            // Otherwise leave as variable
                        }
                        Binding::EncodedPid { p_id } => {
                            // Allow using an encoded predicate IRI as an object IRI.
                            if let Some(gv) = gv {
                                if let Some(iri) = gv.store().resolve_predicate_iri(*p_id) {
                                    pattern.o = Term::Iri(Arc::from(iri));
                                }
                            }
                            // Otherwise leave as variable
                        }
                        Binding::Unbound | Binding::Poisoned => {
                            // Leave as variable (Poisoned vars from OPTIONAL also remain unbound)
                        }
                        Binding::Grouped(_) => {
                            // Grouped bindings shouldn't appear in join codepaths
                            debug_assert!(false, "Grouped binding in join bind");
                            // Leave as variable
                        }
                    }
                }
            }
        }

        Ok(pattern)
    }

    /// Check if left row bindings match right row bindings for shared vars
    ///
    /// Returns true if all shared vars have equal values on both sides.
    ///
    /// Uses `eq_for_join()` for same-ledger SID optimization when comparing
    /// `IriMatch` bindings from the same ledger.
    fn unify_check(
        &self,
        left_batch: &Batch,
        left_row: usize,
        right_batch: &Batch,
        right_row: usize,
    ) -> bool {
        self.unify_instructions.iter().all(|instr| {
            let left_val = left_batch.get_by_col(left_row, instr.left_col);
            let right_val = right_batch.get_by_col(right_row, instr.right_col);
            left_val.eq_for_join(right_val)
        })
    }

    /// Combine left row with right row into output row
    fn combine_rows(
        &self,
        left_batch: &Batch,
        left_row: usize,
        right_batch: &Batch,
        right_row: usize,
    ) -> Vec<Binding> {
        let right_schema = right_batch.schema();

        // Chain left columns with new right columns (skip shared vars already in left).
        //
        // Special case: when a left-side shared variable is Unbound or Poisoned
        // (e.g., from VALUES with UNDEF, or failed OPTIONAL), the right scan may
        // still produce a concrete value for it (because the bind substitution left
        // it as a variable in the scan pattern).  In that case, we take the right-side
        // value instead of propagating the unbound marker.
        //
        // Perf note: the `right_schema` scan is O(right_schema.len()) which is at most
        // 3 for a single triple pattern (s, p, o).  The `is_unbound_or_poisoned()` check
        // is two enum-discriminant comparisons and almost always false for normal queries,
        // so the branch predictor handles it efficiently.  Pre-computing a right-col map
        // is not feasible here because the right schema varies per left row (depends on
        // which bindings were substituted at scan time).
        (0..self.left_schema.len())
            .map(|col| {
                let left_val = left_batch.get_by_col(left_row, col);
                if left_val.is_unbound_or_poisoned() {
                    let var = self.left_schema[col];
                    if let Some(right_col) = right_schema.iter().position(|v| *v == var) {
                        let right_val = right_batch.get_by_col(right_row, right_col);
                        if !right_val.is_unbound_or_poisoned() {
                            return right_val.clone();
                        }
                    }
                }
                left_val.clone()
            })
            .chain(self.right_new_vars.iter().map(|var| {
                right_schema
                    .iter()
                    .position(|v| v == var)
                    .map(|right_col| right_batch.get_by_col(right_row, right_col).clone())
                    .unwrap_or(Binding::Unbound)
            }))
            .collect()
    }

    fn bounds_for_row(
        &self,
        _left_batch: &Batch,
        _left_row: usize,
        _ctx: &ExecutionContext<'_>,
    ) -> Result<Option<ObjectBounds>> {
        Ok(self.object_bounds.clone())
    }
}

#[async_trait]
impl Operator for NestedLoopJoinOperator {
    fn schema(&self) -> &[VarId] {
        effective_schema(&self.out_schema, &self.combined_schema)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(QueryError::OperatorClosed);
            }
            return Err(QueryError::OperatorAlreadyOpened);
        }

        // Open left operator
        self.left.open(ctx).await?;

        // Reset state for fresh execution
        self.pending_output.clear();
        self.pending_right_row = 0;
        self.current_left_batch = None;
        self.current_left_row = 0;
        if let Some(mut scan) = self.active_right_scan.take() {
            scan.close();
        }
        self.active_right_batch_ref = None;
        self.active_right_left_row = 0;
        self.logged_runtime_mode = false;

        tracing::trace!(
            left_schema_cols = self.left_schema.len(),
            right_new_vars = self.right_new_vars.len(),
            bind_instructions = self.bind_instructions.len(),
            subject_bound_from_left = self
                .bind_instructions
                .iter()
                .any(|b| b.position == PatternPosition::Subject),
            object_bound_from_left = self
                .bind_instructions
                .iter()
                .any(|b| b.position == PatternPosition::Object),
            unify_instructions = self.unify_instructions.len(),
            has_object_bounds = self.object_bounds.is_some(),
            right_subject_is_var = matches!(&self.right_pattern.s, Ref::Var(_)),
            right_predicate_is_fixed = self.right_pattern.p.is_sid(),
            right_object_is_var = matches!(&self.right_pattern.o, Term::Var(_)),
            right_object_is_bound = !matches!(&self.right_pattern.o, Term::Var(_)),
            right_has_dtc = self.right_pattern.dtc.is_some(),
            batched_eligible = self.batched_eligible,
            batched_object_eligible = self.batched_object_eligible,
            batched_exists_eligible = self.batched_exists_eligible,
            "opened nested loop join"
        );

        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if !self.state.can_next() {
            if self.state == OperatorState::Created {
                return Err(QueryError::OperatorNotOpened);
            }
            return Ok(None);
        }

        // Check if batched mode is usable at runtime:
        // - Binary store must be available (batched path reads leaf files directly)
        // - Single-db mode (ActiveGraphs::Single), subjects are Binding::Sid
        // - Dataset mode with exactly one graph (ActiveGraphs::Many len==1), subjects are Binding::IriMatch
        let use_batched =
            (self.batched_eligible || self.batched_object_eligible || self.batched_exists_eligible)
                && ctx.binary_store.is_some()
                && match ctx.active_graphs() {
                    // Object-batched path currently emits `Binding::EncodedSid` for the new
                    // subject var. Keep it single-ledger only to avoid dataset-mode IriMatch
                    // requirements.
                    ActiveGraphs::Single => true,
                    ActiveGraphs::Many(graphs) => {
                        (self.batched_eligible || self.batched_exists_eligible) && graphs.len() == 1
                    }
                };

        if !self.logged_runtime_mode {
            let (active_graph_mode, active_graph_count) = match ctx.active_graphs() {
                ActiveGraphs::Single => ("single", 1usize),
                ActiveGraphs::Many(graphs) => ("many", graphs.len()),
            };
            tracing::trace!(
                use_batched,
                batched_eligible = self.batched_eligible,
                batched_object_eligible = self.batched_object_eligible,
                batched_exists_eligible = self.batched_exists_eligible,
                has_binary_store = ctx.binary_store.is_some(),
                active_graph_mode,
                active_graph_count,
                "nested loop join runtime path"
            );
            self.logged_runtime_mode = true;
        }

        // Cache novelty-aware graph view once for the entire next_batch call
        // (avoids repeated Arc::clone + construction per-row).
        let cached_gv = ctx.graph_view();

        // Process until we have output or exhaust input
        loop {
            // 1. Pre-built output from batched flush
            if let Some(batch) = self.batched_output.pop_front() {
                return Ok(trim_batch(&self.out_schema, batch));
            }

            // 2. Pending output from per-row path
            if !self.pending_output.is_empty() {
                if let Some(batch) = self.build_output_batch(ctx).await? {
                    return Ok(trim_batch(&self.out_schema, batch));
                }
                // All pending rows were filtered out — continue to process more left rows
                continue;
            }

            // 3. Resume an in-flight right scan for the current left row.
            if let Some(scan) = &mut self.active_right_scan {
                let next = scan.next_batch(ctx).await?;

                match next {
                    Some(batch) if !batch.is_empty() => {
                        let batch_ref = self
                            .active_right_batch_ref
                            .clone()
                            .expect("active scan should track left batch");
                        self.pending_output.push_back((
                            batch_ref,
                            self.active_right_left_row,
                            batch,
                        ));
                        continue;
                    }
                    Some(_) => continue,
                    None => {
                        if let Some(mut scan) = self.active_right_scan.take() {
                            scan.close();
                        }
                        self.active_right_batch_ref = None;
                        self.active_right_left_row = 0;
                        continue;
                    }
                }
            }

            // 4. Need to process more left rows
            if self.current_left_batch.is_none() {
                match self.left.next_batch(ctx).await? {
                    Some(batch) => {
                        self.current_left_batch = Some(batch);
                        self.current_left_row = 0;
                        // New batch loaded: reset stored index cache.
                        self.current_left_batch_stored_idx = None;
                    }
                    None => {
                        // Left exhausted — flush any remaining accumulator
                        if !self.batched_accumulator.is_empty() {
                            self.flush_batched_accumulator_for_ctx(ctx).await?;
                            continue;
                        }
                        self.state = OperatorState::Exhausted;
                        return Ok(None);
                    }
                }
            }

            // Check if we've exhausted the current left batch
            let batch_len = self.current_left_batch.as_ref().unwrap().len();
            if self.current_left_row >= batch_len {
                self.current_left_batch = None;
                self.current_left_batch_stored_idx = None;
                continue;
            }

            let left_row = self.current_left_row;
            self.current_left_row += 1;

            // Check for poisoned bindings
            {
                let left_batch = self.current_left_batch.as_ref().unwrap();
                if self.has_poisoned_binding(left_batch, left_row) {
                    continue;
                }
                if self.has_invalid_binding_type(left_batch, left_row) {
                    continue;
                }
            }

            if use_batched {
                // Batched path: extract the bound key directly to avoid dictionary round-trips.
                // - Subject-batched: key is subject s_id (bound to right subject).
                // - Object-batched: key is object s_id (bound to right object).
                let left_col = if self.batched_object_eligible {
                    self.object_left_col.unwrap()
                } else {
                    self.subject_left_col.unwrap()
                };

                let resolved: Option<u64> = {
                    let left_batch = self.current_left_batch.as_ref().unwrap();
                    let store = ctx.binary_store.as_deref();
                    match left_batch.get_by_col(left_row, left_col) {
                        Binding::EncodedSid { s_id, .. } => Some(*s_id),
                        Binding::Sid { sid, .. } => store.and_then(|s| {
                            s.find_subject_id_by_parts(sid.namespace_code, &sid.name)
                                .ok()
                                .flatten()
                        }),
                        Binding::IriMatch { primary_sid, .. } => store.and_then(|s| {
                            s.find_subject_id_by_parts(
                                primary_sid.namespace_code,
                                &primary_sid.name,
                            )
                            .ok()
                            .flatten()
                        }),
                        Binding::Unbound => None,
                        _ => {
                            // For subject/predicate bindings we already screened invalid types.
                            // For object-batched mode, non-ref objects can't be converted to s_id.
                            None
                        }
                    }
                };

                if let Some(key) = resolved {
                    let batch_idx = self.ensure_current_batch_stored();
                    self.batched_accumulator.push((batch_idx, left_row, key));
                    if self.batched_accumulator.len() >= BATCHED_JOIN_SIZE {
                        self.flush_batched_accumulator_for_ctx(ctx).await?;
                    }
                } else {
                    // Fall back to per-row scan for unsupported binding types.
                    let batch_idx = self.ensure_current_batch_stored();
                    let batch_ref = BatchRef::Stored(batch_idx);
                    let left_batch = self.stored_left_batches.last().unwrap();
                    let bound_pattern = self.substitute_pattern_with_store(
                        left_batch,
                        left_row,
                        cached_gv.as_ref(),
                    )?;
                    let bounds = self.bounds_for_row(left_batch, left_row, ctx)?;
                    let mut right_scan = make_right_scan(
                        bound_pattern,
                        bounds,
                        self.right_emit,
                        self.right_scan_inline_ops.clone(),
                        self.right_index_hint,
                        ctx,
                        self.mode,
                    );
                    right_scan.open(ctx).await?;
                    self.active_right_scan = Some(right_scan);
                    self.active_right_batch_ref = Some(batch_ref);
                    self.active_right_left_row = left_row;
                }
            } else {
                // Non-batched path: existing per-row join
                let batch_idx = self.ensure_current_batch_stored();
                let batch_ref = BatchRef::Stored(batch_idx);
                let left_batch = self.stored_left_batches.last().unwrap();
                let bound_pattern =
                    self.substitute_pattern_with_store(left_batch, left_row, cached_gv.as_ref())?;
                let bounds = self.bounds_for_row(left_batch, left_row, ctx)?;
                let mut right_scan = make_right_scan(
                    bound_pattern,
                    bounds,
                    self.right_emit,
                    self.right_scan_inline_ops.clone(),
                    self.right_index_hint,
                    ctx,
                    self.mode,
                );
                right_scan.open(ctx).await?;
                self.active_right_scan = Some(right_scan);
                self.active_right_batch_ref = Some(batch_ref);
                self.active_right_left_row = left_row;
            }
        }
    }

    fn close(&mut self) {
        self.left.close();
        self.current_left_batch = None;
        self.current_left_batch_stored_idx = None;
        self.pending_output.clear();
        self.pending_right_row = 0;
        if let Some(mut scan) = self.active_right_scan.take() {
            scan.close();
        }
        self.active_right_batch_ref = None;
        self.active_right_left_row = 0;
        self.batched_accumulator.clear();
        self.stored_left_batches.clear();
        self.batched_output.clear();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Estimate: left rows * some fanout factor
        self.left.estimated_rows().map(|n| n * 10)
    }
}

impl NestedLoopJoinOperator {
    /// Resolve the left batch for a given `BatchRef`.
    fn resolve_left_batch<'a>(&'a self, batch_ref: &BatchRef) -> Option<&'a Batch> {
        match batch_ref {
            BatchRef::Stored(idx) => self.stored_left_batches.get(*idx),
        }
    }

    /// Build output batch from pending results
    async fn build_output_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        let batch_size = ctx.batch_size;
        let mut output_columns: Vec<Vec<Binding>> = (0..self.combined_schema.len())
            .map(|_| Vec::with_capacity(batch_size))
            .collect();

        let mut rows_added = 0;

        while rows_added < batch_size && !self.pending_output.is_empty() {
            let (batch_ref, left_row, right_batch) = self.pending_output.front().unwrap();
            let left_row = *left_row;
            let right_batch_len = right_batch.len();

            let left_batch = match self.resolve_left_batch(batch_ref) {
                Some(b) => b,
                None => {
                    self.pending_output.pop_front();
                    self.pending_right_row = 0;
                    continue;
                }
            };

            // Process rows from this right batch, starting from where we left off
            let mut right_row = self.pending_right_row;
            while right_row < right_batch_len && rows_added < batch_size {
                // Unification check: shared vars must match
                if self.unify_check(left_batch, left_row, right_batch, right_row) {
                    // Combine and add to output
                    let mut combined =
                        self.combine_rows(left_batch, left_row, right_batch, right_row);

                    // Inline operators on the combined row
                    if !apply_inline(
                        &self.inline_ops,
                        &self.combined_schema,
                        &mut combined,
                        Some(ctx),
                    )? {
                        right_row += 1;
                        continue;
                    }

                    for (col, val) in combined.into_iter().enumerate() {
                        output_columns[col].push(val);
                    }
                    rows_added += 1;
                }
                right_row += 1;
            }

            // Check if we've fully processed this right batch
            if right_row >= right_batch_len {
                // Fully processed - remove and reset
                self.pending_output.pop_front();
                self.pending_right_row = 0;
            } else {
                // Partially processed - save our position for next call
                self.pending_right_row = right_row;
            }
        }

        if rows_added == 0 {
            return Ok(None);
        }

        // Special-case: empty schema batches still need a correct row count.
        if self.combined_schema.is_empty() {
            return Ok(Some(Batch::empty_schema_with_len(rows_added)));
        }

        Ok(Some(Batch::new(
            self.combined_schema.clone(),
            output_columns,
        )?))
    }

    /// Ensure the current left batch is stored in `stored_left_batches` and
    /// return its index. If the current batch is already the last stored batch,
    /// returns the existing index.
    fn ensure_current_batch_stored(&mut self) -> usize {
        if let Some(idx) = self.current_left_batch_stored_idx {
            return idx;
        }

        let current = self.current_left_batch.as_ref().unwrap().clone();
        self.stored_left_batches.push(current);
        let idx = self.stored_left_batches.len() - 1;
        self.current_left_batch_stored_idx = Some(idx);
        idx
    }
    /// Flush batched accumulator using the appropriate snapshot/overlay/to_t for the current context.
    ///
    /// - Single-db mode: uses ctx.active_snapshot/ctx.overlay()/ctx.to_t
    /// - Dataset mode with exactly one graph: uses that graph's snapshot/overlay/to_t
    async fn flush_batched_accumulator_for_ctx(
        &mut self,
        ctx: &ExecutionContext<'_>,
    ) -> Result<()> {
        if ctx.binary_store.is_none() {
            return Err(crate::error::QueryError::execution(
                "binary_store is required for batched joins — no non-binary fallback exists",
            ));
        }

        let accum_len = self.batched_accumulator.len();
        if self.batched_object_eligible {
            self.flush_batched_object_accumulator_binary(ctx)
                .instrument(tracing::debug_span!(
                    "join_flush_batched_object_binary",
                    accum_len,
                    batch_size = ctx.batch_size,
                    to_t = ctx.to_t,
                ))
                .await
        } else if self.batched_exists_eligible {
            self.flush_batched_exists_accumulator_binary(ctx)
                .instrument(tracing::debug_span!(
                    "join_flush_batched_exists_binary",
                    accum_len,
                    batch_size = ctx.batch_size,
                    to_t = ctx.to_t,
                ))
                .await
        } else {
            self.flush_batched_accumulator_binary(ctx)
                .instrument(tracing::debug_span!(
                    "join_flush_batched_binary",
                    accum_len,
                    batch_size = ctx.batch_size,
                    to_t = ctx.to_t,
                ))
                .await
        }
    }

    /// Clear all batched accumulator state after a flush or early return.
    fn clear_batched_state(&mut self) {
        self.batched_accumulator.clear();
        self.stored_left_batches.clear();
        self.current_left_batch_stored_idx = None;
    }

    /// Phase 1: Resolve the batched predicate SID to a binary-index p_id.
    ///
    /// Returns `None` if the predicate is not in the binary index (no results possible).
    fn resolve_batched_predicate(&self, store: &BinaryIndexStore) -> Option<u32> {
        let pred_sid = self.batched_predicate.as_ref().unwrap();
        store.sid_to_p_id(pred_sid)
    }

    /// Phase 2: Group accumulator entries by subject ID.
    ///
    /// Returns `(s_id → accumulator indices, sorted unique s_ids)`.
    /// The s_ids are already stored as raw u64 in the accumulator — no dictionary lookup needed.
    fn group_accumulator_by_subject(&self) -> (HashMap<u64, Vec<usize>>, Vec<u64>) {
        let mut s_id_to_accum: HashMap<u64, Vec<usize>> = HashMap::new();
        let mut unique_s_ids: Vec<u64> = Vec::new();
        for (accum_idx, (_, _, s_id)) in self.batched_accumulator.iter().enumerate() {
            s_id_to_accum.entry(*s_id).or_default().push(accum_idx);
            unique_s_ids.push(*s_id);
        }
        unique_s_ids.sort_unstable();
        unique_s_ids.dedup();
        (s_id_to_accum, unique_s_ids)
    }

    /// Group accumulator entries by object ID.
    ///
    /// Returns `(o_s_id → accumulator indices, (min_o, max_o))`.
    fn group_accumulator_by_object(&self) -> (HashMap<u64, Vec<usize>>, u64, u64) {
        let mut o_to_accum: HashMap<u64, Vec<usize>> = HashMap::new();
        let mut min_o: u64 = u64::MAX;
        let mut max_o: u64 = 0;
        for (accum_idx, (_, _, o_s_id)) in self.batched_accumulator.iter().enumerate() {
            let o = *o_s_id;
            o_to_accum.entry(o).or_default().push(accum_idx);
            if o < min_o {
                min_o = o;
            }
            if o > max_o {
                max_o = o;
            }
        }
        (o_to_accum, min_o, max_o)
    }

    /// Phase 3: Compute the PSOT leaf range for a predicate + subject range.
    ///
    /// Restricts the scan to the predicate's PSOT partition AND the subject range
    /// of the current left batch. Without subject bounds we'd scan the entire
    /// predicate partition even when subjects fall into a narrow range.
    fn find_psot_leaf_range(
        branch: &fluree_db_binary_index::BranchManifest,
        _g_id: GraphId,
        p_id: u32,
        min_s_id: u64,
        max_s_id: u64,
    ) -> std::ops::Range<usize> {
        use fluree_db_binary_index::format::run_record_v2::{cmp_v2_for_order, RunRecordV2};
        use fluree_db_binary_index::RunSortOrder;

        let cmp = cmp_v2_for_order(RunSortOrder::Psot);
        let min_key = RunRecordV2 {
            s_id: SubjectId::from_u64(min_s_id),
            o_key: 0,
            p_id,
            t: 0,
            o_i: 0,
            o_type: 0,
            g_id: _g_id,
        };
        let max_key = RunRecordV2 {
            s_id: SubjectId::from_u64(max_s_id),
            o_key: u64::MAX,
            p_id,
            t: u32::MAX,
            o_i: u32::MAX,
            o_type: u16::MAX,
            g_id: _g_id,
        };
        branch.find_leaves_in_range(&min_key, &max_key, cmp)
    }

    /// Phase 4: Scan PSOT leaves, matching accumulated subjects and scattering results.
    ///
    /// Uses V3 column-based leaf loading via `get_leaf_bytes_sync` + `load_leaflet_columns_cached`,
    /// binary-searches for matching subjects within each leaflet's p_id segment, builds
    /// late-materialized bindings, and scatters them to accumulator positions.
    #[allow(clippy::too_many_arguments)]
    fn scan_leaves_into_scatter(
        &self,
        ctx: &ExecutionContext<'_>,
        store: &BinaryIndexStore,
        branch: &fluree_db_binary_index::BranchManifest,
        leaf_range: std::ops::Range<usize>,
        p_id: u32,
        unique_s_ids: &[u64],
        s_id_to_accum: &HashMap<u64, Vec<usize>>,
        scatter: &mut [Vec<Vec<Binding>>],
        dict_overlay: &Option<crate::dict_overlay::DictOverlay>,
    ) -> Result<()> {
        use fluree_db_binary_index::format::leaf::{
            decode_leaf_dir_v3_with_base, decode_leaf_header_v3,
        };
        use fluree_db_binary_index::read::column_loader::load_leaflet_columns_cached;
        use fluree_db_core::o_type::OType;

        let cache = store.leaflet_cache();
        let scan_start = Instant::now();

        let mut leaflets_scanned: u64 = 0;
        let mut matched_rows: u64 = 0;

        for leaf_idx in leaf_range {
            let leaf_entry = &branch.leaves[leaf_idx];
            let leaf_bytes = store
                .get_leaf_bytes_sync(&leaf_entry.leaf_cid)
                .map_err(|e| QueryError::Internal(format!("fetch leaf: {e}")))?;

            let header = decode_leaf_header_v3(&leaf_bytes)
                .map_err(|e| QueryError::Internal(format!("read leaf header: {e}")))?;
            let dir = decode_leaf_dir_v3_with_base(&leaf_bytes, &header)
                .map_err(|e| QueryError::Internal(format!("decode leaf dir: {e}")))?;
            let leaf_id = xxhash_rust::xxh3::xxh3_128(leaf_entry.leaf_cid.to_bytes().as_ref());

            for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
                leaflets_scanned += 1;
                if entry.row_count == 0 {
                    continue;
                }
                // Skip leaflets that don't contain our predicate.
                if entry.p_const.is_some() && entry.p_const != Some(p_id) {
                    continue;
                }

                // Load all columns via V3 column loader (cached when available).
                let batch = if let Some(c) = &cache {
                    load_leaflet_columns_cached(
                        &leaf_bytes,
                        entry,
                        dir.payload_base,
                        header.order,
                        c,
                        leaf_id,
                        u32::try_from(leaflet_idx).map_err(|_| {
                            QueryError::Internal("leaflet idx exceeds u32".to_string())
                        })?,
                    )
                    .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?
                } else {
                    use fluree_db_binary_index::read::column_loader::load_leaflet_columns;
                    load_leaflet_columns(
                        &leaf_bytes,
                        entry,
                        dir.payload_base,
                        &fluree_db_binary_index::ColumnProjection::all(),
                        header.order,
                    )
                    .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?
                };

                let row_count = batch.row_count;

                // Collect matching row indices using PSOT's `(p_id, s_id, ...)` ordering.
                let mut matches: Vec<(usize, u64)> = Vec::with_capacity(64);

                // For PSOT, leaflets are sorted by p_id then s_id.
                // Find the contiguous segment for our p_id.
                let p_start = (0..row_count)
                    .position(|i| batch.p_id.get_or(i, 0) >= p_id)
                    .unwrap_or(row_count);
                let p_end = (p_start..row_count)
                    .position(|i| batch.p_id.get_or(p_start + i, 0) > p_id)
                    .map(|offset| p_start + offset)
                    .unwrap_or(row_count);
                if p_start == p_end {
                    continue;
                }

                let leaflet_s_min = batch.s_id.get(p_start);
                let leaflet_s_max = batch.s_id.get(p_end - 1);
                let subj_start = unique_s_ids.partition_point(|&x| x < leaflet_s_min);
                let subj_end = unique_s_ids.partition_point(|&x| x <= leaflet_s_max);
                if subj_start >= subj_end {
                    continue;
                }

                for &s_id in &unique_s_ids[subj_start..subj_end] {
                    if !s_id_to_accum.contains_key(&s_id) {
                        continue;
                    }
                    let row_start = lower_bound_s_id(&batch, p_start, p_end, s_id);
                    let row_end = upper_bound_s_id(&batch, p_start, p_end, s_id);
                    if row_start == row_end {
                        continue;
                    }
                    for row in row_start..row_end {
                        matches.push((row, s_id));
                    }
                }

                if matches.is_empty() {
                    continue;
                }
                matched_rows += matches.len() as u64;

                for (row, s_id) in matches {
                    let o_type_val = entry
                        .o_type_const
                        .unwrap_or_else(|| batch.o_type.get_or(row, 0));
                    let o_key_val = batch.o_key.get(row);
                    if !self.batched_row_matches_object_bounds(
                        store,
                        ctx.binary_g_id,
                        p_id,
                        o_type_val,
                        o_key_val,
                    )? {
                        continue;
                    }

                    let obj_binding = if o_type_val == OType::IRI_REF.as_u16()
                        || o_type_val == OType::BLANK_NODE.as_u16()
                    {
                        Binding::encoded_sid(o_key_val)
                    } else {
                        let p_id = entry.p_const.unwrap_or_else(|| batch.p_id.get_or(row, 0));
                        let o_i = batch.o_i.get_or(row, u32::MAX);
                        let t = batch.t.get_or(row, 0) as i64;
                        let ot = OType::from_u16(o_type_val);

                        // Prefer a stable EncodedLit representation when possible so that
                        // formatters can materialize using the root's canonical datatype table.
                        match ot.decode_kind() {
                            fluree_db_core::o_type::DecodeKind::StringDict => {
                                use fluree_db_core::ids::DatatypeDictId;
                                use fluree_db_core::value_id::ObjKind;

                                let (dt_id, lang_id) = if ot.is_lang_string() {
                                    (DatatypeDictId::LANG_STRING.as_u16(), ot.payload())
                                } else if o_type_val == OType::FULLTEXT.as_u16() {
                                    (DatatypeDictId::FULL_TEXT.as_u16(), 0)
                                } else {
                                    (DatatypeDictId::STRING.as_u16(), 0)
                                };

                                Binding::EncodedLit {
                                    o_kind: ObjKind::LEX_ID.as_u8(),
                                    o_key: o_key_val,
                                    p_id,
                                    dt_id,
                                    lang_id,
                                    i_val: if o_i == u32::MAX {
                                        i32::MIN
                                    } else {
                                        o_i as i32
                                    },
                                    t,
                                }
                            }
                            fluree_db_core::o_type::DecodeKind::JsonArena => {
                                use fluree_db_core::ids::DatatypeDictId;
                                use fluree_db_core::value_id::ObjKind;
                                Binding::EncodedLit {
                                    o_kind: ObjKind::JSON_ID.as_u8(),
                                    o_key: o_key_val,
                                    p_id,
                                    dt_id: DatatypeDictId::JSON.as_u16(),
                                    lang_id: 0,
                                    i_val: if o_i == u32::MAX {
                                        i32::MIN
                                    } else {
                                        o_i as i32
                                    },
                                    t,
                                }
                            }
                            fluree_db_core::o_type::DecodeKind::VectorArena => {
                                use fluree_db_core::ids::DatatypeDictId;
                                use fluree_db_core::value_id::ObjKind;
                                Binding::EncodedLit {
                                    o_kind: ObjKind::VECTOR_ID.as_u8(),
                                    o_key: o_key_val,
                                    p_id,
                                    dt_id: DatatypeDictId::VECTOR.as_u16(),
                                    lang_id: 0,
                                    i_val: if o_i == u32::MAX {
                                        i32::MIN
                                    } else {
                                        o_i as i32
                                    },
                                    t,
                                }
                            }
                            fluree_db_core::o_type::DecodeKind::NumBigArena => {
                                use fluree_db_core::ids::DatatypeDictId;
                                use fluree_db_core::value_id::ObjKind;
                                Binding::EncodedLit {
                                    o_kind: ObjKind::NUM_BIG.as_u8(),
                                    o_key: o_key_val,
                                    p_id,
                                    dt_id: DatatypeDictId::DECIMAL.as_u16(),
                                    lang_id: 0,
                                    i_val: if o_i == u32::MAX {
                                        i32::MIN
                                    } else {
                                        o_i as i32
                                    },
                                    t,
                                }
                            }
                            _ => {
                                // Fallback: decode eagerly, using DictOverlay for
                                // novelty-aware resolution of string/subject IDs.
                                use fluree_db_core::o_type::{DecodeKind, OType as OT};
                                let ot = OT::from_u16(o_type_val);
                                let val: fluree_db_core::FlakeValue =
                                    match (ot.decode_kind(), dict_overlay.as_ref()) {
                                        (DecodeKind::IriRef, Some(ov)) => {
                                            let iri =
                                                ov.resolve_subject_iri(o_key_val).map_err(|e| {
                                                    crate::error::QueryError::Internal(format!(
                                                        "resolve_subject_iri (batched join): {e}"
                                                    ))
                                                })?;
                                            fluree_db_core::FlakeValue::Ref(store.encode_iri(&iri))
                                        }
                                        (DecodeKind::StringDict, Some(ov)) => {
                                            let s = ov
                                                .resolve_string_value(o_key_val as u32)
                                                .map_err(|e| {
                                                    crate::error::QueryError::Internal(format!(
                                                        "resolve_string_value (batched join): {e}"
                                                    ))
                                                })?;
                                            fluree_db_core::FlakeValue::String(s)
                                        }
                                        (DecodeKind::JsonArena, Some(ov)) => {
                                            let s = ov
                                                .resolve_string_value(o_key_val as u32)
                                                .map_err(|e| {
                                                    crate::error::QueryError::Internal(format!(
                                                    "resolve_string_value (batched join json): {e}"
                                                ))
                                                })?;
                                            fluree_db_core::FlakeValue::Json(s)
                                        }
                                        _ => store
                                            .decode_value_v3(
                                                o_type_val,
                                                o_key_val,
                                                p_id,
                                                ctx.binary_g_id,
                                            )
                                            .map_err(|e| {
                                                crate::error::QueryError::Internal(format!(
                                                    "decode_value_v3 (batched join): {e}"
                                                ))
                                            })?,
                                    };
                                materialized_object_binding(
                                    store,
                                    o_type_val,
                                    p_id,
                                    val,
                                    Some(t),
                                    None,
                                )
                            }
                        }
                    };

                    if let Some(accum_indices) = s_id_to_accum.get(&s_id) {
                        for &accum_idx in accum_indices {
                            let (batch_idx, row_idx, _) = &self.batched_accumulator[accum_idx];
                            let left_batch = &self.stored_left_batches[*batch_idx];
                            let mut right_bindings = Vec::with_capacity(self.right_new_vars.len());
                            for _ in &self.right_new_vars {
                                right_bindings.push(obj_binding.clone());
                            }
                            if !self.apply_right_scan_inline_ops(ctx, &mut right_bindings)? {
                                continue;
                            }

                            let mut combined = Vec::with_capacity(self.combined_schema.len());
                            for col in 0..self.left_schema.len() {
                                combined.push(left_batch.get_by_col(*row_idx, col).clone());
                            }
                            combined.extend(right_bindings);

                            if !apply_inline(
                                &self.inline_ops,
                                &self.combined_schema,
                                &mut combined,
                                Some(ctx),
                            )? {
                                continue;
                            }

                            scatter[accum_idx].push(combined);
                        }
                    }
                }
            }
        }

        tracing::debug!(
            scan_ms = (scan_start.elapsed().as_secs_f64() * 1000.0) as u64,
            leaflets_scanned,
            matched_rows,
            "join batched binary scan complete"
        );

        Ok(())
    }

    fn batched_row_matches_object_bounds(
        &self,
        store: &BinaryIndexStore,
        g_id: GraphId,
        p_id: u32,
        o_type: u16,
        o_key: u64,
    ) -> Result<bool> {
        let Some(bounds) = &self.object_bounds else {
            return Ok(true);
        };
        let val = store
            .decode_value_v3(o_type, o_key, p_id, g_id)
            .map_err(|e| QueryError::Internal(format!("decode_value_v3 (batched bounds): {e}")))?;
        Ok(bounds.matches(&val))
    }

    /// Phase 5: Assemble scattered results into output batches in left-row order.
    ///
    /// Drains the scatter buffer (indexed by accumulator position) into columnar
    /// output batches, respecting `batch_size`. Clears accumulator state when done.
    fn emit_scatter_to_output(
        &mut self,
        mut scatter: Vec<Vec<Vec<Binding>>>,
        batch_size: usize,
    ) -> Result<()> {
        let accum_len = scatter.len();
        let num_cols = self.combined_schema.len();
        let mut output_columns: Vec<Vec<Binding>> = (0..num_cols)
            .map(|_| Vec::with_capacity(batch_size))
            .collect();
        let mut rows_added = 0;

        for scatter_item in scatter.iter_mut().take(accum_len) {
            for combined in scatter_item.drain(..) {
                for (col, val) in combined.into_iter().enumerate() {
                    output_columns[col].push(val);
                }
                rows_added += 1;

                if rows_added >= batch_size {
                    if num_cols == 0 {
                        self.batched_output
                            .push_back(Batch::empty_schema_with_len(rows_added));
                    } else {
                        let batch = Batch::new(self.combined_schema.clone(), output_columns)?;
                        self.batched_output.push_back(batch);
                    }
                    output_columns = (0..num_cols)
                        .map(|_| Vec::with_capacity(batch_size))
                        .collect();
                    rows_added = 0;
                }
            }
        }

        if rows_added > 0 {
            if num_cols == 0 {
                self.batched_output
                    .push_back(Batch::empty_schema_with_len(rows_added));
            } else {
                let batch = Batch::new(self.combined_schema.clone(), output_columns)?;
                self.batched_output.push_back(batch);
            }
        }

        self.clear_batched_state();
        Ok(())
    }

    /// Binary-index batched scan orchestrator.
    ///
    /// Implements a true batched scan over PSOT leaf files:
    /// 1. Resolve the fixed predicate to a binary-index p_id
    /// 2. Group accumulated subjects by s_id
    /// 3. Compute the PSOT leaf range for the subject range
    /// 4. Scan candidate leaves, matching subjects and scattering results
    /// 5. Emit output batches in left-row order
    ///
    /// This avoids opening/decompressing leaflets once per subject, which can be
    /// catastrophically slow for large left batches.
    async fn flush_batched_accumulator_binary(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        use fluree_db_binary_index::RunSortOrder;

        if self.batched_accumulator.is_empty() {
            return Ok(());
        }

        let overall_start = Instant::now();
        let store = ctx.binary_store.as_ref().unwrap().clone();

        // Create DictOverlay for novelty-aware decoding in the fallback path.
        let dict_overlay = make_dict_overlay(ctx, &store);

        // Phase 1: Resolve predicate
        let p_id = match self.resolve_batched_predicate(&store) {
            Some(id) => id,
            None => {
                self.clear_batched_state();
                return Ok(());
            }
        };

        // Phase 2+3: Group by subject and find PSOT leaf range
        let (s_id_to_accum, unique_s_ids) = self.group_accumulator_by_subject();
        if unique_s_ids.is_empty() {
            self.clear_batched_state();
            return Ok(());
        }
        let branch = store
            .branch_for_order(ctx.binary_g_id, RunSortOrder::Psot)
            .ok_or_else(|| QueryError::Internal("PSOT index not found for graph".into()))?;
        let leaf_range = Self::find_psot_leaf_range(
            branch,
            ctx.binary_g_id,
            p_id,
            unique_s_ids[0],
            *unique_s_ids.last().unwrap(),
        );

        // Phase 4: Scan leaves → scatter buffer
        let mut scatter: Vec<Vec<Vec<Binding>>> = vec![Vec::new(); self.batched_accumulator.len()];
        self.scan_leaves_into_scatter(
            ctx,
            &store,
            branch,
            leaf_range,
            p_id,
            &unique_s_ids,
            &s_id_to_accum,
            &mut scatter,
            &dict_overlay,
        )?;

        // Phase 5: Emit output batches
        self.emit_scatter_to_output(scatter, ctx.batch_size)?;

        tracing::debug!(
            total_ms = (overall_start.elapsed().as_secs_f64() * 1000.0) as u64,
            output_rows = self
                .batched_output
                .iter()
                .map(super::binding::Batch::len)
                .sum::<usize>(),
            "join batched binary flush complete"
        );

        Ok(())
    }

    /// Binary-index batched scan for object-bound joins.
    ///
    /// Scans OPST for `(o_type=IRI_REF, o_key in accumulator set, p_id=fixed)` and
    /// emits combined rows that bind the right pattern's new subject var.
    async fn flush_batched_object_accumulator_binary(
        &mut self,
        ctx: &ExecutionContext<'_>,
    ) -> Result<()> {
        use fluree_db_binary_index::format::leaf::{
            decode_leaf_dir_v3_with_base, decode_leaf_header_v3,
        };
        use fluree_db_binary_index::format::run_record_v2::{
            cmp_v2_for_order, read_ordered_key_v2, RunRecordV2,
        };
        use fluree_db_binary_index::read::column_loader::load_leaflet_columns;
        use fluree_db_binary_index::RunSortOrder;
        use fluree_db_core::o_type::OType;

        if self.batched_accumulator.is_empty() {
            return Ok(());
        }

        let overall_start = Instant::now();
        let store = ctx.binary_store.as_ref().unwrap().clone();

        // Resolve predicate
        let p_id = match self.resolve_batched_predicate(&store) {
            Some(id) => id,
            None => {
                self.clear_batched_state();
                return Ok(());
            }
        };

        // Group by object
        let (o_to_accum, _min_o, _max_o) = self.group_accumulator_by_object();
        if o_to_accum.is_empty() {
            self.clear_batched_state();
            return Ok(());
        }

        let Some(branch) = store.branch_for_order(ctx.binary_g_id, RunSortOrder::Opst) else {
            self.clear_batched_state();
            return Ok(());
        };
        let branch = Arc::clone(branch);

        let mut scatter: Vec<Vec<Vec<Binding>>> = vec![Vec::new(); self.batched_accumulator.len()];
        let mut matched_rows: u64 = 0;

        // Batched object join (leaf-level): scan each relevant OPST leaf at most once.
        //
        // We build a set of leaf indices that contain any of our object IDs, then scan
        // those leaves. This avoids re-opening and re-decoding leaflets once per object
        // (which is the dominant cost in `BinaryCursor`-per-object approaches).
        let iri_ref = OType::IRI_REF.as_u16();
        let cmp = cmp_v2_for_order(RunSortOrder::Opst);

        let mut objs: Vec<u64> = o_to_accum.keys().copied().collect();
        objs.sort_unstable();
        objs.dedup();
        if objs.is_empty() {
            self.clear_batched_state();
            return Ok(());
        }

        // Collect leaf indices for all object keys.
        let mut leaf_indices: Vec<usize> = Vec::new();
        for &o_s_id in &objs {
            let min_key = RunRecordV2 {
                s_id: SubjectId(0),
                o_key: o_s_id,
                p_id,
                t: 0,
                o_i: 0,
                o_type: iri_ref,
                g_id: ctx.binary_g_id,
            };
            let max_key = RunRecordV2 {
                s_id: SubjectId(u64::MAX),
                o_key: o_s_id,
                p_id,
                t: u32::MAX,
                o_i: u32::MAX,
                o_type: iri_ref,
                g_id: ctx.binary_g_id,
            };
            let r = branch.find_leaves_in_range(&min_key, &max_key, cmp);
            leaf_indices.extend(r);
        }
        leaf_indices.sort_unstable();
        leaf_indices.dedup();

        let cache = store.leaflet_cache();
        for leaf_idx in leaf_indices {
            let leaf_entry = &branch.leaves[leaf_idx];
            let leaf_bytes = store
                .get_leaf_bytes_sync(&leaf_entry.leaf_cid)
                .map_err(|e| QueryError::Internal(format!("fetch leaf: {e}")))?;

            let header = decode_leaf_header_v3(&leaf_bytes)
                .map_err(|e| QueryError::Internal(format!("read leaf header: {e}")))?;
            let dir = decode_leaf_dir_v3_with_base(&leaf_bytes, &header)
                .map_err(|e| QueryError::Internal(format!("decode leaf dir: {e}")))?;
            let leaf_id = xxhash_rust::xxh3::xxh3_128(leaf_entry.leaf_cid.to_bytes().as_ref());

            for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
                if entry.row_count == 0 {
                    continue;
                }
                if entry.p_const.is_some() && entry.p_const != Some(p_id) {
                    continue;
                }
                if entry.o_type_const.is_some() && entry.o_type_const != Some(iri_ref) {
                    continue;
                }

                // Quick reject based on leaflet key range.
                let first = read_ordered_key_v2(RunSortOrder::Opst, &entry.first_key);
                let last = read_ordered_key_v2(RunSortOrder::Opst, &entry.last_key);
                let first_o = first.o_key;
                let last_o = last.o_key;
                if last_o < objs[0] || first_o > *objs.last().unwrap() {
                    continue;
                }
                // If no object keys fall within [first_o, last_o], skip.
                let start = objs.partition_point(|&x| x < first_o);
                let end = objs.partition_point(|&x| x <= last_o);
                if start >= end {
                    continue;
                }

                // We only need core identity columns for this join. Avoid decoding all
                // columns on cache miss (the cache stores full batches).
                let core_proj = fluree_db_binary_index::read::column_types::ColumnProjection {
                    output: fluree_db_binary_index::read::column_types::ColumnSet::CORE,
                    internal: fluree_db_binary_index::read::column_types::ColumnSet::EMPTY,
                };
                let batch = if let Some(c) = &cache {
                    let key = fluree_db_binary_index::read::leaflet_cache::V3BatchCacheKey {
                        leaf_id,
                        leaflet_idx: u32::try_from(leaflet_idx).map_err(|_| {
                            QueryError::Internal("leaflet idx exceeds u32".to_string())
                        })?,
                    };
                    if let Some(cached) = c.get_v3_batch(&key) {
                        cached
                    } else {
                        load_leaflet_columns(
                            &leaf_bytes,
                            entry,
                            dir.payload_base,
                            &core_proj,
                            header.order,
                        )
                        .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?
                    }
                } else {
                    load_leaflet_columns(
                        &leaf_bytes,
                        entry,
                        dir.payload_base,
                        &core_proj,
                        header.order,
                    )
                    .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?
                };

                // OPST leaflets are ordered by (o_type, o_key, p_id, s_id, t...).
                // Instead of scanning every row in the leaflet, binary-search the
                // `o_key` column for just the object IDs we care about and only
                // visit those row ranges. This keeps work proportional to matches
                // rather than leaflet size.
                let fluree_db_binary_index::read::column_types::ColumnData::Block(o_keys) =
                    &batch.o_key
                else {
                    // o_key is required; AbsentDefault cannot occur here.
                    // Const(o_key) would mean the entire leaflet shares one object key,
                    // which is extremely rare for OPST; fall back to row-scan in that case.
                    for row in 0..batch.row_count {
                        let ot = batch.o_type.get_or(row, 0);
                        if ot != iri_ref {
                            continue;
                        }
                        let pid = batch.p_id.get_or(row, 0);
                        if pid != p_id {
                            continue;
                        }
                        let o_key = batch.o_key.get_or(row, 0);
                        let Some(accum_idxs) = o_to_accum.get(&o_key) else {
                            continue;
                        };
                        let s_id = batch.s_id.get_or(row, 0);
                        for &accum_idx in accum_idxs {
                            let (batch_idx, row_idx, _) = self.batched_accumulator[accum_idx];
                            let left_batch =
                                self.stored_left_batches.get(batch_idx).ok_or_else(|| {
                                    QueryError::Internal(
                                        "batched object join: left batch missing".into(),
                                    )
                                })?;
                            let mut right_bindings = Vec::with_capacity(self.right_new_vars.len());
                            for _ in &self.right_new_vars {
                                right_bindings.push(Binding::encoded_sid(s_id));
                            }
                            if !self.apply_right_scan_inline_ops(ctx, &mut right_bindings)? {
                                continue;
                            }

                            let mut combined: Vec<Binding> =
                                Vec::with_capacity(self.combined_schema.len());
                            for col in 0..self.left_schema.len() {
                                combined.push(left_batch.get_by_col(row_idx, col).clone());
                            }
                            combined.extend(right_bindings);

                            if !apply_inline(
                                &self.inline_ops,
                                &self.combined_schema,
                                &mut combined,
                                Some(ctx),
                            )? {
                                continue;
                            }

                            scatter[accum_idx].push(combined);
                            matched_rows += 1;
                        }
                    }
                    continue;
                };

                // Only consider the subset of objects that intersect this leaflet's object range.
                let mut obj_idx = start;
                let objs_slice = &objs[..];
                let o_keys_slice: &[u64] = o_keys.as_ref();

                // Fast path: if o_type/p_id are const and already filtered by leaflet
                // metadata, we can skip per-row checks.
                let ot_const_ok = batch.o_type.is_const() && batch.o_type.get_or(0, 0) == iri_ref;
                let pid_const_ok = batch.p_id.is_const() && batch.p_id.get_or(0, 0) == p_id;

                // Start scanning at the first possible match within this leaflet.
                let mut row = 0usize;
                while row < batch.row_count && obj_idx < end {
                    let target = objs_slice[obj_idx];

                    // Seek row to the first o_key >= target.
                    if o_keys_slice[row] < target {
                        let next = o_keys_slice[row..].partition_point(|&x| x < target);
                        row = row.saturating_add(next);
                        if row >= batch.row_count {
                            break;
                        }
                    }

                    let cur = o_keys_slice[row];
                    if cur > target {
                        obj_idx += 1;
                        continue;
                    }
                    // cur == target: process run [row, run_end).
                    let run_end = row + o_keys_slice[row..].partition_point(|&x| x == target);

                    // accum indices for this object key (bound from left).
                    let Some(accum_idxs) = o_to_accum.get(&target) else {
                        row = run_end;
                        obj_idx += 1;
                        continue;
                    };

                    for r in row..run_end {
                        if !ot_const_ok {
                            let ot = batch.o_type.get_or(r, 0);
                            if ot != iri_ref {
                                continue;
                            }
                        }
                        if !pid_const_ok {
                            let pid = batch.p_id.get_or(r, 0);
                            if pid != p_id {
                                continue;
                            }
                        }

                        let s_id = batch.s_id.get_or(r, 0);
                        for &accum_idx in accum_idxs {
                            let (batch_idx, row_idx, _) = self.batched_accumulator[accum_idx];
                            let left_batch =
                                self.stored_left_batches.get(batch_idx).ok_or_else(|| {
                                    QueryError::Internal(
                                        "batched object join: left batch missing".into(),
                                    )
                                })?;
                            let mut right_bindings = Vec::with_capacity(self.right_new_vars.len());
                            for _ in &self.right_new_vars {
                                right_bindings.push(Binding::encoded_sid(s_id));
                            }
                            if !self.apply_right_scan_inline_ops(ctx, &mut right_bindings)? {
                                continue;
                            }

                            let mut combined: Vec<Binding> =
                                Vec::with_capacity(self.combined_schema.len());
                            for col in 0..self.left_schema.len() {
                                combined.push(left_batch.get_by_col(row_idx, col).clone());
                            }
                            combined.extend(right_bindings);

                            if !apply_inline(
                                &self.inline_ops,
                                &self.combined_schema,
                                &mut combined,
                                Some(ctx),
                            )? {
                                continue;
                            }

                            scatter[accum_idx].push(combined);
                            matched_rows += 1;
                        }
                    }

                    row = run_end;
                    obj_idx += 1;
                }
            }
        }

        self.emit_scatter_to_output(scatter, ctx.batch_size)?;
        tracing::debug!(
            total_ms = (overall_start.elapsed().as_secs_f64() * 1000.0) as u64,
            matched_rows,
            "join batched object flush complete"
        );
        Ok(())
    }

    /// Binary-index batched existence scan for subject-bound joins with fixed objects.
    ///
    /// Probes PSOT once for the batch of bound subjects and scatters a copy of the
    /// left row for each matching existence hit.
    async fn flush_batched_exists_accumulator_binary(
        &mut self,
        ctx: &ExecutionContext<'_>,
    ) -> Result<()> {
        if self.batched_accumulator.is_empty() {
            return Ok(());
        }

        let overall_start = Instant::now();
        let store = ctx.binary_store.as_ref().unwrap().clone();
        let (s_id_to_accum, unique_s_ids) = self.group_accumulator_by_subject();
        if unique_s_ids.is_empty() {
            self.clear_batched_state();
            return Ok(());
        }

        let probe_matches = batched_subject_probe_binary(
            ctx,
            &store,
            &SubjectProbeParams {
                pred_sid: self
                    .batched_predicate
                    .as_ref()
                    .expect("batched predicate set"),
                subject_ids: &unique_s_ids,
                object_bounds: None,
                bound_object: Some(&self.right_pattern.o),
                emit_object: false,
                dict_overlay: None,
            },
        )?;

        let accum_len = self.batched_accumulator.len();
        let mut scatter: Vec<Vec<Vec<Binding>>> = vec![Vec::new(); accum_len];
        let mut matched_rows: u64 = 0;

        for probe_match in probe_matches {
            if let Some(accum_indices) = s_id_to_accum.get(&probe_match.subject_id) {
                for &accum_idx in accum_indices {
                    let (batch_idx, row_idx, _) = self.batched_accumulator[accum_idx];
                    let left_batch = self.stored_left_batches.get(batch_idx).ok_or_else(|| {
                        QueryError::Internal("batched existence join: left batch missing".into())
                    })?;
                    let mut combined = Vec::with_capacity(self.combined_schema.len());
                    for col in 0..self.left_schema.len() {
                        combined.push(left_batch.get_by_col(row_idx, col).clone());
                    }
                    if !apply_inline(
                        &self.inline_ops,
                        &self.combined_schema,
                        &mut combined,
                        Some(ctx),
                    )? {
                        continue;
                    }
                    scatter[accum_idx].push(combined);
                    matched_rows += 1;
                }
            }
        }

        self.emit_scatter_to_output(scatter, ctx.batch_size)?;
        let total_ms = (overall_start.elapsed().as_secs_f64() * 1000.0) as u64;
        if accum_len >= BATCHED_EXISTS_DEBUG_MIN_ACCUM || total_ms >= BATCHED_EXISTS_DEBUG_MIN_MS {
            tracing::debug!(
                total_ms,
                accum_len,
                matched_rows,
                "join batched existence flush complete"
            );
        } else {
            tracing::trace!(
                total_ms,
                accum_len,
                matched_rows,
                "join batched existence flush complete"
            );
        }
        Ok(())
    }
}

fn term_matches_probe_value(
    store: &BinaryIndexStore,
    term: &Term,
    decoded: &fluree_db_core::FlakeValue,
) -> bool {
    match term {
        Term::Var(_) => true,
        Term::Sid(sid) => matches!(decoded, fluree_db_core::FlakeValue::Ref(found) if found == sid),
        Term::Iri(iri) => {
            let sid = store.encode_iri(iri);
            matches!(decoded, fluree_db_core::FlakeValue::Ref(found) if *found == sid)
        }
        Term::Value(value) => decoded == value,
    }
}

#[allow(clippy::too_many_arguments)]
fn build_probe_object_binding(
    ctx: &ExecutionContext<'_>,
    store: &BinaryIndexStore,
    dict_overlay: Option<&crate::dict_overlay::DictOverlay>,
    p_id: u32,
    o_type_val: u16,
    o_key_val: u64,
    o_i: u32,
    t: i64,
) -> Result<Binding> {
    use fluree_db_core::o_type::{DecodeKind, OType};

    if let Some(binding) =
        late_materialized_object_binding(o_type_val, o_key_val, p_id, t, o_i, None)
    {
        return Ok(binding);
    }

    let ot = OType::from_u16(o_type_val);
    let val: fluree_db_core::FlakeValue = match (ot.decode_kind(), dict_overlay) {
        (DecodeKind::IriRef, Some(ov)) => {
            let iri = ov.resolve_subject_iri(o_key_val).map_err(|e| {
                QueryError::Internal(format!("resolve_subject_iri (batched probe): {e}"))
            })?;
            fluree_db_core::FlakeValue::Ref(store.encode_iri(&iri))
        }
        (DecodeKind::StringDict, Some(ov)) => {
            let s = ov.resolve_string_value(o_key_val as u32).map_err(|e| {
                QueryError::Internal(format!("resolve_string_value (batched probe): {e}"))
            })?;
            fluree_db_core::FlakeValue::String(s)
        }
        (DecodeKind::JsonArena, Some(ov)) => {
            let s = ov.resolve_string_value(o_key_val as u32).map_err(|e| {
                QueryError::Internal(format!("resolve_string_value (batched probe json): {e}"))
            })?;
            fluree_db_core::FlakeValue::Json(s)
        }
        _ => store
            .decode_value_v3(o_type_val, o_key_val, p_id, ctx.binary_g_id)
            .map_err(|e| QueryError::Internal(format!("decode_value_v3 (batched probe): {e}")))?,
    };
    Ok(materialized_object_binding(
        store,
        o_type_val,
        p_id,
        val,
        Some(t),
        None,
    ))
}

/// Bundled parameters for [`batched_subject_probe_binary`].
pub(crate) struct SubjectProbeParams<'a> {
    pub pred_sid: &'a Sid,
    pub subject_ids: &'a [u64],
    pub object_bounds: Option<&'a ObjectBounds>,
    pub bound_object: Option<&'a Term>,
    pub emit_object: bool,
    pub dict_overlay: Option<&'a crate::dict_overlay::DictOverlay>,
}

pub(crate) fn batched_subject_probe_binary(
    ctx: &ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    params: &SubjectProbeParams<'_>,
) -> Result<Vec<BatchedSubjectProbeMatch>> {
    use fluree_db_binary_index::format::leaf::{
        decode_leaf_dir_v3_with_base, decode_leaf_header_v3,
    };
    use fluree_db_binary_index::format::run_record_v2::{cmp_v2_for_order, RunRecordV2};
    use fluree_db_binary_index::read::column_loader::load_leaflet_columns_cached;
    use fluree_db_binary_index::{ColumnProjection, RunSortOrder};

    if params.subject_ids.is_empty() {
        return Ok(Vec::new());
    }

    let Some(p_id) = store.sid_to_p_id(params.pred_sid) else {
        return Ok(Vec::new());
    };
    let Some(branch) = store.branch_for_order(ctx.binary_g_id, RunSortOrder::Psot) else {
        return Ok(Vec::new());
    };

    let mut unique_s_ids = params.subject_ids.to_vec();
    unique_s_ids.sort_unstable();
    unique_s_ids.dedup();
    if unique_s_ids.is_empty() {
        return Ok(Vec::new());
    }

    let cmp = cmp_v2_for_order(RunSortOrder::Psot);
    let min_key = RunRecordV2 {
        s_id: SubjectId::from_u64(unique_s_ids[0]),
        o_key: 0,
        p_id,
        t: 0,
        o_i: 0,
        o_type: 0,
        g_id: ctx.binary_g_id,
    };
    let max_key = RunRecordV2 {
        s_id: SubjectId::from_u64(*unique_s_ids.last().unwrap()),
        o_key: u64::MAX,
        p_id,
        t: u32::MAX,
        o_i: u32::MAX,
        o_type: u16::MAX,
        g_id: ctx.binary_g_id,
    };
    let leaf_range = branch.find_leaves_in_range(&min_key, &max_key, cmp);
    let cache = store.leaflet_cache();
    let mut out = Vec::new();

    for leaf_idx in leaf_range {
        let leaf_entry = &branch.leaves[leaf_idx];
        let leaf_bytes = store
            .get_leaf_bytes_sync(&leaf_entry.leaf_cid)
            .map_err(|e| QueryError::Internal(format!("fetch leaf: {e}")))?;
        let header = decode_leaf_header_v3(&leaf_bytes)
            .map_err(|e| QueryError::Internal(format!("read leaf header: {e}")))?;
        let dir = decode_leaf_dir_v3_with_base(&leaf_bytes, &header)
            .map_err(|e| QueryError::Internal(format!("decode leaf dir: {e}")))?;
        let leaf_id = xxhash_rust::xxh3::xxh3_128(leaf_entry.leaf_cid.to_bytes().as_ref());

        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 {
                continue;
            }
            if entry.p_const.is_some() && entry.p_const != Some(p_id) {
                continue;
            }

            let batch = if let Some(c) = &cache {
                load_leaflet_columns_cached(
                    &leaf_bytes,
                    entry,
                    dir.payload_base,
                    header.order,
                    c,
                    leaf_id,
                    u32::try_from(leaflet_idx)
                        .map_err(|_| QueryError::Internal("leaflet idx exceeds u32".to_string()))?,
                )
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?
            } else {
                use fluree_db_binary_index::read::column_loader::load_leaflet_columns;
                load_leaflet_columns(
                    &leaf_bytes,
                    entry,
                    dir.payload_base,
                    &ColumnProjection::all(),
                    header.order,
                )
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?
            };

            let row_count = batch.row_count;
            let p_start = (0..row_count)
                .position(|i| batch.p_id.get_or(i, 0) >= p_id)
                .unwrap_or(row_count);
            let p_end = (p_start..row_count)
                .position(|i| batch.p_id.get_or(p_start + i, 0) > p_id)
                .map(|offset| p_start + offset)
                .unwrap_or(row_count);
            if p_start == p_end {
                continue;
            }

            let leaflet_s_min = batch.s_id.get(p_start);
            let leaflet_s_max = batch.s_id.get(p_end - 1);
            let subj_start = unique_s_ids.partition_point(|&x| x < leaflet_s_min);
            let subj_end = unique_s_ids.partition_point(|&x| x <= leaflet_s_max);
            if subj_start >= subj_end {
                continue;
            }

            for &s_id in &unique_s_ids[subj_start..subj_end] {
                let row_start = lower_bound_s_id(&batch, p_start, p_end, s_id);
                let row_end = upper_bound_s_id(&batch, p_start, p_end, s_id);
                if row_start == row_end {
                    continue;
                }

                for row in row_start..row_end {
                    let o_type_val = entry
                        .o_type_const
                        .unwrap_or_else(|| batch.o_type.get_or(row, 0));
                    let o_key_val = batch.o_key.get(row);

                    if params.object_bounds.is_some() || params.bound_object.is_some() {
                        let decoded = store
                            .decode_value_v3(o_type_val, o_key_val, p_id, ctx.binary_g_id)
                            .map_err(|e| {
                                QueryError::Internal(format!(
                                    "decode_value_v3 (batched subject probe filter): {e}"
                                ))
                            })?;
                        if let Some(bounds) = params.object_bounds {
                            if !bounds.matches(&decoded) {
                                continue;
                            }
                        }
                        if let Some(term) = params.bound_object {
                            if !term_matches_probe_value(store, term, &decoded) {
                                continue;
                            }
                        }
                    }

                    let object = if params.emit_object {
                        Some(build_probe_object_binding(
                            ctx,
                            store,
                            params.dict_overlay,
                            p_id,
                            o_type_val,
                            o_key_val,
                            batch.o_i.get_or(row, u32::MAX),
                            batch.t.get_or(row, 0) as i64,
                        )?)
                    } else {
                        None
                    };

                    out.push(BatchedSubjectProbeMatch {
                        subject_id: s_id,
                        object,
                    });
                }
            }
        }
    }

    Ok(out)
}

pub(crate) fn batched_subject_star_spot(
    ctx: &ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    subject_ids: &[u64],
    predicates: &[SpotStarPredicateParams<'_>],
    dict_overlay: Option<&crate::dict_overlay::DictOverlay>,
) -> Result<Vec<BatchedSpotStarMatch>> {
    use fluree_db_binary_index::format::leaf::{
        decode_leaf_dir_v3_with_base, decode_leaf_header_v3,
    };
    use fluree_db_binary_index::format::run_record_v2::{cmp_v2_for_order, RunRecordV2};
    use fluree_db_binary_index::read::column_loader::load_leaflet_columns_cached;
    use fluree_db_binary_index::{ColumnProjection, RunSortOrder};

    if subject_ids.is_empty() || predicates.is_empty() {
        return Ok(Vec::new());
    }

    let mut predicates_by_id: HashMap<u32, &SpotStarPredicateParams<'_>> = HashMap::new();
    for predicate in predicates {
        let Some(p_id) = store.sid_to_p_id(&predicate.pred_sid) else {
            continue;
        };
        predicates_by_id.insert(p_id, predicate);
    }
    if predicates_by_id.is_empty() {
        return Ok(Vec::new());
    }

    let Some(branch) = store.branch_for_order(ctx.binary_g_id, RunSortOrder::Spot) else {
        return Ok(Vec::new());
    };

    let mut unique_s_ids = subject_ids.to_vec();
    unique_s_ids.sort_unstable();
    unique_s_ids.dedup();
    if unique_s_ids.is_empty() {
        return Ok(Vec::new());
    }

    let cmp = cmp_v2_for_order(RunSortOrder::Spot);
    let min_key = RunRecordV2 {
        s_id: SubjectId::from_u64(unique_s_ids[0]),
        p_id: 0,
        o_key: 0,
        t: 0,
        o_i: 0,
        o_type: 0,
        g_id: ctx.binary_g_id,
    };
    let max_key = RunRecordV2 {
        s_id: SubjectId::from_u64(*unique_s_ids.last().unwrap()),
        p_id: u32::MAX,
        o_key: u64::MAX,
        t: u32::MAX,
        o_i: u32::MAX,
        o_type: u16::MAX,
        g_id: ctx.binary_g_id,
    };
    let leaf_range = branch.find_leaves_in_range(&min_key, &max_key, cmp);
    let cache = store.leaflet_cache();
    let mut out = Vec::new();

    for leaf_idx in leaf_range {
        let leaf_entry = &branch.leaves[leaf_idx];
        let leaf_bytes = store
            .get_leaf_bytes_sync(&leaf_entry.leaf_cid)
            .map_err(|e| QueryError::Internal(format!("fetch leaf: {e}")))?;
        let header = decode_leaf_header_v3(&leaf_bytes)
            .map_err(|e| QueryError::Internal(format!("read leaf header: {e}")))?;
        let dir = decode_leaf_dir_v3_with_base(&leaf_bytes, &header)
            .map_err(|e| QueryError::Internal(format!("decode leaf dir: {e}")))?;
        let leaf_id = xxhash_rust::xxh3::xxh3_128(leaf_entry.leaf_cid.to_bytes().as_ref());

        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 {
                continue;
            }

            let batch = if let Some(c) = &cache {
                load_leaflet_columns_cached(
                    &leaf_bytes,
                    entry,
                    dir.payload_base,
                    header.order,
                    c,
                    leaf_id,
                    u32::try_from(leaflet_idx)
                        .map_err(|_| QueryError::Internal("leaflet idx exceeds u32".to_string()))?,
                )
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?
            } else {
                use fluree_db_binary_index::read::column_loader::load_leaflet_columns;
                load_leaflet_columns(
                    &leaf_bytes,
                    entry,
                    dir.payload_base,
                    &ColumnProjection::all(),
                    header.order,
                )
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?
            };

            let row_count = batch.row_count;
            if row_count == 0 {
                continue;
            }

            let leaflet_s_min = batch.s_id.get(0);
            let leaflet_s_max = batch.s_id.get(row_count - 1);
            let subj_start = unique_s_ids.partition_point(|&x| x < leaflet_s_min);
            let subj_end = unique_s_ids.partition_point(|&x| x <= leaflet_s_max);
            if subj_start >= subj_end {
                continue;
            }

            for &s_id in &unique_s_ids[subj_start..subj_end] {
                let row_start = lower_bound_s_id(&batch, 0, row_count, s_id);
                let row_end = upper_bound_s_id(&batch, row_start, row_count, s_id);
                if row_start == row_end {
                    continue;
                }

                for row in row_start..row_end {
                    let p_id = entry.p_const.unwrap_or_else(|| batch.p_id.get_or(row, 0));
                    let Some(predicate) = predicates_by_id.get(&p_id).copied() else {
                        continue;
                    };

                    let o_type_val = entry
                        .o_type_const
                        .unwrap_or_else(|| batch.o_type.get_or(row, 0));
                    let o_key_val = batch.o_key.get(row);

                    if predicate.object_bounds.is_some() || predicate.bound_object.is_some() {
                        let decoded = store
                            .decode_value_v3(o_type_val, o_key_val, p_id, ctx.binary_g_id)
                            .map_err(|e| {
                                QueryError::Internal(format!(
                                    "decode_value_v3 (batched spot star filter): {e}"
                                ))
                            })?;
                        if let Some(bounds) = predicate.object_bounds {
                            if !bounds.matches(&decoded) {
                                continue;
                            }
                        }
                        if let Some(term) = predicate.bound_object {
                            if !term_matches_probe_value(store, term, &decoded) {
                                continue;
                            }
                        }
                    }

                    let object = if predicate.emit_object {
                        Some(build_probe_object_binding(
                            ctx,
                            store,
                            dict_overlay,
                            p_id,
                            o_type_val,
                            o_key_val,
                            batch.o_i.get_or(row, u32::MAX),
                            batch.t.get_or(row, 0) as i64,
                        )?)
                    } else {
                        None
                    };

                    out.push(BatchedSpotStarMatch {
                        subject_id: s_id,
                        predicate_idx: predicate.predicate_idx,
                        object,
                    });
                }
            }
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::Sid;

    #[test]
    fn test_bind_instruction_creation() {
        // Left schema: [?s, ?name]
        let left_schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());

        // Right pattern: ?s :age ?age (shares ?s with left)
        let _right_pattern = TriplePattern::new(
            Ref::Var(VarId(0)), // ?s - shared
            Ref::Sid(Sid::new(100, "age")),
            Term::Var(VarId(2)), // ?age - new
        );

        // Create a mock left operator - we'll use this pattern to test bind instructions
        // For now, just verify the bind_instructions are built correctly
        let left_var_positions: std::collections::HashMap<VarId, usize> = left_schema
            .iter()
            .enumerate()
            .map(|(i, v)| (*v, i))
            .collect();

        // Check that ?s would be bound
        assert!(left_var_positions.contains_key(&VarId(0)));
        assert!(!left_var_positions.contains_key(&VarId(2))); // ?age not in left
    }

    #[test]
    fn test_combined_schema() {
        // Left: [?s]
        // Right: ?s :name ?name
        // Combined should be: [?s, ?name]

        let left_schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let right_pattern = TriplePattern::new(
            Ref::Var(VarId(0)), // ?s - shared
            Ref::Sid(Sid::new(100, "name")),
            Term::Var(VarId(1)), // ?name - new
        );

        let left_var_positions: std::collections::HashMap<VarId, usize> = left_schema
            .iter()
            .enumerate()
            .map(|(i, v)| (*v, i))
            .collect();

        // Right pattern vars
        let right_vars = right_pattern.produced_vars();
        assert_eq!(right_vars, vec![VarId(0), VarId(1)]);

        // New vars (not in left)
        let new_vars: Vec<VarId> = right_vars
            .into_iter()
            .filter(|v| !left_var_positions.contains_key(v))
            .collect();
        assert_eq!(new_vars, vec![VarId(1)]);

        // Combined schema
        let mut combined = left_schema.to_vec();
        combined.extend(new_vars);
        assert_eq!(combined, vec![VarId(0), VarId(1)]);
    }

    #[test]
    fn test_batched_subject_join_remains_eligible_with_object_bounds() {
        let left_schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let right_pattern = TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(Sid::new(100, "date")),
            Term::Var(VarId(1)),
        );

        let left_var_positions: std::collections::HashMap<VarId, usize> = left_schema
            .iter()
            .enumerate()
            .map(|(i, v)| (*v, i))
            .collect();
        let bind_instructions = vec![BindInstruction {
            position: PatternPosition::Subject,
            left_col: *left_var_positions.get(&VarId(0)).unwrap(),
        }];

        assert!(is_batched_eligible(&bind_instructions, &right_pattern,));
    }

    #[test]
    fn test_unify_instructions() {
        // Left: [?s, ?name]
        // Right: ?s :age ?age (only ?s is shared)
        // Unify should check: ?s in left col 0 matches ?s in right col 0

        let left_schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let right_pattern = TriplePattern::new(
            Ref::Var(VarId(0)), // ?s at right position 0
            Ref::Sid(Sid::new(100, "age")),
            Term::Var(VarId(2)), // ?age at right position 1
        );

        let left_var_positions: std::collections::HashMap<VarId, usize> = left_schema
            .iter()
            .enumerate()
            .map(|(i, v)| (*v, i))
            .collect();

        // Build unify instructions
        let mut unify_instructions = Vec::new();
        for var in right_pattern.produced_vars() {
            if let Some(&left_col) = left_var_positions.get(&var) {
                let right_vars = right_pattern.produced_vars();
                if let Some(right_idx) = right_vars.iter().position(|v| *v == var) {
                    unify_instructions.push(UnifyInstruction {
                        left_col,
                        right_col: right_idx,
                    });
                }
            }
        }

        // Should have one unify instruction for ?s
        assert_eq!(unify_instructions.len(), 1);
        assert_eq!(unify_instructions[0].left_col, 0); // ?s is col 0 in left
        assert_eq!(unify_instructions[0].right_col, 0); // ?s is col 0 in right pattern output
    }

    #[test]
    fn test_has_poisoned_binding() {
        use fluree_db_core::FlakeValue;

        // Create a simple operator setup
        let left_schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let right_pattern = TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(Sid::new(100, "age")),
            Term::Var(VarId(2)),
        );

        // Create a mock operator
        struct MockOp;
        #[async_trait]
        impl Operator for MockOp {
            fn schema(&self) -> &[VarId] {
                &[]
            }
            async fn open(&mut self, _: &ExecutionContext<'_>) -> Result<()> {
                Ok(())
            }
            async fn next_batch(&mut self, _: &ExecutionContext<'_>) -> Result<Option<Batch>> {
                Ok(None)
            }
            fn close(&mut self) {}
        }

        let join = NestedLoopJoinOperator::new(
            Box::new(MockOp),
            left_schema.clone(),
            right_pattern,
            None, // No object bounds
            Vec::new(),
            EmitMask::ALL,
            crate::temporal_mode::TemporalMode::Current,
        );

        // Create a batch with one row that has Poisoned in position 0 (used for binding)
        let columns_poisoned = vec![
            vec![Binding::Poisoned],
            vec![Binding::lit(
                FlakeValue::String("Alice".to_string()),
                Sid::new(2, "string"),
            )],
        ];
        let batch_poisoned = Batch::new(left_schema.clone(), columns_poisoned).unwrap();

        // Row 0 has Poisoned in position 0, which is used for subject binding
        assert!(join.has_poisoned_binding(&batch_poisoned, 0));

        // Create a batch with one row that has NO Poisoned bindings
        let columns_normal = vec![
            vec![Binding::sid(Sid::new(1, "alice"))],
            vec![Binding::lit(
                FlakeValue::String("Alice".to_string()),
                Sid::new(2, "string"),
            )],
        ];
        let batch_normal = Batch::new(left_schema.clone(), columns_normal).unwrap();

        // Row 0 has no Poisoned bindings
        assert!(!join.has_poisoned_binding(&batch_normal, 0));

        // Create a batch where Poisoned is in position 1 (NOT used for binding)
        let columns_poisoned_unused = vec![
            vec![Binding::sid(Sid::new(1, "alice"))],
            vec![Binding::Poisoned], // This is in position 1, not used for binding ?s
        ];
        let batch_poisoned_unused = Batch::new(left_schema, columns_poisoned_unused).unwrap();

        // Row 0 has Poisoned in position 1, but position 1 is not used for binding
        // (only position 0 is used for ?s)
        assert!(!join.has_poisoned_binding(&batch_poisoned_unused, 0));
    }

    /// Verify that shared vars at Object position with Lit bindings are substituted,
    /// not unified. This tests that the join correctly handles the case where a
    /// shared var is substituted into the pattern rather than requiring unification.
    ///
    /// When ?v is at Object position and has a Lit binding, it gets substituted
    /// into the pattern as a constant. The right scan then only outputs the
    /// remaining variables (?x in this case), and no unification check is needed.
    #[tokio::test]
    async fn test_join_substituted_var_no_unification() {
        use crate::context::ExecutionContext;
        use crate::var_registry::VarRegistry;
        use fluree_db_core::{FlakeValue, LedgerSnapshot};

        // Minimal context (db is unused here; only batch_size matters).
        let snapshot = LedgerSnapshot::genesis("test/main");
        let mut vars = VarRegistry::new();
        let x = vars.get_or_insert("?x"); // VarId(0)
        let v = vars.get_or_insert("?v"); // VarId(1)
        let ctx = ExecutionContext::new(&snapshot, &vars);

        // Left schema: [?v]
        let left_schema: Arc<[VarId]> = Arc::from(vec![v].into_boxed_slice());
        // Right pattern: ?x p ?v (shared ?v at Object position)
        let right_pattern =
            TriplePattern::new(Ref::Var(x), Ref::Sid(Sid::new(100, "p")), Term::Var(v));

        // Mock left operator (unused; we inject batches directly into join state).
        struct MockOp;
        #[async_trait]
        impl Operator for MockOp {
            fn schema(&self) -> &[VarId] {
                &[]
            }
            async fn open(&mut self, _: &ExecutionContext<'_>) -> Result<()> {
                Ok(())
            }
            async fn next_batch(&mut self, _: &ExecutionContext<'_>) -> Result<Option<Batch>> {
                Ok(None)
            }
            fn close(&mut self) {}
        }

        let mut join = NestedLoopJoinOperator::new(
            Box::new(MockOp),
            left_schema.clone(),
            right_pattern,
            None, // No object bounds
            Vec::new(),
            EmitMask::ALL,
            crate::temporal_mode::TemporalMode::Current,
        );

        // Verify that ?v is NOT in unify_instructions (it's substituted, not unified)
        assert!(
            join.unify_instructions.is_empty(),
            "No unify instructions expected when shared var at Object position has Lit binding"
        );

        // Left batch: ?v = 1
        let left_batch = Batch::new(
            left_schema,
            vec![vec![Binding::lit(FlakeValue::Long(1), Sid::new(2, "long"))]],
        )
        .unwrap();

        // Right batch from scan would only have ?x (since ?v was substituted)
        // The scan would have pattern: ?x p 1 (the literal value)
        let right_schema: Arc<[VarId]> = Arc::from(vec![x].into_boxed_slice());
        let right_batch = Batch::new(
            right_schema,
            vec![vec![Binding::lit(
                FlakeValue::Long(10),
                Sid::new(2, "long"),
            )]],
        )
        .unwrap();

        join.stored_left_batches.push(left_batch);
        join.pending_output
            .push_back((BatchRef::Stored(0), 0, right_batch));

        // Should produce output since no unification check is needed
        let out = join.build_output_batch(&ctx).await.unwrap();
        assert!(
            out.is_some(),
            "Expected output when var is substituted (no unification)"
        );
        let batch = out.unwrap();
        assert_eq!(batch.len(), 1);
        // Output schema is [?v, ?x] (left vars + new right vars)
        assert_eq!(batch.schema(), &[v, x]);
    }

    #[test]
    fn test_join_prefers_opst_for_prefix_filtered_right_scan() {
        let left_schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let right_pattern = TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(Sid::new(100, "name")),
            Term::Var(VarId(1)),
        );

        struct MockOp;
        #[async_trait]
        impl Operator for MockOp {
            fn schema(&self) -> &[VarId] {
                &[]
            }
            async fn open(&mut self, _: &ExecutionContext<'_>) -> Result<()> {
                Ok(())
            }
            async fn next_batch(&mut self, _: &ExecutionContext<'_>) -> Result<Option<Batch>> {
                Ok(None)
            }
            fn close(&mut self) {}
        }

        let join = NestedLoopJoinOperator::new(
            Box::new(MockOp),
            left_schema,
            right_pattern,
            None,
            vec![InlineOperator::Filter(
                crate::expression::PreparedBoolExpression::new(crate::ir::Expression::call(
                    crate::ir::Function::StrStarts,
                    vec![
                        crate::ir::Expression::Var(VarId(1)),
                        crate::ir::Expression::Const(crate::ir::FlakeValue::String(
                            "Ali".to_string(),
                        )),
                    ],
                )),
            )],
            EmitMask::ALL,
            crate::temporal_mode::TemporalMode::Current,
        );

        assert_eq!(join.right_index_hint, Some(IndexType::Opst));
        assert_eq!(join.right_scan_inline_ops.len(), 1);
        assert!(join.inline_ops.is_empty());
    }

    #[test]
    fn test_join_enables_batched_subject_existence_for_fixed_object() {
        let left_schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let right_pattern = TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(Sid::new(100, "type")),
            Term::Sid(Sid::new(101, "Person")),
        );

        struct MockOp;
        #[async_trait]
        impl Operator for MockOp {
            fn schema(&self) -> &[VarId] {
                &[]
            }
            async fn open(&mut self, _: &ExecutionContext<'_>) -> Result<()> {
                Ok(())
            }
            async fn next_batch(&mut self, _: &ExecutionContext<'_>) -> Result<Option<Batch>> {
                Ok(None)
            }
            fn close(&mut self) {}
        }

        let join = NestedLoopJoinOperator::new(
            Box::new(MockOp),
            left_schema,
            right_pattern,
            None,
            Vec::new(),
            EmitMask {
                s: false,
                p: false,
                o: false,
            },
            crate::temporal_mode::TemporalMode::Current,
        );

        assert!(!join.batched_eligible);
        assert!(!join.batched_object_eligible);
        assert!(join.batched_exists_eligible);
        assert_eq!(join.subject_left_col, Some(0));
        assert!(join.object_left_col.is_none());
        assert!(join.right_new_vars.is_empty());
    }

    #[test]
    fn test_join_keeps_mixed_left_right_filter_post_join() {
        let left_schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let right_pattern = TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(Sid::new(100, "name")),
            Term::Var(VarId(1)),
        );

        struct MockOp;
        #[async_trait]
        impl Operator for MockOp {
            fn schema(&self) -> &[VarId] {
                &[]
            }
            async fn open(&mut self, _: &ExecutionContext<'_>) -> Result<()> {
                Ok(())
            }
            async fn next_batch(&mut self, _: &ExecutionContext<'_>) -> Result<Option<Batch>> {
                Ok(None)
            }
            fn close(&mut self) {}
        }

        let join = NestedLoopJoinOperator::new(
            Box::new(MockOp),
            left_schema,
            right_pattern,
            None,
            vec![InlineOperator::Filter(
                crate::expression::PreparedBoolExpression::new(crate::ir::Expression::eq(
                    crate::ir::Expression::Var(VarId(0)),
                    crate::ir::Expression::Var(VarId(1)),
                )),
            )],
            EmitMask::ALL,
            crate::temporal_mode::TemporalMode::Current,
        );

        assert!(join.right_scan_inline_ops.is_empty());
        assert_eq!(join.inline_ops.len(), 1);
    }

    /// Verify join produces correct output when unification IS needed.
    ///
    /// This tests a scenario where a shared variable is NOT substituted:
    /// - ?v appears in left schema
    /// - ?v appears at Subject position in right pattern
    /// - But left binding is Lit (not Sid), so substitution doesn't happen
    /// - Variable remains in right pattern, right scan outputs it
    /// - Unification check is needed
    ///
    /// However, since Subject position only substitutes Sid bindings, if the
    /// left binding is Lit, the scan will look for subject=?v (unbound) which
    /// returns ALL subjects. The unification then filters to matching values.
    ///
    /// Actually, the current implementation skips unify_instructions for ALL
    /// vars with bind_instructions, even if substitution might not happen.
    /// This test verifies the behavior when a shared var is at Subject position
    /// but has a Lit binding - currently this scenario isn't expected in practice
    /// because we typically have Sid bindings for Subject/Predicate joins.
    #[tokio::test]
    async fn test_join_multiple_new_vars() {
        use crate::context::ExecutionContext;
        use crate::var_registry::VarRegistry;
        use fluree_db_core::{FlakeValue, LedgerSnapshot};

        let snapshot = LedgerSnapshot::genesis("test/main");
        let mut vars = VarRegistry::new();
        let s = vars.get_or_insert("?s"); // VarId(0)
        let x = vars.get_or_insert("?x"); // VarId(1)
        let y = vars.get_or_insert("?y"); // VarId(2)
        let ctx = ExecutionContext::new(&snapshot, &vars);

        // Left schema: [?s] - a Sid (e.g., from VALUES or prior scan)
        let left_schema: Arc<[VarId]> = Arc::from(vec![s].into_boxed_slice());
        // Right pattern: ?s p ?x with separate object ?y
        // Actually let's test: ?x p ?y where neither is in left schema
        let right_pattern =
            TriplePattern::new(Ref::Var(x), Ref::Sid(Sid::new(100, "p")), Term::Var(y));

        struct MockOp;
        #[async_trait]
        impl Operator for MockOp {
            fn schema(&self) -> &[VarId] {
                &[]
            }
            async fn open(&mut self, _: &ExecutionContext<'_>) -> Result<()> {
                Ok(())
            }
            async fn next_batch(&mut self, _: &ExecutionContext<'_>) -> Result<Option<Batch>> {
                Ok(None)
            }
            fn close(&mut self) {}
        }

        let mut join = NestedLoopJoinOperator::new(
            Box::new(MockOp),
            left_schema.clone(),
            right_pattern,
            None, // No object bounds
            Vec::new(),
            EmitMask::ALL,
            crate::temporal_mode::TemporalMode::Current,
        );

        // No unify_instructions since no shared vars between left [?s] and right [?x, ?y]
        assert!(join.unify_instructions.is_empty());

        // Left batch: ?s = some:subject (a Sid)
        let left_batch = Batch::new(
            left_schema,
            vec![vec![Binding::sid(Sid::new(1, "some:subject"))]],
        )
        .unwrap();

        // Right batch: ?x = some:other, ?y = 42
        let right_schema: Arc<[VarId]> = Arc::from(vec![x, y].into_boxed_slice());
        let right_batch = Batch::new(
            right_schema,
            vec![
                vec![Binding::sid(Sid::new(1, "some:other"))],
                vec![Binding::lit(FlakeValue::Long(42), Sid::new(2, "long"))],
            ],
        )
        .unwrap();

        join.stored_left_batches.push(left_batch);
        join.pending_output
            .push_back((BatchRef::Stored(0), 0, right_batch));

        let out = join
            .build_output_batch(&ctx)
            .await
            .unwrap()
            .expect("Expected output batch");

        assert_eq!(out.len(), 1);
        // Combined schema: left [?s] + new right vars [?x, ?y]
        assert_eq!(out.schema(), &[s, x, y]);
    }
}
