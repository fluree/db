//! Left-join operator for OPTIONAL semantics
//!
//! This module provides `OptionalOperator` which implements left outer join
//! (OPTIONAL) semantics. When the optional pattern has no matches, the operator
//! emits `Binding::Poisoned` for optional-only variables rather than dropping the row.
//!
//! # Correlated Optional Builder
//!
//! OPTIONAL clauses typically reference variables from the required (left) side.
//! To support this correlation, the optional side is built per-row using an
//! `OptionalBuilder` trait. This allows:
//! - Single triple patterns (via `PatternOptionalBuilder`)
//! - Multi-pattern OPTIONAL clauses with joins, filters, property-joins
//! - Arbitrary operator subtrees planned from `Vec<Pattern>`
//!
//! # Poison Binding Semantics
//!
//! A key feature of this implementation is `Binding::Poisoned`:
//! - When an OPTIONAL clause has no matches, variables that are unique to
//!   the optional side are marked as Poisoned (not Unbound)
//! - Poisoned bindings **block** future pattern matching - any pattern that
//!   uses a Poisoned variable yields no matches (not "match anything")
//! - This matches SPARQL OPTIONAL semantics where unbound optional vars
//!   prevent subsequent patterns from matching

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::try_normalize_pred_sid;
use crate::fast_path_common::{subject_probe_lane_plan, ProbeLanePlan, ProbeOps};
use crate::group_aggregate::{binding_to_group_key_normalized, GroupKeyOwned};
use crate::ir::triple::{Ref, Term, TriplePattern};
use crate::ir::Pattern;
use crate::join::{
    batched_subject_probe_binary, BindInstruction, PatternPosition, SubjectProbeParams,
    UnifyInstruction,
};
use crate::object_binding::{equality_norm, EqualityNorm};
use crate::operator::{
    compute_trimmed_vars, effective_schema, trim_batch, BoxedOperator, Operator, OperatorState,
};
use crate::seed::SeedOperator;
use crate::temporal_mode::PlanningContext;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::StatsView;
use lru::LruCache;
use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

/// Keep OPTIONAL diagnostics concise during perf captures by surfacing only
/// expensive batches or obvious cache/planning churn at debug level.
const OPTIONAL_DEBUG_MIN_WORK: usize = 8;
const OPTIONAL_DEBUG_MIN_MS: u64 = 25;

/// Per-row result of a batched optional build: `(row_index, batches)`.
pub type OptionalBatchRow = (usize, Vec<Batch>);
/// Builder for correlated optional operators
///
/// This trait encapsulates how to create an optional-side operator that is
/// correlated with the current required row. The builder receives the required
/// batch and row index, and returns an operator that will be executed for
/// that specific row's bindings.
///
/// # Implementations
///
/// - `PatternOptionalBuilder`: Simple single-pattern OPTIONAL (substitutes vars)
/// - Custom implementations can build complex operator subtrees (joins, filters, etc.)
///
/// # Correlation Semantics
///
/// The builder is responsible for "injecting" left-side bindings into the
/// optional operator. This typically means:
/// - Substituting bound vars into scan patterns
/// - Creating a seed `Values` operator with the left row
/// - Building a join chain that starts from the left bindings
#[async_trait]
pub trait OptionalBuilder: Send + Sync {
    /// Build an optional operator for the given required row
    ///
    /// # Arguments
    ///
    /// * `required_batch` - The batch containing the required row
    /// * `row` - Index of the row in the batch
    ///
    /// # Returns
    ///
    /// - `Ok(Some(op))` - A boxed operator that will find optional matches
    /// - `Ok(None)` - The required row has bindings that make the optional impossible
    ///   (e.g., Poisoned vars in correlation positions)
    /// - `Err(e)` - A planning/building error that should be propagated
    fn build(
        &self,
        required_batch: &Batch,
        row: usize,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<BoxedOperator>>;

    /// Optionally build *and execute* all remaining required rows in the current
    /// batch in one pass, returning each row's optional-side result batches.
    ///
    /// Builders override this to implement batched paths for hot correlated
    /// OPTIONAL shapes — either synchronous index probes (see
    /// `GroupedPatternOptionalBuilder`) or a single seeded subplan execution
    /// hash-partitioned across rows (see `PlanTreeOptionalBuilder`). The method
    /// is `async` so the latter can drive a real operator subtree.
    /// Default: no batched execution (operator falls back to per-row `build`).
    async fn build_batch(
        &self,
        _required_batch: &Batch,
        _start_row: usize,
        _ctx: &ExecutionContext<'_>,
    ) -> Result<Option<Vec<OptionalBatchRow>>> {
        Ok(None)
    }

    /// Optional cache key for correlated OPTIONAL evaluation.
    ///
    /// If this returns `Some(key)`, the OptionalOperator may memoize the optional-side
    /// results across required rows that share the same correlation bindings.
    ///
    /// Default: no caching.
    fn cache_key(
        &self,
        _required_batch: &Batch,
        _row: usize,
        _ctx: &ExecutionContext<'_>,
    ) -> Result<Option<Box<[u8]>>> {
        Ok(None)
    }

    /// Get the output schema of the optional operator
    ///
    /// This must be stable across all calls to `build()`.
    fn schema(&self) -> &[VarId];

    /// Get variables that are only in the optional side (not in required)
    fn optional_only_vars(&self) -> &[VarId];

    /// Get instructions for unification checks on shared vars
    fn unify_instructions(&self) -> &[UnifyInstruction];
}

/// Builder for single-pattern OPTIONAL
///
/// This is the simplest form of optional builder - it creates a `DatasetOperator`
/// for a single triple pattern, substituting left-side bindings into the pattern.
///
/// # Example
///
/// For `OPTIONAL { ?s :email ?email }` where `?s` is bound from the left:
/// - `build()` substitutes the left's `?s` value into the pattern
/// - Returns a `DatasetOperator` for `alice :email ?email` (when ?s = alice)
pub struct PatternOptionalBuilder {
    /// The triple pattern template
    pattern: TriplePattern,
    /// Output schema of the pattern
    pattern_schema: Arc<[VarId]>,
    /// Variables only in optional pattern (not in required)
    optional_only_vars: Vec<VarId>,
    /// Instructions for binding required values into pattern
    bind_instructions: Vec<BindInstruction>,
    /// Instructions for unification checks on shared vars
    unify_instructions: Vec<UnifyInstruction>,
    /// Planning context captured at planner-time for the per-row substituted scan.
    planning: PlanningContext,
}

impl PatternOptionalBuilder {
    /// Create a new pattern-based optional builder
    pub fn new(
        required_schema: Arc<[VarId]>,
        pattern: TriplePattern,
        planning: PlanningContext,
    ) -> Self {
        // Determine optional-only vars (in optional but not in required)
        let required_vars: std::collections::HashSet<_> = required_schema.iter().copied().collect();
        let pattern_vars = pattern.produced_vars();
        let optional_only_vars: Vec<_> = pattern_vars
            .iter()
            .filter(|v| !required_vars.contains(v))
            .copied()
            .collect();

        // Build pattern schema (all vars from pattern)
        let pattern_schema: Arc<[VarId]> = Arc::from(pattern_vars.into_boxed_slice());

        // Build bind instructions (how to substitute required values into pattern)
        let bind_instructions = Self::build_bind_instructions(&required_schema, &pattern);

        // Build unify instructions (shared vars that need equality checks)
        let unify_instructions =
            Self::build_unify_instructions(&required_schema, &pattern, &optional_only_vars);

        Self {
            pattern,
            pattern_schema,
            optional_only_vars,
            bind_instructions,
            unify_instructions,
            planning,
        }
    }

    /// Build bind instructions for substituting required values into pattern
    fn build_bind_instructions(
        required_schema: &[VarId],
        pattern: &TriplePattern,
    ) -> Vec<BindInstruction> {
        let mut instructions = Vec::new();

        for (position, r) in [
            (PatternPosition::Subject, &pattern.s),
            (PatternPosition::Predicate, &pattern.p),
        ] {
            if let Ref::Var(v) = r {
                if let Some(col) = required_schema.iter().position(|rv| rv == v) {
                    instructions.push(BindInstruction {
                        position,
                        left_col: col,
                    });
                }
            }
        }

        if let Term::Var(v) = &pattern.o {
            if let Some(col) = required_schema.iter().position(|rv| rv == v) {
                instructions.push(BindInstruction {
                    position: PatternPosition::Object,
                    left_col: col,
                });
            }
        }

        instructions
    }

    /// Build unify instructions for shared vars that need equality checks
    fn build_unify_instructions(
        required_schema: &[VarId],
        pattern: &TriplePattern,
        optional_only_vars: &[VarId],
    ) -> Vec<UnifyInstruction> {
        let pattern_vars = pattern.produced_vars();

        pattern_vars
            .iter()
            .filter(|var| !optional_only_vars.contains(var)) // Skip optional-only vars
            .filter_map(|pattern_var| {
                let req_col = required_schema.iter().position(|v| v == pattern_var)?;
                let opt_col = pattern_vars.iter().position(|v| v == pattern_var)?;
                Some(UnifyInstruction {
                    left_col: req_col,
                    right_col: opt_col,
                })
            })
            .collect()
    }

    /// Check if any binding used in bind instructions is Poisoned
    fn has_poisoned_binding(&self, required_batch: &Batch, row: usize) -> bool {
        self.bind_instructions
            .iter()
            .any(|instr| required_batch.get_by_col(row, instr.left_col).is_poisoned())
    }

    fn subject_left_col(&self) -> Option<usize> {
        self.bind_instructions
            .iter()
            .find(|instr| instr.position == PatternPosition::Subject)
            .map(|instr| instr.left_col)
    }

    fn emit_object_var(&self) -> Option<VarId> {
        match &self.pattern.o {
            Term::Var(v) if self.optional_only_vars.contains(v) => Some(*v),
            _ => None,
        }
    }

    fn resolve_subject_id(
        &self,
        required_batch: &Batch,
        row: usize,
        subject_left_col: usize,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<u64>> {
        let binding = required_batch.get_by_col(row, subject_left_col);
        let Some(store) = ctx.binary_store.as_deref() else {
            return Ok(None);
        };
        match binding {
            Binding::EncodedSid { s_id, .. } => Ok(Some(*s_id)),
            Binding::Sid { sid, .. } => {
                // Persisted reverse dict first, then DictNovelty — subjects
                // minted after the last index resolve to novelty s_ids, the
                // same id space the overlay ops are translated into.
                let persisted = store
                    .find_subject_id_by_parts(sid.namespace_code, &sid.name)
                    .map_err(|e| QueryError::execution(format!("find_subject_id_by_parts: {e}")))?;
                Ok(persisted.or_else(|| {
                    ctx.dict_novelty
                        .as_ref()
                        .filter(|dn| dn.is_initialized())
                        .and_then(|dn| dn.subjects.find_subject(sid.namespace_code, &sid.name))
                }))
            }
            _ => Ok(None),
        }
    }

    /// Substitute required bindings into pattern
    ///
    /// For IriMatch bindings in subject/predicate positions, uses `Ref::Iri` to carry
    /// the canonical IRI. For IriMatch bindings in object position, uses `Term::Iri`.
    /// The scan operator will encode this IRI for each target ledger's namespace
    /// table, enabling correct cross-ledger OPTIONAL matching.
    fn substitute_pattern(
        &self,
        required_batch: &Batch,
        row: usize,
        ctx: &ExecutionContext<'_>,
    ) -> Result<TriplePattern> {
        let mut pattern = self.pattern.clone();

        for instr in &self.bind_instructions {
            let binding = required_batch.get_by_col(row, instr.left_col);

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
                            // Late materialized subject ID: resolve to IRI for correlation.
                            // Uses novelty-aware BinaryGraphView via ctx.graph_view().
                            let gv = ctx.graph_view().ok_or_else(|| {
                                QueryError::Internal(
                                    "OPTIONAL correlation requires binary store for EncodedSid"
                                        .into(),
                                )
                            })?;
                            let iri = gv.resolve_subject_iri(*s_id).map_err(|e| {
                                QueryError::Internal(format!("resolve subject iri: {e}"))
                            })?;
                            pattern.s = Ref::Iri(Arc::<str>::from(iri));
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
                            // Use Ref::Iri so scan can encode for each target ledger
                            pattern.p = Ref::Iri(iri.clone());
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
                        Binding::EncodedLit { .. } => {
                            // Late materialized literal: no decode context here; leave unbound.
                        }
                        Binding::EncodedSid { .. } | Binding::EncodedPid { .. } => {
                            // Late materialized IRI: no decode context here; leave unbound.
                        }
                        Binding::Unbound | Binding::Poisoned => {
                            // Leave as variable
                        }
                        Binding::Grouped(_) => {
                            debug_assert!(
                                false,
                                "Grouped binding in optional pattern substitution"
                            );
                            // Leave as variable
                        }
                        Binding::Path { .. }
                        | Binding::Rel(_)
                        | Binding::List(_)
                        | Binding::Map(_) => {
                            // A path/list value is never substituted into a triple slot.
                        }
                    }
                }
            }
        }

        Ok(pattern)
    }
}

#[async_trait]
impl OptionalBuilder for PatternOptionalBuilder {
    fn build(
        &self,
        required_batch: &Batch,
        row: usize,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<BoxedOperator>> {
        // Check for poisoned bindings - if any correlation var is poisoned,
        // the optional cannot match
        if self.has_poisoned_binding(required_batch, row) {
            return Ok(None);
        }

        // Substitute bindings into pattern and create scan operator
        let bound_pattern = self.substitute_pattern(required_batch, row, ctx)?;
        Ok(Some(Box::new(
            crate::dataset_operator::DatasetOperator::scan(
                bound_pattern,
                None,
                Vec::new(),
                crate::binary_scan::EmitMask::ALL,
                None,
                self.planning.mode(),
            ),
        )))
    }

    async fn build_batch(
        &self,
        required_batch: &Batch,
        start_row: usize,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<Vec<OptionalBatchRow>>> {
        if start_row >= required_batch.len() || ctx.is_multi_ledger() {
            return Ok(None);
        }
        let Some(store) = ctx.binary_store.as_ref() else {
            return Ok(None);
        };
        let Some(subject_left_col) = self.subject_left_col() else {
            return Ok(None);
        };
        let Some(pred_sid) = try_normalize_pred_sid(store, &self.pattern.p) else {
            return Ok(None);
        };
        if self.pattern.dtc.is_some() {
            return Ok(None);
        }
        // Novelty merges per probed subject; unmergeable overlays fall back
        // to the per-row scan (whose cursor merges the overlay).
        let lane_plan = subject_probe_lane_plan(ctx, store, &pred_sid)?;
        if matches!(lane_plan, ProbeLanePlan::Decline) {
            return Ok(None);
        }

        let emit_object_var = self.emit_object_var();
        let mut row_slots: Vec<Option<Vec<Binding>>> =
            vec![None; required_batch.len().saturating_sub(start_row)];
        let mut subject_rows: std::collections::HashMap<u64, Vec<usize>> =
            std::collections::HashMap::new();
        let mut subject_ids: Vec<u64> = Vec::new();

        for row in start_row..required_batch.len() {
            let slot = row - start_row;
            if self.has_poisoned_binding(required_batch, row) {
                continue;
            }
            let Some(s_id) = self.resolve_subject_id(required_batch, row, subject_left_col, ctx)?
            else {
                return Ok(None);
            };
            subject_rows.entry(s_id).or_insert_with(|| {
                subject_ids.push(s_id);
                Vec::new()
            });
            subject_rows
                .get_mut(&s_id)
                .expect("entry inserted")
                .push(slot);
        }

        // One reconciler per probe call; the dict overlay decodes
        // novelty-minted object values on injected asserts.
        let mut probe_ops = match &lane_plan {
            ProbeLanePlan::Merge(ops) => ProbeOps::new(ops.clone()),
            _ => None,
        };
        let dict_overlay = crate::join::make_dict_overlay(ctx, store);
        let probe_matches = batched_subject_probe_binary(
            ctx,
            store,
            &SubjectProbeParams {
                pred_sid: &pred_sid,
                subject_ids: &subject_ids,
                object_bounds: None,
                bound_object: (!matches!(&self.pattern.o, Term::Var(_))).then_some(&self.pattern.o),
                emit_object: emit_object_var.is_some(),
                dict_overlay: dict_overlay.as_ref(),
            },
            probe_ops.as_mut(),
        )?;

        for probe_match in probe_matches {
            let Some(slots) = subject_rows.get(&probe_match.subject_id) else {
                continue;
            };
            for &slot in slots {
                let values = row_slots[slot].get_or_insert_with(Vec::new);
                if let Some(object) = &probe_match.object {
                    values.push(object.clone());
                } else {
                    values.push(Binding::Unbound);
                }
            }
        }

        let mut pending = Vec::with_capacity(row_slots.len());
        for (slot, maybe_values) in row_slots.into_iter().enumerate() {
            let optional_batches = match maybe_values {
                Some(values) if !values.is_empty() => {
                    if let Some(object_var) = emit_object_var {
                        vec![Batch::new(
                            Arc::from(vec![object_var].into_boxed_slice()),
                            vec![values],
                        )?]
                    } else {
                        vec![Batch::empty_schema_with_len(values.len())]
                    }
                }
                Some(_) | None => Vec::new(),
            };
            pending.push((start_row + slot, optional_batches));
        }

        tracing::debug!(rows = pending.len(), "optional batched probe complete");
        Ok(Some(pending))
    }

    fn cache_key(
        &self,
        required_batch: &Batch,
        row: usize,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<Box<[u8]>>> {
        // Key on the substituted correlation bindings only.
        // For the common case `OPTIONAL { ?s <p> ?o }` with `?s` coming from the left,
        // this makes repeated `?s` values (fan-out on the left) reuse right-side results.
        if self.has_poisoned_binding(required_batch, row) {
            return Ok(None);
        }

        // Today we support cache keys for subjects that are either already encoded
        // or can be resolved to an IRI string without ambiguity.
        // (Multi-ledger mode can still work without caching.)
        for instr in &self.bind_instructions {
            if instr.position != PatternPosition::Subject {
                continue;
            }
            let binding = required_batch.get_by_col(row, instr.left_col);
            return match binding {
                Binding::EncodedSid { s_id, .. } => {
                    let mut v = Vec::with_capacity(1 + 8);
                    v.push(b'S');
                    v.extend_from_slice(&s_id.to_le_bytes());
                    Ok(Some(v.into_boxed_slice()))
                }
                Binding::Sid { sid, .. } => {
                    // Fallback stable key: namespace code + suffix bytes.
                    let mut v = Vec::with_capacity(1 + 2 + sid.name_str().len());
                    v.push(b's');
                    v.extend_from_slice(&sid.namespace_code.to_le_bytes());
                    v.extend_from_slice(sid.name_str().as_bytes());
                    Ok(Some(v.into_boxed_slice()))
                }
                Binding::IriMatch { iri, .. } | Binding::Iri(iri) => {
                    let mut v = Vec::with_capacity(1 + iri.len());
                    v.push(b'i');
                    v.extend_from_slice(iri.as_bytes());
                    Ok(Some(v.into_boxed_slice()))
                }
                Binding::Unbound | Binding::Poisoned => Ok(None),
                Binding::EncodedPid { .. } | Binding::EncodedLit { .. } | Binding::Lit { .. } => {
                    Ok(None)
                }
                Binding::Grouped(_)
                | Binding::Path { .. }
                | Binding::Rel(_)
                | Binding::List(_)
                | Binding::Map(_) => Ok(None),
            };
        }

        // No subject correlation => don't cache.
        let _ = ctx;
        Ok(None)
    }

    fn schema(&self) -> &[VarId] {
        &self.pattern_schema
    }

    fn optional_only_vars(&self) -> &[VarId] {
        &self.optional_only_vars
    }

    fn unify_instructions(&self) -> &[UnifyInstruction] {
        &self.unify_instructions
    }
}

/// Builder for a grouped chain of independent single-triple OPTIONALs that all
/// correlate on the same already-bound subject variable.
///
/// This preserves normal OPTIONAL semantics while allowing the optional side to
/// probe all predicates for a batch of required rows at once.
pub struct GroupedPatternOptionalBuilder {
    required_schema: Arc<[VarId]>,
    triples: Vec<TriplePattern>,
    optional_only_vars: Vec<VarId>,
    subject_left_col: usize,
    /// Planning context captured at planner-time for the per-row chain.
    planning: PlanningContext,
}

impl GroupedPatternOptionalBuilder {
    pub fn new(
        required_schema: Arc<[VarId]>,
        triples: Vec<TriplePattern>,
        planning: PlanningContext,
    ) -> Result<Self> {
        let Some(subject_var) = triples.first().and_then(|tp| tp.s.as_var()) else {
            return Err(QueryError::Internal(
                "grouped optional builder requires variable subject".into(),
            ));
        };
        let Some(subject_left_col) = required_schema.iter().position(|v| *v == subject_var) else {
            return Err(QueryError::Internal(
                "grouped optional builder requires subject bound from required schema".into(),
            ));
        };

        let mut optional_only_vars = Vec::with_capacity(triples.len());
        let mut seen = HashSet::new();
        for triple in &triples {
            let Some(obj_var) = triple.o.as_var() else {
                return Err(QueryError::Internal(
                    "grouped optional builder requires variable objects".into(),
                ));
            };
            if required_schema.contains(&obj_var) || !seen.insert(obj_var) {
                return Err(QueryError::Internal(
                    "grouped optional builder requires distinct optional-only object vars".into(),
                ));
            }
            optional_only_vars.push(obj_var);
        }

        Ok(Self {
            required_schema,
            triples,
            optional_only_vars,
            subject_left_col,
            planning,
        })
    }

    fn has_poisoned_subject(&self, required_batch: &Batch, row: usize) -> bool {
        required_batch
            .get_by_col(row, self.subject_left_col)
            .is_poisoned()
    }

    fn resolve_subject_id(
        &self,
        required_batch: &Batch,
        row: usize,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<u64>> {
        let binding = required_batch.get_by_col(row, self.subject_left_col);
        let Some(store) = ctx.binary_store.as_deref() else {
            return Ok(None);
        };
        match binding {
            Binding::EncodedSid { s_id, .. } => Ok(Some(*s_id)),
            Binding::Sid { sid, .. } => {
                // Persisted reverse dict first, then DictNovelty — subjects
                // minted after the last index resolve to novelty s_ids, the
                // same id space the overlay ops are translated into.
                let persisted = store
                    .find_subject_id_by_parts(sid.namespace_code, &sid.name)
                    .map_err(|e| QueryError::execution(format!("find_subject_id_by_parts: {e}")))?;
                Ok(persisted.or_else(|| {
                    ctx.dict_novelty
                        .as_ref()
                        .filter(|dn| dn.is_initialized())
                        .and_then(|dn| dn.subjects.find_subject(sid.namespace_code, &sid.name))
                }))
            }
            _ => Ok(None),
        }
    }

    fn grouped_schema(&self) -> Arc<[VarId]> {
        Arc::from(self.optional_only_vars.clone().into_boxed_slice())
    }

    fn build_fallback_chain(
        &self,
        required_batch: &Batch,
        row: usize,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<BoxedOperator>> {
        if self.has_poisoned_subject(required_batch, row) {
            return Ok(None);
        }

        let mut op: BoxedOperator = Box::new(SeedOperator::from_batch_row(required_batch, row));
        let mut current_schema: Arc<[VarId]> = self.required_schema.clone();
        for triple in &self.triples {
            op = Box::new(OptionalOperator::new(
                op,
                current_schema.clone(),
                triple.clone(),
                self.planning,
            ));
            current_schema = Arc::from(op.schema().to_vec().into_boxed_slice());
        }

        let _ = ctx;
        Ok(Some(op))
    }

    fn generate_rows(values_per_pred: &[Vec<Binding>]) -> Vec<Vec<Binding>> {
        if values_per_pred.is_empty() {
            return vec![Vec::new()];
        }

        let total: usize = values_per_pred.iter().fold(1usize, |acc, values| {
            acc.saturating_mul(values.len().max(1))
        });
        let mut rows = Vec::with_capacity(total);
        let mut indices = vec![0usize; values_per_pred.len()];

        loop {
            let mut row = Vec::with_capacity(values_per_pred.len());
            for (pred_idx, values) in values_per_pred.iter().enumerate() {
                if values.is_empty() {
                    row.push(Binding::Poisoned);
                } else {
                    row.push(values[indices[pred_idx]].clone());
                }
            }
            rows.push(row);

            let mut carry = true;
            for i in (0..indices.len()).rev() {
                if !carry {
                    break;
                }
                let width = values_per_pred[i].len().max(1);
                indices[i] += 1;
                if indices[i] >= width {
                    indices[i] = 0;
                } else {
                    carry = false;
                }
            }
            if carry {
                break;
            }
        }

        rows
    }
}

#[async_trait]
impl OptionalBuilder for GroupedPatternOptionalBuilder {
    fn build(
        &self,
        required_batch: &Batch,
        row: usize,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<BoxedOperator>> {
        self.build_fallback_chain(required_batch, row, ctx)
    }

    async fn build_batch(
        &self,
        required_batch: &Batch,
        start_row: usize,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<Vec<OptionalBatchRow>>> {
        if start_row >= required_batch.len() || ctx.is_multi_ledger() {
            tracing::debug!(
                predicate_count = self.triples.len(),
                start_row,
                reason = if start_row >= required_batch.len() {
                    "start-row-exhausted"
                } else {
                    "multi-ledger"
                },
                "grouped optional builder fallback"
            );
            return Ok(None);
        }
        let Some(store) = ctx.binary_store.as_ref() else {
            tracing::debug!(
                predicate_count = self.triples.len(),
                start_row,
                reason = "no-binary-store",
                "grouped optional builder fallback"
            );
            return Ok(None);
        };
        if self.triples.iter().any(|tp| tp.dtc.is_some()) {
            tracing::debug!(
                predicate_count = self.triples.len(),
                start_row,
                reason = "datatype-constraint",
                "grouped optional builder fallback"
            );
            return Ok(None);
        }

        let mut subject_ids = Vec::new();
        let mut row_subject_slots: Vec<Option<u64>> =
            Vec::with_capacity(required_batch.len().saturating_sub(start_row));
        let mut subject_rows: HashMap<u64, Vec<usize>> = HashMap::new();

        for row in start_row..required_batch.len() {
            let slot = row - start_row;
            if self.has_poisoned_subject(required_batch, row) {
                row_subject_slots.push(None);
                continue;
            }
            let Some(s_id) = self.resolve_subject_id(required_batch, row, ctx)? else {
                tracing::debug!(
                    predicate_count = self.triples.len(),
                    start_row,
                    row,
                    reason = "unresolved-subject-id",
                    "grouped optional builder fallback"
                );
                return Ok(None);
            };
            row_subject_slots.push(Some(s_id));
            subject_rows.entry(s_id).or_insert_with(|| {
                subject_ids.push(s_id);
                Vec::new()
            });
            subject_rows
                .get_mut(&s_id)
                .expect("entry inserted")
                .push(slot);
        }

        let dict_overlay = crate::join::make_dict_overlay(ctx, store);
        let mut row_values: Vec<Vec<Vec<Binding>>> =
            vec![vec![Vec::new(); self.triples.len()]; row_subject_slots.len()];
        for (pred_idx, triple) in self.triples.iter().enumerate() {
            let Some(pred_sid) = try_normalize_pred_sid(store, &triple.p) else {
                tracing::debug!(
                    predicate_count = self.triples.len(),
                    start_row,
                    pred_idx,
                    reason = "unbound-predicate",
                    "grouped optional builder fallback"
                );
                return Ok(None);
            };
            let lane_plan = subject_probe_lane_plan(ctx, store, &pred_sid)?;
            if matches!(lane_plan, ProbeLanePlan::Decline) {
                tracing::debug!(
                    predicate_count = self.triples.len(),
                    start_row,
                    pred_idx,
                    reason = "overlay-unmergeable",
                    "grouped optional builder fallback"
                );
                return Ok(None);
            }
            let mut probe_ops = match &lane_plan {
                ProbeLanePlan::Merge(ops) => ProbeOps::new(ops.clone()),
                _ => None,
            };
            let probe_matches = batched_subject_probe_binary(
                ctx,
                store,
                &SubjectProbeParams {
                    pred_sid: &pred_sid,
                    subject_ids: &subject_ids,
                    object_bounds: None,
                    bound_object: None,
                    emit_object: true,
                    dict_overlay: dict_overlay.as_ref(),
                },
                probe_ops.as_mut(),
            )?;
            for probe_match in probe_matches {
                let Some(slots) = subject_rows.get(&probe_match.subject_id) else {
                    continue;
                };
                for &slot in slots {
                    if let Some(object) = &probe_match.object {
                        row_values[slot][pred_idx].push(object.clone());
                    }
                }
            }
        }

        let schema = self.grouped_schema();
        let mut pending = Vec::with_capacity(row_values.len());
        for (slot, values_per_pred) in row_values.into_iter().enumerate() {
            let rows = Self::generate_rows(&values_per_pred);
            let optional_batches = if rows.is_empty() {
                Vec::new()
            } else {
                let mut columns: Vec<Vec<Binding>> = (0..self.optional_only_vars.len())
                    .map(|_| Vec::with_capacity(rows.len()))
                    .collect();
                for row in rows {
                    for (col_idx, value) in row.into_iter().enumerate() {
                        columns[col_idx].push(value);
                    }
                }
                vec![Batch::new(schema.clone(), columns)?]
            };
            pending.push((start_row + slot, optional_batches));
        }

        tracing::debug!(
            rows = pending.len(),
            predicate_count = self.triples.len(),
            "grouped optional batched probe complete"
        );
        Ok(Some(pending))
    }

    fn cache_key(
        &self,
        required_batch: &Batch,
        row: usize,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<Box<[u8]>>> {
        if self.has_poisoned_subject(required_batch, row) {
            return Ok(None);
        }
        let binding = required_batch.get_by_col(row, self.subject_left_col);
        let _ = ctx;
        match binding {
            Binding::EncodedSid { s_id, .. } => {
                let mut v = Vec::with_capacity(1 + 8);
                v.push(b'S');
                v.extend_from_slice(&s_id.to_le_bytes());
                Ok(Some(v.into_boxed_slice()))
            }
            Binding::Sid { sid, .. } => {
                let mut v = Vec::with_capacity(1 + 2 + sid.name_str().len());
                v.push(b's');
                v.extend_from_slice(&sid.namespace_code.to_le_bytes());
                v.extend_from_slice(sid.name_str().as_bytes());
                Ok(Some(v.into_boxed_slice()))
            }
            Binding::IriMatch { iri, .. } | Binding::Iri(iri) => {
                let mut v = Vec::with_capacity(1 + iri.len());
                v.push(b'i');
                v.extend_from_slice(iri.as_bytes());
                Ok(Some(v.into_boxed_slice()))
            }
            _ => Ok(None),
        }
    }

    fn schema(&self) -> &[VarId] {
        &self.optional_only_vars
    }

    fn optional_only_vars(&self) -> &[VarId] {
        &self.optional_only_vars
    }

    fn unify_instructions(&self) -> &[UnifyInstruction] {
        &[]
    }
}

/// Builder for multi-pattern OPTIONAL clauses
///
/// This builder supports OPTIONAL clauses containing multiple patterns including:
/// - Multiple triple patterns
/// - FILTER expressions
/// - VALUES clauses
/// - BIND expressions
/// - Nested OPTIONAL/UNION/MINUS/EXISTS
/// - Subqueries
/// - Property paths
///
/// For each required row, it creates a `SeedOperator` with the row's bindings
/// and builds a full operator tree via `build_where_operators_seeded`.
///
/// # Example
///
/// For `OPTIONAL { ?s :age ?age . FILTER(?age > 18) }` where `?s` is bound from left:
/// - `build()` creates a seed with the left's `?s` value
/// - Builds an operator tree for the age triple + filter
/// - Returns the complete operator chain
pub struct PlanTreeOptionalBuilder {
    /// The inner patterns to execute
    inner_patterns: Vec<Pattern>,
    /// All variables in the optional patterns (computed schema)
    optional_schema: Arc<[VarId]>,
    /// Variables only in optional (not in required)
    optional_only_vars: Vec<VarId>,
    /// Shared variables that need unification checks
    unify_instructions: Vec<UnifyInstruction>,
    /// Indices of shared variables in the required schema (for poisoned check)
    shared_var_indices: Vec<usize>,
    /// Stats for nested query optimization
    stats: Option<Arc<StatsView>>,
    /// Planning context captured at planner-time for the per-row inner subplan.
    planning: PlanningContext,
}

impl PlanTreeOptionalBuilder {
    /// Create a new plan-tree optional builder
    ///
    /// # Arguments
    ///
    /// * `required_schema` - Schema of the required (left) operator
    /// * `inner_patterns` - Patterns inside the OPTIONAL clause
    /// * `stats` - Optional stats for selectivity-based pattern reordering
    pub fn new(
        required_schema: Arc<[VarId]>,
        inner_patterns: Vec<Pattern>,
        stats: Option<Arc<StatsView>>,
        planning: PlanningContext,
    ) -> Self {
        let required_vars: HashSet<VarId> = required_schema.iter().copied().collect();

        // Collect all variables from inner patterns (deduped, preserving order)
        let mut optional_vars: Vec<VarId> = Vec::new();
        let mut seen: HashSet<VarId> = HashSet::new();
        for p in &inner_patterns {
            for v in p.produced_vars() {
                if seen.insert(v) {
                    optional_vars.push(v);
                }
            }
        }

        // Determine optional-only vars (in optional but not in required)
        let optional_only_vars: Vec<VarId> = optional_vars
            .iter()
            .filter(|v| !required_vars.contains(v))
            .copied()
            .collect();

        // Shared vars = in both required and optional
        let shared_vars: Vec<VarId> = optional_vars
            .iter()
            .filter(|v| required_vars.contains(v))
            .copied()
            .collect();

        // Build unify instructions for shared vars
        let unify_instructions: Vec<UnifyInstruction> = shared_vars
            .iter()
            .filter_map(|var| {
                let req_col = required_schema.iter().position(|v| v == var)?;
                let opt_col = optional_vars.iter().position(|v| v == var)?;
                Some(UnifyInstruction {
                    left_col: req_col,
                    right_col: opt_col,
                })
            })
            .collect();

        // Track which required columns are shared (for poisoned check)
        let shared_var_indices: Vec<usize> = shared_vars
            .iter()
            .filter_map(|var| required_schema.iter().position(|v| v == var))
            .collect();

        let optional_schema: Arc<[VarId]> = Arc::from(optional_vars.into_boxed_slice());

        Self {
            inner_patterns,
            optional_schema,
            optional_only_vars,
            unify_instructions,
            shared_var_indices,
            stats,
            planning,
        }
    }

    /// Check if any shared variable binding is Poisoned
    fn has_poisoned_shared_var(&self, required_batch: &Batch, row: usize) -> bool {
        self.shared_var_indices
            .iter()
            .any(|&col| required_batch.get_by_col(row, col).is_poisoned())
    }
}

#[async_trait]
impl OptionalBuilder for PlanTreeOptionalBuilder {
    fn build(
        &self,
        required_batch: &Batch,
        row: usize,
        _ctx: &ExecutionContext<'_>,
    ) -> Result<Option<BoxedOperator>> {
        // Check for poisoned bindings - if any shared var is poisoned,
        // the optional cannot match
        if self.has_poisoned_shared_var(required_batch, row) {
            return Ok(None);
        }

        // Create a seed operator from the required row
        let seed = SeedOperator::from_batch_row(required_batch, row);

        tracing::trace!(
            required_schema_cols = required_batch.schema().len(),
            optional_pattern_count = self.inner_patterns.len(),
            optional_only_vars = self.optional_only_vars.len(),
            "planning correlated optional with seeded row"
        );

        // Build the operator tree using build_where_operators_seeded
        // Propagate errors - planning failures should not be silently swallowed
        let op = crate::execute::build_where_operators_seeded(
            Some(Box::new(seed)),
            &self.inner_patterns,
            self.stats.clone(),
            None,
            &self.planning,
        )?;

        Ok(Some(op))
    }

    /// Batched correlated OPTIONAL as a hash left-join.
    ///
    /// Instead of rebuilding and re-executing the inner subplan once per
    /// required row, seed it ONCE with the distinct correlation tuples of the
    /// whole batch, execute it once, then hash-partition the results back to
    /// each row by correlation key. This collapses the per-driving-row subplan
    /// rebuild (the LDBC IC5 cliff) into a single inner scan.
    ///
    /// Soundness: the inner solutions for a required row depend on the row only
    /// through its shared (correlation) variables — the sole overlap with the
    /// inner patterns — so partitioning the single execution by those variables
    /// reproduces the per-row results exactly. Gated to inner shapes whose
    /// per-seed evaluation is a pure restriction by the correlation tuple
    /// (no internal LIMIT / independent correlation / row-multiplying subquery).
    /// Any unmet gate returns `Ok(None)`, deferring to the per-row `build` path.
    ///
    /// Trade-offs (deliberate, and why `FLUREE_OPTIONAL_HASH_JOIN=0` exists):
    /// - The whole inner result is MATERIALIZED into `buckets`/`key_batches`
    ///   before any row is emitted — peak memory is the inner output size, not
    ///   the streaming O(1) of the per-row path. Bounded in practice by the gates
    ///   plus single-ledger correlation, but unbounded inner outputs are why this
    ///   is a kill-switchable fast path, not the default for every shape.
    /// - The inner is rebuilt and re-executed once PER required BATCH (not once
    ///   globally): there is no cross-batch seed dedup, so a correlation tuple
    ///   spanning N required batches drives the inner N times. The win is still
    ///   decisive vs. per-ROW rebuild; a global hash join would need a different
    ///   operator boundary.
    /// - Leaving an object-only correlation var unbound (see
    ///   [`corr_var_only_triple_object`]) can make the seeded inner read MORE
    ///   than the per-row path when that var is the selective one — the bet is
    ///   that the scattered object-probe it replaces is the dominant cost.
    async fn build_batch(
        &self,
        required_batch: &Batch,
        start_row: usize,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<Vec<OptionalBatchRow>>> {
        if start_row >= required_batch.len()
            || optional_hash_join_disabled()
            || ctx.is_multi_ledger()
            || !self
                .inner_patterns
                .iter()
                .all(inner_pattern_is_hash_join_safe)
        {
            return Ok(None);
        }

        // Correlation columns = required columns whose var is referenced anywhere
        // inside the inner patterns (join keys, filter operands, path endpoints).
        let req_schema = required_batch.schema();
        let referenced: HashSet<VarId> = self
            .inner_patterns
            .iter()
            .flat_map(Pattern::referenced_vars)
            .collect();
        let corr_cols: Vec<usize> = req_schema
            .iter()
            .enumerate()
            .filter(|(_, v)| referenced.contains(v))
            .map(|(c, _)| c)
            .collect();
        if corr_cols.is_empty() {
            // Uncorrelated OPTIONAL: leave to the per-row path (rare).
            return Ok(None);
        }
        let corr_vars: Vec<VarId> = corr_cols.iter().map(|&c| req_schema[c]).collect();

        // Seed only the correlation vars that drive the inner subject-first.
        // A correlation var that appears in the inner SOLELY as a Triple object
        // is left UNBOUND: the inner then binds it from the subject side (a fast
        // PSOT scan) instead of object-probing the seeded value through the
        // scattered global object index — the hash-join drive that fixes the
        // LDBC IC5 timeout. Correctness is preserved by partitioning on the full
        // correlation key, so the inner-produced binding is matched back to the
        // required row's value. Vars used anywhere else (subject/predicate/
        // filter/path) must be seeded to keep the inner correct.
        let unbound_corr: HashSet<VarId> = corr_vars
            .iter()
            .copied()
            .filter(|v| corr_var_only_triple_object(*v, &self.inner_patterns))
            .collect();
        let seed_cols: Vec<usize> = corr_cols
            .iter()
            .copied()
            .filter(|&c| !unbound_corr.contains(&req_schema[c]))
            .collect();
        if seed_cols.is_empty() {
            // No subject-drivable correlation: seeding nothing would run the
            // inner uncorrelated (possible explosion). Defer to the per-row path.
            return Ok(None);
        }
        let seed_vars: Vec<VarId> = seed_cols.iter().map(|&c| req_schema[c]).collect();

        let norm = equality_norm(ctx);
        let (store, gv) = EqualityNorm::parts(&norm);

        // Per row: full correlation key (for matching) + distinct seed tuple
        // over the seeded subset. A poisoned/unbound correlation var can never
        // match -> no-match row.
        let n = required_batch.len();
        let mut row_keys: Vec<Option<Vec<GroupKeyOwned>>> = Vec::with_capacity(n - start_row);
        let mut seed_rows: Vec<Vec<Binding>> = Vec::new();
        let mut seen_seed: HashSet<Vec<GroupKeyOwned>> = HashSet::new();
        for row in start_row..n {
            let unmatchable = corr_cols.iter().any(|&c| {
                let b = required_batch.get_by_col(row, c);
                b.is_poisoned() || matches!(b, Binding::Unbound)
            });
            if unmatchable {
                row_keys.push(None);
                continue;
            }
            let key: Vec<GroupKeyOwned> = corr_cols
                .iter()
                .map(|&c| {
                    binding_to_group_key_normalized(required_batch.get_by_col(row, c), store, gv)
                })
                .collect();
            let seed_key: Vec<GroupKeyOwned> = seed_cols
                .iter()
                .map(|&c| {
                    binding_to_group_key_normalized(required_batch.get_by_col(row, c), store, gv)
                })
                .collect();
            if seen_seed.insert(seed_key) {
                seed_rows.push(
                    seed_cols
                        .iter()
                        .map(|&c| required_batch.get_by_col(row, c).clone())
                        .collect(),
                );
            }
            row_keys.push(Some(key));
        }

        if seed_rows.is_empty() {
            // Every row had an unmatchable correlation var.
            return Ok(Some((start_row..n).map(|row| (row, Vec::new())).collect()));
        }

        // Build the inner subplan ONCE seeded by the distinct subject-driving
        // tuples; object-only correlation vars stay unbound so the inner
        // produces them subject-first.
        let seed_schema: Arc<[VarId]> = Arc::from(seed_vars.clone().into_boxed_slice());
        let seed = MaterializedSeedOperator::new(seed_schema, seed_rows, ctx.batch_size)?;
        let mut inner = crate::execute::build_where_operators_seeded(
            Some(Box::new(seed)),
            &self.inner_patterns,
            self.stats.clone(),
            None,
            &self.planning,
        )?;

        // The inner output must expose every correlation var (for partitioning)
        // and every optional-only var (for combine). If emission pruning dropped
        // one, fall back to the per-row path rather than mis-partition.
        let inner_schema = inner.schema().to_vec();
        let mut out_corr_cols: Vec<usize> = Vec::with_capacity(corr_vars.len());
        for v in &corr_vars {
            match inner_schema.iter().position(|x| x == v) {
                Some(c) => out_corr_cols.push(c),
                None => return Ok(None),
            }
        }
        let mut out_opt_cols: Vec<usize> = Vec::with_capacity(self.optional_only_vars.len());
        for v in &self.optional_only_vars {
            match inner_schema.iter().position(|x| x == v) {
                Some(c) => out_opt_cols.push(c),
                None => return Ok(None),
            }
        }
        // Optional-side batches carry ONLY the optional-only vars: correlation
        // equality is already enforced by the partition, so the operator's
        // per-row unify check is intentionally a no-op here (shared vars absent).
        let opt_schema: Arc<[VarId]> =
            Arc::from(self.optional_only_vars.clone().into_boxed_slice());

        // Execute once; hash-partition output rows by the full correlation key.
        inner.open(ctx).await?;
        let mut buckets: HashMap<Vec<GroupKeyOwned>, Vec<Vec<Binding>>> = HashMap::new();
        while let Some(batch) = inner.next_batch(ctx).await? {
            ctx.check_cancelled()?;
            for r in 0..batch.len() {
                let key: Vec<GroupKeyOwned> = out_corr_cols
                    .iter()
                    .map(|&c| binding_to_group_key_normalized(batch.get_by_col(r, c), store, gv))
                    .collect();
                let projected: Vec<Binding> = out_opt_cols
                    .iter()
                    .map(|&c| batch.get_by_col(r, c).clone())
                    .collect();
                buckets.entry(key).or_default().push(projected);
            }
        }
        inner.close();

        // One result Batch per correlation key (optional-only columns only),
        // then assigned to each required row that shares the key.
        let mut key_batches: HashMap<Vec<GroupKeyOwned>, Batch> = HashMap::new();
        for (key, rows) in buckets {
            let batch = if opt_schema.is_empty() {
                Batch::empty_schema_with_len(rows.len())
            } else {
                let mut columns: Vec<Vec<Binding>> = (0..opt_schema.len())
                    .map(|_| Vec::with_capacity(rows.len()))
                    .collect();
                for projected in rows {
                    for (c, b) in projected.into_iter().enumerate() {
                        columns[c].push(b);
                    }
                }
                Batch::new(opt_schema.clone(), columns)?
            };
            key_batches.insert(key, batch);
        }

        let mut pending: Vec<OptionalBatchRow> = Vec::with_capacity(n - start_row);
        for (offset, row) in (start_row..n).enumerate() {
            let batches = match &row_keys[offset] {
                Some(key) => key_batches
                    .get(key)
                    .map(|b| vec![b.clone()])
                    .unwrap_or_default(),
                None => Vec::new(),
            };
            pending.push((row, batches));
        }
        tracing::debug!(
            seed_tuples = seen_seed.len(),
            seed_vars = seed_vars.len(),
            unbound_corr = unbound_corr.len(),
            inner_keys = key_batches.len(),
            rows = pending.len(),
            "optional batched hash-join complete"
        );
        Ok(Some(pending))
    }

    fn schema(&self) -> &[VarId] {
        &self.optional_schema
    }

    fn optional_only_vars(&self) -> &[VarId] {
        &self.optional_only_vars
    }

    fn unify_instructions(&self) -> &[UnifyInstruction] {
        &self.unify_instructions
    }
}

/// Inner-pattern shapes whose per-row OPTIONAL evaluation is exactly a
/// restriction by the correlation tuple — safe to evaluate once over all
/// distinct tuples and hash-partition. Triples, row-local filters, and
/// property paths from a (possibly seeded) endpoint qualify; subqueries,
/// nested OPTIONAL/UNION/MINUS, BIND/UNWIND/VALUES, and search patterns do
/// not (they can carry internal limits or independent correlation).
///
/// PR-4b: a subject-driven R2RML LEAF scan (a scalar POM or a RefObjectMap that
/// binds one object from the correlation subject) is a pure restriction by that
/// subject — exactly a `Pattern::Triple` in R2RML clothing — so it is admitted
/// too, behind `FLUREE_R2RML_BATCHED_OPTIONAL`. This lets a correlated OPTIONAL
/// over an R2RML source take the batched hash-left-join instead of the per-row
/// operator rebuild (`optional.rs::build_correlated_optional_op`), which no
/// operator-scoped cache can span (design: `07-pr4b-batched-optional.md`).
/// PR-4b admits the subject-driven single-object leaf
/// (`r2rml_leaf_is_hash_join_safe`); PR-4c widens to the same-subject STAR
/// (`r2rml_star_is_hash_join_safe`, its own sub-switch). type-var / wildcard /
/// bound-subject shapes stay EXCLUDED pending their own differential evidence.
fn inner_pattern_is_hash_join_safe(p: &Pattern) -> bool {
    match p {
        Pattern::Triple(_) | Pattern::Filter(_) | Pattern::PropertyPath(_) => true,
        Pattern::R2rml(rp) => {
            batched_optional_r2rml_enabled()
                && (r2rml_leaf_is_hash_join_safe(rp)
                    || (batched_optional_r2rml_star_enabled() && r2rml_star_is_hash_join_safe(rp)))
        }
        _ => false,
    }
}

/// PR-4b kill switch: `FLUREE_R2RML_BATCHED_OPTIONAL` (default ON; family falsy
/// spellings, [`crate::r2rml::env_switch_enabled`]). Off ⇒ R2RML inners are
/// never admitted to the batched path, so a correlated OPTIONAL over R2RML
/// falls to the per-row rebuild — the exact pre-PR-4b behavior.
fn batched_optional_r2rml_enabled() -> bool {
    use std::sync::OnceLock;
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| crate::r2rml::env_switch_enabled("FLUREE_R2RML_BATCHED_OPTIONAL"))
}

/// PR-4c sub-switch: `FLUREE_R2RML_BATCHED_OPTIONAL_STAR` (default ON within the
/// PR-4b family). Off ⇒ a same-subject STAR R2RML inner falls back to PR-4b's
/// scalar-only admission (byte-identical PR-4b behavior — the q050 sentinel), so
/// the star widening can be reverted independently.
fn batched_optional_r2rml_star_enabled() -> bool {
    use std::sync::OnceLock;
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| crate::r2rml::env_switch_enabled("FLUREE_R2RML_BATCHED_OPTIONAL_STAR"))
}

/// Whether an R2RML pattern is a subject-driven single-object LEAF scan — the
/// narrow shape PR-4b admits to the batched OPTIONAL. Its solutions depend on
/// the required row ONLY through its subject variable (the correlation), so
/// executing it once over the distinct subjects and hash-partitioning by the
/// correlation key reproduces the per-row results exactly. Covers a scalar POM
/// (`?s :p ?o`) and a single-valued subject-driven RefObjectMap (`?s :ref ?o`);
/// both bind exactly `object_var` from `subject_var`. EXCLUDES:
/// - stars (`star_bindings`/`star_constraints`) — multi-predicate cartesian;
/// - `type_var` — multi-class cartesian;
/// - a wildcard `predicate_var` and bound/constant subjects — not subject-driven
///   restrictions.
///
/// `consumed_filter`/`scan_filters` need NO exclusion: filter fusion only folds
/// a FILTER whose operands are all produced by this scan
/// (`rewrite.rs::consume_scan_local_filters` requires vars ⊆ `produced_vars`),
/// so a fused filter reads only values carried by the produced row itself and
/// evaluates identically whether the scan ran once (batched) or per row — no
/// hidden correlation channel. If filter fusion ever loosens that operand rule,
/// THIS admission is where it breaks.
///
/// (An object-only correlation is still sound via the seed-bound fallback — the
/// var is simply seeded rather than subject-probed — so it needs no gate here.)
fn r2rml_leaf_is_hash_join_safe(rp: &crate::ir::adapters::R2rmlPattern) -> bool {
    rp.subject_var.is_some()
        && rp.object_var.is_some()
        && rp.predicate_filter.is_some()
        && rp.subject_constant.is_none()
        && rp.predicate_var.is_none()
        && rp.type_var.is_none()
        && rp.star_bindings.is_empty()
        && rp.star_constraints.is_empty()
}

/// Whether an R2RML pattern is a same-subject STAR the batched OPTIONAL can admit
/// (PR-4c, q016). A star's solutions for a required row depend on the row ONLY
/// through its correlation var(s), and `R2rmlPattern::referenced_vars` surfaces
/// EVERY star-member object var (the P1 audit, landed + tested as PR-4b's
/// precursor — `adapters.rs`), so the correlation set is complete and the
/// partition is exact. Cartesian multiplicity (a correlation matching several
/// member rows) is reproduced batched≡per-row for LEFT-JOIN — the same leaf
/// materialization runs both paths; it is NOT the excluded row-multiplying
/// subquery (an R2RML leaf carries no internal ops). The correlation may be a
/// member OBJECT (q016 `?sh edw:order ?o`); such a var is seeded BOUND
/// (`corr_var_only_triple_object` is Triple-only) — sound, only de-optimized, so
/// it needs no gate here. `star_constraints` are constant-object existence
/// filters (no var). EXCLUDES `type_var` (multi-class cartesian — a separate
/// shape), a wildcard `predicate_var`, and a bound/constant subject.
fn r2rml_star_is_hash_join_safe(rp: &crate::ir::adapters::R2rmlPattern) -> bool {
    rp.subject_var.is_some()
        && !rp.star_bindings.is_empty()
        && rp.type_var.is_none()
        && rp.predicate_var.is_none()
        && rp.subject_constant.is_none()
}

/// True iff `v` occurs in the inner patterns ONLY as the object of one or more
/// Triples (never a subject/predicate, never inside a filter/path/other
/// pattern). Such a correlation var can be left unbound in the seeded inner so
/// it is produced subject-first — turning a scattered global object-probe into
/// a fast subject scan — while the full-key partition still matches the
/// produced binding back to the required row's value.
fn corr_var_only_triple_object(v: VarId, patterns: &[Pattern]) -> bool {
    let mut seen_as_object = false;
    for p in patterns {
        match p {
            Pattern::Triple(t) => {
                if matches!(&t.s, Ref::Var(x) if *x == v) || matches!(&t.p, Ref::Var(x) if *x == v)
                {
                    return false;
                }
                if matches!(&t.o, Term::Var(x) if *x == v) {
                    seen_as_object = true;
                }
            }
            other => {
                if other.referenced_vars().contains(&v) {
                    return false;
                }
            }
        }
    }
    seen_as_object
}

/// Kill-switch for the batched OPTIONAL hash-join (`FLUREE_OPTIONAL_HASH_JOIN=0`).
/// Read once; defaults to enabled.
fn optional_hash_join_disabled() -> bool {
    use std::sync::OnceLock;
    static DISABLED: OnceLock<bool> = OnceLock::new();
    *DISABLED.get_or_init(|| {
        std::env::var("FLUREE_OPTIONAL_HASH_JOIN")
            .map(|v| v == "0" || v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("off"))
            .unwrap_or(false)
    })
}

/// Single-shot operator that emits a precomputed set of rows (the distinct
/// correlation tuples) as the seed of a batched OPTIONAL hash-join, chunked to
/// the execution batch size.
struct MaterializedSeedOperator {
    schema: Arc<[VarId]>,
    batches: VecDeque<Batch>,
    state: OperatorState,
}

impl MaterializedSeedOperator {
    /// Build the seed from the distinct correlation tuples. Fallible: a
    /// `Batch::new` failure here would otherwise silently drop a seed chunk,
    /// running the inner with fewer correlations and undercounting the OPTIONAL
    /// — a silent wrong answer. Propagate instead so `build_batch` falls back to
    /// the per-row path rather than returning short results.
    fn new(schema: Arc<[VarId]>, rows: Vec<Vec<Binding>>, chunk: usize) -> Result<Self> {
        let chunk = chunk.max(1);
        let ncols = schema.len();
        let mut batches = VecDeque::new();
        for chunk_rows in rows.chunks(chunk) {
            let mut columns: Vec<Vec<Binding>> = (0..ncols)
                .map(|_| Vec::with_capacity(chunk_rows.len()))
                .collect();
            for row in chunk_rows {
                for (c, b) in row.iter().enumerate() {
                    columns[c].push(b.clone());
                }
            }
            batches.push_back(Batch::new(schema.clone(), columns)?);
        }
        Ok(Self {
            schema,
            batches,
            state: OperatorState::Created,
        })
    }
}

#[async_trait]
impl Operator for MaterializedSeedOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, _ctx: &ExecutionContext<'_>) -> Result<()> {
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, _ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }
        match self.batches.pop_front() {
            Some(batch) => Ok(Some(batch)),
            None => {
                self.state = OperatorState::Exhausted;
                Ok(None)
            }
        }
    }

    fn close(&mut self) {
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        Some(self.batches.iter().map(Batch::len).sum())
    }
}

/// Left-join operator for OPTIONAL semantics
///
/// For each row from the required operator, uses the `OptionalBuilder` to
/// create and execute a correlated optional operator. If matches are found,
/// emits combined rows. If no matches, emits the required row with
/// `Binding::Poisoned` for optional-only variables.
///
/// # Example
///
/// For query: `{ ?s :name ?name } OPTIONAL { ?s :email ?email }`
///
/// - If alice has no email: emits `{?s: alice, ?name: "Alice", ?email: Poisoned}`
/// - If bob has an email: emits `{?s: bob, ?name: "Bob", ?email: "bob@..."}`
///
/// # Multi-Pattern OPTIONAL
///
/// Unlike `BindJoinOperator`, this operator supports arbitrary optional subtrees
/// via the `OptionalBuilder` trait. The builder can construct complex operator
/// trees (joins, filters, property-joins) that are correlated with each required row.
pub struct OptionalOperator {
    /// Required (left) operator
    required: BoxedOperator,
    /// Builder for correlated optional operators
    optional_builder: Box<dyn OptionalBuilder>,
    /// Schema from required operator
    required_schema: Arc<[VarId]>,
    /// Combined output schema: required vars + optional-only vars
    combined_schema: Arc<[VarId]>,
    /// Current state
    state: OperatorState,
    /// Current required batch being processed
    current_required_batch: Option<Batch>,
    /// Current row index in required batch
    current_required_row: usize,
    /// Pending output: (required_row_idx, optional_batches, current_batch_idx, current_row_in_batch)
    /// Empty vec means no matches for that required row.
    /// The batch_idx and row_idx track progress for resuming when batch_size limit is hit.
    pending_output: VecDeque<PendingOptionalMatch>,
    /// Variables required by downstream operators; if set, output is trimmed.
    out_schema: Option<Arc<[VarId]>>,
    /// Memoized optional-side results keyed by correlation bindings.
    ///
    /// This prevents repeated OPTIONAL evaluation when the left side has fan-out
    /// on the correlation key (common for `?s <p1> ?o1 OPTIONAL { ?s <p2> ?o2 }`).
    result_cache: LruCache<Box<[u8]>, Arc<Vec<Batch>>>,
}

/// Tracks a required row's optional matches with progress cursor
struct PendingOptionalMatch {
    required_row: usize,
    optional_batches: Vec<Batch>,
    /// Current batch index within optional_batches (for resuming)
    batch_idx: usize,
    /// Current row index within the current batch (for resuming)
    row_idx: usize,
    /// Whether any optional row matched unification
    matched: bool,
}

impl OptionalOperator {
    /// Create a new left-join operator with an optional builder
    ///
    /// This is the general constructor that accepts any `OptionalBuilder`.
    /// For simple single-pattern OPTIONAL, use `new()` instead.
    ///
    /// # Arguments
    ///
    /// * `required` - The required (left) operator
    /// * `required_schema` - Schema of the required operator
    /// * `optional_builder` - Builder that creates correlated optional operators
    pub fn with_builder(
        required: BoxedOperator,
        required_schema: Arc<[VarId]>,
        optional_builder: Box<dyn OptionalBuilder>,
    ) -> Self {
        // Build combined schema: required + optional-only
        let mut combined = required_schema.to_vec();
        combined.extend(optional_builder.optional_only_vars());
        let combined_schema: Arc<[VarId]> = Arc::from(combined.into_boxed_slice());

        Self {
            required,
            optional_builder,
            required_schema,
            combined_schema,
            state: OperatorState::Created,
            current_required_batch: None,
            current_required_row: 0,
            pending_output: VecDeque::new(),
            out_schema: None,
            result_cache: LruCache::new(NonZeroUsize::new(8192).expect("8192 is non-zero")),
        }
    }

    /// Trim output to only the specified downstream variables.
    pub fn with_out_schema(mut self, downstream_vars: Option<&[VarId]>) -> Self {
        self.out_schema = compute_trimmed_vars(&self.combined_schema, downstream_vars);
        self
    }

    /// Create a new left-join operator for a single triple pattern
    ///
    /// This is a convenience constructor for the common case of OPTIONAL
    /// with a single triple pattern.
    ///
    /// # Arguments
    ///
    /// * `required` - The required (left) operator
    /// * `required_schema` - Schema of the required operator
    /// * `optional_pattern` - Single triple pattern for the optional side
    pub fn new(
        required: BoxedOperator,
        required_schema: Arc<[VarId]>,
        optional_pattern: TriplePattern,
        planning: PlanningContext,
    ) -> Self {
        let builder =
            PatternOptionalBuilder::new(required_schema.clone(), optional_pattern, planning);
        Self::with_builder(required, required_schema, Box::new(builder))
    }

    /// Create a row with Poisoned bindings for optional-only vars
    fn create_poisoned_row(&self, required_batch: &Batch, required_row: usize) -> Vec<Binding> {
        let mut result = Vec::with_capacity(self.combined_schema.len());

        // Copy all required columns
        for col in 0..self.required_schema.len() {
            result.push(required_batch.get_by_col(required_row, col).clone());
        }

        // Add Poisoned for optional-only vars
        for _ in self.optional_builder.optional_only_vars() {
            result.push(Binding::Poisoned);
        }

        result
    }

    /// Check if required row bindings match optional row bindings for shared vars
    fn unify_check(
        &self,
        required_batch: &Batch,
        required_row: usize,
        optional_batch: &Batch,
        optional_row: usize,
    ) -> bool {
        // Unification must be resilient to optional operator schemas that do not
        // include substituted correlation vars.
        //
        // If the optional-side batch doesn't have the shared var column, we treat
        // it as already enforced by correlation/substitution and skip the check.
        self.optional_builder
            .unify_instructions()
            .iter()
            .all(|instr| {
                let var = self.required_schema[instr.left_col];
                let opt_col = optional_batch.schema().iter().position(|v| *v == var);
                if let Some(opt_col) = opt_col {
                    let left_val = required_batch.get_by_col(required_row, instr.left_col);
                    let right_val = optional_batch.get_by_col(optional_row, opt_col);

                    // Poisoned blocks matching; Unbound is compatible with anything.
                    if left_val.is_poisoned() || right_val.is_poisoned() {
                        return false;
                    }
                    if matches!(left_val, Binding::Unbound) || matches!(right_val, Binding::Unbound)
                    {
                        return true;
                    }
                    left_val == right_val
                } else {
                    true
                }
            })
    }

    /// Combine required row with optional row into output row
    fn combine_rows(
        &self,
        required_batch: &Batch,
        required_row: usize,
        optional_batch: &Batch,
        optional_row: usize,
    ) -> Vec<Binding> {
        let mut result = Vec::with_capacity(self.combined_schema.len());

        // Copy all required columns
        for col in 0..self.required_schema.len() {
            result.push(required_batch.get_by_col(required_row, col).clone());
        }

        // Copy optional-only columns from optional batch
        let optional_schema = optional_batch.schema();
        for var in self.optional_builder.optional_only_vars() {
            if let Some(opt_col) = optional_schema.iter().position(|v| v == var) {
                result.push(optional_batch.get_by_col(optional_row, opt_col).clone());
            } else {
                // Shouldn't happen, but fallback to Poisoned
                result.push(Binding::Poisoned);
            }
        }

        result
    }
}

#[async_trait]
impl Operator for OptionalOperator {
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        vec![crate::plan_node::PlanChild::child(self.required.as_ref())]
    }
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

        // Open required operator
        self.required.open(ctx).await?;

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

        let batch_start = Instant::now();
        let batch_size = ctx.batch_size;
        let mut max_required_batch_len = 0usize;
        let mut output_columns: Vec<Vec<Binding>> = (0..self.combined_schema.len())
            .map(|_| Vec::with_capacity(batch_size))
            .collect();
        let mut rows_added = 0;
        let mut required_rows_seen = 0usize;
        let mut built_optionals = 0usize;
        let mut batched_builds = 0usize;
        let mut batched_rows = 0usize;
        let mut builder_none = 0usize;
        let mut cache_hits = 0usize;
        let mut optional_result_batches = 0usize;

        // Process until we have a full batch or exhaust input
        loop {
            // First, check if we have pending output from previous iterations
            if !self.pending_output.is_empty() {
                let required_batch = match &self.current_required_batch {
                    Some(b) => b,
                    None => {
                        // Shouldn't happen - pending_output implies we have a batch
                        self.pending_output.clear();
                        continue;
                    }
                };

                while rows_added < batch_size && !self.pending_output.is_empty() {
                    // Extract info we need without holding mutable borrow
                    let (required_row, is_empty, num_batches) = {
                        let pending = self.pending_output.front().unwrap();
                        (
                            pending.required_row,
                            pending.optional_batches.is_empty(),
                            pending.optional_batches.len(),
                        )
                    };

                    if is_empty {
                        // No matches - emit row with Poisoned for optional-only vars
                        let row = self.create_poisoned_row(required_batch, required_row);
                        for (col, val) in row.into_iter().enumerate() {
                            output_columns[col].push(val);
                        }
                        rows_added += 1;
                        self.pending_output.pop_front();
                    } else {
                        // Has matches - emit combined rows with progress tracking
                        let mut fully_processed = false;

                        loop {
                            // Get current progress
                            let (batch_idx, row_idx) = {
                                let pending = self.pending_output.front().unwrap();
                                (pending.batch_idx, pending.row_idx)
                            };

                            if batch_idx >= num_batches {
                                fully_processed = true;
                                break;
                            }

                            // Get the optional batch
                            let optional_batch =
                                &self.pending_output.front().unwrap().optional_batches[batch_idx];
                            let batch_len = optional_batch.len();

                            if row_idx >= batch_len {
                                // Move to next batch
                                let pending = self.pending_output.front_mut().unwrap();
                                pending.batch_idx += 1;
                                pending.row_idx = 0;
                                continue;
                            }

                            if rows_added >= batch_size {
                                // Hit batch limit - return what we have, resume later
                                break;
                            }

                            // Get current opt_row and advance
                            let opt_row = {
                                let pending = self.pending_output.front_mut().unwrap();
                                let r = pending.row_idx;
                                pending.row_idx += 1;
                                r
                            };

                            // Unification check
                            if !self.unify_check(
                                required_batch,
                                required_row,
                                &self.pending_output.front().unwrap().optional_batches[batch_idx],
                                opt_row,
                            ) {
                                continue;
                            }

                            self.pending_output.front_mut().unwrap().matched = true;
                            let row = self.combine_rows(
                                required_batch,
                                required_row,
                                &self.pending_output.front().unwrap().optional_batches[batch_idx],
                                opt_row,
                            );
                            for (col, val) in row.into_iter().enumerate() {
                                output_columns[col].push(val);
                            }
                            rows_added += 1;
                        }

                        if fully_processed {
                            let needs_poisoned = self
                                .pending_output
                                .front()
                                .is_some_and(|pending| !pending.matched);
                            if needs_poisoned {
                                let pending = self.pending_output.front_mut().unwrap();
                                pending.optional_batches.clear();
                                pending.batch_idx = 0;
                                pending.row_idx = 0;
                                continue;
                            }
                            self.pending_output.pop_front();
                        } else {
                            // Not fully processed - we hit batch_size limit
                            // Keep this entry for next call
                            break;
                        }
                    }
                }

                if rows_added > 0 && (rows_added >= batch_size || self.pending_output.is_empty()) {
                    break;
                }
            }

            // Need to process more required rows
            // First, ensure we have a required batch
            if self.current_required_batch.is_none() {
                match self.required.next_batch(ctx).await? {
                    Some(batch) => {
                        self.current_required_batch = Some(batch);
                        self.current_required_row = 0;
                    }
                    None => {
                        // Required exhausted
                        self.state = OperatorState::Exhausted;
                        break;
                    }
                }
            }

            let required_batch = self.current_required_batch.as_ref().unwrap();
            max_required_batch_len = max_required_batch_len.max(required_batch.len());

            // Process current required row
            if self.current_required_row < required_batch.len() {
                if self.pending_output.is_empty() {
                    if let Some(batched_pending) = self
                        .optional_builder
                        .build_batch(required_batch, self.current_required_row, ctx)
                        .await?
                    {
                        batched_builds += 1;
                        batched_rows += batched_pending.len();
                        required_rows_seen += batched_pending.len();
                        optional_result_batches += batched_pending
                            .iter()
                            .map(|(_, optional_batches)| optional_batches.len())
                            .sum::<usize>();
                        self.current_required_row = required_batch.len();
                        self.pending_output.extend(batched_pending.into_iter().map(
                            |(required_row, optional_batches)| PendingOptionalMatch {
                                required_row,
                                optional_batches,
                                batch_idx: 0,
                                row_idx: 0,
                                matched: false,
                            },
                        ));
                        continue;
                    }
                }

                let required_row = self.current_required_row;
                self.current_required_row += 1;
                required_rows_seen += 1;

                // Build optional operator for this row (propagate errors)
                let cache_key =
                    self.optional_builder
                        .cache_key(required_batch, required_row, ctx)?;

                if let Some(key) = cache_key.as_ref() {
                    if let Some(cached) = self.result_cache.get(key) {
                        cache_hits += 1;
                        optional_result_batches += cached.len();
                        self.pending_output.push_back(PendingOptionalMatch {
                            required_row,
                            optional_batches: (**cached).clone(),
                            batch_idx: 0,
                            row_idx: 0,
                            matched: false,
                        });
                        continue;
                    }
                }

                match self
                    .optional_builder
                    .build(required_batch, required_row, ctx)?
                {
                    None => {
                        builder_none += 1;
                        // Builder returned None (e.g., poisoned correlation var)
                        // Emit with Poisoned for optional-only vars
                        self.pending_output.push_back(PendingOptionalMatch {
                            required_row,
                            optional_batches: Vec::new(),
                            batch_idx: 0,
                            row_idx: 0,
                            matched: false,
                        });
                    }
                    Some(mut optional_op) => {
                        built_optionals += 1;
                        // Execute optional operator
                        optional_op.open(ctx).await?;

                        // Collect all optional results
                        let mut optional_batches = Vec::new();
                        while let Some(opt_batch) = optional_op.next_batch(ctx).await? {
                            ctx.check_cancelled()?;
                            if !opt_batch.is_empty() {
                                optional_batches.push(opt_batch);
                            }
                            ctx.check_cancelled()?;
                        }
                        optional_result_batches += optional_batches.len();

                        optional_op.close();

                        if let Some(key) = cache_key {
                            self.result_cache
                                .put(key, Arc::new(optional_batches.clone()));
                        }

                        // Add to pending output with progress cursor at start
                        self.pending_output.push_back(PendingOptionalMatch {
                            required_row,
                            optional_batches,
                            batch_idx: 0,
                            row_idx: 0,
                            matched: false,
                        });
                    }
                }
            } else {
                // Exhausted current required batch, get next
                self.current_required_batch = None;
            }
        }

        let elapsed_ms = (batch_start.elapsed().as_secs_f64() * 1000.0) as u64;
        let should_debug = elapsed_ms >= OPTIONAL_DEBUG_MIN_MS
            || built_optionals >= OPTIONAL_DEBUG_MIN_WORK
            || batched_rows >= OPTIONAL_DEBUG_MIN_WORK
            || cache_hits >= OPTIONAL_DEBUG_MIN_WORK
            || optional_result_batches >= OPTIONAL_DEBUG_MIN_WORK;
        if rows_added == 0 {
            if should_debug {
                tracing::debug!(
                    rows_added,
                    required_rows_seen,
                    max_required_batch_len,
                    built_optionals,
                    batched_builds,
                    batched_rows,
                    builder_none,
                    cache_hits,
                    optional_result_batches,
                    pending_output = self.pending_output.len(),
                    elapsed_ms,
                    "optional batch summary"
                );
            } else {
                tracing::trace!(
                    rows_added,
                    required_rows_seen,
                    max_required_batch_len,
                    built_optionals,
                    batched_builds,
                    batched_rows,
                    builder_none,
                    cache_hits,
                    optional_result_batches,
                    pending_output = self.pending_output.len(),
                    elapsed_ms,
                    "optional batch summary"
                );
            }
            return Ok(None);
        }

        let batch = Batch::new(self.combined_schema.clone(), output_columns)?;
        if should_debug {
            tracing::debug!(
                rows_added,
                required_rows_seen,
                max_required_batch_len,
                built_optionals,
                batched_builds,
                batched_rows,
                builder_none,
                cache_hits,
                optional_result_batches,
                pending_output = self.pending_output.len(),
                elapsed_ms,
                "optional batch summary"
            );
        } else {
            tracing::trace!(
                rows_added,
                required_rows_seen,
                max_required_batch_len,
                built_optionals,
                batched_builds,
                batched_rows,
                builder_none,
                cache_hits,
                optional_result_batches,
                pending_output = self.pending_output.len(),
                elapsed_ms,
                "optional batch summary"
            );
        }
        Ok(trim_batch(&self.out_schema, batch))
    }

    fn close(&mut self) {
        self.required.close();
        self.current_required_batch = None;
        self.pending_output.clear();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Left join preserves all required rows (plus potential fan-out from matches)
        self.required.estimated_rows()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::Sid;

    fn make_optional_pattern() -> TriplePattern {
        // ?s :email ?email
        TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(Sid::new(101, "email")),
            Term::Var(VarId(2)),
        )
    }

    #[test]
    fn test_left_join_schema() {
        // Required schema: [?s, ?name]
        let required_schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let optional_pattern = make_optional_pattern();

        // Mock required operator
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

        let op = OptionalOperator::new(
            Box::new(MockOp),
            required_schema,
            optional_pattern,
            crate::temporal_mode::PlanningContext::current(),
        );

        // Combined schema should be: [?s, ?name, ?email]
        assert_eq!(op.schema().len(), 3);
        assert_eq!(op.schema()[0], VarId(0)); // ?s (from required)
        assert_eq!(op.schema()[1], VarId(1)); // ?name (from required)
        assert_eq!(op.schema()[2], VarId(2)); // ?email (optional-only)
    }

    #[test]
    fn test_pattern_optional_builder() {
        let required_schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let optional_pattern = make_optional_pattern();

        let builder = PatternOptionalBuilder::new(
            required_schema,
            optional_pattern,
            crate::temporal_mode::PlanningContext::current(),
        );

        // Check schema
        assert_eq!(builder.schema().len(), 2); // ?s, ?email
        assert!(builder.schema().contains(&VarId(0)));
        assert!(builder.schema().contains(&VarId(2)));

        // Check optional-only vars
        assert_eq!(builder.optional_only_vars().len(), 1);
        assert_eq!(builder.optional_only_vars()[0], VarId(2)); // ?email

        // Check unify instructions (for ?s)
        assert_eq!(builder.unify_instructions().len(), 1);
        assert_eq!(builder.unify_instructions()[0].left_col, 0); // ?s in required
    }

    // PR-4b: the batched-OPTIONAL admission for R2RML inners is NARROW — only a
    // subject-driven single-object leaf (scalar POM / single-valued ref). Every
    // richer shape stays on the per-row path pending differential evidence.
    #[test]
    fn r2rml_leaf_admission_is_narrow() {
        use crate::ir::adapters::R2rmlPattern;

        let mut leaf = R2rmlPattern::new("gs:main", VarId(0), Some(VarId(1)));
        leaf.predicate_filter = Some("http://ex/rating".to_string());
        assert!(
            r2rml_leaf_is_hash_join_safe(&leaf),
            "subject-driven scalar/ref leaf is admitted"
        );
        // Full gate honors the shape (FLUREE_R2RML_BATCHED_OPTIONAL defaults on).
        assert!(inner_pattern_is_hash_join_safe(&Pattern::R2rml(
            leaf.clone()
        )));

        let mut star = leaf.clone();
        star.star_bindings = vec![("http://ex/p2".to_string(), VarId(2))];
        assert!(!r2rml_leaf_is_hash_join_safe(&star), "star excluded");

        let mut star_c = leaf.clone();
        star_c.star_constraints = vec![(
            "http://ex/p3".to_string(),
            crate::r2rml::ObjectConstant::Iri("http://ex/c".to_string()),
        )];
        assert!(
            !r2rml_leaf_is_hash_join_safe(&star_c),
            "star-constraint excluded"
        );

        let mut tv = leaf.clone();
        tv.type_var = Some(VarId(3));
        assert!(!r2rml_leaf_is_hash_join_safe(&tv), "type-var excluded");

        let mut wild = leaf.clone();
        wild.predicate_var = Some(VarId(4));
        assert!(
            !r2rml_leaf_is_hash_join_safe(&wild),
            "wildcard predicate excluded"
        );

        let mut bound = leaf.clone();
        bound.subject_var = None;
        bound.subject_constant = Some("http://ex/s/1".to_string());
        assert!(
            !r2rml_leaf_is_hash_join_safe(&bound),
            "bound subject excluded"
        );

        let mut no_obj = leaf.clone();
        no_obj.object_var = None;
        assert!(
            !r2rml_leaf_is_hash_join_safe(&no_obj),
            "no object var excluded"
        );
    }

    // PR-4c: the STAR admission (`r2rml_star_is_hash_join_safe`) admits a
    // same-subject star (≥1 star member), incl. an object-correlated one (q016),
    // and keeps type-var / wildcard / bound-subject EXCLUDED. A bare scalar leaf
    // is NOT a star (empty star_bindings) — it stays on the PR-4b arm.
    #[test]
    fn r2rml_star_admission() {
        use crate::ir::adapters::R2rmlPattern;

        // A same-subject star: primary member (object_var) + one star member.
        let mut star = R2rmlPattern::new("gs:main", VarId(3), Some(VarId(0)));
        star.predicate_filter = Some("http://ex/order".to_string());
        star.star_bindings = vec![("http://ex/shipStatus".to_string(), VarId(1))];
        assert!(
            r2rml_star_is_hash_join_safe(&star),
            "same-subject star (incl. object-correlated) is admitted"
        );
        // A constant-object existence constraint carries no var — still safe.
        let mut star_c = star.clone();
        star_c.star_constraints = vec![(
            "http://ex/kind".to_string(),
            crate::r2rml::ObjectConstant::Iri("http://ex/c".to_string()),
        )];
        assert!(
            r2rml_star_is_hash_join_safe(&star_c),
            "star with a constant-object constraint (no var) stays safe"
        );

        // Kept exclusions.
        let mut tv = star.clone();
        tv.type_var = Some(VarId(9));
        assert!(!r2rml_star_is_hash_join_safe(&tv), "type-var excluded");
        let mut wild = star.clone();
        wild.predicate_var = Some(VarId(9));
        assert!(!r2rml_star_is_hash_join_safe(&wild), "wildcard excluded");
        let mut bound = star.clone();
        bound.subject_var = None;
        bound.subject_constant = Some("http://ex/s/1".to_string());
        assert!(
            !r2rml_star_is_hash_join_safe(&bound),
            "bound subject excluded"
        );

        // A non-star scalar leaf is NOT admitted by the star arm.
        let mut leaf = R2rmlPattern::new("gs:main", VarId(0), Some(VarId(1)));
        leaf.predicate_filter = Some("http://ex/rating".to_string());
        assert!(
            !r2rml_star_is_hash_join_safe(&leaf),
            "a scalar leaf has empty star_bindings — handled by the PR-4b arm, not the star arm"
        );
    }

    // PR-4b (B): the hermetic differential — the batched hash-left-join and the
    // per-row rebuild must produce IDENTICAL optional-side bindings on a GENUINE
    // R2RML dangling FK (empty bucket) and a matched FK, driving both OptionalBuilder
    // methods directly on one PlanTreeOptionalBuilder with a mock mapping + table
    // provider (no live Snowflake, no switch — the two code paths are compared
    // head-to-head). This mechanically pins batched≡per-row on the R2RML-specific
    // miss edge that q050's live oracle run covers only by code-path identity.
    #[test]
    fn batched_equals_per_row_on_dangling_fk() {
        use crate::context::ExecutionContext;
        use crate::ir::adapters::R2rmlPattern;
        use crate::r2rml::{ColumnBatchStream, R2rmlProvider, R2rmlTableProvider, ScanFilter};
        use crate::var_registry::VarRegistry;
        use fluree_db_core::LedgerSnapshot;
        use fluree_db_r2rml::mapping::{
            CompiledR2rmlMapping, ObjectMap, PredicateMap, PredicateObjectMap, RefObjectMap,
            TriplesMap,
        };
        use fluree_db_tabular::{BatchSchema, Column, ColumnBatch, FieldInfo, FieldType};
        use std::sync::Arc;

        // ---- synthetic mapping: Product --edw:supplier(RefObjectMap)--> Supplier
        let mapping = Arc::new(CompiledR2rmlMapping::new(vec![
            TriplesMap::new("#Product", "products")
                .with_subject_template("http://ex/product/{PID}")
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://ex/supplier"),
                    object_map: ObjectMap::RefObjectMap(RefObjectMap::new(
                        "#Supplier",
                        "SUP_FK",
                        "SID",
                    )),
                }),
            TriplesMap::new("#Supplier", "suppliers")
                .with_subject_template("http://ex/supplier/{SID}"),
        ]));

        #[derive(Debug)]
        struct MapProvider(Arc<CompiledR2rmlMapping>);
        #[async_trait]
        impl R2rmlProvider for MapProvider {
            async fn has_r2rml_mapping(&self, _gs: &str) -> bool {
                true
            }
            async fn compiled_mapping(
                &self,
                _gs: &str,
                _t: Option<i64>,
            ) -> Result<Arc<CompiledR2rmlMapping>> {
                Ok(Arc::clone(&self.0))
            }
        }

        fn ints(name: &str, id: i32, vals: Vec<Option<i64>>) -> Column {
            let _ = (name, id);
            Column::Int64(vals)
        }
        fn batch(fields: Vec<(&str, i32)>, cols: Vec<Column>) -> ColumnBatch {
            let schema = Arc::new(BatchSchema::new(
                fields
                    .into_iter()
                    .map(|(n, id)| FieldInfo {
                        name: n.to_string(),
                        field_type: FieldType::Int64,
                        nullable: true,
                        field_id: id,
                    })
                    .collect(),
            ));
            ColumnBatch::new(schema, cols).unwrap()
        }

        #[derive(Debug)]
        struct TableProvider;
        #[async_trait]
        impl R2rmlTableProvider for TableProvider {
            async fn scan_table(
                &self,
                _gs: &str,
                table: &str,
                _proj: &[String],
                _filters: &[ScanFilter],
                _t: Option<i64>,
            ) -> Result<ColumnBatchStream> {
                // products: PID 1 (FK 10 -> exists) and PID 2 (FK 99 -> DANGLING).
                // suppliers: only SID 10 exists, so product 2's FK is dangling.
                let b = if table == "products" {
                    batch(
                        vec![("PID", 1), ("SUP_FK", 2)],
                        vec![
                            ints("PID", 1, vec![Some(1), Some(2)]),
                            ints("SUP_FK", 2, vec![Some(10), Some(99)]),
                        ],
                    )
                } else {
                    batch(vec![("SID", 1)], vec![ints("SID", 1, vec![Some(10)])])
                };
                Ok(Box::pin(futures::stream::once(async move { Ok(b) })))
            }
        }

        // ---- OPTIONAL { ?p edw:supplier ?s }, correlated on ?p (VarId 0).
        let mut inner = R2rmlPattern::new("gs:main", VarId(0), Some(VarId(1)));
        inner.predicate_filter = Some("http://ex/supplier".to_string());
        assert!(
            r2rml_leaf_is_hash_join_safe(&inner),
            "the ref leaf must be admitted, else this test is vacuous"
        );
        let inner_patterns = vec![Pattern::R2rml(inner)];
        let required_schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let builder = PlanTreeOptionalBuilder::new(
            required_schema.clone(),
            inner_patterns,
            None,
            crate::temporal_mode::PlanningContext::current(),
        );

        // Required rows: the two products, as subject IRIs (?p bound).
        let required = Batch::new(
            required_schema.clone(),
            vec![vec![
                Binding::iri("http://ex/product/1"),
                Binding::iri("http://ex/product/2"),
            ]],
        )
        .unwrap();

        let map_provider = MapProvider(Arc::clone(&mapping));
        let table_provider = TableProvider;
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let mut ctx = ExecutionContext::new(&snapshot, &vars);
        ctx = ctx.with_r2rml_providers(&map_provider, &table_provider);

        // ---- batched path.
        let batched = futures::executor::block_on(builder.build_batch(&required, 0, &ctx))
            .expect("build_batch")
            .expect("batched path admitted");

        // One produced row as a schema-order-independent VALUE key over the
        // vars the OPTIONAL side actually CONTRIBUTES. The correlation var is
        // excluded: per-row output carries it (the seed binds it, to the
        // required row's value by construction) while batched partition batches
        // don't re-project it — its association is pinned by the partition row
        // index instead, which the multiset compare keys on. A batched row
        // partitioned to the WRONG required row therefore still fails.
        let required_vars: std::collections::HashSet<VarId> =
            required_schema.iter().copied().collect();
        let row_repr = |b: &Batch, r: usize| -> Vec<String> {
            let mut kv: Vec<String> = (0..b.schema().len())
                .filter(|c| !required_vars.contains(&b.schema()[*c]))
                .map(|c| format!("{:?}={:?}", b.schema()[c], b.get_by_col(r, c)))
                .collect();
            kv.sort();
            kv
        };

        // ---- per-row path: build + drain the inner operator for each row.
        let per_row: Vec<Vec<Vec<String>>> = (0..required.len())
            .map(|row| {
                let op = futures::executor::block_on(async {
                    let mut op = builder.build(&required, row, &ctx)?.expect("per-row op");
                    op.open(&ctx).await?;
                    let mut rows = Vec::new();
                    while let Some(b) = op.next_batch(&ctx).await? {
                        for r in 0..b.len() {
                            rows.push(row_repr(&b, r));
                        }
                    }
                    op.close();
                    Ok::<_, crate::error::QueryError>(rows)
                })
                .expect("per-row drain");
                op
            })
            .collect();

        // Product 1 matches (supplier/10); product 2's FK is dangling → OPTIONAL miss.
        // Assert the per-row path produced exactly one binding for row 0 and none
        // for row 1, and that the batched path agrees row-for-row.
        assert_eq!(per_row[0].len(), 1, "matched FK binds ?s: {:?}", per_row[0]);
        assert_eq!(
            per_row[1].len(),
            0,
            "dangling FK is a miss: {:?}",
            per_row[1]
        );

        for (row, batches) in &batched {
            let batched_rows: usize = batches.iter().map(Batch::len).sum();
            assert_eq!(
                batched_rows,
                per_row[*row].len(),
                "batched != per-row optional-row count for required row {row}"
            );
            // Same ANSWER, not just same shape: the (row, binding) multisets
            // must agree, so a batched path that bound ?s to the WRONG supplier
            // IRI with the right cardinality still fails.
            let mut batched_vals: Vec<Vec<String>> = batches
                .iter()
                .flat_map(|b| (0..b.len()).map(move |r| row_repr(b, r)))
                .collect();
            batched_vals.sort();
            let mut per_row_vals = per_row[*row].clone();
            per_row_vals.sort();
            assert_eq!(
                batched_vals, per_row_vals,
                "batched != per-row optional binding VALUES for required row {row}"
            );
        }
        // Rows with no batched entry must be per-row misses too.
        let batched_rows: std::collections::HashSet<usize> =
            batched.iter().map(|(r, _)| *r).collect();
        for (row, pr) in per_row.iter().enumerate() {
            if !batched_rows.contains(&row) {
                assert!(pr.is_empty(), "batched dropped row {row} that per-row kept");
            }
        }
    }

    // PR-4c (the correctness gate): the hermetic batched≡per-row differential for a
    // same-subject STAR OPTIONAL inner with an OBJECT correlation (q016's shape:
    // `?sh edw:order ?o ; edw:shipStatus ?st`, correlated on `?o`). ONE mock dataset
    // carries all three multiplicity risks, asserted row-for-row both paths:
    //   - `?o` = order/1 matched by TWO shipments  → the CARTESIAN (2 optional rows);
    //   - `?o` = order/2 matched by ZERO shipments  → the LEFT-JOIN miss (0 rows);
    //   - `?o` = order/3 matched by one shipment with a NULL star member (`?st`).
    #[test]
    fn batched_equals_per_row_on_object_correlated_star() {
        use crate::context::ExecutionContext;
        use crate::ir::adapters::R2rmlPattern;
        use crate::r2rml::{ColumnBatchStream, R2rmlProvider, R2rmlTableProvider, ScanFilter};
        use crate::var_registry::VarRegistry;
        use fluree_db_core::LedgerSnapshot;
        use fluree_db_r2rml::mapping::{
            CompiledR2rmlMapping, ObjectMap, PredicateMap, PredicateObjectMap, RefObjectMap,
            TriplesMap,
        };
        use fluree_db_tabular::{BatchSchema, Column, ColumnBatch, FieldInfo, FieldType};
        use std::sync::Arc;

        // Shipment --edw:order(RefObjectMap)--> Order  ; edw:shipStatus (scalar col).
        let mapping = Arc::new(CompiledR2rmlMapping::new(vec![
            TriplesMap::new("#Shipment", "shipments")
                .with_subject_template("http://ex/shipment/{SH}")
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://ex/order"),
                    object_map: ObjectMap::RefObjectMap(RefObjectMap::new("#Order", "OFK", "OID")),
                })
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://ex/shipStatus"),
                    object_map: ObjectMap::column("STATUS"),
                }),
            TriplesMap::new("#Order", "orders").with_subject_template("http://ex/order/{OID}"),
        ]));

        #[derive(Debug)]
        struct MapProvider(Arc<CompiledR2rmlMapping>);
        #[async_trait]
        impl R2rmlProvider for MapProvider {
            async fn has_r2rml_mapping(&self, _gs: &str) -> bool {
                true
            }
            async fn compiled_mapping(
                &self,
                _gs: &str,
                _t: Option<i64>,
            ) -> Result<Arc<CompiledR2rmlMapping>> {
                Ok(Arc::clone(&self.0))
            }
        }

        fn batch(fields: Vec<(&str, i32)>, cols: Vec<Column>) -> ColumnBatch {
            let schema = Arc::new(BatchSchema::new(
                fields
                    .into_iter()
                    .map(|(n, id)| FieldInfo {
                        name: n.to_string(),
                        field_type: FieldType::Int64,
                        nullable: true,
                        field_id: id,
                    })
                    .collect(),
            ));
            ColumnBatch::new(schema, cols).unwrap()
        }

        #[derive(Debug)]
        struct TableProvider;
        #[async_trait]
        impl R2rmlTableProvider for TableProvider {
            async fn scan_table(
                &self,
                _gs: &str,
                table: &str,
                _proj: &[String],
                _filters: &[ScanFilter],
                _t: Option<i64>,
            ) -> Result<ColumnBatchStream> {
                // shipments: SH 100/101 → order 1 (TWO → cartesian); 102 → order 3
                // with STATUS=NULL (null star member); no shipment for order 2 (miss).
                let b = if table == "shipments" {
                    batch(
                        vec![("SH", 1), ("OFK", 2), ("STATUS", 3)],
                        vec![
                            Column::Int64(vec![Some(100), Some(101), Some(102), Some(103)]),
                            Column::Int64(vec![Some(1), Some(1), Some(3), Some(4)]),
                            // order 4's only shipment has a NULL shipStatus → the
                            // same-subject star (a conjunction) drops that row.
                            Column::Int64(vec![Some(10), Some(20), Some(30), None]),
                        ],
                    )
                } else {
                    // orders (ref parent): OID 1..4 all exist.
                    batch(
                        vec![("OID", 1)],
                        vec![Column::Int64(vec![Some(1), Some(2), Some(3), Some(4)])],
                    )
                };
                Ok(Box::pin(futures::stream::once(async move { Ok(b) })))
            }
        }

        // Inner star: subject ?sh (VarId 3); primary member edw:order → object ?o
        // (VarId 0, the correlation); star member edw:shipStatus → ?st (VarId 1).
        let mut inner = R2rmlPattern::new("gs:main", VarId(3), Some(VarId(0)));
        inner.predicate_filter = Some("http://ex/order".to_string());
        inner.star_bindings = vec![("http://ex/shipStatus".to_string(), VarId(1))];
        assert!(
            r2rml_star_is_hash_join_safe(&inner) && !r2rml_leaf_is_hash_join_safe(&inner),
            "the object-correlated star must take the PR-4c star arm, not the PR-4b leaf arm"
        );
        let inner_patterns = vec![Pattern::R2rml(inner)];
        let required_schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let builder = PlanTreeOptionalBuilder::new(
            required_schema.clone(),
            inner_patterns,
            None,
            crate::temporal_mode::PlanningContext::current(),
        );

        // Required rows: the four orders, as the ref-rendered object IRIs (?o).
        let required = Batch::new(
            required_schema.clone(),
            vec![vec![
                Binding::iri("http://ex/order/1"),
                Binding::iri("http://ex/order/2"),
                Binding::iri("http://ex/order/3"),
                Binding::iri("http://ex/order/4"),
            ]],
        )
        .unwrap();

        let map_provider = MapProvider(Arc::clone(&mapping));
        let table_provider = TableProvider;
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let mut ctx = ExecutionContext::new(&snapshot, &vars);
        ctx = ctx.with_r2rml_providers(&map_provider, &table_provider);

        let batched = futures::executor::block_on(builder.build_batch(&required, 0, &ctx))
            .expect("build_batch")
            .expect("star path admitted");

        // Value key over the OPTIONAL-contributed vars (correlation ?o excluded — the
        // partition row index pins its association; a mis-partition still fails).
        let required_vars: std::collections::HashSet<VarId> =
            required_schema.iter().copied().collect();
        let row_repr = |b: &Batch, r: usize| -> Vec<String> {
            let mut kv: Vec<String> = (0..b.schema().len())
                .filter(|c| !required_vars.contains(&b.schema()[*c]))
                .map(|c| format!("{:?}={:?}", b.schema()[c], b.get_by_col(r, c)))
                .collect();
            kv.sort();
            kv
        };

        let per_row: Vec<Vec<Vec<String>>> = (0..required.len())
            .map(|row| {
                futures::executor::block_on(async {
                    let mut op = builder.build(&required, row, &ctx)?.expect("per-row op");
                    op.open(&ctx).await?;
                    let mut rows = Vec::new();
                    while let Some(b) = op.next_batch(&ctx).await? {
                        for r in 0..b.len() {
                            rows.push(row_repr(&b, r));
                        }
                    }
                    op.close();
                    Ok::<_, crate::error::QueryError>(rows)
                })
                .expect("per-row drain")
            })
            .collect();

        // order/1 → 2 (cartesian); order/2 → 0 (miss); order/3 → 1 (valid single
        // match); order/4 → 0 (its one shipment's null shipStatus drops the star).
        assert_eq!(
            per_row[0].len(),
            2,
            "cartesian: order/1 has 2 shipments: {:?}",
            per_row[0]
        );
        assert_eq!(
            per_row[1].len(),
            0,
            "miss: order/2 has no shipment: {:?}",
            per_row[1]
        );
        assert_eq!(
            per_row[2].len(),
            1,
            "order/3 has 1 valid shipment: {:?}",
            per_row[2]
        );
        assert_eq!(
            per_row[3].len(),
            0,
            "null star member drops the row: {:?}",
            per_row[3]
        );

        for (row, batches) in &batched {
            let mut batched_vals: Vec<Vec<String>> = batches
                .iter()
                .flat_map(|b| (0..b.len()).map(move |r| row_repr(b, r)))
                .collect();
            batched_vals.sort();
            let mut per_row_vals = per_row[*row].clone();
            per_row_vals.sort();
            assert_eq!(
                batched_vals, per_row_vals,
                "batched != per-row optional VALUES for required row {row} (star cartesian/miss/null)"
            );
        }
        let batched_rows: std::collections::HashSet<usize> =
            batched.iter().map(|(r, _)| *r).collect();
        for (row, rows) in per_row.iter().enumerate() {
            if !rows.is_empty() {
                assert!(
                    batched_rows.contains(&row),
                    "batched dropped non-miss required row {row}"
                );
            }
        }
    }

    #[test]
    fn test_pattern_optional_builder_with_poisoned() {
        use crate::context::ExecutionContext;
        use crate::var_registry::VarRegistry;
        use fluree_db_core::FlakeValue;
        use fluree_db_core::LedgerSnapshot;

        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let required_schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let optional_pattern = make_optional_pattern();

        let builder = PatternOptionalBuilder::new(
            required_schema.clone(),
            optional_pattern,
            crate::temporal_mode::PlanningContext::current(),
        );

        // Create a batch with Poisoned in position 0 (which is used for correlation)
        let columns_poisoned = vec![
            vec![Binding::Poisoned],
            vec![Binding::lit(
                FlakeValue::String("Alice".to_string()),
                Sid::new(2, "string"),
            )],
        ];
        let batch_poisoned = Batch::new(required_schema.clone(), columns_poisoned).unwrap();

        // Builder should return Ok(None) for poisoned correlation var
        assert!(builder.build(&batch_poisoned, 0, &ctx).unwrap().is_none());

        // Create a batch with normal bindings
        let columns_normal = vec![
            vec![Binding::sid(Sid::new(1, "alice"))],
            vec![Binding::lit(
                FlakeValue::String("Alice".to_string()),
                Sid::new(2, "string"),
            )],
        ];
        let batch_normal = Batch::new(required_schema, columns_normal).unwrap();

        // Builder should return Ok(Some(...)) for normal bindings
        assert!(builder.build(&batch_normal, 0, &ctx).unwrap().is_some());
    }

    #[test]
    fn test_create_poisoned_row() {
        use fluree_db_core::FlakeValue;

        let required_schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let optional_pattern = make_optional_pattern();

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

        let op = OptionalOperator::new(
            Box::new(MockOp),
            required_schema.clone(),
            optional_pattern,
            crate::temporal_mode::PlanningContext::current(),
        );

        // Create a required batch with one row
        let columns = vec![
            vec![Binding::sid(Sid::new(1, "alice"))],
            vec![Binding::lit(
                FlakeValue::String("Alice".to_string()),
                Sid::new(2, "string"),
            )],
        ];
        let batch = Batch::new(required_schema, columns).unwrap();

        let row = op.create_poisoned_row(&batch, 0);

        // Should have 3 columns: ?s, ?name, ?email (Poisoned)
        assert_eq!(row.len(), 3);
        assert!(row[0].is_sid()); // ?s
        assert!(row[1].is_lit()); // ?name
        assert!(row[2].is_poisoned()); // ?email
    }

    #[test]
    fn test_with_builder_constructor() {
        let required_schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let optional_pattern = make_optional_pattern();

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

        // Create using with_builder
        let builder = PatternOptionalBuilder::new(
            required_schema.clone(),
            optional_pattern,
            crate::temporal_mode::PlanningContext::current(),
        );
        let op =
            OptionalOperator::with_builder(Box::new(MockOp), required_schema, Box::new(builder));

        // Should have same schema as new() constructor
        assert_eq!(op.schema().len(), 3);
    }
}
