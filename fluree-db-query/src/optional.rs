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
use crate::ir::Pattern;
use crate::join::{
    batched_subject_probe_binary, BindInstruction, PatternPosition, SubjectProbeParams,
    UnifyInstruction,
};
use crate::operator::{
    compute_trimmed_vars, effective_schema, trim_batch, BoxedOperator, Operator, OperatorState,
};
use crate::seed::SeedOperator;
use crate::temporal_mode::PlanningContext;
use crate::ir::triple::{Ref, Term, TriplePattern};
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

    /// Optionally build all remaining required rows in the current batch in one pass.
    ///
    /// Builders can override this to implement batched probe paths for hot
    /// correlated OPTIONAL shapes. Default: no batched execution.
    fn build_batch(
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
        let pattern_vars = pattern.variables();
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
        let pattern_vars = pattern.variables();

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
            Binding::Sid { sid, .. } => store
                .find_subject_id_by_parts(sid.namespace_code, &sid.name)
                .map_err(|e| QueryError::execution(format!("find_subject_id_by_parts: {e}"))),
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
                    }
                }
            }
        }

        Ok(pattern)
    }
}

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

    fn build_batch(
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

        let probe_matches = batched_subject_probe_binary(
            ctx,
            store,
            &SubjectProbeParams {
                pred_sid: &pred_sid,
                subject_ids: &subject_ids,
                object_bounds: None,
                bound_object: (!matches!(&self.pattern.o, Term::Var(_))).then_some(&self.pattern.o),
                emit_object: emit_object_var.is_some(),
                dict_overlay: None,
            },
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
                Binding::Grouped(_) => Ok(None),
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
            Binding::Sid { sid, .. } => store
                .find_subject_id_by_parts(sid.namespace_code, &sid.name)
                .map_err(|e| QueryError::execution(format!("find_subject_id_by_parts: {e}"))),
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

impl OptionalBuilder for GroupedPatternOptionalBuilder {
    fn build(
        &self,
        required_batch: &Batch,
        row: usize,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<BoxedOperator>> {
        self.build_fallback_chain(required_batch, row, ctx)
    }

    fn build_batch(
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
            for v in p.variables() {
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
                    if let Some(batched_pending) = self.optional_builder.build_batch(
                        required_batch,
                        self.current_required_row,
                        ctx,
                    )? {
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
                            if !opt_batch.is_empty() {
                                optional_batches.push(opt_batch);
                            }
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
