//! Property-join operator for same-subject multi-predicate patterns
//!
//! The `PropertyJoinOperator` optimizes queries where multiple triple patterns
//! share the same subject variable and have bound predicates, with either variable
//! objects or bound existence checks.
//!
//! # Example Pattern
//!
//! ```text
//! ?s :name ?name
//! ?s :age ?age
//! ?s :email ?email
//! ```
//!
//! # Semantics
//!
//! PropertyJoinOperator produces a **cartesian product** across properties when
//! predicates are multi-cardinality. For example, if a subject has 2 names and
//! 3 emails, the operator produces 6 rows (not 1 row with nested values).
//! This matches SPARQL solution-set semantics.
//!
//! # Index Usage
//!
//! Uses PSOT index for each predicate scan, which is optimal for
//! "get all subjects with predicate P" queries.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::try_normalize_pred_sid;
use crate::ir::triple::{Ref, Term, TriplePattern};
use crate::join::{
    batched_subject_probe_binary, batched_subject_star_spot, make_dict_overlay,
    BatchedSpotStarMatch, BatchedSubjectProbeMatch, SpotStarPredicateParams, SubjectProbeParams,
};
use crate::operator::inline::{apply_inline, extend_schema, InlineOperator};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::temporal_mode::TemporalMode;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::DatatypeConstraint;
use fluree_db_core::{ObjectBounds, Sid};
use rustc_hash::FxHashMap;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tracing::Instrument;

use crate::binary_scan::EmitMask;

/// Internal temp var for object position in predicate scans.
///
/// We use VarId(u16::MAX - 1) as a sentinel value for temporary object variables
/// in internal scans. This is safe because:
/// 1. We only access scan results by column index, not by VarId
/// 2. VarRegistry panics if > 65534 vars are registered (u16::MAX - 1)
/// 3. This var never escapes to external schemas or user code
const TEMP_OBJECT_VAR: VarId = VarId(u16::MAX - 1);

/// Safety cap for cartesian product row generation. Prevents unbounded memory
/// allocation when predicates have extreme cardinality (e.g. 100k × 100k).
const MAX_CARTESIAN_ROWS: usize = 10_000_000;

fn make_property_join_scan(
    pattern: TriplePattern,
    bounds: Option<ObjectBounds>,
    emit: EmitMask,
    mode: TemporalMode,
) -> BoxedOperator {
    Box::new(crate::dataset_operator::DatasetOperator::scan(
        pattern,
        bounds,
        Vec::new(),
        emit,
        None,
        mode,
    ))
}

/// Property-join operator for same-subject multi-predicate patterns
///
/// Optimizes queries of the form:
/// ```text
/// ?s rdf:type :Deal
/// ?s :pred1 ?obj1
/// ?s :pred2 ?obj2
/// ...
/// ```
///
/// Where all patterns share the same subject variable.
///
/// # Multi-Ledger Support
///
/// In dataset mode, subjects are keyed by canonical IRI (`Arc<str>`) to ensure
/// correct cross-ledger joins. The operator accepts both `Binding::Sid` (single-ledger)
/// and `Binding::IriMatch` (multi-ledger) from scans and emits the appropriate
/// binding type in output rows.
pub struct PropertyJoinOperator {
    /// The shared subject variable
    subject_var: VarId,
    /// Predicates and their object requirements.
    predicates: Vec<PropertyJoinPredicate>,
    /// Output schema: [subject_var, obj_var_1, obj_var_2, ...]
    output_schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
    /// Collected values per subject, keyed by a join-safe subject key.
    ///
    /// - Single-ledger: prefer raw encoded subject IDs (no decoding)
    /// - Dataset/multi-ledger: use canonical IRI strings (cross-ledger safe)
    ///
    /// Value tuple: (subject_binding, vec of value-vectors per predicate).
    /// The subject_binding is preserved from the scan to emit the correct type.
    subject_values: FxHashMap<SubjectKey, (Binding, Vec<Vec<Binding>>)>,
    /// Subjects to process (collected after filtering)
    pending_subjects: Vec<SubjectKey>,
    /// Current index into pending_subjects
    subject_idx: usize,
    /// Optional object bounds for range filter pushdown (VarId -> ObjectBounds)
    object_bounds: HashMap<VarId, ObjectBounds>,
    /// For each predicate index, the position in `subject_values`'s values vec if emitted.
    /// Existence-only predicates are `None`.
    emit_positions: Vec<Option<usize>>,
    /// Whether each emitted position comes from a required predicate.
    emitted_required: Vec<bool>,
    /// Row-local filters/binds applied after star rows are assembled.
    inline_ops: Vec<InlineOperator>,
    /// Temporal mode captured at planner-time for the late per-predicate scans.
    mode: TemporalMode,
}

#[derive(Clone, Debug)]
enum PropertyJoinObject {
    /// Variable object that may or may not be emitted.
    Var(VarId),
    /// Bound object used as an existence constraint (for example `?s rdf:type :Class`).
    Bound(Term),
}

#[derive(Clone, Debug)]
struct PropertyJoinPredicate {
    pred_ref: Ref,
    object: PropertyJoinObject,
    dtc: Option<DatatypeConstraint>,
    emit_object: bool,
    required: bool,
}

/// Join-safe subject key for PropertyJoinOperator.
///
/// This avoids eagerly decoding subjects to canonical IRI strings in single-ledger mode,
/// preserving the late-materialization benefits of `Binding::EncodedSid`.
#[derive(Clone, Debug, Eq)]
enum SubjectKey {
    /// Single-ledger: raw subject/ref ID from the binary index (`Binding::EncodedSid`)
    Id(u64),
    /// Single-ledger (range/overlay paths): already-materialized SID
    Sid(Sid),
    /// Multi-ledger (dataset): canonical IRI string
    Iri(Arc<str>),
}

impl PartialEq for SubjectKey {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SubjectKey::Id(a), SubjectKey::Id(b)) => a == b,
            (SubjectKey::Sid(a), SubjectKey::Sid(b)) => {
                a.namespace_code == b.namespace_code && a.name == b.name
            }
            (SubjectKey::Iri(a), SubjectKey::Iri(b)) => a == b,
            _ => false,
        }
    }
}

impl Hash for SubjectKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // discriminant
        match self {
            SubjectKey::Id(v) => {
                0u8.hash(state);
                v.hash(state);
            }
            SubjectKey::Sid(s) => {
                1u8.hash(state);
                s.namespace_code.hash(state);
                s.name.hash(state);
            }
            SubjectKey::Iri(i) => {
                2u8.hash(state);
                i.hash(state);
            }
        }
    }
}

impl PropertyJoinOperator {
    fn driver_score(
        predicate: &PropertyJoinPredicate,
        object_bounds: &HashMap<VarId, ObjectBounds>,
    ) -> u8 {
        match &predicate.object {
            PropertyJoinObject::Bound(_) if predicate.pred_ref.is_rdf_type() => 0,
            PropertyJoinObject::Bound(_) => 1,
            PropertyJoinObject::Var(obj_var) if object_bounds.contains_key(obj_var) => 2,
            PropertyJoinObject::Var(_) => 3,
        }
    }

    fn select_driver_predicate(
        predicates: &[PropertyJoinPredicate],
        object_bounds: &HashMap<VarId, ObjectBounds>,
    ) -> Option<usize> {
        predicates
            .iter()
            .enumerate()
            .filter(|(_, predicate)| predicate.required)
            .min_by_key(|(idx, predicate)| (Self::driver_score(predicate, object_bounds), *idx))
            .map(|(idx, _)| idx)
    }

    fn predicate_bounds<'a>(
        &'a self,
        predicate: &'a PropertyJoinPredicate,
    ) -> Option<&'a ObjectBounds> {
        match &predicate.object {
            PropertyJoinObject::Var(obj_var) => self.object_bounds.get(obj_var),
            PropertyJoinObject::Bound(_) => None,
        }
    }

    fn predicate_bound_object(predicate: &PropertyJoinPredicate) -> Option<&Term> {
        match &predicate.object {
            PropertyJoinObject::Bound(term) => Some(term),
            PropertyJoinObject::Var(_) => None,
        }
    }

    fn ingest_probe_match(
        &self,
        ctx: &ExecutionContext<'_>,
        all_subject_values: &mut FxHashMap<SubjectKey, (Binding, u64, Vec<Vec<Binding>>)>,
        pred_idx: usize,
        probe_match: BatchedSubjectProbeMatch,
    ) -> Result<()> {
        let subject = Binding::encoded_sid(probe_match.subject_id);
        if let Some(key) = Self::subject_key(ctx, &subject)? {
            if let Some(entry) = all_subject_values.get_mut(&key) {
                entry.1 |= 1u64 << pred_idx;
                if let (Some(epos), Some(object)) =
                    (self.emit_positions[pred_idx], probe_match.object)
                {
                    entry.2[epos].push(object);
                }
            }
        }
        Ok(())
    }

    fn ingest_spot_star_match(
        &self,
        ctx: &ExecutionContext<'_>,
        all_subject_values: &mut FxHashMap<SubjectKey, (Binding, u64, Vec<Vec<Binding>>)>,
        spot_match: BatchedSpotStarMatch,
    ) -> Result<()> {
        let subject = Binding::encoded_sid(spot_match.subject_id);
        if let Some(key) = Self::subject_key(ctx, &subject)? {
            if let Some(entry) = all_subject_values.get_mut(&key) {
                entry.1 |= 1u64 << spot_match.predicate_idx;
                if let (Some(epos), Some(object)) = (
                    self.emit_positions[spot_match.predicate_idx],
                    spot_match.object,
                ) {
                    entry.2[epos].push(object);
                }
            }
        }
        Ok(())
    }

    fn capture_driver_subject_ids(
        &self,
        ctx: &ExecutionContext<'_>,
        order_pos: usize,
        all_subject_values: &FxHashMap<SubjectKey, (Binding, u64, Vec<Vec<Binding>>)>,
    ) -> Option<Vec<u64>> {
        if order_pos != 0 || ctx.binary_store.is_none() {
            return None;
        }

        let mut ids: Vec<u64> = Vec::with_capacity(all_subject_values.len());
        for key in all_subject_values.keys() {
            if let SubjectKey::Id(s_id) = key {
                ids.push(*s_id);
            } else {
                return None;
            }
        }
        (!ids.is_empty()).then_some(ids)
    }

    fn can_spot_walk_remaining(
        &self,
        ctx: &ExecutionContext<'_>,
        driver_subject_ids: &Option<Vec<u64>>,
        remaining_predicates: &[usize],
    ) -> bool {
        driver_subject_ids.is_some()
            && !ctx.is_multi_ledger()
            && ctx.binary_store.is_some()
            && !remaining_predicates.is_empty()
            && remaining_predicates
                .iter()
                .all(|&idx| self.predicates[idx].dtc.is_none())
    }

    /// Create a new property-join operator from patterns
    ///
    /// # Arguments
    ///
    /// * `patterns` - Triple patterns forming a property-join shape
    /// * `object_bounds` - Optional range bounds for object variables (filter pushdown)
    ///
    /// # Errors
    ///
    /// Returns `QueryError::Internal` if patterns don't form a valid property-join shape.
    pub fn new(
        patterns: &[TriplePattern],
        object_bounds: HashMap<VarId, ObjectBounds>,
        mode: TemporalMode,
    ) -> Result<Self> {
        Self::new_with_options(patterns, &[], object_bounds, None, Vec::new(), mode)
    }

    /// Create a new property-join operator, optionally treating some predicate patterns
    /// as existence-only (semijoin) when their object vars are not needed downstream.
    pub fn new_with_needed_vars(
        patterns: &[TriplePattern],
        object_bounds: HashMap<VarId, ObjectBounds>,
        needed_vars: Option<&std::collections::HashSet<VarId>>,
        mode: TemporalMode,
    ) -> Result<Self> {
        Self::new_with_options(patterns, &[], object_bounds, needed_vars, Vec::new(), mode)
    }

    pub fn new_with_options(
        required_patterns: &[TriplePattern],
        optional_patterns: &[TriplePattern],
        object_bounds: HashMap<VarId, ObjectBounds>,
        needed_vars: Option<&std::collections::HashSet<VarId>>,
        inline_ops: Vec<InlineOperator>,
        mode: TemporalMode,
    ) -> Result<Self> {
        if !crate::planner::is_property_join(required_patterns) {
            return Err(QueryError::Internal(
                "Patterns must form a property-join shape".into(),
            ));
        }

        let mut all_patterns = required_patterns.to_vec();
        all_patterns.extend_from_slice(optional_patterns);
        if !crate::planner::is_property_join(&all_patterns) {
            return Err(QueryError::Internal(
                "Required and optional patterns must form a property-join shape".into(),
            ));
        }

        // Extract subject var (guaranteed same for all by is_property_join)
        let subject_var = match &required_patterns[0].s {
            Ref::Var(v) => *v,
            _ => {
                return Err(QueryError::Internal(
                    "Property-join requires variable subject".into(),
                ))
            }
        };

        // Extract predicate/object requirements. Predicates can be Ref::Sid or Ref::Iri
        // depending on how the query was lowered. Bound objects are kept as existence-only
        // constraints so same-subject stars like `?s rdf:type :Class ; :name ?name` can still
        // use the property-join path.
        let mut predicates = Vec::with_capacity(all_patterns.len());
        for (required, patterns) in [(true, required_patterns), (false, optional_patterns)] {
            for p in patterns {
                let pred_ref = match &p.p {
                    Ref::Sid(_) | Ref::Iri(_) => p.p.clone(),
                    _ => {
                        return Err(QueryError::Internal(
                            "Property-join requires bound predicates (Sid or Iri)".into(),
                        ))
                    }
                };
                let (object, emit_object) = match &p.o {
                    Term::Var(v) => {
                        let emit = needed_vars.is_none_or(|n| n.contains(v));
                        (PropertyJoinObject::Var(*v), emit)
                    }
                    _ => (PropertyJoinObject::Bound(p.o.clone()), false),
                };
                predicates.push(PropertyJoinPredicate {
                    pred_ref,
                    object,
                    dtc: p.dtc.clone(),
                    emit_object,
                    required,
                });
            }
        }

        // Build output schema: [subject_var, obj_var_1, obj_var_2, ...] but only for emitted vars.
        let mut schema_vec = vec![subject_var];
        let mut emitted_required = Vec::new();
        for predicate in &predicates {
            if predicate.emit_object {
                let PropertyJoinObject::Var(obj_var) = &predicate.object else {
                    return Err(QueryError::Internal(
                        "property-join cannot emit a bound object".into(),
                    ));
                };
                schema_vec.push(*obj_var);
                emitted_required.push(predicate.required);
            }
        }
        let output_schema: Arc<[VarId]> =
            Arc::from(extend_schema(&schema_vec, &inline_ops).into_boxed_slice());

        let emit_positions = {
            let mut out = Vec::with_capacity(predicates.len());
            let mut next = 0usize;
            for predicate in &predicates {
                if predicate.emit_object {
                    out.push(Some(next));
                    next += 1;
                } else {
                    out.push(None);
                }
            }
            out
        };

        Ok(Self {
            subject_var,
            predicates,
            output_schema,
            state: OperatorState::Created,
            subject_values: FxHashMap::default(),
            pending_subjects: Vec::new(),
            subject_idx: 0,
            object_bounds,
            emit_positions,
            emitted_required,
            inline_ops,
            mode,
        })
    }

    /// Get the subject variable
    pub fn subject_var(&self) -> VarId {
        self.subject_var
    }

    /// Get the output schema (non-trait method for tests)
    pub fn output_schema(&self) -> &Arc<[VarId]> {
        &self.output_schema
    }

    fn subject_key_single(subject: &Binding) -> Option<SubjectKey> {
        match subject {
            Binding::EncodedSid { s_id, .. } => Some(SubjectKey::Id(*s_id)),
            Binding::Sid { sid, .. } => Some(SubjectKey::Sid(sid.clone())),
            Binding::IriMatch { primary_sid, .. } => Some(SubjectKey::Sid(primary_sid.clone())),
            Binding::Iri(iri) => Some(SubjectKey::Iri(iri.clone())),
            _ => None,
        }
    }

    fn subject_key_multi(
        ctx: &ExecutionContext<'_>,
        subject: &Binding,
    ) -> Result<Option<SubjectKey>> {
        Ok(match subject {
            Binding::IriMatch { iri, .. } => Some(SubjectKey::Iri(iri.clone())),
            Binding::Iri(iri) => Some(SubjectKey::Iri(iri.clone())),
            Binding::Sid { sid, .. } => {
                // In dataset mode, use canonical IRI strings as join keys.
                // Prefer decoding within the active ledger when available.
                let Some(iri) = ctx
                    .active_ledger_id()
                    .and_then(|addr| ctx.decode_sid_in_ledger(sid, addr))
                    .or_else(|| ctx.decode_sid(sid))
                else {
                    return Ok(None);
                };
                Some(SubjectKey::Iri(Arc::from(iri)))
            }
            Binding::EncodedSid { s_id, .. } => {
                // Resolve to canonical IRI for cross-ledger comparison.
                // Novelty-aware via ctx.resolve_subject_iri().
                match ctx.resolve_subject_iri(*s_id) {
                    Some(Ok(iri)) => Some(SubjectKey::Iri(Arc::from(iri))),
                    Some(Err(e)) => {
                        tracing::debug!(
                            s_id,
                            error = %e,
                            "property join failed to resolve encoded subject"
                        );
                        return Err(crate::error::QueryError::dictionary_lookup(format!(
                            "property join subject key: resolve subject IRI for s_id={s_id}: {e}"
                        )));
                    }
                    None => None,
                }
            }
            _ => None,
        })
    }

    fn subject_key(ctx: &ExecutionContext<'_>, subject: &Binding) -> Result<Option<SubjectKey>> {
        if ctx.is_multi_ledger() {
            Self::subject_key_multi(ctx, subject)
        } else {
            Ok(Self::subject_key_single(subject))
        }
    }

    /// Generate cartesian product rows for a given subject
    ///
    /// Takes the collected values for each predicate and produces
    /// all combinations. The subject_binding is cloned into each row.
    fn generate_rows(
        output_schema_len: usize,
        subject_binding: &Binding,
        values_per_pred: &[Vec<Binding>],
        emitted_required: &[bool],
    ) -> Vec<Vec<Binding>> {
        // If no object vars are emitted (existence-only predicates), then each matching
        // subject produces exactly one output row.
        if values_per_pred.is_empty() {
            return vec![{
                let mut row = Vec::with_capacity(output_schema_len);
                row.push(subject_binding.clone());
                row
            }];
        }

        // Calculate total combinations (using saturating multiply to avoid overflow
        // on extremely high-cardinality predicates).
        let total: usize = values_per_pred
            .iter()
            .enumerate()
            .fold(1usize, |acc, (idx, values)| {
                let factor = if values.is_empty() {
                    usize::from(!emitted_required.get(idx).copied().unwrap_or(true))
                } else {
                    values.len()
                };
                acc.saturating_mul(factor)
            });
        if total == 0 {
            return Vec::new();
        }

        let mut rows = Vec::with_capacity(total.min(MAX_CARTESIAN_ROWS));

        // Generate cartesian product using indices
        let mut indices: Vec<usize> = vec![0; values_per_pred.len()];

        loop {
            // Build current row
            let mut row = Vec::with_capacity(output_schema_len);
            row.push(subject_binding.clone());
            for (pred_idx, val_idx) in indices.iter().enumerate() {
                if values_per_pred[pred_idx].is_empty() {
                    row.push(Binding::Poisoned);
                } else {
                    row.push(values_per_pred[pred_idx][*val_idx].clone());
                }
            }
            rows.push(row);
            if rows.len() >= MAX_CARTESIAN_ROWS {
                break;
            }

            // Increment indices (like odometer)
            let mut carry = true;
            for i in (0..indices.len()).rev() {
                if carry {
                    indices[i] += 1;
                    let width = if values_per_pred[i].is_empty() {
                        1
                    } else {
                        values_per_pred[i].len()
                    };
                    if indices[i] >= width {
                        indices[i] = 0;
                    } else {
                        carry = false;
                    }
                }
            }

            if carry {
                // Wrapped all the way around
                break;
            }
        }

        rows
    }
}

#[async_trait]
impl Operator for PropertyJoinOperator {
    fn schema(&self) -> &[VarId] {
        &self.output_schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        let span = tracing::debug_span!(
            "property_join_open",
            predicates = self.predicates.len(),
            multi_ledger = ctx.is_multi_ledger(),
            has_binary_store = ctx.binary_store.is_some(),
            has_bounds = !self.object_bounds.is_empty(),
        );
        async {
            self.state = OperatorState::Open;
            self.subject_values.clear();
            self.pending_subjects.clear();
            self.subject_idx = 0;

            // For each predicate, scan and collect (subject -> values) mappings.
            // Key by a join-safe subject key (encoded IDs in single-ledger mode).
            //
            // Optimization: if a predicate has object bounds (range filter pushdown),
            // scan it first to get a selective subject set, then use that as a semi-join
            // driver for subsequent predicates in single-ledger binary mode.
            //
            // This turns a common "date filter + vector predicate" workload from:
            //   scan(date with bounds) + scan(vec full) + intersect
            // into:
            //   scan(date with bounds) + batched probe(vec for matching subjects)
            //
            // NOTE: The batched probe path currently requires:
            // - single-ledger (no dataset)
            // - binary_store present
            // - no datatype constraint on the probed predicate (dt=None)
            // Map: subject -> (subject_binding, presence_mask, emitted_values)
            // presence_mask has one bit per predicate index, regardless of emit flag.
            let mut all_subject_values: FxHashMap<SubjectKey, (Binding, u64, Vec<Vec<Binding>>)> =
                FxHashMap::default();
            let required_mask: u64 = if self.predicates.len() >= 64 {
                u64::MAX
            } else {
                self.predicates
                    .iter()
                    .enumerate()
                    .filter(|(_, predicate)| predicate.required)
                    .fold(0u64, |mask, (idx, _)| mask | (1u64 << idx))
            };

            let driver_pred_idx =
                Self::select_driver_predicate(&self.predicates, &self.object_bounds);
            tracing::debug!(?driver_pred_idx, "property_join: selected driver predicate");

            let mut scan_order: Vec<usize> = self
                .predicates
                .iter()
                .enumerate()
                .filter(|(_, predicate)| predicate.required)
                .map(|(idx, _)| idx)
                .collect();
            if let Some(d) = driver_pred_idx {
                if let Some(driver_pos) = scan_order.iter().position(|idx| *idx == d) {
                    scan_order.swap(0, driver_pos);
                }
            }
            scan_order.extend(
                self.predicates
                    .iter()
                    .enumerate()
                    .filter(|(_, predicate)| !predicate.required)
                    .map(|(idx, _)| idx),
            );

            let mut driver_subject_ids: Option<Vec<u64>> = None;
            let mut used_batched_probe = false;
            let mut used_spot_star_walk = false;
            let mut probe_chunks: u64 = 0;
            let mut probe_subjects_total: u64 = 0;
            let mut scan_rows_total: u64 = 0;
            let emit_count = self.emit_positions.iter().flatten().count();

            for (order_pos, pred_idx) in scan_order.iter().copied().enumerate() {
                let predicate = &self.predicates[pred_idx];

                // If we have a driver subject set and we're in the right execution mode,
                // try a batched subject probe for this predicate.
                // Batched probe requires binary store with batched_lookup support.
                let can_batched_probe = order_pos > 0
                    && driver_subject_ids.is_some()
                    && !ctx.is_multi_ledger()
                    && ctx.binary_store.is_some()
                    && predicate.dtc.is_none();

                if can_batched_probe {
                    let store = ctx.binary_store.as_ref().unwrap();
                    let pred_sid = try_normalize_pred_sid(store, &predicate.pred_ref);

                    if let Some(pred_sid) = pred_sid {
                        let subject_ids = driver_subject_ids.as_ref().unwrap();
                        if !subject_ids.is_empty() {
                            let dict_overlay = make_dict_overlay(ctx, store);
                            // IMPORTANT: Batched join uses the min/max s_id range of the left batch
                            // to decide which leaf files/leaflets to scan. If the subject IDs are
                            // sparse across the full id space, a single huge batch can still scan
                            // nearly the entire predicate partition.
                            //
                            // To improve locality, chunk the subject IDs into smaller sorted ranges
                            // and probe each chunk independently. We split both by count and by
                            // span to avoid scanning large gaps.
                            const PROBE_CHUNK_SIZE: usize = 256;
                            const PROBE_MAX_SPAN: u64 = 100_000;

                            let mut ids = subject_ids.clone();
                            ids.sort_unstable();

                            let mut chunk_start: usize = 0;
                            for i in 1..=ids.len() {
                                let is_end = i == ids.len();
                                let size = i - chunk_start;
                                let span = if size == 0 {
                                    0
                                } else {
                                    ids[i - 1].saturating_sub(ids[chunk_start])
                                };
                                let should_split =
                                    is_end || size >= PROBE_CHUNK_SIZE || span > PROBE_MAX_SPAN;
                                if !should_split {
                                    continue;
                                }

                                let chunk = &ids[chunk_start..i];
                                if chunk.is_empty() {
                                    continue;
                                }
                                used_batched_probe = true;
                                probe_chunks += 1;
                                probe_subjects_total += chunk.len() as u64;
                                let emit_obj = self.emit_positions[pred_idx].is_some();
                                let probe_matches = batched_subject_probe_binary(
                                    ctx,
                                    store,
                                    &SubjectProbeParams {
                                        pred_sid: &pred_sid,
                                        subject_ids: chunk,
                                        object_bounds: self.predicate_bounds(predicate),
                                        bound_object: Self::predicate_bound_object(predicate),
                                        emit_object: emit_obj,
                                        dict_overlay: dict_overlay.as_ref(),
                                    },
                                )?;
                                scan_rows_total += probe_matches.len() as u64;
                                for probe_match in probe_matches {
                                    self.ingest_probe_match(
                                        ctx,
                                        &mut all_subject_values,
                                        pred_idx,
                                        probe_match,
                                    )?;
                                }
                                chunk_start = i;
                            }

                            continue;
                        }
                    }
                }

                // Create pattern: ?s :pred ?o (temp var for object, accessed by index)
                // pred_ref is already a Ref (Sid or Iri) so use it directly.
                let (object, bounds) = match &predicate.object {
                    PropertyJoinObject::Var(obj_var) => (
                        Term::Var(TEMP_OBJECT_VAR),
                        self.object_bounds.get(obj_var).cloned(),
                    ),
                    PropertyJoinObject::Bound(term) => (term.clone(), None),
                };
                let pattern = TriplePattern {
                    s: Ref::Var(self.subject_var),
                    p: predicate.pred_ref.clone(),
                    o: object,
                    dtc: predicate.dtc.clone(),
                };

                // Create scan with optional bounds pushdown for this object variable.
                //
                // `DatasetOperator` wraps the scan for multi-graph fanout;
                // inner `BinaryScanOperator` selects between binary cursor
                // and range fallback at open() time.
                let emit = if predicate.emit_object {
                    // Subject + object (no predicate column) for emitted predicates.
                    EmitMask {
                        s: true,
                        p: false,
                        o: true,
                    }
                } else {
                    // Existence-only: only need the subject column.
                    EmitMask {
                        s: true,
                        p: false,
                        o: false,
                    }
                };
                let mut scan: BoxedOperator =
                    make_property_join_scan(pattern, bounds, emit, self.mode);
                scan.open(ctx).await?;

                while let Some(batch) = scan.next_batch(ctx).await? {
                    // Schema for this scan is either:
                    // - emitted predicate: [subject_var, temp_obj_var]
                    // - existence-only:   [subject_var]
                    let subject_col = batch.column_by_idx(0);
                    let object_col = batch.column_by_idx(1);

                    if let Some(subjects) = subject_col {
                        scan_rows_total += batch.len() as u64;
                        let emit_obj = self.emit_positions[pred_idx].is_some();
                        if emit_obj {
                            if let Some(objects) = object_col {
                                for (subject, object) in subjects.iter().zip(objects.iter()) {
                                    if let Some(key) = Self::subject_key(ctx, subject)? {
                                        if order_pos > 0 && !all_subject_values.is_empty() {
                                            if let Some(entry) = all_subject_values.get_mut(&key) {
                                                entry.1 |= 1u64 << pred_idx;
                                                if let Some(epos) = self.emit_positions[pred_idx] {
                                                    entry.2[epos].push(object.clone());
                                                }
                                            }
                                            continue;
                                        }

                                        let entry =
                                            all_subject_values.entry(key).or_insert_with(|| {
                                                (
                                                    subject.clone(),
                                                    0u64,
                                                    vec![Vec::new(); emit_count],
                                                )
                                            });
                                        entry.1 |= 1u64 << pred_idx;
                                        if let Some(epos) = self.emit_positions[pred_idx] {
                                            entry.2[epos].push(object.clone());
                                        }
                                    }
                                }
                            }
                        } else {
                            // Existence-only: only update presence bit for subjects already tracked,
                            // unless this is the first scan (map empty) where we can seed subjects.
                            for subject in subjects {
                                if let Some(key) = Self::subject_key(ctx, subject)? {
                                    if order_pos > 0 && !all_subject_values.is_empty() {
                                        if let Some(entry) = all_subject_values.get_mut(&key) {
                                            entry.1 |= 1u64 << pred_idx;
                                        }
                                        continue;
                                    }
                                    let entry =
                                        all_subject_values.entry(key).or_insert_with(|| {
                                            (subject.clone(), 0u64, vec![Vec::new(); emit_count])
                                        });
                                    entry.1 |= 1u64 << pred_idx;
                                }
                            }
                        }
                    }
                }

                scan.close();

                // After the first scan, capture subject IDs for batched probing when the
                // subject keys stayed as encoded IDs. This lets a selective exact scan like
                // `?s rdf:type :Deal` drive later subject-bound probes.
                driver_subject_ids =
                    self.capture_driver_subject_ids(ctx, order_pos, &all_subject_values);

                if order_pos == 0 {
                    let remaining_predicates = &scan_order[1..];
                    if self.can_spot_walk_remaining(ctx, &driver_subject_ids, remaining_predicates)
                    {
                        let store = ctx.binary_store.as_ref().unwrap();
                        let dict_overlay = make_dict_overlay(ctx, store);
                        let spot_predicates: Vec<_> = remaining_predicates
                            .iter()
                            .filter_map(|&idx| {
                                let predicate = &self.predicates[idx];
                                let pred_sid = try_normalize_pred_sid(store, &predicate.pred_ref)?;
                                Some(SpotStarPredicateParams {
                                    predicate_idx: idx,
                                    pred_sid,
                                    object_bounds: self.predicate_bounds(predicate),
                                    bound_object: Self::predicate_bound_object(predicate),
                                    emit_object: self.emit_positions[idx].is_some(),
                                })
                            })
                            .collect();

                        if spot_predicates.len() == remaining_predicates.len()
                            && !spot_predicates.is_empty()
                        {
                            used_spot_star_walk = true;
                            let spot_matches = batched_subject_star_spot(
                                ctx,
                                store,
                                driver_subject_ids.as_ref().unwrap(),
                                &spot_predicates,
                                dict_overlay.as_ref(),
                            )?;
                            scan_rows_total += spot_matches.len() as u64;
                            for spot_match in spot_matches {
                                self.ingest_spot_star_match(
                                    ctx,
                                    &mut all_subject_values,
                                    spot_match,
                                )?;
                            }
                            break;
                        }
                    }
                }
            }

            // Filter to only subjects that have values for ALL predicates
            self.subject_values = all_subject_values
                .into_iter()
                .filter(|(_, (_sb, mask, values))| {
                    (*mask & required_mask) == required_mask
                        && (emit_count == 0
                            || values
                                .iter()
                                .enumerate()
                                .all(|(idx, v)| !self.emitted_required[idx] || !v.is_empty()))
                })
                .map(|(k, (sb, _mask, values))| (k, (sb, values)))
                .collect();

            // Collect subjects for iteration
            self.pending_subjects = self.subject_values.keys().cloned().collect();

            tracing::debug!(
                subjects = self.pending_subjects.len(),
                used_batched_probe,
                used_spot_star_walk,
                probe_chunks,
                probe_subjects_total,
                scan_rows_total,
                "property_join: open complete"
            );

            Ok(())
        }
        .instrument(span)
        .await
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        // Collect rows for multiple subjects up to batch size
        let batch_size = ctx.batch_size;
        let mut all_rows: Vec<Vec<Binding>> = Vec::new();

        let schema_len = self.output_schema.len();

        while all_rows.len() < batch_size && self.subject_idx < self.pending_subjects.len() {
            let subject_key = &self.pending_subjects[self.subject_idx];
            self.subject_idx += 1;

            if let Some((subject_binding, values_per_pred)) = self.subject_values.get(subject_key) {
                let rows = Self::generate_rows(
                    schema_len,
                    subject_binding,
                    values_per_pred,
                    &self.emitted_required,
                );
                for mut row in rows {
                    if !apply_inline(&self.inline_ops, &self.output_schema, &mut row, Some(ctx))? {
                        continue;
                    }
                    all_rows.push(row);
                    if all_rows.len() >= batch_size {
                        break;
                    }
                }
            }
        }

        if all_rows.is_empty() {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }

        // Convert rows to columnar batch
        let num_cols = self.output_schema.len();
        let mut columns: Vec<Vec<Binding>> = (0..num_cols).map(|_| Vec::new()).collect();

        for row in all_rows {
            for (col_idx, val) in row.into_iter().enumerate() {
                columns[col_idx].push(val);
            }
        }

        Ok(Some(Batch::new(self.output_schema.clone(), columns)?))
    }

    fn close(&mut self) {
        self.state = OperatorState::Closed;
        self.subject_values.clear();
        self.pending_subjects.clear();
        self.subject_idx = 0;
    }

    fn estimated_rows(&self) -> Option<usize> {
        None // Could potentially estimate based on predicate cardinality
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::Sid;

    fn make_property_join_patterns() -> Vec<TriplePattern> {
        vec![
            TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Sid(Sid::new(100, "name")),
                Term::Var(VarId(1)),
            ),
            TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Sid(Sid::new(101, "age")),
                Term::Var(VarId(2)),
            ),
        ]
    }

    fn make_property_join_patterns_with_bound_object() -> Vec<TriplePattern> {
        vec![
            TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Sid(Sid::new(100, "type")),
                Term::Sid(Sid::new(100, "Deal")),
            ),
            TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Sid(Sid::new(100, "name")),
                Term::Var(VarId(1)),
            ),
            TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Sid(Sid::new(101, "stage")),
                Term::Var(VarId(2)),
            ),
        ]
    }

    #[test]
    fn test_property_join_creation() {
        let patterns = make_property_join_patterns();
        let op =
            PropertyJoinOperator::new(&patterns, HashMap::new(), TemporalMode::Current).unwrap();

        assert_eq!(op.subject_var(), VarId(0));
        assert_eq!(op.predicates.len(), 2);
        assert_eq!(op.output_schema().len(), 3); // subject + 2 object vars
    }

    #[test]
    fn test_property_join_schema() {
        let patterns = make_property_join_patterns();
        let op =
            PropertyJoinOperator::new(&patterns, HashMap::new(), TemporalMode::Current).unwrap();

        let schema = op.output_schema();
        assert_eq!(schema[0], VarId(0)); // subject
        assert_eq!(schema[1], VarId(1)); // name object
        assert_eq!(schema[2], VarId(2)); // age object
    }

    #[test]
    fn test_property_join_schema_with_bound_object_predicate() {
        let patterns = make_property_join_patterns_with_bound_object();
        let op =
            PropertyJoinOperator::new(&patterns, HashMap::new(), TemporalMode::Current).unwrap();

        let schema = op.output_schema();
        assert_eq!(&schema[..], &[VarId(0), VarId(1), VarId(2)]);
    }

    #[test]
    fn test_property_join_prefers_bound_object_driver_over_bounds() {
        let patterns = make_property_join_patterns_with_bound_object();
        let op =
            PropertyJoinOperator::new(&patterns, HashMap::new(), TemporalMode::Current).unwrap();

        let mut bounds = HashMap::new();
        bounds.insert(VarId(2), ObjectBounds::new());

        let driver = PropertyJoinOperator::select_driver_predicate(&op.predicates, &bounds);
        assert_eq!(driver, Some(0));
    }

    #[test]
    fn test_subject_key_single_prefers_encoded_ids() {
        // Single-ledger mode should not require IRI decoding for EncodedSid.
        let key = PropertyJoinOperator::subject_key_single(&Binding::encoded_sid(42));
        assert!(matches!(key, Some(SubjectKey::Id(42))));
    }

    #[test]
    fn test_generate_rows_single_values() {
        let patterns = make_property_join_patterns();
        let op =
            PropertyJoinOperator::new(&patterns, HashMap::new(), TemporalMode::Current).unwrap();

        let subject_sid = Sid::new(1, "alice");
        let subject_binding = Binding::sid(subject_sid.clone());
        let values = vec![
            vec![Binding::sid(Sid::new(200, "Alice"))], // name
            vec![Binding::sid(Sid::new(201, "30"))],    // age
        ];

        let rows = PropertyJoinOperator::generate_rows(
            op.output_schema().len(),
            &subject_binding,
            &values,
            &[true, true],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 3);
        assert!(matches!(&rows[0][0], Binding::Sid { sid: s, .. } if *s == subject_sid));
    }

    #[test]
    fn test_generate_rows_cartesian_product() {
        let patterns = make_property_join_patterns();
        let op =
            PropertyJoinOperator::new(&patterns, HashMap::new(), TemporalMode::Current).unwrap();

        let subject_binding = Binding::sid(Sid::new(1, "alice"));
        let values = vec![
            vec![
                Binding::sid(Sid::new(200, "Alice")),
                Binding::sid(Sid::new(201, "Alicia")),
            ], // 2 names
            vec![
                Binding::sid(Sid::new(300, "30")),
                Binding::sid(Sid::new(301, "31")),
                Binding::sid(Sid::new(302, "32")),
            ], // 3 ages
        ];

        let rows = PropertyJoinOperator::generate_rows(
            op.output_schema().len(),
            &subject_binding,
            &values,
            &[true, true],
        );
        // Cartesian product: 2 * 3 = 6 rows
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn test_generate_rows_empty_pred() {
        let patterns = make_property_join_patterns();
        let op =
            PropertyJoinOperator::new(&patterns, HashMap::new(), TemporalMode::Current).unwrap();

        let subject_binding = Binding::sid(Sid::new(1, "alice"));
        let values = vec![
            vec![Binding::sid(Sid::new(200, "Alice"))], // has name
            vec![],                                     // no age
        ];

        let rows = PropertyJoinOperator::generate_rows(
            op.output_schema().len(),
            &subject_binding,
            &values,
            &[true, true],
        );
        // No rows if any predicate is missing
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_generate_rows_missing_optional_uses_poisoned() {
        let subject_binding = Binding::sid(Sid::new(1, "alice"));
        let values = vec![
            vec![Binding::sid(Sid::new(200, "Alice"))], // required name
            vec![],                                     // optional probability
        ];

        let rows =
            PropertyJoinOperator::generate_rows(3, &subject_binding, &values, &[true, false]);
        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0][2], Binding::Poisoned));
    }

    #[test]
    fn test_required_mask_allows_optional_bits() {
        let required_mask = 0b0011u64;
        let actual_mask = 0b1111u64;
        assert_eq!(actual_mask & required_mask, required_mask);
    }

    #[test]
    fn test_property_join_rejects_invalid_patterns() {
        // Different subjects - not a valid property-join
        let patterns = vec![
            TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Sid(Sid::new(100, "name")),
                Term::Var(VarId(1)),
            ),
            TriplePattern::new(
                Ref::Var(VarId(2)), // Different subject!
                Ref::Sid(Sid::new(101, "age")),
                Term::Var(VarId(3)),
            ),
        ];
        let result = PropertyJoinOperator::new(&patterns, HashMap::new(), TemporalMode::Current);
        assert!(
            result.is_err(),
            "should reject invalid property-join patterns"
        );
    }
}
