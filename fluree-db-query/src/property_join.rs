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
use crate::fast_path_common::{
    star_probe_lane_plan, subject_probe_lane_plan, ProbeLanePlan, ProbeOps,
};
use crate::ir::triple::{Ref, Term, TriplePattern};
use crate::join::{
    batched_subject_probe_binary, batched_subject_star_spot, make_dict_overlay,
    SpotStarPredicateParams, SubjectProbeParams,
};
use crate::operator::flush::FlushSchedule;
use crate::operator::inline::{apply_inline, extend_schema, InlineOperator};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::temporal_mode::{PlanningContext, TemporalMode};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::DatatypeConstraint;
use fluree_db_core::{ObjectBounds, Sid, BATCHED_JOIN_SIZE};
use indexmap::IndexMap;
use rustc_hash::FxBuildHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::ops::Range;
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
#[cfg(test)]
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

/// Per-subject state: (subject binding, predicate presence mask, emitted
/// values per emitted predicate). Insertion-ordered, so rows come out in the
/// driver scan's subject order.
///
/// Emission tracks subjects by position (`subject_idx`, `chunk`,
/// `current_subject`), which stays valid only while the map is append-only:
/// each chunk appends driver subjects, and the map is cleared only in `open`
/// and `close`.
type SubjectMap = IndexMap<SubjectKey, (Binding, u64, Vec<Vec<Binding>>), FxBuildHasher>;

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
///
/// # Streaming
///
/// The driver scan is read a chunk of subjects at a time, and each chunk is
/// probed for the other predicates and emitted before the next is read, so an
/// outer `LIMIT` stops the work. That needs a bound-object driver (a subject
/// takes nothing from later driver rows, so a chunk can close anywhere), a
/// subject-probe lane for every other predicate (a chunk costs in proportion
/// to its subjects), and a current-state read. Otherwise the whole driver is
/// one chunk.
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
    /// The subject_binding is preserved from the scan to emit the correct type.
    /// Holds every driver subject read so far, in driver order; a chunk is a
    /// range of it. Earlier chunks stay so a subject the driver repeats is
    /// joined once.
    subject_values: SubjectMap,
    /// Next position in `subject_values` to expand.
    subject_idx: usize,
    /// Positions in `subject_values` of the chunk being emitted.
    chunk: Range<usize>,
    /// Position of the subject currently being expanded into cartesian rows
    /// across batches.
    current_subject: Option<usize>,
    /// Per-emitted-predicate odometer indices for `current_subject`.
    current_indices: Vec<usize>,
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
    /// Binding emitted for an optional predicate with no values.
    unmatched: Binding,
    /// Row budget from an outer `LIMIT`; sizes the first chunk.
    row_budget: Option<usize>,
    /// Predicate indices in read order, the driver first.
    scan_order: Vec<usize>,
    /// Presence bits a subject must collect to produce rows.
    required_mask: u64,
    /// Driver scan, open until exhausted.
    driver: Option<BoxedOperator>,
    /// Driver rows not yet read when the last chunk filled mid-batch.
    driver_pending: Option<(Batch, usize)>,
    /// Size of the next chunk; `None` reads the rest of the driver into one.
    chunk_schedule: Option<FlushSchedule>,
    /// Last encoded driver subject id, to notice a driver that isn't in
    /// subject order.
    last_driver_id: Option<u64>,
    /// How the other predicates are read, planned on the first chunk whose
    /// subjects all have encoded ids.
    lanes: Option<ChunkLanes>,
    stats: ProbeStats,
}

/// How the non-driver predicates are read for a chunk of driver subjects.
enum ChunkLanes {
    /// One SPOT walk over the chunk's subjects covers every predicate.
    SpotStar {
        predicates: Vec<(usize, Sid)>,
        probe_ops: Option<ProbeOps>,
    },
    /// Each predicate on its own lane, in read order.
    PerPredicate(Vec<PredicateLane>),
}

enum PredicateLane {
    /// Batched PSOT probe over the chunk's subjects.
    Probe {
        pred_idx: usize,
        pred_sid: Sid,
        probe_ops: Option<ProbeOps>,
    },
    /// Full predicate scan, keeping rows for the chunk's subjects.
    Scan { pred_idx: usize },
}

impl ChunkLanes {
    fn has_scan(&self) -> bool {
        matches!(self, ChunkLanes::PerPredicate(lanes)
            if lanes.iter().any(|lane| matches!(lane, PredicateLane::Scan { .. })))
    }
}

#[derive(Default)]
struct ProbeStats {
    chunks: u64,
    used_batched_probe: bool,
    used_spot_star_walk: bool,
    probe_chunks: u64,
    probe_subjects_total: u64,
    scan_rows_total: u64,
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
    /// Rank a predicate as the leading (subject-set-seeding) scan.
    ///
    /// All fully-bound objects share the best score; ties resolve to the
    /// lowest index, and predicate order follows the planner's
    /// selectivity-reordered pattern order. This is what lets a specific
    /// bound value (`{id: 4112}`, ~1 row) drive ahead of its `rdf:type`
    /// class pattern (|class| rows) — while a genuinely rare class, which
    /// the planner orders first, still wins the tie.
    ///
    /// Under time-travel replay (`replay`) a bound `rdf:type` object is
    /// hard-preferred instead: a predicate whose rows were all retracted
    /// after `to_t` has no PSOT/POST partition in the current index, so a
    /// direct scan of it silently returns nothing — its historical rows are
    /// only reachable through the subject-keyed SPOT sidecars the batched
    /// probes replay. Driving from the class scan keeps every non-driver
    /// predicate on that replay-correct probe lane.
    fn driver_score(
        predicate: &PropertyJoinPredicate,
        object_bounds: &HashMap<VarId, ObjectBounds>,
        replay: bool,
    ) -> u8 {
        match &predicate.object {
            PropertyJoinObject::Bound(_) if replay && predicate.pred_ref.is_rdf_type() => 0,
            PropertyJoinObject::Bound(_) if replay => 1,
            PropertyJoinObject::Bound(_) => 0,
            PropertyJoinObject::Var(obj_var) if object_bounds.contains_key(obj_var) => 2,
            PropertyJoinObject::Var(_) => 3,
        }
    }

    fn select_driver_predicate(
        predicates: &[PropertyJoinPredicate],
        object_bounds: &HashMap<VarId, ObjectBounds>,
        replay: bool,
    ) -> Option<usize> {
        predicates
            .iter()
            .enumerate()
            .filter(|(_, predicate)| predicate.required)
            .min_by_key(|(idx, predicate)| {
                (Self::driver_score(predicate, object_bounds, replay), *idx)
            })
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

    /// Mark predicate `pred_idx` present on a probed subject, keeping the
    /// object when that predicate is emitted. The probe lanes only run over a
    /// chunk whose keys are all encoded ids, so the id is the key.
    fn ingest_match(
        subject_values: &mut SubjectMap,
        subject_id: u64,
        pred_idx: usize,
        emit_pos: Option<usize>,
        object: Option<Binding>,
    ) {
        if let Some(entry) = subject_values.get_mut(&SubjectKey::Id(subject_id)) {
            entry.1 |= 1u64 << pred_idx;
            if let (Some(epos), Some(object)) = (emit_pos, object) {
                entry.2[epos].push(object);
            }
        }
    }

    /// Sorted encoded ids of the chunk's subjects, or `None` when the probe
    /// lanes can't serve them (any subject without an encoded id).
    fn chunk_subject_ids(
        &self,
        ctx: &ExecutionContext<'_>,
        chunk: Range<usize>,
    ) -> Option<Vec<u64>> {
        if chunk.is_empty() || ctx.binary_store.is_none() {
            return None;
        }
        let mut ids: Vec<u64> = Vec::with_capacity(chunk.len());
        for (key, _) in self.subject_values.get_range(chunk)? {
            if let SubjectKey::Id(s_id) = key {
                ids.push(*s_id);
            } else {
                return None;
            }
        }
        ids.sort_unstable();
        Some(ids)
    }

    fn can_spot_walk_remaining(
        &self,
        ctx: &ExecutionContext<'_>,
        remaining_predicates: &[usize],
    ) -> bool {
        !ctx.is_multi_ledger()
            && ctx.binary_store.is_some()
            && !remaining_predicates.is_empty()
            && remaining_predicates
                .iter()
                .all(|&idx| self.predicates[idx].dtc.is_none())
    }

    /// Choose a lane for every non-driver predicate. The choice depends only
    /// on the predicates and the overlay, so one plan serves every chunk.
    fn plan_lanes(&self, ctx: &ExecutionContext<'_>) -> Result<ChunkLanes> {
        let remaining = &self.scan_order[1..];
        let Some(store) = ctx.binary_store.as_ref() else {
            return Ok(ChunkLanes::PerPredicate(
                remaining
                    .iter()
                    .map(|&pred_idx| PredicateLane::Scan { pred_idx })
                    .collect(),
            ));
        };

        if self.can_spot_walk_remaining(ctx, remaining) {
            let predicates: Vec<(usize, Sid)> = remaining
                .iter()
                .filter_map(|&idx| {
                    let pred_sid = try_normalize_pred_sid(store, &self.predicates[idx].pred_ref)?;
                    Some((idx, pred_sid))
                })
                .collect();
            if predicates.len() == remaining.len() {
                let pred_refs: Vec<&Sid> = predicates.iter().map(|(_, sid)| sid).collect();
                match star_probe_lane_plan(ctx, store, &pred_refs)? {
                    ProbeLanePlan::Decline => {}
                    ProbeLanePlan::Clean => {
                        return Ok(ChunkLanes::SpotStar {
                            predicates,
                            probe_ops: None,
                        })
                    }
                    ProbeLanePlan::Merge(ops) => {
                        return Ok(ChunkLanes::SpotStar {
                            predicates,
                            probe_ops: ProbeOps::new(ops),
                        })
                    }
                }
            }
        }

        // Probes don't replay an unmergeable overlay; those predicates take
        // the overlay-correct per-predicate scan.
        let mut lanes = Vec::with_capacity(remaining.len());
        for &pred_idx in remaining {
            let predicate = &self.predicates[pred_idx];
            let pred_sid = (!ctx.is_multi_ledger() && predicate.dtc.is_none())
                .then(|| try_normalize_pred_sid(store, &predicate.pred_ref))
                .flatten();
            let lane = match pred_sid {
                Some(pred_sid) => match subject_probe_lane_plan(ctx, store, &pred_sid)? {
                    ProbeLanePlan::Decline => PredicateLane::Scan { pred_idx },
                    ProbeLanePlan::Clean => PredicateLane::Probe {
                        pred_idx,
                        pred_sid,
                        probe_ops: None,
                    },
                    ProbeLanePlan::Merge(ops) => PredicateLane::Probe {
                        pred_idx,
                        pred_sid,
                        probe_ops: ProbeOps::new(ops),
                    },
                },
                None => PredicateLane::Scan { pred_idx },
            };
            lanes.push(lane);
        }
        Ok(ChunkLanes::PerPredicate(lanes))
    }

    /// Scan for one predicate: the subject column, plus the object when it is
    /// emitted.
    fn predicate_scan(&self, ctx: &ExecutionContext<'_>, pred_idx: usize) -> BoxedOperator {
        let predicate = &self.predicates[pred_idx];
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
        } else if ctx.default_graphs_slice().is_some_and(|g| g.len() >= 2) {
            // Existence-only over a MULTI-member default union: the
            // DatasetOperator arms the §13.2 set-dedup, whose key
            // needs every VARIABLE column emitted (a pruned object
            // column would collapse distinct triples — the operator
            // now fails loud on that combination). Widen to include
            // the object; the consumer below keys off `emit_obj` and
            // simply ignores the extra column.
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
        make_property_join_scan(pattern, bounds, emit, self.mode)
    }

    /// Read driver rows into `subject_values` until the chunk starting at
    /// `chunk_start` holds as many subjects as the schedule allows, or the
    /// driver is exhausted.
    async fn fill_chunk(&mut self, ctx: &ExecutionContext<'_>, chunk_start: usize) -> Result<()> {
        let driver_idx = self.scan_order[0];
        let emit_pos = self.emit_positions[driver_idx];
        let emit_count = self.emitted_required.len();
        let chunk_full = |op: &Self| {
            op.chunk_schedule
                .is_some_and(|schedule| op.subject_values.len() - chunk_start >= schedule.size())
        };

        while !chunk_full(self) {
            let (batch, start) = match self.driver_pending.take() {
                Some(pending) => pending,
                None => {
                    let Some(scan) = self.driver.as_mut() else {
                        return Ok(());
                    };
                    match scan.next_batch(ctx).await? {
                        Some(batch) => (batch, 0),
                        None => {
                            scan.close();
                            self.driver = None;
                            return Ok(());
                        }
                    }
                }
            };
            // Rows here are priced by the inner scan's emission charge.
            ctx.check_cancelled()?;
            // Schema for this scan is either:
            // - emitted predicate: [subject_var, temp_obj_var]
            // - existence-only:   [subject_var]
            let Some(subjects) = batch.column_by_idx(0) else {
                continue;
            };
            let objects = batch.column_by_idx(1);
            if emit_pos.is_some() && objects.is_none() {
                continue;
            }

            let mut stop = None;
            for row in start..subjects.len() {
                if chunk_full(self) {
                    stop = Some(row);
                    break;
                }
                let subject = &subjects[row];
                let Some(key) = Self::subject_key(ctx, subject)? else {
                    continue;
                };
                // Chunks must be ascending id ranges so they partition the
                // probe lanes' walk; any other driver order is read whole.
                match key {
                    SubjectKey::Id(s_id) => {
                        if self.last_driver_id.is_some_and(|last| s_id < last) {
                            self.chunk_schedule = None;
                        }
                        self.last_driver_id = Some(s_id);
                    }
                    _ => self.chunk_schedule = None,
                }
                let entry = self
                    .subject_values
                    .entry(key)
                    .or_insert_with(|| (subject.clone(), 0u64, vec![Vec::new(); emit_count]));
                entry.1 |= 1u64 << driver_idx;
                if let (Some(epos), Some(objects)) = (emit_pos, objects) {
                    entry.2[epos].push(objects[row].clone());
                }
            }
            let read = stop.unwrap_or(subjects.len()) - start;
            self.stats.scan_rows_total += read as u64;
            if let Some(row) = stop {
                self.driver_pending = Some((batch, row));
            }
        }
        Ok(())
    }

    /// Read the next chunk of driver subjects and look up the other
    /// predicates for it.
    async fn next_chunk(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        // Emitted subjects' values are never read again.
        if let Some(done) = self.subject_values.get_range_mut(self.chunk.clone()) {
            for (_, _, values) in done.values_mut() {
                *values = Vec::new();
            }
        }

        let chunk_start = self.subject_values.len();
        self.fill_chunk(ctx, chunk_start).await?;
        let mut ids = self.chunk_subject_ids(ctx, chunk_start..self.subject_values.len());
        if ids.is_some() && self.lanes.is_none() {
            self.lanes = Some(self.plan_lanes(ctx)?);
        }
        let probes_only = ids.is_some() && !self.lanes.as_ref().is_some_and(ChunkLanes::has_scan);
        if self.chunk_schedule.is_some() && !probes_only {
            // Only probes cost in proportion to the chunk; a scan reads its
            // whole predicate, so run it once over every remaining subject.
            self.chunk_schedule = None;
            self.fill_chunk(ctx, chunk_start).await?;
            ids = self.chunk_subject_ids(ctx, chunk_start..self.subject_values.len());
        }
        self.chunk = chunk_start..self.subject_values.len();
        self.subject_idx = chunk_start;
        if !self.chunk.is_empty() && self.scan_order.len() > 1 {
            self.read_lanes(ctx, self.chunk.clone(), ids).await?;
        }

        if let Some(schedule) = self.chunk_schedule.as_mut() {
            schedule.advance();
        }
        self.stats.chunks += 1;
        Ok(())
    }

    /// Look up every non-driver predicate for the chunk's subjects. `ids` is
    /// `None` when some subject has no encoded id; only scans serve those.
    async fn read_lanes(
        &mut self,
        ctx: &ExecutionContext<'_>,
        chunk: Range<usize>,
        ids: Option<Vec<u64>>,
    ) -> Result<()> {
        if let Some(ids) = ids {
            let mut lanes = self
                .lanes
                .take()
                .ok_or_else(|| QueryError::Internal("property join lanes not planned".into()))?;
            let result = self.read_planned_lanes(ctx, &mut lanes, chunk, &ids).await;
            self.lanes = Some(lanes);
            return result;
        }
        for pos in 1..self.scan_order.len() {
            self.scan_into_chunk(ctx, self.scan_order[pos], chunk.start)
                .await?;
        }
        Ok(())
    }

    async fn read_planned_lanes(
        &mut self,
        ctx: &ExecutionContext<'_>,
        lanes: &mut ChunkLanes,
        chunk: Range<usize>,
        ids: &[u64],
    ) -> Result<()> {
        let store = ctx.binary_store.as_ref().ok_or_else(|| {
            QueryError::Internal("property join probe without a binary store".into())
        })?;
        let dict_overlay = make_dict_overlay(ctx, store);
        match lanes {
            ChunkLanes::SpotStar {
                predicates,
                probe_ops,
            } => {
                let spot_predicates: Vec<SpotStarPredicateParams<'_>> = predicates
                    .iter()
                    .map(|(idx, pred_sid)| {
                        let predicate = &self.predicates[*idx];
                        SpotStarPredicateParams {
                            predicate_idx: *idx,
                            pred_sid: pred_sid.clone(),
                            object_bounds: self.predicate_bounds(predicate),
                            bound_object: Self::predicate_bound_object(predicate),
                            emit_object: self.emit_positions[*idx].is_some(),
                        }
                    })
                    .collect();
                let spot_matches = batched_subject_star_spot(
                    ctx,
                    store,
                    ids,
                    &spot_predicates,
                    dict_overlay.as_ref(),
                    probe_ops.as_mut(),
                )?;
                self.stats.used_spot_star_walk = true;
                self.stats.scan_rows_total += spot_matches.len() as u64;
                for m in spot_matches {
                    Self::ingest_match(
                        &mut self.subject_values,
                        m.subject_id,
                        m.predicate_idx,
                        self.emit_positions[m.predicate_idx],
                        m.object,
                    );
                }
            }
            ChunkLanes::PerPredicate(lanes) => {
                for lane in lanes {
                    match lane {
                        PredicateLane::Probe {
                            pred_idx,
                            pred_sid,
                            probe_ops,
                        } => self.probe_into_chunk(
                            ctx,
                            store,
                            *pred_idx,
                            pred_sid,
                            probe_ops.as_mut(),
                            ids,
                            dict_overlay.as_ref(),
                        )?,
                        PredicateLane::Scan { pred_idx } => {
                            self.scan_into_chunk(ctx, *pred_idx, chunk.start).await?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Batched PSOT probe of one predicate for the chunk's sorted subject ids.
    #[allow(clippy::too_many_arguments)]
    fn probe_into_chunk(
        &mut self,
        ctx: &ExecutionContext<'_>,
        store: &Arc<fluree_db_binary_index::BinaryIndexStore>,
        pred_idx: usize,
        pred_sid: &Sid,
        mut probe_ops: Option<&mut ProbeOps>,
        ids: &[u64],
        dict_overlay: Option<&crate::dict_overlay::DictOverlay>,
    ) -> Result<()> {
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

        let emit_pos = self.emit_positions[pred_idx];
        let mut chunk_start: usize = 0;
        for i in 1..=ids.len() {
            let is_end = i == ids.len();
            let size = i - chunk_start;
            let span = ids[i - 1].saturating_sub(ids[chunk_start]);
            if !(is_end || size >= PROBE_CHUNK_SIZE || span > PROBE_MAX_SPAN) {
                continue;
            }

            let chunk = &ids[chunk_start..i];
            self.stats.used_batched_probe = true;
            self.stats.probe_chunks += 1;
            self.stats.probe_subjects_total += chunk.len() as u64;
            let predicate = &self.predicates[pred_idx];
            let probe_matches = batched_subject_probe_binary(
                ctx,
                store,
                &SubjectProbeParams {
                    pred_sid,
                    subject_ids: chunk,
                    object_bounds: self.predicate_bounds(predicate),
                    bound_object: Self::predicate_bound_object(predicate),
                    emit_object: emit_pos.is_some(),
                    dict_overlay,
                },
                probe_ops.as_deref_mut(),
            )?;
            self.stats.scan_rows_total += probe_matches.len() as u64;
            for m in probe_matches {
                Self::ingest_match(
                    &mut self.subject_values,
                    m.subject_id,
                    pred_idx,
                    emit_pos,
                    m.object,
                );
            }
            chunk_start = i;
        }
        Ok(())
    }

    /// Scan one predicate in full, keeping rows for subjects at or after
    /// `chunk_start` in `subject_values`.
    async fn scan_into_chunk(
        &mut self,
        ctx: &ExecutionContext<'_>,
        pred_idx: usize,
        chunk_start: usize,
    ) -> Result<()> {
        let emit_pos = self.emit_positions[pred_idx];
        let mut scan = self.predicate_scan(ctx, pred_idx);
        scan.open(ctx).await?;
        while let Some(batch) = scan.next_batch(ctx).await? {
            // Rows here are priced by the inner scan's emission charge.
            ctx.check_cancelled()?;
            let Some(subjects) = batch.column_by_idx(0) else {
                continue;
            };
            let objects = batch.column_by_idx(1);
            if emit_pos.is_some() && objects.is_none() {
                continue;
            }
            self.stats.scan_rows_total += batch.len() as u64;
            for (row, subject) in subjects.iter().enumerate() {
                let Some(key) = Self::subject_key(ctx, subject)? else {
                    continue;
                };
                let Some((idx, _, entry)) = self.subject_values.get_full_mut(&key) else {
                    continue;
                };
                if idx < chunk_start {
                    continue;
                }
                entry.1 |= 1u64 << pred_idx;
                if let (Some(epos), Some(objects)) = (emit_pos, objects) {
                    entry.2[epos].push(objects[row].clone());
                }
            }
            ctx.check_cancelled()?;
        }
        scan.close();
        Ok(())
    }

    /// Move to the next subject in the chunk that has a row to emit.
    fn next_subject(&mut self) -> bool {
        while self.subject_idx < self.chunk.end {
            let idx = self.subject_idx;
            self.subject_idx += 1;
            let Some((_, (_, mask, values_per_pred))) = self.subject_values.get_index(idx) else {
                continue;
            };
            if *mask & self.required_mask != self.required_mask
                || !Self::has_cartesian_row(values_per_pred, &self.emitted_required)
            {
                continue;
            }
            self.current_indices = vec![0; values_per_pred.len()];
            self.current_subject = Some(idx);
            return true;
        }
        false
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
        planning: PlanningContext,
    ) -> Result<Self> {
        Self::new_with_options(patterns, &[], object_bounds, None, Vec::new(), planning)
    }

    /// Create a new property-join operator, optionally treating some predicate patterns
    /// as existence-only (semijoin) when their object vars are not needed downstream.
    pub fn new_with_needed_vars(
        patterns: &[TriplePattern],
        object_bounds: HashMap<VarId, ObjectBounds>,
        needed_vars: Option<&std::collections::HashSet<VarId>>,
        planning: PlanningContext,
    ) -> Result<Self> {
        Self::new_with_options(
            patterns,
            &[],
            object_bounds,
            needed_vars,
            Vec::new(),
            planning,
        )
    }

    pub fn new_with_options(
        required_patterns: &[TriplePattern],
        optional_patterns: &[TriplePattern],
        object_bounds: HashMap<VarId, ObjectBounds>,
        needed_vars: Option<&std::collections::HashSet<VarId>>,
        inline_ops: Vec<InlineOperator>,
        planning: PlanningContext,
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
            subject_values: SubjectMap::default(),
            subject_idx: 0,
            chunk: 0..0,
            current_subject: None,
            current_indices: Vec::new(),
            object_bounds,
            emit_positions,
            emitted_required,
            inline_ops,
            mode: planning.mode(),
            unmatched: planning.unmatched_optional.binding(),
            row_budget: None,
            scan_order: Vec::new(),
            required_mask: 0,
            driver: None,
            driver_pending: None,
            chunk_schedule: None,
            last_driver_id: None,
            lanes: None,
            stats: ProbeStats::default(),
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
            return Self::subject_key_multi(ctx, subject);
        }
        // Normalize `Sid` keys to `SubjectKey::Id` whenever the subject
        // resolves (persisted reverse dict first, then DictNovelty). Scan
        // fallbacks emit `Binding::Sid` rows while the batched probes ingest
        // by encoded id — without normalization the same subject would occupy
        // two map entries, and the driver-id capture (which requires an
        // all-`Id` key set) could never engage the batched walks under an
        // overlay.
        //
        // NOT under eager materialization: eager `Sid` bindings (reasoning
        // views with derived-fact overlays, federation) can carry namespace
        // codes from a different snapshot space, so resolving them against
        // this store's dictionary would key the wrong subject. A policy does
        // not gate this: the batched walks an all-`Id` key set enables
        // (`subject_probe_lane_plan` / `star_probe_lane_plan`) decide per
        // predicate whether the view policy can touch them, and decline to
        // the filtered scans when it can.
        let normalizable = !ctx.eager_materialization;
        Ok(Self::subject_key_single(subject).map(|key| match key {
            SubjectKey::Sid(sid) if normalizable => {
                let resolved = ctx.binary_store.as_deref().and_then(|store| {
                    store
                        .find_subject_id_by_parts(sid.namespace_code, &sid.name)
                        .ok()
                        .flatten()
                        .or_else(|| {
                            ctx.dict_novelty
                                .as_ref()
                                .filter(|dn| dn.is_initialized())
                                .and_then(|dn| {
                                    dn.subjects.find_subject(sid.namespace_code, &sid.name)
                                })
                        })
                });
                resolved.map_or(SubjectKey::Sid(sid), SubjectKey::Id)
            }
            other => other,
        }))
    }

    /// Generate cartesian product rows for a given subject
    ///
    /// Takes the collected values for each predicate and produces
    /// all combinations. The subject_binding is cloned into each row.
    #[cfg(test)]
    fn generate_rows(
        output_schema_len: usize,
        subject_binding: &Binding,
        values_per_pred: &[Vec<Binding>],
        emitted_required: &[bool],
        unmatched: &Binding,
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
                    row.push(unmatched.clone());
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

    fn has_cartesian_row(values_per_pred: &[Vec<Binding>], emitted_required: &[bool]) -> bool {
        values_per_pred.iter().enumerate().all(|(idx, values)| {
            !values.is_empty() || !emitted_required.get(idx).copied().unwrap_or(true)
        })
    }

    fn build_row_at_indices(
        output_schema_len: usize,
        subject_binding: &Binding,
        values_per_pred: &[Vec<Binding>],
        emitted_required: &[bool],
        indices: &[usize],
        unmatched: &Binding,
    ) -> Option<Vec<Binding>> {
        if !Self::has_cartesian_row(values_per_pred, emitted_required) {
            return None;
        }

        let mut row = Vec::with_capacity(output_schema_len);
        row.push(subject_binding.clone());
        for (pred_idx, values) in values_per_pred.iter().enumerate() {
            if values.is_empty() {
                row.push(unmatched.clone());
            } else {
                let val_idx = indices.get(pred_idx).copied().unwrap_or(0);
                row.push(values.get(val_idx)?.clone());
            }
        }
        Some(row)
    }

    /// Advance cartesian-product odometer; returns false after the last row.
    fn advance_indices(indices: &mut [usize], values_per_pred: &[Vec<Binding>]) -> bool {
        if indices.is_empty() {
            return false;
        }

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
        !carry
    }
}

#[async_trait]
impl Operator for PropertyJoinOperator {
    fn plan_details(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut m = serde_json::Map::new();
        m.insert("subject".into(), format!("?v{}", self.subject_var.0).into());
        let preds: Vec<serde_json::Value> = self
            .predicates
            .iter()
            .map(|p| {
                let name = match &p.pred_ref {
                    Ref::Sid(sid) => format!("{}:{}", sid.namespace_code, sid.name),
                    Ref::Iri(iri) => iri.to_string(),
                    Ref::Var(v) => format!("?v{}", v.0),
                };
                serde_json::Value::String(name)
            })
            .collect();
        m.insert("predicates".into(), serde_json::Value::Array(preds));
        m
    }

    fn schema(&self) -> &[VarId] {
        &self.output_schema
    }

    /// Sizes the first chunk; the budget isn't forwarded, since a subject
    /// may produce any number of rows.
    fn set_row_budget(&mut self, budget: usize) {
        self.row_budget = Some(budget);
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
            self.subject_idx = 0;
            self.chunk = 0..0;
            self.current_subject = None;
            self.current_indices.clear();
            self.driver_pending = None;
            self.last_driver_id = None;
            self.lanes = None;
            self.stats = ProbeStats::default();

            // presence_mask has one bit per predicate index, regardless of emit flag.
            self.required_mask = if self.predicates.len() >= 64 {
                u64::MAX
            } else {
                self.predicates
                    .iter()
                    .enumerate()
                    .filter(|(_, predicate)| predicate.required)
                    .fold(0u64, |mask, (idx, _)| mask | (1u64 << idx))
            };

            // The driver scan seeds the subject set; every other predicate is
            // then looked up for those subjects only, by a batched subject
            // probe or SPOT star walk when the binary store can serve it, or
            // else by a scan of the whole predicate.
            let replay = ctx
                .binary_store
                .as_ref()
                .is_some_and(|store| ctx.to_t < store.max_t());
            let driver_pred_idx =
                Self::select_driver_predicate(&self.predicates, &self.object_bounds, replay);
            tracing::debug!(
                ?driver_pred_idx,
                replay,
                "property_join: selected driver predicate"
            );

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
            self.scan_order = scan_order;

            let Some(&driver_idx) = self.scan_order.first() else {
                self.driver = None;
                return Ok(());
            };
            // A historical read replays leaflets uncached, so a leaflet split
            // across chunks would be replayed once per chunk.
            let streamable = matches!(
                self.predicates[driver_idx].object,
                PropertyJoinObject::Bound(_)
            ) && !ctx.is_multi_ledger()
                && ctx.binary_store.is_some()
                && !replay;
            self.chunk_schedule = streamable.then(|| match self.row_budget {
                Some(budget) => FlushSchedule::budgeted(budget, BATCHED_JOIN_SIZE),
                None => FlushSchedule::ramped(BATCHED_JOIN_SIZE),
            });
            let mut driver = self.predicate_scan(ctx, driver_idx);
            driver.open(ctx).await?;
            self.driver = Some(driver);
            Ok(())
        }
        .instrument(span)
        .await
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        // Collect rows up to batch size. Per-subject cartesian products are
        // streamed with `current_indices` so high-fanout subjects do not
        // allocate their full product before an outer LIMIT can stop pulling.
        let batch_size = ctx.batch_size;
        let mut all_rows: Vec<Vec<Binding>> = Vec::new();
        let schema_len = self.output_schema.len();

        while all_rows.len() < batch_size {
            if self.current_subject.is_none() && !self.next_subject() {
                // Hand over this chunk's rows before reading another: a
                // selective star may already have satisfied an outer LIMIT.
                if self.driver.is_none() || !all_rows.is_empty() {
                    break;
                }
                self.next_chunk(ctx).await?;
                continue;
            }

            let Some(idx) = self.current_subject else {
                continue;
            };
            let Some((_, (subject_binding, _, values_per_pred))) =
                self.subject_values.get_index(idx)
            else {
                self.current_subject = None;
                self.current_indices.clear();
                continue;
            };

            let row = Self::build_row_at_indices(
                schema_len,
                subject_binding,
                values_per_pred,
                &self.emitted_required,
                &self.current_indices,
                &self.unmatched,
            );
            let has_next = Self::advance_indices(&mut self.current_indices, values_per_pred);
            if !has_next {
                self.current_subject = None;
                self.current_indices.clear();
            }

            let Some(mut row) = row else {
                continue;
            };
            if !apply_inline(&self.inline_ops, &self.output_schema, &mut row, Some(ctx))? {
                continue;
            }
            all_rows.push(row);
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
        if matches!(self.state, OperatorState::Open | OperatorState::Exhausted) {
            tracing::debug!(
                subjects = self.subject_values.len(),
                chunks = self.stats.chunks,
                driver_exhausted = self.driver.is_none(),
                used_batched_probe = self.stats.used_batched_probe,
                used_spot_star_walk = self.stats.used_spot_star_walk,
                probe_chunks = self.stats.probe_chunks,
                probe_subjects_total = self.stats.probe_subjects_total,
                scan_rows_total = self.stats.scan_rows_total,
                "property_join: complete"
            );
        }
        if let Some(mut driver) = self.driver.take() {
            driver.close();
        }
        self.state = OperatorState::Closed;
        self.subject_values.clear();
        self.subject_idx = 0;
        self.chunk = 0..0;
        self.current_subject = None;
        self.current_indices.clear();
        self.driver_pending = None;
        self.lanes = None;
    }

    fn estimated_rows(&self) -> Option<usize> {
        None // Could potentially estimate based on predicate cardinality
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::UnmatchedOptional;
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
        let op = PropertyJoinOperator::new(&patterns, HashMap::new(), PlanningContext::current())
            .unwrap();

        assert_eq!(op.subject_var(), VarId(0));
        assert_eq!(op.predicates.len(), 2);
        assert_eq!(op.output_schema().len(), 3); // subject + 2 object vars
    }

    #[test]
    fn test_property_join_schema() {
        let patterns = make_property_join_patterns();
        let op = PropertyJoinOperator::new(&patterns, HashMap::new(), PlanningContext::current())
            .unwrap();

        let schema = op.output_schema();
        assert_eq!(schema[0], VarId(0)); // subject
        assert_eq!(schema[1], VarId(1)); // name object
        assert_eq!(schema[2], VarId(2)); // age object
    }

    #[test]
    fn test_property_join_schema_with_bound_object_predicate() {
        let patterns = make_property_join_patterns_with_bound_object();
        let op = PropertyJoinOperator::new(&patterns, HashMap::new(), PlanningContext::current())
            .unwrap();

        let schema = op.output_schema();
        assert_eq!(&schema[..], &[VarId(0), VarId(1), VarId(2)]);
    }

    #[test]
    fn test_property_join_prefers_bound_object_driver_over_bounds() {
        let patterns = make_property_join_patterns_with_bound_object();
        let op = PropertyJoinOperator::new(&patterns, HashMap::new(), PlanningContext::current())
            .unwrap();

        let mut bounds = HashMap::new();
        bounds.insert(VarId(2), ObjectBounds::new());

        let driver = PropertyJoinOperator::select_driver_predicate(&op.predicates, &bounds, false);
        assert_eq!(driver, Some(0));
    }

    #[test]
    fn test_property_join_driver_follows_planner_order_for_bound_objects() {
        // Post-reorder pattern order is [value-bound, rdf:type]: the specific
        // bound value must drive, not the class pattern (PERF-1: a
        // `(:User {id: $id})` star label-scanned because rdf:type outranked
        // the ~1-row id seek).
        let rdf_type = || Ref::Iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type".into());
        let patterns = vec![
            TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Sid(Sid::new(100, "id")),
                Term::Value(fluree_db_core::value::FlakeValue::Long(4112)),
            ),
            TriplePattern::new(
                Ref::Var(VarId(0)),
                rdf_type(),
                Term::Sid(Sid::new(100, "User")),
            ),
        ];
        let op = PropertyJoinOperator::new(&patterns, HashMap::new(), PlanningContext::current())
            .unwrap();
        let driver =
            PropertyJoinOperator::select_driver_predicate(&op.predicates, &HashMap::new(), false);
        assert_eq!(driver, Some(0), "specific bound value should drive");

        // Under time-travel replay the class pattern must reclaim the driver:
        // a fully-retracted value predicate has no PSOT/POST partition in the
        // current index, so it can only be probed via SPOT sidecar replay.
        let driver =
            PropertyJoinOperator::select_driver_predicate(&op.predicates, &HashMap::new(), true);
        assert_eq!(driver, Some(1), "rdf:type should drive under replay");

        // When the planner orders the class first (rare class more selective
        // than the bound value), the tie-break preserves that choice.
        let patterns = vec![
            TriplePattern::new(
                Ref::Var(VarId(0)),
                rdf_type(),
                Term::Sid(Sid::new(100, "RareClass")),
            ),
            TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Sid(Sid::new(100, "status")),
                Term::Value(fluree_db_core::value::FlakeValue::String("active".into())),
            ),
        ];
        let op = PropertyJoinOperator::new(&patterns, HashMap::new(), PlanningContext::current())
            .unwrap();
        let driver =
            PropertyJoinOperator::select_driver_predicate(&op.predicates, &HashMap::new(), false);
        assert_eq!(driver, Some(0), "planner-first class should keep driving");
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
        let op = PropertyJoinOperator::new(&patterns, HashMap::new(), PlanningContext::current())
            .unwrap();

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
            &Binding::Unbound,
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 3);
        assert!(matches!(&rows[0][0], Binding::Sid { sid: s, .. } if *s == subject_sid));
    }

    #[test]
    fn test_generate_rows_cartesian_product() {
        let patterns = make_property_join_patterns();
        let op = PropertyJoinOperator::new(&patterns, HashMap::new(), PlanningContext::current())
            .unwrap();

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
            &Binding::Unbound,
        );
        // Cartesian product: 2 * 3 = 6 rows
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn test_streaming_cartesian_odometer() {
        let subject_binding = Binding::sid(Sid::new(1, "alice"));
        let values = vec![
            vec![
                Binding::sid(Sid::new(200, "Alice")),
                Binding::sid(Sid::new(201, "Alicia")),
            ],
            vec![
                Binding::sid(Sid::new(300, "30")),
                Binding::sid(Sid::new(301, "31")),
                Binding::sid(Sid::new(302, "32")),
            ],
        ];
        let mut indices = vec![0; values.len()];
        let mut rows = 0;
        loop {
            let row = PropertyJoinOperator::build_row_at_indices(
                3,
                &subject_binding,
                &values,
                &[true, true],
                &indices,
                &Binding::Unbound,
            )
            .expect("indices should produce a row");
            assert_eq!(row.len(), 3);
            rows += 1;
            if !PropertyJoinOperator::advance_indices(&mut indices, &values) {
                break;
            }
        }
        assert_eq!(rows, 6);
    }

    #[test]
    fn test_generate_rows_empty_pred() {
        let patterns = make_property_join_patterns();
        let op = PropertyJoinOperator::new(&patterns, HashMap::new(), PlanningContext::current())
            .unwrap();

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
            &Binding::Unbound,
        );
        // No rows if any predicate is missing
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_generate_rows_missing_optional_uses_unmatched_binding() {
        let subject_binding = Binding::sid(Sid::new(1, "alice"));
        let values = vec![
            vec![Binding::sid(Sid::new(200, "Alice"))], // required name
            vec![],                                     // optional probability
        ];

        for unmatched in [UnmatchedOptional::Unbound, UnmatchedOptional::Poisoned] {
            let rows = PropertyJoinOperator::generate_rows(
                3,
                &subject_binding,
                &values,
                &[true, false],
                &unmatched.binding(),
            );
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][2], unmatched.binding());
        }
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
        let result =
            PropertyJoinOperator::new(&patterns, HashMap::new(), PlanningContext::current());
        assert!(
            result.is_err(),
            "should reject invalid property-join patterns"
        );
    }
}
