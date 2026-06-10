//! Targeted physical operator for small cyclic fixed-predicate BGPs.
//!
//! This is intentionally narrower than a general leapfrog triejoin. It covers the
//! Wikidata/WGPB stress shapes that currently fall through to left-deep nested-loop
//! joins: triangles and 4-edge cycles whose joins are all ref-valued subject/object
//! variables. Unsupported shapes keep using the existing fallback operator tree.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    allow_cursor_fast_path, build_head_cursor_for_single_subject, build_psot_cursor_for_predicate,
    cursor_projection_sid_otype_okey, fast_path_store, normalize_pred_sid,
};
use crate::ir::triple::{Ref, TriplePattern};
use crate::object_binding::late_materialized_object_binding;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::plan_node::PlanChild;
use crate::temporal_mode::TemporalMode;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::o_type::OType;
use fluree_db_core::{PropertyStatData, StatsView};
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

const OUTPUT_BATCH_SIZE: usize = 1024;
const DEFAULT_MAX_PREDICATE_ROWS: u64 = 10_000_000;
const DEFAULT_MAX_SQUARE_WEDGE_PAIRS: usize = 5_000_000;
const DEFAULT_MAX_BOUNDED_PROBE_SUBJECTS: usize = 65_536;
const DEFAULT_BOUNDED_PROBE_SCAN_RATIO: u64 = 64;

fn cyclic_bgp_enabled() -> bool {
    !matches!(
        std::env::var("FLUREE_CYCLIC_BGP"),
        Ok(v) if v == "0" || v.eq_ignore_ascii_case("false")
    )
}

fn max_predicate_rows() -> u64 {
    std::env::var("FLUREE_CYCLIC_BGP_MAX_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MAX_PREDICATE_ROWS)
}

fn max_square_wedge_pairs() -> usize {
    std::env::var("FLUREE_CYCLIC_BGP_MAX_WEDGE_PAIRS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MAX_SQUARE_WEDGE_PAIRS)
}

fn max_bounded_probe_subjects() -> usize {
    std::env::var("FLUREE_CYCLIC_BGP_MAX_BOUNDED_SUBJECTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MAX_BOUNDED_PROBE_SUBJECTS)
}

fn bounded_probe_scan_ratio() -> u64 {
    std::env::var("FLUREE_CYCLIC_BGP_PROBE_SCAN_RATIO")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_BOUNDED_PROBE_SCAN_RATIO)
}

/// Probe an edge per-subject instead of scanning its whole relation only when
/// the frontier is small enough AND the estimated scan is at least
/// `bounded_probe_scan_ratio()` rows per probe. A stats-absent estimate means
/// the predicate is likely empty (populated stats omit only empty predicates),
/// so a full scan is already trivially cheap — don't probe.
fn should_probe_edge(frontier_len: usize, estimate: Option<u64>) -> bool {
    frontier_len <= max_bounded_probe_subjects()
        && estimate.is_some_and(|est| {
            (frontier_len as u64).saturating_mul(bounded_probe_scan_ratio()) <= est
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CyclicBgpShape {
    Triangle,
    Square,
}

impl CyclicBgpShape {
    fn as_str(self) -> &'static str {
        match self {
            CyclicBgpShape::Triangle => "triangle",
            CyclicBgpShape::Square => "square",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CyclicJoinMode {
    RefOnly,
    EncodedObject,
}

impl CyclicJoinMode {
    fn as_str(self) -> &'static str {
        match self {
            CyclicJoinMode::RefOnly => "iri-ref",
            CyclicJoinMode::EncodedObject => "encoded",
        }
    }
}

#[derive(Debug, Clone)]
struct CyclicEdge {
    subject: VarId,
    object: VarId,
    predicate: Ref,
    estimate: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct CyclicBgpPlan {
    shape: CyclicBgpShape,
    vars: Arc<[VarId]>,
    edges: Arc<[CyclicEdge]>,
}

impl CyclicBgpPlan {
    pub(crate) fn shape_name(&self) -> &'static str {
        self.shape.as_str()
    }
}

fn property_stats<'a>(stats: &'a StatsView, pred: &Ref) -> Option<&'a PropertyStatData> {
    if let Some(sid) = pred.as_sid() {
        return stats.get_property(sid);
    }
    if let Some(iri) = pred.as_iri() {
        return stats.get_property_by_iri(iri);
    }
    None
}

/// Detect a simple triangle or 4-cycle over fixed-predicate ref joins.
pub(crate) fn analyze_cyclic_bgp(
    triples: &[TriplePattern],
    stats: Option<&StatsView>,
) -> Option<CyclicBgpPlan> {
    if !cyclic_bgp_enabled() || !(triples.len() == 3 || triples.len() == 4) {
        return None;
    }

    let mut vars: Vec<VarId> = Vec::new();
    let mut degree: FxHashMap<VarId, usize> = FxHashMap::default();
    let mut edges = Vec::with_capacity(triples.len());
    let row_cap = max_predicate_rows();

    for tp in triples {
        if tp.dtc.is_some() || tp.p.as_var().is_some() {
            return None;
        }
        let (Some(subject), Some(object)) = (tp.s.as_var(), tp.o.as_var()) else {
            return None;
        };
        if subject == object {
            return None;
        }
        for v in [subject, object] {
            if !vars.contains(&v) {
                vars.push(v);
            }
            *degree.entry(v).or_insert(0) += 1;
        }
        let estimate = stats
            .and_then(|s| property_stats(s, &tp.p))
            .map(|p| p.count);
        if estimate.is_some_and(|count| count > row_cap) {
            return None;
        }
        edges.push(CyclicEdge {
            subject,
            object,
            predicate: tp.p.clone(),
            estimate,
        });
    }

    if vars.len() != triples.len() || degree.values().any(|count| *count != 2) {
        return None;
    }

    // Ensure the variable graph is one connected cycle, not two disjoint 2-edge
    // components that happen to have degree two.
    let mut seen = FxHashSet::default();
    let mut stack = vec![vars[0]];
    while let Some(v) = stack.pop() {
        if !seen.insert(v) {
            continue;
        }
        for edge in &edges {
            if edge.subject == v && !seen.contains(&edge.object) {
                stack.push(edge.object);
            } else if edge.object == v && !seen.contains(&edge.subject) {
                stack.push(edge.subject);
            }
        }
    }
    if seen.len() != vars.len() {
        return None;
    }

    Some(CyclicBgpPlan {
        shape: if triples.len() == 3 {
            CyclicBgpShape::Triangle
        } else {
            CyclicBgpShape::Square
        },
        vars: Arc::from(vars.into_boxed_slice()),
        edges: Arc::from(edges.into_boxed_slice()),
    })
}

#[derive(Clone, Copy, Debug)]
struct RawEdgeRow {
    subject: u64,
    o_type: u16,
    object: u64,
    p_id: u32,
}

#[derive(Clone, Copy, Debug)]
struct RefEdgeRow {
    subject: u64,
    object: u64,
}

struct RefRelationIndex {
    edge: CyclicEdge,
    rows: Vec<RefEdgeRow>,
    by_subject: FxHashMap<u64, Vec<u64>>,
    by_object: FxHashMap<u64, Vec<u64>>,
    pairs: FxHashSet<(u64, u64)>,
}

impl RefRelationIndex {
    fn new(edge: CyclicEdge, rows: Vec<RefEdgeRow>) -> Self {
        let mut by_subject: FxHashMap<u64, Vec<u64>> = FxHashMap::default();
        let mut by_object: FxHashMap<u64, Vec<u64>> = FxHashMap::default();
        let mut pairs: FxHashSet<(u64, u64)> = FxHashSet::default();
        for row in &rows {
            by_subject.entry(row.subject).or_default().push(row.object);
            by_object.entry(row.object).or_default().push(row.subject);
            pairs.insert((row.subject, row.object));
        }
        Self {
            edge,
            rows,
            by_subject,
            by_object,
            pairs,
        }
    }

    fn distinct_subjects(&self) -> usize {
        self.by_subject.len()
    }

    fn distinct_objects(&self) -> usize {
        self.by_object.len()
    }
}

#[derive(Clone, Debug)]
struct EdgeRow {
    subject: u64,
    object: Binding,
}

struct RelationIndex {
    edge: CyclicEdge,
    rows: Vec<EdgeRow>,
    by_subject: FxHashMap<u64, Vec<Binding>>,
    by_object: FxHashMap<Binding, Vec<u64>>,
    pairs: FxHashSet<(u64, Binding)>,
}

impl RelationIndex {
    fn new(edge: CyclicEdge, rows: Vec<EdgeRow>) -> Self {
        let mut by_subject: FxHashMap<u64, Vec<Binding>> = FxHashMap::default();
        let mut by_object: FxHashMap<Binding, Vec<u64>> = FxHashMap::default();
        let mut pairs: FxHashSet<(u64, Binding)> = FxHashSet::default();
        for row in &rows {
            by_subject
                .entry(row.subject)
                .or_default()
                .push(row.object.clone());
            by_object
                .entry(row.object.clone())
                .or_default()
                .push(row.subject);
            pairs.insert((row.subject, row.object.clone()));
        }
        Self {
            edge,
            rows,
            by_subject,
            by_object,
            pairs,
        }
    }

    fn distinct_subjects(&self) -> usize {
        self.by_subject.len()
    }

    fn distinct_objects(&self) -> usize {
        self.by_object.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WedgePairKey(Binding, Binding);

#[derive(Debug, Clone)]
struct EncodedSquareWedgePlan {
    build_center: VarId,
    probe_center: VarId,
    key_a: VarId,
    key_b: VarId,
    build_edge_a: usize,
    build_edge_b: usize,
    probe_edge_a: usize,
    probe_edge_b: usize,
    build_pairs: usize,
    probe_pairs: usize,
}

struct EncodedProbeWedgeCursor {
    center: Binding,
    values_a: Vec<Binding>,
    values_b: Vec<Binding>,
    a_pos: usize,
    b_pos: usize,
    matches: Vec<Binding>,
    match_pos: usize,
}

struct EncodedSquareWedgeState {
    plan: EncodedSquareWedgePlan,
    table: FxHashMap<WedgePairKey, Vec<Binding>>,
    probe_centers: Vec<Binding>,
    probe_center_pos: usize,
    current: Option<EncodedProbeWedgeCursor>,
}

pub(crate) struct CyclicBgpOperator {
    plan: CyclicBgpPlan,
    join_mode: CyclicJoinMode,
    schema: Arc<[VarId]>,
    schema_positions: Arc<[usize]>,
    mode: TemporalMode,
    state: OperatorState,
    fallback: Option<BoxedOperator>,
    ref_relations: Vec<RefRelationIndex>,
    relations: Vec<RelationIndex>,
    driver_idx: usize,
    driver_pos: usize,
    ref_pending: VecDeque<Vec<u64>>,
    pending: VecDeque<Vec<Binding>>,
    square_wedge: Option<EncodedSquareWedgeState>,
    probed_edges: usize,
    used_fast_path: bool,
    raw_relation_rows: usize,
    pruned_relation_rows: usize,
}

impl CyclicBgpOperator {
    fn log_fast_path_bail(&self, reason: &'static str, edge: Option<&CyclicEdge>) {
        tracing::debug!(
            reason,
            shape = self.plan.shape_name(),
            mode = self.join_mode.as_str(),
            predicate = edge.map(|e| predicate_display(&e.predicate)),
            "cyclic bgp fast path bail"
        );
    }

    pub(crate) fn new(
        plan: CyclicBgpPlan,
        required_where_vars: Option<&[VarId]>,
        mode: TemporalMode,
        fallback: BoxedOperator,
    ) -> Self {
        let required: Option<HashSet<VarId>> =
            required_where_vars.map(|vars| vars.iter().copied().collect());
        let mut schema = Vec::new();
        let mut schema_positions = Vec::new();
        for (idx, var) in plan.vars.iter().copied().enumerate() {
            if required.as_ref().is_none_or(|r| r.contains(&var)) {
                schema.push(var);
                schema_positions.push(idx);
            }
        }
        let join_mode = if plan.edges.iter().any(|edge| {
            !plan
                .edges
                .iter()
                .any(|candidate| candidate.subject == edge.object)
        }) {
            CyclicJoinMode::EncodedObject
        } else {
            CyclicJoinMode::RefOnly
        };
        Self {
            plan,
            join_mode,
            schema: Arc::from(schema.into_boxed_slice()),
            schema_positions: Arc::from(schema_positions.into_boxed_slice()),
            mode,
            state: OperatorState::Created,
            fallback: Some(fallback),
            ref_relations: Vec::new(),
            relations: Vec::new(),
            driver_idx: 0,
            driver_pos: 0,
            ref_pending: VecDeque::new(),
            pending: VecDeque::new(),
            square_wedge: None,
            probed_edges: 0,
            used_fast_path: false,
            raw_relation_rows: 0,
            pruned_relation_rows: 0,
        }
    }

    fn var_pos(&self, v: VarId) -> usize {
        self.plan
            .vars
            .iter()
            .position(|candidate| *candidate == v)
            .expect("cyclic plan edge var must be in plan vars")
    }

    fn var_is_subject(&self, v: VarId) -> bool {
        self.plan.edges.iter().any(|edge| edge.subject == v)
    }

    fn object_binding_for_edge(&self, edge: &CyclicEdge, raw: RawEdgeRow) -> Option<Binding> {
        if self.var_is_subject(edge.object) {
            if raw.o_type != OType::IRI_REF.as_u16() {
                return None;
            }
            Some(Binding::encoded_sid(raw.object))
        } else {
            late_materialized_object_binding(raw.o_type, raw.object, raw.p_id, 0, u32::MAX, None)
        }
    }

    fn scan_ref_relation(
        &self,
        ctx: &ExecutionContext<'_>,
        edge: &CyclicEdge,
    ) -> Result<Option<Vec<RefEdgeRow>>> {
        let Some(store) = ctx.binary_store.as_ref() else {
            self.log_fast_path_bail("missing-binary-store", Some(edge));
            return Ok(None);
        };
        let pred_sid = normalize_pred_sid(store, &edge.predicate)?;
        let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
            return Ok(Some(Vec::new()));
        };
        let mut cursor = match build_psot_cursor_for_predicate(
            ctx,
            store,
            ctx.binary_g_id,
            pred_sid,
            p_id,
            cursor_projection_sid_otype_okey(),
        )? {
            Some(cursor) => cursor,
            None => {
                self.log_fast_path_bail("cursor-unavailable", Some(edge));
                return Ok(None);
            }
        };

        let row_cap = max_predicate_rows();
        let mut rows = Vec::new();
        while let Some(batch) = cursor
            .next_batch()
            .map_err(|e| QueryError::Internal(format!("cyclic bgp ref cursor: {e}")))?
        {
            for row in 0..batch.row_count {
                if batch.o_type.get(row) != OType::IRI_REF.as_u16() {
                    self.log_fast_path_bail("non-ref-object-in-ref-mode", Some(edge));
                    return Ok(None);
                }
                rows.push(RefEdgeRow {
                    subject: batch.s_id.get(row),
                    object: batch.o_key.get(row),
                });
                if rows.len() as u64 > row_cap {
                    self.log_fast_path_bail("predicate-row-cap-exceeded", Some(edge));
                    return Ok(None);
                }
            }
        }
        Ok(Some(rows))
    }

    fn scan_relation(
        &self,
        ctx: &ExecutionContext<'_>,
        edge: &CyclicEdge,
    ) -> Result<Option<Vec<EdgeRow>>> {
        let Some(store) = ctx.binary_store.as_ref() else {
            self.log_fast_path_bail("missing-binary-store", Some(edge));
            return Ok(None);
        };
        let pred_sid = normalize_pred_sid(store, &edge.predicate)?;
        let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
            return Ok(Some(Vec::new()));
        };
        let mut cursor = match build_psot_cursor_for_predicate(
            ctx,
            store,
            ctx.binary_g_id,
            pred_sid,
            p_id,
            cursor_projection_sid_otype_okey(),
        )? {
            Some(cursor) => cursor,
            None => {
                self.log_fast_path_bail("cursor-unavailable", Some(edge));
                return Ok(None);
            }
        };

        let row_cap = max_predicate_rows();
        let mut raw_rows = Vec::new();
        while let Some(batch) = cursor
            .next_batch()
            .map_err(|e| QueryError::Internal(format!("cyclic bgp cursor: {e}")))?
        {
            for row in 0..batch.row_count {
                raw_rows.push(RawEdgeRow {
                    subject: batch.s_id.get(row),
                    o_type: batch.o_type.get(row),
                    object: batch.o_key.get(row),
                    p_id,
                });
                if raw_rows.len() as u64 > row_cap {
                    self.log_fast_path_bail("predicate-row-cap-exceeded", Some(edge));
                    return Ok(None);
                }
            }
        }
        let mut rows = Vec::with_capacity(raw_rows.len());
        for raw in raw_rows {
            let Some(object) = self.object_binding_for_edge(edge, raw) else {
                self.log_fast_path_bail("unsupported-object-binding", Some(edge));
                return Ok(None);
            };
            rows.push(EdgeRow {
                subject: raw.subject,
                object,
            });
        }
        Ok(Some(rows))
    }

    fn scan_relation_for_subjects(
        &self,
        ctx: &ExecutionContext<'_>,
        edge: &CyclicEdge,
        subjects: &FxHashSet<u64>,
    ) -> Result<Option<Vec<EdgeRow>>> {
        let Some(store) = ctx.binary_store.as_ref() else {
            self.log_fast_path_bail("missing-binary-store", Some(edge));
            return Ok(None);
        };
        let pred_sid = normalize_pred_sid(store, &edge.predicate)?;
        let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
            return Ok(Some(Vec::new()));
        };

        let mut ordered_subjects: Vec<u64> = subjects.iter().copied().collect();
        ordered_subjects.sort_unstable();

        let row_cap = max_predicate_rows();
        let projection = cursor_projection_sid_otype_okey();
        let mut raw_rows = Vec::new();
        for subject in ordered_subjects {
            let Some(mut cursor) = build_head_cursor_for_single_subject(
                store,
                ctx.binary_g_id,
                p_id,
                projection,
                subject,
                ctx.to_t,
            ) else {
                // None means the PSOT branch is unavailable, never "subject has
                // no rows" — decline the fast path rather than dropping rows.
                self.log_fast_path_bail("cursor-unavailable", Some(edge));
                return Ok(None);
            };
            while let Some(batch) = cursor
                .next_batch()
                .map_err(|e| QueryError::Internal(format!("cyclic bgp bounded cursor: {e}")))?
            {
                for row in 0..batch.row_count {
                    debug_assert_eq!(batch.s_id.get(row), subject);
                    raw_rows.push(RawEdgeRow {
                        subject,
                        o_type: batch.o_type.get(row),
                        object: batch.o_key.get(row),
                        p_id,
                    });
                    if raw_rows.len() as u64 > row_cap {
                        self.log_fast_path_bail("predicate-row-cap-exceeded", Some(edge));
                        return Ok(None);
                    }
                }
            }
        }

        let mut rows = Vec::with_capacity(raw_rows.len());
        for raw in raw_rows {
            let Some(object) = self.object_binding_for_edge(edge, raw) else {
                self.log_fast_path_bail("unsupported-object-binding", Some(edge));
                return Ok(None);
            };
            rows.push(EdgeRow {
                subject: raw.subject,
                object,
            });
        }
        Ok(Some(rows))
    }

    /// O(1) upper bound on the frontier size for `var`: the smallest distinct
    /// count among scanned relations incident to it. `None` when no scanned
    /// relation is incident. Lets the probe loop reject over-cap candidates
    /// without materializing any value set.
    fn frontier_size_bound(relations: &[RelationIndex], var: VarId) -> Option<usize> {
        relations
            .iter()
            .filter_map(|rel| {
                if rel.edge.subject == var {
                    Some(rel.distinct_subjects())
                } else if rel.edge.object == var {
                    Some(rel.distinct_objects())
                } else {
                    None
                }
            })
            .min()
    }

    /// Intersected subject-id frontier for `var` across every already-scanned
    /// relation incident to it. Each new scan tightens the frontier, so probe
    /// candidates are re-derived per cascade level. Only the smallest incident
    /// value set is materialized; the rest intersect via O(1) lookups.
    /// Non-sid object bindings are filtered rather than fatal: a subject var
    /// only ever joins through `Binding::encoded_sid`, so rows binding it to a
    /// literal can never complete an assignment. Returns `None` when no
    /// scanned relation is incident to `var`.
    fn frontier_for_var(&self, relations: &[RelationIndex], var: VarId) -> Option<FxHashSet<u64>> {
        let incident: Vec<&RelationIndex> = relations
            .iter()
            .filter(|rel| rel.edge.subject == var || rel.edge.object == var)
            .collect();
        let seed_pos = incident
            .iter()
            .enumerate()
            .min_by_key(|(_, rel)| {
                if rel.edge.subject == var {
                    rel.distinct_subjects()
                } else {
                    rel.distinct_objects()
                }
            })?
            .0;

        let seed_rel = incident[seed_pos];
        let mut frontier: FxHashSet<u64> = if seed_rel.edge.subject == var {
            seed_rel.by_subject.keys().copied().collect()
        } else {
            seed_rel
                .by_object
                .keys()
                .filter_map(Binding::encoded_s_id)
                .collect()
        };
        for (pos, rel) in incident.iter().enumerate() {
            if pos == seed_pos {
                continue;
            }
            if rel.edge.subject == var {
                frontier.retain(|s| rel.by_subject.contains_key(s));
            } else {
                frontier.retain(|s| rel.by_object.contains_key(&Binding::encoded_sid(*s)));
            }
        }
        Some(frontier)
    }

    fn ref_frontier_size_bound(relations: &[RefRelationIndex], var: VarId) -> Option<usize> {
        relations
            .iter()
            .filter_map(|rel| {
                if rel.edge.subject == var {
                    Some(rel.distinct_subjects())
                } else if rel.edge.object == var {
                    Some(rel.distinct_objects())
                } else {
                    None
                }
            })
            .min()
    }

    fn ref_frontier_for_var(relations: &[RefRelationIndex], var: VarId) -> Option<FxHashSet<u64>> {
        let incident: Vec<&RefRelationIndex> = relations
            .iter()
            .filter(|rel| rel.edge.subject == var || rel.edge.object == var)
            .collect();
        let seed_pos = incident
            .iter()
            .enumerate()
            .min_by_key(|(_, rel)| {
                if rel.edge.subject == var {
                    rel.distinct_subjects()
                } else {
                    rel.distinct_objects()
                }
            })?
            .0;

        let seed_rel = incident[seed_pos];
        let mut frontier: FxHashSet<u64> = if seed_rel.edge.subject == var {
            seed_rel.by_subject.keys().copied().collect()
        } else {
            seed_rel.by_object.keys().copied().collect()
        };
        for (pos, rel) in incident.iter().enumerate() {
            if pos == seed_pos {
                continue;
            }
            if rel.edge.subject == var {
                frontier.retain(|s| rel.by_subject.contains_key(s));
            } else {
                frontier.retain(|s| rel.by_object.contains_key(s));
            }
        }
        Some(frontier)
    }

    fn scan_ref_relation_for_subjects(
        &self,
        ctx: &ExecutionContext<'_>,
        edge: &CyclicEdge,
        subjects: &FxHashSet<u64>,
    ) -> Result<Option<Vec<RefEdgeRow>>> {
        let Some(store) = ctx.binary_store.as_ref() else {
            self.log_fast_path_bail("missing-binary-store", Some(edge));
            return Ok(None);
        };
        let pred_sid = normalize_pred_sid(store, &edge.predicate)?;
        let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
            return Ok(Some(Vec::new()));
        };

        let mut ordered_subjects: Vec<u64> = subjects.iter().copied().collect();
        ordered_subjects.sort_unstable();

        let row_cap = max_predicate_rows();
        let projection = cursor_projection_sid_otype_okey();
        let mut rows = Vec::new();
        for subject in ordered_subjects {
            let Some(mut cursor) = build_head_cursor_for_single_subject(
                store,
                ctx.binary_g_id,
                p_id,
                projection,
                subject,
                ctx.to_t,
            ) else {
                self.log_fast_path_bail("cursor-unavailable", Some(edge));
                return Ok(None);
            };
            while let Some(batch) = cursor
                .next_batch()
                .map_err(|e| QueryError::Internal(format!("cyclic bgp bounded ref cursor: {e}")))?
            {
                for row in 0..batch.row_count {
                    if batch.o_type.get(row) != OType::IRI_REF.as_u16() {
                        self.log_fast_path_bail("non-ref-object-in-ref-mode", Some(edge));
                        return Ok(None);
                    }
                    rows.push(RefEdgeRow {
                        subject,
                        object: batch.o_key.get(row),
                    });
                    if rows.len() as u64 > row_cap {
                        self.log_fast_path_bail("predicate-row-cap-exceeded", Some(edge));
                        return Ok(None);
                    }
                }
            }
        }
        Ok(Some(rows))
    }

    fn open_fast_path(&mut self, ctx: &ExecutionContext<'_>) -> Result<bool> {
        if self.mode.is_history() || !allow_cursor_fast_path(ctx) {
            self.log_fast_path_bail("runtime-mode-or-context-unsupported", None);
            return Ok(false);
        }

        match self.join_mode {
            CyclicJoinMode::RefOnly => self.open_ref_fast_path(ctx),
            CyclicJoinMode::EncodedObject => self.open_encoded_fast_path(ctx),
        }
    }

    /// Cascading relation loader shared by both join modes: scan the cheapest
    /// edge first, then at each level either probe a remaining edge per-subject
    /// (when an already-scanned relation bounds its subject var to a small
    /// frontier and the probe-vs-scan gate passes) or fall back to a full scan
    /// of the cheapest remaining edge. Probed relations are subject-restricted
    /// semi-joins; `prune_relations` re-establishes global consistency after.
    fn open_ref_fast_path(&mut self, ctx: &ExecutionContext<'_>) -> Result<bool> {
        // Per-subject probes bypass the overlay, so they're only sound at HEAD.
        let probing_allowed = fast_path_store(ctx).is_some();
        let mut remaining: Vec<usize> = (0..self.plan.edges.len()).collect();
        let mut relations: Vec<RefRelationIndex> = Vec::with_capacity(remaining.len());
        let mut raw_rows = 0usize;
        let mut probed_edges = 0usize;

        while !remaining.is_empty() {
            let mut probe: Option<(usize, FxHashSet<u64>)> = None;
            if probing_allowed && !relations.is_empty() {
                for (pos, &edge_idx) in remaining.iter().enumerate() {
                    let edge = &self.plan.edges[edge_idx];
                    // O(1) rejects before materializing any frontier set. The
                    // size bound is an over-estimate of the intersection, so
                    // rejecting on it is conservative (never unsound).
                    if edge.estimate.is_none() {
                        continue;
                    }
                    let Some(bound) = Self::ref_frontier_size_bound(&relations, edge.subject)
                    else {
                        continue;
                    };
                    if bound > max_bounded_probe_subjects() {
                        continue;
                    }
                    let Some(frontier) = Self::ref_frontier_for_var(&relations, edge.subject)
                    else {
                        continue;
                    };
                    if !should_probe_edge(frontier.len(), edge.estimate) {
                        continue;
                    }
                    if probe
                        .as_ref()
                        .is_none_or(|(_, best)| frontier.len() < best.len())
                    {
                        probe = Some((pos, frontier));
                    }
                }
            }

            let (pos, rows) = match probe {
                Some((pos, frontier)) => {
                    let edge = &self.plan.edges[remaining[pos]];
                    let rows = if frontier.is_empty() {
                        Vec::new()
                    } else {
                        match self.scan_ref_relation_for_subjects(ctx, edge, &frontier)? {
                            Some(rows) => rows,
                            None => return Ok(false),
                        }
                    };
                    probed_edges += 1;
                    (pos, rows)
                }
                None => {
                    let pos = Self::cheapest_remaining(&self.plan.edges, &remaining);
                    let rows =
                        match self.scan_ref_relation(ctx, &self.plan.edges[remaining[pos]])? {
                            Some(rows) => rows,
                            None => return Ok(false),
                        };
                    (pos, rows)
                }
            };
            let edge_idx = remaining.swap_remove(pos);
            raw_rows += rows.len();
            let empty = rows.is_empty();
            relations.push(RefRelationIndex::new(
                self.plan.edges[edge_idx].clone(),
                rows,
            ));
            if empty {
                self.driver_idx = relations.len() - 1;
                self.ref_relations = relations;
                self.raw_relation_rows = raw_rows;
                self.pruned_relation_rows = 0;
                self.probed_edges = probed_edges;
                self.used_fast_path = true;
                return Ok(true);
            }
        }

        relations = self.prune_ref_relations(relations);
        self.raw_relation_rows = raw_rows;
        self.pruned_relation_rows = relations.iter().map(|rel| rel.rows.len()).sum();
        self.probed_edges = probed_edges;
        self.driver_idx = self.choose_ref_driver(&relations);
        self.ref_relations = relations;
        self.used_fast_path = true;
        Ok(true)
    }

    fn open_encoded_fast_path(&mut self, ctx: &ExecutionContext<'_>) -> Result<bool> {
        let probing_allowed = fast_path_store(ctx).is_some();
        let mut remaining: Vec<usize> = (0..self.plan.edges.len()).collect();
        let mut relations: Vec<RelationIndex> = Vec::with_capacity(remaining.len());
        let mut raw_rows = 0usize;
        let mut probed_edges = 0usize;

        while !remaining.is_empty() {
            let mut probe: Option<(usize, FxHashSet<u64>)> = None;
            if probing_allowed && !relations.is_empty() {
                for (pos, &edge_idx) in remaining.iter().enumerate() {
                    let edge = &self.plan.edges[edge_idx];
                    // O(1) rejects before materializing any frontier set (the
                    // size bound over-estimates the intersection, so this is
                    // conservative).
                    if edge.estimate.is_none() {
                        continue;
                    }
                    let Some(bound) = Self::frontier_size_bound(&relations, edge.subject) else {
                        continue;
                    };
                    if bound > max_bounded_probe_subjects() {
                        continue;
                    }
                    let Some(frontier) = self.frontier_for_var(&relations, edge.subject) else {
                        continue;
                    };
                    if !should_probe_edge(frontier.len(), edge.estimate) {
                        continue;
                    }
                    if probe
                        .as_ref()
                        .is_none_or(|(_, best)| frontier.len() < best.len())
                    {
                        probe = Some((pos, frontier));
                    }
                }
            }

            let (pos, rows) = match probe {
                Some((pos, frontier)) => {
                    let edge = &self.plan.edges[remaining[pos]];
                    let rows = if frontier.is_empty() {
                        Vec::new()
                    } else {
                        match self.scan_relation_for_subjects(ctx, edge, &frontier)? {
                            Some(rows) => rows,
                            None => return Ok(false),
                        }
                    };
                    probed_edges += 1;
                    (pos, rows)
                }
                None => {
                    let pos = Self::cheapest_remaining(&self.plan.edges, &remaining);
                    let rows = match self.scan_relation(ctx, &self.plan.edges[remaining[pos]])? {
                        Some(rows) => rows,
                        None => return Ok(false),
                    };
                    (pos, rows)
                }
            };
            let edge_idx = remaining.swap_remove(pos);
            raw_rows += rows.len();
            let empty = rows.is_empty();
            relations.push(RelationIndex::new(self.plan.edges[edge_idx].clone(), rows));
            if empty {
                self.driver_idx = relations.len() - 1;
                self.relations = relations;
                self.raw_relation_rows = raw_rows;
                self.pruned_relation_rows = 0;
                self.probed_edges = probed_edges;
                self.used_fast_path = true;
                return Ok(true);
            }
        }

        relations = self.prune_relations(relations);
        self.raw_relation_rows = raw_rows;
        self.pruned_relation_rows = relations.iter().map(|rel| rel.rows.len()).sum();
        self.square_wedge = self.open_encoded_square_wedge(&relations);
        self.probed_edges = probed_edges;
        self.driver_idx = self.choose_driver(&relations);
        self.relations = relations;
        self.used_fast_path = true;
        Ok(true)
    }

    /// Position (within `remaining`) of the cheapest edge to full-scan next.
    /// A stats-absent estimate sorts first: with populated stats, absent means
    /// the predicate is empty, making it the ideal early-exit scan.
    fn cheapest_remaining(edges: &[CyclicEdge], remaining: &[usize]) -> usize {
        remaining
            .iter()
            .enumerate()
            .min_by_key(|(_, &edge_idx)| edges[edge_idx].estimate.unwrap_or(0))
            .map(|(pos, _)| pos)
            .expect("cheapest_remaining called with non-empty remaining")
    }

    fn prune_ref_relations(&self, mut relations: Vec<RefRelationIndex>) -> Vec<RefRelationIndex> {
        loop {
            let before: usize = relations.iter().map(|rel| rel.rows.len()).sum();
            let allowed = self.ref_allowed_values(&relations);
            relations = relations
                .into_iter()
                .map(|rel| {
                    let subject_allowed = allowed.get(&rel.edge.subject);
                    let object_allowed = allowed.get(&rel.edge.object);
                    let rows = rel
                        .rows
                        .into_iter()
                        .filter(|row| {
                            subject_allowed.is_none_or(|set| set.contains(&row.subject))
                                && object_allowed.is_none_or(|set| set.contains(&row.object))
                        })
                        .collect();
                    RefRelationIndex::new(rel.edge, rows)
                })
                .collect();
            let after: usize = relations.iter().map(|rel| rel.rows.len()).sum();
            if after == before {
                return relations;
            }
        }
    }

    fn ref_allowed_values(
        &self,
        relations: &[RefRelationIndex],
    ) -> FxHashMap<VarId, FxHashSet<u64>> {
        let mut allowed: FxHashMap<VarId, FxHashSet<u64>> = FxHashMap::default();
        for rel in relations {
            Self::intersect_ref_allowed(
                &mut allowed,
                rel.edge.subject,
                rel.rows.iter().map(|row| row.subject).collect(),
            );
            Self::intersect_ref_allowed(
                &mut allowed,
                rel.edge.object,
                rel.rows.iter().map(|row| row.object).collect(),
            );
        }
        allowed
    }

    fn intersect_ref_allowed(
        allowed: &mut FxHashMap<VarId, FxHashSet<u64>>,
        var: VarId,
        values: FxHashSet<u64>,
    ) {
        allowed
            .entry(var)
            .and_modify(|existing| existing.retain(|value| values.contains(value)))
            .or_insert(values);
    }

    fn prune_relations(&self, mut relations: Vec<RelationIndex>) -> Vec<RelationIndex> {
        loop {
            let before: usize = relations.iter().map(|rel| rel.rows.len()).sum();
            let allowed = self.allowed_values(&relations);
            relations = relations
                .into_iter()
                .map(|rel| {
                    let subject_allowed = allowed.get(&rel.edge.subject);
                    let object_allowed = allowed.get(&rel.edge.object);
                    let rows = rel
                        .rows
                        .into_iter()
                        .filter(|row| {
                            let subject = Binding::encoded_sid(row.subject);
                            subject_allowed.is_none_or(|set| set.contains(&subject))
                                && object_allowed.is_none_or(|set| set.contains(&row.object))
                        })
                        .collect();
                    RelationIndex::new(rel.edge, rows)
                })
                .collect();
            let after: usize = relations.iter().map(|rel| rel.rows.len()).sum();
            if after == before {
                return relations;
            }
        }
    }

    fn allowed_values(&self, relations: &[RelationIndex]) -> FxHashMap<VarId, FxHashSet<Binding>> {
        let mut allowed: FxHashMap<VarId, FxHashSet<Binding>> = FxHashMap::default();
        for rel in relations {
            Self::intersect_allowed(
                &mut allowed,
                rel.edge.subject,
                rel.rows
                    .iter()
                    .map(|row| Binding::encoded_sid(row.subject))
                    .collect(),
            );
            Self::intersect_allowed(
                &mut allowed,
                rel.edge.object,
                rel.rows.iter().map(|row| row.object.clone()).collect(),
            );
        }
        allowed
    }

    fn intersect_allowed(
        allowed: &mut FxHashMap<VarId, FxHashSet<Binding>>,
        var: VarId,
        values: FxHashSet<Binding>,
    ) {
        allowed
            .entry(var)
            .and_modify(|existing| existing.retain(|value| values.contains(value)))
            .or_insert(values);
    }

    fn open_encoded_square_wedge(
        &self,
        relations: &[RelationIndex],
    ) -> Option<EncodedSquareWedgeState> {
        if self.plan.shape != CyclicBgpShape::Square || relations.len() != 4 {
            return None;
        }

        let cap = max_square_wedge_pairs();
        let plan = self
            .encoded_square_wedge_plans(relations)
            .into_iter()
            .min_by_key(|plan| (plan.build_pairs, plan.probe_pairs))?;
        if plan.build_pairs > cap {
            tracing::debug!(
                build_pairs = plan.build_pairs,
                probe_pairs = plan.probe_pairs,
                cap,
                "cyclic bgp square wedge bypassed: build pair cap exceeded"
            );
            return None;
        }

        let table = self.build_encoded_wedge_table(relations, &plan, cap)?;
        let probe_centers = self.wedge_center_intersection(
            &relations[plan.probe_edge_a],
            &relations[plan.probe_edge_b],
            plan.probe_center,
        );

        tracing::debug!(
            build_pairs = plan.build_pairs,
            probe_pairs = plan.probe_pairs,
            build_center = plan.build_center.0,
            probe_center = plan.probe_center.0,
            table_keys = table.len(),
            probe_centers = probe_centers.len(),
            "cyclic bgp square wedge selected"
        );

        Some(EncodedSquareWedgeState {
            plan,
            table,
            probe_centers,
            probe_center_pos: 0,
            current: None,
        })
    }

    fn encoded_square_wedge_plans(
        &self,
        relations: &[RelationIndex],
    ) -> Vec<EncodedSquareWedgePlan> {
        let vars = self.plan.vars.as_ref();
        let mut plans = Vec::new();
        for i in 0..vars.len() {
            for j in (i + 1)..vars.len() {
                let center_a = vars[i];
                let center_b = vars[j];
                if self.edge_between(relations, center_a, center_b).is_some() {
                    continue;
                }
                let keys: Vec<VarId> = vars
                    .iter()
                    .copied()
                    .filter(|v| *v != center_a && *v != center_b)
                    .collect();
                if keys.len() != 2 {
                    continue;
                }
                let key_a = keys[0];
                let key_b = keys[1];
                let Some(a_edge_a) = self.edge_between(relations, center_a, key_a) else {
                    continue;
                };
                let Some(a_edge_b) = self.edge_between(relations, center_a, key_b) else {
                    continue;
                };
                let Some(b_edge_a) = self.edge_between(relations, center_b, key_a) else {
                    continue;
                };
                let Some(b_edge_b) = self.edge_between(relations, center_b, key_b) else {
                    continue;
                };

                let a_pairs =
                    self.wedge_pair_count(&relations[a_edge_a], &relations[a_edge_b], center_a);
                let b_pairs =
                    self.wedge_pair_count(&relations[b_edge_a], &relations[b_edge_b], center_b);
                if a_pairs <= b_pairs {
                    plans.push(EncodedSquareWedgePlan {
                        build_center: center_a,
                        probe_center: center_b,
                        key_a,
                        key_b,
                        build_edge_a: a_edge_a,
                        build_edge_b: a_edge_b,
                        probe_edge_a: b_edge_a,
                        probe_edge_b: b_edge_b,
                        build_pairs: a_pairs,
                        probe_pairs: b_pairs,
                    });
                } else {
                    plans.push(EncodedSquareWedgePlan {
                        build_center: center_b,
                        probe_center: center_a,
                        key_a,
                        key_b,
                        build_edge_a: b_edge_a,
                        build_edge_b: b_edge_b,
                        probe_edge_a: a_edge_a,
                        probe_edge_b: a_edge_b,
                        build_pairs: b_pairs,
                        probe_pairs: a_pairs,
                    });
                }
            }
        }
        plans
    }

    fn edge_between(&self, relations: &[RelationIndex], a: VarId, b: VarId) -> Option<usize> {
        relations.iter().position(|rel| {
            (rel.edge.subject == a && rel.edge.object == b)
                || (rel.edge.subject == b && rel.edge.object == a)
        })
    }

    fn wedge_pair_count(
        &self,
        edge_a: &RelationIndex,
        edge_b: &RelationIndex,
        center: VarId,
    ) -> usize {
        self.wedge_center_intersection(edge_a, edge_b, center)
            .into_iter()
            .map(|center_value| {
                self.relation_degree_for_center(edge_a, center, &center_value)
                    .saturating_mul(self.relation_degree_for_center(edge_b, center, &center_value))
            })
            .sum()
    }

    fn wedge_center_intersection(
        &self,
        edge_a: &RelationIndex,
        edge_b: &RelationIndex,
        center: VarId,
    ) -> Vec<Binding> {
        let values_a = self.relation_center_values(edge_a, center);
        let values_b = self.relation_center_values(edge_b, center);
        values_a
            .into_iter()
            .filter(|value| values_b.contains(value))
            .collect()
    }

    fn relation_center_values(&self, rel: &RelationIndex, center: VarId) -> FxHashSet<Binding> {
        if rel.edge.subject == center {
            rel.by_subject
                .keys()
                .map(|s_id| Binding::encoded_sid(*s_id))
                .collect()
        } else if rel.edge.object == center {
            rel.by_object.keys().cloned().collect()
        } else {
            FxHashSet::default()
        }
    }

    fn relation_degree_for_center(
        &self,
        rel: &RelationIndex,
        center: VarId,
        center_value: &Binding,
    ) -> usize {
        if rel.edge.subject == center {
            center_value
                .encoded_s_id()
                .and_then(|s_id| rel.by_subject.get(&s_id).map(Vec::len))
                .unwrap_or(0)
        } else if rel.edge.object == center {
            rel.by_object.get(center_value).map(Vec::len).unwrap_or(0)
        } else {
            0
        }
    }

    fn relation_values_for_center(
        &self,
        rel: &RelationIndex,
        center: VarId,
        center_value: &Binding,
    ) -> Vec<Binding> {
        if rel.edge.subject == center {
            center_value
                .encoded_s_id()
                .and_then(|s_id| rel.by_subject.get(&s_id).cloned())
                .unwrap_or_default()
        } else if rel.edge.object == center {
            rel.by_object
                .get(center_value)
                .map(|subjects| {
                    subjects
                        .iter()
                        .map(|s_id| Binding::encoded_sid(*s_id))
                        .collect()
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        }
    }

    fn build_encoded_wedge_table(
        &self,
        relations: &[RelationIndex],
        plan: &EncodedSquareWedgePlan,
        cap: usize,
    ) -> Option<FxHashMap<WedgePairKey, Vec<Binding>>> {
        let edge_a = &relations[plan.build_edge_a];
        let edge_b = &relations[plan.build_edge_b];
        let centers = self.wedge_center_intersection(edge_a, edge_b, plan.build_center);
        let mut table: FxHashMap<WedgePairKey, Vec<Binding>> = FxHashMap::default();
        let mut total = 0usize;
        for center in centers {
            let values_a = self.relation_values_for_center(edge_a, plan.build_center, &center);
            let values_b = self.relation_values_for_center(edge_b, plan.build_center, &center);
            for value_a in &values_a {
                for value_b in &values_b {
                    total = total.saturating_add(1);
                    if total > cap {
                        return None;
                    }
                    table
                        .entry(WedgePairKey(value_a.clone(), value_b.clone()))
                        .or_default()
                        .push(center.clone());
                }
            }
        }
        Some(table)
    }

    fn choose_ref_driver(&self, relations: &[RefRelationIndex]) -> usize {
        relations
            .iter()
            .enumerate()
            .min_by_key(|(idx, rel)| {
                let mut used = vec![false; relations.len()];
                used[*idx] = true;
                let assigned = self.ref_assigned_for_edge(&rel.edge);
                (
                    rel.rows.len().saturating_mul(
                        self.choose_next_ref_relation_with(relations, &assigned, &used)
                            .map(|next| {
                                self.ref_bound_fanout_score(&relations[next], &assigned)
                                    .max(1)
                            })
                            .unwrap_or(1),
                    ),
                    rel.rows.len(),
                )
            })
            .map(|(idx, _)| idx)
            .unwrap_or(0)
    }

    fn ref_assigned_for_edge(&self, edge: &CyclicEdge) -> Vec<Option<u64>> {
        let mut assigned = vec![None; self.plan.vars.len()];
        assigned[self.var_pos(edge.subject)] = Some(0);
        assigned[self.var_pos(edge.object)] = Some(0);
        assigned
    }

    fn choose_driver(&self, relations: &[RelationIndex]) -> usize {
        relations
            .iter()
            .enumerate()
            .min_by_key(|(idx, rel)| {
                let mut used = vec![false; relations.len()];
                used[*idx] = true;
                let assigned = self.assigned_for_edge(&rel.edge);
                (
                    rel.rows.len().saturating_mul(
                        self.choose_next_relation_with(relations, &assigned, &used)
                            .map(|next| self.bound_fanout_score(&relations[next], &assigned).max(1))
                            .unwrap_or(1),
                    ),
                    rel.rows.len(),
                )
            })
            .map(|(idx, _)| idx)
            .unwrap_or(0)
    }

    fn assigned_for_edge(&self, edge: &CyclicEdge) -> Vec<Option<Binding>> {
        let mut assigned = vec![None; self.plan.vars.len()];
        assigned[self.var_pos(edge.subject)] = Some(Binding::Unbound);
        assigned[self.var_pos(edge.object)] = Some(Binding::Unbound);
        assigned
    }

    fn choose_next_ref_relation(&self, assigned: &[Option<u64>], used: &[bool]) -> Option<usize> {
        self.choose_next_ref_relation_with(&self.ref_relations, assigned, used)
    }

    fn choose_next_ref_relation_with(
        &self,
        relations: &[RefRelationIndex],
        assigned: &[Option<u64>],
        used: &[bool],
    ) -> Option<usize> {
        relations
            .iter()
            .enumerate()
            .filter(|(idx, _)| !used[*idx])
            .min_by_key(|(_, rel)| {
                let bound_count = self.ref_bound_count(rel, assigned);
                (
                    std::cmp::Reverse(bound_count),
                    self.ref_bound_fanout_score(rel, assigned),
                    rel.rows.len(),
                )
            })
            .map(|(idx, _)| idx)
    }

    fn ref_bound_count(&self, rel: &RefRelationIndex, assigned: &[Option<u64>]) -> u8 {
        let s_bound = assigned[self.var_pos(rel.edge.subject)].is_some();
        let o_bound = assigned[self.var_pos(rel.edge.object)].is_some();
        s_bound as u8 + o_bound as u8
    }

    fn ref_bound_fanout_score(&self, rel: &RefRelationIndex, assigned: &[Option<u64>]) -> usize {
        let s_bound = assigned[self.var_pos(rel.edge.subject)].is_some();
        let o_bound = assigned[self.var_pos(rel.edge.object)].is_some();
        match (s_bound, o_bound) {
            (true, true) => 1,
            (true, false) => average_bucket_size(rel.rows.len(), rel.distinct_subjects()),
            (false, true) => average_bucket_size(rel.rows.len(), rel.distinct_objects()),
            (false, false) => rel.rows.len(),
        }
    }

    fn choose_next_relation(&self, assigned: &[Option<Binding>], used: &[bool]) -> Option<usize> {
        self.choose_next_relation_with(&self.relations, assigned, used)
    }

    fn choose_next_relation_with(
        &self,
        relations: &[RelationIndex],
        assigned: &[Option<Binding>],
        used: &[bool],
    ) -> Option<usize> {
        relations
            .iter()
            .enumerate()
            .filter(|(idx, _)| !used[*idx])
            .min_by_key(|(_, rel)| {
                let bound_count = self.bound_count(rel, assigned);
                (
                    std::cmp::Reverse(bound_count),
                    self.bound_fanout_score(rel, assigned),
                    rel.rows.len(),
                )
            })
            .map(|(idx, _)| idx)
    }

    fn bound_count(&self, rel: &RelationIndex, assigned: &[Option<Binding>]) -> u8 {
        let s_bound = assigned[self.var_pos(rel.edge.subject)].is_some();
        let o_bound = assigned[self.var_pos(rel.edge.object)].is_some();
        s_bound as u8 + o_bound as u8
    }

    fn bound_fanout_score(&self, rel: &RelationIndex, assigned: &[Option<Binding>]) -> usize {
        let s_bound = assigned[self.var_pos(rel.edge.subject)].is_some();
        let o_bound = assigned[self.var_pos(rel.edge.object)].is_some();
        match (s_bound, o_bound) {
            (true, true) => 1,
            (true, false) => average_bucket_size(rel.rows.len(), rel.distinct_subjects()),
            (false, true) => average_bucket_size(rel.rows.len(), rel.distinct_objects()),
            (false, false) => rel.rows.len(),
        }
    }

    fn ref_relation_candidates(
        &self,
        rel: &RefRelationIndex,
        assigned: &[Option<u64>],
    ) -> Vec<RefEdgeRow> {
        let s_pos = self.var_pos(rel.edge.subject);
        let o_pos = self.var_pos(rel.edge.object);
        match (assigned[s_pos], assigned[o_pos]) {
            (Some(s), Some(o)) => rel
                .pairs
                .contains(&(s, o))
                .then_some(RefEdgeRow {
                    subject: s,
                    object: o,
                })
                .into_iter()
                .collect(),
            (Some(s), None) => rel
                .by_subject
                .get(&s)
                .into_iter()
                .flatten()
                .map(|&o| RefEdgeRow {
                    subject: s,
                    object: o,
                })
                .collect(),
            (None, Some(o)) => rel
                .by_object
                .get(&o)
                .into_iter()
                .flatten()
                .map(|&s| RefEdgeRow {
                    subject: s,
                    object: o,
                })
                .collect(),
            (None, None) => rel.rows.clone(),
        }
    }

    fn extend_ref_assignments(
        &self,
        assigned: &mut [Option<u64>],
        used: &mut [bool],
        out: &mut VecDeque<Vec<u64>>,
    ) {
        let Some(rel_idx) = self.choose_next_ref_relation(assigned, used) else {
            out.push_back(
                assigned
                    .iter()
                    .map(|v| v.expect("all cyclic vars assigned before emit"))
                    .collect(),
            );
            return;
        };
        let rel = &self.ref_relations[rel_idx];
        let s_pos = self.var_pos(rel.edge.subject);
        let o_pos = self.var_pos(rel.edge.object);
        let candidates = self.ref_relation_candidates(rel, assigned);
        used[rel_idx] = true;
        for candidate in candidates {
            let old_s = assigned[s_pos];
            let old_o = assigned[o_pos];
            if old_s.is_some_and(|s| s != candidate.subject)
                || old_o.is_some_and(|o| o != candidate.object)
            {
                continue;
            }
            assigned[s_pos] = Some(candidate.subject);
            assigned[o_pos] = Some(candidate.object);
            self.extend_ref_assignments(assigned, used, out);
            assigned[s_pos] = old_s;
            assigned[o_pos] = old_o;
        }
        used[rel_idx] = false;
    }

    fn relation_candidates(
        &self,
        rel: &RelationIndex,
        assigned: &[Option<Binding>],
    ) -> Vec<EdgeRow> {
        let s_pos = self.var_pos(rel.edge.subject);
        let o_pos = self.var_pos(rel.edge.object);
        match (assigned[s_pos].as_ref(), assigned[o_pos].as_ref()) {
            (Some(s), Some(o)) => s.encoded_s_id().map_or_else(Vec::new, |s_id| {
                rel.pairs
                    .contains(&(s_id, o.clone()))
                    .then_some(EdgeRow {
                        subject: s_id,
                        object: o.clone(),
                    })
                    .into_iter()
                    .collect()
            }),
            (Some(s), None) => s.encoded_s_id().map_or_else(Vec::new, |s_id| {
                rel.by_subject
                    .get(&s_id)
                    .into_iter()
                    .flatten()
                    .map(|o| EdgeRow {
                        subject: s_id,
                        object: o.clone(),
                    })
                    .collect()
            }),
            (None, Some(o)) => rel
                .by_object
                .get(o)
                .into_iter()
                .flatten()
                .map(|&s| EdgeRow {
                    subject: s,
                    object: o.clone(),
                })
                .collect(),
            (None, None) => rel.rows.clone(),
        }
    }

    fn extend_assignments(
        &self,
        assigned: &mut [Option<Binding>],
        used: &mut [bool],
        out: &mut VecDeque<Vec<Binding>>,
    ) {
        let Some(rel_idx) = self.choose_next_relation(assigned, used) else {
            out.push_back(
                assigned
                    .iter()
                    .map(|v| v.clone().expect("all cyclic vars assigned before emit"))
                    .collect(),
            );
            return;
        };
        let rel = &self.relations[rel_idx];
        let s_pos = self.var_pos(rel.edge.subject);
        let o_pos = self.var_pos(rel.edge.object);
        let candidates = self.relation_candidates(rel, assigned);
        used[rel_idx] = true;
        for candidate in candidates {
            let subject = Binding::encoded_sid(candidate.subject);
            let old_s = assigned[s_pos].clone();
            let old_o = assigned[o_pos].clone();
            if old_s.as_ref().is_some_and(|s| s != &subject)
                || old_o.as_ref().is_some_and(|o| o != &candidate.object)
            {
                continue;
            }
            assigned[s_pos] = Some(subject);
            assigned[o_pos] = Some(candidate.object);
            self.extend_assignments(assigned, used, out);
            assigned[s_pos] = old_s;
            assigned[o_pos] = old_o;
        }
        used[rel_idx] = false;
    }

    fn seed_next_driver(&mut self) {
        if self.relations.is_empty() {
            return;
        }
        let driver = &self.relations[self.driver_idx];
        if self.driver_pos >= driver.rows.len() {
            return;
        }
        let row = driver.rows[self.driver_pos].clone();
        self.driver_pos += 1;

        let mut assigned = vec![None; self.plan.vars.len()];
        assigned[self.var_pos(driver.edge.subject)] = Some(Binding::encoded_sid(row.subject));
        assigned[self.var_pos(driver.edge.object)] = Some(row.object);
        let mut used = vec![false; self.relations.len()];
        used[self.driver_idx] = true;
        let mut out = VecDeque::new();
        self.extend_assignments(&mut assigned, &mut used, &mut out);
        self.pending.extend(out);
    }

    fn seed_next_ref_driver(&mut self) {
        if self.ref_relations.is_empty() {
            return;
        }
        let driver = &self.ref_relations[self.driver_idx];
        if self.driver_pos >= driver.rows.len() {
            return;
        }
        let row = driver.rows[self.driver_pos];
        self.driver_pos += 1;

        let mut assigned = vec![None; self.plan.vars.len()];
        assigned[self.var_pos(driver.edge.subject)] = Some(row.subject);
        assigned[self.var_pos(driver.edge.object)] = Some(row.object);
        let mut used = vec![false; self.ref_relations.len()];
        used[self.driver_idx] = true;
        let mut out = VecDeque::new();
        self.extend_ref_assignments(&mut assigned, &mut used, &mut out);
        self.ref_pending.extend(out);
    }

    fn assignment_to_columns(&self, assignment: &[Binding], cols: &mut [Vec<Binding>]) {
        for (out_idx, var_idx) in self.schema_positions.iter().copied().enumerate() {
            cols[out_idx].push(assignment[var_idx].clone());
        }
    }

    fn next_square_wedge_match(
        &self,
        state: &mut EncodedSquareWedgeState,
    ) -> Option<(Binding, Binding, Binding, Binding)> {
        loop {
            if state.current.is_none() {
                while state.probe_center_pos < state.probe_centers.len() {
                    let center = state.probe_centers[state.probe_center_pos].clone();
                    state.probe_center_pos += 1;
                    let values_a = self.relation_values_for_center(
                        &self.relations[state.plan.probe_edge_a],
                        state.plan.probe_center,
                        &center,
                    );
                    let values_b = self.relation_values_for_center(
                        &self.relations[state.plan.probe_edge_b],
                        state.plan.probe_center,
                        &center,
                    );
                    if !values_a.is_empty() && !values_b.is_empty() {
                        state.current = Some(EncodedProbeWedgeCursor {
                            center,
                            values_a,
                            values_b,
                            a_pos: 0,
                            b_pos: 0,
                            matches: Vec::new(),
                            match_pos: 0,
                        });
                        break;
                    }
                }
                state.current.as_ref()?;
            }

            let cursor = state.current.as_mut().expect("square wedge cursor");
            if cursor.match_pos < cursor.matches.len() {
                let build_center = cursor.matches[cursor.match_pos].clone();
                cursor.match_pos += 1;
                return Some((
                    build_center,
                    cursor.center.clone(),
                    cursor.values_a[cursor.a_pos].clone(),
                    cursor.values_b[cursor.b_pos].clone(),
                ));
            }
            if !cursor.matches.is_empty() {
                advance_probe_cursor(cursor);
            }

            while cursor.a_pos < cursor.values_a.len() {
                if cursor.b_pos >= cursor.values_b.len() {
                    cursor.a_pos += 1;
                    cursor.b_pos = 0;
                    continue;
                }
                let key_a = cursor.values_a[cursor.a_pos].clone();
                let key_b = cursor.values_b[cursor.b_pos].clone();
                if let Some(matches) = state.table.get(&WedgePairKey(key_a, key_b)) {
                    cursor.matches = matches.clone();
                    cursor.match_pos = 0;
                    if !cursor.matches.is_empty() {
                        break;
                    }
                }
                advance_probe_cursor(cursor);
            }

            if cursor.a_pos >= cursor.values_a.len() {
                state.current = None;
            }
        }
    }

    fn next_encoded_square_wedge_batch(&mut self) -> Result<Option<Batch>> {
        let mut state = self
            .square_wedge
            .take()
            .expect("square wedge state must exist for wedge batch");
        let mut cols: Vec<Vec<Binding>> = self.schema.iter().map(|_| Vec::new()).collect();
        let mut produced = 0usize;
        while produced < OUTPUT_BATCH_SIZE {
            let Some((build_center, probe_center, key_a, key_b)) =
                self.next_square_wedge_match(&mut state)
            else {
                break;
            };
            if cols.is_empty() {
                produced += 1;
                continue;
            }
            let mut assignment = vec![Binding::Unbound; self.plan.vars.len()];
            assignment[self.var_pos(state.plan.build_center)] = build_center;
            assignment[self.var_pos(state.plan.probe_center)] = probe_center;
            assignment[self.var_pos(state.plan.key_a)] = key_a;
            assignment[self.var_pos(state.plan.key_b)] = key_b;
            self.assignment_to_columns(&assignment, &mut cols);
            produced += 1;
        }
        self.square_wedge = Some(state);

        if produced == 0 {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }
        if cols.is_empty() {
            return Ok(Some(Batch::empty_schema_with_len(produced)));
        }
        Batch::new(Arc::clone(&self.schema), cols)
            .map(Some)
            .map_err(|e| QueryError::Internal(format!("cyclic bgp square wedge batch: {e}")))
    }

    fn ref_assignment_to_columns(&self, assignment: &[u64], cols: &mut [Vec<Binding>]) {
        for (out_idx, var_idx) in self.schema_positions.iter().copied().enumerate() {
            cols[out_idx].push(Binding::encoded_sid(assignment[var_idx]));
        }
    }

    fn next_ref_batch(&mut self) -> Result<Option<Batch>> {
        let mut cols: Vec<Vec<Binding>> = self.schema.iter().map(|_| Vec::new()).collect();
        let mut produced = 0usize;
        while produced < OUTPUT_BATCH_SIZE {
            if let Some(assignment) = self.ref_pending.pop_front() {
                if cols.is_empty() {
                    produced += 1;
                } else {
                    self.ref_assignment_to_columns(&assignment, &mut cols);
                    produced += 1;
                }
                continue;
            }
            if self.ref_relations.is_empty()
                || self.driver_pos >= self.ref_relations[self.driver_idx].rows.len()
            {
                break;
            }
            self.seed_next_ref_driver();
        }

        if produced == 0 {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }
        if cols.is_empty() {
            return Ok(Some(Batch::empty_schema_with_len(produced)));
        }
        Batch::new(Arc::clone(&self.schema), cols)
            .map(Some)
            .map_err(|e| QueryError::Internal(format!("cyclic bgp ref batch: {e}")))
    }

    fn next_encoded_batch(&mut self) -> Result<Option<Batch>> {
        if self.square_wedge.is_some() {
            return self.next_encoded_square_wedge_batch();
        }
        let mut cols: Vec<Vec<Binding>> = self.schema.iter().map(|_| Vec::new()).collect();
        let mut produced = 0usize;
        while produced < OUTPUT_BATCH_SIZE {
            if let Some(assignment) = self.pending.pop_front() {
                if cols.is_empty() {
                    produced += 1;
                } else {
                    self.assignment_to_columns(&assignment, &mut cols);
                    produced += 1;
                }
                continue;
            }
            if self.relations.is_empty()
                || self.driver_pos >= self.relations[self.driver_idx].rows.len()
            {
                break;
            }
            self.seed_next_driver();
        }

        if produced == 0 {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }
        if cols.is_empty() {
            return Ok(Some(Batch::empty_schema_with_len(produced)));
        }
        Batch::new(Arc::clone(&self.schema), cols)
            .map(Some)
            .map_err(|e| QueryError::Internal(format!("cyclic bgp batch: {e}")))
    }
}

fn average_bucket_size(rows: usize, distinct: usize) -> usize {
    if rows == 0 {
        0
    } else {
        rows.div_ceil(distinct.max(1))
    }
}

fn advance_probe_cursor(cursor: &mut EncodedProbeWedgeCursor) {
    cursor.matches.clear();
    cursor.match_pos = 0;
    cursor.b_pos += 1;
    if cursor.b_pos >= cursor.values_b.len() {
        cursor.a_pos += 1;
        cursor.b_pos = 0;
    }
}

fn predicate_display(predicate: &Ref) -> String {
    match predicate {
        Ref::Sid(sid) => format!("{}:{}", sid.namespace_code, sid.name),
        Ref::Iri(iri) => iri.to_string(),
        Ref::Var(v) => format!("?v{}", v.0),
    }
}

#[async_trait]
impl Operator for CyclicBgpOperator {
    fn plan_details(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut m = serde_json::Map::new();
        m.insert("strategy".into(), "cyclic_bgp_join".into());
        m.insert("shape".into(), self.plan.shape_name().into());
        m.insert(
            "enabled".into(),
            serde_json::Value::Bool(cyclic_bgp_enabled()),
        );
        m.insert("max-predicate-rows".into(), max_predicate_rows().into());
        m.insert("object-only-values".into(), self.join_mode.as_str().into());
        m.insert("pruning".into(), "semi_join".into());
        m.insert("driver-selection".into(), "pruned_bound_fanout".into());
        if let Some(state) = &self.square_wedge {
            m.insert("square-strategy".into(), "wedge_pair_hash".into());
            m.insert("square-build-pairs".into(), state.plan.build_pairs.into());
            m.insert("square-probe-pairs".into(), state.plan.probe_pairs.into());
            m.insert(
                "square-wedge-pair-cap".into(),
                max_square_wedge_pairs().into(),
            );
        }
        if self.probed_edges > 0 {
            m.insert(
                "bounded-probe-strategy".into(),
                "cascading_subject_probe".into(),
            );
            m.insert("bounded-probe-edges".into(), self.probed_edges.into());
            m.insert(
                "bounded-probe-subject-cap".into(),
                max_bounded_probe_subjects().into(),
            );
            m.insert(
                "bounded-probe-scan-ratio".into(),
                bounded_probe_scan_ratio().into(),
            );
        }
        if self.raw_relation_rows > 0 {
            m.insert("raw-relation-rows".into(), self.raw_relation_rows.into());
            m.insert(
                "pruned-relation-rows".into(),
                self.pruned_relation_rows.into(),
            );
        }
        let predicates: Vec<serde_json::Value> = self
            .plan
            .edges
            .iter()
            .map(|edge| predicate_display(&edge.predicate).into())
            .collect();
        m.insert("predicates".into(), serde_json::Value::Array(predicates));
        let predicate_estimates: Vec<serde_json::Value> = self
            .plan
            .edges
            .iter()
            .map(|edge| edge.estimate.map_or(serde_json::Value::Null, Into::into))
            .collect();
        m.insert(
            "predicate-row-estimates".into(),
            serde_json::Value::Array(predicate_estimates),
        );
        m
    }

    fn plan_children(&self) -> Vec<PlanChild<'_>> {
        self.fallback
            .as_deref()
            .map(|op| PlanChild::fallback(op as &dyn Operator))
            .into_iter()
            .collect()
    }

    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if self.state != OperatorState::Created {
            return Err(QueryError::Internal(
                "CyclicBgpOperator::open() called in invalid state".into(),
            ));
        }
        if self.open_fast_path(ctx)? {
            self.state = OperatorState::Open;
            return Ok(());
        }
        if let Some(fallback) = self.fallback.as_mut() {
            fallback.open(ctx).await?;
            self.state = OperatorState::Open;
            return Ok(());
        }
        self.state = OperatorState::Exhausted;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if !self.state.can_next() {
            return Ok(None);
        }
        if !self.used_fast_path {
            return match self.fallback.as_mut() {
                Some(fallback) => fallback.next_batch(ctx).await,
                None => Ok(None),
            };
        }

        match self.join_mode {
            CyclicJoinMode::RefOnly => self.next_ref_batch(),
            CyclicJoinMode::EncodedObject => self.next_encoded_batch(),
        }
    }

    fn close(&mut self) {
        if let Some(fallback) = self.fallback.as_mut() {
            fallback.close();
        }
        self.ref_relations.clear();
        self.relations.clear();
        self.ref_pending.clear();
        self.pending.clear();
        self.square_wedge = None;
        self.probed_edges = 0;
        self.state = OperatorState::Closed;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::triple::Term;
    use crate::seed::EmptyOperator;
    use fluree_db_core::Sid;

    fn triple(s: u16, p: &str, o: u16) -> TriplePattern {
        TriplePattern::new(
            Ref::Var(VarId(s)),
            Ref::Sid(Sid::new(100, p)),
            Term::Var(VarId(o)),
        )
    }

    fn ref_rel(edge: &CyclicEdge, rows: &[(u64, u64)]) -> RefRelationIndex {
        RefRelationIndex::new(
            edge.clone(),
            rows.iter()
                .map(|(subject, object)| RefEdgeRow {
                    subject: *subject,
                    object: *object,
                })
                .collect(),
        )
    }

    fn rel(edge: &CyclicEdge, rows: &[(u64, u64)]) -> RelationIndex {
        RelationIndex::new(
            edge.clone(),
            rows.iter()
                .map(|(subject, object)| EdgeRow {
                    subject: *subject,
                    object: Binding::encoded_sid(*object),
                })
                .collect(),
        )
    }

    fn operator_for(triples: &[TriplePattern]) -> CyclicBgpOperator {
        let plan = analyze_cyclic_bgp(triples, None).expect("test shape should be cyclic");
        CyclicBgpOperator::new(
            plan,
            None,
            TemporalMode::Current,
            Box::new(EmptyOperator::new()),
        )
    }

    #[test]
    fn object_only_cycle_vars_accept_encoded_literals() {
        let triples = vec![
            triple(0, "p1", 1),
            triple(0, "p2", 2),
            triple(3, "p3", 1),
            triple(3, "p4", 2),
        ];
        let op = operator_for(&triples);
        assert_eq!(op.join_mode, CyclicJoinMode::EncodedObject);
        let object = op.object_binding_for_edge(
            &op.plan.edges[0],
            RawEdgeRow {
                subject: 10,
                o_type: OType::XSD_STRING.as_u16(),
                object: 42,
                p_id: 7,
            },
        );

        assert!(matches!(
            object,
            Some(Binding::EncodedLit { o_key: 42, .. })
        ));
    }

    #[test]
    fn object_only_cycle_vars_accept_encoded_temporal_values() {
        let triples = vec![
            triple(0, "p1", 1),
            triple(0, "p2", 2),
            triple(3, "p3", 1),
            triple(3, "p4", 2),
        ];
        let op = operator_for(&triples);
        assert_eq!(op.join_mode, CyclicJoinMode::EncodedObject);
        let object = op.object_binding_for_edge(
            &op.plan.edges[0],
            RawEdgeRow {
                subject: 10,
                o_type: OType::XSD_DATE.as_u16(),
                object: 12_345,
                p_id: 7,
            },
        );

        assert!(matches!(
            object,
            Some(Binding::EncodedLit { o_key: 12_345, .. })
        ));
    }

    #[test]
    fn subject_bridge_cycle_vars_still_require_ref_objects() {
        let triples = vec![triple(0, "p1", 1), triple(1, "p2", 2), triple(2, "p3", 0)];
        let op = operator_for(&triples);
        assert_eq!(op.join_mode, CyclicJoinMode::RefOnly);

        let literal = op.object_binding_for_edge(
            &op.plan.edges[0],
            RawEdgeRow {
                subject: 10,
                o_type: OType::XSD_STRING.as_u16(),
                object: 42,
                p_id: 7,
            },
        );
        assert!(literal.is_none());

        let iri = op.object_binding_for_edge(
            &op.plan.edges[0],
            RawEdgeRow {
                subject: 10,
                o_type: OType::IRI_REF.as_u16(),
                object: 99,
                p_id: 7,
            },
        );
        assert!(matches!(iri, Some(Binding::EncodedSid { s_id: 99, .. })));
    }

    #[test]
    fn ref_pruning_keeps_only_values_supported_by_all_incident_edges() {
        let triples = vec![triple(0, "p1", 1), triple(1, "p2", 2), triple(2, "p3", 0)];
        let op = operator_for(&triples);

        let relations = vec![
            ref_rel(&op.plan.edges[0], &[(1, 10), (9, 90)]),
            ref_rel(&op.plan.edges[1], &[(10, 20), (90, 99)]),
            ref_rel(&op.plan.edges[2], &[(20, 1)]),
        ];

        let pruned = op.prune_ref_relations(relations);
        let sizes: Vec<usize> = pruned.iter().map(|rel| rel.rows.len()).collect();
        assert_eq!(sizes, vec![1, 1, 1]);
        assert_eq!(pruned[0].rows[0].subject, 1);
        assert_eq!(pruned[1].rows[0].object, 20);
    }

    #[test]
    fn next_relation_prefers_lower_bound_endpoint_fanout_over_total_rows() {
        let triples = vec![
            triple(0, "driver", 1),
            triple(0, "low_fanout", 2),
            triple(1, "high_fanout", 3),
            triple(3, "close", 2),
        ];
        let op = operator_for(&triples);

        let low_fanout_rows: Vec<(u64, u64)> = (1..=100).map(|v| (v, 10_000 + v)).collect();
        let high_fanout_rows: Vec<(u64, u64)> = (1..=50).map(|v| (1, 20_000 + v)).collect();
        let relations = vec![
            ref_rel(&op.plan.edges[0], &[(1, 1)]),
            ref_rel(&op.plan.edges[1], &low_fanout_rows),
            ref_rel(&op.plan.edges[2], &high_fanout_rows),
            ref_rel(&op.plan.edges[3], &[(20_001, 10_001)]),
        ];
        let mut assigned = vec![None; op.plan.vars.len()];
        assigned[op.var_pos(VarId(0))] = Some(1);
        assigned[op.var_pos(VarId(1))] = Some(1);
        let used = vec![true, false, false, false];

        let next = op
            .choose_next_ref_relation_with(&relations, &assigned, &used)
            .expect("one relation should be selected");
        assert_eq!(next, 1);
    }

    #[test]
    fn encoded_square_wedge_uses_smaller_exact_pair_side() {
        let triples = vec![
            triple(0, "left_a", 1),
            triple(0, "left_b", 2),
            triple(3, "right_a", 1),
            triple(3, "right_b", 2),
        ];
        let op = operator_for(&triples);
        let relations = vec![
            // Center ?v0 has 2 * 2 = 4 pairs.
            rel(&op.plan.edges[0], &[(1, 10), (1, 11)]),
            rel(&op.plan.edges[1], &[(1, 20), (1, 21)]),
            // Center ?v3 has 1 * 1 = 1 pair, so it should be the build side.
            rel(&op.plan.edges[2], &[(3, 10)]),
            rel(&op.plan.edges[3], &[(3, 20)]),
        ];

        let state = op
            .open_encoded_square_wedge(&relations)
            .expect("square wedge should be selected");
        assert!(matches!(state.plan.build_center, VarId(1 | 3)));
        assert_ne!(state.plan.build_center, state.plan.probe_center);
        assert_eq!(state.plan.build_pairs, 1);
        assert!(state.plan.probe_pairs >= state.plan.build_pairs);
    }

    #[test]
    fn encoded_square_wedge_streams_probe_pairs_and_outputs_matches() {
        let triples = vec![
            triple(0, "left_a", 1),
            triple(0, "left_b", 2),
            triple(3, "right_a", 1),
            triple(3, "right_b", 2),
        ];
        let mut op = operator_for(&triples);
        let relations = vec![
            rel(&op.plan.edges[0], &[(1, 10), (2, 99)]),
            rel(&op.plan.edges[1], &[(1, 20), (2, 20)]),
            rel(&op.plan.edges[2], &[(3, 10)]),
            rel(&op.plan.edges[3], &[(3, 20)]),
        ];
        let relations = op.prune_relations(relations);
        let state = op
            .open_encoded_square_wedge(&relations)
            .expect("square wedge should be selected");
        op.relations = relations;
        op.square_wedge = Some(state);

        let batch = op
            .next_encoded_batch()
            .expect("square wedge batch should succeed")
            .expect("one batch should be produced");
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.get_by_col(0, 0), &Binding::encoded_sid(1));
        assert_eq!(batch.get_by_col(0, 1), &Binding::encoded_sid(10));
        assert_eq!(batch.get_by_col(0, 2), &Binding::encoded_sid(20));
        assert_eq!(batch.get_by_col(0, 3), &Binding::encoded_sid(3));
    }

    #[test]
    fn probe_gate_requires_known_estimate_and_scan_ratio() {
        // Absent estimate means "likely empty" — full scan is already cheap.
        assert!(!should_probe_edge(10, None));
        // Probe only when the estimated scan is >= ratio rows per probe.
        let ratio = bounded_probe_scan_ratio();
        assert!(should_probe_edge(100, Some(100 * ratio)));
        assert!(!should_probe_edge(100, Some(100 * ratio - 1)));
        // Frontier above the subject cap never probes.
        let over_cap = max_bounded_probe_subjects() + 1;
        assert!(!should_probe_edge(over_cap, Some(u64::MAX)));
        // An empty frontier trivially passes (the caller skips I/O entirely).
        assert!(should_probe_edge(0, Some(1)));
    }

    #[test]
    fn frontier_intersects_across_all_scanned_relations() {
        // Directed triangle: ?0 -p1-> ?1 -p2-> ?2 -p3-> ?0 (RefOnly shape, but
        // the encoded frontier helper sees the same edge endpoints).
        let triples = vec![triple(0, "p1", 1), triple(1, "p2", 2), triple(2, "p3", 0)];
        let op = operator_for(&triples);

        // Var ?1 is object of edge0 and subject of edge1. With both scanned,
        // the frontier is the intersection of edge0's objects and edge1's subjects.
        let scanned = vec![
            rel(&op.plan.edges[0], &[(1, 10), (2, 11), (3, 12)]),
            rel(&op.plan.edges[1], &[(10, 20), (12, 21), (99, 22)]),
        ];
        let frontier = op
            .frontier_for_var(&scanned, VarId(1))
            .expect("frontier should be derivable");
        let mut got: Vec<u64> = frontier.into_iter().collect();
        got.sort_unstable();
        assert_eq!(got, vec![10, 12]);

        // A var not exposed by any scanned relation has no frontier.
        assert!(op.frontier_for_var(&scanned[..1], VarId(2)).is_none());
    }

    #[test]
    fn frontier_from_object_side_requires_encoded_sids() {
        let triples = vec![
            triple(0, "p1", 1),
            triple(0, "p2", 2),
            triple(3, "p3", 1),
            triple(3, "p4", 2),
        ];
        let op = operator_for(&triples);

        // ?1 appears only as an object; EncodedSid objects yield a frontier.
        let scanned = vec![rel(&op.plan.edges[0], &[(1, 10), (2, 11)])];
        let frontier = op
            .frontier_for_var(&scanned, VarId(1))
            .expect("encoded-sid objects should bound the var");
        assert_eq!(frontier.len(), 2);

        // Non-sid object bindings can never join a subject var, so they're
        // filtered out of the frontier (here: down to empty, meaning the
        // overall result is provably empty).
        let lit = op
            .object_binding_for_edge(
                &op.plan.edges[0],
                RawEdgeRow {
                    subject: 1,
                    o_type: OType::XSD_STRING.as_u16(),
                    object: 42,
                    p_id: 7,
                },
            )
            .expect("object-only var accepts encoded literals");
        let lit_rel = RelationIndex::new(
            op.plan.edges[0].clone(),
            vec![EdgeRow {
                subject: 1,
                object: lit,
            }],
        );
        let frontier = op
            .frontier_for_var(&[lit_rel], VarId(1))
            .expect("incident relation still yields a frontier");
        assert!(frontier.is_empty());
    }

    #[test]
    fn ref_frontier_intersects_subject_and_object_sides() {
        let triples = vec![triple(0, "p1", 1), triple(1, "p2", 2), triple(2, "p3", 0)];
        let op = operator_for(&triples);

        let scanned = vec![
            ref_rel(&op.plan.edges[0], &[(1, 10), (2, 11)]),
            ref_rel(&op.plan.edges[1], &[(10, 20), (11, 21), (50, 22)]),
        ];
        let frontier = CyclicBgpOperator::ref_frontier_for_var(&scanned, VarId(1))
            .expect("frontier should be derivable");
        let mut got: Vec<u64> = frontier.into_iter().collect();
        got.sort_unstable();
        assert_eq!(got, vec![10, 11]);
    }

    #[test]
    fn cheapest_remaining_treats_absent_estimate_as_empty() {
        let triples = vec![triple(0, "p1", 1), triple(1, "p2", 2), triple(2, "p3", 0)];
        let op = operator_for(&triples);
        let mut edges: Vec<CyclicEdge> = op.plan.edges.to_vec();
        edges[0].estimate = Some(5);
        edges[1].estimate = None;
        edges[2].estimate = Some(1);

        // Stats-absent (edge 1) sorts before every known estimate.
        let remaining = vec![0, 1, 2];
        let pos = CyclicBgpOperator::cheapest_remaining(&edges, &remaining);
        assert_eq!(remaining[pos], 1);

        let remaining = vec![0, 2];
        let pos = CyclicBgpOperator::cheapest_remaining(&edges, &remaining);
        assert_eq!(remaining[pos], 2);
    }
}
