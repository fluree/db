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
    allow_cursor_fast_path, build_psot_cursor_for_predicate, cursor_projection_sid_otype_okey,
    normalize_pred_sid,
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
}

pub(crate) struct CyclicBgpOperator {
    plan: CyclicBgpPlan,
    schema: Arc<[VarId]>,
    schema_positions: Arc<[usize]>,
    mode: TemporalMode,
    state: OperatorState,
    fallback: Option<BoxedOperator>,
    relations: Vec<RelationIndex>,
    driver_idx: usize,
    driver_pos: usize,
    pending: VecDeque<Vec<Binding>>,
    used_fast_path: bool,
}

impl CyclicBgpOperator {
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
        Self {
            plan,
            schema: Arc::from(schema.into_boxed_slice()),
            schema_positions: Arc::from(schema_positions.into_boxed_slice()),
            mode,
            state: OperatorState::Created,
            fallback: Some(fallback),
            relations: Vec::new(),
            driver_idx: 0,
            driver_pos: 0,
            pending: VecDeque::new(),
            used_fast_path: false,
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

    fn scan_relation(
        &self,
        ctx: &ExecutionContext<'_>,
        edge: &CyclicEdge,
    ) -> Result<Option<Vec<EdgeRow>>> {
        let Some(store) = ctx.binary_store.as_ref() else {
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
            None => return Ok(None),
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
                    return Ok(None);
                }
            }
        }
        let mut rows = Vec::with_capacity(raw_rows.len());
        for raw in raw_rows {
            let Some(object) = self.object_binding_for_edge(edge, raw) else {
                return Ok(None);
            };
            rows.push(EdgeRow {
                subject: raw.subject,
                object,
            });
        }
        Ok(Some(rows))
    }

    fn open_fast_path(&mut self, ctx: &ExecutionContext<'_>) -> Result<bool> {
        if self.mode.is_history() || !allow_cursor_fast_path(ctx) {
            return Ok(false);
        }

        let mut relations = Vec::with_capacity(self.plan.edges.len());
        for edge in self.plan.edges.iter() {
            let Some(rows) = self.scan_relation(ctx, edge)? else {
                return Ok(false);
            };
            if rows.is_empty() {
                relations.push(RelationIndex::new(edge.clone(), rows));
                self.relations = relations;
                self.driver_idx = 0;
                self.used_fast_path = true;
                return Ok(true);
            }
            relations.push(RelationIndex::new(edge.clone(), rows));
        }
        self.driver_idx = relations
            .iter()
            .enumerate()
            .min_by_key(|(_, rel)| {
                (
                    rel.edge.estimate.unwrap_or(rel.rows.len() as u64),
                    rel.rows.len(),
                )
            })
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        self.relations = relations;
        self.used_fast_path = true;
        Ok(true)
    }

    fn choose_next_relation(&self, assigned: &[Option<Binding>], used: &[bool]) -> Option<usize> {
        self.relations
            .iter()
            .enumerate()
            .filter(|(idx, _)| !used[*idx])
            .max_by_key(|(_, rel)| {
                let s_bound = assigned[self.var_pos(rel.edge.subject)].is_some();
                let o_bound = assigned[self.var_pos(rel.edge.object)].is_some();
                (
                    (s_bound as u8 + o_bound as u8),
                    std::cmp::Reverse(rel.rows.len()),
                )
            })
            .map(|(idx, _)| idx)
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

    fn assignment_to_columns(&self, assignment: &[Binding], cols: &mut [Vec<Binding>]) {
        for (out_idx, var_idx) in self.schema_positions.iter().copied().enumerate() {
            cols[out_idx].push(assignment[var_idx].clone());
        }
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
        m.insert("object-only-values".into(), "encoded".into());
        let predicates: Vec<serde_json::Value> = self
            .plan
            .edges
            .iter()
            .map(|edge| predicate_display(&edge.predicate).into())
            .collect();
        m.insert("predicates".into(), serde_json::Value::Array(predicates));
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

    fn close(&mut self) {
        if let Some(fallback) = self.fallback.as_mut() {
            fallback.close();
        }
        self.relations.clear();
        self.pending.clear();
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
    fn subject_bridge_cycle_vars_still_require_ref_objects() {
        let triples = vec![triple(0, "p1", 1), triple(1, "p2", 2), triple(2, "p3", 0)];
        let op = operator_for(&triples);

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
}
