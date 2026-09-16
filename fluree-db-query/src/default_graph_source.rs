//! Default-graph-source operator — iterate the dataset's default graph
//! sources and run an inner subplan once per source.
//!
//! This is a planner-internal construct. The
//! `expand_edge_annotation_patterns` pass synthesizes
//! [`Pattern::DefaultGraphSource`] around each expanded edge-annotation
//! triple chain so that under multi-source default-graph queries
//! (`from: [g1, g2]`), each source's base edge correlates only with
//! its own annotation flakes — without this wrapper the f:reifies*
//! lookups fan across all sources via `DatasetOperator` and produce
//! an N×M cross-product against each base-edge match.
//!
//! Distinct from [`crate::graph::GraphOperator`]:
//! - `GraphOperator` implements SPARQL `GRAPH ?g { ... }` semantics —
//!   it iterates **named** graphs only.
//! - This operator iterates **default** graphs (`from: [...]`
//!   sources) and binds no variable. Per-source correlation is
//!   purely an execution-context switch (`with_graph_ref`); rows
//!   carry only inner-subplan bindings.
//!
//! In single-source default-graph mode (no dataset attached) the
//! wrapper is a no-op: [`DefaultGraphSourceOperator::open`] builds the
//! inner subplan **once**, seeded by the whole child stream, and
//! streams it directly. This lets the base edge hash-join the child
//! instead of replanning + re-executing the inner subplan per parent
//! row — the latter made an annotated object-join O(parent rows) and
//! was the cause of IC5's timeout. The per-row, per-source path is
//! used only when a multi-source dataset is actually attached.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::execute::build_where_operators_seeded;
use crate::ir::{Pattern, Ref};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::seed::{EmptyOperator, SeedOperator};
use crate::temporal_mode::PlanningContext;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::annotation_arena::DEFAULT_TARGET_ROWS_PER_LEAF;
use fluree_db_core::{Sid, StatsView};
use std::collections::HashSet;
use std::sync::Arc;

use crate::annotation_edge_probe::HASH_ANNOTATION_MIN_DRIVING_ROWS;

/// Resolve a recognized relationship predicate ref to a concrete `Sid`
/// for this snapshot. Cypher lowers relationship types to `Ref::Iri`, so
/// the common case is an IRI encode; `None` means the predicate isn't
/// present in this ledger's namespace table (no edges → generic
/// fallback, which yields the same empty result more slowly).
fn resolve_pred_sid(p: &Ref, ctx: &ExecutionContext<'_>) -> Option<Sid> {
    match p {
        Ref::Sid(sid) => Some(sid.clone()),
        Ref::Iri(iri) => ctx.active_snapshot.encode_iri(iri),
        Ref::Var(_) => None,
    }
}

/// The physical lane the delegate takes for a recognized chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChainLane {
    /// Forward-arena probe: scan the base edge, probe the arena per row.
    Arena,
    /// Annotation-first: stream the reverse arena, no base scan.
    Enumerate,
    /// Per-reifier point probes (the hash lane or the generic chain).
    Chain,
}

/// Entry costs of one wrapper with the child's variables bound, in rows
/// walked per driving row (see `DefaultGraphSourceOperator::chain_lane`).
struct LaneInputs {
    /// Base-edge rows an edge-first lane scans.
    edge_first: f64,
    /// Reifier candidates the chain's cheapest `f:reifies*` lookup yields.
    probe_first: f64,
    /// Live rows in the sealed arena (`f:reifiesSubject` count), if known.
    arena_rows: Option<f64>,
    /// The base-edge subject is a constant or bound by the child.
    subject_bound: bool,
    /// The child already binds the reifier.
    reifier_bound: bool,
    /// The chain runs without its redundant checks (see
    /// [`elide_redundant_chain`]): one range scan plus sequential batched
    /// probes instead of a point scan per reifier.
    chain_elided: bool,
}

/// One whole-leaf decode of the forward arena, in probed-row equivalents.
/// A leaf is a 4,096-row CBOR blob of string-keyed rows; decoding one costs
/// ~2.3 ms against ~6 µs per probed edge row (full StarBench ledger, where
/// 82% of P11's samples were leaf decode).
const LEAF_ROW_EQUIV: f64 = 400.0;

/// Pick the lane by entry cost. One scattered point probe is worth about
/// `PROBE_ROW_EQUIV` sequential rows (the crossover measured on the
/// StarBench slice); the arena probe is never allowed to buffer more than
/// `BUFFERED_BASE_MAX_ROWS`, and it pays `LEAF_ROW_EQUIV` per leaf it
/// touches.
fn choose_chain_lane(inputs: LaneInputs) -> ChainLane {
    const PROBE_ROW_EQUIV: f64 = 16.0;
    // An elided chain drives from one `f:reifies*` range scan in reifier
    // order and every later probe is a batched sequential walk: ~2 rows per
    // reifier (P11 full scale: 1.1M reifiers, ~2 µs each, against ~6 µs per
    // arena-probed edge row).
    const SEQUENTIAL_ROW_EQUIV: f64 = 2.0;
    // Each live pair the enumeration emits costs a CBOR row decode plus two
    // dictionary re-encodes of the edge endpoints: ~6× an elided chain's row
    // (P2 full scale: 65 s enumerating 21.4M pairs, 10.6 s on the elided
    // chain), still far under the generic chain's point scan per reifier.
    const ENUMERATE_ROW_EQUIV: f64 = 12.0;
    const BUFFERED_BASE_MAX_ROWS: f64 = 20_000_000.0;
    let LaneInputs {
        edge_first,
        probe_first,
        arena_rows,
        subject_bound,
        reifier_bound,
        chain_elided,
    } = inputs;
    // A bound reifier makes the chain one point probe per driving row; no
    // sweep beats that (P22 / C7 / C10 regressed 200× on the arena here).
    if reifier_bound {
        return ChainLane::Chain;
    }
    let buffered = edge_first <= BUFFERED_BASE_MAX_ROWS;
    // A bound subject keys into one arena leaf per driving row, while the
    // chain's `f:reifiesSubject <s>` lookup walks every reifier of that
    // subject — a hub tail the estimates cannot see (StarBench P23 went
    // from 46 ms to 192 s on the chain). Keep the point probe.
    if subject_bound {
        return if buffered {
            ChainLane::Arena
        } else {
            ChainLane::Chain
        };
    }
    // An unbound subject spreads the edges across the (s, p, o)-ordered
    // arena, so the probe decodes about one leaf per edge up to the whole
    // arena — a predicate-wide probe pays the entire arena whatever the
    // predicate's size (P11 / S5 / C8 / C10 all cost the same 13–15 s).
    let rows_per_leaf = DEFAULT_TARGET_ROWS_PER_LEAF as f64;
    let total_leaves = arena_rows.map_or(edge_first, |rows| (rows / rows_per_leaf).max(1.0));
    let arena_cost = if buffered {
        edge_first + edge_first.min(total_leaves) * LEAF_ROW_EQUIV
    } else {
        f64::INFINITY
    };
    let per_reifier = if chain_elided {
        SEQUENTIAL_ROW_EQUIV
    } else {
        PROBE_ROW_EQUIV
    };
    let chain_cost = probe_first * per_reifier;
    let enumerate_cost = arena_rows.map_or(f64::INFINITY, |rows| rows * ENUMERATE_ROW_EQUIV);
    if arena_cost <= chain_cost && arena_cost <= enumerate_cost {
        ChainLane::Arena
    } else if enumerate_cost < chain_cost {
        ChainLane::Enumerate
    } else {
        ChainLane::Chain
    }
}

/// Drop the chain triples the write invariants make redundant. Every
/// reifier carries exactly one `f:reifiesSubject`, `f:reifiesPredicate` and
/// `f:reifiesObject`, written into the edge's own graph, and its base triple
/// is asserted (`@reifies` without the base is rejected; retracting the base
/// cascades). So once a reifies lookup has bound the reifier, the base edge
/// never removes a row, and a reifies lookup whose position is a variable
/// nobody reads is a cardinality-one no-op. A constant position stays: it is
/// the constraint. A variable in two positions, or naming the reifier, counts
/// as read: its lookups carry the equality the base scan enforced
/// (`<< ?s :p ?s >>` must not match `:a :p :b`). A variable predicate that is
/// read stays too, and keeps
/// the base edge with it — the base scan binds it as a predicate, the
/// reifies lookup as a plain ref. At least one lookup always remains, so a
/// reifier bound by the body still has to be a reifier (P3 has three
/// `:derives_from` rows on plain subjects that P2 must not count).
///
/// Measured on the full StarBench ledger this turns the predicate-only chain
/// from one point scan per reifier (P11: 1.1M SPOT opens) into one POST
/// range plus the body's batched probe.
pub(crate) fn elide_redundant_chain(
    patterns: &[Pattern],
    needed_outside: &HashSet<VarId>,
    child_bound: &HashSet<VarId>,
) -> Option<Vec<Pattern>> {
    let shape = crate::annotation_edge_probe::recognize_annotation_edge(patterns)?;
    let Pattern::Triple(base) = &shape.base else {
        return None;
    };
    let mut referenced: HashSet<VarId> = needed_outside.clone();
    referenced.extend(child_bound.iter().copied());
    let mut counts: std::collections::HashMap<VarId, usize> = std::collections::HashMap::new();
    crate::execute::collect_var_stats(&shape.body, &mut counts, &mut referenced);
    // A variable in two base positions, or naming the reifier, is an
    // equality the base scan enforced; keeping its lookups keeps it.
    let mut seen: HashSet<VarId> = HashSet::from([shape.ann_var]);
    for v in [base.s.as_var(), base.p.as_var(), base.o.as_var()]
        .into_iter()
        .flatten()
    {
        if !seen.insert(v) {
            referenced.insert(v);
        }
    }

    if base.p.as_var().is_some_and(|v| referenced.contains(&v)) {
        return None;
    }
    // A constant position is a constraint; a variable position is one only
    // when something reads it.
    let position_kept = |var: Option<VarId>| var.is_none_or(|v| referenced.contains(&v));
    let keep = [
        position_kept(base.s.as_var()),
        position_kept(base.p.as_var()),
        position_kept(base.o.as_var()),
    ];
    let mut kept: Vec<Pattern> = patterns[1..=3]
        .iter()
        .zip(keep)
        .filter(|(_, keep)| *keep)
        .map(|(p, _)| p.clone())
        .collect();
    if kept.is_empty() {
        kept.push(patterns[1].clone());
    }
    kept.extend(shape.body.iter().cloned());
    Some(kept)
}

/// Whether the hash sidecar lane may take a recognized chain. It drains the
/// three `f:reifies*` predicates and sweeps the base edge before answering a
/// row, which beats per-row probes only against a large or unknown driving
/// stream and a bounded sweep. A child that already binds the reifier keeps
/// the chain, as in [`choose_chain_lane`]: the chain is then one point probe
/// per row, while the lane walks every swept edge per row when the edge
/// subject is unbound. A constant annotation value driving
/// `<< ?s :p ?o >> :q "v"` went from 22 s to 55 s on a 20k-edge ledger that way.
fn hash_lane_admits(driving_rows: Option<usize>, sweep_bounded: bool, reifier_bound: bool) -> bool {
    !reifier_bound
        && sweep_bounded
        && driving_rows.is_none_or(|n| n >= HASH_ANNOTATION_MIN_DRIVING_ROWS)
}

/// Diagnostic override: `FLUREE_ANNOTATION_LANE=arena|enumerate|chain` pins
/// the lane regardless of cost so the same query can be timed on the same
/// ledger per lane. The runtime gates still apply — a forced arena or
/// enumeration lane without a sealed arena falls through to the chain.
fn forced_chain_lane() -> Option<ChainLane> {
    match std::env::var("FLUREE_ANNOTATION_LANE").ok()?.as_str() {
        "arena" => Some(ChainLane::Arena),
        "enumerate" => Some(ChainLane::Enumerate),
        "chain" => Some(ChainLane::Chain),
        _ => None,
    }
}

/// What the annotation-first enumeration lane binds (see
/// `DefaultGraphSourceOperator::enumeration_plan`).
struct EnumerationPlan {
    s_var: VarId,
    p_pos: crate::annotation_edge_probe::EdgePos,
    o_var: VarId,
    estimated: Option<usize>,
}

pub struct DefaultGraphSourceOperator {
    child: BoxedOperator,
    inner_patterns: Vec<Pattern>,
    schema: Arc<[VarId]>,
    state: OperatorState,
    result_buffer: Vec<Vec<Binding>>,
    buffer_pos: usize,
    planning: PlanningContext,
    /// Planner stats for the inner subplan build. Without these the base edge
    /// cannot be costed and falls back to a per-driving-row object scan of the
    /// whole edge predicate instead of an object→subject hash join — the inner
    /// build previously passed `None`, which is what kept the annotated
    /// `HAS_MEMBER` join slow even once it was built once.
    stats: Option<Arc<StatsView>>,
    /// Single default-graph fast path: when no dataset is attached the
    /// per-source correlation is unnecessary, so the inner subplan is built
    /// ONCE seeded by the whole child stream (base edge can hash-join) and
    /// streamed directly, instead of replanning + re-executing per parent row.
    single_graph_delegate: Option<BoxedOperator>,
    /// Planner estimate of the wrapped chain's output, with the child's
    /// variables bound — the same figure `reorder_patterns` placed the
    /// wrapper by. Reported through `estimated_rows` so the join above sees
    /// a real driving count instead of the `None` that read as one row.
    estimated: Option<usize>,
    /// Variables read outside the wrapper (later siblings and the post-WHERE
    /// pipeline); an inner position bound to none of them can drop its
    /// `f:reifies*` lookup (see [`elide_redundant_chain`]).
    needed_outside: HashSet<VarId>,
}

impl DefaultGraphSourceOperator {
    pub fn new(
        child: BoxedOperator,
        inner_patterns: Vec<Pattern>,
        planning: PlanningContext,
        stats: Option<Arc<StatsView>>,
        needed_outside: HashSet<VarId>,
    ) -> Self {
        let mut seen: std::collections::HashSet<VarId> = child.schema().iter().copied().collect();

        // New vars in deterministic first-occurrence order across the inner
        // patterns. A `HashSet` here makes the output column order vary per
        // process (HashSet iteration is seeded randomly), and the inner
        // subplan emits batches in pattern order — the mismatch silently
        // dropped whole result batches ~half the time. Pattern order is what
        // the inner subplan (generic chain or the edge-annotation probe)
        // actually produces, so the reported schema and the emitted batches
        // agree.
        let mut new_vars: Vec<VarId> = Vec::new();
        for p in &inner_patterns {
            for v in p.produced_vars() {
                if seen.insert(v) {
                    new_vars.push(v);
                }
            }
        }

        let mut schema_vec: Vec<VarId> = child.schema().to_vec();
        schema_vec.extend(&new_vars);
        let schema = Arc::from(schema_vec.into_boxed_slice());

        let bound: HashSet<VarId> = child.schema().iter().copied().collect();
        let estimated = crate::planner::estimate_annotation_chain_cardinality(
            &inner_patterns,
            &bound,
            stats.as_deref(),
        )
        .unwrap_or_else(|| {
            crate::planner::estimate_branch_cardinality_from(
                &inner_patterns,
                &bound,
                stats.as_deref(),
            )
        });
        let estimated = Some(estimated.round().max(1.0) as usize);

        Self {
            child,
            inner_patterns,
            schema,
            state: OperatorState::Created,
            result_buffer: Vec::new(),
            buffer_pos: 0,
            planning,
            stats,
            single_graph_delegate: None,
            estimated,
            needed_outside,
        }
    }

    /// Build the single-graph inner subplan. When the chain is a
    /// recognized edge-annotation shape and every fast-path gate holds,
    /// replace the three generic `f:reifies*` joins with a forward-arena
    /// probe (the physical counterpart to a Cypher relationship binding);
    /// otherwise fall back to the ordinary join chain — same results,
    /// just the slower generic path.
    fn build_single_graph_delegate(
        &self,
        child: BoxedOperator,
        ctx: &ExecutionContext<'_>,
    ) -> Result<BoxedOperator> {
        tracing::debug!(
            arena = ctx.active_snapshot.annotation_index.is_some(),
            store = ctx.active_snapshot.content_store.is_some(),
            history = self.planning.is_history(),
            overlay_empty = ctx.overlay().is_effectively_empty(),
            root_policy = ctx.policy_enforcer.as_ref().is_none_or(|p| p.is_root()),
            multi_ledger = ctx.is_multi_ledger(),
            driving_est = ?child.estimated_rows(),
            recognized = crate::annotation_edge_probe::recognize_annotation_edge(
                &self.inner_patterns
            )
            .is_some(),
            "annotation delegate gates"
        );
        let child_bound: HashSet<VarId> = child.schema().iter().copied().collect();
        let elided = if self.elision_gates_pass(ctx) {
            elide_redundant_chain(&self.inner_patterns, &self.needed_outside, &child_bound)
        } else {
            None
        };
        let lane = self.chain_lane(&child, elided.is_some());
        if self.annotation_probe_gates_pass(ctx) && lane == ChainLane::Arena {
            if let Some(shape) =
                crate::annotation_edge_probe::recognize_annotation_edge(&self.inner_patterns)
            {
                // Resolve the relationship predicate: a typed relationship
                // is a concrete IRI/Sid (encode for this snapshot — if it
                // can't be encoded the predicate has no data here, fall
                // back to the generic chain rather than guess); an untyped
                // `-[p]->` is a variable the base scan binds per row.
                let p_pos = match &shape.p_pred {
                    Ref::Var(v) => Some(crate::annotation_edge_probe::EdgePos::Var(*v)),
                    pred => resolve_pred_sid(pred, ctx)
                        .map(crate::annotation_edge_probe::EdgePos::Const),
                };
                if let Some(p_pos) = p_pos {
                    // Base edge plans normally (visibility + policy), seeded
                    // by the whole child stream.
                    let base = build_where_operators_seeded(
                        Some(child),
                        std::slice::from_ref(&shape.base),
                        self.stats.clone(),
                        None,
                        &self.planning,
                    )?;
                    let probe = Box::new(
                        crate::annotation_edge_probe::AnnotationEdgeProbeOperator::new(
                            base,
                            shape.ann_var,
                            shape.s_pos,
                            p_pos,
                            shape.o_pos,
                        ),
                    );
                    // Body (relationship-property reads, filters) plans
                    // normally on top, with the reifier var now bound.
                    tracing::debug!(lane = "arena", "annotation delegate lane");
                    return build_where_operators_seeded(
                        Some(probe),
                        &shape.body,
                        self.stats.clone(),
                        None,
                        &self.planning,
                    );
                }
            }
        }
        // Annotation-first enumeration: the arena lane was declined because
        // the base edge is the wider entry point (a wildcard, or a predicate
        // larger than the arena). Stream the arena instead of scanning the
        // base edge — see `AnnotationEnumerateOperator` for why no base-edge
        // check is needed under these gates.
        if self.annotation_probe_gates_pass(ctx) && lane == ChainLane::Enumerate {
            if let Some(shape) =
                crate::annotation_edge_probe::recognize_annotation_edge(&self.inner_patterns)
            {
                if let Some(plan) = self.enumeration_plan(&shape, &child, ctx) {
                    let op = Box::new(
                        crate::annotation_edge_probe::AnnotationEnumerateOperator::new(
                            child,
                            shape.ann_var,
                            plan.s_var,
                            plan.p_pos,
                            plan.o_var,
                            plan.estimated,
                        ),
                    );
                    tracing::debug!(lane = "enumerate", "annotation delegate lane");
                    return build_where_operators_seeded(
                        Some(op),
                        &shape.body,
                        self.stats.clone(),
                        None,
                        &self.planning,
                    );
                }
            }
        }
        // Required-lane hash fallback: no sealed arena (bulk-imported and
        // reindexed-without-annotations roots) or an arena gate failed. The
        // generic chain evaluates the base edge and the three `f:reifies*`
        // joins per driving row; for a large driving stream (a 21k-row
        // UNWIND) that is tens of thousands of scattered scan re-opens.
        // Drain the sidecar and sweep the base pattern ONCE instead and
        // answer each row by hash lookup — but only when the driving
        // stream is large or unknown (below the threshold the per-row
        // probes beat the sweeps) AND the base sweep is bounded (an
        // untyped relationship sweeps the whole default graph once, so
        // very large ledgers keep the per-row chain).
        if self.hash_annotation_gates_pass(ctx) {
            if let Some(shape) =
                crate::annotation_edge_probe::recognize_annotation_edge(&self.inner_patterns)
            {
                let p_pos = match &shape.p_pred {
                    Ref::Var(v) => Some(crate::annotation_edge_probe::EdgePos::Var(*v)),
                    pred => resolve_pred_sid(pred, ctx)
                        .map(crate::annotation_edge_probe::EdgePos::Const),
                };
                let admitted = hash_lane_admits(
                    child.estimated_rows(),
                    self.base_sweep_bounded(&shape),
                    child_bound.contains(&shape.ann_var),
                );
                if let (
                    Some(p_pos),
                    true,
                    Pattern::Triple(base_tp),
                    Pattern::Triple(r_subj),
                    Pattern::Triple(r_pred),
                    Pattern::Triple(r_obj),
                ) = (
                    p_pos,
                    admitted,
                    &self.inner_patterns[0],
                    &self.inner_patterns[1],
                    &self.inner_patterns[2],
                    &self.inner_patterns[3],
                ) {
                    let probe = Box::new(
                        crate::annotation_edge_probe::HashAnnotationEdgeProbeOperator::new(
                            child,
                            base_tp.clone(),
                            shape.ann_var,
                            shape.s_pos,
                            p_pos,
                            shape.o_pos,
                            r_subj.clone(),
                            r_pred.clone(),
                            r_obj.clone(),
                            self.stats.clone(),
                            self.planning,
                        ),
                    );
                    tracing::debug!(lane = "hash", "annotation delegate lane");
                    return build_where_operators_seeded(
                        Some(probe),
                        &shape.body,
                        self.stats.clone(),
                        None,
                        &self.planning,
                    );
                }
            }
        }
        tracing::debug!(
            lane = "generic",
            elided = elided.is_some(),
            "annotation delegate lane"
        );
        build_where_operators_seeded(
            Some(child),
            elided.as_deref().unwrap_or(&self.inner_patterns),
            self.stats.clone(),
            None,
            &self.planning,
        )
    }

    /// The write invariants [`elide_redundant_chain`] relies on describe
    /// current state under full visibility: a history walk replays events
    /// the cascade later undid, a non-root policy may hide the base triple
    /// but not its reifier, and a dataset correlates sources per row.
    fn elision_gates_pass(&self, ctx: &ExecutionContext<'_>) -> bool {
        !self.planning.is_history()
            && !ctx.is_multi_ledger()
            && ctx.policy_enforcer.as_ref().is_none_or(|p| p.is_root())
    }

    /// Which physical lane the chain's entry costs favour. The three lanes
    /// scale with different things: the forward-arena probe with the base
    /// edge (it drains the base scan into memory and merge-probes the
    /// arena), the enumeration with the whole arena (every live pair is
    /// walked whatever the predicate), and the generic chain with the
    /// number of reifiers it must point-probe (three scattered lookups
    /// each). The reifier count is capped by the base-edge count — a reifier
    /// needs an edge — so the per-predicate NDV cannot put `TREATS` at 31k
    /// reifiers when it has 1.1M, and a child that already binds the
    /// reifier makes the chain a per-row probe no sweep can beat (P22 / C7
    /// / C10 regressed 200× when the arena lane was taken there).
    fn chain_lane(&self, child: &BoxedOperator, chain_elided: bool) -> ChainLane {
        if let Some(forced) = forced_chain_lane() {
            return forced;
        }
        let bound: HashSet<VarId> = child.schema().iter().copied().collect();
        let stats = self.stats.as_deref();
        let Some(shape) =
            crate::annotation_edge_probe::recognize_annotation_edge(&self.inner_patterns)
        else {
            return ChainLane::Arena;
        };
        let Some((edge_first, _)) =
            crate::planner::annotation_chain_entry_rows(&self.inner_patterns, &bound, stats)
        else {
            return ChainLane::Arena;
        };
        let Some(probe_first) =
            crate::planner::annotation_chain_probe_rows(&self.inner_patterns, &bound, stats)
        else {
            return ChainLane::Arena;
        };
        let subject_bound = match &shape.base {
            Pattern::Triple(tp) => match &tp.s {
                Ref::Var(v) => bound.contains(v),
                _ => true,
            },
            _ => true,
        };
        let arena_rows = stats.and_then(|s| {
            s.get_property(&Sid::new(
                fluree_vocab::namespaces::FLUREE_DB,
                fluree_vocab::db::REIFIES_SUBJECT,
            ))
            .map(|p| p.count as f64)
        });
        choose_chain_lane(LaneInputs {
            edge_first,
            probe_first,
            arena_rows,
            subject_bound,
            reifier_bound: bound.contains(&shape.ann_var),
            chain_elided,
        })
    }

    /// Eligibility for the annotation-first enumeration: both base-edge
    /// endpoints are variables the child leaves unbound (so an edge-first
    /// scan would sweep a whole predicate or the whole graph), the child
    /// binds none of the chain's variables (each live pair simply fans out
    /// per driving row), and the predicate is a variable or resolves for
    /// this snapshot. The row estimate is the reifier-first entry cost the
    /// planner already computed for the wrapper.
    fn enumeration_plan(
        &self,
        shape: &crate::annotation_edge_probe::AnnotationEdgeShape,
        child: &BoxedOperator,
        ctx: &ExecutionContext<'_>,
    ) -> Option<EnumerationPlan> {
        use crate::annotation_edge_probe::EdgePos;
        let (EdgePos::Var(s_var), EdgePos::Var(o_var)) = (&shape.s_pos, &shape.o_pos) else {
            return None;
        };
        let p_pos = match &shape.p_pred {
            Ref::Var(v) => EdgePos::Var(*v),
            pred => EdgePos::Const(resolve_pred_sid(pred, ctx)?),
        };
        let child_vars: HashSet<VarId> = child.schema().iter().copied().collect();
        let p_var = match &p_pos {
            EdgePos::Var(v) => Some(*v),
            EdgePos::Const(_) => None,
        };
        if child_vars.contains(s_var)
            || child_vars.contains(o_var)
            || child_vars.contains(&shape.ann_var)
            || p_var.is_some_and(|v| child_vars.contains(&v))
        {
            return None;
        }
        let estimated = crate::planner::annotation_chain_entry_rows(
            &self.inner_patterns,
            &child_vars,
            self.stats.as_deref(),
        )
        .map(|(_, reifier_first)| reifier_first.round().max(1.0) as usize);
        Some(EnumerationPlan {
            s_var: *s_var,
            p_pos,
            o_var: *o_var,
            estimated,
        })
    }

    /// Eligibility for the required-lane hash sidecar probe. Unlike the
    /// arena path, the drained `f:reifies*` scans are ordinary planned
    /// scans — overlay novelty and policy filtering apply — so neither an
    /// empty overlay nor root policy is required. History timelines change
    /// per-row visibility (the maps are one `to_t` snapshot of a planned
    /// scan, which history mode plans differently), and multi-ledger
    /// contexts have no single sidecar — both keep the generic chain.
    fn hash_annotation_gates_pass(&self, ctx: &ExecutionContext<'_>) -> bool {
        !self.planning.is_history() && !ctx.is_multi_ledger()
    }

    /// Is the probe's one-pass base-edge sweep bounded? A typed
    /// relationship sweeps one predicate partition (its stats count); an
    /// untyped one sweeps the whole default graph, bounded by the summed
    /// property counts. Stats absent → the ledger has no built index
    /// (novelty-scale) — trivially bounded.
    fn base_sweep_bounded(
        &self,
        shape: &crate::annotation_edge_probe::AnnotationEdgeShape,
    ) -> bool {
        // Backstop against pathological graphs, not a tuned optimum: ~100x
        // the validated scale (~190k reified edges / 72k nodes). See
        // docs/design/edge-annotations.md ("Buffering and the sweep
        // ceiling") for the derivation and what to measure to replace it.
        const BASE_SWEEP_MAX_ROWS: u64 = 20_000_000;
        let Some(stats) = self.stats.as_deref() else {
            return true;
        };
        let rows = match &shape.p_pred {
            Ref::Var(_) => stats.total_property_flakes(),
            Ref::Sid(sid) => stats.get_property(sid).map_or(0, |p| p.count),
            Ref::Iri(iri) => stats.get_property_by_iri(iri).map_or(0, |p| p.count),
        };
        rows <= BASE_SWEEP_MAX_ROWS
    }

    /// Eligibility for the forward-arena probe fast path. All checked
    /// against the live execution context so no plan-time vouch is
    /// needed. See `annotation_edge_probe` for why each matters.
    fn annotation_probe_gates_pass(&self, ctx: &ExecutionContext<'_>) -> bool {
        ctx.active_snapshot.annotation_index.is_some()
            && ctx.active_snapshot.content_store.is_some()
            && !self.planning.is_history()
            && ctx.overlay().is_effectively_empty()
            && ctx.policy_enforcer.as_ref().is_none_or(|p| p.is_root())
    }

    /// Run the inner subplan against a single source graph and merge
    /// each output row with the parent row.
    async fn execute_in_source(
        &mut self,
        parent_ctx: &ExecutionContext<'_>,
        graph: &crate::dataset::GraphRef<'_>,
        parent_batch: &Batch,
        row_idx: usize,
    ) -> Result<()> {
        let per_graph_ctx = parent_ctx.with_graph_ref(graph);
        self.run_inner_and_merge(&per_graph_ctx, parent_batch, row_idx)
            .await
    }

    /// Single-db fallback: no dataset means there's nothing to
    /// iterate; run the inner subplan once against the parent
    /// context. Mirrors what `GraphOperator` does for `?g unbound`
    /// without a dataset, minus the variable binding.
    async fn execute_in_default_singleton(
        &mut self,
        parent_ctx: &ExecutionContext<'_>,
        parent_batch: &Batch,
        row_idx: usize,
    ) -> Result<()> {
        self.run_inner_and_merge(parent_ctx, parent_batch, row_idx)
            .await
    }

    async fn run_inner_and_merge(
        &mut self,
        ctx: &ExecutionContext<'_>,
        parent_batch: &Batch,
        row_idx: usize,
    ) -> Result<()> {
        let seed = SeedOperator::from_batch_row(parent_batch, row_idx);
        let mut inner = build_where_operators_seeded(
            Some(Box::new(seed)),
            &self.inner_patterns,
            self.stats.clone(),
            None,
            &self.planning,
        )?;

        inner.open(ctx).await?;

        while let Some(batch) = inner.next_batch(ctx).await? {
            for inner_row_idx in 0..batch.len() {
                let mut merged_row: Vec<Binding> = Vec::with_capacity(self.schema.len());

                for var in self.child.schema() {
                    let binding = parent_batch
                        .get(row_idx, *var)
                        .cloned()
                        .unwrap_or(Binding::Unbound);
                    merged_row.push(binding);
                }

                let parent_len = self.child.schema().len();
                for var in self.schema.iter().skip(parent_len) {
                    let binding = batch
                        .get(inner_row_idx, *var)
                        .cloned()
                        .unwrap_or(Binding::Unbound);
                    merged_row.push(binding);
                }

                self.result_buffer.push(merged_row);
            }
        }

        inner.close();
        Ok(())
    }

    fn drain_buffer(&mut self) -> Result<Option<Batch>> {
        if self.buffer_pos >= self.result_buffer.len() {
            return Ok(None);
        }

        let num_cols = self.schema.len();
        if num_cols == 0 {
            let row_count = self.result_buffer.len() - self.buffer_pos;
            self.buffer_pos = self.result_buffer.len();
            return Ok((row_count > 0).then(|| Batch::empty_schema_with_len(row_count)));
        }

        let mut columns: Vec<Vec<Binding>> = (0..num_cols).map(|_| Vec::new()).collect();

        for row in &self.result_buffer[self.buffer_pos..] {
            for (col_idx, binding) in row.iter().enumerate() {
                if col_idx < columns.len() {
                    columns[col_idx].push(binding.clone());
                }
            }
        }

        self.buffer_pos = self.result_buffer.len();

        if columns.is_empty() || columns[0].is_empty() {
            Ok(None)
        } else {
            Ok(Some(Batch::new(self.schema.clone(), columns)?))
        }
    }
}

#[async_trait]
impl Operator for DefaultGraphSourceOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        // (see build_single_graph_delegate for the recognition/fallback split)
        // Single default-graph (no dataset): the per-source correlation this
        // wrapper exists for is a no-op, so build the inner subplan ONCE seeded
        // by the whole child stream. The base edge + f:reifies* triples then plan
        // as one normal join block — the base edge can hash-join the child — and
        // stream directly, instead of replanning and re-executing per parent row
        // (which made an annotated object-join O(parent rows): IC5's 65s cliff).
        // Multi-source datasets keep the per-row, per-source path below.
        if ctx.dataset.is_none() {
            let child = std::mem::replace(&mut self.child, Box::new(EmptyOperator::new()));
            let mut delegate = self.build_single_graph_delegate(child, ctx)?;
            delegate.open(ctx).await?;
            self.single_graph_delegate = Some(delegate);
        } else {
            self.child.open(ctx).await?;
        }
        self.state = OperatorState::Open;
        self.result_buffer.clear();
        self.buffer_pos = 0;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        // Single-graph fast path: stream the once-built inner subplan directly.
        // The delegate's batch column order comes from the REORDERED inner
        // chain (plus whichever probe lane fired), which need not match this
        // operator's declared schema — re-project so positional consumers
        // above (NestedLoopJoin bind instructions read by column index) see
        // the contract they were planned against. Matching layouts pass
        // through untouched.
        if let Some(delegate) = self.single_graph_delegate.as_mut() {
            let out = delegate.next_batch(ctx).await?;
            let Some(batch) = out else { return Ok(None) };
            if batch.schema() == self.schema.as_ref() {
                return Ok(Some(batch));
            }
            let columns: Vec<Vec<Binding>> = self
                .schema
                .iter()
                .map(|var| match batch.column(*var) {
                    Some(col) => col.to_vec(),
                    None => vec![Binding::Unbound; batch.len()],
                })
                .collect();
            return Ok(Some(Batch::new(Arc::clone(&self.schema), columns)?));
        }

        if self.buffer_pos < self.result_buffer.len() {
            return self.drain_buffer();
        }

        loop {
            let parent_batch = match self.child.next_batch(ctx).await? {
                Some(b) if !b.is_empty() => b,
                Some(_) => continue,
                None => {
                    self.state = OperatorState::Exhausted;
                    return Ok(None);
                }
            };

            self.result_buffer.clear();
            self.buffer_pos = 0;

            // Iterate the dataset's default graphs. When no dataset
            // is attached (single-db mode), the inner subplan runs
            // once against the existing context — the wrapper is a
            // no-op for the single-graph path.
            for row_idx in 0..parent_batch.len() {
                if let Some(ds) = ctx.dataset {
                    // Iterate by index so the borrow of `ds` is
                    // re-acquired per iteration, freeing the borrow
                    // checker to let `execute_in_source` take `&mut
                    // self` between iterations. GraphRef isn't Clone
                    // (it carries borrowed snapshot references), so
                    // we can't materialize the slice into an owned
                    // Vec.
                    let n = ds.default_graphs().len();
                    for gi in 0..n {
                        let graph = &ds.default_graphs()[gi];
                        self.execute_in_source(ctx, graph, &parent_batch, row_idx)
                            .await?;
                    }
                } else {
                    self.execute_in_default_singleton(ctx, &parent_batch, row_idx)
                        .await?;
                }
            }

            if !self.result_buffer.is_empty() {
                return self.drain_buffer();
            }
        }
    }

    fn close(&mut self) {
        if let Some(delegate) = self.single_graph_delegate.as_mut() {
            delegate.close();
        } else {
            self.child.close();
        }
        self.result_buffer.clear();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.estimated
    }

    /// Without this the rendered physical plan truncates at the wrapper and the
    /// whole annotated BGP disappears — which is how a threshold that never
    /// reached the scan stayed invisible behind a plan that looked like one
    /// operator.
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        vec![crate::plan_node::PlanChild::child(self.child.as_ref())]
    }

    /// Name the chain and the lane its cost model prefers.
    ///
    /// The inner subplan is built at `open()` (it needs the snapshot to know
    /// whether a sealed arena exists), so `describe()` cannot report the lane
    /// that *ran* — that is EXPLAIN ANALYZE territory, per the `Operator`
    /// contract. Everything [`Self::chain_lane`] consumes is available here
    /// though (the child's schema and the planner stats; only the elision
    /// gates need the context), so the cost model's *preference* is reportable
    /// and is the single most useful fact about an annotated plan. It is
    /// labelled as a preference, and `lane-final` says where the real answer
    /// lives: the `annotation delegate lane` tracing event at DEBUG.
    fn plan_details(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut m = serde_json::Map::new();
        let Some(shape) =
            crate::annotation_edge_probe::recognize_annotation_edge(&self.inner_patterns)
        else {
            m.insert("kind".into(), "unrecognized-chain".into());
            m.insert("patterns".into(), self.inner_patterns.len().into());
            return m;
        };
        m.insert("kind".into(), "edge-annotation".into());
        m.insert(
            "base".into(),
            match &shape.base {
                Pattern::Triple(tp) => crate::explain::format_pattern(tp).into(),
                other => format!("{other:?}").into(),
            },
        );
        m.insert("body-patterns".into(), shape.body.len().into());
        m.insert(
            "body-filters".into(),
            shape
                .body
                .iter()
                .filter(|p| matches!(p, Pattern::Filter(_)))
                .count()
                .into(),
        );
        let child_bound: HashSet<VarId> = self.child.schema().iter().copied().collect();
        let elided =
            elide_redundant_chain(&self.inner_patterns, &self.needed_outside, &child_bound);
        m.insert("chain-elided".into(), elided.is_some().into());
        let lane = match self.chain_lane(&self.child, elided.is_some()) {
            ChainLane::Arena => "arena",
            ChainLane::Enumerate => "enumerate",
            ChainLane::Chain => "chain",
        };
        m.insert("lane-preference".into(), lane.into());
        m.insert(
            "lane-final".into(),
            "decided at open; see the `annotation delegate lane` DEBUG event".into(),
        );
        m
    }
}

#[cfg(test)]
mod tests {
    use super::{
        choose_chain_lane, elide_redundant_chain, hash_lane_admits, ChainLane, LaneInputs,
    };
    use crate::ir::{Pattern, Ref, Term, TriplePattern};
    use crate::var_registry::VarId;
    use fluree_db_core::Sid;
    use fluree_vocab::db::{REIFIES_OBJECT, REIFIES_PREDICATE, REIFIES_SUBJECT};
    use fluree_vocab::namespaces::FLUREE_DB;
    use std::collections::HashSet;

    const FULL_ARENA: Option<f64> = Some(21_400_294.0);
    const SLICE_ARENA: Option<f64> = Some(300_000.0);

    fn lane(
        edge_first: f64,
        probe_first: f64,
        arena_rows: Option<f64>,
        subject_bound: bool,
        reifier_bound: bool,
    ) -> ChainLane {
        choose_chain_lane(LaneInputs {
            edge_first,
            probe_first,
            arena_rows,
            subject_bound,
            reifier_bound,
            chain_elided: false,
        })
    }

    fn lane_elided(edge_first: f64, probe_first: f64, arena_rows: Option<f64>) -> ChainLane {
        choose_chain_lane(LaneInputs {
            edge_first,
            probe_first,
            arena_rows,
            subject_bound: false,
            reifier_bound: false,
            chain_elided: true,
        })
    }

    const S: VarId = VarId(1);
    const P: VarId = VarId(2);
    const O: VarId = VarId(3);
    const ANN: VarId = VarId(4);
    const X: VarId = VarId(5);

    fn triple(s: Ref, p: Ref, o: Term) -> Pattern {
        Pattern::Triple(TriplePattern { s, p, o, dtc: None })
    }

    fn reifies(name: &str) -> Ref {
        Ref::Sid(Sid::new(FLUREE_DB, name))
    }

    /// `<< s p o >> :q ?x` as `expand_edge_annotation_patterns` emits it.
    fn chain(s: Ref, p: Ref, o: Term) -> Vec<Pattern> {
        let p_as_term = match &p {
            Ref::Var(v) => Term::Var(*v),
            Ref::Sid(sid) => Term::Sid(sid.clone()),
            Ref::Iri(iri) => Term::Iri(iri.clone()),
        };
        vec![
            triple(s.clone(), p.clone(), o.clone()),
            triple(Ref::Var(ANN), reifies(REIFIES_SUBJECT), s.into()),
            triple(Ref::Var(ANN), reifies(REIFIES_PREDICATE), p_as_term),
            triple(Ref::Var(ANN), reifies(REIFIES_OBJECT), o),
            triple(Ref::Var(ANN), Ref::Sid(Sid::new(9, "q")), Term::Var(X)),
        ]
    }

    fn reifies_names(patterns: &[Pattern]) -> Vec<String> {
        patterns
            .iter()
            .filter_map(|p| match p {
                Pattern::Triple(tp) => match &tp.p {
                    Ref::Sid(sid) if sid.namespace_code == FLUREE_DB => Some(sid.name.to_string()),
                    _ => None,
                },
                _ => None,
            })
            .collect()
    }

    fn set(vars: &[VarId]) -> HashSet<VarId> {
        vars.iter().copied().collect()
    }

    #[test]
    fn count_shape_keeps_only_the_constraining_lookup() {
        // P11: `<< ?s :P ?o >> :q ?x` with COUNT(*) — nothing reads ?s/?o.
        let typed = Sid::new(9, "P");
        let elided = elide_redundant_chain(
            &chain(Ref::Var(S), Ref::Sid(typed.clone()), Term::Var(O)),
            &set(&[]),
            &set(&[]),
        )
        .expect("recognized chain");
        assert_eq!(reifies_names(&elided), vec![REIFIES_PREDICATE]);
        assert_eq!(elided.len(), 2, "predicate lookup + body: {elided:?}");
        assert!(
            !matches!(&elided[0], Pattern::Triple(tp) if tp.p == Ref::Sid(typed)),
            "the base edge must be gone: {elided:?}"
        );
    }

    #[test]
    fn read_positions_keep_their_lookup_and_a_read_predicate_keeps_everything() {
        let typed = Ref::Sid(Sid::new(9, "P"));
        // ?o projected, ?s read by a later sibling.
        let elided = elide_redundant_chain(
            &chain(Ref::Var(S), typed.clone(), Term::Var(O)),
            &set(&[O]),
            &set(&[S]),
        )
        .expect("recognized chain");
        assert_eq!(
            reifies_names(&elided),
            vec![REIFIES_SUBJECT, REIFIES_PREDICATE, REIFIES_OBJECT]
        );
        assert_eq!(elided.len(), 4, "three lookups + body, no base edge");
        // A read variable predicate needs the base scan's predicate binding.
        assert!(elide_redundant_chain(
            &chain(Ref::Var(S), Ref::Var(P), Term::Var(O)),
            &set(&[P]),
            &set(&[]),
        )
        .is_none());
        // The body's own reads count too.
        let mut with_body_read = chain(Ref::Var(S), typed, Term::Var(O));
        with_body_read.push(triple(
            Ref::Var(O),
            Ref::Sid(Sid::new(9, "r")),
            Term::Var(VarId(6)),
        ));
        let elided =
            elide_redundant_chain(&with_body_read, &set(&[]), &set(&[])).expect("recognized chain");
        assert_eq!(
            reifies_names(&elided),
            vec![REIFIES_PREDICATE, REIFIES_OBJECT]
        );
    }

    #[test]
    fn hash_lane_leaves_a_bound_reifier_to_the_chain() {
        // 2,857 body rows binding the reifier: the lane would walk every
        // swept edge per row, the chain probes once per row.
        assert!(!hash_lane_admits(Some(2_857), true, true));
        assert!(!hash_lane_admits(None, true, true));
        // The same stream binding the edge subject instead is the lane's case.
        assert!(hash_lane_admits(Some(2_857), true, false));
        assert!(hash_lane_admits(None, true, false));
        // Small streams and unbounded sweeps stay on the chain.
        assert!(!hash_lane_admits(Some(10), true, false));
        assert!(!hash_lane_admits(Some(2_857), false, false));
    }

    #[test]
    fn a_repeated_variable_keeps_the_lookups_that_equate_it() {
        let typed = Ref::Sid(Sid::new(9, "P"));
        // `<< ?s :P ?s >>` with nothing reading ?s: once the base edge is
        // gone, the subject and object lookups joining on ?s are the only
        // thing that still requires the two positions to be equal.
        let elided = elide_redundant_chain(
            &chain(Ref::Var(S), typed.clone(), Term::Var(S)),
            &set(&[]),
            &set(&[]),
        )
        .expect("recognized chain");
        assert_eq!(
            reifies_names(&elided),
            vec![REIFIES_SUBJECT, REIFIES_PREDICATE, REIFIES_OBJECT]
        );
        // A reifier that is also the edge's object keeps
        // `?ann f:reifiesObject ?ann`.
        let elided = elide_redundant_chain(
            &chain(Ref::Var(S), typed, Term::Var(ANN)),
            &set(&[]),
            &set(&[]),
        )
        .expect("recognized chain");
        assert_eq!(
            reifies_names(&elided),
            vec![REIFIES_PREDICATE, REIFIES_OBJECT]
        );
        // A variable predicate repeated in another position counts as read,
        // which blocks the rewrite.
        assert!(elide_redundant_chain(
            &chain(Ref::Var(S), Ref::Var(S), Term::Var(O)),
            &set(&[]),
            &set(&[]),
        )
        .is_none());
    }

    #[test]
    fn wildcard_count_keeps_one_lookup_so_the_body_alone_cannot_qualify() {
        // P2: `<< ?s ?p ?o >> :q ?x` COUNT(*) — every position unread, but a
        // plain subject with `:q` must still not count as a reifier.
        let elided = elide_redundant_chain(
            &chain(Ref::Var(S), Ref::Var(P), Term::Var(O)),
            &set(&[]),
            &set(&[]),
        )
        .expect("recognized chain");
        assert_eq!(reifies_names(&elided), vec![REIFIES_SUBJECT]);
        assert_eq!(elided.len(), 2);
    }

    #[test]
    fn elided_chain_costs_sequential_rows() {
        // P11 with its lookups elided: 31k-est reifiers at ~2 rows each
        // beats both the whole-arena decode and the 21.4M enumeration.
        assert_eq!(
            lane_elided(952_406.0, 31_751.0, FULL_ARENA),
            ChainLane::Chain
        );
        // P2 elided: one `f:reifiesSubject` range over 21.4M reifiers plus
        // the body probe ran 10.6 s against 65 s for the enumeration.
        assert_eq!(
            lane_elided(1e12, 21_400_294.0, FULL_ARENA),
            ChainLane::Chain
        );
        // P6 / P7 read the endpoints and the predicate, so nothing is
        // elided and the enumeration still beats a point scan per reifier.
        assert_eq!(
            lane(1e12, 21_400_294.0, FULL_ARENA, false, false),
            ChainLane::Enumerate
        );
    }

    #[test]
    fn lane_choice_matches_measured_starbench_shapes() {
        // P2 / P7 / C2: wildcard base edge — walk the arena, slice and full.
        assert_eq!(
            lane(1e12, 300_000.0, SLICE_ARENA, false, false),
            ChainLane::Enumerate
        );
        assert_eq!(
            lane(1e12, 21_400_294.0, FULL_ARENA, false, false),
            ChainLane::Enumerate
        );
        // P11 full scale: 952k TREATS edges spread over ~5,200 leaves, so the
        // probe decodes the whole arena (14.9 s); the chain drives from
        // `f:reifiesPredicate TREATS` (31k est.) and ran 5.3 s.
        assert_eq!(
            lane(952_406.0, 31_751.0, FULL_ARENA, false, false),
            ChainLane::Chain
        );
        // Slice P11 (102k edges over 73 leaves): 0.81 s chain vs 1.29 s arena.
        assert_eq!(
            lane(102_555.0, 3_846.0, SLICE_ARENA, false, false),
            ChainLane::Chain
        );
        // S20 / S21 object-bound `?s PART_OF <o>`: every edge is its own leaf
        // (1,208 edges → 3.1 s on the arena, 0.19 s on the chain). The
        // estimate says 11 edges and ~100 reifiers with that object.
        assert_eq!(
            lane(11.0, 100.0, FULL_ARENA, false, false),
            ChainLane::Chain
        );
        // P9 / P18 subject-bound hub (3,345 edges in a couple of leaves):
        // 50 ms arena vs 200 ms chain. P23 fully bound: 46 ms vs 192 s.
        assert_eq!(
            lane(3_345.0, 214.0, FULL_ARENA, true, false),
            ChainLane::Arena
        );
        assert_eq!(lane(1.0, 100.0, FULL_ARENA, true, false), ChainLane::Arena);
        // P19: subject bound by a 51k-row child — the chain ran out of memory.
        assert_eq!(lane(12.0, 214.0, FULL_ARENA, true, false), ChainLane::Arena);
        // C7 / P22 / C10 / P1: the child already binds the reifier, so the
        // chain is one probe per driving row — never sweep for it.
        assert_eq!(
            lane(952_406.0, 1.0, FULL_ARENA, false, true),
            ChainLane::Chain
        );
        assert_eq!(lane(1e12, 1.0, FULL_ARENA, true, true), ChainLane::Chain);
        // The same bound reifier behind a bound subject whose edges fit the
        // buffer: without the reifier rule the subject rule takes the arena.
        assert_eq!(lane(3_345.0, 1.0, FULL_ARENA, true, true), ChainLane::Chain);
        // P5 / P13: bound-object wildcard (1000 est.) with ~9 reifiers.
        assert_eq!(
            lane(1000.0, 9.0, SLICE_ARENA, false, false),
            ChainLane::Chain
        );
        // No arena statistics at all: never enumerate.
        assert_ne!(
            lane(1e12, 300_000.0, None, false, false),
            ChainLane::Enumerate
        );
        // A base edge wider than the buffer ceiling never takes the probe.
        assert_ne!(
            lane(30_000_000.0, 30_000_000.0, FULL_ARENA, false, false),
            ChainLane::Arena
        );
        assert_ne!(
            lane(30_000_000.0, 30_000_000.0, FULL_ARENA, true, false),
            ChainLane::Arena
        );
    }

    #[test]
    fn small_arenas_keep_the_edge_first_probe() {
        // Cypher-scale ledger (190k reified edges ≈ 46 leaves): a typed
        // relationship over 3k edges can touch at most the whole arena, so
        // the leaf tax stays bounded and the probe beats 9.5k per-reifier
        // point checks.
        assert_eq!(
            lane(3_000.0, 9_500.0, Some(190_000.0), false, false),
            ChainLane::Arena
        );
    }
}
