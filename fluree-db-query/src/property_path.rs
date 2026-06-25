//! Property path operator - transitive graph traversal
//!
//! Implements property path semantics for transitive predicates:
//! - `+` (one-or-more): Traverse at least one hop
//! - `*` (zero-or-more): Traverse zero or more hops (includes starting node)
//!
//! # Key semantics
//!
//! - BFS traversal with visited set for cycle detection
//! - Ref-only traversal: Only follows edges where object is a Sid (IRI reference)
//! - Short-circuits on cycles to prevent infinite loops
//! - Safety bound on max visited nodes to prevent runaway closure enumeration
//!
//! # Correlated execution modes
//!
//! When both subject and object are variables, the operator requires correlated
//! execution where at least one variable is bound by the upstream child operator:
//!
//! | Subject | Object | Behavior |
//! |---------|--------|----------|
//! | Bound   | Unbound | Forward traversal from subject, bind reachable to object |
//! | Unbound | Bound   | Backward traversal to object, bind sources to subject |
//! | Bound   | Bound   | Reachability filter: keep row only if path exists |
//! | Unbound | Unbound | **Error**: requires at least one bound variable |
//!
//! The "both unbound" case is intentionally an error to prevent accidental
//! full-closure enumeration which can be extremely expensive. If full closure
//! is needed, the query should explicitly bind one side first.

use crate::binary_scan::BinaryScanOperator;
use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::triple::Ref;
use crate::ir::{PathModifier, PropertyPathPattern};
use crate::operator::{
    compute_trimmed_vars, effective_schema, trim_batch, BoxedOperator, Operator, OperatorState,
};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::{
    range_with_overlay, Flake, FlakeValue, IndexType, RangeMatch, RangeOptions, RangeTest, Sid,
};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

/// Default maximum number of nodes to visit during traversal
/// This prevents runaway closure enumeration for both-variable patterns
pub const DEFAULT_MAX_VISITED: usize = 10_000;

/// Predicates a wildcard (untyped) path must never traverse: `rdf:type` (its
/// object is a class, not a node) and the `f:reifies*` reifier bundle (the
/// edge-annotation sidecar, hidden from variable-predicate reads). Data
/// properties are already excluded by the `Ref`-object filter in the scan.
#[inline]
fn is_reserved_edge_predicate(p: &Sid) -> bool {
    fluree_db_core::is_rdf_type(p) || fluree_db_core::is_reserved_reifies_predicate(p)
}

/// Property path operator - transitive graph traversal
///
/// Supports two execution modes:
/// 1. **Unseeded mode** (no child): For constant-bound subject/object or both-variable closure
/// 2. **Correlated mode** (with child): For each input row, read bound var and traverse
///
/// The execution mode is determined at runtime based on the pattern and child bindings.
pub struct PropertyPathOperator {
    /// Optional child operator providing input solutions
    child: Option<BoxedOperator>,
    /// Property path pattern to execute
    pattern: PropertyPathPattern,
    /// Output schema (variables from subject and object)
    in_schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
    /// Safety bound for maximum visited nodes
    max_visited: usize,
    /// Results buffer for unseeded mode
    results_buffer: Option<Vec<(Sid, Sid)>>,
    /// Results buffer index
    results_idx: usize,
    /// Current child batch being processed (correlated mode)
    current_child_batch: Option<Batch>,
    /// Current row index in child batch
    current_child_row: usize,
    /// Variables required by downstream operators; if set, output is trimmed.
    out_schema: Option<Arc<[VarId]>>,
}

impl PropertyPathOperator {
    /// Create a new property path operator
    ///
    /// # Arguments
    ///
    /// * `child` - Optional input solutions operator (for correlated execution)
    /// * `pattern` - Property path pattern to execute
    /// * `max_visited` - Maximum nodes to visit (safety bound)
    pub fn new(
        child: Option<BoxedOperator>,
        pattern: PropertyPathPattern,
        max_visited: usize,
    ) -> Self {
        // Build schema from pattern variables
        let mut schema_vec = Vec::with_capacity(2);
        if let Ref::Var(v) = &pattern.subject {
            schema_vec.push(*v);
        }
        if let Ref::Var(v) = &pattern.object {
            schema_vec.push(*v);
        }

        // If we have a child, extend schema with child's variables
        let schema: Arc<[VarId]> = if let Some(ref child) = child {
            let mut full_schema: Vec<VarId> = child.schema().to_vec();
            let seen: HashSet<VarId> = full_schema.iter().copied().collect();
            for v in schema_vec {
                if !seen.contains(&v) {
                    full_schema.push(v);
                }
            }
            Arc::from(full_schema.into_boxed_slice())
        } else {
            Arc::from(schema_vec.into_boxed_slice())
        };

        Self {
            child,
            pattern,
            in_schema: schema,
            state: OperatorState::Created,
            max_visited,
            results_buffer: None,
            results_idx: 0,
            current_child_batch: None,
            current_child_row: 0,
            out_schema: None,
        }
    }

    /// Create with default max_visited
    pub fn with_defaults(child: Option<BoxedOperator>, pattern: PropertyPathPattern) -> Self {
        Self::new(child, pattern, DEFAULT_MAX_VISITED)
    }

    /// Trim output to only the specified downstream variables.
    pub fn with_out_schema(mut self, downstream_vars: Option<&[VarId]>) -> Self {
        self.out_schema = compute_trimmed_vars(&self.in_schema, downstream_vars);
        self
    }

    /// Whether a node reached at `depth` hops should be emitted, given the
    /// pattern's hop bounds. For an unbounded fixed path (`min_hops`/`max_hops`
    /// both `None`) this is `depth >= modifier-lower-bound`, reproducing the
    /// original `*`/`+` behavior exactly.
    fn emit_at_depth(&self, depth: u32) -> bool {
        depth >= self.pattern.effective_min_hops()
            && self.pattern.max_hops.is_none_or(|hi| depth <= hi)
    }

    /// Whether to keep expanding from a node reached at `depth` — only when a
    /// further hop could still land within `max_hops`. Always true when
    /// unbounded, so fixed `*`/`+` paths expand exactly as before.
    fn can_expand(&self, depth: u32) -> bool {
        self.pattern.max_hops.is_none_or(|hi| depth < hi)
    }

    /// One forward hop from `node`: the ref objects of `(node, p, ?)`. For a
    /// fixed path this is the union over the traversed predicate(s) (alternation
    /// `(a|b)*`); for a wildcard (untyped) path it follows **any** node→node
    /// edge — a subject-prefix scan keeping only `Ref` objects and skipping the
    /// reserved predicates (`rdf:type`, `f:reifies*`).
    async fn forward_step(&self, ctx: &ExecutionContext<'_>, node: &Sid) -> Result<Vec<Sid>> {
        let (db, overlay, to_t) = ctx.require_single_graph()?;
        let mut out = Vec::new();
        if self.pattern.wildcard {
            let range_match = RangeMatch::new().with_subject(node.clone());
            let flakes = range_with_overlay(
                db,
                ctx.binary_g_id,
                overlay,
                IndexType::Spot,
                RangeTest::Eq,
                range_match,
                RangeOptions::new().with_to_t(to_t),
            )
            .await?;
            let flakes = self.filter_edges(ctx, flakes).await?;
            for flake in flakes {
                if is_reserved_edge_predicate(&flake.p) {
                    continue;
                }
                if let FlakeValue::Ref(o) = flake.o {
                    out.push(o);
                }
            }
            return Ok(out);
        }
        for pred in &self.pattern.predicates {
            let range_match = RangeMatch::new()
                .with_subject(node.clone())
                .with_predicate(pred.clone());
            let flakes = range_with_overlay(
                db,
                ctx.binary_g_id,
                overlay,
                IndexType::Spot,
                RangeTest::Eq,
                range_match,
                RangeOptions::new().with_to_t(to_t),
            )
            .await?;
            let flakes = self.filter_edges(ctx, flakes).await?;
            for flake in flakes {
                if let FlakeValue::Ref(o) = flake.o {
                    out.push(o);
                }
            }
        }
        Ok(out)
    }

    /// One backward hop into `node`: the subjects of `(?, p, node)`. Fixed paths
    /// union over the traversed predicate(s); a wildcard path follows any
    /// node→node edge backward — an object-prefix scan (OPST) skipping the
    /// reserved predicates.
    async fn backward_step(&self, ctx: &ExecutionContext<'_>, node: &Sid) -> Result<Vec<Sid>> {
        let (db, overlay, to_t) = ctx.require_single_graph()?;
        let mut out = Vec::new();
        if self.pattern.wildcard {
            let range_match = RangeMatch::new().with_object(FlakeValue::Ref(node.clone()));
            let flakes = range_with_overlay(
                db,
                ctx.binary_g_id,
                overlay,
                IndexType::Opst,
                RangeTest::Eq,
                range_match,
                RangeOptions::new().with_to_t(to_t),
            )
            .await?;
            let flakes = self.filter_edges(ctx, flakes).await?;
            for flake in flakes {
                if is_reserved_edge_predicate(&flake.p) {
                    continue;
                }
                out.push(flake.s);
            }
            return Ok(out);
        }
        for pred in &self.pattern.predicates {
            let range_match = RangeMatch::new()
                .with_predicate(pred.clone())
                .with_object(FlakeValue::Ref(node.clone()));
            let flakes = range_with_overlay(
                db,
                ctx.binary_g_id,
                overlay,
                IndexType::Post,
                RangeTest::Eq,
                range_match,
                RangeOptions::new().with_to_t(to_t),
            )
            .await?;
            let flakes = self.filter_edges(ctx, flakes).await?;
            for flake in flakes {
                out.push(flake.s);
            }
        }
        Ok(out)
    }

    /// Apply view-policy filtering to a batch of edge flakes read during path
    /// traversal.
    ///
    /// Property paths read edges directly via `range_with_overlay`, which (like
    /// every raw-leaflet reader) bypasses the per-flake `filter_flakes` policy
    /// filtering that scan operators apply. Without this, a non-root view policy
    /// would let traversal visit and emit subjects/edges the policy hides. Hidden
    /// edges are removed here so the path neither traverses them nor reaches the
    /// nodes behind them, matching the per-flake semantics of the scan path.
    /// No-op for root / no policy (the filter short-circuits).
    async fn filter_edges(
        &self,
        ctx: &ExecutionContext<'_>,
        flakes: Vec<Flake>,
    ) -> Result<Vec<Flake>> {
        let (db, overlay, to_t) = ctx.require_single_graph()?;
        BinaryScanOperator::filter_flakes_by_policy(ctx, db, overlay, to_t, ctx.binary_g_id, flakes)
            .await
    }

    /// Layered BFS for a **bounded** path (`max_hops` set). Tracks visited per
    /// `(node, depth)` rather than per node, so a node first reached below
    /// `min_hops` can still be reached on a longer in-range path — the correct
    /// `*2..3` semantics (a plain node-visited set would suppress it, and then
    /// disagree with the bound-bound `path_exists` form). Output is de-duped.
    /// Termination is guaranteed by the finite depth cap. Only untyped paths
    /// reach the operator with bounds (typed bounded ranges lower to a UNION of
    /// fixed-length chains), so this never runs for SPARQL/typed paths.
    async fn traverse_bounded(
        &self,
        ctx: &ExecutionContext<'_>,
        anchor: &Sid,
        forward: bool,
        stop_at: Option<&Sid>,
    ) -> Result<Vec<Sid>> {
        let max = self
            .pattern
            .max_hops
            .expect("bounded traversal needs max_hops");
        let mut frontier: Vec<Sid> = vec![anchor.clone()];
        let mut seen_at_depth: HashSet<(Sid, u32)> = HashSet::new();
        seen_at_depth.insert((anchor.clone(), 0));
        let mut emitted: Vec<Sid> = Vec::new();
        let mut emitted_set: HashSet<Sid> = HashSet::new();
        if self.emit_at_depth(0) {
            emitted.push(anchor.clone());
            emitted_set.insert(anchor.clone());
            // Existence probe: stop as soon as the target is reached in range,
            // instead of enumerating the whole bounded frontier.
            if stop_at == Some(anchor) {
                return Ok(emitted);
            }
        }

        let mut depth = 0u32;
        while depth < max && !frontier.is_empty() {
            crate::fast_path_common::bail_if_cancelled(&ctx.cancellation)?;
            if seen_at_depth.len() >= self.max_visited {
                return Err(QueryError::ResourceLimit(format!(
                    "Property path exceeded max visited nodes ({})",
                    self.max_visited
                )));
            }
            let next_depth = depth + 1;
            let mut next_frontier: Vec<Sid> = Vec::new();
            for node in &frontier {
                let neighbors = if forward {
                    self.forward_step(ctx, node).await?
                } else {
                    self.backward_step(ctx, node).await?
                };
                for nb in neighbors {
                    if self.emit_at_depth(next_depth) && emitted_set.insert(nb.clone()) {
                        emitted.push(nb.clone());
                        if stop_at == Some(&nb) {
                            return Ok(emitted);
                        }
                    }
                    if seen_at_depth.insert((nb.clone(), next_depth)) {
                        next_frontier.push(nb);
                    }
                }
            }
            frontier = next_frontier;
            depth = next_depth;
        }
        Ok(emitted)
    }

    /// Traverse forward from a starting node (subject bound)
    ///
    /// Uses SPOT index: (subject=start, predicate=path_pred)
    /// Returns all reachable nodes via the transitive predicate.
    async fn traverse_forward(&self, ctx: &ExecutionContext<'_>, start: &Sid) -> Result<Vec<Sid>> {
        if self.pattern.max_hops.is_some() {
            return self.traverse_bounded(ctx, start, true, None).await;
        }
        let mut visited: HashSet<Sid> = HashSet::new();
        let mut queue: VecDeque<(Sid, u32)> = VecDeque::new();
        let mut results: Vec<Sid> = Vec::new();
        let mut added_start_via_cycle = false;

        // Depth-0 start: emit only when in bounds (`*` / `*0..` include it).
        if self.pattern.modifier == PathModifier::ZeroOrMore && self.emit_at_depth(0) {
            results.push(start.clone());
        }
        visited.insert(start.clone());
        if self.can_expand(0) {
            queue.push_back((start.clone(), 0));
        }

        while let Some((current, depth)) = queue.pop_front() {
            crate::fast_path_common::bail_if_cancelled(&ctx.cancellation)?;
            // Safety bound check
            if visited.len() >= self.max_visited {
                return Err(QueryError::ResourceLimit(format!(
                    "Property path exceeded max visited nodes ({})",
                    self.max_visited
                )));
            }
            let next_depth = depth + 1;

            // Union of one forward hop over every traversed predicate. In
            // dataset mode this runs against a single active graph (there is no
            // meaningful dataset-wide `to_t` for multi-ledger datasets).
            for obj_sid in self.forward_step(ctx, &current).await? {
                // OneOrMore should still be able to emit `start` if a non-zero-length path
                // returns to it (cycle), even though `visited` contains `start`.
                if self.pattern.modifier == PathModifier::OneOrMore
                    && !added_start_via_cycle
                    && &obj_sid == start
                {
                    if self.emit_at_depth(next_depth) {
                        results.push(obj_sid.clone());
                    }
                    added_start_via_cycle = true;
                    continue;
                }
                if visited.insert(obj_sid.clone()) {
                    if self.emit_at_depth(next_depth) {
                        results.push(obj_sid.clone());
                    }
                    if self.can_expand(next_depth) {
                        queue.push_back((obj_sid, next_depth));
                    }
                }
            }
        }

        Ok(results)
    }

    /// Traverse backward to a target node (object bound)
    ///
    /// Uses POST index: (predicate=path_pred, object=target)
    /// Returns all nodes that can reach the target via the transitive predicate.
    async fn traverse_backward(
        &self,
        ctx: &ExecutionContext<'_>,
        target: &Sid,
    ) -> Result<Vec<Sid>> {
        if self.pattern.max_hops.is_some() {
            return self.traverse_bounded(ctx, target, false, None).await;
        }
        let mut visited: HashSet<Sid> = HashSet::new();
        let mut queue: VecDeque<(Sid, u32)> = VecDeque::new();
        let mut results: Vec<Sid> = Vec::new();
        let mut added_target_via_cycle = false;

        // Depth-0 target as its own source: emit only when in bounds.
        if self.pattern.modifier == PathModifier::ZeroOrMore && self.emit_at_depth(0) {
            results.push(target.clone());
        }
        visited.insert(target.clone());
        if self.can_expand(0) {
            queue.push_back((target.clone(), 0));
        }

        while let Some((current, depth)) = queue.pop_front() {
            crate::fast_path_common::bail_if_cancelled(&ctx.cancellation)?;
            // Safety bound check
            if visited.len() >= self.max_visited {
                return Err(QueryError::ResourceLimit(format!(
                    "Property path exceeded max visited nodes ({})",
                    self.max_visited
                )));
            }
            let next_depth = depth + 1;

            // Union of one backward hop over every traversed predicate.
            for src_sid in self.backward_step(ctx, &current).await? {
                if self.pattern.modifier == PathModifier::OneOrMore
                    && !added_target_via_cycle
                    && &src_sid == target
                {
                    if self.emit_at_depth(next_depth) {
                        results.push(src_sid.clone());
                    }
                    added_target_via_cycle = true;
                    continue;
                }
                if visited.insert(src_sid.clone()) {
                    if self.emit_at_depth(next_depth) {
                        results.push(src_sid.clone());
                    }
                    if self.can_expand(next_depth) {
                        queue.push_back((src_sid, next_depth));
                    }
                }
            }
        }

        Ok(results)
    }

    /// Compute full transitive closure for a predicate (both vars unbound).
    ///
    /// Returns pairs (start, reachable) consistent with modifier semantics.
    async fn compute_closure(&self, ctx: &ExecutionContext<'_>) -> Result<Vec<(Sid, Sid)>> {
        // Pull all edges for every traversed predicate using PSOT
        // (predicate-indexed) and merge them into one adjacency map — for an
        // alternation path `(a|b)*` the closure spans both predicates' edges.
        let (db, overlay, to_t) = ctx.require_single_graph()?;
        let mut adj: std::collections::HashMap<Sid, Vec<Sid>> = std::collections::HashMap::new();
        let mut nodes: HashSet<Sid> = HashSet::new();
        let mut ingest = |flake: fluree_db_core::Flake| {
            if let FlakeValue::Ref(o) = flake.o {
                nodes.insert(flake.s.clone());
                nodes.insert(o.clone());
                adj.entry(flake.s).or_default().push(o);
            }
        };
        if self.pattern.wildcard {
            // Wildcard closure: every node→node edge except the reserved ones.
            // A full PSOT scan (no predicate bound) over the active graph.
            let flakes = range_with_overlay(
                db,
                ctx.binary_g_id,
                overlay,
                IndexType::Psot,
                RangeTest::Eq,
                RangeMatch::new(),
                RangeOptions::new().with_to_t(to_t),
            )
            .await?;
            let flakes = self.filter_edges(ctx, flakes).await?;
            for flake in flakes {
                if !is_reserved_edge_predicate(&flake.p) {
                    ingest(flake);
                }
            }
        } else {
            for pred in &self.pattern.predicates {
                let range_match = RangeMatch::predicate(pred.clone());
                let flakes = range_with_overlay(
                    db,
                    ctx.binary_g_id,
                    overlay,
                    IndexType::Psot,
                    RangeTest::Eq,
                    range_match,
                    RangeOptions::new().with_to_t(to_t),
                )
                .await?;
                let flakes = self.filter_edges(ctx, flakes).await?;
                for flake in flakes {
                    ingest(flake);
                }
            }
        }

        let mut out: Vec<(Sid, Sid)> = Vec::new();
        for start in &nodes {
            // Bounded path: layered (node, depth) BFS over the adjacency map, so
            // a node first reached below `min_hops` can still be reached on a
            // longer in-range path (matches `traverse_bounded`).
            if let Some(max) = self.pattern.max_hops {
                let mut frontier: Vec<Sid> = vec![start.clone()];
                let mut seen_at_depth: HashSet<(Sid, u32)> = HashSet::new();
                seen_at_depth.insert((start.clone(), 0));
                let mut emitted: HashSet<Sid> = HashSet::new();
                if self.emit_at_depth(0) {
                    out.push((start.clone(), start.clone()));
                    emitted.insert(start.clone());
                }
                let mut depth = 0u32;
                while depth < max && !frontier.is_empty() {
                    if seen_at_depth.len() >= self.max_visited {
                        return Err(QueryError::ResourceLimit(format!(
                            "Property path exceeded max visited nodes ({})",
                            self.max_visited
                        )));
                    }
                    let next_depth = depth + 1;
                    let mut next_frontier: Vec<Sid> = Vec::new();
                    for node in &frontier {
                        if let Some(nexts) = adj.get(node) {
                            for n in nexts {
                                if self.emit_at_depth(next_depth) && emitted.insert(n.clone()) {
                                    out.push((start.clone(), n.clone()));
                                }
                                if seen_at_depth.insert((n.clone(), next_depth)) {
                                    next_frontier.push(n.clone());
                                }
                            }
                        }
                    }
                    frontier = next_frontier;
                    depth = next_depth;
                }
                continue;
            }

            // BFS from start using adjacency only (no DB calls).
            let mut visited: HashSet<Sid> = HashSet::new();
            let mut queue: VecDeque<(Sid, u32)> = VecDeque::new();

            if self.pattern.modifier == PathModifier::ZeroOrMore && self.emit_at_depth(0) {
                out.push((start.clone(), start.clone()));
            }
            visited.insert(start.clone());
            if self.can_expand(0) {
                queue.push_back((start.clone(), 0));
            }

            let mut added_self_via_cycle = false;
            while let Some((cur, depth)) = queue.pop_front() {
                crate::fast_path_common::bail_if_cancelled(&ctx.cancellation)?;
                if visited.len() >= self.max_visited {
                    return Err(QueryError::ResourceLimit(format!(
                        "Property path exceeded max visited nodes ({})",
                        self.max_visited
                    )));
                }
                let next_depth = depth + 1;
                if let Some(nexts) = adj.get(&cur) {
                    for n in nexts {
                        if self.pattern.modifier == PathModifier::OneOrMore
                            && !added_self_via_cycle
                            && n == start
                        {
                            if self.emit_at_depth(next_depth) {
                                out.push((start.clone(), start.clone()));
                            }
                            added_self_via_cycle = true;
                            continue;
                        }
                        if visited.insert(n.clone()) {
                            if self.emit_at_depth(next_depth) {
                                out.push((start.clone(), n.clone()));
                            }
                            if self.can_expand(next_depth) {
                                queue.push_back((n.clone(), next_depth));
                            }
                        }
                    }
                }
            }
        }

        Ok(out)
    }

    /// Check if a path exists between two nodes
    ///
    /// Used for reachability filter when both subject and object are bound.
    async fn path_exists(
        &self,
        ctx: &ExecutionContext<'_>,
        start: &Sid,
        target: &Sid,
    ) -> Result<bool> {
        // Bounded path: reuse the exact layered (node, depth) traversal the
        // bound-unbound form uses, so the two never disagree — a node-only
        // visited set here would suppress an intermediate that must be revisited
        // at a later depth (e.g. A→B, A→C→B, B→D; `A-[*3..3]->D` via A-C-B-D).
        if self.pattern.max_hops.is_some() {
            return Ok(self
                .traverse_bounded(ctx, start, true, Some(target))
                .await?
                .iter()
                .any(|reached| reached == target));
        }

        // Zero-length path — only when the bounds admit depth 0.
        if self.pattern.modifier == PathModifier::ZeroOrMore
            && start == target
            && self.emit_at_depth(0)
        {
            return Ok(true);
        }

        let mut visited: HashSet<Sid> = HashSet::new();
        let mut queue: VecDeque<(Sid, u32)> = VecDeque::new();

        visited.insert(start.clone());
        if self.can_expand(0) {
            queue.push_back((start.clone(), 0));
        }

        while let Some((current, depth)) = queue.pop_front() {
            crate::fast_path_common::bail_if_cancelled(&ctx.cancellation)?;
            if visited.len() >= self.max_visited {
                return Err(QueryError::ResourceLimit(format!(
                    "Property path exceeded max visited nodes ({})",
                    self.max_visited
                )));
            }
            let next_depth = depth + 1;

            for obj_sid in self.forward_step(ctx, &current).await? {
                // Reaching the target at an in-bounds depth proves a path. The
                // check runs on every encounter (before the visited gate), so a
                // target first seen below `min_hops` can still match at a deeper,
                // in-range depth.
                if &obj_sid == target && self.emit_at_depth(next_depth) {
                    return Ok(true);
                }

                if visited.insert(obj_sid.clone()) && self.can_expand(next_depth) {
                    queue.push_back((obj_sid, next_depth));
                }
            }
        }

        Ok(false)
    }

    /// Execute unseeded mode (no child operator)
    ///
    /// This is called once during open() to compute all results.
    async fn execute_unseeded(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        let results = match (&self.pattern.subject, &self.pattern.object) {
            (Ref::Sid(subj), Ref::Var(_)) => {
                // Subject constant, object variable -> forward traversal
                let reachable = self.traverse_forward(ctx, subj).await?;
                reachable
                    .into_iter()
                    .map(|obj| (subj.clone(), obj))
                    .collect()
            }
            (Ref::Var(_), Ref::Sid(obj)) => {
                // Subject variable, object constant -> backward traversal
                let sources = self.traverse_backward(ctx, obj).await?;
                sources
                    .into_iter()
                    .map(|subj| (subj, obj.clone()))
                    .collect()
            }
            (Ref::Var(_), Ref::Var(_)) => self.compute_closure(ctx).await?,
            (Ref::Sid(_), Ref::Sid(_)) => {
                // Both constants: reachability check (0/1 rows). We use a dummy pair to indicate 1 row.
                let subj = match &self.pattern.subject {
                    Ref::Sid(s) => s,
                    _ => unreachable!(),
                };
                let obj = match &self.pattern.object {
                    Ref::Sid(s) => s,
                    _ => unreachable!(),
                };
                if self.path_exists(ctx, subj, obj).await? {
                    vec![(subj.clone(), obj.clone())]
                } else {
                    vec![]
                }
            }
            _ => {
                return Err(QueryError::InvalidQuery(
                    "Property path subject/object must be Var or Sid".to_string(),
                ));
            }
        };

        self.results_buffer = Some(results);
        self.results_idx = 0;
        Ok(())
    }

    /// Process a single child row in correlated mode
    async fn process_correlated_row(
        &self,
        ctx: &ExecutionContext<'_>,
        child_batch: &Batch,
        row_idx: usize,
    ) -> Result<Vec<Vec<Binding>>> {
        // Determine what's bound from child
        let subj_var = match &self.pattern.subject {
            Ref::Var(v) => Some(*v),
            _ => None,
        };
        let obj_var = match &self.pattern.object {
            Ref::Var(v) => Some(*v),
            _ => None,
        };

        // Get bound values from child
        let subj_binding = subj_var.and_then(|v| child_batch.column(v).map(|col| &col[row_idx]));
        let obj_binding = obj_var.and_then(|v| child_batch.column(v).map(|col| &col[row_idx]));

        // Resolve the active graph (property paths require a single active graph).
        let (db_for_encode, _overlay, _to_t) = ctx.require_single_graph()?;

        // Extract SIDs from constants or child bindings (if bound).
        //
        // Note: lowering may produce `Ref::Iri` constants to support cross-ledger joins.
        // For property paths we must traverse SIDs, so we opportunistically encode IRIs
        // against the selected active graph's namespace table.
        let binary_store = ctx.binary_store.as_ref();
        let resolve_sid = |term: &Ref, binding: Option<&Binding>| -> Option<Sid> {
            match term {
                Ref::Sid(s) => Some(s.clone()),
                Ref::Iri(iri) => db_for_encode.encode_iri(iri),
                Ref::Var(_) => binding.and_then(|b| match b {
                    Binding::Sid { sid: s, .. } => Some(s.clone()),
                    Binding::IriMatch { iri, .. } => db_for_encode.encode_iri(iri),
                    Binding::Iri(iri) => db_for_encode.encode_iri(iri),
                    // Indexed BinaryScan emits late-materialized EncodedSid for a
                    // correlated path endpoint (e.g. the ?mid of `?s p1 ?mid . ?mid p2+ ?o`
                    // with a bound subject). Resolve its raw s_id (only meaningful within
                    // this single ledger, which property paths already require) to its IRI
                    // via the active graph's store, then re-encode against the same graph —
                    // matching the `IriMatch`/`Iri` arms above. Without this arm the binding
                    // resolved to None and fell into the full-closure branch, pairing the
                    // row with the entire p2 closure.
                    Binding::EncodedSid { s_id, .. } => binary_store
                        .and_then(|st| st.resolve_subject_iri(*s_id).ok())
                        .and_then(|iri| db_for_encode.encode_iri(&iri)),
                    _ => None,
                }),
            }
        };

        let subj_sid = resolve_sid(&self.pattern.subject, subj_binding);
        let obj_sid = resolve_sid(&self.pattern.object, obj_binding);

        // Determine execution mode based on what's bound
        match (subj_sid, obj_sid) {
            (Some(start), None) => {
                // Subject bound, object unbound -> forward traversal
                let reachable = self.traverse_forward(ctx, &start).await?;

                // Build output rows
                let mut rows = Vec::with_capacity(reachable.len());
                for obj in reachable {
                    let mut row: Vec<Binding> = Vec::with_capacity(self.in_schema.len());
                    // Copy child bindings
                    for var in self.in_schema.iter() {
                        if let Some(col) = child_batch.column(*var) {
                            row.push(col[row_idx].clone());
                        } else if Some(*var) == obj_var {
                            row.push(Binding::sid(obj.clone()));
                        } else if Some(*var) == subj_var {
                            row.push(Binding::sid(start.clone()));
                        } else {
                            row.push(Binding::Unbound);
                        }
                    }
                    rows.push(row);
                }
                Ok(rows)
            }
            (None, Some(target)) => {
                // Object bound, subject unbound -> backward traversal
                let sources = self.traverse_backward(ctx, &target).await?;

                let mut rows = Vec::with_capacity(sources.len());
                for subj in sources {
                    let mut row: Vec<Binding> = Vec::with_capacity(self.in_schema.len());
                    for var in self.in_schema.iter() {
                        if let Some(col) = child_batch.column(*var) {
                            row.push(col[row_idx].clone());
                        } else if Some(*var) == subj_var {
                            row.push(Binding::sid(subj.clone()));
                        } else if Some(*var) == obj_var {
                            row.push(Binding::sid(target.clone()));
                        } else {
                            row.push(Binding::Unbound);
                        }
                    }
                    rows.push(row);
                }
                Ok(rows)
            }
            (Some(start), Some(target)) => {
                // Both bound -> reachability filter
                let exists = self.path_exists(ctx, &start, &target).await?;
                if exists {
                    // Keep this row unchanged
                    let mut row: Vec<Binding> = Vec::with_capacity(self.in_schema.len());
                    for var in self.in_schema.iter() {
                        if let Some(col) = child_batch.column(*var) {
                            row.push(col[row_idx].clone());
                        } else {
                            row.push(Binding::Unbound);
                        }
                    }
                    Ok(vec![row])
                } else {
                    // Filter out this row
                    Ok(vec![])
                }
            }
            (None, None) => {
                // Neither bound in correlated mode: treat as a bounded full closure for this predicate.
                // This commonly happens when the only child is the seed operator.
                let pairs = self.compute_closure(ctx).await?;
                let mut rows = Vec::with_capacity(pairs.len());
                for (subj, obj) in pairs {
                    let mut row: Vec<Binding> = Vec::with_capacity(self.in_schema.len());
                    for var in self.in_schema.iter() {
                        if let Some(col) = child_batch.column(*var) {
                            row.push(col[row_idx].clone());
                        } else if Some(*var) == subj_var {
                            row.push(Binding::sid(subj.clone()));
                        } else if Some(*var) == obj_var {
                            row.push(Binding::sid(obj.clone()));
                        } else {
                            row.push(Binding::Unbound);
                        }
                    }
                    rows.push(row);
                }
                Ok(rows)
            }
        }
    }
}

#[async_trait]
impl Operator for PropertyPathOperator {
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        self.child
            .as_deref()
            .map(|c| vec![crate::plan_node::PlanChild::child(c)])
            .unwrap_or_default()
    }
    fn schema(&self) -> &[VarId] {
        effective_schema(&self.out_schema, &self.in_schema)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if let Some(child) = &mut self.child {
            // Correlated mode: open child
            child.open(ctx).await?;
        } else {
            // Unseeded mode: execute traversal now
            self.execute_unseeded(ctx).await?;
        }
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        if self.child.is_none() {
            // Unseeded mode: return buffered results
            let results = self
                .results_buffer
                .as_ref()
                .ok_or_else(|| QueryError::OperatorNotOpened)?;

            if self.results_idx >= results.len() {
                self.state = OperatorState::Exhausted;
                return Ok(None);
            }

            // Build a batch from results
            const BATCH_SIZE: usize = 1024;
            let end = std::cmp::min(self.results_idx + BATCH_SIZE, results.len());
            let batch_results = &results[self.results_idx..end];
            self.results_idx = end;

            if batch_results.is_empty() {
                self.state = OperatorState::Exhausted;
                return Ok(None);
            }

            // Build columns
            let mut columns: Vec<Vec<Binding>> = self
                .in_schema
                .iter()
                .map(|_| Vec::with_capacity(batch_results.len()))
                .collect();

            let subj_var = match &self.pattern.subject {
                Ref::Var(v) => Some(*v),
                _ => None,
            };
            let obj_var = match &self.pattern.object {
                Ref::Var(v) => Some(*v),
                _ => None,
            };

            for (subj, obj) in batch_results {
                for (col_idx, var) in self.in_schema.iter().enumerate() {
                    if Some(*var) == subj_var {
                        columns[col_idx].push(Binding::sid(subj.clone()));
                    } else if Some(*var) == obj_var {
                        columns[col_idx].push(Binding::sid(obj.clone()));
                    } else {
                        columns[col_idx].push(Binding::Unbound);
                    }
                }
            }

            let batch = Batch::new(self.in_schema.clone(), columns)?;
            return Ok(trim_batch(&self.out_schema, batch));
        }

        // Correlated mode: process child rows
        loop {
            // Get next child batch if needed
            if self.current_child_batch.is_none() {
                let child = self.child.as_mut().unwrap();
                match child.next_batch(ctx).await? {
                    Some(batch) if !batch.is_empty() => {
                        self.current_child_batch = Some(batch);
                        self.current_child_row = 0;
                    }
                    Some(_) => continue, // Empty batch, try next
                    None => {
                        self.state = OperatorState::Exhausted;
                        return Ok(None);
                    }
                }
            }

            let child_batch = self.current_child_batch.as_ref().unwrap();

            // Process rows from current child batch
            let mut all_rows: Vec<Vec<Binding>> = Vec::new();

            while self.current_child_row < child_batch.len() {
                let rows = self
                    .process_correlated_row(ctx, child_batch, self.current_child_row)
                    .await?;
                self.current_child_row += 1;
                all_rows.extend(rows);

                // Return batch if we have enough rows
                if all_rows.len() >= 1024 {
                    break;
                }
            }

            // Clear current batch if exhausted
            if self.current_child_row >= child_batch.len() {
                self.current_child_batch = None;
            }

            if !all_rows.is_empty() {
                // Build batch from rows
                let mut columns: Vec<Vec<Binding>> = self
                    .in_schema
                    .iter()
                    .map(|_| Vec::with_capacity(all_rows.len()))
                    .collect();

                for row in all_rows {
                    for (col_idx, binding) in row.into_iter().enumerate() {
                        columns[col_idx].push(binding);
                    }
                }

                let batch = Batch::new(self.in_schema.clone(), columns)?;
                return Ok(trim_batch(&self.out_schema, batch));
            }

            // No rows from this batch, try next
        }
    }

    fn close(&mut self) {
        if let Some(ref mut child) = self.child {
            child.close();
        }
        self.results_buffer = None;
        self.current_child_batch = None;
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Hard to estimate for transitive traversal
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::triple::Ref;
    use fluree_db_core::Sid;

    fn make_pattern(subj: Ref, pred: Sid, modifier: PathModifier, obj: Ref) -> PropertyPathPattern {
        PropertyPathPattern::new(subj, pred, modifier, obj)
    }

    #[test]
    fn test_property_path_schema_forward() {
        // Subject constant, object variable -> schema has only object var
        let pattern = make_pattern(
            Ref::Sid(Sid::new(1, "alice")),
            Sid::new(2, "knows"),
            PathModifier::OneOrMore,
            Ref::Var(VarId(0)),
        );

        let op: PropertyPathOperator = PropertyPathOperator::with_defaults(None, pattern);

        assert_eq!(op.schema(), &[VarId(0)]);
    }

    #[test]
    fn test_property_path_schema_backward() {
        // Subject variable, object constant -> schema has only subject var
        let pattern = make_pattern(
            Ref::Var(VarId(0)),
            Sid::new(2, "knows"),
            PathModifier::ZeroOrMore,
            Ref::Sid(Sid::new(1, "bob")),
        );

        let op: PropertyPathOperator = PropertyPathOperator::with_defaults(None, pattern);

        assert_eq!(op.schema(), &[VarId(0)]);
    }

    #[test]
    fn test_property_path_schema_both_vars() {
        // Both subject and object are variables
        let pattern = make_pattern(
            Ref::Var(VarId(0)),
            Sid::new(2, "knows"),
            PathModifier::OneOrMore,
            Ref::Var(VarId(1)),
        );

        let op: PropertyPathOperator = PropertyPathOperator::with_defaults(None, pattern);

        assert_eq!(op.schema(), &[VarId(0), VarId(1)]);
    }

    #[test]
    fn test_property_path_max_visited_configurable() {
        let pattern = make_pattern(
            Ref::Sid(Sid::new(1, "alice")),
            Sid::new(2, "knows"),
            PathModifier::OneOrMore,
            Ref::Var(VarId(0)),
        );

        let op: PropertyPathOperator = PropertyPathOperator::new(None, pattern, 100);

        assert_eq!(op.max_visited, 100);
    }
}
