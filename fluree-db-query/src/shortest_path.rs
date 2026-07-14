//! Shortest-path operator — anchored bidirectional BFS.
//!
//! Implements Cypher `shortestPath((a)-[:T*]-(b))` and
//! `allShortestPaths(...)` over a single typed predicate. V1 contract:
//! **both endpoints must be bound** by a preceding pattern (anchored search).
//!
//! # Algorithm
//!
//! - `Single` mode: bidirectional BFS — two frontiers expand from each
//!   endpoint, alternating the smaller one, until they meet. Reconstructs one
//!   shortest path from the predecessor maps. Bidirectional search explores
//!   ~`O(b^(d/2))` instead of `O(b^d)`, decisive on large social graphs.
//! - `All` mode: layered forward BFS recording the full predecessor *set* at
//!   each distance, stopping at the layer where the end node is first reached,
//!   then enumerates every minimal-length path (capped).
//!
//! Neighbour expansion reuses the index access pattern from
//! [`crate::property_path`]: `Spot` (subject→object) and `Post`
//! (object→subject) range scans, ref-only edges, single active graph.
//!
//! BFS visited sets guarantee node-distinct paths, which for a single-predicate
//! graph also gives relationship-uniqueness (no repeated edge on a path).

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::frontier::{overlay_dirty_ids, reserved_edge_pids, FrontierView, PathNode};
use crate::ir::triple::Ref;
use crate::ir::{PathDirection, ShortestPathMode, ShortestPathPattern};
use crate::operator::{
    compute_trimmed_vars, effective_schema, trim_batch, BoxedOperator, Operator, OperatorState,
};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::{
    range_with_overlay, FlakeValue, IndexType, RangeMatch, RangeOptions, RangeTest, Sid,
};
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::HashMap;
use std::sync::Arc;

/// Safety bound: maximum nodes visited across both BFS frontiers per search.
pub const DEFAULT_MAX_VISITED: usize = crate::property_path::DEFAULT_MAX_VISITED;

/// Safety bound: maximum number of paths returned by `allShortestPaths`.
pub const DEFAULT_MAX_PATHS: usize = 1_000;

/// Depth cap for an unbounded lower-bounded search (`*min..` with no upper
/// bound), used by the iterative-deepening path search. A `*2..` shortest
/// path must stop deepening somewhere; the social-graph diameters this
/// targets are well under this.
pub const UNBOUNDED_DEPTH_CAP: u32 = 15;

/// Anchored shortest-path operator (bidirectional BFS).
pub struct ShortestPathOperator {
    /// Child operator providing bound endpoints (correlated execution).
    child: BoxedOperator,
    /// The shortest-path pattern.
    pattern: ShortestPathPattern,
    /// Output schema (child schema + path_var).
    in_schema: Arc<[VarId]>,
    /// Operator state.
    state: OperatorState,
    /// Safety bound for nodes visited.
    max_visited: usize,
    /// Safety bound for paths returned (All mode).
    max_paths: usize,
    /// Current child batch being processed.
    current_child_batch: Option<Batch>,
    /// Current row index within the child batch.
    current_child_row: usize,
    /// Variables required downstream; if set, output is trimmed.
    out_schema: Option<Arc<[VarId]>>,
}

impl ShortestPathOperator {
    /// Create a new shortest-path operator.
    pub fn new(child: BoxedOperator, pattern: ShortestPathPattern, max_visited: usize) -> Self {
        let mut schema_vec: Vec<VarId> = child.schema().to_vec();
        if !schema_vec.contains(&pattern.path_var) {
            schema_vec.push(pattern.path_var);
        }
        // Endpoint vars are already in the child schema (anchored contract);
        // include them defensively if a constant-bound endpoint slipped a var.
        for v in pattern.referenced_vars() {
            if !schema_vec.contains(&v) {
                schema_vec.push(v);
            }
        }
        Self {
            child,
            pattern,
            in_schema: Arc::from(schema_vec.into_boxed_slice()),
            state: OperatorState::Created,
            max_visited,
            max_paths: DEFAULT_MAX_PATHS,
            current_child_batch: None,
            current_child_row: 0,
            out_schema: None,
        }
    }

    /// Create with default safety bounds.
    pub fn with_defaults(child: BoxedOperator, pattern: ShortestPathPattern) -> Self {
        Self::new(child, pattern, crate::property_path::path_max_visited())
    }

    /// Trim output to only the specified downstream variables.
    pub fn with_out_schema(mut self, downstream_vars: Option<&[VarId]>) -> Self {
        self.out_schema = compute_trimmed_vars(&self.in_schema, downstream_vars);
        self
    }

    /// Resolve an endpoint ref to a Sid, from a constant or a child binding.
    fn resolve_endpoint(
        ctx: &ExecutionContext<'_>,
        term: &Ref,
        binding: Option<&Binding>,
    ) -> Option<Sid> {
        // Property paths / shortest paths require a single active graph; reuse
        // its store for IRI encoding (mirrors property_path::resolve_sid).
        let db = ctx.require_single_graph().ok().map(|(db, _, _)| db);
        let binary_store = ctx.binary_store.as_ref();
        match term {
            Ref::Sid(s) => Some(s.clone()),
            Ref::Iri(iri) => db.and_then(|db| db.encode_iri(iri)),
            Ref::Var(_) => binding.and_then(|b| match b {
                Binding::Sid { sid, .. } => Some(sid.clone()),
                Binding::IriMatch { iri, .. } => db.and_then(|db| db.encode_iri(iri)),
                Binding::Iri(iri) => db.and_then(|db| db.encode_iri(iri)),
                Binding::EncodedSid { s_id, .. } => binary_store
                    .and_then(|st| st.resolve_subject_iri(*s_id).ok())
                    .and_then(|iri| db.and_then(|db| db.encode_iri(&iri))),
                _ => None,
            }),
        }
    }

    /// One-hop neighbours of `node`.
    ///
    /// `forward = true` follows the *successor* direction (toward the end);
    /// `forward = false` follows *predecessors* (toward the start). The pairing
    /// of index probes to direction realises the arrow semantics:
    ///
    /// | direction | succ            | pred            |
    /// |-----------|-----------------|-----------------|
    /// | Outgoing  | Spot (objects)  | Post (subjects) |
    /// | Incoming  | Post (subjects) | Spot (objects)  |
    /// | Either    | Spot ∪ Post     | Spot ∪ Post     |
    async fn neighbors(
        &self,
        ctx: &ExecutionContext<'_>,
        node: &Sid,
        forward: bool,
    ) -> Result<Vec<Sid>> {
        let (use_spot, use_post) = match (self.pattern.direction, forward) {
            (PathDirection::Outgoing, true) => (true, false),
            (PathDirection::Outgoing, false) => (false, true),
            (PathDirection::Incoming, true) => (false, true),
            (PathDirection::Incoming, false) => (true, false),
            (PathDirection::Either, _) => (true, true),
        };

        let (db, overlay, to_t) = ctx.require_single_graph()?;
        let mut out = Vec::new();

        if use_spot {
            // Spot: (subject=node[, predicate]) → ref objects. Wildcard scans
            // all of the node's out-edges and drops reserved predicates
            // (`rdf:type`, `f:reifies*`), matching the wildcard property path.
            let mut range_match = RangeMatch::new().with_subject(node.clone());
            if let Some(pred) = &self.pattern.predicate {
                range_match = range_match.with_predicate(pred.clone());
            }
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
            for flake in flakes {
                if self.pattern.predicate.is_none()
                    && crate::property_path::is_reserved_edge_predicate(&flake.p)
                {
                    continue;
                }
                if let FlakeValue::Ref(obj) = flake.o {
                    out.push(obj);
                }
            }
        }

        if use_post {
            // Typed: Post (predicate, object=node) → subjects. Wildcard has no
            // predicate prefix, so it probes Opst (object=node) instead.
            let (index, range_match) = match &self.pattern.predicate {
                Some(pred) => (
                    IndexType::Post,
                    RangeMatch::new()
                        .with_predicate(pred.clone())
                        .with_object(FlakeValue::Ref(node.clone())),
                ),
                None => (
                    IndexType::Opst,
                    RangeMatch::new().with_object(FlakeValue::Ref(node.clone())),
                ),
            };
            let flakes = range_with_overlay(
                db,
                ctx.binary_g_id,
                overlay,
                index,
                RangeTest::Eq,
                range_match,
                RangeOptions::new().with_to_t(to_t),
            )
            .await?;
            for flake in flakes {
                if self.pattern.predicate.is_none()
                    && crate::property_path::is_reserved_edge_predicate(&flake.p)
                {
                    continue;
                }
                out.push(flake.s);
            }
        }

        Ok(out)
    }

    /// Post-hoc predicate lookup for one hop of a *wildcard* path: the first
    /// non-reserved reference edge stored as `a → b`. Runs only on found
    /// paths (≤ hop-count probes each), not during the search.
    async fn hop_predicate(
        &self,
        ctx: &ExecutionContext<'_>,
        a: &Sid,
        b: &Sid,
    ) -> Result<Option<Sid>> {
        let (db, overlay, to_t) = ctx.require_single_graph()?;
        let flakes = range_with_overlay(
            db,
            ctx.binary_g_id,
            overlay,
            IndexType::Spot,
            RangeTest::Eq,
            RangeMatch::new().with_subject(a.clone()),
            RangeOptions::new().with_to_t(to_t),
        )
        .await?;
        for flake in flakes {
            if crate::property_path::is_reserved_edge_predicate(&flake.p) {
                continue;
            }
            if matches!(&flake.o, FlakeValue::Ref(o) if o == b) {
                return Ok(Some(flake.p));
            }
        }
        Ok(None)
    }

    /// Build the per-hop `(start, predicate, end)` edge tuples for a found
    /// path. Typed patterns stamp the single predicate; wildcard patterns
    /// resolve each hop's stored edge with [`Self::hop_predicate`] (trying
    /// both orientations under `Either`; a hop whose predicate can't be
    /// resolved is skipped defensively).
    async fn build_edges(
        &self,
        ctx: &ExecutionContext<'_>,
        path: &[Sid],
    ) -> Result<Vec<(Sid, Sid, Sid)>> {
        let incoming = matches!(self.pattern.direction, PathDirection::Incoming);
        if let Some(pred) = &self.pattern.predicate {
            return Ok(path
                .windows(2)
                .map(|w| {
                    if incoming {
                        (w[1].clone(), pred.clone(), w[0].clone())
                    } else {
                        (w[0].clone(), pred.clone(), w[1].clone())
                    }
                })
                .collect());
        }
        let either = matches!(self.pattern.direction, PathDirection::Either);
        let mut edges = Vec::with_capacity(path.len().saturating_sub(1));
        for w in path.windows(2) {
            let (s, o) = if incoming {
                (&w[1], &w[0])
            } else {
                (&w[0], &w[1])
            };
            if let Some(p) = self.hop_predicate(ctx, s, o).await? {
                edges.push((s.clone(), p, o.clone()));
            } else if either {
                if let Some(p) = self.hop_predicate(ctx, o, s).await? {
                    edges.push((o.clone(), p, s.clone()));
                }
            }
        }
        Ok(edges)
    }

    /// Bidirectional BFS for a single shortest path. Returns the node sequence
    /// (start..end inclusive) or `None` if no path exists within the bounds.
    ///
    /// Tries the raw-id lane first (batched frontier expansion over base
    /// index rows, u64-keyed state — no per-node Sid materialization); the
    /// Sid-keyed per-node search remains the fallback for views the lane
    /// can't serve (no binary store, active policy, unsummarizable overlay).
    async fn bidirectional(
        &self,
        ctx: &ExecutionContext<'_>,
        start: &Sid,
        end: &Sid,
    ) -> Result<Option<Vec<Sid>>> {
        // Endpoints are part of `nodes(p)`: if either fails a pushed node
        // predicate, no qualifying path exists.
        if self.pattern.node_filter.is_some()
            && !(self.node_qualifies(ctx, start)? && self.node_qualifies(ctx, end)?)
        {
            return Ok(None);
        }
        if let Some(result) = self.bidirectional_ids(ctx, start, end).await? {
            return Ok(result);
        }
        self.bidirectional_sids(ctx, start, end).await
    }

    /// Whether `node` satisfies the pushed-in per-node predicate (an
    /// `all(x IN nodes(p) WHERE …)` filter moved into the search). `true` when
    /// there is no filter. Evaluated with the same member resolution as the
    /// post-filter, so a returned path's every node passes the original `all`.
    fn node_qualifies(&self, ctx: &ExecutionContext<'_>, node: &Sid) -> Result<bool> {
        match &self.pattern.node_filter {
            None => Ok(true),
            Some(nf) => crate::eval::eval_single_node_predicate(
                nf.var,
                &nf.predicate,
                Binding::sid(node.clone()),
                ctx,
            ),
        }
    }

    /// Sid-keyed bidirectional BFS (per-node `range_with_overlay` probes).
    async fn bidirectional_sids(
        &self,
        ctx: &ExecutionContext<'_>,
        start: &Sid,
        end: &Sid,
    ) -> Result<Option<Vec<Sid>>> {
        let min_hops = self.pattern.min_hops.unwrap_or(1);
        let max_hops = self.pattern.max_hops;

        if start == end {
            // Zero-length path; valid only if the min hop bound allows it.
            if min_hops == 0 {
                return Ok(Some(vec![start.clone()]));
            }
            // else fall through: look for a non-trivial cycle back to start.
        }

        // predecessor[node] = node it was reached from on the forward side;
        // the start maps to itself (chain sentinel).
        let mut fwd_prev: HashMap<Sid, Sid> = HashMap::new();
        let mut bwd_next: HashMap<Sid, Sid> = HashMap::new();
        fwd_prev.insert(start.clone(), start.clone());
        bwd_next.insert(end.clone(), end.clone());

        let mut fwd_frontier: Vec<Sid> = vec![start.clone()];
        let mut bwd_frontier: Vec<Sid> = vec![end.clone()];
        let mut depth = 0u32;

        while !fwd_frontier.is_empty() && !bwd_frontier.is_empty() {
            crate::fast_path_common::bail_if_cancelled(&ctx.cancellation)?;
            if fwd_prev.len() + bwd_next.len() >= self.max_visited {
                return Err(QueryError::ResourceLimit(format!(
                    "shortestPath exceeded max visited nodes ({})",
                    self.max_visited
                )));
            }
            if let Some(max) = max_hops {
                if depth >= max {
                    return Ok(None);
                }
            }
            depth += 1;

            // Expand the smaller frontier (the bidirectional win).
            let expand_forward = fwd_frontier.len() <= bwd_frontier.len();
            let frontier = if expand_forward {
                std::mem::take(&mut fwd_frontier)
            } else {
                std::mem::take(&mut bwd_frontier)
            };
            let mut next: Vec<Sid> = Vec::new();

            for node in &frontier {
                let nbrs = self.neighbors(ctx, node, expand_forward).await?;
                for nb in nbrs {
                    // Pushed node predicate: only traverse through qualifying
                    // nodes, so BFS returns the shortest path whose nodes all
                    // pass (endpoints checked up front in `bidirectional`).
                    if self.pattern.node_filter.is_some() && !self.node_qualifies(ctx, &nb)? {
                        continue;
                    }
                    let (near, far) = if expand_forward {
                        (&mut fwd_prev, &bwd_next)
                    } else {
                        (&mut bwd_next, &fwd_prev)
                    };
                    if near.contains_key(&nb) {
                        continue;
                    }
                    near.insert(nb.clone(), node.clone());
                    if far.contains_key(&nb) {
                        // Frontiers meet at `nb`. Reconstruct, honouring min_hops.
                        let path = self.reconstruct(&fwd_prev, &bwd_next, &nb, start, end);
                        if path.len().saturating_sub(1) as u32 >= min_hops {
                            return Ok(Some(path));
                        }
                        // Too short for the requested min; keep searching by not
                        // returning, but the node is recorded so we don't loop.
                    }
                    next.push(nb);
                }
            }

            if expand_forward {
                fwd_frontier = next;
            } else {
                bwd_frontier = next;
            }
        }

        Ok(None)
    }

    /// Stitch the forward and backward predecessor chains through meeting node
    /// `meet` into a single start→end node sequence.
    fn reconstruct(
        &self,
        fwd_prev: &HashMap<Sid, Sid>,
        bwd_next: &HashMap<Sid, Sid>,
        meet: &Sid,
        start: &Sid,
        end: &Sid,
    ) -> Vec<Sid> {
        // Forward: meet back to start.
        let mut left: Vec<Sid> = vec![meet.clone()];
        let mut cur = meet.clone();
        while &cur != start {
            match fwd_prev.get(&cur) {
                Some(p) if p != &cur => {
                    left.push(p.clone());
                    cur = p.clone();
                }
                _ => break,
            }
        }
        left.reverse(); // start .. meet

        // Backward: meet toward end (skip meet itself, already in `left`).
        let mut cur = meet.clone();
        while &cur != end {
            match bwd_next.get(&cur) {
                Some(n) if n != &cur => {
                    left.push(n.clone());
                    cur = n.clone();
                }
                _ => break,
            }
        }
        left
    }

    /// Raw-id bidirectional BFS: state keyed by persisted `s_id` (u64),
    /// frontier levels expanded with ONE batched galloping index sweep per
    /// side instead of a probe per node, and neighbors taken as raw `o_key`
    /// ids (for `IRI_REF` rows, `o_key` IS the target's `s_id` — no
    /// dictionary in the loop).
    ///
    /// Overlay correctness is per-node: a node with any overlay flake on the
    /// side being expanded (as subject for out-edges, as ref-object for
    /// in-edges — retracts stamp both) takes the Sid-space
    /// `range_with_overlay` fallback, whose results merge novelty. Subjects
    /// that exist only in novelty enter the search as `PathNode::Novel` and
    /// always expand via the fallback.
    ///
    /// Returns `None` when the lane can't serve the view (no binary store,
    /// active policy — base-row reads bypass the flake-level policy filter —
    /// or an overlay that can't be summarized per subject).
    async fn bidirectional_ids(
        &self,
        ctx: &ExecutionContext<'_>,
        start: &Sid,
        end: &Sid,
    ) -> Result<Option<Option<Vec<Sid>>>> {
        let Some(store) = ctx.binary_store.as_ref() else {
            return Ok(None);
        };
        if !ctx.allow_unfiltered() || ctx.is_multi_ledger() {
            return Ok(None);
        }
        // A pushed node predicate qualifies nodes by their properties; the
        // raw-id lane never materializes a node's Sid. Decline to the Sid lane,
        // which evaluates the predicate per node during expansion.
        if self.pattern.node_filter.is_some() {
            return Ok(None);
        }
        let (_db, overlay, to_t) = ctx.require_single_graph()?;
        let Some(dirty) = overlay_dirty_ids(overlay, ctx.binary_g_id, store) else {
            return Ok(None);
        };
        let store = Arc::clone(store);
        let g_id = ctx.binary_g_id;

        // Reserved edge predicates as base p_ids (absent from the base dict
        // means absent from base rows; the Sid fallback re-checks by Sid).
        let reserved_pids = reserved_edge_pids(&store);
        let typed_pid = match &self.pattern.predicate {
            Some(pred) => match store.sid_to_p_id(pred) {
                Some(p_id) => Some(p_id),
                // Predicate unknown to the base index: base has no such
                // edges; only novelty could. Let the Sid lane handle it.
                None if !overlay.is_effectively_empty() => return Ok(None),
                None => return Ok(Some(None)),
            },
            None => None,
        };

        let search = IdSearch {
            op: self,
            view: FrontierView {
                store,
                g_id,
                to_t,
                dirty,
            },
            reserved_pids,
            typed_pid,
        };
        search.run(ctx, start, end).await.map(Some)
    }

    /// Layered forward BFS that records all minimal-length predecessors, then
    /// enumerates every shortest path (capped at `max_paths`).
    async fn all_shortest(
        &self,
        ctx: &ExecutionContext<'_>,
        start: &Sid,
        end: &Sid,
    ) -> Result<Vec<Vec<Sid>>> {
        let min_hops = self.pattern.min_hops.unwrap_or(1);
        let max_hops = self.pattern.max_hops;

        if start == end && min_hops == 0 {
            return Ok(vec![vec![start.clone()]]);
        }

        let mut dist: HashMap<Sid, u32> = HashMap::new();
        let mut preds: HashMap<Sid, Vec<Sid>> = HashMap::new();
        dist.insert(start.clone(), 0);
        let mut frontier: Vec<Sid> = vec![start.clone()];
        let mut depth = 0u32;
        let mut found_depth: Option<u32> = None;

        while !frontier.is_empty() {
            crate::fast_path_common::bail_if_cancelled(&ctx.cancellation)?;
            if dist.len() >= self.max_visited {
                return Err(QueryError::ResourceLimit(format!(
                    "allShortestPaths exceeded max visited nodes ({})",
                    self.max_visited
                )));
            }
            if let Some(found) = found_depth {
                if depth >= found {
                    break;
                }
            }
            if let Some(max) = max_hops {
                if depth >= max {
                    break;
                }
            }
            depth += 1;

            let mut next: Vec<Sid> = Vec::new();
            for node in &frontier {
                let nbrs = self.neighbors(ctx, node, true).await?;
                for nb in nbrs {
                    match dist.get(&nb).copied() {
                        None => {
                            dist.insert(nb.clone(), depth);
                            preds.entry(nb.clone()).or_default().push(node.clone());
                            next.push(nb.clone());
                            if &nb == end {
                                found_depth = Some(depth);
                            }
                        }
                        Some(d) if d == depth => {
                            // Another equally-short predecessor.
                            preds.entry(nb.clone()).or_default().push(node.clone());
                        }
                        Some(_) => {}
                    }
                }
            }
            frontier = next;
        }

        let Some(found) = found_depth else {
            return Ok(Vec::new());
        };
        if found < min_hops {
            return Ok(Vec::new());
        }

        // Enumerate all shortest paths via DFS over the predecessor sets.
        let mut paths: Vec<Vec<Sid>> = Vec::new();
        let mut suffix: Vec<Sid> = vec![end.clone()];
        self.enumerate(end, start, &preds, &mut suffix, &mut paths)?;
        Ok(paths)
    }

    /// DFS over predecessor sets, accumulating start→end paths.
    ///
    /// `allShortestPaths` promises *every* minimal-length path, so exceeding
    /// `max_paths` is a hard error rather than a silent truncation — a
    /// quietly-capped result on a high-fan-out lattice would look complete
    /// while dropping paths.
    fn enumerate(
        &self,
        node: &Sid,
        start: &Sid,
        preds: &HashMap<Sid, Vec<Sid>>,
        suffix: &mut Vec<Sid>,
        out: &mut Vec<Vec<Sid>>,
    ) -> Result<()> {
        if node == start {
            let mut path = suffix.clone();
            path.reverse(); // suffix was built end→start
            out.push(path);
            if out.len() >= self.max_paths {
                return Err(QueryError::ResourceLimit(format!(
                    "allShortestPaths exceeded max paths ({})",
                    self.max_paths
                )));
            }
            return Ok(());
        }
        if let Some(parents) = preds.get(node) {
            for p in parents {
                suffix.push(p.clone());
                self.enumerate(p, start, preds, suffix, out)?;
                suffix.pop();
            }
        }
        Ok(())
    }

    /// Find the shortest node-distinct path(s) whose hop count is in
    /// `[min_hops, max_hops]`, used when `min_hops > 1`.
    ///
    /// Distance-finalizing BFS pins each node at its minimal distance, so it
    /// cannot find a qualifying longer path to a node that is also reachable
    /// more cheaply (e.g. `A→D` length 1 hides `A→B→D` length 2 under `*2..`).
    /// This iterative-deepening search tries each candidate length from
    /// `min_hops` upward and returns the path(s) at the first length that
    /// reaches `end`. `want_all` collects every path at that length.
    async fn bounded_qualifying_paths(
        &self,
        ctx: &ExecutionContext<'_>,
        start: &Sid,
        end: &Sid,
        want_all: bool,
    ) -> Result<Vec<Vec<Sid>>> {
        let min_hops = self.pattern.min_hops.unwrap_or(1).max(1);
        let max_hops = self.pattern.max_hops.unwrap_or(UNBOUNDED_DEPTH_CAP);
        for target_len in min_hops..=max_hops {
            let paths = self
                .exact_length_paths(ctx, start, end, target_len, want_all)
                .await?;
            if !paths.is_empty() {
                return Ok(paths);
            }
        }
        Ok(Vec::new())
    }

    /// All node-distinct paths of exactly `target_len` hops from `start` to
    /// `end` (iterative DFS over the live adjacency, honoring direction via
    /// [`Self::neighbors`]). `want_all = false` returns on the first hit.
    /// Bounded by `max_visited` (explored states) and `max_paths` (results →
    /// `ResourceLimit`). Node-distinctness approximates relationship-uniqueness,
    /// matching the bounded variable-length read path.
    async fn exact_length_paths(
        &self,
        ctx: &ExecutionContext<'_>,
        start: &Sid,
        end: &Sid,
        target_len: u32,
        want_all: bool,
    ) -> Result<Vec<Vec<Sid>>> {
        let mut results: Vec<Vec<Sid>> = Vec::new();
        let mut stack: Vec<Vec<Sid>> = vec![vec![start.clone()]];
        let mut states: usize = 0;

        while let Some(path) = stack.pop() {
            crate::fast_path_common::bail_if_cancelled(&ctx.cancellation)?;
            let depth = (path.len() - 1) as u32;
            let last = path.last().expect("path always carries the start node");

            if depth == target_len {
                if last == end {
                    results.push(path);
                    if !want_all {
                        return Ok(results);
                    }
                    if results.len() >= self.max_paths {
                        return Err(QueryError::ResourceLimit(format!(
                            "allShortestPaths exceeded max paths ({})",
                            self.max_paths
                        )));
                    }
                }
                continue;
            }

            // On the final hop only `end` can complete a qualifying path —
            // prune everything else so the last layer doesn't fan out.
            let final_hop = depth + 1 == target_len;
            let nbrs = self.neighbors(ctx, last, true).await?;
            for nb in nbrs {
                if final_hop && &nb != end {
                    continue;
                }
                if path.contains(&nb) {
                    continue; // node-distinct
                }
                states += 1;
                if states >= self.max_visited {
                    return Err(QueryError::ResourceLimit(format!(
                        "shortestPath exceeded max visited nodes ({})",
                        self.max_visited
                    )));
                }
                let mut next = path.clone();
                next.push(nb);
                stack.push(next);
            }
        }
        Ok(results)
    }

    /// Every path from `start` whose hop count lies in `[min_hops, max_hops]`
    /// under Cypher **relationship-uniqueness** (trail semantics — no edge is
    /// traversed twice, but a node MAY be revisited via a different edge; this
    /// matches Neo4j, e.g. the triangle closure `a→b→c→a` is a valid 3-hop
    /// path). A `Some(end)` filter keeps only paths ending there; `None` emits
    /// one row per qualifying path to *any* node. An unbounded `max_hops` has no
    /// depth cap — relationship-uniqueness still makes the search finite (each
    /// edge is used at most once), and the `max_visited` / `max_paths` guards
    /// fail loudly (never truncate silently) when a graph is too dense.
    async fn enumerate_all_paths(
        &self,
        ctx: &ExecutionContext<'_>,
        start: &Sid,
        end: Option<&Sid>,
    ) -> Result<Vec<Vec<Sid>>> {
        let min_hops = self.pattern.min_hops.unwrap_or(1);
        let max_hops = self.pattern.max_hops;
        let mut results: Vec<Vec<Sid>> = Vec::new();
        let mut stack: Vec<Vec<Sid>> = vec![vec![start.clone()]];
        let mut states: usize = 0;

        while let Some(path) = stack.pop() {
            crate::fast_path_common::bail_if_cancelled(&ctx.cancellation)?;
            let depth = (path.len() - 1) as u32;
            let last = path.last().expect("path always carries the start node");

            if depth >= min_hops && end.is_none_or(|e| e == last) {
                results.push(path.clone());
                if results.len() >= self.max_paths {
                    return Err(QueryError::ResourceLimit(format!(
                        "path enumeration exceeded max paths ({}); narrow the pattern \
                         (tighter hop bounds, a bound end node, or a specific type)",
                        self.max_paths
                    )));
                }
            }
            if max_hops.is_some_and(|m| depth >= m) {
                continue;
            }
            // Cypher relationship-uniqueness (trail): a node may be revisited,
            // but no edge twice. The only edge this step can add is `last`→`nb`,
            // so reject exactly that edge if the path already traversed it —
            // gather the nodes already reached across an edge at `last` (directed:
            // as the edge's source; undirected: either orientation).
            let undirected = matches!(self.pattern.direction, PathDirection::Either);
            let reached_via_last: Vec<Sid> = path
                .windows(2)
                .filter_map(|w| {
                    if w[0] == *last {
                        Some(w[1].clone())
                    } else if undirected && w[1] == *last {
                        Some(w[0].clone())
                    } else {
                        None
                    }
                })
                .collect();
            for nb in self.neighbors(ctx, last, true).await? {
                if reached_via_last.contains(&nb) {
                    continue; // relationship-uniqueness: edge last↔nb already used
                }
                states += 1;
                if states >= self.max_visited {
                    return Err(QueryError::ResourceLimit(format!(
                        "path enumeration exceeded max visited nodes ({}); narrow the \
                         pattern (tighter hop bounds, a bound end node, or a specific type)",
                        self.max_visited
                    )));
                }
                let mut next = path.clone();
                next.push(nb);
                stack.push(next);
            }
        }
        Ok(results)
    }

    /// Process one child row: resolve endpoints, search, build output rows.
    async fn process_row(
        &self,
        ctx: &ExecutionContext<'_>,
        child_batch: &Batch,
        row_idx: usize,
    ) -> Result<Vec<Vec<Binding>>> {
        let start_binding = match &self.pattern.start {
            Ref::Var(v) => child_batch.column(*v).map(|c| &c[row_idx]),
            _ => None,
        };
        let end_binding = match &self.pattern.end {
            Ref::Var(v) => child_batch.column(*v).map(|c| &c[row_idx]),
            _ => None,
        };

        let start = Self::resolve_endpoint(ctx, &self.pattern.start, start_binding);
        let end = Self::resolve_endpoint(ctx, &self.pattern.end, end_binding);

        // The start always anchors the search; the shortest modes also anchor
        // the end. A missing anchor emits no row (a mandatory MATCH drops it;
        // an OPTIONAL wrapper restores it as null). Enumerate treats a
        // resolved end as a filter and an unresolved one as "produce it".
        let Some(start) = start else {
            return Ok(Vec::new());
        };
        let enumerate = matches!(self.pattern.mode, ShortestPathMode::Enumerate);
        // The end var (if any), for binding produced ends under Enumerate.
        let end_var = match &self.pattern.end {
            Ref::Var(v) => Some(*v),
            _ => None,
        };

        let paths = if enumerate {
            self.enumerate_all_paths(ctx, &start, end.as_ref()).await?
        } else {
            let Some(end) = end else {
                return Ok(Vec::new());
            };
            let want_all = matches!(self.pattern.mode, ShortestPathMode::All);
            if self.pattern.min_hops.unwrap_or(1) > 1 {
                // A lower hop bound > 1 can require a *longer* path than the plain
                // shortest one, which distance-finalizing BFS cannot discover (it
                // pins each node at its minimal distance). Use iterative-deepening
                // node-distinct search instead.
                self.bounded_qualifying_paths(ctx, &start, &end, want_all)
                    .await?
            } else if want_all {
                self.all_shortest(ctx, &start, &end).await?
            } else {
                match self.bidirectional(ctx, &start, &end).await? {
                    Some(p) => vec![p],
                    None => Vec::new(),
                }
            }
        };

        let mut rows = Vec::with_capacity(paths.len());
        for path in paths {
            // Orient each hop's edge by the traversal direction: outgoing keeps
            // node[i]→node[i+1]; incoming flips to the stored edge. For an
            // undirected (`Either`) search the per-hop orientation isn't
            // recorded, so this falls back to traversal order (best effort;
            // wildcard hops probe both orientations).
            //
            // Only Cypher's `relationships(p)` reads `edges`; skip the per-hop
            // work entirely on surfaces that don't (JSON-LD/FQL), where
            // `needs_relationships` is false.
            let edges: Vec<(Sid, Sid, Sid)> = if self.pattern.needs_relationships {
                self.build_edges(ctx, &path).await?
            } else {
                Vec::new()
            };
            let mut row: Vec<Binding> = Vec::with_capacity(self.in_schema.len());
            for var in self.in_schema.iter() {
                if *var == self.pattern.path_var {
                    row.push(Binding::Path {
                        nodes: path.clone(),
                        edges: edges.clone(),
                    });
                } else if let Some(col) = child_batch.column(*var) {
                    // Enumerate with an unbound end: bind it from the path's
                    // final node (a child column may exist but hold Unbound,
                    // e.g. under an OPTIONAL replay).
                    if enumerate
                        && end_var == Some(*var)
                        && matches!(col[row_idx], Binding::Unbound)
                    {
                        row.push(Binding::sid(path.last().expect("non-empty path").clone()));
                    } else {
                        row.push(col[row_idx].clone());
                    }
                } else if enumerate && end_var == Some(*var) {
                    row.push(Binding::sid(path.last().expect("non-empty path").clone()));
                } else {
                    row.push(Binding::Unbound);
                }
            }
            rows.push(row);
        }
        Ok(rows)
    }
}

// ============================================================================
// Raw-id bidirectional lane (shared frontier machinery in `crate::frontier`)
// ============================================================================

/// One anchored raw-id search over a fixed view.
struct IdSearch<'a> {
    op: &'a ShortestPathOperator,
    view: FrontierView,
    /// Reserved edge predicates (`rdf:type`, `f:reifies*`) as base p_ids;
    /// wildcard expansion drops their rows, mirroring
    /// [`crate::property_path::is_reserved_edge_predicate`].
    reserved_pids: FxHashSet<u32>,
    /// The single typed predicate's base p_id (`None` = wildcard path).
    typed_pid: Option<u32>,
}

impl IdSearch<'_> {
    fn node_for_sid(&self, sid: &Sid) -> PathNode {
        self.view.node_for_sid(sid)
    }

    fn sid_for_node(&self, node: &PathNode) -> Result<Sid> {
        self.view.sid_for_node(node)
    }

    /// Which edge orientations this expansion side follows (mirrors
    /// [`ShortestPathOperator::neighbors`]'s direction table).
    fn orientations(&self, expand_forward: bool) -> (bool, bool) {
        match (self.op.pattern.direction, expand_forward) {
            (PathDirection::Outgoing, true) | (PathDirection::Incoming, false) => (true, false),
            (PathDirection::Outgoing, false) | (PathDirection::Incoming, true) => (false, true),
            (PathDirection::Either, _) => (true, true),
        }
    }

    /// A persisted node's base rows are the whole truth for this side iff
    /// the overlay never touches the orientations being followed.
    fn is_clean(&self, s_id: u64, use_out: bool, use_in: bool) -> bool {
        self.view.is_clean(s_id, use_out, use_in)
    }

    /// Expand one frontier level: batched galloping sweeps for clean
    /// persisted nodes, per-node Sid probes for dirty/novelty ones.
    /// Returns `(source, neighbor)` pairs.
    async fn expand(
        &self,
        ctx: &ExecutionContext<'_>,
        frontier: &[PathNode],
        expand_forward: bool,
    ) -> Result<Vec<(PathNode, PathNode)>> {
        let (use_out, use_in) = self.orientations(expand_forward);
        let mut batched: Vec<u64> = Vec::new();
        let mut fallback: Vec<(PathNode, Sid)> = Vec::new();
        for node in frontier {
            match node {
                PathNode::Id(s_id) if self.is_clean(*s_id, use_out, use_in) => {
                    batched.push(*s_id);
                }
                PathNode::Id(_) => fallback.push((node.clone(), self.sid_for_node(node)?)),
                PathNode::Novel(sid) => fallback.push((node.clone(), sid.clone())),
            }
        }
        batched.sort_unstable();
        batched.dedup();

        let mut out: Vec<(PathNode, PathNode)> = Vec::new();

        if !batched.is_empty() {
            if use_out {
                self.batched_out(&batched, &mut out)?;
            }
            if use_in {
                self.batched_in(&batched, &mut out)?;
            }
        }
        for (node, sid) in fallback {
            let nbrs = self.op.neighbors(ctx, &sid, expand_forward).await?;
            for nb in nbrs {
                let nb_node = self.node_for_sid(&nb);
                out.push((node.clone(), nb_node));
            }
        }
        Ok(out)
    }

    /// Out-edges of `subjects` from base rows. For `IRI_REF` rows `o_key`
    /// is the target's `s_id` — neighbors come back as raw ids.
    fn batched_out(&self, subjects: &[u64], out: &mut Vec<(PathNode, PathNode)>) -> Result<()> {
        let mut pairs: Vec<(u64, u64)> = Vec::new();
        match self.typed_pid {
            Some(p_id) => self.view.out_typed(p_id, subjects, &mut pairs)?,
            None => self
                .view
                .out_wildcard(subjects, &self.reserved_pids, &mut pairs)?,
        }
        out.extend(
            pairs
                .into_iter()
                .map(|(s, t)| (PathNode::Id(s), PathNode::Id(t))),
        );
        Ok(())
    }

    /// In-edges pointing at `objects` from base rows.
    fn batched_in(&self, objects: &[u64], out: &mut Vec<(PathNode, PathNode)>) -> Result<()> {
        let mut pairs: Vec<(u64, u64)> = Vec::new();
        let keep = |p_id: u32| match self.typed_pid {
            Some(tp) => p_id == tp,
            None => !self.reserved_pids.contains(&p_id),
        };
        self.view.in_edges(objects, keep, &mut pairs)?;
        out.extend(
            pairs
                .into_iter()
                .map(|(o, s)| (PathNode::Id(o), PathNode::Id(s))),
        );
        Ok(())
    }

    /// The bidirectional meet-in-the-middle loop, u64-keyed. Structure is
    /// identical to [`ShortestPathOperator::bidirectional_sids`].
    async fn run(
        &self,
        ctx: &ExecutionContext<'_>,
        start: &Sid,
        end: &Sid,
    ) -> Result<Option<Vec<Sid>>> {
        let min_hops = self.op.pattern.min_hops.unwrap_or(1);
        let max_hops = self.op.pattern.max_hops;

        let start_node = self.node_for_sid(start);
        let end_node = self.node_for_sid(end);

        if start_node == end_node && min_hops == 0 {
            return Ok(Some(vec![start.clone()]));
        }

        let mut fwd_prev: FxHashMap<PathNode, PathNode> = FxHashMap::default();
        let mut bwd_next: FxHashMap<PathNode, PathNode> = FxHashMap::default();
        fwd_prev.insert(start_node.clone(), start_node.clone());
        bwd_next.insert(end_node.clone(), end_node.clone());

        let mut fwd_frontier: Vec<PathNode> = vec![start_node.clone()];
        let mut bwd_frontier: Vec<PathNode> = vec![end_node.clone()];
        let mut depth = 0u32;

        while !fwd_frontier.is_empty() && !bwd_frontier.is_empty() {
            crate::fast_path_common::bail_if_cancelled(&ctx.cancellation)?;
            if fwd_prev.len() + bwd_next.len() >= self.op.max_visited {
                return Err(QueryError::ResourceLimit(format!(
                    "shortestPath exceeded max visited nodes ({})",
                    self.op.max_visited
                )));
            }
            if let Some(max) = max_hops {
                if depth >= max {
                    return Ok(None);
                }
            }
            depth += 1;

            let expand_forward = fwd_frontier.len() <= bwd_frontier.len();
            let frontier = if expand_forward {
                std::mem::take(&mut fwd_frontier)
            } else {
                std::mem::take(&mut bwd_frontier)
            };
            let mut next: Vec<PathNode> = Vec::new();

            let pairs = self.expand(ctx, &frontier, expand_forward).await?;
            for (node, nb) in pairs {
                let (near, far) = if expand_forward {
                    (&mut fwd_prev, &bwd_next)
                } else {
                    (&mut bwd_next, &fwd_prev)
                };
                if near.contains_key(&nb) {
                    continue;
                }
                near.insert(nb.clone(), node.clone());
                if far.contains_key(&nb) {
                    let path =
                        self.reconstruct(&fwd_prev, &bwd_next, &nb, &start_node, &end_node)?;
                    if path.len().saturating_sub(1) as u32 >= min_hops {
                        return Ok(Some(path));
                    }
                }
                next.push(nb);
            }

            if expand_forward {
                fwd_frontier = next;
            } else {
                bwd_frontier = next;
            }
        }

        Ok(None)
    }

    /// Stitch the predecessor chains through `meet` and materialize Sids —
    /// the only place the raw-id lane touches the dictionary, bounded by
    /// path length.
    fn reconstruct(
        &self,
        fwd_prev: &FxHashMap<PathNode, PathNode>,
        bwd_next: &FxHashMap<PathNode, PathNode>,
        meet: &PathNode,
        start: &PathNode,
        end: &PathNode,
    ) -> Result<Vec<Sid>> {
        let mut left: Vec<PathNode> = vec![meet.clone()];
        let mut cur = meet.clone();
        while &cur != start {
            match fwd_prev.get(&cur) {
                Some(p) if p != &cur => {
                    left.push(p.clone());
                    cur = p.clone();
                }
                _ => break,
            }
        }
        left.reverse();

        let mut cur = meet.clone();
        while &cur != end {
            match bwd_next.get(&cur) {
                Some(n) if n != &cur => {
                    left.push(n.clone());
                    cur = n.clone();
                }
                _ => break,
            }
        }
        left.iter().map(|n| self.sid_for_node(n)).collect()
    }
}

#[async_trait]
impl Operator for ShortestPathOperator {
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        vec![crate::plan_node::PlanChild::child(self.child.as_ref())]
    }

    fn schema(&self) -> &[VarId] {
        effective_schema(&self.out_schema, &self.in_schema)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        loop {
            if self.current_child_batch.is_none() {
                match self.child.next_batch(ctx).await? {
                    Some(batch) if !batch.is_empty() => {
                        self.current_child_batch = Some(batch);
                        self.current_child_row = 0;
                    }
                    Some(_) => continue,
                    None => {
                        self.state = OperatorState::Exhausted;
                        return Ok(None);
                    }
                }
            }

            let child_batch = self.current_child_batch.as_ref().unwrap();
            let mut all_rows: Vec<Vec<Binding>> = Vec::new();

            while self.current_child_row < child_batch.len() {
                let rows = self
                    .process_row(ctx, child_batch, self.current_child_row)
                    .await?;
                self.current_child_row += 1;
                all_rows.extend(rows);
                if all_rows.len() >= 1024 {
                    break;
                }
            }

            if self.current_child_row >= child_batch.len() {
                self.current_child_batch = None;
            }

            if !all_rows.is_empty() {
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
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.current_child_batch = None;
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Anchored: at most one row per input (Single) — All and Enumerate
        // are unbounded.
        match self.pattern.mode {
            ShortestPathMode::Single => self.child.estimated_rows(),
            ShortestPathMode::All | ShortestPathMode::Enumerate => None,
        }
    }
}
