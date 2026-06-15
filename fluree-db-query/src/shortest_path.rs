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
use std::collections::HashMap;
use std::sync::Arc;

/// Safety bound: maximum nodes visited across both BFS frontiers per search.
pub const DEFAULT_MAX_VISITED: usize = 100_000;

/// Safety bound: maximum number of paths returned by `allShortestPaths`.
pub const DEFAULT_MAX_PATHS: usize = 1_000;

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
        Self::new(child, pattern, DEFAULT_MAX_VISITED)
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
            // Spot: (subject=node, predicate) → ref objects.
            let range_match = RangeMatch::new()
                .with_subject(node.clone())
                .with_predicate(self.pattern.predicate.clone());
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
                if let FlakeValue::Ref(obj) = flake.o {
                    out.push(obj);
                }
            }
        }

        if use_post {
            // Post: (predicate, object=node) → subjects.
            let range_match = RangeMatch::new()
                .with_predicate(self.pattern.predicate.clone())
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
            for flake in flakes {
                out.push(flake.s);
            }
        }

        Ok(out)
    }

    /// Bidirectional BFS for a single shortest path. Returns the node sequence
    /// (start..end inclusive) or `None` if no path exists within the bounds.
    async fn bidirectional(
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
        self.enumerate(end, start, &preds, &mut suffix, &mut paths);
        Ok(paths)
    }

    /// DFS over predecessor sets, accumulating start→end paths (capped).
    fn enumerate(
        &self,
        node: &Sid,
        start: &Sid,
        preds: &HashMap<Sid, Vec<Sid>>,
        suffix: &mut Vec<Sid>,
        out: &mut Vec<Vec<Sid>>,
    ) {
        if out.len() >= self.max_paths {
            return;
        }
        if node == start {
            let mut path = suffix.clone();
            path.reverse(); // suffix was built end→start
            out.push(path);
            return;
        }
        if let Some(parents) = preds.get(node) {
            for p in parents {
                suffix.push(p.clone());
                self.enumerate(p, start, preds, suffix, out);
                suffix.pop();
                if out.len() >= self.max_paths {
                    return;
                }
            }
        }
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

        // Anchored contract: both endpoints must resolve. If not, emit no row
        // (a mandatory MATCH drops it; an OPTIONAL wrapper restores it as null).
        let (Some(start), Some(end)) = (start, end) else {
            return Ok(Vec::new());
        };

        let paths = match self.pattern.mode {
            ShortestPathMode::Single => match self.bidirectional(ctx, &start, &end).await? {
                Some(p) => vec![p],
                None => Vec::new(),
            },
            ShortestPathMode::All => self.all_shortest(ctx, &start, &end).await?,
        };

        let mut rows = Vec::with_capacity(paths.len());
        for path in paths {
            let mut row: Vec<Binding> = Vec::with_capacity(self.in_schema.len());
            for var in self.in_schema.iter() {
                if *var == self.pattern.path_var {
                    row.push(Binding::Path(path.clone()));
                } else if let Some(col) = child_batch.column(*var) {
                    row.push(col[row_idx].clone());
                } else {
                    row.push(Binding::Unbound);
                }
            }
            rows.push(row);
        }
        Ok(rows)
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
        // Anchored: at most one row per input (Single) — All is unbounded.
        match self.pattern.mode {
            ShortestPathMode::Single => self.child.estimated_rows(),
            ShortestPathMode::All => None,
        }
    }
}
