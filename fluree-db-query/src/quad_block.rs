//! `GRAPH ?g { … }` over the pooled tree in one pass.
//!
//! A `GRAPH ?g` block normally runs once per named graph ([`GraphOperator`]):
//! it plans and opens its patterns again for every graph, which over
//! thousands of small pooled graphs costs far more than the rows themselves.
//! When the block is a join of triple patterns, with filters on its own
//! variables, and the rows feeding it share none of them, the pooled graphs
//! without novelty are answered together: each pattern is one quad scan of
//! the pooled tree that binds `?g` per row, and the scans hash-join on their
//! shared variables and `?g`. Every other graph — one with trees of its own,
//! a pooled one with novelty — runs through [`GraphOperator`] as before, and
//! so does the whole block whenever the quad path declines.

use crate::binary_scan::{BinaryScanOperator, QuadGraphs};
use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::graph::GraphOperator;
use crate::ir::triple::{Ref, TriplePattern};
use crate::ir::{GraphName, Pattern};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::seed::BatchReplayOperator;
use crate::sort::SortSpec;
use crate::var_registry::VarId;
use crate::PlanningContext;
use async_trait::async_trait;
use std::sync::Arc;

/// The quad path declines when the pool probe leaves fewer than one graph in
/// this many.
const SELECTIVE_PROBE_FACTOR: usize = 4;

/// Logged when a block's pooled graphs are answered by quad scans.
pub(crate) const QUAD_BLOCK_EVENT: &str = "quad block engaged";

/// A `GRAPH ?g` block this operator can answer with quad scans: its triple
/// patterns (each with a bound predicate, none naming `?g`) and its filters,
/// which read only variables the triples bind.
struct QuadShape {
    graph_var: VarId,
    triples: Vec<TriplePattern>,
    filters: Vec<Pattern>,
}

impl QuadShape {
    fn of(name: &GraphName, patterns: &[Pattern]) -> Option<Self> {
        let GraphName::Var(graph_var) = name else {
            return None;
        };
        let mut triples = Vec::new();
        let mut filters = Vec::new();
        for pattern in patterns {
            match pattern {
                Pattern::Triple(tp) => triples.push(tp.clone()),
                Pattern::Filter(expr) if graph_free(expr) => filters.push(pattern.clone()),
                _ => return None,
            }
        }
        let names_graph = |tp: &TriplePattern| tp.referenced_vars().contains(graph_var);
        if triples.is_empty()
            || triples
                .iter()
                .any(|tp| !matches!(tp.p, Ref::Sid(_)) || names_graph(tp))
        {
            return None;
        }
        // A filter in the block runs before `?g` joins in, so it must not
        // read `?g`, which the quad rows already carry.
        let block_vars: Vec<VarId> = triples
            .iter()
            .flat_map(TriplePattern::referenced_vars)
            .collect();
        if filters
            .iter()
            .flat_map(Pattern::referenced_vars)
            .any(|v| !block_vars.contains(&v))
        {
            return None;
        }
        Some(Self {
            graph_var: *graph_var,
            triples,
            filters,
        })
    }
}

/// Whether a filter can run over quad rows: variables, constants and
/// function calls only — no `EXISTS` or other patterns, which read the active
/// graph; no full-text scoring, which reads its arenas; and no `T` / `OP`,
/// which read flake metadata a quad scan does not carry.
fn graph_free(expr: &crate::ir::Expression) -> bool {
    use crate::ir::{Expression, Function};
    match expr {
        Expression::Var(_) | Expression::Const(_) => true,
        Expression::Call { func, args } => {
            !matches!(func, Function::Fulltext | Function::T | Function::Op)
                && args.iter().all(graph_free)
        }
        _ => false,
    }
}

/// A batch and the query memory charged for it.
struct Charged {
    batch: Batch,
    bytes: usize,
}

/// Charge `rows` rows of `cols` bindings to the query's retained memory and
/// enforce its budget.
fn charge(ctx: &ExecutionContext<'_>, rows: usize, cols: usize) -> Result<usize> {
    let bytes = rows
        .saturating_mul(cols.max(1))
        .saturating_mul(crate::context::BINDING_EST_BYTES);
    ctx.record_alloc(bytes);
    ctx.checkpoint()?;
    Ok(bytes)
}

/// Rows a join appends between memory charges.
const CHARGE_ROWS: usize = 4096;

/// Quad results being paired with the parent rows, one output batch at a
/// time.
struct QuadOutput {
    /// The execution's memory counter, released when the output drops.
    cancellation: fluree_db_core::QueryCancellation,
    schema: Arc<[VarId]>,
    parents: Vec<Batch>,
    rows: Charged,
    parent_bytes: usize,
    /// Next (parent batch, parent row, quad row) to emit.
    cursor: (usize, usize, usize),
}

impl Drop for QuadOutput {
    fn drop(&mut self) {
        self.cancellation
            .release(self.rows.bytes + self.parent_bytes);
    }
}

/// `GRAPH ?g { … }`: quad scans for the pooled graphs without novelty when
/// the block admits them, [`GraphOperator`] for the rest.
pub struct QuadBlockOperator {
    graph: GraphOperator,
    shape: Option<QuadShape>,
    planning: PlanningContext,
    /// A downstream row goal: the per-graph path stops early, which a quad
    /// scan of the whole pool cannot, so the block runs per graph.
    row_budget: Option<usize>,
    /// Quad results, emitted ahead of `graph`'s.
    quad: Option<QuadOutput>,
    state: OperatorState,
}

impl QuadBlockOperator {
    pub fn new(
        child: BoxedOperator,
        graph_name: GraphName,
        inner_patterns: Vec<Pattern>,
        planning: PlanningContext,
    ) -> Self {
        let shape = QuadShape::of(&graph_name, &inner_patterns);
        Self {
            graph: GraphOperator::new(child, graph_name, inner_patterns, planning),
            shape,
            planning,
            row_budget: None,
            quad: None,
            state: OperatorState::Created,
        }
    }

    /// The pooled graphs a quad scan can answer for this query: registered
    /// user graphs in the pool with no novelty, or `None` when the context
    /// rules the quad path out.
    fn quad_graphs(&self, ctx: &ExecutionContext<'_>) -> Option<Arc<QuadGraphs>> {
        let shape = self.shape.as_ref()?;
        let store = ctx.binary_store.as_ref()?;
        if crate::execute::operator_tree::fast_paths_disabled()
            || store.pooled_graph_ids().is_empty()
            || ctx.explicit_dataset().is_some()
            || ctx.is_multi_ledger()
            || ctx.from_t.is_some()
            || ctx.is_history_range()
            // Before the index's `t`, rows would need their history.
            || ctx.to_t < store.max_t()
            || !ctx.allow_unfiltered()
        {
            return None;
        }
        // Rows feeding the block must not share its variables: the quad
        // results are computed once and paired with every parent row.
        let parent = self.graph.parent_schema();
        let mut block_vars = shape
            .triples
            .iter()
            .flat_map(TriplePattern::referenced_vars);
        if parent.contains(&shape.graph_var) || block_vars.any(|v| parent.contains(&v)) {
            return None;
        }
        let clean = |g_id| {
            ctx.overlay
                .is_none_or(|o| o.is_effectively_empty() || o.overlay_flake_count(g_id) == Some(0))
        };
        let graphs = QuadGraphs::new(ctx.single_db_user_graph_iris().into_iter().filter_map(
            |iri| {
                let g_id = ctx.single_db_user_graph_id(&iri)?;
                (store.is_pooled(g_id) && clean(g_id)).then_some((g_id, iri))
            },
        ));
        (graphs.len() > 0).then(|| Arc::new(graphs))
    }

    /// The block over `graphs`: each triple pattern's quad scan, hash-joined
    /// on the variables (and graph) it shares with the patterns before it,
    /// then the block's filters. Every buffer is charged to the query's
    /// memory budget as it grows and released when dropped.
    async fn run_quad(
        &self,
        ctx: &ExecutionContext<'_>,
        shape: &QuadShape,
        graphs: &Arc<QuadGraphs>,
    ) -> Result<Charged> {
        // A pattern binding its object or subject is the most selective
        // start; the rest keep their written order.
        let mut order: Vec<&TriplePattern> = shape.triples.iter().collect();
        order.sort_by_key(|tp| (!tp.o_bound(), !tp.s_bound()));

        let mut acc: Option<Charged> = None;
        for tp in order {
            let mut scan = BinaryScanOperator::new(tp.clone(), None, Vec::new())
                .into_quad(shape.graph_var, Arc::clone(graphs));
            scan.open(ctx).await?;
            let schema: Arc<[VarId]> = Arc::from(scan.schema());
            let mut batches = Vec::new();
            let mut bytes = 0;
            while let Some(batch) = scan.next_batch(ctx).await? {
                bytes += charge(ctx, batch.len(), schema.len())?;
                batches.push(batch);
            }
            scan.close();
            let right = Charged {
                batch: concat(schema, batches)?,
                bytes,
            };
            let joined = match acc.take() {
                None => right,
                Some(left) => {
                    let joined = hash_join(ctx, shape.graph_var, &left.batch, &right.batch)?;
                    ctx.release(left.bytes + right.bytes);
                    joined
                }
            };
            if joined.batch.is_empty() {
                return Ok(joined);
            }
            acc = Some(joined);
        }
        let joined = acc.expect("a quad shape has a triple pattern");
        if shape.filters.is_empty() {
            return Ok(joined);
        }
        let schema = joined.batch.schema_arc();
        let seed = BatchReplayOperator::new(Arc::clone(&schema), vec![joined.batch]);
        let mut filtered = crate::execute::build_where_operators_seeded(
            Some(Box::new(seed)),
            &shape.filters,
            None,
            None,
            &self.planning,
        )?;
        filtered.open(ctx).await?;
        let mut batches = Vec::new();
        let mut bytes = 0;
        while let Some(batch) = filtered.next_batch(ctx).await? {
            bytes += charge(ctx, batch.len(), schema.len())?;
            batches.push(batch);
        }
        filtered.close();
        ctx.release(joined.bytes);
        Ok(Charged {
            batch: concat(schema, batches)?,
            bytes,
        })
    }

    /// The next batch of quad rows paired with parent rows, or `None` once
    /// every pair has been emitted.
    fn next_paired(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        let Some(out) = self.quad.as_mut() else {
            return Ok(None);
        };
        ctx.check_cancelled()?;
        let limit = ctx.batch_size.max(1);
        let mut columns: Vec<Vec<Binding>> = vec![Vec::with_capacity(limit); out.schema.len()];
        let mut produced = 0;
        while produced < limit {
            let (b, p, q) = out.cursor;
            let Some(parent) = out.parents.get(b) else {
                break;
            };
            if p >= parent.len() {
                out.cursor = (b + 1, 0, 0);
                continue;
            }
            if q >= out.rows.batch.len() {
                out.cursor = (b, p + 1, 0);
                continue;
            }
            for (col, var) in out.schema.iter().enumerate() {
                let binding = parent
                    .get(p, *var)
                    .or_else(|| out.rows.batch.get(q, *var))
                    .cloned()
                    .unwrap_or(Binding::Unbound);
                columns[col].push(binding);
            }
            out.cursor = (b, p, q + 1);
            produced += 1;
        }
        if produced == 0 {
            self.quad = None;
            return Ok(None);
        }
        Ok(Some(Batch::new(Arc::clone(&out.schema), columns)?))
    }
}

/// One batch holding every row of `batches`.
fn concat(schema: Arc<[VarId]>, batches: Vec<Batch>) -> Result<Batch> {
    let mut columns: Vec<Vec<Binding>> = vec![Vec::new(); schema.len()];
    for batch in batches {
        for (col, var) in schema.iter().enumerate() {
            if let Some(values) = batch.column(*var) {
                columns[col].extend_from_slice(values);
            }
        }
    }
    if schema.is_empty() {
        return Ok(Batch::empty_schema_with_len(0));
    }
    Ok(Batch::new(schema, columns)?)
}

/// Inner join of `left` and `right` on every variable they share (a quad
/// scan's schema always holds the graph variable, so rows only ever join
/// within one graph). Builds on `left`, probes with `right`.
fn hash_join(
    ctx: &ExecutionContext<'_>,
    graph_var: VarId,
    left: &Batch,
    right: &Batch,
) -> Result<Charged> {
    use crate::hash_join::inner_join_key;
    let shared: Vec<VarId> = left
        .schema()
        .iter()
        .copied()
        .filter(|v| right.schema().contains(v))
        .collect();
    let dicts = crate::object_binding::TermDicts::of(ctx);
    let gv = ctx.graph_view();
    let key = |batch: &Batch, row: usize| -> Option<Vec<crate::hash_join::JoinKey>> {
        shared
            .iter()
            .map(|v| match batch.get(row, *v)? {
                // Every quad scan binds a graph through the one `Arc` its
                // `QuadGraphs` holds, so the pointer names the graph.
                Binding::Iri(iri) if *v == graph_var => Some(crate::hash_join::JoinKey::Ref(
                    Arc::as_ptr(iri).cast::<u8>() as usize as u64,
                )),
                binding => inner_join_key(binding, dicts, gv.as_ref()),
            })
            .collect()
    };
    let table_bytes = charge(ctx, left.len(), shared.len() + 1)?;
    let mut table: rustc_hash::FxHashMap<Vec<crate::hash_join::JoinKey>, Vec<usize>> =
        rustc_hash::FxHashMap::default();
    for row in 0..left.len() {
        if let Some(k) = key(left, row) {
            table.entry(k).or_default().push(row);
        }
    }
    let mut schema: Vec<VarId> = left.schema().to_vec();
    schema.extend(right.schema().iter().filter(|v| !shared.contains(v)));
    let schema: Arc<[VarId]> = schema.into();
    let mut columns: Vec<Vec<Binding>> = vec![Vec::new(); schema.len()];
    let (mut bytes, mut uncharged) = (0, 0);
    for row in 0..right.len() {
        ctx.check_cancelled()?;
        let Some(matches) = key(right, row).and_then(|k| table.get(&k)) else {
            continue;
        };
        for &l in matches {
            for (col, var) in schema.iter().enumerate() {
                let binding = left
                    .get(l, *var)
                    .or_else(|| right.get(row, *var))
                    .cloned()
                    .unwrap_or(Binding::Unbound);
                columns[col].push(binding);
            }
        }
        uncharged += matches.len();
        if uncharged >= CHARGE_ROWS {
            bytes += charge(ctx, uncharged, schema.len())?;
            uncharged = 0;
        }
    }
    bytes += charge(ctx, uncharged, schema.len())?;
    ctx.release(table_bytes);
    Ok(Charged {
        batch: Batch::new(schema, columns)?,
        bytes,
    })
}

#[async_trait]
impl Operator for QuadBlockOperator {
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        self.graph.plan_children()
    }

    fn schema(&self) -> &[VarId] {
        self.graph.schema()
    }

    fn set_row_budget(&mut self, budget: usize) {
        self.row_budget = Some(budget);
        self.graph.set_row_budget(budget);
    }

    fn set_topk(&mut self, ordering: &[SortSpec], k: usize) {
        self.graph.set_topk(ordering, k);
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.quad = None;
        self.state = OperatorState::Open;
        let graphs = match self.quad_graphs(ctx) {
            Some(graphs) if self.row_budget.is_none() => graphs,
            _ => return self.graph.open(ctx).await,
        };
        // When one pattern already rules most graphs out, visiting the few
        // left one by one beats scanning every pattern across the pool.
        if let Some(hits) = self.graph.probe_hit_count(ctx).await? {
            if hits.saturating_mul(SELECTIVE_PROBE_FACTOR) < graphs.len() {
                return self.graph.open(ctx).await;
            }
        }
        // The parent rows pair with the quad results and seed the per-graph
        // run of the remaining graphs, so collect them first.
        let mut child = self
            .graph
            .replace_child(Box::new(crate::seed::EmptyOperator::new()));
        let parent_schema: Arc<[VarId]> = Arc::from(child.schema());
        child.open(ctx).await?;
        let mut parents = Vec::new();
        let mut parent_bytes = 0;
        while let Some(batch) = child.next_batch(ctx).await? {
            if !batch.is_empty() {
                parent_bytes += charge(ctx, batch.len(), parent_schema.len())?;
                parents.push(batch);
            }
        }
        child.close();

        if !parents.is_empty() {
            let shape = self.shape.as_ref().expect("quad graphs imply a quad shape");
            tracing::debug!(graphs = graphs.len(), "{QUAD_BLOCK_EVENT}");
            let rows = self.run_quad(ctx, shape, &graphs).await?;
            self.quad = Some(QuadOutput {
                cancellation: ctx.cancellation.clone(),
                schema: Arc::from(self.graph.schema()),
                parents: parents.clone(),
                rows,
                parent_bytes,
                cursor: (0, 0, 0),
            });
        }
        self.graph
            .replace_child(Box::new(BatchReplayOperator::new(parent_schema, parents)));
        self.graph.skip_graphs(graphs);
        self.graph.open(ctx).await
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }
        if let Some(batch) = self.next_paired(ctx)? {
            return Ok(Some(batch));
        }
        self.graph.next_batch(ctx).await
    }

    fn close(&mut self) {
        self.quad = None;
        self.graph.close();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.graph.estimated_rows()
    }
}
