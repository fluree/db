//! Default-graph-source operator — iterate the dataset's default graph
//! sources and run an inner subplan once per source.
//!
//! This is a planner-internal construct. The edge-annotation expansion
//! synthesizes [`Pattern::DefaultGraphSource`] around an annotated edge's
//! chain (its body and the reifier's `rdf:reifies` link) when the default
//! graph is a union of two or more graphs (`from: [g1, g2]`), so each source's
//! link correlates only with its own body — without the wrapper the lookups
//! fan across all sources via `DatasetOperator` and pair a link from one
//! source with an annotation body from another.
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
//! streams it directly.
//!
//! A union of two or more sources in current mode does the same: each
//! scan of the chain then deduplicates on fact identity across the
//! sources, so the base edge, the link and the body are each one fact
//! however many sources hold them, and the chain over the union answers
//! as over the merged graph, with no cross-product. Run per source, it
//! would answer each source's copy of an annotated edge. The per-row,
//! per-source path is left to unions whose scans keep every member's
//! rows (history mode).

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::dataset::ActiveGraphs;
use crate::dataset_operator::union_dedups_on_fact_identity;
use crate::error::Result;
use crate::execute::build_where_operators_seeded;
use crate::ir::Pattern;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::seed::{EmptyOperator, SeedOperator};
use crate::temporal_mode::PlanningContext;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::StatsView;
use std::collections::HashSet;
use std::sync::Arc;

pub struct DefaultGraphSourceOperator {
    child: BoxedOperator,
    inner_patterns: Vec<Pattern>,
    schema: Arc<[VarId]>,
    state: OperatorState,
    result_buffer: Vec<Vec<Binding>>,
    buffer_pos: usize,
    planning: PlanningContext,
    /// Planner stats for the inner subplan build, so its patterns are costed.
    stats: Option<Arc<StatsView>>,
    /// The inner subplan built ONCE, seeded by the whole child stream (the
    /// base edge can hash-join) and streamed directly, instead of replanned
    /// and re-executed per parent row and source. Built when no dataset is
    /// attached (one graph, nothing to correlate per source), and over a
    /// union whose scans deduplicate on fact identity (see [`Self::open`]).
    delegate: Option<BoxedOperator>,
    /// Planner estimate of the wrapped chain's output, with the child's
    /// variables bound — the same figure `reorder_patterns` placed the
    /// wrapper by. Reported through `estimated_rows` so the join above sees
    /// a real driving count instead of the `None` that read as one row.
    estimated: Option<usize>,
}

impl DefaultGraphSourceOperator {
    pub fn new(
        child: BoxedOperator,
        inner_patterns: Vec<Pattern>,
        planning: PlanningContext,
        stats: Option<Arc<StatsView>>,
    ) -> Self {
        let mut seen: HashSet<VarId> = child.schema().iter().copied().collect();

        // New vars in deterministic first-occurrence order across the inner
        // patterns. A `HashSet` here makes the output column order vary per
        // process (HashSet iteration is seeded randomly), and the inner
        // subplan emits batches in pattern order — the mismatch silently
        // dropped whole result batches ~half the time.
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
        let estimated = crate::planner::estimate_branch_cardinality_from(
            &inner_patterns,
            &bound,
            stats.as_deref(),
        );
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
            delegate: None,
            estimated,
        }
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
        // Single default graph (no dataset): the per-source correlation this
        // wrapper exists for is a no-op, so build the inner subplan ONCE seeded
        // by the whole child stream, and stream it, instead of replanning and
        // re-executing per parent row (which made an annotated object-join
        // O(parent rows): IC5's 65s cliff). A deduplicating union builds once
        // too; other datasets keep the per-row, per-source path below.
        //
        // A union whose every scan deduplicates on fact identity answers as
        // the merged graph: an edge and annotation two sources both hold are
        // one fact each, so the chain built once over the union finds them
        // once. Per source it would find each source's copy.
        let builds_once = ctx.dataset.is_none()
            || union_dedups_on_fact_identity(
                match ctx.active_graphs() {
                    ActiveGraphs::Many(graphs) => graphs.len(),
                    ActiveGraphs::Single => 1,
                },
                self.planning.mode(),
            );
        if builds_once {
            let child = std::mem::replace(&mut self.child, Box::new(EmptyOperator::new()));
            let mut delegate = build_where_operators_seeded(
                Some(child),
                &self.inner_patterns,
                self.stats.clone(),
                None,
                &self.planning,
            )?;
            delegate.open(ctx).await?;
            self.delegate = Some(delegate);
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

        // Stream the once-built inner subplan directly.
        // The delegate's batch column order comes from the REORDERED inner
        // chain, which need not match this
        // operator's declared schema — re-project so positional consumers
        // above (NestedLoopJoin bind instructions read by column index) see
        // the contract they were planned against. Matching layouts pass
        // through untouched.
        if let Some(delegate) = self.delegate.as_mut() {
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
        if let Some(delegate) = self.delegate.as_mut() {
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

    fn plan_details(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut m = serde_json::Map::new();
        m.insert("patterns".into(), self.inner_patterns.len().into());
        m
    }
}
