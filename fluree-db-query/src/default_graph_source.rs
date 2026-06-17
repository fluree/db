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
use crate::ir::Pattern;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::seed::{EmptyOperator, SeedOperator};
use crate::temporal_mode::PlanningContext;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::StatsView;
use std::sync::Arc;

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
}

impl DefaultGraphSourceOperator {
    pub fn new(
        child: BoxedOperator,
        inner_patterns: Vec<Pattern>,
        planning: PlanningContext,
        stats: Option<Arc<StatsView>>,
    ) -> Self {
        let parent_schema: std::collections::HashSet<VarId> =
            child.schema().iter().copied().collect();

        let mut inner_vars: std::collections::HashSet<VarId> = std::collections::HashSet::new();
        for p in &inner_patterns {
            inner_vars.extend(p.produced_vars());
        }

        let new_vars: Vec<VarId> = inner_vars
            .iter()
            .copied()
            .filter(|v| !parent_schema.contains(v))
            .collect();

        let mut schema_vec: Vec<VarId> = child.schema().to_vec();
        schema_vec.extend(&new_vars);
        let schema = Arc::from(schema_vec.into_boxed_slice());

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
        // Single default-graph (no dataset): the per-source correlation this
        // wrapper exists for is a no-op, so build the inner subplan ONCE seeded
        // by the whole child stream. The base edge + f:reifies* triples then plan
        // as one normal join block — the base edge can hash-join the child — and
        // stream directly, instead of replanning and re-executing per parent row
        // (which made an annotated object-join O(parent rows): IC5's 65s cliff).
        // Multi-source datasets keep the per-row, per-source path below.
        if ctx.dataset.is_none() {
            let child = std::mem::replace(&mut self.child, Box::new(EmptyOperator::new()));
            let mut delegate = build_where_operators_seeded(
                Some(child),
                &self.inner_patterns,
                self.stats.clone(),
                None,
                &self.planning,
            )?;
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
        if let Some(delegate) = self.single_graph_delegate.as_mut() {
            return delegate.next_batch(ctx).await;
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
        None
    }
}
