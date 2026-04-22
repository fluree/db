//! UNION operator - executes branches with correlated input
//!
//! Implements SPARQL UNION semantics:
//! - For each input row, execute each branch with that row as a seed
//! - Concatenate results from all branches
//! - Normalize output batches to a unified schema (padding missing vars with Unbound)
//!
//! Correlation is essential: each branch must see the bindings from the current
//! input solution (row).

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::execute::build_where_operators_seeded;
use crate::ir::Pattern;
use crate::operator::{compute_trimmed_vars, BoxedOperator, Operator, OperatorState};
use crate::seed::SeedOperator;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::StatsView;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;

/// UNION operator - executes branches for each input row (correlated).
pub struct UnionOperator {
    /// Child operator providing input solutions
    child: BoxedOperator,
    /// Branch patterns (each branch is its own pattern list)
    branches: Vec<Vec<Pattern>>,
    /// Unified schema across child + all branch patterns
    unified_schema: Arc<[VarId]>,
    /// Effective output schema (trimmed if `downstream_vars` is set, otherwise same as `schema`)
    effective_schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
    /// Buffered output batches produced from processing input rows
    output_buffer: VecDeque<Batch>,
    /// Current input batch being processed
    current_input_batch: Option<Batch>,
    /// Current row index in the input batch
    current_input_row: usize,
    /// True once the child is exhausted; pending output may still need draining.
    input_exhausted: bool,
    /// Optional stats for selectivity-based pattern reordering in branches
    stats: Option<Arc<StatsView>>,
    /// Debug counters for low-noise batch fragmentation summaries.
    input_batches_seen: usize,
    input_rows_seen: usize,
    branch_execs: usize,
    output_batches_buffered: usize,
    output_rows_buffered: usize,
    max_input_batch_len: usize,
    max_output_batch_len: usize,
    output_batches_emitted: usize,
    output_rows_emitted: usize,
    max_emitted_batch_len: usize,
    pending_output_rows: usize,
}

impl UnionOperator {
    /// Create a new correlated UNION operator.
    ///
    /// # Arguments
    ///
    /// * `child` - Input solutions operator
    /// * `branches` - Branch pattern lists (at least one required)
    /// * `stats` - Optional stats for selectivity-based pattern reordering in branches
    pub fn new(
        child: BoxedOperator,
        branches: Vec<Vec<Pattern>>,
        stats: Option<Arc<StatsView>>,
    ) -> Self {
        assert!(!branches.is_empty(), "UNION requires at least one branch");

        // Build unified schema: start with child schema (preserve order),
        // then add any vars referenced/introduced in branch patterns.
        let mut unified_vars: Vec<VarId> = child.schema().to_vec();
        let mut seen: HashSet<VarId> = unified_vars.iter().copied().collect();

        for branch in &branches {
            extend_schema_from_patterns(&mut unified_vars, &mut seen, branch);
        }

        let unified_schema: Arc<[VarId]> = Arc::from(unified_vars.into_boxed_slice());
        let effective_schema = unified_schema.clone();

        Self {
            child,
            branches,
            unified_schema,
            effective_schema,
            state: OperatorState::Created,
            output_buffer: VecDeque::new(),
            current_input_batch: None,
            current_input_row: 0,
            input_exhausted: false,
            stats,
            input_batches_seen: 0,
            input_rows_seen: 0,
            branch_execs: 0,
            output_batches_buffered: 0,
            output_rows_buffered: 0,
            max_input_batch_len: 0,
            max_output_batch_len: 0,
            output_batches_emitted: 0,
            output_rows_emitted: 0,
            max_emitted_batch_len: 0,
            pending_output_rows: 0,
        }
    }

    /// Trim the output schema to only the required downstream variables.
    ///
    /// Variables not in `downstream_vars` are excluded from the output schema,
    /// avoiding unnecessary Unbound padding in `normalize_batch` and carrying
    /// fewer columns through the rest of the pipeline.
    pub fn with_out_schema(mut self, downstream_vars: Option<&[VarId]>) -> Self {
        if let Some(trimmed) = compute_trimmed_vars(&self.unified_schema, downstream_vars) {
            self.effective_schema = trimmed;
        }
        self
    }

    /// Normalize a batch to the effective schema (pad missing vars with Unbound).
    fn normalize_batch(&self, batch: Batch) -> Result<Batch> {
        if batch.is_empty() {
            return Ok(Batch::empty(self.effective_schema.clone())?);
        }

        // Map each effective output var to its source column (if present) or Unbound padding
        let columns: Vec<Vec<Binding>> = self
            .effective_schema
            .iter()
            .map(|&var| {
                batch
                    .schema()
                    .iter()
                    .position(|&v| v == var)
                    .and_then(|src_idx| batch.column_by_idx(src_idx))
                    .map(<[Binding]>::to_vec)
                    .unwrap_or_else(|| vec![Binding::Unbound; batch.len()])
            })
            .collect();

        Ok(Batch::new(self.effective_schema.clone(), columns)?)
    }

    fn take_output_batch(&mut self, batch_size: usize) -> Result<Option<Batch>> {
        if self.output_buffer.is_empty() {
            return Ok(None);
        }

        let mut output_columns: Vec<Vec<Binding>> = self
            .effective_schema
            .iter()
            .map(|_| Vec::with_capacity(batch_size))
            .collect();
        let mut rows_added = 0usize;

        while rows_added < batch_size {
            let Some(batch) = self.output_buffer.pop_front() else {
                break;
            };

            let (schema, mut columns, batch_len) = batch.into_parts();
            debug_assert_eq!(&*schema, &*self.effective_schema);

            let rows_to_take = (batch_size - rows_added).min(batch_len);
            for (dest, source) in output_columns.iter_mut().zip(columns.iter_mut()) {
                dest.extend(source.drain(..rows_to_take));
            }
            rows_added += rows_to_take;
            self.pending_output_rows -= rows_to_take;

            if rows_to_take < batch_len {
                let remainder = Batch::from_parts(schema, columns, batch_len - rows_to_take)?;
                self.output_buffer.push_front(remainder);
                break;
            }
        }

        if rows_added == 0 {
            return Ok(None);
        }

        let batch = Batch::from_parts(self.effective_schema.clone(), output_columns, rows_added)?;
        self.output_batches_emitted += 1;
        self.output_rows_emitted += batch.len();
        self.max_emitted_batch_len = self.max_emitted_batch_len.max(batch.len());
        Ok(Some(batch))
    }
}

#[async_trait]
impl Operator for UnionOperator {
    fn schema(&self) -> &[VarId] {
        &self.effective_schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        self.output_buffer.clear();
        self.current_input_batch = None;
        self.current_input_row = 0;
        self.input_exhausted = false;
        self.input_batches_seen = 0;
        self.input_rows_seen = 0;
        self.branch_execs = 0;
        self.output_batches_buffered = 0;
        self.output_rows_buffered = 0;
        self.max_input_batch_len = 0;
        self.max_output_batch_len = 0;
        self.output_batches_emitted = 0;
        self.output_rows_emitted = 0;
        self.max_emitted_batch_len = 0;
        self.pending_output_rows = 0;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        loop {
            if self.pending_output_rows >= ctx.batch_size
                || (self.input_exhausted && self.pending_output_rows > 0)
            {
                let batch = self
                    .take_output_batch(ctx.batch_size)?
                    .expect("pending_output_rows tracks buffered union rows");
                return Ok(Some(batch));
            }

            if self.input_exhausted {
                self.state = OperatorState::Exhausted;
                return Ok(None);
            }

            // Ensure we have an input batch to process.
            if self.current_input_batch.is_none()
                || self.current_input_row
                    >= self
                        .current_input_batch
                        .as_ref()
                        .map(super::binding::Batch::len)
                        .unwrap_or(0)
            {
                // Fetch next non-empty batch from child.
                let next = match self.child.next_batch(ctx).await? {
                    Some(b) if !b.is_empty() => b,
                    Some(_) => continue,
                    None => {
                        self.input_exhausted = true;
                        continue;
                    }
                };
                self.input_batches_seen += 1;
                self.max_input_batch_len = self.max_input_batch_len.max(next.len());
                self.current_input_batch = Some(next);
                self.current_input_row = 0;
            }

            // Process one input row: execute all branches with this row as seed.
            let input_batch = self.current_input_batch.as_ref().unwrap().clone();
            let row_idx = self.current_input_row;
            self.current_input_row += 1;
            self.input_rows_seen += 1;

            // Pass effective schema as required vars so branches trim internally
            let branch_downstream_vars: Option<&[VarId]> =
                if self.effective_schema.len() < self.unified_schema.len() {
                    Some(&self.effective_schema)
                } else {
                    None
                };

            for branch_patterns in &self.branches {
                self.branch_execs += 1;
                let seed = SeedOperator::from_batch_row(&input_batch, row_idx);
                let mut branch_op = build_where_operators_seeded(
                    Some(Box::new(seed)),
                    branch_patterns,
                    self.stats.clone(),
                    branch_downstream_vars,
                )?;

                branch_op.open(ctx).await?;
                while let Some(batch) = branch_op.next_batch(ctx).await? {
                    if batch.is_empty() {
                        continue;
                    }
                    let normalized = self.normalize_batch(batch)?;
                    self.output_batches_buffered += 1;
                    self.output_rows_buffered += normalized.len();
                    self.max_output_batch_len = self.max_output_batch_len.max(normalized.len());
                    self.pending_output_rows += normalized.len();
                    self.output_buffer.push_back(normalized);
                }
                branch_op.close();
            }
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.output_buffer.clear();
        self.state = OperatorState::Closed;
        self.pending_output_rows = 0;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Upper bound: child rows * number of branches.
        self.child
            .estimated_rows()
            .map(|r| r.saturating_mul(self.branches.len()))
    }
}

fn extend_schema_from_patterns(
    schema: &mut Vec<VarId>,
    seen: &mut HashSet<VarId>,
    patterns: &[Pattern],
) {
    for p in patterns {
        match p {
            Pattern::Triple(tp) => {
                for v in tp.variables() {
                    if seen.insert(v) {
                        schema.push(v);
                    }
                }
            }
            Pattern::Filter(_) => {}
            Pattern::Optional(inner)
            | Pattern::Minus(inner)
            | Pattern::Exists(inner)
            | Pattern::NotExists(inner) => extend_schema_from_patterns(schema, seen, inner),
            Pattern::Union(branches) => {
                for b in branches {
                    extend_schema_from_patterns(schema, seen, b);
                }
            }
            Pattern::Bind { var, .. } => {
                if seen.insert(*var) {
                    schema.push(*var);
                }
            }
            Pattern::Values { vars, .. } => {
                for v in vars {
                    if seen.insert(*v) {
                        schema.push(*v);
                    }
                }
            }
            Pattern::PropertyPath(pp) => {
                for v in pp.variables() {
                    if seen.insert(v) {
                        schema.push(v);
                    }
                }
            }
            Pattern::Subquery(sq) => {
                // Subquery contributes its select variables to the schema
                for v in &sq.select {
                    if seen.insert(*v) {
                        schema.push(*v);
                    }
                }
            }
            Pattern::IndexSearch(isp) => {
                // Index search contributes id, score, and ledger variables to the schema
                for v in isp.variables() {
                    if seen.insert(v) {
                        schema.push(v);
                    }
                }
            }
            Pattern::VectorSearch(vsp) => {
                // Vector search contributes id, score, and ledger variables to the schema
                for v in vsp.variables() {
                    if seen.insert(v) {
                        schema.push(v);
                    }
                }
            }
            Pattern::R2rml(r2rml) => {
                // R2RML pattern contributes subject and object variables to the schema
                for v in r2rml.variables() {
                    if seen.insert(v) {
                        schema.push(v);
                    }
                }
            }
            Pattern::GeoSearch(gsp) => {
                for v in gsp.variables() {
                    if seen.insert(v) {
                        schema.push(v);
                    }
                }
            }
            Pattern::S2Search(s2p) => {
                for v in s2p.variables() {
                    if seen.insert(v) {
                        schema.push(v);
                    }
                }
            }
            Pattern::Graph {
                name,
                patterns: inner,
            } => {
                // Graph pattern contributes variables from inner patterns and the graph variable (if any)
                if let crate::ir::GraphName::Var(v) = name {
                    if seen.insert(*v) {
                        schema.push(*v);
                    }
                }
                extend_schema_from_patterns(schema, seen, inner);
            }
            Pattern::Service(sp) => {
                // Service pattern contributes variables from inner patterns and the endpoint variable (if any)
                if let crate::ir::ServiceEndpoint::Var(v) = &sp.endpoint {
                    if seen.insert(*v) {
                        schema.push(*v);
                    }
                }
                extend_schema_from_patterns(schema, seen, &sp.patterns);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::Binding;
    use crate::context::ExecutionContext;
    use crate::seed::EmptyOperator;
    use crate::values::ValuesOperator;
    use crate::var_registry::VarRegistry;
    use fluree_db_core::FlakeValue;
    use fluree_db_core::Sid;
    use std::sync::Arc;

    #[test]
    fn test_union_operator_schema_computation() {
        // Child schema has ?s, branches introduce ?n and ?e.
        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let child: BoxedOperator = Box::new(TestEmptyWithSchema {
            schema: child_schema,
        });

        let branches = vec![
            vec![Pattern::Triple(crate::triple::TriplePattern::new(
                crate::triple::Ref::Var(VarId(0)),
                crate::triple::Ref::Sid(Sid::new(100, "name")),
                crate::triple::Term::Var(VarId(1)),
            ))],
            vec![Pattern::Triple(crate::triple::TriplePattern::new(
                crate::triple::Ref::Var(VarId(0)),
                crate::triple::Ref::Sid(Sid::new(100, "email")),
                crate::triple::Term::Var(VarId(2)),
            ))],
        ];

        let op = UnionOperator::new(child, branches, None);
        assert_eq!(op.schema(), &[VarId(0), VarId(1), VarId(2)]);
    }

    #[test]
    fn test_union_operator_allows_position_0_via_empty_seed_child() {
        // UNION at position 0 should still be able to run using an EmptyOperator child.
        // Here we only validate it constructs; runtime behavior is covered by execute.rs integration tests.
        let empty = EmptyOperator::new();
        let child: BoxedOperator = Box::new(empty);
        let branches = vec![vec![], vec![]];
        let op = UnionOperator::new(child, branches, None);
        assert_eq!(op.schema().len(), 0);
    }

    #[test]
    fn test_union_with_out_schema_trims_schema() {
        // Unified schema: [?s(0), ?n(1), ?e(2)]
        // Required vars: [?s(0), ?e(2)]
        // Expected effective schema: [?s(0), ?e(2)] (preserves unified order)
        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let child: BoxedOperator = Box::new(TestEmptyWithSchema {
            schema: child_schema,
        });

        let branches = vec![
            vec![Pattern::Triple(crate::triple::TriplePattern::new(
                crate::triple::Ref::Var(VarId(0)),
                crate::triple::Ref::Sid(Sid::new(100, "name")),
                crate::triple::Term::Var(VarId(1)),
            ))],
            vec![Pattern::Triple(crate::triple::TriplePattern::new(
                crate::triple::Ref::Var(VarId(0)),
                crate::triple::Ref::Sid(Sid::new(100, "email")),
                crate::triple::Term::Var(VarId(2)),
            ))],
        ];

        let op =
            UnionOperator::new(child, branches, None).with_out_schema(Some(&[VarId(0), VarId(2)]));

        assert_eq!(op.schema(), &[VarId(0), VarId(2)]);
    }

    #[test]
    fn test_union_with_out_schema_none_preserves_full_schema() {
        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let child: BoxedOperator = Box::new(TestEmptyWithSchema {
            schema: child_schema,
        });

        let branches = vec![vec![Pattern::Triple(crate::triple::TriplePattern::new(
            crate::triple::Ref::Var(VarId(0)),
            crate::triple::Ref::Sid(Sid::new(100, "name")),
            crate::triple::Term::Var(VarId(1)),
        ))]];

        let op = UnionOperator::new(child, branches, None).with_out_schema(None);

        assert_eq!(op.schema(), &[VarId(0), VarId(1)]);
    }

    #[tokio::test]
    async fn test_union_coalesces_fragmented_branch_output() {
        let snapshot = fluree_db_core::LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let mut ctx = ExecutionContext::new(&snapshot, &vars);
        ctx.batch_size = 4;

        let child: BoxedOperator = Box::new(ValuesOperator::new(
            Box::new(EmptyOperator::new()),
            vec![VarId(0)],
            vec![
                vec![Binding::lit(FlakeValue::Long(1), Sid::new(2, "long"))],
                vec![Binding::lit(FlakeValue::Long(2), Sid::new(2, "long"))],
                vec![Binding::lit(FlakeValue::Long(3), Sid::new(2, "long"))],
            ],
        ));

        let branches = vec![
            vec![Pattern::Values {
                vars: vec![VarId(1)],
                rows: vec![
                    vec![Binding::lit(FlakeValue::Long(10), Sid::new(2, "long"))],
                    vec![Binding::lit(FlakeValue::Long(20), Sid::new(2, "long"))],
                ],
            }],
            vec![Pattern::Values {
                vars: vec![VarId(1)],
                rows: vec![vec![Binding::lit(
                    FlakeValue::Long(30),
                    Sid::new(2, "long"),
                )]],
            }],
        ];

        let mut op = UnionOperator::new(child, branches, None);
        op.open(&ctx).await.unwrap();

        let batch1 = op.next_batch(&ctx).await.unwrap().unwrap();
        let batch2 = op.next_batch(&ctx).await.unwrap().unwrap();
        let batch3 = op.next_batch(&ctx).await.unwrap().unwrap();
        let batch4 = op.next_batch(&ctx).await.unwrap();

        assert_eq!(batch1.len(), 4);
        assert_eq!(batch2.len(), 4);
        assert_eq!(batch3.len(), 1);
        assert!(batch4.is_none());
    }

    // Helper struct for testing
    struct TestEmptyWithSchema {
        schema: Arc<[VarId]>,
    }

    #[async_trait]
    impl Operator for TestEmptyWithSchema {
        fn schema(&self) -> &[VarId] {
            &self.schema
        }

        async fn open(&mut self, _ctx: &ExecutionContext<'_>) -> Result<()> {
            Ok(())
        }

        async fn next_batch(&mut self, _ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
            Ok(None)
        }

        fn close(&mut self) {}
    }
}
