//! GRAPH pattern operator - scopes inner patterns to a specific graph
//!
//! Implements SPARQL GRAPH semantics:
//! - `GRAPH <iri> { ... }`: Execute inner patterns against a specific named graph
//! - `GRAPH ?g { ... }`: If ?g is bound, use that graph; if unbound, iterate all named graphs
//!
//! Key semantics:
//! - GraphOperator is a **correlated operator** (like EXISTS/Subquery)
//! - For each parent row, inner patterns are executed in the appropriate graph context
//! - ?g is bound as `Binding::Lit { val: FlakeValue::String(...), dtc: Explicit(xsd:string) }`
//! - Graph-not-found produces empty result (not an error)
//!
//! # Single-DB Mode
//!
//! When there is no dataset (single-db mode), the db's alias acts as its "graph name":
//! - `GRAPH <iri> { ... }`: Only executes if `iri == db.alias`; else empty result
//! - `GRAPH ?g { ... }` with ?g bound: Only executes if bound value == db.alias
//! - `GRAPH ?g { ... }` with ?g unbound: Binds ?g to db.alias and executes
//!
//! # Architecture
//!
//! GraphOperator:
//! 1. Receives input solutions from child operator
//! 2. For each input row, determines which graph(s) to query
//! 3. Switches active graph in ExecutionContext via `with_active_graph()`
//! 4. Executes inner patterns seeded with parent row bindings
//! 5. Merges results with parent row (like SubqueryOperator)

use crate::binding::{Batch, Binding};
use crate::context::{ExecutionContext, WellKnownDatatypes};
use crate::error::Result;
use crate::execute::build_where_operators_seeded;
use crate::ir::{GraphName, Pattern};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::r2rml::rewrite_patterns_for_r2rml;
use crate::seed::SeedOperator;
use crate::temporal_mode::PlanningContext;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::DatatypeConstraint;
use fluree_db_core::FlakeValue;
use std::sync::Arc;
// Note: tracing::debug removed to fix compilation - add tracing dependency if needed

/// GRAPH pattern operator - scopes inner patterns to a specific graph
///
/// This is a correlated operator: for each input row, it executes the inner
/// patterns in the appropriate graph context (determined by the graph name).
pub struct GraphOperator {
    /// Child operator providing input solutions
    child: BoxedOperator,
    /// Graph name (IRI or variable)
    graph_name: GraphName,
    /// Inner patterns to execute within the graph context
    inner_patterns: Vec<Pattern>,
    /// Well-known datatypes for binding ?g as xsd:string
    well_known: WellKnownDatatypes,
    /// Output schema (parent schema + any new vars from inner patterns)
    schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
    /// Buffered output rows (row-based, like SubqueryOperator)
    result_buffer: Vec<Vec<Binding>>,
    /// Current position in result buffer
    buffer_pos: usize,
    /// Planning context captured at planner-time for the per-row inner subplan.
    planning: PlanningContext,
}

impl GraphOperator {
    /// Create a new GRAPH pattern operator
    ///
    /// # Arguments
    ///
    /// * `child` - Input solutions operator
    /// * `graph_name` - The graph name (concrete IRI or variable)
    /// * `inner_patterns` - Patterns to execute within the graph context
    pub fn new(
        child: BoxedOperator,
        graph_name: GraphName,
        inner_patterns: Vec<Pattern>,
        planning: PlanningContext,
    ) -> Self {
        // Compute output schema: parent schema + new vars from inner patterns
        let parent_schema: std::collections::HashSet<VarId> =
            child.schema().iter().copied().collect();

        let mut inner_vars: std::collections::HashSet<VarId> = std::collections::HashSet::new();
        for p in &inner_patterns {
            inner_vars.extend(p.produced_vars());
        }

        // If graph_name is a variable, it may be bound by this operator
        if let GraphName::Var(var) = &graph_name {
            inner_vars.insert(*var);
        }

        // New vars are inner vars not in parent schema
        let new_vars: Vec<VarId> = inner_vars
            .iter()
            .copied()
            .filter(|v| !parent_schema.contains(v))
            .collect();

        // Output schema = parent schema + new vars
        let mut schema_vec: Vec<VarId> = child.schema().to_vec();
        schema_vec.extend(&new_vars);
        let schema = Arc::from(schema_vec.into_boxed_slice());

        Self {
            child,
            graph_name,
            inner_patterns,
            well_known: WellKnownDatatypes::new(),
            schema,
            state: OperatorState::Created,
            result_buffer: Vec::new(),
            buffer_pos: 0,
            planning,
        }
    }

    /// Extract graph IRI from a binding (for "?g already bound" check)
    ///
    /// Accepts `Binding::Lit { val: FlakeValue::String(s), .. }` and returns the string.
    fn extract_graph_iri_from_binding(binding: &Binding) -> Option<Arc<str>> {
        match binding {
            Binding::Lit {
                val: FlakeValue::String(s),
                ..
            } => Some(Arc::from(s.as_str())),
            // Could extend to handle Sid-based IRIs if needed
            _ => None,
        }
    }

    /// Execute inner patterns in a specific graph, seeded with parent row
    async fn execute_in_graph(
        &mut self,
        ctx: &ExecutionContext<'_>,
        parent_batch: &Batch,
        row_idx: usize,
        graph_iri: Arc<str>,
        bind_graph_var: Option<VarId>,
    ) -> Result<()> {
        // Switch to the named graph context
        let graph_ctx = ctx.with_active_graph(graph_iri.clone());

        // Check if this graph is backed by an R2RML mapping.
        // Prefer the precomputed set (populated in runner.rs for dataset queries),
        // but fall back to asking the provider dynamically for the no-dataset
        // single-source path where the GRAPH IRI may differ from the ledger_id.
        let is_r2rml_gs = if ctx.r2rml_graph_ids.contains(graph_iri.as_ref()) {
            true
        } else if let Some(provider) = ctx.r2rml_provider {
            provider.has_r2rml_mapping(&graph_iri).await
        } else {
            false
        };

        // Determine which patterns to use (rewritten for R2RML or original)
        let patterns_to_execute: std::borrow::Cow<'_, [Pattern]> = if is_r2rml_gs {
            // Rewrite triple patterns to R2RML patterns
            let rewrite_result =
                rewrite_patterns_for_r2rml(&self.inner_patterns, &graph_iri, ctx.active_snapshot);

            // If there are unconverted patterns in an R2RML graph source, return an error.
            // R2RML graph sources don't have ledger-backed indexes, so unconverted patterns
            // (e.g., bound subject or bound object constraints) would silently return empty
            // results instead of the expected matches. Fail explicitly so users know their
            // query contains unsupported patterns.
            if rewrite_result.unconverted_count > 0 {
                return Err(crate::error::QueryError::InvalidQuery(format!(
                    "R2RML graph source '{}' contains {} pattern(s) that cannot be converted \
                     to R2RML scans. Patterns with bound subjects (e.g., <iri> ex:name ?o) or \
                     bound objects (e.g., ?s ex:name \"value\") are not yet supported in R2RML \
                     graph sources.",
                    graph_iri, rewrite_result.unconverted_count
                )));
            }

            std::borrow::Cow::Owned(rewrite_result.patterns)
        } else {
            std::borrow::Cow::Borrowed(&self.inner_patterns)
        };

        // Build seed operator from parent row (like EXISTS/Subquery)
        let seed = SeedOperator::from_batch_row(parent_batch, row_idx);
        let mut inner = build_where_operators_seeded(
            Some(Box::new(seed)),
            &patterns_to_execute,
            None,
            None,
            &self.planning,
        )?;

        inner.open(&graph_ctx).await?;

        while let Some(batch) = inner.next_batch(&graph_ctx).await? {
            // Merge each inner result with parent row
            for inner_row_idx in 0..batch.len() {
                let mut merged_row = Vec::with_capacity(self.schema.len());

                // Copy parent bindings first
                for var in self.child.schema() {
                    let binding = parent_batch
                        .get(row_idx, *var)
                        .cloned()
                        .unwrap_or(Binding::Unbound);
                    merged_row.push(binding);
                }

                // Append new variables from inner patterns
                let parent_len = self.child.schema().len();
                for (_i, var) in self.schema.iter().enumerate().skip(parent_len) {
                    // Check if this is the graph variable we need to bind
                    if bind_graph_var == Some(*var) {
                        // Bind ?g to graph IRI using xsd:string
                        let binding = Binding::Lit {
                            val: FlakeValue::String(graph_iri.to_string()),
                            dtc: DatatypeConstraint::Explicit(self.well_known.xsd_string.clone()),
                            t: None,
                            op: None,
                            p_id: None,
                        };
                        merged_row.push(binding);
                    } else {
                        // Get from inner batch
                        let binding = batch
                            .get(inner_row_idx, *var)
                            .cloned()
                            .unwrap_or(Binding::Unbound);
                        merged_row.push(binding);
                    }
                }

                self.result_buffer.push(merged_row);
            }
        }

        inner.close();
        Ok(())
    }

    /// Drain buffered results into a batch
    fn drain_buffer(&mut self) -> Result<Option<Batch>> {
        if self.buffer_pos >= self.result_buffer.len() {
            return Ok(None);
        }

        // Build batch from buffer
        let num_cols = self.schema.len();
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
impl Operator for GraphOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        self.result_buffer.clear();
        self.buffer_pos = 0;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        // Return buffered results first
        if self.buffer_pos < self.result_buffer.len() {
            return self.drain_buffer();
        }

        // Clone graph_name to avoid borrow conflicts when calling execute_in_graph
        let graph_name = self.graph_name.clone();

        loop {
            // Get next batch from child
            let parent_batch = match self.child.next_batch(ctx).await? {
                Some(b) if !b.is_empty() => b,
                Some(_) => continue, // Skip empty batches
                None => {
                    self.state = OperatorState::Exhausted;
                    return Ok(None);
                }
            };

            // Clear buffer for new parent batch
            self.result_buffer.clear();
            self.buffer_pos = 0;

            // Process each parent row
            for row_idx in 0..parent_batch.len() {
                match &graph_name {
                    GraphName::Iri(iri) => {
                        // Concrete graph: run inner patterns in that graph
                        // If graph doesn't exist in dataset → empty result
                        if let Some(ds) = &ctx.dataset {
                            if ds.has_named_graph(iri) {
                                self.execute_in_graph(
                                    ctx,
                                    &parent_batch,
                                    row_idx,
                                    iri.clone(),
                                    None,
                                )
                                .await?;
                            }
                            // else: graph not found → no output for this row
                        } else {
                            // No dataset — check R2RML precomputed set, then provider fallback
                            let is_r2rml_gs = if ctx.r2rml_graph_ids.contains(iri.as_ref()) {
                                true
                            } else if let Some(provider) = ctx.r2rml_provider {
                                provider.has_r2rml_mapping(iri).await
                            } else {
                                false
                            };

                            // Execute if R2RML graph source or if graph name matches db's alias
                            if is_r2rml_gs || iri.as_ref() == ctx.active_snapshot.ledger_id {
                                self.execute_in_graph(
                                    ctx,
                                    &parent_batch,
                                    row_idx,
                                    iri.clone(),
                                    None,
                                )
                                .await?;
                            }
                            // else: graph name doesn't match alias and not R2RML graph source → no output
                        }
                    }
                    GraphName::Var(var) => {
                        // Check if ?g is already bound in parent row
                        if let Some(binding) = parent_batch.get(row_idx, *var) {
                            if let Some(bound_iri) = Self::extract_graph_iri_from_binding(binding) {
                                // ?g already bound: use only that graph
                                if let Some(ds) = &ctx.dataset {
                                    if ds.has_named_graph(&bound_iri) {
                                        self.execute_in_graph(
                                            ctx,
                                            &parent_batch,
                                            row_idx,
                                            bound_iri,
                                            None, // Don't rebind - already bound
                                        )
                                        .await?;
                                    }
                                    // else: graph not found → no output
                                } else {
                                    // No dataset — check R2RML precomputed set, then provider fallback
                                    let is_r2rml_gs =
                                        if ctx.r2rml_graph_ids.contains(bound_iri.as_ref()) {
                                            true
                                        } else if let Some(provider) = ctx.r2rml_provider {
                                            provider.has_r2rml_mapping(&bound_iri).await
                                        } else {
                                            false
                                        };

                                    // Execute if R2RML graph source or alias match
                                    if is_r2rml_gs
                                        || bound_iri.as_ref() == ctx.active_snapshot.ledger_id
                                    {
                                        self.execute_in_graph(
                                            ctx,
                                            &parent_batch,
                                            row_idx,
                                            bound_iri,
                                            None,
                                        )
                                        .await?;
                                    }
                                    // else: bound IRI doesn't match alias and not R2RML graph source → no output
                                }
                            }
                            // else: binding exists but isn't a string IRI → no output
                        } else {
                            // ?g unbound: iterate ALL named graphs, bind ?g
                            if let Some(ds) = &ctx.dataset {
                                for iri in ds.named_graph_iris() {
                                    self.execute_in_graph(
                                        ctx,
                                        &parent_batch,
                                        row_idx,
                                        iri,
                                        Some(*var), // Bind ?g to graph IRI
                                    )
                                    .await?;
                                }
                            } else {
                                // No dataset - single-db mode:
                                // Bind ?g to db's alias and execute
                                let alias_iri: Arc<str> =
                                    Arc::from(ctx.active_snapshot.ledger_id.as_str());
                                self.execute_in_graph(
                                    ctx,
                                    &parent_batch,
                                    row_idx,
                                    alias_iri,
                                    Some(*var), // Bind ?g to db alias
                                )
                                .await?;
                            }
                        }
                    }
                }
            }

            // If we produced any results, return them
            if !self.result_buffer.is_empty() {
                return self.drain_buffer();
            }
            // Otherwise, try next parent batch
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.result_buffer.clear();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // GRAPH patterns can multiply or filter rows; hard to estimate
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::triple::{Ref, Term, TriplePattern};
    use fluree_db_core::Sid;

    // Helper test struct for creating operators with specific schemas
    struct TestChildOperator {
        schema: Arc<[VarId]>,
    }

    #[async_trait]
    impl Operator for TestChildOperator {
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

    #[test]
    fn test_graph_operator_schema_with_iri() {
        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let child: BoxedOperator = Box::new(TestChildOperator {
            schema: child_schema.clone(),
        });

        let patterns = vec![Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(Sid::new(100, "age")),
            Term::Var(VarId(2)), // New variable
        ))];

        let op = GraphOperator::new(
            child,
            GraphName::Iri(Arc::from("http://example.org/graph1")),
            patterns,
            crate::temporal_mode::PlanningContext::current(),
        );

        // Output schema should include parent vars + new var from pattern
        assert!(op.schema().contains(&VarId(0)));
        assert!(op.schema().contains(&VarId(1)));
        assert!(op.schema().contains(&VarId(2)));
    }

    #[test]
    fn test_graph_operator_schema_with_var() {
        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let child: BoxedOperator = Box::new(TestChildOperator {
            schema: child_schema,
        });

        let patterns = vec![Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(Sid::new(100, "name")),
            Term::Var(VarId(1)),
        ))];

        // Graph variable ?g = VarId(2)
        let op = GraphOperator::new(
            child,
            GraphName::Var(VarId(2)),
            patterns,
            crate::temporal_mode::PlanningContext::current(),
        );

        // Output schema should include parent var, new var from pattern, and graph var
        assert!(op.schema().contains(&VarId(0)));
        assert!(op.schema().contains(&VarId(1)));
        assert!(op.schema().contains(&VarId(2))); // Graph variable
    }

    #[test]
    fn test_extract_graph_iri_from_binding() {
        // Valid string binding
        let binding = Binding::Lit {
            val: FlakeValue::String("http://example.org/graph1".to_string()),
            dtc: DatatypeConstraint::Explicit(Sid::new(2, "string")),
            t: None,
            op: None,
            p_id: None,
        };
        let iri = GraphOperator::extract_graph_iri_from_binding(&binding);
        assert_eq!(iri, Some(Arc::from("http://example.org/graph1")));

        // Non-string binding returns None
        let binding = Binding::Lit {
            val: FlakeValue::Long(42),
            dtc: DatatypeConstraint::Explicit(Sid::new(2, "long")),
            t: None,
            op: None,
            p_id: None,
        };
        let iri = GraphOperator::extract_graph_iri_from_binding(&binding);
        assert_eq!(iri, None);

        // Unbound returns None
        let iri = GraphOperator::extract_graph_iri_from_binding(&Binding::Unbound);
        assert_eq!(iri, None);
    }
}
