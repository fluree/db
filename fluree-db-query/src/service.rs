//! SERVICE pattern operator - executes patterns against another ledger
//!
//! Implements SPARQL SERVICE semantics for local Fluree ledgers:
//! - `SERVICE <fluree:ledger:name:branch> { ... }`: Execute against a specific ledger
//! - `SERVICE SILENT <...> { ... }`: Errors produce empty results instead of failure
//!
//! # Semantics
//!
//! For local Fluree ledger queries using `fluree:ledger:<name>` endpoints:
//! - The endpoint IRI identifies a ledger in the current dataset
//! - Inner patterns are executed against that ledger's view
//! - Results are joined with the outer query on shared variables
//!
//! # Architecture
//!
//! ServiceOperator is a correlated operator (like SubqueryOperator):
//! 1. Receives input solutions from child operator
//! 2. For each input row, determines the target ledger from the endpoint
//! 3. Looks up the ledger in the dataset
//! 4. Executes inner patterns seeded with parent row bindings
//! 5. Merges results with parent row

use crate::binding::{Batch, Binding};
use crate::context::{ExecutionContext, WellKnownDatatypes};
use crate::error::{QueryError, Result};
use crate::execute::build_where_operators_seeded;
use crate::ir::{ServiceEndpoint, ServicePattern};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::remote_service::{is_fluree_remote_endpoint, parse_fluree_remote_ref};
use crate::seed::SeedOperator;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::DatatypeConstraint;
use fluree_db_core::{format_ledger_id, split_ledger_id, FlakeValue};
use std::sync::Arc;

/// Fluree ledger SERVICE endpoint prefix
const FLUREE_LEDGER_PREFIX: &str = "fluree:ledger:";

/// SERVICE pattern operator - executes patterns against another ledger
///
/// This is a correlated operator: for each input row, it executes the inner
/// patterns against the appropriate ledger (determined by the endpoint).
pub struct ServiceOperator {
    /// Child operator providing input solutions
    child: BoxedOperator,
    /// SERVICE pattern (silent, endpoint, inner patterns)
    service: ServicePattern,
    /// Well-known datatypes for binding endpoint variable as xsd:string
    well_known: WellKnownDatatypes,
    /// Output schema (parent schema + any new vars from inner patterns)
    schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
    /// Buffered output rows
    result_buffer: Vec<Vec<Binding>>,
    /// Current position in result buffer
    buffer_pos: usize,
}

impl ServiceOperator {
    /// Create a new SERVICE pattern operator
    ///
    /// # Arguments
    ///
    /// * `child` - Input solutions operator
    /// * `service` - The SERVICE pattern (silent, endpoint, patterns)
    pub fn new(child: BoxedOperator, service: ServicePattern) -> Self {
        // Compute output schema: parent schema + new vars from inner patterns
        let parent_schema: std::collections::HashSet<VarId> =
            child.schema().iter().copied().collect();

        let mut inner_vars: std::collections::HashSet<VarId> = std::collections::HashSet::new();
        for p in &service.patterns {
            inner_vars.extend(p.variables());
        }

        // If endpoint is a variable, it may be bound by this operator
        if let ServiceEndpoint::Var(var) = &service.endpoint {
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
            service,
            well_known: WellKnownDatatypes::new(),
            schema,
            state: OperatorState::Created,
            result_buffer: Vec::new(),
            buffer_pos: 0,
        }
    }

    /// Parse a Fluree ledger reference from an endpoint IRI
    ///
    /// Format: `fluree:ledger:<name>` or `fluree:ledger:<name>:<branch>`
    ///
    /// Returns the full ledger ID string that matches dataset storage format.
    /// - `fluree:ledger:orders` → `"orders:main"` (defaults to :main branch)
    /// - `fluree:ledger:orders:main` → `"orders:main"`
    /// - `fluree:ledger:orders:dev` → `"orders:dev"`
    fn parse_fluree_ledger_ref(endpoint: &str) -> Option<String> {
        let rest = endpoint.strip_prefix(FLUREE_LEDGER_PREFIX)?;
        if rest.is_empty() {
            return None;
        }

        let (name, branch) = split_ledger_id(rest).ok()?;
        Some(format_ledger_id(&name, &branch))
    }

    /// Check if an endpoint is a local Fluree ledger reference
    fn is_fluree_ledger_endpoint(endpoint: &str) -> bool {
        endpoint.starts_with(FLUREE_LEDGER_PREFIX)
    }

    /// Extract endpoint IRI from a binding (for variable endpoint check)
    fn extract_endpoint_from_binding(binding: &Binding) -> Option<Arc<str>> {
        match binding {
            Binding::Lit {
                val: FlakeValue::String(s),
                ..
            } => Some(Arc::from(s.as_str())),
            _ => None,
        }
    }

    /// Execute inner patterns against a specific ledger
    ///
    /// The `full_ledger_ref` should be the complete ledger reference string
    /// that matches how datasets store ledger_id (e.g., "orders:main").
    async fn execute_against_ledger(
        &mut self,
        ctx: &ExecutionContext<'_>,
        parent_batch: &Batch,
        row_idx: usize,
        full_ledger_ref: &str,
        bind_endpoint_var: Option<VarId>,
        endpoint_iri: &str,
    ) -> Result<()> {
        // Look up the ledger in the dataset
        let graph_ref = if let Some(ds) = &ctx.dataset {
            ds.find_by_ledger_id(full_ledger_ref)
        } else {
            // No dataset - check if alias matches the current db
            if ctx.active_snapshot.ledger_id != full_ledger_ref {
                if self.service.silent {
                    // SILENT: return empty result for unknown ledgers
                    return Ok(());
                }
                return Err(QueryError::InvalidQuery(format!(
                    "SERVICE endpoint references unknown ledger '{full_ledger_ref}' (no dataset configured)"
                )));
            }
            None // Signal to use ctx directly (self-reference)
        };

        // Build seed operator from parent row (like EXISTS/Subquery)
        let seed = SeedOperator::from_batch_row(parent_batch, row_idx);
        let mut inner =
            build_where_operators_seeded(Some(Box::new(seed)), &self.service.patterns, None, None)?;

        // Create execution context for the target ledger
        // If graph_ref is Some, create a new context; otherwise use the current context (self-reference)
        let target_ctx;
        let ctx_to_use: &ExecutionContext<'_> = if let Some(gref) = graph_ref {
            target_ctx = ctx.with_graph_ref(gref);
            &target_ctx
        } else {
            ctx
        };

        match inner.open(ctx_to_use).await {
            Ok(()) => {}
            Err(e) if self.service.silent => {
                tracing::debug!(
                    error = %e,
                    "SERVICE SILENT: ignoring error from ledger '{}'",
                    full_ledger_ref
                );
                return Ok(());
            }
            Err(e) => return Err(e),
        }

        loop {
            let batch = match inner.next_batch(ctx_to_use).await {
                Ok(Some(b)) => b,
                Ok(None) => break,
                Err(e) if self.service.silent => {
                    tracing::debug!(
                        error = %e,
                        "SERVICE SILENT: ignoring error from ledger '{}'",
                        full_ledger_ref
                    );
                    break;
                }
                Err(e) => {
                    inner.close();
                    return Err(e);
                }
            };

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
                    // Check if this is the endpoint variable we need to bind
                    if bind_endpoint_var == Some(*var) {
                        // Bind endpoint variable to the endpoint IRI
                        let binding = Binding::Lit {
                            val: FlakeValue::String(endpoint_iri.to_string()),
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

    /// Execute SERVICE against a remote Fluree instance via the RemoteServiceExecutor trait.
    async fn execute_against_remote(
        &mut self,
        ctx: &ExecutionContext<'_>,
        parent_batch: &Batch,
        row_idx: usize,
        connection_name: &str,
        ledger: &str,
    ) -> Result<()> {
        let executor = ctx.remote_service.ok_or_else(|| {
            QueryError::InvalidQuery(format!(
                "No remote service executor configured. Cannot reach 'fluree:remote:{connection_name}/{ledger}'"
            ))
        })?;

        let source_body = self.service.source_body.as_deref().ok_or_else(|| {
            QueryError::InvalidQuery(
                "Remote SERVICE requires SPARQL source text (not available for JSON-LD queries)"
                    .into(),
            )
        })?;

        // Build a complete SPARQL query from the captured body.
        // The source_body may or may not include braces depending on whether
        // parse_group_graph_pattern unwrapped a single-pattern group. Always
        // wrap in braces to ensure valid SPARQL (double-braces are legal).
        let sparql = format!("SELECT * WHERE {{ {source_body} }}");

        let result = match executor
            .execute_remote_sparql(connection_name, ledger, &sparql)
            .await
        {
            Ok(r) => r,
            Err(e) if self.service.silent => {
                tracing::debug!(
                    error = %e,
                    connection = connection_name,
                    ledger = ledger,
                    "SERVICE SILENT: ignoring remote execution error"
                );
                return Ok(());
            }
            Err(e) => return Err(e),
        };

        // Merge each remote result row with the parent row
        for remote_row in &result.rows {
            let mut merged_row = Vec::with_capacity(self.schema.len());

            // Copy parent bindings
            for var in self.child.schema() {
                let binding = parent_batch
                    .get(row_idx, *var)
                    .cloned()
                    .unwrap_or(Binding::Unbound);
                merged_row.push(binding);
            }

            // Append new variables from inner patterns
            let parent_len = self.child.schema().len();
            for var in self.schema.iter().skip(parent_len) {
                // Find this var's name and look it up in the remote results
                let var_name = ctx.vars.name(*var);
                let stripped = var_name.strip_prefix('?').unwrap_or(var_name);
                let binding = remote_row
                    .get(stripped)
                    .cloned()
                    .unwrap_or(Binding::Unbound);
                merged_row.push(binding);
            }

            self.result_buffer.push(merged_row);
        }

        Ok(())
    }

    /// Drain buffered results into a batch
    fn drain_buffer(&mut self) -> Result<Option<Batch>> {
        if self.buffer_pos >= self.result_buffer.len() {
            return Ok(None);
        }

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
impl Operator for ServiceOperator {
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

        // Clone service to avoid borrow conflicts
        let service_endpoint = self.service.endpoint.clone();
        let silent = self.service.silent;

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
                match &service_endpoint {
                    ServiceEndpoint::Iri(iri) => {
                        // Concrete endpoint
                        if Self::is_fluree_ledger_endpoint(iri) {
                            if let Some(full_ref) = Self::parse_fluree_ledger_ref(iri) {
                                self.execute_against_ledger(
                                    ctx,
                                    &parent_batch,
                                    row_idx,
                                    &full_ref,
                                    None, // No variable binding needed
                                    iri,
                                )
                                .await?;
                            } else if silent {
                                // Malformed ledger ref with SILENT - skip
                                tracing::debug!(
                                    endpoint = %iri,
                                    "SERVICE SILENT: malformed fluree:ledger endpoint"
                                );
                            } else {
                                return Err(QueryError::InvalidQuery(format!(
                                    "Invalid fluree:ledger endpoint format: '{iri}'. Expected 'fluree:ledger:<alias>' or 'fluree:ledger:<alias>:<branch>'"
                                )));
                            }
                        } else if is_fluree_remote_endpoint(iri) {
                            if let Some((connection, ledger)) = parse_fluree_remote_ref(iri) {
                                self.execute_against_remote(
                                    ctx,
                                    &parent_batch,
                                    row_idx,
                                    connection,
                                    ledger,
                                )
                                .await?;
                            } else if silent {
                                tracing::debug!(
                                    endpoint = %iri,
                                    "SERVICE SILENT: malformed fluree:remote endpoint"
                                );
                            } else {
                                return Err(QueryError::InvalidQuery(format!(
                                    "Invalid fluree:remote endpoint format: '{iri}'. Expected 'fluree:remote:<connection>/<ledger>'"
                                )));
                            }
                        } else {
                            // Non-Fluree endpoint - not supported
                            if silent {
                                tracing::debug!(
                                    endpoint = %iri,
                                    "SERVICE SILENT: external endpoints not supported"
                                );
                            } else {
                                return Err(QueryError::InvalidQuery(format!(
                                    "External SERVICE endpoints not supported. Use 'fluree:ledger:<alias>' for local ledger queries or 'fluree:remote:<connection>/<ledger>' for remote Fluree instances. Got: '{iri}'"
                                )));
                            }
                        }
                    }
                    ServiceEndpoint::Var(var) => {
                        // Variable endpoint - check if bound
                        if let Some(binding) = parent_batch.get(row_idx, *var) {
                            if let Some(bound_iri) = Self::extract_endpoint_from_binding(binding) {
                                // Variable is bound - use that endpoint
                                if Self::is_fluree_ledger_endpoint(&bound_iri) {
                                    if let Some(full_ref) =
                                        Self::parse_fluree_ledger_ref(&bound_iri)
                                    {
                                        self.execute_against_ledger(
                                            ctx,
                                            &parent_batch,
                                            row_idx,
                                            &full_ref,
                                            None, // Already bound
                                            &bound_iri,
                                        )
                                        .await?;
                                    } else if silent {
                                        // Malformed ledger ref with SILENT - skip
                                        tracing::debug!(
                                            endpoint = %bound_iri,
                                            "SERVICE SILENT: malformed fluree:ledger endpoint"
                                        );
                                    } else {
                                        return Err(QueryError::InvalidQuery(format!(
                                            "Invalid fluree:ledger endpoint format: '{bound_iri}'"
                                        )));
                                    }
                                } else if is_fluree_remote_endpoint(&bound_iri) {
                                    if let Some((connection, ledger)) =
                                        parse_fluree_remote_ref(&bound_iri)
                                    {
                                        self.execute_against_remote(
                                            ctx,
                                            &parent_batch,
                                            row_idx,
                                            connection,
                                            ledger,
                                        )
                                        .await?;
                                    } else if silent {
                                        tracing::debug!(
                                            endpoint = %bound_iri,
                                            "SERVICE SILENT: malformed fluree:remote endpoint"
                                        );
                                    } else {
                                        return Err(QueryError::InvalidQuery(format!(
                                            "Invalid fluree:remote endpoint format: '{bound_iri}'"
                                        )));
                                    }
                                } else if silent {
                                    tracing::debug!(
                                        endpoint = %bound_iri,
                                        "SERVICE SILENT: external endpoints not supported"
                                    );
                                } else {
                                    return Err(QueryError::InvalidQuery(format!(
                                        "External SERVICE endpoints not supported: '{bound_iri}'"
                                    )));
                                }
                            }
                            // Binding exists but not a string - skip silently
                        } else {
                            // Variable unbound - iterate all ledgers in dataset
                            if let Some(ds) = &ctx.dataset {
                                // Iterate all ledgers in dataset
                                let ledger_ides: Vec<Arc<str>> = ds
                                    .named_graphs_iter()
                                    .map(|(_, g)| g.ledger_id.clone())
                                    .collect();

                                for ledger_id in ledger_ides {
                                    let endpoint_iri = format!("{FLUREE_LEDGER_PREFIX}{ledger_id}");
                                    self.execute_against_ledger(
                                        ctx,
                                        &parent_batch,
                                        row_idx,
                                        &ledger_id,
                                        Some(*var), // Bind the variable
                                        &endpoint_iri,
                                    )
                                    .await?;
                                }

                                // Also try default graphs
                                for gref in ds.default_graphs() {
                                    let ledger_id = &gref.ledger_id;
                                    let endpoint_iri = format!("{FLUREE_LEDGER_PREFIX}{ledger_id}");
                                    self.execute_against_ledger(
                                        ctx,
                                        &parent_batch,
                                        row_idx,
                                        ledger_id,
                                        Some(*var),
                                        &endpoint_iri,
                                    )
                                    .await?;
                                }
                            } else {
                                // No dataset - use current db as only service
                                let ledger_id = &ctx.active_snapshot.ledger_id;
                                let endpoint_iri = format!("{FLUREE_LEDGER_PREFIX}{ledger_id}");
                                self.execute_against_ledger(
                                    ctx,
                                    &parent_batch,
                                    row_idx,
                                    ledger_id,
                                    Some(*var),
                                    &endpoint_iri,
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
        // SERVICE patterns can multiply rows; hard to estimate
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_fluree_ledger_ref() {
        // Valid: alias only (defaults to main)
        assert_eq!(
            ServiceOperator::parse_fluree_ledger_ref("fluree:ledger:mydb"),
            Some("mydb:main".to_string())
        );

        // Valid: alias and branch
        assert_eq!(
            ServiceOperator::parse_fluree_ledger_ref("fluree:ledger:orders:main"),
            Some("orders:main".to_string())
        );

        // Valid: alias and custom branch
        assert_eq!(
            ServiceOperator::parse_fluree_ledger_ref("fluree:ledger:orders:dev"),
            Some("orders:dev".to_string())
        );

        // Valid: alias containing slash (e.g., org/ledger-name)
        assert_eq!(
            ServiceOperator::parse_fluree_ledger_ref("fluree:ledger:acme/people:main"),
            Some("acme/people:main".to_string())
        );

        // Valid: alias with slash, default branch
        assert_eq!(
            ServiceOperator::parse_fluree_ledger_ref("fluree:ledger:acme/people"),
            Some("acme/people:main".to_string())
        );

        // Invalid: just prefix
        assert_eq!(
            ServiceOperator::parse_fluree_ledger_ref("fluree:ledger:"),
            None
        );

        // Invalid: not a fluree:ledger endpoint
        assert_eq!(
            ServiceOperator::parse_fluree_ledger_ref("http://example.org/sparql"),
            None
        );

        // Invalid: empty alias
        assert_eq!(
            ServiceOperator::parse_fluree_ledger_ref("fluree:ledger::main"),
            None
        );
    }

    #[test]
    fn test_is_fluree_ledger_endpoint() {
        assert!(ServiceOperator::is_fluree_ledger_endpoint(
            "fluree:ledger:mydb"
        ));
        assert!(ServiceOperator::is_fluree_ledger_endpoint(
            "fluree:ledger:orders:main"
        ));
        assert!(!ServiceOperator::is_fluree_ledger_endpoint(
            "http://example.org/sparql"
        ));
        assert!(!ServiceOperator::is_fluree_ledger_endpoint("fluree:other"));
    }
}
