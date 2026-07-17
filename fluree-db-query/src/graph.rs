//! GRAPH pattern operator - scopes inner patterns to a specific graph
//!
//! Implements SPARQL GRAPH semantics:
//! - `GRAPH <iri> { ... }`: Execute inner patterns against a specific named graph
//! - `GRAPH ?g { ... }`: If ?g is bound, use that graph; if unbound, iterate
//!   the **named graphs only** (per SPARQL 1.1 §13.3 the default graph is not
//!   part of the range of `?g`)
//!
//! Key semantics:
//! - GraphOperator is a **correlated operator** (like EXISTS/Subquery)
//! - For each parent row, inner patterns are executed in the appropriate graph context
//! - ?g is bound as an IRI term (`Binding::Iri`). Per the SPARQL algebra the
//!   `{?g → graph}` binding is JOINED with the inner solutions: an inner
//!   occurrence of `?g` bound to a different term drops the row, and `?g` is
//!   NOT in scope inside the group otherwise (`GRAPH ?g { FILTER(BOUND(?g)) }`
//!   is empty — W3C graph-variable-scope). As a join-equivalent optimization
//!   the value is seeded into the inner subplan when the inner *always* binds
//!   `?g` (a required top-level triple/path/sub-SELECT), narrowing the scan.
//! - Graph-not-found produces empty result (not an error)
//!
//! # Single-DB Mode
//!
//! In single-db mode (no dataset) every graph of the ledger lives in one
//! snapshot, partitioned by `g_id`. Named graphs resolve against the snapshot's
//! graph registry (user graphs, `g_id >= FIRST_USER_GRAPH_ID`) without an
//! explicit `FROM NAMED` (issue #1279); the ledger alias EXPLICITLY addresses
//! the default graph, and reserved system graphs (txn-meta, config) stay private.
//! - `GRAPH <iri>` / bound `GRAPH ?g`: executes for a registered user graph,
//!   the ledger alias, or an R2RML graph source; otherwise empty
//! - unbound `GRAPH ?g`: binds ?g to each registered user graph. The ledger
//!   alias (default graph) is NOT enumerated — W3C-conformant since issue
//!   #1442 (decision D-2); the #1279 implicit default-graph enumeration was
//!   dropped, while explicit alias addressing above is retained
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
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::execute::build_where_operators_seeded;
use crate::ir::{GraphName, Pattern};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::r2rml::rewrite_patterns_for_r2rml;
use crate::seed::{BatchSeedOperator, SeedOperator};
use crate::temporal_mode::PlanningContext;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::FlakeValue;
use fluree_db_r2rml::mapping::CompiledR2rmlMapping;
use std::sync::Arc;
// Note: tracing::debug removed to fix compilation - add tracing dependency if needed

/// Best-effort load of the compiled R2RML mapping for `graph_iri`, used only to
/// let [`rewrite_patterns_for_r2rml`] decide whether a same-subject `rdf:type`
/// may be safely fused into a star scan. Returns `None` (which disables class
/// fusion but stays correct) when there is no provider or the load fails; the
/// R2RML operator loads the mapping again at setup, so within a query this is a
/// cache hit under the query-scoped catalog session.
async fn r2rml_mapping_for_rewrite(
    ctx: &ExecutionContext<'_>,
    graph_iri: &str,
) -> Option<Arc<CompiledR2rmlMapping>> {
    let provider = ctx.r2rml_provider?;
    let as_of_t = if ctx.dataset.is_some() {
        None
    } else {
        Some(ctx.to_t)
    };
    provider.compiled_mapping(graph_iri, as_of_t).await.ok()
}

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
    /// LIMIT budget forwarded from a downstream `LIMIT` (via row-preserving
    /// operators). Threaded into the per-parent-batch inner subplan so a scan
    /// under a GRAPH wrapper (notably an R2RML graph source) can early-terminate
    /// instead of draining the whole table into `result_buffer`.
    row_budget: Option<usize>,
    /// Plan-time decision: seed the enumerated graph variable into the inner
    /// subplan. True only when the inner patterns bind the graph var in EVERY
    /// solution (required top-level triple / property path / slice-free
    /// sub-SELECT — `self_produced_vars`), where seeding merely filters and is
    /// therefore equivalent to the SPARQL `{?g → graph}` join while strictly
    /// narrowing the inner scan. When false, the join is enforced at merge
    /// time instead, preserving `?g`-not-in-scope semantics for FILTER-only /
    /// OPTIONAL / UNION references (W3C graph-variable-scope).
    seed_graph_var: bool,
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

        // Plan-time: seeding the graph var is join-equivalent only when the
        // inner always binds it (see the field doc on `seed_graph_var`).
        let seed_graph_var = match &graph_name {
            GraphName::Var(v) => crate::subquery::self_produced_vars(&inner_patterns).contains(v),
            GraphName::Iri(_) => false,
        };

        Self {
            child,
            graph_name,
            inner_patterns,
            schema,
            state: OperatorState::Created,
            result_buffer: Vec::new(),
            buffer_pos: 0,
            planning,
            row_budget: None,
            seed_graph_var,
        }
    }

    /// Extract a graph IRI from a bound `?g`. Handles the IRI-typed forms a
    /// normal query produces — `<iri>` lowered to a `Sid` (decoded against the
    /// active snapshot), a raw `Iri`, or a cross-ledger `IriMatch` — plus the
    /// late-materialized `EncodedSid`/`EncodedLit` forms a binary-index scan
    /// binds (issue #1443: `?s :p ?g . FILTER EXISTS { GRAPH ?g { … } }`), and
    /// also a plain string literal for back-compat.
    fn extract_graph_iri_from_binding(
        ctx: &ExecutionContext<'_>,
        binding: &Binding,
    ) -> Option<Arc<str>> {
        if let Some(iri) = binding.get_iri() {
            return Some(Arc::clone(iri));
        }
        match binding {
            Binding::Sid { sid, .. } => ctx.active_snapshot.decode_sid(sid).map(Arc::from),
            Binding::Lit {
                val: FlakeValue::String(s),
                ..
            } => Some(Arc::from(s.as_str())),
            // Late-materialized bindings from the binary index: decode against
            // the active graph view, then extract from the decoded form.
            // Defense-in-depth: as of PR-1454's audit, every upstream
            // operator (hash join, filter/EXISTS seeding, merge) materializes
            // batches before they reach either extraction call site, so no
            // known plan shape delivers an encoded binding here — but that is
            // a property of operator internals, not of this function's
            // contract, and extraction must stay total across binding kinds.
            // (Subject/string dictionaries are store-global, so decoding
            // against the outer view is sound; when extraction DOES run in
            // the non-seeded UNION/OPTIONAL merge shape it is per inner row.)
            Binding::EncodedSid { .. } | Binding::EncodedLit { .. } => {
                let gv = ctx.graph_view()?;
                match crate::group_aggregate::materialize_encoded(binding, Some(&gv)) {
                    Binding::Sid { sid, .. } => ctx.active_snapshot.decode_sid(&sid).map(Arc::from),
                    Binding::Lit {
                        val: FlakeValue::String(s),
                        ..
                    } => Some(Arc::from(s.as_str())),
                    _ => None,
                }
            }
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
        let mut graph_ctx = ctx.with_active_graph(graph_iri.clone());

        // Cross-ledger provenance for GRAPH patterns.
        //
        // Inside a `GRAPH <iri> { .. }` scope only one graph is active, so the
        // inner `DatasetOperator`'s multi-ledger check (which compares the
        // *active* graphs) is always false and it never stamps. The inner scan
        // therefore emits plain `Binding::Sid` values encoded against this
        // graph's namespace table. If the surrounding dataset spans multiple
        // ledgers, those SIDs would later be decoded against the formatter's
        // primary view (a different ledger), silently mis-decoding the IRI.
        //
        // When the dataset is multi-ledger, stamp inner results with this
        // graph's home ledger so they carry `Binding::IriMatch` provenance —
        // matching what `DatasetOperator` produces for the union (non-GRAPH)
        // path. Forcing eager materialization makes the inner scan resolve
        // `Binding::Sid` rather than late-materialized `Binding::EncodedSid`,
        // which `stamp_provenance` cannot decode without the binary store.
        let stamp_ledger_id: Option<Arc<str>> = match &ctx.dataset {
            Some(ds) if ds.spans_multiple_ledgers() => ds
                .named_graph(graph_iri.as_ref())
                .map(|g| Arc::clone(&g.ledger_id)),
            _ => None,
        };
        if stamp_ledger_id.is_some() {
            graph_ctx.eager_materialization = true;
        }

        // Check if this graph is backed by an R2RML mapping.
        // Prefer the precomputed set (populated in runner.rs for dataset queries),
        // but fall back to asking the provider dynamically for the no-dataset
        // single-source path where the GRAPH IRI may differ from the ledger_id.
        let is_r2rml_gs = if ctx.r2rml_graph_ids.contains(graph_iri.as_ref()) {
            true
        } else if ctx.single_db_user_graph_id(&graph_iri).is_some() {
            // Registered native graph — never R2RML; skip the per-graph probe.
            false
        } else if let Some(provider) = ctx.r2rml_provider {
            provider.has_r2rml_mapping(&graph_iri).await
        } else {
            false
        };

        // Determine which patterns to use (rewritten for R2RML or original)
        let patterns_to_execute: std::borrow::Cow<'_, [Pattern]> = if is_r2rml_gs {
            // Rewrite triple patterns to R2RML patterns
            let mapping = r2rml_mapping_for_rewrite(ctx, &graph_iri).await;
            let rewrite_result = rewrite_patterns_for_r2rml(
                &self.inner_patterns,
                &graph_iri,
                ctx.active_snapshot,
                mapping.as_deref(),
                ctx.reasoning_active,
                ctx.trust_fk_refs,
            );

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

        // Build seed operator from parent row (like EXISTS/Subquery). When this
        // enumeration binds the graph variable AND the inner subplan is
        // guaranteed to bind it in every solution (`seed_graph_var`, decided at
        // plan time), seed it as an IRI term so inner occurrences of `?g` — a
        // triple position, a sub-SELECT projecting `?g` — are constrained to
        // the active graph's name instead of scanning free and being joined
        // away at merge time. Join-equivalent by construction; strictly
        // narrows the inner scan. All other shapes rely on the merge-time
        // `{?g → graph}` join below.
        let seed = match bind_graph_var {
            Some(var) if self.seed_graph_var && !parent_batch.schema().contains(&var) => {
                let mut schema_vec = parent_batch.schema().to_vec();
                schema_vec.push(var);
                let mut row: Vec<Binding> = parent_batch
                    .row_view(row_idx)
                    .expect("row_idx must be valid for batch")
                    .to_vec();
                row.push(Binding::iri(graph_iri.clone()));
                SeedOperator::from_row(Arc::from(schema_vec.into_boxed_slice()), row)
            }
            _ => SeedOperator::from_batch_row(parent_batch, row_idx),
        };
        let mut inner = build_where_operators_seeded(
            Some(Box::new(seed)),
            &patterns_to_execute,
            None,
            None,
            &self.planning,
        )?;

        if let Some(budget) = self.row_budget {
            inner.set_row_budget(budget);
        }
        inner.open(&graph_ctx).await?;

        // NumBig arena handles are scoped per (graph, predicate). When this
        // GRAPH scope runs against a different g_id than the surrounding
        // query, encoded NUM_BIG bindings escaping the scope would later be
        // decoded against the OUTER graph's arena — silently producing wrong
        // values. Materialize them here, against this graph's view, before
        // they leave the scope. (Subject/string/predicate dictionaries are
        // store-global, so all other encoded kinds escape safely.)
        let numbig_exit_gv = if graph_ctx.binary_g_id != ctx.binary_g_id {
            graph_ctx.graph_view()
        } else {
            None
        };

        while let Some(batch) = inner.next_batch(&graph_ctx).await? {
            graph_ctx.check_cancelled()?;
            // Stamp cross-ledger provenance before merging so the formatter
            // decodes SIDs against this graph's home ledger (see above).
            let batch = match &stamp_ledger_id {
                Some(ledger_id) => {
                    crate::dataset_operator::stamp_provenance(batch, ledger_id, &graph_ctx)?
                }
                None => batch,
            };

            // Merge each inner result with parent row
            for inner_row_idx in 0..batch.len() {
                // SPARQL algebra: the `{?g → graph}` binding is JOINED with
                // the inner solutions. A row whose inner `?g` is bound to a
                // different term than the active graph's name is incompatible
                // and is dropped — never overwritten (W3C graph-optional /
                // graph-variable-join). Runs only when the enumeration binds a
                // graph var the inner body actually carries; when the value
                // was seeded (`seed_graph_var`) it short-circuits on the
                // `Binding::Iri` fast path.
                //
                // Two deliberate edges (PR-1454 review): (1) an inner `?g`
                // whose binding fails extraction (`extract → None`: a
                // non-string literal, or an encoded form with no graph view)
                // compares unequal and the row drops — lossy-but-safe over
                // erroring mid-merge; (2) extraction honors the documented
                // string-literal back-compat, so a plain-string graph name
                // joins the IRI-valued enumeration by VALUE across term
                // kinds where strict SPARQL term-equality would drop it
                // (kept for pre-IRI-migration data).
                if let Some(gvar) = bind_graph_var {
                    if let Some(b) = batch.get(inner_row_idx, gvar) {
                        if !matches!(b, Binding::Unbound | Binding::Poisoned)
                            && Self::extract_graph_iri_from_binding(&graph_ctx, b).as_deref()
                                != Some(graph_iri.as_ref())
                        {
                            continue;
                        }
                    }
                }

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
                        // Bind ?g to the graph name as an IRI term (SPARQL
                        // requires an IRI, not a string literal). The inner
                        // subplan was seeded with the same value, so this is
                        // consistent with — not an overwrite of — any inner
                        // occurrence of the variable.
                        merged_row.push(Binding::iri(graph_iri.clone()));
                    } else {
                        // Get from inner batch
                        let binding = batch
                            .get(inner_row_idx, *var)
                            .cloned()
                            .unwrap_or(Binding::Unbound);
                        let binding = if numbig_exit_gv.is_some()
                            && crate::object_binding::is_numbig_encoded(&binding)
                        {
                            crate::group_aggregate::materialize_encoded(
                                &binding,
                                numbig_exit_gv.as_ref(),
                            )
                        } else {
                            binding
                        };
                        merged_row.push(binding);
                    }
                }

                self.result_buffer.push(merged_row);
            }
            graph_ctx.check_cancelled()?;
        }

        inner.close();
        Ok(())
    }

    /// R2RML fast path: run a concrete-IRI graph-source block once over the
    /// WHOLE parent batch (uncorrelated), so the inner R2RML scan hash-joins all
    /// parent rows in a single pass instead of re-scanning the table per parent
    /// row (which is O(parent_rows × table_rows)).
    ///
    /// Sound because an R2RML block is a pure conjunction of scans + filters
    /// (the caller has verified the source and the rewrite rejects unconvertible
    /// patterns): running it once over the batch and hash-joining on the shared
    /// variables yields exactly the per-row correlated result.
    async fn execute_in_graph_batched(
        &mut self,
        ctx: &ExecutionContext<'_>,
        parent_batch: &Batch,
        graph_iri: Arc<str>,
    ) -> Result<()> {
        let mut graph_ctx = ctx.with_active_graph(graph_iri.clone());

        let stamp_ledger_id: Option<Arc<str>> = match &ctx.dataset {
            Some(ds) if ds.spans_multiple_ledgers() => ds
                .named_graph(graph_iri.as_ref())
                .map(|g| Arc::clone(&g.ledger_id)),
            _ => None,
        };
        if stamp_ledger_id.is_some() {
            graph_ctx.eager_materialization = true;
        }

        let mapping = r2rml_mapping_for_rewrite(ctx, &graph_iri).await;
        let rewrite_result = rewrite_patterns_for_r2rml(
            &self.inner_patterns,
            &graph_iri,
            ctx.active_snapshot,
            mapping.as_deref(),
            ctx.reasoning_active,
            ctx.trust_fk_refs,
        );
        if rewrite_result.unconverted_count > 0 {
            return Err(crate::error::QueryError::InvalidQuery(format!(
                "R2RML graph source '{}' contains {} pattern(s) that cannot be converted \
                 to R2RML scans.",
                graph_iri, rewrite_result.unconverted_count
            )));
        }

        let seed = BatchSeedOperator::from_batch(parent_batch.clone());
        let mut inner = build_where_operators_seeded(
            Some(Box::new(seed)),
            &rewrite_result.patterns,
            None,
            None,
            &self.planning,
        )?;
        // Forward a downstream LIMIT budget so the inner scan early-terminates
        // instead of draining the whole table into `result_buffer`. Correctness
        // is bounded by the outer LIMIT; a per-parent-batch budget can over-read
        // across parent batches but never under-reads.
        if let Some(budget) = self.row_budget {
            inner.set_row_budget(budget);
        }
        inner.open(&graph_ctx).await?;

        let numbig_exit_gv = if graph_ctx.binary_g_id != ctx.binary_g_id {
            graph_ctx.graph_view()
        } else {
            None
        };

        while let Some(batch) = inner.next_batch(&graph_ctx).await? {
            graph_ctx.check_cancelled()?;
            let batch = match &stamp_ledger_id {
                Some(ledger_id) => {
                    crate::dataset_operator::stamp_provenance(batch, ledger_id, &graph_ctx)?
                }
                None => batch,
            };

            // The inner output already carries the parent columns (threaded
            // through the multi-row seed) plus the new R2RML vars, so map each
            // output variable of `self.schema` directly from it.
            for inner_row_idx in 0..batch.len() {
                let mut merged_row = Vec::with_capacity(self.schema.len());
                for var in self.schema.iter() {
                    let binding = batch
                        .get(inner_row_idx, *var)
                        .cloned()
                        .unwrap_or(Binding::Unbound);
                    let binding = if numbig_exit_gv.is_some()
                        && crate::object_binding::is_numbig_encoded(&binding)
                    {
                        crate::group_aggregate::materialize_encoded(
                            &binding,
                            numbig_exit_gv.as_ref(),
                        )
                    } else {
                        binding
                    };
                    merged_row.push(binding);
                }
                self.result_buffer.push(merged_row);
            }
            graph_ctx.check_cancelled()?;
        }

        inner.close();
        Ok(())
    }

    /// Drain buffered results into a batch
    fn drain_buffer(&mut self) -> Result<Option<Batch>> {
        if self.buffer_pos >= self.result_buffer.len() {
            return Ok(None);
        }

        // Number of rows about to be drained — needed to size an empty-schema
        // batch, where there are no columns to infer the row count from.
        let drained = self.result_buffer.len() - self.buffer_pos;

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

        // A ground GRAPH body (e.g. `GRAPH <g> { :a :p "1" }`) produces no
        // variables, so the schema is empty. Each matched row is still one
        // empty-binding solution and must be preserved — emit an empty-schema
        // batch carrying the row count rather than collapsing to zero rows
        // (which would wrongly turn a satisfied existence check into a no-match).
        if num_cols == 0 {
            return Ok(Some(Batch::empty_schema_with_len(drained)));
        }

        if columns[0].is_empty() {
            Ok(None)
        } else {
            Ok(Some(Batch::new(self.schema.clone(), columns)?))
        }
    }
}

#[async_trait]
impl Operator for GraphOperator {
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        vec![crate::plan_node::PlanChild::child(self.child.as_ref())]
    }
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    fn set_row_budget(&mut self, budget: usize) {
        // Record the budget and thread it into the per-parent-batch inner subplan
        // (see the execute helpers). Do NOT forward to `self.child`: the child
        // produces parent rows that seed the correlated inner execution, which is
        // not row-preserving, so it must still yield every row the inner needs.
        self.row_budget = Some(budget);
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

            // Fast path: a concrete-IRI R2RML graph source in single-db mode.
            // Run the whole parent batch through ONE uncorrelated scan so the
            // inner R2RML hash join joins all parent rows at once, instead of
            // re-scanning the table per parent row.
            if ctx.dataset.is_none() {
                if let GraphName::Iri(iri) = &graph_name {
                    let is_user_graph = ctx.single_db_user_graph_id(iri).is_some();
                    let is_alias = iri.as_ref() == ctx.active_snapshot.ledger_id;
                    let is_r2rml_gs = !is_user_graph
                        && !is_alias
                        && if ctx.r2rml_graph_ids.contains(iri.as_ref()) {
                            true
                        } else if let Some(provider) = ctx.r2rml_provider {
                            provider.has_r2rml_mapping(iri).await
                        } else {
                            false
                        };
                    if is_r2rml_gs {
                        self.execute_in_graph_batched(ctx, &parent_batch, iri.clone())
                            .await?;
                        if self.result_buffer.is_empty() {
                            continue;
                        }
                        return self.drain_buffer();
                    }
                }
            }

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
                            // Single-db: a registered user graph, the ledger
                            // alias (default graph), or an R2RML graph source.
                            let is_user_graph = ctx.single_db_user_graph_id(iri).is_some();
                            let is_alias = iri.as_ref() == ctx.active_snapshot.ledger_id;
                            let is_r2rml_gs = !is_user_graph
                                && !is_alias
                                && if ctx.r2rml_graph_ids.contains(iri.as_ref()) {
                                    true
                                } else if let Some(provider) = ctx.r2rml_provider {
                                    provider.has_r2rml_mapping(iri).await
                                } else {
                                    false
                                };

                            if is_user_graph || is_alias || is_r2rml_gs {
                                self.execute_in_graph(
                                    ctx,
                                    &parent_batch,
                                    row_idx,
                                    iri.clone(),
                                    None,
                                )
                                .await?;
                            }
                        }
                    }
                    GraphName::Var(var) => {
                        // Check if ?g is already bound in parent row
                        if let Some(binding) = parent_batch.get(row_idx, *var) {
                            if let Some(bound_iri) =
                                Self::extract_graph_iri_from_binding(ctx, binding)
                            {
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
                                    // Single-db: same resolution as the concrete arm.
                                    let is_user_graph =
                                        ctx.single_db_user_graph_id(&bound_iri).is_some();
                                    let is_alias =
                                        bound_iri.as_ref() == ctx.active_snapshot.ledger_id;
                                    let is_r2rml_gs = !is_user_graph
                                        && !is_alias
                                        && if ctx.r2rml_graph_ids.contains(bound_iri.as_ref()) {
                                            true
                                        } else if let Some(provider) = ctx.r2rml_provider {
                                            provider.has_r2rml_mapping(&bound_iri).await
                                        } else {
                                            false
                                        };

                                    if is_user_graph || is_alias || is_r2rml_gs {
                                        self.execute_in_graph(
                                            ctx,
                                            &parent_batch,
                                            row_idx,
                                            bound_iri,
                                            None,
                                        )
                                        .await?;
                                    }
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
                                // Single-db: bind ?g to each registered user
                                // graph (empty graphs emit no rows). The ledger
                                // alias (default graph) is NOT enumerated: per
                                // SPARQL 1.1, `GRAPH ?g` ranges over named
                                // graphs only (D-2 / issue #1442 dropped the
                                // #1279 implicit enumeration). The default
                                // graph remains explicitly addressable via
                                // `GRAPH <alias>` in the arms above.
                                for iri in ctx.single_db_user_graph_iris() {
                                    self.execute_in_graph(
                                        ctx,
                                        &parent_batch,
                                        row_idx,
                                        iri,
                                        Some(*var),
                                    )
                                    .await?;
                                }
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
    use crate::var_registry::VarRegistry;
    use fluree_db_core::{DatatypeConstraint, LedgerSnapshot, Sid};

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
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        // Raw IRI binding — the idiomatic `VALUES ?g { <iri> }` form
        let binding = Binding::Iri(Arc::from("urn:probegraph"));
        assert_eq!(
            GraphOperator::extract_graph_iri_from_binding(&ctx, &binding),
            Some(Arc::from("urn:probegraph"))
        );

        // Cross-ledger IriMatch carries the canonical IRI
        let binding = Binding::iri_match("urn:other", Sid::new(2, "x"), "other:main");
        assert_eq!(
            GraphOperator::extract_graph_iri_from_binding(&ctx, &binding),
            Some(Arc::from("urn:other"))
        );

        // String literal still accepted for back-compat
        let binding = Binding::Lit {
            val: FlakeValue::String("http://example.org/graph1".to_string()),
            dtc: DatatypeConstraint::Explicit(Sid::new(2, "string")),
            t: None,
            op: None,
            p_id: None,
        };
        assert_eq!(
            GraphOperator::extract_graph_iri_from_binding(&ctx, &binding),
            Some(Arc::from("http://example.org/graph1"))
        );

        // Non-string literal and Unbound return None
        let binding = Binding::Lit {
            val: FlakeValue::Long(42),
            dtc: DatatypeConstraint::Explicit(Sid::new(2, "long")),
            t: None,
            op: None,
            p_id: None,
        };
        assert_eq!(
            GraphOperator::extract_graph_iri_from_binding(&ctx, &binding),
            None
        );
        assert_eq!(
            GraphOperator::extract_graph_iri_from_binding(&ctx, &Binding::Unbound),
            None
        );
    }
}
