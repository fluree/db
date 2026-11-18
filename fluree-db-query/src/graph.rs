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
//! explicit `FROM NAMED` (issue #1279), through the table every graph position
//! of a ledger shares (`TargetLedger::graph_position`): the ledger's own
//! address, in any spelling, EXPLICITLY addresses the default graph, `L#<g>`
//! the graph `<g>`, and reserved system graphs (txn-meta, config) stay private.
//! - `GRAPH <iri>` / bound `GRAPH ?g`: executes for a registered user graph,
//!   the ledger's own address, or an R2RML graph source; otherwise empty
//! - unbound `GRAPH ?g`: binds ?g to each registered user graph, under a name
//!   that reads it back (`L#<iri>` for a graph registered under the ledger's
//!   own address). The default graph is NOT enumerated — W3C-conformant since
//!   issue #1442; the #1279 implicit default-graph enumeration
//!   was dropped, while explicit addressing above is retained
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
use crate::r2rml::{
    r2rml_unsupported_pattern_error, rewrite_patterns_for_r2rml_source, unsupported_subscope_error,
};
use crate::seed::{BatchSeedOperator, SeedOperator};
use crate::sort::SortSpec;
use crate::temporal_mode::PlanningContext;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::FlakeValue;
use fluree_db_r2rml::mapping::CompiledR2rmlMapping;
use std::sync::Arc;
// Note: tracing::debug removed to fix compilation - add tracing dependency if needed

/// Load the compiled R2RML mapping for `graph_iri`, which the caller has just
/// found to be an R2RML graph source, so [`rewrite_patterns_for_r2rml`] can
/// decide whether a same-subject `rdf:type` may be safely fused into a star
/// scan. `None` only when there is no provider. A failed load fails the query
/// rather than silently disabling the rewrite; the R2RML operator loads the
/// mapping again at setup, so within a query this is a cache hit under the
/// query-scoped catalog session.
async fn r2rml_mapping_for_rewrite(
    ctx: &ExecutionContext<'_>,
    graph_iri: &str,
) -> Result<Option<Arc<CompiledR2rmlMapping>>> {
    let Some(provider) = ctx.r2rml_provider else {
        return Ok(None);
    };
    let as_of_t = if ctx.explicit_dataset().is_some() {
        None
    } else {
        Some(ctx.to_t)
    };
    provider
        .compiled_mapping(graph_iri, as_of_t)
        .await
        .map(Some)
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
    /// The parent batch whose `(row, graph)` executions are queued in `work`.
    parent_batch: Option<Batch>,
    /// The inner subplans still to run for `parent_batch`, in order.
    work: std::collections::VecDeque<GraphWork>,
    /// Planning context captured at planner-time for the per-row inner subplan.
    planning: PlanningContext,
    /// LIMIT budget forwarded from a downstream `LIMIT` (via row-preserving
    /// operators). Threaded into the per-parent-batch inner subplan so a scan
    /// under a GRAPH wrapper (notably an R2RML graph source) can early-terminate
    /// instead of reading the whole table.
    row_budget: Option<usize>,
    /// Scan-side top-k directive (PR-5): `(primary DESC sort var, LIMIT+OFFSET)`,
    /// threaded into the per-parent inner subplan exactly like `row_budget` so a
    /// `ORDER BY DESC(<scan col>) LIMIT k` above a GRAPH wrapper (an R2RML graph
    /// source) reaches the inner scan. Per-partition top-k is sound: any global
    /// top-k row from partition p is among p's k largest, so the global top-k is a
    /// subset of the union of the per-partition results (the authoritative sort
    /// above re-selects the exact k).
    topk: Option<(Vec<SortSpec>, usize)>,
    /// Plan-time decision: seed the enumerated graph variable into the inner
    /// subplan. True only when the inner patterns bind the graph var in EVERY
    /// solution (required top-level triple / property path / slice-free
    /// sub-SELECT — `self_produced_vars`), where seeding merely filters and is
    /// therefore equivalent to the SPARQL `{?g → graph}` join while strictly
    /// narrowing the inner scan. When false, the join is enforced at merge
    /// time instead, preserving `?g`-not-in-scope semantics for FILTER-only /
    /// OPTIONAL / UNION references (W3C graph-variable-scope).
    seed_graph_var: bool,
    /// The inner subplan being read, streaming its output batch by batch.
    stream: Option<InnerStream>,
}

/// One queued inner execution: a parent row's subplan in one graph.
struct GraphWork {
    row_idx: usize,
    graph_iri: Arc<str>,
    /// The graph variable to bind to `graph_iri` (an enumerated `GRAPH ?g`).
    bind_graph_var: Option<VarId>,
    /// A data-bound `GRAPH ?g` naming a graph source no query reads as a graph:
    /// a refusal of that class (`QueryError::is_unreadable_graph_source`) while
    /// opening means no rows for this binding, not a failed query.
    unreadable_is_empty: bool,
}

/// An inner subplan kept open across `next_batch` calls, so its rows stream
/// out as it produces them instead of being buffered whole (a graph source's
/// answer can be far larger than memory).
struct InnerStream {
    inner: BoxedOperator,
    graph_iri: Arc<str>,
    /// Provenance to stamp on each batch, for a multi-ledger dataset.
    stamp_ledger_id: Option<Arc<str>>,
    /// Whether the graph context also stamps scan output itself (the per-row
    /// path arms it; see [`GraphOperator::open_in_graph`]).
    arm_scan_provenance: bool,
    /// The parent row each output row starts with (the per-row path). `None`
    /// for the batched path, whose inner output carries the parent columns.
    parent_row: Option<Vec<Binding>>,
    /// The graph variable bound to `graph_iri` (per-row path, `GRAPH ?g`).
    bind_graph_var: Option<VarId>,
    /// How the bindings this scope produces leave it for the outer context,
    /// built once when the subplan opens.
    crossing: crate::object_binding::ScopeCrossing,
    /// Net bytes the subplan recorded against the query budget during its own
    /// calls, released when it is exhausted: its build tables drop with it.
    /// Measured inside each call, because the operators above run between
    /// calls and record their own allocations on the same counter.
    charged: isize,
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

        // Collected in pattern order, not through a set: the output schema is
        // part of this operator's contract, and a parent that resolved column
        // positions against the schema declared at plan time (see
        // `r2rml::sql_lane::SqlBlockOperator`) needs the batches to match it. A
        // `HashSet` here seeds a fresh `RandomState` per construction, so with
        // two or more new variables the order differed run to run.
        let mut new_vars: Vec<VarId> = Vec::new();
        let push_new = |v: VarId, out: &mut Vec<VarId>| {
            if !parent_schema.contains(&v) && !out.contains(&v) {
                out.push(v);
            }
        };
        for p in &inner_patterns {
            for v in p.produced_vars() {
                push_new(v, &mut new_vars);
            }
        }

        // If graph_name is a variable, it may be bound by this operator
        if let GraphName::Var(var) = &graph_name {
            push_new(*var, &mut new_vars);
        }

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
            parent_batch: None,
            work: std::collections::VecDeque::new(),
            planning,
            row_budget: None,
            topk: None,
            seed_graph_var,
            stream: None,
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
    ) -> Result<Option<Arc<str>>> {
        if let Some(iri) = binding.get_iri() {
            return Ok(Some(Arc::clone(iri)));
        }
        Ok(match binding {
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
            // A predicate IRI bound in predicate position (`ex:s1 ?g ?o .
            // GRAPH ?g { … }`).
            Binding::EncodedPid { p_id } => ctx
                .binary_store
                .as_ref()
                .and_then(|store| store.resolve_predicate_iri(*p_id))
                .map(Arc::from),
            Binding::EncodedSid { .. } | Binding::EncodedLit { .. } => {
                match crate::group_aggregate::materialize_encoded(
                    binding,
                    ctx.term_view().as_ref(),
                )? {
                    Binding::Sid { sid, .. } => ctx.active_snapshot.decode_sid(&sid).map(Arc::from),
                    Binding::Lit {
                        val: FlakeValue::String(s),
                        ..
                    } => Some(Arc::from(s.as_str())),
                    _ => None,
                }
            }
            _ => None,
        })
    }

    /// Execute inner patterns in a specific graph, seeded with parent row
    async fn open_in_graph(
        &mut self,
        ctx: &ExecutionContext<'_>,
        parent_batch: &Batch,
        row_idx: usize,
        graph_iri: Arc<str>,
        bind_graph_var: Option<VarId>,
    ) -> Result<()> {
        // Switch to the named graph context
        let mut graph_ctx = ctx.with_active_graph(graph_iri.clone());
        crate::fast_path_outcome::stamp_index_scope("graph", &graph_ctx);

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
        if let Some(ledger_id) = &stamp_ledger_id {
            graph_ctx.eager_materialization = true;
            // The scans inside already stamp (the dataset spans ledgers), but
            // a BIND-minted term, a VALUES cell, and a property path do not
            // unless the context arms it — and each of those lands next to
            // stamped scan output, where a raw `Sid` never unifies. Same
            // machinery as the cross-ledger SERVICE path.
            graph_ctx.scan_provenance_ledger = Some(Arc::clone(ledger_id));
        }

        // Check if this graph is backed by an R2RML mapping.
        // Prefer the precomputed set (populated in runner.rs from the query's
        // graphs), but fall back to asking the provider dynamically for the
        // no-dataset single-source path where the GRAPH IRI may differ from the
        // ledger_id.
        let is_r2rml_gs = if ctx.r2rml_graph_ids.contains(graph_iri.as_ref()) {
            true
        } else if ctx.single_db_user_graph_id(&graph_iri).is_some()
            || ctx.graph_is_native(&graph_iri)
        {
            // A registered graph, or a graph loaded as a ledger graph: never
            // R2RML, so skip the per-graph probe.
            false
        } else if let Some(provider) = ctx.r2rml_provider {
            provider.has_r2rml_mapping(&graph_iri).await?
        } else {
            false
        };

        // Determine which patterns to use (rewritten for R2RML or original)
        let patterns_to_execute: std::borrow::Cow<'_, [Pattern]> = if is_r2rml_gs {
            // Rewrite triple patterns to R2RML patterns
            let mapping = r2rml_mapping_for_rewrite(ctx, &graph_iri).await?;
            let rewrite_result = rewrite_patterns_for_r2rml_source(
                ctx,
                &crate::list_view::without_list_view(&self.inner_patterns),
                &graph_iri,
                mapping.as_deref(),
                ctx.trust_fk_refs,
            )
            .await?;

            // If there are unconverted patterns in an R2RML graph source, refuse
            // with a typed error. R2RML graph sources have no ledger-backed index,
            // so an unconverted pattern (a VARIABLE predicate paired with a BOUND
            // term) would silently return empty instead of the expected matches.
            // Fail explicitly with the stable `err:r2rml/UnsupportedPattern` code.
            if rewrite_result.unconverted_count > 0 {
                return Err(r2rml_unsupported_pattern_error(
                    &graph_iri,
                    rewrite_result.unconverted_count,
                ));
            }
            // Non-lowered sub-scope patterns (property/shortest paths,
            // subqueries) whose bodies would evaluate against the R2RML source's
            // empty native index and silently return no rows — fail loudly.
            if !rewrite_result.unsupported.is_empty() {
                return Err(unsupported_subscope_error(
                    &[&graph_iri],
                    &rewrite_result.unsupported,
                ));
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
        //
        // Bindings cross to and from this graph encoded (an arena literal
        // carries its graph), unless one side cannot decode them.
        let crossing = crate::object_binding::ScopeCrossing::between(ctx, &graph_ctx);
        let mut row: Vec<Binding> = parent_batch
            .row_view(row_idx)
            .expect("row_idx must be valid for batch")
            .to_vec();
        crossing.enter(&mut row)?;
        let seed = match bind_graph_var {
            Some(var) if self.seed_graph_var && !parent_batch.schema().contains(&var) => {
                let mut schema_vec = parent_batch.schema().to_vec();
                schema_vec.push(var);
                row.push(Binding::iri(graph_iri.clone()));
                if stamp_ledger_id.is_some() {
                    row = crate::dataset_operator::stamp_seed_row(row, ctx);
                }
                SeedOperator::from_row(Arc::from(schema_vec.into_boxed_slice()), row)
            }
            // Across a ledger boundary the parent's raw `Sid`s are stamped in
            // the requester's ledger before seeding (see `stamp_seed_row`).
            _ if stamp_ledger_id.is_some() => SeedOperator::from_row(
                Arc::from(parent_batch.schema().to_vec().into_boxed_slice()),
                crate::dataset_operator::stamp_seed_row(row, ctx),
            ),
            _ => SeedOperator::from_row(
                Arc::from(parent_batch.schema().to_vec().into_boxed_slice()),
                row,
            ),
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
        if let Some((ordering, k)) = &self.topk {
            inner.set_topk(ordering, *k);
        }
        // Rebuild-boundary memory accounting (D1): this correlated inner subplan is
        // rebuilt per parent row and is dropped when it is exhausted, so what it
        // records (hash-join / GROUP BY / fused-dim build tables) is provably freed
        // then and released in `next_streamed`. Without the release the counter
        // grows ~N× the true one-build peak across N rebuilds and false-aborts a
        // legitimate correlated query with a typed 507.
        let mem_before = graph_ctx.mem_used();
        inner.open(&graph_ctx).await?;
        let charged = graph_ctx.mem_used() as isize - mem_before as isize;
        let parent_row = self
            .child
            .schema()
            .iter()
            .map(|var| {
                parent_batch
                    .get(row_idx, *var)
                    .cloned()
                    .unwrap_or(Binding::Unbound)
            })
            .collect();
        self.stream = Some(InnerStream {
            inner,
            graph_iri,
            arm_scan_provenance: stamp_ledger_id.is_some(),
            stamp_ledger_id,
            parent_row: Some(parent_row),
            bind_graph_var,
            crossing,
            charged,
        });
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
    async fn open_graph_batched(
        &mut self,
        ctx: &ExecutionContext<'_>,
        parent_batch: &Batch,
        graph_iri: Arc<str>,
    ) -> Result<()> {
        let mut graph_ctx = ctx.with_active_graph(graph_iri.clone());
        crate::fast_path_outcome::stamp_index_scope("graph", &graph_ctx);

        let stamp_ledger_id: Option<Arc<str>> = match &ctx.dataset {
            Some(ds) if ds.spans_multiple_ledgers() => ds
                .named_graph(graph_iri.as_ref())
                .map(|g| Arc::clone(&g.ledger_id)),
            _ => None,
        };
        if stamp_ledger_id.is_some() {
            graph_ctx.eager_materialization = true;
        }

        let mapping = r2rml_mapping_for_rewrite(ctx, &graph_iri).await?;
        let rewrite_result = rewrite_patterns_for_r2rml_source(
            ctx,
            &crate::list_view::without_list_view(&self.inner_patterns),
            &graph_iri,
            mapping.as_deref(),
            ctx.trust_fk_refs,
        )
        .await?;
        if rewrite_result.unconverted_count > 0 {
            return Err(r2rml_unsupported_pattern_error(
                &graph_iri,
                rewrite_result.unconverted_count,
            ));
        }
        // Non-lowered sub-scope patterns (property/shortest paths, subqueries)
        // that would silently evaluate against the R2RML source's empty native
        // index — fail loudly rather than return a wrong empty result.
        if !rewrite_result.unsupported.is_empty() {
            return Err(unsupported_subscope_error(
                &[&graph_iri],
                &rewrite_result.unsupported,
            ));
        }

        // The parent batch seeds the source: encoded, unless the source scope
        // cannot decode what the outer context binds (an outer union of
        // graphs over a store the source scope does not read).
        let crossing = crate::object_binding::ScopeCrossing::between(ctx, &graph_ctx);
        let seed = BatchSeedOperator::from_batch(crossing.enter_batch(parent_batch)?);
        let mut inner = build_where_operators_seeded(
            Some(Box::new(seed)),
            &rewrite_result.patterns,
            None,
            None,
            &self.planning,
        )?;
        // Forward a downstream LIMIT budget so the inner scan early-terminates
        // instead of streaming the whole table out. Correctness
        // is bounded by the outer LIMIT; a per-parent-batch budget can over-read
        // across parent batches but never under-reads.
        if let Some(budget) = self.row_budget {
            inner.set_row_budget(budget);
        }
        if let Some((ordering, k)) = &self.topk {
            inner.set_topk(ordering, *k);
        }
        // Rebuild-boundary memory accounting (D1): this correlated inner subplan is
        // rebuilt per parent batch and is dropped when it is exhausted, so what it
        // records (hash-join / GROUP BY / fused-dim / parent-lookup builds) is
        // provably freed then and released in `close_batched`. Count only what it
        // records during its own calls: the operators above run between calls.
        let mem_before = graph_ctx.mem_used();
        inner.open(&graph_ctx).await?;
        let charged = graph_ctx.mem_used() as isize - mem_before as isize;
        self.stream = Some(InnerStream {
            inner,
            graph_iri,
            stamp_ledger_id,
            arm_scan_provenance: false,
            parent_row: None,
            bind_graph_var: None,
            crossing,
            charged,
        });
        Ok(())
    }

    /// The next non-empty batch of the open inner subplan, in this operator's
    /// schema, or `None` once it is exhausted (it is then closed and its charge
    /// released) or when none is open.
    async fn next_streamed(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        let parent_len = self.child.schema().len();
        loop {
            let Some(st) = self.stream.as_mut() else {
                return Ok(None);
            };
            let mut graph_ctx = ctx.with_active_graph(st.graph_iri.clone());
            if let Some(ledger_id) = &st.stamp_ledger_id {
                graph_ctx.eager_materialization = true;
                if st.arm_scan_provenance {
                    graph_ctx.scan_provenance_ledger = Some(Arc::clone(ledger_id));
                }
            }
            let mem_before = graph_ctx.mem_used();
            let next = st.inner.next_batch(&graph_ctx).await;
            st.charged += graph_ctx.mem_used() as isize - mem_before as isize;
            graph_ctx.check_cancelled()?;
            let Some(batch) = next? else {
                if let Some(mut st) = self.stream.take() {
                    st.inner.close();
                    ctx.release(st.charged.max(0) as usize);
                }
                return Ok(None);
            };
            if batch.is_empty() {
                continue;
            }
            // Stamp cross-ledger provenance before merging so the formatter
            // decodes SIDs against this graph's home ledger (see `open_in_graph`).
            let batch = match &st.stamp_ledger_id {
                Some(ledger_id) => {
                    crate::dataset_operator::stamp_provenance(batch, ledger_id, &graph_ctx)?
                }
                None => batch,
            };
            // Only the columns this scope binds leave through the crossing; the
            // parent columns keep the outer graph's handles.
            let crossing = &st.crossing;
            let out = match &st.parent_row {
                // Batched: the inner output already carries the parent columns
                // (threaded through the multi-row seed) plus the new vars.
                None => {
                    let rows = batch.len();
                    if self.schema.is_empty() {
                        return Ok(Some(Batch::empty_schema_with_len(rows)));
                    }
                    let columns = self
                        .schema
                        .iter()
                        .enumerate()
                        .map(|(i, var)| match batch.column(*var) {
                            Some(col) if i < parent_len => Ok(col.to_vec()),
                            Some(col) => col.iter().map(|b| crossing.leave(b.clone())).collect(),
                            None => Ok(vec![Binding::Unbound; rows]),
                        })
                        .collect::<Result<Vec<Vec<Binding>>>>()?;
                    Batch::new(self.schema.clone(), columns)?
                }
                // Per row: each inner solution joins the parent row.
                Some(parent_row) => {
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
                    let mut kept: Vec<usize> = Vec::with_capacity(batch.len());
                    for row in 0..batch.len() {
                        let keep = match st.bind_graph_var.and_then(|gvar| batch.get(row, gvar)) {
                            Some(Binding::Unbound | Binding::Poisoned) | None => true,
                            Some(b) => {
                                Self::extract_graph_iri_from_binding(&graph_ctx, b)?.as_deref()
                                    == Some(st.graph_iri.as_ref())
                            }
                        };
                        if keep {
                            kept.push(row);
                        }
                    }
                    if kept.is_empty() {
                        continue;
                    }
                    if self.schema.is_empty() {
                        return Ok(Some(Batch::empty_schema_with_len(kept.len())));
                    }
                    let columns = self
                        .schema
                        .iter()
                        .enumerate()
                        .map(|(i, var)| {
                            Ok(if i < parent_len {
                                // Copy the parent binding into every row.
                                vec![parent_row[i].clone(); kept.len()]
                            } else if st.bind_graph_var == Some(*var) {
                                // Bind ?g to the graph name as an IRI term (SPARQL
                                // requires an IRI, not a string literal). The inner
                                // subplan was seeded with the same value, so this is
                                // consistent with — not an overwrite of — any inner
                                // occurrence of the variable.
                                vec![Binding::iri(st.graph_iri.clone()); kept.len()]
                            } else {
                                match batch.column(*var) {
                                    Some(col) => kept
                                        .iter()
                                        .map(|&row| crossing.leave(col[row].clone()))
                                        .collect::<Result<Vec<Binding>>>()?,
                                    None => vec![Binding::Unbound; kept.len()],
                                }
                            })
                        })
                        .collect::<Result<Vec<Vec<Binding>>>>()?;
                    Batch::new(self.schema.clone(), columns)?
                }
            };
            return Ok(Some(out));
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

    fn set_topk(&mut self, ordering: &[SortSpec], k: usize) {
        // Record the top-k directive; threaded into the per-parent inner subplan
        // (like `row_budget`). NOT forwarded to `self.child` (the parent seed is
        // not the scan). Per-partition top-k is sound (see the field doc).
        self.topk = Some((ordering.to_vec(), k));
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        self.stream = None;
        self.work.clear();
        self.parent_batch = None;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        // Clone graph_name to avoid borrow conflicts when queueing executions
        let graph_name = self.graph_name.clone();

        loop {
            // The open inner subplan's next batch.
            if let Some(batch) = self.next_streamed(ctx).await? {
                return Ok(Some(batch));
            }
            // Then the next queued execution for the current parent batch.
            if let Some(work) = self.work.pop_front() {
                let parent_batch = self
                    .parent_batch
                    .take()
                    .expect("queued GRAPH work has its parent batch");
                let opened = self
                    .open_in_graph(
                        ctx,
                        &parent_batch,
                        work.row_idx,
                        work.graph_iri,
                        work.bind_graph_var,
                    )
                    .await;
                self.parent_batch = Some(parent_batch);
                match opened {
                    Err(e) if work.unreadable_is_empty && e.is_unreadable_graph_source() => {}
                    opened => opened?,
                }
                continue;
            }

            // Get next batch from child
            let parent_batch = match self.child.next_batch(ctx).await? {
                Some(b) if !b.is_empty() => b,
                Some(_) => continue, // Skip empty batches
                None => {
                    self.state = OperatorState::Exhausted;
                    return Ok(None);
                }
            };

            // Fast path: a concrete-IRI R2RML graph source in single-db mode.
            // Run the whole parent batch through ONE uncorrelated scan so the
            // inner R2RML hash join joins all parent rows at once, instead of
            // re-scanning the table per parent row.
            if ctx.explicit_dataset().is_none() {
                if let GraphName::Iri(iri) = &graph_name {
                    let is_user_graph = ctx.single_db_user_graph_id(iri).is_some();
                    let is_alias = ctx.single_db_names_default_graph(iri);
                    let is_r2rml_gs = !is_user_graph
                        && !is_alias
                        && if ctx.r2rml_graph_ids.contains(iri.as_ref()) {
                            true
                        } else if let Some(provider) = ctx.r2rml_provider {
                            provider.has_r2rml_mapping(iri).await?
                        } else {
                            false
                        };
                    if is_r2rml_gs {
                        self.open_graph_batched(ctx, &parent_batch, iri.clone())
                            .await?;
                        continue;
                    }
                }
            }

            // Queue each parent row's executions (run one at a time, streaming).
            let mut work: std::collections::VecDeque<GraphWork> = std::collections::VecDeque::new();
            for row_idx in 0..parent_batch.len() {
                match &graph_name {
                    GraphName::Iri(iri) => {
                        // Concrete graph: run inner patterns in that graph
                        // If graph doesn't exist in dataset → empty result
                        if let Some(ds) = ctx.explicit_dataset() {
                            if ds.has_named_graph(iri) {
                                work.push_back(GraphWork {
                                    row_idx,
                                    graph_iri: iri.clone(),
                                    bind_graph_var: None,
                                    unreadable_is_empty: false,
                                });
                            }
                            // else: graph not found → no output for this row
                        } else {
                            // Single-db: a registered user graph, a name of
                            // the default graph (`single_db_names_default_graph`),
                            // or an R2RML graph source.
                            ctx.refuse_dropped_graph(iri)?;
                            let is_user_graph = ctx.single_db_user_graph_id(iri).is_some();
                            let is_alias = ctx.single_db_names_default_graph(iri);
                            let is_r2rml_gs = !is_user_graph
                                && !is_alias
                                && if ctx.r2rml_graph_ids.contains(iri.as_ref()) {
                                    true
                                } else if let Some(provider) = ctx.r2rml_provider {
                                    provider.has_r2rml_mapping(iri).await?
                                } else {
                                    false
                                };

                            if is_user_graph || is_alias || is_r2rml_gs {
                                work.push_back(GraphWork {
                                    row_idx,
                                    graph_iri: iri.clone(),
                                    bind_graph_var: None,
                                    unreadable_is_empty: false,
                                });
                            }
                        }
                    }
                    GraphName::Var(var) => {
                        // Check if ?g is already bound in parent row
                        if let Some(binding) = parent_batch.get(row_idx, *var) {
                            if let Some(bound_iri) =
                                Self::extract_graph_iri_from_binding(ctx, binding)?
                            {
                                // ?g already bound: use only that graph
                                if let Some(ds) = ctx.explicit_dataset() {
                                    if ds.has_named_graph(&bound_iri) {
                                        work.push_back(GraphWork {
                                            row_idx,
                                            graph_iri: bound_iri,
                                            bind_graph_var: None,
                                            unreadable_is_empty: false,
                                        });
                                    }
                                    // else: graph not found → no output
                                } else {
                                    // Single-db: same resolution as the concrete arm,
                                    // except for an IRI the data supplied that names a
                                    // graph source no query reads as a graph. A
                                    // constant naming it is refused; here it is a graph
                                    // outside the dataset, so this binding yields no
                                    // rows (SPARQL's answer) and the query goes on.
                                    let is_user_graph =
                                        ctx.single_db_user_graph_id(&bound_iri).is_some();
                                    let is_alias = ctx.single_db_names_default_graph(&bound_iri);
                                    let is_r2rml_gs = !is_user_graph
                                        && !is_alias
                                        && if ctx.r2rml_graph_ids.contains(bound_iri.as_ref()) {
                                            true
                                        } else if let Some(provider) = ctx.r2rml_provider {
                                            match provider.has_r2rml_mapping(&bound_iri).await {
                                                Err(e) if e.is_unreadable_graph_source() => false,
                                                probed => probed?,
                                            }
                                        } else {
                                            false
                                        };

                                    if is_user_graph || is_alias || is_r2rml_gs {
                                        work.push_back(GraphWork {
                                            row_idx,
                                            graph_iri: bound_iri,
                                            bind_graph_var: None,
                                            // An empty mapping surfaces only when the
                                            // rewrite compiles it, as the work opens.
                                            unreadable_is_empty: is_r2rml_gs,
                                        });
                                    }
                                }
                            }
                            // else: binding exists but isn't a string IRI → no output
                        } else {
                            // ?g unbound: iterate ALL named graphs, bind ?g
                            if let Some(ds) = ctx.explicit_dataset() {
                                for iri in ds.named_graph_iris() {
                                    work.push_back(GraphWork {
                                        row_idx,
                                        graph_iri: iri,
                                        bind_graph_var: Some(*var),
                                        unreadable_is_empty: false,
                                    });
                                }
                            } else {
                                // Single-db: bind ?g to each registered user
                                // graph (empty graphs emit no rows), under a
                                // name that reads it back. The default graph is
                                // NOT enumerated: per SPARQL 1.1, `GRAPH ?g`
                                // ranges over named graphs only (issue
                                // #1442 dropped the #1279 implicit
                                // enumeration). It remains explicitly
                                // addressable by the ledger's own address in
                                // the arms above.
                                for iri in ctx.single_db_user_graph_iris() {
                                    work.push_back(GraphWork {
                                        row_idx,
                                        graph_iri: iri,
                                        bind_graph_var: Some(*var),
                                        unreadable_is_empty: false,
                                    });
                                }
                            }
                        }
                    }
                }
            }

            if !work.is_empty() {
                self.work = work;
                self.parent_batch = Some(parent_batch);
            }
        }
    }

    fn close(&mut self) {
        if let Some(mut st) = self.stream.take() {
            // Closed before its stream ran out (a LIMIT above was met): the
            // subplan's tables drop here. The query is ending, so its charge is
            // left with the handle.
            st.inner.close();
        }
        self.work.clear();
        self.parent_batch = None;
        self.child.close();
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

    /// The output schema is part of this operator's contract: `SqlBlockOperator`
    /// declares the same schema at plan time and hands its parent column
    /// positions resolved against it, so a mismatch is an error there rather
    /// than something to permute around. Deriving `new_vars` from a `HashSet`
    /// made the order differ run to run once a block bound two or more new
    /// variables — the existing schema tests use `contains`, so they could not
    /// see it. Assert the exact order, and repeatedly: a fresh `RandomState`
    /// per construction means one sample proves nothing.
    #[test]
    fn new_vars_follow_pattern_order_not_set_order() {
        let inner = |s: VarId, p: &str, o: VarId| {
            Pattern::Triple(TriplePattern::new(
                Ref::Var(s),
                Ref::Sid(Sid::new(100, p)),
                Term::Var(o),
            ))
        };

        for _ in 0..64 {
            let child: BoxedOperator = Box::new(TestChildOperator {
                schema: Arc::from(vec![VarId(0)].into_boxed_slice()),
            });
            // Five new variables: 1/5! chance of matching by luck per run.
            let patterns = vec![
                inner(VarId(0), "a", VarId(4)),
                inner(VarId(0), "b", VarId(2)),
                inner(VarId(0), "c", VarId(5)),
                inner(VarId(0), "d", VarId(1)),
                // A repeat must not re-add VarId(2), and VarId(0) is already
                // in the parent schema.
                inner(VarId(0), "e", VarId(2)),
                inner(VarId(0), "f", VarId(3)),
            ];
            let op = GraphOperator::new(
                child,
                GraphName::Iri(Arc::from("http://example.org/g")),
                patterns,
                crate::temporal_mode::PlanningContext::current(),
            );
            assert_eq!(
                op.schema(),
                &[VarId(0), VarId(4), VarId(2), VarId(5), VarId(1), VarId(3)],
                "parent schema then inner-produced vars in pattern order"
            );
        }
    }

    /// The graph variable is appended after the pattern-produced vars, and only
    /// when the patterns did not already bind it.
    #[test]
    fn graph_var_is_appended_deterministically() {
        for _ in 0..32 {
            let child: BoxedOperator = Box::new(TestChildOperator {
                schema: Arc::from(vec![VarId(0)].into_boxed_slice()),
            });
            let op = GraphOperator::new(
                child,
                GraphName::Var(VarId(9)),
                vec![
                    Pattern::Triple(TriplePattern::new(
                        Ref::Var(VarId(0)),
                        Ref::Sid(Sid::new(100, "name")),
                        Term::Var(VarId(2)),
                    )),
                    Pattern::Triple(TriplePattern::new(
                        Ref::Var(VarId(0)),
                        Ref::Sid(Sid::new(100, "age")),
                        Term::Var(VarId(1)),
                    )),
                ],
                crate::temporal_mode::PlanningContext::current(),
            );
            assert_eq!(op.schema(), &[VarId(0), VarId(2), VarId(1), VarId(9)]);
        }
    }

    #[test]
    fn test_extract_graph_iri_from_binding() {
        let snapshot = LedgerSnapshot::genesis("test:main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        // Raw IRI binding — the idiomatic `VALUES ?g { <iri> }` form
        let binding = Binding::Iri(Arc::from("urn:probegraph"));
        assert_eq!(
            GraphOperator::extract_graph_iri_from_binding(&ctx, &binding).unwrap(),
            Some(Arc::from("urn:probegraph"))
        );

        // Cross-ledger IriMatch carries the canonical IRI
        let binding = Binding::iri_match("urn:other", Sid::new(2, "x"), "other:main");
        assert_eq!(
            GraphOperator::extract_graph_iri_from_binding(&ctx, &binding).unwrap(),
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
            GraphOperator::extract_graph_iri_from_binding(&ctx, &binding).unwrap(),
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
            GraphOperator::extract_graph_iri_from_binding(&ctx, &binding).unwrap(),
            None
        );
        assert_eq!(
            GraphOperator::extract_graph_iri_from_binding(&ctx, &Binding::Unbound).unwrap(),
            None
        );
    }
}
