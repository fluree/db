//! Query execution against GraphDb
//!
//! Provides `query` and related methods that execute queries against
//! a GraphDb, respecting policy and reasoning wrappers.

use crate::query::helpers::{
    build_query_result, charge_query_floor, lower_sparql_ast, parse_and_validate_sparql,
    parse_cypher_to_ir, parse_jsonld_query, prepare_for_execution, sparql_ast_has_dataset,
    status_for_query_error, tracked_query_tracker, tracker_for_limits,
};
use crate::view::{DataSetDb, GraphDb, QueryInput};
use crate::{
    ApiError, ExecutableQuery, Fluree, QueryExecutionOptions, QueryResult, Result, Tracker,
    TrackingOptions,
};
use fluree_db_query::execute::{
    execute_prepared, prepare_execution_with_config, ContextConfig, PrepareConfig,
};
use fluree_db_query::ir::{GraphName, Pattern};
use fluree_db_query::r2rml::{R2rmlProvider, R2rmlTableProvider};
use serde_json::Value as JsonValue;

/// If the view was created from a graph source, wrap all top-level patterns
/// in `GRAPH <gs_id> { ... }` so the R2RML provider handles them.
///
/// Skips wrapping if the query already contains a top-level GRAPH pattern
/// (the user explicitly scoped it). Whatever is left outside a `GRAPH` scope
/// after this runs is covered by [`guard_graph_source_patterns`].
pub(crate) fn maybe_wrap_for_graph_source(db: &GraphDb, parsed: &mut fluree_db_query::ir::Query) {
    if let Some(ref gs_id) = db.graph_source_id {
        let has_graph_pattern = parsed
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Graph { .. }));
        if !has_graph_pattern {
            let inner = std::mem::take(&mut parsed.patterns);
            parsed.patterns = vec![Pattern::Graph {
                name: GraphName::Iri(gs_id.to_string().into()),
                patterns: inner,
            }];
        }
    }
}

/// Refuse the patterns a graph source cannot evaluate, on the route where the
/// per-scope guard inside `GraphOperator` can never see them.
///
/// A graph-source view is a `LedgerSnapshot::genesis` — zero flakes — because
/// its data lives behind a provider that only the `GRAPH <gs_id>` execution path
/// consults. `maybe_wrap_for_graph_source` puts the query on that path, but it
/// bails out for the *whole* query when the user wrote any `GRAPH` block of
/// their own, leaving the remaining top-level patterns to read the empty native
/// index. Property paths, shortest paths, and subqueries are the patterns the
/// R2RML rewrite cannot lower, so out there they answer with silently-wrong
/// results — `p+` with nothing at all, `p*`/`p?` with the zero-length identity
/// match only — as HTTP 200. This check makes the refusal independent of the
/// query's `GRAPH` structure.
///
/// A plain triple stranded out there is refused too, by
/// [`unroutable_top_level_error`]. It is scoped deliberately to triples in
/// *conjunctive* top-level position, where an empty match provably zeroes the
/// whole result and no query can be returning correct rows today. It does not
/// descend into `OPTIONAL`, `MINUS`, `UNION`, or `EXISTS` bodies: those degrade
/// rather than zero (a vacuous `OPTIONAL` still emits its left side, a vacuous
/// `MINUS` excludes nothing), and a `UNION` branch may carry a nested `GRAPH`
/// block that routes and contributes real rows — so refusing there would reject
/// queries that answer today. Those shapes stay silently degraded pending the
/// routing work; see the graph-source docs.
///
/// Call it *after* the wrap: everything the wrap moved into the graph scope is
/// then already covered by the rewrite guard, so a query with no `GRAPH` block
/// of its own keeps refusing on exactly the route it refuses on today, with the
/// same message. It also means a surviving top-level triple is itself proof the
/// wrap declined — no separate flag needed.
pub(crate) fn guard_graph_source_patterns(
    db: &GraphDb,
    parsed: &fluree_db_query::ir::Query,
    syntax: QuerySyntax,
) -> Result<()> {
    let Some(gs_id) = db.graph_source_id.as_deref() else {
        return Ok(());
    };
    guard_graph_source_patterns_for(&[gs_id], parsed, syntax)
}

/// [`guard_graph_source_patterns`] over an explicit source list, so a dataset
/// can name every graph source it is querying rather than just the primary's.
fn guard_graph_source_patterns_for(
    gs_ids: &[&str],
    parsed: &fluree_db_query::ir::Query,
    syntax: QuerySyntax,
) -> Result<()> {
    let unsupported = fluree_db_query::r2rml::unsupported_outside_graph_scopes(&parsed.patterns);
    if !unsupported.is_empty() {
        return Err(ApiError::Query(
            fluree_db_query::r2rml::unsupported_subscope_error(gs_ids, &unsupported),
        ));
    }
    let unroutable = parsed
        .patterns
        .iter()
        .filter(|p| matches!(p, Pattern::Triple(_)))
        .count();
    if unroutable > 0 {
        return Err(ApiError::Query(unroutable_top_level_error(
            gs_ids, unroutable, syntax,
        )));
    }
    Ok(())
}

/// Which surface a query arrived on, so a refusal can name a scoping construct
/// the user could actually have typed.
///
/// The three surfaces spell graph scoping differently, and one of them cannot
/// spell it at all — see [`unroutable_top_level_error`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum QuerySyntax {
    Sparql,
    JsonLd,
    Cypher,
}

impl QuerySyntax {
    pub(crate) fn of(input: &QueryInput<'_>) -> Self {
        match input {
            QueryInput::Sparql(_) => Self::Sparql,
            QueryInput::JsonLd(_) => Self::JsonLd,
        }
    }
}

/// The refusal for a triple pattern the wrap left stranded at the top level of a
/// graph-source query.
///
/// Unlike the property-path family this is *lowerable* — it would convert to an
/// R2RML scan perfectly well — but only inside a graph scope, and on this plan
/// it is not in one. "Lowerable in principle" is no comfort to a user whose
/// pattern will not be lowered on the plan actually running, so it refuses for
/// the same reason: it would otherwise read the graph source's empty native
/// index and return no rows as a success.
///
/// The wording names the workaround rather than the internals, because both
/// fixes are entirely in the user's hands — which means it has to be spelled in
/// the language the user is actually writing. SPARQL scopes with
/// `GRAPH <iri> { … }` and JSON-LD with a `"graph"` object; telling a JSON-LD
/// user to write SPARQL is the same defect as telling them nothing.
///
/// The `Cypher` arm is defensive, not reachable today: Cypher lowering emits no
/// `Pattern::Graph`, so `maybe_wrap_for_graph_source` always wraps a Cypher
/// query whole and never leaves a top-level triple behind (pinned by
/// `cypher_over_a_graph_source_cannot_reach_the_graph_block_advice`). If Cypher
/// ever gains a graph-scoping construct, this must stop claiming there is none.
///
/// With several sources in play the "drop the explicit block" escape is omitted
/// deliberately: dropping it would scope the whole query to one source and
/// silently drop the rest, which is not what the user asked for.
fn unroutable_top_level_error(
    gs_ids: &[&str],
    count: usize,
    syntax: QuerySyntax,
) -> fluree_db_query::QueryError {
    let names = gs_ids
        .iter()
        .map(|id| format!("'{id}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let subject = if gs_ids.len() == 1 {
        format!("graph source {names}")
    } else {
        format!("graph sources {names}")
    };
    let scope_syntax = match syntax {
        QuerySyntax::Sparql => "a `GRAPH <source> { … }` block",
        QuerySyntax::JsonLd => "a `{\"graph\": \"source\", \"where\": [ … ]}` block",
        QuerySyntax::Cypher => "a graph-scoping block",
    };
    let advice = match (syntax, gs_ids.len()) {
        // Cypher has no way to scope a pattern to one graph, so there is no
        // rewrite to suggest — only a different way to address the source.
        (QuerySyntax::Cypher, _) => {
            "Cypher has no graph-scoping syntax, so these patterns cannot be routed from a \
             query that also addresses another graph — query the graph source on its own."
                .to_string()
        }
        (_, 1) => format!(
            "Move them inside {scope_syntax} naming {names}, or drop the explicit block so \
             the whole query is scoped to the graph source."
        ),
        (_, _) => format!(
            "Move each pattern inside {scope_syntax} naming the source it belongs to \
             ({names}). Dropping the explicit block is not an option here: it would scope \
             the whole query to one source and silently exclude the others."
        ),
    };
    fluree_db_query::QueryError::InvalidQuery(format!(
        "{subject} cannot evaluate {count} top-level triple pattern(s) in this query. \
         Patterns reach a graph source only inside {scope_syntax}; Fluree adds that block \
         automatically, but not to a query that already scopes a block of its own — so these \
         patterns would read an empty index and silently return no rows. {advice}"
    ))
}

/// [`guard_graph_source_patterns`] for a dataset query.
///
/// Gated on *every* default graph being a graph source. A dataset that mixes a
/// graph source with a native ledger (`from: ["sales:main", "warehouse-gs"]`)
/// evaluates its top-level patterns over the union of both, so a property path
/// out there has a real index to traverse and refusing it would reject a query
/// that works. With no native member in the union there is no such index, and
/// the single-view reasoning applies unchanged.
///
/// Every default graph's source id is passed to the refusal, not just the
/// primary's. The message tells the user where to move a stranded pattern, and
/// a message naming one id out of several would be advice to silently exclude
/// the rest.
///
/// # Empty default set
///
/// An empty `default` returns `Ok` — the query is not refused. This is the
/// `fromNamed`-only shape, and post-#1631 (which stopped injecting the ledger as
/// a default graph for such bodies) it is the *correct* answer rather than a gap
/// in the gate. SPARQL §13.2 gives a `FROM NAMED`-only query an empty default
/// graph, so its top-level patterns are supposed to match nothing: an empty
/// result is the specified answer, not the silent-wrong one this guard exists to
/// catch. Refusing here would reject a conformant query. Pinned by
/// `a_named_only_body_leaves_the_default_graph_empty` and its end-to-end
/// companion `a_named_only_graph_source_query_is_answered_not_refused`.
pub(crate) fn guard_dataset_graph_source_patterns(
    dataset: &DataSetDb,
    parsed: &fluree_db_query::ir::Query,
    syntax: QuerySyntax,
) -> Result<()> {
    if dataset.default.is_empty() {
        return Ok(());
    }
    let mut gs_ids = Vec::with_capacity(dataset.default.len());
    for view in &dataset.default {
        match view.graph_source_id.as_deref() {
            Some(id) => {
                if !gs_ids.contains(&id) {
                    gs_ids.push(id);
                }
            }
            // A native member can answer top-level patterns — see above.
            None => return Ok(()),
        }
    }
    guard_graph_source_patterns_for(&gs_ids, parsed, syntax)
}

// ============================================================================
// Query Execution
// ============================================================================

impl Fluree {
    /// Execute a query against a GraphDb.
    ///
    /// Accepts JSON-LD or SPARQL via `QueryInput`. Wrapper settings
    /// (policy, reasoning) are applied automatically.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use serde_json::json;
    ///
    /// let db = fluree.db("mydb:main").await?
    ///     .with_reasoning(ReasoningModes::owl2ql());
    ///
    /// // JSON-LD query
    /// let query = json!({"select": ["?s"], "where": [["?s", "?p", "?o"]]});
    /// let result = fluree.query(&db, &query).await?;
    ///
    /// // SPARQL query
    /// let result = fluree.query(&db, "SELECT * WHERE { ?s ?p ?o }").await?;
    /// ```
    ///
    /// # SPARQL Dataset Clauses (`FROM` / `FROM NAMED`)
    ///
    /// A `GraphDb` is one ledger, but a `FROM` / `FROM NAMED` clause whose IRIs
    /// name graphs *within this ledger* (the ledger alias addresses the default
    /// graph; registered named-graph IRIs address named graphs) builds a
    /// within-ledger dataset over the one snapshot and runs through the dataset
    /// execution path. A clause IRI that is not a graph in this ledger is
    /// rejected — use `query_connection_sparql` for cross-ledger datasets.
    pub async fn query(&self, db: &GraphDb, q: impl Into<QueryInput<'_>>) -> Result<QueryResult> {
        self.query_with_options(db, q, QueryExecutionOptions::default())
            .await
    }

    /// Execute a query against a GraphDb with explicit execution controls.
    pub async fn query_with_options(
        &self,
        db: &GraphDb,
        q: impl Into<QueryInput<'_>>,
        options: QueryExecutionOptions,
    ) -> Result<QueryResult> {
        let input = q.into();

        // #1473: parse SPARQL once. The AST is reused for FROM-clause resolution
        // and IR lowering below, instead of re-lexing the string 2–3×.
        let parse_start = fluree_db_core::clock::Instant::now();
        let mut sparql_ast = match input {
            QueryInput::Sparql(sparql) => Some(parse_and_validate_sparql(sparql)?),
            QueryInput::JsonLd(_) => None,
        };

        // A SPARQL FROM/FROM NAMED clause whose IRIs name graphs within this
        // ledger builds a within-ledger dataset and routes through the shared
        // dataset execution path (D-3, Option A). `Err` = a clause IRI is not
        // in this ledger (cross-ledger → connection path).
        let within_ledger_dataset = match &sparql_ast {
            Some(ast) => self.build_within_ledger_dataset_from_ast(db, ast)?,
            None => None,
        };
        if let Some(dataset) = within_ledger_dataset {
            // query_dataset_with_prepared_ast returns a concrete boxed future
            // (`-> Pin<Box<dyn Future + Send>>`), which breaks the
            // mutual-recursion `Send` auto-trait cycle between these two methods
            // and keeps this crate's type-check fast. The already-parsed AST is
            // moved in so the dataset path does not re-parse.
            let ast = sparql_ast
                .take()
                .expect("SPARQL input carries a parsed AST");
            return self
                .query_dataset_with_prepared_ast(&dataset, input, options, ast)
                .await;
        }

        // 0. Tracker for fuel limits only (no tracking overhead for non-tracked
        // calls). Charge the floor up front so a sub-floor `max-fuel` is
        // rejected before we spend parse/plan work; no-op when fuel isn't tracked.
        let tracker = match &input {
            QueryInput::JsonLd(json) => tracker_for_limits(json),
            QueryInput::Sparql(_) => Tracker::disabled(),
        };
        charge_query_floor(&tracker).map_err(fluree_db_query::QueryError::from)?;

        // 1. Lower to common IR (SPARQL reuses the AST parsed above).
        let (vars, mut parsed) = match &input {
            QueryInput::JsonLd(json) => {
                parse_jsonld_query(json, &db.snapshot, db.default_context.as_ref(), None)?
            }
            QueryInput::Sparql(sparql) => {
                let ast = sparql_ast
                    .take()
                    .expect("SPARQL input carries a parsed AST");
                lower_sparql_ast(ast, &db.snapshot, db.default_context.as_ref(), sparql)?
            }
        };
        let parse_ms = parse_start.elapsed().as_secs_f64() * 1000.0;

        // 1b. Auto-wrap for graph source context, then refuse whatever the wrap
        // could not put on the provider's path.
        maybe_wrap_for_graph_source(db, &mut parsed);
        guard_graph_source_patterns(db, &parsed, QuerySyntax::of(&input))?;

        // 2. Build executable with optional reasoning override
        let plan_start = fluree_db_core::clock::Instant::now();
        let executable = self.build_executable_for_view(db, &parsed).await?;
        let plan_ms = plan_start.elapsed().as_secs_f64() * 1000.0;

        // 4. Execute
        let exec_start = fluree_db_core::clock::Instant::now();
        let batches = self
            .execute_view_internal(db, &vars, &executable, &tracker, &options)
            .await?;
        let exec_ms = exec_start.elapsed().as_secs_f64() * 1000.0;

        tracing::info!(
            parse_ms = format!("{:.2}", parse_ms),
            plan_ms = format!("{:.2}", plan_ms),
            exec_ms = format!("{:.2}", exec_ms),
            "query phases"
        );

        // 5. Build result
        Ok(build_query_result(
            vars,
            parsed,
            batches,
            Some(db.t),
            Some(db.overlay.clone()),
            db.binary_graph(),
        ))
    }

    /// Execute a Cypher (openCypher 9 subset) query against a GraphDb.
    ///
    /// Mirror of [`Self::query`] for Cypher syntax. Parsing happens via
    /// `fluree_db_cypher::parse_cypher`; lowering produces the same
    /// shared `Query` IR that JSON-LD and SPARQL use. Executor + planner
    /// + result formatting are reused unchanged.
    ///
    /// The ledger's default context (if any) supplies `@vocab` and bare-
    /// identifier overrides; see `parse_cypher_to_ir` for the resolution
    /// rules.
    pub async fn query_cypher(&self, db: &GraphDb, cypher: &str) -> Result<QueryResult> {
        self.query_cypher_with_params(db, cypher, None).await
    }

    /// Like [`query_cypher`](Self::query_cypher) but substitutes `$param`
    /// references from `params` (a JSON map of name → value) before lowering.
    pub async fn query_cypher_with_params(
        &self,
        db: &GraphDb,
        cypher: &str,
        params: Option<&fluree_db_cypher::ParamMap>,
    ) -> Result<QueryResult> {
        let parse_start = fluree_db_core::clock::Instant::now();
        let (vars, mut parsed) = parse_cypher_to_ir(
            cypher,
            &db.snapshot,
            db.default_context.as_ref(),
            params,
            Some((&*db.overlay, db.graph_id)),
            db.policy_enforcer().map(|e| &**e),
        )?;
        let parse_ms = parse_start.elapsed().as_secs_f64() * 1000.0;

        maybe_wrap_for_graph_source(db, &mut parsed);
        guard_graph_source_patterns(db, &parsed, QuerySyntax::Cypher)?;

        self.execute_cypher_ir(db, vars, parsed, parse_ms).await
    }

    /// Execute an already-constructed Cypher read AST. Used by the
    /// conditional-write probes, which build read ASTs in code rather than
    /// from text.
    pub async fn query_cypher_ast(
        &self,
        db: &GraphDb,
        ast: &fluree_db_cypher::CypherAst,
    ) -> Result<QueryResult> {
        let (vars, mut parsed) = crate::query::helpers::lower_cypher_ast_to_ir(
            ast,
            &db.snapshot,
            db.default_context.as_ref(),
            Some((&*db.overlay, db.graph_id)),
            db.policy_enforcer().map(|e| &**e),
        )?;
        maybe_wrap_for_graph_source(db, &mut parsed);
        guard_graph_source_patterns(db, &parsed, QuerySyntax::Cypher)?;
        self.execute_cypher_ir(db, vars, parsed, 0.0).await
    }

    /// Execute a constructed Cypher read AST whose leading `InlineRows`
    /// clause is a placeholder for caller-supplied binding rows: after
    /// lowering, the placeholder `Values` pattern's rows are replaced
    /// wholesale with `rows` (one cell per `seed_cols` column, in order).
    ///
    /// The sequential write driver uses this to join a threaded row table —
    /// entities as already-resolved `Binding`s — against patterns lowered
    /// with the full context/vocab machinery.
    pub(crate) async fn query_cypher_ast_seeded(
        &self,
        db: &GraphDb,
        ast: &fluree_db_cypher::CypherAst,
        seed_cols: &[String],
        rows: Vec<Vec<fluree_db_query::Binding>>,
    ) -> Result<QueryResult> {
        let (vars, mut parsed) = crate::query::helpers::lower_cypher_ast_to_ir(
            ast,
            &db.snapshot,
            db.default_context.as_ref(),
            Some((&*db.overlay, db.graph_id)),
            db.policy_enforcer().map(|e| &**e),
        )?;

        // The placeholder lowers to the first pattern: a `Values` block over
        // exactly the seed columns. Anything else means the constructed AST
        // and this swap fell out of sync — an internal invariant, not user
        // input.
        let expected: Vec<fluree_db_query::VarId> = seed_cols
            .iter()
            .map(|name| {
                vars.get(name).ok_or_else(|| {
                    crate::ApiError::internal(format!(
                        "seeded Cypher query: column `{name}` was not interned by lowering"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        match parsed.patterns.first_mut() {
            Some(fluree_db_query::ir::Pattern::Values {
                vars: seed_vars,
                rows: seed_rows,
            }) if *seed_vars == expected => {
                for row in &rows {
                    if row.len() != expected.len() {
                        return Err(crate::ApiError::internal(
                            "seeded Cypher query: row width does not match seed columns",
                        ));
                    }
                }
                *seed_rows = rows;
            }
            other => {
                return Err(crate::ApiError::internal(format!(
                    "seeded Cypher query: lowering did not produce the placeholder VALUES \
                     pattern first: got {other:?}, expected vars {expected:?}",
                )));
            }
        }

        maybe_wrap_for_graph_source(db, &mut parsed);
        guard_graph_source_patterns(db, &parsed, QuerySyntax::Cypher)?;
        self.execute_cypher_ir(db, vars, parsed, 0.0).await
    }

    async fn execute_cypher_ir(
        &self,
        db: &GraphDb,
        vars: crate::VarRegistry,
        parsed: fluree_db_query::ir::Query,
        parse_ms: f64,
    ) -> Result<QueryResult> {
        let plan_start = fluree_db_core::clock::Instant::now();
        let executable = self.build_executable_for_view(db, &parsed).await?;
        let plan_ms = plan_start.elapsed().as_secs_f64() * 1000.0;

        let tracker = Tracker::disabled();
        let options = QueryExecutionOptions::default();
        let exec_start = fluree_db_core::clock::Instant::now();
        let batches = self
            .execute_view_internal(db, &vars, &executable, &tracker, &options)
            .await?;
        let exec_ms = exec_start.elapsed().as_secs_f64() * 1000.0;

        tracing::info!(
            parse_ms = format!("{:.2}", parse_ms),
            plan_ms = format!("{:.2}", plan_ms),
            exec_ms = format!("{:.2}", exec_ms),
            "cypher query phases"
        );

        Ok(build_query_result(
            vars,
            parsed,
            batches,
            Some(db.t),
            Some(db.overlay.clone()),
            db.binary_graph(),
        ))
    }

    /// Execute a query against a GraphDb with explicit R2RML providers.
    ///
    /// This is used by connection query paths (and builders) that need to resolve
    /// graph sources via R2RML/Iceberg while still running against a ledger-backed
    /// planning database.
    pub(crate) async fn query_view_with_r2rml_options(
        &self,
        db: &GraphDb,
        q: impl Into<QueryInput<'_>>,
        r2rml_provider: &dyn R2rmlProvider,
        r2rml_table_provider: &dyn R2rmlTableProvider,
        options: QueryExecutionOptions,
    ) -> Result<QueryResult> {
        let input = q.into();

        // #1473: parse SPARQL once. R2RML + within-ledger SPARQL FROM/FROM NAMED
        // is a deliberately unsupported combination (the R2RML dataset execution
        // path is not `Send`, but this method runs from `Send`-required
        // connection/spawn contexts), so a dataset clause is rejected here rather
        // than silently ignored; plain within-ledger datasets are handled by
        // `query_with_options`. Otherwise the AST is reused for lowering.
        let mut sparql_ast = match input {
            QueryInput::Sparql(sparql) => {
                let ast = parse_and_validate_sparql(sparql)?;
                if sparql_ast_has_dataset(&ast) {
                    return Err(single_ledger_dataset_clause_error());
                }
                Some(ast)
            }
            QueryInput::JsonLd(_) => None,
        };

        // 0. Tracker (fuel limits only). Charge the floor up front so a
        // sub-floor `max-fuel` is rejected before parse/plan; no-op untracked.
        let tracker = match &input {
            QueryInput::JsonLd(json) => tracker_for_limits(json),
            QueryInput::Sparql(_) => Tracker::disabled(),
        };
        charge_query_floor(&tracker).map_err(fluree_db_query::QueryError::from)?;

        // 1. Lower to common IR (SPARQL reuses the AST parsed above).
        let (vars, mut parsed) = match &input {
            QueryInput::JsonLd(json) => {
                parse_jsonld_query(json, &db.snapshot, db.default_context.as_ref(), None)?
            }
            QueryInput::Sparql(sparql) => {
                let ast = sparql_ast
                    .take()
                    .expect("SPARQL input carries a parsed AST");
                lower_sparql_ast(ast, &db.snapshot, db.default_context.as_ref(), sparql)?
            }
        };

        // 1b. Auto-wrap for graph source context, then refuse whatever the wrap
        // could not put on the provider's path.
        maybe_wrap_for_graph_source(db, &mut parsed);
        guard_graph_source_patterns(db, &parsed, QuerySyntax::of(&input))?;

        // 2. Build executable with optional reasoning override
        let executable = self.build_executable_for_view(db, &parsed).await?;

        // 4. Execute
        let batches = self
            .execute_view_internal_with_r2rml(
                db,
                &vars,
                &executable,
                &tracker,
                crate::R2rmlProviders {
                    provider: r2rml_provider,
                    table_provider: r2rml_table_provider,
                },
                &options,
            )
            .await?;

        // 5. Build result
        let mut result = build_query_result(
            vars,
            parsed,
            batches,
            Some(db.t),
            Some(db.overlay.clone()),
            db.binary_graph(),
        );
        // This is the R2RML / graph-source execution path. Mark the result so the
        // sparql_json formatter CURIE-compacts its raw graph-source `Binding::Iri`
        // node references (F9). The resolved view of a genuine graph source carries
        // `graph_source_id = Some` (set by `load_view`); a native ledger reached with
        // `.with_r2rml()` attached but no mapping stays `None` → raw, unchanged.
        result.from_graph_source = db.graph_source_id.is_some();
        Ok(result)
    }

    /// Explain a JSON-LD query plan against a GraphDb.
    ///
    /// This uses the same default-context behavior as query execution.
    pub async fn explain(&self, db: &GraphDb, query_json: &JsonValue) -> Result<JsonValue> {
        crate::explain::explain_jsonld_with_default_context(
            &db.snapshot,
            query_json,
            db.default_context.as_ref(),
        )
        .await
    }

    /// Explain a SPARQL query plan against a GraphDb.
    pub async fn explain_sparql(&self, db: &GraphDb, sparql: &str) -> Result<JsonValue> {
        crate::explain::explain_sparql_with_default_context(
            &db.snapshot,
            sparql,
            db.default_context.as_ref(),
        )
        .await
    }

    /// Explain a Cypher query plan against a GraphDb.
    ///
    /// Lowers through the same `parse_cypher_to_ir` path as execution
    /// (default context, `$param` substitution) so the reported plan matches
    /// what [`Self::query_cypher_with_params`] would run.
    pub async fn explain_cypher(
        &self,
        db: &GraphDb,
        cypher: &str,
        params: Option<&fluree_db_cypher::ParamMap>,
    ) -> Result<JsonValue> {
        crate::explain::explain_cypher(&db.snapshot, cypher, db.default_context.as_ref(), params)
            .await
    }

    /// Execute a query with tracking.
    ///
    /// Returns a tracked response with fuel, time, and policy statistics.
    /// When `format_config` is `None`, defaults to JSON-LD for FlureeQL
    /// queries and SPARQL JSON for SPARQL queries.
    pub(crate) async fn query_tracked_with_options(
        &self,
        db: &GraphDb,
        q: impl Into<QueryInput<'_>>,
        format_config: Option<crate::format::FormatterConfig>,
        tracking_override: Option<TrackingOptions>,
        options: QueryExecutionOptions,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        let input = q.into();

        // #1473: parse SPARQL once. Within-ledger SPARQL dataset (see
        // `query_with_options`) on the tracked path: delegate (threading the
        // already-parsed AST) before charging the floor (the dataset method
        // charges it) so fuel is accounted exactly once. `Err` = cross-ledger
        // clause IRI.
        let mut sparql_ast = None;
        if let QueryInput::Sparql(sparql) = input {
            let ast = match parse_and_validate_sparql(sparql) {
                Ok(ast) => ast,
                Err(e) => {
                    let tracker = tracked_query_tracker(&input, &tracking_override);
                    let _ = charge_query_floor(&tracker);
                    return Err(crate::query::TrackedErrorResponse::new(
                        400,
                        e.to_string(),
                        tracker.tally(),
                    ));
                }
            };
            match self.build_within_ledger_dataset_from_ast(db, &ast) {
                Ok(Some(dataset)) => {
                    // Boxed (`dyn`-erased) — see `query_with_options`. Threads
                    // the already-parsed AST straight into the shared impl so
                    // the dataset path does not re-parse (#1473).
                    return futures::FutureExt::boxed(
                        self.query_dataset_tracked_with_options_impl(
                            &dataset,
                            input,
                            format_config,
                            tracking_override,
                            options,
                            Some(ast),
                        ),
                    )
                    .await;
                }
                Ok(None) => sparql_ast = Some(ast),
                Err(e) => {
                    let tracker = tracked_query_tracker(&input, &tracking_override);
                    let _ = charge_query_floor(&tracker);
                    return Err(crate::query::TrackedErrorResponse::new(
                        400,
                        e.to_string(),
                        tracker.tally(),
                    ));
                }
            }
        }

        // Tracker: caller-provided options if given, else per-input defaults.
        let tracker = tracked_query_tracker(&input, &tracking_override);

        // Charge the one-time query floor before parsing so a parse/plan error
        // still reports it and a sub-floor max-fuel is rejected up front.
        charge_query_floor(&tracker)
            .map_err(|e| crate::query::TrackedErrorResponse::fuel_exceeded(&e, tracker.tally()))?;

        // Determine output format: caller override > input-type default
        let default_format = match &input {
            QueryInput::Sparql(_) => crate::format::FormatterConfig::sparql_json(),
            _ => crate::format::FormatterConfig::jsonld(),
        };
        let mut format_config = format_config.unwrap_or(default_format);

        // Lower to common IR (SPARQL reuses the AST parsed above).
        let (vars, mut parsed) = match &input {
            QueryInput::JsonLd(json) => {
                parse_jsonld_query(json, &db.snapshot, db.default_context.as_ref(), None).map_err(
                    |e| {
                        crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
                    },
                )?
            }
            QueryInput::Sparql(sparql) => {
                let ast = sparql_ast
                    .take()
                    .expect("SPARQL input carries a parsed AST");
                lower_sparql_ast(ast, &db.snapshot, db.default_context.as_ref(), sparql).map_err(
                    |e| {
                        crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
                    },
                )?
            }
        };

        // Auto-wrap for graph source context, then refuse whatever the wrap
        // could not put on the provider's path.
        maybe_wrap_for_graph_source(db, &mut parsed);
        guard_graph_source_patterns(db, &parsed, QuerySyntax::of(&input)).map_err(|e| {
            crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
        })?;

        // Build executable with reasoning.
        //
        // The plan-build, execution, and format futures below are each boxed
        // before awaiting. They are deep async chains (operator trees, overlay
        // reads, the hydration crawl), and inlining them here would make this
        // frame — an ancestor for the whole descent — reserve stack for the
        // largest of them, contributing to the ~2 MB worker-stack overflow on a
        // plain `select *` query (fluree/db#1408). Boxing keeps their state on
        // the heap so this frame stays O(1).
        // Report the error's own status. Query preparation completes the
        // ledger's config defaults, so a fault in the config graph surfaces
        // here; a blanket 400 would tell the caller their request was bad
        // when nothing about the request is.
        let executable = Box::pin(self.build_executable_for_view(db, &parsed))
            .await
            .map_err(|e| {
                let status = e.status_code();
                crate::query::TrackedErrorResponse::new(status, e.to_string(), tracker.tally())
            })?;

        // Execute with tracking
        let batches =
            Box::pin(self.execute_view_tracked(db, &vars, &executable, &tracker, &options))
                .await
                .map_err(|e| {
                    let status = query_error_to_status(&e);
                    crate::query::TrackedErrorResponse::new(status, e.to_string(), tracker.tally())
                })?;

        // Build result
        let query_result = build_query_result(
            vars,
            parsed,
            batches,
            Some(db.t),
            Some(db.overlay.clone()),
            db.binary_graph(),
        );

        // CONSTRUCT/DESCRIBE graph results must be formatted as JSON-LD.
        if query_result.output.construct_template().is_some()
            && format_config.format != crate::format::OutputFormat::JsonLd
        {
            format_config = crate::format::FormatterConfig::jsonld();
        }

        // Format with tracking (boxed — the hydration crawl can itself recurse;
        // see the note above and fluree/db#1408).
        let result_json = match db.policy() {
            Some(policy) => Box::pin(query_result.format_async_with_policy_tracked(
                db.as_graph_db_ref(),
                &format_config,
                policy,
                &tracker,
            ))
            .await
            .map_err(|e| {
                crate::query::TrackedErrorResponse::new(500, e.to_string(), tracker.tally())
            })?,
            None => Box::pin(query_result.format_async_tracked(
                db.as_graph_db_ref(),
                &format_config,
                &tracker,
            ))
            .await
            .map_err(|e| {
                crate::query::TrackedErrorResponse::new(500, e.to_string(), tracker.tally())
            })?,
        };

        Ok(crate::query::TrackedQueryResponse::success(
            result_json,
            tracker.tally(),
        ))
    }

    pub(crate) async fn query_tracked_with_r2rml_options(
        &self,
        db: &GraphDb,
        q: impl Into<QueryInput<'_>>,
        format_config: Option<crate::format::FormatterConfig>,
        tracking_override: Option<TrackingOptions>,
        r2rml: crate::R2rmlProviders<'_>,
        options: QueryExecutionOptions,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        let input = q.into();

        // #1473: parse SPARQL once. Within-ledger SPARQL dataset (see
        // `query_with_options`), tracked + R2RML: R2RML + within-ledger
        // FROM/FROM NAMED is unsupported (the R2RML dataset path is non-`Send`;
        // this runs from `Send` contexts), so a resolved clause is rejected
        // rather than silently ignored. Otherwise the AST is reused for lowering.
        let mut sparql_ast = None;
        if let QueryInput::Sparql(sparql) = input {
            let tracker = tracked_query_tracker(&input, &tracking_override);
            let ast = match parse_and_validate_sparql(sparql) {
                Ok(ast) => ast,
                Err(e) => {
                    let _ = charge_query_floor(&tracker);
                    return Err(crate::query::TrackedErrorResponse::new(
                        400,
                        e.to_string(),
                        tracker.tally(),
                    ));
                }
            };
            match self.build_within_ledger_dataset_from_ast(db, &ast) {
                Ok(Some(_dataset)) => {
                    let _ = charge_query_floor(&tracker);
                    return Err(crate::query::TrackedErrorResponse::new(
                        400,
                        "SPARQL FROM/FROM NAMED with R2RML providers is not supported".to_string(),
                        tracker.tally(),
                    ));
                }
                Ok(None) => sparql_ast = Some(ast),
                Err(e) => {
                    let _ = charge_query_floor(&tracker);
                    return Err(crate::query::TrackedErrorResponse::new(
                        400,
                        e.to_string(),
                        tracker.tally(),
                    ));
                }
            }
        }

        let tracker = tracked_query_tracker(&input, &tracking_override);

        // Charge the one-time query floor before parsing (see `query_tracked`).
        charge_query_floor(&tracker)
            .map_err(|e| crate::query::TrackedErrorResponse::fuel_exceeded(&e, tracker.tally()))?;

        let default_format = match &input {
            QueryInput::Sparql(_) => crate::format::FormatterConfig::sparql_json(),
            _ => crate::format::FormatterConfig::jsonld(),
        };
        let mut format_config = format_config.unwrap_or(default_format);

        let (vars, mut parsed) = match &input {
            QueryInput::JsonLd(json) => {
                parse_jsonld_query(json, &db.snapshot, db.default_context.as_ref(), None).map_err(
                    |e| {
                        crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
                    },
                )?
            }
            QueryInput::Sparql(sparql) => {
                let ast = sparql_ast
                    .take()
                    .expect("SPARQL input carries a parsed AST");
                lower_sparql_ast(ast, &db.snapshot, db.default_context.as_ref(), sparql).map_err(
                    |e| {
                        crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
                    },
                )?
            }
        };

        // Auto-wrap for graph source context, then refuse whatever the wrap
        // could not put on the provider's path.
        maybe_wrap_for_graph_source(db, &mut parsed);
        guard_graph_source_patterns(db, &parsed, QuerySyntax::of(&input)).map_err(|e| {
            crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
        })?;

        let executable = self
            .build_executable_for_view(db, &parsed)
            .await
            .map_err(|e| {
                crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
            })?;

        let batches = self
            .execute_view_tracked_with_r2rml(db, &vars, &executable, &tracker, r2rml, &options)
            .await
            .map_err(|e| {
                let status = query_error_to_status(&e);
                crate::query::TrackedErrorResponse::new(status, e.to_string(), tracker.tally())
            })?;

        let query_result = build_query_result(
            vars,
            parsed,
            batches,
            Some(db.t),
            Some(db.overlay.clone()),
            db.binary_graph(),
        );

        if query_result.output.construct_template().is_some()
            && format_config.format != crate::format::OutputFormat::JsonLd
        {
            format_config = crate::format::FormatterConfig::jsonld();
        }

        let result_json = match db.policy() {
            Some(policy) => query_result
                .format_async_with_policy_tracked(
                    db.as_graph_db_ref(),
                    &format_config,
                    policy,
                    &tracker,
                )
                .await
                .map_err(|e| {
                    crate::query::TrackedErrorResponse::new(500, e.to_string(), tracker.tally())
                })?,
            None => query_result
                .format_async_tracked(db.as_graph_db_ref(), &format_config, &tracker)
                .await
                .map_err(|e| {
                    crate::query::TrackedErrorResponse::new(500, e.to_string(), tracker.tally())
                })?,
        };

        Ok(crate::query::TrackedQueryResponse::success(
            result_json,
            tracker.tally(),
        ))
    }

    // ========================================================================
    // Internal Helpers
    // ========================================================================

    /// Build a within-ledger SPARQL dataset from a query's `FROM` / `FROM
    /// NAMED` clause, or `None` when the query carries no dataset clause.
    ///
    /// This actions decision D-3 (Option A). A dataset clause on a single
    /// `GraphDb` names graphs *within this ledger*: each clause IRI resolves
    /// against the ledger's own graph registry (a registered named graph's
    /// g_id, or the ledger alias → the default graph, g_id 0), and every graph
    /// shares the one snapshot. The resulting [`DataSetDb`] runs through the
    /// existing dataset path (`query_dataset*`), so policy, reasoning, and the
    /// runtime `DatasetOperator` are reused unchanged; because all graphs share
    /// one ledger the dataset never `spans_multiple_ledgers`, so no cross-ledger
    /// provenance stamping engages and the scan hot path stays unchanged.
    ///
    /// Takes an already parsed + validated AST: the SPARQL entry methods parse
    /// once and reuse the AST for both dataset-clause resolution here and IR
    /// lowering, rather than re-lexing the string 2–3× (#1473).
    ///
    /// - `Ok(None)` — no dataset clause; run the plain single-graph path.
    /// - `Ok(Some(dataset))` — every clause IRI resolved within this ledger.
    /// - `Err(..)` — a clause IRI is not a graph in this ledger (e.g. it names
    ///   another ledger); cross-ledger datasets go through
    ///   `query_connection_sparql`.
    pub(crate) fn build_within_ledger_dataset_from_ast(
        &self,
        db: &GraphDb,
        ast: &fluree_db_sparql::SparqlAst,
    ) -> Result<Option<DataSetDb>> {
        // Prefix-expanded, BASE-resolved FROM / FROM NAMED IRIs (shared
        // resolution with constant IRIs; shipped by pr-base).
        let Some(clause) = fluree_db_sparql::resolve_dataset_clause(ast)
            .map_err(|e| ApiError::query(e.to_string()))?
        else {
            return Ok(None);
        };

        // `FROM <from> TO <to>` (Fluree history-range extension) is not a plain
        // within-ledger dataset — leave it to the connection path. Its own error
        // (not `cross_ledger_dataset_error`) so the message isn't the misleading
        // "graph not in this ledger" for what is really a history-range clause.
        if clause.to_graph.is_some() {
            return Err(history_range_dataset_error());
        }

        let mut dataset = DataSetDb::new();
        // A default graph named more than once (e.g. `FROM <g> FROM <g>`, or two
        // aliases of the same graph) contributes a single member: the default
        // union is a set (SPARQL §13.2). All within-ledger graphs share the one
        // snapshot, so `graph_id` uniquely identifies a member — collapsing exact
        // duplicates here keeps `FROM <g> FROM <g>` on the byte-identical
        // single-graph path and avoids scanning the same graph twice. Genuinely
        // distinct members that happen to share triples are still deduplicated at
        // scan time by the `DatasetOperator`.
        let mut seen_default_g_ids = std::collections::HashSet::new();
        for iri in &clause.default_graphs {
            let graph = self
                .resolve_within_ledger_graph(db, iri)?
                .ok_or_else(cross_ledger_dataset_error)?;
            if seen_default_g_ids.insert(graph.graph_id) {
                dataset = dataset.with_default(graph);
            }
        }
        for iri in &clause.named_graphs {
            let graph = self
                .resolve_within_ledger_graph(db, iri)?
                .ok_or_else(cross_ledger_dataset_error)?;
            dataset = dataset.with_named(std::sync::Arc::clone(iri), graph);
        }
        Ok(Some(dataset))
    }

    /// Resolve one `FROM` / `FROM NAMED` IRI to a graph *within this ledger*,
    /// or `None` when the IRI does not name a graph in this ledger.
    ///
    /// - the ledger alias → the default graph (g_id 0), mirroring
    ///   `ExecutionContext::single_db_user_graph_id`, which reserves the alias
    ///   for the default graph;
    /// - a registered named-graph IRI → that graph's g_id.
    ///
    /// Reuses [`apply_graph_selector`](Self::apply_graph_selector) — the same
    /// within-ledger selection primitive behind `fluree.db("ledger:main#graph")`
    /// and the JSON-LD `@graph` dataset source — so all three surfaces resolve
    /// and re-scope a graph identically.
    fn resolve_within_ledger_graph(&self, db: &GraphDb, iri: &str) -> Result<Option<GraphDb>> {
        use crate::dataset::GraphSelector;

        if iri == db.snapshot.ledger_id.as_str() || iri == db.ledger_id.as_ref() {
            return Ok(Some(Self::apply_graph_selector(
                db.clone(),
                &GraphSelector::Default,
            )?));
        }

        // Registered USER named graph in this ledger (registry, with the same
        // binary-store fallback `select_graph` uses). The registry also seeds
        // the reserved system graphs — `#txn-meta` (g_id 1) and `#config`
        // (g_id 2) — which must NOT be reachable through `FROM`/`FROM NAMED`:
        // the plain `GRAPH <iri>` path already blocks them via
        // `single_db_user_graph_id`'s `>= FIRST_USER_GRAPH_ID` filter, so gate
        // this path identically (a reserved IRI falls through to the
        // "not in this ledger" rejection below).
        let is_user_graph =
            |g: fluree_db_core::GraphId| g >= fluree_db_core::graph_registry::FIRST_USER_GRAPH_ID;
        let known = db
            .snapshot
            .graph_registry
            .graph_id_for_iri(iri)
            .is_some_and(is_user_graph)
            || db
                .binary_store
                .as_ref()
                .and_then(|s| s.graph_id_for_iri(iri))
                .is_some_and(is_user_graph);
        if known {
            return Ok(Some(Self::apply_graph_selector(
                db.clone(),
                &GraphSelector::Iri(iri.to_string()),
            )?));
        }

        Ok(None)
    }

    /// Validate that SPARQL doesn't have dataset clauses (FROM/FROM NAMED).
    ///
    /// Used by the single-graph *streaming* endpoint (`plan_stream_query`),
    /// which does not support datasets. The buffered `query*` paths instead
    /// build a within-ledger dataset via
    /// [`build_within_ledger_dataset_from_ast`](Self::build_within_ledger_dataset_from_ast)
    /// and only reject cross-ledger clause IRIs.
    pub(crate) fn validate_sparql_for_view(&self, sparql: &str) -> Result<()> {
        let ast = parse_and_validate_sparql(sparql)?;
        if sparql_ast_has_dataset(&ast) {
            return Err(single_ledger_dataset_clause_error());
        }
        Ok(())
    }

    /// Build an ExecutableQuery with optional reasoning override.
    ///
    /// Also enforces config-graph datalog restrictions: if config disables
    /// datalog and the query can't override, the datalog flag and/or
    /// query-time rules are stripped. When reasoning config declares an
    /// `f:schemaSource` (with optional `owl:imports` closure), the resolved
    /// schema bundle is attached to `options.schema_bundle` so the runner
    /// can layer it as a `SchemaBundleOverlay` at prep time.
    pub(crate) async fn build_executable_for_view(
        &self,
        db: &GraphDb,
        parsed: &fluree_db_query::ir::Query,
    ) -> Result<ExecutableQuery> {
        // Start with the standard executable
        let mut executable = prepare_for_execution(parsed);

        self.apply_reasoning_to_executable(db, &mut executable, !db.is_root())
            .await?;

        Ok(executable)
    }

    /// Apply a view's reasoning configuration to an executable.
    ///
    /// Single choke point shared by the single-ledger path
    /// ([`build_executable_for_view`](Self::build_executable_for_view)) and
    /// the dataset path (`build_executable_for_dataset`, which passes the
    /// dataset's primary view) so both engage the same reasoning surface:
    /// the ledger's config-graph defaults, mode precedence, the config
    /// materialization budget, config-graph datalog restrictions, the
    /// query-time rule policy gate, local and cross-ledger `f:rulesSource`,
    /// and the `f:schemaSource` bundle (local, cross-ledger, and inline
    /// `opts.ontology`).
    ///
    /// Config defaults are completed here rather than taken from the view as
    /// received: a view arrives fully prepared, config-attached but
    /// wrapper-less, or bare, depending on which entry point built it.
    ///
    /// `strip_query_rules` is true when a non-root view policy applies —
    /// `!db.is_root()` for a single view, `dataset.any_non_root_policy()`
    /// for a dataset. It strips *query-supplied* rules only; config-sourced
    /// rules attach afterwards because they're admin-controlled.
    pub(crate) async fn apply_reasoning_to_executable(
        &self,
        db: &GraphDb,
        executable: &mut ExecutableQuery,
        strip_query_rules: bool,
    ) -> Result<()> {
        let db = &self.complete_config_defaults(db).await?;

        // Apply wrapper reasoning if applicable
        if db.reasoning().is_some() {
            // Check query's reasoning state
            let query_has_reasoning = executable.reasoning.modes.has_any_enabled();
            let query_disabled = executable.reasoning.modes.is_disabled();

            // Apply precedence rules. Mode replacement keeps the query's
            // budget: wrapper modes come from config mode strings and never
            // carry one, and whether the query budget survives is decided
            // below by `ConfigReasoningBudget::apply` (override control),
            // not by mode precedence.
            if let Some(effective) = db.effective_reasoning(query_has_reasoning, query_disabled) {
                let (max_facts, max_seconds) = (
                    executable.reasoning.modes.max_facts,
                    executable.reasoning.modes.max_seconds,
                );
                executable.reasoning.modes = effective.clone();
                executable.reasoning.modes.max_facts = max_facts;
                executable.reasoning.modes.max_seconds = max_seconds;
            }
        }

        // Apply the ledger-config materialization budget. Runs after mode
        // precedence on purpose: the budget governs whichever modes won
        // (config defaults or a query override), and override control decides
        // whether a query-supplied budget survives.
        if let Some(budget) = db.config_reasoning_budget() {
            budget.apply(&mut executable.reasoning.modes);
        }

        // Enforce config-graph datalog restrictions
        if !db.datalog_override_allowed() {
            // Config override denied — force config settings
            if !db.datalog_enabled() {
                executable.reasoning.modes.datalog = false;
            }
            if !db.query_time_rules_allowed() {
                executable.reasoning.modes.rules.clear();
            }
        }

        // Query-time datalog rule injection is admin-only: a restricted
        // (non-root view policy) request may not supply inference rules. A rule
        // with a viewable head can launder hidden data the policy author never
        // anticipated — the derived flake is filtered only by its own (s,p,o),
        // not its provenance, and a caller-invented predicate can't be
        // pre-denied. DB-stored rules and OWL reasoning are admin-controlled and
        // unaffected. See docs/security/policy-in-queries.md (Reasoning).
        if strip_query_rules && !executable.reasoning.modes.rules.is_empty() {
            tracing::debug!("stripping query-time datalog rules under non-root view policy");
            executable.reasoning.modes.rules.clear();
        }

        // Carry the pre-resolved `f:rulesSource` graph id (if any)
        // into the executable so `compute_derived_facts` extracts
        // datalog rules from the configured graph instead of the
        // query graph.
        executable.reasoning.rules_source_g_id = db.rules_source_g_id();

        // Build a single per-request `ResolveCtx` so every
        // cross-ledger artifact (rules, schema, …) captured by this
        // query observes a coherent head-t per model ledger. Two
        // separate contexts would each lazy-capture a head-t and
        // could disagree if M advances between awaits — that breaks
        // the resolver's per-request consistency contract.
        //
        // Seeded from `db.cross_ledger_resolved_ts` so a preceding
        // `wrap_policy` call's captures carry forward: policy and
        // reasoning/rules on the same M must agree on which
        // version of M they're enforcing, even though they enter
        // through separate Rust API calls.
        let mut ctx = crate::cross_ledger::ResolveCtx::with_resolved_ts(
            db.as_graph_db_ref().snapshot.ledger_id.as_str(),
            self,
            (**db.cross_ledger_resolved_ts()).clone(),
        );

        // Cross-ledger `f:rulesSource`: when M is referenced via
        // `f:ledger`, resolve M's rules graph through the
        // cross-ledger resolver and merge the JSON rule bodies into
        // `executable.reasoning.rules` so they pass through the
        // existing query-time rule code path. Same-ledger references
        // are handled above via `rules_source_g_id`.
        self.attach_cross_ledger_rules(db, executable, &mut ctx)
            .await?;

        // Resolve `f:schemaSource` + `owl:imports` closure, if configured.
        self.attach_schema_bundle(db, executable, &mut ctx).await?;

        Ok(())
    }

    /// If the resolved datalog config carries a cross-ledger
    /// `f:rulesSource`, dispatch through the cross-ledger resolver
    /// and append the parsed JSON rules to
    /// `executable.reasoning.rules`. Short-circuits when:
    /// - the view has no resolved config,
    /// - no `f:rulesSource` is configured,
    /// - `f:rulesSource` is purely local (`f:ledger` unset — handled
    ///   by the `rules_source_g_id` pre-resolution path),
    /// - datalog reasoning is not enabled on the executable (no point
    ///   pulling rules we won't run).
    ///
    /// Errors propagate as `ApiError::CrossLedger`; the server maps
    /// those to HTTP 502.
    async fn attach_cross_ledger_rules(
        &self,
        db: &GraphDb,
        executable: &mut ExecutableQuery,
        ctx: &mut crate::cross_ledger::ResolveCtx<'_>,
    ) -> Result<()> {
        if !executable.reasoning.modes.datalog {
            return Ok(());
        }
        let Some(resolved) = db.resolved_config() else {
            return Ok(());
        };
        let Some(datalog) = resolved.datalog.as_ref() else {
            return Ok(());
        };
        let Some(source) = datalog.rules_source.as_ref() else {
            return Ok(());
        };
        if source.ledger.is_none() {
            return Ok(());
        }

        let resolved = crate::cross_ledger::resolve_graph_ref(
            source,
            crate::cross_ledger::ArtifactKind::Rules,
            ctx,
        )
        .await?;
        let crate::cross_ledger::GovernanceArtifact::Rules(wire) = &resolved.artifact else {
            return Err(crate::error::ApiError::CrossLedger(
                crate::cross_ledger::CrossLedgerError::TranslationFailed {
                    ledger_id: resolved.model_ledger_id.clone(),
                    graph_iri: resolved.graph_iri.clone(),
                    detail: "resolver returned a non-Rules artifact for a Rules request; \
                             resolver dispatch bug"
                        .into(),
                },
            ));
        };
        executable
            .reasoning
            .modes
            .rules
            .extend(wire.parsed_rules()?);
        Ok(())
    }

    /// Resolve the schema bundle from the ledger's reasoning config and attach
    /// the projected schema flakes to `executable.reasoning.schema_bundle`.
    ///
    /// Short-circuits in three cases (no bundle is built, no error is
    /// raised):
    /// - The view has no resolved config.
    /// - Reasoning defaults have no `f:schemaSource`.
    /// - The effective query reasoning is **explicitly disabled**
    ///   (`"reasoning": "none"`). Users who opt out of reasoning must not
    ///   be exposed to errors from an otherwise-unrelated broken ontology
    ///   import; the bundle is a reasoning-only concern.
    ///
    /// Errors with [`ApiError::OntologyImport`] only when reasoning is
    /// actually engaged and an import can't be resolved locally, or when
    /// `f:followOwlImports` is combined with a cross-ledger
    /// `f:schemaSource` (the cross-ledger materializer is single-graph).
    async fn attach_schema_bundle(
        &self,
        db: &GraphDb,
        executable: &mut ExecutableQuery,
        ctx: &mut crate::cross_ledger::ResolveCtx<'_>,
    ) -> Result<()> {
        if executable.reasoning.modes.is_disabled() {
            return Ok(());
        }

        let db_ref = db.as_graph_db_ref();

        // 1. Resolve the configured `f:schemaSource` (if any) into a
        //    bundle. Either branch — cross-ledger or local — may yield
        //    None when the field isn't configured.
        let configured_bundle = self
            .resolve_configured_schema_bundle(db, &db_ref, ctx)
            .await?;

        // 2. Parse inline `opts.ontology` axioms (if any) into a
        //    bundle. Layered on top of `configured_bundle` so a
        //    query can extend the ledger's reasoning with per-request
        //    axioms without persisting them.
        //
        //    `take()` so the (potentially large) raw JSON-LD doesn't
        //    ride along on `ReasoningModes.ontology` for the rest of
        //    query preparation — `Query::with_patterns` clones the
        //    reasoning config downstream, and only the compiled
        //    `SchemaBundleFlakes` overlay is needed past this point.
        let inline_bundle = match executable.reasoning.modes.ontology.take() {
            Some(json) => {
                crate::inline_ontology::parse_inline_ontology_to_bundle(&json, db_ref.snapshot)?
            }
            None => None,
        };

        // 3. Merge.
        executable.reasoning.schema_bundle = match (configured_bundle, inline_bundle) {
            (None, None) => None,
            (Some(b), None) | (None, Some(b)) => Some(b),
            (Some(a), Some(b)) => Some(crate::inline_ontology::merge_bundles(a, b)?),
        };
        Ok(())
    }

    /// Resolve the configured `f:schemaSource` (same- or cross-ledger)
    /// into a [`SchemaBundleFlakes`]. Returns `Ok(None)` when no
    /// `f:schemaSource` is configured. Extracted so
    /// [`attach_schema_bundle`] can layer the inline ontology on top
    /// regardless of which configured branch ran (or whether either
    /// ran).
    async fn resolve_configured_schema_bundle(
        &self,
        db: &GraphDb,
        db_ref: &fluree_db_core::GraphDbRef<'_>,
        ctx: &mut crate::cross_ledger::ResolveCtx<'_>,
    ) -> Result<Option<std::sync::Arc<fluree_db_query::schema_bundle::SchemaBundleFlakes>>> {
        let Some(resolved) = db.resolved_config() else {
            return Ok(None);
        };
        let Some(reasoning) = resolved.reasoning.as_ref() else {
            return Ok(None);
        };
        let Some(schema_source) = reasoning.schema_source.as_ref() else {
            return Ok(None);
        };

        // Cross-ledger detection: if the source carries `f:ledger`,
        // dispatch through the cross-ledger resolver and translate
        // the resulting `SchemaArtifactWire` into a SchemaBundleFlakes
        // against D's snapshot.
        if schema_source.ledger.is_some() {
            // The cross-ledger schema materializer resolves a single
            // graph and does not walk `owl:imports`. Fail closed —
            // silently ignoring `f:followOwlImports` would let the user
            // believe the import closure is part of the reasoning view
            // when only the starting graph is.
            if reasoning.follow_owl_imports.unwrap_or(false) {
                return Err(crate::error::ApiError::OntologyImport(
                    "`f:followOwlImports` is not supported with a cross-ledger \
                     `f:schemaSource` — the cross-ledger resolver materializes \
                     the referenced graph only and does not walk `owl:imports`. \
                     Consolidate the schema closure into the referenced graph, \
                     or remove `f:followOwlImports`."
                        .to_string(),
                ));
            }
            let resolved = crate::cross_ledger::resolve_graph_ref(
                schema_source,
                crate::cross_ledger::ArtifactKind::SchemaClosure,
                ctx,
            )
            .await?;
            let crate::cross_ledger::GovernanceArtifact::SchemaClosure(wire) = &resolved.artifact
            else {
                return Err(crate::error::ApiError::CrossLedger(
                    crate::cross_ledger::CrossLedgerError::TranslationFailed {
                        ledger_id: resolved.model_ledger_id.clone(),
                        graph_iri: resolved.graph_iri.clone(),
                        detail: "resolver returned a non-SchemaClosure artifact for a \
                                SchemaClosure request; resolver dispatch bug"
                            .into(),
                    },
                ));
            };
            return Ok(Some(
                wire.translate_to_schema_bundle_flakes(db_ref.snapshot)?,
            ));
        }

        let Some(bundle) = crate::ontology_imports::resolve_schema_bundle(
            db_ref.snapshot,
            db_ref.overlay,
            db_ref.t,
            reasoning,
        )
        .await?
        else {
            return Ok(None);
        };

        let flakes = crate::ontology_imports::get_or_build_schema_bundle_flakes(
            db_ref.snapshot,
            db_ref.overlay,
            &bundle,
        )
        .await?;
        Ok(Some(flakes))
    }

    /// Execute against a GraphDb with policy awareness.
    ///
    /// Single internal path that handles both policy and non-policy execution.
    /// Threads `binary_store` from the db into `ContextConfig` so that
    /// `BinaryScanOperator` can use the binary cursor path when available.
    pub(crate) async fn execute_view_internal(
        &self,
        db: &GraphDb,
        vars: &crate::VarRegistry,
        executable: &ExecutableQuery,
        tracker: &Tracker,
        options: &QueryExecutionOptions,
    ) -> Result<Vec<crate::Batch>> {
        let noop = crate::NoOpR2rmlProvider::new();
        self.execute_view_internal_with_r2rml(
            db,
            vars,
            executable,
            tracker,
            crate::R2rmlProviders {
                provider: &noop,
                table_provider: &noop,
            },
            options,
        )
        .await
    }

    /// Execute against a GraphDb with explicit R2RML provider.
    ///
    /// Used by callers that need R2RML/Iceberg graph source support
    /// (e.g., server query handlers with iceberg support).
    pub(crate) async fn execute_view_internal_with_r2rml(
        &self,
        db: &GraphDb,
        vars: &crate::VarRegistry,
        executable: &ExecutableQuery,
        tracker: &Tracker,
        r2rml: crate::R2rmlProviders<'_>,
        options: &QueryExecutionOptions,
    ) -> Result<Vec<crate::Batch>> {
        let db_ref = db.as_graph_db_ref();
        // Single-graph view: no dataset-level history detection — current state.
        // Single ledger + root policy ⇒ semantic stats rewrites (redundant
        // `rdf:type` elision) are sound; a non-root enforcer hides rows and must
        // not allow it.
        let allow_semantic_elision = db.policy_enforcer().is_none_or(|p| p.is_root());
        let prepare_config = PrepareConfig::current_with_semantic_elision(
            db.binary_store.as_ref(),
            allow_semantic_elision,
        );
        let prepared = prepare_execution_with_config(db_ref, executable, &prepare_config)
            .await
            .map_err(query_error_to_api_error)?;

        view_context_config!(
            config,
            self,
            db,
            executable,
            tracker,
            options,
            Some((r2rml.provider, r2rml.table_provider)),
        );

        execute_prepared(db_ref, vars, prepared, config)
            .await
            .map_err(query_error_to_api_error)
    }

    /// Execute against a GraphDb with policy awareness (tracked variant).
    ///
    /// Uses tracked execution functions to properly record fuel/time/policy stats.
    pub(crate) async fn execute_view_tracked(
        &self,
        db: &GraphDb,
        vars: &crate::VarRegistry,
        executable: &ExecutableQuery,
        tracker: &Tracker,
        options: &QueryExecutionOptions,
    ) -> std::result::Result<Vec<crate::Batch>, fluree_db_query::QueryError> {
        let noop = crate::NoOpR2rmlProvider::new();
        self.execute_view_tracked_with_r2rml(
            db,
            vars,
            executable,
            tracker,
            crate::R2rmlProviders {
                provider: &noop,
                table_provider: &noop,
            },
            options,
        )
        .await
    }

    pub(crate) async fn execute_view_tracked_with_r2rml(
        &self,
        db: &GraphDb,
        vars: &crate::VarRegistry,
        executable: &ExecutableQuery,
        tracker: &Tracker,
        r2rml: crate::R2rmlProviders<'_>,
        options: &QueryExecutionOptions,
    ) -> std::result::Result<Vec<crate::Batch>, fluree_db_query::QueryError> {
        // Record whether policy governs this request, before executing: the
        // state comes from the prepared view, so it is reported even when
        // execution returns no rows or fails part-way.
        tracker.record_policy_enforcement(db.policy_enforcement());

        let db_ref = db.as_graph_db_ref();
        // Single-graph view: no dataset-level history detection — current state.
        // Single ledger + root policy ⇒ semantic stats rewrites are sound.
        let allow_semantic_elision = db.policy_enforcer().is_none_or(|p| p.is_root());
        let prepare_config = PrepareConfig::current_with_semantic_elision(
            db.binary_store.as_ref(),
            allow_semantic_elision,
        );
        let prepared = prepare_execution_with_config(db_ref, executable, &prepare_config).await?;

        view_context_config!(
            config,
            self,
            db,
            executable,
            tracker,
            options,
            Some((r2rml.provider, r2rml.table_provider)),
        );

        execute_prepared(db_ref, vars, prepared, config).await
    }
}

// ============================================================================
// Error Conversion Helpers
// ============================================================================

fn query_error_to_api_error(err: fluree_db_query::QueryError) -> ApiError {
    ApiError::Query(err)
}

/// Map QueryError to HTTP-ish status code.
fn query_error_to_status(err: &fluree_db_query::QueryError) -> u16 {
    status_for_query_error(err)
}

/// Rejection for a dataset clause on a surface that does not support datasets
/// (streaming views, R2RML-provider queries). One definition so the message
/// cannot drift between its call sites.
fn single_ledger_dataset_clause_error() -> ApiError {
    ApiError::query(
        "SPARQL FROM/FROM NAMED clauses are not supported on a single-ledger GraphDb. \
         Use query_connection_sparql for multi-ledger queries.",
    )
}

/// Rejection for a `FROM` / `FROM NAMED` clause that references a graph outside
/// this ledger (or the `FROM..TO` history extension). The message mentions
/// `FROM` so callers and tests can recognize the dataset-clause rejection.
fn cross_ledger_dataset_error() -> ApiError {
    ApiError::query(
        "SPARQL FROM/FROM NAMED references a graph that is not in this ledger. \
         A within-ledger dataset names this ledger's graphs (its default graph \
         via the ledger alias, or a registered named graph) — check the IRI \
         for typos against the ledger's registered graphs; for a graph in \
         ANOTHER ledger, use query_connection_sparql (cross-ledger datasets).",
    )
}

/// Error for a `FROM <from> TO <to>` history-range clause on the within-ledger
/// path. Distinct from [`cross_ledger_dataset_error`] because the clause names
/// a time range, not a cross-ledger graph — reusing the graph-membership
/// message there would misdescribe the rejection.
fn history_range_dataset_error() -> ApiError {
    ApiError::query(
        "SPARQL `FROM <from> TO <to>` is the Fluree history-range extension, not \
         a within-ledger dataset clause; issue it through the connection/history \
         query path.",
    )
}

#[cfg(test)]
mod tests {

    use crate::FlureeBuilder;
    use serde_json::json;

    #[tokio::test]
    async fn test_query_jsonld() {
        let fluree = FlureeBuilder::memory().build_memory();

        // Create ledger with data (using full IRIs)
        let ledger = fluree.create_ledger("testdb").await.unwrap();
        let txn = json!({
            "insert": [{
                "@id": "http://example.org/alice",
                "http://example.org/name": "Alice"
            }]
        });
        let _ledger = fluree.update(ledger, &txn).await.unwrap().ledger;

        let db = fluree.db("testdb:main").await.unwrap();
        let query = json!({
            "select": ["?name"],
            "where": {"@id": "http://example.org/alice", "http://example.org/name": "?name"}
        });

        let result = fluree.query(&db, &query).await.unwrap();
        assert!(!result.batches.is_empty());
    }

    #[tokio::test]
    async fn test_query_sparql() {
        let fluree = FlureeBuilder::memory().build_memory();

        // Create ledger with data
        let ledger = fluree.create_ledger("testdb").await.unwrap();
        let txn = json!({
            "insert": [{
                "@id": "http://example.org/alice",
                "http://example.org/name": "Alice"
            }]
        });
        let _ledger = fluree.update(ledger, &txn).await.unwrap().ledger;

        let db = fluree.db("testdb:main").await.unwrap();
        let result = fluree
            .query(
                &db,
                "SELECT ?name WHERE { <http://example.org/alice> <http://example.org/name> ?name }",
            )
            .await
            .unwrap();

        assert!(!result.batches.is_empty());
    }

    #[tokio::test]
    async fn test_query_sparql_cross_ledger_dataset_rejected() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        let db = fluree.db("testdb:main").await.unwrap();

        // A FROM clause whose IRI names neither the ledger alias nor a graph in
        // this ledger is a cross-ledger dataset — still rejected (within-ledger
        // FROM is exercised in the it_query_dataset integration tests).
        let result = fluree
            .query(
                &db,
                "SELECT * FROM <http://other.org/ledger> WHERE { ?s ?p ?o }",
            )
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("FROM"));
    }

    #[tokio::test]
    async fn test_query_jsonld_format() {
        let fluree = FlureeBuilder::memory().build_memory();

        // Create ledger with data
        let ledger = fluree.create_ledger("testdb").await.unwrap();
        let txn = json!({
            "insert": [{
                "@id": "http://example.org/alice",
                "http://example.org/name": "Alice"
            }]
        });
        let _ledger = fluree.update(ledger, &txn).await.unwrap().ledger;

        let db = fluree.db("testdb:main").await.unwrap();
        let query = json!({
            "select": ["?name"],
            "where": {"@id": "http://example.org/alice", "http://example.org/name": "?name"}
        });

        let result = db
            .query(&fluree)
            .jsonld(&query)
            .execute_formatted()
            .await
            .unwrap();

        // Should be JSON-LD formatted
        assert!(result.is_array() || result.is_object());
    }

    #[tokio::test]
    async fn test_query_with_time_travel() {
        let fluree = FlureeBuilder::memory().build_memory();

        // Create ledger with data at t=1
        let ledger = fluree.create_ledger("testdb").await.unwrap();
        let txn = json!({
            "insert": [{
                "@id": "http://example.org/alice",
                "http://example.org/name": "Alice"
            }]
        });
        let _ledger = fluree.update(ledger, &txn).await.unwrap().ledger;

        // Query at t=0 (before insert)
        let db = fluree.db_at_t("testdb:main", 0).await.unwrap();
        let query = json!({
            "select": ["?name"],
            "where": {"@id": "http://example.org/alice", "http://example.org/name": "?name"}
        });
        let result = fluree.query(&db, &query).await.unwrap();
        assert!(result.batches.is_empty() || result.batches[0].is_empty());

        // Query at t=1 (after insert)
        let db = fluree.db_at_t("testdb:main", 1).await.unwrap();
        let result = fluree.query(&db, &query).await.unwrap();
        assert!(!result.batches.is_empty());
    }
}

/// End-to-end routing coverage for the graph-source pattern guard.
///
/// The rewrite-level unit test in `fluree-db-query` (`rewrite.rs`,
/// `non_lowered_subscopes_are_flagged_unsupported`) exercises the rewriter in
/// isolation and would keep passing if the routing that feeds it broke — which
/// is exactly how the `GRAPH`-block bypass survived. These drive the real query
/// entry point against a real graph-source view instead, so the four routes a
/// query can take to a graph source stay pinned together: with the auto-wrap,
/// with the user's own `GRAPH` block, with both, and with neither pattern kind
/// involved.
#[cfg(test)]
mod graph_source_guard_tests {
    use crate::view::GraphDb;
    use crate::{FlureeBuilder, QueryExecutionOptions};
    use async_trait::async_trait;
    use fluree_db_query::r2rml::{
        ColumnBatchStream, R2rmlProvider, R2rmlTableProvider, ScanFilter, ScanTopK,
    };
    use fluree_db_r2rml::mapping::CompiledR2rmlMapping;
    use std::sync::Arc;

    const GS_ID: &str = "fs3repro:main";
    const NS_CODE: u16 = 9_999;
    const NS_IRI: &str = "http://fs3.example/";
    const EDGE_PRED: &str = "http://fs3.example/vocab#edge";
    const N1: &str = "http://fs3.example/node/n1";

    /// Marker the stub errors with, so a test can prove a pattern reached the
    /// R2RML scan layer rather than being refused on the way there.
    const SCAN_MARKER: &str = "GSGUARD-SCAN-REACHED";

    /// Reports a mapping for the graph source — all the rewrite decision needs —
    /// and fails loudly if anything gets as far as scanning a table.
    #[derive(Debug)]
    struct StubProvider {
        mapping: Arc<CompiledR2rmlMapping>,
    }

    impl StubProvider {
        fn new() -> Self {
            use fluree_db_r2rml::mapping::{
                ObjectMap, PredicateMap, PredicateObjectMap, TriplesMap,
            };
            let tm = TriplesMap::new("#Edge", "fs3.edge")
                .with_subject_template("http://fs3.example/node/{src}")
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant(EDGE_PRED),
                    object_map: ObjectMap::column("dst"),
                });
            Self {
                mapping: Arc::new(CompiledR2rmlMapping::new(vec![tm])),
            }
        }
    }

    #[async_trait]
    impl R2rmlProvider for StubProvider {
        async fn has_r2rml_mapping(&self, _graph_source_id: &str) -> bool {
            true
        }

        async fn compiled_mapping(
            &self,
            _graph_source_id: &str,
            _as_of_t: Option<i64>,
        ) -> fluree_db_query::Result<Arc<CompiledR2rmlMapping>> {
            Ok(Arc::clone(&self.mapping))
        }
    }

    #[async_trait]
    impl R2rmlTableProvider for StubProvider {
        async fn scan_table(
            &self,
            _graph_source_id: &str,
            _table_name: &str,
            _projection: &[String],
            _filters: &[ScanFilter],
            _topk: Option<&ScanTopK>,
            _as_of_t: Option<i64>,
        ) -> fluree_db_query::Result<ColumnBatchStream> {
            Err(fluree_db_query::QueryError::Internal(
                SCAN_MARKER.to_string(),
            ))
        }
    }

    /// The view `resolve_graph_source` hands back for a registered graph source:
    /// a genesis (zero-flake) snapshot named for the source and tagged with its
    /// id, with the fixture's namespace registered so IRIs encode. Built the
    /// same way here so no nameservice registration (and no `iceberg` feature)
    /// is needed to exercise the routing.
    fn graph_source_view() -> GraphDb {
        let mut snapshot = fluree_db_core::LedgerSnapshot::genesis(GS_ID);
        snapshot
            .insert_namespace_code(NS_CODE, NS_IRI.to_string())
            .expect("namespace registration");
        let state =
            fluree_db_ledger::LedgerState::new(snapshot, fluree_db_novelty::Novelty::new(0));
        let mut db = GraphDb::from_ledger_state(&state);
        db.graph_source_id = Some(GS_ID.into());
        db
    }

    /// Run `sparql` against a graph-source view through the R2RML query entry
    /// point, returning the error message (every arm here ends in an error: a
    /// refusal, or the stub's scan marker).
    async fn query_err(sparql: &str) -> String {
        let fluree = FlureeBuilder::memory().build_memory();
        let db = graph_source_view();
        let stub = StubProvider::new();
        fluree
            .query_view_with_r2rml_options(
                &db,
                sparql,
                &stub,
                &stub,
                QueryExecutionOptions::default(),
            )
            .await
            .expect_err("every arm of this matrix ends in an error")
            .to_string()
    }

    fn assert_refused(msg: &str, context: &str) {
        assert!(
            msg.contains("property path") && msg.contains("cannot be evaluated"),
            "{context}: expected the unsupported-pattern refusal, got: {msg}"
        );
    }

    /// The bypass. A quantifier at the top level next to the user's own `GRAPH`
    /// block: the auto-wrap declines the whole query, so nothing routes the path
    /// to `GraphOperator` and its guard never sees it. Before the pre-execution
    /// check this returned HTTP 200 with zero rows.
    #[tokio::test]
    async fn quantifier_beside_an_explicit_graph_block_is_refused() {
        for label in ["+", "*", "?"] {
            let msg = query_err(&format!(
                "SELECT ?o WHERE {{ <{N1}> <{EDGE_PRED}>{label} ?o . \
                 GRAPH <{GS_ID}> {{ ?a <{EDGE_PRED}> ?c }} }}"
            ))
            .await;
            assert_refused(&msg, &format!("`edge{label}` beside a GRAPH block"));
        }
    }

    /// The route that already worked, unchanged: with no `GRAPH` block of its
    /// own the query is auto-wrapped, and the rewrite guard refuses it inside
    /// the scope. Pinned so the pre-execution check cannot quietly become the
    /// only thing holding this up.
    #[tokio::test]
    async fn quantifier_with_no_graph_block_is_still_refused() {
        for label in ["+", "*", "?"] {
            let msg = query_err(&format!(
                "SELECT ?o WHERE {{ <{N1}> <{EDGE_PRED}>{label} ?o }}"
            ))
            .await;
            assert_refused(&msg, &format!("bare `edge{label}`"));
        }
    }

    /// Also unchanged: a quantifier the user scoped into a `GRAPH` block itself.
    #[tokio::test]
    async fn quantifier_inside_an_explicit_graph_block_is_still_refused() {
        let msg = query_err(&format!(
            "SELECT ?o WHERE {{ GRAPH <{GS_ID}> {{ <{N1}> <{EDGE_PRED}>+ ?o }} }}"
        ))
        .await;
        assert_refused(&msg, "`edge+` inside a GRAPH block");
    }

    /// No over-refusal: a fixed-length pattern in a legitimately scoped `GRAPH`
    /// block still lowers to an R2RML scan and reaches the provider. This is the
    /// contrast the original reporter measured, and the property the guard must
    /// not break.
    #[tokio::test]
    async fn fixed_length_pattern_in_a_graph_block_still_reaches_the_provider() {
        let msg = query_err(&format!(
            "SELECT ?o WHERE {{ GRAPH <{GS_ID}> {{ <{N1}> <{EDGE_PRED}> ?o }} }}"
        ))
        .await;
        assert!(
            msg.contains(SCAN_MARKER),
            "a fixed-length pattern must reach the R2RML scan, not a guard: {msg}"
        );
    }

    /// A plain triple stranded at the top level by the same bypass is refused
    /// too. It is lowerable in principle, but not on this plan — nothing routes
    /// it to the provider, so it reads the empty genesis index and zeroes the
    /// whole conjunction. Verified before implementing: it returns HTTP 200 with
    /// 0 rows today.
    #[tokio::test]
    async fn top_level_triple_beside_an_explicit_graph_block_is_refused() {
        let msg = query_err(&format!(
            "SELECT ?o WHERE {{ <{N1}> <{EDGE_PRED}> ?o . \
             GRAPH <{GS_ID}> {{ ?a <{EDGE_PRED}> ?c }} }}"
        ))
        .await;
        assert!(
            msg.contains("top-level triple pattern") && msg.contains("Move them inside"),
            "expected the unroutable-top-level refusal naming the workaround, got: {msg}"
        );
    }

    /// Scope guard: the refusal is for *conjunctive* top-level triples only. A
    /// vacuous `OPTIONAL` still emits its left side, so refusing it would reject
    /// a query that returns rows today — those stay out of scope until the
    /// patterns can actually be routed.
    #[tokio::test]
    async fn a_top_level_optional_beside_a_graph_block_is_not_refused() {
        let msg = query_err(&format!(
            "SELECT ?o WHERE {{ GRAPH <{GS_ID}> {{ <{N1}> <{EDGE_PRED}> ?o }} \
             OPTIONAL {{ ?o <{EDGE_PRED}> ?x }} }}"
        ))
        .await;
        assert!(
            msg.contains(SCAN_MARKER),
            "an OPTIONAL body must not be refused — the GRAPH block still scans: {msg}"
        );
    }

    /// Scope guard: patterns that never read this view's index — `VALUES`,
    /// `BIND`, `FILTER`, and the search adapters, which carry their own graph
    /// source and route independently — must survive at the top level.
    #[tokio::test]
    async fn top_level_values_beside_a_graph_block_is_not_refused() {
        let msg = query_err(&format!(
            "SELECT ?o ?v WHERE {{ VALUES ?v {{ 1 }} \
             GRAPH <{GS_ID}> {{ <{N1}> <{EDGE_PRED}> ?o }} }}"
        ))
        .await;
        assert!(
            msg.contains(SCAN_MARKER),
            "VALUES reads no index and must not be refused: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // The refusal has to be spelled in the language the user is writing
    // -----------------------------------------------------------------------

    /// SPARQL scopes with `GRAPH <iri> { … }`, so that is what a SPARQL user is
    /// told to write.
    #[tokio::test]
    async fn a_sparql_refusal_names_sparql_scoping_syntax() {
        let msg = query_err(&format!(
            "SELECT ?o WHERE {{ <{N1}> <{EDGE_PRED}> ?o . \
             GRAPH <{GS_ID}> {{ ?a <{EDGE_PRED}> ?c }} }}"
        ))
        .await;
        assert!(
            msg.contains("`GRAPH <source> { … }` block") && msg.contains(GS_ID),
            "a SPARQL user must be shown SPARQL scoping syntax: {msg}"
        );
        assert!(
            !msg.contains("\"graph\""),
            "and must not be shown the JSON-LD form: {msg}"
        );
    }

    /// JSON-LD scopes with a `"graph"` object, not a `GRAPH` keyword. Telling a
    /// JSON-LD user to write SPARQL is the same defect as telling them nothing —
    /// this is the reachable half of the review's query-language finding.
    #[tokio::test]
    async fn a_jsonld_refusal_names_jsonld_scoping_syntax() {
        let fluree = FlureeBuilder::memory().build_memory();
        let db = graph_source_view();
        let stub = StubProvider::new();
        let q = serde_json::json!({
            "select": ["?o"],
            "where": [
                {"@id": N1, EDGE_PRED: "?o"},
                ["graph", GS_ID, {"@id": "?a", EDGE_PRED: "?c"}]
            ]
        });
        let msg = fluree
            .query_view_with_r2rml_options(&db, &q, &stub, &stub, QueryExecutionOptions::default())
            .await
            .expect_err("the stranded top-level pattern is refused")
            .to_string();
        assert!(
            msg.contains("\"graph\": \"source\"") && msg.contains("\"where\""),
            "a JSON-LD user must be shown the JSON-LD scoping form: {msg}"
        );
        assert!(
            !msg.contains("GRAPH <source>"),
            "and must not be shown SPARQL syntax: {msg}"
        );
    }

    /// The review asked whether a Cypher user can be told to write `GRAPH { }`,
    /// which is not Cypher syntax. They cannot: Cypher lowering emits no
    /// `Pattern::Graph`, so `maybe_wrap_for_graph_source` always wraps a Cypher
    /// query whole and never strands a top-level pattern for the guard to find.
    /// This pins that structurally — if Cypher ever gains a graph-scoping
    /// construct, this test fails and the Cypher wording stops being hypothetical.
    #[tokio::test]
    async fn cypher_over_a_graph_source_cannot_reach_the_graph_block_advice() {
        let fluree = FlureeBuilder::memory().build_memory();
        let db = graph_source_view().with_default_context(Some(
            serde_json::json!({"@vocab": "http://fs3.example/vocab#"}),
        ));

        for cypher in [
            "MATCH (a)-[:edge]->(b) RETURN b",
            // A variable-length pattern, which lowers to Pattern::PropertyPath —
            // the kind the other guard refuses when it sits outside a scope.
            "MATCH (a)-[:edge*1..3]->(b) RETURN b",
        ] {
            let result = fluree.query_cypher(&db, cypher).await;
            let msg = match &result {
                Ok(_) => String::new(),
                Err(e) => e.to_string(),
            };
            assert!(
                !msg.contains("cannot evaluate") && !msg.contains("Move them inside"),
                "Cypher must not be handed graph-block advice for `{cypher}`: {msg}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Dataset shapes
    // -----------------------------------------------------------------------

    /// A hand-built IR query: one stranded top-level triple beside a scoped
    /// block. Built directly so the dataset gates can be exercised without
    /// standing up a resolvable multi-source dataset.
    fn stranded_query() -> fluree_db_query::ir::Query {
        use fluree_db_query::ir::triple::{Ref, Term, TriplePattern};
        use fluree_db_query::ir::{GraphName, Pattern, Query, QueryOutput};

        let mut vars = fluree_db_query::VarRegistry::new();
        let o = vars.get_or_insert("?o");
        let mut query = Query::new(fluree_graph_json_ld::ParsedContext::default());
        query.patterns = vec![
            Pattern::Triple(TriplePattern::new(Ref::Var(o), Ref::Var(o), Term::Var(o))),
            Pattern::Graph {
                name: GraphName::Iri(GS_ID.into()),
                patterns: vec![],
            },
        ];
        query.output = QueryOutput::select_all(vec![o]);
        query
    }

    fn graph_source_view_named(gs_id: &str) -> GraphDb {
        let mut db = graph_source_view();
        db.graph_source_id = Some(gs_id.into());
        db
    }

    /// With several *different* sources in the default set, the refusal must
    /// name all of them: "move it into `GRAPH <primary>`" would be advice to
    /// silently exclude the rest.
    #[test]
    fn a_multi_source_dataset_refusal_names_every_source() {
        let dataset = crate::view::DataSetDb::new()
            .with_default(graph_source_view_named("warehouse-a:main"))
            .with_default(graph_source_view_named("warehouse-b:main"));

        let err = super::guard_dataset_graph_source_patterns(
            &dataset,
            &stranded_query(),
            super::QuerySyntax::Sparql,
        )
        .expect_err("all defaults are graph sources, so the stranded triple is refused");
        let msg = err.to_string();

        assert!(
            msg.contains("warehouse-a:main") && msg.contains("warehouse-b:main"),
            "every source in the default set must be named: {msg}"
        );
        assert!(
            msg.contains("graph sources"),
            "and the subject must be plural: {msg}"
        );
        assert!(
            !msg.contains("drop the explicit block"),
            "the drop-the-block escape must be withheld — it would scope the query to one \
             source and exclude the others: {msg}"
        );
    }

    /// A `fromNamed`-only dataset arrives with an empty default set — the shape
    /// #1631 settled on when it stopped injecting the ledger as a default graph.
    /// Returning `Ok` here is correct, not a gap: SPARQL §13.2 gives such a query
    /// an empty default graph, so its top-level patterns are *supposed* to match
    /// nothing. That empty result is the specified answer, and refusing it would
    /// reject a conformant query — unlike the silent-empty this guard exists to
    /// catch, which is an unroutable pattern over a source that does hold data.
    ///
    /// #1631 reached the same conclusion from the other side: rather than refuse
    /// this shape it *warns* on it ("`fromNamed` without `from` leaves the
    /// default graph empty"). Refusing here would have overridden that warning
    /// with a 400 on a conformant query.
    ///
    /// Asserted where the semantics are actually decided — the dataset spec
    /// parsed from a real `fromNamed`-only body — rather than assuming the
    /// dataset shape it produces.
    #[test]
    fn a_named_only_body_leaves_the_default_graph_empty() {
        let body = serde_json::json!({
            "fromNamed": [GS_ID],
            "select": ["?o"],
            "where": [
                {"@id": N1, EDGE_PRED: "?o"},
                ["graph", GS_ID, {"@id": "?a", EDGE_PRED: "?c"}]
            ]
        });
        let (spec, _) = crate::dataset::DatasetSpec::from_query_json(&body)
            .expect("a fromNamed-only body parses");
        assert!(
            spec.default_graphs.is_empty(),
            "fromNamed without from leaves the default graph empty (SPARQL 1.1 §13.2)"
        );
        assert_eq!(
            spec.named_graphs.len(),
            1,
            "the source is a named graph only"
        );

        let dataset =
            crate::view::DataSetDb::new().with_named(GS_ID, graph_source_view_named(GS_ID));
        assert!(dataset.default.is_empty(), "which is this dataset shape");

        super::guard_dataset_graph_source_patterns(
            &dataset,
            &stranded_query(),
            super::QuerySyntax::JsonLd,
        )
        .expect("an empty default graph makes an empty result the spec answer, not a defect");
    }

    /// The same combination driven end-to-end through the real connection-query
    /// path against a registered graph source: body → dataset spec → dataset →
    /// guard → execution. The `where` clause deliberately mixes a stranded
    /// top-level pattern with a `["graph", …]` block, the shape that suppresses
    /// the auto-wrap and reaches the guard, so an accidental refusal would
    /// surface here as an error rather than as an empty result.
    #[tokio::test]
    async fn a_named_only_graph_source_query_is_answered_not_refused() {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("gsguard-src").await.unwrap();
        let txn = serde_json::json!({
            "@context": {"ex": "http://example.org/ns/"},
            "insert": [{"@id": "ex:a1", "@type": "ex:Article", "ex:title": "Graph sources"}]
        });
        let _ledger = fluree.insert(ledger, &txn).await.unwrap().ledger;

        // A BM25 source registers in the nameservice without the iceberg
        // feature, so the dataset resolves a real graph-source view.
        let index_query = serde_json::json!({
            "@context": {"ex": "http://example.org/ns/"},
            "where": [{"@id": "?x", "@type": "ex:Article"}],
            "select": {"?x": ["@id", "ex:title"]}
        });
        let gs = fluree
            .create_full_text_index(crate::Bm25CreateConfig::new(
                "gsguard-search",
                "gsguard-src:main",
                index_query,
            ))
            .await
            .expect("register the BM25 graph source");

        let body = serde_json::json!({
            "@context": {"ex": "http://example.org/ns/"},
            "fromNamed": [gs.graph_source_id],
            "select": ["?title"],
            "where": [
                {"@id": "?s", "ex:title": "?title"},
                ["graph", gs.graph_source_id, {"@id": "?x", "ex:title": "?t"}]
            ]
        });
        let result = fluree.query_connection(&body).await;
        assert!(
            result.is_ok(),
            "a fromNamed-only query must be answered (empty, per §13.2), not refused: {:?}",
            result.err()
        );
    }

    /// The complement: a native member in the default set means top-level
    /// patterns have a real index to read, so nothing is refused.
    #[test]
    fn a_dataset_with_a_native_member_is_not_refused() {
        let mut native = graph_source_view();
        native.graph_source_id = None;
        let dataset = crate::view::DataSetDb::new()
            .with_default(graph_source_view_named("warehouse-a:main"))
            .with_default(native);

        super::guard_dataset_graph_source_patterns(
            &dataset,
            &stranded_query(),
            super::QuerySyntax::Sparql,
        )
        .expect("a native default graph can answer top-level patterns");
    }

    /// The guard is gated on the view being a graph source. A native ledger has
    /// a real index, so its property paths must keep executing — including on
    /// the plain `query` path, which shares the check.
    #[tokio::test]
    async fn a_native_ledger_still_evaluates_property_paths() {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("pathdb").await.unwrap();
        let txn = serde_json::json!({
            "insert": [
                {"@id": "http://example.org/n1", "http://example.org/edge": {"@id": "http://example.org/n2"}},
                {"@id": "http://example.org/n2", "http://example.org/edge": {"@id": "http://example.org/n3"}}
            ]
        });
        let _ledger = fluree.update(ledger, &txn).await.unwrap().ledger;

        let db = fluree.db("pathdb:main").await.unwrap();
        let result = fluree
            .query(
                &db,
                "SELECT ?o WHERE { <http://example.org/n1> <http://example.org/edge>+ ?o }",
            )
            .await
            .expect("a native ledger must still evaluate `edge+`");
        let rows: usize = result.batches.iter().map(fluree_db_query::Batch::len).sum();
        assert_eq!(rows, 2, "`edge+` from n1 reaches n2 and n3");
    }
}
