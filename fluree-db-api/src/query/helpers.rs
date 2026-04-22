use std::sync::Arc;

use serde_json::Value as JsonValue;

use crate::{
    ApiError, Batch, DatasetSpec, ExecutableQuery, OverlayProvider, QueryConnectionOptions, Result,
    Tracker, TrackingOptions, VarRegistry,
};

use fluree_db_binary_index::BinaryGraphView;
use fluree_db_core::LedgerSnapshot;
use fluree_db_query::parse::{parse_query, ParsedQuery};

use super::QueryResult;

// =============================================================================
// Query Parsing Helpers
// =============================================================================

/// Parse a JSON-LD query and prepare it for execution.
///
/// If the query has no `@context` (or `context`) key and a `default_context`
/// is provided, the default is injected so that prefix resolution succeeds
/// without requiring the caller to repeat prefixes on every query.
///
/// Returns the variable registry and parsed query.
pub(crate) fn parse_jsonld_query(
    query_json: &JsonValue,
    snapshot: &LedgerSnapshot,
    default_context: Option<&JsonValue>,
    strict_compact_iri: Option<bool>,
) -> Result<(VarRegistry, ParsedQuery)> {
    let has_context = query_json.get("@context").is_some() || query_json.get("context").is_some();

    let effective_query;
    let query_ref = if !has_context {
        if let Some(ctx) = default_context {
            effective_query = {
                let mut q = query_json.clone();
                if let Some(obj) = q.as_object_mut() {
                    obj.insert("@context".to_string(), ctx.clone());
                }
                q
            };
            &effective_query
        } else {
            query_json
        }
    } else {
        query_json
    };

    let mut vars = VarRegistry::new();
    let parsed = parse_query(query_ref, snapshot, &mut vars, strict_compact_iri)?;
    Ok((vars, parsed))
}

/// Parse a SPARQL query and prepare it for execution.
///
/// If `default_context` is provided (a JSON-LD `@context` object mapping
/// short prefix → namespace IRI), any prefixes not explicitly declared in
/// the SPARQL prologue are injected as defaults. This lets queries omit
/// `PREFIX` declarations for prefixes that were established during import.
///
/// Returns the variable registry and parsed query.
pub(crate) fn parse_sparql_to_ir(
    sparql: &str,
    snapshot: &LedgerSnapshot,
    default_context: Option<&JsonValue>,
) -> Result<(VarRegistry, ParsedQuery)> {
    let mut ast = parse_and_validate_sparql(sparql)?;

    // Inject default prefixes from the ledger's stored @context, but only
    // when the query declares no PREFIX of its own. Any explicit PREFIX
    // declaration (even `PREFIX : <>`) signals the user is managing their
    // own prefix environment, so we skip default injection entirely.
    if ast.prologue.prefixes.is_empty() {
        if let Some(ctx_obj) = default_context.and_then(|v| v.as_object()) {
            for (short, iri_val) in ctx_obj {
                if let Some(iri) = iri_val.as_str() {
                    ast.prologue
                        .prefixes
                        .push(fluree_db_sparql::ast::PrefixDecl::new(
                            short.as_str(),
                            iri,
                            fluree_db_sparql::SourceSpan::new(0, 0),
                        ));
                }
            }
        }
    }

    let mut vars = VarRegistry::new();
    let parsed =
        fluree_db_sparql::lower_sparql_with_source(&ast, snapshot, &mut vars, Some(sparql))?;
    Ok((vars, parsed))
}

/// Prepare a parsed query for execution.
pub(crate) fn prepare_for_execution(parsed: &ParsedQuery) -> ExecutableQuery {
    ExecutableQuery::simple(parsed.clone())
}

// =============================================================================
// Query Result Building
// =============================================================================

/// Build a QueryResult from execution results and parsed query metadata.
///
/// This consolidates the repetitive QueryResult construction that appears
/// in every query method.
pub(crate) fn build_query_result(
    vars: VarRegistry,
    parsed: ParsedQuery,
    batches: Vec<Batch>,
    t: Option<i64>,
    novelty: Option<Arc<dyn OverlayProvider>>,
    binary_graph: Option<BinaryGraphView>,
) -> QueryResult {
    QueryResult {
        vars,
        t,
        novelty,
        context: parsed.context,
        orig_context: parsed.orig_context,
        output: parsed.output,
        batches,
        binary_graph,
        graph_select: parsed.graph_select,
    }
}

// =============================================================================
// R2RML Provider Macro
// =============================================================================

/// Macro to create the appropriate R2RML provider based on feature flags.
///
/// When the `iceberg` feature is enabled, creates a `FlureeR2rmlProvider` using the given
/// Fluree instance. Otherwise, creates a `NoOpR2rmlProvider`.
///
/// # Usage
///
/// ```ignore
/// // In a method on Fluree
/// let r2rml_provider = r2rml_provider!(self);
/// ```
///
/// This replaces the repetitive pattern:
/// ```ignore
/// #[cfg(feature = "iceberg")]
/// let r2rml_provider = crate::graph_source::FlureeR2rmlProvider::new(self);
/// #[cfg(not(feature = "iceberg"))]
/// let r2rml_provider = NoOpR2rmlProvider::new();
/// ```
#[macro_export]
macro_rules! r2rml_provider {
    ($fluree:expr) => {{
        #[cfg(feature = "iceberg")]
        {
            $crate::graph_source::FlureeR2rmlProvider::new($fluree)
        }
        #[cfg(not(feature = "iceberg"))]
        {
            $crate::NoOpR2rmlProvider::new()
        }
    }};
}

pub(crate) fn parse_and_validate_sparql(sparql: &str) -> Result<fluree_db_sparql::SparqlAst> {
    let parse_output = fluree_db_sparql::parse_sparql(sparql);

    // Parse errors: no AST, return ApiError::sparql with structured diagnostics.
    let ast = match parse_output.ast {
        Some(ast) => ast,
        None => {
            let errors: Vec<_> = parse_output
                .diagnostics
                .into_iter()
                .filter(|d| d.severity == fluree_db_sparql::Severity::Error)
                .collect();
            let message = errors
                .first()
                .map(|d| d.message.clone())
                .unwrap_or_else(|| "SPARQL parse error".to_string());
            return Err(ApiError::sparql(message, errors));
        }
    };

    // Validation errors: validate against Fluree capabilities/restrictions.
    let capabilities = fluree_db_sparql::Capabilities::default();
    let diagnostics = fluree_db_sparql::validate(&ast, &capabilities);
    let errors: Vec<_> = diagnostics
        .into_iter()
        .filter(|d| d.severity == fluree_db_sparql::Severity::Error)
        .collect();
    if !errors.is_empty() {
        let message = errors
            .first()
            .map(|d| d.message.clone())
            .unwrap_or_else(|| "SPARQL validation error".to_string());
        return Err(ApiError::sparql(message, errors));
    }

    Ok(ast)
}

/// Creates a tracker for "tracked" query endpoints.
///
/// If the query specifies tracking options via `opts.meta`, those are used.
/// Otherwise, all tracking (time, fuel, policy) is enabled by default since
/// the caller explicitly used a "tracked" endpoint.
pub(crate) fn tracker_for_tracked_endpoint(query_json: &JsonValue) -> Tracker {
    let opts = query_json.as_object().and_then(|o| o.get("opts"));
    let tracking = TrackingOptions::from_opts_value(opts);

    // If no tracking options were specified, enable all tracking by default
    if tracking.any_enabled() {
        Tracker::new(tracking)
    } else {
        Tracker::new(TrackingOptions::all_enabled())
    }
}

pub(crate) fn tracker_for_limits(query_json: &JsonValue) -> Tracker {
    let opts = query_json.as_object().and_then(|o| o.get("opts"));
    let tracking = TrackingOptions::from_opts_value(opts);
    match tracking.max_fuel.filter(|limit| *limit > 0) {
        Some(limit) => Tracker::new(TrackingOptions {
            track_time: false,
            track_fuel: true,
            track_policy: false,
            max_fuel: Some(limit),
        }),
        None => Tracker::disabled(),
    }
}

pub(crate) fn status_for_query_error(err: &fluree_db_query::QueryError) -> u16 {
    match err {
        fluree_db_query::QueryError::FuelLimitExceeded(_) => 400,
        fluree_db_query::QueryError::InvalidQuery(_) => 400,
        fluree_db_query::QueryError::InvalidFilter(_) => 400,
        fluree_db_query::QueryError::InvalidExpression(_) => 400,
        _ => 500,
    }
}

pub(crate) fn parse_dataset_spec(
    query_json: &JsonValue,
) -> Result<(DatasetSpec, QueryConnectionOptions)> {
    DatasetSpec::from_query_json(query_json).map_err(|e| ApiError::query(e.to_string()))
}

/// Extract dataset spec from a SPARQL AST's dataset clause (FROM / FROM NAMED).
pub(crate) fn extract_sparql_dataset_spec(
    ast: &fluree_db_sparql::SparqlAst,
) -> Result<DatasetSpec> {
    let dataset_clause = match &ast.body {
        fluree_db_sparql::ast::QueryBody::Select(q) => q.dataset.as_ref(),
        fluree_db_sparql::ast::QueryBody::Ask(q) => q.dataset.as_ref(),
        fluree_db_sparql::ast::QueryBody::Describe(q) => q.dataset.as_ref(),
        fluree_db_sparql::ast::QueryBody::Construct(q) => q.dataset.as_ref(),
        fluree_db_sparql::ast::QueryBody::Update(_) => None,
    };

    match dataset_clause {
        Some(clause) => {
            DatasetSpec::from_sparql_clause(clause).map_err(|e| ApiError::query(e.to_string()))
        }
        None => Ok(DatasetSpec::default()),
    }
}
