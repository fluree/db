use std::sync::Arc;

use serde_json::Value as JsonValue;

use crate::view::QueryInput;
use crate::{
    ApiError, Batch, DatasetSpec, ExecutableQuery, GovernanceOptions, OverlayProvider, Result,
    Tracker, TrackingOptions, VarRegistry,
};

use fluree_db_binary_index::BinaryGraphView;
use fluree_db_core::LedgerSnapshot;
use fluree_db_query::ir::Query;
use fluree_db_query::parse::parse_query;

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
) -> Result<(VarRegistry, Query)> {
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
) -> Result<(VarRegistry, Query)> {
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

/// Parse a Cypher (openCypher 9) query and prepare it for execution.
///
/// Cypher has no prologue/prefix syntax of its own. Bare identifiers
/// (labels/types/property keys) are plain namespace-0 names by
/// default; the ledger's default JSON-LD context can opt into RDF
/// compat by supplying `@vocab` (a prefix for bare identifiers) and
/// named term mappings (per-name overrides).
pub(crate) fn parse_cypher_to_ir(
    cypher: &str,
    snapshot: &LedgerSnapshot,
    default_context: Option<&JsonValue>,
    params: Option<&fluree_db_cypher::ParamMap>,
    overlay: Option<(&dyn OverlayProvider, u16)>,
    policy: Option<&fluree_db_query::policy::QueryPolicyEnforcer>,
) -> Result<(VarRegistry, Query)> {
    let ast = substituted_cypher_ast(cypher, params)?;
    lower_cypher_ast_to_ir(&ast, snapshot, default_context, overlay, policy)
}

/// Statements longer than this are parsed but never cached: unique bulk
/// payloads (inline CREATE data) would evict the short, hot statements the
/// cache exists for, and entry count — not bytes — bounds the LRU.
const MAX_CACHED_STATEMENT_LEN: usize = 8 * 1024;

/// Process-wide parsed-AST LRU, keyed on statement text.
///
/// The cached value is the **pre-substitution** AST: text-only key, ledger
/// independent, immutable. Callers clone the `Arc`'d AST and run
/// `substitute_params` on the clone, so per-request cost under a hit is
/// clone + substitute instead of a full parse. Capacity comes from
/// `FLUREE_CYPHER_AST_CACHE` (entries, default 512; `0` disables), read once.
#[allow(clippy::type_complexity)]
fn cypher_ast_cache(
) -> Option<&'static parking_lot::Mutex<lru::LruCache<Box<str>, Arc<fluree_db_cypher::CypherAst>>>>
{
    use std::num::NonZeroUsize;
    use std::sync::OnceLock;
    #[allow(clippy::type_complexity)]
    static CACHE: OnceLock<
        Option<parking_lot::Mutex<lru::LruCache<Box<str>, Arc<fluree_db_cypher::CypherAst>>>>,
    > = OnceLock::new();
    CACHE
        .get_or_init(|| {
            let capacity = std::env::var("FLUREE_CYPHER_AST_CACHE")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(512);
            NonZeroUsize::new(capacity).map(|c| parking_lot::Mutex::new(lru::LruCache::new(c)))
        })
        .as_ref()
}

/// Parse Cypher source through the process-wide AST cache.
///
/// Only successful parses are cached; parse failures re-parse on every call
/// so diagnostics stay exact. Returns the shared pre-substitution AST — do
/// not mutate through the `Arc`; clone first (see [`substituted_cypher_ast`]).
pub(crate) fn parse_cypher_ast_cached(cypher: &str) -> Result<Arc<fluree_db_cypher::CypherAst>> {
    if let Some(cache) = cypher_ast_cache() {
        if let Some(hit) = cache.lock().get(cypher) {
            return Ok(Arc::clone(hit));
        }
    }
    let out = fluree_db_cypher::parse_cypher(cypher);
    if out.has_errors() {
        let msg = out
            .diagnostics
            .iter()
            .map(|d| format!("{}: {}", d.code, d.message))
            .collect::<Vec<_>>()
            .join("; ");
        return Err(ApiError::cypher(msg, out.diagnostics));
    }
    let ast = Arc::new(
        out.ast
            .ok_or_else(|| ApiError::cypher("Cypher parse returned no AST", Vec::new()))?,
    );
    if cypher.len() <= MAX_CACHED_STATEMENT_LEN {
        if let Some(cache) = cypher_ast_cache() {
            cache.lock().put(cypher.into(), Arc::clone(&ast));
        }
    }
    Ok(ast)
}

/// Parse (via the AST cache) and return a param-substituted AST clone.
///
/// Substitution always runs (empty map when no params were supplied) so a
/// `$param` with no value reports a clear missing-parameter error rather
/// than reaching the lowering as an unsupported node.
pub(crate) fn substituted_cypher_ast(
    cypher: &str,
    params: Option<&fluree_db_cypher::ParamMap>,
) -> Result<fluree_db_cypher::CypherAst> {
    let ast = parse_cypher_ast_cached(cypher)?;
    let mut ast = (*ast).clone();
    let empty = fluree_db_cypher::ParamMap::new();
    fluree_db_cypher::substitute_params(&mut ast, params.unwrap_or(&empty))
        .map_err(|e| ApiError::cypher(e.to_string(), Vec::new()))?;
    Ok(ast)
}

/// Lower an already-parsed, param-substituted Cypher read AST to the shared
/// query IR. Used by `parse_cypher_to_ir` and by the conditional-write probes,
/// which build read ASTs in code.
pub(crate) fn lower_cypher_ast_to_ir(
    ast: &fluree_db_cypher::CypherAst,
    snapshot: &LedgerSnapshot,
    default_context: Option<&JsonValue>,
    overlay: Option<(&dyn OverlayProvider, u16)>,
    policy: Option<&fluree_db_query::policy::QueryPolicyEnforcer>,
) -> Result<(VarRegistry, Query)> {
    // Pull `@vocab` and named-term overrides out of the default
    // context, then build a `LoweringContext` and pass it to the
    // context-aware lower entry so the ledger's IRI mappings actually
    // apply to bare Cypher identifiers.
    let (vocab, overrides) = extract_cypher_iri_mapping(default_context);

    // Procedure shims (`CALL db.labels() YIELD …`) answer from ledger stats:
    // rewrite the statement to a constant-rows query, then lower that.
    let rewritten;
    let ast = if let fluree_db_cypher::ast::Statement::CallProcedure(call) = &ast.statement {
        let query = crate::cypher_procedures::procedure_call_query(
            call,
            snapshot,
            overlay.map(|(o, _)| o),
            vocab.as_deref(),
            &overrides,
            policy,
        )?;
        rewritten = fluree_db_cypher::CypherAst {
            statement: fluree_db_cypher::ast::Statement::Query(query),
            span: ast.span,
        };
        &rewritten
    } else {
        ast
    };

    let mut vars = VarRegistry::new();
    let mut ctx = fluree_db_cypher::LoweringContext::new(snapshot, &mut vars)
        .with_vocab_opt(vocab)
        .with_allow_full_scan(cypher_full_scan_enabled())
        .with_reified_edges_possible(reified_edges_possible(snapshot, overlay));
    if !overrides.is_empty() {
        ctx = ctx.with_overrides(overrides);
    }
    let parsed = fluree_db_cypher::lower_cypher_with_context(ast, &mut ctx)?;
    Ok((vars, parsed))
}

/// Whether any reified edge can exist in the queried view — the gate for
/// value-only bound relationship variables' per-hop OPTIONAL annotation
/// probe. Three tiers, each conservative (`true`) when it can't decide:
///
/// 1. Dictionary: `f:reifiesSubject` never entered the dictionary — no
///    annotation was ever written; certain `false`.
/// 2. Index stats: per-property counts show `f:reifiesSubject` facts.
/// 3. Overlay: one PSOT walk answering "any `f:reifiesSubject` flake in
///    novelty?", cached process-wide on `content_version` — callers that
///    can't supply the overlay stay conservative.
fn reified_edges_possible(
    snapshot: &LedgerSnapshot,
    overlay: Option<(&dyn OverlayProvider, u16)>,
) -> bool {
    let Some(reifies_sid) = snapshot.encode_iri(fluree_vocab::reifies_iris::SUBJECT) else {
        return false;
    };
    if index_has_reified_edges(snapshot, &reifies_sid) {
        return true;
    }
    match overlay {
        None => true,
        Some((overlay, g_id)) => overlay_has_reified_edges(overlay, g_id, &reifies_sid),
    }
}

fn index_has_reified_edges(snapshot: &LedgerSnapshot, reifies_sid: &fluree_db_core::Sid) -> bool {
    let Some(stats) = &snapshot.stats else {
        return true;
    };
    let Some(props) = &stats.properties else {
        return true;
    };
    props.iter().any(|p| {
        p.count > 0
            && p.sid.0 == reifies_sid.namespace_code
            && p.sid.1.as_str() == reifies_sid.name.as_ref()
    })
}

fn overlay_has_reified_edges(
    overlay: &dyn OverlayProvider,
    g_id: u16,
    reifies_sid: &fluree_db_core::Sid,
) -> bool {
    use std::sync::OnceLock;
    type ReifiesCache = parking_lot::Mutex<lru::LruCache<(u64, u16), bool>>;
    static CACHE: OnceLock<ReifiesCache> = OnceLock::new();

    if overlay.is_effectively_empty() {
        return false;
    }
    let Some(version) = overlay.content_version() else {
        return true;
    };
    let cache = CACHE.get_or_init(|| {
        parking_lot::Mutex::new(lru::LruCache::new(
            std::num::NonZeroUsize::new(8).expect("nonzero"),
        ))
    });
    if let Some(&hit) = cache.lock().get(&(version, g_id)) {
        return hit;
    }
    let mut found = false;
    overlay.for_each_overlay_flake(
        g_id,
        fluree_db_core::IndexType::Psot,
        None,
        None,
        true,
        i64::MAX,
        &mut |flake| {
            if flake.p == *reifies_sid {
                found = true;
            }
        },
    );
    cache.lock().put((version, g_id), found);
    found
}

/// Whether bare `MATCH (n)` whole-graph scans are opted in for Cypher
/// (`FLUREE_CYPHER_ALLOW_FULL_SCAN=1|true`). Read once per process.
fn cypher_full_scan_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FLUREE_CYPHER_ALLOW_FULL_SCAN")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    })
}

/// Extract `@vocab` and bare-identifier → IRI overrides from a
/// JSON-LD `@context` object.
///
/// Shared by the read path (`parse_cypher_to_ir`) and the write path
/// (`Fluree::transact_cypher`) so both surfaces honor the same
/// ledger-context mappings.
pub(crate) fn extract_cypher_iri_mapping(
    default_context: Option<&JsonValue>,
) -> (Option<String>, std::collections::HashMap<String, String>) {
    let mut vocab = None;
    let mut overrides = std::collections::HashMap::new();
    if let Some(obj) = default_context.and_then(|v| v.as_object()) {
        if let Some(v) = obj.get("@vocab").and_then(|v| v.as_str()) {
            vocab = Some(v.to_string());
        }
        for (k, v) in obj {
            if k.starts_with('@') {
                continue;
            }
            if let Some(s) = v.as_str() {
                // Skip prefix-style entries (`"ex": "http://..."`); use
                // only full-term mappings. Heuristic: a prefix entry
                // ends in `#` or `/` and the key looks like a short
                // prefix. We don't try to be exhaustive here.
                if s.ends_with('#') || s.ends_with('/') {
                    continue;
                }
                overrides.insert(k.clone(), s.to_string());
            }
        }
    }
    (vocab, overrides)
}

/// Prepare a parsed query for execution.
pub(crate) fn prepare_for_execution(parsed: &Query) -> ExecutableQuery {
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
    parsed: Query,
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
        // Default: not a graph-source result. The R2RML execution path
        // (`query_view_with_r2rml_options`) overrides this to true so the
        // sparql_json formatter CURIE-compacts graph-source IRIs (F9).
        from_graph_source: false,
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

    // Parse errors: return ApiError::sparql with structured diagnostics.
    //
    // Error-severity diagnostics are authoritative EVEN WHEN the parser's
    // error recovery produced an AST. A recovered AST silently drops or
    // rewrites exactly the parts of the query the parser flagged, so
    // executing it answers a different question than the user asked
    // (docs/audit/burn-down/ROADMAP.md §1 addendum: the API previously
    // swallowed these diagnostics whenever an AST survived recovery).
    // Recovery itself is unchanged — `parse_sparql` still returns the AST
    // plus diagnostics for tooling — and warning-severity diagnostics never
    // reject. The SPARQL UPDATE path (tx_builder::parse_and_lower_sparql_update)
    // already enforces the same rule.
    let has_parse_errors = parse_output.has_errors();
    let ast = match parse_output.ast {
        Some(ast) if !has_parse_errors => ast,
        _ => {
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

/// Charge the one-time query floor at execution entry.
///
/// Every fuel-tracked query is charged [`schedule::QUERY_FLOOR_MICRO_FUEL`]
/// once, before parsing, so that (a) a successful query always reports at least
/// 1.000 fuel, (b) a parse/plan error with tracking still reports the floor, and
/// (c) the floor counts toward any `max-fuel` budget. No-op when fuel isn't
/// tracked (disabled tracker or `track_fuel == false`), since `consume_fuel`
/// short-circuits in that case.
///
/// Returns the same `FuelExceededError` a mid-execution overrun would, so a
/// `max-fuel` below the floor is rejected up front.
pub(crate) fn charge_query_floor(
    tracker: &Tracker,
) -> std::result::Result<(), fluree_db_core::FuelExceededError> {
    tracker.consume_fuel(fluree_db_core::tracking::schedule::QUERY_FLOOR_MICRO_FUEL)
}

/// Tracker for a "tracked" query path: an explicit `tracking_override` if given,
/// otherwise the per-input default — opts-derived for JSON-LD, all-enabled for
/// SPARQL (which has no `opts` carrier). This is the single derivation shared by
/// the view/dataset tracked entry points and [`floor_only_tally`].
pub(crate) fn tracked_query_tracker(
    input: &QueryInput<'_>,
    tracking_override: &Option<TrackingOptions>,
) -> Tracker {
    match tracking_override {
        Some(opts) => Tracker::new(opts.clone()),
        None => match input {
            QueryInput::JsonLd(json) => tracker_for_tracked_endpoint(json),
            QueryInput::Sparql(_) => Tracker::new(TrackingOptions::all_enabled()),
        },
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
        fluree_db_query::QueryError::Cancelled { .. } => 408,
        fluree_db_query::QueryError::InvalidQuery(_) => 400,
        fluree_db_query::QueryError::InvalidFilter(_) => 400,
        fluree_db_query::QueryError::InvalidExpression(_) => 400,
        _ => 500,
    }
}

pub(crate) fn parse_dataset_spec(
    query_json: &JsonValue,
) -> Result<(DatasetSpec, GovernanceOptions)> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cypher_ast_cache_returns_shared_ast() {
        let text = "MATCH (n:CacheHitTest) RETURN n";
        let first = parse_cypher_ast_cached(text).expect("parse");
        let second = parse_cypher_ast_cached(text).expect("parse");
        assert!(
            Arc::ptr_eq(&first, &second),
            "same statement text must hit the cache"
        );
    }

    #[test]
    fn cypher_ast_cache_skips_oversized_statements() {
        let filler = "x".repeat(MAX_CACHED_STATEMENT_LEN);
        let text = format!("MATCH (n:Big) WHERE n.name <> \"{filler}\" RETURN n");
        let first = parse_cypher_ast_cached(&text).expect("parse");
        let second = parse_cypher_ast_cached(&text).expect("parse");
        assert!(
            !Arc::ptr_eq(&first, &second),
            "oversized statements must not be cached"
        );
        assert_eq!(first, second);
    }

    #[test]
    fn cypher_parse_errors_are_not_cached() {
        let text = "MATCH (n:Broken RETURN";
        assert!(parse_cypher_ast_cached(text).is_err());
        assert!(parse_cypher_ast_cached(text).is_err());
    }

    #[test]
    fn substitution_clones_and_leaves_cached_ast_pristine() {
        let text = "MATCH (n:SubstTest {id: $id}) RETURN n";
        let mut params_a = fluree_db_cypher::ParamMap::new();
        params_a.insert("id".into(), serde_json::json!(1));
        let mut params_b = fluree_db_cypher::ParamMap::new();
        params_b.insert("id".into(), serde_json::json!(2));

        let a = substituted_cypher_ast(text, Some(&params_a)).expect("subst a");
        let b = substituted_cypher_ast(text, Some(&params_b)).expect("subst b");
        assert_ne!(a, b, "different params must produce different ASTs");

        // Missing param still errors through the cached path.
        assert!(substituted_cypher_ast(text, None).is_err());
    }
}
