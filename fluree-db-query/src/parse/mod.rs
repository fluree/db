//! JSON-LD Query Parser
//!
//! Parses JSON-based JSON-LD queries into the query execution engine's IR.
//!
//! # Architecture
//!
//! The parser operates in two phases:
//!
//! 1. **Parse phase** (`parse_query_ast`): Pure, sync transformation from JSON to
//!    an unresolved AST with expanded IRIs but no database-specific encoding.
//!    This phase uses `json-ld-rust` for context parsing and IRI expansion.
//!
//! 2. **Lower phase** (`lower_query`): Converts the AST to the execution IR,
//!    encoding IRIs to Sids via the `IriEncoder` trait. This keeps the parser
//!    WASM-compatible.
//!
//! See [`parse_query_ast`] for the pure parsing phase (no DB required),
//! and `lower_query` for the lowering phase (requires `IriEncoder`).

pub mod ast;
pub mod encode;
pub mod error;
pub mod filter_common;
pub mod filter_data;
pub mod filter_sexpr;
pub mod lower;
pub mod node_map;
pub mod options;
pub mod path_expr;
pub mod policy;
pub mod sexpr_tokenize;
pub mod values;
pub mod where_clause;

pub use ast::{
    encode_datatype_constraint, LiteralValue, UnresolvedAggregateFn, UnresolvedAggregateSpec,
    UnresolvedConstructTemplate, UnresolvedDatatypeConstraint, UnresolvedExpression,
    UnresolvedFilterValue, UnresolvedForwardItem, UnresolvedHydrationSpec,
    UnresolvedNestedSelectSpec, UnresolvedOptions, UnresolvedPattern, UnresolvedQuery,
    UnresolvedRoot, UnresolvedSortDirection, UnresolvedSortSpec, UnresolvedTerm,
    UnresolvedTriplePattern, UnresolvedValue,
};
pub use encode::{IriEncoder, MemoryEncoder, NoEncoder};
pub use error::{ParseError, Result};
pub(crate) use lower::{lower_query, SelectMode};
pub use lower::{lower_unresolved_pattern, lower_unresolved_patterns};
pub use policy::{JsonLdParseCtx, JsonLdParsePolicy};
pub use where_clause::parse_where_with_counters;

use crate::ir::{Expression, Query};
use crate::var_registry::VarRegistry;
use ast::{UnresolvedColumn, UnresolvedPathExpr, UnresolvedProjection};
use fluree_graph_json_ld::{parse_context, ParsedContext};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;

/// Map from context term name to its parsed `@path` expression.
///
/// Extracted from the raw query `@context` JSON during parse phase.
/// Used by node-map parsing to recognise property-path aliases.
pub type PathAliasMap = HashMap<String, UnresolvedPathExpr>;

// Re-export is_variable from node_map for use within this module
use node_map::is_variable;

/// Parse a JSON-LD query into an unresolved AST and determine the select mode.
///
/// This is phase 1 of parsing: pure, sync transformation from JSON to AST.
/// No database access is required - IRI expansion uses the provided context.
///
/// # Arguments
///
/// * `json` - The JSON query value
///
/// # Returns
///
/// A tuple of `(UnresolvedQuery, SelectMode)` where the mode is derived from
/// the query's select/selectOne/construct/ask key.
pub(crate) fn parse_query_ast(
    json: &JsonValue,
    strict_override: Option<bool>,
) -> Result<(UnresolvedQuery, SelectMode)> {
    // Use shared counters so nested subqueries can generate unique implicit vars
    // across the full query tree (prevents collisions like ?__s0 between parent/subquery).
    let mut subject_counter: u32 = 0;
    let mut nested_counter: u32 = 0;
    parse_query_ast_internal(
        json,
        &mut subject_counter,
        &mut nested_counter,
        strict_override,
    )
}

fn parse_query_ast_internal(
    json: &JsonValue,
    subject_counter: &mut u32,
    nested_counter: &mut u32,
    strict_override: Option<bool>,
) -> Result<(UnresolvedQuery, SelectMode)> {
    let obj = json
        .as_object()
        .ok_or_else(|| ParseError::InvalidWhere("Query must be an object".to_string()))?;

    // Parse context (accept both "@context" and "context")
    let context_val = obj
        .get("@context")
        .or_else(|| obj.get("context"))
        .unwrap_or(&JsonValue::Null);

    let context = parse_context(&normalize_context_value(context_val))?;

    // Resolve parse policy BEFORE extracting path aliases so that any
    // compact IRIs inside @path expressions honor `opts.strictCompactIri`.
    let parse_policy = policy::resolve_parse_policy(strict_override, obj);
    let path_aliases = extract_path_aliases(context_val, &context, parse_policy)?;
    let ctx = JsonLdParseCtx::new(context.clone(), path_aliases, parse_policy);

    let mut query = UnresolvedQuery::new(context.clone());

    // Store original context JSON for CONSTRUCT output (only if not null)
    if !context_val.is_null() {
        query.orig_context = Some(context_val.clone());
    }

    // Check for CONSTRUCT query first
    if let Some(construct_val) = obj.get("construct") {
        return parse_construct_query(
            obj,
            construct_val,
            &ctx,
            query,
            subject_counter,
            nested_counter,
        );
    }

    // Check for ASK query — boolean existence check, no select clause.
    // The value of `ask` IS the where clause (array or object), e.g.:
    //   { "ask": [{ "@id": "?person", "ex:name": "Alice" }] }
    //   { "ask": { "@id": "?person", "ex:name": "Alice" } }
    if let Some(ask_val) = obj.get("ask") {
        if !ask_val.is_array() && !ask_val.is_object() {
            return Err(ParseError::InvalidWhere(
                "\"ask\" must be an array or object of where-clause patterns".to_string(),
            ));
        }
        let object_var_parsing = options::parse_object_var_parsing(obj);
        where_clause::parse_where_with_counters(
            ask_val,
            &ctx,
            &mut query,
            subject_counter,
            nested_counter,
            object_var_parsing,
        )?;
        // LIMIT 1 for efficiency — only need to know if any solution exists
        query.options.limit = Some(1);
        return Ok((query, SelectMode::Ask));
    }

    // Determine select mode based on which key is present.
    //
    // Compatibility notes:
    // - Support both camelCase and kebab-case variants:
    //   - `selectOne` / `select-one`
    //   - `selectDistinct` / `select-distinct`
    let mut implied_distinct = false;
    let (select, select_mode) =
        if let Some(select_one) = obj.get("selectOne").or_else(|| obj.get("select-one")) {
            (select_one, SelectMode::One)
        } else if let Some(select_distinct) = obj
            .get("selectDistinct")
            .or_else(|| obj.get("select-distinct"))
        {
            implied_distinct = true;
            (select_distinct, SelectMode::Many)
        } else if let Some(select) = obj.get("select") {
            (select, SelectMode::Many)
        } else {
            return Err(ParseError::MissingField(
                "select, selectOne, select-one, selectDistinct, select-distinct, construct, or ask",
            ));
        };

    // Wildcard `select: "*"` lives entirely in the projection; otherwise
    // dispatch on JSON shape.
    if select.as_str() == Some("*") {
        query.select = UnresolvedProjection::Wildcard;
    } else {
        parse_select(select, &ctx, &mut query)?;
    }

    // Parse depth parameter and apply to hydrationion if present
    if let Some(gs) = query.hydration_mut() {
        gs.depth = options::parse_depth(obj)?;
    }

    // Parse top-level VALUES (optional) - mirrors the `:values` initial solution seed.
    if let Some(values_val) = obj.get("values") {
        if !values_val.is_null() {
            let values_pat = values::parse_values_clause(values_val, &ctx)?;
            // Place VALUES first so it seeds the pipeline before WHERE patterns.
            query.patterns.insert(0, values_pat);
        }
    }

    // Parse where clause.
    //
    // Hydration queries like {"select": {"ex:dan": ["*"]}} are allowed.
    // without an explicit WHERE clause (root may be an IRI constant).
    let object_var_parsing = options::parse_object_var_parsing(obj);
    if let Some(where_clause) = obj.get("where") {
        where_clause::parse_where_with_counters(
            where_clause,
            &ctx,
            &mut query,
            subject_counter,
            nested_counter,
            object_var_parsing,
        )?;
    } else if query.hydration().is_some() {
        // Allowed: hydration-only query with no WHERE.
        // Execution will produce an empty solution set, and hydration formatting will use the root
        // (constant root emits one row; variable root yields no rows).
    } else if !query
        .patterns
        .iter()
        .any(|p| matches!(p, UnresolvedPattern::Values { .. }))
    {
        return Err(ParseError::MissingField("where"));
    }

    // Parse query options (limit, offset, orderBy, groupBy, having, etc.)
    // Preserve aggregates collected from S-expression syntax in parse_select
    let aggregates_from_select = std::mem::take(&mut query.options.aggregates);
    let mut opts = options::parse_options(obj, filter_data::parse_filter_expr)?;
    // Merge aggregates from HAVING parsing with aggregates collected from select clause.
    // (HAVING may introduce synthetic aggregates like (count ?x) used only for filtering.)
    opts.aggregates.extend(aggregates_from_select);
    query.options = opts;

    // GROUP BY without explicit ORDER BY defaults to ordering by the group key(s).
    if query.options.order_by.is_empty() && !query.options.group_by.is_empty() {
        let mut seen_names: std::collections::HashSet<&str> = std::collections::HashSet::new();
        query.options.order_by = query
            .options
            .group_by
            .iter()
            .filter_map(|v| {
                let s = v.as_ref();
                if !s.starts_with('?') {
                    return None;
                }
                if !seen_names.insert(s) {
                    return None;
                }
                Some(crate::parse::ast::UnresolvedSortSpec::asc(s))
            })
            .collect();
    }
    // selectOne is semantically "first solution" — enforce LIMIT 1 at the query
    // level so execution can stop after one row, rather than materializing the
    // full sequence and discarding the rest at format time. Overrides any
    // user-provided limit because selectOne intent is unambiguous.
    if select_mode == SelectMode::One {
        query.options.limit = Some(1);
    }

    if implied_distinct {
        query.options.distinct = true;
        // select-distinct without explicit order defaults to sorting
        // by the selected variables (ascending) for deterministic paging.
        if query.options.order_by.is_empty() {
            let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
            query.options.order_by = query
                .select
                .columns()
                .iter()
                .filter_map(UnresolvedColumn::var_name)
                .filter(|s| s.starts_with('?'))
                .filter(|s| seen.insert(*s))
                .map(crate::parse::ast::UnresolvedSortSpec::asc)
                .collect();
        }
    }

    Ok((query, select_mode))
}

fn normalize_context_value(context_val: &JsonValue) -> JsonValue {
    if let JsonValue::Object(map) = context_val {
        if let Some(base) = map.get("@base") {
            if !map.contains_key("@vocab") {
                let mut out = map.clone();
                out.insert("@vocab".to_string(), base.clone());
                return JsonValue::Object(out);
            }
        }
    }
    context_val.clone()
}

/// Extract `@path` term definitions from the raw context JSON.
///
/// Scans the context for term definitions that contain `"@path"` and parses
/// each into an [`UnresolvedPathExpr`]. The `@path` value may be a SPARQL-
/// syntax string or an S-expression JSON array.
///
/// Handles all context shapes: null, string (vocab-only), object, and
/// array-of-contexts. For arrays, later definitions override earlier ones
/// (matching JSON-LD's "last context wins" semantics).
///
/// # Validation
///
/// - `@path` and `@reverse` on the same term → error (mutually exclusive).
/// - `@path` value must be a string or array → error otherwise.
fn extract_path_aliases(
    context_val: &JsonValue,
    parsed_context: &ParsedContext,
    policy: JsonLdParsePolicy,
) -> Result<PathAliasMap> {
    let mut aliases = PathAliasMap::new();
    // Build a temporary JsonLdParseCtx for path alias extraction. Aliases are
    // extracted before the real ctx is built, but we use the resolved policy
    // so @path expressions honor `opts.strictCompactIri`.
    let tmp_ctx = JsonLdParseCtx::new(parsed_context.clone(), PathAliasMap::new(), policy);
    extract_path_aliases_into(context_val, &tmp_ctx, &mut aliases)?;
    Ok(aliases)
}

/// Recursive helper that accumulates path aliases from a context value.
fn extract_path_aliases_into(
    context_val: &JsonValue,
    ctx: &JsonLdParseCtx,
    aliases: &mut PathAliasMap,
) -> Result<()> {
    match context_val {
        JsonValue::Null | JsonValue::String(_) => {
            // Null context or string vocab — no term definitions to inspect.
            Ok(())
        }
        JsonValue::Object(map) => {
            for (key, val) in map {
                // Skip JSON-LD keywords
                if key.starts_with('@') {
                    continue;
                }
                // Only inspect object-valued term definitions
                if let JsonValue::Object(term_def) = val {
                    if let Some(path_val) = term_def.get("@path") {
                        // Validate: @path and @reverse are mutually exclusive
                        if term_def.contains_key("@reverse") {
                            return Err(ParseError::InvalidContext(format!(
                                "term '{key}': @path and @reverse are mutually exclusive",
                            )));
                        }

                        let path_expr = match path_val {
                            JsonValue::String(s) => path_expr::parse_path_string(s, ctx)?,
                            JsonValue::Array(arr) => path_expr::parse_path_array(arr, ctx)?,
                            _ => {
                                return Err(ParseError::InvalidContext(format!(
                                    "term '{}': @path must be a string or array, got {}",
                                    key,
                                    json_type_name(path_val),
                                )));
                            }
                        };

                        // Last definition wins (matching JSON-LD override semantics)
                        aliases.insert(key.clone(), path_expr);
                    }
                }
            }
            Ok(())
        }
        JsonValue::Array(arr) => {
            // Array of contexts — process in order, last wins
            for item in arr {
                extract_path_aliases_into(item, ctx, aliases)?;
            }
            Ok(())
        }
        _ => {
            // Unexpected type (number, bool) — ignore, let json-ld parser handle errors
            Ok(())
        }
    }
}

/// Return a human-readable JSON type name for error messages.
fn json_type_name(val: &JsonValue) -> &'static str {
    match val {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

/// Parse a CONSTRUCT query
///
/// CONSTRUCT queries transform matched data into JSON-LD graph format.
/// Two forms are supported:
/// - Explicit template: `{"construct": {"@id": "?s", ...}, "where": {...}}`
/// - Shorthand: `{"construct": true, "where": {...}}` (uses WHERE as template)
fn parse_construct_query(
    obj: &serde_json::Map<String, JsonValue>,
    construct_val: &JsonValue,
    ctx: &JsonLdParseCtx,
    mut query: UnresolvedQuery,
    subject_counter: &mut u32,
    nested_counter: &mut u32,
) -> Result<(UnresolvedQuery, SelectMode)> {
    // Require WHERE clause
    let where_clause = obj.get("where").ok_or(ParseError::MissingField("where"))?;

    // Parse WHERE clause first (needed for both explicit and shorthand templates)
    let object_var_parsing = options::parse_object_var_parsing(obj);
    where_clause::parse_where_with_counters(
        where_clause,
        ctx,
        &mut query,
        subject_counter,
        nested_counter,
        object_var_parsing,
    )?;

    // Parse template into patterns
    let template_patterns = match construct_val {
        JsonValue::Bool(true) => {
            // Shorthand: use WHERE patterns as template (filter to triples only)
            query
                .patterns
                .iter()
                .filter(|p| matches!(p, UnresolvedPattern::Triple(_)))
                .cloned()
                .collect()
        }
        JsonValue::Bool(false) => {
            return Err(ParseError::InvalidConstruct(
                "construct: false is not valid; use select for non-CONSTRUCT queries".to_string(),
            ));
        }
        JsonValue::Object(_) | JsonValue::Array(_) => {
            // Explicit template: parse separately
            parse_construct_template(construct_val, ctx)?
        }
        _ => {
            return Err(ParseError::InvalidConstruct(
                "construct must be true or a template object/array".to_string(),
            ))
        }
    };

    query.construct_template = Some(ast::UnresolvedConstructTemplate::new(template_patterns));

    // Parse query options (limit, offset, orderBy, etc.)
    // Note: groupBy with CONSTRUCT will error at format time (Binding::Grouped unsupported)
    query.options = options::parse_options(obj, filter_data::parse_filter_expr)?;

    Ok((query, SelectMode::Construct))
}

/// Parse a CONSTRUCT template (explicit form)
///
/// Parses the template node-map(s) into unresolved triple patterns.
/// Only triple patterns are valid in templates (filters/optionals are ignored).
fn parse_construct_template(
    template: &JsonValue,
    ctx: &JsonLdParseCtx,
) -> Result<Vec<UnresolvedPattern>> {
    let mut subject_counter = 0u32;
    let mut nested_counter = 0u32;

    match template {
        JsonValue::Object(map) => {
            // Single node-map template
            let mut temp_query = UnresolvedQuery::new(ctx.context.clone());
            node_map::parse_node_map(
                map,
                ctx,
                &mut temp_query,
                &mut subject_counter,
                &mut nested_counter,
                true,
            )?;
            // Filter to triple patterns only (templates don't have filters/optionals)
            Ok(temp_query
                .patterns
                .into_iter()
                .filter(|p| matches!(p, UnresolvedPattern::Triple(_)))
                .collect())
        }
        JsonValue::Array(arr) => {
            // Array of node-map templates
            let mut patterns = Vec::new();
            for item in arr {
                if let JsonValue::Object(map) = item {
                    let mut temp_query = UnresolvedQuery::new(ctx.context.clone());
                    node_map::parse_node_map(
                        map,
                        ctx,
                        &mut temp_query,
                        &mut subject_counter,
                        &mut nested_counter,
                        true,
                    )?;
                    patterns.extend(
                        temp_query
                            .patterns
                            .into_iter()
                            .filter(|p| matches!(p, UnresolvedPattern::Triple(_))),
                    );
                } else {
                    return Err(ParseError::InvalidConstruct(
                        "construct array items must be objects".to_string(),
                    ));
                }
            }
            Ok(patterns)
        }
        _ => Err(ParseError::InvalidConstruct(
            "construct template must be an object or array".to_string(),
        )),
    }
}

/// Parse the select clause
///
/// Supports five forms:
/// 1. Single string: `"?x"` - single variable selection (unwrap)
/// 2. Simple array: `["?x", "?y"]` - flat variable selection
/// 3. Mixed array: `["?age", {"?person": ["*"]}]` - scalar vars + hydration
/// 4. Single object: `{"?person": ["*"]}` - hydration only
/// 5. S-expression aggregates: `["?name", "(count ?favNums as ?cnt)"]`
fn parse_select(
    select: &JsonValue,
    ctx: &JsonLdParseCtx,
    query: &mut UnresolvedQuery,
) -> Result<()> {
    match select {
        // Case 2, 3, 5: Array form — tuple-shaped rows of any arity.
        // (`["?x"]` → `[[v]]`, `["?x", "?y"]` → `[[v1, v2]]`, etc.)
        JsonValue::Array(arr) => {
            let mut columns = Vec::with_capacity(arr.len());
            for item in arr {
                match item {
                    JsonValue::String(s) => {
                        columns.push(parse_select_string(s, &mut query.options.aggregates)?);
                    }
                    JsonValue::Object(map) => {
                        let spec = parse_hydration_object(map, ctx, query)?;
                        columns.push(UnresolvedColumn::Hydration(spec));
                    }
                    _ => {
                        return Err(ParseError::InvalidSelect(
                            "select items must be strings or objects".to_string(),
                        ));
                    }
                }
            }
            query.select = UnresolvedProjection::Tuple(columns);
        }

        // Case 4: Single object form: {"?person": ["*"]} — one hydration
        // column wrapped as a Tuple (SPARQL has no scalar object form).
        JsonValue::Object(map) => {
            let spec = parse_hydration_object(map, ctx, query)?;
            query.select = UnresolvedProjection::Tuple(vec![UnresolvedColumn::Hydration(spec)]);
        }

        // Case 1: Single string form: "?x" — bare variable, scalar shape.
        JsonValue::String(s) => {
            let column = parse_select_string(s, &mut query.options.aggregates)?;
            query.select = UnresolvedProjection::Scalar(column);
        }

        _ => {
            return Err(ParseError::InvalidSelect(
                "select must be a string, array, or object".to_string(),
            ));
        }
    }

    Ok(())
}

/// Parse a string item from the select clause into a column.
///
/// Handles two cases:
/// - Variable: `"?name"`
/// - S-expression aggregate: `"(count ?x)"` or `"(as (count ?x) ?cnt)"` —
///   the spec is appended to `aggregates` and the output var becomes the
///   returned column.
///
/// Wildcard (`"*"`) is rejected here; it must be the entire `select` value
/// and is handled at the top-level dispatch in `parse_query_inner`.
fn parse_select_string(
    s: &str,
    aggregates: &mut Vec<ast::UnresolvedAggregateSpec>,
) -> Result<UnresolvedColumn> {
    let trimmed = s.trim();

    if trimmed == "*" {
        return Err(ParseError::InvalidSelect(
            "wildcard '*' must be the only select item".to_string(),
        ));
    }

    if trimmed.starts_with('(') {
        // S-expression: aggregate function call
        let agg_spec = parse_aggregate_sexpr(trimmed)?;
        let column = UnresolvedColumn::Var(Arc::from(agg_spec.output_var.as_ref()));
        aggregates.push(agg_spec);
        Ok(column)
    } else {
        // Must be a variable - use validate_var_name for consistent error handling
        validate_var_name(trimmed)?;
        Ok(UnresolvedColumn::Var(Arc::from(trimmed)))
    }
}

// SexprToken and tokenization moved to sexpr_tokenize module

/// Parse an S-expression aggregate function call
///
/// Supports legacy syntax:
/// - `(count ?x)` - COUNT with auto-generated output var (?count)
/// - `(as (count ?x) ?cnt)` - COUNT with explicit alias
/// - `(count *)` - COUNT(*) for all rows
/// - `(sum ?age)` - SUM
/// - `(as (avg ?score) ?avgScore)` - AVG with alias
/// - `(groupconcat ?name ", ")` - GROUP_CONCAT with separator as 2nd arg
fn parse_aggregate_sexpr(s: &str) -> Result<ast::UnresolvedAggregateSpec> {
    let trimmed = s.trim();

    // Must start with ( and end with )
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return Err(ParseError::InvalidSelect(format!(
            "aggregate expression must be wrapped in parentheses: {s}"
        )));
    }

    // Tokenize the entire expression
    let tokens = sexpr_tokenize::tokenize_sexpr(trimmed)?;

    // Should have exactly one top-level list
    if tokens.len() != 1 {
        return Err(ParseError::InvalidSelect(format!(
            "expected single S-expression, got {} tokens",
            tokens.len()
        )));
    }

    let list = tokens[0].as_list()?;

    if list.is_empty() {
        return Err(ParseError::InvalidSelect(
            "empty aggregate expression".to_string(),
        ));
    }

    // Get the function name (first element)
    let fn_name = list[0]
        .expect_atom("aggregate function name")?
        .to_lowercase();

    // Check if this is an `as` wrapper: (as (agg-fn ?var) ?alias)
    if fn_name == "as" {
        return parse_as_aggregate(list);
    }

    // Otherwise, parse as a direct aggregate: (count ?x) or (groupconcat ?x ", ")
    parse_direct_aggregate(&fn_name, &list[1..])
}

/// Parse `(as (agg-fn ?var) ?alias)` form
fn parse_as_aggregate(list: &[sexpr_tokenize::SexprToken]) -> Result<ast::UnresolvedAggregateSpec> {
    // Format: (as <inner-aggregate> ?alias)
    if list.len() != 3 {
        return Err(ParseError::InvalidSelect(format!(
            "(as ...) requires exactly 2 arguments: (as (agg ?var) ?alias), got {} elements",
            list.len() - 1
        )));
    }

    // Second element must be the inner aggregate (a list)
    let inner_agg = list[1].expect_list("first argument to 'as'")?;

    // Third element must be the alias variable
    let alias = list[2].expect_atom("alias in 'as'")?;

    if !is_variable(alias) {
        return Err(ParseError::InvalidSelect(format!(
            "alias must be a variable (start with '?'), got: {alias}"
        )));
    }

    // Parse the inner aggregate
    if inner_agg.is_empty() {
        return Err(ParseError::InvalidSelect(
            "empty inner aggregate in 'as'".to_string(),
        ));
    }

    let inner_fn_name = inner_agg[0]
        .expect_atom("aggregate function name")?
        .to_lowercase();

    // Parse the inner aggregate with explicit output var
    let (function, input_var) = parse_aggregate_fn_and_input(&inner_fn_name, &inner_agg[1..])?;

    Ok(ast::UnresolvedAggregateSpec {
        function,
        input_var: Arc::from(input_var),
        output_var: Arc::from(alias),
    })
}

/// Parse a direct aggregate like `(count ?x)` or `(groupconcat ?x ", ")`
fn parse_direct_aggregate(
    fn_name: &str,
    args: &[sexpr_tokenize::SexprToken],
) -> Result<ast::UnresolvedAggregateSpec> {
    let (function, input_var) = parse_aggregate_fn_and_input(fn_name, args)?;

    // Auto-generate output var: (count ?x) -> ?count, (sum ?age) -> ?sum
    let output_var = format!("?{}", fn_name.replace('-', "_"));

    Ok(ast::UnresolvedAggregateSpec {
        function,
        input_var: Arc::from(input_var),
        output_var: Arc::from(output_var),
    })
}

/// Parse aggregate function and input variable from args
///
/// Returns (function, input_var)
fn parse_aggregate_fn_and_input(
    fn_name: &str,
    args: &[sexpr_tokenize::SexprToken],
) -> Result<(ast::UnresolvedAggregateFn, String)> {
    if args.is_empty() {
        return Err(ParseError::InvalidSelect(format!(
            "aggregate '{fn_name}' requires at least one argument"
        )));
    }

    // First arg is the input variable (or "*" for count)
    let input = args[0].expect_atom("aggregate input")?;

    // Validate input is a variable or "*"
    if input != "*" && !is_variable(input) {
        return Err(ParseError::InvalidSelect(format!(
            "aggregate input must be a variable or '*', got: {input}"
        )));
    }

    // Parse function + validate arity.
    //
    // IMPORTANT: don't silently ignore extra args (user feedback).
    //
    // Most aggregates take exactly 1 argument; special cases handled separately.
    let function = match fn_name {
        // group-concat accepts an optional separator: (groupconcat ?x ", ")
        "group-concat" | "groupconcat" => {
            if args.len() > 2 {
                return Err(ParseError::InvalidSelect(format!(
                    "aggregate '{fn_name}' accepts at most 2 arguments, got {} (expected: (groupconcat ?x) or (groupconcat ?x \", \"))",
                    args.len(),
                )));
            }
            let separator = if args.len() > 1 {
                args[1].expect_atom("groupconcat separator")?.to_string()
            } else {
                " ".to_string() // default separator
            };
            ast::UnresolvedAggregateFn::GroupConcat { separator }
        }
        // All other aggregates require exactly 1 argument.
        _ => {
            let variant = match fn_name {
                "count" => ast::UnresolvedAggregateFn::Count,
                "count-distinct" | "countdistinct" => ast::UnresolvedAggregateFn::CountDistinct,
                "sum" => ast::UnresolvedAggregateFn::Sum,
                "avg" => ast::UnresolvedAggregateFn::Avg,
                "min" => ast::UnresolvedAggregateFn::Min,
                "max" => ast::UnresolvedAggregateFn::Max,
                "median" => ast::UnresolvedAggregateFn::Median,
                "variance" => ast::UnresolvedAggregateFn::Variance,
                "stddev" => ast::UnresolvedAggregateFn::Stddev,
                "sample" => ast::UnresolvedAggregateFn::Sample,
                other => return Err(ParseError::UnknownAggregate(other.to_string())),
            };
            if args.len() != 1 {
                return Err(ParseError::InvalidSelect(format!(
                    "aggregate '{fn_name}' requires exactly 1 argument, got {} (expected: ({fn_name} ?x))",
                    args.len(),
                )));
            }
            variant
        }
    };

    // Validate COUNT(*) is only used with count
    if input == "*" && !matches!(function, ast::UnresolvedAggregateFn::Count) {
        return Err(ParseError::InvalidSelect(format!(
            "'*' can only be used with count, not {fn_name}"
        )));
    }

    Ok((function, input.to_string()))
}

/// Parse a hydration object like `{"?person": ["*", {"ex:friend": ["*"]}]}`
fn parse_hydration_object(
    map: &serde_json::Map<String, JsonValue>,
    ctx: &JsonLdParseCtx,
    query: &UnresolvedQuery,
) -> Result<UnresolvedHydrationSpec> {
    // Error if we already have a hydration (only one allowed)
    if query.hydration().is_some() {
        return Err(ParseError::InvalidSelect(
            "only one graph-select object allowed per query".to_string(),
        ));
    }

    // Must have exactly one key
    if map.len() != 1 {
        return Err(ParseError::InvalidSelect(
            "graph-select object must have exactly one root key".to_string(),
        ));
    }

    let (root_str, specs_val) = map.iter().next().unwrap();

    // Root can be variable OR IRI constant
    let root = if is_variable(root_str) {
        UnresolvedRoot::Var(Arc::from(root_str.as_str()))
    } else {
        // IRI constant root - expand via @context
        let (expanded, _) = ctx.expand_vocab(root_str)?;
        UnresolvedRoot::Iri(expanded)
    };

    // Parse selection specs array
    let specs_arr = specs_val.as_array().ok_or_else(|| {
        ParseError::InvalidSelect("graph-select value must be an array".to_string())
    })?;

    let level = parse_selection_level(specs_arr, ctx)?;

    Ok(UnresolvedHydrationSpec {
        root,
        level,
        depth: 0, // Will be set later from query-level "depth" parameter
    })
}

/// Build an optional boxed nested select spec, returning `None` when empty.
fn make_nested_spec(
    level: UnresolvedNestedSelectSpec,
) -> Option<Box<UnresolvedNestedSelectSpec>> {
    if level.is_empty() {
        None
    } else {
        Some(Box::new(level))
    }
}

/// If `key` uses the inline `@reverse:...` form, return the target predicate IRI
/// (vocab-expanded). Returns `Ok(None)` for any other key so callers can fall
/// through to the regular context-lookup path.
///
/// Supports `@reverse:@type` / `@reverse:type` (and the context's configured
/// type key) as shorthand for the reverse of rdf:type, matching node_map.rs.
fn parse_inline_reverse_key(key: &str, ctx: &JsonLdParseCtx) -> Result<Option<String>> {
    let Some(rest) = key.strip_prefix("@reverse:") else {
        return Ok(None);
    };
    if rest == "@type" || rest == "type" || rest == ctx.context.type_key.as_str() {
        return Ok(Some(node_map::RDF_TYPE.to_string()));
    }
    let (expanded, _) = ctx.expand_vocab(rest)?;
    Ok(Some(expanded))
}

/// Parse a selection-level array (the value of a hydration `{key: [...]}`)
/// into either a `Wildcard` or `Explicit` level.
fn parse_selection_level(
    arr: &[JsonValue],
    ctx: &JsonLdParseCtx,
) -> Result<UnresolvedNestedSelectSpec> {
    let mut wildcard = false;
    let mut forward: Vec<UnresolvedForwardItem> = Vec::new();
    let mut refinements: std::collections::HashMap<String, Box<UnresolvedNestedSelectSpec>> =
        std::collections::HashMap::new();
    let mut reverse: std::collections::HashMap<
        String,
        Option<Box<UnresolvedNestedSelectSpec>>,
    > = std::collections::HashMap::new();

    for item in arr {
        match item {
            // Wildcard: "*"
            JsonValue::String(s) if s == "*" => {
                wildcard = true;
            }
            // Explicit @id selection
            JsonValue::String(s) if s == "@id" || s == "id" || s == ctx.context.id_key.as_str() => {
                forward.push(UnresolvedForwardItem::Id);
            }
            // Property name: "ex:name" or inline "@reverse:ex:friend"
            JsonValue::String(s) => {
                if let Some(rev_iri) = parse_inline_reverse_key(s, ctx)? {
                    reverse.insert(rev_iri, None);
                } else {
                    let (expanded, entry) = ctx.expand_vocab(s)?;
                    if let Some(rev_iri) = entry.and_then(|e| e.reverse) {
                        reverse.insert(rev_iri, None);
                    } else {
                        forward.push(UnresolvedForwardItem::Property {
                            predicate: expanded,
                            sub_spec: None,
                        });
                    }
                }
            }
            // Nested selection: {"ex:friend": ["*"]} or inline {"@reverse:ex:friend": ["*"]}
            JsonValue::Object(map) => {
                if map.len() != 1 {
                    return Err(ParseError::InvalidSelect(
                        "nested selection must have exactly one key".to_string(),
                    ));
                }

                let (pred_str, sub_specs_val) = map.iter().next().unwrap();

                let sub_arr = sub_specs_val.as_array().ok_or_else(|| {
                    ParseError::InvalidSelect("nested selection value must be an array".to_string())
                })?;

                let sub_level = parse_selection_level(sub_arr, ctx)?;
                let nested = make_nested_spec(sub_level);

                if let Some(rev_iri) = parse_inline_reverse_key(pred_str, ctx)? {
                    reverse.insert(rev_iri, nested);
                } else {
                    let (expanded, entry) = ctx.expand_vocab(pred_str)?;
                    let context_reverse = entry.as_ref().and_then(|e| e.reverse.as_ref());
                    if let Some(rev_iri) = context_reverse {
                        reverse.insert(rev_iri.clone(), nested);
                    } else if let Some(boxed) = nested {
                        // Wildcard refinements only matter when the parent is
                        // a wildcard; otherwise it's a regular Property entry.
                        // Decide based on `wildcard` after the loop completes.
                        forward.push(UnresolvedForwardItem::Property {
                            predicate: expanded,
                            sub_spec: Some(boxed),
                        });
                    } else {
                        forward.push(UnresolvedForwardItem::Property {
                            predicate: expanded,
                            sub_spec: None,
                        });
                    }
                }
            }
            _ => {
                return Err(ParseError::InvalidSelect(
                    "selection spec must be string or object".to_string(),
                ));
            }
        }
    }

    if wildcard {
        // For a wildcard level, fold any explicit Property entries back into
        // `refinements` (they refine the wildcard's per-property recursion).
        // Plain `Id` entries are redundant under wildcard and are dropped.
        for item in forward {
            if let UnresolvedForwardItem::Property { predicate, sub_spec } = item {
                if let Some(boxed) = sub_spec {
                    refinements.insert(predicate, boxed);
                }
            }
        }
        Ok(UnresolvedNestedSelectSpec::Wildcard {
            refinements,
            reverse,
        })
    } else {
        Ok(UnresolvedNestedSelectSpec::Explicit { forward, reverse })
    }
}

// parse_depth moved to options module
// WHERE clause parsing functions moved to where_clause module
// parse_values_clause and parse_values_cell moved to values module

/// Parse filter value - can be a string expression "(> ?age 45)" or data expression ["expr", [...]]
fn parse_filter_value(value: &JsonValue) -> Result<UnresolvedExpression> {
    match value {
        // String expression like "(> ?age 45)" - parse as S-expression
        JsonValue::String(s) => filter_sexpr::parse_s_expression(s),
        // Array: could be ["expr", [...]] or direct data expression
        JsonValue::Array(arr) => {
            if arr.is_empty() {
                return Err(ParseError::InvalidFilter(
                    "empty filter expression".to_string(),
                ));
            }
            // Check if it's ["expr", [...]] format
            if let Some(first) = arr[0].as_str() {
                if first.to_lowercase() == "expr" {
                    if arr.len() != 2 {
                        return Err(ParseError::InvalidFilter(
                            "expr format requires exactly one expression".to_string(),
                        ));
                    }
                    return filter_data::parse_filter_expr(&arr[1]);
                }
            }
            // Otherwise parse as direct data expression
            filter_data::parse_filter_array(arr)
        }
        _ => Err(ParseError::InvalidFilter(
            "filter expression must be a string or array".to_string(),
        )),
    }
}

// Parse an S-expression string like "(> ?age 45)" into a filter expression
//
// # Supported syntax
// - Atoms: `?var`, numbers, `true`/`false`, quoted strings `"text"`
// - Expressions: `(op arg1 arg2 ...)`
// - Nested: `(and (> ?x 10) (< ?y 100))`
// Limitations of S-expression filter parsing:
// - Quoted strings cannot contain whitespace, parentheses, or escape sequences
//   (e.g., `"Smith Jr"` with a space will not parse correctly)
// - For complex string comparisons, use the data expression format instead:
//   `["filter", ["=", "?name", "Smith Jr"]]`
// Filter parsing functions moved to filter_sexpr and filter_data modules
// Query modifier parsing - moved to options module

/// Validate that a string looks like a variable (starts with ?)
fn validate_var_name(name: &str) -> Result<()> {
    if !name.starts_with('?') {
        return Err(ParseError::InvalidVariable(name.to_string()));
    }
    Ok(())
}

// Query option parsing functions moved to options module

/// Parse and lower a JSON query in one step
///
/// Convenience function combining `parse_query_ast` and `lower_query`.
///
/// # Arguments
///
/// * `json` - The JSON query value
/// * `encoder` - IRI encoder for converting IRIs to Sids
/// * `vars` - Variable registry
///
/// # Returns
///
/// A fully resolved `Query` ready for execution.
pub fn parse_query<E: IriEncoder>(
    json: &JsonValue,
    encoder: &E,
    vars: &mut VarRegistry,
    strict_override: Option<bool>,
) -> Result<Query> {
    let (ast, select_mode) = parse_query_ast(json, strict_override)?;
    lower_query(ast, encoder, vars, select_mode)
}

/// Parse a filter expression value and lower it to a Expression.
///
/// This reuses the same expression language as FILTER/BIND in queries.
pub fn parse_filter_expression(value: &JsonValue, vars: &mut VarRegistry) -> Result<Expression> {
    let unresolved = parse_filter_value(value)?;
    lower::lower_filter_expr(&unresolved, vars)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::ast::UnresolvedPattern;
    use crate::parse::node_map::RDF_TYPE;
    use serde_json::json;

    /// Helper to extract the triple pattern from an UnresolvedPattern
    fn triple(p: &UnresolvedPattern) -> &UnresolvedTriplePattern {
        p.as_triple().expect("Expected UnresolvedPattern::Triple")
    }

    #[test]
    fn test_parse_minimal_query() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?name"],
            "where": { "@type": "ex:Person", "ex:name": "?name" }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        assert_eq!(ast.select.columns().len(), 1);
        assert_eq!(ast.select.columns()[0].var_name().unwrap(), "?name");
        assert_eq!(ast.patterns.len(), 2); // @type + name
    }

    #[test]
    fn test_parse_order_by_legacy_forms() {
        // Legacy surface supports:
        // - orderBy: "?x" (defaults asc)
        // - orderBy: ["?x", "(desc ?y)", ["asc","?z"]] (mixed forms)
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?x"],
            "where": { "ex:name": "?x" },
            "orderBy": ["?x", "(desc ?y)", ["ASC", "?z"]]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();
        assert_eq!(ast.options.order_by.len(), 3);
        assert_eq!(ast.options.order_by[0].var.as_ref(), "?x");
        assert_eq!(
            ast.options.order_by[0].direction,
            ast::UnresolvedSortDirection::Asc
        );
        assert_eq!(ast.options.order_by[1].var.as_ref(), "?y");
        assert_eq!(
            ast.options.order_by[1].direction,
            ast::UnresolvedSortDirection::Desc
        );
        assert_eq!(ast.options.order_by[2].var.as_ref(), "?z");
        assert_eq!(
            ast.options.order_by[2].direction,
            ast::UnresolvedSortDirection::Asc
        );
    }

    #[test]
    fn test_parse_order_by_single_scalar_string() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?x"],
            "where": { "ex:name": "?x" },
            "orderBy": "?x"
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();
        assert_eq!(ast.options.order_by.len(), 1);
        assert_eq!(ast.options.order_by[0].var.as_ref(), "?x");
        assert_eq!(
            ast.options.order_by[0].direction,
            ast::UnresolvedSortDirection::Asc
        );
    }

    #[test]
    fn test_implicit_subject() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?name"],
            "where": { "ex:name": "?name" }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        // Should have generated ?__s0 as subject (unique per node-map, reserved prefix)
        assert_eq!(ast.patterns.len(), 1);
        let pattern = triple(&ast.patterns[0]);
        assert!(matches!(&pattern.s, UnresolvedTerm::Var(v) if v.as_ref() == "?__s0"));
    }

    #[test]
    fn test_explicit_subject_variable() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?person", "?name"],
            "where": { "@id": "?person", "ex:name": "?name" }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let pattern = triple(&ast.patterns[0]);
        assert!(matches!(&pattern.s, UnresolvedTerm::Var(v) if v.as_ref() == "?person"));
    }

    #[test]
    fn test_explicit_subject_iri() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?name"],
            "where": { "@id": "ex:alice", "ex:name": "?name" }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let pattern = triple(&ast.patterns[0]);
        assert!(
            matches!(&pattern.s, UnresolvedTerm::Iri(iri) if iri.as_ref() == "http://example.org/alice")
        );
    }

    #[test]
    fn test_literal_values() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s"],
            "where": {
                "@id": "?s",
                "ex:age": 42,
                "ex:name": "Alice",
                "ex:active": true
            }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        assert_eq!(ast.patterns.len(), 3);

        // Find the age pattern
        let age_pattern = ast.patterns.iter().find(|p| {
            p.as_triple().is_some_and(
                |tp| matches!(&tp.p, UnresolvedTerm::Iri(iri) if iri.as_ref().ends_with("age")),
            )
        });
        assert!(age_pattern.is_some());
        assert!(matches!(
            &triple(age_pattern.unwrap()).o,
            UnresolvedTerm::Literal(LiteralValue::Long(42))
        ));
    }

    #[test]
    fn test_accepts_both_context_keys() {
        // @context
        let json1 = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?name"],
            "where": { "ex:name": "?name" }
        });
        assert!(parse_query_ast(&json1, None).is_ok());

        // context (without @)
        let json2 = json!({
            "context": { "ex": "http://example.org/" },
            "select": ["?name"],
            "where": { "ex:name": "?name" }
        });
        assert!(parse_query_ast(&json2, None).is_ok());
    }

    #[test]
    fn test_missing_select() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "where": { "ex:name": "?name" }
        });

        let result = parse_query_ast(&json, None);
        assert!(matches!(
            result.unwrap_err(),
            ParseError::MissingField(
                "select, selectOne, select-one, selectDistinct, select-distinct, construct, or ask"
            )
        ));
    }

    #[test]
    fn test_missing_where() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?name"]
        });

        let result = parse_query_ast(&json, None);
        assert!(matches!(
            result.unwrap_err(),
            ParseError::MissingField("where")
        ));
    }

    #[test]
    fn test_values_without_where_is_allowed() {
        let json = json!({
            "@context": { "xsd": "http://www.w3.org/2001/XMLSchema#" },
            "select": ["?x"],
            "values": ["?x", [1, 2, 3]]
        });

        let (ast, _mode) = parse_query_ast(&json, None).unwrap();
        assert_eq!(ast.patterns.len(), 1);
        assert!(matches!(ast.patterns[0], UnresolvedPattern::Values { .. }));
    }

    #[test]
    fn test_values_in_where_keyword() {
        let json = json!({
            "@context": { "ex": "http://example.org/", "xsd": "http://www.w3.org/2001/XMLSchema#" },
            "select": ["?x"],
            "where": [
                ["values", ["?x", [1, 2]]],
                { "@id": "?s", "ex:age": "?x" }
            ]
        });

        let (ast, _mode) = parse_query_ast(&json, None).unwrap();
        assert!(ast
            .patterns
            .iter()
            .any(|p| matches!(p, UnresolvedPattern::Values { .. })));
    }

    #[test]
    fn test_values_typed_vector_cell_parses() {
        let json = json!({
            "@context": { "fluree": "https://ns.flur.ee/db#" },
            "select": ["?v"],
            "values": ["?v", [
                { "@value": [0.7, 0.6], "@type": "fluree:embeddingVector" }
            ]]
        });

        let (ast, _mode) = parse_query_ast(&json, None).unwrap();
        let UnresolvedPattern::Values { vars, rows } = &ast.patterns[0] else {
            panic!("expected Values pattern");
        };
        assert_eq!(vars.len(), 1);
        assert_eq!(rows.len(), 1);
        match &rows[0][0] {
            UnresolvedValue::Literal {
                value: LiteralValue::Vector(v),
                dtc,
            } => {
                assert_eq!(v.as_slice(), &[0.7, 0.6]);
                assert!(dtc
                    .as_ref()
                    .is_some_and(|d| d.datatype_iri() == "https://ns.flur.ee/db#embeddingVector"));
            }
            other => panic!("unexpected cell: {other:?}"),
        }
    }

    #[test]
    fn test_invalid_variable() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["name"],  // Missing ?
            "where": { "ex:name": "?name" }
        });

        let result = parse_query_ast(&json, None);
        assert!(matches!(
            result.unwrap_err(),
            ParseError::InvalidVariable(_)
        ));
    }

    #[test]
    fn test_type_hydration() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s"],
            "where": { "@id": "?s", "@type": "ex:Person" }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let type_pattern = triple(&ast.patterns[0]);
        assert!(matches!(
            &type_pattern.p,
            UnresolvedTerm::Iri(iri) if iri.as_ref() == RDF_TYPE
        ));
        assert!(matches!(
            &type_pattern.o,
            UnresolvedTerm::Iri(iri) if iri.as_ref() == "http://example.org/Person"
        ));
    }

    #[test]
    fn test_multiple_types() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s"],
            "where": { "@id": "?s", "@type": ["ex:Person", "ex:Agent"] }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        // Should have two type patterns
        let type_patterns: Vec<_> = ast
            .patterns
            .iter()
            .filter(|p| {
                p.as_triple().is_some_and(
                    |tp| matches!(&tp.p, UnresolvedTerm::Iri(iri) if iri.as_ref() == RDF_TYPE),
                )
            })
            .collect();
        assert_eq!(type_patterns.len(), 2);
    }

    #[test]
    fn test_parse_and_lower() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?name"],
            "where": { "@id": "?s", "ex:name": "?name" }
        });

        let mut encoder = encode::MemoryEncoder::new();
        encoder.add_namespace("http://example.org/", 100);

        let mut vars = VarRegistry::new();
        let query = parse_query(&json, &encoder, &mut vars, None).unwrap();

        assert_eq!(query.output.projected_vars().unwrap().len(), 2);
        assert_eq!(query.patterns.len(), 1);

        // query.patterns now contains Pattern, not TriplePattern
        if let crate::ir::Pattern::Triple(tp) = &query.patterns[0] {
            // Predicate IRI is lowered to Ref::Iri for deferred encoding
            assert_eq!(tp.p.as_iri(), Some("http://example.org/name"));
        } else {
            panic!("Expected Pattern::Triple");
        }
    }

    #[test]
    fn test_property_datatype_from_context() {
        let json = json!({
            "@context": {
                "ex": "http://example.org/",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
                "age": { "@id": "ex:age", "@type": "xsd:integer" }
            },
            "select": ["?s", "?age"],
            "where": { "@id": "?s", "age": "?age" }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let pattern = triple(&ast.patterns[0]);
        assert!(pattern.dtc.is_some());
        assert_eq!(
            pattern
                .dtc
                .as_ref()
                .map(fluree_vocab::UnresolvedDatatypeConstraint::datatype_iri),
            Some("http://www.w3.org/2001/XMLSchema#integer")
        );
    }

    #[test]
    fn test_where_array_format() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?name", "?age"],
            "where": [
                { "@id": "?s", "ex:name": "?name" },
                { "@id": "?s", "ex:age": "?age" }
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        // Should have patterns from both node-maps
        assert_eq!(ast.patterns.len(), 2);
    }

    #[test]
    fn test_vocab_hydration() {
        let json = json!({
            "@context": {
                "@vocab": "http://schema.org/"
            },
            "select": ["?name"],
            "where": { "name": "?name" }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let pattern = triple(&ast.patterns[0]);
        assert!(
            matches!(&pattern.p, UnresolvedTerm::Iri(iri) if iri.as_ref() == "http://schema.org/name")
        );
    }

    #[test]
    fn test_floating_point_values() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s"],
            "where": { "@id": "?s", "ex:score": 3.13 }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let pattern = triple(&ast.patterns[0]);
        if let UnresolvedTerm::Literal(LiteralValue::Double(d)) = &pattern.o {
            assert!((d - 3.13).abs() < f64::EPSILON);
        } else {
            panic!("Expected double literal");
        }
    }

    #[test]
    fn test_multiple_node_maps_unique_subjects() {
        // Each node-map without @id should get a unique implicit subject
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?name", "?age"],
            "where": [
                { "ex:name": "?name" },
                { "ex:age": "?age" }
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        assert_eq!(ast.patterns.len(), 2);

        // First node-map gets ?__s0
        let p0 = triple(&ast.patterns[0]);
        assert!(matches!(&p0.s, UnresolvedTerm::Var(v) if v.as_ref() == "?__s0"));

        // Second node-map gets ?__s1
        let p1 = triple(&ast.patterns[1]);
        assert!(matches!(&p1.s, UnresolvedTerm::Var(v) if v.as_ref() == "?__s1"));
    }

    #[test]
    fn test_mixed_explicit_implicit_subjects() {
        // Node-maps with explicit @id should not affect the counter
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?person", "?name", "?age"],
            "where": [
                { "ex:name": "?name" },
                { "@id": "?person", "ex:age": "?age" },
                { "ex:email": "?email" }
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        assert_eq!(ast.patterns.len(), 3);

        // First: implicit ?__s0
        assert!(
            matches!(&triple(&ast.patterns[0]).s, UnresolvedTerm::Var(v) if v.as_ref() == "?__s0")
        );
        // Second: explicit ?person
        assert!(
            matches!(&triple(&ast.patterns[1]).s, UnresolvedTerm::Var(v) if v.as_ref() == "?person")
        );
        // Third: implicit ?__s1
        assert!(
            matches!(&triple(&ast.patterns[2]).s, UnresolvedTerm::Var(v) if v.as_ref() == "?__s1")
        );
    }

    #[test]
    fn test_type_array_non_string_error() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s"],
            "where": { "@id": "?s", "@type": ["ex:Person", 42] }
        });

        let result = parse_query_ast(&json, None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, ParseError::InvalidWhere(msg) if msg.contains("@type array items must be strings"))
        );
    }

    #[test]
    fn test_nested_context_error() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?name"],
            "where": {
                "@context": { "foo": "http://foo.org/" },
                "ex:name": "?name"
            }
        });

        let result = parse_query_ast(&json, None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ParseError::InvalidWhere(msg) if msg.contains("nested @context")));
    }

    #[test]
    fn test_variable_predicate() {
        // Variable predicates should be parsed as Var terms, not IRIs
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?p", "?o"],
            "where": { "@id": "?s", "?p": "?o" }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        assert_eq!(ast.patterns.len(), 1);
        let pattern = triple(&ast.patterns[0]);

        // Subject should be a variable
        assert!(matches!(&pattern.s, UnresolvedTerm::Var(v) if v.as_ref() == "?s"));

        // Predicate should be a variable (not an IRI)
        assert!(matches!(&pattern.p, UnresolvedTerm::Var(v) if v.as_ref() == "?p"));

        // Object should be a variable
        assert!(matches!(&pattern.o, UnresolvedTerm::Var(v) if v.as_ref() == "?o"));
    }

    #[test]
    fn test_variable_predicate_multiple_patterns() {
        // Variable predicates work with multiple patterns
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?p", "?o", "?name"],
            "where": {
                "@id": "?s",
                "?p": "?o",
                "ex:name": "?name"
            }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        assert_eq!(ast.patterns.len(), 2);

        // Find the pattern with variable predicate
        let var_pred_pattern = ast
            .patterns
            .iter()
            .find(|p| {
                p.as_triple()
                    .is_some_and(|tp| matches!(&tp.p, UnresolvedTerm::Var(_)))
            })
            .expect("Should have a pattern with variable predicate");
        assert!(
            matches!(&triple(var_pred_pattern).p, UnresolvedTerm::Var(v) if v.as_ref() == "?p")
        );

        // Find the pattern with IRI predicate
        let iri_pred_pattern = ast
            .patterns
            .iter()
            .find(|p| {
                p.as_triple()
                    .is_some_and(|tp| matches!(&tp.p, UnresolvedTerm::Iri(_)))
            })
            .expect("Should have a pattern with IRI predicate");
        assert!(
            matches!(&triple(iri_pred_pattern).p, UnresolvedTerm::Iri(iri) if iri.as_ref() == "http://example.org/name")
        );
    }

    #[test]
    fn test_nested_node_map_basic() {
        // Nested node-maps should generate connecting triples
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?person", "?friendName"],
            "where": {
                "@id": "?person",
                "ex:friend": {
                    "ex:name": "?friendName"
                }
            }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        // Should have 2 patterns:
        // 1. ?person ex:friend ?__n0  (connecting triple)
        // 2. ?__n0 ex:name ?friendName  (nested property)
        assert_eq!(ast.patterns.len(), 2);

        // First pattern: connecting triple
        let p0 = triple(&ast.patterns[0]);
        assert!(matches!(&p0.s, UnresolvedTerm::Var(v) if v.as_ref() == "?person"));
        assert!(
            matches!(&p0.p, UnresolvedTerm::Iri(iri) if iri.as_ref() == "http://example.org/friend")
        );
        assert!(matches!(&p0.o, UnresolvedTerm::Var(v) if v.as_ref() == "?__n0"));

        // Second pattern: nested property
        let p1 = triple(&ast.patterns[1]);
        assert!(matches!(&p1.s, UnresolvedTerm::Var(v) if v.as_ref() == "?__n0"));
        assert!(
            matches!(&p1.p, UnresolvedTerm::Iri(iri) if iri.as_ref() == "http://example.org/name")
        );
        assert!(matches!(&p1.o, UnresolvedTerm::Var(v) if v.as_ref() == "?friendName"));
    }

    #[test]
    fn test_nested_node_map_with_explicit_id() {
        // Nested node-maps with explicit @id should override the generated subject
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?person", "?friend", "?friendName"],
            "where": {
                "@id": "?person",
                "ex:friend": {
                    "@id": "?friend",
                    "ex:name": "?friendName"
                }
            }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        // Should have 2 patterns:
        // 1. ?person ex:friend ?friend  (connecting triple uses explicit @id)
        // 2. ?friend ex:name ?friendName
        assert_eq!(ast.patterns.len(), 2);

        // First pattern: connecting triple uses explicit @id
        let p0 = triple(&ast.patterns[0]);
        assert!(matches!(&p0.s, UnresolvedTerm::Var(v) if v.as_ref() == "?person"));
        assert!(matches!(&p0.o, UnresolvedTerm::Var(v) if v.as_ref() == "?friend"));

        // Second pattern: nested property uses explicit @id
        let p1 = triple(&ast.patterns[1]);
        assert!(matches!(&p1.s, UnresolvedTerm::Var(v) if v.as_ref() == "?friend"));
        assert!(matches!(&p1.o, UnresolvedTerm::Var(v) if v.as_ref() == "?friendName"));
    }

    #[test]
    fn test_nested_node_map_deeply_nested() {
        // Deeply nested node-maps should work recursively
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?person", "?city"],
            "where": {
                "@id": "?person",
                "ex:address": {
                    "ex:city": {
                        "ex:name": "?city"
                    }
                }
            }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        // Should have 3 patterns:
        // 1. ?person ex:address ?__n0
        // 2. ?__n0 ex:city ?__n1
        // 3. ?__n1 ex:name ?city
        assert_eq!(ast.patterns.len(), 3);

        // First: person -> address -> nested0
        let p0 = triple(&ast.patterns[0]);
        assert!(matches!(&p0.s, UnresolvedTerm::Var(v) if v.as_ref() == "?person"));
        assert!(
            matches!(&p0.p, UnresolvedTerm::Iri(iri) if iri.as_ref() == "http://example.org/address")
        );
        assert!(matches!(&p0.o, UnresolvedTerm::Var(v) if v.as_ref() == "?__n0"));

        // Second: nested0 -> city -> nested1
        let p1 = triple(&ast.patterns[1]);
        assert!(matches!(&p1.s, UnresolvedTerm::Var(v) if v.as_ref() == "?__n0"));
        assert!(
            matches!(&p1.p, UnresolvedTerm::Iri(iri) if iri.as_ref() == "http://example.org/city")
        );
        assert!(matches!(&p1.o, UnresolvedTerm::Var(v) if v.as_ref() == "?__n1"));

        // Third: nested1 -> name -> city
        let p2 = triple(&ast.patterns[2]);
        assert!(matches!(&p2.s, UnresolvedTerm::Var(v) if v.as_ref() == "?__n1"));
        assert!(
            matches!(&p2.p, UnresolvedTerm::Iri(iri) if iri.as_ref() == "http://example.org/name")
        );
        assert!(matches!(&p2.o, UnresolvedTerm::Var(v) if v.as_ref() == "?city"));
    }

    #[test]
    fn test_nested_node_map_multiple_properties() {
        // Nested node-maps with multiple properties
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?person", "?friendName", "?friendAge"],
            "where": {
                "@id": "?person",
                "ex:friend": {
                    "ex:name": "?friendName",
                    "ex:age": "?friendAge"
                }
            }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        // Should have 3 patterns:
        // 1. ?person ex:friend ?__n0
        // 2. ?__n0 ex:name ?friendName
        // 3. ?__n0 ex:age ?friendAge
        assert_eq!(ast.patterns.len(), 3);

        // First: connecting triple
        let p0 = triple(&ast.patterns[0]);
        assert!(matches!(&p0.s, UnresolvedTerm::Var(v) if v.as_ref() == "?person"));
        assert!(matches!(&p0.o, UnresolvedTerm::Var(v) if v.as_ref() == "?__n0"));

        // The nested properties both use ?__n0 as subject
        let nested_patterns: Vec<_> = ast.patterns[1..].iter().collect();
        for p in &nested_patterns {
            assert!(matches!(&triple(p).s, UnresolvedTerm::Var(v) if v.as_ref() == "?__n0"));
        }
    }

    // ==========================================
    // Tests ported from legacy tests
    // ==========================================

    #[test]
    fn test_reference_type_in_context() {
        // When a property is typed as @id, string values should be treated as IRIs
        let json = json!({
            "@context": {
                "ex": "http://example.org/",
                "friend": { "@id": "ex:friend", "@type": "@id" }
            },
            "select": ["?s", "?friend"],
            "where": { "@id": "?s", "friend": "?friend" }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let pattern = triple(&ast.patterns[0]);
        // The predicate should be expanded
        assert!(
            matches!(&pattern.p, UnresolvedTerm::Iri(iri) if iri.as_ref() == "http://example.org/friend")
        );
    }

    #[test]
    fn test_reference_value_hydration() {
        // String values for @id-typed properties should expand to IRIs
        let json = json!({
            "@context": {
                "ex": "http://example.org/",
                "friend": { "@id": "ex:friend", "@type": "@id" }
            },
            "select": ["?s"],
            "where": { "@id": "?s", "friend": "ex:alice" }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let pattern = triple(&ast.patterns[0]);
        // The object should be an IRI, not a string literal
        assert!(
            matches!(&pattern.o, UnresolvedTerm::Iri(iri) if iri.as_ref() == "http://example.org/alice")
        );
    }

    #[test]
    fn test_no_context_full_iris() {
        // Queries can use full IRIs without any context
        let json = json!({
            "select": ["?name"],
            "where": {
                "@id": "http://example.org/alice",
                "http://example.org/name": "?name"
            }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let pattern = triple(&ast.patterns[0]);
        assert!(
            matches!(&pattern.s, UnresolvedTerm::Iri(iri) if iri.as_ref() == "http://example.org/alice")
        );
        assert!(
            matches!(&pattern.p, UnresolvedTerm::Iri(iri) if iri.as_ref() == "http://example.org/name")
        );
    }

    #[test]
    fn test_type_as_variable() {
        // @type can bind to a variable to query for types
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?type"],
            "where": { "@id": "?s", "@type": "?type" }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let type_pattern = triple(&ast.patterns[0]);
        assert!(matches!(
            &type_pattern.p,
            UnresolvedTerm::Iri(iri) if iri.as_ref() == RDF_TYPE
        ));
        assert!(matches!(
            &type_pattern.o,
            UnresolvedTerm::Var(v) if v.as_ref() == "?type"
        ));
    }

    #[test]
    fn test_type_key_without_at() {
        // "type" should work the same as "@type"
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s"],
            "where": { "@id": "?s", "type": "ex:Person" }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let type_pattern = triple(&ast.patterns[0]);
        assert!(matches!(
            &type_pattern.p,
            UnresolvedTerm::Iri(iri) if iri.as_ref() == RDF_TYPE
        ));
    }

    #[test]
    fn test_context_array() {
        // Context can be an array of context objects
        let json = json!({
            "@context": [
                { "ex": "http://example.org/" },
                { "schema": "http://schema.org/" }
            ],
            "select": ["?name", "?age"],
            "where": {
                "@id": "?s",
                "ex:name": "?name",
                "schema:age": "?age"
            }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        assert_eq!(ast.patterns.len(), 2);

        // Find the name pattern
        let name_pattern = ast.patterns.iter().find(|p| {
            p.as_triple().is_some_and(|tp| matches!(&tp.p, UnresolvedTerm::Iri(iri) if iri.as_ref().contains("example.org")))
        });
        assert!(name_pattern.is_some());

        // Find the age pattern
        let age_pattern = ast.patterns.iter().find(|p| {
            p.as_triple().is_some_and(|tp| matches!(&tp.p, UnresolvedTerm::Iri(iri) if iri.as_ref().contains("schema.org")))
        });
        assert!(age_pattern.is_some());
    }

    #[test]
    fn test_multiple_properties_same_node_map() {
        // A single node-map can have multiple properties
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?name", "?age", "?email"],
            "where": {
                "@id": "?s",
                "@type": "ex:Person",
                "ex:name": "?name",
                "ex:age": "?age",
                "ex:email": "?email"
            }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        // Should have 4 patterns: type + name + age + email
        assert_eq!(ast.patterns.len(), 4);

        // All patterns should share the same subject variable
        for pattern in &ast.patterns {
            assert!(matches!(&triple(pattern).s, UnresolvedTerm::Var(v) if v.as_ref() == "?s"));
        }
    }

    #[test]
    fn test_joined_node_maps_same_subject() {
        // When multiple node-maps share the same @id variable, they join
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?name", "?age"],
            "where": [
                { "@id": "?s", "ex:name": "?name" },
                { "@id": "?s", "ex:age": "?age" }
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        assert_eq!(ast.patterns.len(), 2);

        // Both patterns should share the same subject variable
        assert!(
            matches!(&triple(&ast.patterns[0]).s, UnresolvedTerm::Var(v) if v.as_ref() == "?s")
        );
        assert!(
            matches!(&triple(&ast.patterns[1]).s, UnresolvedTerm::Var(v) if v.as_ref() == "?s")
        );
    }

    #[test]
    fn test_literal_string_value() {
        // String values that aren't variables should be treated as literals
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s"],
            "where": { "@id": "?s", "ex:name": "Alice" }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let pattern = triple(&ast.patterns[0]);
        assert!(matches!(
            &pattern.o,
            UnresolvedTerm::Literal(LiteralValue::String(s)) if s.as_ref() == "Alice"
        ));
    }

    #[test]
    fn test_negative_integer() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s"],
            "where": { "@id": "?s", "ex:balance": -100 }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let pattern = triple(&ast.patterns[0]);
        assert!(matches!(
            &pattern.o,
            UnresolvedTerm::Literal(LiteralValue::Long(-100))
        ));
    }

    #[test]
    fn test_negative_float() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s"],
            "where": { "@id": "?s", "ex:temperature": -273.15 }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let pattern = triple(&ast.patterns[0]);
        if let UnresolvedTerm::Literal(LiteralValue::Double(d)) = &pattern.o {
            assert!((d - (-273.15)).abs() < f64::EPSILON);
        } else {
            panic!("Expected double literal");
        }
    }

    #[test]
    fn test_boolean_false() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s"],
            "where": { "@id": "?s", "ex:active": false }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let pattern = triple(&ast.patterns[0]);
        assert!(matches!(
            &pattern.o,
            UnresolvedTerm::Literal(LiteralValue::Boolean(false))
        ));
    }

    #[test]
    fn test_empty_select_error() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": [],
            "where": { "ex:name": "?name" }
        });

        // Empty select should parse (though semantically questionable)
        let (ast, _) = parse_query_ast(&json, None).unwrap();
        assert!(ast.select.columns().is_empty());
    }

    #[test]
    fn test_empty_where_object() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s"],
            "where": {}
        });

        // Empty where object produces no patterns (just implicit subject)
        let (ast, _) = parse_query_ast(&json, None).unwrap();
        assert!(ast.patterns.is_empty());
    }

    #[test]
    fn test_custom_type_key_in_context() {
        // Context can define a custom type key via "kind": "@type"
        // This allows using "kind" as an alias for @type in the data
        let json = json!({
            "@context": {
                "kind": "@type",
                "ex": "http://example.org/"
            },
            "select": ["?s"],
            "where": { "@id": "?s", "kind": "ex:Person" }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let type_pattern = triple(&ast.patterns[0]);
        assert!(matches!(
            &type_pattern.p,
            UnresolvedTerm::Iri(iri) if iri.as_ref() == RDF_TYPE
        ));
    }

    #[test]
    fn test_multiple_select_same_variable() {
        // Duplicate variables in select should be preserved
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?name", "?name"],
            "where": { "ex:name": "?name" }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();
        assert_eq!(ast.select.columns().len(), 2);
    }

    #[test]
    fn test_where_array_empty() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s"],
            "where": []
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();
        assert!(ast.patterns.is_empty());
    }

    #[test]
    fn test_base_iri_expansion_fragment() {
        // @base provides the base IRI for relative references
        // Fragment identifiers like "#alice" resolve against @base
        let json = json!({
            "@context": {
                "@base": "http://example.org/resource",
                "name": "http://example.org/name"
            },
            "select": ["?name"],
            "where": { "@id": "#alice", "name": "?name" }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let pattern = triple(&ast.patterns[0]);
        // Subject "#alice" should expand using @base
        assert!(
            matches!(&pattern.s, UnresolvedTerm::Iri(iri) if iri.as_ref() == "http://example.org/resource#alice")
        );
    }

    #[test]
    fn test_base_iri_expansion_relative_path() {
        // Relative paths resolve against @base
        let json = json!({
            "@context": {
                "@base": "http://example.org/",
                "name": "http://example.org/name"
            },
            "select": ["?name"],
            "where": { "@id": "people/alice", "name": "?name" }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let pattern = triple(&ast.patterns[0]);
        // Subject "people/alice" should expand using @base
        assert!(
            matches!(&pattern.s, UnresolvedTerm::Iri(iri) if iri.as_ref() == "http://example.org/people/alice")
        );
    }

    // ==================== FILTER SYNTAX TESTS ====================

    /// Helper to find the first filter pattern in the AST
    fn find_filter(patterns: &[UnresolvedPattern]) -> Option<&UnresolvedExpression> {
        patterns.iter().find_map(|p| {
            if let UnresolvedPattern::Filter(expr) = p {
                Some(expr)
            } else {
                None
            }
        })
    }

    /// Helper to count triple patterns
    fn count_triples(patterns: &[UnresolvedPattern]) -> usize {
        patterns.iter().filter(|p| p.is_triple()).count()
    }

    /// Helper to count filter patterns
    fn count_filters(patterns: &[UnresolvedPattern]) -> usize {
        patterns
            .iter()
            .filter(|p| matches!(p, UnresolvedPattern::Filter(_)))
            .count()
    }

    #[test]
    fn test_filter_simple_comparison() {
        // Using array syntax: ["filter", [">", "?age", 18]]
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?age"],
            "where": [
                { "@id": "?s", "ex:age": "?age" },
                ["filter", [">", "?age", 18]]
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        // Should have 2 patterns: 1 triple + 1 filter
        assert_eq!(ast.patterns.len(), 2);
        assert_eq!(count_triples(&ast.patterns), 1);
        assert_eq!(count_filters(&ast.patterns), 1);

        // Find the filter
        let filter = find_filter(&ast.patterns).expect("Should have a filter");
        match filter {
            UnresolvedExpression::Call { func, args } => {
                assert_eq!(func.as_ref(), ">");
                assert!(matches!(&args[0], UnresolvedExpression::Var(v) if v.as_ref() == "?age"));
                assert!(matches!(
                    &args[1],
                    UnresolvedExpression::Const(UnresolvedFilterValue::Long(18))
                ));
            }
            _ => panic!("Expected Call expression"),
        }
    }

    #[test]
    fn test_filter_s_expression_string() {
        // Using S-expression string syntax: ["filter", "(> ?age 18)"]
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?age"],
            "where": [
                { "@id": "?s", "ex:age": "?age" },
                ["filter", "(> ?age 18)"]
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        assert_eq!(count_triples(&ast.patterns), 1);
        assert_eq!(count_filters(&ast.patterns), 1);

        let filter = find_filter(&ast.patterns).expect("Should have a filter");
        match filter {
            UnresolvedExpression::Call { func, args } => {
                assert_eq!(func.as_ref(), ">");
                assert!(matches!(&args[0], UnresolvedExpression::Var(v) if v.as_ref() == "?age"));
                assert!(matches!(
                    &args[1],
                    UnresolvedExpression::Const(UnresolvedFilterValue::Long(18))
                ));
            }
            _ => panic!("Expected Call expression"),
        }
    }

    #[test]
    fn test_filter_and_expression() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?age"],
            "where": [
                { "@id": "?s", "ex:age": "?age" },
                ["filter", ["and", [">=", "?age", 18], ["<", "?age", 65]]]
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        // Should have 2 patterns: 1 triple + 1 filter
        assert_eq!(count_triples(&ast.patterns), 1);
        assert_eq!(count_filters(&ast.patterns), 1);

        let filter = find_filter(&ast.patterns).expect("Should have a filter");
        match filter {
            UnresolvedExpression::And(exprs) => {
                assert_eq!(exprs.len(), 2);
                // First condition: ?age >= 18
                match &exprs[0] {
                    UnresolvedExpression::Call { func, .. } => {
                        assert_eq!(func.as_ref(), ">=");
                    }
                    _ => panic!("Expected Call"),
                }
                // Second condition: ?age < 65
                match &exprs[1] {
                    UnresolvedExpression::Call { func, .. } => {
                        assert_eq!(func.as_ref(), "<");
                    }
                    _ => panic!("Expected Call"),
                }
            }
            _ => panic!("Expected And expression"),
        }
    }

    #[test]
    fn test_filter_and_s_expression() {
        // Test AND with S-expression syntax
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?age"],
            "where": [
                { "@id": "?s", "ex:age": "?age" },
                ["filter", "(and (>= ?age 18) (< ?age 65))"]
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let filter = find_filter(&ast.patterns).expect("Should have a filter");
        match filter {
            UnresolvedExpression::And(exprs) => {
                assert_eq!(exprs.len(), 2);
            }
            _ => panic!("Expected And expression"),
        }
    }

    #[test]
    fn test_filter_or_expression() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?status"],
            "where": [
                { "@id": "?s", "ex:status": "?status" },
                ["filter", ["or", ["=", "?status", "active"], ["=", "?status", "pending"]]]
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let filter = find_filter(&ast.patterns).expect("Should have a filter");
        match filter {
            UnresolvedExpression::Or(exprs) => {
                assert_eq!(exprs.len(), 2);
            }
            _ => panic!("Expected Or expression"),
        }
    }

    #[test]
    fn test_filter_not_expression() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?active"],
            "where": [
                { "@id": "?s", "ex:active": "?active" },
                ["filter", ["not", ["=", "?active", false]]]
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let filter = find_filter(&ast.patterns).expect("Should have a filter");
        match filter {
            UnresolvedExpression::Not(inner) => {
                assert!(matches!(inner.as_ref(), UnresolvedExpression::Call { .. }));
            }
            _ => panic!("Expected Not expression"),
        }
    }

    #[test]
    fn test_filter_arithmetic_expression() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?x", "?y"],
            "where": [
                { "@id": "?s", "ex:x": "?x", "ex:y": "?y" },
                ["filter", [">", ["+", "?x", "?y"], 100]]
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        // 2 triple patterns (ex:x and ex:y) + 1 filter (@id just sets subject, doesn't create pattern)
        assert_eq!(count_triples(&ast.patterns), 2);
        assert_eq!(count_filters(&ast.patterns), 1);

        let filter = find_filter(&ast.patterns).expect("Should have a filter");
        match filter {
            UnresolvedExpression::Call { func, args } => {
                assert_eq!(func.as_ref(), ">");
                match &args[0] {
                    UnresolvedExpression::Call {
                        func: inner_func, ..
                    } => {
                        assert_eq!(inner_func.as_ref(), "+");
                    }
                    _ => panic!("Expected Call expression on left"),
                }
            }
            _ => panic!("Expected Call"),
        }
    }

    #[test]
    fn test_filter_function_call() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?name"],
            "where": [
                { "@id": "?s", "ex:name": "?name" },
                ["filter", ["contains", "?name", "Smith"]]
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let filter = find_filter(&ast.patterns).expect("Should have a filter");
        match filter {
            UnresolvedExpression::Call { func, args } => {
                assert_eq!(func.as_ref(), "contains");
                assert_eq!(args.len(), 2);
                assert!(matches!(&args[0], UnresolvedExpression::Var(v) if v.as_ref() == "?name"));
                assert!(
                    matches!(&args[1], UnresolvedExpression::Const(UnresolvedFilterValue::String(s)) if s.as_ref() == "Smith")
                );
            }
            _ => panic!("Expected Function"),
        }
    }

    #[test]
    fn test_filter_unary_negation() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?val"],
            "where": [
                { "@id": "?s", "ex:val": "?val" },
                ["filter", [">", ["-", "?val"], -10]]
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let filter = find_filter(&ast.patterns).expect("Should have a filter");
        match filter {
            UnresolvedExpression::Call { func, args } => {
                assert_eq!(func.as_ref(), ">");
                // First arg should be negate(?val)
                match &args[0] {
                    UnresolvedExpression::Call {
                        func: inner_func, ..
                    } => {
                        assert_eq!(inner_func.as_ref(), "negate");
                    }
                    _ => panic!("Expected Call with negate"),
                }
                // Second arg should be -10
                assert!(matches!(
                    &args[1],
                    UnresolvedExpression::Const(UnresolvedFilterValue::Long(-10))
                ));
            }
            _ => panic!("Expected Call"),
        }
    }

    #[test]
    fn test_filter_all_comparison_operators() {
        // Test all comparison operators using array syntax
        let operators = vec![
            ("=", "="),
            ("!=", "!="),
            ("<>", "!="),
            ("<", "<"),
            ("<=", "<="),
            (">", ">"),
            (">=", ">="),
        ];

        for (op_str, expected_func) in operators {
            let json = json!({
                "@context": { "ex": "http://example.org/" },
                "select": ["?x"],
                "where": [
                    { "ex:val": "?x" },
                    ["filter", [op_str, "?x", 0]]
                ]
            });

            let (ast, _) = parse_query_ast(&json, None).unwrap();

            let filter = find_filter(&ast.patterns).expect("Should have a filter");
            match filter {
                UnresolvedExpression::Call { func, .. } => {
                    assert_eq!(
                        func.as_ref(),
                        expected_func,
                        "Operator {op_str} did not match"
                    );
                }
                _ => panic!("Expected Call for operator {op_str}"),
            }
        }
    }

    #[test]
    fn test_filter_expr_format() {
        // Test the ["filter", ["expr", [...]]] format
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?age"],
            "where": [
                { "ex:age": "?age" },
                ["filter", ["expr", [">", "?age", 21]]]
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();
        assert_eq!(count_triples(&ast.patterns), 1);
        assert_eq!(count_filters(&ast.patterns), 1);
    }

    #[test]
    fn test_filter_double_values() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?price"],
            "where": [
                { "ex:price": "?price" },
                ["filter", ["<", "?price", 99.99]]
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let filter = find_filter(&ast.patterns).expect("Should have a filter");
        match filter {
            UnresolvedExpression::Call { args, .. } => match &args[1] {
                UnresolvedExpression::Const(UnresolvedFilterValue::Double(d)) => {
                    assert!((d - 99.99).abs() < f64::EPSILON);
                }
                _ => panic!("Expected Double constant"),
            },
            _ => panic!("Expected Call"),
        }
    }

    #[test]
    fn test_filter_error_empty_array() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?x"],
            "where": [
                { "ex:val": "?x" },
                ["filter", []]
            ]
        });

        let result = parse_query_ast(&json, None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ParseError::InvalidFilter(_)));
    }

    #[test]
    fn test_filter_single_arg_comparison_parses() {
        // Single-arg comparison is valid with variadic ops (vacuously true)
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?x"],
            "where": [
                { "ex:val": "?x" },
                ["filter", [">", "?x"]]
            ]
        });

        let result = parse_query_ast(&json, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_filter_error_zero_args() {
        // Zero-arg comparison should still fail
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?x"],
            "where": [
                { "ex:val": "?x" },
                ["filter", [">"]]
            ]
        });

        let result = parse_query_ast(&json, None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ParseError::InvalidFilter(_)));
    }

    #[test]
    fn test_filter_error_non_string_operator() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?x"],
            "where": [
                { "ex:val": "?x" },
                ["filter", [123, "?x", 1]]  // Operator should be string
            ]
        });

        let result = parse_query_ast(&json, None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ParseError::InvalidFilter(_)));
    }

    // ==================== OPTIONAL SYNTAX TESTS ====================

    /// Helper to find optional patterns
    fn find_optional(patterns: &[UnresolvedPattern]) -> Option<&Vec<UnresolvedPattern>> {
        patterns.iter().find_map(|p| {
            if let UnresolvedPattern::Optional(inner) = p {
                Some(inner)
            } else {
                None
            }
        })
    }

    /// Helper to count optional patterns
    fn count_optionals(patterns: &[UnresolvedPattern]) -> usize {
        patterns
            .iter()
            .filter(|p| matches!(p, UnresolvedPattern::Optional(_)))
            .count()
    }

    #[test]
    fn test_optional_basic() {
        // ["optional", {...}] syntax
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?name", "?email"],
            "where": [
                { "@id": "?s", "ex:name": "?name" },
                ["optional", { "@id": "?s", "ex:email": "?email" }]
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        // Should have 2 patterns: 1 triple + 1 optional
        assert_eq!(ast.patterns.len(), 2);
        assert_eq!(count_triples(&ast.patterns), 1);
        assert_eq!(count_optionals(&ast.patterns), 1);

        // Check optional content
        let optional = find_optional(&ast.patterns).expect("Should have optional");
        assert_eq!(optional.len(), 1);
        assert!(optional[0].is_triple());
    }

    #[test]
    fn test_optional_multiple_patterns() {
        // SPARQL semantics: ["optional", {a}, {b}] is one conjunctive OPTIONAL,
        // equivalent to OPTIONAL { a . b }.
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?name", "?email", "?phone"],
            "where": [
                { "@id": "?s", "ex:name": "?name" },
                ["optional",
                    { "@id": "?s", "ex:email": "?email" },
                    { "@id": "?s", "ex:phone": "?phone" }
                ]
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        assert_eq!(count_triples(&ast.patterns), 1);
        assert_eq!(count_optionals(&ast.patterns), 1);

        let optional = find_optional(&ast.patterns).expect("Should have optional");
        assert_eq!(optional.len(), 2);
        assert!(optional
            .iter()
            .all(super::ast::UnresolvedPattern::is_triple));
    }

    #[test]
    fn test_optional_with_filter() {
        // Optional can contain filters too
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?name", "?age"],
            "where": [
                { "@id": "?s", "ex:name": "?name" },
                ["optional",
                    { "@id": "?s", "ex:age": "?age" },
                    ["filter", "(> ?age 18)"]
                ]
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let optional = find_optional(&ast.patterns).expect("Should have optional");
        // Optional should contain 1 triple + 1 filter
        assert_eq!(optional.len(), 2);

        // Count types inside optional
        let optional_triples = optional.iter().filter(|p| p.is_triple()).count();
        let optional_filters = optional
            .iter()
            .filter(|p| matches!(p, UnresolvedPattern::Filter(_)))
            .count();
        assert_eq!(optional_triples, 1);
        assert_eq!(optional_filters, 1);
    }

    #[test]
    fn test_optional_with_nested_object() {
        // Optional with nested node-map
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?name", "?friendName"],
            "where": [
                { "@id": "?s", "ex:name": "?name" },
                ["optional", {
                    "@id": "?s",
                    "ex:friend": { "ex:name": "?friendName" }
                }]
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let optional = find_optional(&ast.patterns).expect("Should have optional");
        // Optional should contain 2 triples (connecting + nested property)
        assert_eq!(optional.len(), 2);
    }

    #[test]
    fn test_multiple_optionals() {
        // Multiple optional clauses
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?name", "?email", "?phone"],
            "where": [
                { "@id": "?s", "ex:name": "?name" },
                ["optional", { "@id": "?s", "ex:email": "?email" }],
                ["optional", { "@id": "?s", "ex:phone": "?phone" }]
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        assert_eq!(count_triples(&ast.patterns), 1);
        assert_eq!(count_optionals(&ast.patterns), 2);
    }

    #[test]
    fn test_filter_and_optional_together() {
        // Both filter and optional in same where clause
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?name", "?age", "?email"],
            "where": [
                { "@id": "?s", "ex:name": "?name", "ex:age": "?age" },
                ["filter", "(> ?age 21)"],
                ["optional", { "@id": "?s", "ex:email": "?email" }]
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        assert_eq!(count_triples(&ast.patterns), 2); // name + age
        assert_eq!(count_filters(&ast.patterns), 1);
        assert_eq!(count_optionals(&ast.patterns), 1);
    }

    #[test]
    fn test_optional_error_no_patterns() {
        // Optional requires at least one pattern
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s"],
            "where": [
                { "@id": "?s" },
                ["optional"]  // No patterns
            ]
        });

        let result = parse_query_ast(&json, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_optional_values_then_filter() {
        // VALUES is a binding-producing pattern, so a following FILTER inside the
        // same OPTIONAL block has an anchor and must be accepted.
        // SPARQL: OPTIONAL { VALUES (?x) { (1) (2) (3) } FILTER(?x > 0) }
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?x"],
            "where": [
                { "@id": "?s", "ex:name": "?name" },
                ["optional",
                    ["values", ["?x", [1, 2, 3]]],
                    ["filter", "(> ?x 0)"]
                ]
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();
        let optional = find_optional(&ast.patterns).expect("Should have optional");
        assert_eq!(optional.len(), 2);
        assert!(matches!(optional[0], UnresolvedPattern::Values { .. }));
        assert!(matches!(optional[1], UnresolvedPattern::Filter(_)));
    }

    #[test]
    fn test_optional_bind_anchor_for_filter() {
        // BIND inside OPTIONAL produces a binding for ?doubled, anchoring the
        // following FILTER. The leading node-map provides the BIND anchor.
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?x", "?doubled"],
            "where": [
                ["optional",
                    { "@id": "?s", "ex:x": "?x" },
                    ["bind", "?doubled", ["*", "?x", 2]],
                    ["filter", "(> ?doubled 0)"]
                ]
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();
        let optional = find_optional(&ast.patterns).expect("Should have optional");
        // 1 triple from node-map + 1 bind + 1 filter
        assert_eq!(optional.len(), 3);
    }

    #[test]
    fn test_optional_filter_first_rejected() {
        // FILTER as the very first item in an OPTIONAL has nothing to constrain
        // and must be rejected.
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s"],
            "where": [
                { "@id": "?s", "ex:name": "?name" },
                ["optional", ["filter", "(> ?name 0)"]]
            ]
        });

        let result = parse_query_ast(&json, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_optional_bind_first_rejected() {
        // BIND as the very first item in an OPTIONAL has nothing to bind from
        // and must be rejected (matches the pre-existing contract).
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?x"],
            "where": [
                { "@id": "?s", "ex:name": "?name" },
                ["optional", ["bind", "?x", ["+", 1, 1]]]
            ]
        });

        let result = parse_query_ast(&json, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_nested_optional_structure_preserved() {
        // Nested OPTIONAL should create nested Optional(...) wrapper, not flatten
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?email", "?phone"],
            "where": [
                { "@id": "?s", "ex:name": "?name" },
                ["optional",
                    { "@id": "?s", "ex:email": "?email" },
                    ["optional", { "@id": "?s", "ex:phone": "?phone" }]
                ]
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        // Top level: 1 triple + 1 optional
        assert_eq!(count_triples(&ast.patterns), 1);
        assert_eq!(count_optionals(&ast.patterns), 1);

        // Get the outer optional
        let outer_optional = find_optional(&ast.patterns).expect("Should have outer optional");

        // Outer optional should contain: 1 triple (email) + 1 nested optional (phone)
        let outer_triples = outer_optional.iter().filter(|p| p.is_triple()).count();
        let outer_optionals = outer_optional
            .iter()
            .filter(|p| matches!(p, UnresolvedPattern::Optional(_)))
            .count();
        assert_eq!(
            outer_triples, 1,
            "Outer optional should have 1 triple pattern"
        );
        assert_eq!(
            outer_optionals, 1,
            "Outer optional should have 1 nested optional (not flattened)"
        );

        // Get the nested optional
        let nested_optional = outer_optional
            .iter()
            .find_map(|p| {
                if let UnresolvedPattern::Optional(inner) = p {
                    Some(inner)
                } else {
                    None
                }
            })
            .expect("Should have nested optional");

        // Nested optional should contain 1 triple (phone)
        assert_eq!(nested_optional.len(), 1);
        assert!(nested_optional[0].is_triple());
    }

    #[test]
    fn test_s_expression_complex() {
        // Test more complex S-expressions
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "?x", "?y"],
            "where": [
                { "@id": "?s", "ex:x": "?x", "ex:y": "?y" },
                ["filter", "(and (> ?x 10) (< ?y 100) (>= (+ ?x ?y) 50))"]
            ]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        let filter = find_filter(&ast.patterns).expect("Should have filter");
        match filter {
            UnresolvedExpression::And(exprs) => {
                assert_eq!(exprs.len(), 3, "Should have 3 conditions in AND");
            }
            _ => panic!("Expected And expression"),
        }
    }

    #[test]
    fn test_parse_aggregate_sexpr_count_with_alias() {
        // Test parsing aggregate S-expression with alias: (as (count ?favNums) ?count)
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?name", "(as (count ?favNums) ?count)"],
            "where": { "ex:name": "?name", "ex:favNums": "?favNums" },
            "groupBy": ["?name"]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        // Check select has both vars
        assert_eq!(ast.select.columns().len(), 2);
        assert_eq!(ast.select.columns()[0].var_name().unwrap(), "?name");
        assert_eq!(ast.select.columns()[1].var_name().unwrap(), "?count");

        // Check aggregate was parsed
        assert_eq!(ast.options.aggregates.len(), 1);
        let agg = &ast.options.aggregates[0];
        assert_eq!(agg.function, ast::UnresolvedAggregateFn::Count);
        assert_eq!(agg.input_var.as_ref(), "?favNums");
        assert_eq!(agg.output_var.as_ref(), "?count");
    }

    #[test]
    fn test_parse_aggregate_sexpr_count_star() {
        // Test parsing COUNT(*) S-expression: (as (count *) ?total)
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?name", "(as (count *) ?total)"],
            "where": { "ex:name": "?name" },
            "groupBy": ["?name"]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        assert_eq!(ast.options.aggregates.len(), 1);
        let agg = &ast.options.aggregates[0];
        assert_eq!(agg.function, ast::UnresolvedAggregateFn::Count);
        assert_eq!(agg.input_var.as_ref(), "*");
        assert_eq!(agg.output_var.as_ref(), "?total");
    }

    #[test]
    fn test_parse_aggregate_sexpr_auto_output() {
        // Test auto-generated output var: (sum ?age) -> ?sum
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["(sum ?age)"],
            "where": { "ex:age": "?age" }
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        assert_eq!(ast.select.columns().len(), 1);
        assert_eq!(ast.select.columns()[0].var_name().unwrap(), "?sum");

        assert_eq!(ast.options.aggregates.len(), 1);
        let agg = &ast.options.aggregates[0];
        assert_eq!(agg.function, ast::UnresolvedAggregateFn::Sum);
        assert_eq!(agg.input_var.as_ref(), "?age");
        assert_eq!(agg.output_var.as_ref(), "?sum");
    }

    #[test]
    fn test_parse_aggregate_sexpr_groupconcat_with_separator() {
        // Test groupconcat with separator: (as (groupconcat ?x ", ") ?result)
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?s", "(as (groupconcat ?favNums \", \") ?nums)"],
            "where": { "@id": "?s", "ex:favNums": "?favNums" },
            "groupBy": ["?s"]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        assert_eq!(ast.options.aggregates.len(), 1);
        let agg = &ast.options.aggregates[0];
        match &agg.function {
            ast::UnresolvedAggregateFn::GroupConcat { separator } => {
                assert_eq!(separator, ", ");
            }
            _ => panic!("Expected GroupConcat"),
        }
        assert_eq!(agg.input_var.as_ref(), "?favNums");
        assert_eq!(agg.output_var.as_ref(), "?nums");
    }

    #[test]
    fn test_parse_aggregate_sexpr_rejects_extra_args() {
        // Extra args should not be silently ignored (helps users/LLMs self-correct).
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["(count ?x ?y)"],
            "where": { "ex:x": "?x", "ex:y": "?y" }
        });

        let err = parse_query_ast(&json, None).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("requires exactly 1 argument"),
            "unexpected error: {msg}"
        );
        assert!(msg.contains("expected:"), "unexpected error: {msg}");
    }

    #[test]
    fn test_parse_aggregate_sexpr_groupconcat_rejects_too_many_args() {
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["(groupconcat ?x \", \" \"extra\")"],
            "where": { "ex:x": "?x" }
        });

        let err = parse_query_ast(&json, None).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("accepts at most 2 arguments"),
            "unexpected error: {msg}"
        );
        assert!(msg.contains("expected:"), "unexpected error: {msg}");
    }

    #[test]
    fn test_parse_aggregate_sexpr_rejects_unclosed_string_literal() {
        // Missing closing quote around separator should produce a useful error.
        let json = json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["(as (groupconcat ?x \", ) ?out)"],
            "where": { "ex:x": "?x" },
            "groupBy": ["?x"]
        });

        let err = parse_query_ast(&json, None).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unclosed string literal"),
            "unexpected error: {msg}"
        );
        assert!(msg.contains("missing closing"), "unexpected error: {msg}");
    }

    // ==========================================
    // Vector Search Pattern Tests
    // ==========================================

    #[test]
    fn test_parse_vector_search_constant_vector() {
        // Vector search with constant vector using f: prefix
        let json = json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "select": ["?doc", "?score"],
            "where": [{
                "f:graphSource": "embeddings:main",
                "f:queryVector": [0.1, 0.2, 0.3],
                "f:distanceMetric": "cosine",
                "f:searchLimit": 10,
                "f:searchResult": {
                    "f:resultId": "?doc",
                    "f:resultScore": "?score"
                }
            }]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();
        assert_eq!(ast.patterns.len(), 1);

        match &ast.patterns[0] {
            UnresolvedPattern::VectorSearch(vsp) => {
                assert_eq!(vsp.graph_source_id.as_ref(), "embeddings:main");
                assert_eq!(vsp.metric.as_ref(), "cosine");
                assert_eq!(vsp.limit, Some(10));
                assert_eq!(vsp.id_var.as_ref(), "?doc");
                assert_eq!(vsp.score_var.as_deref(), Some("?score"));

                match &vsp.target {
                    ast::UnresolvedVectorSearchTarget::Const(vec) => {
                        assert_eq!(vec.len(), 3);
                        assert!((vec[0] - 0.1).abs() < 0.001);
                        assert!((vec[1] - 0.2).abs() < 0.001);
                        assert!((vec[2] - 0.3).abs() < 0.001);
                    }
                    _ => panic!("Expected constant vector target"),
                }
            }
            _ => panic!("Expected VectorSearch pattern"),
        }
    }

    #[test]
    fn test_parse_vector_search_variable_vector() {
        // Vector search with variable vector
        let json = json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "select": ["?doc"],
            "where": [{
                "f:graphSource": "embeddings:main",
                "f:queryVector": "?queryVec",
                "f:distanceMetric": "dot",
                "f:searchResult": "?doc"
            }]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();
        assert_eq!(ast.patterns.len(), 1);

        match &ast.patterns[0] {
            UnresolvedPattern::VectorSearch(vsp) => {
                assert_eq!(vsp.graph_source_id.as_ref(), "embeddings:main");
                assert_eq!(vsp.metric.as_ref(), "dot");
                assert_eq!(vsp.id_var.as_ref(), "?doc");
                assert!(vsp.score_var.is_none());

                match &vsp.target {
                    ast::UnresolvedVectorSearchTarget::Var(v) => {
                        assert_eq!(v.as_ref(), "?queryVec");
                    }
                    _ => panic!("Expected variable vector target"),
                }
            }
            _ => panic!("Expected VectorSearch pattern"),
        }
    }

    #[test]
    fn test_parse_vector_search_with_sync_and_timeout() {
        // Vector search with sync and timeout options
        let json = json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "select": ["?doc"],
            "where": [{
                "f:graphSource": "embeddings:main",
                "f:queryVector": [0.5, 0.5],
                "f:distanceMetric": "euclidean",
                "f:searchResult": "?doc",
                "f:syncBeforeQuery": true,
                "f:timeoutMs": 5000
            }]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        match &ast.patterns[0] {
            UnresolvedPattern::VectorSearch(vsp) => {
                assert_eq!(vsp.metric.as_ref(), "euclidean");
                assert!(vsp.sync);
                assert_eq!(vsp.timeout, Some(5000));
            }
            _ => panic!("Expected VectorSearch pattern"),
        }
    }

    #[test]
    fn test_parse_vector_search_with_full_iris() {
        // Vector search using full IRIs (no prefix needed)
        let json = json!({
            "select": ["?doc"],
            "where": [{
                "https://ns.flur.ee/db#graphSource": "embeddings:main",
                "https://ns.flur.ee/db#queryVector": [0.1, 0.2],
                "https://ns.flur.ee/db#distanceMetric": "cosine",
                "https://ns.flur.ee/db#searchResult": "?doc"
            }]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        match &ast.patterns[0] {
            UnresolvedPattern::VectorSearch(vsp) => {
                assert_eq!(vsp.graph_source_id.as_ref(), "embeddings:main");
            }
            _ => panic!("Expected VectorSearch pattern"),
        }
    }

    #[test]
    fn test_parse_vector_search_missing_vector() {
        // Missing f:queryVector should fail
        let json = json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "select": ["?doc"],
            "where": [{
                "f:graphSource": "embeddings:main",
                "f:distanceMetric": "cosine",
                "f:searchResult": "?doc"
            }]
        });

        let err = parse_query_ast(&json, None).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("queryVector") && msg.contains("required"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn test_parse_vector_search_missing_result() {
        // Missing f:searchResult should fail
        let json = json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "select": ["?doc"],
            "where": [{
                "f:graphSource": "embeddings:main",
                "f:queryVector": [0.1, 0.2],
                "f:distanceMetric": "cosine"
            }]
        });

        let err = parse_query_ast(&json, None).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("searchResult") && msg.contains("required"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn test_parse_vector_search_invalid_vector_value() {
        // Non-array, non-variable f:queryVector should fail
        let json = json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "select": ["?doc"],
            "where": [{
                "f:graphSource": "embeddings:main",
                "f:queryVector": "not a variable or array",
                "f:distanceMetric": "cosine",
                "f:searchResult": "?doc"
            }]
        });

        let err = parse_query_ast(&json, None).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("queryVector") && msg.contains("variable or array"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn test_parse_vector_search_default_metric() {
        // Default metric should be "cosine" if not specified
        let json = json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "select": ["?doc"],
            "where": [{
                "f:graphSource": "embeddings:main",
                "f:queryVector": [0.1, 0.2],
                "f:searchResult": "?doc"
            }]
        });

        let (ast, _) = parse_query_ast(&json, None).unwrap();

        match &ast.patterns[0] {
            UnresolvedPattern::VectorSearch(vsp) => {
                // Default metric is "cosine"
                assert_eq!(vsp.metric.as_ref(), "cosine");
            }
            _ => panic!("Expected VectorSearch pattern"),
        }
    }
}
