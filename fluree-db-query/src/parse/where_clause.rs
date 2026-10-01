//! WHERE clause parsing
//!
//! Parses JSON-LD WHERE clause patterns in both node-map and array formats.
//!
//! # Syntax
//!
//! WHERE clauses support multiple formats:
//!
//! ## Node-map format
//! ```json
//! {
//!   "where": {
//!     "ex:age": "?age",
//!     "ex:name": "?name"
//!   }
//! }
//! ```
//!
//! ## Array format with special keywords
//! ```json
//! {
//!   "where": [
//!     {"ex:age": "?age"},
//!     ["filter", [">", "?age", 18]],
//!     ["bind", "?doubled", ["*", "?age", 2]],
//!     ["optional", {"ex:email": "?email"}],
//!     ["union", {"ex:type": "Person"}, {"ex:type": "Organization"}],
//!     ["minus", {"ex:deleted": true}],
//!     ["exists", {"ex:verified": true}],
//!     ["not-exists", {"ex:suspended": true}],
//!     ["values", ["?x", [1, 2, 3]]],
//!     ["query", {"select": ["?sub"], "where": {"ex:type": "?sub"}}],
//!     ["graph", "ex:graph1", {"ex:prop": "?val"}]
//!   ]
//! }
//! ```

use super::ast::{UnresolvedPattern, UnresolvedTerm};
use super::error::{ParseError, Result};
use super::policy::JsonLdParseCtx;
use super::{node_map, parse_query_ast_internal, values, UnresolvedQuery};
use crate::ir::path::{PathDirection, ShortestPathMode};
use serde_json::Value as JsonValue;
use std::sync::Arc;

/// Import filter value parsing from parent module
fn parse_filter_value(value: &JsonValue) -> Result<super::ast::UnresolvedExpression> {
    super::parse_filter_value(value)
}

/// Parse a FILTER / BIND / UNWIND expression and resolve its unquoted
/// `prefix:name` atoms against the query's `@context`.
///
/// The S-expression parser has no context, so it hands such atoms over as
/// [`UnresolvedFilterValue::Curie`]. An atom whose prefix the context defines
/// becomes an IRI operand (`(= ?p ex:knows)` compares by term identity, as in
/// SPARQL); one whose prefix is undefined stays the plain string it always
/// was, so `(= ?slot 12:30)` is unaffected.
fn parse_expr_with_ctx(
    value: &JsonValue,
    ctx: &JsonLdParseCtx,
) -> Result<super::ast::UnresolvedExpression> {
    Ok(resolve_compact_iri_atoms(parse_filter_value(value)?, ctx))
}

/// Walk an expression tree, expanding [`UnresolvedFilterValue::Curie`] atoms
/// whose prefix the context knows into [`UnresolvedFilterValue::Iri`].
pub(crate) fn resolve_compact_iri_atoms(
    expr: super::ast::UnresolvedExpression,
    ctx: &JsonLdParseCtx,
) -> super::ast::UnresolvedExpression {
    use super::ast::{UnresolvedExpression as E, UnresolvedFilterValue as V};
    match expr {
        E::Const(V::Curie(atom)) => match ctx.expand_iri(&atom) {
            // Expansion changed the text *and* produced something that is
            // actually an IRI: the prefix was defined and meant a namespace.
            //
            // A context may alias a JSON-LD keyword — `{"type": "@type"}` is
            // ordinary — and expansion then turns `type:admin` into
            // `@typeadmin`, which is not an IRI and matches nothing. The
            // author meant the string. Requiring an absolute IRI keeps
            // keyword aliases out of IRI operand position.
            Ok(expanded)
                if expanded.as_str() != atom.as_ref()
                    && fluree_graph_json_ld::iri::is_absolute(expanded.as_str()) =>
            {
                E::Const(V::Iri(Arc::from(expanded.as_str())))
            }
            _ => E::Const(V::Curie(atom)),
        },
        E::And(items) => E::And(
            items
                .into_iter()
                .map(|e| resolve_compact_iri_atoms(e, ctx))
                .collect(),
        ),
        E::Or(items) => E::Or(
            items
                .into_iter()
                .map(|e| resolve_compact_iri_atoms(e, ctx))
                .collect(),
        ),
        E::Not(inner) => E::Not(Box::new(resolve_compact_iri_atoms(*inner, ctx))),
        E::In {
            expr,
            values,
            negated,
        } => E::In {
            expr: Box::new(resolve_compact_iri_atoms(*expr, ctx)),
            values: values
                .into_iter()
                .map(|e| resolve_compact_iri_atoms(e, ctx))
                .collect(),
            negated,
        },
        E::Call { func, args } => E::Call {
            func,
            args: args
                .into_iter()
                .map(|e| resolve_compact_iri_atoms(e, ctx))
                .collect(),
        },
        other => other,
    }
}

/// Resolve compact-IRI atoms in the expression positions that sit outside the
/// WHERE clause: computed SELECT columns and HAVING.
///
/// Those two parse their expressions without the `@context`, so an unquoted
/// `ex:knows` stayed a plain string there while the identical expression in a
/// FILTER became an IRI operand. `(as (= ?p ex:knows) ?isKnows)` was therefore
/// always false, and the same comparison in a filter matched. An absolute
/// `http://…` was an IRI in both, which made the inconsistency easy to miss.
pub(crate) fn resolve_atoms_outside_where(
    query: &mut super::ast::UnresolvedQuery,
    ctx: &JsonLdParseCtx,
) {
    use super::ast::{UnresolvedColumn as C, UnresolvedProjection as P};

    let mut fix_column = |col: &mut C| {
        if let C::Computation { expr, .. } = col {
            let taken =
                std::mem::replace(expr, super::ast::UnresolvedExpression::Var(Arc::from("")));
            *expr = resolve_compact_iri_atoms(taken, ctx);
        }
    };
    match &mut query.select {
        P::Tuple(cols) => cols.iter_mut().for_each(&mut fix_column),
        P::Scalar(col) => fix_column(col),
        P::Wildcard => {}
    }

    if let Some(having) = query.options.having.take() {
        query.options.having = Some(resolve_compact_iri_atoms(having, ctx));
    }
}

/// Validate that a string looks like a variable (starts with ?)
fn validate_var_name(name: &str) -> Result<()> {
    if !name.starts_with('?') {
        return Err(ParseError::InvalidVariable(name.to_string()));
    }
    Ok(())
}

/// Parse a shortest-path endpoint: a variable (`?x`) or a constant IRI
/// (expanded through the context's `@base`, like a subject `@id`).
fn parse_path_endpoint(s: &str, ctx: &JsonLdParseCtx) -> Result<UnresolvedTerm> {
    if s.starts_with('?') {
        validate_var_name(s)?;
        Ok(UnresolvedTerm::var(s))
    } else {
        let (expanded, _) = ctx.expand_id(s)?;
        Ok(UnresolvedTerm::iri(expanded))
    }
}

/// Parse WHERE clause with explicit counters for generating implicit variables
///
/// Internal function that maintains counters across recursive calls to ensure
/// unique variable names (?__s0, ?__s1, ?__n0, ?__n1, etc.).
pub fn parse_where_with_counters(
    where_val: &JsonValue,
    ctx: &JsonLdParseCtx,
    query: &mut UnresolvedQuery,
    subject_counter: &mut u32,
    nested_counter: &mut u32,
    object_var_parsing: bool,
) -> Result<()> {
    match where_val {
        JsonValue::Object(map) => {
            node_map::parse_node_map(
                map,
                ctx,
                query,
                subject_counter,
                nested_counter,
                object_var_parsing,
            )?;
        }
        JsonValue::Array(arr) => {
            // Array of node-maps, filters, and optionals
            for item in arr {
                match item {
                    JsonValue::Object(map) => {
                        node_map::parse_node_map(
                            map,
                            ctx,
                            query,
                            subject_counter,
                            nested_counter,
                            object_var_parsing,
                        )?;
                    }
                    JsonValue::Array(inner_arr) => {
                        // Array element: could be ["filter", ...] or ["optional", ...]
                        parse_where_array_element(
                            inner_arr,
                            ctx,
                            query,
                            subject_counter,
                            nested_counter,
                            object_var_parsing,
                        )?;
                    }
                    _ => {
                        return Err(ParseError::InvalidWhere(
                            "where array items must be objects or arrays".to_string(),
                        ));
                    }
                }
            }
        }
        _ => {
            return Err(ParseError::InvalidWhere(
                "where must be an object or array".to_string(),
            ));
        }
    }

    Ok(())
}

/// Parse a where clause array element like ["filter", ...] or ["optional", ...]
///
/// Supported keywords:
/// - `values` - Inline data: `["values", ["?x", [1, 2, 3]]]`
/// - `bind` - Variable binding: `["bind", "?doubled", ["*", "?x", 2]]`
/// - `filter` - Filter constraint: `["filter", [">", "?age", 18]]`
/// - `optional` - Left join: `["optional", {"ex:email": "?email"}]`
/// - `union` - Disjunction: `["union", {...}, {...}]`
/// - `minus` - Anti-join: `["minus", {"ex:deleted": true}]`
/// - `exists` - Existential check: `["exists", {"ex:verified": true}]`
/// - `not-exists` - Negated existential: `["not-exists", {"ex:suspended": true}]`
/// - `query` - Subquery: `["query", {"select": [...], "where": {...}}]`
/// - `graph` - Named graph: `["graph", "ex:g1", {...}]`
pub fn parse_where_array_element(
    arr: &[JsonValue],
    ctx: &JsonLdParseCtx,
    query: &mut UnresolvedQuery,
    subject_counter: &mut u32,
    nested_counter: &mut u32,
    object_var_parsing: bool,
) -> Result<()> {
    if arr.is_empty() {
        return Err(ParseError::InvalidWhere(
            "empty array in where clause".to_string(),
        ));
    }

    // First element should be the keyword
    let keyword = arr[0].as_str().ok_or_else(|| {
        ParseError::InvalidWhere("where array element must start with a string keyword".to_string())
    })?;

    let keyword_lower = keyword.to_lowercase();

    match keyword_lower.as_str() {
        "values" => {
            // ["values", [vars, rows]]
            if arr.len() != 2 {
                return Err(ParseError::InvalidWhere(
                    "values requires exactly one argument: [vars, rows]".to_string(),
                ));
            }
            let values_pat = values::parse_values_clause(&arr[1], ctx)?;
            query.patterns.push(values_pat);
            Ok(())
        }
        "bind" => {
            // ["bind", "?var", expr, "?var2", expr2, ...]
            //
            // `expr` supports:
            // - string S-expression: "(+ ?x 1)"
            // - data expr: ["+", "?x", 1]
            // - wrapped expr: ["expr", [...]]
            if arr.len() < 3 || !(arr.len() - 1).is_multiple_of(2) {
                return Err(ParseError::InvalidWhere(
                    "bind requires pairs of arguments: variable and expression".to_string(),
                ));
            }
            // Reuse filter parsing so BIND has the same expression language as FILTER.
            // Allow multiple bindings in a single bind form.
            let mut i = 1;
            while i < arr.len() {
                let var = arr[i].as_str().ok_or_else(|| {
                    ParseError::InvalidWhere("bind var must be a string".to_string())
                })?;
                validate_var_name(var)?;

                let expr = parse_expr_with_ctx(&arr[i + 1], ctx)?;
                query.patterns.push(UnresolvedPattern::Bind {
                    var: Arc::from(var),
                    expr,
                });
                i += 2;
            }
            Ok(())
        }
        "unwind" => {
            // ["unwind", "?var", expr]
            //
            // `expr` evaluates to a list value (e.g. "(range 1 5)", "(list 1 2 3)",
            // or a bound list variable) and is expanded into one row per element,
            // each bound to `?var`. Shares the FILTER/BIND expression language.
            if arr.len() != 3 {
                return Err(ParseError::InvalidWhere(
                    "unwind requires exactly two arguments: a variable and a list expression"
                        .to_string(),
                ));
            }
            let var = arr[1].as_str().ok_or_else(|| {
                ParseError::InvalidWhere("unwind var must be a string".to_string())
            })?;
            validate_var_name(var)?;
            let expr = parse_expr_with_ctx(&arr[2], ctx)?;
            query.patterns.push(UnresolvedPattern::Unwind {
                var: Arc::from(var),
                expr,
            });
            Ok(())
        }
        "shortestpath" | "allshortestpaths" => {
            // ["shortestPath", {"from": ?a, "to": ?b, "via": "ex:knows",
            //   "bind": ?path, "direction": out|in|both,
            //   "minHops": n, "maxHops": n}]
            let mode = if keyword_lower == "shortestpath" {
                ShortestPathMode::Single
            } else {
                ShortestPathMode::All
            };
            if arr.len() != 2 {
                return Err(ParseError::InvalidWhere(format!(
                    "{keyword} requires exactly one config-object argument"
                )));
            }
            let obj = arr[1].as_object().ok_or_else(|| {
                ParseError::InvalidWhere(format!("{keyword} argument must be an object"))
            })?;
            let get_str = |key: &str| -> Result<&str> {
                obj.get(key).and_then(|v| v.as_str()).ok_or_else(|| {
                    ParseError::InvalidWhere(format!("{keyword} requires a string '{key}'"))
                })
            };

            // Endpoints: a variable (`?x`) or a constant IRI (subject position).
            let start = parse_path_endpoint(get_str("from")?, ctx)?;
            let end = parse_path_endpoint(get_str("to")?, ctx)?;
            // Predicate IRI is expanded through @vocab.
            let (predicate, _) = ctx.expand_vocab(get_str("via")?)?;
            let path_var = get_str("bind")?;
            validate_var_name(path_var)?;

            let direction = match obj.get("direction").and_then(|v| v.as_str()) {
                None | Some("out" | "outgoing") => PathDirection::Outgoing,
                Some("in" | "incoming") => PathDirection::Incoming,
                Some("both" | "either" | "undirected") => PathDirection::Either,
                Some(other) => {
                    return Err(ParseError::InvalidWhere(format!(
                        "shortestPath 'direction' must be out|in|both, got '{other}'"
                    )))
                }
            };

            let hops = |key: &str| -> Result<Option<u32>> {
                match obj.get(key) {
                    None => Ok(None),
                    Some(v) => v
                        .as_u64()
                        .and_then(|n| u32::try_from(n).ok())
                        .map(Some)
                        .ok_or_else(|| {
                            ParseError::InvalidWhere(format!(
                                "shortestPath '{key}' must be a non-negative integer"
                            ))
                        }),
                }
            };

            query.patterns.push(UnresolvedPattern::ShortestPath {
                start,
                end,
                predicate: Arc::from(predicate.as_str()),
                direction,
                mode,
                path_var: Arc::from(path_var),
                min_hops: hops("minHops")?,
                max_hops: hops("maxHops")?,
            });
            Ok(())
        }
        "filter" => {
            // ["filter", expression] or ["filter", expr1, expr2, ...]
            if arr.len() < 2 {
                return Err(ParseError::InvalidFilter(
                    "filter requires an expression".to_string(),
                ));
            }

            // Build a pattern parser closure for EXISTS/NOT EXISTS inside filters.
            // This allows compound filter expressions like:
            //   ["filter", ["or", ["=", "?x", "?y"], ["not-exists", {...}]]]
            // Use Cell for interior mutability so the closure is Fn (not FnMut).
            let subj_cell = std::cell::Cell::new(*subject_counter);
            let nest_cell = std::cell::Cell::new(*nested_counter);
            let pattern_parser = |items: &[JsonValue]| -> Result<Vec<UnresolvedPattern>> {
                let mut sc = subj_cell.get();
                let mut nc = nest_cell.get();
                let result =
                    parse_subquery_patterns(items, ctx, &mut sc, &mut nc, object_var_parsing);
                subj_cell.set(sc);
                nest_cell.set(nc);
                result
            };

            for expr_val in &arr[1..] {
                let filter_expr = match expr_val {
                    // Array expressions may contain EXISTS/NOT EXISTS
                    JsonValue::Array(_) => {
                        super::filter_data::parse_filter_expr_ctx(expr_val, &pattern_parser)?
                    }
                    // Non-array values (strings, etc.) use the standard parser
                    _ => parse_expr_with_ctx(expr_val, ctx)?,
                };
                super::filter_common::reject_constant_bool_expr(&filter_expr, "filter")?;
                query.add_filter(filter_expr);
            }
            // Propagate counter updates back
            *subject_counter = subj_cell.get();
            *nested_counter = nest_cell.get();
            Ok(())
        }
        "optional" => parse_optional_patterns(
            &arr[1..],
            ctx,
            query,
            subject_counter,
            nested_counter,
            object_var_parsing,
        ),
        "union" => {
            // ["union", <branch1>, <branch2>, ...]
            //
            // Each branch can be:
            // - an object: interpreted as a single node-map pattern
            // - an array: interpreted as a list of patterns (node-maps and/or nested clauses)
            if arr.len() < 3 {
                return Err(ParseError::InvalidWhere(
                    "union requires at least two branches".to_string(),
                ));
            }

            let mut branches: Vec<Vec<UnresolvedPattern>> = Vec::new();
            for branch_val in &arr[1..] {
                let branch_patterns = match branch_val {
                    JsonValue::Object(_) => {
                        // Single node-map branch
                        parse_subquery_patterns(
                            std::slice::from_ref(branch_val),
                            ctx,
                            subject_counter,
                            nested_counter,
                            object_var_parsing,
                        )?
                    }
                    JsonValue::Array(items) => parse_subquery_patterns(
                        items,
                        ctx,
                        subject_counter,
                        nested_counter,
                        object_var_parsing,
                    )?,
                    _ => {
                        return Err(ParseError::InvalidWhere(
                            "union branches must be objects or arrays".to_string(),
                        ));
                    }
                };
                branches.push(branch_patterns);
            }

            query.patterns.push(UnresolvedPattern::Union(branches));
            Ok(())
        }
        "minus" => {
            // ["minus", {...}, {...}, ...]
            if arr.len() < 2 {
                return Err(ParseError::InvalidWhere(
                    "minus requires at least one pattern".to_string(),
                ));
            }
            let minus_patterns = parse_subquery_patterns(
                &arr[1..],
                ctx,
                subject_counter,
                nested_counter,
                object_var_parsing,
            )?;
            query
                .patterns
                .push(UnresolvedPattern::Minus(minus_patterns));
            Ok(())
        }
        "exists" => {
            // ["exists", {...}, {...}, ...]
            if arr.len() < 2 {
                return Err(ParseError::InvalidWhere(
                    "exists requires at least one pattern".to_string(),
                ));
            }
            let exists_patterns = parse_subquery_patterns(
                &arr[1..],
                ctx,
                subject_counter,
                nested_counter,
                object_var_parsing,
            )?;
            query
                .patterns
                .push(UnresolvedPattern::Exists(exists_patterns));
            Ok(())
        }
        "not-exists" | "notexists" => {
            // ["not-exists", {...}, {...}, ...]
            if arr.len() < 2 {
                return Err(ParseError::InvalidWhere(
                    "not-exists requires at least one pattern".to_string(),
                ));
            }
            let not_exists_patterns = parse_subquery_patterns(
                &arr[1..],
                ctx,
                subject_counter,
                nested_counter,
                object_var_parsing,
            )?;
            query
                .patterns
                .push(UnresolvedPattern::NotExists(not_exists_patterns));
            Ok(())
        }
        "query" => {
            // ["query", { "select": [...], "where": {...}, ... }]
            if arr.len() != 2 {
                return Err(ParseError::InvalidWhere(
                    "query requires exactly one subquery object".to_string(),
                ));
            }
            // Validate it's an object
            if !arr[1].is_object() {
                return Err(ParseError::InvalidWhere(
                    "subquery must be an object".to_string(),
                ));
            }
            // Parse the subquery as a full query, but reuse the parent counters so implicit vars
            // (?__sN/?__nN) cannot collide between parent and subquery scopes.
            // Subqueries inherit context from re-parsing; pass None for strict_override.
            let (subquery, _select_mode) =
                parse_query_ast_internal(&arr[1], subject_counter, nested_counter, None)?;
            query
                .patterns
                .push(UnresolvedPattern::Subquery(Box::new(subquery)));
            Ok(())
        }
        "graph" => {
            // ["graph", "graph-name", pattern1, pattern2, ...]
            // OR ["graph", "?g", pattern1, pattern2, ...]  (variable graph name)
            if arr.len() < 3 {
                return Err(ParseError::InvalidWhere(
                    "graph requires a graph name and at least one pattern".to_string(),
                ));
            }
            // Second element is the graph name (string or variable)
            let graph_name = arr[1].as_str().ok_or_else(|| {
                ParseError::InvalidWhere("graph name must be a string".to_string())
            })?;
            if let Some(refusal) =
                super::graph_name::reserved_graph_name(&ctx.graph_names, graph_name)
            {
                return Err(ParseError::InvalidWhere(refusal));
            }
            // Remaining elements are patterns
            let graph_patterns = ctx.in_graph_scope(|| {
                parse_subquery_patterns(
                    &arr[2..],
                    ctx,
                    subject_counter,
                    nested_counter,
                    object_var_parsing,
                )
            })?;
            query
                .patterns
                .push(UnresolvedPattern::graph(graph_name, graph_patterns));
            Ok(())
        }
        _ => Err(ParseError::InvalidWhere(format!(
            "unknown where clause keyword: {keyword}"
        ))),
    }
}

/// Parse patterns inside a subquery clause (optional, minus, exists, not-exists)
///
/// Converts a list of JSON values (objects or arrays) into patterns.
/// Used by UNION, MINUS, EXISTS, NOT-EXISTS, GRAPH, etc.
pub fn parse_subquery_patterns(
    items: &[JsonValue],
    ctx: &JsonLdParseCtx,
    subject_counter: &mut u32,
    nested_counter: &mut u32,
    object_var_parsing: bool,
) -> Result<Vec<UnresolvedPattern>> {
    let mut patterns = Vec::new();

    for item in items {
        match item {
            JsonValue::Object(map) => {
                // Parse as node-map, collecting patterns
                let mut temp_query = UnresolvedQuery::new(ctx.context.clone());
                node_map::parse_node_map(
                    map,
                    ctx,
                    &mut temp_query,
                    subject_counter,
                    nested_counter,
                    object_var_parsing,
                )?;
                patterns.extend(temp_query.patterns);
            }
            JsonValue::Array(arr) => {
                // Nested array element (could be filter, optional, minus, exists, etc.)
                let mut temp_query = UnresolvedQuery::new(ctx.context.clone());
                parse_where_array_element(
                    arr,
                    ctx,
                    &mut temp_query,
                    subject_counter,
                    nested_counter,
                    object_var_parsing,
                )?;
                patterns.extend(temp_query.patterns);
            }
            _ => {
                return Err(ParseError::InvalidWhere(
                    "subquery patterns must be objects or arrays".to_string(),
                ));
            }
        }
    }

    Ok(patterns)
}

/// Parse OPTIONAL patterns with SPARQL-canonical grouping semantics
///
/// The entire `["optional", ...]` array is a single OPTIONAL block — equivalent
/// to one SPARQL `OPTIONAL { ... }`. All items inside become a conjunctive group
/// (a single `LeftJoin` in the algebra).
///
/// - `["optional", {a}, {b}]` ≡ `OPTIONAL { a . b }` (one left join, conjunctive inner)
/// - `["optional", {a}, ["filter", ...]]` ≡ `OPTIONAL { a FILTER(...) }`
/// - To get two independent left joins, use two sibling arrays:
///   `["optional", {a}], ["optional", {b}]`.
///
/// Filters and binds require a preceding pattern in the group, since they
/// constrain or compute from existing bindings. Any anchor — node-map,
/// `values`, `bind`, nested `optional`, sub-`query`, etc. — qualifies; only a
/// filter/bind as the very first item in the array is rejected. Other array
/// forms are self-contained and may appear in any position.
fn parse_optional_patterns(
    items: &[JsonValue],
    ctx: &JsonLdParseCtx,
    query: &mut UnresolvedQuery,
    subject_counter: &mut u32,
    nested_counter: &mut u32,
    object_var_parsing: bool,
) -> Result<()> {
    if items.is_empty() {
        return Err(ParseError::InvalidWhere(
            "optional requires at least one pattern".to_string(),
        ));
    }

    let mut group: Vec<UnresolvedPattern> = Vec::new();
    let mut has_node_map_anchor = false;

    for item in items {
        match item {
            JsonValue::Object(map) => {
                has_node_map_anchor = true;

                let mut temp_query = UnresolvedQuery::new(ctx.context.clone());
                node_map::parse_node_map(
                    map,
                    ctx,
                    &mut temp_query,
                    subject_counter,
                    nested_counter,
                    object_var_parsing,
                )?;
                group.extend(temp_query.patterns);
            }
            JsonValue::Array(inner_arr) => {
                if !has_node_map_anchor && group.is_empty() {
                    let keyword = inner_arr.first().and_then(|v| v.as_str());
                    if matches!(keyword, Some("filter" | "bind")) {
                        return Err(ParseError::InvalidWhere(
                            "filter and bind in optional must follow a binding-producing \
                             pattern"
                                .to_string(),
                        ));
                    }
                }

                let mut temp_query = UnresolvedQuery::new(ctx.context.clone());
                parse_where_array_element(
                    inner_arr,
                    ctx,
                    &mut temp_query,
                    subject_counter,
                    nested_counter,
                    object_var_parsing,
                )?;
                group.extend(temp_query.patterns);
            }
            _ => {
                return Err(ParseError::InvalidWhere(
                    "optional patterns must be objects or arrays".to_string(),
                ));
            }
        }
    }

    if !group.is_empty() {
        query.add_optional(group);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::policy::JsonLdParsePolicy;
    use super::super::PathAliasMap;
    use super::*;
    use fluree_graph_json_ld::{parse_context, ParsedContext};
    use serde_json::json;

    fn test_context() -> ParsedContext {
        let ctx_json = json!({
            "ex": "http://example.org/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        });
        parse_context(&ctx_json).unwrap()
    }

    fn test_parse_ctx(context: &ParsedContext) -> JsonLdParseCtx {
        JsonLdParseCtx::new(
            context.clone(),
            PathAliasMap::new(),
            JsonLdParsePolicy::default(),
        )
    }

    /// Test helper: parse WHERE clause with fresh counters
    fn parse_where_test(
        where_val: &JsonValue,
        context: &ParsedContext,
        query: &mut UnresolvedQuery,
    ) -> Result<()> {
        let mut subject_counter: u32 = 0;
        let mut nested_counter: u32 = 0;
        let ctx = test_parse_ctx(context);
        parse_where_with_counters(
            where_val,
            &ctx,
            query,
            &mut subject_counter,
            &mut nested_counter,
            true,
        )
    }

    #[test]
    fn test_parse_where_object() {
        let context = test_context();
        let mut query = UnresolvedQuery::new(context.clone());
        let where_val = json!({
            "ex:name": "?name",
            "ex:age": "?age"
        });
        parse_where_test(&where_val, &context, &mut query).unwrap();
        assert!(!query.patterns.is_empty());
    }

    /// A node-level `@graph` selector in `where` is GRAPH sugar: the same
    /// patterns as `["graph", <name>, {…}]`, nested nodes included. It used
    /// to be parsed as a predicate named `@graph` and match nothing.
    #[test]
    fn node_level_graph_in_where_is_graph_sugar() {
        let context = test_context();
        let sugar = |where_val: JsonValue| {
            let mut query = UnresolvedQuery::new(context.clone());
            parse_where_test(&where_val, &context, &mut query).unwrap();
            format!("{:?}", query.patterns)
        };
        let node_level = sugar(json!({
            "@id": "?s", "@graph": "http://example.org/g",
            "ex:p": "?o", "ex:child": {"ex:q": "?v"}
        }));
        let explicit = sugar(json!([[
            "graph", "http://example.org/g",
            {"@id": "?s", "ex:p": "?o", "ex:child": {"ex:q": "?v"}}
        ]]));
        assert_eq!(node_level, explicit);
        assert!(node_level.contains("Graph"), "{node_level}");

        // A variable graph and a context alias of `@graph` work the same way.
        let aliased_ctx =
            parse_context(&json!({"ex": "http://example.org/", "g": "@graph"})).unwrap();
        let mut query = UnresolvedQuery::new(aliased_ctx.clone());
        parse_where_test(
            &json!({"@id": "?s", "g": "?graph", "ex:p": "?o"}),
            &aliased_ctx,
            &mut query,
        )
        .unwrap();
        assert!(
            matches!(&query.patterns[..], [UnresolvedPattern::Graph { name, .. }] if &**name == "?graph"),
            "{:?}",
            query.patterns
        );

        // Content is a named graph, not a pattern.
        let mut query = UnresolvedQuery::new(context.clone());
        assert!(parse_where_test(
            &json!({"@id": "ex:G", "@graph": [{"@id": "?s", "ex:p": "?o"}]}),
            &context,
            &mut query
        )
        .is_err());
    }

    /// A node-level `@graph` in `where` names its graph exactly as the same
    /// key does in an insert or delete: a compact IRI expands against the
    /// `@context`, and in an update's `where` the keywords and `fromNamed`
    /// aliases resolve as the templates resolve them. `default` is the
    /// ledger's default graph, and cannot leave an enclosing graph.
    #[test]
    fn node_level_graph_in_where_resolves_names_like_templates() {
        let context = test_context();
        let env = crate::parse::GraphNameEnv {
            ledger_id: Some("mydb:main".to_string()),
            aliases: [("g1".to_string(), "http://example.org/one".to_string())].into(),
            ledger_default_graph: None,
        };
        let parse = |where_val: JsonValue, env: Option<&crate::parse::GraphNameEnv>| {
            let mut ctx = test_parse_ctx(&context);
            if let Some(env) = env {
                ctx = ctx.with_graph_names(env.clone());
            }
            let mut query = UnresolvedQuery::new(context.clone());
            parse_where_with_counters(&where_val, &ctx, &mut query, &mut 0, &mut 0, true)
                .map(|()| query.patterns)
        };
        let graph_name = |where_val: JsonValue, env| match &parse(where_val, env).unwrap()[..] {
            [UnresolvedPattern::Graph { name, .. }] => name.to_string(),
            other => panic!("expected one GRAPH pattern, got {other:?}"),
        };

        // A compact name expands, in a query and in an update alike.
        for env in [None, Some(&env)] {
            assert_eq!(
                graph_name(json!({"@id": "?s", "@graph": "ex:g", "ex:p": "?o"}), env),
                "http://example.org/g"
            );
            assert_eq!(
                graph_name(json!({"@id": "?s", "@graph": "?g", "ex:p": "?o"}), env),
                "?g"
            );
        }
        // An update's keywords name this ledger's graphs; an alias stays the
        // name the dataset resolves.
        assert_eq!(
            graph_name(
                json!({"@id": "?s", "@graph": "config", "ex:p": "?o"}),
                Some(&env)
            ),
            "urn:fluree:mydb:main#config"
        );
        assert_eq!(
            graph_name(
                json!({"@id": "?s", "@graph": "txn-meta", "ex:p": "?o"}),
                Some(&env)
            ),
            "urn:fluree:mydb:main#txn-meta"
        );
        assert_eq!(
            graph_name(
                json!({"@id": "?s", "@graph": "g1", "ex:p": "?o"}),
                Some(&env)
            ),
            "g1"
        );

        // `default` is the where's default graph: the node's patterns as they
        // are, with no GRAPH around them.
        let plain = parse(json!({"@id": "?s", "ex:p": "?o"}), Some(&env)).unwrap();
        let default = parse(
            json!({"@id": "?s", "@graph": "default", "ex:p": "?o"}),
            Some(&env),
        )
        .unwrap();
        assert_eq!(format!("{plain:?}"), format!("{default:?}"));
        // When the update's where reads another default graph, `default`
        // still names the ledger's default graph: the dataset's name for it.
        let elsewhere = crate::parse::GraphNameEnv {
            ledger_default_graph: Some(crate::parse::LEDGER_DEFAULT_GRAPH.to_string()),
            ..env.clone()
        };
        assert_eq!(
            graph_name(
                json!({"@id": "?s", "@graph": "default", "ex:p": "?o"}),
                Some(&elsewhere)
            ),
            crate::parse::LEDGER_DEFAULT_GRAPH
        );
        // Inside another graph it would have to leave it, which a where
        // pattern cannot do: refused, whichever form encloses it.
        for enclosed in [
            json!({"@id": "?s", "@graph": "ex:g", "ex:child": {"@graph": "default", "ex:q": "?v"}}),
            json!([["graph", "http://example.org/g",
                    {"@id": "?s", "ex:child": {"@graph": "default", "ex:q": "?v"}}]]),
            json!([["graph", "http://example.org/g",
                    {"@id": "?s", "@graph": "default", "ex:p": "?o"}]]),
        ] {
            for env in [&env, &elsewhere] {
                let err = parse(enclosed.clone(), Some(env)).unwrap_err().to_string();
                assert!(err.contains("\"default\""), "{enclosed}: {err}");
            }
        }
    }

    /// The name an update's WHERE dataset gives the ledger's default graph is
    /// the where's to resolve `"default"` to: in an update it is refused as a
    /// node-level or `["graph", …]` name and as a VALUES value; a query reads
    /// what is written. Resolving `"default"` by the name is recorded, so the
    /// dataset carries the name only then.
    #[test]
    fn update_where_refuses_the_reserved_graph_name() {
        let context = test_context();
        let reserved = crate::parse::LEDGER_DEFAULT_GRAPH;
        let update = crate::parse::GraphNameEnv {
            ledger_id: Some("mydb:main".to_string()),
            ledger_default_graph: Some(reserved.to_string()),
            ..Default::default()
        };
        // Whether the where reads the ledger's default graph by its name.
        let parse = |where_val: JsonValue, env: Option<&crate::parse::GraphNameEnv>| {
            let mut ctx = test_parse_ctx(&context);
            if let Some(env) = env {
                ctx = ctx.with_graph_names(env.clone());
            }
            let mut query = UnresolvedQuery::new(context.clone());
            parse_where_with_counters(&where_val, &ctx, &mut query, &mut 0, &mut 0, true)
                .map(|()| ctx.reads_ledger_default())
        };
        for written in [
            json!({"@id": "?s", "@graph": reserved, "ex:p": "?o"}),
            json!([["graph", reserved, {"@id": "?s", "ex:p": "?o"}]]),
            json!([["values", ["?g", [reserved]]], ["graph", "?g", {"@id": "?s", "ex:p": "?o"}]]),
        ] {
            let err = parse(written.clone(), Some(&update))
                .unwrap_err()
                .to_string();
            assert!(err.contains("reserved"), "{written}: {err}");
            assert!(parse(written, None).is_ok());
        }
        assert!(parse(
            json!({"@id": "?s", "@graph": "default", "ex:p": "?o"}),
            Some(&update)
        )
        .unwrap());
        assert!(!parse(
            json!({"@id": "?s", "@graph": "ex:g", "ex:p": "?o"}),
            Some(&update)
        )
        .unwrap());
    }

    #[test]
    fn test_parse_where_array() {
        let context = test_context();
        let mut query = UnresolvedQuery::new(context.clone());
        let where_val = json!([
            {"ex:name": "?name"},
            {"ex:age": "?age"}
        ]);
        parse_where_test(&where_val, &context, &mut query).unwrap();
        assert!(!query.patterns.is_empty());
    }

    #[test]
    fn test_parse_filter_keyword() {
        let context = test_context();
        let mut query = UnresolvedQuery::new(context.clone());
        let arr = vec![json!("filter"), json!([">", "?age", 18])];
        let ctx = test_parse_ctx(&context);
        parse_where_array_element(&arr, &ctx, &mut query, &mut 0, &mut 0, true).unwrap();
        // Filter added to patterns
        assert!(!query.patterns.is_empty());
    }

    #[test]
    fn test_parse_bind_keyword() {
        let context = test_context();
        let mut query = UnresolvedQuery::new(context.clone());
        let arr = vec![json!("bind"), json!("?doubled"), json!(["+", "?x", "?x"])];
        let ctx = test_parse_ctx(&context);
        parse_where_array_element(&arr, &ctx, &mut query, &mut 0, &mut 0, true).unwrap();
        assert!(!query.patterns.is_empty());
    }

    #[test]
    fn test_parse_optional_keyword() {
        let context = test_context();
        let mut query = UnresolvedQuery::new(context.clone());
        let arr = vec![json!("optional"), json!({"ex:email": "?email"})];
        let ctx = test_parse_ctx(&context);
        parse_where_array_element(&arr, &ctx, &mut query, &mut 0, &mut 0, true).unwrap();
        assert!(!query.patterns.is_empty());
    }

    #[test]
    fn test_parse_union_keyword() {
        let context = test_context();
        let mut query = UnresolvedQuery::new(context.clone());
        let arr = vec![
            json!("union"),
            json!({"ex:type": "Person"}),
            json!({"ex:type": "Organization"}),
        ];
        let ctx = test_parse_ctx(&context);
        parse_where_array_element(&arr, &ctx, &mut query, &mut 0, &mut 0, true).unwrap();
        assert!(!query.patterns.is_empty());
    }

    #[test]
    fn test_parse_exists_keyword() {
        let context = test_context();
        let mut query = UnresolvedQuery::new(context.clone());
        let arr = vec![json!("exists"), json!({"ex:verified": true})];
        let ctx = test_parse_ctx(&context);
        parse_where_array_element(&arr, &ctx, &mut query, &mut 0, &mut 0, true).unwrap();
        assert!(!query.patterns.is_empty());
    }

    #[test]
    fn test_unknown_keyword_error() {
        let context = test_context();
        let mut query = UnresolvedQuery::new(context.clone());
        let arr = vec![json!("invalid_keyword")];
        let ctx = test_parse_ctx(&context);
        let result = parse_where_array_element(&arr, &ctx, &mut query, &mut 0, &mut 0, true);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_array_error() {
        let context = test_context();
        let mut query = UnresolvedQuery::new(context.clone());
        let arr: Vec<JsonValue> = vec![];
        let ctx = test_parse_ctx(&context);
        let result = parse_where_array_element(&arr, &ctx, &mut query, &mut 0, &mut 0, true);
        assert!(result.is_err());
    }
}
