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

use super::ast::UnresolvedPattern;
use super::error::{ParseError, Result};
use super::policy::JsonLdParseCtx;
use super::{node_map, parse_query_ast_internal, values, UnresolvedQuery};
use serde_json::Value as JsonValue;
use std::sync::Arc;

/// Import filter value parsing from parent module
fn parse_filter_value(value: &JsonValue) -> Result<super::ast::UnresolvedExpression> {
    super::parse_filter_value(value)
}

/// Validate that a string looks like a variable (starts with ?)
fn validate_var_name(name: &str) -> Result<()> {
    if !name.starts_with('?') {
        return Err(ParseError::InvalidVariable(name.to_string()));
    }
    Ok(())
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

                let expr = parse_filter_value(&arr[i + 1])?;
                query.patterns.push(UnresolvedPattern::Bind {
                    var: Arc::from(var),
                    expr,
                });
                i += 2;
            }
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
                    _ => parse_filter_value(expr_val)?,
                };
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
            // Remaining elements are patterns
            let graph_patterns = parse_subquery_patterns(
                &arr[2..],
                ctx,
                subject_counter,
                nested_counter,
                object_var_parsing,
            )?;
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
/// Filters and binds require a preceding node-map in the group, since they
/// constrain or compute from existing bindings. Other array forms (nested
/// optional, values, query, etc.) are self-contained.
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
                if !has_node_map_anchor {
                    let keyword = inner_arr.first().and_then(|v| v.as_str());
                    if matches!(keyword, Some("filter" | "bind")) {
                        return Err(ParseError::InvalidWhere(
                            "filter and bind in optional must follow a node-map pattern"
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
