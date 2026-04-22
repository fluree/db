//! Query options parsing
//!
//! Parses JSON-LD query modifiers:
//! - `limit` - maximum number of results
//! - `offset` - skip first N results
//! - `distinct` / `selectDistinct` - remove duplicate rows
//! - `orderBy` - sort results
//! - `groupBy` - group results for aggregation
//! - `having` - filter aggregated groups
//! - `depth` - auto-expand nested objects to depth N
//! - `reasoning` - enable/disable reasoning modes
//!
//! # Example
//!
//! ```json
//! {
//!   "select": ["?x"],
//!   "where": { "ex:name": "?x" },
//!   "limit": 10,
//!   "offset": 20,
//!   "orderBy": ["?x"],
//!   "distinct": true
//! }
//! ```

use super::ast::{
    UnresolvedAggregateFn, UnresolvedAggregateSpec, UnresolvedExpression, UnresolvedFilterValue,
    UnresolvedOptions, UnresolvedSortDirection, UnresolvedSortSpec,
};
use super::error::{ParseError, Result};
use super::filter_sexpr;
use serde_json::Value as JsonValue;
use std::sync::Arc;

/// Validate that a string looks like a variable (starts with ?)
fn validate_var_name(name: &str) -> Result<()> {
    if !name.starts_with('?') {
        return Err(ParseError::InvalidVariable(name.to_string()));
    }
    Ok(())
}

/// Parse depth option for graph select auto-expansion
///
/// Default is 0 (no auto-expansion).
///
/// # Example
///
/// ```json
/// { "depth": 3 }
/// ```
pub fn parse_depth(obj: &serde_json::Map<String, JsonValue>) -> Result<usize> {
    match obj.get("depth") {
        None => Ok(0), // Default: no auto-expansion
        Some(v) => {
            // Use as_i64() to catch negative numbers
            let n = v
                .as_i64()
                .ok_or_else(|| ParseError::InvalidOption("depth must be a number".into()))?;
            if n < 0 {
                return Err(ParseError::InvalidOption(
                    "depth must be non-negative".into(),
                ));
            }
            Ok(n as usize)
        }
    }
}

/// Parse limit from JSON
///
/// # Example
///
/// ```json
/// { "limit": 100 }
/// ```
pub fn parse_limit(obj: &serde_json::Map<String, JsonValue>) -> Result<Option<usize>> {
    match obj.get("limit") {
        None => Ok(None),
        Some(v) => {
            // Use as_i64() to catch negative numbers, then validate
            let n = v.as_i64().ok_or(ParseError::InvalidLimit)?;
            if n < 0 {
                return Err(ParseError::InvalidLimit);
            }
            Ok(Some(n as usize))
        }
    }
}

/// Parse offset from JSON
///
/// # Example
///
/// ```json
/// { "offset": 50 }
/// ```
pub fn parse_offset(obj: &serde_json::Map<String, JsonValue>) -> Result<Option<usize>> {
    match obj.get("offset") {
        None => Ok(None),
        Some(v) => {
            // Use as_i64() to catch negative numbers, then validate
            let n = v.as_i64().ok_or(ParseError::InvalidOffset)?;
            if n < 0 {
                return Err(ParseError::InvalidOffset);
            }
            Ok(Some(n as usize))
        }
    }
}

/// Parse distinct from JSON (supports both "distinct" and "selectDistinct")
///
/// # Example
///
/// ```json
/// { "distinct": true }
/// ```
///
/// or
///
/// ```json
/// { "selectDistinct": true }
/// ```
pub fn parse_distinct(obj: &serde_json::Map<String, JsonValue>) -> bool {
    obj.get("distinct")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
        || obj
            .get("selectDistinct")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
}

/// Parse orderBy from JSON
///
/// Supports multiple formats for compatibility:
/// - `"orderBy": "?x"` - single variable (defaults to ascending)
/// - `"orderBy": ["?x", "?y"]` - multiple variables
/// - `"orderBy": ["asc", "?x"]` - SPARQL translator form
/// - `"orderBy": "(desc ?x)"` - EDN-ish string form
/// - `"orderBy": {"var": "?x", "direction": "desc"}` - Rust-native JSON form
///
/// # Examples
///
/// ```json
/// { "orderBy": "?x" }
/// ```
///
/// ```json
/// { "orderBy": ["?x", "(desc ?y)"] }
/// ```
///
/// ```json
/// { "orderBy": [{"var": "?x", "direction": "asc"}] }
/// ```
pub fn parse_order_by(obj: &serde_json::Map<String, JsonValue>) -> Result<Vec<UnresolvedSortSpec>> {
    // Accept both camelCase and kebab-case for compatibility.
    let Some(val) = obj.get("orderBy").or_else(|| obj.get("order-by")) else {
        return Ok(Vec::new());
    };

    match val {
        // Collection of ordering terms
        JsonValue::Array(arr) => arr.iter().map(parse_single_order_term).collect(),
        // Single ordering term (scalar / object / ["asc","?x"])
        other => Ok(vec![parse_single_order_term(other)?]),
    }
}

/// Parse a single order term from any supported format
///
/// Supports multiple formats for compatibility:
/// - Object: `{"var":"?x","direction":"desc"}`
/// - String: `"?x"` (defaults asc) or `"(desc ?x)"` (EDN-ish)
/// - Array: `["asc", "?x"]` (SPARQL translator form)
fn parse_single_order_term(item: &JsonValue) -> Result<UnresolvedSortSpec> {
    match item {
        JsonValue::Object(map) => parse_order_term_object(map),
        JsonValue::String(s) => parse_order_term_string(s),
        JsonValue::Array(arr) => parse_order_term_array(arr),
        _ => Err(ParseError::InvalidOrderBy),
    }
}

/// Parse Rust-native JSON object form: `{"var":"?x","direction":"desc"}`
fn parse_order_term_object(map: &serde_json::Map<String, JsonValue>) -> Result<UnresolvedSortSpec> {
    let var = map
        .get("var")
        .and_then(|v| v.as_str())
        .ok_or(ParseError::InvalidOrderBy)?;
    validate_var_name(var)?;

    // Accept both "direction" and "order" as synonyms (clients vary)
    let dir_val = map
        .get("direction")
        .or_else(|| map.get("order"))
        .and_then(|v| v.as_str());

    let direction = match dir_val {
        None => UnresolvedSortDirection::Asc,
        Some(s) => parse_sort_direction(s)?,
    };

    Ok(UnresolvedSortSpec {
        var: Arc::from(var),
        direction,
    })
}

/// Parse string form: `"?x"` (plain var) or `"(desc ?x)"` (EDN-ish)
fn parse_order_term_string(s: &str) -> Result<UnresolvedSortSpec> {
    let trimmed = s.trim();

    // EDN-ish string form: "(asc ?x)" / "(desc ?x)"
    if trimmed.starts_with('(') && trimmed.ends_with(')') {
        let inner = trimmed.trim_start_matches('(').trim_end_matches(')').trim();
        let parts: Vec<&str> = inner.split_whitespace().collect();
        if parts.len() != 2 {
            return Err(ParseError::InvalidOrderBy);
        }
        let direction = parse_sort_direction(parts[0])?;
        let var = parts[1];
        validate_var_name(var)?;
        return Ok(UnresolvedSortSpec {
            var: Arc::from(var),
            direction,
        });
    }

    // Plain var string (defaults to ascending)
    validate_var_name(trimmed)?;
    Ok(UnresolvedSortSpec {
        var: Arc::from(trimmed),
        direction: UnresolvedSortDirection::Asc,
    })
}

/// Parse SPARQL translator array form: `["asc","?x"]` or `["desc","?x"]`
fn parse_order_term_array(arr: &[JsonValue]) -> Result<UnresolvedSortSpec> {
    if arr.len() != 2 {
        return Err(ParseError::InvalidOrderBy);
    }
    let dir = arr[0].as_str().ok_or(ParseError::InvalidOrderBy)?;
    let var = arr[1].as_str().ok_or(ParseError::InvalidOrderBy)?;
    let direction = parse_sort_direction(dir)?;
    validate_var_name(var)?;
    Ok(UnresolvedSortSpec {
        var: Arc::from(var),
        direction,
    })
}

/// Parse sort direction string ("asc" or "desc")
fn parse_sort_direction(s: &str) -> Result<UnresolvedSortDirection> {
    match s.to_lowercase().as_str() {
        "asc" => Ok(UnresolvedSortDirection::Asc),
        "desc" => Ok(UnresolvedSortDirection::Desc),
        other => Err(ParseError::InvalidSortDirection(other.to_string())),
    }
}

/// Parse groupBy from JSON
///
/// # Example
///
/// ```json
/// { "groupBy": ["?x", "?y"] }
/// ```
pub fn parse_group_by(obj: &serde_json::Map<String, JsonValue>) -> Result<Vec<Arc<str>>> {
    // Accept both camelCase and kebab-case for compatibility.
    let Some(val) = obj.get("groupBy").or_else(|| obj.get("group-by")) else {
        return Ok(Vec::new());
    };
    match val {
        JsonValue::String(s) => {
            validate_var_name(s)?;
            Ok(vec![Arc::from(s.as_str())])
        }
        JsonValue::Array(arr) => arr
            .iter()
            .map(|item| {
                let var = item.as_str().ok_or(ParseError::InvalidGroupBy)?;
                validate_var_name(var)?;
                Ok(Arc::from(var))
            })
            .collect(),
        _ => Err(ParseError::InvalidGroupBy),
    }
}

/// Parse having from JSON
///
/// HAVING filters results after aggregation (similar to SQL HAVING).
///
/// # Example
///
/// ```json
/// {
///   "groupBy": ["?category"],
///   "having": [">=", ["count", "?item"], 5]
/// }
/// ```
///
/// Note: This function requires `parse_filter_expr` from the parent module.
/// It returns `None` if no "having" key is present.
pub fn parse_having(
    obj: &serde_json::Map<String, JsonValue>,
    parse_filter_expr: impl Fn(&JsonValue) -> Result<UnresolvedExpression>,
) -> Result<Option<UnresolvedExpression>> {
    obj.get("having").map(parse_filter_expr).transpose()
}

/// Parse HAVING and extract aggregate specs from S-expression forms.
///
/// Compatibility:
/// - allow `"having": "(>= (count ?x) 2)"` and `"having": "(>= (avg ?x) 10)"`
/// - aggregate forms inside HAVING are rewritten into real query aggregates,
///   and the HAVING expression is rewritten to reference the aggregate output var.
fn parse_having_with_aggregates(
    obj: &serde_json::Map<String, JsonValue>,
    parse_filter_expr: impl Fn(&JsonValue) -> Result<UnresolvedExpression>,
) -> Result<(Option<UnresolvedExpression>, Vec<UnresolvedAggregateSpec>)> {
    let Some(having_val) = obj.get("having") else {
        return Ok((None, Vec::new()));
    };

    let raw_expr = match having_val {
        JsonValue::String(s) if s.trim_start().starts_with('(') => {
            filter_sexpr::parse_s_expression(s)?
        }
        other => parse_filter_expr(other)?,
    };

    let mut aggregates: Vec<UnresolvedAggregateSpec> = Vec::new();
    let mut counter: usize = 0;
    let rewritten = rewrite_having_aggregates(raw_expr, &mut aggregates, &mut counter)?;
    Ok((Some(rewritten), aggregates))
}

fn rewrite_having_aggregates(
    expr: UnresolvedExpression,
    aggregates: &mut Vec<UnresolvedAggregateSpec>,
    counter: &mut usize,
) -> Result<UnresolvedExpression> {
    use super::ast::UnresolvedExpression as E;

    let rewrite_child = |e: UnresolvedExpression,
                         aggregates: &mut Vec<UnresolvedAggregateSpec>,
                         counter: &mut usize|
     -> Result<UnresolvedExpression> {
        rewrite_having_aggregates(e, aggregates, counter)
    };

    match expr {
        E::Var(_) | E::Const(_) => Ok(expr),

        E::And(exprs) => Ok(E::And(
            exprs
                .into_iter()
                .map(|e| rewrite_child(e, aggregates, counter))
                .collect::<Result<Vec<_>>>()?,
        )),

        E::Or(exprs) => Ok(E::Or(
            exprs
                .into_iter()
                .map(|e| rewrite_child(e, aggregates, counter))
                .collect::<Result<Vec<_>>>()?,
        )),

        E::Not(inner) => Ok(E::Not(Box::new(rewrite_child(
            *inner, aggregates, counter,
        )?))),

        E::In {
            expr,
            values,
            negated,
        } => Ok(E::In {
            expr: Box::new(rewrite_child(*expr, aggregates, counter)?),
            values: values
                .into_iter()
                .map(|e| rewrite_child(e, aggregates, counter))
                .collect::<Result<Vec<_>>>()?,
            negated,
        }),

        E::Call { func, args } => {
            let name_lc = func.as_ref().to_ascii_lowercase();
            let agg_fn = match name_lc.as_str() {
                "count" => Some(UnresolvedAggregateFn::Count),
                "avg" => Some(UnresolvedAggregateFn::Avg),
                "sum" => Some(UnresolvedAggregateFn::Sum),
                "min" => Some(UnresolvedAggregateFn::Min),
                "max" => Some(UnresolvedAggregateFn::Max),
                _ => None,
            };

            if let Some(function) = agg_fn {
                if args.len() != 1 {
                    return Err(ParseError::InvalidFilter(format!(
                        "HAVING aggregate {func} requires exactly 1 argument"
                    )));
                }

                match &args[0] {
                    E::Var(v) => {
                        let out = Arc::from(format!("?__having_agg{}", *counter));
                        *counter += 1;
                        aggregates.push(UnresolvedAggregateSpec {
                            function,
                            input_var: Arc::clone(v),
                            output_var: Arc::clone(&out),
                        });
                        Ok(E::Var(out))
                    }
                    // Allow COUNT(*) in s-expression form: (count "*")
                    E::Const(UnresolvedFilterValue::String(s))
                        if s.as_ref() == "*"
                            && matches!(function, UnresolvedAggregateFn::Count) =>
                    {
                        let out = Arc::from(format!("?__having_agg{}", *counter));
                        *counter += 1;
                        aggregates.push(UnresolvedAggregateSpec {
                            function,
                            input_var: Arc::from("*"),
                            output_var: Arc::clone(&out),
                        });
                        Ok(E::Var(out))
                    }
                    _ => Err(ParseError::InvalidFilter(
                        "HAVING aggregate argument must be a variable".to_string(),
                    )),
                }
            } else {
                // Non-aggregate function: just rewrite its arguments.
                let rewritten_args = args
                    .into_iter()
                    .map(|e| rewrite_child(e, aggregates, counter))
                    .collect::<Result<Vec<_>>>()?;
                Ok(E::Call {
                    func,
                    args: rewritten_args,
                })
            }
        }

        // EXISTS/NOT EXISTS makes no sense in HAVING expressions
        E::Exists { .. } => Err(ParseError::InvalidFilter(
            "EXISTS/NOT EXISTS not supported in HAVING expressions".to_string(),
        )),
    }
}

/// Parse reasoning mode(s) from query options
///
/// Supports:
/// - `"reasoning": "none"` - disable all reasoning
/// - `"reasoning": "rdfs"` - RDFS only
/// - `"reasoning": "owl2ql"` - OWL2-QL (implies RDFS)
/// - `"reasoning": ["rdfs", "owl2ql"]` - multiple modes
/// - `"rules": [...]` - query-time datalog rules (enables datalog automatically)
/// - No key present - use defaults (auto-RDFS when hierarchy exists)
///
/// # Example
///
/// ```json
/// { "reasoning": "rdfs" }
/// ```
///
/// or
///
/// ```json
/// { "rules": [...datalog rules...] }
/// ```
pub fn parse_reasoning(
    obj: &serde_json::Map<String, JsonValue>,
) -> Result<Option<crate::rewrite::ReasoningModes>> {
    // Check if either reasoning or rules is present
    let has_reasoning = obj.contains_key("reasoning");
    let has_rules = obj.contains_key("rules");

    if !has_reasoning && !has_rules {
        return Ok(None);
    }

    // Reconstruct the query object for from_query_json
    let query_obj = JsonValue::Object(obj.clone());
    crate::rewrite::ReasoningModes::from_query_json(&query_obj)
        .map(Some)
        .map_err(|e| ParseError::InvalidOption(format!("reasoning: {e}")))
}

/// Parse all query options from JSON
///
/// Note: `aggregates` field is populated by `parse_select` from S-expression syntax,
/// not from a separate "aggregates" key.
///
/// # Example
///
/// ```json
/// {
///   "limit": 10,
///   "offset": 5,
///   "distinct": true,
///   "orderBy": ["?x"],
///   "groupBy": ["?category"]
/// }
/// ```
pub fn parse_options(
    obj: &serde_json::Map<String, JsonValue>,
    parse_filter_expr: impl Fn(&JsonValue) -> Result<UnresolvedExpression>,
) -> Result<UnresolvedOptions> {
    let (having, having_aggs) = parse_having_with_aggregates(obj, &parse_filter_expr)?;
    Ok(UnresolvedOptions {
        limit: parse_limit(obj)?,
        offset: parse_offset(obj)?,
        distinct: parse_distinct(obj),
        order_by: parse_order_by(obj)?,
        group_by: parse_group_by(obj)?,
        aggregates: having_aggs, // will be merged with aggregates from select clause
        having,
        reasoning: parse_reasoning(obj)?,
        object_var_parsing: parse_object_var_parsing(obj),
    })
}

pub fn parse_object_var_parsing(obj: &serde_json::Map<String, JsonValue>) -> bool {
    if let Some(opts) = obj.get("opts").and_then(|v| v.as_object()) {
        if let Some(flag) = opts.get("objectVarParsing") {
            return flag.as_bool().unwrap_or(true);
        }
    }
    obj.get("objectVarParsing")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_limit() {
        let json_val = json!({"limit": 100});
        let obj = json_val.as_object().unwrap();
        assert_eq!(parse_limit(obj).unwrap(), Some(100));

        let json_val = json!({});
        let obj = json_val.as_object().unwrap();
        assert_eq!(parse_limit(obj).unwrap(), None);

        let json_val = json!({"limit": -1});
        let obj = json_val.as_object().unwrap();
        assert!(parse_limit(obj).is_err());
    }

    #[test]
    fn test_parse_offset() {
        let json_val = json!({"offset": 50});
        let obj = json_val.as_object().unwrap();
        assert_eq!(parse_offset(obj).unwrap(), Some(50));

        let json_val = json!({});
        let obj = json_val.as_object().unwrap();
        assert_eq!(parse_offset(obj).unwrap(), None);
    }

    #[test]
    fn test_parse_distinct() {
        let json_val = json!({"distinct": true});
        let obj = json_val.as_object().unwrap();
        assert!(parse_distinct(obj));

        let json_val = json!({"selectDistinct": true});
        let obj = json_val.as_object().unwrap();
        assert!(parse_distinct(obj));

        let json_val = json!({});
        let obj = json_val.as_object().unwrap();
        assert!(!parse_distinct(obj));
    }

    #[test]
    fn test_parse_order_by_single_var() {
        let json_val = json!({"orderBy": "?x"});
        let obj = json_val.as_object().unwrap();
        let specs = parse_order_by(obj).unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].var.as_ref(), "?x");
        assert_eq!(specs[0].direction, UnresolvedSortDirection::Asc);
    }

    #[test]
    fn test_parse_order_by_edn_string() {
        let json_val = json!({"orderBy": "(desc ?y)"});
        let obj = json_val.as_object().unwrap();
        let specs = parse_order_by(obj).unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].var.as_ref(), "?y");
        assert_eq!(specs[0].direction, UnresolvedSortDirection::Desc);
    }

    #[test]
    fn test_parse_order_by_sparql_form() {
        let json_val = json!({"orderBy": [["asc", "?x"], ["desc", "?y"]]});
        let obj = json_val.as_object().unwrap();
        let specs = parse_order_by(obj).unwrap();
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].direction, UnresolvedSortDirection::Asc);
        assert_eq!(specs[1].direction, UnresolvedSortDirection::Desc);
    }

    #[test]
    fn test_parse_order_by_object_form() {
        let json_val = json!({
            "orderBy": {"var": "?x", "direction": "desc"}
        });
        let obj = json_val.as_object().unwrap();
        let specs = parse_order_by(obj).unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].var.as_ref(), "?x");
        assert_eq!(specs[0].direction, UnresolvedSortDirection::Desc);
    }

    #[test]
    fn test_parse_order_by_mixed_forms() {
        let json_val = json!({
            "orderBy": ["?x", "(desc ?y)", ["asc", "?z"]]
        });
        let obj = json_val.as_object().unwrap();
        let specs = parse_order_by(obj).unwrap();
        assert_eq!(specs.len(), 3);
        assert_eq!(specs[0].direction, UnresolvedSortDirection::Asc);
        assert_eq!(specs[1].direction, UnresolvedSortDirection::Desc);
        assert_eq!(specs[2].direction, UnresolvedSortDirection::Asc);
    }

    #[test]
    fn test_parse_group_by() {
        let json_val = json!({"groupBy": ["?x", "?y"]});
        let obj = json_val.as_object().unwrap();
        let vars = parse_group_by(obj).unwrap();
        assert_eq!(vars.len(), 2);
        assert_eq!(vars[0].as_ref(), "?x");
        assert_eq!(vars[1].as_ref(), "?y");
    }

    #[test]
    fn test_parse_group_by_invalid_var() {
        let json_val = json!({"groupBy": ["x"]}); // Missing '?'
        let obj = json_val.as_object().unwrap();
        assert!(parse_group_by(obj).is_err());
    }

    #[test]
    fn test_parse_depth() {
        let json_val = json!({"depth": 3});
        let obj = json_val.as_object().unwrap();
        assert_eq!(parse_depth(obj).unwrap(), 3);

        let json_val = json!({});
        let obj = json_val.as_object().unwrap();
        assert_eq!(parse_depth(obj).unwrap(), 0);

        let json_val = json!({"depth": -1});
        let obj = json_val.as_object().unwrap();
        assert!(parse_depth(obj).is_err());
    }

    #[test]
    fn test_parse_having() {
        let json_val = json!({"having": [">", "?count", 5]});
        let obj = json_val.as_object().unwrap();
        let dummy_filter = |_: &JsonValue| -> Result<UnresolvedExpression> {
            Ok(UnresolvedExpression::boolean(true))
        };
        let result = parse_having(obj, dummy_filter).unwrap();
        assert!(result.is_some());

        let json_val2 = json!({});
        let obj2 = json_val2.as_object().unwrap();
        let result = parse_having(obj2, dummy_filter).unwrap();
        assert!(result.is_none());
    }
}
