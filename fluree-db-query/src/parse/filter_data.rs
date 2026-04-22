//! Data expression (JSON array) filter parsing
//!
//! Parses JSON array-based filter expressions used in JSON-LD queries.
//!
//! # Syntax
//!
//! ```json
//! [">", "?age", 18]
//! ["and", [">", "?age", 18], ["<", "?age", 65]]
//! ["in", "?status", ["active", "pending"]]
//! ```
//!
//! # Supported Constructs
//!
//! - **Variables**: `"?var"`
//! - **Literals**: numbers, booleans, strings
//! - **Comparison**: `=`, `!=`, `<`, `<=`, `>`, `>=`, `eq`, `ne`, `lt`, `le`, `gt`, `ge`
//! - **Logical**: `and`, `or`, `not`
//! - **Arithmetic**: `+`, `-`, `*`, `/`, `add`, `sub`, `mul`, `div`
//! - **Membership**: `in`, `not-in`, `notin`
//! - **Functions**: any other operator treated as function call

use super::ast::{UnresolvedExpression, UnresolvedPattern};
use super::error::{ParseError, Result};
use super::filter_common;
use serde_json::Value as JsonValue;
use std::sync::Arc;

/// Callback type for parsing EXISTS/NOT EXISTS pattern content inside filter expressions.
///
/// Given the arguments after the "exists"/"not-exists" keyword (a slice of JSON values
/// representing node-map patterns), returns a list of `UnresolvedPattern`.
pub type PatternParserFn<'a> = &'a dyn Fn(&[JsonValue]) -> Result<Vec<UnresolvedPattern>>;

/// Check if a string is a variable (starts with '?')
fn is_variable(s: &str) -> bool {
    s.starts_with('?')
}

/// Parse a filter expression from JSON
///
/// Filter expressions can be:
/// - Variables: "?age"
/// - Constants: 18, 3.14, "hello", true
/// - Comparisons: [">", "?age", 18], ["=", "?name", "Alice"]
/// - Logical: ["and", ...], ["or", ...], ["not", ...]
/// - Arithmetic: ["+", "?x", 1], ["-", "?a", "?b"]
/// - Functions: ["strlen", "?name"], ["contains", "?str", "foo"]
pub fn parse_filter_expr(value: &JsonValue) -> Result<UnresolvedExpression> {
    parse_filter_expr_inner(value, parse_filter_array)
}

/// Parse a filter expression with EXISTS/NOT EXISTS support.
///
/// Like `parse_filter_expr` but recognizes `["exists", ...]` and `["not-exists", ...]`
/// inside compound expressions (e.g., `["or", ["=", "?x", "?y"], ["not-exists", {...}]]`).
/// The `pattern_parser` callback handles parsing the node-map patterns inside EXISTS.
pub fn parse_filter_expr_ctx(
    value: &JsonValue,
    pattern_parser: PatternParserFn<'_>,
) -> Result<UnresolvedExpression> {
    parse_filter_expr_inner(value, |arr| {
        dispatch_filter_op(
            arr,
            |v| parse_filter_expr_ctx(v, pattern_parser),
            Some(pattern_parser),
        )
    })
}

/// Parse a filter expression array (operation)
///
/// # Format
///
/// ```json
/// [operator, arg1, arg2, ...]
/// ```
///
/// # Examples
///
/// ```json
/// [">", "?age", 18]
/// ["and", [">", "?x", 10], ["<", "?y", 20]]
/// ["in", "?status", ["active", "pending"]]
/// ["strlen", "?name"]
/// ```
pub fn parse_filter_array(arr: &[JsonValue]) -> Result<UnresolvedExpression> {
    dispatch_filter_op(arr, parse_filter_expr, None)
}

/// Shared expression parser — dispatches array values to `parse_array`, handles all other types.
fn parse_filter_expr_inner(
    value: &JsonValue,
    parse_array: impl Fn(&[JsonValue]) -> Result<UnresolvedExpression>,
) -> Result<UnresolvedExpression> {
    match value {
        JsonValue::String(s) => {
            if is_variable(s) {
                Ok(UnresolvedExpression::var(s))
            } else {
                Ok(UnresolvedExpression::string(s))
            }
        }
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(UnresolvedExpression::long(i))
            } else if let Some(f) = n.as_f64() {
                Ok(UnresolvedExpression::double(f))
            } else {
                Err(ParseError::InvalidFilter(format!(
                    "unsupported number in filter: {n}"
                )))
            }
        }
        JsonValue::Bool(b) => Ok(UnresolvedExpression::boolean(*b)),
        JsonValue::Array(arr) => parse_array(arr),
        JsonValue::Null => Err(ParseError::InvalidFilter(
            "null not supported in filter expressions".to_string(),
        )),
        JsonValue::Object(_) => Err(ParseError::InvalidFilter(
            "objects not supported in filter expressions".to_string(),
        )),
    }
}

/// Shared operator dispatch for filter array expressions.
///
/// When `pattern_parser` is `Some`, handles `exists`/`not-exists`/`notexists` operators
/// by delegating to the pattern parser callback. Otherwise those keywords fall through
/// to the generic function-call arm.
fn dispatch_filter_op(
    arr: &[JsonValue],
    recurse: impl Fn(&JsonValue) -> Result<UnresolvedExpression>,
    pattern_parser: Option<PatternParserFn<'_>>,
) -> Result<UnresolvedExpression> {
    if arr.is_empty() {
        return Err(ParseError::InvalidFilter(
            "empty array in filter expression".to_string(),
        ));
    }

    let op_name = arr[0]
        .as_str()
        .ok_or_else(|| ParseError::InvalidFilter("filter operator must be a string".to_string()))?;

    let op_lower = op_name.to_lowercase();
    let args = &arr[1..];

    match op_lower.as_str() {
        // EXISTS / NOT EXISTS — only when pattern_parser is provided
        "exists" | "not-exists" | "notexists" if pattern_parser.is_some() => {
            if args.is_empty() {
                return Err(ParseError::InvalidFilter(
                    "exists/not-exists requires at least one pattern".to_string(),
                ));
            }
            let negated = op_lower != "exists";
            let patterns = pattern_parser.unwrap()(args)?;
            Ok(UnresolvedExpression::Exists { patterns, negated })
        }

        // Comparison operators
        op @ ("=" | "eq" | "!=" | "<>" | "ne" | "<" | "lt" | "<=" | "le" | ">" | "gt" | ">="
        | "ge") => {
            let canonical = filter_common::normalize_op(op);
            filter_common::build_call(args, canonical, &recurse, 1, "comparison operator")
        }

        // Logical operators
        "and" => filter_common::build_and(args, &recurse),
        "or" => filter_common::build_or(args, &recurse),
        "not" => filter_common::build_not(args, &recurse),

        "in" | "not-in" | "notin" => {
            if args.len() < 2 {
                return Err(ParseError::InvalidFilter(
                    "'in' requires at least 2 arguments".to_string(),
                ));
            }
            let expr = recurse(&args[0])?;
            let negated = matches!(op_lower.as_str(), "not-in" | "notin");
            let values: Result<Vec<_>> = if args.len() == 2 {
                if let JsonValue::Array(list) = &args[1] {
                    list.iter().map(&recurse).collect()
                } else {
                    vec![recurse(&args[1])].into_iter().collect()
                }
            } else {
                args[1..].iter().map(&recurse).collect()
            };
            Ok(UnresolvedExpression::In {
                expr: Box::new(expr),
                values: values?,
                negated,
            })
        }

        // Arithmetic operators
        op @ ("+" | "add" | "*" | "mul" | "/" | "div") => {
            let canonical = filter_common::normalize_op(op);
            filter_common::build_call(args, canonical, &recurse, 1, "arithmetic operator")
        }
        "-" | "sub" => {
            if args.len() == 1 {
                filter_common::build_call(args, "negate", &recurse, 1, "unary negation")
            } else {
                filter_common::build_call(args, "-", &recurse, 1, "arithmetic operator")
            }
        }

        // Everything else is a function call
        _ => {
            let fn_args: Result<Vec<_>> = args.iter().map(&recurse).collect();
            Ok(UnresolvedExpression::Call {
                func: Arc::from(op_name),
                args: fn_args?,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_variable() {
        let json_val = json!("?age");
        let expr = parse_filter_expr(&json_val).unwrap();
        match expr {
            UnresolvedExpression::Var(name) => {
                assert_eq!(name.as_ref(), "?age");
            }
            _ => panic!("Expected variable"),
        }
    }

    #[test]
    fn test_parse_string_constant() {
        let json_val = json!("hello");
        let expr = parse_filter_expr(&json_val).unwrap();
        match expr {
            UnresolvedExpression::Const(_) => {}
            _ => panic!("Expected constant"),
        }
    }

    #[test]
    fn test_parse_number() {
        let json_val = json!(42);
        let expr = parse_filter_expr(&json_val).unwrap();
        match expr {
            UnresolvedExpression::Const(_) => {}
            _ => panic!("Expected constant"),
        }
    }

    #[test]
    fn test_parse_boolean() {
        let json_val = json!(true);
        let expr = parse_filter_expr(&json_val).unwrap();
        match expr {
            UnresolvedExpression::Const(_) => {}
            _ => panic!("Expected constant"),
        }
    }

    #[test]
    fn test_parse_comparison() {
        let json_val = json!([">", "?age", 18]);
        let expr = parse_filter_expr(&json_val).unwrap();
        match expr {
            UnresolvedExpression::Call { func, .. } => {
                assert_eq!(func.as_ref(), ">");
            }
            _ => panic!("Expected Call"),
        }
    }

    #[test]
    fn test_parse_logical_and() {
        let json_val = json!(["and", [">", "?x", 10], ["<", "?y", 20]]);
        let expr = parse_filter_expr(&json_val).unwrap();
        match expr {
            UnresolvedExpression::And(exprs) => {
                assert_eq!(exprs.len(), 2);
            }
            _ => panic!("Expected AND"),
        }
    }

    #[test]
    fn test_parse_in_operator() {
        let json_val = json!(["in", "?status", ["active", "pending"]]);
        let expr = parse_filter_expr(&json_val).unwrap();
        match expr {
            UnresolvedExpression::In {
                negated, values, ..
            } => {
                assert!(!negated);
                assert_eq!(values.len(), 2);
            }
            _ => panic!("Expected IN"),
        }
    }

    #[test]
    fn test_parse_not_in_operator() {
        let json_val = json!(["not-in", "?status", ["inactive"]]);
        let expr = parse_filter_expr(&json_val).unwrap();
        match expr {
            UnresolvedExpression::In { negated, .. } => {
                assert!(negated);
            }
            _ => panic!("Expected IN with negated"),
        }
    }

    #[test]
    fn test_parse_arithmetic() {
        let json_val = json!(["+", "?x", 5]);
        let expr = parse_filter_expr(&json_val).unwrap();
        match expr {
            UnresolvedExpression::Call { func, .. } => {
                assert_eq!(func.as_ref(), "+");
            }
            _ => panic!("Expected Call"),
        }
    }

    #[test]
    fn test_parse_unary_negation() {
        let json_val = json!(["-", "?x"]);
        let expr = parse_filter_expr(&json_val).unwrap();
        match expr {
            UnresolvedExpression::Call { func, args } => {
                assert_eq!(func.as_ref(), "negate");
                assert_eq!(args.len(), 1);
            }
            _ => panic!("Expected Call with negate"),
        }
    }

    #[test]
    fn test_parse_function_call() {
        let json_val = json!(["strlen", "?name"]);
        let expr = parse_filter_expr(&json_val).unwrap();
        match expr {
            UnresolvedExpression::Call { func, args } => {
                assert_eq!(func.as_ref(), "strlen");
                assert_eq!(args.len(), 1);
            }
            _ => panic!("Expected function call"),
        }
    }

    #[test]
    fn test_parse_nested_expression() {
        let json_val = json!(["and", [">", "?age", 18], ["=", "?status", "active"]]);
        let expr = parse_filter_expr(&json_val).unwrap();
        match expr {
            UnresolvedExpression::And(exprs) => {
                assert_eq!(exprs.len(), 2);
                assert!(matches!(&exprs[0], UnresolvedExpression::Call { .. }));
                assert!(matches!(&exprs[1], UnresolvedExpression::Call { .. }));
            }
            _ => panic!("Expected AND"),
        }
    }

    #[test]
    fn test_empty_array_error() {
        let json_val = json!([]);
        assert!(parse_filter_expr(&json_val).is_err());
    }

    #[test]
    fn test_null_not_supported() {
        let json_val = json!(null);
        assert!(parse_filter_expr(&json_val).is_err());
    }

    #[test]
    fn test_parse_variadic_comparison() {
        let json_val = json!(["<", "?a", "?b", "?c"]);
        let expr = parse_filter_expr(&json_val).unwrap();
        match expr {
            UnresolvedExpression::Call { func, args } => {
                assert_eq!(func.as_ref(), "<");
                assert_eq!(args.len(), 3);
            }
            _ => panic!("Expected Call"),
        }
    }

    #[test]
    fn test_parse_variadic_arithmetic() {
        let json_val = json!(["+", "?x", 5, 10]);
        let expr = parse_filter_expr(&json_val).unwrap();
        match expr {
            UnresolvedExpression::Call { func, args } => {
                assert_eq!(func.as_ref(), "+");
                assert_eq!(args.len(), 3);
            }
            _ => panic!("Expected Call"),
        }
    }

    #[test]
    fn test_parse_single_arg_arithmetic() {
        let json_val = json!(["+", "?x"]);
        let expr = parse_filter_expr(&json_val).unwrap();
        match expr {
            UnresolvedExpression::Call { func, args } => {
                assert_eq!(func.as_ref(), "+");
                assert_eq!(args.len(), 1);
            }
            _ => panic!("Expected Call"),
        }
    }

    #[test]
    fn test_parse_single_arg_comparison() {
        let json_val = json!(["=", "?x"]);
        let expr = parse_filter_expr(&json_val).unwrap();
        match expr {
            UnresolvedExpression::Call { func, args } => {
                assert_eq!(func.as_ref(), "=");
                assert_eq!(args.len(), 1);
            }
            _ => panic!("Expected Call"),
        }
    }

    #[test]
    fn test_parse_word_aliases_normalize() {
        // Word aliases should normalize to symbol form
        let expr = parse_filter_expr(&json!(["eq", "?x", 1])).unwrap();
        match expr {
            UnresolvedExpression::Call { func, .. } => assert_eq!(func.as_ref(), "="),
            _ => panic!("Expected Call"),
        }

        let expr = parse_filter_expr(&json!(["add", "?x", 1])).unwrap();
        match expr {
            UnresolvedExpression::Call { func, .. } => assert_eq!(func.as_ref(), "+"),
            _ => panic!("Expected Call"),
        }
    }
}
