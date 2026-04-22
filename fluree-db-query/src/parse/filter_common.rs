//! Common filter parsing utilities
//!
//! Shared utilities for parsing filter expressions from both:
//! - S-expressions: `(> ?age 18)`
//! - Data expressions: `[">", "?age", 18]`
//!
//! Provides generic helpers to reduce duplication between parsing formats.

use super::ast::UnresolvedExpression;
use super::error::{ParseError, Result};
use std::sync::Arc;

/// Validate argument count and return error if it doesn't match expected count
///
/// # Example
///
/// ```
/// use fluree_db_query::parse::filter_common::validate_arg_count;
///
/// assert!(validate_arg_count(&[1, 2], 2, "comparison operator").is_ok());
/// assert!(validate_arg_count(&[1], 2, "comparison operator").is_err());
/// ```
pub fn validate_arg_count<T>(args: &[T], expected: usize, context: &str) -> Result<()> {
    if args.len() != expected {
        return Err(ParseError::InvalidFilter(format!(
            "{} requires exactly {} argument{}, got {}",
            context,
            expected,
            if expected == 1 { "" } else { "s" },
            args.len()
        )));
    }
    Ok(())
}

/// Validate minimum argument count
pub fn validate_min_arg_count<T>(args: &[T], min: usize, context: &str) -> Result<()> {
    if args.len() < min {
        return Err(ParseError::InvalidFilter(format!(
            "{} requires at least {} argument{}, got {}",
            context,
            min,
            if min == 1 { "" } else { "s" },
            args.len()
        )));
    }
    Ok(())
}

/// Build a variadic function call expression
///
/// Generic over the input type `T` so it works with both:
/// - `&UnresolvedExpression` (already parsed, for S-expressions)
/// - `&JsonValue` (needs parsing, for data expressions)
///
/// The `parser` function converts `T` to `UnresolvedExpression`.
pub fn build_call<T, F>(
    args: &[T],
    func: &str,
    parser: F,
    min_args: usize,
    context: &str,
) -> Result<UnresolvedExpression>
where
    F: Fn(&T) -> Result<UnresolvedExpression>,
{
    validate_min_arg_count(args, min_args, context)?;

    let parsed: Result<Vec<_>> = args.iter().map(parser).collect();

    Ok(UnresolvedExpression::Call {
        func: Arc::from(func),
        args: parsed?,
    })
}

/// Build a logical AND expression from a list of sub-expressions
pub fn build_and<T, F>(args: &[T], parser: F) -> Result<UnresolvedExpression>
where
    F: Fn(&T) -> Result<UnresolvedExpression>,
{
    validate_min_arg_count(args, 1, "'and'")?;

    let exprs: Result<Vec<_>> = args.iter().map(parser).collect();
    Ok(UnresolvedExpression::And(exprs?))
}

/// Build a logical OR expression from a list of sub-expressions
pub fn build_or<T, F>(args: &[T], parser: F) -> Result<UnresolvedExpression>
where
    F: Fn(&T) -> Result<UnresolvedExpression>,
{
    validate_min_arg_count(args, 1, "'or'")?;

    let exprs: Result<Vec<_>> = args.iter().map(parser).collect();
    Ok(UnresolvedExpression::Or(exprs?))
}

/// Build a logical NOT expression from a single sub-expression
pub fn build_not<T, F>(args: &[T], parser: F) -> Result<UnresolvedExpression>
where
    F: Fn(&T) -> Result<UnresolvedExpression>,
{
    validate_arg_count(args, 1, "'not'")?;

    let expr = parser(&args[0])?;
    Ok(UnresolvedExpression::Not(Box::new(expr)))
}

/// Check if an operator name is a comparison operator
///
/// Recognizes: `=`, `eq`, `!=`, `<>`, `ne`, `<`, `lt`, `<=`, `le`, `>`, `gt`, `>=`, `ge`
pub fn is_compare_op(op: &str) -> bool {
    matches!(
        op.to_lowercase().as_str(),
        "=" | "eq" | "!=" | "<>" | "ne" | "<" | "lt" | "<=" | "le" | ">" | "gt" | ">=" | "ge"
    )
}

/// Check if an operator name is an arithmetic operator
///
/// Recognizes: `+`, `add`, `-`, `sub`, `*`, `mul`, `/`, `div`
pub fn is_arithmetic_op(op: &str) -> bool {
    matches!(
        op.to_lowercase().as_str(),
        "+" | "add" | "-" | "sub" | "*" | "mul" | "/" | "div"
    )
}

/// Normalize an operator name to its canonical symbol form
///
/// Maps word aliases to their symbol equivalents:
/// - `eq` → `=`, `ne` → `!=`, `lt` → `<`, `le` → `<=`, `gt` → `>`, `ge` → `>=`
/// - `add` → `+`, `sub` → `-`, `mul` → `*`, `div` → `/`
///
/// Returns the input unchanged if it's already in symbol form or unrecognized.
pub fn normalize_op(op: &str) -> &str {
    match op.to_lowercase().as_str() {
        "eq" => "=",
        "ne" | "<>" => "!=",
        "lt" => "<",
        "le" => "<=",
        "gt" => ">",
        "ge" => ">=",
        "add" => "+",
        "sub" => "-",
        "mul" => "*",
        "div" => "/",
        _ => op,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_arg_count() {
        let args = vec![1, 2];
        assert!(validate_arg_count(&args, 2, "test").is_ok());
        assert!(validate_arg_count(&args, 3, "test").is_err());
        assert!(validate_arg_count(&args, 1, "test").is_err());
    }

    #[test]
    fn test_validate_min_arg_count() {
        let args = vec![1, 2, 3];
        assert!(validate_min_arg_count(&args, 1, "test").is_ok());
        assert!(validate_min_arg_count(&args, 3, "test").is_ok());
        assert!(validate_min_arg_count(&args, 4, "test").is_err());
    }

    #[test]
    fn test_build_call() {
        let args = vec![1, 2];
        let parser =
            |x: &i32| -> Result<UnresolvedExpression> { Ok(UnresolvedExpression::long(*x as i64)) };

        let expr = build_call(&args, "=", parser, 1, "test comparison").unwrap();

        match expr {
            UnresolvedExpression::Call { func, args } => {
                assert_eq!(func.as_ref(), "=");
                assert_eq!(args.len(), 2);
                assert!(matches!(args[0], UnresolvedExpression::Const(_)));
                assert!(matches!(args[1], UnresolvedExpression::Const(_)));
            }
            _ => panic!("Expected Call expression"),
        }
    }

    #[test]
    fn test_build_call_variadic() {
        let args = vec![1, 2, 3];
        let parser =
            |x: &i32| -> Result<UnresolvedExpression> { Ok(UnresolvedExpression::long(*x as i64)) };

        let expr = build_call(&args, "<", parser, 1, "test comparison").unwrap();

        match expr {
            UnresolvedExpression::Call { func, args } => {
                assert_eq!(func.as_ref(), "<");
                assert_eq!(args.len(), 3);
            }
            _ => panic!("Expected Call expression"),
        }
    }

    #[test]
    fn test_build_call_single_arg() {
        let args = vec![1];
        let parser =
            |x: &i32| -> Result<UnresolvedExpression> { Ok(UnresolvedExpression::long(*x as i64)) };

        let expr = build_call(&args, "+", parser, 1, "test arithmetic").unwrap();

        match expr {
            UnresolvedExpression::Call { func, args } => {
                assert_eq!(func.as_ref(), "+");
                assert_eq!(args.len(), 1);
            }
            _ => panic!("Expected Call expression"),
        }
    }

    #[test]
    fn test_is_compare_op() {
        assert!(is_compare_op("="));
        assert!(is_compare_op("eq"));
        assert!(is_compare_op("EQ"));
        assert!(is_compare_op("!="));
        assert!(is_compare_op("<>"));
        assert!(is_compare_op("<"));
        assert!(is_compare_op(">="));
        assert!(!is_compare_op("+"));
        assert!(!is_compare_op("unknown"));
    }

    #[test]
    fn test_is_arithmetic_op() {
        assert!(is_arithmetic_op("+"));
        assert!(is_arithmetic_op("add"));
        assert!(is_arithmetic_op("ADD"));
        assert!(is_arithmetic_op("-"));
        assert!(is_arithmetic_op("*"));
        assert!(is_arithmetic_op("/"));
        assert!(!is_arithmetic_op("="));
        assert!(!is_arithmetic_op("unknown"));
    }

    #[test]
    fn test_normalize_op() {
        assert_eq!(normalize_op("eq"), "=");
        assert_eq!(normalize_op("ne"), "!=");
        assert_eq!(normalize_op("lt"), "<");
        assert_eq!(normalize_op("le"), "<=");
        assert_eq!(normalize_op("gt"), ">");
        assert_eq!(normalize_op("ge"), ">=");
        assert_eq!(normalize_op("add"), "+");
        assert_eq!(normalize_op("sub"), "-");
        assert_eq!(normalize_op("mul"), "*");
        assert_eq!(normalize_op("div"), "/");
        assert_eq!(normalize_op("="), "=");
        assert_eq!(normalize_op("unknown"), "unknown");
    }

    #[test]
    fn test_build_and() {
        let args = vec![true, false];
        let parser =
            |x: &bool| -> Result<UnresolvedExpression> { Ok(UnresolvedExpression::boolean(*x)) };

        let expr = build_and(&args, parser).unwrap();

        match expr {
            UnresolvedExpression::And(exprs) => {
                assert_eq!(exprs.len(), 2);
            }
            _ => panic!("Expected And expression"),
        }
    }

    #[test]
    fn test_build_not() {
        let args = vec![true];
        let parser =
            |x: &bool| -> Result<UnresolvedExpression> { Ok(UnresolvedExpression::boolean(*x)) };

        let expr = build_not(&args, parser).unwrap();

        match expr {
            UnresolvedExpression::Not(inner) => {
                assert!(matches!(*inner, UnresolvedExpression::Const(_)));
            }
            _ => panic!("Expected Not expression"),
        }
    }
}
