//! S-expression filter parsing
//!
//! Parses S-expression filter syntax used in JSON-LD queries.
//!
//! # Syntax
//!
//! - `"(> ?age 18)"`
//! - `"(and (> ?age 18) (< ?age 65))"`
//! - `"(in ?status [\"active\" \"pending\"])"`
//!
//! # Supported Constructs
//!
//! - **Variables**: `?var`
//! - **Literals**: numbers, booleans (`true`/`false`), quoted strings
//! - **Comparison**: `=`, `!=`, `<`, `<=`, `>`, `>=`
//! - **Logical**: `and`, `or`, `not`
//! - **Arithmetic**: `+`, `-`, `*`, `/`
//! - **Membership**: `in`, `not-in`, `notin`
//! - **Functions**: any other operator is treated as a function call
//!
//! # Limitations
//!
//! - Quoted strings cannot contain whitespace, parentheses, or escape sequences
//! - For complex string comparisons, use data expression format instead

use super::ast::UnresolvedExpression;
use super::error::{ParseError, Result};
use super::filter_common;
use super::sexpr_tokenize;
use super::sexpr_tokenize::SexprToken;
use std::sync::Arc;

/// Parse an S-expression string like "(> ?age 45)" into a filter expression
///
/// # Supported syntax
/// - Atoms: `?var`, numbers, `true`/`false`, quoted strings `"text"`
/// - Expressions: `(op arg1 arg2 ...)`
/// - Nested: `(and (> ?x 10) (< ?y 100))`
///
/// # Limitations
/// - Quoted strings cannot contain whitespace, parentheses, or escape sequences
///   (e.g., `"Smith Jr"` with a space will not parse correctly)
/// - For complex string comparisons, use the data expression format instead:
///   `["filter", ["=", "?name", "Smith Jr"]]`
pub fn parse_s_expression(s: &str) -> Result<UnresolvedExpression> {
    let s = s.trim();

    // Must start with (
    if !s.starts_with('(') || !s.ends_with(')') {
        // Could be a simple value
        return parse_s_expression_atom(s);
    }

    // Remove outer parens
    let inner = &s[1..s.len() - 1].trim();

    // Find the operator (first token)
    let (op, rest) = sexpr_tokenize::split_first_token(inner)?;
    let op_lower = op.to_lowercase();

    if op_lower.as_str() == "in" || op_lower.as_str() == "not-in" || op_lower.as_str() == "notin" {
        let (expr, values) = parse_s_expression_in_args(rest)?;
        return Ok(UnresolvedExpression::In {
            expr: Box::new(expr),
            values,
            negated: op_lower.as_str() != "in",
        });
    }

    // Parse arguments
    let args = parse_s_expression_args(rest)?;

    // Helper closure to clone already-parsed expressions
    let clone_expr = |e: &UnresolvedExpression| -> Result<UnresolvedExpression> { Ok(e.clone()) };

    // Convert to filter expression based on operator
    match op_lower.as_str() {
        // Comparison operators
        op @ ("=" | "eq" | "!=" | "<>" | "ne" | "<" | "lt" | "<=" | "le" | ">" | "gt" | ">="
        | "ge") => {
            let canonical = filter_common::normalize_op(op);
            filter_common::build_call(&args, canonical, clone_expr, 1, "comparison operator")
        }

        // Logical operators
        "and" => filter_common::build_and(&args, clone_expr),
        "or" => filter_common::build_or(&args, clone_expr),
        "not" => filter_common::build_not(&args, clone_expr),

        // Arithmetic operators
        op @ ("+" | "add" | "*" | "mul" | "/" | "div") => {
            let canonical = filter_common::normalize_op(op);
            filter_common::build_call(&args, canonical, clone_expr, 1, "arithmetic operator")
        }
        "-" | "sub" => {
            if args.len() == 1 {
                filter_common::build_call(&args, "negate", clone_expr, 1, "unary negation")
            } else {
                filter_common::build_call(&args, "-", clone_expr, 1, "arithmetic operator")
            }
        }

        // Function call
        _ => Ok(UnresolvedExpression::Call {
            func: Arc::from(op),
            args,
        }),
    }
}

/// Parse an atom in an S-expression (variable, number, string, boolean)
fn parse_s_expression_atom(s: &str) -> Result<UnresolvedExpression> {
    let s = s.trim();

    // Variable
    if s.starts_with('?') {
        return Ok(UnresolvedExpression::var(s));
    }

    // Boolean
    if s == "true" {
        return Ok(UnresolvedExpression::boolean(true));
    }
    if s == "false" {
        return Ok(UnresolvedExpression::boolean(false));
    }

    // Try to parse as number
    if let Ok(i) = s.parse::<i64>() {
        return Ok(UnresolvedExpression::long(i));
    }
    if let Ok(f) = s.parse::<f64>() {
        return Ok(UnresolvedExpression::double(f));
    }

    // String (might be quoted)
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        let unquoted = &s[1..s.len() - 1];
        return Ok(UnresolvedExpression::string(unquoted));
    }

    // Plain string
    Ok(UnresolvedExpression::string(s))
}

/// Parse arguments in an S-expression
fn parse_s_expression_args(s: &str) -> Result<Vec<UnresolvedExpression>> {
    let mut args = Vec::new();
    let mut remaining = s.trim();

    while !remaining.is_empty() {
        if remaining.starts_with('(') {
            // Find matching close paren
            let end = sexpr_tokenize::find_matching_paren(remaining)?;
            let expr_str = &remaining[..=end];
            args.push(parse_s_expression(expr_str)?);
            remaining = remaining[end + 1..].trim();
        } else if remaining.starts_with('[') {
            let end = sexpr_tokenize::find_matching_bracket(remaining)?;
            let expr_str = &remaining[..=end];
            args.push(parse_s_expression_list(expr_str)?);
            remaining = remaining[end + 1..].trim();
        } else if remaining.starts_with('"') {
            // Handle quoted string as a single token (may contain whitespace/parens)
            let after_open = &remaining[1..];
            if let Some(close_pos) = after_open.find('"') {
                let end = close_pos + 2; // include both quotes
                let atom = &remaining[..end];
                args.push(parse_s_expression_atom(atom)?);
                remaining = remaining[end..].trim();
            } else {
                return Err(ParseError::InvalidFilter(
                    "unclosed string literal".to_string(),
                ));
            }
        } else {
            // Parse as atom until whitespace or paren
            let end = remaining
                .find(|c: char| c.is_whitespace() || c == '(' || c == ')')
                .unwrap_or(remaining.len());
            if end > 0 {
                let atom = &remaining[..end];
                args.push(parse_s_expression_atom(atom)?);
            }
            remaining = remaining[end..].trim();
        }
    }

    Ok(args)
}

/// Parse arguments for 'in' operator which requires special handling
///
/// Format: `expr [val1 val2 ...]`
fn parse_s_expression_in_args(
    s: &str,
) -> Result<(UnresolvedExpression, Vec<UnresolvedExpression>)> {
    let mut remaining = s.trim();
    let (expr, rest) = parse_s_expression_arg(remaining)?;
    remaining = rest.trim();

    if !remaining.starts_with('[') {
        return Err(ParseError::InvalidFilter(
            "in requires a list literal like [...]".to_string(),
        ));
    }
    let end = sexpr_tokenize::find_matching_bracket(remaining)?;
    let list_expr = &remaining[..=end];
    let values_expr = parse_s_expression_list(list_expr)?;
    let values = match values_expr {
        UnresolvedExpression::Call { args, .. } => args,
        other => vec![other],
    };

    Ok((expr, values))
}

/// Parse a single argument in an S-expression, returning the expression and remaining string
fn parse_s_expression_arg(s: &str) -> Result<(UnresolvedExpression, &str)> {
    let s = s.trim_start();
    if s.is_empty() {
        return Err(ParseError::InvalidFilter("missing argument".to_string()));
    }
    if s.starts_with('(') {
        let end = sexpr_tokenize::find_matching_paren(s)?;
        let expr_str = &s[..=end];
        let expr = parse_s_expression(expr_str)?;
        return Ok((expr, &s[end + 1..]));
    }
    if s.starts_with('[') {
        let end = sexpr_tokenize::find_matching_bracket(s)?;
        let expr_str = &s[..=end];
        let expr = parse_s_expression_list(expr_str)?;
        return Ok((expr, &s[end + 1..]));
    }
    let end = s
        .find(|c: char| c.is_whitespace() || c == '(' || c == ')' || c == '[' || c == ']')
        .unwrap_or(s.len());
    let atom = &s[..end];
    let expr = parse_s_expression_atom(atom)?;
    Ok((expr, &s[end..]))
}

/// Convert an already-tokenized S-expression into an [`UnresolvedExpression`].
///
/// Used by the SELECT-clause parser, which tokenizes once to dispatch between
/// aggregate and scalar-expression forms and then needs the scalar form as an
/// expression tree.
///
/// Mirrors the operator handling in [`parse_s_expression`], with one caveat:
/// `in` / `not-in` are rejected because their bracketed-list form (`[...]`)
/// is not represented in [`SexprToken`]. Use the string S-expression form
/// inside BIND/FILTER if `IN` is needed.
pub fn expr_from_sexpr_token(tok: &SexprToken) -> Result<UnresolvedExpression> {
    match tok {
        // Quoted strings are *always* string literals — never reparsed as
        // booleans, numbers, or variables. This is the only way to write
        // a literal whose source spelling collides with a keyword
        // (e.g. `"false"`, `"42"`, `"?notavar"`).
        SexprToken::String(s) => Ok(UnresolvedExpression::string(s)),
        SexprToken::Atom(s) => atom_token_to_expr(s),
        SexprToken::List(items) => list_tokens_to_expr(items),
    }
}

fn atom_token_to_expr(s: &str) -> Result<UnresolvedExpression> {
    if s.starts_with('?') {
        return Ok(UnresolvedExpression::var(s));
    }
    if s == "true" {
        return Ok(UnresolvedExpression::boolean(true));
    }
    if s == "false" {
        return Ok(UnresolvedExpression::boolean(false));
    }
    if let Ok(i) = s.parse::<i64>() {
        return Ok(UnresolvedExpression::long(i));
    }
    if let Ok(f) = s.parse::<f64>() {
        return Ok(UnresolvedExpression::double(f));
    }
    // Unquoted atom that didn't match any other shape — treat as a bare
    // string. Quoted strings take the explicit `String` path above.
    Ok(UnresolvedExpression::string(s))
}

fn list_tokens_to_expr(items: &[SexprToken]) -> Result<UnresolvedExpression> {
    if items.is_empty() {
        return Err(ParseError::InvalidSelect(
            "empty expression in select clause".to_string(),
        ));
    }

    let op = items[0].expect_atom("operator")?;
    let op_lower = op.to_lowercase();

    if matches!(op_lower.as_str(), "in" | "not-in" | "notin") {
        return Err(ParseError::InvalidSelect(format!(
            "'{op}' is not supported in select expressions; rewrite using or/and equality (e.g. (or (= ?x 1) (= ?x 2)))"
        )));
    }

    let arg_tokens = &items[1..];
    let clone_expr = |e: &UnresolvedExpression| -> Result<UnresolvedExpression> { Ok(e.clone()) };
    let parsed: Result<Vec<UnresolvedExpression>> =
        arg_tokens.iter().map(expr_from_sexpr_token).collect();
    let args = parsed?;

    match op_lower.as_str() {
        op @ ("=" | "eq" | "!=" | "<>" | "ne" | "<" | "lt" | "<=" | "le" | ">" | "gt" | ">="
        | "ge") => {
            let canonical = filter_common::normalize_op(op);
            filter_common::build_call(&args, canonical, clone_expr, 1, "comparison operator")
        }
        "and" => filter_common::build_and(&args, clone_expr),
        "or" => filter_common::build_or(&args, clone_expr),
        "not" => filter_common::build_not(&args, clone_expr),
        op @ ("+" | "add" | "*" | "mul" | "/" | "div") => {
            let canonical = filter_common::normalize_op(op);
            filter_common::build_call(&args, canonical, clone_expr, 1, "arithmetic operator")
        }
        "-" | "sub" => {
            if args.len() == 1 {
                filter_common::build_call(&args, "negate", clone_expr, 1, "unary negation")
            } else {
                filter_common::build_call(&args, "-", clone_expr, 1, "arithmetic operator")
            }
        }
        _ => Ok(UnresolvedExpression::Call {
            func: Arc::from(op),
            args,
        }),
    }
}

/// Parse a bracketed list `[...]` as a function call with name "list"
fn parse_s_expression_list(s: &str) -> Result<UnresolvedExpression> {
    let s = s.trim();
    if !s.starts_with('[') || !s.ends_with(']') {
        return Err(ParseError::InvalidFilter(
            "list must be bracketed".to_string(),
        ));
    }
    let inner = &s[1..s.len() - 1];
    let args = parse_s_expression_args(inner)?;
    Ok(UnresolvedExpression::Call {
        func: Arc::from("list"),
        args,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_comparison() {
        let expr = parse_s_expression("(> ?age 18)").unwrap();
        match expr {
            UnresolvedExpression::Call { func, args } => {
                assert_eq!(func.as_ref(), ">");
                assert_eq!(args.len(), 2);
            }
            _ => panic!("Expected Call"),
        }
    }

    #[test]
    fn test_parse_logical_and() {
        let expr = parse_s_expression("(and (> ?x 10) (< ?y 20))").unwrap();
        match expr {
            UnresolvedExpression::And(exprs) => {
                assert_eq!(exprs.len(), 2);
            }
            _ => panic!("Expected AND"),
        }
    }

    #[test]
    fn test_parse_atom_variable() {
        let expr = parse_s_expression_atom("?name").unwrap();
        match expr {
            UnresolvedExpression::Var(name) => {
                assert_eq!(name.as_ref(), "?name");
            }
            _ => panic!("Expected variable"),
        }
    }

    #[test]
    fn test_parse_atom_boolean() {
        let expr = parse_s_expression_atom("true").unwrap();
        match expr {
            UnresolvedExpression::Const(_) => {}
            _ => panic!("Expected constant"),
        }
    }

    #[test]
    fn test_parse_atom_number() {
        let expr = parse_s_expression_atom("42").unwrap();
        match expr {
            UnresolvedExpression::Const(_) => {}
            _ => panic!("Expected constant"),
        }
    }

    #[test]
    fn test_parse_in_operator() {
        let expr = parse_s_expression("(in ?status [\"active\" \"pending\"])").unwrap();
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
    fn test_parse_function_call() {
        let expr = parse_s_expression("(strlen ?name)").unwrap();
        match expr {
            UnresolvedExpression::Call { func, args } => {
                assert_eq!(func.as_ref(), "strlen");
                assert_eq!(args.len(), 1);
            }
            _ => panic!("Expected function call"),
        }
    }

    #[test]
    fn test_parse_arithmetic() {
        let expr = parse_s_expression("(+ ?x 5)").unwrap();
        match expr {
            UnresolvedExpression::Call { func, args } => {
                assert_eq!(func.as_ref(), "+");
                assert_eq!(args.len(), 2);
            }
            _ => panic!("Expected Call"),
        }
    }

    #[test]
    fn test_parse_unary_negation() {
        let expr = parse_s_expression("(- ?x)").unwrap();
        match expr {
            UnresolvedExpression::Call { func, args } => {
                assert_eq!(func.as_ref(), "negate");
                assert_eq!(args.len(), 1);
            }
            _ => panic!("Expected Call with negate"),
        }
    }

    #[test]
    fn test_parse_variadic_comparison() {
        let expr = parse_s_expression("(< ?a ?b ?c)").unwrap();
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
        let expr = parse_s_expression("(+ ?x 5 10)").unwrap();
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
        let expr = parse_s_expression("(+ ?x)").unwrap();
        match expr {
            UnresolvedExpression::Call { func, args } => {
                assert_eq!(func.as_ref(), "+");
                assert_eq!(args.len(), 1);
            }
            _ => panic!("Expected Call"),
        }
    }
}
