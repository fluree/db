//! Property path expression parsers for `@path` context entries.
//!
//! Two forms are supported:
//!
//! **String form** — SPARQL 1.1 property path syntax:
//! ```text
//! "ex:friend+"           OneOrMore(Iri(...))
//! "ex:a/ex:b"            Sequence([Iri(a), Iri(b)])
//! "(ex:a|ex:b)+"         OneOrMore(Alternative([Iri(a), Iri(b)]))
//! "^ex:parent"           Inverse(Iri(...))
//! ```
//!
//! **Array form** — S-expression AST:
//! ```text
//! ["+", "ex:friend"]             OneOrMore(Iri(...))
//! ["/", "ex:a", "ex:b"]          Sequence([Iri(a), Iri(b)])
//! ["+", ["|", "ex:a", "ex:b"]]   OneOrMore(Alternative([Iri(a), Iri(b)]))
//! ["^", "ex:parent"]             Inverse(Iri(...))
//! ```

use super::ast::UnresolvedPathExpr;
use super::error::{ParseError, Result};
use super::policy::JsonLdParseCtx;
use fluree_vocab::rdf;
use serde_json::Value as JsonValue;
use std::sync::Arc;

// ============================================================================
// String form parser (SPARQL 1.1 property path syntax)
// ============================================================================

/// Parse a property path expression from a SPARQL-syntax string.
///
/// The grammar follows SPARQL 1.1 operator precedence:
/// ```text
/// PathExpr    ::= PathSeq ( '|' PathSeq )*
/// PathSeq     ::= PathEltInv ( '/' PathEltInv )*
/// PathEltInv  ::= '^' PathElt | PathElt
/// PathElt     ::= PathPrimary ( '+' | '*' | '?' )?
/// PathPrimary ::= FullIRI | PrefixedName | 'a' | '(' PathExpr ')'
/// ```
///
/// Compact IRIs are expanded using the provided context.
pub fn parse_path_string(input: &str, ctx: &JsonLdParseCtx) -> Result<UnresolvedPathExpr> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(ParseError::InvalidContext(
            "@path string must not be empty".to_string(),
        ));
    }
    let mut pos = 0;
    let result = parse_alternative(trimmed, &mut pos, ctx)?;
    skip_ws(trimmed, &mut pos);
    if pos < trimmed.len() {
        return Err(ParseError::InvalidContext(format!(
            "@path: unexpected character '{}' at position {}",
            &trimmed[pos..=pos],
            pos,
        )));
    }
    Ok(result)
}

/// Lowest precedence: alternative (`|`)
fn parse_alternative(
    input: &str,
    pos: &mut usize,
    ctx: &JsonLdParseCtx,
) -> Result<UnresolvedPathExpr> {
    let mut parts = vec![parse_sequence(input, pos, ctx)?];
    loop {
        skip_ws(input, pos);
        if peek(input, *pos) == Some('|') {
            *pos += 1;
            parts.push(parse_sequence(input, pos, ctx)?);
        } else {
            break;
        }
    }
    if parts.len() == 1 {
        Ok(parts.pop().unwrap())
    } else {
        Ok(UnresolvedPathExpr::Alternative(parts))
    }
}

/// Sequence (`/`)
fn parse_sequence(
    input: &str,
    pos: &mut usize,
    ctx: &JsonLdParseCtx,
) -> Result<UnresolvedPathExpr> {
    let mut parts = vec![parse_elt_or_inverse(input, pos, ctx)?];
    loop {
        skip_ws(input, pos);
        if peek(input, *pos) == Some('/') {
            *pos += 1;
            parts.push(parse_elt_or_inverse(input, pos, ctx)?);
        } else {
            break;
        }
    }
    if parts.len() == 1 {
        Ok(parts.pop().unwrap())
    } else {
        Ok(UnresolvedPathExpr::Sequence(parts))
    }
}

/// Inverse (`^`) or plain element
fn parse_elt_or_inverse(
    input: &str,
    pos: &mut usize,
    ctx: &JsonLdParseCtx,
) -> Result<UnresolvedPathExpr> {
    skip_ws(input, pos);
    if peek(input, *pos) == Some('^') {
        *pos += 1;
        let inner = parse_elt(input, pos, ctx)?;
        Ok(UnresolvedPathExpr::Inverse(Box::new(inner)))
    } else {
        parse_elt(input, pos, ctx)
    }
}

/// Element with optional postfix modifier (`+`, `*`, `?`)
fn parse_elt(input: &str, pos: &mut usize, ctx: &JsonLdParseCtx) -> Result<UnresolvedPathExpr> {
    let primary = parse_primary(input, pos, ctx)?;
    // Allow optional whitespace before modifier (SPARQL tokenization allows it)
    skip_ws(input, pos);
    match peek(input, *pos) {
        Some('+') => {
            *pos += 1;
            Ok(UnresolvedPathExpr::OneOrMore(Box::new(primary)))
        }
        Some('*') => {
            *pos += 1;
            Ok(UnresolvedPathExpr::ZeroOrMore(Box::new(primary)))
        }
        Some('?') => {
            *pos += 1;
            Ok(UnresolvedPathExpr::ZeroOrOne(Box::new(primary)))
        }
        _ => Ok(primary),
    }
}

/// Primary: full IRI, prefixed name, `a`, or parenthesized group
fn parse_primary(input: &str, pos: &mut usize, ctx: &JsonLdParseCtx) -> Result<UnresolvedPathExpr> {
    skip_ws(input, pos);
    if *pos >= input.len() {
        return Err(ParseError::InvalidContext(
            "@path: unexpected end of expression".to_string(),
        ));
    }

    let ch = input.as_bytes()[*pos] as char;

    if ch == '(' {
        // Grouped expression
        *pos += 1; // skip '('
        let expr = parse_alternative(input, pos, ctx)?;
        skip_ws(input, pos);
        if peek(input, *pos) != Some(')') {
            return Err(ParseError::InvalidContext(format!(
                "@path: expected ')' at position {}, found '{}'",
                *pos,
                input.get(*pos..*pos + 1).unwrap_or("EOF"),
            )));
        }
        *pos += 1; // skip ')'
        Ok(expr)
    } else if ch == '<' {
        // Full IRI: <http://example.org/foo>
        let iri = parse_full_iri(input, pos)?;
        Ok(UnresolvedPathExpr::Iri(Arc::from(iri.as_str())))
    } else {
        // Prefixed name or 'a' keyword
        let name = parse_pname(input, pos)?;
        if name == "a" {
            Ok(UnresolvedPathExpr::Iri(Arc::from(rdf::TYPE)))
        } else {
            let expanded = ctx.expand_iri(&name)?;
            Ok(UnresolvedPathExpr::Iri(Arc::from(expanded.as_str())))
        }
    }
}

/// Parse a full IRI wrapped in angle brackets: `<http://example.org/foo>`
fn parse_full_iri(input: &str, pos: &mut usize) -> Result<String> {
    debug_assert_eq!(input.as_bytes()[*pos] as char, '<');
    *pos += 1; // skip '<'
    let start = *pos;
    while *pos < input.len() && input.as_bytes()[*pos] != b'>' {
        *pos += 1;
    }
    if *pos >= input.len() {
        return Err(ParseError::InvalidContext(format!(
            "@path: unterminated IRI starting at position {}",
            start - 1,
        )));
    }
    let iri = &input[start..*pos];
    *pos += 1; // skip '>'
    Ok(iri.to_string())
}

/// Parse a prefixed name like `ex:foo` or a bare keyword like `a`.
///
/// Reads until hitting an operator char, whitespace, or end of input.
/// The operator chars are: `/`, `|`, `*`, `+`, `?`, `^`, `(`, `)`, `<`
fn parse_pname(input: &str, pos: &mut usize) -> Result<String> {
    let start = *pos;
    while *pos < input.len() {
        let ch = input.as_bytes()[*pos] as char;
        if is_operator_or_delim(ch) || ch.is_ascii_whitespace() {
            break;
        }
        *pos += 1;
    }
    if *pos == start {
        return Err(ParseError::InvalidContext(format!(
            "@path: expected IRI at position {start}",
        )));
    }
    Ok(input[start..*pos].to_string())
}

/// Characters that terminate an IRI token in path syntax
fn is_operator_or_delim(ch: char) -> bool {
    matches!(
        ch,
        '/' | '|' | '*' | '+' | '?' | '^' | '(' | ')' | '<' | '>'
    )
}

fn skip_ws(input: &str, pos: &mut usize) {
    while *pos < input.len() && input.as_bytes()[*pos].is_ascii_whitespace() {
        *pos += 1;
    }
}

fn peek(input: &str, pos: usize) -> Option<char> {
    input.as_bytes().get(pos).map(|&b| b as char)
}

// ============================================================================
// Array form parser (S-expression AST)
// ============================================================================

/// Parse a property path expression from an S-expression JSON array.
///
/// The first element is an operator string, remaining elements are operands.
///
/// Unary operators (`+`, `*`, `?`, `^`): exactly 2 elements total.
/// N-ary operators (`/`, `|`): 3 or more elements total.
///
/// Operands can be:
/// - String: expanded as a compact IRI via context
/// - Array: recursively parsed as a nested S-expression
pub fn parse_path_array(arr: &[JsonValue], ctx: &JsonLdParseCtx) -> Result<UnresolvedPathExpr> {
    if arr.is_empty() {
        return Err(ParseError::InvalidContext(
            "@path array must not be empty".to_string(),
        ));
    }

    let op = arr[0].as_str().ok_or_else(|| {
        ParseError::InvalidContext(
            "@path array: first element must be an operator string".to_string(),
        )
    })?;

    match op {
        "+" | "*" | "?" | "^" => {
            // Unary operators: exactly 2 elements
            if arr.len() != 2 {
                return Err(ParseError::InvalidContext(format!(
                    "@path array: operator '{}' requires exactly 1 operand, got {}",
                    op,
                    arr.len() - 1,
                )));
            }
            let operand = parse_array_operand(&arr[1], ctx)?;
            match op {
                "+" => Ok(UnresolvedPathExpr::OneOrMore(Box::new(operand))),
                "*" => Ok(UnresolvedPathExpr::ZeroOrMore(Box::new(operand))),
                "?" => Ok(UnresolvedPathExpr::ZeroOrOne(Box::new(operand))),
                "^" => Ok(UnresolvedPathExpr::Inverse(Box::new(operand))),
                _ => unreachable!(),
            }
        }
        "/" | "|" => {
            // N-ary operators: 3+ elements (operator + 2+ operands)
            if arr.len() < 3 {
                return Err(ParseError::InvalidContext(format!(
                    "@path array: operator '{}' requires at least 2 operands, got {}",
                    op,
                    arr.len() - 1,
                )));
            }
            let operands: Vec<UnresolvedPathExpr> = arr[1..]
                .iter()
                .map(|v| parse_array_operand(v, ctx))
                .collect::<Result<_>>()?;
            match op {
                "/" => Ok(UnresolvedPathExpr::Sequence(operands)),
                "|" => Ok(UnresolvedPathExpr::Alternative(operands)),
                _ => unreachable!(),
            }
        }
        _ => Err(ParseError::InvalidContext(format!(
            "@path array: unknown operator '{op}'; expected one of +, *, ?, ^, /, |",
        ))),
    }
}

/// Parse a single operand in an S-expression array.
///
/// - String: expanded as a compact IRI (`"a"` becomes rdf:type)
/// - Array: recursively parsed
fn parse_array_operand(val: &JsonValue, ctx: &JsonLdParseCtx) -> Result<UnresolvedPathExpr> {
    match val {
        JsonValue::String(s) => {
            if s == "a" {
                Ok(UnresolvedPathExpr::Iri(Arc::from(rdf::TYPE)))
            } else {
                let expanded = ctx.expand_iri(s)?;
                Ok(UnresolvedPathExpr::Iri(Arc::from(expanded.as_str())))
            }
        }
        JsonValue::Array(arr) => parse_path_array(arr, ctx),
        _ => Err(ParseError::InvalidContext(
            "@path array: operands must be strings (IRIs) or arrays (nested expressions)"
                .to_string(),
        )),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::policy::JsonLdParsePolicy;
    use super::super::PathAliasMap;
    use super::*;
    use fluree_graph_json_ld::parse_context;
    use serde_json::json;

    fn test_ctx() -> JsonLdParseCtx {
        let context = parse_context(&json!({
            "ex": "http://example.org/",
            "schema": "http://schema.org/"
        }))
        .unwrap();
        JsonLdParseCtx::new(context, PathAliasMap::new(), JsonLdParsePolicy::default())
    }

    // -- String parser tests --

    #[test]
    fn string_simple_plus() {
        let ctx = test_ctx();
        let expr = parse_path_string("ex:friend+", &ctx).unwrap();
        assert_eq!(
            expr,
            UnresolvedPathExpr::OneOrMore(Box::new(UnresolvedPathExpr::Iri(Arc::from(
                "http://example.org/friend"
            ))))
        );
    }

    #[test]
    fn string_simple_star() {
        let ctx = test_ctx();
        let expr = parse_path_string("ex:friend*", &ctx).unwrap();
        assert_eq!(
            expr,
            UnresolvedPathExpr::ZeroOrMore(Box::new(UnresolvedPathExpr::Iri(Arc::from(
                "http://example.org/friend"
            ))))
        );
    }

    #[test]
    fn string_simple_question() {
        let ctx = test_ctx();
        let expr = parse_path_string("ex:friend?", &ctx).unwrap();
        assert_eq!(
            expr,
            UnresolvedPathExpr::ZeroOrOne(Box::new(UnresolvedPathExpr::Iri(Arc::from(
                "http://example.org/friend"
            ))))
        );
    }

    #[test]
    fn string_inverse() {
        let ctx = test_ctx();
        let expr = parse_path_string("^ex:parent", &ctx).unwrap();
        assert_eq!(
            expr,
            UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::Iri(Arc::from(
                "http://example.org/parent"
            ))))
        );
    }

    #[test]
    fn string_sequence() {
        let ctx = test_ctx();
        let expr = parse_path_string("ex:friend/ex:name", &ctx).unwrap();
        assert_eq!(
            expr,
            UnresolvedPathExpr::Sequence(vec![
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/friend")),
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/name")),
            ])
        );
    }

    #[test]
    fn string_alternative() {
        let ctx = test_ctx();
        let expr = parse_path_string("ex:friend|ex:colleague", &ctx).unwrap();
        assert_eq!(
            expr,
            UnresolvedPathExpr::Alternative(vec![
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/friend")),
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/colleague")),
            ])
        );
    }

    #[test]
    fn string_grouped_alternative_plus() {
        let ctx = test_ctx();
        let expr = parse_path_string("(ex:a|ex:b)+", &ctx).unwrap();
        assert_eq!(
            expr,
            UnresolvedPathExpr::OneOrMore(Box::new(UnresolvedPathExpr::Alternative(vec![
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/a")),
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/b")),
            ])))
        );
    }

    #[test]
    fn string_full_iri() {
        let ctx = test_ctx();
        let expr = parse_path_string("<http://example.org/foo>+", &ctx).unwrap();
        assert_eq!(
            expr,
            UnresolvedPathExpr::OneOrMore(Box::new(UnresolvedPathExpr::Iri(Arc::from(
                "http://example.org/foo"
            ))))
        );
    }

    #[test]
    fn string_a_keyword() {
        let ctx = test_ctx();
        let expr = parse_path_string("a+", &ctx).unwrap();
        assert_eq!(
            expr,
            UnresolvedPathExpr::OneOrMore(Box::new(UnresolvedPathExpr::Iri(Arc::from(rdf::TYPE))))
        );
    }

    #[test]
    fn string_complex_path() {
        // ^ex:parent/ex:child+
        let ctx = test_ctx();
        let expr = parse_path_string("^ex:parent/ex:child+", &ctx).unwrap();
        assert_eq!(
            expr,
            UnresolvedPathExpr::Sequence(vec![
                UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::Iri(Arc::from(
                    "http://example.org/parent"
                )))),
                UnresolvedPathExpr::OneOrMore(Box::new(UnresolvedPathExpr::Iri(Arc::from(
                    "http://example.org/child"
                )))),
            ])
        );
    }

    #[test]
    fn string_precedence_seq_over_alt() {
        // ex:a/ex:b|ex:c => Alternative([Sequence([a, b]), c])
        let ctx = test_ctx();
        let expr = parse_path_string("ex:a/ex:b|ex:c", &ctx).unwrap();
        assert_eq!(
            expr,
            UnresolvedPathExpr::Alternative(vec![
                UnresolvedPathExpr::Sequence(vec![
                    UnresolvedPathExpr::Iri(Arc::from("http://example.org/a")),
                    UnresolvedPathExpr::Iri(Arc::from("http://example.org/b")),
                ]),
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/c")),
            ])
        );
    }

    #[test]
    fn string_whitespace_tolerance() {
        let ctx = test_ctx();
        let expr = parse_path_string("  ex:a / ex:b  ", &ctx).unwrap();
        assert_eq!(
            expr,
            UnresolvedPathExpr::Sequence(vec![
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/a")),
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/b")),
            ])
        );
    }

    #[test]
    fn string_whitespace_before_modifier() {
        let ctx = test_ctx();
        let expr = parse_path_string("ex:knows +", &ctx).unwrap();
        assert_eq!(
            expr,
            UnresolvedPathExpr::OneOrMore(Box::new(UnresolvedPathExpr::Iri(Arc::from(
                "http://example.org/knows"
            ))))
        );
    }

    #[test]
    fn string_three_part_sequence() {
        let ctx = test_ctx();
        let expr = parse_path_string("ex:a/ex:b/ex:c", &ctx).unwrap();
        assert_eq!(
            expr,
            UnresolvedPathExpr::Sequence(vec![
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/a")),
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/b")),
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/c")),
            ])
        );
    }

    #[test]
    fn string_empty_error() {
        let ctx = test_ctx();
        assert!(parse_path_string("", &ctx).is_err());
    }

    #[test]
    fn string_unmatched_paren_error() {
        let ctx = test_ctx();
        assert!(parse_path_string("(ex:a|ex:b", &ctx).is_err());
    }

    #[test]
    fn string_trailing_garbage_error() {
        let ctx = test_ctx();
        assert!(parse_path_string("ex:a ex:b", &ctx).is_err());
    }

    // -- Array parser tests --

    #[test]
    fn array_plus() {
        let ctx = test_ctx();
        let arr = json!(["+", "ex:knows"]);
        let expr = parse_path_array(arr.as_array().unwrap(), &ctx).unwrap();
        assert_eq!(
            expr,
            UnresolvedPathExpr::OneOrMore(Box::new(UnresolvedPathExpr::Iri(Arc::from(
                "http://example.org/knows"
            ))))
        );
    }

    #[test]
    fn array_star() {
        let ctx = test_ctx();
        let arr = json!(["*", "ex:knows"]);
        let expr = parse_path_array(arr.as_array().unwrap(), &ctx).unwrap();
        assert_eq!(
            expr,
            UnresolvedPathExpr::ZeroOrMore(Box::new(UnresolvedPathExpr::Iri(Arc::from(
                "http://example.org/knows"
            ))))
        );
    }

    #[test]
    fn array_inverse() {
        let ctx = test_ctx();
        let arr = json!(["^", "ex:parent"]);
        let expr = parse_path_array(arr.as_array().unwrap(), &ctx).unwrap();
        assert_eq!(
            expr,
            UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::Iri(Arc::from(
                "http://example.org/parent"
            ))))
        );
    }

    #[test]
    fn array_sequence() {
        let ctx = test_ctx();
        let arr = json!(["/", "ex:friend", "ex:name"]);
        let expr = parse_path_array(arr.as_array().unwrap(), &ctx).unwrap();
        assert_eq!(
            expr,
            UnresolvedPathExpr::Sequence(vec![
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/friend")),
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/name")),
            ])
        );
    }

    #[test]
    fn array_alternative() {
        let ctx = test_ctx();
        let arr = json!(["|", "ex:friend", "ex:colleague"]);
        let expr = parse_path_array(arr.as_array().unwrap(), &ctx).unwrap();
        assert_eq!(
            expr,
            UnresolvedPathExpr::Alternative(vec![
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/friend")),
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/colleague")),
            ])
        );
    }

    #[test]
    fn array_nested() {
        let ctx = test_ctx();
        let arr = json!(["/", "ex:a", ["+", "ex:b"]]);
        let expr = parse_path_array(arr.as_array().unwrap(), &ctx).unwrap();
        assert_eq!(
            expr,
            UnresolvedPathExpr::Sequence(vec![
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/a")),
                UnresolvedPathExpr::OneOrMore(Box::new(UnresolvedPathExpr::Iri(Arc::from(
                    "http://example.org/b"
                )))),
            ])
        );
    }

    #[test]
    fn array_multi_operand_sequence() {
        let ctx = test_ctx();
        let arr = json!(["/", "ex:a", "ex:b", "ex:c"]);
        let expr = parse_path_array(arr.as_array().unwrap(), &ctx).unwrap();
        assert_eq!(
            expr,
            UnresolvedPathExpr::Sequence(vec![
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/a")),
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/b")),
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/c")),
            ])
        );
    }

    #[test]
    fn array_a_keyword() {
        let ctx = test_ctx();
        let arr = json!(["+", "a"]);
        let expr = parse_path_array(arr.as_array().unwrap(), &ctx).unwrap();
        assert_eq!(
            expr,
            UnresolvedPathExpr::OneOrMore(Box::new(UnresolvedPathExpr::Iri(Arc::from(rdf::TYPE))))
        );
    }

    #[test]
    fn array_empty_error() {
        let ctx = test_ctx();
        let arr: Vec<JsonValue> = vec![];
        assert!(parse_path_array(&arr, &ctx).is_err());
    }

    #[test]
    fn array_unknown_operator_error() {
        let ctx = test_ctx();
        let arr = json!(["!", "ex:a"]);
        assert!(parse_path_array(arr.as_array().unwrap(), &ctx).is_err());
    }

    #[test]
    fn array_unary_arity_error() {
        let ctx = test_ctx();
        // + needs exactly 1 operand
        let arr = json!(["+", "ex:a", "ex:b"]);
        assert!(parse_path_array(arr.as_array().unwrap(), &ctx).is_err());
    }

    #[test]
    fn array_binary_arity_error() {
        let ctx = test_ctx();
        // / needs at least 2 operands
        let arr = json!(["/", "ex:a"]);
        assert!(parse_path_array(arr.as_array().unwrap(), &ctx).is_err());
    }

    #[test]
    fn array_invalid_operand_type_error() {
        let ctx = test_ctx();
        let arr = json!(["+", 42]);
        assert!(parse_path_array(arr.as_array().unwrap(), &ctx).is_err());
    }
}
