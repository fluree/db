//! SPARQL expression parsing.
//!
//! This module implements expression parsing with proper operator precedence.
//!
//! ## SPARQL Expression Precedence (lowest to highest)
//!
//! 1. `||` (OR)
//! 2. `&&` (AND)
//! 3. `=`, `!=`, `<`, `<=`, `>`, `>=`, `IN`, `NOT IN`
//! 4. `+`, `-` (additive)
//! 5. `*`, `/` (multiplicative)
//! 6. `+`, `-`, `!` (unary)
//! 7. Primary expressions (literals, variables, function calls, parenthesized)

use crate::ast::expr::{AggregateFunction, BinaryOp, Expression, FunctionName, UnaryOp};
use crate::ast::pattern::GraphPattern;
use crate::ast::term::{Iri, IriValue, Literal, Var};
use crate::lex::TokenKind;
use crate::parse::stream::TokenStream;
use crate::span::SourceSpan;
use std::sync::Arc;

/// Parse a SPARQL expression.
///
/// This is the main entry point for expression parsing.
pub fn parse_expression(tokens: &mut TokenStream) -> Result<Expression, String> {
    parse_or_expr(tokens)
}

/// Parse an OR expression: expr1 || expr2
fn parse_or_expr(tokens: &mut TokenStream) -> Result<Expression, String> {
    let start = tokens.current_span().start;
    let mut left = parse_and_expr(tokens)?;

    while tokens.check(&TokenKind::Or) {
        tokens.advance(); // consume ||
        let right = parse_and_expr(tokens)?;
        let span = SourceSpan::new(start, tokens.previous_span().end);
        left = Expression::binary(BinaryOp::Or, left, right, span);
    }

    Ok(left)
}

/// Parse an AND expression: expr1 && expr2
fn parse_and_expr(tokens: &mut TokenStream) -> Result<Expression, String> {
    let start = tokens.current_span().start;
    let mut left = parse_relational_expr(tokens)?;

    while tokens.check(&TokenKind::And) {
        tokens.advance(); // consume &&
        let right = parse_relational_expr(tokens)?;
        let span = SourceSpan::new(start, tokens.previous_span().end);
        left = Expression::binary(BinaryOp::And, left, right, span);
    }

    Ok(left)
}

/// Parse a relational expression: =, !=, <, <=, >, >=, IN, NOT IN
fn parse_relational_expr(tokens: &mut TokenStream) -> Result<Expression, String> {
    let start = tokens.current_span().start;
    let mut left = parse_additive_expr(tokens)?;

    loop {
        let op = if tokens.check(&TokenKind::Eq) {
            tokens.advance();
            Some(BinaryOp::Eq)
        } else if tokens.check(&TokenKind::Ne) {
            tokens.advance();
            Some(BinaryOp::Ne)
        } else if tokens.check(&TokenKind::Lt) {
            tokens.advance();
            Some(BinaryOp::Lt)
        } else if tokens.check(&TokenKind::Le) {
            tokens.advance();
            Some(BinaryOp::Le)
        } else if tokens.check(&TokenKind::Gt) {
            tokens.advance();
            Some(BinaryOp::Gt)
        } else if tokens.check(&TokenKind::Ge) {
            tokens.advance();
            Some(BinaryOp::Ge)
        } else {
            None
        };

        if let Some(op) = op {
            let right = parse_additive_expr(tokens)?;
            let span = SourceSpan::new(start, tokens.previous_span().end);
            left = Expression::binary(op, left, right, span);
        } else if tokens.check_keyword(TokenKind::KwIn) {
            // IN (expr, expr, ...)
            tokens.advance(); // consume IN
            let list = parse_expression_list(tokens)?;
            let span = SourceSpan::new(start, tokens.previous_span().end);
            left = Expression::In {
                expr: Box::new(left),
                list,
                negated: false,
                span,
            };
        } else if tokens.check_keyword(TokenKind::KwNot) {
            // Check for NOT IN
            let saved_pos = tokens.position();
            tokens.advance(); // consume NOT
            if tokens.check_keyword(TokenKind::KwIn) {
                tokens.advance(); // consume IN
                let list = parse_expression_list(tokens)?;
                let span = SourceSpan::new(start, tokens.previous_span().end);
                left = Expression::In {
                    expr: Box::new(left),
                    list,
                    negated: true,
                    span,
                };
            } else {
                // Backtrack - this NOT belongs to something else
                tokens.restore(saved_pos);
                break;
            }
        } else {
            break;
        }
    }

    Ok(left)
}

/// Parse an additive expression: +, -
fn parse_additive_expr(tokens: &mut TokenStream) -> Result<Expression, String> {
    let start = tokens.current_span().start;
    let mut left = parse_multiplicative_expr(tokens)?;

    loop {
        let op = if tokens.check(&TokenKind::Plus) {
            tokens.advance();
            Some(BinaryOp::Add)
        } else if tokens.check(&TokenKind::Minus) {
            tokens.advance();
            Some(BinaryOp::Sub)
        } else {
            None
        };

        if let Some(op) = op {
            let right = parse_multiplicative_expr(tokens)?;
            let span = SourceSpan::new(start, tokens.previous_span().end);
            left = Expression::binary(op, left, right, span);
        } else {
            break;
        }
    }

    Ok(left)
}

/// Parse a multiplicative expression: *, /
fn parse_multiplicative_expr(tokens: &mut TokenStream) -> Result<Expression, String> {
    let start = tokens.current_span().start;
    let mut left = parse_unary_expr(tokens)?;

    loop {
        let op = if tokens.check(&TokenKind::Star) {
            tokens.advance();
            Some(BinaryOp::Mul)
        } else if tokens.check(&TokenKind::Slash) {
            tokens.advance();
            Some(BinaryOp::Div)
        } else {
            None
        };

        if let Some(op) = op {
            let right = parse_unary_expr(tokens)?;
            let span = SourceSpan::new(start, tokens.previous_span().end);
            left = Expression::binary(op, left, right, span);
        } else {
            break;
        }
    }

    Ok(left)
}

/// Parse a unary expression: !, +, -
fn parse_unary_expr(tokens: &mut TokenStream) -> Result<Expression, String> {
    let start = tokens.current_span().start;

    let op = if tokens.check(&TokenKind::Bang) {
        tokens.advance();
        Some(UnaryOp::Not)
    } else if tokens.check(&TokenKind::Plus) {
        tokens.advance();
        Some(UnaryOp::Pos)
    } else if tokens.check(&TokenKind::Minus) {
        tokens.advance();
        Some(UnaryOp::Neg)
    } else {
        None
    };

    if let Some(op) = op {
        let operand = parse_unary_expr(tokens)?;
        let span = SourceSpan::new(start, tokens.previous_span().end);
        Ok(Expression::unary(op, operand, span))
    } else {
        parse_primary_expr(tokens)
    }
}

/// Parse a primary expression.
fn parse_primary_expr(tokens: &mut TokenStream) -> Result<Expression, String> {
    let start = tokens.current_span().start;

    // Parenthesized expression
    if tokens.check(&TokenKind::LParen) {
        tokens.advance(); // consume (
        let inner = parse_expression(tokens)?;
        if !tokens.check(&TokenKind::RParen) {
            return Err(format!(
                "Expected ')' at position {}",
                tokens.current_span().start
            ));
        }
        tokens.advance(); // consume )
        let span = SourceSpan::new(start, tokens.previous_span().end);
        return Ok(Expression::Bracketed {
            inner: Box::new(inner),
            span,
        });
    }

    // Variable
    if let Some((name, var_span)) = tokens.consume_var() {
        return Ok(Expression::Var(Var::new(name, var_span)));
    }

    // Literals
    if let Some(expr) = try_parse_literal(tokens)? {
        return Ok(expr);
    }

    // Keywords: EXISTS, NOT EXISTS, BOUND, etc.
    if let Some(expr) = try_parse_keyword_expr(tokens)? {
        return Ok(expr);
    }

    // Full IRI - could be function call or just an IRI
    if let Some((iri_str, iri_span)) = tokens.consume_iri() {
        let iri = Iri::full(iri_str, iri_span);
        if tokens.check(&TokenKind::LParen) {
            // Function call with IRI
            return parse_function_call_with_iri(tokens, iri, start);
        }
        return Ok(Expression::iri(iri));
    }

    // Prefixed name - could be function call or just IRI
    if let Some((prefix, local, pn_span)) = tokens.consume_prefixed_name() {
        let iri = Iri {
            value: IriValue::Prefixed { prefix, local },
            span: pn_span,
        };
        if tokens.check(&TokenKind::LParen) {
            // Function call with prefixed IRI
            return parse_function_call_with_iri(tokens, iri, start);
        }
        return Ok(Expression::iri(iri));
    }

    // PrefixedNameNs (just namespace, no local part) - like "ex:"
    if let Some((prefix, ns_span)) = tokens.consume_prefixed_name_ns() {
        let iri = Iri {
            value: IriValue::Prefixed {
                prefix,
                local: Arc::from(""),
            },
            span: ns_span,
        };
        if tokens.check(&TokenKind::LParen) {
            return parse_function_call_with_iri(tokens, iri, start);
        }
        return Ok(Expression::iri(iri));
    }

    Err(format!(
        "Expected expression at position {}",
        tokens.current_span().start
    ))
}

/// Try to parse a literal expression.
fn try_parse_literal(tokens: &mut TokenStream) -> Result<Option<Expression>, String> {
    // Integer literal
    if let Some((value, span)) = tokens.consume_integer() {
        return Ok(Some(Expression::Literal(Literal::integer(value, span))));
    }

    // Decimal literal
    if let Some((value, span)) = tokens.consume_decimal() {
        return Ok(Some(Expression::Literal(Literal::decimal(value, span))));
    }

    // Double literal
    if let Some((value, span)) = tokens.consume_double() {
        return Ok(Some(Expression::Literal(Literal::double(value, span))));
    }

    // String literal (with possible language tag or datatype)
    if let Some((value, span)) = tokens.consume_string() {
        // Check for language tag
        if let Some((lang, lang_span)) = tokens.consume_lang_tag() {
            let full_span = SourceSpan::new(span.start, lang_span.end);
            return Ok(Some(Expression::Literal(Literal::lang_string(
                value, lang, full_span,
            ))));
        }

        // Check for datatype (^^)
        if tokens.check(&TokenKind::DoubleCaret) {
            tokens.advance(); // consume ^^

            // Parse datatype IRI
            if let Some((dt_str, dt_span)) = tokens.consume_iri() {
                let dt_iri = Iri::full(dt_str, dt_span);
                let full_span = SourceSpan::new(span.start, dt_span.end);
                return Ok(Some(Expression::Literal(Literal::typed(
                    value, dt_iri, full_span,
                ))));
            } else if let Some((prefix, local, dt_span)) = tokens.consume_prefixed_name() {
                let dt_iri = Iri {
                    value: IriValue::Prefixed { prefix, local },
                    span: dt_span,
                };
                let full_span = SourceSpan::new(span.start, dt_span.end);
                return Ok(Some(Expression::Literal(Literal::typed(
                    value, dt_iri, full_span,
                ))));
            }
            return Err("Expected datatype IRI after '^^'".to_string());
        }

        // Simple string literal
        return Ok(Some(Expression::Literal(Literal::string(value, span))));
    }

    // Boolean literal
    if tokens.check_keyword(TokenKind::KwTrue) {
        let span = tokens.current_span();
        tokens.advance();
        return Ok(Some(Expression::Literal(Literal::boolean(true, span))));
    }
    if tokens.check_keyword(TokenKind::KwFalse) {
        let span = tokens.current_span();
        tokens.advance();
        return Ok(Some(Expression::Literal(Literal::boolean(false, span))));
    }

    Ok(None)
}

/// Try to parse a keyword-based expression (EXISTS, NOT EXISTS, BOUND, IF, etc.)
fn try_parse_keyword_expr(tokens: &mut TokenStream) -> Result<Option<Expression>, String> {
    let start = tokens.current_span().start;

    // EXISTS { pattern }
    if tokens.check_keyword(TokenKind::KwExists) {
        tokens.advance();
        let pattern = parse_group_pattern_for_exists(tokens)?;
        let span = SourceSpan::new(start, tokens.previous_span().end);
        return Ok(Some(Expression::Exists {
            pattern: Box::new(pattern),
            span,
        }));
    }

    // NOT EXISTS { pattern }
    if tokens.check_keyword(TokenKind::KwNot) {
        let saved_pos = tokens.position();
        tokens.advance();
        if tokens.check_keyword(TokenKind::KwExists) {
            tokens.advance();
            let pattern = parse_group_pattern_for_exists(tokens)?;
            let span = SourceSpan::new(start, tokens.previous_span().end);
            return Ok(Some(Expression::NotExists {
                pattern: Box::new(pattern),
                span,
            }));
        }
        // Backtrack - NOT is unary operator, handled elsewhere
        tokens.restore(saved_pos);
        return Ok(None);
    }

    // BOUND(?var)
    if tokens.check_keyword(TokenKind::KwBound) {
        tokens.advance();
        if !tokens.check(&TokenKind::LParen) {
            return Err("Expected '(' after BOUND".to_string());
        }
        tokens.advance();
        if let Some((name, var_span)) = tokens.consume_var() {
            let var = Var::new(name, var_span);
            if !tokens.check(&TokenKind::RParen) {
                return Err("Expected ')' after BOUND(?var)".to_string());
            }
            tokens.advance();
            let span = SourceSpan::new(start, tokens.previous_span().end);
            return Ok(Some(Expression::FunctionCall {
                name: FunctionName::Bound,
                args: vec![Expression::Var(var)],
                distinct: false,
                span,
            }));
        }
        return Err("Expected variable in BOUND()".to_string());
    }

    // IF(cond, then, else)
    if tokens.check_keyword(TokenKind::KwIf) {
        tokens.advance();
        if !tokens.check(&TokenKind::LParen) {
            return Err("Expected '(' after IF".to_string());
        }
        tokens.advance();
        let condition = parse_expression(tokens)?;
        if !tokens.check(&TokenKind::Comma) {
            return Err("Expected ',' in IF expression".to_string());
        }
        tokens.advance();
        let then_expr = parse_expression(tokens)?;
        if !tokens.check(&TokenKind::Comma) {
            return Err("Expected ',' in IF expression".to_string());
        }
        tokens.advance();
        let else_expr = parse_expression(tokens)?;
        if !tokens.check(&TokenKind::RParen) {
            return Err("Expected ')' after IF expression".to_string());
        }
        tokens.advance();
        let span = SourceSpan::new(start, tokens.previous_span().end);
        return Ok(Some(Expression::If {
            condition: Box::new(condition),
            then_expr: Box::new(then_expr),
            else_expr: Box::new(else_expr),
            span,
        }));
    }

    // COALESCE(expr, expr, ...)
    if tokens.check_keyword(TokenKind::KwCoalesce) {
        tokens.advance();
        let args = parse_expression_list(tokens)?;
        let span = SourceSpan::new(start, tokens.previous_span().end);
        return Ok(Some(Expression::Coalesce { args, span }));
    }

    // Aggregate functions: COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT, SAMPLE
    if let Some(agg) = check_aggregate_keyword(tokens) {
        return parse_aggregate_expr(tokens, agg, start).map(Some);
    }

    // Built-in functions that look like keywords
    if let Some(func_name) = check_builtin_function_keyword(tokens) {
        tokens.advance();
        let args = parse_expression_list(tokens)?;
        let span = SourceSpan::new(start, tokens.previous_span().end);
        return Ok(Some(Expression::FunctionCall {
            name: func_name,
            args,
            distinct: false,
            span,
        }));
    }

    Ok(None)
}

/// Check if current token is an aggregate keyword.
fn check_aggregate_keyword(tokens: &TokenStream) -> Option<AggregateFunction> {
    if tokens.check_keyword(TokenKind::KwCount) {
        Some(AggregateFunction::Count)
    } else if tokens.check_keyword(TokenKind::KwSum) {
        Some(AggregateFunction::Sum)
    } else if tokens.check_keyword(TokenKind::KwAvg) {
        Some(AggregateFunction::Avg)
    } else if tokens.check_keyword(TokenKind::KwMin) {
        Some(AggregateFunction::Min)
    } else if tokens.check_keyword(TokenKind::KwMax) {
        Some(AggregateFunction::Max)
    } else if tokens.check_keyword(TokenKind::KwGroupConcat) {
        Some(AggregateFunction::GroupConcat)
    } else if tokens.check_keyword(TokenKind::KwSample) {
        Some(AggregateFunction::Sample)
    } else {
        None
    }
}

/// Check if current token is a built-in function keyword.
fn check_builtin_function_keyword(tokens: &TokenStream) -> Option<FunctionName> {
    // Type checking
    if tokens.check_keyword(TokenKind::KwIsIri) {
        return Some(FunctionName::IsIri);
    }
    if tokens.check_keyword(TokenKind::KwIsUri) {
        return Some(FunctionName::IsUri);
    }
    if tokens.check_keyword(TokenKind::KwIsBlank) {
        return Some(FunctionName::IsBlank);
    }
    if tokens.check_keyword(TokenKind::KwIsLiteral) {
        return Some(FunctionName::IsLiteral);
    }
    if tokens.check_keyword(TokenKind::KwIsNumeric) {
        return Some(FunctionName::IsNumeric);
    }

    // Accessors
    if tokens.check_keyword(TokenKind::KwStr) {
        return Some(FunctionName::Str);
    }
    if tokens.check_keyword(TokenKind::KwLang) {
        return Some(FunctionName::Lang);
    }
    if tokens.check_keyword(TokenKind::KwDatatype) {
        return Some(FunctionName::Datatype);
    }

    // Constructors
    if tokens.check_keyword(TokenKind::KwIri) {
        return Some(FunctionName::Iri);
    }
    if tokens.check_keyword(TokenKind::KwUri) {
        return Some(FunctionName::Uri);
    }
    if tokens.check_keyword(TokenKind::KwBNode) {
        return Some(FunctionName::BNode);
    }

    // String functions
    if tokens.check_keyword(TokenKind::KwStrlen) {
        return Some(FunctionName::Strlen);
    }
    if tokens.check_keyword(TokenKind::KwSubstr) {
        return Some(FunctionName::Substr);
    }
    if tokens.check_keyword(TokenKind::KwUcase) {
        return Some(FunctionName::Ucase);
    }
    if tokens.check_keyword(TokenKind::KwLcase) {
        return Some(FunctionName::Lcase);
    }
    if tokens.check_keyword(TokenKind::KwStrStarts) {
        return Some(FunctionName::StrStarts);
    }
    if tokens.check_keyword(TokenKind::KwStrEnds) {
        return Some(FunctionName::StrEnds);
    }
    if tokens.check_keyword(TokenKind::KwContains) {
        return Some(FunctionName::Contains);
    }
    if tokens.check_keyword(TokenKind::KwStrBefore) {
        return Some(FunctionName::StrBefore);
    }
    if tokens.check_keyword(TokenKind::KwStrAfter) {
        return Some(FunctionName::StrAfter);
    }
    if tokens.check_keyword(TokenKind::KwEncodeForUri) {
        return Some(FunctionName::EncodeForUri);
    }
    if tokens.check_keyword(TokenKind::KwConcat) {
        return Some(FunctionName::Concat);
    }
    if tokens.check_keyword(TokenKind::KwLangMatches) {
        return Some(FunctionName::LangMatches);
    }
    if tokens.check_keyword(TokenKind::KwRegex) {
        return Some(FunctionName::Regex);
    }
    if tokens.check_keyword(TokenKind::KwReplace) {
        return Some(FunctionName::Replace);
    }
    if tokens.check_keyword(TokenKind::KwStrDt) {
        return Some(FunctionName::StrDt);
    }
    if tokens.check_keyword(TokenKind::KwStrLang) {
        return Some(FunctionName::StrLang);
    }

    // Numeric functions
    if tokens.check_keyword(TokenKind::KwAbs) {
        return Some(FunctionName::Abs);
    }
    if tokens.check_keyword(TokenKind::KwRound) {
        return Some(FunctionName::Round);
    }
    if tokens.check_keyword(TokenKind::KwCeil) {
        return Some(FunctionName::Ceil);
    }
    if tokens.check_keyword(TokenKind::KwFloor) {
        return Some(FunctionName::Floor);
    }
    if tokens.check_keyword(TokenKind::KwRand) {
        return Some(FunctionName::Rand);
    }

    // Date/time functions
    if tokens.check_keyword(TokenKind::KwNow) {
        return Some(FunctionName::Now);
    }
    if tokens.check_keyword(TokenKind::KwYear) {
        return Some(FunctionName::Year);
    }
    if tokens.check_keyword(TokenKind::KwMonth) {
        return Some(FunctionName::Month);
    }
    if tokens.check_keyword(TokenKind::KwDay) {
        return Some(FunctionName::Day);
    }
    if tokens.check_keyword(TokenKind::KwHours) {
        return Some(FunctionName::Hours);
    }
    if tokens.check_keyword(TokenKind::KwMinutes) {
        return Some(FunctionName::Minutes);
    }
    if tokens.check_keyword(TokenKind::KwSeconds) {
        return Some(FunctionName::Seconds);
    }
    if tokens.check_keyword(TokenKind::KwTimezone) {
        return Some(FunctionName::Timezone);
    }
    if tokens.check_keyword(TokenKind::KwTz) {
        return Some(FunctionName::Tz);
    }

    // Hash functions
    if tokens.check_keyword(TokenKind::KwMd5) {
        return Some(FunctionName::Md5);
    }
    if tokens.check_keyword(TokenKind::KwSha1) {
        return Some(FunctionName::Sha1);
    }
    if tokens.check_keyword(TokenKind::KwSha256) {
        return Some(FunctionName::Sha256);
    }
    if tokens.check_keyword(TokenKind::KwSha384) {
        return Some(FunctionName::Sha384);
    }
    if tokens.check_keyword(TokenKind::KwSha512) {
        return Some(FunctionName::Sha512);
    }

    // Other functions
    if tokens.check_keyword(TokenKind::KwSameTerm) {
        return Some(FunctionName::SameTerm);
    }
    if tokens.check_keyword(TokenKind::KwUuid) {
        return Some(FunctionName::Uuid);
    }
    if tokens.check_keyword(TokenKind::KwStrUuid) {
        return Some(FunctionName::StrUuid);
    }

    // Vector similarity functions
    if tokens.check_keyword(TokenKind::KwDotProduct) {
        return Some(FunctionName::DotProduct);
    }
    if tokens.check_keyword(TokenKind::KwCosineSimilarity) {
        return Some(FunctionName::CosineSimilarity);
    }
    if tokens.check_keyword(TokenKind::KwEuclideanDistance) {
        return Some(FunctionName::EuclideanDistance);
    }

    None
}

/// Parse an aggregate expression.
fn parse_aggregate_expr(
    tokens: &mut TokenStream,
    function: AggregateFunction,
    start: usize,
) -> Result<Expression, String> {
    tokens.advance(); // consume aggregate keyword

    if !tokens.check(&TokenKind::LParen) {
        return Err(format!("Expected '(' after {}", function.as_str()));
    }
    tokens.advance();

    // Check for DISTINCT
    let distinct = if tokens.check_keyword(TokenKind::KwDistinct) {
        tokens.advance();
        true
    } else {
        false
    };

    // Check for * (COUNT(*))
    let expr = if tokens.check(&TokenKind::Star) {
        tokens.advance();
        None
    } else {
        Some(Box::new(parse_expression(tokens)?))
    };

    // Check for separator (GROUP_CONCAT)
    let separator = if function == AggregateFunction::GroupConcat {
        if tokens.check(&TokenKind::Semicolon) {
            tokens.advance();
            // Expect SEPARATOR = "string"
            if !tokens.check_keyword(TokenKind::KwSeparator) {
                return Err("Expected SEPARATOR in GROUP_CONCAT".to_string());
            }
            tokens.advance();
            if !tokens.check(&TokenKind::Eq) {
                return Err("Expected '=' after SEPARATOR".to_string());
            }
            tokens.advance();
            if let Some((sep_value, _)) = tokens.consume_string() {
                Some(sep_value)
            } else {
                return Err("Expected string after SEPARATOR =".to_string());
            }
        } else {
            None
        }
    } else {
        None
    };

    if !tokens.check(&TokenKind::RParen) {
        return Err(format!(
            "Expected ')' after {} expression",
            function.as_str()
        ));
    }
    tokens.advance();

    let span = SourceSpan::new(start, tokens.previous_span().end);
    Ok(Expression::Aggregate {
        function,
        expr,
        distinct,
        separator,
        span,
    })
}

/// Parse a function call with an IRI name.
fn parse_function_call_with_iri(
    tokens: &mut TokenStream,
    iri: Iri,
    start: usize,
) -> Result<Expression, String> {
    let args = parse_expression_list(tokens)?;
    let span = SourceSpan::new(start, tokens.previous_span().end);
    Ok(Expression::FunctionCall {
        name: FunctionName::Extension(iri),
        args,
        distinct: false,
        span,
    })
}

/// Parse a parenthesized expression list: (expr, expr, ...)
fn parse_expression_list(tokens: &mut TokenStream) -> Result<Vec<Expression>, String> {
    // Handle NIL token - empty arg list like `NOW()` is tokenized as a single Nil token
    if tokens.check(&TokenKind::Nil) {
        tokens.advance();
        return Ok(Vec::new());
    }

    if !tokens.check(&TokenKind::LParen) {
        return Err("Expected '(' for expression list".to_string());
    }
    tokens.advance();

    let mut args = Vec::new();

    if !tokens.check(&TokenKind::RParen) {
        args.push(parse_expression(tokens)?);
        while tokens.check(&TokenKind::Comma) {
            tokens.advance();
            args.push(parse_expression(tokens)?);
        }
    }

    if !tokens.check(&TokenKind::RParen) {
        return Err("Expected ')' after expression list".to_string());
    }
    tokens.advance();

    Ok(args)
}

/// Parse a group graph pattern for EXISTS/NOT EXISTS.
///
/// This is a forward reference to the pattern parser.
fn parse_group_pattern_for_exists(tokens: &mut TokenStream) -> Result<GraphPattern, String> {
    super::query::parse_group_graph_pattern(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::term::LiteralValue;
    use crate::lex::tokenize;

    fn parse_expr_str(input: &str) -> Result<Expression, String> {
        let tokens = tokenize(input);
        let mut stream = TokenStream::new(tokens);
        parse_expression(&mut stream)
    }

    #[test]
    fn test_simple_variable() {
        let expr = parse_expr_str("?x").unwrap();
        assert!(matches!(expr, Expression::Var(_)));
    }

    #[test]
    fn test_integer_literal() {
        let expr = parse_expr_str("42").unwrap();
        match expr {
            Expression::Literal(lit) => {
                assert!(matches!(lit.value, LiteralValue::Integer(42)));
            }
            _ => panic!("Expected literal"),
        }
    }

    #[test]
    fn test_string_literal() {
        let expr = parse_expr_str("\"hello\"").unwrap();
        match expr {
            Expression::Literal(lit) => {
                assert!(matches!(lit.value, LiteralValue::Simple(_)));
            }
            _ => panic!("Expected literal"),
        }
    }

    #[test]
    fn test_boolean_literals() {
        let expr = parse_expr_str("true").unwrap();
        match expr {
            Expression::Literal(lit) => {
                assert!(matches!(lit.value, LiteralValue::Boolean(true)));
            }
            _ => panic!("Expected literal"),
        }
    }

    #[test]
    fn test_binary_arithmetic() {
        let expr = parse_expr_str("1 + 2").unwrap();
        match expr {
            Expression::Binary { op, .. } => {
                assert_eq!(op, BinaryOp::Add);
            }
            _ => panic!("Expected binary expression"),
        }
    }

    #[test]
    fn test_binary_comparison() {
        let expr = parse_expr_str("?x = 5").unwrap();
        match expr {
            Expression::Binary { op, .. } => {
                assert_eq!(op, BinaryOp::Eq);
            }
            _ => panic!("Expected binary expression"),
        }
    }

    #[test]
    fn test_logical_and() {
        let expr = parse_expr_str("?x > 0 && ?x < 10").unwrap();
        match expr {
            Expression::Binary { op, .. } => {
                assert_eq!(op, BinaryOp::And);
            }
            _ => panic!("Expected binary expression"),
        }
    }

    #[test]
    fn test_logical_or() {
        let expr = parse_expr_str("?x = 1 || ?x = 2").unwrap();
        match expr {
            Expression::Binary { op, .. } => {
                assert_eq!(op, BinaryOp::Or);
            }
            _ => panic!("Expected binary expression"),
        }
    }

    #[test]
    fn test_operator_precedence() {
        // 1 + 2 * 3 should parse as 1 + (2 * 3)
        let expr = parse_expr_str("1 + 2 * 3").unwrap();
        match expr {
            Expression::Binary { op, right, .. } => {
                assert_eq!(op, BinaryOp::Add);
                assert!(matches!(
                    *right,
                    Expression::Binary {
                        op: BinaryOp::Mul,
                        ..
                    }
                ));
            }
            _ => panic!("Expected binary expression"),
        }
    }

    #[test]
    fn test_unary_not() {
        let expr = parse_expr_str("!?bound").unwrap();
        match expr {
            Expression::Unary { op, .. } => {
                assert_eq!(op, UnaryOp::Not);
            }
            _ => panic!("Expected unary expression"),
        }
    }

    #[test]
    fn test_unary_negation() {
        // Note: "-5" is lexed as a single negative integer token
        // Use negation on a variable to test unary minus
        let expr = parse_expr_str("-?x").unwrap();
        match expr {
            Expression::Unary { op, .. } => {
                assert_eq!(op, UnaryOp::Neg);
            }
            _ => panic!("Expected unary expression"),
        }
    }

    #[test]
    fn test_parenthesized() {
        let expr = parse_expr_str("(1 + 2) * 3").unwrap();
        match expr {
            Expression::Binary { op, left, .. } => {
                assert_eq!(op, BinaryOp::Mul);
                assert!(matches!(*left, Expression::Bracketed { .. }));
            }
            _ => panic!("Expected binary expression"),
        }
    }

    #[test]
    fn test_bound_function() {
        let expr = parse_expr_str("BOUND(?x)").unwrap();
        match expr {
            Expression::FunctionCall { name, args, .. } => {
                assert!(matches!(name, FunctionName::Bound));
                assert_eq!(args.len(), 1);
            }
            _ => panic!("Expected function call"),
        }
    }

    #[test]
    fn test_count_star() {
        let expr = parse_expr_str("COUNT(*)").unwrap();
        match expr {
            Expression::Aggregate {
                function,
                expr,
                distinct,
                ..
            } => {
                assert_eq!(function, AggregateFunction::Count);
                assert!(expr.is_none());
                assert!(!distinct);
            }
            _ => panic!("Expected aggregate"),
        }
    }

    #[test]
    fn test_count_distinct() {
        let expr = parse_expr_str("COUNT(DISTINCT ?x)").unwrap();
        match expr {
            Expression::Aggregate {
                function, distinct, ..
            } => {
                assert_eq!(function, AggregateFunction::Count);
                assert!(distinct);
            }
            _ => panic!("Expected aggregate"),
        }
    }

    #[test]
    fn test_sum_aggregate() {
        let expr = parse_expr_str("SUM(?price)").unwrap();
        match expr {
            Expression::Aggregate { function, .. } => {
                assert_eq!(function, AggregateFunction::Sum);
            }
            _ => panic!("Expected aggregate"),
        }
    }

    #[test]
    fn test_in_expression() {
        let expr = parse_expr_str("?x IN (1, 2, 3)").unwrap();
        match expr {
            Expression::In { negated, list, .. } => {
                assert!(!negated);
                assert_eq!(list.len(), 3);
            }
            _ => panic!("Expected IN expression"),
        }
    }

    #[test]
    fn test_not_in_expression() {
        let expr = parse_expr_str("?x NOT IN (1, 2)").unwrap();
        match expr {
            Expression::In { negated, .. } => {
                assert!(negated);
            }
            _ => panic!("Expected NOT IN expression"),
        }
    }

    #[test]
    fn test_lang_tagged_literal() {
        let expr = parse_expr_str("\"hello\"@en").unwrap();
        match expr {
            Expression::Literal(lit) => match lit.value {
                LiteralValue::LangTagged { lang, .. } => {
                    assert_eq!(lang.as_ref(), "en");
                }
                _ => panic!("Expected lang-tagged literal"),
            },
            _ => panic!("Expected literal"),
        }
    }

    #[test]
    fn test_zero_arg_function_now() {
        // NOW() is tokenized as KwNow + Nil (not KwNow + LParen + RParen)
        let expr = parse_expr_str("NOW()").unwrap();
        match expr {
            Expression::FunctionCall { name, args, .. } => {
                assert!(matches!(name, FunctionName::Now));
                assert!(args.is_empty());
            }
            _ => panic!("Expected function call, got {expr:?}"),
        }
    }

    #[test]
    fn test_zero_arg_function_rand() {
        let expr = parse_expr_str("RAND()").unwrap();
        match expr {
            Expression::FunctionCall { name, args, .. } => {
                assert!(matches!(name, FunctionName::Rand));
                assert!(args.is_empty());
            }
            _ => panic!("Expected function call"),
        }
    }

    #[test]
    fn test_zero_arg_function_uuid() {
        let expr = parse_expr_str("UUID()").unwrap();
        match expr {
            Expression::FunctionCall { name, args, .. } => {
                assert!(matches!(name, FunctionName::Uuid));
                assert!(args.is_empty());
            }
            _ => panic!("Expected function call"),
        }
    }

    #[test]
    fn test_zero_arg_function_struuid() {
        let expr = parse_expr_str("STRUUID()").unwrap();
        match expr {
            Expression::FunctionCall { name, args, .. } => {
                assert!(matches!(name, FunctionName::StrUuid));
                assert!(args.is_empty());
            }
            _ => panic!("Expected function call"),
        }
    }

    #[test]
    fn test_zero_arg_function_bnode() {
        // BNODE() can be called with or without args
        let expr = parse_expr_str("BNODE()").unwrap();
        match expr {
            Expression::FunctionCall { name, args, .. } => {
                assert!(matches!(name, FunctionName::BNode));
                assert!(args.is_empty());
            }
            _ => panic!("Expected function call"),
        }
    }
}
