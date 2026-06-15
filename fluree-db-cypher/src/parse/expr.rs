//! Expression parser — Pratt-style precedence climbing.

use crate::ast::{BinOp, CaseExpr, Expr, FuncCall, Literal, MapLit, ParamRef, UnaryOp, Variable};
use crate::diag::{DiagCode, Diagnostic};
use crate::lex::TokenKind;

use super::pattern::parse_pattern;
use super::stmt::parse_ident_or_keyword;
use super::stream::TokenStream;

pub fn parse_expr(s: &mut TokenStream) -> Result<Expr, Diagnostic> {
    parse_or(s)
}

fn parse_or(s: &mut TokenStream) -> Result<Expr, Diagnostic> {
    let mut left = parse_xor(s)?;
    while s.eat(&TokenKind::Or).is_some() {
        let right = parse_xor(s)?;
        let span = left.span().union(right.span());
        left = Expr::BinOp(BinOp::Or, Box::new(left), Box::new(right), span);
    }
    Ok(left)
}

fn parse_xor(s: &mut TokenStream) -> Result<Expr, Diagnostic> {
    let left = parse_and(s)?;
    if matches!(s.peek_kind(), TokenKind::Xor) {
        return Err(s.error(
            DiagCode::DeferredFunction,
            "XOR is deferred — rewrite as `(a OR b) AND NOT (a AND b)`",
        ));
    }
    Ok(left)
}

fn parse_and(s: &mut TokenStream) -> Result<Expr, Diagnostic> {
    let mut left = parse_not(s)?;
    while s.eat(&TokenKind::And).is_some() {
        let right = parse_not(s)?;
        let span = left.span().union(right.span());
        left = Expr::BinOp(BinOp::And, Box::new(left), Box::new(right), span);
    }
    Ok(left)
}

fn parse_not(s: &mut TokenStream) -> Result<Expr, Diagnostic> {
    let start = s.peek_span();
    if s.eat(&TokenKind::Not).is_some() {
        let e = parse_not(s)?;
        let span = start.union(e.span());
        return Ok(Expr::UnaryOp(UnaryOp::Not, Box::new(e), span));
    }
    parse_comparison(s)
}

fn parse_comparison(s: &mut TokenStream) -> Result<Expr, Diagnostic> {
    let left = parse_string_pred(s)?;
    // Comparison operators are chainable in some languages but Cypher
    // treats them left-associative.
    let op = match s.peek_kind() {
        TokenKind::Eq => Some(BinOp::Eq),
        TokenKind::NotEq => Some(BinOp::Ne),
        TokenKind::Lt => Some(BinOp::Lt),
        TokenKind::Le => Some(BinOp::Le),
        TokenKind::Gt => Some(BinOp::Gt),
        TokenKind::Ge => Some(BinOp::Ge),
        _ => None,
    };
    if let Some(op) = op {
        s.advance();
        let right = parse_string_pred(s)?;
        let span = left.span().union(right.span());
        return Ok(Expr::BinOp(op, Box::new(left), Box::new(right), span));
    }
    Ok(left)
}

fn parse_string_pred(s: &mut TokenStream) -> Result<Expr, Diagnostic> {
    let left = parse_null_pred(s)?;
    match s.peek_kind() {
        TokenKind::Starts => {
            s.advance();
            s.expect(&TokenKind::With)?;
            let right = parse_null_pred(s)?;
            let span = left.span().union(right.span());
            Ok(Expr::StartsWith(Box::new(left), Box::new(right), span))
        }
        TokenKind::Ends => {
            s.advance();
            s.expect(&TokenKind::With)?;
            let right = parse_null_pred(s)?;
            let span = left.span().union(right.span());
            Ok(Expr::EndsWith(Box::new(left), Box::new(right), span))
        }
        TokenKind::Contains => {
            s.advance();
            let right = parse_null_pred(s)?;
            let span = left.span().union(right.span());
            Ok(Expr::Contains(Box::new(left), Box::new(right), span))
        }
        TokenKind::In => {
            s.advance();
            let right = parse_null_pred(s)?;
            let span = left.span().union(right.span());
            Ok(Expr::In(Box::new(left), Box::new(right), span))
        }
        _ => Ok(left),
    }
}

fn parse_null_pred(s: &mut TokenStream) -> Result<Expr, Diagnostic> {
    let left = parse_additive(s)?;
    if matches!(s.peek_kind(), TokenKind::Is) {
        s.advance();
        let is_not = s.eat(&TokenKind::Not).is_some();
        s.expect(&TokenKind::Null)?;
        let span = left.span();
        if is_not {
            return Ok(Expr::IsNotNull(Box::new(left), span));
        }
        return Ok(Expr::IsNull(Box::new(left), span));
    }
    Ok(left)
}

fn parse_additive(s: &mut TokenStream) -> Result<Expr, Diagnostic> {
    let mut left = parse_multiplicative(s)?;
    loop {
        let op = match s.peek_kind() {
            TokenKind::Plus => BinOp::Add,
            TokenKind::Minus => BinOp::Sub,
            _ => break,
        };
        s.advance();
        let right = parse_multiplicative(s)?;
        let span = left.span().union(right.span());
        left = Expr::BinOp(op, Box::new(left), Box::new(right), span);
    }
    Ok(left)
}

fn parse_multiplicative(s: &mut TokenStream) -> Result<Expr, Diagnostic> {
    let mut left = parse_unary(s)?;
    loop {
        let op = match s.peek_kind() {
            TokenKind::Star => BinOp::Mul,
            TokenKind::Slash => BinOp::Div,
            TokenKind::Percent => {
                return Err(s.error(
                    DiagCode::DeferredFunction,
                    "`%` (modulus) is deferred — pending IR support",
                ));
            }
            TokenKind::Caret => {
                return Err(s.error(
                    DiagCode::DeferredFunction,
                    "`^` (exponent) is deferred — pending IR support",
                ));
            }
            _ => break,
        };
        s.advance();
        let right = parse_unary(s)?;
        let span = left.span().union(right.span());
        left = Expr::BinOp(op, Box::new(left), Box::new(right), span);
    }
    Ok(left)
}

fn parse_unary(s: &mut TokenStream) -> Result<Expr, Diagnostic> {
    let start = s.peek_span();
    if s.eat(&TokenKind::Minus).is_some() {
        let e = parse_unary(s)?;
        let span = start.union(e.span());
        return Ok(Expr::UnaryOp(UnaryOp::Neg, Box::new(e), span));
    }
    parse_postfix(s)
}

fn parse_postfix(s: &mut TokenStream) -> Result<Expr, Diagnostic> {
    let mut e = parse_primary(s)?;
    while let TokenKind::Dot = s.peek_kind() {
        let start = e.span();
        s.advance();
        let prop = parse_ident_or_keyword(s)?;
        let end = s.peek_span();
        e = Expr::Prop(Box::new(e), prop, start.union(end));
    }
    Ok(e)
}

fn parse_primary(s: &mut TokenStream) -> Result<Expr, Diagnostic> {
    let start = s.peek_span();
    match s.peek_kind().clone() {
        TokenKind::Integer(n) => {
            s.advance();
            Ok(Expr::Lit(Literal::Integer(n, start)))
        }
        TokenKind::Float(f) => {
            s.advance();
            Ok(Expr::Lit(Literal::Float(f, start)))
        }
        TokenKind::String(t) => {
            s.advance();
            Ok(Expr::Lit(Literal::String(t, start)))
        }
        TokenKind::True => {
            s.advance();
            Ok(Expr::Lit(Literal::Bool(true, start)))
        }
        TokenKind::False => {
            s.advance();
            Ok(Expr::Lit(Literal::Bool(false, start)))
        }
        TokenKind::Null => {
            s.advance();
            Ok(Expr::Lit(Literal::Null(start)))
        }
        TokenKind::Param(name) => {
            s.advance();
            Ok(Expr::Param(ParamRef { name, span: start }))
        }
        TokenKind::LParen => {
            s.advance();
            let e = parse_expr(s)?;
            s.expect(&TokenKind::RParen)?;
            Ok(e)
        }
        TokenKind::LBracket => {
            s.advance();
            let mut items = Vec::new();
            if !matches!(s.peek_kind(), TokenKind::RBracket) {
                loop {
                    items.push(parse_expr(s)?);
                    if s.eat(&TokenKind::Comma).is_none() {
                        break;
                    }
                }
            }
            let end = s.expect(&TokenKind::RBracket)?;
            Ok(Expr::List(items, start.union(end)))
        }
        TokenKind::Case => parse_case(s),
        TokenKind::Exists => {
            s.advance();
            s.expect(&TokenKind::LBrace)?;
            // Accept both the bare-pattern form `EXISTS { (a)-[:T]-(b) }` and the
            // openCypher subquery form `EXISTS { MATCH (a)-[:T]-(b) WHERE … }`.
            // The leading MATCH is optional; an inner WHERE filters the test.
            s.eat(&TokenKind::Match);
            let pat = parse_pattern(s)?;
            let inner_where = if s.eat(&TokenKind::Where).is_some() {
                Some(Box::new(parse_expr(s)?))
            } else {
                None
            };
            let end = s.expect(&TokenKind::RBrace)?;
            Ok(Expr::Exists(Box::new(pat), inner_where, start.union(end)))
        }
        TokenKind::Count => {
            // count(*) | count(DISTINCT x) | count(x)
            s.advance();
            s.expect(&TokenKind::LParen)?;
            let distinct = s.eat(&TokenKind::Distinct).is_some();
            let args = if matches!(s.peek_kind(), TokenKind::Star) {
                s.advance();
                Vec::new()
            } else {
                vec![parse_expr(s)?]
            };
            let end = s.expect(&TokenKind::RParen)?;
            Ok(Expr::Call(FuncCall {
                name: "count".to_string(),
                args,
                distinct,
                span: start.union(end),
            }))
        }
        TokenKind::Ident(_) => parse_var_or_call(s),
        other => Err(s.error(
            DiagCode::UnexpectedToken,
            format!("unexpected `{other}` in expression"),
        )),
    }
}

fn parse_var_or_call(s: &mut TokenStream) -> Result<Expr, Diagnostic> {
    let start = s.peek_span();
    let name = if let TokenKind::Ident(n) = s.peek_kind() {
        n.clone()
    } else {
        return Err(s.error(DiagCode::UnexpectedToken, "expected identifier"));
    };
    s.advance();
    if matches!(s.peek_kind(), TokenKind::LParen) {
        s.advance();
        let distinct = s.eat(&TokenKind::Distinct).is_some();
        let mut args = Vec::new();
        if !matches!(s.peek_kind(), TokenKind::RParen) {
            loop {
                args.push(parse_expr(s)?);
                if s.eat(&TokenKind::Comma).is_none() {
                    break;
                }
            }
        }
        let end = s.expect(&TokenKind::RParen)?;
        // Reject deferred functions early with specific error.
        let lower = name.to_ascii_lowercase();
        if matches!(
            lower.as_str(),
            "labels" | "keys" | "properties" | "type" | "id" | "point" | "distance"
        ) {
            return Err(Diagnostic {
                code: DiagCode::DeferredFunction,
                severity: crate::diag::Severity::Error,
                message: format!("function `{name}` is deferred in v1"),
                span: start.union(end),
                help: None,
            });
        }
        Ok(Expr::Call(FuncCall {
            name,
            args,
            distinct,
            span: start.union(end),
        }))
    } else {
        Ok(Expr::Var(Variable { name, span: start }))
    }
}

fn parse_case(s: &mut TokenStream) -> Result<Expr, Diagnostic> {
    let start = s.expect(&TokenKind::Case)?;
    // optional subject expression for "CASE expr WHEN ..." form
    let subject = if !matches!(s.peek_kind(), TokenKind::When) {
        Some(parse_expr(s)?)
    } else {
        None
    };
    let mut branches = Vec::new();
    while s.eat(&TokenKind::When).is_some() {
        let cond = parse_expr(s)?;
        s.expect(&TokenKind::Then)?;
        let val = parse_expr(s)?;
        branches.push((cond, val));
    }
    let else_branch = if s.eat(&TokenKind::Else).is_some() {
        Some(parse_expr(s)?)
    } else {
        None
    };
    let end = s.expect(&TokenKind::End)?;
    Ok(Expr::Case(Box::new(CaseExpr {
        subject,
        branches,
        else_branch,
        span: start.union(end),
    })))
}

pub fn parse_map_lit(s: &mut TokenStream) -> Result<MapLit, Diagnostic> {
    let start = s.expect(&TokenKind::LBrace)?;
    let mut entries = Vec::new();
    if !matches!(s.peek_kind(), TokenKind::RBrace) {
        loop {
            let key = parse_ident_or_keyword(s)?;
            s.expect(&TokenKind::Colon)?;
            let v = parse_expr(s)?;
            entries.push((key, v));
            if s.eat(&TokenKind::Comma).is_none() {
                break;
            }
        }
    }
    let end = s.expect(&TokenKind::RBrace)?;
    Ok(MapLit {
        entries,
        span: start.union(end),
    })
}
