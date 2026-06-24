//! Expression parser — Pratt-style precedence climbing.

use crate::ast::{
    BinOp, CaseExpr, Expr, FuncCall, ListPredicateKind, Literal, MapLit, ParamRef, UnaryOp,
    Variable,
};
use crate::diag::{DiagCode, Diagnostic};
use crate::lex::TokenKind;
use crate::span::SourceSpan;

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
    let mut left = parse_and(s)?;
    while s.eat(&TokenKind::Xor).is_some() {
        let right = parse_and(s)?;
        let span = left.span().union(right.span());
        let either = Expr::BinOp(
            BinOp::Or,
            Box::new(left.clone()),
            Box::new(right.clone()),
            span,
        );
        let both = Expr::BinOp(BinOp::And, Box::new(left), Box::new(right), span);
        let not_both = Expr::UnaryOp(UnaryOp::Not, Box::new(both), span);
        left = Expr::BinOp(BinOp::And, Box::new(either), Box::new(not_both), span);
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
            TokenKind::Percent => BinOp::Mod,
            _ => break,
        };
        s.advance();
        let right = parse_unary(s)?;
        let span = left.span().union(right.span());
        left = Expr::BinOp(op, Box::new(left), Box::new(right), span);
    }
    Ok(left)
}

/// Unary `-` — binds looser than `^` but tighter than `* / %`, so
/// `-2 ^ 2` = `-(2 ^ 2)` = `-4`, matching openCypher / Neo4j precedence.
fn parse_unary(s: &mut TokenStream) -> Result<Expr, Diagnostic> {
    let start = s.peek_span();
    if s.eat(&TokenKind::Minus).is_some() {
        let e = parse_unary(s)?;
        let span = start.union(e.span());
        return Ok(Expr::UnaryOp(UnaryOp::Neg, Box::new(e), span));
    }
    parse_power(s)
}

/// Exponentiation `^` — binds tighter than unary `-` and `* / %`, and is
/// right-associative (`2 ^ 3 ^ 2` = `2 ^ (3 ^ 2)`), matching openCypher. The
/// left operand is a postfix expression (a leading sign belongs to the looser
/// unary layer above), while the right operand is a full unary expression so a
/// signed exponent (`2 ^ -3`) parses.
fn parse_power(s: &mut TokenStream) -> Result<Expr, Diagnostic> {
    let left = parse_postfix(s)?;
    if matches!(s.peek_kind(), TokenKind::Caret) {
        s.advance();
        let right = parse_unary(s)?;
        let span = left.span().union(right.span());
        Ok(Expr::BinOp(
            BinOp::Pow,
            Box::new(left),
            Box::new(right),
            span,
        ))
    } else {
        Ok(left)
    }
}

fn parse_postfix(s: &mut TokenStream) -> Result<Expr, Diagnostic> {
    let mut e = parse_primary(s)?;
    loop {
        match s.peek_kind() {
            TokenKind::Dot => {
                let start = e.span();
                s.advance();
                let prop = parse_ident_or_keyword(s)?;
                let end = s.peek_span();
                e = Expr::Prop(Box::new(e), prop, start.union(end));
            }
            // `expr[index]` — list element access. (A leading `[` at primary
            // position is a list literal; here it follows an expression.)
            TokenKind::LBracket => {
                let start = e.span();
                s.advance();
                let index = parse_expr(s)?;
                let end = s.expect(&TokenKind::RBracket)?;
                e = Expr::Index(Box::new(e), Box::new(index), start.union(end));
            }
            _ => break,
        }
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
            // `[var IN list …]` is a list comprehension; `[a, b, …]` a literal.
            // The distinguishing shape is an identifier immediately followed by
            // `IN`.
            if matches!(s.peek_kind(), TokenKind::Ident(_)) && matches!(s.peek_at(1), TokenKind::In)
            {
                return parse_list_comprehension(s, start);
            }
            // `[(a)-[:T]->(b) … | proj]` is a pattern comprehension. It starts
            // with a node pattern `(`, but so does a parenthesized list element
            // (`[(1 + 2)]`); the two are only distinguishable by trying to parse
            // a pattern + `|`, so speculatively parse and backtrack on failure.
            if matches!(s.peek_kind(), TokenKind::LParen) {
                let mark = s.mark();
                if let Ok(pc) = parse_pattern_comprehension(s, start) {
                    return Ok(pc);
                }
                s.reset(mark);
            }
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
        TokenKind::All => {
            // `all(var IN list WHERE pred)` — the only keyword-tokenized list
            // predicate (any/none/single are identifiers, handled in calls).
            s.advance();
            parse_list_predicate(s, ListPredicateKind::All, start)
        }
        TokenKind::LBrace => {
            // Map literal in expression position: `{key: expr, ...}`. Reuses the
            // pattern-side map parser, then re-homes it as an `Expr::Map`.
            let map = parse_map_lit(s)?;
            Ok(Expr::Map(map.entries, map.span))
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
    // List-iteration forms tokenized as identifiers (`all` is a keyword, handled
    // in `parse_primary`). They take a `var IN list …` argument, not a normal
    // call arg list — so intercept before generic call parsing.
    match name.to_ascii_lowercase().as_str() {
        "reduce" => {
            s.advance();
            return parse_reduce(s, start);
        }
        "any" => {
            s.advance();
            return parse_list_predicate(s, ListPredicateKind::Any, start);
        }
        "none" => {
            s.advance();
            return parse_list_predicate(s, ListPredicateKind::None, start);
        }
        "single" => {
            s.advance();
            return parse_list_predicate(s, ListPredicateKind::Single, start);
        }
        _ => {}
    }
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
        if matches!(lower.as_str(), "point" | "distance") {
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
    } else if matches!(s.peek_kind(), TokenKind::LBrace) {
        // `var{ … }` — a map projection (distinct from the bare map literal
        // `{ … }`, which has no leading variable).
        parse_map_projection(s, Variable { name, span: start }, start)
    } else {
        Ok(Expr::Var(Variable { name, span: start }))
    }
}

/// `var{ .key, .*, key: expr }` — the leading `var` is already parsed.
fn parse_map_projection(
    s: &mut TokenStream,
    var: Variable,
    start: SourceSpan,
) -> Result<Expr, Diagnostic> {
    use crate::ast::{MapProjectionExpr, MapProjectionSelector};
    s.expect(&TokenKind::LBrace)?;
    let mut selectors = Vec::new();
    if !matches!(s.peek_kind(), TokenKind::RBrace) {
        loop {
            if s.eat(&TokenKind::Dot).is_some() {
                // `.key` property selector, or `.*` all-properties.
                if s.eat(&TokenKind::Star).is_some() {
                    selectors.push(MapProjectionSelector::AllProperties);
                } else {
                    selectors.push(MapProjectionSelector::Property(parse_ident_or_keyword(s)?));
                }
            } else {
                // `key: expr` explicit entry.
                let key = parse_ident_or_keyword(s)?;
                s.expect(&TokenKind::Colon)?;
                selectors.push(MapProjectionSelector::Literal(
                    key,
                    Box::new(parse_expr(s)?),
                ));
            }
            if s.eat(&TokenKind::Comma).is_none() {
                break;
            }
        }
    }
    let end = s.expect(&TokenKind::RBrace)?;
    Ok(Expr::MapProjection(Box::new(MapProjectionExpr {
        var,
        selectors,
        span: start.union(end),
    })))
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

/// Parse a bare identifier as a (loop) variable.
fn parse_variable(s: &mut TokenStream) -> Result<Variable, Diagnostic> {
    let span = s.peek_span();
    let TokenKind::Ident(name) = s.peek_kind().clone() else {
        return Err(s.error(DiagCode::UnexpectedToken, "expected a variable name"));
    };
    s.advance();
    Ok(Variable { name, span })
}

/// `[var IN list (WHERE filter)? (| map)?]` — the leading `[` is already eaten.
fn parse_list_comprehension(s: &mut TokenStream, start: SourceSpan) -> Result<Expr, Diagnostic> {
    let var = parse_variable(s)?;
    s.expect(&TokenKind::In)?;
    let list = Box::new(parse_expr(s)?);
    let filter = if s.eat(&TokenKind::Where).is_some() {
        Some(Box::new(parse_expr(s)?))
    } else {
        None
    };
    let map = if s.eat(&TokenKind::Pipe).is_some() {
        Some(Box::new(parse_expr(s)?))
    } else {
        None
    };
    let end = s.expect(&TokenKind::RBracket)?;
    Ok(Expr::ListComprehension(Box::new(
        crate::ast::ListComprehensionExpr {
            var,
            list,
            filter,
            map,
            span: start.union(end),
        },
    )))
}

/// `(var IN list (WHERE pred)?)` for a list predicate — the kind token is
/// already eaten. A missing `WHERE` defaults the predicate to `true` (so
/// `any(x IN xs)` is "xs is non-empty").
fn parse_list_predicate(
    s: &mut TokenStream,
    kind: ListPredicateKind,
    start: SourceSpan,
) -> Result<Expr, Diagnostic> {
    s.expect(&TokenKind::LParen)?;
    let var = parse_variable(s)?;
    s.expect(&TokenKind::In)?;
    let list = Box::new(parse_expr(s)?);
    let predicate = if s.eat(&TokenKind::Where).is_some() {
        Box::new(parse_expr(s)?)
    } else {
        Box::new(Expr::Lit(Literal::Bool(true, start)))
    };
    let end = s.expect(&TokenKind::RParen)?;
    Ok(Expr::ListPredicate(Box::new(
        crate::ast::ListPredicateExpr {
            kind,
            var,
            list,
            predicate,
            span: start.union(end),
        },
    )))
}

/// `[pattern (WHERE filter)? | projection]` — the leading `[` is already eaten,
/// and the caller has confirmed the next token is `(`. Returns `Err` (for the
/// caller to backtrack) if it doesn't parse as a pattern comprehension.
fn parse_pattern_comprehension(s: &mut TokenStream, start: SourceSpan) -> Result<Expr, Diagnostic> {
    use crate::ast::PatternComprehensionExpr;
    let pattern = parse_pattern(s)?;
    let filter = if s.eat(&TokenKind::Where).is_some() {
        Some(Box::new(parse_expr(s)?))
    } else {
        None
    };
    // The `|` projection is mandatory — it's what distinguishes a pattern
    // comprehension from a parenthesized list element.
    s.expect(&TokenKind::Pipe)?;
    let projection = Box::new(parse_expr(s)?);
    let end = s.expect(&TokenKind::RBracket)?;
    Ok(Expr::PatternComprehension(Box::new(
        PatternComprehensionExpr {
            pattern,
            filter,
            projection,
            span: start.union(end),
        },
    )))
}

/// `(acc = init, var IN list | body)` for `reduce` — `reduce` already eaten.
fn parse_reduce(s: &mut TokenStream, start: SourceSpan) -> Result<Expr, Diagnostic> {
    s.expect(&TokenKind::LParen)?;
    let acc = parse_variable(s)?;
    s.expect(&TokenKind::Eq)?;
    let init = Box::new(parse_expr(s)?);
    s.expect(&TokenKind::Comma)?;
    let var = parse_variable(s)?;
    s.expect(&TokenKind::In)?;
    let list = Box::new(parse_expr(s)?);
    s.expect(&TokenKind::Pipe)?;
    let body = Box::new(parse_expr(s)?);
    let end = s.expect(&TokenKind::RParen)?;
    Ok(Expr::Reduce(Box::new(crate::ast::ReduceExpr {
        acc,
        init,
        var,
        list,
        body,
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
