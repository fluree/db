//! Statement-level parser.

use crate::ast::{
    CreateClause, DeleteClause, MatchClause, MergeClause, OrderDirection, OrderItem,
    ProjectionItem, Query, ReadClause, RemoveClause, RemoveItem, ReturnClause, SetClause, SetItem,
    Statement, UnionTail, UnwindClause, Update, WithClause, WriteClause,
};
use crate::ast::{Expr, Variable};
use crate::diag::{DiagCode, Diagnostic};
use crate::lex::TokenKind;

use super::expr::{parse_expr, parse_map_lit};
use super::pattern::parse_pattern;
use super::stream::TokenStream;

pub fn parse_statement(s: &mut TokenStream) -> Result<Statement, Diagnostic> {
    let start = s.peek_span();

    // Categorize by first token. Queries start with MATCH / OPTIONAL /
    // WITH / UNWIND / RETURN. Writes start with CREATE / MERGE /
    // ((MATCH | OPTIONAL | WITH | UNWIND)+ then CREATE/MERGE/SET/REMOVE/DELETE).
    let mut read_clauses = Vec::new();
    let mut write_clauses = Vec::new();
    let mut return_clause: Option<ReturnClause> = None;

    loop {
        match s.peek_kind() {
            TokenKind::Match => {
                read_clauses.push(ReadClause::Match(parse_match(s, false)?));
            }
            TokenKind::Optional => {
                s.advance();
                read_clauses.push(ReadClause::OptionalMatch(parse_match(s, true)?));
            }
            TokenKind::With => {
                read_clauses.push(ReadClause::With(parse_with(s)?));
            }
            TokenKind::Unwind => {
                read_clauses.push(ReadClause::Unwind(parse_unwind(s)?));
            }
            TokenKind::Return => {
                return_clause = Some(parse_return(s)?);
                break;
            }
            TokenKind::Create => {
                write_clauses.push(WriteClause::Create(parse_create(s)?));
            }
            TokenKind::Merge => {
                write_clauses.push(WriteClause::Merge(parse_merge(s)?));
            }
            TokenKind::Set => {
                write_clauses.push(WriteClause::Set(parse_set(s)?));
            }
            TokenKind::Remove => {
                write_clauses.push(WriteClause::Remove(parse_remove(s)?));
            }
            TokenKind::Delete => {
                write_clauses.push(WriteClause::Delete(parse_delete(s, false)?));
            }
            TokenKind::Detach => {
                s.advance();
                if !matches!(s.peek_kind(), TokenKind::Delete) {
                    return Err(s.error(DiagCode::UnexpectedToken, "expected DELETE after DETACH"));
                }
                write_clauses.push(WriteClause::Delete(parse_delete(s, true)?));
            }
            TokenKind::Eof => break,
            other => {
                return Err(s.error(
                    DiagCode::UnexpectedToken,
                    format!("unexpected `{other}` — expected MATCH / WITH / RETURN / CREATE / MERGE / SET / REMOVE / DELETE / DETACH"),
                ));
            }
        }

        if matches!(s.peek_kind(), TokenKind::Semicolon) {
            return Err(s.error(
                DiagCode::DeferredMultiStatement,
                "multi-statement scripts (semicolon-separated) are deferred; submit one statement per request",
            ));
        }
    }

    let end = s.peek_span();
    let span = start.union(end);

    if !write_clauses.is_empty() {
        Ok(Statement::Update(Update {
            read_clauses,
            write_clauses,
            return_clause,
            span,
        }))
    } else if let Some(rc) = return_clause {
        // After a RETURN, an optional `UNION [ALL] <next query>` may
        // follow. Parse it right-recursively.
        let union_tail = if matches!(s.peek_kind(), TokenKind::Union) {
            Some(Box::new(parse_union_tail(s)?))
        } else {
            None
        };
        let end = s.peek_span();
        let span = start.union(end);
        Ok(Statement::Query(Query {
            clauses: read_clauses,
            return_clause: rc,
            union_tail,
            span,
        }))
    } else {
        Err(Diagnostic {
            code: DiagCode::UnexpectedEof,
            severity: crate::diag::Severity::Error,
            message: "query has no RETURN clause and no write operation".to_string(),
            span,
            help: Some(
                "add a `RETURN ...` clause or a write operation (CREATE/MERGE/SET/REMOVE/DELETE)"
                    .to_string(),
            ),
        })
    }
}

/// Parse a `UNION [ALL] <query>` tail. The leading `UNION` keyword
/// is consumed here.
fn parse_union_tail(s: &mut TokenStream) -> Result<UnionTail, Diagnostic> {
    let start = s.expect(&TokenKind::Union)?;
    let all = s.eat(&TokenKind::All).is_some();
    // The right side is another full query (read-shaped only —
    // UNION of writes is rejected by Cypher).
    let right = match parse_statement(s)? {
        Statement::Query(q) => q,
        Statement::Update(_) => {
            return Err(s.error(
                DiagCode::UnexpectedToken,
                "UNION cannot combine write statements — both sides must be read queries",
            ));
        }
    };
    let end = s.peek_span();
    Ok(UnionTail {
        all,
        right,
        span: start.union(end),
    })
}

fn parse_match(s: &mut TokenStream, _is_optional: bool) -> Result<MatchClause, Diagnostic> {
    let start = s.expect(&TokenKind::Match)?;
    let pattern = parse_pattern(s)?;
    let where_clause = if matches!(s.peek_kind(), TokenKind::Where) {
        s.advance();
        Some(parse_expr(s)?)
    } else {
        None
    };
    let end = s.peek_span();
    Ok(MatchClause {
        pattern,
        where_clause,
        span: start.union(end),
    })
}

fn parse_with(s: &mut TokenStream) -> Result<WithClause, Diagnostic> {
    let start = s.expect(&TokenKind::With)?;
    let distinct = s.eat(&TokenKind::Distinct).is_some();
    let items = parse_projection_items(s)?;
    let (order_by, skip, limit) = parse_modifiers(s)?;
    let where_clause = if matches!(s.peek_kind(), TokenKind::Where) {
        s.advance();
        Some(parse_expr(s)?)
    } else {
        None
    };
    let end = s.peek_span();
    Ok(WithClause {
        items,
        distinct,
        where_clause,
        order_by,
        skip,
        limit,
        span: start.union(end),
    })
}

fn parse_unwind(s: &mut TokenStream) -> Result<UnwindClause, Diagnostic> {
    let start = s.expect(&TokenKind::Unwind)?;
    let expr = parse_expr(s)?;
    s.expect(&TokenKind::As)?;
    let alias = parse_var(s)?;
    let end = alias.span;
    Ok(UnwindClause {
        expr,
        alias,
        span: start.union(end),
    })
}

fn parse_return(s: &mut TokenStream) -> Result<ReturnClause, Diagnostic> {
    let start = s.expect(&TokenKind::Return)?;
    let distinct = s.eat(&TokenKind::Distinct).is_some();
    let items = parse_projection_items(s)?;
    let (order_by, skip, limit) = parse_modifiers(s)?;
    let end = s.peek_span();
    Ok(ReturnClause {
        items,
        distinct,
        order_by,
        skip,
        limit,
        span: start.union(end),
    })
}

fn parse_projection_items(s: &mut TokenStream) -> Result<Vec<ProjectionItem>, Diagnostic> {
    let mut items = Vec::new();
    loop {
        // `*` in RETURN means "all bound vars" — we treat it as a marker
        // projection item via a sentinel variable.
        let item_start = s.peek_span();
        let (expr, alias) = if matches!(s.peek_kind(), TokenKind::Star) {
            s.advance();
            (
                Expr::Var(Variable {
                    name: "*".to_string(),
                    span: item_start,
                }),
                None,
            )
        } else {
            let expr = parse_expr(s)?;
            let alias = if matches!(s.peek_kind(), TokenKind::As) {
                s.advance();
                Some(parse_var(s)?)
            } else {
                None
            };
            (expr, alias)
        };
        let end = alias
            .as_ref()
            .map(|v| v.span)
            .unwrap_or_else(|| s.peek_span());
        items.push(ProjectionItem {
            expr,
            alias,
            span: item_start.union(end),
        });
        if s.eat(&TokenKind::Comma).is_none() {
            break;
        }
    }
    Ok(items)
}

type Modifiers = (Vec<OrderItem>, Option<Expr>, Option<Expr>);

fn parse_modifiers(s: &mut TokenStream) -> Result<Modifiers, Diagnostic> {
    let mut order_by = Vec::new();
    if matches!(s.peek_kind(), TokenKind::Order) {
        s.advance();
        s.expect(&TokenKind::By)?;
        loop {
            let expr = parse_expr(s)?;
            let direction = if s.eat(&TokenKind::Asc).is_some() {
                OrderDirection::Ascending
            } else if s.eat(&TokenKind::Desc).is_some() {
                OrderDirection::Descending
            } else {
                OrderDirection::Ascending
            };
            order_by.push(OrderItem { expr, direction });
            if s.eat(&TokenKind::Comma).is_none() {
                break;
            }
        }
    }
    let skip = if s.eat(&TokenKind::Skip).is_some() {
        Some(parse_expr(s)?)
    } else {
        None
    };
    let limit = if s.eat(&TokenKind::Limit).is_some() {
        Some(parse_expr(s)?)
    } else {
        None
    };
    Ok((order_by, skip, limit))
}

fn parse_create(s: &mut TokenStream) -> Result<CreateClause, Diagnostic> {
    let start = s.expect(&TokenKind::Create)?;
    let pattern = parse_pattern(s)?;
    let end = pattern.span;
    Ok(CreateClause {
        pattern,
        span: start.union(end),
    })
}

fn parse_merge(s: &mut TokenStream) -> Result<MergeClause, Diagnostic> {
    let start = s.expect(&TokenKind::Merge)?;
    let pattern = parse_pattern(s)?;
    let mut on_create = Vec::new();
    let mut on_match = Vec::new();
    while matches!(s.peek_kind(), TokenKind::On) {
        s.advance();
        match s.peek_kind() {
            TokenKind::Create => {
                s.advance();
                s.expect(&TokenKind::Set)?;
                on_create = parse_set_items(s)?;
            }
            TokenKind::Match => {
                s.advance();
                s.expect(&TokenKind::Set)?;
                on_match = parse_set_items(s)?;
            }
            _ => {
                return Err(s.error(
                    DiagCode::UnexpectedToken,
                    "expected CREATE or MATCH after ON",
                ));
            }
        }
    }
    let end = s.peek_span();
    Ok(MergeClause {
        pattern,
        on_create,
        on_match,
        span: start.union(end),
    })
}

fn parse_set(s: &mut TokenStream) -> Result<SetClause, Diagnostic> {
    let start = s.expect(&TokenKind::Set)?;
    let items = parse_set_items(s)?;
    let end = s.peek_span();
    Ok(SetClause {
        items,
        span: start.union(end),
    })
}

fn parse_set_items(s: &mut TokenStream) -> Result<Vec<SetItem>, Diagnostic> {
    let mut items = Vec::new();
    loop {
        let target = parse_var(s)?;
        match s.peek_kind() {
            TokenKind::Dot => {
                s.advance();
                let property = parse_ident_or_keyword(s)?;
                s.expect(&TokenKind::Eq)?;
                let value = parse_expr(s)?;
                items.push(SetItem::Property {
                    target,
                    property,
                    value,
                });
            }
            TokenKind::Eq => {
                s.advance();
                let map = parse_map_lit(s)?;
                items.push(SetItem::MapReplace { target, map });
            }
            TokenKind::PlusEq => {
                s.advance();
                let map = parse_map_lit(s)?;
                items.push(SetItem::MapMerge { target, map });
            }
            TokenKind::Colon => {
                let mut labels = Vec::new();
                while s.eat(&TokenKind::Colon).is_some() {
                    labels.push(parse_ident_or_keyword(s)?);
                }
                items.push(SetItem::Labels { target, labels });
            }
            other => {
                return Err(s.error(
                    DiagCode::UnexpectedToken,
                    format!("expected `.`, `=`, `+=`, or `:` in SET item, got `{other}`"),
                ));
            }
        }
        if s.eat(&TokenKind::Comma).is_none() {
            break;
        }
    }
    Ok(items)
}

fn parse_remove(s: &mut TokenStream) -> Result<RemoveClause, Diagnostic> {
    let start = s.expect(&TokenKind::Remove)?;
    let mut items = Vec::new();
    loop {
        let target = parse_var(s)?;
        match s.peek_kind() {
            TokenKind::Dot => {
                s.advance();
                let property = parse_ident_or_keyword(s)?;
                items.push(RemoveItem::Property { target, property });
            }
            TokenKind::Colon => {
                let mut labels = Vec::new();
                while s.eat(&TokenKind::Colon).is_some() {
                    labels.push(parse_ident_or_keyword(s)?);
                }
                items.push(RemoveItem::Labels { target, labels });
            }
            other => {
                return Err(s.error(
                    DiagCode::UnexpectedToken,
                    format!("expected `.` or `:` in REMOVE item, got `{other}`"),
                ));
            }
        }
        if s.eat(&TokenKind::Comma).is_none() {
            break;
        }
    }
    let end = s.peek_span();
    Ok(RemoveClause {
        items,
        span: start.union(end),
    })
}

fn parse_delete(s: &mut TokenStream, detach: bool) -> Result<DeleteClause, Diagnostic> {
    let start = s.expect(&TokenKind::Delete)?;
    let mut targets = Vec::new();
    loop {
        targets.push(parse_var(s)?);
        if s.eat(&TokenKind::Comma).is_none() {
            break;
        }
    }
    let end = s.peek_span();
    Ok(DeleteClause {
        detach,
        targets,
        span: start.union(end),
    })
}

pub(crate) fn parse_var(s: &mut TokenStream) -> Result<Variable, Diagnostic> {
    let span = s.peek_span();
    let kind = s.peek_kind().clone();
    if let TokenKind::Ident(name) = kind {
        s.advance();
        Ok(Variable { name, span })
    } else {
        Err(s.error(
            DiagCode::UnexpectedToken,
            format!("expected identifier, got `{}`", s.peek_kind()),
        ))
    }
}

/// Parses an identifier or a keyword-as-identifier (Cypher allows
/// reserved words in property/label position). We accept any token
/// whose textual form is a valid identifier.
pub(crate) fn parse_ident_or_keyword(s: &mut TokenStream) -> Result<String, Diagnostic> {
    let kind = s.peek_kind().clone();
    if let TokenKind::Ident(name) = kind {
        s.advance();
        Ok(name)
    } else {
        // Accept the token's display form if it's a keyword (uppercase).
        let display = format!("{}", s.peek_kind());
        if display.chars().all(|c| c.is_ascii_alphabetic() || c == '_') {
            s.advance();
            Ok(display)
        } else {
            Err(s.error(
                DiagCode::UnexpectedToken,
                format!("expected identifier, got `{}`", s.peek_kind()),
            ))
        }
    }
}
