//! Statement-level parser.

use crate::ast::{
    CallSubqueryClause, CreateClause, DeleteClause, ForeachClause, MatchClause, MergeClause,
    OrderDirection, OrderItem, ProcedureCall, ProjectionItem, Query, ReadClause, RemoveClause,
    RemoveItem, ReturnClause, SchemaCommand, SchemaCommandKind, SetClause, SetItem, Statement,
    UnionTail, UnwindClause, Update, WithClause, WriteClause, YieldItem,
};
use crate::ast::{Expr, MapLit, ParamRef, Variable};
use crate::diag::{DiagCode, Diagnostic};
use crate::lex::TokenKind;

use super::expr::{parse_expr, parse_map_lit};
use super::pattern::parse_pattern;
use super::stream::TokenStream;

pub fn parse_statement(s: &mut TokenStream) -> Result<Statement, Diagnostic> {
    // Depth-guard statement nesting: UNION tails and `CALL { … }` subqueries
    // both recurse back through this entry, so a bound here (shared with the
    // expression guard via the same counter) caps total recursion — and hence
    // AST depth, which bounds every downstream walker (lowering, param
    // substitution, Drop) — against stack overflow.
    s.enter_recursion()?;
    let result = parse_statement_inner(s);
    s.leave_recursion();
    result
}

fn parse_statement_inner(s: &mut TokenStream) -> Result<Statement, Diagnostic> {
    let start = s.peek_span();

    if let Some(cmd) = parse_schema_command(s)? {
        return Ok(Statement::Schema(cmd));
    }

    // `CALL <ident>` is a procedure call (`CALL db.labels() YIELD …`);
    // `CALL {` / `CALL (` is a subquery clause handled in the loop below.
    if matches!(s.peek_kind(), TokenKind::Call) && matches!(s.peek_at(1), TokenKind::Ident(_)) {
        return parse_procedure_call(s);
    }

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
            TokenKind::Call if matches!(s.peek_at(1), TokenKind::Ident(_)) => {
                return Err(s.error(
                    DiagCode::DeferredProcedure,
                    "CALL <procedure> is supported only as the first clause of a statement",
                ));
            }
            TokenKind::Call => {
                read_clauses.push(ReadClause::CallSubquery(parse_call_subquery(s)?));
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
            TokenKind::Ident(w) if w.eq_ignore_ascii_case("foreach") => {
                write_clauses.push(WriteClause::Foreach(parse_foreach(s)?));
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
    // UNION of writes is rejected by Cypher). The recursion through
    // `parse_statement` is depth-guarded, bounding the union-chain length
    // (and so the AST depth that Drop / param substitution recurse over).
    let right = match parse_statement(s)? {
        Statement::Query(q) => q,
        Statement::Update(_) | Statement::Schema(_) | Statement::CallProcedure(_) => {
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
    let alias = parse_binding_name(s)?;
    let end = alias.span;
    Ok(UnwindClause {
        expr,
        alias,
        span: start.union(end),
    })
}

/// Parse `CALL [(a, b) | (*)] { <read-query ending in RETURN> }`. The optional
/// scope clause names the imported variables, or `(*)` imports the whole outer
/// scope. The body reuses the read-clause grammar and must terminate in RETURN.
fn parse_call_subquery(s: &mut TokenStream) -> Result<CallSubqueryClause, Diagnostic> {
    // Nested `CALL { … CALL { … } }` recurses through here (via `parse_call_body`),
    // a cycle that bypasses `parse_statement` — so it needs its own depth guard.
    s.enter_recursion()?;
    let start = s.expect(&TokenKind::Call)?;

    let (imports, import_all) = if matches!(s.peek_kind(), TokenKind::LParen) {
        s.advance();
        if s.eat(&TokenKind::Star).is_some() {
            s.expect(&TokenKind::RParen)?;
            (Vec::new(), true)
        } else {
            let mut vars = Vec::new();
            if !matches!(s.peek_kind(), TokenKind::RParen) {
                loop {
                    vars.push(parse_var(s)?);
                    if s.eat(&TokenKind::Comma).is_none() {
                        break;
                    }
                }
            }
            s.expect(&TokenKind::RParen)?;
            (vars, false)
        }
    } else {
        (Vec::new(), false)
    };

    s.expect(&TokenKind::LBrace)?;
    let query = parse_call_body(s)?;
    let end = s.expect(&TokenKind::RBrace)?;
    s.leave_recursion();
    Ok(CallSubqueryClause {
        imports,
        import_all,
        query: Box::new(query),
        span: start.union(end),
    })
}

/// Parse the body of a `CALL { … }` subquery: read clauses terminating in
/// RETURN, stopping at the closing `}`. Writes inside CALL and inner UNION are
/// deferred.
fn parse_call_body(s: &mut TokenStream) -> Result<Query, Diagnostic> {
    let start = s.peek_span();
    let mut read_clauses = Vec::new();
    let return_clause = loop {
        match s.peek_kind() {
            TokenKind::Match => read_clauses.push(ReadClause::Match(parse_match(s, false)?)),
            TokenKind::Optional => {
                s.advance();
                read_clauses.push(ReadClause::OptionalMatch(parse_match(s, true)?));
            }
            TokenKind::With => read_clauses.push(ReadClause::With(parse_with(s)?)),
            TokenKind::Unwind => read_clauses.push(ReadClause::Unwind(parse_unwind(s)?)),
            TokenKind::Call if matches!(s.peek_at(1), TokenKind::Ident(_)) => {
                return Err(s.error(
                    DiagCode::DeferredProcedure,
                    "CALL <procedure> is supported only as the first clause of a statement",
                ));
            }
            TokenKind::Call => read_clauses.push(ReadClause::CallSubquery(parse_call_subquery(s)?)),
            TokenKind::Return => break parse_return(s)?,
            TokenKind::Create
            | TokenKind::Merge
            | TokenKind::Set
            | TokenKind::Remove
            | TokenKind::Delete
            | TokenKind::Detach => {
                return Err(s.error(
                    DiagCode::DeferredProcedure,
                    "writes inside CALL { … } are deferred — the subquery body must be read-only (MATCH / OPTIONAL MATCH / WITH / UNWIND / RETURN)",
                ));
            }
            other => {
                return Err(s.error(
                    DiagCode::UnexpectedToken,
                    format!("unexpected `{other}` in CALL subquery — expected MATCH / OPTIONAL MATCH / WITH / UNWIND / RETURN"),
                ));
            }
        }
    };

    // `UNION [ALL]` inside the body: parse the next branch right-recursively,
    // stopping at the closing `}` (unlike the top-level `parse_union_tail`,
    // which recurses through `parse_statement` to EOF).
    let union_tail = if matches!(s.peek_kind(), TokenKind::Union) {
        Some(Box::new(parse_call_union_tail(s)?))
    } else {
        None
    };

    let end = s.peek_span();
    Ok(Query {
        clauses: read_clauses,
        return_clause,
        union_tail,
        span: start.union(end),
    })
}

/// Parse a `UNION [ALL] <call-body branch>` tail inside a `CALL { … }` body.
///
/// The mutual recursion `parse_call_body → parse_call_union_tail →
/// parse_call_body` bypasses `parse_statement`, so it carries its OWN depth
/// guard — without it a long `CALL { … UNION … UNION … }` chain would recurse
/// unbounded and overflow the stack (the top-level `parse_union_tail` is bounded
/// only because it routes back through the guarded `parse_statement`).
fn parse_call_union_tail(s: &mut TokenStream) -> Result<UnionTail, Diagnostic> {
    s.enter_recursion()?;
    let start = s.expect(&TokenKind::Union)?;
    let all = s.eat(&TokenKind::All).is_some();
    let right = parse_call_body(s)?;
    let end = s.peek_span();
    s.leave_recursion();
    Ok(UnionTail {
        all,
        right,
        span: start.union(end),
    })
}

/// Parse a standalone procedure-call statement:
/// `CALL dotted.name[(args)] [YIELD col [AS alias], … | YIELD * [WHERE expr]] [RETURN …]`.
/// The leading `CALL <ident>` has already been sighted by the caller.
fn parse_procedure_call(s: &mut TokenStream) -> Result<Statement, Diagnostic> {
    let start = s.expect(&TokenKind::Call)?;
    let mut name = parse_ident_or_keyword(s)?;
    while s.eat(&TokenKind::Dot).is_some() {
        name.push('.');
        name.push_str(&parse_ident_or_keyword(s)?);
    }

    let mut args = Vec::new();
    if s.eat(&TokenKind::LParen).is_some() {
        if !matches!(s.peek_kind(), TokenKind::RParen) {
            loop {
                args.push(parse_expr(s)?);
                if s.eat(&TokenKind::Comma).is_none() {
                    break;
                }
            }
        }
        s.expect(&TokenKind::RParen)?;
    }

    let mut yields = Vec::new();
    let mut where_clause = None;
    if s.eat(&TokenKind::Yield).is_some() {
        // `YIELD *` exposes all columns — same as omitting YIELD entirely.
        if s.eat(&TokenKind::Star).is_none() {
            loop {
                let span = s.peek_span();
                let column = parse_ident_or_keyword(s)?;
                let alias = if s.eat(&TokenKind::As).is_some() {
                    Some(parse_binding_name(s)?)
                } else {
                    None
                };
                yields.push(YieldItem {
                    column,
                    alias,
                    span,
                });
                if s.eat(&TokenKind::Comma).is_none() {
                    break;
                }
            }
        }
        if s.eat(&TokenKind::Where).is_some() {
            where_clause = Some(parse_expr(s)?);
        }
    }

    // After the YIELD the statement continues like a read query — the
    // schema-introspection shape `CALL apoc.meta.data() YIELD … WHERE …
    // UNWIND other AS o RETURN …` needs WITH / UNWIND / MATCH here.
    let mut rest = Vec::new();
    let return_clause = loop {
        match s.peek_kind() {
            TokenKind::Match => rest.push(ReadClause::Match(parse_match(s, false)?)),
            TokenKind::Optional => {
                s.advance();
                rest.push(ReadClause::OptionalMatch(parse_match(s, true)?));
            }
            TokenKind::With => rest.push(ReadClause::With(parse_with(s)?)),
            TokenKind::Unwind => rest.push(ReadClause::Unwind(parse_unwind(s)?)),
            TokenKind::Call if matches!(s.peek_at(1), TokenKind::Ident(_)) => {
                return Err(s.error(
                    DiagCode::DeferredProcedure,
                    "CALL <procedure> is supported only as the first clause of a statement",
                ));
            }
            TokenKind::Call => rest.push(ReadClause::CallSubquery(parse_call_subquery(s)?)),
            TokenKind::Return => break Some(parse_return(s)?),
            TokenKind::Eof => break None,
            TokenKind::Semicolon => {
                return Err(s.error(
                    DiagCode::DeferredMultiStatement,
                    "multi-statement scripts (semicolon-separated) are deferred; \
                     submit one statement per request",
                ));
            }
            other => {
                return Err(s.error(
                    DiagCode::UnexpectedToken,
                    format!(
                        "unexpected `{other}` after CALL {name} — expected YIELD / WITH / \
                         UNWIND / MATCH / RETURN"
                    ),
                ));
            }
        }
    };

    if return_clause.is_none() && !rest.is_empty() {
        return Err(s.error(
            DiagCode::UnexpectedEof,
            "a procedure call followed by additional clauses must end in RETURN",
        ));
    }

    let end = s.peek_span();
    Ok(Statement::CallProcedure(ProcedureCall {
        name,
        args,
        yields,
        where_clause,
        rest,
        return_clause,
        span: start.union(end),
    }))
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
                Some(parse_binding_name(s)?)
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
                let map = parse_set_map(s)?;
                items.push(SetItem::MapReplace { target, map });
            }
            TokenKind::PlusEq => {
                s.advance();
                let map = parse_set_map(s)?;
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

/// Detect a schema DDL statement head: `CREATE [OR REPLACE] INDEX|CONSTRAINT`,
/// `DROP INDEX|CONSTRAINT`, `SHOW INDEX[ES]|CONSTRAINT[S]`. Fluree has no
/// user-managed index/constraint catalog (everything is indexed), so these
/// are accepted for tooling compatibility; the body is consumed without
/// detailed parsing. Returns `None` for every other statement head.
fn parse_schema_command(s: &mut TokenStream) -> Result<Option<SchemaCommand>, Diagnostic> {
    fn ident_at(s: &TokenStream, off: usize, words: &[&str]) -> bool {
        matches!(s.peek_at(off), TokenKind::Ident(w)
            if words.iter().any(|x| w.eq_ignore_ascii_case(x)))
    }
    let start = s.peek_span();
    let kind = match s.peek_kind() {
        TokenKind::Create
            if ident_at(s, 1, &["index", "constraint"])
                || (ident_at(s, 1, &["or"]) && ident_at(s, 2, &["replace"])) =>
        {
            SchemaCommandKind::CreateSchema
        }
        TokenKind::Ident(w)
            if w.eq_ignore_ascii_case("drop") && ident_at(s, 1, &["index", "constraint"]) =>
        {
            SchemaCommandKind::DropSchema
        }
        TokenKind::Ident(w)
            if w.eq_ignore_ascii_case("show")
                && ident_at(s, 1, &["indexes", "index", "constraints", "constraint"]) =>
        {
            SchemaCommandKind::ShowSchema
        }
        _ => return Ok(None),
    };
    // Swallow the command body; keep the one-statement-per-request rule.
    while !s.is_eof() {
        if matches!(s.peek_kind(), TokenKind::Semicolon) {
            return Err(s.error(
                DiagCode::DeferredMultiStatement,
                "multi-statement scripts (semicolon-separated) are deferred; \
                 submit one statement per request",
            ));
        }
        s.advance();
    }
    let end = s.peek_span();
    Ok(Some(SchemaCommand {
        kind,
        span: start.union(end),
    }))
}

/// The map side of `SET n = …` / `SET n += …`: an inline `{k: v, …}` literal,
/// or a whole-map parameter (`SET n += $props`) — encoded as a single entry
/// under [`crate::params::WHOLE_MAP_PARAM_KEY`] and expanded to real entries
/// during param substitution (map keys parse as identifiers, so the reserved
/// empty key cannot occur otherwise).
fn parse_set_map(s: &mut TokenStream) -> Result<MapLit, Diagnostic> {
    if let TokenKind::Param(name) = s.peek_kind() {
        let name = name.clone();
        let span = s.peek_span();
        s.advance();
        return Ok(MapLit {
            entries: vec![(
                crate::params::WHOLE_MAP_PARAM_KEY.to_string(),
                Expr::Param(ParamRef { name, span }),
            )],
            span,
        });
    }
    parse_map_lit(s)
}

/// Parse `FOREACH (var IN <list expr> | <write clauses>)`. The leading
/// `FOREACH` identifier has been sighted by the caller.
fn parse_foreach(s: &mut TokenStream) -> Result<ForeachClause, Diagnostic> {
    let start = s.peek_span();
    s.advance(); // FOREACH
    s.expect(&TokenKind::LParen)?;
    let var = parse_var(s)?;
    if !matches!(s.peek_kind(), TokenKind::In) {
        return Err(s.error(
            DiagCode::UnexpectedToken,
            "expected IN after the FOREACH variable",
        ));
    }
    s.advance();
    let list = parse_expr(s)?;
    s.expect(&TokenKind::Pipe)?;
    let mut body = Vec::new();
    loop {
        match s.peek_kind() {
            TokenKind::Create => body.push(WriteClause::Create(parse_create(s)?)),
            TokenKind::Merge => body.push(WriteClause::Merge(parse_merge(s)?)),
            TokenKind::Set => body.push(WriteClause::Set(parse_set(s)?)),
            TokenKind::Remove => body.push(WriteClause::Remove(parse_remove(s)?)),
            TokenKind::Delete => body.push(WriteClause::Delete(parse_delete(s, false)?)),
            TokenKind::Detach => {
                s.advance();
                if !matches!(s.peek_kind(), TokenKind::Delete) {
                    return Err(s.error(DiagCode::UnexpectedToken, "expected DELETE after DETACH"));
                }
                body.push(WriteClause::Delete(parse_delete(s, true)?));
            }
            TokenKind::Ident(w) if w.eq_ignore_ascii_case("foreach") => {
                body.push(WriteClause::Foreach(parse_foreach(s)?));
            }
            TokenKind::RParen => break,
            other => {
                return Err(s.error(
                    DiagCode::UnexpectedToken,
                    format!(
                        "unexpected `{other}` in FOREACH body — expected \
                         CREATE / MERGE / SET / REMOVE / DELETE"
                    ),
                ));
            }
        }
    }
    let end = s.expect(&TokenKind::RParen)?;
    if body.is_empty() {
        return Err(s.error(
            DiagCode::UnexpectedToken,
            "FOREACH body needs at least one write clause",
        ));
    }
    Ok(ForeachClause {
        var,
        list,
        body,
        span: start.union(end),
    })
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
/// whose textual form is a valid identifier — returning the **as-written**
/// source text, not the token's canonical uppercase display (`{end: 1}`
/// must produce the key `end`, not `END`).
pub(crate) fn parse_ident_or_keyword(s: &mut TokenStream) -> Result<String, Diagnostic> {
    match s.peek_ident_text() {
        Some(text) => {
            s.advance();
            Ok(text)
        }
        None => Err(s.error(
            DiagCode::UnexpectedToken,
            format!("expected identifier, got `{}`", s.peek_kind()),
        )),
    }
}

/// Parse a binding name — a variable introduced in name position (`AS <name>`,
/// `UNWIND … AS <name>`, `YIELD col AS <name>`). Unlike [`parse_var`], this
/// accepts keyword tokens as identifiers (`AS end`, `AS count`): after `AS`
/// exactly one name is expected, so there is no grammar ambiguity. This is a
/// deliberate leniency over strict openCypher, which requires backticking
/// reserved words.
pub(crate) fn parse_binding_name(s: &mut TokenStream) -> Result<Variable, Diagnostic> {
    let span = s.peek_span();
    match s.peek_ident_text() {
        Some(name) => {
            s.advance();
            Ok(Variable { name, span })
        }
        None => Err(s.error(
            DiagCode::UnexpectedToken,
            format!("expected identifier, got `{}`", s.peek_kind()),
        )),
    }
}
