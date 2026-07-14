//! Statement-wide scan for variables whose evaluation depends on the
//! *annotation identity* of a relationship.
//!
//! A bound relationship variable is used in one of two ways:
//!
//! - **Value surface** — `RETURN e`, `type(e)`, `startNode(e)`, `endNode(e)`,
//!   equality, `collect(e)`, … These read only `{start, predicate, end}` and
//!   are satisfiable by a relationship value synthesized from the plain base
//!   triple (`MakeRel`), so unreified (plain-RDF) edges match too.
//! - **Annotation surface** — `e.prop`, `properties(e)`, `keys(e)`, map
//!   projections `e{...}`. These need the `f:reifies*` annotation node, so the
//!   variable must bind the annotation SID (`Pattern::EdgeAnnotation`).
//!
//! This scan over-approximates the annotation surface: any variable that
//! *might* need annotation identity is collected, and pattern lowering only
//! applies the plain-triple fallback to relationship variables absent from
//! the set.

use std::collections::HashSet;

use crate::ast::{
    CaseExpr, Expr, MapLit, MapProjectionSelector, NodePattern, Pattern, PatternPart, Query,
    ReadClause, RelPattern, ReturnClause, WithClause,
};

/// Collect the names of variables used on the annotation surface anywhere in
/// the query (including UNION tails, CALL subqueries, and nested patterns).
pub(super) fn annotation_dependent_vars(q: &Query) -> HashSet<String> {
    let mut out = HashSet::new();
    scan_query(q, &mut out);
    out
}

fn scan_query(q: &Query, out: &mut HashSet<String>) {
    for c in &q.clauses {
        scan_read_clause(c, out);
    }
    scan_return(&q.return_clause, out);
    if let Some(t) = &q.union_tail {
        scan_query(&t.right, out);
    }
}

fn scan_read_clause(c: &ReadClause, out: &mut HashSet<String>) {
    match c {
        ReadClause::Match(m) | ReadClause::OptionalMatch(m) => {
            scan_pattern(&m.pattern, out);
            if let Some(w) = &m.where_clause {
                scan_expr(w, out);
            }
        }
        ReadClause::With(w) => scan_with(w, out),
        ReadClause::Unwind(u) => scan_expr(&u.expr, out),
        ReadClause::CallSubquery(cs) => scan_query(&cs.query, out),
        ReadClause::InlineRows { rows, .. } => {
            for row in rows {
                for cell in row {
                    scan_expr(cell, out);
                }
            }
        }
    }
}

fn scan_with(w: &WithClause, out: &mut HashSet<String>) {
    for item in &w.items {
        scan_expr(&item.expr, out);
    }
    if let Some(e) = &w.where_clause {
        scan_expr(e, out);
    }
    for o in &w.order_by {
        scan_expr(&o.expr, out);
    }
    for e in w.skip.iter().chain(w.limit.iter()) {
        scan_expr(e, out);
    }
}

fn scan_return(r: &ReturnClause, out: &mut HashSet<String>) {
    for item in &r.items {
        scan_expr(&item.expr, out);
    }
    for o in &r.order_by {
        scan_expr(&o.expr, out);
    }
    for e in r.skip.iter().chain(r.limit.iter()) {
        scan_expr(e, out);
    }
}

fn scan_pattern(p: &Pattern, out: &mut HashSet<String>) {
    for part in &p.parts {
        scan_part(part, out);
    }
}

fn scan_part(part: &PatternPart, out: &mut HashSet<String>) {
    scan_node(&part.head, out);
    for (rel, node) in &part.tail {
        scan_rel(rel, out);
        scan_node(node, out);
    }
}

fn scan_node(n: &NodePattern, out: &mut HashSet<String>) {
    if let Some(props) = &n.props {
        scan_map_lit(props, out);
    }
}

fn scan_rel(r: &RelPattern, out: &mut HashSet<String>) {
    if let Some(props) = &r.props {
        scan_map_lit(props, out);
    }
}

fn scan_map_lit(m: &MapLit, out: &mut HashSet<String>) {
    for (_, e) in &m.entries {
        scan_expr(e, out);
    }
}

fn scan_expr(e: &Expr, out: &mut HashSet<String>) {
    match e {
        Expr::Var(_) | Expr::Lit(_) | Expr::Param(_) => {}
        Expr::Prop(target, _, _) => {
            collect_vars(target, out);
            scan_expr(target, out);
        }
        Expr::Call(c) => {
            let name = c.name.to_ascii_lowercase();
            if name == "properties" || name == "keys" {
                for a in &c.args {
                    collect_vars(a, out);
                }
            }
            for a in &c.args {
                scan_expr(a, out);
            }
        }
        Expr::MapProjection(mp) => {
            out.insert(mp.var.name.clone());
            for sel in &mp.selectors {
                if let MapProjectionSelector::Literal(_, e) = sel {
                    scan_expr(e, out);
                }
            }
        }
        Expr::BinOp(_, l, r, _)
        | Expr::In(l, r, _)
        | Expr::StartsWith(l, r, _)
        | Expr::EndsWith(l, r, _)
        | Expr::Contains(l, r, _)
        | Expr::Index(l, r, _) => {
            scan_expr(l, out);
            scan_expr(r, out);
        }
        Expr::UnaryOp(_, inner, _) | Expr::IsNull(inner, _) | Expr::IsNotNull(inner, _) => {
            scan_expr(inner, out);
        }
        Expr::Case(c) => scan_case(c, out),
        Expr::Exists(pattern, where_clause, _) => {
            scan_pattern(pattern, out);
            if let Some(w) = where_clause {
                scan_expr(w, out);
            }
        }
        Expr::List(items, _) => {
            for i in items {
                scan_expr(i, out);
            }
        }
        Expr::Map(entries, _) => {
            for (_, v) in entries {
                scan_expr(v, out);
            }
        }
        Expr::ListComprehension(lc) => {
            scan_expr(&lc.list, out);
            if let Some(f) = &lc.filter {
                scan_expr(f, out);
            }
            if let Some(m) = &lc.map {
                scan_expr(m, out);
            }
        }
        Expr::Reduce(r) => {
            scan_expr(&r.init, out);
            scan_expr(&r.list, out);
            scan_expr(&r.body, out);
        }
        Expr::ListPredicate(p) => {
            scan_expr(&p.list, out);
            scan_expr(&p.predicate, out);
        }
        Expr::PatternComprehension(pc) => {
            scan_pattern(&pc.pattern, out);
            if let Some(f) = &pc.filter {
                scan_expr(f, out);
            }
            scan_expr(&pc.projection, out);
        }
    }
}

fn scan_case(c: &CaseExpr, out: &mut HashSet<String>) {
    if let Some(s) = &c.subject {
        scan_expr(s, out);
    }
    for (w, t) in &c.branches {
        scan_expr(w, out);
        scan_expr(t, out);
    }
    if let Some(e) = &c.else_branch {
        scan_expr(e, out);
    }
}

/// Every variable name occurring anywhere in `e`.
fn collect_vars(e: &Expr, out: &mut HashSet<String>) {
    if let Expr::Var(v) = e {
        out.insert(v.name.clone());
        return;
    }
    // Reuse the structural walk: a nested Prop/properties target inside is
    // already collected by `scan_expr`; here we need *all* vars, so walk
    // manually over the same shapes.
    match e {
        Expr::Var(_) | Expr::Lit(_) | Expr::Param(_) => {}
        Expr::Prop(t, _, _) => collect_vars(t, out),
        Expr::Call(c) => {
            for a in &c.args {
                collect_vars(a, out);
            }
        }
        Expr::MapProjection(mp) => {
            out.insert(mp.var.name.clone());
        }
        Expr::BinOp(_, l, r, _)
        | Expr::In(l, r, _)
        | Expr::StartsWith(l, r, _)
        | Expr::EndsWith(l, r, _)
        | Expr::Contains(l, r, _)
        | Expr::Index(l, r, _) => {
            collect_vars(l, out);
            collect_vars(r, out);
        }
        Expr::UnaryOp(_, inner, _) | Expr::IsNull(inner, _) | Expr::IsNotNull(inner, _) => {
            collect_vars(inner, out);
        }
        Expr::Case(c) => {
            if let Some(s) = &c.subject {
                collect_vars(s, out);
            }
            for (w, t) in &c.branches {
                collect_vars(w, out);
                collect_vars(t, out);
            }
            if let Some(el) = &c.else_branch {
                collect_vars(el, out);
            }
        }
        Expr::Exists(_, _, _) => {}
        Expr::List(items, _) => {
            for i in items {
                collect_vars(i, out);
            }
        }
        Expr::Map(entries, _) => {
            for (_, v) in entries {
                collect_vars(v, out);
            }
        }
        Expr::ListComprehension(lc) => {
            collect_vars(&lc.list, out);
        }
        Expr::Reduce(r) => {
            collect_vars(&r.init, out);
            collect_vars(&r.list, out);
        }
        Expr::ListPredicate(p) => {
            collect_vars(&p.list, out);
        }
        Expr::PatternComprehension(_) => {}
    }
}
