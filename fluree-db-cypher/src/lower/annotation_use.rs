//! Per-scope scan for variables whose evaluation depends on the *annotation
//! identity* of a relationship.
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
//! **This scan must be exact, not conservative in either direction.** Both
//! errors are silent wrong answers, not merely slower plans:
//!
//! - A **false positive** makes lowering pick the bare `Pattern::EdgeAnnotation`
//!   lane, which matches only *reified* edges — a strictly smaller result set.
//!   Plain-RDF rows disappear with no signal.
//! - A **false negative** makes lowering pick the plain-triple + `Coalesce`
//!   lane, where the variable binds a synthesized relationship value on an
//!   unreified edge; a later `e.prop` triple then fails to constrain on that
//!   non-SID subject and reports a property value belonging to some *other*
//!   edge's annotation.
//!
//! Both were reproduced against this scan (see
//! `it_query_cypher::cypher_union_branch_rel_var_name_collision_keeps_both_branches`
//! and `cypher_untyped_rel_prop_read_matches_reified_edges_only`).
//!
//! Scoping therefore follows Cypher's own rules rather than the whole
//! statement:
//!
//! - **UNION branches are independent scopes.** `r` in one branch is a
//!   different variable from `r` in the next, so a branch's reads never reach
//!   its siblings. Scanning the whole statement into one set is what made a
//!   name collision across `UNION` drop rows from the *other* branch.
//! - **A `CALL { … }` body can reference imported outer variables**, which
//!   resolve to the same `VarId` through the shared registry. Its reads
//!   therefore propagate *up* into the enclosing scope, where the relationship
//!   pattern that binds the name is lowered.
//!
//! The one binding form the scan does model is the **loop-local** variable of a
//! list iteration (`all(x IN L …)`, `[x IN L | …]`, `reduce(… x IN L …)`):
//! `lower/expr.rs` binds it with `bind_local`, so it can never name a row
//! variable, and its name is dropped at the iteration boundary rather than
//! escaping into the enclosing scope.
//!
//! Residual, stated rather than fixed: a `WITH` that rebinds a name inside one
//! branch still false-positives, and a name bound *inside* a `CALL` body that
//! shadows an outer relationship variable propagates up. Both need a
//! scope-chain walk keyed on binding sites rather than on names.

use std::collections::HashSet;

use crate::ast::{
    CaseExpr, Expr, MapLit, MapProjectionSelector, NodePattern, Pattern, PatternPart, Query,
    ReadClause, RelPattern, ReturnClause, WithClause,
};

/// The variable-name sets one `Query` scope contributes to lowering.
#[derive(Debug, Default, Clone)]
pub(super) struct ScopeUses {
    /// Names read on the relationship annotation surface in this scope.
    pub(super) annotation: HashSet<String>,
    /// Names whose list *elements* are read on the annotation surface:
    /// `all(x IN rs WHERE x.p)`, `[x IN tail(rs) | x.p]`, `reduce(… x IN rs …)`,
    /// `UNWIND rs AS x … x.p`, `properties(rs[0])` / `keys(head(rs))`, and any
    /// of those reached through a `WITH … rs AS xs` rename.
    ///
    /// A variable-length relationship variable binds a *list*, so this — not
    /// [`Self::annotation`] — is what says whether its elements need per-hop
    /// edge identity.
    pub(super) element_property: HashSet<String>,
}

/// Scan one `Query` scope.
///
/// Recurses into `CALL { … }` bodies — they may read imported outer variables,
/// and the relationship pattern binding such a name is lowered in *this* scope
/// — but NOT into `union_tail`: each UNION branch is an independent scope and
/// is scanned when it is itself lowered.
pub(super) fn scope_uses(q: &Query) -> ScopeUses {
    let mut out = ScopeUses::default();
    scan_query(q, &mut out);
    // `UNWIND rs AS r … r.p` reads an element property through a *row*
    // variable, so it never passes through a list-iteration expression. Match
    // it up after the fact: the alias is now in `annotation` if its properties
    // were read anywhere in this scope, and the list it came from is whatever
    // the UNWIND expression names.
    let mut unwinds = Vec::new();
    collect_unwind_aliases(q, &mut unwinds);
    // `WITH b, rs AS xs WHERE all(r IN xs …)` renames the *list itself*, so the
    // scan attributes the element reads to `xs` and the pattern that binds `rs`
    // never sees them — the refusal is bypassed and the read is lowered onto a
    // route that answers `[]`. Map each alias back to the variables its
    // expression names, using the same `collect_rel_list_vars` walk, so
    // `WITH nodes(p) AS ns` still contributes nothing.
    let mut with_aliases = Vec::new();
    collect_with_aliases(q, &mut with_aliases);
    // To a fixpoint: an alias of an alias (`rs AS xs` … `xs AS ys`) and a
    // `WITH` alias later consumed by an `UNWIND` both need the renames to
    // compose, and clause order does not guarantee one pass resolves them.
    // Each pass only ever inserts, over a name set bounded by the statement's
    // own identifiers, so this terminates.
    loop {
        let before = out.element_property.len();
        for (alias, list_vars) in &unwinds {
            if out.annotation.contains(alias) {
                out.element_property.extend(list_vars.iter().cloned());
            }
        }
        for (alias, list_vars) in &with_aliases {
            if out.element_property.contains(alias) {
                out.element_property.extend(list_vars.iter().cloned());
            }
        }
        if out.element_property.len() == before {
            break;
        }
    }
    out
}

/// Every `(alias, variables-named-by-the-aliased-expression)` pair for the
/// `WITH … AS alias` items in this scope. Same recursion rule as
/// [`scan_query`].
fn collect_with_aliases(q: &Query, out: &mut Vec<(String, HashSet<String>)>) {
    for c in &q.clauses {
        match c {
            ReadClause::With(w) => {
                for item in &w.items {
                    if let Some(alias) = &item.alias {
                        let mut list_vars = HashSet::new();
                        collect_rel_list_vars(&item.expr, &mut list_vars);
                        out.push((alias.name.clone(), list_vars));
                    }
                }
            }
            ReadClause::CallSubquery(cs) => collect_with_aliases(&cs.query, out),
            _ => {}
        }
    }
}

/// Every `(alias, variables-named-by-the-list-expression)` pair for the UNWIND
/// clauses in this scope. Same recursion rule as [`scan_query`].
fn collect_unwind_aliases(q: &Query, out: &mut Vec<(String, HashSet<String>)>) {
    for c in &q.clauses {
        match c {
            ReadClause::Unwind(u) => {
                let mut list_vars = HashSet::new();
                collect_rel_list_vars(&u.expr, &mut list_vars);
                out.push((u.alias.name.clone(), list_vars));
            }
            ReadClause::CallSubquery(cs) => collect_unwind_aliases(&cs.query, out),
            _ => {}
        }
    }
}

fn scan_query(q: &Query, out: &mut ScopeUses) {
    for c in &q.clauses {
        scan_read_clause(c, out);
    }
    scan_return(&q.return_clause, out);
}

fn scan_read_clause(c: &ReadClause, out: &mut ScopeUses) {
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

fn scan_with(w: &WithClause, out: &mut ScopeUses) {
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

fn scan_return(r: &ReturnClause, out: &mut ScopeUses) {
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

fn scan_pattern(p: &Pattern, out: &mut ScopeUses) {
    for part in &p.parts {
        scan_part(part, out);
    }
}

fn scan_part(part: &PatternPart, out: &mut ScopeUses) {
    scan_node(&part.head, out);
    for (rel, node) in &part.tail {
        scan_rel(rel, out);
        scan_node(node, out);
    }
}

fn scan_node(n: &NodePattern, out: &mut ScopeUses) {
    if let Some(props) = &n.props {
        scan_map_lit(props, out);
    }
}

fn scan_rel(r: &RelPattern, out: &mut ScopeUses) {
    if let Some(props) = &r.props {
        scan_map_lit(props, out);
    }
}

fn scan_map_lit(m: &MapLit, out: &mut ScopeUses) {
    for (_, e) in &m.entries {
        scan_expr(e, out);
    }
}

fn scan_expr(e: &Expr, out: &mut ScopeUses) {
    match e {
        Expr::Var(_) | Expr::Lit(_) | Expr::Param(_) => {}
        Expr::Prop(target, _, _) => {
            collect_vars(target, &mut out.annotation);
            scan_expr(target, out);
        }
        Expr::Call(c) => {
            let name = c.name.to_ascii_lowercase();
            if name == "properties" || name == "keys" {
                for a in &c.args {
                    collect_vars(a, &mut out.annotation);
                    // `properties(rs[0])` / `keys(head(relationships(p)))` read
                    // an *element* of a relationship list, so they are
                    // element-property reads — the conclusion
                    // `scan_list_iteration` draws for a loop body, reached here
                    // without a loop variable. `annotation` alone cannot carry
                    // it: that set is keyed on row variables and is inert for
                    // the list or path variable named here, so the read would
                    // slip past both identity guards and answer null.
                    //
                    // Additive — `annotation` above is left exactly as it was,
                    // so no lane selection changes. `element_property`'s only
                    // consumers are the two refusals.
                    if let Some(list) = element_extraction_source(a) {
                        collect_rel_list_vars(list, &mut out.element_property);
                    }
                }
            }
            for a in &c.args {
                scan_expr(a, out);
            }
        }
        Expr::MapProjection(mp) => {
            out.annotation.insert(mp.var.name.clone());
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
        | Expr::RegexMatch(l, r, _)
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
            let bodies: Vec<&Expr> = lc
                .filter
                .iter()
                .chain(lc.map.iter())
                .map(|b| &**b)
                .collect();
            scan_list_iteration(&lc.var.name, &lc.list, &bodies, out);
        }
        Expr::Reduce(r) => {
            scan_expr(&r.init, out);
            scan_list_iteration(&r.var.name, &r.list, &[&r.body], out);
        }
        Expr::ListPredicate(p) => {
            scan_list_iteration(&p.var.name, &p.list, &[&p.predicate], out);
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

/// Scan a list-iteration form — `all/any/none/single(x IN L WHERE …)`,
/// `[x IN L | …]`, `reduce(… x IN L …)`.
///
/// The body evaluates in a loop-local scope (`lower/expr.rs` binds `x` with
/// `bind_local`, so `x.p` lowers to an eval-time `Expression::Member` rather
/// than a graph join). A property read on `x` there is therefore a read of
/// **L's elements**, not of a row variable — so every variable `L` names is
/// element-property dependent.
///
/// Taking the variables of the whole list expression, rather than requiring
/// `L` to be a bare `Expr::Var`, is what covers `relationships(p)`, `tail(rs)`
/// and `reverse(rs)`. It over-approximates for a list expression that names
/// several variables, which is safe here: the only consumer is a refusal, so
/// the cost of a false positive is an actionable error rather than a wrong
/// answer.
fn scan_list_iteration(var: &str, list: &Expr, bodies: &[&Expr], out: &mut ScopeUses) {
    scan_expr(list, out);
    let mut body_uses = ScopeUses::default();
    for b in bodies {
        scan_expr(b, &mut body_uses);
    }
    if body_uses.annotation.contains(var) {
        collect_rel_list_vars(list, &mut out.element_property);
    }
    // The loop variable is loop-local and can never name a row variable, so
    // its name must not escape into the enclosing scope's annotation set: a
    // same-named relationship variable elsewhere in the scope would be lowered
    // annotation-dependent and silently drop its unreified rows. Removing it
    // after the `contains` check above keeps the element-property conclusion
    // this body just licensed. A nested iteration that shadows the same name
    // has already removed its own binding by the time we see its uses, so what
    // remains here belongs to *this* loop variable.
    body_uses.annotation.remove(var);
    out.annotation.extend(body_uses.annotation);
    out.element_property.extend(body_uses.element_property);
}

fn scan_case(c: &CaseExpr, out: &mut ScopeUses) {
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

/// The list expression an *element extraction* draws from, if `e` is one.
///
/// `rs[0]`, `head(rs)` and `last(relationships(p))` each yield a single
/// relationship out of a list. A read on the result is a read of that list's
/// elements, so the list's variables — not the extraction expression — are
/// what needs per-hop edge identity.
///
/// Deliberately narrow. `tail(rs)` and a bare `rs` yield a *list*, not an
/// element, so `properties(rs)` stays unmarked: it is not an element read, and
/// answering it with the identity refusal would name a remedy that has nothing
/// to do with what was asked.
fn element_extraction_source(e: &Expr) -> Option<&Expr> {
    match e {
        Expr::Index(list, _, _) => Some(list),
        Expr::Call(c) if matches!(c.name.to_ascii_lowercase().as_str(), "head" | "last") => {
            c.args.first()
        }
        _ => None,
    }
}

/// The variables whose **relationship** elements a list expression yields.
///
/// `nodes(p)` yields node SIDs — real subjects, whose properties read
/// correctly on every route — so it contributes nothing and
/// `[n IN nodes(p) | n.name]` stays allowed. `relationships(p)`, `tail(rs)`,
/// `reverse(rs)` and a bare `rs` all yield relationships and contribute their
/// variables.
fn collect_rel_list_vars(e: &Expr, out: &mut HashSet<String>) {
    match e {
        Expr::Call(c) if c.name.eq_ignore_ascii_case("nodes") => {}
        Expr::Call(c) => {
            for a in &c.args {
                collect_rel_list_vars(a, out);
            }
        }
        other => collect_vars(other, out),
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
        | Expr::RegexMatch(l, r, _)
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
