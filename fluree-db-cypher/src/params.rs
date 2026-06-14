//! Parameter substitution.
//!
//! Cypher `$name` references are replaced with literal / list expressions
//! drawn from a request-supplied params map **before** lowering, so the read
//! and write lowering paths only ever see concrete literals. This keeps the
//! substitution rules in one place rather than threading a params map through
//! two separate lowering contexts (read in this crate, write in
//! `fluree-db-transact`).
//!
//! v1 parameter values are scalars (string / number / bool / null) or flat
//! lists of scalars (for `UNWIND $list`). Map/object parameters and nested
//! collections are rejected with a clear error.

use serde_json::Value as JsonValue;

use crate::ast::{
    CaseExpr, CreateClause, CypherAst, Expr, Literal, MapLit, MatchClause, NodePattern, Pattern,
    Query, ReadClause, RelPattern, ReturnClause, SetItem, Statement, Update, Variable, WriteClause,
};
use crate::span::SourceSpan;

/// Map of parameter name → JSON value, as supplied in the request envelope.
pub type ParamMap = serde_json::Map<String, JsonValue>;

/// Error raised while substituting `$param` references.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParamError {
    /// A `$name` reference had no matching entry in the params map.
    Missing(String),
    /// A param value's JSON shape isn't supported in v1.
    Unsupported { name: String, reason: String },
}

impl std::fmt::Display for ParamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParamError::Missing(name) => {
                write!(f, "missing value for parameter `${name}`")
            }
            ParamError::Unsupported { name, reason } => {
                write!(f, "parameter `${name}` is unsupported: {reason}")
            }
        }
    }
}

impl std::error::Error for ParamError {}

/// Replace every `$param` reference in the AST with the corresponding value
/// from `params`. No-op when `params` is empty (but still errors if the query
/// references a `$param` that isn't supplied).
pub fn substitute_params(ast: &mut CypherAst, params: &ParamMap) -> Result<(), ParamError> {
    // Compile-time unroll of `UNWIND $list AS row CREATE (...)` (pure node
    // batch) and the VALUES desugaring of `UNWIND $list AS row MATCH … CREATE`
    // (batched edge insert) both run first, so the generic substitution below
    // never sees the list-of-maps parameter it would otherwise reject.
    expand_unwind_create(ast, params)?;
    expand_unwind_match(ast, params)?;
    subst_statement(&mut ast.statement, params)
}

fn unsupported_param(name: &str, reason: &str) -> ParamError {
    ParamError::Unsupported {
        name: name.to_string(),
        reason: reason.to_string(),
    }
}

/// Unroll `UNWIND $list AS row CREATE (...)` into a literal multi-pattern
/// CREATE when `$list` is a constant array parameter (the standard driver
/// batched-insert shape). Each element — a scalar or a flat map — becomes its
/// own CREATE with the `row` alias resolved to literals (`row.field` → the
/// map's value; a bare scalar `row` → the value). Node/relationship variables
/// are suffixed per row so every element creates **distinct** nodes (otherwise
/// the shared blank-node id would collapse them into one).
///
/// Only fires for the pure-`UNWIND` + CREATE-only shape. A leading MATCH
/// (edge loading) or any non-CREATE write is left untouched for the generic
/// path / VALUES desugaring.
fn expand_unwind_create(ast: &mut CypherAst, params: &ParamMap) -> Result<(), ParamError> {
    let (alias, pname) = {
        let Statement::Update(u) = &ast.statement else {
            return Ok(());
        };
        if u.read_clauses.len() != 1 {
            return Ok(());
        }
        let ReadClause::Unwind(unwind) = &u.read_clauses[0] else {
            return Ok(());
        };
        let Expr::Param(pref) = &unwind.expr else {
            return Ok(());
        };
        if !u
            .write_clauses
            .iter()
            .all(|w| matches!(w, WriteClause::Create(_)))
        {
            return Ok(());
        }
        (unwind.alias.name.clone(), pref.name.clone())
    };

    let val = params
        .get(&pname)
        .ok_or_else(|| ParamError::Missing(pname.clone()))?;
    let JsonValue::Array(elems) = val else {
        return Err(unsupported_param(&pname, "UNWIND parameter must be a list"));
    };

    let Statement::Update(u) = &mut ast.statement else {
        unreachable!("checked above");
    };
    let creates: Vec<CreateClause> = u
        .write_clauses
        .iter()
        .filter_map(|w| match w {
            WriteClause::Create(c) => Some(c.clone()),
            _ => None,
        })
        .collect();

    let mut new_writes: Vec<WriteClause> = Vec::with_capacity(elems.len() * creates.len());
    let mut anon = 0u32;
    for (i, elem) in elems.iter().enumerate() {
        for c in &creates {
            let mut cloned = c.clone();
            replace_alias_in_pattern(&mut cloned.pattern, &alias, elem, &pname)?;
            rename_pattern_vars(&mut cloned.pattern, i, &mut anon);
            new_writes.push(WriteClause::Create(cloned));
        }
    }
    u.read_clauses.clear();
    u.write_clauses = new_writes;
    Ok(())
}

/// Desugar `UNWIND $list AS row <MATCH …> CREATE/SET …` (the batched **edge**
/// insert) into a constant `InlineRows` (→ a `VALUES` join) plus `row.field`
/// rewritten to per-column variables. Unlike the pure-CREATE node batch this
/// can't unroll — each row's MATCH must stay independent, so a missing id drops
/// only its own row. Reuses the existing VALUES + MATCH…CREATE machinery.
///
/// Fires only when an `UNWIND $param AS row` clause coexists with a MATCH; the
/// pure-CREATE shape is handled by [`expand_unwind_create`] before this.
fn expand_unwind_match(ast: &mut CypherAst, params: &ParamMap) -> Result<(), ParamError> {
    let (unwind_idx, alias, pname, span) = {
        let Statement::Update(u) = &ast.statement else {
            return Ok(());
        };
        let mut found = None;
        for (i, c) in u.read_clauses.iter().enumerate() {
            if let ReadClause::Unwind(uw) = c {
                if let Expr::Param(pref) = &uw.expr {
                    found = Some((i, uw.alias.name.clone(), pref.name.clone(), uw.span));
                    break;
                }
            }
        }
        let Some(found) = found else {
            return Ok(());
        };
        let has_match = u
            .read_clauses
            .iter()
            .any(|c| matches!(c, ReadClause::Match(_) | ReadClause::OptionalMatch(_)));
        if !has_match {
            return Ok(());
        }
        found
    };

    let val = params
        .get(&pname)
        .ok_or_else(|| ParamError::Missing(pname.clone()))?;
    let JsonValue::Array(elems) = val else {
        return Err(unsupported_param(&pname, "UNWIND parameter must be a list"));
    };

    // Which fields of `row` are referenced, and is the bare alias used?
    let (fields, bare_used) = {
        let Statement::Update(u) = &ast.statement else {
            unreachable!("checked above");
        };
        let mut fields = Vec::new();
        let mut bare = false;
        collect_alias_in_update(u, &alias, unwind_idx, &mut fields, &mut bare);
        (fields, bare)
    };
    if bare_used && !fields.is_empty() {
        return Err(unsupported_param(
            &pname,
            "cannot use an UNWIND element both as a whole value (`row`) and by field (`row.x`)",
        ));
    }
    if !bare_used && fields.is_empty() {
        return Err(unsupported_param(
            &pname,
            "UNWIND element is never referenced — use `row.field` to bind values",
        ));
    }

    let col_var = |field: &str| format!("__cyrow_{alias}_{field}");
    let bare_var = format!("__cyrow_{alias}");

    // One VALUES row per element, columns aligned to `fields` (or one bare cell).
    let mut rows: Vec<Vec<Expr>> = Vec::with_capacity(elems.len());
    for elem in elems {
        let mut row = Vec::new();
        if bare_used {
            match elem {
                JsonValue::Object(_) | JsonValue::Array(_) => {
                    return Err(unsupported_param(
                        &pname,
                        "using the whole UNWIND element as a value is deferred — reference fields like `row.field`",
                    ))
                }
                _ => row.push(json_scalar_to_expr(elem, &pname, span)?),
            }
        } else {
            let JsonValue::Object(map) = elem else {
                return Err(unsupported_param(
                    &pname,
                    "property access (`row.field`) requires the UNWIND list to contain maps",
                ));
            };
            for f in &fields {
                let fv = map.get(f).cloned().unwrap_or(JsonValue::Null);
                row.push(json_scalar_to_expr(&fv, &pname, span)?);
            }
        }
        rows.push(row);
    }

    let vars: Vec<Variable> = if bare_used {
        vec![Variable {
            name: bare_var.clone(),
            span,
        }]
    } else {
        fields
            .iter()
            .map(|f| Variable {
                name: col_var(f),
                span,
            })
            .collect()
    };

    let Statement::Update(u) = &mut ast.statement else {
        unreachable!("checked above");
    };
    let bare_ref = if bare_used {
        Some(bare_var.as_str())
    } else {
        None
    };
    rewrite_alias_in_update(u, &alias, unwind_idx, &col_var, bare_ref);
    u.read_clauses[unwind_idx] = ReadClause::InlineRows { vars, rows };
    Ok(())
}

// ---- alias collection (which `row.field`s are referenced) -------------------

fn collect_alias_in_update(
    u: &Update,
    alias: &str,
    skip_idx: usize,
    fields: &mut Vec<String>,
    bare: &mut bool,
) {
    for (i, c) in u.read_clauses.iter().enumerate() {
        if i == skip_idx {
            continue;
        }
        if let ReadClause::Match(m) | ReadClause::OptionalMatch(m) = c {
            collect_alias_in_pattern(&m.pattern, alias, fields, bare);
            if let Some(w) = &m.where_clause {
                collect_alias_in_expr(w, alias, fields, bare);
            }
        }
    }
    for w in &u.write_clauses {
        match w {
            WriteClause::Create(c) => collect_alias_in_pattern(&c.pattern, alias, fields, bare),
            WriteClause::Merge(m) => {
                collect_alias_in_pattern(&m.pattern, alias, fields, bare);
                for s in m.on_create.iter().chain(&m.on_match) {
                    collect_alias_in_set_item(s, alias, fields, bare);
                }
            }
            WriteClause::Set(s) => {
                for it in &s.items {
                    collect_alias_in_set_item(it, alias, fields, bare);
                }
            }
            WriteClause::Remove(_) | WriteClause::Delete(_) => {}
        }
    }
}

fn collect_alias_in_pattern(pat: &Pattern, alias: &str, fields: &mut Vec<String>, bare: &mut bool) {
    for part in &pat.parts {
        collect_alias_in_props(&part.head.props, alias, fields, bare);
        for (rel, node) in &part.tail {
            collect_alias_in_props(&rel.props, alias, fields, bare);
            collect_alias_in_props(&node.props, alias, fields, bare);
        }
    }
}

fn collect_alias_in_props(
    props: &Option<MapLit>,
    alias: &str,
    fields: &mut Vec<String>,
    bare: &mut bool,
) {
    if let Some(m) = props {
        for (_, e) in &m.entries {
            collect_alias_in_expr(e, alias, fields, bare);
        }
    }
}

fn collect_alias_in_set_item(s: &SetItem, alias: &str, fields: &mut Vec<String>, bare: &mut bool) {
    match s {
        SetItem::Property { value, .. } => collect_alias_in_expr(value, alias, fields, bare),
        SetItem::MapMerge { map, .. } | SetItem::MapReplace { map, .. } => {
            for (_, e) in &map.entries {
                collect_alias_in_expr(e, alias, fields, bare);
            }
        }
        SetItem::Labels { .. } => {}
    }
}

fn collect_alias_in_expr(e: &Expr, alias: &str, fields: &mut Vec<String>, bare: &mut bool) {
    match e {
        Expr::Prop(inner, field, _) => {
            if matches!(inner.as_ref(), Expr::Var(v) if v.name == alias) {
                if !fields.iter().any(|x| x == field) {
                    fields.push(field.clone());
                }
            } else {
                collect_alias_in_expr(inner, alias, fields, bare);
            }
        }
        Expr::Var(v) => {
            if v.name == alias {
                *bare = true;
            }
        }
        Expr::BinOp(_, l, r, _)
        | Expr::In(l, r, _)
        | Expr::StartsWith(l, r, _)
        | Expr::EndsWith(l, r, _)
        | Expr::Contains(l, r, _) => {
            collect_alias_in_expr(l, alias, fields, bare);
            collect_alias_in_expr(r, alias, fields, bare);
        }
        Expr::UnaryOp(_, x, _) | Expr::IsNull(x, _) | Expr::IsNotNull(x, _) => {
            collect_alias_in_expr(x, alias, fields, bare);
        }
        Expr::Call(c) => {
            for a in &c.args {
                collect_alias_in_expr(a, alias, fields, bare);
            }
        }
        Expr::List(items, _) => {
            for it in items {
                collect_alias_in_expr(it, alias, fields, bare);
            }
        }
        Expr::Lit(_) | Expr::Param(_) | Expr::Case(_) | Expr::Exists(_, _) => {}
    }
}

// ---- alias rewrite (`row.field` → column var, bare `row` → bare var) --------

fn rewrite_alias_in_update<F: Fn(&str) -> String>(
    u: &mut Update,
    alias: &str,
    skip_idx: usize,
    col_var: &F,
    bare_var: Option<&str>,
) {
    for (i, c) in u.read_clauses.iter_mut().enumerate() {
        if i == skip_idx {
            continue;
        }
        if let ReadClause::Match(m) | ReadClause::OptionalMatch(m) = c {
            rewrite_alias_in_pattern(&mut m.pattern, alias, col_var, bare_var);
            if let Some(w) = &mut m.where_clause {
                rewrite_alias_in_expr_to_var(w, alias, col_var, bare_var);
            }
        }
    }
    for w in &mut u.write_clauses {
        match w {
            WriteClause::Create(c) => {
                rewrite_alias_in_pattern(&mut c.pattern, alias, col_var, bare_var);
            }
            WriteClause::Merge(m) => {
                rewrite_alias_in_pattern(&mut m.pattern, alias, col_var, bare_var);
                for s in m.on_create.iter_mut().chain(&mut m.on_match) {
                    rewrite_alias_in_set_item(s, alias, col_var, bare_var);
                }
            }
            WriteClause::Set(s) => {
                for it in &mut s.items {
                    rewrite_alias_in_set_item(it, alias, col_var, bare_var);
                }
            }
            WriteClause::Remove(_) | WriteClause::Delete(_) => {}
        }
    }
}

fn rewrite_alias_in_pattern<F: Fn(&str) -> String>(
    pat: &mut Pattern,
    alias: &str,
    col_var: &F,
    bare_var: Option<&str>,
) {
    for part in &mut pat.parts {
        rewrite_alias_in_props(&mut part.head.props, alias, col_var, bare_var);
        for (rel, node) in &mut part.tail {
            rewrite_alias_in_props(&mut rel.props, alias, col_var, bare_var);
            rewrite_alias_in_props(&mut node.props, alias, col_var, bare_var);
        }
    }
}

fn rewrite_alias_in_props<F: Fn(&str) -> String>(
    props: &mut Option<MapLit>,
    alias: &str,
    col_var: &F,
    bare_var: Option<&str>,
) {
    if let Some(m) = props {
        for (_, e) in &mut m.entries {
            rewrite_alias_in_expr_to_var(e, alias, col_var, bare_var);
        }
    }
}

fn rewrite_alias_in_set_item<F: Fn(&str) -> String>(
    s: &mut SetItem,
    alias: &str,
    col_var: &F,
    bare_var: Option<&str>,
) {
    match s {
        SetItem::Property { value, .. } => {
            rewrite_alias_in_expr_to_var(value, alias, col_var, bare_var);
        }
        SetItem::MapMerge { map, .. } | SetItem::MapReplace { map, .. } => {
            for (_, e) in &mut map.entries {
                rewrite_alias_in_expr_to_var(e, alias, col_var, bare_var);
            }
        }
        SetItem::Labels { .. } => {}
    }
}

fn rewrite_alias_in_expr_to_var<F: Fn(&str) -> String>(
    e: &mut Expr,
    alias: &str,
    col_var: &F,
    bare_var: Option<&str>,
) {
    match e {
        Expr::Prop(inner, field, span) => {
            if matches!(inner.as_ref(), Expr::Var(v) if v.name == alias) {
                *e = Expr::Var(Variable {
                    name: col_var(field),
                    span: *span,
                });
            } else {
                rewrite_alias_in_expr_to_var(inner, alias, col_var, bare_var);
            }
        }
        Expr::Var(v) if v.name == alias => {
            if let Some(bv) = bare_var {
                v.name = bv.to_string();
            }
        }
        Expr::BinOp(_, l, r, _)
        | Expr::In(l, r, _)
        | Expr::StartsWith(l, r, _)
        | Expr::EndsWith(l, r, _)
        | Expr::Contains(l, r, _) => {
            rewrite_alias_in_expr_to_var(l, alias, col_var, bare_var);
            rewrite_alias_in_expr_to_var(r, alias, col_var, bare_var);
        }
        Expr::UnaryOp(_, x, _) | Expr::IsNull(x, _) | Expr::IsNotNull(x, _) => {
            rewrite_alias_in_expr_to_var(x, alias, col_var, bare_var);
        }
        Expr::Call(c) => {
            for a in &mut c.args {
                rewrite_alias_in_expr_to_var(a, alias, col_var, bare_var);
            }
        }
        Expr::List(items, _) => {
            for it in items {
                rewrite_alias_in_expr_to_var(it, alias, col_var, bare_var);
            }
        }
        Expr::Var(_) | Expr::Lit(_) | Expr::Param(_) | Expr::Case(_) | Expr::Exists(_, _) => {}
    }
}

fn replace_alias_in_pattern(
    pat: &mut Pattern,
    alias: &str,
    elem: &JsonValue,
    pname: &str,
) -> Result<(), ParamError> {
    for part in &mut pat.parts {
        replace_alias_in_node(&mut part.head, alias, elem, pname)?;
        for (rel, node) in &mut part.tail {
            if let Some(m) = &mut rel.props {
                replace_alias_in_maplit(m, alias, elem, pname)?;
            }
            replace_alias_in_node(node, alias, elem, pname)?;
        }
    }
    Ok(())
}

fn replace_alias_in_node(
    n: &mut NodePattern,
    alias: &str,
    elem: &JsonValue,
    pname: &str,
) -> Result<(), ParamError> {
    if let Some(m) = &mut n.props {
        replace_alias_in_maplit(m, alias, elem, pname)?;
    }
    Ok(())
}

fn replace_alias_in_maplit(
    m: &mut MapLit,
    alias: &str,
    elem: &JsonValue,
    pname: &str,
) -> Result<(), ParamError> {
    for (_, e) in &mut m.entries {
        replace_alias_in_expr(e, alias, elem, pname)?;
    }
    Ok(())
}

fn replace_alias_in_expr(
    e: &mut Expr,
    alias: &str,
    elem: &JsonValue,
    pname: &str,
) -> Result<(), ParamError> {
    match e {
        // `row.field` → the element map's value for `field` (missing → null).
        Expr::Prop(inner, field, span) => {
            if matches!(inner.as_ref(), Expr::Var(v) if v.name == alias) {
                let JsonValue::Object(map) = elem else {
                    return Err(unsupported_param(
                        pname,
                        "property access (`row.field`) requires the UNWIND list to contain maps",
                    ));
                };
                let fv = map.get(field).cloned().unwrap_or(JsonValue::Null);
                *e = json_scalar_to_expr(&fv, pname, *span)?;
                Ok(())
            } else {
                replace_alias_in_expr(inner, alias, elem, pname)
            }
        }
        // Bare `row` as a value: a scalar element substitutes directly; a map
        // element used whole is deferred (no map-valued property in v1).
        Expr::Var(v) if v.name == alias => match elem {
            JsonValue::Object(_) | JsonValue::Array(_) => Err(unsupported_param(
                pname,
                "using the whole UNWIND element as a value is deferred — reference fields like `row.field`",
            )),
            _ => {
                *e = json_scalar_to_expr(elem, pname, v.span)?;
                Ok(())
            }
        },
        Expr::BinOp(_, l, r, _)
        | Expr::In(l, r, _)
        | Expr::StartsWith(l, r, _)
        | Expr::EndsWith(l, r, _)
        | Expr::Contains(l, r, _) => {
            replace_alias_in_expr(l, alias, elem, pname)?;
            replace_alias_in_expr(r, alias, elem, pname)
        }
        Expr::UnaryOp(_, x, _) | Expr::IsNull(x, _) | Expr::IsNotNull(x, _) => {
            replace_alias_in_expr(x, alias, elem, pname)
        }
        Expr::Call(c) => {
            for a in &mut c.args {
                replace_alias_in_expr(a, alias, elem, pname)?;
            }
            Ok(())
        }
        Expr::List(items, _) => {
            for it in items {
                replace_alias_in_expr(it, alias, elem, pname)?;
            }
            Ok(())
        }
        Expr::Case(_) | Expr::Exists(_, _) | Expr::Var(_) | Expr::Lit(_) | Expr::Param(_) => Ok(()),
    }
}

/// Convert a map field value to a literal expression. Scalars only — nested
/// maps/lists as field values are deferred (no map-valued property in v1).
fn json_scalar_to_expr(v: &JsonValue, name: &str, span: SourceSpan) -> Result<Expr, ParamError> {
    match v {
        JsonValue::Array(_) | JsonValue::Object(_) => Err(unsupported_param(
            name,
            "map/list field values inside an UNWIND element are not supported in v1",
        )),
        _ => json_to_expr(v, name, span),
    }
}

/// Suffix every node/relationship variable with the row index (and name
/// anonymous nodes), so each unrolled element creates distinct graph nodes
/// rather than colliding on a shared blank-node id.
fn rename_pattern_vars(pat: &mut Pattern, row: usize, anon: &mut u32) {
    for part in &mut pat.parts {
        rename_node_var(&mut part.head, row, anon);
        for (rel, node) in &mut part.tail {
            if let Some(v) = &mut rel.var {
                v.name = format!("{}__cyrow{}", v.name, row);
            }
            rename_node_var(node, row, anon);
        }
    }
}

fn rename_node_var(n: &mut NodePattern, row: usize, anon: &mut u32) {
    match &mut n.var {
        Some(v) => v.name = format!("{}__cyrow{}", v.name, row),
        None => {
            let name = format!("__cyanon{anon}__cyrow{row}");
            *anon += 1;
            n.var = Some(Variable { name, span: n.span });
        }
    }
}

fn subst_statement(s: &mut Statement, p: &ParamMap) -> Result<(), ParamError> {
    match s {
        Statement::Query(q) => subst_query(q, p),
        Statement::Update(u) => subst_update(u, p),
    }
}

fn subst_query(q: &mut Query, p: &ParamMap) -> Result<(), ParamError> {
    for c in &mut q.clauses {
        subst_read_clause(c, p)?;
    }
    subst_return(&mut q.return_clause, p)?;
    if let Some(tail) = &mut q.union_tail {
        subst_query(&mut tail.right, p)?;
    }
    Ok(())
}

fn subst_update(u: &mut Update, p: &ParamMap) -> Result<(), ParamError> {
    for c in &mut u.read_clauses {
        subst_read_clause(c, p)?;
    }
    for w in &mut u.write_clauses {
        subst_write_clause(w, p)?;
    }
    if let Some(r) = &mut u.return_clause {
        subst_return(r, p)?;
    }
    Ok(())
}

fn subst_read_clause(c: &mut ReadClause, p: &ParamMap) -> Result<(), ParamError> {
    match c {
        ReadClause::Match(m) | ReadClause::OptionalMatch(m) => subst_match(m, p),
        ReadClause::With(w) => {
            for i in &mut w.items {
                subst_expr(&mut i.expr, p)?;
            }
            if let Some(e) = &mut w.where_clause {
                subst_expr(e, p)?;
            }
            for o in &mut w.order_by {
                subst_expr(&mut o.expr, p)?;
            }
            subst_opt(&mut w.skip, p)?;
            subst_opt(&mut w.limit, p)
        }
        ReadClause::Unwind(u) => subst_expr(&mut u.expr, p),
        // Desugared constant rows: cells are already literals, but recurse for
        // robustness (a future producer might leave a `$param` cell).
        ReadClause::InlineRows { rows, .. } => {
            for row in rows {
                for cell in row {
                    subst_expr(cell, p)?;
                }
            }
            Ok(())
        }
    }
}

fn subst_match(m: &mut MatchClause, p: &ParamMap) -> Result<(), ParamError> {
    subst_pattern(&mut m.pattern, p)?;
    if let Some(w) = &mut m.where_clause {
        subst_expr(w, p)?;
    }
    Ok(())
}

fn subst_return(r: &mut ReturnClause, p: &ParamMap) -> Result<(), ParamError> {
    for i in &mut r.items {
        subst_expr(&mut i.expr, p)?;
    }
    for o in &mut r.order_by {
        subst_expr(&mut o.expr, p)?;
    }
    subst_opt(&mut r.skip, p)?;
    subst_opt(&mut r.limit, p)
}

fn subst_write_clause(w: &mut WriteClause, p: &ParamMap) -> Result<(), ParamError> {
    match w {
        WriteClause::Create(c) => subst_pattern(&mut c.pattern, p),
        WriteClause::Merge(m) => {
            subst_pattern(&mut m.pattern, p)?;
            for s in &mut m.on_create {
                subst_set_item(s, p)?;
            }
            for s in &mut m.on_match {
                subst_set_item(s, p)?;
            }
            Ok(())
        }
        WriteClause::Set(s) => {
            for it in &mut s.items {
                subst_set_item(it, p)?;
            }
            Ok(())
        }
        WriteClause::Remove(_) | WriteClause::Delete(_) => Ok(()),
    }
}

fn subst_set_item(s: &mut SetItem, p: &ParamMap) -> Result<(), ParamError> {
    match s {
        SetItem::Property { value, .. } => subst_expr(value, p),
        SetItem::MapMerge { map, .. } | SetItem::MapReplace { map, .. } => subst_maplit(map, p),
        SetItem::Labels { .. } => Ok(()),
    }
}

fn subst_pattern(pat: &mut Pattern, p: &ParamMap) -> Result<(), ParamError> {
    for part in &mut pat.parts {
        subst_node(&mut part.head, p)?;
        for (rel, node) in &mut part.tail {
            subst_rel(rel, p)?;
            subst_node(node, p)?;
        }
    }
    Ok(())
}

fn subst_node(n: &mut NodePattern, p: &ParamMap) -> Result<(), ParamError> {
    if let Some(m) = &mut n.props {
        subst_maplit(m, p)?;
    }
    Ok(())
}

fn subst_rel(r: &mut RelPattern, p: &ParamMap) -> Result<(), ParamError> {
    if let Some(m) = &mut r.props {
        subst_maplit(m, p)?;
    }
    Ok(())
}

fn subst_maplit(m: &mut MapLit, p: &ParamMap) -> Result<(), ParamError> {
    for (_, e) in &mut m.entries {
        subst_expr(e, p)?;
    }
    Ok(())
}

fn subst_opt(e: &mut Option<Expr>, p: &ParamMap) -> Result<(), ParamError> {
    if let Some(e) = e {
        subst_expr(e, p)?;
    }
    Ok(())
}

fn subst_expr(e: &mut Expr, p: &ParamMap) -> Result<(), ParamError> {
    match e {
        Expr::Param(pref) => {
            let val = p
                .get(&pref.name)
                .ok_or_else(|| ParamError::Missing(pref.name.clone()))?;
            *e = json_to_expr(val, &pref.name, pref.span)?;
            Ok(())
        }
        Expr::Prop(inner, _, _) => subst_expr(inner, p),
        Expr::BinOp(_, l, r, _)
        | Expr::In(l, r, _)
        | Expr::StartsWith(l, r, _)
        | Expr::EndsWith(l, r, _)
        | Expr::Contains(l, r, _) => {
            subst_expr(l, p)?;
            subst_expr(r, p)
        }
        Expr::UnaryOp(_, x, _) | Expr::IsNull(x, _) | Expr::IsNotNull(x, _) => subst_expr(x, p),
        Expr::Call(c) => {
            for a in &mut c.args {
                subst_expr(a, p)?;
            }
            Ok(())
        }
        Expr::Case(c) => subst_case(c, p),
        Expr::Exists(pat, _) => subst_pattern(pat, p),
        Expr::List(items, _) => {
            for it in items {
                subst_expr(it, p)?;
            }
            Ok(())
        }
        Expr::Var(_) | Expr::Lit(_) => Ok(()),
    }
}

fn subst_case(c: &mut CaseExpr, p: &ParamMap) -> Result<(), ParamError> {
    if let Some(s) = &mut c.subject {
        subst_expr(s, p)?;
    }
    for (when, then) in &mut c.branches {
        subst_expr(when, p)?;
        subst_expr(then, p)?;
    }
    if let Some(e) = &mut c.else_branch {
        subst_expr(e, p)?;
    }
    Ok(())
}

/// Convert a JSON parameter value into a literal / list expression. The `span`
/// of the originating `$param` is reused so downstream errors still point at
/// the reference site.
fn json_to_expr(v: &JsonValue, name: &str, span: SourceSpan) -> Result<Expr, ParamError> {
    let unsupported = |reason: &str| ParamError::Unsupported {
        name: name.to_string(),
        reason: reason.to_string(),
    };
    Ok(match v {
        JsonValue::String(s) => Expr::Lit(Literal::String(s.clone(), span)),
        JsonValue::Bool(b) => Expr::Lit(Literal::Bool(*b, span)),
        JsonValue::Null => Expr::Lit(Literal::Null(span)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Expr::Lit(Literal::Integer(i, span))
            } else if let Some(f) = n.as_f64() {
                Expr::Lit(Literal::Float(f, span))
            } else {
                return Err(unsupported("numeric value out of range"));
            }
        }
        JsonValue::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for it in items {
                if it.is_array() || it.is_object() {
                    return Err(unsupported(
                        "nested arrays/objects inside a list parameter are not supported in v1",
                    ));
                }
                out.push(json_to_expr(it, name, span)?);
            }
            Expr::List(out, span)
        }
        JsonValue::Object(_) => {
            return Err(unsupported("map/object parameters are not supported in v1"))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse_cypher;

    fn params(json: serde_json::Value) -> ParamMap {
        json.as_object().unwrap().clone()
    }

    /// Parse, substitute, and return the (possibly mutated) AST or the param
    /// error.
    fn run(src: &str, p: &ParamMap) -> Result<CypherAst, ParamError> {
        let out = parse_cypher(src);
        assert!(!out.has_errors(), "parse errors: {:?}", out.diagnostics);
        let mut ast = out.ast.unwrap();
        substitute_params(&mut ast, p)?;
        Ok(ast)
    }

    /// Collect every literal string in the AST's expressions (shallow probe
    /// over the projection + where) to confirm substitution happened. Simplest
    /// to just re-render via Debug and look for the marker.
    fn debug_contains(ast: &CypherAst, needle: &str) -> bool {
        format!("{ast:?}").contains(needle)
    }

    #[test]
    fn scalar_param_in_inline_filter() {
        let ast = run(
            "MATCH (n:Person {name: $name}) RETURN n",
            &params(serde_json::json!({"name": "Alice"})),
        )
        .unwrap();
        assert!(debug_contains(&ast, "Alice"), "{ast:?}");
        // The Param node should be gone.
        assert!(!debug_contains(&ast, "Param"), "{ast:?}");
    }

    #[test]
    fn numeric_param_in_where() {
        let ast = run(
            "MATCH (n:Person) WHERE n.age > $min RETURN n",
            &params(serde_json::json!({"min": 30})),
        )
        .unwrap();
        assert!(debug_contains(&ast, "Integer(30"), "{ast:?}");
    }

    #[test]
    fn list_param_in_unwind() {
        let ast = run(
            "UNWIND $ids AS x RETURN x",
            &params(serde_json::json!({"ids": [1, 2, 3]})),
        )
        .unwrap();
        assert!(debug_contains(&ast, "List("), "{ast:?}");
        assert!(!debug_contains(&ast, "Param"), "{ast:?}");
    }

    #[test]
    fn missing_param_errors() {
        let err = run(
            "MATCH (n:Person {name: $name}) RETURN n",
            &params(serde_json::json!({})),
        )
        .unwrap_err();
        assert_eq!(err, ParamError::Missing("name".to_string()));
    }

    #[test]
    fn object_param_is_rejected() {
        let err = run(
            "MATCH (n:Person {name: $p}) RETURN n",
            &params(serde_json::json!({"p": {"nested": 1}})),
        )
        .unwrap_err();
        assert!(matches!(err, ParamError::Unsupported { .. }), "{err:?}");
    }

    #[test]
    fn param_in_create_write() {
        let ast = run(
            "CREATE (n:Person {name: $name, age: $age})",
            &params(serde_json::json!({"name": "Bob", "age": 41})),
        )
        .unwrap();
        assert!(debug_contains(&ast, "Bob"), "{ast:?}");
        assert!(debug_contains(&ast, "Integer(41"), "{ast:?}");
    }
}
