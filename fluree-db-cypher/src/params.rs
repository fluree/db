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
    CaseExpr, CypherAst, Expr, Literal, MapLit, MatchClause, NodePattern, Pattern, Query,
    ReadClause, RelPattern, ReturnClause, SetItem, Statement, Update, WriteClause,
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
    subst_statement(&mut ast.statement, params)
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
