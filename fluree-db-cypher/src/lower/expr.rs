//! Expression lowering — Cypher Expr → fluree-db-query Expression.

use fluree_db_core::FlakeValue;
use fluree_db_query::ir::{Expression, Function, Pattern, Ref, Term, TriplePattern};
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::var_registry::VarId;

use crate::ast::{BinOp, CaseExpr, Expr, Literal, ParamRef, UnaryOp};

use super::context::LoweringContext;
use super::pattern::lower_pattern;
use super::{LowerError, Result};

/// Lower a Cypher expression to an `Expression`. Any auxiliary
/// patterns the expression requires (e.g., property-accessor joins)
/// are appended to `aux`. The caller is responsible for splicing
/// `aux` into the enclosing pattern list before the position where
/// the expression is evaluated.
pub fn lower_expr<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    e: &Expr,
    aux: &mut Vec<Pattern>,
) -> Result<Expression> {
    match e {
        Expr::Var(v) => Ok(Expression::Var(ctx.intern_var(&v.name))),
        Expr::Lit(l) => Ok(Expression::Const(lower_literal(l)?)),
        Expr::Param(_) => Err(LowerError::unsupported(
            "parameter substitution is wired at the API layer, not the lowering layer; submit pre-substituted Cypher in v1",
        )),
        Expr::Prop(target, key, _) => {
            let prop_var = resolve_property_accessor(ctx, target, key, aux)?;
            Ok(Expression::Var(prop_var))
        }
        Expr::BinOp(op, l, r, _) => {
            let l = lower_expr(ctx, l, aux)?;
            let r = lower_expr(ctx, r, aux)?;
            let f = match op {
                BinOp::Eq => Function::Eq,
                BinOp::Ne => Function::Ne,
                BinOp::Lt => Function::Lt,
                BinOp::Le => Function::Le,
                BinOp::Gt => Function::Gt,
                BinOp::Ge => Function::Ge,
                BinOp::Add => Function::Add,
                BinOp::Sub => Function::Sub,
                BinOp::Mul => Function::Mul,
                BinOp::Div => Function::Div,
                BinOp::And => Function::And,
                BinOp::Or => Function::Or,
            };
            Ok(Expression::binary(f, l, r))
        }
        Expr::UnaryOp(op, inner, _) => {
            let inner = lower_expr(ctx, inner, aux)?;
            let f = match op {
                UnaryOp::Neg => Function::Negate,
                UnaryOp::Not => Function::Not,
            };
            Ok(Expression::call(f, vec![inner]))
        }
        Expr::In(left, list, _) => {
            // Lower to `Function::In(test, candidate1, candidate2, ...)`.
            // The right-hand side must be a list literal in v1; parameter-
            // bound list expressions are deferred.
            let test = lower_expr(ctx, left, aux)?;
            let items = match list.as_ref() {
                Expr::List(items, _) => items,
                _ => {
                    return Err(LowerError::unsupported(
                        "`IN` right-hand side must be an inline list `[a, b, ...]` in v1",
                    ));
                }
            };
            let mut args = Vec::with_capacity(items.len() + 1);
            args.push(test);
            for item in items {
                args.push(lower_expr(ctx, item, aux)?);
            }
            Ok(Expression::call(Function::In, args))
        }
        Expr::IsNull(inner, _) => {
            let inner = lower_expr(ctx, inner, aux)?;
            Ok(Expression::call(Function::Not, vec![Expression::call(
                Function::Bound,
                vec![inner],
            )]))
        }
        Expr::IsNotNull(inner, _) => {
            let inner = lower_expr(ctx, inner, aux)?;
            Ok(Expression::call(Function::Bound, vec![inner]))
        }
        Expr::StartsWith(l, r, _) => {
            let l = lower_expr(ctx, l, aux)?;
            let r = lower_expr(ctx, r, aux)?;
            Ok(Expression::binary(Function::StrStarts, l, r))
        }
        Expr::EndsWith(l, r, _) => {
            let l = lower_expr(ctx, l, aux)?;
            let r = lower_expr(ctx, r, aux)?;
            Ok(Expression::binary(Function::StrEnds, l, r))
        }
        Expr::Contains(l, r, _) => {
            let l = lower_expr(ctx, l, aux)?;
            let r = lower_expr(ctx, r, aux)?;
            Ok(Expression::binary(Function::Contains, l, r))
        }
        Expr::Case(case) => lower_case(ctx, case, aux),
        Expr::Exists(pattern, inner_where, _) => {
            let mut patterns = lower_pattern(ctx, pattern)?;
            // An inner WHERE is ANDed into the existence test. Its own auxiliary
            // patterns (e.g. property-accessor Optionals) must live INSIDE the
            // subquery so `x.id` resolves within the existence scope, not the
            // outer query — so lower it against a local aux, not the caller's.
            if let Some(cond) = inner_where {
                let mut inner_aux = Vec::new();
                let filter = lower_expr(ctx, cond, &mut inner_aux)?;
                patterns.extend(inner_aux);
                patterns.push(Pattern::Filter(filter));
            }
            Ok(Expression::Exists {
                patterns,
                negated: false,
            })
        }
        Expr::List(_, _) => Err(LowerError::unsupported(
            "list literals in expressions are deferred (no list-value type yet)",
        )),
        Expr::Call(call) => {
            let name = call.name.to_ascii_lowercase();
            let args: std::result::Result<Vec<_>, _> =
                call.args.iter().map(|a| lower_expr(ctx, a, aux)).collect();
            let args = args?;
            let func = match name.as_str() {
                "coalesce" => Function::Coalesce,
                "abs" => Function::Abs,
                // Cypher `length(p)` is a path's hop count; `size(x)` is the
                // list/string length (Cypher 9 split these).
                "length" => Function::PathLength,
                "size" => Function::Size,
                // List functions over `collect()` lists.
                "head" => Function::Head,
                "last" => Function::Last,
                "tail" => Function::Tail,
                "reverse" => Function::Reverse,
                "tostring" => Function::Str,
                _ => {
                    return Err(LowerError::unsupported(format!(
                        "function `{}` is not in the v1 expression surface",
                        call.name
                    )));
                }
            };
            Ok(Expression::call(func, args))
        }
    }
}

/// Resolve a Cypher `target.key` property accessor to a VarId.
///
/// Emits `Pattern::Optional([Triple(target, <key IRI>, ?#__prop_target_key)])`
/// into `aux`. The **optional** wrap matches Cypher's nullable
/// property-access semantics: when the target has no value for the
/// key, the accessor evaluates to null and the row still flows
/// through the query, not filtered out by a mandatory join.
///
/// This makes the following work as Cypher users expect:
///
/// - `WHERE n.missing IS NULL` returns nodes lacking the property
///   (the property var is unbound; `IS NULL` evaluates true).
/// - `RETURN n.name` for a sparse property returns one row per
///   matched node, with `null` where the property is absent.
/// - `avg(n.age)` averages across nodes that have age, skipping
///   nulls — the aggregate's natural unbound-input behavior.
/// - `RETURN n.dept, count(*)` groups by dept (with a "null"
///   group for nodes without one).
/// - `WHERE n.age > 30` continues to filter to age-bearing nodes
///   above 30: the `>` comparison on an unbound binding yields a
///   filter-context error → effective boolean false → row excluded.
///   Same end result as the previous mandatory-join behavior.
///
/// Why always-emit rather than dedup at lower time: subquery
/// boundaries (`WITH`) can drop the property variable from the
/// outer scope if it isn't in the WITH's select list. A naive
/// "have we already emitted this name?" check would skip the
/// re-emit in the outer scope, leaving the property var unbound.
/// Re-emitting is correct and the planner handles redundant
/// Optionals cheaply.
///
/// v1 only accepts a bare-variable target (`n.prop`); chained
/// accessors (`n.address.city`) and accessors on non-variable
/// expressions (e.g., `(n {p:1}).p`) are rejected.
pub(crate) fn resolve_property_accessor<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    target: &Expr,
    key: &str,
    aux: &mut Vec<Pattern>,
) -> Result<VarId> {
    let target_var = match target {
        Expr::Var(v) => v,
        Expr::Prop(_, _, _) => {
            return Err(LowerError::unsupported(
                "chained property accessors (`n.foo.bar`) are deferred — bind to an intermediate variable via WITH",
            ));
        }
        _ => {
            return Err(LowerError::unsupported(
                "property accessors require a bare-variable target in v1 (e.g., `n.prop`)",
            ));
        }
    };
    let target_id = ctx.intern_var(&target_var.name);
    let pred_iri = ctx.resolve_predicate(key)?;

    let prop_var_name = format!("?#__prop_{}_{}", target_var.name, key);
    let prop_var = ctx.intern_var(&prop_var_name);

    aux.push(Pattern::Optional(vec![Pattern::Triple(
        TriplePattern::new(
            Ref::Var(target_id),
            Ref::Iri(pred_iri.into()),
            Term::Var(prop_var),
        ),
    )]));
    Ok(prop_var)
}

/// Lower a Cypher `CASE` expression to nested `Function::If` calls.
///
/// Cypher has two forms:
///   `CASE WHEN cond THEN val [...] [ELSE val] END`              (simple)
///   `CASE subj WHEN cand THEN val [...] [ELSE val] END`         (subject)
///
/// In the subject form, each `WHEN cand` desugars to `subj = cand`.
/// Both forms then lower to a right-folded `If(c1, v1, If(c2, v2, ... else))`.
fn lower_case<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    case: &CaseExpr,
    aux: &mut Vec<Pattern>,
) -> Result<Expression> {
    if case.branches.is_empty() {
        return Err(LowerError::unsupported(
            "CASE requires at least one WHEN branch",
        ));
    }

    // The final ELSE — Cypher omits it as implicit NULL; we surface that
    // as Bound→false via Function::Coalesce with zero remaining args
    // (an empty Coalesce returns unbound).
    let else_expr = match &case.else_branch {
        Some(e) => lower_expr(ctx, e, aux)?,
        None => Expression::call(Function::Coalesce, Vec::new()),
    };

    // The subject expression, if any, lowered once and reused per branch
    // wrapped in equality.
    let subject = match &case.subject {
        Some(s) => Some(lower_expr(ctx, s, aux)?),
        None => None,
    };

    // Right-fold over branches.
    let mut acc = else_expr;
    for (cond, val) in case.branches.iter().rev() {
        let cond_expr = lower_expr(ctx, cond, aux)?;
        let val_expr = lower_expr(ctx, val, aux)?;
        let test = match &subject {
            Some(subj) => Expression::binary(Function::Eq, subj.clone(), cond_expr),
            None => cond_expr,
        };
        acc = Expression::call(Function::If, vec![test, val_expr, acc]);
    }
    Ok(acc)
}

pub fn lower_literal(lit: &Literal) -> Result<FlakeValue> {
    Ok(match lit {
        Literal::Integer(n, _) => FlakeValue::Long(*n),
        Literal::Float(f, _) => FlakeValue::Double(*f),
        Literal::String(s, _) => FlakeValue::String(s.clone()),
        Literal::Bool(b, _) => FlakeValue::Boolean(*b),
        Literal::Null(_) => {
            return Err(LowerError::unsupported(
                "NULL literals in lowered expressions are deferred",
            ));
        }
    })
}

// Silence unused-import lints for ParamRef which we keep for future
// non-error wiring.
#[allow(dead_code)]
fn _retain_paramref(_p: &ParamRef) {}
