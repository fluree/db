//! Expression lowering — Cypher Expr → fluree-db-query Expression.

use fluree_db_core::FlakeValue;
use fluree_db_query::ir::{Expression, Function};
use fluree_db_query::parse::encode::IriEncoder;

use crate::ast::{BinOp, CaseExpr, Expr, Literal, ParamRef, UnaryOp};

use super::context::LoweringContext;
use super::pattern::lower_pattern;
use super::{LowerError, Result};

pub fn lower_expr<E: IriEncoder>(ctx: &mut LoweringContext<'_, E>, e: &Expr) -> Result<Expression> {
    match e {
        Expr::Var(v) => Ok(Expression::Var(ctx.intern_var(&v.name))),
        Expr::Lit(l) => Ok(Expression::Const(lower_literal(l)?)),
        Expr::Param(_) => Err(LowerError::unsupported(
            "parameter substitution is wired at the API layer, not the lowering layer; submit pre-substituted Cypher in v1",
        )),
        Expr::Prop(_, _, _) => Err(LowerError::unsupported(
            "property accessors (`n.prop`) inside expressions are deferred; project nodes via RETURN and reference properties in WHERE via separate triple patterns",
        )),
        Expr::BinOp(op, l, r, _) => {
            let l = lower_expr(ctx, l)?;
            let r = lower_expr(ctx, r)?;
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
            let inner = lower_expr(ctx, inner)?;
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
            let test = lower_expr(ctx, left)?;
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
                args.push(lower_expr(ctx, item)?);
            }
            Ok(Expression::call(Function::In, args))
        }
        Expr::IsNull(inner, _) => {
            let inner = lower_expr(ctx, inner)?;
            Ok(Expression::call(Function::Not, vec![Expression::call(
                Function::Bound,
                vec![inner],
            )]))
        }
        Expr::IsNotNull(inner, _) => {
            let inner = lower_expr(ctx, inner)?;
            Ok(Expression::call(Function::Bound, vec![inner]))
        }
        Expr::StartsWith(l, r, _) => {
            let l = lower_expr(ctx, l)?;
            let r = lower_expr(ctx, r)?;
            Ok(Expression::binary(Function::StrStarts, l, r))
        }
        Expr::EndsWith(l, r, _) => {
            let l = lower_expr(ctx, l)?;
            let r = lower_expr(ctx, r)?;
            Ok(Expression::binary(Function::StrEnds, l, r))
        }
        Expr::Contains(l, r, _) => {
            let l = lower_expr(ctx, l)?;
            let r = lower_expr(ctx, r)?;
            Ok(Expression::binary(Function::Contains, l, r))
        }
        Expr::Case(case) => lower_case(ctx, case),
        Expr::Exists(pattern, _) => {
            let patterns = lower_pattern(ctx, pattern)?;
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
                call.args.iter().map(|a| lower_expr(ctx, a)).collect();
            let args = args?;
            let func = match name.as_str() {
                "coalesce" => Function::Coalesce,
                "abs" => Function::Abs,
                "length" => Function::Strlen,
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
        Some(e) => lower_expr(ctx, e)?,
        None => Expression::call(Function::Coalesce, Vec::new()),
    };

    // The subject expression, if any, lowered once and reused per branch
    // wrapped in equality.
    let subject = match &case.subject {
        Some(s) => Some(lower_expr(ctx, s)?),
        None => None,
    };

    // Right-fold over branches.
    let mut acc = else_expr;
    for (cond, val) in case.branches.iter().rev() {
        let cond_expr = lower_expr(ctx, cond)?;
        let val_expr = lower_expr(ctx, val)?;
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
