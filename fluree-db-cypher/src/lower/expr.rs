//! Expression lowering — Cypher Expr → fluree-db-query Expression.

use fluree_db_core::FlakeValue;
use fluree_db_query::ir::{Expression, Function};
use fluree_db_query::parse::encode::IriEncoder;

use crate::ast::{BinOp, Expr, Literal, ParamRef, UnaryOp};

use super::context::LoweringContext;
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
            let _ = (left, list);
            Err(LowerError::unsupported(
                "`IN` is partially supported via FILTER but the expression-level lowering is deferred in v1",
            ))
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
        Expr::Case(_) => Err(LowerError::unsupported(
            "CASE is deferred in v1 lowering — open follow-up",
        )),
        Expr::Exists(_, _) => Err(LowerError::unsupported(
            "EXISTS in expression position is deferred — use `OPTIONAL MATCH` plus a null check or stay in pattern position",
        )),
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
