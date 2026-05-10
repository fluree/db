//! Conditional function implementations
//!
//! Implements SPARQL conditional functions: IF, COALESCE

use super::helpers::check_arity;
use super::value::ComparableValue;
use crate::binding::RowAccess;
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::ir::Expression;

pub fn eval_if<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 3, "IF")?;
    // Per W3C SPARQL spec §17.4.1: if evaluation of the condition raises
    // an error, IF also raises an error. The caller (eval_to_binding)
    // converts errors to Binding::Unbound for BIND expressions.
    let cond = args[0].eval_to_bool(row, ctx)?;
    if cond {
        args[1].eval_to_comparable(row, ctx)
    } else {
        args[2].eval_to_comparable(row, ctx)
    }
}

pub fn eval_coalesce<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    // Per W3C SPARQL spec: COALESCE returns the first argument that evaluates
    // without error and is not unbound. Errors are caught and skipped.
    for arg in args {
        match arg.eval_to_comparable(row, ctx) {
            Ok(Some(val)) => return Ok(Some(val)),
            Ok(None) => continue,
            Err(err) if err.can_demote_in_expression() => continue,
            Err(err) => return Err(err),
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::BindingRow;
    use crate::ir::{FlakeValue, Function};
    use crate::var_registry::VarId;

    #[test]
    fn if_condition_errors_propagate() {
        let expr = Expression::call(
            Function::If,
            vec![
                Expression::call(
                    Function::StrStarts,
                    vec![
                        Expression::Const(FlakeValue::Long(1)),
                        Expression::Const(FlakeValue::String("x".to_string())),
                    ],
                ),
                Expression::Const(FlakeValue::String("then".to_string())),
                Expression::Const(FlakeValue::String("else".to_string())),
            ],
        );

        let row = BindingRow::new(&[] as &[VarId], &[]);
        assert!(expr.eval_to_comparable(&row, None).is_err());
    }
}
