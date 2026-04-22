//! Inline operator types and per-row evaluation.
//!
//! [`InlineOperator`] and [`apply_inline`] support inlining eligible operations
//! into host operators so they execute per-row without separate wrapper nodes.

use crate::binding::{Binding, BindingRow};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::expression::PreparedBoolExpression;
use crate::ir::Expression;
use crate::var_registry::VarId;

/// An inline operation evaluated per-row inside a host operator.
///
/// Replaces separate wrapper operator nodes for eligible operations whose
/// required variables are all bound by the time the host operator executes.
#[derive(Debug, Clone)]
pub enum InlineOperator {
    /// Drop the row if the expression evaluates to false.
    Filter(PreparedBoolExpression),
    /// Evaluate expression and bind result to variable.
    Bind { var: VarId, expr: Expression },
}

/// Extend a base schema with bind target variables from inline operators.
///
/// Appends any `Bind` target variables not already present in `base_schema`.
/// Variables appear in the order the inline operators execute.
pub fn extend_schema(base_schema: &[VarId], operators: &[InlineOperator]) -> Vec<VarId> {
    let mut schema = base_schema.to_vec();
    for op in operators {
        if let InlineOperator::Bind { var, .. } = op {
            if !schema.contains(var) {
                schema.push(*var);
            }
        }
    }
    schema
}

/// Execute inline operators against a growing binding row.
///
/// For each operator:
/// - **Filter**: evaluates against current bindings; returns `false` (skip row)
///   if it fails.
/// - **Bind**: evaluates expression and either pushes a new binding or performs
///   a clobber check on an existing variable. In non-strict mode (default),
///   expression-level errors produce `Binding::Unbound`, while fatal execution
///   errors (for example dictionary lookup failures) still propagate. In strict
///   mode (`ctx.strict_bind_errors`) all errors propagate as `Result::Err`.
///
/// Returns `true` if the row survived all filters and clobber checks.
///
/// # Schema contract
///
/// `schema` must contain all variables from the triple pattern *plus* all bind
/// target variables from `ops`, in the order they will appear in the final row.
/// `bindings` starts with only the triple-pattern bindings and grows as Bind
/// operators execute.
pub fn apply_inline(
    ops: &[InlineOperator],
    schema: &[VarId],
    bindings: &mut Vec<Binding>,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<bool> {
    let strict = ctx.is_some_and(|c| c.strict_bind_errors);

    for op in ops {
        let row = BindingRow::new(&schema[..bindings.len()], bindings);
        match op {
            InlineOperator::Filter(expr) => {
                if !expr.eval_to_bool_non_strict(&row, ctx)? {
                    return Ok(false);
                }
            }
            InlineOperator::Bind { var, expr } => {
                let value = if strict {
                    expr.try_eval_to_binding(&row, ctx)?
                } else {
                    expr.try_eval_to_binding_non_strict(&row, ctx)?
                };
                match schema[..bindings.len()].iter().position(|&v| v == *var) {
                    None => bindings.push(value),
                    Some(pos) => match (&bindings[pos], &value) {
                        (Binding::Unbound, _) => bindings[pos] = value,
                        (_, Binding::Unbound) => continue,
                        (a, b) if a == b => continue,
                        _ => return Ok(false),
                    },
                }
            }
        }
    }
    Ok(true)
}
