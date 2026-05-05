//! Logical operator implementations
//!
//! Implements logical operators: AND, OR, NOT

use super::value::ComparableValue;
use crate::binding::RowAccess;
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::ir::Expression;

/// Evaluate logical AND
///
/// Returns true if all arguments evaluate to true.
/// Short-circuits on first false value.
pub fn eval_and<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    for arg in args {
        if !arg.eval_to_bool(row, ctx)? {
            return Ok(Some(ComparableValue::Bool(false)));
        }
    }
    Ok(Some(ComparableValue::Bool(true)))
}

/// Evaluate logical OR
///
/// Returns true if any argument evaluates to true.
/// Short-circuits on first true value.
pub fn eval_or<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    for arg in args {
        if arg.eval_to_bool(row, ctx)? {
            return Ok(Some(ComparableValue::Bool(true)));
        }
    }
    Ok(Some(ComparableValue::Bool(false)))
}

/// Evaluate logical NOT
///
/// Returns the logical negation of the single argument.
pub fn eval_not<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    if args.is_empty() {
        return Ok(Some(ComparableValue::Bool(true))); // NOT of nothing is true
    }
    let result = args[0].eval_to_bool(row, ctx)?;
    Ok(Some(ComparableValue::Bool(!result)))
}

/// Evaluate IN expression
///
/// First argument is the test value, remaining arguments are the set values.
/// Returns true if test value equals any set value.
pub fn eval_in<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    if args.is_empty() {
        return Ok(Some(ComparableValue::Bool(false)));
    }

    let test_val = args[0].eval_to_comparable(row, ctx)?;
    match test_val {
        Some(tv) => {
            let mut found = false;
            for v in &args[1..] {
                match v.eval_to_comparable(row, ctx) {
                    Ok(Some(cv)) if cv == tv => {
                        found = true;
                        break;
                    }
                    Ok(Some(_) | None) => {}
                    Err(err) if err.can_demote_in_expression() => {}
                    Err(err) => return Err(err),
                }
            }
            Ok(Some(ComparableValue::Bool(found)))
        }
        None => Ok(Some(ComparableValue::Bool(false))), // Unbound value -> not in list
    }
}

/// Evaluate NOT IN expression
///
/// First argument is the test value, remaining arguments are the set values.
/// Returns true if test value does not equal any set value.
pub fn eval_not_in<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    if args.is_empty() {
        return Ok(Some(ComparableValue::Bool(true)));
    }

    let test_val = args[0].eval_to_comparable(row, ctx)?;
    match test_val {
        Some(tv) => {
            let mut found = false;
            for v in &args[1..] {
                match v.eval_to_comparable(row, ctx) {
                    Ok(Some(cv)) if cv == tv => {
                        found = true;
                        break;
                    }
                    Ok(Some(_) | None) => {}
                    Err(err) if err.can_demote_in_expression() => {}
                    Err(err) => return Err(err),
                }
            }
            Ok(Some(ComparableValue::Bool(!found)))
        }
        None => Ok(Some(ComparableValue::Bool(true))), // Unbound value -> not in list (vacuously true)
    }
}
