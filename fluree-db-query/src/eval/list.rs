//! List function implementations (Cypher list values).
//!
//! List values are carried in [`Binding::List`] (produced by `collect()`, list
//! literals, and the list-returning functions here). The eval architecture is
//! scalar (`ComparableValue`), so:
//!
//! - `size` / `head` / `last` return scalars → ordinary `Function::eval`.
//! - `tail` / `reverse` return lists → the binding-producing path
//!   ([`eval_list_fn_to_binding`], dispatched from `try_eval_to_binding`).
//! - `reverse` of a *string* is a scalar and stays on the `Function::eval` path.

use crate::binding::{Binding, RowAccess};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{Expression, Function};
use std::sync::Arc;

use super::value::ComparableValue;

/// Resolve a list-function argument to its input binding without losing a list.
///
/// A bare variable is read straight from the row (so a `Binding::List` survives
/// — `eval_to_comparable` would collapse it to `None`). Any other expression
/// (e.g. a nested `reverse(...)`) is evaluated through the binding-producing
/// path, which preserves list outputs.
fn resolve_arg_binding<R: RowAccess>(
    arg: &Expression,
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<Binding>> {
    match arg {
        Expression::Var(v) => Ok(row.get(*v).cloned()),
        other => Ok(Some(other.try_eval_to_binding(row, ctx)?)),
    }
}

/// Convert a list element binding to a comparable scalar (for `head`/`last`).
/// Collect materializes elements, so they are decoded literals / refs; a
/// non-scalar element (e.g. a nested list) yields `None`.
fn element_to_comparable(b: &Binding) -> Option<ComparableValue> {
    match b {
        Binding::Lit { val, .. } => ComparableValue::try_from(val).ok(),
        Binding::Sid { sid, .. } => Some(ComparableValue::Sid(sid.clone())),
        Binding::IriMatch { iri, .. } => Some(ComparableValue::Iri(Arc::clone(iri))),
        Binding::Iri(iri) => Some(ComparableValue::Iri(Arc::clone(iri))),
        _ => None,
    }
}

fn arity1<'a>(args: &'a [Expression], name: &str) -> Result<&'a Expression> {
    if args.len() != 1 {
        return Err(QueryError::InvalidFilter(format!(
            "{name}() expects 1 argument, got {}",
            args.len()
        )));
    }
    Ok(&args[0])
}

/// `size(list)` → element count; `size(string)` → character count.
pub fn eval_size<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    let arg = arity1(args, "size")?;
    match resolve_arg_binding(arg, row, ctx)? {
        Some(Binding::List(items)) => Ok(Some(ComparableValue::Long(items.len() as i64))),
        Some(Binding::Lit {
            val: fluree_db_core::FlakeValue::String(s),
            ..
        }) => Ok(Some(ComparableValue::Long(s.chars().count() as i64))),
        _ => Ok(None),
    }
}

/// `head(list)` → first element, or null when empty / not a list.
pub fn eval_head<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    let arg = arity1(args, "head")?;
    match resolve_arg_binding(arg, row, ctx)? {
        Some(Binding::List(items)) => Ok(items.first().and_then(element_to_comparable)),
        _ => Ok(None),
    }
}

/// `last(list)` → last element, or null when empty / not a list.
pub fn eval_last<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    let arg = arity1(args, "last")?;
    match resolve_arg_binding(arg, row, ctx)? {
        Some(Binding::List(items)) => Ok(items.last().and_then(element_to_comparable)),
        _ => Ok(None),
    }
}

/// `reverse(string)` → reversed string. The list case is handled on the
/// binding-producing path ([`eval_list_fn_to_binding`]); a list argument here
/// yields `None` so it falls through to that path.
pub fn eval_reverse_string<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    let arg = arity1(args, "reverse")?;
    match resolve_arg_binding(arg, row, ctx)? {
        Some(Binding::Lit {
            val: fluree_db_core::FlakeValue::String(s),
            ..
        }) => Ok(Some(ComparableValue::String(Arc::from(
            s.chars().rev().collect::<String>(),
        )))),
        _ => Ok(None),
    }
}

/// Binding-producing evaluation for the list-*returning* functions
/// (`tail`, `reverse` of a list). Returns `Ok(None)` for any other function
/// (or a non-list `reverse`), so `try_eval_to_binding` falls through to the
/// scalar `ComparableValue` path.
pub fn eval_list_fn_to_binding<R: RowAccess>(
    func: &Function,
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<Binding>> {
    match func {
        Function::Tail => {
            let arg = arity1(args, "tail")?;
            match resolve_arg_binding(arg, row, ctx)? {
                Some(Binding::List(items)) => {
                    let rest = items.into_iter().skip(1).collect();
                    Ok(Some(Binding::List(rest)))
                }
                _ => Ok(Some(Binding::Unbound)),
            }
        }
        Function::Reverse => {
            let arg = arity1(args, "reverse")?;
            match resolve_arg_binding(arg, row, ctx)? {
                Some(Binding::List(mut items)) => {
                    items.reverse();
                    Ok(Some(Binding::List(items)))
                }
                // Not a list — let the scalar path handle `reverse(string)`.
                _ => Ok(None),
            }
        }
        _ => Ok(None),
    }
}
