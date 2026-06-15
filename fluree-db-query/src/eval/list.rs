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

/// `list[index]` element access — 0-based, negative indexes count from the end
/// (`list[-1]` is the last element). A non-list left operand, a non-integer
/// index, or an out-of-range index yields `Binding::Unbound` (Cypher null).
///
/// Returns the element binding directly so a nested-list element survives (e.g.
/// `pathPairs(p)[0]` is itself a two-element list).
fn eval_list_index_to_binding<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Binding> {
    if args.len() != 2 {
        return Err(QueryError::InvalidFilter(format!(
            "list indexing expects 2 arguments (list, index), got {}",
            args.len()
        )));
    }
    let items = match resolve_arg_binding(&args[0], row, ctx)? {
        Some(Binding::List(items)) => items,
        _ => return Ok(Binding::Unbound),
    };
    let idx = match args[1].eval_to_comparable(row, ctx)? {
        Some(ComparableValue::Long(n)) => n,
        _ => return Ok(Binding::Unbound),
    };
    let len = items.len() as i64;
    let resolved = if idx < 0 { len + idx } else { idx };
    if resolved < 0 || resolved >= len {
        return Ok(Binding::Unbound);
    }
    Ok(items[resolved as usize].clone())
}

/// Scalar (`ComparableValue`) view of `list[index]`, for use in comparison /
/// arithmetic contexts (`WHERE pair[0] > 5`). A list-valued element collapses
/// to `None`; scalar contexts reaching one is a query error caught upstream.
pub fn eval_list_index<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    Ok(element_to_comparable(&eval_list_index_to_binding(
        args, row, ctx,
    )?))
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
        Function::ListIndex => Ok(Some(eval_list_index_to_binding(args, row, ctx)?)),
        Function::Nodes => {
            // The node sequence of a path value, as a list of node refs.
            let arg = arity1(args, "nodes")?;
            match resolve_arg_binding(arg, row, ctx)? {
                Some(Binding::Path(nodes)) => Ok(Some(Binding::List(
                    nodes.into_iter().map(Binding::sid).collect(),
                ))),
                _ => Ok(Some(Binding::Unbound)),
            }
        }
        Function::PathPairs => {
            // Consecutive node pairs of a path, each a two-element list. The
            // building block for per-edge aggregation (unwind pairs → match).
            let arg = arity1(args, "pathPairs")?;
            match resolve_arg_binding(arg, row, ctx)? {
                Some(Binding::Path(nodes)) => {
                    let pairs = nodes
                        .windows(2)
                        .map(|w| {
                            Binding::List(vec![
                                Binding::sid(w[0].clone()),
                                Binding::sid(w[1].clone()),
                            ])
                        })
                        .collect();
                    Ok(Some(Binding::List(pairs)))
                }
                _ => Ok(Some(Binding::Unbound)),
            }
        }
        Function::Range => {
            // Inclusive integer range `range(start, end[, step])`.
            if args.len() != 2 && args.len() != 3 {
                return Err(QueryError::InvalidFilter(format!(
                    "range() expects 2 or 3 arguments, got {}",
                    args.len()
                )));
            }
            let as_i64 = |e: &Expression| -> Result<Option<i64>> {
                Ok(match e.eval_to_comparable(row, ctx)? {
                    Some(ComparableValue::Long(n)) => Some(n),
                    _ => None,
                })
            };
            let (Some(start), Some(end)) = (as_i64(&args[0])?, as_i64(&args[1])?) else {
                return Ok(Some(Binding::Unbound));
            };
            let step = match args.get(2) {
                Some(e) => match as_i64(e)? {
                    Some(0) | None => {
                        return Err(QueryError::InvalidFilter(
                            "range() step must be a non-zero integer".to_string(),
                        ))
                    }
                    Some(s) => s,
                },
                None => 1,
            };
            let mut items = Vec::new();
            let mut cur = start;
            // Cap to guard against an accidental huge/unbounded range.
            const MAX_RANGE: usize = 1_000_000;
            while (step > 0 && cur <= end) || (step < 0 && cur >= end) {
                items.push(Binding::lit(
                    fluree_db_core::FlakeValue::Long(cur),
                    fluree_db_core::Sid::new(fluree_vocab::namespaces::XSD, "integer"),
                ));
                if items.len() >= MAX_RANGE {
                    return Err(QueryError::ResourceLimit(
                        "range() exceeded 1,000,000 elements".to_string(),
                    ));
                }
                cur += step;
            }
            Ok(Some(Binding::List(items)))
        }
        Function::MakeList => {
            // Build a list from each argument's binding value (preserving order
            // and nulls, so structured `collect([a, b])` keeps tuple shape).
            let mut items = Vec::with_capacity(args.len());
            for a in args {
                items.push(a.try_eval_to_binding(row, ctx)?);
            }
            Ok(Some(Binding::List(items)))
        }
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
