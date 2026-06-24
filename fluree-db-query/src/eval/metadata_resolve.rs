//! Async pre-resolution of Cypher metadata reads under a view policy.
//!
//! The metadata functions (`labels`/`keys`/`properties`/`type`/`startNode`/
//! `endNode`) and loop-local member access (`x.prop` inside a comprehension)
//! read graph flakes lazily during scalar expression evaluation — a context
//! that is synchronous and so cannot await the engine's async policy enforcer.
//! Under a non-root policy the synchronous readers are fail-closed (they return
//! empty rather than leak unfiltered flakes), so something must evaluate these
//! reads through the policy filter *before* the synchronous evaluator runs.
//!
//! That is this module's job, mirroring how [`crate::filter`] pre-resolves
//! `EXISTS` / pattern comprehensions: walk the expression for one row, evaluate
//! every metadata read through the policy-filtered async path, and substitute an
//! [`Expression::Resolved`] holding the computed value. Comprehensions, `reduce`,
//! and list predicates are evaluated *whole* (their loop-local scope only exists
//! during iteration), so their bodies' metadata reads resolve per element.
//!
//! Gated entirely on an active non-root policy: when `ctx.allow_unfiltered()`,
//! the operators never call this and the synchronous fast path is unchanged.

use crate::binding::{Binding, RowAccess};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::eval::iter::RowWithLocals;
use crate::eval::metadata::{eval_metadata_call_async, eval_node_property_async, is_metadata_function};
use crate::ir::expression::ListPredicateKind;
use crate::ir::{Expression, FlakeValue};
use std::future::Future;
use std::pin::Pin;

/// True when `expr` contains a Cypher metadata read that must be policy-filtered
/// asynchronously: a metadata `Call`, loop-local member access, or one nested in
/// a comprehension / `reduce` / list predicate. Used as the runtime gate (with
/// an active policy) for routing to the async resolver.
pub(crate) fn contains_metadata_read(expr: &Expression) -> bool {
    match expr {
        Expression::Call { func, args } => {
            is_metadata_function(func) || args.iter().any(contains_metadata_read)
        }
        // A member target may be a node (graph read) or a map (pure) — we can't
        // tell statically, so treat any member access as a candidate. The
        // resolver dispatches correctly at eval time.
        Expression::Member { .. } => true,
        Expression::Map(entries) => entries.iter().any(|(_, v)| contains_metadata_read(v)),
        Expression::ListComprehension {
            list, filter, map, ..
        } => {
            contains_metadata_read(list)
                || filter.as_deref().is_some_and(contains_metadata_read)
                || map.as_deref().is_some_and(contains_metadata_read)
        }
        Expression::Reduce {
            init, list, body, ..
        } => {
            contains_metadata_read(init)
                || contains_metadata_read(list)
                || contains_metadata_read(body)
        }
        Expression::ListPredicate {
            list, predicate, ..
        } => contains_metadata_read(list) || contains_metadata_read(predicate),
        _ => false,
    }
}

/// Resolve every metadata read in `expr` for one row, evaluating it through the
/// policy-filtered async path and substituting an [`Expression::Resolved`].
/// Subtrees without a metadata read are cloned unchanged (cheap). The returned
/// expression is then handed to the synchronous evaluator, whose fail-closed
/// metadata readers are never reached because every read is already resolved.
pub(crate) fn resolve_row_metadata<'a>(
    expr: &'a Expression,
    row: &'a dyn RowAccess,
    ctx: &'a ExecutionContext<'a>,
) -> Pin<Box<dyn Future<Output = Result<Expression>> + Send + 'a>> {
    Box::pin(async move {
        if !contains_metadata_read(expr) {
            return Ok(expr.clone());
        }
        match expr {
            Expression::Call { func, args } if is_metadata_function(func) => {
                // Resolve args first so a nested metadata read (e.g.
                // `properties(startNode(r))`) is policy-filtered too.
                let resolved_args = resolve_args(args, row, ctx).await?;
                let scoped = RowWithLocals::new(row, &[]);
                let binding = eval_metadata_call_async(func, &resolved_args, &scoped, ctx)
                    .await?
                    .unwrap_or(Binding::Unbound);
                Ok(Expression::Resolved(Box::new(binding)))
            }
            Expression::Call { func, args } => Ok(Expression::Call {
                func: func.clone(),
                args: resolve_args(args, row, ctx).await?,
            }),
            Expression::Map(entries) => {
                let mut out = Vec::with_capacity(entries.len());
                for (k, v) in entries {
                    out.push((k.clone(), resolve_row_metadata(v, row, ctx).await?));
                }
                Ok(Expression::Map(out))
            }
            Expression::Member {
                target,
                key,
                predicate_iri,
            } => {
                let resolved_target = resolve_row_metadata(target, row, ctx).await?;
                let scoped = RowWithLocals::new(row, &[]);
                let target_binding = resolved_target.try_eval_to_binding(&scoped, Some(ctx))?;
                let binding = match target_binding {
                    Binding::Map(entries) => entries
                        .iter()
                        .find(|(k, _)| k.as_ref() == key.as_ref())
                        .map(|(_, v)| v.clone())
                        .unwrap_or(Binding::Unbound),
                    node @ (Binding::Sid { .. }
                    | Binding::EncodedSid { .. }
                    | Binding::IriMatch { .. }
                    | Binding::Iri(_)) => {
                        eval_node_property_async(&node, predicate_iri, ctx).await?
                    }
                    _ => Binding::Unbound,
                };
                Ok(Expression::Resolved(Box::new(binding)))
            }
            Expression::ListComprehension {
                var,
                list,
                filter,
                map,
            } => {
                let Some(elements) = resolve_list(list, row, ctx).await? else {
                    return Ok(Expression::Resolved(Box::new(Binding::Unbound)));
                };
                let mut out = Vec::with_capacity(elements.len());
                for elem in elements {
                    let locals = [(*var, elem.clone())];
                    let scoped = RowWithLocals::new(row, &locals);
                    if let Some(f) = filter {
                        let rf = resolve_row_metadata(f, &scoped, ctx).await?;
                        if !rf.eval_to_bool(&scoped, Some(ctx))? {
                            continue;
                        }
                    }
                    match map {
                        Some(m) => {
                            let rm = resolve_row_metadata(m, &scoped, ctx).await?;
                            out.push(rm.try_eval_to_binding(&scoped, Some(ctx))?);
                        }
                        None => out.push(elem),
                    }
                }
                Ok(Expression::Resolved(Box::new(Binding::List(out))))
            }
            Expression::Reduce {
                acc,
                init,
                var,
                list,
                body,
            } => {
                let ri = resolve_row_metadata(init, row, ctx).await?;
                let scoped0 = RowWithLocals::new(row, &[]);
                let mut acc_val = ri.try_eval_to_binding(&scoped0, Some(ctx))?;
                let Some(elements) = resolve_list(list, row, ctx).await? else {
                    return Ok(Expression::Resolved(Box::new(Binding::Unbound)));
                };
                for elem in elements {
                    let locals = [(*acc, acc_val.clone()), (*var, elem)];
                    let scoped = RowWithLocals::new(row, &locals);
                    let rb = resolve_row_metadata(body, &scoped, ctx).await?;
                    acc_val = rb.try_eval_to_binding(&scoped, Some(ctx))?;
                }
                Ok(Expression::Resolved(Box::new(acc_val)))
            }
            Expression::ListPredicate {
                kind,
                var,
                list,
                predicate,
            } => {
                let Some(elements) = resolve_list(list, row, ctx).await? else {
                    return Ok(Expression::Resolved(Box::new(Binding::Unbound)));
                };
                let mut matches = 0usize;
                let mut short: Option<bool> = None;
                for elem in elements {
                    let locals = [(*var, elem)];
                    let scoped = RowWithLocals::new(row, &locals);
                    let rp = resolve_row_metadata(predicate, &scoped, ctx).await?;
                    let holds = rp.eval_to_bool(&scoped, Some(ctx))?;
                    match kind {
                        ListPredicateKind::All if !holds => {
                            short = Some(false);
                            break;
                        }
                        ListPredicateKind::Any if holds => {
                            short = Some(true);
                            break;
                        }
                        ListPredicateKind::None if holds => {
                            short = Some(false);
                            break;
                        }
                        ListPredicateKind::Single if holds => {
                            matches += 1;
                            if matches > 1 {
                                short = Some(false);
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                let result = short.unwrap_or(match kind {
                    ListPredicateKind::All | ListPredicateKind::None => true,
                    ListPredicateKind::Any => false,
                    ListPredicateKind::Single => matches == 1,
                });
                Ok(Expression::Const(FlakeValue::Boolean(result)))
            }
            // No metadata read (caught by the guard above) or a leaf — clone.
            other => Ok(other.clone()),
        }
    })
}

async fn resolve_args(
    args: &[Expression],
    row: &dyn RowAccess,
    ctx: &ExecutionContext<'_>,
) -> Result<Vec<Expression>> {
    let mut out = Vec::with_capacity(args.len());
    for a in args {
        out.push(resolve_row_metadata(a, row, ctx).await?);
    }
    Ok(out)
}

/// Resolve a comprehension/reduce source expression and return its elements, or
/// `None` when it isn't a list (Cypher null → null result, never empty list).
async fn resolve_list(
    list: &Expression,
    row: &dyn RowAccess,
    ctx: &ExecutionContext<'_>,
) -> Result<Option<Vec<Binding>>> {
    let resolved = resolve_row_metadata(list, row, ctx).await?;
    let scoped = RowWithLocals::new(row, &[]);
    match resolved.try_eval_to_binding(&scoped, Some(ctx))? {
        Binding::List(items) => Ok(Some(items)),
        _ => Ok(None),
    }
}
