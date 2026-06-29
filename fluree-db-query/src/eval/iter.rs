//! List-iteration expressions — comprehensions, `reduce`, list predicates — and
//! eval-time member access. They all need the same capability: evaluate a
//! sub-expression with one or two **loop-local** variables bound to per-element
//! values. That binding is one [`RowWithLocals`] overlay, implemented here once
//! and shared by every form (and reusable by future scoped constructs).
//!
//! Null / non-list inputs yield null (`Unbound`), never an empty list. The
//! empty-list predicate identities are explicit: `all` = true, `any` = false,
//! `none` = true, `single` = false.

use crate::binding::{Binding, RowAccess};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::ir::expression::ListPredicateKind;
use crate::ir::Expression;
use crate::var_registry::VarId;

/// A row with loop-local bindings overlaid on a base row. Later locals shadow
/// earlier ones and the base (`reduce` binds `acc` then the element; nested
/// comprehensions stack frames), matching lexical scoping.
///
/// The base is `&dyn RowAccess` (not a generic `&R`) on purpose: a nested
/// comprehension overlays a `RowWithLocals` on a `RowWithLocals`, which with a
/// generic base would be an unbounded recursive type (`RowWithLocals<RowWith…>`)
/// and blow the monomorphization recursion limit. Dynamic dispatch keeps the
/// type flat at any nesting depth.
pub(crate) struct RowWithLocals<'a> {
    base: &'a dyn RowAccess,
    locals: &'a [(VarId, Binding)],
}

impl<'a> RowWithLocals<'a> {
    /// Overlay `locals` (later ones shadow earlier ones and the base) on `base`.
    /// Shared with the async metadata resolver so loop-local scoping matches the
    /// synchronous comprehension/reduce evaluators exactly.
    pub(crate) fn new(base: &'a dyn RowAccess, locals: &'a [(VarId, Binding)]) -> Self {
        RowWithLocals { base, locals }
    }
}

impl RowAccess for RowWithLocals<'_> {
    fn get(&self, var: VarId) -> Option<&Binding> {
        self.locals
            .iter()
            .rev()
            .find(|(v, _)| *v == var)
            .map(|(_, b)| b)
            .or_else(|| self.base.get(var))
    }
}

/// The elements of `list`, or `None` when it isn't a list (a null or non-list
/// value yields a null result, never an empty list).
fn list_elements<R: RowAccess>(
    list: &Expression,
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<Vec<Binding>>> {
    match list.try_eval_to_binding(row, ctx)? {
        Binding::List(items) => Ok(Some(items)),
        _ => Ok(None),
    }
}

/// `[var IN list WHERE filter | map]`.
pub fn eval_list_comprehension<R: RowAccess>(
    var: VarId,
    list: &Expression,
    filter: Option<&Expression>,
    map: Option<&Expression>,
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Binding> {
    let Some(elements) = list_elements(list, row, ctx)? else {
        return Ok(Binding::Unbound);
    };
    let mut out = Vec::with_capacity(elements.len());
    for elem in elements {
        let locals = [(var, elem.clone())];
        let scoped = RowWithLocals {
            base: row,
            locals: &locals,
        };
        if let Some(f) = filter {
            if !f.eval_to_bool(&scoped, ctx)? {
                continue;
            }
        }
        match map {
            Some(m) => out.push(m.try_eval_to_binding(&scoped, ctx)?),
            None => out.push(elem),
        }
    }
    Ok(Binding::List(out))
}

/// `reduce(acc = init, var IN list | body)`.
pub fn eval_reduce<R: RowAccess>(
    acc_var: VarId,
    init: &Expression,
    var: VarId,
    list: &Expression,
    body: &Expression,
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Binding> {
    let mut acc = init.try_eval_to_binding(row, ctx)?;
    let Some(elements) = list_elements(list, row, ctx)? else {
        return Ok(Binding::Unbound);
    };
    for elem in elements {
        let locals = [(acc_var, acc.clone()), (var, elem)];
        let scoped = RowWithLocals {
            base: row,
            locals: &locals,
        };
        acc = body.try_eval_to_binding(&scoped, ctx)?;
    }
    Ok(acc)
}

/// `all/any/none/single(var IN list WHERE predicate)`. Returns `None` for a
/// null / non-list input (Cypher null).
pub fn eval_list_predicate<R: RowAccess>(
    kind: ListPredicateKind,
    var: VarId,
    list: &Expression,
    predicate: &Expression,
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<bool>> {
    let Some(elements) = list_elements(list, row, ctx)? else {
        return Ok(None);
    };
    let mut matches = 0usize;
    for elem in elements {
        let locals = [(var, elem)];
        let scoped = RowWithLocals {
            base: row,
            locals: &locals,
        };
        let holds = predicate.eval_to_bool(&scoped, ctx)?;
        match kind {
            ListPredicateKind::All if !holds => return Ok(Some(false)),
            ListPredicateKind::Any if holds => return Ok(Some(true)),
            ListPredicateKind::None if holds => return Ok(Some(false)),
            ListPredicateKind::Single if holds => {
                matches += 1;
                if matches > 1 {
                    return Ok(Some(false));
                }
            }
            _ => {}
        }
    }
    // Empty-list / no-short-circuit identities.
    Ok(Some(match kind {
        ListPredicateKind::All | ListPredicateKind::None => true,
        ListPredicateKind::Any => false,
        ListPredicateKind::Single => matches == 1,
    }))
}

/// Eval-time `target.key` — map key lookup, or a node data-property scan.
pub fn eval_member<R: RowAccess>(
    target: &Expression,
    key: &str,
    predicate_iri: &str,
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Binding> {
    match target.try_eval_to_binding(row, ctx)? {
        Binding::Map(entries) => Ok(entries
            .iter()
            .find(|(k, _)| k.as_ref() == key)
            .map(|(_, v)| v.clone())
            .unwrap_or(Binding::Unbound)),
        node @ (Binding::Sid { .. }
        | Binding::EncodedSid { .. }
        | Binding::IriMatch { .. }
        | Binding::Iri(_)) => match ctx {
            Some(ctx) => super::metadata::eval_node_property(&node, predicate_iri, ctx),
            None => Ok(Binding::Unbound),
        },
        _ => Ok(Binding::Unbound),
    }
}
