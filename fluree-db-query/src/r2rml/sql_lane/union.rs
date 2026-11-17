//! One statement for the branches of a `UNION`: the branch plans
//! `UNION ALL`ed under shared, typed output slots, each row tagged with
//! the branch it came from, so the branch's own materializer, join plan
//! and residual filters still run over it in the engine.
//!
//! A slot is one variable's column at one type: branches binding the
//! variable on columns of the same probed type share it, a branch binding
//! it on another type gets a slot of its own, and a branch not binding it
//! pads the slot with `NULL` — which only works where the database types
//! the union's column from the branches that project a value there.

use std::collections::HashMap;
use std::sync::Arc;

use fluree_db_r2rml::mapping::CompiledR2rmlMapping;
use fluree_db_tabular::plan::{
    ColRef, OrderKey, OutputCol, OutputExpr, PushdownCapabilities, RelSource,
};
use fluree_db_tabular::{BatchSchema, FieldType};

use super::lower::{KeyShape, Lowered};
use crate::sort::SortSpec;
use crate::var_registry::VarId;

/// The alias of the union's derived table in a grouped statement.
pub(super) const UNION_ALIAS: &str = "u";

pub(crate) struct UnionLayout {
    /// Slot names in output order; the tag follows them.
    pub slots: Vec<String>,
    pub tag: String,
    /// Per branch, its statement's outputs in slot order (a `NULL` where it
    /// projects nothing there) followed by its tag.
    pub branch_outputs: Vec<Vec<OutputCol>>,
    /// Per branch, its own outputs renamed to their slots: what its
    /// materializer reads a grouped page through.
    pub renamed: Vec<Vec<OutputCol>>,
    /// Per branch, the slot of each `ORDER BY`-able column.
    order_slots: Vec<HashMap<VarId, usize>>,
    /// The slots (and their types) the seeds' key columns land in, in seed
    /// column order — the same in every branch, so one key set joins the
    /// union once instead of each branch.
    pub seed_slots: Vec<(usize, FieldType)>,
    /// Every branch's statement answers exactly its rows, so the union's
    /// `LIMIT` may be pushed.
    pub limit_is_exact: bool,
}

impl UnionLayout {
    /// The layout of `branches` as one statement, or `None` when they
    /// cannot share one: different seeds (each would need its own key
    /// set), a seed column a branch does not project or lands in another
    /// slot, a column of unknown type, padding the provider cannot type,
    /// or branches disagreeing on whether their `LIMIT` is exact.
    pub(super) fn new(
        branches: &[Lowered],
        caps: &PushdownCapabilities,
        mapping: &CompiledR2rmlMapping,
        schemas: &HashMap<RelSource, Arc<BatchSchema>>,
    ) -> Option<Self> {
        if branches.len() < 2 {
            return None;
        }
        let first = &branches[0];
        if branches[1..].iter().any(|b| {
            b.seeds.len() != first.seeds.len()
                || b.seeds
                    .iter()
                    .zip(&first.seeds)
                    .any(|(x, y)| x.var != y.var || !same_shape(&x.shape, &y.shape))
                || b.limit_is_exact != first.limit_is_exact
        }) {
            return None;
        }
        let column_type = |lowered: &Lowered, col: &ColRef| -> Option<FieldType> {
            let tm_iri = lowered
                .accesses
                .iter()
                .find(|a| a.alias == col.alias)
                .map(|a| a.tm_iri.as_str())?;
            let tm = mapping.get(tm_iri)?;
            schemas
                .get(&super::lower::source_of_tm(tm))?
                .field_by_name(&col.column)
                .map(|f| f.field_type)
        };
        // Slots keyed by (variable, column position, type); per branch,
        // the slot of each of its outputs.
        let mut keys: Vec<(VarId, usize, FieldType)> = Vec::new();
        let mut assigned: Vec<Vec<usize>> = Vec::with_capacity(branches.len());
        for b in branches {
            let mut slots_of = Vec::with_capacity(b.outputs.len());
            for o in &b.outputs {
                let col = o.expr.col()?;
                let (var, pos) = b.block_vars.iter().find_map(|v| {
                    b.var_columns
                        .get(v)?
                        .iter()
                        .position(|c| c == col)
                        .map(|p| (*v, p))
                })?;
                let ty = column_type(b, col)?;
                let key = (var, pos, ty);
                let slot = match keys.iter().position(|k| *k == key) {
                    Some(k) => k,
                    None => {
                        keys.push(key);
                        keys.len() - 1
                    }
                };
                slots_of.push(slot);
            }
            assigned.push(slots_of);
        }
        let padded = assigned.iter().any(|a| a.len() < keys.len());
        if padded && !caps.union_null_is_typed {
            return None;
        }
        let slots: Vec<String> = (0..keys.len()).map(|k| format!("c{k}")).collect();
        let tag = format!("c{}", keys.len());
        let mut seed_slots: Option<Vec<(usize, FieldType)>> = None;
        for (i, b) in branches.iter().enumerate() {
            let mine: Vec<(usize, FieldType)> = b
                .seeds
                .iter()
                .flat_map(|seed| match &seed.shape {
                    KeyShape::Template { cols, .. } => cols.clone(),
                    KeyShape::Column { col, .. } => vec![col.clone()],
                })
                .map(|col| {
                    let at = b.outputs.iter().position(|o| o.expr.col() == Some(&col))?;
                    let slot = assigned[i][at];
                    Some((slot, keys[slot].2))
                })
                .collect::<Option<_>>()?;
            match &seed_slots {
                None => seed_slots = Some(mine),
                Some(first) if *first != mine => return None,
                Some(_) => {}
            }
        }
        let mut branch_outputs = Vec::with_capacity(branches.len());
        let mut renamed = Vec::with_capacity(branches.len());
        let mut order_slots = Vec::with_capacity(branches.len());
        for (i, b) in branches.iter().enumerate() {
            let mut outs: Vec<OutputCol> = slots
                .iter()
                .map(|name| OutputCol {
                    expr: OutputExpr::Null,
                    name: name.clone(),
                })
                .collect();
            let mut own = Vec::with_capacity(b.outputs.len());
            for (o, slot) in b.outputs.iter().zip(&assigned[i]) {
                outs[*slot].expr = o.expr.clone();
                own.push(OutputCol {
                    expr: o.expr.clone(),
                    name: slots[*slot].clone(),
                });
            }
            outs.push(OutputCol {
                expr: OutputExpr::Tag(i as i64),
                name: tag.clone(),
            });
            branch_outputs.push(outs);
            renamed.push(own);
            order_slots.push(
                b.order_columns
                    .iter()
                    .filter_map(|(v, (col, _))| {
                        let at = b.outputs.iter().position(|o| o.expr.col() == Some(col))?;
                        Some((*v, assigned[i][at]))
                    })
                    .collect(),
            );
        }
        Some(Self {
            slots,
            tag,
            branch_outputs,
            renamed,
            order_slots,
            seed_slots: seed_slots.unwrap_or_default(),
            limit_is_exact: first.limit_is_exact,
        })
    }

    /// The union's `ORDER BY` for a top-k: every key must be a required,
    /// orderable column of every branch, in one slot.
    pub(super) fn order_keys(&self, ordering: &[SortSpec]) -> Option<Vec<(OrderKey, bool)>> {
        ordering
            .iter()
            .map(|s| {
                let slot = self.order_slots[0].get(&s.var)?;
                if self.order_slots[1..]
                    .iter()
                    .any(|m| m.get(&s.var) != Some(slot))
                {
                    return None;
                }
                Some((
                    OrderKey::Col(ColRef::new(UNION_ALIAS, &self.slots[*slot])),
                    s.ascending(),
                ))
            })
            .collect()
    }

    /// The grouped statement's own projection: every slot and the tag.
    pub(super) fn outputs(&self) -> Vec<OutputCol> {
        self.slots
            .iter()
            .chain(std::iter::once(&self.tag))
            .map(|name| OutputCol::column(ColRef::new(UNION_ALIAS, name), name.clone()))
            .collect()
    }
}

fn same_shape(a: &KeyShape, b: &KeyShape) -> bool {
    match (a, b) {
        (
            KeyShape::Template {
                template: ta,
                cols: ca,
                types: xa,
            },
            KeyShape::Template {
                template: tb,
                cols: cb,
                types: xb,
            },
        ) => ta == tb && ca.len() == cb.len() && xa == xb,
        (KeyShape::Column { class: ca, .. }, KeyShape::Column { class: cb, .. }) => ca == cb,
        _ => false,
    }
}
