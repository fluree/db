//! The list entries a CONSTRUCT writes.
//!
//! A query reads a list's members as values of the property holding it, with
//! no position. A CONSTRUCT template triple that restates a triple of its
//! WHERE ([`ConstructTemplate::mark_list_entries`]) writes the triples the
//! WHERE read, so where those are entries of a list, it writes that list.
//! [`ListEntriesOperator`], above the whole plan, binds for each solution the
//! positions of the entries of the owner's list that hold the triple's
//! object, and whether the owner also holds the object beside the list. A
//! value names every entry holding it, as a delete naming it does.
//!
//! A list is read from the query's default graph. An owner holding lists in
//! more than one of its graphs, like every owner over a dataset spanning
//! ledgers, binds nothing, and its triples are written as values.

use crate::binary_scan::BinaryScanOperator;
use crate::binding::{Batch, Binding};
use crate::context::{ExecutionContext, PathGraph};
use crate::error::Result;
use crate::ir::{ConstructTemplate, ListEntryVars, Ref, TriplePattern};
use crate::list_view::{is_member, member_binding, ListView};
use crate::operator::{BoxedOperator, Operator};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::{
    range_with_overlay, FlakeValue, IndexType, RangeMatch, RangeOptions, RangeTest, Sid,
};
use fluree_vocab::namespaces::XSD;
use fluree_vocab::xsd_names;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Binds the list entries of each template triple that restates a triple of
/// the WHERE (see the [module docs](self)).
pub struct ListEntriesOperator {
    child: BoxedOperator,
    /// The template triples that restate a triple of the WHERE, with the
    /// variables their list entries are bound to.
    marked: Vec<(TriplePattern, ListEntryVars)>,
    view: Arc<ListView>,
    schema: Arc<[VarId]>,
    /// Each owner's list as this request read it; `None` for an owner with
    /// no list, or lists in several graphs.
    held: HashMap<(Sid, Sid), Option<Arc<Held>>>,
    /// The owners and values whose entries a solution has been bound to:
    /// later solutions naming them are bound to none.
    bound: HashSet<(Sid, Sid, Binding)>,
}

/// An owner's list, and the values it holds beside it.
struct Held {
    /// The positions of the entries holding each value, in order.
    entries: HashMap<Binding, Vec<i64>>,
    beside: HashSet<Binding>,
}

impl ListEntriesOperator {
    pub fn new(child: BoxedOperator, template: &ConstructTemplate, view: Arc<ListView>) -> Self {
        let marked: Vec<_> = (0..template.patterns().len())
            .filter_map(|k| Some((template.patterns()[k].clone(), template.list_entries(k)?)))
            .collect();
        let mut schema = child.schema().to_vec();
        schema.extend(
            marked
                .iter()
                .flat_map(|(_, vars)| [vars.positions, vars.beside]),
        );
        Self {
            child,
            marked,
            view,
            schema: schema.into(),
            held: HashMap::new(),
            bound: HashSet::new(),
        }
    }

    /// `batch` with the list entries of each marked template triple bound.
    async fn bind(&mut self, ctx: &ExecutionContext<'_>, batch: Batch) -> Result<Batch> {
        let len = batch.len();
        let mut added: Vec<Vec<Binding>> = Vec::new();
        let graphs = if ctx.is_multi_ledger() {
            None
        } else {
            Some(ctx.path_graphs()?)
        };
        for k in 0..self.marked.len() {
            let t = self.marked[k].0.clone();
            let mut positions = Vec::with_capacity(len);
            let mut beside = Vec::with_capacity(len);
            for row in 0..len {
                let found = match &graphs {
                    Some(graphs) => self.entries(ctx, graphs, &batch, row, &t).await?,
                    None => None,
                };
                match found {
                    Some((at, also)) => {
                        positions.push(Binding::List(at));
                        beside.push(if also { yes() } else { Binding::Unbound });
                    }
                    None => {
                        positions.push(Binding::Unbound);
                        beside.push(Binding::Unbound);
                    }
                }
            }
            added.push(positions);
            added.push(beside);
        }
        let (_, mut columns, _) = batch.into_parts();
        columns.extend(added);
        Ok(Batch::from_parts(Arc::clone(&self.schema), columns, len)?)
    }

    /// The positions of the entries holding row `row`'s object of `t`, none
    /// when an earlier solution named them, and whether the owner holds it
    /// beside them; `None` when no entry does.
    async fn entries(
        &mut self,
        ctx: &ExecutionContext<'_>,
        graphs: &[PathGraph<'_>],
        batch: &Batch,
        row: usize,
        t: &TriplePattern,
    ) -> Result<Option<(Vec<Binding>, bool)>> {
        let (Some(s), Some(p)) = (node(ctx, batch, row, &t.s), node(ctx, batch, row, &t.p)) else {
            return Ok(None);
        };
        if !self.view.may_hold_lists(&p) {
            return Ok(None);
        }
        let Some(o) = t.o.as_var().and_then(|v| batch.get(row, v)) else {
            return Ok(None);
        };
        let o = decoded(ctx, o);
        let held = match self.held.get(&(s.clone(), p.clone())) {
            Some(held) => held.clone(),
            None => {
                let held = read_held(ctx, graphs, &s, &p).await?.map(Arc::new);
                self.held.insert((s.clone(), p.clone()), held.clone());
                held
            }
        };
        let Some(held) = held else {
            return Ok(None);
        };
        let Some(at) = held.entries.get(&o) else {
            return Ok(None);
        };
        let beside = held.beside.contains(&o);
        if !self.bound.insert((s, p, o)) {
            return Ok(Some((Vec::new(), beside)));
        }
        let at = at
            .iter()
            .map(|&i| Binding::lit(FlakeValue::Long(i), Sid::xsd_integer()))
            .collect();
        Ok(Some((at, beside)))
    }
}

fn yes() -> Binding {
    Binding::lit(FlakeValue::Boolean(true), Sid::new(XSD, xsd_names::BOOLEAN))
}

fn decoded(ctx: &ExecutionContext<'_>, b: &Binding) -> Binding {
    crate::group_aggregate::materialize_encoded(b, ctx.graph_view().as_ref())
}

/// The node at `r` in row `row`.
fn node(ctx: &ExecutionContext<'_>, batch: &Batch, row: usize, r: &Ref) -> Option<Sid> {
    let bound = match r {
        Ref::Var(v) => decoded(ctx, batch.get(row, *v)?),
        Ref::Sid(sid) => return Some(sid.clone()),
        Ref::Iri(iri) => return ctx.active_snapshot.encode_iri(iri),
    };
    match bound {
        Binding::Sid { sid, .. } => Some(sid),
        Binding::IriMatch { iri, .. } | Binding::Iri(iri) => ctx.active_snapshot.encode_iri(&iri),
        _ => None,
    }
}

/// The list `s` holds under `p` in `graphs`, read under each graph's view
/// policy, with the values held beside it; `None` when no graph, or more
/// than one, holds a list there.
async fn read_held(
    ctx: &ExecutionContext<'_>,
    graphs: &[PathGraph<'_>],
    s: &Sid,
    p: &Sid,
) -> Result<Option<Held>> {
    let mut entries: HashMap<Binding, Vec<i64>> = HashMap::new();
    let mut beside = HashSet::new();
    let mut lists = 0;
    for graph in graphs {
        let flakes = range_with_overlay(
            graph.snapshot,
            graph.g_id,
            graph.overlay,
            IndexType::Spot,
            RangeTest::Eq,
            RangeMatch::new()
                .with_subject(s.clone())
                .with_predicate(p.clone()),
            RangeOptions::new().with_to_t(graph.to_t),
        )
        .await?;
        let flakes: Vec<_> = flakes.into_iter().filter(|f| f.op).collect();
        ctx.tracker.consume_fuel(
            flakes.len() as u64 * fluree_db_core::tracking::schedule::PER_ROW_MICRO_FUEL,
        )?;
        let flakes = BinaryScanOperator::filter_flakes_by_enforcer(
            ctx,
            graph.policy_enforcer.as_ref(),
            graph.snapshot,
            graph.overlay,
            graph.to_t,
            graph.g_id,
            flakes,
        )
        .await?;
        let (members, values): (Vec<_>, Vec<_>) = flakes.into_iter().partition(is_member);
        if !members.is_empty() {
            lists += 1;
            for member in &members {
                if let Some(i) = member.m.as_ref().and_then(|m| m.i) {
                    entries
                        .entry(member_binding(member))
                        .or_default()
                        .push(i64::from(i));
                }
            }
        }
        beside.extend(values.iter().map(member_binding));
    }
    if lists != 1 {
        return Ok(None);
    }
    for at in entries.values_mut() {
        at.sort_unstable();
    }
    Ok(Some(Held { entries, beside }))
}

#[async_trait]
impl Operator for ListEntriesOperator {
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        vec![crate::plan_node::PlanChild::child(self.child.as_ref())]
    }

    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.held.clear();
        self.bound.clear();
        self.child.open(ctx).await
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        match self.child.next_batch(ctx).await? {
            Some(batch) => Ok(Some(self.bind(ctx, batch).await?)),
            None => Ok(None),
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.held.clear();
        self.bound.clear();
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.child.estimated_rows()
    }
}
