//! Cypher metadata functions: `labels(n)` and `type(r)`.
//!
//! These read live graph facts (index + novelty overlay) rather than
//! re-deriving from pattern context.

use crate::binding::{Binding, RowAccess};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{cached_overlay_ops, subject_ref_to_s_id};
use crate::ir::{Expression, Ref};
use fluree_db_binary_index::batched_lookup_predicate_refs;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_core::comparator::IndexType;
use fluree_db_core::flake::Flake;
use fluree_db_core::o_type::OType;
use fluree_db_core::query_bounds::{RangeMatch, RangeOptions, RangeTest};
use fluree_db_core::range_provider::RangeQuery;
use fluree_db_core::{FlakeValue, NoOverlay, Sid};
use fluree_vocab::rdf;
use std::collections::HashMap;
use std::sync::Arc;

use super::value::ComparableValue;

fn arity1<'a>(args: &'a [Expression], name: &str) -> Result<&'a Expression> {
    if args.len() != 1 {
        return Err(QueryError::InvalidFilter(format!(
            "{name}() expects 1 argument, got {}",
            args.len()
        )));
    }
    Ok(&args[0])
}

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

pub(crate) fn binding_subject_sid(
    binding: &Binding,
    ctx: &ExecutionContext<'_>,
) -> Result<Option<Sid>> {
    match binding {
        Binding::Sid { sid, .. } => Ok(Some(sid.clone())),
        Binding::EncodedSid { s_id, .. } => {
            let Some(gv) = ctx.graph_view() else {
                return Ok(None);
            };
            Ok(gv.resolve_subject_sid(*s_id).ok())
        }
        Binding::IriMatch { iri, .. } | Binding::Iri(iri) => {
            if let Some(store) = ctx.binary_store.as_ref() {
                if let Ok(Some(s_id)) = store.find_subject_id(iri) {
                    if let Some(gv) = ctx.graph_view() {
                        return Ok(gv.resolve_subject_sid(s_id).ok());
                    }
                }
            }
            Ok(ctx.active_snapshot.encode_iri(iri.as_ref()))
        }
        // A relationship value's "node" for property lookup is its reifier (the
        // edge-annotation node). A plain path edge has none → no properties.
        Binding::Rel(rel) => Ok(rel.reifier.clone()),
        Binding::Unbound | Binding::Poisoned => Ok(None),
        _ => Ok(None),
    }
}

fn binding_subject_s_id(
    binding: &Binding,
    ctx: &ExecutionContext<'_>,
    store: &fluree_db_binary_index::BinaryIndexStore,
) -> Result<Option<u64>> {
    match binding {
        Binding::EncodedSid { s_id, .. } => Ok(Some(*s_id)),
        Binding::Sid { sid, .. } => {
            subject_ref_to_s_id(ctx.active_snapshot, store, &Ref::Sid(sid.clone()))
        }
        Binding::IriMatch { iri, .. } | Binding::Iri(iri) => {
            subject_ref_to_s_id(ctx.active_snapshot, store, &Ref::Iri(Arc::clone(iri)))
        }
        Binding::Unbound | Binding::Poisoned => Ok(None),
        _ => Ok(None),
    }
}

/// Extract the Cypher-local name from a full IRI (inverse of `@vocab` concat).
fn cypher_name_from_iri(iri: &str) -> String {
    if let Some((_, local)) = iri.rsplit_once('#') {
        if !local.is_empty() {
            return local.to_string();
        }
    }
    if let Some((_, local)) = iri.rsplit_once('/') {
        if !local.is_empty() {
            return local.to_string();
        }
    }
    iri.to_string()
}

fn cypher_name_from_sid(sid: &Sid, ctx: &ExecutionContext<'_>) -> Result<Option<String>> {
    if let Some(iri) = ctx
        .active_snapshot
        .decode_sid(sid)
        .or_else(|| ctx.binary_store.as_ref().and_then(|s| s.sid_to_iri(sid)))
    {
        return Ok(Some(cypher_name_from_iri(&iri)));
    }
    if let Some(store) = ctx.binary_store.as_ref() {
        if let Ok(Some(s_id)) = store.find_subject_id_by_parts(sid.namespace_code, &sid.name) {
            if let Some(resolved) = ctx.resolve_subject_iri(s_id) {
                let iri = resolved.map_err(|e| {
                    QueryError::dictionary_lookup(format!("metadata: resolve sid {s_id}: {e}"))
                })?;
                return Ok(Some(cypher_name_from_iri(&iri)));
            }
        }
    }
    Ok(Some(cypher_name_from_iri(&sid.name)))
}

fn merge_latest_ref_objects(flakes: Vec<Flake>) -> Vec<Sid> {
    let mut latest: HashMap<Sid, (i64, bool)> = HashMap::new();
    for flake in flakes {
        let FlakeValue::Ref(obj) = flake.o else {
            continue;
        };
        match latest.get(&obj) {
            None => {
                latest.insert(obj, (flake.t, flake.op));
            }
            Some(&(t0, _)) if flake.t > t0 => {
                latest.insert(obj, (flake.t, flake.op));
            }
            _ => {}
        }
    }
    let mut out: Vec<Sid> = latest
        .into_iter()
        .filter_map(|(sid, (_, op))| op.then_some(sid))
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

fn collect_subject_predicate_overlay_refs(
    ctx: &ExecutionContext<'_>,
    subject: &Sid,
    predicate: &Sid,
) -> Result<Vec<Flake>> {
    let Some(overlay) = ctx.overlay else {
        return Ok(Vec::new());
    };
    if overlay.is_effectively_empty() {
        return Ok(Vec::new());
    }
    let mut flakes = Vec::new();
    overlay.for_each_overlay_flake(
        ctx.binary_g_id,
        IndexType::Psot,
        None,
        None,
        true,
        ctx.to_t,
        &mut |flake| {
            if flake.s == *subject && flake.p == *predicate && matches!(flake.o, FlakeValue::Ref(_))
            {
                flakes.push(flake.clone());
            }
        },
    );
    Ok(flakes)
}

fn lookup_ref_objects_via_range(
    ctx: &ExecutionContext<'_>,
    subject: &Sid,
    predicate: &Sid,
) -> Result<Vec<Sid>> {
    let overlay = ctx.overlay.unwrap_or(&NoOverlay);
    let match_val = RangeMatch::subject_predicate(subject.clone(), predicate.clone());
    let opts = RangeOptions::default().with_to_t(ctx.to_t);

    let flakes = if let Some(provider) = ctx.active_snapshot.range_provider.as_ref() {
        let query = RangeQuery {
            g_id: ctx.binary_g_id,
            index: IndexType::Psot,
            test: RangeTest::Eq,
            match_val: &match_val,
            opts: &opts,
            overlay,
            tracker: Some(&ctx.tracker),
        };
        provider
            .range(&query)
            .map_err(|e| QueryError::Internal(format!("metadata range lookup: {e}")))?
    } else {
        collect_subject_predicate_overlay_refs(ctx, subject, predicate)?
    };

    Ok(merge_latest_ref_objects(flakes))
}

fn subject_ref_object_keys_merged(
    ctx: &ExecutionContext<'_>,
    store: &Arc<fluree_db_binary_index::BinaryIndexStore>,
    pred_sid: &Sid,
    p_id: u32,
    subject_s_id: u64,
) -> Result<Vec<u64>> {
    let g_id = ctx.binary_g_id;
    let mut refs = batched_lookup_predicate_refs(store, g_id, p_id, &[subject_s_id], ctx.to_t)
        .map_err(|e| QueryError::Internal(format!("batched_lookup_predicate_refs: {e}")))?
        .remove(&subject_s_id)
        .unwrap_or_default();

    if let Some(ops) = cached_overlay_ops(ctx, store, g_id, RunSortOrder::Psot, pred_sid)? {
        let iri_ref = OType::IRI_REF.as_u16();
        for op in ops.iter() {
            if op.s_id != subject_s_id || op.o_type != iri_ref {
                continue;
            }
            if op.op {
                if !refs.contains(&op.o_key) {
                    refs.push(op.o_key);
                }
            } else {
                refs.retain(|k| *k != op.o_key);
            }
        }
    }

    refs.sort_unstable();
    refs.dedup();
    Ok(refs)
}

fn lookup_ref_objects(
    ctx: &ExecutionContext<'_>,
    subject: &Sid,
    predicate: &Sid,
) -> Result<Vec<Sid>> {
    if let Some(store) = ctx.binary_store.as_ref() {
        if let Some(subject_s_id) = binding_subject_s_id(
            &Binding::Sid {
                sid: subject.clone(),
                t: None,
                op: None,
            },
            ctx,
            store,
        )? {
            let p_id = store
                .sid_to_p_id(predicate)
                .or_else(|| store.find_predicate_id(&predicate.name))
                .ok_or_else(|| {
                    QueryError::execution(format!(
                        "metadata lookup: unknown predicate {}",
                        predicate.name
                    ))
                })?;
            let ref_s_ids =
                subject_ref_object_keys_merged(ctx, store, predicate, p_id, subject_s_id)?;
            let mut out = Vec::with_capacity(ref_s_ids.len());
            for s_id in ref_s_ids {
                if let Some(gv) = ctx.graph_view() {
                    if let Ok(sid) = gv.resolve_subject_sid(s_id) {
                        out.push(sid);
                    }
                }
            }
            if !out.is_empty() {
                return Ok(out);
            }
        }
    }
    lookup_ref_objects_via_range(ctx, subject, predicate)
}

fn string_binding(s: String, dt: &Sid) -> Binding {
    Binding::lit(FlakeValue::String(s), dt.clone())
}

fn xsd_string_sid(ctx: &ExecutionContext<'_>) -> Sid {
    ctx.binary_store
        .as_ref()
        .map(|s| s.encode_iri(fluree_vocab::xsd::STRING))
        .or_else(|| ctx.active_snapshot.encode_iri(fluree_vocab::xsd::STRING))
        .unwrap_or_else(|| Sid::new(2, "string"))
}

/// `labels(node)` → list of Cypher label strings from `rdf:type` assertions.
pub fn eval_labels_to_binding<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Binding> {
    let arg = arity1(args, "labels")?;
    let Some(ctx) = ctx else {
        return Ok(Binding::Unbound);
    };
    let Some(binding) = resolve_arg_binding(arg, row, Some(ctx))? else {
        return Ok(Binding::Unbound);
    };
    let Some(subject) = binding_subject_sid(&binding, ctx)? else {
        return Ok(Binding::Unbound);
    };

    ctx.tracker.consume_fuel(1)?;

    let rdf_type = ctx
        .active_snapshot
        .encode_iri(rdf::TYPE)
        .unwrap_or_else(|| Sid::new(3, "type"));
    let class_sids = lookup_ref_objects(ctx, &subject, &rdf_type)?;

    let dt = xsd_string_sid(ctx);
    let mut labels = Vec::with_capacity(class_sids.len());
    for class_sid in class_sids {
        if let Some(name) = cypher_name_from_sid(&class_sid, ctx)? {
            labels.push(string_binding(name, &dt));
        }
    }
    Ok(Binding::List(labels))
}

/// All flakes for `subject` (SPOT subject-prefix), provider-merged with the
/// novelty overlay (falling back to an overlay-only scan when no range provider
/// is present). Used by `keys` / `properties` to enumerate a node's predicates.
fn subject_all_flakes(ctx: &ExecutionContext<'_>, subject: &Sid) -> Result<Vec<Flake>> {
    let overlay = ctx.overlay.unwrap_or(&NoOverlay);
    let match_val = RangeMatch::subject(subject.clone());
    let opts = RangeOptions::default().with_to_t(ctx.to_t);
    if let Some(provider) = ctx.active_snapshot.range_provider.as_ref() {
        let query = RangeQuery {
            g_id: ctx.binary_g_id,
            index: IndexType::Spot,
            test: RangeTest::Eq,
            match_val: &match_val,
            opts: &opts,
            overlay,
            tracker: Some(&ctx.tracker),
        };
        return provider
            .range(&query)
            .map_err(|e| QueryError::Internal(format!("properties/keys range lookup: {e}")));
    }
    // Overlay-only fallback (no index provider).
    let mut flakes = Vec::new();
    overlay.for_each_overlay_flake(
        ctx.binary_g_id,
        IndexType::Spot,
        None,
        None,
        true,
        ctx.to_t,
        &mut |flake| {
            if flake.s == *subject {
                flakes.push(flake.clone());
            }
        },
    );
    Ok(flakes)
}

/// One asserted data property: `(predicate, value, datatype, language, list
/// index)`. The list index (`FlakeMeta::i`) orders the elements of a
/// list-valued (`@list`) property; `None` for a plain scalar.
type DataProperty = (Sid, FlakeValue, Sid, Option<String>, Option<i32>);

/// The `Binding` for a property value, carrying its language tag when present
/// (a `rdf:langString`) so map output keeps `@language`.
fn property_value_binding(val: FlakeValue, dt: Sid, lang: Option<String>) -> Binding {
    match lang {
        Some(l) => Binding::lit_lang(val, l),
        None => Binding::lit(val, dt),
    }
}

/// Resolve a node's current **data** properties (literal-valued, non-reserved
/// predicates) by replaying the subject's flakes in time order — assertions add,
/// retractions remove. Preserves multiplicity (a multi-valued predicate yields
/// several). Excludes `rdf:type`, the `f:reifies*` bundle, and relationship
/// (ref) edges.
fn subject_data_properties(ctx: &ExecutionContext<'_>, subject: &Sid) -> Result<Vec<DataProperty>> {
    let mut flakes = subject_all_flakes(ctx, subject)?;
    flakes.sort_by_key(|f| f.t);
    let mut live: Vec<DataProperty> = Vec::new();
    for flake in flakes {
        // Data properties only: skip references (relationships), rdf:type, and
        // the reifier sidecar.
        if matches!(flake.o, FlakeValue::Ref(_))
            || fluree_db_core::is_rdf_type(&flake.p)
            || fluree_db_core::is_reserved_reifies_predicate(&flake.p)
        {
            continue;
        }
        let lang = flake.m.as_ref().and_then(|m| m.lang.clone());
        let list_index = flake.m.as_ref().and_then(|m| m.i);
        let key = (
            flake.p.clone(),
            flake.o.clone(),
            flake.dt.clone(),
            lang.clone(),
            list_index,
        );
        if flake.op {
            if !live.contains(&key) {
                live.push(key);
            }
        } else {
            live.retain(|e| e != &key);
        }
    }
    Ok(live)
}

/// `keys(node)` → list of a node's data-property keys (local names), sorted and
/// de-duplicated.
pub fn eval_keys_to_binding<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Binding> {
    let arg = arity1(args, "keys")?;
    let Some(ctx) = ctx else {
        return Ok(Binding::Unbound);
    };
    let Some(binding) = resolve_arg_binding(arg, row, Some(ctx))? else {
        return Ok(Binding::Unbound);
    };
    let Some(subject) = binding_subject_sid(&binding, ctx)? else {
        return Ok(Binding::Unbound);
    };
    ctx.tracker.consume_fuel(1)?;

    let props = subject_data_properties(ctx, &subject)?;
    let dt = xsd_string_sid(ctx);
    let mut names: Vec<String> = Vec::new();
    for (pred, ..) in &props {
        if let Some(name) = cypher_name_from_sid(pred, ctx)? {
            if !names.contains(&name) {
                names.push(name);
            }
        }
    }
    names.sort_unstable();
    Ok(Binding::List(
        names.into_iter().map(|n| string_binding(n, &dt)).collect(),
    ))
}

/// `properties(node)` → a map of a node's data properties (`{key: value}`).
/// A multi-valued predicate becomes a list value under its key.
pub fn eval_properties_to_binding<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Binding> {
    let arg = arity1(args, "properties")?;
    let Some(ctx) = ctx else {
        return Ok(Binding::Unbound);
    };
    let Some(binding) = resolve_arg_binding(arg, row, Some(ctx))? else {
        return Ok(Binding::Unbound);
    };
    let Some(subject) = binding_subject_sid(&binding, ctx)? else {
        return Ok(Binding::Unbound);
    };
    ctx.tracker.consume_fuel(1)?;

    let props = subject_data_properties(ctx, &subject)?;
    // Group by key (local name), preserving first-seen order. Each value keeps
    // its list index so a list-valued property renders in `@list` order.
    let mut order: Vec<String> = Vec::new();
    let mut grouped: HashMap<String, Vec<(Binding, Option<i32>)>> = HashMap::new();
    for (pred, val, dt, lang, list_index) in props {
        let Some(name) = cypher_name_from_sid(&pred, ctx)? else {
            continue;
        };
        let value = property_value_binding(val, dt, lang);
        grouped
            .entry(name.clone())
            .or_default()
            .push((value, list_index));
        if !order.contains(&name) {
            order.push(name);
        }
    }
    let entries = order
        .into_iter()
        .map(|name| {
            let mut vals = grouped.remove(&name).unwrap_or_default();
            let value = if vals.len() == 1 {
                vals.pop().expect("len == 1").0
            } else {
                // Order list elements by their stored index (stable for absent).
                vals.sort_by_key(|(_, i)| i.unwrap_or(i32::MAX));
                Binding::List(vals.into_iter().map(|(b, _)| b).collect())
            };
            (Arc::from(name.as_str()), value)
        })
        .collect();
    Ok(Binding::Map(entries))
}

/// Eval-time single data-property lookup for a node binding: `node.<predicate>`.
/// Returns the scalar value (with language tag), a list for a multi-valued
/// `@list` property (ordered by index), or `Unbound` when absent. Used by
/// loop-local member access (`[x IN nodes(p) | x.name]`).
pub fn eval_node_property(
    node: &Binding,
    predicate_iri: &str,
    ctx: &ExecutionContext<'_>,
) -> Result<Binding> {
    let Some(subject) = binding_subject_sid(node, ctx)? else {
        return Ok(Binding::Unbound);
    };
    let pred_sid = ctx
        .binary_store
        .as_ref()
        .map(|s| s.encode_iri(predicate_iri))
        .or_else(|| ctx.active_snapshot.encode_iri(predicate_iri));
    let Some(pred_sid) = pred_sid else {
        return Ok(Binding::Unbound);
    };
    ctx.tracker.consume_fuel(1)?;

    let mut vals: Vec<(Binding, Option<i32>)> = subject_data_properties(ctx, &subject)?
        .into_iter()
        .filter(|(p, ..)| *p == pred_sid)
        .map(|(_, val, dt, lang, i)| (property_value_binding(val, dt, lang), i))
        .collect();
    Ok(match vals.len() {
        0 => Binding::Unbound,
        1 => vals.pop().expect("len == 1").0,
        _ => {
            vals.sort_by_key(|(_, i)| i.unwrap_or(i32::MAX));
            Binding::List(vals.into_iter().map(|(b, _)| b).collect())
        }
    })
}

/// `type(rel)` → relationship type string from `f:reifiesPredicate`.
pub fn eval_rel_type<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    let arg = arity1(args, "type")?;
    let Some(ctx) = ctx else {
        return Ok(None);
    };
    let Some(binding) = resolve_arg_binding(arg, row, Some(ctx))? else {
        return Ok(None);
    };

    ctx.tracker.consume_fuel(1)?;

    // A relationship value carries its predicate intrinsically (e.g. from
    // `relationships(p)`); a reifier-node binding (bound `-[r:T]->`) needs the
    // `f:reifiesPredicate` lookup.
    let pred_sid = match &binding {
        Binding::Rel(rel) => rel.predicate.clone(),
        _ => {
            let Some(reifier) = binding_subject_sid(&binding, ctx)? else {
                return Ok(None);
            };
            let reifies_pred = ctx
                .active_snapshot
                .encode_iri(fluree_vocab::reifies_iris::PREDICATE)
                .unwrap_or_else(|| {
                    Sid::new(fluree_vocab::namespaces::FLUREE_DB, "reifiesPredicate")
                });
            match lookup_ref_objects(ctx, &reifier, &reifies_pred)?
                .into_iter()
                .next()
            {
                Some(p) => p,
                None => return Ok(None),
            }
        }
    };

    let name = cypher_name_from_sid(&pred_sid, ctx)?;
    Ok(name.map(|s| ComparableValue::String(Arc::from(s))))
}

/// `startNode(rel)` → the relationship's start node ref (`f:reifiesSubject`).
pub fn eval_start_node<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_rel_endpoint(
        args,
        row,
        ctx,
        fluree_vocab::reifies_iris::SUBJECT,
        "reifiesSubject",
        "startNode",
    )
}

/// `endNode(rel)` → the relationship's end node ref (`f:reifiesObject`).
pub fn eval_end_node<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_rel_endpoint(
        args,
        row,
        ctx,
        fluree_vocab::reifies_iris::OBJECT,
        "reifiesObject",
        "endNode",
    )
}

/// Shared body for `startNode` / `endNode`: read the named `f:reifies*` ref off
/// the reifier and return it as a node ref. Mirrors [`eval_rel_type`] but yields
/// the node SID (a ref) rather than a type-name string.
fn eval_rel_endpoint<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
    reifies_iri: &str,
    reifies_local: &'static str,
    fn_name: &str,
) -> Result<Option<ComparableValue>> {
    let arg = arity1(args, fn_name)?;
    let Some(ctx) = ctx else {
        return Ok(None);
    };
    let Some(binding) = resolve_arg_binding(arg, row, Some(ctx))? else {
        return Ok(None);
    };

    // A relationship value carries its endpoints intrinsically; a reifier-node
    // binding needs the `f:reifiesSubject`/`f:reifiesObject` lookup. `is_start`
    // selects the field for the Rel case.
    if let Binding::Rel(rel) = &binding {
        let node = if reifies_iri == fluree_vocab::reifies_iris::SUBJECT {
            &rel.start
        } else {
            &rel.end
        };
        return Ok(Some(ComparableValue::Sid(node.clone())));
    }

    let Some(reifier) = binding_subject_sid(&binding, ctx)? else {
        return Ok(None);
    };

    ctx.tracker.consume_fuel(1)?;

    let reifies = ctx
        .active_snapshot
        .encode_iri(reifies_iri)
        .unwrap_or_else(|| Sid::new(fluree_vocab::namespaces::FLUREE_DB, reifies_local));
    let refs = lookup_ref_objects(ctx, &reifier, &reifies)?;
    Ok(refs.first().map(|s| ComparableValue::Sid(s.clone())))
}
