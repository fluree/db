//! Pattern lowering — Cypher MATCH patterns → fluree-db-query Pattern.

use fluree_db_query::ir::{Pattern, Ref, Term, TriplePattern};
use fluree_db_query::parse::encode::IriEncoder;

use crate::ast::{
    Direction, Expr, Label, MapLit, NodePattern, Pattern as CypherPattern, PatternPart, RelPattern,
    Variable,
};

use super::context::LoweringContext;
use super::expr::lower_literal;
use super::{LowerError, Result};

/// Lower a Cypher pattern (used by MATCH / OPTIONAL MATCH / CREATE /
/// MERGE) into a sequence of IR patterns. The returned vector is the
/// conjunction of triple/edge-annotation patterns that make up the
/// pattern's logical match.
pub fn lower_pattern<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    pat: &CypherPattern,
) -> Result<Vec<Pattern>> {
    let mut out = Vec::new();
    for part in &pat.parts {
        lower_part(ctx, part, &mut out)?;
    }
    Ok(out)
}

fn lower_part<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    part: &PatternPart,
    out: &mut Vec<Pattern>,
) -> Result<()> {
    // Head node anchored. If tail is empty (single node) and the node
    // has no labels, no inline props, no participating relationships,
    // reject.
    if part.tail.is_empty() {
        require_node_anchored(&part.head)?;
        lower_node(ctx, &part.head, out)?;
        return Ok(());
    }

    // Otherwise, the relationship anchors the node — it can be
    // unlabeled but it must participate in a relationship.
    lower_node(ctx, &part.head, out)?;
    let mut prev = part.head.clone();
    for (rel, next) in &part.tail {
        lower_node(ctx, next, out)?;
        lower_rel(ctx, &prev, rel, next, out)?;
        prev = next.clone();
    }
    Ok(())
}

fn require_node_anchored(node: &NodePattern) -> Result<()> {
    if node.labels.is_empty() && node.props.is_none() {
        let name = node
            .var
            .as_ref()
            .map(|v| v.name.clone())
            .unwrap_or_default();
        return Err(LowerError::BareNodePattern(name));
    }
    Ok(())
}

fn node_ref<E: IriEncoder>(ctx: &mut LoweringContext<'_, E>, n: &NodePattern) -> Ref {
    // Use the same stable per-occurrence naming as `lookup_node_ref`
    // so the relationship triple and the node's own label/prop
    // triples share a variable.
    lookup_node_ref(ctx, n)
}

fn lower_node<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    n: &NodePattern,
    out: &mut Vec<Pattern>,
) -> Result<()> {
    let subj = node_ref(ctx, n);

    // Labels — emit `s rdf:type <label-iri>` for each.
    for Label { name, .. } in &n.labels {
        let iri = ctx.resolve_iri(name);
        let pred = Ref::Iri(ctx.rdf_type_iri().into());
        let obj = Term::Iri(iri.into());
        out.push(Pattern::Triple(TriplePattern::new(subj.clone(), pred, obj)));
    }

    // Inline properties — emit `s <prop-iri> <value-term>`.
    if let Some(props) = &n.props {
        lower_inline_props(ctx, &subj, props, out)?;
    }

    Ok(())
}

fn lower_inline_props<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    subj: &Ref,
    props: &MapLit,
    out: &mut Vec<Pattern>,
) -> Result<()> {
    for (key, val_expr) in &props.entries {
        let pred_iri = ctx.resolve_predicate(key)?;
        let pred = Ref::Iri(pred_iri.into());
        let obj = expr_to_object_term(ctx, val_expr)?;
        out.push(Pattern::Triple(TriplePattern::new(subj.clone(), pred, obj)));
    }
    Ok(())
}

/// Inline pattern values must reduce to a literal or a bound variable.
fn expr_to_object_term<E: IriEncoder>(ctx: &mut LoweringContext<'_, E>, e: &Expr) -> Result<Term> {
    match e {
        Expr::Lit(lit) => Ok(Term::Value(lower_literal(lit)?)),
        Expr::Var(v) => Ok(Term::Var(ctx.intern_var(&v.name))),
        _ => Err(LowerError::unsupported(
            "inline pattern property values must be literals or variables in v1",
        )),
    }
}

fn lower_rel<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    left: &NodePattern,
    rel: &RelPattern,
    right: &NodePattern,
    out: &mut Vec<Pattern>,
) -> Result<()> {
    // Reject deferred shapes early.
    if matches!(rel.direction, Direction::Either) {
        return Err(LowerError::unsupported(
            "undirected relationship `-[r]-` is rejected in v1 — write `-[r]->` or `<-[r]-`",
        ));
    }
    if rel.length.is_some() {
        return Err(LowerError::unsupported(
            "variable-length paths (`-[*N..M]->`) are deferred — see GQL_CYPHER_SUPPORT.md",
        ));
    }

    // Subject/object based on direction. Both nodes' refs were
    // already minted in `lower_node`; re-resolve them by name (or
    // re-mint anonymous via fresh_synth — but we want the same
    // var, so re-look-up).
    let left_ref = lookup_node_ref(ctx, left);
    let right_ref = lookup_node_ref(ctx, right);

    let (s, o) = match rel.direction {
        Direction::Outgoing => (left_ref, right_ref),
        Direction::Incoming => (right_ref, left_ref),
        Direction::Either => unreachable!(),
    };

    // Predicate — typed, untyped, or alternation.
    let pred = match rel.types.len() {
        0 => {
            // Untyped — use a var predicate, and the executor's
            // existing system-fact filter (set via
            // Query.include_system_facts = false) hides f:reifies*.
            Ref::Var(ctx.fresh_synth())
        }
        1 => {
            let iri = ctx.resolve_predicate(&rel.types[0].name)?;
            Ref::Iri(iri.into())
        }
        _ => {
            // Multiple types — emit a `Union` of one branch per type
            // with a concrete predicate IRI. Using a var predicate +
            // FILTER(IN ...) does not work: the predicate variable
            // binds to an IRI/SID term but the IN comparison constants
            // would be string literals, never matching.
            let mut branches: Vec<Vec<Pattern>> = Vec::with_capacity(rel.types.len());
            for t in &rel.types {
                let iri = ctx.resolve_predicate(&t.name)?;
                let mut branch = Vec::new();
                push_rel_triple(
                    ctx,
                    &rel.var,
                    &rel.props,
                    Ref::Iri(iri.into()),
                    s.clone(),
                    o.clone(),
                    &mut branch,
                )?;
                branches.push(branch);
            }
            out.push(Pattern::Union(branches));
            return Ok(());
        }
    };

    push_rel_triple(ctx, &rel.var, &rel.props, pred, s, o, out)
}

/// Determine the IR ref for an already-lowered node. Re-uses the
/// variable interning so the same Cypher variable name resolves to
/// the same VarId. Anonymous nodes — those without a `var` — get a
/// fresh synthetic in `lower_node` and another fresh synthetic here;
/// to share, we look up by deterministic naming.
///
/// **Important:** anonymous nodes are tricky — `lower_node` mints a
/// fresh synthetic each call. To share the SID between the node's
/// label/prop triples and its relationship triple, we instead key the
/// synthetic on the node's *position*. v1 simplifies this by re-doing
/// `node_ref`: when the node has a `var`, that name resolves to the
/// same VarId via the registry; when it doesn't, we call `fresh_synth`
/// once per appearance, which is **buggy** — anonymous nodes break.
/// For v1, we reject patterns that have an anonymous node *with* a
/// participating relationship until we can rework this to assign
/// stable per-pattern-part synthetic IDs.
///
/// This is a known v1 gap; tracked in the open-questions section.
fn lookup_node_ref<E: IriEncoder>(ctx: &mut LoweringContext<'_, E>, n: &NodePattern) -> Ref {
    match &n.var {
        Some(v) => Ref::Var(ctx.intern_var(&v.name)),
        None => {
            // Stable per-pattern-occurrence anon naming — derive from
            // the node's span so two lowering passes produce the
            // same name. v1: use `__anon_<offset>_<len>`.
            let name = format!("?#__anon_{}_{}", n.span.start, n.span.end);
            Ref::Var(ctx.vars.get_or_insert(&name))
        }
    }
}

fn push_rel_triple<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    rel_var: &Option<Variable>,
    rel_props: &Option<MapLit>,
    pred: Ref,
    s: Ref,
    o: Ref,
    out: &mut Vec<Pattern>,
) -> Result<()> {
    let edge_o: Term = o.into();
    match (rel_var, rel_props) {
        (None, None) => {
            // Shape 1 — plain triple, set semantics.
            out.push(Pattern::Triple(TriplePattern::new(s, pred, edge_o)));
            Ok(())
        }
        (Some(v), props) => {
            // Shape 2/3 — EdgeAnnotation with named annotation.
            let ann = Ref::Var(ctx.intern_var(&v.name));
            let body = build_annotation_body(ctx, &ann, props.as_ref())?;
            let edge = TriplePattern::new(s, pred, edge_o);
            out.push(Pattern::EdgeAnnotation {
                edge,
                annotation: ann,
                body,
            });
            Ok(())
        }
        (None, Some(props)) => {
            // Shape 3 — anonymous annotation with property filter.
            let ann = Ref::Var(ctx.fresh_synth());
            let body = build_annotation_body(ctx, &ann, Some(props))?;
            let edge = TriplePattern::new(s, pred, edge_o);
            out.push(Pattern::EdgeAnnotation {
                edge,
                annotation: ann,
                body,
            });
            Ok(())
        }
    }
}

fn build_annotation_body<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    ann: &Ref,
    props: Option<&MapLit>,
) -> Result<Vec<Pattern>> {
    let mut body = Vec::new();
    if let Some(map) = props {
        for (key, val_expr) in &map.entries {
            let pred_iri = ctx.resolve_predicate(key)?;
            let pred = Ref::Iri(pred_iri.into());
            let obj = expr_to_object_term(ctx, val_expr)?;
            body.push(Pattern::Triple(TriplePattern::new(ann.clone(), pred, obj)));
        }
    }
    Ok(body)
}
