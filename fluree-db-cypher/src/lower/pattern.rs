//! Pattern lowering — Cypher MATCH patterns → fluree-db-query Pattern.

use fluree_db_query::ir::{PathModifier, Pattern, PropertyPathPattern, Ref, Term, TriplePattern};
use fluree_db_query::parse::encode::IriEncoder;

use crate::ast::{
    Direction, Expr, Label, MapLit, NodePattern, Pattern as CypherPattern, PatternPart, RelPattern,
    Variable,
};

use super::context::LoweringContext;
use super::expr::lower_literal;
use super::{LowerError, Result};

/// Upper bound on the fixed-length-chain expansion of a bounded variable-length
/// path. LDBC and similar workloads use small bounds (`*1..3`); deeper bounded
/// traversal should use an unbounded `*` instead of a huge UNION.
const MAX_BOUNDED_HOPS: u32 = 16;

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
    if rel.length.is_some() {
        return lower_var_length_rel(ctx, left, rel, right, out);
    }

    // Both nodes' refs were already minted in `lower_node`; re-resolve
    // by name so the relationship triple shares their variable.
    let left_ref = lookup_node_ref(ctx, left);
    let right_ref = lookup_node_ref(ctx, right);

    match rel.direction {
        Direction::Outgoing => {
            let mut p = build_rel_hop(ctx, left_ref, right_ref, rel)?;
            out.append(&mut p);
        }
        Direction::Incoming => {
            let mut p = build_rel_hop(ctx, right_ref, left_ref, rel)?;
            out.append(&mut p);
        }
        // Undirected `-[:T]-` — match the edge in either orientation. A
        // KNOWS-style symmetric relationship is stored once as a directed
        // triple; the reverse branch finds it via the object (`Opst`) index.
        Direction::Either => {
            let fwd = build_rel_hop(ctx, left_ref.clone(), right_ref.clone(), rel)?;
            let rev = build_rel_hop(ctx, right_ref, left_ref, rel)?;
            out.push(Pattern::Union(vec![fwd, rev]));
        }
    }
    Ok(())
}

/// Build the IR patterns for a single relationship hop in one fixed
/// orientation (`s` → `o`). Handles untyped (var predicate), single-typed,
/// and alternation (`-[:A|B]->`, a `Union` of per-type branches) relationships,
/// plus the bound/anonymous + property-filter shapes.
fn build_rel_hop<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    s: Ref,
    o: Ref,
    rel: &RelPattern,
) -> Result<Vec<Pattern>> {
    let mut out = Vec::new();
    let pred = match rel.types.len() {
        0 => {
            // Untyped — var predicate; the executor's system-fact filter
            // (`Query.include_system_facts = false`) hides `f:reifies*`.
            Ref::Var(ctx.fresh_synth())
        }
        1 => {
            let iri = ctx.resolve_predicate(&rel.types[0].name)?;
            Ref::Iri(iri.into())
        }
        _ => {
            // Alternation — a `Union` of one concrete-predicate branch per
            // type. (A var predicate + `FILTER(IN ...)` can't work: the
            // predicate binds an IRI/SID term, the IN constants are strings.)
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
            return Ok(out);
        }
    };

    push_rel_triple(ctx, &rel.var, &rel.props, pred, s, o, &mut out)?;
    Ok(out)
}

/// Lower a variable-length relationship `-[:T*m..n]->`. Anonymous, single-typed
/// relationships only — a bound relationship variable binds a *list* of
/// relationships, which needs list-valued bindings (deferred). Unbounded ranges
/// map to the existing transitive `PropertyPathPattern`; bounded ranges expand
/// to a UNION of fixed-length join chains so they reuse the ordinary join
/// machinery (and honor undirected hops as forward∪reverse).
fn lower_var_length_rel<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    left: &NodePattern,
    rel: &RelPattern,
    right: &NodePattern,
    out: &mut Vec<Pattern>,
) -> Result<()> {
    if rel.var.is_some() {
        return Err(LowerError::unsupported(
            "binding a variable to a variable-length relationship needs list-valued bindings \
             (deferred); use an anonymous `-[:T*m..n]->`",
        ));
    }
    if rel.props.is_some() {
        return Err(LowerError::unsupported(
            "property filters on a variable-length relationship are deferred",
        ));
    }
    if rel.types.len() != 1 {
        return Err(LowerError::unsupported(
            "variable-length paths need exactly one relationship type (`-[:T*m..n]->`); untyped \
             and alternation forms are deferred",
        ));
    }

    let length = rel
        .length
        .as_ref()
        .expect("caller checked length.is_some()");
    let lo = length.min.unwrap_or(1);
    let hi = length.max; // None = unbounded

    let left_ref = lookup_node_ref(ctx, left);
    let right_ref = lookup_node_ref(ctx, right);
    let type_iri = ctx.resolve_predicate(&rel.types[0].name)?;

    match hi {
        // Unbounded — reuse the transitive PropertyPath operator. Cypher `*`
        // means one-or-more (lower bound defaults to 1); `*0..` is zero-or-more.
        None => {
            let modifier = match lo {
                0 => PathModifier::ZeroOrMore,
                1 => PathModifier::OneOrMore,
                _ => {
                    return Err(LowerError::unsupported(
                        "unbounded variable-length paths with a lower bound > 1 (`*N..`) are \
                         deferred; use a bounded range like `*N..M`",
                    ))
                }
            };
            if matches!(rel.direction, Direction::Either) {
                return Err(LowerError::unsupported(
                    "unbounded undirected variable-length paths are deferred; use a bounded \
                     range like `-[:T*1..3]-`",
                ));
            }
            let (s, o) = match rel.direction {
                Direction::Outgoing => (left_ref, right_ref),
                Direction::Incoming => (right_ref, left_ref),
                Direction::Either => unreachable!(),
            };
            match ctx.encoder.encode_iri(&type_iri) {
                Some(predicate) => out.push(Pattern::PropertyPath(PropertyPathPattern::new(
                    s, predicate, modifier, o,
                ))),
                // Unknown relationship type ⇒ no such edges ⇒ no rows, matching
                // how absent labels/types and the bounded (string-IRI) path
                // behave — a missing predicate is empty, not a query error.
                None => out.push(empty_path_result(&s, &o)),
            }
            Ok(())
        }
        // Bounded — expand to a UNION of fixed-length join chains.
        Some(hi) => {
            if lo == 0 {
                return Err(LowerError::unsupported(
                    "zero-length bounded paths (`*0..M`) are deferred; use `*1..M`",
                ));
            }
            if hi < lo {
                return Err(LowerError::unsupported(
                    "variable-length path upper bound must be ≥ the lower bound",
                ));
            }
            if hi > MAX_BOUNDED_HOPS {
                return Err(LowerError::unsupported(
                    "bounded variable-length paths above 16 hops are not supported; use an \
                     unbounded `*` for deeper traversal",
                ));
            }
            let mut chains: Vec<Vec<Pattern>> = Vec::with_capacity((hi - lo + 1) as usize);
            for k in lo..=hi {
                chains.push(build_fixed_chain(
                    ctx,
                    &left_ref,
                    &right_ref,
                    k,
                    &type_iri,
                    rel.direction,
                )?);
            }
            if chains.len() == 1 {
                out.append(&mut chains.pop().expect("non-empty range yields ≥ 1 chain"));
            } else {
                out.push(Pattern::Union(chains));
            }
            Ok(())
        }
    }
}

/// Build a `k`-hop chain from `s` to `o` through `k - 1` fresh intermediate
/// nodes, each hop honoring `direction`. Uses string-IRI predicate triples so
/// an absent relationship type yields no rows rather than erroring.
fn build_fixed_chain<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    s: &Ref,
    o: &Ref,
    k: u32,
    type_iri: &str,
    direction: Direction,
) -> Result<Vec<Pattern>> {
    let mut chain = Vec::new();
    let mut prev = s.clone();
    for hop in 0..k {
        let next = if hop == k - 1 {
            o.clone()
        } else {
            Ref::Var(ctx.fresh_synth())
        };
        push_hop(&prev, &next, type_iri, direction, &mut chain);
        prev = next;
    }
    Ok(chain)
}

/// An always-empty result over the path's endpoint variables — used when a
/// variable-length path names a relationship type absent from the ledger. A
/// `Values` with zero rows yields no solutions, so the conjunction is empty
/// (the same outcome as the bounded string-IRI path probing a missing type).
fn empty_path_result(s: &Ref, o: &Ref) -> Pattern {
    let mut vars = Vec::new();
    for r in [s, o] {
        if let Ref::Var(v) = r {
            if !vars.contains(v) {
                vars.push(*v);
            }
        }
    }
    Pattern::Values {
        vars,
        rows: Vec::new(),
    }
}

/// Push one hop between `a` and `b`. Directed hops emit a single triple;
/// undirected hops emit a forward∪reverse `Union`.
fn push_hop(a: &Ref, b: &Ref, type_iri: &str, direction: Direction, out: &mut Vec<Pattern>) {
    let pred = Ref::Iri(type_iri.into());
    let fwd = || {
        Pattern::Triple(TriplePattern::new(
            a.clone(),
            pred.clone(),
            b.clone().into(),
        ))
    };
    let rev = || {
        Pattern::Triple(TriplePattern::new(
            b.clone(),
            pred.clone(),
            a.clone().into(),
        ))
    };
    match direction {
        Direction::Outgoing => out.push(fwd()),
        Direction::Incoming => out.push(rev()),
        Direction::Either => out.push(Pattern::Union(vec![vec![fwd()], vec![rev()]])),
    }
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
