//! Pattern lowering — Cypher MATCH patterns → fluree-db-query Pattern.

use fluree_db_core::FlakeValue;
use fluree_db_query::ir::{
    Expression, Function, PathDirection, PathModifier, Pattern, PropertyPathPattern, Ref,
    ShortestPathMode, ShortestPathPattern, Term, TriplePattern,
};
use fluree_db_query::parse::encode::IriEncoder;

use crate::ast::{
    Direction, Expr, Label, MapLit, NodePattern, PathSearch, Pattern as CypherPattern, PatternPart,
    RelPattern, Variable,
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
    if let Some(search) = part.path_search {
        return lower_shortest_path(ctx, part, search, out);
    }

    // Head node anchored. If tail is empty (single node) and the node
    // has no labels, no inline props, no participating relationships,
    // reject — unless whole-graph scans are opted in, in which case the
    // node binds every distinct subject in the graph.
    if part.tail.is_empty() {
        if part.head.labels.is_empty() && part.head.props.is_none() && ctx.allow_full_scan {
            out.push(all_subjects_scan(ctx, &part.head));
            return Ok(());
        }
        require_node_anchored(&part.head)?;
        lower_node(ctx, &part.head, out)?;
        return Ok(());
    }

    // Otherwise, the relationship anchors the node — it can be
    // unlabeled but it must participate in a relationship.
    lower_node(ctx, &part.head, out)?;

    // A plain `p = …` path variable (not shortestPath) is supported for a
    // single relationship segment — fixed or variable-length — which builds
    // the path value. Reject multi-hop shapes rather than silently leaving
    // p unbound.
    let single_var_length = part.tail.len() == 1 && part.tail[0].0.length.is_some();
    let single_fixed_hop = part.tail.len() == 1 && part.tail[0].0.length.is_none();
    // A multi-hop chain of fixed single-typed directed hops builds its path
    // value from the interleaved node refs and per-hop relationship values.
    if part.path_var.is_some() && !single_var_length && !single_fixed_hop {
        return lower_multi_hop_path(ctx, part, out);
    }
    // `p = (a)-[:T]->(b)`: a fixed hop is a `*1..1` path — reuse the
    // variable-length machinery to build the path value.
    if part.path_var.is_some() && single_fixed_hop {
        let (rel, next) = &part.tail[0];
        if rel.props.is_some() {
            return Err(LowerError::unsupported(
                "a path variable over a property-filtered relationship is deferred — \
                 filter via WHERE on the relationship variable instead",
            ));
        }
        lower_node(ctx, next, out)?;
        let mut fixed = rel.clone();
        fixed.length = Some(crate::ast::LengthRange {
            min: Some(1),
            max: Some(1),
            span: rel.span,
        });
        return lower_var_length_rel(ctx, &part.head, &fixed, next, out, part.path_var.as_ref());
    }

    let mut prev = part.head.clone();
    let mut i = 0;
    while i < part.tail.len() {
        // Reachability fusion: a run of ≥2 anonymous untyped outgoing hops
        // (`-->()-->()-->x`) collapses to one exact-depth wildcard path —
        // frontier BFS with per-level dedup instead of per-walk join rows.
        // Gated statement-wide (`fuse_reachability_chains`: DISTINCT output,
        // no aggregates, interior nodes unreferenced by construction).
        if ctx.fuse_reachability_chains && part.path_var.is_none() {
            let run = fusible_run_len(&part.tail[i..]);
            if run >= 2 {
                let end_node = &part.tail[i + run - 1].1;
                lower_node(ctx, end_node, out)?;
                let s_ref = lookup_node_ref(ctx, &prev);
                let o_ref = lookup_node_ref(ctx, end_node);
                out.push(Pattern::PropertyPath(
                    fluree_db_query::ir::PropertyPathPattern::new_wildcard(
                        s_ref,
                        fluree_db_query::ir::PathModifier::OneOrMore,
                        Some(run as u32),
                        Some(run as u32),
                        o_ref,
                    ),
                ));
                prev = end_node.clone();
                i += run;
                continue;
            }
        }

        let (rel, next) = &part.tail[i];
        lower_node(ctx, next, out)?;
        let pv = if single_var_length {
            part.path_var.as_ref()
        } else {
            None
        };
        lower_rel(ctx, &prev, rel, next, out, pv)?;
        prev = next.clone();
        i += 1;
    }
    Ok(())
}

/// Length of the maximal fusible hop run at the start of `tail`: consecutive
/// plain anonymous untyped outgoing hops (no rel var/props/types/length)
/// whose *interior* nodes are fully anonymous (`()` — no var, label, or
/// props). The run's final node may be anything (it is lowered normally as
/// the path endpoint).
fn fusible_run_len(tail: &[(RelPattern, NodePattern)]) -> usize {
    let mut run = 0;
    for (idx, (rel, node)) in tail.iter().enumerate() {
        let rel_fusible = rel.length.is_none()
            && rel.types.is_empty()
            && rel.var.is_none()
            && rel.props.is_none()
            && rel.direction == Direction::Outgoing;
        if !rel_fusible {
            break;
        }
        run = idx + 1;
        // The node just reached is interior only if another hop follows; an
        // interior node must be fully anonymous to stay unobservable.
        let is_anonymous = node.var.is_none() && node.labels.is_empty() && node.props.is_none();
        if !is_anonymous {
            break;
        }
    }
    run
}

/// Lower `p = shortestPath((a)-[:T*]-(b))` / `allShortestPaths(...)` into a
/// [`Pattern::ShortestPath`]. V1 contract: the inner pattern is exactly
/// node–relationship–node over a single typed predicate; both endpoints must be
/// bound by a preceding mandatory MATCH (the planner defers the operator until
/// they are). The relationship variable / property filters and multi-hop inner
/// patterns are rejected (they need list-valued / richer path semantics).
fn lower_shortest_path<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    part: &PatternPart,
    search: PathSearch,
    out: &mut Vec<Pattern>,
) -> Result<()> {
    let path_var = part.path_var.as_ref().ok_or_else(|| {
        LowerError::unsupported("shortestPath must bind a path variable (`p = shortestPath(...)`)")
    })?;

    if part.tail.len() != 1 {
        return Err(LowerError::unsupported(
            "shortestPath inner pattern must be exactly `(a)-[:T*]-(b)` (single relationship)",
        ));
    }
    let (rel, end_node) = &part.tail[0];

    if rel.var.is_some() {
        return Err(LowerError::unsupported(
            "binding a relationship variable inside shortestPath needs list-valued path bindings \
             (deferred)",
        ));
    }
    if rel.props.is_some() {
        return Err(LowerError::unsupported(
            "property filters on a shortestPath relationship are deferred",
        ));
    }
    if rel.types.len() > 1 {
        return Err(LowerError::unsupported(
            "shortestPath over a type alternation (`-[:A|B*]-`) is deferred; use a single \
             type or the untyped form",
        ));
    }

    // Emit any label / inline-prop constraints on the endpoint nodes (no-ops
    // for bare `(p1)` / `(p2)` references), then take their refs.
    lower_node(ctx, &part.head, out)?;
    lower_node(ctx, end_node, out)?;
    let start_ref = lookup_node_ref(ctx, &part.head);
    let end_ref = lookup_node_ref(ctx, end_node);
    let path_var_id = ctx.intern_var(&path_var.name);

    let direction = match rel.direction {
        Direction::Outgoing => PathDirection::Outgoing,
        Direction::Incoming => PathDirection::Incoming,
        Direction::Either => PathDirection::Either,
    };
    let mode = match search {
        PathSearch::Shortest => ShortestPathMode::Single,
        PathSearch::AllShortest => ShortestPathMode::All,
    };
    // A bare `-[:T]-` (no `*`) inside shortestPath is a fixed single hop.
    let (min_hops, max_hops) = match &rel.length {
        Some(len) => (len.min, len.max),
        None => (Some(1), Some(1)),
    };

    // Untyped (`-[*..15]->`) → wildcard edge-set; typed → the named predicate.
    // An unknown relationship type ⇒ no edges ⇒ no path: an empty result over
    // the endpoints yields no rows (mandatory MATCH drops; an OPTIONAL wrapper
    // restores the row with the path var null).
    let predicate = match rel.types.first() {
        None => None,
        Some(t) => {
            let type_iri = ctx.resolve_predicate(&t.name)?;
            match ctx.encoder.encode_iri(&type_iri) {
                Some(sid) => Some(sid),
                None => {
                    out.push(empty_path_result(&start_ref, &end_ref));
                    return Ok(());
                }
            }
        }
    };
    out.push(Pattern::ShortestPath(ShortestPathPattern {
        start: start_ref,
        end: end_ref,
        predicate,
        direction,
        mode,
        path_var: path_var_id,
        min_hops,
        max_hops,
        // Conservatively build edges: Cypher's `relationships(p)` may read
        // them, and that usage isn't visible at pattern-lowering time.
        needs_relationships: true,
        // A trailing `WHERE all(x IN nodes(p) …)` is absorbed into this field
        // by a post-lowering pass (see `absorb_shortest_path_node_filter`).
        node_filter: None,
    }));
    Ok(())
}

/// Whole-graph node scan for an opted-in bare `MATCH (n)`: an uncorrelated
/// `SELECT DISTINCT ?n { ?n ?p ?o }` subquery. Nodes are distinct subjects —
/// an IRI referenced only as an object (never described) is not matched.
fn all_subjects_scan<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    node: &NodePattern,
) -> Pattern {
    let subj = lookup_node_ref(ctx, node);
    let n = match &subj {
        Ref::Var(v) => *v,
        // `lookup_node_ref` always yields a variable (named or anonymous).
        _ => unreachable!("node refs are variables"),
    };
    let p = ctx.fresh_synth();
    let o = ctx.fresh_synth();
    let scan = Pattern::Triple(TriplePattern::new(subj, Ref::Var(p), Term::Var(o)));
    Pattern::Subquery(
        fluree_db_query::ir::SubqueryPattern::new(vec![n], vec![scan])
            .with_distinct()
            .with_uncorrelated(),
    )
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
        let pred = ctx.iri_ref(ctx.rdf_type_iri().to_string());
        let obj = ctx.iri_term(iri);
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
        let pred = ctx.iri_ref(pred_iri);
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
    path_var: Option<&Variable>,
) -> Result<()> {
    if rel.length.is_some() {
        return lower_var_length_rel(ctx, left, rel, right, out, path_var);
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
            // (`Query.include_system_facts = false`) hides `f:reifies*`, but
            // `rdf:type` (labels) and data properties are ordinary facts and
            // must be excluded too — Cypher `-->` follows only relationships.
            // The edge-set filter below handles that for the plain-triple
            // shape; annotation shapes match reified edges only, which are
            // relationships by construction.
            Ref::Var(ctx.fresh_synth())
        }
        1 => {
            let iri = ctx.resolve_predicate(&rel.types[0].name)?;
            ctx.iri_ref(iri)
        }
        _ => {
            // Alternation — a `Union` of one concrete-predicate branch per
            // type. (A var predicate + `FILTER(IN ...)` can't work: the
            // predicate binds an IRI/SID term, the IN constants are strings.)
            let mut branches: Vec<Vec<Pattern>> = Vec::with_capacity(rel.types.len());
            for t in &rel.types {
                let iri = ctx.resolve_predicate(&t.name)?;
                let pred = ctx.iri_ref(iri);
                let mut branch = Vec::new();
                push_rel_triple(
                    ctx,
                    &rel.var,
                    &rel.props,
                    pred,
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

/// Lower `p = (a)-[:R1]->(b)-[:R2]->(c)` — a multi-hop chain of FIXED
/// single-typed directed hops — binding `p` via `MakePathHops(node0, rel1,
/// node1, rel2, node2, …)`. Hops lower as ordinary relationship patterns
/// first (so labels/props/rel vars behave normally); the path value is a
/// pure expression over the bound node refs. The caller has already lowered
/// the head node. Variable-length or undirected segments in a multi-hop
/// path value stay deferred.
fn lower_multi_hop_path<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    part: &PatternPart,
    out: &mut Vec<Pattern>,
) -> Result<()> {
    let path_var = part
        .path_var
        .as_ref()
        .expect("caller checked path_var.is_some()");
    for (rel, _) in &part.tail {
        if rel.length.is_some() {
            return Err(LowerError::unsupported(
                "a variable-length segment inside a multi-hop path value \
                 (`p = (a)-[:T*]->(b)-[:U]->(c)`) is deferred — bind the segments separately",
            ));
        }
        if rel.types.len() != 1 {
            return Err(LowerError::unsupported(
                "multi-hop path values need single-typed hops (`-[:T]->`) in v1",
            ));
        }
        if matches!(rel.direction, Direction::Either) {
            return Err(LowerError::unsupported(
                "multi-hop path values need directed hops (`->` or `<-`) in v1",
            ));
        }
    }

    let node_expr = |ctx: &mut LoweringContext<'_, E>, node: &NodePattern| {
        ref_to_expr(&lookup_node_ref(ctx, node)).ok_or_else(|| {
            LowerError::unsupported(
                "an IRI-anchored node in a multi-hop path value is deferred — bind it \
                 to a variable first",
            )
        })
    };

    let mut args: Vec<Expression> = Vec::with_capacity(part.tail.len() * 2 + 1);
    args.push(node_expr(ctx, &part.head)?);
    let mut prev = part.head.clone();
    for (rel, next) in &part.tail {
        lower_node(ctx, next, out)?;
        lower_rel(ctx, &prev, rel, next, out, None)?;
        let s_ref = lookup_node_ref(ctx, &prev);
        let o_ref = lookup_node_ref(ctx, next);
        // Stored edge orientation for the hop's relationship value.
        let (es, eo) = match rel.direction {
            Direction::Outgoing => (&s_ref, &o_ref),
            Direction::Incoming => (&o_ref, &s_ref),
            Direction::Either => unreachable!("rejected above"),
        };
        let type_iri = ctx.resolve_predicate(&rel.types[0].name)?;
        let Some(rel_expr) = make_rel_expr(ctx, &Ref::Iri(type_iri.as_str().into()), es, eo) else {
            // The type never entered the dictionary — no such edges exist, so
            // the whole path matches nothing.
            out.push(empty_path_result(&lookup_node_ref(ctx, &part.head), &o_ref));
            return Ok(());
        };
        args.push(rel_expr);
        args.push(node_expr(ctx, next)?);
        prev = next.clone();
    }
    out.push(Pattern::Bind {
        var: ctx.intern_var(&path_var.name),
        expr: Expression::call(Function::MakePathHops, args),
    });
    Ok(())
}

/// `MakeRel(start, pred, end)` for a value-only relationship variable, or
/// `None` when a term isn't expressible as an expression (an IRI-anchored
/// endpoint, or a typed predicate absent from the dictionary — which has no
/// edges anyway); callers then keep the annotation lowering.
fn make_rel_expr<E: IriEncoder>(
    ctx: &LoweringContext<'_, E>,
    pred: &Ref,
    s: &Ref,
    o: &Ref,
) -> Option<Expression> {
    let p = match pred {
        Ref::Var(v) => Expression::Var(*v),
        Ref::Sid(sid) => Expression::Const(FlakeValue::Ref(sid.clone())),
        Ref::Iri(iri) => Expression::Const(FlakeValue::Ref(ctx.encoder.encode_iri(iri)?)),
    };
    Some(Expression::call(
        Function::MakeRel,
        vec![ref_to_expr(s)?, p, ref_to_expr(o)?],
    ))
}

/// The relationship edge-set constraint for an untyped single-hop pattern
/// (`-->`): the predicate must not be `rdf:type` (a label, not a relationship)
/// and a variable object must be a node (data properties bind literals).
/// Matches the edge-set the untyped var-length wildcard path traverses.
fn untyped_edge_set_filter<E: IriEncoder>(
    ctx: &LoweringContext<'_, E>,
    pvar: fluree_db_query::var_registry::VarId,
    o: &Ref,
) -> Expression {
    let not_type = Expression::ne(
        Expression::Var(pvar),
        Expression::call(
            Function::Iri,
            vec![Expression::Const(FlakeValue::String(
                ctx.rdf_type_iri().to_string(),
            ))],
        ),
    );
    match o {
        Ref::Var(ov) => Expression::binary(
            Function::And,
            not_type,
            Expression::binary(
                Function::Or,
                Expression::call(Function::IsIri, vec![Expression::Var(*ov)]),
                Expression::call(Function::IsBlank, vec![Expression::Var(*ov)]),
            ),
        ),
        // An anchored object is a node by construction.
        _ => not_type,
    }
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
    path_var: Option<&Variable>,
) -> Result<()> {
    if rel.props.is_some() {
        // Only the bounded single-typed directed chain expansion can filter
        // per hop (each hop's annotation carries the property values).
        let bounded_typed_directed = rel.types.len() == 1
            && rel.length.as_ref().is_some_and(|l| l.max.is_some())
            && rel.length.as_ref().map_or(1, |l| l.min.unwrap_or(1)) >= 1
            && !matches!(rel.direction, Direction::Either);
        if !bounded_typed_directed {
            return Err(LowerError::unsupported(
                "property filters on a variable-length relationship are supported only on \
                 bounded single-typed directed ranges (`-[:T*1..3 {p: v}]->`) in v1",
            ));
        }
        if rel.var.is_some() || path_var.is_some() {
            return Err(LowerError::unsupported(
                "binding a relationship/path variable on a property-filtered \
                 variable-length range is deferred — filter via WHERE on the bound \
                 relationships instead",
            ));
        }
    }
    if rel.types.is_empty() {
        // Untyped wildcard paths with a rel/path binding enumerate (the
        // wildcard operator resolves each found hop's stored predicate to
        // build the values); without a binding the cheaper reachability
        // operator answers.
        if rel.var.is_some() || path_var.is_some() {
            return lower_enumerate_path(ctx, left, rel, right, out, path_var);
        }
        return lower_untyped_var_length_rel(ctx, left, rel, right, out);
    }

    let length = rel
        .length
        .as_ref()
        .expect("caller checked length.is_some()");
    let lo = length.min.unwrap_or(1);
    let hi = length.max; // None = unbounded

    let left_ref = lookup_node_ref(ctx, left);
    let right_ref = lookup_node_ref(ctx, right);

    match hi {
        // Unbounded — reuse the transitive PropertyPath operator. Cypher `*`
        // means one-or-more (lower bound defaults to 1); `*0..` is zero-or-more.
        // A type alternation `[:A|B*]` becomes an alternation-transitive path
        // whose closure follows an edge of any listed type per hop (LDBC IC12's
        // `[:HAS_TYPE|IS_SUBCLASS_OF*0..]`).
        None => {
            // The transitive operator yields reachable endpoints but no
            // enumerated hops; a rel/path binding enumerates instead.
            if rel.var.is_some() || path_var.is_some() {
                return lower_enumerate_path(ctx, left, rel, right, out, path_var);
            }
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
            // Resolve each named type to a predicate Sid. An unknown type
            // contributes no edges (matching absent-predicate semantics), so it
            // is simply dropped; only if EVERY type is unknown is the path empty.
            let mut predicates = Vec::with_capacity(rel.types.len());
            for t in &rel.types {
                let iri = ctx.resolve_predicate(&t.name)?;
                if let Some(sid) = ctx.encoder.encode_iri(&iri) {
                    predicates.push(sid);
                }
            }
            if predicates.is_empty() {
                out.push(empty_path_result(&s, &o));
            } else {
                out.push(Pattern::PropertyPath(
                    PropertyPathPattern::new_alternatives(s, predicates, modifier, o),
                ));
            }
            Ok(())
        }
        // Bounded — expand to a UNION of fixed-length join chains.
        Some(hi) => {
            // Bounded alternation would need a per-hop union inside each fixed
            // chain (combinatorial); only the unbounded transitive form supports
            // alternation today. Single-type bounded ranges expand as before.
            if rel.types.len() != 1 {
                return Err(LowerError::unsupported(
                    "bounded variable-length paths over a type alternation (`[:A|B*m..n]`) are \
                     deferred; use an unbounded `[:A|B*]` or a single type",
                ));
            }
            // Bounded shapes the fixed-chain expansion can't express — an
            // undirected hop, a zero lower bound, or a bound too deep to
            // unroll — bind via path enumeration instead.
            if (rel.var.is_some() || path_var.is_some())
                && (matches!(rel.direction, Direction::Either) || lo == 0 || hi > MAX_BOUNDED_HOPS)
            {
                return lower_enumerate_path(ctx, left, rel, right, out, path_var);
            }
            let type_iri = ctx.resolve_predicate(&rel.types[0].name)?;
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
            // The predicate SID for constructing rel/path values per branch. If the
            // type isn't in the dictionary there are no edges, so binding is moot;
            // the chains below simply match nothing.
            let pred_sid = ctx.encoder.encode_iri(&type_iri);
            let rel_var_id = rel.var.as_ref().map(|v| ctx.intern_var(&v.name));
            let path_var_id = path_var.map(|v| ctx.intern_var(&v.name));

            let mut chains: Vec<Vec<Pattern>> = Vec::with_capacity((hi - lo + 1) as usize);
            for k in lo..=hi {
                let (mut chain, nodes) = build_fixed_chain(
                    ctx,
                    &left_ref,
                    &right_ref,
                    k,
                    &type_iri,
                    rel.direction,
                    rel.props.as_ref(),
                )?;
                if let Some(pred) = &pred_sid {
                    if let Some(rv) = rel_var_id {
                        chain.push(Pattern::Bind {
                            var: rv,
                            expr: build_rel_list_expr(&nodes, pred, rel.direction)?,
                        });
                    }
                    if let Some(pv) = path_var_id {
                        chain.push(Pattern::Bind {
                            var: pv,
                            expr: build_path_expr(&nodes, pred, rel.direction)?,
                        });
                    }
                }
                chains.push(chain);
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

/// Lower a variable-length relationship that binds a relationship or path
/// variable into an `Enumerate`-mode path search: one row per path whose hop
/// count is in the pattern's bounds, with the path variable bound to the path
/// value and a relationship variable bound to `relationships(path)`. The end
/// node is produced per path when unbound, or filters the enumeration when an
/// earlier pattern bound it.
///
/// Handles the shapes the cheaper operators can't: unbounded with binding,
/// untyped (wildcard) with binding, undirected, lower bounds above 1, and
/// zero-length (`*0..`). The search enforces Cypher relationship-uniqueness
/// (trail semantics — no edge reused, but a node may be revisited via a
/// different edge).
fn lower_enumerate_path<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    left: &NodePattern,
    rel: &RelPattern,
    right: &NodePattern,
    out: &mut Vec<Pattern>,
    path_var: Option<&Variable>,
) -> Result<()> {
    if rel.types.len() > 1 {
        return Err(LowerError::unsupported(
            "binding a relationship/path variable over a type alternation (`[:A|B*]`) is \
             deferred; use a single type or the untyped form",
        ));
    }
    let length = rel
        .length
        .as_ref()
        .expect("caller checked length.is_some()");
    let lo = length.min.unwrap_or(1);
    let hi = length.max;
    if let Some(hi) = hi {
        if hi < lo {
            return Err(LowerError::unsupported(
                "variable-length path upper bound must be ≥ the lower bound",
            ));
        }
    }

    let start_ref = lookup_node_ref(ctx, left);
    let end_ref = lookup_node_ref(ctx, right);
    let direction = match rel.direction {
        Direction::Outgoing => PathDirection::Outgoing,
        Direction::Incoming => PathDirection::Incoming,
        Direction::Either => PathDirection::Either,
    };

    // Typed → the named predicate; a type absent from the dictionary has no
    // edges, so the pattern matches nothing. Untyped → wildcard edge-set.
    let predicate = match rel.types.first() {
        None => None,
        Some(t) => {
            let type_iri = ctx.resolve_predicate(&t.name)?;
            match ctx.encoder.encode_iri(&type_iri) {
                Some(sid) => Some(sid),
                None => {
                    out.push(empty_path_result(&start_ref, &end_ref));
                    return Ok(());
                }
            }
        }
    };

    let path_var_id = match path_var {
        Some(v) => ctx.intern_var(&v.name),
        // A rel-var-only binding (`-[r:T*]->`) still enumerates through a
        // path value; the synthetic path var carries it to the Bind below.
        None => ctx.fresh_synth(),
    };
    out.push(Pattern::ShortestPath(ShortestPathPattern {
        start: start_ref,
        end: end_ref,
        predicate,
        direction,
        mode: ShortestPathMode::Enumerate,
        path_var: path_var_id,
        min_hops: Some(lo),
        max_hops: hi,
        needs_relationships: true,
        // Var-length enumeration carries no `all(x IN nodes(p) …)` node filter.
        node_filter: None,
    }));
    if let Some(rv) = &rel.var {
        let rel_var_id = ctx.intern_var(&rv.name);
        out.push(Pattern::Bind {
            var: rel_var_id,
            expr: Expression::call(Function::Relationships, vec![Expression::Var(path_var_id)]),
        });
    }
    Ok(())
}

/// Lower an **untyped** variable-length relationship `-[*m..n]->` (no relationship
/// type named). This maps to a *wildcard* transitive `PropertyPathPattern`: the
/// operator follows any node→node edge per hop (skipping `rdf:type` and the
/// `f:reifies*` reifier bundle, and ignoring data properties since only `Ref`
/// objects are followed). Bounds `m..n` become the path's `min_hops`/`max_hops`.
///
/// Undirected (`-[*]-`) is rejected — the transitive operator traverses a single
/// direction (the bound endpoint drives forward/backward). A type alternation
/// cannot occur here (there are no types).
fn lower_untyped_var_length_rel<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    left: &NodePattern,
    rel: &RelPattern,
    right: &NodePattern,
    out: &mut Vec<Pattern>,
) -> Result<()> {
    if matches!(rel.direction, Direction::Either) {
        return Err(LowerError::unsupported(
            "undirected untyped variable-length paths (`-[*m..n]-`) are deferred; give a \
             direction (`-[*m..n]->` or `<-[*m..n]-`)",
        ));
    }

    let length = rel
        .length
        .as_ref()
        .expect("caller checked length.is_some()");
    let lo = length.min.unwrap_or(1);
    let hi = length.max; // None = unbounded
    match hi {
        Some(hi) => {
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
        }
        // An UNBOUNDED lower bound above 1 (`-[*2..]->`) can't be evaluated
        // soundly with the transitive operator's node-reachability state (a node
        // reached below the bound on its shortest path would be wrongly
        // suppressed, with no finite depth cap to recover it). Bounded ranges
        // (`-[*2..5]->`) are fine — give an upper bound, or name a type.
        None if lo >= 2 => {
            return Err(LowerError::unsupported(
                "an untyped variable-length path with a lower bound above 1 needs an upper \
                 bound (`-[*2..N]->`) or a named type (`-[:T*2..]->`)",
            ));
        }
        None => {}
    }

    let left_ref = lookup_node_ref(ctx, left);
    let right_ref = lookup_node_ref(ctx, right);
    let (s, o) = match rel.direction {
        Direction::Outgoing => (left_ref, right_ref),
        Direction::Incoming => (right_ref, left_ref),
        Direction::Either => unreachable!(),
    };

    // `*` / `*1..` are one-or-more (exclude the start); `*0..` is zero-or-more.
    let modifier = if lo == 0 {
        PathModifier::ZeroOrMore
    } else {
        PathModifier::OneOrMore
    };
    out.push(Pattern::PropertyPath(PropertyPathPattern::new_wildcard(
        s,
        modifier,
        Some(lo),
        hi,
        o,
    )));
    Ok(())
}

/// Build a `k`-hop chain from `s` to `o` through `k - 1` fresh intermediate
/// nodes, each hop honoring `direction`. Uses string-IRI predicate triples so
/// an absent relationship type yields no rows rather than erroring.
///
/// For `k ≥ 2` a **relationship-uniqueness** `Filter` is appended so the walk
/// can't reuse a relationship — Cypher's actual rule (no edge traversed twice;
/// a node *may* be revisited via different edges). This matches Neo4j on cyclic
/// graphs: a triangle closure `a-b-c-a` is allowed (three distinct edges),
/// while an out-and-back `a-b-a` over one edge is excluded (the edge would be
/// reused). The filter compares consecutive-node *pairs* (edges); for an
/// undirected hop an edge is the unordered pair, so the reverse orientation is
/// forbidden too.
fn build_fixed_chain<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    s: &Ref,
    o: &Ref,
    k: u32,
    type_iri: &str,
    direction: Direction,
    props: Option<&MapLit>,
) -> Result<(Vec<Pattern>, Vec<Ref>)> {
    let mut chain = Vec::new();
    let mut nodes: Vec<Ref> = vec![s.clone()];
    let mut prev = s.clone();
    for hop in 0..k {
        let next = if hop == k - 1 {
            o.clone()
        } else {
            Ref::Var(ctx.fresh_synth())
        };
        if let Some(props) = props {
            // A property-filtered hop matches only edges whose annotation
            // carries the values (one row per matching annotation).
            let (gs, go) = match direction {
                Direction::Outgoing => (prev.clone(), next.clone()),
                Direction::Incoming => (next.clone(), prev.clone()),
                Direction::Either => unreachable!("rejected by the props gate"),
            };
            let ann = Ref::Var(ctx.fresh_synth());
            let body = build_annotation_body(ctx, &ann, Some(props))?;
            chain.push(Pattern::EdgeAnnotation {
                edge: TriplePattern::new(gs, ctx.iri_ref(type_iri.to_string()), go.into()),
                annotation: ann,
                body,
            });
        } else {
            push_hop(
                &prev,
                &next,
                ctx.iri_ref(type_iri.to_string()),
                direction,
                &mut chain,
            );
        }
        nodes.push(next.clone());
        prev = next;
    }
    if k >= 2 {
        if let Some(filter) = relationship_distinctness_filter(&nodes, direction) {
            chain.push(filter);
        }
    }
    Ok((chain, nodes))
}

/// Build `MakeList([MakeRel(start, pred, end), …])` over a fixed chain's nodes,
/// one relationship per hop. Directed orientation: `Outgoing` keeps the chain
/// order (subject → object); `Incoming` flips each hop so the relationship's
/// start/end match the stored edge.
fn build_rel_list_expr(
    nodes: &[Ref],
    pred_sid: &fluree_db_core::Sid,
    direction: Direction,
) -> Result<Expression> {
    let pred_const = Expression::Const(FlakeValue::Ref(pred_sid.clone()));
    let mut rels = Vec::with_capacity(nodes.len().saturating_sub(1));
    for w in nodes.windows(2) {
        let (a, b) = match direction {
            Direction::Incoming => (&w[1], &w[0]),
            _ => (&w[0], &w[1]),
        };
        let (Some(sa), Some(sb)) = (ref_to_expr(a), ref_to_expr(b)) else {
            return Err(LowerError::unsupported(
                "binding a variable-length relationship over IRI-anchored endpoints is deferred",
            ));
        };
        rels.push(Expression::call(
            Function::MakeRel,
            vec![sa, pred_const.clone(), sb],
        ));
    }
    Ok(Expression::call(Function::MakeList, rels))
}

/// Build `MakePath(Const(Bool(forward)), pred, node0, …, nodeN)` over a fixed
/// chain's nodes (traversal order). `forward` orients each hop's edge: `Outgoing`
/// keeps node[i]→node[i+1]; `Incoming` flips to the stored edge.
fn build_path_expr(
    nodes: &[Ref],
    pred_sid: &fluree_db_core::Sid,
    direction: Direction,
) -> Result<Expression> {
    let forward = !matches!(direction, Direction::Incoming);
    let mut args = Vec::with_capacity(nodes.len() + 2);
    args.push(Expression::Const(FlakeValue::Boolean(forward)));
    args.push(Expression::Const(FlakeValue::Ref(pred_sid.clone())));
    for n in nodes {
        match ref_to_expr(n) {
            Some(e) => args.push(e),
            None => {
                return Err(LowerError::unsupported(
                    "binding a variable-length path over IRI-anchored endpoints is deferred",
                ))
            }
        }
    }
    Ok(Expression::call(Function::MakePath, args))
}

/// A `Filter` enforcing Cypher relationship-uniqueness: every pair of hops on
/// the walk must be a *different* edge. Each edge `i` spans `nodes[i]..nodes[i+1]`;
/// two edges are the same when their endpoint nodes match (ordered for a
/// directed hop, unordered for an undirected one). Equalities evaluate at
/// runtime, so `nodes[i] != nodes[j]` over the same variable is simply `false`
/// — no static analysis is needed. Returns `None` with fewer than two edges.
fn relationship_distinctness_filter(nodes: &[Ref], direction: Direction) -> Option<Pattern> {
    let edge_count = nodes.len().saturating_sub(1);
    if edge_count < 2 {
        return None;
    }
    let mut conds: Vec<Expression> = Vec::new();
    for i in 0..edge_count {
        for j in (i + 1)..edge_count {
            if let Some(c) = edges_differ(nodes, i, j, direction) {
                conds.push(c);
            }
        }
    }
    let mut iter = conds.into_iter();
    let first = iter.next()?;
    let combined = iter.fold(first, |acc, c| Expression::binary(Function::And, acc, c));
    Some(Pattern::Filter(combined))
}

/// "Edge `i` and edge `j` are different relationships." Directed: distinct
/// ordered `(source, target)` pairs. Undirected: distinct *unordered* pairs
/// (the reverse orientation is the same edge, so it's forbidden too).
fn edges_differ(nodes: &[Ref], i: usize, j: usize, direction: Direction) -> Option<Expression> {
    let (si, ti) = (&nodes[i], &nodes[i + 1]);
    let (sj, tj) = (&nodes[j], &nodes[j + 1]);
    let forward = ne_or(si, sj, ti, tj)?; // (si != sj) OR (ti != tj)
    match direction {
        Direction::Either => {
            let reverse = ne_or(si, tj, ti, sj)?; // (si != tj) OR (ti != sj)
            Some(Expression::binary(Function::And, forward, reverse))
        }
        _ => Some(forward),
    }
}

/// `(a != b) OR (c != d)`, skipping any comparison whose refs aren't
/// comparable (only IRI refs, which don't arise on the var-length chain).
fn ne_or(a: &Ref, b: &Ref, c: &Ref, d: &Ref) -> Option<Expression> {
    let pair = |x: &Ref, y: &Ref| match (ref_to_expr(x), ref_to_expr(y)) {
        (Some(ex), Some(ey)) => Some(Expression::ne(ex, ey)),
        _ => None,
    };
    match (pair(a, b), pair(c, d)) {
        (Some(l), Some(r)) => Some(Expression::binary(Function::Or, l, r)),
        (Some(x), None) | (None, Some(x)) => Some(x),
        (None, None) => None,
    }
}

fn ref_to_expr(r: &Ref) -> Option<Expression> {
    match r {
        Ref::Var(v) => Some(Expression::Var(*v)),
        Ref::Sid(s) => Some(Expression::Const(FlakeValue::Ref(s.clone()))),
        Ref::Iri(_) => None,
    }
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
fn push_hop(a: &Ref, b: &Ref, pred: Ref, direction: Direction, out: &mut Vec<Pattern>) {
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

/// Determine the IR ref for an already-lowered node, re-using variable
/// interning so the node's label/property triples and its relationship triples
/// resolve to the same `VarId`.
///
/// A named node resolves through the variable registry by name. An anonymous
/// node (no `var`) is keyed on its source span — `?#__anon_{start}_{end}` via
/// `VarRegistry::get_or_insert` — so every appearance of the same node within a
/// pattern derives the identical synthetic name and thus the same `VarId`. This
/// makes anonymous nodes participating in relationships work correctly
/// (`it_lower.rs::anonymous_relationship_lowers_to_plain_triple`).
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
    let edge_o: Term = o.clone().into();

    // Value-only bound relationship variable — the statement never reads the
    // relationship's *properties*, only its value surface (`RETURN e`,
    // `type(e)`, `startNode(e)`, …). Match the plain base triple so unreified
    // (plain-RDF) edges count too, and bind the variable per annotation when
    // the edge is reified (parallel edges stay distinct) or to a synthesized
    // relationship value when it isn't. Property-reading statements keep the
    // annotation-only lowering below.
    if let (Some(v), None) = (rel_var, rel_props) {
        if !ctx.is_annotation_dependent(&v.name) {
            if let Some(rel_value) = make_rel_expr(ctx, &pred, &s, &o) {
                let var = ctx.intern_var(&v.name);
                out.push(Pattern::Triple(TriplePattern::new(
                    s.clone(),
                    pred.clone(),
                    edge_o.clone(),
                )));
                if let Ref::Var(pv) = &pred {
                    out.push(Pattern::Filter(untyped_edge_set_filter(ctx, *pv, &o)));
                }
                // Skip the per-edge annotation probe when no edge in this
                // view can be reified: `f:reifies*` absent from the
                // dictionary, or the caller proved (index stats + overlay)
                // that no `f:reifies*` fact exists.
                let expr = if ctx.reified_edges_possible
                    && ctx
                        .encoder
                        .encode_iri(fluree_vocab::reifies_iris::SUBJECT)
                        .is_some()
                {
                    let ann = ctx.fresh_synth();
                    out.push(Pattern::Optional(vec![Pattern::EdgeAnnotation {
                        edge: TriplePattern::new(s, pred, edge_o),
                        annotation: Ref::Var(ann),
                        body: Vec::new(),
                    }]));
                    Expression::call(Function::Coalesce, vec![Expression::Var(ann), rel_value])
                } else {
                    rel_value
                };
                out.push(Pattern::Bind { var, expr });
                return Ok(());
            }
        }
    }

    match (rel_var, rel_props) {
        (None, None) => {
            // Shape 1 — plain triple, set semantics.
            out.push(Pattern::Triple(TriplePattern::new(s, pred.clone(), edge_o)));
            if let Ref::Var(pv) = &pred {
                out.push(Pattern::Filter(untyped_edge_set_filter(ctx, *pv, &o)));
            }
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
            let pred = ctx.iri_ref(pred_iri);
            let obj = expr_to_object_term(ctx, val_expr)?;
            body.push(Pattern::Triple(TriplePattern::new(ann.clone(), pred, obj)));
        }
    }
    Ok(body)
}
