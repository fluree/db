//! Fold `FILTER(?x = ?y)` / `FILTER(sameTerm(?x, ?y))` into an equijoin.
//!
//! When a filter equates two variables that are each bound by triple patterns
//! in the same scope, unifying the variables — renaming one to the other and
//! dropping the filter — turns a cross-product-plus-filter into a shared-
//! variable join. The stats-driven pattern reordering, the count planner, and
//! the index fast paths all then apply, which is a large, common analytic
//! speedup (e.g. BSBM BI-2: ~28s → ~0.03s).
//!
//! ## Soundness
//!
//! A join unifies on the *encoded term*; SPARQL `=` is *value* equality. They
//! coincide for nodes (IRIs / blank nodes have no `"1" = "1.0"`-style cross-
//! datatype equality) but diverge for literals. So `=` is folded only when both
//! variables are provably **node-valued**: each is used as a subject/predicate
//! position (always a node) or appears only as the object of predicates whose
//! stats prove every object is a ref ([`StatsView::is_property_ref_only`],
//! current-state-exact including novelty). `sameTerm` *is* term equality, so it
//! folds with no node guard. When node-ness can't be proven, the filter is left
//! untouched — correct, just not accelerated.
//!
//! The fold runs once over the lowered query (recursing into every sub-SELECT
//! scope) before operator construction. It only rewrites variables it can
//! safely rename; eligibility rejects any case where the renamed variable
//! appears in a pattern [`Pattern::substitute_var`] cannot rewrite (property
//! paths, search adapters) or in a nested scope.

use crate::ir::triple::{Ref, Term};
use crate::ir::{Expression, Function, Pattern, Query, SubqueryPattern};
use crate::var_registry::VarId;
use fluree_db_core::StatsView;

/// Cheap structural pre-check: does the query contain any `FILTER(?x = ?y)` /
/// `sameTerm(?x, ?y)` (recursively, including subqueries)? Lets callers skip
/// cloning the IR for the (common) queries with nothing to fold.
pub fn has_equijoin_filter(query: &Query) -> bool {
    patterns_have_equijoin_filter(&query.patterns)
}

fn patterns_have_equijoin_filter(patterns: &[Pattern]) -> bool {
    patterns.iter().any(|p| match p {
        Pattern::Filter(e) => equality_vars(e).is_some(),
        Pattern::Optional(i) | Pattern::Minus(i) | Pattern::Exists(i) | Pattern::NotExists(i) => {
            patterns_have_equijoin_filter(i)
        }
        Pattern::Union(branches) => branches.iter().any(|b| patterns_have_equijoin_filter(b)),
        Pattern::Graph { patterns, .. } => patterns_have_equijoin_filter(patterns),
        Pattern::Service(sp) => patterns_have_equijoin_filter(&sp.patterns),
        Pattern::Subquery(sq) => patterns_have_equijoin_filter(&sq.patterns),
        _ => false,
    })
}

/// Fold equijoin filters across the whole query, recursing into every subquery
/// scope. No-op without stats (the node guard needs the per-predicate datatype
/// breakdown).
pub fn fold_equijoin_filters(query: &mut Query, stats: Option<&StatsView>) {
    let Some(stats) = stats else {
        return;
    };

    // Top-level scope. We do NOT rewrite `QueryOutput`, so we only fold when the
    // projection is a concrete variable list: the survivor choice keeps a
    // projected variable (skipping when both sides are projected), so the
    // dropped variable is never an output column. A wildcard / non-tuple output
    // (`projected_vars()` is `None`, e.g. `SELECT *`, CONSTRUCT, ASK) implicitly
    // projects every variable, so dropping one would change the result shape —
    // skip the top-level scope there (subquery scopes still fold; their SELECT
    // list is always concrete and is rewritten in place).
    if let Some(output_vars) = query.output.projected_vars() {
        while let Some((drop, keep, idx)) =
            find_foldable(&query.patterns, Some(&output_vars), stats)
        {
            query.patterns.remove(idx);
            for p in &mut query.patterns {
                p.substitute_var(drop, keep);
            }
            if let Some(g) = &mut query.grouping {
                g.substitute_var(drop, keep);
            }
            for spec in &mut query.ordering {
                if spec.var == drop {
                    spec.var = keep;
                }
            }
            for (v, expr) in &mut query.order_binds {
                if *v == drop {
                    *v = keep;
                }
                expr.substitute_var(drop, keep);
            }
            // The post-query VALUES clause lives outside `patterns`; it must be
            // unified too, or a `VALUES ?y { … }` on the dropped side would be
            // left referencing a variable the join no longer produces.
            if let Some(post_values) = &mut query.post_values {
                post_values.substitute_var(drop, keep);
            }
        }
    }

    for p in &mut query.patterns {
        fold_in_pattern(p, stats);
    }
}

/// Recurse through container patterns to find and fold subquery scopes. Only
/// `SubqueryPattern` bodies are folded as scopes; OPTIONAL/UNION/etc. inner
/// blocks are traversed for nested subqueries but not folded themselves
/// (their join semantics make in-place unification subtler — conservative).
fn fold_in_pattern(p: &mut Pattern, stats: &StatsView) {
    match p {
        Pattern::Subquery(sq) => fold_subquery(sq, stats),
        Pattern::Optional(inner)
        | Pattern::Minus(inner)
        | Pattern::Exists(inner)
        | Pattern::NotExists(inner) => {
            for q in inner {
                fold_in_pattern(q, stats);
            }
        }
        Pattern::Graph { patterns, .. } => {
            for q in patterns {
                fold_in_pattern(q, stats);
            }
        }
        Pattern::Service(sp) => {
            for q in &mut sp.patterns {
                fold_in_pattern(q, stats);
            }
        }
        Pattern::Union(branches) => {
            for branch in branches {
                for q in branch {
                    fold_in_pattern(q, stats);
                }
            }
        }
        _ => {}
    }
}

/// Fold a subquery scope. The subquery's SELECT list is rewritten, so there's
/// no output restriction (unlike the top-level scope).
fn fold_subquery(sq: &mut SubqueryPattern, stats: &StatsView) {
    // Pass the SELECT list as the protected set: when both equated vars are
    // selected, keeping one and renaming the other would collapse two output
    // columns into one — so the survivor rule skips that case.
    while let Some((drop, keep, idx)) = find_foldable(&sq.patterns, Some(&sq.select), stats) {
        sq.patterns.remove(idx);
        // Rewrites SELECT list, remaining WHERE patterns, grouping, ORDER BY,
        // and expression-ORDER-BY binds in one pass.
        sq.substitute_var(drop, keep);
    }
    for p in &mut sq.patterns {
        fold_in_pattern(p, stats);
    }
}

/// Find the first foldable `FILTER(?x = ?y)` / `sameTerm` in `patterns`.
///
/// Returns `(drop, keep, filter_index)`: rename `drop` to `keep` and remove the
/// filter at `filter_index`. `output_vars` (the enclosing scope's projected
/// variables, when its projection is NOT rewritten — i.e. the top-level query)
/// constrains the survivor so the dropped variable is never a projected output.
fn find_foldable(
    patterns: &[Pattern],
    output_vars: Option<&[VarId]>,
    stats: &StatsView,
) -> Option<(VarId, VarId, usize)> {
    for (idx, p) in patterns.iter().enumerate() {
        let Pattern::Filter(expr) = p else {
            continue;
        };
        let Some((x, y, same_term)) = equality_vars(expr) else {
            continue;
        };

        // Both variables must be bound by a triple pattern at this scope level,
        // so unifying them is a valid equijoin (not a cross-scope correlation).
        if !var_bound_by_triple(patterns, x) || !var_bound_by_triple(patterns, y) {
            continue;
        }

        // Skip when a single triple already binds both vars (e.g. a self-loop
        // `?s :p ?o FILTER(?s = ?o)`): there is no cross-product to eliminate,
        // and dedicated overlay-aware encoded-filter count fast paths handle
        // that shape — folding would bypass them.
        if triple_binds_both(patterns, x, y) {
            continue;
        }

        // `=` is value equality; a join is term equality. Equivalent only when
        // both vars are node-valued. `sameTerm` is term equality unconditionally.
        if !same_term
            && (!var_node_valued(patterns, x, stats) || !var_node_valued(patterns, y, stats))
        {
            continue;
        }

        // Choose the survivor. The dropped variable is renamed away, so prefer
        // to keep a projected output variable; skip when both are projected.
        let projected = |v: VarId| output_vars.is_some_and(|o| o.contains(&v));
        let (drop, keep) = match (projected(x), projected(y)) {
            (true, true) => continue,
            (true, false) => (y, x),
            (false, true) => (x, y),
            (false, false) if x.0 <= y.0 => (y, x),
            (false, false) => (x, y),
        };

        // The dropped variable must appear only in patterns `substitute_var`
        // can rewrite (triple / filter / bind), never in a property path or
        // search adapter (where substitution is a no-op).
        if !drop_safe_to_substitute(patterns, drop) {
            continue;
        }

        return Some((drop, keep, idx));
    }
    None
}

/// Match `FILTER(?x = ?y)` or `FILTER(sameTerm(?x, ?y))`; returns the two
/// variables and whether the comparison was `sameTerm` (term equality).
fn equality_vars(expr: &Expression) -> Option<(VarId, VarId, bool)> {
    let Expression::Call { func, args } = expr else {
        return None;
    };
    let same_term = match func {
        Function::Eq => false,
        Function::SameTerm => true,
        _ => return None,
    };
    if args.len() != 2 {
        return None;
    }
    match (&args[0], &args[1]) {
        (Expression::Var(x), Expression::Var(y)) => Some((*x, *y, same_term)),
        _ => None,
    }
}

/// True if `v` appears in any position of triple pattern `tp`.
fn triple_has_var(tp: &crate::ir::TriplePattern, v: VarId) -> bool {
    matches!(tp.s, Ref::Var(x) if x == v)
        || matches!(tp.p, Ref::Var(x) if x == v)
        || matches!(tp.o, Term::Var(x) if x == v)
}

/// True if `v` appears in any top-level triple pattern (s/p/o) of `patterns`.
fn var_bound_by_triple(patterns: &[Pattern], v: VarId) -> bool {
    patterns
        .iter()
        .any(|p| matches!(p, Pattern::Triple(tp) if triple_has_var(tp, v)))
}

/// True if any single top-level triple binds BOTH `x` and `y`.
fn triple_binds_both(patterns: &[Pattern], x: VarId, y: VarId) -> bool {
    patterns
        .iter()
        .any(|p| matches!(p, Pattern::Triple(tp) if triple_has_var(tp, x) && triple_has_var(tp, y)))
}

/// True if `v` is provably node-valued across the scope's top-level triples:
/// used as a subject/predicate (always a node), or only as the object of
/// predicates whose stats prove every object is a ref.
fn var_node_valued(patterns: &[Pattern], v: VarId, stats: &StatsView) -> bool {
    let mut has_object = false;
    let mut all_object_predicates_ref = true;
    for p in patterns {
        let Pattern::Triple(tp) = p else {
            continue;
        };
        if matches!(tp.s, Ref::Var(x) if x == v) || matches!(tp.p, Ref::Var(x) if x == v) {
            return true;
        }
        if matches!(tp.o, Term::Var(x) if x == v) {
            has_object = true;
            if !predicate_ref_only(&tp.p, stats) {
                all_object_predicates_ref = false;
            }
        }
    }
    has_object && all_object_predicates_ref
}

/// Whether the predicate's objects are provably all node/IRI refs.
fn predicate_ref_only(pred: &Ref, stats: &StatsView) -> bool {
    match pred {
        Ref::Sid(sid) => stats.is_property_ref_only(sid).unwrap_or(false),
        Ref::Iri(iri) => stats.is_property_ref_only_by_iri(iri).unwrap_or(false),
        // A variable predicate has no known object datatype.
        Ref::Var(_) => false,
    }
}

/// True if the dropped variable appears only in patterns that
/// [`Pattern::substitute_var`] can rewrite (triple / filter / bind). Any other
/// top-level pattern (property path, search adapter, container, VALUES, …) that
/// references it makes the fold ineligible.
fn drop_safe_to_substitute(patterns: &[Pattern], drop: VarId) -> bool {
    patterns.iter().all(|p| match p {
        Pattern::Triple(_) | Pattern::Filter(_) | Pattern::Bind { .. } => true,
        other => !other.referenced_vars().contains(&drop),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{
        AggregateFn, AggregateSpec, Grouping, Query, QueryOutput, ReasoningConfig, TriplePattern,
    };
    use fluree_db_core::Sid;
    use fluree_graph_json_ld::ParsedContext;

    const FEAT: u16 = 100;

    fn feature_pred() -> Sid {
        Sid::new(FEAT, "feature")
    }

    // `<p1> feature ?f1 . ?other feature ?f2 . FILTER(?f1 <op> ?f2)` GROUP BY
    // ?other, COUNT(?f2). Mirrors BSBM BI-2 (shared-feature count).
    fn shared_feature_query(eq_func: Function) -> Query {
        let f1 = VarId(0);
        let f2 = VarId(1);
        let other = VarId(2);
        let cnt = VarId(3);
        let feat = feature_pred();
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(
                Ref::Sid(Sid::new(FEAT, "p1")),
                Ref::Sid(feat.clone()),
                Term::Var(f1),
            )),
            Pattern::Triple(TriplePattern::new(
                Ref::Var(other),
                Ref::Sid(feat),
                Term::Var(f2),
            )),
            Pattern::Filter(Expression::Call {
                func: eq_func,
                args: vec![Expression::Var(f1), Expression::Var(f2)],
            }),
        ];
        let grouping = Grouping::assemble(
            vec![other],
            vec![AggregateSpec {
                function: AggregateFn::Count(f2),
                output_var: cnt,
            }],
            vec![],
            None,
        );
        Query {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::select_all(vec![other, cnt]),
            patterns,
            reasoning: ReasoningConfig::default(),
            include_system_facts: false,
            grouping,
            ordering: Vec::new(),
            order_binds: Vec::new(),
            limit: None,
            offset: None,
            post_values: None,
        }
    }

    fn stats_with_feature_ref_only(ref_only: bool) -> StatsView {
        let mut stats = StatsView::default();
        stats.property_ref_only.insert(feature_pred(), ref_only);
        stats
    }

    fn count_input_var(query: &Query) -> Option<VarId> {
        query
            .grouping
            .as_ref()?
            .aggregates()
            .next()?
            .function
            .input_var()
    }

    #[test]
    fn folds_equality_on_ref_predicate() {
        let mut query = shared_feature_query(Function::Eq);
        fold_equijoin_filters(&mut query, Some(&stats_with_feature_ref_only(true)));

        // FILTER removed, two triples remain.
        assert_eq!(query.patterns.len(), 2);
        assert!(query
            .patterns
            .iter()
            .all(|p| !matches!(p, Pattern::Filter(_))));
        // The two feature objects are now the SAME variable (an equijoin), and
        // the aggregate counts that surviving variable.
        let object_vars: Vec<VarId> = query
            .patterns
            .iter()
            .filter_map(|p| match p {
                Pattern::Triple(tp) => tp.o.as_var(),
                _ => None,
            })
            .collect();
        assert_eq!(object_vars, vec![VarId(0), VarId(0)]);
        assert_eq!(count_input_var(&query), Some(VarId(0)));
    }

    #[test]
    fn does_not_fold_equality_on_literal_predicate() {
        // Not ref-only => `=` is value equality, which a term-join would not
        // preserve. The filter must stay.
        let mut query = shared_feature_query(Function::Eq);
        fold_equijoin_filters(&mut query, Some(&stats_with_feature_ref_only(false)));

        assert_eq!(query.patterns.len(), 3);
        assert!(query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Filter(_))));
        assert_eq!(count_input_var(&query), Some(VarId(1)));
    }

    #[test]
    fn does_not_fold_without_stats() {
        let mut query = shared_feature_query(Function::Eq);
        fold_equijoin_filters(&mut query, None);
        assert_eq!(query.patterns.len(), 3);
    }

    #[test]
    fn rewrites_post_values_on_dropped_var() {
        // `{ ?s1 feature ?x . ?s2 feature ?y . FILTER(?x = ?y) } VALUES ?y { … }`
        // with `?x` projected. Folding drops `?y` (renamed to `?x`); the
        // post-query VALUES must follow, or it would constrain a variable the
        // unified join no longer produces.
        let s1 = VarId(0);
        let s2 = VarId(1);
        let x = VarId(2);
        let y = VarId(3);
        let feat = feature_pred();
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(
                Ref::Var(s1),
                Ref::Sid(feat.clone()),
                Term::Var(x),
            )),
            Pattern::Triple(TriplePattern::new(
                Ref::Var(s2),
                Ref::Sid(feat),
                Term::Var(y),
            )),
            Pattern::Filter(Expression::Call {
                func: Function::Eq,
                args: vec![Expression::Var(x), Expression::Var(y)],
            }),
        ];
        let mut query = Query {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::select_all(vec![x]),
            patterns,
            reasoning: ReasoningConfig::default(),
            include_system_facts: false,
            grouping: None,
            ordering: Vec::new(),
            order_binds: Vec::new(),
            limit: None,
            offset: None,
            post_values: Some(Pattern::Values {
                vars: vec![y],
                rows: Vec::new(),
            }),
        };
        fold_equijoin_filters(&mut query, Some(&stats_with_feature_ref_only(true)));

        assert_eq!(query.patterns.len(), 2);
        match &query.post_values {
            Some(Pattern::Values { vars, .. }) => assert_eq!(vars, &vec![x]),
            other => panic!("expected post-query VALUES, got {other:?}"),
        }
    }

    #[test]
    fn does_not_fold_self_loop_in_single_triple() {
        // `?s feature ?o . FILTER(?s = ?o)` — both vars in one triple; there is
        // no cross-product to eliminate and a dedicated encoded-filter count
        // fast path owns this shape. Must not fold.
        let s = VarId(0);
        let o = VarId(1);
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(
                Ref::Var(s),
                Ref::Sid(feature_pred()),
                Term::Var(o),
            )),
            Pattern::Filter(Expression::Call {
                func: Function::Eq,
                args: vec![Expression::Var(s), Expression::Var(o)],
            }),
        ];
        let mut query = Query {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::select_all(vec![s, o]),
            patterns,
            reasoning: ReasoningConfig::default(),
            include_system_facts: false,
            grouping: None,
            ordering: Vec::new(),
            order_binds: Vec::new(),
            limit: None,
            offset: None,
            post_values: None,
        };
        fold_equijoin_filters(&mut query, Some(&stats_with_feature_ref_only(true)));
        assert_eq!(query.patterns.len(), 2);
        assert!(query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Filter(_))));
    }

    #[test]
    fn sameterm_folds_without_node_guard() {
        // sameTerm is term equality, so it folds even when the predicate is not
        // provably ref-only.
        let mut query = shared_feature_query(Function::SameTerm);
        fold_equijoin_filters(&mut query, Some(&stats_with_feature_ref_only(false)));

        assert_eq!(query.patterns.len(), 2);
        assert_eq!(count_input_var(&query), Some(VarId(0)));
    }
}
