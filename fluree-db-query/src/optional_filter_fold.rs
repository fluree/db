//! Well-formed left-join simplification: convert a single-triple `OPTIONAL`
//! into a required triple when a filter in the same group is
//! **error-rejecting** on a variable only that OPTIONAL produces.
//!
//! Canonical source: the Cypher property accessor. `MATCH (n:User) WITH n
//! WHERE n.id = $id RETURN n` lowers `n.id` to
//! `OPTIONAL { ?n <id> ?v } FILTER(?v = $id)` (nullable-property
//! semantics). Rows where the OPTIONAL didn't extend leave `?v` unbound, the
//! comparison errors, and the filter drops the row — exactly the rows a
//! required `?n <id> ?v` would never produce. The two forms are therefore
//! row-for-row equivalent, but the OPTIONAL wrapper hides the pattern from
//! equality/range pushdown and selectivity estimation, turning a ~1-row seek
//! into a label/whole-graph scan plus post-filter. SPARQL
//! `OPTIONAL { … } FILTER(…)` hits the same cliff.
//!
//! The filter is kept (only the OPTIONAL wrapper is removed): substituting
//! the constant into the triple would switch value equality to index-term
//! matching, which differs on cross-datatype numeric equality.
//!
//! Soundness gates:
//! - The OPTIONAL body is exactly one triple.
//! - Some variable produced **only** by that triple (nowhere else in the
//!   whole query, so left-join compatibility semantics can't be observed) is
//!   error-rejected by a filter in the same pattern list. If the triple
//!   doesn't match a row, *all* its fresh variables are unbound, so the
//!   filter rejects the row either way.
//!
//! Profitability gates (the rewrite is sound more broadly, but a folded
//! REQUIRED triple participates in join reordering, and reorder has no view
//! of the filter's selectivity):
//! - Only **equality/IN with constants** folds — the folded triple is then a
//!   near-unique anchor, so being ordered early is exactly right. A range or
//!   string predicate (`?v >= 18`) would fold into a full property scan that
//!   can hijack the anchor from a far more selective seed (measured:
//!   filtered expansion queries went from milliseconds to timeouts when
//!   range filters folded).
//! - Queries containing property-path / shortest-path patterns are skipped
//!   entirely: paths anchor traversal order, and a folded accessor on the
//!   path's endpoint can become the driving side, re-running the traversal
//!   once per property row.

use std::collections::HashSet;

use crate::ir::{Expression, Function, Pattern, Query};
use crate::var_registry::VarId;

/// Cheap candidate test: some pattern list contains both a single-triple
/// OPTIONAL and a filter. (The full gates run in [`fold_optional_filters`].)
pub fn has_optional_filter_candidate(query: &Query) -> bool {
    fn list_has(patterns: &[Pattern]) -> bool {
        let mut has_opt = false;
        let mut has_filter = false;
        for p in patterns {
            match p {
                Pattern::Optional(inner) => {
                    if matches!(inner.as_slice(), [Pattern::Triple(_)]) {
                        has_opt = true;
                    }
                    if list_has(inner) {
                        return true;
                    }
                }
                Pattern::Filter(_) => has_filter = true,
                _ => {
                    if walk_inner_lists(p, &list_has) {
                        return true;
                    }
                }
            }
        }
        has_opt && has_filter
    }
    list_has(&query.patterns)
}

/// Apply the rewrite everywhere in the query (top level, subquery bodies,
/// and nested containers).
pub fn fold_optional_filters(query: &mut Query) {
    // Path operators anchor traversal order; see the profitability gates in
    // the module docs.
    if patterns_contain_path(&query.patterns) {
        return;
    }
    // Vars produced anywhere in the query, with multiplicity — the "produced
    // only by this optional" gate needs global knowledge (a var also produced
    // in another scope could observe left-join compatibility).
    let mut produced: Vec<VarId> = Vec::new();
    collect_produced(&query.patterns, &mut produced);
    let mut counts = std::collections::HashMap::new();
    for v in produced {
        *counts.entry(v).or_insert(0usize) += 1;
    }

    fold_in_list(&mut query.patterns, &counts);
}

fn fold_in_list(
    patterns: &mut [Pattern],
    produced_counts: &std::collections::HashMap<VarId, usize>,
) {
    // Vars rejected by some filter in THIS list.
    let mut rejected: HashSet<VarId> = HashSet::new();
    for p in patterns.iter() {
        if let Pattern::Filter(expr) = p {
            collect_rejected_vars(expr, &mut rejected);
        }
    }

    for p in patterns.iter_mut() {
        // Recurse first (subquery bodies, optional bodies, unions, ...).
        recurse_containers(p, produced_counts);

        let Pattern::Optional(inner) = p else {
            continue;
        };
        let [Pattern::Triple(tp)] = inner.as_slice() else {
            continue;
        };
        // Some fresh var of the triple (produced exactly once in the whole
        // query, i.e. only here) must be filter-rejected in this list.
        let qualifies = tp
            .produced_vars()
            .into_iter()
            .any(|v| rejected.contains(&v) && produced_counts.get(&v).copied() == Some(1));
        if qualifies {
            let tp = tp.clone();
            *p = Pattern::Triple(tp);
        }
    }
}

fn recurse_containers(p: &mut Pattern, produced_counts: &std::collections::HashMap<VarId, usize>) {
    match p {
        Pattern::Subquery(sq) => fold_in_list(&mut sq.patterns, produced_counts),
        Pattern::Optional(inner)
        | Pattern::Minus(inner)
        | Pattern::Exists(inner)
        | Pattern::NotExists(inner) => fold_in_list(inner, produced_counts),
        Pattern::Graph { patterns, .. } => fold_in_list(patterns, produced_counts),
        Pattern::Service(sp) => fold_in_list(&mut sp.patterns, produced_counts),
        Pattern::Union(branches) => {
            for branch in branches {
                fold_in_list(branch, produced_counts);
            }
        }
        _ => {}
    }
}

/// Collect produced vars across every nested pattern list, counting each
/// producing *leaf* once. Container variants recurse without re-adding their
/// inner vars (`Pattern::produced_vars` on a container already includes them,
/// which would double-count). A subquery counts its SELECT list (the producer
/// visible to the outer scope) *and* its body's leaves.
fn collect_produced(patterns: &[Pattern], out: &mut Vec<VarId>) {
    for p in patterns {
        match p {
            Pattern::Subquery(sq) => {
                out.extend(sq.select.iter().copied());
                collect_produced(&sq.patterns, out);
            }
            Pattern::Optional(inner)
            | Pattern::Minus(inner)
            | Pattern::Exists(inner)
            | Pattern::NotExists(inner) => collect_produced(inner, out),
            Pattern::Graph { name, patterns } => {
                if let crate::ir::GraphName::Var(v) = name {
                    out.push(*v);
                }
                collect_produced(patterns, out);
            }
            Pattern::Service(sp) => {
                if let crate::ir::ServiceEndpoint::Var(v) = &sp.endpoint {
                    out.push(*v);
                }
                collect_produced(&sp.patterns, out);
            }
            Pattern::Union(branches) => {
                for branch in branches {
                    collect_produced(branch, out);
                }
            }
            _ => out.extend(p.produced_vars()),
        }
    }
}

fn walk_inner_lists(p: &Pattern, f: &impl Fn(&[Pattern]) -> bool) -> bool {
    match p {
        Pattern::Subquery(sq) => f(&sq.patterns),
        Pattern::Minus(inner) | Pattern::Exists(inner) | Pattern::NotExists(inner) => f(inner),
        Pattern::Graph { patterns, .. } => f(patterns),
        Pattern::Service(sp) => f(&sp.patterns),
        Pattern::Union(branches) => branches.iter().any(|b| f(b)),
        _ => false,
    }
}

/// Add every var `v` for which `expr`, used as a filter, is guaranteed to
/// reject rows where `v` is unbound AND pins `v` to a constant — an
/// equality (`?v = const`, `sameTerm`) or constant `IN` list, possibly under
/// `AND` conjuncts. An erroring conjunct makes the whole `AND` error-or-false
/// in filter context, so any qualifying conjunct rejects the row.
fn collect_rejected_vars(expr: &Expression, out: &mut HashSet<VarId>) {
    let Expression::Call { func, args } = expr else {
        return;
    };
    match func {
        Function::And => {
            for a in args {
                collect_rejected_vars(a, out);
            }
        }
        Function::Eq | Function::SameTerm if args.len() == 2 => {
            if let (Expression::Var(v), Expression::Const(_))
            | (Expression::Const(_), Expression::Var(v)) = (&args[0], &args[1])
            {
                out.insert(*v);
            }
        }
        Function::In => {
            if let Some(Expression::Var(v)) = args.first() {
                if args[1..].iter().all(|a| matches!(a, Expression::Const(_))) {
                    out.insert(*v);
                }
            }
        }
        _ => {}
    }
}

/// Whether any pattern list in the query contains a path operator.
fn patterns_contain_path(patterns: &[Pattern]) -> bool {
    patterns.iter().any(|p| match p {
        Pattern::PropertyPath(_) | Pattern::ShortestPath(_) => true,
        Pattern::Subquery(sq) => patterns_contain_path(&sq.patterns),
        Pattern::Optional(inner)
        | Pattern::Minus(inner)
        | Pattern::Exists(inner)
        | Pattern::NotExists(inner) => patterns_contain_path(inner),
        Pattern::Graph { patterns, .. } => patterns_contain_path(patterns),
        Pattern::Service(sp) => patterns_contain_path(&sp.patterns),
        Pattern::Union(branches) => branches.iter().any(|b| patterns_contain_path(b)),
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::triple::{Ref, Term, TriplePattern};
    use crate::ir::{QueryOutput, SubqueryPattern};
    use fluree_db_core::{FlakeValue, Sid};

    fn eq_filter(v: VarId, val: i64) -> Pattern {
        Pattern::Filter(Expression::Call {
            func: Function::Eq,
            args: vec![Expression::Var(v), Expression::Const(FlakeValue::Long(val))],
        })
    }

    fn accessor_optional(subject: VarId, prop: VarId) -> Pattern {
        Pattern::Optional(vec![Pattern::Triple(TriplePattern::new(
            Ref::Var(subject),
            Ref::Sid(Sid::new(100, "id")),
            Term::Var(prop),
        ))])
    }

    fn query_with(patterns: Vec<Pattern>) -> Query {
        Query {
            context: fluree_graph_json_ld::ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::select_all(vec![VarId(0)]),
            patterns,
            reasoning: crate::ir::ReasoningConfig::default(),
            include_system_facts: false,
            grouping: None,
            ordering: Vec::new(),
            order_binds: Vec::new(),
            limit: None,
            offset: None,
            post_values: None,
        }
    }

    #[test]
    fn folds_equality_rejected_accessor() {
        let mut q = query_with(vec![
            Pattern::Triple(TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Sid(Sid::new(3, "type")),
                Term::Sid(Sid::new(100, "User")),
            )),
            accessor_optional(VarId(0), VarId(1)),
            eq_filter(VarId(1), 4112),
        ]);
        fold_optional_filters(&mut q);
        assert!(
            matches!(&q.patterns[1], Pattern::Triple(tp) if tp.o.as_var() == Some(VarId(1))),
            "optional should become required: {:?}",
            q.patterns[1]
        );
        assert!(matches!(&q.patterns[2], Pattern::Filter(_)), "filter kept");
    }

    #[test]
    fn folds_inside_subquery_body() {
        let sq = SubqueryPattern::new(
            vec![VarId(0)],
            vec![
                Pattern::Triple(TriplePattern::new(
                    Ref::Var(VarId(0)),
                    Ref::Sid(Sid::new(3, "type")),
                    Term::Sid(Sid::new(100, "User")),
                )),
                accessor_optional(VarId(0), VarId(1)),
                eq_filter(VarId(1), 7),
            ],
        );
        let mut q = query_with(vec![Pattern::Subquery(sq)]);
        fold_optional_filters(&mut q);
        let Pattern::Subquery(sq) = &q.patterns[0] else {
            panic!("subquery");
        };
        assert!(matches!(&sq.patterns[1], Pattern::Triple(_)));
    }

    #[test]
    fn keeps_optional_for_bound_check() {
        // `FILTER(!bound(?v))` keeps unbound rows — must NOT fold.
        let mut q = query_with(vec![
            accessor_optional(VarId(0), VarId(1)),
            Pattern::Filter(Expression::Call {
                func: Function::Not,
                args: vec![Expression::Call {
                    func: Function::Bound,
                    args: vec![Expression::Var(VarId(1))],
                }],
            }),
        ]);
        fold_optional_filters(&mut q);
        assert!(matches!(&q.patterns[0], Pattern::Optional(_)));
    }

    #[test]
    fn keeps_optional_when_var_produced_elsewhere() {
        // The repeated-identical-optional shape: ?v has two producers, so
        // left-join compatibility is observable — must NOT fold.
        let mut q = query_with(vec![
            accessor_optional(VarId(0), VarId(1)),
            accessor_optional(VarId(0), VarId(1)),
            eq_filter(VarId(1), 1),
        ]);
        fold_optional_filters(&mut q);
        assert!(matches!(&q.patterns[0], Pattern::Optional(_)));
        assert!(matches!(&q.patterns[1], Pattern::Optional(_)));
    }

    #[test]
    fn keeps_optional_for_or_with_non_rejecting_branch() {
        // `FILTER(?v = 1 OR true)` keeps unbound rows via the OR branch.
        let mut q = query_with(vec![
            accessor_optional(VarId(0), VarId(1)),
            Pattern::Filter(Expression::Call {
                func: Function::Or,
                args: vec![
                    Expression::Call {
                        func: Function::Eq,
                        args: vec![
                            Expression::Var(VarId(1)),
                            Expression::Const(FlakeValue::Long(1)),
                        ],
                    },
                    Expression::Const(FlakeValue::Boolean(true)),
                ],
            }),
        ]);
        fold_optional_filters(&mut q);
        assert!(matches!(&q.patterns[0], Pattern::Optional(_)));
    }

    #[test]
    fn keeps_optional_for_range_comparison() {
        // A folded range predicate would become a full property scan the
        // planner can misorder as an anchor — only equality/IN folds.
        let mut q = query_with(vec![
            accessor_optional(VarId(0), VarId(1)),
            Pattern::Filter(Expression::Call {
                func: Function::Gt,
                args: vec![
                    Expression::Var(VarId(1)),
                    Expression::Const(FlakeValue::Long(30)),
                ],
            }),
        ]);
        fold_optional_filters(&mut q);
        assert!(matches!(&q.patterns[0], Pattern::Optional(_)));
    }

    #[test]
    fn keeps_optional_when_query_has_path_pattern() {
        // Paths anchor traversal order; the fold skips such queries wholesale.
        let mut q = query_with(vec![
            Pattern::PropertyPath(crate::ir::PropertyPathPattern::new_wildcard(
                Ref::Var(VarId(2)),
                crate::ir::PathModifier::OneOrMore,
                Some(1),
                Some(2),
                Ref::Var(VarId(0)),
            )),
            accessor_optional(VarId(0), VarId(1)),
            eq_filter(VarId(1), 4112),
        ]);
        fold_optional_filters(&mut q);
        assert!(matches!(&q.patterns[1], Pattern::Optional(_)));
    }

    #[test]
    fn folds_constant_in_list() {
        let mut q = query_with(vec![
            accessor_optional(VarId(0), VarId(1)),
            Pattern::Filter(Expression::Call {
                func: Function::In,
                args: vec![
                    Expression::Var(VarId(1)),
                    Expression::Const(FlakeValue::Long(1)),
                    Expression::Const(FlakeValue::Long(2)),
                ],
            }),
        ]);
        fold_optional_filters(&mut q);
        assert!(matches!(&q.patterns[0], Pattern::Triple(_)));
    }
}
