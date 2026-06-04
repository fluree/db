//! Rewrite `avg` over an anti-join complement into a difference of aggregates.
//!
//! BSBM BI-4's "average price of products WITHOUT feature F" sub-SELECT is
//! written as a cartesian product of every feature with every product-offer,
//! filtered by `FILTER NOT EXISTS { ?product bsbm:productFeature ?feature }`:
//!
//! ```sparql
//! SELECT ?feature (avg(EXPR(?price)) AS ?out) {
//!   { SELECT DISTINCT ?feature { ... } }               # feature universe
//!   ?product a <Type> .
//!   ?offer bsbm:product ?product ; bsbm:price ?price .  # universe offers
//!   FILTER NOT EXISTS { ?product bsbm:productFeature ?feature }
//! } GROUP BY ?feature
//! ```
//!
//! Because `SUM` and `COUNT` are distributive over set difference, the
//! complement average is derivable without the cross-product:
//!
//! ```text
//! without(F) = (universeSum - withSum(F)) / (universeCount - withCount(F))
//! ```
//!
//! where `universe` is the offers of all type-`Type` products and `withSum/Cnt(F)`
//! are over offers of products that DO have `F` (the `NOT EXISTS` inner as a
//! positive join, grouped by `?feature`). We rewrite the sub-SELECT to compute a
//! scalar universe total once and a per-feature WITH aggregate, driving the row
//! set from the original DISTINCT-feature universe (so a feature whose products
//! have no offers still appears, defaulting its WITH aggregate to 0), and drop
//! features whose complement is empty (`without count == 0`) to match the
//! original `GROUP BY`'s empty-group semantics.
//!
//! ## Scope
//!
//! Intentionally narrow: a single `AVG` aggregate, a single-key explicit
//! `GROUP BY`, one `FILTER NOT EXISTS` referencing that key, and a `DISTINCT`
//! feature-universe subquery var-disjoint from the universe triples. Anything
//! else is left untouched (safe no-op). `AVG` only — the empty-complement filter
//! needs the count, and `MIN`/`MAX`/`COUNT(DISTINCT)`/etc. are not distributive.

use crate::ir::{
    AggregateFn, AggregateSpec, Aggregation, Expression, Function, Grouping, InputSemantics,
    Pattern, Query, SubqueryPattern,
};
use crate::var_registry::VarId;
use fluree_db_core::{FlakeValue, NonEmpty};
use std::collections::HashSet;

/// Cheap structural pre-check: is there a sub-SELECT that might match the
/// avg-over-anti-join-complement shape? Lets callers skip cloning the IR.
pub fn has_aggregate_complement_candidate(query: &Query) -> bool {
    // Kill-switch for this new rewrite (set the env var to fall back to the
    // original cross-product execution — used to A/B correctness and as a safety
    // valve).
    if std::env::var_os("FLUREE_DISABLE_AGG_COMPLEMENT_FOLD").is_some() {
        return false;
    }
    patterns_have_candidate(&query.patterns)
}

fn patterns_have_candidate(patterns: &[Pattern]) -> bool {
    patterns.iter().any(|p| match p {
        Pattern::Subquery(sq) => is_candidate_subquery(sq) || patterns_have_candidate(&sq.patterns),
        Pattern::Optional(inner)
        | Pattern::Graph {
            patterns: inner, ..
        } => patterns_have_candidate(inner),
        Pattern::Union(branches) => branches.iter().any(|b| patterns_have_candidate(b)),
        _ => false,
    })
}

/// Loose shape check used by the pre-check (full validation happens in the fold).
fn is_candidate_subquery(sq: &SubqueryPattern) -> bool {
    matches!(&sq.grouping, Some(Grouping::Explicit { .. }))
        && sq.patterns.iter().any(|p| not_exists_inner(p).is_some())
        && sq
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Subquery(inner) if inner.distinct))
}

/// The inner patterns of a NOT EXISTS, whether lowered as the pattern-level
/// `Pattern::NotExists` (which is what `FILTER NOT EXISTS { ... }` becomes — see
/// `fluree-db-sparql` `lower/pattern.rs`) or as a negated `Expression::Exists`
/// inside a `Pattern::Filter` (the compound-expression form).
fn not_exists_inner(p: &Pattern) -> Option<&[Pattern]> {
    match p {
        Pattern::NotExists(inner) => Some(inner),
        Pattern::Filter(Expression::Exists {
            patterns,
            negated: true,
        }) => Some(patterns),
        _ => None,
    }
}

/// Rewrite every matching avg-over-complement sub-SELECT in `query`, recursing
/// into nested scopes. No-op for queries that do not match.
pub fn fold_aggregate_complements(query: &mut Query) {
    let mut next_var = max_var_id(query).saturating_add(1);
    fold_in_patterns(&mut query.patterns, &mut next_var);
}

fn fold_in_patterns(patterns: &mut [Pattern], next_var: &mut u16) {
    for p in patterns.iter_mut() {
        match p {
            Pattern::Subquery(sq) => {
                // Recurse first (inner scopes), then try to rewrite this scope.
                fold_in_patterns(&mut sq.patterns, next_var);
                try_rewrite_without_feature(sq, next_var);
            }
            Pattern::Optional(inner)
            | Pattern::Graph {
                patterns: inner, ..
            } => fold_in_patterns(inner, next_var),
            Pattern::Union(branches) => {
                for b in branches.iter_mut() {
                    fold_in_patterns(b, next_var);
                }
            }
            _ => {}
        }
    }
}

fn alloc_var(next_var: &mut u16) -> VarId {
    let v = VarId(*next_var);
    *next_var = next_var.saturating_add(1);
    v
}

/// If `sq` matches the avg-over-anti-join-complement shape, rewrite its body in
/// place into the distributive-difference form and return `true`.
fn try_rewrite_without_feature(sq: &mut SubqueryPattern, next_var: &mut u16) -> bool {
    // --- match the grouping: explicit single-key, single AVG, no binds/having ---
    let Some(Grouping::Explicit {
        group_by,
        aggregation: Some(agg),
        having: None,
    }) = &sq.grouping
    else {
        return false;
    };
    if group_by.len() != 1 || !agg.binds.is_empty() || agg.aggregates.len() != 1 {
        return false;
    }
    let key = *group_by.iter().next().expect("len checked == 1");
    let spec = agg.aggregates.iter().next().expect("len checked == 1");
    let AggregateFn::Avg(av, _) = spec.function else {
        return false;
    };
    let out = spec.output_var;

    // --- locate the NOT EXISTS (referencing the key) and the DISTINCT universe ---
    let mut ne_inner: Option<Vec<Pattern>> = None;
    let mut has_distinct_universe = false;
    for p in &sq.patterns {
        if let Some(inner) = not_exists_inner(p) {
            if inner
                .iter()
                .flat_map(Pattern::referenced_vars)
                .any(|v| v == key)
            {
                if ne_inner.is_some() {
                    return false; // more than one NOT EXISTS — bail
                }
                ne_inner = Some(inner.to_vec());
            }
        } else if matches!(p, Pattern::Subquery(s) if s.distinct) {
            has_distinct_universe = true;
        }
    }
    let Some(ne_inner) = ne_inner else {
        return false;
    };
    if !has_distinct_universe {
        return false;
    }

    // --- find the aggregate-input coercion bind: `BIND(EXPR(?price) AS ?av)` ---
    let coercion = sq.patterns.iter().find_map(|p| match p {
        Pattern::Bind { var, expr } if *var == av => Some(expr.clone()),
        _ => None,
    });
    let Some(coercion) = coercion else {
        return false; // avg input is not a recognizable pre-aggregation bind
    };

    // --- partition the body ----------------------------------------------------
    // Keep the DISTINCT feature-universe subquery as the driving set; the
    // "universe" patterns are everything except it, the NOT EXISTS filter, and
    // the aggregate-input bind (which we rebuild per synthesized subquery).
    let mut distinct_universe: Option<Pattern> = None;
    let mut universe: Vec<Pattern> = Vec::new();
    for p in &sq.patterns {
        if not_exists_inner(p).is_some() {
            // dropped here — re-added to the WITH set as a positive join below.
        } else if matches!(p, Pattern::Subquery(s) if s.distinct) && distinct_universe.is_none() {
            distinct_universe = Some(p.clone());
        } else if matches!(p, Pattern::Bind { var, .. } if *var == av) {
            // the aggregate-input coercion — rebuilt per synthesized subquery.
        } else {
            universe.push(p.clone());
        }
    }
    let Some(distinct_universe) = distinct_universe else {
        return false;
    };

    // Soundness guard: the decomposition `without = total - with` is valid only
    // when the GROUP BY key is partitioned purely by the DISTINCT universe and
    // the NOT EXISTS — i.e. the key must NOT be bound by the universe triples.
    // If it were, the "scalar total" (computed ignoring the key) would actually
    // be per-key, and `total - with` would be wrong. Bail in that case.
    if universe
        .iter()
        .flat_map(Pattern::produced_vars)
        .any(|v| v == key)
    {
        return false;
    }

    // The WITH set = universe + the NOT EXISTS inner as a POSITIVE join.
    let mut with_patterns = universe.clone();
    with_patterns.extend(ne_inner);

    // --- synthesize fresh variables -------------------------------------------
    let u_sum = alloc_var(next_var);
    let u_cnt = alloc_var(next_var);
    let w_sum = alloc_var(next_var);
    let w_cnt = alloc_var(next_var);
    let wo_sum = alloc_var(next_var);
    let wo_cnt = alloc_var(next_var);
    let av_u = alloc_var(next_var);
    let av_w = alloc_var(next_var);

    // --- scalar universe total: SELECT (SUM(av_u) AS u_sum)(COUNT(av_u) AS u_cnt) ---
    let mut universe_body = universe;
    universe_body.push(Pattern::Bind {
        var: av_u,
        expr: coercion.clone(),
    });
    let universe_sq =
        SubqueryPattern::new(vec![u_sum, u_cnt], universe_body).with_grouping(Grouping::Implicit {
            aggregation: Aggregation {
                aggregates: non_empty(vec![
                    AggregateSpec {
                        function: AggregateFn::Sum(av_u, InputSemantics::List),
                        output_var: u_sum,
                    },
                    AggregateSpec {
                        function: AggregateFn::Count(av_u),
                        output_var: u_cnt,
                    },
                ]),
                binds: Vec::new(),
            },
            having: None,
        });

    // --- per-feature WITH aggregate: SELECT ?key (SUM(av_w))(COUNT(av_w)) GROUP BY ?key ---
    with_patterns.push(Pattern::Bind {
        var: av_w,
        expr: coercion,
    });
    let with_sq = SubqueryPattern::new(vec![key, w_sum, w_cnt], with_patterns).with_grouping(
        Grouping::Explicit {
            group_by: non_empty(vec![key]),
            aggregation: Some(Aggregation {
                aggregates: non_empty(vec![
                    AggregateSpec {
                        function: AggregateFn::Sum(av_w, InputSemantics::List),
                        output_var: w_sum,
                    },
                    AggregateSpec {
                        function: AggregateFn::Count(av_w),
                        output_var: w_cnt,
                    },
                ]),
                binds: Vec::new(),
            }),
            having: None,
        },
    );

    // --- assemble the rewritten body ------------------------------------------
    // distinct features (drive) + scalar total (broadcast) + OPTIONAL with-agg
    // (left join: missing features default to 0) + complement arithmetic.
    let zero = || Expression::Const(FlakeValue::Long(0));
    let coalesce0 = |v: VarId| {
        Expression::call(
            Function::If,
            vec![
                Expression::call(Function::Bound, vec![Expression::Var(v)]),
                Expression::Var(v),
                zero(),
            ],
        )
    };

    let new_patterns = vec![
        distinct_universe,
        Pattern::Subquery(universe_sq),
        Pattern::Optional(vec![Pattern::Subquery(with_sq)]),
        // without_count = universeCount - (withCount or 0)
        Pattern::Bind {
            var: wo_cnt,
            expr: Expression::sub(Expression::Var(u_cnt), coalesce0(w_cnt)),
        },
        // without_sum = universeSum - (withSum or 0)
        Pattern::Bind {
            var: wo_sum,
            expr: Expression::sub(Expression::Var(u_sum), coalesce0(w_sum)),
        },
        // drop features whose complement is empty (matches GROUP BY group-drop)
        Pattern::Filter(Expression::gt(Expression::Var(wo_cnt), zero())),
        // out = without_sum / without_count (AVG of the complement)
        Pattern::Bind {
            var: out,
            expr: Expression::div(Expression::Var(wo_sum), Expression::Var(wo_cnt)),
        },
    ];

    sq.patterns = new_patterns;
    sq.grouping = None;
    // sq.select already projects [?key, ?out] — unchanged.
    true
}

fn non_empty<T>(v: Vec<T>) -> NonEmpty<T> {
    NonEmpty::try_from_vec(v).expect("constructed with at least one element")
}

/// Largest `VarId` referenced anywhere in the query (patterns recursively,
/// grouping, ordering, select), so synthetic vars can be minted above it.
fn max_var_id(query: &Query) -> u16 {
    let mut vars: HashSet<VarId> = HashSet::new();
    for p in &query.patterns {
        vars.extend(p.referenced_vars());
        vars.extend(p.produced_vars());
    }
    if let Some(g) = &query.grouping {
        vars.extend(g.group_by_vars());
        for spec in g.aggregates() {
            vars.insert(spec.output_var);
            if let Some(iv) = spec.function.input_var() {
                vars.insert(iv);
            }
        }
        for (v, e) in g.binds() {
            vars.insert(*v);
            vars.extend(e.referenced_vars());
        }
    }
    for (v, e) in &query.order_binds {
        vars.insert(*v);
        vars.extend(e.referenced_vars());
    }
    vars.into_iter().map(|VarId(n)| n).max().unwrap_or(0)
}
