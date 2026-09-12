//! Reuse the WITH aggregate already present in a joined WITH/WITHOUT query.
//!
//! This deliberately recognizes a pair of independent, unsliced subqueries,
//! not arbitrary common subexpressions. It does not change join planning or
//! UNION key inference. The original AVG is retained; only the existing
//! complement fold's SUM/COUNT are added to the same grouped scan.
use super::{non_empty, not_exists_inner};
use crate::ir::{
    AggregateFn, AggregateSpec, Aggregation, Expression, Function, Grouping, InputSemantics,
    Pattern, Query, QueryOutput, Ref, SubqueryPattern, Term, TriplePattern,
};
use crate::var_registry::VarId;

struct Average<'a> {
    key: VarId,
    input: VarId,
    output: VarId,
    cast: &'a Expression,
}

fn average(sq: &SubqueryPattern) -> Option<Average<'_>> {
    if !sq.uncorrelated
        || !sq.pinned_vars.is_empty()
        || sq.distinct
        || sq.limit.is_some()
        || sq.offset.is_some()
        || !sq.ordering.is_empty()
        || !sq.order_binds.is_empty()
        || sq.select.len() != 2
    {
        return None;
    }
    let Some(Grouping::Explicit {
        group_by,
        aggregation: Some(agg),
        having: None,
    }) = &sq.grouping
    else {
        return None;
    };
    if group_by.len() != 1 || agg.aggregates.len() != 1 || !agg.binds.is_empty() {
        return None;
    }
    let key = *group_by.iter().next()?;
    let spec = agg.aggregates.iter().next()?;
    let AggregateFn::Avg(input, InputSemantics::List) = spec.function else {
        return None;
    };
    if key == spec.output_var || !sq.select.contains(&key) || !sq.select.contains(&spec.output_var)
    {
        return None;
    }
    let mut binds = sq.patterns.iter().filter_map(|p| match p {
        Pattern::Bind { var, expr } => Some((*var, expr)),
        _ => None,
    });
    let (var, cast) = binds.next()?;
    if var != input || binds.next().is_some() || float_cast(cast).is_none() {
        return None;
    }
    Some(Average {
        key,
        input,
        output: spec.output_var,
        cast,
    })
}

/// The benchmark's numeric cast, with or without its intervening string cast.
fn float_cast(expr: &Expression) -> Option<(Function, bool, VarId)> {
    let Expression::Call { func, args } = expr else {
        return None;
    };
    if !matches!(func, Function::XsdFloat | Function::XsdDouble) || args.len() != 1 {
        return None;
    }
    match &args[0] {
        Expression::Var(v) => Some((func.clone(), false, *v)),
        Expression::Call {
            func: Function::XsdString,
            args,
        } if args.len() == 1 => match args[0] {
            Expression::Var(v) => Some((func.clone(), true, v)),
            _ => None,
        },
        _ => None,
    }
}

fn plain_triples(patterns: &[Pattern], input: VarId) -> Option<Vec<TriplePattern>> {
    let mut triples = Vec::new();
    for p in patterns {
        match p {
            Pattern::Triple(t) if t.p.is_bound() => triples.push(t.clone()),
            Pattern::Bind { var, .. } if *var == input => {}
            _ => return None,
        }
    }
    Some(triples)
}

fn same_triples(left: &[TriplePattern], right: &[TriplePattern]) -> bool {
    let mut remaining = right.to_vec();
    for t in left {
        let Some(i) = remaining.iter().position(|r| r == t) else {
            return false;
        };
        remaining.remove(i);
    }
    remaining.is_empty()
}

/// Prove that every WITH key occurs in the DISTINCT feature universe: its two
/// required triples are the same type restriction and feature edge, with only
/// the feature-universe's local product variable renamed.
fn covers_with_keys(
    d: &SubqueryPattern,
    edge: &TriplePattern,
    universe: &[TriplePattern],
    key: VarId,
) -> bool {
    if !d.uncorrelated
        || !d.pinned_vars.is_empty()
        || !d.distinct
        || d.select != [key]
        || d.grouping.is_some()
        || d.limit.is_some()
        || d.offset.is_some()
        || !d.ordering.is_empty()
        || !d.order_binds.is_empty()
        || d.patterns.len() != 2
    {
        return false;
    }
    let Ref::Var(product) = edge.s else {
        return false;
    };
    let Some(local_product) = d.patterns.iter().find_map(|p| match p {
        Pattern::Triple(t) if t.p == edge.p && t.o == Term::Var(key) && t.dtc == edge.dtc => {
            t.s.as_var()
        }
        _ => None,
    }) else {
        return false;
    };
    if local_product == key {
        return false;
    }
    let mut mapped = d.patterns.clone();
    for p in &mut mapped {
        p.substitute_var(local_product, product);
    }
    let Some(triples) = plain_triples(&mapped, VarId(u16::MAX)) else {
        return false;
    };
    triples.iter().any(|t| t == edge)
        && triples
            .iter()
            .any(|t| t.s == edge.s && t.p.is_rdf_type() && t.o.is_bound() && universe.contains(t))
}

/// Mutate only after the complete pair and variable budget have been proved.
/// Caller gates this to current-state, single-domain, root-policy execution.
pub(super) fn fold(query: &mut Query) -> bool {
    if std::env::var_os("FLUREE_DISABLE_AGG_COMPLEMENT_SHARING").is_some()
        || query.grouping.is_some()
        || query.post_values.is_some()
        || query.reasoning.has_reasoning()
        || !matches!(query.output, QueryOutput::Select { .. })
    {
        return false;
    }
    let Some(output_vars) = query.output.referenced_vars() else {
        return false;
    };
    // No graph, optional, union, service, lateral producer or VALUES sibling
    // can acquire a new plan as a side effect of this optimization.
    if query
        .patterns
        .iter()
        .any(|p| !matches!(p, Pattern::Subquery(_) | Pattern::Bind { .. }))
    {
        return false;
    }
    let subqueries: Vec<_> = query
        .patterns
        .iter()
        .enumerate()
        .filter_map(|(i, p)| match p {
            Pattern::Subquery(sq) => Some((i, sq)),
            _ => None,
        })
        .collect();
    if subqueries.len() != 2 {
        return false;
    }
    let Some(wo_pos) = subqueries
        .iter()
        .position(|(_, s)| s.patterns.iter().any(|p| not_exists_inner(p).is_some()))
    else {
        return false;
    };
    let (wo_idx, wo) = subqueries[wo_pos];
    let (with_idx, with) = subqueries[1 - wo_pos];
    let (Some(a), Some(b)) = (average(with), average(wo)) else {
        return false;
    };
    if a.key != b.key || a.output == b.output || float_cast(a.cast) != float_cast(b.cast) {
        return false;
    }
    // Only the direct ratio projection is admitted outside the pair. Do not
    // move work across user-defined BINDs, EXISTS expressions or volatile calls.
    for p in &query.patterns {
        if let Pattern::Bind { var, expr } = p {
            if [a.key, a.output, b.output].contains(var) {
                return false;
            }
            let Expression::Call {
                func: Function::Div,
                args,
            } = expr
            else {
                return false;
            };
            if !matches!(args.as_slice(), [Expression::Var(x), Expression::Var(y)]
                if *x == a.output && *y == b.output)
            {
                return false;
            }
        }
    }
    let mut edge = None;
    let mut distinct = None;
    let mut universe = Vec::new();
    for p in &wo.patterns {
        if let Some(inner) = not_exists_inner(p) {
            let [Pattern::Triple(t)] = inner else {
                return false;
            };
            if edge.replace(t.clone()).is_some() {
                return false;
            }
        } else {
            match p {
                Pattern::Subquery(d) if distinct.is_none() => distinct = Some(d),
                Pattern::Triple(t) if t.p.is_bound() => universe.push(t.clone()),
                Pattern::Bind { var, .. } if *var == b.input => {}
                _ => return false,
            }
        }
    }
    let (Some(edge), Some(distinct)) = (edge, distinct) else {
        return false;
    };
    let Ref::Var(product) = edge.s else {
        return false;
    };
    if product == a.key
        || edge.o != Term::Var(a.key)
        || !edge.p.is_bound()
        || edge.dtc.is_some()
        || universe
            .iter()
            .any(|t| t.referenced_vars().contains(&a.key))
        || !covers_with_keys(distinct, &edge, &universe, a.key)
    {
        return false;
    }
    let Some(with_triples) = plain_triples(&with.patterns, a.input) else {
        return false;
    };
    let mut expected = universe.clone();
    expected.push(edge);
    if !same_triples(&with_triples, &expected) {
        return false;
    }
    let Some((_, _, price)) = float_cast(a.cast) else {
        return false;
    };
    // Keep admission to the measured three-triple universe: a typed product
    // and a separate offer with a product link and numeric price. Larger join
    // graphs get no new plan merely because they also contain two averages.
    if universe.len() != 3 || price == product {
        return false;
    }
    let offer_star = universe.iter().any(|link| {
        let Ref::Var(offer) = link.s else {
            return false;
        };
        offer != product
            && offer != price
            && link.o == Term::Var(product)
            && universe
                .iter()
                .any(|value| value.s == link.s && value.p != link.p && value.o == Term::Var(price))
    });
    if !offer_star {
        return false;
    }

    // The admitted subqueries have no hidden aggregate/order expressions; all
    // their variables occur in patterns or SELECT. Include outer projection
    // and ORDER BY variables too, including deliberately unbound ones.
    let max_var = super::max_var_id(query)
        .max(output_vars.iter().map(|v| v.0).max().unwrap_or(0))
        .max(query.ordering.iter().map(|s| s.var.0).max().unwrap_or(0));
    if max_var.checked_add(5).is_none() {
        return false;
    }
    let [w_sum, w_cnt, u_sum, u_cnt, u_input] = [1, 2, 3, 4, 5].map(|n| VarId(max_var + n));
    let mut shared = with.clone();
    let Some(Grouping::Explicit {
        aggregation: Some(agg),
        ..
    }) = &mut shared.grouping
    else {
        unreachable!()
    };
    let mut specs: Vec<_> = agg.aggregates.iter().cloned().collect();
    specs.extend([
        AggregateSpec {
            function: AggregateFn::Sum(a.input, InputSemantics::List),
            output_var: w_sum,
        },
        AggregateSpec {
            function: AggregateFn::Count(a.input),
            output_var: w_cnt,
        },
    ]);
    agg.aggregates = non_empty(specs);
    shared.select.extend([w_sum, w_cnt]);
    let mut body: Vec<_> = universe.into_iter().map(Pattern::Triple).collect();
    body.push(Pattern::Bind {
        var: u_input,
        expr: b.cast.clone(),
    });
    let total = SubqueryPattern::new(vec![u_sum, u_cnt], body)
        .with_uncorrelated()
        .with_grouping(Grouping::Implicit {
            aggregation: Aggregation {
                aggregates: non_empty(vec![
                    AggregateSpec {
                        function: AggregateFn::Sum(u_input, InputSemantics::List),
                        output_var: u_sum,
                    },
                    AggregateSpec {
                        function: AggregateFn::Count(u_input),
                        output_var: u_cnt,
                    },
                ]),
                binds: vec![],
            },
            having: None,
        });
    let without = b.output;
    let mut result = Vec::new();
    for (i, p) in query.patterns.iter().enumerate() {
        if i == with_idx {
            result.push(Pattern::Subquery(shared.clone()));
        } else if i == wo_idx {
            result.extend([
                Pattern::Subquery(total.clone()),
                Pattern::Filter(Expression::gt(
                    Expression::Var(u_cnt),
                    Expression::Var(w_cnt),
                )),
                Pattern::Bind {
                    var: without,
                    expr: Expression::div(
                        Expression::sub(Expression::Var(u_sum), Expression::Var(w_sum)),
                        Expression::sub(Expression::Var(u_cnt), Expression::Var(w_cnt)),
                    ),
                },
            ]);
        } else {
            result.push(p.clone());
        }
    }
    query.patterns = result;
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temporal_mode::PlanningContext;
    use fluree_db_core::FlakeValue;

    fn fixture() -> Query {
        let (key, product, offer, price, av, out, bv, bout, local, ratio) = (
            VarId(0),
            VarId(1),
            VarId(2),
            VarId(3),
            VarId(4),
            VarId(5),
            VarId(6),
            VarId(7),
            VarId(8),
            VarId(9),
        );
        let triple =
            |s, p: &str, o| Pattern::Triple(TriplePattern::new(Ref::Var(s), Ref::Iri(p.into()), o));
        let ty = |s| {
            triple(
                s,
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                Term::Iri("urn:Type".into()),
            )
        };
        let feature = |s| triple(s, "urn:feature", Term::Var(key));
        let universe = vec![
            ty(product),
            triple(offer, "urn:product", Term::Var(product)),
            triple(offer, "urn:price", Term::Var(price)),
        ];
        let cast = Expression::call(
            Function::XsdFloat,
            vec![Expression::call(
                Function::XsdString,
                vec![Expression::Var(price)],
            )],
        );
        let grouped = |patterns, input, output| {
            SubqueryPattern::new(vec![key, output], patterns)
                .with_uncorrelated()
                .with_grouping(Grouping::Explicit {
                    group_by: non_empty(vec![key]),
                    aggregation: Some(Aggregation {
                        aggregates: non_empty(vec![AggregateSpec {
                            function: AggregateFn::Avg(input, InputSemantics::List),
                            output_var: output,
                        }]),
                        binds: vec![],
                    }),
                    having: None,
                })
        };
        let mut present = universe.clone();
        present.push(feature(product));
        present.push(Pattern::Bind {
            var: av,
            expr: cast.clone(),
        });
        let mut absent = vec![Pattern::Subquery(
            SubqueryPattern::new(vec![key], vec![ty(local), feature(local)])
                .with_uncorrelated()
                .with_distinct(),
        )];
        absent.extend(universe);
        absent.push(Pattern::NotExists(vec![feature(product)]));
        absent.push(Pattern::Bind {
            var: bv,
            expr: cast,
        });
        let mut q = Query::new(Default::default());
        q.output = QueryOutput::select_all(vec![key, ratio]);
        q.patterns = vec![
            Pattern::Subquery(grouped(present, av, out)),
            Pattern::Subquery(grouped(absent, bv, bout)),
            Pattern::Bind {
                var: ratio,
                expr: Expression::div(Expression::Var(out), Expression::Var(bout)),
            },
        ];
        q
    }

    fn sq(q: &mut Query, i: usize) -> &mut SubqueryPattern {
        let Pattern::Subquery(s) = &mut q.patterns[i] else {
            panic!("fixture subquery")
        };
        s
    }
    fn agg(s: &mut SubqueryPattern) -> &mut Aggregation {
        let Some(Grouping::Explicit {
            aggregation: Some(a),
            ..
        }) = &mut s.grouping
        else {
            panic!("fixture grouping")
        };
        a
    }

    #[test]
    fn matching_pair_shares_statistics_and_preserves_original_average() {
        let mut q = fixture();
        let original = format!("{:?}", sq(&mut q, 0).patterns);
        assert!(fold(&mut q));
        let shared = sq(&mut q, 0);
        assert_eq!(format!("{:?}", shared.patterns), original);
        let specs: Vec<_> = shared.grouping.as_ref().unwrap().aggregates().collect();
        assert_eq!(specs.len(), 3);
        assert!(matches!(
            specs[0].function,
            AggregateFn::Avg(VarId(4), InputSemantics::List)
        ));
        assert!(matches!(
            specs[1].function,
            AggregateFn::Sum(VarId(4), InputSemantics::List)
        ));
        assert!(matches!(specs[2].function, AggregateFn::Count(VarId(4))));
        let shape = format!("{:?}", q.patterns);
        assert!(!shape.contains("NotExists"));
        assert!(!shape.contains("Optional"));
        assert!(!shape.contains("Union"));
    }

    #[test]
    fn trusted_plan_eliminates_the_repeated_optional() {
        let q = fixture();
        for trusted in [false, true] {
            let context = PlanningContext::current().with_semantic_elision(trusted);
            let op = crate::execute::build_operator_tree(&q, None, &context).unwrap();
            let plan = serde_json::to_string(&op.describe()).unwrap();
            assert_eq!(plan.contains("OptionalOperator"), !trusted, "{plan}");
            if trusted {
                assert_eq!(plan.matches("SubqueryOperator").count(), 2, "{plan}");
            }
        }
    }

    #[test]
    fn neighboring_query_shapes_are_left_unchanged() {
        for case in 0..27 {
            let mut q = fixture();
            match case {
                0 => sq(&mut q, 0).uncorrelated = false,
                1 => sq(&mut q, 1).uncorrelated = false,
                2 => sq(&mut q, 0).pinned_vars.push(VarId(0)),
                3 => sq(&mut q, 0).limit = Some(1),
                4 => sq(&mut q, 1).offset = Some(1),
                5 => sq(&mut q, 0).distinct = true,
                6 => {
                    agg(sq(&mut q, 0)).aggregates = non_empty(vec![AggregateSpec {
                        function: AggregateFn::Avg(VarId(4), InputSemantics::Set),
                        output_var: VarId(5),
                    }]);
                }
                7 => {
                    agg(sq(&mut q, 1)).aggregates = non_empty(vec![AggregateSpec {
                        function: AggregateFn::Avg(VarId(6), InputSemantics::Set),
                        output_var: VarId(7),
                    }]);
                }
                8 => sq(&mut q, 0)
                    .patterns
                    .push(Pattern::Filter(Expression::Const(FlakeValue::Boolean(
                        true,
                    )))),
                9 => sq(&mut q, 1).patterns.push(Pattern::NotExists(vec![])),
                10 => {
                    let Pattern::Subquery(d) = &mut sq(&mut q, 1).patterns[0] else {
                        unreachable!()
                    };
                    d.select.push(VarId(8));
                }
                11 => {
                    let Pattern::Subquery(d) = &mut sq(&mut q, 1).patterns[0] else {
                        unreachable!()
                    };
                    let Pattern::Triple(t) = &mut d.patterns[0] else {
                        unreachable!()
                    };
                    t.o = Term::Iri("urn:OtherType".into());
                }
                12 => {
                    let Pattern::Bind { expr, .. } = sq(&mut q, 0).patterns.last_mut().unwrap()
                    else {
                        unreachable!()
                    };
                    *expr = Expression::call(Function::XsdDouble, vec![Expression::Var(VarId(3))]);
                }
                13 => {
                    let Pattern::NotExists(inner) = &mut sq(&mut q, 1).patterns[4] else {
                        unreachable!()
                    };
                    inner.push(inner[0].clone());
                }
                14 => {
                    let Pattern::NotExists(inner) = &mut sq(&mut q, 1).patterns[4] else {
                        unreachable!()
                    };
                    let Pattern::Triple(t) = &mut inner[0] else {
                        unreachable!()
                    };
                    t.p = Ref::Var(VarId(10));
                }
                15 => q.patterns.push(Pattern::Optional(vec![])),
                16 => q.patterns.push(Pattern::Union(vec![])),
                17 => q.patterns.push(Pattern::Values {
                    vars: vec![VarId(0)],
                    rows: vec![],
                }),
                18 => q.patterns.push(q.patterns[0].clone()),
                19 => q.output = QueryOutput::wildcard(),
                20 => {
                    q.post_values = Some(Pattern::Values {
                        vars: vec![VarId(0)],
                        rows: vec![],
                    });
                }
                21 => q.grouping = sq(&mut q, 0).grouping.clone(),
                22 => sq(&mut q, 1).substitute_var(VarId(1), VarId(12)),
                23 => q.output = QueryOutput::select_all(vec![VarId(u16::MAX)]),
                24 => {
                    let Pattern::Bind { expr, .. } = q.patterns.last_mut().unwrap() else {
                        unreachable!()
                    };
                    *expr = Expression::call(Function::Rand, vec![]);
                }
                25 => {
                    // Even identical extra joins in both halves must not
                    // broaden admission beyond the measured offer star.
                    let extra = Pattern::Triple(TriplePattern::new(
                        Ref::Var(VarId(2)),
                        Ref::Iri("urn:vendor".into()),
                        Term::Var(VarId(10)),
                    ));
                    sq(&mut q, 0).patterns.push(extra.clone());
                    sq(&mut q, 1).patterns.push(extra);
                }
                26 => q.reasoning.modes = crate::ir::ReasoningModes::rdfs(),
                _ => unreachable!(),
            }
            let before = format!("{q:?}");
            assert!(!fold(&mut q), "unexpectedly admitted case {case}");
            assert_eq!(format!("{q:?}"), before, "mutated rejected case {case}");
        }
    }

    #[test]
    fn generated_variables_do_not_capture_unbound_projection() {
        let mut q = fixture();
        q.output = QueryOutput::select_all(vec![VarId(500)]);
        assert!(fold(&mut q));
        let shared = sq(&mut q, 0);
        assert!(shared.select.iter().skip(2).all(|v| v.0 > 500));
    }

    #[test]
    fn sharing_is_not_selected_for_policy_history_or_multi_graph_contexts() {
        let root = PlanningContext::current().with_semantic_elision(true);
        for planning in [
            PlanningContext::current(),
            PlanningContext::history(),
            root.with_multi_default_graph(true),
            root,
        ] {
            let mut q = fixture();
            super::super::fold_aggregate_complements(&mut q, &planning);
            // The old fold retains the single AVG in the first subquery.
            let count = sq(&mut q, 0)
                .grouping
                .as_ref()
                .unwrap()
                .aggregates()
                .count();
            assert_eq!(count, if planning == root { 3 } else { 1 });
        }
    }
}
