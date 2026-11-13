//! A walk over every pattern a query evaluates, for analyses that must see
//! all of them: the graphs a query reads, the facts it reads, the search
//! indexes it consults.

use super::expression::Expression;
use super::grouping::Grouping;
use super::pattern::Pattern;
use super::query::Query;
use crate::var_registry::VarId;

/// Calls `visit` on each pattern `query` evaluates, wherever it sits: nested
/// in another pattern or a subquery, or in an expression (an `EXISTS` in a
/// FILTER, a BIND, a HAVING, a per-group bind, an ORDER BY key or a bind
/// after the trailing VALUES). A pattern is visited before those nested in it.
pub fn for_each_pattern(query: &Query, visit: &mut dyn FnMut(&Pattern)) {
    patterns(&query.patterns, visit);
    solution_modifiers(query.grouping.as_ref(), &query.order_binds, visit);
    if let Some(post_values) = &query.post_values {
        for (_, expr) in &post_values.then {
            expression(expr, visit);
        }
    }
}

fn patterns(list: &[Pattern], visit: &mut dyn FnMut(&Pattern)) {
    for pattern in list {
        visit(pattern);
        match pattern {
            Pattern::Optional(inner)
            | Pattern::Minus(inner)
            | Pattern::Exists(inner)
            | Pattern::NotExists(inner)
            | Pattern::DefaultGraphSource { patterns: inner }
            | Pattern::Graph {
                patterns: inner, ..
            }
            | Pattern::EdgeAnnotation { body: inner, .. } => patterns(inner, visit),
            Pattern::Union(branches) => {
                for branch in branches {
                    patterns(branch, visit);
                }
            }
            Pattern::Subquery(sub) => {
                patterns(&sub.patterns, visit);
                solution_modifiers(sub.grouping.as_ref(), &sub.order_binds, visit);
            }
            Pattern::Filter(expr)
            | Pattern::Bind { expr, .. }
            | Pattern::Unwind { list: expr, .. } => expression(expr, visit),
            Pattern::Triple(_)
            | Pattern::ListView(_)
            | Pattern::Values { .. }
            | Pattern::PropertyPath(_)
            | Pattern::TermComponents(_)
            | Pattern::ShortestPath(_)
            | Pattern::IndexSearch(_)
            | Pattern::VectorSearch(_)
            | Pattern::R2rml(_)
            | Pattern::GeoSearch(_)
            | Pattern::S2Search(_)
            | Pattern::Service(_) => {}
        }
    }
}

fn solution_modifiers(
    grouping: Option<&Grouping>,
    order_binds: &[(VarId, Expression)],
    visit: &mut dyn FnMut(&Pattern),
) {
    if let Some(
        Grouping::Implicit { having, binds, .. } | Grouping::Explicit { having, binds, .. },
    ) = grouping
    {
        for expr in having.iter().chain(binds.iter().map(|(_, expr)| expr)) {
            expression(expr, visit);
        }
    }
    for (_, expr) in order_binds {
        expression(expr, visit);
    }
}

fn expression(expr: &Expression, visit: &mut dyn FnMut(&Pattern)) {
    expr.for_each_embedded_patterns(&mut |inner| patterns(inner, visit));
}
