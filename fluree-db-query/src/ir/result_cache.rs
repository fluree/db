//! Which queries a query-result cache must not answer from a stored result.

use super::{Expression, Function, Grouping, Pattern, Query, ServiceEndpoint};
use crate::var_registry::VarId;

const LOCAL_LEDGER_SERVICE: &str = "fluree:ledger:";

/// Why two runs of `query` against the same ledger state can return different
/// answers, or `None` when they cannot.
///
/// This covers what the query text reads. What the view reads (policy, graph
/// sources, reasoning) is the caller's to check.
///
/// Exhaustive over `Pattern` and `Expression`, so a new variant must decide
/// here whether it reads anything beyond the ledger state.
pub fn result_cache_bypass(query: &Query) -> Option<&'static str> {
    patterns(&query.patterns)
        .or_else(|| modifiers(query.grouping.as_ref(), &query.order_binds))
        .or_else(|| {
            query
                .post_values
                .as_ref()
                .and_then(|pv| pv.then.iter().find_map(|(_, e)| expression(e)))
        })
}

fn modifiers(
    grouping: Option<&Grouping>,
    order_binds: &[(VarId, Expression)],
) -> Option<&'static str> {
    order_binds
        .iter()
        .find_map(|(_, e)| expression(e))
        .or_else(|| {
            grouping.and_then(|g| {
                g.having()
                    .and_then(expression)
                    .or_else(|| g.binds().find_map(|(_, e)| expression(e)))
            })
        })
}

fn patterns(patterns: &[Pattern]) -> Option<&'static str> {
    patterns.iter().find_map(pattern)
}

fn pattern(p: &Pattern) -> Option<&'static str> {
    match p {
        Pattern::Filter(expr) | Pattern::Bind { expr, .. } | Pattern::Unwind { list: expr, .. } => {
            expression(expr)
        }
        Pattern::Optional(inner)
        | Pattern::Minus(inner)
        | Pattern::Exists(inner)
        | Pattern::NotExists(inner) => patterns(inner),
        Pattern::Union(branches) => branches.iter().find_map(|b| patterns(b)),
        Pattern::Graph {
            patterns: inner, ..
        }
        | Pattern::DefaultGraphSource { patterns: inner } => patterns(inner),
        Pattern::EdgeAnnotation { body, .. } => patterns(body),
        Pattern::Subquery(sq) => {
            patterns(&sq.patterns).or_else(|| modifiers(sq.grouping.as_ref(), &sq.order_binds))
        }
        Pattern::Service(sp) => match &sp.endpoint {
            ServiceEndpoint::Iri(iri) if iri.starts_with(LOCAL_LEDGER_SERVICE) => {
                patterns(&sp.patterns)
            }
            _ => Some("remote or variable SERVICE"),
        },
        Pattern::ShortestPath(sp) => sp
            .node_filter
            .as_ref()
            .and_then(|f| expression(&f.predicate)),
        Pattern::IndexSearch(_) => Some("full-text index search"),
        Pattern::VectorSearch(_) => Some("vector search"),
        Pattern::R2rml(_) => Some("graph source"),
        Pattern::Triple(_)
        | Pattern::TermComponents(_)
        | Pattern::ListView(_)
        | Pattern::PropertyPath(_)
        | Pattern::Values { .. }
        | Pattern::GeoSearch(_)
        | Pattern::S2Search(_) => None,
    }
}

fn expression(e: &Expression) -> Option<&'static str> {
    match e {
        Expression::Var(_) | Expression::Const(_) | Expression::Resolved(_) => None,
        Expression::Call { func, args } => {
            volatile(func).or_else(|| args.iter().find_map(expression))
        }
        Expression::Map(entries) => entries.iter().find_map(|(_, v)| expression(v)),
        Expression::ListComprehension {
            list, filter, map, ..
        } => expression(list)
            .or_else(|| filter.as_deref().and_then(expression))
            .or_else(|| map.as_deref().and_then(expression)),
        Expression::Reduce {
            init, list, body, ..
        } => expression(init)
            .or_else(|| expression(list))
            .or_else(|| expression(body)),
        Expression::ListPredicate {
            list, predicate, ..
        } => expression(list).or_else(|| expression(predicate)),
        Expression::Member { target, .. } => expression(target),
        Expression::Exists {
            patterns: inner, ..
        } => patterns(inner),
        Expression::PatternComprehension {
            patterns: inner,
            projection,
        } => patterns(inner).or_else(|| expression(projection)),
    }
}

/// Functions that read the clock or a random source on every call. `BNODE`
/// with a label is stable for one plan, but its label hashes the binding's
/// representation, which a different plan over the same state can change.
fn volatile(func: &Function) -> Option<&'static str> {
    match func {
        Function::Rand => Some("RAND()"),
        Function::Now => Some("NOW()"),
        Function::Today => Some("TODAY()"),
        Function::Uuid => Some("UUID()"),
        Function::StrUuid => Some("STRUUID()"),
        Function::Bnode => Some("BNODE()"),
        _ => None,
    }
}
