//! The graphs a query reads beyond the one it runs against.

use super::expression::Expression;
use super::grouping::Grouping;
use super::pattern::{GraphName, Pattern};
use super::query::Query;
use crate::var_registry::VarId;
use std::collections::BTreeSet;
use std::sync::Arc;

/// The graphs a query reads beyond the graph it runs against, as its parsed
/// patterns name them, so every form the parser accepts (`["graph", …]`, a
/// node-level `@graph` or a context alias of it) is read the same way.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GraphReads {
    /// Each graph a `GRAPH <iri>` names, by the name it parsed to.
    pub named: BTreeSet<Arc<str>>,
    /// Whether the query can read a graph no name lists: a `GRAPH ?g`, a
    /// `SERVICE`, the union default graph, or reasoning, whose schema can come
    /// from any graph.
    pub any: bool,
}

impl GraphReads {
    /// What `query` reads, its subqueries and `EXISTS` patterns included.
    pub fn of(query: &Query) -> Self {
        let mut reads = Self {
            any: query.union_default_graph == Some(true) || query.reasoning.has_reasoning(),
            ..Self::default()
        };
        reads.patterns(&query.patterns);
        reads.solution_modifiers(query.grouping.as_ref(), &query.order_binds);
        if let Some(post_values) = &query.post_values {
            for (_, expr) in &post_values.then {
                reads.expression(expr);
            }
        }
        reads
    }

    fn patterns(&mut self, patterns: &[Pattern]) {
        for pattern in patterns {
            match pattern {
                Pattern::Graph { name, patterns } => {
                    match name {
                        GraphName::Iri(iri) => {
                            self.named.insert(Arc::clone(iri));
                        }
                        GraphName::Var(_) => self.any = true,
                    }
                    self.patterns(patterns);
                }
                Pattern::Service(_) => self.any = true,
                Pattern::Optional(inner)
                | Pattern::Minus(inner)
                | Pattern::Exists(inner)
                | Pattern::NotExists(inner)
                | Pattern::DefaultGraphSource { patterns: inner }
                | Pattern::EdgeAnnotation { body: inner, .. } => self.patterns(inner),
                Pattern::Union(branches) => {
                    for branch in branches {
                        self.patterns(branch);
                    }
                }
                Pattern::Subquery(sub) => {
                    self.patterns(&sub.patterns);
                    self.solution_modifiers(sub.grouping.as_ref(), &sub.order_binds);
                }
                Pattern::Filter(expr)
                | Pattern::Bind { expr, .. }
                | Pattern::Unwind { list: expr, .. } => self.expression(expr),
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
                | Pattern::S2Search(_) => {}
            }
        }
    }

    fn solution_modifiers(
        &mut self,
        grouping: Option<&Grouping>,
        order_binds: &[(VarId, Expression)],
    ) {
        if let Some(
            Grouping::Implicit { having, binds, .. } | Grouping::Explicit { having, binds, .. },
        ) = grouping
        {
            for expr in having.iter().chain(binds.iter().map(|(_, expr)| expr)) {
                self.expression(expr);
            }
        }
        for (_, expr) in order_binds {
            self.expression(expr);
        }
    }

    fn expression(&mut self, expr: &Expression) {
        expr.for_each_embedded_patterns(&mut |patterns| self.patterns(patterns));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::parse_query;
    use crate::var_registry::VarRegistry;
    use fluree_db_core::LedgerSnapshot;
    use serde_json::json;

    fn reads(query: serde_json::Value) -> GraphReads {
        let snapshot = LedgerSnapshot::genesis("graph-reads:main");
        let mut vars = VarRegistry::new();
        let parsed = parse_query(&query, &snapshot, &mut vars, None).expect("query parses");
        GraphReads::of(&parsed)
    }

    fn named(iris: &[&str]) -> GraphReads {
        GraphReads {
            named: iris.iter().map(|iri| Arc::from(*iri)).collect(),
            any: false,
        }
    }

    const G: &str = "http://example.org/g";

    #[test]
    fn a_query_without_graph_patterns_reads_no_other_graph() {
        assert_eq!(
            reads(json!({
                "@context": {"ex": "http://example.org/"},
                "select": ["?x"],
                "where": {"@id": "?x", "ex:title": "?t"}
            })),
            GraphReads::default()
        );
    }

    #[test]
    fn every_way_of_naming_a_graph_reads_it() {
        let context = json!({"ex": "http://example.org/", "in": "@graph"});
        for pattern in [
            json!(["graph", "ex:g", {"@id": "?x", "ex:title": "?t"}]),
            json!(["graph", G, {"@id": "?x", "ex:title": "?t"}]),
            json!({"@id": "?x", "@graph": "ex:g", "ex:title": "?t"}),
            json!({"@id": "?x", "in": "ex:g", "ex:title": "?t"}),
        ] {
            assert_eq!(
                reads(json!({"@context": context, "select": ["?x"], "where": [pattern]})),
                named(&[G]),
                "{pattern}"
            );
        }
    }

    #[test]
    fn a_graph_name_is_an_iri_not_a_vocabulary_term() {
        assert_eq!(
            reads(json!({
                "@context": {"@vocab": "http://vocab.example/", "@base": "http://example.org/"},
                "select": ["?x"],
                "where": {"@id": "?x", "@graph": "g", "title": "?t"}
            })),
            named(&[G])
        );
    }

    #[test]
    fn nested_patterns_read_their_graphs() {
        let in_g = json!(["graph", "ex:g", {"@id": "?x", "ex:tag": "?tag"}]);
        for pattern in [
            json!(["optional", in_g]),
            json!(["union", [in_g], [{"@id": "?x", "ex:tag": "?tag"}]]),
            json!(["minus", in_g]),
            json!(["filter", ["exists", in_g]]),
            json!(["query", {
                "@context": {"ex": "http://example.org/"},
                "select": ["?x"],
                "where": [in_g]
            }]),
        ] {
            assert_eq!(
                reads(json!({
                    "@context": {"ex": "http://example.org/"},
                    "select": ["?x"],
                    "where": [{"@id": "?x", "ex:title": "?t"}, pattern]
                })),
                named(&[G]),
                "{pattern}"
            );
        }
    }

    #[test]
    fn a_graph_variable_reads_any_graph() {
        assert!(
            reads(json!({
                "@context": {"ex": "http://example.org/"},
                "select": ["?x"],
                "where": [["graph", "?g", {"@id": "?x", "ex:title": "?t"}]]
            }))
            .any
        );
    }
}
