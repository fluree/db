//! The graphs a query reads beyond the one it runs against.

use super::pattern::{GraphName, Pattern};
use super::query::Query;
use super::walk::for_each_pattern;
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
        for_each_pattern(query, &mut |pattern| match pattern {
            Pattern::Graph { name, .. } => match name {
                GraphName::Iri(iri) => {
                    reads.named.insert(Arc::clone(iri));
                }
                GraphName::Var(_) => reads.any = true,
            },
            Pattern::Service(_) => reads.any = true,
            _ => {}
        });
        reads
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
