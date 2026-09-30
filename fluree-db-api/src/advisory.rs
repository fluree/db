//! Query advisories: non-fatal notes about how a query was read.
//!
//! An advisory never changes a result. It explains a query shape whose
//! meaning reliably surprises people, computed once here from the resolved
//! dataset and the lowered query, the same for SPARQL and JSON-LD, so every
//! surface that shows it (a CLI's stderr, an embedder reading
//! [`crate::QueryResult::advisories`]) agrees.

use fluree_db_query::ir::Pattern;

/// A non-fatal note about how a query was read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum QueryAdvisory {
    /// The dataset names only named graphs (SPARQL `FROM NAMED` / JSON-LD
    /// `fromNamed`, with no `FROM` / `from`), so its default graph is empty
    /// (SPARQL 1.1 §13.2), and the query also matches patterns against that
    /// default graph, which therefore match nothing.
    EmptyDefaultGraph,
}

impl QueryAdvisory {
    /// A one-sentence explanation, with the fix.
    pub fn message(&self) -> &'static str {
        match self {
            Self::EmptyDefaultGraph => {
                "The dataset names only named graphs (FROM NAMED / fromNamed), so its default \
                 graph is empty (SPARQL 1.1 13.2) and patterns outside GRAPH { } / [\"graph\", ...] \
                 match nothing. Add a FROM / from to name a default graph."
            }
        }
    }
}

/// Whether `patterns` match against the default graph somewhere: a triple, a
/// path, or a spatial search reached outside every `GRAPH` scope, including
/// inside `OPTIONAL`, `UNION`, `MINUS`, sub-queries and `EXISTS` / `NOT
/// EXISTS` filters. `GRAPH` and `SERVICE` bodies read other graphs, and a
/// graph-source search reads its own index.
pub(crate) fn reads_default_graph(patterns: &[Pattern]) -> bool {
    patterns.iter().any(pattern_reads_default_graph)
}

fn pattern_reads_default_graph(pattern: &Pattern) -> bool {
    let embedded = |expr: &fluree_db_query::ir::Expression| {
        let mut found = false;
        expr.for_each_embedded_patterns(&mut |ps| found |= reads_default_graph(ps));
        found
    };
    match pattern {
        Pattern::Triple(_)
        | Pattern::PropertyPath(_)
        | Pattern::ShortestPath(_)
        | Pattern::GeoSearch(_)
        | Pattern::S2Search(_)
        | Pattern::EdgeAnnotation { .. }
        | Pattern::AnnotationTarget { .. } => true,
        Pattern::DefaultGraphSource { patterns } => !patterns.is_empty(),
        Pattern::Optional(ps)
        | Pattern::Minus(ps)
        | Pattern::Exists(ps)
        | Pattern::NotExists(ps) => reads_default_graph(ps),
        Pattern::Union(branches) => branches.iter().any(|b| reads_default_graph(b)),
        Pattern::Subquery(sub) => reads_default_graph(&sub.patterns),
        Pattern::Filter(expr) | Pattern::Bind { expr, .. } => embedded(expr),
        Pattern::Unwind { list, .. } => embedded(list),
        Pattern::Graph { .. }
        | Pattern::Service(_)
        | Pattern::Values { .. }
        | Pattern::IndexSearch(_)
        | Pattern::VectorSearch(_)
        | Pattern::R2rml(_) => false,
    }
}
