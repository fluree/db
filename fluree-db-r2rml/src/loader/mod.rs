//! R2RML loader module
//!
//! This module provides format-agnostic R2RML loading. The core loader operates
//! on the internal Graph IR, with format-specific parsing delegated to optional
//! parser integrations:
//!
//! - **Turtle**: Enable with `features = ["turtle"]`
//! - **JSON-LD**: Future support via separate feature
//!
//! For custom parsers, use `R2rmlLoader::from_graph()` directly with a pre-parsed Graph.

mod extractor;

pub use extractor::MappingExtractor;

use fluree_graph_ir::Graph;
#[cfg(feature = "turtle")]
use fluree_graph_ir::GraphCollectorSink;
#[cfg(feature = "turtle")]
use fluree_graph_turtle::parse as parse_turtle;

use crate::error::{R2rmlError, R2rmlResult};
use crate::mapping::CompiledR2rmlMapping;

/// R2RML mapping loader
///
/// Provides format-agnostic loading of R2RML mappings. The loader first
/// parses the mapping document into a Graph IR, then extracts TriplesMap
/// definitions from the graph.
///
/// Use `from_turtle()` (requires the `turtle` feature) or `from_graph()` to
/// create a loader, then call `compile()` to extract and index the mappings.
pub struct R2rmlLoader {
    /// The parsed graph IR
    graph: Graph,
}

impl R2rmlLoader {
    /// Load R2RML from a Graph IR
    ///
    /// Use this if you already have a parsed graph.
    pub fn from_graph(graph: Graph) -> Self {
        Self { graph }
    }

    /// Load R2RML from Turtle format
    ///
    /// Parses the Turtle document and prepares for extraction.
    ///
    /// Requires the `turtle` feature to be enabled.
    #[cfg(feature = "turtle")]
    pub fn from_turtle(content: &str) -> R2rmlResult<Self> {
        let mut sink = GraphCollectorSink::new();
        parse_turtle(content, &mut sink).map_err(|e| R2rmlError::Parse(e.to_string()))?;
        let graph = sink.finish();
        Ok(Self { graph })
    }

    /// Load R2RML from JSON-LD format
    ///
    /// TODO: Implement when json-ld integration is needed
    pub fn from_jsonld(_content: &str) -> R2rmlResult<Self> {
        Err(R2rmlError::Unsupported(
            "JSON-LD R2RML loading not yet implemented".to_string(),
        ))
    }

    /// Get a reference to the underlying graph
    pub fn graph(&self) -> &Graph {
        &self.graph
    }

    /// Compile the loaded mapping into an optimized structure
    ///
    /// This extracts all TriplesMap definitions from the graph and builds
    /// indexes for efficient lookup during query execution.
    pub fn compile(self) -> R2rmlResult<CompiledR2rmlMapping> {
        let extractor = MappingExtractor::new(&self.graph);
        let triples_maps = extractor.extract_all()?;
        Ok(CompiledR2rmlMapping::new(triples_maps))
    }
}

#[cfg(all(test, feature = "turtle"))]
mod tests {
    use super::*;

    const SIMPLE_MAPPING: &str = r#"
        @prefix rr: <http://www.w3.org/ns/r2rml#> .
        @prefix ex: <http://example.org/> .

        <http://example.org/mapping#AirlineMapping> a rr:TriplesMap ;
            rr:logicalTable [ rr:tableName "airlines" ] ;
            rr:subjectMap [
                rr:template "http://example.org/airline/{id}" ;
                rr:class ex:Airline
            ] ;
            rr:predicateObjectMap [
                rr:predicate ex:name ;
                rr:objectMap [ rr:column "name" ]
            ] .
    "#;

    #[test]
    fn test_from_turtle() {
        let loader = R2rmlLoader::from_turtle(SIMPLE_MAPPING).unwrap();
        assert!(!loader.graph().is_empty());
    }

    #[test]
    fn test_compile() {
        let loader = R2rmlLoader::from_turtle(SIMPLE_MAPPING).unwrap();
        let mapping = loader.compile().unwrap();

        assert_eq!(mapping.len(), 1);
        assert!(mapping
            .get("http://example.org/mapping#AirlineMapping")
            .is_some());
    }
}
