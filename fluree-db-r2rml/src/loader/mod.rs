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

/// Serialization format of an R2RML mapping document.
///
/// Resolving the format once — from an optional media-type hint plus the
/// mapping source identifier — and using the result at every compile site keeps
/// registration-time and query-time format selection provably identical. This
/// is the single source of truth that fixes the asymmetric default in
/// <https://github.com/fluree/db/issues/1397>, where registration defaulted a
/// missing media type to Turtle but the query path defaulted it to JSON-LD.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MappingFormat {
    /// Turtle (`text/turtle`) — the only format the loader supports today.
    Turtle,
    /// JSON-LD (`application/ld+json`) — recognized but not yet implemented.
    JsonLd,
}

impl MappingFormat {
    /// Resolve the mapping format from an optional media type and the mapping
    /// `source` (a filename, storage address, or content-addressed CID).
    ///
    /// Precedence: an explicit, recognized `media_type` wins; otherwise the
    /// `source` extension decides (`.jsonld`/`.json` → JSON-LD); otherwise the
    /// format defaults to [`MappingFormat::Turtle`]. An unrecognized media type
    /// falls through to the extension/default rules rather than erroring.
    ///
    /// Turtle is the sole supported format, so defaulting an unknown input to
    /// Turtle only ever turns a guaranteed failure into a working mapping — a
    /// genuine JSON-LD document (recognized media type or `.jsonld`/`.json`
    /// extension) still resolves to [`MappingFormat::JsonLd`] and errors clearly
    /// at the call site. A CID or extensionless address therefore resolves to
    /// Turtle, which is what registration always assumed.
    pub fn resolve(media_type: Option<&str>, source: &str) -> Self {
        if let Some(mt) = media_type {
            let mt = mt.to_ascii_lowercase();
            if mt.contains("turtle") {
                return Self::Turtle;
            }
            if mt.contains("json") {
                return Self::JsonLd;
            }
            // Unrecognized media type: fall through to extension inference.
        }

        let source = source.to_ascii_lowercase();
        if source.ends_with(".jsonld") || source.ends_with(".json") {
            Self::JsonLd
        } else {
            Self::Turtle
        }
    }

    /// The canonical IANA media type for this format.
    ///
    /// Persisting this concrete value on a registered graph source keeps stored
    /// configs self-describing, so the query path reuses the resolved format
    /// instead of re-defaulting a `null` media type.
    pub fn media_type(self) -> &'static str {
        match self {
            Self::Turtle => "text/turtle",
            Self::JsonLd => "application/ld+json",
        }
    }
}

#[cfg(test)]
mod format_tests {
    use super::MappingFormat;

    #[test]
    fn resolve_defaults_to_turtle_for_no_media_type_and_no_extension() {
        // Regression guard for #1397: a CAS CID (no extension) and no media type
        // must resolve to Turtle, not JSON-LD.
        assert_eq!(
            MappingFormat::resolve(None, "bagiibqexamplecidwithnoextension"),
            MappingFormat::Turtle
        );
        assert_eq!(MappingFormat::resolve(None, ""), MappingFormat::Turtle);
    }

    #[test]
    fn resolve_infers_format_from_source_extension() {
        assert_eq!(
            MappingFormat::resolve(None, "mapping.ttl"),
            MappingFormat::Turtle
        );
        assert_eq!(
            MappingFormat::resolve(None, "mapping.turtle"),
            MappingFormat::Turtle
        );
        assert_eq!(
            MappingFormat::resolve(None, "mapping.jsonld"),
            MappingFormat::JsonLd
        );
        assert_eq!(
            MappingFormat::resolve(None, "mapping.json"),
            MappingFormat::JsonLd
        );
        // Extension matching is case-insensitive.
        assert_eq!(
            MappingFormat::resolve(None, "MAPPING.JSONLD"),
            MappingFormat::JsonLd
        );
    }

    #[test]
    fn resolve_explicit_media_type_overrides_extension() {
        // Explicit media type beats a conflicting extension in both directions.
        assert_eq!(
            MappingFormat::resolve(Some("application/ld+json"), "mapping.ttl"),
            MappingFormat::JsonLd
        );
        assert_eq!(
            MappingFormat::resolve(Some("text/turtle"), "mapping.jsonld"),
            MappingFormat::Turtle
        );
        // Media type comparison is case-insensitive.
        assert_eq!(
            MappingFormat::resolve(Some("Text/Turtle"), "mapping.jsonld"),
            MappingFormat::Turtle
        );
    }

    #[test]
    fn resolve_unrecognized_media_type_falls_back_to_extension_then_turtle() {
        assert_eq!(
            MappingFormat::resolve(Some("application/octet-stream"), "mapping.jsonld"),
            MappingFormat::JsonLd
        );
        assert_eq!(
            MappingFormat::resolve(Some("application/octet-stream"), "cid-no-ext"),
            MappingFormat::Turtle
        );
    }

    #[test]
    fn media_type_returns_canonical_strings() {
        assert_eq!(MappingFormat::Turtle.media_type(), "text/turtle");
        assert_eq!(MappingFormat::JsonLd.media_type(), "application/ld+json");
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
