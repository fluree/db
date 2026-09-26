//! Text serializations of a CONSTRUCT / DESCRIBE graph: RDF/XML, Turtle and
//! N-Triples.
//!
//! All three instantiate the template into one `Graph`, sort and dedupe it
//! (a CONSTRUCT result is a set of triples, SPARQL 1.1 §16.2), and hand it to
//! the format's writer.

use super::config::{FormatterConfig, OutputFormat};
use super::construct::instantiate_construct_graph;
use super::iri::IriCompactor;
use super::{rdf_xml, FormatError, Result};
use crate::QueryResult;
use fluree_graph_format::{format_ntriples, format_turtle, PrefixMap};

pub fn format(
    result: &QueryResult,
    compactor: &IriCompactor,
    config: &FormatterConfig,
) -> Result<String> {
    let name = match config.format {
        OutputFormat::RdfXml => "RDF/XML",
        OutputFormat::Turtle => "Turtle",
        OutputFormat::NTriples => "N-Triples",
        other => unreachable!("{other:?} is not a graph text format"),
    };
    if result.output.construct_template().is_none() {
        return Err(FormatError::InvalidBinding(format!(
            "{name} is only valid for graph results (SPARQL CONSTRUCT/DESCRIBE)"
        )));
    }

    let mut graph = instantiate_construct_graph(result, compactor)?;
    graph.canonicalize();
    match config.format {
        OutputFormat::RdfXml => rdf_xml::format_graph(&graph),
        // The query's PREFIX declarations (its @context) name the prefixes.
        OutputFormat::Turtle => {
            let prefixes = result
                .orig_context
                .as_ref()
                .map(PrefixMap::from_context)
                .unwrap_or_default();
            format_turtle(&graph, &prefixes).map_err(|e| FormatError::InvalidBinding(e.to_string()))
        }
        _ => format_ntriples(&graph).map_err(|e| FormatError::InvalidBinding(e.to_string())),
    }
}
