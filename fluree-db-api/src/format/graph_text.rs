//! Text serializations of a CONSTRUCT / DESCRIBE result: RDF/XML, Turtle,
//! N-Triples, and, for templates that write into named graphs, TriG and
//! N-Quads.
//!
//! All of them instantiate the template into one `Dataset`, sort and dedupe
//! each graph (a CONSTRUCT result is a set of triples, SPARQL 1.1 §16.2), and
//! hand it to the format's writer.

use super::config::{FormatterConfig, OutputFormat};
use super::construct::instantiate_construct_graph;
use super::iri::IriCompactor;
use super::{rdf_xml, FormatError, Result};
use crate::QueryResult;
use fluree_graph_format::{format_nquads, format_ntriples, format_trig, format_turtle, PrefixMap};

pub fn format(
    result: &QueryResult,
    compactor: &IriCompactor,
    config: &FormatterConfig,
) -> Result<String> {
    let name = match config.format {
        OutputFormat::RdfXml => "RDF/XML",
        OutputFormat::Turtle => "Turtle",
        OutputFormat::NTriples => "N-Triples",
        OutputFormat::TriG => "TriG",
        OutputFormat::NQuads => "N-Quads",
        other => unreachable!("{other:?} is not a graph text format"),
    };
    if result.output.construct_template().is_none() {
        return Err(FormatError::InvalidBinding(format!(
            "{name} is only valid for graph results (SPARQL CONSTRUCT/DESCRIBE)"
        )));
    }

    let mut dataset = instantiate_construct_graph(result, compactor)?;
    dataset.canonicalize();
    // The query's PREFIX declarations (its @context) name the prefixes.
    let prefixes = || {
        result
            .orig_context
            .as_ref()
            .map(PrefixMap::from_context)
            .unwrap_or_default()
    };
    match config.format {
        OutputFormat::TriG => return format_trig(&dataset, &prefixes()).map_err(invalid),
        OutputFormat::NQuads => return format_nquads(&dataset).map_err(invalid),
        _ => {}
    }
    if !dataset.is_default_only() {
        return Err(FormatError::InvalidBinding(format!(
            "the CONSTRUCT template writes into named graphs, which {name} cannot express; \
             use TriG, N-Quads or JSON-LD"
        )));
    }
    let graph = &dataset.default;
    match config.format {
        OutputFormat::RdfXml => rdf_xml::format_graph(graph),
        OutputFormat::Turtle => format_turtle(graph, &prefixes()).map_err(invalid),
        _ => format_ntriples(graph).map_err(invalid),
    }
}

fn invalid(e: fluree_graph_format::InvalidLangTag) -> FormatError {
    FormatError::InvalidBinding(e.to_string())
}
