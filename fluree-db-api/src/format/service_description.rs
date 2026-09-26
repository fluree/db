//! SPARQL Service Description: the RDF a SPARQL query endpoint returns for a
//! `GET` with no query (SPARQL 1.1 Service Description §2, and the SPARQL 1.2
//! draft's versioned languages).

use super::config::{FormatterConfig, OutputFormat};
use super::{rdf_xml, FormatError, Result};
use fluree_graph_format::{
    format_jsonld, format_ntriples, format_turtle, JsonLdFormatConfig, PrefixMap,
};
use fluree_graph_ir::{Graph, Term};

const SD: &str = "http://www.w3.org/ns/sparql-service-description#";
const FORMATS: &str = "http://www.w3.org/ns/formats/";
const SPARQL: &str = "http://www.w3.org/ns/sparql#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

/// Result formats the query routes negotiate.
const RESULT_FORMATS: [&str; 8] = [
    "SPARQL_Results_JSON",
    "SPARQL_Results_XML",
    "SPARQL_Results_CSV",
    "SPARQL_Results_TSV",
    "JSON-LD",
    "Turtle",
    "N-Triples",
    "RDF_XML",
];

/// The service description of the SPARQL query endpoint at `endpoint` (an
/// absolute IRI), serialized as `config`'s graph format.
///
/// It claims the SPARQL query language at versions 1.0 through 1.2 (SPARQL
/// 1.2's unversioned `sd:SPARQLQuery` with `sd:supportedVersion`, plus 1.1's
/// `sd:SPARQL11Query` for older clients), the result formats above, and simple
/// entailment, since reasoning is opt-in per query. It claims no `sd:feature`:
/// the default graph is not the union of the named graphs, and Fluree has no
/// empty named graph.
pub fn sparql_service_description(endpoint: &str, config: &FormatterConfig) -> Result<String> {
    let service = Term::blank("service");
    let sd = |local: &str| Term::iri(format!("{SD}{local}"));
    let mut graph = Graph::new();
    graph.add_triple(service.clone(), Term::iri(RDF_TYPE), sd("Service"));
    graph.add_triple(service.clone(), sd("endpoint"), Term::iri(endpoint));
    graph.add_triple(service.clone(), sd("supportedLanguage"), sd("SPARQLQuery"));
    graph.add_triple(
        service.clone(),
        sd("supportedLanguage"),
        sd("SPARQL11Query"),
    );
    for version in [
        "version-1.2",
        "version-1.2-basic",
        "version-1.1",
        "version-1.0",
    ] {
        graph.add_triple(
            service.clone(),
            sd("supportedVersion"),
            Term::iri(format!("{SPARQL}{version}")),
        );
    }
    for format in RESULT_FORMATS {
        graph.add_triple(
            service.clone(),
            sd("resultFormat"),
            Term::iri(format!("{FORMATS}{format}")),
        );
    }
    graph.add_triple(
        service,
        sd("defaultEntailmentRegime"),
        Term::iri("http://www.w3.org/ns/entailment/Simple"),
    );
    graph.canonicalize();

    match config.format {
        OutputFormat::JsonLd => Ok(serde_json::to_string(&format_jsonld(
            &graph,
            &JsonLdFormatConfig::new(),
        ))?),
        OutputFormat::Turtle => {
            let prefixes = PrefixMap::from_context(&serde_json::json!({
                "sd": SD,
                "formats": FORMATS,
                "sparql": SPARQL
            }));
            Ok(format_turtle(&graph, &prefixes))
        }
        OutputFormat::NTriples => Ok(format_ntriples(&graph)),
        OutputFormat::RdfXml => rdf_xml::format_graph(&graph),
        other => Err(FormatError::InvalidBinding(format!(
            "a service description is an RDF graph; {other:?} is not a graph format"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describes_the_endpoint_in_every_graph_format() {
        let endpoint = "http://example.test/v1/fluree/query/db:main";
        let nt = sparql_service_description(endpoint, &FormatterConfig::ntriples()).unwrap();
        for line in [
            format!("<{SD}endpoint> <{endpoint}> ."),
            format!("<{SD}supportedLanguage> <{SD}SPARQLQuery> ."),
            format!("<{SD}supportedLanguage> <{SD}SPARQL11Query> ."),
            format!("<{SD}supportedVersion> <{SPARQL}version-1.2> ."),
        ] {
            assert!(nt.lines().any(|l| l.ends_with(&line)), "{line}\n{nt}");
        }
        for config in [
            FormatterConfig::jsonld(),
            FormatterConfig::turtle(),
            FormatterConfig::rdf_xml(),
        ] {
            let doc = sparql_service_description(endpoint, &config).unwrap();
            assert!(doc.contains(endpoint), "{:?}\n{doc}", config.format);
        }
    }
}
