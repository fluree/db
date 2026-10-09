//! RDF documents to and from [`Dataset`]s, with no ledger involved.
//!
//! [`parse`] reads a document with the standalone readers (the Turtle
//! parser's conformant shape, the strict N-Triples / N-Quads reader, and
//! JSON-LD expansion); [`serialize`] writes a dataset with the RDF writers. Nothing is stored,
//! and no ledger's namespaces or context take part, so a parse followed by a
//! serialize keeps every IRI and literal as the document wrote it, apart
//! from the writer's own spelling of numbers and strings.
//!
//! RDF 1.2 is carried whole: a triple term is a [`Term::TripleTerm`], and an
//! annotation or reified triple is a reification of its graph (see
//! [`fluree_graph_ir::Reification`]); [`Dataset::add_quad`] builds one from
//! its `rdf:reifies` quad.
//!
//! ```
//! use fluree_db_api::rdf::{self, PrefixMap, RdfFormat};
//!
//! let doc = "PREFIX ex: <http://example.org/>\nex:g { ex:a ex:p ex:b {| ex:since 2020 |} }";
//! let dataset = rdf::parse(doc, RdfFormat::TriG, None).unwrap();
//! let text = rdf::serialize(&dataset, RdfFormat::NQuads, &PrefixMap::default()).unwrap();
//! assert_eq!(text.lines().count(), 3);
//! ```

use std::fmt;
use std::str::FromStr;

use fluree_graph_format::{
    format_jsonld_dataset, format_nquads, format_ntriples, format_trig, format_turtle,
    BlankNodePolicy, ContextPolicy, JsonLdFormatConfig, TypeHandling,
};
use fluree_graph_ir::GraphCollectorSink;
use fluree_graph_turtle::{
    parse_nquads, parse_ntriples, parse_with_prefixes_base_options, Dialect, ParserOptions,
    TurtleError,
};
use fluree_vocab::iri::is_absolute_iri;

pub use fluree_graph_format::PrefixMap;
pub use fluree_graph_ir::{Dataset, Datatype, Graph, Term};

/// An RDF document syntax.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RdfFormat {
    /// Turtle: the default graph only.
    Turtle,
    /// TriG: Turtle with named graphs.
    TriG,
    /// N-Triples: one triple per line, the default graph only.
    NTriples,
    /// N-Quads: N-Triples with an optional graph name per line.
    NQuads,
    /// JSON-LD, with named graphs as `{"@id": g, "@graph": [...]}` nodes.
    JsonLd,
}

impl RdfFormat {
    /// Every format, in the order the names are usually listed.
    pub const ALL: [RdfFormat; 5] = [
        RdfFormat::Turtle,
        RdfFormat::TriG,
        RdfFormat::NTriples,
        RdfFormat::NQuads,
        RdfFormat::JsonLd,
    ];

    /// The format's name: `turtle`, `trig`, `ntriples`, `nquads` or `jsonld`.
    pub fn name(self) -> &'static str {
        match self {
            RdfFormat::Turtle => "turtle",
            RdfFormat::TriG => "trig",
            RdfFormat::NTriples => "ntriples",
            RdfFormat::NQuads => "nquads",
            RdfFormat::JsonLd => "jsonld",
        }
    }

    /// The format a file extension (`ttl`, `trig`, `nt`, `nq`, `jsonld`,
    /// `json`; any case, with or without the dot) names.
    pub fn from_extension(extension: &str) -> Option<Self> {
        let extension = extension.strip_prefix('.').unwrap_or(extension);
        match extension.to_ascii_lowercase().as_str() {
            "ttl" => Some(RdfFormat::Turtle),
            "trig" => Some(RdfFormat::TriG),
            "nt" => Some(RdfFormat::NTriples),
            "nq" => Some(RdfFormat::NQuads),
            "jsonld" | "json" => Some(RdfFormat::JsonLd),
            _ => None,
        }
    }

    /// The format's media type.
    pub fn media_type(self) -> &'static str {
        match self {
            RdfFormat::Turtle => "text/turtle",
            RdfFormat::TriG => "application/trig",
            RdfFormat::NTriples => "application/n-triples",
            RdfFormat::NQuads => "application/n-quads",
            RdfFormat::JsonLd => "application/ld+json",
        }
    }

    /// Whether the format can hold named graphs.
    pub fn has_named_graphs(self) -> bool {
        matches!(
            self,
            RdfFormat::TriG | RdfFormat::NQuads | RdfFormat::JsonLd
        )
    }
}

impl fmt::Display for RdfFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for RdfFormat {
    type Err = RdfError;

    /// A format by name, or by a common alias (`ttl`, `nt`, `n-triples`,
    /// `nq`, `n-quads`, `json-ld`, `json`), in any case.
    fn from_str(name: &str) -> Result<Self, RdfError> {
        match name.to_ascii_lowercase().as_str() {
            "turtle" | "ttl" => Ok(RdfFormat::Turtle),
            "trig" => Ok(RdfFormat::TriG),
            "ntriples" | "n-triples" | "nt" => Ok(RdfFormat::NTriples),
            "nquads" | "n-quads" | "nq" => Ok(RdfFormat::NQuads),
            "jsonld" | "json-ld" | "json" => Ok(RdfFormat::JsonLd),
            _ => Err(RdfError::Invalid(format!(
                "unknown RDF format {name:?}; expected turtle, trig, ntriples, nquads or jsonld"
            ))),
        }
    }
}

/// Why a document could not be read or written.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RdfError {
    /// The document is malformed at `line` and `column` (each from 1; the
    /// column counts characters).
    #[error("line {line}, column {column}: {message}")]
    Syntax {
        line: usize,
        column: usize,
        message: String,
    },
    /// The document or the request is invalid in a way with no one position:
    /// an undefined prefix, a relative IRI with no base, an unknown format.
    #[error("{0}")]
    Invalid(String),
    /// The dataset cannot be written in the format asked for.
    #[error("{0}")]
    Unwritable(String),
}

/// The dataset `text` holds, read as `format`. Turtle, TriG and JSON-LD
/// resolve relative IRIs against `base`, and refuse one that stays relative;
/// N-Triples and N-Quads have none.
///
/// Literals keep the document's lexical form; a JSON-LD number or boolean
/// is its JSON value, with the datatype JSON-LD gives it. Blank nodes keep
/// the document's labels, and an anonymous one (`[]`, a collection, an
/// annotation) is labeled `bN` apart from them, so the dataset can be
/// written back in any format.
///
/// A JSON-LD document's named graphs are its `{"@id": g, "@graph": [...]}`
/// nodes, an annotation is `@annotation` on a value, a reified triple a
/// node's `@reifies`, and a triple term `{"@id": {"@id": s, p: o}}`.
pub fn parse(text: &str, format: RdfFormat, base: Option<&str>) -> Result<Dataset, RdfError> {
    let mut sink = GraphCollectorSink::with_named_graphs();
    let parsed = match format {
        RdfFormat::Turtle | RdfFormat::TriG => {
            let dialect = if format == RdfFormat::TriG {
                Dialect::TriG
            } else {
                Dialect::Turtle
            };
            let options = ParserOptions::conformant().with_dialect(dialect);
            parse_with_prefixes_base_options(text, &mut sink, &[], base, options)
        }
        RdfFormat::NTriples => parse_ntriples(text, &mut sink),
        RdfFormat::NQuads => parse_nquads(text, &mut sink),
        RdfFormat::JsonLd => {
            read_jsonld(text, base, &mut sink)?;
            let dataset = sink.into_dataset();
            check_absolute(&dataset)?;
            return Ok(dataset);
        }
    };
    parsed.map_err(|e| located(text, e))?;
    Ok(sink.into_dataset())
}

fn read_jsonld(
    text: &str,
    base: Option<&str>,
    sink: &mut GraphCollectorSink,
) -> Result<(), RdfError> {
    let doc: serde_json::Value = serde_json::from_str(text).map_err(|e| {
        let message = e.to_string();
        let suffix = format!(" at line {} column {}", e.line(), e.column());
        RdfError::Syntax {
            line: e.line(),
            column: e.column(),
            message: message
                .strip_suffix(&suffix)
                .unwrap_or(&message)
                .to_string(),
        }
    })?;
    let invalid = |e: &dyn fmt::Display| RdfError::Invalid(e.to_string());
    let expanded = match base {
        Some(base) => {
            let context =
                fluree_graph_json_ld::parse_context(&serde_json::json!({ "@base": base }))
                    .map_err(|e| invalid(&e))?;
            fluree_graph_json_ld::expand_with_context(&doc, &context)
        }
        None => fluree_graph_json_ld::expand(&doc),
    }
    .map_err(|e| invalid(&e))?;
    fluree_graph_json_ld::adapter::to_graph_events(&expanded, sink).map_err(|e| invalid(&e))
}

/// Refuse an IRI that JSON-LD expansion left relative: no RDF syntax can
/// write it. A property name with no context entry is the usual cause.
fn check_absolute(dataset: &Dataset) -> Result<(), RdfError> {
    fn check(term: &Term) -> Result<(), RdfError> {
        match term {
            Term::Iri(iri) if !is_absolute_iri(iri) => Err(RdfError::Invalid(format!(
                "\"{iri}\" is not an absolute IRI; define it in the @context, or pass a base"
            ))),
            Term::TripleTerm(t) => t.iter().try_for_each(check),
            _ => Ok(()),
        }
    }
    for (name, graph) in dataset.graphs() {
        name.map(check).transpose()?;
        for t in graph.iter() {
            [&t.s, &t.p, &t.o].into_iter().try_for_each(check)?;
        }
        for r in graph.reifications() {
            check(&r.reifier)?;
            [&r.triple.s, &r.triple.p, &r.triple.o]
                .into_iter()
                .try_for_each(check)?;
        }
    }
    Ok(())
}

/// `dataset` written as `format`. Turtle and TriG declare `prefixes` and
/// write IRIs with them, and JSON-LD makes them its `@context`; Turtle and
/// N-Triples hold the default graph only. A reification of an asserted
/// triple is written as an annotation where the format has one.
pub fn serialize(
    dataset: &Dataset,
    format: RdfFormat,
    prefixes: &PrefixMap,
) -> Result<String, RdfError> {
    if !format.has_named_graphs() && !dataset.is_default_only() {
        return Err(RdfError::Unwritable(format!(
            "{format} has no named graphs; write them as trig or nquads"
        )));
    }
    let written = match format {
        RdfFormat::Turtle => format_turtle(&dataset.default, prefixes),
        RdfFormat::TriG => format_trig(dataset, prefixes),
        RdfFormat::NTriples => format_ntriples(&dataset.default),
        RdfFormat::NQuads => format_nquads(dataset),
        RdfFormat::JsonLd => return write_jsonld(dataset, prefixes),
    };
    written.map_err(|e| RdfError::Unwritable(e.to_string()))
}

fn write_jsonld(dataset: &Dataset, prefixes: &PrefixMap) -> Result<String, RdfError> {
    let context = if prefixes.is_empty() {
        ContextPolicy::None
    } else {
        ContextPolicy::UseProvided(prefixes.to_context())
    };
    let compactor = prefixes.clone();
    let config = JsonLdFormatConfig::new()
        .with_context_policy(context)
        .with_type_handling(TypeHandling::AsAtType)
        .with_blank_node_policy(BlankNodePolicy::PreserveLabeled)
        .with_compactor(move |iri| compactor.compact(iri).unwrap_or_else(|| iri.to_string()));
    let document =
        format_jsonld_dataset(dataset, &config).map_err(|e| RdfError::Unwritable(e.to_string()))?;
    serde_json::to_string_pretty(&document).map_err(|e| RdfError::Unwritable(e.to_string()))
}

/// A reader error with its line and column rather than a byte offset.
fn located(text: &str, error: TurtleError) -> RdfError {
    let (TurtleError::Parse { position, message } | TurtleError::Lexer { position, message }) =
        error
    else {
        return RdfError::Invalid(error.to_string());
    };
    let before = &text[..floor_char_boundary(text, position)];
    let line = before.matches('\n').count() + 1;
    let column = before.rsplit('\n').next().map_or(0, |l| l.chars().count()) + 1;
    RdfError::Syntax {
        line,
        column,
        message,
    }
}

fn floor_char_boundary(text: &str, position: usize) -> usize {
    let mut at = position.min(text.len());
    while !text.is_char_boundary(at) {
        at -= 1;
    }
    at
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    const EX: &str = "http://example.org/";

    fn ex(name: &str) -> Term {
        Term::iri(format!("{EX}{name}"))
    }

    #[test]
    fn every_format_reads_its_own_writing() {
        let doc = format!(
            "PREFIX ex: <{EX}>\n\
             ex:a ex:p ex:b {{| ex:since 2020 |}} ; ex:q ( 1 [ ex:r \"x\"@en ] ) .\n\
             ex:g {{ ex:a ex:p <<( ex:c ex:p ex:d )>> }}"
        );
        let mut dataset = parse(&doc, RdfFormat::TriG, None).unwrap();
        dataset.canonicalize();
        let prefixes = PrefixMap::from_map(BTreeMap::from([("ex".into(), EX.into())]));
        for format in [RdfFormat::TriG, RdfFormat::NQuads] {
            let text = serialize(&dataset, format, &prefixes).unwrap();
            let mut again = parse(&text, format, None).unwrap();
            again.canonicalize();
            assert_eq!(
                again.default.triples(),
                dataset.default.triples(),
                "{format}"
            );
            assert_eq!(again.default.reifications(), dataset.default.reifications());
            assert_eq!(again.named.len(), 1, "{format}");
            assert_eq!(
                again.named[&ex("g")].triples(),
                dataset.named[&ex("g")].triples()
            );
        }
    }

    /// The JSON-LD writer's output reads back as the dataset it wrote.
    #[test]
    fn json_ld_reads_its_own_writing() {
        let doc = format!(
            "PREFIX ex: <{EX}>\n\
             PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n\
             PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
             ex:a a ex:T ; ex:knows ex:b {{| ex:since 2020 |}} ; ex:q ( 1 [ ex:r \"x\"@en ] ) ;\n\
                  ex:n \"01\"^^xsd:integer, \"2.50\"^^xsd:decimal, \"y\"^^ex:dt, true .\n\
             ex:z a \"a literal type\" .\n\
             ex:g {{ ex:a ex:age 42 {{| ex:by _:who |}} . _:who ex:name \"W\" .\n\
                    ex:doc ex:says <<( ex:c ex:p <<( ex:d ex:q \"z\" )>> )>> .\n\
                    ex:claim rdf:reifies <<( ex:e ex:p ex:f )>> . }}"
        );
        let mut dataset = parse(&doc, RdfFormat::TriG, None).unwrap();
        dataset.canonicalize();
        let prefixes = PrefixMap::from_map(BTreeMap::from([("ex".into(), EX.into())]));
        for prefixes in [PrefixMap::default(), prefixes] {
            let text = serialize(&dataset, RdfFormat::JsonLd, &prefixes).unwrap();
            let mut again = parse(&text, RdfFormat::JsonLd, None).unwrap();
            again.canonicalize();
            let nquads = |d: &Dataset| serialize(d, RdfFormat::NQuads, &PrefixMap::default());
            assert_eq!(nquads(&again).unwrap(), nquads(&dataset).unwrap(), "{text}");
        }
    }

    #[test]
    fn json_ld_named_graphs_annotations_and_reifiers() {
        let doc = format!(
            r#"{{"@context": {{"ex": "{EX}", "@base": "{EX}"}},
                "@graph": [
                  {{"@id": "ex:g", "ex:label": "G", "@graph": [
                     {{"@id": "ex:a", "ex:age": {{"@value": 3, "@annotation": {{"ex:by": "x"}}}},
                       "ex:knows": {{"@id": "ex:b", "@annotation": {{"@id": "ex:r"}}}}}}]}},
                  {{"@id": "ex:claim", "@reifies": {{"@id": "ex:c", "ex:p": {{"@id": "ex:d"}}}}}},
                  {{"@id": "rel", "ex:n": [1, 1.5, false],
                    "@reverse": {{"ex:child": {{"@id": "ex:parent"}}}},
                    "@included": [{{"@id": "ex:i", "@type": "_:t"}}]}}
                ]}}"#
        );
        let dataset = parse(&doc, RdfFormat::JsonLd, None).unwrap();
        let g = &dataset.named[&ex("g")];
        assert_eq!(
            g.len(),
            3,
            "the age, the knows edge and the annotation's ex:by"
        );
        assert_eq!(g.reifications().len(), 2);
        assert!(g.reifications().iter().any(|r| r.reifier == ex("r")));
        let default = &dataset.default;
        assert!(
            default.iter().any(|t| t.s == ex("g") && t.p == ex("label")),
            "the graph's own node"
        );
        let claim = &default.reifications()[0];
        assert_eq!((&claim.reifier, &claim.triple.o), (&ex("claim"), &ex("d")));
        assert!(
            !default.iter().any(|t| t.s == ex("c")),
            "a reified triple is not asserted"
        );
        let datatypes: Vec<_> = default
            .iter()
            .filter(|t| t.p == ex("n"))
            .map(|t| t.o.as_literal().unwrap().1.as_iri().to_string())
            .collect();
        let xsd = |name: &str| format!("http://www.w3.org/2001/XMLSchema#{name}");
        assert_eq!(datatypes, [xsd("integer"), xsd("double"), xsd("boolean")]);
        assert!(
            default
                .iter()
                .any(|t| t.s == ex("parent") && t.o == ex("rel")),
            "@reverse"
        );
        assert!(
            default
                .iter()
                .any(|t| t.s == ex("i") && t.o == Term::blank("t")),
            "@included"
        );
    }

    #[test]
    fn json_ld_errors_name_where_or_what() {
        let err = parse(
            "{\n  \"@id\": \"http://ex/a\",\n  oops\n}",
            RdfFormat::JsonLd,
            None,
        );
        assert!(
            matches!(
                err,
                Err(RdfError::Syntax {
                    line: 3,
                    column: 3,
                    ..
                })
            ),
            "{err:?}"
        );
        let err = parse(
            r#"{"@id": "http://ex/a", "name": "x"}"#,
            RdfFormat::JsonLd,
            None,
        );
        assert!(
            matches!(&err, Err(RdfError::Invalid(m)) if m.contains("\"name\"")),
            "{err:?}"
        );
        let based = parse(
            r#"{"@id": "a", "http://ex/p": "x"}"#,
            RdfFormat::JsonLd,
            Some(EX),
        );
        assert_eq!(based.unwrap().default.iter().next().unwrap().s, ex("a"));
    }

    #[test]
    fn a_named_graph_needs_a_format_that_holds_one() {
        let mut dataset = Dataset::new();
        dataset.add_quad(ex("a"), ex("p"), ex("b"), Some(&ex("g")));
        for format in [RdfFormat::Turtle, RdfFormat::NTriples] {
            let err = serialize(&dataset, format, &PrefixMap::default()).unwrap_err();
            assert!(matches!(err, RdfError::Unwritable(_)), "{format}: {err}");
        }
    }

    #[test]
    fn an_error_names_its_line_and_column() {
        let doc = format!("<{EX}a> <{EX}p> <{EX}b> .\n<{EX}a> <{EX}p> é <{EX}b> .\n");
        let err = parse(&doc, RdfFormat::NTriples, None).unwrap_err();
        let RdfError::Syntax { line, column, .. } = err else {
            panic!("{err:?}");
        };
        assert_eq!((line, column), (2, 47), "the column counts characters");

        let err = parse(
            "@prefix ex: <http://ex/> .\nex:a ex:p ex:b ex:c .",
            RdfFormat::Turtle,
            None,
        );
        assert!(
            matches!(err, Err(RdfError::Syntax { line: 2, .. })),
            "{err:?}"
        );
        let err = parse("ex:a ex:p ex:b .", RdfFormat::Turtle, None);
        assert!(matches!(err, Err(RdfError::Invalid(_))), "{err:?}");
    }

    #[test]
    fn a_format_goes_by_its_name_alias_or_extension() {
        for format in RdfFormat::ALL {
            assert_eq!(format.name().parse::<RdfFormat>().unwrap(), format);
        }
        assert_eq!("N-Quads".parse::<RdfFormat>().unwrap(), RdfFormat::NQuads);
        assert_eq!(RdfFormat::from_extension(".TTL"), Some(RdfFormat::Turtle));
        assert_eq!(RdfFormat::from_extension("rdf"), None);
        assert!("rdfxml".parse::<RdfFormat>().is_err());
    }
}
