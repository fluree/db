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

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;

use fluree_graph_format::{
    format_jsonld_dataset_ordered, format_nquads, format_nquads_ordered, format_ntriples,
    format_trig, format_trig_ordered, format_turtle, BlankNodePolicy, ContextPolicy,
    JsonLdFormatConfig, TypeHandling,
};
use fluree_graph_ir::{GraphCollectorSink, OrderedDataset, Reification, StatementPosition, Triple};
use fluree_graph_turtle::{
    parse_nquads, parse_ntriples, parse_with_prefixes_base_options, Dialect, ParserOptions,
    TurtleError,
};
use fluree_vocab::iri::is_absolute_iri;

pub use fluree_graph_format::canon::{Canonical, CanonicalizeOptions, HashAlgorithm};
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
    read(text, format, base, GraphCollectorSink::with_named_graphs()).map(|read| read.dataset)
}

/// A document read with its order: the dataset [`parse`] gives, the
/// statements in the order the document writes them, and the prefixes it
/// declares.
#[derive(Debug, Clone)]
pub struct Document {
    pub dataset: Dataset,
    order: Vec<StatementPosition>,
    /// The prefixes a Turtle or TriG document declares, in order: a prefix
    /// declared again appears again, with its new IRI. Other formats declare
    /// none.
    pub prefixes: Vec<(String, String)>,
}

/// One statement of a [`Document`], in the graph it is in (`None` for the
/// default graph).
#[derive(Debug, Clone, Copy)]
pub enum Statement<'a> {
    Triple(Option<&'a Term>, &'a Triple),
    /// `reifier rdf:reifies <<( s p o )>>`.
    Reification(Option<&'a Term>, &'a Reification),
}

impl Document {
    /// The statements in the order the document writes them. A TriG or
    /// N-Quads document's graphs interleave as written; a JSON-LD node's
    /// statement comes before the statements of a node nested in it, and a
    /// list's cells stand where the list does.
    pub fn statements(&self) -> impl Iterator<Item = Statement<'_>> {
        self.order.iter().map(|position| {
            let name = position.graph.as_ref();
            let graph = match name {
                None => &self.dataset.default,
                Some(name) => &self.dataset.named[name],
            };
            if position.reification {
                Statement::Reification(name, &graph.reifications()[position.index])
            } else {
                Statement::Triple(name, &graph.triples()[position.index])
            }
        })
    }
}

/// [`parse`], keeping the document's order and prefixes.
pub fn parse_document(
    text: &str,
    format: RdfFormat,
    base: Option<&str>,
) -> Result<Document, RdfError> {
    let sink = GraphCollectorSink::with_named_graphs().recording_order();
    let read = read(text, format, base, sink)?;
    Ok(Document {
        dataset: read.dataset,
        order: read.order,
        prefixes: read.prefixes,
    })
}

fn read(
    text: &str,
    format: RdfFormat,
    base: Option<&str>,
    mut sink: GraphCollectorSink,
) -> Result<OrderedDataset, RdfError> {
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
            let mut read = sink.into_ordered_dataset();
            lists_as_cells(&mut read.dataset, &mut read.order);
            check_absolute(&read.dataset)?;
            return Ok(read);
        }
    };
    parsed.map_err(|e| located(text, e))?;
    Ok(sink.into_ordered_dataset())
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

/// Where a list member's cells went, by graph and member: the indices of the
/// statements written just before the member and just after it.
type CellPlacements = HashMap<Option<Term>, HashMap<usize, (Vec<usize>, Vec<usize>)>>;

/// JSON-LD reads a `@list` as positioned members, the form Fluree ingests.
/// As RDF a list is a chain of `rdf:first`/`rdf:rest` cells, which the
/// conformant Turtle reader produces for `( … )`, so each list is rewritten
/// as its cells, labeled `bN` apart from the dataset's own blank nodes.
/// In `order`, a member's link from its owner or the previous cell comes
/// just before it, and its list's `rdf:nil` close just after its last.
fn lists_as_cells(dataset: &mut Dataset, order: &mut Vec<StatementPosition>) {
    use fluree_vocab::rdf;
    let mut used = HashSet::new();
    for (name, graph) in dataset.graphs() {
        name.into_iter().for_each(|n| blank_labels(n, &mut used));
        for t in graph.iter() {
            blank_labels(&t.s, &mut used);
            blank_labels(&t.o, &mut used);
        }
        for r in graph.reifications() {
            blank_labels(&r.reifier, &mut used);
            blank_labels(&r.triple.s, &mut used);
            blank_labels(&r.triple.o, &mut used);
        }
    }
    let mut n = 0;
    let mut cell = || loop {
        n += 1;
        let label = format!("b{n}");
        if used.insert(label.clone()) {
            return Term::blank(label);
        }
    };
    let (first, rest, nil) = (
        Term::iri(rdf::FIRST),
        Term::iri(rdf::REST),
        Term::iri(rdf::NIL),
    );
    // Per graph, per member: the cells' statements just before and after it.
    let mut placements = CellPlacements::new();
    let graphs = std::iter::once((None, &mut dataset.default))
        .chain(dataset.named.iter_mut().map(|(n, g)| (Some(n.clone()), g)));
    for (name, graph) in graphs {
        if graph.iter().all(|t| t.list_index.is_none()) {
            continue;
        }
        // Each owner's last cell and its member; position 0 begins a list.
        let mut open: HashMap<(Term, Term), (Term, usize)> = HashMap::new();
        let mut before = Vec::new();
        let mut after = Vec::new();
        for (member, t) in graph.iter_mut().enumerate() {
            let Some(index) = t.list_index.take() else {
                continue;
            };
            let owner = (t.s.clone(), t.p.clone());
            let this = cell();
            match open.insert(owner.clone(), (this.clone(), member)) {
                Some((prev, _)) if index != 0 => {
                    before.push((member, Triple::new(prev, rest.clone(), this.clone())));
                }
                prev => {
                    if let Some((prev, last)) = prev {
                        after.push((last, Triple::new(prev, rest.clone(), nil.clone())));
                    }
                    before.push((member, Triple::new(owner.0, owner.1, this.clone())));
                }
            }
            t.s = this;
            t.p = first.clone();
        }
        for (last_cell, last) in open.into_values() {
            after.push((last, Triple::new(last_cell, rest.clone(), nil.clone())));
        }
        after.sort_by_key(|(member, _)| *member);
        let placed = placements.entry(name).or_default();
        for (is_before, (member, triple)) in before
            .into_iter()
            .map(|added| (true, added))
            .chain(after.into_iter().map(|added| (false, added)))
        {
            let (just_before, just_after) = placed.entry(member).or_default();
            if is_before {
                just_before.push(graph.len());
            } else {
                just_after.push(graph.len());
            }
            graph.add(triple);
        }
    }
    if order.is_empty() || placements.is_empty() {
        return;
    }
    let at = |graph: &Option<Term>, index| StatementPosition {
        graph: graph.clone(),
        index,
        reification: false,
    };
    let mut placed_order = Vec::with_capacity(order.len());
    for position in order.drain(..) {
        let cells = (!position.reification)
            .then(|| placements.get(&position.graph)?.get(&position.index))
            .flatten();
        match cells {
            Some((just_before, just_after)) => {
                placed_order.extend(just_before.iter().map(|&i| at(&position.graph, i)));
                let graph = position.graph.clone();
                placed_order.push(position);
                placed_order.extend(just_after.iter().map(|&i| at(&graph, i)));
            }
            None => placed_order.push(position),
        }
    }
    *order = placed_order;
}

fn blank_labels(term: &Term, out: &mut HashSet<String>) {
    match term {
        Term::BlankNode(id) => {
            out.insert(id.as_str().to_string());
        }
        Term::TripleTerm(parts) => parts.iter().for_each(|part| blank_labels(part, out)),
        _ => {}
    }
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

/// One term in N-Triples syntax, as [`serialize`] writes it in N-Triples.
pub fn term_ntriples(term: &Term) -> Result<String, RdfError> {
    fluree_graph_format::format_nt_term(term).map_err(|e| RdfError::Unwritable(e.to_string()))
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
        RdfFormat::JsonLd => return write_jsonld(dataset, &[], prefixes),
    };
    written.map_err(|e| RdfError::Unwritable(e.to_string()))
}

/// [`serialize`], keeping the order the dataset was built in: graphs in
/// `graph_order` (each a name, or `None` for the default graph, which TriG
/// then writes at top level where it comes), and in Turtle and TriG each
/// subject's statements together, with `;` and `,`, in the order the
/// subjects first appear.
pub fn serialize_ordered(
    mut dataset: Dataset,
    graph_order: &[Option<Term>],
    format: RdfFormat,
    prefixes: &PrefixMap,
) -> Result<String, RdfError> {
    if !format.has_named_graphs() && !dataset.is_default_only() {
        return Err(RdfError::Unwritable(format!(
            "{format} has no named graphs; write them as trig or nquads"
        )));
    }
    if matches!(format, RdfFormat::Turtle | RdfFormat::TriG) {
        dataset.default.gather_subjects();
        dataset.named.values_mut().for_each(Graph::gather_subjects);
    }
    let written = match format {
        RdfFormat::Turtle => format_turtle(&dataset.default, prefixes),
        RdfFormat::TriG => format_trig_ordered(&dataset, graph_order, prefixes),
        RdfFormat::NTriples => format_ntriples(&dataset.default),
        RdfFormat::NQuads => format_nquads_ordered(&dataset, graph_order),
        RdfFormat::JsonLd => return write_jsonld(&dataset, graph_order, prefixes),
    };
    written.map_err(|e| RdfError::Unwritable(e.to_string()))
}

/// `dataset` as expanded JSON-LD (the `http://www.w3.org/ns/json-ld#expanded`
/// profile): a top-level array of node objects, full IRIs, no context, and
/// every value in an array, a literal as a value object. Graphs come in
/// `graph_order`, as [`serialize_ordered`] writes them.
pub fn serialize_expanded_jsonld(
    dataset: &Dataset,
    graph_order: &[Option<Term>],
) -> Result<String, RdfError> {
    let document = jsonld_document(dataset, graph_order, &PrefixMap::default())?;
    let nodes = match document {
        serde_json::Value::Object(mut map) => map
            .remove("@graph")
            .unwrap_or_else(|| serde_json::Value::Array(Vec::new())),
        other => other,
    };
    serde_json::to_string_pretty(&expanded(nodes)).map_err(|e| RdfError::Unwritable(e.to_string()))
}

/// The writer's full-IRI JSON-LD in expanded form: each property's values
/// in an array, a bare scalar as `{"@value": …}`, and `@type` as an array.
fn expanded(value: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    fn values(value: Value) -> Value {
        let items = match value {
            Value::Array(items) => items,
            one => vec![one],
        };
        Value::Array(items.into_iter().map(item).collect())
    }
    fn item(value: Value) -> Value {
        match value {
            Value::Object(map) => node(map),
            scalar => serde_json::json!({ "@value": scalar }),
        }
    }
    fn node(map: serde_json::Map<String, Value>) -> Value {
        // A value object keeps its `@type` a string; only an annotation on
        // it holds nodes.
        if map.contains_key("@value") {
            let map = map
                .into_iter()
                .map(|(key, value)| match key.as_str() {
                    "@annotation" => (key, values(value)),
                    _ => (key, value),
                })
                .collect();
            return Value::Object(map);
        }
        let map = map
            .into_iter()
            .map(|(key, value)| {
                let value = match key.as_str() {
                    "@id" => match value {
                        Value::Object(term) => node(term),
                        id => id,
                    },
                    "@type" => match value {
                        Value::Array(types) => Value::Array(types),
                        one => Value::Array(vec![one]),
                    },
                    "@list" => values(value),
                    "@graph" | "@annotation" => match value {
                        Value::Array(nodes) => Value::Array(nodes.into_iter().map(item).collect()),
                        one => Value::Array(vec![item(one)]),
                    },
                    _ => values(value),
                };
                (key, value)
            })
            .collect();
        Value::Object(map)
    }
    match value {
        Value::Array(nodes) => Value::Array(nodes.into_iter().map(item).collect()),
        one => Value::Array(vec![item(one)]),
    }
}

fn write_jsonld(
    dataset: &Dataset,
    graph_order: &[Option<Term>],
    prefixes: &PrefixMap,
) -> Result<String, RdfError> {
    let document = jsonld_document(dataset, graph_order, prefixes)?;
    serde_json::to_string_pretty(&document).map_err(|e| RdfError::Unwritable(e.to_string()))
}

fn jsonld_document(
    dataset: &Dataset,
    graph_order: &[Option<Term>],
    prefixes: &PrefixMap,
) -> Result<serde_json::Value, RdfError> {
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
    format_jsonld_dataset_ordered(dataset, graph_order, &config)
        .map_err(|e| RdfError::Unwritable(e.to_string()))
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

/// `dataset` canonicalized by RDFC-1.0 (<https://www.w3.org/TR/rdf-canon/>):
/// its blank nodes labeled `c14n0`, `c14n1`, … from its content alone, and
/// its canonical N-Quads, which every dataset holding the same statements
/// shares whatever its blank node labels and statement order. A dataset
/// whose blank nodes take too much work to tell apart is refused (see
/// [`CanonicalizeOptions::max_deep_calls`]).
///
/// It runs only when called: nothing in reading, writing or transacting
/// canonicalizes.
pub fn canonicalize(
    dataset: &Dataset,
    options: &CanonicalizeOptions,
) -> Result<Canonical, RdfError> {
    fluree_graph_format::canon::canonicalize(dataset, options)
        .map_err(|e| RdfError::Invalid(e.to_string()))
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
            let again = parse(&text, format, None).unwrap();
            // TriG writes the collection as `( … )`, whose nodes read back
            // under fresh labels: the same dataset, up to blank-node labels.
            assert!(isomorphic(&again, &dataset), "{format}:\n{text}");
        }
    }

    /// Every statement of `dataset` with its graph, a reification as
    /// `r rdf:reifies <<( s p o )>>`.
    fn statements(dataset: &Dataset) -> Vec<(Option<Term>, [Term; 3])> {
        let reifies = Term::iri(fluree_vocab::rdf::REIFIES);
        let mut out = Vec::new();
        for (name, graph) in dataset.graphs() {
            for t in graph.iter() {
                let triple = [t.subject(), t.predicate(), t.object()].map(Term::clone);
                out.push((name.cloned(), triple));
            }
            for r in graph.reifications() {
                let t = &r.triple;
                let parts = [t.subject(), t.predicate(), t.object()].map(Term::clone);
                let reified = Term::TripleTerm(std::sync::Arc::new(parts));
                out.push((name.cloned(), [r.reifier.clone(), reifies.clone(), reified]));
            }
        }
        out
    }

    fn rename(term: &Term, to: &std::collections::HashMap<&Term, &Term>) -> Term {
        match term {
            Term::BlankNode(_) => to.get(term).map_or_else(|| term.clone(), |t| (*t).clone()),
            Term::TripleTerm(parts) => Term::TripleTerm(std::sync::Arc::new(
                parts.as_ref().clone().map(|p| rename(&p, to)),
            )),
            _ => term.clone(),
        }
    }

    fn blank_nodes(statements: &[(Option<Term>, [Term; 3])]) -> Vec<Term> {
        fn walk(term: &Term, out: &mut std::collections::BTreeSet<Term>) {
            match term {
                Term::BlankNode(_) => {
                    out.insert(term.clone());
                }
                Term::TripleTerm(parts) => parts.iter().for_each(|p| walk(p, out)),
                _ => {}
            }
        }
        let mut out = std::collections::BTreeSet::new();
        for (name, triple) in statements {
            name.iter()
                .chain(triple.iter())
                .for_each(|t| walk(t, &mut out));
        }
        out.into_iter().collect()
    }

    fn permutations(items: &[Term]) -> Vec<Vec<Term>> {
        if items.is_empty() {
            return vec![Vec::new()];
        }
        let mut out = Vec::new();
        for i in 0..items.len() {
            let mut rest = items.to_vec();
            let first = rest.remove(i);
            for mut tail in permutations(&rest) {
                tail.insert(0, first.clone());
                out.push(tail);
            }
        }
        out
    }

    /// Whether `a` and `b` hold the same statements up to a renaming of their
    /// blank nodes. It tries every renaming, so it suits small datasets only.
    fn isomorphic(a: &Dataset, b: &Dataset) -> bool {
        let (from, mut to) = (statements(a), statements(b));
        to.sort();
        let (blanks_a, blanks_b) = (blank_nodes(&from), blank_nodes(&to));
        if from.len() != to.len() || blanks_a.len() != blanks_b.len() {
            return false;
        }
        permutations(&blanks_b).iter().any(|image| {
            let map = blanks_a.iter().zip(image).collect();
            let mut renamed: Vec<_> = from
                .iter()
                .map(|(name, triple)| {
                    (
                        name.as_ref().map(|n| rename(n, &map)),
                        triple.clone().map(|t| rename(&t, &map)),
                    )
                })
                .collect();
            renamed.sort();
            renamed == to
        })
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
                    ex:claim rdf:reifies <<( ex:e ex:p ex:f )>> . }}\n\
             _:g2 {{ ex:a ex:p 1 . _:g2 ex:label \"g\" }}"
        );
        let mut dataset = parse(&doc, RdfFormat::TriG, None).unwrap();
        dataset.canonicalize();
        let prefixes = PrefixMap::from_map(BTreeMap::from([("ex".into(), EX.into())]));
        for prefixes in [PrefixMap::default(), prefixes] {
            let text = serialize(&dataset, RdfFormat::JsonLd, &prefixes).unwrap();
            let mut again = parse(&text, RdfFormat::JsonLd, None).unwrap();
            again.canonicalize();
            // A list written as `@list` keeps no labels for its cells.
            assert!(isomorphic(&again, &dataset), "{text}");
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

    /// Each statement of `text` as `graph? s p o` with the IRIs' local
    /// names, a blank node as `_`, and a reification as `r reifies (s p o)`.
    fn in_order(text: &str, format: RdfFormat) -> Vec<String> {
        fn short(term: &Term) -> String {
            match term {
                Term::Iri(iri) => iri.rsplit(['/', '#']).next().unwrap_or("").to_string(),
                Term::BlankNode(_) => "_".to_string(),
                Term::Literal { value, .. } => value.lexical(),
                Term::TripleTerm(_) => "<<>>".to_string(),
            }
        }
        let graph =
            |name: Option<&Term>| name.map(|n| format!("{} ", short(n))).unwrap_or_default();
        parse_document(text, format, None)
            .unwrap()
            .statements()
            .map(|statement| match statement {
                Statement::Triple(name, t) => {
                    format!(
                        "{}{} {} {}",
                        graph(name),
                        short(&t.s),
                        short(&t.p),
                        short(&t.o)
                    )
                }
                Statement::Reification(name, r) => format!(
                    "{}{} reifies ({} {} {})",
                    graph(name),
                    short(&r.reifier),
                    short(&r.triple.s),
                    short(&r.triple.p),
                    short(&r.triple.o)
                ),
            })
            .collect()
    }

    /// A document's statements come back in the order it writes them:
    /// graphs interleaved as written, an annotation where it stands.
    #[test]
    fn a_document_keeps_its_order() {
        let trig = format!(
            "PREFIX ex: <{EX}>\n\
             ex:g1 {{ ex:a ex:p ex:b }}\n\
             ex:a ex:p ex:c {{| ex:since 2020 |}} .\n\
             ex:g2 {{ ex:d ex:p ex:e }}\n\
             ex:z ex:p ex:y ."
        );
        assert_eq!(
            in_order(&trig, RdfFormat::TriG),
            [
                "g1 a p b",
                "a p c",
                "_ reifies (a p c)",
                "_ since 2020",
                "g2 d p e",
                "z p y"
            ]
        );
        let nquads = format!("<{EX}a> <{EX}p> <{EX}b> <{EX}g> .\n<{EX}c> <{EX}p> <{EX}d> .\n");
        assert_eq!(in_order(&nquads, RdfFormat::NQuads), ["g a p b", "c p d"]);
    }

    /// A JSON-LD node's statement comes before those of the node nested in
    /// it, and a list's cells stand where the list does, before the nodes it
    /// holds.
    #[test]
    fn json_ld_nests_and_lists_keep_their_order() {
        let doc = format!(
            r#"{{"@context": {{"@vocab": "{EX}"}}, "@id": "{EX}a",
                "p": {{"@id": "{EX}b", "q": "x"}},
                "r": {{"@list": [1, {{"s": "y"}}]}},
                "t": "z"}}"#
        );
        assert_eq!(
            in_order(&doc, RdfFormat::JsonLd),
            [
                "a p b",
                "b q x",
                "a r _",
                "_ first 1",
                "_ rest _",
                "_ first _",
                "_ rest nil",
                "_ s y",
                "a t z"
            ]
        );
    }

    /// The prefixes a document declares, in order, a redeclared one again.
    #[test]
    fn a_document_keeps_its_prefixes() {
        let doc = "PREFIX ex: <http://a.example/>\n\
                   @prefix dc: <http://purl.org/dc/terms/> .\n\
                   ex:s dc:title \"x\" .\n\
                   PREFIX ex: <http://b.example/>\n\
                   ex:s dc:title \"y\" .";
        let document = parse_document(doc, RdfFormat::Turtle, None).unwrap();
        assert_eq!(
            document.prefixes,
            [
                ("ex".to_string(), "http://a.example/".to_string()),
                ("dc".to_string(), "http://purl.org/dc/terms/".to_string()),
                ("ex".to_string(), "http://b.example/".to_string()),
            ]
        );
    }

    /// An ordered TriG writes graphs in the order given, the default graph
    /// where it comes, and a subject's statements together with `;`.
    #[test]
    fn serialize_ordered_keeps_graph_and_subject_order() {
        let mut dataset = Dataset::new();
        let (g1, g2) = (ex("g1"), ex("g2"));
        dataset.add_quad(ex("a"), ex("p"), ex("b"), Some(&g2));
        dataset.add_quad(ex("z"), ex("p"), ex("y"), None);
        dataset.add_quad(ex("c"), ex("p"), ex("d"), Some(&g1));
        dataset.add_quad(ex("a"), ex("q"), ex("e"), Some(&g2));
        dataset.add_quad(ex("f"), ex("p"), ex("g"), Some(&g2));
        dataset.add_quad(ex("a"), ex("p"), ex("h"), Some(&g2));
        let prefixes = PrefixMap::from_map(BTreeMap::from([("ex".into(), EX.into())]));
        let order = [Some(g2), None, Some(g1)];
        let trig = serialize_ordered(dataset, &order, RdfFormat::TriG, &prefixes).unwrap();
        let g2_at = trig.find("GRAPH ex:g2").unwrap();
        let default_at = trig.find("ex:z ex:p ex:y").unwrap();
        let g1_at = trig.find("GRAPH ex:g1").unwrap();
        assert!(g2_at < default_at && default_at < g1_at, "{trig}");
        assert!(
            trig.contains("ex:a ex:p ex:b, ex:h ;\n        ex:q ex:e ."),
            "{trig}"
        );
    }

    /// Expanded JSON-LD: an array of nodes, every value in an array, each
    /// literal a value object, and no context.
    #[test]
    fn expanded_json_ld_has_every_value_in_an_array() {
        let doc = format!(
            "PREFIX ex: <{EX}>\nex:a a ex:T ; ex:n 1 ; ex:s \"x\"@ar--rtl ; ex:r ex:b ; ex:l ( 1 ) ."
        );
        let dataset = parse(&doc, RdfFormat::Turtle, None).unwrap();
        let text = serialize_expanded_jsonld(&dataset, &[]).unwrap();
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        let node = &value[0];
        assert_eq!(
            node["@type"],
            serde_json::json!([format!("{EX}T")]),
            "{text}"
        );
        let one = serde_json::json!({"@value": "1", "@type": fluree_vocab::xsd::INTEGER});
        assert_eq!(node[format!("{EX}n")], serde_json::json!([one]));
        assert_eq!(
            node[format!("{EX}s")],
            serde_json::json!([{"@value": "x", "@language": "ar", "@direction": "rtl"}])
        );
        assert_eq!(
            node[format!("{EX}r")],
            serde_json::json!([{"@id": format!("{EX}b")}])
        );
        assert_eq!(
            node[format!("{EX}l")],
            serde_json::json!([{"@list": [one]}])
        );
        assert!(!text.contains("@context"));
        let again = parse(&text, RdfFormat::JsonLd, None).unwrap();
        assert_eq!(again.len(), dataset.len());
    }
}
