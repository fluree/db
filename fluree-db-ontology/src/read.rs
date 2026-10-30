//! Reading an ontology file: RDF text to quads, then ontology and catalog.
//!
//! The policy counts triples, so the reader keeps the RDF a file denotes:
//! collections arrive as their `rdf:first`/`rdf:rest` spine (an OWL
//! `owl:unionOf` list is several blank-node triples, and each one counts when
//! it is dropped), and named graphs keep their names.
//!
//! A file is split the way an extraction request is. A dataset that uses the
//! request's graph roles (`xt:ontology`, `xt:instances`, ...) is split by
//! graph, so a saved request can be checked as it is. Anything else is read
//! as one flat graph: declared classes and properties, SHACL shapes and the
//! blank nodes they point at are the ontology; document nodes and request
//! controls are set aside; what remains is the catalog.

use crate::policy::shape_nodes;
use crate::vocab::*;
use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, SinkError, SinkResult, Term, TermId};
use fluree_graph_json_ld::adapter::{to_graph_events_scoped, GraphNameRef};
use fluree_graph_turtle::ParserOptions;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

/// One statement, in the graph it was written to (`None`: the default graph).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Quad {
    pub subject: Term,
    pub predicate: Arc<str>,
    pub object: Term,
    pub graph: Option<Arc<str>>,
}

impl Quad {
    pub fn new(subject: Term, predicate: &str, object: Term) -> Self {
        Quad {
            subject,
            predicate: Arc::from(predicate),
            object,
            graph: None,
        }
    }

    pub fn is(&self, predicate: &str) -> bool {
        &*self.predicate == predicate
    }
}

/// The RDF syntaxes an ontology file may be written in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RdfFormat {
    Turtle,
    JsonLd,
}

impl RdfFormat {
    /// The format a file's extension names, if it is one this crate reads.
    pub fn from_path(path: &Path) -> Option<Self> {
        let ext = path.extension()?.to_str()?.to_ascii_lowercase();
        match ext.as_str() {
            "ttl" | "turtle" | "n3" | "nt" => Some(Self::Turtle),
            "jsonld" | "json" => Some(Self::JsonLd),
            _ => None,
        }
    }

    /// The format some text looks like: JSON-LD when it opens with `{` or
    /// `[`, Turtle otherwise.
    pub fn sniff(text: &str) -> Self {
        match text.trim_start().chars().next() {
            Some('{' | '[') => Self::JsonLd,
            _ => Self::Turtle,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("invalid Turtle: {0}")]
    Turtle(String),
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid JSON-LD: {0}")]
    JsonLd(String),
}

/// A parsed file: its quads, and the prefixes it declared (in order).
#[derive(Clone, Debug, Default)]
pub struct Parsed {
    pub quads: Vec<Quad>,
    pub prefixes: Vec<(String, String)>,
}

/// A file split into the parts of a request.
#[derive(Clone, Debug, Default)]
pub struct Split {
    pub ontology: Vec<Quad>,
    pub catalog: Vec<Quad>,
    /// Set when the file named the request's graph roles.
    pub by_graph: bool,
    /// Statements about a document or request controls, which `check` sets
    /// aside.
    pub other: usize,
}

/// Parse RDF text. `base` resolves relative IRIs.
pub fn parse(text: &str, format: RdfFormat, base: Option<&str>) -> Result<Parsed, ReadError> {
    let mut sink = QuadSink::default();
    match format {
        RdfFormat::Turtle => {
            fluree_graph_turtle::parse_with_prefixes_base_options(
                text,
                &mut sink,
                &[],
                base,
                ParserOptions::conformant(),
            )
            .map_err(|e| ReadError::Turtle(e.to_string()))?;
        }
        RdfFormat::JsonLd => {
            let json: serde_json::Value = serde_json::from_str(text)?;
            collect_context_prefixes(&json, &mut sink.prefixes);
            let expanded = fluree_graph_json_ld::expand(&json)
                .map_err(|e| ReadError::JsonLd(e.to_string()))?;
            to_graph_events_scoped(&expanded, &mut sink, |name| match name {
                GraphNameRef::NamedGraph(iri) => Ok(Some(iri.to_string())),
                GraphNameRef::Selector(sel) if sel == "default" || sel == "@default" => Ok(None),
                GraphNameRef::Selector(sel) => Ok(Some(sel.to_string())),
            })
            .map_err(|e| ReadError::JsonLd(e.to_string()))?;
        }
    }
    Ok(sink.into_parsed())
}

impl Parsed {
    /// The ontology and catalog, as a request reads them.
    pub fn split(&self) -> Split {
        let roles: HashSet<&str> = [XT_ONTOLOGY, XT_INSTANCES, XT_DOCUMENT, XT_REQUEST].into();
        let by_graph = self
            .quads
            .iter()
            .any(|q| q.graph.as_deref().is_some_and(|g| roles.contains(g)));
        if by_graph {
            let in_graph = |name: &str| -> Vec<Quad> {
                self.quads
                    .iter()
                    .filter(|q| q.graph.as_deref() == Some(name))
                    .cloned()
                    .collect()
            };
            let ontology = in_graph(XT_ONTOLOGY);
            let catalog = in_graph(XT_INSTANCES);
            let other = self.quads.len() - ontology.len() - catalog.len();
            return Split {
                ontology,
                catalog,
                by_graph,
                other,
            };
        }
        split_flat(&self.quads)
    }
}

/// OWL axioms written as nodes of their own (`[] a owl:AllDisjointClasses ;
/// owl:members (...)`): a reader reads them as catalog records.
pub const OWL_AXIOM_TYPES: &[&str] = &[
    "http://www.w3.org/2002/07/owl#AllDisjointClasses",
    "http://www.w3.org/2002/07/owl#AllDifferent",
    "http://www.w3.org/2002/07/owl#AllDisjointProperties",
    "http://www.w3.org/2002/07/owl#NegativePropertyAssertion",
    "http://www.w3.org/2002/07/owl#Axiom",
];

/// Declarations that are neither a class nor a property a reader shows:
/// read as catalog records too.
pub const OWL_DECLARATION_TYPES: &[&str] = &[
    "http://www.w3.org/2002/07/owl#AnnotationProperty",
    "http://www.w3.org/2002/07/owl#OntologyProperty",
    "http://www.w3.org/2000/01/rdf-schema#Datatype",
];

/// The OWL machinery in a catalog: axiom nodes (with the blank nodes their
/// lists use) and declarations of annotation properties and datatypes, as
/// ontology editors such as Protégé write them. A reader splitting a flat
/// graph files them as catalog records and refuses them (records need IRIs
/// and SKOS labels). Returns (every subject involved, the axioms and
/// declarations themselves).
pub fn owl_machinery(catalog: &[Quad]) -> (HashSet<Term>, Vec<(Term, String)>) {
    let mut roots: Vec<(Term, String)> = Vec::new();
    for q in catalog {
        if !q.is(RDF_TYPE) {
            continue;
        }
        if let Some(t) = q.object.as_iri() {
            let axiom = OWL_AXIOM_TYPES.contains(&t) && q.subject.is_blank();
            if axiom || OWL_DECLARATION_TYPES.contains(&t) {
                roots.push((q.subject.clone(), t.to_string()));
            }
        }
    }
    let mut involved: HashSet<Term> = roots.iter().map(|(s, _)| s.clone()).collect();
    let mut stack: Vec<Term> = roots
        .iter()
        .filter(|(s, _)| s.is_blank())
        .map(|(s, _)| s.clone())
        .collect();
    while let Some(node) = stack.pop() {
        for q in catalog {
            if q.subject == node && q.object.is_blank() && involved.insert(q.object.clone()) {
                stack.push(q.object.clone());
            }
        }
    }
    (involved, roots)
}

/// A flat graph split as the request reader splits Turtle.
fn split_flat(quads: &[Quad]) -> Split {
    let mut schema: HashSet<&Term> = quads
        .iter()
        .filter(|q| q.is(RDF_TYPE) && q.object.as_iri().is_some_and(|o| SCHEMA_TYPES.contains(&o)))
        .map(|q| &q.subject)
        .collect();
    schema.extend(shape_nodes(quads));
    // and the blank nodes a term points at (OWL restrictions, unionOf lists)
    let mut stack: Vec<&Term> = quads
        .iter()
        .filter(|q| schema.contains(&q.subject) && q.object.is_blank())
        .map(|q| &q.object)
        .collect();
    while let Some(node) = stack.pop() {
        if schema.insert(node) {
            stack.extend(
                quads
                    .iter()
                    .filter(|q| &q.subject == node && q.object.is_blank())
                    .map(|q| &q.object),
            );
        }
    }
    // a document is a node with nif:isString that is not an element of one
    let elements: HashSet<&Term> = quads
        .iter()
        .filter(|q| q.is(DOC_SOURCE_DOCUMENT) && q.object != q.subject)
        .map(|q| &q.subject)
        .collect();
    let documents: HashSet<&Term> = quads
        .iter()
        .filter(|q| q.is(NIF_IS_STRING) && !elements.contains(&q.subject))
        .map(|q| &q.subject)
        .collect();
    let mut set_aside: HashSet<&Term> = documents.clone();
    set_aside.extend(
        quads
            .iter()
            .filter(|q| q.is(DOC_SOURCE_DOCUMENT) && documents.contains(&q.object))
            .map(|q| &q.subject),
    );
    let requests: Vec<&Term> = quads
        .iter()
        .filter(|q| q.is(RDF_TYPE) && q.object.as_iri() == Some(XT_REQUEST_CLASS))
        .map(|q| &q.subject)
        .collect();
    set_aside.extend(requests.iter().copied());
    set_aside.extend(
        quads
            .iter()
            .filter(|q| q.is(XT_WINDOW) && requests.contains(&&q.subject))
            .map(|q| &q.object),
    );
    let mut split = Split::default();
    for q in quads {
        if schema.contains(&q.subject) {
            split.ontology.push(q.clone());
        } else if set_aside.contains(&q.subject) {
            split.other += 1;
        } else {
            split.catalog.push(q.clone());
        }
    }
    split
}

fn collect_context_prefixes(json: &serde_json::Value, out: &mut Vec<(String, String)>) {
    match json {
        serde_json::Value::Array(items) => {
            for item in items {
                collect_context_prefixes(item, out);
            }
        }
        serde_json::Value::Object(map) => {
            if let Some(ctx) = map.get("@context") {
                collect_context_entries(ctx, out);
            }
        }
        _ => {}
    }
}

fn collect_context_entries(ctx: &serde_json::Value, out: &mut Vec<(String, String)>) {
    match ctx {
        serde_json::Value::Array(items) => {
            for item in items {
                collect_context_entries(item, out);
            }
        }
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                if let Some(ns) = value.as_str() {
                    if !key.starts_with('@') && (ns.ends_with('/') || ns.ends_with('#')) {
                        out.push((key.clone(), ns.to_string()));
                    }
                }
            }
        }
        _ => {}
    }
}

/// A list's subject, predicate and graph, by term index.
type ListKey = (usize, usize, Option<usize>);

/// Collects quads; turns indexed list items (JSON-LD `@list`) into the
/// `rdf:first`/`rdf:rest` spine they denote.
#[derive(Default)]
struct QuadSink {
    terms: Vec<Term>,
    blanks: HashMap<String, TermId>,
    fresh: usize,
    quads: Vec<Quad>,
    lists: BTreeMap<ListKey, Vec<(i32, usize)>>,
    prefixes: Vec<(String, String)>,
}

impl QuadSink {
    fn add(&mut self, term: Term) -> TermId {
        self.terms.push(term);
        TermId::new((self.terms.len() - 1) as u32)
    }

    fn term(&self, id: TermId) -> &Term {
        &self.terms[id.index() as usize]
    }

    fn fresh_blank(&mut self) -> Term {
        self.fresh += 1;
        // `~` cannot appear in a Turtle or JSON-LD blank node label
        Term::blank(format!("~{}", self.fresh))
    }

    fn push(&mut self, s: TermId, p: TermId, o: TermId, g: Option<TermId>) -> SinkResult {
        let Some(predicate) = self.term(p).as_iri().map(Arc::from) else {
            return Err(SinkError::rejected("a predicate must be an IRI"));
        };
        let graph = match g {
            None => None,
            Some(g) => match self.term(g) {
                Term::Iri(iri) => Some(iri.clone()),
                _ => return Err(SinkError::rejected("a graph name must be an IRI")),
            },
        };
        self.quads.push(Quad {
            subject: self.term(s).clone(),
            predicate,
            object: self.term(o).clone(),
            graph,
        });
        Ok(())
    }

    fn into_parsed(mut self) -> Parsed {
        let lists = std::mem::take(&mut self.lists);
        for ((s, p, g), mut items) in lists {
            items.sort_by_key(|(index, _)| *index);
            let subject = self.terms[s].clone();
            let predicate: Arc<str> = Arc::from(self.terms[p].as_iri().unwrap_or_default());
            let graph = g.and_then(|g| self.terms[g].as_iri().map(Arc::from));
            let quad = |s: Term, p: &str, o: Term| Quad {
                subject: s,
                predicate: Arc::from(p),
                object: o,
                graph: graph.clone(),
            };
            let mut node = self.fresh_blank();
            self.quads.push(Quad {
                subject,
                predicate,
                object: node.clone(),
                graph: graph.clone(),
            });
            for (i, (_, item)) in items.iter().enumerate() {
                self.quads
                    .push(quad(node.clone(), RDF_FIRST, self.terms[*item].clone()));
                let next = if i + 1 == items.len() {
                    Term::iri(RDF_NIL)
                } else {
                    self.fresh_blank()
                };
                self.quads.push(quad(node.clone(), RDF_REST, next.clone()));
                node = next;
            }
        }
        Parsed {
            quads: self.quads,
            prefixes: self.prefixes,
        }
    }
}

impl GraphSink for QuadSink {
    fn on_base(&mut self, _base_iri: &str) {}

    fn on_prefix(&mut self, prefix: &str, namespace_iri: &str) {
        self.prefixes
            .push((prefix.to_string(), namespace_iri.to_string()));
    }

    fn term_iri(&mut self, iri: &str) -> TermId {
        self.add(Term::iri(iri))
    }

    fn term_blank(&mut self, label: Option<&str>) -> TermId {
        match label {
            Some(label) => {
                if let Some(id) = self.blanks.get(label) {
                    return *id;
                }
                let id = self.add(Term::blank(label));
                self.blanks.insert(label.to_string(), id);
                id
            }
            None => {
                let term = self.fresh_blank();
                self.add(term)
            }
        }
    }

    fn term_literal(&mut self, value: &str, datatype: Datatype, language: Option<&str>) -> TermId {
        let term = match language {
            Some(lang) => Term::lang_string(value, lang),
            None => Term::typed(value, datatype),
        };
        self.add(term)
    }

    fn term_literal_value(&mut self, value: LiteralValue, datatype: Datatype) -> TermId {
        self.add(Term::Literal {
            value,
            datatype,
            language: None,
        })
    }

    fn emit_triple(&mut self, subject: TermId, predicate: TermId, object: TermId) -> SinkResult {
        self.push(subject, predicate, object, None)
    }

    fn emit_list_item(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        index: i32,
    ) -> SinkResult {
        self.lists
            .entry((subject.index() as usize, predicate.index() as usize, None))
            .or_default()
            .push((index, object.index() as usize));
        Ok(())
    }

    fn supports_quads(&self) -> bool {
        true
    }

    fn emit_quad(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        graph: TermId,
    ) -> SinkResult {
        self.push(subject, predicate, object, Some(graph))
    }

    fn emit_quad_list_item(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        graph: TermId,
        index: i32,
    ) -> SinkResult {
        self.lists
            .entry((
                subject.index() as usize,
                predicate.index() as usize,
                Some(graph.index() as usize),
            ))
            .or_default()
            .push((index, object.index() as usize));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn turtle_collections_are_spines() {
        let parsed = parse(
            "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n<urn:a> owl:unionOf ( <urn:b> <urn:c> ) .",
            RdfFormat::Turtle,
            None,
        )
        .unwrap();
        // the union triple, then first/rest for each of two items
        assert_eq!(parsed.quads.len(), 5);
        assert_eq!(parsed.prefixes, vec![("owl".to_string(), OWL.to_string())]);
    }

    #[test]
    fn jsonld_lists_become_spines() {
        let parsed = parse(
            r#"{"@id": "urn:a", "http://www.w3.org/2002/07/owl#unionOf": {"@list": [{"@id": "urn:b"}, {"@id": "urn:c"}]}}"#,
            RdfFormat::JsonLd,
            None,
        )
        .unwrap();
        assert_eq!(parsed.quads.len(), 5);
        assert_eq!(parsed.quads.iter().filter(|q| q.is(RDF_FIRST)).count(), 2);
    }

    #[test]
    fn flat_split_separates_catalog_and_document() {
        let parsed = parse(
            r#"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
               @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
               @prefix nif: <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#> .
               <urn:Org> a rdfs:Class ; rdfs:label "organization" .
               <urn:acme> a <urn:Org> ; skos:prefLabel "Acme" .
               <urn:doc> nif:isString "Acme wrote." ."#,
            RdfFormat::Turtle,
            None,
        )
        .unwrap();
        let split = parsed.split();
        assert!(!split.by_graph);
        assert_eq!(split.ontology.len(), 2);
        assert_eq!(split.catalog.len(), 2);
        assert_eq!(split.other, 1);
    }

    #[test]
    fn request_graphs_split_by_role() {
        let parsed = parse(
            r#"{"@graph": [
                 {"@id": "https://ns.flur.ee/extract#ontology", "@graph": [
                   {"@id": "urn:Org", "@type": "http://www.w3.org/2000/01/rdf-schema#Class"}]},
                 {"@id": "https://ns.flur.ee/extract#instances", "@graph": [
                   {"@id": "urn:acme", "@type": "urn:Org"}]}]}"#,
            RdfFormat::JsonLd,
            None,
        )
        .unwrap();
        let split = parsed.split();
        assert!(split.by_graph);
        assert_eq!(split.ontology.len(), 1);
        assert_eq!(split.catalog.len(), 1);
    }
}
