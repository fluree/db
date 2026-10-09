//! RDF documents to and from quads, with no ledger involved.
//!
//! The reading and writing is [`fluree_db_api::rdf`]'s; this module turns
//! its datasets into `(subject, predicate, object, graph)` tuples of term
//! tuples (see [`crate::convert`]) and `fluree.Quad`s back into datasets. A
//! reification travels as the quad `(reifier, rdf:reifies, triple term,
//! graph)`, as RDF 1.2 defines it.

use std::collections::{BTreeMap, HashMap};

use fluree_db_api::rdf::{self, Dataset, PrefixMap, RdfError, RdfFormat, Term};
use fluree_vocab::rdf::REIFIES;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyString, PyTuple};

use crate::convert::{ir_term, is_node, is_triple};
use crate::error::invalid_request;

/// The quads of `text`, read as `format`: `"turtle"`, `"trig"`,
/// `"ntriples"` or `"nquads"`. Turtle and TriG resolve relative IRIs
/// against `base`.
///
/// Each quad is `(subject, predicate, object, graph)`. An IRI or blank node
/// is a ready `fluree.IRI` or `fluree.BlankNode`, one object for every use of
/// it; a literal is a `("literal", lexical, datatype, language)` cell, and a
/// triple term a `("triple", subject, predicate, object)` cell whose parts
/// follow the same rule.
#[pyfunction]
#[pyo3(signature = (text, format, base = None))]
pub(crate) fn parse_rdf<'py>(
    py: Python<'py>,
    text: &str,
    format: &str,
    base: Option<&str>,
) -> PyResult<Bound<'py, PyList>> {
    let format: RdfFormat = format.parse().map_err(rdf_error)?;
    let dataset = py
        .detach(|| rdf::parse(text, format, base))
        .map_err(rdf_error)?;
    let mut terms = Terms::new(py);
    let reifies = terms.iri(REIFIES)?;
    let quads = PyList::empty(py);
    for (name, graph) in dataset.graphs() {
        let name = name.map(|n| terms.term(n)).transpose()?;
        for t in graph.iter() {
            let quad = (
                terms.term(&t.s)?,
                terms.term(&t.p)?,
                terms.term(&t.o)?,
                &name,
            );
            quads.append(quad)?;
        }
        for r in graph.reifications() {
            let triple = terms.triple(&r.triple.s, &r.triple.p, &r.triple.o)?;
            quads.append((terms.term(&r.reifier)?, &reifies, triple, &name))?;
        }
    }
    Ok(quads)
}

/// Python objects for one dataset's terms. An IRI or blank node label is
/// shared by every use of it in the dataset, so its object is made once and
/// keyed by where its text lives; so is a datatype IRI's string.
struct Terms<'py> {
    py: Python<'py>,
    nodes: HashMap<usize, Bound<'py, PyAny>>,
    datatypes: HashMap<usize, Bound<'py, PyString>>,
    literal: Bound<'py, PyString>,
    triple: Bound<'py, PyString>,
}

impl<'py> Terms<'py> {
    fn new(py: Python<'py>) -> Self {
        Self {
            py,
            nodes: HashMap::new(),
            datatypes: HashMap::new(),
            literal: intern!(py, "literal").clone(),
            triple: intern!(py, "triple").clone(),
        }
    }

    fn term(&mut self, term: &Term) -> PyResult<Bound<'py, PyAny>> {
        match term {
            Term::Iri(iri) => self.iri(iri),
            Term::BlankNode(id) => self.node(id.as_str(), crate::convert::blank_node),
            Term::Literal {
                value,
                datatype,
                language,
            } => {
                let datatype = datatype.as_iri();
                let datatype = self
                    .datatypes
                    .entry(datatype.as_ptr() as usize)
                    .or_insert_with(|| PyString::new(self.py, datatype))
                    .clone();
                let cell = (
                    &self.literal,
                    value.lexical(),
                    datatype,
                    language.as_deref(),
                );
                Ok(cell.into_pyobject(self.py)?.into_any())
            }
            Term::TripleTerm(t) => Ok(self.triple(&t[0], &t[1], &t[2])?.into_any()),
        }
    }

    fn iri(&mut self, iri: &str) -> PyResult<Bound<'py, PyAny>> {
        self.node(iri, crate::convert::iri)
    }

    fn node(
        &mut self,
        text: &str,
        make: fn(Python<'py>, &str) -> PyResult<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let key = text.as_ptr() as usize;
        if let Some(object) = self.nodes.get(&key) {
            return Ok(object.clone());
        }
        let object = make(self.py, text)?;
        self.nodes.insert(key, object.clone());
        Ok(object)
    }

    fn triple(&mut self, s: &Term, p: &Term, o: &Term) -> PyResult<Bound<'py, PyTuple>> {
        let parts = (self.term(s)?, self.term(p)?, self.term(o)?);
        (&self.triple, parts.0, parts.1, parts.2).into_pyobject(self.py)
    }
}

/// `quads`, each a `fluree.Quad`, written as `format`: `"turtle"`, `"trig"`,
/// `"ntriples"` or `"nquads"`. Turtle and TriG declare `prefixes` and write
/// with them. A quad `(r, rdf:reifies, triple term)` is written as an
/// annotation where the format has one.
#[pyfunction]
#[pyo3(signature = (quads, format, prefixes = None))]
pub(crate) fn serialize_rdf<'py>(
    py: Python<'py>,
    quads: &Bound<'py, PyAny>,
    format: &str,
    prefixes: Option<BTreeMap<String, String>>,
) -> PyResult<String> {
    let format: RdfFormat = format.parse().map_err(rdf_error)?;
    let mut nodes = Nodes::default();
    let mut dataset = Dataset::new();
    for quad in quads.try_iter()? {
        let quad = quad?;
        let graph = quad.getattr(intern!(py, "graph"))?;
        let graph = (!graph.is_none()).then(|| nodes.term(graph)).transpose()?;
        dataset.add_quad(
            nodes.term(quad.getattr(intern!(py, "subject"))?)?,
            nodes.term(quad.getattr(intern!(py, "predicate"))?)?,
            nodes.object(quad.getattr(intern!(py, "object"))?)?,
            graph.as_ref(),
        );
    }
    let prefixes = PrefixMap::from_map(prefixes.unwrap_or_default());
    py.detach(|| rdf::serialize(&dataset, format, &prefixes))
        .map_err(rdf_error)
}

/// Graph terms for the IRIs and blank nodes of one serialize call, each
/// object converted once. An entry holds its object, so the object's address
/// cannot be reused by another while the call runs.
#[derive(Default)]
struct Nodes<'py> {
    terms: HashMap<usize, (Bound<'py, PyAny>, Term)>,
}

impl<'py> Nodes<'py> {
    fn term(&mut self, object: Bound<'py, PyAny>) -> PyResult<Term> {
        let key = object.as_ptr() as usize;
        if let Some((_, term)) = self.terms.get(&key) {
            return Ok(term.clone());
        }
        let term = ir_term(&object)?;
        self.terms.insert(key, (object, term.clone()));
        Ok(term)
    }

    /// An object-position value: cached when it is an IRI or a blank node,
    /// converted afresh otherwise, and a triple term part by part.
    fn object(&mut self, object: Bound<'py, PyAny>) -> PyResult<Term> {
        let py = object.py();
        if is_node(&object)? {
            self.term(object)
        } else if is_triple(&object)? {
            Ok(Term::triple(
                self.term(object.getattr(intern!(py, "subject"))?)?,
                self.term(object.getattr(intern!(py, "predicate"))?)?,
                self.object(object.getattr(intern!(py, "object"))?)?,
            ))
        } else {
            ir_term(&object)
        }
    }
}

fn rdf_error(error: RdfError) -> PyErr {
    invalid_request(error.to_string())
}
