//! RDF documents to and from quads, with no ledger involved.
//!
//! The reading and writing is [`fluree_db_api::rdf`]'s; this module turns
//! its datasets into `(subject, predicate, object, graph)` tuples of term
//! tuples (see [`crate::convert`]) and `fluree.Quad`s back into datasets. A
//! reification travels as the quad `(reifier, rdf:reifies, triple term,
//! graph)`, as RDF 1.2 defines it.

use std::collections::BTreeMap;

use fluree_db_api::rdf::{self, Dataset, PrefixMap, RdfError, RdfFormat, Term};
use fluree_vocab::rdf::REIFIES;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use crate::convert::ir_term;
use crate::error::invalid_request;

/// The quads of `text`, read as `format`: `"turtle"`, `"trig"`,
/// `"ntriples"` or `"nquads"`. Turtle and TriG resolve relative IRIs
/// against `base`.
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
    let cell = |term: &Term| term_cell(py, term);
    let quads = PyList::empty(py);
    for (name, graph) in dataset.graphs() {
        let name = name.map(cell).transpose()?;
        for t in graph.iter() {
            quads.append((cell(&t.s)?, cell(&t.p)?, cell(&t.o)?, &name))?;
        }
        for r in graph.reifications() {
            let triple = (
                "triple",
                cell(&r.triple.s)?,
                cell(&r.triple.p)?,
                cell(&r.triple.o)?,
            );
            quads.append((cell(&r.reifier)?, ("iri", REIFIES), triple, &name))?;
        }
    }
    Ok(quads)
}

fn term_cell<'py>(py: Python<'py>, term: &Term) -> PyResult<Bound<'py, PyTuple>> {
    match term {
        Term::Iri(iri) => ("iri", iri.as_ref()).into_pyobject(py),
        Term::BlankNode(id) => ("bnode", id.as_str()).into_pyobject(py),
        Term::Literal {
            value,
            datatype,
            language,
        } => (
            "literal",
            value.lexical(),
            datatype.as_iri(),
            language.as_deref(),
        )
            .into_pyobject(py),
        Term::TripleTerm(t) => (
            "triple",
            term_cell(py, &t[0])?,
            term_cell(py, &t[1])?,
            term_cell(py, &t[2])?,
        )
            .into_pyobject(py),
    }
}

/// `quads`, each a `fluree.Quad`, written as `format`: `"turtle"`, `"trig"`,
/// `"ntriples"` or `"nquads"`. Turtle and TriG declare `prefixes` and write
/// with them. A quad `(r, rdf:reifies, triple term)` is written as an
/// annotation where the format has one.
#[pyfunction]
#[pyo3(signature = (quads, format, prefixes = None))]
pub(crate) fn serialize_rdf(
    py: Python<'_>,
    quads: &Bound<'_, PyAny>,
    format: &str,
    prefixes: Option<BTreeMap<String, String>>,
) -> PyResult<String> {
    let format: RdfFormat = format.parse().map_err(rdf_error)?;
    let mut dataset = Dataset::new();
    for quad in quads.try_iter()? {
        let quad = quad?;
        let graph = quad.getattr("graph")?;
        let graph = (!graph.is_none()).then(|| ir_term(&graph)).transpose()?;
        dataset.add_quad(
            ir_term(&quad.getattr("subject")?)?,
            ir_term(&quad.getattr("predicate")?)?,
            ir_term(&quad.getattr("object")?)?,
            graph.as_ref(),
        );
    }
    let prefixes = PrefixMap::from_map(prefixes.unwrap_or_default());
    py.detach(|| rdf::serialize(&dataset, format, &prefixes))
        .map_err(rdf_error)
}

fn rdf_error(error: RdfError) -> PyErr {
    invalid_request(error.to_string())
}
