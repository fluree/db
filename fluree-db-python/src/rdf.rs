//! RDF documents to and from quads, with no ledger involved.
//!
//! `parse_rdf` reads Turtle, TriG, N-Triples or N-Quads with the standalone
//! readers (the Turtle parser's conformant shape, and the strict line
//! reader) into `(subject, predicate, object, graph)` tuples of term tuples
//! (see [`crate::convert`]); `serialize_rdf` writes quads with the RDF text
//! writers. A reifier attachment travels as the quad
//! `(reifier, rdf:reifies, triple term, graph)`, as RDF 1.2 defines it.

use std::collections::{BTreeMap, HashMap, HashSet};

use fluree_graph_format::{format_nquads, format_ntriples, format_trig, format_turtle, PrefixMap};
use fluree_graph_ir::{Dataset, GraphCollectorSink, Term};
use fluree_graph_turtle::{
    parse_nquads, parse_ntriples, parse_with_prefixes_base_options, Dialect, ParserOptions,
    TurtleError,
};
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
    let dataset = py
        .detach(|| read(text, format, base))
        .map_err(invalid_request)?;
    let names = minted_names(&dataset);
    let cell = |term: &Term| term_cell(py, term, &names);
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

fn read(text: &str, format: &str, base: Option<&str>) -> Result<Dataset, String> {
    let mut sink = GraphCollectorSink::with_named_graphs();
    let dialect = match format {
        "turtle" => Some(Dialect::Turtle),
        "trig" => Some(Dialect::TriG),
        "ntriples" | "nquads" => None,
        other => return Err(format!("cannot parse {other:?}")),
    };
    let parsed = match (dialect, format) {
        (Some(dialect), _) => {
            let options = ParserOptions::conformant().with_dialect(dialect);
            parse_with_prefixes_base_options(text, &mut sink, &[], base, options)
        }
        (None, "ntriples") => parse_ntriples(text, &mut sink),
        (None, _) => parse_nquads(text, &mut sink),
    };
    parsed.map_err(|e| located(text, e))?;
    Ok(sink.into_dataset())
}

/// A parse error with its line and column rather than a byte offset.
fn located(text: &str, error: TurtleError) -> String {
    let TurtleError::Parse { position, message } = &error else {
        return error.to_string();
    };
    let before = &text[..(*position).min(text.len())];
    let line = before.matches('\n').count() + 1;
    let column = before.rsplit('\n').next().map_or(0, |l| l.chars().count()) + 1;
    format!("line {line}, column {column}: {message}")
}

/// New labels for the blank nodes the readers minted for `[]`, `[ … ]`,
/// collections and annotations. Their labels (`-b1`, …) are kept apart from
/// every label a document can write, and no text format accepts them back,
/// so each becomes `bN` — or `bN_k` when the document already uses `bN`.
fn minted_names(dataset: &Dataset) -> HashMap<String, String> {
    fn labels<'a>(term: &'a Term, out: &mut Vec<&'a str>) {
        match term {
            Term::BlankNode(id) => out.push(id.as_str()),
            Term::TripleTerm(t) => t.iter().for_each(|part| labels(part, out)),
            Term::Iri(_) | Term::Literal { .. } => {}
        }
    }
    let mut all = Vec::new();
    for (name, graph) in dataset.graphs() {
        if let Some(name) = name {
            labels(name, &mut all);
        }
        for t in graph.iter() {
            [&t.s, &t.p, &t.o]
                .into_iter()
                .for_each(|term| labels(term, &mut all));
        }
        for r in graph.reifications() {
            [&r.reifier, &r.triple.s, &r.triple.o]
                .into_iter()
                .for_each(|term| labels(term, &mut all));
        }
    }
    let mut taken: HashSet<String> = all
        .iter()
        .filter(|l| !l.starts_with('-'))
        .map(|l| (*l).to_string())
        .collect();
    let mut names = HashMap::new();
    for minted in all.into_iter().filter(|l| l.starts_with('-')) {
        if names.contains_key(minted) {
            continue;
        }
        let stem = &minted[1..];
        let mut name = stem.to_string();
        let mut k = 0;
        while taken.contains(&name) {
            k += 1;
            name = format!("{stem}_{k}");
        }
        taken.insert(name.clone());
        names.insert(minted.to_string(), name);
    }
    names
}

fn term_cell<'py>(
    py: Python<'py>,
    term: &Term,
    names: &HashMap<String, String>,
) -> PyResult<Bound<'py, PyTuple>> {
    match term {
        Term::Iri(iri) => ("iri", iri.as_ref()).into_pyobject(py),
        Term::BlankNode(id) => {
            let label = id.as_str();
            ("bnode", names.get(label).map_or(label, String::as_str)).into_pyobject(py)
        }
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
            term_cell(py, &t[0], names)?,
            term_cell(py, &t[1], names)?,
            term_cell(py, &t[2], names)?,
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
    let mut dataset = Dataset::new();
    for quad in quads.try_iter()? {
        let quad = quad?;
        let graph = quad.getattr("graph")?;
        let graph = (!graph.is_none()).then(|| ir_term(&graph)).transpose()?;
        let subject = ir_term(&quad.getattr("subject")?)?;
        let predicate = ir_term(&quad.getattr("predicate")?)?;
        let object = ir_term(&quad.getattr("object")?)?;
        let target = dataset.graph_mut(graph.as_ref());
        match object {
            Term::TripleTerm(t) if predicate.as_iri() == Some(REIFIES) => {
                let [s, p, o] = (*t).clone();
                target.add_reification(s, p, o, subject);
            }
            object => target.add_triple(subject, predicate, object),
        }
    }
    let prefixes = PrefixMap::from_map(prefixes.unwrap_or_default());
    let named = !dataset.is_default_only();
    let written = py.detach(|| match format {
        "ntriples" | "turtle" if named => Err(format!(
            "{format} has no named graphs; write them as trig or nquads"
        )),
        "ntriples" => format_ntriples(&dataset.default).map_err(|e| e.to_string()),
        "nquads" => format_nquads(&dataset).map_err(|e| e.to_string()),
        "turtle" => format_turtle(&dataset.default, &prefixes).map_err(|e| e.to_string()),
        "trig" => format_trig(&dataset, &prefixes).map_err(|e| e.to_string()),
        other => Err(format!("cannot write {other:?}")),
    });
    written.map_err(invalid_request)
}
