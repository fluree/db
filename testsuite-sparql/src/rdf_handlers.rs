//! W3C RDF syntax / evaluation test handlers (`rdft:` test types) for the
//! Turtle parser.
//!
//! The RDF 1.1 and 1.2 Turtle suites are plain parser conformance tests:
//! positive-syntax actions must parse, negative-syntax actions must not,
//! and evaluation tests parse an action `.ttl` and compare the resulting
//! graph with an expected `.nt` up to blank-node isomorphism. Everything
//! runs through [`GraphCollectorSink`] — the same sink behind
//! `parse_to_json`, i.e. the Turtle → JSON-LD conversion used by upsert,
//! graph sync and memory import — so a suite regression here is a
//! regression on a shipped ingest path.
//!
//! N-Triples tests are dispatched to the same parser: N-Triples is a
//! syntactic subset of Turtle, so every positive N-Triples document is a
//! valid Turtle document. Negative N-Triples tests can legitimately be valid
//! Turtle (prefixed names, `a`, numeric shorthands); such entries belong in
//! the suite's skip register with that reason.

use std::collections::BTreeMap;

use anyhow::{bail, ensure, Context, Result};
use fluree_graph_ir::{Graph, GraphCollectorSink, Term as IrTerm};
use fluree_graph_turtle::parse as parse_turtle;

use crate::evaluator::TestEvaluator;
use crate::files::read_file_to_string;
use crate::manifest::Test;
use crate::result_comparison::{are_results_isomorphic, format_results_diff};
use crate::result_format::{
    ir_term_to_rdf_term, parse_expected_graph, RdfTerm, SparqlResults, Triple,
};
use crate::vocab::rdft;

const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
const RDF_REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";

/// Register handlers for every `rdft:` test type the Turtle parser can serve.
pub fn register_rdf_tests(evaluator: &mut TestEvaluator) {
    evaluator.register(rdft::TEST_TURTLE_POSITIVE_SYNTAX, evaluate_positive_syntax);
    evaluator.register(rdft::TEST_TURTLE_NEGATIVE_SYNTAX, evaluate_negative_syntax);
    evaluator.register(rdft::TEST_TURTLE_EVAL, evaluate_eval);
    evaluator.register(rdft::TEST_TURTLE_NEGATIVE_EVAL, evaluate_negative_syntax);
    evaluator.register(
        rdft::TEST_NTRIPLES_POSITIVE_SYNTAX,
        evaluate_positive_syntax,
    );
    evaluator.register(
        rdft::TEST_NTRIPLES_NEGATIVE_SYNTAX,
        evaluate_negative_syntax,
    );
}

/// Parse an action document from its manifest URL, resolving relative IRIs
/// against that URL as the W3C harness contract requires.
fn parse_action(url: &str) -> Result<GraphCollectorSink> {
    let content = read_file_to_string(url).with_context(|| format!("Reading {url}"))?;
    let with_base = format!("@base <{url}> .\n{content}");
    let mut sink = GraphCollectorSink::new();
    parse_turtle(&with_base, &mut sink).with_context(|| format!("Parsing {url}"))?;
    Ok(sink)
}

fn action_url(test: &Test) -> Result<&str> {
    test.action
        .as_deref()
        .with_context(|| format!("{}: test has no mf:action document", test.id))
}

/// `rdft:TestTurtlePositiveSyntax` / `rdft:TestNTriplesPositiveSyntax`: the
/// document must parse.
fn evaluate_positive_syntax(test: &Test) -> Result<()> {
    let url = action_url(test)?;
    parse_action(url).map(|_| ()).with_context(|| {
        format!(
            "Positive syntax test failed — parser rejected a valid document.\n\
             Test: {}\nFile: {url}",
            test.id
        )
    })
}

/// `rdft:TestTurtleNegativeSyntax` / `rdft:TestTurtleNegativeEval` /
/// `rdft:TestNTriplesNegativeSyntax`: the document must be rejected.
fn evaluate_negative_syntax(test: &Test) -> Result<()> {
    let url = action_url(test)?;
    let content = read_file_to_string(url).with_context(|| format!("Reading {url}"))?;
    let with_base = format!("@base <{url}> .\n{content}");
    let mut sink = GraphCollectorSink::new();
    let outcome = parse_turtle(&with_base, &mut sink);
    ensure!(
        outcome.is_err(),
        "Negative syntax test failed — parser accepted an invalid document.\n\
         Test: {}\nFile: {url}",
        test.id
    );
    Ok(())
}

/// `rdft:TestTurtleEval`: parse the action, parse the expected N-Triples,
/// compare the two graphs up to blank-node isomorphism.
///
/// Reifier attachments (`Graph::reifications`) are not part of the
/// comparison: the expected `.nt` of every RDF 1.2 star test encodes them as
/// `rdf:reifies <<( s p o )>>` triple terms, which the graph IR has no term
/// for, so those documents fail at parse time and are registered as such.
fn evaluate_eval(test: &Test) -> Result<()> {
    let url = action_url(test)?;
    let result_url = test
        .result
        .as_deref()
        .with_context(|| format!("{}: evaluation test has no mf:result", test.id))?;

    let actual = parse_action(url)
        .map(|sink| graph_to_rdf_triples(&sink.into_graph()))
        .with_context(|| {
            format!(
                "Evaluation test failed — parser rejected the action document.\n\
                 Test: {}\nFile: {url}",
                test.id
            )
        })?;
    let expected = parse_expected_graph(result_url).with_context(|| {
        format!(
            "Evaluation test failed — could not parse the expected graph.\n\
             Test: {}\nFile: {result_url}",
            test.id
        )
    })?;

    let expected = SparqlResults::Graph(expected);
    let actual = SparqlResults::Graph(actual);
    if !are_results_isomorphic(&expected, &actual) {
        bail!(
            "Evaluation test failed — graphs differ.\nTest: {}\nFile: {url}\n{}",
            test.id,
            format_results_diff(&expected, &actual)
        );
    }
    Ok(())
}

/// Convert a parsed graph to harness triples, re-expanding Fluree's
/// `list_index` collection encoding into the `rdf:first` / `rdf:rest` chains
/// the expected N-Triples spell out.
///
/// The Turtle parser emits `( a b )` in object position as one triple per
/// element carrying `list_index` (the transaction layer stores lists that
/// way). The W3C expected graphs are plain RDF, so the comparison needs the
/// chain back: one fresh blank node per element, terminated by `rdf:nil`.
fn graph_to_rdf_triples(graph: &Graph) -> Vec<Triple> {
    let mut out = Vec::new();
    // (subject, predicate) → elements in list order.
    let mut lists: BTreeMap<(IrTerm, IrTerm), Vec<(i32, IrTerm)>> = BTreeMap::new();
    for t in graph.iter() {
        match t.list_index {
            Some(i) => lists
                .entry((t.s.clone(), t.p.clone()))
                .or_default()
                .push((i, t.o.clone())),
            None => out.push(Triple {
                subject: ir_term_to_rdf_term(&t.s),
                predicate: ir_term_to_rdf_term(&t.p),
                object: ir_term_to_rdf_term(&t.o),
            }),
        }
    }
    let mut next_cell = 0usize;
    for ((s, p), mut items) in lists {
        items.sort_by_key(|(i, _)| *i);
        let cells: Vec<RdfTerm> = (0..items.len())
            .map(|_| {
                next_cell += 1;
                RdfTerm::BlankNode(format!("list-cell-{next_cell}"))
            })
            .collect();
        out.push(Triple {
            subject: ir_term_to_rdf_term(&s),
            predicate: ir_term_to_rdf_term(&p),
            object: cells[0].clone(),
        });
        for (k, (_, item)) in items.iter().enumerate() {
            out.push(Triple {
                subject: cells[k].clone(),
                predicate: RdfTerm::Iri(RDF_FIRST.to_string()),
                object: ir_term_to_rdf_term(item),
            });
            let rest = match cells.get(k + 1) {
                Some(next) => next.clone(),
                None => RdfTerm::Iri(RDF_NIL.to_string()),
            };
            out.push(Triple {
                subject: cells[k].clone(),
                predicate: RdfTerm::Iri(RDF_REST.to_string()),
                object: rest,
            });
        }
    }
    out
}
