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
//!
//! TriG documents parse the way TriG ingest parses them ([`trig_dataset`]),
//! and N-Quads documents are first regrouped into TriG by
//! `nquads_to_trig`, as bulk import does. Canonicalization (C14N) tests
//! write the parsed document back with the N-Triples / N-Quads writer that
//! serves CONSTRUCT results and compare it with the canonical form.
//!
//! [`register_reader_tests`] runs the same test types through the standalone
//! readers instead: the strict N-Triples / N-Quads reader, and the Turtle
//! parser's conformant Turtle and TriG.

use std::collections::{BTreeMap, HashSet};

use anyhow::{bail, ensure, Context, Result};
use fluree_graph_ir::{Graph, GraphCollectorSink, Term as IrTerm};
use fluree_graph_turtle::{parse as parse_turtle, Dialect, ParserOptions};

use crate::evaluator::TestEvaluator;
use crate::files::read_file_to_string;
use crate::manifest::Test;
use crate::result_comparison::{are_results_isomorphic, format_results_diff};
use crate::result_format::{
    ir_term_to_rdf_term, reification_triples, trig_dataset, RdfTerm, SparqlResults, Triple,
};
use crate::vocab::rdft;
use fluree_graph_ir::Dataset;

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
    evaluator.register(rdft::TEST_NTRIPLES_POSITIVE_C14N, evaluate_ntriples_c14n);
    evaluator.register(rdft::TEST_NQUADS_POSITIVE_SYNTAX, |t| {
        positive_dataset_syntax(t, nquads_dataset)
    });
    evaluator.register(rdft::TEST_NQUADS_NEGATIVE_SYNTAX, |t| {
        negative_dataset_syntax(t, nquads_dataset)
    });
    evaluator.register(rdft::TEST_NQUADS_POSITIVE_C14N, evaluate_nquads_c14n);
    evaluator.register(rdft::TEST_TRIG_POSITIVE_SYNTAX, |t| {
        positive_dataset_syntax(t, trig_action)
    });
    evaluator.register(rdft::TEST_TRIG_NEGATIVE_SYNTAX, |t| {
        negative_dataset_syntax(t, trig_action)
    });
    evaluator.register(rdft::TEST_TRIG_NEGATIVE_EVAL, |t| {
        negative_dataset_syntax(t, trig_action)
    });
    evaluator.register(rdft::TEST_TRIG_EVAL, evaluate_trig_eval);
}

/// The syntaxes the standalone readers take.
#[derive(Clone, Copy)]
enum Syntax {
    Turtle,
    TriG,
    NTriples,
    NQuads,
}

/// Register every RDF syntax test type against the standalone readers: the
/// strict line reader for N-Triples and N-Quads, and the Turtle parser in
/// its conformant shape (`rdf:first`/`rdf:rest` collections, numeric lexical
/// forms kept) for Turtle and TriG. Expected results are read the same way.
pub fn register_reader_tests(evaluator: &mut TestEvaluator) {
    use Syntax::*;
    for (positive, negative, syntax) in [
        (
            rdft::TEST_TURTLE_POSITIVE_SYNTAX,
            rdft::TEST_TURTLE_NEGATIVE_SYNTAX,
            Turtle,
        ),
        (
            rdft::TEST_TRIG_POSITIVE_SYNTAX,
            rdft::TEST_TRIG_NEGATIVE_SYNTAX,
            TriG,
        ),
        (
            rdft::TEST_NTRIPLES_POSITIVE_SYNTAX,
            rdft::TEST_NTRIPLES_NEGATIVE_SYNTAX,
            NTriples,
        ),
        (
            rdft::TEST_NQUADS_POSITIVE_SYNTAX,
            rdft::TEST_NQUADS_NEGATIVE_SYNTAX,
            NQuads,
        ),
    ] {
        evaluator.register(positive, move |t| reader_syntax(t, syntax, true));
        evaluator.register(negative, move |t| reader_syntax(t, syntax, false));
    }
    evaluator.register(rdft::TEST_TURTLE_NEGATIVE_EVAL, |t| {
        reader_syntax(t, Turtle, false)
    });
    evaluator.register(rdft::TEST_TRIG_NEGATIVE_EVAL, |t| {
        reader_syntax(t, TriG, false)
    });
    evaluator.register(rdft::TEST_TURTLE_EVAL, |t| reader_eval(t, Turtle, NTriples));
    evaluator.register(rdft::TEST_TRIG_EVAL, |t| reader_eval(t, TriG, NQuads));
    evaluator.register(rdft::TEST_NTRIPLES_POSITIVE_C14N, |t| {
        let dataset = read(action_url(t)?, NTriples)?;
        let written = fluree_graph_format::format_ntriples(&dataset.default)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        compare_c14n(t, &written)
    });
    evaluator.register(rdft::TEST_NQUADS_POSITIVE_C14N, |t| {
        let dataset = read(action_url(t)?, NQuads)?;
        let written =
            fluree_graph_format::format_nquads(&dataset).map_err(|e| anyhow::anyhow!("{e}"))?;
        compare_c14n(t, &written)
    });
}

/// The document at `url`, read by the standalone reader for `syntax`;
/// Turtle and TriG resolve relative IRIs against `url`.
fn read(url: &str, syntax: Syntax) -> Result<Dataset> {
    let content = read_file_to_string(url).with_context(|| format!("Reading {url}"))?;
    let mut sink = GraphCollectorSink::with_named_graphs();
    let parsed = match syntax {
        Syntax::Turtle | Syntax::TriG => {
            let dialect = match syntax {
                Syntax::TriG => Dialect::TriG,
                _ => Dialect::Turtle,
            };
            fluree_graph_turtle::parse_with_options(
                &format!("@base <{url}> .\n{content}"),
                &mut sink,
                ParserOptions::conformant().with_dialect(dialect),
            )
        }
        Syntax::NTriples => fluree_graph_turtle::parse_ntriples(&content, &mut sink),
        Syntax::NQuads => fluree_graph_turtle::parse_nquads(&content, &mut sink),
    };
    parsed.with_context(|| format!("Parsing {url}"))?;
    Ok(sink.into_dataset())
}

fn reader_syntax(test: &Test, syntax: Syntax, valid: bool) -> Result<()> {
    let url = action_url(test)?;
    match read(url, syntax) {
        Ok(_) if !valid => bail!(
            "Negative syntax test failed — reader accepted an invalid document.\n\
             Test: {}\nFile: {url}",
            test.id
        ),
        Err(e) if valid => Err(e).with_context(|| {
            format!(
                "Positive syntax test failed — reader rejected a valid document.\n\
                 Test: {}\nFile: {url}",
                test.id
            )
        }),
        _ => Ok(()),
    }
}

/// The action read as `syntax` against the result read as `expected`.
fn reader_eval(test: &Test, syntax: Syntax, expected: Syntax) -> Result<()> {
    let url = action_url(test)?;
    let result_url = test
        .result
        .as_deref()
        .with_context(|| format!("{}: evaluation test has no mf:result", test.id))?;
    let actual = read(url, syntax).with_context(|| {
        format!(
            "Evaluation test failed — reader rejected the action document.\n\
             Test: {}\nFile: {url}",
            test.id
        )
    })?;
    let expected = read(result_url, expected).with_context(|| {
        format!(
            "Evaluation test failed — could not read the expected dataset.\n\
             Test: {}\nFile: {result_url}",
            test.id
        )
    })?;
    compare_datasets(test, url, &expected, &actual)
}

/// A TriG action, relative IRIs resolved against its manifest URL.
fn trig_action(url: &str) -> Result<Dataset> {
    let content = read_file_to_string(url).with_context(|| format!("Reading {url}"))?;
    trig_dataset(&format!("@base <{url}> .\n{content}"))
}

/// An N-Quads document, regrouped into TriG as bulk import does.
fn nquads_dataset(url: &str) -> Result<Dataset> {
    let content = read_file_to_string(url).with_context(|| format!("Reading {url}"))?;
    let trig =
        fluree_db_transact::parse::nquads_to_trig(&content).map_err(|e| anyhow::anyhow!("{e}"))?;
    trig_dataset(&trig)
}

fn positive_dataset_syntax(test: &Test, parse: fn(&str) -> Result<Dataset>) -> Result<()> {
    let url = action_url(test)?;
    parse(url).map(|_| ()).with_context(|| {
        format!(
            "Positive syntax test failed — parser rejected a valid document.\n\
             Test: {}\nFile: {url}",
            test.id
        )
    })
}

fn negative_dataset_syntax(test: &Test, parse: fn(&str) -> Result<Dataset>) -> Result<()> {
    let url = action_url(test)?;
    ensure!(
        parse(url).is_err(),
        "Negative syntax test failed — parser accepted an invalid document.\n\
         Test: {}\nFile: {url}",
        test.id
    );
    Ok(())
}

/// `rdft:TestTrigEval`: the action's dataset against the expected N-Quads,
/// graph by graph up to blank-node isomorphism.
fn evaluate_trig_eval(test: &Test) -> Result<()> {
    let url = action_url(test)?;
    let result_url = test
        .result
        .as_deref()
        .with_context(|| format!("{}: evaluation test has no mf:result", test.id))?;
    let actual = trig_action(url).with_context(|| {
        format!(
            "Evaluation test failed — parser rejected the action document.\n\
             Test: {}\nFile: {url}",
            test.id
        )
    })?;
    let expected = nquads_dataset(result_url).with_context(|| {
        format!(
            "Evaluation test failed — could not parse the expected dataset.\n\
             Test: {}\nFile: {result_url}",
            test.id
        )
    })?;
    compare_datasets(test, url, &expected, &actual)
}

/// Graph names in order, then each graph up to blank-node isomorphism.
fn compare_datasets(test: &Test, url: &str, expected: &Dataset, actual: &Dataset) -> Result<()> {
    let names = |d: &Dataset| d.named.keys().map(ir_term_to_rdf_term).collect::<Vec<_>>();
    let as_graphs = |d: &Dataset| {
        std::iter::once(graph_to_rdf_triples(&d.default))
            .chain(d.named.values().map(graph_to_rdf_triples))
            .collect::<Vec<_>>()
    };
    let (expected_names, actual_names) = (names(expected), names(actual));
    ensure!(
        expected_names.len() == actual_names.len()
            && expected_names
                .iter()
                .zip(&actual_names)
                .all(|(e, a)| e == a
                    || matches!((e, a), (RdfTerm::BlankNode(_), RdfTerm::BlankNode(_)))),
        "Evaluation test failed — graph names differ.\nTest: {}\nFile: {url}\n\
         expected {expected_names:?}\nactual {actual_names:?}",
        test.id
    );
    for (expected, actual) in as_graphs(expected).into_iter().zip(as_graphs(actual)) {
        let expected = SparqlResults::Graph(expected);
        let actual = SparqlResults::Graph(actual);
        if !are_results_isomorphic(&expected, &actual) {
            bail!(
                "Evaluation test failed — graphs differ.\nTest: {}\nFile: {url}\n{}",
                test.id,
                format_results_diff(&expected, &actual)
            );
        }
    }
    Ok(())
}

/// `rdft:TestNTriplesPositiveC14N`: the parsed document, written by the
/// N-Triples writer, is the expected canonical form.
fn evaluate_ntriples_c14n(test: &Test) -> Result<()> {
    let url = action_url(test)?;
    let graph = parse_action(url)?.into_graph();
    let written =
        fluree_graph_format::format_ntriples(&graph).map_err(|e| anyhow::anyhow!("{e}"))?;
    compare_c14n(test, &written)
}

/// `rdft:TestNQuadsPositiveC14N`: as for N-Triples, with the N-Quads writer.
fn evaluate_nquads_c14n(test: &Test) -> Result<()> {
    let dataset = nquads_dataset(action_url(test)?)?;
    let written =
        fluree_graph_format::format_nquads(&dataset).map_err(|e| anyhow::anyhow!("{e}"))?;
    compare_c14n(test, &written)
}

/// Canonical documents are sets of lines; their order is unspecified.
fn compare_c14n(test: &Test, written: &str) -> Result<()> {
    let result_url = test
        .result
        .as_deref()
        .with_context(|| format!("{}: C14N test has no mf:result", test.id))?;
    let expected = read_file_to_string(result_url)?;
    let lines = |s: &str| {
        let mut v: Vec<String> = s.lines().map(str::to_string).collect();
        v.sort();
        v
    };
    ensure!(
        lines(&expected) == lines(written),
        "C14N test failed — written form differs.\nTest: {}\nexpected:\n{expected}\nwritten:\n{written}",
        test.id
    );
    Ok(())
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
/// Both documents go through the same parser, reifier attachments included:
/// the expected `.nt` spells each one `r rdf:reifies <<( s p o )>>`, which
/// the parser reads as the same attachment an action's `<< s p o >>` or
/// `{| |}` produces. Only the annotation syntax asserts `s p o`, on either
/// side.
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
    let expected = parse_action(result_url)
        .map(|sink| graph_to_rdf_triples(&sink.into_graph()))
        .with_context(|| {
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
/// the expected N-Triples spell out, and each reifier attachment into
/// `rdf:reifies` triples.
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
    for r in graph.reifications() {
        out.extend(reification_triples(
            ir_term_to_rdf_term(&r.reifier),
            ir_term_to_rdf_term(&r.triple.s),
            ir_term_to_rdf_term(&r.triple.p),
            ir_term_to_rdf_term(&r.triple.o),
        ));
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
    // An RDF graph is a set.
    let mut seen = HashSet::new();
    out.retain(|t| seen.insert(t.clone()));
    out
}
