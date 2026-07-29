//! Executing a single W3C RDF syntax test.

use anyhow::{anyhow, bail, Result};
use fluree_graph_ir::DatasetCollectorSink;
use fluree_graph_ir::{GraphCollectorSink, Triple};
use fluree_graph_turtle::{
    parse_nquads, parse_ntriples, parse_with_prefixes_base_options, Dialect, ParserOptions,
};

use crate::files::read_file_to_string;
use crate::isomorphism::{are_datasets_isomorphic, are_graphs_isomorphic};
use crate::manifest::Test;
use crate::vocab::rdft;

/// Which parser configuration a suite run exercises.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParseMode {
    /// [`ParserOptions::conformant`] — spine collections, preserved numeric
    /// lexical forms. **This is the mode the suite gates**: it is the shape
    /// `fluree rdf convert` has to produce, and the shape W3C defines.
    Conformant,
    /// [`ParserOptions::default`] — the ingest shape (indexed list items,
    /// canonicalized numerics). Deliberately lossy as RDF, so its failures
    /// are expected and reported for information only, never gated.
    IngestDefault,
}

impl ParseMode {
    pub fn options(self) -> ParserOptions {
        match self {
            ParseMode::Conformant => ParserOptions::conformant(),
            ParseMode::IngestDefault => ParserOptions::default(),
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            ParseMode::Conformant => "conformant",
            ParseMode::IngestDefault => "ingest-default",
        }
    }
}

/// Outcome of one test: `Ok(())` passed, `Err(_)` failed with the reason.
pub type TestOutcome = Result<()>;

/// Run a test according to its `rdf:type`.
///
/// An unrecognized type is an error, not a skip: a suite that silently
/// ignored a whole test class would report a green pass rate over a
/// denominator that quietly shrank.
pub fn run_test(test: &Test, mode: ParseMode) -> TestOutcome {
    let nq_positive = test.is_kind(rdft::NQUADS_POSITIVE_SYNTAX);
    let nq_negative = test.is_kind(rdft::NQUADS_NEGATIVE_SYNTAX);
    let nt_positive = test.is_kind(rdft::NTRIPLES_POSITIVE_SYNTAX);
    let nt_negative = test.is_kind(rdft::NTRIPLES_NEGATIVE_SYNTAX);
    if nq_positive || nt_positive {
        let action = test
            .action
            .as_deref()
            .ok_or_else(|| anyhow!("positive syntax test has no mf:action"))?;
        return parse_line_document(action, nq_positive)
            .map_err(|e| anyhow!("expected the document to parse, but it was rejected: {e:#}"));
    }
    if nq_negative || nt_negative {
        let action = test
            .action
            .as_deref()
            .ok_or_else(|| anyhow!("negative syntax test has no mf:action"))?;
        return match parse_line_document(action, nq_negative) {
            Ok(()) => bail!("expected the document to be REJECTED, but it parsed"),
            Err(_) => Ok(()),
        };
    }
    if test.is_kind(rdft::TRIG_POSITIVE_SYNTAX) {
        let action = test
            .action
            .as_deref()
            .ok_or_else(|| anyhow!("positive syntax test has no mf:action"))?;
        return parse_trig_document(action, test.base.as_deref())
            .map_err(|e| anyhow!("expected the document to parse, but it was rejected: {e:#}"));
    }
    if test.is_kind(rdft::TRIG_NEGATIVE_SYNTAX) {
        let action = test
            .action
            .as_deref()
            .ok_or_else(|| anyhow!("negative syntax test has no mf:action"))?;
        return match parse_trig_document(action, test.base.as_deref()) {
            Ok(()) => bail!("expected the document to be REJECTED, but it parsed"),
            Err(_) => Ok(()),
        };
    }
    if test.is_kind(rdft::TRIG_NEGATIVE_EVAL) {
        let action = test
            .action
            .as_deref()
            .ok_or_else(|| anyhow!("negative eval test has no mf:action"))?;
        return match parse_trig_dataset(action, test.base.as_deref()) {
            Ok(_) => bail!("expected the document to be REJECTED, but it parsed"),
            Err(_) => Ok(()),
        };
    }
    if test.is_kind(rdft::TRIG_EVAL) {
        let action = test
            .action
            .as_deref()
            .ok_or_else(|| anyhow!("eval test has no mf:action"))?;
        let result = test
            .result
            .as_deref()
            .ok_or_else(|| anyhow!("eval test has no mf:result"))?;

        let actual = parse_trig_dataset(action, test.base.as_deref())
            .map_err(|e| anyhow!("action document failed to parse: {e:#}"))?;
        let expected = parse_nquads_dataset(result)
            .map_err(|e| anyhow!("gold N-Quads file failed to parse: {e:#}"))?;

        return if are_datasets_isomorphic(&expected, &actual) {
            Ok(())
        } else {
            bail!(
                "parsed dataset is not isomorphic to the expected dataset\n\
                 expected: default={} named={}\n\
                 actual:   default={} named={}",
                expected.default_graph().len(),
                expected.named_graph_count(),
                actual.default_graph().len(),
                actual.named_graph_count()
            )
        };
    }
    if test.is_kind(rdft::TURTLE_POSITIVE_SYNTAX) || test.is_kind(rdft::NTRIPLES_POSITIVE_SYNTAX) {
        return positive_syntax(test, mode);
    }
    if test.is_kind(rdft::TURTLE_NEGATIVE_SYNTAX) || test.is_kind(rdft::NTRIPLES_NEGATIVE_SYNTAX) {
        return negative_syntax(test, mode);
    }
    // A negative *eval* test is a document that lexes but denotes nothing
    // valid (bad language tag, ill-formed IRI). The required behavior is the
    // same as negative syntax — reject it — so they share a handler.
    if test.is_kind(rdft::TURTLE_NEGATIVE_EVAL) {
        return negative_syntax(test, mode);
    }
    if test.is_kind(rdft::TURTLE_EVAL) {
        return eval(test, mode);
    }
    if test.is_kind(rdft::NTRIPLES_POSITIVE_C14N) {
        bail!(
            "canonical N-Triples serialization is not implemented \
             (no writer exists yet — M1 scope)"
        );
    }

    Err(anyhow!(
        "unrecognized test type(s) {:?} — the harness must learn this class \
         before the suite's pass rate means anything",
        test.kinds
    ))
}

/// Parse a document, returning its triples.
/// Parse a TriG document into a dataset, discarding it — syntax tests only
/// ask whether it parses.
///
/// A dataset sink is required, not incidental: named graphs are refused
/// against a triple-only sink, so parsing TriG into `GraphCollectorSink`
/// would report every named-graph document as a syntax error.
/// Parse a line-format document with the STRICT reader.
///
/// N-Triples and N-Quads go through `nquads.rs`, not the Turtle parser.
/// That is what makes their negative-syntax tests meaningful: every one of
/// them is a document that is valid Turtle and invalid here, so a
/// Turtle-based reader could not fail them (burn-down cause E).
fn parse_line_document(url: &str, quads: bool) -> Result<()> {
    let content = read_file_to_string(url)?;
    let mut sink = DatasetCollectorSink::new();
    if quads {
        parse_nquads(&content, &mut sink)?;
    } else {
        parse_ntriples(&content, &mut sink)?;
    }
    Ok(())
}

/// Parse a TriG document into a dataset.
fn parse_trig_dataset(url: &str, base: Option<&str>) -> Result<fluree_graph_ir::Dataset> {
    let content = read_file_to_string(url)?;
    let mut sink = DatasetCollectorSink::new();
    parse_with_prefixes_base_options(
        &content,
        &mut sink,
        &[],
        base,
        ParserOptions::conformant().with_dialect(Dialect::TriG),
    )?;
    Ok(sink.into_dataset())
}

/// Parse an N-Quads gold file into a dataset.
fn parse_nquads_dataset(url: &str) -> Result<fluree_graph_ir::Dataset> {
    let content = read_file_to_string(url)?;
    let mut sink = DatasetCollectorSink::new();
    parse_nquads(&content, &mut sink)?;
    Ok(sink.into_dataset())
}

fn parse_trig_document(url: &str, base: Option<&str>) -> Result<()> {
    let content = read_file_to_string(url)?;
    let mut sink = DatasetCollectorSink::new();
    parse_with_prefixes_base_options(
        &content,
        &mut sink,
        &[],
        base,
        ParserOptions::conformant().with_dialect(Dialect::TriG),
    )?;
    Ok(())
}

fn parse_document(url: &str, base: Option<&str>, mode: ParseMode) -> Result<Vec<Triple>> {
    let content = read_file_to_string(url)?;
    let mut sink = GraphCollectorSink::new();
    parse_with_prefixes_base_options(&content, &mut sink, &[], base, mode.options())?;
    Ok(sink.into_graph().into_triples())
}

fn positive_syntax(test: &Test, mode: ParseMode) -> TestOutcome {
    let action = test
        .action
        .as_deref()
        .ok_or_else(|| anyhow!("positive syntax test has no mf:action"))?;
    parse_document(action, test.base.as_deref(), mode)
        .map(|_| ())
        .map_err(|e| anyhow!("expected the document to parse, but it was rejected: {e:#}"))
}

fn negative_syntax(test: &Test, mode: ParseMode) -> TestOutcome {
    let action = test
        .action
        .as_deref()
        .ok_or_else(|| anyhow!("negative syntax test has no mf:action"))?;
    match parse_document(action, test.base.as_deref(), mode) {
        Ok(triples) => bail!(
            "expected the document to be REJECTED, but it parsed into {} triple(s)",
            triples.len()
        ),
        Err(_) => Ok(()),
    }
}

fn eval(test: &Test, mode: ParseMode) -> TestOutcome {
    let action = test
        .action
        .as_deref()
        .ok_or_else(|| anyhow!("eval test has no mf:action"))?;
    let result = test
        .result
        .as_deref()
        .ok_or_else(|| anyhow!("eval test has no mf:result"))?;

    let actual = parse_document(action, test.base.as_deref(), mode)
        .map_err(|e| anyhow!("action document failed to parse: {e:#}"))?;

    // The gold file is N-Triples, which has no relative IRIs and no
    // directives, so it needs no base. It is read with the same parser (see
    // the N-Triples note in lib.rs) and in the same mode, so a mode-specific
    // term shape cannot make a test pass by canceling out on both sides —
    // gold files contain no collections and no bare numeric literals.
    let expected = parse_document(result, None, mode)
        .map_err(|e| anyhow!("gold N-Triples file failed to parse: {e:#}"))?;

    if are_graphs_isomorphic(&expected, &actual) {
        Ok(())
    } else {
        bail!(
            "parsed graph is not isomorphic to the expected graph\n\
             expected {} triple(s):\n{}\n\
             actual {} triple(s):\n{}",
            expected.len(),
            render(&expected),
            actual.len(),
            render(&actual)
        )
    }
}

fn render(triples: &[Triple]) -> String {
    let mut sorted: Vec<&Triple> = triples.iter().collect();
    sorted.sort();
    sorted
        .iter()
        .map(|t| format!("  {t}"))
        .collect::<Vec<_>>()
        .join("\n")
}
