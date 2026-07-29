//! Executing a single W3C RDF syntax test.

use anyhow::{anyhow, bail, Result};
use fluree_graph_ir::{GraphCollectorSink, Triple};
use fluree_graph_turtle::{parse_with_prefixes_base_options, ParserOptions};

use crate::files::read_file_to_string;
use crate::isomorphism::are_graphs_isomorphic;
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
