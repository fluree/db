//! `fluree rdf check` — parse a document and report what is wrong with it.
//!
//! The verb exists because "does this file parse" is a question people ask of
//! a corpus before they ask anything else, and answering it today means
//! loading the file into a ledger and reading the failure out of a transaction
//! error. Here it is a parse and an exit code.
//!
//! # Exit codes
//!
//! - `0` — the document parsed.
//! - `1` — the document did not parse. Diagnostics are on stdout.
//! - `2` — the invocation was wrong: no such file, unreadable, unknown syntax.
//!
//! The 1/2 split is the contract that makes this usable in a script. A tool
//! that exits non-zero for both "your RDF is broken" and "your path was
//! typo'd" cannot be wrapped by anything that wants to react differently.

use crate::cli::RdfCommonArgs;
use crate::error::CliResult;
use crate::rdf::diagnostic::{self, Diagnostic};
use crate::rdf::{self, exit_document_invalid};
use colored::Colorize;
use fluree_graph_ir::PhaseTimings;
use serde::Serialize;

/// Schema identifier carried by `--format json` output.
pub const CHECK_SCHEMA: &str = "fluree.rdf.check.v1";

/// How `check` reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum CheckFormat {
    /// Human-readable diagnostics with a caret under the offending column.
    #[default]
    Table,
    /// One JSON document, for a script that wants the offsets.
    Json,
}

/// A machine-readable check result.
#[derive(Serialize)]
struct CheckReport {
    schema: &'static str,
    input: String,
    syntax: &'static str,
    /// Whether the document parsed clean.
    ok: bool,
    /// Statements the parser completed before it stopped — where in the
    /// document the trouble started, measured in statements rather than
    /// bytes. Turtle counts a directive as a statement (`statement ::=
    /// directive | triples '.'`), so a file's `@prefix` block is included.
    grammar_statements: u64,
    diagnostics: Vec<Diagnostic>,
}

/// Run `fluree rdf check`.
pub fn run(common: &RdfCommonArgs, format: CheckFormat, quiet: bool) -> CliResult<()> {
    let mut timings = PhaseTimings::start();
    let loaded = rdf::load(common, &mut timings)?;
    let outcome = rdf::parse_document(
        &loaded.text,
        loaded.resolved.syntax,
        common.base.as_deref(),
        rdf::verb_options(common.nocheck),
        &mut timings,
    )?;
    let wall = timings.wall();

    let diagnostics: Vec<Diagnostic> = outcome
        .error
        .as_ref()
        .map(|e| vec![diagnostic::from_turtle_error(e, &loaded.text)])
        .unwrap_or_default();
    let ok = diagnostics.is_empty();

    match format {
        CheckFormat::Json => {
            let report = CheckReport {
                schema: CHECK_SCHEMA,
                input: loaded.input.display(),
                syntax: loaded.resolved.syntax.as_str(),
                ok,
                grammar_statements: outcome.counts.statements,
                diagnostics,
            };
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        CheckFormat::Table => print_table(&loaded.input.display(), &diagnostics, ok, quiet),
    }

    rdf::report_run(common, "check", &loaded, &outcome, &timings, wall)?;

    if ok {
        Ok(())
    } else {
        Err(exit_document_invalid())
    }
}

/// Human diagnostics, on **stderr**.
///
/// riot puts parse errors on stderr and so does `fluree rdf count`; a `check`
/// that put them on stdout would be the one verb in the group whose failure
/// output lands in a redirect. `--format json` still goes to stdout — that one
/// is a document a script consumes, not a message.
fn print_table(input: &str, diagnostics: &[Diagnostic], ok: bool, quiet: bool) {
    for d in diagnostics {
        match (d.line, d.column) {
            (Some(line), Some(column)) => {
                eprintln!(
                    "{} {input}:{line}:{column}: {}",
                    "error:".red().bold(),
                    d.message
                );
            }
            _ => eprintln!("{} {input}: {}", "error:".red().bold(), d.message),
        }
        if let (Some(snippet), Some(caret)) = (d.snippet.as_ref(), d.caret()) {
            eprintln!("  {snippet}");
            eprintln!("  {caret}");
        }
    }

    // The clean case is the one a script runs a thousand times; under
    // --quiet the exit code is the whole answer.
    if ok && !quiet {
        eprintln!("{} {input}: no syntax errors", "ok:".green().bold());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_json_report_shape_is_stable_and_round_trips() {
        let report = CheckReport {
            schema: CHECK_SCHEMA,
            input: "d.ttl".to_string(),
            syntax: "turtle",
            ok: false,
            grammar_statements: 12,
            diagnostics: vec![diagnostic::from_turtle_error(
                &fluree_graph_turtle::TurtleError::parse(5, "unexpected"),
                "abc\ndef\n",
            )],
        };
        let v: serde_json::Value = serde_json::to_value(&report).unwrap();
        assert_eq!(v["schema"], CHECK_SCHEMA);
        assert_eq!(v["ok"], false);
        assert_eq!(v["grammar_statements"], 12);
        assert_eq!(v["syntax"], "turtle");
        assert_eq!(v["diagnostics"][0]["line"], 2);
        assert_eq!(v["diagnostics"][0]["column"], 2);
        assert_eq!(v["diagnostics"][0]["severity"], "error");
    }

    #[test]
    fn a_clean_report_carries_an_empty_diagnostic_list_not_a_missing_one() {
        // A consumer should be able to read `.diagnostics.length` without
        // first checking whether the key exists.
        let report = CheckReport {
            schema: CHECK_SCHEMA,
            input: "d.ttl".to_string(),
            syntax: "turtle",
            ok: true,
            grammar_statements: 3,
            diagnostics: Vec::new(),
        };
        let v: serde_json::Value = serde_json::to_value(&report).unwrap();
        assert_eq!(v["ok"], true);
        assert_eq!(v["diagnostics"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn table_is_the_default_format() {
        assert_eq!(CheckFormat::default(), CheckFormat::Table);
    }
}
