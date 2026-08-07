//! `fluree count` — how many statements are in this document.
//!
//! Counting is a parse with the results thrown away, which makes it the
//! cheapest honest measurement of parser throughput there is: no sink work,
//! no serialization, no I/O on the way out. That is why it is also the verb
//! the benchmark lane leans on.
//!
//! # What is counted
//!
//! Triples, as RDF defines them. Collections are counted as the
//! `rdf:first`/`rdf:rest` spine they denote rather than as the flattened list
//! items Fluree's ingest path stores (see
//! [`crate::rdf::parse_document`]), so the number is comparable with riot,
//! serdi, and rapper on the same file.
//!
//! Quads are reported as a separate line once there is a reader that can
//! produce them; the sink already accepts and counts them, so the line appears
//! the day N-Quads and TriG land, with no change here.

use crate::cli::RdfCommonArgs;
use crate::error::CliResult;
use crate::rdf::diagnostic;
use crate::rdf::{self, exit_document_invalid};
use colored::Colorize;
use fluree_graph_ir::PhaseTimings;
use std::time::Duration;

/// Run `fluree count`.
pub fn run(common: &RdfCommonArgs, quiet: bool) -> CliResult<()> {
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
    let counts = outcome.counts;

    // A count of a document that did not parse is a count of a prefix of it,
    // so say so and exit 1 rather than printing a number that looks whole.
    if let Some(err) = &outcome.error {
        let d = diagnostic::from_turtle_error(err, &loaded.text);
        let where_ = match (d.line, d.column) {
            (Some(line), Some(column)) => format!("{}:{line}:{column}", loaded.input.display()),
            _ => loaded.input.display(),
        };
        eprintln!("{} {where_}: {}", "error:".red().bold(), d.message);
        eprintln!(
            "  counted {} triple(s) before the document stopped parsing",
            counts.emitted()
        );
        // Profile the failed run too — the reason a broken corpus was slow is
        // as worth having as the reason a good one was.
        rdf::report_run(common, "count", &loaded, &outcome, &timings, wall)?;
        return Err(exit_document_invalid());
    }

    if quiet {
        // One number, for `$(fluree count -q f.ttl)`.
        println!("{}", counts.emitted());
    } else {
        println!("triples: {}", counts.triples);
        if counts.quads > 0 {
            println!("quads: {}", counts.quads);
        }
        if counts.list_items > 0 {
            println!("list items: {}", counts.list_items);
        }
        if counts.reified > 0 {
            println!("reified: {}", counts.reified);
        }
        println!(
            "terms: {} (iri {}, blank {}, literal {})",
            counts.terms(),
            counts.terms_iri,
            counts.terms_blank,
            counts.terms_literal
        );
        println!("grammar statements: {}", counts.statements);
        println!("prefixes: {}", counts.prefixes);
    }

    rdf::report_run(common, "count", &loaded, &outcome, &timings, wall)
}

/// The one-line `--time` footer: elapsed, triple rate, byte rate.
///
/// Shared with `check`, and on stderr for the same reason the profile is —
/// `fluree count f.ttl --time | …` must keep piping the count.
pub fn print_timing(wall: Duration, triples: u64, bytes: u64) {
    let secs = wall.as_secs_f64();
    let (rate, mib) = if secs > 0.0 {
        (
            triples as f64 / secs,
            bytes as f64 / (1024.0 * 1024.0) / secs,
        )
    } else {
        (0.0, 0.0)
    };
    eprintln!(
        "  {} triples in {} ({} triples/s, {mib:.1} MiB/s)",
        format_count(triples),
        format_duration(wall),
        format_count(rate as u64),
    );
}

/// Same thresholds as `commands/query.rs::format_duration`.
fn format_duration(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs >= 1.0 {
        format!("{secs:.2}s")
    } else if secs >= 0.001 {
        format!("{:.1}ms", secs * 1_000.0)
    } else {
        format!("{}μs", d.as_micros())
    }
}

/// Comma-grouped thousands, matching `commands/query.rs::format_count`.
fn format_count(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            result.push(',');
        }
        result.push(ch);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_are_grouped_the_way_the_rest_of_the_cli_groups_them() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(999), "999");
        assert_eq!(format_count(1_000), "1,000");
        assert_eq!(format_count(27_000_000), "27,000,000");
    }

    #[test]
    fn durations_pick_the_same_units_as_the_query_footer() {
        assert_eq!(format_duration(Duration::from_millis(1_500)), "1.50s");
        assert_eq!(format_duration(Duration::from_micros(1_500)), "1.5ms");
        assert_eq!(format_duration(Duration::from_nanos(1_500)), "1μs");
    }

    #[test]
    fn a_zero_length_run_does_not_divide_by_zero() {
        // Nothing to assert but the absence of a panic or a NaN in output.
        print_timing(Duration::ZERO, 100, 100);
    }
}
