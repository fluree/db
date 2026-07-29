//! `fluree rdf convert` — read one syntax, write another.
//!
//! The whole verb is four moving parts, and three of them already existed:
//! the input plumbing `check` and `count` use, the parser, the writers, and a
//! driver that decides which writer and reports what happened. Nothing
//! materializes the document — the parser emits into the writer and bytes
//! leave as they are produced, which is what makes `convert big.ttl | head -5`
//! cost five statements instead of a full parse.
//!
//! # Exit codes
//!
//! Same contract as the other verbs. `0` converted (and also a closed
//! downstream pipe, which is a normal end to a `| head`), `1` the input
//! document did not parse, `2` the invocation or the destination was wrong —
//! no such file, unwritable path, a syntax with no writer, a conversion that
//! cannot represent the input.

use crate::cli::{BnodePolicyArg, RdfCommonArgs};
use crate::error::{CliError, CliResult};
use crate::rdf::profile::{ProfileReport, RunContext};
use crate::rdf::syntax::{split_compression, RdfSyntax};
use crate::rdf::writer::AnyWriter;
use crate::rdf::{self, destination, diagnostic, exit_document_invalid};
use colored::Colorize;
use fluree_graph_format::{BlankNodeLabels, PrefixMap, WriterConfig, WriterStats};
use fluree_graph_ir::{Phase, PhaseTimings};
use std::io::Write;
use std::path::Path;
use std::time::Duration;

/// The output syntax when neither `--to` nor an output extension says.
///
/// N-Quads, matching riot: Jena's `riot` documents N-Quads as the default
/// output for its streaming path, and N-Quads is the only syntax in the set
/// that holds any dataset without loss and needs no context, which is what
/// makes it the right thing to get when you did not say.
pub const DEFAULT_OUTPUT_SYNTAX: RdfSyntax = RdfSyntax::NQuads;

/// Everything `convert` takes beyond the shared input arguments.
pub struct ConvertArgs<'a> {
    /// Explicit output syntax.
    pub to: Option<RdfSyntax>,
    /// Output file, or `None` for stdout.
    pub output: Option<&'a Path>,
    /// Buffered, regrouped Turtle. Not implemented; see [`run`].
    pub pretty: bool,
    /// Blank-node labelling policy.
    pub bnode_policy: BnodePolicyArg,
    /// Prefixes to seed compaction with, as JSON or a path to JSON.
    pub prefixes: Option<&'a str>,
}

/// Run `fluree rdf convert`.
pub fn run(common: &RdfCommonArgs, args: &ConvertArgs<'_>, quiet: bool) -> CliResult<()> {
    let target = resolve_output_syntax(args.to, args.output)?;

    if args.pretty && target != RdfSyntax::Turtle {
        return Err(CliError::Usage(format!(
            "--pretty applies to turtle output; --to {target} has no pretty form"
        )));
    }
    if args.pretty {
        // Honest refusal rather than a silent no-op: `--pretty` names a
        // fidelity tier that costs O(document) memory, and a user who asked
        // for it and quietly got blocks-tier output would have no way to tell.
        return Err(CliError::Usage(format!(
            "--pretty is not implemented yet — turtle output is the streaming \"blocks\" \
             tier: consecutive same-subject runs fold with ';' and ',', but a subject that \
             recurs later is not regrouped\n  {} drop --pretty for streaming output",
            "help:".cyan().bold(),
        )));
    }

    let mut timings = PhaseTimings::start();
    let loaded = rdf::load(common, &mut timings)?;
    let prefixes = load_prefixes(args.prefixes)?;

    let config = WriterConfig::new()
        .with_blank_labels(BlankNodeLabels::from(args.bnode_policy))
        .with_prefixes(prefixes.clone());

    let destination::Destination { out, clock } = destination::open(args.output, target)?;
    let writer = AnyWriter::new(target, out, &config, &prefixes)?;

    let run = rdf::parse_into(&loaded.text, common.base.as_deref(), writer, &mut timings);
    let stats = run.sink.stats();

    // Flush before reporting anything, and through a handle that can return
    // the failure. A `BufWriter` that flushes on drop discards the error —
    // and that error is the one saying the output is truncated.
    let mut out = run.sink.into_inner();
    timings.enter(Phase::Write);
    let flushed = out
        .flush()
        .map_err(|e| CliError::Usage(format!("cannot write output: {e}")));
    timings.finish();

    let wall = timings.wall();
    // The writer's bytes went through the destination clock during the parse;
    // the flush above is the tail of the same phase.
    timings.set(
        Phase::Write,
        clock.elapsed() + timings.elapsed(Phase::Write),
    );
    attribute_serialize(&mut timings, &run.outcome);

    // A closed downstream is how `| head -5` ends, not a failure. Checked
    // before anything else, because every later report would be noise written
    // to the same dead pipe.
    if is_broken_pipe(&run.outcome, &run.finished, &flushed) {
        return Ok(());
    }
    flushed?;
    run.finished?;

    if let Some(err) = &run.outcome.error {
        report_parse_failure(&loaded, err, stats);
        rdf::report_run(common, "convert", &loaded, &run.outcome, &timings, wall)?;
        return Err(exit_document_invalid());
    }

    if let Some(format) = common.profile {
        emit_profile(common, &loaded, &run.outcome, &timings, wall, format)?;
    } else if common.time {
        rdf::count::print_timing(wall, stats.statements, loaded.text.len() as u64);
    }

    // A summary on the way to a file is useful; the same line beside piped
    // data is noise, so it is only printed when the data went somewhere else.
    if !quiet {
        if let Some(path) = args.output {
            eprintln!(
                "{} {} statements → {} ({target})",
                "✓".green(),
                stats.statements,
                path.display(),
            );
        }
    }
    Ok(())
}

/// Resolve the output syntax: `--to`, then the output file's extension, then
/// [`DEFAULT_OUTPUT_SYNTAX`].
///
/// Same precedence as the input resolver, minus the sniff — there is nothing
/// to sniff on output — and `--to` wins for the same reason: it is the only
/// way to override a wrong guess.
pub fn resolve_output_syntax(to: Option<RdfSyntax>, output: Option<&Path>) -> CliResult<RdfSyntax> {
    if let Some(syntax) = to {
        return Ok(syntax);
    }
    if let Some(path) = output {
        let (ext, _) = split_compression(path);
        if let Some(syntax) = ext.as_deref().and_then(RdfSyntax::from_extension) {
            return Ok(syntax);
        }
    }
    Ok(DEFAULT_OUTPUT_SYNTAX)
}

/// Read `--prefixes`: an inline JSON object, or a path to one.
///
/// A JSON-LD `@context` document works unchanged, which is the point — a user
/// who already has one should not have to transcribe it.
fn load_prefixes(source: Option<&str>) -> CliResult<PrefixMap> {
    let Some(source) = source else {
        return Ok(PrefixMap::new());
    };
    let text = if source.trim_start().starts_with('{') {
        source.to_string()
    } else {
        std::fs::read_to_string(source)
            .map_err(|e| CliError::Usage(format!("cannot read prefixes from '{source}': {e}")))?
    };
    let json: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| CliError::Usage(format!("--prefixes is not valid JSON: {e}")))?;
    // A whole JSON-LD document, or just its context, or a bare prefix map:
    // `from_context` drops `@`-prefixed keys, so the wrapper has to come off
    // here or a real context file would silently yield no prefixes at all.
    let map = json.get("@context").unwrap_or(&json);
    Ok(PrefixMap::from_context(map))
}

impl From<BnodePolicyArg> for BlankNodeLabels {
    fn from(arg: BnodePolicyArg) -> Self {
        match arg {
            BnodePolicyArg::Relabel => BlankNodeLabels::Relabel,
            BnodePolicyArg::Preserve => BlankNodeLabels::Preserve,
        }
    }
}

/// Split the sink estimate into serialize and write.
///
/// The writers take no timestamps and the review ruled out a per-emit clock,
/// so the only decomposition available is by subtraction: the sink estimate
/// covers formatting *and* the I/O it triggered, the destination clock covers
/// the I/O exactly, and the difference is the formatting.
///
/// Only done when the sink estimate resolved at all. Below the measurement
/// floor there is no total to subtract from, and inventing a serialize phase
/// out of an unresolved one would be the same error the estimator was
/// corrected for.
fn attribute_serialize(timings: &mut PhaseTimings, outcome: &rdf::ParseOutcome) {
    let Some(total) = outcome.sink.body else {
        return;
    };
    let write = timings.elapsed(Phase::Write);
    timings.set(Phase::Serialize, total.saturating_sub(write));
}

/// Whether the run ended because something downstream stopped reading.
///
/// `convert f.ttl | head -5` closes the pipe after five lines. Every layer
/// then reports `EPIPE`: the writer to the parser, the parser out of
/// `parse_into`, and the final flush. Any of them is the same event, and the
/// right response to all of them is to stop quietly with status 0 — riot's
/// behaviour, and what makes the tool usable in a shell at all.
fn is_broken_pipe(
    outcome: &rdf::ParseOutcome,
    finished: &CliResult<()>,
    flushed: &CliResult<()>,
) -> bool {
    let in_parse = matches!(
        &outcome.error,
        Some(fluree_graph_turtle::TurtleError::Sink(e)) if e.is_broken_pipe()
    );
    // `finished` and `flushed` have already been flattened into CliError, so
    // the io::ErrorKind is gone and the message is what is left to match on.
    let mentions_pipe = |r: &CliResult<()>| {
        r.as_ref()
            .err()
            .is_some_and(|e| e.to_string().contains("Broken pipe"))
    };
    in_parse || mentions_pipe(finished) || mentions_pipe(flushed)
}

fn report_parse_failure(
    loaded: &rdf::Loaded,
    err: &fluree_graph_turtle::TurtleError,
    stats: WriterStats,
) {
    let d = diagnostic::from_turtle_error(err, &loaded.text);
    let where_ = match (d.line, d.column) {
        (Some(line), Some(column)) => format!("{}:{line}:{column}", loaded.input.display()),
        _ => loaded.input.display(),
    };
    eprintln!("{} {where_}: {}", "error:".red().bold(), d.message);
    eprintln!(
        "  wrote {} statement(s) before the document stopped parsing — the output is a \
         prefix of the conversion, not the whole of it",
        stats.statements
    );
}

fn emit_profile(
    common: &RdfCommonArgs,
    loaded: &rdf::Loaded,
    outcome: &rdf::ParseOutcome,
    timings: &PhaseTimings,
    wall: Duration,
    format: crate::rdf::profile::ProfileFormat,
) -> CliResult<()> {
    let ctx = RunContext {
        verb: "convert",
        input: loaded.input.display(),
        syntax: loaded.resolved.syntax,
        syntax_source: loaded.resolved.source,
        compression: loaded.resolved.compression,
        bytes_on_wire: loaded.bytes_on_wire,
        bytes_decoded: loaded.text.len() as u64,
        sha256: (!common.no_hash).then(|| crate::rdf::profile::sha256_hex(&loaded.text)),
    };
    ProfileReport::build(&ctx, timings, wall, outcome.counts, outcome.sink).emit(format)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rdf::writer::is_writable;

    #[test]
    fn the_output_syntax_is_the_flag_then_the_extension_then_nquads() {
        let by_ext = |name: &str| resolve_output_syntax(None, Some(Path::new(name))).unwrap();

        // Nothing said: riot's default.
        assert_eq!(
            resolve_output_syntax(None, None).unwrap(),
            RdfSyntax::NQuads
        );
        assert_eq!(DEFAULT_OUTPUT_SYNTAX, RdfSyntax::NQuads);

        // The extension speaks when the flag does not.
        assert_eq!(by_ext("out.ttl"), RdfSyntax::Turtle);
        assert_eq!(by_ext("out.nt"), RdfSyntax::NTriples);
        assert_eq!(by_ext("out.trig"), RdfSyntax::TriG);
        assert_eq!(by_ext("out.jsonld"), RdfSyntax::JsonLd);
        // An unrecognized extension falls through to the default rather than
        // erroring: a default is a better answer than a lecture about naming.
        assert_eq!(by_ext("out.bin"), RdfSyntax::NQuads);

        // The flag wins over an extension that disagrees.
        assert_eq!(
            resolve_output_syntax(Some(RdfSyntax::Turtle), Some(Path::new("out.nt"))).unwrap(),
            RdfSyntax::Turtle
        );
    }

    #[test]
    fn every_default_and_extension_resolved_syntax_is_actually_writable() {
        // A default that resolved to a syntax with no writer would turn a
        // no-flag invocation into an error about a flag the user never used.
        assert!(is_writable(DEFAULT_OUTPUT_SYNTAX));
        for name in ["out.ttl", "out.nt", "out.nq", "out.trig", "out.jsonld"] {
            let syntax = resolve_output_syntax(None, Some(Path::new(name))).unwrap();
            assert!(
                is_writable(syntax),
                "{name} resolved to unwritable {syntax}"
            );
        }
    }

    #[test]
    fn prefixes_accept_inline_json_and_a_context_document() {
        let inline = load_prefixes(Some(r#"{"ex": "http://example.org/"}"#)).unwrap();
        assert_eq!(
            inline.compact("http://example.org/a").as_deref(),
            Some("ex:a")
        );

        // A JSON-LD context document, unchanged — the shape a user already has.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ctx.json");
        std::fs::write(&path, r#"{"@context": {"ex": "http://example.org/"}}"#).unwrap();
        let from_file = load_prefixes(Some(path.to_str().unwrap())).unwrap();
        assert_eq!(
            from_file.compact("http://example.org/a").as_deref(),
            Some("ex:a")
        );

        // Absent means empty, not an error.
        assert!(load_prefixes(None).unwrap().is_empty());
    }

    #[test]
    fn malformed_prefixes_are_a_usage_error_naming_the_problem() {
        let err = load_prefixes(Some("{not json")).unwrap_err();
        assert!(err.to_string().contains("not valid JSON"), "{err}");
        let err = load_prefixes(Some("/nonexistent/ctx.json")).unwrap_err();
        assert!(err.to_string().contains("cannot read prefixes"), "{err}");
    }

    #[test]
    fn the_bnode_policy_flag_maps_onto_the_writer_policy() {
        assert_eq!(
            BlankNodeLabels::from(BnodePolicyArg::Relabel),
            BlankNodeLabels::Relabel
        );
        assert_eq!(
            BlankNodeLabels::from(BnodePolicyArg::Preserve),
            BlankNodeLabels::Preserve
        );
        // Relabel is the default on both sides, so the flag's default and the
        // writer's cannot drift apart unnoticed.
        assert_eq!(
            BlankNodeLabels::from(BnodePolicyArg::default()),
            BlankNodeLabels::default()
        );
    }

    #[test]
    fn serialize_is_derived_only_when_the_sink_estimate_resolved() {
        use fluree_graph_ir::{SinkCounts, SinkTiming};

        let outcome = |body| rdf::ParseOutcome {
            counts: SinkCounts::default(),
            sink: SinkTiming {
                body,
                finish: Duration::ZERO,
                artifact: Duration::from_micros(1),
                calls: 100,
                sampled_calls: 8,
                sampled_statements: 2,
                clock_reads: 16,
                clock_pair: Duration::from_nanos(20),
                relative_std_error: None,
            },
            error: None,
        };

        // Resolvable: serialize is the sink total minus the measured write.
        let mut timings = PhaseTimings::start();
        timings.set(Phase::Write, Duration::from_millis(10));
        attribute_serialize(&mut timings, &outcome(Some(Duration::from_millis(30))));
        assert_eq!(timings.elapsed(Phase::Serialize), Duration::from_millis(20));

        // Unresolvable: no serialize phase invented from a number that is not
        // there. That is the estimator error this seam was corrected for.
        let mut timings = PhaseTimings::start();
        timings.set(Phase::Write, Duration::from_millis(10));
        attribute_serialize(&mut timings, &outcome(None));
        assert_eq!(timings.elapsed(Phase::Serialize), Duration::ZERO);

        // Write exceeding the estimate saturates rather than wrapping.
        let mut timings = PhaseTimings::start();
        timings.set(Phase::Write, Duration::from_millis(50));
        attribute_serialize(&mut timings, &outcome(Some(Duration::from_millis(30))));
        assert_eq!(timings.elapsed(Phase::Serialize), Duration::ZERO);
    }
}
