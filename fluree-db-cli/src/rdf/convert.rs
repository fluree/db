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
use crate::rdf::parallel::ParallelConfig;
use crate::rdf::profile::{ProfileReport, RunContext};
use crate::rdf::syntax::{split_compression, RdfSyntax};
use crate::rdf::writer::{is_writable, AnyWriter};
use crate::rdf::{self, destination, diagnostic, exit_document_invalid};
use colored::Colorize;
use fluree_graph_format::{BlankNodeLabels, PrefixMap, WriterConfig, WriterStats};
use fluree_graph_ir::{GraphSink, Phase, PhaseTimings, SinkError};
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
    /// Parse threads. `0` means "as many as this host has"; `1` is the serial
    /// path exactly, so the flag is never a correctness decision.
    pub parallelism: usize,
    /// Skip statements that do not parse rather than stopping at the first.
    pub continue_on_error: bool,
}

/// Run `fluree rdf convert`.
pub fn run(common: &RdfCommonArgs, args: &ConvertArgs<'_>, quiet: bool) -> CliResult<()> {
    let target = resolve_output_syntax(args.to, args.output)?;

    if args.pretty && target != RdfSyntax::Turtle {
        // Blame the syntax the way it was actually chosen. "--to nquads has no
        // pretty form" reads as a lie to someone who never passed --to.
        return Err(CliError::Usage(format!(
            "--pretty applies to turtle output; {} has no pretty form\n  {} pass \
             --to turtle, or drop --pretty",
            describe_target(target, args.to, args.output),
            "help:".cyan().bold(),
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

    // Everything that can be refused without reading the input is refused
    // here, BEFORE the destination is opened. `File::create` truncates, so a
    // refusal after it destroys whatever `-o` pointed at — and a run that was
    // never going to produce output should not be the thing that empties a
    // file. What cannot move is a failure only the parse can discover; that
    // one does leave a partial file, and the error says so.
    if !is_writable(target) {
        return Err(no_writer_error(target, args.to, args.output));
    }

    let mut timings = PhaseTimings::start();
    let loaded = rdf::load(common, &mut timings)?;
    let prefixes = load_prefixes(args.prefixes)?;

    let config = WriterConfig::new()
        .with_blank_labels(BlankNodeLabels::from(args.bnode_policy))
        .with_prefixes(prefixes.clone())
        // Recovery needs `abort_statement` to be a true rollback: the parser
        // emits during descent, so a bad statement has usually written part of
        // itself by the time it is rejected. Without buffering, "skipped"
        // would still leave fragments in the output.
        .with_statement_buffering(args.continue_on_error);

    let destination::Destination { out, clock } = destination::open(args.output, target)?;
    let writer = AnyWriter::new(target, out, &config, &prefixes)?;

    // Parallel where it is byte-identical to serial, serial otherwise. The
    // decision is reported under --profile rather than left implicit, because
    // "why is this not using my cores" is otherwise unanswerable from outside.
    //
    // Recovery is serial: resync needs to see the document as one sequence of
    // statements, and a chunk boundary is not a place a skipped statement can
    // be reasoned about.
    let plan = if args.continue_on_error {
        ParallelPlan::serial("--continue-on-error resyncs over the whole document")
    } else {
        ParallelPlan::decide(
            args.parallelism,
            target,
            loaded.text.len(),
            config.blank_labels,
        )
    };
    if args.continue_on_error {
        return run_recovering(
            common, args, &loaded, writer, &clock, target, quiet, timings,
        );
    }

    // Chunk before committing to the parallel path. A mid-file directive makes
    // a document unchunkable — only the first chunk would carry the
    // redefinition — but the document itself is perfectly legal Turtle and
    // must still convert, so this falls back to serial rather than refusing.
    // Plan §1.4 specifies the fallback; aborting was the wrong reading.
    let mut plan = plan;
    let chunked = plan.workers.and_then(|workers| {
        // Its own lane. This is a whole-document single-threaded pass that runs
        // before the first worker starts, so it is the parallel path's Amdahl
        // term — and while it had no lane it landed in `unattributed_ns`, where
        // it went unnoticed at 43% of the wall.
        timings.enter(Phase::Chunk);
        let split = fluree_graph_turtle::splitter::chunk_in_memory(
            &loaded.text,
            ParallelConfig::for_input(workers, loaded.text.len()).chunk_bytes,
        );
        timings.enter(Phase::Parse);
        match split {
            Ok(split) => Some((workers, split)),
            Err(e) => {
                plan = ParallelPlan::serial(match e {
                    fluree_graph_turtle::splitter::SplitError::PrefixAfterData { .. } => {
                        "a directive after the header makes the input unchunkable"
                    }
                    _ => "the input could not be split into chunks",
                });
                if !quiet {
                    eprintln!(
                        "{} {} — converting serially",
                        "note:".cyan().bold(),
                        plan.reason
                    );
                }
                None
            }
        }
    });

    if let Some((workers, (prefix_block, ranges))) = chunked {
        // Workers write their own bytes; see `parallel` for the label scheme
        // that makes that sound without a shared relabeller.
        return run_parallel(
            common,
            args,
            &loaded,
            writer,
            &clock,
            target,
            &config,
            workers,
            quiet,
            timings,
            &ranges,
            &prefix_block,
        );
    }

    let run = rdf::parse_into(
        &loaded.text,
        loaded.resolved.syntax,
        common.base.as_deref(),
        writer,
        rdf::verb_options(common.nocheck),
        &mut timings,
    );
    let stats = run.sink.stats();

    // Flush before reporting anything, and through a handle that can return
    // the failure. A `BufWriter` that flushes on drop discards the error —
    // and that error is the one saying the output is truncated.
    let mut out = run.sink.into_inner();
    timings.enter(Phase::Write);
    // Kept as an `io::Result` until after the broken-pipe check below, so the
    // `ErrorKind` is still there to check.
    let flushed = out.flush();
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

    // The writer's FIRST refusal, before the latch. The writers deliberately
    // latch — a sink that failed once keeps failing rather than pretending —
    // but the latched message ("this writer already refused an event") carries
    // no cause. The cause is in the error the parse loop got, and reporting
    // `finished` first threw it away in favour of the placeholder.
    if let Some(fluree_graph_turtle::TurtleError::Sink(cause)) = &run.outcome.error {
        return Err(refusal_error(cause));
    }
    flushed.map_err(|e| CliError::Usage(format!("cannot write output: {e}")))?;
    run.finished
        .map_err(|e| CliError::Usage(format!("sink error: {e}")))?;

    if let Some(err) = &run.outcome.error {
        report_parse_failure(&loaded, err, stats);
        rdf::report_run(common, "convert", &loaded, &run.outcome, &timings, wall)?;
        return Err(exit_document_invalid());
    }

    if let Some(format) = common.profile {
        emit_profile(
            common,
            &loaded,
            &run.outcome,
            &timings,
            wall,
            format,
            plan,
            None,
        )?;
    } else if common.time {
        rdf::count::print_timing(wall, stats.statements, loaded.text.len() as u64);
    }

    // A summary on the way to a file is useful; the same line beside piped
    // data is noise. And under `--profile=json` stderr is not a place for
    // prose at all — `2> run.json` is the bench lane's idiom, and one ✓ line
    // ahead of the document makes the file unparseable.
    let stderr_is_a_document = common.profile == Some(crate::rdf::profile::ProfileFormat::Json);
    if !quiet && !stderr_is_a_document {
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

/// The parallel path: workers write bytes, the driver concatenates in order.
#[allow(clippy::too_many_arguments)]
fn run_parallel(
    common: &RdfCommonArgs,
    args: &ConvertArgs<'_>,
    loaded: &rdf::Loaded,
    writer: AnyWriter<destination::Out>,
    clock: &crate::rdf::writer::WriteClock,
    target: RdfSyntax,
    writer_config: &WriterConfig,
    workers: usize,
    quiet: bool,
    mut timings: PhaseTimings,
    ranges: &[std::ops::Range<usize>],
    prefix_block: &str,
) -> CliResult<()> {
    // The destination was opened through a writer that the parallel path does
    // not use — the workers each build their own. Take the sink back out.
    let mut out = writer.into_inner();

    let config = ParallelConfig {
        workers,
        ..ParallelConfig::for_input(workers, loaded.text.len())
    };

    timings.enter(Phase::Parse);
    let produced = crate::rdf::parallel::convert_parallel_bytes(
        &loaded.text,
        common.base.as_deref(),
        &mut out,
        target,
        writer_config,
        config,
        ranges,
        prefix_block,
    );
    timings.finish();

    let outcome = match produced {
        Ok(o) => o,
        // A closed downstream is how `| head -5` ends. The failure carries its
        // `io::ErrorKind` precisely so this check does not have to read a
        // localized message.
        Err(crate::rdf::parallel::ParallelFailure::Write(e))
            if e.kind() == std::io::ErrorKind::BrokenPipe =>
        {
            return Ok(())
        }
        Err(e) => return Err(CliError::Usage(e.to_string())),
    };

    timings.enter(Phase::Write);
    let flushed = out.flush();
    timings.finish();

    let wall = timings.wall();
    timings.set(
        Phase::Write,
        clock.elapsed() + timings.elapsed(Phase::Write),
    );
    timings.set(
        Phase::Workers,
        Duration::from_nanos(outcome.worker_parse_nanos as u64),
    );
    timings.set(
        Phase::Reassembly,
        Duration::from_nanos(outcome.reassembly_wait_nanos as u64),
    );

    if flushed
        .as_ref()
        .err()
        .is_some_and(|e| e.kind() == std::io::ErrorKind::BrokenPipe)
    {
        return Ok(());
    }
    flushed.map_err(|e| CliError::Usage(format!("cannot write output: {e}")))?;

    if let Some(err) = &outcome.error {
        let d = diagnostic::from_turtle_error(err, &loaded.text);
        eprintln!(
            "{} {}: {}",
            "error:".red().bold(),
            loaded.input.display(),
            d.message
        );
        eprintln!(
            "  wrote {} statement(s) before the document stopped parsing — the output is a \
             prefix of the conversion, not the whole of it",
            outcome.statements
        );
        return Err(exit_document_invalid());
    }

    if let Some(format) = common.profile {
        let empty = rdf::ParseOutcome {
            counts: fluree_graph_ir::SinkCounts {
                triples: outcome.statements,
                ..fluree_graph_ir::SinkCounts::default()
            },
            sink: unresolved_sink_timing(),
            error: None,
        };
        emit_profile(
            common,
            loaded,
            &empty,
            &timings,
            wall,
            format,
            ParallelPlan {
                workers: Some(workers),
                reason: "parallel",
            },
            None,
        )?;
    } else if common.time {
        rdf::count::print_timing(wall, outcome.statements, loaded.text.len() as u64);
    }

    if !quiet && common.profile != Some(crate::rdf::profile::ProfileFormat::Json) {
        if let Some(path) = args.output {
            eprintln!(
                "{} {} statements → {} ({target}, {workers} workers, {} chunks)",
                "✓".green(),
                outcome.statements,
                path.display(),
                outcome.chunks,
            );
        }
    }
    Ok(())
}

/// The parallel path has no single sink to sample, so it reports no sink
/// estimate rather than a fabricated one — each worker has its own writer and
/// there is no one instrument that saw them all.
fn unresolved_sink_timing() -> fluree_graph_ir::SinkTiming {
    fluree_graph_ir::SinkTiming {
        body: None,
        finish: Duration::ZERO,
        artifact: Duration::ZERO,
        calls: 0,
        sampled_calls: 0,
        sampled_statements: 0,
        clock_reads: 0,
        clock_pair: Duration::ZERO,
        relative_std_error: None,
    }
}

/// The `--continue-on-error` path: parse with resync, report every skip, and
/// exit 1 if anything was skipped.
///
/// Serial by construction and separate from the main driver because almost
/// every step differs — the parse loop, the exit code, and what "success"
/// means. Folding it into `run` with three `if recovering` branches would make
/// both harder to read than either is apart.
#[allow(clippy::too_many_arguments)]
fn run_recovering(
    common: &RdfCommonArgs,
    args: &ConvertArgs<'_>,
    loaded: &rdf::Loaded,
    writer: AnyWriter<destination::Out>,
    clock: &crate::rdf::writer::WriteClock,
    target: RdfSyntax,
    quiet: bool,
    mut timings: PhaseTimings,
) -> CliResult<()> {
    let mut sink = crate::rdf::recover::PrefixRecorder::new(writer);

    timings.enter(Phase::Parse);
    let recovery =
        crate::rdf::recover::parse_recovering(&loaded.text, common.base.as_deref(), &mut sink);
    timings.finish();
    let recovery = recovery?;

    let mut writer = sink.into_inner();
    let finished = GraphSink::finish(&mut writer);
    let stats = writer.stats();
    let mut out = writer.into_inner();

    timings.enter(Phase::Write);
    let flushed = out.flush();
    timings.finish();

    let wall = timings.wall();
    timings.set(
        Phase::Write,
        clock.elapsed() + timings.elapsed(Phase::Write),
    );

    if flushed
        .as_ref()
        .err()
        .is_some_and(|e| e.kind() == std::io::ErrorKind::BrokenPipe)
        || finished
            .as_ref()
            .err()
            .is_some_and(SinkError::is_broken_pipe)
    {
        return Ok(());
    }
    flushed.map_err(|e| CliError::Usage(format!("cannot write output: {e}")))?;
    finished.map_err(|e| CliError::Usage(format!("sink error: {e}")))?;

    // Every skip, in document order, before the summary — unless stderr is
    // carrying a JSON document, in which case the count travels inside it.
    let human_stderr = common.profile != Some(crate::rdf::profile::ProfileFormat::Json);
    for d in recovery.skipped.iter().filter(|_| human_stderr) {
        let where_ = match (d.line, d.column) {
            (Some(line), Some(column)) => format!("{}:{line}:{column}", loaded.input.display()),
            _ => loaded.input.display(),
        };
        eprintln!("{} {where_}: {}", "skipped:".yellow().bold(), d.message);
    }

    // Recovery is exactly when profiling matters — resync re-parses from each
    // error, so the cost of a dirty document is the thing a user most wants
    // attributed. Emitting the diagnostics instead of the profile silently
    // dropped a flag the user passed.
    if let Some(format) = common.profile {
        let reported = rdf::ParseOutcome {
            counts: fluree_graph_ir::SinkCounts {
                triples: stats.statements,
                ..fluree_graph_ir::SinkCounts::default()
            },
            sink: unresolved_sink_timing(),
            error: None,
        };
        emit_profile(
            common,
            loaded,
            &reported,
            &timings,
            wall,
            format,
            ParallelPlan::serial("--continue-on-error resyncs over the whole document"),
            Some(recovery.skipped.len() as u64),
        )?;
    } else if common.time {
        rdf::count::print_timing(wall, stats.statements, loaded.text.len() as u64);
    }

    if recovery.is_clean() {
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
        return Ok(());
    }

    // riot semantics: skipping is not success. The summary goes to stderr even
    // under --quiet, because the one thing a script must not do is read a
    // partial conversion as a whole one.
    if common.profile == Some(crate::rdf::profile::ProfileFormat::Json) {
        // stderr is carrying a JSON document; the skip count is in the exit
        // code and the diagnostics already went out before it.
        return Err(exit_document_invalid());
    }
    eprintln!(
        "{} {} statement(s) skipped, {} written → {}",
        "warning:".yellow().bold(),
        recovery.skipped.len(),
        stats.statements,
        args.output
            .map_or_else(|| "stdout".to_string(), |p| p.display().to_string()),
    );
    Err(exit_document_invalid())
}

/// Whether this conversion runs across threads, and why not when it does not.
///
/// Reported rather than silent: a user who passed `--parallelism 8` and got
/// one core has no way to find out why from the outside.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParallelPlan {
    /// Worker count, or `None` for the serial path.
    pub workers: Option<usize>,
    /// Why the serial path was chosen, for the profile report.
    pub reason: &'static str,
}

impl ParallelPlan {
    /// Below this, threads cost more than they save — the whole document
    /// parses in less time than it takes to start a pool.
    const MIN_PARALLEL_BYTES: usize = 4 * 1024 * 1024;

    /// The serial path, for a named reason.
    pub fn serial(reason: &'static str) -> Self {
        Self {
            workers: None,
            reason,
        }
    }

    /// Decide, from the flag, the output syntax, the input size and the label
    /// policy.
    ///
    /// The policy is a parameter rather than a check at the call site so a
    /// second call site cannot be added that forgets it.
    pub fn decide(
        parallelism: usize,
        target: RdfSyntax,
        input_len: usize,
        labels: BlankNodeLabels,
    ) -> Self {
        let requested = match parallelism {
            // 0 is the global flag's "auto".
            0 => std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get),
            n => n,
        };
        if requested <= 1 {
            return Self {
                workers: None,
                reason: "one worker requested",
            };
        }
        // Ahead of the capability and size checks, because this one is about
        // correctness rather than benefit. The parallel path renames every
        // blank node into the coordination-free scheme so workers need no
        // shared relabeller, which is the opposite of preserving labels: the
        // user's `_:named` came out as `_:unamed`, byte-identical to relabel
        // and silently ignoring the flag. Worse, the writer's refusal to
        // preserve a label inside its own reserved namespace disappeared too,
        // because the renamed label no longer collides — a run that must exit
        // 2 exited 0 with labels the user did not write.
        //
        // Serial delivers the fidelity the flag asks for, so this downgrades
        // rather than refusing, exactly as a mid-file directive does. The cost
        // is speed, not correctness.
        if labels == BlankNodeLabels::Preserve {
            return Self {
                workers: None,
                reason: "--bnode-policy preserve requires serial label fidelity",
            };
        }
        if !crate::rdf::parallel::can_run_parallel(target) {
            return Self {
                workers: None,
                // Not a limitation of the writer: splitting the input changes
                // the bytes for a syntax that folds across statements.
                reason: "output syntax is not line-based, so chunking would change the bytes",
            };
        }
        if input_len < Self::MIN_PARALLEL_BYTES {
            return Self {
                workers: None,
                reason: "input is smaller than the parallel threshold",
            };
        }
        Self {
            workers: Some(requested),
            reason: "parallel",
        }
    }
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
///
/// Every namespace is checked for being an absolute IRI before it can reach a
/// writer. Without that, `--prefixes '{"ok":"not an iri"}'` produced
/// `@prefix ok: <not an iri> .` and exited 0 — a document this
/// tool's own reader rejects, written by this tool, reported as a success.
/// A converter that emits something it cannot read back has failed at the one
/// thing it is for.
fn load_prefixes(source: Option<&str>) -> CliResult<PrefixMap> {
    let Some(source) = source else {
        return Ok(PrefixMap::new());
    };
    let trimmed = source.trim_start();
    let text = if trimmed.starts_with('{') || trimmed.starts_with('[') {
        source.to_string()
    } else {
        std::fs::read_to_string(source).map_err(|e| {
            // Valid JSON that is not a path — `42`, `"x"`, `null` — is an
            // argument, not a missing file, and saying "No such file" about it
            // sends the reader looking in the wrong place entirely.
            if serde_json::from_str::<serde_json::Value>(source).is_ok() {
                return CliError::Usage(format!(
                    "--prefixes must be a JSON object mapping prefix to namespace IRI, or a \
                     JSON-LD @context, or a path to one; got {}",
                    json_shape(&serde_json::from_str(source).unwrap_or(serde_json::Value::Null)),
                ));
            }
            CliError::Usage(format!("cannot read prefixes from '{source}': {e}"))
        })?
    };
    let json: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| CliError::Usage(format!("--prefixes is not valid JSON: {e}")))?;

    // A whole JSON-LD document, or just its context, or a bare prefix map:
    // `from_context` drops `@`-prefixed keys, so the wrapper has to come off
    // here or a real context file would silently yield no prefixes at all.
    let map = json.get("@context").unwrap_or(&json);
    let Some(entries) = map.as_object() else {
        // Valid JSON, wrong shape. Reported as such rather than let through to
        // produce an empty prefix map, and distinct from the file-not-found
        // message it used to collide with.
        return Err(CliError::Usage(format!(
            "--prefixes must be a JSON object mapping prefix to namespace IRI, or a \
             JSON-LD @context; got {}\n  {} for example: \
             --prefixes '{{\"ex\": \"http://example.org/\"}}'",
            json_shape(map),
            "help:".cyan().bold(),
        )));
    };

    for (prefix, value) in entries {
        if prefix.starts_with('@') {
            continue; // a context term, not a prefix — `from_context` drops it
        }
        let Some(iri) = value.as_str() else {
            continue; // likewise: a term definition object, not a namespace
        };
        if !fluree_vocab::iri::is_absolute_iri(iri) {
            return Err(CliError::Usage(format!(
                "--prefixes: namespace for '{prefix}' is not an absolute IRI: '{iri}'\n  \
                 {} a namespace needs a scheme, like \"http://example.org/\" — a relative \
                 or malformed one produces a document no RDF reader accepts",
                "help:".cyan().bold(),
            )));
        }
    }

    Ok(PrefixMap::from_context(map))
}

/// Namespace the writers reserve for blank nodes they mint under
/// `--bnode-policy preserve`. A refusal mentioning it is a label collision,
/// and has a remedy this CLI can name.
const RESERVED_BNODE_NAMESPACE: &str = "fdbw-";

/// Render a writer's refusal with its cause and, where one exists, a remedy in
/// this CLI's vocabulary.
fn refusal_error(cause: &fluree_graph_ir::SinkError) -> CliError {
    let mut message = format!("the output is incomplete — the writer refused an event: {cause}");
    if cause.to_string().contains(RESERVED_BNODE_NAMESPACE) {
        message.push_str(&format!(
            "\n  {} convert with --bnode-policy relabel, which renames every blank node \
             and cannot collide",
            "help:".cyan().bold(),
        ));
    }
    CliError::Usage(message)
}

/// Refuse an unwritable target, blaming it the way it was chosen.
fn no_writer_error(target: RdfSyntax, to: Option<RdfSyntax>, output: Option<&Path>) -> CliError {
    CliError::Usage(format!(
        "cannot write {target} yet — {}\n  {} writable today: turtle, ntriples, nquads, \
         trig, jsonld ({})",
        match target {
            RdfSyntax::RdfXml => "the RDF/XML writer lands with the XML family",
            RdfSyntax::RdfJson => "RDF/JSON lands with the XML family",
            RdfSyntax::Jelly => "Jelly lands last in the format set",
            _ => "no writer exists for it",
        },
        "help:".cyan().bold(),
        describe_target(target, to, output),
    ))
}

/// How the output syntax came to be what it is, for an error that blames it.
fn describe_target(target: RdfSyntax, to: Option<RdfSyntax>, output: Option<&Path>) -> String {
    if to.is_some() {
        return format!("--to {target}");
    }
    if let Some(path) = output {
        let (ext, _) = split_compression(path);
        if ext.as_deref().and_then(RdfSyntax::from_extension).is_some() {
            return format!("{target}, from the '{}' extension", path.display());
        }
    }
    format!("no --to given, so the default {target}")
}

/// Name a JSON value's shape, for an error that has to explain what arrived.
fn json_shape(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "a boolean",
        serde_json::Value::Number(_) => "a number",
        serde_json::Value::String(_) => "a string",
        serde_json::Value::Array(_) => "an array",
        serde_json::Value::Object(_) => "an object",
    }
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
    finished: &Result<(), fluree_graph_ir::SinkError>,
    flushed: &std::io::Result<()>,
) -> bool {
    // Every check is on `ErrorKind`, never on the message. `strerror` is
    // localized by glibc under `LC_MESSAGES`, so a substring match for
    // "Broken pipe" is a check that passes for whoever wrote it and fails for
    // everyone running in another language — and the failure mode is a
    // spurious error at the end of an ordinary `| head`.
    let in_parse = matches!(
        &outcome.error,
        Some(fluree_graph_turtle::TurtleError::Sink(e)) if e.is_broken_pipe()
    );
    let at_finish = finished
        .as_ref()
        .err()
        .is_some_and(SinkError::is_broken_pipe);
    let at_flush = flushed
        .as_ref()
        .err()
        .is_some_and(|e| e.kind() == std::io::ErrorKind::BrokenPipe);
    in_parse || at_finish || at_flush
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

#[allow(clippy::too_many_arguments)]
fn emit_profile(
    common: &RdfCommonArgs,
    loaded: &rdf::Loaded,
    outcome: &rdf::ParseOutcome,
    timings: &PhaseTimings,
    wall: Duration,
    format: crate::rdf::profile::ProfileFormat,
    plan: ParallelPlan,
    skipped: Option<u64>,
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
        validate: !common.nocheck,
        skipped_statements: skipped,
        threads_used: plan.workers.unwrap_or(1),
        parallel_reason: plan.reason,
    };
    ProfileReport::build(&ctx, timings, wall, outcome.counts, outcome.sink).emit(format)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn preserving_labels_downgrades_to_serial_whatever_else_allows_parallel() {
        // Everything else says parallel: eight workers, a line-based syntax, an
        // input well over the threshold.
        let big = 64 * 1024 * 1024;
        let relabel = ParallelPlan::decide(8, RdfSyntax::NTriples, big, BlankNodeLabels::Relabel);
        assert_eq!(relabel.workers, Some(8));

        let preserve = ParallelPlan::decide(8, RdfSyntax::NTriples, big, BlankNodeLabels::Preserve);
        assert_eq!(preserve.workers, None);
        assert!(
            preserve.reason.contains("preserve"),
            "the profile has to say which flag cost the parallelism: {}",
            preserve.reason
        );
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
