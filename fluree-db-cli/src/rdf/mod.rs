//! `fluree rdf` — RDF syntax tooling that does not touch a ledger.
//!
//! These verbs work on files, not databases. There is no `.fluree/` to find,
//! no ledger to open, no connection to build: `fluree rdf check dump.ttl`
//! reads a file and reports on it. `commands/validate.rs` set the precedent
//! for a ledger-free file mode, and these go further — validate still spins up
//! an ephemeral in-memory ledger to run SHACL through, and nothing here needs
//! even that.
//!
//! Three verbs today. `check` and `count` are complete; `convert` is a named
//! stub, because the surface is settled and the writers are not.

pub mod check;
pub mod convert;
pub mod count;
pub mod destination;
pub mod diagnostic;
pub mod input;
pub mod profile;
pub mod syntax;
pub mod writer;

use crate::cli::{RdfAction, RdfCommonArgs};
use crate::error::{CliError, CliResult, EXIT_ERROR};
use fluree_graph_ir::{
    Datatype, GraphSink, LiteralValue, Phase, PhaseTimings, SinkCounts, SinkResult, SinkTiming,
    TermId, TimingSink,
};
use fluree_graph_turtle::{ParserOptions, TurtleError};
use input::RdfInput;
use std::time::Duration;
use syntax::Resolved;

/// Dispatch an `rdf` subcommand.
pub fn run(action: &RdfAction, quiet: bool) -> CliResult<()> {
    let common = match action {
        RdfAction::Convert { common, .. }
        | RdfAction::Check { common, .. }
        | RdfAction::Count { common } => common,
    };
    reject_profile_space_form(common)?;

    match action {
        RdfAction::Convert {
            common,
            to,
            output,
            pretty,
            bnode_policy,
            prefixes,
        } => convert::run(
            common,
            &convert::ConvertArgs {
                to: *to,
                output: output.as_deref(),
                pretty: *pretty,
                bnode_policy: *bnode_policy,
                prefixes: prefixes.as_deref(),
            },
            quiet,
        ),
        RdfAction::Check { common, format } => check::run(common, *format, quiet),
        RdfAction::Count { common } => count::run(common, quiet),
    }
}

/// Catch `--profile json` (space) and say what was meant.
///
/// `--profile` takes an optional value, so clap can only accept it attached:
/// `--profile=json`. Written with a space, clap gives `--profile` its default
/// value and hands `json` to the positional input — and the verb then goes
/// looking for a file called `json`. The "no such file" that follows is true
/// and useless.
///
/// The ambiguity is real and not clap's fault: with a space-separated optional
/// value, `fluree rdf count --profile data.ttl` could not be told apart from a
/// format named `data.ttl`. So the form stays attached-only, and this turns
/// the confusing failure into an instruction.
fn reject_profile_space_form(common: &RdfCommonArgs) -> CliResult<()> {
    let Some(input) = common.input.as_deref() else {
        return Ok(());
    };
    if common.profile.is_none() || input.exists() {
        return Ok(());
    }
    let name = input.as_os_str().to_string_lossy();
    if !["json", "human"].contains(&name.as_ref()) {
        return Ok(());
    }
    Err(CliError::Usage(format!(
        "`--profile` takes its format attached: `--profile={name}`, not `--profile {name}`\n  \
         {} written with a space, '{name}' is read as the input file, and no such file exists",
        colored::Colorize::bold(colored::Colorize::cyan("help:")),
    )))
}

/// A sink that accepts every event and keeps nothing.
///
/// `check` and `count` both need a parse to *happen* without needing anything
/// it produces: check wants the errors, count wants the tallies, and neither
/// wants a graph. The tallies come from the [`TimingSink`] decorator wrapped
/// around this, which counts every event class already — so counting lives in
/// one place instead of being reimplemented per verb.
///
/// Term ids are minted monotonically rather than all being zero. Nothing in
/// the parser compares them today, but a sink that hands the same id back for
/// every term is one refactor away from silently merging things, and a
/// wrapping counter costs an increment.
#[derive(Debug, Default)]
pub struct DiscardSink {
    next: u32,
}

impl DiscardSink {
    /// Create one.
    pub fn new() -> Self {
        Self::default()
    }

    fn mint(&mut self) -> TermId {
        self.next = self.next.wrapping_add(1);
        TermId::new(self.next)
    }
}

impl GraphSink for DiscardSink {
    fn on_base(&mut self, _base_iri: &str) {}
    fn on_prefix(&mut self, _prefix: &str, _namespace_iri: &str) {}

    fn term_iri(&mut self, _iri: &str) -> TermId {
        self.mint()
    }

    fn term_blank(&mut self, _label: Option<&str>) -> TermId {
        self.mint()
    }

    fn term_literal(
        &mut self,
        _value: &str,
        _datatype: Datatype,
        _language: Option<&str>,
    ) -> TermId {
        self.mint()
    }

    fn term_literal_value(&mut self, _value: LiteralValue, _datatype: Datatype) -> TermId {
        self.mint()
    }

    fn emit_triple(&mut self, _s: TermId, _p: TermId, _o: TermId) -> SinkResult {
        Ok(())
    }

    fn emit_list_item(&mut self, _s: TermId, _p: TermId, _o: TermId, _index: i32) -> SinkResult {
        Ok(())
    }

    /// Counting a quad is as easy as counting a triple, so claim the
    /// capability rather than making the future quad reader special-case
    /// these verbs. No producer emits quads yet; when one does, `count`
    /// reports them with no change here.
    fn supports_quads(&self) -> bool {
        true
    }

    fn emit_quad(&mut self, _s: TermId, _p: TermId, _o: TermId, _g: TermId) -> SinkResult {
        Ok(())
    }

    fn emit_quad_list_item(
        &mut self,
        _s: TermId,
        _p: TermId,
        _o: TermId,
        _index: i32,
        _g: TermId,
    ) -> SinkResult {
        Ok(())
    }

    /// Same reasoning for RDF 1.2 reifiers, which the Turtle parser emits
    /// today for asserting forms — refusing them would make `check` reject
    /// documents the parser actually accepts.
    fn supports_reified_triples(&self) -> bool {
        true
    }

    fn emit_reified_triple(
        &mut self,
        _s: TermId,
        _p: TermId,
        _o: TermId,
        _r: TermId,
    ) -> SinkResult {
        Ok(())
    }
}

/// An input that has been read, decompressed, and had its syntax settled.
pub struct Loaded {
    /// Where it came from.
    pub input: RdfInput,
    /// The decoded document.
    pub text: String,
    /// Syntax, compression, and the rule that decided the syntax.
    pub resolved: Resolved,
    /// Bytes read off the wire — the compressed size, when compressed.
    pub bytes_on_wire: u64,
}

/// Read the input, strip any compression, and resolve its syntax.
///
/// Order matters and is not obvious: compression has to be settled before the
/// bytes can be decoded, and the syntax sniff has to run *after*, on the
/// decoded text — sniffing a gzip member tells you nothing about the RDF
/// inside it. So compression is resolved from the file suffix or the magic
/// bytes (both available up front) and the syntax sniff gets the real head.
pub fn load(common: &RdfCommonArgs, timings: &mut PhaseTimings) -> CliResult<Loaded> {
    let source = RdfInput::resolve(common.input.as_deref());
    let decoded = input::read_decoded(&source, timings)?;

    // Sniffing only needs the opening of the document, and a whole-file head
    // would make the "did the sniffer see enough" question depend on file size.
    let head_len = floor_char_boundary(&decoded.text, decoded.text.len().min(1024));
    let resolved = syntax::resolve(
        source.path(),
        common.syntax,
        &decoded.text.as_bytes()[..head_len],
        decoded.compression,
    )?;
    resolved.syntax.require_readable()?;

    Ok(Loaded {
        input: source,
        text: decoded.text,
        resolved,
        bytes_on_wire: decoded.bytes_on_wire,
    })
}

/// Round `index` down to a UTF-8 character boundary.
///
/// Slicing a `&str` off a boundary panics, and both the syntax sniffer (which
/// truncates a head to a fixed byte length) and the diagnostic renderer (which
/// clamps an offset to the input length) can land mid-character.
/// `str::floor_char_boundary` is still unstable.
pub(crate) fn floor_char_boundary(s: &str, mut index: usize) -> usize {
    while index > 0 && !s.is_char_boundary(index) {
        index -= 1;
    }
    index
}

/// What a parse produced, whether or not it succeeded.
pub struct ParseOutcome {
    /// Events seen, counted exactly.
    pub counts: SinkCounts,
    /// What can honestly be said about the sink's cost — which, for the
    /// discard sink these verbs use, is usually "less than this instrument can
    /// resolve". See [`SinkTiming`].
    pub sink: SinkTiming,
    /// The failure, if the document did not parse.
    ///
    /// Returned rather than raised because the two callers want opposite
    /// things from it: `check` renders it as its product, `count` treats it
    /// as a failure.
    pub error: Option<TurtleError>,
}

/// Parse a Turtle-family document, timing and counting it.
///
/// Uses [`ParserOptions::conformant`], not the ingest default: these verbs
/// report on the RDF a document denotes, so collections have to arrive as the
/// `rdf:first`/`rdf:rest` spine W3C says they are, and numeric literals have
/// to keep the lexical form they were written with. Under the ingest options
/// a one-item collection would count as one statement instead of three, and
/// `fluree rdf count` would disagree with every other tool in the field.
pub fn parse_document(
    text: &str,
    base: Option<&str>,
    timings: &mut PhaseTimings,
) -> CliResult<ParseOutcome> {
    let run = parse_into(text, base, DiscardSink::new(), timings);
    // A sink failure is the pipeline's problem, not the document's — a broken
    // pipe or a full disk must not be reported as a syntax error. The discard
    // sink has none, but the shared path can.
    run.finished
        .map_err(|e| CliError::Usage(format!("sink error: {e}")))?;
    Ok(run.outcome)
}

/// A finished parse: what happened, the sink it happened into, and whether
/// the sink itself is in a good state.
pub struct ParseRun<S> {
    /// Counts, sink timing, and the parse error if there was one.
    pub outcome: ParseOutcome,
    /// The sink, handed back so the caller can read its product and — for
    /// anything buffering bytes — flush it where a failure can still be
    /// reported.
    pub sink: S,
    /// The sink's `finish()` verdict, in the sink's own error type.
    ///
    /// Separate from [`ParseOutcome::error`] because the two mean opposite
    /// things: a parse error is the document's fault and a finish error is the
    /// destination's, and a converter that conflates them tells a user their
    /// RDF is broken when their disk is full.
    ///
    /// Deliberately NOT flattened into [`CliError`] here. Flattening loses the
    /// [`std::io::ErrorKind`], which left callers matching the *text* of the
    /// error to recognize a broken pipe — and glibc translates `strerror`
    /// under `LC_MESSAGES`, so that check silently stops working in any locale
    /// but the tester's.
    pub finished: Result<(), fluree_graph_ir::SinkError>,
}

/// Parse a Turtle-family document into `sink`, timing and counting it.
///
/// The generalization behind [`parse_document`]: `check` and `count` want the
/// events discarded, `convert` wants them written, and everything else about
/// the run — the parser options, the sampling seed, which phase the clock is
/// in — has to be identical or the three verbs stop describing the same work.
///
/// The sink comes back in the [`ParseRun`], because [`TimingSink`] has to own
/// what it measures and a caller holding buffered bytes has to flush them
/// somewhere a failure can still be returned. Letting a `BufWriter` flush on
/// drop instead would discard exactly the error that says the output is
/// incomplete.
///
/// # Conformant options are not optional here
///
/// [`ParserOptions::conformant`], never the ingest default. These verbs report
/// on and reproduce the RDF a document denotes, so collections must arrive as
/// the `rdf:first`/`rdf:rest` spine W3C says they are and numeric literals
/// must keep the lexical form they were written with. Under the ingest options
/// a collection arrives as indexed list items instead — which `count` would
/// under-report, and which every writer refuses outright, because an indexed
/// list item is a Fluree storage shape with no RDF serialization.
pub fn parse_into<S: GraphSink>(
    text: &str,
    base: Option<&str>,
    sink: S,
    timings: &mut PhaseTimings,
) -> ParseRun<S> {
    // Seeding the sampler from the corpus keeps repeat profiles of one input
    // comparable while leaving no fixed pattern for a periodic corpus to
    // alias against.
    let mut sink = TimingSink::with_corpus(sink, text.as_bytes());

    timings.enter(Phase::Parse);
    let result = fluree_graph_turtle::parse_with_prefixes_base_options(
        text,
        &mut sink,
        &[],
        base,
        ParserOptions::conformant(),
    );
    // `finish` is the owner's call, and the owner is here. The writers latch
    // their failures precisely so this call sees one that had no earlier
    // return value to ride out on.
    let finished = sink.finish();
    timings.finish();

    ParseRun {
        outcome: ParseOutcome {
            counts: sink.counts(),
            sink: sink.sink_timing(),
            error: result.err(),
        },
        sink: sink.into_inner(),
        finished,
    }
}

/// Emit `--profile` or `--time` for a finished run.
///
/// Shared by every verb, and called on the failure path as well as the success
/// one: a profile that only appears when the document was valid is a profile
/// you cannot use to find out why the broken one was slow. The counts it
/// reports are then the counts up to the failure, which is what the diagnostic
/// beside it already says.
pub fn report_run(
    common: &RdfCommonArgs,
    verb: &'static str,
    loaded: &Loaded,
    outcome: &ParseOutcome,
    timings: &PhaseTimings,
    wall: Duration,
) -> CliResult<()> {
    if let Some(format) = common.profile {
        let ctx = profile::RunContext {
            verb,
            input: loaded.input.display(),
            syntax: loaded.resolved.syntax,
            syntax_source: loaded.resolved.source,
            compression: loaded.resolved.compression,
            bytes_on_wire: loaded.bytes_on_wire,
            bytes_decoded: loaded.text.len() as u64,
            // Hashing happens here, after `wall` was taken — see the
            // `wall_ns` docs for what the measured window covers.
            sha256: (!common.no_hash).then(|| profile::sha256_hex(&loaded.text)),
        };
        profile::ProfileReport::build(&ctx, timings, wall, outcome.counts, outcome.sink)
            .emit(format)?;
    } else if common.time {
        count::print_timing(wall, outcome.counts.emitted(), loaded.text.len() as u64);
    }
    Ok(())
}

/// Terminate with exit code 1 — "the document is bad" — after the command has
/// already written whatever it had to say.
///
/// Distinct from [`CliError::Usage`], which exits 2 and means the *invocation*
/// was wrong. Keeping the two apart is what lets `fluree rdf check` be used in
/// a script: a non-zero exit tells you there is a problem, and the code tells
/// you whose.
pub fn exit_document_invalid() -> CliError {
    CliError::ExitCode(EXIT_ERROR)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(doc: &str) -> ParseOutcome {
        parse_document(doc, None, &mut PhaseTimings::start()).unwrap()
    }

    fn args_with(input: Option<&str>, profile: Option<profile::ProfileFormat>) -> RdfCommonArgs {
        RdfCommonArgs {
            input: input.map(std::path::PathBuf::from),
            syntax: None,
            base: None,
            time: false,
            profile,
            no_hash: false,
        }
    }

    #[test]
    fn profile_written_with_a_space_is_explained_rather_than_read_as_a_file() {
        // `--profile json` leaves clap holding `json` as the positional input,
        // and the verb then reports that no file called `json` exists. True,
        // and no help at all.
        let err = reject_profile_space_form(&args_with(
            Some("json"),
            Some(profile::ProfileFormat::Human),
        ))
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("--profile=json"), "{msg}");
        assert!(matches!(err, CliError::Usage(_)));
    }

    #[test]
    fn the_space_form_guard_does_not_fire_on_ordinary_inputs() {
        // A real input alongside --profile is the normal case.
        assert!(reject_profile_space_form(&args_with(
            Some("data.ttl"),
            Some(profile::ProfileFormat::Json)
        ))
        .is_ok());
        // A file genuinely named `json` is read, not second-guessed.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("json");
        std::fs::write(&path, "<a> <b> <c> .\n").unwrap();
        assert!(reject_profile_space_form(&RdfCommonArgs {
            input: Some(path),
            ..args_with(None, Some(profile::ProfileFormat::Human))
        })
        .is_ok());
        // And without --profile the word `json` is just a missing file.
        assert!(reject_profile_space_form(&args_with(Some("json"), None)).is_ok());
    }

    #[test]
    fn a_valid_document_parses_with_no_error_and_exact_counts() {
        let out = parse("@prefix ex: <http://e/> .\nex:a ex:b \"c\" ; ex:d 1 .\n");
        assert!(out.error.is_none());
        assert_eq!(out.counts.triples, 2);
        assert_eq!(
            out.counts.statements, 2,
            "the Turtle grammar counts a directive as a statement, so this is \
             `@prefix` plus one predicate-object list — not two triples"
        );
        assert_eq!(out.counts.prefixes, 1);
    }

    #[test]
    fn a_malformed_document_returns_the_error_instead_of_raising_it() {
        let out = parse("@prefix ex: <http://e/> .\nex:a ex:b ?? .\n");
        assert!(out.error.is_some(), "the parser must have refused this");
        // And the counts still describe what was seen before the failure.
        assert_eq!(out.counts.prefixes, 1);
    }

    #[test]
    fn collections_arrive_as_a_spine_so_counts_match_the_rest_of_the_field() {
        // The ingest default would emit ONE indexed list-item event for this
        // and `count` would report 1 where riot reports 7 (3 spine nodes × 2,
        // plus the statement itself). The conformant options are what make
        // the number comparable.
        let out = parse("<http://e/s> <http://e/p> ( \"a\" \"b\" \"c\" ) .\n");
        assert!(out.error.is_none());
        assert_eq!(out.counts.list_items, 0, "no indexed items in spine mode");
        assert_eq!(
            out.counts.triples, 7,
            "3 rdf:first + 3 rdf:rest + the statement itself"
        );
    }

    #[test]
    fn an_empty_collection_in_object_position_is_not_silently_dropped() {
        // Under the ingest options this emits nothing at all. Conformant
        // options make it the one `rdf:nil` triple it denotes.
        let out = parse("<http://e/s> <http://e/p> () .\n");
        assert!(out.error.is_none());
        assert_eq!(out.counts.triples, 1);
    }

    #[test]
    fn the_discard_sink_accepts_quads_and_reifiers_so_neither_is_refused() {
        // Both capabilities are probed by producers before they emit. A
        // triple-only answer here would make `check` reject documents the
        // parser can read.
        let sink = DiscardSink::new();
        assert!(sink.supports_quads());
        assert!(sink.supports_reified_triples());
    }

    #[test]
    fn the_discard_sink_mints_distinct_term_ids() {
        let mut sink = DiscardSink::new();
        let a = sink.term_iri("http://e/a");
        let b = sink.term_iri("http://e/b");
        let c = sink.term_blank(None);
        assert_ne!(a, b);
        assert_ne!(b, c);
    }

    #[test]
    fn an_rdf12_asserting_form_parses_rather_than_being_refused() {
        let out = parse("<http://e/s> <http://e/p> <http://e/o> {| <http://e/q> \"r\" |} .\n");
        assert!(out.error.is_none(), "{:?}", out.error);
        assert!(out.counts.reified > 0, "the reifier event must have fired");
    }

    #[test]
    fn a_base_iri_resolves_relative_references() {
        let mut timings = PhaseTimings::start();
        let out =
            parse_document("<a> <b> <c> .\n", Some("http://example.org/"), &mut timings).unwrap();
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(out.counts.triples, 1);

        // Without a base the same document has nothing to resolve against.
        let out = parse("<a> <b> <c> .\n");
        assert!(out.error.is_some(), "a relative IRI needs a base");
    }

    #[test]
    fn parsing_charges_its_time_to_the_parse_phase_only() {
        let mut timings = PhaseTimings::start();
        let doc = "<http://e/s> <http://e/p> \"o\" .\n".repeat(200);
        parse_document(&doc, None, &mut timings).unwrap();
        assert!(timings.elapsed(Phase::Parse) > Duration::ZERO);
        assert_eq!(timings.elapsed(Phase::Read), Duration::ZERO);
        assert_eq!(timings.elapsed(Phase::Decompress), Duration::ZERO);
    }
}
