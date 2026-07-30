//! `--continue-on-error`: skip the statements that do not parse, keep the rest.
//!
//! The parser stops at the first error, which is the right default — a
//! converter that quietly drops input is worse than one that refuses. But a
//! 40 GB dump with nine bad statements is a real thing to have, and refusing
//! the whole of it is not useful either. riot draws the line the same way, and
//! so does this: opt in, report every skip, and exit non-zero so a script
//! cannot mistake a partial conversion for a clean one.
//!
//! # How recovery works
//!
//! Parse. On failure, record a diagnostic, find the next statement boundary
//! after the error with [`splitter::next_statement_boundary`], and parse again
//! from there. Repeat until the document is exhausted or no boundary remains.
//!
//! Two things make that sound rather than merely plausible.
//!
//! **A rejected statement must contribute nothing.** The parser emits during
//! descent — a property list or collection pushes triples before the
//! terminating `.` proves the statement well-formed — so by the time an error
//! is raised, part of the bad statement may already be in the writer. That is
//! what [`WriterConfig::buffer_statements`](fluree_graph_format::WriterConfig)
//! is for: with it on, `abort_statement` is a true rollback and the failed
//! statement leaves no trace. Recovery turns it on and does not offer the
//! choice, because the alternative is emitting fragments of statements the
//! tool has just declared invalid.
//!
//! **Prefixes have to survive the restart.** A resumed parse starts partway
//! through the document, after the `@prefix` block, so it would resolve every
//! prefixed name against nothing. [`PrefixRecorder`] captures the directives
//! as they go past and re-seeds each restart with them. Re-seeding is not the
//! same as replaying: the parser takes them as *bindings* without emitting
//! `on_prefix` events, so the writer does not redeclare a prefix it has
//! already written.
//!
//! # What it cannot do
//!
//! Recovery resumes at a statement boundary found by scanning forward from the
//! error. A directive that appears *after* the error position is honoured on
//! the next restart; one that was skipped over as part of a bad statement is
//! not, and the statements that needed it will fail too and be skipped in
//! turn. This is reported rather than hidden: every skip is a diagnostic.

use crate::error::CliResult;
use crate::rdf::diagnostic::{self, Diagnostic};
use crate::rdf::syntax::RdfSyntax;
use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, SinkResult, TermId};
use fluree_graph_turtle::{splitter, ParserOptions};

/// Records `@prefix` and `@base` so a resumed parse can be re-seeded.
///
/// Forwards everything unchanged; it only listens.
pub struct PrefixRecorder<S> {
    inner: S,
    prefixes: Vec<(String, String)>,
    base: Option<String>,
}

impl<S: GraphSink> PrefixRecorder<S> {
    /// Wrap `inner`.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            prefixes: Vec::new(),
            base: None,
        }
    }

    /// Bindings seen so far, for re-seeding a restart.
    pub fn prefixes(&self) -> &[(String, String)] {
        &self.prefixes
    }

    /// The base in scope, which a restart also has to inherit.
    pub fn base(&self) -> Option<&str> {
        self.base.as_deref()
    }

    /// Unwrap.
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: GraphSink> GraphSink for PrefixRecorder<S> {
    fn on_prefix(&mut self, prefix: &str, namespace_iri: &str) {
        // A redeclaration shadows the earlier binding, matching Turtle.
        if let Some(existing) = self.prefixes.iter_mut().find(|(p, _)| p == prefix) {
            existing.1 = namespace_iri.to_string();
        } else {
            self.prefixes
                .push((prefix.to_string(), namespace_iri.to_string()));
        }
        self.inner.on_prefix(prefix, namespace_iri);
    }

    fn on_base(&mut self, base_iri: &str) {
        self.base = Some(base_iri.to_string());
        self.inner.on_base(base_iri);
    }

    fn term_iri(&mut self, iri: &str) -> TermId {
        self.inner.term_iri(iri)
    }
    fn term_blank(&mut self, label: Option<&str>) -> TermId {
        self.inner.term_blank(label)
    }
    fn term_literal(&mut self, value: &str, datatype: Datatype, language: Option<&str>) -> TermId {
        self.inner.term_literal(value, datatype, language)
    }
    fn term_literal_value(&mut self, value: LiteralValue, datatype: Datatype) -> TermId {
        self.inner.term_literal_value(value, datatype)
    }
    fn emit_triple(&mut self, s: TermId, p: TermId, o: TermId) -> SinkResult {
        self.inner.emit_triple(s, p, o)
    }
    fn emit_list_item(&mut self, s: TermId, p: TermId, o: TermId, index: i32) -> SinkResult {
        self.inner.emit_list_item(s, p, o, index)
    }
    fn supports_quads(&self) -> bool {
        self.inner.supports_quads()
    }
    fn emit_quad(&mut self, s: TermId, p: TermId, o: TermId, g: TermId) -> SinkResult {
        self.inner.emit_quad(s, p, o, g)
    }
    fn supports_reified_triples(&self) -> bool {
        self.inner.supports_reified_triples()
    }
    fn emit_reified_triple(&mut self, s: TermId, p: TermId, o: TermId, r: TermId) -> SinkResult {
        self.inner.emit_reified_triple(s, p, o, r)
    }
    fn end_statement(&mut self) {
        self.inner.end_statement();
    }
    fn abort_statement(&mut self) {
        self.inner.abort_statement();
    }
    fn finish(&mut self) -> SinkResult {
        self.inner.finish()
    }
}

/// What a recovering parse skipped.
#[derive(Debug, Default)]
pub struct Recovery {
    /// One per skipped statement, in document order, positioned against the
    /// whole document rather than the fragment that failed.
    pub skipped: Vec<Diagnostic>,
}

impl Recovery {
    /// Whether anything was skipped — the exit-code question.
    pub fn is_clean(&self) -> bool {
        self.skipped.is_empty()
    }
}

/// Parse `text` into `sink`, skipping statements that do not parse.
///
/// The sink must be configured with statement buffering; see the module docs
/// for why that is a requirement and not a preference.
///
/// `source` picks the reader, for the same reason the streaming and parallel
/// paths dispatch on it: the line formats are grammars defined by what they
/// refuse. Recovering with the Turtle parser would not merely accept the
/// Turtle-only constructs an N-Triples document must be refused for — it would
/// accept them *silently*, since recovery reports what it skipped and a
/// construct that parses is never skipped.
pub fn parse_recovering<S: GraphSink>(
    text: &str,
    source: RdfSyntax,
    base: Option<&str>,
    sink: &mut PrefixRecorder<S>,
) -> CliResult<Recovery> {
    let mut recovery = Recovery::default();
    let mut offset = 0usize;

    loop {
        let seeded_base = sink.base().map(str::to_string);
        let effective_base = seeded_base.as_deref().or(base);
        let prefixes = sink.prefixes().to_vec();

        // The same four readers `rdf::parse_into` dispatches to. The line
        // formats take neither prefixes nor a base, because they have neither.
        let fragment = &text[offset..];
        let result = match source {
            RdfSyntax::NTriples => fluree_graph_turtle::parse_ntriples(fragment, sink),
            RdfSyntax::NQuads => fluree_graph_turtle::parse_nquads(fragment, sink),
            RdfSyntax::TriG => fluree_graph_turtle::parse_with_prefixes_base_options(
                fragment,
                sink,
                &prefixes,
                effective_base,
                ParserOptions::conformant().with_dialect(fluree_graph_turtle::Dialect::TriG),
            ),
            _ => fluree_graph_turtle::parse_with_prefixes_base_options(
                fragment,
                sink,
                &prefixes,
                effective_base,
                ParserOptions::conformant(),
            ),
        };

        let Err(error) = result else {
            return Ok(recovery);
        };

        // A sink failure is the destination's problem, not the document's, and
        // retrying would write the rest of a document into a broken pipe.
        if matches!(error, fluree_graph_turtle::TurtleError::Sink(_)) {
            return Ok(recovery);
        }

        // Position the diagnostic against the whole document, not the
        // fragment — a user counting lines is counting the file's.
        let absolute = rebase(&error, offset);
        recovery
            .skipped
            .push(diagnostic::from_turtle_error(&absolute, text));

        let error_at = error_position(&error).map_or(offset, |p| offset + p);
        let Some(resume) = splitter::next_statement_boundary(text, error_at) else {
            // Nothing parseable remains.
            return Ok(recovery);
        };
        if resume <= offset {
            // Cannot happen — `next_statement_boundary` scans forward from
            // `error_at >= offset` — but a resync that failed to advance would
            // spin forever, so it stops instead.
            return Ok(recovery);
        }
        offset = resume;
        if offset >= text.len() {
            return Ok(recovery);
        }
    }
}

/// Byte offset a parse error points at, when it has one.
fn error_position(error: &fluree_graph_turtle::TurtleError) -> Option<usize> {
    match error {
        fluree_graph_turtle::TurtleError::Lexer { position, .. }
        | fluree_graph_turtle::TurtleError::Parse { position, .. } => Some(*position),
        _ => None,
    }
}

/// Shift an error's position from the parsed fragment onto the whole document.
fn rebase(
    error: &fluree_graph_turtle::TurtleError,
    offset: usize,
) -> fluree_graph_turtle::TurtleError {
    use fluree_graph_turtle::TurtleError as E;
    match error {
        E::Lexer { position, message } => E::lexer(position + offset, message.clone()),
        E::Parse { position, message } => E::parse(position + offset, message.clone()),
        // Errors without a position are reported as they are.
        other => E::parse(offset, other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_graph_format::{NTriplesWriter, WriterConfig};

    fn convert_recovering(ttl: &str) -> (String, Recovery) {
        let config = WriterConfig::new().with_statement_buffering(true);
        let writer = NTriplesWriter::with_config(Vec::new(), &config);
        let mut sink = PrefixRecorder::new(writer);
        let recovery = parse_recovering(ttl, RdfSyntax::Turtle, None, &mut sink)
            .expect("recovery must not fail");
        let mut writer = sink.into_inner();
        writer.finish().ok();
        (String::from_utf8(writer.into_inner()).unwrap(), recovery)
    }

    #[test]
    fn a_bad_statement_is_skipped_and_the_rest_survives() {
        let ttl = "@prefix ex: <http://example.org/> .\n\
                   ex:a ex:p \"1\" .\n\
                   ex:b ex:p ?? .\n\
                   ex:c ex:p \"3\" .\n";
        let (out, recovery) = convert_recovering(ttl);

        assert_eq!(recovery.skipped.len(), 1, "{:?}", recovery.skipped);
        assert!(out.contains("\"1\""), "{out}");
        assert!(
            out.contains("\"3\""),
            "the statements after the bad one: {out}"
        );
        assert!(
            !out.contains("ex:b"),
            "the bad statement contributed output: {out}"
        );
    }

    #[test]
    fn a_rejected_statement_contributes_nothing_even_when_it_emitted_first() {
        // The reason statement buffering is mandatory here: a property list
        // pushes triples during descent, before the terminating `.` proves the
        // statement well-formed. Without rollback, half of `ex:bad` would be
        // in the output of a statement the tool just called invalid.
        let ttl = "@prefix ex: <http://example.org/> .\n\
                   ex:good ex:p \"keep\" .\n\
                   ex:bad ex:p \"emitted\" ; ex:q \"also emitted\" ; ex:r ?? .\n\
                   ex:after ex:p \"keep too\" .\n";
        let (out, recovery) = convert_recovering(ttl);

        assert_eq!(recovery.skipped.len(), 1);
        assert!(out.contains("keep"), "{out}");
        assert!(out.contains("keep too"), "{out}");
        assert!(
            !out.contains("emitted"),
            "a rolled-back statement left its first triples behind:\n{out}"
        );
    }

    #[test]
    fn prefixes_survive_a_restart() {
        // The resumed parse begins after the @prefix block. Without
        // re-seeding, every prefixed name after the first error is unresolvable
        // and the whole rest of the document is skipped one statement at a
        // time.
        let ttl = "@prefix ex: <http://example.org/> .\n\
                   ex:a ex:p \"1\" .\n\
                   ex:b ex:p ?? .\n\
                   ex:c ex:p \"3\" .\n\
                   ex:d ex:p \"4\" .\n";
        let (out, recovery) = convert_recovering(ttl);

        assert_eq!(recovery.skipped.len(), 1, "{:?}", recovery.skipped);
        assert!(out.contains("http://example.org/c"), "prefix lost: {out}");
        assert!(out.contains("http://example.org/d"), "prefix lost: {out}");
    }

    #[test]
    fn several_bad_statements_are_each_reported_once() {
        let ttl = "@prefix ex: <http://example.org/> .\n\
                   ex:a ex:p ?? .\n\
                   ex:b ex:p \"ok\" .\n\
                   ex:c ex:p ?? .\n\
                   ex:d ex:p ?? .\n\
                   ex:e ex:p \"ok2\" .\n";
        let (out, recovery) = convert_recovering(ttl);

        assert_eq!(recovery.skipped.len(), 3, "{:?}", recovery.skipped);
        assert!(out.contains("\"ok\""), "{out}");
        assert!(out.contains("\"ok2\""), "{out}");
        // Diagnostics are positioned against the whole document and increase.
        let lines: Vec<usize> = recovery.skipped.iter().filter_map(|d| d.line).collect();
        assert_eq!(
            lines.len(),
            3,
            "every skip is located: {:?}",
            recovery.skipped
        );
        assert!(
            lines.windows(2).all(|w| w[0] < w[1]),
            "diagnostics are out of document order: {lines:?}"
        );
    }

    #[test]
    fn a_clean_document_reports_nothing_skipped() {
        let ttl = "@prefix ex: <http://example.org/> .\nex:a ex:p \"1\" .\n";
        let (out, recovery) = convert_recovering(ttl);
        assert!(recovery.is_clean());
        assert!(out.contains("\"1\""));
    }

    #[test]
    fn a_document_that_is_entirely_broken_terminates() {
        // Recovery must always advance; a document with no parseable statement
        // must end rather than spin.
        let ttl = "?? ?? ?? .\n?? ?? ?? .\n";
        let (_, recovery) = convert_recovering(ttl);
        assert!(!recovery.is_clean());
        assert!(recovery.skipped.len() <= 4, "{:?}", recovery.skipped);
    }

    #[test]
    fn a_trailing_broken_statement_with_no_terminator_terminates() {
        let ttl = "@prefix ex: <http://example.org/> .\nex:a ex:p \"1\" .\nex:b ex:p ??";
        let (out, recovery) = convert_recovering(ttl);
        assert!(!recovery.is_clean());
        assert!(out.contains("\"1\""), "{out}");
    }
}
