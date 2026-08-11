//! Line-based writers: N-Triples and N-Quads
//!
//! One statement per line, every term written in full. No prefixes, no
//! grouping, no lookahead — which is what makes these the formats to reach for
//! when the consumer is `sort`, `split`, or another program.

use super::terms::{write_nt_term, WriterTerms};
use super::{blank::BlankLabeler, Deferred, Out, WriterConfig, WriterStats};
use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, SinkResult, TermId};
use std::io::Write;

/// Shared machinery: the two line formats differ only in whether a graph term
/// may follow the object.
struct LineCore<W: Write> {
    out: Out<W>,
    terms: WriterTerms,
    statements: u64,
    aborted: u64,
    /// Failures raised where the protocol has no error channel.
    deferred: Deferred,
}

impl<W: Write> LineCore<W> {
    fn new(writer: W, config: &WriterConfig) -> Self {
        Self {
            out: Out::new(writer, config.buffer_statements),
            terms: WriterTerms::new(BlankLabeler::new(config.blank_labels)),
            statements: 0,
            aborted: 0,
            deferred: Deferred::default(),
        }
    }

    fn write_statement(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        graph: Option<TermId>,
    ) -> SinkResult {
        self.deferred.check()?;
        self.refuse_unwritable_positions(subject, predicate, graph)?;

        let Self { out, terms, .. } = self;
        write_nt_term(out, terms.get(subject))?;
        out.write_all(b" ")?;
        write_nt_term(out, terms.get(predicate))?;
        out.write_all(b" ")?;
        write_nt_term(out, terms.get(object))?;
        if let Some(graph) = graph {
            out.write_all(b" ")?;
            write_nt_term(out, terms.get(graph))?;
        }
        out.write_all(b" .\n")?;
        self.statements += 1;
        Ok(())
    }

    /// Refuse a term in a position whose syntax cannot hold it.
    ///
    /// A literal may not be a subject, a predicate, or a graph name in any RDF
    /// syntax. The protocol does not enforce this — `Triple` documents the
    /// invariant without checking it — so a hand-fed producer can present one,
    /// and writing it would produce a document no parser reads.
    fn refuse_unwritable_positions(
        &mut self,
        subject: TermId,
        predicate: TermId,
        graph: Option<TermId>,
    ) -> SinkResult {
        for (id, position) in [
            (Some(subject), "a subject"),
            (Some(predicate), "a predicate"),
            (graph, "a graph name"),
        ] {
            let Some(id) = id else { continue };
            if self.terms.get(id).is_literal() {
                return Err(self.deferred.refuse(format!(
                    "a literal cannot be {position}; it must be an IRI or a blank node"
                )));
            }
        }
        Ok(())
    }

    /// The `emit_list_item` refusal, shared by both formats.
    fn refuse_list_item(&mut self) -> SinkResult {
        Err(self.deferred.refuse(
            "an indexed list item has no RDF spelling: a collection is three triples per element, \
             and this event carries one triple plus a position. Parse with \
             CollectionStyle::Spine to write collections as RDF.",
        ))
    }

    fn stats(&self) -> WriterStats {
        WriterStats {
            statements: self.statements,
            bytes: self.out.bytes(),
            aborted: self.aborted,
            refused: self.deferred.refusals(),
        }
    }
}

/// Generate the shared `GraphSink` term/lifecycle methods for a writer that
/// delegates to a [`LineCore`].
macro_rules! line_sink_common {
    () => {
        fn on_base(&mut self, _base_iri: &str) {
            // Every IRI is written absolute, so a `@base` would resolve
            // nothing — and the line formats have no directive syntax anyway.
        }

        fn on_prefix(&mut self, _prefix: &str, _namespace_iri: &str) {
            // No prefixed-name syntax in the line formats.
        }

        fn term_iri(&mut self, iri: &str) -> TermId {
            self.0.terms.iri(iri)
        }

        fn term_blank(&mut self, label: Option<&str>) -> TermId {
            let core = &mut self.0;
            core.terms.blank(label, &mut core.deferred)
        }

        fn term_literal(
            &mut self,
            value: &str,
            datatype: Datatype,
            language: Option<&str>,
        ) -> TermId {
            self.0.terms.literal(value, datatype, language)
        }

        fn term_literal_value(&mut self, value: LiteralValue, datatype: Datatype) -> TermId {
            self.0.terms.literal_value(value, datatype)
        }

        fn emit_list_item(
            &mut self,
            _subject: TermId,
            _predicate: TermId,
            _object: TermId,
            _index: i32,
        ) -> SinkResult {
            self.0.refuse_list_item()
        }

        /// Same refusal as the triple form, for the same reason: the problem
        /// is the indexed item, not the graph it names. Overridden
        /// explicitly rather than left to the trait default, because these
        /// writers DO claim quad support, and a reader of this impl should
        /// see the refusal beside the capability rather than infer it.
        fn emit_quad_list_item(
            &mut self,
            _subject: TermId,
            _predicate: TermId,
            _object: TermId,
            _index: i32,
            _graph: TermId,
        ) -> SinkResult {
            self.0.refuse_list_item()
        }

        fn end_statement(&mut self) {
            self.0.terms.end_statement();
            // `end_statement` has no error channel, so a broken pipe hit
            // while releasing a buffered statement is stashed for the next
            // emission to raise — which is what keeps early termination
            // working in buffered mode too.
            if let Err(e) = self.0.out.commit_statement() {
                self.0.deferred.stash(e.into());
            }
        }

        fn abort_statement(&mut self) {
            self.0.terms.end_statement();
            self.0.out.discard_statement();
            self.0.aborted += 1;
        }

        fn finish(&mut self) -> SinkResult {
            // Flush first, then report — a failure stashed earlier is the one
            // with the context, and it must not be lost behind a secondary
            // error from flushing an already-broken pipe.
            let flushed = self.0.out.finish();
            self.0.deferred.check()?;
            flushed?;
            Ok(())
        }
    };
}

/// Writes N-Triples: `subject predicate object .`, one per line.
///
/// Triple-only — [`supports_quads`](GraphSink::supports_quads) is `false`, so
/// a producer handed a named-graph document is refused rather than having its
/// graph names silently dropped. Use [`NQuadsWriter`] for datasets.
pub struct NTriplesWriter<W: Write>(LineCore<W>);

impl<W: Write> NTriplesWriter<W> {
    /// Write N-Triples to `writer` with default configuration.
    pub fn new(writer: W) -> Self {
        Self::with_config(writer, &WriterConfig::default())
    }

    /// Write N-Triples to `writer` under `config`.
    pub fn with_config(writer: W, config: &WriterConfig) -> Self {
        Self(LineCore::new(writer, config))
    }

    /// Statements and bytes written so far.
    pub fn stats(&self) -> WriterStats {
        self.0.stats()
    }

    /// Recover the underlying writer. Call
    /// [`finish`](GraphSink::finish) first — this does not flush.
    pub fn into_inner(self) -> W {
        self.0.out.into_inner()
    }
}

impl<W: Write> GraphSink for NTriplesWriter<W> {
    line_sink_common!();

    fn emit_triple(&mut self, subject: TermId, predicate: TermId, object: TermId) -> SinkResult {
        self.0.write_statement(subject, predicate, object, None)
    }
}

/// Writes N-Quads: `subject predicate object [graph] .`, one per line.
///
/// Quad-capable. Statements in the default graph are written without a fourth
/// term, which is exactly what N-Quads means by the default graph — and what
/// makes an N-Quads file of default-graph-only statements byte-identical to
/// the N-Triples of the same data.
pub struct NQuadsWriter<W: Write>(LineCore<W>);

impl<W: Write> NQuadsWriter<W> {
    /// Write N-Quads to `writer` with default configuration.
    pub fn new(writer: W) -> Self {
        Self::with_config(writer, &WriterConfig::default())
    }

    /// Write N-Quads to `writer` under `config`.
    pub fn with_config(writer: W, config: &WriterConfig) -> Self {
        Self(LineCore::new(writer, config))
    }

    /// Statements and bytes written so far.
    pub fn stats(&self) -> WriterStats {
        self.0.stats()
    }

    /// Recover the underlying writer. Call
    /// [`finish`](GraphSink::finish) first — this does not flush.
    pub fn into_inner(self) -> W {
        self.0.out.into_inner()
    }
}

impl<W: Write> GraphSink for NQuadsWriter<W> {
    line_sink_common!();

    fn supports_quads(&self) -> bool {
        true
    }

    fn emit_triple(&mut self, subject: TermId, predicate: TermId, object: TermId) -> SinkResult {
        self.0.write_statement(subject, predicate, object, None)
    }

    fn emit_quad(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        graph: TermId,
    ) -> SinkResult {
        self.0
            .write_statement(subject, predicate, object, Some(graph))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::BlankNodeLabels;

    /// Drive a sink the way a statement-oriented producer does.
    fn triple<S: GraphSink>(sink: &mut S, s: &str, p: &str, o: &str) {
        let s = sink.term_iri(s);
        let p = sink.term_iri(p);
        let o = sink.term_literal(o, Datatype::xsd_string(), None);
        sink.emit_triple(s, p, o).unwrap();
        sink.end_statement();
    }

    fn write_nt(f: impl FnOnce(&mut NTriplesWriter<&mut Vec<u8>>)) -> String {
        let mut buf = Vec::new();
        let mut w = NTriplesWriter::new(&mut buf);
        f(&mut w);
        w.finish().unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn one_triple_per_line() {
        let out = write_nt(|w| {
            triple(w, "http://ex/a", "http://ex/p", "one");
            triple(w, "http://ex/a", "http://ex/p", "two");
        });
        assert_eq!(
            out,
            "<http://ex/a> <http://ex/p> \"one\" .\n\
             <http://ex/a> <http://ex/p> \"two\" .\n"
        );
    }

    /// The probe is the contract: a producer of a named-graph syntax must
    /// consult it and refuse the input, rather than emit quads and let them
    /// be flattened. Calling `emit_quad` anyway trips the trait default's
    /// `debug_assert` before its refusal, which is deliberate and is covered
    /// by `fluree-graph-ir`'s own tests — so what is checked here is the
    /// answer the producer actually reads.
    #[test]
    fn ntriples_does_not_claim_quad_support() {
        let mut buf = Vec::new();
        let w = NTriplesWriter::new(&mut buf);
        assert!(!w.supports_quads());
        assert!(!w.supports_reified_triples());
    }

    #[test]
    fn nquads_writes_the_graph_term_and_omits_it_for_the_default_graph() {
        let mut buf = Vec::new();
        let mut w = NQuadsWriter::new(&mut buf);
        assert!(w.supports_quads());

        let s = w.term_iri("http://ex/s");
        let p = w.term_iri("http://ex/p");
        let o = w.term_literal("v", Datatype::xsd_string(), None);
        w.emit_triple(s, p, o).unwrap();
        w.end_statement();
        let g = w.term_iri("http://ex/g");
        let o = w.term_literal("v", Datatype::xsd_string(), None);
        w.emit_quad(s, p, o, g).unwrap();
        w.end_statement();
        w.finish().unwrap();

        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "<http://ex/s> <http://ex/p> \"v\" .\n\
             <http://ex/s> <http://ex/p> \"v\" <http://ex/g> .\n"
        );
    }

    #[test]
    fn a_literal_graph_name_is_refused() {
        let mut buf = Vec::new();
        let mut w = NQuadsWriter::new(&mut buf);
        let s = w.term_iri("http://ex/s");
        let p = w.term_iri("http://ex/p");
        let o = w.term_iri("http://ex/o");
        let g = w.term_literal("not a graph", Datatype::xsd_string(), None);
        let err = w
            .emit_quad(s, p, o, g)
            .expect_err("literals cannot name graphs");
        assert!(err.to_string().contains("a graph name"), "{err}");
        // `finish` must NOT report success after a refusal: the document is
        // missing whatever the refused event carried.
        assert!(w.finish().is_err(), "finish claimed a clean document");
        assert!(buf.is_empty(), "nothing was written");
    }

    /// Indexed list items are Fluree's storage shape, not RDF. Writing one as
    /// a plain triple would emit one statement where the document means three,
    /// so the writer refuses and names the option that fixes it.
    #[test]
    fn an_indexed_list_item_is_refused_with_the_remedy() {
        let mut buf = Vec::new();
        let mut w = NTriplesWriter::new(&mut buf);
        let s = w.term_iri("http://ex/s");
        let p = w.term_iri("http://ex/p");
        let o = w.term_literal("first", Datatype::xsd_string(), None);
        let err = w.emit_list_item(s, p, o, 0).expect_err("not RDF");
        assert!(err.to_string().contains("CollectionStyle::Spine"), "{err}");
    }

    /// H-1, path 1: the pipe closes while committing the **last** buffered
    /// statement. Nothing emits afterwards, so `finish` is the only place left
    /// that can tell the driver the document is incomplete.
    #[test]
    fn finish_reports_a_broken_pipe_hit_on_the_last_buffered_statement() {
        let config = WriterConfig::new().with_statement_buffering(true);
        let mut w = NTriplesWriter::with_config(Closed, &config);

        let s = w.term_iri("http://ex/s");
        let p = w.term_iri("http://ex/p");
        let o = w.term_literal("v", Datatype::xsd_string(), None);
        w.emit_triple(s, p, o)
            .expect("buffered, so nothing failed yet");
        w.end_statement();

        let err = w.finish().expect_err("the last statement never landed");
        assert!(err.is_broken_pipe(), "{err:?}");
    }

    /// H-1, path 2: a refusal is the last event. Reporting `Ok` here would
    /// tell a driver the conversion succeeded.
    #[test]
    fn finish_reports_a_refusal_that_was_the_last_event() {
        let mut buf = Vec::new();
        let mut w = NTriplesWriter::with_config(&mut buf, &WriterConfig::default());
        let s = w.term_iri("http://ex/s");
        let p = w.term_iri("http://ex/p");
        let o = w.term_literal("kept", Datatype::xsd_string(), None);
        w.emit_triple(s, p, o).unwrap();
        w.end_statement();

        let bad = w.term_literal("x", Datatype::xsd_string(), None);
        assert!(w.emit_list_item(bad, p, o, 0).is_err());

        let err = w.finish().expect_err("a refusal must reach finish");
        assert!(err.to_string().contains("already refused"), "{err}");
    }

    #[test]
    fn stats_count_aborted_statements_and_refusals() {
        let mut buf = Vec::new();
        let mut w = NTriplesWriter::with_config(&mut buf, &WriterConfig::default());
        triple(&mut w, "http://ex/a", "http://ex/p", "one");

        let s = w.term_iri("http://ex/b");
        let p = w.term_iri("http://ex/p");
        let o = w.term_literal("two", Datatype::xsd_string(), None);
        w.emit_triple(s, p, o).unwrap();
        w.abort_statement();

        assert!(w.emit_list_item(s, p, o, 0).is_err());
        // The latched re-refusal is not a second event.
        assert!(w.emit_triple(s, p, o).is_err());

        let stats = w.stats();
        assert_eq!(stats.statements, 2, "both reached the output, unbuffered");
        assert_eq!(stats.aborted, 1);
        assert_eq!(stats.refused, 1, "one event was refused, not two");
    }

    /// A literal cannot be a subject or a predicate in any RDF syntax. The
    /// protocol documents the invariant on `Triple` without enforcing it, so a
    /// hand-fed producer can present one and the writer would emit a document
    /// no parser reads. Symmetric with the graph-name refusal.
    #[test]
    fn a_literal_in_an_unwritable_position_is_refused() {
        for (position, subject_is_literal) in [("a subject", true), ("a predicate", false)] {
            let mut buf = Vec::new();
            let mut w = NTriplesWriter::new(&mut buf);
            let iri = w.term_iri("http://ex/x");
            let lit = w.term_literal("nope", Datatype::xsd_string(), None);
            let (s, p) = if subject_is_literal {
                (lit, iri)
            } else {
                (iri, lit)
            };

            let err = w
                .emit_triple(s, p, iri)
                .unwrap_err_or_else_message(position);
            assert!(err.contains(position), "{err}");
            assert!(w.finish().is_err(), "a refusal must reach finish");
            assert!(buf.is_empty(), "nothing was written");
        }
    }

    /// Small helper so the loop above reads as one assertion per position.
    trait UnwrapErrMessage {
        fn unwrap_err_or_else_message(self, position: &str) -> String;
    }

    impl UnwrapErrMessage for SinkResult {
        fn unwrap_err_or_else_message(self, position: &str) -> String {
            match self {
                Ok(()) => panic!("a literal in {position} position must be refused"),
                Err(e) => e.to_string(),
            }
        }
    }

    #[test]
    fn stats_count_statements_and_bytes() {
        let mut buf = Vec::new();
        let mut w = NTriplesWriter::new(&mut buf);
        assert_eq!(w.stats(), WriterStats::default());
        triple(&mut w, "http://ex/a", "http://ex/p", "one");
        triple(&mut w, "http://ex/a", "http://ex/p", "two");
        w.finish().unwrap();
        let stats = w.stats();
        assert_eq!(stats.statements, 2);
        assert_eq!(stats.bytes as usize, buf.len());
    }

    /// A writer whose consumer has gone away — `… | head -5`.
    struct Closed;

    impl Write for Closed {
        fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "closed",
            ))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// A broken pipe has to surface at the emission that hit it, so a
    /// producer stops instead of parsing to EOF against a dead consumer.
    #[test]
    fn a_write_failure_reaches_the_producer() {
        let mut w = NTriplesWriter::new(Closed);
        let s = w.term_iri("http://ex/s");
        let p = w.term_iri("http://ex/p");
        let o = w.term_literal("v", Datatype::xsd_string(), None);
        let err = w.emit_triple(s, p, o).expect_err("the pipe is closed");
        assert!(err.is_broken_pipe(), "{err:?}");
    }

    #[test]
    fn unbuffered_abort_keeps_what_was_already_written() {
        // The honest contract: bytes handed to the writer cannot be
        // unwritten, and pretending otherwise would be the lie.
        let out = write_nt(|w| {
            let s = w.term_iri("http://ex/s");
            let p = w.term_iri("http://ex/p");
            let o = w.term_literal("partial", Datatype::xsd_string(), None);
            w.emit_triple(s, p, o).unwrap();
            w.abort_statement();
        });
        assert!(out.contains("\"partial\""), "{out}");
    }

    #[test]
    fn buffered_abort_contributes_nothing() {
        let mut buf = Vec::new();
        let config = WriterConfig::new().with_statement_buffering(true);
        let mut w = NTriplesWriter::with_config(&mut buf, &config);

        let s = w.term_iri("http://ex/s");
        let p = w.term_iri("http://ex/p");
        let kept = w.term_literal("kept", Datatype::xsd_string(), None);
        w.emit_triple(s, p, kept).unwrap();
        w.end_statement();

        let dropped = w.term_literal("dropped", Datatype::xsd_string(), None);
        w.emit_triple(s, p, dropped).unwrap();
        w.abort_statement();

        let after = w.term_literal("after", Datatype::xsd_string(), None);
        w.emit_triple(s, p, after).unwrap();
        w.end_statement();
        w.finish().unwrap();

        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("\"kept\""), "{out}");
        assert!(out.contains("\"after\""), "{out}");
        assert!(!out.contains("\"dropped\""), "{out}");
    }

    /// Buffered mode moves the failure: nothing reaches the pipe until the
    /// statement commits, and `end_statement` has no way to report. Early
    /// termination still has to work, so the failure is stashed and the next
    /// emission raises it.
    #[test]
    fn a_buffered_write_failure_still_stops_the_producer() {
        let config = WriterConfig::new().with_statement_buffering(true);
        let mut w = NTriplesWriter::with_config(Closed, &config);

        let s = w.term_iri("http://ex/s");
        let p = w.term_iri("http://ex/p");
        let o = w.term_literal("v", Datatype::xsd_string(), None);
        w.emit_triple(s, p, o)
            .expect("buffered, so nothing failed yet");
        w.end_statement();

        let o = w.term_literal("v2", Datatype::xsd_string(), None);
        let err = w
            .emit_triple(s, p, o)
            .expect_err("the commit hit a closed pipe");
        assert!(err.is_broken_pipe(), "{err:?}");
    }

    /// A producer that ignores a refusal must not get the refused thing
    /// written on its next attempt.
    #[test]
    fn a_refusal_latches_rather_than_clearing_itself() {
        let mut buf = Vec::new();
        let config = WriterConfig::new().with_blank_labels(BlankNodeLabels::Preserve);
        let mut w = NTriplesWriter::with_config(&mut buf, &config);

        let bad = w.term_blank(Some("fdbw-9"));
        let p = w.term_iri("http://ex/p");
        let o = w.term_iri("http://ex/o");
        assert!(w.emit_triple(bad, p, o).is_err());

        let good = w.term_iri("http://ex/s");
        let err = w
            .emit_triple(good, p, o)
            .expect_err("the writer stays refused");
        assert!(err.to_string().contains("already refused"), "{err}");

        assert!(w.finish().is_err(), "finish claimed a clean document");
        assert!(buf.is_empty(), "nothing reached the output");
    }

    #[test]
    fn preserve_mode_refusals_surface_at_the_next_emission() {
        let mut buf = Vec::new();
        let config = WriterConfig::new().with_blank_labels(BlankNodeLabels::Preserve);
        let mut w = NTriplesWriter::with_config(&mut buf, &config);

        let s = w.term_blank(Some("fdbw-9"));
        let p = w.term_iri("http://ex/p");
        let o = w.term_iri("http://ex/o");
        let err = w
            .emit_triple(s, p, o)
            .expect_err("the reserved label must be refused");
        assert!(err.to_string().contains("reserves"), "{err}");
        assert!(w.finish().is_err(), "finish claimed a clean document");
        assert!(buf.is_empty(), "nothing reached the output");
    }
}
