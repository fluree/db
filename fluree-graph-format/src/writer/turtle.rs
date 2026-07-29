//! Turtle and TriG writers — the "blocks" fidelity tier
//!
//! # What the blocks tier is
//!
//! Consecutive statements about the same subject fold with `;`, and
//! consecutive statements about the same subject *and* predicate fold with
//! `,`. Everything else is written in document order, one statement at a time.
//!
//! That is the whole of it, and the boundary is deliberate. Regrouping a
//! subject that recurs later in the document, reconstructing `[ … ]`, and
//! re-collapsing an `rdf:first`/`rdf:rest` spine into `( … )` all require
//! holding statements back until the document ends — O(document) memory, on a
//! path whose reason to exist is that it does not need any. riot draws the
//! same line between its streaming and pretty writers. A buffered `--pretty`
//! tier is the follow-up; the blocks tier is the v1 contract.
//!
//! In particular, a collection parsed with
//! [`CollectionStyle::Spine`](fluree_graph_turtle::CollectionStyle) is written
//! out as its spine triples, not as `( … )`. The RDF is identical; the
//! spelling is not.

use super::terms::{write_nt_term, write_ttl_predicate, write_ttl_term, WriterTerms};
use super::{blank::BlankLabeler, Deferred, Out, WriterConfig, WriterStats};
use crate::prefix::{write_prefix_declarations, write_turtle_iri, PrefixMap};
use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, SinkError, SinkResult, Term, TermId};
use std::io::Write;

/// Which graph the writer currently has open.
///
/// `None` is "nothing open yet"; `Some(None)` is the default graph, written
/// bare; `Some(Some(term))` is a named graph, written inside a `GRAPH … { }`
/// block.
type OpenGraph = Option<Option<Term>>;

/// The run state a `;`/`,` fold depends on: the subject and predicate of the
/// statement the writer has not yet terminated.
#[derive(Clone, Debug, Default)]
struct Run {
    subject: Option<Term>,
    predicate: Option<Term>,
    graph: OpenGraph,
}

/// Shared machinery for the two Turtle-family writers.
struct BlockCore<W: Write> {
    out: Out<W>,
    terms: WriterTerms,
    prefixes: PrefixMap,
    /// Prefix declarations are written once, immediately before the first
    /// statement, so that directives arriving during the prologue all land in
    /// the header.
    header_written: bool,
    /// Committed run state — what has actually reached the output.
    committed: Run,
    /// Run state including the statement in flight.
    working: Run,
    in_statement: bool,
    statements: u64,
    /// Failures raised where the protocol has no error channel.
    deferred: Deferred,
}

impl<W: Write> BlockCore<W> {
    fn new(writer: W, config: &WriterConfig) -> Self {
        Self {
            out: Out::new(writer, config.buffer_statements),
            terms: WriterTerms::new(BlankLabeler::new(config.blank_labels)),
            prefixes: config.prefixes.clone(),
            header_written: false,
            committed: Run::default(),
            working: Run::default(),
            in_statement: false,
            statements: 0,
            deferred: Deferred::default(),
        }
    }

    /// Declare a prefix.
    ///
    /// Before the first statement this only populates the map, which the
    /// header will declare. Afterwards the declaration is written inline: a
    /// mid-document `@prefix` is legal Turtle, it only binds statements that
    /// follow it, and writing it there is what keeps compaction available for
    /// the rest of the document instead of silently expanding every IRI.
    fn on_prefix(&mut self, prefix: &str, namespace_iri: &str) -> SinkResult {
        self.prefixes.insert(prefix, namespace_iri);
        if !self.header_written {
            return Ok(());
        }
        self.close_statement()?;
        write!(self.out, "@prefix {prefix}: <")?;
        crate::escape::write_escaped_iri(&mut self.out, namespace_iri)?;
        writeln!(self.out, "> .")?;
        // A directive belongs to no statement, so it must not be sitting in a
        // statement buffer that a later `abort_statement` would discard.
        self.out.commit_statement()?;
        Ok(())
    }

    /// Terminate the open statement, if any, and close an open named-graph
    /// block. Leaves the writer at the top level.
    fn close_statement(&mut self) -> SinkResult {
        if self.committed.subject.is_some() {
            self.out.write_all(b" .\n")?;
            self.committed.subject = None;
            self.committed.predicate = None;
            self.working.subject = None;
            self.working.predicate = None;
        }
        Ok(())
    }

    fn close_graph_block(&mut self) -> SinkResult {
        self.close_statement()?;
        if matches!(self.committed.graph, Some(Some(_))) {
            self.out.write_all(b"}\n")?;
        }
        self.committed.graph = None;
        self.working.graph = None;
        Ok(())
    }

    /// Emit one statement, folding it into the run in flight when it can be.
    ///
    /// `graph` is `None` for a triple-only writer and for TriG's default
    /// graph, `Some(term)` for a named graph.
    fn write_statement(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        graph: Option<TermId>,
        quad_capable: bool,
    ) -> SinkResult {
        self.deferred.check()?;
        if let Some(graph) = graph {
            if self.terms.get(graph).is_literal() {
                return Err(SinkError::rejected(
                    "a literal cannot name a graph; graph names are IRIs or blank nodes",
                ));
            }
        }

        if !self.header_written {
            write_prefix_declarations(&self.prefixes, &mut self.out)?;
            // The header belongs to no statement; commit it before the first
            // one can be rolled back and take it along.
            self.out.commit_statement()?;
            self.header_written = true;
        }

        if !self.in_statement {
            self.working = self.committed.clone();
            self.in_statement = true;
        }

        let target_graph: Option<Term> = graph.map(|g| self.terms.get(g).clone());
        if quad_capable && self.working.graph != Some(target_graph.clone()) {
            self.enter_graph(target_graph)?;
        }

        let s = self.terms.get(subject).clone();
        let p = self.terms.get(predicate).clone();
        let inside_block = matches!(self.working.graph, Some(Some(_)));
        let indent: &[u8] = if inside_block { b"        " } else { b"    " };

        let Self {
            out,
            terms,
            prefixes,
            working,
            statements,
            ..
        } = self;

        if working.subject.as_ref() == Some(&s) {
            if working.predicate.as_ref() == Some(&p) {
                // Same subject and predicate: another object in the list.
                out.write_all(b" ,\n")?;
                out.write_all(indent)?;
                out.write_all(b"    ")?;
            } else {
                out.write_all(b" ;\n")?;
                out.write_all(indent)?;
                write_ttl_predicate(out, &p, prefixes)?;
                out.write_all(b" ")?;
            }
        } else {
            if working.subject.is_some() {
                out.write_all(b" .\n")?;
            }
            if inside_block {
                out.write_all(b"    ")?;
            }
            write_ttl_term(out, &s, prefixes)?;
            out.write_all(b"\n")?;
            out.write_all(indent)?;
            write_ttl_predicate(out, &p, prefixes)?;
            out.write_all(b" ")?;
        }
        write_ttl_term(out, terms.get(object), prefixes)?;

        working.subject = Some(s);
        working.predicate = Some(p);
        *statements += 1;
        Ok(())
    }

    /// Switch to `graph`, closing whatever block was open.
    ///
    /// TriG allows a graph to be opened more than once, so arriving statements
    /// are written where they arrive rather than sorted into one block per
    /// graph. See [`TrigWriter`] for why.
    fn enter_graph(&mut self, graph: Option<Term>) -> SinkResult {
        if self.working.subject.is_some() {
            self.out.write_all(b" .\n")?;
            self.working.subject = None;
            self.working.predicate = None;
        }
        if matches!(self.working.graph, Some(Some(_))) {
            self.out.write_all(b"}\n")?;
        }
        if let Some(name) = &graph {
            let Self { out, prefixes, .. } = self;
            out.write_all(b"GRAPH ")?;
            match name {
                Term::Iri(iri) => write_turtle_iri(out, iri, prefixes)?,
                other => write_nt_term(out, other)?,
            }
            out.write_all(b" {\n")?;
        }
        self.working.graph = Some(graph);
        Ok(())
    }

    fn commit_statement(&mut self) {
        if self.in_statement {
            self.committed = self.working.clone();
            self.in_statement = false;
        }
        self.terms.end_statement();
        // No error channel here; stash for the next emission to raise.
        if let Err(e) = self.out.commit_statement() {
            self.deferred.set(e.into());
        }
    }

    /// Roll the statement in flight back.
    ///
    /// Only the *buffered* configuration can actually take bytes back; without
    /// it the run state has to keep the statement, because the output already
    /// has it. Rolling the state back in that case would emit a second subject
    /// line for a subject that is still open.
    fn abort_statement(&mut self) {
        if self.out.is_buffered() {
            self.working = self.committed.clone();
            self.out.discard_statement();
        } else {
            self.committed = self.working.clone();
        }
        self.in_statement = false;
        self.terms.end_statement();
    }

    fn finish(&mut self) -> SinkResult {
        // A producer that never delimits statements still gets its last
        // statement written.
        if self.in_statement {
            self.committed = self.working.clone();
            self.in_statement = false;
        }
        // An empty document still declares the prefixes it was given.
        if !self.header_written {
            write_prefix_declarations(&self.prefixes, &mut self.out)?;
            self.header_written = true;
        }
        self.close_graph_block()?;
        self.out.finish()?;
        Ok(())
    }

    fn refuse_list_item(&self) -> SinkResult {
        Err(SinkError::rejected(
            "an indexed list item has no RDF spelling: a collection is three triples per element, \
             and this event carries one triple plus a position. Parse with \
             CollectionStyle::Spine to write collections as RDF.",
        ))
    }

    fn stats(&self) -> WriterStats {
        WriterStats {
            statements: self.statements,
            bytes: self.out.bytes(),
        }
    }
}

/// Generate the `GraphSink` methods both Turtle-family writers share.
macro_rules! block_sink_common {
    () => {
        fn on_base(&mut self, _base_iri: &str) {
            // Every IRI this writer emits is absolute or a prefixed name, so
            // an emitted `@base` would resolve nothing. Declaring one anyway
            // would be a directive with no effect on how the document reads
            // back.
        }

        fn on_prefix(&mut self, prefix: &str, namespace_iri: &str) {
            // `on_prefix` is infallible in the protocol; a write failure here
            // is raised by the next emission or by `finish`.
            if let Err(e) = self.0.on_prefix(prefix, namespace_iri) {
                self.0.deferred.set(e);
            }
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

        fn end_statement(&mut self) {
            self.0.commit_statement();
        }

        fn abort_statement(&mut self) {
            self.0.abort_statement();
        }

        fn finish(&mut self) -> SinkResult {
            self.0.finish()
        }
    };
}

/// Writes Turtle at the blocks tier (see the [module docs](self)).
///
/// Triple-only. A producer holding a dataset is refused rather than having its
/// graph names dropped — use [`TrigWriter`].
pub struct TurtleWriter<W: Write>(BlockCore<W>);

impl<W: Write> TurtleWriter<W> {
    /// Write Turtle to `writer` with default configuration.
    pub fn new(writer: W) -> Self {
        Self::with_config(writer, &WriterConfig::default())
    }

    /// Write Turtle to `writer` under `config`.
    pub fn with_config(writer: W, config: &WriterConfig) -> Self {
        Self(BlockCore::new(writer, config))
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

impl<W: Write> GraphSink for TurtleWriter<W> {
    block_sink_common!();

    fn emit_triple(&mut self, subject: TermId, predicate: TermId, object: TermId) -> SinkResult {
        self.0
            .write_statement(subject, predicate, object, None, false)
    }
}

/// Writes TriG at the blocks tier, in arrival order.
///
/// # Why arrival order, and why blocks may repeat
///
/// Quads arrive interleaved: a TriG document can leave a graph and come back
/// to it, and so can any producer streaming from a query. There are two ways
/// to write that out.
///
/// *Buffer per graph* — hold every graph's statements until the end, then emit
/// one block each. That produces the tidier document, and costs O(dataset)
/// memory the moment the input interleaves, which is precisely the case a
/// streaming writer exists for.
///
/// *Write where they arrive* — close the open block, open the next, and let a
/// graph appear in more than one block. TriG permits exactly this: a graph's
/// statements are the union of its blocks. Memory stays O(1) and the output is
/// valid.
///
/// This writer takes the second. Input that is already grouped by graph — what
/// a sorted dataset or a per-graph export produces — yields one block per
/// graph regardless, so the tidy case costs nothing. A buffered pretty mode is
/// the same deferred follow-up as Turtle's.
pub struct TrigWriter<W: Write>(BlockCore<W>);

impl<W: Write> TrigWriter<W> {
    /// Write TriG to `writer` with default configuration.
    pub fn new(writer: W) -> Self {
        Self::with_config(writer, &WriterConfig::default())
    }

    /// Write TriG to `writer` under `config`.
    pub fn with_config(writer: W, config: &WriterConfig) -> Self {
        Self(BlockCore::new(writer, config))
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

impl<W: Write> GraphSink for TrigWriter<W> {
    block_sink_common!();

    fn supports_quads(&self) -> bool {
        true
    }

    fn emit_triple(&mut self, subject: TermId, predicate: TermId, object: TermId) -> SinkResult {
        self.0
            .write_statement(subject, predicate, object, None, true)
    }

    fn emit_quad(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        graph: TermId,
    ) -> SinkResult {
        self.0
            .write_statement(subject, predicate, object, Some(graph), true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn prefixed_config() -> WriterConfig {
        let mut prefixes = PrefixMap::new();
        prefixes.insert("ex", "http://ex/");
        WriterConfig::new().with_prefixes(prefixes)
    }

    fn write_ttl(f: impl FnOnce(&mut TurtleWriter<&mut Vec<u8>>)) -> String {
        let mut buf = Vec::new();
        let mut w = TurtleWriter::with_config(&mut buf, &prefixed_config());
        f(&mut w);
        w.finish().unwrap();
        String::from_utf8(buf).unwrap()
    }

    fn triple<S: GraphSink>(sink: &mut S, s: &str, p: &str, o: &str) {
        let s = sink.term_iri(s);
        let p = sink.term_iri(p);
        let o = sink.term_literal(o, Datatype::xsd_string(), None);
        sink.emit_triple(s, p, o).unwrap();
        sink.end_statement();
    }

    #[test]
    fn consecutive_subjects_and_predicates_fold() {
        let out = write_ttl(|w| {
            triple(w, "http://ex/a", "http://ex/p", "1");
            triple(w, "http://ex/a", "http://ex/p", "2");
            triple(w, "http://ex/a", "http://ex/q", "3");
            triple(w, "http://ex/b", "http://ex/p", "4");
        });
        assert_eq!(
            out,
            "@prefix ex: <http://ex/> .\n\
             \n\
             ex:a\n    ex:p \"1\" ,\n        \"2\" ;\n    ex:q \"3\" .\n\
             ex:b\n    ex:p \"4\" .\n"
        );
    }

    /// The tier's boundary, pinned: a subject that recurs after another
    /// subject intervenes is written again, not regrouped. Regrouping needs
    /// the whole document in memory.
    #[test]
    fn a_recurring_subject_is_not_regrouped() {
        let out = write_ttl(|w| {
            triple(w, "http://ex/a", "http://ex/p", "1");
            triple(w, "http://ex/b", "http://ex/p", "2");
            triple(w, "http://ex/a", "http://ex/p", "3");
        });
        assert_eq!(out.matches("ex:a\n").count(), 2, "{out}");
    }

    #[test]
    fn rdf_type_uses_the_a_keyword() {
        let out = write_ttl(|w| {
            let s = w.term_iri("http://ex/a");
            let p = w.term_iri(fluree_vocab::rdf::TYPE);
            let o = w.term_iri("http://ex/Thing");
            w.emit_triple(s, p, o).unwrap();
            w.end_statement();
        });
        assert!(out.contains("    a ex:Thing .\n"), "{out}");
    }

    #[test]
    fn a_mid_document_prefix_is_declared_where_it_appears() {
        let out = write_ttl(|w| {
            triple(w, "http://ex/a", "http://ex/p", "1");
            w.on_prefix("late", "http://late/");
            triple(w, "http://late/b", "http://ex/p", "2");
        });
        // The open statement is terminated before the directive, and the
        // new prefix compacts everything after it.
        assert!(
            out.contains("\"1\" .\n@prefix late: <http://late/> .\n"),
            "{out}"
        );
        assert!(out.contains("late:b\n"), "{out}");
    }

    #[test]
    fn an_empty_document_still_declares_its_prefixes() {
        let out = write_ttl(|_| {});
        assert_eq!(out, "@prefix ex: <http://ex/> .\n\n");
    }

    /// The probe a producer of a named-graph syntax must read before it
    /// emits anything. See the note on N-Triples' equivalent for why the
    /// trait default is not exercised here.
    #[test]
    fn turtle_does_not_claim_quad_support() {
        let mut buf = Vec::new();
        let w = TurtleWriter::new(&mut buf);
        assert!(!w.supports_quads());
    }

    // ---------------------------------------------------------------- TriG

    fn write_trig(f: impl FnOnce(&mut TrigWriter<&mut Vec<u8>>)) -> String {
        let mut buf = Vec::new();
        let mut w = TrigWriter::with_config(&mut buf, &prefixed_config());
        f(&mut w);
        w.finish().unwrap();
        String::from_utf8(buf).unwrap()
    }

    fn quad<S: GraphSink>(sink: &mut S, s: &str, p: &str, o: &str, g: Option<&str>) {
        let s = sink.term_iri(s);
        let p = sink.term_iri(p);
        let o = sink.term_literal(o, Datatype::xsd_string(), None);
        match g {
            Some(g) => {
                let g = sink.term_iri(g);
                sink.emit_quad(s, p, o, g).unwrap();
            }
            None => sink.emit_triple(s, p, o).unwrap(),
        }
        sink.end_statement();
    }

    #[test]
    fn default_graph_statements_are_bare_and_named_ones_are_wrapped() {
        let out = write_trig(|w| {
            assert!(w.supports_quads());
            quad(w, "http://ex/a", "http://ex/p", "d", None);
            quad(w, "http://ex/b", "http://ex/p", "n", Some("http://ex/g"));
        });
        assert_eq!(
            out,
            "@prefix ex: <http://ex/> .\n\
             \n\
             ex:a\n    ex:p \"d\" .\n\
             GRAPH ex:g {\n    ex:b\n        ex:p \"n\" .\n}\n"
        );
    }

    /// The arrival-order design, pinned: a graph revisited after another
    /// graph intervenes opens a second block. That is valid TriG — a graph is
    /// the union of its blocks — and it is what buys O(1) memory.
    #[test]
    fn an_interleaved_graph_gets_a_second_block_rather_than_being_buffered() {
        let out = write_trig(|w| {
            quad(w, "http://ex/a", "http://ex/p", "1", Some("http://ex/g1"));
            quad(w, "http://ex/b", "http://ex/p", "2", Some("http://ex/g2"));
            quad(w, "http://ex/c", "http://ex/p", "3", Some("http://ex/g1"));
        });
        assert_eq!(out.matches("GRAPH ex:g1 {").count(), 2, "{out}");
        assert_eq!(out.matches("GRAPH ex:g2 {").count(), 1, "{out}");
        assert_eq!(out.matches('}').count(), 3, "every block is closed: {out}");
    }

    /// Input already grouped by graph — a sorted dataset, a per-graph export —
    /// gets one block per graph, so the tidy case costs nothing.
    #[test]
    fn graph_grouped_input_yields_one_block_per_graph() {
        let out = write_trig(|w| {
            quad(w, "http://ex/a", "http://ex/p", "1", Some("http://ex/g1"));
            quad(w, "http://ex/b", "http://ex/p", "2", Some("http://ex/g1"));
            quad(w, "http://ex/c", "http://ex/p", "3", Some("http://ex/g2"));
        });
        assert_eq!(out.matches("GRAPH ex:g1 {").count(), 1, "{out}");
        assert_eq!(out.matches("GRAPH ex:g2 {").count(), 1, "{out}");
    }

    #[test]
    fn returning_to_the_default_graph_closes_the_block() {
        let out = write_trig(|w| {
            quad(w, "http://ex/a", "http://ex/p", "1", Some("http://ex/g"));
            quad(w, "http://ex/b", "http://ex/p", "2", None);
        });
        assert!(out.contains("}\nex:b\n"), "{out}");
    }

    #[test]
    fn a_blank_node_may_name_a_graph() {
        let out = write_trig(|w| {
            let s = w.term_iri("http://ex/s");
            let p = w.term_iri("http://ex/p");
            let o = w.term_iri("http://ex/o");
            let g = w.term_blank(Some("g0"));
            w.emit_quad(s, p, o, g).unwrap();
            w.end_statement();
        });
        assert!(out.contains("GRAPH _:b1 {"), "{out}");
    }

    #[test]
    fn stats_count_statements_and_bytes() {
        let mut buf = Vec::new();
        let mut w = TurtleWriter::new(&mut buf);
        triple(&mut w, "http://ex/a", "http://ex/p", "1");
        triple(&mut w, "http://ex/a", "http://ex/q", "2");
        w.finish().unwrap();
        assert_eq!(w.stats().statements, 2);
        assert_eq!(w.stats().bytes as usize, buf.len());
    }

    #[test]
    fn buffered_abort_drops_the_statement_and_its_run_state() {
        let mut buf = Vec::new();
        let mut config = prefixed_config();
        config.buffer_statements = true;
        let mut w = TurtleWriter::with_config(&mut buf, &config);

        triple(&mut w, "http://ex/a", "http://ex/p", "keep");

        let s = w.term_iri("http://ex/b");
        let p = w.term_iri("http://ex/p");
        let o = w.term_literal("dropped", Datatype::xsd_string(), None);
        w.emit_triple(s, p, o).unwrap();
        w.abort_statement();

        triple(&mut w, "http://ex/a", "http://ex/q", "after");
        w.finish().unwrap();

        let out = String::from_utf8(buf).unwrap();
        assert!(!out.contains("dropped"), "{out}");
        // The run state rolled back too, so `ex:a` is still the open subject
        // and the next statement folds into it.
        assert_eq!(
            out,
            "@prefix ex: <http://ex/> .\n\
             \n\
             ex:a\n    ex:p \"keep\" ;\n    ex:q \"after\" .\n"
        );
    }
}
