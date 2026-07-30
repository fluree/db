//! JSON-LD writer — document mode
//!
//! Unlike the four text writers, this one is buffered by construction. JSON-LD
//! nests a subject's properties inside one node object, so a subject's last
//! statement can decide the shape of its first; there is no point in the
//! stream at which a node object is known to be complete. The format's
//! streaming answer is NDJSON — one node object per line — and that is a
//! separate output format, deferred.
//!
//! So: collect into a [`Graph`], format at `finish()`. Memory is O(document),
//! stated plainly rather than implied by the absence of a claim.

use super::terms::WriterTerms;
use super::{blank::BlankLabeler, Deferred, Out, WriterConfig, WriterStats};
use crate::jsonld::{format_jsonld, JsonLdFormatConfig};
use fluree_graph_ir::{
    Datatype, Graph, GraphSink, LiteralValue, SinkError, SinkResult, TermId, TermScope, Triple,
};
use std::io::Write;

/// Collects a graph and writes it as a JSON-LD document when finished.
///
/// # What it is not
///
/// Triple-only. JSON-LD can express a dataset — `@graph` nesting inside a node
/// object — but [`format_jsonld`] does not, so this writer answers `false` to
/// [`supports_quads`](GraphSink::supports_quads) and a producer holding named
/// graphs is refused. Refusing is the point: the JSON-LD *adapter* on the read
/// side already drops `@graph` contents silently, and reproducing that on the
/// write side would turn one known defect into two.
pub struct JsonLdWriter<W: Write> {
    out: Out<W>,
    terms: WriterTerms,
    graph: Graph,
    config: JsonLdFormatConfig,
    statements: u64,
    /// Graph length at the statement in flight's start — the rewind point for
    /// [`GraphSink::abort_statement`]. A statement may contribute several
    /// triples, so this is not derivable from the statement counter.
    mark: usize,
    aborted: u64,
    /// Failures raised where the protocol has no error channel.
    deferred: Deferred,
    finished: bool,
}

impl<W: Write> JsonLdWriter<W> {
    /// Write JSON-LD to `writer` with default configuration.
    pub fn new(writer: W) -> Self {
        Self::with_config(
            writer,
            &WriterConfig::default(),
            JsonLdFormatConfig::default(),
        )
    }

    /// Write JSON-LD to `writer` under a writer config and a JSON-LD format
    /// config.
    ///
    /// The two are separate because they answer different questions:
    /// [`WriterConfig`] decides what the *labels* are, the format config
    /// decides what the *document* looks like. Blank-node labelling is applied
    /// here, by the writer, so `fluree rdf convert` has one blank-node
    /// contract across every output syntax; leave the format config's own
    /// [`BlankNodePolicy`](crate::BlankNodePolicy) at its default
    /// (`PreserveLabeled`) or it will relabel a second time.
    pub fn with_config(writer: W, config: &WriterConfig, jsonld: JsonLdFormatConfig) -> Self {
        Self {
            // Statement buffering is meaningless here: nothing reaches the
            // writer until `finish`, so a rollback is always possible.
            out: Out::new(writer, false),
            terms: WriterTerms::new(BlankLabeler::new(config.blank_labels)),
            graph: Graph::new(),
            config: jsonld,
            statements: 0,
            mark: 0,
            aborted: 0,
            deferred: Deferred::default(),
            finished: false,
        }
    }

    /// Statements collected and bytes written.
    ///
    /// `bytes` stays zero until [`finish`](GraphSink::finish) — that is when
    /// the document exists.
    pub fn stats(&self) -> WriterStats {
        WriterStats {
            statements: self.statements,
            bytes: self.out.bytes(),
            aborted: self.aborted,
            refused: self.deferred.refusals(),
        }
    }

    /// Recover the underlying writer. Call
    /// [`finish`](GraphSink::finish) first, or the document is never written.
    pub fn into_inner(self) -> W {
        self.out.into_inner()
    }

    fn triple(&self, s: TermId, p: TermId, o: TermId) -> Triple {
        Triple::new(
            self.terms.get(s).clone(),
            self.terms.get(p).clone(),
            self.terms.get(o).clone(),
        )
    }
}

impl<W: Write> GraphSink for JsonLdWriter<W> {
    fn on_base(&mut self, base_iri: &str) {
        self.graph.set_base(base_iri);
    }

    fn on_prefix(&mut self, prefix: &str, namespace_iri: &str) {
        self.graph.add_prefix(prefix, namespace_iri);
    }

    fn term_iri(&mut self, iri: &str) -> TermId {
        self.terms.iri(iri)
    }

    fn term_blank(&mut self, label: Option<&str>) -> TermId {
        self.terms.blank(label, &mut self.deferred)
    }

    fn term_literal(&mut self, value: &str, datatype: Datatype, language: Option<&str>) -> TermId {
        self.terms.literal(value, datatype, language)
    }

    fn term_literal_value(&mut self, value: LiteralValue, datatype: Datatype) -> TermId {
        self.terms.literal_value(value, datatype)
    }

    /// Honor a producer's statement-scope declaration. This writer keeps the
    /// whole document in a `Graph` regardless — it is the document tier — but
    /// the term table need not be a second copy of it, and every triple is
    /// cloned into the graph before its slot can be reused.
    fn declare_term_scope(&mut self, scope: TermScope) {
        self.terms.set_scope(scope);
    }

    fn emit_triple(&mut self, subject: TermId, predicate: TermId, object: TermId) -> SinkResult {
        self.deferred.check()?;
        let triple = self.triple(subject, predicate, object);
        self.graph.add(triple);
        self.statements += 1;
        Ok(())
    }

    /// Indexed list items are kept, not refused.
    ///
    /// The text writers refuse this event because N-Triples has no way to say
    /// "position 2 of a list" — writing one triple where RDF means three would
    /// be data loss. JSON-LD has `@list`, and [`format_jsonld`] already groups
    /// indexed items into it, so here the event is expressible and keeping it
    /// is the lossless choice.
    fn emit_list_item(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        index: i32,
    ) -> SinkResult {
        self.deferred.check()?;
        let Triple { s, p, o, .. } = self.triple(subject, predicate, object);
        self.graph.add_list_item(s, p, o, index);
        self.statements += 1;
        Ok(())
    }

    fn end_statement(&mut self) {
        self.terms.end_statement();
        self.mark = self.graph.len();
    }

    fn abort_statement(&mut self) {
        self.terms.end_statement();
        // Nothing has been written yet, so this is a true rollback whatever
        // the configuration — the one writer where that is free.
        self.statements -= (self.graph.len() - self.mark) as u64;
        self.graph.truncate(self.mark);
        self.aborted += 1;
    }

    fn finish(&mut self) -> SinkResult {
        self.deferred.check()?;
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        let document = format_jsonld(&self.graph, &self.config);
        serde_json::to_writer_pretty(&mut self.out, &document)
            .map_err(|e| SinkError::Io(std::io::Error::other(e)))?;
        self.out.write_all(b"\n")?;
        self.out.finish()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value as JsonValue;

    fn write_jsonld(f: impl FnOnce(&mut JsonLdWriter<&mut Vec<u8>>)) -> JsonValue {
        let mut buf = Vec::new();
        let mut w = JsonLdWriter::new(&mut buf);
        f(&mut w);
        w.finish().unwrap();
        serde_json::from_slice(&buf).expect("the writer emits valid JSON")
    }

    #[test]
    fn a_graph_becomes_a_jsonld_document() {
        let doc = write_jsonld(|w| {
            let s = w.term_iri("http://ex/alice");
            let p = w.term_iri("http://ex/name");
            let o = w.term_literal("Alice", Datatype::xsd_string(), None);
            w.emit_triple(s, p, o).unwrap();
            w.end_statement();
        });
        let nodes = doc["@graph"].as_array().expect("an @graph array");
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0]["@id"], "http://ex/alice");
        assert_eq!(nodes[0]["http://ex/name"], "Alice");
    }

    /// See the note on N-Triples' equivalent: the capability probe is what a
    /// producer reads, and calling the trait default anyway trips its
    /// `debug_assert` first.
    #[test]
    fn jsonld_does_not_claim_quad_support() {
        let mut buf = Vec::new();
        let w = JsonLdWriter::new(&mut buf);
        assert!(!w.supports_quads());
    }

    /// The asymmetry with the text writers, pinned: JSON-LD can say `@list`,
    /// so it keeps the index rather than refusing the event.
    #[test]
    fn indexed_list_items_become_an_at_list() {
        let doc = write_jsonld(|w| {
            let s = w.term_iri("http://ex/s");
            let p = w.term_iri("http://ex/items");
            for (i, v) in ["a", "b", "c"].iter().enumerate() {
                let o = w.term_literal(v, Datatype::xsd_string(), None);
                w.emit_list_item(s, p, o, i as i32).unwrap();
            }
            w.end_statement();
        });
        let list = &doc["@graph"][0]["http://ex/items"]["@list"];
        assert_eq!(list.as_array().unwrap().len(), 3, "{doc}");
        assert_eq!(list[0], "a");
        assert_eq!(list[2], "c");
    }

    #[test]
    fn blank_labels_follow_the_writer_policy_not_the_formatter() {
        let doc = write_jsonld(|w| {
            let s = w.term_blank(Some("alice"));
            let p = w.term_iri("http://ex/knows");
            let o = w.term_blank(Some("bob"));
            w.emit_triple(s, p, o).unwrap();
            w.end_statement();
        });
        assert_eq!(doc["@graph"][0]["@id"], "_:b1", "{doc}");
        assert_eq!(doc["@graph"][0]["http://ex/knows"]["@id"], "_:b2", "{doc}");
    }

    #[test]
    fn an_aborted_statement_contributes_nothing() {
        let doc = write_jsonld(|w| {
            let s = w.term_iri("http://ex/kept");
            let p = w.term_iri("http://ex/p");
            let o = w.term_literal("1", Datatype::xsd_string(), None);
            w.emit_triple(s, p, o).unwrap();
            w.end_statement();

            let s = w.term_iri("http://ex/dropped");
            let o = w.term_literal("2", Datatype::xsd_string(), None);
            w.emit_triple(s, p, o).unwrap();
            w.abort_statement();
        });
        let nodes = doc["@graph"].as_array().unwrap();
        assert_eq!(nodes.len(), 1, "{doc}");
        assert_eq!(nodes[0]["@id"], "http://ex/kept");
    }

    #[test]
    fn finish_is_idempotent() {
        let mut buf = Vec::new();
        let mut w = JsonLdWriter::new(&mut buf);
        let s = w.term_iri("http://ex/s");
        let p = w.term_iri("http://ex/p");
        let o = w.term_literal("v", Datatype::xsd_string(), None);
        w.emit_triple(s, p, o).unwrap();
        w.end_statement();
        w.finish().unwrap();
        w.finish().unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert_eq!(text.matches("\"@graph\"").count(), 1, "{text}");
    }
}
