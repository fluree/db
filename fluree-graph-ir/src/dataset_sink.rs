//! Quad-capable collecting sink
//!
//! [`DatasetCollectorSink`] is the [`GraphCollectorSink`](crate::GraphCollectorSink)
//! analog for named-graph syntaxes: it answers `true` to
//! [`GraphSink::supports_quads`] and routes each event to the graph it names.

use crate::sink::{GraphSink, SinkError, SinkResult, TermId};
use crate::term_table::TermTable;
use crate::{Dataset, Datatype, Graph, LiteralValue, Term, Triple};

/// A sink that collects quads into a [`Dataset`]
///
/// # Routing
///
/// - [`GraphSink::emit_quad`] appends to the named graph, creating it on
///   first use.
/// - [`GraphSink::emit_triple`] and [`GraphSink::emit_list_item`] append to
///   the default graph — the same events a triple-only producer emits, given
///   the same meaning they have in Turtle or N-Triples. That is what makes
///   the default graph of a dataset agree, statement for statement, with the
///   `Graph` a [`GraphCollectorSink`](crate::GraphCollectorSink) would build
///   from the identical event stream.
///
/// Term interning, statement-scoped literal recycling, and the statement
/// lifecycle are shared with `GraphCollectorSink` (see [`TermId`]); the only
/// thing this sink adds is graph routing and a rollback that spans graphs.
///
/// # Example
///
/// ```
/// use fluree_graph_ir::{Datatype, DatasetCollectorSink, GraphSink};
///
/// let mut sink = DatasetCollectorSink::new();
/// assert!(sink.supports_quads());
///
/// let s = sink.term_iri("http://example.org/alice");
/// let p = sink.term_iri("http://xmlns.com/foaf/0.1/name");
/// let o = sink.term_literal("Alice", Datatype::xsd_string(), None);
/// let g = sink.term_iri("http://example.org/people");
///
/// sink.emit_quad(s, p, o, g).unwrap();
/// sink.end_statement();
/// sink.finish().unwrap();
///
/// let dataset = sink.into_dataset();
/// assert_eq!(dataset.len(), 1);
/// assert_eq!(dataset.named_graph_count(), 1);
/// assert!(dataset.default_graph().is_empty());
/// ```
#[derive(Debug)]
pub struct DatasetCollectorSink {
    /// The dataset being built
    dataset: Dataset,
    /// Terms indexed by TermId, with statement-scoped literal recycling
    terms: TermTable,
    /// Default-graph length at the current statement's start — the rewind
    /// point for [`GraphSink::abort_statement`].
    default_mark: usize,
    /// Rewind points for the named graphs this statement has touched, in
    /// first-touch order.
    ///
    /// The `Option<usize>` is the graph's length when the statement first
    /// touched it, or `None` if the graph did not exist at all — in which
    /// case an abort must *remove* it, not leave an empty graph the statement
    /// conjured. Keyed by [`TermId`] rather than by term so that recording a
    /// mark allocates nothing; two ids can denote the same graph, which
    /// [`Self::abort_statement`] handles by rewinding in reverse order.
    named_marks: Vec<(TermId, Option<usize>)>,
}

impl DatasetCollectorSink {
    /// Create a new dataset collector sink
    pub fn new() -> Self {
        Self {
            dataset: Dataset::new(),
            terms: TermTable::new(),
            default_mark: 0,
            named_marks: Vec::new(),
        }
    }

    /// Create a sink with a pre-configured base IRI
    pub fn with_base(base: impl Into<String>) -> Self {
        Self {
            dataset: Dataset::with_base(base),
            terms: TermTable::new(),
            default_mark: 0,
            named_marks: Vec::new(),
        }
    }

    /// Consume the sink and return the collected dataset.
    ///
    /// This is the sink's *product*, distinct from the protocol's
    /// [`GraphSink::finish`] (flush/finalize) — see the trait docs.
    pub fn into_dataset(self) -> Dataset {
        self.dataset
    }

    /// Get the current dataset (non-consuming)
    pub fn dataset(&self) -> &Dataset {
        &self.dataset
    }

    /// Get the current dataset mutably
    pub fn dataset_mut(&mut self) -> &mut Dataset {
        &mut self.dataset
    }

    /// Get a term by its ID
    fn get_term(&self, id: TermId) -> &Term {
        self.terms.get(id)
    }

    /// Note the rewind point for a named graph the statement in flight is
    /// about to write to, if this is its first write through this id.
    fn mark_named_graph(&mut self, graph: TermId) {
        if self.named_marks.iter().any(|(id, _)| *id == graph) {
            return;
        }
        let existing = self
            .dataset
            .named_graph(self.terms.get(graph))
            .map(Graph::len);
        self.named_marks.push((graph, existing));
    }
}

impl Default for DatasetCollectorSink {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphSink for DatasetCollectorSink {
    fn on_base(&mut self, base_iri: &str) {
        self.dataset.set_base(base_iri);
    }

    fn on_prefix(&mut self, prefix: &str, namespace_iri: &str) {
        self.dataset.add_prefix(prefix, namespace_iri);
    }

    fn term_iri(&mut self, iri: &str) -> TermId {
        self.terms.iri(iri)
    }

    fn term_blank(&mut self, label: Option<&str>) -> TermId {
        self.terms.blank(label)
    }

    fn term_literal(&mut self, value: &str, datatype: Datatype, language: Option<&str>) -> TermId {
        self.terms.literal(value, datatype, language)
    }

    fn term_literal_value(&mut self, value: LiteralValue, datatype: Datatype) -> TermId {
        self.terms.literal_value(value, datatype)
    }

    fn supports_quads(&self) -> bool {
        true
    }

    fn emit_triple(&mut self, subject: TermId, predicate: TermId, object: TermId) -> SinkResult {
        let s = self.get_term(subject).clone();
        let p = self.get_term(predicate).clone();
        let o = self.get_term(object).clone();
        self.dataset.default_graph_mut().add(Triple::new(s, p, o));
        Ok(())
    }

    /// Append an indexed list element to the default graph.
    ///
    /// This event carries no graph term, so the default graph is the only
    /// thing it can mean — it is not a routing choice this sink makes. A
    /// producer that wants an indexed collection inside a NAMED graph now
    /// says so with [`GraphSink::emit_quad_list_item`]; before that event
    /// existed it had no way to, and the item landed here, which is silent
    /// cross-graph data movement.
    ///
    /// The reasoning that used to live here — "a gap in the protocol, narrow
    /// because the spine style routes through `emit_quad` anyway" — is
    /// retired by that addition. What remains true is the narrowness: under
    /// the spine style a collection is ordinary triples, so only the indexed
    /// style (Fluree ingest) ever reaches either list-item event.
    fn emit_list_item(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        index: i32,
    ) -> SinkResult {
        let s = self.get_term(subject).clone();
        let p = self.get_term(predicate).clone();
        let o = self.get_term(object).clone();
        self.dataset
            .default_graph_mut()
            .add_list_item(s, p, o, index);
        Ok(())
    }

    /// Append an indexed list element to `graph`, creating it on first use.
    ///
    /// The quad sibling of `emit_list_item`. It refuses a literal graph name
    /// for the same reason [`Self::emit_quad`] does: the dataset keys graphs
    /// by that term and no syntax can serialize a literal there.
    fn emit_quad_list_item(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        index: i32,
        graph: TermId,
    ) -> SinkResult {
        if self.get_term(graph).is_literal() {
            return Err(SinkError::rejected(
                "a literal cannot name a graph; graph names are IRIs or blank nodes",
            ));
        }

        let s = self.get_term(subject).clone();
        let p = self.get_term(predicate).clone();
        let o = self.get_term(object).clone();

        self.mark_named_graph(graph);
        let name = self.terms.get(graph);
        self.dataset
            .graph_mut(Some(name))
            .add_list_item(s, p, o, index);
        Ok(())
    }

    /// Append the triple to `graph`, creating that graph on first use.
    ///
    /// A literal graph name is refused rather than stored: the dataset keys
    /// its graphs by this term, and no dataset syntax can serialize a literal
    /// there, so accepting one would build a dataset that cannot be written
    /// back out. Refusing is loud; storing it would be silent corruption.
    fn emit_quad(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        graph: TermId,
    ) -> SinkResult {
        if self.get_term(graph).is_literal() {
            return Err(SinkError::rejected(
                "a literal cannot name a graph; graph names are IRIs or blank nodes",
            ));
        }

        let s = self.get_term(subject).clone();
        let p = self.get_term(predicate).clone();
        let o = self.get_term(object).clone();

        self.mark_named_graph(graph);
        let name = self.terms.get(graph);
        self.dataset.graph_mut(Some(name)).add(Triple::new(s, p, o));
        Ok(())
    }

    /// Retire this statement's literal slots for reuse, and move every rewind
    /// point past the statement just committed.
    fn end_statement(&mut self) {
        self.terms.end_statement();
        self.default_mark = self.dataset.default_graph().len();
        self.named_marks.clear();
    }

    /// Roll every graph the statement touched back to where it started, so a
    /// statement that failed mid-emit contributes nothing — including no
    /// empty named graph that only its own failed writes brought into
    /// existence.
    ///
    /// Statements only. A `@base` or `@prefix` accepted before the failure
    /// stays accepted, exactly as in
    /// [`GraphCollectorSink`](crate::GraphCollectorSink): directives are
    /// document-scoped, `on_base`/`on_prefix` are infallible, and a producer
    /// that has resolved later IRIs against a prefix cannot un-resolve them.
    /// A `--continue-on-error` driver inherits that.
    fn abort_statement(&mut self) {
        self.terms.end_statement();
        self.dataset.default_graph_mut().truncate(self.default_mark);

        // Reverse order so that when one statement reached the same graph
        // through two ids, the earliest (smallest) mark is applied last and
        // therefore wins.
        for i in (0..self.named_marks.len()).rev() {
            let (id, mark) = self.named_marks[i];
            let name = self.terms.get(id);
            match mark {
                Some(len) => {
                    if let Some(graph) = self.dataset.named_graph_mut(name) {
                        graph.truncate(len);
                    }
                }
                None => {
                    self.dataset.remove_named_graph(name);
                }
            }
        }
        self.named_marks.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GraphCollectorSink, Quad};

    /// The event that closed the protocol's cross-graph gap: an indexed list
    /// item inside a named graph lands in THAT graph, not the default one.
    /// Before `emit_quad_list_item` existed a producer had no way to say
    /// which graph it meant, and the item silently moved to the default
    /// graph.
    #[test]
    fn an_indexed_list_item_lands_in_its_named_graph() {
        let mut sink = DatasetCollectorSink::new();
        let s = sink.term_iri("http://a/s");
        let p = sink.term_iri("http://a/p");
        let o = sink.term_literal("first", Datatype::xsd_string(), None);
        let g = sink.term_iri("http://a/g");
        sink.emit_quad_list_item(s, p, o, 0, g).unwrap();
        sink.end_statement();

        let dataset = sink.into_dataset();
        assert_eq!(
            dataset.default_graph().len(),
            0,
            "the item must NOT land in the default graph"
        );
        let named = dataset.named_graph(&Term::iri("http://a/g")).unwrap();
        assert_eq!(named.len(), 1);
        assert_eq!(named.iter().next().unwrap().list_index, Some(0));
    }

    /// The triple form still means the default graph — it carries no graph
    /// term, so there is nothing else it could mean.
    #[test]
    fn the_triple_form_of_a_list_item_still_means_the_default_graph() {
        let mut sink = DatasetCollectorSink::new();
        let s = sink.term_iri("http://a/s");
        let p = sink.term_iri("http://a/p");
        let o = sink.term_literal("first", Datatype::xsd_string(), None);
        sink.emit_list_item(s, p, o, 0).unwrap();
        sink.end_statement();

        assert_eq!(sink.into_dataset().default_graph().len(), 1);
    }

    /// A literal cannot name a graph, in the indexed form either.
    #[test]
    fn a_literal_graph_name_is_refused_for_indexed_items_too() {
        let mut sink = DatasetCollectorSink::new();
        let s = sink.term_iri("http://a/s");
        let p = sink.term_iri("http://a/p");
        let o = sink.term_iri("http://a/o");
        let g = sink.term_literal("not a graph", Datatype::xsd_string(), None);
        assert!(sink.emit_quad_list_item(s, p, o, 0, g).is_err());
    }

    /// A triple-only sink refuses the indexed quad form rather than
    /// relocating it — the same fail-closed posture `emit_quad` takes.
    #[test]
    fn a_triple_only_sink_refuses_the_indexed_quad_form() {
        // `supports_quads()` is false here, so a correct producer never calls
        // this. The backstop is what keeps a BUGGY one from losing the graph
        // name, so it is what this test exercises — via the trait default,
        // reached through a sink that claims the capability it cannot honor.
        struct Claims(GraphCollectorSink);
        impl GraphSink for Claims {
            fn term_iri(&mut self, iri: &str) -> TermId {
                self.0.term_iri(iri)
            }
            fn term_blank(&mut self, label: Option<&str>) -> TermId {
                self.0.term_blank(label)
            }
            fn term_literal(
                &mut self,
                value: &str,
                datatype: Datatype,
                language: Option<&str>,
            ) -> TermId {
                self.0.term_literal(value, datatype, language)
            }
            fn term_literal_value(&mut self, value: LiteralValue, datatype: Datatype) -> TermId {
                self.0.term_literal_value(value, datatype)
            }
            fn emit_triple(&mut self, s: TermId, p: TermId, o: TermId) -> SinkResult {
                self.0.emit_triple(s, p, o)
            }
            fn on_prefix(&mut self, prefix: &str, namespace: &str) {
                self.0.on_prefix(prefix, namespace);
            }
            fn on_base(&mut self, base: &str) {
                self.0.on_base(base);
            }
            fn supports_quads(&self) -> bool {
                true
            }
        }

        let mut sink = Claims(GraphCollectorSink::new());
        let s = sink.term_iri("http://a/s");
        let p = sink.term_iri("http://a/p");
        let o = sink.term_iri("http://a/o");
        let g = sink.term_iri("http://a/g");
        let err = sink
            .emit_quad_list_item(s, p, o, 0, g)
            .expect_err("the default body must refuse, not silently drop the graph");
        assert!(format!("{err}").contains("named graph"), "{err}");
    }

    /// Hand-fed event sequence: no TriG parser exists in the light crates
    /// yet, so the tests drive the protocol directly, in the order a
    /// statement-oriented producer would.
    fn quad(
        sink: &mut DatasetCollectorSink,
        s: &str,
        p: &str,
        o: &str,
        g: Option<&str>,
    ) -> SinkResult {
        let s = sink.term_iri(s);
        let p = sink.term_iri(p);
        let o = sink.term_literal(o, Datatype::xsd_string(), None);
        let result = match g {
            Some(g) => {
                let g = sink.term_iri(g);
                sink.emit_quad(s, p, o, g)
            }
            None => sink.emit_triple(s, p, o),
        };
        if result.is_ok() {
            sink.end_statement();
        }
        result
    }

    #[test]
    fn quads_round_trip_through_the_sink() {
        let mut sink = DatasetCollectorSink::new();
        assert!(sink.supports_quads());

        quad(&mut sink, "http://ex/s", "http://ex/p", "d", None).unwrap();
        quad(
            &mut sink,
            "http://ex/s",
            "http://ex/p",
            "a",
            Some("http://ex/g1"),
        )
        .unwrap();
        quad(
            &mut sink,
            "http://ex/s",
            "http://ex/p",
            "b",
            Some("http://ex/g2"),
        )
        .unwrap();
        quad(
            &mut sink,
            "http://ex/s",
            "http://ex/p",
            "c",
            Some("http://ex/g1"),
        )
        .unwrap();
        sink.finish().unwrap();

        let dataset = sink.into_dataset();
        assert_eq!(dataset.len(), 4);
        assert_eq!(dataset.named_graph_count(), 2);

        let expected = |o: &str, g: Option<&str>| {
            let triple = Triple::new(
                Term::iri("http://ex/s"),
                Term::iri("http://ex/p"),
                Term::string(o),
            );
            Quad::new(triple, g.map(Term::iri))
        };
        let quads: Vec<Quad> = dataset.into_quads().collect();
        assert_eq!(
            quads,
            vec![
                expected("d", None),
                expected("a", Some("http://ex/g1")),
                expected("c", Some("http://ex/g1")),
                expected("b", Some("http://ex/g2")),
            ]
        );
    }

    /// A TriG document's event shape: directives, statements outside any
    /// block, a `<g> { … }` block holding several statements, then more
    /// statements back outside it. No TriG parser exists in the light crates
    /// yet, so the sequence is hand-fed in the order such a parser would
    /// produce it.
    #[test]
    fn a_trig_shaped_event_sequence_lands_where_the_document_says() {
        let mut sink = DatasetCollectorSink::new();
        sink.on_base("http://example.org/");
        sink.on_prefix("ex", "http://example.org/");

        // ex:a ex:p "outside-1" .
        quad(&mut sink, "http://ex/a", "http://ex/p", "outside-1", None).unwrap();

        // ex:g { ex:b ex:p "inside-1" . ex:c ex:p "inside-2" . }
        quad(
            &mut sink,
            "http://ex/b",
            "http://ex/p",
            "inside-1",
            Some("http://ex/g"),
        )
        .unwrap();
        quad(
            &mut sink,
            "http://ex/c",
            "http://ex/p",
            "inside-2",
            Some("http://ex/g"),
        )
        .unwrap();

        // ex:d ex:p "outside-2" .
        quad(&mut sink, "http://ex/d", "http://ex/p", "outside-2", None).unwrap();
        sink.finish().unwrap();

        let dataset = sink.into_dataset();
        assert_eq!(dataset.base(), Some("http://example.org/"));
        assert_eq!(
            dataset.prefixes().get("ex"),
            Some(&"http://example.org/".to_string()),
            "directives are document-scoped, not per-graph"
        );

        assert_eq!(
            dataset.default_graph().len(),
            2,
            "the two outside the block"
        );
        let inside = dataset
            .named_graph(&Term::iri("http://ex/g"))
            .expect("the block's graph");
        assert_eq!(inside.len(), 2);
        assert_eq!(inside.base, None, "no directives leaked into the block");

        // Re-entering the default graph after the block appends, and does not
        // start a second default graph.
        let subjects: Vec<&str> = dataset
            .default_graph()
            .iter()
            .filter_map(|t| t.s.as_iri())
            .collect();
        assert_eq!(subjects, vec!["http://ex/a", "http://ex/d"]);
    }

    #[test]
    fn list_items_land_in_the_default_graph_because_the_protocol_has_no_quad_form() {
        // What this guarantees, precisely: the ROUTING of today's
        // `emit_list_item` is pinned, so it cannot start going somewhere else
        // unnoticed. What it does NOT do is notice a new quad-shaped event
        // being added to the trait — a default-bodied `emit_quad_list_item`
        // would compile and this test would stay green. That tripwire is
        // `PROTOCOL_QUAD_EVENTS` in sink.rs, and it is a forced decision
        // point rather than a detector; see the note there.
        let mut sink = DatasetCollectorSink::new();
        let s = sink.term_iri("http://ex/s");
        let p = sink.term_iri("http://ex/p");
        let o = sink.term_literal("first", Datatype::xsd_string(), None);
        sink.emit_list_item(s, p, o, 0).unwrap();
        sink.end_statement();

        let dataset = sink.into_dataset();
        assert_eq!(dataset.named_graph_count(), 0);
        assert_eq!(dataset.default_graph().len(), 1);
        assert!(dataset
            .default_graph()
            .iter()
            .next()
            .unwrap()
            .is_list_element());
    }

    #[test]
    fn a_triple_only_event_stream_builds_the_same_default_graph() {
        // The equivalence that lets one parser feed either sink: given the
        // events a triple-only syntax produces, the dataset's default graph
        // must be indistinguishable from the collector's graph.
        let mut dataset_sink = DatasetCollectorSink::new();
        let mut graph_sink = GraphCollectorSink::new();

        fn drive<S: GraphSink>(sink: &mut S) {
            sink.on_base("http://example.org/");
            sink.on_prefix("ex", "http://example.org/");
            for i in 0..8 {
                let s = sink.term_iri(&format!("http://ex/s{i}"));
                let p = sink.term_iri("http://ex/p");
                let o = sink.term_literal(&format!("v{i}"), Datatype::xsd_string(), None);
                sink.emit_triple(s, p, o).unwrap();
                let blank = sink.term_blank(None);
                let labeled = sink.term_blank(Some("shared"));
                sink.emit_triple(blank, p, labeled).unwrap();
                let item = sink.term_literal("item", Datatype::xsd_string(), None);
                sink.emit_list_item(s, p, item, i).unwrap();
                sink.end_statement();
            }
            sink.finish().unwrap();
        }

        drive(&mut dataset_sink);
        drive(&mut graph_sink);

        let dataset = dataset_sink.into_dataset();
        let graph = graph_sink.into_graph();

        assert_eq!(dataset.named_graph_count(), 0);
        assert_eq!(dataset.default_graph().triples(), graph.triples());
        assert_eq!(dataset.base(), graph.base.as_deref());
        assert_eq!(dataset.prefixes(), &graph.prefixes);
    }

    #[test]
    fn literal_slots_recycle_exactly_as_the_graph_collector_does() {
        let mut sink = DatasetCollectorSink::new();
        let a = sink.term_literal("a", Datatype::xsd_string(), None);
        let b = sink.term_literal("b", Datatype::xsd_string(), None);
        assert_ne!(a, b, "literals within one statement must stay distinct");

        sink.end_statement();

        let c = sink.term_literal("c", Datatype::xsd_string(), None);
        let d = sink.term_literal("d", Datatype::xsd_string(), None);
        assert_eq!((a, b), (c, d), "the next statement reuses the same slots");
    }

    #[test]
    fn the_term_table_tracks_the_widest_statement_not_the_document() {
        let mut sink = DatasetCollectorSink::new();
        let s = sink.term_iri("http://ex/s");
        let p = sink.term_iri("http://ex/p");
        let g = sink.term_iri("http://ex/g");
        for i in 0..1000 {
            let a = sink.term_literal(&format!("a{i}"), Datatype::xsd_string(), None);
            let b = sink.term_literal(&format!("b{i}"), Datatype::xsd_string(), None);
            sink.emit_quad(s, p, a, g).unwrap();
            sink.emit_quad(s, p, b, g).unwrap();
            sink.end_statement();
        }
        // 3 IRIs + 2 recycled literal slots — the same high-water-mark
        // property `GraphCollectorSink` has.
        assert_eq!(sink.terms.len(), 5);
        assert_eq!(sink.into_dataset().len(), 2000);
    }

    #[test]
    fn recycling_does_not_disturb_already_emitted_quads() {
        let mut sink = DatasetCollectorSink::new();
        let s = sink.term_iri("http://ex/s");
        let p = sink.term_iri("http://ex/p");
        let g = sink.term_iri("http://ex/g");

        let first = sink.term_literal("first", Datatype::xsd_string(), None);
        sink.emit_quad(s, p, first, g).unwrap();
        sink.end_statement();

        let second = sink.term_literal("second", Datatype::xsd_string(), None);
        assert_eq!(first, second, "slot reused — the aliasing case under test");
        sink.emit_quad(s, p, second, g).unwrap();
        sink.end_statement();

        let dataset = sink.into_dataset();
        let objects: Vec<String> = dataset
            .quads()
            .map(|(_, t)| match &t.o {
                Term::Literal { value, .. } => value.lexical(),
                other => panic!("expected literal, got {other:?}"),
            })
            .collect();
        assert_eq!(objects, vec!["first".to_string(), "second".to_string()]);
    }

    #[test]
    fn anonymous_mints_are_disjoint_from_user_written_labels() {
        // Same `-b{N}` namespace discipline as `GraphCollectorSink`: a bare
        // `b1` mint would silently merge with a document's own `_:b1`.
        let mut sink = DatasetCollectorSink::new();
        let user = sink.term_blank(Some("b1"));
        let minted = sink.term_blank(None);
        assert_ne!(user, minted);

        let label_of = |sink: &DatasetCollectorSink, id: TermId| match sink.get_term(id) {
            Term::BlankNode(b) => b.as_str().to_string(),
            other => panic!("expected blank, got {other:?}"),
        };
        assert_eq!(label_of(&sink, user), "b1");
        let minted_label = label_of(&sink, minted);
        assert_eq!(minted_label, "-b1");
        assert!(
            !minted_label.starts_with(|c: char| c.is_alphanumeric() || c == '_'),
            "a mint that can lex as BLANK_NODE_LABEL can collide: {minted_label}"
        );
    }

    #[test]
    fn a_literal_graph_name_is_refused_not_stored() {
        let mut sink = DatasetCollectorSink::new();
        let s = sink.term_iri("http://ex/s");
        let p = sink.term_iri("http://ex/p");
        let o = sink.term_iri("http://ex/o");
        let g = sink.term_literal("not a graph", Datatype::xsd_string(), None);

        let err = sink
            .emit_quad(s, p, o, g)
            .expect_err("a literal cannot key a graph");
        assert!(matches!(err, SinkError::Rejected(_)), "{err:?}");
        assert!(err.to_string().contains("name a graph"), "{err}");
        assert!(sink.into_dataset().is_empty(), "nothing was stored");
    }

    #[test]
    fn a_blank_node_may_name_a_graph() {
        let mut sink = DatasetCollectorSink::new();
        let s = sink.term_iri("http://ex/s");
        let p = sink.term_iri("http://ex/p");
        let o = sink.term_iri("http://ex/o");
        let g = sink.term_blank(Some("g0"));

        sink.emit_quad(s, p, o, g).unwrap();
        sink.end_statement();

        let dataset = sink.into_dataset();
        assert_eq!(dataset.named_graph_count(), 1);
        assert!(dataset.contains_named_graph(&Term::blank("g0")));
    }

    #[test]
    fn an_aborted_statement_contributes_nothing_to_any_graph() {
        let mut sink = DatasetCollectorSink::new();

        // A committed statement in `g1` — the floor an abort must not eat.
        quad(
            &mut sink,
            "http://ex/s",
            "http://ex/p",
            "keep",
            Some("http://ex/g1"),
        )
        .unwrap();

        // Now a statement that writes into the default graph, back into the
        // pre-existing `g1`, and into a brand-new `g2` — then fails.
        let s = sink.term_iri("http://ex/s");
        let p = sink.term_iri("http://ex/p");
        let g1 = sink.term_iri("http://ex/g1");
        let g2 = sink.term_iri("http://ex/g2");
        let o = sink.term_literal("dropped", Datatype::xsd_string(), None);
        sink.emit_triple(s, p, o).unwrap();
        sink.emit_quad(s, p, o, g1).unwrap();
        sink.emit_quad(s, p, o, g2).unwrap();
        sink.abort_statement();

        let dataset = sink.dataset();
        assert!(dataset.default_graph().is_empty(), "default graph rewound");
        assert_eq!(
            dataset
                .named_graph(&Term::iri("http://ex/g1"))
                .unwrap()
                .len(),
            1
        );
        assert!(
            !dataset.contains_named_graph(&Term::iri("http://ex/g2")),
            "a graph only the aborted statement created must not survive"
        );

        // And the sink keeps working: the next statement commits normally.
        quad(
            &mut sink,
            "http://ex/s",
            "http://ex/p",
            "after",
            Some("http://ex/g2"),
        )
        .unwrap();
        assert_eq!(sink.dataset().len(), 2);
    }

    #[test]
    fn abort_rewinds_to_the_earliest_mark_when_one_graph_is_reached_twice() {
        // Two `term_iri` calls for the same IRI produce two distinct ids, so
        // one statement can mark the same graph twice. The earliest mark has
        // to win, or the rollback under-rewinds and leaves a phantom quad.
        let mut sink = DatasetCollectorSink::new();
        quad(
            &mut sink,
            "http://ex/s",
            "http://ex/p",
            "keep",
            Some("http://ex/g"),
        )
        .unwrap();

        let s = sink.term_iri("http://ex/s");
        let p = sink.term_iri("http://ex/p");
        let g_a = sink.term_iri("http://ex/g");
        let g_b = sink.term_iri("http://ex/g");
        assert_ne!(g_a, g_b, "two ids, one graph — the case under test");

        let o = sink.term_literal("dropped", Datatype::xsd_string(), None);
        sink.emit_quad(s, p, o, g_a).unwrap();
        sink.emit_quad(s, p, o, g_b).unwrap();
        sink.abort_statement();

        let dataset = sink.into_dataset();
        assert_eq!(
            dataset.len(),
            1,
            "both writes rolled back, the earlier quad kept"
        );
    }

    #[test]
    fn an_empty_named_graph_that_predates_the_statement_survives_an_abort() {
        let mut sink = DatasetCollectorSink::new();
        // `<g> {}` — the graph exists with zero statements before the
        // statement that fails.
        sink.dataset_mut()
            .graph_mut(Some(&Term::iri("http://ex/g")));
        sink.end_statement();

        let s = sink.term_iri("http://ex/s");
        let p = sink.term_iri("http://ex/p");
        let o = sink.term_iri("http://ex/o");
        let g = sink.term_iri("http://ex/g");
        sink.emit_quad(s, p, o, g).unwrap();
        sink.abort_statement();

        let dataset = sink.into_dataset();
        assert!(dataset.is_empty(), "the quad is gone");
        assert_eq!(
            dataset.named_graph_count(),
            1,
            "but the graph the statement did not create stays"
        );
    }

    #[test]
    fn end_statement_moves_the_rewind_points_forward() {
        let mut sink = DatasetCollectorSink::new();
        quad(
            &mut sink,
            "http://ex/s",
            "http://ex/p",
            "a",
            Some("http://ex/g"),
        )
        .unwrap();
        quad(&mut sink, "http://ex/s", "http://ex/p", "b", None).unwrap();

        // Nothing is in flight, so an abort here has nothing to roll back.
        sink.abort_statement();
        assert_eq!(sink.into_dataset().len(), 2);
    }

    #[test]
    fn protocol_finish_leaves_the_product_alone() {
        let mut sink = DatasetCollectorSink::new();
        quad(
            &mut sink,
            "http://ex/s",
            "http://ex/p",
            "a",
            Some("http://ex/g"),
        )
        .unwrap();

        sink.finish().expect("default finish is infallible");
        sink.finish().expect("and idempotent");

        assert_eq!(sink.into_dataset().len(), 1);
    }

    #[test]
    fn reified_triples_stay_unsupported() {
        // Quad support and reification support are independent capabilities;
        // claiming quads must not imply the other.
        let sink = DatasetCollectorSink::new();
        assert!(sink.supports_quads());
        assert!(!sink.supports_reified_triples());
    }
}
