//! TriG: the Turtle parser's [`Dialect::TriG`].

use fluree_graph_ir::{Dataset, GraphCollectorSink, GraphSink, Term};
use fluree_graph_turtle::{parse_trig, parse_with_options, Dialect, ParserOptions};

const PREFIXES: &str = "@prefix ex: <http://ex/> .\n";

fn dataset(doc: &str) -> Dataset {
    let mut sink = GraphCollectorSink::with_named_graphs();
    parse_trig(&format!("{PREFIXES}{doc}"), &mut sink).unwrap_or_else(|e| panic!("{e}\n{doc}"));
    sink.into_dataset()
}

fn conformant(doc: &str) -> Dataset {
    let mut sink = GraphCollectorSink::with_named_graphs();
    let options = ParserOptions::conformant().with_dialect(Dialect::TriG);
    parse_with_options(&format!("{PREFIXES}{doc}"), &mut sink, options)
        .unwrap_or_else(|e| panic!("{e}\n{doc}"));
    sink.into_dataset()
}

fn ex(name: &str) -> Term {
    Term::iri(format!("http://ex/{name}"))
}

fn sizes(d: &Dataset) -> Vec<(Option<Term>, usize)> {
    d.graphs()
        .map(|(name, g)| (name.cloned(), g.len()))
        .collect()
}

#[test]
fn every_block_form_names_its_graph() {
    let d = dataset(
        "ex:a ex:p ex:o .
         GRAPH ex:g1 { ex:a ex:p ex:o }
         ex:g2 { ex:a ex:p ex:o . ex:b ex:p ex:o . }
         { ex:c ex:p ex:o }
         graph ex:g1 { ex:d ex:p ex:o }",
    );
    assert_eq!(
        sizes(&d),
        vec![(None, 2), (Some(ex("g1")), 2), (Some(ex("g2")), 2)]
    );
}

#[test]
fn a_blank_node_names_a_graph() {
    let d = dataset("_:g { ex:a ex:p ex:o } [] { ex:a ex:p ex:o }");
    assert_eq!(d.named.len(), 2);
    assert!(d
        .named
        .keys()
        .all(|name| matches!(name, Term::BlankNode(_))));
}

#[test]
fn turtle_productions_land_in_the_block_graph() {
    let d = conformant(
        "ex:g {
             [ ex:p ex:o ] ex:q ( 1 2 ) ;
                 ex:r ex:s ; .
             ex:a ex:knows ex:b {| ex:since 2020 |} .
             << ex:c ex:p ex:d >> .
             ex:e ex:says <<( ex:f ex:p ex:g )>>
         }",
    );
    assert!(d.default.is_empty(), "nothing leaks into the default graph");
    let g = &d.named[&ex("g")];
    // [ex:p ex:o], ex:q, two list cells x2, ex:r, ex:knows, ex:since, ex:says
    assert_eq!(g.len(), 10);
    assert_eq!(
        g.reifications().len(),
        2,
        "the annotation's and the bare reified triple's"
    );
}

#[test]
fn ingest_lists_are_indexed_items_in_the_graph() {
    let d = dataset("ex:g { ex:a ex:list ( ex:x ex:y ) }");
    let g = &d.named[&ex("g")];
    assert_eq!(g.len(), 2);
    assert!(g.iter().all(|t| t.list_index().is_some()));
}

#[test]
fn turtle_refuses_graph_blocks() {
    for doc in [
        "GRAPH ex:g { ex:a ex:p ex:o }",
        "ex:g { ex:a ex:p ex:o }",
        "{ ex:a ex:p ex:o }",
    ] {
        let mut sink = GraphCollectorSink::with_named_graphs();
        assert!(
            fluree_graph_turtle::parse(&format!("{PREFIXES}{doc}"), &mut sink).is_err(),
            "{doc}"
        );
    }
}

#[test]
fn a_triple_only_sink_refuses_a_named_graph_but_not_the_default_block() {
    let mut sink = GraphCollectorSink::new();
    let err = parse_trig(&format!("{PREFIXES}ex:g {{ ex:a ex:p ex:o }}"), &mut sink).unwrap_err();
    assert!(err.to_string().contains("named graph"), "{err}");
    assert!(sink.into_graph().is_empty());

    let mut sink = GraphCollectorSink::new();
    parse_trig(&format!("{PREFIXES}{{ ex:a ex:p ex:o }}"), &mut sink).unwrap();
    assert_eq!(sink.into_graph().len(), 1);
}

#[test]
fn a_failed_block_contributes_nothing() {
    let mut sink = GraphCollectorSink::with_named_graphs();
    let doc = format!("{PREFIXES}ex:g1 {{ ex:a ex:p ex:o }} ex:g2 {{ ex:a ex:p ex:o . ex:b }}");
    assert!(parse_trig(&doc, &mut sink).is_err());
    let d = sink.into_dataset();
    assert_eq!(sizes(&d), vec![(None, 0), (Some(ex("g1")), 1)]);
}

#[test]
fn a_block_commits_before_the_next_token_is_read() {
    // The `}` is the block's terminator; the bad token after it belongs to
    // the next statement, so the complete block must not be rolled back.
    let mut sink = GraphCollectorSink::with_named_graphs();
    let doc = format!("{PREFIXES}ex:g {{ ex:a ex:p ex:o }} \"unterminated");
    assert!(parse_trig(&doc, &mut sink).is_err());
    assert_eq!(sink.into_dataset().named[&ex("g")].len(), 1);
}

#[test]
fn blocks_do_not_nest_and_need_their_brace() {
    for doc in [
        "ex:g { ex:h { ex:a ex:p ex:o } }",
        "GRAPH ex:g { GRAPH ex:h { ex:a ex:p ex:o } }",
        "GRAPH ex:g ex:a ex:p ex:o .",
        "GRAPH [ ex:p ex:o ] { ex:a ex:p ex:o }",
        "ex:g { ex:a ex:p ex:o ",
        "ex:g { ex:a }",
        "ex:g { @prefix ex2: <http://ex2/> . }",
    ] {
        let mut sink = GraphCollectorSink::with_named_graphs();
        assert!(
            parse_trig(&format!("{PREFIXES}{doc}"), &mut sink).is_err(),
            "{doc}"
        );
    }
}

#[test]
fn the_sink_hears_one_statement_per_block() {
    #[derive(Default)]
    struct Counting {
        inner: Option<GraphCollectorSink>,
        ends: usize,
    }
    impl Counting {
        fn inner(&mut self) -> &mut GraphCollectorSink {
            self.inner
                .get_or_insert_with(GraphCollectorSink::with_named_graphs)
        }
    }
    impl GraphSink for Counting {
        fn on_base(&mut self, b: &str) {
            self.inner().on_base(b);
        }
        fn on_prefix(&mut self, p: &str, n: &str) {
            self.inner().on_prefix(p, n);
        }
        fn term_iri(&mut self, i: &str) -> fluree_graph_ir::TermId {
            self.inner().term_iri(i)
        }
        fn term_blank(&mut self, l: Option<&str>) -> fluree_graph_ir::TermId {
            self.inner().term_blank(l)
        }
        fn term_literal(
            &mut self,
            v: &str,
            d: fluree_graph_ir::Datatype,
            l: Option<&str>,
        ) -> fluree_graph_ir::TermId {
            self.inner().term_literal(v, d, l)
        }
        fn term_literal_value(
            &mut self,
            v: fluree_graph_ir::LiteralValue,
            d: fluree_graph_ir::Datatype,
        ) -> fluree_graph_ir::TermId {
            self.inner().term_literal_value(v, d)
        }
        fn emit_triple(
            &mut self,
            s: fluree_graph_ir::TermId,
            p: fluree_graph_ir::TermId,
            o: fluree_graph_ir::TermId,
        ) -> fluree_graph_ir::SinkResult {
            self.inner().emit_triple(s, p, o)
        }
        fn supports_quads(&self) -> bool {
            true
        }
        fn emit_quad(
            &mut self,
            s: fluree_graph_ir::TermId,
            p: fluree_graph_ir::TermId,
            o: fluree_graph_ir::TermId,
            g: fluree_graph_ir::TermId,
        ) -> fluree_graph_ir::SinkResult {
            self.inner().emit_quad(s, p, o, g)
        }
        fn end_statement(&mut self) {
            self.ends += 1;
            self.inner().end_statement();
        }
    }
    let mut sink = Counting::default();
    parse_trig(
        &format!("{PREFIXES}ex:g {{ ex:a ex:p ex:o . ex:b ex:p ex:o }} ex:c ex:p ex:o ."),
        &mut sink,
    )
    .unwrap();
    // The prefix directive, the block, and the triple.
    assert_eq!(sink.ends, 3);
}

#[test]
fn an_annotation_block_is_never_empty() {
    // `annotationBlock ::= '{|' predicateObjectList '|}'`, in Turtle and TriG.
    let mut sink = GraphCollectorSink::with_named_graphs();
    let turtle = format!("{PREFIXES}ex:s ex:p ex:o {{|  |}} .");
    assert!(fluree_graph_turtle::parse(&turtle, &mut sink).is_err());
    let mut sink = GraphCollectorSink::with_named_graphs();
    assert!(parse_trig(
        &format!("{PREFIXES}ex:g {{ ex:s ex:p ex:o {{| |}} }}"),
        &mut sink
    )
    .is_err());
    // One pair is enough.
    dataset("ex:g { ex:s ex:p ex:o {| ex:q ex:r |} }");
}
