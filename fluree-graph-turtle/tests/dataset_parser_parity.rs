//! Default-graph parity under the REAL parser, authored by the review of
//! this branch and adopted verbatim.
//!
//! The in-crate equivalence test hand-feeds a tidy event stream; this drives
//! the actual #1552 Turtle parser — `committed_current` lifecycle,
//! abort-after-emit, one-token lookahead — into both sinks and demands they
//! agree. That is the load-bearing version of the claim, because the parser
//! is what decides when `end_statement` and `abort_statement` fire, and seven
//! abort shapes × two collection modes is where a routing or rollback
//! difference between the sinks would actually surface.

use fluree_graph_ir::{Dataset, DatasetCollectorSink, Graph, GraphCollectorSink};
use fluree_graph_turtle::{parse, parse_with_options, CollectionStyle, ParserOptions};

const HOSTILE: &str = r#"
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:wide ex:a "l1" ; ex:b "l2"@en ; ex:c "l3"^^xsd:token ; ex:d 1 ; ex:e 2.5 ;
        ex:f 3.0e0 ; ex:g true ; ex:h "l1" , "l1" , "l2" .
ex:narrow ex:a "only" .
ex:outer ex:has [ ex:x "inner1" ; ex:y [ ex:z "inner2" ; ex:w "inner3" ] ] ;
         ex:tail "outer-tail" .
ex:coll ex:items ( "c1" "c2" "c3" ) .
ex:nest ex:items ( "n1" ( "n2" "n3" ) "n4" ) .
( "s1" "s2" ) ex:p "s-obj" .
ex:empty ex:e () .
ex:wide2 ex:a "z1" ; ex:b "z2" ; ex:c "z3" ; ex:d "z4" ; ex:e "z5" ; ex:f "z6" .
ex:r1 ex:p "same" .
ex:r2 ex:p "same" .
ex:r3 ex:p "same" , "same" , "same" .
_:b0 ex:p "labeled0" .
_:b1 ex:p "labeled1" ; ex:q [ ex:r "anon-in-labeled" ] .
"#;

const TWO_GOOD_THEN: &str = r#"@prefix ex: <http://example.org/> .
                 ex:a ex:p "1" ; ex:q "2" .
"#;

fn rows(g: &Graph) -> Vec<String> {
    let mut v: Vec<String> = g
        .iter()
        .map(|t| format!("{} {} {} [{:?}]", t.s, t.p, t.o, t.list_index()))
        .collect();
    v.sort();
    v
}

fn dataset_rows(d: &Dataset) -> Vec<String> {
    rows(d.default_graph())
}

fn assert_parity(doc: &str, options: ParserOptions, must_fail: bool) {
    let mut graph_sink = GraphCollectorSink::new();
    let g_res = parse_with_options(doc, &mut graph_sink, options);

    let mut dataset_sink = DatasetCollectorSink::new();
    let d_res = parse_with_options(doc, &mut dataset_sink, options);

    assert_eq!(
        g_res.is_err(),
        must_fail,
        "graph sink outcome unexpected for:\n{doc}"
    );
    assert_eq!(
        g_res.is_err(),
        d_res.is_err(),
        "the two sinks disagreed on whether the parse failed:\n{doc}"
    );

    let d = dataset_sink.into_dataset();
    assert_eq!(
        d.named_graph_count(),
        0,
        "a triple-only document must create no named graph"
    );
    assert_eq!(
        dataset_rows(&d),
        rows(&graph_sink.into_graph()),
        "default graph diverged from the collector's graph for:\n{doc}"
    );
}

#[test]
fn hostile_document_parity_all_modes() {
    assert_parity(HOSTILE, ParserOptions::default(), false);
    assert_parity(HOSTILE, ParserOptions::conformant(), false);
    assert_parity(
        HOSTILE,
        ParserOptions::new().with_collections(CollectionStyle::Spine),
        false,
    );
}

/// The abort path: a statement that fails AFTER emitting. Both sinks must
/// roll it back identically.
#[test]
fn abort_after_emit_parity() {
    let docs = [
        // lexical error at the second object, mid-statement
        format!("{TWO_GOOD_THEN}                 ex:b ex:p \"3\" ; ex:q @@@ ."),
        // missing final dot
        format!("{TWO_GOOD_THEN}                 ex:b ex:p \"3\" ; ex:q \"4\""),
        // wrong terminator
        format!("{TWO_GOOD_THEN}                 ex:b ex:p \"3\" ; ex:q \"4\" ;"),
        // failure before any emit
        format!("{TWO_GOOD_THEN}                 @@@ ex:p \"3\" ."),
        // literal-suffix lookahead failure
        format!("{TWO_GOOD_THEN}                 ex:b ex:p \"3\"^^ @@@ ."),
        // nested descent then failure
        format!("{TWO_GOOD_THEN}                 ex:b ex:p [ ex:q \"deep\" ; ex:r @@@ ] ."),
        // collection then failure
        format!("{TWO_GOOD_THEN}                 ex:b ex:p ( \"a\" \"b\" ) ; ex:q @@@ ."),
    ];
    for doc in &docs {
        assert_parity(doc, ParserOptions::default(), true);
        assert_parity(doc, ParserOptions::conformant(), true);
    }
}

/// The bulk-import shape: ONE sink, many parse() calls — slots retired by the
/// last statement of chunk N are handed out again in chunk N+1.
#[test]
fn many_parses_one_sink_parity() {
    let chunks = [
        r#"@prefix ex: <http://example.org/> . ex:a ex:p "1" ; ex:q "2" ; ex:r "3" ."#,
        r#"@prefix ex: <http://example.org/> . ex:b ex:p "4" ."#,
        r#"@prefix ex: <http://example.org/> . ex:c ex:p "5" ; ex:q "6" ; ex:r "7" ; ex:s "8" ."#,
        HOSTILE,
    ];
    let mut graph_sink = GraphCollectorSink::new();
    let mut dataset_sink = DatasetCollectorSink::new();
    for c in chunks {
        parse(c, &mut graph_sink).unwrap();
        parse(c, &mut dataset_sink).unwrap();
    }
    let d = dataset_sink.into_dataset();
    assert_eq!(d.named_graph_count(), 0);
    assert_eq!(dataset_rows(&d), rows(&graph_sink.into_graph()));
}
