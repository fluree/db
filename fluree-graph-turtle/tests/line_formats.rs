//! The strict N-Triples and N-Quads reader.

use fluree_graph_ir::{GraphCollectorSink, Term};
use fluree_graph_turtle::{parse_nquads, parse_ntriples};

fn nquads(doc: &str) -> fluree_graph_ir::Dataset {
    let mut sink = GraphCollectorSink::with_named_graphs();
    parse_nquads(doc, &mut sink).unwrap_or_else(|e| panic!("{e}\n{doc}"));
    sink.into_dataset()
}

#[test]
fn quads_land_in_their_graphs_and_literals_keep_their_form() {
    let d = nquads(
        "<http://ex/s> <http://ex/p> \"01\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n\
         <http://ex/s> <http://ex/p> \"x\"@en--rtl <http://ex/g> .\n\
         _:b <http://ex/p> \"0.9957\"^^<http://www.w3.org/2001/XMLSchema#double> _:g . # comment\n",
    );
    assert_eq!(d.default.len(), 1);
    assert_eq!(d.named.len(), 2);
    let Term::Literal { value, .. } = &d.default.iter().next().unwrap().o else {
        panic!("a literal")
    };
    assert_eq!(value.lexical(), "01");
}

#[test]
fn triple_terms_nest_in_object_position() {
    let d = nquads(
        "<http://ex/doc> <http://ex/says> \
         <<( _:s <http://ex/p> <<( <http://ex/a> <http://ex/b> \"c\" )>> )>> .\n",
    );
    let object = &d.default.iter().next().unwrap().o;
    let Term::TripleTerm(outer) = object else {
        panic!("{object:?}")
    };
    assert!(matches!(outer[0], Term::BlankNode(_)));
    assert!(matches!(outer[2], Term::TripleTerm(_)));
}

/// `r rdf:reifies <<( s p o )>>` is a reification, as Turtle reads it, in
/// either line format and in any graph; it does not assert its triple.
#[test]
fn a_reifies_statement_is_a_reification() {
    let reifies = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies>";
    let d = nquads(&format!(
        "_:r {reifies} <<( <http://ex/a> <http://ex/b> \"c\" )>> .\n\
         <http://ex/r> {reifies} <<( <http://ex/a> <http://ex/b> <http://ex/c> )>> <http://ex/g> .\n\
         <http://ex/r> {reifies} <http://ex/c> .\n"
    ));
    assert_eq!(d.default.reifications().len(), 1);
    assert_eq!(d.default.reifications()[0].reifier, Term::blank("r"));
    assert_eq!(
        d.default.len(),
        1,
        "only the IRI-valued rdf:reifies is a triple"
    );
    let named = &d.named[&Term::iri("http://ex/g")];
    assert!(named.is_empty());
    assert_eq!(named.reifications()[0].reifier, Term::iri("http://ex/r"));

    let mut sink = GraphCollectorSink::new();
    parse_ntriples(
        &format!("_:r {reifies} <<( <http://ex/a> <http://ex/b> \"c\" )>> .\n"),
        &mut sink,
    )
    .unwrap();
    let graph = sink.into_graph();
    assert!(graph.is_empty());
    assert_eq!(graph.reifications().len(), 1);
}

#[test]
fn turtle_is_refused() {
    for doc in [
        "@prefix ex: <http://ex/> .",
        "<http://ex/s> <http://ex/p> ex:o .",
        "<s> <http://ex/p> <http://ex/o> .",
        "<http://ex/s> <http://ex/p> 1 .",
        "<http://ex/s> <http://ex/p> 'o' .",
        "<http://ex/s> <http://ex/p> \"\"\"o\"\"\" .",
        "<http://ex/s> <http://ex/p> <http://ex/o>, <http://ex/q> .",
        "<http://ex/s> a <http://ex/o> .",
        "<http://ex/s> <http://ex/p> << <http://ex/a> <http://ex/b> <http://ex/c> >> .",
        "<http://ex/s> <http://ex/p> <http://ex/o> {| <http://ex/q> \"x\" |} .",
        "<<( <http://ex/a> <http://ex/b> <http://ex/c> )>> <http://ex/p> <http://ex/o> .",
        "<http://ex/s> <http://ex/p> <http://ex/o> . <http://ex/s> <http://ex/p> <http://ex/o> .",
        "<http://ex/s> <http://ex/p> \"x\"@en--LTR .",
        "<http://ex/s> <http://ex/p> \"x\"@toolongsubtag .",
        "<http://ex/s> <http://ex/p> \"x\"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString> .",
        "<http://ex/s> <http://ex/p> <http://ex/a\\u0020b> .",
    ] {
        let mut sink = GraphCollectorSink::with_named_graphs();
        assert!(parse_nquads(doc, &mut sink).is_err(), "{doc}");
    }
}

#[test]
fn n_triples_has_no_graph_label() {
    let mut sink = GraphCollectorSink::with_named_graphs();
    let doc = "<http://ex/s> <http://ex/p> <http://ex/o> <http://ex/g> .\n";
    assert!(parse_ntriples(doc, &mut sink).is_err());
}

#[test]
fn a_triple_only_sink_refuses_a_graph_label() {
    let mut sink = GraphCollectorSink::new();
    let doc = "<http://ex/s> <http://ex/p> <http://ex/o> .\n\
               <http://ex/s> <http://ex/p> <http://ex/o> <http://ex/g> .\n";
    let err = parse_nquads(doc, &mut sink).unwrap_err();
    assert!(err.to_string().contains("named graph"), "{err}");
}
