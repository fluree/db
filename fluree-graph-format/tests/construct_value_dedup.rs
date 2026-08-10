//! Regression corpus: CONSTRUCT value dedup must key on RDF triple identity.
//!
//! A CONSTRUCT result is an RDF graph, so triple identity is the full
//! `(s, p, o)` tuple (SPARQL 1.1 §16.2 — set union of instantiated templates).
//! The JSON-LD formatter's `dedupe_values` pass once tracked seen objects per
//! SUBJECT, which silently dropped `<s> <p2> "x"` whenever `<s> <p1> "x"` had
//! already been emitted. These tests pin the object-dedup scope to a single
//! predicate, and pin the cases that must still collapse.
//!
//! Each test builds the Graph by hand, so a failure here is unambiguously a
//! serializer defect — no query engine involved.

use fluree_graph_format::{format_jsonld, JsonLdFormatConfig};
use fluree_graph_ir::{Datatype, Graph, Term};
use serde_json::Value as JsonValue;

/// Count object values across every `@graph` node, excluding `@id`/`@context`.
fn count_values(v: &JsonValue) -> usize {
    let graph = v
        .get("@graph")
        .and_then(JsonValue::as_array)
        .expect("@graph");
    graph
        .iter()
        .filter_map(JsonValue::as_object)
        .flat_map(serde_json::Map::iter)
        .filter(|(k, _)| k.as_str() != "@id" && k.as_str() != "@context")
        .map(|(_, val)| match val {
            JsonValue::Array(a) => a.len(),
            _ => 1,
        })
        .sum()
}

fn render(graph: &mut Graph) -> JsonValue {
    graph.sort();
    format_jsonld(
        graph,
        &JsonLdFormatConfig::construct_parity(None, std::string::ToString::to_string),
    )
}

fn triple(g: &mut Graph, s: &str, p: &str, o: Term) {
    g.add_triple(Term::iri(s), Term::iri(p), o);
}

/// The reported shape: one subject, one literal, two predicates. Two distinct
/// RDF triples — both must survive.
#[test]
fn same_literal_under_two_predicates_both_survive() {
    let mut g = Graph::new();
    triple(&mut g, "http://ex/s", "http://ex/p1", Term::string("same"));
    triple(&mut g, "http://ex/s", "http://ex/p2", Term::string("same"));
    assert_eq!(g.len(), 2, "graph must hold both triples pre-serialization");

    let out = render(&mut g);
    assert_eq!(count_values(&out), 2, "p1 and p2 must both emit: {out}");
}

/// The defect was never literal-specific: IRI objects drop the same way.
#[test]
fn same_iri_object_under_two_predicates_both_survive() {
    let mut g = Graph::new();
    triple(
        &mut g,
        "http://ex/s",
        "http://ex/p1",
        Term::iri("http://ex/o"),
    );
    triple(
        &mut g,
        "http://ex/s",
        "http://ex/p2",
        Term::iri("http://ex/o"),
    );

    let out = render(&mut g);
    assert_eq!(count_values(&out), 2, "IRI objects: {out}");
}

/// Loss grows with predicate fan-out, not linearly: this collapsed to ONE.
#[test]
fn many_predicates_sharing_one_value_all_survive() {
    let mut g = Graph::new();
    for i in 0..10 {
        triple(
            &mut g,
            "http://ex/s",
            &format!("http://ex/p{i}"),
            Term::string("same"),
        );
    }

    let out = render(&mut g);
    assert_eq!(count_values(&out), 10, "10 predicates, one value: {out}");
}

/// Control: distinct values were never affected. Guards against a fix that
/// disables dedup wholesale.
#[test]
fn distinct_values_under_two_predicates_both_survive() {
    let mut g = Graph::new();
    triple(&mut g, "http://ex/s", "http://ex/p1", Term::string("alpha"));
    triple(&mut g, "http://ex/s", "http://ex/p2", Term::string("beta"));

    let out = render(&mut g);
    assert_eq!(count_values(&out), 2, "control: {out}");
}

/// The behavior the fix must PRESERVE: an identical `(s, p, o)` appearing twice
/// is one RDF triple and must be emitted once.
#[test]
fn identical_triple_still_collapses() {
    let mut g = Graph::new();
    for _ in 0..2 {
        triple(&mut g, "http://ex/s", "http://ex/p1", Term::string("same"));
    }

    let out = render(&mut g);
    assert_eq!(count_values(&out), 1, "true duplicate must collapse: {out}");
}

/// Dedup is per-subject as well as per-predicate: the same value on a different
/// subject is a different triple. Bounds the blast radius of the scope change.
#[test]
fn same_value_on_different_subjects_both_survive() {
    let mut g = Graph::new();
    triple(&mut g, "http://ex/s1", "http://ex/p1", Term::string("same"));
    triple(&mut g, "http://ex/s2", "http://ex/p1", Term::string("same"));

    let out = render(&mut g);
    assert_eq!(count_values(&out), 2, "distinct subjects: {out}");
}

/// RDF term identity includes the datatype: `"1"^^xsd:integer` and
/// `"1"^^xsd:string` share a lexical form but are distinct terms.
#[test]
fn equal_lexical_form_different_datatype_both_survive() {
    let mut g = Graph::new();
    triple(&mut g, "http://ex/s", "http://ex/p1", Term::integer(1));
    triple(
        &mut g,
        "http://ex/s",
        "http://ex/p1",
        Term::typed("1", Datatype::xsd_string()),
    );

    let out = render(&mut g);
    assert_eq!(count_values(&out), 2, "datatypes must not collapse: {out}");
}

/// Term identity also includes the language tag.
#[test]
fn language_tagged_and_plain_literal_both_survive() {
    let mut g = Graph::new();
    triple(&mut g, "http://ex/s", "http://ex/p1", Term::string("same"));
    triple(
        &mut g,
        "http://ex/s",
        "http://ex/p2",
        Term::lang_string("same", "en"),
    );

    let out = render(&mut g);
    assert_eq!(count_values(&out), 2, "lang tag vs plain: {out}");
}
