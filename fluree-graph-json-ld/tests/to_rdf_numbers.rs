//! How a JSON number becomes an RDF literal: by its value, as JSON-LD 1.1
//! API § 8.2.2 (Object to RDF Conversion) converts it. Every reader of
//! JSON-LD data shares the reading (`fluree_graph_ir::JsonNumber`).
//!
//! Documents are parsed from text, never built with `json!`: the spelling of
//! a number (`1.0`, `-0e0`, `1e21`) is what is under test.

use fluree_graph_ir::{GraphCollectorSink, GraphSink, Term};
use fluree_graph_json_ld::adapter::to_graph_events;
use fluree_graph_json_ld::expand;
use pretty_assertions::assert_eq;
use serde_json::Value;

const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

/// The document's literals as `"lexical"^^datatype`, sorted. An `xsd:`
/// datatype is written with the prefix.
fn rdf(doc: &str) -> Vec<String> {
    let doc: Value = serde_json::from_str(doc).unwrap();
    let expanded = expand(&doc).unwrap();
    let mut sink = GraphCollectorSink::new();
    to_graph_events(&expanded, &mut sink).unwrap();
    sink.finish().unwrap();
    let mut out: Vec<String> = sink
        .into_graph()
        .iter()
        .map(|t| match &t.o {
            Term::Literal {
                value, datatype, ..
            } => {
                let datatype = datatype.as_iri();
                match datatype.strip_prefix(XSD) {
                    Some(local) => format!("{:?}^^xsd:{local}", value.lexical()),
                    None => format!("{:?}^^<{datatype}>", value.lexical()),
                }
            }
            other => panic!("expected a literal, got {other:?}"),
        })
        .collect();
    out.sort();
    out
}

/// W3C toRdf `rt01`, "Representing numbers >= 1e21".
#[test]
fn w3c_rt01_integral_numbers_are_integers_below_1e21() {
    let doc = r#"{
      "@context": { "number": {"@id": "http://example.com/number"} },
      "number": [-0e0, 8, 9.9, 1e21 ]
    }"#;
    assert_eq!(
        rdf(doc),
        [
            r#""0"^^xsd:integer"#,
            r#""1.0E21"^^xsd:double"#,
            r#""8"^^xsd:integer"#,
            r#""9.9E0"^^xsd:double"#,
        ]
    );
}

/// W3C toRdf `0022` and `0023`: a fraction is a double, a whole number an
/// integer.
#[test]
fn w3c_0022_0023_fraction_is_double_and_whole_is_integer() {
    let doc = r#"{
      "@context": { "measure": "http://example/measure#", "chem": "http://example/chem#" },
      "measure:cups": 5.3,
      "chem:protons": 12
    }"#;
    assert_eq!(rdf(doc), [r#""12"^^xsd:integer"#, r#""5.3E0"^^xsd:double"#]);
}

/// W3C toRdf `0035`, "non-fractional numbers converted to xsd:double": a
/// number coerced to `xsd:double` is a double whatever its value, and one
/// coerced to another datatype keeps the form its value gives it.
#[test]
fn w3c_0035_coerced_numbers() {
    let doc = r#"{
      "@context": {
        "double": {
          "@id": "http://example.com/double",
          "@type": "http://www.w3.org/2001/XMLSchema#double"
        },
        "integer": {
          "@id": "http://example.com/integer",
          "@type": "http://www.w3.org/2001/XMLSchema#integer"
        }
      },
      "double": [1, 2.2 ],
      "integer": [8, 9.9 ]
    }"#;
    assert_eq!(
        rdf(doc),
        [
            r#""1.0E0"^^xsd:double"#,
            r#""2.2E0"^^xsd:double"#,
            r#""8"^^xsd:integer"#,
            r#""9.9E0"^^xsd:integer"#,
        ]
    );
}

/// W3C toRdf `e061`, "Coercing native types to arbitrary datatypes".
#[test]
fn w3c_e061_native_values_under_another_datatype() {
    let doc = r#"{
      "@context": {
        "property": {
          "@id": "http://example.com/property",
          "@type": "http://example.com/datatype"
        }
      },
      "property": [ 1, true, false, 5.1 ]
    }"#;
    assert_eq!(
        rdf(doc),
        [
            r#""1"^^<http://example.com/datatype>"#,
            r#""5.1E0"^^<http://example.com/datatype>"#,
            r#""false"^^<http://example.com/datatype>"#,
            r#""true"^^<http://example.com/datatype>"#,
        ]
    );
}

/// The spelling of a whole number does not change what it is: `1.0`, `1e3`
/// and `1000` name integers, in a property, a value object and a list alike.
#[test]
fn a_whole_number_is_an_integer_however_it_is_spelled() {
    let doc = r#"{
      "@context": { "p": "http://example.com/p", "l": {"@id": "http://example.com/l", "@container": "@list"} },
      "p": [1.0, 1e3, -2.0, {"@value": 4.0}, {"@value": 5.0, "@type": "http://example.com/datatype"}],
      "l": [7.0, 7.5]
    }"#;
    assert_eq!(
        rdf(doc),
        [
            r#""-2"^^xsd:integer"#,
            r#""1"^^xsd:integer"#,
            r#""1000"^^xsd:integer"#,
            r#""4"^^xsd:integer"#,
            r#""5"^^<http://example.com/datatype>"#,
            r#""7"^^xsd:integer"#,
            r#""7.5E0"^^xsd:double"#,
        ]
    );
}

/// An integer past `i64::MAX` is still an integer below 10^21, with every
/// digit JSON kept; from 10^21 it is a double.
#[test]
fn an_integer_past_i64_is_an_integer_below_1e21() {
    let doc = r#"{
      "@context": { "p": "http://example.com/p" },
      "p": [18446744073709551615, 1e20, -1e20, 999999999999999900000, 1000000000000000000000]
    }"#;
    assert_eq!(
        rdf(doc),
        [
            r#""-100000000000000000000"^^xsd:integer"#,
            r#""1.0E21"^^xsd:double"#,
            r#""100000000000000000000"^^xsd:integer"#,
            r#""18446744073709551615"^^xsd:integer"#,
            r#""999999999999999868928"^^xsd:integer"#,
        ]
    );
}

/// A number keeps every digit it was written with: the workspace builds
/// `serde_json` with `float_roundtrip`, without which this one reads as the
/// neighbouring double `0.9452706955539224`.
#[test]
fn a_fraction_reads_as_the_nearest_double() {
    let doc = r#"{
      "@context": { "p": "http://example.com/p" },
      "p": 0.9452706955539223
    }"#;
    assert_eq!(rdf(doc), [r#""9.452706955539223E-1"^^xsd:double"#]);
}
