//! The policy's counts against the extraction service's own request reader.
//!
//! Each fixture exercises a corner of the input policy: SKOS text in several
//! languages, OWL restrictions and `owl:unionOf` lists, a second parent, an
//! ontology header, SHACL shapes with unsupported constraints and non-direct
//! paths, schema.org's `domainIncludes`. The expected counts were recorded by
//! running the service's request reader (input policy `ontology-adapter-v1`)
//! on the same files; a change here means `check` no longer says what a
//! request's `X-Fluree-Ontology-Adapted` / `-Not-Shown` headers would.

use fluree_db_ontology::{check, parse, CheckOptions, RdfFormat};
use serde_json::{json, Value};

fn counts(file: &str, format: RdfFormat) -> (Value, Value) {
    let path = format!("{}/tests/fixtures/{file}", env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(path).unwrap();
    let report = check(
        &parse(&text, format, None).unwrap(),
        &CheckOptions::default(),
    )
    .unwrap();
    assert!(report.passes(), "{file}: {:?}", report.will_fail);
    (json!(report.adapted), json!(report.not_shown))
}

#[test]
fn owl_and_skos() {
    let (adapted, not_shown) = counts("owl-skos.ttl", RdfFormat::Turtle);
    assert_eq!(
        adapted,
        json!({
            "blank-node triples dropped": 10,
            "rdfs:comment from skos:definition": 1,
            "rdfs:label from skos:prefLabel": 2,
            "rdfs:label from the IRI": 3,
        })
    );
    assert_eq!(
        not_shown,
        json!({
            "(not a shown term)": 3,
            "owl:equivalentClass": 1,
            "owl:inverseOf": 1,
            "owl:onProperty": 1,
            "owl:someValuesFrom": 1,
            "owl:unionOf": 1,
            "rdf:first": 2,
            "rdf:rest": 2,
            "rdf:type owl:FunctionalProperty": 1,
            "rdf:type owl:Restriction": 1,
            "rdfs:domain": 2,
            "rdfs:range": 3,
            "rdfs:subClassOf": 1,
            "rdfs:subClassOf (a second parent)": 1,
            "skos:altLabel": 1,
            "skos:definition": 2,
            "skos:prefLabel": 1,
        })
    );
}

#[test]
fn shacl_shapes() {
    let (adapted, not_shown) = counts("shapes.ttl", RdfFormat::Turtle);
    assert_eq!(adapted, json!({}));
    // Person's shape restates employer's and name's domain and range; city's
    // range isn't (its shape gives two datatypes), nor employs' domain (it
    // appears only as an inverse path); nine constraints aren't supported
    assert_eq!(
        not_shown,
        json!({"rdfs:domain": 1, "rdfs:range": 1, "sh:(unsupported)": 9})
    );
}

#[test]
fn schema_org_style() {
    let (adapted, not_shown) = counts("schema-style.jsonld", RdfFormat::JsonLd);
    assert_eq!(
        adapted,
        json!({"blank-node triples dropped": 5, "rdfs:label from the IRI": 1})
    );
    assert_eq!(
        not_shown,
        json!({
            "https://schema.org/domainIncludes": 1,
            "https://schema.org/rangeIncludes": 1,
            "owl:unionOf": 1,
            "rdf:first": 2,
            "rdf:rest": 2,
        })
    );
}
