//! OWL 2 RL list axioms in every list encoding.
//!
//! `owl:unionOf`, `owl:intersectionOf`, `owl:oneOf`, `owl:propertyChainAxiom`
//! and `owl:hasKey` take an RDF list. A Turtle `( … )` and a JSON-LD `@list`
//! are stored as members of the axiom's subject, carrying their positions; an
//! explicit `rdf:first`/`rdf:rest` chain is stored as the triples written. All
//! three are the same RDF and must reason the same.

use crate::support;
use crate::support::{genesis_ledger, normalize_rows};
use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};

const PREFIXES: &str = "@prefix ex: <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
";

#[derive(Clone, Copy, Debug)]
enum Encoding {
    TurtleCollection,
    JsonLdList,
    Spine,
}

const ENCODINGS: [Encoding; 3] = [
    Encoding::TurtleCollection,
    Encoding::JsonLdList,
    Encoding::Spine,
];

/// One list axiom: its Turtle, with `LIST` where the list goes; the same in
/// JSON-LD, with the string `"LIST"` there; and the list's members.
struct Axiom {
    turtle: &'static str,
    jsonld: JsonValue,
    members: &'static [&'static str],
}

/// Replace the `"LIST"` placeholder in `doc` with `list`.
fn place_list(doc: &mut JsonValue, list: &JsonValue) {
    match doc {
        JsonValue::String(s) if s == "LIST" => *doc = list.clone(),
        JsonValue::Array(items) => items.iter_mut().for_each(|v| place_list(v, list)),
        JsonValue::Object(map) => map.values_mut().for_each(|v| place_list(v, list)),
        _ => {}
    }
}

/// A ledger holding `axiom` in `encoding`, then `data`.
async fn ledger_with(
    fluree: &fluree_db_api::Fluree,
    name: &str,
    axiom: &Axiom,
    encoding: Encoding,
    data: &JsonValue,
) -> fluree_db_api::LedgerState {
    let ledger0 = genesis_ledger(fluree, &format!("owl2rl-lists/{name}-{encoding:?}")).await;
    let ledger1 = match encoding {
        Encoding::TurtleCollection => {
            let list = format!("( {} )", axiom.members.join(" "));
            let ttl = format!("{PREFIXES}{}", axiom.turtle.replace("LIST", &list));
            fluree.insert_turtle(ledger0, &ttl).await
        }
        Encoding::Spine => {
            let mut cells = String::new();
            for (k, member) in axiom.members.iter().enumerate() {
                let rest = if k + 1 == axiom.members.len() {
                    "rdf:nil".to_string()
                } else {
                    format!("_:cell{}", k + 1)
                };
                cells.push_str(&format!(
                    "_:cell{k} rdf:first {member} ; rdf:rest {rest} .\n"
                ));
            }
            let ttl = format!(
                "{PREFIXES}{}\n{cells}",
                axiom.turtle.replace("LIST", "_:cell0")
            );
            fluree.insert_turtle(ledger0, &ttl).await
        }
        Encoding::JsonLdList => {
            let members: Vec<JsonValue> = axiom.members.iter().map(|m| json!({"@id": m})).collect();
            let mut doc = axiom.jsonld.clone();
            place_list(&mut doc, &json!({"@list": members}));
            fluree.insert(ledger0, &doc).await
        }
    }
    .unwrap_or_else(|e| panic!("{name} {encoding:?}: schema: {e}"))
    .ledger;
    fluree
        .insert(ledger1, data)
        .await
        .unwrap_or_else(|e| panic!("{name} {encoding:?}: data: {e}"))
        .ledger
}

async fn reason(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    query: &JsonValue,
) -> Vec<JsonValue> {
    let rows = support::query_jsonld(fluree, ledger, query)
        .await
        .expect("query")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    normalize_rows(&rows)
}

fn ctx() -> JsonValue {
    json!({
        "ex": "http://example.org/",
        "owl": "http://www.w3.org/2002/07/owl#"
    })
}

/// Run `query` over `axiom` + `data` in every encoding, and check the
/// results contain `expected` and none of `excluded`.
async fn check(
    name: &str,
    axiom: Axiom,
    data: JsonValue,
    query: JsonValue,
    expected: &[&str],
    excluded: &[&str],
) {
    let fluree = FlureeBuilder::memory().build_memory();
    let mut wrong = Vec::new();
    for encoding in ENCODINGS {
        let ledger = ledger_with(&fluree, name, &axiom, encoding, &data).await;
        let results = reason(&fluree, &ledger, &query).await;
        let missing: Vec<_> = expected
            .iter()
            .filter(|iri| !results.contains(&json!(iri)))
            .collect();
        let extra: Vec<_> = excluded
            .iter()
            .filter(|iri| results.contains(&json!(iri)))
            .collect();
        if !missing.is_empty() || !extra.is_empty() {
            wrong.push(format!(
                "{encoding:?}: missing {missing:?}, unexpected {extra:?}, got {results:?}"
            ));
        }
    }
    assert!(wrong.is_empty(), "{name}:\n{}", wrong.join("\n"));
}

#[tokio::test]
async fn union_of_in_every_list_encoding() {
    check(
        "union",
        Axiom {
            turtle: "ex:Parent owl:equivalentClass [ a owl:Class ; owl:unionOf LIST ] .",
            jsonld: json!({
                "@context": ctx(),
                "@id": "ex:Parent",
                "owl:equivalentClass": {"@type": "owl:Class", "owl:unionOf": "LIST"}
            }),
            members: &["ex:Mother", "ex:Father"],
        },
        json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:carol", "@type": "ex:Mother"},
                {"@id": "ex:bob", "@type": "ex:Father"},
                {"@id": "ex:alice", "@type": "ex:Woman"}
            ]
        }),
        json!({
            "@context": ctx(),
            "select": "?s",
            "where": {"@id": "?s", "@type": "ex:Parent"},
            "reasoning": "owl2rl"
        }),
        &["ex:carol", "ex:bob"],
        &["ex:alice"],
    )
    .await;
}

#[tokio::test]
async fn intersection_of_in_every_list_encoding() {
    check(
        "intersection",
        Axiom {
            turtle: "ex:Mother owl:equivalentClass [ a owl:Class ; owl:intersectionOf LIST ] .",
            jsonld: json!({
                "@context": ctx(),
                "@id": "ex:Mother",
                "owl:equivalentClass": {"@type": "owl:Class", "owl:intersectionOf": "LIST"}
            }),
            members: &["ex:Woman", "ex:Parent"],
        },
        json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:carol", "@type": ["ex:Woman", "ex:Parent"]},
                {"@id": "ex:alice", "@type": "ex:Woman"}
            ]
        }),
        json!({
            "@context": ctx(),
            "select": "?s",
            "where": {"@id": "?s", "@type": "ex:Mother"},
            "reasoning": "owl2rl"
        }),
        &["ex:carol"],
        &["ex:alice"],
    )
    .await;
}

#[tokio::test]
async fn one_of_in_every_list_encoding() {
    check(
        "one-of",
        Axiom {
            turtle: "ex:RedOrGreen owl:equivalentClass [ a owl:Class ; owl:oneOf LIST ] .",
            jsonld: json!({
                "@context": ctx(),
                "@id": "ex:RedOrGreen",
                "owl:equivalentClass": {"@type": "owl:Class", "owl:oneOf": "LIST"}
            }),
            members: &["ex:Red", "ex:Green"],
        },
        json!({"@context": ctx(), "@id": "ex:Blue", "ex:hue": "blue"}),
        json!({
            "@context": ctx(),
            "select": "?s",
            "where": {"@id": "?s", "@type": "ex:RedOrGreen"},
            "reasoning": "owl2rl"
        }),
        &["ex:Red", "ex:Green"],
        &["ex:Blue"],
    )
    .await;
}

/// The chain repeats a member, which a list keeps and a set would not.
#[tokio::test]
async fn property_chain_in_every_list_encoding() {
    check(
        "chain",
        Axiom {
            turtle: "ex:grandparent owl:propertyChainAxiom LIST .",
            jsonld: json!({
                "@context": ctx(),
                "@id": "ex:grandparent",
                "owl:propertyChainAxiom": "LIST"
            }),
            members: &["ex:parents", "ex:parents"],
        },
        json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:person-a", "ex:parents": [{"@id": "ex:mom"}, {"@id": "ex:dad"}]},
                {"@id": "ex:mom", "ex:parents": [{"@id": "ex:mom-mom"}, {"@id": "ex:mom-dad"}]},
                {"@id": "ex:dad", "ex:parents": [{"@id": "ex:dad-mom"}, {"@id": "ex:dad-dad"}]}
            ]
        }),
        json!({
            "@context": ctx(),
            "select": "?gp",
            "where": {"@id": "ex:person-a", "ex:grandparent": "?gp"},
            "reasoning": "owl2rl"
        }),
        &["ex:mom-mom", "ex:mom-dad", "ex:dad-mom", "ex:dad-dad"],
        &["ex:mom", "ex:dad"],
    )
    .await;
}

/// A chain is read in member order: `hasParent` then `hasBrother` is an
/// uncle, where the reverse would be a sibling's parent.
#[tokio::test]
async fn property_chain_order_in_every_list_encoding() {
    check(
        "chain-order",
        Axiom {
            turtle: "ex:uncle owl:propertyChainAxiom LIST .",
            jsonld: json!({
                "@context": ctx(),
                "@id": "ex:uncle",
                "owl:propertyChainAxiom": "LIST"
            }),
            members: &["ex:hasParent", "ex:hasBrother"],
        },
        json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:person-a", "ex:hasParent": {"@id": "ex:dad"}, "ex:hasBrother": {"@id": "ex:sib"}},
                {"@id": "ex:dad", "ex:hasBrother": {"@id": "ex:uncle-bob"}},
                {"@id": "ex:sib", "ex:hasParent": {"@id": "ex:other"}}
            ]
        }),
        json!({
            "@context": ctx(),
            "select": "?u",
            "where": {"@id": "ex:person-a", "ex:uncle": "?u"},
            "reasoning": "owl2rl"
        }),
        &["ex:uncle-bob"],
        &["ex:other"],
    )
    .await;
}

#[tokio::test]
async fn has_key_in_every_list_encoding() {
    check(
        "has-key",
        Axiom {
            turtle: "ex:RegisteredPatient owl:hasKey LIST .",
            jsonld: json!({
                "@context": ctx(),
                "@id": "ex:RegisteredPatient",
                "owl:hasKey": "LIST"
            }),
            members: &["ex:hasWaitingListN"],
        },
        json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:brian", "@type": "ex:RegisteredPatient", "ex:hasWaitingListN": {"@id": "ex:ssn123"}},
                {"@id": "ex:brian2", "@type": "ex:RegisteredPatient", "ex:hasWaitingListN": {"@id": "ex:ssn123"}},
                {"@id": "ex:bob", "@type": "ex:RegisteredPatient", "ex:hasWaitingListN": {"@id": "ex:ssn456"}}
            ]
        }),
        json!({
            "@context": ctx(),
            "select": "?same",
            "where": {"@id": "ex:brian", "owl:sameAs": "?same"},
            "reasoning": "owl2rl"
        }),
        &["ex:brian2"],
        &["ex:bob"],
    )
    .await;
}
