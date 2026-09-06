//! `explain` and language selection.

use crate::support::{genesis_ledger, MemoryFluree};
use fluree_db_api::graphql::GraphQlRequest;
use fluree_db_api::{FlureeBuilder, GraphDb, LedgerState};
use serde_json::{json, Value as JsonValue};

const EX: &str = "http://example.org/";

fn context() -> JsonValue {
    json!({
        "ex": EX,
        "sh": "http://www.w3.org/ns/shacl#",
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    })
}

fn view(ledger: &LedgerState) -> GraphDb {
    GraphDb::from_ledger_state(ledger).with_default_context(Some(context()))
}

async fn seeded(ledger_id: &str, graph: JsonValue) -> (MemoryFluree, LedgerState) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, ledger_id);
    let ledger = fluree
        .insert(ledger, &json!({ "@context": context(), "@graph": graph }))
        .await
        .expect("seed")
        .ledger;
    (fluree, ledger)
}

async fn run(fluree: &MemoryFluree, db: &GraphDb, request: GraphQlRequest) -> JsonValue {
    let response = fluree.graphql(db, &request).await.expect("request");
    assert!(
        response.get("errors").is_none(),
        "unexpected errors: {}",
        serde_json::to_string_pretty(&response).unwrap()
    );
    response
}

#[tokio::test]
async fn explain_returns_the_query_each_root_field_lowered_to() {
    let (fluree, ledger) = seeded(
        "gql-explain",
        json!([{ "@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice" }]),
    )
    .await;

    let response = run(
        &fluree,
        &view(&ledger),
        GraphQlRequest::new(r#"{ persons(where: { name: { EQ: "Alice" } }) { id name } }"#)
            .explained(),
    )
    .await;

    let explain = &response["extensions"]["explain"];
    assert_eq!(explain["tier"], "inferred");
    assert_eq!(explain["warnings"], json!([]));

    let field = &explain["fields"][0];
    assert_eq!(field["field"], "persons");
    assert_eq!(field["provenance"], "inferred");
    // The lowered query is the JSON-LD a user could have written by hand, which
    // is the point of showing it.
    assert_eq!(
        field["query"]["where"],
        json!([
            { "@id": "?_gql0", "@type": format!("{EX}Person") },
            { "@id": "?_gql0", format!("{EX}name"): "?_gql1" },
            ["filter", "(= ?_gql1 \"Alice\")"]
        ])
    );

    // Absent unless asked for: it is a debugging aid, not part of the contract.
    let plain = run(
        &fluree,
        &view(&ledger),
        GraphQlRequest::new("{ persons { id } }"),
    )
    .await;
    assert!(plain.get("extensions").is_none(), "{plain}");
}

#[tokio::test]
async fn explain_reports_the_tier_and_its_approximations() {
    let (fluree, ledger) = seeded(
        "gql-explain-tier",
        json!([
            {
                "@id": "ex:PersonShape",
                "@type": "sh:NodeShape",
                "sh:targetClass": { "@id": "ex:Person" },
                "sh:property": [{
                    "sh:path": { "@id": "ex:name" },
                    "sh:datatype": { "@id": "xsd:string" },
                    "sh:maxCount": 1
                }]
            },
            // A property holding both a reference and a literal, which no
            // GraphQL type covers — the model records a warning.
            {
                "@id": "ex:alice",
                "@type": "ex:Person",
                "ex:name": "Alice",
                "ex:about": ["a string", { "@id": "ex:alice" }]
            }
        ]),
    )
    .await;

    let response = run(
        &fluree,
        &view(&ledger),
        GraphQlRequest::new("{ persons { name } }").explained(),
    )
    .await;
    let explain = &response["extensions"]["explain"];
    assert_eq!(explain["tier"], "shaped");
    let warnings = explain["warnings"].as_array().unwrap();
    assert!(
        warnings
            .iter()
            .any(|w| w.as_str().unwrap().contains("references and literals")),
        "{warnings:?}"
    );
}

#[tokio::test]
async fn explain_shows_the_transaction_a_mutation_committed() {
    let (fluree, ledger) = seeded(
        "gql-explain-mutation",
        json!([
            {
                "@id": "ex:PersonShape",
                "@type": "sh:NodeShape",
                "sh:targetClass": { "@id": "ex:Person" },
                "sh:property": [{
                    "sh:path": { "@id": "ex:name" },
                    "sh:datatype": { "@id": "xsd:string" },
                    "sh:maxCount": 1
                }]
            },
            {
                "@id": "ex:Api",
                "@type": "http://datashapes.org/graphql#Schema",
                "http://datashapes.org/graphql#publicShape": { "@id": "ex:PersonShape" },
                "https://ns.flur.ee/db#graphqlEnableMutations": true,
                "https://ns.flur.ee/db#graphqlIriBase": EX
            }
        ]),
    )
    .await;

    let (response, ledger) = fluree
        .graphql_transact(
            ledger,
            Some(context()),
            &GraphQlRequest::new(
                r#"mutation { create_Person(input: { id: "ex:bob", name: "Bob" }) { id } }"#,
            )
            .explained(),
        )
        .await
        .expect("mutation");
    assert!(response.get("errors").is_none(), "{response}");

    let field = &response["extensions"]["explain"]["fields"][0];
    assert_eq!(field["field"], "create_Person");
    assert_eq!(field["provenance"], "curated");
    assert_eq!(
        field["transaction"]["@graph"][0],
        json!({
            "@id": format!("{EX}bob"),
            "@type": format!("{EX}Person"),
            format!("{EX}name"): "Bob"
        })
    );

    // `explain` reports what ran; it is not a dry run.
    let read = fluree
        .graphql(&view(&ledger), &GraphQlRequest::new("{ persons_count }"))
        .await
        .unwrap();
    assert_eq!(read["data"], json!({ "persons_count": 1 }));
}

#[tokio::test]
async fn a_language_tagged_field_selects_by_preference() {
    let (fluree, ledger) = seeded(
        "gql-lang",
        json!([{
            "@id": "ex:paris",
            "@type": "ex:City",
            "ex:label": [
                { "@value": "Paris", "@language": "en" },
                { "@value": "Parigi", "@language": "it" }
            ]
        }]),
    )
    .await;
    let db = view(&ledger);

    // The field offers `lang` because the data carries tags. (The argument
    // carries a description, so the SDL renders it across several lines.)
    let sdl = fluree_db_api::graphql::schema_sdl(&db).await.unwrap();
    assert!(sdl.contains("lang: String"), "{sdl}");
    assert!(sdl.contains("): [String!]"), "{sdl}");

    // A preference list yields the first language that exists, not both.
    let data = run(
        &fluree,
        &db,
        GraphQlRequest::new(r#"{ cities { label(lang: "en,it") } }"#),
    )
    .await;
    assert_eq!(data["data"], json!({ "cities": [{ "label": ["Paris"] }] }));

    let data = run(
        &fluree,
        &db,
        GraphQlRequest::new(r#"{ cities { label(lang: "fr,it") } }"#),
    )
    .await;
    assert_eq!(data["data"], json!({ "cities": [{ "label": ["Parigi"] }] }));

    // A language nothing is tagged with yields nothing, rather than falling
    // back to a language the caller did not ask for.
    let data = run(
        &fluree,
        &db,
        GraphQlRequest::new(r#"{ cities { label(lang: "de") } }"#),
    )
    .await;
    assert_eq!(data["data"], json!({ "cities": [{ "label": [] }] }));

    // Omitting the argument returns plain strings, not the
    // `{"@value": …, "@language": …}` form the hydration produces for a tagged
    // literal — the field is declared `String`, so the schema would otherwise
    // be describing something the response does not match.
    let plain_only = run(&fluree, &db, GraphQlRequest::new("{ cities { label } }")).await;
    for value in plain_only["data"]["cities"][0]["label"].as_array().unwrap() {
        assert!(value.is_string(), "expected a plain string, got {value}");
    }

    // `*` and omitting the argument both mean every value.
    let starred = run(
        &fluree,
        &db,
        GraphQlRequest::new(r#"{ cities { label(lang: "*") } }"#),
    )
    .await;
    let plain = run(&fluree, &db, GraphQlRequest::new("{ cities { label } }")).await;
    let mut a = starred["data"]["cities"][0]["label"]
        .as_array()
        .unwrap()
        .clone();
    let mut b = plain["data"]["cities"][0]["label"]
        .as_array()
        .unwrap()
        .clone();
    a.sort_by_key(|v| v.as_str().unwrap().to_string());
    b.sort_by_key(|v| v.as_str().unwrap().to_string());
    assert_eq!(a, b);
    assert_eq!(a.len(), 2);
}

#[tokio::test]
async fn an_untagged_field_offers_no_lang_argument() {
    let (_fluree, ledger) = seeded(
        "gql-lang-absent",
        json!([{ "@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice" }]),
    )
    .await;
    let sdl = fluree_db_api::graphql::schema_sdl(&view(&ledger))
        .await
        .unwrap();
    // No tagged values, so nothing to select among.
    assert!(sdl.contains("name: [String!]"), "{sdl}");
    assert!(!sdl.contains("name(lang:"), "{sdl}");
}
