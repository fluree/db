//! Tier 3 end to end: a `graphql:Schema` written to a ledger decides what its
//! GraphQL endpoint publishes.

use crate::support::{genesis_ledger, MemoryFluree};
use fluree_db_api::graphql::{schema_sdl, GraphQlRequest};
use fluree_db_api::{FlureeBuilder, GraphDb, LedgerState};
use serde_json::{json, Value as JsonValue};

const EX: &str = "http://example.org/";

fn context() -> JsonValue {
    json!({
        "ex": EX,
        "sh": "http://www.w3.org/ns/shacl#",
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "graphql": "http://datashapes.org/graphql#",
        "f": "https://ns.flur.ee/db#"
    })
}

fn view(ledger: &LedgerState) -> GraphDb {
    GraphDb::from_ledger_state(ledger).with_default_context(Some(context()))
}

async fn run(fluree: &MemoryFluree, db: &GraphDb, query: &str) -> JsonValue {
    let response = fluree
        .graphql(db, &GraphQlRequest::new(query))
        .await
        .expect("graphql request");
    assert!(
        response.get("errors").is_none(),
        "unexpected errors: {}",
        serde_json::to_string_pretty(&response).unwrap()
    );
    response["data"].clone()
}

async fn error_of(fluree: &MemoryFluree, db: &GraphDb, query: &str) -> String {
    let response = fluree
        .graphql(db, &GraphQlRequest::new(query))
        .await
        .expect("graphql request");
    response["errors"][0]["message"]
        .as_str()
        .unwrap_or_else(|| panic!("expected an error, got {response}"))
        .to_string()
}

/// A ledger with three classes, shapes for each, and whatever curation is passed.
async fn seeded(ledger_id: &str, curation: Option<JsonValue>) -> (MemoryFluree, LedgerState) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, ledger_id);
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": context(),
                "@graph": [
                    {
                        "@id": "ex:alice",
                        "@type": "ex:Person",
                        "ex:name": "Alice",
                        "ex:employer": { "@id": "ex:acme" },
                        "ex:audit": { "@id": "ex:log1" }
                    },
                    { "@id": "ex:acme", "@type": "ex:Company", "ex:name": "Acme" },
                    { "@id": "ex:log1", "@type": "ex:AuditRecord", "ex:note": "secret" }
                ]
            }),
        )
        .await
        .expect("seed data")
        .ledger;

    let shapes = json!({
        "@context": context(),
        "@graph": [
            {
                "@id": "ex:PersonShape",
                "@type": "sh:NodeShape",
                "sh:targetClass": { "@id": "ex:Person" },
                "sh:property": [
                    { "sh:path": { "@id": "ex:name" }, "sh:datatype": { "@id": "xsd:string" },
                      "sh:maxCount": 1 },
                    { "sh:path": { "@id": "ex:employer" }, "sh:class": { "@id": "ex:Company" },
                      "sh:maxCount": 1 },
                    { "sh:path": { "@id": "ex:audit" }, "sh:class": { "@id": "ex:AuditRecord" } }
                ]
            },
            {
                "@id": "ex:CompanyShape",
                "@type": "sh:NodeShape",
                "sh:targetClass": { "@id": "ex:Company" },
                "sh:property": [
                    { "sh:path": { "@id": "ex:name" }, "sh:datatype": { "@id": "xsd:string" },
                      "sh:maxCount": 1 }
                ]
            },
            {
                "@id": "ex:AuditShape",
                "@type": "sh:NodeShape",
                "sh:targetClass": { "@id": "ex:AuditRecord" },
                "sh:property": [
                    { "sh:path": { "@id": "ex:note" }, "sh:datatype": { "@id": "xsd:string" },
                      "sh:maxCount": 1 }
                ]
            }
        ]
    });
    let ledger = fluree.insert(ledger, &shapes).await.expect("shapes").ledger;

    let ledger = match curation {
        Some(doc) => fluree.insert(ledger, &doc).await.expect("curation").ledger,
        None => ledger,
    };
    (fluree, ledger)
}

#[tokio::test]
async fn without_a_curated_schema_every_shaped_class_is_published() {
    let (_fluree, ledger) = seeded("gql-cur-none", None).await;
    let sdl = schema_sdl(&view(&ledger)).await.expect("sdl");
    for expected in ["type Person {", "type Company {", "type AuditRecord {"] {
        assert!(sdl.contains(expected), "{sdl}");
    }
}

#[tokio::test]
async fn a_curated_schema_publishes_only_what_it_lists() {
    let (fluree, ledger) = seeded(
        "gql-cur-exposure",
        Some(json!({
            "@context": context(),
            "@graph": [{
                "@id": "ex:PublicApi",
                "@type": "graphql:Schema",
                "graphql:name": "public",
                "graphql:publicShape": { "@id": "ex:PersonShape" },
                "graphql:protectedShape": { "@id": "ex:CompanyShape" },
                "graphql:privateShape": { "@id": "ex:AuditShape" }
            }]
        })),
    )
    .await;
    let db = view(&ledger);
    let sdl = schema_sdl(&db).await.expect("sdl");

    // Public and protected are both types; private is not.
    assert!(sdl.contains("type Person {"), "{sdl}");
    assert!(sdl.contains("type Company {"), "{sdl}");
    assert!(!sdl.contains("type AuditRecord"), "{sdl}");

    // Only the public class is enumerable.
    assert!(sdl.contains("persons(where:"), "{sdl}");
    assert!(!sdl.contains("companies("), "{sdl}");
    let msg = error_of(&fluree, &db, "{ companies { id } }").await;
    assert!(msg.contains("companies"), "{msg}");

    // A protected class is still readable through a reference.
    let data = run(
        &fluree,
        &db,
        r#"{ person(id: "ex:alice") { name employer { name } } }"#,
    )
    .await;
    assert_eq!(
        data,
        json!({ "person": { "name": "Alice", "employer": { "name": "Acme" } } })
    );

    // A reference to a private class degrades to `Node`: the edge is visible as
    // an IRI, but the type behind it is not.
    let data = run(
        &fluree,
        &db,
        r#"{ person(id: "ex:alice") { audit { id } } }"#,
    )
    .await;
    assert_eq!(data["person"]["audit"], json!([{ "id": "ex:log1" }]));
    let msg = error_of(
        &fluree,
        &db,
        r#"{ person(id: "ex:alice") { audit { note } } }"#,
    )
    .await;
    assert!(msg.contains("note"), "{msg}");
}

#[tokio::test]
async fn graphql_name_and_plural_name_rename_the_surface() {
    let (fluree, ledger) = seeded(
        "gql-cur-names",
        Some(json!({
            "@context": context(),
            "@graph": [
                {
                    "@id": "ex:Api",
                    "@type": "graphql:Schema",
                    "graphql:publicShape": { "@id": "ex:PersonShape" }
                },
                {
                    "@id": "ex:PersonShape",
                    "graphql:name": "Human",
                    "f:graphqlPluralName": "people"
                }
            ]
        })),
    )
    .await;
    let db = view(&ledger);

    let sdl = schema_sdl(&db).await.expect("sdl");
    assert!(sdl.contains("type Human {"), "{sdl}");
    assert!(sdl.contains("people(where: HumanFilter"), "{sdl}");
    assert!(sdl.contains("people_count(where: HumanFilter)"), "{sdl}");

    let data = run(&fluree, &db, "{ people { name __typename } }").await;
    assert_eq!(
        data,
        json!({ "people": [{ "name": "Alice", "__typename": "Human" }] })
    );
}

#[tokio::test]
async fn an_abstract_class_becomes_a_queryable_interface() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "gql-cur-interface");
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": context(),
                "@graph": [
                    { "@id": "ex:Person", "rdfs:subClassOf": { "@id": "ex:Agent" } },
                    { "@id": "ex:Company", "rdfs:subClassOf": { "@id": "ex:Agent" } },
                    { "@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice" },
                    { "@id": "ex:acme", "@type": "ex:Company", "ex:name": "Acme" },
                    { "@id": "ex:d1", "@type": "ex:Document", "ex:owner": { "@id": "ex:alice" } }
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger;

    let name_property = json!({
        "sh:path": { "@id": "ex:name" },
        "sh:datatype": { "@id": "xsd:string" },
        "sh:maxCount": 1
    });
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": context(),
                "@graph": [
                    { "@id": "ex:AgentShape", "@type": "sh:NodeShape",
                      "sh:targetClass": { "@id": "ex:Agent" },
                      "graphql:isInterface": true,
                      "sh:property": [name_property] },
                    { "@id": "ex:PersonShape", "@type": "sh:NodeShape",
                      "sh:targetClass": { "@id": "ex:Person" },
                      "sh:property": [name_property] },
                    { "@id": "ex:CompanyShape", "@type": "sh:NodeShape",
                      "sh:targetClass": { "@id": "ex:Company" },
                      "sh:property": [name_property] },
                    { "@id": "ex:DocumentShape", "@type": "sh:NodeShape",
                      "sh:targetClass": { "@id": "ex:Document" },
                      "sh:property": [{ "sh:path": { "@id": "ex:owner" },
                                        "sh:class": { "@id": "ex:Agent" },
                                        "sh:maxCount": 1 }] },
                    { "@id": "ex:Api", "@type": "graphql:Schema",
                      "graphql:publicShape": [
                          { "@id": "ex:AgentShape" }, { "@id": "ex:PersonShape" },
                          { "@id": "ex:CompanyShape" }, { "@id": "ex:DocumentShape" }
                      ] }
                ]
            }),
        )
        .await
        .expect("shapes")
        .ledger;
    let db = view(&ledger);

    let sdl = schema_sdl(&db).await.expect("sdl");
    assert!(sdl.contains("interface Agent"), "{sdl}");
    assert!(sdl.contains("type Person implements Agent"), "{sdl}");
    assert!(sdl.contains("type Company implements Agent"), "{sdl}");
    assert!(sdl.contains("owner: Agent"), "{sdl}");

    // A reference typed as the interface resolves to the concrete type.
    let data = run(
        &fluree,
        &db,
        r#"{ document(id: "ex:d1") { owner { __typename ... on Person { name } } } }"#,
    )
    .await;
    assert_eq!(
        data,
        json!({ "document": { "owner": { "__typename": "Person", "name": "Alice" } } })
    );
}

#[tokio::test]
async fn several_curated_schemas_fall_back_rather_than_guess() {
    // Serving one of two schemas arbitrarily would be worse than serving the
    // tier below, which is at least a defined answer.
    let (_fluree, ledger) = seeded(
        "gql-cur-ambiguous",
        Some(json!({
            "@context": context(),
            "@graph": [
                { "@id": "ex:ApiA", "@type": "graphql:Schema",
                  "graphql:publicShape": { "@id": "ex:PersonShape" } },
                { "@id": "ex:ApiB", "@type": "graphql:Schema",
                  "graphql:publicShape": { "@id": "ex:CompanyShape" } }
            ]
        })),
    )
    .await;
    let sdl = schema_sdl(&view(&ledger)).await.expect("sdl");
    assert!(
        sdl.contains("type AuditRecord {"),
        "fell back to tier 2:\n{sdl}"
    );
}

#[tokio::test]
async fn editing_the_curated_schema_reshapes_the_endpoint() {
    let (fluree, ledger) = seeded(
        "gql-cur-invalidate",
        Some(json!({
            "@context": context(),
            "@graph": [{
                "@id": "ex:Api",
                "@type": "graphql:Schema",
                "graphql:publicShape": { "@id": "ex:PersonShape" }
            }]
        })),
    )
    .await;
    assert!(!schema_sdl(&view(&ledger))
        .await
        .unwrap()
        .contains("type Company {"));

    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": context(),
                "@graph": [{
                    "@id": "ex:Api",
                    "graphql:publicShape": { "@id": "ex:CompanyShape" }
                }]
            }),
        )
        .await
        .expect("publish another shape")
        .ledger;

    assert!(
        schema_sdl(&view(&ledger))
            .await
            .unwrap()
            .contains("type Company {"),
        "the derivation cache should have been invalidated by the write"
    );
}
