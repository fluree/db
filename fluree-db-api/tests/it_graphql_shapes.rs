//! Tier 2 end to end: SHACL shapes written to a ledger change its GraphQL
//! schema, with no GraphQL-specific configuration anywhere.

use crate::support::{genesis_ledger, MemoryFluree};
use fluree_db_api::graphql::{schema_sdl, GraphQlRequest};
use fluree_db_api::{FlureeBuilder, GraphDb, LedgerState};
use serde_json::{json, Value as JsonValue};

const EX: &str = "http://example.org/";

fn context() -> JsonValue {
    json!({
        "ex": EX,
        "sh": "http://www.w3.org/ns/shacl#",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
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

/// Two people and a company, plus shapes over both classes.
async fn seeded(ledger_id: &str, shapes: JsonValue) -> (MemoryFluree, LedgerState) {
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
                        "ex:status": { "@id": "ex:Active" },
                        "ex:internalNote": "do not show",
                        "ex:employer": { "@id": "ex:acme" }
                    },
                    {
                        "@id": "ex:bob",
                        "@type": "ex:Person",
                        "ex:name": "Bob",
                        "ex:status": { "@id": "ex:Retired" },
                        "ex:employer": { "@id": "ex:acme" }
                    },
                    { "@id": "ex:acme", "@type": "ex:Company", "ex:name": "Acme" }
                ]
            }),
        )
        .await
        .expect("seed data")
        .ledger;

    let ledger = fluree
        .insert(ledger, &shapes)
        .await
        .expect("seed shapes")
        .ledger;
    (fluree, ledger)
}

fn person_shape(extra: Vec<JsonValue>) -> JsonValue {
    let mut properties = vec![json!({
        "sh:path": { "@id": "ex:name" },
        "sh:datatype": { "@id": "xsd:string" },
        "sh:minCount": 1,
        "sh:maxCount": 1,
        "sh:description": "The person's full name.",
        "sh:order": 1
    })];
    properties.extend(extra);
    json!({
        "@context": context(),
        "@graph": [{
            "@id": "ex:PersonShape",
            "@type": "sh:NodeShape",
            "sh:targetClass": { "@id": "ex:Person" },
            "sh:description": "A person we know about.",
            "sh:property": properties
        }]
    })
}

#[tokio::test]
async fn a_shape_supplies_cardinality_and_documentation() {
    let (_fluree, ledger) = seeded("gql-shape-card", person_shape(vec![])).await;
    let sdl = schema_sdl(&view(&ledger)).await.expect("sdl");

    // `sh:minCount 1` + `sh:maxCount 1` is what makes this `String!` rather than
    // the inferred `[String!]`.
    assert!(sdl.contains("name: String!"), "{sdl}");
    assert!(sdl.contains("A person we know about."), "{sdl}");
    assert!(sdl.contains("The person's full name."), "{sdl}");

    // Properties the shape did not mention are still there — the shape is open.
    assert!(sdl.contains("internalNote: [String!]"), "{sdl}");
    // And the unshaped class keeps its inferred type.
    assert!(sdl.contains("type Company {"), "{sdl}");
}

#[tokio::test]
async fn a_closed_shape_drops_undeclared_properties() {
    let mut shapes = person_shape(vec![]);
    shapes["@graph"][0]["sh:closed"] = json!(true);
    // `sh:closed` also rejects rdf:type unless ignored; that is a validation
    // concern, not a schema one, and the GraphQL side reads only the flag.
    let (fluree, ledger) = seeded("gql-shape-closed", shapes).await;
    let db = view(&ledger);

    let sdl = schema_sdl(&db).await.expect("sdl");
    assert!(sdl.contains("name: String!"), "{sdl}");
    assert!(!sdl.contains("internalNote"), "{sdl}");
    assert!(!sdl.contains("employer"), "{sdl}");

    // A field that is not in the schema cannot be queried.
    let response = fluree
        .graphql(&db, &GraphQlRequest::new("{ persons { internalNote } }"))
        .await
        .expect("request");
    assert!(response["errors"][0]["message"]
        .as_str()
        .unwrap()
        .contains("internalNote"));

    let data = run(&fluree, &db, "{ persons(orderBy: { id: ASC }) { name } }").await;
    assert_eq!(
        data,
        json!({ "persons": [{ "name": "Alice" }, { "name": "Bob" }] })
    );
}

#[tokio::test]
async fn sh_in_becomes_a_queryable_enum() {
    let shapes = person_shape(vec![json!({
        "sh:path": { "@id": "ex:status" },
        "sh:maxCount": 1,
        "sh:in": { "@list": [{ "@id": "ex:Active" }, { "@id": "ex:Retired" }] }
    })]);
    let (fluree, ledger) = seeded("gql-shape-enum", shapes).await;
    let db = view(&ledger);

    let sdl = schema_sdl(&db).await.expect("sdl");
    assert!(sdl.contains("enum PersonStatusEnum"), "{sdl}");
    assert!(sdl.contains("status: PersonStatusEnum"), "{sdl}");

    // Values come back as member names, not IRIs.
    let data = run(
        &fluree,
        &db,
        "{ persons(orderBy: { id: ASC }) { name status } }",
    )
    .await;
    assert_eq!(
        data,
        json!({
            "persons": [
                { "name": "Alice", "status": "Active" },
                { "name": "Bob", "status": "Retired" }
            ]
        })
    );

    // And they filter by name, with the IRI supplied by the schema.
    let data = run(
        &fluree,
        &db,
        "{ persons(where: { status: { EQ: Retired } }) { name } }",
    )
    .await;
    assert_eq!(data, json!({ "persons": [{ "name": "Bob" }] }));

    let data = run(
        &fluree,
        &db,
        "{ persons(where: { status: { IN: [Active, Retired] } }) { name } }",
    )
    .await;
    assert_eq!(data["persons"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn an_inverse_path_becomes_a_queryable_reverse_field() {
    let shapes = json!({
        "@context": context(),
        "@graph": [{
            "@id": "ex:CompanyShape",
            "@type": "sh:NodeShape",
            "sh:targetClass": { "@id": "ex:Company" },
            "sh:property": [{
                "sh:path": { "sh:inversePath": { "@id": "ex:employer" } },
                "sh:class": { "@id": "ex:Person" },
                "sh:name": "staff"
            }]
        }]
    });
    let (fluree, ledger) = seeded("gql-shape-inverse", shapes).await;
    let db = view(&ledger);

    let sdl = schema_sdl(&db).await.expect("sdl");
    // A reverse field takes no nested arguments: the hydration IR has no
    // modifier slot for a reverse selection, so the schema does not offer them.
    assert!(sdl.contains("staff: [Person!]"), "{sdl}");

    // The reverse edge is traversed, not just declared.
    let data = run(
        &fluree,
        &db,
        r#"{ company(id: "ex:acme") { name staff { id } } }"#,
    )
    .await;
    assert_eq!(
        data,
        json!({
            "company": {
                "name": ["Acme"],
                "staff": [{ "id": "ex:alice" }, { "id": "ex:bob" }]
            }
        })
    );
}

#[tokio::test]
async fn a_shape_for_a_class_with_no_instances_still_produces_a_type() {
    let shapes = json!({
        "@context": context(),
        "@graph": [{
            "@id": "ex:WidgetShape",
            "@type": "sh:NodeShape",
            "sh:targetClass": { "@id": "ex:Widget" },
            "sh:property": [{
                "sh:path": { "@id": "ex:label" },
                "sh:datatype": { "@id": "xsd:string" },
                "sh:maxCount": 1
            }]
        }]
    });
    let (fluree, ledger) = seeded("gql-shape-empty-class", shapes).await;
    let db = view(&ledger);

    // A client has to be able to see the schema it is about to write against.
    let sdl = schema_sdl(&db).await.expect("sdl");
    assert!(sdl.contains("type Widget {"), "{sdl}");
    assert!(sdl.contains("label: String"), "{sdl}");

    let data = run(&fluree, &db, "{ widgets { id label } }").await;
    assert_eq!(data, json!({ "widgets": [] }));
}

#[tokio::test]
async fn editing_a_shape_reshapes_the_schema() {
    let (fluree, ledger) = seeded("gql-shape-invalidate", person_shape(vec![])).await;
    assert!(schema_sdl(&view(&ledger))
        .await
        .unwrap()
        .contains("name: String!"));

    // Deactivating the shape drops back to the inferred schema.
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": context(),
                "@graph": [{ "@id": "ex:PersonShape", "sh:deactivated": true }]
            }),
        )
        .await
        .expect("deactivate")
        .ledger;

    let sdl = schema_sdl(&view(&ledger)).await.expect("sdl");
    assert!(
        sdl.contains("name: [String!]"),
        "the shape cache should have been invalidated by the write:\n{sdl}"
    );
}
