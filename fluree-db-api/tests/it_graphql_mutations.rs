//! Mutations: tier 3 only, opt-in, and going through the same write path
//! everything else does.

use crate::support::{genesis_ledger, MemoryFluree};
use fluree_db_api::graphql::{schema_sdl, schema_sdl_with_mutations, GraphQlRequest};
use fluree_db_api::{FlureeBuilder, GraphDb, LedgerState};
use serde_json::{json, Value as JsonValue};

const EX: &str = "http://example.org/";

fn context() -> JsonValue {
    json!({
        "ex": EX,
        "sh": "http://www.w3.org/ns/shacl#",
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        "graphql": "http://datashapes.org/graphql#",
        "f": "https://ns.flur.ee/db#"
    })
}

fn view(ledger: &LedgerState) -> GraphDb {
    GraphDb::from_ledger_state(ledger).with_default_context(Some(context()))
}

/// A ledger with a Person shape and a curated schema built from `schema_extra`.
async fn seeded(ledger_id: &str, schema_extra: JsonValue) -> (MemoryFluree, LedgerState) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, ledger_id);

    let mut schema_node = json!({
        "@id": "ex:Api",
        "@type": "graphql:Schema",
        "graphql:publicShape": [{ "@id": "ex:PersonShape" }, { "@id": "ex:CompanyShape" }]
    });
    if let (Some(node), Some(extra)) = (schema_node.as_object_mut(), schema_extra.as_object()) {
        for (k, v) in extra {
            node.insert(k.clone(), v.clone());
        }
    }

    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": context(),
                "@graph": [
                    {
                        "@id": "ex:PersonShape",
                        "@type": "sh:NodeShape",
                        "sh:targetClass": { "@id": "ex:Person" },
                        "sh:property": [
                            { "sh:path": { "@id": "ex:name" },
                              "sh:datatype": { "@id": "xsd:string" },
                              "sh:minCount": 1, "sh:maxCount": 1 },
                            { "sh:path": { "@id": "ex:nickname" },
                              "sh:datatype": { "@id": "xsd:string" }, "sh:maxCount": 1 },
                            { "sh:path": { "@id": "ex:employer" },
                              "sh:class": { "@id": "ex:Company" }, "sh:maxCount": 1 }
                        ]
                    },
                    {
                        "@id": "ex:CompanyShape",
                        "@type": "sh:NodeShape",
                        "sh:targetClass": { "@id": "ex:Company" },
                        "sh:property": [
                            { "sh:path": { "@id": "ex:name" },
                              "sh:datatype": { "@id": "xsd:string" }, "sh:maxCount": 1 }
                        ]
                    },
                    schema_node,
                    { "@id": "ex:acme", "@type": "ex:Company", "ex:name": "Acme" }
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger;
    (fluree, ledger)
}

/// The usual configuration: mutations on, with a base to mint IRIs under.
fn mutations_enabled() -> JsonValue {
    json!({ "f:graphqlEnableMutations": true, "f:graphqlIriBase": EX })
}

async fn mutate(
    fluree: &MemoryFluree,
    ledger: LedgerState,
    query: &str,
) -> (JsonValue, LedgerState) {
    let (response, ledger) = fluree
        .graphql_transact(ledger, Some(context()), &GraphQlRequest::new(query))
        .await
        .expect("graphql request");
    assert!(
        response.get("errors").is_none(),
        "unexpected errors: {}",
        serde_json::to_string_pretty(&response).unwrap()
    );
    (response["data"].clone(), ledger)
}

async fn mutate_expecting_error(
    fluree: &MemoryFluree,
    ledger: LedgerState,
    query: &str,
) -> (String, LedgerState) {
    let (response, ledger) = fluree
        .graphql_transact(ledger, Some(context()), &GraphQlRequest::new(query))
        .await
        .expect("graphql request");
    let message = response["errors"][0]["message"]
        .as_str()
        .unwrap_or_else(|| panic!("expected an error, got {response}"))
        .to_string();
    (message, ledger)
}

#[tokio::test]
async fn mutations_are_absent_unless_the_schema_enables_them() {
    // A schema derived from whatever a ledger happens to hold must never become
    // a write surface by accident.
    let (fluree, ledger) = seeded("gql-mut-off", json!({})).await;
    let sdl = schema_sdl_with_mutations(&view(&ledger))
        .await
        .expect("sdl");
    assert!(!sdl.contains("type Mutation"), "{sdl}");
    assert!(!sdl.contains("create_Person"), "{sdl}");

    // async-graphql rejects the document outright: with no mutation fields
    // registered there is no Mutation type for it to validate against.
    let (msg, _) = mutate_expecting_error(
        &fluree,
        ledger,
        r#"mutation { create_Person(input: { name: "Alice" }) { id } }"#,
    )
    .await;
    assert!(msg.contains("not configured for mutations"), "{msg}");
}

#[tokio::test]
async fn the_read_endpoint_never_advertises_or_runs_mutations() {
    let (fluree, ledger) = seeded("gql-mut-readonly", mutations_enabled()).await;
    let db = view(&ledger);

    // Enabled on the write path...
    assert!(schema_sdl_with_mutations(&db)
        .await
        .unwrap()
        .contains("create_Person"));
    // ...and absent from the read one, so the SDL a read endpoint serves
    // matches what it can actually answer.
    assert!(!schema_sdl(&db).await.unwrap().contains("create_Person"));

    let response = fluree
        .graphql(
            &db,
            &GraphQlRequest::new(r#"mutation { create_Person(input: { name: "A" }) { id } }"#),
        )
        .await
        .expect("request");
    assert!(response["errors"][0]["message"]
        .as_str()
        .unwrap()
        .contains("not configured for mutations"));
}

#[tokio::test]
async fn create_mints_an_iri_and_returns_the_new_object() {
    let (fluree, ledger) = seeded("gql-mut-create", mutations_enabled()).await;

    let (data, ledger) = mutate(
        &fluree,
        ledger,
        r#"mutation {
            create_Person(input: { name: "Alice", employer: "ex:acme" }) {
              id
              name
              employer { name }
            }
        }"#,
    )
    .await;

    let person = &data["create_Person"];
    assert_eq!(person["name"], "Alice");
    assert_eq!(person["employer"]["name"], "Acme");
    let id = person["id"].as_str().expect("minted id");
    assert!(
        id.starts_with("ex:"),
        "minted under the declared base: {id}"
    );

    // It is really in the ledger, readable by an ordinary query.
    let data = fluree
        .graphql(&view(&ledger), &GraphQlRequest::new("{ persons { name } }"))
        .await
        .expect("query");
    assert_eq!(data["data"], json!({ "persons": [{ "name": "Alice" }] }));
}

#[tokio::test]
async fn create_accepts_an_explicit_id_and_needs_a_base_without_one() {
    let (fluree, ledger) = seeded("gql-mut-explicit-id", mutations_enabled()).await;
    let (data, _) = mutate(
        &fluree,
        ledger,
        r#"mutation { create_Person(input: { id: "ex:bob", name: "Bob" }) { id name } }"#,
    )
    .await;
    assert_eq!(
        data["create_Person"],
        json!({ "id": "ex:bob", "name": "Bob" })
    );

    // Without a base there is no safe IRI to invent, so it says so rather than
    // minting one somewhere arbitrary.
    let (fluree, ledger) = seeded(
        "gql-mut-no-base",
        json!({ "f:graphqlEnableMutations": true }),
    )
    .await;
    let (msg, _) = mutate_expecting_error(
        &fluree,
        ledger,
        r#"mutation { create_Person(input: { name: "Alice" }) { id } }"#,
    )
    .await;
    assert!(msg.contains("graphqlIriBase"), "{msg}");

    // An explicit id still works there.
    let (fluree, ledger) = seeded(
        "gql-mut-no-base-2",
        json!({ "f:graphqlEnableMutations": true }),
    )
    .await;
    let (data, _) = mutate(
        &fluree,
        ledger,
        r#"mutation { create_Person(input: { id: "ex:carol", name: "Carol" }) { id } }"#,
    )
    .await;
    assert_eq!(data["create_Person"]["id"], "ex:carol");
}

#[tokio::test]
async fn update_replaces_the_listed_properties_and_a_null_clears_one() {
    let (fluree, ledger) = seeded("gql-mut-update", mutations_enabled()).await;
    let (_, ledger) = mutate(
        &fluree,
        ledger,
        r#"mutation {
            create_Person(input: { id: "ex:alice", name: "Alice", nickname: "Al" }) { id }
        }"#,
    )
    .await;

    let (data, ledger) = mutate(
        &fluree,
        ledger,
        r#"mutation {
            update_Person(ids: ["ex:alice"], set: { name: "Alice B" }) {
              affected_count
              affected_objects { id name nickname }
            }
        }"#,
    )
    .await;
    assert_eq!(
        data["update_Person"],
        json!({
            "affected_count": 1,
            // `name` replaced; `nickname` untouched, because `set` lists only
            // what it means to change.
            "affected_objects": [{ "id": "ex:alice", "name": "Alice B", "nickname": "Al" }]
        })
    );

    let (data, _) = mutate(
        &fluree,
        ledger,
        r#"mutation {
            update_Person(ids: ["ex:alice"], set: { nickname: null }) {
              affected_objects { name nickname }
            }
        }"#,
    )
    .await;
    assert_eq!(
        data["update_Person"]["affected_objects"],
        json!([{ "name": "Alice B", "nickname": null }])
    );
}

#[tokio::test]
async fn delete_retracts_the_subject_and_is_scoped_to_its_type() {
    let (fluree, ledger) = seeded("gql-mut-delete", mutations_enabled()).await;
    let (_, ledger) = mutate(
        &fluree,
        ledger,
        r#"mutation { create_Person(input: { id: "ex:alice", name: "Alice" }) { id } }"#,
    )
    .await;

    // `delete_Person` on a Company's IRI is a no-op, not a wipe: the delete is
    // scoped to the type the field names.
    let (data, ledger) = mutate(
        &fluree,
        ledger,
        r#"mutation { delete_Person(ids: ["ex:acme"]) { affected_count } }"#,
    )
    .await;
    assert_eq!(data["delete_Person"]["affected_count"], 1);
    let read = fluree
        .graphql(
            &view(&ledger),
            &GraphQlRequest::new("{ companies { name } }"),
        )
        .await
        .unwrap();
    assert_eq!(
        read["data"],
        json!({ "companies": [{ "name": "Acme" }] }),
        "the Company survived a delete_Person"
    );

    let (_, ledger) = mutate(
        &fluree,
        ledger,
        r#"mutation { delete_Person(ids: ["ex:alice"]) { affected_count } }"#,
    )
    .await;
    let read = fluree
        .graphql(&view(&ledger), &GraphQlRequest::new("{ persons { name } }"))
        .await
        .unwrap();
    assert_eq!(read["data"], json!({ "persons": [] }));
}

#[tokio::test]
async fn a_shacl_violation_surfaces_as_a_graphql_error_and_writes_nothing() {
    let (fluree, ledger) = seeded("gql-mut-shacl", mutations_enabled()).await;

    // `ex:name` is `sh:minCount 1`; a create without it must be rejected by the
    // same validation any other write would face.
    let (msg, ledger) = mutate_expecting_error(
        &fluree,
        ledger,
        r#"mutation { create_Person(input: { nickname: "Al" }) { id } }"#,
    )
    .await;
    assert!(
        msg.to_lowercase().contains("shacl") || msg.contains("minCount") || msg.contains("count"),
        "expected a validation error, got: {msg}"
    );

    let read = fluree
        .graphql(&view(&ledger), &GraphQlRequest::new("{ persons_count }"))
        .await
        .unwrap();
    assert_eq!(read["data"], json!({ "persons_count": 0 }));
}

#[tokio::test]
async fn mutations_run_serially_and_later_ones_see_earlier_writes() {
    let (fluree, ledger) = seeded("gql-mut-serial", mutations_enabled()).await;
    let (data, ledger) = mutate(
        &fluree,
        ledger,
        r#"mutation {
            first: create_Person(input: { id: "ex:alice", name: "Alice" }) { id }
            second: update_Person(ids: ["ex:alice"], set: { nickname: "Al" }) {
              affected_objects { name nickname }
            }
        }"#,
    )
    .await;
    assert_eq!(data["first"]["id"], "ex:alice");
    assert_eq!(
        data["second"]["affected_objects"],
        json!([{ "name": "Alice", "nickname": "Al" }]),
        "the update saw the create's subject"
    );

    let read = fluree
        .graphql(&view(&ledger), &GraphQlRequest::new("{ persons { name } }"))
        .await
        .unwrap();
    assert_eq!(read["data"], json!({ "persons": [{ "name": "Alice" }] }));
}

#[tokio::test]
async fn the_input_type_refuses_what_it_cannot_write() {
    let (fluree, ledger) = seeded("gql-mut-input", mutations_enabled()).await;
    let sdl = schema_sdl_with_mutations(&view(&ledger))
        .await
        .expect("sdl");

    // A reference is written as the target's `id`; creating one as a side
    // effect would write an object the caller did not name.
    assert!(sdl.contains("employer: ID"), "{sdl}");
    // Every input field is nullable, so a partial `update` is expressible even
    // where the output type says `!`.
    assert!(sdl.contains("name: String\n"), "{sdl}");

    // `id` is identity: renaming a subject is a create and a delete.
    let (msg, _) = mutate_expecting_error(
        &fluree,
        ledger,
        r#"mutation { update_Person(ids: ["ex:acme"], set: { id: "ex:other" }) { affected_count } }"#,
    )
    .await;
    assert!(msg.contains("cannot change `id`"), "{msg}");
}

#[tokio::test]
async fn a_mutation_with_no_ids_is_refused() {
    let (fluree, ledger) = seeded("gql-mut-empty", mutations_enabled()).await;
    let (msg, _) = mutate_expecting_error(
        &fluree,
        ledger,
        "mutation { delete_Person(ids: []) { affected_count } }",
    )
    .await;
    assert!(msg.contains("no `ids`"), "{msg}");
}
