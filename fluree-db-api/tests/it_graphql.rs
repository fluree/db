//! GraphQL end to end: a ledger with no GraphQL configuration answers a
//! GraphQL query, with the schema derived from what the data actually contains.

use crate::support::{genesis_ledger, MemoryFluree};
use fluree_db_api::graphql::{schema_sdl, GraphQlRequest};
use fluree_db_api::{FlureeBuilder, GraphDb, LedgerState};
use serde_json::{json, Value as JsonValue};

const EX: &str = "http://example.org/";

fn context() -> JsonValue {
    json!({ "ex": EX })
}

/// Alice knows Bob and works for Acme; Bob has two names and no employer.
async fn seed(fluree: &MemoryFluree, ledger_id: &str) -> LedgerState {
    let ledger = genesis_ledger(fluree, ledger_id);
    fluree
        .insert(
            ledger,
            &json!({
                "@context": context(),
                "@graph": [
                    {
                        "@id": "ex:alice",
                        "@type": "ex:Person",
                        "ex:name": "Alice",
                        "ex:age": 34,
                        "ex:knows": [{ "@id": "ex:bob" }],
                        "ex:employer": [{ "@id": "ex:acme" }]
                    },
                    {
                        "@id": "ex:bob",
                        "@type": "ex:Person",
                        "ex:name": ["Bob", "Bobby"],
                        "ex:age": 41
                    },
                    { "@id": "ex:acme", "@type": "ex:Company", "ex:name": "Acme" }
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger
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

async fn run_expecting_error(fluree: &MemoryFluree, db: &GraphDb, query: &str) -> String {
    let response = fluree
        .graphql(db, &GraphQlRequest::new(query))
        .await
        .expect("graphql request");
    response["errors"][0]["message"]
        .as_str()
        .unwrap_or_else(|| panic!("expected an error, got {response}"))
        .to_string()
}

#[tokio::test]
async fn schema_is_derived_from_the_data_with_no_configuration() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-schema").await;
    let sdl = schema_sdl(&view(&ledger)).await.expect("sdl");

    for expected in [
        "type Person {",
        "type Company {",
        "id: ID!",
        "name: [String!]",
        "age: [Long!]",
        "knows(",
        "): [Person!]",
    ] {
        assert!(
            sdl.contains(expected),
            "SDL is missing `{expected}`:\n{sdl}"
        );
    }
    // Root fields, pluralised.
    assert!(sdl.contains("persons(where: PersonFilter"), "{sdl}");
    assert!(sdl.contains("companies(where: CompanyFilter"), "{sdl}");
    assert!(
        sdl.contains("persons_count(where: PersonFilter): Int!"),
        "{sdl}"
    );
    // Fluree's own vocabulary is not part of anyone's data model.
    assert!(!sdl.contains("flur.ee"), "{sdl}");
}

#[tokio::test]
async fn a_list_query_returns_nested_data() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-list").await;
    let db = view(&ledger);

    let data = run(
        &fluree,
        &db,
        r#"{
            persons(where: { name: { EQ: "Alice" } }) {
              id
              fullName: name
              age
              knows { id name }
              employer { id name }
            }
        }"#,
    )
    .await;

    assert_eq!(
        data,
        json!({
            "persons": [{
                "id": "ex:alice",
                "fullName": ["Alice"],
                "age": [34],
                "knows": [{ "id": "ex:bob", "name": ["Bob", "Bobby"] }],
                "employer": [{ "id": "ex:acme", "name": ["Acme"] }]
            }]
        })
    );
}

#[tokio::test]
async fn multi_valued_properties_come_back_as_lists() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-lists").await;
    let db = view(&ledger);

    // Bob has two names, Alice one: both render as arrays, because the schema
    // says the field is a list regardless of how many values a subject holds.
    let data = run(&fluree, &db, "{ persons { id name } }").await;
    let mut people = data["persons"].as_array().unwrap().clone();
    people.sort_by_key(|p| p["id"].as_str().unwrap().to_string());
    assert_eq!(
        people,
        vec![
            json!({ "id": "ex:alice", "name": ["Alice"] }),
            json!({ "id": "ex:bob", "name": ["Bob", "Bobby"] }),
        ]
    );

    // A subject with no value for a selected field gets an empty list, not null.
    let data = run(&fluree, &db, "{ persons { id employer { id } } }").await;
    let bob = data["persons"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["id"] == "ex:bob")
        .unwrap();
    assert_eq!(bob["employer"], json!([]));
}

#[tokio::test]
async fn a_single_root_returns_null_for_a_subject_of_another_type() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-single").await;
    let db = view(&ledger);

    let data = run(&fluree, &db, r#"{ person(id: "ex:alice") { id name } }"#).await;
    assert_eq!(
        data,
        json!({ "person": { "id": "ex:alice", "name": ["Alice"] } })
    );

    // A subject that exists but is a Company, and one that does not exist at all,
    // are both `null` — not an `{id}`-only stub.
    let data = run(&fluree, &db, r#"{ person(id: "ex:acme") { id name } }"#).await;
    assert_eq!(data, json!({ "person": null }));
    let data = run(&fluree, &db, r#"{ person(id: "ex:nobody") { id name } }"#).await;
    assert_eq!(data, json!({ "person": null }));
}

#[tokio::test]
async fn counts_are_distinct_over_subjects() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-count").await;
    let db = view(&ledger);

    let data = run(&fluree, &db, "{ persons_count }").await;
    assert_eq!(data, json!({ "persons_count": 2 }));

    // Bob has two names; the filter binds them both, so a plain count would
    // report him twice.
    let data = run(
        &fluree,
        &db,
        r#"{ persons_count(where: { name: { RE: "^Bob" } }) }"#,
    )
    .await;
    assert_eq!(data, json!({ "persons_count": 1 }));
}

#[tokio::test]
async fn filters_pagination_and_ordering_reach_the_engine() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-filters").await;
    let db = view(&ledger);

    let data = run(
        &fluree,
        &db,
        "{ persons(where: { age: { GT: 40 } }) { id } }",
    )
    .await;
    assert_eq!(data, json!({ "persons": [{ "id": "ex:bob" }] }));

    let data = run(&fluree, &db, "{ persons(orderBy: { id: DESC }) { id } }").await;
    assert_eq!(
        data,
        json!({ "persons": [{ "id": "ex:bob" }, { "id": "ex:alice" }] })
    );

    let data = run(
        &fluree,
        &db,
        "{ persons(orderBy: { id: ASC }, limit: 1) { id } }",
    )
    .await;
    assert_eq!(data, json!({ "persons": [{ "id": "ex:alice" }] }));

    // A join through a reference.
    let data = run(
        &fluree,
        &db,
        r#"{ persons(where: { knows: { name: { EQ: "Bobby" } } }) { id } }"#,
    )
    .await;
    assert_eq!(data, json!({ "persons": [{ "id": "ex:alice" }] }));

    // Absence.
    let data = run(
        &fluree,
        &db,
        "{ persons(where: { employer: { EXISTS: false } }) { id } }",
    )
    .await;
    assert_eq!(data, json!({ "persons": [{ "id": "ex:bob" }] }));
}

#[tokio::test]
async fn typename_and_introspection_work() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-introspect").await;
    let db = view(&ledger);

    let data = run(
        &fluree,
        &db,
        r#"{ person(id: "ex:alice") { __typename id } }"#,
    )
    .await;
    assert_eq!(data["person"]["__typename"], "Person");

    let data = run(
        &fluree,
        &db,
        r#"{ __type(name: "Person") { kind fields { name } } }"#,
    )
    .await;
    assert_eq!(data["__type"]["kind"], "OBJECT");
    let fields: Vec<&str> = data["__type"]["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();
    for expected in ["id", "name", "age", "knows", "employer"] {
        assert!(
            fields.contains(&expected),
            "missing field {expected} in {fields:?}"
        );
    }
}

#[tokio::test]
async fn invalid_documents_come_back_in_the_graphql_envelope() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-errors").await;
    let db = view(&ledger);

    let msg = run_expecting_error(&fluree, &db, "{ persons { nope } }").await;
    assert!(msg.contains("nope"), "{msg}");

    let msg = run_expecting_error(&fluree, &db, "{ persons { id ").await;
    assert!(!msg.is_empty());

    // A refusal from lowering surfaces as a GraphQL error, not a panic.
    let msg = run_expecting_error(
        &fluree,
        &db,
        r#"{ persons { knows(where: { name: { EQ: "x" } }) { id } } }"#,
    )
    .await;
    assert!(msg.contains("`where` on the nested field"), "{msg}");
}

#[tokio::test]
async fn a_ledger_with_no_typed_data_reports_an_empty_schema() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "gql-empty");
    let db = view(&ledger);
    let response = fluree
        .graphql(&db, &GraphQlRequest::new("{ __typename }"))
        .await
        .expect("request");
    assert_eq!(response["errors"][0]["extensions"]["code"], "EMPTY_SCHEMA");
}

// =============================================================================
// Schema derivation cache
// =============================================================================
//
// A schema cache that fails to invalidate is worse than no cache: it serves a
// schema for data that has changed. These pin both directions.

use fluree_db_api::graphql::derive_schema;
use std::sync::Arc;

#[tokio::test]
async fn the_derived_schema_is_reused_for_an_unchanged_view() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-cache-hit").await;

    let first = derive_schema(&view(&ledger)).await;
    let second = derive_schema(&view(&ledger)).await;
    assert!(
        Arc::ptr_eq(&first, &second),
        "an unchanged view should reuse the derivation, not rebuild it"
    );
}

#[tokio::test]
async fn a_write_invalidates_the_derived_schema() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-cache-invalidate").await;

    let before = derive_schema(&view(&ledger)).await;
    assert!(before.model.object("Widget").is_none());

    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": context(),
                "@graph": [{ "@id": "ex:w1", "@type": "ex:Widget", "ex:name": "Widget one" }]
            }),
        )
        .await
        .expect("insert")
        .ledger;

    let after = derive_schema(&view(&ledger)).await;
    // Two key components each catch this independently — the overlay's content
    // version and the view's `t`. Verified by breaking them: either alone still
    // invalidates, both broken and this assertion fails. The redundancy is
    // deliberate; `t` cannot distinguish two overlays that share it.
    assert!(
        !Arc::ptr_eq(&before, &after),
        "the write should have produced a new derivation"
    );
    assert!(
        after.model.object("Widget").is_some(),
        "the new class should be in the schema"
    );

    // And the endpoint agrees, not just the cache.
    let data = run(&fluree, &view(&ledger), "{ widgets { id } }").await;
    assert_eq!(data, json!({ "widgets": [{ "id": "ex:w1" }] }));
}

#[tokio::test]
async fn the_default_context_is_part_of_the_cache_key() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-cache-context").await;

    // The context decides every name in the schema, so two views of identical
    // data under different contexts must not share a derivation.
    let with_prefix = derive_schema(&view(&ledger)).await;
    let without = derive_schema(&GraphDb::from_ledger_state(&ledger)).await;
    assert!(!Arc::ptr_eq(&with_prefix, &without));

    assert_eq!(with_prefix.namer.compact(&format!("{EX}alice")), "ex:alice");
    assert_eq!(
        without.namer.compact(&format!("{EX}alice")),
        format!("{EX}alice"),
        "with no context there is no prefix to compact with"
    );
}

#[tokio::test]
async fn nested_field_arguments_page_each_subject_s_values() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "gql-nested-args");
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
                        "ex:knows": [
                            { "@id": "ex:bob" }, { "@id": "ex:carol" }, { "@id": "ex:dave" }
                        ]
                    },
                    { "@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob", "ex:age": 41 },
                    { "@id": "ex:carol", "@type": "ex:Person", "ex:name": "Carol", "ex:age": 29 },
                    { "@id": "ex:dave", "@type": "ex:Person", "ex:name": "Dave", "ex:age": 35 }
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger;
    let db = view(&ledger);

    // `limit` here bounds how many friends *Alice* shows, which the root
    // `limit` (which bounds people) cannot express.
    let data = run(
        &fluree,
        &db,
        r#"{
            person(id: "ex:alice") {
              id
              knows(orderBy: { name: ASC }, limit: 2) { id name }
            }
        }"#,
    )
    .await;
    assert_eq!(
        data,
        json!({
            "person": {
                "id": "ex:alice",
                "knows": [
                    { "id": "ex:bob", "name": ["Bob"] },
                    { "id": "ex:carol", "name": ["Carol"] }
                ]
            }
        })
    );

    // Descending, with an offset.
    let data = run(
        &fluree,
        &db,
        r#"{ person(id: "ex:alice") { knows(orderBy: { name: DESC }, offset: 1) { name } } }"#,
    )
    .await;
    assert_eq!(
        data["person"]["knows"],
        json!([{ "name": ["Carol"] }, { "name": ["Bob"] }])
    );

    // Ordering by `id` reads the subject IRI, not a predicate.
    let data = run(
        &fluree,
        &db,
        r#"{ person(id: "ex:alice") { knows(orderBy: { id: DESC }, limit: 1) { id } } }"#,
    )
    .await;
    assert_eq!(data["person"]["knows"], json!([{ "id": "ex:dave" }]));
}
