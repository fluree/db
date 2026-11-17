//! Policy is a boundary on the *schema*, not only on the rows.
//!
//! A class or property an identity cannot read is absent from introspection
//! rather than present-but-empty, and a derivation made under policy is never
//! served from the cache to another identity. Both are load-bearing: the first
//! is what stops the SDL naming types the caller could not query, the second is
//! what stops one identity being handed another's schema.

use crate::support::{genesis_ledger, MemoryFluree};
use fluree_db_api::graphql::{schema_sdl, GraphQlRequest};
use fluree_db_api::{FlureeBuilder, GovernanceOptions, GraphDb, LedgerState};
use serde_json::{json, Value as JsonValue};

const EX: &str = "http://example.org/";

fn context() -> JsonValue {
    json!({ "ex": EX, "f": "https://ns.flur.ee/db#" })
}

/// Two classes and a sensitive property, so a policy has something to remove in
/// each dimension.
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
                        "ex:ssn": "111-11-1111"
                    },
                    { "@id": "ex:acme", "@type": "ex:Secret", "ex:name": "Acme" }
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger
}

fn root_view(ledger: &LedgerState) -> GraphDb {
    GraphDb::from_ledger_state(ledger).with_default_context(Some(context()))
}

/// A view restricted by the given policy, as the route's `wrap_policy` builds it.
async fn restricted_view(
    fluree: &MemoryFluree,
    ledger: &LedgerState,
    identity: &str,
    policy: JsonValue,
) -> GraphDb {
    let opts = GovernanceOptions {
        identity: Some(identity.to_string()),
        policy: Some(policy),
        default_allow: Some(true),
        ..GovernanceOptions::default()
    };
    fluree
        .wrap_policy(root_view(ledger), &opts)
        .await
        .expect("policy view")
}

/// Deny every `ex:Secret` subject.
fn deny_secret_class() -> JsonValue {
    json!([
        {
            "@id": "ex:denySecret",
            "@type": "f:AccessPolicy",
            "f:action": {"@id": "f:view"},
            "f:onClass": [{"@id": "http://example.org/Secret"}],
            "f:allow": false
        }
    ])
}

/// Deny the `ex:ssn` property everywhere.
fn deny_ssn_property() -> JsonValue {
    json!([
        {
            "@id": "ex:denySsn",
            "@type": "f:AccessPolicy",
            "f:action": {"@id": "f:view"},
            "f:onProperty": [{"@id": "http://example.org/ssn"}],
            "f:allow": false
        }
    ])
}

#[tokio::test]
async fn a_denied_class_is_absent_from_the_derived_schema() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-policy-class:main").await;

    let root_sdl = schema_sdl(&root_view(&ledger)).await.expect("root sdl");
    assert!(root_sdl.contains("type Secret"), "{root_sdl}");
    assert!(root_sdl.contains("type Person"), "{root_sdl}");

    let view = restricted_view(&fluree, &ledger, "did:key:zAlice", deny_secret_class()).await;
    let sdl = schema_sdl(&view).await.expect("restricted sdl");

    // Absent, not present-and-empty: a type the caller cannot query must not be
    // named in introspection at all.
    assert!(
        !sdl.contains("type Secret"),
        "denied class leaked into the schema:\n{sdl}"
    );
    assert!(sdl.contains("type Person"), "{sdl}");
    assert!(
        !sdl.contains("secrets("),
        "denied class kept its root field:\n{sdl}"
    );
}

#[tokio::test]
async fn a_denied_property_is_absent_from_its_type() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-policy-property:main").await;

    let root_sdl = schema_sdl(&root_view(&ledger)).await.expect("root sdl");
    assert!(root_sdl.contains("ssn"), "{root_sdl}");

    let view = restricted_view(&fluree, &ledger, "did:key:zAlice", deny_ssn_property()).await;
    let sdl = schema_sdl(&view).await.expect("restricted sdl");

    assert!(
        !sdl.contains("ssn"),
        "denied property leaked into the schema:\n{sdl}"
    );
    assert!(sdl.contains("name"), "{sdl}");
}

/// The cache is keyed on ledger version and context, neither of which changes
/// with identity — so a policy view declines the key outright. Without that,
/// the first (root) request would populate an entry every later identity reads.
#[tokio::test]
async fn a_policy_view_is_not_served_the_cached_root_schema() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-policy-cache:main").await;

    // Populate the cache as root first: the ordering is the whole point.
    let root_sdl = schema_sdl(&root_view(&ledger)).await.expect("root sdl");
    assert!(root_sdl.contains("type Secret"), "{root_sdl}");

    let view = restricted_view(&fluree, &ledger, "did:key:zAlice", deny_secret_class()).await;
    let sdl = schema_sdl(&view).await.expect("restricted sdl");
    assert!(
        !sdl.contains("type Secret"),
        "the restricted view was served the cached root schema:\n{sdl}"
    );

    // And back the other way: the restricted derivation must not have replaced
    // the entry root reads.
    let root_again = schema_sdl(&root_view(&ledger))
        .await
        .expect("root sdl again");
    assert!(
        root_again.contains("type Secret"),
        "root lost a class to a policy view's derivation:\n{root_again}"
    );
}

/// Introspection and execution have to agree: a denied class is not merely
/// hidden from the SDL, it is unqueryable.
#[tokio::test]
async fn a_denied_class_cannot_be_queried() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-policy-query:main").await;

    let view = restricted_view(&fluree, &ledger, "did:key:zAlice", deny_secret_class()).await;
    let response = fluree
        .graphql(&view, &GraphQlRequest::new("{ secrets { id } }"))
        .await
        .expect("graphql request");

    assert!(
        response.get("errors").is_some(),
        "querying a denied class must not succeed: {response}"
    );
}

// =========================================================================
// Execution-level filtering, where the schema cannot help
// =========================================================================

/// A per-subject rule keeps its property in the SDL, because some subjects do
/// allow it. Introspection therefore cannot hide the property and the filtering
/// has to happen while the result is formatted.
///
/// Every GraphQL root field lowers to a subgraph projection, so the whole surface
/// is hydration. Formatting through `to_jsonld_async` passed no policy, and the
/// hydrated nodes carried other identities' values even though the rows were
/// filtered correctly.
#[tokio::test]
async fn a_per_subject_policy_filters_the_returned_data() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "gql-policy-per-subject:main");
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": context(),
                "@graph": [
                    { "@id": "ex:alice", "@type": "ex:Person",
                      "ex:name": "Alice", "ex:ssn": "111-11-1111" },
                    { "@id": "ex:bob", "@type": "ex:Person",
                      "ex:name": "Bob", "ex:ssn": "222-22-2222" },
                    { "@id": "ex:aliceId", "ex:owns": { "@id": "ex:alice" } }
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger;

    // Reveals `ex:ssn` only on the record the caller's identity owns.
    let own_ssn_only = json!([{
        "@id": "ex:ownSsn",
        "@type": "f:AccessPolicy",
        "f:required": true,
        "f:action": { "@id": "f:view" },
        "f:onProperty": [{ "@id": "http://example.org/ssn" }],
        "f:query": serde_json::to_string(&json!({
            "where": { "@id": "?$identity", "http://example.org/owns": { "@id": "?$this" } }
        }))
        .expect("policy query")
    }]);
    let view = restricted_view(&fluree, &ledger, &format!("{EX}aliceId"), own_ssn_only).await;

    // The property survives introspection, so the query below is valid GraphQL
    // and the only defence left is execution.
    let sdl = schema_sdl(&view).await.expect("sdl");
    assert!(
        sdl.contains("ssn"),
        "a per-subject rule must leave ssn in the schema, or this test is not \
         exercising execution-level filtering:\n{sdl}"
    );

    let response = fluree
        .graphql(&view, &GraphQlRequest::new("{ persons { id name ssn } }"))
        .await
        .expect("graphql request");
    let body = response.to_string();

    assert!(
        body.contains("111-11-1111"),
        "alice owns her own record, so her SSN must still be returned: {response}"
    );
    assert!(
        !body.contains("222-22-2222"),
        "bob's SSN is denied to this identity and must not be returned: {response}"
    );
    assert!(
        body.contains("Alice") && body.contains("Bob"),
        "both people stay visible; only the SSN is restricted: {response}"
    );
}
